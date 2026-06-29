use std::collections::BTreeMap;
use std::sync::Arc;

use crate::function_tool::FunctionCallError;
use crate::sandboxing::SandboxPermissions;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::handlers::apply_granted_turn_permissions;
use crate::tools::handlers::parse_arguments;
use crate::tools::handlers::resolve_tool_environment;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use crate::unified_exec::ExecCommandRequest;
use crate::unified_exec::UnifiedExecContext;
use crate::unified_exec::spawn_delivery;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use codex_utils_path_uri::PathConvention;
use serde::Deserialize;
use serde_json::json;

use super::ExecCommandArgs;
use super::get_command;
use super::shell_mode_for_environment;

const MONITOR_TOOL_NAME: &str = "monitor";

/// Time the spawn blocks for initial output before returning. Kept short so the
/// tool call returns quickly while the watcher keeps running in the background.
const MONITOR_YIELD_MS: u64 = 250;

/// Byte cap on the initial-yield output kept as the delivery seed, so a fast
/// producer cannot hand a large buffer to the delivery loop before its
/// per-line, flood, and notification caps apply. Keeps the earliest output (a
/// banner, an already-matching line); the live broadcast covers the rest once
/// the delivery loop subscribes.
const MONITOR_SEED_MAX_BYTES: usize = 8 * 1024;

/// A monitor description is a short label; bound it so it cannot inflate the
/// size of every notification (which prepends it) or the `list` output.
const MAX_DESCRIPTION_BYTES: usize = 256;

/// Truncates a description to `MAX_DESCRIPTION_BYTES` at a UTF-8 char boundary.
fn truncate_description(mut description: String) -> String {
    if description.len() > MAX_DESCRIPTION_BYTES {
        let mut end = MAX_DESCRIPTION_BYTES;
        while end > 0 && !description.is_char_boundary(end) {
            end -= 1;
        }
        description.truncate(end);
    }
    description
}

pub struct MonitorHandler;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum MonitorAction {
    Start,
    Stop,
    List,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorArgs {
    action: MonitorAction,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    id: Option<String>,
}

fn create_monitor_tool() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "action".to_string(),
            JsonSchema::string_enum(
                vec![json!("start"), json!("stop"), json!("list")],
                Some("Which monitor operation to perform.".to_string()),
            ),
        ),
        (
            "command".to_string(),
            JsonSchema::string(Some(
                "Shell command to run as the watcher (action=start). Each line it prints (stdout or stderr) becomes one notification, so filter to the lines you care about (e.g. `tail -F app.log | grep --line-buffered ERROR`).".to_string(),
            )),
        ),
        (
            "description".to_string(),
            JsonSchema::string(Some(
                "Short label prefixed to every notification this watcher emits (action=start), e.g. \"errors in app.log\".".to_string(),
            )),
        ),
        (
            "id".to_string(),
            JsonSchema::string(Some(
                "The monitor id returned by a previous start (action=stop).".to_string(),
            )),
        ),
    ]);

    ToolSpec::Function(ResponsesApiTool {
        name: MONITOR_TOOL_NAME.to_string(),
        description: "Run a shell command as a long-lived background watcher. Each line the command prints (stdout or stderr) is delivered to you as a notification, prefixed with the label; lines emitted close together are batched. The watch ends when the command exits. Use it to react to events without polling: `fswatch <path>` or `inotifywait -m <path>` for file changes, `tail -F <log> | grep --line-buffered <pattern>` for log signals, or a poll loop for remote state. action=start begins a watch and returns its id; action=stop ends the watch with that id; action=list shows active watches."
            .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["action".to_string()]),
            /*additional_properties*/ Some(false.into()),
        ),
        output_schema: None,
    })
}

impl ToolExecutor<ToolInvocation> for MonitorHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(MONITOR_TOOL_NAME)
    }

    fn spec(&self) -> ToolSpec {
        create_monitor_tool()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(handle_call(invocation))
    }
}

impl CoreToolRuntime for MonitorHandler {}

async fn handle_call(
    invocation: ToolInvocation,
) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
    let ToolInvocation {
        session,
        turn,
        step_context,
        call_id,
        payload,
        ..
    } = invocation;
    let ToolPayload::Function { arguments } = payload else {
        return Err(FunctionCallError::RespondToModel(format!(
            "{MONITOR_TOOL_NAME} handler received unsupported payload"
        )));
    };
    let args: MonitorArgs = parse_arguments(&arguments)?;

    match args.action {
        MonitorAction::Start => {
            start(
                &session,
                &turn,
                &step_context.environments,
                call_id,
                args.command,
                args.description,
            )
            .await
        }
        MonitorAction::Stop => {
            let Some(id) = args.id else {
                return Err(FunctionCallError::RespondToModel(
                    "action=stop requires `id`".to_string(),
                ));
            };
            // Look up the process id without pruning yet, terminate the
            // underlying process first, then remove the registry entry (which
            // also aborts the delivery task). Ordering it this way keeps the
            // watcher observable if termination fails, and surfaces that outcome
            // instead of silently dropping it.
            let message = match session.services.monitor_manager.process_id(&id).await {
                Some(process_id) => {
                    let terminated = session
                        .services
                        .unified_exec_manager
                        .terminate_process(process_id)
                        .await;
                    session.services.monitor_manager.remove(&id).await;
                    if terminated {
                        format!("Stopped monitor {id}.")
                    } else {
                        format!(
                            "Removed monitor {id}; its watcher process had already exited or could not be terminated."
                        )
                    }
                }
                None => format!("No active monitor with id {id}."),
            };
            Ok(text_output(message))
        }
        MonitorAction::List => {
            let monitors = session.services.monitor_manager.list().await;
            let message = if monitors.is_empty() {
                "No active monitors.".to_string()
            } else {
                monitors
                    .iter()
                    // Use debug formatting so a description or command that
                    // contains newlines, brackets, or spacing stays a single
                    // unambiguous quoted/escaped token per row.
                    .map(|m| {
                        format!(
                            "{}  description={:?}  command={:?}",
                            m.id, m.description, m.command
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            Ok(text_output(message))
        }
    }
}

async fn start(
    session: &Arc<crate::session::session::Session>,
    turn: &Arc<crate::session::turn_context::TurnContext>,
    environments: &crate::environment_selection::TurnEnvironmentSnapshot,
    call_id: String,
    command: Option<String>,
    description: Option<String>,
) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
    let command = command.filter(|c| !c.trim().is_empty()).ok_or_else(|| {
        FunctionCallError::RespondToModel("action=start requires a non-empty `command`".to_string())
    })?;
    let description = description
        .filter(|d| !d.trim().is_empty())
        .ok_or_else(|| {
            FunctionCallError::RespondToModel(
                "action=start requires a non-empty `description`".to_string(),
            )
        })?;
    // Bound the label so it cannot inflate every notification or the list output.
    let description = truncate_description(description);

    let manager = &session.services.unified_exec_manager;
    let context = UnifiedExecContext::new(session.clone(), turn.clone(), call_id);
    let Some(turn_environment) =
        resolve_tool_environment(environments, /*environment_id*/ None)?
    else {
        return Err(FunctionCallError::RespondToModel(
            "unified exec is unavailable in this session".to_string(),
        ));
    };

    let cwd = turn_environment.cwd().clone();

    // Run the watcher with the same sandbox access the session has granted,
    // exactly like the normal exec tool (see exec_command.rs). Without this the
    // watcher would always fall back to the turn's restrictive default sandbox
    // even when the session has been granted escalated (e.g. full-access)
    // permissions, so a monitor command would be sandboxed differently from
    // every other command the agent runs in the same session.
    //
    // Match granted permissions against the cwd the watcher will actually run in
    // (`cwd`), falling back to the session cwd for non-native/foreign paths,
    // mirroring exec_command's `permission_cwd`. Matching against a different cwd
    // than the command runs in would let a cwd-scoped grant be evaluated for one
    // directory and applied to a command in another.
    // Only use the watcher cwd for permission matching when it is a native-
    // convention absolute path; otherwise fall back to the session cwd, exactly
    // as exec_command does (a foreign drive-style path can otherwise look like a
    // host absolute path on POSIX).
    let native_cwd = cwd
        .to_abs_path()
        .ok()
        .filter(|_| cwd.infer_path_convention() == Some(PathConvention::native()));
    let permission_cwd = match native_cwd.as_ref() {
        Some(native) => native.as_path(),
        None => turn.config.cwd.as_path(),
    };
    let effective_permissions = apply_granted_turn_permissions(
        session.as_ref(),
        &turn_environment.environment_id,
        permission_cwd,
        SandboxPermissions::UseDefault,
        /*additional_permissions*/ None,
    )
    .await;
    let environment = Arc::clone(&turn_environment.environment);
    let shell_mode =
        shell_mode_for_environment(&turn.unified_exec_shell_mode, environment.as_ref());
    let shell = turn_environment
        .shell
        .clone()
        .map(Arc::new)
        .unwrap_or_else(|| session.user_shell());

    // Resolve `command` to a concrete shell invocation with the session default
    // shell and no permission escalation; the monitor only needs the resolved
    // command + shell type back from `get_command`.
    let exec_args = ExecCommandArgs {
        cmd: command.clone(),
        shell: None,
        login: None,
        tty: false,
        yield_time_ms: 0,
        max_output_tokens: None,
        sandbox_permissions: Default::default(),
        additional_permissions: None,
        justification: None,
        prefix_rule: None,
    };
    let resolved = get_command(
        &exec_args,
        shell,
        &shell_mode,
        turn.config.permissions.allow_login_shell,
    )
    .map_err(FunctionCallError::RespondToModel)?;

    let process_id = manager.allocate_process_id().await;
    let request = ExecCommandRequest {
        command: resolved.command,
        shell_type: resolved.shell_type,
        hook_command: command.clone(),
        process_id,
        yield_time_ms: MONITOR_YIELD_MS,
        max_output_tokens: None,
        cwd: cwd.clone(),
        sandbox_cwd: cwd,
        turn_environment: turn_environment.clone(),
        shell_mode,
        network: turn.network.clone(),
        tty: false,
        sandbox_permissions: effective_permissions.sandbox_permissions,
        additional_permissions: effective_permissions.additional_permissions.clone(),
        additional_permissions_preapproved: effective_permissions.permissions_preapproved,
        justification: None,
        prefix_rule: None,
    };

    let initial_output = match manager.exec_command(request, &context).await {
        Ok(output) => output,
        Err(err) => {
            manager.release_process_id(process_id).await;
            return Err(FunctionCallError::RespondToModel(format!(
                "failed to start monitor: {err:?}"
            )));
        }
    };

    // The delivery loop subscribes to the process output stream only inside
    // `spawn_delivery`, and the broadcast does not replay to a late subscriber.
    // Seed it with whatever the initial yield already captured so the first
    // lines (a banner, an already-matching grep) are not dropped.
    let id = format!("mon_{}", uuid::Uuid::new_v4());
    // The delivery loop blocks on `registered_rx` until we have inserted the
    // monitor below, so it can never deregister an id that is not yet in the
    // registry (which would leave a dead "ghost" entry).
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
    // Bound the transient seed: `raw_output` is the untruncated capture from the
    // initial yield, so cap it before handing it to the delivery loop (whose
    // per-line/flood/notification caps only apply afterwards). Keeps the
    // earliest bytes; the live broadcast covers everything after subscription.
    let mut seed = initial_output.raw_output;
    seed.truncate(MONITOR_SEED_MAX_BYTES);
    let Some(task) = spawn_delivery(
        manager,
        process_id,
        id.clone(),
        Arc::downgrade(session),
        description.clone(),
        seed,
        registered_rx,
    )
    .await
    else {
        return Err(FunctionCallError::RespondToModel(
            "command exited immediately; a monitor command must keep running and print events"
                .to_string(),
        ));
    };

    session
        .services
        .monitor_manager
        .insert(id.clone(), process_id, description.clone(), command, task)
        .await;
    // Registration is complete; release the delivery loop.
    let _ = registered_tx.send(());
    Ok(text_output(format!(
        "Started monitor {id}: watching \"{description}\". Stop it with action=stop, id={id}."
    )))
}

fn text_output(message: String) -> Box<dyn crate::tools::context::ToolOutput> {
    boxed_tool_output(FunctionToolOutput::from_text(
        message,
        /*success*/ Some(true),
    ))
}

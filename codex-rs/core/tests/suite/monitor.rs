//! Proves the `monitor` tool: a background command's output wakes the agent and
//! reaches the model as a labelled user notification.

use std::time::Duration;
use std::time::Instant;

use codex_features::Feature;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::skip_if_sandbox;
use core_test_support::test_codex::test_codex;
use serde_json::json;

/// Starts a monitor on `command` and asserts `marker` reaches the model as a
/// `[signal watch] ...` user message within 20s. Exercises the whole path: the
/// tool call, a real background process, its output line, the idle-wake, and the
/// labelled delivery.
async fn assert_monitor_wakes_with(command: &str, marker: &str) -> anyhow::Result<()> {
    let server = start_mock_server().await;

    let args = json!({
        "action": "start",
        "command": command,
        "description": "signal watch",
    })
    .to_string();

    let mock = mount_sse_sequence(
        &server,
        vec![
            // User turn: the model calls the monitor tool.
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call("call-1", "monitor", &args),
                ev_completed("resp-1"),
            ]),
            // Continuation after the tool result.
            sse(vec![
                ev_assistant_message("msg-1", "watching"),
                ev_completed("resp-2"),
            ]),
            // The turn the monitor's output wakes.
            sse(vec![
                ev_assistant_message("msg-2", "saw it"),
                ev_completed("resp-3"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::Monitor)
            .expect("enable monitor feature");
    });
    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "watch for the signal".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let delivered = mock.requests().iter().any(|request| {
            request
                .message_input_texts("user")
                .iter()
                .any(|text| text.contains("[signal watch]") && text.contains(marker))
        });
        if delivered {
            return Ok(());
        }
        assert!(
            Instant::now() < deadline,
            "monitor output never reached the model as a labelled notification"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_stdout_output_wakes_agent() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // The common case: a stdout line, as fswatch / tail / grep watchers emit.
    assert_monitor_wakes_with("sleep 0.5; echo MONITOR_STDOUT; sleep 2", "MONITOR_STDOUT").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_stderr_output_wakes_agent() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // stderr wakes too: the process output stream the monitor reads carries both.
    assert_monitor_wakes_with(
        "sleep 0.5; echo MONITOR_STDERR 1>&2; sleep 2",
        "MONITOR_STDERR",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_delivers_unterminated_final_line() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // A final line with no trailing newline is held until the watch ends, then
    // delivered, so a line-oriented watcher never loses its last line.
    assert_monitor_wakes_with("sleep 0.5; printf MONITOR_PARTIAL", "MONITOR_PARTIAL").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_flood_guard_auto_stops() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // A watcher that floods output is auto-stopped and the agent is told why,
    // rather than being woken without bound.
    assert_monitor_wakes_with("sleep 0.5; seq 1 50000; sleep 5", "flood guard").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_delivers_exit_notice_when_command_ends() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // When the watched command exits, the agent is woken with an exit notice so
    // it learns the watch ended.
    assert_monitor_wakes_with("sleep 0.5; true", "watcher exited").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_delivers_output_from_before_the_initial_yield() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // A line printed before the spawn's initial yield ends is captured in the
    // seed and still delivered. The delivery loop subscribes to the output
    // stream only after that yield and the broadcast does not replay, so without
    // the seed an immediate first line would be lost.
    assert_monitor_wakes_with("echo MONITOR_IMMEDIATE; sleep 2", "MONITOR_IMMEDIATE").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_self_prunes_from_registry_when_command_exits() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));

    // After the watched command exits, the monitor removes itself from the
    // registry, so a later `action=list` reports no active monitors instead of a
    // dead entry that `stop` could no longer terminate.
    let server = start_mock_server().await;

    let start_args = json!({
        "action": "start",
        "command": "sleep 1; true",
        "description": "short watch",
    })
    .to_string();
    let list_args = json!({ "action": "list" }).to_string();

    let mock = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_response_created("resp-1"),
                ev_function_call("call-start", "monitor", &start_args),
                ev_completed("resp-1"),
            ]),
            sse(vec![
                ev_assistant_message("msg-1", "watching"),
                ev_completed("resp-2"),
            ]),
            // The exit notice wakes this turn; the model lists active monitors.
            sse(vec![
                ev_response_created("resp-3"),
                ev_function_call("call-list", "monitor", &list_args),
                ev_completed("resp-3"),
            ]),
            sse(vec![
                ev_assistant_message("msg-2", "done"),
                ev_completed("resp-4"),
            ]),
        ],
    )
    .await;

    let mut builder = test_codex().with_config(|config| {
        config
            .features
            .enable(Feature::Monitor)
            .expect("enable monitor feature");
    });
    let test = builder.build(&server).await?;

    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "watch briefly".into(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(output) = mock.function_call_output_text("call-list") {
            assert!(
                output.contains("No active monitors."),
                "expected an empty monitor list after the watch ended, got: {output}"
            );
            return Ok(());
        }
        assert!(
            Instant::now() < deadline,
            "the list call never ran after the watched command exited"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_truncates_a_newline_free_flood() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    skip_if_sandbox!(Ok(()));
    // A watcher that streams without newlines must not grow the buffer without
    // bound. The run is truncated and delivered with a marker, instead of
    // accumulating unbounded host memory while the line-count flood guard sleeps.
    assert_monitor_wakes_with(
        "head -c 200000 /dev/zero | tr '\\0' x; sleep 2",
        "line truncated",
    )
    .await
}

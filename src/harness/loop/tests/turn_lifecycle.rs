use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::agent::events::RpcEvent;
use crate::agent::harness::provider::{
    ChatRole, FinishReason, MessageContent, ProviderError, ProviderEvent, ToolCall,
};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::tools::ContentBlock;

/// (finding 3 — ADR 0025's UNBOUNDED `events` channel): a slow /
/// blocked consumer NEVER drops an event (the pre-fix bounded channel
/// dropped the `agent_settled` delivery); the settle watch fires
/// regardless — the driver's settle (and the `send_prompt` /
/// `wait_for_settle` waits it drives) is never lost.
#[tokio::test]
async fn a_slow_events_consumer_never_drops_the_settle() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    for _ in 0..4 {
        events_tx.send(RpcEvent::turn_start).unwrap();
    }
    let (settle_tx, settle_rx) = watch::channel(0u64);
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    // The raw `agent_settled` was NOT dropped (unbounded — the
    // pre-filled events are still ahead of it in the queue; EVERY
    // event is delivered, in order).
    let mut seen = Vec::new();
    while let Ok(ev) = events_rx.try_recv() {
        seen.push(ev);
    }
    assert_eq!(
        seen.first().map(|e| e.kind()),
        Some("turn_start".into()),
        "the pre-filled events are still at the head (nothing was dropped)"
    );
    assert_eq!(
        seen.last().map(|e| e.kind()),
        Some("agent_settled".into()),
        "the settled event was delivered (unbounded — no drop)"
    );
    // ...and the settle watch fired (the reliable signal).
    assert_eq!(*settle_rx.borrow(), 1, "the settle watch fired");
}

/// THE regression this file exists for: the payload the LIVE card is drawn
/// from and the payload a RELOAD reads back out of SQLite are the SAME
/// normalized shape.
///
/// The ask payload leaves Rust at THREE sites — the `interactive-request`
/// event (the live card), the `tool-call` display row that is persisted
/// (`rawInput`, written from `tool_execution_start`), and the
/// `load_history` → `rowToMessages` replay a reload / restart reads. Only
/// ONE of them can be normalized: normalizing the event alone gives a card
/// that looks right live and is blank after a reload (the classic
/// write-path/read-path split), because the replay reads `rawInput` from
/// storage, not the event.
#[tokio::test]
async fn the_persisted_ask_row_and_the_emitted_request_carry_the_same_payload() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    // The shape this app's own model sends: bare-string options, no `id`.
    let raw = json!({
        "questions": [{
            "question": "Which framework?",
            "options": ["Tauri", "Electron"]
        }]
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            name: "ask".to_string(),
            arguments: raw.clone(),
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let sink = Arc::new(AllEventsSink {
        events: Arc::new(StdMutex::new(Vec::new())),
    });
    let captured = sink.events.clone();
    let (mut loop_, db) = build_loop_with_sink(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
        sink,
    );
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
    wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_start { tool_name, .. } if tool_name == "ask"),
    )
    .await
    .expect("the `ask` tool started");

    // The LIVE payload (what the modal renders from).
    let emitted = {
        let mut found = None;
        for _ in 0..100 {
            // The guard is scoped to this block: it is DROPPED before the
            // await below, never held across it.
            found = captured
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .iter()
                .find(|(event, _)| event == "interactive-request")
                .map(|(_, payload)| payload["params"].clone());
            if found.is_some() {
                break;
            }
            tokio::task::yield_now().await;
        }
        found.expect("the interactive-request was emitted")
    };

    // The PERSISTED payload (`load_history` reads this row back on a
    // reload; `rowToMessages` hands its `rawInput` to the transcript).
    let persisted = {
        let mut found = None;
        for _ in 0..100 {
            found = db
                .messages_for("s1")
                .expect("messages_for")
                .iter()
                .find(|r| r.kind == "tool-call" && r.message_key.as_deref() == Some("t1"))
                .and_then(|r| serde_json::from_str::<Value>(&r.payload_json).ok())
                .and_then(|payload| {
                    payload["rawInput"]["questions"]
                        .is_array()
                        .then(|| payload["rawInput"].clone())
                });
            if found.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        found.expect("the `ask` tool-call row was persisted with the questions")
    };

    // THE invariant: byte-for-byte the same normalized payload.
    assert_eq!(
        persisted, emitted,
        "the persisted/replayed payload must EQUAL the emitted payload"
    );
    assert_eq!(
        persisted["questions"][0]["options"],
        json!([{ "label": "Tauri" }, { "label": "Electron" }]),
        "the reloaded card must not be blank: the bare strings became `{{label}}` objects, got {persisted}"
    );
    assert_eq!(
        persisted["questions"][0]["id"],
        json!("q0"),
        "the reloaded card's question has the same positional id the live card used"
    );
    // The raw model arguments are NOT what gets stored/emitted.
    assert_ne!(
        persisted, raw,
        "the raw tool arguments are no longer what leaves Rust"
    );

    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let _ = tokio::time::timeout(SETTLE, task).await;
}

/// (finding 8a) A turn with 2 tool calls: a cancel after the first
/// call (the `ask` blocks until the cancel) → the second is NOT
/// executed (a cancelled tool result; the turn settles).
#[tokio::test]
async fn a_cancel_stops_the_remaining_tool_calls_in_a_batch() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "ask".to_string(),
                arguments: json!({
                    "questions": [{ "id": "q1", "question": "Which?", "options": [{ "label": "A" }] }]
                }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "read".to_string(),
                arguments: json!({ "path": "/nonexistent-file" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
    // The first tool starts (the `ask` blocks until the cancel).
    wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_start { tool_name, .. } if tool_name == "ask"),
    )
    .await
    .expect("the `ask` tool started");
    tokio::time::sleep(Duration::from_millis(100)).await;
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn settled after the cancel (it did not wait out the `ask`)");
    // The `read` was NOT executed — a cancelled tool result.
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t2"),
    )
    .await
    .expect("the `read`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert_eq!(
        result["content"][0]["text"], "cancelled",
        "the skipped call is NOT executed"
    );
    assert!(is_error, "the skipped call is an error result");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// (finding 8b) A model call that never finishes: a cancel settles
/// the turn (the loop does NOT wait out the model — pre-fix the
/// cancel only took effect when the call happened to finish, and a
/// stalled `complete` / `stream.next()` blocks indefinitely).
#[tokio::test]
async fn a_cancel_stops_an_in_flight_model_call() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
    tokio::time::sleep(Duration::from_millis(200)).await; // the call is in flight
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the cancel settled the turn (it did not wait out the model)");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled after the cancel");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the turn did not loop again"
    );
}

/// (finding 1) A prompt that is SKIPPED (a Stop is pending when
/// `handle_prompt` runs it) must settle its `pending_turn` resolver:
/// the loop emits `agent_settled` (the driver resolves the slot
/// `Cancelled` via the `cancel_requested` flag). Pre-fix the early
/// return emitted nothing, so the skipped prompt's `send_prompt`
/// caller hung until a `close_session`.
#[tokio::test]
async fn a_skipped_prompt_settles_the_pending_turn() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (settle_tx, settle_rx) = watch::channel(0u64);
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // A Stop is pending (the turn token is cancelled) BEFORE the prompt
    // runs: `handle_prompt` must settle the skipped prompt (not just
    // drop it).
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    loop_.handle_prompt(&text_prompt("hello")).await;
    // The skipped prompt's settle (pre-fix this was missing → the
    // `send_prompt` caller hung).
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled after the skipped prompt");
    assert_eq!(*settle_rx.borrow(), 1, "the settle watch fired");
    // The token is RE-ARMED (a later prompt starts a fresh turn).
    assert!(
        !turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_cancelled(),
        "the token was re-armed"
    );
}

/// (finding 1) A prompt that is DRAINED (a Stop lands while it is
/// still queued — `run()`'s turn-cancel arm) must settle its
/// `pending_turn` resolver: the loop emits `agent_settled` (the driver
/// resolves the slot `Cancelled`). Pre-fix the drain arm just dropped
/// the items, so the queued prompt's `send_prompt` caller hung.
#[tokio::test]
async fn a_drained_queued_prompt_settles_the_pending_turn() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (settle_tx, settle_rx) = watch::channel(0u64);
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
    let loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Queue a prompt (its `pending_turn` resolver is conceptually held by
    // the caller — here the loop just has to settle it).
    loop_.send_prompt("hello", &[]);
    // A Stop (the turn token) arrives — the queued prompt is drained (or
    // dequeued + skipped; either way the loop must settle it). Pre-fix the
    // drain arm dropped the item and `agent_settled` never fired.
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let _task = tokio::spawn(async move { loop_.run().await });
    // The drained prompt's settle (pre-fix missing → the `send_prompt`
    // caller hung).
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled after the drained prompt");
    assert_eq!(*settle_rx.borrow(), 1, "the settle watch fired");
    // The spawned loop task runs until the test's runtime drops it
    // (it is not awaited — a turn-cancelled loop keeps running).
}

/// (finding 7) A model call that FAILS (a non-retryable `Fatal` error)
/// ends the turn with `turn_end` + `agent_settled` (the timeline closes
/// cleanly — pre-fix the error exits emitted a bare `agent_settled` with
/// no `turn_end`, so a failed turn's timeline did not close cleanly).
#[tokio::test]
async fn a_failed_model_call_settles_with_turn_end_and_agent_settled() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let provider = FailingProvider {
        error: ProviderError::Fatal("the model call failed".to_string()),
    };
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    // The failed turn settles with `turn_end` (the timeline closes) +
    // `agent_settled` (the reliable settle signal) — `turn_end` FIRST.
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::turn_end { .. })
    })
    .await
    .expect("turn_end after the failed model call (finding 7)");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled after the failed model call");
}

/// (finding 8b, round 2) A `sudo_exec` blocking on its confirm
/// sub-prompt + a TURN cancel → the flow returns a cancelled result
/// PROMPTLY (the turn token is raced — pre-fix the flow selected
/// only on the SESSION teardown token, so a Stop waited for an answer
/// that might never come, and a confirm answered AFTER
/// the Stop still EXECUTED the elevated command). The command is
/// NEVER run.
#[tokio::test]
async fn a_cancel_stops_a_sudo_flow_blocking_on_its_confirm_prompt() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            name: "sudo_exec".to_string(),
            arguments: json!({ "command": "echo never-runs", "reason": "test" }),
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // A recording runner: if the elevated command ever runs, the
    // test fails.
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    loop_.sudo.runner = Arc::new(RecordingSudoRunner { ran: ran.clone() });
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
    // The `sudo_exec` is now blocking on its confirm sub-prompt
    // (never answered in the test).
    tokio::time::sleep(Duration::from_millis(500)).await;
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect(
            "the turn settled promptly (the Stop won the race — the prompt has no deadline to wait out)",
        );
    assert!(
        !ran.load(Ordering::SeqCst),
        "the elevated command was NEVER executed"
    );
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the sudo tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .starts_with("cancelled"),
        "the cancelled flow is a cancelled tool result (the finding 3 honest text)"
    );
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("kills the started command"),
        "the sudo cancel result is the honest kill-behavior text (finding 3), not the generic `cancelled`"
    );
    assert!(is_error, "the cancelled flow is an error result");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// (the nit) A mid-stream `Retryable` error: the retry is the SAME
/// assistant message — ONE `message_start` (pre-fix a retried model
/// call emitted a `message_start` per attempt — multiple starts for
/// one message).
#[tokio::test]
async fn a_retried_model_call_emits_one_message_start() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("part".to_string()),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let mut events = Vec::new();
    while let Ok(ev) = events_rx.try_recv() {
        events.push(ev);
    }
    let starts = events
        .iter()
        .filter(|e| matches!(e, RpcEvent::message_start { .. }))
        .count();
    assert_eq!(
        starts, 1,
        "one message_start for the retried assistant message"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "the retry happened"
    );
    assert!(
        events.iter().any(|e| matches!(e, RpcEvent::agent_settled)),
        "the turn settled"
    );
}

/// (the nit) A retryable error arms a 30 s backoff: a cancel settles
/// the turn FAST (the backoff sleep races the cancel — pre-fix a
/// cancelled turn waited out up to 8 s of backoff).
#[tokio::test]
async fn a_cancel_during_the_retry_backoff_settles_fast() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![ProviderEvent::Error(
        ProviderError::Retryable("transient".to_string()),
    )])]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(3, Duration::from_secs(30)),
    );
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::auto_retry_start { .. })
    })
    .await
    .expect("the retry backoff was armed (30 s)");
    tokio::time::sleep(Duration::from_millis(100)).await;
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the cancel settled the turn (the 30 s backoff was cut short)");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled after the cancel");
}

/// (ADR 0017) `prepend_system` PERSISTS the leading system message at
/// seq 0 (the transcript record of the session's system prompt — a
/// resume replays it verbatim via `load_transcript`). Asserted on the
/// STORE (`load_messages` returns the rows in seq order — the seq-0
/// row is `loaded[0]`).
#[tokio::test]
async fn prepend_system_persists_at_seq_zero() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.prepend_system("the prompt".to_string());
    let loaded = loop_.store.load_messages("s1").unwrap();
    assert_eq!(loaded.len(), 1, "the system message was persisted at seq 0");
    let m = &loaded[0];
    assert!(
        matches!(m.role, ChatRole::System),
        "the persisted row is the system message, got {:?}",
        m.role
    );
    assert_eq!(
        m.content,
        MessageContent::Text("the prompt".to_string()),
        "the persisted content is the prompt text"
    );
}

/// (ADR 0017) a resume REPLAYS the stored system message verbatim
/// (the `load_transcript` of the stored rows restores it
/// byte-identically — same role, same content JSON).
#[tokio::test]
async fn resume_replays_system_message_verbatim() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.prepend_system("the prompt".to_string());
    let loaded = loop_.store.load_messages("s1").unwrap();
    loop_.load_transcript(loaded.clone());
    assert_eq!(
        loop_.messages.len(),
        1,
        "the replayed transcript has the stored row"
    );
    assert!(
        matches!(loop_.messages[0].role, ChatRole::System),
        "the replayed message is the system message"
    );
    assert_eq!(
        loop_.messages[0].content,
        MessageContent::Text("the prompt".to_string()),
        "the replayed content is the prompt text"
    );
    assert_eq!(
        serde_json::to_string(&loop_.messages[0]).unwrap(),
        serde_json::to_string(&loaded[0]).unwrap(),
        "the replayed message is byte-identical to the stored row"
    );
}

#[test]
fn result_text_takes_the_first_text_block() {
    let r = ToolResult {
        content: vec![
            ContentBlock::Image {
                image: crate::agent::tools::ImageRef {
                    data: "a".into(),
                    mime_type: "image/png".into(),
                },
            },
            ContentBlock::Text {
                text: "the text".to_string(),
            },
        ],
        details: None,
        is_error: false,
    };
    assert_eq!(result_text(&r), "the text");
}

/// A tool result's IMAGE blocks reach the model: the `tool` message
/// the turn pushes is the result's FULL `Blocks` (a `read` on an
/// image file returns an image-only result — flattening it to the
/// first text block would drop the image the model is meant to see;
/// the provider's `to_wire` then sends it as an `image_url`
/// data-URI on the request wire).
#[tokio::test]
async fn a_tool_result_image_reaches_the_model_transcript() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "read".to_string(),
                arguments: json!({ "path": "img.png" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // A 1x1 transparent PNG (the `read` keys on the extension —
    // the bytes are base64-encoded, not decoded).
    std::fs::write(
        loop_.space_cwd.join("img.png"),
        [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
            0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
            0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78,
            0x9C, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xD4, 0x00,
            0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
        ],
    )
    .expect("the test png writes");
    // The turn completes on its own (the scripted provider ends it —
    // no cancel), so a plain await (the `loop_` stays in scope for
    // the `messages` assertion below).
    loop_.handle_prompt(&text_prompt("read the image")).await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
    // The `tool` message is the result's FULL `Blocks` (the image
    // survived — NOT flattened to the first text block, which would
    // be empty for an image-only result).
    let tool_msg = loop_
        .messages
        .iter()
        .find(|m| m.role == ChatRole::Tool)
        .expect("a tool message was pushed");
    match &tool_msg.content {
        MessageContent::Blocks(blocks) => assert!(
            blocks
                .iter()
                .any(|b| matches!(b, ContentBlock::Image { .. })),
            "the image block survived into the model transcript (got {blocks:?})"
        ),
        MessageContent::Text(t) => {
            panic!("the tool message was flattened to text ({t:?}) — the image is lost")
        }
    }
}

/// A prompt WITH images pushes a `Blocks` user message (text FIRST,
/// then the image blocks — the model sees the image on the request
/// wire via the provider's `to_wire`; an empty text + images is an
/// image-only `Blocks`, NO empty text block).
#[tokio::test]
async fn a_prompt_with_images_pushes_a_blocks_user_message() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("ok".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("ok2".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // A prompt with text + an image → `Blocks` (text first, image
    // after).
    loop_
        .handle_prompt(&Prompt {
            text: "describe".to_string(),
            images: vec![crate::agent::tools::ImageRef {
                data: "BASE64DATA".to_string(),
                mime_type: "image/png".to_string(),
            }],
        })
        .await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
    let first = loop_.messages.first().expect("a user message was pushed");
    match &first.content {
        MessageContent::Blocks(blocks) => {
            assert_eq!(blocks.len(), 2, "a text block + an image block");
            assert!(
                matches!(&blocks[0], ContentBlock::Text { text } if text == "describe"),
                "the text block is FIRST (got {blocks:?})"
            );
            assert!(
                matches!(&blocks[1], ContentBlock::Image { image } if image.mime_type == "image/png" && image.data == "BASE64DATA"),
                "the image block follows (got {blocks:?})"
            );
        }
        other => panic!("a prompt with images is a Blocks user message, got {other:?}"),
    }
    // An EMPTY text + an image → an image-only `Blocks` (no empty
    // text block).
    loop_
        .handle_prompt(&Prompt {
            text: String::new(),
            images: vec![crate::agent::tools::ImageRef {
                data: "D".to_string(),
                mime_type: "image/gif".to_string(),
            }],
        })
        .await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
    let second = loop_
        .messages
        .get(2)
        .expect("the second user message was pushed");
    match &second.content {
        MessageContent::Blocks(blocks) => {
            assert_eq!(blocks.len(), 1, "an empty text adds no text block");
            assert!(matches!(&blocks[0], ContentBlock::Image { .. }));
        }
        other => panic!("an image-only prompt is a Blocks user message, got {other:?}"),
    }
}

#[tokio::test]
async fn model_requests_carry_the_session_id_across_turns_and_tool_calls() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let recorded = Arc::new(StdMutex::new(Vec::new()));
    let provider = SessionRecordingProvider::with_scripts(
        recorded.clone(),
        vec![
            // Turn 1, call 1: tool call
            vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "ls".to_string(),
                    arguments: json!({ "path": "." }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ],
            // Turn 1, call 2: finish
            vec![
                ProviderEvent::TextDelta("done with ls".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            // Turn 2, call 3: simple text response
            vec![
                ProviderEvent::TextDelta("turn 2".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
        ],
    );
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );

    // Turn 1: has a tool call and follow-up completion
    loop_.handle_prompt(&text_prompt("run ls")).await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("turn 1 settled");

    // Turn 2: another conversational prompt
    loop_.handle_prompt(&text_prompt("second turn")).await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("turn 2 settled");

    let sessions = recorded.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert_eq!(
        sessions.len(),
        3,
        "expected 3 model calls across the two turns"
    );
    for (i, session_id) in sessions.iter().enumerate() {
        assert_eq!(
            session_id,
            &Some("s1".to_string()),
            "call {i} should carry Some(\"s1\")",
        );
    }
}

/// (ADR 0018) A session with a temp `mcp.json` (the fake stdio server)
/// where the model emits an `mcp` tool call → the tool result
/// round-trips into the transcript (a `tool_execution_end` with the
/// echoed text).
#[tokio::test]
async fn an_mcp_tool_call_round_trips_into_the_transcript() {
    // A temp dir with a `.pi/mcp.json` (the fake stdio server).
    let dir = std::env::temp_dir().join(format!("mcp-loop-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(dir.join(".pi")).unwrap();
    let bin = std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/target/debug/fake_mcp_stdio"
    ));
    std::fs::write(
        dir.join(".pi/mcp.json"),
        serde_json::json!({
            "mcpServers": { "a": { "command": bin.display().to_string() } }
        })
        .to_string(),
    )
    .unwrap();
    // The model emits an `mcp` tool call (a `echo`), then a final
    // response.
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "m1".to_string(),
                name: "mcp".to_string(),
                arguments: json!({ "tool": "echo", "args": { "text": "hi" } }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Swap in the temp-config MCP manager (the `build_loop` default reads
    // the real home — this one reads the temp `mcp.json`).
    loop_.mcp = McpManager::new(dir.clone(), dir.clone(), None);
    loop_.handle_prompt(&text_prompt("call the mcp tool")).await;
    // The `mcp` tool result round-tripped (the `echo` → "hi").
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "m1"),
    )
    .await
    .expect("the `mcp` tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(!is_error, "the `mcp` call succeeded: {result:?}");
    assert_eq!(result["content"][0]["text"], "hi");
}

/// (ADR 0018) A Stop mid-`mcp`-call cancels (the in-flight request is
/// cancelled — the tool result is `cancelled`; the turn settles, the
/// stdio child is killed via `kill_on_drop`).
#[tokio::test]
async fn a_stop_mid_mcp_call_cancels() {
    let dir = std::env::temp_dir().join(format!("mcp-loop-stop-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(dir.join(".pi")).unwrap();
    let bin = std::path::Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/target/debug/fake_mcp_stdio"
    ));
    std::fs::write(
        dir.join(".pi/mcp.json"),
        serde_json::json!({
            "mcpServers": { "a": { "command": bin.display().to_string() } }
        })
        .to_string(),
    )
    .unwrap();
    // The model emits an `mcp` tool call (a `hang` — never answers), then
    // a final response.
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "m1".to_string(),
                name: "mcp".to_string(),
                arguments: json!({ "tool": "hang", "args": {} }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.mcp = McpManager::new(dir.clone(), dir.clone(), None);
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("call mcp")).await });
    // The `mcp` tool starts (the `hang` never answers). This wait covers
    // SPAWNING the MCP server process + handshake inside a spawned task,
    // so it rides `SPAWN_MS` (see the const's why-comment: 5 s lost to
    // run 38750380996, 15 s lost to 38756887012, both while the same
    // commit passed elsewhere). The assertion this protects is what
    // happens AFTER the start (the cancel), so a spawn that merely takes
    // its time is not what the test pins.
    wait_for_event(
        &mut events_rx,
        SPAWN_MS,
        |e| matches!(e, RpcEvent::tool_execution_start { tool_name, .. } if tool_name == "mcp"),
    )
    .await
    .expect("the `mcp` tool started");
    tokio::time::sleep(Duration::from_millis(100)).await;
    // A Stop (the TURN cancel) mid-`mcp`-call.
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    match tokio::time::timeout(SETTLE, task).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("the turn task failed: {e}"),
        Err(_) => panic!("the turn did not settle after the Stop (it waited out the `hang`)"),
    }
    // The tool result is `cancelled` (the in-flight request was cancelled).
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "m1"),
    )
    .await
    .expect("the `mcp` tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        is_error,
        "a cancelled `mcp` call is an error result: {result:?}"
    );
    assert_eq!(result["content"][0]["text"], "cancelled");
}

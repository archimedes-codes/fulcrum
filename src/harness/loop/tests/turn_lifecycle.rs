use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::events::{EventSink, RpcEvent};
use crate::harness::catalog::Model;
use crate::harness::dispatch::SubagentDispatcher;
use crate::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ProviderError, ProviderEvent, ToolCall,
    Usage,
};
use crate::harness::retry::RetryPolicy;
use crate::subagent::{AgentNameResolution, LaunchConfig, SubagentOutcome};
use crate::tools::ContentBlock;

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
    let (_space, mut loop_) = build_loop(
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
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let sink = Arc::new(AllEventsSink {
        events: Arc::new(StdMutex::new(Vec::new())),
    });
    let captured = sink.events.clone();
    let (_space, mut loop_, db) = build_loop_with_sink(
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
    //
    // (T5/S5b) RE-POINTED at `MemoryStore::display_rows()`. This used to read
    // `db.messages_for("s1")` off a real `Db`, and the S5b `MemoryStore` swap
    // left the call behind — the fifteenth compile error in this cluster, hidden
    // behind the fourteen path errors because name resolution failed before type
    // checking ran. The RE-POINT is honest here: the invariant this test exists to
    // hold is `persisted == emitted`, i.e. what the loop HANDED the store seam,
    // and `display_rows()` answers that exactly. What the double does not model —
    // the `(session_id, kind, message_key)` UPSERT collapse — is not asserted
    // anywhere in this test, and is pinned on the desktop side
    // (`src-tauri/tests/harness_store.rs`, `src-tauri/src/agent/harness/store.rs`).
    let persisted = {
        let mut found = None;
        for _ in 0..100 {
            found = db
                .display_rows()
                .into_iter()
                .find(|(_, kind, key, _)| kind == "tool-call" && key == "t1")
                .and_then(|(_, _, _, payload)| serde_json::from_str::<Value>(&payload).ok())
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
                defect: None,
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "read".to_string(),
                arguments: json!({ "path": "/nonexistent-file" }),
                defect: None,
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop(
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
    let (_space, mut loop_) = build_loop(
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
    let (_space, mut loop_) = build_loop(
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
    let (_space, loop_) = build_loop(
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
    let (_space, mut loop_) = build_loop(
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
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let (_space, mut loop_) = build_loop(
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
    let (_space, mut loop_) = build_loop(
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
    let (_space, mut loop_) = build_loop(
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

/// (issue #64) THE regression this exists for: a `Done(Stop)` that carried
/// no text and no tool call is NOT a turn — it is the signature of a model
/// that wrote its tool call as literal `<invoke name="…">` text INTO the
/// reasoning channel, so the API answers `content: ""` / `tool_calls: null`
/// and the stream looks like a clean stop. `thinking` is streamed to the UI
/// but never persisted, so the intended tool call is invisible in the DB:
/// pre-fix the loop accepted the attempt and persisted the 33-byte row
/// `{"role":"assistant","content":"","tool_calls":[]}` — nothing ran, nothing
/// was reported, and the user had to re-prompt (13 of these in 23 519
/// assistant rows, ALL of them followed by a `user` row). It now reuses the
/// mid-stream `Retryable` recovery: retract the abandoned attempt, spend the
/// SHARED retry budget, re-run the turn, persist nothing.
#[tokio::test]
async fn an_empty_stop_turn_is_retried_and_never_persisted() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // The production shape: reasoning text (the leaked `<invoke>`), no
        // text, no tool call, a clean `stop`.
        Some(vec![
            ProviderEvent::ThinkingDelta(
                "I will read the file now. <invoke name=\"read\">".to_string(),
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        // The retry, which answers properly.
        Some(vec![
            ProviderEvent::TextDelta("here is the file".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("read the file")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    // The retry happened, on the SHARED budget (`attempt` is the RETRY's own
    // number — the second model call — exactly as the mid-stream branch emits).
    let start = events
        .iter()
        .find(|e| matches!(e, RpcEvent::auto_retry_start { .. }))
        .expect("the empty stop retried");
    assert!(
        matches!(
            start,
            RpcEvent::auto_retry_start {
                attempt: 2,
                max_attempts: 5,
                error_message,
                ..
            } if error_message.contains("empty")
        ),
        "the retry names the condition in user terms, got {start:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the empty attempt + the retry"
    );
    // The abandoned attempt streamed thinking, so it is RETRACTED (the same
    // gate the mid-stream retry uses) — otherwise the leaked reasoning welds
    // onto the retry's reply in every same-`messageId` consumer.
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_retracted").count(),
        1,
        "the thinking-only attempt was retracted, got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_end").count(),
        1,
        "ONE message_end — the empty attempt never announced a message, got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_start").count(),
        1,
        "ONE message_start — the retry reuses the announced message, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_settled"),
        "the turn settled, got {kinds:?}"
    );
    // The abandoned attempt is NOT a successful call, so the retry it triggered
    // is never reported `success: true` before the retry has even answered: the
    // ONE `auto_retry_end` is the real success, and it comes after the retry's
    // `message_end` would have been — i.e. the detection sits BEFORE
    // `note_success`.
    let ends: Vec<&RpcEvent> = events
        .iter()
        .filter(|e| matches!(e, RpcEvent::auto_retry_end { .. }))
        .collect();
    assert_eq!(
        ends.len(),
        1,
        "ONE auto_retry_end — the abandoned attempt reported no success, got {kinds:?}"
    );
    assert!(
        matches!(
            ends[0],
            RpcEvent::auto_retry_end {
                success: true,
                final_error: None,
                ..
            }
        ),
        "the single auto_retry_end is the retry's real success, got {:?}",
        ends[0]
    );

    // THE bug: the 33-byte empty assistant row must not exist — neither in
    // the model transcript nor in the store a reload reads back.
    assert!(
        !loop_.messages.iter().any(|m| {
            matches!(&m.content, MessageContent::Text(t) if t.is_empty())
                && matches!(m.role, ChatRole::Assistant)
        }),
        "the empty attempt was never pushed into the transcript, got {:?}",
        loop_.messages
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    let assistants: Vec<&ChatMessage> = loaded
        .iter()
        .filter(|m| matches!(m.role, ChatRole::Assistant))
        .collect();
    assert_eq!(
        assistants.len(),
        1,
        "exactly ONE assistant row (the retry's), got {loaded:?}"
    );
    assert_eq!(
        assistants[0].content,
        MessageContent::Text("here is the file".to_string()),
        "the only persisted assistant row is the retry's reply"
    );
}

/// The same path with NOTHING streamed at all (`Done(Stop)` and no deltas):
/// the turn is still empty, so it is still retried — and since nothing
/// streamed there is nothing to retract (ADR 0048's silence rule).
#[tokio::test]
async fn a_stop_that_streamed_nothing_at_all_is_retried() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![ProviderEvent::Done(FinishReason::Stop)]),
        Some(vec![
            ProviderEvent::TextDelta("ok".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "an empty turn with nothing streamed still retried, got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the empty attempt + the retry"
    );
    assert!(
        !kinds.iter().any(|k| k == "message_retracted"),
        "nothing streamed, so nothing is retracted, got {kinds:?}"
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert_eq!(
        loaded
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Assistant))
            .count(),
        1,
        "no empty assistant row survived the retry, got {loaded:?}"
    );
}

/// The budget spent: an empty turn settles as a VISIBLE failure
/// (`auto_retry_end { success: false }` + the error settle) and still
/// persists NO empty row. A loud failure is the correct outcome — the DB's
/// silent 33-byte rows are the bug.
#[tokio::test]
async fn an_empty_stop_with_an_exhausted_budget_settles_as_an_error() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ThinkingDelta("thinking <invoke name=\"read\">".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // ONE attempt: the first call spends the budget, so the empty turn
        // has nothing left to retry with.
        RetryPolicy::new_with(1, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    let end = events
        .iter()
        .find(|e| matches!(e, RpcEvent::auto_retry_end { .. }))
        .expect("the exhausted budget reports the failure");
    assert!(
        matches!(
            end,
            RpcEvent::auto_retry_end {
                success: false,
                final_error: Some(_),
                ..
            }
        ),
        "the turn failed LOUDLY, got {end:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start"),
        "no budget, so no retry was announced, got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a turn never makes more than `max_attempts` model calls"
    );
    assert!(
        !kinds.iter().any(|k| k == "agent_end"),
        "an exhausted empty turn is not a clean stop, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_settled"),
        "the failed turn still settles, got {kinds:?}"
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        !loaded.iter().any(|m| matches!(m.role, ChatRole::Assistant)),
        "a failed turn persists NO assistant row, got {loaded:?}"
    );
}

/// The guard rail for the detection condition: a reply WITH text and no tool
/// call is the normal end of a turn — one `message_end`, one persisted row,
/// no retry.
#[tokio::test]
async fn a_reply_with_text_and_no_tool_call_settles_normally() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("a normal reply".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a normal reply costs one model call"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_end").count(),
        1,
        "the reply ended normally, got {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start"),
        "a non-empty reply is never retried, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_end"),
        "the turn ended cleanly, got {kinds:?}"
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert_eq!(
        loaded
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Assistant))
            .count(),
        1,
        "the reply was persisted once, got {loaded:?}"
    );
}

/// The OTHER guard rail, and the case the condition must NOT catch: empty
/// text with a tool call is a perfectly legitimate turn (`finish` is
/// `ToolCalls`, and the tool call is what the turn is made of). Detection
/// keyed on `Some(Stop)` + no tool calls leaves it untouched.
#[tokio::test]
async fn an_empty_text_tool_calls_turn_is_not_treated_as_empty() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "ls".to_string(),
                arguments: json!({ "path": "." }),
                defect: None,
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done with ls".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("list the dir")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the tool-call turn ran, then the model answered — no retry"
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start"),
        "an empty-text `ToolCalls` turn is legitimate, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_end"),
        "the turn ended cleanly, got {kinds:?}"
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        loaded
            .iter()
            .any(|m| matches!(m.role, ChatRole::Assistant) && m.tool_calls.is_some()),
        "the tool-call row was persisted with its calls, got {loaded:?}"
    );
}

/// The empty turn spends the SAME budget the retry ladder owns — there is no
/// second counter, so a turn never makes more than `max_attempts` model calls
/// however many distinct conditions fire inside it. Here a mid-stream
/// `Retryable` error takes the first retry of a two-attempt budget, so the
/// empty turn that follows has nothing left: it settles as a failure after
/// exactly TWO model calls.
#[tokio::test]
async fn an_empty_turn_shares_the_retry_budget_with_the_ladder() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("part".to_string()),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        Some(vec![
            ProviderEvent::ThinkingDelta("reasoning only".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        // The reply this turn can never reach — the budget is spent.
        Some(vec![
            ProviderEvent::TextDelta("unreachable".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(2, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        kinds.iter().filter(|k| *k == "auto_retry_start").count(),
        1,
        "the ladder's retry consumed the budget, so the empty turn retried zero times, \
         got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "a turn never exceeds `max_attempts` model calls, no matter which conditions fire"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            RpcEvent::auto_retry_end {
                success: false,
                final_error: Some(_),
                ..
            }
        )),
        "the exhausted empty turn failed LOUDLY, got {kinds:?}"
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        !loaded.iter().any(|m| matches!(m.role, ChatRole::Assistant)),
        "no empty assistant row was persisted for the abandoned turn, got {loaded:?}"
    );
}

/// An abandoned attempt is not a successful call: the detection sits BEFORE
/// `note_success` for the same reason the mid-stream arm does, so a turn that
/// retries twice never announces `auto_retry_end { success: true }` between the
/// two `auto_retry_start`s (a "success" for an attempt that produced nothing).
#[tokio::test]
async fn an_empty_turn_after_a_ladder_retry_reports_no_success() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("part".to_string()),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        Some(vec![
            ProviderEvent::ThinkingDelta("reasoning only".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("finally".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the dying call + the empty turn + the reply that settled"
    );
    let starts: Vec<usize> = kinds
        .iter()
        .enumerate()
        .filter(|(_, k)| *k == "auto_retry_start")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        starts.len(),
        2,
        "both conditions retried, on the shared budget, got {kinds:?}"
    );
    let end = kinds.iter().position(|k| *k == "auto_retry_end");
    assert!(
        matches!(end, Some(e) if e > starts[1]),
        "no success is reported before the SECOND retry even starts, got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "auto_retry_end").count(),
        1,
        "ONE auto_retry_end — the real success, got {kinds:?}"
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    let assistants: Vec<&ChatMessage> = loaded
        .iter()
        .filter(|m| matches!(m.role, ChatRole::Assistant))
        .collect();
    assert_eq!(
        assistants.len(),
        1,
        "only the settling reply was persisted, got {loaded:?}"
    );
    assert_eq!(
        assistants[0].content,
        MessageContent::Text("finally".to_string()),
        "the persisted row is the real reply"
    );
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
    let (_space, mut loop_, _db) = build_loop_with_db(
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
    let (_space, mut loop_, _db) = build_loop_with_db(
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
                image: crate::tools::ImageRef {
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
                defect: None,
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop(
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
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // A prompt with text + an image → `Blocks` (text first, image
    // after).
    loop_
        .handle_prompt(&PromptTurn {
            text: "describe".to_string(),
            images: vec![crate::tools::ImageRef {
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
        .handle_prompt(&PromptTurn {
            text: String::new(),
            images: vec![crate::tools::ImageRef {
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
                    defect: None,
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
    let (_space, mut loop_) = build_loop(
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
    //
    // #96 ownership: `dir` is a TEST-SCOPE local declared BEFORE the loop,
    // and nothing here is spawned — the last use of the path is inside
    // `handle_prompt(..).await`, which completes in this body, so the guard
    // drops strictly after it. `McpManager` gets a `PathBuf` COPY, never the
    // guard (`ScratchDir` is deliberately not `Clone`: a second owner would
    // `remove_dir_all` the same path).
    let dir = crate::test_support::ScratchDir::in_env_temp("mcp-loop-test-", "roundtrip");
    // Stays: this makes a SUBDIR inside the guard's dir, not the root.
    std::fs::create_dir_all(dir.join(".pi")).unwrap();
    // The workspace root's `target/debug`, through the crate's ONE resolver.
    // (T5/S5b) This file moved from `src-tauri/` to `crates/fulcrum-core`, and its
    // local `CARGO_MANIFEST_DIR.parent()` arithmetic came with it — which is
    // exactly the trap the T5 plan's MAJOR finding #3 names: one `.parent()` was
    // the repo root while the file lived in `src-tauri/`, and is `…/crates` from
    // `crates/fulcrum-core`, so the path named `crates/target/debug/fake_mcp_stdio`
    // and BOTH mcp tests here failed with ENOENT (`spawn failed: No such file or
    // directory`) while everything still COMPILED. `test_support::target_debug_dir`
    // walks up TWO levels and `childproc::target_debug_dir_is_where_cargo_actually_builds`
    // pins it against `current_exe()`, so it cannot go green on a wrong level count.
    let bin = crate::test_support::target_debug_dir().join("fake_mcp_stdio_core");
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
                defect: None,
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Swap in the temp-config MCP manager (the `build_loop` default reads
    // the real home — this one reads the temp `mcp.json`).
    loop_.mcp = Arc::new(McpManager::new(dir.to_path_buf(), dir.to_path_buf(), None));
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
    // #96 ownership — THE SPAWN CRUX. `dir` is a TEST-SCOPE local and is
    // NEVER moved into the `tokio::spawn` below: only `loop_` crosses, and it
    // carries a `PathBuf` COPY (`to_path_buf()`), never the guard. The task
    // handle is awaited (`timeout(SETTLE, task).await`) before this body
    // ends — and the `Err(_)` arm PANICS, so even the timeout path unwinds
    // the body while the runtime is still dropping the task's future. Hence
    // the guard outlives the task that holds the path.
    //
    // The ordering that actually matters is the CHILD PROCESS: the stdio MCP
    // server has `current_dir` inside this dir, and it dies via
    // `kill_on_drop` when its `StdioClient` drops — which happens when
    // `McpManager` drops, i.e. when `loop_` drops INSIDE the spawned task,
    // at or before that `timeout(SETTLE, task).await`. So the child is gone
    // before `dir` drops. THAT AWAIT IS LOAD-BEARING FOR THIS GUARD: if it
    // were ever removed, the dir could be removed under a live child (the
    // `a_drained_queued_prompt_settles_the_pending_turn` limit), so keep it.
    let dir = crate::test_support::ScratchDir::in_env_temp("mcp-loop-stop-", "cancel");
    // Stays: a SUBDIR inside the guard's dir, not the root.
    std::fs::create_dir_all(dir.join(".pi")).unwrap();
    // The workspace root's `target/debug`, through the crate's ONE resolver.
    // (T5/S5b) This file moved from `src-tauri/` to `crates/fulcrum-core`, and its
    // local `CARGO_MANIFEST_DIR.parent()` arithmetic came with it — which is
    // exactly the trap the T5 plan's MAJOR finding #3 names: one `.parent()` was
    // the repo root while the file lived in `src-tauri/`, and is `…/crates` from
    // `crates/fulcrum-core`, so the path named `crates/target/debug/fake_mcp_stdio`
    // and BOTH mcp tests here failed with ENOENT (`spawn failed: No such file or
    // directory`) while everything still COMPILED. `test_support::target_debug_dir`
    // walks up TWO levels and `childproc::target_debug_dir_is_where_cargo_actually_builds`
    // pins it against `current_exe()`, so it cannot go green on a wrong level count.
    let bin = crate::test_support::target_debug_dir().join("fake_mcp_stdio_core");
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
                defect: None,
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
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.mcp = Arc::new(McpManager::new(dir.to_path_buf(), dir.to_path_buf(), None));
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

/// (issue #122) The upstream sanitisation placeholder —
/// `[System: Empty message content sanitised to satisfy protocol]`, injected by
/// LiteLLM's `_sanitize_empty_text_content` when empty string content passes
/// through it — must never reach the UI, the transcript, or the store.
///
/// The scripted provider plays the two-turn production shape: a placeholder-only
/// reply, then the retry that answers properly. After the filter, the
/// placeholder-only attempt accumulates NOTHING into `acc.text`, so it is the
/// genuine empty-turn condition of issue #64 (`Stop` + no text + no tool call)
/// and is retried on the shared budget — persisting nothing.
#[tokio::test]
async fn a_placeholder_stream_is_never_emitted_or_persisted() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    // The placeholder-only attempt, then the retry that answers properly
    // (issue #64: after the filter `acc.text` is empty, so `handle_empty_turn`
    // retries on the shared budget — the SAME shape the empty-turn tests use).
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta(
                "[System: Empty message content sanitised to satisfy protocol]".to_string(),
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("ok".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hi")).await;
    let events = drain_events(&mut events_rx);
    // The placeholder must never be EMITTED as a `text_delta` (the frontend
    // renders exactly these deltas, so "never emitted" is "never in the UI").
    let leaked: Vec<&RpcEvent> = events
        .iter()
        .filter(|e| {
            matches!(e, RpcEvent::message_update { assistant_message_event, .. }
                if assistant_message_event["type"] == "text_delta"
                    && assistant_message_event["delta"]
                        .as_str()
                        .is_some_and(|d| d.contains("Empty message")))
        })
        .collect();
    assert!(
        leaked.is_empty(),
        "the placeholder delta is never emitted to the UI, got {leaked:?}"
    );
    // The placeholder-only attempt was retried (an empty turn), exactly once.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the placeholder attempt + the retry"
    );
    // No stored row ever contains the placeholder…
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        loaded.iter().all(|m| !serde_json::to_string(m)
            .unwrap_or_default()
            .contains("Empty message")),
        "no stored row carries the placeholder, got {loaded:?}"
    );
    // …and the abandoned attempt persisted NO assistant row (only the retry's).
    assert_eq!(
        loaded
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Assistant))
            .count(),
        1,
        "exactly ONE assistant row (the retry's), got {loaded:?}"
    );
}

// ---------------------------------------------------------------------------
// (issue #190) The turn the output cap cut off.
//
// `FinishReason::Length` was mapped by all three wires and read by nobody in
// the turn loop: a stream that ended `Done(Length)` fell through the #64 gate
// (whose first conjunct demanded `Some(Stop)`) and reached the SUCCESS triple —
// `agent_end` / `turn_end` / `agent_settled` — with the model's half-finished
// sentence as its reply. The user watched the composer unlock and re-prompted.
// The three tests below are the three shapes of that turn, and the fourth is
// the gate the fix generalised.
// ---------------------------------------------------------------------------

/// `abandoned_tool_result()`'s text (`loop/tools.rs`), pinned byte-for-byte so
/// a truncated call's card cannot silently reword itself into the misleading
/// ``missing `path` or `content` parameter`` sentence the bug produced.
const ABANDONED: &str = "discarded — the model call that asked for this tool was \
                         abandoned before it ran";

/// The `(id, tool_name, result, is_error)` of every `tool_execution_end`, in
/// emission order.
fn closes(events: &[RpcEvent]) -> Vec<(String, String, Value, bool)> {
    events
        .iter()
        .filter_map(|e| match e {
            RpcEvent::tool_execution_end {
                tool_call_id,
                tool_name,
                result,
                is_error,
            } => Some((
                tool_call_id.clone(),
                tool_name.clone(),
                result.clone(),
                *is_error,
            )),
            _ => None,
        })
        .collect()
}

/// A `SubagentDispatcher` that COUNTS dispatches and resolves `Completed` with
/// a value no assertion here accepts — so a loop that DID dispatch the
/// truncated call fails on the counter AND on the close's text, instead of
/// hanging on a never-resolving oneshot and reporting a timeout.
struct CountingDispatcher {
    dispatches: Arc<std::sync::atomic::AtomicUsize>,
}

impl SubagentDispatcher for CountingDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        _task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::subagent::SubagentCancel,
    ) {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = tx.send(SubagentOutcome::Completed {
            output: "A TRUNCATED CALL WAS DISPATCHED — this must never happen".to_string(),
            metrics: crate::subagent::SubagentMetrics {
                output: "A TRUNCATED CALL WAS DISPATCHED — this must never happen".to_string(),
                input_tokens: 1,
                output_tokens: 1,
                cost: Some(0.0),
                duration_ms: 1,
            },
        });
        (rx, crate::subagent::SubagentCancel::none())
    }
}

/// (issue #190, Decision 1) A turn the output cap cut off is TERMINAL and
/// reads as a truncation, never as a success: `turn_truncated` is emitted
/// BEFORE the settle, `agent_end` (the success event) is never emitted, the
/// text the model did produce is persisted exactly once, and no retry is
/// attempted — re-issuing the assistant turn can duplicate a non-idempotent
/// tool call, which is why the whole family of "retry with a bigger
/// `max_tokens`" designs was rejected (ADR 0108).
#[tokio::test]
async fn a_turn_cut_off_by_the_output_cap_settles_as_truncated_and_persists_its_text() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        // The field report verbatim: a confident half-sentence, then the cap.
        ProviderEvent::TextDelta("OK, the plans look good — I'm going to write it now".to_string()),
        ProviderEvent::Done(FinishReason::Length),
    ])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("write it")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    // The event is a UNIT variant (Decision 3): it states one fact about the
    // turn and re-reports nothing from the wire.
    assert!(
        events.iter().any(|e| matches!(e, RpcEvent::turn_truncated)),
        "`turn_truncated` was emitted as a unit variant, got {kinds:?}",
    );

    // The disposition is announced BEFORE the settle resolves the turn, so a
    // peer can stash it while `turn_end` is still ahead of it.
    let truncated_at = kinds
        .iter()
        .position(|k| k == "turn_truncated")
        .unwrap_or_else(|| panic!("the truncation was announced, got {kinds:?}"));
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("the turn settled with turn_end");
    let settled_at = kinds
        .iter()
        .position(|k| k == "agent_settled")
        .expect("the turn settled with agent_settled");
    assert!(
        truncated_at < turn_end_at && truncated_at < settled_at,
        "`turn_truncated` precedes `turn_end` ({turn_end_at}) and `agent_settled` \
         ({settled_at}) — an event after the settle is an event no peer is still \
         listening for, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "agent_end"),
        "a truncated turn is NOT a clean stop — `agent_end` is the success event and \
         this turn never succeeded, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start"),
        "a `Length` stop is terminal, never retried (Decision 1: replaying the \
         assistant turn can duplicate a non-idempotent tool call), got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly ONE model call — no re-issue, no bigger `max_tokens`"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_end").count(),
        1,
        "the streamed reply is announced once, got {kinds:?}",
    );

    // The text IS persisted. It is real content the model produced; showing it
    // in the display while the transcript lacks it recreates the display /
    // transcript split the compaction-recovery follow-ups §4 documents.
    let assistants: Vec<&ChatMessage> = loop_
        .messages
        .iter()
        .filter(|m| matches!(m.role, ChatRole::Assistant))
        .collect();
    assert_eq!(
        assistants.len(),
        1,
        "exactly ONE assistant row in the transcript, got {:?}",
        loop_.messages
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    let persisted: Vec<&ChatMessage> = loaded
        .iter()
        .filter(|m| matches!(m.role, ChatRole::Assistant))
        .collect();
    assert_eq!(
        persisted.len(),
        1,
        "exactly ONE assistant row in the store a reload reads back, got {loaded:?}",
    );
    assert_eq!(
        persisted[0].content,
        MessageContent::Text("OK, the plans look good — I'm going to write it now".to_string()),
        "the persisted row carries the streamed text"
    );
    assert!(
        persisted[0].tool_calls.is_none(),
        "a truncated turn never carries its tool calls to the provider"
    );
}

/// (issue #190, Decision 2) A truncated turn's tool calls are NEVER executed.
/// Truncated arguments go `parse_arguments` → `{}` → `unknown_param_error`
/// passes (it checks only EXTRANEOUS keys) → the permission gate returns
/// `Allow` (there is no `path` to judge) → the executor refuses with
/// ``write: missing `path` or `content` parameter``. That card is red for the
/// wrong reason, and 566 of them are measured in this machine's DB. The close
/// here is `abandoned_tool_result()`'s honest sentence, and the counting
/// dispatcher proves nothing ran.
#[tokio::test]
async fn a_truncated_turns_tool_calls_are_closed_as_abandoned_and_never_dispatched() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("writing the file now".to_string()),
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            // The truncated shape: `parse_arguments` hands `{}` for arguments
            // the cap cut off mid-object, which is exactly what used to reach
            // the executor and produce the misleading refusal.
            name: "write".to_string(),
            arguments: json!({}),
            defect: None,
        }),
        ProviderEvent::ToolCall(ToolCall {
            id: "t2".to_string(),
            // A `subagent` rides along so the no-dispatch claim is OBSERVABLE
            // rather than inferred: `run_tool_batch` would have spawned it in
            // the concurrent `join_all` ahead of the sequential loop.
            name: "subagent".to_string(),
            arguments: json!({}),
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::Length),
    ])]);
    let dispatches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatcher = Arc::new(CountingDispatcher {
        dispatches: dispatches.clone(),
    });
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(dispatcher.clone() as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("write it")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        dispatches.load(Ordering::SeqCst),
        0,
        "NOT ONE call was dispatched — a truncated `arguments` payload is not a \
         request the model finished making (Decision 2 / ADR 0108)"
    );

    // Both announced rows closed, in accumulator order, BEFORE the settle.
    let closed = closes(&events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        vec!["t1", "t2"],
        "every announced row is closed exactly once, in `acc.tool_calls` order, \
         got {kinds:?}",
    );
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("the turn settled with turn_end");
    for (idx, k) in kinds.iter().enumerate() {
        if k == "tool_execution_end" {
            assert!(
                idx < turn_end_at,
                "the close at {idx} must precede `turn_end` ({turn_end_at}) — \
                 `normalize` closes a row on `tool_execution_end` and on nothing \
                 after it, got {kinds:?}",
            );
        }
    }

    for (id, _, result, is_error) in &closed {
        assert!(*is_error, "a truncated call is a failed card, got {id}");
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert_eq!(
            text, ABANDONED,
            "the close is the honest `abandoned_tool_result()` sentence, not the \
             executor's misleading refusal, got {id} → {text}"
        );
        assert!(
            !text.contains("missing"),
            "the card must NOT blame the model for a `path`/`content` parameter \
             the output cap never let it send, got {id} → {text}"
        );
    }

    // And no `tool` transcript row: nothing dispatched, so nothing to answer.
    assert!(
        !loop_
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Tool)),
        "no `tool` row is pushed for a call that never ran, got {:?}",
        loop_.messages
    );
    let RpcEvent::turn_end { tool_results, .. } = &events[turn_end_at] else {
        unreachable!("turn_end_at indexes a turn_end");
    };
    assert!(
        tool_results.is_empty(),
        "the close adds no entry to `turn_end.tool_results`, got {tool_results:?}",
    );
}

/// (issue #190) A `Length` stop that streamed NO text persists NOTHING — the
/// 33-byte `{"role":"assistant","content":"","tool_calls":[]}` row issue #64
/// exists to prevent is gone on this route too, and the turn still settles
/// (loudly, as a truncation) rather than hanging.
#[tokio::test]
async fn a_truncated_turn_with_no_text_persists_no_assistant_row() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) =
        ScriptedProvider::new(vec![Some(vec![ProviderEvent::Done(FinishReason::Length)])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert!(
        events.iter().any(|e| matches!(e, RpcEvent::turn_truncated)),
        "an empty truncation is still announced as one, got {kinds:?}",
    );
    assert!(
        kinds.iter().any(|k| k == "turn_end") && kinds.iter().any(|k| k == "agent_settled"),
        "the truncated turn settles (it is terminal, not stuck), got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "message_end"),
        "with no text there is no reply to announce, got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no retry — a `Length` stop is terminal"
    );
    assert!(
        !loop_
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Assistant)),
        "no assistant row was pushed at all, got {:?}",
        loop_.messages
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        !loaded.iter().any(|m| matches!(m.role, ChatRole::Assistant)),
        "the 33-byte empty assistant row does not exist on the `Length` route \
         either, got {loaded:?}",
    );
}

/// A `Model` whose compaction threshold sits at exactly `threshold` tokens:
/// the `Compactor` is built with the DEFAULT reserve (`16384`) and the config
/// is fixed at `AgentLoop::new`, so the only knob a test can turn AFTER
/// construction is the window — `should_compact()` is
/// `context_tokens > context_window - reserve`, so a threshold of `t` is a
/// window of `16384 + t`. (The same fixture exists in
/// `context_usage.rs::model_with_threshold`; that one is private to its
/// module, so this file carries its own copy rather than widening it.)
fn model_with_threshold(id: &str, threshold: u32) -> Model {
    Model {
        context_window: 16_384 + threshold,
        ..fake_model(id)
    }
}

/// (issue #190, follow-up) A truncation that streamed NOTHING must leave the
/// compaction watermark honest.
///
/// The arithmetic this pins, in `compact.rs`. `record_usage` (`:72`) writes
/// `usage_watermark = messages.len() + 1` — the `+1` anticipates the reply the
/// caller is about to append — and `turn_abandoned` (`:104`) early-returns on
/// `usage_watermark <= messages.len()`, otherwise drops `pending_output` from
/// the anchor and pulls the watermark back to what the transcript HOLDS. So on
/// the two halves of the truncation route the SAME call does two different
/// things, and both matter:
///
/// * **text streamed** — `handle_truncated_turn` pushes the reply BEFORE the
///   settle, so `watermark == len` and the guard fires: a NO-OP, Δ = 0 on every
///   field. (Asserted below, in
///   `a_truncation_that_streamed_text_leaves_the_context_count_where_usage_put_it`.)
/// * **nothing streamed** — nothing is pushed, so `watermark == len + 1 > len`
///   and the guard does NOT fire: the call ACTS, and acting is correct.
///
/// `settle_truncated` left the call out on the theory that it was either harmful
/// (the text-streamed half) or a no-op (this half). Both halves of that theory
/// are false, and this is the half that costs real money: without the reconcile
/// the count keeps the 50 output tokens of a reply that was never persisted and
/// the watermark keeps an index past the end of the transcript, so the loop
/// believes in an assistant row that does not exist.
#[tokio::test]
async fn a_truncation_that_streamed_nothing_does_not_leave_phantom_output_tokens() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        // The provider charges for a reply the output cap then cut off before
        // a single character of it reached us: `input + output` = 150, of which
        // 50 is output that NEVER EXISTED.
        ProviderEvent::Usage(
            Usage {
                input_tokens: 100,
                output_tokens: 50,
            },
            None,
        ),
        ProviderEvent::Done(FinishReason::Length),
    ])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let kinds: Vec<String> = drain_events(&mut events_rx)
        .iter()
        .map(|e| e.kind().to_string())
        .collect();
    assert!(
        kinds.iter().any(|k| k == "turn_truncated"),
        "the turn settled as a truncation (this pin is about the counter, not \
         about the disposition), got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one model call — the truncation is terminal (Decision 1)"
    );

    // THE watermark claim: the reply `record_usage` assumed was never appended,
    // so the anchor must have given its output back. 150 - 50 = 100.
    assert_eq!(
        loop_.compactor.context_tokens(),
        100,
        "a truncation with NO text must retract the phantom reply's output from \
         the context count: the provider reported `input 100 + output 50`, but the \
         50 belongs to a reply that was never pushed or persisted, so the honest \
         count is the 100 the transcript actually holds. Leaving `turn_abandoned` \
         out of `settle_truncated` leaves 150 here — 50 tokens the model never \
         generated, and the number the next turn's threshold is compared against.",
    );

    // And the transcript agrees: nothing was persisted, so nothing may be
    // counted. `context_tokens` and the row count are the two halves of the
    // same lie — the count asserts a row the transcript does not have.
    assert!(
        !loop_
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Assistant)),
        "ZERO assistant rows in the transcript (issue #64's 33-byte row must not \
         come back on this route) — which is exactly why the count above may not \
         carry a reply, got {:?}",
        loop_.messages
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        !loaded.iter().any(|m| matches!(m.role, ChatRole::Assistant)),
        "and ZERO in the store a reload reads back, got {loaded:?}",
    );
}

/// The OTHER half of the same `turn_abandoned` guard, and the fact that made the
/// omission in `settle_truncated` look safe: when text DID stream, the reply is
/// pushed before the settle, so `usage_watermark == messages.len()`, the
/// `watermark <= len` guard fires, and the call is a **no-op on every field**
/// (Δ = 0). The docblock that justified the omission claimed the call would
/// "discount a real message and reset the count to the bare anchor" on this
/// route — that harm is arithmetically unreachable.
///
/// Pinned as an invariant rather than as a behaviour, because it is what stops a
/// later reader from "fixing" the no-text case back the other way: the count is
/// the provider's own `input + output` here, and it stays that way WITH the
/// correction — the correction does not touch a reply that is really in the
/// transcript.
#[tokio::test]
async fn a_truncation_that_streamed_text_leaves_the_context_count_where_usage_put_it() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("half a sentence, then the cap".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 100,
                output_tokens: 50,
            },
            None,
        ),
        ProviderEvent::Done(FinishReason::Length),
    ])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    drain_events(&mut events_rx);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "terminal, not retried");

    // The reply IS in the transcript — that is the premise the guard reads.
    assert_eq!(
        loop_
            .messages
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Assistant))
            .count(),
        1,
        "a truncated turn persists its streamed text (Decision 3 / §7), got {:?}",
        loop_.messages
    );

    // ...so `turn_abandoned` — which `settle_truncated` now makes — is a no-op:
    // the count is still the provider's own report, 100 + 50. Δ = 0 against the
    // same run without the call, which is the whole reason making the two halves
    // of the settle identical is safe.
    assert_eq!(
        loop_.compactor.context_tokens(),
        150,
        "with text streamed the reply is real, the watermark guard fires, and the \
         count stays at the provider's `input + output` = 150 — the correction is a \
         NO-OP here, not the \"discount a real message and reset to the bare \
         anchor\" the old docblock claimed, got {}",
        loop_.compactor.context_tokens(),
    );
}

/// Pin B — the pin that would have caught the omission.
///
/// A phantom 50 output tokens is not a rounding detail: it is the number the
/// NEXT turn's `should_compact()` compares against the threshold, and the
/// inflated watermark swallows that turn's own prompt inside the watermark
/// window (`refresh` estimates only `messages[watermark..]`, so a watermark at
/// `len` after the prompt is pushed estimates nothing) — which is how the
/// inflation self-perpetuates instead of decaying.
///
/// The arithmetic, re-derived from the fixtures rather than trusted:
/// `model_with_threshold(_, 120)` is a window of `16384 + 120` = 16504, and with
/// the default reserve the threshold is exactly **120**, which sits BETWEEN the
/// honest 100 and the inflated 150. Turn 1 is the no-text truncation; turn 2 is a
/// cheap prompt (`"next"` = 4 chars / 4 + 1 = 2 tokens of trailing estimate).
///
/// Correct: `refresh` adds the new prompt to the corrected anchor → 102, not
/// > 120 → no compaction, ONE model call for turn 2, TWO for the session.
/// Omitting the call: the watermark (`len + 1` = 2) already covers the freshly
/// pushed prompt, so `refresh` estimates NOTHING and the count stays 150 →
/// 150 > 120 → a compaction that had nothing real to compact: one
/// `compaction_start`, a SUMMARY model call the session did not need, the
/// transcript rewritten, and turn 2's own call on top — 3 calls for the session
/// instead of 2.
#[tokio::test]
async fn a_truncation_that_streamed_nothing_does_not_compact_the_next_turn() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // Turn 1: charged for a reply that never arrived.
        Some(vec![
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 100,
                    output_tokens: 50,
                },
                None,
            ),
            ProviderEvent::Done(FinishReason::Length),
        ]),
        // Turn 2: the cheap follow-up. A provider that is ASKED FOR A SUMMARY
        // (the spurious compaction's call) gets this same script, so the
        // compaction runs to completion and rewrites the transcript exactly as
        // production would — the failure this pin reports is therefore the real
        // cost, not a stubbed one.
        Some(vec![
            ProviderEvent::TextDelta("the follow-up reply".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        // Threshold 120: the honest 100 (plus the next prompt's 2) stays under
        // it, the inflated 150 crosses it.
        vec![model_with_threshold("m1", 120)],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let first_turn: Vec<String> = drain_events(&mut events_rx)
        .iter()
        .map(|e| e.kind().to_string())
        .collect();
    assert!(
        first_turn.iter().any(|k| k == "turn_truncated"),
        "turn 1 is the truncation, got {first_turn:?}",
    );
    // What the counter is carrying INTO turn 2 is reported by the assertions
    // below rather than asserted here: this test's evidence is the compaction,
    // and a precondition assert would replace that evidence with the same
    // 100-vs-150 number pin A already reports.
    let after_turn_one = loop_.compactor.context_tokens();

    loop_.handle_prompt(&text_prompt("next")).await;
    let second_turn: Vec<String> = drain_events(&mut events_rx)
        .iter()
        .map(|e| e.kind().to_string())
        .collect();

    assert!(
        !second_turn.iter().any(|k| k == "compaction_start"),
        "turn 2 must NOT compact. Turn 1 left the counter at {after_turn_one} and \
         turn 2's own prompt adds 2 more; the threshold is 120. A `compaction_start` \
         here means the threshold was read against the 150 the phantom reply left \
         behind — a spurious compaction that spends a SUMMARY model call and \
         rewrites the transcript for context pressure that does not exist, got \
         {second_turn:?}",
    );
    assert!(
        !second_turn.iter().any(|k| k == "compaction_end"),
        "and therefore no `compaction_end` either, got {second_turn:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "exactly TWO model calls for the session: the truncated one and turn 2's. \
         A third is the summary call of the compaction that should never have been \
         considered — the extra money and the rewritten transcript this pin exists \
         to prevent.",
    );
    assert!(
        second_turn.iter().any(|k| k == "agent_settled"),
        "turn 2 settles normally (it is a clean stop), got {second_turn:?}",
    );
}

/// The generalised #64 gate: with the `Some(Stop)` conjunct gone, the gate is
/// keyed on \"nothing was produced\", so the provider that CLAIMS tool calls
/// (`Done(ToolCalls)`) while delivering none is now caught by the same
/// recovery instead of settling clean on an empty message.
#[tokio::test]
async fn a_provider_that_claims_tool_calls_and_dispatches_none_is_retried() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // `finish_reason: tool_calls` with NO tool call in the stream — the
        // shape the old `Some(Stop)` conjunct let through, settling as a clean
        // stop on an empty reply.
        Some(vec![ProviderEvent::Done(FinishReason::ToolCalls)]),
        Some(vec![
            ProviderEvent::TextDelta("here you go".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("do the thing")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    let start = events
        .iter()
        .find(|e| matches!(e, RpcEvent::auto_retry_start { .. }))
        .expect("an attempt that produced nothing is retried whatever it claimed");
    assert!(
        matches!(
            start,
            RpcEvent::auto_retry_start {
                attempt: 2,
                max_attempts: 5,
                ..
            }
        ),
        "the retry rides the SHARED budget, got {start:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the empty attempt + the retry, got {kinds:?}",
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    let assistants: Vec<&ChatMessage> = loaded
        .iter()
        .filter(|m| matches!(m.role, ChatRole::Assistant))
        .collect();
    assert_eq!(
        assistants.len(),
        1,
        "the empty attempt persisted NOTHING — only the retry's reply, got {loaded:?}",
    );
    assert_eq!(
        assistants[0].content,
        MessageContent::Text("here you go".to_string()),
        "the single persisted assistant row is the retry's"
    );
}

/// The same generalised gate with the budget spent: `auto_retry_start` IS
/// announced (so the catch is the retry path, not the error path) and NO
/// assistant row is persisted at all — the claimed-`ToolCalls`-with-nothing
/// attempt leaves no 33-byte row behind it either, and the turn ends as a
/// VISIBLE failure rather than a silent clean stop.
#[tokio::test]
async fn a_claimed_tool_calls_turn_with_a_spent_budget_announces_the_retry_and_persists_nothing() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    // ONE script, repeated (out of range repeats the last): every attempt
    // claims tool calls and delivers none.
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![ProviderEvent::Done(
        FinishReason::ToolCalls,
    )])]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // TWO attempts: the first empty turn spends the budget, so the second
        // settles as a failure — after ONE announced retry.
        RetryPolicy::new_with(2, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("do the thing")).await;
    let events = drain_events(&mut events_rx);
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "the retry WAS announced (so this is the generalised gate firing, not a \
         different exit), got {kinds:?}",
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            RpcEvent::auto_retry_end {
                success: false,
                final_error: Some(_),
                ..
            }
        )),
        "the spent budget fails LOUDLY, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "agent_end"),
        "a turn that produced nothing is never a clean stop, got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "a turn never makes more than `max_attempts` model calls"
    );
    assert!(
        !loop_
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Assistant)),
        "no assistant row in the transcript, got {:?}",
        loop_.messages
    );
    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        !loaded.iter().any(|m| matches!(m.role, ChatRole::Assistant)),
        "and NOTHING persisted to the store a reload reads back, got {loaded:?}",
    );
}

/// (issue #190 follow-up, F3) A `Length` turn must RETIRE the retry report the
/// turn's own abandoned attempt left pending — otherwise the NEXT ordinary
/// successful prompt announces `auto_retry_end { success: true }` for the
/// truncated turn, i.e. a "Retry succeeded" banner for a turn that did not
/// succeed.
///
/// The mechanism is `RetryPolicy::reported` (`retry.rs:40`): set ONLY by
/// `note_success()` and cleared ONLY by `next_retry_delay()`. Turn 1 here retries
/// (the empty turn consumes an attempt, so `attempt == 2`, `reported == false`)
/// and its next response ends `Done(Length)`. The truncation gate sits BEFORE
/// `note_success()`, so the pending report survives the settle — and turn 2, a
/// clean stop, walks into `note_success()`, gets `Some(2)`, and emits the
/// success frame for turn 1.
///
/// The fix closes the pending report on the truncation route the way
/// `retry_abandoned_attempt`'s budget-exhausted arm closes it (`turn.rs:530`):
/// one `auto_retry_end { success: false }` whose `final_error` names the
/// truncation, emitted before the settle, and the report retired so no later
/// turn can report it as a success.
#[tokio::test]
async fn a_truncated_turn_does_not_leave_a_retry_report_for_the_next_prompt_to_call_success() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // Turn 1, attempt 1: the #64 empty turn — nothing streamed, no tool call.
        Some(vec![ProviderEvent::Done(FinishReason::Stop)]),
        // Turn 1, attempt 2: the cap ends it. The budget is NOT spent, so
        // `reported` is still false when this settles.
        Some(vec![
            ProviderEvent::TextDelta("half a sentence, then the cap".to_string()),
            ProviderEvent::Done(FinishReason::Length),
        ]),
        // Turn 2: an ordinary clean stop. This is the prompt that must NOT
        // inherit turn 1's retry.
        Some(vec![
            ProviderEvent::TextDelta("all good".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );

    loop_.handle_prompt(&text_prompt("go")).await;
    let first_turn: Vec<RpcEvent> = drain_events(&mut events_rx);
    let first_kinds: Vec<String> = first_turn.iter().map(|e| e.kind().to_string()).collect();
    assert!(
        first_kinds.iter().any(|k| k == "auto_retry_start"),
        "precondition: turn 1 DID retry (that is what leaves `reported == false` \
         pending), got {first_kinds:?}",
    );
    assert!(
        first_kinds.iter().any(|k| k == "turn_truncated"),
        "precondition: turn 1 ended as a truncation, got {first_kinds:?}",
    );

    loop_.handle_prompt(&text_prompt("and now?")).await;
    let second_turn: Vec<RpcEvent> = drain_events(&mut events_rx);
    let second_kinds: Vec<String> = second_turn.iter().map(|e| e.kind().to_string()).collect();

    // THE FINDING: turn 2 is a clean stop on a fresh prompt. It has no retry of
    // its own, so it has no report to make — and it must not speak for turn 1.
    assert!(
        !second_kinds.iter().any(|k| k == "auto_retry_end"),
        "the prompt AFTER a truncated turn emits NO `auto_retry_end` at all: \
         turn 1's retry was closed on the truncation route, so there is nothing \
         left for turn 2 to report. A `success: true` here is the false \
         \"Retry succeeded\" banner for a turn the output cap cut off, got \
         {second_kinds:?}",
    );
    assert!(
        !second_turn
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_end { success: true, .. })),
        "explicitly: never `success: true` for the truncated turn, got {second_kinds:?}",
    );
    assert!(
        second_kinds.iter().any(|k| k == "agent_settled"),
        "turn 2 still settles normally (this pin is about the retry frame only), \
         got {second_kinds:?}",
    );

    // AND the close itself: the pending report is retired ON TURN 1, as a
    // `success: false` naming the truncation — the same shape the
    // budget-exhausted arm of `retry_abandoned_attempt` uses, so a peer that
    // opened a retry card sees it CLOSE rather than hang.
    let closed: Vec<&RpcEvent> = first_turn
        .iter()
        .filter(|e| matches!(e, RpcEvent::auto_retry_end { success: false, .. }))
        .collect();
    assert_eq!(
        closed.len(),
        1,
        "the truncated turn closes its own pending retry exactly once, got \
         {first_kinds:?}",
    );
    let RpcEvent::auto_retry_end {
        attempt,
        final_error,
        ..
    } = closed[0]
    else {
        unreachable!("the filter keeps only auto_retry_end {{ success: false, .. }}");
    };
    assert_eq!(
        *attempt, 2,
        "the close names the attempt the retry reached (the empty turn + the \
         truncated one)"
    );
    let reason = final_error.as_deref().unwrap_or_default();
    assert!(
        reason.contains("truncat"),
        "the close says WHY the retry ended in a truncation rather than a \
         success, got {final_error:?}"
    );
    // The close precedes the settle, so a peer sees the retry resolved before
    // the turn is over (the same ordering the budget-exhausted arm keeps).
    let settle_at = first_kinds
        .iter()
        .position(|k| k == "agent_settled")
        .expect("turn 1 settles");
    let close_at = first_kinds
        .iter()
        .position(|k| k == "auto_retry_end")
        .expect("the close is emitted");
    assert!(
        close_at < settle_at,
        "the retry close at {close_at} precedes `agent_settled` ({settle_at}), got \
         {first_kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the empty attempt, the truncated one, and turn 2's — the truncation is \
         still terminal (Decision 1: no re-issue)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// (the turn-loop bound) Two guards on the turn loop's inner `loop`, both
// producing `RpcEvent::turn_capped`: a ceiling on rounds per prompt, and a
// ceiling on CONSECUTIVE tool batches refused with an identical signature.
//
// Every test here is wrapped in `tokio::time::timeout`. That is not decoration:
// the thing under test is a loop that does not stop, and an unbounded one hangs
// to the 15 s settle timeout and then reports a confusing empty-event failure
// instead of "the guard did not fire".
//
// The provider call counts are asserted through `ScriptedProvider`'s own
// `Arc<AtomicU32>`, never inferred from an event count — an event count can be
// right for the wrong reason (a turn that emitted the frame and then looped
// again would still show one `turn_capped`).
// ─────────────────────────────────────────────────────────────────────────────

/// The turn bound's own event order, as `(kind)` names (this file's other tests
/// each build what they need; the bound's tests all need the same three
/// positions, so they share this one).
fn capped_kinds(events: &[RpcEvent]) -> Vec<String> {
    events.iter().map(|e| e.kind().to_string()).collect()
}

/// A `write` call carrying the unknown `contentt` key ALONGSIDE the legal pair
/// — `unknown_param_error`'s extraneous-key trigger, refused BEFORE dispatch and
/// then re-sent verbatim. The legal twin stays present so the rejection is the
/// only reason the file is never written.
///
/// This is NOT the field report's shape. The report is `bash` with `arguments:
/// {}`, which has no keys for the checker to call extraneous and so is DISPATCHED
/// — that one is `bash_empty_call()` /
/// `a_run_of_identical_empty_argument_batches_stops_the_turn_as_capped` below.
/// Both trigger classes must stay covered: this test pins the pre-dispatch one.
fn refused_write_call() -> ToolCall {
    ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({
            "path": "never-written-by-the-bound.txt",
            "content": "legal content",
            "contentt": "typo",
        }),
        defect: None,
    }
}

/// A run of IDENTICAL refused batches stops the turn as `turn_capped`.
///
/// This pins the PRE-DISPATCH trigger class: a call the param checker refuses
/// before it runs. It is NOT the burst the field report describes — the report is
/// **303** `{}`-argument `bash` calls in 19 minutes, and `{}` has no keys to
/// refuse, so that call is DISPATCHED and refused by the executor instead. That
/// shape is
/// `a_run_of_identical_empty_argument_batches_stops_the_turn_as_capped`, and it
/// is the one the guard was originally written for and did not catch. What both
/// shapes share is why nothing could see them: every individual round is a
/// well-formed turn — a call, a refusal, a new attempt — and only the RUN is
/// pathological.
///
/// One script entry reproduces a stuck turn, because `ScriptedProvider` repeats
/// the LAST script when the index runs out — which is exactly what a looping
/// turn does.
#[tokio::test]
async fn a_run_of_identical_refused_batches_stops_the_turn_as_capped() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(refused_write_call()),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    // The scratch guard is the FIRST tuple element (#96) — bind it, never
    // `.0`-project it away: the no-dispatch proof below reads inside it.
    let (space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
        loop_
    });
    let _loop_ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the bounded turn TERMINATED (an unbounded loop hangs here)")
        .expect("the turn task did not panic");
    let events = drain_events(&mut events_rx);
    let kinds = capped_kinds(&events);

    // The bound fired, and fired ONCE — a guard that emitted the frame on every
    // remaining iteration would be as broken as no guard.
    assert_eq!(
        kinds.iter().filter(|k| *k == "turn_capped").count(),
        1,
        "exactly ONE `turn_capped`, got {kinds:?}",
    );

    // The ordering contract: BEFORE `turn_end` and `agent_settled`, so a peer can
    // stash the disposition while the turn is still unresolved.
    let capped_at = kinds
        .iter()
        .position(|k| k == "turn_capped")
        .unwrap_or_else(|| panic!("the capped turn emitted `turn_capped`, got {kinds:?}"));
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .unwrap_or_else(|| panic!("the capped turn settled with `turn_end`, got {kinds:?}"));
    let settled_at = kinds
        .iter()
        .position(|k| k == "agent_settled")
        .unwrap_or_else(|| panic!("the capped turn settled with `agent_settled`, got {kinds:?}"));
    assert!(
        capped_at < turn_end_at && capped_at < settled_at,
        "`turn_capped` at {capped_at} must PRECEDE `turn_end` ({turn_end_at}) and \
         `agent_settled` ({settled_at}) — after the settle the driver has resolved \
         the turn and stops listening, got {kinds:?}",
    );

    // THE BOUND, measured at the provider rather than inferred from the frame:
    // 3 refused batches are tolerated, and the guard is consulted at the TOP of
    // the iteration AFTER the run reaches the ceiling — so the model is called
    // exactly `MAX_IDENTICAL_REFUSAL_RUNS + 1` times and never a 5th.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_IDENTICAL_REFUSAL_RUNS + 1,
        "the run is bounded at {MAX_IDENTICAL_REFUSAL_RUNS} repeats, so the model \
         gets exactly {} calls — the 303-call loop is the failure this pins, got \
         {kinds:?}",
        MAX_IDENTICAL_REFUSAL_RUNS + 1,
    );

    // The refused calls were NEVER DISPATCHED — three rounds of refusals and a
    // fourth, and not one of them touched the filesystem. `param_rejection.rs`
    // owns the one-batch version of this proof; this is the version that says
    // "and it stayed true across the whole bounded run".
    assert!(
        !space.join("never-written-by-the-bound.txt").exists(),
        "a refused call must not touch the filesystem, in any round of the run"
    );

    // And the settle is the FAILURE settle, not a cancel and not a truncation:
    // no `turn_truncated` frame (that is the provider's output cap, a different
    // fact), and the disposition the desktop's `attention.ts` reads is not the
    // user-Stop one.
    assert!(
        !kinds.iter().any(|k| k == "turn_truncated"),
        "the agent's own bound is not the provider's output cap, got {kinds:?}",
    );
}

/// THE BURST SHAPE — `bash` with `arguments: {}`, the same call every round.
///
/// This is the shape the card, the plan and `MAX_IDENTICAL_REFUSAL_RUNS`'
/// docblock all cite by number (**303** `{}`-argument `bash` calls in 19
/// minutes), and it is NOT the shape `refused_write_call()` reproduces. The
/// difference decides whether the guard fires at all:
///
/// * `refused_write_call` carries an EXTRANEOUS key, so `unknown_param_error`
///   refuses it BEFORE dispatch and `rejected[i]` is true.
/// * `bash` with `{}` has NO keys at all, so `unknown_param_error` — which
///   reports only extraneous keys — answers `None` (`items.is_empty()`), the
///   call is DISPATCHED, and `exec_bash` answers
///   ``bash: missing `command` parameter``. That is a tool-RESULT error, which
///   the pre-dispatch checker knows nothing about.
///
/// So the burst shape reaches this guard only through the SECOND trigger
/// class: "the engine refused this call and told the model to re-send it with
/// correct arguments", read off the advertised `required` list plus the
/// result's own `is_error`. Without that class the 303-call run is stopped by
/// the 64-round ceiling — 64 rounds of a doomed call instead of 3.
///
/// The bound is asserted at the provider, exactly as the extraneous-key test
/// asserts it, for the same reason: a call count can be right for no wrong
/// reason where an event count can.
#[tokio::test]
async fn a_run_of_identical_empty_argument_batches_stops_the_turn_as_capped() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            name: "bash".to_string(),
            // NO keys at all — the field report's shape, verbatim.
            arguments: json!({}),
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
        loop_
    });
    let _loop_ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the bounded turn TERMINATED (an unbounded loop hangs here)")
        .expect("the turn task did not panic");
    let events = drain_events(&mut events_rx);
    let kinds = capped_kinds(&events);

    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_IDENTICAL_REFUSAL_RUNS + 1,
        "the empty-argument burst is bounded at {MAX_IDENTICAL_REFUSAL_RUNS} repeats \
         and the model is called exactly {} times — the 303-call loop is the failure \
         this pins, and at HEAD the guard never sees this shape, so the turn runs to \
         the {MAX_MODEL_ROUNDS_PER_PROMPT}-round ceiling instead, got {kinds:?}",
        MAX_IDENTICAL_REFUSAL_RUNS + 1,
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "turn_capped").count(),
        1,
        "exactly ONE `turn_capped`, got {kinds:?}",
    );
    // And it is the REFUSAL guard, not the round ceiling, that ended it: the
    // round count is the discriminator, and it is the smaller number.
    assert!(
        calls.load(Ordering::SeqCst) < MAX_MODEL_ROUNDS_PER_PROMPT,
        "the refusal guard fired BEFORE the round ceiling, got {}",
        calls.load(Ordering::SeqCst),
    );
}

/// The false-positive half, and the reason the broadened predicate is not
/// `is_error`: a batch whose calls FAILED BUT WERE WELL-FORMED is progress,
/// not a stuck loop, and bounding it would end the user's prompt over a
/// command that simply did not work.
///
/// `bash { "command": "exit 1" }` is the sharpest case available: identical
/// every round, `is_error: true` every round, and NOTHING but the round
/// ceiling can stop it. Every required key is present, so the `required`
/// conjunct answers `false` and the run never grows — which is exactly the
/// assertion that reddens if someone widens the predicate to "any tool error".
///
/// Reads the same way `a_mixed_batch_resets_the_identical_refusal_run` does:
/// the run counter is loop-internal, so the observable is the provider call
/// count. A run that grew stops at `MAX_IDENTICAL_REFUSAL_RUNS + 1`; a run
/// that stays at 0 reaches `MAX_MODEL_ROUNDS_PER_PROMPT`.
#[tokio::test]
async fn a_repeated_batch_of_failing_well_formed_calls_is_never_a_refusal_run() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            name: "bash".to_string(),
            // `command` IS there. The command fails; the CALL is well-formed.
            arguments: json!({ "command": "exit 1" }),
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
    tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn TERMINATED (on the round ceiling, not the refusal guard)")
        .expect("the turn task did not panic");
    let events = drain_events(&mut events_rx);
    let kinds = capped_kinds(&events);

    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_MODEL_ROUNDS_PER_PROMPT,
        "a failing command is NOT a refusal: the run stays at 0 and the turn \
         survives to the round ceiling ({MAX_MODEL_ROUNDS_PER_PROMPT} calls). A turn \
         stopped at {} means the guard fired on `is_error` alone, which is the \
         false positive the plan rejected and this pin refuses, got {kinds:?}",
        MAX_IDENTICAL_REFUSAL_RUNS + 1,
    );
}

/// A batch with ONE accepted call and one refused call never increments the
/// identical-refusal run, however many times it repeats.
///
/// This is the reset rule, and it is the difference between bounding a runaway
/// and punishing the behaviour the rejection sentence asks for: an agent that
/// fixes one key and re-sends the rest IS making progress.
///
/// HOW the pin reads. `state.identical_refusal_runs` is loop-internal, so the
/// observable that separates "the run never grew" from "the run grew" is the
/// PROVIDER CALL COUNT: a run that grew would stop the turn at
/// `MAX_IDENTICAL_REFUSAL_RUNS + 1` calls; a run that stays at 0 lets the turn
/// run all the way to the ROUND ceiling. So this asserts the turn reached
/// `MAX_MODEL_ROUNDS_PER_PROMPT` calls — strictly more than the refusal guard
/// would ever allow — which is `turn_capped`-by-the-round-guard, and proves the
/// refusal guard never fired. Asserting "no `turn_capped` at all" would be
/// unsatisfiable: the round ceiling is the other guard in the same `if`, and a
/// turn that repeats forever must end somehow.
#[tokio::test]
async fn a_mixed_batch_resets_the_identical_refusal_run() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        // ACCEPTED: `list_agents` advertises `{}` (no `properties`), so
        // `unknown_param_error` has nothing to call extraneous and the call
        // dispatches.
        ProviderEvent::ToolCall(ToolCall {
            id: "a1".to_string(),
            name: "list_agents".to_string(),
            arguments: json!({}),
            defect: None,
        }),
        // REFUSED: the same trigger as the test above.
        ProviderEvent::ToolCall(refused_write_call()),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
        loop_
    });
    let _loop_ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the bounded turn TERMINATED")
        .expect("the turn task did not panic");
    let events = drain_events(&mut events_rx);
    let kinds = capped_kinds(&events);

    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_MODEL_ROUNDS_PER_PROMPT,
        "a mixed batch RESETS the run, so the refusal guard never fires and the \
         turn survives all the way to the round ceiling ({} calls). A run that \
         counted mixed batches would have stopped at {}, got {kinds:?}",
        MAX_MODEL_ROUNDS_PER_PROMPT,
        MAX_IDENTICAL_REFUSAL_RUNS + 1,
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "turn_capped").count(),
        1,
        "the turn still ends as capped — by the ROUND guard, which is the only \
         one a mixed batch can arm, got {kinds:?}",
    );
}

/// A turn that hits the ROUND ceiling settles as `turn_capped`.
///
/// The script always returns a DISPATCHABLE tool call whose tool SUCCEEDS
/// (`list_agents` advertises `{}`, takes no arguments, reads the Agent
/// definitions from disk and answers — nothing about it can fail, be rejected or
/// be denied), so NOTHING but the ceiling can stop this turn.
///
/// **This test passes only because `ScriptedProvider` never retries, and it pins
/// the ROUND unit — it must not be described as a provider-call ceiling.**
/// `MAX_MODEL_ROUNDS_PER_PROMPT` counts rounds through the turn loop (one model
/// call plus its tool batch). Inside one round `RetryPolicy::call_with_retry` can
/// issue up to `DEFAULT_MAX_ATTEMPTS` provider calls, and `compact_if_due` issues
/// an uncounted summary call. A retrying or compacting provider would make the
/// count exceed the ceiling while the ROUND count stayed exactly at it, and this
/// assertion would then be false for a reason that has nothing to do with the
/// guard.
#[tokio::test]
async fn a_turn_that_hits_the_round_ceiling_settles_as_capped() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "a1".to_string(),
            name: "list_agents".to_string(),
            arguments: json!({}),
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
        loop_
    });
    let _loop_ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the round ceiling TERMINATED the turn")
        .expect("the turn task did not panic");
    let events = drain_events(&mut events_rx);
    let kinds = capped_kinds(&events);

    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_MODEL_ROUNDS_PER_PROMPT,
        "the ceiling is a ceiling on ROUNDS, and a `ScriptedProvider` issues \
         exactly one call per round — so exactly {MAX_MODEL_ROUNDS_PER_PROMPT} \
         calls, got {kinds:?}",
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "turn_capped").count(),
        1,
        "the round ceiling emitted `turn_capped` once, got {kinds:?}",
    );
    let capped_at = kinds
        .iter()
        .position(|k| k == "turn_capped")
        .expect("the capped turn emitted `turn_capped`");
    let settled_at = kinds
        .iter()
        .position(|k| k == "agent_settled")
        .expect("the capped turn settled");
    assert!(
        capped_at < settled_at,
        "`turn_capped` at {capped_at} precedes `agent_settled` ({settled_at}), got {kinds:?}",
    );
    // Every round dispatched its call, so every round closed its own row — and
    // the ceiling settle added nothing (it passes `&[]`). The count is the
    // discriminator: one close per round, no more.
    assert_eq!(
        kinds.iter().filter(|k| *k == "tool_execution_end").count(),
        MAX_MODEL_ROUNDS_PER_PROMPT as usize,
        "one close per DISPATCHED call and no synthetic extra from the settle, \
         got {kinds:?}",
    );
}

/// Cancellation still wins over the bound, when BOTH are armed.
///
/// The two guards sit in one `if` at the top of the iteration, immediately AFTER
/// the existing cancel guard, and this test arms both at once so the ORDER of
/// those two checks is what decides the outcome — not which one happens to be
/// true first in time.
///
/// The instrument is a `subagent` call whose dispatcher cancels the TURN token on
/// the last round's dispatch. That lands the cancel after the model call and
/// after the stream check (site 8 — the token is armed inside the batch, so
/// `consume_stream` finished before it) and before the NEXT iteration's top,
/// which is the only window where both guards are simultaneously true. The
/// dispatcher's child never resolves, so the batch ends on `SubagentWait::Cancelled`
/// and returns normally — the loop reaches the loop top with `model_rounds` at
/// the ceiling AND the token cancelled.
///
/// What it asserts is `settle_cancelled`'s disposition, not `turn_capped`:
/// telling the desktop the turn was capped when the user pressed Stop would let
/// `attention.ts` mis-attribute a user-initiated stop to an agent failure — and
/// the reverse (the bound winning) is the regression this pins.
#[tokio::test]
async fn a_cancel_beats_the_turn_bound() {
    struct CancellingOnLastRoundDispatcher {
        turn_cancel: Arc<StdMutex<CancellationToken>>,
        dispatches: AtomicU32,
        fire_on: u32,
    }

    impl SubagentDispatcher for CancellingOnLastRoundDispatcher {
        fn dispatch(
            &self,
            _parent_session_id: &str,
            _parent_cwd: &std::path::Path,
            _parent_model: &Model,
            _parent_enabled_tools: Vec<String>,
            _agent_name: String,
            _name_resolution: AgentNameResolution,
            _launch: LaunchConfig,
            _task: String,
            _sink: &Arc<dyn crate::events::EventSink>,
        ) -> (
            tokio::sync::oneshot::Receiver<SubagentOutcome>,
            crate::subagent::SubagentCancel,
        ) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if self.dispatches.fetch_add(1, Ordering::SeqCst) + 1 >= self.fire_on {
                // The token `handle_turn` cloned into this same slot.
                self.turn_cancel
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .cancel();
                // The LAST round's child NEVER resolves (the sender is forgotten,
                // never sent and never dropped), so the wait runs to the
                // `select!`'s `turn.cancelled()` arm — the batch ENDS, on cancel,
                // and returns normally to the loop top.
                std::mem::forget(tx);
            } else {
                // An EARLIER round's child answers immediately. It must, or the
                // `select!` has nothing to wake on for 63 rounds and the test
                // hangs for a reason that has nothing to do with the guard.
                let _ = tx.send(SubagentOutcome::Completed {
                    output: "done".to_string(),
                    metrics: crate::subagent::SubagentMetrics {
                        output: "done".to_string(),
                        input_tokens: 1,
                        output_tokens: 1,
                        cost: None,
                        duration_ms: 1,
                    },
                });
            }
            (rx, crate::subagent::SubagentCancel::none())
        }
    }

    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "s1".to_string(),
            name: "subagent".to_string(),
            arguments: json!({ "task": "loop forever" }),
            defect: None,
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    let dispatcher = Arc::new(CancellingOnLastRoundDispatcher {
        turn_cancel: turn_cancel.clone(),
        dispatches: AtomicU32::new(0),
        fire_on: MAX_MODEL_ROUNDS_PER_PROMPT,
    });
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(dispatcher as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    let task = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
        loop_
    });
    let loop_ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn terminated")
        .expect("the turn task did not panic");
    let events = drain_events(&mut events_rx);
    let kinds = capped_kinds(&events);

    // Both guards really were armed: the round counter reached the ceiling (the
    // dispatcher fired on the last round's dispatch, and one dispatch = one
    // round) and the token is cancelled.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        MAX_MODEL_ROUNDS_PER_PROMPT,
        "the turn ran every round up to the ceiling (so `model_rounds` was AT \
         the bound when the loop top was next reached), got {kinds:?}",
    );
    assert!(
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_cancelled(),
        "the turn token IS cancelled — this is the both-armed case, not a \
         round-ceiling turn that never saw a Stop, got {kinds:?}",
    );

    // And the cancel won.
    assert!(
        !kinds.iter().any(|k| k == "turn_capped"),
        "a Stop wins over the bound: `turn_capped` must NOT be emitted when the \
         user stopped the turn (the desktop would report an agent failure for a \
         user-initiated stop), got {kinds:?}",
    );
    assert!(
        kinds.iter().any(|k| k == "turn_end") && kinds.iter().any(|k| k == "agent_settled"),
        "the turn settled on the cancel path, got {kinds:?}",
    );
    // The cancelled batch's result is still carried (the turn did real rounds).
    assert!(
        !loop_.messages.is_empty(),
        "the bounded-and-then-stopped turn's transcript is intact"
    );
}

/// (issue #193) `batch_signature` compares `(name, arguments)` ONLY — a pair of
/// batches whose calls differ **solely in `defect`** still sign as identical.
///
/// This is deliberate and it is the stuck case: the defect is preserved, the
/// arguments are still `{}`, so the refusal the model reads back is still the
/// same refusal, and re-sending the same truncated payload produces the same
/// result. A signature that folded `defect` in would call that a NEW batch and
/// reset the run — which would un-bound exactly the 303-calls-in-19-minutes loop
/// this guard exists to stop, for the very defect this change introduces.
///
/// The behaviour was already true (the `format!` names `tc.name` and
/// `tc.arguments` and nothing else); it had no pin, and the field this commit
/// adds is the first thing that could plausibly be added to that `format!` for
/// a good reason. This test is what makes that edit loud.
#[test]
fn batch_signature_ignores_the_defect_field() {
    let clean = ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({}),
        defect: None,
    };
    let mut defective = clean.clone();
    defective.defect = Some(crate::harness::provider::ArgumentDefect {
        error: "EOF while parsing an object at line 1 column 41".to_string(),
        raw: r#"{"path": "/x", "content": "truncated he"#.to_string(),
    });
    let mut other_defect = defective.clone();
    other_defect.defect = Some(crate::harness::provider::ArgumentDefect {
        error: "expected value at line 1 column 1".to_string(),
        raw: "{not json".to_string(),
    });

    let rejected = [true];
    let clean_sig = turn::batch_signature(std::slice::from_ref(&clean), &rejected)
        .expect("a fully refused batch signs");
    let defect_sig =
        turn::batch_signature(&[defective], &rejected).expect("a defective batch signs too");
    let other_sig = turn::batch_signature(&[other_defect], &rejected).expect("and so does another");
    assert_eq!(
        clean_sig, defect_sig,
        "the same (name, arguments) with and without a defect is the SAME batch"
    );
    assert_eq!(
        defect_sig, other_sig,
        "and two DIFFERENT defects on the same (name, arguments) are the same \
         batch too — the refusal is identical either way, so the run counts"
    );

    // The half that keeps the pin honest: a differing name or argument set still
    // signs DIFFERENTLY, so the equality above is not a constant function.
    let renamed = ToolCall {
        name: "read".to_string(),
        ..clean.clone()
    };
    assert_ne!(
        clean_sig,
        turn::batch_signature(&[renamed], &rejected).expect("a refused batch signs"),
        "a different NAME still changes the signature"
    );
    let reargued = ToolCall {
        arguments: json!({ "path": "/x" }),
        ..clean.clone()
    };
    assert_ne!(
        clean_sig,
        turn::batch_signature(&[reargued], &rejected).expect("a refused batch signs"),
        "different ARGUMENTS still change the signature"
    );
    // And the `None`-on-any-accepted rule is untouched.
    assert_eq!(
        turn::batch_signature(std::slice::from_ref(&clean), &[false]),
        None,
        "one accepted call and there is no signature at all"
    );
}

/// `batch_signature`'s docblock claims the `sort()` makes the signature
/// independent of the order the provider emitted the calls in — "the difference
/// between catching a model that repeats itself and missing one that permutes" —
/// and until now **no test exercised a permutation at all**. Every existing
/// signature test is a one-call batch, where sorting is a no-op and the claim is
/// untestable; a `parts.sort()` deleted from the function would have left the
/// whole suite green while silently un-arming the guard on any multi-call batch
/// whose provider reordered (which providers do — the `subagent` batch's
/// concurrent route is one reason the loop does not control emission order).
///
/// Two DIFFERENT calls, both refused, asserted equal in both orders — and then
/// the half that keeps the equality honest: a batch that is actually a different
/// SET still signs differently, so this is not a constant function either.
#[test]
fn batch_signature_is_independent_of_the_call_order() {
    let bash_empty = ToolCall {
        id: "t1".to_string(),
        name: "bash".to_string(),
        arguments: json!({}),
        defect: None,
    };
    let read_missing = ToolCall {
        id: "t2".to_string(),
        name: "read".to_string(),
        arguments: json!({ "pathe": "typo" }),
        defect: None,
    };
    let both = [true, true];

    let forward = turn::batch_signature(&[bash_empty.clone(), read_missing.clone()], &both)
        .expect("a fully refused batch signs");
    let reversed = turn::batch_signature(&[read_missing.clone(), bash_empty.clone()], &both)
        .expect("and so does the same batch permuted");
    assert_eq!(
        forward, reversed,
        "the same two refused calls in the OTHER order are the SAME batch — this \
         is the permutation the `sort()` exists for, and the only assertion that \
         would redden if it were dropped"
    );

    // The honesty half: a batch with a DIFFERENT membership is a different
    // signature, so the equality above is a fact about order-independence and
    // not about a function that ignores its input.
    let third = ToolCall {
        name: "grep".to_string(),
        ..read_missing.clone()
    };
    assert_ne!(
        forward,
        turn::batch_signature(&[bash_empty.clone(), third], &both)
            .expect("a fully refused batch signs"),
        "swapping one call for another call changes the signature"
    );
    // And a permutation with an ACCEPTED call anywhere in it still answers `None`
    // — the batch-wide rule is order-independent too, so the reset on progress
    // cannot be lost by reordering.
    assert_eq!(
        turn::batch_signature(&[bash_empty.clone(), read_missing.clone()], &[true, false]),
        None,
        "one accepted call at the END and there is no signature"
    );
    assert_eq!(
        turn::batch_signature(&[bash_empty, read_missing], &[false, true]),
        None,
        "one accepted call at the START and there is no signature"
    );
}

/// `batch_signature` answers `None` when the two slices DISAGREE ON LENGTH.
///
/// The function `zip`ped the two, so a SHORT `refused` slice stopped the
/// all-refused scan early and the function returned `Some(sig)` for a batch whose
/// LAST call had been accepted — a false positive on the one guard whose false
/// positives end the user's prompt. Production is safe today (`run_tool_batch`
/// builds the vector to the exact length of the batch), but this is a
/// `pub(super)` function over two INDEPENDENT slices, and the mis-alignment it
/// would take to trigger is silent: no panic, no `debug_assert`, just a run that
/// counts a batch the engine mostly ACCEPTED.
///
/// Both directions are pinned, and the short-`refused` case is the one that was
/// actually wrong: two calls, one refusal flag, that flag `true`, and the batch
/// is NOT all-refused because the second call was accepted.
#[test]
fn batch_signature_refuses_to_sign_a_length_mismatch() {
    let first = ToolCall {
        id: "t1".to_string(),
        name: "bash".to_string(),
        arguments: json!({}),
        defect: None,
    };
    let second = ToolCall {
        id: "t2".to_string(),
        name: "read".to_string(),
        arguments: json!({ "path": "/x" }),
        defect: None,
    };

    // The one that was WRONG: `zip` saw the single `true` and stopped.
    assert_eq!(
        turn::batch_signature(&[first.clone(), second.clone()], &[true]),
        None,
        "a `refused` slice SHORTER than the batch must not sign — the call the \
         missing flag stands for may well have been accepted, and signing here is \
         the false positive"
    );
    // The symmetric case, for the same reason (a longer flag slice means the
    // caller is describing a batch it did not hand over).
    assert_eq!(
        turn::batch_signature(&[first.clone(), second.clone()], &[true, true, true]),
        None,
        "a `refused` slice LONGER than the batch is just as uninterpretable"
    );
    // And the empty pair, which is a length match: an EMPTY batch has nothing
    // accepted in it, so it signs the empty signature. Nothing in production
    // reaches this (an empty batch never gets here — the clean-stop return is
    // gated on `tool_calls.is_empty()`), so it is pinned as the boundary of the
    // rule rather than as behaviour anyone depends on.
    assert_eq!(
        turn::batch_signature(&[], &[]),
        Some(String::new()),
        "an empty batch is vacuously all-refused, and that is the boundary case \
         the length check must not swallow"
    );
    // The control: the same batch with a correctly-sized flag slice DOES sign,
    // so the two `None`s above came from the length and not from the calls.
    assert!(
        turn::batch_signature(&[first, second], &[true, true]).is_some(),
        "the well-formed pair still signs — the mismatch arm did not become a \
         blanket `None`"
    );
}

/// The discriminator itself, pinned DIRECTLY on `run_tool_batch`'s second return
/// value rather than inferred from a turn's provider-call count. The two
/// end-to-end tests above prove the guard fires and does not over-fire; this says
/// WHY, one call at a time, including the one shape that is the entire
/// justification for requiring `is_error` beside the `required` test.
///
/// | call | refused | why |
/// | --- | --- | --- |
/// | `bash {}` | **true** | the field report's burst: `command` is advertised `required`, absent, and `exec_bash` errors |
/// | `bash {"command": "exit 1"}` | false | well-formed; the COMMAND failed — progress, not a stuck loop |
/// | `ls {}` | false | `path` is advertised `required` and IS absent, but `exec_ls` defaults it to the `cwd` and the call SUCCEEDS. This is the shape that makes `required` alone insufficient, and `is_error` is what saves it |
/// | `write {path, content, contentt}` | **true** | the pre-dispatch extraneous-key class |
/// | `list_agents {}` | false | declares no `required` at all, and succeeds |
///
/// The `ls` row is the one to keep. A future edit that drops the `is_error`
/// conjunct "because a missing required param is obviously a refusal" reddens
/// here — and would bound every successful bare `ls` in a repeated batch.
#[tokio::test]
async fn the_refusal_predicate_is_the_required_key_and_the_error_together() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("unused".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let call = |name: &str, arguments: Value| ToolCall {
        id: format!("c-{name}"),
        name: name.to_string(),
        arguments,
        defect: None,
    };
    let clean = CancellationToken::new();

    // Each probe is its OWN batch, so one call's verdict cannot colour another's.
    let burst = loop_
        .run_tool_batch(&[call("bash", json!({}))], &clean)
        .await
        .1;
    assert_eq!(
        burst,
        vec![true],
        "`bash` with NO arguments is the burst shape and MUST count as a refusal — \
         this is the 303-call case, and at the pre-dispatch checker alone it is `false`"
    );

    let failing = loop_
        .run_tool_batch(&[call("bash", json!({ "command": "exit 1" }))], &clean)
        .await
        .1;
    assert_eq!(
        failing,
        vec![false],
        "a failing command with every required key present is NOT a refusal"
    );

    let bare_ls = loop_
        .run_tool_batch(&[call("ls", json!({}))], &clean)
        .await
        .1;
    assert_eq!(
        bare_ls,
        vec![false],
        "`ls` DECLARES `path` as required and `ls {{}}` omits it, but `exec_ls` \
         defaults it and the call SUCCEEDS — so `is_error` is what keeps a working \
         tool out of the refusal run. Drop that conjunct and this assertion reddens"
    );

    let extraneous = loop_
        .run_tool_batch(
            &[call(
                "write",
                json!({ "path": "x.txt", "content": "c", "contentt": "typo" }),
            )],
            &clean,
        )
        .await
        .1;
    assert_eq!(
        extraneous,
        vec![true],
        "the pre-dispatch extraneous-key class is still a refusal"
    );

    let no_required = loop_
        .run_tool_batch(&[call("list_agents", json!({}))], &clean)
        .await
        .1;
    assert_eq!(
        no_required,
        vec![false],
        "a tool advertising no `required` list can never be a missing-parameter refusal"
    );

    // And the standing rule: on a CANCELLED batch nothing is a refusal, even the
    // burst shape — every synthetic cancel result carries `is_error: true`, so
    // without the guard's cancel arm a Stop would grow a run.
    let stopped = CancellationToken::new();
    stopped.cancel();
    let cancelled = loop_
        .run_tool_batch(&[call("bash", json!({}))], &stopped)
        .await;
    assert_eq!(
        cancelled.1,
        vec![false],
        "cancellation beats refusal: a Stop must not arm the guard"
    );
    assert_eq!(
        cancelled.0[0]["content"][0]["text"], "cancelled",
        "and the model still reads `cancelled`, not a missing-parameter sentence"
    );
}

//! (issue #93) An unknown TOP-LEVEL tool parameter REJECTS the call: the tool
//! is never dispatched and the result is a synthetic `is_error: true` message
//! naming the offending key and the closest advertised one. These are the
//! loop-level pins for that contract (the checker itself is
//! `tool_specs::unknown_param_error`; its unit tests live in that file).
//!
//! The shape of the rejection is pinned here as `starts_with` / `contains`
//! only — the byte-for-byte pin of the sentence deliberately lives elsewhere
//! (the checker's own test), so this module does not duplicate a wording pin
//! that a reword is expected to move.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::support::*;
use crate::agent::events::RpcEvent;
use crate::agent::harness::dispatch::SubagentDispatcher;
use crate::agent::harness::provider::{FinishReason, ProviderEvent, ToolCall};
use crate::agent::harness::retry::RetryPolicy;

/// A rejected `write` returns `is_error: true` as its WHOLE result and never
/// touches the filesystem.
///
/// The call carries the unknown `contentt` ALONGSIDE the legal pair, so the
/// pre-change behaviour is a SUCCESS (which is the bug): the write ran and the
/// warning rode along as a second content block. With only the typo'd key the
/// executor would fail for the missing-`content` reason and this test would
/// pass for the wrong one.
#[tokio::test]
async fn a_rejected_call_returns_is_error_and_never_touches_the_filesystem() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "write".to_string(),
                arguments: json!({
                    "path": "rejected-by-t01.txt",
                    "content": "legal content",
                    // `contentt` is NOT advertised — the checker's whole
                    // trigger. Its legal twin stays present on purpose (see
                    // the doc above); `suggest` hits `content` by containment,
                    // so the suggestion fires.
                    "contentt": "typo",
                }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    // The scratch guard is the FIRST tuple element (#96) — bind it, never
    // `.0`-project it away: the no-dispatch proof below reads inside it.
    let (space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the rejected call's tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        is_error,
        "an unknown top-level key REJECTS the call: {result:?}"
    );
    assert_eq!(
        result["content"].as_array().unwrap().len(),
        1,
        "the rejection is the WHOLE result — no tool output, no appended block"
    );
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.starts_with("unknown param(s) for `write`"),
        "the rejection names the tool: {text}"
    );
    assert!(text.contains("`contentt`"), "it names the key: {text}");
    assert!(
        text.contains("content"),
        "and suggests the closest advertised key: {text}"
    );
    // The no-dispatch proof: the file was never written.
    assert!(
        !space.join("rejected-by-t01.txt").exists(),
        "a rejected call must not touch the filesystem"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// A rejected `subagent` call never creates a child.
///
/// This is the futures-`.filter` test: the `dispatch_subagent` futures are
/// built and `join_all`-awaited BEFORE the dispatch loop, so a rejection that
/// only lives in the loop has already paid for the child it then refuses to
/// report. The proof is the `MockDispatcher` queue — `dispatch()` pops one
/// entry per child, so a still-full queue after the turn means no child was
/// ever spawned.
#[tokio::test]
async fn a_rejected_subagent_call_never_creates_a_child() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                // `agent` is the trigger; `agentName` is the advertised twin.
                arguments: json!({ "task": "the task", "agent": "reviewer" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = Arc::new(crate::agent::harness::dispatch::MockDispatcher::new());
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::agent::subagent::SubagentCancel::none());
    // Keep the SENDER alive: dropping it would resolve the queued receiver
    // and make the pre-fix path look like a `Completed` child instead of a
    // child that was never spawned.
    std::mem::forget(tx);
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
    // Bounded: pre-change the spawned child waits on the never-resolving
    // oneshot forever, so the turn would hang — a FAIL naming the missing
    // rejection is the point, not a stall.
    tokio::time::timeout(
        Duration::from_secs(30),
        loop_.handle_prompt(&text_prompt("go")),
    )
    .await
    .unwrap_or_else(|_| panic!("the turn never settled — the call was not rejected pre-dispatch"));
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the rejected call's tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        is_error,
        "a rejected `subagent` call is an error: {result:?}"
    );
    assert_eq!(
        result["content"].as_array().unwrap().len(),
        1,
        "the rejection is the WHOLE result — no child output rides along"
    );
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.starts_with("unknown param(s) for `subagent`"),
        "the rejection names the tool: {text}"
    );
    assert!(text.contains("`agent`"), "it names the key: {text}");
    assert!(
        text.contains("agentName"),
        "and suggests the advertised twin: {text}"
    );
    // THE load-bearing line: no child was spawned.
    assert_eq!(
        dispatcher.outcomes.lock().unwrap().len(),
        1,
        "dispatch() pops one entry per child: a still-full queue proves no child was spawned"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// The `settings.json` shape the gate test pins (the wire key is
/// `filePolicy` — re-implemented locally because `policy_settings` is private
/// to `policy_gate.rs`, which this module must not edit).
fn policy_settings(reads: &str, writes: &str, shell: &str) -> String {
    format!(
        r#"{{ "filePolicy": {{ "reads": "{reads}", "writes": "{writes}", "shell": "{shell}" }} }}"#
    )
}

/// A rejected call never reaches the permission gate: no prompt for a call
/// that will not run.
///
/// The `path` is LEGAL (a well-formed string) and points OUTSIDE the
/// `space_cwd`, with `writes: "ask"` — so the old code really would have
/// prompted (a typo'd `path` would make `policy_verdict` answer `Allow` for
/// the missing-path reason and this test would pass vacuously).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_rejected_call_never_reaches_the_permission_gate() {
    // `protected_dirs()` reads `$HOME`; an empty scratch home keeps the
    // verdict `Ask` rather than `Deny` (the precedent is
    // `the_default_policy_reads_and_writes_beyond_the_boundary_in_silence`).
    let home = Tmp::new("reject-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span;
    // no other thread mutates HOME concurrently.
    unsafe { std::env::set_var("HOME", home.path()) };

    let outside = Tmp::new("reject-outside");
    let outside_path = outside.path().join("rejected.txt");

    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "write".to_string(),
                arguments: json!({
                    "path": outside_path.display().to_string(),
                    // Legal-shaped `path` OUTSIDE the boundary + the unknown
                    // key that must reject BEFORE any gate decision.
                    "confidential_extra_key": 1,
                }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let cfg = Tmp::new("cfg");
    std::fs::write(
        cfg.path().join("settings.json"),
        policy_settings("ask", "ask", "ask"),
    )
    .unwrap();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        Some(cfg.path()),
    );
    // Swap the `TestSink` for a recording one (`loop_.sink` is `pub`) — the
    // `TestSink` cannot see `permission-request`.
    loop_.sink = Arc::new(RecordingSink {
        events: sink_events.clone(),
    });
    // `bounded_prompt`'s bound, inlined: a gate that still prompts FAILS here
    // instead of hanging the suite (nobody answers the prompt).
    tokio::time::timeout(
        Duration::from_secs(30),
        loop_.handle_prompt(&text_prompt("go")),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("the turn never settled — a rejected call raised a prompt nobody answered")
    });

    let prompts: Vec<Value> = sink_events
        .lock()
        .unwrap()
        .iter()
        .filter(|(name, _)| name == "permission-request")
        .map(|(_, p)| p.clone())
        .collect();
    assert!(
        prompts.is_empty(),
        "a call that will not run must never raise a permission prompt: {prompts:?}"
    );
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the rejected call's tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "the call is rejected: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.starts_with("unknown param(s) for `write`"),
        "the rejection names the tool: {text}"
    );
    assert!(
        !outside_path.exists(),
        "and the file was not written: {}",
        outside_path.display()
    );
}

/// A cancelled turn reports `cancelled`, not a rejection — the ordering rule
/// for the SEQUENTIAL arm, end-to-end.
///
/// What is load-bearing here (and what is not):
/// - `content.len() == 1` is the DISCRIMINATOR: at HEAD the append block
///   reaches the synthetic `cancelled` result (its own comment conceded "the
///   extra line is harmless truth, so no branch"), so HEAD emits 2 blocks and
///   this assertion is RED before the change.
/// - `content[0]["text"] == "cancelled"` is GREEN on BOTH sides of the change
///   — at HEAD there is no rejection arm for it to beat, so no assertion about
///   which arm wins can be red before the arm exists. That is structural, not
///   a harness gap.
/// The PIN is the PAIR: exactly one implementation satisfies both together —
/// the cancellation arm chosen AND nothing appended. Keep both assertions in
/// this one test; deleting either deletes half of the ordering evidence (the
/// other half is `a_cancelled_subagent_call_reports_cancelled_and_not_a_rejection`,
/// which pins the same rule for the `subagent` arm deterministically).
///
/// This test exercises only the SEQUENTIAL arm (`read`) via the `ask`-blocking
/// cancellation dance; the `subagent` arm's precedence is pinned by the direct
/// `run_tool_batch` test below.
#[tokio::test]
async fn a_cancelled_turn_reports_cancelled_and_not_a_rejection() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                // The legal `ask` blocks until the turn is cancelled (the
                // `a_cancel_stops_the_remaining_tool_calls_in_a_batch` pattern).
                name: "ask".to_string(),
                arguments: json!({
                    "questions": [{ "id": "q1", "question": "Which?", "options": [{ "label": "A" }] }]
                }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "read".to_string(),
                // An unknown key, so this call WOULD be rejected on a live
                // turn — the cancelled turn must answer `cancelled` instead.
                arguments: json!({ "path": "missing.txt", "pathe": 1 }),
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
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
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
        .expect("the turn settled after the cancel");
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t2"),
    )
    .await
    .expect("the cancelled call's tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "a cancelled call is an error result");
    assert_eq!(
        result["content"][0]["text"], "cancelled",
        "a call that never ran because the user stopped the turn must not be \
         told to fix the key and retry (green on both sides of the change by \
         construction — see the doc comment; it is the PAIR that pins the order)"
    );
    assert_eq!(
        result["content"].as_array().unwrap().len(),
        1,
        "nothing is appended to a synthetic cancelled result (RED at HEAD: the \
         append block reached the cancelled result — see the doc comment)"
    );
}

/// The same ordering rule for the SUBAGENT arm, with `run_tool_batch` driven
/// DIRECTLY on an already-cancelled token — no provider, no spawned task, no
/// event choreography, so arm order is the ONLY thing that can decide
/// `content[0]`.
///
/// Why this pins the `subagent` arm's precedence and the end-to-end
/// cancellation test cannot: the batch gate
/// (`if turn.is_cancelled() { HashMap::new() } else { …the futures… }`)
/// produces an EMPTY result map for the whole batch on a cancelled turn, so
/// the subagent arm's `None` entry is reached by CANCELLATION, not by the
/// futures `.filter` — a clean precedence probe. Rejection-before-cancel
/// answers `unknown param(s) for …`; cancel-before-rejection answers
/// `cancelled`. Nothing is asserted about the dispatcher queue here on
/// purpose: with the batch gate taken, the queue stays full whether or not
/// the `.filter` exists, so a queue assertion would be vacuous (that proof is
/// `a_rejected_subagent_call_never_creates_a_child`, on a live turn).
///
/// Like its end-to-end sibling, `content[0] == "cancelled"` alone is green on
/// both sides of the change (HEAD has no rejection arm to beat — structural,
/// see the roadmap's *What could not be confirmed*); the discriminating pair
/// is `content[0] == "cancelled"` AND `content.len() == 1`, jointly RED at
/// HEAD because the append block reaches the synthetic result.
#[tokio::test]
async fn a_cancelled_subagent_call_reports_cancelled_and_not_a_rejection() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("unused".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        // A dispatcher is present so the `subagent` call is a live shape; the
        // cancelled batch gate means it is never consulted (see the doc above).
        Some(
            Arc::new(crate::agent::harness::dispatch::MockDispatcher::new())
                as Arc<dyn SubagentDispatcher>,
        ),
        vec![fake_model("m1")],
        None,
    );
    let tool_calls = vec![
        ToolCall {
            id: "t1".to_string(),
            name: "subagent".to_string(),
            // The unknown `agent` key beside the legal pair.
            arguments: json!({ "task": "the task", "agentName": "reviewer", "agent": "reviewer" }),
        },
        ToolCall {
            id: "t2".to_string(),
            name: "read".to_string(),
            arguments: json!({ "path": "missing.txt", "pathe": 1 }),
        },
    ];
    let cancel = CancellationToken::new();
    cancel.cancel(); // the turn is already stopped when the batch starts
                     // The return value is the SAME `serde_json::to_value(&result)` the
                     // `tool_execution_end` payload carries, so this pins the emitted shape
                     // without an event channel.
    let results = loop_.run_tool_batch(&tool_calls, &cancel).await;
    assert_eq!(results.len(), 2, "one result per call");
    for (i, result) in results.iter().enumerate() {
        assert_eq!(
            result["isError"], true,
            "result {i} is an error (`is_error` serialises as `isError`): {result:?}"
        );
        assert_eq!(
            result["content"][0]["text"], "cancelled",
            "result {i}: cancellation beats rejection (green on both sides by \
             construction — see the doc comment; the PAIR is the pin)"
        );
        assert_eq!(
            result["content"].as_array().unwrap().len(),
            1,
            "result {i}: nothing is appended to a synthetic cancelled result \
             (RED at HEAD — the append block reached the cancelled result)"
        );
    }
}

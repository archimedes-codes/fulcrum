use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::support::*;
use crate::events::{EventSink, RpcEvent};
use crate::harness::catalog::Model;
use crate::harness::dispatch::SubagentDispatcher;
use crate::harness::provider::{ChatRole, FinishReason, MessageContent, ProviderEvent, ToolCall};
use crate::harness::retry::RetryPolicy;
use crate::subagent::{AgentNameResolution, LaunchConfig, SubagentOutcome};
use crate::tools::ContentBlock;

/// `SubagentWait` mechanics (the `MockDispatcher` seam — a QUEUED
/// oneshot): a resolving `Completed` outcome completes the `subagent`
/// tool with the captured output (the `select!`'s outcome arm fires
/// before the turn-cancel arm).
#[tokio::test]
async fn subagent_wait_a_resolving_completed_outcome_completes_the_tool() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = crate::harness::dispatch::MockDispatcher::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::subagent::SubagentCancel::none());
    // The test resolves the QUEUED oneshot BEFORE the turn starts (the
    // oneshot's value is buffered — the loop's `select!` sees it
    // resolved immediately when it pops it at `dispatch`).
    tx.send(SubagentOutcome::Completed {
        output: "the answer".to_string(),
        metrics: crate::subagent::SubagentMetrics {
            output: "the answer".to_string(),
            input_tokens: 10,
            output_tokens: 5,
            cost: Some(0.0),
            duration_ms: 1,
        },
    })
    .unwrap();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    // The `subagent` tool result: the `Completed` output verbatim.
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(!is_error, "a `Completed` outcome is a success result");
    assert_eq!(
        result["content"][0]["text"], "the answer",
        "the tool result is the `Completed` output verbatim"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// (issue #93, narrowing issue #78) A `subagent` call carrying `agent` instead
/// of `agentName` is REJECTED before dispatch: NO child is spawned, and the
/// rejection sentence is the WHOLE tool result.
///
/// WHY THE PREMISE CHANGED. This test was
/// `a_mis_named_subagent_param_still_dispatches_and_announces_itself`. #78
/// pinned `!is_error` because its checker ran AFTER the child had been
/// dispatched: failing the call would have destroyed an expensive, already
/// COMPLETED delegation over a typo, so the only remedy #78 could reach was to
/// dispatch anyway and ANNOUNCE the mis-named key as a second text block. #93
/// moved that same verdict to a PRE-DISPATCH rejection — `run_tool_batch`
/// (`harness/loop/turn.rs`) gates the subagent futures' `.filter()` on it, so
/// the child is never spawned. A rejection therefore spends nothing: there is
/// no completed delegation left to destroy, and #78's subagent exemption has no
/// remaining justification (roadmap D1: "narrowed, not erased"). Hence the
/// inverted `is_error` and the DELETED child-output assertions — there is no
/// child output any more, the rejection IS the result.
///
/// Still on the `MockDispatcher` seam (not the `fake_worker` `WorkerManager`),
/// now for a sharper reason: the mock's queue IS the no-dispatch proof —
/// `dispatch()` pops one entry per child, so a queue that is still full after
/// the turn proves the loop never spawned one. The queued outcome is deliberately
/// RESOLVABLE (the sender is sent to, not forgotten): pre-change the child ran to
/// `Completed { output: "the answer" }`, so this test fails for the right reason
/// — a child that ran — instead of hanging on a dropped sender.
#[tokio::test]
async fn a_mis_named_subagent_param_is_rejected_and_never_dispatches_a_child() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                // `agent` is NOT advertised — the checker's whole trigger.
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task", "agent": "reviewer" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = Arc::new(crate::harness::dispatch::MockDispatcher::new());
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::subagent::SubagentCancel::none());
    // A child that was never dispatched never pops this entry — see the doc
    // comment for why it is resolvable rather than left hanging.
    tx.send(SubagentOutcome::Completed {
        output: "the answer".to_string(),
        metrics: crate::subagent::SubagentMetrics {
            output: "the answer".to_string(),
            input_tokens: 10,
            output_tokens: 5,
            cost: Some(0.0),
            duration_ms: 1,
        },
    })
    .unwrap();
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
    loop_.handle_prompt(&text_prompt("go")).await;
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        is_error,
        "a mis-named param REJECTS the call (#93). #78's `!is_error` pinned a \
         warning that could only announce a child it had already paid for; the \
         verdict now lands before dispatch, so there is nothing to be lenient \
         about: {result:?}"
    );
    assert_eq!(
        result["content"].as_array().unwrap().len(),
        1,
        "the rejection is the WHOLE result — no child output to append it to: {result:?}"
    );
    let rejection = result["content"][0]["text"]
        .as_str()
        .expect("the rejection is the first (only) text block");
    assert!(
        rejection.starts_with("unknown param(s) for `subagent`"),
        "the rejection names the tool (no `⚠`, no `— ignored.` — it is no longer \
         a warning riding inside a success): {rejection}"
    );
    assert!(rejection.contains("`agent`"), "{rejection}");
    assert!(
        rejection.contains("agentName"),
        "the suggestion is the useful half — the model needs the key to retry \
         with: {rejection}"
    );
    assert!(
        !result.to_string().contains("the answer"),
        "no child output rides anywhere in a rejected call's result: {result:?}"
    );

    // THE load-bearing line: the child was NEVER DISPATCHED. `dispatch()` pops
    // one queue entry per child, so a still-full queue is the no-spawn proof —
    // the reason #78's "an expensive delegation would be wasted" premise no
    // longer holds (nothing was spent).
    assert_eq!(
        dispatcher.outcomes.lock().unwrap().len(),
        1,
        "a rejected `subagent` call must not consume a dispatch queue entry"
    );

    // The rejection travels into the TRANSCRIPT (the message list the model is
    // re-sent next turn), not just the UI event — and it travels ALONE.
    let tool_message = loop_
        .messages
        .iter()
        .find(|m| m.role == ChatRole::Tool && m.tool_call_id.as_deref() == Some("t1"))
        .expect("the tool message for t1");
    let MessageContent::Blocks(blocks) = &tool_message.content else {
        panic!("the tool message carries content blocks");
    };
    assert_eq!(
        blocks.len(),
        1,
        "the transcript carries the rejection and nothing else — #78's two-block \
         shape (child output + announcement) is gone with the child: {blocks:?}"
    );
    let ContentBlock::Text { text } = &blocks[0] else {
        panic!("the rejection is a text block: {blocks:?}")
    };
    assert_eq!(
        text.as_str(),
        rejection,
        "the model reads the same sentence the UI event carried"
    );

    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// `dispatch` resolves after `delay`) while counting how many subagents
/// are in-flight CONCURRENTLY (the parallel-dispatch assertion — the max
/// in-flight count is N for a concurrent batch, 1 for sequential).
struct ConcurrentCountingDispatcher {
    delay: Duration,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

impl ConcurrentCountingDispatcher {
    fn new(delay: Duration) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Self {
                delay,
                in_flight: in_flight.clone(),
                max_in_flight: max_in_flight.clone(),
            },
            max_in_flight,
        )
    }
}

impl SubagentDispatcher for ConcurrentCountingDispatcher {
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
        let in_flight = self.in_flight.clone();
        let max_in_flight = self.max_in_flight.clone();
        let c = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let m = max_in_flight.load(Ordering::SeqCst);
            if c <= m {
                break;
            }
            if max_in_flight
                .compare_exchange(m, c, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let delay = self.delay;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(SubagentOutcome::Completed {
                output: "done".to_string(),
                metrics: crate::subagent::SubagentMetrics {
                    output: "done".to_string(),
                    input_tokens: 1,
                    output_tokens: 1,
                    cost: Some(0.0),
                    duration_ms: 1,
                },
            });
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
        (rx, crate::subagent::SubagentCancel::none())
    }
}

/// ADR 0025 follow-up: the `subagent` tool calls in ONE assistant
/// message run CONCURRENTLY (the model-loop tool execution batches them
/// via `join_all` — N subagents take ~1× the individual duration, not
/// N×). The assertion: the max CONCURRENT in-flight subagent count is
/// ≥ 2 (a sequential loop would peak at 1).
#[tokio::test]
async fn subagent_calls_in_one_message_run_in_parallel() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t3".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "c" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, max_in_flight) = ConcurrentCountingDispatcher::new(Duration::from_millis(50));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
    // CONCURRENT dispatch: ≥ 2 subagents were in-flight at the same
    // time (a sequential loop would peak at 1).
    assert!(
        max_in_flight.load(Ordering::SeqCst) >= 2,
        "the subagent tool calls ran concurrently (max in-flight = {}), \n\
         not sequentially (which would peak at 1)",
        max_in_flight.load(Ordering::SeqCst)
    );
}

/// A test `SubagentDispatcher` for the CANCEL-during-batch test: each
/// `dispatch` returns a NEVER-resolving oneshot (the subagent stays
/// in-flight) + a `SubagentCancel::Flag` whose flip records the cancel.
/// The test stores the watch receivers so it can assert `cancel.cancel()`
/// fired for every in-flight subagent when the turn is cancelled.
struct CancellingDispatcher {
    cancels: Arc<StdMutex<Vec<watch::Receiver<bool>>>>,
}

impl CancellingDispatcher {
    fn new() -> (Self, Arc<StdMutex<Vec<watch::Receiver<bool>>>>) {
        let cancels = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                cancels: cancels.clone(),
            },
            cancels,
        )
    }
}

impl SubagentDispatcher for CancellingDispatcher {
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
        let (flag_tx, flag_rx) = watch::channel(false);
        self.cancels.lock().unwrap().push(flag_rx);
        // The oneshot sender is FORGOTTEN (never sent, never dropped) so
        // the receiver stays PENDING — the subagent remains in-flight
        // until the turn's `select!` `turn.cancelled()` arm fires.
        let (os_tx, os_rx) = tokio::sync::oneshot::channel();
        std::mem::forget(os_tx);
        (os_rx, crate::subagent::SubagentCancel::new_flag(flag_tx))
    }
}

/// A test `SubagentDispatcher` for the ORDERING test: a subagent whose
/// `task` contains `"a"` (tool call `t1`) resolves SLOW (300 ms); one
/// whose `task` contains `"b"` (tool call `t2`) resolves FAST (20 ms) —
/// so `t2` completes FIRST. It records the COMPLETION order so the test
/// can prove the append-order reordering is real (not vacuous).
struct OutOfOrderDispatcher {
    completion_order: Arc<StdMutex<Vec<String>>>,
}

impl OutOfOrderDispatcher {
    fn new() -> (Self, Arc<StdMutex<Vec<String>>>) {
        let completion_order = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                completion_order: completion_order.clone(),
            },
            completion_order,
        )
    }
}

impl SubagentDispatcher for OutOfOrderDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::subagent::SubagentCancel,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let delay = if task.contains('a') {
            Duration::from_millis(300)
        } else {
            Duration::from_millis(20)
        };
        let completion_order = self.completion_order.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            completion_order.lock().unwrap().push(task.clone());
            let _ = tx.send(SubagentOutcome::Completed {
                output: task.clone(),
                metrics: crate::subagent::SubagentMetrics {
                    output: task.clone(),
                    input_tokens: 1,
                    output_tokens: 1,
                    cost: Some(0.0),
                    duration_ms: 1,
                },
            });
        });
        (rx, crate::subagent::SubagentCancel::none())
    }
}

/// Parallel-batch CANCEL safety: when the turn is cancelled while N
/// subagents are in-flight, EVERY in-flight subagent's `cancel.cancel()`
/// fires (the `select!`'s `turn.cancelled()` arm) and the turn settles
/// (the `join_all` unwinds through all N cancel arms — no hang on a slow
/// / never-resolving worker).
#[tokio::test]
async fn subagent_batch_a_turn_cancel_cancels_all_in_flight_subagents() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, cancels) = CancellingDispatcher::new();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    // The `join_all` batch blocks (the oneshots never resolve) until the
    // turn is cancelled — so drive it in a task.
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
    // Wait (up to `SETTLE`) for BOTH subagents to be dispatched (in-flight).
    let deadline = tokio::time::Instant::now() + SETTLE;
    while cancels.lock().unwrap().len() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        cancels.lock().unwrap().len(),
        2,
        "both subagents were dispatched (in-flight) before the cancel"
    );
    // Cancel the turn — the in-flight `select!`s must fire.
    turn_cancel.lock().unwrap().cancel();
    // The turn must SETTLE (the `join_all` unwinds through both cancel
    // arms — it does NOT hang on the never-resolving oneshots).
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn settled after the cancel (no hang)");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled");
    // BOTH in-flight subagents were cancelled (`cancel.cancel()` fired for
    // each — the watch flag flipped to `true`).
    let flipped = cancels
        .lock()
        .unwrap()
        .iter()
        .filter(|r| *r.borrow())
        .count();
    assert_eq!(
        flipped, 2,
        "every in-flight subagent's `cancel.cancel()` fired on a turn cancel"
    );
}

/// Parallel-batch ORDERING: a subagent that completes FIRST (t2, fast)
/// must still be appended AFTER a slower one (t1) — the results are
/// appended in the ORIGINAL `tool_calls` order, not completion order.
#[tokio::test]
async fn subagent_batch_results_are_appended_in_original_order_regardless_of_completion_order() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, completion_order) = OutOfOrderDispatcher::new();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    // Collect the `tool_execution_end` order (drain until `agent_settled`).
    let mut order = Vec::new();
    while let Ok(e) = events_rx.try_recv() {
        if let RpcEvent::tool_execution_end {
            ref tool_call_id, ..
        } = e
        {
            order.push(tool_call_id.clone());
        }
        if matches!(e, RpcEvent::agent_settled) {
            break;
        }
    }
    // PROVE the reordering is real: the fast one (t2 / "b") completed
    // FIRST (so appending t1-then-t2 is a genuine reordering, not a
    // vacuous pass where t1 happened to finish first).
    assert_eq!(
        *completion_order.lock().unwrap(),
        vec!["b".to_string(), "a".to_string()],
        "t2 (the fast one) completed before t1 — the append-order test is not vacuous"
    );
    assert_eq!(
        order,
        vec!["t1".to_string(), "t2".to_string()],
        "results appended in the ORIGINAL `tool_calls` order (t1 before t2), even though t2 (the fast one) completed first"
    );
}

/// Parallel-batch MIXED message: a `subagent` call + a non-`subagent`
/// tool (`ls`, ungated) in ONE assistant message — the `subagent` is
/// batched (Phase 1), the `ls` runs sequentially (Phase 2); BOTH results
/// are appended in the original order and the turn settles.
#[tokio::test]
async fn subagent_batch_mixed_with_a_non_subagent_tool_runs_both_in_order() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "ls".to_string(),
                arguments: json!({ "path": "." }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, _max) = ConcurrentCountingDispatcher::new(Duration::from_millis(20));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let mut order = Vec::new();
    while let Ok(e) = events_rx.try_recv() {
        if let RpcEvent::tool_execution_end {
            ref tool_call_id, ..
        } = e
        {
            order.push(tool_call_id.clone());
        }
        if matches!(e, RpcEvent::agent_settled) {
            break;
        }
    }
    assert_eq!(
        order,
        vec!["t1".to_string(), "t2".to_string()],
        "both the `subagent` (batched) and the `ls` (sequential) ran, in the original `tool_calls` order"
    );
}

/// A test `SubagentDispatcher` for the COMPLETED-result-survives-Stop
/// test: a subagent whose `task` contains `"a"` (t1) resolves FAST (20 ms,
/// `Completed`); one whose `task` contains `"b"` (t2) NEVER resolves
/// (stays in-flight until the turn is cancelled).
struct PartialResolveDispatcher;

impl SubagentDispatcher for PartialResolveDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::subagent::SubagentCancel,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if task.contains('a') {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = tx.send(SubagentOutcome::Completed {
                    output: "t1-done".to_string(),
                    metrics: crate::subagent::SubagentMetrics {
                        output: "t1-done".to_string(),
                        input_tokens: 1,
                        output_tokens: 1,
                        cost: Some(0.0),
                        duration_ms: 1,
                    },
                });
            });
        } else {
            // t2: the oneshot sender is FORGOTTEN (never sent) so the
            // receiver stays PENDING — t2 stays in-flight until cancelled.
            std::mem::forget(tx);
        }
        (rx, crate::subagent::SubagentCancel::none())
    }
}

/// Parallel-batch CANCEL: a subagent that COMPLETED before a mid-batch
/// Stop keeps its real (`Completed`) result — it is NOT replaced by
/// `"cancelled"` (the fix-#1 core behavior). A still-in-flight subagent
/// IS `"cancelled"`.
#[tokio::test]
async fn subagent_batch_a_completed_result_survives_a_mid_batch_stop() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(PartialResolveDispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    // The `join_all` batch blocks (t2 never resolves) until the turn is
    // cancelled — so drive it in a task.
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
    // Let t1 complete (its `Completed` result is cached in the batch);
    // t2 stays in-flight.
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Cancel the turn — t2 (in-flight) is cancelled; t1's result is cached.
    turn_cancel.lock().unwrap().cancel();
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn settled after the cancel");
    // Collect the tool results (drain until `agent_settled` — the
    // `tool_execution_end` events are emitted BEFORE the settle, so a
    // `wait_for_event` first would have consumed them and the drain
    // below would see nothing).
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut results: HashMap<String, Value> = HashMap::new();
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("agent_settled not seen within the settle bound");
        }
        match tokio::time::timeout(Duration::from_millis(100), events_rx.recv()).await {
            Ok(Some(e)) => {
                if let RpcEvent::tool_execution_end {
                    ref tool_call_id,
                    ref result,
                    ..
                } = e
                {
                    results.insert(tool_call_id.clone(), result.clone());
                }
                if matches!(e, RpcEvent::agent_settled) {
                    break;
                }
            }
            Ok(None) | Err(_) => {
                panic!("the event channel closed / timed out before agent_settled");
            }
        }
    }
    // t1 (completed before the Stop) keeps its REAL result, not "cancelled".
    assert_eq!(
        results
            .get("t1")
            .and_then(|v| v["content"][0]["text"].as_str()),
        Some("t1-done"),
        "a completed subagent's result survives a mid-batch Stop (fix #1)"
    );
    // t2 (in-flight at the Stop) is "cancelled".
    assert_eq!(
        results
            .get("t2")
            .and_then(|v| v["content"][0]["text"].as_str()),
        Some("cancelled"),
        "an in-flight subagent is 'cancelled' on a mid-batch Stop"
    );
}

/// `SubagentWait` mechanics: a `Failed` outcome is an ERROR tool
/// result (the `subagent failed: {error}` text — the parent can
/// retry / reword the task).
#[tokio::test]
async fn subagent_wait_a_failed_outcome_is_an_error_result() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = crate::harness::dispatch::MockDispatcher::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::subagent::SubagentCancel::none());
    // The test resolves the QUEUED oneshot BEFORE the turn starts
    // (the oneshot's value is buffered — the loop's `select!` sees
    // it resolved immediately when it pops it at `dispatch`).
    tx.send(SubagentOutcome::Failed {
        reason: crate::subagent::SubagentFailureReason::Other,
        error: "boom".to_string(),
        output: String::new(),
        metrics: crate::subagent::SubagentMetrics::default(),
    })
    .unwrap();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    // The `subagent` tool result: the `Failed` error, verbatim.
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "a `Failed` outcome is an error result");
    assert_eq!(
        result["content"][0]["text"], "subagent failed: boom",
        "the tool result carries the `Failed` error verbatim"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// `SubagentWait` mechanics: an UNRESOLVED outcome (the `MockDispatcher`
/// empty queue — a never-resolving oneshot) + a turn cancel → the
/// `select!`'s turn-cancel arm wins (the `SubagentCancel` is flipped
/// — a no-op here — and the tool result is `cancelled`).
#[tokio::test]
async fn subagent_wait_a_turn_cancel_cancels_the_dispatch() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            name: "subagent".to_string(),
            arguments: json!({ "task": "the task" }),
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    // The EMPTY queue — the `dispatch` returns a never-resolving
    // oneshot (the outcome arm never fires).
    let dispatcher = crate::harness::dispatch::MockDispatcher::new();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let turn_cancel_c = turn_cancel.clone();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel,
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    // The turn runs in a task (`handle_prompt` awaits the WHOLE turn
    // — the `SubagentWait` select blocks on the never-resolving
    // oneshot until the turn-cancel arm fires).
    let turn = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
    });
    // Give the turn time to reach the `SubagentWait` select, then
    // cancel (the `select!`'s turn-cancel arm fires — the
    // `SubagentCancel` is flipped + the `Cancelled` tool result).
    tokio::time::sleep(Duration::from_millis(100)).await;
    turn_cancel_c
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    turn.await.expect("the turn task completed");
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "a cancelled dispatch is an error result");
    assert_eq!(
        result["content"][0]["text"], "cancelled",
        "the `Cancelled` tool result"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled after the cancel");
}

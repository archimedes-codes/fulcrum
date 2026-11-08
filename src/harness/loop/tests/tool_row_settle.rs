//! (issue #11) The remaining exits that can orphan a `tool_call` row, one test
//! each, none of them vacuous.
//!
//! A `tool_call` row opens at `status: "in_progress"` when the model finishes
//! NAMING a call — `consume_stream`'s `ProviderEvent::ToolCall` arm emits
//! `toolcall_end` + `tool_execution_start`, and `normalize` renders either into
//! a `tool_call` frame at `in_progress`. It leaves `in_progress` ONLY on a
//! `tool_call_update` carrying `completed`/`failed`, which `normalize` derives
//! from `tool_execution_end` ALONE; there is no arm for `turn_end` /
//! `agent_settled`. So an abandoned attempt that announced calls and then
//! settled left every one of those rows spinning forever — in the desktop, a
//! Subagent frozen on a stale tool name with a climbing duration while its tool
//! count and cost keep rising.
//!
//! Task 1 shipped the fix (`close_announced_tool_calls`, reached from both
//! settle functions; the eight-site table and its vacuity derivations live in
//! `docs/roadmap/settle-announced-tool-rows.md`). This module pins the exits
//! Task 1 did not itself test: site 2 (mid-stream `Fatal`), site 3 asserted as
//! the PAIR its asymmetry was the bug, site 4 (budget exhausted), site 8 (Stop
//! pressed mid-stream — the exit the field report is about), the emission
//! ORDER, the no-double-close property, and the vacuity of the new settle path
//! when nothing was announced.
//!
//! Every test here carries a WITNESS that the exit it names was actually
//! walked, before it asserts that a row closed (the house pattern at
//! `compaction_overflow.rs::an_abandoned_attempt_that_announced_nothing_closes_nothing`).
//! A scripted provider whose error branch never fires produces a green test
//! that tested nothing, and a green test that tested nothing is worse than no
//! test: it retires the bug report while leaving the bug.
//!
//! And every test that asserts on the ORDER of the turn's emissions collects
//! the events itself instead of using `wait_for_event`, which DISCARDS the
//! events it passes over on the way to its predicate.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::stream::StreamExt;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::events::RpcEvent;
use crate::harness::provider::{
    ChatRole, FinishReason, ModelRequest, Provider, ProviderError, ProviderEvent, ToolCall,
};
use crate::harness::retry::RetryPolicy;

/// `abandoned_tool_result()`'s text (`loop/tools.rs`), pinned byte-for-byte
/// rather than re-derived: several tests already pin it and the close must not
/// silently reword it.
const ABANDONED: &str = "discarded — the model call that asked for this tool was \
                         abandoned before it ran";

/// The `(kind, payload)` view of a turn's emissions, so an assertion that
/// needs a field and an assertion that needs the order read the same list.
fn kinds(events: &[RpcEvent]) -> Vec<String> {
    events.iter().map(|e| e.kind().to_string()).collect()
}

/// The `(id, tool_name, result, is_error)` of every `tool_execution_end`, in
/// EMISSION order (the order the desktop reads them as transcript rows).
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

/// The ids of every `tool_execution_start`, in emission order — the rows that
/// OPENED at `in_progress`.
fn opens(events: &[RpcEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            RpcEvent::tool_execution_start { tool_call_id, .. } => Some(tool_call_id.clone()),
            _ => None,
        })
        .collect()
}

/// The `content[0].text` of a close's result payload.
fn close_text(result: &Value) -> Option<&str> {
    result["content"][0]["text"].as_str()
}

/// Assert the shape EVERY abandoned-attempt close must have: one close per
/// announced id, in `acc.tool_calls` order, each an honest failure carrying
/// `abandoned_tool_result()`'s text, every close BEFORE `turn_end` /
/// `agent_settled`, `turn_end.tool_results` no fatter, and no `tool`
/// transcript row pushed for a call that never dispatched.
///
/// This is the shared half of the site-3 PAIR — both arms of
/// `retry_abandoned_attempt` run through THIS function, so an edit that
/// reintroduces the asymmetry reddens one of the two named tests rather than
/// quietly passing on the arm nobody looked at.
fn assert_closed_before_the_settle(events: &[RpcEvent], loop_: &AgentLoop, path: &str) {
    let kinds = kinds(events);

    // The announce happened (otherwise "nothing closed" would be correct and
    // this test would be proving nothing).
    assert_eq!(
        opens(events),
        vec!["t1".to_string(), "t2".to_string()],
        "{path}: BOTH calls were announced at `in_progress` before the attempt \
         was abandoned, got {kinds:?}",
    );

    // One close per announced id, in the accumulator's order.
    let closed = closes(events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        vec!["t1", "t2"],
        "{path}: every announced row is closed exactly once, in `acc.tool_calls` \
         order (the order the desktop reads these frames as), got {kinds:?}",
    );

    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .unwrap_or_else(|| panic!("{path}: the turn settled with `turn_end`, got {kinds:?}"));
    let settled_at = kinds
        .iter()
        .position(|k| k == "agent_settled")
        .unwrap_or_else(|| panic!("{path}: the turn settled with `agent_settled`, got {kinds:?}"));
    for (idx, k) in kinds.iter().enumerate() {
        if k == "tool_execution_end" {
            assert!(
                idx < turn_end_at && idx < settled_at,
                "{path}: the close at {idx} must PRECEDE `turn_end` ({turn_end_at}) and \
                 `agent_settled` ({settled_at}) — `normalize` closes a row on \
                 `tool_execution_end` and on nothing that follows it, so a close after the \
                 settle is a row that still reads `in_progress`, got {kinds:?}",
            );
        }
    }

    for (id, name, result, is_error) in &closed {
        assert!(
            *is_error,
            "{path}: an abandoned call is a failed card (`status: \"failed\"`), never a \
             row that spun forever, got {id}"
        );
        assert_eq!(
            result["isError"].as_bool(),
            Some(true),
            "{path}: the result itself says it is an error, got {result}"
        );
        assert_eq!(
            close_text(result),
            Some(ABANDONED),
            "{path}: the existing `abandoned_tool_result()` text, byte-identical, \
             got {result}"
        );
        assert_eq!(
            name,
            if id == "t1" { "bash" } else { "read" },
            "{path}: the row closes under its own tool name, got {id} → {name}"
        );
    }

    // FRAMES ONLY, never transcript rows: the abandoned attempt persists no
    // assistant row, so a `tool` message would dangle with no preceding
    // assistant `tool_calls` for the provider to pair against.
    assert!(
        !loop_
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Tool)),
        "{path}: the close is a FRAME only — no `tool` transcript row is pushed for \
         a call that never dispatched, got {:?}",
        loop_.messages
    );

    // And the close adds no result to `turn_end`: the batch never ran.
    let RpcEvent::turn_end { tool_results, .. } = &events[turn_end_at] else {
        unreachable!("turn_end_at indexes a turn_end");
    };
    assert_eq!(
        tool_results.len(),
        0,
        "{path}: the close adds no entry to `turn_end.tool_results`, got {tool_results:?}",
    );
}

/// A `Provider` that announces a fixed batch of `ToolCall`s and then cancels
/// the TURN token as it reports the END of its own stream — the field shape of
/// Stop pressed while the model is streaming tool calls.
///
/// The cancel is delivered from the stream's terminal poll, not from a sleep,
/// because that is the only placement that is deterministic here. `consume_stream`
/// selects on the turn token, so once the token is cancelled two things are
/// both true and both fine: the cancel arm can win a poll and return early, or
/// the stream's own `None` ends the loop. Either way `acc.tool_calls` already
/// holds every announced call (each announce was consumed on an earlier pass,
/// while nothing was cancelled yet) and `turn.is_cancelled()` is true, so both
/// land on the SAME site — the `if turn.is_cancelled()` check immediately after
/// `consume_stream` in `loop.rs` (site 8). A test that cancelled from a timer
/// instead would race the stream and could land on a different site entirely.
///
/// Later calls reply with a plain text turn, so the provider itself never
/// hangs a test that wants a SECOND prompt. Note the caveat the in-repo
/// `CancelAfterUsageProvider` documents (`context_usage.rs:776-782`): the
/// Stop-pending guard is real — `handle_prompt` SKIPS a prompt while the turn
/// token is cancelled and then re-arms — so a test that wants a second turn
/// must re-arm the token itself. None of the tests here does.
struct CancelAfterAnnounceProvider {
    calls: Arc<AtomicU32>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    announced: Vec<ToolCall>,
}

impl CancelAfterAnnounceProvider {
    fn new(
        turn_cancel: Arc<StdMutex<CancellationToken>>,
        announced: Vec<ToolCall>,
    ) -> (Self, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        (
            Self {
                calls: calls.clone(),
                turn_cancel,
                announced,
            },
            calls,
        )
    }
}

#[async_trait::async_trait]
impl Provider for CancelAfterAnnounceProvider {
    async fn complete(
        &self,
        _req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return Ok(futures_util::stream::iter(vec![
                ProviderEvent::TextDelta("later".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ])
            .boxed());
        }
        let token = self
            .turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let announced = self.announced.clone();
        Ok(futures_util::stream::unfold(0usize, move |i| {
            let token = token.clone();
            let announced = announced.clone();
            async move {
                if i < announced.len() {
                    Some((ProviderEvent::ToolCall(announced[i].clone()), i + 1))
                } else {
                    // The Stop lands on the poll that ENDS the stream: every
                    // announce above was already consumed and accumulated.
                    token.cancel();
                    None
                }
            }
        })
        .boxed())
    }
}

/// A `Provider` that runs ONE tool batch to completion and then cancels the
/// TURN token as it opens the NEXT model call — the shape that asks "does the
/// settle re-close rows the batch already closed?".
///
/// The second call returns an EMPTY stream, so whichever arm of the model-call
/// `select!` wins, the turn settles on a cancel site (`loop.rs` site 6, or site
/// 8 with an empty accumulator — nothing was announced) and both emit the same
/// frames. The cancel therefore cannot change the assertion, only which
/// vacuous `&[]` carries it.
struct CancelOnSecondCallProvider {
    calls: Arc<AtomicU32>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    first: Vec<ProviderEvent>,
}

#[async_trait::async_trait]
impl Provider for CancelOnSecondCallProvider {
    async fn complete(
        &self,
        _req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            return Ok(futures_util::stream::iter(self.first.clone()).boxed());
        }
        self.turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        Ok(futures_util::stream::iter(Vec::new()).boxed())
    }
}

/// SITE 2 — the non-retryable fall-through of `handle_mid_stream_error`: a
/// mid-stream `ProviderError::Fatal` after the stream announced a call closes
/// the row, then the turn settles.
///
/// `Fatal` is the tier the retry ladder never sees (`retry.rs`: the catch-all
/// arm returns it verbatim, which is what keeps ADR 0045's
/// `context_overflow_is_never_retried` invariant reachable), so
/// `handle_mid_stream_error` skips both the retry arm and the overflow arm and
/// reaches `settle_error` — the exit BELOW both of those arms, which is the
/// same settle `retry_abandoned_attempt` reaches on its own abandoned attempts.
///
/// THE VACUITY WITNESS: this is a settle-with-a-close, and several OTHER sites
/// also settle with a close, so the witnesses are the exclusions that leave
/// site 2 as the only reachable one —
///   * `tool_execution_start` IS present (so the stream really was consumed and
///     announced: this is not site 7, where `complete` failed and `acc` never
///     existed — that site passes `&[]` and closes nothing);
///   * NO `auto_retry_start` and NO `auto_retry_end` (so the error did not enter
///     the ladder: not site 3, not site 4);
///   * NO `compaction_start` (so it was not the overflow recovery: not site 1);
///   * `calls == 1` (one model call: nothing was retried);
///   * NO `message_end` and no assistant row (the attempt was ABANDONED, not
///     accepted — a turn that finished normally settles through the success
///     path, which is not one of the eight).
/// Together those say the Fatal branch was the exit, not merely that a close
/// happened to appear.
#[tokio::test]
async fn a_mid_stream_fatal_error_closes_the_announced_tool_calls() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // The attempt that dies: it ANNOUNCED a tool call (a row opened at
        // `in_progress`), then hit a permanent error.
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
            }),
            ProviderEvent::Error(ProviderError::Fatal("400 bad request".to_string())),
        ]),
        // Unreachable — `Fatal` is not retried.
        Some(vec![
            ProviderEvent::TextDelta("never".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // A FIVE-attempt budget: if this error were ever mistaken for
        // retryable, the ladder would fire and the witnesses below would say
        // so. The delay is the house millisecond idiom.
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds = kinds(&events);

    // ── the witnesses: site 2 is the only exit these exclusions leave ──
    assert!(
        kinds.iter().any(|k| k == "tool_execution_start"),
        "the abandoned attempt DID announce a tool row (without this, \
         `settle_error(&[])` at site 7 would satisfy every assertion below), got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start")
            && !kinds.iter().any(|k| k == "auto_retry_end"),
        "`Fatal` never enters the retry ladder, so this settled from the \
         fall-through and NOT from sites 3/4, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "compaction_start"),
        "this was not the overflow recovery (site 1), got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "message_end") && !kinds.iter().any(|k| k == "agent_end"),
        "the attempt was abandoned, not accepted — a turn that finished is not \
         one of the eight settle sites, got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a non-retryable error makes ONE model call"
    );
    assert!(
        kinds.iter().any(|k| k == "turn_end") && kinds.iter().any(|k| k == "agent_settled"),
        "the turn settled, got {kinds:?}",
    );

    // ── the fix ──
    let closed = closes(&events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        vec!["t1"],
        "the announced row is CLOSED, not left spinning forever, got {kinds:?}",
    );
    assert!(closed[0].3, "an abandoned call is a failed card");
    assert_eq!(
        close_text(&closed[0].2),
        Some(ABANDONED),
        "the existing `abandoned_tool_result()` text, got {}",
        closed[0].2
    );
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("turn_end");
    let closed_at = kinds
        .iter()
        .position(|k| k == "tool_execution_end")
        .expect("the close");
    assert!(
        closed_at < turn_end_at,
        "the close precedes the settle (a close after `turn_end` is a row that \
         still reads `in_progress`): close at {closed_at}, turn_end at {turn_end_at}, \
         got {kinds:?}",
    );

    // The failed turn persists the prompt ONLY — no assistant reply, and no
    // `tool` row for a call that never dispatched.
    assert_eq!(
        loop_
            .messages
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Tool))
            .count(),
        0,
        "the close is a FRAME only, got {:?}",
        loop_.messages
    );
    assert_eq!(
        loop_.messages.len(),
        1,
        "a failed turn persists the prompt ONLY, got {:?}",
        loop_.messages
    );
}

/// SITE 3, asserted as the PAIR. The original bug was an asymmetry INSIDE ONE
/// FUNCTION: `retry_abandoned_attempt`'s backoff-completes path closed the
/// abandoned attempt's announced rows and its cancelled path settled and
/// RETURNED, skipping the close loop that follows the backoff. Same function,
/// same abandoned attempt, same `acc`.
///
/// So this test does not assert one path and trust the other: it walks BOTH and
/// runs the SAME `assert_closed_before_the_settle` over both event streams. A
/// future edit that reintroduces the asymmetry reddens whichever arm it broke,
/// by name — which is the property a single-arm test cannot have.
///
/// THE VACUITY WITNESS (both arms): `auto_retry_start` must be present — that
/// frame is emitted ONLY inside `retry_abandoned_attempt`, after the budget
/// answers `Some(delay)`, so it is the proof the function was entered. The two
/// arms then differ in exactly one thing: the backoff is 1 ms in the completes
/// arm (it expires) and 30 s in the cancelled arm (the cancel wins it, and the
/// turn settles instead of retrying — which is also why the long delay cannot
/// slow the suite).
#[tokio::test]
async fn both_paths_of_retry_abandoned_attempt_close_the_announced_tool_calls_identically() {
    // The script BOTH arms run: announce two calls, then fail retryably. The
    // second entry is the retry only the completes arm can reach.
    let script = || {
        ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "bash".to_string(),
                    arguments: json!({ "command": "echo hi" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "a.txt" }),
                }),
                ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ])
    };

    // ── arm A: the backoff COMPLETES (the path that always closed) ──
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls_a) = script();
    let (_space_a, mut loop_a) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task_a = tokio::spawn(async move {
        loop_a.handle_prompt(&text_prompt("hello")).await;
        loop_a
    });
    let mut events_a = collect_events_until(&mut events_rx, |e| {
        matches!(e, RpcEvent::auto_retry_start { .. })
    })
    .await;
    assert!(
        events_a
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "arm A: `retry_abandoned_attempt` WAS entered (the witness frame)",
    );
    let loop_a = tokio::time::timeout(SETTLE, task_a)
        .await
        .expect("arm A: the backoff completed and the retry settled the turn")
        .expect("arm A: the turn task did not panic");
    events_a.extend(drain_events(&mut events_rx));
    assert_eq!(
        calls_a.load(Ordering::SeqCst),
        2,
        "arm A: the abandoning call + the retry"
    );
    assert_closed_before_the_settle(&events_a, &loop_a, "arm A (backoff completes)");

    // ── arm B: the backoff is CANCELLED (the path that used to settle and
    // return, skipping the close) ──
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, calls_b) = script();
    let (_space_b, mut loop_b) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        // A backoff long enough that only the cancel can end the wait — the
        // duration is the mechanism, not a synchronisation sleep.
        RetryPolicy::new_with(3, Duration::from_secs(30)),
    );
    let task_b = tokio::spawn(async move {
        loop_b.handle_prompt(&text_prompt("hello")).await;
        loop_b
    });
    let mut events_b = collect_events_until(&mut events_rx, |e| {
        matches!(e, RpcEvent::auto_retry_start { .. })
    })
    .await;
    assert!(
        events_b
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "arm B: `retry_abandoned_attempt` WAS entered — the cancel was pressed \
         while the backoff was armed, which is the arm that used to skip the close",
    );
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    let loop_b = tokio::time::timeout(SETTLE, task_b)
        .await
        .expect("arm B: the cancel cut the 30 s backoff short and settled the turn")
        .expect("arm B: the turn task did not panic");
    events_b.extend(drain_events(&mut events_rx));
    assert_eq!(
        calls_b.load(Ordering::SeqCst),
        1,
        "arm B: the cancelled turn makes ONE model call (the close does not \
         re-call the model)"
    );
    // THE PAIR: the identical assertions, on the arm that used to fail them.
    assert_closed_before_the_settle(&events_b, &loop_b, "arm B (cancelled in the backoff)");
}

/// SITE 4 — the retry budget exhausted: `auto_retry_end { success: false }`
/// and then the settle, after a `ToolCall` announce. The attempt is abandoned
/// for good, so its rows never dispatch and must close.
///
/// THE VACUITY WITNESS: `auto_retry_end` with `success == false` — the frame
/// `retry_abandoned_attempt` emits ONLY on the exhaustion path, immediately
/// before this settle. Plus `calls == 1`: with a ONE-attempt budget the very
/// first mid-stream `Retryable` finds the budget spent, so the exhaustion arm
/// is not merely asserted, it is the only arm available.
#[tokio::test]
async fn an_exhausted_retry_budget_closes_the_announced_tool_calls() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
            }),
            ProviderEvent::Error(ProviderError::Retryable("still down".to_string())),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("never".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // ONE attempt: `call_with_retry` records attempt 1, so the first
        // `next_retry_delay` finds the budget spent and the exhaustion arm is
        // the only arm.
        RetryPolicy::new_with(1, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds = kinds(&events);

    assert!(
        events.iter().any(|e| matches!(
            e,
            RpcEvent::auto_retry_end {
                success: false,
                final_error: Some(_),
                ..
            }
        )),
        "the exhaustion frame `auto_retry_end {{ success: false }}` is present — the \
         budget-exhausted settle is the exit that was walked, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start"),
        "with a ONE-attempt budget nothing was ever re-issued (that would be site 3's \
         territory, not this one), got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "an exhausted budget makes no further model call"
    );
    assert!(
        kinds.iter().any(|k| k == "tool_execution_start"),
        "the abandoned attempt DID announce a tool row, got {kinds:?}",
    );

    let closed = closes(&events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        vec!["t1"],
        "the announced row is closed exactly once before the settle, got {kinds:?}",
    );
    assert!(closed[0].3, "an abandoned call is a failed card");
    assert_eq!(
        close_text(&closed[0].2),
        Some(ABANDONED),
        "the same honest close the retry path emits, got {}",
        closed[0].2
    );
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("turn_end");
    let closed_at = kinds
        .iter()
        .position(|k| k == "tool_execution_end")
        .expect("the close");
    assert!(
        closed_at < turn_end_at,
        "the close precedes `turn_end` ({turn_end_at}) — close at {closed_at}, got {kinds:?}",
    );
    assert_eq!(
        loop_
            .messages
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Tool))
            .count(),
        0,
        "the close is a FRAME only, got {:?}",
        loop_.messages
    );
}

/// SITE 8 — Stop pressed while the model is streaming tool calls. This is the
/// exit the field report actually describes, and it was NOT in the issue's list
/// of four: `consume_stream` has announced calls in `acc.tool_calls`, the very
/// next statement is `if turn.is_cancelled()`, and that settle used to close
/// nothing.
///
/// THE VACUITY WITNESS: the turn settled (`turn_end` + `agent_settled`) with NO
/// `message_end` and NO assistant row — the attempt was abandoned mid-stream —
/// while `tool_execution_start` says rows were opened; and NO
/// `auto_retry_start` / `auto_retry_end` / `compaction_start`, which excludes
/// sites 1, 3 and 4. The scripted stream carries no `ProviderEvent::Error` at
/// all, so the mid-stream error handler is unreachable by construction: the
/// cancel check after `consume_stream` is the only settle this shape can reach.
/// `calls == 1` closes the remaining gap (no second attempt happened).
#[tokio::test]
async fn a_stop_while_the_model_streams_tool_calls_closes_the_announced_rows() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, calls) = CancelAfterAnnounceProvider::new(
        turn_cancel.clone(),
        vec![ToolCall {
            id: "t1".to_string(),
            name: "bash".to_string(),
            arguments: json!({ "command": "echo hi" }),
        }],
    );
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds = kinds(&events);

    assert_eq!(
        opens(&events),
        vec!["t1".to_string()],
        "the stream announced a row at `in_progress`, got {kinds:?}",
    );
    assert!(
        kinds.iter().any(|k| k == "message_start"),
        "the attempt got as far as consuming its stream (the announce is proof, \
         and this says the model call itself succeeded — site 6/7 never open a \
         stream), got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start")
            && !kinds.iter().any(|k| k == "auto_retry_end")
            && !kinds.iter().any(|k| k == "compaction_start"),
        "no retry and no compaction ran, so the only settle this shape can reach is \
         the cancel check after `consume_stream`, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "message_end") && !kinds.iter().any(|k| k == "agent_end"),
        "the attempt was abandoned by the Stop, not accepted, got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the cancelled turn makes ONE model call"
    );
    assert!(
        kinds.iter().any(|k| k == "turn_end") && kinds.iter().any(|k| k == "agent_settled"),
        "the turn settled, got {kinds:?}",
    );

    let closed = closes(&events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        vec!["t1"],
        "the Stop does not leave the announced row spinning — the field report's \
         Subagent frozen on a stale tool name with a climbing duration, got {kinds:?}",
    );
    assert_eq!(
        closed[0].1, "bash",
        "the row closes under its own tool name"
    );
    assert!(closed[0].3, "an abandoned call is a failed card");
    assert_eq!(
        close_text(&closed[0].2),
        Some(ABANDONED),
        "the existing `abandoned_tool_result()` text, got {}",
        closed[0].2
    );
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("turn_end");
    let settled_at = kinds
        .iter()
        .position(|k| k == "agent_settled")
        .expect("agent_settled");
    let closed_at = kinds
        .iter()
        .position(|k| k == "tool_execution_end")
        .expect("the close");
    assert!(
        closed_at < turn_end_at && closed_at < settled_at,
        "the close precedes the settle: close at {closed_at}, turn_end at \
         {turn_end_at}, agent_settled at {settled_at}, got {kinds:?}",
    );
    assert_eq!(
        loop_.messages.len(),
        1,
        "a cancelled turn persists the prompt ONLY — no assistant reply, no tool \
         row, got {:?}",
        loop_.messages
    );
}

/// THE ORDER PIN, on a MULTI-call batch.
///
/// `TurnState::announced_tool_calls` is a `HashSet` (`normalize.rs:72`) — it is
/// insert-only and it also holds compaction card ids (`compact:N`) that
/// `compaction_end` closes rather than `tool_execution_end`, which is why the
/// helper takes the ordered `Vec<ToolCall>` and never the set. The set is
/// nonetheless the obvious "tidy" refactor, and a `HashSet` iteration order is
/// unspecified: a close emitted in hash order would render the transcript in an
/// order the model never asked for, and the desktop reads these frames as
/// transcript rows.
///
/// The ids here are declared `zzz-…`, `mmm-…`, `aaa-…` so that DECLARATION
/// order is the reverse of sorted order: an implementation that collected into
/// a set (or sorted, or reversed) cannot produce this sequence.
///
/// THE VACUITY WITNESS: three `tool_execution_start`s in the SAME declared
/// order (so the announce really happened, three times, in that order), and the
/// turn settled on the cancel site with no `message_end` / `agent_end` — the
/// same exclusion set as the site-8 test above.
#[tokio::test]
async fn the_closes_arrive_in_accumulator_order_and_every_one_precedes_the_settle() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    // Declaration order is the REVERSE of lexicographic order, and it is also
    // not any hash order a `HashSet` could be relied on to reproduce.
    let declared = ["zzz-third", "mmm-second", "aaa-first"];
    let (provider, calls) = CancelAfterAnnounceProvider::new(
        turn_cancel.clone(),
        declared
            .iter()
            .enumerate()
            .map(|(i, id)| ToolCall {
                id: (*id).to_string(),
                name: ["bash", "read", "grep"][i].to_string(),
                arguments: json!({}),
            })
            .collect(),
    );
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds = kinds(&events);

    // ── witnesses ──
    assert_eq!(
        opens(&events),
        declared.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "all THREE calls were announced, in the order the model named them, got {kinds:?}",
    );
    assert!(
        !kinds.iter().any(|k| k == "message_end")
            && !kinds.iter().any(|k| k == "auto_retry_start")
            && !kinds.iter().any(|k| k == "compaction_start"),
        "the batch was abandoned by the Stop (not accepted, not retried, not \
         compacted away), so the closes below came from the settle, got {kinds:?}",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "ONE model call");

    // ── the order itself ──
    let closed = closes(&events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        declared.to_vec(),
        "the closes arrive in `acc.tool_calls` order — NOT hash order, NOT sorted \
         order, NOT reversed (the desktop reads these frames as transcript rows), \
         got {kinds:?}",
    );
    assert_eq!(
        closed.iter().map(|c| c.1.as_str()).collect::<Vec<_>>(),
        vec!["bash", "read", "grep"],
        "each row closes under its own tool name, got {kinds:?}",
    );
    // And the order is NOT accidentally the sorted one (a `sort()` in the
    // helper would produce this, and the assertion above alone would not
    // distinguish it if the ids were declared in sorted order).
    let mut sorted: Vec<String> = declared.iter().map(|s| s.to_string()).collect();
    sorted.sort();
    assert_eq!(
        sorted[0], "aaa-first",
        "the fixture's declared order really IS the reverse of sorted order, so no \
         sorting implementation can satisfy the assertion above"
    );
    assert_ne!(
        closed.iter().map(|c| c.0.clone()).collect::<Vec<_>>(),
        sorted,
        "the closes are in DECLARATION order, not sorted order, got {kinds:?}",
    );

    // ── every close precedes both settle frames ──
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("turn_end");
    let settled_at = kinds
        .iter()
        .position(|k| k == "agent_settled")
        .expect("agent_settled");
    assert!(
        turn_end_at < settled_at,
        "`turn_end` then `agent_settled`, got {kinds:?}",
    );
    let last_close = kinds
        .iter()
        .rposition(|k| k == "tool_execution_end")
        .expect("at least one close");
    assert!(
        last_close < turn_end_at,
        "the LAST close precedes `turn_end` ({turn_end_at}) — a close after the \
         settle is a row that still reads `in_progress`, got {kinds:?}",
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "tool_execution_end").count(),
        3,
        "exactly one close per announced row, got {kinds:?}",
    );
    assert!(
        closed.iter().all(|c| c.3),
        "all three abandoned calls are failed cards"
    );
}

/// THE NO-DOUBLE-CLOSE PIN: a batch that RAN closes each call once, and the
/// settle that ends the turn afterwards must not close them again.
///
/// `close_announced_tool_calls` is now reached from both settle functions, so
/// the property that keeps it honest is the one this test walks: the batch
/// completes (`run_tool_batch` emits the real `tool_execution_end` per call),
/// the accumulator is emptied by the `std::mem::take` that hands the calls to
/// the assistant row, and the NEXT iteration's cancel reaches a settle. That
/// settle must emit nothing. A double close would render as a completed row
/// flipping to `failed` a frame after it completed.
///
/// THE VACUITY WITNESS: exactly one `tool_execution_start` and exactly one
/// `tool_execution_end` for `t1`, carrying the REAL read result (`is_error ==
/// false`, and NOT the abandoned text), one `tool` transcript row,
/// `turn_end.tool_results` of length 1, and TWO model calls — so the batch
/// demonstrably ran before the settle. The sharp half is the text: if the
/// settle re-closed the row, one of the two closes would carry
/// `abandoned_tool_result()`'s string.
#[tokio::test]
async fn a_settle_after_a_completed_batch_does_not_re_close_its_rows() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let provider = CancelOnSecondCallProvider {
        calls: Arc::new(AtomicU32::new(0)),
        turn_cancel: turn_cancel.clone(),
        first: vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "read".to_string(),
                arguments: json!({ "path": "a.txt" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ],
    };
    let calls = provider.calls.clone();
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // A real file INSIDE the Space, so the `read` dispatches for real (inside
    // the boundary → `Allow`, no gate) and produces a real result.
    std::fs::write(loop_.space_cwd.join("a.txt"), "hello from the fixture\n")
        .expect("the test file writes");
    loop_.handle_prompt(&text_prompt("read a.txt")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds = kinds(&events);

    // ── witnesses: the batch really ran, and the turn really settled ──
    assert_eq!(
        opens(&events),
        vec!["t1".to_string()],
        "the call was announced, got {kinds:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the batch ran and the loop went back to the model — the settle this test \
         cares about is the one AFTER a completed batch"
    );
    assert_eq!(
        loop_
            .messages
            .iter()
            .filter(|m| matches!(m.role, ChatRole::Tool))
            .count(),
        1,
        "the batch dispatched the call and pushed its `tool` row, got {:?}",
        loop_.messages
    );
    assert!(
        kinds.iter().any(|k| k == "turn_end") && kinds.iter().any(|k| k == "agent_settled"),
        "the turn settled, got {kinds:?}",
    );

    // ── exactly ONE close, and it is the batch's real one ──
    let closed = closes(&events);
    assert_eq!(
        closed.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
        vec!["t1"],
        "`run_tool_batch` closed the row once and the settle did NOT re-close it \
         (a second frame would flip a completed row to `failed`), got {kinds:?}",
    );
    assert!(
        !closed.iter().any(|c| close_text(&c.2) == Some(ABANDONED)),
        "no close carries the abandoned-attempt text — that string can only come \
         from `close_announced_tool_calls`, got {kinds:?}",
    );
    assert!(
        !closed[0].3,
        "the batch's own result is a success, got {}",
        closed[0].2
    );
    assert_eq!(
        closed[0].1, "read",
        "the row closes under its own tool name, got {}",
        closed[0].1
    );

    // The batch's result still rides `turn_end` — one entry, not two.
    let turn_end_at = kinds
        .iter()
        .position(|k| k == "turn_end")
        .expect("turn_end");
    let RpcEvent::turn_end { tool_results, .. } = &events[turn_end_at] else {
        unreachable!("turn_end_at indexes a turn_end");
    };
    assert_eq!(
        tool_results.len(),
        1,
        "the completed batch's result survives the settle exactly once, got \
         {tool_results:?}",
    );
}

/// THE VACUITY PIN, on the NEW settle path.
///
/// `compaction_overflow.rs::an_abandoned_attempt_that_announced_nothing_closes_nothing`
/// proves the property on the RETRY path (which `Continue`s and never settles).
/// Task 1 put a `close_announced_tool_calls` call at the top of BOTH settle
/// functions, so the property has to be re-proven where the new code actually
/// lives: a settle that receives nothing must emit nothing. The risk it guards
/// is real and not hypothetical — a "tidy" implementation could close from
/// `TurnState::announced_tool_calls` (a set that is insert-only and never
/// cleared per attempt), and then a turn that announced nothing in THIS attempt
/// would still fake a failed row for something earlier, or for a `compact:N`
/// card id that `compaction_end` owns.
///
/// Both settle functions get the case, because both changed:
///   * `settle_error`, via the budget-exhausted site. Witness:
///     `auto_retry_end { success: false }` — the exhaustion frame — plus
///     `calls == 1`.
///   * `settle_cancelled`, via the cancel-after-`consume_stream` site, on a
///     stream that announced NOTHING (the same shape as the site-8 test with an
///     empty batch). Witness: `message_start` (the stream was opened and
///     consumed) plus the settle itself, with no `message_end` and `calls == 1`.
///   * `settle_error` again, but with the insert-only set NON-empty: an earlier
///     attempt announced `t1` and the retry closed it, and the SECOND attempt
///     announces nothing before exhausting the budget. Witness:
///     `auto_retry_start` (the first attempt took the retry, which is what
///     closed `t1` and left its id in the set) + `auto_retry_end { success:
///     false }` + `calls == 2`. The assertion is that `t1` is closed ONCE — the
///     sharp form of the hazard above, because a set-driven close would re-emit
///     it here.
#[tokio::test]
async fn a_settle_with_nothing_announced_emits_no_tool_execution_end() {
    // ── `settle_error`: budget exhausted, nothing announced ──
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![ProviderEvent::Error(
        ProviderError::Retryable("boom".to_string()),
    )])]);
    let (_space_e, mut loop_e) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(1, Duration::from_millis(1)),
    );
    let task_e = tokio::spawn(async move {
        loop_e.handle_prompt(&text_prompt("hello")).await;
        loop_e
    });
    let mut events_e =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let loop_e = tokio::time::timeout(SETTLE, task_e)
        .await
        .expect("the exhausted turn settled")
        .expect("the turn task did not panic");
    events_e.extend(drain_events(&mut events_rx));
    let kinds_e = kinds(&events_e);
    assert!(
        events_e
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_end { success: false, .. })),
        "the settle path WAS the budget-exhausted one (the witness frame), got {kinds_e:?}",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "ONE model call");
    assert!(
        !kinds_e.iter().any(|k| k == "tool_execution_end"),
        "nothing was announced, so the new settle path invents no synthetic close, \
         got {kinds_e:?}",
    );
    assert!(
        !kinds_e.iter().any(|k| k == "message_retracted"),
        "nothing streamed, so nothing is retracted, got {kinds_e:?}",
    );
    assert!(
        !loop_e
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Tool)),
        "and no `tool` row is pushed either, got {:?}",
        loop_e.messages
    );

    // ── `settle_cancelled`: Stop mid-stream, nothing announced ──
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (provider, calls) = CancelAfterAnnounceProvider::new(turn_cancel.clone(), Vec::new());
    let (_space_c, mut loop_c) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let task_c = tokio::spawn(async move {
        loop_c.handle_prompt(&text_prompt("hello")).await;
        loop_c
    });
    let mut events_c =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let loop_c = tokio::time::timeout(SETTLE, task_c)
        .await
        .expect("the cancelled turn settled")
        .expect("the turn task did not panic");
    events_c.extend(drain_events(&mut events_rx));
    let kinds_c = kinds(&events_c);
    assert!(
        kinds_c.iter().any(|k| k == "message_start"),
        "the model call succeeded and its stream WAS consumed (so this reached the \
         cancel check after `consume_stream`, rather than settling before the \
         accumulator existed), got {kinds_c:?}",
    );
    assert!(
        kinds_c.iter().any(|k| k == "turn_end") && kinds_c.iter().any(|k| k == "agent_settled"),
        "the turn settled on the cancel path, got {kinds_c:?}",
    );
    assert!(
        !kinds_c.iter().any(|k| k == "message_end"),
        "the attempt was abandoned, not accepted, got {kinds_c:?}",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "ONE model call");
    assert!(
        !kinds_c.iter().any(|k| k == "tool_execution_end"),
        "an empty batch settles without a single close — `close_announced_tool_calls` \
         iterates the `Vec`, it does not read the insert-only \
         `TurnState::announced_tool_calls` set, got {kinds_c:?}",
    );
    assert!(
        !loop_c
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Tool)),
        "and no `tool` row is pushed either, got {:?}",
        loop_c.messages
    );

    // ── `settle_error`, with the set NON-empty from an EARLIER attempt ──
    //
    // The sharp case. `TurnState::announced_tool_calls` is insert-only and is
    // never cleared per attempt, so an implementation that closed from the set
    // would re-close the PREVIOUS attempt's id at this settle — a row that the
    // abandon path had already closed flipping back to `failed` a frame after
    // it was closed, once per settle. THIS attempt announced nothing, so the
    // settle must add nothing: exactly one close for `t1` across the whole
    // turn, and it is the abandon path's.
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // Attempt 1: announces `t1`, then fails retryably — the retry closes it.
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
            }),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        // Attempt 2: announces NOTHING and exhausts the budget → `settle_error`.
        Some(vec![ProviderEvent::Error(ProviderError::Retryable(
            "still down".to_string(),
        ))]),
    ]);
    let (_space_s, mut loop_s) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // TWO attempts: the first `Retryable` spends the budget on a retry, the
        // second finds it exhausted and settles.
        RetryPolicy::new_with(2, Duration::from_millis(1)),
    );
    let task_s = tokio::spawn(async move {
        loop_s.handle_prompt(&text_prompt("hello")).await;
        loop_s
    });
    let mut events_s =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let loop_s = tokio::time::timeout(SETTLE, task_s)
        .await
        .expect("the exhausted turn settled")
        .expect("the turn task did not panic");
    events_s.extend(drain_events(&mut events_rx));
    let kinds_s = kinds(&events_s);
    assert!(
        events_s
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_end { success: false, .. })),
        "the settle path WAS the budget-exhausted one (the witness frame), got {kinds_s:?}",
    );
    assert!(
        events_s
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "the FIRST attempt really did take the retry (that is what closed `t1` and \
         left its id in the insert-only set), got {kinds_s:?}",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the abandoning call + the retry, and nothing after"
    );
    assert_eq!(
        closes(&events_s)
            .iter()
            .map(|c| c.0.as_str())
            .collect::<Vec<_>>(),
        vec!["t1"],
        "`t1` is closed ONCE — by the abandon path — and the settle of the attempt \
         that announced NOTHING adds no second close (this is exactly the failure \
         mode of closing from the insert-only `announced_tool_calls` set), got {kinds_s:?}",
    );
    assert!(
        !loop_s
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::Tool)),
        "and no `tool` row is pushed, got {:?}",
        loop_s.messages
    );
}

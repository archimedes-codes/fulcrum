use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::stream::StreamExt;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::CompactionRan;
use super::support::*;
use crate::agent::events::EventSink;
use crate::agent::harness::catalog::{CompactionConfig, Model};
use crate::agent::harness::compact::{self, Compactor};
use crate::agent::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ModelRequest, Provider, ProviderError,
    ProviderEvent, ToolCall, Usage,
};
use crate::agent::harness::retry::RetryPolicy;

// ── `context_usage_update` frames (the frontend's context-percentage
// display: the session's current context size vs the model's window —
// emitted via the sink, NOT the `normalize`/`persist_update` pipeline
// (a bookkeeping frame, like the client-synthesized
// `config_option_update`)). ──

/// The `updates` of the LAST `context_usage_update` frame (the tests
/// assert on the most recent frame — the frames are cumulative).
fn last_context_usage_frame(updates: &StdMutex<Vec<Value>>) -> Option<Value> {
    updates
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .rev()
        .find(|u| u["sessionUpdate"] == "context_usage_update")
        .cloned()
}

/// A provider `Usage` event re-anchors the context to what the provider holds:
/// `input + output` — the full prompt INCLUDING the reply it just
/// produced — plus anything appended since (the same number pi counts),
/// and emits a `context_usage_update` frame (the window is the session
/// model's `context_window`).
#[tokio::test]
async fn a_usage_event_emits_a_context_usage_update_frame() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 4321,
                output_tokens: 100,
            },
            None,
        ),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let frame =
        last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
    assert_eq!(
        frame["usedTokens"], 4421,
        "the context is the usage anchor (input + output)"
    );
    assert_eq!(frame["windowTokens"], 128000, "the window is the model's");
}

/// A model call that reports NO `Usage` (a proxy that strips the
/// usage chunk — the pre-`stream_options` openai-completions wire,
/// or a non-conformant endpoint) must still move the context bar:
/// the turn falls back to the LOCAL estimate of the transcript, so
/// the display — and, through the same `Compactor`, the compaction
/// threshold — track the growing context instead of freezing at the
/// value the session started with.
#[tokio::test]
async fn a_model_call_without_usage_falls_back_to_the_local_estimate() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    // A BIG prompt, and a stream that never reports usage.
    let prompt = "x".repeat(4000);
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt(&prompt)).await;
    let frame =
        last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
    assert!(
        frame["usedTokens"].as_u64().unwrap_or(0) > 900,
        "a usage-less turn re-estimates from the transcript (the prompt is \
         ~1000 tokens), got {}",
        frame["usedTokens"]
    );
    assert_eq!(
        frame["usedTokens"],
        loop_.compactor.context_tokens(),
        "the fallback estimate is the SAME source the compaction threshold reads"
    );
}

/// The P1 review fix: a usage-less turn must never move the context
/// accounting BACKWARDS. A session that mixes reporting and
/// non-reporting calls (a proxy that emits the usage chunk on SOME
/// responses) used to risk having the authoritative usage-based count
/// replaced by the smaller `chars / 4` transcript estimate on the gapped
/// calls — the bar visibly shrinking and the compaction threshold metric
/// dropping (compaction delayed while the session grew). The guarantee is
/// now STRUCTURAL rather than a floor-raiser: the anchor only moves when
/// the provider reports, and a gapped `refresh` merely adds the trailing
/// estimate on top of it, so it cannot shrink the accounting.
#[tokio::test]
async fn a_usage_less_turn_never_lowers_the_context_accounting() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    // Turn 1 reports usage (the authoritative, LARGE count); turn 2
    // reports NONE and its transcript estimate is far smaller.
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("one".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 40_000,
                    output_tokens: 100,
                },
                None,
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("two".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("first")).await;
    let after_usage = last_context_usage_frame(&updates).expect("frame after turn 1");
    let authoritative = after_usage["usedTokens"].as_u64().unwrap();
    assert_eq!(
        authoritative, 40_100,
        "turn 1 anchors on the provider's report (input + output)"
    );
    loop_.handle_prompt(&text_prompt("second")).await;
    let after_gap = last_context_usage_frame(&updates).expect("frame after turn 2");
    assert!(
        after_gap["usedTokens"].as_u64().unwrap_or(0) >= authoritative,
        "the usage-less turn must not shrink the bar below the anchor: {} < {authoritative}",
        after_gap["usedTokens"]
    );
    assert!(
        loop_.compactor.context_tokens() >= authoritative,
        "the compaction threshold metric must not drop on a usage-less turn \
         (it would delay compaction), got {}",
        loop_.compactor.context_tokens()
    );
}

/// A model whose compaction threshold is exactly `context_window -
/// 16384` (the DEFAULT reserve the `Compactor` is built with — the
/// config is fixed at `AgentLoop::new`, so the knob a test can turn
/// AFTER construction is the window): a threshold of `t` is a window of
/// `16384 + t`.
fn model_with_threshold(id: &str, threshold: u32) -> Model {
    Model {
        context_window: 16_384 + threshold,
        ..fake_model(id)
    }
}

/// The tool batch must be MEASURED before the next threshold check.
///
/// `should_compact()` is read at the TOP of the iteration, so anything
/// appended after the last re-anchor is invisible to it — and tool
/// results are by far the largest messages in an agentic transcript (a
/// `read`, a `grep`, a `cat` of a doc set). Pre-fix the batch was pushed
/// without re-anchoring, so the ENTIRE batch's output was unmeasured when
/// the next check ran: on a usage-less endpoint (anchor 0 — the fallback
/// path that exists precisely for those endpoints) the metric lagged a
/// whole batch, and one batch of huge output could blow past the window
/// undetected so the provider 400s instead of compacting.
///
/// The arithmetic: threshold 100 (window `16384 + 100`, the default
/// 16384 reserve). Prompt `"go"` = 2/4 + 1 = `1`, the assistant's `"ok"`
/// + one tool call = 0 + 20 + 1 = `21` → 22 measured at the top of the
/// next iteration: under the threshold. The `read` result is 1200 chars =
/// 300 + 1 = `301` → the batch takes the context to 323, clearly over it.
/// So the SECOND iteration must see the compaction due — which it can
/// only do if the batch was measured when it was appended.
#[tokio::test]
async fn a_tool_batch_is_measured_before_the_next_compaction_check() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    // NO `Usage` anywhere — the anchor stays 0 and the local estimate is
    // the ONLY measurement (the usage-less-endpoint case).
    let (provider, calls) = ScriptedProvider::new(vec![
        // Call 1: one tool call whose result is huge.
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "read".to_string(),
                arguments: json!({ "path": "big.txt" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        // The summary call (the compaction the batch must trigger).
        Some(vec![
            ProviderEvent::TextDelta("SUMMARY".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        // The turn's final reply.
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![model_with_threshold("m1", 100)],
        None,
    );
    // Keep only 1 token of tail, so the compaction is a REAL one (the
    // older messages are summarized) rather than a skip.
    loop_.catalog.compaction.keep_recent_tokens = 1;
    // The tool's input: a 1200-char file (300 tokens by the local
    // estimate) the `read` returns verbatim.
    std::fs::write(loop_.space_cwd.join("big.txt"), "x".repeat(1200)).unwrap();
    loop_.handle_prompt(&text_prompt("go")).await;

    let kinds = event_kinds(&mut events_rx);
    assert!(
        kinds.contains(&"compaction_start".to_string()),
        "the tool batch (301 tokens) takes the context past the threshold, so the \
         NEXT iteration's `should_compact()` must be due — the batch was appended \
         without re-anchoring and stayed invisible, got {kinds:?}"
    );
    assert!(
        kinds.contains(&"compaction_end".to_string()),
        "the compaction ran to completion, got {kinds:?}"
    );
    assert!(
        kinds.contains(&"agent_settled".to_string()),
        "the turn still settles, got {kinds:?}"
    );
    // The batch is ALSO what the display saw: the frames recorded it
    // (the prompt alone would have been a ~1-token bar).
    let frame =
        last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
    assert!(
        frame["usedTokens"].as_u64().unwrap_or(0) > 100,
        "the batch is above the threshold in the frame too, got {}",
        frame["usedTokens"]
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the turn is the tool call, the summary, and the final reply"
    );
}

/// A large PASTED prompt must be counted before the turn's first
/// threshold check (the same one-call lag as the tool batch, on the other
/// side of the turn): the push used to leave the `Compactor` untouched, so
/// compaction that was ALREADY due was deferred by a whole model call —
/// the call that had to send the oversized context to the provider.
///
/// Threshold 100, prompt 4000 chars = 1000 + 1 = `1001` — the prompt ALONE
/// crosses it, on a provider that reports NO usage. Post-fix the very
/// first thing after `turn_start` is the compaction (nothing older to
/// summarize yet, so it is a skip — the point is that the threshold READ
/// is due), and the FIRST frame of the turn is the prompt's own estimate
/// and nothing else (pre-fix it would be the prompt PLUS the assistant
/// reply, because the first frame only came after that reply was pushed).
#[tokio::test]
async fn a_large_prompt_is_measured_before_the_first_compaction_check() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let prompt_text = "x".repeat(4000);
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![model_with_threshold("m1", 100)],
        None,
    );
    loop_.handle_prompt(&text_prompt(&prompt_text)).await;

    // The prompt's own local estimate (the same function the threshold
    // uses): 4000 / 4 + 1 for the role overhead.
    let prompt_estimate = compact::estimate_context(&loop_.messages[..1]);
    assert_eq!(prompt_estimate, 1001, "the prompt alone is 1001 tokens");
    let kinds = event_kinds(&mut events_rx);
    // The compaction check fired BEFORE the first model call — i.e. the
    // prompt was in the number the threshold read.
    let compaction_at = kinds.iter().position(|k| k == "compaction_start");
    let first_model_call = kinds.iter().position(|k| k == "message_start");
    assert!(
        matches!(
            (compaction_at, first_model_call),
            (Some(c), Some(m)) if c < m
        ),
        "a prompt that alone crosses the threshold must be due at the turn's FIRST \
         check, before the oversized context is sent to the provider, got {kinds:?}"
    );
    let frames: Vec<Value> = updates
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .rev()
        .filter(|u| u["sessionUpdate"] == "context_usage_update")
        .cloned()
        .collect();
    let first = frames
        .last()
        .expect("the prompt push itself emitted a context_usage_update frame");
    assert_eq!(
        first["usedTokens"].as_u64(),
        Some(prompt_estimate),
        "the prompt was measured the moment it was pushed — the frame at that point \
         is the prompt alone, got {}",
        first["usedTokens"]
    );
    // One source, one number: the LAST frame is what the threshold reads.
    let last = frames.first().expect("at least one frame");
    assert_eq!(
        last["usedTokens"].as_u64(),
        Some(loop_.compactor.context_tokens()),
        "the frame and the compaction threshold read ONE number"
    );
}

/// The turn's opening event opens the turn: the prompt-time context frame is
/// session-scoped bookkeeping, so it belongs AFTER `turn_start`, not before
/// it. The measurement itself stays where it must be — before the turn's
/// first `should_compact()` read.
#[tokio::test]
async fn the_context_frame_of_a_turn_follows_turn_start() {
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let log = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(OrderingSink {
        log: log.clone(),
        events: Arc::new(StdMutex::new(events_rx)),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![model_with_threshold("m1", 100)],
        None,
    );
    loop_.handle_prompt(&text_prompt(&"x".repeat(4000))).await;
    let log = log.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let turn_start_at = log
        .iter()
        .position(|k| k == "turn_start")
        .expect("the turn opened with `turn_start`");
    let frame_at = log
        .iter()
        .position(|k| k == "context_usage_update")
        .expect("the prompt push emitted the context frame");
    assert!(
        turn_start_at < frame_at,
        "every turn opens with its own `turn_start`, THEN the context frame that \
         describes it — not a frame ahead of the turn it belongs to: {log:?}"
    );
    // And the real guarantee (commit `a846abb`) survives the reordering: the
    // prompt was measured before the threshold was read, so the compaction
    // check fires before the oversized context is sent.
    let compaction_at = log
        .iter()
        .position(|k| k == "compaction_start")
        .expect("a prompt that alone crosses the threshold is due at the FIRST check");
    let model_call_at = log
        .iter()
        .position(|k| k == "message_start")
        .expect("the model call");
    assert!(
        compaction_at < model_call_at,
        "the compaction is checked BEFORE the oversized context goes out: {log:?}"
    );
}

/// A turn whose model call REPORTED usage must emit ONE
/// `context_usage_update` frame, not two. The `Usage` arm is the
/// authoritative re-anchor (it sets the number from the provider's own
/// report and emits), and by the time the assistant message is pushed the
/// watermark equals the transcript length — the trailing slice is EMPTY,
/// so `note_context`'s refresh recomputes the SAME number. Re-emitting it
/// is a duplicate sink frame AND a duplicate `record_context_usage` (an IPC
/// round trip → an `ensure_session_row` SELECT + an `UPDATE` on the main
/// process's `Db` mutex), once per iteration, for nothing — and this is the
/// COMMON path (every usage-reporting endpoint).
#[tokio::test]
async fn a_turn_that_reported_usage_emits_the_context_frame_once() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 4321,
                output_tokens: 100,
            },
            None,
        ),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let frames: Vec<Value> = updates
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|u| u["sessionUpdate"] == "context_usage_update")
        .cloned()
        .collect();
    assert_eq!(
        frames.len(),
        2,
        "two REAL changes — the prompt push, then the provider's report — and the \
         assistant push that follows the report changes NOTHING, so it must not write a \
         frame (nor a `sessions` row): {frames:?}"
    );
    assert_eq!(
        frames[1]["usedTokens"], 4421,
        "the last frame is the provider's own anchor"
    );
    assert_ne!(
        frames[0]["usedTokens"], frames[1]["usedTokens"],
        "no two frames of a turn carry the same number"
    );
    assert_eq!(
        loop_.compactor.context_tokens(),
        4421,
        "and the threshold reads the same number"
    );
}

/// …while a turn that reported NO usage still moves the number (the
/// assistant push grows the estimate), so its frame is untouched by the
/// emit-if-changed guard above.
#[tokio::test]
async fn a_turn_without_usage_emits_a_frame_per_context_change() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("a somewhat longer reply".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;
    let frames: Vec<Value> = updates
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|u| u["sessionUpdate"] == "context_usage_update")
        .cloned()
        .collect();
    assert_eq!(
        frames.len(),
        2,
        "the prompt push and the assistant push are two REAL changes on a \
         usage-less endpoint — both must still be recorded: {frames:?}"
    );
    let prompt_only = frames[0]["usedTokens"].as_u64().unwrap();
    let with_reply = frames[1]["usedTokens"].as_u64().unwrap();
    assert!(
        with_reply > prompt_only,
        "the reply grew the estimate ({prompt_only} → {with_reply})"
    );
}

/// A `Provider` whose FIRST call reports a `Usage` and then STALLS
/// forever, cancelling the TURN token the moment the loop asks for the
/// next event: the stream-consume `select!` takes the `Usage` (the one
/// event that lands — nothing is cancelled yet, so that arm is
/// unopposed) and then the cancel arm wins deterministically (the stream
/// never becomes ready again) → `settle_cancelled` with NO assistant
/// message pushed, which is exactly what a Stop does to a partial reply.
/// Later calls reply normally with no usage (the turn AFTER the cancel).
struct CancelAfterUsageProvider {
    calls: AtomicU32,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
}

#[async_trait::async_trait]
impl Provider for CancelAfterUsageProvider {
    async fn complete(
        &self,
        _req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return Ok(futures_util::stream::iter(vec![
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ])
            .boxed());
        }
        let token = self
            .turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        Ok(futures_util::stream::unfold(0u8, move |s| {
            let token = token.clone();
            async move {
                match s {
                    0 => Some((
                        ProviderEvent::Usage(
                            Usage {
                                input_tokens: 90,
                                output_tokens: 50,
                            },
                            None,
                        ),
                        1u8,
                    )),
                    // The loop asks for the NEXT event: that is when the
                    // Stop lands, so the usage above is always recorded
                    // before the turn settles.
                    _ => {
                        token.cancel();
                        std::future::pending::<()>().await;
                        None
                    }
                }
            }
        })
        .boxed())
    }
}

/// A turn that ENDS WITHOUT the assistant message (here: the provider
/// reported a `Usage`, then the stream died and the retry budget
/// exhausted) leaves the `Compactor`'s watermark pointing PAST the reply
/// that was never pushed — a failed (or cancelled) turn deliberately does
/// NOT persist a partial reply. The next turn's prompt then lands INSIDE
/// the watermark window, so it is excluded from the trailing estimate: the
/// very lag commit `a846abb` exists to prevent, resurrected for the
/// post-failure case, and the anchor still carries the discarded reply's
/// output.
///
/// The arithmetic: threshold 300 (`model_with_threshold`), prompt `"go"`
/// = 1, the failed call reports `input 90 + output 50` = a 140 anchor over
/// a 1-message transcript, so the watermark claims 2 messages. The NEXT
/// prompt is 4000 chars = 1001. Left unreconciled the snapshot is the bare
/// 140 — under the 300 threshold, so no compaction and the oversized
/// context goes to the provider. Reconciled it is `90 + 1001 = 1091`
/// (the discarded 50 of output is gone with the reply) and the SECOND
/// turn's first threshold check is due.
#[tokio::test]
async fn a_failed_turn_still_measures_the_next_prompt() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    // Call 1 (turn 1): a usage report, then `Done(Error)` — with a
    // ONE-attempt budget the turn settles FAILED after that single call,
    // never pushing a reply. Call 2 (turn 2): the compaction SUMMARY (the
    // window is 16 684 against the default 16 384 reserve, so the derived
    // keep-recent budget is 0 and the due compaction is a REAL one — see
    // `Compactor::keep_budget`). Call 3 (turn 2): an ordinary usage-less
    // reply.
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 90,
                    output_tokens: 50,
                },
                None,
            ),
            ProviderEvent::Done(FinishReason::Error),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("SUMMARY".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // ONE attempt: the mid-stream error exhausts the budget at once,
        // so turn 1 is exactly one model call (no backoff sleeps).
        RetryPolicy::new_with(1, Duration::from_millis(1)),
        sink,
        None,
        vec![model_with_threshold("m1", 300)],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    assert!(
        !event_kinds(&mut events_rx).contains(&"compaction_start".to_string()),
        "the failed turn itself is not due (140 < 300)"
    );
    assert_eq!(
        loop_.messages.len(),
        1,
        "a failed turn persists the prompt ONLY — no assistant reply"
    );

    loop_.handle_prompt(&text_prompt(&"x".repeat(4000))).await;

    let kinds = event_kinds(&mut events_rx);
    assert!(
        kinds.contains(&"compaction_start".to_string()),
        "the SECOND turn must measure the prompt its predecessor's watermark \
         swallowed — an abandoned reply must not make a 1091-token context look \
         like a 140-token one, got {kinds:?}"
    );
    let prompt_estimate = compact::estimate_context(&loop_.messages[1..2]);
    assert_eq!(prompt_estimate, 1001, "the prompt alone is 1001 tokens");
    assert!(
        compact::is_compaction_summary(&loop_.messages[0]),
        "the due compaction SHRANK the transcript — the summary leads, and the prompt \
         it kept (a budget of 0 keeps the last message) follows it"
    );
    let frames: Vec<Value> = updates
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|u| u["sessionUpdate"] == "context_usage_update")
        .cloned()
        .collect();
    assert!(
        frames
            .iter()
            .any(|f| f["usedTokens"].as_u64() == Some(90 + prompt_estimate)),
        "the prompt is measured on top of the ANCHOR LESS the discarded reply's \
         output (90 + {prompt_estimate}), got {frames:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "one failed call, then turn 2's summary and reply — the measurement reconcile \
         still retries nothing",
    );
}

/// The same reconcile on the CANCEL exit (the case that motivated it — a
/// Stop drops the partial reply, it is never persisted). Same arithmetic
/// as the failed-turn test: the reported `output_tokens` of the reply that
/// never landed must leave the anchor, and the watermark must pull back so
/// the next prompt is measured.
#[tokio::test]
async fn a_cancelled_turn_still_measures_the_next_prompt() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let provider = CancelAfterUsageProvider {
        calls: AtomicU32::new(0),
        turn_cancel: turn_cancel.clone(),
    };
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![model_with_threshold("m1", 300)],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    assert!(
        !event_kinds(&mut events_rx).contains(&"compaction_start".to_string()),
        "the cancelled turn itself is not due (140 < 300)"
    );
    assert_eq!(
        loop_.messages.len(),
        1,
        "a cancelled turn persists the prompt ONLY — the partial reply is NOT \
         persisted, which is what makes the watermark wrong"
    );
    // The provider cancelled the TURN token (that is how it makes the cancel
    // arm win), so the test re-arms it before the next prompt. It has to:
    // `handle_prompt` does install a fresh token, but only AFTER its
    // Stop-pending guard — which sees the cancelled token, drains the queue,
    // settles, and SKIPS the prompt. In the live loop `run()` re-arms a
    // stale idle token between turns, so a later prompt is a FRESH turn.
    *turn_cancel.lock().unwrap_or_else(|p| p.into_inner()) = CancellationToken::new();

    loop_.handle_prompt(&text_prompt(&"x".repeat(4000))).await;

    let kinds = event_kinds(&mut events_rx);
    assert!(
        kinds.contains(&"compaction_start".to_string()),
        "the prompt after a cancelled turn must be measured before that turn's \
         first threshold check, got {kinds:?}"
    );
    let prompt_estimate = compact::estimate_context(&loop_.messages[1..2]);
    let frames: Vec<Value> = updates
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|u| u["sessionUpdate"] == "context_usage_update")
        .cloned()
        .collect();
    assert!(
        frames
            .iter()
            .any(|f| f["usedTokens"].as_u64() == Some(90 + prompt_estimate)),
        "the anchor dropped the cancelled reply's output (90 + {prompt_estimate}), \
         got {frames:?}"
    );
}

/// The reconcile is needed INSIDE the turn as well: the mid-stream retry
/// loop-back is the one path that returns to `should_compact()` WITHOUT
/// exiting the turn, so the settle paths' coverage cannot reach it. A
/// stream that reports a `Usage` and THEN dies retryably takes that branch
/// — it pushes NO assistant message (the partial reply is discarded and
/// re-generated), yet the watermark `record_usage` wrote still claims the
/// reply landed and the anchor still carries the discarded reply's
/// `output_tokens`. Pre-fix the very next iteration's threshold check read
/// that phantom output — tokens the provider generated for a reply it threw
/// away — so a dying reply large enough to cross `window - reserve`
/// compacted the session BEFORE the retry's own (successful) call:
/// "Compacting context…" for a context nobody holds, the exact symptom this
/// metric exists to kill.
///
/// The arithmetic: threshold 100 (`model_with_threshold`), and the dying
/// call reports `input 90 + output 50` = a 140 anchor over a 1-message
/// transcript. So the phantom 140 is OVER the threshold while the honest 90
/// is UNDER it: the observable is `compaction_start` itself, not an
/// internal field.
#[tokio::test]
async fn a_retried_model_call_does_not_compact_for_the_reply_it_threw_away() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    // Call 1: a usage report, THEN the stream dies retryably
    // (`Done(Error)`) — the reply is discarded, no assistant message is
    // pushed. Call 2: the retry succeeds.
    let (provider, calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 90,
                    output_tokens: 50,
                },
                None,
            ),
            ProviderEvent::Done(FinishReason::Error),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // TWO attempts: the mid-stream error retries ONCE and the retry
        // succeeds (a 1 ms backoff), so the turn ends CLEANLY — no
        // `settle_error` to reconcile for it.
        RetryPolicy::new_with(2, Duration::from_millis(1)),
        sink,
        None,
        vec![model_with_threshold("m1", 100)],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    let kinds = event_kinds(&mut events_rx);
    assert!(
        !kinds.contains(&"compaction_start".to_string()),
        "the threshold must NOT read the 50 tokens of output the retry threw away: the \
         context is 90 (not due at 100), the phantom is 140 (due) — a compaction here \
         was triggered by a reply that was never kept: {kinds:?}"
    );
    assert!(
        kinds.contains(&"auto_retry_start".to_string()),
        "the dying stream took the mid-stream retry — the loop-back under test: {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the retry really re-issued the call"
    );
    // …and the same number the threshold just read is the one the bar shows.
    assert_eq!(
        loop_.compactor.context_tokens(),
        90 + compact::estimate_context(&loop_.messages[1..]),
        "the anchor is the SURVIVING reply on the 90 the provider still holds, not \
         140 (the discarded attempt's output)"
    );
}

/// `load_transcript` (a resume) re-estimates the context from the
/// loaded messages and emits the frame — a resumed session shows its
/// context percentage BEFORE the first turn.
#[tokio::test]
async fn load_transcript_emits_a_context_usage_update_from_the_reestimate() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.load_transcript(vec![
        ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("u1".to_string()),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text("a1".to_string()),
            tool_call_id: None,
            tool_calls: None,
        },
    ]);
    let frame =
        last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
    // "u1" / "a1": 2 chars / 4 = 0 each +1 (the role overhead) = 2.
    assert_eq!(frame["usedTokens"], 2, "the re-estimated context size");
    assert_eq!(frame["windowTokens"], 128000);
}

/// A compaction rewrites the transcript (the older messages replaced
/// by the summary) — the re-estimated (post-compaction) context size
/// is emitted, so the percentage drops after the compaction instead
/// of staying at the pre-compaction value.
#[tokio::test]
async fn a_compaction_emits_a_context_usage_update_from_the_reestimate() {
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("the summary".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    // The keep-recent budget is read from the COMPACTOR (its config copy +
    // THIS model's window), not from `catalog.compaction` after
    // `AgentLoop::new`, so mutating the catalog here would be inert: a 128k
    // window affords the full 20 000, `older` comes back empty and the
    // compaction is skipped. Replace the compactor so the budget lands where
    // production reads it.
    loop_.compactor = Compactor::new(
        CompactionConfig {
            keep_recent_tokens: 1,
            ..Default::default()
        },
        128_000,
    );
    loop_.load_transcript(vec![
        ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("u1".to_string()),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text("a1".to_string()),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("u2".to_string()),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text("a2".to_string()),
            tool_call_id: None,
            tool_calls: None,
        },
    ]);
    // The compaction REALLY shrank the transcript (the wrapper's return).
    assert_eq!(
        loop_
            .run_compaction("threshold", false, None, &CancellationToken::new())
            .await,
        CompactionRan::Compacted,
        "a compaction that rewrote the transcript shrank it"
    );
    // The compaction REALLY ran — it is not a `Skipped` whose only frame is
    // the one `load_transcript` already emitted (which would make the frame
    // assertions below tautological): the 4-message transcript is now the
    // summary + the kept tail.
    assert_eq!(
        loop_.messages.len(),
        2,
        "the compaction rewrote the 4-message transcript to the summary + the kept \
         tail, got {} messages",
        loop_.messages.len()
    );
    assert!(
        loop_.messages.iter().any(compact::is_compaction_summary),
        "the rewritten transcript carries the compaction summary"
    );
    let frame = last_context_usage_frame(&updates)
        .expect("a context_usage_update frame was emitted after the compaction");
    // The emitted value is the compactor's re-estimated (post-compaction)
    // context size — the same source the compaction threshold reads.
    assert_eq!(frame["usedTokens"], loop_.compactor.context_tokens());
    assert_eq!(frame["windowTokens"], 128000);
}

/// A model switch changes the WINDOW (the context — the messages — is
/// unchanged) AND rebuilds the `Compactor`, which drops the usage anchor.
/// The frame, the tracked count, and the threshold the next turn reads must
/// all agree the moment the switch happens: a switch that emits the OLD
/// number while `should_compact()` reads the freshly-reset `0` is a
/// transient breach of ADR 0028's "one number, two consumers", and switching
/// INTO a smaller window defers an already-due compaction by a whole prompt
/// (the call that has to send the oversized context).
#[tokio::test]
async fn a_model_switch_emits_a_context_usage_update_with_the_new_window() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let updates = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(TestSink {
        updates: updates.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    // The switch TARGET is a model whose compaction threshold is 100 (the
    // default 16384 reserve), so a 1001-token transcript is already due.
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.load_transcript(vec![ChatMessage {
        role: ChatRole::User,
        content: MessageContent::Text("x".repeat(4000)),
        tool_call_id: None,
        tool_calls: None,
    }]);
    let before = last_context_usage_frame(&updates).expect("a frame from the resume");
    assert_eq!(before["usedTokens"], 1001, "the resumed context");
    assert_eq!(
        before["windowTokens"], 128000,
        "the window is the old model's"
    );
    let switched = Model {
        context_window: 16_384 + 100,
        ..fake_model("m2")
    };
    loop_.set_model(switched);
    let frame = last_context_usage_frame(&updates)
        .expect("a context_usage_update frame was emitted on the model switch");
    assert_eq!(
        frame["usedTokens"], 1001,
        "the frame re-estimates the SAME transcript the switch left the `Compactor` \
         with — it must not show a number the threshold no longer reads"
    );
    assert_eq!(
        frame["windowTokens"], 16_484,
        "the window is the new model's"
    );
    assert_eq!(
        loop_.compactor.context_tokens(),
        frame["usedTokens"].as_u64().unwrap(),
        "one number, two consumers: the bar and the threshold agree IMMEDIATELY \
         (before the next turn's `note_context`)"
    );
    assert!(
        loop_.compactor.should_compact(),
        "a transcript already over the NEW window's threshold must be due the moment \
         the switch happens — not one prompt later"
    );
    // …and the next turn acts on it: the threshold is read at the top of
    // the iteration, so the switch's own number is what fires.
    loop_.catalog.compaction.keep_recent_tokens = 1;
    loop_.handle_prompt(&text_prompt("go")).await;
    let kinds = event_kinds(&mut events_rx);
    assert!(
        kinds.contains(&"compaction_start".to_string()),
        "the switch to a smaller window compacts instead of sending a context it \
         already knows is too large, got {kinds:?}"
    );
}

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
    ChatMessage, ChatRole, FinishReason, MessageContent, ProviderError, ProviderEvent, ToolCall,
};
use crate::agent::harness::retry::RetryPolicy;

/// (finding 4) `run_compaction` over a failing summary: the
/// transcript is UNCHANGED (a failed compaction is never a lost
/// context — the `compaction_end` is `aborted` with the error).
#[tokio::test]
async fn a_failed_summary_does_not_rewrite_the_transcript() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("partial".to_string()),
        ProviderEvent::Done(FinishReason::Error),
    ])]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // 3 large messages (~20000 tokens each — over the default
    // `keep_recent_tokens` budget, so `older` is non-empty and the
    // summary is actually attempted).
    loop_.messages = (0..3)
        .map(|i| ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text(format!("m{i}{}", "x".repeat(80000))),
            tool_call_id: None,
            tool_calls: None,
        })
        .collect();
    loop_.force_compact = Some(CompactionTrigger {
        reason: "threshold",
        will_retry: false,
    });
    // The failed compaction reports `Failed` — the outcome the overflow caller
    // now reads differently from `Skipped` (issue #90).
    assert_eq!(
        loop_
            .run_compaction("threshold", false, None, &CancellationToken::new())
            .await,
        CompactionRan::Failed,
        "a failed compaction rewrote nothing"
    );
    // The summary failed: the transcript is UNCHANGED (NOT replaced
    // by a truncated summary + a rewritten `native_messages`).
    assert_eq!(loop_.messages.len(), 3, "the transcript is intact");
    assert!(
        !loop_
            .messages
            .iter()
            .any(|m| matches!(m.role, ChatRole::System)),
        "no summary message was inserted"
    );
    let end = wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::compaction_end { .. })
    })
    .await
    .expect("the compaction_end was emitted");
    let RpcEvent::compaction_end {
        aborted,
        error_message,
        ..
    } = end
    else {
        unreachable!()
    };
    assert!(aborted, "the compaction was aborted");
    assert!(error_message.is_some(), "the error was reported");
}

/// A compaction the `Compactor` SKIPPED (the transcript is under the
/// keep-recent budget: `older` is empty, so no summary call is made and
/// nothing is rewritten) reports that the transcript did NOT shrink.
/// The wrapper's return value is "the transcript shrank" — the signal a
/// caller reads to decide whether a second request over the shrunken
/// context is worth sending at all.
#[tokio::test]
async fn a_skipped_compaction_reports_no_shrink() {
    // An empty script: a skipped compaction makes NO model call, so the
    // test would also catch a summary call that should not have happened.
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Two tiny messages — far under the keep-recent budget, so nothing is
    // old enough to summarize.
    loop_.messages = vec![
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
    ];
    let len_before = loop_.messages.len();
    assert_eq!(
        loop_
            .run_compaction("threshold", false, None, &CancellationToken::new())
            .await,
        CompactionRan::Skipped,
        "a skipped compaction shrank nothing"
    );
    assert_eq!(loop_.messages.len(), len_before, "the transcript is intact");
}

/// Collect events until `pred` matches (bounded — a missing event is a
/// test failure, not a hang). Unlike `event_kinds` this keeps EVERY event
/// it saw, so the caller can also assert what did NOT arrive.
async fn collect_events_until(
    rx: &mut mpsc::UnboundedReceiver<RpcEvent>,
    pred: impl Fn(&RpcEvent) -> bool,
) -> Vec<RpcEvent> {
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut events = Vec::new();
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ev)) => {
                let hit = pred(&ev);
                events.push(ev);
                if hit {
                    return events;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
    events
}

/// A transcript large enough that `older` is non-empty (three ~20 000
/// token messages — over the default `keep_recent_tokens` budget), so a
/// compaction actually attempts the summary call.
fn large_transcript(n: usize) -> Vec<ChatMessage> {
    (0..n)
        .map(|i| ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text(format!("m{i}{}", "x".repeat(80000))),
            tool_call_id: None,
            tool_calls: None,
        })
        .collect()
}

/// A transcript FAR under the keep-recent budget, so a compaction has
/// nothing old enough to summarize (`older` empty → `Skipped` → nothing
/// shrinks) — the shape an overflow recovery cannot rescue.
fn tiny_transcript() -> Vec<ChatMessage> {
    vec![
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
    ]
}

/// (ADR 0045) A context-length 400 is recovered INSIDE the turn: the dead
/// attempt is retracted, a forced compaction runs, and the SAME model call
/// is re-issued — the user types nothing (pre-fix the `Fatal` it used to be
/// settled the turn and left a dead transcript to nudge with "continue").
/// This is the PRE-STREAM path (`transport::map_status` decides the tier).
#[tokio::test]
async fn a_context_overflow_error_compacts_and_retries_the_turn() {
    let (provider, calls) = OverflowingProvider::new(
        &[0],
        vec![vec![
            ProviderEvent::TextDelta("ok".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]],
    );
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Large enough that `older` is non-empty, so the compaction SHRINKS and
    // the retry is a request that can actually succeed.
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    // `reason` is the UI contract (ADR 0045) and `will_retry` is the loop's
    // promise that the turn continues on the other side of the indicator.
    let start: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            RpcEvent::compaction_start { reason } => Some(reason.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        start,
        vec!["overflow".to_string()],
        "exactly one compaction, for the overflow, got {kinds:?}"
    );
    let end = events.iter().find_map(|e| match e {
        RpcEvent::compaction_end {
            reason,
            will_retry,
            error_message,
            aborted,
            ..
        } => Some((reason.clone(), *will_retry, *aborted, error_message.clone())),
        _ => None,
    });
    assert_eq!(
        end,
        Some(("overflow".to_string(), true, false, None)),
        "the end frame repeats the reason and the retry promise; a compaction that SHRANK has no actionable error"
    );

    // NO error settle: the turn ends the SUCCESSFUL way (`agent_end` then
    // `turn_end`) — an error settle emits `turn_end` with NO `agent_end`.
    let agent_end = kinds.iter().position(|k| k == "agent_end");
    let turn_end = kinds.iter().position(|k| k == "turn_end");
    assert!(
        matches!((agent_end, turn_end), (Some(a), Some(t)) if a < t),
        "the recovered turn completed normally (agent_end then turn_end), got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "turn_end").count(),
        1,
        "one settle for the turn, got {kinds:?}"
    );
    // The provider was called again — the overflowed call, the summary call,
    // then the RETRIED model call.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the failed call + the summary + the retry"
    );
}

/// The SAME recovery for a mid-stream overflow — an SSE `error` event INSIDE
/// the stream, which is what `map_anthropic_error` produces (a provider can
/// accept the request and then fail it once it has read the prompt). The
/// intercept sits where `mid_stream_error` is HANDLED (after the consumption
/// loop), not where it is captured: a `continue` at the capture site would
/// re-poll the same dying stream.
#[tokio::test]
async fn a_mid_stream_context_overflow_compacts_and_retries_the_turn() {
    let (provider, calls) = ScriptedProvider::new(vec![
        // The call that dies mid-stream, after partial text (no `Done`).
        Some(vec![
            ProviderEvent::TextDelta("parti".to_string()),
            ProviderEvent::Error(ProviderError::ContextOverflow(
                "invalid_request_error: prompt is too long: 213462 tokens > 200000 maximum"
                    .to_string(),
            )),
        ]),
        // The summary call, then the retried model call.
        Some(vec![
            ProviderEvent::TextDelta("summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("recovered".to_string()),
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
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    assert!(
        events.iter().any(|e| matches!(
            e,
            RpcEvent::compaction_start { reason } if reason == "overflow"
        )),
        "the mid-stream overflow compacted the session, got {kinds:?}"
    );
    assert!(
        matches!(
            events.iter().find(|e| matches!(e, RpcEvent::compaction_end { .. })),
            Some(RpcEvent::compaction_end {
                reason,
                will_retry: true,
                ..
            }) if reason == "overflow"
        ),
        "the end frame carries reason: overflow + will_retry: true, got {kinds:?}"
    );
    // The retry happened, and it is the SAME assistant message: the loop-back
    // keeps `message_started` true, so the already-announced `message_start`
    // is reused (a second one would start a second bubble).
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the dying call + the summary + the retry"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_start").count(),
        1,
        "ONE message_start for the retried message, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_end"),
        "no error settle — the turn completed, got {kinds:?}"
    );
}

/// (ADR 0048) The retryable retry reuses the message id, so the partial
/// reply it abandons is RETRACTED — otherwise `parti` welds onto `done` in
/// every consumer that appends same-`messageId` text. The retraction is the
/// ONLY way the discard can be seen: one `message_start` per assistant
/// message means the retry announces nothing new.
#[tokio::test]
async fn a_retried_reply_is_retracted_before_the_retry() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        // The attempt that dies mid-stream, after partial text.
        Some(vec![
            ProviderEvent::TextDelta("part".to_string()),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        // The retry, which REUSES the announced `messageId`.
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
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "this is the retryable retry path, got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_retracted").count(),
        1,
        "exactly one Retraction for the one abandoned attempt, got {kinds:?}"
    );
    let retracted = kinds.iter().position(|k| k == "message_retracted");
    let agent_end = kinds.iter().position(|k| k == "agent_end");
    assert!(
        matches!((retracted, agent_end), (Some(r), Some(e)) if r < e),
        "the Retraction precedes the turn's end (the retry must not weld \
         onto text that is already discarded), got {kinds:?}"
    );
    // The invariant this whole task exists to protect: a Retraction is NOT
    // expressed as "the next message starts".
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_start").count(),
        1,
        "ONE message_start for the retried message — the invariant holds, got {kinds:?}"
    );
}

/// (ADR 0048) The overflow recovery retracts too — the SECOND retry path,
/// and the ADR says the rule is general ("any third retry path must emit
/// the retraction too"). Same reuse of the announced id, same weld.
#[tokio::test]
async fn a_mid_stream_overflow_retracts_the_partial_reply() {
    // The exact script of
    // `a_mid_stream_context_overflow_compacts_and_retries_the_turn`.
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("parti".to_string()),
            ProviderEvent::Error(ProviderError::ContextOverflow(
                "invalid_request_error: prompt is too long: 213462 tokens > 200000 maximum"
                    .to_string(),
            )),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("recovered".to_string()),
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
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        kinds.iter().filter(|k| *k == "message_retracted").count(),
        1,
        "the mid-stream overflow retracts its partial reply, got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_start").count(),
        1,
        "ONE message_start for the retried message — the invariant holds, got {kinds:?}"
    );
    assert!(
        events.iter().any(|e| matches!(
            e,
            RpcEvent::compaction_start { reason } if reason == "overflow"
        )),
        "the overflow recovery still fires its own compaction, got {kinds:?}"
    );
    let retracted = kinds.iter().position(|k| k == "message_retracted");
    let compacted = kinds.iter().position(|k| k == "compaction_start");
    assert!(
        matches!((retracted, compacted), (Some(r), Some(c)) if r < c),
        "the dead attempt is retracted before the recovery starts, got {kinds:?}"
    );
}

/// The gate is "streamed something", not "the attempt was abandoned": a
/// PRE-STREAM overflow streams nothing, so there is no partial reply to
/// discard and a Retraction would erase nothing while saying so (ADR 0048's
/// silence rule). Pinned for the arm where `acc` does not even exist yet.
#[tokio::test]
async fn a_pre_stream_overflow_retracts_nothing() {
    let (provider, _calls) = OverflowingProvider::new(
        &[0],
        vec![vec![
            ProviderEvent::TextDelta("ok".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]],
    );
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert!(
        events.iter().any(|e| matches!(e, RpcEvent::agent_settled)),
        "the recovery still ran to the end of the turn, got {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "message_retracted"),
        "an attempt that streamed nothing is not a Retraction, got {kinds:?}"
    );
}

/// Same rule on the other path: a retryable error that arrives BEFORE any
/// text emits no Retraction — the common pre-stream retry stays exactly as
/// quiet as it was before ADR 0048.
#[tokio::test]
async fn an_abandoned_attempt_that_streamed_nothing_is_silent() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![ProviderEvent::Error(ProviderError::Retryable(
            "boom".to_string(),
        ))]),
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
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    // Without this the test would pass vacuously on a path never taken.
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "the abandon-and-retry path WAS walked, got {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "message_retracted"),
        "nothing streamed, so nothing is retracted, got {kinds:?}"
    );
}

/// (ADR 0048) An Abandoned attempt's tool calls were ANNOUNCED — the stream
/// already opened a `tool_call` card at `in_progress` — and are never
/// dispatched, so the card would spin forever, live AND in the persisted
/// `tool-call` row that reloads. The cancel path already closes calls that
/// never ran; an Abandoned attempt owes the same. A tool-only discard is
/// still a discard, so the Retraction rides with it.
#[tokio::test]
async fn a_retried_reply_closes_the_tool_calls_it_abandoned() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, calls) = ScriptedProvider::new(vec![
        // The attempt that dies: it ANNOUNCED a tool call, then failed.
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
            }),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        // The retry, which replays the SAME messages.
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

    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the abandoned tool call is CLOSED, not left spinning forever");
    let RpcEvent::tool_execution_end {
        tool_name,
        result,
        is_error,
        ..
    } = end
    else {
        unreachable!()
    };
    assert_eq!(tool_name, "bash", "the card closes under its own tool name");
    assert!(
        is_error,
        "an abandoned call is a failed card (`status: \"failed\"`), never a card \
         that spun forever"
    );
    assert_eq!(
        result["isError"].as_bool(),
        Some(true),
        "the result itself says it is an error"
    );
    assert_eq!(
        result["content"][0]["text"].as_str(),
        Some(
            "discarded — the model call that asked for this tool was abandoned \
             before it ran"
        ),
        "honest about the reason: the tool did not FAIL, it was discarded, got {result}"
    );

    // The rest of the turn. NOTE the ordering this pins by construction: the
    // wait above consumed everything up to the card close, so a Retraction
    // that had been emitted BEFORE the close would be missing here — cards
    // close first, then the text is retracted.
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_retracted").count(),
        1,
        "a tool-only discard is still a discard, and it follows the card close, \
         got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_settled"),
        "the turn still settles, got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "the abandoning call + the retry (the close does not re-call the model)"
    );
}

/// The Provider transcript is UNTOUCHED by the close: no `tool` message is
/// appended for a call that never dispatched, because the retry replays the
/// SAME messages — an appended `tool` result with no kept assistant message
/// would be an orphan the provider 400s on.
#[tokio::test]
async fn an_abandoned_tool_call_appends_no_tool_message() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
            }),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (mut loop_, _db) = build_loop_with_db(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;

    wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the abandoned tool call is closed");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");

    let loaded = loop_
        .store
        .load_messages("s1")
        .expect("the native transcript loads");
    assert!(
        !loaded.iter().any(|m| matches!(m.role, ChatRole::Tool)),
        "the synthetic close is a CARD close only — the Provider transcript keeps \
         no `tool` result for a call that never dispatched, got {loaded:?}"
    );
    // The retry's own reply IS the transcript's assistant message, and it
    // carries no `tool_calls` (the abandoned attempt's were never kept).
    let assistant = loaded
        .iter()
        .find(|m| matches!(m.role, ChatRole::Assistant))
        .expect("the retry's reply is persisted");
    assert!(
        assistant.tool_calls.is_none(),
        "the abandoned attempt's tool calls are not replayed to the provider, \
         got {:?}",
        assistant.tool_calls
    );
}

/// The overflow recovery closes them too — the second abandonment path, and
/// the ADR says the rule is general.
#[tokio::test]
async fn a_mid_stream_overflow_closes_the_abandoned_tool_call() {
    let (provider, _calls) = ScriptedProvider::new(vec![
        // The call that dies mid-stream: it announced a tool call, then
        // crossed the window (no `Done`).
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
            }),
            ProviderEvent::Error(ProviderError::ContextOverflow(
                "invalid_request_error: prompt is too long: 213462 tokens > 200000 maximum"
                    .to_string(),
            )),
        ]),
        // The summary call, then the retried model call.
        Some(vec![
            ProviderEvent::TextDelta("summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("recovered".to_string()),
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
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;

    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the overflowed attempt's tool call is CLOSED, not left spinning");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "the abandoned card is closed as failed");
    assert_eq!(
        result["content"][0]["text"].as_str(),
        Some(
            "discarded — the model call that asked for this tool was abandoned \
             before it ran"
        ),
        "the same honest result as the retryable path, got {result}"
    );

    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    assert!(
        events.iter().any(|e| matches!(
            e,
            RpcEvent::compaction_start { reason } if reason == "overflow"
        )),
        "the recovery still compacts, got {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| *k == "message_retracted").count(),
        1,
        "the abandoned partial reply is retracted after its cards close, \
         got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_settled"),
        "the turn settles after the recovery, got {kinds:?}"
    );
}

/// Nothing announced, nothing to close — the silence rule still holds: an
/// attempt that streamed no text and announced no tool call emits NEITHER a
/// synthetic card close NOR a Retraction.
#[tokio::test]
async fn an_abandoned_attempt_that_announced_nothing_closes_nothing() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![ProviderEvent::Error(ProviderError::Retryable(
            "boom".to_string(),
        ))]),
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
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    // Without this the test would pass vacuously on a path never taken.
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
        "the abandon-and-retry path WAS walked, got {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "tool_execution_end"),
        "no card was announced, so no card is closed, got {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "message_retracted"),
        "nothing streamed, so nothing is retracted, got {kinds:?}"
    );
}

/// The guard bounds one EPISODE, not one turn: a model call that SUCCEEDS
/// ends the episode (pi resets on `stopReason !== "error"`), so a later call
/// in the SAME turn that crosses the window again gets its own
/// compact-and-retry. A 60-call turn can cross the window more than once
/// legitimately, and a per-turn guard would kill the second recovery — the
/// one that would have been needed.
#[tokio::test]
async fn a_later_overflow_in_the_same_turn_gets_its_own_compact_and_retry() {
    let summary = || {
        vec![
            ProviderEvent::TextDelta("summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]
    };
    let (provider, calls) = OverflowingProvider::new(
        &[0, 3],
        vec![
            summary(), // call 0: OVERFLOWED (entry unused)
            summary(), // call 1: episode 1's summary
            // call 2: a SUCCESSFUL model call (a `manage_todo_list` read —
            // ungated, so the test needs no permission bridge) that ends
            // episode 1 and keeps the turn alive for another model call.
            vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "manage_todo_list".to_string(),
                    arguments: json!({ "operation": "read" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ],
            summary(), // call 3: OVERFLOWED again (entry unused)
            summary(), // call 4: episode 2's summary
            // call 5: the retry that lands.
            vec![
                ProviderEvent::TextDelta("recovered twice".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
        ],
    );
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    let starts: Vec<String> = events
        .iter()
        .filter_map(|e| match e {
            RpcEvent::compaction_start { reason } => Some(reason.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        starts,
        vec!["overflow".to_string(), "overflow".to_string()],
        "TWO episodes, TWO recoveries — the first successful model call reset the guard, got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        6,
        "2 overflowed calls + 2 summaries + the tool-calling call + the final retry"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_end"),
        "the turn still ended normally, got {kinds:?}"
    );
}

/// When the compaction CANNOT shrink the transcript (`Skipped` — the kept
/// tail alone is what is over the window) the turn settles there with the
/// actionable message and NO second request: the oversized prompt that just
/// failed is exactly what a retry would re-send, so a request that cannot
/// succeed would only cost a round trip. ONE `compaction_end` (the
/// actionable line rides on it — `run_compaction` emits the single one).
#[tokio::test]
async fn a_second_overflow_after_a_failed_recovery_gives_up_without_a_second_request() {
    // EVERY call overflows (more of them than this turn could ever make);
    // the empty script also pins that no summary call is spent on a
    // compaction that has nothing to summarize.
    let (provider, calls) = OverflowingProvider::new(&[0, 1, 2, 3, 4], vec![Vec::new()]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink = Arc::new(AllEventsSink {
        events: Arc::new(StdMutex::new(Vec::new())),
    });
    let captured = sink.events.clone();
    let (mut loop_, _db) = build_loop_with_sink(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
        sink,
    );
    // Too small to compact — `older` is empty.
    loop_.messages = tiny_transcript();
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert_eq!(
        kinds.iter().filter(|k| *k == "compaction_start").count(),
        1,
        "ONE compact-and-retry attempt per overflow episode, got {kinds:?}"
    );
    let ends: Vec<&RpcEvent> = events
        .iter()
        .filter(|e| matches!(e, RpcEvent::compaction_end { .. }))
        .collect();
    assert_eq!(
        ends.len(),
        1,
        "exactly one compaction_end — a second one would double the transcript line and the state frame, got {kinds:?}"
    );
    let RpcEvent::compaction_end {
        reason,
        will_retry,
        error_message,
        aborted,
        ..
    } = ends[0]
    else {
        unreachable!()
    };
    assert_eq!(reason, "overflow");
    // (issue #90 item 1) `will_retry` describes what the loop is about to DO,
    // not why the compaction was triggered. The loop settles this turn — no
    // second request is spent — so the flag is `false`: pre-fix it echoed the
    // trigger and promised a retry on the way to a dead turn.
    assert!(
        !will_retry,
        "the turn settles here, so no retry is promised"
    );
    assert!(
        !aborted,
        "a Skipped compaction is not an abort (nothing failed — there was nothing to summarize)"
    );
    assert_eq!(
        error_message.as_deref(),
        Some("Context overflow: not enough older context to compact — try a larger-context model."),
        "the user gets an actionable line, not a provider string"
    );

    // The transcript frames the normalize arm produced for the pair (issue #90
    // item 2): a card announced, then settled `failed` carrying the actionable
    // line — and NO compaction `system_chunk`, the shape that used to weld.
    let frames: Vec<Value> = captured
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(event, _)| event == "session-update")
        .map(|(_, payload)| payload["update"].clone())
        .filter(|u| {
            let kind = u["sessionUpdate"].as_str().unwrap_or_default();
            kind == "tool_call" || kind == "tool_call_update" || kind == "agent_message_chunk"
        })
        .collect();
    let announce = frames
        .iter()
        .find(|u| u["sessionUpdate"] == "tool_call")
        .expect("the compaction announced a card");
    let settle = frames
        .iter()
        .find(|u| u["sessionUpdate"] == "tool_call_update")
        .expect("the compaction settled its card");
    assert_eq!(settle["toolCallId"], announce["toolCallId"]);
    assert_eq!(settle["status"], "failed");
    assert_eq!(
        settle["rawOutput"]["details"]["error"].as_str(),
        Some("Context overflow: not enough older context to compact — try a larger-context model."),
        "the actionable line is the card's failure text"
    );
    assert!(
        !frames.iter().any(|u| {
            u["sessionUpdate"] == "agent_message_chunk"
                && u["content"]["text"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("Compact")
        }),
        "no `Compacting context…` / `Compaction finished` one-liner survives — the \
         weld these lines needed replacing is now unreachable"
    );

    // No doomed second request: the overflowed call was the ONLY model call
    // (the skipped compaction spent none either), and the turn settled as an
    // error (`turn_end` + `agent_settled`, no `agent_end`).
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the retry is not re-issued when the compaction shrank nothing"
    );
    assert!(
        !kinds.iter().any(|k| k == "agent_end"),
        "the turn settled as a failure, got {kinds:?}"
    );
    assert!(
        kinds.iter().any(|k| k == "turn_end"),
        "the failed turn's timeline still closes, got {kinds:?}"
    );
}

/// The queued overflow trigger belongs to the turn that queued it. A Stop
/// that lands while an overflow is being handled ends the turn at the
/// cancellation check at the TOP of the loop — which runs BEFORE the
/// trigger the overflow just queued is taken — so a trigger set on the
/// LOOP survived the settle. The next prompt then took it: the fresh turn
/// compacted under the PREVIOUS turn's `overflow` reason and, that
/// compaction having nothing to shrink, settled as an error WITHOUT ever
/// calling the model — the user's new prompt died for an overflow it never
/// had.
#[tokio::test]
async fn a_stale_overflow_trigger_does_not_hijack_the_next_turn() {
    let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // The leak, spelled out: the PREVIOUS turn was cancelled after its
    // overflow queued this trigger, so it is still armed on the loop.
    loop_.force_compact = Some(CompactionTrigger {
        reason: "overflow",
        will_retry: true,
    });
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    assert!(
        !kinds.iter().any(|k| k == "compaction_start"),
        "a trigger a PREVIOUS turn queued compacts nothing on this one, got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the new prompt made its OWN model call instead of dying on the stale trigger"
    );
    assert!(
        kinds.iter().any(|k| k == "agent_end"),
        "the new turn ran to completion, got {kinds:?}"
    );
}

/// ADR 0044's `/compact`: the `Compact` CONTROL command compacts with NO
/// turn in flight. The composer is locked during a turn, so a
/// hand-triggered compaction is ALWAYS an idle action — the idle control
/// channel is the channel that reaches the loop. The observable is the
/// event pair on the `events` channel, in order, with `reason:
/// "manual"` — and nothing else (the command is NOT a prompt: no turn).
#[tokio::test]
async fn a_compact_control_command_compacts_when_idle() {
    // The transcript is under the keep-recent budget, so the compaction is
    // SKIPPED — both frames still fire, and the empty script pins that no
    // model call happened.
    let (provider, calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.messages = vec![
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
    ];
    let control_tx = loop_.control_tx.clone();
    // The session token (the loop owns it — the clone ends `run()`).
    let session_cancel = loop_.cancel.clone();
    let join = tokio::spawn(loop_.run());

    control_tx
        .send(ControlCmd::Compact { instructions: None })
        .await
        .expect("the loop's control queue is open");
    let events = collect_events_until(&mut events_rx, |e| {
        matches!(e, RpcEvent::compaction_end { .. })
    })
    .await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    let start = kinds.iter().position(|k| k == "compaction_start");
    let end = kinds.iter().position(|k| k == "compaction_end");
    assert!(
        matches!((start, end), (Some(s), Some(e2)) if s < e2),
        "compaction_start THEN compaction_end, got {kinds:?}"
    );
    assert!(
        events.iter().all(|e| match e {
            RpcEvent::compaction_start { reason } | RpcEvent::compaction_end { reason, .. } =>
                reason == "manual",
            _ => true,
        }),
        "a hand-triggered compaction carries the `manual` reason (ADR 0045), got {kinds:?}"
    );
    assert!(
        matches!(
            events.last(),
            Some(RpcEvent::compaction_end {
                aborted: false,
                will_retry: false,
                ..
            })
        ),
        "the compaction ended cleanly and the loop goes back to idle"
    );
    assert!(
        !kinds
            .iter()
            .any(|k| k == "turn_start" || k == "turn_end" || k == "agent_settled"),
        "the compact command is NOT a prompt — no turn ran, got {kinds:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a skipped compaction makes no model call"
    );
    session_cancel.cancel();
    let _ = tokio::time::timeout(SETTLE, join).await;
}

/// The Stop (Esc → `cancel_session` → `Inbound::Abort`) cancels whatever
/// token is in the shared `turn_cancel` mutex, so an IDLE compaction is
/// cancellable only because `run()`'s compact arm ARMS a fresh turn token
/// before compacting. A summary over a large history is bounded solely by
/// the transport's `read_timeout` *between bytes*, so this is the real
/// hang the arming defends.
#[tokio::test]
async fn abort_cancels_an_idle_compaction() {
    // The single script is `None`: `complete` never resolves (the hanging
    // call the turn-cancel tests use), so the summary is genuinely
    // in flight when the Stop lands.
    let (provider, _calls) = ScriptedProvider::new(vec![None]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Large enough that `older` is non-empty — the summary is attempted.
    loop_.messages = large_transcript(3);
    // The observation point for "the transcript is unchanged": a
    // SUCCESSFUL compaction REWRITES the stored transcript
    // (`replace_messages`), an aborted one writes nothing.
    let store = loop_.store.clone();
    let control_tx = loop_.control_tx.clone();
    let session_cancel = loop_.cancel.clone();
    let join = tokio::spawn(loop_.run());

    control_tx
        .send(ControlCmd::Compact { instructions: None })
        .await
        .expect("the loop's control queue is open");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::compaction_start { .. })
    })
    .await
    .expect("the idle compaction started");
    // The Stop: `Inbound::Abort` cancels the token in the SHARED mutex —
    // which is the FRESH token `run()`'s arm armed for this compaction.
    turn_cancel
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .cancel();
    let end = wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::compaction_end { .. })
    })
    .await
    .expect("the cancelled compaction still ended");
    assert!(
        matches!(
            &end,
            RpcEvent::compaction_end {
                aborted: true,
                reason,
                ..
            } if reason == "manual"
        ),
        "the compaction ended ABORTED with the manual reason, got {end:?}"
    );
    assert!(
        store.load_messages("s1").expect("load_messages").is_empty(),
        "an aborted compaction rewrote NOTHING (a failed compaction is never a lost context)"
    );
    session_cancel.cancel();
    let _ = tokio::time::timeout(SETTLE, join).await;
}

/// (issue #90 item 1) An overflow recovery whose compaction FAILED continues
/// inference anyway. The summary call is a model call like any other and it
/// fails for TRANSIENT reasons — measured on real sessions, 29 of 75
/// compactions ended `Compaction failed: retryable: the summary hit the output
/// token limit` — so `Failed` says nothing about whether the turn's own
/// request is doomed. Pre-fix the loop settled the turn as an error the moment
/// the compaction shrank nothing, while the SAME `compaction_end` promised
/// `will_retry: true` ("the turn continues on the other side of this
/// compaction"), so the user watched the indicator clear and then had to type
/// "continue" — the exact symptom ADR 0045 was written to end.
///
/// This is deliberately NOT the `Skipped` case below: `Skipped` proves the kept
/// tail alone is over the window, so a second request genuinely cannot
/// succeed; `Failed` proves only that the summary call did not.
#[tokio::test]
async fn an_overflow_recovery_reissues_the_turn_when_the_summary_fails() {
    // Call 0 overflows; call 1 is the summary and it comes back TRUNCATED
    // (`Done(Length)` — the real failure shape above), so the transcript is
    // left intact and shrinks nothing; call 2 is the retried model call.
    let (provider, calls) = OverflowingProvider::new(
        &[0],
        vec![
            Vec::new(),
            vec![ProviderEvent::Done(FinishReason::Length)],
            vec![
                ProviderEvent::TextDelta("recovered".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
        ],
    );
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut loop_ = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // Large enough that `older` is non-empty, so the summary IS attempted
    // (a `Skipped` would test the other outcome).
    loop_.messages = large_transcript(3);
    loop_.handle_prompt(&text_prompt("hello")).await;
    let events =
        collect_events_until(&mut events_rx, |e| matches!(e, RpcEvent::agent_settled)).await;
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();

    // The turn CONTINUES: the model call is re-issued after the failed
    // compaction, and the turn ends the SUCCESSFUL way.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "the overflowed call + the summary + the RETRIED call — settling the turn here \
         is the bug (the user had to type 'continue'), got {kinds:?}"
    );
    let agent_end = kinds.iter().position(|k| k == "agent_end");
    let turn_end = kinds.iter().position(|k| k == "turn_end");
    assert!(
        matches!((agent_end, turn_end), (Some(a), Some(t)) if a < t),
        "the turn completed normally after the failed compaction, got {kinds:?}"
    );

    // The transcript holds the reply the model actually produced on the retry.
    assert!(
        loop_
            .messages
            .iter()
            .any(|m| matches!(&m.content, MessageContent::Text(t) if t == "recovered")),
        "the retried reply is in the transcript"
    );
}

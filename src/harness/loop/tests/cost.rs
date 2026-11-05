//! Issue #87 (T09) / Fulcrum T2: a priced model call emits exactly one
//! `cost_update` `interactive-event` frame, and nothing else about the turn
//! changes.
//!
//! The wire contract (D5, shared with the frontend task): the frame is
//! `{ sessionId, seq: 0, event: "cost_update", payload: { costUsd, source } }`
//! emitted DIRECTLY on the sink — it is an interactive-event, NOT an
//! `RpcEvent`.
//!
//! **`costUsd` is the session's CUMULATIVE TOTAL, not one call's delta** (Fulcrum
//! T2, amending ADR 0082's "deltas, not totals" and its limit 7). The loop owns
//! the running figure (`AgentLoop::cost_acc`, seeded from `sessions.cost_usd` on
//! `Resume`), every consumer OVERWRITES with the last frame instead of summing,
//! and a re-delivered frame therefore cannot double the stored figure. The
//! numbers below are chosen so the accumulation is visible: the second frame of
//! every pair is the SUM, never the second call's own figure.
//!
//! The fixture is the `context_usage.rs` shape with one correction: the sink
//! must be `AllEventsSink`, not `TestSink` — `TestSink` records only
//! `session-update` frames and is structurally blind to the
//! `interactive-event` this feature emits.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::CompactionRan;
use super::support::*;
use crate::events::{CompactionReason, EventSink, RpcEvent};
use crate::harness::catalog::CompactionConfig;
use crate::harness::compact::Compactor;
use crate::harness::dispatch::{MockDispatcher, SubagentDispatcher};
use crate::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ProviderError, ProviderEvent, ToolCall,
    Usage,
};
use crate::harness::retry::RetryPolicy;
use crate::subagent::{SubagentCancel, SubagentMetrics, SubagentOutcome};

/// The `cost_update` frames a run emitted, in order (the payloads WITH their
/// envelope, so the pins can assert `sessionId` / `seq` / `event` alongside the
/// inner `payload`). `AllEventsSink` records EVERY emission under its channel
/// name, so the filter is name + the envelope's `event` discriminant.
fn cost_frames(events: &StdMutex<Vec<(String, Value)>>) -> Vec<Value> {
    events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(name, frame)| name == "interactive-event" && frame["event"] == "cost_update")
        .map(|(_, frame)| frame.clone())
        .collect()
}

/// The `session-update` frames a run emitted (the context anchor gauge —
/// pin 4 needs it, and `AllEventsSink` is the only sink that sees BOTH it and
/// the `interactive-event` frames, which is the whole point of using it here).
fn session_update_frames(events: &StdMutex<Vec<(String, Value)>>) -> Vec<Value> {
    events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(name, _)| name == "session-update")
        .map(|(_, frame)| frame["update"].clone())
        .collect()
}

/// A priced call emits its cost as exactly one `cost_update` frame, with the
/// envelope the frontend already routes (`sessionId` rides the ENVELOPE —
/// `App.tsx` reads `payload.sessionId` — so the inner payload stays
/// `{ costUsd, source }`).
///
/// The script carries TWO priced `Usage`s so the "exactly one frame PER CALL"
/// half is checkable in the same run, and the FIRST one is at a REAL measured
/// magnitude (T08's probe P1: `0.000011662`) rather than a convenient one.
///
/// Fulcrum T2 moved the frame's VALUE from delta to total, and that moved which
/// frame carries the precision probe: on a fresh loop (accumulator seeded to
/// `0.0`) the FIRST frame IS the first call's figure, so the exact sub-cliff
/// round-trip assertion lives on `frames[0]` now. The SECOND frame carries the
/// running total, so it is asserted against the EXPRESSION the emitter computes
/// (`0.0042 + 0.000011662`), not a decimal literal — a literal would put the
/// test one ulp away from the accumulator's own sum and could fail for a reason
/// that has nothing to do with the contract. Keeping the probe on `frames[1]`
/// at `0.000011662` would be simply wrong under totals.
#[tokio::test]
async fn a_priced_call_emits_one_cost_update_frame_carrying_that_calls_cost() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        // FIRST priced call, at T08's measured sub-cliff magnitude — so the
        // first frame IS that figure on a loop seeded to zero.
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1100,
                output_tokens: 60,
            },
            Some(0.000011662),
        ),
        // The SECOND priced call: its frame carries the running total.
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1000,
                output_tokens: 50,
            },
            Some(0.0042),
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

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        2,
        "exactly one `cost_update` frame PER priced `Usage` — two priced `Usage`s, two frames"
    );

    // The envelope (the `todos_update` shape the Client already consumes).
    let first = &frames[0];
    assert_eq!(first["sessionId"], "s1", "the loop's own session id");
    assert_eq!(first["seq"], 0, "the same convention as `todos_update`");
    assert_eq!(first["event"], "cost_update");
    assert_eq!(first["payload"]["source"], "main");
    assert!(
        first["payload"].get("sessionId").is_none(),
        "`sessionId` rides the envelope — a second copy inside the payload is redundant"
    );
    assert_eq!(
        first["payload"]
            .as_object()
            .expect("an object payload")
            .keys()
            .collect::<Vec<_>>()
            .len(),
        2,
        "the inner payload is EXACTLY {{costUsd, source}} — the pinned field set"
    );

    // The real magnitude, round-tripped EXACTLY — asserted on the FIRST frame,
    // which on a loop seeded to zero IS the sub-cliff figure. Asserted on the
    // serialised number re-parsed (what the frontend actually receives), so a
    // narrowing at the `json!` seam is caught here and nowhere else. Every
    // other pin in this file sits above the `$0.00005` formatter cliff, so
    // nothing else here would notice a truncation or a float→string→float
    // narrowing at sub-`0.0001` magnitudes. The formatter is the frontend's
    // problem; the WIRE must not be where the precision is lost.
    let wire = serde_json::to_string(&frames[0]["payload"]["costUsd"]).expect("serialisable");
    let round_trip: f64 = serde_json::from_str(&wire).expect("a JSON number");
    assert_eq!(
        round_trip, 0.000011662,
        "the sub-cliff figure survives the wire intact (serialised as {wire})"
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.000011662,
        "the first frame carries the first call's figure (the accumulator started at 0.0)"
    );

    // The SECOND frame is the RUNNING TOTAL — asserted against the expression
    // the emitter adds, so the comparison is bitwise the emitter's own.
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0042 + 0.000011662,
        "the second frame carries the accumulated session total, not the \
         second call's own delta"
    );
}

/// A call whose provider reported NO cost emits NOTHING at all.
///
/// This is the phantom-`$0.00` defence, and it is an ABSENCE pin: the only way
/// to guarantee a `None` cost never renders as a zero is to never put a frame
/// on the bus for it — no `costUsd: null`, no `costUsd: 0`. REGRESSION GUARD:
/// it passes before the emitter exists for the trivial reason that nothing
/// emitted anything at all, so it does not count as a RED; the regression it
/// guards is a future edit that emits `null` or `0` for a `None`, and THAT is
/// what reddens it.
#[tokio::test]
async fn a_call_without_a_cost_emits_no_cost_update_frame_at_all() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1000,
                output_tokens: 50,
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

    assert!(
        cost_frames(&events).is_empty(),
        "a `None` cost emits NOTHING — not a null, not a zero (the phantom `$0.00`)"
    );
    // And the turn still did everything it did before: the usage arm's other
    // emissions are untouched by the absence of a figure.
    assert!(
        session_update_frames(&events)
            .iter()
            .any(|u| u["sessionUpdate"] == "context_usage_update"),
        "the context anchor frame still fires for a usage with no cost"
    );
}

/// Two priced calls emit TWO TOTALS — the second frame carries the SUM, never
/// the second call's own figure.
///
/// This is the falsifier for the contract FLIPPED by Fulcrum T2. It used to pin
/// the opposite (`0.0020`, "a running total here would double-count in the
/// summing frontend store"). The store no longer sums — `combineSessionCost` is
/// last-frame-wins and `Db::record_session_cost` is an overwrite — so a frame
/// that carried a per-call delta would now UNDER-report, and the total is what
/// every consumer needs. The discriminating numbers are still the whole point:
/// `0.0010` then `0.0020` sums to `0.0030`, a value neither call carries on its
/// own, so the assertion cannot be satisfied by an emitter that forwards the
/// figure it was handed.
#[tokio::test]
async fn two_priced_calls_emit_running_totals_and_never_a_bare_delta() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    // Two SEPARATE prompts = two model calls on ONE loop (the session
    // accumulator is the loop's own field, and it survives a turn boundary).
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("one".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                },
                Some(0.0010),
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("two".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 200,
                    output_tokens: 20,
                },
                Some(0.0020),
            ),
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
    loop_.handle_prompt(&text_prompt("second")).await;

    let frames = cost_frames(&events);
    assert_eq!(frames.len(), 2, "one total frame per priced call");
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0010,
        "the first frame is the first call's figure (the loop started at zero)"
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0010 + 0.0020,
        "the SECOND frame is the session's running total — `0.0030`, not the \
         second call's own `0.0020` (a delta here would UNDER-report against a \
         store that overwrites)"
    );
}

/// A `Resume` continues the stored figure instead of restarting at zero.
///
/// This is the loop-side half of the Resume-seeding seam (`sessions.cost_usd` →
/// `StartEnv.cost_usd` → `default_build_loop` → `AgentLoop::seed_cost`). ADR
/// 0082 rejected a loop-side total because "a re-incarnation or `Resume`
/// rebuilds the loop with no memory of the old figure" — T2's answer is that the
/// figure is re-LOADED each time from the row that outlives the loop, so the
/// first frame of a resumed session must be `stored + first_call`, never
/// `0 + first_call`. Without `seed_cost` this frame reads `0.0009`.
#[tokio::test]
async fn a_seeded_loop_continues_the_stored_total_instead_of_restarting_at_zero() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 100,
                output_tokens: 10,
            },
            Some(0.0009),
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
    // The seam `default_build_loop` calls on the `(StartMode::Resume, _)` arm,
    // with the figure read off the row.
    loop_.seed_cost(0.0025);
    loop_.handle_prompt(&text_prompt("hello")).await;

    let frames = cost_frames(&events);
    assert_eq!(frames.len(), 1, "one frame for the one priced call");
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0025 + 0.0009,
        "a resumed session's first frame continues the stored total"
    );
}

/// A cost frame does not disturb the context anchor.
///
/// Money must NEVER enter `Compactor::record_usage` — that path is a REPLACED
/// context snapshot, not an accumulator, and a sum in there re-sends the same
/// prompt on every compaction. REGRESSION GUARD: `record_usage` is unchanged by
/// this task, so this passes on write; it exists to catch a LATER edit that
/// routes money into the anchor while a cost is present on the same event.
#[tokio::test]
async fn a_cost_frame_leaves_the_context_usage_anchor_alone() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 4321,
                output_tokens: 100,
            },
            // A cost IS present on the very event the anchor reads.
            Some(0.0042),
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

    assert!(
        !cost_frames(&events).is_empty(),
        "precondition: this run DID carry a priced call, so the anchor is being \
         read with money on the same event"
    );
    let anchor = session_update_frames(&events)
        .into_iter()
        .rev()
        .find(|u| u["sessionUpdate"] == "context_usage_update")
        .expect("a context_usage_update frame was emitted");
    assert_eq!(
        anchor["usedTokens"], 4421,
        "the anchor is still input + output — money never enters `record_usage`"
    );
    assert_eq!(anchor["windowTokens"], 128000, "the window is the model's");
}

/// The cost frame is an interactive-event, NOT an `RpcEvent`.
///
/// `App.tsx` routes `cost_update` off the `interactive-event` payload's `event`
/// discriminant; an `RpcEvent`-carried cost would reach no UI and would force a
/// `kind()` arm for a phantom event. Also pins that `usage_value()` is
/// untouched: the `message_update.usage` object still carries exactly the two
/// token fields. REGRESSION GUARD on the `usage_value()` half — that shape
/// already exists today.
#[tokio::test]
async fn a_cost_frame_is_not_an_rpc_event() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1000,
                output_tokens: 50,
            },
            Some(0.0042),
        ),
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
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;

    assert!(
        !cost_frames(&events).is_empty(),
        "precondition: a cost frame WAS emitted (on the interactive-event channel)"
    );

    // Drain the `RpcEvent` channel: NOTHING on it mentions money. Serialising
    // each event is what makes this a falsifier — `kind()` names alone could
    // never contain the payload key, so asserting on the names only would be a
    // tautology that passes through an `RpcEvent`-carried cost.
    let mut kinds: Vec<String> = Vec::new();
    let mut wire = String::new();
    let mut usages: Vec<Value> = Vec::new();
    while let Ok(ev) = events_rx.try_recv() {
        kinds.push(ev.kind().to_string());
        wire.push_str(&serde_json::to_string(&ev).unwrap_or_default());
        if let RpcEvent::message_update { usage, .. } = &ev {
            usages.push(usage.clone());
        }
    }
    assert!(
        !kinds.iter().any(|k| k.contains("cost")),
        "no cost-bearing `RpcEvent` variant, got {kinds:?}"
    );
    assert!(
        !wire.contains("costUsd") && !wire.contains("\"cost\""),
        "the money key never appears anywhere on the `RpcEvent` channel"
    );
    let usage = usages
        .last()
        .cloned()
        .expect("the usage arm emitted a message_update");
    assert_eq!(
        usage,
        serde_json::json!({ "inputTokens": 1000, "outputTokens": 50 }),
        "`usage_value()` is untouched — the money rides its OWN frame, never the \
         `message_update.usage` object"
    );
}

/// An attempt that delivered a `Usage` and then DIED still emits its money.
///
/// The provider billed that attempt. #87's figure is money spent, not money
/// spent by successful calls, so the frame is emitted from inside the `Usage`
/// arm — strictly before `note_success` / `message_end` / persist — and a turn
/// that never settles cannot lose it.
///
/// This is the pin a "only count settled turns" refactor would silently break:
/// moving the emission to any settle point drops the FIRST frame and
/// under-bills the user. The assertion is therefore on the first frame's VALUE
/// (the abandoned attempt's `0.0007`), not on a frame count — a count-only
/// assertion would be satisfied by the settled attempt alone.
///
/// The shape is TWO scripts, one clean retry. Never a single error-only script:
/// `ScriptedProvider` REPEATS its last script when the list is exhausted, so a
/// one-script error stream re-runs the same attempt until the retry budget
/// spends and the frame count becomes budget-dependent.
#[tokio::test]
async fn an_abandoned_attempt_that_delivered_a_usage_still_emits_its_cost() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, calls) = ScriptedProvider::new(vec![
        // The attempt that dies mid-stream — AFTER reporting its usage.
        Some(vec![
            ProviderEvent::TextDelta("part".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 700,
                    output_tokens: 7,
                },
                Some(0.0007),
            ),
            ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
        ]),
        // The retry, which settles.
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 800,
                    output_tokens: 8,
                },
                Some(0.0009),
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        // A millisecond base, the idiom every retry test here uses — never the
        // 1 s default, which only makes the wait long, not the assertion true.
        RetryPolicy::new_with(3, Duration::from_millis(1)),
        sink,
        None,
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("hello")).await;

    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "precondition: the abandoned attempt really was followed by a retry"
    );
    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        2,
        "two billed attempts, two frames — the abandoned attempt's money is not forfeited"
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0007,
        "the FIRST frame is the ABANDONED attempt's cost — emission happens in \
         the `Usage` arm, before the turn settles"
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0007 + 0.0009,
        "the settled attempt's figure lands on the RUNNING TOTAL (the emission \
         point is unchanged; only the VALUE the frame carries moved)"
    );
}

/// `-0.0` never reaches the wire: the emitter normalises the sign.
///
/// The two sibling plans disagreed about how a negative zero renders (nothing,
/// vs `$0.00` because JS `-0 === 0`). The resolution is EMITTER-side so the
/// disagreement is unrepresentable rather than arbitrated in copy: `c + 0.0`
/// maps `-0.0 -> +0.0` and leaves a real `0.0` a real `0.0`, so it is NOT the
/// `!= 0.0` filter that would eat a legitimate `Some(0.0)`.
///
/// The assertion is on the SERIALISED number, deliberately: `as_f64() == 0.0`
/// is a tautology — Rust and JS both report `-0.0 == 0.0` as true, so an
/// equality check passes through exactly the bug this pin exists to catch.
#[tokio::test]
async fn a_negative_zero_cost_emits_positive_zero() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("hi".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 10,
                output_tokens: 1,
            },
            Some(-0.0),
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

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        1,
        "a `-0.0` is still a figure (`Some`), so the frame is still emitted"
    );
    let wire = serde_json::to_string(&frames[0]["payload"]["costUsd"]).expect("serialisable");
    assert!(
        !wire.contains('-'),
        "the serialised cost must carry no sign, got {wire}"
    );
    assert_eq!(
        wire, "0.0",
        "a real zero is still emitted (the normalisation is `+ 0.0`, never a filter)"
    );
}

/// The compaction summariser's own call reaches the cost channel like any
/// other priced call (issue #106), and under Fulcrum T2 its figure lands on the
/// SESSION TOTAL rather than as a bare per-call delta.
///
/// Before #106 `compact.rs::summarize` swallowed the provider's `Usage` event in
/// its catch-all arm, so a compaction's money was invisible: the figure
/// under-stated by one summariser call per compaction (ADR 0082 limit 2,
/// follow-up F2). `summarize` now reads the cost off the stream and returns it,
/// and `AgentLoop::run_compaction` emits it through the SAME `emit_cost_update`
/// the turn's `Usage` arm uses — one frame, keyed to the loop's own session id
/// (no child id, no synthetic id), `"source": "main"` (the label is inert —
/// ADR 0041).
///
/// This fixture is a FRESH loop (accumulator seeded to `0.0`), so the total and
/// the summary call's own figure coincide at `0.0042`. That is not a vacuous
/// pass: it pins that the compaction arm does not emit the session total PLUS a
/// second copy of its own figure, and the sibling
/// `a_priced_compaction_adds_its_figure_to_the_sessions_running_total` below
/// separates the two readings.
#[tokio::test]
async fn a_priced_compaction_emits_one_cost_update_frame_carrying_that_calls_cost() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    // The summary call's stream: the priced `Usage` PRECEDES `Done` (the wire
    // order ADR 0082 records), so the figure is on the stream the compaction
    // drains.
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("the summary".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1000,
                output_tokens: 50,
            },
            Some(0.0042),
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
    // Force a REAL compaction: replace the compactor so the keep-recent budget
    // lands where production reads it (the `context_usage.rs` fixture's seam).
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
    // Precondition: the compaction REALLY ran (a `Skipped` would make the
    // frame assertion below a test that never reached the summariser).
    assert_eq!(
        loop_
            .run_compaction(
                CompactionReason::Threshold,
                false,
                None,
                &CancellationToken::new()
            )
            .await,
        CompactionRan::Compacted,
        "the fixture really compacted, so the summary call really happened"
    );

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        1,
        "exactly one `cost_update` frame for the summary call (got {})",
        frames.len()
    );
    let frame = &frames[0];
    assert_eq!(
        frame["sessionId"], "s1",
        "the loop's OWN session id — the same id the turn figures use"
    );
    assert_eq!(frame["seq"], 0, "the emitter's dedupe-free convention");
    assert_eq!(frame["event"], "cost_update");
    assert_eq!(
        frame["payload"]["costUsd"], 0.0042,
        "the summary call's figure on a fresh (seeded-0) loop IS the session total"
    );
}

/// A compaction's figure joins the calls that came BEFORE it in the same loop.
///
/// The turn arm and the compaction arm share ONE accumulator, so this is the
/// pin that the compaction frame is a TOTAL and not a second, independent
/// per-call delta — the case the fresh-loop fixture above cannot distinguish.
/// A child of the same seam that summed only its own arm would read `0.0042`
/// here instead of `0.0054`.
#[tokio::test]
async fn a_priced_compaction_adds_its_figure_to_the_sessions_running_total() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    // ONE script serves both model calls: the turn's own (which reports a
    // priced `Usage` and settles) and the summary call the compaction drives.
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("the summary".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1000,
                output_tokens: 50,
            },
            Some(0.0042),
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
    // First: a normal priced turn, so the accumulator is no longer zero.
    loop_.handle_prompt(&text_prompt("hello")).await;
    assert_eq!(
        cost_frames(&events)
            .last()
            .expect("the turn emitted its cost frame")["payload"]["costUsd"]
            .as_f64(),
        Some(0.0042),
        "precondition: the turn's frame put 0.0042 on the accumulator"
    );

    // Then: a real compaction on the same loop (the fixture seam the fresh-loop
    // compaction pins use).
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
    assert_eq!(
        loop_
            .run_compaction(
                CompactionReason::Threshold,
                false,
                None,
                &CancellationToken::new()
            )
            .await,
        CompactionRan::Compacted,
        "precondition: the compaction really ran"
    );

    let frames = cost_frames(&events);
    assert_eq!(frames.len(), 2, "the turn's frame, then the compaction's");
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0042 + 0.0042,
        "the summary call's figure ADDS to the session total — the two arms \
         share one accumulator"
    );
}

/// A compaction whose summary reported NO cost emits no frame at all.
///
/// REGRESSION GUARD, not a RED pin: it asserts an absence, so it passes before
/// the change too (nothing emitted anything at all then). Its discriminating
/// moment is AFTER the change — it proves the `None` cost survived the
/// `summarize` → `CompactionOutcome` → `emit_cost_update` hand-off as `None`
/// and never collapsed into a `Some(0.0)` frame, which would manufacture the
/// phantom `$0.00` the cost channel's whole contract exists to prevent.
#[tokio::test]
async fn a_compaction_whose_summary_reported_no_cost_emits_no_frame_at_all() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("the summary".to_string()),
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1000,
                output_tokens: 50,
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
    assert_eq!(
        loop_
            .run_compaction(
                CompactionReason::Threshold,
                false,
                None,
                &CancellationToken::new()
            )
            .await,
        CompactionRan::Compacted,
        "precondition: the compaction really ran and the summary call really happened"
    );

    assert!(
        cost_frames(&events).is_empty(),
        "a `None` summary cost emits NOTHING — not a null, not a zero"
    );
}

// ---------------------------------------------------------------------------
// Fulcrum T2 follow-up — ONE WRITER PER STORE KEY: the parent folds its
// children's money into its OWN accumulator.
// ---------------------------------------------------------------------------
//
// THE DEFECT these pins exist for. Two producers used to write the SAME store
// key (the parent's id): the parent's own `emit_cost_update` (from `cost_acc`)
// and the Supervisor's roll-up frame in `finish_subagent`. A frame is a
// SNAPSHOT and the store overwrites, so the last frame won — and the parent's
// accumulator had never seen the child. Parent spends 0.003, child spends
// 0.002, parent spends 0.006: the roll-up wrote 0.005 and the parent's next
// frame overwrote it with 0.009. **The child's money was erased.** The old
// delta wire ADDED, so it could not lose this; the totals wire made the loss
// structural.
//
// The fix is not a second writer on the parent key — it is ONE writer: the
// parent's loop. `harness/dispatch.rs` carries the child's `metrics.cost` out
// of the outcome (loop state, never prompt text), `run_tool_batch` folds it
// into `cost_acc` at the point the outcome is received, and the roll-up frame
// is GONE. These tests are the loop-side proof; the absence of the second
// writer is pinned where the removed frame used to be emitted
// (`subagent_flow.rs`'s `rollup_tests`).

/// The `subagent` tool call every fold fixture scripts (the params the
/// advertised spec accepts — `task` only).
fn subagent_tool_call(id: &str) -> ProviderEvent {
    ProviderEvent::ToolCall(ToolCall {
        id: id.to_string(),
        name: "subagent".to_string(),
        arguments: serde_json::json!({ "task": "delegate" }),
    })
}

/// A `Completed` outcome carrying `cost` as the child's reported total (the
/// `SubagentMetrics.cost: Option<f64>` hand-off, issue #107's spelling).
fn completed_with_cost(cost: Option<f64>) -> SubagentOutcome {
    SubagentOutcome::Completed {
        output: "the answer".to_string(),
        metrics: SubagentMetrics {
            output: "the answer".to_string(),
            input_tokens: 10,
            output_tokens: 5,
            cost,
            duration_ms: 1,
        },
    }
}

/// Queue a canned outcome on a `MockDispatcher` (the loop's `SubagentWait`
/// `select!` sees it the moment the dispatch pops it — the oneshot's value is
/// buffered, so the sender may be dropped right after `send`).
fn queue_outcome(dispatcher: &MockDispatcher, outcome: SubagentOutcome) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, SubagentCancel::none());
    tx.send(outcome).expect("the queued receiver is alive");
}

/// (THE MONEY-LOSS PIN) A parent that delegates must not lose its child's
/// money: parent 0.003 → child 0.002 → parent 0.006 lands as `0.011`, never
/// `0.009`.
///
/// This is the defect the totals wire introduced and the delta wire could not
/// have: the parent's own frames are keyed to the parent, and so was the
/// Supervisor's roll-up, so the two producers on one store key raced and the
/// parent's accumulator — which never saw the child — wrote last. Under the
/// one-writer contract the child's figure is folded into `cost_acc` when the
/// loop receives the outcome, so the parent's NEXT frame already includes it
/// and there is nothing left to overwrite.
///
/// The three figures are chosen so every reading is distinguishable: `0.009`
/// (the parent's own two calls — the loss), `0.005` (the child's figure alone
/// — the old roll-up's last-word), `0.003` (the first call), and `0.011` (the
/// truth). Asserted against the EMITTER'S OWN sum expression, not a decimal
/// literal, so the comparison is bitwise the accumulator's.
#[tokio::test]
async fn a_delegating_parents_total_keeps_its_childs_money() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![
        // Call 1: the parent's own priced call, then the delegation.
        Some(vec![
            ProviderEvent::TextDelta("working".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                },
                Some(0.003),
            ),
            subagent_tool_call("t1"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        // Call 2: the parent's second priced call, AFTER the child returned.
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 200,
                    output_tokens: 20,
                },
                Some(0.006),
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = MockDispatcher::new();
    queue_outcome(&dispatcher, completed_with_cost(Some(0.002)));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        3,
        "the parent's call, the fold at the outcome, the parent's next call — \
         got {:?} (2 frames means the child's money was never folded in)",
        frames
            .iter()
            .map(|f| f["payload"]["costUsd"].clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.003,
        "the parent's own call lands on a zero accumulator"
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.003 + 0.002,
        "the fold happens AT THE OUTCOME — the frame the parent emits when it \
         receives its child already carries the child's money"
    );
    assert_eq!(
        frames[2]["payload"]["costUsd"].as_f64().expect("a number"),
        0.003 + 0.002 + 0.006,
        "the parent's NEXT frame carries the session total INCLUDING the child \
         — `0.009` is the money-loss bug (the accumulator never saw the child), \
         `0.005` would be the retired roll-up's last word"
    );
    // ONE writer on the parent's store key: every frame that lands there is the
    // parent's own emitter — the parent's session id, `source: "main"`. A second
    // producer spelling the same key differently is the bug these pins retire.
    for frame in &frames {
        assert_eq!(
            frame["sessionId"], "s1",
            "every parent-keyed frame carries the parent's OWN session id"
        );
        assert_eq!(
            frame["payload"]["source"], "main",
            "and the parent's own label — the fold is the parent's money, not a \
             separately-labelled second opinion on the same key"
        );
    }
}

/// The fold PERSISTS the child's money even when the parent makes no further
/// priced call.
///
/// This is why the fold EMITS and not merely adds: a parent that delegates as
/// its last act never writes another `cost_update`, so an add-only fold would
/// leave the child's spend in memory and out of the row — and out of the figure
/// under the parent's title. `#105`'s requirement ("the parent's STORED total
/// includes its children") is bought by this frame, not by the removed roll-up.
#[tokio::test]
async fn a_childs_cost_is_folded_and_emitted_when_the_parent_makes_no_further_priced_call() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("working".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                },
                Some(0.003),
            ),
            subagent_tool_call("t1"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        // The tail call reports NO cost — so the fold's frame is the last frame
        // this session ever emits.
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 200,
                    output_tokens: 20,
                },
                None,
            ),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = MockDispatcher::new();
    queue_outcome(&dispatcher, completed_with_cost(Some(0.002)));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx_for_fold_test(),
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    let frames = cost_frames(&events);
    assert_eq!(
        frames
            .iter()
            .map(|f| f["payload"]["costUsd"].clone())
            .collect::<Vec<_>>(),
        vec![serde_json::json!(0.003), serde_json::json!(0.005)],
        "the fold's frame IS the last frame of the session — the child's money \
         reaches the store even though the parent never prices again"
    );
}

/// The `events` channel of the fold fixtures (nobody reads it — the assertion
/// point is the sink). A named helper so the two-arg `mpsc::unbounded_channel()`
/// inference does not depend on the surrounding call's argument order.
fn events_tx_for_fold_test() -> mpsc::UnboundedSender<RpcEvent> {
    mpsc::unbounded_channel().0
}

/// Two children on ONE parent accumulate MONOTONICALLY — the second child's
/// money ADDS to the first child's, it does not replace it.
///
/// This is the case the retired roll-up provably could not express (its own
/// docblock said so: "a parent with two children takes the second child's
/// figure"). It is the direct consequence of there being ONE accumulator that
/// spans a parent's own calls and its children's, and it is why the ruling said
/// keep-the-roll-up-as-the-sole-writer was rejected rather than merely worse.
#[tokio::test]
async fn two_children_accumulate_into_one_parent_total() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![
        // ONE batch, TWO delegations (the concurrent-subagents path).
        Some(vec![
            subagent_tool_call("t1"),
            subagent_tool_call("t2"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = MockDispatcher::new();
    queue_outcome(&dispatcher, completed_with_cost(Some(0.002)));
    queue_outcome(&dispatcher, completed_with_cost(Some(0.004)));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx_for_fold_test(),
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        2,
        "one fold frame per child, in the batch's tool-call order — got {frames:?}"
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.002,
        "the first child folds onto a zero accumulator"
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.002 + 0.004,
        "the SECOND child ADDS to the first — `0.004` alone is the retired \
         roll-up's last-frame-wins, which lost the first child entirely"
    );
}

/// A child that reported NO figure folds NOTHING — the fold reuses the
/// emitter's frame-existence gate, so `None` adds nothing and emits nothing.
///
/// REGRESSION GUARD, and honest about it: it passes before the fold exists too
/// (nothing emitted anything at all), so it is not a RED. What it forbids is
/// the obvious shortcut — `metrics.cost.unwrap_or(0.0)` — which would put a
/// phantom `$0.00` frame on the parent's key for a child that never cost
/// anything, the exact manufacture the cost channel's whole contract exists to
/// prevent (D5 / ADR 0082).
#[tokio::test]
async fn a_never_priced_child_contributes_no_fold_frame() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::TextDelta("working".to_string()),
            ProviderEvent::Usage(
                Usage {
                    input_tokens: 100,
                    output_tokens: 10,
                },
                Some(0.003),
            ),
            subagent_tool_call("t1"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = MockDispatcher::new();
    queue_outcome(&dispatcher, completed_with_cost(None));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx_for_fold_test(),
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        1,
        "exactly the parent's own frame — a child with no figure adds no frame \
         (absence is spelled by NOT emitting, never by a `0.0` fold)",
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.003,
        "the parent's own money, undiluted"
    );
}

/// A DEAD child's money still folds.
///
/// ADR 0055's salvage rule in money form: a child that was reaped mid-work was
/// still billed for it, and the `Failed` outcome carries REAL metrics. The
/// retired roll-up claimed to honour this; the fold honours it on the ONE key
/// that matters.
#[tokio::test]
async fn a_failed_childs_cost_still_folds_into_the_parent() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            subagent_tool_call("t1"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = MockDispatcher::new();
    queue_outcome(
        &dispatcher,
        SubagentOutcome::Failed {
            reason: crate::subagent::SubagentFailureReason::Unresponsive,
            error: "unresponsive".to_string(),
            output: "partial finding".to_string(),
            metrics: SubagentMetrics {
                output: "partial finding".to_string(),
                input_tokens: 40,
                output_tokens: 4,
                cost: Some(0.00001243),
                duration_ms: 100,
            },
        },
    );
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx_for_fold_test(),
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    let frames = cost_frames(&events);
    assert_eq!(
        frames.len(),
        1,
        "the dead child's money folds into exactly one parent frame, got {frames:?}"
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.00001243,
        "the sub-cliff figure survives the fold intact (the same precision pin \
         the parent's own frames carry)"
    );
    assert_eq!(frames[0]["sessionId"], "s1", "keyed to the PARENT loop");
}

/// The child's figure reaches the parent's LOOP STATE and never its PROMPT.
///
/// `dispatch.rs`'s doc gives the reason and it still stands: the parent reads
/// its child's output, and the Client reads the child's usage off
/// `subagent-closed`. Folding money must not smuggle a number into the text the
/// model sees — a model that can see a price can reason about a price.
#[tokio::test]
async fn the_folded_figure_never_enters_the_parents_prompt() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            subagent_tool_call("t1"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = MockDispatcher::new();
    queue_outcome(&dispatcher, completed_with_cost(Some(0.002)));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;

    // Precondition: the fold DID happen (otherwise "the prompt is clean" is a
    // statement about a run that never carried money at all).
    assert_eq!(
        cost_frames(&events)
            .last()
            .expect("the fold emitted the parent's total")["payload"]["costUsd"]
            .as_f64(),
        Some(0.002),
        "precondition: the child's 0.002 was folded"
    );

    // The tool result the parent's transcript — and the next model call's
    // prompt — is built from is the child's output and NOTHING else.
    let mut tool_texts: Vec<String> = Vec::new();
    let mut wire = String::new();
    while let Ok(ev) = events_rx.try_recv() {
        if let RpcEvent::tool_execution_end { result, .. } = &ev {
            tool_texts.push(serde_json::to_string(result).unwrap_or_default());
        }
        wire.push_str(&serde_json::to_string(&ev).unwrap_or_default());
    }
    assert_eq!(
        tool_texts,
        vec![serde_json::json!({
            "content": [{ "type": "text", "text": "the answer" }],
            "details": Value::Null,
            // `isError` — `ToolResult` is `rename_all = "camelCase"` on the wire.
            "isError": false,
        })
        .to_string()],
        "the `subagent` tool result is the child's output byte-for-byte — no \
         cost rides into the parent's prompt"
    );
    assert!(
        !wire.contains("0.002") && !wire.contains("costUsd"),
        "the folded figure never appears on the `RpcEvent` channel either \
         (it is loop state + its own interactive-event frame): {wire}"
    );
}

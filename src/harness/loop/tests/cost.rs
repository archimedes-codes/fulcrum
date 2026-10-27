//! Issue #87 (T09): a priced model call emits exactly one `cost_update`
//! `interactive-event` DELTA frame, and nothing else about the turn changes.
//!
//! The wire contract (D5, shared with the frontend task): the frame is
//! `{ sessionId, seq: 0, event: "cost_update", payload: { costUsd, source } }`
//! emitted DIRECTLY on the sink — it is an interactive-event, NOT an
//! `RpcEvent`. Every frame is ONE model call's cost (a delta); the frontend
//! store sums them, so Rust holds no accumulator and never emits a total.
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

use super::support::*;
use crate::agent::events::{EventSink, RpcEvent};
use crate::agent::harness::provider::{FinishReason, ProviderError, ProviderEvent, Usage};
use crate::agent::harness::retry::RetryPolicy;

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
/// half is checkable in the same run, and the second one is at a REAL measured
/// magnitude (T08's probe P1: `0.000011662`) rather than a convenient one. That
/// second assertion is not redundant with the first: every other pin in this
/// file sits above the `$0.00005` formatter cliff, so nothing else here would
/// notice a truncation or a float→string→float narrowing at sub-`0.0001`
/// magnitudes. The formatter is the frontend's problem; the WIRE must not be
/// where the precision is lost.
#[tokio::test]
async fn a_priced_call_emits_one_cost_update_frame_carrying_that_calls_cost() {
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
            Some(0.0042),
        ),
        // A SECOND priced call's figure, at T08's measured magnitude.
        ProviderEvent::Usage(
            Usage {
                input_tokens: 1100,
                output_tokens: 60,
            },
            Some(0.000011662),
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
    assert_eq!(
        first["payload"]["costUsd"], 0.0042,
        "the provider's per-call figure, not a total"
    );
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

    // The real magnitude, round-tripped EXACTLY. Asserted on the serialised
    // number re-parsed (what the frontend actually receives), so a narrowing at
    // the `json!` seam is caught here and nowhere else.
    let wire = serde_json::to_string(&frames[1]["payload"]["costUsd"]).expect("serialisable");
    let round_trip: f64 = serde_json::from_str(&wire).expect("a JSON number");
    assert_eq!(
        round_trip, 0.000011662,
        "the sub-cliff figure survives the wire intact (serialised as {wire})"
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.000011662
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

/// Two priced calls emit TWO DELTAS — the second frame carries the SECOND
/// call's cost, never the running sum.
///
/// This is the falsifier for "emit a cumulative total into a summing store",
/// which is the double-count trap the frontend cannot recover from across a
/// Worker re-incarnation. The store's sum IS the cumulative figure; Rust must
/// never pre-sum it.
#[tokio::test]
async fn two_priced_calls_emit_two_deltas_never_a_running_total() {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sink: Arc<dyn EventSink> = Arc::new(AllEventsSink {
        events: events.clone(),
    });
    // Two SEPARATE prompts = two model calls on ONE loop (the session-
    // accumulator temptation), each with its own figure.
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
    assert_eq!(frames.len(), 2, "one delta per priced call");
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0010
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0020,
        "the SECOND call's own cost — a running total (0.0030) here would \
         double-count in the summing frontend store"
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
        "two billed attempts, two deltas — the abandoned attempt's money is not forfeited"
    );
    assert_eq!(
        frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0007,
        "the FIRST frame is the ABANDONED attempt's cost — emission happens in \
         the `Usage` arm, before the turn settles"
    );
    assert_eq!(
        frames[1]["payload"]["costUsd"].as_f64().expect("a number"),
        0.0009,
        "the settled attempt's own cost follows, still a delta"
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

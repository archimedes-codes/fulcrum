//! The silent-settle diagnostic: a turn can settle as a SUCCESS having
//! dispatched NOTHING, and the settle site could not tell the two causes apart.
//!
//! The settle site (`loop.rs`, the `if tool_calls.is_empty()` arm) reads
//! `acc.tool_calls` — the COMPLETED calls — and a completed call is a thing the
//! wire's parser produces only after it has assembled the streamed fragments. A
//! parser that drops an assembled call therefore leaves `acc.tool_calls` empty
//! while the stream carried the call the whole way, and from where the
//! diagnostic has to be written that is indistinguishable from a model that
//! never called a tool at all. `TurnAccumulator::tool_call_deltas` is the
//! counter that separates them (one per `ProviderEvent::ToolCallDelta`, counted
//! at the top of `consume_stream`'s arm so it counts ARRIVALS, not outcomes),
//! and the message helper in `turn.rs` is what makes the resulting line
//! testable at all: every other `harness: ` site in the engine is an
//! untested `eprintln!`, because `[dev-dependencies]` carries no
//! stderr-capture crate.
//!
//! # What the behavioural tests can and cannot assert
//!
//! The line goes to BOTH sinks (`provider/mod.rs`'s `log_wire` is the
//! precedent, and this restates its finding rather than paraphrasing it more
//! cheerfully): the `eprintln!` half is the Worker's stderr, which the
//! Supervisor's drain task reads into `debuglog` ONLY with `ARCHIMEDES_DEBUG=1`
//! set; the `diagnostics::record` half takes no env var but is UNARMED until a
//! host injects a directory, and inside a Worker it is a no-op today — every
//! `diagnostics::init` in the crate is a test, arming lives in the desktop's
//! `run()` (the Supervisor, which a `--worker` child never reaches), and
//! `run_worker_inner` arms neither channel. So in a DEFAULT launch NEITHER half
//! reaches a file from inside a Worker (ADR 0110, "What this does not fix");
//! `provider/mod.rs` states that in full and this file used to contradict it.
//!
//! What that leaves assertable: a lib unit test can assert the `record` half —
//! the module arms the channel at a scratch dir itself, which is precisely the
//! arming production does not do — and CANNOT assert the `eprintln!` half, whose
//! only production reader is a drain task that needs a real spawned Worker
//! process. That asymmetry is `provider/mod.rs::the_chosen_wire_is_logged_at_construction`'s
//! own honest limit, restated here rather than papered over: the stderr line is
//! covered by sharing the payload this file asserts, not by an assertion on its
//! bytes.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::events::RpcEvent;
use crate::harness::provider::{FinishReason, ProviderEvent, ToolCallDelta};
use crate::harness::retry::RetryPolicy;

/// The streamed text of the deltas-but-nothing-dispatched script, pinned so the
/// recorded line's `N chars of streamed text` can be asserted byte for byte
/// rather than re-derived from the same literal by the assertion.
const STREAMED_TEXT: &str = "I will read it now.";
/// The number of `ToolCallDelta`s the script streams.
const STREAMED_DELTAS: usize = 3;

/// A `ToolCallDelta` (the shape the wires emit while a call is being assembled:
/// the first fragment carries the `id` + `name`, the rest only `arguments`).
fn delta(index: u32, id: Option<&str>, arguments: &str) -> ProviderEvent {
    ProviderEvent::ToolCallDelta(ToolCallDelta {
        index,
        id: id.map(str::to_string),
        name: id.map(|_| "read".to_string()),
        arguments: Some(arguments.to_string()),
    })
}

/// The script the defect rides: the call STREAMS (three deltas, the argument
/// fragments of a `read` call) and then the turn stops with NO `ToolCall` event
/// and a sentence of text — so `acc.tool_calls` is empty, the empty-turn gate
/// does not fire (text arrived), and the turn settles as a clean success having
/// dispatched nothing.
fn deltas_then_a_clean_stop() -> Vec<ProviderEvent> {
    vec![
        delta(0, Some("call_1"), "{\"pa"),
        delta(0, None, "th\": \"/e"),
        delta(0, None, "tc/passwd\"}"),
        ProviderEvent::TextDelta(STREAMED_TEXT.to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ]
}

/// A text-only reply — the shape the guard must NOT report (nothing was
/// streamed, so there is nothing to have lost).
fn text_only() -> Vec<ProviderEvent> {
    vec![
        ProviderEvent::TextDelta("nothing to run here".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ]
}

/// `tool_call_deltas` counts ONE per `ProviderEvent::ToolCallDelta`, whatever
/// became of the call, and stays 0 for a stream that carried none.
///
/// Driven through `consume_stream` rather than a whole turn, because the
/// counter's contract is about the stream arm alone. The WITNESS comes first:
/// the arm's own `toolcall_delta` `message_update` frames are counted out of the
/// event channel, so a counter reading 0 can only mean the increment is missing
/// — never that the events were dropped before the arm ran (the vacuity this
/// file's house pattern exists to prevent).
#[tokio::test]
async fn tool_call_deltas_counts_every_streamed_delta_and_zero_without_one() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (_space, mut loop_) = build_loop(
        // Never used: `consume_stream` is driven directly.
        Box::new(ScriptedProvider::new(vec![Some(text_only())]).0),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );

    let mut stream = futures_util::stream::iter(deltas_then_a_clean_stop()).boxed();
    let mut acc = emit::TurnAccumulator::default();
    let err = loop_
        .consume_stream(&mut stream, &CancellationToken::new(), &mut acc)
        .await;
    assert!(err.is_none(), "the script carries no error event");

    // The witness: the arm RAN three times (its emissions are on the channel),
    // so the count below is measuring the counter and not the script.
    let streamed_frames = drain_events(&mut events_rx)
        .iter()
        .filter(|e| {
            matches!(
                e,
                RpcEvent::message_update { assistant_message_event, .. }
                    if assistant_message_event["type"] == "toolcall_delta"
            )
        })
        .count();
    assert_eq!(
        streamed_frames, STREAMED_DELTAS,
        "the script's {STREAMED_DELTAS} deltas each reached the arm (otherwise a \
         zero count below would only prove the events never arrived)"
    );
    assert_eq!(
        acc.tool_call_deltas, STREAMED_DELTAS,
        "one per streamed delta, including the fragments of a call that was \
         never assembled into a `ToolCall`"
    );

    // The zero half, on a FRESH accumulator (the same `acc` would still hold
    // the three above, and a reused one could not tell "no increment" from
    // "no reset").
    let mut quiet = futures_util::stream::iter(text_only()).boxed();
    let mut acc2 = emit::TurnAccumulator::default();
    loop_
        .consume_stream(&mut quiet, &CancellationToken::new(), &mut acc2)
        .await;
    assert_eq!(
        acc2.tool_call_deltas, 0,
        "a text-only stream increments nothing"
    );
}

/// Arms the process-global diagnostics channel at a fresh scratch dir for one
/// test, and disables it again on the way out — the `provider/mod.rs` pair,
/// folded into ONE guard rather than copied as two locals.
///
/// The fold is what fixes the drop-order trap that module documents: a tuple or
/// two locals can easily drop the dir BEFORE the global is cleared, and the next
/// `record` anywhere in the process then `create_dir_all`s the deleted dir back
/// into existence and writes there for the rest of the run. Here `Drop::drop`
/// clears the global FIRST and the owned dir is removed only after the drop body
/// returns — the right order, by construction, for every caller.
fn arm_diagnostics(tag: &str) -> ArmedDiagnostics {
    let dir = crate::test_support::ScratchDir::in_env_temp("settle-diag-", tag);
    // The env lock is held by every caller across this call — the module's
    // stated contract for `init`.
    crate::diagnostics::init(Some(dir.to_path_buf()));
    ArmedDiagnostics { dir }
}

/// The armed scratch dir + its teardown (see [`arm_diagnostics`]).
struct ArmedDiagnostics {
    dir: crate::test_support::ScratchDir,
}
impl Drop for ArmedDiagnostics {
    fn drop(&mut self) {
        crate::diagnostics::init(None);
    }
}

/// The turn's recorded diagnostics lines — the file the ALWAYS-ON half of the
/// settle line lands in. Truncated by the caller before the turn, so every
/// assertion here is proven by a line THIS test wrote.
fn recorded_lines(dir: &std::path::Path) -> Vec<String> {
    let path = dir.join(crate::diagnostics::FILE_NAME);
    match std::fs::read_to_string(&path) {
        Ok(text) => text.lines().map(str::to_string).collect(),
        // `record` creates the file on its first write, so its absence means
        // nothing was recorded — which is a legitimate answer for the negative
        // test and must not be an error there.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("the scratch diagnostics file is readable: {e}"),
    }
}

/// Truncate the armed file so the next assertion is proven by its own line (the
/// `provider/mod.rs::reset_diagnostics_file` precedent — a sibling test that
/// reaches `record` without the env lock can append here).
fn reset_diagnostics_file(dir: &std::path::Path) {
    let path = dir.join(crate::diagnostics::FILE_NAME);
    match std::fs::write(&path, "") {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("the scratch diagnostics file can be truncated: {e}"),
    }
}

/// Drive one prompt against a scripted stream and hand back the turn's
/// `RpcEvent`s plus the diagnostics lines the turn recorded.
///
/// The diagnostics half is read AFTER the turn completed (`handle_prompt`
/// returns once the turn settled), so there is no race with the append.
async fn drive(script: Vec<ProviderEvent>, dir: &std::path::Path) -> (Vec<RpcEvent>, Vec<String>) {
    reset_diagnostics_file(dir);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(script)]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.handle_prompt(&text_prompt("read the file")).await;
    (drain_events(&mut events_rx), recorded_lines(dir))
}

/// THE defect this commit exists for: the tool call STREAMED (three deltas) and
/// the turn settled as a clean success having dispatched NOTHING. The settle
/// site now says so, on the always-on channel, naming the three facts that
/// identify the shape.
///
/// Asserted on the `record` half — the `eprintln!` half is unpinnable from
/// inside a lib test (its only production reader is the Worker-stderr drain
/// task, which needs a spawned Worker; see this file's header).
///
/// `#[allow(clippy::await_holding_lock)]` is INTENTIONAL and follows the
/// `provider_switch.rs` / `policy_gate.rs` precedent: the diagnostics target is
/// process-global, so the `env_lock` must stay held ACROSS the driven turn — a
/// sibling test that arms its own scratch dir mid-turn would otherwise steal the
/// target and this file's line would land in its file.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_turn_that_streamed_deltas_and_dispatched_none_records_the_line() {
    // The process-global diagnostics target is what `record` writes: the module
    // prescribes `init` under this lock, and the hold spans the driven turn.
    let _guard = crate::test_support::env_lock();
    let armed = arm_diagnostics("deltas");
    let (events, lines) = drive(deltas_then_a_clean_stop(), armed.dir.path()).await;

    // WITNESS 1: the settle block was walked — this IS the clean-success path.
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    for kind in ["message_end", "agent_end", "turn_end", "agent_settled"] {
        assert!(
            kinds.iter().any(|k| k == kind),
            "the turn took the success settle it is being diagnosed (no \
             `{kind}` in {kinds:?})"
        );
    }
    assert!(
        !kinds.iter().any(|k| k == "auto_retry_start"),
        "the turn was NOT retried — an abandoned attempt never reaches this \
         settle, so a retry here would mean the line was credited to the wrong \
         path, got {kinds:?}",
    );

    // WITNESS 2: the deltas reached the loop (this file's counter test proves
    // the counter is fed by that arm; without a witness a missing line could be
    // blamed on a script that never streamed anything).
    let deltas = events
        .iter()
        .filter(|e| {
            matches!(
                e,
                RpcEvent::message_update { assistant_message_event, .. }
                    if assistant_message_event["type"] == "toolcall_delta"
            )
        })
        .count();
    assert_eq!(
        deltas, STREAMED_DELTAS,
        "the script's deltas were consumed by the turn"
    );

    let hits: Vec<&String> = lines
        .iter()
        .filter(|l| l.ends_with(&expected_record()))
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "the settle records EXACTLY ONE line for the shape, and records it with \
         no `harness: ` prefix (the `log_wire` convention); got:\n{lines:#?}"
    );
}

/// The `record` payload for the script above, byte for byte (`record` prefixes
/// `<epoch-millis> `, so the assertion is a suffix match).
fn expected_record() -> String {
    format!(
        "settled with no tool call dispatched: finish_reason `stop` with {} chars \
         of streamed text and {} streamed tool-call delta(s) — a call was streamed \
         and never dispatched, so the turn settled as a success with nothing run \
         (where the call went is the provider parser's account)",
        STREAMED_TEXT.chars().count(),
        STREAMED_DELTAS,
    )
}

/// The message helper is PURE and takes three scalars, which is the whole
/// reason it exists: every other `harness: ` site in the engine is an
/// `eprintln!` inline in a `&mut self` method and therefore untestable (there
/// is no stderr-capture dev-dependency). This pins the three facts it must name
/// and the one condition it must stay silent on.
#[test]
fn the_message_names_the_finish_reason_the_text_length_and_the_delta_count() {
    let msg = turn::settled_with_nothing_dispatched(Some(FinishReason::Stop), 4213, 7)
        .expect("a non-zero delta count produces a message");
    assert!(
        msg.contains("finish_reason `stop`"),
        "the finish reason is named in the same snake-case-in-backticks spelling \
         `handle_truncated_turn` uses, so the two sibling lines read alike; got \
         {msg:?}"
    );
    assert!(
        msg.contains("4213 chars of streamed text"),
        "the streamed text LENGTH rides the line as a count (never the text — \
         `diagnostics` forbids a model answer at a `record` site); got {msg:?}"
    );
    assert!(
        msg.contains("7 streamed tool-call delta(s)"),
        "the counter — the fact that separates 'the model called nothing' from \
         'a call was streamed and dropped'; got {msg:?}"
    );
    // The prefix convention: `eprintln!` carries `harness: `, `record` does
    // not, so the helper answers the PREFIX-FREE form and the call site adds
    // it for the stderr half (the `log_wire` split).
    assert!(
        !msg.starts_with("harness:"),
        "the helper answers what `record` takes; got {msg:?}"
    );
    // The consequence clause, em-dash separated (the house style: `— treating
    // as a stop`, `— terminal, not retried`).
    assert!(
        msg.contains('—'),
        "the consequence clause is introduced by an em dash like every other \
         `harness: ` line; got {msg:?}"
    );

    // Each finish reason the settle can carry (the `Length` case cannot reach
    // here — `handle_truncated_turn` returns first — but the helper is total
    // over the enum, and a line that mis-named the reason would be worse than
    // no line at all).
    for (reason, spelling) in [
        (FinishReason::Stop, "`stop`"),
        (FinishReason::ToolCalls, "`tool_calls`"),
        (FinishReason::Length, "`length`"),
        (FinishReason::Error, "`error`"),
    ] {
        let msg = turn::settled_with_nothing_dispatched(Some(reason), 1, 1)
            .expect("deltas streamed, so a message");
        assert!(
            msg.contains(&format!("finish_reason {spelling}")),
            "`{reason:?}` is spelled {spelling:?} on the line, as serde spells it \
             on the wire; got {msg:?}"
        );
    }

    // `None`: a stream that ended with no `finish_reason` at all — one of the
    // shapes the #64 gate's comment says used to settle clean, so the helper
    // has to say so rather than invent a reason.
    let msg = turn::settled_with_nothing_dispatched(None, 1, 1).expect("deltas streamed");
    assert!(
        msg.contains("finish_reason `none`"),
        "a missing finish reason is reported as `none`, not fabricated; got {msg:?}"
    );
}

/// The guard, tested at the helper: zero deltas answer NO message. This is the
/// difference between a diagnostic and a false alarm on every ordinary final
/// answer in the tree.
#[test]
fn no_deltas_produce_no_message() {
    for finish in [
        Some(FinishReason::Stop),
        Some(FinishReason::ToolCalls),
        None,
    ] {
        assert_eq!(
            turn::settled_with_nothing_dispatched(finish, 0, 0),
            None,
            "zero deltas is the ordinary text-only settle — nothing arrived, so \
             nothing can have been lost, whatever the finish reason or the text \
             length ({finish:?})"
        );
        // The text length does not open the gate either: an EMPTY reply with
        // deltas is still the shape worth reporting (and the empty reply the
        // #64 gate handles can never reach here — it `continue`s or `return`s).
        assert!(
            turn::settled_with_nothing_dispatched(finish, 0, 1).is_some(),
            "zero streamed text with a delta is still the reported shape ({finish:?})"
        );
    }
}

/// The guard is REAL: a text-only reply — no delta ever streamed — records no
/// line. Without this test the new site would fire on every ordinary final
/// answer in the tree, and the always-on file would carry a failure sentence
/// for every successful turn.
///
/// `#[allow(clippy::await_holding_lock)]` for the same reason the positive test
/// carries it — the process-global diagnostics target must not be re-armed by a
/// sibling test while this turn is running.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_text_only_turn_records_no_line() {
    let _guard = crate::test_support::env_lock();
    let armed = arm_diagnostics("text-only");
    let (events, lines) = drive(text_only(), armed.dir.path()).await;

    // The witness: the SAME settle block was walked (the turn settled as a
    // clean success with nothing dispatched) — so the absence below is the
    // guard holding, not the code path being unreachable.
    let kinds: Vec<String> = events.iter().map(|e| e.kind().to_string()).collect();
    assert!(
        kinds.iter().all(|k| k != "toolcall_delta"),
        "the script streamed no delta at all, got {kinds:?}"
    );
    for kind in ["agent_end", "turn_end", "agent_settled"] {
        assert!(
            kinds.iter().any(|k| k == kind),
            "the text-only turn took the same success settle (no `{kind}` in \
             {kinds:?}) — otherwise the empty assertion below is vacuous"
        );
    }

    let hits: Vec<&String> = lines
        .iter()
        .filter(|l| l.contains("settled with no tool call dispatched"))
        .collect();
    assert!(
        hits.is_empty(),
        "a turn that streamed no tool-call delta has nothing to have lost, so \
         the guard must not fire; got:\n{lines:#?}"
    );

    // The POSITIVE CONTROL, so the assertion above cannot be vacuous by way of
    // a channel that records nothing at all: with the counter forced NON-zero
    // on an accumulator no turn is using, the same armed channel takes the same
    // settle line. If this second half ever stops writing, the empty assertion
    // above has stopped meaning anything.
    let control = turn::settled_with_nothing_dispatched(
        Some(FinishReason::Stop),
        STREAMED_TEXT.chars().count(),
        STREAMED_DELTAS,
    )
    .expect("a non-zero delta count always produces the message");
    crate::diagnostics::record(&control);
    let after = recorded_lines(armed.dir.path());
    assert!(
        after.iter().any(|l| l.ends_with(&control)),
        "the armed channel DOES take this line — so the empty assertion above \
         measured the guard, not a dead sink; got:\n{after:#?}"
    );
}

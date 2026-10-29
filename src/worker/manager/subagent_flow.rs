use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::watch;

use crate::agent::events::RpcEvent;
use crate::agent::harness::prompt::build_child_system_message;
use crate::agent::harness::provider::openai::content_is_effectively_empty;
use crate::agent::harness::r#loop::tool_specs;
use crate::agent::harness::{ModelKey, ModelRef};
use crate::agent::is_bookkeeping_message_id;
use crate::agent::session::resolve_composed_model;
use crate::agent::subagent::{SubagentFailureReason, SubagentMetrics, SubagentOutcome};
use crate::agent::types::mint_session_id;
use crate::agent::worker::client::{WorkerHandle, WorkerInboundEvent};
use crate::agent::worker::protocol::{Inbound, StartEnv, StartMode, SubagentDispatchWire};

use super::pump::start_pump;
use super::registry::{reap_worker, ManagerState};

/// The drive's BOUNDED grace for the pump's exit signal (ADR 0055 review,
/// thread 3): when the read task's `exited` watch fires FIRST, the drive waits
/// this long for the pump to publish its OWN exit (the source that guarantees a
/// fully-fed `SubagentCapture`) before salvaging what it has.
///
/// It is a grace, NOT a correctness window: in the normal case the pump's
/// `Exited` is already queued behind frames the child wrote in one burst and
/// lands within microseconds, so this timeout is only ever paid by a pump that
/// is genuinely wedged (parked inside a router callback). The bound exists so
/// such a pump cannot hold the child's teardown — and the parent's `SubagentResult`
/// — open indefinitely. Test stalls MUST stay shorter than this, or a test
/// exercises the fallback instead of the ordering it means to force.
pub(crate) const PUMP_EXIT_GRACE: Duration = Duration::from_secs(1);

/// The `SubagentOutcome` delivery target (the `dispatch_subagent` flow's
/// two callers): the REQUESTING (main-session) Worker (the
/// `IpcDispatcher` waiter resolves — the `SubagentResult` frame) or the
/// in-process `dispatch_native`'s oneshot (the `SubagentSessionManager`
/// re-plumb — ADR 0025 Task 5; there is no requesting Worker — the
/// outcome resolves the oneshot directly).
pub(crate) enum SubagentOutcomeSink {
    /// The requesting (main-session) Worker (the `SubagentResult` frame
    /// — the `IpcDispatcher` waiter resolves; a dropped waiter is a
    /// no-op).
    Requester,
    /// The in-process `dispatch_native`'s oneshot (an `Arc`'d sender —
    /// `deliver` is a shared-reference method, so the `send`'s move is
    /// of the `Arc`, not the sender; a dropped receiver is a no-op).
    Oneshot(
        std::sync::Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<SubagentOutcome>>>>,
    ),
}

impl SubagentOutcomeSink {
    /// Deliver the outcome (the `Requester` sends the `SubagentResult`
    /// frame to the requesting Worker; the `Oneshot` resolves the
    /// sender). A miss (a dead Worker / a dropped receiver) is a no-op.
    pub(crate) fn deliver(
        &self,
        state: &ManagerState,
        parent_session_id: &str,
        wire: &SubagentDispatchWire,
        outcome: SubagentOutcome,
    ) {
        match self {
            SubagentOutcomeSink::Requester => {
                let _ = send_subagent_result(state, parent_session_id, wire, outcome);
            }
            SubagentOutcomeSink::Oneshot(slot) => {
                if let Some(tx) = slot.lock().unwrap_or_else(|p| p.into_inner()).take() {
                    let _ = tx.send(outcome);
                }
            }
        }
    }
}

/// The `SubagentCapture` (reviewer-corrected: capture from the subagent's
/// `SinkFrame` `session-update` stream — the enveloped `agent_message_chunk`
/// frames — the last-`messageId`-with-non-empty-text rule; NOT from the
/// `message_end` `RpcEvent`s, which are a different shape and carry no text
/// for tool-call messages). The `message_update` `usage` fields feed the
/// metrics' token fields (the `SubagentMetrics` accumulation).
pub struct SubagentCapture {
    /// `messageId` → accumulated text (a `HashMap` has no insertion
    /// order — the LAST id is tracked separately).
    text: StdMutex<HashMap<String, String>>,
    /// The LAST `messageId` with a non-empty capture (updated on every
    /// non-empty capture of a chunk that is NOT harness bookkeeping — the
    /// final-output source; the bookkeeping chunks (`normalize::
    /// is_bookkeeping_message_id`: the shared one-liner key and the
    /// `retract:N` discard line, EITHER stamped — `{stamp}:system` /
    /// `{stamp}:retract:N`, the shape production emits (ADR 0049) — OR bare,
    /// which is the pre-stamp id shape: the test fixtures that hand-write these
    /// ids and `TurnState::default()`'s empty stamp) are NEVER captured: a
    /// failed child turn must not settle with a one-liner as its output, and a
    /// parent must never read a discard line as its child's answer). NOTE the
    /// scope: `observe_sink_frame` reads LIVE frames off the Worker's stdout and
    /// never touches the `messages` table, so the bare shape is accepted for an
    /// id with NO stamp — not because any stored database is re-read here.
    last_message_id: StdMutex<Option<String>>,
    /// The `message_update` `usage` accumulation (the `inputTokens` /
    /// `outputTokens` fields — the MAX across the frames: a turn's
    /// usage is cumulative per message, the last is the largest).
    usage: StdMutex<(u64, u64)>,
    /// The child's **cost** accumulation (issue #87, T09): the SUM of the
    /// `costUsd` figures on the child's own `cost_update` interactive-event
    /// frames. `None` until a priced frame arrives — the distinction between
    /// "no call reported a cost" and "a call reported zero" is preserved HERE
    /// and carried through the `SubagentMetrics.cost: Option<f64>` hand-off
    /// (issue #107) — no `unwrap_or(0.0)`, no named follow-up, not a decision
    /// to invent money.
    ///
    /// A SUM, deliberately NOT a `max` like the token fields next door: a
    /// turn's token usage is cumulative per message (so the last frame is the
    /// largest), but money accumulates across calls — copying the `max` rule
    /// here would report one call's cost for a ten-call child.
    cost: StdMutex<Option<f64>>,
    /// (issue #122) Did the child announce ANY tool call? Set on the first
    /// `tool_execution_start` the pump feeds.
    ///
    /// It exists because "no text" and "no output" are NOT the same claim. A
    /// child that ran a tool (or was parked on a permission gate — which is a
    /// tool call held open) and then settled without a closing message did REAL
    /// WORK, and ADR 0055 requires that to stay a `Completed`: a bash call that
    /// fixed a file has nothing to say afterwards, and its parent must not be
    /// told the child failed (pinned by
    /// `manager::tests::a_child_inside_a_tool_call_is_never_reaped` and
    /// `manager::tests::an_answered_gates_card_is_not_closed_again_at_teardown`).
    /// Only a child that produced NEITHER text NOR a tool boundary settled with
    /// nothing at all — and only that is the empty-`Completed` the #122 guard
    /// reclassifies.
    saw_tool_activity: std::sync::atomic::AtomicBool,
}

impl Default for SubagentCapture {
    fn default() -> Self {
        Self::new()
    }
}

impl SubagentCapture {
    pub fn new() -> Self {
        Self {
            text: StdMutex::new(HashMap::new()),
            last_message_id: StdMutex::new(None),
            usage: StdMutex::new((0, 0)),
            cost: StdMutex::new(None),
            saw_tool_activity: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Observe one `SinkFrame`.
    ///
    /// Two event names matter, and nothing else does:
    /// - `session-update` — the `agent_message_chunk` frames that are the
    ///   final-text source (the ENVELOPED shape: `{ sessionId, update }`; the
    ///   `update`'s `sessionUpdate` discriminator is `agent_message_chunk`),
    ///   and a `message_retracted` frame (ADR 0048) which ERASES the Abandoned
    ///   attempt's text for the `messageId` it names.
    /// - `interactive-event` — the child's own `cost_update` frames (issue #87,
    ///   T09), whose `payload.costUsd` sums into this capture's cost. Without
    ///   this branch a child's money reached the parent's Supervisor and was
    ///   dropped ON THE FLOOR here, which is why `SubagentMetrics.cost` was a
    ///   hardcoded `0.0` and why the roadmap's "rolls up through the existing
    ///   path" was false.
    pub fn observe_sink_frame(&self, event: &str, payload: &Value) {
        // (issue #87, T09) The child's money, BEFORE the `session-update`
        // early return — the cost frames are `interactive-event`s and would
        // otherwise never be read at all. Scoped to the ONE event name (pin:
        // `observe_sink_frame_still_ignores_unrelated_interactive_frames`), so
        // the `todos_update` push on the same channel cannot move a figure.
        if event == "interactive-event" {
            if payload.get("event").and_then(Value::as_str) == Some("cost_update") {
                // The SAME sanitising rule the provider readers use: a malformed
                // or negative figure is not money. `is_finite()` is defence in
                // depth (`as_f64` does not self-filter). `>= 0.0` and NOT
                // `!= 0.0`: a real `Some(0.0)` IS a figure and a filter on zero
                // would eat it.
                let reported = payload
                    .get("payload")
                    .and_then(|p| p.get("costUsd"))
                    .and_then(Value::as_f64)
                    .filter(|c| c.is_finite() && *c >= 0.0);
                if let Some(cost) = reported {
                    // Sign-normalised (`-0.0 + 0.0` is `+0.0`) so the roll-up
                    // can never carry a signed zero onto the wire.
                    let cost = cost + 0.0;
                    let mut held = self.cost.lock().unwrap_or_else(|p| p.into_inner());
                    *held = Some(held.unwrap_or(0.0) + cost);
                }
            }
            return;
        }
        if event != "session-update" {
            return;
        }
        let update = payload.get("update");
        // (ADR 0048) A Retraction discards the abandoned attempt's text HERE too.
        // This map is what `captured_text()` hands the PARENT as the subagent tool
        // result, so a retried child would otherwise feed the parent `partdone`.
        if update
            .and_then(|u| u.get("sessionUpdate"))
            .and_then(Value::as_str)
            == Some("message_retracted")
        {
            if let Some(id) = update
                .and_then(|u| u.get("messageId"))
                .and_then(Value::as_str)
            {
                self.text
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(id);
                let mut last = self
                    .last_message_id
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if last.as_deref() == Some(id) {
                    *last = None;
                }
            }
            return;
        }
        if update
            .and_then(|u| u.get("sessionUpdate"))
            .and_then(Value::as_str)
            != Some("agent_message_chunk")
        {
            return;
        }
        let mid = update
            .and_then(|u| u.get("messageId"))
            .and_then(Value::as_str);
        let delta = update
            .and_then(|u| u.get("content"))
            .and_then(|c| c.get("text"))
            .and_then(Value::as_str);
        if let (Some(mid), Some(delta)) = (mid, delta) {
            if !delta.is_empty() && !is_bookkeeping_message_id(mid) {
                let mut text = self.text.lock().unwrap_or_else(|p| p.into_inner());
                text.entry(mid.to_string()).or_default().push_str(delta);
                *self
                    .last_message_id
                    .lock()
                    .unwrap_or_else(|p| p.into_inner()) = Some(mid.to_string());
            }
        }
    }

    /// Observe one `RpcEvent` (the `message_update` `usage` field —
    /// the metrics' token fields; the other events are ignored).
    pub fn observe_event(&self, event: &RpcEvent) {
        // (issue #122) The tool-activity marker (see `saw_tool_activity`): a
        // child that announced a tool call DID something, even if it never
        // streamed a word, so its empty settle is not the #122 shape.
        if matches!(event, RpcEvent::tool_execution_start { .. }) {
            self.saw_tool_activity
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        if let RpcEvent::message_update { usage, .. } = event {
            let input = usage
                .get("inputTokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let output = usage
                .get("outputTokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let mut u = self.usage.lock().unwrap_or_else(|p| p.into_inner());
            u.0 = u.0.max(input);
            u.1 = u.1.max(output);
        }
    }

    /// The last-`messageId`-with-non-empty-text ("" when no chunk
    /// arrived — a tool-using turn does NOT concatenate every
    /// intermediate message).
    pub fn captured_text(&self) -> String {
        let last = self
            .last_message_id
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        last.and_then(|id| {
            self.text
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&id)
                .cloned()
        })
        .unwrap_or_default()
    }

    /// The accumulated usage (the metrics' token fields — the MAX
    /// across the `message_update` frames).
    pub fn usage(&self) -> (u64, u64) {
        *self.usage.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn input_tokens(&self) -> u64 {
        self.usage().0
    }

    /// (issue #122) Did the child announce a tool call at any point? The
    /// settle guard reads this to tell a child that DID work and had nothing to
    /// say (a legitimate `Completed` — ADR 0055) from one that produced neither
    /// text nor a tool boundary (the empty-`Completed` this guard reclassifies).
    pub fn saw_tool_activity(&self) -> bool {
        self.saw_tool_activity
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn output_tokens(&self) -> u64 {
        self.usage().1
    }

    /// The child's summed cost (`None` when NO frame carried a figure — a
    /// different state from `Some(0.0)`, which means a call reported a real
    /// zero). Issue #87, T09.
    pub fn cost_usd(&self) -> Option<f64> {
        *self.cost.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// The subagent's liveness signal (ADR 0055): `seq` bumps on EVERY frame the
/// pump sees for this child, and `in_flight` holds the `tool_call_id`s that
/// have been ANNOUNCED (`tool_execution_start`) and not yet closed
/// (`tool_execution_end`) — NOT "currently executing" (a batch announces every
/// call before running any of them, so the set is deliberately generous).
/// The skew is one-directional: it can only keep a child alive longer, never
/// cut it sooner.
///
/// A `HashSet` and NOT a counter, deliberately: ADR 0048 has the cancel and
/// turn-abandoned paths emit `tool_execution_end` for calls that were announced
/// but never dispatched, so an `end` without a `start` is LEGAL. A counter would
/// go negative and un-block a child that is still working; a set's removal is
/// idempotent.
#[derive(Debug, Default)]
pub struct SubagentLiveness {
    pub seq: u64,
    pub in_flight: std::collections::HashSet<String>,
    /// The **gates** this child opened that the user has not answered
    /// (`PermissionRequest` / `InteractiveRequest` seen, answer not yet relayed).
    /// Drained at teardown to close the child's own cards (ADR 0055) — on
    /// `SubagentLiveness`, NOT a second `SubagentDrive` field, so the pump
    /// reaches it through the one `feed_capture` borrow; two overlapping sets on
    /// the drive would drift.
    ///
    /// The ids are the gate's `requestId` VERBATIM (see `note_liveness_gate`),
    /// which is NOT the same set as `in_flight`'s tool-call ids: the sudo flow's
    /// sub-prompts carry the DERIVED `"{tool_call_id}:confirm"` / `":password"`
    /// forms, and those are exactly the ids the Client stored its card under.
    pub open_gates: std::collections::HashSet<String>,
}

/// Bump the liveness clock for ONE frame the pump saw (ADR 0055). A
/// `SinkFrame` — the child's `session-update` text stream — is NOT an
/// `RpcEvent` and carries no tool boundary at all, so the frame count is a
/// SEPARATE fact from the outstanding-call set: a child that only streams
/// tokens is an **Active Subagent** and must register as such.
pub(crate) fn note_liveness_frame(liveness: &mut SubagentLiveness) {
    liveness.seq += 1;
}

/// Bump the liveness clock for one `RpcEvent` AND maintain the outstanding
/// `tool_call_id` set (ADR 0055): the set means "ANNOUNCED and not yet closed",
/// so `tool_execution_start` adds the id (the announce fires while the Provider
/// still streams the `toolCall` — before the batch executes anything) and
/// `tool_execution_end` removes it. The removal is idempotent on purpose: an
/// `end` whose call was never announced is a LEGAL frame (ADR 0048's cancel /
/// turn-abandoned paths close announced-but-never-dispatched calls), and a
/// child parked on a **gate** holds its id open for the whole wait.
///
/// Everything else leaves the set alone — the clock still advances.
pub(crate) fn note_liveness_event(liveness: &mut SubagentLiveness, event: &RpcEvent) {
    liveness.seq += 1;
    match event {
        RpcEvent::tool_execution_start { tool_call_id, .. } => {
            liveness.in_flight.insert(tool_call_id.clone());
        }
        RpcEvent::tool_execution_end { tool_call_id, .. } => {
            liveness.in_flight.remove(tool_call_id);
            // NO `open_gates` removal here (ADR 0055 review): a tool close is
            // NOT an answer. On Stop the child's permission gate resolves
            // `Cancelled`, the loop FABRICATES a "permission denied" tool
            // result, and the matching `tool_execution_end` arrives for a card
            // that was never closed — removing the gate here would decide, by a
            // race between this pump arm and `finish_subagent`'s snapshot,
            // whether the user's dialog leaks forever. Clearing happens ONLY at
            // the true answer moment (`WorkerManager::note_gate_answered`).
        }
        _ => {}
    }
}

/// Record one unanswered **gate** (ADR 0055): the child's
/// `PermissionRequest` / `InteractiveRequest` frame announces a gate the user
/// has not answered, so teardown must retract the card the Client rendered.
///
/// The id is recorded EXACTLY as it arrived, because `finish_subagent` emits
/// these ids VERBATIM as `interactive-request-close` `requestId`s and the
/// Client's `removeRequest` / `removePrompt` (`src/store/interactive.ts`) filter
/// by an EXACT `requestId` match. A derived id is what the Client stored under:
/// `sudo_run_flow` emits `requestId: "{tool_call_id}:confirm"` / `":password"`
/// (`src/agent/interactive/mod.rs`), so stripping the suffix here would record
/// an id no card was ever filed under — the sudo dialogs of a reaped child would
/// survive forever. The permission gate's `requestId` IS the bare `tool_call_id`
/// (`gate_tool` passes `&tc.id` to `native_permission_gate`), so it needs no
/// transformation at all: one id space, one verbatim rule.
///
/// Touches ONLY `open_gates`: the gate's tool call is already covered by
/// `in_flight`, and the frame-count contract stays owned by the pump's
/// `SinkFrame` / `Event` arms.
pub(crate) fn note_liveness_gate(liveness: &mut SubagentLiveness, request_id: &str) {
    liveness.open_gates.insert(request_id.to_string());
}

/// The in-flight subagent dispatch's bookkeeping (the `drives` map
/// value — the pump reads it on the subagent's `agent_settled` /
/// `Exited`; the drive task removes it on the UNCONDITIONAL teardown):
/// the settle watch (the pump flips it on `agent_settled`), the cancel
/// watch (the `SubagentCancel` flow — `cancel_subagent` flips it), the
/// `SubagentCapture` (the pump feeds it; the drive task reads the
/// final text on settle), the handle (the `cancel_subagent`'s
/// `send_abort`), and the `reaping` flag (the drive task's teardown
/// sets it — the `Exited` is EXPECTED: NO `on_crash`).
pub(crate) struct SubagentDrive {
    /// The PARENT session id (the subagent's UI belongs to the
    /// parent's scope — the pump delivers the subagent's frames
    /// under this id + the `is_subagent` flag; the `on_event`
    /// router uses it for the UI delivery, the flag for the
    /// subagent-specific handling).
    pub(crate) parent_session_id: String,
    /// The DISPATCH id (the `SubagentDispatchWire`'s `id` — what the
    /// requesting Worker's `IpcDispatcher` cancel watcher names in its
    /// `SubagentCancel` frame; the `drives` map is keyed by the CHILD
    /// session id, so the frame's id is matched through this field).
    pub(crate) dispatch_id: String,
    pub(crate) settle_tx: watch::Sender<bool>,
    pub(crate) cancel_tx: watch::Sender<bool>,
    /// The child's process exit, published BY THE PUMP (ADR 0055 review,
    /// thread 3 — the crash salvage raced the pump).
    ///
    /// `WorkerHandle::exited()` is a DIFFERENT channel from the frame
    /// `broadcast`: the read task publishes it the moment `child.wait()`
    /// returns, which is BEFORE the frames the child wrote last have been
    /// consumed. A drive that ends on that watch can therefore snapshot the
    /// `SubagentCapture` mid-queue and freeze a TRUNCATED salvage (the lost
    /// tail is unrecoverable — the `Failed` outcome is delivered once).
    ///
    /// THIS watch is fired from the pump's own `WorkerInboundEvent::Exited`
    /// arm, i.e. after every frame the child managed to write has gone
    /// through `feed_capture` — the `broadcast` is IN ORDER, so `Exited`
    /// cannot arrive before a preceding `agent_message_chunk` /
    /// `message_update`. Reading the salvage off this signal is what makes a
    /// crashed child's complete output + real usage arrive at the parent.
    pub(crate) exit_tx: watch::Sender<Option<i32>>,
    /// The child's liveness signal (ADR 0055 — the `SubagentLiveness`
    /// doc): the pump bumps it on EVERY frame it sees for this child
    /// and maintains the outstanding `tool_call_id` set. The drive
    /// task's receiver half is threaded through `drive_subagent`.
    pub(crate) liveness_tx: watch::Sender<SubagentLiveness>,
    pub(crate) capture: Arc<SubagentCapture>,
    pub(crate) handle: Option<WorkerHandle>,
    pub(crate) reaping: bool,
}

/// Feed the subagent's `SubagentCapture` AND its `SubagentLiveness` (the
/// `drives` map entry — `None` when the dispatch is gone); the closure
/// returns `true` when the settle watch should be flipped (the
/// `agent_settled` event).
///
/// The capture borrow stays IMMUTABLE on purpose (`observe_sink_frame` /
/// `observe_event` take `&self` — they mutate through their internal
/// `StdMutex`es), and the liveness value is mutated in place through the
/// drive's `watch` sender (`send_modify` takes `&self`, so the two field
/// borrows stay disjoint).
pub(crate) fn feed_capture(
    state: &ManagerState,
    session_id: &str,
    f: impl FnOnce(&SubagentCapture, &mut SubagentLiveness) -> bool,
) {
    let mut drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
    let Some(drive) = drives.get_mut(session_id) else {
        return;
    };
    // The disjoint-field borrows made explicit (the capture is read through
    // the `Arc`, the liveness value is written through the `watch`).
    let SubagentDrive {
        capture,
        liveness_tx,
        settle_tx,
        ..
    } = drive;
    let mut settle = false;
    liveness_tx.send_modify(|liveness| {
        settle = f(capture, liveness);
    });
    if settle {
        let _ = settle_tx.send(true);
    }
}

/// Publish the child's process exit through the DRIVE's `exit` watch (the
/// pump's `Exited` arm calls this AFTER it has fed every frame the child
/// managed to write — see the `SubagentDrive::exit_tx` doc for why the pump
/// and not the read task owns this signal).
///
/// A MISSING drive is a NO-OP and not a bug: the drive task removes its own
/// entry at teardown, so an `Exited` that arrives after the dispatch is gone
/// (the reap's own `Exited` — an EXPECTED exit) has nobody left to notify.
/// `send_replace` (NOT `send` — the drive's receiver subscribes at dispatch
/// time, but the value must land even if it had already dropped, and a stale
/// code must never be readable as the current one).
pub(crate) fn publish_pump_exit(state: &ManagerState, session_id: &str, code: Option<i32>) {
    let drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(drive) = drives.get(session_id) {
        drive.exit_tx.send_replace(code);
    }
}

/// Fire one in-flight dispatch's cancel (the `SubagentCancel` flow's two
/// actions): the child's `send_abort` (its turn token cancels — the
/// session stays alive until the reap) + the drive task's `cancel` flag
/// (the `Cancelled` outcome — the UNCONDITIONAL teardown reaps the child).
pub(crate) fn cancel_drive(drive: &SubagentDrive) {
    if let Some(handle) = &drive.handle {
        let _ = handle.send_abort();
    }
    let _ = drive.cancel_tx.send(true);
}

/// Cancel by the CHILD session id (the in-process `dispatch_native`'s
/// `SubagentCancel` targets the pre-minted id). A gone drive is a NO-OP
/// (the dispatch already settled — the cancel is idempotent).
pub(crate) fn cancel_subagent_by_child_id(state: &ManagerState, child_id: &str) {
    let drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(drive) = drives.get(child_id) {
        cancel_drive(drive);
    }
}

/// Cancel by the DISPATCH id (the Worker's `IpcDispatcher` cancel watcher
/// — the `SubagentCancel { id }` frame carries the dispatch id, NOT a
/// session id). A dispatch that already settled is a NO-OP.
pub(crate) fn cancel_subagent_by_dispatch_id(state: &ManagerState, dispatch_id: &str) {
    let drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(drive) = drives.values().find(|d| d.dispatch_id == dispatch_id) {
        cancel_drive(drive);
    }
}

/// The `dispatch_subagent` flow (the `Arc`-backed form — the `&self`
/// method delegates here; the pump task calls it with its `Arc`
/// clone). `sink` is the `SubagentOutcome` delivery target (the
/// `Requester` Worker / the in-process `dispatch_native`'s oneshot);
/// `pre_minted` is the PRE-MINTED child session id (the in-process
/// `dispatch_native`'s `SubagentCancel` targets it — `None` → the flow
/// mints its own, the `Requester` path).
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch_subagent(
    state: Arc<ManagerState>,
    dead_bound: Duration,
    parent_session_id: String,
    wire: SubagentDispatchWire,
    sink: SubagentOutcomeSink,
    pre_minted: Option<String>,
) {
    // The parent's `attach`-time start envelope (the effective
    // `catalog` / `config_dir` / `trusted` — the Supervisor owns
    // them). The parent is gone (detached / reaped) → the
    // dispatch is dropped (the requester's `IpcDispatcher` waiter
    // is never resolved — its `SubagentWait` select sees the
    // dropped oneshot as `Cancelled` — parity with the current
    // driver's "worker task vanished" behavior).
    let Some(parent_env) = state
        .workers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&parent_session_id)
        .map(|e| e.env.clone())
    else {
        return;
    };
    // 1. The child `Model` (the `launch.model` override — a
    // trailing `:<level>` suffix is stripped FIRST — the REAL
    // `resolve_composed_model` splits `provider/id` only; the
    // suffix is a candidate thinking level (step 2, when
    // `launch.thinking` is `None`)); `None` → the `model_key`
    // (the loop's pre-flight resolved it against the Worker's
    // catalog). An unresolvable key is `Failed` (the current
    // driver's "unknown model" — NO fallback).
    // NOTE: ADR 0023 chain — see `resolve_model_ref` (not yet
    // unified). The last-`:` strip is `ModelRef::parse`; a ref
    // whose BARE part is not a valid key (no `/`) parses to
    // `None` — the old code used the WHOLE string as the bare key
    // (a `"a:b"` key: old message `unknown model: a`, new
    // `unknown model: a:b`). Both are unresolvable (a key without
    // `/` can never be in the catalog) — same `Failed` outcome;
    // only the error MESSAGE on a pathological input differs.
    let (model, level_suffix) = match &wire.launch.model {
        Some(key) => {
            let (bare, level) = match ModelRef::parse(key) {
                Some(ref_) => (ref_.key.to_string(), ref_.level),
                None => (key.clone(), None),
            };
            match resolve_composed_model(&parent_env.catalog, &bare) {
                Some(m) => (m, level),
                None => {
                    sink.deliver(
                        &state,
                        &parent_session_id,
                        &wire,
                        preflight_failed(format!("unknown model: {bare}")),
                    );
                    return;
                }
            }
        }
        None => match resolve_composed_model(&parent_env.catalog, &wire.model_key) {
            Some(m) => (m, None),
            None => {
                sink.deliver(
                    &state,
                    &parent_session_id,
                    &wire,
                    preflight_failed(format!("unknown model: {}", wire.model_key)),
                );
                return;
            }
        },
    };
    // 2. The child thinking level: explicit > the `:<level>` suffix
    // of the resolved model key > the frontmatter's `thinking` (the
    // doc-correct order — the frontmatter's `thinking` is the LAST
    // rung). Validate against `model.thinking_levels` (when
    // non-empty — an empty set soft-passes) — a mismatch is
    // DROPPED (never sent upstream as a bogus `reasoning_effort`).
    let mut thinking = wire
        .launch
        .thinking
        .clone()
        .or(level_suffix)
        .or(wire.launch.frontmatter_thinking.clone());
    if let Some(level) = &thinking {
        if !model.supports_thinking_level(level) {
            thinking = None;
        }
    }
    // 3. The child tool set (the FULL three-way rule — the current
    // driver's step 7, moved verbatim): `launch.tools` `Some` →
    // VERBATIM minus `subagent`/`list_agents` (a non-empty set that
    // empties out yields NO tools — NOT re-expanded);
    // `launch.tools` `None` → the parent's list minus the guard;
    // the `[]` (empty parent list) case → ALL tools minus the
    // guard (the "empty = all" expansion applies ONLY to the
    // inherited-parent case — the `parent_enabled_tools` `[]` is
    // the documented harness convention).
    let child_tools: Vec<String> = match &wire.launch.tools {
        Some(tools) => tools
            .iter()
            .filter(|t| !matches!(t.as_str(), "subagent" | "list_agents"))
            .cloned()
            .collect(),
        None => {
            if wire.parent_enabled_tools.is_empty() {
                tool_specs()
                    .into_iter()
                    .map(|t| t.name)
                    .filter(|t| !matches!(t.as_str(), "subagent" | "list_agents"))
                    .collect()
            } else {
                wire.parent_enabled_tools
                    .iter()
                    .filter(|t| !matches!(t.as_str(), "subagent" | "list_agents"))
                    .cloned()
                    .collect()
            }
        }
    };
    // 4. The child system message (the `harness/prompt` function —
    // `has_todo_tool` from the resolved child tool list; `None` → the child
    // gets NO system message at all and NO seq-0 row, and the envelope's
    // `FreshChild` mode is what makes that true: `(Fresh, None)` is the
    // MAIN-prompt arm and a child must never reach it (issue #9)).
    let has_todo_tool = child_tools.iter().any(|t| t == "manage_todo_list");
    let system_prompt =
        build_child_system_message(wire.launch.system_prompt.as_deref(), has_todo_tool);
    // 5. The child `StartEnv` (a FRESH ephemeral session —
    // `mint_session_id` (or the PRE-MINTED id — the in-process
    // `dispatch_native`'s `SubagentCancel` targets it); `enabled_tools`
    // = `Some(child_tools)` VERBATIM — an empty `child_tools` = NO tools,
    // NOT re-expanded; `subagent_enabled: false` — the recursion
    // guard).
    let child_id = pre_minted.unwrap_or_else(mint_session_id);
    // The child's `cwd` (the persister's `ensure_session_row` — the
    // ephemeral `sessions` row; the `StoreFrame` carries no `cwd`).
    state.hooks.register_child_cwd(&child_id, &wire.parent_cwd);
    let env = StartEnv::from_parts(
        child_id.clone(),
        wire.parent_cwd.clone(),
        StartMode::FreshChild,
        None,
        model.clone(),
        parent_env.catalog.clone(),
        thinking.clone(),
        parent_env.trusted,
        Some(child_tools.clone()),
        parent_env.config_dir.clone(),
        false,
        system_prompt,
        // (issue #101) The per-agent ruleset rides the child's start envelope
        // VERBATIM — the child Worker's `core.rs` applies it to the loop's gate
        // (`set_permission`), which is the hop that makes a definition's
        // `permission:` block ENFORCE rather than decorate. `wire.launch.permission`
        // is the definition's declared block, carried through task05a's
        // `LaunchConfig` (`Option<Box<…>>` — `as_deref` on the way out of the box;
        // `clone()` on the `Box` clones the block, which is what we want here).
        wire.launch.permission.as_deref().cloned(),
    );
    // 6. The `subagent-session-started` UI lifecycle event (the
    // current driver's step 9 — the PARENT session's sink: the
    // `model` in the COMPOSED `provider/id` form, the
    // `thinkingLevel` the step-2 RESOLVED value, the
    // `enabledTools` the child's resolved set, and the
    // `nameResolution` the resolver's tri-state (issue #47) — the
    // eight-key payload the subagent directory row and the sheet read.
    if let Some(sink) = state.hooks.sink_for(&parent_session_id) {
        sink.emit(
            "subagent-session-started",
            json!({
                "sessionId": child_id,
                "parentSessionId": parent_session_id,
                // The label, TRIMMED by the time it reaches the wire —
                // `harness::dispatch::dispatch_subagent` trims the `agentName`
                // param BEFORE `resolve_launch`, so this field and
                // `nameResolution` below describe the SAME string. Nothing here
                // re-trims: the emitter normalising as well would be a second
                // name selector for one label.
                "agentName": wire.agent_name,
                // (issue #47) The tri-state, verbatim off the wire — NOT
                // re-derived here (a second `discover_agents` on
                // `wire.parent_cwd` would be a second name selector, and two
                // selectors is how the dispatch and the report drift).
                "nameResolution": wire.name_resolution,
                "task": wire.task,
                "model": ModelKey::from(&model).to_string(),
                "thinkingLevel": thinking.as_deref(),
                "enabledTools": child_tools,
            }),
        );
    }
    // 7. The in-flight dispatch bookkeeping (the `settle` watch —
    // the pump flips it on `agent_settled`; the `cancel` watch —
    // the `SubagentCancel` flow; the `SubagentCapture` — the pump
    // feeds it, the drive task reads the final text on settle; the
    // `liveness` watch — the ADR 0055 signal the pump bumps on every
    // frame it sees for this child).
    let (settle_tx, settle_rx) = watch::channel(false);
    let (cancel_tx, cancel_rx) = watch::channel(false);
    // The PUMP-FIRED exit watch (ADR 0055 review, thread 3): `None` until the
    // pump's own `Exited` arm publishes the code — see `SubagentDrive::exit_tx`.
    let (exit_tx, exit_rx) = watch::channel(None);
    let (liveness_tx, liveness_rx) = watch::channel(SubagentLiveness {
        seq: 0,
        in_flight: Default::default(),
        open_gates: Default::default(),
    });
    let capture = Arc::new(SubagentCapture::new());
    {
        let mut drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
        drives.insert(
            child_id.clone(),
            SubagentDrive {
                parent_session_id: parent_session_id.clone(),
                dispatch_id: wire.id.clone(),
                settle_tx: settle_tx.clone(),
                cancel_tx: cancel_tx.clone(),
                exit_tx,
                liveness_tx,
                capture: capture.clone(),
                handle: None,
                reaping: false,
            },
        );
    }
    // 8. The drive task (the spawn + `wait_ready` + `send_start` +
    // the initial `prompt` (the `task`) + the settle vs
    // `dead_bound` vs the process exit vs the cancel race +
    // the UNCONDITIONAL teardown + the `SubagentResult` delivery).
    tokio::spawn(async move {
        let start = std::time::Instant::now();
        let outcome = match (state.factory).spawn() {
            Ok(handle) => {
                // The `cancel_subagent`'s `send_abort` target (the
                // drive owns the handle for the race; the `cancel`
                // reads it from the `drives` map — set it NOW).
                {
                    let mut drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(d) = drives.get_mut(&child_id) {
                        d.handle = Some(handle.clone());
                    }
                }
                // The subagent's read-loop pump (the `is_subagent`
                // flag threaded — `true`; subscribed BEFORE
                // `send_start` so the `Start` frame — the advertised
                // `enabledTools` — is seen; the `agent_settled`
                // flips the settle watch; the `SubagentCapture` is
                // fed from the `SinkFrame` / `message_update`
                // frames).
                start_pump(
                    state.clone(),
                    dead_bound,
                    child_id.clone(),
                    true,
                    handle.clone(),
                );
                drive_subagent(
                    &state,
                    handle,
                    env,
                    &parent_session_id,
                    &wire,
                    start,
                    settle_rx,
                    cancel_rx,
                    // The pump's exit signal (the crash arm reads THIS, not the
                    // process watch — the salvage must see every frame first).
                    exit_rx,
                    // The ADR 0055 liveness receiver (the drive's future
                    // silence bound reads it — this task only threads it,
                    // the race still runs on `dead_bound`).
                    liveness_rx,
                    &capture,
                    dead_bound,
                    &sink,
                )
                .await
            }
            Err(e) => {
                sink.deliver(
                    &state,
                    &parent_session_id,
                    &wire,
                    preflight_failed(format!("spawn the subagent worker: {e}")),
                );
                None
            }
        };
        // The UNCONDITIONAL teardown (settle / Dead / cancel /
        // crash / spawn failure): the `reaping` flag is set FIRST
        // (the `Exited` is EXPECTED: NO `on_crash`), the `drives`
        // entry is removed (the pump's `Exited` lookup misses — no
        // `on_crash`), the `SubagentResult` is delivered (the
        // `drive_subagent` already sent it on the early-failure
        // paths — this is the settle / Dead / cancel / crash
        // delivery).
        if let Some(outcome) = outcome {
            finish_subagent(&state, &child_id, &parent_session_id, &wire, outcome, &sink).await;
        } else {
            // The early-failure path (the `SubagentResult` was
            // already sent — the teardown is the `drives` removal).
            let mut drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
            drives.remove(&child_id);
        }
    });
}

/// The drive task (the settle vs `dead_bound` vs the process exit
/// vs the cancel race — the `drive_subagent` core): the `wait_ready`,
/// `send_start`, and the initial `prompt` (the `task`) preflight, then
/// the race. Returns the `SubagentOutcome` (`None` on a preflight
/// failure — the `SubagentResult` was already sent).
#[allow(clippy::too_many_arguments)]
async fn drive_subagent(
    state: &ManagerState,
    handle: WorkerHandle,
    env: StartEnv,
    parent_session_id: &str,
    wire: &SubagentDispatchWire,
    start: std::time::Instant,
    mut settle_rx: watch::Receiver<bool>,
    mut cancel_rx: watch::Receiver<bool>,
    // The child's exit AS THE PUMP SAW IT (ADR 0055 review, thread 3) — the
    // drive's half of the `watch` the pump's `Exited` arm publishes. This is the
    // arm that may end the race on a crash, because the pump fires it only AFTER
    // `feed_capture` consumed every frame the child wrote (see
    // `SubagentDrive::exit_tx`).
    mut exit_rx: watch::Receiver<Option<i32>>,
    // The child's liveness signal (ADR 0055) — the drive's half of the
    // `watch` the pump feeds. Every frame bumps its `seq` (activity);
    // `in_flight` holds the `tool_call_id`s announced and not yet closed.
    // This is what the self-rearming silence clock below reads.
    mut liveness_rx: watch::Receiver<SubagentLiveness>,
    capture: &Arc<SubagentCapture>,
    dead_bound: Duration,
    sink: &SubagentOutcomeSink,
) -> Option<SubagentOutcome> {
    // The preflight (a `wait_ready` failure / a `send_start` error
    // means the child is gone before the turn started → `Failed` —
    // the `SubagentResult` is sent here, the teardown is the
    // `drives` removal).
    if handle.wait_ready(Duration::from_secs(5)).await.is_err() || handle.send_start(&env).is_err()
    {
        sink.deliver(
            state,
            parent_session_id,
            wire,
            preflight_failed("the subagent worker failed to start".to_string()),
        );
        return None;
    }
    // The initial `prompt` (the `task` — a `SendError` means the
    // child died before the turn started → `Failed`).
    if handle.send_prompt(&wire.task, &[]).is_err() {
        sink.deliver(
            state,
            parent_session_id,
            wire,
            preflight_failed("send the subagent task".to_string()),
        );
        return None;
    }
    // The race: the child's settle (the pump's `agent_settled` flips
    // it) vs the SILENCE bound (ADR 0055 — NOT a bound on the delegation's
    // duration) vs the process exit (an UNEXPECTED exit — a crash; a
    // settle-then-exit is the teardown below) vs the `SubagentCancel` (a
    // parent turn abort — `cancel_subagent` flips it).
    //
    // The bound is a SELF-REARMING silence clock: `dead_bound` is the longest run
    // of UNEXPLAINED SILENCE THIS DRIVE OBSERVES — the gap between frames that
    // actually reached `liveness_rx`, NOT a measurement of what the child was
    // doing — so it is recomputed on every pass of the loop below — a pass is
    // only ever entered because something CHANGED. There is no ceiling on how
    // long a delegation may run, and while a `tool_call_id` is outstanding the
    // bound does not run at all (the arm is disabled, not merely lengthened).
    // The declared-outside binding is only a scope (the value is meaningless
    // before the loop's first pass, which is exactly where it is computed).
    let mut deadline;
    // The SECOND receiver of the SAME pump-exit `watch` (ADR 0055 review,
    // thread 3), for the safety net's bounded grace in `exited_with_pump_grace`:
    // `select!` builds every arm's future up front, so the exit arm's
    // `&mut exit_rx` borrow is live for the whole race and the grace wait cannot
    // alias it. Both receivers read the SAME latest value (a `watch` is not a
    // queue), so a code the pump published BEFORE the process watch won is still
    // seen immediately by the safety net.
    let mut pumped_exit_rx = exit_rx.clone();
    let race = loop {
        // EVERY pass of this loop was entered because something changed (the
        // first pass, or the `changed()` arm) — so the deadline is recomputed
        // UNCONDITIONALLY here. That IS the activity reset: a frame bumps
        // `seq`, `changed()` fires, we come round, the clock restarts. Do NOT
        // guard this recompute on `in_flight` — a guarded recompute leaves
        // the deadline at its ORIGINAL arm time for an unblocked quiet child,
        // which silently restores the absolute wall-clock bound this change
        // removes.
        deadline = std::time::Instant::now() + dead_bound;
        let in_flight = {
            // The borrow is scoped so the `RefMut` CANNOT be held across the
            // `select!` — held across an await point it makes the future
            // non-`Send` and the spawn fails to compile.
            //
            // `borrow_and_update` and NOT `borrow`: it marks the value seen
            // at borrow time, so a frame that lands BETWEEN this borrow and
            // the `select!` still leaves `changed()` ready (no missed
            // wakeup). A plain `borrow()` would leave it unseen, making
            // `changed()` return immediately on every pass — a hot spin that
            // starves the other arms.
            liveness_rx
                .borrow_and_update()
                .in_flight
                .iter()
                .next()
                .is_some()
        };
        // One fresh `sleep_until` per pass (each pass IS the reset, so this
        // is never skipped — a child streaming at ~30 fps re-arms ~30
        // times/sec, which is negligible next to the frame it just processed).
        let mut sleep = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline,
        )));
        // The `select!` arms. The exit arms resolve to OWNED `Option<i32>`s (the
        // safety net's grace await lives in `exited_with_pump_grace`, NOT in an
        // arm body): `wait_for` yields a `watch::Ref` (a `std::sync::RwLockReadGuard`
        // — NOT `Send`, and borrowing the receiver), and an `await` inside an arm
        // body would hold the `select!` result tuple — every arm's value, guards
        // included — across it, making the spawned future non-`Send` and aliasing
        // the receiver borrow. A free future keeps the race loop in its original
        // `break Race::…` shape.
        tokio::select! {
            _ = settle_rx.wait_for(|v| *v) => break Race::Settled,
            // THE EXIT ARM (ADR 0055 review, thread 3): the PUMP's signal, NOT the
            // process watch. The pump publishes this from its own
            // `WorkerInboundEvent::Exited` arm, and the frame `broadcast` is IN
            // ORDER — so `Exited` is only ever delivered AFTER every
            // `agent_message_chunk` / `message_update` the child managed to write
            // has passed through `feed_capture`. Ending the race here is therefore
            // what makes the salvage below read a COMPLETE capture: the `Failed`
            // outcome is frozen at delivery, so a snapshot taken while the pump is
            // still mid-queue truncates the child's findings permanently and
            // reports 0 tokens for work that really cost money.
            //
            // A `None` means the watch's sender is gone (the drive entry was
            // already removed at teardown) — nothing more can be fed, so the
            // salvage reads exactly what the pump had delivered.
            code = exit_rx.wait_for(|v| v.is_some()) => break Race::Exited(code.ok().and_then(|v| *v)),
            // THE SAFETY NET — see `exited_with_pump_grace`: the process watch may
            // NOT end the drive directly (it fires before the pump is done), but a
            // wedged pump must not hang teardown either, so it gets a bounded
            // grace to publish its own exit first.
            code = exited_with_pump_grace(&handle, &mut pumped_exit_rx) => break Race::Exited(code),
            _ = cancel_rx.changed() => break Race::Cancelled,
            // Activity (ADR 0055): any frame from the child — a streamed
            // token, a tool boundary, a control-plane reply — re-arms the
            // clock instead of letting it expire.
            _ = liveness_rx.changed() => continue,
            // The guard DISABLES the arm entirely while a tool call is
            // outstanding — a child inside a tool, parked on a gate, or
            // awaiting its own nested Subagent is Active and has NO timeout.
            // In that state the bound is not "longer"; it does not run.
            _ = &mut sleep, if !in_flight => break Race::Dead,
        };
    };
    // The outcome (the `SubagentCapture`'s final text — the
    // `SinkFrame` `agent_message_chunk` source; the `usage` from the
    // `message_update` frames; the wall clock).
    let duration_ms = start.elapsed().as_millis() as u64;
    let outcome = match race {
        Race::Settled => {
            let (input_tokens, output_tokens) = capture.usage();
            let output = capture.captured_text();
            let metrics = SubagentMetrics {
                output: output.clone(),
                input_tokens,
                output_tokens,
                // (issue #87, T09 / #107) The child's REAL summed cost, as the
                // capture reports it: `Option<f64>` straight through, with no
                // `unwrap_or(0.0)`. That collapse used to live here and it made
                // a child that never reported a figure indistinguishable from
                // one whose calls reported a genuine zero; the capture already
                // holds the `Option` (`SubagentCapture::cost_usd()` returns it,
                // and its `>= 0.0` filter already preserves a real zero), so the
                // hand-off now carries the distinction instead of erasing it.
                cost: capture.cost_usd(),
                duration_ms,
            };
            // (issue #122) A child that SETTLED with no output at all (nothing
            // streamed, or only the upstream sanitisation placeholder) is NOT a
            // `Completed { output: "" }`: dispatch.rs's `Completed` arm builds
            // `ToolResult { content: [Text { text: output }] }` with no
            // `salvage_text` shield (only the `Failed` arm has one), so an empty
            // `Completed` lands in the parent's transcript as a `tool` row with
            // empty text and puts `"content": ""` on the parent's NEXT request —
            // the wire LiteLLM rewrites into the placeholder. It becomes the
            // honest `Failed`, with the child's metrics preserved on that path
            // and `subagent-closed` still firing `status: "failed"`.
            //
            // EXACT-MATCH ONLY (`content_is_effectively_empty` is empty-ish or
            // the whole placeholder, never a substring): a child that ends a real
            // answer with the banner sentence keeps it — don't-filter-too-much.
            //
            // Pinned end-to-end through this real settle by
            // `manager::tests::a_silent_child_that_settled_is_not_completed_empty`.
            if content_is_effectively_empty(&output) && !capture.saw_tool_activity() {
                // The metrics ride the `Failed` path UNCHANGED (the child's real
                // tokens and money are not forfeited by its empty answer); only
                // the `output` mirror is emptied, so nothing placeholder-shaped
                // survives into the parent.
                let mut metrics = metrics;
                metrics.output = String::new();
                SubagentOutcome::Failed {
                    // `Other`, NOT `Cancelled`: the child was not stopped — it
                    // SETTLED. `Cancelled` is the parent/user-Stop meaning (ADR
                    // 0055) and a `Failed` here must never read as one.
                    reason: SubagentFailureReason::Other,
                    error: "the subagent settled with no output".to_string(),
                    // No partial to salvage: `captured_text()` IS the empty (or
                    // placeholder-only) run this guard rejects, and forwarding
                    // the placeholder into the parent's `output` / `error` fields
                    // would re-leak the very banner this fix silences.
                    output: String::new(),
                    metrics,
                }
            } else {
                SubagentOutcome::Completed { output, metrics }
            }
        }
        Race::Dead => failed_with_salvage(
            SubagentFailureReason::Unresponsive,
            "unresponsive".to_string(),
            capture,
            duration_ms,
        ),
        Race::Cancelled => failed_with_salvage(
            SubagentFailureReason::Cancelled,
            "cancelled".to_string(),
            capture,
            duration_ms,
        ),
        Race::Exited(code) => {
            let code_str = code
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string());
            failed_with_salvage(
                SubagentFailureReason::Crashed,
                format!("subagent worker exited unexpectedly (code {code_str})"),
                capture,
                duration_ms,
            )
        }
    };
    Some(outcome)
}

/// The SAFETY-NET exit future (ADR 0055 review, thread 3): the child's exit code
/// as the PROCESS watch reports it, but only after the pump had its chance.
///
/// `WorkerHandle::exited()` is the read task's own `watch`, published the moment
/// `child.wait()` returns — possibly while the child's last frames are STILL
/// queued behind it in the frame `broadcast`. Ending the drive there is exactly
/// the truncation this fix removes, so the process watch may NOT decide the
/// outcome directly: when it wins, wait `PUMP_EXIT_GRACE` for the pump to
/// publish its own exit (the source that guarantees a fully-fed
/// `SubagentCapture`) and prefer that code.
///
/// It is a grace, not an unbounded await, because a pump that is genuinely
/// wedged — parked inside a router callback — must not hold the child's teardown
/// and the parent's `SubagentResult` open forever; past the grace the salvage
/// reads whatever the pump had delivered. Lives in a free future rather than in
/// the `select!` arm body because it awaits, and the arm bodies run while the
/// `select!` result tuple (holding the other arms' non-`Send` `watch::Ref`s) is
/// still alive.
async fn exited_with_pump_grace(
    handle: &WorkerHandle,
    pumped_exit_rx: &mut watch::Receiver<Option<i32>>,
) -> Option<i32> {
    let code = handle.exited().await;
    match tokio::time::timeout(PUMP_EXIT_GRACE, pumped_exit_rx.wait_for(|v| v.is_some())).await {
        // The pump caught up inside the grace: take ITS code — the capture is
        // fully fed at this point.
        Ok(Ok(pumped)) => *pumped,
        // Silent past the grace (a wedged pump) or the watch is gone: the process
        // watch's code is all there is, and the capture is whatever the pump fed.
        Ok(Err(_)) | Err(_) => code,
    }
}

/// A **Dead Subagent**'s outcome (ADR 0055): `failed` in STATUS, but the
/// SALVAGE rides with it — whatever the `SubagentCapture` had accumulated when
/// the child stopped making progress, plus its REAL usage and wall clock.
///
/// This is the fix for the discarding bug: the drive holds its own `Arc` clone
/// of the capture, so the text and the tokens are readable on every crash /
/// reap / cancel path even though the pump has already dropped the `drives`
/// entry. A dead child is NEVER reported as `Completed` — it never decided it
/// was finished — but its 29 minutes of findings must not be thrown away.
fn failed_with_salvage(
    reason: SubagentFailureReason,
    error: String,
    capture: &Arc<SubagentCapture>,
    duration_ms: u64,
) -> SubagentOutcome {
    let (input_tokens, output_tokens) = capture.usage();
    let output = capture.captured_text();
    SubagentOutcome::Failed {
        reason,
        error,
        output: output.clone(),
        metrics: SubagentMetrics {
            output,
            input_tokens,
            output_tokens,
            // (issue #87, T09 / #107) The REAL cost, `Option` straight through
            // — a child that died after four priced calls spent real money, and
            // the salvage exists precisely so its work is not reported as free.
            // No `unwrap_or(0.0)` here either: a child that died BEFORE any call
            // reported a figure reports `None` ("no figure"), not a zero.
            cost: capture.cost_usd(),
            duration_ms,
        },
    }
}

/// A PREFLIGHT failure (ADR 0055): the child never started, so the empty
/// `output` and the default `metrics` are DELIBERATE — there is nothing to
/// salvage and nothing was ever spent. `reason: Other` (no **Dead bound**, no
/// crash, no cancel — the dispatch simply never got off the ground).
fn preflight_failed(error: String) -> SubagentOutcome {
    SubagentOutcome::Failed {
        reason: SubagentFailureReason::Other,
        error,
        output: String::new(),
        metrics: SubagentMetrics::default(),
    }
}

/// The UNCONDITIONAL teardown (settle / Dead / cancel / crash): the
/// `subagent-closed` UI lifecycle event (EVERY exit path — the
/// `subagent.rs:721/743` payload shapes: `status` / `reason` / `error` /
/// `metrics`), the `reaping` flag (the `Exited` is EXPECTED: NO
/// `on_crash`), the graceful `close` + 2 s grace + `kill`, the
/// `drives` entry removal, and the `SubagentResult` delivery (the
/// REQUESTING (main-session) Worker).
pub(crate) async fn finish_subagent(
    state: &ManagerState,
    child_id: &str,
    parent_session_id: &str,
    wire: &SubagentDispatchWire,
    outcome: SubagentOutcome,
    sink: &SubagentOutcomeSink,
) {
    // The child's own UNANSWERED gates (ADR 0055): drained BEFORE the
    // `subagent-closed` emission, from the `drives` entry — which is still
    // valid here, because the removal below happens AFTER the emissions. A gate
    // the user ANSWERED is already gone (`WorkerManager::note_gate_answered`
    // cleared it at the relay), so only parked gates are closed — and a gate the
    // user never answered is closed HERE even though its tool call came to a
    // close (a cancelled gate fabricates one).
    //
    // Read here and NOT snapshotted in `drive_subagent`: a gate can open
    // between that snapshot and this teardown, and this is the path that must
    // be exact.
    let open_gates: Vec<String> = {
        let drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
        match drives.get(child_id) {
            Some(drive) => {
                let mut ids: Vec<String> = drive
                    .liveness_tx
                    .borrow()
                    .open_gates
                    .iter()
                    .cloned()
                    .collect();
                // Sorted for a deterministic emission order (a `HashSet`'s
                // iteration order is unstable, and the emission order is
                // observable on the wire).
                ids.sort();
                ids
            }
            // The drive is already gone (an early-failure teardown) → nothing
            // was ever recorded.
            None => Vec::new(),
        }
    };
    // The guards are DROPPED before the emissions (a sink emission must never
    // run under the `drives` lock, and the borrow above is the only one).
    if let Some(sink) = state.hooks.sink_for(parent_session_id) {
        for request_id in open_gates {
            // ADR 0055: keyed to the CHILD id — the Client stored the gate under
            // `payload.sessionId` (the child) in App.tsx's `interactive-request`
            // listener, so a close keyed to the parent is a no-op on the child's
            // slice and the card would live forever.
            sink.emit(
                "interactive-request-close",
                json!({ "sessionId": child_id, "requestId": request_id }),
            );
        }
    }
    // The `subagent-closed` UI lifecycle event (the PARENT session's
    // sink — the `subagent.rs:721/743` payload shapes).
    if let Some(sink) = state.hooks.sink_for(parent_session_id) {
        match &outcome {
            SubagentOutcome::Completed { metrics, .. } => {
                sink.emit(
                    "subagent-closed",
                    json!({
                        "sessionId": child_id,
                        "status": "completed",
                        "metrics": metrics_json(metrics),
                    }),
                );
            }
            SubagentOutcome::Failed {
                reason,
                error,
                metrics,
                ..
            } => {
                // The REAL metrics (the outcome's — the salvage): a dead
                // child's usage reaches the Client through THIS payload and
                // nothing else, so reporting `default()` here made 29 minutes
                // of real work look free. `reason` is the machine-readable
                // why (ADR 0055) — the card's short line reads it instead of
                // string-matching `error`. The `output` deliberately does NOT
                // ride: the child's transcript is already persisted, and a
                // 29-minute blob does not belong in a lifecycle event.
                sink.emit(
                    "subagent-closed",
                    json!({
                        "sessionId": child_id,
                        "status": "failed",
                        "reason": reason,
                        "error": error,
                        "metrics": metrics_json(metrics),
                    }),
                );
            }
        }
    }
    // (issue #87, T09) THE SUBAGENT ROLL-UP: the child's summed cost, emitted
    // as ONE `cost_update` DELTA keyed to the PARENT.
    //
    // Why it must be emitted HERE rather than by the parent's loop: the parent
    // loop structurally cannot know a child's cost — `dispatch.rs` discards the
    // metrics (`let _ = (reason, metrics);`) and the parent's only view of a
    // child is the `ToolResult` text. So a parent-owned total could never
    // include children; a store-side sum can, provided the figure reaches the
    // store at all. This is that route.
    //
    // Why there is no double count: the child's OWN frames are keyed to the
    // CHILD id (they feed the child's own card), and this one is keyed to the
    // PARENT — different store keys, and different `sessions` rows.
    //
    // It goes through `hooks.on_event` as a `SinkFrame` — i.e. through
    // `SessionManager::route_event` — and NOT straight to the parent's Tauri
    // sink. That is the issue #105 fix: persistence happens ONLY inside
    // `route_event`'s `apply_interactive_event` call, so a frame emitted
    // directly via `hooks.sink_for` reached the UI and never the DB, which made
    // the parent's STORED total disagree with the figure displayed under its
    // own title. `route_event` persists it and then re-emits the SAME payload
    // verbatim (the `Value` is moved, not re-serialised), so the frontend
    // contract is byte-identical to the old direct emission. `is_subagent:
    // false` is correct: this frame is the PARENT's money, and the router's
    // gate keys on that flag.
    //
    // The gate is the `Option` itself (the `emit_cost_update` mirror — the same
    // rule the main path uses): `None` → NO frame, `Some(0.0)` → frame. It is
    // still a FRAME-EXISTENCE gate and never a value filter: a child that
    // reported nothing contributes nothing, and a child whose calls reported a
    // REAL zero is a figure and its frame lands, keyed to the PARENT. The old
    // `> 0.0` test could not express that (it ate the real zero, which is the
    // filter `emit.rs`'s doc names as the bug), and the `f64` hand-off left it
    // no `Option` to match on. There is no `!= 0` filter either — the roll-up's
    // only writer is the capture's `filter(c.is_finite() && *c >= 0.0)`, so a
    // non-negative figure is all that can ever arrive, and a real zero reaches
    // the wheel.
    let rollup_cost = match &outcome {
        SubagentOutcome::Completed { metrics, .. } => metrics.cost,
        SubagentOutcome::Failed { metrics, .. } => metrics.cost,
    };
    // The gate is an `if let` and NOT a `let … else { return }`: `finish_subagent`
    // CONTINUES after this block (the reap, the `drives` removal, the
    // `SubagentResult` delivery), so an early `return` would silently skip the
    // child's teardown and never answer the parent's wait. The `if let` scopes
    // the gate to the ONE frame, which is all a frame-existence gate may do.
    if let Some(rollup_cost) = rollup_cost {
        state.hooks.on_event(
            parent_session_id,
            false,
            WorkerInboundEvent::SinkFrame {
                event: "interactive-event".to_string(),
                payload: json!({
                    "sessionId": parent_session_id,
                    "seq": 0,
                    "event": "cost_update",
                    // `+ 0.0` is defence in depth, not the load-bearing guard:
                    // the capture rejects negatives, so no `-0.0` can reach
                    // here. It keeps the wire's "never a signed zero" invariant
                    // true at every writer of `costUsd`.
                    "payload": { "costUsd": rollup_cost + 0.0, "source": "subagent" },
                }),
            },
        );
    }
    // The `reaping` flag is set FIRST (the `Exited` is EXPECTED: NO
    // `on_crash`).
    {
        let mut drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(d) = drives.get_mut(child_id) {
            d.reaping = true;
        }
    }
    // The graceful `close` + 2 s grace + `kill` (the `drives` entry's
    // handle — the drive task's handle clone is the same process).
    {
        let handle = state
            .drives
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(child_id)
            .and_then(|d| d.handle.clone());
        if let Some(handle) = handle {
            reap_worker(handle).await;
        }
    }
    // The `drives` entry removal (the pump's `Exited` lookup misses —
    // no `on_crash`).
    {
        let mut drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
        drives.remove(child_id);
    }
    // The `SubagentResult` delivery (the REQUESTING (main-session)
    // Worker — or the in-process `dispatch_native`'s oneshot, the
    // `sink` target).
    sink.deliver(state, parent_session_id, wire, outcome);
}

/// Send `Inbound::SubagentResult { id, outcome }` to the REQUESTING
/// (main-session) Worker (the `IpcDispatcher` waiter resolves — the
/// loop's `SubagentWait` select sees it). The parent is gone → the
/// delivery is a no-op (the waiter is dropped — `Cancelled`).
fn send_subagent_result(
    state: &ManagerState,
    parent_session_id: &str,
    wire: &SubagentDispatchWire,
    outcome: SubagentOutcome,
) -> bool {
    let handle = state
        .workers
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(parent_session_id)
        .map(|e| e.handle.clone());
    let Some(handle) = handle else {
        return false;
    };
    handle
        .send_inbound_frame(Inbound::SubagentResult {
            id: wire.id.clone(),
            outcome,
        })
        .is_ok()
}

/// The `metrics` object shape (the wire contract: `output` /
/// `inputTokens` / `outputTokens` / `cost` / `durationMs` — the
/// `subagent.rs` `metrics_json` moved verbatim).
fn metrics_json(m: &SubagentMetrics) -> Value {
    json!({
        "output": m.output,
        "inputTokens": m.input_tokens,
        "outputTokens": m.output_tokens,
        "cost": m.cost,
        "durationMs": m.duration_ms,
    })
}

/// The subagent drive's outcome (the settle vs `dead_bound` vs the
/// process exit vs the `SubagentCancel`).
enum Race {
    /// The child settled (the pump's `agent_settled` flipped the
    /// settle watch).
    Settled,
    /// The process exited (an UNEXPECTED exit — a crash; a
    /// settle-then-exit is the teardown below).
    Exited(Option<i32>),
    /// The **Dead bound** won — the child was silent with nothing in flight
    /// for the whole bound, so it is **Dead** and torn down unconditionally.
    Dead,
    /// The `SubagentCancel` handle fired (a user / caller cancel —
    /// `cancel_subagent` flipped it).
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One child-side `cost_update` frame, as the child's own loop emits it
    /// (`emit_cost_update`) and as the pump hands it to `observe_sink_frame`
    /// after `IpcEventSink`'s catch-all wrapped it in a `SinkFrame`.
    fn child_cost_frame(session: &str, cost: Value) -> Value {
        json!({
            "sessionId": session,
            "seq": 0,
            "event": "cost_update",
            "payload": { "costUsd": cost, "source": "main" },
        })
    }

    /// (issue #122) The CAPTURE half of the settle guard, pinned at the input:
    /// a child whose capture holds ONLY the upstream sanitisation placeholder is
    /// what `captured_text()` hands the `Race::Settled` guard, byte-for-byte.
    /// The capture applies no value filter of its own (by design — it records
    /// what the child said), which is exactly why the guard has to live at the
    /// settle, and why it can be an EXACT match: the string it judges is this
    /// one. GREEN before the guard existed (it pins the guard's INPUT, not its
    /// decision; the decision is pinned end-to-end by
    /// `manager::tests::a_silent_child_that_settled_is_not_completed_empty`).
    #[test]
    fn a_settled_placeholder_only_capture_is_captured_text_placeholder() {
        let capture = SubagentCapture::new();
        capture.observe_sink_frame(
            "session-update",
            &json!({
                "sessionId": "child-1",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "m1",
                    "content": { "type": "text",
                        "text": "[System: Empty message content sanitised to satisfy protocol]" }
                }
            }),
        );
        assert_eq!(
            capture.captured_text(),
            "[System: Empty message content sanitised to satisfy protocol]",
            "the capture records the placeholder byte-for-byte, so the settle \
             guard sees exactly what it filters"
        );
        // And the guard's predicate agrees it is "no output" — the whole-output
        // check, never a substring match.
        assert!(content_is_effectively_empty(&capture.captured_text()));
        // The don't-filter-too-much rail: a child that ENDS its real answer with
        // the banner sentence keeps it (the whole output is not the placeholder).
        let real = SubagentCapture::new();
        real.observe_sink_frame(
            "session-update",
            &json!({
                "sessionId": "child-1",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "m1",
                    "content": { "type": "text",
                        "text": "the file is empty [System: Empty message content sanitised to satisfy protocol]" }
                }
            }),
        );
        assert!(
            !content_is_effectively_empty(&real.captured_text()),
            "a real answer that merely mentions the banner is never reclassified"
        );
    }

    /// A child's money accumulates ACROSS calls — a SUM, not the `max` the
    /// neighbouring token fields use. A turn's token usage is cumulative per
    /// message (so the largest frame wins), but money is spent per call, and
    /// copying the `max` rule here would report one call's cost for a
    /// ten-call child. This pin is what discriminates the two rules.
    #[test]
    fn a_child_cost_frame_accumulates_across_calls() {
        let capture = SubagentCapture::new();
        capture.observe_sink_frame(
            "interactive-event",
            &child_cost_frame("child-1", json!(0.001)),
        );
        capture.observe_sink_frame(
            "interactive-event",
            &child_cost_frame("child-1", json!(0.002)),
        );
        let cost = capture.cost_usd().expect("two priced frames were observed");
        assert!(
            (cost - 0.003).abs() < f64::EPSILON,
            "the child's cost is the SUM of its frames (0.001 + 0.002), got {cost} — \
             a `max` copy of the token rule would report 0.002"
        );
    }

    /// No frames means NO FIGURE (`None`), not `Some(0.0)` — and a malformed
    /// frame cannot invent money. If a bad wire value collapsed that to a
    /// number, a free child would arrive with a figure attached; the hand-off
    /// (`SubagentMetrics.cost`) now carries this `Option` through untouched
    /// (issue #107), so the capture is no longer the only place that can tell
    /// "nothing was reported" apart from "a call cost zero".
    #[test]
    fn a_child_that_reported_no_cost_reports_no_cost() {
        // Nothing observed at all.
        assert_eq!(SubagentCapture::new().cost_usd(), None);

        // A frame whose `costUsd` is a string / null / an object / absent.
        for bad in [
            json!("0.5"),
            json!(null),
            json!({ "amount": 0.5 }),
            Value::Null,
        ] {
            let capture = SubagentCapture::new();
            capture.observe_sink_frame("interactive-event", &child_cost_frame("c", bad.clone()));
            assert_eq!(
                capture.cost_usd(),
                None,
                "a non-number `costUsd` ({bad}) must not become money"
            );
        }

        // A NEGATIVE figure is rejected (the same sanitising rule the provider
        // readers use). Deliberately NOT `-0.0`: `-0.0 >= 0.0` is true, so a
        // negative zero is ACCEPTED and normalised to `+0.0` — writing it here
        // as a "negative" case would push the guard toward a `!= 0.0` filter
        // that eats a legitimate real zero.
        let capture = SubagentCapture::new();
        capture.observe_sink_frame("interactive-event", &child_cost_frame("c", json!(-0.5)));
        assert_eq!(capture.cost_usd(), None, "a negative cost is not money");

        // A REAL zero IS a figure and is kept (the filter this pin forbids).
        let capture = SubagentCapture::new();
        capture.observe_sink_frame("interactive-event", &child_cost_frame("c", json!(0)));
        assert_eq!(
            capture.cost_usd(),
            Some(0.0),
            "`Some(0.0)` is a reported figure, not an absence"
        );
    }

    /// A `-0.0` the child reports is normalised to `+0.0` at the capture's parse
    /// seam, so the roll-up can never carry a signed zero onto the wire (the
    /// emitter-side normalisation covers the loop's own frames; this covers the
    /// sum). A real zero stays a real figure.
    #[test]
    fn a_negative_zero_child_cost_is_normalised_to_positive_zero() {
        let capture = SubagentCapture::new();
        capture.observe_sink_frame("interactive-event", &child_cost_frame("c", json!(-0.0)));
        let cost = capture
            .cost_usd()
            .expect("a negative zero is still a reported figure");
        let wire = serde_json::to_string(&cost).expect("serialisable");
        assert!(
            !wire.contains('-'),
            "the captured cost carries no sign, got {wire}"
        );
    }

    /// The new branch is scoped to the ONE event name: an unrelated
    /// interactive-event frame (the `todos_update` push rides the same channel)
    /// must not move the child's cost.
    #[test]
    fn observe_sink_frame_still_ignores_unrelated_interactive_frames() {
        let capture = SubagentCapture::new();
        capture.observe_sink_frame(
            "interactive-event",
            &json!({
                "sessionId": "child-1",
                "seq": 0,
                "event": "todos_update",
                "payload": { "source": "main", "todos": [] },
            }),
        );
        // A frame that merely LOOKS like ours but names a different event.
        capture.observe_sink_frame(
            "interactive-event",
            &json!({
                "sessionId": "child-1",
                "seq": 0,
                "event": "cost_estimate",
                "payload": { "costUsd": 5.0, "source": "main" },
            }),
        );
        assert_eq!(
            capture.cost_usd(),
            None,
            "only `event == \"cost_update\"` moves the money"
        );
        // And the text capture still ignores interactive frames entirely.
        assert_eq!(capture.captured_text(), "");
    }

    /// The `metrics` wire shape is pinned here: the key set is exactly
    /// `{output, inputTokens, outputTokens, cost, durationMs}`.
    /// REGRESSION GUARD: the shape is already correct, so this passes on write;
    /// it exists to catch a later "just add a `costUsd` too" edit.
    ///
    /// EDITED by issue #107, with the reason: the sibling test
    /// `metrics_json_carries_the_childs_cost` used to pin the collapse
    /// (`"a number, never null"`), and this key-set pin sat next to it. That
    /// contract is REPLACED — `null` is now legal, and is the ONLY legal
    /// spelling of ABSENT; `0.0` is a legal FIGURE. What stays pinned is that
    /// the `cost` KEY is always present (absence is spelled by the value
    /// `null`, never by dropping the key), and that a `Some` figure is not
    /// emitted as `null`.
    #[test]
    fn metrics_json_keeps_the_wire_shape_exact() {
        let m = SubagentMetrics {
            output: "text".to_string(),
            input_tokens: 10,
            output_tokens: 3,
            cost: Some(0.0000341),
            duration_ms: 900,
        };
        let v = metrics_json(&m);
        let object = v.as_object().expect("an object");
        let mut keys: Vec<&String> = object.keys().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "cost",
                "durationMs",
                "inputTokens",
                "output",
                "outputTokens"
            ],
            "the key set is byte-identical to the pinned contract"
        );
        assert!(
            !v["cost"].is_null(),
            "a `Some` figure is a number on the wire, never `null` (`null` is \
             the spelling of ABSENT — see the sibling pins)"
        );
    }

    /// The child's real cost rides `metrics.cost` (the sole child→parent money
    /// hand-off — `dispatch.rs` discards the metrics, so the parent loop can
    /// never total a child itself).
    ///
    /// EDITED by issue #107, with the reason: its old assertion
    /// (`is_number(), "a number, never null"`) PINNED THE COLLAPSE — it forbade
    /// the one spelling of "no figure". The contract is REPLACED: `null` is now
    /// legal and is the ONLY legal spelling of ABSENT, and `0.0` is a legal
    /// FIGURE (see `distinguishes_a_real_zero_child_from_a_never_priced_child`,
    /// which pins both ends). What stays pinned here is that a `Some` figure is
    /// never emitted as `null`.
    #[test]
    fn metrics_json_carries_the_childs_cost() {
        let m = SubagentMetrics {
            output: String::new(),
            input_tokens: 0,
            output_tokens: 0,
            cost: Some(0.0000341),
            duration_ms: 0,
        };
        let v = metrics_json(&m);
        assert_eq!(
            v["cost"].as_f64(),
            Some(0.0000341),
            "the sub-cliff figure survives the metrics object intact — the \
             formatter is the frontend's problem, not the wire's"
        );
        assert!(
            !v["cost"].is_null(),
            "a REPORTED figure is never spelled `null` (a `null` means no \
             figure arrived — the other end of the #107 rule)"
        );
    }

    /// A real-zero child and a never-priced child are **NOT interchangeable**
    /// on the wire (issue #107): a child whose calls reported a genuine `$0.00`
    /// hand-offs `"cost": 0.0` (a FIGURE), and a child whose calls reported
    /// nothing hand-offs `"cost": null` (the wire spelling of "no figure").
    /// The `SubagentCapture` already tells the two apart; the hand-off must not
    /// collapse them again, because a renderer that cannot tell them apart
    /// prints `$0.00` for a child that never cost anything (ADR 0082 / D5).
    #[test]
    fn distinguishes_a_real_zero_child_from_a_never_priced_child() {
        // A child whose ONE call reported a real zero.
        let priced = SubagentCapture::new();
        priced.observe_sink_frame("interactive-event", &child_cost_frame("child-1", json!(0)));
        let real_zero = metrics_json(&SubagentMetrics {
            output: String::new(),
            input_tokens: 0,
            output_tokens: 0,
            cost: priced.cost_usd(),
            duration_ms: 0,
        });

        // A child that never had a priced frame at all.
        let silent = SubagentCapture::new();
        let never_priced = metrics_json(&SubagentMetrics {
            output: String::new(),
            input_tokens: 0,
            output_tokens: 0,
            cost: silent.cost_usd(),
            duration_ms: 0,
        });

        assert_eq!(
            real_zero["cost"].as_f64(),
            Some(0.0),
            "a call that reported a real zero is a FIGURE and rides the wire \
             as the number 0.0 — never as `null`"
        );
        assert!(
            never_priced["cost"].is_null(),
            "a child that never reported a cost is `\"cost\": null` (PRESENT \
             AND NULL — the `SessionInfo.costUsd` spelling), got {:?}",
            never_priced["cost"]
        );
        assert_ne!(
            real_zero["cost"], never_priced["cost"],
            "the two states are NOT interchangeable: collapsing them is the \
             defect #107 exists to end"
        );
    }
}

/// The roll-up half of issue #87 driven end to end through `finish_subagent`
/// (the capture pins above cannot see the emission, and the emission is the
/// other half of the roll-up).
#[cfg(test)]
mod rollup_tests {
    use super::*;
    use crate::agent::events::EventSink;
    use crate::agent::worker::client::{WorkerError, WorkerErrorKind};
    use crate::agent::worker::manager::registry::ManagerState;
    use crate::agent::worker::manager::{ClosureHooks, WorkerFactory};

    /// A sink that records every `(name, payload)` it is given.
    struct RecordingSink {
        events: Arc<StdMutex<Vec<(String, Value)>>>,
    }

    impl EventSink for RecordingSink {
        fn emit(&self, event: &str, payload: Value) {
            self.events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((event.to_string(), payload));
        }
    }

    /// A `WorkerFactory` that refuses to spawn — this test never starts a
    /// process (`drives` has no entry, so `finish_subagent` skips the reap).
    struct NeverSpawns;

    impl WorkerFactory for NeverSpawns {
        fn spawn(&self) -> Result<crate::agent::worker::client::WorkerHandle, WorkerError> {
            Err(WorkerError {
                kind: WorkerErrorKind::SpawnFailed,
            })
        }
    }

    /// The harness's `WorkerHooks`.
    ///
    /// `on_event` FORWARDS a `SinkFrame` into the same `RecordingSink` the
    /// `sink_for` closure hands out. Production's `on_event` is
    /// `SessionWorkerHooks::on_event` → `SessionManager::route_event`, whose
    /// `SinkFrame` arm re-emits the payload verbatim to `self.sink` — so a
    /// forwarding sink here is the faithful stand-in for that path (and
    /// `sink_for` stays wired to the same collector, because the other
    /// emissions in `finish_subagent` — `interactive-request-close` and
    /// `subagent-closed` — still go straight to the sink).
    ///
    /// It is deliberately NOT the real router: `ManagerState` has no `db`
    /// field (`registry.rs`), so nothing in this module can assert a write.
    /// That half lives in `router.rs`'s
    /// `a_parent_keyed_cost_frame_from_the_router_writes_the_parents_row`.
    fn state_with(collector: Arc<RecordingSink>) -> ManagerState {
        let forwarded = collector.clone();
        ManagerState::new(
            Arc::new(NeverSpawns),
            Arc::new(ClosureHooks {
                on_crash: Box::new(|_, _, _| {}),
                on_event: Box::new(move |_, _, evt| {
                    // The roll-up now reaches the UI through the ROUTER (the
                    // `SinkFrame` re-emission), not through `sink_for`. A
                    // no-op `on_event` here would silently swallow it and make
                    // the `frames.len() == 1` assertions vacuous.
                    if let WorkerInboundEvent::SinkFrame { event, payload } = evt {
                        forwarded.emit(&event, payload);
                    }
                }),
                sink_for: Box::new(move |_| Some(collector.clone() as Arc<dyn EventSink>)),
                cwd_registrar: std::sync::OnceLock::new(),
            }),
        )
    }

    fn wire() -> crate::agent::worker::protocol::SubagentDispatchWire {
        crate::agent::worker::protocol::SubagentDispatchWire {
            id: "d1".to_string(),
            parent_session_id: "parent-1".to_string(),
            parent_cwd: "/tmp".to_string(),
            parent_enabled_tools: Vec::new(),
            agent_name: "tester".to_string(),
            name_resolution: crate::agent::subagent::AgentNameResolution::Matched,
            launch: crate::agent::subagent::LaunchConfig::default(),
            task: "t".to_string(),
            model_key: "fake/m1".to_string(),
        }
    }

    fn rollups(events: &StdMutex<Vec<(String, Value)>>) -> Vec<Value> {
        events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|(name, frame)| name == "interactive-event" && frame["event"] == "cost_update")
            .map(|(_, frame)| frame.clone())
            .collect()
    }

    /// The roll-up frame is keyed to the **PARENT**, never to the child — which
    /// is what makes it additive rather than a double count. The child's OWN
    /// frames carry the CHILD id (pinned loop-side by
    /// `a_priced_call_emits_one_cost_update_frame_carrying_that_calls_cost`,
    /// where the envelope's `sessionId` is the emitting loop's own id), so the
    /// two land in DIFFERENT store keys and the parent's sum gains the child's
    /// money exactly once. Also pins the envelope, the `source`, and that the
    /// `subagent-closed` hand-off still carries the same figure.
    #[tokio::test]
    async fn the_rollup_frame_is_keyed_to_the_parent_not_the_child() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let collector = Arc::new(RecordingSink {
            events: events.clone(),
        });
        let state = state_with(collector);
        finish_subagent(
            &state,
            "child-1",
            "parent-1",
            &wire(),
            SubagentOutcome::Completed {
                output: "done".to_string(),
                metrics: SubagentMetrics {
                    output: "done".to_string(),
                    input_tokens: 10,
                    output_tokens: 2,
                    // T08's measured band: four priced calls summed.
                    cost: Some(0.0000341),
                    duration_ms: 5,
                },
            },
            &SubagentOutcomeSink::Oneshot(Arc::new(StdMutex::new(None))),
        )
        .await;

        let frames = rollups(&events);
        assert_eq!(frames.len(), 1, "exactly ONE roll-up frame per child");
        assert_eq!(
            frames[0]["sessionId"], "parent-1",
            "the roll-up is keyed to the PARENT — the child's own frames carry \
             the CHILD id, so the two never share a store key (no double count)"
        );
        assert_ne!(
            frames[0]["sessionId"], "child-1",
            "the roll-up must NOT re-key the child's money to the child"
        );
        assert_eq!(frames[0]["seq"], 0);
        assert_eq!(frames[0]["event"], "cost_update");
        assert_eq!(frames[0]["payload"]["source"], "subagent");
        assert_eq!(
            frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
            0.0000341,
            "the child's summed cost, whole (the sub-cliff figure survives the wire)"
        );

        // The `subagent-closed` hand-off still fires, still CHILD-keyed, with
        // the SAME figure — the roll-up is an addition, not a relocation.
        let closed = {
            let held = events.lock().unwrap_or_else(|p| p.into_inner());
            held.iter()
                .find(|(name, _)| name == "subagent-closed")
                .map(|(_, p)| p.clone())
        }
        .expect("the `subagent-closed` still fires");
        assert_eq!(closed["sessionId"], "child-1", "closed stays CHILD-keyed");
        assert_eq!(closed["metrics"]["cost"].as_f64(), Some(0.0000341));
    }

    /// A child that reported NO figure contributes NO frame — the gate is the
    /// `Option` itself, and the wire spelling of "no figure" on the hand-off is
    /// `"cost": null` (present-and-null), never a `costUsd: 0` that the summing
    /// store would turn into a phantom `$0.00`.
    #[tokio::test]
    async fn a_child_that_spent_nothing_emits_no_rollup_frame() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let collector = Arc::new(RecordingSink {
            events: events.clone(),
        });
        let state = state_with(collector);
        finish_subagent(
            &state,
            "child-2",
            "parent-1",
            &wire(),
            // `preflight_failed` — the child never started, nothing was spent.
            preflight_failed("never started".to_string()),
            &SubagentOutcomeSink::Oneshot(Arc::new(StdMutex::new(None))),
        )
        .await;

        assert!(
            rollups(&events).is_empty(),
            "a child that spent nothing contributes NO frame at all"
        );
    }

    /// A FAILED child's money still rolls up: the salvage exists so a child's
    /// real work is not reported as free, and the same applies to its cost.
    #[tokio::test]
    async fn a_failed_childs_cost_still_rolls_up() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let collector = Arc::new(RecordingSink {
            events: events.clone(),
        });
        let state = state_with(collector);
        let capture = Arc::new(SubagentCapture::new());
        capture.observe_sink_frame(
            "interactive-event",
            &json!({
                "sessionId": "child-3",
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": 0.00001243, "source": "main" },
            }),
        );
        let outcome = failed_with_salvage(
            crate::agent::subagent::SubagentFailureReason::Unresponsive,
            "unresponsive".to_string(),
            &capture,
            100,
        );
        finish_subagent(
            &state,
            "child-3",
            "parent-1",
            &wire(),
            outcome,
            &SubagentOutcomeSink::Oneshot(Arc::new(StdMutex::new(None))),
        )
        .await;

        let frames = rollups(&events);
        assert_eq!(frames.len(), 1, "a dead child's money is still money spent");
        assert_eq!(frames[0]["sessionId"], "parent-1");
        assert_eq!(
            frames[0]["payload"]["costUsd"].as_f64().expect("a number"),
            0.00001243
        );
    }

    /// A child that NEVER reported a figure contributes NO roll-up frame, and
    /// its `subagent-closed` hand-off says `"cost": null` — the wire spelling
    /// of "no figure" (issue #107). Under the old `f64` hand-off this same
    /// child arrived with `"cost": 0.0`, which is a FIGURE: the card could not
    /// tell a child that never cost anything from one whose calls reported a
    /// genuine `$0.00`, and the summing store would have turned a phantom zero
    /// into a phantom `$0.00`. The frame-EXISTENCE gate is now the `Option`
    /// itself: `None` → no frame, `Some(0.0)` → frame.
    #[tokio::test]
    async fn a_never_priced_child_emits_no_rollup_frame() {
        let events = Arc::new(StdMutex::new(Vec::new()));
        let collector = Arc::new(RecordingSink {
            events: events.clone(),
        });
        let state = state_with(collector);
        finish_subagent(
            &state,
            "child-4",
            "parent-1",
            &wire(),
            // `preflight_failed` — the child never started, so NO frame of the
            // capture's was ever observed: `cost_usd()` is `None`.
            preflight_failed("never started".to_string()),
            &SubagentOutcomeSink::Oneshot(Arc::new(StdMutex::new(None))),
        )
        .await;

        assert!(
            rollups(&events).is_empty(),
            "a child that reported NO figure contributes NO `cost_update` \
             frame to the parent — absence is spelled by NOT emitting"
        );
        let closed = {
            let held = events.lock().unwrap_or_else(|p| p.into_inner());
            held.iter()
                .find(|(name, _)| name == "subagent-closed")
                .map(|(_, p)| p.clone())
        }
        .expect("the `subagent-closed` still fires");
        assert!(
            closed["metrics"]["cost"].is_null(),
            "the hand-off says `\"cost\": null` (present-and-null, the \
             `SessionInfo.costUsd` spelling), got {:?}",
            closed["metrics"]["cost"]
        );
    }
}

use std::sync::Arc;
use std::time::Duration;

use crate::agent::debuglog;
use crate::agent::events::RpcEvent;
use crate::agent::worker::client::{WorkerHandle, WorkerInboundEvent};

use super::registry::{unexpected_exit, ManagerState};
use super::subagent_flow::{
    cancel_subagent_by_dispatch_id, dispatch_subagent, feed_capture, note_liveness_event,
    note_liveness_frame, note_liveness_gate, publish_pump_exit, SubagentOutcomeSink,
};

/// The read-loop pump (the `events()` consumer — the `on_event` /
/// `on_crash` routing; the `is_subagent` flag threaded; the
/// `dead_bound` for the `SubagentDispatch` it runs).
pub(crate) fn start_pump(
    state: Arc<ManagerState>,
    dead_bound: Duration,
    session_id: String,
    is_subagent: bool,
    handle: WorkerHandle,
) {
    // The pump owns the sole ACTIVE receiver (a clone — the handle's
    // stored receiver is idle, so every frame routes to the polled
    // clone).
    let rx = handle.events();
    // The DEAD child's pid, read HERE rather than at the fire site below:
    // `Child::id()` answers `None` once the child is reaped — exactly the
    // state a crash leaves — so the only value that can still attribute the
    // crash is the one the handle captured at spawn. Read BEFORE the
    // `spawn`: the pump does not move the `handle` into its future, and this
    // keeps that true (only an `Option<u32>` is captured).
    let pid = handle.pid();
    tokio::spawn(run_pump_loop(
        state,
        dead_bound,
        session_id,
        is_subagent,
        rx,
        pid,
    ));
}

/// The main pump loop consuming events from the worker's broadcast receiver.
pub(crate) async fn run_pump_loop(
    state: Arc<ManagerState>,
    dead_bound: Duration,
    session_id: String,
    is_subagent: bool,
    mut rx: tokio::sync::broadcast::Receiver<WorkerInboundEvent>,
    pid: Option<u32>,
) {
    loop {
        let result = rx.recv().await;
        let Ok(evt) = result else {
            // `Lagged` — the ring rotated past the cursor (a
            // stalled consumer; the frames are lost — the loop
            // continues); `Closed` — the read task is gone (the
            // process is reaped — the loop ends).
            match result {
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    crate::agent::debuglog::log(&format!(
                        "worker pump lagged {n} frames for session {session_id}"
                    ));
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                // The `Ok` variant is excluded by the `let-else`
                // above (unreachable — the pattern is exhaustive
                // for the linter).
                Ok(_) => unreachable!("the `let-else` excluded `Ok`"),
            }
        };
        // (a) The router sees EVERY variant (including `Exited`).
        // A subagent's frames are delivered under the PARENT
        // session's id (the subagent's UI belongs to the parent's
        // scope — the `SubagentDrive` entry holds the parent id
        // until the teardown; a missing entry — the frame is
        // after the teardown — falls back to the child id).
        let delivery_id = if is_subagent {
            state
                .drives
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&session_id)
                .map(|d| d.parent_session_id.clone())
                .unwrap_or_else(|| session_id.clone())
        } else {
            session_id.clone()
        };
        state.hooks.on_event(&delivery_id, is_subagent, evt.clone());
        match evt {
            // The `SubagentCapture` feed (the subagent's
            // `SinkFrame` `session-update` stream — the final-text
            // source; the `message_update` `usage` — the metrics'
            // token fields) + the settle signal (the drive task
            // races it against the `dead_bound` / the process
            // exit / the cancel) + the liveness signal (ADR 0055:
            // EVERY frame — a `SinkFrame` included — bumps the
            // drive's `seq`, so a child that only STREAMS still
            // reads as an Active Subagent).
            WorkerInboundEvent::SinkFrame { event, payload } if is_subagent => {
                feed_capture(&state, &session_id, |c, liveness| {
                    note_liveness_frame(liveness);
                    c.observe_sink_frame(&event, &payload);
                    false
                });
            }
            WorkerInboundEvent::Event(e) if is_subagent => {
                feed_capture(&state, &session_id, |c, liveness| {
                    // The boundary bookkeeping runs BEFORE the capture
                    // feed (ADR 0055): the announced-and-not-yet-closed
                    // `tool_call_id` set is what tells a legitimately
                    // silent **Active Subagent** (inside a tool, parked
                    // on a gate, awaiting a nested delegation) from a
                    // **Dead** one.
                    note_liveness_event(liveness, &e);
                    c.observe_event(&e);
                    matches!(e, RpcEvent::agent_settled)
                });
            }
            // (a2) A child's **gate** (ADR 0055): record the unanswered gate
            // so `finish_subagent` can close the child's own card at
            // teardown. This arm REPLACES nothing — the router already
            // received this exact frame VERBATIM through the
            // `(state.on_event)(delivery_id, is_subagent, evt)` call above
            // (which is where the `permission-request` /
            // `interactive-request` re-emission happens), and it still
            // does: recording runs ALONGSIDE, never instead. These two
            // variants used to fall through to the permissive `_` arm.
            WorkerInboundEvent::PermissionRequest { id, .. }
            | WorkerInboundEvent::InteractiveRequest { id, .. }
                if is_subagent =>
            {
                feed_capture(&state, &session_id, |_c, liveness| {
                    note_liveness_gate(liveness, &id);
                    false
                });
            }
            // (b) The Supervisor-side subagent-dispatch flow (the
            // `on_event` above already delivered the frame to the
            // router).
            WorkerInboundEvent::SubagentDispatch(wire) => {
                dispatch_subagent(
                    state.clone(),
                    dead_bound,
                    session_id.clone(),
                    wire,
                    SubagentOutcomeSink::Requester,
                    None,
                );
            }
            // (b2) The parent's turn abort (the `IpcDispatcher`'s
            // cancel watcher): the frame names the DISPATCH id, so the
            // drive is found by it. This frame used to fall through to
            // the permissive `_` arm and vanish — a Stop then did NOT
            // stop the subagent (it ran its whole turn, mutating files
            // the user had just asked to leave alone) and its Worker
            // process stayed alive until the 30-minute **Dead bound**.
            WorkerInboundEvent::SubagentCancel { id } => {
                cancel_subagent_by_dispatch_id(&state, &id);
            }
            WorkerInboundEvent::FetchRenderRequest { .. } => {
                // Handled by SessionManager router via on_event.
            }
            // (c) Crash detection: an UNEXPECTED `Exited` (no
            // `detach` / `reap_all` / drive-task teardown in
            // flight) fires `on_crash` exactly once (the
            // `Exited` frame is terminal — the pump loop ends);
            // an EXPECTED `Exited` (a `detach` / `reap_all` was in
            // flight) fires NO `on_crash`.
            WorkerInboundEvent::Exited(code) => {
                // The lifecycle transition (the `ARCHIMEDES_DEBUG` log —
                // a CRASH when the `Exited` is unexpected, a clean
                // `exit` otherwise; the code + the session id). Computed
                // BEFORE the drive is woken (below): publishing first would
                // let the drive's teardown remove the `drives` entry in
                // between, and a genuinely crashed child would then read as
                // an EXPECTED exit and lose its `on_crash`.
                let crashed = unexpected_exit(&state, &session_id);
                // The PUMP-FIRED exit signal (ADR 0055 review, thread 3):
                // publish the code through the drive's `exit` watch once
                // every frame the child wrote has ALREADY been fed (the
                // `broadcast` is IN ORDER, so `Exited` is the last thing the
                // pump sees) — the drive reads the salvage the instant this
                // fires. Firing it from the read task's `exited` watch
                // instead is what truncated the capture.
                if is_subagent {
                    publish_pump_exit(&state, &session_id, code);
                }
                debuglog::log(&format!(
                    "worker {}: {session_id} (code {code:?})",
                    if crashed { "crash" } else { "exit" }
                ));
                if crashed {
                    // The pid of the process that just died — the router's
                    // attribution key (the crash log's name carries the
                    // writing pid, so the banner names THIS Worker's log
                    // rather than the newest one in the dir).
                    state.hooks.on_crash(&session_id, code, pid);
                }
                break;
            }
            _ => {}
        }
    }
}

//! The Supervisor-side Worker registry (ADR 0025 Task 3): the
//! `WorkerManager` (the registry of live Workers — session-keyed, the
//! `on_crash` / `on_event` callbacks threaded with the `is_subagent`
//! flag) + the Supervisor-side subagent-dispatch flow (a
//! main-session Worker's `subagent` tool → its `IpcDispatcher` →
//! `Outbound::SubagentDispatch` → here: the model-key resolution
//! (the ADR 0020/0023 resolution moved Supervisor-side), the
//! three-way `enabled_tools` rule, the child `StartEnv`, the
//! `SubagentCapture`, the **Dead bound**, and the
//! `subagent-session-started` / `subagent-closed` UI lifecycle
//! events).
//!
//! The read-loop plumbing: each attached Worker's `events()` is
//! consumed by a pump task that (a) calls `on_event(session_id,
//! is_subagent, evt)` for EVERY variant (the Task-4 router splits UI
//! vs persistence — the `is_subagent` flag is known per attached
//! Worker: main `false`, subagent `true`), (b) on `SubagentDispatch`
//! runs `dispatch_subagent`, and (c) on `Exited` (unexpected — no
//! `detach` / `reap_all` in flight) calls `on_crash(session_id,
//! code, pid)` — the pid being the DEAD child's, captured off the handle
//! at pump start, which is how the router attributes the crash log to the
//! Worker that actually died; an EXPECTED `Exited` (a `detach` / `reap_all`
//! was in flight) fires NO `on_crash` (the Task-4 router's
//! `session-closed` handling, Task 4 step 1).
//!
//! The state is `Arc`-backed (`ManagerState`): the pump / drive tasks
//! are `tokio::spawn`ed (`'static`), so they own clones of the `Arc`
//! — the `&self` methods delegate to the `Arc`-backed free functions.

pub mod pump;
pub mod registry;
pub mod subagent_flow;

#[cfg(test)]
mod tests;

pub use subagent_flow::SubagentCapture;

use subagent_flow::SubagentOutcomeSink;

pub(crate) use registry::{ManagerState, WorkerEntry};
#[cfg(test)]
pub(crate) use subagent_flow::{
    feed_capture, finish_subagent, note_liveness_event, note_liveness_frame, note_liveness_gate,
    SubagentDrive, SubagentLiveness,
};

use std::sync::Arc;
use std::time::Duration;

use crate::agent::debuglog;
use crate::agent::events::EventSink;
use crate::agent::subagent::SubagentOutcome;
use crate::agent::worker::client::{WorkerError, WorkerHandle, WorkerInboundEvent};
use crate::agent::worker::protocol::{StartEnv, SubagentDispatchWire};

/// The default **Dead bound** (ADR 0055): 30 min of UNEXPLAINED SILENCE **as the
/// Supervisor OBSERVES it**. It is the most a **Subagent session** may go with no
/// frame OBSERVED by the pump AND no `tool_call_id` outstanding — it is NOT a
/// ceiling on the length of the delegation (an **Active Subagent** has no timeout
/// of any kind), and it is NOT a measurement of the child: the clock is rearmed by
/// the frames that actually reach the drive's `liveness_rx`
/// (`subagent_flow.rs`), so a parent starved of CPU can OBSERVE a gap the child
/// never stopped producing into and reap it `Unresponsive` with its partial
/// output. The default is 30 min rather than one second because of exactly that:
/// the observation lag implied by the one recorded CI failure was ~700 ms on a
/// loaded runner (issue #34), 0.04% of this bound, against a provider layer that
/// already self-bounds a stalled stream at 5 min. Overridden in production by
/// `settings.json`'s `subagentDeadBoundSecs`.
pub const DEFAULT_SUBAGENT_DEAD_BOUND: std::time::Duration =
    std::time::Duration::from_secs(30 * 60);

/// The Worker spawn seam (the test injection point): production is
/// `WorkerHandle::spawn(std::env::current_exe().as_path())`; tests are
/// the `fake_worker` binary (the manifest-dir path convention).
pub trait WorkerFactory: Send + Sync {
    fn spawn(&self) -> Result<WorkerHandle, WorkerError>;
}

/// The child-cwd registrar signature (the `TranscriptPersister`'s
/// `set_cwd` — `(session_id, cwd)`; a type alias to keep the
/// `OnceLock<Arc<dyn Fn>>` out of the field's type — the clippy
/// `type_complexity` lint).
pub type CwdRegistrar = Arc<dyn Fn(&str, &str) + Send + Sync>;

/// Closure hook types for [`ClosureHooks`].
pub type BoxOnCrash = Box<dyn Fn(&str, Option<i32>, Option<u32>) + Send + Sync>;
pub type BoxOnEvent = Box<dyn Fn(&str, bool, WorkerInboundEvent) + Send + Sync>;
pub type BoxSinkFor = Box<dyn Fn(&str) -> Option<Arc<dyn EventSink>> + Send + Sync>;

/// The Supervisor-side lifecycle and routing hooks for Workers.
pub trait WorkerHooks: Send + Sync {
    fn on_event(&self, session_id: &str, is_subagent: bool, evt: WorkerInboundEvent);
    fn on_crash(&self, session_id: &str, code: Option<i32>, pid: Option<u32>);
    fn sink_for(&self, parent_session_id: &str) -> Option<Arc<dyn EventSink>>;
    fn register_child_cwd(&self, session_id: &str, cwd: &str);
    fn attach_cwd_registrar(&self, _registrar: CwdRegistrar) {}
}

/// A `WorkerHooks` implementation wrapping individual closures with a late-bound
/// `cwd_registrar`.
pub struct ClosureHooks {
    pub on_crash: BoxOnCrash,
    pub on_event: BoxOnEvent,
    pub sink_for: BoxSinkFor,
    pub cwd_registrar: std::sync::OnceLock<CwdRegistrar>,
}

impl ClosureHooks {
    pub fn attach_cwd_registrar(&self, registrar: CwdRegistrar) {
        let _ = self.cwd_registrar.set(registrar);
    }
}

impl WorkerHooks for ClosureHooks {
    fn on_event(&self, session_id: &str, is_subagent: bool, evt: WorkerInboundEvent) {
        (self.on_event)(session_id, is_subagent, evt);
    }
    fn on_crash(&self, session_id: &str, code: Option<i32>, pid: Option<u32>) {
        (self.on_crash)(session_id, code, pid);
    }
    fn sink_for(&self, parent_session_id: &str) -> Option<Arc<dyn EventSink>> {
        (self.sink_for)(parent_session_id)
    }
    fn register_child_cwd(&self, session_id: &str, cwd: &str) {
        if let Some(registrar) = self.cwd_registrar.get() {
            registrar(session_id, cwd);
        }
    }
    fn attach_cwd_registrar(&self, registrar: CwdRegistrar) {
        let _ = self.cwd_registrar.set(registrar);
    }
}

/// The parent-scope sink lookup (session_id → the session's
/// `TauriSink` — the `subagent-session-started` /
/// `subagent-closed` UI lifecycle events are delivered on the
/// PARENT session's sink; `None` = no UI delivery).
pub type SinkLookup = Arc<dyn Fn(&str) -> Option<Arc<dyn EventSink>> + Send + Sync>;

/// The Worker-crash callback (session_id, exit code, the DEAD child's pid) —
/// a type alias to keep the `Arc<dyn Fn>` out of the field's and the ctor
/// parameter's types (the clippy `type_complexity` lint; the same remedy
/// [`SinkLookup`] exists for). The pid is the attribution key: the crash log's
/// name carries the writing process's pid, so the router selects the log that
/// belongs to the Worker that actually died instead of the newest one on disk.
pub type OnCrash = Arc<dyn Fn(String, Option<i32>, Option<u32>) + Send + Sync>;

/// A registry of live Workers (session-keyed) + the Supervisor-side
/// subagent-dispatch flow. The `dead_bound` is OUTSIDE the `Arc`
/// (the `with_dead_bound` test variant swaps it without rebuilding
/// the `Arc`-backed state — the `StdMutex` maps are not `Clone`).
pub struct WorkerManager {
    state: Arc<ManagerState>,
    hooks: Arc<dyn WorkerHooks>,
    /// The **Dead bound** (default 30 min of unexplained silence AS THE
    /// SUPERVISOR OBSERVES IT — the gap between frames the pump actually read,
    /// not the child's own; see [`DEFAULT_SUBAGENT_DEAD_BOUND`]; production
    /// reads it from `settings.json`'s `subagentDeadBoundSecs` via
    /// `with_dead_bound_secs`, tests shorten it via `with_dead_bound`).
    dead_bound: Duration,
}

impl WorkerManager {
    pub fn new(
        factory: Arc<dyn WorkerFactory>,
        on_crash: OnCrash,
        on_event: Arc<dyn Fn(String, bool, WorkerInboundEvent) + Send + Sync>,
        sink_for: SinkLookup,
    ) -> Self {
        let hooks = Arc::new(ClosureHooks {
            on_crash: Box::new(move |session_id, code, pid| {
                on_crash(session_id.to_string(), code, pid);
            }),
            on_event: Box::new(move |session_id, is_subagent, evt| {
                on_event(session_id.to_string(), is_subagent, evt);
            }),
            sink_for: Box::new(move |parent_session_id| sink_for(parent_session_id)),
            cwd_registrar: std::sync::OnceLock::new(),
        });
        Self::with_hooks(factory, hooks)
    }

    pub fn with_hooks(factory: Arc<dyn WorkerFactory>, hooks: Arc<dyn WorkerHooks>) -> Self {
        Self {
            state: Arc::new(ManagerState::new(factory, hooks.clone())),
            hooks,
            dead_bound: DEFAULT_SUBAGENT_DEAD_BOUND,
        }
    }

    pub fn hooks(&self) -> &Arc<dyn WorkerHooks> {
        &self.hooks
    }

    /// A test variant with a shortened **Dead bound** (the `__slow__`
    /// silence-bound tests — the fixture's 10 s settle is longer than
    /// the bound). The `dead_bound` is outside the `Arc`, so the
    /// swap is a field assignment (the `Arc`-backed state is shared).
    pub fn with_dead_bound(self, bound: Duration) -> Self {
        Self {
            state: self.state,
            hooks: self.hooks,
            dead_bound: bound,
        }
    }

    /// The production seam for the ADR 0055 **Dead bound** (the `settings.json`
    /// `subagentDeadBoundSecs` key, applied at the one `lib.rs` construction
    /// site): SECONDS of unexplained silence AS THE SUPERVISOR OBSERVES IT after
    /// which a **Subagent session** is reaped as Dead. Tests inject their bound in
    /// `Duration`s via [`Self::with_dead_bound`]; the unit here is seconds because
    /// that is the unit the setting is written in.
    pub fn with_dead_bound_secs(self, secs: u64) -> Self {
        self.with_dead_bound(Duration::from_secs(secs))
    }

    /// Spawn a Worker for `session_id` + the `ready` handshake (5 s
    /// bound) + the `start` envelope. The read-loop pump is spawned
    /// (the `on_event` / `on_crash` routing — `is_subagent: false`).
    pub async fn attach(&self, session_id: &str, env: &StartEnv) -> Result<(), WorkerError> {
        let handle = (self.state.factory).spawn()?;
        handle.wait_ready(Duration::from_secs(5)).await?;
        // The lifecycle transition (the `ARCHIMEDES_DEBUG` log — a
        // no-op when the var is unset).
        debuglog::log(&format!("worker ready: {session_id}"));
        // The read-loop pump (the `on_event` / `on_crash` routing —
        // `is_subagent: false`; subscribed BEFORE `send_start` so the
        // `Start` frame — the advertised `enabledTools` — is seen).
        pump::start_pump(
            self.state.clone(),
            self.dead_bound,
            session_id.to_string(),
            false,
            handle.clone(),
        );
        handle.send_start(env)?;
        self.state
            .workers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                session_id.to_string(),
                WorkerEntry {
                    handle: handle.clone(),
                    env: env.clone(),
                    reaping: false,
                },
            );
        Ok(())
    }

    /// Detach a session's Worker (the `workers` map bookkeeping — the
    /// entry is REMOVED (the `Exited` is EXPECTED: NO `on_crash`);
    /// the graceful `close` + 2 s grace + `kill` runs in the
    /// background). Returns the detached handle.
    pub fn detach(&self, session_id: &str) -> Option<WorkerHandle> {
        self.state.detach_worker(session_id)
    }

    /// Reap ALL attached Workers (a `close` + grace (2 s) + `kill` —
    /// called on app exit; the `reaping` flags make the `Exited`s
    /// EXPECTED — NO `on_crash`).
    pub async fn reap_all(&self) {
        registry::reap_all(&self.state).await;
    }

    /// The Supervisor-side subagent-dispatch flow (a main-session
    /// Worker's `subagent` tool → its `IpcDispatcher` →
    /// `Outbound::SubagentDispatch` → here): resolve the `model_key`
    /// against the effective catalog (the ADR 0020/0066 resolution — the
    /// `agentOverrides` entry is already folded into
    /// `launch.model` by the Worker's `resolve_launch`, soft-degraded),
    /// build the child `StartEnv` (the resolved `Model` with its
    /// provider config, the catalog, `cwd` = `parent_cwd`, `trusted` =
    /// the parent Space's trust — the Supervisor owns the `spaces`
    /// table, `system_prompt` =
    /// `build_child_system_message(launch.system_prompt,
    /// has_todo_tool)` — a `None` there means the child gets NO system
    /// message at all and NO seq-0 row, `mode: FreshChild` (the start
    /// signal that keeps a child out of the main-prompt arm — issue #9),
    /// `enabled_tools` = the FULL three-way rule
    /// VERBATIM, `subagent_enabled: false` — the recursion guard),
    /// spawn the subagent Worker (a fresh ephemeral session —
    /// `mint_session_id`), `wait_ready` + `send_start` + the initial
    /// `prompt` (the `task`), then the drive task (`agent_settled` →
    /// the `SubagentCapture`'s final text → `Completed`; an unexpected
    /// `Exited` → `Failed { reason: Crashed, error: "subagent worker exited
    /// unexpectedly (code N)" }`; the **Dead bound** → `Failed { reason:
    /// Unresponsive, error: "unresponsive" }`; a `SubagentCancel` →
    /// `send_abort` + reap → `Failed { reason: Cancelled }`). EVERY `Failed`
    /// carries the `SubagentCapture`'s partial text + the real usage (ADR 0055
    /// — a **Dead Subagent** is never `Completed`, but its work is not thrown
    /// away). The `Inbound::SubagentResult { id, outcome }` is sent to
    /// the REQUESTING (main-session) Worker. The
    /// `subagent-session-started` / `subagent-closed` UI lifecycle
    /// events fire on the PARENT session's sink (the `subagent.rs:635`
    /// / `721/743` payload shapes — the `metrics` captured from the
    /// subagent's event stream + the wall clock).
    pub fn dispatch_subagent(&self, parent_session_id: &str, wire: SubagentDispatchWire) {
        subagent_flow::dispatch_subagent(
            self.state.clone(),
            self.dead_bound,
            parent_session_id.to_string(),
            wire,
            SubagentOutcomeSink::Requester,
            None,
        );
    }

    /// The in-process `dispatch_native` variant (ADR 0025 Task 5 — the
    /// `SubagentSessionManager` re-plumb): the outcome RESOLVES the
    /// oneshot (NOT delivered to a requesting Worker — there is none),
    /// and the child session id is PRE-MINTED (the caller's
    /// `SubagentCancel` targets it via `cancel_subagent`).
    pub fn dispatch_subagent_to(
        &self,
        parent_session_id: &str,
        wire: SubagentDispatchWire,
        child_session_id: String,
        outcome: tokio::sync::oneshot::Sender<SubagentOutcome>,
    ) {
        subagent_flow::dispatch_subagent(
            self.state.clone(),
            self.dead_bound,
            parent_session_id.to_string(),
            wire,
            SubagentOutcomeSink::Oneshot(std::sync::Arc::new(std::sync::Mutex::new(Some(outcome)))),
            Some(child_session_id),
        );
    }

    /// Set the child-cwd registrar (the `lib.rs` late-wire — the persister lives
    /// in the `SessionManager`, which the `WorkerManager` cannot see at construction).
    pub fn set_cwd_registrar(&self, registrar: CwdRegistrar) {
        self.hooks.attach_cwd_registrar(registrar);
    }

    /// Cancel an in-flight subagent dispatch (a parent turn abort —
    /// the `SubagentCancel` flow: `send_abort` + the drive task's
    /// `cancel` flag → the `Cancelled` outcome + the reap).
    pub fn cancel_subagent(&self, session_id: &str) {
        subagent_flow::cancel_subagent_by_child_id(&self.state, session_id);
    }

    // ── Test support (the registry view) ────────────────────────────

    /// Whether a session's `Exited` is EXPECTED (a `detach` / `reap_all` was in
    /// flight — the `SessionManager`'s router `Exited` handling uses this to decide
    /// the `session-closed` cleanup vs. the `on_crash` path): `true` when the
    /// session is NOT in the `workers` map (already `detach`ed) OR is reaping
    /// (`reap_all` set the flag); `false` when it is in the map and NOT reaping
    /// (a crash — the `on_crash` fires).
    pub fn is_exit_expected(&self, session_id: &str) -> bool {
        self.state.is_exit_expected(session_id)
    }

    /// The registry view (the `test_support`-exposed accessor — the
    /// session count). NOT `#[cfg(test)]`-gated: the headless IPC test
    /// (an integration target — the lib's `cfg(test)` is not compiled
    /// there) asserts through the `test_support` accessor.
    pub fn test_session_count(&self) -> usize {
        self.state.worker_count()
    }

    /// The in-flight SUBAGENT view (how many `drives` entries exist — a
    /// subagent Worker is NOT in `workers`, so `test_session_count` never
    /// sees it: the count here is the leak check for a dispatch that was
    /// cancelled / settled but never torn down). NOT `#[cfg(test)]`-gated
    /// (see `test_session_count`).
    pub fn test_drive_count(&self) -> usize {
        self.state.drive_count()
    }

    /// The registry view (is the session attached?). NOT `#[cfg(test)]`-gated
    /// (the headless IPC test asserts through it — see `test_session_count`).
    pub fn test_is_attached(&self, session_id: &str) -> bool {
        self.state.is_worker_attached(session_id)
    }

    /// The registry view (are ALL sessions reaping?). NOT `#[cfg(test)]`-gated
    /// (the headless IPC test asserts through it — see `test_session_count`).
    pub fn test_all_reaping(&self) -> bool {
        self.state.all_workers_reaping()
    }

    /// Send a `Config { trusted }` frame to every RUNNING Worker in the
    /// `cwd` Space (the `set_space_trusted` mid-session trust toggle —
    /// the Worker's `StaticTrustSource` flip + the `ControlCmd` queue;
    /// the `trust-space` permission-outcome path is the separate
    /// `respond_permission` `db` write + flip). Returns the number of
    /// Workers the frame was DELIVERED to (a failed send — the Worker
    /// died / the pipe closed — is not counted; a mid-session toggle
    /// for a dead Worker is a no-op, not an error).
    pub fn send_config_to_space(&self, cwd: &str, trusted: bool) -> usize {
        let map = self.state.workers.lock().unwrap_or_else(|p| p.into_inner());
        let mut delivered = 0;
        for entry in map.values() {
            if entry.env.cwd == cwd && entry.handle.send_config(None, None, Some(trusted)).is_ok() {
                delivered += 1;
            }
        }
        delivered
    }

    /// The session's `WorkerHandle` (a clone) — `None` when the session is not
    /// attached (or was already reaped). The `SessionManager` uses this to relay
    /// frames (prompt / config / abort / responses) to the session's Worker.
    /// The `drives` fallback (ADR 0025 Task 5 — the `dispatch_native` re-plumb):
    /// a SUBAGENT session's handle is in the `drives` map (the in-flight
    /// dispatchs — the subagent's pending maps live in its Worker; the
    /// `respond_*` commands route through this lookup — main AND subagent
    /// sessions both resolve).
    pub fn handle_for(&self, session_id: &str) -> Option<WorkerHandle> {
        self.state.get_handle_for(session_id)
    }

    /// The TRUE answer moment for a **gate** (ADR 0055 review): the Supervisor's
    /// `respond_permission` / `respond_interactive_request` relay the user's
    /// answer to the session's Worker, and this clears the child's record of it
    /// so teardown stops re-emitting a close for a card the user already
    /// answered. The `session_id` is the CHILD id for a child gate (the gate
    /// payload is stored child-keyed and the Client answers with that same
    /// `sessionId`).
    ///
    /// A MISSING drive is a NO-OP — a main session has no `drives` entry at all,
    /// and a child whose dispatch is already gone has nothing left to tear down.
    ///
    /// This is the ONLY place `open_gates` is cleared: inferring the answer from
    /// the gate's `tool_execution_end` would be wrong, because a cancelled gate
    /// also closes its tool call (the loop fabricates the "permission denied"
    /// result) while the card stays on screen.
    pub fn note_gate_answered(&self, session_id: &str, request_id: &str) {
        let drives = self.state.drives.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(drive) = drives.get(session_id) {
            drive.liveness_tx.send_modify(|l| {
                l.open_gates.remove(request_id);
            });
        }
    }
}

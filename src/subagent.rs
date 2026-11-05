//! The subagent session manager (ADR 0025 Task 5 — the re-plumb): a
//! subagent dispatch runs the child in a WORKER process (the
//! dispatcher's `dispatch_subagent` flow — a subagent Worker per
//! dispatch). The in-process child `AgentLoop` + the throwaway-`Db`
//! machinery are GONE (the subagent's transcript now persists as a
//! hidden ephemeral row — the Supervisor's `TranscriptPersister` applies
//! the subagent Worker's store frames with the `is_subagent` flag).
//!
//! The manager is the SUPERVISOR-side dispatcher: the
//! `InProcessDispatcher` (the `AgentLoop`'s `subagent` seam — the test
//! path) wraps `dispatch_native`, which delegates to the
//! dispatcher's `dispatch_subagent` flow (the production path is a
//! main-session Worker's `IpcDispatcher` → `Outbound::SubagentDispatch`
//! → the dispatcher's flow directly). Subagents cannot dispatch
//! subagents (the child Worker's `enabled_tools` exclude `subagent` /
//! `list_agents` — the recursion guard, the dispatcher's flow).

use std::path::Path;
use std::sync::Arc;

use tokio::sync::{oneshot, watch};

use crate::events::EventSink;
use crate::harness::Model;
use crate::harness::ModelKey;
use crate::seams::Dispatcher;
use crate::types::mint_session_id;
use crate::worker_types::SubagentDispatchWire;

// T5/S3: `LaunchConfig` moved to `fulcrum_core::launch_config` — it is pure
// data (six `Option` fields, its only non-`std` reference is
// `definition_permission::PermissionBlock`, which moved in S2) while THIS file
// could not move until S5b because of the `WorkerManager` edge below it.
// `interactive`
// (S3) imports it, so the type had to move ahead of its neighbours; the E.1
// wire-type pattern, not a seam. Re-exported HERE so every existing
// `subagent::LaunchConfig` path — `bin/fake_worker.rs`, `commands/agents.rs`,
// `harness/launch.rs`, `harness/dispatch.rs`, `worker/protocol.rs`,
// `worker/dispatch.rs`, `worker/manager/tests/mod.rs`,
// `tests/harness_dispatch_native.rs` — keeps resolving with zero edits. See the
// crate module's docs for the full rationale.
pub use crate::launch_config::LaunchConfig;

// T5/S4 (plan Step E.2/E.3): the subagent dispatch VOCABULARY moved to
// `fulcrum_core::subagent_types` with `LaunchConfig`'s siblings. The reason is the
// same ordering constraint S3 hit and for the same remedy — `SubagentDispatchWire`
// (E3) names `AgentNameResolution` in a field, and the crate's `Dispatcher` trait
// (E2) takes a `oneshot::Sender<SubagentOutcome>`, so neither the wire type nor the
// trait can be DECLARED while those two still live here; and THIS file cannot move
// until S5b because of that edge. Pure data moves ahead of the
// behaviour that owns it, re-exported so every existing path resolves.
//
// Re-exported HERE (and through `agent/mod.rs`) for all four, so
// `subagent::AgentNameResolution`, `agent::SubagentOutcome` and
// `archimedes_lib::agent::subagent::SubagentMetrics` keep compiling at their ~40
// sites, including `bin/fake_worker.rs`, `harness/launch.rs`, `harness/dispatch.rs`
// and the two-sided `worker_e2e` fixtures.
//
// `SubagentCancel` lives below, in this crate: its dispatch variant used to
// hold `Arc<WorkerManager>` — the edge E2 re-appearing through the back door —
// and S5b replaced it with `Arc<dyn Dispatcher>`, exactly the re-plumb the trait's
// own docs promised. The `Flag` and `None` arms are unchanged.
pub use crate::subagent_types::{
    AgentNameResolution, SubagentFailureReason, SubagentMetrics, SubagentOutcome,
};

/// Manages the subagent dispatchs (ADR 0025 Task 5 — the re-plumb):
/// `dispatch_native` delegates to the [`Dispatcher`] (the
/// `dispatch_subagent` flow — a subagent Worker per dispatch; the
/// model / `thinking` resolution is SUPERVISOR-side, the three-way
/// `enabled_tools` rule + the `system_prompt` envelope field + the
/// `SubagentCapture` + the **Dead bound** + the
/// `subagent-session-started` / `subagent-closed` UI lifecycle events
/// live in the flow). The dispatcher is set ONCE at the
/// `SessionManager` wiring time (`set_dispatcher` — the
/// `SessionManager`'s `attach_worker_manager` / `set_subagent_manager`
/// late-wire forwards it). The subagent's `sessions` row is recorded by
/// the Supervisor's `TranscriptPersister` (the `is_subagent` flag) — the
/// manager no longer records it.
pub struct SubagentSessionManager {
    /// The dispatch seam (the `dispatch_native` delegate — the subagent
    /// runs in a Worker process; `None` = a manager never wired into a
    /// `SessionManager` → `dispatch_native` is an already-resolved
    /// `Failed`).
    ///
    /// T5/S5b (edge E2): the concrete `Arc<WorkerManager>` this held is the
    /// edge itself, and it is now the crate's [`Dispatcher`] trait. The
    /// desktop's `WorkerManager` implements it in `worker/manager/mod.rs`, one
    /// line per method, and the S4 instantiation pin
    /// (`tests/fulcrum_seams.rs`) is what proves the impl is real. Renamed
    /// beside the type: the field is PRIVATE, so nothing outside this file
    /// named it (measured: the only readers were `set_worker_manager` /
    /// `worker_manager`, both `pub` and both renamed here — A.3's rule that the
    /// STAYING desktop's names decide visibility applies to the name too).
    dispatcher: std::sync::OnceLock<Arc<dyn Dispatcher>>,
}

impl Default for SubagentSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SubagentSessionManager {
    /// Create a manager (the dispatcher is threaded at the
    /// `SessionManager` wiring time — `set_dispatcher`).
    pub fn new() -> Self {
        Self {
            dispatcher: std::sync::OnceLock::new(),
        }
    }

    /// Store the dispatcher (set-once — a second call is a NO-OP;
    /// the `OnceLock` is set at the `SessionManager` wiring time, so
    /// the setter takes `&self` and is callable through the `Arc`).
    pub fn set_dispatcher(&self, dispatcher: Arc<dyn Dispatcher>) {
        let _ = self.dispatcher.set(dispatcher);
    }

    /// The dispatcher (`None` until `set_dispatcher` — a
    /// manager that was never wired into a `SessionManager`).
    pub fn dispatcher(&self) -> Option<Arc<dyn Dispatcher>> {
        self.dispatcher.get().cloned()
    }

    /// Spawn a WORKER for a subagent dispatch (the ADR 0025 Task 5
    /// re-plumb): delegate to the `WorkerManager`'s `dispatch_subagent`
    /// flow (a subagent Worker per dispatch — the model / `thinking`
    /// resolution is SUPERVISOR-side, the `SubagentCapture`'s final text
    /// → `Completed`, a crash → `Failed { error: "subagent worker
    /// exited unexpectedly (code N)" }`, the **Dead bound** → `Failed`
    /// (`reason: Unresponsive`), a `SubagentCancel` → `send_abort` + reap). The
    /// child's transcript persists as a hidden ephemeral row (the
    /// `TranscriptPersister`'s `is_subagent` flag — the child's seq-0
    /// system-message row is the `system_prompt` envelope field).
    ///
    /// The return adapts to the existing
    /// `(oneshot::Receiver<SubagentOutcome>, SubagentCancel)` contract
    /// (the `SubagentCancel` the caller — the loop's `dispatch_subagent`
    /// — uses to abort the dispatch maps to the flow's `cancel_subagent`
    /// — the pre-minted child session id is the `drives` map key).
    ///
    /// `None` dispatcher (a manager never wired into a
    /// `SessionManager`) → an already-resolved `Failed` (NO fallback —
    /// a native parent always has an attached `WorkerManager` in
    /// production; the "no manager" case is handled by
    /// `dispatch_subagent`'s existing `subagent: None` check).
    ///
    /// (issue #47) `name_resolution` is the report the loop's
    /// `resolve_launch` computed on `agent_name`; it rides the wire VERBATIM
    /// so the flow's `subagent-session-started` payload can name a resolve
    /// miss. Nothing here branches on it — the child runs identically in all
    /// three states.
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch_native(
        &self,
        parent_session_id: &str,
        parent_cwd: &Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        name_resolution: AgentNameResolution,
        launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        // The dispatcher (the `dispatch_subagent` flow's home — a
        // `None` manager is an already-resolved `Failed`).
        let Some(dispatcher) = self.dispatcher.get().cloned() else {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(SubagentOutcome::Failed {
                // A preflight failure — the child NEVER started, so the empty
                // `output` / default `metrics` are deliberate: nothing was
                // ever produced.
                reason: SubagentFailureReason::Other,
                error: "native subagent dispatch is not configured".to_string(),
                output: String::new(),
                metrics: SubagentMetrics::default(),
            });
            return (rx, SubagentCancel::none());
        };
        // The PRE-MINTED child session id (the `SubagentCancel` targets
        // it — the flow's `cancel_subagent` looks it up in the `drives`
        // map; the flow would mint its own without the pre-mint).
        let child_id = mint_session_id();
        let (tx, rx) = oneshot::channel();
        let wire = SubagentDispatchWire {
            id: uuid::Uuid::new_v4().to_string(),
            parent_session_id: parent_session_id.to_string(),
            parent_cwd: parent_cwd.display().to_string(),
            parent_enabled_tools,
            agent_name,
            name_resolution,
            launch,
            task,
            // The RESOLVED model key (the `provider/id` composition —
            // the flow resolves it against the parent's effective
            // catalog; a `launch.model` override wins over it).
            model_key: ModelKey::from(parent_model).to_string(),
        };
        dispatcher.dispatch_subagent_to(parent_session_id.as_ref(), wire, child_id.clone(), tx);
        (rx, SubagentCancel::new_dispatcher(dispatcher, child_id))
    }
}

/// The cancel handle for a subagent dispatch (the ADR 0025 Task 5
/// re-plumb — the in-process watch-flag form is gone: the child is a
/// WORKER process, and the cancel is the `WorkerManager`'s
/// `cancel_subagent` (the `send_abort` + the drive's `Cancelled`
/// outcome + the reap)). `cancel()` is idempotent.
#[derive(Clone)]
pub enum SubagentCancel {
    /// A Worker-backed dispatch: `cancel()` asks the `WorkerManager` to
    /// abort the child Worker (the `send_abort` + the drive's
    /// `Cancelled` outcome + the reap).
    Dispatcher {
        dispatcher: Arc<dyn Dispatcher>,
        child_session_id: String,
    },
    /// The flag (the `IpcDispatcher` — a watcher task turns the flip
    /// into the `SubagentCancel` frame; the Worker's `SubagentCancel`
    /// frame routes to the Supervisor's `cancel_subagent`).
    Flag(watch::Sender<bool>),
    /// A no-op (the `MockDispatcher`'s empty-queue default — the
    /// `SubagentWait` select's outcome arm never fires; the turn-cancel
    /// arm wins).
    None,
}

impl SubagentCancel {
    /// A no-op cancel (the `MockDispatcher`'s empty-queue default).
    pub fn none() -> Self {
        SubagentCancel::None
    }

    /// A flag-backed cancel (the `IpcDispatcher` — a watcher task
    /// observes the flip and emits the `SubagentCancel` frame).
    pub fn new_flag(tx: watch::Sender<bool>) -> Self {
        SubagentCancel::Flag(tx)
    }

    /// A Worker-backed cancel (the `dispatch_native` re-plumb — the
    /// dispatcher's `cancel_subagent` targets the pre-minted child
    /// session id).
    pub fn new_dispatcher(dispatcher: Arc<dyn Dispatcher>, child_session_id: String) -> Self {
        SubagentCancel::Dispatcher {
            dispatcher,
            child_session_id,
        }
    }

    /// Cancel the dispatch. Idempotent (a second `cancel` is a no-op —
    /// the dispatcher's `cancel_subagent` is a no-op for a gone
    /// drive; the flag `send` is a no-op for a gone receiver).
    pub fn cancel(&self) {
        match self {
            SubagentCancel::Dispatcher {
                dispatcher,
                child_session_id,
            } => dispatcher.cancel_subagent(child_session_id),
            SubagentCancel::Flag(tx) => {
                let _ = tx.send(true);
            }
            SubagentCancel::None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use tokio::sync::oneshot;

    use crate::seams::Dispatcher;
    use crate::subagent::{SubagentCancel, SubagentOutcome, SubagentSessionManager};

    /// A crate-side [`Dispatcher`] double (T5/S5b). The two pins below are the
    /// only survivors of the `WorkerManager`-backed cluster in this module, and
    /// they need a dispatcher to hold — so they get the crate's own trait object
    /// rather than a `WorkerManager`. WHEN THIS WAS WRITTEN THAT WAS A BOUNDARY
    /// RULE (`worker/` was desktop through T8 and this crate could not name it);
    /// T8/S8.3 moved `worker/manager/` in, so `crate::worker::manager::
    /// WorkerManager` is nameable here now. The double is still the right choice,
    /// for the reason stated below rather than the boundary rule: a real manager
    /// spawns a child process, and neither claim below is about the manager.
    ///
    /// WHY A DOUBLE IS HONEST HERE AND WAS NOT HONEST FOR THE OTHER EIGHT
    ///
    /// These two tests' claims are about THIS file: the `OnceLock`'s set-once
    /// rule, and `SubagentCancel::cancel()` routing to the right arm. The
    /// dispatcher is not the thing under test — it is the collaborator the thing
    /// under test is handed — so implementing it here asserts nothing circular.
    /// The eight tests that RELOCATED to `src-tauri/tests/harness_subagent_worker.rs`
    /// each asserted something the DESKTOP's `dispatch_subagent` flow computes
    /// (the capture text, the exit code, the Dead bound, the recursion guard, the
    /// `trusted` inheritance), and a double would have made all of those
    /// tautological. That is the line this split is drawn on, and it is drawn per
    /// claim rather than per file.
    struct RecordingDispatcher {
        cancels: StdMutex<Vec<String>>,
    }

    impl RecordingDispatcher {
        fn new() -> Self {
            Self {
                cancels: StdMutex::new(Vec::new()),
            }
        }

        fn cancels(&self) -> Vec<String> {
            self.cancels
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
        }
    }

    impl Dispatcher for RecordingDispatcher {
        fn dispatch_subagent_to(
            &self,
            _parent_session_id: &str,
            _wire: crate::worker_types::SubagentDispatchWire,
            _child_session_id: String,
            _outcome: oneshot::Sender<SubagentOutcome>,
        ) {
        }

        fn cancel_subagent(&self, child_session_id: &str) {
            self.cancels
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(child_session_id.to_string());
        }
    }

    /// (6) `set_dispatcher` (`&self`, `OnceLock`): unset in `new`; the first
    /// `set_dispatcher` stores the dispatcher (readable via `dispatcher()`); a
    /// second `set_dispatcher` is a NO-OP (the first set wins — the `OnceLock` is
    /// set-once at wiring time).
    ///
    /// (T5/S5b) Re-pointed, not relocated: the claim is about THIS file's
    /// `OnceLock`, and the S5b edge-E2 re-plumb renamed both the field and its
    /// accessors with the type they hold (`Arc<WorkerManager>` →
    /// `Arc<dyn Dispatcher>`, `set_worker_manager`/`worker_manager` →
    /// `set_dispatcher`/`dispatcher`), so the fixture names the trait object the
    /// setter now takes. See `RecordingDispatcher` for why a double is honest for
    /// this pin and was not for the eight that relocated.
    #[test]
    fn set_worker_manager_is_set_once_on_the_manager() {
        let manager = SubagentSessionManager::new();
        // Unset in `new` (the dispatcher is threaded at the `SessionManager`
        // wiring time).
        assert!(manager.dispatcher().is_none());
        let first: Arc<dyn Dispatcher> = Arc::new(RecordingDispatcher::new());
        manager.set_dispatcher(first.clone());
        // The first `set_dispatcher` stored the dispatcher (readable through the
        // `OnceLock`).
        assert!(manager.dispatcher().is_some(), "the first set is stored");
        // A second `set_dispatcher` is a NO-OP (the first set wins).
        let second: Arc<dyn Dispatcher> = Arc::new(RecordingDispatcher::new());
        manager.set_dispatcher(second.clone());
        assert!(
            Arc::ptr_eq(&manager.dispatcher().unwrap(), &first),
            "a second set is a NO-OP (the first set wins)"
        );
        // The double was NOT the one stored — the pin would fail if `set_once`
        // became set-last, because then `second` would be the stored one.
        assert!(
            !Arc::ptr_eq(&manager.dispatcher().unwrap(), &second),
            "the rejected dispatcher is never reachable through the getter"
        );
    }

    /// (7) `SubagentCancel` variants: `None` is a NO-OP (idempotent, never
    /// panics); `Flag` flips the watch flag (the `IpcDispatcher`'s watcher task
    /// turns the flip into the `SubagentCancel` frame — a second `cancel` is a
    /// no-op); `Dispatcher` routes `cancel()` to the dispatcher's
    /// `cancel_subagent` with the CHILD's session id, and is idempotent.
    ///
    /// (T5/S5b) The variant this pin used to name held `Arc<WorkerManager>` —
    /// edge E2 arriving through the back door — and S5b replaced it with
    /// `Arc<dyn Dispatcher>`. The routing assertion got STRONGER with the swap:
    /// against the desktop's `WorkerManager` a gone session id made `cancel()` an
    /// unobservable no-op, so the pin could only prove "does not panic". Against
    /// this double it proves the CHILD ID IS PASSED, which is the behaviour a
    /// mis-threaded `child_session_id` would break (a parent's cancel reaching the
    /// wrong drive, or no drive).
    #[test]
    fn subagent_cancel_variants_are_well_behaved() {
        // `None` — a no-op (idempotent).
        SubagentCancel::none().cancel();
        SubagentCancel::none().cancel();
        // `Flag` — flips the watch flag (the `IpcDispatcher`'s watcher task
        // observes it); a second `cancel` is a no-op.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let cancel = SubagentCancel::new_flag(tx);
        assert!(!*rx.borrow());
        cancel.cancel();
        assert!(*rx.borrow(), "the flag flipped");
        cancel.cancel();
        assert!(*rx.borrow(), "the flag stays flipped (idempotent)");
        // `Dispatcher` — `cancel()` reaches the dispatcher's `cancel_subagent`
        // with the child's id, every time (idempotent by repetition, not by
        // suppression — the desktop's own `cancel_subagent` is a no-op for a gone
        // drive, which is what makes calling it twice safe).
        let dispatcher = Arc::new(RecordingDispatcher::new());
        let cancel = SubagentCancel::new_dispatcher(dispatcher.clone(), "child-7".to_string());
        cancel.cancel();
        assert_eq!(
            dispatcher.cancels(),
            vec!["child-7".to_string()],
            "`cancel()` routes to `cancel_subagent` with the CHILD's id"
        );
        cancel.cancel();
        assert_eq!(
            dispatcher.cancels(),
            vec!["child-7".to_string(), "child-7".to_string()],
            "a second `cancel()` is safe (the same id again — never a panic, \
             never a different id)"
        );
    }
}

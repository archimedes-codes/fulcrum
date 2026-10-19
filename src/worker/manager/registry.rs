use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use crate::agent::worker::client::WorkerHandle;
use crate::agent::worker::protocol::StartEnv;

use super::subagent_flow::SubagentDrive;
use super::{WorkerFactory, WorkerHooks};

/// One live Worker (the `attach`-time `StartEnv` + the `reaping`
/// flag — an `Exited` while reaping is EXPECTED: no `on_crash`).
pub(crate) struct WorkerEntry {
    pub(crate) handle: WorkerHandle,
    pub(crate) env: StartEnv,
    pub(crate) reaping: bool,
}

/// The `WorkerManager`'s `Arc`-backed state (the pump / drive tasks
/// own clones of the `Arc` — the `&self` methods delegate to the
/// `Arc`-backed free functions; the `StdMutex` fields are NOT
/// `Clone`, so the `Arc` wrap is the `'static` move).
pub(crate) struct ManagerState {
    /// session_id → the live Worker (the `attach`-time `StartEnv` —
    /// the `dispatch_subagent` reads the parent's effective `catalog`
    /// / `config_dir` / `trusted` from it — the Supervisor owns them).
    pub(crate) workers: StdMutex<HashMap<String, WorkerEntry>>,
    /// The test seam (production: `current_exe()`; tests: the
    /// `fake_worker` binary).
    pub(crate) factory: Arc<dyn WorkerFactory>,
    /// The Supervisor lifecycle and routing hooks (`WorkerHooks`).
    pub(crate) hooks: Arc<dyn WorkerHooks>,
    /// The in-flight subagent dispatchs (session_id → the drive —
    /// the pump flips the settle watch on `agent_settled`, checks the
    /// `reaping` flag on `Exited`, and the `cancel_subagent` flips
    /// the cancel watch).
    pub(crate) drives: StdMutex<HashMap<String, SubagentDrive>>,
}

impl ManagerState {
    pub(crate) fn new(factory: Arc<dyn WorkerFactory>, hooks: Arc<dyn WorkerHooks>) -> Self {
        Self {
            workers: StdMutex::new(HashMap::new()),
            factory,
            hooks,
            drives: StdMutex::new(HashMap::new()),
        }
    }

    pub(crate) fn is_exit_expected(&self, session_id: &str) -> bool {
        let map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        match map.get(session_id) {
            Some(e) => e.reaping,
            None => true,
        }
    }

    pub(crate) fn worker_count(&self) -> usize {
        self.workers.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub(crate) fn drive_count(&self) -> usize {
        self.drives.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub(crate) fn is_worker_attached(&self, session_id: &str) -> bool {
        self.workers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(session_id)
    }

    pub(crate) fn all_workers_reaping(&self) -> bool {
        let map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        map.values().all(|e| e.reaping)
    }

    pub(crate) fn get_handle_for(&self, session_id: &str) -> Option<WorkerHandle> {
        {
            let map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(e) = map.get(session_id) {
                return Some(e.handle.clone());
            }
        }
        self.drives
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(session_id)
            .and_then(|d| d.handle.clone())
    }

    pub(crate) fn detach_worker(&self, session_id: &str) -> Option<WorkerHandle> {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let entry = map.remove(session_id)?;
        let reap_handle = entry.handle.clone();
        tokio::spawn(async move {
            reap_worker(reap_handle).await;
        });
        Some(entry.handle)
    }
}

/// An UNEXPECTED `Exited` (the `on_crash` fires): the session is still
/// registered (the `workers` map for main sessions, the `drives` map
/// for subagents) AND not reaping. An already-detached / torn-down
/// session (absent from both) is EXPECTED — no `on_crash`.
pub(crate) fn unexpected_exit(state: &ManagerState, session_id: &str) -> bool {
    {
        let workers = state.workers.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = workers.get(session_id) {
            return !entry.reaping;
        }
    }
    let drives = state.drives.lock().unwrap_or_else(|p| p.into_inner());
    match drives.get(session_id) {
        Some(drive) => !drive.reaping,
        None => false,
    }
}

/// The graceful reap (the `close` + grace (2 s) + `kill` — the
/// `reaping` flag is set by the caller BEFORE this runs, so the
/// `Exited` is EXPECTED: NO `on_crash`).
pub(crate) async fn reap_worker(handle: WorkerHandle) {
    let _ = handle.send_close();
    let _ = tokio::time::timeout(Duration::from_secs(2), handle.exited()).await;
    let _ = handle.kill().await;
}

/// Reap ALL attached Workers (a `close` + grace (2 s) + `kill` —
/// called on app exit; the `reaping` flags make the `Exited`s
/// EXPECTED — NO `on_crash`).
pub(crate) async fn reap_all(state: &ManagerState) {
    let handles: Vec<WorkerHandle> = {
        let mut map = state.workers.lock().unwrap_or_else(|p| p.into_inner());
        for entry in map.values_mut() {
            entry.reaping = true;
        }
        map.values().map(|e| e.handle.clone()).collect()
    };
    for handle in handles {
        reap_worker(handle).await;
    }
}

//! The subagent dispatch seam (ADR 0025): `InProcessDispatcher`
//! (delegates to `SubagentSessionManager::dispatch_native` — the current
//! behavior; after Task 5, `dispatch_native` delegates to the
//! `WorkerManager`, so this is the test path) / `IpcDispatcher` (Task 2 —
//! the Worker: `SubagentDispatch` / `SubagentResult` round-trip) /
//! `MockDispatcher` (test double — the `SubagentWait` select tests).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::agent::harness::catalog::Model;
use crate::agent::session::EventSink;
use crate::agent::subagent::{
    LaunchConfig, SubagentCancel, SubagentOutcome, SubagentSessionManager,
};

/// The subagent dispatch seam (ADR 0025): `InProcessDispatcher`
/// (delegates to `SubagentSessionManager::dispatch_native` — the current
/// behavior; after Task 5, `dispatch_native` delegates to the
/// `WorkerManager`, so this is the test path) / `IpcDispatcher` (Task 2 —
/// the Worker: `SubagentDispatch` / `SubagentResult` round-trip) /
/// `MockDispatcher` (test double — the `SubagentWait` select tests).
pub trait SubagentDispatcher: Send + Sync {
    /// Mirrors `SubagentSessionManager::dispatch_native`'s signature
    /// VERBATIM (same parameters, same
    /// `(oneshot::Receiver<SubagentOutcome>, SubagentCancel)` return).
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &self,
        parent_session_id: &str,
        parent_cwd: &std::path::Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        SubagentCancel,
    );
}

/// The in-process dispatcher (delegates to
/// `SubagentSessionManager::dispatch_native` verbatim — the current
/// behavior).
pub struct InProcessDispatcher(Arc<SubagentSessionManager>);

impl InProcessDispatcher {
    pub fn new(manager: Arc<SubagentSessionManager>) -> Self {
        Self(manager)
    }
}

impl SubagentDispatcher for InProcessDispatcher {
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &self,
        parent_session_id: &str,
        parent_cwd: &std::path::Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        SubagentCancel,
    ) {
        self.0.dispatch_native(
            parent_session_id,
            parent_cwd,
            parent_model,
            parent_enabled_tools,
            agent_name,
            launch,
            task,
            sink,
        )
    }
}

/// A test double for the `SubagentWait` select tests (the `loop.rs`
/// in-file tests that exercise the dispatch SELECT — the outcome vs
/// turn-cancel race — without a real dispatch): a canned
/// `(oneshot, SubagentCancel)` queue the test resolves.
///
/// The queue holds the oneshot's RECEIVER (the test keeps the paired
/// SENDER — it is the only party that can resolve the oneshot; `dispatch`
/// returns the receiver, which the loop's `select!` awaits). An empty
/// queue returns a never-resolving oneshot + a no-op `SubagentCancel`
/// (the `select!`'s outcome arm never fires; the turn-cancel arm wins).
pub struct MockDispatcher {
    pub outcomes: Mutex<
        VecDeque<(
            tokio::sync::oneshot::Receiver<SubagentOutcome>,
            SubagentCancel,
        )>,
    >,
}

impl MockDispatcher {
    pub fn new() -> Self {
        Self {
            outcomes: Mutex::new(VecDeque::new()),
        }
    }

    /// Queue a canned `(oneshot, SubagentCancel)` pair (the next
    /// `dispatch` pops it — the test keeps the SENDER to resolve it).
    pub fn queue(
        &self,
        rx: tokio::sync::oneshot::Receiver<SubagentOutcome>,
        cancel: SubagentCancel,
    ) {
        self.outcomes.lock().unwrap().push_back((rx, cancel));
    }
}

impl Default for MockDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl SubagentDispatcher for MockDispatcher {
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _launch: LaunchConfig,
        _task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        SubagentCancel,
    ) {
        match self
            .outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop_front()
        {
            Some((rx, cancel)) => (rx, cancel),
            None => {
                // A never-resolving oneshot + a no-op `SubagentCancel`
                // (the test's `select!` waits on the turn token instead —
                // the outcome never arrives).
                let (tx, rx) = tokio::sync::oneshot::channel();
                std::mem::forget(tx);
                (rx, SubagentCancel::none())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::ModelCatalog;
    use crate::agent::session::EventSink;
    use crate::agent::subagent::SubagentOutcome;
    use crate::agent::worker::client::{WorkerError, WorkerHandle};
    use crate::agent::worker::manager::{WorkerFactory, WorkerManager};
    use std::time::Duration;

    /// A no-op `EventSink` (the `dispatch` signature's `sink` parameter —
    /// the `dispatch_native` re-plumb's no-op; the lifecycle events ride
    /// the `WorkerManager`'s `sink_for` lookup).
    struct NoopSink;
    impl EventSink for NoopSink {
        fn emit(&self, _event: &str, _payload: serde_json::Value) {}
    }

    /// A fake OpenAI-compatible model (the `fake_worker`'s `model_key`
    /// `fake/m1` resolves against the parent's catalog).
    fn test_model() -> Model {
        Model {
            id: "m1".to_string(),
            provider: "fake".to_string(),
            base_url: "http://fake/v1".to_string(),
            api_key: "k".to_string(),
            context_window: 100_000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }

    /// The `fake_worker` fixture factory (the `WorkerFactory` seam — the
    /// `CARGO_MANIFEST_DIR`/`target/debug` convention; `cargo test` builds
    /// the bin targets).
    struct FakeWorkerFactory;

    impl WorkerFactory for FakeWorkerFactory {
        fn spawn(&self) -> Result<WorkerHandle, WorkerError> {
            WorkerHandle::spawn(&std::path::PathBuf::from(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/target/debug/fake_worker"
            )))
        }
    }

    /// A `SubagentSessionManager` on a `WorkerManager` (the ADR 0025 Task 5
    /// re-plumb — the subagent runs in a `fake_worker` process): the parent
    /// `parent-1` is `attach`ed with a single-model catalog (the
    /// `dispatch_subagent` flow resolves the child's `model` against it),
    /// and the `sink_for` lookup returns `None` (the lifecycle events are
    /// not asserted on here — the oneshot is the observation point).
    async fn make_manager() -> Arc<crate::agent::subagent::SubagentSessionManager> {
        let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
        let wm = Arc::new(
            WorkerManager::new(
                factory,
                Arc::new(|_id: String, _code: Option<i32>| {}),
                Arc::new(
                    |_id: String,
                     _is_subagent: bool,
                     _evt: crate::agent::worker::client::WorkerInboundEvent| {},
                ),
                Arc::new(|_id: &str| None),
            )
            .with_settle_timeout(Duration::from_secs(30)),
        );
        let env = crate::agent::worker::protocol::StartEnv::from_parts(
            "parent-1".to_string(),
            "/tmp/space".to_string(),
            crate::agent::worker::protocol::StartMode::Fresh,
            None,
            test_model(),
            ModelCatalog {
                models: vec![test_model()],
                ..Default::default()
            },
            None,
            true,
            None,
            "/tmp/config".to_string(),
            true,
            None,
        );
        wm.attach("parent-1", &env)
            .await
            .expect("the parent attach completes");
        let manager = Arc::new(crate::agent::subagent::SubagentSessionManager::new());
        manager.set_worker_manager(wm);
        manager
    }

    /// `InProcessDispatcher::dispatch` on a `SubagentSessionManager` (the
    /// ADR 0025 Task 5 re-plumb — the `dispatch_native` delegation to the
    /// `WorkerManager`'s `dispatch_subagent` flow — a `fake_worker` child
    /// settles) resolves the oneshot (`Completed` — the
    /// `SubagentCapture`'s final text), and returns the `SubagentCancel`
    /// handle (its `cancel()` is idempotent — the `WorkerManager`'s
    /// `cancel_subagent` for a gone drive is a no-op).
    #[tokio::test]
    async fn in_process_dispatcher_resolves_the_oneshot() {
        let manager = make_manager().await;
        let dispatcher = InProcessDispatcher::new(manager);
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let cwd = std::env::temp_dir();
        let (rx, cancel) = dispatcher.dispatch(
            "parent-1",
            &cwd,
            &test_model(),
            Vec::new(),
            "tester".to_string(),
            LaunchConfig::default(),
            "do the thing".to_string(),
            &sink,
        );
        // The oneshot resolves `Completed` (the `fake_worker` child
        // settles — the `SubagentCapture`'s final text — the
        // `dispatch_native` re-plumb's delegation works end-to-end).
        let outcome = tokio::time::timeout(Duration::from_secs(10), rx)
            .await
            .expect("the oneshot resolved within the bound")
            .expect("the oneshot was not dropped");
        assert!(
            matches!(outcome, SubagentOutcome::Completed { .. }),
            "a settling child resolves `Completed` — got {outcome:?}"
        );
        // The `SubagentCancel` handle is returned + idempotent (the
        // `WorkerManager`'s `cancel_subagent` — a gone drive is a no-op).
        cancel.cancel();
        cancel.cancel();
    }

    /// `MockDispatcher` — the test resolves the QUEUED oneshot (the test
    /// keeps the SENDER; the `dispatch`'s receiver — the queued oneshot's
    /// receiver — gets the outcome) and the `SubagentCancel` is a no-op
    /// (idempotent — the `SubagentWait` select tests use it).
    #[tokio::test]
    async fn mock_dispatcher_resolves_the_queued_oneshot_and_cancel_works() {
        let dispatcher = MockDispatcher::new();
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let (tx, rx) = tokio::sync::oneshot::channel::<SubagentOutcome>();
        let cancel = SubagentCancel::none();
        dispatcher.queue(rx, cancel);

        let (dispatch_rx, cancel) = dispatcher.dispatch(
            "s1",
            std::path::Path::new("/tmp"),
            &test_model(),
            Vec::new(),
            "a".to_string(),
            LaunchConfig::default(),
            "t".to_string(),
            &sink,
        );
        // The test resolves the QUEUED oneshot (the `dispatch` returned
        // its receiver — the loop's `select!` sees the outcome).
        tx.send(SubagentOutcome::Failed {
            error: "canned".to_string(),
        })
        .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), dispatch_rx)
            .await
            .expect("the queued oneshot resolved")
            .expect("the oneshot was not dropped");
        match outcome {
            SubagentOutcome::Failed { error } => assert_eq!(error, "canned"),
            other => panic!("expected `Failed`, got {other:?}"),
        }
        // The `SubagentCancel` is a no-op (idempotent — never panics).
        cancel.cancel();
        cancel.cancel();
    }

    /// `MockDispatcher` with an EMPTY queue — a never-resolving oneshot
    /// + a no-op `SubagentCancel` (the `SubagentWait` select's outcome
    /// arm never fires; the turn-cancel arm wins).
    #[tokio::test]
    async fn mock_dispatcher_empty_queue_never_resolves() {
        let dispatcher = MockDispatcher::new();
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let (rx, cancel) = dispatcher.dispatch(
            "s1",
            std::path::Path::new("/tmp"),
            &test_model(),
            Vec::new(),
            "a".to_string(),
            LaunchConfig::default(),
            "t".to_string(),
            &sink,
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx)
                .await
                .is_err(),
            "the empty queue's oneshot never resolves"
        );
        cancel.cancel();
        cancel.cancel(); // idempotent, never panics
    }
}

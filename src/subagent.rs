//! The subagent session manager (ADR 0025 Task 5 — the re-plumb): a
//! subagent dispatch runs the child in a WORKER process (the
//! `WorkerManager`'s `dispatch_subagent` flow — a subagent Worker per
//! dispatch). The in-process child `AgentLoop` + the throwaway-`Db`
//! machinery are GONE (the subagent's transcript now persists as a
//! hidden ephemeral row — the Supervisor's `TranscriptPersister` applies
//! the subagent Worker's store frames with the `is_subagent` flag).
//!
//! The manager is the SUPERVISOR-side dispatcher: the
//! `InProcessDispatcher` (the `AgentLoop`'s `subagent` seam — the test
//! path) wraps `dispatch_native`, which delegates to the
//! `WorkerManager`'s `dispatch_subagent` flow (the production path is a
//! main-session Worker's `IpcDispatcher` → `Outbound::SubagentDispatch`
//! → the `WorkerManager`'s flow directly). Subagents cannot dispatch
//! subagents (the child Worker's `enabled_tools` exclude `subagent` /
//! `list_agents` — the recursion guard, the `WorkerManager`'s flow).

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};

use crate::agent::events::EventSink;
use crate::agent::harness::Model;
use crate::agent::harness::ModelKey;
use crate::agent::types::mint_session_id;
use crate::agent::worker::manager::WorkerManager;
use crate::agent::worker::protocol::SubagentDispatchWire;

/// Per-dispatch pi configuration for a subagent session (moved verbatim
/// from `launch_wrapper.rs` — the wrapper script is deleted in this task;
/// the flags are now passed directly to the `pi` spawn).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LaunchConfig {
    /// The agent file body (named agents); `None` for config-less dispatch.
    pub system_prompt: Option<String>,
    /// Resolved model ("provider/id" or "provider/id:<thinking>"); `None` = pi default.
    pub model: Option<String>,
    /// Explicit thinking level from the tool call ONLY (the frontmatter's
    /// `thinking` moved to `frontmatter_thinking` — the dispatch layers it
    /// LAST).
    pub thinking: Option<String>,
    /// The frontmatter's `thinking` (the dispatch resolves the thinking in
    /// the doc-correct order: explicit > model-key `:<level>` suffix >
    /// this — the frontmatter's `thinking` is the LAST rung). `None` for
    /// a config-less dispatch.
    pub frontmatter_thinking: Option<String>,
    /// Tool allowlist (named agents with `tools`); `None` → `--exclude-tools subagent`.
    pub tools: Option<Vec<String>>,
}

/// Manages the subagent dispatchs (ADR 0025 Task 5 — the re-plumb):
/// `dispatch_native` delegates to the `WorkerManager` (the
/// `dispatch_subagent` flow — a subagent Worker per dispatch; the
/// model / `thinking` resolution is SUPERVISOR-side, the three-way
/// `enabled_tools` rule + the `system_prompt` envelope field + the
/// `SubagentCapture` + the `settle_timeout` + the
/// `subagent-session-started` / `subagent-closed` UI lifecycle events
/// live in the flow). The `WorkerManager` is set ONCE at the
/// `SessionManager` wiring time (`set_worker_manager` — the
/// `SessionManager`'s `attach_worker_manager` / `set_subagent_manager`
/// late-wire forwards it). The subagent's `sessions` row is recorded by
/// the Supervisor's `TranscriptPersister` (the `is_subagent` flag) — the
/// manager no longer records it.
pub struct SubagentSessionManager {
    /// The `WorkerManager` (the `dispatch_native` delegate — the subagent
    /// runs in a Worker process; `None` = a manager never wired into a
    /// `SessionManager` → `dispatch_native` is an already-resolved
    /// `Failed`).
    worker_manager: std::sync::OnceLock<Arc<WorkerManager>>,
}

impl Default for SubagentSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SubagentSessionManager {
    /// Create a manager (the `WorkerManager` is threaded at the
    /// `SessionManager` wiring time — `set_worker_manager`).
    pub fn new() -> Self {
        Self {
            worker_manager: std::sync::OnceLock::new(),
        }
    }

    /// Store the `WorkerManager` (set-once — a second call is a NO-OP;
    /// the `OnceLock` is set at the `SessionManager` wiring time, so
    /// the setter takes `&self` and is callable through the `Arc`).
    pub fn set_worker_manager(&self, wm: Arc<WorkerManager>) {
        let _ = self.worker_manager.set(wm);
    }

    /// The `WorkerManager` (`None` until `set_worker_manager` — a
    /// manager that was never wired into a `SessionManager`).
    pub fn worker_manager(&self) -> Option<Arc<WorkerManager>> {
        self.worker_manager.get().cloned()
    }

    /// Spawn a WORKER for a subagent dispatch (the ADR 0025 Task 5
    /// re-plumb): delegate to the `WorkerManager`'s `dispatch_subagent`
    /// flow (a subagent Worker per dispatch — the model / `thinking`
    /// resolution is SUPERVISOR-side, the `SubagentCapture`'s final text
    /// → `Completed`, a crash → `Failed { error: "subagent worker
    /// exited unexpectedly (code N)" }`, the `settle_timeout` → the
    /// timeout outcome, a `SubagentCancel` → `send_abort` + reap). The
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
    /// `None` `WorkerManager` (a manager never wired into a
    /// `SessionManager`) → an already-resolved `Failed` (NO fallback —
    /// a native parent always has an attached `WorkerManager` in
    /// production; the "no manager" case is handled by
    /// `dispatch_subagent`'s existing `subagent: None` check).
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch_native(
        &self,
        parent_session_id: &str,
        parent_cwd: &Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        // The `WorkerManager` (the `dispatch_subagent` flow's home — a
        // `None` manager is an already-resolved `Failed`).
        let Some(wm) = self.worker_manager.get().cloned() else {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(SubagentOutcome::Failed {
                error: "native subagent dispatch is not configured".to_string(),
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
            launch,
            task,
            // The RESOLVED model key (the `provider/id` composition —
            // the flow resolves it against the parent's effective
            // catalog; a `launch.model` override wins over it).
            model_key: ModelKey::from(parent_model).to_string(),
        };
        wm.dispatch_subagent_to(parent_session_id.as_ref(), wire, child_id.clone(), tx);
        (rx, SubagentCancel::new_worker(wm, child_id))
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
    Worker {
        wm: Arc<WorkerManager>,
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
    /// `WorkerManager`'s `cancel_subagent` targets the pre-minted child
    /// session id).
    pub fn new_worker(wm: Arc<WorkerManager>, child_session_id: String) -> Self {
        SubagentCancel::Worker {
            wm,
            child_session_id,
        }
    }

    /// Cancel the dispatch. Idempotent (a second `cancel` is a no-op —
    /// the `WorkerManager`'s `cancel_subagent` is a no-op for a gone
    /// drive; the flag `send` is a no-op for a gone receiver).
    pub fn cancel(&self) {
        match self {
            SubagentCancel::Worker {
                wm,
                child_session_id,
            } => wm.cancel_subagent(child_session_id),
            SubagentCancel::Flag(tx) => {
                let _ = tx.send(true);
            }
            SubagentCancel::None => {}
        }
    }
}

/// The metrics captured for a subagent session (the accumulated
/// `cost_update` usage + the wall-clock duration). The suite self-emits a
/// per-turn `cost_update` (source `main` — `subagent-metrics-cost-push`
/// Task 1); the token/cost fields are real when the session pushed usage (0
/// when it didn't — the `CostAccumulator` default) and `duration_ms` is
/// always the wall clock (see the wire contract's metrics note).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubagentMetrics {
    /// The last message's accumulated text ("" when the turn produced no
    /// assistant text — the stale-carry-over test's EMPTY sentinel).
    pub output: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost: f64,
    pub duration_ms: u64,
}

/// The outcome of a subagent dispatch. `Serialize` / `Deserialize` /
/// `Clone` (ADR 0025 Task 2): the `SubagentResult` wire frame carries it
/// verbatim (the Supervisor → Worker resolution), and the client-side
/// enum needs both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SubagentOutcome {
    /// The task completed; `output` is the last message's accumulated text,
    /// `metrics` is the accumulated `cost_update` usage + duration.
    Completed {
        output: String,
        metrics: SubagentMetrics,
    },
    /// The task failed (or was cancelled — `error` includes `"cancelled"`).
    Failed { error: String },
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use crate::agent::events::EventSink;
    use crate::agent::harness::catalog::{Model, ModelCatalog};
    use crate::agent::subagent::{
        LaunchConfig, SubagentCancel, SubagentOutcome, SubagentSessionManager,
    };
    use crate::agent::worker::client::{WorkerError, WorkerHandle, WorkerInboundEvent};
    use crate::agent::worker::manager::{WorkerFactory, WorkerManager};
    use crate::agent::worker::protocol::{StartEnv, StartMode};

    type SinkEvents = Arc<StdMutex<Vec<(String, serde_json::Value)>>>;
    type EventLog = Arc<StdMutex<Vec<(String, bool, WorkerInboundEvent)>>>;
    type SinkMap = Arc<StdMutex<HashMap<String, Arc<CollectorSink>>>>;

    /// The `fake_worker` fixture (the `fake_mcp_stdio` convention — the
    /// `CARGO_MANIFEST_DIR`/`target/debug` path; `cargo test` builds the
    /// bin targets).
    fn fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_worker"
        ))
    }

    /// The test factory (the `WorkerFactory` seam — the `fake_worker`
    /// binary; production is `WorkerHandle::spawn(std::env::
    /// current_exe().as_path())`).
    struct FakeWorkerFactory;

    impl WorkerFactory for FakeWorkerFactory {
        fn spawn(&self) -> Result<WorkerHandle, WorkerError> {
            WorkerHandle::spawn(&fixture_path())
        }
    }

    /// A UI sink collector (the parent-scope `TauriSink` test double).
    #[derive(Clone)]
    struct CollectorSink {
        events: SinkEvents,
    }

    impl CollectorSink {
        fn new() -> (Self, SinkEvents) {
            let events = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    events: events.clone(),
                },
                events,
            )
        }
    }

    impl EventSink for CollectorSink {
        fn emit(&self, event: &str, payload: serde_json::Value) {
            self.events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((event.to_string(), payload));
        }
    }

    /// A test `Model` (a fake provider — the `fake_worker`'s
    /// `model_key` `fake/m1` resolves against the parent's catalog).
    fn test_model(id: &str, thinking_levels: Vec<String>) -> Model {
        Model {
            id: id.to_string(),
            provider: "fake".to_string(),
            base_url: "http://fake/v1".to_string(),
            api_key: "k".to_string(),
            context_window: 100_000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: !thinking_levels.is_empty(),
            thinking_levels,
            api: Some("openai-completions".to_string()),
        }
    }

    /// A parent `StartEnv` (the `dispatch_subagent` flow reads the
    /// effective `catalog` / `config_dir` / `trusted` from it — the
    /// Supervisor owns them).
    fn test_env(
        session_id: &str,
        models: Vec<Model>,
        enabled_tools: Option<Vec<String>>,
        trusted: bool,
    ) -> StartEnv {
        StartEnv::from_parts(
            session_id.to_string(),
            "/tmp/space".to_string(),
            StartMode::Fresh,
            None,
            models
                .first()
                .cloned()
                .unwrap_or_else(|| test_model("m1", Vec::new())),
            ModelCatalog {
                models,
                ..Default::default()
            },
            None,
            trusted,
            enabled_tools,
            "/tmp/config".to_string(),
            true,
            None,
        )
    }

    /// A `SubagentSessionManager` on a `WorkerManager` (the
    /// `dispatch_native` re-plumb — the subagent runs in a
    /// `fake_worker` process): the parent `p1` is `attach`ed with the
    /// given `StartEnv` (the flow's `catalog` / `trusted` source) + the
    /// parent-scope sink is registered (the
    /// `subagent-session-started` / `subagent-closed` lifecycle events).
    async fn make_manager(
        settle_timeout: Option<Duration>,
        parent_env: StartEnv,
    ) -> (
        Arc<SubagentSessionManager>,
        Arc<WorkerManager>,
        SinkEvents,
        EventLog,
    ) {
        let (sink, sink_events) = CollectorSink::new();
        let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
        let sinks: SinkMap = Arc::new(StdMutex::new(HashMap::new()));
        {
            let mut m = sinks.lock().unwrap_or_else(|p| p.into_inner());
            m.insert("p1".to_string(), Arc::new(sink));
        }
        let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
        let events_c = events.clone();
        let manager = WorkerManager::new(
            factory,
            Arc::new(|_id: String, _code: Option<i32>| {}),
            Arc::new(
                move |id: String, is_subagent: bool, evt: WorkerInboundEvent| {
                    events_c
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .push((id, is_subagent, evt));
                },
            ),
            Arc::new(move |id: &str| {
                sinks
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(id)
                    .cloned()
                    .map(|s| s as Arc<dyn EventSink>)
            }),
        );
        let manager = match settle_timeout {
            Some(t) => manager.with_settle_timeout(t),
            None => manager,
        };
        let wm = Arc::new(manager);
        wm.attach("p1", &parent_env)
            .await
            .expect("the parent attach completes");
        let manager = Arc::new(SubagentSessionManager::new());
        manager.set_worker_manager(wm.clone());
        (manager, wm, sink_events, events)
    }

    /// A `dispatch_native` call (the `SubagentSessionManager` → the
    /// `WorkerManager` flow — the subagent runs in a `fake_worker`
    /// process). The `sink` parameter is the re-plumb's no-op (the
    /// lifecycle events ride the `sink_for` lookup — the `CollectorSink`
    /// registered in `make_manager`).
    fn dispatch(
        manager: &SubagentSessionManager,
        launch: LaunchConfig,
        task: &str,
        parent_enabled_tools: Vec<String>,
        parent_model: &Model,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        let sink: Arc<dyn EventSink> = Arc::new(CollectorSink::new().0);
        manager.dispatch_native(
            "p1",
            std::path::Path::new("/tmp/space"),
            parent_model,
            parent_enabled_tools,
            "tester".to_string(),
            launch,
            task.to_string(),
            &sink,
        )
    }

    /// Bounded-await the dispatch outcome (a missing resolution is a test
    /// failure, not a hang).
    async fn outcome(rx: oneshot::Receiver<SubagentOutcome>, timeout: Duration) -> SubagentOutcome {
        tokio::time::timeout(timeout, rx)
            .await
            .expect("the dispatch must resolve (the teardown cannot hang)")
            .expect("the oneshot must not be dropped")
    }

    /// Bounded-wait until the `on_event` deliveries for `session_id`
    /// contain a frame matching `pred` (a deadline, so a missing frame
    /// is a test failure, not a hang).
    async fn wait_for_event(
        events: &EventLog,
        session_id: &str,
        pred: impl Fn(&WorkerInboundEvent) -> bool,
    ) -> Vec<(bool, WorkerInboundEvent)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let got = events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .iter()
                .filter(|(id, _, _)| id == session_id)
                .map(|(_, is_sub, evt)| (*is_sub, evt.clone()))
                .collect::<Vec<_>>();
            if got.iter().any(|(_, e)| pred(e)) || tokio::time::Instant::now() >= deadline {
                return got;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// (1) **the core (the re-plumb)**: `dispatch_native` (the
    /// `WorkerManager` flow) spawns a subagent WORKER (a
    /// `fake_worker` process — NOT an in-process `AgentLoop`), runs the
    /// task, and resolves `Completed` with the `SubagentCapture`'s final
    /// text (the `SinkFrame` `agent_message_chunk` source — the
    /// `last_message_id`'s accumulated text). The `subagent-closed`
    /// lifecycle event fires on the parent's sink.
    #[tokio::test]
    async fn dispatch_native_resolves_completed_with_the_captured_final_text() {
        let (manager, wm, sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![test_model("m1", Vec::new())], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "do the thing",
            Vec::new(),
            &test_model("m1", Vec::new()),
        );
        let outcome = outcome(dispatch_rx, Duration::from_secs(30)).await;
        match outcome {
            SubagentOutcome::Completed { output, metrics } => {
                assert_eq!(
                    output, "canned answer",
                    "the `SubagentCapture`'s final text (the `SinkFrame` `agent_message_chunk` source)"
                );
                assert!(metrics.duration_ms >= 1, "the wall clock (ms)");
            }
            other => panic!("expected `Completed`, got {other:?}"),
        }
        // The `subagent-closed` lifecycle event fired on the PARENT's
        // sink (the `subagent-session-started` / `subagent-closed` UI
        // contract — the `SubagentCapture`'s metrics ride verbatim).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let got: Vec<(String, serde_json::Value)> = loop {
            let got = sink_events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if got.iter().any(|(e, _)| e == "subagent-closed")
                || tokio::time::Instant::now() >= deadline
            {
                break got;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let started = got
            .iter()
            .find(|(e, _)| e == "subagent-session-started")
            .map(|(_, p)| p.clone())
            .expect("the `subagent-session-started` fired on the parent's sink");
        assert_eq!(started["parentSessionId"], "p1");
        assert_eq!(
            started["model"], "fake/m1",
            "the parent's model (the `model_key`)"
        );
        let closed = got
            .iter()
            .find(|(e, _)| e == "subagent-closed")
            .map(|(_, p)| p.clone())
            .expect("the `subagent-closed` fired on the parent's sink");
        assert_eq!(closed["status"], "completed");
        assert_eq!(closed["sessionId"], started["sessionId"], "the same child");
        // The subagent Worker ran (a fresh ephemeral session — its
        // frames reached the router with `is_subagent: true`).
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        assert!(
            got.iter().any(|(is_sub, _)| *is_sub),
            "a subagent Worker's frames reached the router with the flag: {got:?}"
        );
        wm.detach("p1");
    }

    /// (2) **crash**: a subagent WORKER that crashes (a `__crash__` task
    /// — the `fake_worker` exits 137) is a `Failed` with the EXIT CODE in
    /// the error (NOT `Completed` — the process exit is the crash signal),
    /// + `subagent-closed { status: "failed" }`.
    #[tokio::test]
    async fn dispatch_native_a_crashed_child_is_failed_with_the_exit_code() {
        let (manager, wm, sink_events, _events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![test_model("m1", Vec::new())], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "__crash__",
            Vec::new(),
            &test_model("m1", Vec::new()),
        );
        let outcome = outcome(dispatch_rx, Duration::from_secs(30)).await;
        match outcome {
            SubagentOutcome::Failed { error } => {
                assert_eq!(
                    error, "subagent worker exited unexpectedly (code 137)",
                    "the crash carries the EXIT CODE in the error"
                );
            }
            other => panic!("expected `Failed` (crash), got {other:?}"),
        }
        // The `subagent-closed` lifecycle event fired (EVERY exit path —
        // the crash included) with the failure.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let got: Vec<(String, serde_json::Value)> = loop {
            let got = sink_events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if got.iter().any(|(e, _)| e == "subagent-closed")
                || tokio::time::Instant::now() >= deadline
            {
                break got;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let closed = got
            .iter()
            .find(|(e, _)| e == "subagent-closed")
            .map(|(_, p)| p.clone())
            .expect("the `subagent-closed` fired (every exit path)");
        assert_eq!(closed["status"], "failed");
        assert_eq!(
            closed["error"],
            "subagent worker exited unexpectedly (code 137)"
        );
        wm.detach("p1");
    }

    /// (3) **`settle_timeout`**: a subagent WORKER whose turn HANGS (a
    /// `__slow__` task — the `fake_worker` sleeps 10 s before settling)
    /// is torn down by the flow's `settle_timeout` (the bound is SHORTER
    /// than the 10 s sleep): `Failed { "timed out" }` +
    /// `subagent-closed { status: "failed" }`, WITHIN THE BOUND.
    #[tokio::test]
    async fn dispatch_native_a_slow_child_hits_the_settle_timeout() {
        let (manager, wm, _sink_events, _events) = make_manager(
            Some(Duration::from_secs(1)),
            test_env("p1", vec![test_model("m1", Vec::new())], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "__slow__",
            Vec::new(),
            &test_model("m1", Vec::new()),
        );
        let outcome = outcome(dispatch_rx, Duration::from_secs(10)).await;
        assert!(
            matches!(
                outcome,
                SubagentOutcome::Failed { ref error } if error == "timed out"
            ),
            "a hanging child settles `Failed` (timed out) — got {outcome:?}"
        );
        wm.detach("p1");
    }

    /// (4) **`SubagentCancel`**: a parent-turn cancel (the
    /// `SubagentCancel` handle's `cancel()` — the loop's
    /// `dispatch_subagent` `turn.cancelled()` arm calls it) aborts the
    /// in-flight dispatch (the `WorkerManager`'s `cancel_subagent` — the
    /// `send_abort` + the drive's `Cancelled` outcome + the reap):
    /// `Failed { "cancelled" }` + `subagent-closed { error: "cancelled"
    /// }`, WITHIN THE BOUND (a `__slow__` child would otherwise run out
    /// the `settle_timeout` — the 30 s bound proves the CANCEL arm, not
    /// the timeout, wins).
    #[tokio::test]
    async fn dispatch_native_cancel_aborts_the_dispatch() {
        let (manager, wm, _sink_events, _events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![test_model("m1", Vec::new())], None, true),
        )
        .await;
        let (dispatch_rx, cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "__slow__",
            Vec::new(),
            &test_model("m1", Vec::new()),
        );
        // Let the child start (the `fake_worker`'s 10 s sleep is in
        // flight), then CANCEL (the `WorkerManager`'s `cancel_subagent`
        // — the `send_abort` + the drive's `cancel` flag).
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
        let outcome = outcome(dispatch_rx, Duration::from_secs(10)).await;
        assert!(
            matches!(
                outcome,
                SubagentOutcome::Failed { ref error } if error == "cancelled"
            ),
            "a cancel settles `Failed` (cancelled) — got {outcome:?}"
        );
        wm.detach("p1");
    }

    /// (5) **not configured**: a manager that never got
    /// `set_worker_manager` resolves an ALREADY-RESOLVED oneshot with
    /// `Failed { "native subagent dispatch is not configured" }` (NO
    /// fallback — a native parent always has an attached `WorkerManager`
    /// in production; the "no manager" case is handled by
    /// `dispatch_subagent`'s existing `subagent: None` check).
    #[test]
    fn dispatch_native_without_a_worker_manager_fails_immediately() {
        let manager = SubagentSessionManager::new();
        let sink: Arc<dyn EventSink> = Arc::new(CollectorSink::new().0);
        let (mut dispatch_rx, cancel) = manager.dispatch_native(
            "parent-1",
            std::path::Path::new("/tmp/space"),
            &test_model("m1", Vec::new()),
            Vec::new(),
            "tester".to_string(),
            LaunchConfig::default(),
            "do the thing".to_string(),
            &sink,
        );
        // The oneshot is ALREADY resolved (a `recv` on a resolved oneshot
        // is immediate — no `tokio` runtime needed).
        let outcome = dispatch_rx
            .try_recv()
            .expect("the oneshot is already resolved (not configured)");
        assert!(
            matches!(
                outcome,
                SubagentOutcome::Failed { ref error }
                    if error == "native subagent dispatch is not configured"
            ),
            "a `None` `WorkerManager` is an already-resolved `Failed` — got {outcome:?}"
        );
        // The `SubagentCancel` handle is returned + idempotent (a no-op
        // for a never-started dispatch).
        cancel.cancel();
        cancel.cancel();
    }

    /// (6) `set_worker_manager` (`&self`, `OnceLock`): unset in `new`;
    /// the first `set_worker_manager` stores the manager (readable via
    /// `worker_manager()`); a second `set_worker_manager` is a NO-OP
    /// (the first set wins — the `OnceLock` is set-once at wiring time).
    #[test]
    fn set_worker_manager_is_set_once_on_the_manager() {
        let manager = SubagentSessionManager::new();
        // Unset in `new` (the `WorkerManager` is threaded at the
        // `SessionManager` wiring time).
        assert!(manager.worker_manager().is_none());
        let first = Arc::new(
            WorkerManager::new(
                Arc::new(FakeWorkerFactory),
                Arc::new(|_id: String, _code: Option<i32>| {}),
                Arc::new(|_id: String, _is_subagent: bool, _evt: WorkerInboundEvent| {}),
                Arc::new(|_id: &str| None),
            )
            .with_settle_timeout(Duration::from_secs(30 * 60)),
        );
        manager.set_worker_manager(first.clone());
        // The first `set_worker_manager` stored the manager (readable
        // through the `OnceLock`).
        assert!(
            manager.worker_manager().is_some(),
            "the first set is stored"
        );
        // A second `set_worker_manager` is a NO-OP (the first set wins).
        let second = Arc::new(
            WorkerManager::new(
                Arc::new(FakeWorkerFactory),
                Arc::new(|_id: String, _code: Option<i32>| {}),
                Arc::new(|_id: String, _is_subagent: bool, _evt: WorkerInboundEvent| {}),
                Arc::new(|_id: &str| None),
            )
            .with_settle_timeout(Duration::from_secs(1)),
        );
        manager.set_worker_manager(second);
        assert!(
            Arc::ptr_eq(&manager.worker_manager().unwrap(), &first),
            "a second set is a NO-OP (the first set wins)"
        );
    }

    /// (7) `SubagentCancel` variants: `None` is a NO-OP (idempotent,
    /// never panics); `Flag` flips the watch flag (the `IpcDispatcher`'s
    /// watcher task turns the flip into the `SubagentCancel` frame — a
    /// second `cancel` is a no-op); `Worker` (a GONE session id) is a
    /// no-op (the `cancel_subagent` lookup misses).
    #[test]
    fn subagent_cancel_variants_are_well_behaved() {
        // `None` — a no-op (idempotent).
        SubagentCancel::none().cancel();
        SubagentCancel::none().cancel();
        // `Flag` — flips the watch flag (the `IpcDispatcher`'s watcher
        // task observes it); a second `cancel` is a no-op.
        let (tx, rx) = tokio::sync::watch::channel(false);
        let cancel = SubagentCancel::new_flag(tx);
        assert!(!*rx.borrow());
        cancel.cancel();
        assert!(*rx.borrow(), "the flag flipped");
        cancel.cancel();
        assert!(*rx.borrow(), "the flag stays flipped (idempotent)");
        // `Worker` — a GONE session id (the `cancel_subagent` lookup
        // misses — a no-op, never panics).
        let wm = Arc::new(WorkerManager::new(
            Arc::new(FakeWorkerFactory),
            Arc::new(|_id: String, _code: Option<i32>| {}),
            Arc::new(|_id: String, _is_subagent: bool, _evt: WorkerInboundEvent| {}),
            Arc::new(|_id: &str| None),
        ));
        let cancel = SubagentCancel::new_worker(wm, "gone-session".to_string());
        cancel.cancel();
        cancel.cancel();
    }

    /// (8) **the recursion guard (the re-plumb)**: a child's
    /// `enabledTools` (the `Start` envelope — the `fake_worker`'s
    /// `session-update` `enabledTools` echo) NEVER contains `subagent`
    /// (the recursion guard) nor `list_agents` (a child cannot dispatch,
    /// so listing dispatch targets is pointless token burn) — even when
    /// the PARENT's enabled tools carry `list_agents` (the three-way
    /// rule's inherited-parent branch — the `WorkerManager`'s flow).
    #[tokio::test]
    async fn a_child_excludes_subagent_and_list_agents_tools() {
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![test_model("m1", Vec::new())], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "do the thing",
            vec!["read".to_string(), "list_agents".to_string()],
            &test_model("m1", Vec::new()),
        );
        // AWAIT the outcome BEFORE asserting (the child's `Start` frame
        // — the advertised `enabledTools` — is emitted before the
        // settle; the `await` guarantees it is present).
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        // The child's advertised tool set (the `Start` → the
        // `fake_worker`'s `session-update` `enabledTools` frame — the
        // `is_subagent` events).
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let child_enabled = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("enabledTools"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect("the child advertised its tool set");
        assert_eq!(
            child_enabled,
            serde_json::json!(["read"]),
            "the child's `enabledTools` never contains `subagent` or `list_agents` (the three-way rule's inherited-parent branch)"
        );
        wm.detach("p1");
    }

    /// (9) **the child's system message (the re-plumb — the `system_prompt`
    /// ENVELOPE field)**: the `build_child_system_message` computation
    /// (the `launch.system_prompt` + the todo guidance line — the
    /// `has_todo_tool` derivation from the resolved child tool list) rides
    /// the `Start` envelope (the `fake_worker`'s `systemPrompt` echo —
    /// the child's seq-0 transcript row is the Supervisor's
    /// `TranscriptPersister`'s `is_subagent` row): `Some` prompt + the
    /// todo tool → the prompt + the todo line; `Some` prompt + NO todo
    /// tool → the prompt VERBATIM; `None` + the todo tool → the todo line
    /// ALONE; `None` + NO todo tool → `None` (no system message).
    #[tokio::test]
    async fn the_child_system_prompt_rides_the_start_envelope() {
        let m1 = test_model("m1", Vec::new());
        // Case 1: `Some` prompt + `launch.tools: None` + the FULL parent
        // tool set (the child HAS `manage_todo_list`) → the prompt + the
        // todo line.
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![m1.clone()], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig {
                system_prompt: Some("You are a careful reviewer.".to_string()),
                ..Default::default()
            },
            "do the thing",
            Vec::new(),
            &m1,
        );
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let system_prompt = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("systemPrompt"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect("the child's `systemPrompt` rides the `Start` envelope");
        assert_eq!(
            system_prompt,
            serde_json::json!("You are a careful reviewer.\nUse manage_todo_list to track multi-step work — write the plan before starting, mark items completed as you go"),
            "`Some` prompt + the todo tool → the prompt + the todo line"
        );
        wm.detach("p1");

        // Case 2: `Some` prompt + NO `manage_todo_list` (the child's
        // tools = the parent's minus `subagent`) → the prompt VERBATIM.
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![m1.clone()], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig {
                system_prompt: Some("You are a careful reviewer.".to_string()),
                ..Default::default()
            },
            "do the thing",
            vec!["read".to_string(), "bash".to_string()],
            &m1,
        );
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let system_prompt = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("systemPrompt"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect("the child's `systemPrompt` rides the `Start` envelope");
        assert_eq!(
            system_prompt,
            serde_json::json!("You are a careful reviewer."),
            "`Some` prompt + NO todo tool → the prompt VERBATIM"
        );
        wm.detach("p1");

        // Case 3: `None` prompt + the todo tool → the todo line ALONE.
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![m1.clone()], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "do the thing",
            Vec::new(),
            &m1,
        );
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let system_prompt = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("systemPrompt"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect("the child's `systemPrompt` rides the `Start` envelope");
        assert_eq!(
            system_prompt,
            serde_json::json!("Use manage_todo_list to track multi-step work — write the plan before starting, mark items completed as you go"),
            "`None` prompt + the todo tool → the todo line ALONE"
        );
        wm.detach("p1");

        // Case 4: `None` prompt + NO todo tool → `None` (NO system
        // message — the child's seq-0 row is absent).
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![m1.clone()], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "do the thing",
            vec!["read".to_string(), "bash".to_string()],
            &m1,
        );
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let system_prompt = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("systemPrompt"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect(
                "the `Start` envelope's `systemPrompt` field is present (the `fake_worker`'s echo)",
            );
        assert!(
            system_prompt.is_null(),
            "`None` prompt + NO todo tool → `None` (no system message): {system_prompt:?}"
        );
        wm.detach("p1");
    }

    /// (10) **trust inheritance (the re-plumb — the ADR 0010
    /// `trusted` ENVELOPE field)**: a child's `Start` envelope carries
    /// the PARENT Space's trust flag (the `WorkerManager`'s flow threads
    /// `parent_env.trusted` verbatim — the Supervisor owns the `spaces`
    /// table): a TRUSTED parent Space → `trusted: true` (the child's
    /// permission gate auto-approves mutating tools); a NON-trusted
    /// parent → `trusted: false` (fail-closed — the gate prompts).
    #[tokio::test]
    async fn a_trusted_parent_space_inherits_onto_the_child_env() {
        let m1 = test_model("m1", Vec::new());
        // A TRUSTED parent Space → the child's `Start` envelope carries
        // `trusted: true` (the child's permission gate auto-approves
        // mutating tools).
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![m1.clone()], None, true),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "do the thing",
            Vec::new(),
            &m1,
        );
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let trusted = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("trusted"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect("the child's `trusted` flag rides the `Start` envelope");
        assert_eq!(
            trusted,
            serde_json::json!(true),
            "a TRUSTED parent Space → the child's `Start` envelope carries `trusted: true`"
        );
        wm.detach("p1");

        // A NON-trusted parent Space → `trusted: false` (fail-closed).
        let (manager, wm, _sink_events, events) = make_manager(
            Some(Duration::from_secs(30)),
            test_env("p1", vec![m1.clone()], None, false),
        )
        .await;
        let (dispatch_rx, _cancel) = dispatch(
            &manager,
            LaunchConfig::default(),
            "do the thing",
            Vec::new(),
            &m1,
        );
        let _ = outcome(dispatch_rx, Duration::from_secs(30)).await;
        let got = wait_for_event(&events, "p1", |e| {
            matches!(
                e,
                WorkerInboundEvent::SinkFrame { event, .. }
                    if event == "session-update"
            )
        })
        .await;
        let trusted = got
            .iter()
            .find_map(|(is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { payload, .. } => payload
                        .get("update")
                        .and_then(|u| u.get("trusted"))
                        .cloned(),
                    _ => None,
                }
            })
            .expect("the child's `trusted` flag rides the `Start` envelope");
        assert_eq!(
            trusted,
            serde_json::json!(false),
            "a NON-trusted parent Space → the child's `Start` envelope carries `trusted: false` (fail-closed)"
        );
        wm.detach("p1");
    }
}

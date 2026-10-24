//! The subagent dispatch seam (ADR 0025): `InProcessDispatcher`
//! (delegates to `SubagentSessionManager::dispatch_native` — the current
//! behavior; after Task 5, `dispatch_native` delegates to the
//! `WorkerManager`, so this is the test path) / `IpcDispatcher` (Task 2 —
//! the Worker: `SubagentDispatch` / `SubagentResult` round-trip) /
//! `MockDispatcher` (test double — the `SubagentWait` select tests).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::harness::catalog::{Model, ModelCatalog};
use crate::agent::harness::provider::ToolCall;
use crate::agent::subagent::{
    AgentNameResolution, LaunchConfig, SubagentCancel, SubagentFailureReason, SubagentMetrics,
    SubagentOutcome, SubagentSessionManager,
};
use crate::agent::tools::{ContentBlock, ToolResult};

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
        name_resolution: AgentNameResolution,
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
        name_resolution: AgentNameResolution,
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
            name_resolution,
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
        _name_resolution: AgentNameResolution,
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

// ---------------------------------------------------------------------------
// The subagent tool-call gate + dispatch flow (moved from `loop.rs` — these
// are free functions of the values they need, so this module never depends
// on `AgentLoop`; `loop.rs` keeps a thin wrapper that supplies its fields).
// ---------------------------------------------------------------------------

/// The `enabled_tools`-filter name for `name` (the `dispatch_subagent` →
/// `subagent` alias — the model may emit either name for the same tool).
pub(crate) fn effective_tool_name(name: &str) -> &str {
    if name == "dispatch_subagent" {
        "subagent"
    } else {
        name
    }
}

/// Whether `name` is a `subagent` / `dispatch_subagent` tool call (the
/// parallel-batch detection — the `dispatch_tool` `match` routes both to
/// `dispatch_subagent`).
pub(crate) fn is_subagent_call(name: &str) -> bool {
    name == "subagent" || name == "dispatch_subagent"
}

/// Whether `name` passes the `enabled_tools` filter (`None` = all enabled).
pub(crate) fn is_tool_enabled(enabled_tools: Option<&[String]>, name: &str) -> bool {
    match enabled_tools {
        Some(tools) => tools.iter().any(|t| t == effective_tool_name(name)),
        None => true,
    }
}

/// Whether `tc` is a `subagent` call that PASSES the `enabled_tools`
/// filter (the `dispatch_tool` check, replicated — so the parallel batch
/// only dispatches ENABLED subagents; a disabled one falls through to
/// `dispatch_tool`'s "tool not enabled" error in the sequential pass).
pub(crate) fn is_enabled_subagent_call(enabled_tools: Option<&[String]>, tc: &ToolCall) -> bool {
    is_subagent_call(&tc.name) && is_tool_enabled(enabled_tools, &tc.name)
}

/// The parent-session values a subagent dispatch needs (the `AgentLoop`
/// fields the flow reads, passed explicitly so `dispatch.rs` stays free of
/// `AgentLoop`).
pub(crate) struct SubagentParent<'a> {
    pub session_id: &'a str,
    pub space_cwd: &'a std::path::Path,
    pub model: &'a Model,
    pub enabled_tools: Option<&'a [String]>,
    pub sink: &'a Arc<dyn EventSink>,
    pub catalog: &'a ModelCatalog,
    pub config_dir: Option<&'a std::path::Path>,
}

/// The `subagent` dispatch's outcome (the `SubagentOutcome` mapping).
enum SubagentWait {
    Completed {
        output: String,
    },
    /// A **Dead Subagent** (ADR 0055): the three fields the `Failed` outcome
    /// gained, plus the human `error`. `output` is the salvage the parent
    /// reads (rendered by `salvage_text` — the ONE place); `reason` /
    /// `metrics` are carried for completeness and deliberately NOT put in the
    /// parent's prompt (the Client reads them off `subagent-closed`).
    Failed {
        reason: SubagentFailureReason,
        error: String,
        output: String,
        metrics: SubagentMetrics,
    },
    Cancelled,
}

/// `subagent` dispatch: the parent's `SubagentDispatcher` (the
/// `WorkerManager`'s `dispatch_subagent` flow — the child is a WORKER
/// process; the ADR 0025 re-plumb — the in-process `AgentLoop` child
/// is GONE).
pub(crate) async fn dispatch_subagent(
    dispatcher: Option<&Arc<dyn SubagentDispatcher>>,
    parent: &SubagentParent<'_>,
    params: &Value,
    turn: &CancellationToken,
) -> ToolResult {
    let Some(dispatcher) = dispatcher else {
        return ToolResult {
            content: vec![ContentBlock::Text {
                text: "subagent dispatch is not available in this session".to_string(),
            }],
            details: None,
            is_error: true,
        };
    };
    let (task, launch) = match crate::agent::interactive::dispatch_params(params) {
        Some(p) => p,
        None => {
            return ToolResult {
                content: vec![ContentBlock::Text {
                    text: "invalid params: `task` is required".to_string(),
                }],
                details: None,
                is_error: true,
            };
        }
    };
    // The label is TRIMMED here, at the one binding that feeds BOTH consumers:
    // `resolve_launch` selects on `agent_name.trim()`, so a padded `"  scout  "`
    // is a genuine `Matched`. Handing the RAW string downstream would put a
    // label on the wire that the resolver never selected by, and the
    // `subagent-session-started` payload would print the padding while its
    // `nameResolution` described the trimmed name — a marker and a label about
    // two different strings (the UI's row shows the padding verbatim). Trimming
    // at the EMITTER instead would leave the wire raw and the payload trimmed,
    // i.e. the same disagreement one hop earlier; and `resolve_launch` keeps its
    // own `trim()` because its other callers (the Agents modal) pass raw names.
    let agent_name = params
        .get("agentName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    // ADR 0020: resolve `agentName` against the discovered Agent definitions
    // (layered — explicit params win; no match / empty = config-less, never an
    // error). (issue #47) The resolution is NOT a control-flow decision — it
    // changes nothing here — it is the payload the UI reads: `Unmatched` is a
    // name that matched no definition, i.e. a child running the parent's config
    // under a label no one wrote down.
    let (launch, name_resolution) = crate::agent::harness::launch::resolve_launch(
        &launch,
        &agent_name,
        parent.catalog,
        parent.config_dir,
        parent.space_cwd,
    );
    let (dispatch_rx, cancel) = dispatcher.dispatch(
        parent.session_id,
        parent.space_cwd,
        parent.model,
        parent.enabled_tools.map(|v| v.to_vec()).unwrap_or_default(),
        agent_name,
        name_resolution,
        launch,
        task,
        parent.sink,
    );
    let outcome = tokio::select! {
        r = dispatch_rx => match r {
            Ok(SubagentOutcome::Completed { output, .. }) => SubagentWait::Completed { output },
            Ok(SubagentOutcome::Failed {
                reason,
                error,
                output,
                metrics,
            }) => SubagentWait::Failed {
                reason,
                error,
                output,
                metrics,
            },
            // The worker task vanished without resolving (app exit) —
            // the dispatch is gone: cancel. NO outcome was in scope, so
            // there is nothing to salvage here (ADR 0055): the `Cancelled`
            // arm below stays exactly as terse as it was.
            Err(_) => SubagentWait::Cancelled,
        },
        _ = turn.cancelled() => {
            cancel.cancel();
            SubagentWait::Cancelled
        }
    };
    match outcome {
        SubagentWait::Completed { output } => ToolResult {
            content: vec![ContentBlock::Text { text: output }],
            details: None,
            is_error: false,
        },
        SubagentWait::Failed {
            reason,
            error,
            output,
            metrics,
        } => {
            // The `reason` and the child's REAL usage are NOT part of the
            // parent's prompt — the Client reads them off the `subagent-closed`
            // payload (the sole hand-off of a child's usage). They are carried
            // here so the dispatch layer has the full truth in scope.
            let _ = (reason, metrics);
            ToolResult {
                content: vec![ContentBlock::Text {
                    text: salvage_text(&error, &output),
                }],
                details: None,
                is_error: true,
            }
        }
        SubagentWait::Cancelled => ToolResult {
            content: vec![ContentBlock::Text {
                text: "cancelled".to_string(),
            }],
            details: None,
            is_error: true,
        },
    }
}

/// The parent-facing text for a **Dead Subagent** (ADR 0055) — the ONE place
/// the salvage is rendered. A reaped / crashed child's partial answer is the
/// most valuable thing the parent can get, so it follows the error instead of
/// being thrown away; the banner says plainly that the child did NOT finish,
/// so the parent neither mistakes the text for a settled answer nor re-does
/// work the child already did.
///
/// With NOTHING captured, the banner would be a lie ("its partial output
/// follows" — followed by nothing), so the bare error line is all it gets.
fn salvage_text(error: &str, output: &str) -> String {
    if output.is_empty() {
        return format!("subagent failed: {error}");
    }
    format!(
        "subagent failed: {error}\n\
         It may have been making progress; its partial output follows. Its full \
         transcript is still open in the Subagents panel.\n\
         ─── partial output ───\n\
         {output}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::events::EventSink;
    use crate::agent::harness::ModelCatalog;
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
                Arc::new(|_id: String, _code: Option<i32>, _pid: Option<u32>| {}),
                Arc::new(
                    |_id: String,
                     _is_subagent: bool,
                     _evt: crate::agent::worker::client::WorkerInboundEvent| {},
                ),
                Arc::new(|_id: &str| None),
            )
            .with_dead_bound(Duration::from_secs(30)),
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
            // A real `InProcessDispatcher` + a named Agent: the truthful value
            // for this fixture (nothing here asserts on the resolution).
            AgentNameResolution::Matched,
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

    /// (ADR 0055 — the salvage) A `Failed` outcome WITH partial output gets
    /// the banner: the parent is told the child did NOT finish, that the text
    /// below it is unfinished work, and where to read the full transcript.
    /// Before this the parent got the bare `subagent failed: <outcome>` line
    /// and 29 minutes of findings were thrown away.
    #[tokio::test]
    async fn a_failed_dispatch_with_output_includes_the_partial_output_banner() {
        let text = dispatch_failed_with_output("Finding one. Finding two.").await;
        assert!(
            text.starts_with("subagent failed: unresponsive\n"),
            "the error line still leads (the failure is never hidden), got: {text}"
        );
        assert!(
            text.contains("It may have been making progress; its partial output follows."),
            "the parent is told the text below is unfinished, got: {text}"
        );
        assert!(
            text.contains("Its full transcript is still open in the Subagents panel."),
            "the parent is pointed at the transcript, got: {text}"
        );
        assert!(
            text.contains("─── partial output ───\nFinding one. Finding two."),
            "the banner is followed by the captured text verbatim, got: {text}"
        );
    }

    /// (ADR 0055) A `Failed` outcome with NOTHING captured emits ONLY the
    /// error line. A banner promising "its partial output follows" over an
    /// empty block would be a lie the parent has to parse around, so the
    /// pre-existing terse text is exactly what a truly-empty child gets.
    #[tokio::test]
    async fn a_failed_dispatch_with_no_output_omits_the_partial_output_banner() {
        let text = dispatch_failed_with_output("").await;
        assert_eq!(
            text, "subagent failed: unresponsive",
            "no output → no banner, and no trailing separator"
        );
        assert!(
            !text.contains("partial output"),
            "the banner must not appear, got: {text}"
        );
    }

    /// The harness for both salvage tests: `dispatch_subagent` (the real flow
    /// — the params parse, the launch resolve, the `SubagentWait` `select!`)
    /// on a `MockDispatcher` resolving a `Failed` whose `output` is `output`.
    async fn dispatch_failed_with_output(output: &str) -> String {
        let dispatcher = MockDispatcher::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<SubagentOutcome>();
        dispatcher.queue(rx, SubagentCancel::none());
        tx.send(SubagentOutcome::Failed {
            reason: SubagentFailureReason::Unresponsive,
            error: "unresponsive".to_string(),
            output: output.to_string(),
            metrics: crate::agent::subagent::SubagentMetrics::default(),
        })
        .unwrap();
        let dispatcher: Arc<dyn SubagentDispatcher> = Arc::new(dispatcher);
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let catalog = ModelCatalog::default();
        let model = test_model();
        let result = dispatch_subagent(
            Some(&dispatcher),
            &SubagentParent {
                session_id: "p1",
                space_cwd: std::path::Path::new("/tmp/space"),
                model: &model,
                enabled_tools: None,
                sink: &sink,
                catalog: &catalog,
                config_dir: None,
            },
            &serde_json::json!({ "task": "the task" }),
            &CancellationToken::new(),
        )
        .await;
        assert!(
            result.is_error,
            "a `Failed` outcome is an ERROR tool result"
        );
        let ContentBlock::Text { text } = &result.content[0] else {
            panic!("expected a text block, got {:?}", result.content);
        };
        text.clone()
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
            // The honest value for a nameless test call (the `MockDispatcher`
            // drops the parameter either way).
            AgentNameResolution::NoName,
            LaunchConfig::default(),
            "t".to_string(),
            &sink,
        );
        // The test resolves the QUEUED oneshot (the `dispatch` returned
        // its receiver — the loop's `select!` sees the outcome).
        tx.send(SubagentOutcome::Failed {
            reason: crate::agent::subagent::SubagentFailureReason::Other,
            error: "canned".to_string(),
            output: String::new(),
            metrics: crate::agent::subagent::SubagentMetrics::default(),
        })
        .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), dispatch_rx)
            .await
            .expect("the queued oneshot resolved")
            .expect("the oneshot was not dropped");
        match outcome {
            SubagentOutcome::Failed { error, .. } => assert_eq!(error, "canned"),
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
            // The honest value for a nameless test call (the `MockDispatcher`
            // drops the parameter either way).
            AgentNameResolution::NoName,
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

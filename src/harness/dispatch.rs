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

use crate::events::EventSink;
use crate::harness::catalog::{Model, ModelCatalog};
use crate::harness::provider::ToolCall;
use crate::subagent::{
    AgentNameResolution, LaunchConfig, SubagentCancel, SubagentFailureReason, SubagentOutcome,
    SubagentSessionManager,
};
use crate::tools::{ContentBlock, ToolResult};

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
    /// (Fulcrum T3) The PACKAGED ADR 0066 override layer the parent loop holds
    /// (was `config_dir`, which `resolve_launch` read `settings.json` from).
    /// `None` = a layer that cannot speak; `Some(map)` = a layer that may be
    /// empty — the distinction `resolve_launch` keeps.
    pub agent_overrides:
        Option<&'a std::collections::HashMap<String, crate::config_types::AgentOverrideFields>>,
}

/// The `subagent` dispatch's outcome (the `SubagentOutcome` mapping).
///
/// BOTH arms carry the child's reported cost out of the outcome, because the
/// parent's loop owns the session total that must include it (see
/// [`dispatch_subagent`]'s return type). The figure is LOOP STATE: it is folded
/// into `AgentLoop::cost_acc` and framed as the parent's next `cost_update`. It
/// is never put in the parent's PROMPT — that half of the original ruling
/// stands, and `subagent_prompt_carries_no_money` /
/// `the_folded_figure_never_enters_the_parents_prompt` pin it.
enum SubagentWait {
    Completed {
        output: String,
        /// The child's reported total
        /// (`crate::subagent::SubagentMetrics::cost`), `None` = it never
        /// reported a figure. Folded into the parent's accumulator, never
        /// rendered into the parent's prompt.
        cost: Option<f64>,
    },
    /// A **Dead Subagent** (ADR 0055): the three fields the `Failed` outcome
    /// gained, plus the human `error`. `output` is the salvage the parent
    /// reads (rendered by `salvage_text` — the ONE place); `reason` is carried
    /// for completeness and deliberately NOT put in the parent's prompt (the
    /// Client reads it off `subagent-closed`). `cost` is the one metric the
    /// parent's LOOP needs — a child that was reaped mid-work was still billed
    /// for it, and the parent's stored total must say so.
    Failed {
        reason: SubagentFailureReason,
        error: String,
        output: String,
        cost: Option<f64>,
    },
    Cancelled,
}

/// `subagent` dispatch: the parent's `SubagentDispatcher` (the
/// `WorkerManager`'s `dispatch_subagent` flow — the child is a WORKER
/// process; the ADR 0025 re-plumb — the in-process `AgentLoop` child
/// is GONE).
///
/// Returns the `ToolResult` the parent reads AND the child's reported cost as a
/// separate value (Fulcrum T2's one-writer fix). The cost is a return value and
/// not a field of the `ToolResult` on purpose: `ToolResult::content` is the
/// text that goes into the parent's transcript and therefore into the next
/// model call's prompt, so money that rides in it is money the model reads.
/// The CALLER — the parent's loop — folds it into `cost_acc` (see
/// `AgentLoop::fold_child_cost`), which is the only thing that can produce a
/// parent total that includes its children: the Supervisor that owns the child
/// cannot see the parent's accumulator, and a frame it wrote to the parent's
/// store key would be a SECOND writer racing the parent's own frames — which is
/// exactly the defect this seam removed.
pub(crate) async fn dispatch_subagent(
    dispatcher: Option<&Arc<dyn SubagentDispatcher>>,
    parent: &SubagentParent<'_>,
    params: &Value,
    turn: &CancellationToken,
) -> (ToolResult, Option<f64>) {
    let Some(dispatcher) = dispatcher else {
        return (
            ToolResult {
                content: vec![ContentBlock::Text {
                    text: "subagent dispatch is not available in this session".to_string(),
                }],
                details: None,
                is_error: true,
            },
            // Nothing was dispatched, so nothing was spent.
            None,
        );
    };
    let (task, launch) = match crate::interactive::dispatch_params(params) {
        Some(p) => p,
        None => {
            return (
                ToolResult {
                    content: vec![ContentBlock::Text {
                        text: "invalid params: `task` is required".to_string(),
                    }],
                    details: None,
                    is_error: true,
                },
                None,
            );
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
    let (launch, name_resolution) = crate::harness::launch::resolve_launch(
        &launch,
        &agent_name,
        parent.catalog,
        parent.agent_overrides,
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
            Ok(SubagentOutcome::Completed { output, metrics }) => SubagentWait::Completed {
                output,
                cost: metrics.cost,
            },
            Ok(SubagentOutcome::Failed {
                reason,
                error,
                output,
                metrics,
            }) => SubagentWait::Failed {
                reason,
                error,
                output,
                cost: metrics.cost,
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
        SubagentWait::Completed { output, cost } => (
            ToolResult {
                content: vec![ContentBlock::Text { text: output }],
                details: None,
                is_error: false,
            },
            cost,
        ),
        SubagentWait::Failed {
            reason,
            error,
            output,
            cost,
        } => {
            // The `reason` is NOT part of the parent's prompt — the Client
            // reads it off the `subagent-closed` payload. The child's money is
            // not in the prompt either: it leaves through the SECOND half of
            // this return value, which the loop folds into its own accumulator.
            // The salvage text below stays byte-identical — a parent must never
            // see a price in the text its child produced.
            let _ = reason;
            (
                ToolResult {
                    content: vec![ContentBlock::Text {
                        text: salvage_text(&error, &output),
                    }],
                    details: None,
                    is_error: true,
                },
                cost,
            )
        }
        SubagentWait::Cancelled => (
            ToolResult {
                content: vec![ContentBlock::Text {
                    text: "cancelled".to_string(),
                }],
                details: None,
                is_error: true,
            },
            // A cancelled dispatch has no outcome to read a figure off. The
            // child's own frames still land on the CHILD's row, and the
            // Supervisor's `subagent-closed` hand-off still reports its usage —
            // the parent simply has nothing to fold.
            None,
        ),
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
    use crate::events::EventSink;
    use crate::harness::ModelCatalog;
    use crate::subagent::SubagentOutcome;
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
    /// Returns the `ToolResult` only — these two pins read the salvage TEXT;
    /// the money half of the return value is pinned by
    /// `a_failed_outcome_carries_the_childs_cost_out_of_the_select`.
    async fn dispatch_failed_with_output(output: &str) -> String {
        let dispatcher = MockDispatcher::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<SubagentOutcome>();
        dispatcher.queue(rx, SubagentCancel::none());
        tx.send(SubagentOutcome::Failed {
            reason: SubagentFailureReason::Unresponsive,
            error: "unresponsive".to_string(),
            output: output.to_string(),
            metrics: crate::subagent::SubagentMetrics::default(),
        })
        .unwrap();
        let dispatcher: Arc<dyn SubagentDispatcher> = Arc::new(dispatcher);
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let catalog = ModelCatalog::default();
        let model = test_model();
        let (result, _cost) = dispatch_subagent(
            Some(&dispatcher),
            &SubagentParent {
                session_id: "p1",
                space_cwd: std::path::Path::new("/tmp/space"),
                model: &model,
                enabled_tools: None,
                sink: &sink,
                catalog: &catalog,
                agent_overrides: None,
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

    /// The `reason` and the child's REAL usage are NOT part of the
    /// parent's prompt — the Client reads them off the `subagent-closed`
    /// payload (the sole hand-off of a child's usage). They are carried
    /// here so the dispatch layer has the full truth in scope.
    #[tokio::test]
    async fn a_failed_outcome_carries_the_childs_cost_out_of_the_select() {
        let (result, cost) = dispatch_failed_with_metrics(0.00001243).await;
        assert_eq!(
            cost,
            Some(0.00001243),
            "the child's figure leaves the dispatch as a VALUE — the parent's \
             loop is the only thing that can add it to a total that also spans \
             the parent's own calls"
        );
        let ContentBlock::Text { text } = &result.content[0] else {
            panic!("expected a text block")
        };
        assert!(
            !text.contains("0.00001243"),
            "the figure is loop state, never prompt content: {text}"
        );
    }

    /// The harness for the money-carrying pins: the same `MockDispatcher`
    /// `Failed` flow as `dispatch_failed_with_output`, but with a REAL
    /// `metrics.cost` and returning BOTH halves of the return value.
    async fn dispatch_failed_with_metrics(cost: f64) -> (ToolResult, Option<f64>) {
        let dispatcher = MockDispatcher::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<SubagentOutcome>();
        dispatcher.queue(rx, SubagentCancel::none());
        tx.send(SubagentOutcome::Failed {
            reason: SubagentFailureReason::Unresponsive,
            error: "unresponsive".to_string(),
            output: "partial".to_string(),
            metrics: crate::subagent::SubagentMetrics {
                output: "partial".to_string(),
                input_tokens: 10,
                output_tokens: 1,
                cost: Some(cost),
                duration_ms: 10,
            },
        })
        .unwrap();
        let dispatcher: Arc<dyn SubagentDispatcher> = Arc::new(dispatcher);
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let catalog = ModelCatalog::default();
        let model = test_model();
        dispatch_subagent(
            Some(&dispatcher),
            &SubagentParent {
                session_id: "p1",
                space_cwd: std::path::Path::new("/tmp/space"),
                model: &model,
                enabled_tools: None,
                sink: &sink,
                catalog: &catalog,
                agent_overrides: None,
            },
            &serde_json::json!({ "task": "the task" }),
            &CancellationToken::new(),
        )
        .await
    }

    /// A `Completed` child's figure crosses the seam too — and the parent's
    /// prompt stays exactly the child's output.
    ///
    /// The `Completed` arm is the shape the money-loss defect lived in: it used
    /// to destructure `{ output, .. }` and DROP the metrics, which is what made
    /// a parent-owned total structurally unable to include children. This is
    /// the pin that says the metrics are read now, and the pin that says only
    /// the COST is read — the output stays byte-identical.
    #[tokio::test]
    async fn a_completed_outcome_carries_the_childs_cost_and_nothing_else() {
        let dispatcher = MockDispatcher::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<SubagentOutcome>();
        dispatcher.queue(rx, SubagentCancel::none());
        tx.send(SubagentOutcome::Completed {
            output: "the answer".to_string(),
            metrics: crate::subagent::SubagentMetrics {
                output: "the answer".to_string(),
                input_tokens: 10,
                output_tokens: 5,
                cost: Some(0.002),
                duration_ms: 1,
            },
        })
        .unwrap();
        let dispatcher: Arc<dyn SubagentDispatcher> = Arc::new(dispatcher);
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let catalog = ModelCatalog::default();
        let model = test_model();
        let (result, cost) = dispatch_subagent(
            Some(&dispatcher),
            &SubagentParent {
                session_id: "p1",
                space_cwd: std::path::Path::new("/tmp/space"),
                model: &model,
                enabled_tools: None,
                sink: &sink,
                catalog: &catalog,
                agent_overrides: None,
            },
            &serde_json::json!({ "task": "the task" }),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(cost, Some(0.002), "the child's figure is returned");
        assert!(!result.is_error, "a `Completed` child is a success");
        let ContentBlock::Text { text } = &result.content[0] else {
            panic!("expected a text block")
        };
        assert_eq!(
            text, "the answer",
            "the parent's prompt is the child's output and NOTHING else — no \
             cost, no token counts, no duration"
        );
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
            reason: crate::subagent::SubagentFailureReason::Other,
            error: "canned".to_string(),
            output: String::new(),
            metrics: crate::subagent::SubagentMetrics::default(),
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

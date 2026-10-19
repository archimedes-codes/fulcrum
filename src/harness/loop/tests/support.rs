use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::stream::StreamExt;
use serde_json::Value;
use tokio::sync::{mpsc, watch, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;

use super::super::*;
use crate::agent::events::{EventSink, RpcEvent};
use crate::agent::harness::catalog::{CompactionConfig, Model, ModelCatalog};
use crate::agent::harness::dispatch::SubagentDispatcher;
use crate::agent::harness::provider::{
    FinishReason, ModelRequest, Provider, ProviderError, ProviderEvent,
};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::harness::store::SessionStore;
use crate::agent::interactive::SudoRunner;
use crate::storage::Db;

/// Dev machines see this whole suite in ~10 s on 8 cores, so the bare 5 s
/// bounds these waits used were tuned against margins they never came
/// close to spending. The shared 4-core CI runners starve SPAWNED tasks
/// long enough to lose that margin: three DIFFERENT pre-existing timing
/// tests tripped across three consecutive runs — the macOS `worker_e2e`
/// round-trip (run 38739902831), then `a_stop_mid_mcp_call_cancels` on
/// Windows (run 38750380996) alongside the macOS mcp http stream test
/// (run 38750373114), then
/// `a_cancel_stops_the_remaining_tool_calls_in_a_batch` +
/// `a_compact_control_command_compacts_when_idle` on 1c5a0e6's runs.
///
/// This is NOT a weakened assertion. Every wait still fails LOUDLY — its
/// own `.expect(...)` / `panic!(...)` names the invariant it protects — it
/// just gets 15 s of patience instead of 5, so a real hang still fails and
/// CI only stops reporting "merely slow". The durations that ARE the thing
/// under test (retry backoffs, provider stub delays, settle beats, read
/// timeouts) stay exactly where they were tuned.
pub(super) const SETTLE: Duration = Duration::from_secs(15);
/// `SETTLE` as a count of milliseconds, for `wait_for_event`'s
/// `timeout_ms` parameter.
pub(super) const SETTLE_MS: u64 = 15_000;
/// The bound for waits that cover SPAWNING A REAL PROCESS (the MCP
/// server child, the `fake_worker` subagent children) before any of the
/// test's own logic runs. `SETTLE` covers in-process scheduling; it is
/// NOT enough here: `a_stop_mid_mcp_call_cancels` lost its 15 s wait on
/// a windows-latest runner that needed 181 s for the whole suite
/// (d38db13's push run 38756887012 — while the SAME commit passed the
/// PR run 38756898713 on a quieter runner). Windows Defender scans every
/// spawned exe, and under contention that dwarfs any scheduling margin.
/// A hung child still fails the test — at 60 s instead of 20, and only
/// in the runs where it was going to fail anyway.
pub(super) const SPAWN_MS: u64 = 60_000;

/// A `SudoRunner` that records a call (the tests assert the
/// elevated command was / was NOT executed) and never runs it.
pub(super) struct RecordingSudoRunner {
    pub(super) ran: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl SudoRunner for RecordingSudoRunner {
    fn run(
        &self,
        _argv: Vec<String>,
        _password: String,
        _timeout: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::agent::interactive::SudoRun> + Send + 'static>,
    > {
        self.ran.store(true, Ordering::SeqCst);
        Box::pin(async { unreachable!("the recording runner never completes a run") })
    }
}

/// A scripted `Provider` (canned `ProviderEvent`s per `complete` call
/// — out of range repeats the LAST script; `None` = a HANGING call
/// (`complete` never resolves — the cancel tests). `calls` counts the
/// invocations (the tests assert the turn did not loop again).
pub(super) struct ScriptedProvider {
    pub(super) calls: Arc<AtomicU32>,
    pub(super) scripts: Vec<Option<Vec<ProviderEvent>>>,
}

impl ScriptedProvider {
    pub(super) fn new(scripts: Vec<Option<Vec<ProviderEvent>>>) -> (Self, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        (
            Self {
                calls: calls.clone(),
                scripts,
            },
            calls,
        )
    }
}

#[async_trait::async_trait]
impl Provider for ScriptedProvider {
    async fn complete(
        &self,
        _req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        let i = self.calls.fetch_add(1, Ordering::SeqCst) as usize;
        let entry = if i < self.scripts.len() {
            &self.scripts[i]
        } else {
            self.scripts.last().expect("at least one script")
        };
        match entry {
            Some(events) => Ok(futures_util::stream::iter(events.clone()).boxed()),
            None => futures_util::future::pending().await,
        }
    }
}

/// A `Provider` whose `complete` returns an error (the finding 7 error-
/// exit test — a non-retryable `Fatal` error ends the turn immediately;
/// the turn settles `turn_end` + `agent_settled`).
pub(super) struct FailingProvider {
    pub(super) error: ProviderError,
}

#[async_trait::async_trait]
impl Provider for FailingProvider {
    async fn complete(
        &self,
        _req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        Err(self.error.clone())
    }
}

/// A `Provider` that plays canned `ProviderEvent`s per `complete` call
/// (`scripts[i]` for the i-th call, out of range repeats the last), EXCEPT
/// that the calls numbered in `overflow_on` fail PRE-STREAM with a
/// `ContextOverflow` — the exact value `transport::map_status` hands back
/// for a context-length 400 (ADR 0045). `calls` counts EVERY invocation,
/// including the compaction summary's, which is how the give-up test knows
/// no doomed request was spent.
pub(super) struct OverflowingProvider {
    pub(super) calls: Arc<AtomicU32>,
    pub(super) overflow_on: Vec<u32>,
    pub(super) scripts: Vec<Vec<ProviderEvent>>,
}

impl OverflowingProvider {
    pub(super) fn new(
        overflow_on: &[u32],
        scripts: Vec<Vec<ProviderEvent>>,
    ) -> (Self, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        (
            Self {
                calls: calls.clone(),
                overflow_on: overflow_on.to_vec(),
                scripts,
            },
            calls,
        )
    }
}

#[async_trait::async_trait]
impl Provider for OverflowingProvider {
    async fn complete(
        &self,
        _req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        let i = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.overflow_on.contains(&i) {
            return Err(ProviderError::ContextOverflow(
                "status 400: {\"error\":{\"message\":\"prompt is too long: 213462 tokens > 200000 maximum\"}}"
                    .to_string(),
            ));
        }
        let script = self
            .scripts
            .get(i as usize)
            .or_else(|| self.scripts.last())
            .cloned()
            .unwrap_or_default();
        Ok(futures_util::stream::iter(script).boxed())
    }
}

/// A `Provider` that records `req.session_id.clone()` into an `Arc<StdMutex<Vec<Option<String>>>>`
/// and returns canned responses (or `text_then_done` by default).
pub(super) struct SessionRecordingProvider {
    pub(super) recorded_session_ids: Arc<StdMutex<Vec<Option<String>>>>,
    pub(super) scripts: Vec<Vec<ProviderEvent>>,
    pub(super) call_idx: AtomicU32,
}

impl SessionRecordingProvider {
    pub(super) fn with_scripts(
        recorded_session_ids: Arc<StdMutex<Vec<Option<String>>>>,
        scripts: Vec<Vec<ProviderEvent>>,
    ) -> Self {
        Self {
            recorded_session_ids,
            scripts,
            call_idx: AtomicU32::new(0),
        }
    }
}

#[async_trait::async_trait]
impl Provider for SessionRecordingProvider {
    async fn complete(
        &self,
        req: &ModelRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError> {
        self.recorded_session_ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(req.session_id.clone());
        let idx = self.call_idx.fetch_add(1, Ordering::SeqCst) as usize;
        let events = if let Some(script) = self.scripts.get(idx) {
            script.clone()
        } else {
            vec![
                ProviderEvent::TextDelta("ok".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        };
        Ok(futures_util::stream::iter(events).boxed())
    }
}

/// A recording `EventSink` (the `session-update` frames — the tests
/// assert on the `session-update` payloads).
pub(super) struct TestSink {
    pub(super) updates: Arc<StdMutex<Vec<Value>>>,
}

/// A recording `EventSink` that keeps EVERY emission (the `ask` tests
/// assert on the `interactive-request` the flow emits, which is not a
/// `session-update`).
pub(super) struct AllEventsSink {
    pub(super) events: Arc<StdMutex<Vec<(String, Value)>>>,
}

impl EventSink for AllEventsSink {
    fn emit(&self, event: &str, payload: Value) {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((event.to_string(), payload));
    }
}

impl EventSink for TestSink {
    fn emit(&self, event: &str, payload: Value) {
        if event == "session-update" {
            if let Some(update) = payload.get("update") {
                self.updates
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(update.clone());
            }
        }
    }
}

/// Build an `AgentLoop` + its `Db` (the harness unit-test seam — a
/// temp-dir `Db`, a single `fake/m1` catalog model, a scripted
/// provider; the `events` / `turn_cancel` / `settle_tx` wiring mirrors
/// `build_native_session`). The `Db` is returned (the tests assert on
/// the STORE via `loop_.store` against the same `Db`).
pub(super) fn build_loop_with_db(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
    models: Vec<Model>,
    config_dir: Option<&std::path::Path>,
) -> (AgentLoop, Arc<Db>) {
    let sink = Arc::new(TestSink {
        updates: Arc::new(StdMutex::new(Vec::new())),
    });
    let (loop_, db) = build_loop_with_sink(
        provider,
        events,
        turn_cancel,
        settle_tx,
        retry,
        models,
        config_dir,
        sink,
    );
    (loop_, db)
}

/// `build_loop_with_db` with the sink INJECTED (the tests that assert on
/// what the loop EMITTED, not only on what it stored). The argument list is
/// `AgentLoop::new`'s own (which carries the same allow) minus the parts the
/// builder fixes for every test.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_loop_with_sink(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
    models: Vec<Model>,
    config_dir: Option<&std::path::Path>,
    sink: Arc<dyn EventSink>,
) -> (AgentLoop, Arc<Db>) {
    let dir = std::env::temp_dir().join(format!("harness-loop-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Arc::new(Db::open(&dir.join("t.db")).expect("db should open"));
    // The `native_messages` rows FK to `sessions`: record the test
    // session so transcript writes (e.g. the `run_compaction`
    // rewrite) are valid.
    db.record_session(&crate::agent::SessionInfo {
        session_id: "s1".to_string(),
        cwd: std::path::PathBuf::from("/tmp"),
        capabilities: serde_json::json!({}),
        config_options: None,
        archived: false,
        title: None,
        context_usage: None,
        is_subagent: false,
    })
    .expect("record_session");
    let model = models.first().cloned().expect("at least one model");
    let catalog = ModelCatalog {
        models,
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let (prompt_tx, prompt_rx) = mpsc::channel(8);
    let sink: Arc<dyn EventSink> = sink;
    let loop_ = AgentLoop::new(
        "s1".to_string(),
        dir,
        model,
        provider,
        catalog,
        Arc::new(SessionStore::new(db.clone())),
        events,
        CancellationToken::new(),
        turn_cancel,
        settle_tx,
        prompt_tx,
        prompt_rx,
        Arc::new(TokioMutex::new(HashMap::new())),
        Arc::new(TokioMutex::new(HashMap::new())),
        None,
        sink,
        Arc::new(crate::agent::todo::TodoStore::new()),
        None,
        SudoDeps::default(),
        retry,
        config_dir.map(|p| p.to_path_buf()),
        "t".to_string(),
    );
    (loop_, db)
}

/// Build an `AgentLoop` (drops the `Db` — the tests that need the
/// `Db` use `build_loop_with_db`).
pub(super) fn build_loop(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
) -> AgentLoop {
    build_loop_with_db(
        provider,
        events,
        turn_cancel,
        settle_tx,
        retry,
        vec![fake_model("m1")],
        None,
    )
    .0
}

/// A text-only `PromptTurn` (the test's common case — no image
/// attachments).
pub(super) fn text_prompt(text: &str) -> PromptTurn {
    PromptTurn {
        text: text.to_string(),
        images: Vec::new(),
    }
}

/// Await the first event matching `pred` (bounded — the tests must
/// not hang).
pub(super) async fn wait_for_event(
    rx: &mut mpsc::UnboundedReceiver<RpcEvent>,
    timeout_ms: u64,
    pred: impl Fn(&RpcEvent) -> bool,
) -> Result<RpcEvent, ()> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(());
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(ev)) => {
                if pred(&ev) {
                    return Ok(ev);
                }
            }
            Ok(None) | Err(_) => return Err(()),
        }
    }
}

/// The kinds of every `RpcEvent` the loop emitted, in order (the tests
/// assert on the ORDER of the turn's events, not just their presence).
pub(super) fn event_kinds(rx: &mut mpsc::UnboundedReceiver<RpcEvent>) -> Vec<String> {
    let mut kinds = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        kinds.push(ev.kind().to_string());
    }
    kinds
}

/// A sink that interleaves the `RpcEvent` stream with the `session-update`
/// frames it receives: on every emission it first drains the event channel
/// into the same log, so the log is the turn's TRUE emission order across
/// BOTH channels (the context frame bypasses the `RpcEvent` pipeline, so its
/// position relative to `turn_start` is otherwise unobservable).
pub(super) struct OrderingSink {
    pub(super) log: Arc<StdMutex<Vec<String>>>,
    pub(super) events: Arc<StdMutex<mpsc::UnboundedReceiver<RpcEvent>>>,
}

impl EventSink for OrderingSink {
    fn emit(&self, event: &str, payload: Value) {
        if let Ok(mut rx) = self.events.try_lock() {
            while let Ok(ev) = rx.try_recv() {
                self.log
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(ev.kind().to_string());
            }
        }
        let kind = payload
            .get("update")
            .and_then(|u| u["sessionUpdate"].as_str())
            .unwrap_or(event)
            .to_string();
        self.log
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(kind);
    }
}

/// `HOME` restore guard for the `list_agents_tool` tests (a scope-exit
/// `Drop` — the Task 1 `RestoreHome` pattern: the restore happens even
/// when an assertion panics mid-test).
pub(super) struct RestoreHome(pub(super) Option<std::ffi::OsString>);
impl Drop for RestoreHome {
    fn drop(&mut self) {
        match self.0.take() {
            // SAFETY: the `ENV_LOCK` is still held at drop time (this
            // guard is declared after the `_lock` guard and outlives
            // it in reverse); no other thread mutates HOME concurrently.
            Some(v) => unsafe {
                std::env::set_var("HOME", v);
            },
            // SAFETY: the `ENV_LOCK` is still held at drop time (this
            // guard is declared after the `_lock` guard and outlives
            // it in reverse); no other thread mutates HOME concurrently.
            None => unsafe {
                std::env::remove_var("HOME");
            },
        }
    }
}

/// A `Model` for a given bare id (a `fake` provider — the unit
/// tests' catalog entry; the single-model `build_loop` catalog is
/// `fake/m1`, so a frontmatter `model: fake/m2` is STALE by
/// construction — exactly the degrade case).
pub(super) fn fake_model(id: &str) -> Model {
    Model {
        id: id.to_string(),
        provider: "fake".to_string(),
        base_url: "http://fake".to_string(),
        api_key: "k".to_string(),
        context_window: 128000,
        cost_per_mtok_in: 0.0,
        cost_per_mtok_out: 0.0,
        supports_tools: true,
        supports_thinking: false,
        thinking_levels: Vec::new(),
        api: Some("openai-completions".to_string()),
    }
}

/// Build an `AgentLoop` with a `subagent` dispatcher + a MULTI-model
/// catalog (a copy of `build_loop_with_db`'s body with the `Db`
/// assertion seam dropped — the `models` vec in the `ModelCatalog`,
/// the `subagent` `AgentLoop::new` arg set to the given dispatcher,
/// the `sink` passed through instead of the fixed `TestSink`). The
/// PARENT's `model` arg is `models[0]`; the parent's `ModelCatalog`
/// gets the full `models` vec.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_loop_with_dispatcher(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
    sink: Arc<dyn EventSink>,
    subagent: Option<Arc<dyn SubagentDispatcher>>,
    models: Vec<Model>,
    config_dir: Option<&std::path::Path>,
) -> AgentLoop {
    let dir = std::env::temp_dir().join(format!("harness-loop-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = Arc::new(Db::open(&dir.join("t.db")).expect("db should open"));
    let catalog = ModelCatalog {
        models: models.clone(),
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let (prompt_tx, prompt_rx) = mpsc::channel(8);
    AgentLoop::new(
        "s1".to_string(),
        dir,
        models[0].clone(),
        provider,
        catalog,
        Arc::new(SessionStore::new(db)),
        events,
        CancellationToken::new(),
        turn_cancel,
        settle_tx,
        prompt_tx,
        prompt_rx,
        Arc::new(TokioMutex::new(HashMap::new())),
        Arc::new(TokioMutex::new(HashMap::new())),
        None,
        sink,
        Arc::new(crate::agent::todo::TodoStore::new()),
        subagent,
        SudoDeps::default(),
        retry,
        config_dir.map(|p| p.to_path_buf()),
        "t".to_string(),
    )
}

/// A recording `EventSink` (pushes EVERY event's `(name, payload)`
/// — the `TestSink` captures only `session-update` and cannot see
/// `subagent-session-started`).
pub(super) struct RecordingSink {
    pub(super) events: Arc<StdMutex<Vec<(String, Value)>>>,
}

impl EventSink for RecordingSink {
    fn emit(&self, event: &str, payload: Value) {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((event.to_string(), payload));
    }
}

/// A fresh temp dir (created, removed on drop — the boundary tests'
/// `Tmp`, local to this module).
pub(super) struct Tmp(pub(super) PathBuf);
impl Tmp {
    pub(super) fn new(tag: &str) -> Self {
        let d = std::env::temp_dir().join(format!("loop-policy-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }
    pub(super) fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

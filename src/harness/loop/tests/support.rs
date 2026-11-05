use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::stream::StreamExt;
use serde_json::Value;
use tokio::sync::{mpsc, watch, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;

use super::super::*;
use crate::events::{EventSink, RpcEvent};
use crate::harness::catalog::{CompactionConfig, Model, ModelCatalog};
use crate::harness::dispatch::SubagentDispatcher;
use crate::harness::provider::{
    FinishReason, ModelRequest, Provider, ProviderError, ProviderEvent,
};
use crate::harness::retry::RetryPolicy;
use crate::interactive::SudoRunner;
use crate::test_support::{MemoryStore, ScratchDir};

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
        Box<dyn std::future::Future<Output = crate::interactive::SudoRun> + Send + 'static>,
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
///
/// #96: the scratch dir is the FIRST tuple element, owned by the caller.
/// `AgentLoop::space_cwd` is a production `PathBuf` and stays one, so the
/// guard cannot live in the loop — it rides out beside it. Position is
/// load-bearing: destructured locals drop in REVERSE declaration order, so
/// a guard-first tuple closes the `Db` (a live SQLite file inside that dir)
/// before `remove_dir_all` runs. Wrong order is invisible on Linux
/// (unlink-while-open succeeds) and leaks on Windows.
pub(super) fn build_loop_with_db(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
    models: Vec<Model>,
    config_dir: Option<&std::path::Path>,
) -> (ScratchDir, AgentLoop, Arc<MemoryStore>) {
    let sink = Arc::new(TestSink {
        updates: Arc::new(StdMutex::new(Vec::new())),
    });
    let (space, loop_, db) = build_loop_with_sink(
        provider,
        events,
        turn_cancel,
        settle_tx,
        retry,
        models,
        config_dir,
        sink,
    );
    (space, loop_, db)
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
) -> (ScratchDir, AgentLoop, Arc<MemoryStore>) {
    let space = crate::test_support::ScratchDir::in_env_temp("harness-loop-test-", "space");
    // (T5/S5b) The fixture's store RE-POINTED from a temp-dir `Db` +
    // `SessionStore` to the crate's `MemoryStore` (the crate cannot open a
    // `Db` — no `rusqlite` in its graph). The `record_session` call that
    // followed the `Db::open` is GONE WITH THE `Db`: its whole job was to
    // satisfy the SQLite FK from `native_messages` to `sessions`, and a double
    // has no FK. That is not coverage lost — the FK, the UPSERT collapse and
    // the transaction are pinned on the DESKTOP side (`tests/harness_store.rs`,
    // `tests/harness_loop.rs`, the four `SessionStore` tests in
    // `src-tauri/src/agent/harness/store.rs`). What these ~190 loop tests assert
    // through `loop_.store` is which rows the loop handed the seam, and that is
    // exactly what `MemoryStore` answers.
    let db = Arc::new(MemoryStore::new());
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
        // The loop gets a COPY of the path and never owns the dir (#96).
        space.to_path_buf(),
        model,
        provider,
        catalog,
        db.clone(),
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
        Arc::new(crate::todo::TodoStore::new()),
        None,
        SudoDeps::default(),
        retry,
        config_dir.map(|p| p.to_path_buf()),
        "t".to_string(),
        // (T5/S5a, SEAM 2) The opener is now a REQUIRED argument (the loop
        // takes it instead of calling `crate::open` itself, which it cannot do
        // once `harness` moves). A fixture injects the NO-OP opener: a unit
        // test must never launch a real browser, and none of these tests drives
        // the MCP `auth` action — the seam's own consumption is pinned inside
        // the crate (`mcp::manager::tests::
        // the_auth_action_opens_the_browser_through_the_seam`) and the
        // desktop's wiring is pinned by `open.rs`'s two source lints.
        Arc::new(crate::mcp::NoOpenRequester),
    );
    (space, loop_, db)
}

/// `build_loop_with_db` with PACKAGED provider rows carrying the provider's
/// custom headers (issue #129, R1 — the mid-session-switch fixture).
///
/// The header-less builders cannot pin R1: they leave the loop's packaged
/// `providers` empty, so `set_model`'s re-lookup would find no row and a switch
/// test built on one could never fire the header assert. This builder therefore
/// 1. builds the provider row (`id == provider_id`) carrying `headers` — the
///    RAW template map, so the loop's own expansion through the SAME
///    `provider_auth::headers::expand_map` the production spine calls produces
///    the value the test asserts on the wire, and
/// 2. packages it through [`AgentLoop::set_desktop_config`], so
///    `AgentLoop::set_model` re-resolves from the packaged slice.
///
/// (Fulcrum T3) It used to write a scratch `settings.json` and hand the dir to
/// `build_loop_with_sink` as `config_dir`. That is exactly the disk read this
/// task removes, so the row now rides the same seam production uses
/// (`StartEnv.providers` → `set_desktop_config`); there is no settings home to
/// own any more, which is why the tuple lost its guard.
///
/// The caller builds the INITIAL provider itself (through
/// `build_provider_with_headers`) so a test can distinguish the initial
/// provider's headers from the switched one's.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_loop_with_headers_and_db(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
    models: Vec<Model>,
    provider_id: &str,
    headers: HashMap<String, String>,
    sink: Arc<dyn EventSink>,
) -> (ScratchDir, AgentLoop, Arc<MemoryStore>) {
    let (space, mut loop_, db) = build_loop_with_sink(
        provider,
        events,
        turn_cancel,
        settle_tx,
        retry,
        models,
        // No settings dir — the row arrives packaged below.
        None,
        sink,
    );
    let row = crate::config_types::ProviderConfig {
        id: provider_id.to_string(),
        name: provider_id.to_string(),
        // The row's base URL is IRRELEVANT here — the `Model` carries the
        // wire's base URL; this row exists so the header re-lookup in
        // `set_model` finds a row whose `id == model.provider`.
        base_url: "https://example.invalid/v1".to_string(),
        api_key: String::new(),
        api: "openai-completions".to_string(),
        key_url: None,
        auth_mode: "api_key".to_string(),
        // The RAW template map (the shape the settings document holds — the
        // expansion happens in `headers_for_providers`, exactly as in
        // production).
        headers: Some(headers),
        allow_insecure_http: false,
        // (issue #128) The mechanical additive grow: `ProviderConfig`
        // gained T4's `use_gcloud_token_auth`; this fixture does not use
        // the ADC flow.
        use_gcloud_token_auth: false,
        // (issue #127) The mechanical additive grow: no OIDC login on this fixture.
        oidc: None,
    };
    loop_.set_desktop_config(None, None, None, vec![row], HashMap::new());
    (space, loop_, db)
}

/// Build an `AgentLoop` (drops the `Db` — the tests that need the
/// `Db` use `build_loop_with_db`). The scratch guard is the first element
/// for the reason recorded on `build_loop_with_db`: never `.0`-project a
/// builder result to discard the guard — bind it.
pub(super) fn build_loop(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    settle_tx: watch::Sender<u64>,
    retry: RetryPolicy,
) -> (ScratchDir, AgentLoop) {
    let (space, loop_, _db) = build_loop_with_db(
        provider,
        events,
        turn_cancel,
        settle_tx,
        retry,
        vec![fake_model("m1")],
        None,
    );
    (space, loop_)
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
    agent_overrides: Option<
        &std::collections::HashMap<String, crate::config_types::AgentOverrideFields>,
    >,
) -> (ScratchDir, AgentLoop) {
    let space = crate::test_support::ScratchDir::in_env_temp("harness-loop-test-", "dispatch");
    // (T5/S5b) `MemoryStore`, as in `build_loop_with_sink` — see that
    // fixture's comment for what the swap costs and where the lost properties
    // are pinned instead. This builder used to mint a `t.db` it never returned.
    let db = Arc::new(MemoryStore::new());
    let catalog = ModelCatalog {
        models: models.clone(),
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let (prompt_tx, prompt_rx) = mpsc::channel(8);
    let mut loop_ = AgentLoop::new(
        "s1".to_string(),
        // A copy of the path — the guard below owns the dir (#96).
        space.to_path_buf(),
        models[0].clone(),
        provider,
        catalog,
        db,
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
        Arc::new(crate::todo::TodoStore::new()),
        subagent,
        SudoDeps::default(),
        retry,
        // (Fulcrum T3) No settings dir: the override layer arrives packaged.
        None,
        "t".to_string(),
        // (T5/S5a, SEAM 2) The no-op opener — see the note in
        // `build_loop_with_sink` for why a fixture injects `NoOpenRequester`.
        Arc::new(crate::mcp::NoOpenRequester),
    );
    // The PACKAGED ADR 0066 layer the parent loop hands to `resolve_launch` on
    // every dispatch (`loop/tools.rs` reads
    // `self.packaged_agent_overrides`). `None` = the layer that cannot speak,
    // which is what every other test in this file wants; `Some(map)` is the
    // override case. Only the overrides half is set — the policy/web/providers
    // halves stay inert so a dispatch test gates on the default posture.
    if let Some(map) = agent_overrides {
        loop_.set_desktop_config(None, None, Some(map.clone()), Vec::new(), HashMap::new());
    }
    (space, loop_)
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

/// A fresh temp dir (created, removed on drop — the removal belongs to the
/// shared `test_support` guard; this only fixes the BASE and the PREFIX, so
/// #96's per-prefix census keeps attributing these dirs here). The field is
/// private: nothing outside this module read it.
pub(super) struct Tmp(crate::test_support::ScratchDir);
impl Tmp {
    pub(super) fn new(tag: &str) -> Self {
        Self(crate::test_support::ScratchDir::in_env_temp(
            "loop-policy-",
            tag,
        ))
    }
    pub(super) fn path(&self) -> &Path {
        self.0.path()
    }
}

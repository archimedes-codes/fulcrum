//! The Worker's testable message-handling core (ADR 0025 Task 2): NO
//! stdio I/O — a pure message handler (one inbound message → zero or
//! more outbound frames; the store / sink frames ride the outbound
//! channel). The `run_worker` (the stdio layer) drives it.
//!
//! `Start` builds the `AgentLoop` via the `build_loop` seam
//! (injectable in tests — the DEFAULT is the production path: the
//! session initialization moved from `session.rs`'s start path: the
//! main system prompt for `Fresh`, the transcript hydration for
//! `Resume`, the `enabled_tools` VERBATIM application, the thinking
//! level).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use tokio::sync::{mpsc, watch, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::events::RpcEvent;
use crate::agent::harness::prompt::{build_main_prompt, PromptContext};
use crate::agent::harness::provider::build_provider;
use crate::agent::harness::r#loop::{AgentLoop, ControlCmd, Prompt, SudoDeps};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::harness::store::Store;
use crate::agent::harness::trust::{StaticTrustSource, TrustSource};
use crate::agent::interactive::{interactive_key, PendingInteractive, PendingSudo};
use crate::agent::permission::{permission_key, PendingPermissions, PermissionOutcome};
use crate::agent::todo::TodoStore;
use crate::agent::worker::dispatch::{IpcDispatcher, SubagentWaiters};
use crate::agent::worker::protocol::{Inbound, Outbound, StartEnv, StartMode};
use crate::agent::worker::sink::IpcEventSink;
use crate::agent::worker::store::IpcStore;

/// The handles `build_loop` returns (the core keeps the control fields —
/// the `AgentLoop`'s `prompt_tx` / `turn_cancel` / `settle_tx` fields are
/// `pub` per Task 1's visibility exception, and the events receiver is
/// consumed by `AgentLoop::new`, so the seam creates the channels, passes
/// the senders into `new`, and returns the handles).
pub struct LoopHandles {
    /// `tokio::spawn(loop_.run())` — THE crash-detection seam (the
    /// `run_worker` crash-watch task awaits it: `Err` (a panic — the
    /// panic hook already wrote the crash log) → `WorkerError` frame +
    /// `std::process::exit(1)`; a panic in a `tokio::spawn`'d task is
    /// otherwise SWALLOWED by tokio, and without this watch a panicked
    /// loop would leave the Worker running forever with a dead loop —
    /// no exit code, the session hangs instead of stalling).
    pub task: tokio::task::JoinHandle<()>,
    /// A CLONE of the sender (taken from the `pub` field before
    /// `tokio::spawn` — `AgentLoop::new` consumes the original sender
    /// AND the receiver; the core's `Prompt` handler `send`s via this
    /// clone — `Sender::send` takes `&self`, one clone suffices; the
    /// bounded channel's backpressure is the intended `send().await`
    /// stall).
    pub prompt_tx: mpsc::Sender<Prompt>,
    /// The `pump_events` consumer (moved out of the core by the
    /// `run_worker` pump task).
    pub events_rx: mpsc::UnboundedReceiver<RpcEvent>,
    /// The `Abort` handler cancels the CURRENT turn token (the loop
    /// re-arms a fresh one per prompt).
    pub turn_cancel: Arc<StdMutex<CancellationToken>>,
    /// The `Close` handler cancels this (AND `turn_cancel` — the session
    /// token alone doesn't stop a mid-model-call turn promptly).
    pub cancel: CancellationToken,
    /// The `Config` handler's `ControlCmd` queue (`set_model` /
    /// `set_thinking_level` applied when idle — the loop's `run()`
    /// consumes its own receiver).
    pub control_tx: mpsc::Sender<ControlCmd>,
}

/// The loop's control fields (moved out of `LoopHandles` by `handle` —
/// the `events_rx` + `JoinHandle` ride `take_pump` instead, because the
/// `JoinHandle` is not `Clone` and the `run_worker` tasks MOVE it).
#[derive(Clone)]
pub struct LoopControl {
    pub prompt_tx: mpsc::Sender<Prompt>,
    pub control_tx: mpsc::Sender<ControlCmd>,
    pub turn_cancel: Arc<StdMutex<CancellationToken>>,
    pub cancel: CancellationToken,
}

/// The testable core (NO stdio I/O — a pure message handler so tests
/// drive it directly): one inbound message → zero or more outbound
/// frames (the store / sink frames ride the outbound channel).
pub struct WorkerCore {
    /// Drained to stdout by `run_worker` (the `IpcStore` / `IpcEventSink`
    /// / `IpcDispatcher` write here as the loop runs).
    pub outbound: mpsc::UnboundedSender<Outbound>,
    /// Fed by `run_worker`'s stdin loop (the main loop calls
    /// `handle` directly — the sender is kept for the API contract).
    pub inbound_rx: mpsc::Receiver<Inbound>,
    /// The existing types from `permission.rs` / `interactive.rs`
    /// (`HashMap<String, oneshot::Sender<…>>` — the `PendingPermissions`
    /// / `PendingInteractive` / `PendingSudo` aliases, FRESH per Worker).
    pub(crate) pending_permissions: PendingPermissions,
    pub(crate) pending_bridge: PendingInteractive,
    pub(crate) pending_sudo: PendingSudo,
    /// The `IpcDispatcher`'s waiter map (shared — the `SubagentResult`
    /// handler resolves it).
    pub(crate) subagent_waiters: SubagentWaiters,
    /// The Worker's `StaticTrustSource` (the `start` envelope's `trusted`
    /// flag; flipped by `config { trusted }` AND by the `trust-space`
    /// permission outcome). SHARED with the `Start`-built loop (the
    /// `build_loop` seam's closure captures the same `Arc` — a `Start`
    /// sets the flag, it does NOT replace the source the loop holds).
    trust: Option<Arc<StaticTrustSource>>,
    /// The `Start`-built loop's control fields (`None` until `Start`).
    loop_control: Option<LoopControl>,
    /// The `Start`-built pump handles (the `events_rx` + the loop's
    /// `JoinHandle` — taken by the `run_worker` pump / crash-watch task
    /// spawn; a second `Start` replaces them — one loop per Worker).
    pump: Option<(
        mpsc::UnboundedReceiver<RpcEvent>,
        tokio::task::JoinHandle<()>,
    )>,
    session_id: Option<String>,
    closed: bool,
    /// The `build_loop` seam (injectable in tests — the DEFAULT is the
    /// production path).
    build_loop: Box<dyn Fn(&StartEnv) -> LoopHandles + Send>,
}

/// The `build_loop` seam's DEFAULT (the production path): the session
/// initialization moved from `session.rs`'s start path — (1) construct
/// the channels/tokens (the events `unbounded_channel`, the prompt
/// `mpsc::channel(8)`, the cancel/turn_cancel tokens, the settle watch),
/// (2) `AgentLoop::new` with the Task-1 seams (`IpcStore` / `Some`
/// `StaticTrustSource` / `IpcEventSink` / `IpcDispatcher` when
/// `subagent_enabled`, FRESH `PendingPermissions` /
/// `PendingInteractive` / `PendingSudo` maps, `SudoDeps::default` (the
/// `RealSudoRunner`), `RetryPolicy::new()`, `env.config_dir`), (3) the
/// SESSION INITIALIZATION (the `session.rs` steps, moved verbatim: the
/// main prompt for `Fresh`, the transcript hydration for `Resume`, the
/// subagent Worker's `system_prompt`, the `enabled_tools` VERBATIM
/// application — the wire field IS the harness convention — the thinking
/// level), (4) the `LoopHandles` (the `prompt_tx` / `control_tx` clones
/// taken from the `pub` fields BEFORE the `tokio::spawn`).
pub fn default_build_loop(
    outbound: mpsc::UnboundedSender<Outbound>,
    subagent_waiters: SubagentWaiters,
    pending_permissions: PendingPermissions,
    pending_bridge: PendingInteractive,
    pending_sudo: PendingSudo,
    trust: Option<Arc<StaticTrustSource>>,
) -> Box<dyn Fn(&StartEnv) -> LoopHandles + Send> {
    Box::new(move |env: &StartEnv| {
        let cwd = std::path::PathBuf::from(&env.cwd);
        // (1) The channels/tokens (the in-process construction sites —
        // the unbounded events channel per Task 1's global rule).
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (prompt_tx, prompt_rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (settle_tx, _settle_rx) = watch::channel(0u64);
        // (2) `AgentLoop::new` with the Task-1 seams, Worker-backed
        // (`build_provider(&env.model)` — the envelope's `Model` carries
        // the provider config; the CORE's maps — the `PermissionResponse`
        // / `InteractiveResponse` handlers resolve the loop's gate
        // oneshots through them — + the shared `StaticTrustSource` + the
        // `TodoStore`).
        let store: Arc<dyn Store> = Arc::new(IpcStore::new(outbound.clone()));
        let trust: Option<Arc<dyn TrustSource>> = trust.clone().map(|t| t as Arc<dyn TrustSource>);
        let sink: Arc<dyn EventSink> = Arc::new(IpcEventSink::new(outbound.clone()));
        let subagent: Option<Arc<dyn crate::agent::harness::SubagentDispatcher>> =
            env.subagent_enabled.then(|| {
                Arc::new(IpcDispatcher::new(
                    outbound.clone(),
                    subagent_waiters.clone(),
                )) as Arc<dyn crate::agent::harness::SubagentDispatcher>
            });
        let sudo = SudoDeps {
            pending_sudo: pending_sudo.clone(),
            ..SudoDeps::default()
        };
        let mut loop_ = AgentLoop::new(
            env.session_id.clone(),
            cwd.clone(),
            env.model.clone(),
            build_provider(&env.model),
            env.catalog.clone(),
            store,
            events_tx,
            cancel.clone(),
            turn_cancel.clone(),
            settle_tx,
            prompt_tx.clone(),
            prompt_rx,
            pending_permissions.clone(),
            pending_bridge.clone(),
            trust,
            sink,
            Arc::new(TodoStore::new()),
            subagent,
            sudo,
            RetryPolicy::new(),
            Some(std::path::PathBuf::from(&env.config_dir)),
        );
        // (3) The wire field IS the harness convention (`None` = all,
        // `Some(v)` = exactly `v`, `Some(vec![])` = NO tools) — applied
        // VERBATIM, NO mapping (the settings `[]` = all convention is
        // mapped at `StartEnv` CONSTRUCTION, Task 3/4; a legitimately-
        // empty child tool set must NOT re-expand into all tools — the
        // recursion guard). BEFORE the system prompt build (the
        // `advertised_specs`' `<tools>` section must match the enabled
        // set — the in-process order: `set_enabled_tools` before
        // `build_main_prompt`).
        loop_.set_enabled_tools(env.enabled_tools.clone());
        if let Some(level) = &env.thinking {
            loop_.set_thinking_level(Some(level.clone()));
        }
        // (4) The SESSION INITIALIZATION (the `session.rs` start-path
        // steps, moved verbatim).
        match (&env.mode, &env.system_prompt) {
            (StartMode::Resume, _) => {
                // A RESUME: the Supervisor rehydrated the provider
                // transcript (the `native_messages` re-read — moved from
                // harness to Supervisor) — the data arrives in the
                // envelope instead of the DB (`load_transcript` restores
                // it BEFORE the first model call; the `Compactor` is
                // re-estimated on the loaded context).
                let messages = env.transcript.clone().unwrap_or_default();
                loop_.load_transcript(messages);
            }
            (StartMode::Fresh, Some(sp)) => {
                // A subagent Worker: the Supervisor-computed
                // `build_child_system_message(launch.system_prompt,
                // has_todo_tool)` output (it needs the resolved child
                // tool list's `has_todo_tool`) — the child's seq-0 row.
                loop_.prepend_system(sp.clone());
            }
            (StartMode::Fresh, None) => {
                // The main system prompt (ADR 0017) — NEW sessions only:
                // the `cwd`, the `~/.pi/agent` roots (best-effort: no
                // home dir → no global file), the advertised tool specs
                // (the `<tools>` section matches the `tools[]` API param),
                // the discovered skills (ADR 0013).
                let agent_dir = crate::skills::home_dir()
                    .map(|h| h.join(".pi/agent"))
                    .unwrap_or_default();
                let skills = crate::skills::discover_skills(Some(&cwd));
                let specs = loop_.advertised_specs();
                let prompt = build_main_prompt(&PromptContext {
                    cwd: &cwd,
                    agent_dir: &agent_dir,
                    tools: &specs,
                    skills: &skills,
                });
                loop_.prepend_system(prompt);
            }
        }
        // (5) The handles (the `prompt_tx` / `control_tx` clones are
        // taken from the `pub` fields BEFORE the `spawn` — `AgentLoop::
        // new` consumes the original sender AND the receiver).
        let prompt_tx = loop_.prompt_tx.clone();
        let control_tx = loop_.control_tx.clone();
        LoopHandles {
            task: tokio::spawn(loop_.run()),
            prompt_tx,
            events_rx,
            turn_cancel,
            cancel,
            control_tx,
        }
    })
}

impl WorkerCore {
    /// Build the core (the `build_loop` seam defaults to the production
    /// path; the inbound channel is fed by `run_worker`'s stdin loop).
    pub fn new(outbound: mpsc::UnboundedSender<Outbound>) -> (Self, mpsc::Sender<Inbound>) {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        let subagent_waiters = Arc::new(StdMutex::new(HashMap::new()));
        // The per-Worker maps + the shared `StaticTrustSource` (created
        // BEFORE the `build_loop` seam — the seam's closure captures the
        // same instances, so the `PermissionResponse` /
        // `InteractiveResponse` handlers resolve the loop's gate
        // oneshots + the loop's `trust-space` flip lands on the source
        // the loop's gate consults).
        let pending_permissions: PendingPermissions = Arc::new(TokioMutex::new(HashMap::new()));
        let pending_bridge: PendingInteractive = Arc::new(TokioMutex::new(HashMap::new()));
        let pending_sudo: PendingSudo = Arc::new(TokioMutex::new(HashMap::new()));
        let trust = Some(Arc::new(StaticTrustSource::new(false)));
        let core = Self {
            outbound: outbound.clone(),
            inbound_rx,
            pending_permissions: pending_permissions.clone(),
            pending_bridge: pending_bridge.clone(),
            pending_sudo: pending_sudo.clone(),
            subagent_waiters: subagent_waiters.clone(),
            trust: trust.clone(),
            loop_control: None,
            pump: None,
            session_id: None,
            closed: false,
            build_loop: default_build_loop(
                outbound,
                subagent_waiters,
                pending_permissions,
                pending_bridge,
                pending_sudo,
                trust,
            ),
        };
        (core, inbound_tx)
    }

    /// Swap the `build_loop` seam (tests inject a canned-provider stub).
    pub fn with_build_loop(
        mut self,
        f: impl Fn(&StartEnv) -> LoopHandles + Send + 'static,
    ) -> Self {
        self.build_loop = Box::new(f);
        self
    }

    /// The `closed` flag (the `run_worker` loop exits 0 when set).
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// The session id (set by `Start` — the `PermissionResponse` /
    /// `InteractiveResponse` handlers compose the compound map keys with
    /// it: the wire `id` is the payload's `requestId`, the map key is
    /// `"{session_id}/{request_id}"`).
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// The Worker's `StaticTrustSource` (the `start` envelope's `trusted`
    /// flag; the `config` / `trust-space` flips land here).
    pub fn trust_source(&self) -> Option<&Arc<StaticTrustSource>> {
        self.trust.as_ref()
    }

    /// The `Start`-built loop's control fields (a clone — the test /
    /// `Close`-assertion access).
    pub fn loop_control(&self) -> Option<LoopControl> {
        self.loop_control.clone()
    }

    /// The `Start`-built pump handles (the `events_rx` + the loop's
    /// `JoinHandle` — moved out of the core; the `run_worker` spawns the
    /// pump task (the `events_rx`) + the crash-watch task (the
    /// `JoinHandle`) with them). `None` when the `Start` hasn't arrived
    /// (or the handles were already taken — one loop per Worker).
    pub fn take_pump(
        &mut self,
    ) -> Option<(
        mpsc::UnboundedReceiver<RpcEvent>,
        tokio::task::JoinHandle<()>,
    )> {
        self.pump.take()
    }

    /// One inbound message → zero or more outbound frames (the test
    /// observation point — the store / sink frames ride the outbound
    /// channel, so the return is the frames the handler emitted DIRECTLY
    /// — none today: the `Ready` is emitted by `run_worker` at startup).
    pub async fn handle(&mut self, msg: Inbound) -> Vec<Outbound> {
        match msg {
            Inbound::Start(env) => {
                self.session_id = Some(env.session_id.clone());
                // The `StaticTrustSource` flag SET (the source is shared
                // with the `Start`-built loop — a replace would orphan
                // the loop's `Arc`; the flag is the single source of
                // truth).
                if let Some(s) = &self.trust {
                    s.set(env.trusted);
                }
                let handles = (self.build_loop)(&env);
                self.loop_control = Some(LoopControl {
                    prompt_tx: handles.prompt_tx,
                    control_tx: handles.control_tx,
                    turn_cancel: handles.turn_cancel,
                    cancel: handles.cancel,
                });
                self.pump = Some((handles.events_rx, handles.task));
                Vec::new()
            }
            Inbound::Prompt { text, images } => {
                if let Some(ctl) = &self.loop_control {
                    // Backpressure = the bounded channel's natural stall.
                    if let Err(e) = ctl.prompt_tx.send(Prompt { text, images }).await {
                        eprintln!("worker: prompt send failed (the loop is gone?): {e}");
                    }
                }
                Vec::new()
            }
            Inbound::Config {
                model,
                thinking,
                trusted,
            } => {
                if let Some(ctl) = &self.loop_control {
                    if let Some(m) = model {
                        if let Err(e) = ctl.control_tx.send(ControlCmd::SetModel(m)).await {
                            eprintln!("worker: control send failed: {e}");
                        }
                    }
                    if let Some(l) = thinking {
                        if let Err(e) = ctl
                            .control_tx
                            .send(ControlCmd::SetThinkingLevel(Some(l)))
                            .await
                        {
                            eprintln!("worker: control send failed: {e}");
                        }
                    }
                }
                if let Some(t) = trusted {
                    if let Some(s) = &self.trust {
                        s.set(t);
                    }
                }
                Vec::new()
            }
            Inbound::Abort => {
                // The existing turn-cancel semantics (`NativeHandle::
                // cancel`): cancel the CURRENT turn token (the in-flight
                // turn settles `Cancelled`, the session STAYS ALIVE — the
                // loop re-arms a fresh token per prompt).
                if let Some(ctl) = &self.loop_control {
                    ctl.turn_cancel
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .cancel();
                }
                Vec::new()
            }
            Inbound::Close => {
                // The existing in-process teardown (`NativeHandle::close`)
                // cancels BOTH — the session token alone doesn't stop a
                // mid-model-call turn promptly.
                if let Some(ctl) = &self.loop_control {
                    ctl.turn_cancel
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .cancel();
                    ctl.cancel.cancel();
                }
                self.closed = true;
                Vec::new()
            }
            Inbound::PermissionResponse { id, outcome } => {
                // Resolve the `pending_permissions` oneshot (the compound
                // key — the wire `id` is the payload's `requestId`, the
                // map key is `"{session_id}/{request_id}"`; the `PermissionOutcome` verbatim — the real 3-option outcome, mirroring the existing `respond_permission` resolution).
                if let Some(sid) = &self.session_id {
                    let key = permission_key(sid, &id);
                    if let Some(tx) = self.pending_permissions.lock().await.remove(&key) {
                        let _ = tx.send(outcome.clone());
                    }
                    // The mid-session trust flip (the very next tool call
                    // auto-approves, matching today's live-lookup behavior;
                    // the Supervisor's `db.set_space_trusted` write — Task
                    // 4 — is the persistence half).
                    if let PermissionOutcome::Selected { option_id } = &outcome {
                        if option_id == "trust-space" {
                            if let Some(s) = &self.trust {
                                s.set(true);
                            }
                        }
                    }
                }
                Vec::new()
            }
            Inbound::InteractiveResponse { id, value } => {
                // Resolve `pending_bridge` / `pending_sudo` (the compound
                // key — the wire `id` is the payload's `requestId`, e.g.
                // `"{id}:confirm"`; mirroring the existing
                // `respond_interactive_request` resolution — the `result`
                // `Value` verbatim, no wrapper).
                if let Some(sid) = &self.session_id {
                    let key = interactive_key(sid, &id);
                    let mut delivered = false;
                    if let Some(tx) = self.pending_bridge.lock().await.remove(&key) {
                        delivered = tx.send(value.clone()).is_ok();
                    }
                    if !delivered {
                        if let Some(tx) = self.pending_sudo.lock().await.remove(&key) {
                            let _ = tx.send(value);
                        }
                    }
                }
                Vec::new()
            }
            Inbound::SubagentResult { id, outcome } => {
                // Resolve the `IpcDispatcher` waiter (the loop's oneshot
                // gets the outcome — the `SubagentWait` select sees it).
                let mut waiters = self
                    .subagent_waiters
                    .lock()
                    .unwrap_or_else(|p| p.into_inner());
                if let Some(tx) = waiters.remove(&id) {
                    let _ = tx.send(outcome);
                }
                Vec::new()
            }
            Inbound::Unknown { .. } => {
                // Permissive — an unknown inbound type is ignored (the
                // surface is unversioned).
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::events::RpcEvent;
    use crate::agent::harness::catalog::Model;
    use crate::agent::harness::provider::{
        FinishReason, ModelRequest, Provider, ProviderError, ProviderEvent,
    };
    use crate::agent::harness::{RetryPolicy, Store, SudoDeps};
    use crate::agent::permission::{permission_key, PendingPermissions, PermissionOutcome};
    use crate::agent::subagent::SubagentOutcome;
    use crate::agent::todo::TodoStore;
    use crate::agent::worker::protocol::StartMode;
    use futures_util::stream;
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::{mpsc, oneshot, watch};
    use tokio_util::sync::CancellationToken;

    /// A `Provider` that answers with a short canned stream (the
    /// harness test fixture — the turn settles on `Done(Stop)`).
    struct CannedProvider;

    #[async_trait::async_trait]
    impl Provider for CannedProvider {
        async fn complete(
            &self,
            _req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            Ok(stream::iter(vec![
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ])
            .boxed())
        }
    }

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

    fn test_env(
        mode: StartMode,
        transcript: Option<Vec<crate::agent::harness::provider::ChatMessage>>,
        system_prompt: Option<String>,
    ) -> StartEnv {
        StartEnv::from_parts(
            "s1".to_string(),
            "/tmp".to_string(),
            mode,
            transcript,
            test_model(),
            crate::agent::harness::ModelCatalog {
                models: vec![test_model()],
                ..Default::default()
            },
            None,
            false,
            None,
            "/tmp".to_string(),
            true,
            system_prompt,
        )
    }

    /// The `build_loop` seam stub: a REAL `AgentLoop` on the canned
    /// provider + the Worker seams (`IpcStore` / `IpcEventSink` — the
    /// store + sink frames ride the test-observed outbound channel).
    fn stub_handles(env: &StartEnv, outbound: mpsc::UnboundedSender<Outbound>) -> LoopHandles {
        let cwd = std::path::PathBuf::from(&env.cwd);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let (prompt_tx, prompt_rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (settle_tx, _settle_rx) = watch::channel(0u64);
        let store: Arc<dyn Store> = Arc::new(IpcStore::new(outbound.clone()));
        let sink: Arc<dyn crate::agent::events::EventSink> =
            Arc::new(IpcEventSink::new(outbound.clone()));
        let mut loop_ = AgentLoop::new(
            env.session_id.clone(),
            cwd,
            env.model.clone(),
            Box::new(CannedProvider),
            env.catalog.clone(),
            store,
            events_tx,
            cancel.clone(),
            turn_cancel.clone(),
            settle_tx,
            prompt_tx.clone(),
            prompt_rx,
            Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            None,
            sink,
            Arc::new(TodoStore::new()),
            None,
            SudoDeps::default(),
            RetryPolicy::new(),
            None,
        );
        // The session initialization (the seam stub mirrors the
        // `build_loop` default's init steps).
        match (&env.mode, &env.system_prompt) {
            (StartMode::Resume, _) => {
                let messages = env.transcript.clone().unwrap_or_default();
                loop_.load_transcript(messages);
            }
            (StartMode::Fresh, Some(sp)) => {
                loop_.prepend_system(sp.clone());
            }
            (StartMode::Fresh, None) => {
                loop_.prepend_system("test system prompt".to_string());
            }
        }
        loop_.set_enabled_tools(env.enabled_tools.clone());
        if let Some(level) = &env.thinking {
            loop_.set_thinking_level(Some(level.clone()));
        }
        let prompt_tx = loop_.prompt_tx.clone();
        let control_tx = loop_.control_tx.clone();
        LoopHandles {
            task: tokio::spawn(loop_.run()),
            prompt_tx,
            events_rx,
            turn_cancel,
            cancel,
            control_tx,
        }
    }

    /// A `WorkerCore` with the stub `build_loop` (the outbound channel
    /// is test-observed).
    fn stub_core() -> (
        WorkerCore,
        mpsc::UnboundedReceiver<Outbound>,
        mpsc::Sender<Inbound>,
    ) {
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let out_tx2 = out_tx.clone();
        let (core, in_tx) = WorkerCore::new(out_tx);
        let core = core.with_build_loop(move |env: &StartEnv| stub_handles(env, out_tx2.clone()));
        (core, out_rx, in_tx)
    }

    /// Drain the outbound channel (no blocking).
    fn drain(rx: &mut mpsc::UnboundedReceiver<Outbound>) -> Vec<Outbound> {
        let mut frames = Vec::new();
        while let Ok(f) = rx.try_recv() {
            frames.push(f);
        }
        frames
    }

    /// Collect outbound frames until `pred` matches (bounded — a
    /// deadline, so a missing frame is a test failure, not a hang).
    async fn collect_until(
        rx: &mut mpsc::UnboundedReceiver<Outbound>,
        pred: impl Fn(&Outbound) -> bool,
    ) -> Vec<Outbound> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
        let mut frames = Vec::new();
        while tokio::time::Instant::now() < deadline {
            while let Ok(f) = rx.try_recv() {
                frames.push(f);
                if pred(frames.last().unwrap()) {
                    return frames;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        frames
    }

    /// `Start` (the `build_loop` seam stub — a real `AgentLoop` on the
    /// canned provider) emits the loop's `RpcEvent`s via the pump AND
    /// the store frames (`TranscriptInsert` for the system-prompt seq 0
    /// + the user message, `DisplayUpsert` for the display rows); a
    /// `Prompt` reaches the loop's prompt queue (observed via the
    /// `turn_start` / `agent_settled` events — the full turn runs to
    /// completion).
    #[tokio::test]
    async fn start_builds_the_loop_and_a_prompt_runs_a_full_turn() {
        let (mut core, mut out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        // The initialization frames: the seq-0 system-prompt transcript
        // row + the `session-update` context-usage display frame (the
        // `prepend_system` side effects).
        let frames = drain(&mut out_rx);
        assert!(
            frames.iter().any(|f| {
                matches!(f, Outbound::TranscriptInsert { seq: 0, role, .. } if role == "system")
            }),
            "the system prompt lands as the seq-0 transcript row"
        );
        assert!(
            frames.iter().any(|f| {
                matches!(f, Outbound::SinkFrame { event, .. } if event == "session-update")
            }),
            "the context-usage display frame is emitted"
        );
        // The `pump_events` consumer (the `run_worker` pump task's
        // logic over the moved `events_rx`): forward each `RpcEvent` as
        // an `Outbound::Event` frame (into a second observed channel —
        // the core's outbound carries the store / sink frames).
        let (events_rx, _task) = core.take_pump().expect("the Start built the loop");
        // The pump task (the `pump_events` free function over the moved
        // receiver).
        let (pump_tx, mut pump_rx) = mpsc::unbounded_channel::<Outbound>();
        tokio::spawn(async move {
            let mut rx = events_rx;
            while let Some(event) = rx.recv().await {
                let frame = Outbound::Event { event };
                let _ = pump_tx.send(frame);
            }
        });
        // A `Prompt` reaches the loop's prompt queue (the `prompt_tx`
        // clone — the bounded channel's backpressure is the intended
        // `send().await` stall).
        core.handle(Inbound::Prompt {
            text: "hello".to_string(),
            images: vec![],
        })
        .await;
        let frames = collect_until(&mut pump_rx, |f| {
            matches!(
                f,
                Outbound::Event {
                    event: RpcEvent::agent_settled
                }
            )
        })
        .await;
        assert!(
            frames.iter().any(|f| matches!(
                f,
                Outbound::Event {
                    event: RpcEvent::turn_start
                }
            )),
            "the prompt reached the loop (a `turn_start` was emitted)"
        );
        assert!(
            frames.iter().any(|f| {
                matches!(
                    f,
                    Outbound::Event {
                        event: RpcEvent::agent_settled
                    }
                )
            }),
            "the turn ran to completion (the canned provider settles)"
        );
        // The store frames ride the core's outbound channel (the
        // `IpcStore` — the user message seq 1 + the display rows).
        let frames = drain(&mut out_rx);
        assert!(
            frames.iter().any(|f| {
                matches!(f, Outbound::TranscriptInsert { seq: 1, role, .. } if role == "user")
            }),
            "the user message lands at seq 1"
        );
        assert!(
            frames
                .iter()
                .any(|f| matches!(f, Outbound::DisplayUpsert { .. })),
            "the display rows were framed (`DisplayUpsert`)"
        );
    }

    /// `Resume` — the transcript is HYDRATED (`load_transcript` — the
    /// `session-update` re-estimate) but NOT re-persisted (no
    /// `TranscriptInsert` — the Supervisor's rows are authoritative).
    #[tokio::test]
    async fn resume_start_hydrates_the_transcript_without_re_persisting() {
        let (mut core, mut out_rx, _in_tx) = stub_core();
        let transcript = vec![
            crate::agent::harness::provider::ChatMessage {
                role: crate::agent::harness::provider::ChatRole::System,
                content: crate::agent::harness::provider::MessageContent::Text("sp".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            crate::agent::harness::provider::ChatMessage {
                role: crate::agent::harness::provider::ChatRole::User,
                content: crate::agent::harness::provider::MessageContent::Text("hi".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
        ];
        let env = test_env(StartMode::Resume, Some(transcript), None);
        core.handle(Inbound::from(&env)).await;
        let frames = drain(&mut out_rx);
        assert!(
            frames.iter().any(|f| {
                matches!(f, Outbound::SinkFrame { event, .. } if event == "session-update")
            }),
            "the `load_transcript` re-estimate re-emits the context usage"
        );
        assert!(
            !frames
                .iter()
                .any(|f| matches!(f, Outbound::TranscriptInsert { .. })),
            "a resume does NOT re-persist the transcript"
        );
    }

    /// A subagent Worker (`Fresh` + `system_prompt: Some`) — the
    /// Supervisor-computed `build_child_system_message` output is
    /// `prepend_system`ed (the child's seq-0 row).
    #[tokio::test]
    async fn subagent_worker_system_prompt_is_the_seq_zero_row() {
        let (mut core, mut out_rx, _in_tx) = stub_core();
        let env = test_env(
            StartMode::Fresh,
            None,
            Some("child system prompt".to_string()),
        );
        core.handle(Inbound::from(&env)).await;
        let frames = drain(&mut out_rx);
        let seq0 = frames
            .iter()
            .find_map(|f| match f {
                Outbound::TranscriptInsert {
                    seq: 0,
                    content_json,
                    ..
                } => Some(content_json.clone()),
                _ => None,
            })
            .expect("the child's seq-0 transcript row");
        assert!(
            seq0.contains("child system prompt"),
            "the Supervisor-computed child prompt is the seq-0 row"
        );
    }

    /// `PermissionResponse { outcome: Selected { option_id: "allow" } }`
    /// resolves a seeded `pending_permissions` oneshot with the right
    /// `PermissionOutcome` (verbatim — the real 3-option outcome) and
    /// removes the entry.
    #[tokio::test]
    async fn permission_response_resolves_the_pending_oneshot() {
        let (mut core, _out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        let (ptx, prx) = oneshot::channel::<PermissionOutcome>();
        {
            let map: PendingPermissions = core.pending_permissions.clone();
            map.lock().await.insert(permission_key("s1", "r1"), ptx);
        }
        core.handle(Inbound::PermissionResponse {
            id: "r1".to_string(),
            outcome: PermissionOutcome::Selected {
                option_id: "allow".to_string(),
            },
        })
        .await;
        let outcome = prx
            .await
            .expect("the oneshot resolved with the user's outcome");
        assert_eq!(
            outcome,
            PermissionOutcome::Selected {
                option_id: "allow".to_string()
            },
            "the outcome rides verbatim"
        );
        assert!(
            !core
                .pending_permissions
                .lock()
                .await
                .contains_key(&permission_key("s1", "r1")),
            "the entry is removed after the answer"
        );
    }

    /// A `PermissionResponse { outcome: Selected { option_id:
    /// "trust-space" } }` ALSO flips the `StaticTrustSource` (the
    /// mid-session trust flip — the very next tool call auto-approves,
    /// matching today's live-lookup behavior).
    #[tokio::test]
    async fn permission_response_trust_space_flips_the_trust_source() {
        let (mut core, _out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        let trust = core
            .trust_source()
            .expect("the Start set the trust source")
            .clone();
        assert!(
            !trust.is_trusted(std::path::Path::new("/x")),
            "untrusted before the outcome"
        );
        let (ptx, prx) = oneshot::channel::<PermissionOutcome>();
        {
            let map: PendingPermissions = core.pending_permissions.clone();
            map.lock().await.insert(permission_key("s1", "r2"), ptx);
        }
        core.handle(Inbound::PermissionResponse {
            id: "r2".to_string(),
            outcome: PermissionOutcome::Selected {
                option_id: "trust-space".to_string(),
            },
        })
        .await;
        let _ = prx.await.expect("the oneshot resolved");
        assert!(
            trust.is_trusted(std::path::Path::new("/x")),
            "the trust-space outcome flips the StaticTrustSource"
        );
    }

    /// `Config { trusted: Some(true) }` flips the `StaticTrustSource`
    /// (the `config` message's trust update).
    #[tokio::test]
    async fn config_trusted_flips_the_trust_source() {
        let (mut core, _out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        let trust = core
            .trust_source()
            .expect("the Start set the trust source")
            .clone();
        assert!(!trust.is_trusted(std::path::Path::new("/x")));
        core.handle(Inbound::Config {
            model: None,
            thinking: None,
            trusted: Some(true),
        })
        .await;
        assert!(
            trust.is_trusted(std::path::Path::new("/x")),
            "the config message flips the trust source"
        );
    }

    /// `SubagentResult` resolves a registered subagent waiter (the
    /// `IpcDispatcher`'s waiter map — the loop's oneshot gets the
    /// outcome).
    #[tokio::test]
    async fn subagent_result_resolves_the_waiter() {
        let (mut core, _out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        let (stx, srx) = oneshot::channel::<SubagentOutcome>();
        core.subagent_waiters
            .lock()
            .unwrap()
            .insert("sub1".to_string(), stx);
        core.handle(Inbound::SubagentResult {
            id: "sub1".to_string(),
            outcome: SubagentOutcome::Failed {
                error: "boom".to_string(),
            },
        })
        .await;
        let outcome = srx.await.expect("the waiter resolved with the outcome");
        assert_eq!(
            outcome,
            SubagentOutcome::Failed {
                error: "boom".to_string()
            }
        );
        assert!(
            core.subagent_waiters.lock().unwrap().is_empty(),
            "the waiter is removed after the resolution"
        );
    }

    /// `Close` sets the closed flag AND cancels BOTH the session token
    /// (`cancel`) and the current turn token (`turn_cancel` — the
    /// session token alone doesn't stop a mid-model-call turn promptly).
    #[tokio::test]
    async fn close_sets_closed_and_cancels_both_tokens() {
        let (mut core, _out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        let control = core.loop_control().expect("the Start built the loop");
        assert!(!control.cancel.is_cancelled());
        assert!(!control.turn_cancel.lock().unwrap().is_cancelled());
        core.handle(Inbound::Close).await;
        assert!(core.is_closed(), "the closed flag is set");
        assert!(
            control.cancel.is_cancelled(),
            "the session token is cancelled"
        );
        assert!(
            control.turn_cancel.lock().unwrap().is_cancelled(),
            "the turn token is cancelled"
        );
    }

    /// `Abort` cancels the CURRENT turn token only (the turn settles
    /// `Cancelled`, the session STAYS ALIVE — the `close_session`
    /// teardown is `Close`).
    #[tokio::test]
    async fn abort_cancels_the_turn_token_only() {
        let (mut core, _out_rx, _in_tx) = stub_core();
        let env = test_env(StartMode::Fresh, None, None);
        core.handle(Inbound::from(&env)).await;
        let control = core.loop_control().expect("the Start built the loop");
        core.handle(Inbound::Abort).await;
        assert!(
            control.turn_cancel.lock().unwrap().is_cancelled(),
            "the turn token is cancelled"
        );
        assert!(
            !control.cancel.is_cancelled(),
            "the session token is NOT cancelled (the session stays alive)"
        );
        assert!(!core.is_closed(), "the session is NOT closed");
    }
}

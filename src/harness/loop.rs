//! The native agent loop (native-agent-harness Task 6 — the core of
//! Phase 2): one in-process `AgentLoop` (a tokio task) per native session
//! owns the conversation's control flow: it receives prompts, runs the
//! model → tool → retry/compaction loop, emits the normalized events
//! (the SAME `RpcEvent` shapes an external session emits — the loop runs
//! them through the existing `normalize` + `persist_update` pipeline, so
//! the frontend is unchanged), and persists the provider transcript to
//! the `SessionStore` (the `native_messages` table).
//!
//! Tools are dispatched IN-PROCESS (the native `ToolRegistry`): the
//! built-ins (Task 1's `execute_tool`) + the suite tools (the frame-free
//! cores of the existing bridge handlers — `todo_apply` /
//! `sudo_run_flow` / the in-process `ask` waiter) + `subagent` (a native
//! parent spawns an IN-PROCESS native child — `dispatch_native`: an
//! in-process `AgentLoop` with the parent's model / tools minus
//! `subagent` (the recursion guard), a `CapturingSink`, a throwaway
//! `Db` — NO external `pi` process). A permission gate (the handle-free waiter —
//! `permission::native_permission_gate`) precedes every MUTATING tool
//! (`bash` / `edit` / `write`): a trusted Space is auto-approved
//! (ADR 0010), a deny is a tool-result error `"permission denied"`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;

use crate::agent::bridge::{
    bridge_key, sudo_run_flow, todo_apply, CachedPassword, PendingBridge, PendingSudo,
    RealSudoRunner, SudoRunner,
};
use crate::agent::harness::catalog::{Model, ModelCatalog};
use crate::agent::harness::compact::{split_for_compaction, Compactor};
use crate::agent::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ModelOptions, ModelRequest, Provider,
    ProviderError, ProviderEvent, ToolCall, ToolSpec, Usage,
};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::harness::store::SessionStore;
use crate::agent::permission::{native_permission_gate, PendingPermissions, PermissionOutcome};
use crate::agent::rpc::RpcEvent;
use crate::agent::session::{normalize, persist_update, EventSink, ThoughtState, TurnState};
use crate::agent::subagent::SubagentSessionManager;
use crate::agent::todo::TodoStore;
use crate::agent::tools::{execute_tool, ContentBlock, ToolCtx, ToolResult};
use crate::storage::Db;

/// The `ask` flow's cap (the suite's `timeoutMs: 300_000` — 5 min; the
/// agent's 300 s cancel deterministically wins the desktop's 330 s
/// waiter, so a 330 s client timeout would RACE the desktop's own
/// waiter).
const ASK_TIMEOUT: Duration = Duration::from_secs(300);

/// The built-ins the `ToolRegistry` gates before execution
/// (`bash` / `edit` / `write` — the mutating built-ins; `read` /
/// `find` / `grep` / `ls` are read-only and skip the gate).
const MUTATING_TOOLS: &[&str] = &["bash", "edit", "write"];

/// A prompt to the loop (the `prompt_queue` item).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    pub text: String,
}

/// A control command to the loop (the `control_queue` item — the
/// `set_config_option` native branch routes the model / thinking-level
/// changes here; the loop applies them via `set_model` /
/// `set_thinking_level` when idle).
#[derive(Debug, Clone)]
pub enum ControlCmd {
    /// Switch the model (the `Compactor` is rebuilt — the context window
    /// is per-model).
    SetModel(Model),
    /// Set the thinking level (`None` = the model's default).
    SetThinkingLevel(Option<String>),
}

/// The suite's `sudo_exec` state (the frame-free core's dependencies —
/// `runner` / `pending_sudo` / `sudo_password`, mirroring the
/// `SessionDriver` fields).
#[derive(Clone)]
pub struct SudoDeps {
    pub runner: Arc<dyn SudoRunner>,
    pub pending_sudo: PendingSudo,
    pub sudo_password: Arc<TokioMutex<HashMap<String, CachedPassword>>>,
}

impl Default for SudoDeps {
    fn default() -> Self {
        Self {
            runner: Arc::new(RealSudoRunner),
            pending_sudo: Arc::new(TokioMutex::new(HashMap::new())),
            sudo_password: Arc::new(TokioMutex::new(HashMap::new())),
        }
    }
}

/// The native agent loop (one tokio task per native session — spawned
/// via `tokio::spawn(loop.run())`).
pub struct AgentLoop {
    pub session_id: String,
    pub space_cwd: PathBuf,
    pub model: Model,
    pub provider: Box<dyn Provider>,
    pub catalog: ModelCatalog,
    pub store: SessionStore,
    /// The `RpcEvent`-shaped values (the SAME vocabulary an external
    /// session emits — the driver / tests consume it).
    pub events: mpsc::Sender<RpcEvent>,
    /// The SESSION teardown token (`close_session` — `run()` exits on it;
    /// the driver tears the session down when the loop task ends).
    pub cancel: CancellationToken,
    /// The current TURN's cancel token (`cancel_session` Stop — finding
    /// 8c): SHARED with the `NativeHandle` (the handle cancels the
    /// CURRENT turn; the loop arms a fresh token per prompt — a stale
    /// cancel does not settle the next turn, and a Stop keeps the
    /// session ALIVE, matching the external `abort`).
    turn_cancel: Arc<StdMutex<CancellationToken>>,
    /// The RELIABLE settle signal (finding 3): `emit` writes it on every
    /// `agent_settled` (a watch send is NEVER dropped — a full / slow
    /// `events` mpsc can drop the raw event, but the driver's settle
    /// watch fires regardless). The counter forces `changed()` to fire on
    /// every settle (a watch coalesces equal values).
    settle_tx: watch::Sender<u64>,
    settle_count: AtomicU64,
    prompt_tx: mpsc::Sender<Prompt>,
    prompt_queue: mpsc::Receiver<Prompt>,
    /// The control channel (the `set_config_option` native branch — the
    /// sender is `pub` so the session's `NativeHandle` can clone it; the
    /// receiver is consumed by `run()`). Created here (NOT a `new`
    /// parameter — the `new` signature is unchanged).
    pub control_tx: mpsc::Sender<ControlCmd>,
    control_queue: mpsc::Receiver<ControlCmd>,
    pub pending_permissions: PendingPermissions,
    pub pending_bridge: PendingBridge,
    /// The trust lookup source (ADR 0010 — the permission gate's
    /// `space_trusted` lookup; `None` = fail-closed: the gate prompts).
    pub trust_db: Option<Arc<Db>>,
    /// The Tauri event sink (the `permission-request` / `bridge-request`
    /// / `session-update` frames).
    pub sink: Arc<dyn EventSink>,
    pub todo_store: Arc<TodoStore>,
    /// The subagent dispatch handle (a native parent spawns an IN-PROCESS
    /// native child — `dispatch_native`; `None` when the manager is
    /// absent).
    pub subagent: Option<Arc<SubagentSessionManager>>,
    pub sudo: SudoDeps,
    retry: RetryPolicy,
    /// The enabled tools (`None` = ALL tools; `Some(v)` = exactly `v`
    /// — `Some(vec![])` = NO tools). A disabled tool is a tool-result
    /// error, NOT executed; finding 13b.
    enabled_tools: Option<Vec<String>>,
    // ── per-session internal state (owned by the loop task) ──
    thinking_level: Option<String>,
    /// The normalizer's turn state (the `messageId` counter + the
    /// tool-call partial-args buffers) — `Mutex`-wrapped so the
    /// `emit` helper takes `&self` (the model-call borrows coexist with
    /// the retry's `&mut`).
    turn_state: std::sync::Mutex<TurnState>,
    /// The display-persistence accumulators (the SAME shapes the driver
    /// task uses — `persist_update` locks them internally).
    text_acc: std::sync::Mutex<HashMap<String, String>>,
    tool_state: std::sync::Mutex<HashMap<String, Value>>,
    thought_state: std::sync::Mutex<ThoughtState>,
    /// The provider transcript (the `native_messages` content).
    messages: Vec<ChatMessage>,
    compactor: Compactor,
    /// Set by `compact()` (a forced compaction before the next model
    /// call).
    force_compact: bool,
}

impl AgentLoop {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: String,
        space_cwd: PathBuf,
        model: Model,
        provider: Box<dyn Provider>,
        catalog: ModelCatalog,
        store: SessionStore,
        events: mpsc::Sender<RpcEvent>,
        cancel: CancellationToken,
        turn_cancel: Arc<StdMutex<CancellationToken>>,
        settle_tx: watch::Sender<u64>,
        prompt_tx: mpsc::Sender<Prompt>,
        prompt_queue: mpsc::Receiver<Prompt>,
        pending_permissions: PendingPermissions,
        pending_bridge: PendingBridge,
        trust_db: Option<Arc<Db>>,
        sink: Arc<dyn EventSink>,
        todo_store: Arc<TodoStore>,
        subagent: Option<Arc<SubagentSessionManager>>,
        sudo: SudoDeps,
        retry: RetryPolicy,
    ) -> Self {
        let compactor = Compactor::new(catalog.compaction, model.context_window);
        let (control_tx, control_queue) = mpsc::channel(8);
        Self {
            session_id,
            space_cwd,
            model,
            provider,
            catalog,
            store,
            events,
            cancel,
            turn_cancel,
            settle_tx,
            settle_count: AtomicU64::new(0),
            prompt_tx,
            prompt_queue,
            control_tx,
            control_queue,
            pending_permissions,
            pending_bridge,
            trust_db,
            sink,
            todo_store,
            subagent,
            sudo,
            retry,
            enabled_tools: None,
            thinking_level: None,
            turn_state: std::sync::Mutex::new(TurnState::default()),
            text_acc: std::sync::Mutex::new(HashMap::new()),
            tool_state: std::sync::Mutex::new(HashMap::new()),
            thought_state: std::sync::Mutex::new(ThoughtState::default()),
            messages: Vec::new(),
            compactor,
            force_compact: false,
        }
    }

    /// Queue a prompt (best-effort — a full / closed queue is dropped).
    pub fn send_prompt(&self, text: &str) {
        let _ = self.prompt_tx.try_send(Prompt {
            text: text.to_string(),
        });
    }

    /// Switch the model (the `Compactor` is rebuilt — the context window
    /// is per-model).
    pub fn set_model(&mut self, model: Model) {
        self.compactor = Compactor::new(self.catalog.compaction, model.context_window);
        self.model = model;
    }

    /// Set the thinking level (the `reasoning_effort` of the model
    /// request; `None` = the model's default).
    pub fn set_thinking_level(&mut self, level: Option<String>) {
        self.thinking_level = level;
    }

    /// Set the enabled tools (`None` = ALL; `Some(v)` = exactly `v` —
    /// `Some(vec![])` = NO tools; a disabled tool is a tool-result
    /// error, NOT executed; finding 13b).
    pub fn set_enabled_tools(&mut self, tools: Option<Vec<String>>) {
        self.enabled_tools = tools;
    }

    /// The enabled tools (`None` = ALL — finding 13b; the
    /// `dispatch_native` driver derives the child's tool set from it).
    pub fn enabled_tools(&self) -> Option<&[String]> {
        self.enabled_tools.as_deref()
    }

    /// The thinking level (`None` = the model's default — the
    /// `dispatch_native` driver reads it for the `subagent-session-
    /// started` payload).
    pub fn thinking_level(&self) -> Option<&str> {
        self.thinking_level.as_deref()
    }

    /// Prepend a system message to the provider transcript (the
    /// `dispatch_native` `launch.system_prompt` seed — pushed to the
    /// FRONT of `self.messages` + the `Compactor` is re-estimated, the
    /// `load_transcript` shape; the `model_request` sends
    /// `messages: self.messages.clone()`, so it leads every model call).
    ///
    /// **Call AT MOST ONCE per session**: this PREPENDS (it does not
    /// replace) — a second call stacks another `System` message at index
    /// 0 (`[sp2, sp1, …]`). The `dispatch_native` driver applies it
    /// exactly once per child; do not call it again mid-session.
    pub fn prepend_system(&mut self, text: String) {
        self.messages.insert(
            0,
            ChatMessage {
                role: ChatRole::System,
                content: MessageContent::Text(text),
                tool_call_id: None,
                tool_calls: None,
            },
        );
        self.compactor.reestimate(&self.messages);
    }

    /// Seed the provider transcript (resume: `SessionStore::load_messages`
    /// restores the stored transcript BEFORE the first model call — the
    /// `Compactor` is re-estimated on the loaded context).
    pub fn load_transcript(&mut self, messages: Vec<ChatMessage>) {
        self.messages = messages;
        self.compactor.reestimate(&self.messages);
    }

    /// Stop the in-flight TURN (the `cancel_session` Stop — finding 8c:
    /// the turn settles `Cancelled` and the session STAYS ALIVE — a new
    /// prompt reuses it, matching the external `abort`; only a
    /// `close_session` tears the loop down).
    ///
    /// The Stop cancels the TURN token: the in-flight turn settles on its
    /// own check, and `run()`'s turn-cancel arm DRAINS the prompt queue
    /// (a queued prompt must not start a FRESH turn after the user
    /// pressed Stop — it would run to completion, a whole new streamed
    /// turn the user asked to stop). The queue is rarely used (a
    /// concurrent `send_prompt` is rejected "busy"), but a prompt enqueued
    /// before that, or a programmatic enqueue, could still be queued.
    pub fn cancel_turn(&self) {
        self.turn_cancel_token().cancel();
    }

    /// Tear the loop down (the `close_session` teardown — the prompt
    /// queue + the in-flight turn stop; the driver teardown cancels too
    /// — idempotent).
    pub fn close(&self) {
        self.cancel.cancel();
    }

    /// The current turn's cancel token (cloned — cheap; the loop arms a
    /// fresh one per prompt, `run()` re-arms a stale idle cancel).
    fn turn_cancel_token(&self) -> CancellationToken {
        self.turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Force a compaction before the next model call.
    pub fn compact(&mut self) {
        self.force_compact = true;
    }

    /// The loop body (the spawned tokio task): receive prompts (and
    /// control commands) until the session teardown or the queue closes.
    pub async fn run(mut self) {
        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            // The current turn token (bound — the `select!` polls its
            // `cancelled()` future; a temporary would be dropped at the
            // end of the statement).
            let turn_cancel = self.turn_cancel_token();
            tokio::select! {
                // The SESSION teardown (`close_session` — the loop task
                // ends; the driver tears the session down).
                _ = self.cancel.cancelled() => break,
                // A TURN cancel (the `cancel_session` Stop — finding 8c:
                // the in-flight `handle_prompt` settles the turn on its
                // own check; the session STAYS ALIVE, matching the
                // external `abort`). A stale cancel (no turn in flight)
                // is RE-ARMED here — the next prompt is a fresh turn,
                // not a cancel (and the re-arm cannot affect an in-flight
                // turn: `handle_prompt` raced its own clone of the token).
                _ = turn_cancel.cancelled() => {
                    // A TURN cancel (the `cancel_session` Stop — finding 8c):
                    // the in-flight `handle_prompt` settles the turn on its
                    // own check; the session STAYS ALIVE (matching the
                    // external `abort`). The Stop also DRAINS the prompt
                    // queue: a queued prompt must not start a FRESH turn
                    // after the user pressed Stop. A stale cancel (no turn
                    // in flight) is RE-ARMED here — the next prompt is a
                    // fresh turn, not a cancel. A DRAINED prompt had a
                    // `pending_turn` resolver (set by
                    // `send_prompt_with_images`) that was never run: settle
                    // it (`agent_settled` → the driver resolves the slot
                    // `Cancelled` via the `cancel_requested` flag) — pre-fix
                    // the drained prompt's resolver was never resolved, so
                    // its `send_prompt` caller hung until a `close_session`.
                    let mut drained = 0;
                    while self.prompt_queue.try_recv().is_ok() {
                        drained += 1;
                    }
                    *self.turn_cancel.lock().unwrap_or_else(|p| p.into_inner()) =
                        CancellationToken::new();
                    if drained > 0 {
                        self.emit(RpcEvent::agent_settled);
                    }
                }
                // A control command (the `set_config_option` native branch —
                // `AgentLoop::set_model` / `set_thinking_level` applied when
                // idle; a queued command waits for the in-flight turn to end).
                cmd = self.control_queue.recv() => match cmd {
                    Some(ControlCmd::SetModel(m)) => self.set_model(m),
                    Some(ControlCmd::SetThinkingLevel(l)) => self.set_thinking_level(l),
                    None => break,
                },
                p = self.prompt_queue.recv() => match p {
                    Some(p) => self.handle_prompt(&p.text).await,
                    None => break,
                },
            }
        }
    }

    /// Handle one prompt (the model → tool → retry/compaction loop).
    pub async fn handle_prompt(&mut self, text: &str) {
        // A Stop is pending (the turn token is cancelled — `NativeHandle::cancel`
        // cancelled it): a queued prompt must not start a FRESH turn after the
        // user pressed Stop. Drain the queue + skip this prompt (the token is
        // re-armed so a LATER prompt starts a fresh turn — the session stays
        // alive after a Stop). `run()`'s turn-cancel arm drains too: the two
        // defenses cover the `select!` race (whichever arm is polled first, the
        // queue is drained before a queued prompt starts a turn). The SKIPPED
        // prompt had a `pending_turn` resolver (set by `send_prompt_with_images`)
        // that was never run: settle it (`agent_settled` → the driver resolves
        // the slot `Cancelled` via the `cancel_requested` flag) — pre-fix this
        // early return emitted nothing, so the skipped prompt's `send_prompt`
        // caller hung until a `close_session`.
        if self.turn_cancel_token().is_cancelled() {
            while self.prompt_queue.try_recv().is_ok() {}
            *self.turn_cancel.lock().unwrap_or_else(|p| p.into_inner()) = CancellationToken::new();
            self.emit(RpcEvent::agent_settled);
            return;
        }
        // A FRESH turn token (finding 8c): a stale cancel from a
        // previous turn must not settle this turn — `run()` re-arms a
        // stale idle cancel, and the in-flight turn code races THIS
        // token (the `handle_prompt`-local clone, so `run()`'s re-arm
        // cannot race an in-flight turn).
        let turn = {
            let fresh = CancellationToken::new();
            *self.turn_cancel.lock().unwrap_or_else(|p| p.into_inner()) = fresh.clone();
            fresh
        };
        self.handle_turn(&turn, text).await;
    }

    /// The turn body (the model → tool → retry/compaction loop) — `turn`
    /// is the turn's cancel token (finding 8b: the model call, the
    /// stream, the backoff sleeps, the tool batch, the gate, and the
    /// `ask` / `subagent` / `summarize` flows all race it — a cancel
    /// stops the turn, it is never waited out).
    async fn handle_turn(&mut self, turn: &CancellationToken, text: &str) {
        // The display `user` row is written SOLELY by the manager's
        // `send_prompt_with_images` (`record_message` — the command
        // delegates to it and adds no persistence of its own): the loop
        // must NOT re-persist the user message (the `(session_id, kind,
        // message_key)` key with `message_key = NULL` treats NULLs as
        // DISTINCT in `ON CONFLICT`, so a double write would show a
        // duplicate user bubble in restored history). The provider
        // transcript is `native_messages` (the push below).
        self.messages.push(ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text(text.to_string()),
            tool_call_id: None,
            tool_calls: None,
        });
        self.persist_transcript_message();

        self.emit(RpcEvent::turn_start);

        let mut turn_tool_results: Vec<Value> = Vec::new();
        // `message_start` ONCE per assistant message (the nit: a
        // mid-stream retry re-enters the turn loop for the SAME message
        // — a second `message_start` would start a NEW one; the flag
        // suppresses it. A new model call after tool results resets it —
        // a new assistant message).
        let mut message_started = false;
        loop {
            if turn.is_cancelled() {
                // A CANCELLED turn settles with `turn_end` +
                // `agent_settled` (the open `messageId`'s accumulators /
                // the UI timeline end cleanly, not mid-message).
                self.settle_cancelled(std::mem::take(&mut turn_tool_results));
                return;
            }
            // (1) The `Compactor` (the context threshold or a forced
            // `compact()`): a summary model call, the older messages
            // replaced by the summary, `compaction_start` /
            // `compaction_end` emitted.
            if self.compactor.should_compact() || self.force_compact {
                self.force_compact = false;
                self.run_compaction(turn).await;
            }
            // (2) The model call (the `RetryPolicy` wraps it — on
            // `ProviderError::Retryable`, exponential backoff up to 5
            // attempts, `auto_retry_start` / `auto_retry_end` emitted).
            let req = self.model_request();
            let provider = &self.provider;
            let mut retry = self.retry;
            // The emitter (bound — `call_with_retry` takes a `&mut` of it;
            // a temporary closure would be dropped at the end of the
            // statement, before the `select!` polls the future).
            let mut emit = |ev: RpcEvent| self.emit(ev);
            // The closure returns a `'static` future (a `FnMut` closure's
            // captures cannot escape its body): `req` is cloned to a LOCAL
            // per call (a retry re-issues the SAME request), and the
            // `async move` moves the local — not the capture.
            // The model call RACES the cancel (finding 8b — a cancelled
            // turn must not wait out the model call: `provider.complete`
            // has no request timeout, and neither does a stalled stream —
            // pre-fix a cancel only took effect when the call happened
            // to finish; on cancel the turn settles and does not loop
            // again).
            let stream = tokio::select! {
                s = retry.call_with_retry(
                    move || {
                        let req = req.clone();
                        Box::pin(async move { provider.complete(&req).await })
                    },
                    turn,
                    &mut emit,
                ) => s,
                _ = turn.cancelled() => {
                    self.settle_cancelled(std::mem::take(&mut turn_tool_results));
                    return;
                }
            };
            self.retry = retry;
            let stream = match stream {
                Ok(s) => s,
                Err(e) => {
                    // The retry budget is exhausted (or a non-retryable
                    // error) — end the turn (finding 7: settle with
                    // `turn_end` + `agent_settled`, so the timeline closes
                    // cleanly).
                    eprintln!("harness: model call failed: {e}");
                    self.settle_error(std::mem::take(&mut turn_tool_results));
                    return;
                }
            };

            // (3) Consume the stream, emitting the normalized events
            // (`TextDelta` → `message_update { text_delta }`,
            // `ThinkingDelta` → `message_update { thinking_delta }`,
            // `ToolCall` → `tool_execution_start`, `Usage` →
            // `message_update { usage }`, `Done` → `message_end`).
            let mut acc = TurnAccumulator::default();
            // `message_start` ONCE per assistant message (the nit — see
            // the `message_started` flag above: a mid-stream retry is the
            // SAME message; a new model call after tool results is a new
            // one).
            if !message_started {
                self.emit(RpcEvent::message_start {
                    message: json!({ "role": "assistant" }),
                });
                message_started = true;
            }
            let mut stream = stream;
            let mut mid_stream_error: Option<ProviderError> = None;
            // The stream RACES the cancel (finding 8b — a cancelled turn
            // stops consuming; a stalled `stream.next()` is never waited
            // out). A partial message is NOT persisted (the turn is over).
            loop {
                tokio::select! {
                    item = stream.next() => {
                        let Some(ev) = item else { break };
                        match ev {
                            ProviderEvent::TextDelta(d) => {
                                acc.text.push_str(&d);
                                self.emit(RpcEvent::message_update {
                                    usage: acc.usage_value(),
                                    assistant_message_event: json!({
                                        "type": "text_delta",
                                        "contentIndex": 0,
                                        "delta": d,
                                    }),
                                });
                            }
                            ProviderEvent::ThinkingDelta(d) => {
                                acc.thinking.push_str(&d);
                                self.emit(RpcEvent::message_update {
                                    usage: acc.usage_value(),
                                    assistant_message_event: json!({
                                        "type": "thinking_delta",
                                        "contentIndex": 0,
                                        "delta": d,
                                    }),
                                });
                            }
                            ProviderEvent::ToolCallDelta(d) => {
                                self.emit(RpcEvent::message_update {
                                    usage: acc.usage_value(),
                                    assistant_message_event: json!({
                                        "type": "toolcall_delta",
                                        "id": d.id,
                                        "delta": d.arguments.unwrap_or_default(),
                                    }),
                                });
                            }
                            ProviderEvent::ToolCall(tc) => {
                                acc.tool_calls.push(tc.clone());
                                self.emit(RpcEvent::message_update {
                                    usage: acc.usage_value(),
                                    assistant_message_event: json!({
                                        "type": "toolcall_end",
                                        "toolCall": tc,
                                    }),
                                });
                                self.emit(RpcEvent::tool_execution_start {
                                    tool_call_id: tc.id.clone(),
                                    tool_name: tc.name.clone(),
                                    args: tc.arguments.clone(),
                                });
                            }
                            ProviderEvent::Usage(u) => {
                                acc.usage = Some(u);
                                self.compactor.add_usage(&u);
                                self.emit(RpcEvent::message_update {
                                    usage: acc.usage_value(),
                                    assistant_message_event: Value::Object(Default::default()),
                                });
                            }
                            ProviderEvent::Done(f) => acc.finish = Some(f),
                            ProviderEvent::Error(e) => mid_stream_error = Some(e),
                        }
                    }
                    // A cancelled turn stops consuming (finding 8b) —
                    // the turn settles (a partial assistant message is
                    // NOT persisted — the turn is over).
                    _ = turn.cancelled() => {
                        self.settle_cancelled(std::mem::take(&mut turn_tool_results));
                        return;
                    }
                }
            }
            // A stream that ended WITHOUT a `finish_reason` is a
            // transport failure (the provider synthesizes
            // `Done(Error)` for it) — treat it as a retryable error.
            if mid_stream_error.is_none() && acc.finish == Some(FinishReason::Error) {
                mid_stream_error = Some(ProviderError::Retryable(
                    "the model stream ended without a finish_reason".to_string(),
                ));
            }

            // A mid-stream `Retryable` error: a bounded re-try (the SAME
            // attempt budget — `next_retry_delay` shares the counter with
            // `call_with_retry`, so a turn never makes more than
            // `max_attempts` model calls).
            if let Some(ProviderError::Retryable(_)) = &mid_stream_error {
                if let Some(delay) = self.retry.next_retry_delay() {
                    self.emit(RpcEvent::auto_retry_start {
                        attempt: self.retry.attempt(),
                        max_attempts: self.retry.max_attempts(),
                        delay_ms: delay.as_millis() as u64,
                        error_message: mid_stream_error.as_ref().unwrap().to_string(),
                    });
                    // The backoff RACES the cancel (finding 8 / the
                    // backoff nit — a cancelled turn must not wait out
                    // the delay, up to 8 s).
                    tokio::select! {
                        _ = turn.cancelled() => {
                            self.settle_cancelled(std::mem::take(&mut turn_tool_results));
                            return;
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                    continue;
                }
                self.emit(RpcEvent::auto_retry_end {
                    success: false,
                    attempt: self.retry.attempt(),
                    final_error: Some(mid_stream_error.unwrap().to_string()),
                });
                self.settle_error(std::mem::take(&mut turn_tool_results));
                return;
            }
            if let Some(e) = mid_stream_error {
                // A non-retryable mid-stream error — end the turn (finding
                // 7: settle with `turn_end` + `agent_settled`, so the
                // timeline closes cleanly).
                eprintln!("harness: mid-stream model error: {e}");
                self.settle_error(std::mem::take(&mut turn_tool_results));
                return;
            }
            // `auto_retry_end { success: true }` when the turn used a
            // retry (the `RetryPolicy`'s success half — `call_with_retry`
            // emits only the `auto_retry_start`s + the exhaustion
            // `auto_retry_end`).
            if let Some(attempt) = self.retry.note_success() {
                self.emit(RpcEvent::auto_retry_end {
                    success: true,
                    attempt,
                    final_error: None,
                });
            }

            // The assistant message (text + tool calls) — `message_end`
            // (the authoritative content), then append + persist BEFORE
            // the tool results (the transcript order).
            let message_value = self.assistant_message_value(&acc);
            self.emit(RpcEvent::message_end {
                message: message_value,
            });
            let tool_calls = std::mem::take(&mut acc.tool_calls);
            self.messages.push(ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text(acc.text.clone()),
                tool_call_id: None,
                tool_calls: (!tool_calls.is_empty()).then_some(tool_calls.clone()),
            });
            self.persist_transcript_message();

            if tool_calls.is_empty() {
                // (5) The turn is done.
                self.emit(RpcEvent::agent_end {
                    messages: vec![self.assistant_message_value(&acc)],
                    will_retry: false,
                });
                self.emit(RpcEvent::turn_end {
                    message: Value::Null,
                    tool_results: turn_tool_results,
                });
                self.emit(RpcEvent::agent_settled);
                return;
            }

            // (4) For each tool call: GATE (mutating only — a deny is a
            // tool-result error `"permission denied"`), EXECUTE (the
            // native `ToolRegistry` — in-process), append the `ToolResult`
            // as a `tool` message + `tool_execution_end`.
            for tc in &tool_calls {
                // A CANCELLED turn must not execute the remaining calls
                // (finding 8a — a Stop is honored BETWEEN calls: each
                // skipped call gets a cancelled tool result and is NOT
                // executed; the turn settles at the top of the loop).
                let result = if turn.is_cancelled() {
                    Self::cancelled_tool_result()
                } else {
                    let allowed = if MUTATING_TOOLS.contains(&tc.name.as_str()) {
                        self.gate_tool(tc, turn).await
                    } else {
                        true
                    };
                    if allowed {
                        self.dispatch_tool(tc, turn).await
                    } else {
                        ToolResult {
                            content: vec![ContentBlock::Text {
                                text: "permission denied".to_string(),
                            }],
                            details: None,
                            is_error: true,
                        }
                    }
                };
                let result_value = serde_json::to_value(&result).unwrap_or(Value::Null);
                turn_tool_results.push(result_value.clone());
                self.emit(RpcEvent::tool_execution_end {
                    tool_call_id: tc.id.clone(),
                    tool_name: tc.name.clone(),
                    result: result_value,
                    is_error: result.is_error,
                });
                self.messages.push(ChatMessage {
                    role: ChatRole::Tool,
                    content: MessageContent::Text(result_text(&result)),
                    tool_call_id: Some(tc.id.clone()),
                    tool_calls: None,
                });
                self.persist_transcript_message();
            }
            // Loop back to (1) — the model sees the tool results (a NEW
            // assistant message — `message_start` is re-armed).
            message_started = false;
        }
    }

    /// Settle a CANCELLED turn: `turn_end` (the open `messageId`'s
    /// accumulators / the UI timeline end cleanly, not mid-message — the
    /// normal exit's `turn_end` shape) followed by `agent_settled` (the
    /// reliable settle signal).
    fn settle_cancelled(&self, tool_results: Vec<Value>) {
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }

    /// Settle a FAILED turn (finding 7 — a model call that exhausted the
    /// retry budget, a non-retryable error, or a mid-stream retry
    /// exhaustion): `turn_end` (the open `messageId`'s accumulators / the UI
    /// timeline end cleanly, not mid-message — the normal exit's `turn_end`
    /// shape) followed by `agent_settled` (the reliable settle signal).
    /// Mirrors `settle_cancelled` (pre-fix the three error exits emitted a
    /// bare `agent_settled` with no `turn_end`, so a failed turn's timeline
    /// did not close cleanly — one can be reached on a cancel race, since
    /// `select!` picks randomly when `call_with_retry` errors as the token
    /// fires).
    fn settle_error(&self, tool_results: Vec<Value>) {
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }

    /// The permission gate (the handle-free waiter — reviewer-corrected
    /// Major #14): a MUTATING tool on an untrusted Space prompts
    /// (`permission-request` via the sink, a `PendingPermissions` oneshot;
    /// a deny / cancel is a denial); a TRUSTED Space is auto-approved
    /// (ADR 0010 — replicated here: the native path has no
    /// `handle_extension_ui_request` to do it). A CANCELLED turn is a
    /// `Cancelled` (a deny) — the gate consults the token BEFORE the
    /// trusted short-circuit (finding 8a).
    async fn gate_tool(&self, tc: &ToolCall, turn: &CancellationToken) -> bool {
        let outcome = native_permission_gate(
            &self.session_id,
            &tc.id,
            &format!("{} {}", tc.name, tc.arguments),
            &self.sink,
            &self.pending_permissions,
            self.trust_db.as_ref(),
            &self.space_cwd,
            turn,
        )
        .await;
        matches!(
            outcome,
            PermissionOutcome::Selected { option_id }
                if option_id == "allow" || option_id == "trust-space"
        )
    }

    /// The cancelled tool result (the shape the batch loop uses for a
    /// call skipped by a cancel — a cancel that lands WHILE a flow is
    /// blocking on a sub-prompt returns the same shape: a cancel stops
    /// the turn, it is never waited out).
    fn cancelled_tool_result() -> ToolResult {
        ToolResult {
            content: vec![ContentBlock::Text {
                text: "cancelled".to_string(),
            }],
            details: None,
            is_error: true,
        }
    }

    /// The sudo `cancelled` result (finding 3 — honest about the kill
    /// behavior: a Stop BEFORE the command started PREVENTS it (not run);
    /// a Stop MID-RUN KILLS the started command — `kill_on_drop` + the
    /// process-group kill stop the direct command, though a process that
    /// had detached from the group may survive).
    fn sudo_cancelled_result() -> ToolResult {
        ToolResult {
            content: vec![ContentBlock::Text {
                text: "cancelled — a Stop before the command started prevents it; a Stop mid-run kills the started command".to_string(),
            }],
            details: None,
            is_error: true,
        }
    }

    /// The native `ToolRegistry`: a `match` on the tool name — the
    /// built-ins (Task 1's `execute_tool`, in-process, `ToolCtx { cwd,
    /// cancel }`) + the suite tools (the frame-free cores of the existing
    /// bridge handlers, called in-process — NOT over the bridge) +
    /// `subagent` (a native parent spawns an IN-PROCESS native child via
    /// `dispatch_native`).
    async fn dispatch_tool(&mut self, tc: &ToolCall, turn: &CancellationToken) -> ToolResult {
        // The `enabled_tools` filter (`None` = ALL — finding 13b): a
        // disabled tool is a tool-result error, NOT executed.
        // `dispatch_subagent` is an ALIAS of `subagent` (the `match`
        // below routes both to it): the gate must know the alias — a
        // parent with `enabled_tools: Some(["subagent"])` must not get
        // `tool not enabled: dispatch_subagent` when the model emits it.
        if let Some(tools) = &self.enabled_tools {
            let effective = if tc.name == "dispatch_subagent" {
                "subagent"
            } else {
                tc.name.as_str()
            };
            if !tools.iter().any(|t| t == effective) {
                return ToolResult {
                    content: vec![ContentBlock::Text {
                        text: format!("tool not enabled: {}", tc.name),
                    }],
                    details: None,
                    is_error: true,
                };
            }
        }
        match tc.name.as_str() {
            "bash" | "read" | "write" | "edit" | "find" | "grep" | "ls" => {
                execute_tool(
                    &ToolCtx {
                        cwd: self.space_cwd.clone(),
                        cancel: turn.clone(),
                    },
                    &tc.name,
                    &tc.arguments,
                )
                .await
            }
            "manage_todo_list" => {
                // RACED against the TURN token (finding 8b, round 2 —
                // a cancel stops the turn, it is never waited out; the
                // flow's own `cancel` is the SESSION teardown token,
                // which a Stop does NOT set).
                tokio::select! {
                    r = todo_apply(
                        &self.todo_store,
                        &self.session_id,
                        "native",
                        &tc.arguments,
                        &self.sink,
                        &self.cancel,
                    ) => r,
                    _ = turn.cancelled() => Self::cancelled_tool_result(),
                }
            }
            "sudo_exec" => {
                // RACED against the TURN token (finding 8b, round 2 —
                // the confirm / password sub-prompts `select!` only on
                // the SESSION teardown token + the 330 s bridge
                // timeout: a Stop while a dialog is open would wait out
                // up to 330 s, and a confirm answered AFTER the Stop
                // would still EXECUTE the elevated command).
                tokio::select! {
                    r = sudo_run_flow(
                        &self.session_id,
                        &tc.id,
                        "native",
                        &tc.arguments,
                        &self.sink,
                        &self.sudo.runner,
                        &self.sudo.pending_sudo,
                        &self.sudo.sudo_password,
                        &self.cancel,
                    ) => r,
                    _ = turn.cancelled() => Self::sudo_cancelled_result(),
                }
            }
            "ask" => self.ask_flow(&tc.arguments, &tc.id, turn).await,
            "subagent" | "dispatch_subagent" => self.dispatch_subagent(&tc.arguments, turn).await,
            other => ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("unknown tool: {other}"),
                }],
                details: None,
                is_error: true,
            },
        }
    }

    /// The in-process `ask` flow (the `ask` bridge method's flow — a
    /// `pending_bridge` oneshot + `bridge-request` event + the 300 s
    /// cap): the user's answer is shaped per the suite's
    /// `shapeAskResult` (a cancel → the cancelled shape; otherwise the
    /// `User answers` content).
    async fn ask_flow(
        &mut self,
        params: &Value,
        request_id: &str,
        turn: &CancellationToken,
    ) -> ToolResult {
        let key = bridge_key(&self.session_id, request_id);
        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.pending_bridge.lock().await;
            map.insert(key.clone(), tx);
        }
        self.sink.emit(
            "bridge-request",
            json!({
                "sessionId": self.session_id,
                "requestId": request_id,
                "method": "ask",
                "source": "native",
                "toolCallId": Value::Null,
                "params": params,
            }),
        );
        let response = tokio::select! {
            r = rx => r.ok(),
            _ = tokio::time::sleep(ASK_TIMEOUT) => None,
            _ = turn.cancelled() => None,
        };
        self.pending_bridge.lock().await.remove(&key);
        shape_ask_result(response, params)
    }

    /// `subagent` dispatch: a native parent session spawns an IN-PROCESS
    /// NATIVE child (the native-native subagent — `dispatch_native`: an
    /// in-process `AgentLoop` with the parent's model / tools minus
    /// `subagent` (the recursion guard), a `CapturingSink`, a throwaway
    /// `Db` — NO external `pi` process).
    async fn dispatch_subagent(&mut self, params: &Value, turn: &CancellationToken) -> ToolResult {
        let Some(manager) = &self.subagent else {
            return ToolResult {
                content: vec![ContentBlock::Text {
                    text: "subagent dispatch is not available in this session".to_string(),
                }],
                details: None,
                is_error: true,
            };
        };
        let (task, launch) = match crate::agent::bridge::dispatch_params(params) {
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
        let agent_name = params
            .get("agentName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let (dispatch_rx, cancel) = manager.dispatch_native(
            &self.session_id,
            &self.space_cwd,
            &self.model,
            self.enabled_tools().map(|v| v.to_vec()).unwrap_or_default(),
            agent_name,
            launch,
            task,
            &self.sink,
        );
        let outcome = tokio::select! {
            r = dispatch_rx => match r {
                Ok(crate::agent::subagent::SubagentOutcome::Completed { output, .. }) => {
                    SubagentWait::Completed { output }
                }
                Ok(crate::agent::subagent::SubagentOutcome::Failed { error }) => {
                    SubagentWait::Failed { error }
                }
                // The worker task vanished without resolving (app exit) —
                // the dispatch is gone: cancel.
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
            SubagentWait::Failed { error } => ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("subagent failed: {error}"),
                }],
                details: None,
                is_error: true,
            },
            SubagentWait::Cancelled => ToolResult {
                content: vec![ContentBlock::Text {
                    text: "cancelled".to_string(),
                }],
                details: None,
                is_error: true,
            },
        }
    }

    /// The compaction (a "summarize these messages" system prompt over
    /// the OLDER messages — the most recent `keepRecentTokens` kept; the
    /// older messages replaced by the summary, the transcript REWRITTEN
    /// (the old rows are replaced), `compaction_start` /
    /// `compaction_end` emitted).
    async fn run_compaction(&mut self, turn: &CancellationToken) {
        self.emit(RpcEvent::compaction_start {
            reason: "context_limit".to_string(),
        });
        let keep = self.catalog.compaction.keep_recent_tokens;
        let (older, recent) = split_for_compaction(&self.messages, keep);
        let mut aborted = false;
        let mut error_message: Option<String> = None;
        if !older.is_empty() {
            match self.summarize(&older, turn).await {
                Ok(summary) => {
                    let summary_msg = ChatMessage {
                        role: ChatRole::System,
                        content: MessageContent::Text(format!(
                            "Summary of previous conversation:\n{summary}"
                        )),
                        tool_call_id: None,
                        tool_calls: None,
                    };
                    let mut compacted = Vec::with_capacity(1 + recent.len());
                    compacted.push(summary_msg);
                    compacted.extend(recent);
                    self.messages = compacted;
                    self.compactor.reestimate(&self.messages);
                    // Rewrite the transcript (the old rows are replaced —
                    // a fresh `seq` run) ATOMICALLY: a single `Db`
                    // transaction (clear + reinsert), so a crash
                    // mid-rewrite never loses the transcript.
                    let rows: Vec<(u64, String, String)> = self
                        .messages
                        .iter()
                        .enumerate()
                        .map(|(seq, m)| {
                            (
                                seq as u64,
                                role_str(m.role).to_string(),
                                serde_json::to_string(m).unwrap_or_default(),
                            )
                        })
                        .collect();
                    if let Err(e) = self.store.replace_messages(&self.session_id, &rows) {
                        eprintln!("harness: transcript rewrite failed: {e}");
                    }
                }
                Err(e) => {
                    // The summary failed: the transcript is UNCHANGED (the
                    // next model call runs on the full context — a failed
                    // compaction is never a lost context).
                    aborted = true;
                    error_message = Some(e.to_string());
                    eprintln!("harness: compaction failed: {e}");
                }
            }
        }
        self.emit(RpcEvent::compaction_end {
            reason: "context_limit".to_string(),
            result: None,
            aborted,
            will_retry: false,
            error_message,
        });
    }

    /// The summary model call (a "summarize these messages" system prompt
    /// over the older messages — NO tools; the stream is consumed
    /// silently — the summary is internal, like pi's). The call + stream
    /// race the turn's cancel (a cancelled turn must not wait out the
    /// summary — the compaction is aborted; the turn settles at the top
    /// of the loop). A stream that ends with `Done(Error)` (the provider
    /// synthesizes it when the stream ends without a `finish_reason`) or
    /// without a `Done` at all is a retryable `Err` — a truncated summary
    /// must NEVER rewrite the transcript (finding 4: the `run_compaction`
    /// error path keeps the transcript intact).
    async fn summarize(
        &mut self,
        older: &[ChatMessage],
        turn: &CancellationToken,
    ) -> Result<String, ProviderError> {
        let system = ChatMessage {
            role: ChatRole::System,
            content: MessageContent::Text(
                "Summarize these messages concisely, preserving decisions, \
                 file paths, and open tasks. Respond with the summary only."
                    .to_string(),
            ),
            tool_call_id: None,
            tool_calls: None,
        };
        let mut messages = vec![system];
        messages.extend(older.iter().cloned());
        let req = ModelRequest {
            model: self.model.id.clone(),
            messages,
            tools: Vec::new(),
            options: ModelOptions {
                temperature: Some(0.0),
                max_tokens: None,
                reasoning_effort: None,
                stream: true,
            },
        };
        let mut stream = tokio::select! {
            s = self.provider.complete(&req) => s?,
            _ = turn.cancelled() => {
                return Err(ProviderError::Fatal(
                    "the turn was cancelled during compaction".to_string(),
                ));
            }
        };
        let mut summary = String::new();
        let mut finished: Option<FinishReason> = None;
        loop {
            tokio::select! {
                ev = stream.next() => {
                    let Some(ev) = ev else { break };
                    match ev {
                        ProviderEvent::TextDelta(d) => summary.push_str(&d),
                        ProviderEvent::Done(f) => {
                            finished = Some(f);
                            break;
                        }
                        ProviderEvent::Error(e) => return Err(e),
                        _ => {}
                    }
                }
                _ = turn.cancelled() => {
                    return Err(ProviderError::Fatal(
                        "the turn was cancelled during compaction".to_string(),
                    ));
                }
            }
        }
        // A `Done(Error)` (the provider synthesizes it when the stream
        // ends without a `finish_reason`), a `Done(Length)` (the summary
        // hit the OUTPUT TOKEN LIMIT — the wire `finish_reason`
        // `"length"`: the summary is literally TRUNCATED), or a stream
        // that ended WITHOUT a `Done` at all is a truncated summary — a
        // retryable `Err`, NOT a partial `Ok` (a partial summary would
        // rewrite the transcript and permanently lose the tail of the
        // summarized history from a mere transport hiccup).
        match finished {
            Some(FinishReason::Error) => Err(ProviderError::Retryable(
                "the summary stream ended without a finish_reason".to_string(),
            )),
            Some(FinishReason::Length) => Err(ProviderError::Retryable(
                "the summary hit the output token limit".to_string(),
            )),
            Some(_) => Ok(summary),
            None => Err(ProviderError::Retryable(
                "the summary stream ended without a finish_reason".to_string(),
            )),
        }
    }

    /// The model request (the `messages` vec + the tool specs + the
    /// sampling options — `reasoning_effort` from `set_thinking_level`).
    /// The advertised specs are FILTERED (finding B): `tool_specs()`
    /// restricted to the `enabled_tools` set (`None` = all) + `subagent`
    /// dropped for a subagent child (`subagent: None` — the child cannot
    /// dispatch subagents; the parent may).
    fn model_request(&self) -> ModelRequest {
        ModelRequest {
            model: self.model.id.clone(),
            messages: self.messages.clone(),
            tools: Self::advertised_tool_specs(&self.enabled_tools, self.subagent.is_none()),
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: self.thinking_level.clone(),
                stream: true,
            },
        }
    }

    /// The tool specs advertised to the model (finding B): `tool_specs()`
    /// filtered by (a) `enabled_tools` (`None` = all; `Some(v)` = exactly
    /// `v` — `Some(vec![])` = NO tools) and (b) dropping `subagent` when
    /// the session is a subagent child (`is_child` — `subagent: None`; the
    /// child cannot dispatch subagents, but a native parent may). The
    /// `dispatch_tool` gate remains as defense in depth.
    fn advertised_tool_specs(enabled_tools: &Option<Vec<String>>, is_child: bool) -> Vec<ToolSpec> {
        tool_specs()
            .into_iter()
            .filter(|spec| {
                let enabled = match enabled_tools {
                    Some(tools) => tools.contains(&spec.name),
                    None => true,
                };
                let not_child_subagent = !is_child || spec.name != "subagent";
                enabled && not_child_subagent
            })
            .collect()
    }

    /// The `message_end` / `agent_end` assistant message (the pi wire
    /// `AgentMessage` shape — `content` blocks: `text` / `thinking` /
    /// `toolCall` `{ id, name, arguments }`).
    fn assistant_message_value(&self, acc: &TurnAccumulator) -> Value {
        let mut content: Vec<Value> = Vec::new();
        if !acc.text.is_empty() {
            content.push(json!({ "type": "text", "text": acc.text }));
        }
        if !acc.thinking.is_empty() {
            content.push(json!({ "type": "thinking", "thinking": acc.thinking }));
        }
        for tc in &acc.tool_calls {
            content.push(json!({
                "type": "toolCall",
                "id": tc.id,
                "name": tc.name,
                "arguments": tc.arguments,
            }));
        }
        json!({ "role": "assistant", "content": content })
    }

    /// Persist the LAST `messages` entry (idempotent upsert at its `seq`).
    fn persist_transcript_message(&self) {
        let len = self.messages.len();
        let m = self
            .messages
            .get(len - 1)
            .cloned()
            .expect("a message was just pushed");
        let content_json = serde_json::to_string(&m).unwrap_or_default();
        if let Err(e) = self.store.insert_message(
            &self.session_id,
            (len - 1) as u64,
            role_str(m.role),
            &content_json,
        ) {
            eprintln!("harness: transcript persist failed: {e}");
        }
    }

    /// Emit one `RpcEvent`: through the EXISTING `normalize` +
    /// `persist_update` pipeline (the FROZEN `session-update` frames via
    /// the sink + the display `record_message` persistence) AND onto the
    /// `events` channel (the raw `RpcEvent` values — `try_send`: a full /
    /// closed channel drops the event, mirroring `send_prompt`;
    /// `Sender::send` is ASYNC in tokio and `emit` is sync — the
    /// `RetryPolicy`'s emitter closure is a `FnMut`, so the send cannot
    /// be awaited here). The settle is NOT taken from the (lossy)
    /// `events` channel: an `agent_settled` is ALSO written to the settle
    /// watch (a watch send is NEVER dropped — finding 3: a full / slow
    /// `events` mpsc can drop the raw event, but the driver's settle
    /// watch fires regardless, so the turn settle is never lost).
    fn emit(&self, ev: RpcEvent) {
        let updates = normalize(
            &ev,
            &mut self.turn_state.lock().unwrap_or_else(|p| p.into_inner()),
        );
        for u in &updates {
            self.sink.emit(
                "session-update",
                json!({ "sessionId": self.session_id, "update": u }),
            );
            let db = self.store.db();
            persist_update(
                db,
                &self.session_id,
                u,
                &self.text_acc,
                &self.tool_state,
                &self.thought_state,
            );
        }
        if matches!(ev, RpcEvent::agent_settled) {
            let n = self.settle_count.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.settle_tx.send(n);
        }
        if let Err(e) = self.events.try_send(ev) {
            eprintln!("harness: events channel full / closed, dropped an event: {e}");
        }
    }
}

/// The per-model-call accumulator (the streamed `ProviderEvent`s → the
/// assistant message).
#[derive(Default)]
struct TurnAccumulator {
    text: String,
    thinking: String,
    tool_calls: Vec<ToolCall>,
    usage: Option<Usage>,
    finish: Option<FinishReason>,
}

impl TurnAccumulator {
    /// The `message_update` `usage` field (REQUIRED on the wire — an
    /// object; zeros until a `Usage` event arrives).
    fn usage_value(&self) -> Value {
        match self.usage {
            Some(u) => json!({
                "inputTokens": u.input_tokens,
                "outputTokens": u.output_tokens,
            }),
            None => json!({ "inputTokens": 0, "outputTokens": 0 }),
        }
    }
}

/// The `subagent` dispatch's outcome (the `SubagentOutcome` mapping).
enum SubagentWait {
    Completed { output: String },
    Failed { error: String },
    Cancelled,
}

/// The denormalized `role` index (the `native_messages.role` column).
fn role_str(role: ChatRole) -> &'static str {
    match role {
        ChatRole::System => "system",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::Tool => "tool",
    }
}

/// The tool result's text (the first text block — the provider
/// transcript's `tool` message content).
fn result_text(result: &ToolResult) -> String {
    result
        .content
        .iter()
        .find_map(|b| match b {
            ContentBlock::Text { text } => Some(text.clone()),
            ContentBlock::Image { .. } => None,
        })
        .unwrap_or_default()
}

/// The tool specs sent to the model (the built-ins + the suite tools —
/// the schemas mirror the `tools.ts` override). `pub(crate)` so the
/// `dispatch_native` driver derives the child's tool set (the names
/// minus `subagent`) from it.
pub(crate) fn tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "bash".into(),
            description:
                "Run a shell command (sh -c; NOT re-parsed), capturing interleaved stdout+stderr."
                    .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "timeout_ms": { "type": "number" }
                },
                "required": ["command"]
            }),
        },
        ToolSpec {
            name: "read".into(),
            description: "Read a file (or an image, as a base64 content block).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "offset": { "type": "number" },
                    "limit": { "type": "number" }
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "write".into(),
            description: "Write a file (creating parent directories).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["path", "content"]
            }),
        },
        ToolSpec {
            name: "edit".into(),
            description: "Edit a file (exact text replacements).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "old_text": { "type": "string" },
                    "new_text": { "type": "string" },
                    "replace_all": { "type": "boolean" }
                },
                "required": ["path", "old_text", "new_text"]
            }),
        },
        ToolSpec {
            name: "find".into(),
            description: "Find files by name (a glob).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "max_results": { "type": "number" }
                },
                "required": ["pattern"]
            }),
        },
        ToolSpec {
            name: "grep".into(),
            description: "Search file contents (a regex).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "glob": { "type": "string" },
                    "max_results": { "type": "number" },
                    "-i": { "type": "boolean" }
                },
                "required": ["pattern"]
            }),
        },
        ToolSpec {
            name: "ls".into(),
            description: "List a directory.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "long": { "type": "boolean" }
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "ask".into(),
            description: "Ask the user a question (one or more, each with options).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "questions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "question": { "type": "string" },
                                "options": { "type": "array", "items": { "type": "object" } },
                                "multi": { "type": "boolean" }
                            },
                            "required": ["id", "question", "options"]
                        }
                    }
                },
                "required": ["questions"]
            }),
        },
        ToolSpec {
            name: "sudo_exec".into(),
            description: "Run a command with elevated privileges (confirm + masked password)."
                .into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "reason": { "type": "string" },
                    "timeoutMs": { "type": "number" }
                },
                "required": ["command", "reason"]
            }),
        },
        ToolSpec {
            name: "manage_todo_list".into(),
            description: "Manage a structured todo list (write / read).".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "operation": { "type": "string", "enum": ["write", "read"] },
                    "todoList": { "type": "array" }
                },
                "required": ["operation"]
            }),
        },
        ToolSpec {
            name: "subagent".into(),
            description: "Dispatch a subagent to run a task in a separate session.".into(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "task": { "type": "string" },
                    "agentName": { "type": "string" },
                    "model": { "type": "string" },
                    "systemPrompt": { "type": "string" },
                    "tools": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["task"]
            }),
        },
    ]
}

/// One question's answer (the suite's `QuestionResult` shape —
/// `packages/ask/src/tool.ts`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AskQuestionResult {
    id: String,
    question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    options: Vec<String>,
    multi: bool,
    selected: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom: Option<String>,
}

/// The suite's `responseToResults` (`packages/ask/src/tool.ts:192`): the
/// `AskResponsePayload` (`{ cancelled, results: [{ id, selectedOptions,
/// customInput? }] }`) → one `QuestionResult` per question (a missing
/// `results[i]` is an empty answer).
fn response_to_results(response: Option<&Value>, questions: &[Value]) -> Vec<AskQuestionResult> {
    questions
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let r = response
                .and_then(|r| r.get("results"))
                .and_then(Value::as_array)
                .and_then(|a| a.get(i));
            let options = q
                .get("options")
                .and_then(Value::as_array)
                .map(|opts| {
                    opts.iter()
                        .map(|o| {
                            o.get("label")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string()
                        })
                        .collect()
                })
                .unwrap_or_default();
            AskQuestionResult {
                id: q
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                question: q
                    .get("question")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                description: q
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .filter(|s| !s.trim().is_empty()),
                options,
                multi: q.get("multi").and_then(Value::as_bool).unwrap_or(false),
                selected: r
                    .and_then(|r| r.get("selectedOptions"))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
                custom: r
                    .and_then(|r| r.get("customInput"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .filter(|s| !s.is_empty()),
            }
        })
        .collect()
}

/// The suite's `shapeAskResult` (a cancel — `cancelled: true` with no
/// selected options — → the cancelled shape; otherwise the `User
/// answers` content).
fn shape_ask_result(response: Option<Value>, params: &Value) -> ToolResult {
    let questions = params
        .get("questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let results = response_to_results(response.as_ref(), &questions);
    let all_empty = results.iter().all(|r| r.selected.is_empty());
    let cancelled = response
        .as_ref()
        .and_then(|r| r.get("cancelled"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let details = json!({
        "results": serde_json::to_value(&results).unwrap_or(Value::Array(Vec::new())),
        "customInput": Value::Null,
        "description": Value::Null,
    });
    if cancelled && all_empty {
        return ToolResult {
            content: vec![ContentBlock::Text {
                text: "User cancelled the question.".to_string(),
            }],
            details: Some(details),
            is_error: false,
        };
    }
    ToolResult {
        content: vec![ContentBlock::Text {
            text: build_ask_session_content(&results),
        }],
        details: Some(details),
        is_error: false,
    }
}

/// The suite's `buildAskSessionContent` (the summary + the per-question
/// context).
fn build_ask_session_content(results: &[AskQuestionResult]) -> String {
    let summary: Vec<String> = results
        .iter()
        .map(|r| format!("{}: {}", r.id, selection_summary(r)))
        .collect();
    let context: Vec<String> = results
        .iter()
        .enumerate()
        .map(|(i, r)| question_context(r, i))
        .collect();
    format!(
        "User answers:\n{}\n\nAnswer context:\n{}",
        summary.join("\n"),
        context.join("\n\n")
    )
}

/// The suite's `formatSelectionForSummary`.
fn selection_summary(r: &AskQuestionResult) -> String {
    let has_selected = !r.selected.is_empty();
    let has_custom = r.custom.is_some();
    if !has_selected && !has_custom {
        return "(cancelled)".to_string();
    }
    if has_selected && has_custom {
        let selected_part = if r.multi {
            format!("[{}]", r.selected.join(", "))
        } else {
            r.selected.first().cloned().unwrap_or_default()
        };
        return format!(
            "{selected_part} + Other: \"{}\"",
            r.custom.as_deref().unwrap_or("")
        );
    }
    if has_custom {
        return format!("\"{}\"", r.custom.as_deref().unwrap_or(""));
    }
    if r.multi {
        return format!("[{}]", r.selected.join(", "));
    }
    r.selected.first().cloned().unwrap_or_default()
}

/// The suite's `formatQuestionContext`.
fn question_context(r: &AskQuestionResult, index: usize) -> String {
    let mut lines = vec![
        format!("Question {} ({})", index + 1, r.id),
        format!("Prompt: {}", r.question),
    ];
    if let Some(d) = &r.description {
        lines.push("Context:".to_string());
        for line in d.split('\n') {
            lines.push(format!("  {line}"));
        }
    }
    lines.push("Options:".to_string());
    for (i, option) in r.options.iter().enumerate() {
        lines.push(format!("  {}. {option}", i + 1));
    }
    lines.push("Response:".to_string());
    let has_selected = !r.selected.is_empty();
    let has_custom = r.custom.is_some();
    if !has_selected && !has_custom {
        lines.push("  Selected: (cancelled)".to_string());
    } else {
        if has_selected {
            let selected_text = if r.multi {
                format!("[{}]", r.selected.join(", "))
            } else {
                r.selected.first().cloned().unwrap_or_default()
            };
            lines.push(format!("  Selected: {selected_text}"));
        }
        if has_custom {
            if !has_selected {
                lines.push("  Selected: Other (type your own)".to_string());
            }
            lines.push(format!(
                "  Custom input: {}",
                r.custom.as_deref().unwrap_or("")
            ));
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::agent::harness::catalog::CompactionConfig;
    use crate::storage::Db;

    /// A `SudoRunner` that records a call (the tests assert the
    /// elevated command was / was NOT executed) and never runs it.
    struct RecordingSudoRunner {
        ran: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl SudoRunner for RecordingSudoRunner {
        fn run(
            &self,
            _argv: Vec<String>,
            _password: String,
            _timeout: Duration,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::agent::bridge::SudoRun> + Send + 'static>,
        > {
            self.ran.store(true, Ordering::SeqCst);
            Box::pin(async { unreachable!("the recording runner never completes a run") })
        }
    }

    /// A scripted `Provider` (canned `ProviderEvent`s per `complete` call
    /// — out of range repeats the LAST script; `None` = a HANGING call
    /// (`complete` never resolves — the cancel tests). `calls` counts the
    /// invocations (the tests assert the turn did not loop again).
    struct ScriptedProvider {
        calls: Arc<AtomicU32>,
        scripts: Vec<Option<Vec<ProviderEvent>>>,
    }

    impl ScriptedProvider {
        fn new(scripts: Vec<Option<Vec<ProviderEvent>>>) -> (Self, Arc<AtomicU32>) {
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
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
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
    struct FailingProvider {
        error: ProviderError,
    }

    #[async_trait::async_trait]
    impl Provider for FailingProvider {
        async fn complete(
            &self,
            _req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            Err(self.error.clone())
        }
    }

    /// A recording `EventSink` (the `session-update` frames — the tests
    /// assert on the `session-update` payloads).
    struct TestSink {
        updates: Arc<StdMutex<Vec<Value>>>,
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

    /// Build an `AgentLoop` (the harness unit-test seam — a temp-dir `Db`,
    /// a single `fake/m1` catalog model, a scripted provider; the
    /// `events` / `turn_cancel` / `settle_tx` wiring mirrors
    /// `build_native_session`).
    fn build_loop(
        provider: Box<dyn Provider>,
        events: mpsc::Sender<RpcEvent>,
        turn_cancel: Arc<StdMutex<CancellationToken>>,
        settle_tx: watch::Sender<u64>,
        retry: RetryPolicy,
    ) -> AgentLoop {
        let dir = std::env::temp_dir().join(format!("harness-loop-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Arc::new(Db::open(&dir.join("t.db")).expect("db should open"));
        // The `native_messages` rows FK to `sessions`: record the test
        // session so transcript writes (e.g. the `run_compaction`
        // rewrite) are valid.
        db.record_session(&crate::agent::SessionInfo {
            session_id: "s1".to_string(),
            agent_id: "native".to_string(),
            cwd: std::path::PathBuf::from("/tmp"),
            capabilities: serde_json::json!({}),
            config_options: None,
        })
        .expect("record_session");
        let model = Model {
            id: "m1".to_string(),
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
        };
        let catalog = ModelCatalog {
            models: vec![model.clone()],
            default_model: None,
            compaction: CompactionConfig::default(),
        };
        let (prompt_tx, prompt_rx) = mpsc::channel(8);
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        });
        AgentLoop::new(
            "s1".to_string(),
            dir,
            model,
            provider,
            catalog,
            SessionStore::new(db),
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
        )
    }

    /// Await the first event matching `pred` (bounded — the tests must
    /// not hang).
    async fn wait_for_event(
        rx: &mut mpsc::Receiver<RpcEvent>,
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

    /// (finding 3) The `events` mpsc is FULL (the consumer is slow /
    /// blocked): the `agent_settled` delivery is DROPPED, but the settle
    /// watch fires regardless — the driver's settle (and the `send_prompt`
    /// / `wait_for_settle` waits it drives) is never lost to a lossy
    /// `events` delivery.
    #[tokio::test]
    async fn a_full_events_channel_does_not_lose_the_settle() {
        let (events_tx, mut events_rx) = mpsc::channel(4);
        for _ in 0..4 {
            events_tx.try_send(RpcEvent::turn_start).unwrap();
        }
        let (settle_tx, settle_rx) = watch::channel(0u64);
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            settle_tx,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        loop_.handle_prompt("hello").await;
        // The raw `agent_settled` was DROPPED (the channel is still full —
        // the first event is still the pre-filled `turn_start`).
        assert_eq!(
            events_rx.try_recv().unwrap().kind(),
            "turn_start",
            "the settled event was dropped (the channel was full)"
        );
        // ...but the settle watch fired (the reliable signal).
        assert_eq!(
            *settle_rx.borrow(),
            1,
            "the settle watch fired despite the dropped events delivery"
        );
    }

    /// (finding 8a) A turn with 2 tool calls: a cancel after the first
    /// call (the `ask` blocks until the cancel) → the second is NOT
    /// executed (a cancelled tool result; the turn settles).
    #[tokio::test]
    async fn a_cancel_stops_the_remaining_tool_calls_in_a_batch() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "ask".to_string(),
                    arguments: json!({
                        "questions": [{ "id": "q1", "question": "Which?", "options": [{ "label": "A" }] }]
                    }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/nonexistent-file" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let task = tokio::spawn(async move { loop_.handle_prompt("hello").await });
        // The first tool starts (the `ask` blocks until the cancel).
        wait_for_event(
            &mut events_rx,
            5000,
            |e| matches!(e, RpcEvent::tool_execution_start { tool_name, .. } if tool_name == "ask"),
        )
        .await
        .expect("the `ask` tool started");
        tokio::time::sleep(Duration::from_millis(100)).await;
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the turn settled after the cancel (it did not wait out the `ask`)");
        // The `read` was NOT executed — a cancelled tool result.
        let end = wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t2")
        })
        .await
        .expect("the `read`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert_eq!(
            result["content"][0]["text"], "cancelled",
            "the skipped call is NOT executed"
        );
        assert!(is_error, "the skipped call is an error result");
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
    }

    /// (finding 8b) A model call that never finishes: a cancel settles
    /// the turn (the loop does NOT wait out the model — pre-fix the
    /// cancel only took effect when the call happened to finish, and a
    /// stalled `complete` / `stream.next()` blocks indefinitely).
    #[tokio::test]
    async fn a_cancel_stops_an_in_flight_model_call() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let task = tokio::spawn(async move { loop_.handle_prompt("hello").await });
        tokio::time::sleep(Duration::from_millis(200)).await; // the call is in flight
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the cancel settled the turn (it did not wait out the model)");
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("agent_settled after the cancel");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the turn did not loop again"
        );
    }

    /// (finding 1) A prompt that is SKIPPED (a Stop is pending when
    /// `handle_prompt` runs it) must settle its `pending_turn` resolver:
    /// the loop emits `agent_settled` (the driver resolves the slot
    /// `Cancelled` via the `cancel_requested` flag). Pre-fix the early
    /// return emitted nothing, so the skipped prompt's `send_prompt`
    /// caller hung until a `close_session`.
    #[tokio::test]
    async fn a_skipped_prompt_settles_the_pending_turn() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let (settle_tx, settle_rx) = watch::channel(0u64);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            settle_tx,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        // A Stop is pending (the turn token is cancelled) BEFORE the prompt
        // runs: `handle_prompt` must settle the skipped prompt (not just
        // drop it).
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        loop_.handle_prompt("hello").await;
        // The skipped prompt's settle (pre-fix this was missing → the
        // `send_prompt` caller hung).
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("agent_settled after the skipped prompt");
        assert_eq!(*settle_rx.borrow(), 1, "the settle watch fired");
        // The token is RE-ARMED (a later prompt starts a fresh turn).
        assert!(
            !turn_cancel
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_cancelled(),
            "the token was re-armed"
        );
    }

    /// (finding 1) A prompt that is DRAINED (a Stop lands while it is
    /// still queued — `run()`'s turn-cancel arm) must settle its
    /// `pending_turn` resolver: the loop emits `agent_settled` (the driver
    /// resolves the slot `Cancelled`). Pre-fix the drain arm just dropped
    /// the items, so the queued prompt's `send_prompt` caller hung.
    #[tokio::test]
    async fn a_drained_queued_prompt_settles_the_pending_turn() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let (settle_tx, settle_rx) = watch::channel(0u64);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
        let loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            settle_tx,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        // Queue a prompt (its `pending_turn` resolver is conceptually held by
        // the caller — here the loop just has to settle it).
        loop_.send_prompt("hello");
        // A Stop (the turn token) arrives — the queued prompt is drained (or
        // dequeued + skipped; either way the loop must settle it). Pre-fix the
        // drain arm dropped the item and `agent_settled` never fired.
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        let _task = tokio::spawn(async move { loop_.run().await });
        // The drained prompt's settle (pre-fix missing → the `send_prompt`
        // caller hung).
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("agent_settled after the drained prompt");
        assert_eq!(*settle_rx.borrow(), 1, "the settle watch fired");
        // The spawned loop task runs until the test's runtime drops it
        // (it is not awaited — a turn-cancelled loop keeps running).
    }

    /// (finding 7) A model call that FAILS (a non-retryable `Fatal` error)
    /// ends the turn with `turn_end` + `agent_settled` (the timeline closes
    /// cleanly — pre-fix the error exits emitted a bare `agent_settled` with
    /// no `turn_end`, so a failed turn's timeline did not close cleanly).
    #[tokio::test]
    async fn a_failed_model_call_settles_with_turn_end_and_agent_settled() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let provider = FailingProvider {
            error: ProviderError::Fatal("the model call failed".to_string()),
        };
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        loop_.handle_prompt("hello").await;
        // The failed turn settles with `turn_end` (the timeline closes) +
        // `agent_settled` (the reliable settle signal) — `turn_end` FIRST.
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::turn_end { .. })
        })
        .await
        .expect("turn_end after the failed model call (finding 7)");
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("agent_settled after the failed model call");
    }

    /// (finding 8b, round 2) A `sudo_exec` blocking on its confirm
    /// sub-prompt + a TURN cancel → the flow returns a cancelled result
    /// PROMPTLY (the turn token is raced — pre-fix the flow selected
    /// only on the SESSION teardown token + the 330 s bridge timeout,
    /// so a Stop waited out up to 330 s, and a confirm answered AFTER
    /// the Stop still EXECUTED the elevated command). The command is
    /// NEVER run.
    #[tokio::test]
    async fn a_cancel_stops_a_sudo_flow_blocking_on_its_confirm_prompt() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "sudo_exec".to_string(),
                arguments: json!({ "command": "echo never-runs", "reason": "test" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ])]);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        // A recording runner: if the elevated command ever runs, the
        // test fails.
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        loop_.sudo.runner = Arc::new(RecordingSudoRunner { ran: ran.clone() });
        let task = tokio::spawn(async move { loop_.handle_prompt("hello").await });
        // The `sudo_exec` is now blocking on its confirm sub-prompt
        // (never answered in the test).
        tokio::time::sleep(Duration::from_millis(500)).await;
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the turn settled promptly (it did not wait out the 330 s bridge timeout)");
        assert!(
            !ran.load(Ordering::SeqCst),
            "the elevated command was NEVER executed"
        );
        let end = wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the sudo tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .starts_with("cancelled"),
            "the cancelled flow is a cancelled tool result (the finding 3 honest text)"
        );
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .contains("kills the started command"),
            "the sudo cancel result is the honest kill-behavior text (finding 3), not the generic `cancelled`"
        );
        assert!(is_error, "the cancelled flow is an error result");
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
    }

    /// (the nit) A mid-stream `Retryable` error: the retry is the SAME
    /// assistant message — ONE `message_start` (pre-fix a retried model
    /// call emitted a `message_start` per attempt — multiple starts for
    /// one message).
    #[tokio::test]
    async fn a_retried_model_call_emits_one_message_start() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::TextDelta("part".to_string()),
                ProviderEvent::Error(ProviderError::Retryable("transient".to_string())),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        loop_.handle_prompt("hello").await;
        let mut events = Vec::new();
        while let Ok(ev) = events_rx.try_recv() {
            events.push(ev);
        }
        let starts = events
            .iter()
            .filter(|e| matches!(e, RpcEvent::message_start { .. }))
            .count();
        assert_eq!(
            starts, 1,
            "one message_start for the retried assistant message"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, RpcEvent::auto_retry_start { .. })),
            "the retry happened"
        );
        assert!(
            events.iter().any(|e| matches!(e, RpcEvent::agent_settled)),
            "the turn settled"
        );
    }

    /// (the nit) A retryable error arms a 30 s backoff: a cancel settles
    /// the turn FAST (the backoff sleep races the cancel — pre-fix a
    /// cancelled turn waited out up to 8 s of backoff).
    #[tokio::test]
    async fn a_cancel_during_the_retry_backoff_settles_fast() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![ProviderEvent::Error(
            ProviderError::Retryable("transient".to_string()),
        )])]);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(3, Duration::from_secs(30)),
        );
        let task = tokio::spawn(async move { loop_.handle_prompt("hello").await });
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::auto_retry_start { .. })
        })
        .await
        .expect("the retry backoff was armed (30 s)");
        tokio::time::sleep(Duration::from_millis(100)).await;
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the cancel settled the turn (the 30 s backoff was cut short)");
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("agent_settled after the cancel");
    }

    // ── (finding 4) a truncated summary stream is NOT a good summary ──

    /// (finding 4) The provider synthesizes `Done(FinishReason::Error)`
    /// when a stream ends without a `finish_reason` — `summarize` must
    /// treat it as a retryable `Err`, NOT return a partial `Ok` (a
    /// partial summary would rewrite the transcript and lose history
    /// from a mere transport hiccup).
    #[tokio::test]
    async fn a_summary_stream_ending_in_an_error_is_not_a_good_summary() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("partial".to_string()),
            ProviderEvent::Done(FinishReason::Error),
        ])]);
        let mut loop_ = build_loop(
            Box::new(provider),
            mpsc::channel(8).0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let older = vec![ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("the old messages".to_string()),
            tool_call_id: None,
            tool_calls: None,
        }];
        let r = loop_.summarize(&older, &CancellationToken::new()).await;
        assert!(
            matches!(r, Err(ProviderError::Retryable(_))),
            "a `Done(Error)` stream is a retryable error, got {r:?}"
        );
    }

    /// (finding 4, round 2) A summary that hits the OUTPUT TOKEN LIMIT
    /// (`Done(FinishReason::Length)` — the wire `finish_reason`
    /// `"length"`) is TRUNCATED: it must be a retryable `Err`, NOT an
    /// `Ok` (an accepted truncated summary would rewrite the
    /// transcript and permanently lose the tail of the summarized
    /// history — the same invariant as the `Done(Error)` case above).
    #[tokio::test]
    async fn a_summary_stream_ending_in_length_is_not_a_good_summary() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("partial".to_string()),
            ProviderEvent::Done(FinishReason::Length),
        ])]);
        let mut loop_ = build_loop(
            Box::new(provider),
            mpsc::channel(8).0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let older = vec![ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("the old messages".to_string()),
            tool_call_id: None,
            tool_calls: None,
        }];
        let r = loop_.summarize(&older, &CancellationToken::new()).await;
        assert!(
            matches!(r, Err(ProviderError::Retryable(_))),
            "a `Done(Length)` stream is a retryable error, got {r:?}"
        );
    }

    /// (finding 4) A summary stream that ends WITHOUT a `Done` at all
    /// (the transport just dropped) is a retryable `Err` too — no
    /// partial summary is returned.
    #[tokio::test]
    async fn a_summary_stream_without_a_done_is_not_a_good_summary() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![ProviderEvent::TextDelta(
            "partial".to_string(),
        )])]);
        let mut loop_ = build_loop(
            Box::new(provider),
            mpsc::channel(8).0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let older = vec![ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("the old messages".to_string()),
            tool_call_id: None,
            tool_calls: None,
        }];
        let r = loop_.summarize(&older, &CancellationToken::new()).await;
        assert!(
            matches!(r, Err(ProviderError::Retryable(_))),
            "a stream that ends without a `Done` is a retryable error, got {r:?}"
        );
    }

    /// (finding 4) `run_compaction` over a failing summary: the
    /// transcript is UNCHANGED (a failed compaction is never a lost
    /// context — the `compaction_end` is `aborted` with the error).
    #[tokio::test]
    async fn a_failed_summary_does_not_rewrite_the_transcript() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("partial".to_string()),
            ProviderEvent::Done(FinishReason::Error),
        ])]);
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        // 3 large messages (~20000 tokens each — over the default
        // `keep_recent_tokens` budget, so `older` is non-empty and the
        // summary is actually attempted).
        loop_.messages = (0..3)
            .map(|i| ChatMessage {
                role: ChatRole::User,
                content: MessageContent::Text(format!("m{i}{}", "x".repeat(80000))),
                tool_call_id: None,
                tool_calls: None,
            })
            .collect();
        loop_.force_compact = true;
        loop_.run_compaction(&CancellationToken::new()).await;
        // The summary failed: the transcript is UNCHANGED (NOT replaced
        // by a truncated summary + a rewritten `native_messages`).
        assert_eq!(loop_.messages.len(), 3, "the transcript is intact");
        assert!(
            !loop_
                .messages
                .iter()
                .any(|m| matches!(m.role, ChatRole::System)),
            "no summary message was inserted"
        );
        let end = wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::compaction_end { .. })
        })
        .await
        .expect("the compaction_end was emitted");
        let RpcEvent::compaction_end {
            aborted,
            error_message,
            ..
        } = end
        else {
            unreachable!()
        };
        assert!(aborted, "the compaction was aborted");
        assert!(error_message.is_some(), "the error was reported");
    }

    /// (finding 13b) `enabled_tools` is wired into `dispatch_tool`:
    /// a disabled tool (`bash`, with only `read` enabled) is a
    /// tool-result error, NOT executed; an enabled tool (`read`) is
    /// still dispatched. A `None` `enabled_tools` (the default) leaves
    /// every tool dispatchable (the current behavior).
    #[tokio::test]
    async fn a_disabled_tool_is_a_tool_result_error_not_execution() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
        let mut loop_ = build_loop(
            Box::new(provider),
            mpsc::channel(8).0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        loop_.set_enabled_tools(Some(vec!["read".to_string()]));
        // `bash` is disabled (only `read` is enabled) → a tool-result
        // error, NOT executed.
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t1".to_string(),
                    name: "bash".to_string(),
                    arguments: json!({ "command": "echo hi" }),
                },
                &CancellationToken::new(),
            )
            .await;
        assert!(r.is_error, "the disabled tool is an error result");
        assert_eq!(
            result_text(&r),
            "tool not enabled: bash",
            "the error names the disabled tool"
        );
        // `read` is enabled → still dispatched (the executor's own
        // result for a missing file — NOT a "tool not enabled" error).
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t2".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/nonexistent-file" }),
                },
                &CancellationToken::new(),
            )
            .await;
        assert_ne!(
            result_text(&r),
            "tool not enabled: read",
            "an enabled tool is dispatched"
        );
        // A `None` `enabled_tools` (the default) → every tool
        // dispatchable (the current behavior).
        loop_.set_enabled_tools(None);
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t3".to_string(),
                    name: "bash".to_string(),
                    arguments: json!({ "command": "echo hi" }),
                },
                &CancellationToken::new(),
            )
            .await;
        assert_ne!(
            result_text(&r),
            "tool not enabled: bash",
            "a `None` `enabled_tools` enables every tool"
        );
    }

    /// (finding 13b) End-to-end: a native session with
    /// `enabled_tools: ["read"]` cannot dispatch `ls` (a non-mutating
    /// tool — no permission gate) — the model gets a `tool not enabled`
    /// tool-result error and the turn settles.
    #[tokio::test]
    async fn a_native_session_with_enabled_tools_cannot_dispatch_a_disabled_tool() {
        let (events_tx, mut events_rx) = mpsc::channel(256);
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "ls".to_string(),
                    arguments: json!({ "path": "/tmp" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        loop_.set_enabled_tools(Some(vec!["read".to_string()]));
        loop_.handle_prompt("hello").await;
        let end = wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the `ls`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(is_error, "the disabled tool is an error result");
        assert_eq!(
            result["content"][0]["text"], "tool not enabled: ls",
            "the model gets a tool-result error (the tool was NOT executed)"
        );
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
    }

    /// `dispatch_subagent` is an ALIAS of `subagent` (the `dispatch_tool`
    /// `match` routes both to `dispatch_subagent`): the `enabled_tools`
    /// gate must know the alias — a parent with
    /// `enabled_tools: Some(["subagent"])` must NOT get
    /// `tool not enabled: dispatch_subagent` when the model emits the
    /// alias (here `subagent: None` → the "not available" dispatch error
    /// instead). A parent with `enabled_tools: Some(["read"])` (subagent
    /// disabled) still gets the "not enabled" error for the alias.
    #[tokio::test]
    async fn the_dispatch_subagent_alias_is_gated_by_the_subagent_enabled_tool() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
        let mut loop_ = build_loop(
            Box::new(provider),
            mpsc::channel(8).0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        // `subagent` enabled → the alias is enabled too: NOT a
        // "tool not enabled" error (here `subagent: None` → the
        // "not available" dispatch error instead).
        loop_.set_enabled_tools(Some(vec!["subagent".to_string()]));
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t1".to_string(),
                    name: "dispatch_subagent".to_string(),
                    arguments: json!({ "task": "do the thing" }),
                },
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(
            result_text(&r),
            "subagent dispatch is not available in this session",
            "the alias is enabled when `subagent` is (the `subagent: None` manager is the \"not available\" error, NOT \"not enabled\")"
        );
        // `subagent` disabled (only `read` enabled) → the alias is
        // still disabled: the "not enabled" error.
        loop_.set_enabled_tools(Some(vec!["read".to_string()]));
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t2".to_string(),
                    name: "dispatch_subagent".to_string(),
                    arguments: json!({ "task": "do the thing" }),
                },
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(
            result_text(&r),
            "tool not enabled: dispatch_subagent",
            "the alias is disabled when `subagent` is disabled"
        );
    }

    // ── the `exec_*` param keys ──────────────────────────────────────

    /// The param keys each built-in `exec_*` reads (the contract table —
    /// kept in sync with `tools/exec.rs`). The `tool_specs()` advertised
    /// schema must match this EXACTLY: a drift makes the model send
    /// params the executor never reads (a dead tool) or omit ones it
    /// does (an undiscoverable feature).
    fn exec_param_keys() -> [(&'static str, &'static [&'static str]); 7] {
        [
            ("bash", &["command", "timeout_ms"]),
            ("read", &["path", "offset", "limit"]),
            ("write", &["path", "content"]),
            ("edit", &["path", "old_text", "new_text", "replace_all"]),
            ("find", &["pattern", "path", "max_results"]),
            ("grep", &["pattern", "path", "glob", "max_results", "-i"]),
            ("ls", &["path", "long"]),
        ]
    }

    #[test]
    fn tool_specs_advertise_exactly_the_executor_param_keys() {
        let specs = tool_specs();
        for (name, keys) in exec_param_keys() {
            let spec = specs
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("no `tool_specs` entry for `{name}`"));
            let advertised: BTreeSet<String> = spec
                .parameters
                .get("properties")
                .and_then(Value::as_object)
                .map(|o| o.keys().cloned().collect())
                .unwrap_or_default();
            let expected: BTreeSet<String> = keys.iter().map(|s| s.to_string()).collect();
            assert_eq!(
                advertised, expected,
                "tool `{name}`: advertised {advertised:?} != executor keys {expected:?}"
            );
        }
    }

    #[test]
    fn role_str_maps_the_roles() {
        assert_eq!(role_str(ChatRole::System), "system");
        assert_eq!(role_str(ChatRole::User), "user");
        assert_eq!(role_str(ChatRole::Assistant), "assistant");
        assert_eq!(role_str(ChatRole::Tool), "tool");
    }

    #[test]
    fn result_text_takes_the_first_text_block() {
        let r = ToolResult {
            content: vec![
                ContentBlock::Image {
                    image: crate::agent::tools::ImageRef {
                        data: "a".into(),
                        mime_type: "image/png".into(),
                    },
                },
                ContentBlock::Text {
                    text: "the text".to_string(),
                },
            ],
            details: None,
            is_error: false,
        };
        assert_eq!(result_text(&r), "the text");
    }

    #[test]
    fn shape_ask_result_cancelled() {
        let params = json!({
            "questions": [{ "id": "q1", "question": "Which?", "options": [{ "label": "A" }] }]
        });
        // A `cancelled: true` with no selections (or a missing response —
        // a timeout) → the cancelled shape.
        let r = shape_ask_result(
            Some(json!({ "cancelled": true, "results": [{ "id": "q1", "selectedOptions": [] }] })),
            &params,
        );
        assert_eq!(
            result_text(&r),
            "User cancelled the question.",
            "a cancel is the cancelled shape"
        );
        assert!(!r.is_error);
        let r = shape_ask_result(None, &params);
        assert_eq!(result_text(&r), "User cancelled the question.");
    }

    #[test]
    fn shape_ask_result_answered() {
        let params = json!({
            "questions": [{
                "id": "q1",
                "question": "Which framework?",
                "options": [{ "label": "Tauri" }, { "label": "Electron" }],
                "multi": false
            }]
        });
        let r = shape_ask_result(
            Some(json!({
                "cancelled": false,
                "results": [{ "id": "q1", "selectedOptions": ["Tauri"] }]
            })),
            &params,
        );
        let text = result_text(&r);
        assert!(
            text.starts_with("User answers:"),
            "the answered shape, got {text}"
        );
        assert!(text.contains("q1: Tauri"), "the summary line, got {text}");
        assert!(
            text.contains("Prompt: Which framework?"),
            "the context block, got {text}"
        );
        assert!(!r.is_error);
    }
}

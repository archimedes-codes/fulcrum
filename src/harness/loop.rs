//! The native agent loop (native-agent-harness Task 6 — the core of
//! Phase 2): one in-process `AgentLoop` (a tokio task) per native session
//! owns the conversation's control flow: it receives prompts, runs the
//! model → tool → retry/compaction loop, emits the normalized events
//! (the SAME `RpcEvent` shapes an external session emits — the loop runs
//! them through the existing `normalize` + `compute_display_rows` +
//! `Store::persist_display` pipeline, so the frontend is unchanged), and
//! persists the provider transcript through the `Store` seam (the
//! `native_messages` table).
//!
//! Tools are dispatched IN-PROCESS (the native `ToolRegistry`): the
//! built-ins (Task 1's `execute_tool`) + the suite tools (the in-process
//! cores of the interactive channel — `todo_apply` /
//! `sudo_run_flow` / the in-process `ask` waiter) + `subagent` (the
//! parent's `SubagentDispatcher` — the `WorkerManager`'s `dispatch_subagent`
//! flow spawns a WORKER child process (the ADR 0025 re-plumb: the
//! in-process `AgentLoop` child is GONE — the child's transcript persists
//! as an ephemeral `is_subagent` `sessions` row). A permission gate precedes
//! every tool call the file-access policy (ADR 0030) sends to `Ask`: a
//! trusted Space is auto-approved (ADR 0010), a deny is a tool-result
//! error `"permission denied"`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::events::RpcEvent;
use crate::agent::fs_backend::{FsBackend, FsError};
use crate::agent::harness::catalog::{Model, ModelCatalog};
use crate::agent::harness::compact::{self, Compactor};
use crate::agent::harness::dispatch::SubagentDispatcher;
use crate::agent::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ModelOptions, ModelRequest, Provider,
    ProviderError, ProviderEvent, ToolCall, ToolSpec, Usage,
};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::harness::store::{role_str, Store};
use crate::agent::harness::trust::TrustSource;
use crate::agent::interactive::{
    sudo_run_flow, todo_apply, CachedPassword, PendingInteractive, PendingSudo, RealSudoRunner,
    SudoRunner,
};
use crate::agent::mcp::{mcp_tool, McpManager};
use crate::agent::normalize::{compute_display_rows, normalize, ThoughtState, TurnState};
use crate::agent::permission::{
    native_permission_gate, permission_title, PendingPermissions, PermissionOutcome,
};
use crate::agent::policy::{decision_for, AccessPolicy, Decision, FilePolicy};
use crate::agent::todo::TodoStore;
use crate::agent::tools::exec::protected_refusal_text;
use crate::agent::tools::{execute_tool, ContentBlock, ImageRef, ToolCtx, ToolResult};

/// The built-ins the file-access policy (ADR 0030) judges, and the DIRECTION
/// each is judged in: the read-direction tools are measured against the read
/// boundary (`cwd` + the discovery roots), the write-direction ones against
/// the session `cwd` alone. `bash` is listed for the gate's benefit but has
/// NO path to judge (it is always `beyond`, see
/// [`crate::agent::policy::decision_for`]).
///
/// Deliberately absent: `read_skill` / `list_skills` (their own per-skill
/// containment, ADR 0029 — a skill may never read a sibling skill, so the
/// session boundary is not their judge), `subagent` / `mcp` /
/// `manage_todo_list` / the interactive flows (no argument of theirs reaches
/// the filesystem in a way this policy describes).
const POLICIED_TOOLS: &[(&str, PolicyDirection)] = &[
    ("bash", PolicyDirection::Shell),
    ("read", PolicyDirection::Read),
    ("find", PolicyDirection::Read),
    ("grep", PolicyDirection::Read),
    ("ls", PolicyDirection::Read),
    ("write", PolicyDirection::Write),
    ("edit", PolicyDirection::Write),
];

/// The direction a policed tool is judged in (ADR 0030: the policy is
/// per-DIRECTION, so `read` obeys `reads` and `edit` obeys `writes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyDirection {
    /// Judged against the read boundary; policy `reads`.
    Read,
    /// Judged against the session `cwd`; policy `writes`.
    Write,
    /// No path: always `beyond`; policy `shell`.
    Shell,
}

/// The tool the file-access policy judges, and in which direction
/// (`None` = ungated — the skill tools, `subagent`, `mcp`, the interactive
/// flows, everything else).
fn policed_direction(tool: &str) -> Option<PolicyDirection> {
    POLICIED_TOOLS
        .iter()
        .find(|(name, _)| *name == tool)
        .map(|(_, dir)| *dir)
}

/// A prompt to the loop (the `prompt_queue` item): the text + the image
/// attachments (`ImageRef` — the pi `ImageContent` shape; the manager
/// maps the wire `ImagePayload` onto it). An empty `images` is a
/// text-only prompt (the common case).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prompt {
    pub text: String,
    pub images: Vec<ImageRef>,
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
    /// The settings dir (the `settings.json` home — ADR 0023:
    /// `resolve_launch` reads the `subagentModels` override from here at
    /// dispatch time; `None` = no override layer).
    config_dir: Option<PathBuf>,
    pub store: Arc<dyn Store>,
    /// The `RpcEvent`-shaped values (the SAME vocabulary an external
    /// session emits — the driver / tests consume it). UNBOUNDED (ADR
    /// 0025: the event stream drives the Supervisor's internal
    /// bookkeeping — settle detection, the `SubagentCapture`'s
    /// `agent_settled`-adjacent signals, the debug log — a full bounded
    /// channel dropping those frames is unacceptable; the unbounded
    /// `send` never fails, so nothing is dropped).
    pub events: mpsc::UnboundedSender<RpcEvent>,
    /// The SESSION teardown token (`close_session` — `run()` exits on it;
    /// the driver tears the session down when the loop task ends).
    pub cancel: CancellationToken,
    /// The current TURN's cancel token (`cancel_session` Stop — finding
    /// 8c): SHARED with the `NativeHandle` (the handle cancels the
    /// CURRENT turn; the loop arms a fresh token per prompt — a stale
    /// cancel does not settle the next turn, and a Stop keeps the
    /// session ALIVE, matching the external `abort`). `pub` (the Worker's
    /// `build_loop` seam keeps the handle — ADR 0025 Task 2).
    pub turn_cancel: Arc<StdMutex<CancellationToken>>,
    /// The RELIABLE settle signal (finding 3): `emit` writes it on every
    /// `agent_settled` (a watch send is NEVER dropped — the settle watch
    /// fires regardless of the `events` delivery). The counter forces
    /// `changed()` to fire on every settle (a watch coalesces equal
    /// values). `pub` (the Worker's `build_loop` seam — ADR 0025 Task 2).
    pub settle_tx: watch::Sender<u64>,
    settle_count: AtomicU64,
    /// The prompt queue (the `pub` sender — `pub` so the Worker's
    /// `build_loop` seam can clone it before `tokio::spawn` (ADR 0025
    /// Task 2); the bounded channel's backpressure is the intended
    /// `send().await` stall).
    pub prompt_tx: mpsc::Sender<Prompt>,
    prompt_queue: mpsc::Receiver<Prompt>,
    /// The control channel (the `set_config_option` native branch — the
    /// sender is `pub` so the session's `NativeHandle` can clone it; the
    /// receiver is consumed by `run()`). Created here (NOT a `new`
    /// parameter — the `new` signature is unchanged).
    pub control_tx: mpsc::Sender<ControlCmd>,
    control_queue: mpsc::Receiver<ControlCmd>,
    pub pending_permissions: PendingPermissions,
    pub pending_bridge: PendingInteractive,
    /// The trust lookup source (ADR 0010 — the permission gate's
    /// `space_trusted` lookup; `None` = fail-closed: the gate prompts).
    pub trust: Option<Arc<dyn TrustSource>>,
    /// The Tauri event sink (the `permission-request` / `interactive-request`
    /// / `session-update` frames).
    pub sink: Arc<dyn EventSink>,
    pub todo_store: Arc<TodoStore>,
    /// The MCP server manager (ADR 0018 — the `mcp` tool's machinery;
    /// `pub` so a test can swap in a temp-config manager; constructed in
    /// `new` with `home_dir` = `dirs::home_dir` + `project_cwd` =
    /// `space_cwd`).
    pub mcp: McpManager,
    /// The subagent dispatch handle (a native parent spawns a native
    /// child — `SubagentDispatcher::dispatch`; `None` when the manager is
    /// absent).
    pub subagent: Option<Arc<dyn SubagentDispatcher>>,
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
    /// The session's current context size in tokens (the `context_usage_
    /// update` frame source — the frontend's context-percentage display).
    ///
    /// The SAME snapshot the compaction threshold reads: the last `Usage`
    /// the provider reported (`input + output` — the full prompt plus that
    /// reply's output) plus the local estimate of whatever was appended
    /// since (the `Compactor`'s anchor + trailing tail). One number for the
    /// bar and the trigger — never the sum of every call's prompt.
    last_context_tokens: u64,
    /// (ADR 0030) The session's READ boundary — `cwd` + the discovery roots
    /// (user + space level), computed ONCE at construction (the boundary is
    /// frozen for the session's life: a skill dir created mid-session must
    /// not silently widen it). Handed to every `ToolCtx`.
    read_boundary: Vec<PathBuf>,
    /// (ADR 0030) The write DENY-LIST (`boundary::protected_dirs()` — the
    /// user-level agent-definition dirs), frozen with the boundary.
    protected: Vec<PathBuf>,
    /// (ADR 0030 Task 3) The file-access policy IN FORCE for the current
    /// turn — refreshed from `settings.json` at the top of every
    /// `handle_prompt` (Deviation 4: one read per turn, so every tool call
    /// in a turn agrees and a mid-turn hand-edit is observed at the NEXT
    /// prompt; the same in-process read `launch.rs` does per dispatch).
    /// `config_dir: None` → all-`Allow` (no settings layer to read).
    /// Handed to every `ToolCtx` so the executor enforces the SAME tier the
    /// gate decided, and read by the gate itself.
    file_policy: FilePolicy,
}

impl AgentLoop {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session_id: String,
        space_cwd: PathBuf,
        model: Model,
        provider: Box<dyn Provider>,
        catalog: ModelCatalog,
        store: Arc<dyn Store>,
        events: mpsc::UnboundedSender<RpcEvent>,
        cancel: CancellationToken,
        turn_cancel: Arc<StdMutex<CancellationToken>>,
        settle_tx: watch::Sender<u64>,
        prompt_tx: mpsc::Sender<Prompt>,
        prompt_queue: mpsc::Receiver<Prompt>,
        pending_permissions: PendingPermissions,
        pending_bridge: PendingInteractive,
        trust: Option<Arc<dyn TrustSource>>,
        sink: Arc<dyn EventSink>,
        todo_store: Arc<TodoStore>,
        subagent: Option<Arc<dyn SubagentDispatcher>>,
        sudo: SudoDeps,
        retry: RetryPolicy,
        // The settings dir (the `settings.json` home — ADR 0019: the MCP
        // manager's desktop layer; `None` = no desktop layer).
        config_dir: Option<PathBuf>,
    ) -> Self {
        let compactor = Compactor::new(catalog.compaction, model.context_window);
        let (control_tx, control_queue) = mpsc::channel(8);
        // The MCP manager (ADR 0018 — the `mcp` tool's machinery): the
        // global `~/.pi/agent/mcp.json` + the desktop `settings.json`
        // `mcpServers` (ADR 0019) + the project `<space_cwd>/.pi/mcp.json`.
        let mcp = McpManager::new(
            dirs::home_dir().unwrap_or_else(|| PathBuf::from("/")),
            space_cwd.clone(),
            config_dir.as_deref(),
        );
        // (ADR 0030) The boundary is computed ONCE here, not per tool call
        // (it is frozen for the session, and `read_roots` walks the tree +
        // canonicalizes). Reading `$HOME` is legitimate HERE — the Worker
        // process already does it for MCP and for skill discovery.
        let read_boundary = crate::agent::boundary::read_roots(&space_cwd);
        let protected = crate::agent::boundary::protected_dirs();
        Self {
            session_id,
            space_cwd,
            model,
            provider,
            catalog,
            config_dir,
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
            trust,
            sink,
            todo_store,
            mcp,
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
            last_context_tokens: 0,
            read_boundary,
            protected,
            // Refreshed per turn by `handle_prompt` (Deviation 4); the
            // construction value only covers a tool call made BEFORE the
            // first prompt, of which there are none.
            file_policy: FilePolicy::default(),
        }
    }

    /// Emit the `context_usage_update` frame (the frontend's
    /// context-percentage display — the session's current context size vs
    /// the model's window). NOT part of the FROZEN ACP `session-update`
    /// vocabulary: it bypasses the `normalize` / `persist_update` pipeline
    /// on purpose (a bookkeeping frame, like the client-synthesized
    /// `config_option_update` — the desktop, not the agent, owns it).
    ///
    /// ALSO persists the usage on the `sessions` row (the
    /// `context_usage_json` column — via the `Store` seam, ADR 0025:
    /// `SqliteStore` writes, `IpcStore` frames, `NoopStore` ignores —
    /// the old `trust_db`-gating is gone: an ephemeral subagent row now
    /// also gets a `context_usage_json` write, which is harmless — the
    /// persister's `ensure_session_row` creates the row; the display is
    /// per-session): the frontend's store drops the entry on close, so
    /// the row is the source of truth for a CLOSED session's context bar
    /// (the stored session's `context_usage` over IPC). A write failure
    /// is a silent no-op (the frame is the primary path — a db hiccup
    /// must not break the display).
    fn emit_context_usage(&self) {
        self.sink.emit(
            "session-update",
            json!({
                "sessionId": self.session_id,
                "update": {
                    "sessionUpdate": "context_usage_update",
                    "usedTokens": self.last_context_tokens,
                    "windowTokens": self.model.context_window,
                },
            }),
        );
        let _ = self.store.record_context_usage(
            &self.session_id,
            self.last_context_tokens,
            u64::from(self.model.context_window),
        );
    }

    /// Queue a prompt (best-effort — a full / closed queue is dropped).
    pub fn send_prompt(&self, text: &str, images: &[ImageRef]) {
        let _ = self.prompt_tx.try_send(Prompt {
            text: text.to_string(),
            images: images.to_vec(),
        });
    }

    /// Switch the model. The context (the `messages`) is UNCHANGED, but the
    /// `Compactor` IS rebuilt (the context window — and therefore the
    /// threshold — is per-model), which drops the usage anchor: so it is
    /// RE-ESTIMATED on the current transcript and the frame is emitted from
    /// that same re-estimate. The number itself usually lands where it was (the
    /// transcript did not change), but the anchor is gone and the local estimate
    /// is what stands in until the next `Usage` the provider reports corrects it.
    ///
    /// The re-estimate is not cosmetic: emitting the PREVIOUS value while the
    /// rebuilt `Compactor` still reads `0` would leave the bar and
    /// `should_compact()` disagreeing until the next prompt, and switching INTO a
    /// smaller window would defer a compaction that is already due by a whole
    /// model call (the call that has to send the oversized context).
    pub fn set_model(&mut self, model: Model) {
        self.compactor = Compactor::new(self.catalog.compaction, model.context_window);
        self.model = model;
        self.compactor.reestimate(&self.messages);
        self.last_context_tokens = self.compactor.context_tokens();
        self.emit_context_usage();
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
    /// `messages: self.messages.clone()`, so it leads every model call),
    /// and persisted at seq 0 (a resume replays it; a compaction
    /// preserves it).
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
        self.last_context_tokens = self.compactor.context_tokens();
        self.emit_context_usage();
        self.persist_system_message();
    }

    /// Seed the provider transcript (resume: `SessionStore::load_messages`
    /// restores the stored transcript BEFORE the first model call — the
    /// `Compactor` is re-estimated on the loaded context).
    pub fn load_transcript(&mut self, messages: Vec<ChatMessage>) {
        self.messages = messages;
        self.compactor.reestimate(&self.messages);
        self.last_context_tokens = self.compactor.context_tokens();
        self.emit_context_usage();
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
                    Some(p) => self.handle_prompt(&p).await,
                    None => break,
                },
            }
        }
        // Session teardown: kill the MCP stdio children (dropping a
        // `StdioClient` kills the child via `kill_on_drop`) + clear the state.
        self.mcp.close_all();
    }

    /// Handle one prompt (the model → tool → retry/compaction loop).
    /// `prompt` carries the text + the image attachments (the `user`
    /// message the turn pushes is `Blocks` — text first, then the image
    /// blocks — when images are present; a text-only prompt keeps the
    /// `Text` shape).
    pub async fn handle_prompt(&mut self, prompt: &Prompt) {
        // (ADR 0030 Task 3, Deviation 4) The policy is re-read from
        // `settings.json` ONCE per turn — the same in-process read
        // `launch.rs` does per dispatch, so a user who flips a select in
        // Settings affects an ALREADY-RUNNING session at its next prompt
        // (no restart, no relaunch). ONE read per turn (not per tool call):
        // every tool call in a turn must agree, or a half-asked batch would
        // be a policy the user never chose. `config_dir: None` → the
        // all-`Allow` default (no settings layer).
        self.file_policy = self
            .config_dir
            .as_deref()
            .map(crate::config::load_settings)
            .map(|s| s.file_policy)
            .unwrap_or_default();
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
        self.handle_turn(&turn, &prompt.text, &prompt.images).await;
    }

    /// The turn body (the model → tool → retry/compaction loop) — `turn`
    /// is the turn's cancel token (finding 8b: the model call, the
    /// stream, the backoff sleeps, the tool batch, the gate, and the
    /// `ask` / `subagent` / `summarize` flows all race it — a cancel
    /// stops the turn, it is never waited out).
    async fn handle_turn(&mut self, turn: &CancellationToken, text: &str, images: &[ImageRef]) {
        // The display `user` row is written SOLELY by the manager's
        // `send_prompt_with_images` (`record_message` — the command
        // delegates to it and adds no persistence of its own): the loop
        // must NOT re-persist the user message (the `(session_id, kind,
        // message_key)` key with `message_key = NULL` treats NULLs as
        // DISTINCT in `ON CONFLICT`, so a double write would show a
        // duplicate user bubble in restored history). The provider
        // transcript is `native_messages` (the push below).
        //
        // The provider-transcript `user` message is `Text` for a
        // text-only prompt (the common case) and `Blocks` (text FIRST,
        // then the image blocks — an empty text adds no text block)
        // when images are present: the provider's `to_wire` sends the
        // blocks as OpenAI `text` / `image_url` parts (the model sees
        // the image), while the `ContentBlock`'s own pi-shaped
        // serialization is the transcript / DB shape.
        let content = if images.is_empty() {
            MessageContent::Text(text.to_string())
        } else {
            let mut blocks: Vec<ContentBlock> = Vec::new();
            if !text.is_empty() {
                blocks.push(ContentBlock::Text {
                    text: text.to_string(),
                });
            }
            for image in images {
                blocks.push(ContentBlock::Image {
                    image: image.clone(),
                });
            }
            MessageContent::Blocks(blocks)
        };
        self.messages.push(ChatMessage {
            role: ChatRole::User,
            content,
            tool_call_id: None,
            tool_calls: None,
        });
        self.persist_transcript_message();
        // The turn's own opening event FIRST — the context frame below is
        // session-scoped bookkeeping, so it must not lead the turn it belongs to.
        self.emit(RpcEvent::turn_start);
        // The prompt is in the transcript: measure it NOW. `should_compact()`
        // is read at the TOP of the loop below, so a push that left the
        // `Compactor` untouched was invisible to the turn's FIRST threshold
        // check — a large pasted prompt therefore deferred a compaction that
        // was already due by a whole model call (the call that had to send
        // the oversized context to the provider). Cheap: `refresh` sums only
        // the trailing slice. Measured AFTER `turn_start` (the frame is
        // bookkeeping), still BEFORE the first threshold read.
        self.note_context();

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
                                self.compactor.record_usage(&u, &self.messages);
                                // The context is the usage ANCHOR the
                                // provider just set: `input + output` is the
                                // whole prompt plus this reply's output (the
                                // same count pi keeps), and the bar and the
                                // compaction threshold read that ONE number.
                                self.last_context_tokens = self.compactor.context_tokens();
                                self.emit_context_usage();
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
                    // The retry DISCARDS the partial reply this stream was
                    // producing — and the `Usage` it reported (if any) left the
                    // `Compactor` with the same optimistic watermark the settle
                    // paths retract: `messages.len() + 1`, "the reply lands at
                    // index `len`", when no reply is ever pushed. This
                    // loop-back is the ONE path that returns to the top of the
                    // turn — i.e. to `should_compact()` — WITHOUT exiting the
                    // turn, so `settle_cancelled` / `settle_error` cannot cover
                    // it: unreconciled, the very next threshold check reads the
                    // discarded attempt's `output_tokens`, and a large dying
                    // reply compacts the session before the retry's own call
                    // even runs. Same reconcile the exits do, and the
                    // `watermark > len` guard makes it a no-op if the reply did
                    // land (it cannot here — the push happens only on the clean
                    // path).
                    self.compactor.turn_abandoned(&self.messages);
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
            // The assistant message is now in the transcript, so the
            // context is re-anchored on it (the usage anchor + the tail).
            self.note_context();

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
            // (4a) Dispatch the enabled `subagent` calls CONCURRENTLY (the
            // long-running fan-out — `dispatch_subagent` is `&self`, so a
            // shared reborrow lets N run at once via `join_all`). The results
            // are keyed by tool-call INDEX (unique by construction — a
            // duplicate tool-call `id` would collide on a `HashMap` keyed by
            // id); (4b) appends them in the ORIGINAL `tool_calls` order. (A
            // CANCELLED turn skips the batch — the `turn.is_cancelled()`
            // checks below handle it.)
            let mut subagent_results: HashMap<usize, ToolResult> = if turn.is_cancelled() {
                HashMap::new()
            } else {
                let me: &Self = self;
                let futures: Vec<_> = tool_calls
                    .iter()
                    .enumerate()
                    .filter(|(_, tc)| {
                        crate::agent::harness::dispatch::is_enabled_subagent_call(
                            me.enabled_tools(),
                            tc,
                        )
                    })
                    .map(|(idx, tc)| {
                        let args = tc.arguments.clone();
                        async move { (idx, me.dispatch_subagent(&args, turn).await) }
                    })
                    .collect();
                futures_util::future::join_all(futures)
                    .await
                    .into_iter()
                    .collect()
            };

            // (4b) Append the tool results in the ORIGINAL `tool_calls` order
            // (the `subagent` calls use the pre-computed concurrent result;
            // the other tools run sequentially — a `subagent` call that is NOT
            // enabled falls through to `dispatch_tool`'s "tool not enabled"
            // error). A `subagent` call's CACHED result wins even on a
            // cancelled turn — it already reflects the actual outcome (a
            // subagent cancelled mid-batch is `Cancelled` from the `select!`;
            // one that finished is `Completed`), so a completed subagent's
            // result is preserved rather than silently replaced by
            // `"cancelled"`. A non-`subagent` call that is skipped by a Stop
            // gets a cancelled tool result (finding 8a — a Stop is honored
            // BETWEEN calls; the turn settles at the top of the loop).
            for (idx, tc) in tool_calls.iter().enumerate() {
                let result = if crate::agent::harness::dispatch::is_subagent_call(&tc.name) {
                    match subagent_results.remove(&idx) {
                        Some(r) => r,
                        None if turn.is_cancelled() => Self::cancelled_tool_result(),
                        None => self.dispatch_tool(tc, turn).await,
                    }
                } else if turn.is_cancelled() {
                    Self::cancelled_tool_result()
                } else {
                    // (ADR 0030) The ONE policy decision point. `Ask` is the
                    // ONLY outcome that prompts (`gate_tool` blocks until the
                    // user responds, or the turn is cancelled); `Deny` is a
                    // tool-result error that NEVER reaches the UI; `Allow` and
                    // `Confine` dispatch (a `Confine`d `bash` confines itself
                    // in the executor — Task 5). Ungated tools (`read_skill`,
                    // `mcp`, the interactive flows, …) fall straight through.
                    // (A `match` GUARD cannot `.await`, hence the nested `if`.)
                    match self.policy_verdict(tc) {
                        Some(Decision::Ask) => {
                            if self.gate_tool(tc, turn).await {
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
                        }
                        Some(Decision::Deny) => self.deny_tool_result(tc),
                        // `Allow` / `Confine` / an ungated tool: dispatch.
                        _ => self.dispatch_tool(tc, turn).await,
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
                    // The result's FULL `Blocks` (a `read` on an image
                    // file is an image-only result — flattening to the
                    // first text block would drop the image the model is
                    // meant to see; the provider's `to_wire` sends the
                    // blocks as OpenAI `text` / `image_url` parts).
                    content: MessageContent::Blocks(result.content.clone()),
                    tool_call_id: Some(tc.id.clone()),
                    tool_calls: None,
                });
                self.persist_transcript_message();
            }
            // The batch is over: re-anchor the context ONCE for the whole
            // batch. `should_compact()` is read at the TOP of the next
            // iteration, so anything appended after the last re-anchor is
            // invisible to the threshold — and tool results are by far the
            // largest messages a turn produces (a `read`, a `grep`, a `cat`
            // of a doc set), so this is the measurement that matters most.
            // On a usage-less endpoint the usage anchor is `0` and this
            // estimate is the ONLY measurement there is: pre-fix a whole
            // batch of huge output stayed uncounted and the session blew
            // past the window undetected (the provider 400s where compaction
            // was due). ONCE per batch, not per tool call: the batch is one
            // logical change to the context, and the frame `note_context`
            // emits also writes the `sessions` row (`record_context_usage`) —
            // a per-call refresh would be N re-estimates and N DB writes for
            // nothing new in between.
            self.note_context();
            // Loop back to (1) — the model sees the tool results (a NEW
            // assistant message — `message_start` is re-armed).
            message_started = false;
        }
    }

    /// Re-anchor the context after the transcript grew: the single point
    /// where the context is recomputed (the `Compactor`'s usage anchor plus
    /// the local estimate of the messages appended since), feeding BOTH the
    /// compaction threshold and the `context_usage_update` frame — one
    /// number, one source. Called after the assistant message is pushed, so
    /// the watermark `record_usage` set (past that message) lines up.
    ///
    /// The invariant that matters for the bar and the trigger: a model call
    /// that reported NO `Usage` cannot shrink the accounting, because the
    /// anchor only moves when the provider really reports — the estimate is
    /// purely ADDITIVE on top of it (it replaced the old floor-raiser, which
    /// had to special-case exactly this). It CAN move down on `reestimate`
    /// (post-compaction / resume), where the transcript genuinely shrank.
    ///
    /// The frame is emitted ONLY when the refresh actually moved the number.
    /// Equality is the exact condition — no delta threshold: when the stream
    /// reported usage, the `ProviderEvent::Usage` arm already anchored the
    /// count and emitted, and the assistant push that follows leaves the
    /// trailing slice EMPTY, so a re-emit would be a value-identical frame
    /// AND a value-identical `record_context_usage` — which is not free: the
    /// frame also writes the `sessions` row (an IPC round trip → an
    /// `ensure_session_row` SELECT + an `UPDATE`), once per iteration, on the
    /// COMMON (usage-reporting) path. On a usage-less endpoint the value
    /// always moves (the reply / the tool batch grew the estimate), so the
    /// emissions that matter are untouched.
    fn note_context(&mut self) {
        let next = self.compactor.refresh(&self.messages);
        if next != self.last_context_tokens {
            self.last_context_tokens = next;
            self.emit_context_usage();
        }
    }

    /// Settle a CANCELLED turn: `turn_end` (the open `messageId`'s
    /// accumulators / the UI timeline end cleanly, not mid-message — the
    /// normal exit's `turn_end` shape) followed by `agent_settled` (the
    /// reliable settle signal).
    ///
    /// ALSO reconciles the `Compactor` (see [`Compactor::turn_abandoned`]): a
    /// cancelled turn does NOT persist the partial assistant message, so the
    /// watermark `record_usage` set points one past the transcript. The
    /// reconcile lives HERE rather than at the call sites so a future exit
    /// cannot forget it. No context frame is emitted: the next turn's
    /// prompt-time `note_context` re-anchors and emits (the value here is
    /// strictly between two authoritative readings, and the frame also writes
    /// the `sessions` row).
    fn settle_cancelled(&mut self, tool_results: Vec<Value>) {
        self.compactor.turn_abandoned(&self.messages);
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
    ///
    /// Reconciles the `Compactor` for the same reason `settle_cancelled` does
    /// — a failed turn persists no assistant message — and for the same reason
    /// lives here rather than at the call sites.
    fn settle_error(&mut self, tool_results: Vec<Value>) {
        self.compactor.turn_abandoned(&self.messages);
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }

    /// The file-access verdict for one tool call (ADR 0030), or `None` for
    /// a tool the policy does NOT judge (`read_skill` / `list_skills` —
    /// their own containment, ADR 0029 — `subagent`, `mcp`,
    /// `manage_todo_list`, the interactive flows).
    ///
    /// "Beyond the boundary" is answered by the SAME canonicalize-then-check
    /// the ADR 0029 attack matrix proved (`FsBackend::validate` over the
    /// direction's boundary roots) — deliberately NOT a second canonicalizer
    /// here: a gate and an executor that resolve `..` / symlinks differently
    /// is the classic sandbox escape, so they share one resolver and can not
    /// drift. `Ok` = in bounds; ANY `Err` (escape, or a root set that cannot
    /// be judged) = `beyond`, so an unjudgeable path is never silently
    /// allowed.
    ///
    /// A tool call with NO usable `path` is in bounds (the executor's own
    /// "missing parameter" error still fires, ungated — a malformed call is
    /// not a policy event), and a `find` / `grep` / `ls` whose `path` is
    /// omitted means the `cwd` (their advertised default), so it too is in
    /// bounds.
    fn policy_verdict(&self, tc: &ToolCall) -> Option<Decision> {
        let dir = policed_direction(&tc.name)?;
        let (policy, beyond) = match dir {
            PolicyDirection::Shell => {
                // No path to judge: a command can reach ANYWHERE, so it is
                // always beyond the boundary (see `decision_for`).
                (self.file_policy.shell, true)
            }
            PolicyDirection::Read | PolicyDirection::Write => {
                let Some(raw) = tc.arguments.get("path").and_then(|v| v.as_str()) else {
                    // `read` / `write` / `edit` REQUIRE a path (the executor
                    // rejects the call without one) and the directory tools
                    // default it to the `cwd` — either way there is nothing
                    // to gate.
                    return Some(Decision::Allow);
                };
                let policy = self.direction_policy(dir);
                let beyond = self.beyond_error(dir, Path::new(raw)).is_some();
                (policy, beyond)
            }
        };
        let trusted = self
            .trust
            .as_ref()
            .map(|t| t.is_trusted(&self.space_cwd))
            .unwrap_or(false);
        Some(decision_for(
            policy,
            beyond,
            trusted,
            dir == PolicyDirection::Shell,
        ))
    }

    /// The policy that governs a direction (ADR 0030: the settings are
    /// per-direction, which is why `read` and `edit` can disagree).
    fn direction_policy(&self, dir: PolicyDirection) -> AccessPolicy {
        match dir {
            PolicyDirection::Read => self.file_policy.reads,
            PolicyDirection::Write => self.file_policy.writes,
            PolicyDirection::Shell => self.file_policy.shell,
        }
    }

    /// The root set a direction is judged against: the READ boundary (the
    /// `cwd` plus the discovery roots) for reads, the session `cwd` ALONE
    /// for writes (`boundary::write_roots` — a repo-level `.agents/skills`
    /// is repo content and stays writable from a package cwd). NOT the
    /// policy-derived executor roots, which widen to `/` under `Ask`/`Allow`
    /// — the gate must see the real boundary even when the executor is
    /// unrestricted (that widening is the gate's DECISION, not its input).
    fn direction_boundary(&self, dir: PolicyDirection) -> Vec<PathBuf> {
        match dir {
            PolicyDirection::Read | PolicyDirection::Shell => self.read_boundary.clone(),
            PolicyDirection::Write => vec![self.space_cwd.clone()],
        }
    }

    /// The error the direction's boundary check produced for a path, or
    /// `None` when the path is IN bounds. This is the single answer the gate
    /// needs: `Some` is exactly "beyond" (which a `Deny` implies), and its
    /// payload is what the rejection quotes back.
    ///
    /// Answered by the SAME canonicalize-then-check the ADR 0029 attack
    /// matrix proved (`FsBackend::validate`), so gate and executor cannot
    /// resolve `..` / a symlink differently — a gate that resolves differently
    /// than the thing it gates is the classic escape, hence ONE resolver
    /// shared by both layers rather than a second canonicalizer here. ANY
    /// error — an escape, or a root set that cannot be judged at all — means
    /// beyond, so an unjudgeable path is never silently allowed.
    fn beyond_error(&self, dir: PolicyDirection, path: &Path) -> Option<FsError> {
        let roots = self.direction_boundary(dir);
        FsBackend { roots }.validate(path).err()
    }

    /// The tool-result error for a policy DENY (never a prompt). Worded like
    /// the executor's own rejection — `FsError`'s display, prefixed with the
    /// tool name exactly as the executors prefix it — so a read the GATE
    /// denied looks identical to the model to one the executor would have
    /// rejected: the model's recovery behavior must not depend on which
    /// layer said no. A write into a user-level agent-definition dir gets the
    /// deny-list wording instead (the true reason — refused there in EVERY
    /// policy, so "escapes the sandbox" would send the model off looking for
    /// a boundary that is not the problem).
    fn deny_tool_result(&self, tc: &ToolCall) -> ToolResult {
        let dir = policed_direction(&tc.name);
        let text = match tc.arguments.get("path").and_then(|v| v.as_str()) {
            Some(raw) => {
                let path = Path::new(raw);
                // The deny-list is a WRITE-floor question (`protected_dirs`
                // are dirs a WRITE may not enter), and it is checked BEFORE
                // the boundary: the refusal is true in every policy, so its
                // wording must win over "escapes the sandbox", which would
                // send the model hunting for a boundary that is not the
                // problem.
                let protected = (dir == Some(PolicyDirection::Write))
                    .then(|| protected_refusal_text(&self.protected, &tc.name, path))
                    .flatten();
                match protected {
                    Some(msg) => msg,
                    None => self.escape_text(dir, &tc.name, path, raw),
                }
            }
            // `bash` has no path: the policy simply refused the command.
            None => format!("{}: refused by the file-access policy (Sandboxed)", tc.name),
        };
        ToolResult {
            content: vec![ContentBlock::Text { text }],
            details: None,
            is_error: true,
        }
    }

    /// The escape rejection's text, in the executor's own words: the tool
    /// name, then `FsError::PathEscape` — exactly what a `read` of the same
    /// path would have returned. The variant's `path` is the RAW argument
    /// the model passed, which is also what the executor's own rejection
    /// carries when the path canonicalizes to nothing useful; a
    /// `FsError::Io` (no boundary at all) is reported verbatim.
    fn escape_text(
        &self,
        dir: Option<PolicyDirection>,
        tool: &str,
        path: &Path,
        raw: &str,
    ) -> String {
        let dir = dir.unwrap_or(PolicyDirection::Read);
        let body = FsError::PathEscape {
            path: match self.beyond_error(dir, path) {
                // The RESOLVED form when the canonicalizer produced one — it
                // tells the user where the path actually landed (`../x` under
                // a symlinked `cwd` is not where it looks).
                Some(FsError::PathEscape { path }) => path,
                _ => raw.to_string(),
            },
        };
        format!("{tool}: {body}")
    }

    /// The permission gate (the handle-free waiter — reviewer-corrected
    /// Major #14): a tool the policy sent to `Ask` on an untrusted Space
    /// prompts (`permission-request` via the sink, a `PendingPermissions`
    /// oneshot; a deny / cancel is a denial); a TRUSTED Space is
    /// auto-approved (ADR 0010 — replicated here: the native path has no
    /// `handle_extension_ui_request` to do it). A CANCELLED turn is a
    /// `Cancelled` (a deny) — the gate consults the token BEFORE the
    /// trusted short-circuit (finding 8a).
    ///
    /// The trust check inside the gate is now REDUNDANT with
    /// `decision_for` (which returns `Allow` for a trusted Space, so an
    /// `Ask` reaching here already implies untrusted); it stays because the
    /// gate is also the seam that answers `trust-space`, and a
    /// fail-closed gate is cheaper than a clever one.
    async fn gate_tool(&self, tc: &ToolCall, turn: &CancellationToken) -> bool {
        let outcome = native_permission_gate(
            &self.session_id,
            &tc.id,
            &permission_title(&tc.name, &tc.arguments),
            &self.sink,
            &self.pending_permissions,
            self.trust.as_deref(),
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
    /// cancel }`) + the suite tools (the in-process cores of the
    /// interactive channel, called in-process — NOT over a socket) +
    /// `subagent` (a native parent spawns an IN-PROCESS native child via
    /// `dispatch_native`).
    async fn dispatch_tool(&mut self, tc: &ToolCall, turn: &CancellationToken) -> ToolResult {
        // The `enabled_tools` filter (`None` = ALL — finding 13b): a
        // disabled tool is a tool-result error, NOT executed.
        // `dispatch_subagent` is an ALIAS of `subagent` (the `match`
        // below routes both to it): the gate must know the alias — a
        // parent with `enabled_tools: Some(["subagent"])` must not get
        // `tool not enabled: dispatch_subagent` when the model emits it.
        if !self.is_tool_enabled(&tc.name) {
            return ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("tool not enabled: {}", tc.name),
                }],
                details: None,
                is_error: true,
            };
        }
        // The `subagent` / `dispatch_subagent` alias (the single source of
        // truth is `is_subagent_call` — the parallel batch + this dispatch
        // share it; a future third alias updates only that one place).
        if crate::agent::harness::dispatch::is_subagent_call(&tc.name) {
            return self.dispatch_subagent(&tc.arguments, turn).await;
        }
        match tc.name.as_str() {
            "bash" | "read" | "write" | "edit" | "find" | "grep" | "ls" | "list_skills"
            | "read_skill" => {
                execute_tool(
                    &ToolCtx {
                        cwd: self.space_cwd.clone(),
                        cancel: turn.clone(),
                        // `None` = the real discovery roots (the same
                        // `discover_skills` the prompt builder used).
                        skill_roots: None,
                        // (ADR 0030) The frozen boundary + deny-list.
                        boundary: self.read_boundary.clone(),
                        protected: self.protected.clone(),
                        // (ADR 0030 Task 3) The policy the gate decided with
                        // — the SAME per-turn value, so the executor cannot
                        // re-decide differently from the gate (it enforces
                        // the `Sandboxed` floor and confines `bash`).
                        file_policy: self.file_policy,
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
                // the SESSION teardown token + the 330 s interactive
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
            "ask" => {
                crate::agent::harness::ask::ask_flow(
                    &self.pending_bridge,
                    &self.sink,
                    &self.session_id,
                    &tc.arguments,
                    &tc.id,
                    turn,
                )
                .await
            }
            "list_agents" => self.list_agents_tool().await,
            "mcp" => {
                // RACED against the TURN token (finding 8b, round 2 — a Stop
                // mid-mcp-call cancels the in-flight request + kills the stdio
                // child; the flow's internal `cancel` is the TURN token too).
                tokio::select! {
                    r = mcp_tool(&mut self.mcp, &tc.arguments, turn) => r,
                    _ = turn.cancelled() => Self::cancelled_tool_result(),
                }
            }
            other => ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("unknown tool: {other}"),
                }],
                details: None,
                is_error: true,
            },
        }
    }

    /// Whether `name` passes the `enabled_tools` filter (`None` = all
    /// enabled). The logic lives in `harness::dispatch` (no loop state).
    fn is_tool_enabled(&self, name: &str) -> bool {
        crate::agent::harness::dispatch::is_tool_enabled(self.enabled_tools(), name)
    }

    /// `subagent` dispatch — the thin wrapper that supplies the loop's
    /// fields; the dispatch flow itself lives in `harness::dispatch`.
    async fn dispatch_subagent(&self, params: &Value, turn: &CancellationToken) -> ToolResult {
        crate::agent::harness::dispatch::dispatch_subagent(
            self.subagent.as_ref(),
            &crate::agent::harness::dispatch::SubagentParent {
                session_id: &self.session_id,
                space_cwd: &self.space_cwd,
                model: &self.model,
                enabled_tools: self.enabled_tools(),
                sink: &self.sink,
                catalog: &self.catalog,
                config_dir: self.config_dir.as_deref(),
            },
            params,
            turn,
        )
        .await
    }

    /// Resolve `agentName` against the discovered Agent definitions and
    /// layer the frontmatter UNDER the explicit launch params (ADR 0020
    /// — explicit > frontmatter > parent defaults). `agentName` empty /
    /// no match → the `launch` is returned VERBATIM (the label-only,
    /// config-less behavior — never an error).
    ///
    /// The implementation lives in `harness::launch` (a pure function of
    /// the catalog / settings dir / space cwd — no loop state); the
    /// `subagent` dispatch flow calls it directly in `harness::dispatch`.
    ///
    /// The `list_agents` tool result: one line per discovered Agent
    /// definition (ADR 0020), or `"none"`.
    async fn list_agents_tool(&self) -> ToolResult {
        let agents = crate::agents::discover_agents(Some(&self.space_cwd));
        let text = if agents.is_empty() {
            "none".to_string()
        } else {
            agents
                .iter()
                .map(|a| {
                    // `description` may be a block scalar (`|`) with embedded
                    // newlines — flatten it so the one-line-per-agent contract
                    // holds. Only `description` is flattened: a block-scalar
                    // `model` / `thinking` / `tools` is pathological (a
                    // block-scalar `model` fails `resolve_composed_model` and
                    // degrades; a block-scalar `tools` yields names that fail
                    // the known-name filter and are dropped), so no flattening
                    // is needed there.
                    let description = a.description.replace('\n', " ");
                    let mut line = format!("{} ({}): {}", a.name, a.scope, description);
                    let mut notes: Vec<String> = Vec::new();
                    if let Some(m) = &a.model {
                        notes.push(format!("model: {m}"));
                    }
                    if let Some(t) = &a.thinking {
                        notes.push(format!("thinking: {t}"));
                    }
                    if let Some(t) = &a.tools {
                        notes.push(format!("tools: {}", t.join(", ")));
                    }
                    if !notes.is_empty() {
                        line.push_str(&format!(" [{}]", notes.join(", ")));
                    }
                    line
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        ToolResult {
            content: vec![ContentBlock::Text { text }],
            details: None,
            is_error: false,
        }
    }

    /// The compaction (the `compact.rs` orchestration — the OLDER messages
    /// summarized and replaced by the summary, the transcript REWRITTEN) +
    /// the `compaction_start` / `compaction_end` `RpcEvent` emission (which
    /// needs the loop's `normalize` / display-persistence pipeline) and the
    /// post-compaction `context_usage_update` frame.
    async fn run_compaction(&mut self, turn: &CancellationToken) {
        self.emit(RpcEvent::compaction_start {
            reason: "context_limit".to_string(),
        });
        let mut aborted = false;
        let mut error_message: Option<String> = None;
        let mut ctx = compact::CompactionCtx {
            session_id: &self.session_id,
            model: &self.model,
            provider: &*self.provider,
            store: &self.store,
            keep_recent_tokens: self.catalog.compaction.keep_recent_tokens,
            compactor: &mut self.compactor,
        };
        match compact::run_compaction(&mut ctx, &mut self.messages, turn).await {
            compact::CompactionOutcome::Skipped => {}
            compact::CompactionOutcome::Compacted { context_tokens } => {
                // The re-estimate is the post-compaction BASELINE (the
                // `context_usage_update` frame — the percentage drops
                // after the compaction instead of staying at the
                // pre-compaction value).
                self.last_context_tokens = context_tokens;
                self.emit_context_usage();
            }
            compact::CompactionOutcome::Failed { error } => {
                aborted = true;
                error_message = Some(error);
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
            session_id: Some(self.session_id.clone()),
        }
    }

    /// The tool specs advertised to the model (the `enabled_tools` filter +
    /// the subagent drop for a child) — the SAME value `model_request` sends
    /// as `tools[]`, so the prompt's `<tools>` section can never disagree
    /// with the API param (ADR 0017).
    pub fn advertised_specs(&self) -> Vec<ToolSpec> {
        Self::advertised_tool_specs(&self.enabled_tools, self.subagent.is_none())
    }

    /// The tool specs advertised to the model (finding B): `tool_specs()`
    /// filtered by (a) `enabled_tools` (`None` = all; `Some(v)` = exactly
    /// `v` — `Some(vec![])` = NO tools) and (b) dropping `subagent` AND
    /// `list_agents` when the session is a subagent child (`is_child` —
    /// `subagent: None`; the child cannot dispatch subagents, so listing
    /// dispatch targets is pointless token burn — but a native parent may). The
    /// `dispatch_tool` gate remains as defense in depth.
    fn advertised_tool_specs(enabled_tools: &Option<Vec<String>>, is_child: bool) -> Vec<ToolSpec> {
        tool_specs()
            .into_iter()
            .filter(|spec| {
                let enabled = match enabled_tools {
                    Some(tools) => tools.contains(&spec.name),
                    None => true,
                };
                let not_child =
                    !is_child || (spec.name != "subagent" && spec.name != "list_agents");
                enabled && not_child
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

    /// Persist the LEADING system message (if any) at seq 0 (an idempotent
    /// upsert — the transcript record of the session's system prompt;
    /// ADR 0017). A resume replays it verbatim via `load_transcript`.
    /// The `len == 1` guard makes `prepend_system`'s "call AT MOST ONCE"
    /// contract explicit: a second call on a non-empty transcript is
    /// LOUDLY skipped (the in-memory prepend happened, but the seq-0
    /// row is left untouched — the eprintln is the only trace).
    fn persist_system_message(&self) {
        let m = match self.messages.first() {
            Some(m) if self.messages.len() == 1 => m,
            Some(_) => {
                eprintln!(
                    "harness: system message persist skipped (the transcript already has {} messages — the 'call AT MOST ONCE' contract is broken; the seq-0 row is left untouched)",
                    self.messages.len()
                );
                return;
            }
            None => return, // empty transcript — nothing to persist
        };
        if !matches!(m.role, ChatRole::System) {
            return;
        }
        let content_json = serde_json::to_string(m).unwrap_or_default();
        if let Err(e) =
            self.store
                .insert_message(&self.session_id, 0, role_str(m.role), &content_json)
        {
            eprintln!("harness: system message persist failed: {e}");
        }
    }

    /// Emit one `RpcEvent`: through the EXISTING `normalize` +
    /// `compute_display_rows` + `Store::persist_display` pipeline (the
    /// FROZEN `session-update` frames via the sink + the display
    /// persistence) AND onto the `events` channel (the raw `RpcEvent`
    /// values — the UNBOUNDED `send` never fails — ADR 0025: nothing is
    /// dropped; a closed channel (the driver gone) is a silent no-op).
    /// The settle is NOT taken from the (lossy) `events` channel: an
    /// `agent_settled` is ALSO written to the settle watch (a watch send
    /// is NEVER dropped — the driver's settle watch fires regardless, so
    /// the turn settle is never lost).
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
            let rows =
                compute_display_rows(u, &self.text_acc, &self.tool_state, &self.thought_state);
            let _ = self.store.persist_display(&self.session_id, &rows);
        }
        if matches!(ev, RpcEvent::agent_settled) {
            let n = self.settle_count.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.settle_tx.send(n);
        }
        let _ = self.events.send(ev);
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

/// The tool result's text (the first text block). TEST-ONLY (the
/// production `tool` message keeps the result's FULL `Blocks` — the
/// provider's `to_wire` is the wire shape; this is the assertion
/// helper for the `dispatch_tool` results).
#[cfg(test)]
pub(crate) fn result_text(result: &ToolResult) -> String {
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
            name: "list_skills".into(),
            description: "List the discoverable Skills (name, scope, description) — the catalog the \
                          `<skills>` system-prompt section advertises, re-read from disk."
                .into(),
            parameters: json!({ "type": "object" }),
        },
        ToolSpec {
            name: "read_skill".into(),
            description: [
                "Load a Skill's instructions by NAME — the sanctioned way to read a skill file.",
                "",
                "`name` matches a discovered Skill (see list_skills, or the `<available_skills>` \
                section); no `path` returns that skill's `SKILL.md` body. `path` reads a file \
                BUNDLED with that skill, resolved against the skill's directory — it must stay \
                inside it.",
                "",
                "Skills live OUTSIDE the session sandbox (`~/.agents/skills`, `.agents/skills`), \
                so the read tool CANNOT reach them — `read` on a skill path always fails with \
                \"path escapes the session sandbox\". Use this tool instead.",
            ]
            .join("\n"),
            parameters: json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "description": "The Skill's name" },
                    "path": {
                        "type": "string",
                        "description": "A file inside the skill's directory (e.g. a referenced ./format.md)"
                    }
                },
                "required": ["name"]
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
            description: "Delegate tasks to subagents. `task` is required. Optional: `agentName` (a discovered Agent definition — its frontmatter model/thinking/tools + system-prompt body apply, layered under any explicit params), `model`, `systemPrompt`, `tools`. Omit `agentName` for a config-less dispatch (parent model, all tools, no system prompt). Model override is rarely needed.".into(),
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
        ToolSpec {
            name: "list_agents".into(),
            description: "List available subagent configurations (name, description, source, model/tools overrides). Call before dispatching if unsure which agents exist or which fits the task.".into(),
            parameters: json!({ "type": "object" }),
        },
        ToolSpec {
            name: "mcp".into(),
            description: [
                "Gateway to MCP (Model Context Protocol) servers. Use this tool to discover and call tools from connected MCP servers.",
                "",
                "Workflow:",
                "1. Search: mcp({ search: 'keyword' }) — find available tools",
                "2. Describe: mcp({ describe: 'tool_name' }) — see full parameters",
                "3. Call: mcp({ tool: 'tool_name', args: { ... } }) — execute the tool",
                "",
                "Other actions:",
                "  Status:  mcp({}) or mcp({ action: 'status' }) — list all servers and their connection status",
                "  List:    mcp({ server: 'name' }) — list all tools on a specific server",
                "  Connect: mcp({ connect: 'name' }) — eagerly connect to a server",
                "  Auth:    mcp({ action: 'auth', server: 'name' }) — run the interactive OAuth flow",
                "",
                "Use 'server' to disambiguate when two servers export a tool with the same name.",
            ]
            .join("\n"),
            parameters: json!({
                "type": "object",
                "properties": {
                    "tool": { "type": "string", "description": "Tool name to call" },
                    "args": { "description": "Tool arguments (a JSON string or an object)" },
                    "search": { "type": "string", "description": "Search tools by name/description keyword" },
                    "describe": { "type": "string", "description": "Tool name to show full parameter schema for" },
                    "connect": { "type": "string", "description": "Server name to eagerly connect" },
                    "server": { "type": "string", "description": "Filter to a specific server (for list, search, or disambiguating calls)" },
                    "action": { "type": "string", "description": "Action string ('status' or 'auth')" }
                }
            }),
        },
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::agent::harness::catalog::CompactionConfig;
    use crate::agent::harness::dispatch::InProcessDispatcher;
    use crate::agent::harness::store::SessionStore;
    use crate::agent::subagent::{LaunchConfig, SubagentOutcome, SubagentSessionManager};
    use crate::agent::worker::client::{WorkerError, WorkerHandle, WorkerInboundEvent};
    use crate::agent::worker::manager::{WorkerFactory, WorkerManager};
    use crate::agent::worker::protocol::{StartEnv, StartMode};
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
            Box<
                dyn std::future::Future<Output = crate::agent::interactive::SudoRun>
                    + Send
                    + 'static,
            >,
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

    /// A `Provider` that records `req.session_id.clone()` into an `Arc<StdMutex<Vec<Option<String>>>>`
    /// and returns canned responses (or `text_then_done` by default).
    struct SessionRecordingProvider {
        recorded_session_ids: Arc<StdMutex<Vec<Option<String>>>>,
        scripts: Vec<Vec<ProviderEvent>>,
        call_idx: AtomicU32,
    }

    impl SessionRecordingProvider {
        fn with_scripts(
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
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
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

    /// Build an `AgentLoop` + its `Db` (the harness unit-test seam — a
    /// temp-dir `Db`, a single `fake/m1` catalog model, a scripted
    /// provider; the `events` / `turn_cancel` / `settle_tx` wiring mirrors
    /// `build_native_session`). The `Db` is returned (the tests assert on
    /// the STORE via `loop_.store` against the same `Db`).
    fn build_loop_with_db(
        provider: Box<dyn Provider>,
        events: mpsc::UnboundedSender<RpcEvent>,
        turn_cancel: Arc<StdMutex<CancellationToken>>,
        settle_tx: watch::Sender<u64>,
        retry: RetryPolicy,
        models: Vec<Model>,
        config_dir: Option<&std::path::Path>,
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
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        });
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
        );
        (loop_, db)
    }

    /// Build an `AgentLoop` (drops the `Db` — the tests that need the
    /// `Db` use `build_loop_with_db`).
    fn build_loop(
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

    /// A text-only `Prompt` (the test's common case — no image
    /// attachments).
    fn text_prompt(text: &str) -> Prompt {
        Prompt {
            text: text.to_string(),
            images: Vec::new(),
        }
    }

    /// Await the first event matching `pred` (bounded — the tests must
    /// not hang).
    async fn wait_for_event(
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

    /// (finding 3 — ADR 0025's UNBOUNDED `events` channel): a slow /
    /// blocked consumer NEVER drops an event (the pre-fix bounded channel
    /// dropped the `agent_settled` delivery); the settle watch fires
    /// regardless — the driver's settle (and the `send_prompt` /
    /// `wait_for_settle` waits it drives) is never lost.
    #[tokio::test]
    async fn a_slow_events_consumer_never_drops_the_settle() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        for _ in 0..4 {
            events_tx.send(RpcEvent::turn_start).unwrap();
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
        loop_.handle_prompt(&text_prompt("hello")).await;
        // The raw `agent_settled` was NOT dropped (unbounded — the
        // pre-filled events are still ahead of it in the queue; EVERY
        // event is delivered, in order).
        let mut seen = Vec::new();
        while let Ok(ev) = events_rx.try_recv() {
            seen.push(ev);
        }
        assert_eq!(
            seen.first().map(|e| e.kind()),
            Some("turn_start".into()),
            "the pre-filled events are still at the head (nothing was dropped)"
        );
        assert_eq!(
            seen.last().map(|e| e.kind()),
            Some("agent_settled".into()),
            "the settled event was delivered (unbounded — no drop)"
        );
        // ...and the settle watch fired (the reliable signal).
        assert_eq!(*settle_rx.borrow(), 1, "the settle watch fired");
    }

    /// (finding 8a) A turn with 2 tool calls: a cancel after the first
    /// call (the `ask` blocks until the cancel) → the second is NOT
    /// executed (a cancelled tool result; the turn settles).
    #[tokio::test]
    async fn a_cancel_stops_the_remaining_tool_calls_in_a_batch() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, calls) = ScriptedProvider::new(vec![None]); // a HANGING `complete`
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        loop_.handle_prompt(&text_prompt("hello")).await;
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        loop_.send_prompt("hello", &[]);
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        loop_.handle_prompt(&text_prompt("hello")).await;
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
    /// only on the SESSION teardown token + the 330 s interactive timeout,
    /// so a Stop waited out up to 330 s, and a confirm answered AFTER
    /// the Stop still EXECUTED the elevated command). The command is
    /// NEVER run.
    #[tokio::test]
    async fn a_cancel_stops_a_sudo_flow_blocking_on_its_confirm_prompt() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
        // The `sudo_exec` is now blocking on its confirm sub-prompt
        // (never answered in the test).
        tokio::time::sleep(Duration::from_millis(500)).await;
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect(
                "the turn settled promptly (it did not wait out the 330 s interactive timeout)",
            );
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        loop_.handle_prompt(&text_prompt("hello")).await;
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("hello")).await });
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

    /// (finding 4) `run_compaction` over a failing summary: the
    /// transcript is UNCHANGED (a failed compaction is never a lost
    /// context — the `compaction_end` is `aborted` with the error).
    #[tokio::test]
    async fn a_failed_summary_does_not_rewrite_the_transcript() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("partial".to_string()),
            ProviderEvent::Done(FinishReason::Error),
        ])]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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

    /// (ADR 0017) `prepend_system` PERSISTS the leading system message at
    /// seq 0 (the transcript record of the session's system prompt — a
    /// resume replays it verbatim via `load_transcript`). Asserted on the
    /// STORE (`load_messages` returns the rows in seq order — the seq-0
    /// row is `loaded[0]`).
    #[tokio::test]
    async fn prepend_system_persists_at_seq_zero() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (mut loop_, _db) = build_loop_with_db(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            vec![fake_model("m1")],
            None,
        );
        loop_.prepend_system("the prompt".to_string());
        let loaded = loop_.store.load_messages("s1").unwrap();
        assert_eq!(loaded.len(), 1, "the system message was persisted at seq 0");
        let m = &loaded[0];
        assert!(
            matches!(m.role, ChatRole::System),
            "the persisted row is the system message, got {:?}",
            m.role
        );
        assert_eq!(
            m.content,
            MessageContent::Text("the prompt".to_string()),
            "the persisted content is the prompt text"
        );
    }

    // ── `context_usage_update` frames (the frontend's context-percentage
    // display: the session's current context size vs the model's window —
    // emitted via the sink, NOT the `normalize`/`persist_update` pipeline
    // (a bookkeeping frame, like the client-synthesized
    // `config_option_update`)). ──

    /// The `updates` of the LAST `context_usage_update` frame (the tests
    /// assert on the most recent frame — the frames are cumulative).
    fn last_context_usage_frame(updates: &StdMutex<Vec<Value>>) -> Option<Value> {
        updates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .rev()
            .find(|u| u["sessionUpdate"] == "context_usage_update")
            .cloned()
    }

    /// A provider `Usage` event re-anchors the context to what the provider holds:
    /// `input + output` — the full prompt INCLUDING the reply it just
    /// produced — plus anything appended since (the same number pi counts),
    /// and emits a `context_usage_update` frame (the window is the session
    /// model's `context_window`).
    #[tokio::test]
    async fn a_usage_event_emits_a_context_usage_update_frame() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Usage(Usage {
                input_tokens: 4321,
                output_tokens: 100,
            }),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("hello")).await;
        let frame =
            last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
        assert_eq!(
            frame["usedTokens"], 4421,
            "the context is the usage anchor (input + output)"
        );
        assert_eq!(frame["windowTokens"], 128000, "the window is the model's");
    }

    /// A model call that reports NO `Usage` (a proxy that strips the
    /// usage chunk — the pre-`stream_options` openai-completions wire,
    /// or a non-conformant endpoint) must still move the context bar:
    /// the turn falls back to the LOCAL estimate of the transcript, so
    /// the display — and, through the same `Compactor`, the compaction
    /// threshold — track the growing context instead of freezing at the
    /// value the session started with.
    #[tokio::test]
    async fn a_model_call_without_usage_falls_back_to_the_local_estimate() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        // A BIG prompt, and a stream that never reports usage.
        let prompt = "x".repeat(4000);
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt(&prompt)).await;
        let frame =
            last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
        assert!(
            frame["usedTokens"].as_u64().unwrap_or(0) > 900,
            "a usage-less turn re-estimates from the transcript (the prompt is \
             ~1000 tokens), got {}",
            frame["usedTokens"]
        );
        assert_eq!(
            frame["usedTokens"],
            loop_.compactor.context_tokens(),
            "the fallback estimate is the SAME source the compaction threshold reads"
        );
    }

    /// The P1 review fix: a usage-less turn must never move the context
    /// accounting BACKWARDS. A session that mixes reporting and
    /// non-reporting calls (a proxy that emits the usage chunk on SOME
    /// responses) used to risk having the authoritative usage-based count
    /// replaced by the smaller `chars / 4` transcript estimate on the gapped
    /// calls — the bar visibly shrinking and the compaction threshold metric
    /// dropping (compaction delayed while the session grew). The guarantee is
    /// now STRUCTURAL rather than a floor-raiser: the anchor only moves when
    /// the provider reports, and a gapped `refresh` merely adds the trailing
    /// estimate on top of it, so it cannot shrink the accounting.
    #[tokio::test]
    async fn a_usage_less_turn_never_lowers_the_context_accounting() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        // Turn 1 reports usage (the authoritative, LARGE count); turn 2
        // reports NONE and its transcript estimate is far smaller.
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::TextDelta("one".to_string()),
                ProviderEvent::Usage(Usage {
                    input_tokens: 40_000,
                    output_tokens: 100,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("two".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("first")).await;
        let after_usage = last_context_usage_frame(&updates).expect("frame after turn 1");
        let authoritative = after_usage["usedTokens"].as_u64().unwrap();
        assert_eq!(
            authoritative, 40_100,
            "turn 1 anchors on the provider's report (input + output)"
        );
        loop_.handle_prompt(&text_prompt("second")).await;
        let after_gap = last_context_usage_frame(&updates).expect("frame after turn 2");
        assert!(
            after_gap["usedTokens"].as_u64().unwrap_or(0) >= authoritative,
            "the usage-less turn must not shrink the bar below the anchor: {} < {authoritative}",
            after_gap["usedTokens"]
        );
        assert!(
            loop_.compactor.context_tokens() >= authoritative,
            "the compaction threshold metric must not drop on a usage-less turn \
             (it would delay compaction), got {}",
            loop_.compactor.context_tokens()
        );
    }

    /// A model whose compaction threshold is exactly `context_window -
    /// 16384` (the DEFAULT reserve the `Compactor` is built with — the
    /// config is fixed at `AgentLoop::new`, so the knob a test can turn
    /// AFTER construction is the window): a threshold of `t` is a window of
    /// `16384 + t`.
    fn model_with_threshold(id: &str, threshold: u32) -> Model {
        Model {
            context_window: 16_384 + threshold,
            ..fake_model(id)
        }
    }

    /// The kinds of every `RpcEvent` the loop emitted, in order (the tests
    /// assert on the ORDER of the turn's events, not just their presence).
    fn event_kinds(rx: &mut mpsc::UnboundedReceiver<RpcEvent>) -> Vec<String> {
        let mut kinds = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            kinds.push(ev.kind().to_string());
        }
        kinds
    }

    /// The tool batch must be MEASURED before the next threshold check.
    ///
    /// `should_compact()` is read at the TOP of the iteration, so anything
    /// appended after the last re-anchor is invisible to it — and tool
    /// results are by far the largest messages in an agentic transcript (a
    /// `read`, a `grep`, a `cat` of a doc set). Pre-fix the batch was pushed
    /// without re-anchoring, so the ENTIRE batch's output was unmeasured when
    /// the next check ran: on a usage-less endpoint (anchor 0 — the fallback
    /// path that exists precisely for those endpoints) the metric lagged a
    /// whole batch, and one batch of huge output could blow past the window
    /// undetected so the provider 400s instead of compacting.
    ///
    /// The arithmetic: threshold 100 (window `16384 + 100`, the default
    /// 16384 reserve). Prompt `"go"` = 2/4 + 1 = `1`, the assistant's `"ok"`
    /// + one tool call = 0 + 20 + 1 = `21` → 22 measured at the top of the
    /// next iteration: under the threshold. The `read` result is 1200 chars =
    /// 300 + 1 = `301` → the batch takes the context to 323, clearly over it.
    /// So the SECOND iteration must see the compaction due — which it can
    /// only do if the batch was measured when it was appended.
    #[tokio::test]
    async fn a_tool_batch_is_measured_before_the_next_compaction_check() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        // NO `Usage` anywhere — the anchor stays 0 and the local estimate is
        // the ONLY measurement (the usage-less-endpoint case).
        let (provider, calls) = ScriptedProvider::new(vec![
            // Call 1: one tool call whose result is huge.
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "big.txt" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            // The summary call (the compaction the batch must trigger).
            Some(vec![
                ProviderEvent::TextDelta("SUMMARY".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
            // The turn's final reply.
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![model_with_threshold("m1", 100)],
            None,
        );
        // Keep only 1 token of tail, so the compaction is a REAL one (the
        // older messages are summarized) rather than a skip.
        loop_.catalog.compaction.keep_recent_tokens = 1;
        // The tool's input: a 1200-char file (300 tokens by the local
        // estimate) the `read` returns verbatim.
        std::fs::write(loop_.space_cwd.join("big.txt"), "x".repeat(1200)).unwrap();
        loop_.handle_prompt(&text_prompt("go")).await;

        let kinds = event_kinds(&mut events_rx);
        assert!(
            kinds.contains(&"compaction_start".to_string()),
            "the tool batch (301 tokens) takes the context past the threshold, so the \
             NEXT iteration's `should_compact()` must be due — the batch was appended \
             without re-anchoring and stayed invisible, got {kinds:?}"
        );
        assert!(
            kinds.contains(&"compaction_end".to_string()),
            "the compaction ran to completion, got {kinds:?}"
        );
        assert!(
            kinds.contains(&"agent_settled".to_string()),
            "the turn still settles, got {kinds:?}"
        );
        // The batch is ALSO what the display saw: the frames recorded it
        // (the prompt alone would have been a ~1-token bar).
        let frame =
            last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
        assert!(
            frame["usedTokens"].as_u64().unwrap_or(0) > 100,
            "the batch is above the threshold in the frame too, got {}",
            frame["usedTokens"]
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "the turn is the tool call, the summary, and the final reply"
        );
    }

    /// A large PASTED prompt must be counted before the turn's first
    /// threshold check (the same one-call lag as the tool batch, on the other
    /// side of the turn): the push used to leave the `Compactor` untouched, so
    /// compaction that was ALREADY due was deferred by a whole model call —
    /// the call that had to send the oversized context to the provider.
    ///
    /// Threshold 100, prompt 4000 chars = 1000 + 1 = `1001` — the prompt ALONE
    /// crosses it, on a provider that reports NO usage. Post-fix the very
    /// first thing after `turn_start` is the compaction (nothing older to
    /// summarize yet, so it is a skip — the point is that the threshold READ
    /// is due), and the FIRST frame of the turn is the prompt's own estimate
    /// and nothing else (pre-fix it would be the prompt PLUS the assistant
    /// reply, because the first frame only came after that reply was pushed).
    #[tokio::test]
    async fn a_large_prompt_is_measured_before_the_first_compaction_check() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let prompt_text = "x".repeat(4000);
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![model_with_threshold("m1", 100)],
            None,
        );
        loop_.handle_prompt(&text_prompt(&prompt_text)).await;

        // The prompt's own local estimate (the same function the threshold
        // uses): 4000 / 4 + 1 for the role overhead.
        let prompt_estimate = compact::estimate_context(&loop_.messages[..1]);
        assert_eq!(prompt_estimate, 1001, "the prompt alone is 1001 tokens");
        let kinds = event_kinds(&mut events_rx);
        // The compaction check fired BEFORE the first model call — i.e. the
        // prompt was in the number the threshold read.
        let compaction_at = kinds.iter().position(|k| k == "compaction_start");
        let first_model_call = kinds.iter().position(|k| k == "message_start");
        assert!(
            matches!(
                (compaction_at, first_model_call),
                (Some(c), Some(m)) if c < m
            ),
            "a prompt that alone crosses the threshold must be due at the turn's FIRST \
             check, before the oversized context is sent to the provider, got {kinds:?}"
        );
        let frames: Vec<Value> = updates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .rev()
            .filter(|u| u["sessionUpdate"] == "context_usage_update")
            .cloned()
            .collect();
        let first = frames
            .last()
            .expect("the prompt push itself emitted a context_usage_update frame");
        assert_eq!(
            first["usedTokens"].as_u64(),
            Some(prompt_estimate),
            "the prompt was measured the moment it was pushed — the frame at that point \
             is the prompt alone, got {}",
            first["usedTokens"]
        );
        // One source, one number: the LAST frame is what the threshold reads.
        let last = frames.first().expect("at least one frame");
        assert_eq!(
            last["usedTokens"].as_u64(),
            Some(loop_.compactor.context_tokens()),
            "the frame and the compaction threshold read ONE number"
        );
    }

    /// A sink that interleaves the `RpcEvent` stream with the `session-update`
    /// frames it receives: on every emission it first drains the event channel
    /// into the same log, so the log is the turn's TRUE emission order across
    /// BOTH channels (the context frame bypasses the `RpcEvent` pipeline, so its
    /// position relative to `turn_start` is otherwise unobservable).
    struct OrderingSink {
        log: Arc<StdMutex<Vec<String>>>,
        events: Arc<StdMutex<mpsc::UnboundedReceiver<RpcEvent>>>,
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

    /// The turn's opening event opens the turn: the prompt-time context frame is
    /// session-scoped bookkeeping, so it belongs AFTER `turn_start`, not before
    /// it. The measurement itself stays where it must be — before the turn's
    /// first `should_compact()` read.
    #[tokio::test]
    async fn the_context_frame_of_a_turn_follows_turn_start() {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let log = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(OrderingSink {
            log: log.clone(),
            events: Arc::new(StdMutex::new(events_rx)),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![model_with_threshold("m1", 100)],
            None,
        );
        loop_.handle_prompt(&text_prompt(&"x".repeat(4000))).await;
        let log = log.lock().unwrap_or_else(|p| p.into_inner()).clone();
        let turn_start_at = log
            .iter()
            .position(|k| k == "turn_start")
            .expect("the turn opened with `turn_start`");
        let frame_at = log
            .iter()
            .position(|k| k == "context_usage_update")
            .expect("the prompt push emitted the context frame");
        assert!(
            turn_start_at < frame_at,
            "every turn opens with its own `turn_start`, THEN the context frame that \
             describes it — not a frame ahead of the turn it belongs to: {log:?}"
        );
        // And the real guarantee (commit `a846abb`) survives the reordering: the
        // prompt was measured before the threshold was read, so the compaction
        // check fires before the oversized context is sent.
        let compaction_at = log
            .iter()
            .position(|k| k == "compaction_start")
            .expect("a prompt that alone crosses the threshold is due at the FIRST check");
        let model_call_at = log
            .iter()
            .position(|k| k == "message_start")
            .expect("the model call");
        assert!(
            compaction_at < model_call_at,
            "the compaction is checked BEFORE the oversized context goes out: {log:?}"
        );
    }

    /// A turn whose model call REPORTED usage must emit ONE
    /// `context_usage_update` frame, not two. The `Usage` arm is the
    /// authoritative re-anchor (it sets the number from the provider's own
    /// report and emits), and by the time the assistant message is pushed the
    /// watermark equals the transcript length — the trailing slice is EMPTY,
    /// so `note_context`'s refresh recomputes the SAME number. Re-emitting it
    /// is a duplicate sink frame AND a duplicate `record_context_usage` (an IPC
    /// round trip → an `ensure_session_row` SELECT + an `UPDATE` on the main
    /// process's `Db` mutex), once per iteration, for nothing — and this is the
    /// COMMON path (every usage-reporting endpoint).
    #[tokio::test]
    async fn a_turn_that_reported_usage_emits_the_context_frame_once() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Usage(Usage {
                input_tokens: 4321,
                output_tokens: 100,
            }),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("hello")).await;
        let frames: Vec<Value> = updates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|u| u["sessionUpdate"] == "context_usage_update")
            .cloned()
            .collect();
        assert_eq!(
            frames.len(),
            2,
            "two REAL changes — the prompt push, then the provider's report — and the \
             assistant push that follows the report changes NOTHING, so it must not write a \
             frame (nor a `sessions` row): {frames:?}"
        );
        assert_eq!(
            frames[1]["usedTokens"], 4421,
            "the last frame is the provider's own anchor"
        );
        assert_ne!(
            frames[0]["usedTokens"], frames[1]["usedTokens"],
            "no two frames of a turn carry the same number"
        );
        assert_eq!(
            loop_.compactor.context_tokens(),
            4421,
            "and the threshold reads the same number"
        );
    }

    /// …while a turn that reported NO usage still moves the number (the
    /// assistant push grows the estimate), so its frame is untouched by the
    /// emit-if-changed guard above.
    #[tokio::test]
    async fn a_turn_without_usage_emits_a_frame_per_context_change() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("a somewhat longer reply".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("hello")).await;
        let frames: Vec<Value> = updates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|u| u["sessionUpdate"] == "context_usage_update")
            .cloned()
            .collect();
        assert_eq!(
            frames.len(),
            2,
            "the prompt push and the assistant push are two REAL changes on a \
             usage-less endpoint — both must still be recorded: {frames:?}"
        );
        let prompt_only = frames[0]["usedTokens"].as_u64().unwrap();
        let with_reply = frames[1]["usedTokens"].as_u64().unwrap();
        assert!(
            with_reply > prompt_only,
            "the reply grew the estimate ({prompt_only} → {with_reply})"
        );
    }

    /// A `Provider` whose FIRST call reports a `Usage` and then STALLS
    /// forever, cancelling the TURN token the moment the loop asks for the
    /// next event: the stream-consume `select!` takes the `Usage` (the one
    /// event that lands — nothing is cancelled yet, so that arm is
    /// unopposed) and then the cancel arm wins deterministically (the stream
    /// never becomes ready again) → `settle_cancelled` with NO assistant
    /// message pushed, which is exactly what a Stop does to a partial reply.
    /// Later calls reply normally with no usage (the turn AFTER the cancel).
    struct CancelAfterUsageProvider {
        calls: AtomicU32,
        turn_cancel: Arc<StdMutex<CancellationToken>>,
    }

    #[async_trait::async_trait]
    impl Provider for CancelAfterUsageProvider {
        async fn complete(
            &self,
            _req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                return Ok(futures_util::stream::iter(vec![
                    ProviderEvent::TextDelta("hi".to_string()),
                    ProviderEvent::Done(FinishReason::Stop),
                ])
                .boxed());
            }
            let token = self
                .turn_cancel
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            Ok(futures_util::stream::unfold(0u8, move |s| {
                let token = token.clone();
                async move {
                    match s {
                        0 => Some((
                            ProviderEvent::Usage(Usage {
                                input_tokens: 90,
                                output_tokens: 50,
                            }),
                            1u8,
                        )),
                        // The loop asks for the NEXT event: that is when the
                        // Stop lands, so the usage above is always recorded
                        // before the turn settles.
                        _ => {
                            token.cancel();
                            std::future::pending::<()>().await;
                            None
                        }
                    }
                }
            })
            .boxed())
        }
    }

    /// A turn that ENDS WITHOUT the assistant message (here: the provider
    /// reported a `Usage`, then the stream died and the retry budget
    /// exhausted) leaves the `Compactor`'s watermark pointing PAST the reply
    /// that was never pushed — a failed (or cancelled) turn deliberately does
    /// NOT persist a partial reply. The next turn's prompt then lands INSIDE
    /// the watermark window, so it is excluded from the trailing estimate: the
    /// very lag commit `a846abb` exists to prevent, resurrected for the
    /// post-failure case, and the anchor still carries the discarded reply's
    /// output.
    ///
    /// The arithmetic: threshold 300 (`model_with_threshold`), prompt `"go"`
    /// = 1, the failed call reports `input 90 + output 50` = a 140 anchor over
    /// a 1-message transcript, so the watermark claims 2 messages. The NEXT
    /// prompt is 4000 chars = 1001. Left unreconciled the snapshot is the bare
    /// 140 — under the 300 threshold, so no compaction and the oversized
    /// context goes to the provider. Reconciled it is `90 + 1001 = 1091`
    /// (the discarded 50 of output is gone with the reply) and the SECOND
    /// turn's first threshold check is due.
    #[tokio::test]
    async fn a_failed_turn_still_measures_the_next_prompt() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        // Call 1 (turn 1): a usage report, then `Done(Error)` — with a
        // ONE-attempt budget the turn settles FAILED after that single call,
        // never pushing a reply. Call 2 (turn 2): an ordinary usage-less
        // reply.
        let (provider, calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::Usage(Usage {
                    input_tokens: 90,
                    output_tokens: 50,
                }),
                ProviderEvent::Done(FinishReason::Error),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            // ONE attempt: the mid-stream error exhausts the budget at once,
            // so turn 1 is exactly one model call (no backoff sleeps).
            RetryPolicy::new_with(1, Duration::from_millis(1)),
            sink,
            None,
            vec![model_with_threshold("m1", 300)],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        assert!(
            !event_kinds(&mut events_rx).contains(&"compaction_start".to_string()),
            "the failed turn itself is not due (140 < 300)"
        );
        assert_eq!(
            loop_.messages.len(),
            1,
            "a failed turn persists the prompt ONLY — no assistant reply"
        );

        loop_.handle_prompt(&text_prompt(&"x".repeat(4000))).await;

        let kinds = event_kinds(&mut events_rx);
        assert!(
            kinds.contains(&"compaction_start".to_string()),
            "the SECOND turn must measure the prompt its predecessor's watermark \
             swallowed — an abandoned reply must not make a 1091-token context look \
             like a 140-token one, got {kinds:?}"
        );
        let prompt_estimate = compact::estimate_context(&loop_.messages[1..2]);
        assert_eq!(prompt_estimate, 1001, "the prompt alone is 1001 tokens");
        let frames: Vec<Value> = updates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|u| u["sessionUpdate"] == "context_usage_update")
            .cloned()
            .collect();
        assert!(
            frames
                .iter()
                .any(|f| f["usedTokens"].as_u64() == Some(90 + prompt_estimate)),
            "the prompt is measured on top of the ANCHOR LESS the discarded reply's \
             output (90 + {prompt_estimate}), got {frames:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one failed call, then turn 2's reply"
        );
    }

    /// The same reconcile on the CANCEL exit (the case that motivated it — a
    /// Stop drops the partial reply, it is never persisted). Same arithmetic
    /// as the failed-turn test: the reported `output_tokens` of the reply that
    /// never landed must leave the anchor, and the watermark must pull back so
    /// the next prompt is measured.
    #[tokio::test]
    async fn a_cancelled_turn_still_measures_the_next_prompt() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let provider = CancelAfterUsageProvider {
            calls: AtomicU32::new(0),
            turn_cancel: turn_cancel.clone(),
        };
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![model_with_threshold("m1", 300)],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        assert!(
            !event_kinds(&mut events_rx).contains(&"compaction_start".to_string()),
            "the cancelled turn itself is not due (140 < 300)"
        );
        assert_eq!(
            loop_.messages.len(),
            1,
            "a cancelled turn persists the prompt ONLY — the partial reply is NOT \
             persisted, which is what makes the watermark wrong"
        );
        // The provider cancelled the TURN token (that is how it makes the cancel
        // arm win), so the test re-arms it before the next prompt. It has to:
        // `handle_prompt` does install a fresh token, but only AFTER its
        // Stop-pending guard — which sees the cancelled token, drains the queue,
        // settles, and SKIPS the prompt. In the live loop `run()` re-arms a
        // stale idle token between turns, so a later prompt is a FRESH turn.
        *turn_cancel.lock().unwrap_or_else(|p| p.into_inner()) = CancellationToken::new();

        loop_.handle_prompt(&text_prompt(&"x".repeat(4000))).await;

        let kinds = event_kinds(&mut events_rx);
        assert!(
            kinds.contains(&"compaction_start".to_string()),
            "the prompt after a cancelled turn must be measured before that turn's \
             first threshold check, got {kinds:?}"
        );
        let prompt_estimate = compact::estimate_context(&loop_.messages[1..2]);
        let frames: Vec<Value> = updates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|u| u["sessionUpdate"] == "context_usage_update")
            .cloned()
            .collect();
        assert!(
            frames
                .iter()
                .any(|f| f["usedTokens"].as_u64() == Some(90 + prompt_estimate)),
            "the anchor dropped the cancelled reply's output (90 + {prompt_estimate}), \
             got {frames:?}"
        );
    }

    /// The reconcile is needed INSIDE the turn as well: the mid-stream retry
    /// loop-back is the one path that returns to `should_compact()` WITHOUT
    /// exiting the turn, so the settle paths' coverage cannot reach it. A
    /// stream that reports a `Usage` and THEN dies retryably takes that branch
    /// — it pushes NO assistant message (the partial reply is discarded and
    /// re-generated), yet the watermark `record_usage` wrote still claims the
    /// reply landed and the anchor still carries the discarded reply's
    /// `output_tokens`. Pre-fix the very next iteration's threshold check read
    /// that phantom output — tokens the provider generated for a reply it threw
    /// away — so a dying reply large enough to cross `window - reserve`
    /// compacted the session BEFORE the retry's own (successful) call:
    /// "Compacting context…" for a context nobody holds, the exact symptom this
    /// metric exists to kill.
    ///
    /// The arithmetic: threshold 100 (`model_with_threshold`), and the dying
    /// call reports `input 90 + output 50` = a 140 anchor over a 1-message
    /// transcript. So the phantom 140 is OVER the threshold while the honest 90
    /// is UNDER it: the observable is `compaction_start` itself, not an
    /// internal field.
    #[tokio::test]
    async fn a_retried_model_call_does_not_compact_for_the_reply_it_threw_away() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        // Call 1: a usage report, THEN the stream dies retryably
        // (`Done(Error)`) — the reply is discarded, no assistant message is
        // pushed. Call 2: the retry succeeds.
        let (provider, calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::Usage(Usage {
                    input_tokens: 90,
                    output_tokens: 50,
                }),
                ProviderEvent::Done(FinishReason::Error),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            // TWO attempts: the mid-stream error retries ONCE and the retry
            // succeeds (a 1 ms backoff), so the turn ends CLEANLY — no
            // `settle_error` to reconcile for it.
            RetryPolicy::new_with(2, Duration::from_millis(1)),
            sink,
            None,
            vec![model_with_threshold("m1", 100)],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;

        let kinds = event_kinds(&mut events_rx);
        assert!(
            !kinds.contains(&"compaction_start".to_string()),
            "the threshold must NOT read the 50 tokens of output the retry threw away: the \
             context is 90 (not due at 100), the phantom is 140 (due) — a compaction here \
             was triggered by a reply that was never kept: {kinds:?}"
        );
        assert!(
            kinds.contains(&"auto_retry_start".to_string()),
            "the dying stream took the mid-stream retry — the loop-back under test: {kinds:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "the retry really re-issued the call"
        );
        // …and the same number the threshold just read is the one the bar shows.
        assert_eq!(
            loop_.compactor.context_tokens(),
            90 + compact::estimate_context(&loop_.messages[1..]),
            "the anchor is the SURVIVING reply on the 90 the provider still holds, not \
             140 (the discarded attempt's output)"
        );
    }

    /// `load_transcript` (a resume) re-estimates the context from the
    /// loaded messages and emits the frame — a resumed session shows its
    /// context percentage BEFORE the first turn.
    #[tokio::test]
    async fn load_transcript_emits_a_context_usage_update_from_the_reestimate() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.load_transcript(vec![
            ChatMessage {
                role: ChatRole::User,
                content: MessageContent::Text("u1".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text("a1".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
        ]);
        let frame =
            last_context_usage_frame(&updates).expect("a context_usage_update frame was emitted");
        // "u1" / "a1": 2 chars / 4 = 0 each +1 (the role overhead) = 2.
        assert_eq!(frame["usedTokens"], 2, "the re-estimated context size");
        assert_eq!(frame["windowTokens"], 128000);
    }

    /// A compaction rewrites the transcript (the older messages replaced
    /// by the summary) — the re-estimated (post-compaction) context size
    /// is emitted, so the percentage drops after the compaction instead
    /// of staying at the pre-compaction value.
    #[tokio::test]
    async fn a_compaction_emits_a_context_usage_update_from_the_reestimate() {
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("the summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.catalog.compaction.keep_recent_tokens = 1;
        loop_.load_transcript(vec![
            ChatMessage {
                role: ChatRole::User,
                content: MessageContent::Text("u1".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text("a1".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            ChatMessage {
                role: ChatRole::User,
                content: MessageContent::Text("u2".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text("a2".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
        ]);
        loop_.run_compaction(&CancellationToken::new()).await;
        let frame = last_context_usage_frame(&updates)
            .expect("a context_usage_update frame was emitted after the compaction");
        // The emitted value is the compactor's re-estimated (post-compaction)
        // context size — the same source the compaction threshold reads.
        assert_eq!(frame["usedTokens"], loop_.compactor.context_tokens());
        assert_eq!(frame["windowTokens"], 128000);
    }

    /// A model switch changes the WINDOW (the context — the messages — is
    /// unchanged) AND rebuilds the `Compactor`, which drops the usage anchor.
    /// The frame, the tracked count, and the threshold the next turn reads must
    /// all agree the moment the switch happens: a switch that emits the OLD
    /// number while `should_compact()` reads the freshly-reset `0` is a
    /// transient breach of ADR 0028's "one number, two consumers", and switching
    /// INTO a smaller window defers an already-due compaction by a whole prompt
    /// (the call that has to send the oversized context).
    #[tokio::test]
    async fn a_model_switch_emits_a_context_usage_update_with_the_new_window() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let updates = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(TestSink {
            updates: updates.clone(),
        });
        let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
        // The switch TARGET is a model whose compaction threshold is 100 (the
        // default 16384 reserve), so a 1001-token transcript is already due.
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            None,
            vec![fake_model("m1")],
            None,
        );
        loop_.load_transcript(vec![ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("x".repeat(4000)),
            tool_call_id: None,
            tool_calls: None,
        }]);
        let before = last_context_usage_frame(&updates).expect("a frame from the resume");
        assert_eq!(before["usedTokens"], 1001, "the resumed context");
        assert_eq!(
            before["windowTokens"], 128000,
            "the window is the old model's"
        );
        let switched = Model {
            context_window: 16_384 + 100,
            ..fake_model("m2")
        };
        loop_.set_model(switched);
        let frame = last_context_usage_frame(&updates)
            .expect("a context_usage_update frame was emitted on the model switch");
        assert_eq!(
            frame["usedTokens"], 1001,
            "the frame re-estimates the SAME transcript the switch left the `Compactor` \
             with — it must not show a number the threshold no longer reads"
        );
        assert_eq!(
            frame["windowTokens"], 16_484,
            "the window is the new model's"
        );
        assert_eq!(
            loop_.compactor.context_tokens(),
            frame["usedTokens"].as_u64().unwrap(),
            "one number, two consumers: the bar and the threshold agree IMMEDIATELY \
             (before the next turn's `note_context`)"
        );
        assert!(
            loop_.compactor.should_compact(),
            "a transcript already over the NEW window's threshold must be due the moment \
             the switch happens — not one prompt later"
        );
        // …and the next turn acts on it: the threshold is read at the top of
        // the iteration, so the switch's own number is what fires.
        loop_.catalog.compaction.keep_recent_tokens = 1;
        loop_.handle_prompt(&text_prompt("go")).await;
        let kinds = event_kinds(&mut events_rx);
        assert!(
            kinds.contains(&"compaction_start".to_string()),
            "the switch to a smaller window compacts instead of sending a context it \
             already knows is too large, got {kinds:?}"
        );
    }

    /// (ADR 0017) a resume REPLAYS the stored system message verbatim
    /// (the `load_transcript` of the stored rows restores it
    /// byte-identically — same role, same content JSON).
    #[tokio::test]
    async fn resume_replays_system_message_verbatim() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("hi".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (mut loop_, _db) = build_loop_with_db(
            Box::new(provider),
            mpsc::unbounded_channel().0,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            vec![fake_model("m1")],
            None,
        );
        loop_.prepend_system("the prompt".to_string());
        let loaded = loop_.store.load_messages("s1").unwrap();
        loop_.load_transcript(loaded.clone());
        assert_eq!(
            loop_.messages.len(),
            1,
            "the replayed transcript has the stored row"
        );
        assert!(
            matches!(loop_.messages[0].role, ChatRole::System),
            "the replayed message is the system message"
        );
        assert_eq!(
            loop_.messages[0].content,
            MessageContent::Text("the prompt".to_string()),
            "the replayed content is the prompt text"
        );
        assert_eq!(
            serde_json::to_string(&loop_.messages[0]).unwrap(),
            serde_json::to_string(&loaded[0]).unwrap(),
            "the replayed message is byte-identical to the stored row"
        );
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
            mpsc::unbounded_channel().0,
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
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
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
        loop_.handle_prompt(&text_prompt("hello")).await;
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
            mpsc::unbounded_channel().0,
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
    fn exec_param_keys() -> [(&'static str, &'static [&'static str]); 9] {
        [
            ("bash", &["command", "timeout_ms"]),
            ("read", &["path", "offset", "limit"]),
            ("write", &["path", "content"]),
            ("edit", &["path", "old_text", "new_text", "replace_all"]),
            ("find", &["pattern", "path", "max_results"]),
            ("grep", &["pattern", "path", "glob", "max_results", "-i"]),
            ("ls", &["path", "long"]),
            ("list_skills", &[]),
            ("read_skill", &["name", "path"]),
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

    /// `HOME` restore guard for the `list_agents_tool` tests (a scope-exit
    /// `Drop` — the Task 1 `RestoreHome` pattern: the restore happens even
    /// when an assertion panics mid-test).
    struct RestoreHome(Option<std::ffi::OsString>);
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

    /// (ADR 0020) `list_agents` is advertised to a native PARENT: it is in
    /// `tool_specs()` with its description, and in the parent's advertised
    /// specs (the `enabled_tools` filter `None` = all).
    #[test]
    fn list_agents_is_in_the_default_tool_specs() {
        let specs = tool_specs();
        let spec = specs
            .iter()
            .find(|s| s.name == "list_agents")
            .expect("`list_agents` in `tool_specs`");
        assert_eq!(
            spec.description,
            "List available subagent configurations (name, description, source, model/tools overrides). Call before dispatching if unsure which agents exist or which fits the task."
        );
        let advertised = AgentLoop::advertised_tool_specs(&None, false);
        assert!(
            advertised.iter().any(|s| s.name == "list_agents"),
            "a native parent advertises `list_agents`"
        );
    }

    /// (ADR 0020) a subagent CHILD gets neither `subagent` (the recursion
    /// guard) nor `list_agents` (a child cannot dispatch, so listing
    /// dispatch targets is pointless token burn). The CONTRAST assertions
    /// (the parent advertises BOTH) are what keep this test from passing
    /// VACUOUSLY — without them, "the child drops `list_agents`" is
    /// trivially true while the spec is not in `tool_specs()` yet.
    #[test]
    fn a_child_does_not_advertise_subagent_or_list_agents() {
        let child = AgentLoop::advertised_tool_specs(&None, true);
        assert!(
            !child.iter().any(|s| s.name == "subagent"),
            "a child does not advertise `subagent`"
        );
        assert!(
            !child.iter().any(|s| s.name == "list_agents"),
            "a child does not advertise `list_agents`"
        );
        // CONTRAST: the parent advertises BOTH.
        let parent = AgentLoop::advertised_tool_specs(&None, false);
        assert!(
            parent.iter().any(|s| s.name == "subagent"),
            "a parent advertises `subagent` (contrast)"
        );
        assert!(
            parent.iter().any(|s| s.name == "list_agents"),
            "a parent advertises `list_agents` (contrast)"
        );
    }

    /// (ADR 0020) the `list_agents` handler lists the DISCOVERED Agent
    /// definitions (one line each: `name (scope): description [+ the
    /// model / thinking / tools notes]`). The user-level roots are
    /// ISOLATED first (a developer's real `~/.agents/agents` /
    /// `~/.pi/agent/agents` files would add lines and break the exact
    /// assertion: hold `ENV_LOCK` for the WHOLE set→assert→restore span;
    /// `HOME` → an empty scratch; restore via the drop guard).
    /// `#[allow(clippy::await_holding_lock)]` is INTENTIONAL: the handler reads
    /// `HOME` (via `user_roots`), so the guard must stay held ACROSS the
    /// `.await` to serialize against the other HOME-reading tests — dropping
    /// it before the await would open a window where a sibling test could
    /// flip `HOME` mid-discovery.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn list_agents_tool_lists_discovered_agents() {
        let (provider, _calls) = ScriptedProvider::new(vec![]);
        let (events_tx, _events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (settle_tx, _settle_rx) = watch::channel(0u64);
        let loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel,
            settle_tx,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let empty_home =
            std::env::temp_dir().join(format!("harness-list-agents-home-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&empty_home).unwrap();
        // The `env_lock` helper is poison-tolerant (a sibling test
        // panicking while holding the lock must not turn this test's
        // failure into an opaque `PoisonError` panic — see its docs).
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span (the `env_lock` guard); no other thread mutates HOME
        // concurrently.
        unsafe {
            std::env::set_var("HOME", &empty_home);
        }
        let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
        if let Some(parent) = agent_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &agent_file,
            "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\ntools: [read, bash]\n---\nYou are a scout.\n",
        )
        .unwrap();
        let result = loop_.list_agents_tool().await;
        assert_eq!(
            result_text(&result),
            "scout (space): Fast recon. [model: fake/m2, tools: read, bash]"
        );
    }

    /// (ADR 0020) with NO discovered definitions, the `list_agents` result
    /// is `none` (same `ENV_LOCK` + `HOME`-to-empty-scratch isolation —
    /// the developer's user-level files must not appear; the guard is held
    /// across the `.await` for the same reason as above — `#[allow]` is
    /// intentional).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn list_agents_tool_returns_none_when_empty() {
        let (provider, _calls) = ScriptedProvider::new(vec![]);
        let (events_tx, _events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (settle_tx, _settle_rx) = watch::channel(0u64);
        let loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel,
            settle_tx,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let empty_home =
            std::env::temp_dir().join(format!("harness-list-agents-home-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&empty_home).unwrap();
        // The `env_lock` helper is poison-tolerant (a sibling test
        // panicking while holding the lock must not turn this test's
        // failure into an opaque `PoisonError` panic — see its docs).
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span (the `env_lock` guard); no other thread mutates HOME
        // concurrently.
        unsafe {
            std::env::set_var("HOME", &empty_home);
        }
        let result = loop_.list_agents_tool().await;
        assert_eq!(result_text(&result), "none");
    }

    /// (ADR 0020) a block-scalar `description` (`|` with embedded
    /// newlines) is FLATTENED to one line in the `list_agents` output
    /// (the one-line-per-agent contract holds — the block's internal
    /// newlines become single spaces). The agent is written to the
    /// space-level `.pi/agents` root (the other space root,
    /// `.agents/agents`, is covered by the sibling test above); same
    /// `ENV_LOCK` + `HOME`-to-empty-scratch isolation (the developer's
    /// user-level files must not appear; the guard is held across the
    /// `.await` — `#[allow]` is intentional).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn list_agents_tool_flattens_block_scalar_descriptions() {
        let (provider, _calls) = ScriptedProvider::new(vec![]);
        let (events_tx, _events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (settle_tx, _settle_rx) = watch::channel(0u64);
        let loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel,
            settle_tx,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        let empty_home =
            std::env::temp_dir().join(format!("harness-list-agents-home-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&empty_home).unwrap();
        // The `env_lock` helper is poison-tolerant (a sibling test
        // panicking while holding the lock must not turn this test's
        // failure into an opaque `PoisonError` panic — see its docs).
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span (the `env_lock` guard); no other thread mutates HOME
        // concurrently.
        unsafe {
            std::env::set_var("HOME", &empty_home);
        }
        // The space-level `.pi/agents` root (the other space root,
        // `.agents/agents`, is covered by the sibling test above).
        let agent_file = loop_.space_cwd.join(".pi/agents/scout.md");
        if let Some(parent) = agent_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &agent_file,
            "---\nname: scout\ndescription: |\n  line one\n  line two\n---\nYou are a scout.\n",
        )
        .unwrap();
        let result = loop_.list_agents_tool().await;
        let text = result_text(&result);
        assert!(
            text.contains("line one line two"),
            "a block-scalar description is flattened to one line: {text:?}"
        );
        // The one-line-per-agent contract holds: a raw embedded newline
        // leaking from the block scalar would split the output into
        // extra lines (we wrote exactly one agent).
        assert_eq!(
            text.lines().count(),
            1,
            "one line per agent (a leaked block-scalar newline would add lines): {text:?}"
        );
    }

    /// A `Model` for a given bare id (a `fake` provider — the unit
    /// tests' catalog entry; the single-model `build_loop` catalog is
    /// `fake/m1`, so a frontmatter `model: fake/m2` is STALE by
    /// construction — exactly the degrade case).
    fn fake_model(id: &str) -> Model {
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
    fn build_loop_with_dispatcher(
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
        )
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

    /// A `WorkerManager` with the loop's parent session (`"s1"` — the
    /// `AgentLoop`'s `session_id`) `attach`ed (the ADR 0025 Task 5
    /// re-plumb — the subagent runs in a `fake_worker` process): the
    /// parent's `StartEnv` carries the given `models` catalog (the
    /// `dispatch_subagent` flow resolves the child's `model` against it)
    /// and the given `config_dir` (the settings' `subagentModels`
    /// override is resolved loop-side; the `config_dir` is the
    /// parent's). The `sink_for` lookup returns the given sink for the
    /// parent (the `subagent-session-started` / `subagent-closed` UI
    /// lifecycle events are delivered on the PARENT's sink).
    async fn make_worker_manager(
        settle_timeout: Duration,
        models: Vec<Model>,
        config_dir: Option<&std::path::Path>,
        sink: Arc<dyn EventSink>,
    ) -> Arc<WorkerManager> {
        let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
        // The `on_event` closure mirrors the router's `SinkFrame` re-emit
        // (the child's `SinkFrame`s — the `session_info` Start echo with
        // the `systemPrompt` / `trusted` envelope fields — land on the
        // test's sink; the `RpcEvent` stream is a no-op here).
        let on_event_sink = sink.clone();
        let wm = WorkerManager::new(
            factory,
            Arc::new(|_id: String, _code: Option<i32>| {}),
            Arc::new(
                move |_id: String, is_subagent: bool, evt: WorkerInboundEvent| {
                    if is_subagent {
                        if let WorkerInboundEvent::SinkFrame { event, payload } = &evt {
                            on_event_sink.emit(event, payload.clone());
                        }
                    }
                },
            ),
            Arc::new(move |id: &str| (id == "s1").then(|| sink.clone())),
        )
        .with_settle_timeout(settle_timeout);
        let wm = Arc::new(wm);
        let env = StartEnv::from_parts(
            "s1".to_string(),
            "/tmp/space".to_string(),
            StartMode::Fresh,
            None,
            models[0].clone(),
            ModelCatalog {
                models,
                default_model: None,
                compaction: CompactionConfig::default(),
            },
            None,
            true,
            None,
            config_dir
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| "/tmp".to_string()),
            true,
            None,
        );
        wm.attach("s1", &env)
            .await
            .expect("the parent attach completes");
        wm
    }

    /// A `SubagentSessionManager` on a `WorkerManager` (the ADR 0025
    /// Task 5 re-plumb — the `InProcessDispatcher`'s manager): the parent
    /// `"s1"` is `attach`ed with the given `models` catalog (the
    /// `dispatch_subagent` flow resolves the child's `model` against it).
    async fn make_subagent_manager(
        settle_timeout: Duration,
        models: Vec<Model>,
        config_dir: Option<&std::path::Path>,
        sink: Arc<dyn EventSink>,
    ) -> Arc<SubagentSessionManager> {
        let wm = make_worker_manager(settle_timeout, models, config_dir, sink).await;
        let manager = Arc::new(SubagentSessionManager::new());
        manager.set_worker_manager(wm);
        manager
    }

    /// A recording `EventSink` (pushes EVERY event's `(name, payload)`
    /// — the `TestSink` captures only `session-update` and cannot see
    /// `subagent-session-started`).
    struct RecordingSink {
        events: Arc<StdMutex<Vec<(String, Value)>>>,
    }

    impl EventSink for RecordingSink {
        fn emit(&self, event: &str, payload: Value) {
            self.events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((event.to_string(), payload));
        }
    }

    /// (ADR 0023 thinking-order regression) the `:<level>` suffix of the
    /// resolved model key beats the frontmatter's `thinking` (the
    /// doc-correct order — explicit > suffix > frontmatter; the OLD code
    /// produced the frontmatter's `"low"` here). The ADR 0025 Task 5
    /// re-plumb: the subagent runs in a `fake_worker` process (the
    /// `WorkerManager`'s `dispatch_subagent` flow) — the
    /// `subagent-session-started` frame carries the RESOLVED model +
    /// thinking, and the `subagent` tool result is the child's captured
    /// final text (the `fake_worker`'s canned `"canned answer"`).
    #[tokio::test]
    async fn a_native_subagent_with_a_suffix_model_key_uses_the_suffix_thinking() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "recon the auth code", "agentName": "scout" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        // A TWO-model catalog: `fake/m1` (the parent) + `fake/m2` (the
        // frontmatter target — IN the catalog, so it is NOT stale).
        let models = vec![fake_model("m1"), fake_model("m2")];
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
            events: sink_events.clone(),
        });
        let manager =
            make_subagent_manager(Duration::from_secs(30), models.clone(), None, sink.clone())
                .await;
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
            models,
            None,
        );
        // The Agent definition: `model: fake/m2:high` (IN the catalog —
        // the `:<level>` suffix is stripped for the resolvability check)
        // + `thinking: low` (the LAST rung — the suffix beats it).
        let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
        if let Some(parent) = agent_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &agent_file,
            "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2:high\nthinking: low\n---\nYou are a scout.\n",
        )
        .unwrap();
        loop_.handle_prompt(&text_prompt("go")).await;
        // The `subagent` tool result: the child's captured final text
        // (the `fake_worker`'s canned `"canned answer"` — the
        // `SubagentCapture` over the child's `SinkFrame`
        // `agent_message_chunk` stream).
        let end = wait_for_event(&mut events_rx, 20000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the `subagent`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(!is_error, "the child completed");
        assert_eq!(
            result["content"][0]["text"], "canned answer",
            "the tool result is the child's captured final text"
        );
        // The parent turn settled (bounded — the `ScriptedProvider`
        // settles it; the child ran to completion inside the dispatch).
        wait_for_event(&mut events_rx, 20000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the parent turn settled");
        // The `subagent-session-started` frame: the suffix's `high`
        // beats the frontmatter's `low` (the doc-correct order), and
        // the model is the suffix-STRIPPED key.
        let events = sink_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let started = events
            .iter()
            .find(|(name, _)| name == "subagent-session-started")
            .expect("the `subagent-session-started` frame was emitted");
        assert_eq!(
            started.1["model"], "fake/m2",
            "the suffix is stripped for the model resolution"
        );
        assert_eq!(
            started.1["thinkingLevel"], "high",
            "the suffix beats the frontmatter's `thinking` (the order fix)"
        );
    }

    /// (ADR 0023) end-to-end: a settings `subagentModels` override beats
    /// the frontmatter `model` (the child runs the override), and the
    /// frontmatter's `thinking` STILL applies (the override only
    /// replaces the model). The ADR 0025 Task 5 re-plumb: the subagent
    /// runs in a `fake_worker` process — the `subagent-session-started`
    /// frame carries the RESOLVED model + thinking + tools, and the
    /// `subagent` tool result is the child's captured final text (the
    /// `fake_worker`'s canned `"canned answer"`).
    #[tokio::test]
    async fn a_native_subagent_with_a_settings_override_runs_the_override_model() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "recon the auth code", "agentName": "scout" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        // A THREE-model catalog: `fake/m1` (the parent) + `fake/m2` (the
        // frontmatter target) + `fake/m3` (the override target — IN the
        // catalog, so it is NOT stale).
        let models = vec![fake_model("m1"), fake_model("m2"), fake_model("m3")];
        let config_dir = std::env::temp_dir().join(format!(
            "harness-agent-def-e2e-cfg-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{ "subagentModels": { "scout": "fake/m3" } }"#,
        )
        .unwrap();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
            events: sink_events.clone(),
        });
        let manager = make_subagent_manager(
            Duration::from_secs(30),
            models.clone(),
            Some(&config_dir),
            sink.clone(),
        )
        .await;
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
            models,
            Some(&config_dir),
        );
        // The Agent definition: `model: fake/m2` (IN the catalog — NOT
        // stale) + `thinking: low` (the override replaces ONLY the
        // model — the thinking still comes from the file).
        let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
        if let Some(parent) = agent_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &agent_file,
            "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
        )
        .unwrap();
        loop_.handle_prompt(&text_prompt("go")).await;
        // The `subagent` tool result: the child's captured final text
        // (the `fake_worker`'s canned `"canned answer"`).
        let end = wait_for_event(&mut events_rx, 20000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the `subagent`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(!is_error, "the child completed");
        assert_eq!(
            result["content"][0]["text"], "canned answer",
            "the tool result is the child's captured final text"
        );
        // The parent turn settled (bounded — the `ScriptedProvider`
        // settles it; the child ran to completion inside the dispatch).
        wait_for_event(&mut events_rx, 20000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the parent turn settled");
        // The `subagent-session-started` frame: the override beats the
        // frontmatter's model, the frontmatter's `thinking` still
        // applies, and the tools are the frontmatter's allowlist.
        let events = sink_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let started = events
            .iter()
            .find(|(name, _)| name == "subagent-session-started")
            .expect("the `subagent-session-started` frame was emitted");
        assert_eq!(
            started.1["model"], "fake/m3",
            "the settings override beats the frontmatter model"
        );
        assert_eq!(
            started.1["thinkingLevel"], "low",
            "the frontmatter's `thinking` still applies (the override only replaces the model)"
        );
        assert_eq!(
            started.1["enabledTools"],
            json!(["read"]),
            "the child's tools are the frontmatter's allowlist"
        );
    }

    /// (ADR 0020) End-to-end: a native subagent dispatched with an
    /// `agentName` matching a discovered definition runs the FRONTMATTER
    /// config (the `dispatch_subagent` → `resolve_launch` layering — the
    /// child's `subagent-session-started` payload carries the
    /// frontmatter's `model` + `tools`, and the child's `Start` envelope
    /// (the `fake_worker`'s `session_info` echo) carries the frontmatter
    /// body as the `systemPrompt` + exactly the frontmatter's tools).
    /// The ADR 0025 Task 5 re-plumb: the subagent runs in a `fake_worker`
    /// process — the `subagent` tool result is the child's captured
    /// final text (the `fake_worker`'s canned `"canned answer"`).
    ///
    /// No `env_lock()` here (intentional): `resolve_launch` reads `HOME`
    /// (via `discover_agents` → `user_roots`), concurrently with the
    /// `HOME`-mutating tests — but the isolation is safe by construction:
    /// (1) the space-level roots are scanned FIRST (first-wins dedupe),
    /// so a user-level `scout` file can never shadow the space-level
    /// `scout` written below; (2) the mutators' scratch homes contain no
    /// agent files. If this test ever asserts on a USER-level definition,
    /// it must take `env_lock()` (and set `HOME`) first.
    #[tokio::test]
    async fn a_native_subagent_with_a_matching_agent_name_runs_the_frontmatter_config() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "recon the auth code", "agentName": "scout" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        // A TWO-model catalog: `fake/m1` (the parent) + `fake/m2` (the
        // frontmatter target — IN the catalog, so it is NOT stale).
        let models = vec![fake_model("m1"), fake_model("m2")];
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
            events: sink_events.clone(),
        });
        let manager =
            make_subagent_manager(Duration::from_secs(30), models.clone(), None, sink.clone())
                .await;
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            sink,
            Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
            models,
            None,
        );
        // The Agent definition (the loop's `space_cwd` is the Space root
        // — the frontmatter's `model` is IN the catalog, so it is NOT
        // stale).
        let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
        if let Some(parent) = agent_file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(
            &agent_file,
            "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
        )
        .unwrap();
        loop_.handle_prompt(&text_prompt("go")).await;
        // The parent turn settled (bounded — the `ScriptedProvider`
        // settles it; the child ran to completion inside the dispatch).
        wait_for_event(&mut events_rx, 20000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the parent turn settled");
        // The `subagent-session-started` frame (the `TestSink` cannot
        // see it — the `RecordingSink` records EVERY event): the child's
        // RESOLVED model / tools / thinking carry the frontmatter.
        let events = sink_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let started = events
            .iter()
            .find(|(name, _)| name == "subagent-session-started")
            .expect("the `subagent-session-started` frame was emitted");
        assert_eq!(
            started.1["model"], "fake/m2",
            "the child runs the frontmatter's model (the catalog's `fake/m2`)"
        );
        assert_eq!(
            started.1["enabledTools"],
            json!(["read"]),
            "the child's tools are the frontmatter's allowlist"
        );
        assert_eq!(
            started.1["thinkingLevel"], "low",
            "the child's thinking level is the frontmatter's"
        );
        // The child's `Start` envelope (the `fake_worker`'s `session_info`
        // echo — re-emitted on the parent's scope by the `on_event`
        // router): the frontmatter body is the `systemPrompt` (the
        // `build_child_system_message` output — the body + the todo
        // instructions), and the `enabledTools` is exactly the
        // frontmatter's.
        let child_info = events
            .iter()
            .find(|(name, payload)| {
                name == "session-update"
                    && payload["update"]["sessionUpdate"] == "session_info"
                    && payload["update"]["systemPrompt"].is_string()
            })
            .expect("the child's `session_info` Start echo was re-emitted");
        assert!(
            child_info.1["update"]["systemPrompt"]
                .as_str()
                .unwrap()
                .starts_with("You are a scout."),
            "the frontmatter body is the child's system message (the `build_child_system_message` output)"
        );
        assert_eq!(
            child_info.1["update"]["enabledTools"],
            json!(["read"]),
            "the child gets exactly the frontmatter's tool"
        );
    }

    /// `SubagentWait` mechanics (the `MockDispatcher` seam — a QUEUED
    /// oneshot): a resolving `Completed` outcome completes the `subagent`
    /// tool with the captured output (the `select!`'s outcome arm fires
    /// before the turn-cancel arm).
    #[tokio::test]
    async fn subagent_wait_a_resolving_completed_outcome_completes_the_tool() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "the task" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let dispatcher = crate::agent::harness::dispatch::MockDispatcher::new();
        let (tx, rx) = tokio::sync::oneshot::channel();
        dispatcher.queue(rx, crate::agent::subagent::SubagentCancel::none());
        // The test resolves the QUEUED oneshot BEFORE the turn starts (the
        // oneshot's value is buffered — the loop's `select!` sees it
        // resolved immediately when it pops it at `dispatch`).
        tx.send(SubagentOutcome::Completed {
            output: "the answer".to_string(),
            metrics: crate::agent::subagent::SubagentMetrics {
                output: "the answer".to_string(),
                input_tokens: 10,
                output_tokens: 5,
                cost: 0.0,
                duration_ms: 1,
            },
        })
        .unwrap();
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        // The `subagent` tool result: the `Completed` output verbatim.
        let end = wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the `subagent`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(!is_error, "a `Completed` outcome is a success result");
        assert_eq!(
            result["content"][0]["text"], "the answer",
            "the tool result is the `Completed` output verbatim"
        );
        wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
    }

    /// A test `SubagentDispatcher` that simulates a SLOW subagent (each
    /// `dispatch` resolves after `delay`) while counting how many subagents
    /// are in-flight CONCURRENTLY (the parallel-dispatch assertion — the max
    /// in-flight count is N for a concurrent batch, 1 for sequential).
    struct ConcurrentCountingDispatcher {
        delay: Duration,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ConcurrentCountingDispatcher {
        fn new(delay: Duration) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
            let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            (
                Self {
                    delay,
                    in_flight: in_flight.clone(),
                    max_in_flight: max_in_flight.clone(),
                },
                max_in_flight,
            )
        }
    }

    impl SubagentDispatcher for ConcurrentCountingDispatcher {
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
            crate::agent::subagent::SubagentCancel,
        ) {
            let in_flight = self.in_flight.clone();
            let max_in_flight = self.max_in_flight.clone();
            let c = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            loop {
                let m = max_in_flight.load(Ordering::SeqCst);
                if c <= m {
                    break;
                }
                if max_in_flight
                    .compare_exchange(m, c, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            let delay = self.delay;
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = tx.send(SubagentOutcome::Completed {
                    output: "done".to_string(),
                    metrics: crate::agent::subagent::SubagentMetrics {
                        output: "done".to_string(),
                        input_tokens: 1,
                        output_tokens: 1,
                        cost: 0.0,
                        duration_ms: 1,
                    },
                });
                in_flight.fetch_sub(1, Ordering::SeqCst);
            });
            (rx, crate::agent::subagent::SubagentCancel::none())
        }
    }

    /// ADR 0025 follow-up: the `subagent` tool calls in ONE assistant
    /// message run CONCURRENTLY (the model-loop tool execution batches them
    /// via `join_all` — N subagents take ~1× the individual duration, not
    /// N×). The assertion: the max CONCURRENT in-flight subagent count is
    /// ≥ 2 (a sequential loop would peak at 1).
    #[tokio::test]
    async fn subagent_calls_in_one_message_run_in_parallel() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "a" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "b" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t3".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "c" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (dispatcher, max_in_flight) =
            ConcurrentCountingDispatcher::new(Duration::from_millis(50));
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
        // CONCURRENT dispatch: ≥ 2 subagents were in-flight at the same
        // time (a sequential loop would peak at 1).
        assert!(
            max_in_flight.load(Ordering::SeqCst) >= 2,
            "the subagent tool calls ran concurrently (max in-flight = {}), \n\
             not sequentially (which would peak at 1)",
            max_in_flight.load(Ordering::SeqCst)
        );
    }

    /// A test `SubagentDispatcher` for the CANCEL-during-batch test: each
    /// `dispatch` returns a NEVER-resolving oneshot (the subagent stays
    /// in-flight) + a `SubagentCancel::Flag` whose flip records the cancel.
    /// The test stores the watch receivers so it can assert `cancel.cancel()`
    /// fired for every in-flight subagent when the turn is cancelled.
    struct CancellingDispatcher {
        cancels: Arc<StdMutex<Vec<watch::Receiver<bool>>>>,
    }

    impl CancellingDispatcher {
        fn new() -> (Self, Arc<StdMutex<Vec<watch::Receiver<bool>>>>) {
            let cancels = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    cancels: cancels.clone(),
                },
                cancels,
            )
        }
    }

    impl SubagentDispatcher for CancellingDispatcher {
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
            crate::agent::subagent::SubagentCancel,
        ) {
            let (flag_tx, flag_rx) = watch::channel(false);
            self.cancels.lock().unwrap().push(flag_rx);
            // The oneshot sender is FORGOTTEN (never sent, never dropped) so
            // the receiver stays PENDING — the subagent remains in-flight
            // until the turn's `select!` `turn.cancelled()` arm fires.
            let (os_tx, os_rx) = tokio::sync::oneshot::channel();
            std::mem::forget(os_tx);
            (
                os_rx,
                crate::agent::subagent::SubagentCancel::new_flag(flag_tx),
            )
        }
    }

    /// A test `SubagentDispatcher` for the ORDERING test: a subagent whose
    /// `task` contains `"a"` (tool call `t1`) resolves SLOW (300 ms); one
    /// whose `task` contains `"b"` (tool call `t2`) resolves FAST (20 ms) —
    /// so `t2` completes FIRST. It records the COMPLETION order so the test
    /// can prove the append-order reordering is real (not vacuous).
    struct OutOfOrderDispatcher {
        completion_order: Arc<StdMutex<Vec<String>>>,
    }

    impl OutOfOrderDispatcher {
        fn new() -> (Self, Arc<StdMutex<Vec<String>>>) {
            let completion_order = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    completion_order: completion_order.clone(),
                },
                completion_order,
            )
        }
    }

    impl SubagentDispatcher for OutOfOrderDispatcher {
        fn dispatch(
            &self,
            _parent_session_id: &str,
            _parent_cwd: &std::path::Path,
            _parent_model: &Model,
            _parent_enabled_tools: Vec<String>,
            _agent_name: String,
            _launch: LaunchConfig,
            task: String,
            _sink: &Arc<dyn EventSink>,
        ) -> (
            tokio::sync::oneshot::Receiver<SubagentOutcome>,
            crate::agent::subagent::SubagentCancel,
        ) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let delay = if task.contains('a') {
                Duration::from_millis(300)
            } else {
                Duration::from_millis(20)
            };
            let completion_order = self.completion_order.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                completion_order.lock().unwrap().push(task.clone());
                let _ = tx.send(SubagentOutcome::Completed {
                    output: task.clone(),
                    metrics: crate::agent::subagent::SubagentMetrics {
                        output: task.clone(),
                        input_tokens: 1,
                        output_tokens: 1,
                        cost: 0.0,
                        duration_ms: 1,
                    },
                });
            });
            (rx, crate::agent::subagent::SubagentCancel::none())
        }
    }

    /// Parallel-batch CANCEL safety: when the turn is cancelled while N
    /// subagents are in-flight, EVERY in-flight subagent's `cancel.cancel()`
    /// fires (the `select!`'s `turn.cancelled()` arm) and the turn settles
    /// (the `join_all` unwinds through all N cancel arms — no hang on a slow
    /// / never-resolving worker).
    #[tokio::test]
    async fn subagent_batch_a_turn_cancel_cancels_all_in_flight_subagents() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "a" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "b" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (dispatcher, cancels) = CancellingDispatcher::new();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        // The `join_all` batch blocks (the oneshots never resolve) until the
        // turn is cancelled — so drive it in a task.
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
        // Wait (up to 5 s) for BOTH subagents to be dispatched (in-flight).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while cancels.lock().unwrap().len() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            cancels.lock().unwrap().len(),
            2,
            "both subagents were dispatched (in-flight) before the cancel"
        );
        // Cancel the turn — the in-flight `select!`s must fire.
        turn_cancel.lock().unwrap().cancel();
        // The turn must SETTLE (the `join_all` unwinds through both cancel
        // arms — it does NOT hang on the never-resolving oneshots).
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the turn settled after the cancel (no hang)");
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("agent_settled");
        // BOTH in-flight subagents were cancelled (`cancel.cancel()` fired for
        // each — the watch flag flipped to `true`).
        let flipped = cancels
            .lock()
            .unwrap()
            .iter()
            .filter(|r| *r.borrow())
            .count();
        assert_eq!(
            flipped, 2,
            "every in-flight subagent's `cancel.cancel()` fired on a turn cancel"
        );
    }

    /// Parallel-batch ORDERING: a subagent that completes FIRST (t2, fast)
    /// must still be appended AFTER a slower one (t1) — the results are
    /// appended in the ORIGINAL `tool_calls` order, not completion order.
    #[tokio::test]
    async fn subagent_batch_results_are_appended_in_original_order_regardless_of_completion_order()
    {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "a" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "b" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (dispatcher, completion_order) = OutOfOrderDispatcher::new();
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        // Collect the `tool_execution_end` order (drain until `agent_settled`).
        let mut order = Vec::new();
        while let Ok(e) = events_rx.try_recv() {
            if let RpcEvent::tool_execution_end {
                ref tool_call_id, ..
            } = e
            {
                order.push(tool_call_id.clone());
            }
            if matches!(e, RpcEvent::agent_settled) {
                break;
            }
        }
        // PROVE the reordering is real: the fast one (t2 / "b") completed
        // FIRST (so appending t1-then-t2 is a genuine reordering, not a
        // vacuous pass where t1 happened to finish first).
        assert_eq!(
            *completion_order.lock().unwrap(),
            vec!["b".to_string(), "a".to_string()],
            "t2 (the fast one) completed before t1 — the append-order test is not vacuous"
        );
        assert_eq!(
            order,
            vec!["t1".to_string(), "t2".to_string()],
            "results appended in the ORIGINAL `tool_calls` order (t1 before t2), even though t2 (the fast one) completed first"
        );
    }

    /// Parallel-batch MIXED message: a `subagent` call + a non-`subagent`
    /// tool (`ls`, ungated) in ONE assistant message — the `subagent` is
    /// batched (Phase 1), the `ls` runs sequentially (Phase 2); BOTH results
    /// are appended in the original order and the turn settles.
    #[tokio::test]
    async fn subagent_batch_mixed_with_a_non_subagent_tool_runs_both_in_order() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "a" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "ls".to_string(),
                    arguments: json!({ "path": "." }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (dispatcher, _max) = ConcurrentCountingDispatcher::new(Duration::from_millis(20));
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        let mut order = Vec::new();
        while let Ok(e) = events_rx.try_recv() {
            if let RpcEvent::tool_execution_end {
                ref tool_call_id, ..
            } = e
            {
                order.push(tool_call_id.clone());
            }
            if matches!(e, RpcEvent::agent_settled) {
                break;
            }
        }
        assert_eq!(
            order,
            vec!["t1".to_string(), "t2".to_string()],
            "both the `subagent` (batched) and the `ls` (sequential) ran, in the original `tool_calls` order"
        );
    }

    /// A test `SubagentDispatcher` for the COMPLETED-result-survives-Stop
    /// test: a subagent whose `task` contains `"a"` (t1) resolves FAST (20 ms,
    /// `Completed`); one whose `task` contains `"b"` (t2) NEVER resolves
    /// (stays in-flight until the turn is cancelled).
    struct PartialResolveDispatcher;

    impl SubagentDispatcher for PartialResolveDispatcher {
        fn dispatch(
            &self,
            _parent_session_id: &str,
            _parent_cwd: &std::path::Path,
            _parent_model: &Model,
            _parent_enabled_tools: Vec<String>,
            _agent_name: String,
            _launch: LaunchConfig,
            task: String,
            _sink: &Arc<dyn EventSink>,
        ) -> (
            tokio::sync::oneshot::Receiver<SubagentOutcome>,
            crate::agent::subagent::SubagentCancel,
        ) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if task.contains('a') {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let _ = tx.send(SubagentOutcome::Completed {
                        output: "t1-done".to_string(),
                        metrics: crate::agent::subagent::SubagentMetrics {
                            output: "t1-done".to_string(),
                            input_tokens: 1,
                            output_tokens: 1,
                            cost: 0.0,
                            duration_ms: 1,
                        },
                    });
                });
            } else {
                // t2: the oneshot sender is FORGOTTEN (never sent) so the
                // receiver stays PENDING — t2 stays in-flight until cancelled.
                std::mem::forget(tx);
            }
            (rx, crate::agent::subagent::SubagentCancel::none())
        }
    }

    /// Parallel-batch CANCEL: a subagent that COMPLETED before a mid-batch
    /// Stop keeps its real (`Completed`) result — it is NOT replaced by
    /// `"cancelled"` (the fix-#1 core behavior). A still-in-flight subagent
    /// IS `"cancelled"`.
    #[tokio::test]
    async fn subagent_batch_a_completed_result_survives_a_mid_batch_stop() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "a" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "b" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(PartialResolveDispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        // The `join_all` batch blocks (t2 never resolves) until the turn is
        // cancelled — so drive it in a task.
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
        // Let t1 complete (its `Completed` result is cached in the batch);
        // t2 stays in-flight.
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Cancel the turn — t2 (in-flight) is cancelled; t1's result is cached.
        turn_cancel.lock().unwrap().cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the turn settled after the cancel");
        // Collect the tool results (drain until `agent_settled` — the
        // `tool_execution_end` events are emitted BEFORE the settle, so a
        // `wait_for_event` first would have consumed them and the drain
        // below would see nothing).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut results: HashMap<String, Value> = HashMap::new();
        loop {
            if tokio::time::Instant::now() >= deadline {
                panic!("agent_settled not seen within 5 s");
            }
            match tokio::time::timeout(Duration::from_millis(100), events_rx.recv()).await {
                Ok(Some(e)) => {
                    if let RpcEvent::tool_execution_end {
                        ref tool_call_id,
                        ref result,
                        ..
                    } = e
                    {
                        results.insert(tool_call_id.clone(), result.clone());
                    }
                    if matches!(e, RpcEvent::agent_settled) {
                        break;
                    }
                }
                Ok(None) | Err(_) => {
                    panic!("the event channel closed / timed out before agent_settled");
                }
            }
        }
        // t1 (completed before the Stop) keeps its REAL result, not "cancelled".
        assert_eq!(
            results
                .get("t1")
                .and_then(|v| v["content"][0]["text"].as_str()),
            Some("t1-done"),
            "a completed subagent's result survives a mid-batch Stop (fix #1)"
        );
        // t2 (in-flight at the Stop) is "cancelled".
        assert_eq!(
            results
                .get("t2")
                .and_then(|v| v["content"][0]["text"].as_str()),
            Some("cancelled"),
            "an in-flight subagent is 'cancelled' on a mid-batch Stop"
        );
    }

    /// `SubagentWait` mechanics: a `Failed` outcome is an ERROR tool
    /// result (the `subagent failed: {error}` text — the parent can
    /// retry / reword the task).
    #[tokio::test]
    async fn subagent_wait_a_failed_outcome_is_an_error_result() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "subagent".to_string(),
                    arguments: json!({ "task": "the task" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let dispatcher = crate::agent::harness::dispatch::MockDispatcher::new();
        let (tx, rx) = tokio::sync::oneshot::channel();
        dispatcher.queue(rx, crate::agent::subagent::SubagentCancel::none());
        // The test resolves the QUEUED oneshot BEFORE the turn starts
        // (the oneshot's value is buffered — the loop's `select!` sees
        // it resolved immediately when it pops it at `dispatch`).
        tx.send(SubagentOutcome::Failed {
            error: "boom".to_string(),
        })
        .unwrap();
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        loop_.handle_prompt(&text_prompt("go")).await;
        // The `subagent` tool result: the `Failed` error, verbatim.
        let end = wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the `subagent`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(is_error, "a `Failed` outcome is an error result");
        assert_eq!(
            result["content"][0]["text"], "subagent failed: boom",
            "the tool result carries the `Failed` error verbatim"
        );
        wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
    }

    /// `SubagentWait` mechanics: an UNRESOLVED outcome (the `MockDispatcher`
    /// empty queue — a never-resolving oneshot) + a turn cancel → the
    /// `select!`'s turn-cancel arm wins (the `SubagentCancel` is flipped
    /// — a no-op here — and the tool result is `cancelled`).
    #[tokio::test]
    async fn subagent_wait_a_turn_cancel_cancels_the_dispatch() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ])]);
        // The EMPTY queue — the `dispatch` returns a never-resolving
        // oneshot (the outcome arm never fires).
        let dispatcher = crate::agent::harness::dispatch::MockDispatcher::new();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let turn_cancel_c = turn_cancel.clone();
        let mut loop_ = build_loop_with_dispatcher(
            Box::new(provider),
            events_tx,
            turn_cancel,
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            Arc::new(TestSink {
                updates: Arc::new(StdMutex::new(Vec::new())),
            }),
            Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
            vec![fake_model("m1")],
            None,
        );
        // The turn runs in a task (`handle_prompt` awaits the WHOLE turn
        // — the `SubagentWait` select blocks on the never-resolving
        // oneshot until the turn-cancel arm fires).
        let turn = tokio::spawn(async move {
            loop_.handle_prompt(&text_prompt("go")).await;
        });
        // Give the turn time to reach the `SubagentWait` select, then
        // cancel (the `select!`'s turn-cancel arm fires — the
        // `SubagentCancel` is flipped + the `Cancelled` tool result).
        tokio::time::sleep(Duration::from_millis(100)).await;
        turn_cancel_c
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        turn.await.expect("the turn task completed");
        let end = wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1")
        })
        .await
        .expect("the `subagent`'s tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(is_error, "a cancelled dispatch is an error result");
        assert_eq!(
            result["content"][0]["text"], "cancelled",
            "the `Cancelled` tool result"
        );
        wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled after the cancel");
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

    /// A tool result's IMAGE blocks reach the model: the `tool` message
    /// the turn pushes is the result's FULL `Blocks` (a `read` on an
    /// image file returns an image-only result — flattening it to the
    /// first text block would drop the image the model is meant to see;
    /// the provider's `to_wire` then sends it as an `image_url`
    /// data-URI on the request wire).
    #[tokio::test]
    async fn a_tool_result_image_reaches_the_model_transcript() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "img.png" }),
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
        // A 1x1 transparent PNG (the `read` keys on the extension —
        // the bytes are base64-encoded, not decoded).
        std::fs::write(
            loop_.space_cwd.join("img.png"),
            [
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48,
                0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00,
                0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78,
                0x9C, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xD4, 0x00,
                0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
            ],
        )
        .expect("the test png writes");
        // The turn completes on its own (the scripted provider ends it —
        // no cancel), so a plain await (the `loop_` stays in scope for
        // the `messages` assertion below).
        loop_.handle_prompt(&text_prompt("read the image")).await;
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
        // The `tool` message is the result's FULL `Blocks` (the image
        // survived — NOT flattened to the first text block, which would
        // be empty for an image-only result).
        let tool_msg = loop_
            .messages
            .iter()
            .find(|m| m.role == ChatRole::Tool)
            .expect("a tool message was pushed");
        match &tool_msg.content {
            MessageContent::Blocks(blocks) => assert!(
                blocks
                    .iter()
                    .any(|b| matches!(b, ContentBlock::Image { .. })),
                "the image block survived into the model transcript (got {blocks:?})"
            ),
            MessageContent::Text(t) => {
                panic!("the tool message was flattened to text ({t:?}) — the image is lost")
            }
        }
    }

    /// A prompt WITH images pushes a `Blocks` user message (text FIRST,
    /// then the image blocks — the model sees the image on the request
    /// wire via the provider's `to_wire`; an empty text + images is an
    /// image-only `Blocks`, NO empty text block).
    #[tokio::test]
    async fn a_prompt_with_images_pushes_a_blocks_user_message() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::TextDelta("ok".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("ok2".to_string()),
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
        // A prompt with text + an image → `Blocks` (text first, image
        // after).
        loop_
            .handle_prompt(&Prompt {
                text: "describe".to_string(),
                images: vec![crate::agent::tools::ImageRef {
                    data: "BASE64DATA".to_string(),
                    mime_type: "image/png".to_string(),
                }],
            })
            .await;
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
        let first = loop_.messages.first().expect("a user message was pushed");
        match &first.content {
            MessageContent::Blocks(blocks) => {
                assert_eq!(blocks.len(), 2, "a text block + an image block");
                assert!(
                    matches!(&blocks[0], ContentBlock::Text { text } if text == "describe"),
                    "the text block is FIRST (got {blocks:?})"
                );
                assert!(
                    matches!(&blocks[1], ContentBlock::Image { image } if image.mime_type == "image/png" && image.data == "BASE64DATA"),
                    "the image block follows (got {blocks:?})"
                );
            }
            other => panic!("a prompt with images is a Blocks user message, got {other:?}"),
        }
        // An EMPTY text + an image → an image-only `Blocks` (no empty
        // text block).
        loop_
            .handle_prompt(&Prompt {
                text: String::new(),
                images: vec![crate::agent::tools::ImageRef {
                    data: "D".to_string(),
                    mime_type: "image/gif".to_string(),
                }],
            })
            .await;
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("the turn settled");
        let second = loop_
            .messages
            .get(2)
            .expect("the second user message was pushed");
        match &second.content {
            MessageContent::Blocks(blocks) => {
                assert_eq!(blocks.len(), 1, "an empty text adds no text block");
                assert!(matches!(&blocks[0], ContentBlock::Image { .. }));
            }
            other => panic!("an image-only prompt is a Blocks user message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn model_requests_carry_the_session_id_across_turns_and_tool_calls() {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let recorded = Arc::new(StdMutex::new(Vec::new()));
        let provider = SessionRecordingProvider::with_scripts(
            recorded.clone(),
            vec![
                // Turn 1, call 1: tool call
                vec![
                    ProviderEvent::ToolCall(ToolCall {
                        id: "t1".to_string(),
                        name: "ls".to_string(),
                        arguments: json!({ "path": "." }),
                    }),
                    ProviderEvent::Done(FinishReason::ToolCalls),
                ],
                // Turn 1, call 2: finish
                vec![
                    ProviderEvent::TextDelta("done with ls".to_string()),
                    ProviderEvent::Done(FinishReason::Stop),
                ],
                // Turn 2, call 3: simple text response
                vec![
                    ProviderEvent::TextDelta("turn 2".to_string()),
                    ProviderEvent::Done(FinishReason::Stop),
                ],
            ],
        );
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );

        // Turn 1: has a tool call and follow-up completion
        loop_.handle_prompt(&text_prompt("run ls")).await;
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("turn 1 settled");

        // Turn 2: another conversational prompt
        loop_.handle_prompt(&text_prompt("second turn")).await;
        wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::agent_settled)
        })
        .await
        .expect("turn 2 settled");

        let sessions = recorded.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(
            sessions.len(),
            3,
            "expected 3 model calls across the two turns"
        );
        for (i, session_id) in sessions.iter().enumerate() {
            assert_eq!(
                session_id,
                &Some("s1".to_string()),
                "call {i} should carry Some(\"s1\")",
            );
        }
    }

    /// (ADR 0018) A session with a temp `mcp.json` (the fake stdio server)
    /// where the model emits an `mcp` tool call → the tool result
    /// round-trips into the transcript (a `tool_execution_end` with the
    /// echoed text).
    #[tokio::test]
    async fn an_mcp_tool_call_round_trips_into_the_transcript() {
        // A temp dir with a `.pi/mcp.json` (the fake stdio server).
        let dir = std::env::temp_dir().join(format!("mcp-loop-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join(".pi")).unwrap();
        let bin = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ));
        std::fs::write(
            dir.join(".pi/mcp.json"),
            serde_json::json!({
                "mcpServers": { "a": { "command": bin.display().to_string() } }
            })
            .to_string(),
        )
        .unwrap();
        // The model emits an `mcp` tool call (a `echo`), then a final
        // response.
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "m1".to_string(),
                    name: "mcp".to_string(),
                    arguments: json!({ "tool": "echo", "args": { "text": "hi" } }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        // Swap in the temp-config MCP manager (the `build_loop` default reads
        // the real home — this one reads the temp `mcp.json`).
        loop_.mcp = McpManager::new(dir.clone(), dir.clone(), None);
        loop_.handle_prompt(&text_prompt("call the mcp tool")).await;
        // The `mcp` tool result round-tripped (the `echo` → "hi").
        let end = wait_for_event(&mut events_rx, 10000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "m1")
        })
        .await
        .expect("the `mcp` tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(!is_error, "the `mcp` call succeeded: {result:?}");
        assert_eq!(result["content"][0]["text"], "hi");
    }

    /// (ADR 0018) A Stop mid-`mcp`-call cancels (the in-flight request is
    /// cancelled — the tool result is `cancelled`; the turn settles, the
    /// stdio child is killed via `kill_on_drop`).
    #[tokio::test]
    async fn a_stop_mid_mcp_call_cancels() {
        let dir = std::env::temp_dir().join(format!("mcp-loop-stop-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join(".pi")).unwrap();
        let bin = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ));
        std::fs::write(
            dir.join(".pi/mcp.json"),
            serde_json::json!({
                "mcpServers": { "a": { "command": bin.display().to_string() } }
            })
            .to_string(),
        )
        .unwrap();
        // The model emits an `mcp` tool call (a `hang` — never answers), then
        // a final response.
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "m1".to_string(),
                    name: "mcp".to_string(),
                    arguments: json!({ "tool": "hang", "args": {} }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
        let mut loop_ = build_loop(
            Box::new(provider),
            events_tx,
            turn_cancel.clone(),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
        );
        loop_.mcp = McpManager::new(dir.clone(), dir.clone(), None);
        let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("call mcp")).await });
        // The `mcp` tool starts (the `hang` never answers).
        wait_for_event(
            &mut events_rx,
            5000,
            |e| matches!(e, RpcEvent::tool_execution_start { tool_name, .. } if tool_name == "mcp"),
        )
        .await
        .expect("the `mcp` tool started");
        tokio::time::sleep(Duration::from_millis(100)).await;
        // A Stop (the TURN cancel) mid-`mcp`-call.
        turn_cancel
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel();
        match tokio::time::timeout(Duration::from_secs(5), task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("the turn task failed: {e}"),
            Err(_) => panic!("the turn did not settle after the Stop (it waited out the `hang`)"),
        }
        // The tool result is `cancelled` (the in-flight request was cancelled).
        let end = wait_for_event(&mut events_rx, 5000, |e| {
            matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "m1")
        })
        .await
        .expect("the `mcp` tool_execution_end");
        let RpcEvent::tool_execution_end {
            result, is_error, ..
        } = end
        else {
            unreachable!()
        };
        assert!(
            is_error,
            "a cancelled `mcp` call is an error result: {result:?}"
        );
        assert_eq!(result["content"][0]["text"], "cancelled");
    }

    // ── (ADR 0030 Task 3) the file-access policy gate ───────────────────

    /// `handle_prompt` under a hard bound. Every policy test uses it: a
    /// gate that regressed into prompting on a turn nobody answers would
    /// otherwise hang the suite until the test harness timeout (a FAIL
    /// pointing at the gate is the point — not a stall).
    async fn bounded_prompt(loop_: &mut AgentLoop, text: &str) {
        tokio::time::timeout(
            Duration::from_secs(30),
            loop_.handle_prompt(&text_prompt(text)),
        )
        .await
        .unwrap_or_else(|_| {
            panic!("the turn never settled — a prompt nobody answered, or a tool that hung")
        });
    }

    /// The `settings.json` shape the policy tests pin (the wire key is
    /// `filePolicy` — the same one the Settings UI writes).
    fn policy_settings(reads: &str, writes: &str, shell: &str) -> String {
        format!(
            r#"{{ "filePolicy": {{ "reads": "{reads}", "writes": "{writes}", "shell": "{shell}" }} }}"#
        )
    }

    /// A fresh temp dir (created, removed on drop — the boundary tests'
    /// `Tmp`, local to this module).
    struct Tmp(PathBuf);
    impl Tmp {
        fn new(tag: &str) -> Self {
            let d =
                std::env::temp_dir().join(format!("loop-policy-{tag}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&d).unwrap();
            Self(d)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The policy-gate fixture: an `AgentLoop` whose `config_dir` holds a
    /// `settings.json` with the given policy JSON (`None` = NO `config_dir`
    /// at all — the no-settings-layer case), on a fresh `space_cwd`, with a
    /// RECORDING sink (the `TestSink` cannot see `permission-request`) and
    /// `trust: None` (untrusted — fail-closed, so an `Ask` really prompts).
    /// The `config_dir` is returned WITH the loop: it must outlive it (the
    /// loop re-reads the file every turn, and the temp dir is removed on
    /// drop — dropping it early would silently degrade every test to the
    /// all-`Allow` default).
    ///
    /// The caller pins `$HOME` FIRST (the `RestoreHome` + `env_lock`
    /// pattern) so `AgentLoop::new`'s `read_roots` / `protected_dirs` see an
    /// empty scratch home: the read boundary is then EXACTLY the `cwd` and
    /// the deny-list is whatever the test created, never whatever happens to
    /// be in the developer's real `~/.agents`.
    fn build_policy_loop(
        provider: Box<dyn Provider>,
        events: mpsc::UnboundedSender<RpcEvent>,
        sink: Arc<dyn EventSink>,
        settings: Option<&str>,
    ) -> (AgentLoop, Option<Tmp>) {
        let config_dir = settings.map(|_| Tmp::new("cfg"));
        if let (Some(dir), Some(json)) = (config_dir.as_ref(), settings) {
            std::fs::write(dir.path().join("settings.json"), json).unwrap();
        }
        let (mut loop_, _db) = build_loop_with_db(
            provider,
            events,
            Arc::new(StdMutex::new(CancellationToken::new())),
            watch::channel(0u64).0,
            RetryPolicy::new_with(5, Duration::from_millis(1)),
            vec![fake_model("m1")],
            config_dir
                .as_ref()
                .map(|d| d.path().to_path_buf())
                .as_deref(),
        );
        // Swap the `TestSink` for the recording one (the field is `pub`).
        loop_.sink = sink;
        (loop_, config_dir)
    }

    /// The tool call the policy tests share: a `read` of `path`.
    fn read_call(id: &str, path: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            name: "read".to_string(),
            arguments: json!({ "path": path }),
        }
    }

    /// Await the loop's pending-permission entry and answer it (the mock
    /// handle-free waiter — the `tests/harness_loop.rs` pattern, the
    /// `pending` map being a `pub` field). Spawned BEFORE the prompt so a
    /// gate that must NOT prompt can never hang the test.
    async fn answer_permission(pending: PendingPermissions, session: &str, option: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let mut map = pending.lock().await;
            if let Some(key) = map
                .keys()
                .find(|k| k.starts_with(&format!("{session}/")))
                .cloned()
            {
                let sender = map.remove(&key).expect("the entry is in the map");
                let _ = sender.send(PermissionOutcome::Selected {
                    option_id: option.to_string(),
                });
                return;
            }
            drop(map);
            if tokio::time::Instant::now() >= deadline {
                return; // the test asserts no prompt arrived
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The `permission-request` frames a sink recorded.
    fn permission_requests(events: &[(String, Value)]) -> Vec<Value> {
        events
            .iter()
            .filter(|(name, _)| name == "permission-request")
            .map(|(_, p)| p.clone())
            .collect()
    }

    /// Drain every event the turn emitted and return the
    /// `tool_execution_end` results keyed by tool-call id (a turn may batch
    /// several calls, so ONE drain collects them all — draining per id would
    /// consume the later calls' events while looking for an earlier one).
    fn tool_ends(events: &mut mpsc::UnboundedReceiver<RpcEvent>) -> HashMap<String, (Value, bool)> {
        let mut out = HashMap::new();
        while let Ok(ev) = events.try_recv() {
            if let RpcEvent::tool_execution_end {
                tool_call_id,
                result,
                is_error,
                ..
            } = ev
            {
                out.insert(tool_call_id, (result, is_error));
            }
        }
        out
    }

    /// (ADR 0030) `reads: Ask` + an out-of-boundary `read` on an UNTRUSTED
    /// Space: a `permission-request` naming THE PATH (not a JSON blob), and
    /// after the user allows, the read really lands.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn ask_beyond_the_boundary_prompts_naming_the_path_then_runs() {
        let home = Tmp::new("ask-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span; no other thread mutates HOME concurrently.
        unsafe { std::env::set_var("HOME", home.path()) };

        let outside = Tmp::new("ask-outside");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
        let path = outside.path().join("secret.txt");
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(read_call("t1", &path.display().to_string())),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let (mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            Some(&policy_settings("ask", "ask", "ask")),
        );
        let pending = loop_.pending_permissions.clone();
        let answer = tokio::spawn(answer_permission(pending, "s1", "allow"));
        bounded_prompt(&mut loop_, "go").await;
        answer.await.unwrap();

        let prompts = permission_requests(&sink_events.lock().unwrap());
        assert_eq!(prompts.len(), 1, "exactly one prompt, got {prompts:?}");
        assert_eq!(prompts[0]["requestId"], "t1");
        assert_eq!(
            prompts[0]["request"]["toolCall"]["title"],
            format!("read {}", path.display()),
            "the prompt names the path in plain form"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
        assert!(!is_error, "the allowed read succeeded: {result:?}");
        assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
    }

    /// (ADR 0030) `reads: Sandboxed` + an out-of-boundary `read`: a
    /// tool-result error worded like the executor's own rejection, and NO
    /// `permission-request` (a Deny never reaches the UI — Trust included:
    /// this loop is untrusted, and the trusted case is the sibling test).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn sandboxed_beyond_the_boundary_errors_with_no_prompt() {
        let home = Tmp::new("deny-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let outside = Tmp::new("deny-outside");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
        let path = outside.path().join("secret.txt");
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(read_call("t1", &path.display().to_string())),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let (mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            Some(&policy_settings("sandboxed", "sandboxed", "allow")),
        );
        bounded_prompt(&mut loop_, "go").await;

        assert!(
            permission_requests(&sink_events.lock().unwrap()).is_empty(),
            "a Deny emits no permission-request"
        );
        assert!(
            loop_.pending_permissions.try_lock().unwrap().is_empty(),
            "and registers no oneshot"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t1").expect("a tool result").clone();
        assert!(is_error, "the denied read is an error");
        let text = result["content"][0]["text"].as_str().unwrap().to_string();
        assert_eq!(
            text,
            format!(
                "read: path escapes the session sandbox: {}",
                path.canonicalize().unwrap().display()
            ),
            "worded EXACTLY like the executor's rejection, so the model cannot tell which layer said no"
        );
    }

    /// (ADR 0030, Deviation 2 — the intended widening) the DEFAULT policy
    /// (all `Allow`, and the no-`config_dir` case that behaves the same):
    /// an out-of-boundary `read` AND an out-of-boundary `write` both
    /// dispatch silently — no prompt, no error — and the write LANDS. This
    /// is the behavior the feature deliberately widens (pre-feature a write
    /// prompted); it is asserted as the widening, so a regression that
    /// re-tightens the default reddens it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn the_default_policy_reads_and_writes_beyond_the_boundary_in_silence() {
        let home = Tmp::new("allow-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let outside = Tmp::new("allow-outside");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
        let read_path = outside.path().join("secret.txt");
        let write_path = outside.path().join("written-by-agent.txt");
        for settings in [Some(&policy_settings("allow", "allow", "allow")), None] {
            let (provider, _calls) = ScriptedProvider::new(vec![
                Some(vec![
                    ProviderEvent::ToolCall(read_call("t1", &read_path.display().to_string())),
                    ProviderEvent::ToolCall(ToolCall {
                        id: "t2".to_string(),
                        name: "write".to_string(),
                        arguments: json!({
                            "path": write_path.display().to_string(),
                            "content": "WROTE-IT",
                        }),
                    }),
                    ProviderEvent::Done(FinishReason::ToolCalls),
                ]),
                Some(vec![
                    ProviderEvent::TextDelta("done".to_string()),
                    ProviderEvent::Done(FinishReason::Stop),
                ]),
            ]);
            let (events_tx, mut events_rx) = mpsc::unbounded_channel();
            let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
            let (mut loop_, _cfg) = build_policy_loop(
                Box::new(provider),
                events_tx,
                Arc::new(RecordingSink {
                    events: sink_events.clone(),
                }),
                settings.map(|s| s.as_str()),
            );
            bounded_prompt(&mut loop_, "go").await;
            assert!(
                permission_requests(&sink_events.lock().unwrap()).is_empty(),
                "the default policy prompts for nothing"
            );
            // The `read` AND the `write` are separate tool calls in one
            // batch: the script emits them as two `ToolCall` events (the
            // provider stream carries one call per event — see the
            // `read`-then-`write` batch this mirrors).
            let ends = tool_ends(&mut events_rx);
            let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
            assert!(!is_error, "the default policy never errors: {result:?}");
            assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
            let (result, is_error) = ends.get("t2").expect("the write dispatched").clone();
            assert!(
                !is_error,
                "the default policy writes beyond the cwd: {result:?}"
            );
            assert_eq!(
                std::fs::read_to_string(&write_path).unwrap_or_default(),
                "WROTE-IT",
                "the widened write actually landed"
            );
            std::fs::remove_file(&write_path).unwrap();
        }
    }

    /// (ADR 0030 Deviation 4 — "affects an already-running session") the
    /// policy is re-read from `settings.json` at the top of EVERY prompt:
    /// a settings edit BETWEEN two prompts changes the SECOND prompt's
    /// gating, with no restart and no relaunch.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_settings_edit_between_prompts_changes_the_next_turns_gating() {
        let home = Tmp::new("fresh-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let outside = Tmp::new("fresh-outside");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
        let path = outside.path().join("secret.txt").display().to_string();
        // A THREE-call script: turn 1's `read`, the summary-free turn 2's
        // `read`, then the final text (the provider repeats its LAST entry,
        // so the third script entry must be the terminal one).
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(read_call("t1", &path)),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("first done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
            Some(vec![
                ProviderEvent::ToolCall(read_call("t2", &path)),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("second done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let settings = policy_settings("ask", "ask", "ask");
        let (mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            Some(&settings),
        );
        // The loop's own `config_dir` (the fixture's temp dir): re-read
        // between the prompts, exactly as a user editing Settings would.
        let config_dir = loop_.config_dir.clone().expect("a settings dir");

        // Turn 1: `reads: ask` → the prompt fires (answered `allow`).
        let pending = loop_.pending_permissions.clone();
        let answer = tokio::spawn(answer_permission(pending, "s1", "allow"));
        bounded_prompt(&mut loop_, "one").await;
        answer.await.unwrap();
        assert_eq!(
            permission_requests(&sink_events.lock().unwrap()).len(),
            1,
            "turn 1 prompts under `ask`"
        );

        // The user edits the file BETWEEN the prompts: `ask` → `sandboxed`.
        std::fs::write(
            config_dir.join("settings.json"),
            policy_settings("sandboxed", "sandboxed", "allow"),
        )
        .unwrap();

        // Turn 2: the SAME out-of-boundary read is now a Deny — no prompt,
        // an error result. A per-session (or per-process) snapshot of the
        // policy would still prompt here, which is what this asserts against.
        bounded_prompt(&mut loop_, "two").await;
        assert_eq!(
            permission_requests(&sink_events.lock().unwrap()).len(),
            1,
            "the second turn must NOT prompt after the file changed to `sandboxed`"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t2").expect("a tool result for t2").clone();
        assert!(is_error, "the now-Sandboxed read is refused: {result:?}");
    }

    /// (ADR 0030 Task 5) `shell: Sandboxed` means CONFINE: the gate never
    /// prompts, the command runs INSIDE the Landlock ruleset, and a kernel
    /// that cannot confine FAILS CLOSED with a visible error instead of
    /// running unsandboxed. Both branches assert "no prompt".
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_sandboxed_shell_dispatches_confined_with_no_prompt() {
        let home = Tmp::new("shell-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "bash".to_string(),
                    arguments: json!({ "command": "touch confined_ran.txt" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let cwd = {
            let (mut loop_, _cfg) = build_policy_loop(
                Box::new(provider),
                events_tx,
                Arc::new(RecordingSink {
                    events: sink_events.clone(),
                }),
                Some(&policy_settings("allow", "allow", "sandboxed")),
            );
            let cwd = loop_.space_cwd.clone();
            bounded_prompt(&mut loop_, "go").await;
            cwd
        };
        assert!(
            permission_requests(&sink_events.lock().unwrap()).is_empty(),
            "`shell: sandboxed` asks nothing — it confines (here: refuses to run unsandboxed)"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t1").expect("a tool result").clone();
        // Can THIS machine confine? `cfg`-gated (not `cfg!`) so a non-Linux
        // build does not name the sandbox module, which is not compiled
        // there at all.
        #[cfg(target_os = "linux")]
        let confined_here = crate::agent::tools::sandbox::landlock_available();
        #[cfg(not(target_os = "linux"))]
        let confined_here = false;
        if confined_here {
            // The kernel confines → the command RUNS (inside the ruleset)
            // and its write inside the boundary lands.
            assert!(!is_error, "a confined run succeeds: {result:?}");
            assert!(
                cwd.join("confined_ran.txt").exists(),
                "the confined command ran inside the boundary"
            );
        } else {
            // No Landlock / not Linux → a VISIBLE error and NO run.
            assert!(is_error, "an unavailable sandbox is an error: {result:?}");
            assert!(
                result["content"][0]["text"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("Sandboxed shell is unavailable"),
                "the error names the reason: {result:?}"
            );
            assert!(
                !cwd.join("confined_ran.txt").exists(),
                "fail CLOSED: the command must not have run"
            );
        }
    }

    /// (ADR 0030 Deviation 3) a write into a user-level agent-definition dir
    /// is refused by the GATE in the deny-list's own words (the same text the
    /// executor produces), even though `writes: Sandboxed` would otherwise
    /// read as an escape — the deny-list is the true reason there.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_gate_denial_of_a_protected_dir_uses_the_protected_wording() {
        let home = Tmp::new("protected-home");
        for dir in [
            ".agents/skills",
            ".pi/agent/skills",
            ".agents/agents",
            ".pi/agent/agents",
        ] {
            std::fs::create_dir_all(home.path().join(dir)).unwrap();
        }
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let target = home.path().join(".agents/skills/pwn/SKILL.md");
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "write".to_string(),
                    arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let (mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            // `writes: allow` on purpose: the deny-list is a FLOOR — it
            // refuses whatever the policy says (the executor owns that
            // case; here the gate never sees it, so `sandboxed` is used to
            // exercise the GATE's wording).
            Some(&policy_settings("allow", "sandboxed", "allow")),
        );
        bounded_prompt(&mut loop_, "go").await;
        assert!(
            permission_requests(&sink_events.lock().unwrap()).is_empty(),
            "a protected write is a Deny, never a prompt"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t1").expect("a tool result").clone();
        assert!(is_error, "the protected write is refused: {result:?}");
        let text = result["content"][0]["text"].as_str().unwrap().to_string();
        assert!(
            text.contains("protected agent-definition directory"),
            "the refusal names the real reason, got {text:?}"
        );
        assert!(
            !target.exists(),
            "and nothing was written (the gate stopped it before the executor)"
        );
    }

    /// (ADR 0030) A TRUSTED Space suppresses an `Ask` at the GATE: the
    /// verdict itself is `Allow` (so no `PendingPermissions` entry is ever
    /// registered), and the out-of-boundary `read` runs. Asserted on
    /// `policy_verdict` DIRECTLY as well as end-to-end — the end-to-end half
    /// alone cannot tell the gate's own trust check apart from
    /// `decision_for`'s (both suppress, by design), and the precedence rule
    /// is worth pinning at the layer that owns it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_trusted_space_suppresses_the_prompt_the_policy_would_raise() {
        let home = Tmp::new("trust-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let outside = Tmp::new("trust-outside");
        std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
        let path = outside.path().join("secret.txt");
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(read_call("t1", &path.display().to_string())),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let (mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            // `ask` — the policy WOULD prompt; Trust is what stops it.
            Some(&policy_settings("ask", "ask", "ask")),
        );
        // The Space the loop's `space_cwd` points at, marked trusted (the
        // `Db` the fixture built is reachable through `loop_.store`, so the
        // trust source is wired to the same rows).
        let db = Arc::new(Db::open(&home.path().join("trust.db")).unwrap());
        let cwd = loop_.space_cwd.clone();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        db.set_space_trusted(&cwd.display().to_string(), true)
            .unwrap();
        loop_.trust = Some(Arc::new(
            crate::agent::harness::trust::SqliteTrustSource::new(db),
        ));

        // The gate's OWN verdict, read AFTER the turn (so `file_policy` is
        // the `ask` the settings file carries — before the first prompt the
        // field is still the all-`Allow` default and any verdict would
        // pass). `Allow`, not `Ask`: Trust suppressed it before any prompt
        // machinery was reached.
        bounded_prompt(&mut loop_, "go").await;
        let tc = read_call("t1", &path.display().to_string());
        assert_eq!(
            loop_.policy_verdict(&tc),
            Some(Decision::Allow),
            "Trust suppresses the `Ask` at the decision point itself"
        );
        // CONTRAST (so the assertion above cannot pass vacuously on a
        // policy that was never `ask`): the SAME loop and call with the
        // trust removed is an `Ask`.
        loop_.trust = None;
        assert_eq!(
            loop_.policy_verdict(&tc),
            Some(Decision::Ask),
            "without Trust the very same access prompts"
        );

        assert!(
            permission_requests(&sink_events.lock().unwrap()).is_empty(),
            "a trusted Space is never prompted (Trust suppresses `Ask`)"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
        assert!(!is_error, "and the read runs: {result:?}");
        assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
    }

    /// (ADR 0030) A tool the policy does NOT judge stays ungated:
    /// `read_skill` takes a skill NAME and carries its own per-skill
    /// containment (ADR 0029), so a `Sandboxed` read policy must not touch
    /// it — and it must not gain a prompt either.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn the_skill_tools_stay_outside_the_policy_gate() {
        let home = Tmp::new("skill-home");
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };

        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "read_skill".to_string(),
                    arguments: json!({ "name": "nope" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let (mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            Some(&policy_settings("sandboxed", "sandboxed", "sandboxed")),
        );
        bounded_prompt(&mut loop_, "go").await;
        assert!(
            permission_requests(&sink_events.lock().unwrap()).is_empty(),
            "the skill tools never prompt"
        );
        let ends = tool_ends(&mut events_rx);
        let (result, _is_error) = ends.get("t1").expect("a tool result").clone();
        let text = result["content"][0]["text"].as_str().unwrap().to_string();
        assert!(
            text.contains("unknown skill"),
            "the executor's OWN error (the gate stayed out of it), got {text:?}"
        );
        assert!(
            !text.contains("escapes the session sandbox"),
            "the policy must not re-word the skill tool's failure: {text:?}"
        );
    }
}

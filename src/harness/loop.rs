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
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Mutex as TokioMutex};
use tokio_util::sync::CancellationToken;

use crate::agent::events::CompactionReason;
use crate::agent::events::EventSink;
use crate::agent::events::RpcEvent;
use crate::agent::harness::catalog::{Model, ModelCatalog};
use crate::agent::harness::compact::{self, Compactor};
use crate::agent::harness::dispatch::SubagentDispatcher;
use crate::agent::harness::model_key::ModelKey;
use crate::agent::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ModelOptions, ModelRequest, Provider,
    ProviderError, ToolSpec,
};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::harness::store::Store;
use crate::agent::harness::trust::TrustSource;
use crate::agent::interactive::{
    CachedPassword, PendingInteractive, PendingSudo, RealSudoRunner, SudoRunner,
};
use crate::agent::mcp::McpManager;
use crate::agent::normalize::{ThoughtState, TurnState};
use crate::agent::permission::PendingPermissions;
use crate::agent::policy::FilePolicy;
use crate::agent::todo::TodoStore;
use crate::agent::tools::ImageRef;
#[cfg(test)]
use crate::agent::tools::{ContentBlock, ToolResult};

/// A prompt to the loop (the `prompt_queue` item): the text + the image
/// attachments (`ImageRef` — the pi `ImageContent` shape; the manager
/// maps the wire `ImagePayload` onto it). An empty `images` is a
/// text-only prompt (the common case).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptTurn {
    pub text: String,
    pub images: Vec<ImageRef>,
}

/// Disambiguation alias for `PromptTurn` (see CONTEXT.md: bare `prompt`
/// refers to permission prompts). Kept for backward compatibility.
pub type Prompt = PromptTurn;

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
    /// Compact NOW (the `/compact` Composer command — ADR 0044). Applied when
    /// IDLE — a command sent mid-turn queues behind the turn, the same rule as
    /// `SetModel`. `instructions` are the user's extra summary instructions.
    Compact { instructions: Option<String> },
}

/// A compaction the loop must run before its next model call, and what to
/// report it as (`reason` is a UI contract — ADR 0045, and [`CompactionReason`]
/// is its vocabulary; `will_retry` is true only for overflow recovery, where
/// the turn continues on the other side of the indicator with the SAME model
/// call).
///
/// This replaced the `force_compact: bool` flag, which could carry the
/// instruction "compact" but not the reason the mid-turn caller needed to
/// report: an overflow recovery and a threshold check looked identical on the
/// wire (and the flag never had a production caller until the overflow).
#[derive(Debug, Clone, Copy)]
pub(crate) struct CompactionTrigger {
    pub reason: CompactionReason,
    pub will_retry: bool,
}

/// What [`AgentLoop::run_compaction`] decided, and therefore what the turn does
/// next. The three-way shape exists because the two ways a compaction shrinks
/// nothing are NOT the same fact: `Skipped` proves the kept tail alone is over
/// the window (a second request cannot succeed), while `Failed` proves only
/// that the summary call did not — a transient mechanism failure that says
/// nothing about the turn's own request. Collapsing both into one boolean is
/// what made an overflow recovery abandon a turn it should have re-issued
/// (issue #90).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionRan {
    /// The older messages were replaced by a summary: the transcript shrank.
    Compacted,
    /// Nothing old enough to summarize (`older` empty) — the transcript is
    /// unchanged and, for an overflow, a second request is provably doomed.
    Skipped,
    /// The summary call errored: the transcript is intact and the failure is
    /// the SUMMARY's, not the turn's.
    Failed,
}

/// The "compact before the next model call" state of ONE turn (finding 6).
/// Its lifetime is ONE TURN, and [`TurnControl::enter_turn`] is the ONLY way
/// to clear it — called from `begin_turn`, so a trigger a PREVIOUS turn
/// queued can never survive into the next prompt.
///
/// Both producers set it and `continue` straight to the loop top, where the
/// cancellation check runs FIRST, so a Stop that lands as the overflow is
/// handled settles the turn (`settle_cancelled`) without ever taking the
/// trigger — leaving it armed would make the NEXT prompt run the canceled
/// turn's overflow recovery (and, when that compaction shrinks nothing, end
/// the new turn as an error before its own model call). That used to be a
/// `pub` field with a clear the turn had to remember to perform; it is now a
/// private latch behind a method, so the invariant is enforced by the type
/// instead of by a comment — the pin is
/// `a_stale_overflow_trigger_does_not_hijack_the_next_turn`.
///
/// `CompactionRan` deliberately does NOT live here: it is a FUNCTION RESULT
/// (returned by [`AgentLoop::run_compaction`], consumed by the overflow
/// caller), not one-turn state that needs clearing — bending it into a field
/// would change the live by-value flow for no invariant gained. This bundle
/// owns the TRIGGER; the verdict stays the return value.
#[derive(Default)]
pub(crate) struct TurnControl {
    force_compact: Option<CompactionTrigger>,
}

impl TurnControl {
    /// The ONE clear: called at the top of every turn, dropping whatever a
    /// previous turn left behind (see the struct doc — a Stop racing an
    /// overflow recovery exits BEFORE the trigger is consumed).
    fn enter_turn(&mut self) {
        self.force_compact = None;
    }

    /// Queue a compaction for the next model call of THIS turn. The typed
    /// entry point for every producer (both overflow sites — and the tests
    /// that pre-arm a trigger), so a malformed trigger cannot be built.
    fn arm(&mut self, reason: CompactionReason, will_retry: bool) {
        self.force_compact = Some(CompactionTrigger { reason, will_retry });
    }

    /// Take the queued trigger, if any (the compaction check at the top of
    /// the model-call loop; taking it is what makes it one-shot).
    fn take(&mut self) -> Option<CompactionTrigger> {
        self.force_compact.take()
    }
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

/// (finding 6) The permission cluster: the state the file-access policy
/// (ADR 0030) judges a tool call with, and the state every `ToolCtx` carries
/// so the executor enforces the SAME decision the gate made. The eight
/// fields moved out of [`AgentLoop`] verbatim (four frozen at construction,
/// three refreshed once per turn, one per-agent narrowing) plus `web` and
/// `render_requester` — the two that share the per-turn refresh cadence AND
/// ride the same single `ToolCtx` construction, so splitting them off would
/// re-open the construction this bundle exists to own.
///
/// Lifetime is the SESSION (the loop task owns it): `new` computes the
/// frozen half from the ONE `$HOME` read, `refresh` re-reads the per-turn
/// half at the top of every `handle_prompt`, and `to_tool_ctx` is the ONE
/// place a `ToolCtx` is built. `space_cwd` deliberately STAYS an `AgentLoop`
/// field — it is the `ToolCtx`'s `cwd` but also the write-boundary root, the
/// trust lookup key, and the subagent parent's cwd, none of which is
/// permission state — so it arrives as `to_tool_ctx`'s argument, as does the
/// turn's cancel token (a per-CALL value, never loop state).
///
/// Fields are `pub(super)`: the gate (`loop::gate`), the dispatcher
/// (`loop::tools`), the turn (`loop::turn`) and the loop's own tests read
/// them the way they read the flat fields before — `self.permissions.<field>`
/// — with no accessor explosion.
pub(crate) struct PermissionContext {
    // ── frozen at construction (ADR 0030) — the ONE `$HOME` read ──
    /// (ADR 0030) The session's READ boundary — `cwd` + the discovery roots
    /// (user + space level), computed ONCE at construction (the boundary is
    /// frozen for the session's life: a skill dir created mid-session must
    /// not silently widen it). Handed to every `ToolCtx`.
    pub(super) read_boundary: Vec<PathBuf>,
    /// (ADR 0030) The write GUARD-MEMBERSHIP list (`boundary::protected_dirs()`
    /// — the four user-level agent-definition dirs), frozen with the boundary.
    /// Since the permissions spec's round-3 correction this is NOT a rule set:
    /// a write under one of these with no rule matching is the guard's ROW-3
    /// ASK, not a deny. It is also the list the Landlock subtraction consumes,
    /// which is why it is a plain four-path list and never rule-derived.
    pub(super) protected: Vec<PathBuf>,
    /// (T03) The DEFAULT rule set (`policy::default_rules()` — EMPTY since the
    /// round-3 correction: the four agent dirs are the guard-membership set on
    /// [`PermissionContext::protected`], not default deny rules, so the spec's
    /// single behavioural change — "today's hardcoded deny becomes an ask" — is
    /// reachable), computed ONCE at construction exactly like `protected` (the
    /// same `$HOME` read: `skills::home_dir()`), frozen for the session's life.
    /// The USER half lives in `file_policy.rules` (per-turn by Deviation 4);
    /// the two halves are concatenated per turn into [`PermissionContext::rules`].
    /// The defaults-first concat shape stays even with the default half empty:
    /// it is the contract, and it protects whatever the defaults become next.
    pub(super) default_rules: Vec<crate::agent::policy::AccessRule>,
    /// (T03) The home `rule_for` expands a leading `~` against — the SAME
    /// value `default_rules()`/`protected_dirs()` go through, captured in
    /// `new` so no per-call `$HOME` read happens. `None` = no home: a `~`
    /// rule matches NOTHING (silently — `rule_for`'s `expand_rule_path`
    /// expands nothing against `None`; the CONSEQUENCE is pinned by
    /// `a_tilde_rule_with_home_none_matches_nothing_and_with_a_real_home_matches_and_decides`).
    pub(super) rules_home: Option<std::path::PathBuf>,
    // ── refreshed once per turn (ADR 0030 Deviation 4) ──
    /// (ADR 0030 Task 3) The file-access policy IN FORCE for the current
    /// turn — refreshed from `settings.json` at the top of every
    /// `handle_prompt` (Deviation 4: one read per turn, so every tool call
    /// in a turn agrees and a mid-turn hand-edit is observed at the NEXT
    /// prompt; the same in-process read `launch.rs` does per dispatch).
    /// `config_dir: None` → all-`Allow` (no settings layer to read).
    /// Handed to every `ToolCtx` so the executor enforces the SAME tier the
    /// gate decided, and read by the gate itself.
    pub(super) file_policy: FilePolicy,
    /// (T03) The rule list IN FORCE for the current turn:
    /// `default_rules() ++ file_policy.rules` — defaults FIRST, the
    /// CONCATENATION CONTRACT of [`crate::agent::policy::default_rules`] (an
    /// exact-tie user `allow` must beat a rule listed before it; concatenated
    /// the other way round the allow-directory action silently no-ops),
    /// reassembled at the top of every `handle_prompt` next to the
    /// `file_policy` re-read (ONE concat per turn, so every tool call in a
    /// turn agrees). Handed to every `ToolCtx` so the gate and the
    /// executors' rule check judge the SAME list.
    ///
    /// The default half is EMPTY today ([`PermissionContext::default_rules`]),
    /// so in practice this is the user rules — but the concat keeps the
    /// contract's SHAPE so a rule the user set (or a default reintroduced)
    /// lands in the documented order rather than by accident of there being
    /// one list.
    ///
    /// INITIAL VALUE (before the first `handle_prompt`): `Vec::new()` — the
    /// same construction-time stance as `file_policy` (`FilePolicy::default()`
    /// until the first prompt; there are no tool calls before a prompt, so
    /// the empty value is inert). The concat first runs at the top of the
    /// FIRST `handle_prompt`, so the first turn's calls always see
    /// `default_rules() ++ user rules` — never `vec![]` once a prompt was
    /// handled, which is why the concat site needs no "was a turn handled
    /// yet?" branch.
    pub(super) rules: Vec<crate::agent::policy::AccessRule>,
    /// The web tools' configuration IN FORCE for the current turn —
    /// refreshed from `settings.json` + env at the top of every
    /// `handle_prompt` alongside `file_policy` (one read per turn).
    pub(super) web: crate::agent::web::config::WebConfig,
    // ── setter-driven (never re-read by `refresh`) ──
    /// (finding 13b) The enabled tools (`None` = ALL tools; `Some(v)` =
    /// exactly `v` — `Some(vec![])` = NO tools). A disabled tool is a
    /// tool-result error, NOT executed.
    pub(super) enabled_tools: Option<Vec<String>>,
    /// (issue #101) The per-agent permission ruleset — `None` until set, the
    /// same construction-time stance as `file_policy`. Wired via
    /// [`AgentLoop::set_permission`] by the child Worker's `core.rs` (the line
    /// that makes the gate LIVE); gate tests set it before the first prompt.
    ///
    /// A NARROWING layer only: the gate's strict-of-two merge
    /// (`gate::stricter_of_two`) can only make a call MORE restricted than
    /// `file_policy` would, never less — ADR 0030's "trust suppresses prompts
    /// but never widens a deny" applied to definitions.
    pub(super) permission_block: Option<crate::agent::definition_permission::PermissionBlock>,
    /// Render requester for escalated web fetches (Task 7). Travels with the
    /// cluster into the ONE `ToolCtx` construction.
    pub(super) render_requester: Option<Arc<dyn crate::agent::web::RenderRequester>>,
}

impl PermissionContext {
    /// Compute the frozen half from the ONE `$HOME` read the construction is
    /// allowed (the Worker process already reads `$HOME` for MCP and skill
    /// discovery — reading it per tool call is what this binding replaced),
    /// and init the per-turn half to the same inert values the flat fields
    /// carried before the first prompt.
    fn new(config_dir: Option<&Path>, space_cwd: &Path) -> Self {
        // The construction NEVER reads `settings.json` (the per-turn half
        // starts inert, exactly as the flat fields did) — `config_dir` is the
        // settings root only `refresh` consumes, carried here so the bundle's
        // two entry points share one shape.
        let _ = config_dir;
        // (ADR 0030) The boundary is computed ONCE here, not per tool call
        // (it is frozen for the session, and `read_roots` walks the tree +
        // canonicalizes). Reading `$HOME` is legitimate HERE — the Worker
        // process already does it for MCP and for skill discovery.
        let read_boundary = crate::agent::boundary::read_roots(space_cwd);
        let protected = crate::agent::boundary::protected_dirs();
        // (T03) The rule layer's frozen half. `default_rules()` goes through
        // the SAME `skills::home_dir()` chain `protected_dirs()` just walked,
        // so ONE binding serves both the rule set and the `~`-expansion home
        // `rule_for` needs — no second `$HOME` read after construction.
        let rules_home = crate::skills::home_dir();
        let default_rules = crate::agent::policy::default_rules();
        Self {
            read_boundary,
            protected,
            default_rules,
            rules_home,
            // Refreshed per turn by `handle_prompt` (Deviation 4); the
            // construction value only covers a tool call made BEFORE the
            // first prompt, of which there are none.
            file_policy: FilePolicy::default(),
            // (T03) The per-turn rule list. `Vec::new()` mirrors the
            // `file_policy` default above: the concat first runs at the top
            // of `handle_prompt`, and there is no tool call before a prompt.
            rules: Vec::new(),
            web: crate::agent::web::config::WebConfig::default(),
            enabled_tools: None,
            // (issue #101) `None` = "the file does not speak about permissions"
            // — no narrowing. Set before the first prompt by `core.rs` via
            // [`AgentLoop::set_permission`] (the child Worker) or by a gate test.
            permission_block: None,
            render_requester: None,
        }
    }

    /// Re-read the per-turn half from `settings.json` (ADR 0030 Deviation 4:
    /// ONE read per turn, called at the top of every `handle_prompt`, so every
    /// tool call in a turn agrees and a mid-turn hand-edit is observed at the
    /// NEXT prompt). `permission_block` / `enabled_tools` / `render_requester`
    /// are setter-driven and deliberately untouched here.
    fn refresh(&mut self, config_dir: Option<&Path>) {
        // (ADR 0030 Deviation 4) The policy and web config are
        // re-read from `settings.json` ONCE per turn — the same in-process read
        // `launch.rs` does per dispatch, so a user who flips a select in
        // Settings affects an ALREADY-RUNNING session at its next prompt
        // (no restart, no relaunch). ONE read per turn (not per tool call):
        // every tool call in a turn must agree, or a half-asked batch would
        // be a policy the user never chose. `config_dir: None` → the
        // all-`Allow` default (no settings layer) + env-only web config.
        let settings = config_dir.map(crate::config::load_settings);
        self.file_policy = settings
            .as_ref()
            .map(|s| s.file_policy.clone())
            .unwrap_or_default();
        // (T03) The rules IN FORCE for this turn: the frozen defaults FIRST,
        // the user rules SECOND — the CONCATENATION CONTRACT of
        // `policy::default_rules()` (an exact-tie appended `allow` must beat a
        // rule listed before it; inverted, the allow-directory action silently
        // no-ops). The default half is EMPTY today (the four agent dirs are the
        // guard-membership set, not rules), so this is the user rules — but the
        // shape stays, because it is the contract and it is what protects the
        // order of whatever the defaults become. ONE concat per turn, next to
        // the `file_policy` re-read, so the gate and the executors judge the
        // SAME list for the whole turn and a mid-turn hand-edit lands at the
        // NEXT prompt (Deviation 4).
        // UNCONDITIONAL — `self.rules` starts as `Vec::new()` and is
        // overwritten here on every prompt, so it is never the empty init
        // again once a prompt has been handled.
        self.rules = {
            let mut rules = self.default_rules.clone();
            rules.extend(self.file_policy.rules.iter().cloned());
            rules
        };
        self.web = crate::agent::web::config::WebConfig::from_web_settings(
            settings.as_ref().map(|s| &s.web),
        );
    }

    /// The ONE `ToolCtx` construction (moved verbatim from `tools.rs`): the
    /// frozen boundary + guard list, the per-turn rule list and policy, the
    /// web config, and the render requester — the exact set the gate judged
    /// with, so an executor can never re-decide differently from its gate.
    ///
    /// `cwd` is the loop's `space_cwd` (an `AgentLoop` field, not permission
    /// state — see the struct doc) and `cancel` is the TURN token (a per-call
    /// value; a Stop must kill the tool child). `skill_roots: None` = the real
    /// discovery roots (the same `discover_skills` the prompt builder used) —
    /// the test seam that pins them never goes through here.
    fn to_tool_ctx(&self, cwd: PathBuf, cancel: CancellationToken) -> crate::agent::tools::ToolCtx {
        crate::agent::tools::ToolCtx {
            cwd,
            cancel,
            // `None` = the real discovery roots (the same
            // `discover_skills` the prompt builder used).
            skill_roots: None,
            // (ADR 0030) The frozen boundary + deny-list.
            boundary: self.read_boundary.clone(),
            protected: self.protected.clone(),
            // (T03) The rule list IN FORCE for this turn — the
            // SAME concatenated list the gate judged with (one
            // concat per turn, so gate and executor agree).
            rules: self.rules.clone(),
            // (T03) The home `rule_for` expands a `~` rule
            // against (frozen at construction — no per-call
            // `$HOME` read).
            home: self.rules_home.clone(),
            // (ADR 0030 Task 3) The policy the gate decided with
            // — the SAME per-turn value, so the executor cannot
            // re-decide differently from the gate (it enforces
            // the `Sandboxed` floor and confines `bash`).
            file_policy: self.file_policy.clone(),
            web: self.web.clone(),
            render_requester: self.render_requester.clone(),
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
    /// The settings dir (the `settings.json` home — ADR 0066:
    /// `resolve_launch` reads the `agentOverrides` entry for the agent's name
    /// from here at dispatch time; `None` = no override layer).
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
    pub prompt_tx: mpsc::Sender<PromptTurn>,
    prompt_queue: mpsc::Receiver<PromptTurn>,
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
    /// (finding 6) The permission cluster — the 8-field cluster + `web` +
    /// `render_requester` — owned by ONE bundle (see [`PermissionContext`]):
    /// the frozen half computed once at construction, the per-turn half
    /// refreshed at the top of every `handle_prompt`, and the ONE `ToolCtx`
    /// construction. `space_cwd` STAYS a field of this loop (it is the
    /// write-boundary root, the trust key and the subagent parent's cwd, not
    /// permission state) and arrives as `to_tool_ctx`'s `cwd` argument.
    permissions: PermissionContext,
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
    /// (finding 6) The turn-scoped "compact before the next model call" latch
    /// — see [`TurnControl`] (the type owns the one-turn lifetime; `arm` sets
    /// it, `enter_turn` — called from `begin_turn` — is the ONLY clear).
    turn_control: TurnControl,
    /// The session's current context size in tokens (the `context_usage_
    /// update` frame source — the frontend's context-percentage display).
    ///
    /// The SAME snapshot the compaction threshold reads: the last `Usage`
    /// the provider reported (`input + output` — the full prompt plus that
    /// reply's output) plus the local estimate of whatever was appended
    /// since (the `Compactor`'s anchor + trailing tail). One number for the
    /// bar and the trigger — never the sum of every call's prompt.
    last_context_tokens: u64,
}

impl AgentLoop {
    // 22 args — kept; > default threshold 7 (no clippy.toml). A LoopParams
    // collapse is out of F6's scope (the commit is the type pair, not the lint
    // allow). The permission cluster is computed INSIDE this constructor from
    // `space_cwd` + `config_dir`, never passed in, so extracting it into
    // [`PermissionContext`] removed field-lines, not parameters.
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
        prompt_tx: mpsc::Sender<PromptTurn>,
        prompt_queue: mpsc::Receiver<PromptTurn>,
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
        // The stamp for this Worker's life (ADR 0049): EVERY id the harness
        // mints is prefixed with it, so a Resume's new loop — whose counters
        // restart at 0 — cannot mint an id its predecessor already used.
        incarnation: String,
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
        // (ADR 0030 / finding 6) The permission cluster — the frozen half
        // (boundary + guard list + default rules + `~`-expansion home, all from
        // the ONE `$HOME` read) and the inert per-turn values — is built by the
        // bundle itself; see [`PermissionContext`].
        let permissions = PermissionContext::new(config_dir.as_deref(), &space_cwd);
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
            permissions,
            thinking_level: None,
            turn_state: std::sync::Mutex::new(TurnState::stamped(incarnation)),
            text_acc: std::sync::Mutex::new(HashMap::new()),
            tool_state: std::sync::Mutex::new(HashMap::new()),
            thought_state: std::sync::Mutex::new(ThoughtState::default()),
            messages: Vec::new(),
            compactor,
            turn_control: TurnControl::default(),
            last_context_tokens: 0,
        }
    }

    pub fn set_render_requester(
        &mut self,
        requester: Option<Arc<dyn crate::agent::web::RenderRequester>>,
    ) {
        self.permissions.render_requester = requester;
    }

    /// Queue a prompt (best-effort — a full / closed queue is dropped).
    pub fn send_prompt(&self, text: &str, images: &[ImageRef]) {
        let _ = self.prompt_tx.try_send(PromptTurn {
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
        let switched = ModelKey::from(&self.model) != ModelKey::from(&model);
        if switched {
            self.provider = crate::agent::harness::provider::build_provider(&model);
        }
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
        self.permissions.enabled_tools = tools;
    }

    /// The enabled tools (`None` = ALL — finding 13b; the
    /// `dispatch_native` driver derives the child's tool set from it).
    pub fn enabled_tools(&self) -> Option<&[String]> {
        self.permissions.enabled_tools.as_deref()
    }

    /// Set the per-agent permission ruleset (issue #101) — `None` = the file
    /// does not speak about permissions (no narrowing). The gate consumes it
    /// in the strict-of-two merge; a definition's rules can only NARROW the
    /// user's `filePolicy`, never widen it.
    pub fn set_permission(
        &mut self,
        permission: Option<crate::agent::definition_permission::PermissionBlock>,
    ) {
        self.permissions.permission_block = permission;
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
                    Some(ControlCmd::Compact { instructions }) => {
                        // Arm a fresh TURN token here (never in
                        // `WorkerCore::handle`: a `Compact` can arrive while a
                        // turn is still in flight — the command QUEUES here —
                        // and arming there would swap out the token that
                        // in-flight turn is racing, so a Stop for it would be
                        // silently lost). The existing Stop path
                        // (`Inbound::Abort`) cancels whatever token is in the
                        // shared mutex, so THIS is what makes an idle
                        // compaction cancellable — `summarize` already races
                        // `turn.cancelled()`. The cancelled token is left in
                        // place: this `select!`'s turn-cancel arm re-arms it,
                        // and `handle_prompt` arms its own fresh token per
                        // prompt, so a cancel cannot leak into the next turn.
                        let turn = {
                            let fresh = CancellationToken::new();
                            *self.turn_cancel.lock().unwrap_or_else(|p| p.into_inner()) =
                                fresh.clone();
                            fresh
                        };
                        self.run_compaction(CompactionReason::Manual, false, instructions, &turn)
                            .await;
                    }
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
    pub async fn handle_prompt(&mut self, prompt: &PromptTurn) {
        // (ADR 0030 Deviation 4) The policy, the rule concat and the web
        // config are re-read ONCE per turn — `PermissionContext::refresh`
        // (the same in-process read `launch.rs` does per dispatch, so a user
        // who flips a select in Settings affects an ALREADY-RUNNING session at
        // its next prompt; ONE read per turn, so every tool call in a turn
        // agrees). The setter-driven members (`permission_block` /
        // `enabled_tools` / `render_requester`) are not touched by it.
        self.permissions.refresh(self.config_dir.as_deref());
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
        self.begin_turn(text, images);

        let mut state = turn::TurnLoopState {
            turn_tool_results: Vec::new(),
            message_started: false,
            overflow_recovered: false,
        };

        loop {
            if turn.is_cancelled() {
                self.settle_cancelled(std::mem::take(&mut state.turn_tool_results));
                return;
            }

            // (1) Compaction check
            if let turn::TurnFlow::Return = self
                .compact_if_due(turn, &mut state.turn_tool_results)
                .await
            {
                return;
            }

            // (2) Model call
            let req = self.model_request();
            let provider = &self.provider;
            let mut retry = self.retry;
            let mut emit = |ev: RpcEvent| self.emit(ev);
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
                    self.settle_cancelled(std::mem::take(&mut state.turn_tool_results));
                    return;
                }
            };
            self.retry = retry;
            let mut stream = match stream {
                Ok(s) => s,
                Err(ProviderError::ContextOverflow(_)) if !state.overflow_recovered => {
                    state.overflow_recovered = true;
                    self.compactor.turn_abandoned(&self.messages);
                    self.turn_control.arm(CompactionReason::Overflow, true);
                    continue;
                }
                Err(e) => {
                    eprintln!("harness: model call failed: {e}");
                    self.settle_error(std::mem::take(&mut state.turn_tool_results));
                    return;
                }
            };

            // (3) Consume stream
            let mut acc = TurnAccumulator::default();
            if !state.message_started {
                self.emit(RpcEvent::message_start {
                    message: json!({ "role": "assistant" }),
                });
                state.message_started = true;
            }

            let mid_stream_error = self.consume_stream(&mut stream, turn, &mut acc).await;
            if turn.is_cancelled() {
                self.settle_cancelled(std::mem::take(&mut state.turn_tool_results));
                return;
            }
            if let Some(err) = mid_stream_error {
                match self
                    .handle_mid_stream_error(err, turn, &mut state, &acc)
                    .await
                {
                    turn::TurnFlow::Continue => continue,
                    turn::TurnFlow::Return => return,
                }
            }

            // A `stop` that streamed NEITHER text NOR a tool call is not a
            // turn — it is the signature of a model that emitted its tool call
            // as literal `<invoke name="…">` text INSIDE the reasoning channel,
            // so the API reports `content: ""` / `tool_calls: null` and the
            // stream reads as a clean stop. The reasoning text is streamed to
            // the UI but NEVER persisted (this site persists `acc.text` alone),
            // so the intended call is invisible everywhere downstream: pre-fix
            // this arm accepted the attempt, wrote the 33-byte empty assistant
            // row and settled as a clean stop, and the user re-prompted every
            // time. It is therefore retried on the SHARED budget, and the empty
            // message is never pushed / persisted; with the budget spent the
            // turn settles as a VISIBLE failure. `acc.thinking` deliberately
            // does NOT gate this — non-empty reasoning IS the shape of the
            // failure — and `handle_empty_turn`'s `eprintln` is the only trace
            // it leaves, since the reasoning text itself is lost.
            //
            // `Some(Stop)` keeps this disjoint from `consume_stream`, which has
            // already turned a stream that ended with NO `finish_reason` into a
            // `Retryable` error handled above.
            //
            // BEFORE `note_success` (and so before `message_end` / the push /
            // the persist) for the same reason the mid-stream arm above is: an
            // abandoned attempt is not a successful call, so it must not report
            // `auto_retry_end { success: true }` a moment before retrying again.
            if acc.finish == Some(FinishReason::Stop)
                && acc.text.is_empty()
                && acc.tool_calls.is_empty()
            {
                match self.handle_empty_turn(turn, &mut state, &acc).await {
                    turn::TurnFlow::Continue => continue,
                    turn::TurnFlow::Return => return,
                }
            }

            if let Some(attempt) = self.retry.note_success() {
                self.emit(RpcEvent::auto_retry_end {
                    success: true,
                    attempt,
                    final_error: None,
                });
            }

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
            self.note_context();
            state.overflow_recovered = false;

            if tool_calls.is_empty() {
                self.emit(RpcEvent::agent_end {
                    messages: vec![self.assistant_message_value(&acc)],
                    will_retry: false,
                });
                self.emit(RpcEvent::turn_end {
                    message: Value::Null,
                    tool_results: state.turn_tool_results,
                });
                self.emit(RpcEvent::agent_settled);
                return;
            }

            // (4) Tool execution
            let batch_results = self.run_tool_batch(&tool_calls, turn).await;
            state.turn_tool_results.extend(batch_results);
            state.message_started = false;
        }
    }

    /// The compaction (the `compact.rs` orchestration — the OLDER messages
    /// summarized and replaced by the summary, the transcript REWRITTEN) +
    /// the `compaction_start` / `compaction_end` `RpcEvent` emission (which
    /// needs the loop's `normalize` / display-persistence pipeline), the
    /// post-compaction `context_usage_update` frame, and the summary call's
    /// own `cost_update` frame (issue #106 — the summariser's provider figure
    /// goes through `emit_cost_update`, so a priced summary call reaches the
    /// cost channel like any other priced call, and a `None` emits nothing).
    ///
    /// `reason` is the TRIGGER ([`CompactionReason`] — `Manual` for `/compact`,
    /// `Threshold` for the context-pressure check, `Overflow` for a provider
    /// context-overflow recovery; the wire spellings `"manual"` / `"threshold"`
    /// / `"overflow"` are the enum's serde contract) — it rides both events and
    /// the renderer's `compaction_update` state frame. `will_retry` is the
    /// event's "the turn continues on the other side of this compaction" flag.
    /// `instructions` are the user's extra summary instructions
    /// (`/compact <args>`); the automatic paths (threshold / overflow) pass
    /// `None`.
    ///
    /// Returns [`CompactionRan`] — the three-way outcome, because a caller
    /// recovering from an overflow must NOT read "shrunk nothing" as one fact:
    /// `Skipped` means the kept tail alone is over the window (a second request
    /// is doomed), `Failed` means the SUMMARY call errored (which says nothing
    /// about the turn's own request, so the turn is re-issued).
    ///
    /// For [`CompactionReason::Overflow`] the SAME `compaction_end` also carries
    /// an ACTIONABLE `error_message` when the transcript did not shrink (ADR
    /// 0045): the one event the user sees must say what to do (a larger window,
    /// or that the summary call failed and the transcript is unchanged). It is
    /// composed HERE rather than by the caller because this is the only place
    /// that emits `compaction_end` — a second one would double the transcript
    /// line and the state frame.
    ///
    /// `will_retry` is reconciled with the outcome HERE for the same reason: it
    /// is a promise about what the loop does next, and the only party that
    /// knows both halves is this function. An overflow recovery that lands on
    /// `Skipped` settles the turn, so it must not promise a retry.
    async fn run_compaction(
        &mut self,
        reason: CompactionReason,
        will_retry: bool,
        instructions: Option<String>,
        turn: &CancellationToken,
    ) -> CompactionRan {
        self.emit(RpcEvent::compaction_start { reason });
        let mut aborted = false;
        let mut error_message: Option<String> = None;
        // The compaction's own output (pi's `CompactionResult` shape), shown on
        // the transcript card. `None` unless the summary was actually produced.
        let mut result: Option<Value> = None;
        // The overflow recovery's outcome needs a DIFFERENT message from the
        // transient ones (below): only the user can act on a window too small.
        let overflow = reason == CompactionReason::Overflow;
        // What actually happened (the return value — see above).
        let mut ran = CompactionRan::Skipped;
        // The keep-recent budget comes from the COMPACTOR (its config copy +
        // THIS model's window), not from `catalog.compaction` directly: the
        // catalog budget is catalog-wide while the window is per-model, and an
        // uncapped budget can cover the whole transcript of a small-window
        // model — a compaction that shrinks nothing (see `keep_budget`).
        // Computed BEFORE the ctx: `keep_budget(&self)` must not overlap the
        // `compactor: &mut self.compactor` borrow.
        let keep_recent_tokens = self.compactor.keep_budget();
        let mut ctx = compact::CompactionCtx {
            session_id: &self.session_id,
            model: &self.model,
            provider: &*self.provider,
            store: &self.store,
            keep_recent_tokens,
            instructions,
            compactor: &mut self.compactor,
        };
        match compact::run_compaction(&mut ctx, &mut self.messages, turn).await {
            compact::CompactionOutcome::Skipped => {
                // (ADR 0045) An overflow that found NOTHING old enough to
                // summarize is a dead end the user can act on (only a bigger
                // window fixes it), so the ONE `compaction_end` carries that
                // sentence instead of leaving the caller to invent a second
                // event. `aborted` stays `false` — a `Skipped` compaction is
                // not an abort, nothing failed.
                if overflow {
                    error_message = Some(
                        "Context overflow: not enough older context to compact — try a larger-context model."
                            .to_string(),
                    );
                }
            }
            compact::CompactionOutcome::Compacted {
                context_tokens,
                summary,
                cost,
            } => {
                ran = CompactionRan::Compacted;
                // The summary is the compaction's OUTPUT — the one thing worth
                // reading and, until issue #90, visible only to the model. It
                // rides `result` (pi's `CompactionResult` carries a `summary`
                // field for the same reason) so `normalize` can render it as the
                // card's body.
                result = Some(json!({ "summary": summary }));
                // The re-estimate is the post-compaction BASELINE (the
                // `context_usage_update` frame — the percentage drops
                // after the compaction instead of staying at the
                // pre-compaction value).
                self.last_context_tokens = context_tokens;
                self.emit_context_usage();
                // Issue #106: the summary call's own provider figure reaches
                // the cost channel through the SAME emitter the turn's `Usage`
                // arm uses — one frame, this call's DELTA, keyed to
                // `self.session_id` by the emitter. A `None` cost emits nothing
                // (the emitter's own gate), never a phantom `$0.00`. Emitted
                // HERE, inside the arm, so no other outcome can carry it and no
                // early exit can skip it.
                self.emit_cost_update(cost);
            }
            compact::CompactionOutcome::Failed { error } => {
                ran = CompactionRan::Failed;
                aborted = true;
                // An overflow's failure says what it cost — the transcript was
                // left AS-IS — without claiming the turn is over: the caller
                // re-issues the request (issue #90), because a failed SUMMARY
                // says nothing about whether the turn's own request fits.
                error_message = Some(if overflow {
                    format!(
                        "Context overflow: the summary call failed ({error}) — the transcript was left unchanged."
                    )
                } else {
                    error
                });
            }
        }
        // `will_retry` is the promise that the turn continues on the other side
        // of this compaction, so it is RECONCILED with what the caller is about
        // to do rather than echoed from the trigger: the one overflow outcome
        // that settles the turn is `Skipped` (the kept tail alone is over the
        // window, so a second request cannot succeed — ADR 0045's "no doomed
        // second request"). Pre-fix this line carried the trigger's `true`
        // straight through to a dead turn.
        let will_retry = will_retry && !(overflow && ran == CompactionRan::Skipped);
        self.emit(RpcEvent::compaction_end {
            reason,
            result,
            aborted,
            will_retry,
            error_message,
        });
        ran
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
            tools: Self::advertised_tool_specs(
                &self.permissions.enabled_tools,
                self.subagent.is_none(),
            ),
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
        Self::advertised_tool_specs(&self.permissions.enabled_tools, self.subagent.is_none())
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

pub(crate) use super::tool_specs::tool_specs;

mod emit;
mod gate;
mod tools;
mod turn;

use emit::TurnAccumulator;

#[cfg(test)]
mod tests;

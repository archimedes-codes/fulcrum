//! The subagent session manager: runs delegated subagent sessions on the
//! SAME shared [`SessionDriver`] machinery as [`SessionManager`].
//!
//! A subagent session is **ephemeral** (not persisted — `db: None`), runs
//! on the main runtime (a `tokio::spawn` driver task), and builds an
//! in-process native `AgentLoop` (the desktop is native-only — there is
//! no external pi path). Subagents cannot dispatch subagents (the tool is
//! excluded from their spawn — `subagent: None`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::agent::harness::{
    build_child_system_message, tool_specs, AgentLoop, Model, Prompt, RetryPolicy, SessionStore,
    SudoDeps,
};
use crate::agent::interactive;
use crate::agent::permission::{self, PermissionOutcome};
use crate::agent::session::{
    resolve_composed_model, CloseKind, EffectiveCatalog, EventSink, ExternalClose, ProviderFactory,
    SessionDriver, SessionInfo,
};
use crate::agent::todo::TodoStore;
use crate::storage::Db;

/// Manages all live SUBAGENT sessions.
///
/// Owns a shared [`SessionDriver`] (db: `None` — ephemeral, `trust_db`:
/// threaded via [`Self::new`], subagent: `None`) whose
/// `sessions` / `pending_*` maps EVERY per-dispatch driver shares (the
/// manager's `respond_*` and the driver-task cleanup operate on the shared
/// maps). Each dispatch builds a FRESH driver on top of the shared maps
/// (fresh `text_capture` / `last_message_id` /
/// `cost_capture` — a concurrent dispatch must not clobber another's final
/// output, and a no-text dispatch must not return a PREVIOUS dispatch's
/// text). The one-live policy does NOT apply to subagents
/// (subagent sessions are excluded by definition).
pub struct SubagentSessionManager {
    /// The shared driver (db: `None` — ephemeral, `trust_db` threaded via
    /// `new` — the ADR 0010 trust lookup, `subagent: None`), behind an `Arc`:
    /// its `sessions` / `pending_*` maps are shared by every per-dispatch
    /// driver (see `dispatch_native`), and `respond_*` reads them here.
    driver: Arc<SessionDriver>,
    /// The native-harness deps (`dispatch_native` reads
    /// `self.native_deps.get()`); set ONCE at the `SessionManager`
    /// wiring time (`set_subagent_manager` — the manager is received as
    /// an `Arc`, so the setter takes `&self` + a `OnceLock`).
    native_deps: std::sync::OnceLock<NativeDeps>,
}

/// Per-dispatch pi configuration for a subagent session (moved verbatim
/// from `launch_wrapper.rs` — the wrapper script is deleted in this task;
/// the flags are now passed directly to the `pi` spawn).
#[derive(Clone, Debug, Default, PartialEq)]
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

/// The deps `dispatch_native` needs to build a native `AgentLoop` (the
/// pieces `build_native_session` consumes — the `ProviderFactory` /
/// `EffectiveCatalog` / `TodoStore` / `SudoDeps` / the child's settle
/// bound). Set ONCE at the `SessionManager` wiring time
/// (`set_subagent_manager`) via [`Self::set_native_deps`] (a `&self`
/// `OnceLock` — the manager is received as an `Arc` there). The child's
/// THROWAWAY `SessionStore` `Db` is NOT a field (it is built FRESH in
/// `dispatch_native` as `Db::open(Path::new(":memory:"))`).
#[derive(Clone)]
pub struct NativeDeps {
    pub provider_factory: ProviderFactory,
    /// The EFFECTIVE catalog supplier (the base catalog + the settings'
    /// providers + live discovery — resolved at dispatch time, NOT a
    /// startup snapshot: the desktop is native-only, so a named agent's
    /// `model:` frontmatter resolves against the effective catalog, and
    /// the base catalog is empty after the pi-config seeding removal).
    pub catalog: EffectiveCatalog,
    pub todo_store: Arc<TodoStore>,
    pub sudo: SudoDeps,
    /// The child's settle bound (default 30 min — the `SessionDriver`
    /// `settle_timeout`; a later task's tests shorten it).
    pub settle_timeout: Duration,
    /// The TRUST lookup source threaded onto the child `AgentLoop`
    /// (ADR 0010 — a native child in a trusted Space inherits the
    /// parent's trust: the permission gate auto-approves mutating tools,
    /// matching the external `dispatch`, which threads `driver.trust_db`
    /// onto its child). `None` = fail-closed (the gate prompts on every
    /// `bash` / `edit` / `write` in the child).
    pub trust_db: Option<Arc<Db>>,
    /// The settings dir (ADR 0019: the child `AgentLoop`'s MCP manager
    /// loads the desktop `mcpServers` layer from `settings.json`).
    pub config_dir: Option<PathBuf>,
}

/// An `EventSink` decorator (the `dispatch_native` child's sink): wraps
/// the parent's real sink + accumulates the `agent_message_chunk` text
/// per `messageId` (the `last_message_id`'s accumulated text is the final
/// output — mirroring the driver's `captures()`; a tool-using turn does
/// NOT concatenate every intermediate message). The normalizer's
/// `messageId: "system"` bookkeeping chunks ("Retry failed", "Compacting
/// context…") are NEVER captured (a failed child turn settles with the
/// LAST captured chunk as its output — a bookkeeping one-liner must not
/// hijack the subagent's answer) but ARE still forwarded (the user sees
/// the one-liner).
///
/// **CRITICAL — the frame is ENVELOPED**: the child's `emit` helper (the
/// `AgentLoop`) does `self.sink.emit("session-update", json!({
/// "sessionId", "update" }))` — so `EventSink::emit` receives the
/// ENVELOPE `{ sessionId, update }`, NOT the bare normalized frame. The
/// `agent_message_chunk` detection reads `payload["update"]`.
///
/// EVERY frame is forwarded to the wrapped real sink EXACTLY ONCE (the
/// child's `emit` helper already normalizes + `sink.emit`s — a re-emit
/// here would double-emit; the driver is capture-only).
pub struct CapturingSink {
    /// The parent's real sink (the forward target — one emission).
    real: Arc<dyn EventSink>,
    /// `messageId` → accumulated text (a `HashMap` has no insertion
    /// order — the LAST id is tracked separately, below).
    text: StdMutex<HashMap<String, String>>,
    /// The LAST `messageId` with a non-empty capture (updated on every
    /// non-empty capture of a NON-`system` chunk — the final-output
    /// source, mirroring the driver's `last_message_id`).
    last_message_id: StdMutex<Option<String>>,
}

impl CapturingSink {
    /// Wrap the parent's real sink (the forward target).
    pub fn new(real: Arc<dyn EventSink>) -> Self {
        Self {
            real,
            text: StdMutex::new(HashMap::new()),
            last_message_id: StdMutex::new(None),
        }
    }

    /// The `last_message_id`'s accumulated text ("" when no chunk arrived
    /// — the final-output source; a tool-using turn does NOT concatenate
    /// every intermediate message).
    ///
    /// **Lock order**: the `last_message_id` value is cloned out FIRST
    /// (the guard is dropped) and `text` is locked SECOND — `emit` holds
    /// `text` while locking `last_message_id` (the reverse), so holding
    /// BOTH at once would be an ABBA inversion (a concurrent
    /// `captured_text` + `emit` would deadlock).
    pub fn captured_text(&self) -> String {
        let last = self
            .last_message_id
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        last.and_then(|id| {
            self.text
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&id)
                .cloned()
        })
        .unwrap_or_default()
    }

    /// The captured `messageId`s (arbitrary order — a `HashMap` has no
    /// insertion order).
    pub fn captured_ids(&self) -> Vec<String> {
        self.text
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}

impl EventSink for CapturingSink {
    /// **Lock order**: `text` is locked FIRST (and held while
    /// `last_message_id` is locked) — `captured_text` must NOT take the
    /// reverse order (see it for the ABBA rationale).
    fn emit(&self, event: &str, payload: Value) {
        // The ENVELOPED frame: capture the `agent_message_chunk` deltas
        // (`messageId` + `content.text`; an empty delta is skipped, and
        // so is the normalizer's `messageId: "system"` bookkeeping chunk
        // — "Retry failed", "Compacting context…": the UI's one-liners,
        // NOT the subagent's answer. A failed child turn settles with
        // the LAST captured chunk as its output — a bookkeeping chunk
        // must not hijack it. The frame is still forwarded below — only
        // the capture is skipped).
        if event == "session-update" {
            let update = payload.get("update");
            if update
                .and_then(|u| u.get("sessionUpdate"))
                .and_then(Value::as_str)
                == Some("agent_message_chunk")
            {
                let mid = update
                    .and_then(|u| u.get("messageId"))
                    .and_then(Value::as_str);
                let delta = update
                    .and_then(|u| u.get("content"))
                    .and_then(|c| c.get("text"))
                    .and_then(Value::as_str);
                if let (Some(mid), Some(delta)) = (mid, delta) {
                    if !delta.is_empty() && mid != "system" {
                        let mut text = self.text.lock().unwrap_or_else(|p| p.into_inner());
                        text.entry(mid.to_string()).or_default().push_str(delta);
                        *self
                            .last_message_id
                            .lock()
                            .unwrap_or_else(|p| p.into_inner()) = Some(mid.to_string());
                    }
                }
            }
        }
        // ALWAYS forward (one emission — NEVER re-emit: the child's
        // `emit` helper already normalized + `sink.emit`ed). Includes
        // the `system` bookkeeping chunks (the user sees the one-liner).
        self.real.emit(event, payload);
    }
}

impl SubagentSessionManager {
    /// Create a manager (a shared driver with the capture hooks enabled per
    /// dispatch). `trust_db` is the TRUST lookup source threaded onto the
    /// shared driver (ADR 0010 — subagent Sessions inherit Space trust
    /// through it; `None` = fail-closed, the gate always prompts). It is
    /// SEPARATE from the driver's `db` (transcript persistence — always
    /// `None` for subagents, which are ephemeral): threading the full db
    /// here would make subagent updates attempt transcript inserts (a FK
    /// failure — subagent sessions have no `sessions` row).
    pub fn new(trust_db: Option<Arc<Db>>) -> Self {
        // The shared driver (db: `None` — ephemeral; `subagent: None` —
        // subagents cannot dispatch subagents): its `sessions` /
        // `pending_*` maps are shared by EVERY per-dispatch driver (the
        // manager's `respond_*` and the driver-task cleanup operate on
        // these maps). The captures are per-dispatch (a fresh `Some`
        // instance in `dispatch_native` — a concurrent dispatch must not
        // clobber another's final output).
        let mut driver = SessionDriver::new();
        // The trust lookup source (ADR 0010 — subagent Sessions inherit
        // Space trust through `trust_db`; the driver's `db` stays `None`
        // — subagents are ephemeral and must not attempt transcript
        // inserts).
        driver.trust_db = trust_db;
        Self {
            driver: Arc::new(driver),
            native_deps: std::sync::OnceLock::new(),
        }
    }

    /// The shared driver (exposed for tests — `respond_*` and the
    /// per-dispatch drivers read its shared `sessions` / `pending_*` maps).
    pub fn driver(&self) -> &SessionDriver {
        &self.driver
    }

    /// Store the native-harness deps (set-once — a second call is a
    /// NO-OP; the `OnceLock` is set at the `SessionManager` wiring time,
    /// so the setter takes `&self` and is callable through the `Arc`
    /// `set_subagent_manager` receives).
    pub fn set_native_deps(&self, deps: NativeDeps) {
        let _ = self.native_deps.set(deps);
    }

    /// The native-harness deps (`None` until `set_native_deps` — a
    /// manager that was never wired into a `SessionManager` with a `db`
    /// is external-pi-only).
    pub fn native_deps(&self) -> Option<&NativeDeps> {
        self.native_deps.get()
    }

    /// The subagent's persistence database (always `None` — subagents are
    /// ephemeral). Exposed for symmetry with `SessionManager`.
    pub fn db(&self) -> Option<Arc<Db>> {
        self.driver.db.clone()
    }

    /// Spawn an IN-PROCESS native child `AgentLoop` for a subagent
    /// dispatch (the native-native subagent — Task 2): the driver task
    /// (the MAIN runtime — in-process, no external process) builds the child
    /// (the parent's model / tools minus `subagent`, `subagent: None`
    /// — the recursion guard, a [`CapturingSink`], a THROWAWAY `Db`),
    /// spawns it, drives it (the task, the settle vs `settle_timeout`
    /// vs the [`SubagentCancel`] race, the UNCONDITIONAL teardown on
    /// EVERY exit), and resolves the [`SubagentOutcome`].
    ///
    /// The child's frames flow to the UI **once** (the `CapturingSink`
    /// forwards to the parent's real sink — the child's `emit` helper
    /// already normalizes + `sink.emit`s, so the driver is
    /// capture-only: it does NOT re-normalize / re-emit — that would
    /// double-emit). The child's `native_messages` writes go to the
    /// throwaway `Db` (discarded on teardown — never the parent's real
    /// `Db`). `parent_enabled_tools` is the parent's `enabled_tools`
    /// (`[]` = all — the documented harness convention).
    ///
    /// `None` `NativeDeps` (a manager never wired into a `SessionManager`
    /// with a `db`) → an already-resolved `Failed` (NO pi fallback — a
    /// native parent always has an attached `db` in production; the
    /// "no manager" case is handled by `dispatch_subagent`'s existing
    /// `subagent: None` check).
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
        sink: &Arc<dyn EventSink>,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        self.dispatch_native_inner(
            parent_session_id,
            parent_cwd,
            parent_model,
            parent_enabled_tools,
            agent_name,
            launch,
            task,
            sink,
            false,
        )
    }

    /// The test-only temp-file-forcing variant of [`Self::dispatch_native`]:
    /// forces the throwaway `Db` temp-file fallback (the `:memory:` path is
    /// not failable in practice, so this is the only way to exercise the
    /// fallback + its cleanup end-to-end). Production ALWAYS uses `:memory:`
    /// first (`force_temp_file: false`).
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch_native_force_temp_file(
        &self,
        parent_session_id: &str,
        parent_cwd: &Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        sink: &Arc<dyn EventSink>,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        self.dispatch_native_inner(
            parent_session_id,
            parent_cwd,
            parent_model,
            parent_enabled_tools,
            agent_name,
            launch,
            task,
            sink,
            true,
        )
    }

    /// The `dispatch_native` driver (the shared core of
    /// [`Self::dispatch_native`] + [`Self::dispatch_native_force_temp_file`]):
    /// `force_temp_file` (test-only) forces the throwaway `Db` temp-file
    /// fallback (the `:memory:` path is not failable in practice).
    #[allow(clippy::too_many_arguments)]
    fn dispatch_native_inner(
        &self,
        parent_session_id: &str,
        parent_cwd: &Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        sink: &Arc<dyn EventSink>,
        force_temp_file: bool,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        let Some(deps) = self.native_deps().cloned() else {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(SubagentOutcome::Failed {
                error: "native subagent dispatch is not configured".to_string(),
            });
            let (_, cancel) = SubagentCancel::new_external_close();
            return (rx, cancel);
        };
        let (dispatch_tx, dispatch_rx) = oneshot::channel();
        // The external close (the driver's cancel path — it keeps the
        // `rx`) + the cancel handle (the caller's cancel path — `cancel()`
        // flips the flag the `ec.rx.changed()` arm sees).
        let (ec, sub_cancel) = SubagentCancel::new_external_close();
        let mut cancel_rx = ec.rx;
        // Owned clones so the driver task borrows NOTHING from `self`
        // (`tokio::spawn` requires `Future + Send + 'static`): the
        // manager's SHARED `pending_*` maps (the UI's `respond_*` —
        // which search the manager's shared maps — can resolve the
        // child's prompts; fresh maps would hang the child until
        // `settle_timeout`), and owned copies of the arguments.
        let pending_permissions = self.driver.pending_permissions.clone();
        let pending_bridge = self.driver.pending_bridge.clone();
        let parent_session_id = parent_session_id.to_string();
        let parent_cwd = parent_cwd.to_path_buf();
        let parent_model = parent_model.clone();
        let sink = sink.clone();
        // The driver task (the MAIN runtime — in-process; the oneshot is
        // the observation point, so the `JoinHandle` is detached — the task
        // runs until it resolves the oneshot).
        tokio::spawn(async move {
            let start = std::time::Instant::now();

            // 0. The EFFECTIVE catalog (the base catalog + the settings'
            // providers + live discovery — resolved NOW, at dispatch time:
            // a named agent's `model:` frontmatter must resolve against the
            // effective catalog, not a startup snapshot).
            let catalog = deps.catalog.resolve(None).await;

            // 1. The child `Model`: a `launch.model` override — a
            // trailing `:<level>` suffix is stripped FIRST (the REAL
            // `resolve_composed_model` splits `provider/id` only — it
            // does NOT split the suffix); the suffix is a candidate
            // thinking level (step 2, when `launch.thinking` is `None`)
            // — `None` (unknown / malformed) → `Failed` (no fallback).
            let (model, level_suffix) = match &launch.model {
                Some(key) => {
                    let (bare, suffix) = key
                        .rsplit_once(':')
                        .map(|(b, s)| (b.to_string(), Some(s.to_string())))
                        .unwrap_or_else(|| (key.clone(), None));
                    match resolve_composed_model(&catalog, &bare) {
                        Some(m) => (m, suffix),
                        None => {
                            let _ = dispatch_tx.send(SubagentOutcome::Failed {
                                error: format!("unknown model: {bare}"),
                            });
                            return;
                        }
                    }
                }
                None => (parent_model, None),
            };
            // 2. The child thinking level: explicit > the `:<level>` suffix of
            // the resolved model key > the frontmatter's `thinking` (the
            // doc-correct order — the frontmatter's `thinking` is the LAST rung).
            // Validate against `model.thinking_levels` (when non-empty — an empty
            // set soft-passes) — a mismatch is DROPPED (never sent upstream as a
            // bogus `reasoning_effort`).
            let mut thinking = launch
                .thinking
                .clone()
                .or(level_suffix)
                .or(launch.frontmatter_thinking.clone());
            if let Some(level) = &thinking {
                if !model.thinking_levels.is_empty()
                    && !model.thinking_levels.iter().any(|l| l == level)
                {
                    thinking = None;
                }
            }
            // 3. The child `Provider` (the `provider_factory` seam).
            let provider = (deps.provider_factory)(&model);
            // 4. The THROWAWAY child `Db` + `SessionStore` (the child's
            // `persist_update` / `persist_transcript_message` write to
            // it — discarded on teardown, never the parent's real
            // `Db`). `:memory:` first (SQLite honors the filename — the
            // `parent()` is `""`, so `create_dir_all` is a no-op `Ok`);
            // a temp file fallback. The `TempFileGuard` is held for the
            // WHOLE driver task (the temp file is removed on ANY exit —
            // happy OR panic/early-return; `None` = the `:memory:` case,
            // a no-op). A `Db` open failure (both `:memory:` and the
            // temp file) is `Failed` (NOT a panic — a panic would drop
            // the oneshot, surfacing as a bare "cancelled" with zero
            // diagnostics).
            let child_id = crate::agent::session::mint_session_id();
            // `_child_db_guard` is held for the WHOLE driver task (dropped
            // at scope end — the `_` only suppresses the unused-variable
            // warning; the `Drop` still runs, removing the temp file on ANY
            // exit).
            let (child_db, _child_db_guard) = match open_throwaway_db(force_temp_file) {
                Ok(v) => v,
                Err(e) => {
                    let _ = dispatch_tx.send(SubagentOutcome::Failed {
                        error: format!("throwaway db unavailable: {e}"),
                    });
                    return;
                }
            };
            // The FK (the `native_messages` rows): record the child
            // session (the `SessionInfo` has no `Default` — all fields
            // filled; a `Value::Null`-ish capability envelope — the
            // child is ephemeral, nothing reads its capabilities).
            // A `record_session` failure is `Failed` (NOT a panic — the
            // child's `SessionStore` can't satisfy the FK, so failing the
            // dispatch is correct; a panic would drop the oneshot + leak
            // the temp file).
            if let Err(e) = child_db.record_session(&SessionInfo {
                session_id: child_id.clone(),
                cwd: parent_cwd.clone(),
                capabilities: json!({}),
                config_options: None,
                // Ephemeral (subagent) sessions are never archived (ADR 0016).
                archived: false,
            }) {
                let _ = dispatch_tx.send(SubagentOutcome::Failed {
                    error: format!("record the throwaway child session: {e}"),
                });
                // Drop the `child_db` clone BEFORE the `return`: Rust
                // drops locals in REVERSE declaration order, so the
                // `_child_db_guard` would drop FIRST while this clone
                // still holds the `Arc<Db>` — the guard's `Drop` (release
                // the connection first, then remove the file — safe on
                // Windows) would `remove_file` a still-open file (failing
                // silently on Windows, leaking the temp file). Releasing
                // the clone makes the guard the LAST holder: its `Drop`
                // closes the connection, then removes the file.
                drop(child_db);
                return;
            }
            let store = SessionStore::new(child_db);
            // 5. The `CapturingSink` (wraps the parent's real sink — the
            // child's frames flow to the UI ONCE; the final text is
            // captured from it).
            let capturing = Arc::new(CapturingSink::new(sink.clone()));
            // 6. FRESH channels / tokens for the child (NOT the
            // parent's) + the child `AgentLoop` (`subagent: None` — the
            // recursion guard: the child cannot dispatch subagents; the
            // manager's SHARED `pending_*` maps; `trust_db` threaded from
            // the deps — ADR 0010, a native child in a trusted Space
            // inherits the parent's trust; `None` = fail-closed).
            // `prompt_tx.clone()` goes into `new`; the
            // driver KEEPS the original for step 10 (mirroring
            // `build_native_session`).
            let (events_tx, mut events_rx) = mpsc::channel(256);
            let (prompt_tx, prompt_rx) = mpsc::channel(8);
            let (settle_tx, mut settle_rx) = watch::channel(0u64);
            let child_cancel = CancellationToken::new();
            let child_turn_cancel: Arc<StdMutex<CancellationToken>> =
                Arc::new(StdMutex::new(CancellationToken::new()));
            let mut loop_ = AgentLoop::new(
                child_id.clone(),
                parent_cwd.clone(),
                model.clone(),
                provider,
                catalog.clone(),
                store,
                events_tx,
                child_cancel.clone(),
                child_turn_cancel.clone(),
                settle_tx,
                prompt_tx.clone(),
                prompt_rx,
                pending_permissions,
                pending_bridge,
                deps.trust_db.clone(),
                capturing.clone(),
                deps.todo_store.clone(),
                None,
                deps.sudo.clone(),
                RetryPolicy::new(),
                deps.config_dir.clone(),
            );
            // 7. Configure the child (ALL of this happens BEFORE
            // `run()` consumes the loop — `run(mut self)` moves it, so
            // nothing may touch `loop_` after the spawn). The child tool
            // set: `launch.tools` VERBATIM (a non-empty set that empties
            // out after the minus yields NO tools — NOT re-expanded) else
            // the parent's — MINUS `subagent` AND `list_agents` (the
            // recursion guard — a child cannot dispatch, so it gets
            // neither). The "empty = all" expansion applies ONLY to the
            // inherited `parent_enabled_tools` case (where `[]` is the
            // documented harness convention) — `tool_specs()`' names minus
            // `subagent` and `list_agents`.
            let child_tools: Vec<String> = match &launch.tools {
                Some(tools) => tools
                    .iter()
                    .filter(|t| !matches!(t.as_str(), "subagent" | "list_agents"))
                    .cloned()
                    .collect(),
                None => {
                    if parent_enabled_tools.is_empty() {
                        tool_specs()
                            .into_iter()
                            .map(|t| t.name)
                            .filter(|t| !matches!(t.as_str(), "subagent" | "list_agents"))
                            .collect()
                    } else {
                        parent_enabled_tools
                            .iter()
                            .filter(|t| !matches!(t.as_str(), "subagent" | "list_agents"))
                            .cloned()
                            .collect()
                    }
                }
            };
            loop_.set_enabled_tools(Some(child_tools.clone()));
            if let Some(level) = &thinking {
                loop_.set_thinking_level(Some(level.clone()));
            }
            // The child's system message (ADR 0017): [launch.systemPrompt,
            // if any] + the todo guidance line (when the child has the
            // manage_todo_list tool — its tools = the parent's minus
            // subagent); `None` → no system message (today's behavior).
            // `prepend_system` persists it at seq 0 (the child's
            // throwaway Db — the child is ephemeral; the persist is the
            // uniform code path, not a resume record).
            let has_todo_tool = child_tools.iter().any(|t| t == "manage_todo_list");
            if let Some(msg) =
                build_child_system_message(launch.system_prompt.as_deref(), has_todo_tool)
            {
                loop_.prepend_system(msg);
            }
            // 8. Spawn the child loop (it owns `prompt_tx` +
            // `control_tx` — `run()` exits ONLY via `self.cancel`).
            let loop_handle = tokio::spawn(loop_.run());
            // 9. Emit `subagent-session-started` on the parent's REAL
            // sink (NOT the `CapturingSink` — the driver emits lifecycle
            // events directly). The `model` (the COMPOSED `provider/id`
            // form, matching `native_capabilities`) / `thinkingLevel`
            // (the step-2 RESOLVED value — driver-local: `loop_` is
            // consumed by `run()` above, so `loop_.thinking_level()` is
            // unavailable here) / `enabledTools` fields make the child's
            // resolved config observable.
            sink.emit(
                "subagent-session-started",
                json!({
                    "sessionId": child_id,
                    "parentSessionId": parent_session_id,
                    "agentName": agent_name,
                    "task": task,
                    "model": format!("{}/{}", model.provider, model.id),
                    "thinkingLevel": thinking.as_deref(),
                    "enabledTools": child_tools,
                }),
            );
            // 10. The task (the preflight — a `SendError` means the
            // child died before the turn started → `Failed`). The
            // system message was already seeded in step 7 (BEFORE the
            // task prompt).
            let preflight = prompt_tx
                .send(Prompt {
                    text: task.clone(),
                    images: Vec::new(),
                })
                .await;
            // 11. Race: the child's settle (a `changed()` `Err` = the
            // child loop task DIED — the `settle_tx` was dropped —
            // `Failed`, NOT `Completed`, mirroring
            // `drive_native_session`) vs the `settle_timeout` vs the
            // `SubagentCancel` (a user / caller cancel — `cancel()`
            // flips the flag the `cancel_rx` arm sees).
            let race = if let Err(e) = preflight {
                Race::Preflight(e.to_string())
            } else {
                tokio::select! {
                    r = settle_rx.changed() => {
                        if r.is_err() {
                            Race::Died
                        } else {
                            Race::Settled
                        }
                    }
                    _ = tokio::time::sleep(deps.settle_timeout) => Race::TimedOut,
                    _ = cancel_rx.changed() => Race::Cancelled,
                }
            };
            // 12. UNCONDITIONAL teardown (settle / timeout / cancel /
            // preflight error): the session + turn tokens, `abort()`
            // (the belt-and-braces for a child hung inside a provider
            // HTTP read that ignores cancellation — `run()` owns
            // `prompt_tx` + `control_tx`, so it exits only via
            // `self.cancel`), the `events_rx` drain (after
            // `loop_handle.await` the child task has ended — its
            // `events_tx` is dropped and `recv()` returns `None`; a
            // drain that never ends means an orphaned child → the
            // oneshot never resolves), the throwaway `Db` (the temp
            // file, if used).
            child_cancel.cancel();
            child_turn_cancel
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .cancel();
            loop_handle.abort();
            let _ = loop_handle.await;
            while events_rx.recv().await.is_some() {}
            // The throwaway `Db` (the temp file, if used) is cleaned up by
            // the `TempFileGuard` (held for the WHOLE driver task — dropped
            // at scope end, on ANY exit; the child task's `SessionStore`
            // `Arc<Db>` clone was released by the `loop_handle.await` above,
            // so the guard is the sole holder and its `Drop` closes the
            // connection before removing the file).
            // 13. The outcome + `subagent-closed` (the parent's REAL
            // sink). A failed child TURN (an exhausted-retry model
            // call) settles "successfully" via a bare `agent_settled`
            // and reports `Completed { output: "" }` — parity with the
            // pi `dispatch` mapping (a documented follow-up, NOT a bug
            // to fix here).
            let duration_ms = start.elapsed().as_millis() as u64;
            match race {
                Race::Settled => {
                    // `output` = the `last_message_id`'s accumulated
                    // text (the `CapturingSink` — a tool-using turn does
                    // NOT concatenate every intermediate message).
                    let output = capturing.captured_text();
                    let metrics = SubagentMetrics {
                        output: output.clone(),
                        duration_ms,
                        ..Default::default()
                    };
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": child_id,
                            "status": "completed",
                            "metrics": metrics_json(&metrics),
                        }),
                    );
                    let _ = dispatch_tx.send(SubagentOutcome::Completed { output, metrics });
                }
                race => {
                    let error = match race {
                        Race::Preflight(e) => e,
                        Race::TimedOut => "timed out".to_string(),
                        Race::Cancelled => "cancelled".to_string(),
                        Race::Died => "child loop task died".to_string(),
                        Race::Settled => unreachable!("handled above"),
                    };
                    let metrics = SubagentMetrics {
                        duration_ms,
                        ..Default::default()
                    };
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": child_id,
                            "status": "failed",
                            "error": error.clone(),
                            "metrics": metrics_json(&metrics),
                        }),
                    );
                    let _ = dispatch_tx.send(SubagentOutcome::Failed { error });
                }
            }
        });
        (dispatch_rx, sub_cancel)
    }

    /// Resolve a pending request of a SUBAGENT session (its own
    /// interactive channel's map — the `session_id` is the subagent's ACP id,
    /// set on the loop once the ACP id is known).
    ///
    /// Direct map access — callable from ANY runtime (the map is a
    /// `tokio::sync::Mutex`, locked briefly). `false` when the entry is gone
    /// (the session closed, or the request already resolved — the silent
    /// no-op).
    pub async fn respond_interactive_request(
        &self,
        session_id: &str,
        request_id: &str,
        result: Value,
    ) -> bool {
        let key = interactive::interactive_key(session_id, request_id);
        let sender = self.driver.pending_bridge.lock().await.remove(&key);
        match sender {
            Some(sender) => {
                let _ = sender.send(result);
                true
            }
            None => {
                // Phase 2: the `pending_bridge` lookup missed — check the
                // `pending_sudo` sub-prompt oneshots (the `sudo_exec`
                // `:confirm` / `:password` keys, `"{sid}/{id}:confirm"` /
                // `"{sid}/{id}:password"`). The `confirm` /
                // `password` flows still resolve via `pending_bridge` (the
                // lookup above is NOT removed). NOTE the plain-`bool`
                // return (NOT `Result<bool>` — the command shim computes
                // `main_hit || subagent_state.respond_interactive_request(…)`
                // as a `bool`).
                let sudo_sender = self.driver.pending_sudo.lock().await.remove(&key);
                match sudo_sender {
                    Some(sender) => {
                        let _ = sender.send(result);
                        true
                    }
                    None => false,
                }
            }
        }
    }

    /// Resolve a pending permission prompt of a SUBAGENT session (same
    /// semantics as [`Self::respond_interactive_request`]: the subagent's ACP id,
    /// `false` when the entry is gone).
    pub async fn respond_permission(
        &self,
        session_id: &str,
        request_id: &str,
        outcome: PermissionOutcome,
    ) -> bool {
        let key = permission::permission_key(session_id, request_id);
        let sender = self.driver.pending_permissions.lock().await.remove(&key);
        match sender {
            Some(sender) => {
                let _ = sender.send(outcome);
                true
            }
            None => false,
        }
    }
}

/// The cancel handle for a subagent dispatch (the driver task holds one so
/// a parent close / the agent's EOF cancels the in-flight dispatch).
///
/// Keeps the [`ExternalClose`]'s `tx` + `kind`; `cancel()` sets the kind
/// `User` (first-set-wins, mirroring `LiveSession`'s close plumbing) then
/// flips the flag (the driver task's select arm — establish + block phases
/// — tears the session down). `cancel()` is idempotent.
#[derive(Clone)]
pub struct SubagentCancel {
    /// The close flag sender (flipped by `cancel`; the driver task selects
    /// on its receiver; the subagent's driver task observes it so a
    /// cancel cancels in-flight `ask` waiters via their `close_rx` arm).
    tx: watch::Sender<bool>,
    /// The close kind (first-set-wins): set `User` before the flag flips;
    /// the driver task reads it for the close reason.
    kind: Arc<StdMutex<Option<CloseKind>>>,
}

impl SubagentCancel {
    /// Build the [`ExternalClose`] (the driver task's cancel path — it
    /// keeps the `rx`) + the [`SubagentCancel`] handle (the caller's
    /// cancel path — it keeps the `tx` + `kind`) from ONE channel + kind
    /// (one kind, first-set-wins across the whole session).
    pub(crate) fn new_external_close() -> (ExternalClose, SubagentCancel) {
        let (tx, rx) = watch::channel(false);
        let kind = Arc::new(StdMutex::new(None));
        let ec = ExternalClose { rx };
        (ec, SubagentCancel { tx, kind })
    }

    /// Cancel the dispatch: kind `User` (first-set-wins — a kind already
    /// present means the reason is already decided) + flip the flag.
    /// Idempotent (a second `cancel` is a no-op).
    pub fn cancel(&self) {
        if let Ok(mut kind) = self.kind.lock() {
            if kind.is_none() {
                *kind = Some(CloseKind::User);
            }
        }
        // Ignore `SendError`: the receiver (the driver task) may already be
        // gone (the session already closed).
        let _ = self.tx.send(true);
    }
}

/// The `metrics` object shape (the wire contract: `inputTokens` /
/// `outputTokens` / `cost` / `durationMs`).
fn metrics_json(m: &SubagentMetrics) -> Value {
    json!({
        "output": m.output,
        "inputTokens": m.input_tokens,
        "outputTokens": m.output_tokens,
        "cost": m.cost,
        "durationMs": m.duration_ms,
    })
}

/// The metrics captured for a subagent session (the accumulated
/// `cost_update` usage + the wall-clock duration). The suite self-emits a
/// per-turn `cost_update` (source `main` — `subagent-metrics-cost-push`
/// Task 1); the token/cost fields are real when the session pushed usage (0
/// when it didn't — the `CostAccumulator` default) and `duration_ms` is
/// always the wall clock (see the wire contract's metrics note).
#[derive(Debug, Clone, Default)]
pub struct SubagentMetrics {
    /// The last message's accumulated text ("" when the turn produced no
    /// assistant text — the stale-carry-over test's EMPTY sentinel).
    pub output: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost: f64,
    pub duration_ms: u64,
}

/// The outcome of a subagent dispatch.
#[derive(Debug)]
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

/// The `dispatch_native` driver's race outcome (the child's settle vs the
/// `settle_timeout` vs the `SubagentCancel` handle; a preflight failure is
/// not a race — the task `send` errored before the turn started).
#[derive(Debug)]
enum Race {
    /// The child settled (a `settle_rx` `changed()` `Ok` — the turn ended
    /// via `agent_settled`).
    Settled,
    /// The `settle_timeout` won (a hung / slow turn — the child is torn
    /// down unconditionally).
    TimedOut,
    /// The `SubagentCancel` handle fired (a user / caller cancel).
    Cancelled,
    /// The child loop task DIED (a `settle_rx` `changed()` `Err` — the
    /// `settle_tx` was dropped — `Failed`, NOT `Completed`). The child
    /// is an IN-PROCESS task (not a process) — the error string says
    /// "child loop task died" accordingly.
    Died,
    /// The preflight failed (the task `send` errored — the child died
    /// before the turn started).
    Preflight(String),
}

/// A RAII guard for a throwaway child `Db`'s temp file: the `Drop` removes
/// the file on ANY exit (happy OR panic/early-return) and releases the
/// `Arc<Db>` FIRST (the SQLite connection closes before the file is
/// removed — safe on Windows, where deleting an open file fails). `None`
/// `path` = the `:memory:` case (a no-op for the file; the `db` is still
/// released so the connection closes). Held for the WHOLE driver task so
/// the temp file is cleaned up even on a non-happy exit (a `.expect()`
/// panic would otherwise leak it — the cleanup used to be imperative-only,
/// reached only at the happy teardown step).
struct TempFileGuard {
    /// The throwaway `Db` (released on `Drop` — BEFORE the file is removed,
    /// so the remove is safe on Windows).
    db: Option<Arc<Db>>,
    /// The temp file to remove (`None` = the `:memory:` case — a no-op).
    path: Option<PathBuf>,
}

impl TempFileGuard {
    /// Build a guard from the throwaway `Db` + its temp-file path
    /// (`None` = the `:memory:` case — no file, `Drop` is a no-op for the
    /// file; the `db` is still released so the connection closes).
    fn new(db: Arc<Db>, path: Option<PathBuf>) -> Self {
        Self { db: Some(db), path }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        // Release the `Db` FIRST (the field order: `db` before `path`):
        // the SQLite connection closes BEFORE the file is removed (safe on
        // Windows, where removing an open file fails).
        self.db = None;
        if let Some(p) = self.path.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

/// Open the throwaway child `Db` (`:memory:` first, a temp file on
/// failure) + its [`TempFileGuard`] (held for the WHOLE driver task so
/// the temp file is removed on ANY exit). `force_temp_file` (test-only)
/// skips the `:memory:` attempt (forcing the temp-file fallback — the
/// `:memory:` path is not failable in practice). A `Db` open failure (both
/// `:memory:` and the temp file) is a `String` error (NOT a panic — a panic
/// would drop the oneshot, surfacing as a bare "cancelled" with zero
/// diagnostics) + removes the partial temp file best-effort.
fn open_throwaway_db(force_temp_file: bool) -> Result<(Arc<Db>, TempFileGuard), String> {
    let (db, path) = if force_temp_file {
        let (db, tmp) = open_temp_db()?;
        (db, Some(tmp))
    } else {
        match Db::open(Path::new(":memory:")) {
            Ok(db) => (db, None),
            Err(e) => {
                eprintln!("harness: throwaway :memory: db failed ({e}); using a temp file");
                let (db, tmp) = open_temp_db()?;
                (db, Some(tmp))
            }
        }
    };
    let arc = Arc::new(db);
    // A clone for the `SessionStore` (the guard OWNS the original — it is
    // the sole local holder of the `Arc<Db>`; the child task's `SessionStore`
    // clone is dropped at the driver teardown's `loop_handle.await`, before
    // the guard drops at scope end).
    let store_db = arc.clone();
    Ok((store_db, TempFileGuard::new(arc, path)))
}

/// Open a throwaway temp-file `Db` (a `Some` path — the guard removes it on
/// `Drop`). A failed open is a `String` error (a partial/empty file is
/// removed best-effort) — NOT a panic.
fn open_temp_db() -> Result<(Db, PathBuf), String> {
    let tmp = std::env::temp_dir().join(format!("native-subagent-{}", uuid::Uuid::new_v4()));
    match Db::open(&tmp) {
        Ok(db) => Ok((db, tmp)),
        Err(e) => {
            // Best effort: the open failed (a partial/empty file may exist)
            // — remove it.
            let _ = std::fs::remove_file(&tmp);
            Err(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use crate::agent::harness::{
        ChatRole, FinishReason, MessageContent, Model, ModelCatalog, ModelRequest, Provider,
        ProviderError, ProviderEvent, SudoDeps,
    };
    use crate::agent::permission::PermissionOutcome;
    use crate::agent::session::{CloseKind, EffectiveCatalog, EventSink};
    use crate::storage::Db;
    use async_trait::async_trait;
    use futures_util::StreamExt;

    use super::{
        CapturingSink, LaunchConfig, NativeDeps, SubagentCancel, SubagentOutcome,
        SubagentSessionManager,
    };

    #[derive(Clone)]
    struct TestSink {
        tx: std::sync::mpsc::Sender<(String, serde_json::Value)>,
    }

    impl EventSink for TestSink {
        fn emit(&self, event: &str, payload: serde_json::Value) {
            let _ = self.tx.send((event.to_string(), payload));
        }
    }

    /// (Lock order) `captured_text` must NOT hold `last_message_id`
    /// while acquiring `text`: `emit` takes `text` FIRST — the reverse
    /// order is an ABBA inversion (a concurrent `captured_text` +
    /// `emit` would deadlock). A chunk is emitted FIRST so that
    /// `last_message_id` is `Some` (an `Option::and_then` on `None`
    /// short-circuits and never takes the `text` lock).
    #[test]
    fn captured_text_does_not_hold_last_message_id_while_locking_text() {
        let (tx, _rx) = std::sync::mpsc::channel();
        let sink = Arc::new(CapturingSink::new(Arc::new(TestSink { tx })));
        // Seed a capture (`last_message_id` → `Some`).
        sink.emit(
            "session-update",
            serde_json::json!({
                "sessionId": "s1",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "m1",
                    "content": { "text": "hello" }
                }
            }),
        );
        // Hold `text` here (the order `emit` uses: `text` first).
        let _text_guard = sink.text.lock().unwrap_or_else(|p| p.into_inner());
        let sink_worker = sink.clone();
        let handle = std::thread::spawn(move || {
            let _ = sink_worker.captured_text();
        });
        std::thread::sleep(Duration::from_millis(500));
        // The spawned thread must not be sitting on `last_message_id`
        // (a hold across the `text` acquisition is the ABBA).
        assert!(
            sink.last_message_id.try_lock().is_ok(),
            "`captured_text` holds `last_message_id` while waiting on `text` (ABBA)"
        );
        drop(_text_guard);
        handle.join().unwrap();
        assert_eq!(sink.captured_text(), "hello");
    }

    fn temp_config_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("subagent-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// (1) `SubagentCancel`: first-set-wins `kind` = `User`; the flag flips
    /// (the driver's `external_close` arm sees it); idempotent (a second
    /// `cancel` is a no-op — `kind` stays `User`).
    #[test]
    fn subagent_cancel_first_set_wins_user_and_flips_flag() {
        let (ec, cancel) = SubagentCancel::new_external_close();
        // Initially: kind is `None`, the flag is `false`.
        assert!(cancel.kind.lock().unwrap().is_none());
        assert!(!*ec.rx.borrow());
        cancel.cancel();
        // After cancel: kind is `User`, the flag is `true`.
        assert_eq!(*cancel.kind.lock().unwrap(), Some(CloseKind::User));
        assert!(*ec.rx.borrow());
        // Idempotent: cancel again, kind stays `User`.
        cancel.cancel();
        assert_eq!(*cancel.kind.lock().unwrap(), Some(CloseKind::User));
    }

    #[tokio::test]
    async fn respond_interactive_request_resolves_and_misses() {
        let manager = SubagentSessionManager::new(None);
        let (tx, rx) = oneshot::channel();
        manager
            .driver()
            .pending_bridge
            .lock()
            .await
            .insert("sess/r1".to_string(), tx);
        // Resolves the entry (returns `true`).
        assert!(
            manager
                .respond_interactive_request("sess", "r1", serde_json::json!({ "x": 1 }))
                .await
        );
        let v = rx.await.unwrap();
        assert_eq!(v, serde_json::json!({ "x": 1 }));
        // A missing key returns `false`.
        assert!(
            !manager
                .respond_interactive_request("sess", "r2", serde_json::json!({}))
                .await
        );
    }

    #[tokio::test]
    async fn respond_permission_resolves_and_misses() {
        let manager = SubagentSessionManager::new(None);
        let (tx, rx) = oneshot::channel();
        manager
            .driver()
            .pending_permissions
            .lock()
            .await
            .insert("sess/r1".to_string(), tx);
        // Resolves the entry (returns `true`).
        assert!(
            manager
                .respond_permission("sess", "r1", PermissionOutcome::Cancelled)
                .await
        );
        let v = rx.await.unwrap();
        assert_eq!(v, PermissionOutcome::Cancelled);
        // A missing key returns `false`.
        assert!(
            !manager
                .respond_permission("sess", "r2", PermissionOutcome::Cancelled)
                .await
        );
    }

    /// (5) `NativeDeps` + `set_native_deps` (`&self`, `OnceLock`): unset
    /// in `new`; the first `set_native_deps` stores the deps (readable via
    /// `native_deps()`); a second `set_native_deps` is a NO-OP (the first
    /// set wins — the `OnceLock` is set-once at wiring time).
    #[test]
    fn set_native_deps_is_set_once_on_the_manager() {
        let manager = SubagentSessionManager::new(None);
        // Unset in `new` (the native wiring is injected at the
        // `SessionManager` wiring time — `set_subagent_manager`).
        assert!(manager.native_deps().is_none());
        // A mock provider factory (the 6-field `NativeDeps` — NO `db`
        // field; the throwaway child `Db` is built fresh in
        // `dispatch_native`).
        let mock_factory: crate::agent::session::ProviderFactory =
            Arc::new(|_m: &crate::agent::harness::Model| {
                Box::new(crate::agent::harness::OpenAiCompatibleProvider {
                    base_url: "https://example.com/v1".to_string(),
                    api_key: "k".to_string(),
                })
            });
        let first = super::NativeDeps {
            provider_factory: mock_factory.clone(),
            catalog: EffectiveCatalog {
                config_dir: PathBuf::new(),
                cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
                base: crate::agent::harness::ModelCatalog::default(),
            },
            todo_store: Arc::new(crate::agent::todo::TodoStore::new()),
            sudo: crate::agent::harness::SudoDeps::default(),
            settle_timeout: Duration::from_secs(30 * 60),
            trust_db: None,
            config_dir: None,
        };
        manager.set_native_deps(first.clone());
        // The first `set_native_deps` stored the deps (readable through
        // the `OnceLock`).
        let got = manager.native_deps().expect("first set should be stored");
        assert_eq!(got.settle_timeout, Duration::from_secs(30 * 60));
        // A second `set_native_deps` is a NO-OP (the first set wins).
        let second = super::NativeDeps {
            settle_timeout: Duration::from_secs(1),
            ..first
        };
        manager.set_native_deps(second);
        assert_eq!(
            manager.native_deps().unwrap().settle_timeout,
            Duration::from_secs(30 * 60)
        );
    }

    // ── The `dispatch_native` throwaway-`Db` tests (the review fix) ──

    /// A `Provider` whose stream emits the canned events, then HANGS (no
    /// `Done` — the child cannot settle, so the driver's `settle_timeout`
    /// and the unconditional teardown must win). Used by the temp-file
    /// teardown test.
    struct HangingProvider {
        events: Vec<ProviderEvent>,
    }

    #[async_trait]
    impl Provider for HangingProvider {
        async fn complete(
            &self,
            _req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            Ok(futures_util::stream::iter(self.events.clone())
                .chain(futures_util::stream::pending::<ProviderEvent>())
                .boxed())
        }
    }

    /// A `Provider` that answers with a short canned stream (the happy
    /// path — the turn settles). Used by the temp-file happy-path test.
    struct CannedProvider {
        events: Vec<ProviderEvent>,
    }

    impl CannedProvider {
        fn new(events: Vec<ProviderEvent>) -> Self {
            Self { events }
        }
    }

    #[async_trait]
    impl Provider for CannedProvider {
        async fn complete(
            &self,
            _req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            Ok(futures_util::stream::iter(self.events.clone()).boxed())
        }
    }

    /// A `Provider` that RECORDS each `ModelRequest` (a `CannedProvider`
    /// that pushes `req.clone()` into a shared `Vec` before returning the
    /// canned stream — the child system-message tests assert on the
    /// recorded `messages`).
    struct RecordingCannedProvider {
        events: Vec<ProviderEvent>,
        requests: Arc<StdMutex<Vec<ModelRequest>>>,
    }

    impl RecordingCannedProvider {
        fn new(events: Vec<ProviderEvent>) -> (Self, Arc<StdMutex<Vec<ModelRequest>>>) {
            let requests = Arc::new(StdMutex::new(Vec::new()));
            (
                Self {
                    events,
                    requests: requests.clone(),
                },
                requests,
            )
        }
    }

    #[async_trait]
    impl Provider for RecordingCannedProvider {
        async fn complete(
            &self,
            req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            self.requests.lock().unwrap().push(req.clone());
            Ok(futures_util::stream::iter(self.events.clone()).boxed())
        }
    }

    /// A `Provider` wrapper (the factory closure returns a CONCRETE type
    /// — `Box<dyn Provider>` itself does not implement `Provider`).
    struct ArcBoxProvider(Arc<dyn Provider>);

    #[async_trait]
    impl Provider for ArcBoxProvider {
        async fn complete(
            &self,
            req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            self.0.complete(req).await
        }
    }

    /// A native test `Model` (a fake OpenAI-compatible model — the mock
    /// provider never hits the network).
    fn native_test_model() -> Model {
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

    /// The default `LaunchConfig` (all `None` — inherit the parent's).
    fn default_native_launch() -> LaunchConfig {
        LaunchConfig {
            system_prompt: None,
            model: None,
            thinking: None,
            frontmatter_thinking: None,
            tools: None,
        }
    }

    /// Build a `SubagentSessionManager` with `NativeDeps` set (a mock
    /// `provider_factory` + a single-model catalog + the given settle
    /// bound).
    fn make_native_manager(
        config_dir: &std::path::Path,
        provider: Arc<dyn Provider>,
        settle_timeout: Duration,
    ) -> Arc<SubagentSessionManager> {
        let manager = Arc::new(SubagentSessionManager::new(None));
        let catalog = ModelCatalog {
            models: vec![native_test_model()],
            ..Default::default()
        };
        let factory: crate::agent::session::ProviderFactory =
            Arc::new(move |_m: &Model| Box::new(ArcBoxProvider(provider.clone())));
        manager.set_native_deps(NativeDeps {
            provider_factory: factory,
            catalog: EffectiveCatalog {
                config_dir: config_dir.to_path_buf(),
                cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
                base: catalog,
            },
            todo_store: Arc::new(crate::agent::todo::TodoStore::new()),
            sudo: SudoDeps::default(),
            settle_timeout,
            trust_db: None,
            config_dir: None,
        });
        manager
    }

    /// A `TestSink` + receiver (the full `dispatch_native` tests need a
    /// real sink — the child's frames flow to it once).
    fn native_rec_sink() -> Arc<dyn EventSink> {
        let (tx, _rx) = std::sync::mpsc::channel();
        Arc::new(TestSink { tx })
    }

    /// Count the `native-subagent-*` temp files (the throwaway `Db`
    /// fallback) — the temp-file cleanup tests assert this returns to
    /// baseline (the `TempFileGuard` removed the file on exit).
    fn count_native_subagent_temp_files() -> usize {
        std::fs::read_dir(std::env::temp_dir())
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|e| {
                        e.file_name()
                            .to_string_lossy()
                            .starts_with("native-subagent-")
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// (A) The `TempFileGuard` (the `dispatch_native` throwaway-`Db`
    /// cleanup): a guard for a REAL temp file removes it on `Drop` (ANY
    /// exit — happy OR panic/early-return). The guard ALSO owns the
    /// `Arc<Db>` (released BEFORE the file is removed — safe on Windows,
    /// where deleting an open file fails).
    #[test]
    fn temp_file_guard_removes_the_file_on_drop() {
        let tmp =
            std::env::temp_dir().join(format!("native-subagent-guard-{}", uuid::Uuid::new_v4()));
        let db = Db::open(&tmp).expect("open a temp-file db");
        assert!(tmp.exists(), "the temp file exists before the guard");
        {
            let _guard = super::TempFileGuard::new(Arc::new(db), Some(tmp.clone()));
        }
        assert!(!tmp.exists(), "the guard removed the temp file on drop");
    }

    /// (B) A `TempFileGuard` with `None` (the `:memory:` case) is a NO-OP
    /// (no file removed, no panic).
    #[test]
    fn temp_file_guard_none_is_a_noop() {
        let db = Db::open(std::path::Path::new(":memory:")).expect("open a :memory: db");
        // `None` path → no file to remove; dropping is a no-op (no panic).
        let _guard = super::TempFileGuard::new(Arc::new(db), None);
    }

    /// (C) A `TempFileGuard` DROPPED EARLY (before the enclosing block
    /// ends) still removes the file (the `Drop` is immediate, not deferred
    /// to scope end — the early-return / panic path).
    #[test]
    fn temp_file_guard_removes_the_file_when_dropped_early() {
        let tmp = std::env::temp_dir().join(format!(
            "native-subagent-guard-early-{}",
            uuid::Uuid::new_v4()
        ));
        let db = Db::open(&tmp).expect("open a temp-file db");
        let guard = super::TempFileGuard::new(Arc::new(db), Some(tmp.clone()));
        drop(guard); // dropped early (explicitly)
        assert!(
            !tmp.exists(),
            "the guard removed the temp file when dropped early"
        );
    }

    /// (D) `open_throwaway_db`: `false` (the `:memory:` path) → a
    /// `:memory:` `Db` + a `None` guard (a no-op — no temp file); `true`
    /// (the forced temp-file fallback) → a temp-file `Db` + a `Some`
    /// guard, and the file is CLEANED UP when the guard is dropped.
    #[test]
    fn open_throwaway_db_memory_and_temp_file_fallback() {
        // `false` → `:memory:` (no temp file — the guard is `None`).
        let (db, guard) = super::open_throwaway_db(false).expect("open the throwaway db");
        let _ = db;
        assert!(guard.path.is_none(), "the :memory: path has no temp file");

        // `true` → the temp-file fallback (a `Some` guard), and the file
        // is cleaned up when the guard is dropped.
        let (db, guard) = super::open_throwaway_db(true).expect("open the temp-file db");
        let tmp = guard.path.clone().expect("the temp-file path is recorded");
        let _ = db;
        assert!(tmp.exists(), "the temp file exists before the guard drops");
        drop(guard);
        assert!(!tmp.exists(), "the guard removed the temp file on drop");
    }

    /// (E) **temp-file fallback + cleanup (end-to-end, happy path)**: a
    /// forced temp-file `dispatch_native` (the `:memory:` path skipped)
    /// cleans up the temp file after the dispatch completes (the
    /// `TempFileGuard` is held for the WHOLE driver task — dropped on the
    /// happy exit). The `:memory:` happy-path behavior is unchanged (the
    /// guard is a no-op for it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_native_temp_file_fallback_cleans_up_after_dispatch() {
        let config_dir = temp_config_dir();
        let provider: Arc<dyn Provider> = Arc::new(CannedProvider::new(vec![
            ProviderEvent::TextDelta("hello ".to_string()),
            ProviderEvent::TextDelta("world".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]));
        let manager = make_native_manager(&config_dir, provider, Duration::from_secs(20));
        let sink = native_rec_sink();
        let before = count_native_subagent_temp_files();

        let (dispatch_rx, _cancel) = manager.dispatch_native_force_temp_file(
            "parent-1",
            &config_dir,
            &native_test_model(),
            Vec::new(),
            "tester".to_string(),
            default_native_launch(),
            "do the thing".to_string(),
            &sink,
        );

        let outcome = tokio::time::timeout(Duration::from_secs(30), dispatch_rx)
            .await
            .expect("the dispatch must resolve (the teardown cannot hang)")
            .expect("the oneshot must not be dropped");
        assert!(
            matches!(outcome, SubagentOutcome::Completed { .. }),
            "the temp-file dispatch completes — got {outcome:?}"
        );

        // The temp file is CLEANED UP after the dispatch (the
        // `TempFileGuard` is dropped at the driver task's scope end, a
        // moment AFTER the oneshot resolves) — poll until it is gone.
        wait_for_temp_file_cleanup(before).await;
    }

    /// (F) **temp-file fallback + cleanup on the TEARDOWN path**: a forced
    /// temp-file `dispatch_native` whose child HANGS (no `Done` — it cannot
    /// settle) is torn down by the driver's `settle_timeout` (the
    /// `Race::TimedOut` path), and the temp file is STILL cleaned up (the
    /// `TempFileGuard` is dropped on the teardown exit — not just the happy
    /// exit).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_native_temp_file_fallback_cleans_up_on_teardown() {
        let config_dir = temp_config_dir();
        let provider: Arc<dyn Provider> = Arc::new(HangingProvider {
            events: vec![ProviderEvent::TextDelta("partial".to_string())],
        });
        let manager = make_native_manager(&config_dir, provider, Duration::from_secs(3));
        let sink = native_rec_sink();
        let before = count_native_subagent_temp_files();

        let (dispatch_rx, _cancel) = manager.dispatch_native_force_temp_file(
            "parent-1",
            &config_dir,
            &native_test_model(),
            Vec::new(),
            "tester".to_string(),
            default_native_launch(),
            "do the thing".to_string(),
            &sink,
        );

        let outcome = tokio::time::timeout(Duration::from_secs(20), dispatch_rx)
            .await
            .expect("the dispatch must resolve (the timeout teardown cannot hang)")
            .expect("the oneshot must not be dropped");
        assert!(
            matches!(
                outcome,
                SubagentOutcome::Failed { ref error } if error == "timed out"
            ),
            "a hanging child settles Failed (timed out) — got {outcome:?}"
        );

        // The temp file is STILL cleaned up on the teardown exit (the
        // `TempFileGuard` is dropped at the driver task's scope end).
        wait_for_temp_file_cleanup(before).await;
    }

    /// (ADR 0020) a native child's `enabledTools` (the
    /// `subagent-session-started` payload) NEVER contains `subagent` (the
    /// recursion guard) nor `list_agents` (a child cannot dispatch, so
    /// listing dispatch targets is pointless token burn) — even when the
    /// PARENT's enabled tools carry `list_agents`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_native_child_excludes_subagent_and_list_agents_tools() {
        let config_dir = temp_config_dir();
        let provider: Arc<dyn Provider> = Arc::new(CannedProvider::new(vec![
            ProviderEvent::TextDelta("hello".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]));
        let manager = make_native_manager(&config_dir, provider, Duration::from_secs(20));
        // A LIVE receiver (NOT `native_rec_sink` — it drops the
        // receiver) + the existing `TestSink` (its `emit` records EVERY
        // `(event, payload)` pair, so the `subagent-session-started`
        // frame is captured).
        let (tx, rx) = std::sync::mpsc::channel();
        let sink: Arc<dyn EventSink> = Arc::new(TestSink { tx });

        let (dispatch_rx, _cancel) = manager.dispatch_native(
            "parent-1",
            &config_dir,
            &native_test_model(),
            vec!["read".to_string(), "list_agents".to_string()],
            "tester".to_string(),
            default_native_launch(),
            "do the thing".to_string(),
            &sink,
        );
        // AWAIT the outcome BEFORE asserting: `dispatch_native` returns
        // immediately and the `subagent-session-started` emit happens
        // inside the `tokio::spawn`ed driver task (the emit
        // happens-before the outcome resolves, so the frame is guaranteed
        // present after the `await` — a `try_iter` called immediately
        // could observe an empty channel).
        let _ = tokio::time::timeout(Duration::from_secs(10), dispatch_rx)
            .await
            .expect("the dispatch must resolve within 10 s");
        let started = rx
            .try_iter()
            .into_iter()
            .find(|(event, _)| event == "subagent-session-started")
            .expect("the `subagent-session-started` frame was emitted");
        let (_, payload) = &started;
        assert_eq!(
            payload["enabledTools"],
            serde_json::json!(["read"]),
            "the child's `enabledTools` never contains `subagent` or `list_agents`"
        );
    }

    /// Dispatch a native child (a `RecordingCannedProvider` + the
    /// default 20s settle bound) and return the child's recorded
    /// `ModelRequest`s (the child's system message is seeded in step 7
    /// — BEFORE the task prompt — so the child's first model request
    /// shows it at `messages[0]`).
    async fn dispatch_and_record(
        config_dir: &std::path::Path,
        launch: LaunchConfig,
        parent_enabled_tools: Vec<String>,
    ) -> Vec<ModelRequest> {
        let (provider, requests) = RecordingCannedProvider::new(vec![
            ProviderEvent::TextDelta("hello".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]);
        let provider: Arc<dyn Provider> = Arc::new(provider);
        let manager = make_native_manager(config_dir, provider, Duration::from_secs(20));
        let sink = native_rec_sink();
        let (dispatch_rx, _cancel) = manager.dispatch_native_force_temp_file(
            "parent-1",
            config_dir,
            &native_test_model(),
            parent_enabled_tools,
            "tester".to_string(),
            launch,
            "do the thing".to_string(),
            &sink,
        );
        let outcome = tokio::time::timeout(Duration::from_secs(30), dispatch_rx)
            .await
            .expect("the dispatch must resolve (the teardown cannot hang)")
            .expect("the oneshot must not be dropped");
        assert!(
            matches!(outcome, SubagentOutcome::Completed { .. }),
            "the dispatch completes — got {outcome:?}"
        );
        let recorded = requests.lock().unwrap().clone();
        recorded
    }

    /// (H) **the child's system message — `launch.systemPrompt` + the
    /// todo line (ADR 0017)**: a `Some` prompt + the full parent tool
    /// set (the child's tools = the parent's minus `subagent` → the
    /// child HAS `manage_todo_list`) → the child's first model request
    /// leads with a `System` message of EXACTLY the prompt + the todo
    /// guidance line (the task `User` message follows).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_child_system_prompt_plus_todo_line() {
        let config_dir = temp_config_dir();
        let launch = LaunchConfig {
            system_prompt: Some("You are a careful reviewer.".to_string()),
            ..default_native_launch()
        };
        // `Vec::new()` = ALL parent tools → the child HAS
        // `manage_todo_list`.
        let requests = dispatch_and_record(&config_dir, launch, Vec::new()).await;
        assert_eq!(
            requests.len(),
            1,
            "the canned turn settles in one model call"
        );
        let system = &requests[0].messages[0];
        assert_eq!(system.role, ChatRole::System, "the system message leads");
        assert_eq!(
            system.content,
            MessageContent::Text(
                "You are a careful reviewer.\nUse manage_todo_list to track multi-step work — write the plan before starting, mark items completed as you go"
                    .to_string()
            ),
            "the system message is EXACTLY the prompt + the todo line"
        );
        // The task `User` message follows (the system message LEADS the
        // task prompt — the step-7 seed is BEFORE the step-10 task).
        let task = &requests[0].messages[1];
        assert_eq!(task.role, ChatRole::User, "the task prompt follows");
        assert_eq!(
            task.content,
            MessageContent::Text("do the thing".to_string()),
            "the task prompt is verbatim"
        );
    }

    /// (I) **the child's system message — `launch.systemPrompt` ALONE
    /// (no todo tool)**: a `Some` prompt + a parent tool set WITHOUT
    /// `manage_todo_list` (the child's tools = those minus `subagent`
    /// → no todo tool) → the child's `System` message is the prompt
    /// VERBATIM (no todo line).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_child_system_prompt_alone_without_todo_tool() {
        let config_dir = temp_config_dir();
        let launch = LaunchConfig {
            system_prompt: Some("You are a careful reviewer.".to_string()),
            ..default_native_launch()
        };
        // The child's tools = the parent's minus `subagent` — NO
        // `manage_todo_list`.
        let requests =
            dispatch_and_record(&config_dir, launch, vec!["read".into(), "bash".into()]).await;
        assert_eq!(
            requests.len(),
            1,
            "the canned turn settles in one model call"
        );
        let system = &requests[0].messages[0];
        assert_eq!(system.role, ChatRole::System, "the system message leads");
        assert_eq!(
            system.content,
            MessageContent::Text("You are a careful reviewer.".to_string()),
            "the system message is the prompt VERBATIM (no todo line)"
        );
    }

    /// (J) **the child's system message — the todo line ALONE (the
    /// DEFAULT dispatch)**: `launch.system_prompt = None` + the full
    /// parent tool set (the child HAS `manage_todo_list`) → the
    /// child's `System` message is EXACTLY the todo guidance line (the
    /// `has_todo_tool` derivation from `child_tools` in the `None`
    /// case — the common default).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_child_todo_line_only_when_no_system_prompt() {
        let config_dir = temp_config_dir();
        // `Vec::new()` = ALL parent tools → the child HAS
        // `manage_todo_list`.
        let requests = dispatch_and_record(&config_dir, default_native_launch(), Vec::new()).await;
        assert_eq!(
            requests.len(),
            1,
            "the canned turn settles in one model call"
        );
        let system = &requests[0].messages[0];
        assert_eq!(system.role, ChatRole::System, "the system message leads");
        assert_eq!(
            system.content,
            MessageContent::Text(
                "Use manage_todo_list to track multi-step work — write the plan before starting, mark items completed as you go"
                    .to_string()
            ),
            "the system message is EXACTLY the todo line"
        );
    }

    /// (K) **the child's system message — NONE (the `None` + no-todo-
    /// tool case, today's behavior preserved)**: `launch.system_prompt
    /// = None` + a parent tool set WITHOUT `manage_todo_list` → the
    /// child's first model request leads with the TASK `User` message
    /// (NO system message).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_child_no_system_message_when_neither() {
        let config_dir = temp_config_dir();
        let requests = dispatch_and_record(
            &config_dir,
            default_native_launch(),
            vec!["read".into(), "bash".into()],
        )
        .await;
        assert_eq!(
            requests.len(),
            1,
            "the canned turn settles in one model call"
        );
        let first = &requests[0].messages[0];
        assert_eq!(
            first.role,
            ChatRole::User,
            "no system message — the task leads"
        );
        assert_eq!(
            first.content,
            MessageContent::Text("do the thing".to_string()),
            "the task prompt leads"
        );
    }

    /// (G) **drop order — the `record_session` failure path (the
    /// drop-order finding)**: `dispatch_native_inner` declares `child_db`
    /// BEFORE `_child_db_guard`, and Rust drops locals in REVERSE
    /// declaration order — so on that early `return`, the guard drops
    /// FIRST while the `child_db` `Arc<Db>` clone is still alive. The
    /// guard's `Drop` invariant (release the connection first, then
    /// remove the file — safe on Windows, where removing an open file
    /// fails) holds ONLY when the guard is the LAST holder: the failure
    /// path's `drop(child_db)` (before the `return`) releases the clone,
    /// so the guard's `Drop` closes the connection + removes the file.
    /// This test verifies that ordering (the pre-fix order — guard first,
    /// clone still alive — leaks the temp file on Windows; an
    /// end-to-end `record_session`-failure test is infeasible: the
    /// failure is untriggerable without a read-only dir, which would
    /// break the cleanup assertion itself, and the guard drops in the
    /// spawned driver task — beyond the test's reach).
    #[test]
    fn temp_file_guard_drop_order_releases_the_clone_first() {
        let (db, guard) = super::open_throwaway_db(true).expect("open the throwaway db");
        let tmp = guard.path.clone().expect("the temp-file path is recorded");
        assert!(tmp.exists(), "the temp file exists before the drops");
        // The failure path's ordering: release the `child_db` clone
        // FIRST (the fix — `drop(child_db)` before the `return`)...
        drop(db);
        // ...then the guard (the LAST holder — its `Drop` closes the
        // connection BEFORE the file is removed — safe on Windows).
        drop(guard);
        assert!(
            !tmp.exists(),
            "the file is removed when the clone is released before the guard"
        );
    }

    /// Poll (bounded — the tests must not hang) until the `native-subagent-*`
    /// temp-file count returns to `before` (the `TempFileGuard` removed the
    /// file on the driver task's exit). `before` is the baseline captured
    /// BEFORE the dispatch (a concurrent test's short-lived temp file is
    /// tolerated — the poll waits for it to clear too).
    async fn wait_for_temp_file_cleanup(before: usize) {
        let deadline = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(deadline);
        loop {
            if count_native_subagent_temp_files() <= before {
                return;
            }
            tokio::select! {
                _ = &mut deadline => {
                    panic!(
                        "the temp file was NOT cleaned up after the dispatch ({} files; baseline {})",
                        count_native_subagent_temp_files(),
                        before
                    );
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    }
}

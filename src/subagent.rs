//! The subagent session manager: runs delegated subagent sessions on the
//! dedicated worker runtime (ADR 0004), through the SAME shared
//! [`SessionDriver`] machinery as [`SessionManager`].
//!
//! A subagent session is **ephemeral** (not persisted — `db: None`), runs on
//! the worker runtime (so two concurrent ACP sessions never share one
//! reactor, ADR 0004), and spawns the SAME registry entry as its parent
//! (the built-in `pi` entry in production). Subagents cannot dispatch
//! subagents (the tool is excluded from their spawn — `subagent: None`).
//!
//! The whole lifecycle (spawn → establish → prompt → close) runs on the
//! worker runtime via [`WorkerRuntime::spawn_task`] (channel-based handoff
//! only — never `block_on` across runtimes).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};
use tokio::sync::{oneshot, watch};

use crate::agent::bridge;
use crate::agent::permission::{self, PermissionOutcome};
use crate::agent::rpc::{PiRpc, PiRpcHandle};
use crate::agent::session::{
    bridge_spawn_setup, CloseKind, CostAccumulator, EventSink, ExternalClose, SessionDriver,
    SessionInfo,
};
use crate::agent::worker_runtime::WorkerRuntime;
use crate::config::{ConfigError, Registry};
use crate::storage::Db;

/// Manages all live SUBAGENT sessions (on the worker runtime).
///
/// Owns a shared [`SessionDriver`] (db: `None`, subagent: `None`) whose
/// `sessions` / `pending_*` maps EVERY per-dispatch driver shares (the
/// manager's `respond_*` and the driver-task cleanup operate on the shared
/// maps), plus a [`WorkerRuntime`]. Each dispatch builds a FRESH driver on
/// top of the shared maps (fresh `text_capture` / `last_message_id` /
/// `cost_capture` — a concurrent dispatch must not clobber another's final
/// output, and a no-text dispatch must not return a PREVIOUS dispatch's
/// text). The one-live policy does NOT apply to subagents (ADR 0002 —
/// subagent sessions are excluded by definition).
pub struct SubagentSessionManager {
    /// The shared driver (db: `None`, subagent: `None`), behind an `Arc`:
    /// its `sessions` / `pending_*` maps are shared by every per-dispatch
    /// driver (see `dispatch`), and `respond_*` reads them here.
    driver: Arc<SessionDriver>,
    /// The dedicated worker runtime (ADR 0004) all subagent sessions run on.
    worker: WorkerRuntime,
    registry: Registry,
    config_dir: PathBuf,
    /// The installed gate extension's path (`None` when the install
    /// failed — the dispatch runs ungated rather than broken).
    gate_path: Option<PathBuf>,
    /// The installed tools-override extension's path (`None` when the
    /// install failed — the dispatch runs on the suite's original tools
    /// rather than broken).
    tools_path: Option<PathBuf>,
}

/// Per-dispatch pi configuration for a subagent session (moved verbatim
/// from `launch_wrapper.rs` — the wrapper script is deleted in this task;
/// the flags are now passed directly to the `pi` spawn).
pub struct LaunchConfig {
    /// The agent file body (named agents); `None` for config-less dispatch.
    pub system_prompt: Option<String>,
    /// Resolved model ("provider/id" or "provider/id:<thinking>"); `None` = pi default.
    pub model: Option<String>,
    /// Explicit thinking level from the agent file; `None` = pi's own resolution.
    pub thinking: Option<String>,
    /// Tool allowlist (named agents with `tools`); `None` → `--exclude-tools subagent`.
    pub tools: Option<Vec<String>>,
}

/// The `pi` CLI args for a subagent dispatch (the flags the wrapper script
/// used to `exec`, now passed directly). Each of `--system-prompt` /
/// `--model` / `--thinking` is emitted only when its field is `Some`;
/// `--tools <csv>` is emitted when `tools` is `Some`, and
/// `--exclude-tools subagent` when it is `None`; `--no-session` is always
/// appended.
pub fn subagent_pi_args(cfg: &LaunchConfig) -> Vec<String> {
    let mut args = vec![
        "--mode".to_string(),
        "rpc".to_string(),
        "--no-themes".to_string(),
    ];
    if let Some(sp) = &cfg.system_prompt {
        args.push("--system-prompt".to_string());
        args.push(sp.clone());
    }
    if let Some(model) = &cfg.model {
        args.push("--model".to_string());
        args.push(model.clone());
    }
    if let Some(thinking) = &cfg.thinking {
        args.push("--thinking".to_string());
        args.push(thinking.clone());
    }
    match &cfg.tools {
        Some(tools) => {
            args.push("--tools".to_string());
            args.push(tools.join(","));
        }
        None => {
            args.push("--exclude-tools".to_string());
            args.push("subagent".to_string());
        }
    }
    args.push("--no-session".to_string());
    args
}

impl SubagentSessionManager {
    /// Create a manager (a shared driver with the capture hooks enabled per
    /// dispatch, a dedicated worker runtime, the agent registry from
    /// `config_dir`).
    ///
    /// The `WorkerRuntime` is built here (two idle threads, negligible);
    /// a spawn / build failure (EAGAIN under load) is an `io::Error` —
    /// mapped to the existing `ConfigError::Io` case, NOT a panic (the app
    /// degrades instead of crashing at startup). Dropping the manager drops
    /// the runtime (the shutdown `Sender` is dropped, unblocking the
    /// dedicated thread — the app-exit path).
    pub fn new(config_dir: PathBuf) -> Result<Self, ConfigError> {
        let registry = Registry::load(&config_dir)?;
        // The shared driver (db: `None` — ephemeral; `subagent: None` —
        // subagents cannot dispatch subagents): its `sessions` /
        // `pending_*` maps are shared by EVERY per-dispatch driver (the
        // manager's `respond_*` and the driver-task cleanup operate on
        // these maps). The captures are per-dispatch (a fresh `Some`
        // instance in `dispatch` — a concurrent dispatch must not clobber
        // another's final output).
        let driver = SessionDriver::new();
        let worker = WorkerRuntime::new()?;
        // Install the bundled gate extension (idempotent — the main
        // manager installs the same file; the write is skipped when it
        // matches). A failure is NON-fatal: dispatches run ungated.
        let gate_path = match crate::agent::gate::install_gate_extension(&config_dir) {
            Ok(path) => Some(path),
            Err(e) => {
                eprintln!("gate extension install failed: {e} (dispatches run ungated)");
                None
            }
        };
        // Install the desktop-provided tools override (idempotent — the main
        // manager installs the same file; the write is skipped when it
        // matches). A failure is NON-fatal: dispatches run on the suite's
        // original tools.
        let tools_path = match crate::agent::tools::install_tools_extension(&config_dir) {
            Ok(path) => Some(path),
            Err(e) => {
                eprintln!(
                    "tools extension install failed: {e} (dispatches run on the suite's tools)"
                );
                None
            }
        };
        Ok(Self {
            driver: Arc::new(driver),
            worker,
            registry,
            config_dir,
            gate_path,
            tools_path,
        })
    }

    /// The shared driver (exposed for tests — `respond_*` and the
    /// per-dispatch drivers read its shared `sessions` / `pending_*` maps).
    pub fn driver(&self) -> &SessionDriver {
        &self.driver
    }

    /// The agent registry (consumed by the dispatch lifecycle to look up
    /// the parent's registry entry).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The configured config directory.
    pub fn config_dir(&self) -> &PathBuf {
        &self.config_dir
    }

    /// The dedicated worker runtime (subagent sessions run on it, ADR 0004).
    pub fn worker(&self) -> &WorkerRuntime {
        &self.worker
    }

    /// The subagent's persistence database (always `None` — subagents are
    /// ephemeral). Exposed for symmetry with `SessionManager`.
    pub fn db(&self) -> Option<Arc<Db>> {
        self.driver.db.clone()
    }

    /// Spawn + establish + prompt one subagent session. `parent_session_id`
    /// is the parent's ACP id (from the listener's session-id state — after
    /// the parent's `set_session_id`; a `dispatch_subagent` frame arrives
    /// mid-turn, so it is always the ACP id); `parent_cwd` is the parent's
    /// Space folder (the subagent's cwd + fs sandbox root); `parent_agent_id`
    /// is the parent's registry agent id (the subagent spawns the SAME
    /// registry entry as the parent — the built-in `pi` entry in production);
    /// `agent_name` is the dispatch's agent name (the `subagent-session-
    /// started` payload); `launch` is the per-dispatch pi configuration
    /// (ADR 0005); `task` is the prompt body (the first `session/prompt`).
    ///
    /// The whole lifecycle (spawn → establish → prompt → close) runs on the
    /// worker runtime via [`WorkerRuntime::spawn_task`] (channel-based
    /// handoff only — never `block_on` across runtimes). Returns the result
    /// oneshot + a cancel handle (flips the external close — the driver task
    /// tears the session down; the agent's process group dies on Unix).
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch(
        &self,
        parent_session_id: &str,
        parent_cwd: &Path,
        parent_agent_id: &str,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        sink: &Arc<dyn EventSink>,
    ) -> (oneshot::Receiver<SubagentOutcome>, SubagentCancel) {
        // A per-dispatch driver (the review fix for the shared-capture bug):
        // the `sessions` / `pending_*` maps are the SAME `Arc`s as the
        // manager's driver (the manager's `respond_*` and the driver-task
        // cleanup operate on the shared maps — a per-dispatch entry is
        // resolvable from the manager, and the driver-task cleanup removes
        // the session from the shared map), but the captures are FRESH:
        // two CONCURRENT dispatches must not clobber each other's
        // `last_message_id` / `text_capture` / `cost_capture` (a
        // `messageId` collision — per-process ids like `m1` — would garble
        // the shared entries), and a no-text dispatch must return the empty
        // string, NOT a PREVIOUS dispatch's final text (stale carry-over).
        // `drive_session` takes `&SessionDriver`, so the owned per-dispatch
        // driver moves into the worker task (it borrows NOTHING from
        // `self` — `spawn_task` requires `Future + Send + 'static`).
        let base = &self.driver;
        let driver = SessionDriver {
            sessions: base.sessions.clone(),
            pending_permissions: base.pending_permissions.clone(),
            pending_bridge: base.pending_bridge.clone(),
            // Phase 2 (Task 1): the shared `todo_update` / `sudo_exec`
            // handler state — cloned the same way `pending_bridge` is
            // (the subagent children get the bridge env via
            // `bridge_spawn_setup` and can resolve their prompts through
            // the shared maps).
            todo_store: base.todo_store.clone(),
            pending_sudo: base.pending_sudo.clone(),
            sudo_password: base.sudo_password.clone(),
            runner: base.runner.clone(),
            establish_timeout: base.establish_timeout,
            db: base.db.clone(),
            text_capture: Some(Arc::new(StdMutex::new(std::collections::HashMap::new()))),
            last_message_id: Some(Arc::new(StdMutex::new(None))),
            cost_capture: Some(Arc::new(StdMutex::new(CostAccumulator::default()))),
            subagent: base.subagent.clone(),
            generation_counter: base.generation_counter.clone(),
        };
        // Cheap owned clones so the worker task borrows NOTHING from `self`
        // (`spawn_task` requires `Future + Send + 'static`): the (cheap)
        // `Registry`, and owned `String` / `PathBuf` copies of the
        // arguments. `worker` is NOT cloned — `spawn_task` is a `&self`
        // method call; the closure only captures owned values.
        let registry = self.registry.clone();
        let sink = sink.clone();
        let parent_session_id = parent_session_id.to_string();
        let parent_cwd = parent_cwd.to_path_buf();
        let parent_agent_id = parent_agent_id.to_string();
        // The external close (the driver task's cancel path) + the cancel
        // handle (the caller's cancel path) from ONE channel + kind (one
        // kind, first-set-wins across the whole session).
        let (ec, cancel) = SubagentCancel::new_external_close();
        let task_cancel = cancel.clone();
        // A probe receiver (cloned BEFORE `ec` moves into `drive_session`):
        // after a failed prompt, a flipped flag means the prompt failed
        // because the session was closed (a cancel won the race) — the
        // error is reported as "cancelled", not the prompt's error.
        let close_probe = ec.rx.clone();

        // The gate path (an OWNED clone — the task closure is `'static`
        // and cannot borrow `self`).
        let gate_path = self.gate_path.clone();
        // The tools path (an OWNED clone — same `'static` constraint; a
        // verbatim `self.tools_path` inside the closure is a compile
        // error).
        let tools_path = self.tools_path.clone();
        let handle = self.worker.spawn_task(async move {
            let start = std::time::Instant::now();

            // 1. The bridge-listener placeholder (exactly like
            // `start_session`'s `client_session_id`): the ACP
            // `session_id` is agent-generated and is the identity for
            // everything else (see step 4).
            let client_session_id = uuid::Uuid::new_v4().to_string();

            // 2. Bridge setup (4 env vars + per-spawn socket, the parent's
            // registry entry). `None` when the agent is not a bridge
            // agent / the bridge is unavailable (the suite's fork path
            // covers that — no bridge env).
            let entry = match registry.get(&parent_agent_id) {
                Some(e) => e,
                None => {
                    return SubagentOutcome::Failed {
                        error: format!("unknown agent: {parent_agent_id}"),
                    }
                }
            };
            let (mut agent_env, bridge_setup) = match bridge_spawn_setup(entry, &client_session_id)
            {
                Some((env, sid, socket_path)) => (env, Some((sid, socket_path))),
                None => (entry.env.clone(), None),
            };

            // 2a. The gate injection (Task 4): `-e <gate.ts>` +
            // `PI_ARCHIMEDES_GATE=1` (the extension is inert without the
            // env var) — appended to the `pi` CLI args (the per-dispatch
            // config flags the wrapper script used to `exec` are now
            // passed DIRECTLY — the wrapper is deleted, Task 5).
            // The tools override (Phase 2): a SECOND `-e <tools.ts>` (the
            // extension is inert without the bridge env the setup above
            // already set when available).
            let args = subagent_pi_args(&launch);
            let args = crate::agent::gate::gate_spawn_args(gate_path.as_deref(), &args);
            let args = crate::agent::tools::tools_spawn_args(tools_path.as_deref(), &args);
            if gate_path.is_some() {
                crate::agent::gate::gate_env(&mut agent_env);
            }
            // A dedicated discriminator env (harmless in production; lets
            // a `fake_pi`-based test distinguish subagent children).
            agent_env.insert("ARCHIMEDES_SUBAGENT".to_string(), "1".to_string());

            let rpc = match PiRpc::spawn(&entry.command, &args, &agent_env, &parent_cwd) {
                Ok(r) => r,
                Err(e) => {
                    return SubagentOutcome::Failed {
                        error: e.to_string(),
                    };
                }
            };
            let handle = rpc.handle();
            let hint = format!(
                "could not spawn the subagent agent '{}' (parent session {parent_session_id})",
                entry.command
            );

            // 3. Establish: `get_state` (the pi session's id + capability
            // envelope) — bounded by the establish timeout, external-close
            // aware (a cancel during the window is honored, not deferred).
            // The subagent's `SessionInfo` carries a `Value::Null`
            // capability envelope: subagents are ephemeral (nothing reads
            // their capabilities — no Resume button, no config UI).
            let establish_cwd = parent_cwd.clone();
            // A separate owned copy for the establisher closure (the
            // `move` closure captures it; `parent_agent_id` itself is only
            // BORROWED by the `drive_session` argument below).
            let establish_agent_id = parent_agent_id.clone();
            let info = driver
                .drive_session(
                    handle.clone(),
                    &parent_agent_id,
                    hint,
                    parent_cwd,
                    &sink,
                    bridge_setup,
                    Some(ec),
                    move |h: PiRpcHandle| async move {
                        let state = h.send(json!({ "type": "get_state" })).await?;
                        let session_id = state
                            .get("sessionId")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        Ok(SessionInfo {
                            session_id,
                            agent_id: establish_agent_id,
                            cwd: establish_cwd,
                            capabilities: Value::Null,
                            config_options: None,
                        })
                    },
                )
                .await;

            // On establish failure the session never materialized: NO
            // `subagent-*` events (the main agent's tool result carries the
            // error); the driver teardown already unlinked the socket, the
            // worker task unlinks the wrapper (it owns the path).
            let info = match info {
                Ok(i) => i,
                Err(e) => {
                    return SubagentOutcome::Failed {
                        error: e.to_string(),
                    };
                }
            };

            // 4. The pi `session_id` is known NOW — emit
            // `subagent-session-started`. It carries the pi id (NEVER the
            // placeholder — every downstream artifact the panel
            // cross-references is keyed by the pi id) + the parent's ACP id.
            sink.emit(
                "subagent-session-started",
                json!({
                    "sessionId": info.session_id,
                    "parentSessionId": parent_session_id,
                    "agentName": agent_name,
                    "task": task,
                }),
            );

            // 5. The task as the first `prompt` (the `prompt` response is
            // the preflight — the turn's OUTCOME comes from the driver's
            // `agent_settled` watch, awaited UNBOUNDED below: no timeout;
            // cancellation is the external close, which tears the session
            // down and resolves the wait via the dropped watch sender).
            let sid = info.session_id.clone();
            let prompt = handle
                .send(json!({ "type": "prompt", "content": task }))
                .await;
            let settle = driver.wait_for_settle(&sid).await;

            // Ensure teardown on completion or failure.
            task_cancel.cancel();

            // 6 / 7 / 8. Close + emit + resolve (the `end_turn` path) or
            // fail (cancellation / the agent died mid-turn).
            let cancelled = *close_probe.borrow();
            match (prompt, settle) {
                (Ok(_), Ok(_)) => {
                    // `output` = the accumulated text of the
                    // `last_message_id` (NOT `HashMap` iteration order; no
                    // text → empty string); `metrics` from `cost_capture`
                    // (the accumulated `cost_update` usage, defaulting to 0)
                    // + `duration_ms` (wall clock since step 1).
                    let (output, metrics) = captures(&driver, start.elapsed().as_millis() as u64);
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": sid,
                            "status": "completed",
                            "metrics": metrics_json(&metrics),
                        }),
                    );
                    SubagentOutcome::Completed { output, metrics }
                }
                (Err(e), _) => {
                    // The preflight failed (the turn never started).
                    let error = e.to_string();
                    let (_, metrics) = captures(&driver, start.elapsed().as_millis() as u64);
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": sid,
                            "status": "failed",
                            "error": error.clone(),
                            "metrics": metrics_json(&metrics),
                        }),
                    );
                    SubagentOutcome::Failed { error }
                }
                (Ok(_), Err(e)) => {
                    // The turn did not settle (cancellation — a flipped
                    // flag means the session was closed → "cancelled" — or
                    // the agent died mid-turn).
                    let error = if cancelled {
                        "cancelled".to_string()
                    } else {
                        e.to_string()
                    };
                    let (_, metrics) = captures(&driver, start.elapsed().as_millis() as u64);
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": sid,
                            "status": "failed",
                            "error": error.clone(),
                            "metrics": metrics_json(&metrics),
                        }),
                    );
                    SubagentOutcome::Failed { error }
                }
            }
        });
        (handle, cancel)
    }

    /// Resolve a pending request of a SUBAGENT session (its own bridge
    /// listener's map — the `session_id` is the subagent's ACP id, the
    /// listener's `set_session_id` ran on the ACP id after establishment).
    ///
    /// Direct map access — callable from ANY runtime (the map is a
    /// `tokio::sync::Mutex`, locked briefly). `false` when the entry is gone
    /// (the session closed, or the request already resolved — the silent
    /// no-op).
    pub async fn respond_bridge_request(
        &self,
        session_id: &str,
        request_id: &str,
        result: Value,
    ) -> bool {
        let key = bridge::bridge_key(session_id, request_id);
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
                // `"{sid}/{id}:password"`). The legacy `confirm` /
                // `password` flow still resolves via `pending_bridge` (the
                // lookup above is NOT removed). NOTE the plain-`bool`
                // return (NOT `Result<bool>` — the command shim computes
                // `main_hit || subagent_state.respond_bridge_request(…)`
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
    /// semantics as [`Self::respond_bridge_request`]: the subagent's ACP id,
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

/// The cancel handle for a subagent dispatch (the bridge waiter holds one so
/// a parent close / the agent's EOF cancels the in-flight dispatch).
///
/// Keeps the [`ExternalClose`]'s `tx` + `kind`; `cancel()` sets the kind
/// `User` (first-set-wins, mirroring `LiveSession`'s close plumbing) then
/// flips the flag (the driver task's select arm — establish + block phases
/// — tears the session down). `cancel()` is idempotent.
#[derive(Clone)]
pub struct SubagentCancel {
    /// The close flag sender (flipped by `cancel`; the driver task selects
    /// on its receiver; the subagent's bridge listener observes it so a
    /// cancel cancels in-flight `ask` waiters via their `close_rx` arm).
    tx: watch::Sender<bool>,
    /// The close kind (first-set-wins): set `User` before the flag flips;
    /// the driver task reads it for the close reason.
    kind: Arc<StdMutex<Option<CloseKind>>>,
}

impl SubagentCancel {
    /// Build the [`ExternalClose`] (the driver task's cancel path — it keeps
    /// the `rx` + `kind`) + the [`SubagentCancel`] handle (the caller's
    /// cancel path — it keeps the `tx` + `kind`) from ONE channel + kind
    /// (one kind, first-set-wins across the whole session).
    pub(crate) fn new_external_close() -> (ExternalClose, SubagentCancel) {
        let (tx, rx) = watch::channel(false);
        let kind = Arc::new(StdMutex::new(None));
        let ec = ExternalClose {
            tx: tx.clone(),
            rx,
            kind: kind.clone(),
        };
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

/// Read the final output + metrics from the driver's capture hooks:
/// `output` is the accumulated text of the `last_message_id` (NOT `HashMap`
/// iteration order; no text → empty string); the token/cost fields come from
/// the accumulated `cost_update` usage (defaulting to 0 — the
/// `CostAccumulator` default when the session pushed none); `duration_ms` is
/// the caller's wall clock.
///
/// The capture reads are TOLERANT of a poisoned mutex (`into_inner` — a
/// poisoned capture degrades to its last good state, not a panic): a panic
/// while a brief capture guard is held must not chain into every subsequent
/// `captures()` / hook (and, on the worker task, into a dropped oneshot —
/// a crash silently reported as a "cancelled" dispatch).
fn captures(driver: &SessionDriver, duration_ms: u64) -> (String, SubagentMetrics) {
    let mut metrics = SubagentMetrics {
        duration_ms,
        ..Default::default()
    };
    if let Some(cc) = &driver.cost_capture {
        let c = cc.lock().unwrap_or_else(|p| p.into_inner());
        metrics.input_tokens = c.input_tokens;
        metrics.output_tokens = c.output_tokens;
        metrics.cost = c.cost;
    }
    let output = driver
        .last_message_id
        .as_ref()
        .and_then(|lmi| lmi.lock().unwrap_or_else(|p| p.into_inner()).clone())
        .and_then(|id| {
            driver.text_capture.as_ref().and_then(|tc| {
                tc.lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&id)
                    .cloned()
            })
        })
        .unwrap_or_default();
    metrics.output = output.clone();
    (output, metrics)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use crate::agent::permission::PermissionOutcome;
    use crate::agent::rpc::{PiRpc, PiRpcHandle};
    use crate::agent::session::{CloseKind, EventSink, ExternalClose, SessionDriver, SessionInfo};
    use crate::config::Registry;

    use super::{LaunchConfig, SubagentCancel, SubagentSessionManager};

    /// The `fake_pi` binary path. These tests spawn it DIRECTLY (no copy):
    /// they never reap processes by binary path (the driver kills via the
    /// child handle `PiRpc` owns), so a shared path cannot false-positive —
    /// and a copy races the kernel's ETXTBSY check (the copy's write-fd
    /// can still be in flight when the forked child execs).
    fn fake_pi_bin() -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/target/debug/fake_pi"))
    }

    #[derive(Clone)]
    struct TestSink {
        tx: std::sync::mpsc::Sender<(String, serde_json::Value)>,
    }

    impl EventSink for TestSink {
        fn emit(&self, event: &str, payload: serde_json::Value) {
            let _ = self.tx.send((event.to_string(), payload));
        }
    }

    /// Close a live session in a `SessionDriver` (the `SessionManager`
    /// `close_session` logic, inlined for tests): set the `User` kind
    /// (first-set-wins) + flip the close flag.
    async fn close_session_internal(driver: &Arc<SessionDriver>, session_id: &str) {
        let (close_tx, close_kind) = {
            let sessions = driver.sessions.lock().await;
            match sessions.get(session_id) {
                Some(live) => (live.close_tx.clone(), live.close_kind.clone()),
                None => return,
            }
        };
        if let Ok(mut kind) = close_kind.lock() {
            if kind.is_none() {
                *kind = Some(CloseKind::User);
            }
        }
        let _ = close_tx.send(true);
    }

    /// (1) Named agent with tools: every flag present, in order.
    #[test]
    fn subagent_pi_args_named_with_tools() {
        let args = super::subagent_pi_args(&LaunchConfig {
            system_prompt: Some("You are a careful reviewer.".into()),
            model: Some("anthropic/claude-sonnet-4-5".into()),
            thinking: Some("high".into()),
            tools: Some(vec!["read".into(), "bash".into()]),
        });
        assert_eq!(
            args,
            vec![
                "--mode",
                "rpc",
                "--no-themes",
                "--system-prompt",
                "You are a careful reviewer.",
                "--model",
                "anthropic/claude-sonnet-4-5",
                "--thinking",
                "high",
                "--tools",
                "read,bash",
                "--no-session",
            ]
        );
    }

    /// (2) Named agent without tools: `--exclude-tools subagent`, no `--tools`.
    #[test]
    fn subagent_pi_args_named_without_tools() {
        let args = super::subagent_pi_args(&LaunchConfig {
            system_prompt: Some("body".into()),
            model: None,
            thinking: None,
            tools: None,
        });
        assert_eq!(
            args,
            vec![
                "--mode",
                "rpc",
                "--no-themes",
                "--system-prompt",
                "body",
                "--exclude-tools",
                "subagent",
                "--no-session"
            ]
        );
    }

    /// (3) Config-less dispatch (all `None`): only the base + exclude + no-session.
    #[test]
    fn subagent_pi_args_config_less_is_minimal() {
        let args = super::subagent_pi_args(&LaunchConfig {
            system_prompt: None,
            model: None,
            thinking: None,
            tools: None,
        });
        assert_eq!(
            args,
            vec![
                "--mode",
                "rpc",
                "--no-themes",
                "--exclude-tools",
                "subagent",
                "--no-session"
            ]
        );
    }

    /// (4) Model + thinking only (a common partial config).
    #[test]
    fn subagent_pi_args_model_and_thinking_only() {
        let args = super::subagent_pi_args(&LaunchConfig {
            system_prompt: None,
            model: Some("openai/gpt-5".into()),
            thinking: Some("medium".into()),
            tools: Some(vec!["read".into()]),
        });
        assert_eq!(
            args,
            vec![
                "--mode",
                "rpc",
                "--no-themes",
                "--model",
                "openai/gpt-5",
                "--thinking",
                "medium",
                "--tools",
                "read",
                "--no-session"
            ]
        );
    }

    fn temp_config_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("subagent-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Write an agents.json with ONE `fake` entry pointing at `fake_pi`
    /// with the given env (e.g. `FAKE_PI_TWO_MSGS=1`).
    fn write_agents_json_pi(dir: &std::path::Path, env: &[(&str, &str)]) {
        // `env` is a JSON OBJECT (a `BTreeMap` on the wire) — the slice
        // form would serialize as an array of pairs and fail to
        // deserialize.
        let env_map: serde_json::Map<String, serde_json::Value> = env
            .iter()
            .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
            .collect();
        let json = serde_json::json!({
            "agents": [
                {
                    "id": "fake",
                    "name": "Fake Pi",
                    "command": fake_pi_bin(),
                    "args": [],
                    "env": env_map,
                }
            ]
        });
        std::fs::write(
            dir.join("agents.json"),
            serde_json::to_string_pretty(&json).unwrap(),
        )
        .unwrap();
    }

    /// Spawn a `PiRpc` from a registry entry (the command + args + env).
    fn make_rpc(
        entry: &crate::config::AgentEntry,
        cwd: &std::path::Path,
    ) -> Result<PiRpc, crate::agent::RpcError> {
        PiRpc::spawn(&entry.command, &entry.args, &entry.env, cwd)
    }

    /// (1) `SubagentCancel`: first-set-wins `kind` = `User`; the flag flips
    /// (the driver's `external_close` arm sees it); idempotent (a second
    /// `cancel` is a no-op — `kind` stays `User`).
    #[test]
    fn subagent_cancel_first_set_wins_user_and_flips_flag() {
        let (ec, cancel) = SubagentCancel::new_external_close();
        // Initially: kind is `None`, the flag is `false`.
        assert!(ec.kind.lock().unwrap().is_none());
        assert!(!*ec.rx.borrow());
        cancel.cancel();
        // After cancel: kind is `User`, the flag is `true`.
        assert_eq!(*ec.kind.lock().unwrap(), Some(CloseKind::User));
        assert!(*ec.rx.borrow());
        // Idempotent: cancel again, kind stays `User`.
        cancel.cancel();
        assert_eq!(*ec.kind.lock().unwrap(), Some(CloseKind::User));
    }

    #[tokio::test]
    async fn respond_bridge_request_resolves_and_misses() {
        let manager = SubagentSessionManager::new(temp_config_dir()).unwrap();
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
                .respond_bridge_request("sess", "r1", serde_json::json!({ "x": 1 }))
                .await
        );
        let v = rx.await.unwrap();
        assert_eq!(v, serde_json::json!({ "x": 1 }));
        // A missing key returns `false`.
        assert!(
            !manager
                .respond_bridge_request("sess", "r2", serde_json::json!({}))
                .await
        );
    }

    #[tokio::test]
    async fn respond_permission_resolves_and_misses() {
        let manager = SubagentSessionManager::new(temp_config_dir()).unwrap();
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

    /// (3) The `text_capture` / `last_message_id` pair: drive a `fake_pi`
    /// `FAKE_PI_TWO_MSGS` session through a `SessionDriver` with both
    /// `Some`; assert the per-`messageId` texts (m1 = "hello", m2 =
    /// "world") AND that `last_message_id` is `m2` (the final-output
    /// source, NOT `HashMap` iteration order).
    // The polling loop holds the (brief) `std::sync::Mutex` guards in scope
    // across the `sleep` / teardown awaits on purpose (a plain `StdMutex` is
    // the driver's capture type by design — the guards are dropped before
    // each await; clippy's liveness analysis is scope-based, not `drop`-aware).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn text_capture_and_last_message_id_track_distinct_messages() {
        let config_dir = temp_config_dir();
        write_agents_json_pi(&config_dir, &[("FAKE_PI_TWO_MSGS", "1")]);

        let (tx, _rx) = std::sync::mpsc::channel();
        let sink: Arc<dyn EventSink> = Arc::new(TestSink { tx });

        let registry = Registry::load(&config_dir).unwrap();
        let entry = registry.get("fake").unwrap();
        let cwd = config_dir.clone();

        let mut driver = SessionDriver::new();
        driver.text_capture = Some(Arc::new(StdMutex::new(HashMap::new())));
        driver.last_message_id = Some(Arc::new(StdMutex::new(None)));
        let text_capture = driver.text_capture.clone().unwrap();
        let last_message_id = driver.last_message_id.clone().unwrap();
        let driver = Arc::new(driver);

        // Drive the session (the fake in `FAKE_PI_TWO_MSGS` mode answers
        // `get_state`, then streams m1 ("hello") then m2 ("world") on
        // `prompt`).
        let info = crate::test_support::run_with_retry(|| {
            let driver = driver.clone();
            let sink = sink.clone();
            let entry = entry.clone();
            let cwd = cwd.clone();
            async move {
                let rpc = make_rpc(&entry, &cwd)?;
                let handle: PiRpcHandle = rpc.handle();
                let cwd = cwd.clone();
                driver
                    .drive_session(
                        handle,
                        "fake",
                        String::new(),
                        cwd.clone(),
                        &sink,
                        None,
                        None,
                        move |h: PiRpcHandle| async move {
                            let state = h.send(serde_json::json!({ "type": "get_state" })).await?;
                            let session_id = state
                                .get("sessionId")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_default()
                                .to_string();
                            Ok(SessionInfo {
                                session_id,
                                agent_id: "fake".to_string(),
                                cwd,
                                capabilities: serde_json::Value::Null,
                                config_options: None,
                            })
                        },
                    )
                    .await
            }
        })
        .await
        .expect("drive_session should establish");

        // Send the prompt (the preflight response), then await the turn's
        // settle (the driver's `agent_settled` watch).
        let sid = info.session_id.clone();
        let handle = {
            let sessions = driver.sessions.lock().await;
            sessions.get(&sid).unwrap().handle.clone()
        };
        handle
            .send(serde_json::json!({ "type": "prompt", "content": "hi" }))
            .await
            .expect("prompt should succeed");
        driver
            .wait_for_settle(&sid)
            .await
            .expect("the turn should settle");

        // Poll the captures until both messages are accumulated.
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let acc = text_capture.lock().unwrap();
            let done = acc.get("m1").is_some() && acc.get("m2").is_some();
            drop(acc);
            if done {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "timeout waiting for the captures; got {:?}",
                    &*text_capture.lock().unwrap()
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let acc = text_capture.lock().unwrap();
        assert_eq!(acc.get("m1").map(|s| s.as_str()), Some("hello"));
        assert_eq!(acc.get("m2").map(|s| s.as_str()), Some("world"));
        // The last-seen messageId is m2 (NOT derived from HashMap order).
        assert_eq!(last_message_id.lock().unwrap().as_deref(), Some("m2"));
        drop(acc);

        // Teardown (best effort — the process dies on close).
        close_session_internal(&driver, &info.session_id).await;
        let _ = std::fs::remove_dir_all(&config_dir);
    }

    /// (4) The `external_close` arm: a driver task with `external_close: Some`
    /// tears down when the external flag flips DURING the establish phase (the
    /// fake in `FAKE_PI_HANG` mode never answers `get_state` — WITHOUT the
    /// external close, `drive_session` would wait out the full establish
    /// timeout).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_close_tears_down_during_establish() {
        let config_dir = temp_config_dir();
        write_agents_json_pi(&config_dir, &[("FAKE_PI_HANG", "1")]);

        let (tx, _rx) = std::sync::mpsc::channel();
        let sink: Arc<dyn EventSink> = Arc::new(TestSink { tx });

        let registry = Registry::load(&config_dir).unwrap();
        let entry: crate::config::AgentEntry = registry.get("fake").cloned().unwrap();
        let cwd = config_dir.clone();

        let mut driver = SessionDriver::new();
        driver.establish_timeout = Duration::from_secs(10);
        let driver = Arc::new(driver);

        let (tx, rx) = tokio::sync::watch::channel(false);
        let kind = Arc::new(StdMutex::new(None));
        let external_close = ExternalClose {
            tx: tx.clone(),
            rx,
            kind: kind.clone(),
        };

        let drive = tokio::spawn(async move {
            let rpc = make_rpc(&entry, &cwd).expect("fake_pi should spawn");
            let handle = rpc.handle();
            driver
                .drive_session(
                    handle,
                    "fake",
                    String::new(),
                    cwd,
                    &sink,
                    None,
                    Some(external_close),
                    move |h: PiRpcHandle| async move {
                        let state = h.send(serde_json::json!({ "type": "get_state" })).await?;
                        let session_id = state
                            .get("sessionId")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        Ok(SessionInfo {
                            session_id,
                            agent_id: "fake".to_string(),
                            cwd: config_dir.clone(),
                            capabilities: serde_json::Value::Null,
                            config_options: None,
                        })
                    },
                )
                .await
        });

        // The establish phase is in progress (the hung fake never
        // answers `get_state`): flip the external close.
        tokio::time::sleep(Duration::from_millis(300)).await;
        *kind.lock().unwrap() = Some(CloseKind::User);
        let _ = tx.send(true);

        let result = tokio::time::timeout(Duration::from_secs(8), drive).await;

        let final_res = match result {
            Ok(inner) => inner,
            Err(_) => panic!("Test timed out at 8s"),
        };

        match final_res {
            Ok(Ok(_)) => panic!("Session established unexpectedly"),
            Err(e) => panic!("Task panicked: {:?}", e),
            Ok(Err(e)) => {
                // The external close won the race: the establisher's
                // select arm resolved the teardown (a `spawn` /
                // `io`-class error the `run_with_retry` mapping does
                // NOT retry — the hang-mode fake spawned fine, so the
                // error is the teardown's, not a spawn flake).
                assert!(
                    !e.to_string().contains("did not answer get_state"),
                    "Expected a prompt teardown (User), not the establish timeout; got: {e}"
                );
            }
        }
    }
}

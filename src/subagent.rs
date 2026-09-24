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

use agent_client_protocol::schema::v1::{
    ClientCapabilities, ContentBlock, FileSystemCapabilities, InitializeRequest, NewSessionRequest,
    PromptRequest, TextContent,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use serde_json::{json, Value};
use tokio::sync::{oneshot, watch};

use crate::agent::bridge;
use crate::agent::launch_wrapper::{self, LaunchConfig};
use crate::agent::permission::{self, PermissionOutcome};
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
        Ok(Self {
            driver: Arc::new(driver),
            worker,
            registry,
            config_dir,
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
            establish_timeout: base.establish_timeout,
            db: base.db.clone(),
            text_capture: Some(Arc::new(StdMutex::new(std::collections::HashMap::new()))),
            last_message_id: Some(Arc::new(StdMutex::new(None))),
            cost_capture: Some(Arc::new(StdMutex::new(CostAccumulator::default()))),
            subagent: base.subagent.clone(),
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

        let handle = self.worker.spawn_task(async move {
            let start = std::time::Instant::now();

            // 1. The bridge-listener placeholder (exactly like
            // `start_session`'s `client_session_id`): the ACP
            // `session_id` is agent-generated and is the identity for
            // everything else (see step 4).
            let client_session_id = uuid::Uuid::new_v4().to_string();

            // 2. Bridge setup (4 env vars + per-spawn socket, the parent's
            // registry entry) + the per-dispatch launch wrapper (ADR 0005)
            // in the socket's dir (the `archimedes-bridge-<uid>` dir).
            // `None` when the agent is not a bridge agent / the bridge is
            // unavailable (the suite's fork path covers that — no wrapper,
            // no bridge env).
            let entry = match registry.get(&parent_agent_id) {
                Some(e) => e,
                None => {
                    return SubagentOutcome::Failed {
                        error: format!("unknown agent: {parent_agent_id}"),
                    }
                }
            };
            let (agent_env, bridge_setup, wrapper_path) =
                match bridge_spawn_setup(entry, &client_session_id) {
                    Some((mut env, sid, socket_path)) => {
                        let dir = match socket_path.parent() {
                            Some(d) => d,
                            None => {
                                return SubagentOutcome::Failed {
                                    error: "bridge socket path has no parent dir".to_string(),
                                }
                            }
                        };
                        match launch_wrapper::write_wrapper(dir, &launch, "pi") {
                            Ok(p) => {
                                // The wrapper is the subagent's `pi` command — export it
                                // as `PI_ACP_PI_COMMAND` (the suite's pi honors it; the
                                // fake agent's mode rule reads it). ONLY the subagent
                                // spawn gets the wrapper env (the `None` arm — non-bridge
                                // agents — does not, and neither do main sessions).
                                env.insert(
                                    "PI_ACP_PI_COMMAND".to_string(),
                                    p.to_string_lossy().to_string(),
                                );
                                (env, Some((sid, socket_path)), Some(p))
                            }
                            Err(e) => {
                                return SubagentOutcome::Failed {
                                    error: format!("launch wrapper: {e}"),
                                }
                            }
                        }
                    }
                    None => (entry.env.clone(), None, None),
                };

            let agent = AcpAgent::new(
                AcpAgentConfig::new(entry.command.clone())
                    .args(entry.args.clone())
                    .envs(agent_env),
            );
            let hint = format!(
                "could not spawn the subagent agent '{}' (parent session {parent_session_id})",
                entry.command
            );

            // 3. Establish: `initialize` (same client capabilities as main:
            // fs read/write true, terminal false) + `session/new` (cwd =
            // `parent_cwd`) — bounded by the establish timeout, external-
            // close aware (a cancel during the window is honored, not
            // deferred to the timeout).
            let establish_cwd = parent_cwd.clone();
            // A separate owned copy for the establisher closure (the
            // `move` closure captures it; `parent_agent_id` itself is only
            // BORROWED by the `drive_session` argument below).
            let establish_agent_id = parent_agent_id.clone();
            let info = driver
                .drive_session(
                    agent,
                    &parent_agent_id,
                    hint,
                    parent_cwd,
                    &sink,
                    bridge_setup,
                    Some(ec),
                    move |cx: ConnectionTo<Agent>| {
                        let cwd = establish_cwd.clone();
                        async move {
                            let init = cx
                                .send_request(
                                    InitializeRequest::new(ProtocolVersion::V1)
                                        .client_capabilities(
                                            ClientCapabilities::default()
                                                .fs(FileSystemCapabilities::default()
                                                    .read_text_file(true)
                                                    .write_text_file(true))
                                                .terminal(false),
                                        ),
                                )
                                .block_task()
                                .await?;
                            let new_session = cx
                                .send_request(NewSessionRequest::new(cwd.clone()))
                                .block_task()
                                .await?;
                            Ok((
                                new_session.session_id.clone(),
                                SessionInfo {
                                    session_id: new_session.session_id.clone(),
                                    agent_id: establish_agent_id.clone(),
                                    cwd,
                                    capabilities: init.agent_capabilities,
                                    config_options: None,
                                },
                            ))
                        }
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
                    if let Some(p) = &wrapper_path {
                        let _ = std::fs::remove_file(p);
                    }
                    return SubagentOutcome::Failed {
                        error: e.to_string(),
                    };
                }
            };

            // 4. The ACP `session_id` is known NOW — emit
            // `subagent-session-started`. It carries the ACP id (NEVER the
            // placeholder — every downstream artifact the panel
            // cross-references is keyed by the ACP id) + the parent's ACP id.
            sink.emit(
                "subagent-session-started",
                json!({
                    "sessionId": info.session_id.to_string(),
                    "parentSessionId": parent_session_id,
                    "agentName": agent_name,
                    "task": task,
                }),
            );

            // 5. The task as the first `session/prompt` (UNBOUNDED await —
            // no timeout; cancellation is the external close, which fails
            // the prompt when the driver tears the session down).
            let sid = info.session_id.clone();
            let cx = match driver.sessions.lock().await.get(&sid) {
                Some(live) => live.cx.clone(),
                None => {
                    // Invariant violation (the entry vanished between
                    // `drive_session` returning and this lookup): tear the
                    // session down and fail.
                    let (_, metrics) = captures(&driver, start.elapsed().as_millis() as u64);
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": sid.to_string(),
                            "status": "failed",
                            "error": "subagent session vanished after establishment",
                            "metrics": metrics_json(metrics),
                        }),
                    );
                    if let Some(p) = &wrapper_path {
                        let _ = std::fs::remove_file(p);
                    }
                    return SubagentOutcome::Failed {
                        error: "subagent session vanished after establishment".to_string(),
                    };
                }
            };
            let request = PromptRequest::new(
                sid.clone(),
                vec![ContentBlock::Text(TextContent::new(task))],
            );
            let prompt = cx.send_request(request).block_task().await;

            // Ensure teardown on completion or failure.
            task_cancel.cancel();

            // 6 / 7 / 8. Close + emit + resolve (the `end_turn` path) or
            // fail (cancellation / the agent died mid-turn).
            match prompt {
                Ok(_) => {
                    // `output` = the accumulated text of the
                    // `last_message_id` (NOT `HashMap` iteration order; no
                    // text → empty string); `metrics` from `cost_capture`
                    // (the accumulated `cost_update` usage, defaulting to 0)
                    // + `duration_ms` (wall clock since step 1).
                    let (output, metrics) = captures(&driver, start.elapsed().as_millis() as u64);
                    // The worker task owns the wrapper path — unlink it.
                    if let Some(p) = &wrapper_path {
                        let _ = std::fs::remove_file(p);
                    }
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": sid.to_string(),
                            "status": "completed",
                            "metrics": metrics_json(metrics),
                        }),
                    );
                    SubagentOutcome::Completed { output, metrics }
                }
                Err(e) => {
                    // Cancellation (or the agent died mid-turn). A flipped
                    // flag means the prompt failed because the session was
                    // closed → "cancelled"; otherwise the prompt's error.
                    let error = if *close_probe.borrow() {
                        "cancelled".to_string()
                    } else {
                        e.message.clone()
                    };
                    let (_, metrics) = captures(&driver, start.elapsed().as_millis() as u64);
                    if let Some(p) = &wrapper_path {
                        let _ = std::fs::remove_file(p);
                    }
                    sink.emit(
                        "subagent-closed",
                        json!({
                            "sessionId": sid.to_string(),
                            "status": "failed",
                            "error": error.clone(),
                            "metrics": metrics_json(metrics),
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
            None => false,
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
    (output, metrics)
}

/// The `metrics` object shape (the wire contract: `inputTokens` /
/// `outputTokens` / `cost` / `durationMs`).
fn metrics_json(m: SubagentMetrics) -> Value {
    json!({
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
#[derive(Debug, Clone, Copy, Default)]
pub struct SubagentMetrics {
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

    use agent_client_protocol::schema::v1::{
        ClientCapabilities, ContentBlock, FileSystemCapabilities, InitializeRequest,
        NewSessionRequest, PromptRequest, TextContent,
    };
    use agent_client_protocol::schema::ProtocolVersion;
    use agent_client_protocol::{AcpAgent, AcpAgentConfig, ConnectionTo};
    use tokio::sync::oneshot;

    use crate::agent::permission::PermissionOutcome;
    use crate::agent::session::{CloseKind, EventSink, ExternalClose, SessionDriver, SessionInfo};
    use crate::config::Registry;

    use super::{SubagentCancel, SubagentSessionManager};

    /// The full path to the compiled `fake_agent` binary (unit tests can't use
    /// `CARGO_BIN_EXE_*` — it's only set for integration tests — so construct
    /// the path from `CARGO_MANIFEST_DIR` + `target/debug`).
    const FAKE_AGENT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/target/debug/fake_agent");

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
    async fn close_session_internal(
        driver: &Arc<SessionDriver>,
        session_id: &agent_client_protocol::schema::v1::SessionId,
    ) {
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

    fn temp_config_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("subagent-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Copy the fake agent binary to a unique path (tests run in parallel).
    fn unique_fake_agent(dir: &std::path::Path) -> PathBuf {
        let path = dir.join(format!("fake_agent-{}", uuid::Uuid::new_v4()));
        std::fs::copy(FAKE_AGENT, &path).unwrap();
        path
    }

    /// Write an agents.json with ONE `fake` entry pointing at `cmd` (mode =
    /// the fake agent's positional arg, e.g. `"two-msgs"` / `"hang"`).
    fn write_agents_json_cmd(cmd: &std::path::Path, dir: &std::path::Path, mode: Option<&str>) {
        let json = serde_json::json!({
            "agents": [
                {
                    "id": "fake",
                    "name": "Fake Agent",
                    "command": cmd.to_string_lossy(),
                    "args": mode.map(|m| vec![m.to_string()]).unwrap_or_default(),
                    "env": {}
                }
            ]
        });
        std::fs::write(
            dir.join("agents.json"),
            serde_json::to_string_pretty(&json).unwrap(),
        )
        .unwrap();
    }

    /// Build an `AcpAgent` from a registry entry (the command + args + env).
    fn make_agent(entry: &crate::config::AgentEntry) -> AcpAgent {
        AcpAgent::new(
            AcpAgentConfig::new(entry.command.clone())
                .args(entry.args.clone())
                .envs(entry.env.clone()),
        )
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

    /// (2) `respond_bridge_request` / `respond_permission`: resolve a pending
    /// entry (return `true` + the oneshot resolves with the value); a missing
    /// key returns `false` (no panic).
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

    /// (3) The `text_capture` / `last_message_id` pair: drive a `fake_agent`
    /// `two-msgs` session through a `SessionDriver` with both `Some`; assert
    /// the per-`messageId` texts AND that `last_message_id` is `m2` (the
    /// final-output source, NOT `HashMap` iteration order).
    // The polling loop holds the (brief) `std::sync::Mutex` guards in scope
    // across the `sleep` / teardown awaits on purpose (a plain `StdMutex` is
    // the driver's capture type by design — the guards are dropped before
    // each await; clippy's liveness analysis is scope-based, not `drop`-aware).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn text_capture_and_last_message_id_track_distinct_messages() {
        let config_dir = temp_config_dir();
        let agent_bin = unique_fake_agent(&config_dir);
        write_agents_json_cmd(&agent_bin, &config_dir, Some("two-msgs"));

        let (tx, rx) = std::sync::mpsc::channel();
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

        // Drive the session (the fake agent in `two-msgs` mode answers
        // initialize + session/new, then streams m1 then m2 on prompt).
        let establish_cwd = cwd.clone();
        let driver_clone = driver.clone();
        let sink_clone = sink.clone();
        let entry_clone = entry.clone();
        let info = drive_with_retry(|| {
            let agent = make_agent(&entry_clone);
            let cwd = establish_cwd.clone();
            let sink = sink_clone.clone();
            let establish_cwd = establish_cwd.clone();
            let driver_inner = driver_clone.clone();
            async move {
                driver_inner
                    .drive_session(
                        agent,
                        "fake",
                        String::new(),
                        cwd,
                        &sink,
                        None,
                        None,
                        move |cx: ConnectionTo<agent_client_protocol::Agent>| {
                            let cwd = establish_cwd.clone();
                            async move {
                                let init = cx
                                    .send_request(
                                        InitializeRequest::new(ProtocolVersion::V1)
                                            .client_capabilities(
                                                ClientCapabilities::default()
                                                    .fs(FileSystemCapabilities::default()
                                                        .read_text_file(true)
                                                        .write_text_file(true))
                                                    .terminal(false),
                                            ),
                                    )
                                    .block_task()
                                    .await?;
                                let new_session = cx
                                    .send_request(NewSessionRequest::new(cwd.clone()))
                                    .block_task()
                                    .await?;
                                Ok((
                                    new_session.session_id.clone(),
                                    SessionInfo {
                                        session_id: new_session.session_id.clone(),
                                        agent_id: "fake".to_string(),
                                        cwd,
                                        capabilities: init.agent_capabilities,
                                        config_options: None,
                                    },
                                ))
                            }
                        },
                    )
                    .await
            }
        })
        .await
        .expect("drive_session should establish");

        // Send a prompt to trigger the chunks.
        let cx = driver
            .sessions
            .lock()
            .await
            .get(&info.session_id)
            .unwrap()
            .cx
            .clone();
        let request = PromptRequest::new(
            info.session_id.clone(),
            vec![ContentBlock::Text(TextContent::new("hi".to_string()))],
        );
        cx.send_request(request)
            .block_task()
            .await
            .expect("prompt should succeed");

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

        // Teardown (best effort — the process group dies on close).
        close_session_internal(&driver, &info.session_id).await;
        let _ = std::fs::remove_dir_all(&config_dir);
        let _ = rx;
    }

    /// (4) The `external_close` arm: a driver task with `external_close: Some`
    /// tears down when the external flag flips DURING the establish phase (the
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn external_close_tears_down_during_establish() {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let config_dir = temp_config_dir();
            let agent_bin = unique_fake_agent(&config_dir);
            write_agents_json_cmd(&agent_bin, &config_dir, Some("hang"));

            let (tx, _rx) = std::sync::mpsc::channel();
            let sink: Arc<dyn EventSink> = Arc::new(TestSink { tx });

            let registry = Registry::load(&config_dir).unwrap();
            let entry = registry.get("fake").unwrap();
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

            let establish_cwd = cwd.clone();
            let driver_clone = driver.clone();
            let sink_clone = sink.clone();
            let registry_entry = entry.clone();
            let drive = tokio::spawn(async move {
                driver_clone
                    .drive_session(
                        make_agent(&registry_entry),
                        "fake",
                        String::new(),
                        establish_cwd.clone(),
                        &sink_clone,
                        None,
                        Some(external_close),
                        move |cx: ConnectionTo<agent_client_protocol::Agent>| {
                            let cwd = establish_cwd.clone();
                            async move {
                                let init = cx
                                    .send_request(
                                        InitializeRequest::new(ProtocolVersion::V1)
                                            .client_capabilities(
                                                ClientCapabilities::default()
                                                    .fs(FileSystemCapabilities::default()
                                                        .read_text_file(true)
                                                        .write_text_file(true))
                                                    .terminal(false),
                                            ),
                                    )
                                    .block_task()
                                    .await?;
                                let new_session = cx
                                    .send_request(NewSessionRequest::new(cwd.clone()))
                                    .block_task()
                                    .await?;
                                Ok((
                                    new_session.session_id.clone(),
                                    SessionInfo {
                                        session_id: new_session.session_id.clone(),
                                        agent_id: "fake".to_string(),
                                        cwd,
                                        capabilities: init.agent_capabilities,
                                        config_options: None,
                                    },
                                ))
                            }
                        },
                    )
                    .await
            });

            tokio::time::sleep(Duration::from_millis(300)).await;
            *kind.lock().unwrap() = Some(CloseKind::User);
            let _ = tx.send(true);

            let result = tokio::time::timeout(Duration::from_secs(8), drive).await;

            let final_res = match result {
                Ok(inner) => inner,
                Err(_) => panic!("Test timed out at 8s"),
            };

            match final_res {
                Ok(Err(crate::agent::errors::AcpError::SpawnFailed { .. })) if attempts < 3 => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Ok(Err(crate::agent::errors::AcpError::InitializeFailed { detail })) => {
                    assert!(
                        detail.contains("agent did not complete initialize/session-new"),
                        "Expected teardown (User), but got: {}",
                        detail
                    );
                    break;
                }
                Ok(Ok(_)) => panic!("Session established unexpectedly"),
                Err(e) => panic!("Task panicked: {:?}", e),
                Ok(Err(e)) => panic!("Unexpected error: {:?}", e),
            }
        }
    }

    async fn drive_with_retry<F, Fut>(
        attempt_fn: F,
    ) -> Result<SessionInfo, crate::agent::errors::AcpError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<SessionInfo, crate::agent::errors::AcpError>>,
    {
        crate::test_support::run_with_retry(attempt_fn).await
    }
}

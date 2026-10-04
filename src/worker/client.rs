//! The Supervisor-side Worker client (ADR 0025 Task 3): one `WorkerHandle`
//! per Worker process — spawns `<exe> --worker` (piped stdio), speaks the
//! JSONL protocol, and routes the Worker's `Outbound` frames into the
//! `WorkerInboundEvent` vocabulary (the 4 store-frame variants →
//! `Store(StoreFrame::…)`; the control frames verbatim). Crash detection
//! is the process exit (the read loop ends on the stdout EOF →
//! `Exited` + the `exited` watch).
//!
//! The `exe` PATH is a PARAMETER: production callers pass
//! `std::env::current_exe()` (the self-exec); tests/e2e pass the
//! `fake_worker` fixture path (the `CARGO_MANIFEST_DIR`/`target/debug`
//! convention — a no-arg `current_exe()`-only version would point at
//! the test-harness binary under `cargo test` — wrong).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};

use crate::agent::debuglog;
use crate::agent::events::RpcEvent;
use crate::agent::harness::catalog::Model;
use crate::agent::permission::PermissionOutcome;
use crate::agent::tools::ImageRef;
use crate::agent::worker::protocol::{
    decode_outbound, encode_inbound, Inbound, Outbound, StartEnv, StoreFrame, SubagentDispatchWire,
};

/// A Worker client error.
#[derive(Debug)]
pub struct WorkerError {
    pub kind: WorkerErrorKind,
}

/// The `WorkerError` classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerErrorKind {
    /// The `spawn` failed (the binary is missing / not executable).
    SpawnFailed,
    /// The `ready` handshake did not complete within the bound.
    ReadyTimeout,
    /// A stdio I/O failure (the stdin writer / stdout reader).
    Io(String),
    /// A protocol failure (a `worker-error` frame, or a malformed
    /// outbound line the decoder rejected).
    Protocol(String),
    /// The Worker process has already exited (a `send_*` after the
    /// `exited` watch is set — the frame is dropped).
    WorkerExited,
}

impl WorkerErrorKind {
    fn describe(&self) -> String {
        match self {
            WorkerErrorKind::SpawnFailed => "spawn failed".to_string(),
            WorkerErrorKind::ReadyTimeout => "ready timeout".to_string(),
            WorkerErrorKind::Io(m) => format!("io: {m}"),
            WorkerErrorKind::Protocol(m) => format!("protocol: {m}"),
            WorkerErrorKind::WorkerExited => "worker exited".to_string(),
        }
    }
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "worker error: {}", self.kind.describe())
    }
}

impl std::error::Error for WorkerError {}

/// One inbound observation from a Worker process (the read loop's
/// output — every `Outbound` frame routed into this vocabulary).
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerInboundEvent {
    /// The startup handshake (the Worker's binary version — emitted
    /// BEFORE `Start`).
    Ready { version: String },
    /// The event stream (the existing `RpcEvent` vocabulary verbatim —
    /// the Supervisor's INTERNAL bookkeeping: settle detection, the
    /// `SubagentCapture`'s usage accumulation, the debug log — NOT the
    /// UI).
    Event(RpcEvent),
    /// The persistence frames (the `Store` seam over IPC — the
    /// Supervisor's `TranscriptPersister` applies them to SQLite, the
    /// sole writer).
    Store(StoreFrame),
    /// The FULL gate payload verbatim (the `permission-request` sink
    /// payload — `sessionId` / `requestId` / `request: { toolCall,
    /// options }`; the frontend consumes exactly those fields). `id` =
    /// the payload's `requestId` (the `PendingPermissions` map key).
    PermissionRequest { id: String, payload: Value },
    /// The FULL interactive payload verbatim (`method` / `source` /
    /// `toolCallId` / `params`).
    InteractiveRequest { id: String, payload: Value },
    /// The `subagent` tool's dispatch frame (the Supervisor runs the
    /// subagent-dispatch flow — `WorkerManager::dispatch_subagent`).
    SubagentDispatch(SubagentDispatchWire),
    /// The loop's `SubagentCancel` (a parent turn abort cancelled an
    /// in-flight dispatch — the Supervisor aborts + reaps the
    /// subagent Worker).
    SubagentCancel { id: String },
    /// The CATCH-ALL for every other `EventSink` emission — the ENTIRE
    /// UI contract (the `interactive-event` todo frames,
    /// `interactive-request-close` modal cleanup, the `session-update`
    /// context-usage display frames — the Supervisor re-emits it as the
    /// same-named Tauri event verbatim) + the `SubagentCapture`'s input
    /// (the enveloped `agent_message_chunk` `session-update` stream).
    SinkFrame { event: String, payload: Value },
    /// A Worker-side error (e.g. the loop task panicked — the crash
    /// log is already written; the process exits right after).
    WorkerError { code: String, message: String },
    /// The process exited (crash detection — the read loop ended on
    /// the stdout EOF; the code is the `child.wait()` result).
    Exited(Option<i32>),
}

impl WorkerInboundEvent {
    /// The `agent_settled` bookkeeping event (the subagent drive's
    /// settle signal).
    pub fn is_agent_settled(&self) -> bool {
        matches!(self, WorkerInboundEvent::Event(RpcEvent::agent_settled))
    }
}

/// The Supervisor-side Worker client: one child process (`<exe>
/// --worker`), a stdin writer task (one line per message), a stdout
/// read task (line-by-line → `decode_outbound` → routed
/// `WorkerInboundEvent`s on a `broadcast` channel), the `round_trips`
/// bookkeeping (a `permission-response` / `interactive-response`
/// resolves the pending oneshot of the matching request `id` — proof
/// the `PermissionOutcome` / `Value` was delivered verbatim), the
/// `ready` watch (the `ready` handshake — the version), and the
/// `exited` watch (the process exit — crash detection).
///
/// `Clone` (the manager stores + pumps + reaps clones): the `broadcast`
/// `Sender` / the `watch` `Sender`s / the `UnboundedSender` are all
/// `Clone`; a `events()` call is a FRESH `broadcast` receiver (a new
/// cursor — it sees every frame sent AFTER the subscription; the
/// `Ready` frame is observed via the `ready` watch instead, so the
/// `wait_ready` race is gone).
pub struct WorkerHandle {
    /// The child process (interior-mutable — `kill` needs `&mut`
    /// (`Child::kill` / `Child::wait` are `&mut`); the read task holds
    /// the lock ONLY across the final `wait()` (a dead process reaps
    /// instantly — no contention with `kill`). `Arc`-wrapped so the
    /// handle is `Clone` (the manager stores + pumps + reaps clones).
    child: std::sync::Arc<tokio::sync::Mutex<tokio::process::Child>>,
    /// The stdin writer task's input (one line per message — the
    /// `encode_inbound` output, `\n`-terminated).
    stdin_tx: mpsc::UnboundedSender<String>,
    /// The read loop's output (a `broadcast` channel — `events()` is a
    /// FRESH receiver per call; the read task holds its own keepalive
    /// receiver for the task's lifetime, so the channel is live from
    /// the start and the read task's `send` never errors before the
    /// first subscriber).
    events_tx: tokio::sync::broadcast::Sender<WorkerInboundEvent>,
    /// The `ready` handshake (the read task sends the version on the
    /// `Ready` frame — `wait_ready` blocks until then; a `worker-error`
    /// frame / the process exit before `ready` is an error — the
    /// `exited` watch races it).
    ready: watch::Sender<Option<String>>,
    /// The response round-trips (request `id` → oneshot resolved when
    /// the matching `permission-response` / `interactive-response` is
    /// sent — the `PermissionOutcome` / `Value` delivery proof).
    /// `Arc`-wrapped so the handle is `Clone`.
    round_trips: std::sync::Arc<std::sync::Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    /// The process exit (the read task sends the code after
    /// `child.wait()` — `exited()` blocks until then).
    exited: watch::Sender<Option<i32>>,
}

impl Clone for WorkerHandle {
    fn clone(&self) -> Self {
        Self {
            child: self.child.clone(),
            stdin_tx: self.stdin_tx.clone(),
            events_tx: self.events_tx.clone(),
            ready: self.ready.clone(),
            round_trips: self.round_trips.clone(),
            exited: self.exited.clone(),
        }
    }
}

impl WorkerHandle {
    /// Spawn `<exe> --worker` (piped stdio). The `exe` PATH is a
    /// PARAMETER: production callers pass
    /// `std::env::current_exe()` (the self-exec); tests/e2e pass the
    /// `fake_worker` fixture path. On Unix the child owns its process
    /// group (`setpgid(0, 0)` — the `RealSudoRunner` pattern) so
    /// `kill` SIGKILLs the whole group.
    pub fn spawn(exe: &std::path::Path) -> Result<Self, WorkerError> {
        let mut cmd = tokio::process::Command::new(exe);
        cmd.arg("--worker");
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());
        // The child owns its process group (a single async-signal-safe
        // libc call — the `RealSudoRunner` pattern): `kill` SIGKILLs
        // the whole group (a Worker that forked further does not
        // survive a bare kill of the direct child).
        #[cfg(unix)]
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn().map_err(|e| WorkerError {
            kind: WorkerErrorKind::Io(format!("spawn the worker: {e}")),
        })?;
        // The lifecycle transition (the `ARCHIMEDES_DEBUG` log — a
        // no-op when the var is unset).
        debuglog::log(&format!("worker spawn: {}", exe.display()));
        // Take the stdio handles BEFORE the `Arc` wrap (`take` needs
        // `&mut Child`).
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        // The child is `Arc`-wrapped (the handle is `Clone` — the
        // manager stores + pumps + reaps clones; the read task holds
        // the lock ONLY across the final `wait()`).
        let child = std::sync::Arc::new(tokio::sync::Mutex::new(child));

        // The `ready` / `exited` watches (the `exited` watch is ALSO
        // the stdin writer's shutdown signal — the writer `select`s
        // on it, so a dead Worker's writer task ends and the
        // `UnboundedSender`'s receiver drops; `send_inbound_frame`
        // checks it directly for the deterministic error).
        let (ready_tx, _ready_rx) = watch::channel(None);
        let (exited_tx, _exited_rx) = watch::channel(None);
        // The stdin writer task (drains `stdin_tx` → line writes,
        // `flush` per line — the unbounded channel never fails; a
        // write failure means the child's stdin is gone — the task
        // ends; the `exited` watch also ends it — the child is gone,
        // so the write would EPIPE anyway).
        let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel::<String>();
        let mut writer_exited_rx = exited_tx.subscribe();
        tokio::spawn(async move {
            let mut w = tokio::io::BufWriter::new(stdin);
            loop {
                tokio::select! {
                    line = stdin_rx.recv() => {
                        let Some(line) = line else {
                            break; // the sender is dropped.
                        };
                        if w.write_all(line.as_bytes()).await.is_err()
                            || w.flush().await.is_err()
                        {
                            break; // the child's stdin is gone.
                        }
                    }
                    _ = writer_exited_rx.changed() => {
                        break; // the process exited — the stdin is dead.
                    }
                }
            }
        });

        // The stdout read task (line-by-line → `decode_outbound` →
        // routed `WorkerInboundEvent`s on the `broadcast` channel; the
        // task holds a keepalive receiver for its lifetime, so the
        // channel is live from the start and `send` never errors
        // before the first subscriber; on the EOF → the process exit —
        // `child.wait()` reaps the child, the `exited` watch + the
        // `Exited` frame).
        let (events_tx, _events_keepalive) = tokio::sync::broadcast::channel(256);
        let events_tx2 = events_tx.clone();
        let ready_tx2 = ready_tx.clone();
        let exited_tx2 = exited_tx.clone();
        let child_task = child.clone();
        tokio::spawn(async move {
            // The keepalive receiver (owned for the task's lifetime —
            // the channel is live from the start; `send` returns `Err`
            // only when the channel is closed — the task's end).
            let _keepalive = events_tx2.subscribe();
            let mut r = tokio::io::BufReader::new(stdout);
            let mut line = String::new();
            loop {
                match r.read_line(&mut line).await {
                    Ok(0) => break, // EOF — the child's stdout is gone.
                    Ok(_) => {
                        let l = line.trim_end_matches(['\n', '\r']).to_string();
                        line.clear();
                        if l.is_empty() {
                            continue;
                        }
                        // The raw inbound line (the `ARCHIMEDES_DEBUG` log —
                        // truncated to 2 KB — a streaming token stream would
                        // otherwise be unbounded; a no-op when unset).
                        debuglog::log_truncated("←", &l);
                        let frame = match decode_outbound(&l) {
                            Ok(f) => f,
                            Err(e) => {
                                eprintln!("worker client: bad outbound line: {e}");
                                continue;
                            }
                        };
                        let Some(evt) = route_outbound(frame) else {
                            continue; // an unknown `type` — permissive.
                        };
                        // The `ready` handshake (the `Ready` frame —
                        // the version on the `ready` watch; a
                        // `worker-error` frame before `ready` is a
                        // `Protocol` error — the `exited` watch races
                        // it in `wait_ready`). `send_replace` (NOT
                        // `send` — tokio's `send` is a NO-OP when the
                        // channel has zero receivers — the `ready` /
                        // `exited` receivers are subscribed AFTER
                        // `spawn`, so a `send` before the first
                        // `subscribe` would drop the value).
                        if let WorkerInboundEvent::Ready { version } = &evt {
                            ready_tx2.send_replace(Some(version.clone()));
                        }
                        // The `broadcast` `send` is synchronous (no
                        // backpressure — the 256-frame ring rotates;
                        // `Err` only when the channel is closed — the
                        // task's end).
                        if events_tx2.send(evt).is_err() {
                            break; // the channel is closed.
                        }
                    }
                    Err(e) => {
                        eprintln!("worker client: stdout read failed: {e}");
                        break;
                    }
                }
            }
            // The process exit (crash detection): the read loop ended
            // (the stdout EOF) — reap the child + publish the code.
            // `send_replace` (NOT `send` — tokio's `send` is a NO-OP
            // when the channel has zero receivers — the `exited`
            // receiver is subscribed AFTER `spawn`, so a `send` before
            // the first `subscribe` would drop the value and
            // `exited()` would wait forever).
            let code = child_task
                .lock()
                .await
                .wait()
                .await
                .ok()
                .and_then(|s| s.code());
            exited_tx2.send_replace(code);
            let _ = events_tx2.send(WorkerInboundEvent::Exited(code));
        });

        Ok(Self {
            child,
            stdin_tx,
            events_tx,
            ready: ready_tx,
            round_trips: std::sync::Arc::new(Mutex::new(HashMap::new())),
            exited: exited_tx,
        })
    }

    /// The `ready` handshake (bounded — the version; a `worker-error`
    /// frame or the process exit before `ready` is an error — the
    /// `ready` watch races the `exited` watch + the timeout).
    pub async fn wait_ready(&self, timeout: Duration) -> Result<String, WorkerError> {
        let mut ready_rx = self.ready.subscribe();
        let mut exited_rx = self.exited.subscribe();
        tokio::select! {
            r = ready_rx.wait_for(|v| v.is_some()) => match r {
                Ok(v) => Ok((*v).clone().unwrap()),
                // The `ready` sender is gone (the read task died
                // without publishing — treat as a timeout).
                Err(_) => Err(WorkerError {
                    kind: WorkerErrorKind::ReadyTimeout,
                }),
            },
            r = exited_rx.wait_for(|v| v.is_some()) => {
                // The process exited before `ready` (a crash / an
                // early exit — the `ready` handshake never completed).
                match r {
                    Ok(code) => Err(WorkerError {
                        kind: WorkerErrorKind::Io(format!(
                            "the worker exited before ready (code {code:?})"
                        )),
                    }),
                    Err(_) => Err(WorkerError {
                        kind: WorkerErrorKind::ReadyTimeout,
                    }),
                }
            }
            _ = tokio::time::sleep(timeout) => Err(WorkerError {
                kind: WorkerErrorKind::ReadyTimeout,
            }),
        }
    }

    /// Send one `Inbound` frame (the `encode_inbound` line — the
    /// generic form; the `send_*` methods are the typed wrappers).
    ///
    /// An ERROR when the process has already exited (the `exited`
    /// watch is set — a frame for a dead Worker is dropped) or when
    /// the writer task is gone (the `stdin` `UnboundedSender`'s
    /// receiver dropped).
    pub fn send_inbound_frame(&self, msg: Inbound) -> Result<(), WorkerError> {
        // The process is already gone (the `exited` watch — the read
        // task set it on the stdout EOF): a frame for a dead Worker
        // is an error (the caller can retry on a fresh Worker).
        if self.exited.borrow().is_some() {
            return Err(WorkerError {
                kind: WorkerErrorKind::WorkerExited,
            });
        }
        let line = encode_inbound(&msg);
        // The raw outbound line (the `ARCHIMEDES_DEBUG` log — truncated
        // to 2 KB; a no-op when the var is unset).
        debuglog::log_truncated("→", &line);
        self.stdin_tx.send(line).map_err(|_| WorkerError {
            kind: WorkerErrorKind::Io(
                "the worker's stdin is closed (the process is gone?)".to_string(),
            ),
        })
    }

    /// The session-start envelope (encodes `Inbound::Start`).
    pub fn send_start(&self, env: &StartEnv) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Start(Box::new(env.clone())))
    }

    /// A prompt (the existing `Prompt` type's fields verbatim).
    pub fn send_prompt(&self, text: &str, images: &[ImageRef]) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Prompt {
            text: text.to_string(),
            images: images.to_vec(),
        })
    }

    /// A config update (model / thinking / trusted — the `ControlCmd`
    /// queue + the `StaticTrustSource` flip).
    pub fn send_config(
        &self,
        model: Option<&Model>,
        thinking: Option<&str>,
        trusted: Option<bool>,
    ) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Config {
            model: model.cloned(),
            thinking: thinking.map(str::to_string),
            trusted,
        })
    }

    /// A turn abort (the current-turn token cancel — the session stays
    /// alive).
    pub fn send_abort(&self) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Abort)
    }

    /// The session close (the Worker self-exits 0 — the Supervisor does
    /// NOT close the stdin pipe on `Close`).
    pub fn send_close(&self) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Close)
    }

    /// A permission response (the REAL `PermissionOutcome` verbatim —
    /// the 3-option `Selected { option_id }` or `Cancelled`; resolves
    /// the `round_trips` oneshot of the request `id`).
    pub fn send_permission_response(
        &self,
        id: &str,
        outcome: &PermissionOutcome,
    ) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::PermissionResponse {
            id: id.to_string(),
            outcome: outcome.clone(),
        })?;
        if let Some(tx) = self
            .round_trips
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id)
        {
            let _ = tx.send(Value::Object(Default::default()));
        }
        Ok(())
    }

    /// An interactive response (the `Value` verbatim — the `result`
    /// with no wrapper; resolves the `round_trips` oneshot of the
    /// request `id`).
    pub fn send_interactive_response(&self, id: &str, value: &Value) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::InteractiveResponse {
            id: id.to_string(),
            value: value.clone(),
        })?;
        if let Some(tx) = self
            .round_trips
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id)
        {
            let _ = tx.send(value.clone());
        }
        Ok(())
    }

    /// Register a response round-trip (the request `id` → oneshot
    /// resolved when the matching response is sent — the delivery
    /// proof; the test observation point).
    pub fn register_round_trip(&self, id: &str) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.round_trips
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id.to_string(), tx);
        rx
    }

    /// The number of pending round-trips (the test observation point).
    pub fn round_trip_count(&self) -> usize {
        self.round_trips
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .len()
    }

    /// The read loop's output (the pump owns the sole ACTIVE receiver —
    /// a clone; the stored receiver is idle, so every frame routes to
    /// the polled clone).
    /// The read loop's output (a FRESH `broadcast` receiver per call —
    /// it sees every frame sent AFTER the subscription; the `Ready`
    /// frame is observed via the `ready` watch instead, so the
    /// `wait_ready` race is gone). A `Lagged(n)` error skips `n`
    /// frames (the 256-frame ring rotated past the receiver's cursor —
    /// a stalled consumer; the pump drains promptly, so it doesn't
    /// lag).
    pub fn events(&self) -> tokio::sync::broadcast::Receiver<WorkerInboundEvent> {
        self.events_tx.subscribe()
    }

    /// Block until the process exits (crash detection — the exit code;
    /// `None` when the exit status carried no code — a signal).
    /// The process exit (crash detection — the read task set it on
    /// the stdout EOF; a missing code — the read task died without
    /// publishing — is `None`).
    pub async fn exited(&self) -> Option<i32> {
        let mut rx = self.exited.subscribe();
        // The code (copied out of the `Ref` — the `Ref` borrows `rx`
        // for the match, so the value is COPIED into a local first;
        // a missing code — the read task died without publishing —
        // is `None`).
        let code = match rx.wait_for(|v| v.is_some()).await {
            Ok(v) => *v,
            Err(_) => None,
        };
        code
    }

    /// Kill the Worker: on Unix the WHOLE process group SIGKILL (the
    /// child is the group leader via `setpgid` — the `RealSudoRunner`
    /// pattern) + a fallback kill of the direct child; on Windows the
    /// direct `child.kill()`. Best-effort (an already-reaped child is a
    /// no-op).
    pub async fn kill(&self) -> Result<(), WorkerError> {
        let mut guard = self.child.lock().await;
        #[cfg(unix)]
        {
            // A negative pid SIGKILLs the WHOLE process group (the
            // child is the group leader via `setpgid(0, 0)`); `None`
            // when the child was already reaped (then the kill below
            // is a no-op too).
            if let Some(pid) = guard.id() {
                let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
            }
            // Fallback: kill the direct child as well.
            let _ = guard.kill().await;
        }
        #[cfg(not(unix))]
        {
            let _ = guard.kill().await;
        }
        Ok(())
    }
}

/// Route one `Outbound` frame into the `WorkerInboundEvent` vocabulary
/// (the 4 store-frame variants → `Store(StoreFrame::…)`; the control
/// frames verbatim; `Unknown` → `None` — the surface is unversioned,
/// an unknown `type` is ignored, never an error).
fn route_outbound(frame: Outbound) -> Option<WorkerInboundEvent> {
    Some(match frame {
        Outbound::Ready { version, .. } => WorkerInboundEvent::Ready { version },
        Outbound::Event { event } => WorkerInboundEvent::Event(event),
        Outbound::TranscriptInsert {
            session_id,
            seq,
            role,
            content_json,
        } => WorkerInboundEvent::Store(StoreFrame::Insert {
            session_id,
            seq,
            role,
            content_json,
        }),
        Outbound::TranscriptReplace { session_id, rows } => {
            WorkerInboundEvent::Store(StoreFrame::Replace { session_id, rows })
        }
        Outbound::DisplayUpsert { session_id, rows } => {
            WorkerInboundEvent::Store(StoreFrame::Display { session_id, rows })
        }
        Outbound::ContextUsage {
            session_id,
            used,
            window,
        } => WorkerInboundEvent::Store(StoreFrame::ContextUsage {
            session_id,
            used,
            window,
        }),
        Outbound::PermissionRequest { id, payload } => {
            WorkerInboundEvent::PermissionRequest { id, payload }
        }
        Outbound::InteractiveRequest { id, payload } => {
            WorkerInboundEvent::InteractiveRequest { id, payload }
        }
        Outbound::SubagentDispatch {
            id,
            parent_session_id,
            parent_cwd,
            parent_enabled_tools,
            agent_name,
            launch,
            task,
            model_key,
        } => WorkerInboundEvent::SubagentDispatch(SubagentDispatchWire {
            id,
            parent_session_id,
            parent_cwd,
            parent_enabled_tools,
            agent_name,
            launch,
            task,
            model_key,
        }),
        Outbound::SubagentCancel { id } => WorkerInboundEvent::SubagentCancel { id },
        Outbound::SinkFrame { event, payload } => WorkerInboundEvent::SinkFrame { event, payload },
        Outbound::WorkerError {
            code,
            message,
            backtrace: _,
        } => WorkerInboundEvent::WorkerError { code, message },
        // The surface is unversioned — an unknown outbound type is
        // ignored (never an error).
        Outbound::Unknown { .. } => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `fake_worker` fixture (the `fake_mcp_stdio` convention — the
    /// `CARGO_MANIFEST_DIR`/`target/debug` path; `cargo test` builds the
    /// bin targets).
    pub fn fixture_path() -> std::path::PathBuf {
        std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_worker"
        ))
    }

    /// A test `StartEnv` (a fake model the fixture's `SubagentDispatch`
    /// `model_key` — `fake/m1` — resolves against).
    pub fn test_env(session_id: &str, enabled_tools: Option<Vec<String>>) -> StartEnv {
        use crate::agent::harness::catalog::{Model, ModelCatalog};
        use crate::agent::worker::protocol::StartMode;

        fn model(id: &str) -> Model {
            Model {
                id: id.to_string(),
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
        StartEnv::from_parts(
            session_id.to_string(),
            "/tmp/space".to_string(),
            StartMode::Fresh,
            None,
            model("m1"),
            ModelCatalog {
                models: vec![model("m1")],
                ..Default::default()
            },
            None,
            true,
            enabled_tools,
            "/tmp/config".to_string(),
            true,
            None,
        )
    }

    /// Spawn the fixture (the `WorkerHandle` API — the test factory).
    pub fn spawn_fixture() -> Result<WorkerHandle, WorkerError> {
        WorkerHandle::spawn(&fixture_path())
    }

    /// Collect events until `pred` matches (bounded — a deadline, so a
    /// missing frame is a test failure, not a hang). The receiver is a
    /// FRESH `broadcast` receiver (subscribed before the prompt — it
    /// sees every frame sent after the subscription; a `Lagged` skip is
    /// tolerated, a `Closed` ends the loop).
    pub async fn collect_until(
        rx: &mut tokio::sync::broadcast::Receiver<WorkerInboundEvent>,
        pred: impl Fn(&WorkerInboundEvent) -> bool,
    ) -> Vec<WorkerInboundEvent> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut events = Vec::new();
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Ok(evt)) => {
                    events.push(evt);
                    if pred(events.last().unwrap()) {
                        return events;
                    }
                }
                // `Lagged` — the ring rotated past the cursor (a
                // stalled consumer; tolerated — the frames are lost,
                // the loop continues).
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                    let _ = n;
                    continue;
                }
                // `Closed` — the read task is gone (the process is
                // reaped; the loop ends).
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
                Err(_) => continue,
            }
        }
        events
    }

    /// `spawn` + the `ready` handshake (the version).
    #[tokio::test]
    async fn spawn_and_wait_ready_returns_the_version() {
        let handle = spawn_fixture().expect("the fixture binary exists (cargo test built it)");
        let version = handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("the ready handshake completes");
        assert!(!version.is_empty(), "the ready frame carries the version");
        handle.send_close().expect("close encodes");
        let code = handle.exited().await;
        assert_eq!(code, Some(0), "close → exit 0");
    }

    /// A `prompt` yields the canned `RpcEvent` stream + store frames +
    /// `SinkFrame`s IN ORDER (the `agent_settled` terminates the turn).
    #[tokio::test]
    async fn a_prompt_yields_the_canned_event_store_and_sink_sequence_in_order() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        handle
            .send_start(&test_env("s1", Some(vec!["bash".to_string()])))
            .expect("start");
        let mut rx = handle.events();
        handle.send_prompt("hello", &[]).expect("prompt");
        // Collect until the LAST canned frame — `agent_settled`
        // (the fixture mirrors the REAL harness: the `SinkFrame`
        // `agent_message_chunk`s stream DURING the message and the
        // store frames land at the message completion — ALL before
        // `agent_settled`).
        let events = collect_until(&mut rx, |e| e.is_agent_settled()).await;
        let kinds: Vec<String> = events
            .iter()
            .map(|e| match e {
                WorkerInboundEvent::Event(RpcEvent::turn_start) => "turn_start".to_string(),
                WorkerInboundEvent::Event(RpcEvent::message_start { .. }) => {
                    "message_start".to_string()
                }
                WorkerInboundEvent::Event(RpcEvent::message_end { .. }) => {
                    "message_end".to_string()
                }
                WorkerInboundEvent::Event(RpcEvent::turn_end { .. }) => "turn_end".to_string(),
                WorkerInboundEvent::Event(RpcEvent::agent_settled) => "agent_settled".to_string(),
                WorkerInboundEvent::Store(StoreFrame::Insert { role, .. }) => {
                    format!("store-insert:{role}")
                }
                WorkerInboundEvent::Store(StoreFrame::Replace { .. }) => {
                    "store-replace".to_string()
                }
                WorkerInboundEvent::Store(StoreFrame::Display { .. }) => {
                    "store-display".to_string()
                }
                WorkerInboundEvent::Store(StoreFrame::ContextUsage { .. }) => {
                    "store-context-usage".to_string()
                }
                WorkerInboundEvent::SinkFrame { .. } => "sink-frame".to_string(),
                other => format!("other:{other:?}"),
            })
            .collect();
        // The canned stream (the `RpcEvent`s in order).
        assert!(
            kinds.iter().position(|k| k == "turn_start").is_some(),
            "a `turn_start` was emitted: {kinds:?}"
        );
        assert!(
            kinds.iter().position(|k| k == "message_start").is_some(),
            "a `message_start` was emitted: {kinds:?}"
        );
        assert!(
            kinds.iter().position(|k| k == "message_end").is_some(),
            "a `message_end` was emitted: {kinds:?}"
        );
        assert!(
            kinds.iter().position(|k| k == "turn_end").is_some(),
            "a `turn_end` was emitted: {kinds:?}"
        );
        // The order (the `RpcEvent`s in order — the `SinkFrame` /
        // store frames interleave BEFORE `agent_settled` — the REAL
        // harness's emission order).
        let pos = |k: &str| kinds.iter().position(|x| x == k);
        let order = [
            "turn_start",
            "message_start",
            "message_end",
            "turn_end",
            "agent_settled",
        ];
        let mut prev = None;
        for k in order {
            let p = pos(k).unwrap_or_else(|| panic!("{k} missing: {kinds:?}"));
            if let Some(prev) = prev {
                assert!(p > prev, "out of order ({k} after {prev}): {kinds:?}");
            }
            prev = Some(p);
        }
        assert!(
            events.iter().any(|e| e.is_agent_settled()),
            "the `agent_settled` terminated the turn: {kinds:?}"
        );
        // The store frames (the Supervisor's persister input — the
        // transcript rows land with the message completion — BEFORE
        // the `turn_end` / `agent_settled`).
        let turn_end_pos = pos("turn_end").unwrap();
        assert!(
            kinds
                .iter()
                .position(|k| k.starts_with("store-insert:"))
                .is_some_and(|p| p < turn_end_pos),
            "store frames were routed (before the turn end): {kinds:?}"
        );
        assert!(
            pos("store-display").is_some_and(|p| p < turn_end_pos),
            "a `DisplayUpsert` was routed to `Store(Display)` (before the turn end): {kinds:?}"
        );
        // The `SinkFrame` (the `SubagentCapture` input — the enveloped
        // `agent_message_chunk` `session-update` stream — streamed
        // DURING the message, BEFORE the `message_end`).
        let message_end_pos = pos("message_end").unwrap();
        assert!(
            kinds
                .iter()
                .position(|k| k == "sink-frame")
                .is_some_and(|p| p < message_end_pos),
            "the `SinkFrame` `agent_message_chunk`s streamed during the message: {kinds:?}"
        );
        assert!(
            events.iter().any(|e| {
                matches!(e, WorkerInboundEvent::SinkFrame { payload, .. }
                    if payload.to_string().contains("agent_message_chunk"))
            }),
            "a `SinkFrame` `agent_message_chunk` was routed: {kinds:?}"
        );
        handle.kill().await.expect("the kill is best-effort");
    }

    /// A `"__crash__"` prompt → the process exits 137 (crash detection —
    /// the `Exited` frame + the `exited` watch).
    #[tokio::test]
    async fn a_crash_prompt_exits_137() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        handle.send_start(&test_env("s2", None)).expect("start");
        let mut rx = handle.events();
        handle.send_prompt("__crash__", &[]).expect("prompt");
        let events = collect_until(&mut rx, |e| matches!(e, WorkerInboundEvent::Exited(_))).await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkerInboundEvent::Exited(Some(137)))),
            "the process exited 137: {events:?}"
        );
        assert_eq!(
            handle.exited().await,
            Some(137),
            "the `exited` watch carries the code"
        );
    }

    /// A `close` → the Worker self-exits 0 (the `Exited` frame + the
    /// `exited` watch).
    #[tokio::test]
    async fn a_close_exits_zero() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        handle.send_start(&test_env("s3", None)).expect("start");
        let mut rx = handle.events();
        handle.send_close().expect("close encodes");
        let events = collect_until(&mut rx, |e| matches!(e, WorkerInboundEvent::Exited(_))).await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, WorkerInboundEvent::Exited(Some(0)))),
            "the process exited 0: {events:?}"
        );
        assert_eq!(handle.exited().await, Some(0));
    }

    /// A `permission-response` round-trip completes the canned
    /// permission flow (the `PermissionRequest` arrives with the FULL
    /// gate payload verbatim — `requestId` + `request: { toolCall,
    /// options }`; the tool-execution events arrive AFTER the response;
    /// the `PermissionOutcome` was delivered verbatim — the
    /// `round_trips` oneshot resolves).
    #[tokio::test]
    async fn a_permission_response_round_trip_completes_the_canned_flow() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        handle.send_start(&test_env("s4", None)).expect("start");
        let mut rx = handle.events();
        handle.send_prompt("__permission__", &[]).expect("prompt");
        // The `PermissionRequest` (the FULL gate payload verbatim).
        let events = collect_until(&mut rx, |e| {
            matches!(e, WorkerInboundEvent::PermissionRequest { .. })
        })
        .await;
        let req = events
            .iter()
            .find_map(|e| match e {
                WorkerInboundEvent::PermissionRequest { id, payload } => {
                    Some((id.clone(), payload.clone()))
                }
                _ => None,
            })
            .expect("the `PermissionRequest` frame arrived");
        assert_eq!(req.0, "p1", "the `id` is the payload's `requestId`");
        // The FULL gate payload verbatim (the `permission.rs:135-158`
        // shape — `requestId` + `request: { toolCall: { title },
        // options }`).
        assert_eq!(req.1["requestId"], "p1");
        assert_eq!(req.1["request"]["toolCall"]["title"], "Allow bash?");
        assert_eq!(req.1["request"]["options"][0]["optionId"], "allow");
        assert_eq!(req.1["request"]["options"][2]["optionId"], "trust-space");
        // The response (the `PermissionOutcome` verbatim — the
        // `round_trips` oneshot resolves when it is sent).
        let mut rt = handle.register_round_trip("p1");
        handle
            .send_permission_response(
                "p1",
                &PermissionOutcome::Selected {
                    option_id: "trust-space".to_string(),
                },
            )
            .expect("the response encodes");
        assert!(
            rt.try_recv().is_ok(),
            "the `PermissionOutcome` was delivered (the round-trip resolved)"
        );
        // The tool-execution events arrive AFTER the response (the
        // `agent_settled` terminates the turn).
        let events = collect_until(&mut rx, |e| e.is_agent_settled()).await;
        assert!(
            events.iter().any(|e| {
                matches!(
                    e,
                    WorkerInboundEvent::Event(RpcEvent::tool_execution_start { .. })
                )
            }),
            "the tool-execution events arrived after the response: {events:?}"
        );
        assert!(
            events.last().is_some_and(|e| e.is_agent_settled()),
            "the turn settled after the tool execution: {events:?}"
        );
        handle.kill().await.expect("best-effort");
    }

    /// A `send_*` on a dead Worker's stdin is an error (the writer task
    /// ended when the child's stdin closed).
    #[tokio::test]
    async fn a_send_on_a_dead_worker_is_an_error() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        handle.send_close().expect("close encodes");
        let _ = handle.exited().await;
        // A little grace for the stdin writer task to notice the EOF.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            handle.send_prompt("late", &[]).is_err(),
            "a send on a dead worker's stdin is an error"
        );
    }
}

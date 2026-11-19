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
//! `fake_worker` fixture path — since S8.3 resolved through
//! `test_support::target_debug_dir()`, the crate's ONE two-levels-up `target/`
//! resolver (a no-arg `current_exe()`-only version would point at the
//! test-harness binary under `cargo test` — wrong).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};

use crate::debuglog;
use crate::events::RpcEvent;
use crate::harness::catalog::Model;
use crate::harness::provider::openai::EMPTY_CONTENT_SANITISE_PLACEHOLDER;
// The `DisplayUpsert` frame's row type — the same one `protocol.rs` names in
// the variant (`crate::harness::store` is where `DisplayRow` lives; the wire
// module does not re-export it).
use crate::harness::store::DisplayRow;
use crate::hostenv::EnvPatch;
use crate::permission::PermissionOutcome;
use crate::tools::ImageRef;
use crate::worker::protocol::{
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
    /// A Supervisor-side fetch render request forwarded from the worker.
    FetchRenderRequest { id: String, payload: Value },
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
    /// A task-board call from the Worker's `IpcTaskStore` (issue #180 F1).
    ///
    /// The Supervisor-side consumer must ANSWER it — the Worker parked a oneshot
    /// before emitting the frame and is waiting on it, racing the turn token. In
    /// the desktop that is D2's `route_event` branch (answering with
    /// `Inbound::TaskResponse` through the handle's inbound writer); the daemon
    /// carries a loud refusal instead (see `fulcrum/src/serve.rs::worker_frame`),
    /// because it has no `TaskStore` to answer from yet.
    ///
    /// THIS VARIANT IS THE CRATE-SIDE FREIGHT of F1 and it is a breaking change
    /// for SOURCE, not for the wire: every exhaustive `match` over this enum must
    /// gain an arm, and there are exactly two — the daemon's `worker_frame` (in
    /// this repo, landed here) and the desktop's `route_event` (D2, which must
    /// therefore land its arm in the SAME commit that bumps the crate pin).
    TaskRequest {
        request_id: String,
        call: crate::seams::TaskCall,
    },
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
/// `Clone`; the `pid` is a `Copy` field copied verbatim (every clone of a
/// handle names the same child); a `events()` call is a FRESH `broadcast`
/// receiver (a new cursor — it sees every frame sent AFTER the
/// subscription; the `Ready` frame is observed via the `ready` watch
/// instead, so the `wait_ready` race is gone).
pub struct WorkerHandle {
    /// The child process (interior-mutable — `kill` needs `&mut`
    /// (`Child::kill` / `Child::wait` are `&mut`); the read task holds
    /// the lock ONLY across the final `wait()` (a dead process reaps
    /// instantly — no contention with `kill`). `Arc`-wrapped so the
    /// handle is `Clone` (the manager stores + pumps + reaps clones).
    child: std::sync::Arc<tokio::sync::Mutex<tokio::process::Child>>,
    /// The child's pid, captured at spawn (`Child::id()` right after
    /// `cmd.spawn()` succeeded) and NEVER re-read: `Child::id()` returns
    /// `None` once tokio has reaped the child (`FusedChild::Done`) — the
    /// exact state a crash leaves behind — so the LAST KNOWN value is what
    /// a crashed Worker's attribution has to work from. `None` only when the
    /// platform gave no pid at spawn.
    pid: Option<u32>,
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
    /// The protocol-version handshake (Fulcrum T1) — the read task
    /// publishes the `protocolVersion` from the `hello` frame, the first
    /// line the Worker writes. A SEPARATE watch from `ready`, because the
    /// two answer different questions: `ready` is "which build is this"
    /// (the BINARY version) and this is "which wire does it speak".
    /// Deliberately NOT routed as a `WorkerInboundEvent` (a 12th variant
    /// would force every consumer, including `session/router.rs`'s
    /// exhaustive `route_event`, to account for a frame the UI never sees).
    protocol_version: watch::Sender<Option<u32>>,
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
            pid: self.pid,
            stdin_tx: self.stdin_tx.clone(),
            events_tx: self.events_tx.clone(),
            ready: self.ready.clone(),
            protocol_version: self.protocol_version.clone(),
            round_trips: self.round_trips.clone(),
            exited: self.exited.clone(),
        }
    }
}

/// Drain a piped child stream line-by-line into `on_line` (a DEDICATED
/// TASK that only ever awaits the read — never a `select!` arm, because a
/// dropped tokio `read_line` future takes the bytes it already consumed
/// with it). The EOF (the child's exit) ends the task.
///
/// # What ends the drain, and what does not
///
/// A GENUINE read error ends it (a pipe that dies mid-read has nothing left
/// to say) — as does EOF. An `InvalidData` (one line of the stream is not
/// valid UTF-8) does NOT: that one line is dropped and the drain continues.
/// See the arm below for why continuing cannot spin, and for why this channel
/// takes the permissive rule where the daemon's frame reader takes the strict
/// one.
fn spawn_pipe_reader<R, F>(pipe: R, mut on_line: F) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    F: FnMut(String) + Send + 'static,
{
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(pipe);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                // EOF — the child is gone. A final line with NO trailing
                // newline still gets forwarded (a panic message that dies
                // mid-pipe is exactly the line worth keeping).
                Ok(0) => {
                    let tail = line.trim_end();
                    if !tail.is_empty() {
                        on_line(tail.to_string());
                    }
                    break;
                }
                Ok(_) => {
                    let line = line.trim_end();
                    if !line.is_empty() {
                        on_line(line.to_string());
                    }
                }
                // ONE BAD BYTE IS NOT A DEAD PIPE. `read_line` validates the
                // bytes it read as UTF-8 AFTER reading them, and reports a
                // failure as `InvalidData`; the bytes of the offending line
                // were already `consume`d from the reader
                // (`tokio-1.54.1/src/io/util/read_until.rs:69`, reached from
                // `read_line.rs:99` BEFORE the `String::from_utf8` at
                // `read_line.rs:100`), and on the error they are NOT handed
                // back to the reader — `finish_string_read`'s `InvalidData`
                // arm (`read_line.rs:77-83`) calls `put_back_original_data`
                // (`read_line.rs:46-50`), which TRUNCATES the newly read bytes
                // away and restores only the caller `String`'s pre-existing
                // prefix. So the bad line has left the stream either way and
                // the next read starts at the NEXT line — continuing cannot
                // busy-loop.
                //
                // WHY THE PERMISSIVE RULE HERE, AND NOT THERE: the daemon's
                // frame reader (`fulcrum/src/transport/frame.rs`, the
                // `String::from_utf8(bytes) else { break }` in its reader
                // task) deliberately ENDS the stream on a non-UTF-8 line, and
                // says so — a socket is a PROTOCOL channel, and a byte stream
                // that is not text is not a peer worth resyncing with. This is
                // the opposite kind of channel: a DIAGNOSTIC one, whose only
                // consumer is the log a supporter reads when a Worker dies. A
                // diagnosis must not be suppressed by bytes in an UNRELATED
                // line — one stray byte from a raw dump or a truncated
                // multi-byte char would otherwise silence that Worker for the
                // rest of its life, with no diagnostic of its own. Losing one
                // undecodable line costs a line; ending the drain costs
                // everything after it.
                Err(ref e) if e.kind() == std::io::ErrorKind::InvalidData => continue,
                // A DEAD PIPE HAS NOTHING LEFT TO SAY. Every other error
                // (`BrokenPipe`, `ConnectionReset`, `UnexpectedEof`, …) is the
                // pipe itself failing, not one line of its content — retrying
                // a dead pipe spins on a permanent error.
                Err(_) => break,
            }
        }
    })
}

/// Build the `Command` the Worker self-spawn runs — everything except the
/// `Stdio` trio and the `#[cfg(unix)]` `pre_exec` process-group block, which
/// stay in [`WorkerHandle::spawn`].
///
/// The seam exists so the env/argv policy can be asserted on a `Command` that
/// was never spawned; it is called by production `spawn`, so it needs no `cfg`
/// and no `allow`.
fn build_command(exe: &std::path::Path) -> tokio::process::Command {
    build_command_with(exe, crate::hostenv::for_child())
}

/// [`build_command`] with the patch as an argument (production passes exactly
/// [`for_child`](crate::hostenv::for_child); the parameter exists
/// because that cached patch is EMPTY under `cargo test` — its gate needs this
/// exe under `$APPDIR`, and under cargo it is `target/debug/deps/<hash>`).
///
/// THE ONE JUDGEMENT CALL IN THIS CHANGE, stated where it can be undone: the
/// Worker is the SAME ELF as the GUI process, so if it ever `dlopen`s a
/// bundled library through a path ONLY `LD_LIBRARY_PATH` resolves, scrubbing
/// breaks the Worker. The expectation is that it does not — the Worker renders
/// nothing and owns no window — but that is an expectation, not a proof, and it
/// is field-tested on a real bundle rather than here.
///
/// The escape hatch is this one call: delete the `patch.apply` below and leave
/// the MCP site, the `sudo` site and the `bash`/`fd`/`rg` sites in place. The
/// pollution then stays INSIDE the Worker and never reaches the user's
/// commands, because those sites scrub again on their own.
///
/// The constructor is `crate::childproc`'s — it owns the child's Windows
/// console window policy (`CREATE_NO_WINDOW`); the Worker's JSONL stdin is a
/// Runtime pipe and stays untouched.
fn build_command_with(exe: &std::path::Path, patch: &EnvPatch) -> tokio::process::Command {
    let mut cmd = crate::childproc::command(exe);
    cmd.arg("--worker");
    // The self-exec inherits the GUI process' tainted env IN FULL: inside an
    // AppImage `current_exe()` resolves THROUGH the mount (`/proc/<pid>/exe` →
    // `/tmp/.mount_archim*/usr/bin/archimedes`), so without this patch the
    // Worker — the process that spawns the user's commands — starts polluted
    // and generation N's garbage propagates down
    // (`docs/research/appimage-child-process-env-leak.md`).
    patch.apply(&mut cmd);
    cmd
}

impl WorkerHandle {
    /// Spawn `<exe> --worker` (piped stdio, and with the inherited env
    /// SCRUBBED of the AppImage's own paths — the decision and its risk are
    /// in [`build_command`], next to the argv it must not disturb). The
    /// `exe` PATH is a PARAMETER: production callers pass
    /// `std::env::current_exe()` (the self-exec); tests/e2e pass the
    /// `fake_worker` fixture path. On Unix the child owns its process
    /// group (`setpgid(0, 0)` — the `RealSudoRunner` pattern) so
    /// `kill` SIGKILLs the whole group.
    pub fn spawn(exe: &std::path::Path) -> Result<Self, WorkerError> {
        let mut cmd = build_command(exe);
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        // STDERR IS PIPE-D, NOT `/dev/null`: a Worker's own diagnostics
        // (`bad inbound line`, `prompt send failed`, the harness's
        // `eprintln!`s) were unobservable, which is how a swallowed IPC
        // frame stayed invisible for a session. The `spawn_pipe_reader`
        // task below drains it into the `ARCHIMEDES_DEBUG` log (and
        // draining it also means a chatty Worker cannot block on a full
        // pipe).
        cmd.stderr(std::process::Stdio::piped());
        // The child owns its process group (a single async-signal-safe
        // libc call — the `RealSudoRunner` pattern): `kill` SIGKILLs
        // the whole group (a Worker that forked further does not
        // survive a bare kill of the direct child). The ENV decision is NOT
        // here — it is applied in [`build_command`], before this block and
        // before `spawn`.
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
        // The pid is read HERE — while `child` is still a live process and
        // `&mut` (the `Arc` wrap and the stdio `take`s come below). It is
        // captured once and carried on the handle: `Child::id()` goes `None`
        // once the child is reaped, which is the state a crash leaves, so a
        // later reader (the crash-log attribution) has nothing else to read.
        let pid = child.id();
        // The lifecycle transition (the `ARCHIMEDES_DEBUG` log — a
        // no-op when the var is unset).
        debuglog::log(&format!("worker spawn: {}", exe.display()));
        // Take the stdio handles BEFORE the `Arc` wrap (`take` needs
        // `&mut Child`).
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");
        // The child is `Arc`-wrapped (the handle is `Clone` — the
        // manager stores + pumps + reaps clones; the read task holds
        // the lock ONLY across the final `wait()`).
        let child = std::sync::Arc::new(tokio::sync::Mutex::new(child));

        // The stderr drain (a DEDICATED TASK — its `read_line` is never
        // cancelled, so no line can be lost mid-read).
        let _stderr_drain = spawn_pipe_reader(stderr, |line| {
            debuglog::log_truncated("worker-stderr:", &line);
        });

        // The `ready` / `exited` watches (the `exited` watch is ALSO
        // the stdin writer's shutdown signal — the writer `select`s
        // on it, so a dead Worker's writer task ends and the
        // `UnboundedSender`'s receiver drops; `send_inbound_frame`
        // checks it directly for the deterministic error).
        let (ready_tx, _ready_rx) = watch::channel(None);
        let (protocol_version_tx, _protocol_version_rx) = watch::channel(None);
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
        let protocol_version_tx2 = protocol_version_tx.clone();
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
                        // The inbound line in the `ARCHIMEDES_DEBUG` log (a
                        // no-op when unset) — logged AFTER the decode,
                        // because the summary needs the typed frame.
                        let frame = match decode_outbound(&l) {
                            Ok(f) => f,
                            Err(e) => {
                                // The undecodable line is STILL logged
                                // verbatim, HERE, because the log call
                                // below is unreachable on this path (this
                                // arm `continue`s). It used to sit above the
                                // decode and so covered this case for free;
                                // moving it down to serve the summary would
                                // otherwise have lost the single most
                                // diagnostic line in the stream — a wire the
                                // two sides disagree about — from the log
                                // entirely.
                                debuglog::log_truncated("←", &l);
                                eprintln!("worker client: bad outbound line: {e}");
                                continue;
                            }
                        };
                        // The log line. Every frame type is logged VERBATIM
                        // (`log_truncated` — 2 KB cap, a no-op under it)
                        // EXCEPT `display-upsert`, which gets the short
                        // summary: its `rows` are a re-SERIALISATION of the
                        // deltas already logged verbatim in this same file,
                        // so the row content stays recoverable while the
                        // quadratic body (75 % of a rotated log) is gone.
                        //
                        // `log` and NOT `log_truncated` — the one asymmetry in
                        // this match, and it is deliberate. `truncate_line` cuts
                        // BYTES, so on this line it lands mid-JSON: the `rowKeys`
                        // array loses its closer, the line stops parsing, and
                        // `rowKeys` is precisely what the cut removes (76.5 % of
                        // the verbatim lines this replaced were already cut
                        // mid-JSON and unparseable — that is the measurement that
                        // motivated the summary). So the line is bounded by
                        // CONSTRUCTION instead of by a cut: `rowKeys` is capped at
                        // `MAX_SUMMARY_ROW_KEYS` entries and every listed field
                        // (and the `sessionId`) at `MAX_SUMMARY_KEY_BYTES`, which
                        // makes the whole line `O(1)` in rows and keeps it inside
                        // the SAME `MAX_SUMMARY_LINE_BYTES` = 2 KB budget every
                        // other inbound frame is cut at (vs the 2.6 MB one
                        // verbatim frame reached). It is therefore exempt from the
                        // truncation MECHANISM and NOT from the size bound — and
                        // it is the only frame type whose logged line is
                        // guaranteed to parse, since a byte cut is exactly what
                        // made 76.5 % of the old lines unparseable. The worst case
                        // is a `message_retracted` frame (one row per erased
                        // thought segment); the ordinary frame is ONE row and
                        // lands near 150 bytes. Both caps, the
                        // `rowKeysOmitted` field that keeps a capped list from
                        // reading as a small frame, and the ceiling itself are
                        // asserted in
                        // `a_large_row_set_is_capped_and_the_summary_stays_valid_json`.
                        // Taken from `&frame` — `route_outbound` below takes
                        // `frame` BY VALUE.
                        match &frame {
                            Outbound::DisplayUpsert { session_id, rows } => {
                                debuglog::log(&format!(
                                    "← {}",
                                    summarise_display_upsert(session_id, rows, l.len())
                                ))
                            }
                            _ => debuglog::log_truncated("←", &l),
                        }
                        // The `hello` handshake (Fulcrum T1): the protocol
                        // version lands on its own watch BEFORE routing,
                        // because `route_outbound` deliberately drops the
                        // frame (it is not a `WorkerInboundEvent`).
                        // `send_replace` for the reason in the `ready` note
                        // below — the receiver is subscribed after `spawn`.
                        if let Outbound::Hello {
                            protocol_version, ..
                        } = &frame
                        {
                            protocol_version_tx2.send_replace(Some(*protocol_version));
                        }
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
            pid,
            stdin_tx,
            events_tx,
            ready: ready_tx,
            protocol_version: protocol_version_tx,
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

    /// The protocol-version handshake (Fulcrum T1 — the bounded twin of
    /// `wait_ready`): the `protocolVersion` the Worker published in its
    /// `hello` frame, the FIRST line it writes.
    ///
    /// A SEPARATE method from `wait_ready` rather than an extra return
    /// value on it, because `wait_ready` answers "which build is this" and
    /// has ≈20 callers (including the `worker_stderr` / `ipc` /
    /// `worker_e2e` integration tests) whose contract must not move. The
    /// two watches are set by the same read task off two consecutive lines,
    /// so a Worker that answered `wait_ready` has almost always answered
    /// this one already — but it is raced independently, because "the
    /// handshake is at line 2" is a wire property this API must not assume.
    pub async fn wait_protocol_version(&self, timeout: Duration) -> Result<u32, WorkerError> {
        let mut version_rx = self.protocol_version.subscribe();
        let mut exited_rx = self.exited.subscribe();
        tokio::select! {
            r = version_rx.wait_for(|v| v.is_some()) => match r {
                // `(*v)` with NO `clone` — the deliberate divergence from
                // `wait_ready` below, whose payload is a `String`. This one
                // is `Copy`.
                Ok(v) => Ok((*v).expect("the `is_some` predicate just held")),
                // The sender is gone (the read task died without
                // publishing — treat as a timeout, the `wait_ready` shape).
                Err(_) => Err(WorkerError {
                    kind: WorkerErrorKind::ReadyTimeout,
                }),
            },
            r = exited_rx.wait_for(|v| v.is_some()) => {
                // The process exited before the `hello` frame (a crash /
                // an early exit — the negotiation never happened).
                match r {
                    Ok(code) => Err(WorkerError {
                        kind: WorkerErrorKind::Io(format!(
                            "the worker exited before the protocol handshake (code {code:?})"
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

    /// A prompt (the `PromptTurn` type's fields verbatim).
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
        self.send_config_clearing(model, thinking, false, trusted)
    }

    /// A config update that can also CLEAR the thinking level (issue #146).
    /// `clear_thinking` resets the loop to the model's default (no
    /// `reasoning_effort`); it is independent of `thinking`, whose `None`
    /// means "leave the level alone".
    pub fn send_config_clearing(
        &self,
        model: Option<&Model>,
        thinking: Option<&str>,
        clear_thinking: bool,
        trusted: Option<bool>,
    ) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Config {
            model: model.cloned(),
            thinking: thinking.map(str::to_string),
            clear_thinking,
            trusted,
        })
    }

    /// Push the three live-reload desktop-config fields (Fulcrum T3) — the
    /// `config-update` frame, the twin of [`Self::send_config`] for the
    /// APP-level settings rather than the per-session composer controls.
    ///
    /// `None` for any field means "leave that one alone", so a caller that only
    /// knows about one of the three cannot wipe the other two. That is also why
    /// this takes the three values instead of a whole `Settings`: the frame is
    /// additive on purpose, and an older Worker that never heard of a field
    /// ignores it rather than decoding garbage.
    ///
    /// The return is "the frame was written to stdin", NOT "the loop applied
    /// it" — the same contract [`Self::send_compact`] has. The Worker applies
    /// it when its loop is IDLE, so the observable bound is the next prompt
    /// (ADR 0030 Deviation 4's bound, which the per-turn `settings.json` re-read
    /// this replaces could never beat either).
    pub fn send_config_update(
        &self,
        file_policy: Option<crate::policy::FilePolicy>,
        web: Option<crate::web::config::WebSettings>,
        agent_overrides: Option<
            std::collections::HashMap<String, crate::config_types::AgentOverrideFields>,
        >,
    ) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::ConfigUpdate {
            file_policy,
            web,
            agent_overrides,
        })
    }

    /// A turn abort (the current-turn token cancel — the session stays
    /// alive).
    pub fn send_abort(&self) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Abort)
    }

    /// A `/compact` (ADR 0044): the idle-time compaction command. The Worker
    /// forwards it to the loop's control queue (`instructions` = the user's
    /// extra summary instructions, `None` for a bare `/compact`). The return
    /// is "the frame was written", NOT "the transcript shrank" — the outcome
    /// rides the event stream.
    pub fn send_compact(&self, instructions: Option<String>) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Compact { instructions })
    }

    /// The session close (the Worker self-exits 0 — the Supervisor does
    /// NOT close the stdin pipe on `Close`).
    pub fn send_close(&self) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::Close)
    }

    /// A permission response (the REAL `PermissionOutcome` verbatim —
    /// the `Selected { option_id, value }` (the optional `value` is the
    /// option's payload, e.g. `allow-directory`'s `path`) or `Cancelled`;
    /// resolves the `round_trips` oneshot of the request `id`). The outcome
    /// rides its own serde attrs — no wrapper — so a `None` `value` never
    /// reaches the wire.
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

    /// A fetch render response (the `Value` verbatim — the `result`
    /// with no wrapper; resolves the `round_trips` oneshot of the
    /// request `id`).
    pub fn send_fetch_render_response(&self, id: &str, value: &Value) -> Result<(), WorkerError> {
        self.send_inbound_frame(Inbound::FetchRenderResponse {
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

    /// The child's pid (`None` only when the platform gave none at spawn).
    /// The value is the one captured in [`WorkerHandle::spawn`], NOT a live
    /// read of `Child::id()` — that goes `None` once the child has been
    /// reaped, which is the state a crash leaves, and the caller that needs
    /// the pid most is precisely the one looking at a dead child.
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Kill the Worker: on Unix the WHOLE process group SIGKILL (the child is
    /// the group leader via `setpgid(0, 0)` — the `RealSudoRunner` pattern),
    /// then a fallback kill of the direct child; off Unix the direct
    /// `child.kill()`. Best-effort (every failure is discarded — the caller is
    /// a teardown that has nothing to do with the outcome).
    ///
    /// The group kill uses the **latched** [`Self::pid`] — captured at spawn
    /// and never re-read — and NOT `Child::id()`: that goes `None` the moment
    /// the exit status is collected, which is exactly the state a crash leaves
    /// (the read task's `wait()` has reaped the child by the time a crashed
    /// Worker is reaped). A live read there silently SKIPPED the group kill on
    /// the one path the group kill exists for, leaving the Worker's own
    /// children alive; the latched value fires there, so the WHOLE-group claim
    /// this comment makes is the behaviour the code delivers. The direct-child
    /// fallback is still a no-op on an already-reaped child — that is the
    /// belt-and-braces half, not the group half.
    ///
    /// The syscall itself lives in
    /// [`crate::childproc::kill_process_group`] (one owner for the
    /// process-group kill — ADR 0080's amendment).
    pub async fn kill(&self) -> Result<(), WorkerError> {
        let mut guard = self.child.lock().await;
        #[cfg(unix)]
        {
            crate::childproc::kill_process_group(self.pid, &mut guard).await;
        }
        #[cfg(not(unix))]
        {
            let _ = guard.kill().await;
        }
        Ok(())
    }
}

/// (issue #122) The store-row belt, PARSE-FREE: an `assistant` row whose
/// serialized `content` is the upstream sanitisation placeholder is rewritten to
/// the canonical empty-`Text` row content (`"content":""`) before it reaches the
/// Supervisor's SQLite. A placeholder row that gets persisted is a row a Resume
/// re-reads and re-sends to the model, which is how the banner survives a restart.
///
/// Why a byte match on the serialized row is exact rather than sloppy:
///   * it matches the WHOLE value region `"content":"<placeholder>"` — a row
///     whose content merely CONTAINS the banner (a real answer quoting it) does
///     not match, because the closing `"` of the needle must follow the `]`
///     immediately, and because a leading `"content":"[` cannot land mid-string;
///   * the placeholder needs no JSON escaping (letters, spaces, `[`, `]`, `:`),
///     so `serde_json::escape` is the identity here and is used anyway to keep
///     that true if the literal ever changes;
///   * serde's field order for `ChatMessage` is declaration order and stable for
///     this type (`role`, `content`, then the two `skip_serializing_if` fields),
///     and `content` is always written (never skipped).
///
/// Deliberately narrow, and that narrowness is the point (ADR 0054: durable rows
/// are never re-labelled for a wire's sake):
///   * ONLY the placeholder literal is clamped — an `assistant` row whose content
///     is an empty string is NOT touched here. That shape is either today's
///     legitimate tool-call row (whose fix is the outbound `request_body` guard)
///     or the empty-turn case `handle_empty_turn` already retries without
///     persisting; rewriting it would be editing durable state for a wire's sake.
///   * non-`assistant` rows are untouched (a `tool` row's content pairs with its
///     `tool_call_id`).
///   * the `TranscriptReplace` (compaction) arm is NOT clamped in this task —
///     recorded as follow-up F3.
fn clamp_placeholder_row(role: &str, content_json: String) -> String {
    if role != "assistant" {
        return content_json;
    }
    // `Value::String(..).to_string()` IS serde_json's escaper (the crate exposes
    // no free `escape`), and it emits the surrounding quotes — so the needle is
    // the byte-exact `"content":"…"` region as `persist_transcript_message`
    // writes it. The placeholder needs no escaping today (letters, spaces, `[`,
    // `]`, `:`); going through the serializer keeps that true if the literal
    // ever changes.
    let needle = format!(
        "\"content\":{}",
        Value::String(EMPTY_CONTENT_SANITISE_PLACEHOLDER.to_string())
    );
    if content_json.contains(&needle) {
        content_json.replace(&needle, "\"content\":\"\"")
    } else {
        content_json
    }
}

/// Route one `Outbound` frame into the `WorkerInboundEvent` vocabulary
/// (the 4 store-frame variants → `Store(StoreFrame::…)`; the control
/// frames verbatim; `Hello` → `None` — it lands on the `protocol_version`
/// watch in the read task instead; `Unknown` → `None` — an unknown
/// `type` is ignored, never an error, so a newer Worker cannot wedge an
/// older Supervisor).
fn route_outbound(frame: Outbound) -> Option<WorkerInboundEvent> {
    Some(match frame {
        // The version-negotiation opener is NOT routed as a
        // `WorkerInboundEvent` — it lands on the `protocol_version`
        // watch in the read task instead (adding a 12th variant here
        // would force every `WorkerInboundEvent` consumer, including
        // `session/router.rs`'s exhaustive `route_event`, to account for
        // a frame the UI never sees).
        Outbound::Hello { .. } => return None,
        Outbound::Ready { version, .. } => WorkerInboundEvent::Ready { version },
        Outbound::Event { event } => WorkerInboundEvent::Event(event),
        Outbound::TranscriptInsert {
            session_id,
            seq,
            role,
            content_json,
        } => {
            // (issue #122) BELT AT THE STORE SEAM. This is the one seam outside
            // the harness that can observe a transport-carrying child's poisoned
            // row, and the durable row is what a Resume re-reads
            // (`core.rs`'s `(Resume, _)` arm -> `load_transcript`) — so a
            // placeholder row persisted here would be re-sent onto the wire and
            // rendered again in the room. Clamp it on the way in.
            //
            // PARSE-FREE on purpose (see `clamp_placeholder_row`): the row is
            // opaque `content_json` here, and the only shape the upstream
            // sanitizer produces is an `assistant` row whose WHOLE content value
            // is the placeholder literal.
            let content_json = clamp_placeholder_row(&role, content_json);
            WorkerInboundEvent::Store(StoreFrame::Insert {
                session_id,
                seq,
                role,
                content_json,
            })
        }
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
            name_resolution,
            launch,
            task,
            model_key,
        } => WorkerInboundEvent::SubagentDispatch(SubagentDispatchWire {
            id,
            parent_session_id,
            parent_cwd,
            parent_enabled_tools,
            agent_name,
            name_resolution,
            launch,
            task,
            model_key,
        }),
        Outbound::SubagentCancel { id } => WorkerInboundEvent::SubagentCancel { id },
        Outbound::FetchRenderRequest { id, payload } => {
            WorkerInboundEvent::FetchRenderRequest { id, payload }
        }
        Outbound::SinkFrame { event, payload } => WorkerInboundEvent::SinkFrame { event, payload },
        Outbound::WorkerError {
            code,
            message,
            backtrace: _,
        } => WorkerInboundEvent::WorkerError { code, message },
        // (issue #180 F1) The task-board call. Routing it DESKTOP-side is D2's
        // half (a branch in `agent/session/router.rs::route_event` that answers
        // with `Inbound::TaskResponse`); the crate's job here is only to carry the
        // variant across the boundary, because `WorkerInboundEvent` is what the
        // desktop's pump consumes.
        //
        // NOTE the deliberate asymmetry with `Outbound::TranscriptInsert`: the
        // store frames land on `WorkerInboundEvent::Store` because the Supervisor
        // ANSWERS them by writing SQLite. This one has no crate-side answer — the
        // crate holds no `TaskStore` (the Supervisor's `Db` is the only impl) — so
        // it is relayed as its own event and the consumer decides. `IpcStore`'s
        // write-only sibling behaviour is NOT mirrored: an older Supervisor never
        // sees this event at all, because its decoder routes `task-request` to
        // `Outbound::Unknown` above and returns `None` (the F1→D2 window, and the
        // reason the Worker's park must race the turn token).
        Outbound::TaskRequest { request_id, call } => {
            WorkerInboundEvent::TaskRequest { request_id, call }
        }
        // A newer Worker's frame this build predates — ignored, never an
        // error (the permissive-decoder contract; the overall surface is
        // negotiated on the `hello` frame, handled above).
        Outbound::Unknown { .. } => return None,
    })
}

/// The summary's shape — a struct rather than a `json!` literal, for one
/// reason that is easy to undo by accident: `serde_json`'s `Map` in this
/// tree is a `BTreeMap` (`preserve_order` is OFF — the same fact
/// `src/mcp/http.rs`'s determinism note records), so a `json!` object
/// serialises in SORTED key order and would emit
/// `{"bytes":…,"rowKeys":…,"rows":…,"sessionId":…,"type":…}` — `type`
/// LAST, which makes the instruments' `"type":"` prefix regex miss every
/// line. A `derive(Serialize)` struct writes its fields in DECLARATION
/// order, so `type` is first by construction, and every key name is still
/// serde's own (a field name + the `camelCase` rename), not a hand-typed
/// copy in a `format!`.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DisplayUpsertSummary {
    /// The frame's own `type` tag — the SAME literal the wire uses (it is
    /// `Outbound::DisplayUpsert`'s serde `rename`, restated here because
    /// this is a log record, not a frame; the unit test pins it).
    #[serde(rename = "type")]
    frame_type: &'static str,
    /// Cut to [`MAX_SUMMARY_KEY_BYTES`] like the row fields — it is a wire-side
    /// string too, and an uncut one would make the line `O(session_id)`.
    session_id: String,
    /// The frame's TRUE row count — never the (possibly capped) length of
    /// `rowKeys`. `MAX_SUMMARY_ROW_KEYS` caps what is LISTED, not what is counted.
    rows: usize,
    row_keys: Vec<DisplayRowKey>,
    /// How many row keys the line LEFT OUT because [`MAX_SUMMARY_ROW_KEYS`] was
    /// reached. `None` (and so absent from the JSON — `skip_serializing_if`) for
    /// every ordinary frame, so the shape both measurement instruments read is
    /// unchanged in the one-row case.
    ///
    /// It exists so a SHORT `rowKeys` array beside a LARGE `rows` can never be
    /// misread as "the frame had few rows": `rows` stays the frame's true count and
    /// this states the difference, so the line is honest about its own truncation.
    #[serde(skip_serializing_if = "Option::is_none")]
    row_keys_omitted: Option<usize>,
    bytes: usize,
}

/// How many row keys a summary line LISTS before it reports the rest as a count.
///
/// A SIZE bound, not an aesthetic one. This line is the one inbound frame that does
/// not go through `debuglog::log_truncated` (see [`summarise_display_upsert`] for
/// why cutting it would be worse than leaving it alone), so the cap has to live
/// HERE or the line is `O(rows)` — measured, 600 rows at a realistic key length is
/// **60 082 bytes** on a file whose every other line is cut at 2 048. With this cap
/// and [`MAX_SUMMARY_KEY_BYTES`] the line is `O(1)` in rows and stays inside the
/// SAME 2 KB budget its siblings get, so the frame is exempt from the truncation
/// MECHANISM and not from the size bound.
///
/// A frame with more rows than this is a `message_retracted` (one row per erased
/// thought segment); the ordinary frame carries exactly ONE row, so the cap never
/// moves in the case the two measurement instruments were written for.
const MAX_SUMMARY_ROW_KEYS: usize = 12;

/// The byte budget for each listed `kind` / `message_key` and for `sessionId`, cut
/// at a char boundary with the same `…` marker `debuglog::truncate_line` uses (see
/// [`cut_field`], which reserves room for the marker so the result never exceeds
/// this).
///
/// Why the fields and not just the count: all three are strings that cross the
/// process boundary, so a row cap alone leaves the line `O(longest string)` — a
/// Worker that sent a 1 MB `message_key` would still write a 1 MB line, which is the
/// asymmetry this closes. A PER-FIELD cut also makes the total arithmetic provable
/// ([`MAX_SUMMARY_LINE_BYTES`]), and unlike a whole-line cut it cannot break the
/// JSON: the quotes and the array closers all survive, so the line stays parseable —
/// the property `log_truncated` would have destroyed. The three are ids
/// (`{incarnation}:{id}`, a session id) and a handful of `kind` literals, so 48
/// bytes never bites a real frame.
const MAX_SUMMARY_KEY_BYTES: usize = 48;

/// The ceiling for the whole summary line — deliberately `debuglog::MAX_LINE_BYTES`,
/// the SAME number every other inbound frame is cut at, so this frame type is not
/// privileged in size, only in mechanism.
///
/// Why not just call `log_truncated` then? Because it cuts BYTES and appends `…`, so
/// on this line it lands mid-JSON: the `rowKeys` array loses its closer, the line
/// stops parsing, and `rowKeys` is exactly what the cut removes — 76.5 % of the
/// verbatim `display-upsert` lines this summary replaced were already cut mid-JSON
/// and unparseable, which is the measurement that prompted the summary. So the line
/// is bounded BY CONSTRUCTION and never cut: the guarantee is "≤ 2 KB **and** always
/// parseable", where `log_truncated` can only ever give "≤ 2 KB **or** parseable".
///
/// The arithmetic, with each of the three strings at [`MAX_SUMMARY_KEY_BYTES`]:
/// one entry's overhead is `{"kind":"","message_key":""}` = 28 B, so a row costs
/// ≤ 28 + 2×48 + 1 (the `,`) = 125 B, and [`MAX_SUMMARY_ROW_KEYS`] of them cost
/// ≤ 1 500 B. The fixed part — the `type` tag, `sessionId`, `rows`, `bytes`,
/// `rowKeysOmitted`, the brackets and braces — is ≤ 200 B at any plausible integer
/// width (every number is a `usize`, and even 20-digit values are counted). Under
/// 2 048 with headroom, and the unit test
/// `a_large_row_set_is_capped_and_the_summary_stays_valid_json` asserts the real
/// serialisation against it at 600 rows.
const MAX_SUMMARY_LINE_BYTES: usize = crate::debuglog::MAX_LINE_BYTES;

/// One row's identity (the SQLite coalesce triple's two discriminators).
#[derive(serde::Serialize)]
struct DisplayRowKey {
    kind: String,
    message_key: String,
}

/// The short log line for a `display-upsert` frame: the frame's identity and size,
/// never its payload. `bytes` is the PRE-truncation wire length — once the body is
/// gone this is the only surviving record of the true frame size.
///
/// Why a summary at all (measured, `docs/research/`): **75.30 %** of a
/// rotated `supervisor.log` is `display-upsert` frames — one accumulating
/// display row re-serialised IN FULL on every streamed token (one
/// `message_key` re-emitted 1 388× = 2 691 601 B = 51.3 % of a whole 5 MB
/// file), and 76.5 % of those lines were already cut mid-JSON by the 2 KB
/// cap, so they were unparseable anyway. Only the LOG pays: SQLite
/// coalesces by `(session_id, kind, message_key)` and the renderer never
/// sees a `StoreFrame::Display`, so this is a logging problem, not a
/// protocol one — the wire and the DB path are untouched.
///
/// The body is JSON with `type` as the FIRST key, not prose: the two
/// measurement instruments in that document regex on `"type":"` and on
/// `"message_key":"…"`, and prose would make both silently report 0.
/// `payload_json` and `created_at` are never read — the payload is the
/// quadratic body this removes, and the deltas that produced it are logged
/// verbatim in the same file, so the row content stays recoverable.
fn summarise_display_upsert(session_id: &str, rows: &[DisplayRow], bytes: usize) -> String {
    // The two caps, and WHY they are here rather than a `log_truncated` at the call
    // site — the asymmetry the call-site comment refers to:
    //
    // The summary is `O(rows)`; every other inbound frame is `O(text)` and gets cut
    // at `MAX_LINE_BYTES`. Cutting THIS line at 2 KB is the worse option, because
    // `truncate_line` cuts BYTES and appends `…`: the `rowKeys` array loses its
    // closer, the line stops parsing, and `rowKeys` — the whole point of the
    // summary — is what the cut removes. So the line is bounded by CONSTRUCTION
    // instead (row count × per-field bytes), it is never cut, and it always parses.
    // The resulting ceiling is [`MAX_SUMMARY_LINE_BYTES`]; the unit test asserts a
    // 600-row frame lands under it.
    //
    // `rows` stays `rows.len()` — the frame's TRUE count — and the number left out
    // is carried in its own field, so capping what is LISTED never silently becomes
    // "the frame had fewer rows".
    let omitted = rows.len().saturating_sub(MAX_SUMMARY_ROW_KEYS);
    // `expect`: every field is a `str` / number / `Vec` of owned strings —
    // there is no value here that a serialiser can fail on.
    let line = serde_json::to_string(&DisplayUpsertSummary {
        frame_type: "display-upsert",
        session_id: cut_field(session_id),
        rows: rows.len(),
        row_keys: rows
            .iter()
            .take(MAX_SUMMARY_ROW_KEYS)
            .map(|r| DisplayRowKey {
                kind: cut_field(&r.kind),
                message_key: cut_field(&r.message_key),
            })
            .collect(),
        row_keys_omitted: (omitted > 0).then_some(omitted),
        bytes,
    })
    .expect("the summary holds only strings, numbers and an array of strings");
    // The ceiling is an INVARIANT, not a comment: the two caps above are what the
    // `debuglog::log` call site is exempt BECAUSE of, so if the arithmetic ever
    // stops holding the exemption has to be revisited — and a debug build / the
    // test suite is where that must surface, not in a 5 MB rotated log.
    debug_assert!(
        line.len() <= MAX_SUMMARY_LINE_BYTES,
        "the summary is exempt from `log_truncated` only while it is bounded by \
         construction: {} bytes, over the {MAX_SUMMARY_LINE_BYTES}-byte ceiling \
         every other inbound frame is cut at",
        line.len(),
    );
    line
}

/// Cut a wire string to [`MAX_SUMMARY_KEY_BYTES`] IN TOTAL, at a char boundary,
/// appending the same `…` marker `debuglog::truncate_line` uses (so a cut field
/// looks like a cut field everywhere else in this file). The marker's own 3 bytes
/// are reserved out of the budget, so the result never exceeds the constant — that
/// is what makes [`MAX_SUMMARY_LINE_BYTES`] arithmetic rather than optimism.
///
/// Only the listed identity fields and `sessionId` go through this, and they are cut
/// BEFORE serialisation, so a cut can never split the JSON the way a whole-line cut
/// would.
fn cut_field(s: &str) -> String {
    if s.len() <= MAX_SUMMARY_KEY_BYTES {
        return s.to_string();
    }
    // 3 bytes is `…` in UTF-8; the budget is ≥ 4 everywhere it is used, so the
    // subtraction below cannot underflow.
    const MARKER: &str = "…";
    let mut end = MAX_SUMMARY_KEY_BYTES - MARKER.len();
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut cut = s[..end].to_string();
    cut.push_str(MARKER);
    cut
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::protocol::PROTOCOL_VERSION;

    // ── the `display-upsert` log summary ───────────────────────────────
    //
    // Why these exist: 75.30 % of a rotated `supervisor.log` is
    // `display-upsert` frames — ONE accumulating display row re-serialised
    // IN FULL on every streamed token (measured: one `message_key`
    // re-emitted 1 388× = 2 691 601 B = 51.3 % of a whole 5 MB file). The
    // log is the ONLY thing that pays (SQLite coalesces by
    // `(session_id, kind, message_key)` and the renderer never sees a
    // `StoreFrame::Display`), so the fix is in the log line, not the wire.
    // The assertions below are the contract the two measurement
    // instruments in
    // `docs/research/supervisor-log-quadratic-display-upsert-reemission.md`
    // regex on — `"type":"` and `"message_key":"…"` — which is why the
    // body is JSON with `type` FIRST rather than prose (prose would make
    // them silently report 0).

    /// A display row carrying a payload that MUST NOT survive into the log.
    fn row_with_payload(kind: &str, message_key: &str) -> DisplayRow {
        DisplayRow {
            kind: kind.to_string(),
            message_key: message_key.to_string(),
            payload_json: serde_json::json!({ "text": "SECRET-PAYLOAD-MARKER" }),
            created_at: 1,
        }
    }

    /// (a)+(b) `type` is the FIRST key (the instruments' regex) and every
    /// row's `message_key` is present in the `"message_key":"…"` form.
    #[test]
    fn summarise_display_upsert_leads_with_type_and_names_every_row_key() {
        let rows = vec![
            row_with_payload("agent-text", "inc1:m1"),
            row_with_payload("tool-call", "inc1:tc7"),
        ];
        let out = summarise_display_upsert("sess-9", &rows, 4096);
        assert!(
            out.starts_with(r#"{"type":"display-upsert""#),
            "`type` must be the first key or the instruments' `\"type\":\"` \
             regex misses the line:\n{out}"
        );
        assert!(
            out.contains(r#""sessionId":"sess-9""#),
            "the session id:\n{out}"
        );
        assert!(out.contains(r#""rows":2"#), "the row count:\n{out}");
        for key in ["inc1:m1", "inc1:tc7"] {
            assert!(
                out.contains(&format!(r#""message_key":"{key}""#)),
                "the row key {key:?} must survive in the instrument's exact \
                 form:\n{out}"
            );
        }
        // The `kind` rides beside it (the SQLite coalesce key triple).
        assert!(out.contains(r#""kind":"agent-text""#), "\n{out}");
        // And the whole line IS the documented shape — parseable JSON.
        let parsed: Value = serde_json::from_str(&out).expect("the summary is valid JSON");
        assert_eq!(parsed["type"], "display-upsert");
        assert_eq!(parsed["rows"], 2);
    }

    /// (c) `bytes` is the PRE-truncation wire length — once the body is
    /// gone this is the only surviving record of the true frame size.
    #[test]
    fn summarise_display_upsert_reports_the_wire_length_it_was_handed() {
        let rows = vec![row_with_payload("agent-text", "inc1:m1")];
        // A number larger than `MAX_LINE_BYTES`: the point of the field is
        // that it survives the 2 KB cap the body used to hit.
        let out = summarise_display_upsert("s", &rows, 2_691_601);
        assert!(
            out.contains(r#""bytes":2691601"#),
            "`bytes` must be the input length verbatim:\n{out}"
        );
        // And it is a NUMBER, not a string (the instruments read it as one).
        let parsed: Value = serde_json::from_str(&out).expect("valid JSON");
        assert_eq!(parsed["bytes"], 2_691_601);
    }

    /// (d) The payload NEVER reaches the log — that body is the 75 % of
    /// the file this change exists to remove.
    #[test]
    fn summarise_display_upsert_never_carries_a_row_payload() {
        let rows = vec![
            row_with_payload("agent-text", "inc1:m1"),
            row_with_payload("agent-text", "inc1:m2"),
        ];
        let out = summarise_display_upsert("s", &rows, 10);
        assert!(
            !out.contains("SECRET-PAYLOAD-MARKER"),
            "the row payload is the quadratic body; it must not be in the log:\n{out}"
        );
        assert!(
            !out.contains("payload"),
            "no payload field at all (identity + size only):\n{out}"
        );
        // `created_at` is likewise never read.
        assert!(!out.contains("created_at"), "\n{out}");
    }

    /// (e) An empty `rows` slice is still a valid, self-describing line.
    #[test]
    fn summarise_display_upsert_on_no_rows_is_valid_json_reporting_zero_rows() {
        let out = summarise_display_upsert("s", &[], 312);
        let parsed: Value = serde_json::from_str(&out)
            .unwrap_or_else(|e| panic!("an empty summary must still parse: {e}\n{out}"));
        assert_eq!(parsed["type"], "display-upsert");
        assert_eq!(parsed["rows"], 0);
        assert_eq!(parsed["bytes"], 312);
        assert!(out.contains(r#""rows":0"#), "in the raw text too:\n{out}");
        assert!(out.starts_with(r#"{"type":"display-upsert""#), "\n{out}");
    }

    /// Arms the process-global `debuglog` at a fresh scratch dir for ONE test and
    /// disables it again on the way out — the `DisableDiagnosticsOnExit`
    /// (`provider/mod.rs`) / `ArmedDiagnostics` (the three provider files) shape,
    /// applied to `debuglog`'s `ENABLED`/`DIR` pair.
    ///
    /// The trailing-statement shape it replaces does not survive a failure: an
    /// assertion that panics unwinds past a final `init(false, None)`, the global
    /// stays armed at THIS test's scratch dir, `ScratchDir::Drop` removes that dir,
    /// and every later `debuglog` write anywhere calls `create_dir_all` on the
    /// removed path — resurrecting a deleted directory in `$TMPDIR` and writing
    /// into it for the rest of the run, never cleaned, while a sibling that armed
    /// nothing reads lines it did not write. `Drop` runs on the unwind path, so the
    /// clear happens whether the test passes or fails.
    ///
    /// The ORDER inside `drop` is the load-bearing part, and folding the dir into
    /// the guard is what makes it unbreakable: the global is cleared BEFORE the
    /// directory it names is removed. As two separate locals, reverse-drop order can
    /// remove the dir first and reopen the exact window this closes.
    struct ArmedDebugLog {
        dir: crate::test_support::ScratchDir,
    }
    impl ArmedDebugLog {
        fn new(prefix: &str, tag: &str) -> Self {
            let dir = crate::test_support::ScratchDir::in_env_temp(prefix, tag);
            debuglog::init(true, Some(dir.to_path_buf()));
            Self { dir }
        }
    }
    impl Drop for ArmedDebugLog {
        fn drop(&mut self) {
            debuglog::init(false, None);
            // Dropping `self.dir` here (after the clear) removes the scratch dir.
        }
    }
    impl std::ops::Deref for ArmedDebugLog {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            self.dir.path()
        }
    }

    /// (f) THE ROW LIST IS BOUNDED — and the bound keeps the line PARSEABLE.
    ///
    /// Why this exists: the summary goes out through `debuglog::log`, NOT
    /// `log_truncated`, so unlike every other inbound frame it is not cut at
    /// `MAX_LINE_BYTES`. That is a deliberate exemption, not an oversight —
    /// `truncate_line` cuts BYTES and appends `…`, which for this line means
    /// mid-JSON: the array loses its closer, the whole line stops parsing, and the
    /// `rowKeys` it exists to carry are among the first things the cut removes (and
    /// 76.5 % of the old verbatim `display-upsert` lines were already unparseable
    /// for exactly this reason). Truncating is therefore the WORST of the two
    /// options.
    ///
    /// The exemption is only safe if the line is bounded by something else, and
    /// the summary is `O(rows)` while every other frame is `O(text)`. A
    /// `message_retracted` frame is the shape that makes `rows` large — one row per
    /// erased thought segment — so the ROW LIST is what gets capped here, and the
    /// cap is asserted rather than assumed.
    #[test]
    fn a_large_row_set_is_capped_and_the_summary_stays_valid_json() {
        // Well past the cap, with keys long enough that an uncapped line would
        // blow through `MAX_LINE_BYTES` several times over.
        let key = |i: usize| format!("inc1:segment-{i:04}-{}", "x".repeat(40));
        let rows: Vec<DisplayRow> = (0..600)
            .map(|i| row_with_payload("agent-thought", &key(i)))
            .collect();
        let out = summarise_display_upsert("s-big", &rows, 90_000);

        // 0. The budget IS the one every other inbound frame is cut at — the whole
        //    "exempt in mechanism, not in size" argument rests on this equality.
        assert_eq!(
            MAX_SUMMARY_LINE_BYTES,
            crate::debuglog::MAX_LINE_BYTES,
            "the summary's ceiling must stay the same number `log_truncated` \
             enforces, or the exemption becomes a privilege"
        );

        // 1. BOUNDED: the line is under the cap every other inbound frame obeys,
        //    so the one exempt call site is exempt in MECHANISM only, not in size.
        assert!(
            out.len() <= MAX_SUMMARY_LINE_BYTES,
            "the summary is the one frame that bypasses `log_truncated`, so it \
             must be bounded by the two caps instead — got {} bytes, over the \
             {MAX_SUMMARY_LINE_BYTES}-byte budget the sibling arm's cap gives \
             every other inbound frame",
            out.len()
        );

        // 2. STILL VALID JSON — the property `log_truncated` would have destroyed.
        let parsed: Value = serde_json::from_str(&out)
            .unwrap_or_else(|e| panic!("a capped summary must still parse: {e}"));

        // 3. NO LIE: `rows` stays the frame's TRUE count. Capping what is LISTED
        //    must not silently become "there were fewer rows" — a supporter
        //    reading `rows` has to be able to trust it.
        assert_eq!(parsed["rows"], 600, "`rows` is the frame's real count");
        assert_eq!(parsed["bytes"], 90_000, "`bytes` is the true wire length");

        // 4. The list is capped AND the line says so, so a reader who sees a short
        //    `rowKeys` against a large `rows` knows it was cut and not absent.
        let keys = parsed["rowKeys"].as_array().expect("`rowKeys` is an array");
        assert!(
            keys.len() < 600,
            "the row list must be capped, got {} of 600",
            keys.len()
        );
        assert_eq!(
            keys.len(),
            MAX_SUMMARY_ROW_KEYS,
            "the cap is the named constant, so the number lives in one place"
        );
        assert_eq!(
            parsed["rowKeysOmitted"],
            600 - MAX_SUMMARY_ROW_KEYS as u64,
            "the line states how many keys it left out — `rows` minus the listed \
             count must be recoverable from the line itself"
        );
        // The FIRST keys survive (the frame's own order), so the row a
        // single-row frame is grepped for is never the one dropped — and each is
        // cut at `MAX_SUMMARY_KEY_BYTES` with the `…` marker, which is the SECOND
        // cap doing its job (without it the line would still be `O(longest key)`).
        assert!(
            keys[0]["message_key"]
                .as_str()
                .expect("message_key is a string")
                .starts_with("inc1:segment-0000-"),
            "the frame's first row keeps its key:\n{out}"
        );
        let listed_len = keys[0]["message_key"].as_str().expect("a string").len();
        assert!(
            listed_len <= MAX_SUMMARY_KEY_BYTES,
            "a listed key is cut at the per-field budget, got {listed_len}"
        );
        assert!(
            keys[0]["message_key"]
                .as_str()
                .expect("a string")
                .ends_with('…'),
            "the cut is MARKED, so a supporter can tell a cut key from a short \
             one (an unmarked cut would be a lie about the id): {}",
            keys[0]["message_key"]
        );

        // 5. UNDER the cap nothing is capped and no omission field appears — the
        //    ordinary one-row frame keeps the shape every existing assertion and
        //    both measurement instruments read.
        let small =
            summarise_display_upsert("s-one", &[row_with_payload("agent-text", "inc1:m1")], 400);
        let small: Value = serde_json::from_str(&small).expect("a one-row summary parses");
        assert_eq!(small["rowKeys"].as_array().expect("rowKeys").len(), 1);
        assert_eq!(small["rows"], 1);
        assert!(
            small.get("rowKeysOmitted").is_none(),
            "an uncapped frame must not grow an omission field:\n{small}"
        );
    }

    /// (g) The regression guard for the MOVED log call.
    ///
    /// The log call used to sit ABOVE `decode_outbound`; moving it below is
    /// what makes a summary possible, and the `Err` arm `continue`s — so a
    /// line the decoder rejects would be logged NOWHERE unless the `Err`
    /// arm logs it itself. That line is the single most diagnostic line in
    /// the stream (a wire the two sides disagree about), so this drives one
    /// through the REAL read loop.
    ///
    /// The seam is a child process: the read loop lives inside `spawn`'s
    /// stdout task and has no in-process entry point, so the child is a
    /// two-line shell script rather than the `fake_worker_core` fixture —
    /// `build_command` appends `--worker` to argv and the script ignores
    /// its argv entirely. Unix only (a shebang script is not executable on
    /// Windows); the Windows proof of the same property is the verbatim
    /// `_` arm plus the unit tests above.
    #[cfg(unix)]
    #[allow(clippy::await_holding_lock)]
    // INTENTIONAL: the read task logs through
    // the PROCESS-GLOBAL `debuglog` dir, so `env_lock` must stay held across the
    // `.await` below — released early, a sibling global-state test's
    // `init(false, None)` would silence this test's own line mid-flight (the
    // `provider_switch.rs` / `policy_gate.rs` precedent for the same reason).
    #[tokio::test]
    async fn an_undecodable_inbound_line_is_still_logged_verbatim() {
        // The debug log's `ENABLED`/`DIR` pair is process-global, so this
        // test serialises with `debuglog`'s own global-state tests on the
        // crate's shared lock (they all take it; a sibling's
        // `init(false, None)` mid-test would silence this one).
        let _g = crate::test_support::env_lock();
        // Armed through the guard, so a panicking assertion below still clears the
        // global before the scratch dir it names is removed.
        let dir = ArmedDebugLog::new("client-log-test-", "log");

        let script = dir.join("no-frame.sh");
        // One line, on stdout, that is not JSON at all.
        std::fs::write(
            &script,
            "#!/bin/sh\necho 'NOT-A-FRAME-MARKER this is not json'\n",
        )
        .expect("the script is written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
                .expect("the script is executable");
        }

        let handle = WorkerHandle::spawn(&script).expect("the script spawns");
        // The read loop ends at the child's stdout EOF → `exited` (bounded,
        // so a loop that never reaches the line fails rather than hangs).
        let code = tokio::time::timeout(Duration::from_secs(20), handle.exited())
            .await
            .expect("the child exits and the read loop publishes the code");
        assert_eq!(code, Some(0), "the script exits 0");

        let text = std::fs::read_to_string(dir.join("supervisor.log"))
            .expect("the log file exists once enabled");
        assert!(
            text.contains("← NOT-A-FRAME-MARKER this is not json"),
            "an undecodable inbound line must STILL be logged verbatim (the \
             log call moved below the decode; the `Err` arm `continue`s, so \
             without its own log call this line is lost):\n{text}"
        );
        // No trailing `debuglog::init(false, None)` — `ArmedDebugLog::Drop` clears
        // the global, and it does so on the panic path too.
    }

    /// End-to-end, through a REAL child process: one non-UTF-8 byte on the
    /// Worker's stderr must not stop the lines after it reaching the log —
    /// and the prefix they reach it under must stay BYTE-EXACT.
    ///
    /// Why the unit test above is not enough: it drives `spawn_pipe_reader`
    /// over a `duplex`. This drives the production wiring —
    /// `WorkerHandle::spawn`'s stderr drain (`debuglog::log_truncated
    /// ("worker-stderr:", &line)`) fed by a real process writing real bytes —
    /// which is the only place the two halves of the card meet.
    ///
    /// # Why this is a second log-owning test in THIS binary
    ///
    /// `debuglog`'s `ENABLED`/`DIR` pair is process-global, so the rule this
    /// crate inherits from the desktop's `src-tauri/tests/worker_stderr.rs:11-12`
    /// is that one binary holds that state for one test at a time. This module
    /// already holds one such test
    /// (`an_undecodable_inbound_line_is_still_logged_verbatim`) and both it and
    /// this one serialise on the crate's shared `env_lock()` — the same lock
    /// every `debuglog` global-state test takes — so neither can have its
    /// `init` undone mid-flight by the other. A NEW integration binary was the
    /// alternative; it is not taken because this card's file list is
    /// `client.rs` + `fixtures/worker.rs` and the lock already buys the
    /// serialization a new binary would buy.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    // INTENTIONAL, and the same reason the sibling above carries it: the
    // Worker's stderr drain logs through the PROCESS-GLOBAL `debuglog` dir, so
    // `env_lock` must stay held across every `.await` below — released early, a
    // sibling global-state test's `init(false, None)` would silence this test's
    // own lines mid-flight and it would fail for a reason unrelated to the
    // drain (`provider_switch.rs` / `policy_gate.rs` precedent).
    async fn one_bad_byte_on_the_workers_stderr_does_not_silence_the_lines_after_it() {
        let _g = crate::test_support::env_lock();
        // Armed through the guard (see `ArmedDebugLog`): a failing assertion below
        // must not leave the global aimed at a dir that `Drop` then deletes.
        let dir = ArmedDebugLog::new("client-stderr-log-test-", "log");

        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(15))
            .await
            .expect("the ready frame arrives");
        handle
            .send_start(&test_env("s-badbytes", Some(vec!["bash".to_string()])))
            .expect("the start frame is queued");

        // FIRST the plain `__stderr__` prompt, and the desktop's marker
        // asserted BYTE-EXACT. The desktop's `src-tauri/tests/worker_stderr.rs:87`
        // greps for this exact string; the prefix in `spawn`'s drain is part of
        // that contract and this is the engine-side pin that it stays put.
        handle
            .send_prompt("__stderr__", &[])
            .expect("the prompt encodes");
        let text = await_log_line(dir.join("supervisor.log"), WORKER_STDERR_MARKER).await;
        assert!(
            text.contains(WORKER_STDERR_MARKER),
            "the `worker-stderr:` prefix must stay byte-exact (the desktop's \
             `worker_stderr.rs` marker):\n{text}"
        );

        // THEN the raw-bytes prompt: `ok\n`, an UNDECODABLE line, `after\n`.
        handle
            .send_prompt("__stderr_badbytes__", &[])
            .expect("the prompt encodes");
        let text = await_log_line(dir.join("supervisor.log"), "worker-stderr: after").await;

        // The line BEFORE the bad one arrived (it is what the drain was
        // already proven to do) — so a missing `after` below means the bad
        // line ended the drain, not that the fixture wrote nothing.
        assert!(
            text.contains("worker-stderr: ok"),
            "the line before the bad one must reach the log:\n{text}"
        );
        // THE CARD: the line AFTER it arrives too.
        assert!(
            text.contains("worker-stderr: after"),
            "one non-UTF-8 line must not end the drain — `after` is the \
             diagnostic that was lost, and this is the whole point of the card:\n{text}"
        );

        let _ = handle.send_close();
        let _ = handle.kill().await;
        // The child is killed BEFORE the guard drops (explicitly above), so the
        // stderr drain task is done writing before `ArmedDebugLog::Drop` clears the
        // global — the ordering the trailing `init(false, None)` used to provide by
        // being the last statement, now provided by the guard on the panic path too.
    }

    /// The desktop's byte-exact stderr marker
    /// (`archimedes/src-tauri/tests/worker_stderr.rs:87`). Spelled out ONCE
    /// here so the prefix pin is a single string that cannot drift from the
    /// `log_truncated("worker-stderr:", …)` call in [`WorkerHandle::spawn`]
    /// without this test going red.
    const WORKER_STDERR_MARKER: &str = "worker-stderr: fake worker: a diagnostic on stderr";

    /// Poll the log until `needle` appears in it (or the bound expires — a
    /// missing line is a failure, never a hang). The drain task writes the log
    /// BESIDE this test's task, so it must be polled; the shape is the one
    /// `tests/worker_log_summary.rs` uses.
    async fn await_log_line(log: std::path::PathBuf, needle: &str) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let text = loop {
            let now = std::fs::read_to_string(&log).unwrap_or_default();
            if now.contains(needle) || tokio::time::Instant::now() > deadline {
                break now;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert!(
            text.contains(needle),
            "`{needle}` never reached the log within 20 s:\n{text}"
        );
        text
    }

    // ── (issue #122) the store-row belt ────────────────────────────────

    /// The belt clamps an `assistant` row whose WHOLE content is the upstream
    /// sanitisation placeholder: a row persisted like that is a row a Resume
    /// re-reads (`load_transcript`) and re-sends to the model, so the banner
    /// would survive a restart and be re-rendered in the room.
    #[test]
    fn clamp_placeholder_row_rewrites_an_assistant_placeholder_row() {
        let row = format!(
            r#"{{"role":"assistant","content":"{}"}}"#,
            EMPTY_CONTENT_SANITISE_PLACEHOLDER
        );
        let clamped = clamp_placeholder_row("assistant", row);
        assert_eq!(
            clamped, r#"{"role":"assistant","content":""}"#,
            "the placeholder content becomes the canonical empty-`Text` row, and \
             the row stays valid JSON with its `role` intact"
        );
        // It still parses as a `ChatMessage`, and its content is now empty.
        let parsed: crate::harness::provider::ChatMessage =
            serde_json::from_str(&clamped).expect("the clamped row is a valid row");
        assert_eq!(
            parsed.content,
            crate::harness::provider::MessageContent::Text(String::new()),
            "the clamped row round-trips to an empty `Text`"
        );
    }

    /// The guard rails — the belt is a whole-value match on ONE literal and
    /// nothing else. Every one of these rows must come back BYTE-IDENTICAL:
    /// a real answer (including one that merely starts with the placeholder's
    /// `[`, and one that QUOTES the banner inside a longer line), an empty
    /// `Text` row (deliberately NOT clamped — durable rows are not re-labelled
    /// for a wire's sake, ADR 0054), a row with `tool_calls`, and the non-
    /// `assistant` roles.
    #[test]
    fn clamp_placeholder_row_leaves_everything_else_byte_identical() {
        let placeholder = EMPTY_CONTENT_SANITISE_PLACEHOLDER;
        let rows = [
            // A real answer.
            r#"{"role":"assistant","content":"the file is at /x"}"#.to_string(),
            // A real answer that merely STARTS like the placeholder.
            r#"{"role":"assistant","content":"["}"#.to_string(),
            // A real answer QUOTING the banner inside a longer line — the
            // don't-filter-too-much case (the belt is a whole-value match).
            format!(
                r#"{{"role":"assistant","content":"what does {placeholder} mean?"}}"#
            ),
            // The placeholder with a real sentence BEFORE it: not the whole value.
            format!(r#"{{"role":"assistant","content":"note: {placeholder}"}}"#),
            // An EMPTY `Text` assistant row — deliberately NOT clamped (the
            // legitimate tool-call shape; its wire fix is `request_body`).
            r#"{"role":"assistant","content":""}"#.to_string(),
            // An empty-`Text` assistant row that DOES carry tool calls (the
            // shape the outbound guard omits `content` for).
            r#"{"role":"assistant","content":"","tool_calls":[{"id":"c1","name":"read","arguments":{}}]}"#
                .to_string(),
            // The placeholder on a NON-assistant role: untouched (a `tool` row's
            // content pairs with its `tool_call_id`; a `user` row is what the
            // user typed).
            format!(r#"{{"role":"tool","content":"{placeholder}","tool_call_id":"c1"}}"#),
            format!(r#"{{"role":"user","content":"{placeholder}"}}"#),
            format!(r#"{{"role":"system","content":"{placeholder}"}}"#),
        ];
        for row in rows {
            let role = serde_json::from_str::<serde_json::Value>(&row)
                .expect("the canned row is JSON")
                .get("role")
                .and_then(Value::as_str)
                .expect("a role")
                .to_string();
            assert_eq!(
                clamp_placeholder_row(&role, row.clone()),
                row,
                "a row the belt must not touch came back changed"
            );
        }
    }

    /// Every line of a piped stream reaches the sink WHOLE and IN ORDER,
    /// however the writer chunks it (the same partial-delivery hazard that
    /// made the Worker's `read_line` lose an IPC frame — here the reader is
    /// never cancelled, so nothing is lost), and the task ends at EOF.
    #[tokio::test]
    async fn every_line_of_a_piped_stream_reaches_the_sink_whole_and_in_order() {
        let (mut tx, rx) = tokio::io::duplex(64);
        let got: std::sync::Arc<Mutex<Vec<String>>> = std::sync::Arc::new(Mutex::new(Vec::new()));
        let g = got.clone();
        let drain = spawn_pipe_reader(rx, move |l| g.lock().unwrap().push(l));

        // Chunked writes: two whole lines split across three writes, then a
        // final line with NO trailing newline (the EOF tail).
        tx.write_all(b"first ").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        tx.write_all(b"line\nsecond\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        tx.write_all(b"tail without a newline").await.unwrap();
        drop(tx); // EOF.

        // The task ends at EOF.
        tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("the drain task ends at EOF")
            .expect("the drain task does not panic");
        assert_eq!(
            &*got.lock().unwrap(),
            &[
                "first line".to_string(),
                "second".to_string(),
                "tail without a newline".to_string()
            ],
            "every line arrived whole, in order, including the EOF tail"
        );
    }

    /// One non-UTF-8 line must NOT end the drain — the lines after it are the
    /// diagnostics the whole function exists to deliver.
    ///
    /// Why this is the card's real bug: `tokio::io::AsyncBufReadExt::read_line`
    /// returns `io::ErrorKind::InvalidData` when the bytes it read are not
    /// valid UTF-8, and this drain used to answer EVERY error with `break`. A
    /// Worker that wrote one stray byte (a raw protocol dump, a `println!` of
    /// a `&[u8]`, a truncated multi-byte char) was therefore silenced for the
    /// rest of its life, and the silencing had no diagnostic of its own.
    ///
    /// # Why `continue` cannot spin
    ///
    /// `read_line` delegates to `read_until_internal` and validates the UTF-8
    /// ONLY AFTERWARDS (`tokio-1.54.1/src/io/util/read_line.rs:99-100`), and
    /// `read_until_internal` calls `reader.as_mut().consume(used)`
    /// (`read_until.rs:69`) — the delimiter-terminated bytes leave the reader
    /// BEFORE validation runs. And on the error they are not restored
    /// ANYWHERE they could be re-read: `finish_string_read`'s `InvalidData`
    /// arm (`read_line.rs:77-83`) truncates them off the caller's buffer
    /// (`read_line.rs:46-50`). So the bad line's bytes are gone from the
    /// stream either way and the next `read_line` starts at the NEXT line: no
    /// busy loop.
    ///
    /// This test is that proof, empirically: the drain must reach EOF and END.
    /// A spinning `continue` never reaches EOF, so the `timeout` below — which
    /// bounds the whole task, not one read — is the no-spin assertion.
    #[tokio::test]
    async fn a_non_utf8_line_does_not_end_the_drain_the_lines_after_it_still_arrive() {
        let (mut tx, rx) = tokio::io::duplex(64);
        let got: std::sync::Arc<Mutex<Vec<String>>> = std::sync::Arc::new(Mutex::new(Vec::new()));
        let g = got.clone();
        let drain = spawn_pipe_reader(rx, move |l| g.lock().unwrap().push(l));

        // One whole BAD line between two good ones. The bad bytes must form a
        // COMPLETE line (`\n`-terminated) before EOF: `read_until_internal`
        // only returns once it has seen the delimiter, so an unterminated run
        // of bad bytes at EOF is a different path (the `Ok(0)` tail arm).
        tx.write_all(b"good\n\xff\xfe bad\nafter\n").await.unwrap();
        drop(tx); // EOF.

        // The task ends at EOF — i.e. the bad line did NOT trap it.
        tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect(
                "the drain task ends at EOF (a `continue` that re-reads the \
                 same consumed bytes would spin here forever)",
            )
            .expect("the drain task does not panic");
        assert_eq!(
            &*got.lock().unwrap(),
            &["good".to_string(), "after".to_string()],
            "the undecodable line is dropped, and every line after it still \
             reaches the sink — `after` is the diagnostic this card is about"
        );
    }

    /// A pipe whose first read fails with a GENUINE I/O error ends the drain.
    ///
    /// The counterweight to the test above, and the reason the `Err(_) =>
    /// break` arm must stay: `InvalidData` is tolerated because it is a
    /// CONTENT problem in one line, every other error is the PIPE itself
    /// (`BrokenPipe`, `ConnectionReset`, `UnexpectedEof`) and a dead pipe has
    /// nothing left to say. Were that arm widened to `continue`, this test
    /// hangs — the `timeout` is the assertion, and it is the same shape the
    /// sibling test uses so both arms are proven by the same instrument.
    #[tokio::test]
    async fn a_genuine_io_error_ends_the_drain() {
        let got: std::sync::Arc<Mutex<Vec<String>>> = std::sync::Arc::new(Mutex::new(Vec::new()));
        let g = got.clone();
        let drain = spawn_pipe_reader(FailingPipe(std::io::ErrorKind::ConnectionReset), move |l| {
            g.lock().unwrap().push(l)
        });

        tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("a real I/O error ends the drain (it must not retry)")
            .expect("the drain task does not panic");
        assert!(
            got.lock().unwrap().is_empty(),
            "a pipe that errors has delivered nothing"
        );
    }

    /// A pipe whose every read fails with the given error kind — the input for
    /// [`a_genuine_io_error_ends_the_drain`]. A `duplex` whose writer was
    /// dropped gives a clean EOF, not an error, so the `break` arm has no
    /// other reachable input in-process.
    struct FailingPipe(std::io::ErrorKind);

    impl tokio::io::AsyncRead for FailingPipe {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::from(self.0)))
        }
    }

    /// The `fake_worker` fixture — resolved through the crate's ONE
    /// `target/debug` resolver.
    ///
    /// **S8.3 RE-POINTED this, and the old body was not merely ugly, it was
    /// wrong.** It read
    /// `PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().join("target/debug/fake_worker")`,
    /// which was correct while this file lived in `src-tauri/` — one level under
    /// the repo root, so `.parent()` WAS the workspace root that owns `target/`.
    /// This crate sits TWO levels under the root, so the same expression names
    /// `<root>/crates/target/debug/fake_worker`: a directory no build ever writes
    /// to. Every fixture-spawning test in this module (and all 30 in
    /// `manager/tests`) dies on `SpawnFailed(Io(ENOENT))` — measured at this
    /// commit before the re-point, `spawn_and_wait_ready_returns_the_version`
    /// failed with `spawn the worker: No such file or directory (os error 2)`.
    ///
    /// `test_support::target_debug_dir()` walks up TWO levels and is the single
    /// resolver S2 established for exactly this (the same fix `mcp/stdio.rs`,
    /// `mcp/tool.rs` and `mcp/manager.rs` carry). It is `pub(crate) +
    /// #[cfg(test)]`, which reaches this module — both are crate-internal test
    /// code. `childproc::target_debug_dir_is_where_cargo_actually_builds` pins it
    /// against `current_exe()`, so a wrong level count cannot go green there, and
    /// the equality + `current_exe()` asserts below pin THIS helper to it.
    ///
    /// `CARGO_BIN_EXE_fake_worker` is NOT an option here: cargo sets it only for
    /// integration-test targets, never inside a lib's `#[cfg(test)]` (T4's
    /// two-shape rule), which is why the manifest-derived resolver is the only
    /// correct form at this depth.
    ///
    /// S8b.1 repointed the NAME from `fake_worker` to `fake_worker_core`: the
    /// crate now owns its fixture bin (declared in `Cargo.toml`, body in
    /// `crate::fixtures::worker`), and `_core` is what keeps the two packages'
    /// bins from colliding on one output file in the shared `target/debug/`.
    /// Before that, every test in this module spawned a bin owned by the desktop
    /// package — which is why 11 of them failed `ENOENT` the moment the crate was
    /// built alone.
    pub fn fixture_path() -> std::path::PathBuf {
        crate::test_support::fixture_bin("fake_worker_core")
    }

    /// T4's pin, REWRITTEN in S8.3 — the old form was VACUOUS for the exact
    /// failure it existed to catch.
    ///
    /// T4 asserted two things: `p.starts_with(pkg_root.parent())` and
    /// `!p.starts_with(pkg_root.join("target"))`. Both hold for the BROKEN
    /// post-move path: `<root>/crates/target/debug/fake_worker` starts with
    /// `<root>/crates` (this crate's manifest parent) and does not start with
    /// `<root>/crates/fulcrum-core/target`. Measured at this commit with the old
    /// body in place: **both asserts passed** and only the `is_file()` witness
    /// failed — so a pin that trusted them would have printed "verified" while
    /// ~45 fixture-spawning tests hit ENOENT.
    ///
    /// What discriminates now:
    ///   1. EQUALITY against the crate's one resolver (the shape, not a prefix);
    ///   2. the same directory `current_exe()` proves cargo actually builds into
    ///      — the `childproc` discipline, so a wrong LEVEL count fails even on a
    ///      machine where a stray `crates/target/` happened to exist;
    ///   3. `is_file()`, the existence witness (the RED the old body produced).
    #[test]
    fn fixture_path_resolves_to_the_workspace_root_target() {
        let p = fixture_path();
        // The name the platform's cargo actually writes. `is_file()` — unlike
        // `CreateProcess` — does NOT append an exe suffix, so on Windows the
        // suffix-less literal below is a file that does not exist even though
        // the spawn three lines away succeeds; that asymmetry is what reddened
        // this witness on `windows-latest` while Linux and macOS stayed green
        // (their `EXE_SUFFIX` is empty). `test_support::exe_file_name` carries
        // the rule and `the_fixture_binary_name_carries_the_platform_exe_suffix`
        // pins both arms of it on every OS.
        let want_name = format!("fake_worker_core{}", std::env::consts::EXE_SUFFIX);
        assert_eq!(
            p.file_name().and_then(|s| s.to_str()),
            Some(want_name.as_str()),
            "still the crate's own fake_worker fixture (`fake_worker_core` — the \
             `_core` suffix is what keeps it from colliding with the desktop's \
             `fake_worker` bin on one output file), under the name cargo builds it as"
        );
        // (1) The equality half — the ONLY resolver. Spelled against
        //     `target_debug_dir()` and NOT against `fixture_bin`, so it still
        //     catches a `fixture_path` that stopped going through the shared
        //     resolver. SINCE S8b.1 IT IS NOT THE LEVEL-COUNT WITNESS ANY MORE:
        //     the resolver's body is now `current_exe()`-derived too (see
        //     `test_support::target_debug_dir`), so this half agrees with the
        //     body by construction and cannot go red on a level count. Half (2)
        //     — which computes `built_into` from `current_exe()` ITSELF, without
        //     calling the resolver — is what still discriminates.
        assert_eq!(
            p,
            crate::test_support::target_debug_dir().join(&want_name),
            "the fixture must resolve through the crate's ONE resolver, not a \
             manifest-parent guess"
        );
        // (2) The load-bearing half: compare against where cargo BUILT this test
        // binary. A lib test lives in `target/debug/deps/`, so its grandparent is
        // the `target/debug` the fixture must live in. The old manifest-parent
        // body resolved to `<root>/crates/target/debug` and fails HERE, on any
        // machine, with or without a stray directory on disk.
        let exe = std::env::current_exe().expect("a test binary has a path");
        let built_into = exe
            .parent()
            .and_then(|deps| deps.parent())
            .expect("the test binary sits under target/debug/deps");
        assert_eq!(
            p.parent(),
            Some(built_into),
            "the fixture resolves to {p:?}, but cargo built THIS test binary into \
             {built_into:?} — a fixture path that is not a sibling of the built \
             binaries spawns ENOENT (`crates/target/…` is exactly that)"
        );
        // (3) The existence witness (the RED the pre-S8.3 body produced).
        assert!(
            p.is_file(),
            "the fixture must exist (SpawnFailed is the old literal's red): {p:?}"
        );
    }

    /// A test `StartEnv` (a fake model the fixture's `SubagentDispatch`
    /// `model_key` — `fake/m1` — resolves against).
    pub fn test_env(session_id: &str, enabled_tools: Option<Vec<String>>) -> StartEnv {
        use crate::harness::catalog::{Model, ModelCatalog};
        use crate::worker::protocol::StartMode;

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
            // (issue #101) `from_parts`' new trailing arg (test fixture: no block).
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

    /// `spawn` + the `ready` handshake (the version), and the `hello`
    /// handshake (the protocol version) — the two watches coexist, each
    /// carrying its own axis (binary version vs wire version).
    #[tokio::test]
    async fn spawn_and_wait_ready_returns_the_version() {
        let handle = spawn_fixture().expect("the fixture binary exists (cargo test built it)");
        let version = handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("the ready handshake completes");
        assert!(!version.is_empty(), "the ready frame carries the version");
        // The protocol version arrives on its OWN watch, off the `hello`
        // line that precedes `ready` — proving the two handshakes do not
        // disturb each other.
        let protocol_version = handle
            .wait_protocol_version(Duration::from_secs(10))
            .await
            .expect("the hello handshake completes");
        assert_eq!(
            protocol_version, PROTOCOL_VERSION,
            "the fixture's `hello` advertises the current protocol version"
        );
        handle.send_close().expect("close encodes");
        let code = handle.exited().await;
        assert_eq!(code, Some(0), "close → exit 0");
    }

    /// The `hello` frame's `protocolVersion` reaches
    /// `wait_protocol_version` through the client's read task (the frame
    /// is NOT routed as a `WorkerInboundEvent`, so this watch is the only
    /// way it surfaces — a route arm that swallowed it before the watch
    /// write would show up here and nowhere else).
    #[tokio::test]
    async fn wait_protocol_version_reports_the_fixture_hello_version() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        let observed = handle
            .wait_protocol_version(Duration::from_secs(10))
            .await
            .expect("the `hello` frame is observed by the read task");
        assert_eq!(
            observed, PROTOCOL_VERSION,
            "the client reports the version the fixture negotiated"
        );
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

    /// The handle reports the child's pid, and still reports it AFTER the
    /// reap. `Child::id()` goes `None` once tokio has reaped the child (the
    /// `FusedChild::Done` state) — which is exactly what a crash leaves
    /// behind — so the value has to be captured at spawn and carried on the
    /// handle, not read from the child at the moment of interest.
    ///
    /// The reap is reached with `send_close()` + `exited().await`, NOT with
    /// `kill()`: a signal death publishes exit code `None` and `exited()`
    /// waits for `Some`, so `kill()` + `exited()` would hang forever. The
    /// read task publishes the code only AFTER `child.wait()` returns, so
    /// `exited()` returning IS the proof the child has been reaped — without
    /// that step a naive accessor delegating to `Child::id()` would pass this
    /// test.
    #[tokio::test]
    async fn the_handle_reports_the_child_pid_and_keeps_it_after_the_reap() {
        let handle = spawn_fixture().expect("the fixture binary exists (cargo test built it)");
        let pid = handle.pid();
        assert!(pid.is_some(), "a live child has a pid (captured at spawn)");
        assert_ne!(
            pid,
            Some(std::process::id()),
            "the pid is the CHILD's, not our own"
        );
        // The graceful close (the fixture self-exits 0) + the reap.
        handle.send_close().expect("close encodes");
        let _ = handle.exited().await;
        assert!(
            handle.pid().is_some(),
            "the pid survives the reap — `Child::id()` is `None` once the child is reaped, \
             which is the state a crash leaves"
        );
        assert_eq!(
            handle.pid(),
            pid,
            "the value after the reap is the one captured at spawn"
        );
    }

    /// (ADR 0080 amendment, Finding 3 / A5) The method-level contract pin for
    /// `kill()` on an ALREADY-REAPED handle: the pid survives the reap, the
    /// kill is best-effort `Ok`, and it completes (no hang).
    ///
    /// This is the METHOD statement of the fix; the BEHAVIOURAL red — the
    /// negative-pid group kill actually firing on a reaped direct child — lives
    /// in `childproc::kill_process_group_kills_by_the_latched_pid_after_the_direct_child_is_reaped`,
    /// because `kill()` delegates the group kill to that helper and the
    /// `fake_worker` fixture spawns no grandchild (adding one is a file
    /// boundary outside this task's owned set). Do NOT "fix" this test by
    /// dropping the post-reap `kill()` — that call is the whole point: before
    /// the fix `kill()` read `guard.id()` (live) and skipped the group kill
    /// here, while `self.pid` — the value its own doc says is the only one a
    /// crashed Worker's attribution has to work from — sat unused one method
    /// away.
    #[tokio::test]
    async fn a_kill_on_an_already_reaped_handle_is_best_effort_and_uses_the_latched_pid() {
        let handle = spawn_fixture().expect("the fixture binary exists (cargo test built it)");
        let pid = handle.pid();
        assert!(pid.is_some(), "a live child has a pid (captured at spawn)");
        // The graceful close + the reap (the same route the pid-latch pin
        // uses: `kill()` + `exited()` would hang, since a signal death
        // publishes `None` and `exited()` waits for `Some`). `exited()`
        // returning IS the proof the read task's `child.wait()` collected the
        // status — the state `Child::id()` reports as `None`.
        handle.send_close().expect("close encodes");
        let _ = handle.exited().await;
        assert_eq!(
            handle.pid(),
            pid,
            "the latched pid survives the reap — the value `kill()` must signal the \
             group with, since a live `Child::id()` is `None` by now"
        );
        // The kill on the reaped child: best-effort and bounded (no hang).
        let killed = tokio::time::timeout(Duration::from_secs(10), handle.kill())
            .await
            .expect(
                "kill() on an already-reaped handle completes — it never waits on a dead child",
            );
        assert!(killed.is_ok(), "the kill is best-effort, got {killed:?}");
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
                    value: None,
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

    /// A `fetch-render-response` resolves the registered round-trip oneshot.
    #[tokio::test]
    async fn a_fetch_render_response_round_trip_resolves_the_round_trip() {
        let handle = spawn_fixture().expect("the fixture binary exists");
        handle
            .wait_ready(Duration::from_secs(10))
            .await
            .expect("ready");
        let mut rt = handle.register_round_trip("r1");
        let value = serde_json::json!({
            "html": "<html><body>rendered</body></html>",
            "status": 200,
            "finalUrl": "https://example.com"
        });
        handle
            .send_fetch_render_response("r1", &value)
            .expect("fetch-render-response encodes");
        let delivered = rt.try_recv().expect("round trip resolves");
        assert_eq!(delivered["status"], 200);
        handle.send_close().expect("close encodes");
    }

    // ── the AppImage child-env scrub, at the Worker self-spawn ──────────
    //
    // The Agent runtime is a SELF-EXEC of this same binary (`<exe> --worker`,
    // ADR 0022), and inside an AppImage `current_exe()` resolves THROUGH the
    // mount (`/proc/<pid>/exe` → `/tmp/.mount_archim*/usr/bin/archimedes`), so
    // the Worker inherits the GUI process' entire tainted stack — and the
    // Worker is the process that spawns the user's commands, so this is the
    // hop where generation N's pollution reaches generation N+1.
    //
    // ⚠ A seam-built `Command` is NOT the command that spawns: the `Stdio`
    // trio and the `#[cfg(unix)]` `pre_exec` (`setpgid`) block stay in `spawn()`
    // and are NOT asserted here. What this proves is the env/argv POLICY of
    // the builder; the end-to-end spawn under a real mount is field-tested
    // separately.
    //
    // `#[cfg(target_os = "linux")]` for the `hostenv` reason: off Linux `scrub`
    // is DEFINED to return the empty patch, so the same assertions would pass
    // VACUOUSLY on macOS and Windows.

    /// The self-spawn carries the patch's removals and re-sets, and its argv is
    /// untouched.
    ///
    /// The patch is injected through the seam instead of read from
    /// [`for_child`](crate::hostenv::for_child): under `cargo test`
    /// `current_exe()` is `target/debug/deps/<hash>`, the production gate is
    /// closed, and that patch is EMPTY on every host — a test built on it
    /// could not fail. The fabricated host in
    /// [`fixtures`](crate::hostenv::fixtures) is what makes a real patch
    /// flow through this seam.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_worker_self_spawn_gets_a_scrubbed_command() {
        use crate::hostenv::fixtures;
        use std::ffi::{OsStr, OsString};

        let patch = fixtures::tainted_patch();
        // Production passes `std::env::current_exe()`; the seam takes the same
        // `&Path`, so any path serves — nothing is spawned here.
        let cmd = build_command_with(std::path::Path::new("/usr/bin/archimedes"), &patch);

        assert_eq!(
            cmd.as_std().get_program(),
            OsStr::new("/usr/bin/archimedes"),
            "the seam must not disturb the program"
        );
        // `get_args()` EXCLUDES argv0, so the Worker's argv is exactly this.
        let args: Vec<&OsStr> = cmd.as_std().get_args().collect();
        assert_eq!(
            args,
            [OsStr::new("--worker")],
            "the self-spawn flag must still be argv[1]"
        );

        let got = fixtures::configured_env(&cmd);
        assert_eq!(
            got,
            fixtures::reference_env(&patch),
            "the Worker's env must be exactly what the patch says"
        );
        // The same fact key-by-key, so a failure names the key.
        assert_eq!(
            got.get(&OsString::from("LD_LIBRARY_PATH")),
            Some(&Some(OsString::from(fixtures::SCRUBBED_LD_LIBRARY_PATH))),
            "the loader path is re-set to the user's entries"
        );
        assert_eq!(
            got.get(&OsString::from("PATH")),
            Some(&Some(OsString::from(fixtures::USER_BIN))),
            "the Worker's own `PATH` keeps the user's entry (a Worker with no \
             `PATH` would fail to find `git`/`rg` and still exit 0)"
        );
        assert_eq!(
            got.get(&OsString::from("PYTHONHOME")),
            Some(&None),
            "a removal appears as a None-valued entry, NOT as an absent key"
        );
        for (key, value) in &got {
            if let Some(value) = value {
                assert!(
                    !fixtures::lists_a_mount_path(value),
                    "the Worker would hand {key:?} to every command it spawns: {value:?}"
                );
            }
        }
    }
}

//! The `fulcrum serve` daemon: argv, the accept loop, and the client↔session
//! multiplex (T9 Task 5).
//!
//! # What the daemon IS
//!
//! A process supervisor and a transport, nothing else. Every engine capability
//! it needs already exists and is reused rather than re-made:
//!
//! * [`WorkerManager`] is the session-keyed Worker registry — `attach` spawns the
//!   child, waits for `ready`, starts the read-loop pump, sends `start`, and
//!   registers under the session id; crash detection, the **Dead bound**, and
//!   subagent dispatch all live inside it. **There is deliberately no second
//!   registry here.** The only per-session state this file keeps is which socket
//!   wants which session's frames, which is routing, not supervision.
//! * [`TranscriptPersister`] is the sole writer of `fulcrum.db`, fed from the
//!   pump's `on_event` — the same seam the desktop drives.
//! * [`WorkerHandle`] is the only child constructor (the `childproc` census exists
//!   to keep it that way; see `worker_factory.rs`).
//!
//! # The multiplex, and why it is shaped like this
//!
//! `run_worker` reads `stdin` and writes `stdout` — one process, one session. So
//! N sessions are N child processes, and the daemon's job is to carry each
//! child's frames to whichever socket asked for that session. There is **no**
//! "attach to session X on worker Y" frame in the worker protocol
//! (`Inbound::Start` is per-process), so the client↔daemon surface below is the
//! daemon's OWN new surface. Nothing in `fulcrum_core::worker::protocol` changed
//! to build it, and the worker protocol stays byte-compatible.
//!
//! The fan-out is ONE `tokio` broadcast of already-encoded client frames, with
//! every connection running a relay that forwards the frames whose `sessionId`
//! it subscribed to. The alternative — a channel per session, or per
//! session×client pair — is what a session registry grows into, and T10 owns
//! that design (`attach { afterSeq }`, replay off the `frameSeq` the protocol
//! already stamps). One broadcast means one buffer and no lifecycle to reconcile
//! with `WorkerManager`'s; the cost is that a frame is copied to every
//! connection's relay for filtering, which for a local daemon with a handful of
//! clients is free and is stated here rather than discovered later.
//!
//! # The client frames (the daemon's surface)
//!
//! JSONL, same framing as the worker protocol on purpose (`transport`'s module
//! docs give the reason: two codecs means two sets of framing bugs).
//!
//! client → daemon:
//! * `hello` — negotiate; the daemon answers `hello` with `protocolVersion`.
//! * `start` — the `StartEnv` fields verbatim (`sessionId`, `cwd`, `mode`,
//!   `model`, `catalog`, …) plus `"type":"start"`; spawns the session's Worker.
//! * `prompt {sessionId, text, images?}` — one turn.
//! * `close {sessionId}` — the ONLY way a session ends. A socket going away is
//!   NOT one; see the durability note below.
//!
//! daemon → client: `hello`, `start-result`, `prompt-result`, `closed`,
//! `error`, and `event {sessionId, frame, isSubagent}` where `frame` is the
//! worker's outbound frame VERBATIM (including its `frameSeq`, which T10's
//! replay reads — the daemon does not renumber, re-order, or consume it).
//!
//! `abort` / `permission-response` / `interactive-response` are deliberately
//! absent: they are the gate round-trip, and shipping half of it (a card a client
//! can open but never answer, or an abort with no test) is how a "minimum" turns
//! into a liability. They are T10's, and the daemon answers any frame it does not
//! know with an `error` frame naming the type rather than killing the connection.
//!
//! # ⚠ A client disconnect does NOT end a session
//!
//! This is the roadmap's `done-when`, so the rule and its reasoning are here
//! rather than in a commit message: the daemon supervises; the client watches.
//! When a socket drops, the daemon drops its *subscription* and nothing else —
//! the child keeps running, the pump keeps pumping, and the persister keeps
//! writing rows that nobody on that socket will ever see. That is the behaviour
//! a background agent needs (close the laptop lid, the turn finishes) and it is
//! what `tests/serve.rs::a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting`
//! proves both ways: the child's pid is still alive after the drop, AND
//! `native_messages` grows after the drop.
//!
//! The tempting wrong implementation is to treat a session as owned by the
//! connection that started it and `detach` it in the client handler's cleanup —
//! and it looks *reasonable*, because leaking a Worker per disconnected client is
//! a real concern. It is wrong because `detach` SIGKILLs a session that was doing
//! useful work. The leak that concern actually describes is bounded by
//! `close`-when-done and by the daemon's own lifetime (`reap_all` on shutdown),
//! not by the socket. That test's plant experiment is the guard; do not "tidy"
//! the handler's cleanup without re-running it.
//!
//! # What T9 does NOT do
//!
//! No `attach`, no replay, no re-join after a reconnect (T10). No graceful
//! shutdown frame, so `run` never returns and `WorkerManager::reap_all` is not
//! wired (T6's pins and T12's sidecar lifecycle own that; the daemon is killed,
//! and a killed daemon's children exit on their stdin EOF — the Worker's own
//! clean-exit path). No authentication: the socket is the credential, which is
//! why it lives in the user's own data dir.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use fulcrum_core::events::EventSink;
use fulcrum_core::persist::TranscriptPersister;
use fulcrum_core::seams::FulcrumPersistence;
use fulcrum_core::store_rows::SessionInfo;
use fulcrum_core::worker::client::{WorkerHandle, WorkerInboundEvent};
use fulcrum_core::worker::manager::{OnCrash, SinkLookup};
use fulcrum_core::worker::protocol::{StartEnv, PROTOCOL_VERSION};
use fulcrum_core::worker::{WorkerFactory, WorkerManager};

use crate::db::Db;
use crate::transport::{Connection, Endpoint, Listener};

pub use crate::worker_factory::{DaemonWorkerFactory, WorkerExeError};

/// The pump's `on_event` signature, named so the field and the constructor
/// parameter below stay inside clippy's `type_complexity` budget (the engine
/// aliases [`OnCrash`] and [`SinkLookup`] for exactly this reason and leaves this
/// one unaliased because only `WorkerManager::new` spells it out).
type OnEvent = Arc<dyn Fn(String, bool, WorkerInboundEvent) + Send + Sync>;

/// How many undelivered frames the fan-out broadcast holds before it starts
/// dropping for the slowest connection.
///
/// Sized against a burst, not against a session: the ring is shared by every
/// connection, so it has to cover the longest chunk a stalled client can stop
/// while a fast session streams. A dropped frame is reported (`Lagged`) and
/// skipped — recovery is T10's `attach { afterSeq }`, and until then a client
/// that cannot keep up seeing a gap is better than the daemon buffering without
/// bound on a streaming model's behalf.
const FAN_OUT_CAPACITY: usize = 4096;

// ── argv ──────────────────────────────────────────────────────────────────────

/// What the process should do, decided from argv by [`parse_command`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Become a Worker (the engine's stdio loop). Never returns.
    Worker,
    /// Run the daemon.
    Serve(ServeConfig),
    /// Print [`usage`] on stdout, exit 0.
    Help,
    /// Print [`usage`] on stderr and exit non-zero, with the reason.
    Usage(String),
}

/// Everything `serve` needs before it touches the filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeConfig {
    /// Where to bind. `None` here means "the platform default" and is resolved
    /// by [`Listener::bind`]'s caller in `main`, not by the parser — the parser
    /// has no business knowing that a default needs `dirs::data_dir()`.
    pub endpoint: Endpoint,
    /// `fulcrum.db`.
    pub db_path: PathBuf,
    /// `--worker-exe`. `None` = `current_exe()` (see
    /// [`DaemonWorkerFactory::resolve`]).
    pub worker_exe: Option<PathBuf>,
}

impl ServeConfig {
    /// The defaults: the platform's default endpoint (the data-dir socket or
    /// pipe) and `<data_dir>/fulcrum/fulcrum.db`.
    ///
    /// Both live under the data dir for one reason: the socket is the only
    /// credential this daemon has, so it must not be world-writable-adjacent, and
    /// the database must sit next to the file the operator's backup already
    /// covers.
    pub fn defaults() -> Result<Self, ServeError> {
        let endpoint = crate::transport::default_endpoint()
            .map_err(|e| ServeError::Endpoint(e.to_string()))?;
        Ok(Self {
            endpoint,
            db_path: default_db_path()?,
            worker_exe: None,
        })
    }
}

/// `<data_dir>/fulcrum/fulcrum.db` — the sibling of the socket Task 3 puts at
/// `<data_dir>/fulcrum/daemon.sock`.
pub fn default_db_path() -> Result<PathBuf, ServeError> {
    let data_dir = dirs::data_dir().ok_or(ServeError::NoDataDir)?;
    Ok(data_dir.join("fulcrum").join("fulcrum.db"))
}

/// The operator-facing help text. Kept as one `&'static str` so the usage error
/// and `help` print the identical thing (a usage error that omits the flag the
/// user just typo'd is not much of an error).
pub fn usage() -> &'static str {
    r#"fulcrum — the Fulcrum agent daemon

USAGE:
    fulcrum serve [OPTIONS]         run the supervisor + local socket (foreground)
    fulcrum --worker                run as a session worker (spawned by the daemon; not for humans)
    fulcrum help                    this text

SERVE OPTIONS:
    --socket <PATH>       unix socket to bind (default: <data_dir>/fulcrum/daemon.sock)
    --pipe <NAME>         named pipe to bind, without the \\.\pipe\ prefix (Windows)
    --db <PATH>           the database file (default: <data_dir>/fulcrum/fulcrum.db)
    --worker-exe <PATH>   the executable to spawn per session
                          (default: this executable, i.e. `fulcrum --worker`)

A session belongs to the daemon, not to the connection that started it: closing a
client does not stop a running session. Send `close` to end one.
"#
}

/// The one argv parser. Split from `main` so the surface is testable without
/// spawning a process, and so `main` is a `match` with no logic in it.
///
/// The FIRST argument picks the mode; the rest belong to it. `--worker` is
/// matched exactly because that is the literal `WorkerHandle::spawn` passes
/// (`build_command`), and a mode flag that also accepted `--worker=true` would be
/// a second spelling nothing produces.
pub fn parse_command(argv: &[String]) -> Command {
    let Some(first) = argv.first() else {
        return Command::Usage("no command given".to_string());
    };
    match first.as_str() {
        "--worker" => Command::Worker,
        "help" | "--help" | "-h" => Command::Help,
        "serve" => match parse_serve_options(&argv[1..]) {
            Ok(config) => Command::Serve(config),
            Err(reason) => Command::Usage(reason),
        },
        other => Command::Usage(format!("unknown command or flag {other:?}")),
    }
}

/// The `serve` sub-argv. Unknown flags are an error, not ignored — see the
/// module header on why a silent `--worker-exe` typo is the expensive failure.
fn parse_serve_options(argv: &[String]) -> Result<ServeConfig, String> {
    let mut config = ServeConfig::defaults().map_err(|e| e.to_string())?;
    let mut i = 0;
    while i < argv.len() {
        // `value` pulls the next argument, and refuses a flag that ran off the
        // end of argv: `--socket` with no path must not bind `<data_dir>`'s
        // default and let the operator think they moved the socket.
        let value = |flag: &str| -> Result<&String, String> {
            argv.get(i + 1)
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        // `take`d rather than borrowed so the closure cannot hold `&mut i`.
        match argv[i].as_str() {
            "--socket" => {
                let path = value("--socket")?;
                config.endpoint = Endpoint::socket(path.clone());
                i += 1;
            }
            "--pipe" => {
                let name = value("--pipe")?;
                // The prefix is added here so the operator types the name they
                // chose, not the platform's address syntax. `Endpoint::pipe`
                // documents that it wants the FULL form, so this is where the
                // two meet.
                config.endpoint = Endpoint::pipe(format!(r"\\.\pipe\{name}"));
                i += 1;
            }
            "--db" => {
                let path = value("--db")?;
                config.db_path = PathBuf::from(path);
                i += 1;
            }
            "--worker-exe" => {
                let path = value("--worker-exe")?;
                if path.is_empty() {
                    return Err("--worker-exe needs a path (an empty one would \
                     make the daemon spawn the wrong thing)"
                        .to_string());
                }
                config.worker_exe = Some(PathBuf::from(path));
                i += 1;
            }
            other => return Err(format!("unknown serve flag {other:?}")),
        }
        i += 1;
    }
    Ok(config)
}

/// Everything that can stop the daemon before it serves, or end the serve loop.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// The endpoint could not be decided (no data dir for the default socket).
    #[error("cannot decide the endpoint: {0}")]
    Endpoint(String),
    /// No data dir, so neither the default socket nor the default database
    /// location exists. Named separately from [`ServeError::Db`] because the fix
    /// is environmental (`$XDG_DATA_HOME` / `$HOME`), not a corrupt file.
    #[error("no platform data directory available (dirs::data_dir() returned None)")]
    NoDataDir,
    /// `fulcrum.db` would not open (see [`crate::db::Db::open`] — including the
    /// WAL-refused case, which is a filesystem, not a bug).
    #[error("cannot open the database: {0}")]
    Db(String),
    /// The socket / pipe would not bind (a live daemon is already there, or the
    /// unix `sun_path` limit).
    #[error("cannot bind: {0}")]
    Bind(String),
    /// The tokio runtime would not build.
    #[error("cannot build the runtime: {0}")]
    Runtime(String),
    /// The Worker executable could not be decided.
    #[error("cannot decide the worker executable: {0}")]
    WorkerExe(#[from] WorkerExeError),
}

/// Open the daemon's database (the ONE handle; see [`run`]).
pub fn open_db(config: &ServeConfig) -> Result<Db, ServeError> {
    Db::open(&config.db_path).map_err(|e| ServeError::Db(e.to_string()))
}

// ── The daemon ────────────────────────────────────────────────────────────────

/// The supervisor + the fan-out. One per process, `Arc`'d, shared by every client
/// task.
struct Daemon {
    /// THE session registry (the engine's). Every session operation goes through
    /// it; nothing here keeps its own list of live sessions.
    manager: WorkerManager,
    /// The sole writer of `fulcrum.db` (shared with the pump's `on_event`).
    persister: Arc<TranscriptPersister>,
    /// The eager `sessions`-row writer (see [`Daemon::record_session`]).
    db: Arc<Db>,
    /// The multiplex: every client-facing frame, in order, to every connection.
    fan_out: tokio::sync::broadcast::Sender<serde_json::Value>,
}

impl Daemon {
    fn new(db: Arc<Db>, factory: Arc<dyn WorkerFactory>) -> Arc<Self> {
        let (fan_out, _) = tokio::sync::broadcast::channel::<serde_json::Value>(FAN_OUT_CAPACITY);
        let persister = Arc::new(TranscriptPersister::new(
            Arc::clone(&db) as Arc<dyn FulcrumPersistence>
        ));

        // The pump's `on_event` — the ONE place a Worker frame is turned into
        // (a) durable rows and (b) socket traffic. Both halves run for EVERY
        // frame, including `Exited`, because the pump calls it first and
        // unconditionally (`pump::run_pump_loop`), and the desktop's router makes
        // the same split for the same reason: persistence must not depend on
        // someone watching.
        let persist = Arc::clone(&persister);
        let fan = fan_out.clone();
        let on_event: OnEvent = Arc::new(move |session_id, is_subagent, evt| {
            // (a) Durable first: the store frames, and the `interactive-event`
            // sink frames (todos / cost) that the persister owns. Both are
            // synchronous and both are brief — one statement each — which is what
            // makes calling them from the pump task acceptable even though the
            // store is a `std::sync::Mutex` (see `db/mod.rs`: never hold the
            // guard across an await, and nothing here does).
            match &evt {
                WorkerInboundEvent::Store(frame) => persist.apply(is_subagent, frame),
                // The guard is the filter: every OTHER `SinkFrame` event name is
                // the desktop's rendering concern, not a row this store owns.
                WorkerInboundEvent::SinkFrame { event, payload }
                    if event == "interactive-event" =>
                {
                    persist.apply_interactive_event(&session_id, is_subagent, payload);
                }
                _ => {}
            }
            // (b) Then the socket traffic. `Err` means no connection is
            // subscribed, which is the NORMAL state once a client has gone away
            // and the session is still running — the durability case — so it is
            // discarded rather than logged.
            let _ = fan.send(event_frame(&session_id, is_subagent, &evt));
        });

        // The crash report. `WorkerManager` fires this for an UNEXPECTED `Exited`
        // only (a `detach` / `reap_all` in flight makes it expected), so a client
        // that sees this frame is looking at a real death. The pid is the DEAD
        // child's, which is what a crash log's name carries.
        let fan_crash = fan_out.clone();
        let on_crash: OnCrash = Arc::new(move |session_id, code, pid| {
            let _ = fan_crash.send(serde_json::json!({
                "type": "event",
                "sessionId": session_id,
                "isSubagent": false,
                "frame": { "type": "worker-crashed", "exitCode": code, "pid": pid },
            }));
        });

        // The parent-scope sink lookup (the subagent lifecycle frames —
        // `subagent-session-started` / `subagent-closed` / the gate closes — are
        // emitted on the PARENT's sink, so they must reach the sockets watching
        // the PARENT). A sink per lookup is cheap: it publishes into the same
        // broadcast, and `send` failing just means nobody is watching.
        let fan_sink = fan_out.clone();
        let sink_for: SinkLookup = Arc::new(move |session_id| {
            Some(Arc::new(FanOutSink {
                session_id: session_id.to_string(),
                fan_out: fan_sink.clone(),
            }) as Arc<dyn EventSink>)
        });

        let manager = WorkerManager::new(factory, on_crash, on_event, sink_for);
        let daemon = Arc::new(Self {
            manager,
            persister: Arc::clone(&persister),
            db,
            fan_out,
        });

        // A subagent's `sessions` row is created lazily by the persister, and the
        // FK to `sessions(id)` means it needs a `cwd` — which no store frame
        // carries. The engine's late-wire for that is this registrar; the daemon
        // has to hook it up or a dispatched subagent's first transcript row fails
        // the FK and the child's work is silently lost.
        let reg_persister = Arc::clone(&persister);
        daemon
            .manager
            .set_cwd_registrar(Arc::new(move |session_id, cwd| {
                reg_persister.set_cwd(session_id, cwd);
            }));
        daemon
    }

    /// Write the `sessions` row at `start`, before the Worker exists.
    ///
    /// The persister CAN create it lazily (`ensure_session_row` on the first
    /// frame) and does for subagents, so this is not the only thing that keeps
    /// the FK satisfied. It is here because a session that never produces a frame
    /// — a spawn that failed, a turn that asked no questions — would otherwise
    /// have no row at all, and "the daemon ran that session" would be invisible
    /// on disk. It is also the honest place to record the `cwd` the client asked
    /// for: the lazy path writes an empty one for a session whose frames arrive
    /// before anything registered a `cwd`.
    fn record_session(&self, env: &StartEnv) -> Result<(), String> {
        self.db
            .record_session(&SessionInfo {
                session_id: env.session_id.clone(),
                cwd: PathBuf::from(&env.cwd),
                // The daemon has no capability envelope to offer: ACP's
                // `loadSession` etc. are the desktop's negotiation, and inventing
                // `true` here would put a claim about this host in the database.
                // `Value::Null` is what the engine's own lazy path writes.
                capabilities: serde_json::Value::Null,
                config_options: None,
                archived: false,
                title: None,
                context_usage: None,
                cost_usd: None,
                // A client-started session is a MAIN session by definition (a
                // subagent's row comes from `dispatch_subagent`, never here).
                is_subagent: false,
            })
            .map_err(|e| e.to_string())
    }
}

/// The pump's frame, as the client sees it.
///
/// The worker's outbound frame VERBATIM under `frame` — including its `frameSeq`.
/// The daemon neither renumbers nor strips it: T10's replay reads the number the
/// worker wrote, and a daemon that stamped its own sequence would create a second
/// numbering for T10 to reconcile (the exact hazard `transport`'s module docs
/// call out).
fn event_frame(session_id: &str, is_subagent: bool, evt: &WorkerInboundEvent) -> serde_json::Value {
    serde_json::json!({
        "type": "event",
        "sessionId": session_id,
        "isSubagent": is_subagent,
        "frame": worker_frame(evt),
    })
}

/// The `WorkerInboundEvent` → wire frame half of [`event_frame`].
///
/// `Ready` and `Exited` are the two variants with no worker-protocol frame behind
/// them (`route_outbound` consumes both into the handle's watches), so they are
/// given daemon-surface names rather than being dropped: a client that cannot see
/// "the child is up" cannot tell a slow start from a hung one, and a client that
/// cannot see "the child exited" has to infer it from silence.
fn worker_frame(evt: &WorkerInboundEvent) -> serde_json::Value {
    match evt {
        WorkerInboundEvent::Ready { version } => {
            serde_json::json!({ "type": "worker-ready", "version": version })
        }
        WorkerInboundEvent::Exited(code) => {
            serde_json::json!({ "type": "worker-exited", "exitCode": code })
        }
        WorkerInboundEvent::Event(event) => {
            serde_json::json!({ "type": "event", "event": event })
        }
        WorkerInboundEvent::Store(frame) => serde_json::json!({
            "type": "store",
            "frame": store_frame(frame),
        }),
        WorkerInboundEvent::PermissionRequest { id, payload } => serde_json::json!({
            "type": "permission-request", "id": id, "payload": payload,
        }),
        WorkerInboundEvent::InteractiveRequest { id, payload } => serde_json::json!({
            "type": "interactive-request", "id": id, "payload": payload,
        }),
        WorkerInboundEvent::SubagentDispatch(wire) => {
            serde_json::to_value(wire).unwrap_or(serde_json::json!({ "type": "subagent-dispatch" }))
        }
        WorkerInboundEvent::SubagentCancel { id } => {
            serde_json::json!({ "type": "subagent-cancel", "id": id })
        }
        WorkerInboundEvent::FetchRenderRequest { id, payload } => serde_json::json!({
            "type": "fetch-render-request", "id": id, "payload": payload,
        }),
        WorkerInboundEvent::SinkFrame { event, payload } => serde_json::json!({
            "type": "sink-frame", "event": event, "payload": payload,
        }),
        WorkerInboundEvent::WorkerError { code, message } => serde_json::json!({
            "type": "worker-error", "code": code, "message": message,
        }),
    }
}

/// The persistence frames, for the client's view.
///
/// The engine's `StoreFrame` is a Rust enum with no wire impl (it is the
/// `Outbound` frames collapsed into one vocabulary for the persister), so this
/// re-derives the worker-protocol shape the client already recognises from
/// `Outbound`'s names — the same names, the same field names, no new dialect.
fn store_frame(frame: &fulcrum_core::worker_types::StoreFrame) -> serde_json::Value {
    use fulcrum_core::worker_types::StoreFrame;
    match frame {
        StoreFrame::Insert {
            session_id,
            seq,
            role,
            content_json,
        } => serde_json::json!({
            "type": "transcript-insert",
            "sessionId": session_id,
            "seq": seq,
            "role": role,
            "contentJson": content_json,
        }),
        StoreFrame::Replace { session_id, rows } => serde_json::json!({
            "type": "transcript-replace",
            "sessionId": session_id,
            "rows": rows,
        }),
        StoreFrame::Display { session_id, rows } => serde_json::json!({
            "type": "display-upsert",
            "sessionId": session_id,
            "rows": rows,
        }),
        StoreFrame::ContextUsage {
            session_id,
            used,
            window,
        } => serde_json::json!({
            "type": "context-usage",
            "sessionId": session_id,
            "used": used,
            "window": window,
        }),
    }
}

/// The [`EventSink`] `WorkerManager` hands to the subagent-dispatch flow.
///
/// Keyed to the session the lookup was made for (the PARENT, for the subagent
/// lifecycle frames), so the frame it publishes carries that `sessionId` and
/// reaches the relays watching it.
struct FanOutSink {
    session_id: String,
    fan_out: tokio::sync::broadcast::Sender<serde_json::Value>,
}

impl EventSink for FanOutSink {
    fn emit(&self, event: &str, payload: serde_json::Value) {
        // The daemon's own surface for a sink emission is the `sink-frame`
        // spelling the worker protocol uses, so a client sees ONE shape for
        // "something the UI would render" whether it came from the child or from
        // the supervisor's dispatch flow.
        let _ = self.fan_out.send(serde_json::json!({
            "type": "event",
            "sessionId": self.session_id,
            "isSubagent": false,
            "frame": { "type": "sink-frame", "event": event, "payload": payload },
        }));
    }
}

// ── The accept loop ───────────────────────────────────────────────────────────

/// Run the accept loop forever.
///
/// `db` and `factory` are shared by every client task: **the database is opened
/// once, by the caller, before this runs.** A `Db` per client would put N
/// connections on one WAL file for no benefit (WAL's concurrency is about
/// readers, and every client here writes through the same persister) and would
/// make "the sole writer" a lie the moment two clients started a session.
///
/// Never returns. T9 has no shutdown frame; see the module header.
pub async fn run(listener: Listener, db: Arc<Db>, factory: Arc<dyn WorkerFactory>) {
    let daemon = Daemon::new(db, factory);
    loop {
        match listener.accept().await {
            Ok(connection) => {
                let daemon = Arc::clone(&daemon);
                // One task per connection, so a client that stops reading (or
                // stops sending) stalls only itself. Detached on purpose: no one
                // joins these, and a `JoinHandle` nobody polls is how a client
                // task's panic becomes invisible — but so is a leak, hence
                // `log_on_join_error` below.
                let handle = tokio::spawn(serve_client(daemon, connection));
                log_on_join_error(handle);
            }
            // An `accept` failure is a per-connection condition (a descriptor
            // limit, a client that vanished between `poll` and `accept`), not a
            // reason to stop supervising the sessions already running. The loop
            // reports and continues.
            Err(e) => eprintln!("fulcrum serve: accept failed: {e}"),
        }
    }
}

/// Surface a panicked client task instead of letting tokio swallow it.
///
/// The daemon's own panics are survivable by design (`panic = "unwind"` at the
/// workspace root, ADR 0025): one task dying takes one client's view, not the
/// supervisor. That is only true if the operator can find out it happened.
fn log_on_join_error(handle: tokio::task::JoinHandle<()>) {
    tokio::spawn(async move {
        if let Err(e) = handle.await {
            eprintln!("fulcrum serve: a client task ended abnormally: {e}");
        }
    });
}

/// One client: read its frames, answer them, and forward the events it asked for.
///
/// ONE task owns the `Connection`, because `Connection` is not `clone`able and
/// its write half must not be fought over. So the inbound read, the outbound
/// relay, and the replies all meet in one `select!`. Both directions are safe
/// under it: `Connection::recv` awaits an `mpsc` (cancellation-safe), and
/// `send_frame` keeps its unwritten tail in the connection and drains it on the
/// next call, so an abandoned send cannot desync the stream (see `transport::frame`).
async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection) {
    // The frames this connection asked to see. Populated by `start`; a
    // connection that never started anything sees nothing, which is the point of
    // filtering by session rather than broadcasting everything to everyone.
    let mut watching: HashSet<String> = HashSet::new();
    // The outbound queue (replies + relayed events). Unbounded so `send` is
    // infallible from inside the inbound arm; the bound that matters is
    // `FAN_OUT_CAPACITY` upstream, and a client that cannot drain its socket
    // holds the queue open rather than stalling the fan-out for everyone else.
    let (tx, mut queued) = tokio::sync::mpsc::unbounded_channel::<serde_json::Value>();
    let mut events = daemon.fan_out.subscribe();

    // Frames seen from this peer, so the exit path can tell "a probe that
    // connected and hung up" from "a client that talked to us and left".
    let mut seen = 0usize;

    loop {
        tokio::select! {
            // The peer's frames first: a client that sends `close` expects the
            // answer before it expects the next event.
            inbound = connection.recv() => {
                let Some(frame) = inbound else { break };
                seen += 1;
                let frame = match frame {
                    Ok(frame) => frame,
                    // A malformed line is the peer's bug, not our reason to hang
                    // up (`transport::frame` keeps the stream alive for the same
                    // reason the worker protocol's decoder is permissive).
                    Err(e) => {
                        let _ = tx.send(serde_json::json!({
                            "type": "error",
                            "message": format!("malformed frame: {e}"),
                        }));
                        continue;
                    }
                };
                let reply = handle_frame(&daemon, &mut watching, &frame).await;
                if let Some(reply) = reply {
                    if tx.send(reply).is_err() {
                        break; // this task's own queue is gone; nothing to do.
                    }
                }
            }

            // The fan-out. Filtered here, not upstream: one buffer for the whole
            // daemon is what keeps this from becoming a registry.
            fan = events.recv() => match fan {
                Ok(frame) => {
                    if watched(&frame, &watching) && tx.send(frame).is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    // The client is slower than the session. Report it on the
                    // socket (a UI that silently lost 400 tokens of answer is
                    // worse than one that knows) and keep going — recovery is
                    // T10's replay.
                    eprintln!("fulcrum serve: a client lagged {skipped} frames");
                    let _ = tx.send(serde_json::json!({
                        "type": "gap", "skipped": skipped,
                    }));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },

            // The queue, drained to the socket.
            queued_frame = queued.recv() => {
                let Some(frame) = queued_frame else { break };
                if connection.send_frame(&frame).await.is_err() {
                    break; // the peer is gone (EPIPE / a closed socket).
                }
            }
        }
    }

    // ── The connection is gone. ───────────────────────────────────────────────
    //
    // Nothing else happens here. NO `detach`, NO `kill`, NO "cleanup" of the
    // sessions this connection started — dropping the subscription is the whole
    // teardown, and the sessions keep running and keep persisting. That is the
    // roadmap's `done-when` and the reason this comment is next to the code that
    // must not change; `tests/serve.rs`'s plant experiment is what keeps it true.
    //
    // If you are tempted to add a teardown here anyway, note what makes that
    // experiment able to catch you: `detach` does not kill, it runs
    // `reap_worker` (`close`, then a 2 s grace, then `kill`), so a wrong
    // teardown here looks like a child that dies 2 s LATE rather than one that
    // dies at once. `tests/serve.rs::REAP_GRACE` is the observation window that
    // outlasts it, and the test's row-count half is the independent check. Both
    // are load-bearing: a window shorter than the grace makes the pid check
    // decorative, which is exactly how the inherited version of that test
    // managed to pass while the daemon was killing the session.
    //
    // A connection that never sent a frame is not even worth a log line: Task 3's
    // stale-socket probe (`transport::unix::reclaim_stale_socket`) lands exactly
    // such a connection in this `accept()` backlog every time someone accidentally
    // starts a second daemon, and a `curl`-style liveness check does the same.
    // Both EOF with no frame. The loop treats them as ordinary, and the
    // `seen == 0` case is the only one that could tempt someone into logging an
    // error — which would train an operator to ignore the log.
    if seen > 0 {
        eprintln!("fulcrum serve: client disconnected after {seen} frame(s)");
    }
}

/// Does this connection want this frame?
///
/// Every daemon→client frame that is per-session carries `sessionId` at the top
/// level (see [`event_frame`]); the ones that do not (`hello`, the `gap` notice)
/// are addressed to the connection itself and go through unfiltered.
fn watched(frame: &serde_json::Value, watching: &HashSet<String>) -> bool {
    match frame.get("sessionId").and_then(serde_json::Value::as_str) {
        Some(id) => watching.contains(id),
        None => true,
    }
}

/// Handle one client frame; return the reply to write back, if any.
///
/// Unknown `type`s get an `error` frame naming them: permissive about the
/// connection (a newer client may send something this daemon predates), loud
/// about the frame.
async fn handle_frame(
    daemon: &Arc<Daemon>,
    watching: &mut HashSet<String>,
    frame: &serde_json::Value,
) -> Option<serde_json::Value> {
    let kind = frame.get("type").and_then(serde_json::Value::as_str)?;
    match kind {
        "hello" => Some(serde_json::json!({
            "type": "hello",
            // The SAME version constant the worker protocol uses, because the
            // framing and the frame vocabulary are shared; a client that
            // negotiates 1 with the daemon is speaking 1 to the worker.
            "protocolVersion": PROTOCOL_VERSION,
            "serverVersion": env!("CARGO_PKG_VERSION"),
        })),
        "start" => Some(start(daemon, watching, frame).await),
        "prompt" => Some(prompt(daemon, frame)),
        "close" => Some(close(daemon, watching, frame)),
        other => Some(serde_json::json!({
            "type": "error",
            "message": format!("unsupported frame type {other:?} \
             (the daemon speaks hello / start / prompt / close)"),
        })),
    }
}

/// `start`: spawn the session's Worker and register it under its session id.
///
/// `attach` is the whole lifecycle — `factory.spawn()` → `wait_ready` (the
/// engine's own 5 s bound, not exposed and not re-chosen here) → the read-loop
/// pump → `send_start(env)` → register — so this function adds only what the
/// daemon owns: the `cwd` registration the persister needs, the eager `sessions`
/// row, the subscription, and the reply. A `start-result` that took ~5 s to say
/// `ok: false` is that bound expiring, which is why the number is named in
/// `fulcrum_core::worker::manager::WorkerManager::attach` rather than duplicated.
async fn start(
    daemon: &Arc<Daemon>,
    watching: &mut HashSet<String>,
    frame: &serde_json::Value,
) -> serde_json::Value {
    let env = match start_env(frame) {
        Ok(env) => env,
        Err(e) => return start_result(frame, false, None, Some(e)),
    };
    let session_id = env.session_id.clone();
    if session_id.is_empty() {
        return start_result(
            frame,
            false,
            None,
            Some("`start` needs a sessionId".to_string()),
        );
    }
    // The duplicate guard, asked of the registry rather than a local set — a
    // second `attach` for one id would REPLACE the entry and orphan the first
    // child process with no owner, which is the one way to leak a Worker for
    // real. `handle_for` is the engine's production accessor (the desktop's
    // relays route through it), not a test view.
    if daemon.manager.handle_for(&session_id).is_some() {
        return start_result(
            frame,
            false,
            None,
            Some(format!("session {session_id} is already running")),
        );
    }

    // Before the spawn: the `cwd` (the persister's `ensure_session_row` source —
    // the store frames carry no `cwd`, so a frame that arrives before this is a
    // row with an empty path) and the row itself.
    daemon.persister.set_cwd(&session_id, &env.cwd);
    if let Err(e) = daemon.record_session(&env) {
        return start_result(
            frame,
            false,
            None,
            Some(format!("cannot record the session row: {e}")),
        );
    }

    match daemon.manager.attach(&session_id, &env).await {
        Ok(()) => {
            watching.insert(session_id.clone());
            let pid = daemon
                .manager
                .handle_for(&session_id)
                .and_then(|handle| handle.pid());
            start_result(frame, true, pid, None)
        }
        // A failed spawn is a `start-result`, not a closed connection and not a
        // log line only: a bad `--worker-exe` has to reach whoever typed it. The
        // row written above stays (the daemon did try to run that session), and
        // the next `start` for the id re-records it.
        Err(e) => start_result(
            frame,
            false,
            None,
            Some(format!(
                "cannot start session {session_id} (worker exe: is --worker-exe right?): {e}"
            )),
        ),
    }
}

/// The `start` frame's payload IS a [`StartEnv`] (the task's design note: it
/// already derives `Serialize`/`Deserialize` in the camelCase the socket uses, so
/// inventing a second DTO for it would be inventing a second thing to keep in
/// step). `type` is stripped before the deserialize, because `StartEnv` has no
/// such field and an unknown-field-tolerant struct would silently ignore a
/// `type` that is the ONE field the daemon routes on.
fn start_env(frame: &serde_json::Value) -> Result<StartEnv, String> {
    let mut payload = frame
        .as_object()
        .cloned()
        .ok_or_else(|| "`start` must be a JSON object".to_string())?;
    payload.remove("type");
    serde_json::from_value::<StartEnv>(serde_json::Value::Object(payload))
        .map_err(|e| format!("malformed `start` envelope: {e}"))
}

fn start_result(
    frame: &serde_json::Value,
    ok: bool,
    worker_pid: Option<u32>,
    error: Option<String>,
) -> serde_json::Value {
    serde_json::json!({
        "type": "start-result",
        "sessionId": frame.get("sessionId").cloned().unwrap_or(serde_json::Value::Null),
        "ok": ok,
        // The child's pid, on the surface deliberately: it is the key an operator
        // greps `ps` for when a session will not start, and the key a test needs
        // to assert the child SURVIVED its client (the durability claim has to
        // name the process it is talking about, and there is no other way for a
        // client to learn it).
        "workerPid": worker_pid,
        "error": error,
    })
}

/// `prompt`: relay one turn to the session's Worker.
///
/// The reply means DELIVERED, not settled — the turn's outcome arrives as the
/// `event` stream (`agent_settled` and friends). Conflating the two would make
/// every long turn look hung.
fn prompt(daemon: &Arc<Daemon>, frame: &serde_json::Value) -> serde_json::Value {
    let session_id = frame.get("sessionId").and_then(serde_json::Value::as_str);
    let Some(session_id) = session_id else {
        return serde_json::json!({
            "type": "prompt-result", "ok": false,
            "error": "`prompt` needs a sessionId",
        });
    };
    let text = frame.get("text").and_then(serde_json::Value::as_str);
    let Some(text) = text else {
        return serde_json::json!({
            "type": "prompt-result", "sessionId": session_id, "ok": false,
            "error": "`prompt` needs a text",
        });
    };
    // `images` is optional and, like every additive field on this surface,
    // absent-means-empty (`Inbound::Prompt` carries the pair and the worker
    // protocol has no "text only" spelling of a prompt).
    let images = match frame.get("images") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(value) => {
            match serde_json::from_value::<Vec<fulcrum_core::tools::ImageRef>>(value.clone()) {
                Ok(images) => images,
                Err(e) => {
                    return serde_json::json!({
                        "type": "prompt-result", "sessionId": session_id, "ok": false,
                        "error": format!("malformed `images`: {e}"),
                    })
                }
            }
        }
    };

    let result = relay(daemon.manager.handle_for(session_id), |handle| {
        handle.send_prompt(text, &images)
    });
    relay_result("prompt-result", session_id, result)
}

/// `close`: end a session on purpose — the ONLY way a session ends from the
/// socket side.
///
/// `detach` removes the registry entry and runs the graceful `close`, then a
/// 2 s grace, then `kill`, all in the background; and it marks the `Exited`
/// EXPECTED so the crash report does not fire for a shutdown the client asked
/// for.
///
/// (The sentence above is deliberately not spelled "`close` + grace + `kill`":
/// a doc line that begins with `+` is a markdown list item, and
/// `clippy::doc_lazy_continuation` — on under `-D warnings` — then demands the
/// following line be indented as its continuation.)
fn close(
    daemon: &Arc<Daemon>,
    watching: &mut HashSet<String>,
    frame: &serde_json::Value,
) -> serde_json::Value {
    let Some(session_id) = frame.get("sessionId").and_then(serde_json::Value::as_str) else {
        return serde_json::json!({
            "type": "closed", "ok": false,
            "error": "`close` needs a sessionId",
        });
    };
    watching.remove(session_id);
    let detached = daemon.manager.detach(session_id);
    serde_json::json!({
        "type": "closed",
        "sessionId": session_id,
        // `false` here means "nothing was running under that id" — including the
        // case where it already exited, or where this connection never started
        // it. Reporting that as an error would make an idempotent `close` (the
        // shape a client sends on shutdown) look like a failure.
        "ok": detached.is_some(),
    })
}

/// Run `f` against the session's handle, with the same reply for "no such
/// session" and "the handle is gone" — from the client's side those are one
/// thing to act on (start it, or stop talking to it).
fn relay(
    handle: Option<WorkerHandle>,
    f: impl FnOnce(&WorkerHandle) -> Result<(), fulcrum_core::worker::WorkerError>,
) -> Result<(), String> {
    let handle = handle.ok_or_else(|| "no session is running under that id".to_string())?;
    f(&handle).map_err(|e| e.to_string())
}

fn relay_result(kind: &str, session_id: &str, result: Result<(), String>) -> serde_json::Value {
    match result {
        Ok(()) => serde_json::json!({ "type": kind, "sessionId": session_id, "ok": true }),
        Err(e) => serde_json::json!({
            "type": kind, "sessionId": session_id, "ok": false, "error": e,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    /// `--worker` is the literal `WorkerHandle::spawn` passes, and it is the ONLY
    /// spelling that selects Worker mode. A mode flag that also accepted
    /// `--worker=1` would be a second spelling nothing produces, and the second
    /// spelling is where a future `--worker-exe` prefix match would creep in.
    #[test]
    fn worker_flag_selects_worker_mode_and_serve_selects_the_daemon() {
        assert_eq!(parse_command(&argv(&["--worker"])), Command::Worker);
        // Anything after `--worker` is the child's business, not the parser's:
        // the spawn passes exactly one argument, and a strict "argv must be
        // length 1" rule would make the mode depend on an argument count nothing
        // guarantees.
        match parse_command(&argv(&["--worker", "--anything"])) {
            Command::Worker => {}
            other => panic!("`--worker` must win as the first argument, got {other:?}"),
        }
        let serve = parse_command(&argv(&["serve"]));
        assert!(
            matches!(serve, Command::Serve(_)),
            "`serve` alone must reach the daemon with defaults, got {serve:?}"
        );
    }

    /// Every flag that names a path must actually carry it, and a flag that ran
    /// off the end of argv must be an error rather than a silent default. The
    /// failure this guards is the quiet one: `--socket` with the value eaten by a
    /// shell quoting mistake, binding the default and telling nobody.
    #[test]
    fn serve_flags_are_parsed_and_a_missing_or_unknown_one_is_a_usage_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let socket = dir.path().join("s.sock");
        let db = dir.path().join("fulcrum.db");
        let exe = dir.path().join("worker");
        let config = match parse_command(&argv(&[
            "serve",
            "--socket",
            socket.to_str().unwrap(),
            "--db",
            db.to_str().unwrap(),
            "--worker-exe",
            exe.to_str().unwrap(),
        ])) {
            Command::Serve(config) => config,
            other => panic!("expected a parsed serve config, got {other:?}"),
        };
        assert_eq!(config.endpoint, Endpoint::socket(socket));
        assert_eq!(config.db_path, db);
        assert_eq!(config.worker_exe, Some(exe));

        for bad in [
            argv(&["serve", "--socket"]),
            argv(&["serve", "--db"]),
            argv(&["serve", "--worker-exe"]),
            argv(&["serve", "--worker-exe", ""]),
            argv(&["serve", "--sockt", "/tmp/x.sock"]),
        ] {
            let parsed = parse_command(&bad);
            assert!(
                matches!(parsed, Command::Usage(_)),
                "{bad:?} must be a usage error, got {parsed:?}"
            );
        }
    }

    /// No arguments, an unknown command, and a near-miss flag are all usage
    /// errors; `help` is the only thing that succeeds without doing work.
    #[test]
    fn no_command_and_unknown_commands_are_usage_errors() {
        assert!(matches!(parse_command(&argv(&[])), Command::Usage(_)));
        assert!(matches!(parse_command(&argv(&["nope"])), Command::Usage(_)));
        // The typo this parser's strictness exists for: a near-miss on the
        // worker-exe flag must not be ignored (ignored, the daemon spawns itself
        // as its own worker, recursively).
        assert!(matches!(
            parse_command(&argv(&["serve", "--worker-ex"])),
            Command::Usage(_)
        ));
        for help in ["help", "--help", "-h"] {
            assert_eq!(
                parse_command(&argv(&[help])),
                Command::Help,
                "`{help}` must print the usage and exit 0"
            );
        }
        assert!(usage().contains("--worker-exe"));
    }

    /// The `start` payload is a `StartEnv`, verbatim.
    ///
    /// The assertion is on the ROUND TRIP through the daemon's own stripper,
    /// because the risk is not "can serde parse a StartEnv" (the worker protocol
    /// already proves it can) — it is that the daemon strips `type` and nothing
    /// else, and that a field the client omits fails loudly instead of arriving
    /// as a default. A `start` that silently lost `model` would spawn a Worker
    /// pointed at the wrong provider.
    #[test]
    fn a_start_frame_deserializes_to_the_same_startenv_the_worker_gets() {
        let env = test_env("s1");
        let mut wire = serde_json::to_value(&env).expect("a StartEnv is encodable");
        wire.as_object_mut()
            .expect("an object")
            .insert("type".to_string(), serde_json::json!("start"));

        let parsed = start_env(&wire).expect("the daemon parses its own `start` frame");
        assert_eq!(
            parsed, env,
            "the daemon must hand the worker the SAME envelope the client sent"
        );
        assert_eq!(parsed.session_id, "s1");
        assert_eq!(parsed.model.id, env.model.id);

        // A missing required field is an error, not a default.
        let mut broken = wire.clone();
        broken.as_object_mut().expect("an object").remove("model");
        assert!(
            start_env(&broken).is_err(),
            "a `start` with no model is malformed"
        );

        // And `type` is genuinely stripped, not merely tolerated: `StartEnv` has
        // no `type` field, so if it were left in place with a `deny_unknown_fields`
        // struct this would fail — and the point of stripping it is that the
        // envelope the WORKER receives is the envelope minus the routing key.
        let re_encoded = serde_json::to_value(&parsed).expect("encodable");
        assert!(
            re_encoded.get("type").is_none(),
            "the envelope forwarded to the worker must not carry the daemon's \
             routing key: {re_encoded}"
        );
    }

    /// The relay's two failure shapes, both of which must be a FRAME and not a
    /// panic: the desktop's client can answer "no such session" only if the
    /// daemon stays up to say it.
    #[tokio::test]
    async fn unknown_frames_and_missing_sessions_are_answered_not_fatal() {
        let Harness { daemon, _dir } = test_daemon();
        let reply = handle_frame(
            &daemon,
            &mut HashSet::new(),
            &serde_json::json!({"type": "wat"}),
        )
        .await
        .expect("an unknown frame still gets a reply");
        assert_eq!(reply["type"], "error");
        assert!(
            reply["message"].as_str().is_some_and(|m| m.contains("wat")),
            "the error must name the frame it did not understand: {reply}"
        );

        let reply = handle_frame(
            &daemon,
            &mut HashSet::new(),
            &serde_json::json!({"type": "prompt", "sessionId": "nobody", "text": "hi"}),
        )
        .await
        .expect("a prompt for a missing session gets a reply");
        assert_eq!(reply["type"], "prompt-result");
        assert_eq!(
            reply["ok"], false,
            "a prompt for a session that is not running is not ok: {reply}"
        );

        // `close` on an id nothing runs is `ok: false`, not an error, and not a
        // panic (the idempotent-shutdown argument in `close`'s doc).
        let reply = handle_frame(
            &daemon,
            &mut HashSet::new(),
            &serde_json::json!({"type": "close", "sessionId": "nobody"}),
        )
        .await
        .expect("close gets a reply");
        assert_eq!(reply["type"], "closed");
        assert_eq!(reply["ok"], false);
    }

    /// A `start` whose envelope will not parse must be a `start-result` with
    /// `ok: false`, on the SAME connection.
    ///
    /// The alternative — closing the socket on a malformed envelope — is the
    /// failure mode that turns one typo in a client into "the daemon keeps
    /// dropping me" with nothing to read.
    #[tokio::test]
    async fn a_malformed_start_is_a_start_result_and_keeps_the_connection() {
        let Harness { daemon, _dir } = test_daemon();
        let reply = handle_frame(
            &daemon,
            &mut HashSet::new(),
            &serde_json::json!({"type": "start", "sessionId": "s1"}),
        )
        .await
        .expect("a malformed start still gets a reply");
        assert_eq!(reply["type"], "start-result");
        assert_eq!(
            reply["ok"], false,
            "a half envelope must not start: {reply}"
        );
        assert!(reply["error"].is_string(), "and it must say why: {reply}");

        // A `start` with no sessionId at all: same shape, still not fatal.
        let reply = handle_frame(
            &daemon,
            &mut HashSet::new(),
            &serde_json::json!({"type": "start"}),
        )
        .await
        .expect("a session-less start still gets a reply");
        assert_eq!(reply["type"], "start-result");
        assert_eq!(reply["ok"], false);
    }

    /// The fan-out frame carries the session id at the TOP level, because that is
    /// what the relay filters on — a filter that reached into `frame` would
    /// silently drop every frame the moment a worker frame gained a `sessionId`
    /// of its own (which the store frames all have).
    #[test]
    fn event_frames_carry_their_session_id_where_the_relay_reads_it() {
        let frame = event_frame(
            "s7",
            false,
            &WorkerInboundEvent::Event(fulcrum_core::events::RpcEvent::turn_start),
        );
        assert_eq!(frame["sessionId"], "s7");
        assert_eq!(frame["type"], "event");
        assert!(
            watched(&frame, &HashSet::from(["s7".to_string()])),
            "a relay watching s7 must receive an s7 frame"
        );
        assert!(
            !watched(&frame, &HashSet::from(["other".to_string()])),
            "a relay watching another session must NOT"
        );
        // The store frames carry their OWN `sessionId` inside `frame` — the
        // filter reads the top level, so a subagent's frame routed under its
        // parent's id is not re-filtered by the child's id.
        let store = event_frame(
            "parent",
            true,
            &WorkerInboundEvent::Store(fulcrum_core::worker_types::StoreFrame::Insert {
                session_id: "child".to_string(),
                seq: 1,
                role: "user".to_string(),
                content_json: "{}".to_string(),
            }),
        );
        assert_eq!(store["sessionId"], "parent");
        // THREE levels, and that is the daemon's shape, not an accident:
        // `event_frame` puts the worker frame under `frame`, and
        // `worker_frame(Store(..))` puts the persistence frame under a `frame`
        // of its own. The integration client spells the same path
        // (`frame["frame"]["frame"]["type"]` in `tests/serve.rs`) — the two
        // sites are the same wire and must be read the same way.
        assert_eq!(store["frame"]["frame"]["sessionId"], "child");
        assert!(
            watched(&store, &HashSet::from(["parent".to_string()])),
            "the parent's relay must see its child's rows"
        );
        // A frame with no session at all (the `gap` notice) is addressed to the
        // connection and always passes.
        assert!(watched(
            &serde_json::json!({"type": "gap", "skipped": 3}),
            &HashSet::new()
        ));
    }

    /// The daemon must be constructible with NO tokio runtime beyond the test's
    /// own and NO worker executable — the fan-out and the store are the only
    /// things `Daemon::new` touches. This is also the guard on the `cwd`
    /// registrar: a missing wire there is a subagent's transcript silently
    /// failing its FK, and the registrar is observable through the manager.
    #[tokio::test]
    async fn the_daemon_wires_the_persister_to_the_manager() {
        let Harness { daemon, _dir } = test_daemon();
        let env = test_env("wired");
        // The observable proof the cwd wiring landed is that a row the persister
        // creates from a `cwd` it was told about carries that path, and that the
        // daemon's own eager write records the cwd the client asked for rather
        // than the empty string the lazy path would produce.
        daemon.persister.set_cwd("wired", &env.cwd);
        daemon.record_session(&env).expect("the row is written");
        let row = daemon
            .db
            .session("wired")
            .expect("the read works")
            .expect("the session row exists");
        assert_eq!(
            row.cwd, env.cwd,
            "the eager row carries the cwd the client asked for, not the empty \
             string the lazy path would have written"
        );
        assert_eq!(row.id, "wired");
        assert!(
            !row.is_subagent,
            "a client-started session is a MAIN session"
        );
    }

    /// A test `StartEnv` (the shape `client.rs`'s own `test_env` uses — a fake
    /// provider that needs no network, because the daemon must be testable
    /// without an LLM).
    fn test_env(session_id: &str) -> StartEnv {
        use fulcrum_core::harness::catalog::{Model, ModelCatalog};
        use fulcrum_core::worker::protocol::StartMode;

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
            None,
            "/tmp/config".to_string(),
            false,
            None,
            None,
        )
    }

    /// A daemon on a temp database with a factory nothing spawns, plus the
    /// `TempDir` it lives in.
    ///
    /// `Arc<Db>` in a temp dir, so the store is the real one — a double would
    /// certify nothing about the FK or the WAL, which is what the durability
    /// claim rests on. The dir comes back because it must OUTLIVE the daemon:
    /// dropping it removes the directory under a live WAL connection, which is a
    /// test failure waiting to be misread as a store bug — and leaking it instead
    /// is what `check-temp-dir-census.sh` exists to catch.
    struct Harness {
        daemon: Arc<Daemon>,
        _dir: tempfile::TempDir,
    }

    fn test_daemon() -> Harness {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = Arc::new(Db::open(&dir.path().join("fulcrum.db")).expect("open the daemon store"));
        // The `WorkerManager` holds the factory for the process's life and only
        // calls it from `attach`, which these tests never reach.
        let factory = Arc::new(DaemonWorkerFactory::new("never-spawned"));
        Harness {
            daemon: Daemon::new(db, factory),
            _dir: dir,
        }
    }
}

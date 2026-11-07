//! The `fulcrum serve` daemon: argv, the accept loop, and the client↔session
//! multiplex (T9 Task 5).
//!
//! # What the daemon IS
//!
//! A process supervisor and a transport, nothing else. Every engine capability
//! it needs already exists and is reused rather than re-made:
//!
//! * `WorkerManager` is the session-keyed Worker registry — `attach` spawns the
//!   child, waits for `ready`, starts the read-loop pump, sends `start`, and
//!   registers under the session id; crash detection, the **Dead bound**, and
//!   subagent dispatch all live inside it. **There is deliberately no second
//!   registry here.** The only per-session state this file keeps is which socket
//!   wants which session's frames, which is routing, not supervision.
//! * `TranscriptPersister` is the sole writer of `fulcrum.db`, fed from the
//!   pump's `on_event` — the same seam the desktop drives.
//! * `WorkerHandle` is the only child constructor (the `childproc` census exists
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
//!   **This one is required**: it is the first frame the daemon will serve, and a
//!   connection that sends anything else first is refused — see
//!   `MAX_PRE_HELLO_FRAMES` for the exact shape (a couple of frames are dropped
//!   unseen, then one `error`, then the close).
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
//! clean-exit path).
//!
//! # The local endpoint's defenses, and what they still do not buy
//!
//! There is still **no authentication** — that is T13. What T3 (issue #4) added
//! is the local-level hardening that has to exist regardless of T13, in three
//! layers, each with its own numbers named at its own definition:
//!
//! * **Peer identity** at accept: the unix leg refuses a peer whose uid is not the
//!   daemon's (`TransportError::PeerIdentity`, and `transport::unix`'s platform
//!   table). The Windows leg cannot prove this and says so in
//!   `transport::windows`'s header. So the sentence "the socket is the credential"
//!   is now: the socket's mode is the credential, AND the peer-credential check is
//!   the second, defense-in-depth check.
//! * **A required `hello` handshake**: `MAX_PRE_HELLO_FRAMES`. The `hello` frame
//!   already existed and already got answered; what was missing was that anything
//!   sent before it was served anyway. That is measured, not rhetorical — a raw
//!   socket writing `start` with no greeting got `ok: true` and a child's pid.
//! * **Bounded inbound**: `SESSION_LIMIT` concurrent MAIN sessions and
//!   `FRAME_RATE` non-greeting frames per second per connection. Before the
//!   session bound, one client was measured taking 249 sessions before the runner
//!   hit `EMFILE`.
//!
//! None of these is authentication, and none should be described as such. A
//! same-uid process still reaches this daemon on every platform; the residual is
//! T13's.
//!
//! **Open question (issue #4, and a non-goal of both T3 and T13):** transcripts
//! are stored in plaintext in `fulcrum.db`. Nothing here encrypts them at rest,
//! and T13's protocol-layer auth does not either — a bearer token on the wire says
//! nothing about the file. If a compliance requirement ever demands encryption at
//! rest, it is its own task, not a clause of this one.
//!
//! # A slow frame on ONE connection no longer stalls that connection's own relay
//!
//! Stated here because it WAS a known latency of the daemon's own design, because a
//! commit message once claimed it was fixed when it was not, and because the shape
//! that fixed it is the one thing a future reader is most tempted to undo.
//!
//! `serve_client` used to be ONE task running ONE `select!`, with a `start` running
//! inline in its inbound arm — `attach` is the engine's 5 s `ready` bound — so while a
//! client waited for ITS `start-result`, the same task could not drain that client's
//! event relay. It is now TWO tasks (`reader_loop` and `writer_loop`): the reader
//! may stay parked in `attach` for its full bound while the writer keeps relaying this
//! connection's fan-out and draining its queue.
//! `tests/serve.rs::a_slow_start_does_not_stall_that_clients_own_event_relay` is the
//! measurement, and it holds a worker fixture's `ready` frame so the park is a fact the
//! test controls rather than a duration it guesses — which is what makes the old design
//! red *by construction* in that span instead of by timing margin.
//!
//! The stall was never a correctness problem (no frame was lost, only delayed) and
//! never affected another connection (the task was per connection). What it cost was
//! latency a client caused to itself, up to `attach`'s bound.
//!
//! # ⚠ Why the relay lives in the writer, and why you may not merge the tasks back
//!
//! The placement is load-bearing: the fan-out arm is in `writer_loop` because that
//! task never calls `handle_frame`. Move the relay into the reader's loop — even with
//! a second `select!`, even "temporarily" — and the split becomes decorative: the relay
//! stalls for exactly the window the bug was about.
//!
//! And the reason a second writer is safe AT ALL is `transport::frame`: a JSONL stream
//! has no way to resynchronise once two writers interleave a half-written line, so
//! `Connection::pending` (the tail of a partially-accepted frame) must have exactly one
//! owner. `ConnectionWriter` owns it, `Connection::split_writer` hands it out once, and
//! `Connection` has no write method — so "two tasks reach `pending`" is not discouraged
//! here, it is unrepresentable. Do not put a `Mutex<Connection>` back, do not re-add a
//! convenience `send_frame` to the reader half, and do not hand a `ConnectionWriter` to
//! anything but `writer_loop`. `tests/transport.rs`'s writer block is the pin behind
//! that sentence, and `a_shared_writer_under_a_mutex_never_interleaves_frames` is the
//! one in that block that reddens the mutex-shaped regression the others false-green on.
//!
//! Every buffer a client can grow is BOUNDED (finding 7) — the fan-out ring
//! (`FAN_OUT_CAPACITY`), one connection's outbound queue
//! (`CLIENT_QUEUE_CAPACITY`), whose full case produces a `gap` notice instead of
//! an unbounded allocation), and in `transport::frame` the inbound line length and
//! the reader's queue. The last pair is where the growth actually was; that
//! module's header carries the measurement. The split adds a second PRODUCER to that
//! queue, not a second buffer — see `CLIENT_QUEUE_CAPACITY`'s note.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use fulcrum_core::events::EventSink;
use fulcrum_core::persist::TranscriptPersister;
use fulcrum_core::seams::FulcrumPersistence;
use fulcrum_core::store_rows::SessionInfo;
use fulcrum_core::worker::client::{WorkerHandle, WorkerInboundEvent};
use fulcrum_core::worker::manager::{OnCrash, SinkLookup};
use fulcrum_core::worker::protocol::{StartEnv, PROTOCOL_VERSION};
use fulcrum_core::worker::{WorkerFactory, WorkerManager};

use crate::db::Db;
use crate::transport::{Connection, ConnectionWriter, Endpoint, Listener, TransportError};

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

/// How many undelivered frames ONE connection's outbound queue holds before the
/// daemon starts dropping for it (finding 7).
///
/// # Why this bound exists at all
///
/// `serve` has no authentication — legitimately deferred to T13 — so any local
/// process that can reach the socket is a client. This queue used to be an
/// `mpsc::unbounded_channel`, justified in its own comment as "unbounded so
/// `send` is infallible from inside the inbound arm", and that justification is
/// what made the hole hard to see: it removed the error that would have made the
/// case observable.
///
/// # What this bound does NOT buy, measured
///
/// Being straight about this, because the intuition is seductive and was wrong:
/// this queue is NOT where finding 7's unbounded growth lived. In the SINGLE-task
/// `serve_client` this file used to have, while the drain arm's body awaited a
/// blocked `send_frame` the `select!` was not re-polled and the inbound arm could
/// not run and could not enqueue — which capped the queue's depth at about 1
/// regardless of how much a peer sent. Instrumented directly at the time: at a
/// capacity of 4 the drop path fired constantly, and at 1024 a flood of 40 000
/// requests never filled it.
///
/// The buffer that actually grew was `transport::frame`'s inbound queue — the
/// reader task draining the socket into an unbounded channel nothing was
/// consuming. That is measured, with numbers, in that module's header, and it is
/// where the fix for the growth is.
///
/// # After T4: two producers (reader replies + writer relay)
///
/// That "~1" is a property of the shape that no longer exists. `serve_client` is two
/// tasks now, so [`reader_loop`] can enqueue replies while [`writer_loop`] is parked
/// mid-`send_frame` to a slow peer — the depth is no longer capped by the drain
/// blocking the producer, because they are different tasks. This number is therefore
/// the bound that MATTERS, exactly as the paragraph below (written before the split)
/// predicted it would: a stalled connection's queue is now genuinely as deep as its
/// patience, and 1 024 is what stops it.
///
/// What did NOT change is the total write-path buffering per connection, and it is
/// worth naming in one sentence because it is the number a reviewer should want:
/// `CLIENT_QUEUE_CAPACITY` frames in the queue + `ConnectionWriter::pending`'s
/// in-flight tail + the socket's own OS send buffer. All three are bounded, all three
/// are per connection, and none of them grew.
///
/// The bound is still the right thing for the two reasons that never depended on it
/// being the big one: it is what makes "we dropped a frame for you" a thing the daemon
/// knows and can say (see [`gap_frame`]), which an unbounded queue can never produce;
/// and it is what keeps one stalled client from holding the daemon's memory.
///
/// # Consequence for the tests, stated rather than glossed
///
/// The full-queue `gap` path is therefore NOT reachable from a socket test at this
/// capacity, and no test pretends otherwise. Measured on the SINGLE-task shape: a
/// client that wrote 60 000 requests and read nothing got 1 377 through in total —
/// the inbound bound plus the kernel buffers cap the flood below the 1 024 the
/// outbound queue needs, so the queue peaked around 353. (That peak is the old shape's
/// number: with two tasks the queue can now fill, since the producer no longer waits
/// on the drain. The measurement this rests on — the inbound bound capping the flood
/// at ~1.4 k — is about `transport::frame`'s reader and the socket, and the split does
/// not touch the inbound path at all, so the conclusion still holds; the depth figure
/// is the part that belonged to the old shape. See
/// `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat`, which still
/// measures frames accepted and the daemon's RSS against the same caps.)
/// What IS covered, and where:
///
/// * the drop-and-count-and-keep mechanism, deterministically — `enqueue`'s unit
///   test, which drives a capacity-1 channel;
/// * the notice's shape — [`gap_frame`]'s unit test;
/// * the growth that was actually unbounded — `a_client_that_never_reads_is_
///   throttled_and_the_daemon_stays_flat` in `tests/serve.rs`, which measures
///   frames accepted and the daemon's RSS.
///
/// # Why 1024
///
/// Two numbers, both already in this file:
///
/// * **A quarter of [`FAN_OUT_CAPACITY`].** The fan-out ring upstream is 4096 and
///   is SHARED by every connection, so a per-client queue larger than it would
///   let one stalled client hold more than the daemon's entire event backlog —
///   the bound would be looser than the thing feeding it, which is not a bound.
/// * **Bytes, not counts, are what matters.** The frames here are small JSON
///   values — an `agent_message_chunk` is ~300 bytes of text inside a few
///   hundred bytes of `Value`. At ~0.5 KB per frame, 1024 entries is well under
///   1 MB per stalled connection, so a hundred stalled connections cost tens of
///   megabytes instead of whatever the attacker's patience buys. That is the
///   budget this number is chosen against: a stalled connection should cost less
///   than the worker process it is watching, and 1 MB clears that bar with room.
///
/// Dropping is not fatal and is not silent: the drain arm publishes a `gap`
/// frame carrying the count, the same notice the fan-out's lag path already
/// emitted, and keeps the connection. Recovery is T10's `attach { afterSeq }`,
/// and until then a client that cannot keep up seeing a gap beats a daemon that
/// buffers on a stranger's behalf.
const CLIENT_QUEUE_CAPACITY: usize = 1024;

/// The bound's two invariants, checked at COMPILE time.
///
/// A compile-time assertion rather than a `#[test]`: these are statements about
/// constants, so a test could only ever be a tautology that runs after the build
/// already succeeded — the break it guards against is someone editing one of the
/// two numbers, and that is a build failure, not a test failure. Same shape as
/// `db::Db`'s `const _: [(); 10]` arity tie.
const _: () = assert!(
    // Non-zero, because `channel(0)` panics at RUNTIME — the quietest possible
    // way to discover this is on the first client that connects.
    CLIENT_QUEUE_CAPACITY > 0,
    "CLIENT_QUEUE_CAPACITY must be non-zero: tokio's channel panics on a capacity of 0"
);
const _: () = assert!(
    // Strictly below the fan-out ring. That ring is SHARED by every connection,
    // so a per-client queue as large as it would let one stalled client hold the
    // daemon's entire event backlog — a bound looser than the thing feeding it is
    // not a bound.
    CLIENT_QUEUE_CAPACITY < FAN_OUT_CAPACITY,
    "CLIENT_QUEUE_CAPACITY must be below FAN_OUT_CAPACITY: the per-client queue \
         must not be able to buffer more than the shared ring upstream holds"
);

// ── The per-connection hardening bounds (issue #4) ───────────────────────────

/// How many NON-`hello` frames one connection may send before it has greeted.
///
/// The `hello` frame already existed in the protocol and `handle_frame` already
/// answered it — what did NOT exist was a requirement to send it. Any local
/// process that reached the socket could go straight to `start` and get a child
/// process spawned for it. That is measured, not argued: before this gate existed,
/// a raw socket writing three `start` frames and no `hello` got
/// `ok: true` plus a worker pid on the first one.
///
/// Two, not zero, and not one: the greeting is one frame, and a client that
/// PIPELINED `hello` + `start` in one write (which
/// `tests/serve.rs::two_connections_racing_to_start_one_id_leave_exactly_one_child`
/// does on purpose, to keep two `start`s in flight together) must not be refused
/// for a race its own test is studying. Two non-`hello` frames get dropped
/// unseen; the third is refused by name and the connection is closed. Refusing
/// rather than serving is the issue's wording — a pre-`hello` frame gets NO reply,
/// because answering a `start` before the handshake would leak routing behaviour
/// to a peer that has not identified itself as a client at all.
const MAX_PRE_HELLO_FRAMES: usize = 2;

/// How many NON-GREETING frames one connection may pull out of the inbound queue
/// per second, sustained (issue #4).
///
/// A token bucket, [`FRAME_RATE_BURST`] deep, refilled at this rate, and a HARD
/// rejection when it runs dry: the peer gets one `error` frame and then a closed
/// socket. Never a delay — a delayed flood stresses the queue, and the queue is
/// the buffer this daemon has already been measured growing on (see
/// [`CLIENT_QUEUE_CAPACITY`]'s "What this bound does NOT buy" section); a closed
/// socket puts the pressure back on the sender's own kernel.
///
/// # Why `hello` frames are NOT counted, and why that is not a hole
///
/// `hello` is answered by one `json!` construction with no session, no child, no
/// database row. The frames this gate exists to bound are `start` (a process
/// spawn) and `prompt` (a relay to a live child) — the frames that cost the daemon
/// a syscall each.
///
/// A hostile client can still flood `hello`, and that is deliberately left to the
/// bound that already exists and is already measured: the inbound reader queue
/// (`frame::MAX_QUEUED_INBOUND_FRAMES`) plus socket backpressure, whose numbers
/// `tests/serve.rs::a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat`
/// pins (1 375 frames then blocked, RSS 15 → 19 MB, against 1 168 009 frames and
/// 895 MB unbounded). Counting `hello` here would close that connection at ~600
/// frames into the same flood and break that test's measurement — which is not a
/// reason to keep it green for its own sake, but IS the measurement that says the
/// `hello` path is already bounded by something better placed than this gate.
const FRAME_RATE: usize = 200;

/// The burst the bucket starts with, and its ceiling (issue #4).
///
/// Three seconds' worth of the sustained rate. Generous on purpose: a client
/// opening a dozen sessions at once (a supervisor starting a fan-out) is
/// well-behaved and should not be refused for it, while a loop that wants to
/// exhaust the process table hits the wall inside a second. The frames counted here
/// are already DECODED and already queued, so a burst costs the daemon the queue
/// depth it has already paid for, not a new buffer.
const FRAME_RATE_BURST: usize = FRAME_RATE * 3;

/// The per-connection inbound frame-rate gate (issue #4).
///
/// Plain f64 tokens, `Instant` (monotonic — a wall-clock jump must not be able to
/// refill the bucket or starve it), and no new dependency. Per-connection LOCAL state,
/// deliberately: the rate is a property of one socket, so it belongs to the one task
/// that reads that socket, not to `Daemon` — and keeping it a local rather than daemon
/// state is what let the whole gate move into [`reader_loop`] intact when
/// `serve_client` was split (issue #4's state is READER-half state: the writer never
/// sees a frame the reader refused, and it must not have to).
struct FrameRateGate {
    /// Tokens available now (one per frame the peer may still send).
    tokens: f64,
    /// When the bucket was last refilled.
    last: std::time::Instant,
}

impl FrameRateGate {
    fn new(now: std::time::Instant) -> Self {
        Self {
            tokens: FRAME_RATE_BURST as f64,
            last: now,
        }
    }

    /// Take one token, refilling by elapsed time first. `false` means refuse.
    fn accept(&mut self, now: std::time::Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * FRAME_RATE as f64).min(FRAME_RATE_BURST as f64);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

/// Where one connection's handshake has got to (issue #4).
enum Handshake {
    /// Nothing has greeted yet, with the count of non-greeting frames seen so far.
    WaitingHello { pre_hello: usize },
    /// The peer said `hello`; every frame type is now on the table (subject to
    /// [`FrameRateGate`]).
    Done,
}

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

/// One dispatched subagent, as the daemon's `subagents` map remembers it.
///
/// `closed` is a TOMBSTONE flag rather than a deletion signal — see the field
/// doc on [`Daemon::subagents`] for the pump ordering that makes deleting wrong.
#[derive(Clone)]
struct SubagentRecord {
    /// The parent that dispatched it (the id the lifecycle frame was emitted
    /// on). Kept because it is the only part of the pairing that is not
    /// recoverable from anywhere else in the daemon.
    parent: String,
    /// Whether `subagent-closed` has been seen. Prunable once it has.
    closed: bool,
}

/// The supervisor + the fan-out. One per process, `Arc`'d, shared by every client
/// task.
struct Daemon {
    /// THE session registry (the engine's). Every session operation goes through
    /// it; nothing here keeps its own list of live sessions.
    ///
    /// `Arc`'d rather than held inline so `on_crash` can reach it through a
    /// `Weak` — the closure is a *dependency* of the manager (it is an argument
    /// to `WorkerManager::new`), so it cannot hold a strong handle on it.
    manager: Arc<WorkerManager>,
    /// The sole writer of `fulcrum.db` (shared with the pump's `on_event`).
    persister: Arc<TranscriptPersister>,
    /// The eager `sessions`-row writer (see [`Daemon::record_session`]).
    db: Arc<Db>,
    /// The multiplex: every client-facing frame, in order, to every connection.
    fan_out: tokio::sync::broadcast::Sender<serde_json::Value>,
    /// Child session id → the parent that dispatched it, plus whether the
    /// dispatch has been closed.
    ///
    /// Built from the engine's own `subagent-session-started` / `subagent-closed`
    /// lifecycle frames, which `FanOutSink::emit` already carries through (they
    /// are emitted on the PARENT's sink and name the child in `sessionId` +
    /// `parentSessionId`). The daemon needs this because the pump's `on_crash`
    /// passes only an id and an exit code — no `is_subagent` flag — so without a
    /// mapping the daemon would have to guess, and guessing `false` is what made
    /// a subagent crash both mislabelled and invisible on the socket (`watched`
    /// holds only ids a client `start`ed, which are always parents).
    ///
    /// A closed dispatch leaves a TOMBSTONE (`SubagentRecord::closed`) rather
    /// than being dropped, because of an ordering the pump forces: it publishes
    /// the drive's exit watch BEFORE it calls `on_crash`, and the drive task's
    /// teardown emits `subagent-closed` on another worker thread. Deleting on
    /// close would let that teardown erase the entry between the two and put the
    /// label back to a wrong `false` — the race the field exists to close.
    /// Tombstones are dropped by [`Daemon::prune_subagents`], which runs on the
    /// same registry touch as `start`.
    subagents: Arc<StdMutex<HashMap<String, SubagentRecord>>>,
    /// Session ids with an `attach` in flight.
    ///
    /// The duplicate-`start` guard cannot be just `handle_for(id).is_some()`:
    /// `attach` is a ~5 s await and its registry `insert` is the only atomic
    /// step, so two clients could both pass a check made BEFORE either insert
    /// and the second would replace the first's entry — orphaning a live child
    /// process, which is the exact leak the guard exists to prevent. Holding the
    /// id across the await makes check-and-claim atomic.
    starting: Arc<StdMutex<HashSet<String>>>,
    /// How many MAIN sessions this daemon is committed to running (issue #4).
    ///
    /// The session bound needs a COUNT, and this is the only honest place to keep
    /// one: `WorkerManager` exposes no `len()` and `handle_for` iterates nothing,
    /// so a derived count would need an engine change (out of scope here), and a
    /// second registry would be the thing this file has repeatedly promised not to
    /// become ("there is deliberately no second registry here").
    ///
    /// # It counts COMMITMENTS, not live handles — and the difference is the point
    ///
    /// The slot is taken in [`Daemon::claim_session`], BEFORE `attach`, and handed
    /// to the session by [`SessionClaim::commit`] when `attach` returns `Ok`. So a
    /// session in flight counts. That is deliberate: the threat is a flood of
    /// `start`s, and a counter that only counted registered workers would let an
    /// unbounded number of `attach`es queue up behind the bound.
    ///
    /// Four touch points, and the ORDER of the first two is load-bearing:
    ///
    /// * **increment** — in [`Daemon::claim_session`], under the `starting` lock.
    ///   MAIN only, which it is by definition: a subagent's row comes from
    ///   `dispatch_subagent`, never from `start`.
    /// * **release-on-failure** — in [`SessionClaim::drop`], when the claim was
    ///   never committed: a `start` whose row write or `attach` failed holds nothing.
    ///   Without this, `SESSION_LIMIT` failed starts (a typoed `--worker-exe` retried
    ///   thirty times) would consume every slot and blame the session limit for the
    ///   typo. Pinned by `a_failed_start_gives_its_session_slot_back`.
    /// * **decrement** — in [`close`], and ONLY when `detach` returned `Some`.
    ///   `None` means nothing was running, and decrementing that would drive the
    ///   counter below zero — which on an `AtomicUsize` is not a panic and not a
    ///   clamp but a WRAP to `usize::MAX`, i.e. a session limit that fires for the
    ///   rest of the daemon's life. Pinned by
    ///   `a_close_with_nothing_running_releases_nothing`.
    /// * **decrement** — in `Daemon::new`'s `on_crash`, guarded on the `subagents`
    ///   map having NO record for the id. `on_crash` receives only
    ///   `(session_id, code, pid)` — no `is_subagent` flag — so that lookup is how
    ///   this closure knows whether the dead id was ever counted; decrementing a
    ///   child's id wraps the counter the same way.
    ///
    /// Taking the slot before the child exists means the increment precedes any
    /// `on_crash` (nothing can crash a session that has not been spawned), which is
    /// the ordering that keeps the counter from wrapping. One window remains and it
    /// is stated rather than glossed: `attach` registers the worker BEFORE it
    /// returns, so a `close` for the same id arriving from ANOTHER connection
    /// between that registration and [`SessionClaim::commit`] decrements the slot
    /// this claim took, and the commit then stops `Drop` from giving it back — the
    /// counter ends up one BELOW the live-session count. That direction is the safe
    /// one (a bound that is momentarily one permissive, never a wrap: the claim's
    /// own `+1` is still in the counter when the foreign `close` takes its `-1`, so
    /// the subtraction cannot cross zero), and closing it would mean holding the
    /// `starting` lock across the `attach` await, which this file's own rule forbids.
    /// No site here therefore needs a saturating decrement.
    ///
    /// That is the whole discipline, and it is the discipline `Daemon::dead` failed
    /// (see [`close`]'s invariant note: a per-session set is one of the two places
    /// that has to release it). An atomic counter was chosen over a set precisely
    /// because there is nothing here to forget to remove.
    ///
    /// `Arc` so the `on_crash` closure and each `SessionClaim` can hold it without
    /// reaching back at the `Daemon` they are dependencies of — the same late-binding
    /// reason `manager_slot` exists.
    attached_sessions: Arc<std::sync::atomic::AtomicUsize>,
}

/// How many MAIN sessions one daemon will run at once (issue #4).
///
/// Every local process that can reach the socket can `start`, and `start` spawns a
/// child. Without a bound the ceiling on that is the process table, so one buggy
/// client in a retry loop — measured, before this bound existed: 249 sessions
/// accepted before the runner hit `EMFILE` — takes fork away from every other
/// process running as this user.
///
/// 32, sized against the use and not against the machine: this daemon supervises
/// ONE user's interactive sessions, so 32 is far above any realistic concurrent
/// count and far below the fd/memory ceiling the real constraint is. Deliberately
/// well above what this repo's own suite ever opens (the widest test holds two
/// sessions at once), so the bound is invisible to legitimate traffic and loud to
/// a runaway client.
///
/// Enforced in [`Daemon::claim_session`] — the seam every `start` already routes
/// through — rather than in `serve_client`, so a future second entry point cannot
/// bypass it.
const SESSION_LIMIT: usize = 32;

impl Daemon {
    fn new(db: Arc<Db>, factory: Arc<dyn WorkerFactory>) -> Arc<Self> {
        let (fan_out, _) = tokio::sync::broadcast::channel::<serde_json::Value>(FAN_OUT_CAPACITY);
        let persister = Arc::new(TranscriptPersister::new(
            Arc::clone(&db) as Arc<dyn FulcrumPersistence>
        ));
        let subagents: Arc<StdMutex<HashMap<String, SubagentRecord>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let starting: Arc<StdMutex<HashSet<String>>> = Arc::new(StdMutex::new(HashSet::new()));
        let attached_sessions: Arc<std::sync::atomic::AtomicUsize> =
            Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let manager_slot: Arc<std::sync::OnceLock<std::sync::Weak<WorkerManager>>> =
            Arc::new(std::sync::OnceLock::new());

        // The pump's `on_event` — the ONE place a Worker frame is turned into
        // (a) durable rows and (b) socket traffic. Both halves run for EVERY
        // frame, including `Exited`, because the pump calls it first and
        // unconditionally (`pump::run_pump_loop`), and the desktop's router makes
        // the same split for the same reason: persistence must not depend on
        // someone watching.
        let persist = Arc::clone(&persister);
        let fan = fan_out.clone();
        let on_event: OnEvent = Arc::new(move |session_id, is_subagent, evt| {
            // This closure touches NO registry state, and that is a rule rather
            // than an accident. The pump calls `on_event` BEFORE it computes
            // `unexpected_exit` (`pump.rs`: the `on_event` call, then the
            // `Exited` arm), and `unexpected_exit` answers "still in the map and
            // not reaping" — so a `detach` issued from in here would remove the
            // entry first and make EVERY crash read as an EXPECTED exit,
            // silently killing the crash report this file exists to deliver.
            // Clearing a dead registration therefore lives in `on_crash` alone,
            // which the pump fires only AFTER it has made that decision.
            //
            // There used to be a second clearing path here: a `dead` set this
            // closure inserted into on `Exited`, which `start` consulted as a
            // belt to `on_crash`'s `detach`. It was deleted, on measurement
            // rather than on taste — see `on_crash`'s step-1 note for the two
            // experiments and what each one proved. The short form: the belt was
            // unreachable in the only scenario it was built for, and it leaked
            // one entry per crash-then-close because `close` never cleared it.
            //
            // (a) Durable first: the store frames, and the `interactive-event`
            // sink frames (todos / cost) that the persister owns. Both are
            // synchronous and both are brief — one statement each — which is what
            // makes calling them from the pump task acceptable even though the
            // store is a `std::sync::Mutex` (see `db/mod.rs`: never hold the
            // guard across an await, and nothing here does).
            persist_frame(&persist, &session_id, is_subagent, &evt);
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
        //
        // Two things happen here beyond the frame, and both matter:
        //
        // 1. THE REGISTRATION IS CLEARED — and this is the ONLY place it is
        //    cleared. The engine removes a worker from its map only on `detach`
        //    / `reap_all`, so without this a crashed session answers "already
        //    running" to every later `start` — for the life of the daemon.
        //    Reaching the manager from here needs the late-bound `Weak` below:
        //    `on_crash` is an ARGUMENT to `WorkerManager::new`, so the closure
        //    cannot hold the manager it helps construct.
        //
        //    Detaching here is safe in a way that doing it from `on_event` is
        //    NOT: the pump has already computed `unexpected_exit` by the time it
        //    calls `on_crash`, so removing the entry cannot retroactively make
        //    the exit look expected and swallow the report.
        //
        //    MEASURED, because a second mechanism used to claim this job too. A
        //    `dead` set (inserted by `on_event` on `Exited`, consulted by
        //    `claim_session`) was supposed to be the belt for the two windows
        //    this closure is weak to: an exit the pump decides is EXPECTED, and
        //    the `Weak` not yet filled. Two experiments against a real daemon
        //    and a real `kill -9` (`tests/serve.rs::
        //    a_child_killed_by_sigkill_is_reported_and_can_be_started_again`
        //    is the permanent version of that measurement):
        //
        //    * never insert into `dead` → the SIGKILL test still passes, and so
        //      does the whole suite: `on_crash`'s `detach` is what recovers a
        //      killed child, on both the exit-code and the signal path;
        //    * disable this `detach` and leave `dead` working → the SIGKILL test
        //      still passes, with `claim_session` logging its reclaim: the belt
        //      DOES work when the primary is removed.
        //
        //    So the belt was real machinery that could only ever fire if this
        //    `detach` did not — and the two windows it named are not reachable
        //    in a way that matters: the pump fires `on_crash` for a main
        //    session's every unexpected exit, and `manager_slot` is filled
        //    before the `Arc<Daemon>` exists, so no worker can ever die while it
        //    is empty (`attach` is the only spawn path, and it runs after
        //    `Daemon::new` returns). Meanwhile the set cost an unbounded leak —
        //    `close` never released an id, so every crash-then-close left one
        //    entry for the life of the process — and no test in the file
        //    exercised it. Deleted on that evidence. If a future change ever
        //    makes `on_crash` conditional for main sessions, THIS is the line to
        //    re-read before re-introducing a second clearing path.
        //
        // 2. `is_subagent` IS THE TRUTH. The pump fires this with the CHILD's
        //    id for a subagent (its `session_id` argument is the pump's own,
        //    not the `delivery_id` `on_event` gets) and passes no flag, so the
        //    daemon has to know. A hardcoded `false` did two kinds of damage:
        //    the frame mislabelled a child's death as a main session's, and
        //    because `watched` only ever holds ids a client `start`ed — always
        //    parents — a subagent crash was filtered out and never reached the
        //    socket at all. The daemon learns the mapping from the engine's own
        //    lifecycle frames (see [`FanOutSink::subagents`]).
        let fan_crash = fan_out.clone();
        let crash_manager = Arc::clone(&manager_slot);
        let crash_subagents = Arc::clone(&subagents);
        let crash_sessions = Arc::clone(&attached_sessions);
        let on_crash: OnCrash = Arc::new(move |session_id, code, pid| {
            // Clear the dead registration (1 above). `Weak::upgrade` fails only
            // once the `Daemon` is gone, and then there is nothing to clean.
            // A subagent's handle lives in the engine's `drives` map, which
            // `detach` does not touch and which the drive task's own teardown
            // owns — so a `None` here for a child is expected, not a miss.
            if let Some(manager) = crash_manager.get().and_then(std::sync::Weak::upgrade) {
                manager.detach(&session_id);
            }
            // Release the session slot (issue #4) — but only for a MAIN session,
            // because only MAIN sessions were ever counted. `on_crash` is handed an
            // id and an exit, no `is_subagent` flag, so the child→parent map is how
            // this closure tells the two apart: a record means the id is a
            // dispatched CHILD, which `start` never incremented, and decrementing it
            // would underflow an `AtomicUsize` (and a wrapped counter is a session
            // limit that fires on the daemon's first client).
            //
            // Scoped, because `crash_report_frame` takes this same mutex — holding
            // the guard into that call would self-deadlock a `std` mutex.
            {
                let subagents = crash_subagents.lock().unwrap_or_else(|p| p.into_inner());
                if !subagents.contains_key(&session_id) {
                    crash_sessions.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let _ = fan_crash.send(crash_report_frame(&crash_subagents, &session_id, code, pid));
        });

        // The parent-scope sink lookup (the subagent lifecycle frames —
        // `subagent-session-started` / `subagent-closed` / the gate closes — are
        // emitted on the PARENT's sink, so they must reach the sockets watching
        // the PARENT). A sink per lookup is cheap: it publishes into the same
        // broadcast, and `send` failing just means nobody is watching.
        let fan_sink = fan_out.clone();
        let sink_subagents = Arc::clone(&subagents);
        let sink_for: SinkLookup = Arc::new(move |session_id| {
            Some(Arc::new(FanOutSink {
                session_id: session_id.to_string(),
                fan_out: fan_sink.clone(),
                subagents: Arc::clone(&sink_subagents),
            }) as Arc<dyn EventSink>)
        });

        let manager = Arc::new(WorkerManager::new(factory, on_crash, on_event, sink_for));
        // Fill the late-bound slot FIRST, before anything else can observe the
        // daemon (and before the `Arc<Daemon>` even exists): the only window in
        // which `on_crash` cannot reach the manager is between
        // `WorkerManager::new` and this line, and no worker can exist yet.
        let _ = manager_slot.set(Arc::downgrade(&manager));
        let daemon = Arc::new(Self {
            manager,
            persister: Arc::clone(&persister),
            db,
            fan_out,
            subagents,
            starting,
            attached_sessions,
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

    /// Claim a session id for a `start`, or explain why it cannot be claimed.
    ///
    /// Takes the `starting` lock and does NOT release it — the returned
    /// [`SessionClaim`] is a guard that drops the id on the way out, so the
    /// check and the claim cover the whole `attach` await (finding F). The lock
    /// itself is only ever held for the synchronous work below, never across an
    /// await: one `start` waiting 5 s for `ready` must not be able to stall
    /// another client's `start` for a different session.
    ///
    /// Reclaims a STALE registration instead of refusing it (finding E): the
    /// entry is `detach`ed, which removes it from the map and runs the engine's
    /// reap on the already-dead handle (`close` is a write to a closed stdin,
    /// the grace is bounded, and the `kill` is a no-op on a reaped child — see
    /// `childproc::kill_process_group`'s discarded `ESRCH`).
    fn claim_session(&self, session_id: &str) -> Result<SessionClaim, String> {
        // ── The session bound (issue #4) ─────────────────────────────────────
        //
        // Counted HERE, under the `starting` lock, rather than after `attach`
        // returns. Two reasons, and the second is the one that matters:
        //
        // 1. Check-and-increment under ONE lock means N simultaneous `start`s at
        //    the bound cannot all pass the check and all land. With the increment
        //    after the ~5 s `attach` await, they could — the bound would be
        //    overshootable by exactly the number of clients that dialed in the
        //    same instant.
        // 2. It makes the increment precede any `on_crash` this session can cause:
        //    the child does not exist until `attach` spawns it, so no crash can be
        //    reported for it before this line. (A `close` from ANOTHER connection can
        //    still land between `attach`'s registration and this claim's `commit` —
        //    see [`Daemon::attached_sessions`] for why that off-by-one is the safe
        //    direction and why closing it is not worth the lock.) What this ordering
        //    buys is that the counter never goes NEGATIVE by ordering: the one thing
        //    that would matter, because an `AtomicUsize` at zero does not clamp and
        //    does not panic, it WRAPS to `usize::MAX`, which is a session limit that
        //    refuses every `start` for the rest of the daemon's life.
        //
        // The counter therefore means "sessions the daemon is committed to running",
        // not "sessions with a live handle" — the more protective reading, and the
        // relevant one, since the threat is a flood of `start`s. A claim that never
        // becomes a session (a bad `--worker-exe`, a refused row write) gives the
        // slot back when the guard drops; see [`SessionClaim`].
        let mut starting = self.starting.lock().unwrap_or_else(|p| p.into_inner());
        // An id already in flight, or already live, is refused below with its OWN
        // message, and must not be charged a slot it already holds. Those two
        // messages are load-bearing for existing tests: the race test asserts the
        // loser's error is a string, and the crash-recovery test asserts a live
        // session answers "already" — neither may become the limit message.
        // (`handle_for` is called under this lock by the guard further down too —
        // the engine's accessor does not take this mutex, so holding it here is the
        // existing shape, not a nesting hazard.)
        let re_claim =
            starting.contains(session_id) || self.manager.handle_for(session_id).is_some();
        if !re_claim {
            let live = self
                .attached_sessions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if live >= SESSION_LIMIT {
                // Undo the reservation: this `start` was refused, so it holds
                // nothing. (`fetch_add` then `fetch_sub` rather than a CAS loop
                // because the lock above already makes this the only writer for
                // this id, and the counter is allowed to be transiently one high.)
                self.attached_sessions
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return Err(format!(
                    "session limit of {SESSION_LIMIT} reached (a session already \
                     started must close first)"
                ));
            }
        }
        if starting.contains(session_id) {
            return Err(format!("session {session_id} is already starting"));
        }
        // A live child is refused. `handle_for` is the engine's production
        // accessor (the desktop's relays route through it), not a test view, and
        // for a MAIN session it is a faithful liveness signal: the only way an
        // entry survives its child is a crash the pump has not reported yet, and
        // `on_crash` clears the entry synchronously (see its step-1 note —
        // measured, twice, against a real `kill -9`).
        //
        // The residual window is the child's death → the pump's `Exited` → this
        // closure, during which a `start` reads "already running" about a child
        // that just died. It is milliseconds wide, it is self-healing on the
        // client's next retry, and the `dead` set this file used to carry did
        // not close it either (its insert happens in the same callback chain).
        //
        // `handle_for` also resolves the subagent `drives` map, so a client that
        // reuses a live CHILD's id lands here too and is refused. That is the
        // behaviour both before and after the `dead` set's removal — the set was
        // only ever populated for `!is_subagent` exits, so it could not have
        // distinguished the two — and refusing is the safe direction: the
        // alternative would let a client `attach` over a running subagent and
        // orphan its process.
        if self.manager.handle_for(session_id).is_some() {
            return Err(format!("session {session_id} is already running"));
        }
        self.prune_subagents();
        starting.insert(session_id.to_string());
        Ok(SessionClaim {
            starting: Arc::clone(&self.starting),
            session_id: session_id.to_string(),
            committed: false,
            attached_sessions: Arc::clone(&self.attached_sessions),
        })
    }

    /// Drop tombstoned subagents from the child→parent map (see
    /// [`Daemon::subagents`] for why closing is a tombstone and not a removal).
    fn prune_subagents(&self) {
        let mut subagents = self.subagents.lock().unwrap_or_else(|p| p.into_inner());
        if subagents.is_empty() {
            return;
        }
        subagents.retain(|_, record| !record.closed);
    }

    /// Write the `sessions` row at `start`, before the Worker exists.    ///
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

/// A claimed session id, for the duration of one `start`.
///
/// `Drop` releases the claim, so every exit path from `start` — the row-write
/// failure, the `attach` failure, the success — gives the id back. An RAII guard
/// rather than an explicit remove at each site, because the bug this prevents is
/// precisely the one where a future early-return forgets the cleanup and leaves
/// an id permanently un-startable, which is the same class of defect finding E
/// reports.
struct SessionClaim {
    starting: Arc<StdMutex<HashSet<String>>>,
    session_id: String,
    /// Whether `start` has taken responsibility for the slot this claim took.
    /// `false` at creation: the slot belongs to the CLAIM, so `Drop` gives it back.
    /// [`SessionClaim::commit`] hands it to the SESSION, after which `close` /
    /// `on_crash` are the only things that release it — which is the pairing the
    /// counter's correctness rests on.
    ///
    /// The default is deliberately the conservative one: a `start` that returns
    /// early (a malformed envelope's row write failing, an `attach` that times out)
    /// releases the slot automatically, because forgetting to is the exact bug this
    /// guard exists to prevent.
    committed: bool,
    attached_sessions: Arc<std::sync::atomic::AtomicUsize>,
}

impl SessionClaim {
    /// Hand the session slot from this claim to the session (issue #4). Called
    /// once, by [`start`], when `attach` returns `Ok` — the moment the slot stops
    /// being "a `start` in flight" and becomes "a running session".
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for SessionClaim {
    fn drop(&mut self) {
        self.starting
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.session_id);
        if !self.committed {
            self.attached_sessions
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// The crash report's frame (finding E's sibling: the `isSubagent` label).
///
/// Extracted from `on_crash` so the label is testable — the pump fires `on_crash`
/// with only an id, an exit code and a pid, so the flag is entirely the
/// daemon's, and the fixture cannot be made to dispatch a subagent that then
/// dies (its dispatch task text is fixed in the frozen engine).
///
/// A child's death is published under its PARENT's id with `isSubagent: true` —
/// the same re-target the pump applies to every other frame a child emits, and
/// the only way this frame reaches a socket AT ALL: `watched` holds the ids a
/// client `start`ed, which are always parents, so a frame keyed to a minted child
/// id is filtered out and the crash is invisible. Fixing the flag without the id
/// would produce a correct label on a frame nobody receives. The child's own id
/// rides in the frame body so a client tracking its children can still tell
/// WHICH one died.
fn crash_report_frame(
    subagents: &StdMutex<HashMap<String, SubagentRecord>>,
    session_id: &str,
    code: Option<i32>,
    pid: Option<u32>,
) -> serde_json::Value {
    let record = subagents
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(session_id)
        .cloned();
    let (delivery_id, is_subagent, child_id) = match record {
        Some(record) => (record.parent, true, Some(session_id.to_string())),
        None => (session_id.to_string(), false, None),
    };
    serde_json::json!({
        "type": "event",
        "sessionId": delivery_id,
        "isSubagent": is_subagent,
        "frame": {
            "type": "worker-crashed",
            "exitCode": code,
            "pid": pid,
            "subagentSessionId": child_id,
        },
    })
}

/// The pump's persistence half of `on_event`, extracted so the routing decision
/// below is callable from a test without a live Worker.
///
/// Both calls are synchronous and brief — one statement each — which is what
/// makes calling them from the pump task acceptable even though the store is a
/// `std::sync::Mutex` (see `db/mod.rs`: never hold the guard across an await,
/// and nothing here does).
fn persist_frame(
    persist: &Arc<TranscriptPersister>,
    session_id: &str,
    is_subagent: bool,
    evt: &WorkerInboundEvent,
) {
    match evt {
        WorkerInboundEvent::Store(frame) => persist.apply(is_subagent, frame),
        // The guard is the filter: every OTHER `SinkFrame` event name is
        // the desktop's rendering concern, not a row this store owns.
        WorkerInboundEvent::SinkFrame { event, payload } if event == "interactive-event" => {
            let Some(inner) = payload.get("event").and_then(serde_json::Value::as_str) else {
                return;
            };
            // A subagent's todo list is its PARENT's UI's rendering of the
            // delegation, not a row the child owns; `cost_update` is the one
            // event that carries money, and a child's spend is its OWN money.
            // Mirrors the desktop's gate (`router.rs`:
            // `!is_subagent || event == "cost_update"`).
            if is_subagent && inner != "cost_update" {
                return;
            }
            // WHICH row owns the frame. The pump re-targets a subagent's frames
            // to the PARENT's delivery id so the child's UI lives in the
            // parent's scope (`pump.rs`'s `delivery_id`), but the ROW belongs to
            // the CHILD — the same distinction `TranscriptPersister::apply`
            // draws for `StoreFrame`s, and load-bearing here: `record_session_cost`
            // is a SNAPSHOT `UPDATE` that OVERWRITES, so writing a child's total
            // to the delivery id does not merely misplace it, it REPLACES the
            // parent's own figure. The parent would report its child's spend as
            // its own.
            //
            // The payload's `sessionId` is the emitting loop's own id (pinned
            // loop-side by `a_priced_call_emits_one_cost_update_frame_carrying_that_calls_cost`),
            // so it is the child's id for a child frame. A MAIN session's frames
            // — including the one `fold_child_cost` emits when a child returns —
            // arrive with `is_subagent: false` and take the `else`: the parent's
            // row, written by ONE producer.
            let target = if is_subagent {
                match payload.get("sessionId").and_then(serde_json::Value::as_str) {
                    Some(id) => id,
                    // SKIP, do not fall back. The desktop's twin of this branch
                    // falls back to the delivery id and its own comment calls
                    // that fallback "DELIBERATELY UNREACHABLE … Do not read it
                    // as a safe default — it writes the child's money to the
                    // PARENT's row, which is precisely the double-count the
                    // branch above exists to prevent … this must SKIP the write
                    // (a frame with no owner is unpersistable), NOT fall back."
                    // The desktop keeps the dead fallback because a loop-side
                    // pin makes it unreachable; new code has no reason to copy
                    // a default its own author documents as wrong. Skipping
                    // costs one frame and is loud; falling back is silent
                    // corruption of a row that never earned it.
                    None => {
                        eprintln!(
                            "fulcrum serve: dropping a subagent `{inner}` frame delivered \
                             under {session_id} with no sessionId of its own (unpersistable)"
                        );
                        return;
                    }
                }
            } else {
                session_id
            };
            persist.apply_interactive_event(target, is_subagent, payload);
        }
        _ => {}
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
///
/// It also MINTS the daemon's child→parent knowledge: the engine's
/// `subagent-session-started` payload names both ids, and this is the only seam
/// where the daemon sees it. That map is what lets `on_crash` label a child's
/// death correctly (see [`Daemon::subagents`]).
struct FanOutSink {
    session_id: String,
    fan_out: tokio::sync::broadcast::Sender<serde_json::Value>,
    /// Child session id → parent session id (`closed` is the tombstone flag;
    /// see [`Daemon::subagents`]).
    subagents: Arc<StdMutex<HashMap<String, SubagentRecord>>>,
}

impl EventSink for FanOutSink {
    fn emit(&self, event: &str, payload: serde_json::Value) {
        // Learn / retire the child↔parent pairing from the engine's own
        // lifecycle frames. `subagent-session-started` carries `sessionId` (the
        // child) and `parentSessionId`; `subagent-closed` carries the child's id
        // only. Retirement is a TOMBSTONE, not a removal — the pump publishes
        // the drive's exit watch before it calls `on_crash`, and this frame is
        // emitted from the drive task's teardown on another thread, so removing
        // here could erase the label the crash report is about to read.
        match event {
            "subagent-session-started" => {
                if let (Some(child), Some(parent)) = (
                    payload.get("sessionId").and_then(serde_json::Value::as_str),
                    payload
                        .get("parentSessionId")
                        .and_then(serde_json::Value::as_str),
                ) {
                    self.subagents
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .insert(
                            child.to_string(),
                            SubagentRecord {
                                parent: parent.to_string(),
                                closed: false,
                            },
                        );
                }
            }
            "subagent-closed" => {
                if let Some(child) = payload.get("sessionId").and_then(serde_json::Value::as_str) {
                    if let Some(record) = self
                        .subagents
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .get_mut(child)
                    {
                        record.closed = true;
                    }
                }
            }
            _ => {}
        }
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
            // A refused peer (issue #4) is counted rather than logged per
            // attempt. A hostile LOCAL process can dial this socket in a loop —
            // it needs no credentials to try — and one `eprintln!` per attempt
            // would hand it the operator's stderr as the DoS surface. The first
            // three rejections print (an operator's own mistake must be visible
            // immediately); after that every 1024th, so the counter stays
            // discoverable without being the attack's amplifier.
            //
            // Note this is NOT the second-daemon refusal: that one fails at BIND
            // time (`reclaim_stale_socket`'s probe) and never reaches this loop.
            // Its probe connection lands here as an ordinary EOF-with-no-frame
            // peer, which `serve_client` already treats silently.
            Err(TransportError::PeerIdentity {
                peer_uid,
                expected_uid,
            }) => {
                let seen =
                    REJECTED_FOREIGN_PEERS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                if seen <= 3 || seen.is_multiple_of(1024) {
                    eprintln!(
                        "fulcrum serve: rejected a peer (uid {peer_uid}, expected \
                         {expected_uid}) — {seen} such rejection(s) so far"
                    );
                }
            }
            // An `accept` failure is a per-connection condition (a descriptor
            // limit, a client that vanished between `poll` and `accept`), not a
            // reason to stop supervising the sessions already running. The loop
            // reports and continues.
            Err(e) => eprintln!("fulcrum serve: accept failed: {e}"),
        }
    }
}

/// How many peers have been refused for not being this daemon's uid (issue #4).
///
/// A process-global counter rather than per-listener state because there is one
/// accept loop per process and the number is an operator-facing total; it is never
/// reset, which is what makes the "every 1024th" reporting schedule stay flat
/// instead of restarting its flood potential after each quiet spell.
static REJECTED_FOREIGN_PEERS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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

/// One client: TWO tasks — one reads and answers, one owns the socket's writes.
///
/// Issue #3's fix. `serve_client` used to be ONE task running ONE `select!`, with
/// `handle_frame` awaited INLINE in its inbound arm — and a `start` inside
/// `handle_frame` awaits `WorkerManager::attach`, the engine's 5 s `ready` bound. For
/// that window the task was not in its `select!` at all, so this connection's own
/// fan-out arm could not run: a client that started a session while its other session
/// was streaming saw its own events only after the start finished. The two halves are
/// tasks of their own now, so the relay runs while the reader is parked.
///
/// # Why the split is safe at all, and why TWO tasks rather than one with a mutex
///
/// A JSONL stream cannot resynchronise once two writers interleave a half-written
/// line, so the tail of a frame the OS only partly took (`pending`) must have exactly
/// one owner. That exclusivity is what `transport::frame` provides: `ConnectionWriter`
/// owns `pending` and is the only type with a write method, and
/// `Connection::split_writer` hands it out exactly once. So the writer task holds the
/// only write handle by construction — not by discipline, and not by wrapping a
/// `Mutex` around a `Connection`, which would make the desync merely unlikely instead
/// of unrepresentable. `tests/transport.rs`'s `a_shared_writer_under_a_mutex_never_interleaves_frames`
/// is the pin that reddens the mutex-shaped regression.
///
/// * [`reader_loop`] — the reader half of the `Connection`, the `watching` set,
///   issue #4's hardening state (`Handshake`, `FrameRateGate`, `seen`), the reply side
///   of the outbound queue, and the two protocol-gate refusals.
/// * [`writer_loop`] — the `ConnectionWriter`, the fan-out subscription, the drain,
///   and the refusal sender. THE RELAY LIVES HERE, and that placement is the whole
///   point: only a task that never calls `handle_frame` can carry it through a slow
///   `start`.
///
/// The two share exactly two pieces of per-connection state, `watching` and
/// `dropped`, both behind an `Arc<StdMutex<..>>` (the `Daemon::subagents` /
/// `Daemon::starting` precedent). NO guard is ever held across an `await` — see each
/// loop's notes — because a `std::sync::MutexGuard` is `!Send`, which a spawned future
/// cannot survive, and holding one across `attach` would re-create the very stall this
/// split removes.
///
/// # The `gap` ordering contract, which the split changes deliberately
///
/// One task serialised every `send_frame`, so a `gap` notice could not interleave into
/// the reply stream. Two producers into one queue can, and the contract is now stated
/// instead of inherited:
///
/// * A `gap` may appear anywhere in the stream, INCLUDING before the very reply whose
///   drop it reports — the drain announces `dropped` from the queue head when it wakes
///   with a non-zero count, and a reply enqueued just before that wake races the
///   announcement.
/// * A `gap` never precedes a frame that predates the drops it reports: the queue is
///   FIFO, so a prelude can only leak drops that happened before this drain wake.
/// * The fan-out's `Lagged` arm still emits its `gap` as an ordinary queued frame (not
///   folded into `dropped`), so it shares the queue with replies.
///
/// The shape (`{"type":"gap","skipped":N}`, no `sessionId`), the counts, and both
/// buffer bounds are unchanged. No test in `tests/serve.rs` depended on
/// reply-before-event ordering — the client harness matches frames by predicate, never
/// by position — and `the_gap_drain_prelude_uses_the_current_drop_count` pins the
/// prelude's new counting rule directly.
///
/// # Teardown: drop-based, and the writer is `abort()`ed, never joined
///
/// `ConnectionWriter::send_frame` loops `write(&self.pending)` with no timeout, so a
/// peer that stops reading parks it — and while the writer task sits inside that
/// future, NONE of its `select!` arms is pollable, so no message and no stop one-shot
/// can reach it. Awaiting its `JoinHandle` would therefore hang forever. `serve_client`
/// keeps the handle and `abort()`s it once the reader returns: aborting (not joining)
/// makes the parked write-loop's drop SYNCHRONOUS, so no `Drop`-time `flush` is ever
/// awaited (a flush on a half-closed socket would itself hang). That abort is the ONLY
/// mechanism carrying "no task outlives `serve_client`" — dropping the handle would
/// orphan the parked writer, and `tokio::io::split`'s halves share an `Arc` with no
/// `Drop` impl, so dropping one half closes nothing while the other lives.
///
/// What the writer does on its OWN exits is `ConnectionWriter::shutdown` — the single
/// place the daemon reaches `poll_shutdown`. There is no reader-side close and no
/// raw-byte drain of any kind: draining the read half conveys nothing to the write
/// direction (`ReadHalf::poll_read` never calls `poll_shutdown`), and a close at
/// `serve_client`'s START would shut the daemon's own write direction before the
/// writer had put one frame on the wire.
///
/// # What is deliberately NOT here
///
/// No `detach`, no `kill`, no session cleanup when the connection goes: dropping the
/// subscription is the whole teardown and the sessions keep running and persisting.
/// `tests/serve.rs`'s durability test is what keeps that true, and its `REAP_GRACE`
/// window is why a wrong teardown cannot hide (`detach` does not kill — it runs
/// `reap_worker`, a `close` then a 2 s grace then `kill`, so a wrong one looks like a
/// child that dies 2 s LATE rather than one that dies at once).
async fn serve_client(daemon: Arc<Daemon>, connection: Connection) {
    // The frames this connection asked to see. Populated by `start`; a connection
    // that never started anything sees nothing, which is the point of filtering by
    // session rather than broadcasting everything to everyone.
    //
    // SHARED because the writer's relay arm has to read it while the reader's
    // `handle_frame` owns it. The READER is the only writer of this set (see
    // `reader_loop`'s mirror note); the writer only ever filters on a snapshot of it.
    // `StdMutex` rather than `tokio::sync::Mutex` for that reason: the reader must not
    // hold this across `attach`, and a `tokio` mutex would make the wrong shape
    // (`lock().await`) look natural.
    let watching: Arc<StdMutex<HashSet<String>>> = Arc::new(StdMutex::new(HashSet::new()));
    // Frames dropped for THIS connection since the last `gap` it was told about.
    //
    // Kept out of the queue because the queue is the thing that is full: an
    // announcement of a drop cannot be allowed to fail for lack of room, and it cannot
    // be sent inline either (the task that notices is not the task that owns the write
    // half). So the count waits for the drain to free a slot, and the notice goes out
    // with the next frame the client actually gets.
    //
    // `Arc` rather than a local because it OUTLIVES BOTH TASKS: the writer's relay arm
    // increments it, the writer's drain announces it, and `serve_client` reads it after
    // the abort to log what the client was never told. That last reader of it is the
    // reason it cannot simply live inside `writer_loop` — an aborted task runs no code
    // after its loop, so the notice would vanish on exactly the parked-writer path it
    // exists to report.
    let dropped: Arc<StdMutex<u64>> = Arc::new(StdMutex::new(0));
    // The outbound queue (replies + relayed events), BOUNDED — see
    // [`CLIENT_QUEUE_CAPACITY`] for what it is sized against and for why the
    // "unbounded so `send` is infallible" reasoning that replaced the bound originally
    // was the defect. Every push below is a `try_send` via [`enqueue_shared`], and the
    // full case is handled the way the fan-out's lag case already was: drop, count,
    // announce, keep the connection.
    //
    // `Sender` is `Clone`, so BOTH tasks hold one: the reader (replies) and the writer
    // (the relay, and the `Lagged` gap). That is deliberate, and it has a teardown
    // consequence worth naming — the channel can never close because ONE task dropped
    // its handle, so the writer's `recv() == None` exit is driven specifically by the
    // READER's sender going away.
    let (tx, queued) = tokio::sync::mpsc::channel::<serde_json::Value>(CLIENT_QUEUE_CAPACITY);

    // Issue #4's protocol-gate refusal, across the task boundary. TWO one-shots,
    // because the reader's guarantee is "told ONCE, then closed" while closing is
    // the reader's job and writing is the writer's:
    //
    //   reader: refuse_tx.send(frame); ack_rx.await; break
    //   writer: refuse_rx fires → send_frame(frame) → ack_tx.send(()) → exit
    //
    // So the refusal is really on the socket before the connection ends, which is what
    // the old inline `send_frame(…).await; break;` did. If the writer is already gone,
    // `send` returns `Err(frame)` and the reader's `ack_rx.await` yields
    // `Err(Canceled)` — the same break either way, just without the frame. And a peer
    // that neither reads nor closes parks the writer's refusal send, which parks the
    // reader's ack await too: identical to the old inline behaviour, and not a
    // teardown case (the connection never ends; the test suite's daemon kill closes
    // every socket).
    let (refuse_tx, refuse_rx) = tokio::sync::oneshot::channel::<serde_json::Value>();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();

    // SPLIT FIRST, then spawn: `writer_loop` must receive its half already moved, and
    // `split_writer` consumes the `Connection`, so what the reader task gets is the
    // reader half. This ordering is also what makes "exactly one writer" checkable by
    // reading the types — after this line, `connection` has no write method at all.
    let (reader_conn, writer) = connection.split_writer();

    let reader = tokio::spawn(reader_loop(
        Arc::clone(&daemon),
        reader_conn,
        tx.clone(),
        Arc::clone(&watching),
        refuse_tx,
        ack_rx,
    ));
    let writer_task = tokio::spawn(writer_loop(
        writer,
        queued,
        tx,
        daemon.fan_out.subscribe(),
        SharedConnectionState {
            watching,
            dropped: Arc::clone(&dropped),
        },
        refuse_rx,
        ack_tx,
    ));

    // The reader's whole life-span. `await` and NOT `select!`: the inbound channel is
    // this task's only input, which is exactly why the split does not need the relay to
    // share its loop. This parks for up to `attach`'s 5 s on a `start`, and that is the
    // bounded per-connection stall issue #3 accepts — the difference from before is
    // that the relay is no longer hostage to it.
    //
    // `expect` rather than a match: `reader_loop` has no error path, so the only way
    // this fails is a panic, and a panic here is a bug worth a backtrace. The writer is
    // still aborted below on that path, so a panicking reader cannot leak its writer
    // either, and `run`'s `log_on_join_error` reports the panic itself.
    reader.await.expect("reader_loop must not panic");

    // The backstop, and the ONLY thing that ends a writer parked inside `send_frame`:
    // a peer that closed its inbound half but left the write direction open gives the
    // writer no error to notice, and while it sits in that write loop it cannot poll
    // its own exit arms. `abort` — never `join`, the shape that can never resolve (see
    // this function's teardown note). It makes the parked future's drop synchronous, so
    // no `Drop`-time `flush` on a half-closed socket is ever awaited. A no-op for a
    // writer that already exited, which is why it is unconditional: every path out of
    // `reader.await` takes it, and "no task outlives `serve_client`" is mechanical
    // rather than a promise. T1's process census on this branch is what reddens if it
    // is ever removed.
    writer_task.abort();

    // The WRITER's exit epilogue lives here rather than inside `writer_loop`, and the
    // reason is the abort itself: cancelling a task drops its future, so an epilogue
    // written as code AFTER its `loop` would be skipped on exactly the one path that
    // needs it most — a writer parked in `send_frame` for a peer that stopped reading is
    // precisely a client that fell behind and was never told. `dropped` is an `Arc` for
    // this reason as much as for the sharing.
    //
    // One accepted imprecision, because it is the kind that gets "fixed" into a bug:
    // `abort` is a REQUEST, and a writer already running on another worker may reach its
    // next yield point after this read. So this line can under-report by whatever the
    // writer drops in those last microseconds. That is the right trade for a log line —
    // the alternative is awaiting the writer, which is the deadlock this whole design
    // exists to avoid. It is NOT a correctness surface: the frames are already gone
    // either way, and the queue's own bound is what limits them.
    let still_dropped = *dropped.lock().unwrap_or_else(|p| p.into_inner());
    if still_dropped > 0 {
        // The drops it never heard about are worth one line: without it, an operator
        // debugging "the UI stopped mid-answer" has no evidence the daemon ever told
        // that client it had fallen behind.
        eprintln!(
            "fulcrum serve: a client disconnected {still_dropped} frame(s) behind \
             (it was never told)"
        );
    }

    // `serve_client` returns; its `Arc<Daemon>` refcount drops. No stop one-shot, no
    // join, no timeout — every shape that awaited the writer deadlocks against a peer
    // that stopped reading, which is an ordinary client, not an attack.
}

/// The READER half of a split `serve_client`: the peer's frames, the gates, the
/// replies. Never the socket.
///
/// Owns the reader half of the `Connection` and all three of issue #4's
/// per-connection locals (`Handshake`, `FrameRateGate`, `seen`) — the state only the
/// half that SEES inbound frames can meaningfully hold. The writer never reads any of
/// it.
///
/// # The `watching` mirror, and why it is not a lock across the await
///
/// `handle_frame` takes `&mut HashSet<String>` and awaits `attach` for up to 5 s
/// inside `start`. Handing it `&mut *watching.lock()` would hold a
/// `std::sync::MutexGuard` across that await, which fails to compile in a spawned
/// future (`MutexGuard` is `!Send`) and, if it could compile, would let a `start`
/// freeze the writer's relay filter for the same 5 s — the exact stall this split
/// exists to remove.
///
/// So the set is OWNED here and the shared `Arc<StdMutex<..>>` is a MIRROR the writer
/// filters on: this task is the only writer of either copy, and it publishes its
/// private copy after each `handle_frame` returns. Publishing unconditionally (rather
/// than only on `start`/`close`) is deliberate — it costs one clone of a set capped at
/// [`SESSION_LIMIT`] ids per inbound frame, and it buys the removal of a "did the
/// caller remember to publish" hazard that would otherwise silently drop a session's
/// frames forever.
///
/// Publishing AFTER the await is also the correct ordering, not merely the permitted
/// one: `start` inserts only after `attach` succeeds, so a session with no worker
/// cannot have any fan-out frames to miss; and the publish happens before the
/// `start-result` is enqueued, so a client can never be told a session is live before
/// this daemon's relay filter knows about it.
/// # Why a reply WAITS for room, when the old code dropped one
///
/// This loop is the one place the split had to change a rule instead of moving it, and
/// it is worth understanding before "simplifying" it back to `enqueue`:
///
/// The old single task both produced and drained this queue. Its drain arm awaited a
/// blocked `send_frame`, so while a peer stopped reading, the `select!` was not
/// re-polled and the inbound arm could not run — the head-of-line stall that caused
/// issue #3 was ALSO what throttled a flood. That is measured, and the number is the
/// suite's: `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat` records
/// 1 375 frames accepted before the peer's own `write` blocked.
///
/// Removing the stall removes that side effect. With a free-running reader, the same
/// peer drove **679 060 frames in 5 s** and the daemon's heap went with it — not because
/// a buffer grew, but because the reader happily decoded a million requests in order to
/// `try_send` a million replies into a full queue and DROP them. Dropping is the right
/// answer for a relayed event a slow client is not reading; it is a bad answer for the
/// reply to a request nobody asked to have dropped, and it is also what un-throttles the
/// inbound edge, since a reader that drops never stops reading.
///
/// So replies go through `send().await`: a reader that cannot enqueue waits, the inbound
/// queue fills, the socket fills, and the PEER'S OWN `write` is what blocks — which is
/// the backpressure design this file has always claimed, restored on the correct edge.
/// The relay arm in [`writer_loop`] keeps `enqueue`'s drop-and-count-and-`gap` behaviour,
/// because a streaming client that cannot keep up is exactly what that path is for.
///
/// The cost is honest and bounded: a peer that stops reading parks the reader here
/// instead of in `send_frame`, so `serve_client` cannot reach its `abort()` until that
/// peer closes. That is the SAME stall the single task had (and the plan's teardown note
/// already accepts for the writer: an open-but-unreading peer is backpressure, not a
/// teardown case — the connection has not ended). Every real close unblocks both tasks:
/// the peer's close makes the writer's `send_frame` fail, the writer's exit drops its
/// `Sender`, and a `send` on a channel whose receiver is gone returns `Err`, which this
/// loop treats as "the peer is gone" and breaks on.
async fn reader_loop(
    daemon: Arc<Daemon>,
    mut connection: Connection,
    tx: tokio::sync::mpsc::Sender<serde_json::Value>,
    watching: Arc<StdMutex<HashSet<String>>>,
    refuse_tx: tokio::sync::oneshot::Sender<serde_json::Value>,
    ack_rx: tokio::sync::oneshot::Receiver<()>,
) {
    // The authoritative copy (see this function's mirror note). `handle_frame`'s
    // signature is unchanged, so the `handle_frame`/`start`/`close` unit tests keep
    // calling it with a plain `HashSet` exactly as before.
    let mut watching_set: HashSet<String> = HashSet::new();

    // Frames seen from this peer, so the exit path can tell "a probe that connected
    // and hung up" from "a client that talked to us and left". READER-half state: it
    // counts INBOUND frames, and the writer never sees a frame the reader refused.
    let mut seen = 0usize;

    // ── The per-connection hardening state (issue #4) ───────────────────────
    //
    // Both are LOCALS on purpose: a handshake and a frame rate are properties of ONE
    // socket, so they belong to the one task that reads it, not to `Daemon` (which
    // would make a hostile client's counters a shared resource — the DoS shape this
    // file keeps re-learning).
    //
    // Starts WAITING: a connection that sends nothing (the stale-socket probe, a
    // `curl`-style liveness check, `wait_until_listening`) never reaches the gate at
    // all — `recv` returns `None` and the loop breaks on the first iteration, so the
    // `seen == 0` no-log rule below still fires and the probe stays invisible.
    let mut handshake = Handshake::WaitingHello { pre_hello: 0 };
    let mut rate = FrameRateGate::new(std::time::Instant::now());

    loop {
        // No `select!` and no timeout: the inbound channel is this task's only input,
        // and the relay it used to share a `select!` with is in the writer now.
        let Some(frame) = connection.recv().await else {
            break; // EOF — the peer is gone. No drain: see `serve_client`'s teardown.
        };
        seen += 1;
        let frame = match frame {
            Ok(frame) => frame,
            // A malformed line is the peer's bug, not our reason to hang up
            // (`transport::frame` keeps the stream alive for the same reason the
            // worker protocol's decoder is permissive).
            Err(e) => {
                // `send().await`, not `enqueue`'s `try_send` — see this loop's "Why a
                // reply WAITS for room". A malformed line is still the peer's bug and
                // not our reason to hang up; it is only our reason to answer, and an
                // answer the peer is not reading is a reason to stop reading.
                if tx
                    .send(serde_json::json!({
                        "type": "error",
                        "message": format!("malformed frame: {e}"),
                    }))
                    .await
                    .is_err()
                {
                    break; // the writer is gone; nothing can reach this peer any more.
                }
                continue;
            }
        };
        // ── The required handshake (issue #4) ────────────────────────────────
        //
        // Decode FIRST (above), so a malformed line keeps its existing
        // answer-and-continue path and is charged to neither gate: a peer's JSON typo
        // is not our reason to hang up, and it is not evidence of an attack either.
        //
        // `hello` itself is NOT a grant of authority — it is a pure greeting, and the
        // rate gate and the session bound still apply after it. What this gate decides
        // is only whether the peer is allowed to ask for anything yet.
        let is_hello = frame.get("type").and_then(serde_json::Value::as_str) == Some("hello");
        // The state transition is decided first and APPLIED after, because the arm
        // that sets `Done` cannot also hold the `&mut` that read `pre_hello` — and
        // because the two outcomes ("drop it" / "close it") have to escape this block
        // to reach the `continue`/`break` below, where only the loop's own control flow
        // can end the iteration.
        let mut drop_pre_hello = false;
        let mut refuse_pre_hello = false;
        match &mut handshake {
            Handshake::Done => {}
            Handshake::WaitingHello { pre_hello } => {
                if is_hello {
                    handshake = Handshake::Done; // `handle_frame` answers it below.
                } else {
                    *pre_hello += 1;
                    if *pre_hello > MAX_PRE_HELLO_FRAMES {
                        refuse_pre_hello = true;
                    } else {
                        drop_pre_hello = true;
                    }
                }
            }
        }
        if refuse_pre_hello {
            // Told ONCE, then closed. The refusal goes to the writer rather than to
            // this task's own `send_frame`, because after `split_writer` this half has
            // no write path — and the ack await is what keeps "told ONCE, THEN closed"
            // true across the task boundary instead of letting the reader tear the
            // connection down while the refusal is still unwritten.
            //
            // Not `enqueue`: the queue's drain exists for frames a client has time to
            // receive, and a peer that skipped the handshake is about to be hung up on.
            // One refusal, because a peer that sends a hundred pre-hello frames does
            // not get a hundred lectures.
            eprintln!(
                "fulcrum serve: a peer sent more than {MAX_PRE_HELLO_FRAMES} \
                 frame(s) before `hello`; closing the connection"
            );
            let _ = refuse_tx.send(serde_json::json!({
                "type": "error",
                "message": "this daemon requires a `hello` frame before \
                            any other frame",
            }));
            let _ = ack_rx.await;
            break;
        }
        if drop_pre_hello {
            // Under the limit: counted and DROPPED, no reply. Serving it would mean a
            // peer that never identified itself gets a child process — the exact
            // behaviour this gate was written to remove.
            continue;
        }
        // ── The inbound frame rate (issue #4) ────────────────────────────────
        //
        // NON-greeting frames only, for the reasons in [`FRAME_RATE`]'s doc (which also
        // names the bound that already covers a `hello` flood).
        if !is_hello && !rate.accept(std::time::Instant::now()) {
            eprintln!(
                "fulcrum serve: a peer exceeded the {FRAME_RATE}-frame/s \
                 inbound limit; closing the connection"
            );
            let _ = refuse_tx.send(serde_json::json!({
                "type": "error",
                "message": format!(
                    "frame rate limit exceeded (at most {FRAME_RATE} \
                     non-greeting frames per second per connection)"
                ),
            }));
            let _ = ack_rx.await;
            break;
        }
        // A `start` here still awaits `attach` for up to 5 s, but the relay and the
        // socket are in another task, so this connection keeps streaming while this one
        // is parked.
        let reply = handle_frame(&daemon, &mut watching_set, &frame).await;
        // Publish the mirror, then reply — in that order, and see this function's
        // mirror note for why both halves of that order matter. The lock is taken and
        // released inside the statement, so it spans no await.
        *watching.lock().unwrap_or_else(|p| p.into_inner()) = watching_set.clone();
        if let Some(reply) = reply {
            // `send().await` and NOT `enqueue`'s `try_send`, and this is the one place
            // the split had to CHANGE a rule rather than move it. See this function's
            // "Why a reply WAITS for room" note.
            if tx.send(reply).await.is_err() {
                break; // the writer is gone; nothing can reach this peer any more.
            }
        }
    }

    // The connection is gone. Drop this task's sender BEFORE the epilogue, so the
    // writer's teardown starts now rather than after the log line. That is the whole
    // extent of the reader's teardown: no stop signal (there is no stop channel in this
    // design), no socket close, no drain.
    //
    // `refuse_tx` is deliberately NOT dropped here by name: `oneshot::Sender::send`
    // CONSUMES the sender, so on the refusal path it is already gone, and naming it
    // would be a use-of-moved-value. It drops with this function either way, which
    // cancels an unread refusal a fraction of a microsecond later than the ideal — a
    // refusal the writer is about to send on a connection that is ending anyway.
    drop(tx);

    // What the reader owns at the end is the inbound count, and nothing else. The
    // `dropped` half of the old epilogue lives in `serve_client` (see its note): the
    // writer drops frames for a full queue, but the line has to survive the writer's
    // abort, and only `serve_client` is guaranteed to run after it.
    //
    // A connection that never sent a frame is not even worth a log line: Task 3's
    // stale-socket probe (`transport::unix::reclaim_stale_socket`) lands exactly such a
    // connection in this `accept()` backlog every time someone accidentally starts a
    // second daemon, and a `curl`-style liveness check does the same. Both EOF with no
    // frame. The loop treats them as ordinary, and the `seen == 0` case is the only one
    // that could tempt someone into logging an error — which would train an operator to
    // ignore the log.
    if seen > 0 {
        eprintln!("fulcrum serve: client disconnected after {seen} frame(s)");
    }
}

/// The WRITER half of a split `serve_client`: the relay, the queue drain, and the
/// socket. Never `handle_frame`.
///
/// THE ONLY task that calls `send_frame` — by construction, since `ConnectionWriter`
/// is not `Clone` and its `send_frame` takes `&mut self`. It never awaits
/// `handle_frame`, so `attach`'s 5 s can never park it: while the reader is parked in a
/// `start`, this task keeps relaying the fan-out and draining the queue. That is the
/// whole of issue #3's fix, and it is why the relay lives HERE and not in the reader's
/// loop — an arm of the reader's `select!` would stall for exactly the window the bug
/// is about, which would make the split decorative.
///
/// # Why this task is never joined
///
/// `send_frame` loops `write(&self.pending)` with no timeout. A peer that stops reading
/// parks that loop at the OS send buffer, and while this task is inside that future
/// none of its `select!` arms is pollable — so no message can reach it, and awaiting
/// its `JoinHandle` would hang forever. `serve_client` therefore `abort()`s it after
/// the reader returns. The exits this task CAN reach on its own:
///
/// 1. `send_frame` returns `Err` — the peer is gone (its close, or the socket's final
///    `Arc` drop after the abort).
/// 2. The refusal oneshot fires — issue #4's protocol-gate path: send the one frame,
///    ack the reader so its exit lands after the refusal is on the socket, exit.
/// 3. The fan-out reports `Closed` — the `Daemon`'s own senders are gone, which needs a
///    whole-daemon teardown this daemon does not have yet.
///
/// There is deliberately NO "`queued.recv()` returned `None`" exit, and the reason is
/// worth keeping in mind before adding one: this task holds a `Sender` of its own (the
/// relay arm needs it), and an `mpsc` channel closes only when EVERY sender is gone —
/// so the reader ending cannot make this call return `None` while this task is alive.
/// The plan named that as an exit; it is unreachable in this shape, and the honest
/// consequence is that a writer with nothing to send simply stays parked in
/// `events.recv()` until the abort. That is bounded (the abort follows the reader
/// immediately) and it is the same backpressure-for-its-own-connection behaviour the
/// single-task design had, so nothing here waits on a peer that is not there.
///
/// The exit NO writer path reaches at all is a peer that closed only its INBOUND half
/// and left the write direction open: no error fires, and no arm is pollable. That one
/// is the abort's job, and it is why the abort is unconditional.
///
/// A self-reached exit ends the same way: `ConnectionWriter::shutdown` — the daemon's
/// only `poll_shutdown`, reached only from here, only after `send_frame` has returned,
/// and never from inside the `select!`. The `dropped` epilogue is NOT here, because an
/// aborted task never runs code after its loop; it lives in `serve_client`, after the
/// abort, where it covers every path. See the note at the end of this function.
async fn writer_loop(
    mut writer: ConnectionWriter,
    mut queued: tokio::sync::mpsc::Receiver<serde_json::Value>,
    tx: tokio::sync::mpsc::Sender<serde_json::Value>,
    mut events: tokio::sync::broadcast::Receiver<serde_json::Value>,
    shared: SharedConnectionState,
    mut refuse_rx: tokio::sync::oneshot::Receiver<serde_json::Value>,
    ack_tx: tokio::sync::oneshot::Sender<()>,
) {
    let SharedConnectionState { watching, dropped } = shared;
    loop {
        tokio::select! {
            // The fan-out. Filtered here, not upstream: one buffer for the whole
            // daemon is what keeps this from becoming a registry.
            fan = events.recv() => match fan {
                Ok(frame) => {
                    // The one read of shared per-connection state, and the guard dies
                    // with this `let` statement — before `enqueue_shared`, whose own
                    // lock is a different mutex. A filter holding `watching` across an
                    // await would block the reader's `start` on this task.
                    let wanted = watched(
                        &frame,
                        &watching.lock().unwrap_or_else(|p| p.into_inner()),
                    );
                    if wanted && !enqueue_shared(&tx, frame, &dropped) {
                        break; // this connection's own queue is gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    // The client is slower than the session. Report it on the socket
                    // (a UI that silently lost 400 tokens of answer is worse than one
                    // that knows) and keep going — recovery is T10's replay.
                    eprintln!("fulcrum serve: a client lagged {skipped} frames");
                    // Queued as its own frame, NOT folded into `dropped`: the queue may
                    // well be empty here (the fan-out can lag while this connection's
                    // socket is fine), and `dropped` is only ever announced by the
                    // drain — which does not run on an empty queue. Counting it there
                    // would mean a client that lagged and then went idle was never told
                    // at all.
                    if !enqueue_shared(&tx, gap_frame(skipped), &dropped) {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },

            // The queue, drained to the socket.
            queued_frame = queued.recv() => {
                // Unwrapped FIRST, which differs from the old arm's order on purpose:
                // with every sender gone there is nothing left this connection could be
                // told, so announcing a `gap` first would be writing into a teardown.
                // (See this task's doc for why that state is not reachable while this
                // task holds its own sender — this is defensive, and the count is not
                // lost either way: the epilogue below still logs it.)
                let Some(frame) = queued_frame else { break };
                // Room just appeared, so anything dropped since the last notice can be
                // announced NOW — with the frame the client is about to receive, rather
                // than after a wait that may never come.
                //
                // `take` inside a block, so the guard is gone before either
                // `send_frame`: a parked write can outlive the task holding it, and a
                // `std::sync::Mutex` held across that park would lock the reader's
                // `enqueue` out of a connection that is still nominally being served.
                let still_dropped = {
                    let mut guard = dropped.lock().unwrap_or_else(|p| p.into_inner());
                    std::mem::take(&mut *guard)
                };
                if still_dropped > 0 {
                    eprintln!("fulcrum serve: a client lagged {still_dropped} frames");
                    if send_frame_no_select(&mut writer, &gap_frame(still_dropped)).await.is_err() {
                        break; // the peer is gone (EPIPE / a closed socket).
                    }
                }
                if send_frame_no_select(&mut writer, &frame).await.is_err() {
                    break; // the peer is gone (EPIPE / a closed socket).
                }
            }

            // Issue #4's pre-protocol refusal (see `serve_client`): the one
            // reader→writer control message. `send_frame_no_select`, then the ack, so
            // the reader does not end the connection while the refusal is still
            // unwritten. The send result is deliberately ignored: a refusal that
            // cannot be written means the peer is gone, and the reader is leaving
            // either way — what it waits for is the ACK, not the outcome.
            refused = &mut refuse_rx => {
                // `Ok` carries the refusal frame; `Err` means the reader dropped its
                // sender without asking for a refusal, i.e. it is already gone.
                let Ok(frame) = refused else { break };
                let _ = send_frame_no_select(&mut writer, &frame).await;
                let _ = ack_tx.send(());
                break;
            }
        }
    }

    // Reached ONLY on an exit this task reached by itself. That qualification is the
    // whole point of putting it here rather than in `serve_client`: `abort()` drops this
    // future without polling it, so NOTHING below this loop runs on the parked-writer
    // path — and that path does not need it, because the socket's final `Arc` drop (the
    // reader's `Connection` plus this task's `ConnectionWriter`, both gone once the abort
    // lands) closes the socket outright.
    //
    // The `dropped` epilogue is therefore NOT here; it lives in `serve_client` after the
    // abort, where it runs on every path including this one.
    //
    // The teardown close (the daemon's ONLY `poll_shutdown`): asks the OS to close the
    // write direction, so FUTURE writes fail and the peer sees EOF. Best-effort — an
    // `Err` is ignored, because every path here is already leaving. It does NOT un-park a
    // write already blocked in the OS (that is the peer's close, or the socket's final
    // `Arc` drop after the abort), which is exactly why the abort and not this call is
    // what bounds this task's life.
    writer.shutdown().await;
}

/// The two pieces of per-connection state the reader and the writer must share.
///
/// Grouped into one type because they are one concept — "what the other task needs to
/// know about this connection" — and because passing them as two loose `Arc`s made
/// `writer_loop` an eight-argument function, which is a smell worth naming rather than
/// `allow`ing. There are exactly two of them and there must not be a third without
/// thinking about it: every field here is a lock the OTHER task can see, and a lock held
/// across an await in either task re-creates the head-of-line stall issue #3 was about.
///
/// * `watching` — written by [`reader_loop`] (it owns the authoritative copy and
///   publishes a mirror; see its doc), read by the writer's relay filter.
/// * `dropped` — written by the writer's own relay arm when the queue is full, read by
///   the writer's drain prelude and by `serve_client`'s exit epilogue. The reader does
///   NOT touch it: a reply is never dropped for a full queue, it waits
///   (see [`reader_loop`]'s "Why a reply WAITS for room").
///
/// `Arc` rather than a bare reference because each half lives on its own task, so neither
/// can borrow from the other's frame.
struct SharedConnectionState {
    watching: Arc<StdMutex<HashSet<String>>>,
    dropped: Arc<StdMutex<u64>>,
}

/// Queue one frame for one connection, taking the shared `dropped` count for the
/// duration of the `try_send` (see [`enqueue`], whose contract and signature are
/// unchanged).
///
/// The adapter exists because `enqueue`'s signature is a PIN — both its unit tests
/// drive it directly with a `&mut u64` — while the split makes the counter shared. The
/// lock is taken and released inside this call, so it never spans an `await`.
fn enqueue_shared(
    tx: &tokio::sync::mpsc::Sender<serde_json::Value>,
    frame: serde_json::Value,
    dropped: &StdMutex<u64>,
) -> bool {
    enqueue(
        tx,
        frame,
        &mut dropped.lock().unwrap_or_else(|p| p.into_inner()),
    )
}

/// `ConnectionWriter::send_frame`, named so the writer's `select!` reads honestly: the
/// send is one plain future the selector may drop when a competing arm fires, with its
/// final `flush` (a no-op on an unbuffered socket) inside it rather than a second await
/// the selector cannot reach past.
///
/// Not a replacement for `send_frame` — a thin wrapper that exists to make "nothing
/// inside the `select!` can park this task" visible at the call site.
async fn send_frame_no_select(
    writer: &mut ConnectionWriter,
    frame: &serde_json::Value,
) -> Result<(), TransportError> {
    writer.send_frame(frame).await
}

/// The notice that tells a client it fell behind (finding 7).
///
/// ONE constructor for the two paths that can lose a client frames — the fan-out
/// ring lagging, and this connection's outbound queue being full — because to the
/// client they are one condition: frames it did not get, and a count of them. A
/// second spelling of this object is how a client ends up parsing `skipped` on one
/// path and `count` on the other.
///
/// Addressed to the CONNECTION, not a session: it carries no `sessionId`, which is
/// what lets [`watched`] pass it through to every client. That is also why
/// `two_sessions_on_two_sockets_do_not_see_each_others_events` treats a
/// session-less frame as legitimate rather than as a leak.
fn gap_frame(skipped: u64) -> serde_json::Value {
    serde_json::json!({ "type": "gap", "skipped": skipped })
}

/// Queue one frame for one connection, or count it as dropped when the queue is
/// full (finding 7).
///
/// `try_send` and NOT `send`, and the difference is the fix: `send` is `async`
/// precisely because it waits for room, so awaiting it inside the `select!` would
/// park this task's INBOUND arm — a client that cannot read would then also stop
/// the daemon from reading its `close`, and the daemon would be holding a session
/// open on a client that is not there. `try_send` says "no room" immediately, and
/// the answer to "no room" for a frame the client is not reading is to drop it.
///
/// Returns whether the connection should stay up: `Full` keeps it (a slow client
/// is a slow client, not a banned one), `Closed` ends it. `Closed` cannot happen
/// from an ordinary client on the single-task shape this function was written for,
/// where the receiver lived in the same task; after T4b's split the receiver lives in
/// [`writer_loop`], so `Closed` now means "the writer task is gone" — which is exactly
/// as good a reason to stop, and slightly better observed, since the reader can no
/// longer keep enqueueing into a queue whose drainer has already left. Either way it is
/// reported rather than mapped to `true`: a `true` there would be a silently-ignored
/// invariant.
///
/// `dropped` counts frames THIS connection asked for and did not get. It is
/// reported by the drain arm as a `gap` frame — the same notice the fan-out's lag
/// path emits — because a client that lost replies has to be able to find out.
fn enqueue(
    tx: &tokio::sync::mpsc::Sender<serde_json::Value>,
    frame: serde_json::Value,
    dropped: &mut u64,
) -> bool {
    match tx.try_send(frame) {
        Ok(()) => true,
        // The frame is dropped and named by the count, not logged per-frame: a
        // client 1 000 frames behind would otherwise generate 1 000 lines about
        // being 1 000 frames behind, and the log is the operator's channel.
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            *dropped += 1;
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
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
    // ── The duplicate guard, and why it is not one check ─────────────────────
    //
    // Two separate concerns live here. (a) is settled OUTSIDE this function and
    // is recorded here because it is where a reader looking at the guard would
    // expect to find it; (b) is what the code below actually does.
    //
    // (a) A STALE registration must not read as a running session. The engine
    //     removes a worker from its map only on `detach` / `reap_all` —
    //     `unexpected_exit` READS the `reaping` flag and the registry's single
    //     `remove` sits in `detach_worker` — so a crashed child would stay
    //     registered for the life of the daemon, and "already running" about a
    //     dead process would make this supervisor unable to ever recover a
    //     session: the one job it has. It does not, because the daemon's
    //     `on_crash` clears the entry (`Daemon::new`, step 1 — including the two
    //     experiments that established it is the ONLY mechanism needed, on both
    //     the exit-code and the SIGKILL path). `claim_session` therefore asks
    //     `handle_for` one question and takes its answer at face value.
    //
    // (b) The check must be ATOMIC with the claim. `attach` is a ~5 s await
    //     whose registry `insert` is the only atomic step, so two clients on two
    //     connections could both pass a check made before either insert; the
    //     second `insert` then REPLACES the first's entry and orphans a live
    //     child process — the exact leak this guard's own comment claims to
    //     prevent. Holding the id in `Daemon::starting` across the await closes
    //     the window, and the guard below releases it on every exit path.
    let mut claim = match daemon.claim_session(&session_id) {
        Ok(claim) => claim,
        Err(reason) => return start_result(frame, false, None, Some(reason)),
    };

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
            // Hand the [`SESSION_LIMIT`] slot from the CLAIM to the SESSION
            // (issue #4): from here, `close` (on a `Some` detach) and `on_crash`
            // (for a main session) are the only things that release it. See
            // [`Daemon::attached_sessions`] for why those and no others.
            claim.commit();
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
/// # The invariant this function has to keep: it releases EVERYTHING the daemon
/// # keys by this id
///
/// Nothing here is optional, because the daemon is long-lived and a session id
/// that leaves a trace in a set nobody removes is a trace that outlives every
/// session. What `close` therefore releases, one line each: the connection's
/// `watching` set, the registry entry (via `detach`), and the [`SESSION_LIMIT`]
/// slot (only when that `detach` returned `Some` — see below). `starting` needs
/// nothing — a `start` claim is an RAII guard released on every exit path — and
/// `subagents` is keyed by minted CHILD ids and is pruned on the next `start`.
///
/// This is stated because it was violated once. `Daemon::dead` was a per-session
/// set that `close` did not touch, so every crash-then-close left an entry for
/// the life of the process, and the field's own doc claimed `close` pruned it.
/// The set has been deleted (see `Daemon::new`'s `on_crash` note for the two
/// experiments behind that), which removes the leak rather than fixing it — so
/// the invariant is now the only defence, and it belongs here rather than only in
/// a review comment: if you add a per-session set to `Daemon`, this function is
/// one of the two places that has to release it.
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
    // Release the session slot (issue #4) — only on a `Some`, because `None` means
    // nothing was running under that id and therefore nothing was counted. An
    // idempotent `close` (the shape a client sends on shutdown) must not be able to
    // drive the counter down, or a client that closes twice gets a free session.
    //
    // This is the `close` half of the invariant this function's doc states: a
    // per-session resource is one of the two places that has to release it.
    if detached.is_some() {
        daemon
            .attached_sessions
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
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

    /// A subagent's `cost_update` is the CHILD's money and must land on the
    /// CHILD's row (finding K).
    ///
    /// The pump re-targets a subagent's frames to the PARENT's id so the child's
    /// UI lives in the parent's scope (`pump.rs`'s `delivery_id`), so the id
    /// `on_event` is handed is NOT the row the frame owns.
    /// `TranscriptPersister::apply` re-derives the frame's own sid for exactly
    /// this reason; `apply_interactive_event` has no sid to re-derive from — it
    /// writes the id it is handed — so the CALLER must pick the row. Writing a
    /// child's total to the delivery id is not merely misplaced: the cost write
    /// is a SNAPSHOT that OVERWRITES, so the child's figure would REPLACE the
    /// parent's own — the parent reporting its child's spend as its own.
    ///
    /// This is the daemon-side twin of the desktop's router branch
    /// (`archimedes-desktop/src-tauri/src/agent/session/router.rs`), which makes
    /// the same distinction for the same reason.
    #[tokio::test]
    async fn a_subagents_cost_update_lands_on_the_childs_row_not_the_parents() {
        let Harness { daemon, _dir } = test_daemon();
        daemon
            .record_session(&test_env("parent"))
            .expect("the parent's row");
        // A child's row is created lazily by the persister and needs a `cwd` for
        // the FK — the same wire `dispatch_subagent`'s registrar provides.
        daemon.persister.set_cwd("child", "/tmp/child");

        let frame = WorkerInboundEvent::SinkFrame {
            event: "interactive-event".to_string(),
            payload: serde_json::json!({
                "sessionId": "child",
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": 0.25, "source": "main" },
            }),
        };
        // Delivered under the PARENT's id, exactly as the pump delivers it.
        persist_frame(&daemon.persister, "parent", true, &frame);

        assert_eq!(
            daemon.db.session_cost("child").expect("read child"),
            Some(0.25),
            "the child's own spend must be on the child's row"
        );
        assert_eq!(
            daemon.db.session_cost("parent").expect("read parent"),
            None,
            "the parent's row must be untouched by a frame the child emitted — \
             the write is a snapshot, so a misplaced one overwrites the parent's \
             own figure rather than adding to it"
        );
    }

    /// The parent's OWN `cost_update` still lands on the parent's row even
    /// though its payload carries a `sessionId` — the `is_subagent: false` arm
    /// is the whole path for main sessions, and `fold_child_cost` (the parent
    /// folding a child's total into its own) emits on the parent's own sink.
    /// Without this half the fix could pass by routing EVERYTHING to the payload
    /// id.
    #[tokio::test]
    async fn a_main_sessions_cost_update_lands_on_the_delivery_id() {
        let Harness { daemon, _dir } = test_daemon();
        daemon
            .record_session(&test_env("parent"))
            .expect("the parent's row");

        let frame = WorkerInboundEvent::SinkFrame {
            event: "interactive-event".to_string(),
            payload: serde_json::json!({
                "sessionId": "parent",
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": 0.5, "source": "main" },
            }),
        };
        persist_frame(&daemon.persister, "parent", false, &frame);

        assert_eq!(
            daemon.db.session_cost("parent").expect("read parent"),
            Some(0.5),
            "a main session's frame is written under its delivery id"
        );
    }

    /// A subagent's `todos_update` is NOT persisted at all (finding K's second
    /// half — the desktop gates it with `!is_subagent || event == "cost_update"`).
    ///
    /// A child's todo list is the parent's UI's rendering of the delegation, not
    /// a row the child owns; persisting it would put the child's list on the
    /// child's row where no UI looks, and — under the pre-fix routing — on the
    /// PARENT's row over the parent's own list.
    #[tokio::test]
    async fn a_subagents_todos_update_is_not_persisted() {
        let Harness { daemon, _dir } = test_daemon();
        daemon
            .record_session(&test_env("parent"))
            .expect("the parent's row");
        daemon.persister.set_cwd("child", "/tmp/child");

        let frame = WorkerInboundEvent::SinkFrame {
            event: "interactive-event".to_string(),
            payload: serde_json::json!({
                "sessionId": "child",
                "seq": 0,
                "event": "todos_update",
                "payload": {
                    "source": "main",
                    "todos": [{ "content": "child work", "status": "pending" }],
                },
            }),
        };
        persist_frame(&daemon.persister, "parent", true, &frame);

        assert_eq!(
            daemon.db.session_todos("parent").expect("read parent"),
            None,
            "the parent's todos must not be replaced by its child's list"
        );
        assert!(
            daemon.db.session("child").expect("read child").is_none(),
            "and no child row is conjured up for a frame that owns no row"
        );

        // A MAIN session's `todos_update` still persists — the gate is on
        // subagents, not on the event.
        let main = WorkerInboundEvent::SinkFrame {
            event: "interactive-event".to_string(),
            payload: serde_json::json!({
                "sessionId": "parent",
                "seq": 0,
                "event": "todos_update",
                "payload": {
                    "source": "main",
                    "todos": [{ "content": "parent work", "status": "pending" }],
                },
            }),
        };
        persist_frame(&daemon.persister, "parent", false, &main);
        let stored = daemon
            .db
            .session_todos("parent")
            .expect("read parent")
            .expect("the main session's todos are stored");
        assert!(
            stored.contains("parent work"),
            "a main session's todos persist as before: {stored}"
        );
    }

    /// A subagent frame that carries NO `sessionId` is UNPERSISTABLE, and must
    /// be skipped rather than fall back to the delivery id.
    ///
    /// The desktop's equivalent branch falls back to the parent's id and
    /// documents that fallback as deliberately unreachable AND dangerous — its
    /// own comment says it "must SKIP the write … NOT fall back". The desktop
    /// can afford the dead code because a loop-side pin makes it unreachable;
    /// new code should not copy a footgun its own author calls wrong. Skipping
    /// loses one frame; falling back silently writes a child's money into the
    /// parent's row, which is the exact corruption this branch exists to prevent.
    #[tokio::test]
    async fn a_subagent_frame_with_no_session_id_of_its_own_is_skipped() {
        let Harness { daemon, _dir } = test_daemon();
        daemon
            .record_session(&test_env("parent"))
            .expect("the parent's row");

        let frame = WorkerInboundEvent::SinkFrame {
            event: "interactive-event".to_string(),
            payload: serde_json::json!({
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": 0.75, "source": "main" },
            }),
        };
        persist_frame(&daemon.persister, "parent", true, &frame);

        assert_eq!(
            daemon.db.session_cost("parent").expect("read parent"),
            None,
            "a frame with no owner is unpersistable: skipping it is correct, \
             writing it to the delivery id is the double-count"
        );
    }

    /// A `SinkFrame` whose event is not `interactive-event` writes no row — the
    /// event-name guard is the filter (the desktop's rendering concerns are not
    /// this store's rows).
    #[tokio::test]
    async fn a_non_interactive_sink_frame_writes_no_rows() {
        let Harness { daemon, _dir } = test_daemon();
        daemon.record_session(&test_env("s1")).expect("the row");
        persist_frame(
            &daemon.persister,
            "s1",
            false,
            &WorkerInboundEvent::SinkFrame {
                event: "session-update".to_string(),
                payload: serde_json::json!({
                    "sessionId": "s1",
                    "event": "cost_update",
                    "payload": { "costUsd": 9.0 },
                }),
            },
        );
        assert_eq!(
            daemon.db.session_cost("s1").expect("read"),
            None,
            "only the `interactive-event` envelope reaches the persister"
        );
    }

    /// A SUBAGENT's crash is labelled `isSubagent: true` AND re-targeted to the
    /// parent (finding E's sibling: the hardcoded `false`).
    ///
    /// Two bugs in one literal. The label was wrong, and — because `watched`
    /// holds only ids a client `start`ed, which are always parents — a frame
    /// keyed to the minted child id was filtered out by every relay, so a
    /// subagent crash was invisible on the socket. Both are pinned here.
    #[test]
    fn a_subagent_crash_is_labelled_and_reaches_the_parents_relay() {
        let subagents = StdMutex::new(HashMap::from([(
            "child-1".to_string(),
            SubagentRecord {
                parent: "parent-1".to_string(),
                closed: false,
            },
        )]));
        let frame = crash_report_frame(&subagents, "child-1", Some(137), Some(4242));
        assert_eq!(
            frame["isSubagent"], true,
            "a child's death is not a main session's: {frame}"
        );
        assert_eq!(
            frame["sessionId"], "parent-1",
            "the frame must carry the PARENT's id or no relay is watching it"
        );
        assert_eq!(frame["frame"]["type"], "worker-crashed");
        assert_eq!(frame["frame"]["exitCode"], 137);
        assert_eq!(frame["frame"]["pid"], 4242);
        assert_eq!(
            frame["frame"]["subagentSessionId"], "child-1",
            "the child's own id must still be there — a parent with several
             children cannot act on 'one of them died'"
        );
        // And it passes the relay filter for a client watching the parent.
        assert!(
            watched(&frame, &HashSet::from(["parent-1".to_string()])),
            "a client watching the parent must receive its child's crash"
        );
    }

    /// A MAIN session's crash keeps the old shape: the delivery id, no
    /// subagent flag, and no child id to report.
    #[test]
    fn a_main_session_crash_keeps_its_own_id_and_no_subagent_flag() {
        let subagents = StdMutex::new(HashMap::new());
        let frame = crash_report_frame(&subagents, "main-1", Some(1), Some(7));
        assert_eq!(frame["sessionId"], "main-1");
        assert_eq!(frame["isSubagent"], false);
        assert!(
            frame["frame"]["subagentSessionId"].is_null(),
            "a main session has no child to name: {frame}"
        );
        assert!(watched(&frame, &HashSet::from(["main-1".to_string()])));
    }

    /// The `FanOutSink` learns the pairing from the engine's lifecycle frames —
    /// the only seam where the daemon ever sees both ids together.
    #[test]
    fn the_sink_learns_the_child_parent_pairing_from_the_lifecycle_frames() {
        let (fan_out, mut rx) = tokio::sync::broadcast::channel(8);
        let subagents: Arc<StdMutex<HashMap<String, SubagentRecord>>> =
            Arc::new(StdMutex::new(HashMap::new()));
        let sink = FanOutSink {
            session_id: "parent-1".to_string(),
            fan_out,
            subagents: Arc::clone(&subagents),
        };

        sink.emit(
            "subagent-session-started",
            serde_json::json!({"sessionId": "child-9", "parentSessionId": "parent-1"}),
        );
        assert_eq!(
            subagents
                .lock()
                .expect("lock")
                .get("child-9")
                .map(|r| (r.parent.as_str(), r.closed)),
            Some(("parent-1", false)),
            "the started frame must register the pairing"
        );

        // Closing TOMBSTONES rather than deleting: the pump publishes the drive's
        // exit watch BEFORE it calls `on_crash`, and `subagent-closed` is emitted
        // from the drive task's teardown on another thread, so a delete here
        // could erase the label the crash report is about to read.
        sink.emit(
            "subagent-closed",
            serde_json::json!({"sessionId": "child-9", "status": "completed"}),
        );
        assert_eq!(
            subagents
                .lock()
                .expect("lock")
                .get("child-9")
                .map(|r| (r.parent.as_str(), r.closed)),
            Some(("parent-1", true)),
            "a closed dispatch keeps its label until it is pruned"
        );

        // The lifecycle frame still reaches the socket unchanged (the sink's
        // PRIMARY job — a label-learning side effect must not eat the frame).
        let delivered = rx.try_recv().expect("the frame is still published");
        assert_eq!(delivered["sessionId"], "parent-1");
        assert_eq!(delivered["frame"]["event"], "subagent-session-started");
    }

    /// Pruning keeps the child→parent map bounded (it is keyed by ids that mint
    /// fresh per dispatch, so an unbounded map is a slow leak).
    ///
    /// This used to assert the same property for a second set (`dead`), which has
    /// been deleted — see `Daemon::new`'s `on_crash` note for the measurement.
    /// The `subagents` half is the one that still exists and still needs a prune:
    /// a closed dispatch leaves a TOMBSTONE deliberately, so without this the map
    /// grows by one entry per delegation for the daemon's whole life.
    #[tokio::test]
    async fn the_bookkeeping_sets_are_pruned_on_a_registry_touch() {
        let Harness { daemon, _dir } = test_daemon();
        daemon.subagents.lock().expect("lock").insert(
            "child".to_string(),
            SubagentRecord {
                parent: "p".to_string(),
                closed: true,
            },
        );
        daemon.prune_subagents();
        assert!(
            daemon.subagents.lock().expect("lock").is_empty(),
            "a tombstoned subagent is dropped once it can be"
        );
    }

    /// A `start` claim is released on EVERY exit path, including a failed
    /// `attach` — an id left claimed would be un-startable forever, which is the
    /// same class of defect finding E reports.
    #[tokio::test]
    async fn a_failed_start_releases_its_claim() {
        let Harness { daemon, _dir } = test_daemon();
        // `never-spawned` is not an executable, so `attach` fails.
        let frame = {
            let env = test_env("claim-me");
            let mut frame = serde_json::to_value(&env).expect("encodable");
            frame
                .as_object_mut()
                .expect("an object")
                .insert("type".to_string(), serde_json::json!("start"));
            frame
        };
        let reply = start(&daemon, &mut HashSet::new(), &frame).await;
        assert_eq!(
            reply["ok"], false,
            "the bogus worker exe must fail: {reply}"
        );
        assert!(
            daemon.starting.lock().expect("lock").is_empty(),
            "a failed start must not leave the id claimed"
        );
    }

    /// A `start` that FAILS must give its [`SESSION_LIMIT`] reservation back.
    ///
    /// The bound is now taken at claim time (issue #4), which is what makes the
    /// increment precede any possible decrement — but it moves the reservation
    /// earlier than the thing that can fail. So a `start` whose `attach` never
    /// happened has to release the slot, or `SESSION_LIMIT` failed starts (a wrong
    /// `--worker-exe`, which a user can typo once and retry thirty times) would
    /// consume every slot and leave the daemon unable to start ANY session, with a
    /// message about the session limit that points nowhere near the typo.
    ///
    /// This is the failure mode the RAII release in [`SessionClaim::drop`] exists
    /// for, so it is pinned rather than trusted: `SESSION_LIMIT + 4` doomed starts,
    /// and not one of them may be refused for want of a slot.
    #[tokio::test]
    async fn a_failed_start_gives_its_session_slot_back() {
        let Harness { daemon, _dir } = test_daemon();
        for attempt in 0..(SESSION_LIMIT + 4) {
            let env = test_env(&format!("doomed-{attempt}"));
            let mut frame = serde_json::to_value(&env).expect("encodable");
            frame
                .as_object_mut()
                .expect("an object")
                .insert("type".to_string(), serde_json::json!("start"));
            let reply = start(&daemon, &mut HashSet::new(), &frame).await;
            assert_eq!(reply["ok"], false, "the bogus exe must still fail: {reply}");
            let error = reply["error"].as_str().expect("a reason");
            assert!(
                !error.contains("session limit"),
                "attempt {} was refused by the session LIMIT, so a failed `start` \
                 kept its reservation — {} doomed starts would leave the daemon \
                 unable to start anything, blaming the session bound for a bad \
                 --worker-exe: {error}",
                attempt + 1,
                SESSION_LIMIT + 4,
            );
        }
        assert_eq!(
            daemon
                .attached_sessions
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "every failed start must have released its slot — a non-zero counter \
             here is a leak, and the leak `Daemon::dead` was deleted for having"
        );
    }

    /// The bound must not be exhausted by a `close` that never had a session.
    ///
    /// `close` reports `ok: false` for an id nothing is running (the idempotent
    /// shutdown shape a client sends), and that `false` is exactly what guards the
    /// decrement — an unconditional one would let any client drive the counter
    /// negative by calling `close` on ids it never started, and a wrapped
    /// `AtomicUsize` is a session limit that fires forever.
    #[test]
    fn a_close_with_nothing_running_releases_nothing() {
        let Harness { daemon, _dir } = test_daemon();
        for attempt in 0..(SESSION_LIMIT + 4) {
            let reply = close(
                &daemon,
                &mut HashSet::new(),
                &serde_json::json!({"type": "close", "sessionId": format!("never-{attempt}")}),
            );
            assert_eq!(reply["ok"], false, "nothing was running: {reply}");
        }
        assert_eq!(
            daemon
                .attached_sessions
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "an idempotent `close` must not move the counter — a decrement on a \
             `None` detach wraps an `AtomicUsize` at zero to `usize::MAX`, and that \
             is a session limit that refuses every start for the daemon's whole life"
        );
    }

    /// A live claim makes a second `start` for the same id refuse rather than
    /// race into a second `attach` (finding F, the unit-level half).
    #[tokio::test]
    async fn a_claimed_session_id_refuses_a_second_start() {
        let Harness { daemon, _dir } = test_daemon();
        let claim = daemon.claim_session("racing").expect("claim");
        let reply = start(&daemon, &mut HashSet::new(), &{
            let env = test_env("racing");
            let mut frame = serde_json::to_value(&env).expect("encodable");
            frame
                .as_object_mut()
                .expect("an object")
                .insert("type".to_string(), serde_json::json!("start"));
            frame
        })
        .await;
        assert_eq!(
            reply["ok"], false,
            "an id with an `attach` in flight must not be attached twice: {reply}"
        );
        assert!(
            reply["error"]
                .as_str()
                .is_some_and(|m| m.contains("starting")),
            "and say which race it refused: {reply}"
        );
        drop(claim);
        // Released, so the next attempt gets past the claim (and fails on the
        // bogus exe instead — proving the refusal was the claim, not the id).
        assert!(daemon.claim_session("racing").is_ok());
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

    /// The bounded queue's full case, deterministically (finding 7).
    ///
    /// The socket-level version lives in `tests/serve.rs`, but it can only
    /// observe the `gap` frame — it cannot read the daemon's heap, and the exact
    /// count depends on a schedule. This tests the mechanism directly: a full
    /// queue DROPS, COUNTS, and keeps the connection, and only a gone receiver
    /// ends it.
    #[test]
    fn a_full_client_queue_drops_counts_and_keeps_the_connection() {
        // Capacity 1, so "full" is two sends away and the test does not have to
        // know `CLIENT_QUEUE_CAPACITY` to exercise the branch.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<serde_json::Value>(1);
        let mut dropped = 0u64;

        // `try_send` is synchronous, which is the whole point: nothing here needs
        // a runtime, because nothing here waits for room.
        assert!(
            enqueue(&tx, serde_json::json!({"n": 1}), &mut dropped),
            "the first frame fits, so the connection stays up"
        );
        assert_eq!(dropped, 0, "an accepted frame is not a drop");
        assert_eq!(rx.try_recv().expect("the frame is queued")["n"], 1);

        // Fill it, then overflow it.
        assert!(enqueue(&tx, serde_json::json!({"n": 2}), &mut dropped));
        assert!(
            enqueue(&tx, serde_json::json!({"n": 3}), &mut dropped),
            "a FULL queue must keep the connection — a client that cannot read \
             fast enough is slow, not banned (recovery is T10's replay)"
        );
        assert!(
            enqueue(&tx, serde_json::json!({"n": 4}), &mut dropped),
            "and it must stay up for the frames after the first drop"
        );
        assert_eq!(
            dropped, 2,
            "the two frames that did not fit must be COUNTED — the count is what \
             the `gap` frame carries, and an uncounted drop is a silent one"
        );
        assert_eq!(rx.try_recv().expect("the queued frame survives")["n"], 2);

        // The receiver is gone: that, and only that, ends the connection.
        drop(rx);
        assert!(
            !enqueue(&tx, serde_json::json!({"n": 5}), &mut dropped),
            "a gone receiver must report false so the caller breaks — this is \
             the case the old `send(..).is_err()` conflated with \"no room\""
        );
    }

    /// The `gap` notice's shape, pinned once for the two paths that emit it.
    ///
    /// Both the fan-out's lag arm and a full per-client queue publish this object,
    /// and a client parses `skipped` by name — so a second spelling (a `count`, a
    /// nested `gap`) is a client that silently reads `null`. `tests/serve.rs` can
    /// only see one frame arrive; this pins the field.
    #[test]
    fn the_gap_notice_carries_its_count_where_the_client_reads_it() {
        let frame = gap_frame(7);
        assert_eq!(frame["type"], "gap");
        assert_eq!(
            frame["skipped"].as_u64(),
            Some(7),
            "the count must be a number at the top level: {frame}"
        );
        // Addressed to the CONNECTION, which is what lets `watched` pass it to
        // every client regardless of what it is watching.
        assert!(
            frame.get("sessionId").is_none(),
            "a `gap` naming a session would be filtered out for a client not \
             watching it, which is the opposite of telling the client it fell \
             behind: {frame}"
        );
        let watching = ["only-this".to_string()].into_iter().collect();
        assert!(
            watched(&frame, &watching),
            "the notice must survive the per-session filter"
        );
    }

    /// What the writer's drain prelude ACTUALLY promises after the T4b split: its
    /// `gap` carries the CURRENT `dropped` count at the moment the drain wakes, and
    /// taking it does not disturb the queue's FIFO order.
    ///
    /// # Why this is the honest property, and what it deliberately does NOT claim
    ///
    /// Before the split, one task serialised every `send_frame`, so a `gap` could not
    /// land in the middle of the reply stream. After it, the reader enqueues replies
    /// while the writer drains, so a `gap` CAN be written before the very reply whose
    /// drop it reports — `serve_client`'s doc states that as the new contract. A test
    /// that asserted "the reply comes first" would be asserting the OLD contract and
    /// would go red on correct new code; a test that asserted nothing about the
    /// prelude would let the count itself drift. So this pins the three things that ARE
    /// the writer's own guarantees, none of which depends on how the two tasks race:
    ///
    /// 1. the announced `skipped` equals the count at drain wake (not a stale one, and
    ///    not a count that grew during the send);
    /// 2. the count is TAKEN, so the next drain wake does not announce the same drops a
    ///    second time;
    /// 3. the queue stays FIFO — the prelude writes its notice around the frames, never
    ///    reorders them.
    ///
    /// True reply-vs-`gap` ORDERING under load is a property of two tasks racing inside
    /// `serve_client`, and it lives where it can be observed: the relay test in
    /// `tests/serve.rs`, plus the contract in `serve_client`'s doc.
    #[test]
    fn the_gap_drain_prelude_uses_the_current_drop_count() {
        // Capacity 1, so the second enqueue is the overflow — the same trick
        // `a_full_client_queue_drops_counts_and_keeps_the_connection` uses, and for the
        // same reason: the test need not know `CLIENT_QUEUE_CAPACITY`.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<serde_json::Value>(1);
        // The SHARED shape, because this is what the split actually passes to the
        // drain: driving `enqueue_shared` covers the adapter too (its lock is taken and
        // released inside the call, which is the property that lets the writer hold no
        // guard across `send_frame`).
        let dropped = StdMutex::new(0u64);

        assert!(enqueue_shared(
            &tx,
            serde_json::json!({"reply": "a"}),
            &dropped
        ));
        assert!(
            enqueue_shared(&tx, serde_json::json!({"reply": "b"}), &dropped),
            "a full queue keeps the connection — it just drops the frame"
        );
        assert_eq!(
            *dropped.lock().unwrap(),
            1,
            "`b` did not fit, so exactly one drop is owed to the client"
        );

        // ── The drain arm, transcribed: wake, announce, send. ────────────────
        let frame = rx.try_recv().expect("`a` is the queued frame");
        assert_eq!(frame["reply"], "a", "the queue is FIFO from the head");

        let still_dropped = {
            let mut guard = dropped.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *guard)
        };
        assert_eq!(still_dropped, 1, "the prelude announces the CURRENT count");
        let notice = gap_frame(still_dropped);
        assert_eq!(
            notice["skipped"].as_u64(),
            Some(1),
            "the notice the client sees carries the count, not a placeholder: {notice}"
        );
        assert_eq!(
            *dropped.lock().unwrap(),
            0,
            "the count is TAKEN, not read — otherwise every later frame would \
             re-announce the same drops and a client 1 frame behind would be told \
             about it once per reply forever"
        );

        // ── The next wake has nothing to say, and the queue order survived. ──
        assert!(enqueue_shared(
            &tx,
            serde_json::json!({"reply": "c"}),
            &dropped
        ));
        let next = rx.try_recv().expect("`c` is queued behind nothing");
        assert_eq!(
            next["reply"], "c",
            "`a` was delivered ahead of `c`, so the prelude reordered nothing"
        );
        let second_prelude = {
            let mut guard = dropped.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *guard)
        };
        assert_eq!(
            second_prelude, 0,
            "no new drop means no second `gap` — the drain emits one only when the \
             count is non-zero, exactly as the single-task design did"
        );
    }
}

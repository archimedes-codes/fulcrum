//! Windows named-pipe listener + client (T9 Task 3).
//!
//! This file MUST EXIST even though `transport/mod.rs` declares it under
//! `#[cfg(windows)]`: omitting it gives `error[E0583]: file not found for module
//! 'windows'` on the Windows target ONLY, so a Linux-only developer never sees
//! it and CI is the first to notice. The cross-target clippy run on
//! `x86_64-pc-windows-gnu` is what holds this open.
//!
//! Backed by `tokio::net::windows::named_pipe::{NamedPipeServer,
//! NamedPipeClient}` — tokio's `net` feature provides those on Windows, so no
//! `windows` / `windows-sys` dependency is needed (and none is declared; adding
//! one would be a DEVIATION from the plan's acceptance criteria).
//!
//! PROOF LEVEL FOR THIS FILE — three tiers, because "was it run?" now has three
//! different answers depending on which part of this file you mean, and the
//! header used to answer all three with "never". That was wrong, and being wrong
//! here makes the file's other caveats read as vaguer than they are.
//!
//! **1. Type-checked and lint-checked, always.** `cargo check --target
//! x86_64-pc-windows-gnu --all-targets` and the same under clippy. This is the
//! only proof available on a Linux box and it is the cheap one: it proves the API
//! is used with the right types and nothing about whether a pipe carries a frame.
//!
//! **2. RUN ON REAL WINDOWS, and passing.** CI's `windows-latest` leg executes
//! this file's code — `bind`, `accept`, `connect`, and the framing over a real
//! named pipe — and on `main` that leg completes green in about 4 minutes. So the
//! continuous-instance invariant is NOT unexecuted: `Listener::bind`'s
//! `FILE_FLAG_FIRST_PIPE_INSTANCE` guard is exercised by
//! `a_second_daemon_on_the_same_endpoint_exits_non_zero_and_leaves_the_first_running`,
//! which passes there, and `accept` handing an instance to a real client and that
//! client round-tripping a frame is what `tests/transport.rs`'s loopback tests do
//! on that leg. The `#[cfg(not(unix))]` tests in `tests/` have passed there too.
//! The old "NEVER BEEN RUN / there is no Windows runner in this suite" sentence
//! described a CI configuration that no longer exists; do not restore it.
//!
//! **3. RUN ON REAL WINDOWS, and hanging — and this is the live problem.** The
//! same leg on `feature/daemon-hardening` does not finish: two runs (a push and a
//! PR) were cancelled at 3h43m with `cargo test` still going, both parked in the
//! same test, `a_shared_writer_under_a_mutex_never_interleaves_frames` in
//! `tests/transport.rs`. It is a NEW test on this branch, so the hang is this
//! branch's, not the Windows leg's; `transport.rs`'s other tests pass on that leg.
//!
//! And it is NOT this file's `connect`: that test dials through
//! `tests/transport.rs`'s `dial`, which wraps `connect` in a 10 s
//! `tokio::time::timeout`, and a stall inside `connect` would have failed there
//! with `connect resolves (a hang means the dial path is broken)`. The leg printed
//! no such panic, so the stall is downstream — after the connection exists. Do
//! not read the ceiling added to [`connect`] as this branch's fix, and do not
//! close the hang on the strength of it.
//!
//! What it IS: a genuine defect removed (an unbounded wait in a library function
//! is wrong whatever triggers it) and a diagnostic for the one caller whose
//! deadline this loop could swallow — `tests/serve.rs`'s `wait_until_listening`
//! checks its own deadline only AFTER `connect` returns, so behind an exit-less
//! retry its `panic!("the daemon never listened on {endpoint}")` was unreachable
//! and a stall there was silent. With the ceiling, that class of stall now names
//! the pipe. Root cause of the observed hang is still unknown, and provable only
//! on the Windows runner.
//!
//! The one behaviour in this file that has NEVER been observed to happen, on any
//! machine: the `ERROR_PIPE_BUSY` retry actually retrying. No test creates a
//! condition where every instance of a name is busy, so the busy arm of
//! [`open_with_retry`] has never been taken outside its own unit tests, which
//! drive it through a closure rather than through `CreateFileW`. Its BOUNDED
//! behaviour is pinned there; its triggering condition is unobserved.
//!
//! A Linux box can now get tier 2 without CI: the `x86_64-pc-windows-gnu` test
//! binaries run under wine, and doing so reproduced tier 3 exactly (8 of
//! `tests/transport.rs`'s 9 pass, the 9th never returns). That is a strong local
//! repro, and it is still not tier 2 — wine is an implementation of the API, not
//! the API, so a wine result never licenses a claim about Windows. Anyone who
//! changes this file should treat "clippy is clean cross-target" as necessary and
//! nowhere near sufficient, and should treat "green under wine" as a repro, not
//! as proof.
//!
//! ACCESS CONTROL, stated here because the unix leg has a real answer and this
//! one does not: `ServerOptions::create` passes a NULL `lpSecurityAttributes` to
//! `CreateNamedPipeW` (tokio exposes no setter — its `create` is
//! `create_with_security_attributes_raw(addr, ptr::null_mut())`), so the pipe's
//! DACL comes from the default security descriptor of the creating token. There
//! is therefore NO daemon-side equivalent of the unix socket's 0600: on Windows
//! the object namespace is global to the logon session and the pipe's access is
//! whatever the default grants, plus `reject_remote_clients(true)` below. The
//! only real mitigation available without a `windows-sys` dependency (which this
//! member deliberately does not carry) is the one T13 owns: authenticate at the
//! protocol layer. See `README`'s "What the local endpoint grants".
//!
//! # What this platform can and cannot prove about a peer (issue #4)
//!
//! The unix leg resolves a peer credential at `accept` (`SO_PEERCRED` on Linux,
//! `getpeereid` on the BSDs) and refuses a peer whose uid is not the daemon's.
//! **This leg cannot, and this file says so rather than pretending otherwise.**
//!
//! | | Claim |
//! |---|---|
//! | **Can prove** | the caller is LOCAL. `reject_remote_clients(true)` (see `create_pipe` below) means a client reaching this pipe over the network is refused by the OS, and the one-instance invariant means it is *this* daemon answering. That is the same class of guarantee unix gets from the socket family itself. |
//! | **Cannot prove** | WHO the caller is. The honest statement: **the named pipe cannot prove the caller's identity beyond "a local process that passed the pipe's DACL"; the DACL is the creating token's default (the NULL `lpSecurityAttributes` above); same-session peers are indistinguishable; T13's protocol-layer auth is the only real mitigation on Windows.** |
//!
//! Both rows are now backed by real execution, which they were not when they were
//! written — see "PROOF LEVEL" at the top of this file for the three tiers and
//! what each one does and does not license. Concretely: the **Can prove** row's
//! mechanism is exercised on CI's `windows-latest` leg, which passes on `main`
//! (`reject_remote_clients` is set unconditionally in `create_pipe`, and the
//! second-daemon test that pins the one-instance guard passes there). The
//! **Cannot prove** row needs no execution to hold — it is an absence, and it is
//! the row this file cannot make worse. What the branch's hang on that same leg
//! changes is neither row: it says this platform's RUNTIME is not yet understood,
//! which is a different gap from not knowing who the peer is, and conflating the
//! two would make the peer gap look resolved when only its evidence improved.
//!
//! The one Win32 call that would close the gap is `GetNamedPipeClientProcessId`,
//! and it is deliberately NOT called here. It is reachable only through the
//! `Kernel32` bindings of the `windows` / `windows-sys` crates, neither of which
//! this member declares — `Cargo.toml`'s tokio comment says the crate "is
//! deliberately ABSENT and must not be added", and this file's header repeats it.
//! Hand-rolled `#[cfg(windows)]` FFI (an `unsafe extern "system"` declaration and
//! a raw `HANDLE` cast) was considered and rejected, for three reasons:
//!
//! 1. **It fights the house pattern for its own sake.** This member's precedent
//!    for a Win32 fact is a raw constant — `is_pipe_busy`'s `ERROR_PIPE_BUSY`,
//!    `childproc.rs`'s `CREATE_NO_WINDOW` — precisely to avoid a dependency. FFI
//!    is not that precedent; it is a different, heavier thing.
//! 2. **It would be an unsafe surface whose first execution is CI's.**
//!    `extern "system"`, `GetLastError` discipline and handle-lifetime
//!    correctness are exactly the obligations that show up at LINK time and at
//!    RUN time. Per this file's "PROOF LEVEL" note above, CI's `windows-latest`
//!    leg DOES run this file — so a mistake in an FFI boundary called from
//!    `accept` would not be caught locally at all, it would surface there, on the
//!    leg that currently takes 4 minutes to reach the tests and 3h43m to fail
//!    unhelpfully when something on this path stalls (tier 3 above). A wrong
//!    `HANDLE` cast is not a compile error; it is a hang or a wrong number, which
//!    is the class of failure this file is already having trouble diagnosing. A
//!    documented gap anyone can read beats an unsafe boundary whose first
//!    feedback loop is that one.
//! 3. **The residual it would close is already small, and the number it returns
//!    is weak evidence anyway.** `GetNamedPipeClientProcessId` yields the client's
//!    PID — not its integrity level, not its token, not a secret — and any process
//!    running as you can forge which PID it appears as by timing. Combined with
//!    `reject_remote_clients(true)`, the only callers left are local processes
//!    holding your logon token: the same-user case, which is precisely the case no
//!    peer-credential check on any platform closes (see `unix.rs`'s residual note,
//!    which is the same sentence about a different OS).
//!
//! So the platform difference is documented, not papered over: unix proves the
//! connecting uid; Windows proves the connection is local and nothing more.

use std::sync::Mutex as StdMutex;

use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};

use crate::transport::frame::{self, Connection};
use crate::transport::{Endpoint, TransportError};

/// The pipe name the daemon listens on by default.
///
/// `\\.\pipe\` is the NT named-pipe namespace for the local machine; a name
/// without that prefix is not a pipe address. No length check applies here the
/// way `sun_path` applies on unix — the pipe namespace takes names far longer
/// than a socket path — so `default_endpoint` has nothing to pre-check.
const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\fulcrum";

/// The daemon's pipe. Unlike the unix default this needs no data dir: a pipe
/// name lives in the object namespace, not the filesystem, so there is no path
/// to be too long and no directory to create.
pub fn default_endpoint() -> Result<Endpoint, TransportError> {
    Ok(Endpoint::Pipe(DEFAULT_PIPE_NAME.to_string()))
}

/// A daemon-side named-pipe listener.
///
/// A named pipe is NOT a listening socket: one `NamedPipeServer` instance serves
/// exactly ONE client. So this holds the NAME and one ARMED instance, and
/// [`Listener::accept`] swaps them — the armed instance is handed to the caller
/// as its [`Connection`] and a fresh one is armed in its place.
///
/// THE INVARIANT, which is the whole design of this file: **at least one
/// instance of the pipe name exists at every moment of the daemon's life.** Two
/// separate guarantees rest on it, and they are the two findings this file had:
///
/// * A second daemon's `bind` uses `FILE_FLAG_FIRST_PIPE_INSTANCE`, which fails
///   while ANY instance of the name exists. The previous code created that
///   instance as a temporary and dropped it in the same statement, so the guard
///   held for the microseconds that handle was alive and for nothing after — a
///   second daemon started a moment later bound happily and the two split the
///   clients, which is the corrupted multiplex the flag exists to prevent.
///   Holding one instance for the daemon's life makes the guard continuous.
/// * A client that arrives when NO instance exists gets `ERROR_PIPE_BUSY`. The
///   client side retries that, but the daemon should not be the reason it has to.
///
/// The naive way to hold an instance — create one and never `connect()` it —
/// would be a CLIENT TRAP, and is the reason this is a pipeline rather than a
/// spare handle. Windows assigns an incoming client to any available instance of
/// the name; an instance that nobody is waiting on accepts the connection and
/// then never reads it, so the client's `CreateFile` succeeds into a dead pipe
/// and hangs. Every instance this type owns is one somebody is about to `await`
/// a `connect()` on.
pub struct Listener {
    name: String,
    /// The armed instance. `Option` only so `accept` can hand it out and refill
    /// it under one lock; it is `None` for the length of a statement, never
    /// observably, and a `None` reaching `accept` would mean the invariant is
    /// already broken — which `accept` reports rather than panicking on.
    armed: StdMutex<Option<NamedPipeServer>>,
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-written: `NamedPipeServer` is not `Debug`, so a derive is not
        // available on anything that holds one.
        fmt.debug_struct("Listener")
            .field("name", &self.name)
            .field("armed", &format_args!("<instance>"))
            .finish()
    }
}

impl Listener {
    /// Create the pipe named by `endpoint` and arm its first instance.
    ///
    /// Errors: [`Endpoint::Socket`] on Windows → [`TransportError::WrongPlatform`];
    /// anything `CreateNamedPipeW` returns after the retries below →
    /// [`TransportError::Io`].
    ///
    /// Panics outside a tokio runtime with IO enabled (tokio's `create`
    /// contract).
    pub fn bind(endpoint: Endpoint) -> Result<Self, TransportError> {
        let Endpoint::Pipe(name) = endpoint else {
            return Err(frame::wrong_platform("unix", "windows", endpoint));
        };
        // `first_pipe_instance(true)` makes a SECOND daemon on the same name fail
        // here instead of silently sharing the namespace: without it
        // `CreateNamedPipeW` happily creates another instance and the two daemons
        // each accept a subset of the clients — a corrupted multiplex and one of
        // the hardest classes of bug to diagnose after the fact. The error it
        // raises is `PermissionDenied`, matching the shape tokio's own example
        // documents.
        //
        // The instance is ARMED, not dropped: see the invariant on the type. It
        // also serves the first client, which is why there is no spare handle
        // sitting here unattended.
        //
        // Retried on `ERROR_PIPE_BUSY`, matching what `connect` does for the same
        // code. Its reach is narrow and stated here rather than oversold:
        // `CreateNamedPipeW` reports `ERROR_ACCESS_DENIED` — which tokio surfaces
        // as `PermissionDenied` — when an instance of the name already exists, and
        // `ERROR_PIPE_BUSY` is the code `CreateFile` (the CLIENT) returns when
        // every instance is serving a client. So this arm mostly covers a create
        // that fails busy on a name the namespace still holds a reference to.
        //
        // The restart race (this daemon starting while its predecessor's
        // instances are still alive) therefore surfaces as `PermissionDenied`,
        // and it is deliberately NOT retried: that is byte-for-byte the same code
        // the genuine "a live daemon owns this name" refusal carries, and there is
        // no cheap way to tell the two apart from here — the unix leg can PROBE
        // the socket by connecting to it, and connecting to a pipe would consume a
        // real client slot on the predecessor. Retrying the refusal would turn a
        // clear, immediate answer into a two-second hang followed by the same
        // answer, which is worse for the operator who really does have a daemon
        // running. The consequence — a restart too fast after a SIGKILL can be
        // refused — is bounded by waiting a moment and trying again, and is the
        // honest residual.
        let first = bind_with_retry(&name, true)?;
        Ok(Self {
            name,
            armed: StdMutex::new(Some(first)),
        })
    }

    /// The pipe name clients must dial.
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::Pipe(self.name.clone())
    }

    /// Wait for the next client.
    ///
    /// Arms the NEXT instance BEFORE waiting on this one, in the same lock
    /// critical section as taking it — so the swap is atomic, the invariant
    /// ("one instance exists at every moment") is never observably broken, and a
    /// client that arrives while we are waiting is never turned away with
    /// `ERROR_PIPE_BUSY`. Arming first and taking second is deliberate: if the
    /// create fails, the armed instance is still in place and the daemon is left
    /// exactly as it was rather than with nothing to offer.
    ///
    /// The lock is never held across the `connect()` await.
    ///
    /// # No peer credential is checked here, and that is a documented platform
    /// limit, not an oversight (issue #4)
    ///
    /// The unix leg's `Listener::accept` resolves the peer's uid from the accepted
    /// fd (`SO_PEERCRED` / `getpeereid`) and returns
    /// [`TransportError::PeerIdentity`] for a peer under another account, before a
    /// `Connection` is ever built. The equivalent here would be
    /// `GetNamedPipeClientProcessId`, which needs a crate this member is forbidden
    /// to declare — and which returns a forgeable PID rather than an identity
    /// anyone could act on. The full reasoning, and the exact statement of what
    /// this platform can and cannot prove, is in this module's header ("What this
    /// platform can and cannot prove about a peer").
    ///
    /// Consequence for `serve::run`: the `PeerIdentity` arm of its accept-error
    /// match is unreachable on Windows, and deliberately so — it is not dead code
    /// to be removed, it is the unix leg's path. Nothing on this platform reaches
    /// that arm, because nothing on this platform can refuse a peer for who it is.
    pub async fn accept(&self) -> Result<Connection, TransportError> {
        let next = create_pipe(&self.name, false)?;
        let server = {
            let mut armed = self.armed.lock().unwrap_or_else(|p| p.into_inner());
            let current = armed.take().ok_or_else(|| {
                TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    format!(
                        "no instance of {name} was armed — the listener's \
                         one-instance-always invariant was broken, so a client may \
                         already be stranded on nothing",
                        name = self.name
                    ),
                ))
            })?;
            *armed = Some(next);
            current
        };
        // `connect()` completes when a client opens the pipe. Cancel-safe per
        // tokio's docs, but we do not `select!` over it either way — this is
        // awaited straight through, like the unix `accept`.
        server.connect().await?;
        Ok(Connection::new(server))
    }
}

/// How long [`bind`](Listener::bind) waits on a busy name before giving up.
///
/// The same 20 ms step [`open_with_retry`] waits between attempts for (there it
/// is the named [`BUSY_RETRY_STEP`]; here it stays a literal because `bind` is
/// sync and cannot share that loop), for the same error code and the same
/// reason: a busy name clears when an instance is released. The total bound is
/// what stops a genuinely stuck create from hanging startup forever — a daemon
/// that refuses to start after two seconds and says so beats one that sits
/// silent. See [`bind_with_retry`] for which error code this actually catches,
/// and for the case it pointedly does NOT retry.
const BIND_BUSY_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// The create, retrying `ERROR_PIPE_BUSY` up to [`BIND_BUSY_BUDGET`].
fn bind_with_retry(name: &str, first: bool) -> Result<NamedPipeServer, TransportError> {
    let deadline = std::time::Instant::now() + BIND_BUSY_BUDGET;
    loop {
        match create_pipe(name, first) {
            Ok(server) => return Ok(server),
            Err(e) if is_pipe_busy_err(&e) && std::time::Instant::now() < deadline => {
                // Blocking sleep, deliberately: `bind` is a SYNC fn (the daemon
                // calls it before the accept loop exists), so there is no task to
                // yield to — and tokio's async sleep would panic outside a
                // runtime's time driver. The unix leg has the same shape.
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Is this [`TransportError`] the "every instance is busy" answer?
///
/// `create_pipe` returns the transport's own error type, so the raw code has to
/// be reached through its `Io` arm — `TransportError` deliberately does not
/// mirror `std::io::ErrorKind`, so there is no richer variant to match on, and
/// adding one for a single call site would widen the surface for nothing.
fn is_pipe_busy_err(e: &TransportError) -> bool {
    matches!(e, TransportError::Io(io) if is_pipe_busy(io))
}

/// One pipe instance, byte-mode, local clients only.
///
/// `first` is the `first_pipe_instance` flag — see [`Listener::bind`] for why
/// only the binding call sets it.
fn create_pipe(name: &str, first: bool) -> Result<NamedPipeServer, TransportError> {
    let mut options = ServerOptions::new();
    options.first_pipe_instance(first);
    // Byte mode (tokio's default) is what the JSONL framing needs: MESSAGE mode
    // would make each write a discrete message and reject a read whose buffer is
    // smaller than it, which breaks `read_line`'s byte-at-a-time refill. Spelled
    // out rather than left to the default so the choice is visible next to the
    // framing it serves.
    options.pipe_mode(tokio::net::windows::named_pipe::PipeMode::Byte);
    // Refuse remote clients: the daemon's socket is a local IPC endpoint and a
    // pipe reachable over the network is an unauthenticated control channel.
    // (Tokio's default already rejects; naming it keeps the decision from being
    // "optimized" into a default later.)
    options.reject_remote_clients(true);
    Ok(options.create(name)?)
}

/// Dial the daemon's pipe.
///
/// `CreateFileW` on a pipe is synchronous in tokio's wrapper, and returns
/// `ERROR_PIPE_BUSY` when every instance is already serving a client. That one
/// code IS retried — see [`open_with_retry`] — and the retry is BOUNDED by
/// [`PIPE_BUSY_CEILING`]: past it this returns
/// [`std::io::ErrorKind::TimedOut`] naming the pipe, not another wait.
///
/// `NotFound` (no daemon running) is still returned as-is, on the first pass.
///
/// # Why the transport retries busy at all, and why it now stops
///
/// Both halves of that sentence are load-bearing, and they pull against each
/// other, so the resolution is stated rather than left for a reader to infer.
///
/// It retries because a caller CANNOT do it: `ERROR_PIPE_BUSY` reaches a caller
/// as an `io::Error` whose kind is a raw OS code, and nothing in it distinguishes
/// "the listener is between handing out an instance and re-arming one" — which
/// clears in microseconds, and is normal operation — from "the listener is stuck
/// and will never arm another instance". A caller handed both of those and no way
/// to tell them apart can only pick one, and either pick is wrong for the other.
/// The transport is the one place that knows which code it is looking at, so the
/// transient case belongs here.
///
/// It stops because "keep waiting" stops being the honest answer once the wait
/// has outgrown every explanation for it. An unbounded retry does not make a
/// stuck daemon recoverable; it makes a stuck daemon INVISIBLE. A caller that
/// checks its own deadline AFTER awaiting `connect` — which is the shape
/// `tests/serve.rs`'s `wait_until_listening` uses — can never reach that check,
/// so its "the daemon never listened on <endpoint>" failure is unreachable code
/// and the stall reads as a silent hang. Giving up converts "something is wrong
/// somewhere" into "every instance of `\\.\pipe\…` was busy for 5s", which is a
/// sentence an operator can act on. (A caller that wraps the call in
/// `tokio::time::timeout`, like `tests/transport.rs`'s `dial`, could already see
/// a failure — but all it could say was "connect did not resolve", not why.)
///
/// So: the transport retries busy because the caller cannot tell transient from
/// stuck, and it gives up so that the caller can.
///
/// Every other error, including `NotFound`, stays the caller's startup policy —
/// the transport does not decide whether a daemon that is not up yet is worth
/// waiting for, because that question has different answers for a CLI, a test and
/// a supervisor, and only the caller knows which one it is.
pub async fn connect(endpoint: Endpoint) -> Result<Connection, TransportError> {
    let Endpoint::Pipe(name) = endpoint else {
        return Err(frame::wrong_platform("unix", "windows", endpoint.clone()));
    };
    let client = open_with_retry(&name, PIPE_BUSY_CEILING, || {
        ClientOptions::new()
            .read(true)
            .write(true)
            .open(name.as_str())
    })
    .await?;
    Ok(Connection::new(client))
}

/// How long [`connect`] will wait for a free instance of a pipe name.
///
/// # Why a bounded retry is the correct shape here
///
/// Under normal operation this wait is measured in MICROSECONDS, not seconds. The
/// listener re-arms an instance at the TOP of `accept` — [`Listener::accept`]
/// creates `next` before it ever awaits `connect()` on the one it is handing out,
/// and the swap happens under one lock — so at least one armed instance exists at
/// every moment of the daemon's life (the invariant on [`Listener`]). A client
/// that arrives in the gap between `CreateFile` and the daemon's `connect()` can
/// land on the instance the daemon has not gotten to yet, and the busy answer
/// therefore means "look again in an instant", which is what the 20 ms step is
/// for.
///
/// Needing SECONDS of it means the invariant is already broken: the daemon is
/// stuck inside `accept`, or is gone with the namespace still holding a reference,
/// or is serving more clients than it has instances. None of those fix themselves
/// by waiting longer, and the wait itself destroys the evidence — a caller that is
/// parked here cannot report that the daemon never answered. So the ceiling is not
/// a compromise with correctness, it is the point where continuing to wait becomes
/// a lie about what the transport knows.
///
/// # Why 5 seconds
///
/// The two things it has to sit between are the normal case and the caller's
/// patience, and they are 5 to 6 orders of magnitude apart:
///
/// * Against the normal case it is ~100 000× the microseconds the re-arm takes,
///   and 250 retries at [`BUSY_RETRY_STEP`] — so no plausible scheduling delay on
///   a loaded CI runner, a slow antivirus hook on `CreateFile`, or a brief pause
///   of the daemon's accept task can reach it. A bound that a busy machine could
///   trip would be a new flake instead of a fix, and it would report a stuck
///   daemon that was merely slow.
/// * Against the caller's patience it is small. `tests/transport.rs`'s `dial`
///   gives `connect` one 10 s pass and `tests/serve.rs`'s `wait_until_listening`
///   spends a 30 s budget on repeated passes, so 5 s fits the first whole and
///   leaves the second room for ~six attempts before its own deadline fires —
///   each one now answering with a named error instead of silence. A CLI
///   operator will not watch a command sit quiet for a minute either.
///
/// It also matches [`BIND_BUSY_BUDGET`]'s order of magnitude deliberately: bind
/// waits 2 s to create, connect waits 5 s for a slot. The asymmetry is real — the
/// connect side is the one a client has no way to diagnose for itself, and it is
/// on the path every test dials through.
const PIPE_BUSY_CEILING: std::time::Duration = std::time::Duration::from_secs(5);

/// The pause between attempts of [`open_with_retry`].
///
/// The same 20 ms [`bind_with_retry`] steps by, for the same reason: a busy name
/// clears when an instance is released, which is not something a shorter poll can
/// observe sooner. It is a separate const rather than a literal so the tests can
/// assert the attempt COUNT against the ceiling — the count is the only way to
/// tell "honoured the bound" from "gave up after one try" without watching time.
const BUSY_RETRY_STEP: std::time::Duration = std::time::Duration::from_millis(20);

/// Run `open` until it stops answering `ERROR_PIPE_BUSY`, or until `ceiling`.
///
/// The open attempt is a closure, not a call: that is what lets the retry policy
/// be tested at all, since a real `ERROR_PIPE_BUSY` needs a live daemon holding
/// every instance of a real pipe name on a real Windows machine, which this suite
/// has never had (see this module's header). The closure is the seam; the loop
/// around it is what the tests below pin.
///
/// A non-busy error returns immediately and untouched — this function only ever
/// adds the give-up error, it never relabels someone else's.
///
/// # The deadline rides tokio's clock, and that is not an implementation detail
///
/// `tokio::time::Instant` and `tokio::time::sleep` are used instead of the
/// `std::time` pair so that `#[tokio::test(start_paused = true)]` auto-advances
/// this loop's clock and the ceiling can be tested in microseconds of wall time.
/// With `std::time::Instant` the paused clock would never move, the deadline would
/// never arrive, and the "gives up" test would hang — the exact shape of the bug
/// this function exists to remove. Under a real runtime tokio's `Instant` is the
/// same monotonic clock `std`'s is, so production behaviour is unchanged.
///
/// `bind` has the mirror-image constraint ([`bind_with_retry`] is sync and must
/// use the `std` pair); the two are deliberately not unified.
async fn open_with_retry<T, F>(
    name: &str,
    ceiling: std::time::Duration,
    mut open: F,
) -> Result<T, TransportError>
where
    F: FnMut() -> Result<T, std::io::Error>,
{
    let deadline = tokio::time::Instant::now() + ceiling;
    loop {
        match open() {
            Ok(value) => return Ok(value),
            Err(e) if is_pipe_busy(&e) && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(BUSY_RETRY_STEP).await;
            }
            Err(e) if is_pipe_busy(&e) => {
                return Err(TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "every instance of {name} was busy for {ceiling:?} and \
                         none of them freed up: the daemon is not arming a \
                         pipe instance (this is `ERROR_PIPE_BUSY` outlasting \
                         the retry budget, not a busy moment)"
                    ),
                )));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// `ERROR_PIPE_BUSY` (231), the "every instance is busy" code.
///
/// A bare constant compared against `raw_os_error()` — the same no-`windows-sys`
/// approach `childproc.rs`'s `CREATE_NO_WINDOW` takes, for the same reason: a
/// constant is cheaper than a dependency. Named (rather than inlined into the
/// comparison) because the tests below have to build this exact error to drive
/// the retry loop, and a fixture that spells the number out a second time can
/// drift away from what the loop recognises without any test noticing.
const ERROR_PIPE_BUSY: i32 = 231;

fn is_pipe_busy(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(ERROR_PIPE_BUSY)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The "every instance of this name is already serving a client" answer
    /// `CreateFileW` gives, built from the SAME constant the retry loop matches
    /// on — see [`ERROR_PIPE_BUSY`] for why that matters.
    fn every_instance_is_busy() -> std::io::Error {
        std::io::Error::from_raw_os_error(ERROR_PIPE_BUSY)
    }

    /// The fixture is the code the loop retries on.
    ///
    /// The anti-vacuous guard for the two tests below: if this fixture were not
    /// the busy code, `open_with_retry` would return it on the FIRST pass as an
    /// ordinary error and the "gives up" test would still see an `Err` — a green
    /// that pinned nothing.
    #[test]
    fn the_busy_fixture_is_the_code_the_retry_loop_retries_on() {
        assert!(
            is_pipe_busy(&every_instance_is_busy()),
            "the fixture must be the code `is_pipe_busy` recognises, or the \
             retry tests below are exercising the non-busy arm"
        );
    }

    /// A busy answer that clears inside [`PIPE_BUSY_CEILING`] still connects.
    ///
    /// The property is that the bound does NOT eat the normal case: three busy
    /// answers then success is the ordinary race (the listener is between
    /// handing out an instance and re-arming one), and it must reach the `Ok`.
    /// The attempt count is asserted so this cannot pass by the loop never
    /// retrying at all.
    ///
    /// `start_paused` makes tokio's clock auto-advance, so the real
    /// [`PIPE_BUSY_CEILING`] is exercised in microseconds of wall time — see
    /// [`open_with_retry`]'s note on why the deadline rides that clock.
    #[tokio::test(start_paused = true)]
    async fn busy_instances_that_free_up_inside_the_ceiling_still_connect() {
        let attempts = std::cell::Cell::new(0usize);
        let client = open_with_retry(r"\\.\pipe\fulcrum-test-clears", PIPE_BUSY_CEILING, || {
            let seen = attempts.get();
            attempts.set(seen + 1);
            if seen < 3 {
                Err(every_instance_is_busy())
            } else {
                Ok("armed client")
            }
        })
        .await
        .expect("a busy answer that clears inside the ceiling is not a failure");
        assert_eq!(client, "armed client", "the successful open is handed back");
        assert_eq!(
            attempts.get(),
            4,
            "three busy answers are retried and the fourth attempt is the one \
             that returns — a loop that gave up early would report fewer"
        );
    }

    /// A busy answer that NEVER clears gives up, and says which condition it was.
    ///
    /// # The property under test is that this loop is BOUNDED
    ///
    /// Delete the ceiling from [`open_with_retry`] and this test does not go RED,
    /// it HANGS — which is precisely the failure mode it exists to make
    /// impossible, and precisely what the `windows-latest` leg of CI looked like
    /// when a wait on this path had no exit. Termination is therefore the
    /// assertion that matters most, and it is the one a reader cannot see in the
    /// source: there is no `timeout(...)` wrapped around the call, because a
    /// wrapper timeout would let an unbounded loop pass by being cut off. The
    /// call is awaited bare, so an unbounded loop is an unred test binary.
    ///
    /// That claim was MEASURED, not asserted: with the `&& … < deadline` guard
    /// removed from the retry arm, this test ran 60+ seconds without returning
    /// under the `x86_64-pc-windows-gnu` test binary on wine, while all three
    /// other tests in this module stayed green. Re-add the guard and all four
    /// pass in 0.04 s. A future edit that weakens the bound therefore has a good
    /// chance of being caught by a hang that names this test.
    ///
    /// The rest of the assertions pin the SHAPE of the give-up: the error names
    /// the condition (busy, not a generic timeout), names the pipe it could not
    /// get, names the ceiling it gave up at, and carries
    /// [`std::io::ErrorKind::TimedOut`] so a caller can branch on it. Plus the
    /// retry count, which is what makes "it stopped because of the ceiling"
    /// distinguishable from "it stopped after one try" and from "it spun for a
    /// hundred thousand tries because the ceiling is absurd".
    #[tokio::test(start_paused = true)]
    async fn busy_instances_that_never_free_up_are_given_up_on_by_name_and_ceiling() {
        let attempts = std::cell::Cell::new(0usize);
        let result = open_with_retry(r"\\.\pipe\fulcrum-test-stuck", PIPE_BUSY_CEILING, || {
            attempts.set(attempts.get() + 1);
            Err::<&'static str, std::io::Error>(every_instance_is_busy())
        })
        .await;

        let Err(TransportError::Io(io)) = result else {
            panic!(
                "exhausting the ceiling must surface as `TransportError::Io` \
                 carrying the timeout, got {result:?}"
            );
        };
        assert_eq!(
            io.kind(),
            std::io::ErrorKind::TimedOut,
            "the kind says 'we ran out of patience', not 'the pipe is busy' \
             (which is what the retry loop was told to tolerate) and not \
             `NotFound` (which would read as 'no daemon'): {io}"
        );
        let message = io.to_string();
        assert!(
            message.contains(r"\\.\pipe\fulcrum-test-stuck"),
            "the message names the pipe that was busy, so an operator reading \
             it knows which endpoint to go look at: {message}"
        );
        assert!(
            message.contains(&format!("{PIPE_BUSY_CEILING:?}")),
            "the message names the ceiling it gave up at, so 'how long did it \
             wait' is answerable from the log line and not from the source: \
             {message}"
        );
        assert!(
            message.contains("busy"),
            "the message names the CONDITION, so this is not mistaken for a \
             generic timeout: {message}"
        );

        // The retry count is the ceiling's shadow: `ceiling / step` tries is what
        // a loop that honoured the bound and then stopped has to have made.
        let expected = (PIPE_BUSY_CEILING.as_millis() / BUSY_RETRY_STEP.as_millis()) as usize;
        let tried = attempts.get();
        assert!(
            tried >= expected,
            "the loop retried for the whole ceiling ({tried} attempts, \
             {expected} expected from {PIPE_BUSY_CEILING:?} / {BUSY_RETRY_STEP:?})"
        );
        assert!(
            tried <= expected * 2,
            "the loop STOPPED at the ceiling ({tried} attempts against \
             a ceiling of {PIPE_BUSY_CEILING:?} allows ~{expected}); far more \
             means the deadline is not being honoured"
        );
    }

    /// An error that is NOT the busy code is returned on the first pass.
    ///
    /// `NotFound` (no daemon running) is the caller's startup policy, not the
    /// transport's, and the ceiling must not quietly turn THAT into a five-second
    /// wait. One attempt, and the error arrives untouched — the raw code, not a
    /// `TimedOut` the transport invented.
    #[tokio::test(start_paused = true)]
    async fn an_error_that_is_not_the_busy_code_is_answered_immediately_untouched() {
        // `ERROR_FILE_NOT_FOUND`, what `CreateFileW` gives for "no daemon".
        const ERROR_FILE_NOT_FOUND: i32 = 2;
        let attempts = std::cell::Cell::new(0usize);
        let result = open_with_retry(r"\\.\pipe\fulcrum-test-absent", PIPE_BUSY_CEILING, || {
            attempts.set(attempts.get() + 1);
            Err::<&'static str, std::io::Error>(std::io::Error::from_raw_os_error(
                ERROR_FILE_NOT_FOUND,
            ))
        })
        .await;

        assert_eq!(attempts.get(), 1, "a non-busy error is not retried at all");
        let Err(TransportError::Io(io)) = result else {
            panic!("the caller's own error must come back, got {result:?}");
        };
        assert_ne!(
            io.kind(),
            std::io::ErrorKind::TimedOut,
            "the transport must not relabel someone else's error as its own \
             give-up: {io}"
        );
        assert_eq!(
            io.raw_os_error(),
            Some(ERROR_FILE_NOT_FOUND),
            "the OS's code is carried through, not swallowed"
        );
    }
}

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
//! PROOF LEVEL FOR THIS FILE: it is type-checked by
//! `cargo check --target x86_64-pc-windows-gnu --all-targets` and lint-checked
//! by the same under clippy. It has NEVER BEEN RUN — the development machine is
//! Linux with no wine and no `lib.exe`. Compilation proves the API is used with
//! the right types; it proves nothing about whether the pipe actually carries a
//! frame. The framing itself IS run, on unix, through the same `frame::Connection`.
//!
//! That limit applies squarely to the two behaviours `bind` and `accept` own —
//! the continuous-instance invariant and the `ERROR_PIPE_BUSY` retry. Both are
//! stated in code comments with the Win32 semantics they rest on, and NEITHER
//! has an executed test: there is no Windows runner in this suite, so the first
//! thing to exercise them is the Windows leg of CI, and the first thing to
//! PROVE them is a run on real Windows. Anyone who changes this file should
//! treat "clippy is clean cross-target" as necessary and nowhere near
//! sufficient.
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
//! The one Win32 call that would close the gap is `GetNamedPipeClientProcessId`,
//! and it is deliberately NOT called here. It is reachable only through the
//! `Kernel32` bindings of the `windows` / `windows-sys` crates, neither of which
//! this member declares — `Cargo.toml`'s tokio comment says the crate "is
//! deliberately ABSENT and must not be added", and this file's header repeats it.
//! Hand-rolled `#[cfg(windows)]` FFI (an `unsafe extern "system"` declaration and
//! a raw `HANDLE` cast) was considered and rejected, for three reasons:
//!
//! 1. **It fights the house pattern for its own sake.** This member's precedent
//!    for a Win32 fact is a raw constant — `is_pipe_busy`'s `231`,
//!    `childproc.rs`'s `CREATE_NO_WINDOW` — precisely to avoid a dependency. FFI
//!    is not that precedent; it is a different, heavier thing.
//! 2. **It would be an untestable unsafe surface.** `extern "system"`,
//!    `GetLastError` discipline and handle-lifetime correctness are exactly the
//!    obligations that show up at LINK time and at RUN time — and, per this file's
//!    "PROOF LEVEL" note above, this file has never been executed anywhere. An
//!    unsafe FFI boundary nobody can run is a worse trade than a documented gap
//!    anyone can read.
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

use std::ffi::OsStr;
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
/// The same 20 ms step [`connect`] uses for the same error code, for the same
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
/// `ERROR_PIPE_BUSY` when every instance is already serving a client — which is
/// a "try again in a moment", not a failure, so it is retried with a short back
/// off. `NotFound` (no daemon running) is returned as-is: retrying that is the
/// caller's startup policy, not the transport's.
pub async fn connect(endpoint: Endpoint) -> Result<Connection, TransportError> {
    let Endpoint::Pipe(name) = endpoint else {
        return Err(frame::wrong_platform("unix", "windows", endpoint.clone()));
    };
    let name: &OsStr = name.as_ref();
    let client = loop {
        match ClientOptions::new().read(true).write(true).open(name) {
            Ok(client) => break client,
            Err(e) if is_pipe_busy(&e) => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(e) => return Err(e.into()),
        }
    };
    Ok(Connection::new(client))
}

/// `ERROR_PIPE_BUSY` (231), the "every instance is busy" code.
///
/// A bare `u32` compared against `raw_os_error()` — the same no-`windows-sys`
/// approach `childproc.rs`'s `CREATE_NO_WINDOW` takes, for the same reason: a
/// constant is cheaper than a dependency.
fn is_pipe_busy(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(231)
}

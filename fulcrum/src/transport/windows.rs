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

use std::ffi::OsStr;

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
/// exactly ONE client. So this holds the NAME and mints a fresh instance per
/// [`Listener::accept`] — instance N is handed to the caller as its
/// [`Connection`], instance N+1 is created before the next wait so there is
/// always a pipe open for the next client to find.
#[derive(Debug)]
pub struct Listener {
    name: String,
}

impl Listener {
    /// Create the pipe named by `endpoint`.
    ///
    /// Errors: [`Endpoint::Socket`] on Windows → [`TransportError::WrongPlatform`];
    /// anything `CreateNamedPipeW` returns → [`TransportError::Io`].
    ///
    /// Panics outside a tokio runtime with IO enabled (tokio's `create`
    /// contract).
    pub fn bind(endpoint: Endpoint) -> Result<Self, TransportError> {
        let Endpoint::Pipe(name) = endpoint else {
            return Err(frame::wrong_platform("unix", "windows", endpoint));
        };
        // `first_pipe_instance(true)` makes a SECOND daemon on the same name
        // fail here instead of silently sharing the namespace: without it,
        // `CreateNamedPipeW` happily creates another instance and the two
        // daemons each accept a subset of the clients, which is a corrupted
        // multiplex and one of the hardest classes of bug to diagnose after the
        // fact. The error it raises is `PermissionDenied`, matching the shape
        // tokio's own example documents.
        create_pipe(&name, true)?;
        Ok(Self { name })
    }

    /// The pipe name clients must dial.
    pub fn endpoint(&self) -> Endpoint {
        Endpoint::Pipe(self.name.clone())
    }

    /// Wait for the next client.
    ///
    /// Creates the next pipe instance FIRST, then waits on this one, so a client
    /// that arrives while we are waiting for the previous one is not turned away
    /// with `ERROR_PIPE_BUSY`.
    pub async fn accept(&self) -> Result<Connection, TransportError> {
        let server = create_pipe(&self.name, false)?;
        // `connect()` completes when a client opens the pipe. Cancel-safe per
        // tokio's docs, but we do not `select!` over it either way — this is
        // awaited straight through, like the unix `accept`.
        server.connect().await?;
        Ok(Connection::new(server))
    }
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

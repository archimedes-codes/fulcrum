//! The framed transport between a client and the `fulcrum serve` daemon (T9
//! Task 3).
//!
//! TWO things live here and nowhere else in this workspace: a socket/pipe
//! listener, and the JSONL framing on top of it. The framing is
//! **one frame per `\n`-terminated JSON line — identical to the worker protocol
//! in [`fulcrum_core::worker::protocol`]**. That is deliberate: the daemon
//! speaks the worker protocol to its workers AND to its clients, and a second,
//! different framing on the client side would mean two codecs to keep in step
//! and two sets of framing bugs. Do not "improve" the client framing.
//!
//! # The platform split
//!
//! `Listener` / `connect` exist once per platform, under `#[cfg(unix)]`
//! (`unix.rs`, `tokio::net::UnixListener`) and `#[cfg(windows)]`
//! (`windows.rs`, `tokio::net::windows::named_pipe`), and `mod.rs` re-exports
//! whichever one compiled — so a caller writes `transport::Listener` and never
//! a `cfg`. `Endpoint` and [`TransportError`] are the platform-neutral surface
//! both legs speak, defined HERE, unconditional.
//!
//! The gating is load-bearing and is NOT to be replaced by `if cfg!(unix)`: a
//! `cfg!` condition is a runtime `if`, so the inactive platform's code still
//! compiles into the binary and then trips `dead_code`/`unused` under
//! `cargo clippy --all-targets -- -D warnings`. This repo has been caught by
//! that twice. `cfg` on the `mod` items removes the module from the build
//! outright, which is the same mechanism `childproc.rs`'s `_`-prefixed
//! `hide`/`append_cmd_c` family uses for the smaller cases.
//!
//! # The reader is a task, not a future
//!
//! [`Connection`] puts its line reader on a dedicated [`tokio::spawn`]ed task
//! for the reason `client.rs`'s `spawn_pipe_reader` gives: `AsyncBufReadExt::read_line`
//! appends into the caller's `String` as it goes and is **not cancellation-safe**
//! — a `select!` that abandons a partially-filled `read_line` loses the bytes it
//! already consumed. On a socket that is not a theoretical problem, it is a
//! silently truncated frame. See the spawn site in [`frame`] for the full note.
//!
//! # Out of scope: replay and sessions (T10)
//!
//! No `attach`, no `afterSeq`, no replay buffer, no session registry. The
//! protocol already stamps `frameSeq` on outbound frames
//! (`fulcrum_core::worker::protocol::stamp_outbound` — 1-based, reset per
//! `Start`) and nothing consumes it yet; this transport deliberately neither
//! reads nor writes it, so T10 can build replay on the same counter without
//! reconciling a second numbering the transport invented.

pub mod frame;

#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

pub use frame::Connection;

#[cfg(unix)]
pub use unix::{connect, default_endpoint, Listener};
#[cfg(windows)]
pub use windows::{connect, default_endpoint, Listener};

/// Where a [`Listener`] binds, and where [`connect`] dials.
///
/// BOTH variants exist on EVERY platform, and so do both constructors. That is
/// not tidiness: `Endpoint` is the type the platform-neutral API takes, so a
/// `cfg`-gated variant would make `Endpoint::pipe(...)` — or the `match` arm
/// that rejects it — a compile error on the other OS instead of an ordinary
/// runtime error. The platform decision belongs in `Listener`/`connect`, which
/// ARE gated; `Endpoint` just names an address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// An `AF_UNIX` socket file. Only bindable on unix.
    Socket(std::path::PathBuf),
    /// A Windows named pipe, `\\.\pipe\<name>`. Only bindable on Windows.
    Pipe(String),
}

impl Endpoint {
    /// The unix socket endpoint at `path`.
    pub fn socket(path: impl Into<std::path::PathBuf>) -> Self {
        Self::Socket(path.into())
    }

    /// The named-pipe endpoint `name` (pass the full `\\.\pipe\…` form).
    pub fn pipe(name: impl Into<String>) -> Self {
        Self::Pipe(name.into())
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Socket(path) => write!(fmt, "{}", path.display()),
            Self::Pipe(name) => write!(fmt, "{name}"),
        }
    }
}

/// Everything this transport can fail with.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// A socket/pipe syscall or a read/write on a live connection.
    #[error("transport io error: {0}")]
    Io(#[from] std::io::Error),
    /// An outbound frame would not serialize.
    #[error("encode frame: {0}")]
    Encode(String),
    /// An inbound line was not a valid JSON value. Carried as a stream item,
    /// never as a closed connection — see [`Connection::recv`].
    #[error("decode frame: {0}")]
    Decode(String),
    /// A unix socket path that `sockaddr_un` cannot hold.
    ///
    /// `sun_path` is a FIXED-size array (104 bytes on macOS/BSD, 108 on Linux),
    /// so a long path fails in `bind()` with `EINVAL` — which names neither the
    /// path nor the limit. Both are reported here because the fix (move the
    /// socket) needs both.
    #[error(
        "socket path is {length} bytes, over the {limit}-byte `sockaddr_un` \
         sun_path limit: {path}"
    )]
    SocketPathTooLong {
        /// The path's length in bytes (not chars — the limit is bytes).
        length: usize,
        /// The limit that was exceeded, on THIS platform.
        limit: usize,
        /// The offending path.
        path: String,
    },
    /// The endpoint names the other platform's address family (a pipe on unix,
    /// a socket on Windows).
    #[error("{family} endpoint {endpoint} is not supported on this platform ({platform})")]
    WrongPlatform {
        /// `unix` or `windows` — the family the endpoint names.
        family: &'static str,
        /// The platform the call is running on.
        platform: &'static str,
        /// The endpoint that was rejected.
        endpoint: Endpoint,
    },
    /// `default_endpoint` had nowhere to put the socket.
    #[error("no platform data directory available (dirs::data_dir() returned None)")]
    NoDataDir,
}

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
//! # Every buffer here is bounded (finding 7)
//!
//! Two, and both are reachable by ANY local process: `serve` has no
//! authentication — legitimately deferred to T13 — so the socket is the only
//! credential, and a buffer with no ceiling is a denial-of-service with no
//! prerequisites.
//!
//! * [`frame::MAX_INBOUND_LINE_BYTES`] — the longest LINE one peer may make the
//!   daemon assemble. A peer that sends bytes with no `\n` used to make
//!   `read_line` grow for as long as it could send.
//! * [`frame`]'s inbound queue (`MAX_QUEUED_INBOUND_FRAMES`) — how many decoded
//!   lines may sit undelivered before the reader task stops draining the socket.
//!   Measured before the bound: one connection, ten seconds, a client that read
//!   nothing, **1 168 009 requests absorbed and the daemon's RSS 15 MB → 895 MB**.
//!
//! Both respond with BACKPRESSURE rather than by dropping: the reader waits, the
//! socket fills, and the peer's own `write` blocks. Dropping an inbound REQUEST
//! would be a client bug the daemon caused. The one exception is the oversized
//! line, which cannot be backpressured (the peer is not going to stop) and cannot
//! be skipped either — see [`TransportError::LineTooLong`] for why it ends the
//! connection instead.
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

pub use frame::{Connection, ConnectionWriter};

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
    /// An inbound line exceeded `transport::frame::MAX_INBOUND_LINE_BYTES`
    /// without a terminator (finding 7).
    ///
    /// The ONE error item that is also a full stop: it is delivered as the last
    /// item and the stream ends. Skipping the bytes and resyncing on the next
    /// newline would keep the connection and lose the agreement about where
    /// frames begin — the client believes it sent a frame, the daemon has
    /// re-cut the stream — and a desync is the failure this transport's framing
    /// exists to prevent. Closing is the honest answer, and it costs the attacker
    /// a reconnect instead of the daemon a buffer.
    #[error("inbound line exceeds the {limit}-byte limit")]
    LineTooLong {
        /// The limit that was exceeded, so the message names the number the peer
        /// has to get under.
        limit: usize,
    },
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
    /// A unix peer whose uid is not the daemon's own (issue #4).
    ///
    /// The ONLY thing the local transport can refuse a peer for on its own
    /// account: `serve` has no authentication (deferred to T13), so before this
    /// variant existed the transport had no vocabulary for "I know who you are
    /// and you are not who I am". The peer's uid comes from the kernel
    /// (`SO_PEERCRED` on Linux, `getpeereid` on the BSDs) and is not forgeable by
    /// the peer, so this is a real refusal rather than a hint.
    ///
    /// Both uids are fields rather than prose because the accept loop reports
    /// rejections on a COUNTED schedule (a hostile local process can connect
    /// repeatedly, and its attempts must not be able to flood the operator's
    /// stderr), and because `serve::run` matches on the variant to pick that path
    /// instead of the generic accept-failure log line.
    ///
    /// What this does NOT claim: that a peer under the daemon's own uid is
    /// trusted. Same-uid peers pass, and that residual is exactly what the
    /// socket's 0600 mode cannot close either — see `unix.rs`'s module header,
    /// and T13 for the protocol-layer answer.
    #[error("peer uid {peer_uid} is not the daemon's uid {expected_uid}")]
    PeerIdentity {
        /// The uid the kernel reported for the connecting process.
        peer_uid: u32,
        /// The uid the daemon demanded — its own effective uid.
        expected_uid: u32,
    },
    /// `default_endpoint` had nowhere to put the socket.
    #[error("no platform data directory available (dirs::data_dir() returned None)")]
    NoDataDir,
}

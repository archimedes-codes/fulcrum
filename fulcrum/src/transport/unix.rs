//! `AF_UNIX` listener + client (T9 Task 3).
//!
//! Declared `#[cfg(unix)]` in `transport/mod.rs`: ungated, its body naming
//! `tokio::net::UnixListener` fails the Windows target with `E0433: cannot find
//! 'UnixListener' in 'net'`. The Windows twin is `windows.rs`, and `mod.rs`
//! re-exports one of the two, so callers see one `Listener`.

use std::path::{Path, PathBuf};

use tokio::net::{UnixListener as TokioUnixListener, UnixStream};

use crate::transport::frame::{self, Connection};
use crate::transport::{Endpoint, TransportError};

/// The `sun_path` array size in `sockaddr_un`, in bytes.
///
/// Read off `libc`'s own struct rather than hardcoded: it is 108 on Linux and
/// 104 on macOS/BSD, and the plan's "~104 bytes" is the macOS figure — the one
/// that bites first, because the same data-dir path that fits on Linux can
/// overflow on a Mac.
fn sun_path_capacity() -> usize {
    // `zeroed` is sound for a plain C struct of integers/arrays, and it is the
    // only way to ask "how big is that field" without constructing a socket.
    let addr = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    std::mem::size_of_val(&addr.sun_path)
}

/// Reject a path `sockaddr_un` cannot hold, naming BOTH the length and the path.
///
/// Without this, `bind()` fails with `EINVAL` (`Invalid argument`) and says
/// neither — the developer is left guessing which of a dozen paths was too long,
/// and the fix (put the socket somewhere short) depends on knowing the path.
///
/// Byte length, not `chars().count()`: `sun_path` holds bytes and a multi-byte
/// directory name costs what it costs. Abstract namespace names (a leading NUL,
/// Linux) get the same treatment; that is one byte stricter than the kernel
/// requires there, which is the safe direction to be wrong.
fn check_socket_path_len(path: &Path) -> Result<(), TransportError> {
    let bytes = path.as_os_str().len();
    // One byte is the NUL terminator `connect`/`bind` require.
    let limit = sun_path_capacity() - 1;
    if bytes > limit {
        return Err(TransportError::SocketPathTooLong {
            length: bytes,
            limit,
            path: path.display().to_string(),
        });
    }
    Ok(())
}

/// The daemon's socket: `<data_dir>/fulcrum/daemon.sock`.
///
/// The LENGTH IS CHECKED HERE, not left to `bind`: the default data dir is
/// usually short but is not guaranteed to be (`$HOME` is whatever the machine
/// says), and on macOS 104 bytes is easy to overrun. Failing at `bind` with
/// `EINVAL` would hand the operator an error that names nothing.
///
/// The parent directory is NOT created here — [`Listener::bind`] creates it, so
/// an endpoint that was never this one's default still gets a directory.
pub fn default_endpoint() -> Result<Endpoint, TransportError> {
    let data_dir = dirs::data_dir().ok_or(TransportError::NoDataDir)?;
    let path = data_dir.join("fulcrum").join("daemon.sock");
    check_socket_path_len(&path)?;
    Ok(Endpoint::Socket(path))
}

/// A daemon-side `AF_UNIX` listener.
#[derive(Debug)]
pub struct Listener {
    listener: TokioUnixListener,
    endpoint: Endpoint,
}

impl Listener {
    /// Bind `endpoint`.
    ///
    /// Errors, in order of how often they happen:
    /// - [`Endpoint::Pipe`] on unix → [`TransportError::WrongPlatform`].
    /// - A path over `sun_path` → [`TransportError::SocketPathTooLong`].
    /// - The parent directory missing → created with `create_dir_all`.
    /// - A socket file already at that path → see [`reclaim_stale_socket`].
    ///
    /// Panics if called outside a tokio runtime with IO enabled (tokio's
    /// `UnixListener::bind` contract) — true of every `bind` here, and the
    /// daemon is a runtime by definition.
    pub fn bind(endpoint: Endpoint) -> Result<Self, TransportError> {
        let Endpoint::Socket(path) = endpoint else {
            // The `let … else` only moves the `Socket` payload on the success
            // path, so the rejection path still owns `endpoint` and can move it
            // straight into the error.
            return Err(frame::wrong_platform("windows", "unix", endpoint));
        };
        bind_socket(path)
    }

    /// The endpoint this listener is bound to (what a client must dial).
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint.clone()
    }

    /// Wait for the next client.
    ///
    /// The `Listener` is taken by `&self`, so one accept loop can serve many
    /// connections; each returned [`Connection`] owns its own reader task.
    pub async fn accept(&self) -> Result<Connection, TransportError> {
        let (stream, _addr) = self.listener.accept().await?;
        Ok(Connection::new(stream))
    }
}

/// The filesystem half of [`Listener::bind`], split out so the `cfg`-specific
/// body is one function and the `Endpoint` unpacking above stays readable.
fn bind_socket(path: PathBuf) -> Result<Listener, TransportError> {
    check_socket_path_len(&path)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    match TokioUnixListener::bind(&path) {
        Ok(listener) => Ok(Listener {
            listener,
            endpoint: Endpoint::Socket(path),
        }),
        // A crashed daemon leaves its socket FILE behind, and `bind` then fails
        // `EADDRINUSE` — so a restart would be impossible without manual `rm`.
        // That is the daemon's own failure mode, so the transport reclaims it.
        Err(e) if e.raw_os_error() == Some(libc::EADDRINUSE) => {
            reclaim_stale_socket(&path)?;
            let listener = TokioUnixListener::bind(&path)?;
            Ok(Listener {
                listener,
                endpoint: Endpoint::Socket(path),
            })
        }
        Err(e) => Err(e.into()),
    }
}

/// Remove a socket file nothing is listening on, so a restart can bind again.
///
/// Only reachable from the `EADDRINUSE` arm, and the decision is a PROBE, not a
/// guess: `connect` to the path. Nothing listening answers `ECONNREFUSED` (the
/// inode exists but no bound socket accepts) → the file is stale, unlink it. A
/// live daemon answers, and its socket is left strictly ALONE — deleting a live
/// daemon's socket would silently cut every existing client off, which is far
/// worse than failing to start.
///
/// The probe is `std`'s BLOCKING connect, not tokio's: `bind` is a sync fn (the
/// daemon calls it before the accept loop is running) and a `connect(2)` on a
/// local socket either succeeds or fails immediately — it never blocks on a
/// peer, so there is nothing for an async reader to be stranded by.
///
/// THE PROBE'S SIDE EFFECT, because it is observable from the other process: a
/// successful probe CONNECTION IS QUEUED on the live daemon, and its `accept()`
/// will return that socket, which reads as an immediate EOF. So an accidental
/// second `fulcrum serve` shows up on the first one as one connection that
/// closes instantly. There is no way to test whether a stream socket has a
/// listener without connecting to it (and a DGRAM probe cannot tell, because
/// connecting one to a stream listener always gives `ECONNREFUSED`), and an
/// accept loop has to survive a client that EOFs before sending anything
/// regardless — that is what a `curl`-style probe does to it every day.
///
/// THE RESIDUAL RACE, stated because it cannot be removed from userspace:
/// between the probe and the `remove_file` another process could bind that path,
/// and this unlink would then delete a live socket. It is a window of
/// microseconds on a path the daemon owns in its own data dir, and the
/// consequence is bounded by the retry below: if we unlink and someone else
/// re-binds first, our `bind` fails `EADDRINUSE` again and the caller sees that.
/// `unlink` + `bind` is not atomic on any unix; SO_REUSEADDR does not apply to
/// `AF_UNIX`.
fn reclaim_stale_socket(path: &Path) -> Result<(), TransportError> {
    if std::os::unix::net::UnixStream::connect(path).is_ok() {
        return Err(TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            format!(
                "{} is in use: a live daemon is answering on it",
                path.display()
            ),
        )));
    }
    // The connect failed ⇒ nothing is listening ⇒ the inode is stale.
    std::fs::remove_file(path)?;
    Ok(())
}

/// Dial the daemon at `endpoint`.
///
/// The client side lives here (not in `frame`) for the same reason the listener
/// does: `UnixStream` does not exist on Windows, so the dial belongs to the
/// platform leg and only the framing is shared.
pub async fn connect(endpoint: Endpoint) -> Result<Connection, TransportError> {
    let Endpoint::Socket(path) = endpoint else {
        return Err(frame::wrong_platform("windows", "unix", endpoint));
    };
    check_socket_path_len(&path)?;
    let stream = UnixStream::connect(&path).await?;
    Ok(Connection::new(stream))
}

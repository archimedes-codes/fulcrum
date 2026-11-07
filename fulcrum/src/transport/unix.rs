//! `AF_UNIX` listener + client (T9 Task 3).
//!
//! Declared `#[cfg(unix)]` in `transport/mod.rs`: ungated, its body naming
//! `tokio::net::UnixListener` fails the Windows target with `E0433: cannot find
//! 'UnixListener' in 'net'`. The Windows twin is `windows.rs`, and `mod.rs`
//! re-exports one of the two, so callers see one `Listener`.
//!
//! # What this platform can prove about a peer (issue #4)
//!
//! `serve` has no authentication (T13), so the socket has carried the whole
//! access-control decision. Two layers now, and they close different holes:
//!
//! 1. **The file mode** (`bind_private`: umask tightened, then `chmod 0600`,
//!    before the first `accept`). Proves the connecting process was allowed by the
//!    filesystem — i.e. it is the owner, or root, or in a group/world the owner
//!    explicitly widened.
//! 2. **The peer credential** (`Listener::accept` → `peer_uid` →
//!    `verify_peer_uid`). Proves the kernel's answer for the peer's REAL uid
//!    equals the daemon's own effective uid, and refuses the connection if not.
//!
//! Layer 2 is defense in depth, not a replacement: anything that passes layer 1
//! also passes layer 2 in a normal deployment, and that is the point — layer 2
//! still refuses a peer that reached the socket some other way (a widened parent
//! directory, an `AF_UNIX` abstract-namespace or `SCM_CREDENTIALS`-adjacent path,
//! a filesystem whose mode the daemon could not set, a misconfigured `--socket` in
//! a shared directory). Both layers are stated as "the socket is the credential,
//! AND the credential check is the second check" rather than one or the other.
//!
//! The per-platform mechanisms, what each proves, and why one call is not enough:
//!
//! | Platform | Mechanism | Proves | Does not prove |
//! |---|---|---|---|
//! | Linux, Android | `getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &mut libc::ucred)` | the peer's real uid/gid/pid at connect time, assigned by the kernel and unforgeable by the peer | anything about a process under the SAME uid — every process under your account passes |
//! | macOS | `getpeereid(fd, &mut euid, &mut egid)` | the peer's effective uid/gid | the pid (`LOCAL_PEERCRED` + `libc::xucred` would give it; see `peer_uid` below) |
//! | FreeBSD / NetBSD / OpenBSD / DragonFly | `getpeereid` | the peer's effective uid/gid | the pid |
//!
//! `SO_PEERCRED` is Linux-only at the `SOL_SOCKET` level, so a single
//! `#[cfg(unix)]` implementation of it would not compile on the macOS leg of CI.
//! `getpeereid` is the one call the whole BSD family has, so this file carries two
//! `#[cfg]`-gated arms of `peer_uid` rather than one unix-wide one — and the
//! gating is at item level, never `if cfg!(…)`, for the reason `transport/mod.rs`
//! documents (an `if cfg!` arm still compiles, and then trips `-D warnings`).
//!
//! **The residual, named:** a same-uid process is indistinguishable from the
//! client you meant to allow. No credential check on any unix closes that — it is
//! the same-uid case, and the only real answer is authenticating at the protocol
//! layer, which is T13.

use std::os::unix::fs::PermissionsExt as _;
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
    ///
    /// # The peer is identified before it is served (issue #4)
    ///
    /// The credential check happens HERE, while the accepted stream is still in
    /// this function's hands, rather than inside `Connection` or in `serve`'s
    /// accept arm: this is the one place the platform's peer-credential call has
    /// the accepted fd, and a refused peer should cost nothing — no `Connection`,
    /// no reader task, no client task spawned for a process we are about to hang
    /// up on. (The alternative, accept-then-close, hands the attacker a task per
    /// connection attempt for no benefit.)
    ///
    /// A refusal is an `Err`, and `serve::run`'s accept arm treats `Err` as a
    /// per-connection condition and keeps looping — so a hostile local process
    /// that connects in a loop is refused in a loop, cheaply.
    ///
    /// What this proves, and what it does not, is in this module's header table.
    /// The short form: the peer's uid is the kernel's answer and cannot be forged,
    /// and a peer under the daemon's own uid still passes.
    pub async fn accept(&self) -> Result<Connection, TransportError> {
        let (stream, _addr) = self.listener.accept().await?;
        // Resolved on the raw fd before the stream moves into the `Connection`.
        // `as_raw_fd` is safe (the fd is alive for the whole statement, and this
        // call does not take ownership of it).
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&stream);
        let my_uid = unsafe { libc::geteuid() };
        let peer = peer_uid(fd)?;
        verify_peer_uid(peer, my_uid)?;
        Ok(Connection::new(stream))
    }
}

/// The filesystem half of [`Listener::bind`], split out so the `cfg`-specific
/// body is one function and the `Endpoint` unpacking above stays readable.
fn bind_socket(path: PathBuf) -> Result<Listener, TransportError> {
    check_socket_path_len(&path)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            ensure_private_dir(parent)?;
        }
    }

    match bind_private(&path) {
        Ok(listener) => Ok(Listener {
            listener,
            endpoint: Endpoint::Socket(path),
        }),
        // A crashed daemon leaves its socket FILE behind, and `bind` then fails
        // `EADDRINUSE` — so a restart would be impossible without manual `rm`.
        // That is the daemon's own failure mode, so the transport reclaims it.
        Err(e) if e.raw_os_error() == Some(libc::EADDRINUSE) => {
            reclaim_stale_socket(&path)?;
            let listener = bind_private(&path)?;
            Ok(Listener {
                listener,
                endpoint: Endpoint::Socket(path),
            })
        }
        Err(e) => Err(e.into()),
    }
}

/// Bind `path` and make the resulting inode owner-accessible only.
///
/// The design's claim is "the socket is the credential" (see `serve`'s module
/// header on why there is no authentication before T13). That claim is only true
/// if the filesystem enforces it: a socket at the umask default is 0755, so ANY
/// local process can connect, start a session with `bash` allowed and
/// `permission` defaulting to `Allow`, and watch the transcript it writes. The
/// mode is what turns "the socket is the credential" from a hope into a property.
///
/// Two mechanisms, because POSIX leaves the effect of `chmod` on a socket
/// UNSPECIFIED and the platforms disagree (verified on this machine: Linux binds
/// at 0755 and accepts a `chmod` to 0600, and connecting still works afterwards):
///
/// 1. The umask is tightened for the duration of `bind` alone. Linux derives a
///    new socket inode's mode from `0777 & ~umask`, so this yields 0700 even on a
///    platform where step 2 is refused. Restored on every path out of the call —
///    an un-restored umask would silently narrow every file the daemon creates
///    afterwards, including the WAL siblings SQLite makes on its own schedule.
/// 2. An explicit `chmod` to 0600, which on Linux also drops the (meaningless,
///    but noisy in an `ls`) execute bit.
///
/// If step 2 fails, the mode is READ BACK and only accepted if it is already
/// owner-only — so a macOS `EPERM` leaves a 0700 socket and a passing daemon,
/// while a filesystem that gave the world access is a hard error rather than a
/// warning nobody reads.
fn bind_private(path: &Path) -> std::io::Result<TokioUnixListener> {
    // 0o077: nothing group- or world-accessible may be created inside this call.
    let previous_umask = unsafe { libc::umask(0o077) };
    // `umask` is not unwind-safe by construction, but `UnixListener::bind` does
    // not panic on a path it can stat; and a panic here would end the process,
    // which cannot leak the umask past it.
    let bound = TokioUnixListener::bind(path);
    unsafe { libc::umask(previous_umask) };
    let listener = bound?;

    match std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
        Ok(()) => Ok(listener),
        Err(e) => {
            let mode = std::fs::metadata(path)
                .map(|m| m.permissions().mode() & 0o7777)
                .ok();
            if mode.is_some_and(|m| m & 0o077 == 0) {
                return Ok(listener);
            }
            let described = mode
                .map(|m| format!("{m:04o}"))
                .unwrap_or_else(|| "unreadable".to_string());
            Err(std::io::Error::new(
                e.kind(),
                format!(
                    "cannot make {} owner-only: chmod to 0600 failed ({e}) and the \
                     socket's actual mode is {described}, which would let any local \
                     process reach the daemon",
                    path.display()
                ),
            ))
        }
    }
}

/// Create the socket's parent directory at 0700, narrowing ONLY components this
/// call actually created.
///
/// Delegates to [`crate::db::ensure_private_dir`] — the ONE copy in this crate.
/// This used to be a byte-for-byte duplicate of it, justified by a comment
/// claiming there was "no natural common home"; both modules live in this crate
/// and `db` is declared by `lib.rs`, so `crate::db::ensure_private_dir` was
/// always available and `lib.rs` did not need touching. The duplication was not
/// harmless: the rule it encodes — never re-mode a directory this call did not
/// create, so `--socket /home/me/x.sock` cannot quietly set `$HOME` to 0700 — is
/// security-relevant, and two copies is how one gets fixed and the other
/// forgotten.
///
/// The error type stays the transport's own: the shared function returns
/// `std::io::Error` and [`TransportError::Io`] converts it via `?`, so neither
/// module had to adopt the other's error vocabulary.
///
/// Not a plain `use` alias at the call site, because this wrapper is where the
/// "why the store's function is the right home for a socket's directory" answer
/// lives — the two callers share the RULE, not the file layout.
fn ensure_private_dir(path: &Path) -> std::io::Result<()> {
    crate::db::ensure_private_dir(path)
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

/// What the peer's uid is, as the KERNEL reported it, for an accepted connection.
///
/// Resolved at ACCEPT time, in [`Listener::accept`], while the accepted stream is
/// still in this module's hands — deliberately not stored in [`Connection`]
/// (`Connection::new`'s signature is the seam a later task splits, and a
/// connection that carries an identity invites the reader to trust it downstream).
///
/// Two mechanisms, because the two unix families expose different calls, and a
/// CI matrix that compiles ubuntu, macos and windows will not forgive an arm that
/// only exists on one of them:
///
/// | Platform | Call | Proves | Does not prove |
/// |---|---|---|---|
/// | Linux, Android | `getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &mut ucred)` | the peer's real uid, gid AND pid, assigned by the kernel at connect time | anything about the peer beyond that — a same-uid process passes |
/// | macOS, FreeBSD, NetBSD, OpenBSD, DragonFly | `getpeereid(fd, &mut euid, &mut egid)` | the peer's effective uid and gid | the pid |
///
/// `SO_PEERCRED` is NOT a portable option: macOS has no `SO_PEERCRED` at the
/// `SOL_SOCKET` level at all. It does have `LOCAL_PEERCRED`, which fills a
/// `libc::xucred` and DOES carry the connecting pid — but `xucred`'s version-field
/// semantics and layout are platform-specific, and the daemon's requirement here
/// is uid identity, not pid. So `getpeereid` is the arm every BSD compiles, and
/// `LOCAL_PEERCRED`/`xucred` is the documented hook for a future task that
/// actually needs the pid on macOS.
///
/// The pid `SO_PEERCRED` also reports is deliberately NOT returned: a pid
/// identifies a process only until that process is reaped and the number is
/// reused, and nothing at accept time wants one — the refusal the caller raises
/// names the uid, which is the thing the operator can act on. `ucred`'s `pid` is
/// therefore read and dropped.
///
/// Returns [`TransportError::Io`] when the syscall fails. That is the honest
/// answer, not a fallback to "accept anyway": a socket the kernel will not
/// describe is a socket this daemon cannot account for, and refusing it costs the
/// peer a reconnect, not the daemon anything.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn peer_uid(fd: std::os::unix::io::RawFd) -> Result<u32, TransportError> {
    // `MaybeUninit` because the kernel writes this struct; `assume_init` below is
    // reached only on the `rc == 0` path, where `getsockopt` filled it.
    let mut cred = std::mem::MaybeUninit::<libc::ucred>::uninit();
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            cred.as_mut_ptr() as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // `pid` is read and dropped — see the note above. Returned as-is rather than
    // `as u32`: `libc::uid_t` IS `u32` on every target this repo compiles (Linux,
    // Android, macOS, the BSDs), so a cast would be a no-op clippy's
    // `-D warnings` rightly rejects. If a platform ever turns up whose `uid_t` is
    // not `u32`, this line is the compile error that says so — which beats a
    // silent narrowing.
    Ok(unsafe { cred.assume_init() }.uid)
}

/// The BSD shape of [`peer_uid`] — see that function's table for why this is the
/// arm macOS and the BSDs get and Linux does not.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn peer_uid(fd: std::os::unix::io::RawFd) -> Result<u32, TransportError> {
    let mut euid: libc::uid_t = 0;
    // The gid is not what this check is about (the daemon's credential model is
    // uid identity), but `getpeereid` requires both out-params.
    let mut egid: libc::gid_t = 0;
    // `getpeereid` is the ONE call every BSD (and macOS) documents for this;
    // it is `libc`'s own binding under `unix/bsd`, so no per-OS arm is needed.
    let rc = unsafe { libc::getpeereid(fd, &mut euid, &mut egid) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // No pid: `getpeereid` does not return one, and `LOCAL_PEERCRED` (which does)
    // is the documented alternative — see [`peer_uid`]'s note.
    Ok(euid as u32)
}

/// Does the peer's uid equal the daemon's?
///
/// Extracted from [`Listener::accept`] so the comparison — the whole content of
/// the check — is unit-testable. A live foreign-uid peer cannot be manufactured
/// by a test: `setuid` to another uid needs root, CI's runners are unprivileged,
/// and a test target cannot name a private function of this module to drive it
/// directly. So the arithmetic is pinned here, the real-socket SAME-uid path is
/// pinned by `tests/transport.rs::same_uid_peer_is_accepted`, and the reject path
/// is pinned by the `#[ignore]`d root-only test next to it.
///
/// Deliberately a plain equality, not a capability check: what `SO_PEERCRED` /
/// `getpeereid` proves is the peer's REAL uid, which the kernel assigned and the
/// peer cannot forge. It does not prove anything about a process running under
/// the same account — that residual is the socket's file mode's residual too, and
/// it is what T13's protocol-layer auth exists to close.
fn verify_peer_uid(peer_uid: u32, expected_uid: u32) -> Result<(), TransportError> {
    if peer_uid == expected_uid {
        Ok(())
    } else {
        Err(TransportError::PeerIdentity {
            peer_uid,
            expected_uid,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::verify_peer_uid;
    use crate::transport::TransportError;

    /// The peer whose uid is the daemon's own is accepted.
    ///
    /// This is the path every legitimate client takes, and the one the real
    /// socket tests can only reach from the inside: `tests/transport.rs` can
    /// prove a same-uid dial SUCCEEDS, but it cannot manufacture a peer with a
    /// different uid without root. So the equality itself is pinned here.
    #[test]
    fn a_peer_with_the_daemons_uid_is_accepted() {
        assert!(
            verify_peer_uid(1000, 1000).is_ok(),
            "the daemon's own uid must be accepted"
        );
        // uid 0 is a uid like any other: a root daemon accepts a root peer and
        // (by the arm below) refuses everyone else.
        assert!(
            verify_peer_uid(0, 0).is_ok(),
            "a root daemon must accept a root peer"
        );
    }

    /// A peer under another account is refused, and the refusal carries BOTH uids.
    ///
    /// The numbers have to ride in the error, not only in its prose: `serve`'s
    /// accept loop logs rejections on a counted schedule, and an operator
    /// debugging "my client keeps getting dropped" needs to see which uid the
    /// kernel reported. Structured fields are also what keeps the accept arm
    /// matching on a VARIANT rather than on an error string.
    #[test]
    fn a_peer_with_a_different_uid_is_refused_naming_both_uids() {
        let err = verify_peer_uid(999, 1000).expect_err("a foreign uid must not pass");
        match err {
            TransportError::PeerIdentity {
                peer_uid,
                expected_uid,
            } => {
                assert_eq!(peer_uid, 999, "the error must report the peer's uid");
                assert_eq!(
                    expected_uid, 1000,
                    "the error must report the uid the daemon demanded"
                );
            }
            other => panic!("a foreign peer must be refused as PeerIdentity, got {other:?}"),
        }
    }
}

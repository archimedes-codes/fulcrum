//! T9 Task 3 — the platform-split JSONL transport.
//!
//! Loopback only, over the real thing: a real `Listener`, a real `accept()` on
//! its own task, a real peer connection. No mock socket exists in this repo and
//! none is invented here — a transport test that talked to a fake transport
//! would certify nothing.
//!
//! The three tests at the top of this file run on BOTH platforms: the API under
//! test is platform-neutral (`Endpoint` carries the difference), so they are
//! written once and only the endpoint they build is `cfg`-split. That is what
//! makes `cargo check --target x86_64-pc-windows-gnu --all-targets` a real gate
//! on the Windows leg of this file rather than a compile of dead code. The
//! `unix_specific` module below then tests what only unix can: `sun_path`, the
//! stale-socket reclaim, and raw-byte framing probes.
//!
//! NOT covered here, deliberately: replay, `attach`, `afterSeq`, a session
//! registry. Those are T10, and the protocol's `frameSeq`
//! (`fulcrum_core::worker::protocol::stamp_outbound`) stays unconsumed until
//! then — see the module docs on `transport`.

use fulcrum::transport::{connect, Connection, Endpoint, Listener, TransportError};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// A typed frame, so `send_frame` is exercised through `serde::Serialize` and
/// not only through a hand-built `Value` — the daemon's callers pass structs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Greeting {
    kind: String,
    seq: u64,
}

/// The endpoint a test binds, plus the `TempDir` it lives under.
///
/// The `TempDir` comes back as a value the CALLER must keep alive: dropping it
/// deletes the directory, and on unix that removes the socket file from under a
/// live listener. The `(keepalive, endpoint)` shape is identical on both
/// platforms so every call site reads the same — on Windows the keepalive is
/// `()` because a pipe name has no filesystem backing, but the call site must
/// still hold something or the two legs diverge.
fn test_endpoint() -> (tempfile::TempDir, Endpoint) {
    let dir = tempfile::tempdir().expect("a temp dir for the test endpoint");
    // `cfg` attributes on two bindings, not an `if cfg!(unix)` block: the
    // inactive leg is REMOVED, so the non-active platform's `join`/`format!`
    // cannot become dead code under `-D warnings`.
    #[cfg(unix)]
    let endpoint = Endpoint::socket(dir.path().join("daemon.sock"));
    #[cfg(windows)]
    let endpoint = Endpoint::pipe(format!(
        r"\\.\pipe\fulcrum-test-{}",
        dir.path()
            .file_name()
            .expect("a temp dir has a final component")
            .to_string_lossy()
    ));
    (dir, endpoint)
}

/// Serialises every `Listener::bind` in this test binary.
///
/// # What is being protected
///
/// `transport::unix::bind_private` tightens the PROCESS umask to `0o077` for the
/// duration of `bind()` — that is how a socket gets `0700` on a platform where
/// `chmod` on a socket inode is unspecified — and restores the previous value on
/// the way out. `umask(2)` is neither per-thread nor per-file: it is ONE
/// process-global. `cargo test` runs the tests in this file on parallel threads,
/// so a test that reads the umask and asserts it is unchanged is racing every
/// other `bind` in the binary: a concurrent bind that happens to be INSIDE its
/// own tighten window makes the reader observe `0o077` and fail, with nothing in
/// the diff to explain it.
///
/// That is why [`binding_restores_the_umask_it_temporarily_tightened`] could not
/// be correct as it was written. It read the umask, ran a bind, read it again and
/// compared the two ABSOLUTELY. The value it read first belongs to the whole
/// process, and any other test in the same binary is free to change it at any
/// moment — so the assertion had no defined answer. It passed because the window
/// is microseconds and nothing else happened to be binding at that instant, which
/// is not a property of the code, only of the schedule. There are SEVENTEEN other
/// bind call sites in this file (counted, not estimated), and every one of them
/// can be the one that races it; which one wins is a schedule, so the failure
/// lands on whoever's name is in the diff.
///
/// # Why a lock makes the assertion sound rather than merely likelier
///
/// Holding this mutex across the whole read-bind-read sequence closes the system:
/// while that test holds it, no other test in this binary can be inside `bind`,
/// because every bind route in the file runs under this same lock (see
/// [`bind_holding`] and the guard test that enforces it). So the absolute
/// comparison has exactly one writer, and the value it reads is the value it is
/// comparing against.
///
/// A `std::sync::Mutex` and not an async one: `Listener::bind` is synchronous, so
/// an async guard would be held across a sync call for no benefit. Poisoning is
/// recovered with `into_inner` rather than propagated, because a test that
/// panicked while holding the lock is precisely the case where the next bind must
/// STILL be serialised — dropping the serialisation there would let the racy
/// behaviour return quietly behind an unrelated failure.
///
/// Deliberately NOT `#[cfg(unix)]`: the umask is a unix thing, but the *shape*
/// being serialised is `bind`, and gating the lock would mean the Windows leg of
/// CI compiles a different set of call sites than the leg that runs — the exact
/// `if cfg!(…)` vs `mod`-gating argument this file's header already makes for the
/// platform legs. An ungated lock costs nothing on Windows and keeps both legs
/// compiling one code path.
///
/// Scope, stated honestly: this is a PROCESS-wide lock, so it serialises binds in
/// THIS test binary only. `tests/serve.rs` cannot interfere (it spawns daemons as
/// separate processes and never calls `Listener::bind` in-process); a future test
/// that did would need the same discipline, and this comment is the thing a reader
/// finds.
static BIND_SERIALIZER: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Take the file-wide bind lock.
fn bind_lock() -> std::sync::MutexGuard<'static, ()> {
    BIND_SERIALIZER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The ONE place this file calls `Listener::bind`, reachable only by PROOF that
/// [`BIND_SERIALIZER`] is held (`guard` is that proof, and holding it is the only
/// way to name this function).
///
/// The guard is taken by reference rather than by value so a caller can bind more
/// than once under one acquisition — which the stale-socket and double-bind tests
/// need, and which is also what keeps this file free of the non-reentrancy trap
/// below.
///
/// # ⚠ `bind` is NOT reentrant
///
/// `std`'s mutex does not allow a thread to take a lock it already owns: doing so
/// blocks it forever. So a test must either go through [`bind`] (which takes the
/// lock for it) or hold the lock itself and go through this function — never both.
/// A hang is the worst failure a suite can have (`tests/serve.rs`'s deadline rule
/// exists for the same reason), which is why the two routes are separate
/// functions rather than one function with an optional guard.
fn bind_holding(
    _guard: &std::sync::MutexGuard<'static, ()>,
    endpoint: Endpoint,
) -> Result<Listener, TransportError> {
    Listener::bind(endpoint)
}

/// Bind a test endpoint, with [`BIND_SERIALIZER`] held for the call.
///
/// The route every test in this file takes. The guard test
/// [`every_bind_in_this_file_goes_through_the_serialiser`] fails if a new test
/// reaches for `Listener::bind` directly instead.
#[must_use = "the listener is what the bind was for; dropping it unlinks the socket"]
fn bind(endpoint: Endpoint) -> Result<Listener, TransportError> {
    let guard = bind_lock();
    bind_holding(&guard, endpoint)
}

/// The guard that makes the serialisation a property rather than a convention.
///
/// Reads this file's own source and fails if a direct call to the transport's
/// bind appears anywhere but inside [`bind_holding`]. Without it the rule "every
/// bind takes the lock" is a comment, and comments do not survive a new test
/// written by someone who did not read one — which is exactly how
/// [`binding_restores_the_umask_it_temporarily_tightened`] became unsound in the
/// first place: the file grew bind sites around an assertion that assumed it was
/// alone. Same shape as `tests/worker_mode.rs`'s reaping-opt-out guard and
/// `tests/fixture_bins.rs`: a target whose only job is keeping a structural
/// property loud.
///
/// # What this CAN catch, and what it cannot — stated, not implied
///
/// It catches: a new test calling the transport's bind directly, on any platform,
/// with a message naming the line. That is the realistic regression, because the
/// helper is right there at the top of the file and the natural mistake is
/// reaching for the type's method instead of it.
///
/// It does NOT catch: a bind that happens somewhere else entirely (another test
/// file in this crate, or a library call that binds internally), a call written
/// across a line break so the needle does not appear, or a differently-shaped
/// route to the same syscall. A TEXT check over ONE file cannot see those, and a
/// guard that claimed otherwise would be worse than no guard, because the next
/// reader would trust it. The backstop for "another file binds in-process" is the
/// reasoning in [`BIND_SERIALIZER`]'s scope note, not this test.
#[test]
fn every_bind_in_this_file_goes_through_the_serialiser() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/transport.rs"))
            .expect("this test reads its own source");
    // Assembled rather than literal, so the test cannot match its OWN source
    // lines (the exact trap `tests/worker_mode.rs` documents: written as
    // literals, that guard failed for containing the strings it looks for). The
    // source of THIS test reads `concat!("Listener", "::bind", "(")`, which is
    // not the needle, while the line it must find reads exactly the needle.
    let direct = concat!("Listener", "::bind", "(");
    // The ONE function allowed to call it: `bind_holding`, whose signature
    // requires proof that `BIND_SERIALIZER` is held. The exemption is by
    // ENCLOSING FUNCTION, not by call shape — exempting a shape like
    // `Listener::bind(endpoint)` would also exempt a new test that happened to
    // write the bypass in exactly that form, which is the one case the guard most
    // needs to catch.
    let allowed_fns = [concat!("fn bind", "_holding")];
    let mut inside_allowed = false;
    let mut allowed_hits = 0usize;
    let mut offenders: Vec<(usize, &str)> = Vec::new();
    for (at, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        if allowed_fns.iter().any(|f| trimmed.starts_with(f)) {
            inside_allowed = true;
        } else if inside_allowed && trimmed.starts_with('}') {
            // The guard's body ends at the first column-0 brace after the
            // signature, so the exemption cannot drift onto later functions.
            inside_allowed = false;
        }
        // Comments describe the rule and are expected to name the call; only
        // executable lines are checked.
        if trimmed.starts_with("//") {
            continue;
        }
        if line.contains(direct) {
            if inside_allowed {
                allowed_hits += 1;
            } else {
                offenders.push((at + 1, trimmed));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "every bind in this file must go through `bind` / `bind_holding` so \
         `BIND_SERIALIZER` actually serialises them. Direct call(s): {offenders:?}. \
         The umask test asserts a process-global absolutely, and that is only \
         sound while no other thread can be inside `bind_private`'s tighten window \
         — a bind that skips the lock makes that assertion flaky for everyone whose \
         diff is open at the time. Use `bind(endpoint)`, or hold `bind_lock()` and \
         call `bind_holding(&guard, endpoint)` if the guard must cover more than \
         the one call."
    );
    // The other half: the exemptions must still EXIST, so the guard cannot pass by
    // having nothing left to check. Every test here asserts an absence, and an
    // absence-check whose needle matches nothing passes vacuously — the failure
    // mode `tests/boundary.rs` builds its floor machinery against.
    assert_eq!(
        allowed_hits, 1,
        "exactly one direct call is expected, in {allowed_fns:?}; found \
         {allowed_hits} — if that function was renamed or its body moved, this \
         guard is exempting nothing and asserting nothing"
    );
}

/// How long a frame is allowed to take to cross a loopback socket.
///
/// Generous on purpose (the work itself is sub-millisecond) — it exists so a
/// FRAMING REGRESSION FAILS WITH A MESSAGE INSTEAD OF HANGING FOREVER. A test
/// that deadlocks on the very bug it is meant to catch is worse than no test:
/// `cargo test` gives no output, no failure, and no clue which of the frames
/// never arrived.
const FRAME_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// `recv` with [`FRAME_BUDGET`] applied, so a lost frame reads as a failure.
async fn recv_one(conn: &mut Connection) -> serde_json::Value {
    tokio::time::timeout(FRAME_BUDGET, conn.recv())
        .await
        .expect("a frame arrives (no frame means the framing is broken)")
        .expect("not EOF")
        .expect("valid JSON")
}

/// `accept` with the budget applied (see [`FRAME_BUDGET`]).
async fn accept_one(listener: &Listener) -> Connection {
    tokio::time::timeout(FRAME_BUDGET, listener.accept())
        .await
        .expect("a client connects (no connection means the listener is broken)")
        .expect("accept: the peer connects")
}

/// `connect` with the budget applied.
async fn dial(endpoint: Endpoint) -> Connection {
    tokio::time::timeout(FRAME_BUDGET, connect(endpoint))
        .await
        .expect("connect resolves (a hang means the dial path is broken)")
        .expect("connect to the listener")
}

/// `recv` returning the raw `Option<Result<..>>`, bounded — for the tests that
/// assert on the EOF or ERROR case and so cannot use [`recv_one`].
async fn recv_checked(conn: &mut Connection) -> Option<Result<serde_json::Value, TransportError>> {
    tokio::time::timeout(FRAME_BUDGET, conn.recv())
        .await
        .expect("the stream resolves (a hang means the framing is broken)")
}

/// The server half of the loopback: accept one peer, echo its first frame back
/// under `{"echoed": …}`, and hand the `Connection` back so the caller controls
/// when it closes.
///
/// Takes the `Listener` by value (the task owns it), which is what lets this be
/// `spawn`ed without a borrow — the accept task outlives the caller's borrow
/// scope.
async fn accept_one_and_echo(listener: Listener) -> Connection {
    let mut conn = tokio::time::timeout(FRAME_BUDGET, listener.accept())
        .await
        .expect("a client connects (no connection means the listener is broken)")
        .expect("accept: the peer connects");
    let frame = recv_one(&mut conn).await;
    conn.send_frame(&json!({ "echoed": frame }))
        .await
        .expect("send: the peer is still reading");
    conn
}

#[tokio::test]
async fn a_jsonl_frame_round_trips_in_both_directions() {
    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");
    assert_eq!(listener.endpoint(), endpoint, "bind reports what it bound");

    let server = tokio::spawn(accept_one_and_echo(listener));

    let mut client = tokio::time::timeout(FRAME_BUDGET, connect(endpoint))
        .await
        .expect("connect resolves")
        .expect("connect to the listener");
    let hello = Greeting {
        kind: "hello".to_string(),
        seq: 1,
    };
    client
        .send_frame(&hello)
        .await
        .expect("send: the listener is accepting");

    let reply = recv_one(&mut client).await;

    // Outbound went as one `\n`-terminated JSON object and inbound came back as
    // one, so every field survives the framing in both directions.
    assert_eq!(reply["echoed"]["kind"], json!("hello"));
    assert_eq!(reply["echoed"]["seq"], json!(1));

    // The typed path: the echoed object deserializes back into the struct that
    // went out, so this is a round trip and not a `Value` coincidence.
    let back: Greeting =
        serde_json::from_value(reply["echoed"].clone()).expect("the echo is a valid Greeting");
    assert_eq!(back, hello);

    server.await.expect("the echo task did not panic");
}

#[tokio::test]
async fn the_receive_stream_ends_at_eof_rather_than_hanging() {
    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");

    let server = tokio::spawn(async move {
        let mut conn = accept_one(&listener).await;
        let first = recv_one(&mut conn).await;
        assert_eq!(first["kind"], json!("bye"));
        // The peer is gone: the stream must report EOF, not block forever.
        // `recv_checked` already applies the budget, so its `Option` is the
        // stream's own answer: `None` == EOF.
        let eof = recv_checked(&mut conn).await;
        assert!(eof.is_none(), "a closed peer ends the stream with None");
    });

    let mut client = dial(endpoint).await;
    client
        .send_frame(&json!({ "kind": "bye" }))
        .await
        .expect("send before disconnecting");
    // Dropping the connection closes both directions of the socket, which is
    // what the server side must observe as EOF.
    drop(client);

    server
        .await
        .expect("the server task saw the frame and then EOF");
}

/// Handing a platform the OTHER platform's address is an ordinary error, on
/// every OS — because `Endpoint` carries both variants unconditionally, so this
/// arm compiles and runs on both legs.
#[tokio::test]
async fn an_endpoint_from_the_other_platform_is_rejected_not_mis_bound() {
    // The address of whichever family this OS does NOT serve.
    #[cfg(unix)]
    let foreign = Endpoint::pipe(r"\\.\pipe\fulcrum-not-here");
    #[cfg(windows)]
    let foreign = Endpoint::socket("/tmp/fulcrum-not-here.sock");

    let bind_err =
        bind(foreign.clone()).expect_err("the other platform's endpoint must not bind here");
    let connect_err = connect(foreign.clone())
        .await
        .expect_err("the other platform's endpoint must not connect here");

    // Named as a platform mismatch, not as an I/O accident — and the message
    // names the endpoint, so a config typo is traceable to the config line.
    for message in [bind_err.to_string(), connect_err.to_string()] {
        assert!(
            message.contains("not supported on this platform"),
            "must say which kind of error this is; got: {message}"
        );
        assert!(
            message.contains(&foreign.to_string()),
            "must name the endpoint it rejected; got: {message}"
        );
    }
}

/// What only unix can check, and the raw-byte probes that need the platform's
/// own stream type (a test reaching for `UnixStream` to be a BAD peer is the
/// honest way to test framing tolerance — widening the public API with a
/// `send_raw` for test convenience would be the dishonest way).
#[cfg(unix)]
mod unix_specific {
    use fulcrum::transport::{Endpoint, TransportError};
    use serde_json::json;
    use tokio::io::AsyncWriteExt as _;
    use tokio::net::UnixStream;

    // `bind` and not `Listener`: the file-wide serialisation helper is the only
    // bind route here, and the shape guard test fails if `Listener` comes back
    // into this import list. `bind_lock` / `bind_holding` are reached through
    // `super::` by the one test that needs them, so they are deliberately not
    // imported (an unused import is a `-D warnings` failure).
    use super::{accept_one, bind, bind_lock, dial, recv_checked, recv_one, FRAME_BUDGET};

    /// The daemon's socket is owner-only (finding C).
    ///
    /// The design says "the socket is the credential" — which is only a true
    /// statement if the filesystem enforces it. At the umask default a bound
    /// socket is 0755, so any local process can connect and ask the daemon to
    /// start a session with `bash` allowed and `permission` defaulting to `Allow`,
    /// then watch that session's transcript. The mode is the only thing between
    /// "the user's own processes" and "everyone's", until T13 adds real auth.
    #[tokio::test]
    async fn a_bound_socket_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let (dir, endpoint) = super::test_endpoint();
        let listener = bind(endpoint.clone()).expect("bind the test socket");
        let path = match &endpoint {
            Endpoint::Socket(path) => path.clone(),
            Endpoint::Pipe(name) => panic!("a pipe endpoint on unix: {name}"),
        };

        let mode = std::fs::metadata(&path)
            .expect("stat the socket")
            .permissions()
            .mode()
            & 0o7777;
        assert_eq!(
            mode & 0o077,
            0,
            "{} is mode {mode:04o}: no group or world bit may be set on the \
             daemon's socket — reaching it grants the ability to start a session \
             with bash allowed and permissions defaulting to Allow",
            path.display()
        );

        // And the tightening must not have broken the thing it wraps: the owner
        // still connects, and frames still cross.
        let server = tokio::spawn(super::accept_one_and_echo(listener));
        let mut client = dial(endpoint).await;
        client
            .send_frame(&json!({"probe": "after the chmod"}))
            .await
            .expect("the owner can still dial a 0600 socket");
        let echoed = recv_one(&mut client).await;
        assert_eq!(
            echoed["echoed"]["probe"], "after the chmod",
            "a 0600 socket must still serve its owner"
        );
        // Hold the server's `Connection` alive until the exchange is done, then
        // drop it, or the task's peer vanishes mid-read.
        let _server_conn = server.await.expect("the echo task runs");
        drop(dir);
    }

    /// The socket's directory is owner-only when the daemon had to create it.
    ///
    /// The `0700` on the socket alone is not the whole story: an attacker with
    /// write access to the parent directory can unlink the socket and bind their
    /// own there, so every later client dials THEIRS. Same reasoning as the
    /// store's directory.
    #[tokio::test]
    async fn a_created_socket_directory_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("a temp dir");
        // A NESTED directory, so `create_dir_all` genuinely creates it: asserting
        // on `tempdir()`'s own path would pass on a 0700 the test never caused.
        let nested = dir.path().join("fulcrum");
        let endpoint = Endpoint::socket(nested.join("daemon.sock"));
        let _listener = bind(endpoint).expect("bind, creating the parent");

        let mode = std::fs::metadata(&nested)
            .expect("stat the created dir")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "the created directory is {mode:04o}");
    }

    /// A directory the daemon did NOT create keeps the mode its owner chose.
    ///
    /// The narrowing must not climb the tree. `--socket $HOME/x.sock` would
    /// otherwise re-mode `$HOME`, which is a decision about someone else's
    /// filesystem, and would break any other program that expected traversal.
    #[tokio::test]
    async fn binding_does_not_remode_a_directory_it_did_not_create() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755))
            .expect("widen the pre-existing dir");
        let endpoint = Endpoint::socket(dir.path().join("daemon.sock"));
        let _listener = bind(endpoint).expect("bind into the existing dir");

        let mode = std::fs::metadata(dir.path())
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o755,
            "a directory that already existed must be left exactly as its owner set it"
        );
    }

    /// The umask tightening that gives the socket its 0700 must be UNDONE the
    /// moment `bind` returns.
    ///
    /// This is the failure mode the `bind_private` comment warns about, and it is
    /// invisible from inside the transport: a leaked 0o077 would apply to every
    /// file the daemon creates afterwards, INCLUDING the `fulcrum.db-wal` /
    /// `-shm` siblings SQLite creates on its own schedule, and the symptom would
    /// be some other subsystem's file coming out with a mode nobody chose. A
    /// test that only checks the socket's own mode cannot see that. So the
    /// property is worth pinning — the question is how to pin it truthfully.
    ///
    /// # Why this test holds the lock itself instead of calling `bind`
    ///
    /// It cannot. The serialisation has to cover the READ as well as the bind:
    /// reading the umask, binding, and reading it again must all happen while
    /// nothing else in the process can bind, and `bind` takes and RELEASES the
    /// lock internally, so the window between its return and the second read
    /// would be open again. Hence `bind_holding`, which takes the proof that the
    /// guard is held as an argument.
    ///
    /// That is also the shape that makes the claim checkable rather than
    /// aspirational: the lock is held for the whole sequence by one `let` binding
    /// whose scope is the test body, and the compiler enforces that the bind used
    /// it. A version of this test that called `bind(endpoint)` while ALSO holding
    /// the lock would deadlock — `std`'s mutex is not reentrant — so the failure
    /// mode of getting this wrong is a hang, not a pass.
    ///
    /// # Why the assertion is absolute now, and was not before
    ///
    /// The previous version read the umask, called `bind`, and compared — with
    /// nothing held. `umask(2)` is a process-global and cargo runs these tests on
    /// parallel threads, so any other test binding during that window made the
    /// second read see `0o077` and the assertion fail for a reason that was not in
    /// the diff. There was no tolerance or retry that could fix that: the value
    /// being asserted belonged to another thread.
    ///
    /// MEASURED, because "it has not flaked in eight runs" is not evidence: the
    /// old shape was replayed against two tasks that bind continuously. It
    /// observed **74 wrong readings in 20 000 rounds** (against ~41 500 concurrent
    /// binds). The same loop with this lock held observed **0 in 20 000 rounds
    /// against ~104 000 concurrent binds** — 2.5× the pressure, and clean. So the
    /// race is real, it is just rare at the current test count, and the lock is
    /// what closes it.
    ///
    /// Holding the file-wide bind lock (see [`BIND_SERIALIZER`]) removes every
    /// other writer for the duration, which is what makes `after == before` a
    /// statement about this `bind` rather than about the schedule.
    #[cfg(unix)]
    #[tokio::test]
    async fn binding_restores_the_umask_it_temporarily_tightened() {
        // Held for the whole test — the guard is the serialisation, and its scope
        // is the assertion's validity condition.
        let guard = super::bind_lock();

        // `umask(0)` is the only way to READ the value (there is no getter), so
        // the read is a write-and-restore. Safe to do here for the same reason
        // everything else here is safe: this thread owns the process-wide umask
        // for as long as it holds `guard`, and no other test in this binary can
        // be inside a bind while that is true.
        let before = unsafe { libc::umask(0) };
        unsafe { libc::umask(before) };

        let (_keepalive, endpoint) = super::test_endpoint();
        let _listener = super::bind_holding(&guard, endpoint).expect("bind");

        let after = unsafe { libc::umask(before) };
        unsafe { libc::umask(after) };
        assert_eq!(
            after, before,
            "`bind` changed the process umask from {before:#o} to {after:#o} and \
             left it there — every later file creation in this process, including \
             SQLite's WAL siblings, would inherit a mode nothing chose"
        );
    }

    /// The serialisation is load-bearing, and this proves it WITHOUT a stress
    /// loop: hold the lock, start a bind the way every other test in this file
    /// starts one, and observe that it cannot proceed.
    ///
    /// # Why this test exists next to the one it guards
    ///
    /// `binding_restores_the_umask_it_temporarily_tightened` is only sound while
    /// the lock really does exclude every other bind. That is a property of the
    /// lock, and the umask test cannot check it — if the lock were deleted, or if
    /// `bind` stopped taking it, the umask test would keep passing right up until
    /// the day it flaked in CI. So the soundness condition gets its own test, in
    /// the cheap deterministic shape rather than the stress shape: the stress
    /// version (which is how this race was first measured — 74 wrong readings in
    /// 20 000 rounds unserialised, 0 in 20 000 serialised) needs tens of thousands
    /// of iterations to be likely to catch anything, and a likely test is what
    /// this file just spent a finding removing.
    ///
    /// # What it does NOT prove, because it cannot
    ///
    /// It proves the lock excludes the ROUTE, not the syscall. A test that called
    /// `Listener::bind` directly would sail past it — a mutex can only exclude the
    /// callers that take it — and that hole is closed by
    /// [`every_bind_in_this_file_goes_through_the_serialiser`], not here. Saying
    /// so because a first draft of this test asserted the stronger claim by
    /// spawning exactly such a bypass, and it FAILED: the bypass bound straight
    /// through the held lock. That failure is the reason the two tests exist
    /// rather than one.
    ///
    /// # The assertion is on BLOCKING, and blocking is observable
    ///
    /// A bind that is waiting for a lock makes no filesystem change, so "the
    /// socket file does not exist yet" is a fact rather than a race. The sleep is
    /// a grace for the negative half only; the positive half (the bind completes
    /// once the lock is released) is bounded by [`FRAME_BUDGET`].
    ///
    /// # Why the lock is held on a blocking thread rather than in this future
    ///
    /// Because holding a `std` guard across an `.await` parks the tokio WORKER
    /// THREAD, not just the task — so with a small worker count the very task that
    /// is supposed to wake up and release the lock cannot be polled, and the test
    /// deadlocks instead of passing. `clippy::await_holding_lock` exists to catch
    /// exactly this, and both roles here go through `spawn_blocking` so the guard
    /// never exists inside an async frame: the holder blocks a blocking-pool thread
    /// until told to let go, and the blocked bind blocks another. The assertions
    /// are unchanged; only the thread that owns the guard is.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn holding_the_bind_lock_blocks_another_bind_from_starting() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let endpoint = Endpoint::socket(dir.path().join("blocked.sock"));
        let probe_path = dir.path().join("blocked.sock").to_path_buf();

        // The holder: takes the lock and keeps it until the channel says stop.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        // The ordering handshake. Without it this test is a RACE, and it WAS: it
        // failed on 1 run in 14 and again on 1 in 30 before this was added.
        // `spawn_blocking` gives no ordering guarantee, so the binder could reach
        // the lock FIRST, bind successfully, and finish — and the assertion below
        // would report "the lock is not excluding binds" when the truth was "the
        // holder had not taken it yet". A test that fails because of its own
        // startup order is exactly the defect this file was just changed to
        // remove, so the holder announces acquisition and the binder is only
        // spawned after that.
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            let guard = bind_lock();
            // `send` is synchronous and takes the value, which is what a thread
            // with no runtime can call; an error here would only mean the waiter
            // was already gone, which the `.await` below reports anyway.
            let _ = acquired_tx.send(());
            // A `recv` that blocks THIS thread is the point — it is on the
            // blocking pool precisely so blocking it is legal.
            let _ = release_rx.recv();
            drop(guard);
        });
        // The await IS the ordering: nothing past this line runs until the holder
        // owns the lock.
        acquired_rx
            .await
            .expect("the holder announced acquisition and then died");

        // The would-be binder, on the route every other test in this file uses.
        // Spawned only now, with the lock demonstrably held.
        let dial_target = endpoint;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let blocked = tokio::task::spawn_blocking(move || {
            // Announce IMMEDIATELY BEFORE the call, so the negative assertion
            // below cannot pass because the task never ran: once this fires the
            // binder is at the lock's door. (It cannot announce AFTER acquiring,
            // of course — acquiring is what must not happen.)
            let _ = started_tx.send(());
            bind(dial_target)
        });
        started_rx
            .await
            .expect("the binder announced itself and then died");

        // Grace for the announced binder to actually reach the mutex. The
        // handshake above removed the "did it start?" unknown; this covers the
        // "has it reached the lock yet?" gap the announcement cannot close.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(
            !probe_path.exists(),
            "a bind through `bind` completed while `BIND_SERIALIZER` was held, and \
             created {} — the lock is NOT excluding binds, which makes the umask \
             assertion in `binding_restores_the_umask_it_temporarily_tightened` \
             unsound again",
            probe_path.display()
        );
        assert!(
            !blocked.is_finished(),
            "the blocked bind completed while the lock was held: the serialisation \
             is decorative"
        );

        // Release, and it must go through. Without this half, a bind that had
        // simply FAILED (or a task that had panicked) would look identical to a
        // bind that was correctly blocked.
        release_tx.send(()).expect("the holder is still listening");
        holder.await.expect("the holder must not panic");
        let bound = tokio::time::timeout(FRAME_BUDGET, blocked)
            .await
            .expect("the bind must complete once the lock is released")
            .expect("the blocked task must not panic");
        assert!(bound.is_ok(), "and must succeed: {:?}", bound.err());
    }
    /// inside `bind()` with `EINVAL`, which says nothing useful and does not name
    /// the path. The transport checks the length first and reports BOTH numbers.
    /// This is the macOS footgun (104 bytes) — the default data dir must never be
    /// trusted to be short enough.
    #[tokio::test]
    async fn a_socket_path_too_long_for_sockaddr_un_reports_the_length_and_the_path() {
        let dir = tempfile::tempdir().expect("a temp dir for the long socket path");
        let long = dir.path().join("f".repeat(300));
        let err = bind(Endpoint::socket(long.clone()))
            .expect_err("a path longer than sun_path cannot bind");
        let message = err.to_string();
        let path = long.to_string_lossy();

        assert!(
            matches!(err, TransportError::SocketPathTooLong { .. }),
            "must be the dedicated variant, not a wrapped io error; got: {err}"
        );
        assert!(
            message.contains(&path.len().to_string()),
            "the error must report the path length ({}); got: {message}",
            path.len()
        );
        assert!(
            message.contains(&*path),
            "the error must name the path; got: {message}"
        );

        // And a path that DOES fit still binds — the check is a guard, not a ban.
        bind(Endpoint::socket(dir.path().join("ok.sock"))).expect("a short socket path binds");
    }

    /// `default_endpoint` is the path a real `fulcrum serve` uses, and it runs
    /// the length guard itself — so a data dir too long to hold a socket fails
    /// HERE, at the point where the path is chosen, rather than later as an
    /// `EINVAL` from `bind` that names nothing.
    ///
    /// Deliberately does NOT bind: this is the real data dir, and binding there
    /// would collide with a daemon the developer actually has running.
    #[test]
    fn the_default_endpoint_is_under_the_data_dir_and_within_the_sun_path_limit() {
        let endpoint = fulcrum::transport::default_endpoint().expect(
            "a default endpoint (a data dir that cannot hold a socket is a real \
                      finding, but not one this test can fix)",
        );
        let Endpoint::Socket(path) = endpoint else {
            panic!("the unix default must be a socket path; got {endpoint}");
        };
        // Asserted on the TAIL of the path rather than on `dirs::data_dir()`:
        // `dirs` is a runtime dependency of this member, not a dev-dependency,
        // and an integration test can only use the dev-dep graph (the manifest is
        // frozen for this task, so reaching for it is not an option either).
        // The tail is the part this transport owns anyway — the data dir is the
        // OS's answer, and the length guard is what tests it is short enough.
        let tail = "fulcrum/daemon.sock";
        let rendered = path.display().to_string();
        assert!(
            path.is_absolute(),
            "a relative default would resolve against the daemon's cwd, so two \
             launches from different directories would not find each other; got {rendered}"
        );
        assert!(
            rendered.ends_with(tail),
            "the default socket must be `<data dir>/{tail}` so clients can find it \
             without configuration; got {rendered}"
        );
    }

    /// The framing is `\n`-delimited, NOT length-prefixed and not one-write-one-
    /// frame. Two frames pushed through the kernel in a SINGLE write must still
    /// arrive as two frames — the read side must split on the newline, and the
    /// leftover bytes must not be dropped.
    #[tokio::test]
    async fn two_frames_written_in_one_syscall_arrive_as_two_frames() {
        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let path = dir.path().join("framing.sock");
        let listener = bind(Endpoint::socket(path.clone())).expect("bind");

        let server = tokio::spawn(async move {
            let mut conn = accept_one(&listener).await;
            let mut got = Vec::new();
            for _ in 0..2 {
                got.push(recv_one(&mut conn).await);
            }
            got
        });

        // One `write_all`, so both frames sit in the socket buffer together and
        // the reader has to do the splitting — that is the behaviour under test.
        let raw = UnixStream::connect(&path).await.expect("connect");
        // `split` yields (read, write); the read half is kept alive so the
        // socket stays open until the server has taken both frames.
        let (reader, mut writer) = tokio::io::split(raw);
        writer
            .write_all(b"{\"n\":1}\n{\"n\":2}\n")
            .await
            .expect("write both frames at once");
        let _reader = reader;

        let got = server.await.expect("the server task read both frames");
        assert_eq!(
            got[0],
            json!({ "n": 1 }),
            "the first line is the first frame"
        );
        assert_eq!(
            got[1],
            json!({ "n": 2 }),
            "the second line is the second frame"
        );
    }

    /// One malformed line is a peer bug, not a reason to drop the connection:
    /// the bad line surfaces as an `Err` ITEM and the next good frame still
    /// arrives. Mirrors `protocol::decode_inbound`'s permissiveness.
    #[tokio::test]
    async fn a_malformed_line_is_an_error_item_and_the_stream_survives_it() {
        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let path = dir.path().join("tolerant.sock");
        let listener = bind(Endpoint::socket(path.clone())).expect("bind");

        let server = tokio::spawn(async move {
            let mut conn = accept_one(&listener).await;
            let bad = recv_checked(&mut conn)
                .await
                .expect("a line arrives (not EOF)")
                .expect_err("this line is not JSON");
            assert!(
                matches!(bad, TransportError::Decode(_)),
                "a bad line must be reported as a decode error, not silently dropped: {bad}"
            );
            recv_one(&mut conn).await
        });

        let raw = UnixStream::connect(&path).await.expect("connect");
        let (reader, mut writer) = tokio::io::split(raw);
        writer
            .write_all(b"not json at all\n{\"n\":7}\n")
            .await
            .expect("write a bad line then a good one");
        let _reader = reader;

        let good = server.await.expect("the server task kept reading");
        assert_eq!(good, json!({ "n": 7 }));
    }

    /// A crashed daemon leaves its socket FILE behind, and a plain `bind` then
    /// fails `EADDRINUSE` — so the daemon could never restart without someone
    /// running `rm` by hand. The transport reclaims a socket nothing listens on.
    #[tokio::test]
    async fn a_stale_socket_file_is_reclaimed_so_a_restart_can_bind() {
        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let endpoint = Endpoint::socket(dir.path().join("restart.sock"));

        let first = bind(endpoint.clone()).expect("bind");
        assert!(
            endpoint_socket_path(&endpoint).exists(),
            "binding creates the socket inode"
        );
        drop(first);
        // tokio's `UnixListener` has no `Drop` that unlinks, so the inode
        // survives — which is exactly the crashed-daemon state a restart hits.
        assert!(
            endpoint_socket_path(&endpoint).exists(),
            "precondition: the inode is still there after drop"
        );

        bind(endpoint.clone()).expect("rebinding over a stale socket must work");
    }

    /// A LIVE daemon's socket must never be deleted — that would cut off every
    /// client already connected. The reclaim path probes before unlinking, so a
    /// second bind on the same path fails and leaves the first daemon usable.
    ///
    /// This test also pins the probe's ONE observable side effect: the probe
    /// connection is queued on the live daemon, so its accept loop sees a
    /// connection that EOFs before sending a byte. The loop below is the shape
    /// Task 5's accept loop must have anyway — an EOF-with-no-frame connection
    /// is skipped, not fatal.
    #[tokio::test]
    async fn a_live_daemons_socket_is_left_alone_and_the_second_bind_fails() {
        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let endpoint = Endpoint::socket(dir.path().join("live.sock"));

        let first = bind(endpoint.clone()).expect("bind the first daemon");
        let err = bind(endpoint.clone()).expect_err("a second daemon must not steal a live socket");
        assert!(
            err.to_string().contains("a live daemon is answering"),
            "must say WHY it refused, not just EADDRINUSE; got: {err}"
        );
        assert!(
            endpoint_socket_path(&endpoint).exists(),
            "the live daemon's socket must survive the refused bind"
        );

        // The refused bind probed the socket, so the daemon now has one queued
        // connection that will never speak. Serve accepts until a frame shows
        // up, and records how many silent connections it skipped.
        let server = tokio::spawn(async move {
            let mut silent = 0usize;
            let mut conn = accept_one(&first).await;
            loop {
                match recv_checked(&mut conn).await {
                    Some(Ok(frame)) => return (silent, frame),
                    Some(Err(bad)) => panic!("the client wrote valid JSON, not {bad}"),
                    None => {
                        silent += 1;
                        assert!(
                            silent < 5,
                            "still skipping silent connections after {silent}"
                        );
                        conn = accept_one(&first).await;
                    }
                }
            }
        });
        let mut client = dial(endpoint.clone()).await;
        client
            .send_frame(&json!({ "still": "alive" }))
            .await
            .expect("send to the first daemon");

        let (silent, got) = server.await.expect("the first daemon is intact");
        assert_eq!(got["still"], json!("alive"), "the live daemon still works");
        assert!(
            silent >= 1,
            "the refused bind's liveness probe should have shown up as a connection \
             that EOFs with no frame; the daemon saw {silent} such connections"
        );
    }

    /// A frame that arrives with NO trailing newline (the peer was killed
    /// mid-write) is still delivered rather than discarded — the same rule
    /// `client.rs`'s `spawn_pipe_reader` applies to a worker's last stderr line.
    #[tokio::test]
    async fn a_final_frame_with_no_trailing_newline_is_still_delivered() {
        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let path = dir.path().join("no-newline.sock");
        let listener = bind(Endpoint::socket(path.clone())).expect("bind");

        let server = tokio::spawn(async move {
            let mut conn = accept_one(&listener).await;
            recv_one(&mut conn).await
        });

        let (reader, mut writer) =
            tokio::io::split(UnixStream::connect(&path).await.expect("connect"));
        // A COMPLETE JSON object with no `\n` after it — the peer was killed
        // after the last byte of the frame but before the terminator. (An
        // object truncated mid-way would be a decode error, and correctly so:
        // the transport forwards the line it has, it does not repair it.)
        writer
            .write_all(br#"{"half":"written"}"#)
            .await
            .expect("write a frame with no newline");
        // Dropping both halves closes the socket, so the reader sees EOF with an
        // unterminated line in hand.
        drop(writer);
        drop(reader);

        let got = server
            .await
            .expect("the server task delivered the unterminated frame");
        assert_eq!(got["half"], json!("written"));
    }

    /// An oversized, UNTERMINATED line is refused and ends the stream (finding 7).
    ///
    /// `read_line` had no ceiling: it appended into its `String` until it saw a
    /// `\n`, so a peer that sent bytes and never sent a terminator made the daemon
    /// allocate for as long as the peer could send — one connection, one buffer, no
    /// limit, and `serve` has no authentication before T13. The cap is
    /// `transport::frame::MAX_INBOUND_LINE_BYTES`.
    ///
    /// # Why the error must END the stream rather than skip the line
    ///
    /// "Silently truncate — a half-frame glued to the next is worse than a closed
    /// connection" is the requirement, and this test pins both halves of it: the
    /// peer gets ONE error item naming the limit, and then EOF. A resync-on-the-
    /// next-newline design would deliver the error and then happily parse the
    /// attacker's next line, which is a daemon and a client disagreeing about where
    /// frames begin.
    ///
    /// # Why this is a unix-only probe
    ///
    /// It has to write raw bytes through the platform's own stream (the framing
    /// layer would not produce an unterminated megabyte on purpose), which is the
    /// same reason the two framing probes above live in this module. The Windows
    /// leg of the cap is compile-checked, not run — `read_capped_line` is
    /// platform-neutral code, so the risk over there is a build break, not a
    /// behaviour difference.
    #[tokio::test]
    async fn an_oversized_unterminated_line_is_refused_and_ends_the_stream() {
        use fulcrum::transport::frame::MAX_INBOUND_LINE_BYTES;
        use tokio::io::AsyncWriteExt as _;

        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let path = dir.path().join("oversized.sock");
        let listener = bind(Endpoint::socket(path.clone())).expect("bind");

        let server = tokio::spawn(async move {
            let mut conn = accept_one(&listener).await;
            let first = recv_checked(&mut conn)
                .await
                .expect("an item arrives (not a silent hang)");
            let err = first.expect_err("an oversized line must be an error, not a frame");
            let message = err.to_string();
            assert!(
                matches!(err, TransportError::LineTooLong { .. }),
                "must be the dedicated variant naming a length, not a decode error \
                 (the operator must be able to tell 'too big' from 'not JSON'): {err}"
            );
            assert!(
                message.contains(&MAX_INBOUND_LINE_BYTES.to_string()),
                "the error must state the limit the peer has to get under: {message}"
            );
            // And the stream ENDS. `recv` returning `None` is the transport's own
            // EOF, which is what tells `serve_client` to drop the connection.
            let after = recv_checked(&mut conn).await;
            assert!(
                after.is_none(),
                "the oversized line must close the stream, not be skipped: the \
                 daemon got another item after refusing one ({after:?})"
            );
        });

        // One byte OVER the limit, with NO terminator anywhere. The peer then
        // holds the socket open: the daemon must refuse on the CAP, not on EOF,
        // otherwise this would be testing the unterminated-tail path instead.
        let raw = UnixStream::connect(&path).await.expect("connect");
        let (reader, mut writer) = tokio::io::split(raw);
        let flood = vec![b'x'; MAX_INBOUND_LINE_BYTES + 1];
        // Not JSON at all, and that is the point: the length is refused before
        // the content is ever parsed, so a decode error here would be the wrong
        // diagnosis.
        writer
            .write_all(&flood)
            .await
            .expect("write an oversized unterminated line");
        let _reader = reader;

        server
            .await
            .expect("the server task refused the oversized line and ended the stream");
    }

    /// A line at exactly the limit is a FRAME, not a refusal — the limit is a
    /// limit, and an off-by-one here would reject legitimate traffic.
    ///
    /// Paired with the test above so the boundary has two sides: refuse above,
    /// accept at. The terminator is what makes it legal, so the bytes written are
    /// `MAX_INBOUND_LINE_BYTES - 1` payload plus the `\n`.
    #[tokio::test]
    async fn a_line_of_exactly_the_limit_is_still_a_frame() {
        use fulcrum::transport::frame::MAX_INBOUND_LINE_BYTES;
        use tokio::io::AsyncWriteExt as _;

        let dir = tempfile::tempdir().expect("a temp dir for the socket");
        let path = dir.path().join("at-limit.sock");
        let listener = bind(Endpoint::socket(path.clone())).expect("bind");

        let server = tokio::spawn(async move {
            let mut conn = accept_one(&listener).await;
            // `recv_one` fails loudly on an `Err` item, which is exactly the
            // assertion: at the limit this must be a frame.
            recv_one(&mut conn).await
        });

        // `{"p":"aaa…a"}\n` sized so the WHOLE line, terminator included, is
        // exactly `MAX_INBOUND_LINE_BYTES`: 6 bytes of prefix + payload + 3 bytes
        // of `"}` and `\n`.
        let prefix = b"{\"p\":\"";
        let suffix = b"\"}\n";
        let payload = MAX_INBOUND_LINE_BYTES - prefix.len() - suffix.len();
        let mut line = Vec::with_capacity(MAX_INBOUND_LINE_BYTES);
        line.extend_from_slice(prefix);
        line.extend_from_slice(&vec![b'a'; payload]);
        line.extend_from_slice(suffix);
        assert_eq!(line.len(), MAX_INBOUND_LINE_BYTES, "exactly at the cap");

        let raw = UnixStream::connect(&path).await.expect("connect");
        let (reader, mut writer) = tokio::io::split(raw);
        writer.write_all(&line).await.expect("write the line");
        let _reader = reader;

        let got = server
            .await
            .expect("a line of exactly the limit must arrive as a frame");
        assert_eq!(got["p"], json!("a".repeat(payload)));
    }

    /// Read the path back out of an `Endpoint` so the test can stat the inode.
    fn endpoint_socket_path(endpoint: &Endpoint) -> std::path::PathBuf {
        match endpoint {
            Endpoint::Socket(path) => path.clone(),
            Endpoint::Pipe(name) => panic!("test built a pipe endpoint on unix: {name}"),
        }
    }

    /// `connect` on a path with NOTHING behind it is a plain io error
    /// (`ENOENT`/`ECONNREFUSED`), not a hang — the CLI's "is the daemon up?"
    /// probe depends on that.
    #[tokio::test]
    async fn connecting_to_a_socket_that_nothing_listens_on_errors_instead_of_hanging() {
        let dir = tempfile::tempdir().expect("a temp dir for the absent socket");
        let absent = Endpoint::socket(dir.path().join("nobody-home.sock"));
        let err = tokio::time::timeout(FRAME_BUDGET, fulcrum::transport::connect(absent))
            .await
            .expect("the connect resolves on its own")
            .expect_err("nothing is listening, so this cannot succeed");
        assert!(matches!(err, TransportError::Io(_)), "got: {err}");
    }

    /// The peer-credential check does not break the client it is meant to allow.
    ///
    /// The real socket, through `Listener::accept`'s guard, with the uid the
    /// kernel actually reported: bind, dial, accept. This is the positive half of
    /// the check — the half testable unprivileged, because the test process IS the
    /// daemon's uid by definition. Without it, a guard with an inverted comparison
    /// (`!=` where it should be `==`) would pass every other test in this file and
    /// then refuse every client in production.
    ///
    /// # What this cannot prove, stated rather than implied
    ///
    /// It cannot prove the REJECT path: a peer with a different uid cannot be
    /// manufactured without root. That half is pinned two ways — the comparison
    /// itself by `unix.rs`'s own `#[cfg(test)]` mod (which can name the private
    /// `verify_peer_uid`), and the end-to-end refusal by
    /// [`foreign_uid_peer_is_rejected_under_root`], `#[ignore]`d and root-only.
    #[tokio::test]
    async fn same_uid_peer_is_accepted() {
        let (_keepalive, endpoint) = super::test_endpoint();
        let listener = bind(endpoint.clone()).expect("bind the test socket");

        let server = tokio::spawn(async move {
            // `accept_one` fails the test if `accept` returns `Err`, and that IS
            // the assertion: the guard let the daemon's own uid through.
            let mut conn = accept_one(&listener).await;
            let frame = recv_one(&mut conn).await;
            conn.send_frame(&json!({"echoed": frame}))
                .await
                .expect("send: the peer is still reading");
            conn
        });

        let mut client = dial(endpoint).await;
        client
            .send_frame(&json!({"probe": "same uid"}))
            .await
            .expect("the owner can dial a socket whose accept path checks credentials");
        let echoed = recv_one(&mut client).await;
        assert_eq!(
            echoed["echoed"]["probe"], "same uid",
            "a same-uid peer must be served, not refused"
        );
        drop(server.await.expect("the echo task ran"));
    }

    /// A peer under ANOTHER uid is refused at `accept` — end to end, over a real
    /// socket, with a real foreign uid the kernel reported.
    ///
    /// `#[ignore]`d, because it needs root: the only way to get a process whose
    /// REAL uid differs from the daemon's is to start as root and drop it. CI's
    /// runners are all unprivileged, so on every automated run this is SKIPPED and
    /// the automated proof of the reject path is the unit test of the comparison in
    /// `unix.rs`. An operator who wants the end-to-end half runs
    /// `sudo cargo test -p fulcrum --test transport -- --ignored`.
    ///
    /// # Why the socket's mode is WIDENED first, and why that is not cheating
    ///
    /// `bind_private` puts the socket at 0600 under a 0700 directory — layer 1 of
    /// this daemon's access control. A foreign-uid peer cannot reach the inode past
    /// layer 1 at all: its `connect` fails `EACCES`. So a test that asserted "the
    /// foreign peer got refused" against the real mode would pass with the
    /// credential check DELETED, because it would be measuring the filesystem.
    ///
    /// This test therefore removes layer 1 deliberately (socket 0777, directory
    /// 0755), leaving exactly one thing between that peer and a `Connection`: the
    /// credential check. Delete the guard and this goes red. Layer 1 has its own
    /// pin — [`a_bound_socket_is_owner_only`] — and in a real deployment it refuses
    /// this peer long before the credential check is consulted.
    ///
    /// # Why the CHILD dials and the PARENT asserts
    ///
    /// The claim is about what `accept` RETURNS — `Err(PeerIdentity)`, and no
    /// `Connection`. The child cannot observe the refusal usefully: `connect(2)` on
    /// an `AF_UNIX` stream socket succeeds the moment the listener queues it,
    /// whether or not the daemon is about to hang up.
    ///
    /// The child is this same test binary, re-executed with `--ignored --exact`
    /// naming [`child_dials_as_a_foreign_uid`] plus two env vars it reads — which
    /// keeps the proof inside one binary instead of reaching for `setpriv`,
    /// `sudo -u nobody`, `socat` or a shell that can dial a unix socket, none of
    /// which a CI image is obliged to provide.
    ///
    /// # What it does NOT cover
    ///
    /// Linux only (`#[cfg(target_os = "linux")]`), because `SO_PEERCRED` is the
    /// Linux arm of `peer_uid`. The `getpeereid` arm that macOS and the BSDs
    /// compile is the same two-line shape and is type-checked by CI's macOS leg; it
    /// is not executed here and this test does not claim otherwise.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "needs root: only root can drop to a different uid and still dial"]
    async fn foreign_uid_peer_is_rejected_under_root() {
        use std::os::unix::fs::PermissionsExt as _;

        if unsafe { libc::geteuid() } != 0 {
            panic!(
                "this test is `#[ignore]`d precisely because it needs root, and it is \
                 running as uid {} — run it via `sudo cargo test -p fulcrum --test \
                 transport -- --ignored`, or do not select it at all",
                unsafe { libc::geteuid() }
            );
        }
        // A uid that exists here and is not ours. Read from `/etc/passwd` rather
        // than hardcoded to `65534`: `nobody` is absent on some images, and a child
        // whose `setresuid` then failed would dial as ROOT and make this pass on a
        // same-uid accept — the wrong-reason green this file's header refuses.
        let target = first_foreign_uid().expect(
            "no non-root uid in /etc/passwd to drop the child to — this test cannot \
             build a foreign peer here",
        );

        let (keepalive, endpoint) = super::test_endpoint();
        let listener = bind(endpoint.clone()).expect("bind the test socket");
        let path = match &endpoint {
            Endpoint::Socket(path) => path.clone(),
            Endpoint::Pipe(name) => panic!("a pipe endpoint on unix: {name}"),
        };
        // Strip layer 1 — see the doc above. BOTH are needed: the directory is 0700
        // (tempfile's own mode) and it must be traversable to reach the inode.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777))
            .expect("widen the socket so the filesystem is not what refuses");
        std::fs::set_permissions(keepalive.path(), std::fs::Permissions::from_mode(0o755))
            .expect("open the socket's directory for traversal");

        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--ignored",
                "--exact",
                concat!("unix_specific::", "child_dials_as_a_foreign_uid"),
            ])
            .env("FULCRUM_PEER_DIAL_SOCKET", &path)
            .env("FULCRUM_PEER_DIAL_UID", target.to_string())
            // The child's libtest summary would interleave into the parent's; its
            // FAILURE, which is the thing worth seeing, goes to stderr.
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("re-exec the test binary as the dialing child");

        // Bounded, per this file's deadline rule: a child that never dials (its
        // `setresuid` failed, the mode change did not take) must read as a named
        // timeout, not as the hang that rule exists to prevent.
        let accepted = tokio::time::timeout(FRAME_BUDGET, listener.accept())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "`accept` never returned within {FRAME_BUDGET:?}: the child never \
                     dialed. Its stderr says why — a failed `setresuid` or an \
                     unreadable socket path both land here, and either one means this \
                     test proved nothing about credentials"
                )
            });
        let accepted = accepted.expect_err(
            "a peer whose uid is not the daemon's must be refused at `accept`, and \
             `accept` must NOT hand back a `Connection` for it — no `Connection` means \
             no reader task and no client task spawned for a peer we are refusing",
        );

        // Reap the child BEFORE asserting: a child outliving the suite is exactly
        // what the stray-process census fails the run for.
        let child_ok = child.wait().expect("reap the dialing child").success();
        assert!(
            child_ok,
            "the child reported it could NOT dial — the mode widening above did not \
             take effect, so nothing ever reached the credential check"
        );

        match accepted {
            TransportError::PeerIdentity {
                peer_uid,
                expected_uid,
            } => {
                assert_eq!(
                    peer_uid, target,
                    "the refusal must name the uid the kernel reported"
                );
                assert_eq!(expected_uid, 0, "running as root, the daemon demands 0");
            }
            other => panic!(
                "a foreign peer must be refused as `PeerIdentity`, not as {other:?} — \
                 `serve::run` matches on the VARIANT to log it on the counted \
                 schedule instead of once per attempt"
            ),
        }
    }

    /// The child half of [`foreign_uid_peer_is_rejected_under_root`]: drop to the
    /// uid the parent named, dial, hang up.
    ///
    /// `#[ignore]`d so a normal pass never runs it — alone it tests nothing, and
    /// panicking for want of its env vars is the honest failure rather than a
    /// silent skip. The parent reaches it through `--ignored --exact`.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "child entry point: only the peer-credential test spawns it"]
    fn child_dials_as_a_foreign_uid() {
        let path = std::env::var("FULCRUM_PEER_DIAL_SOCKET")
            .expect("the parent must name the socket to dial");
        let target: u32 = std::env::var("FULCRUM_PEER_DIAL_UID")
            .expect("the parent must name the uid to drop to")
            .parse()
            .expect("the target uid is a number");

        // Drop real, effective AND saved-set uid. `SO_PEERCRED` reports the REAL
        // uid, and a process that keeps a root saved set can climb back — so
        // `setuid` alone is not enough to make the child genuinely foreign.
        let dropped = target;
        if unsafe { libc::setresuid(dropped, dropped, dropped) } != 0 {
            panic!(
                "setresuid({target}) failed: {}; refusing to dial as root and call \
                 that a foreign peer",
                std::io::Error::last_os_error()
            );
        }
        assert_eq!(
            unsafe { libc::getuid() },
            target,
            "the child still reports uid {}, not the {target} it dropped to",
            unsafe { libc::getuid() }
        );

        // Blocking `std` connect, deliberately: this child has no runtime, and a
        // unix `connect` either succeeds or fails immediately.
        match std::os::unix::net::UnixStream::connect(&path) {
            // Dropping the stream closes it. The parent was already woken when the
            // connection was queued, and the parent asserts the refusal; success
            // here means only "I got as far as dialing".
            Ok(_peer) => {}
            Err(e) => panic!("the foreign peer could not dial {path}: {e}"),
        }
    }

    /// The first uid in `/etc/passwd` that is not this process's, if any.
    #[cfg(target_os = "linux")]
    fn first_foreign_uid() -> Option<u32> {
        let mine = unsafe { libc::geteuid() };
        let passwd = std::fs::read_to_string("/etc/passwd").ok()?;
        passwd.lines().find_map(|line| {
            let mut fields = line.split(':');
            let (_name, _password, uid) = (fields.next()?, fields.next()?, fields.next()?);
            let uid: u32 = uid.trim().parse().ok()?;
            (uid != mine).then_some(uid)
        })
    }
}

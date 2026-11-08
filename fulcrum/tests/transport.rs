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
//! (`fulcrum_harness::worker::protocol::stamp_outbound`) stays unconsumed until
//! then — see the module docs on `transport`.

use fulcrum::transport::{
    connect, Connection, ConnectionWriter, Endpoint, Listener, TransportError,
};
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
/// under `{"echoed": …}`, and hand the writer half back so the caller controls
/// when the socket's write direction goes away.
///
/// Takes the `Listener` by value (the task owns it), which is what lets this be
/// `spawn`ed without a borrow — the accept task outlives the caller's borrow
/// scope.
///
/// # Why it returns a `ConnectionWriter` and not a `Connection`
///
/// Since T4a the write half is a separate type, and this helper is the one place
/// the file's shared loopback writes. It reads its one frame on the reader half
/// FIRST (the reader is intact until `split_writer`), then splits and sends. The
/// reader half it no longer needs drops at the end of the helper — the same
/// moment the old `Connection` used to drop for the write direction's purposes,
/// and the caller keeps the writer alive exactly as long as it kept the
/// `Connection`, so the socket's lifetime is unchanged.
async fn accept_one_and_echo(listener: Listener) -> ConnectionWriter {
    let mut conn = tokio::time::timeout(FRAME_BUDGET, listener.accept())
        .await
        .expect("a client connects (no connection means the listener is broken)")
        .expect("accept: the peer connects");
    let frame = recv_one(&mut conn).await;
    let (conn, mut writer) = conn.split_writer();
    writer
        .send_frame(&json!({ "echoed": frame }))
        .await
        .expect("send: the peer is still reading");
    // The server task is done reading; dropping the reader half aborts its task.
    drop(conn);
    writer
}

#[tokio::test]
async fn a_jsonl_frame_round_trips_in_both_directions() {
    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");
    assert_eq!(listener.endpoint(), endpoint, "bind reports what it bound");

    let server = tokio::spawn(accept_one_and_echo(listener));

    let client = tokio::time::timeout(FRAME_BUDGET, connect(endpoint))
        .await
        .expect("connect resolves")
        .expect("connect to the listener");
    // The split pattern every client-side send in this file now uses: the writer
    // does the sending, the reader does the receiving, and BOTH stay alive for
    // the exchange (dropping the writer would close the socket under the reply).
    let (mut client, mut client_writer) = client.split_writer();
    let hello = Greeting {
        kind: "hello".to_string(),
        seq: 1,
    };
    client_writer
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

    let client = dial(endpoint).await;
    let (client_reader, mut client_writer) = client.split_writer();
    client_writer
        .send_frame(&json!({ "kind": "bye" }))
        .await
        .expect("send before disconnecting");
    // Dropping BOTH halves closes the socket, which is what the server side must
    // observe as EOF. One half alone would not: `tokio::io::split`'s halves share
    // the stream's `Arc`, so the socket closes when the last one goes.
    drop(client_reader);
    drop(client_writer);

    server
        .await
        .expect("the server task saw the frame and then EOF");
}

// ---------------------------------------------------------------------------
// The writer half (T4a, issue #3).
//
// `Connection::pending` — the tail of a frame the OS has only partly taken —
// must be reachable by exactly ONE writer, because a JSONL stream cannot
// resynchronise once two writers interleave a half-written line. That rule used
// to be a comment ("Two tasks may not call `send_frame` on one `Connection`",
// `serve.rs`) and nothing else: there was NO test in this repo that failed when
// it was broken. These tests are that missing pin.
//
// What each one is FOR, stated honestly:
//   * `two_frames_from_the_sole_writer_arrive_as_two_clean_lines` — the serial
//     `pending`-drain path, and proof `split_writer` hands back a pair whose
//     reader half still reads.
//   * `a_second_split_writer_panics` — the structural pin: the writer may be
//     handed out ONCE.
//   * `a_partially_accepted_frame_is_drained_before_any_new_bytes` — the hazard
//     as it appears in production: a frame the OS took only part of, followed by
//     a second frame.
//   * `a_shared_writer_under_a_mutex_never_interleaves_frames` — the only
//     behavioural pin here that can see interleaving at all, and only the
//     UNSERIALISED kind: two tasks reaching one `pending` with no lock spanning
//     the write loop (a guard dropped before the await, or `pending` hoisted onto
//     a `Clone`/`Sync` handle). It CANNOT see the serialising shape — a
//     `Mutex<ConnectionWriter>` + re-added `Connection::send_frame` whose lock
//     covers each send end to end — because that design produces exactly the
//     whole-send serialisation this test itself implements, and its frames arrive
//     clean. Nothing behavioural guards that shape and nothing needs to: it is
//     unreachable in these types (`ConnectionWriter` is neither `Clone` nor
//     shareable, `send_frame` takes `&mut self`, `Connection` has no write
//     method), with `a_second_split_writer_panics` as the runtime pin on the one
//     mistake that IS still expressible. This test is a regression pin against a
//     POSSIBLE future design, not a proof about the current one — and no
//     behavioural test can prove structural impossibility. The three pins above
//     it all stay GREEN under a mutex-shared design — the mutex serialises them.
//   * `a_send_against_a_shut_down_connection_errors_that_the_writer_survives` —
//     `ConnectionWriter::shutdown` closes the WRITE direction while the read
//     half lives (the daemon's teardown contract, T4 D3).
//
// T4b deleted the sixth pin this block used to list,
// `send_frame_after_split_writer_fails_not_drops`, together with the transient
// `Connection::send_frame` delegator it tested. That deletion is what makes the
// exclusivity absolute rather than merely signalled: after `split_writer` the reader
// half has no write method to call, so "a `Connection` and a `ConnectionWriter` alive
// at once, both able to reach `pending`" is no longer a state a caller can be in — it
// does not compile. The mutual exclusion the deleted test pinned is now enforced by
// the absence of the method, and `a_second_split_writer_panics` remains as the
// runtime pin on the one mistake that is still expressible.
// ---------------------------------------------------------------------------

/// Two frames from the sole writer arrive as two clean lines, in order — and
/// the pair `split_writer` returns is a working reader AND a working writer.
///
/// This is the behaviour the whole framing layer exists to protect, exercised
/// through the `pending` drain path serially. It stays green through the split
/// and through a serialised (mutex-shared) regression; the concurrent driver
/// below is the one that reddens an interleaving design — the unserialised kind,
/// as its own doc spells out.
#[tokio::test]
async fn two_frames_from_the_sole_writer_arrive_as_two_clean_lines() {
    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");

    let server = tokio::spawn(async move {
        let conn = accept_one(&listener).await;
        // The server splits too, so this exercises the pair on both ends.
        let (mut conn, mut writer) = conn.split_writer();
        let first = recv_one(&mut conn).await;
        let second = recv_one(&mut conn).await;
        writer
            .send_frame(&json!({ "got": 2 }))
            .await
            .expect("send the ack: the peer is still reading");
        (first, second)
    });

    let client = dial(endpoint).await;
    // The split: this task keeps the reader half, `writer` is the ONLY handle on
    // `pending`. BOTH halves stay alive for the whole exchange — dropping the
    // writer would close the socket under the frames still to be sent.
    let (mut reader, mut writer) = client.split_writer();
    writer
        .send_frame(&json!({ "n": 1 }))
        .await
        .expect("send the first frame");
    writer
        .send_frame(&json!({ "n": 2 }))
        .await
        .expect("send the second frame");

    let (first, second) = server.await.expect("the server read both frames");
    assert_eq!(
        first,
        json!({ "n": 1 }),
        "the first frame is the first line"
    );
    assert_eq!(
        second,
        json!({ "n": 2 }),
        "the second frame is the second line, glued to nothing"
    );

    // The reader half of the pair is still a reader after the split: the ack
    // travels the other direction on the same socket.
    assert_eq!(
        recv_one(&mut reader).await["got"],
        json!(2),
        "the reader half of a split connection still receives"
    );
}

/// Asking for the writer twice is a caller bug and PANICS.
///
/// The second call would hand out a second handle on `pending`, which is exactly
/// issue #3's hazard, so it is refused loudly at the point of the mistake rather
/// than surfacing later as a corrupted frame on a production socket. Cheap
/// structural pin — note it stays GREEN under a `Mutex<ConnectionWriter>`
/// regression (that design never calls `split_writer` twice), which is why
/// [`a_shared_writer_under_a_mutex_never_interleaves_frames`] exists beside it —
/// that one is the behavioural pin, and its doc states exactly which shapes of
/// that regression it can and cannot see.
#[tokio::test]
#[should_panic(expected = "the writer was already taken")]
async fn a_second_split_writer_panics() {
    let (_keepalive, endpoint) = test_endpoint();
    let _listener = bind(endpoint.clone()).expect("bind");
    // Dialling without accepting is enough here, and it is the shape that works
    // on BOTH platforms: on unix the connection completes into the listener's
    // backlog, and on Windows an instance of the name is armed from `bind` onward
    // (that is `windows.rs`'s one-instance-always invariant), so `CreateFileW`
    // succeeds. Nothing is read or written — `split_writer` is a struct
    // operation — so no peer is needed, and no accept task can be left stranded
    // by the panic.
    let client = dial(endpoint).await;
    let (reader, _writer) = client.split_writer();
    // The panic IS the assertion.
    let (_second_reader, _second_writer) = reader.split_writer();
}

/// A frame the OS accepted only PART of is fully drained before a single byte of
/// the next frame — the `pending` drain exactly as it appears in production.
///
/// # How the partial write is forced without a fake transport
///
/// No mock stream exists in this repo (see the file header), so both halves of
/// the setup come from the real thing. The peer stops reading for the production
/// reason: `MAX_QUEUED_INBOUND_FRAMES` (frame.rs, finding 7) bounds its inbound
/// channel at 1024 and the reader task WAITS for room, so a peer that never calls
/// `recv` stops draining the socket once its channel is full. The test fills
/// exactly that many slots with tiny frames, then writes more bytes than the
/// buffers on the path can swallow — so `send_frame`'s write loop stops with a
/// tail in `pending`.
///
/// When the peer finally drains, the frames arrive in the order they were handed
/// to `send_frame`, each one whole. That is the whole assertion: a design that
/// let a second writer slip bytes in ahead of `pending` produces one glued line,
/// and a glued line does not decode.
///
/// The accept happens BEFORE the writes on purpose. On unix it could be deferred
/// (a `connect` completes into the backlog), but on Windows it cannot: `connect`
/// retries `ERROR_PIPE_BUSY` until an instance of the name calls
/// `ConnectNamedPipe`, so a dial with no `accept` in flight is a stall, not an
/// unattended socket. Parking the peer's READ (by never calling `recv`) is the
/// shape both platforms agree on.
///
/// # Why THREE big frames, and what the sizes are
///
/// One big frame is not enough, and that is worth stating because it is the thing
/// a reader of this test will trip on: the peer's reader task does not stop at
/// the socket, it assembles a line in its own buffer up to
/// `MAX_INBOUND_LINE_BYTES` before it needs channel room. So the bytes one
/// connection can swallow before its writer notices are roughly the receive queue
/// (~208 KiB on Linux AF_UNIX; the BSDs cap a unix socket buffer at 256 KiB;
/// tokio's named-pipe instances get 64 KiB each — `in_buffer_size`/
/// `out_buffer_size`, tokio 1.54 `named_pipe.rs`) plus `BufReader`'s 8 KiB plus
/// that 1 MiB line buffer — about 1.3 MiB — and a single 900 KiB frame rides
/// straight through it (an earlier draft of this test asserted a partial write at
/// 512 KiB and measured exactly that). Hence `BIG_FRAMES` × `OVERWHELM` =
/// 2.7 MiB, more than double what can be swallowed, which makes the park a fact
/// rather than a hope. Each frame stays under the 1 MiB inbound cap, because the
/// reader must ACCEPT these frames, not refuse the connection.
///
/// The park is CHECKED, not assumed: the `is_finished` assert below fails loudly,
/// with this paragraph in its message, if a platform ever buffers more than
/// that — the test would then be asserting nothing about the drain path.
#[tokio::test]
async fn a_partially_accepted_frame_is_drained_before_any_new_bytes() {
    /// Mirror of frame.rs's private `MAX_QUEUED_INBOUND_FRAMES` — a `tests/`
    /// crate cannot name a private const, so the number is stated here and the
    /// test below parks the peer's reader by filling exactly that many slots.
    const INBOUND_QUEUE_DEPTH: usize = 1024;
    /// A few extra fillers, so the park does not depend on the channel's
    /// off-by-one (capacity vs "blocks at capacity"). They are tiny and they sit
    /// in the socket buffer, which the big frame then has to overshoot anyway.
    const SPARE: usize = 8;
    const FILLERS: usize = INBOUND_QUEUE_DEPTH + SPARE;
    /// Under `MAX_INBOUND_LINE_BYTES` (1 MiB) so the frame is ACCEPTED, not
    /// refused for length.
    const OVERWHELM: usize = 900 * 1024;
    /// Enough of them that the total beats every buffer on the path (see the doc
    /// above): 3 × 900 KiB against a ~1.3 MiB swallow budget, so the park is a
    /// fact and not a hope.
    const BIG_FRAMES: usize = 3;

    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");
    // The gate that holds the peer's reader. `accept` runs immediately (the
    // platform requirement above), the READ does not.
    let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let mut conn = accept_one(&listener).await;
        start_rx
            .await
            .expect("the test releases the drain when the write is parked");
        // Everything the client queued, in order, sorted by shape.
        let mut fillers = 0usize;
        let mut bigs: Vec<String> = Vec::new();
        let mut marker = None;
        while marker.is_none() {
            let frame = recv_one(&mut conn).await;
            if let Some(payload) = frame.get("payload").and_then(|v| v.as_str()) {
                bigs.push(payload.to_string());
            } else if frame.get("marker").is_some() {
                marker = Some(frame);
            } else if frame.get("filler").is_some() {
                fillers += 1;
            } else {
                panic!("a frame with none of the three shapes arrived: {frame}");
            }
        }
        (fillers, bigs, marker.expect("the marker frame arrives"))
    });

    let client = dial(endpoint).await;
    let (_reader, mut writer) = client.split_writer();

    let big = json!({ "payload": "a".repeat(OVERWHELM) });
    let small = json!({ "marker": "last" });
    // The assertion compares against a COPY: the frames the sender moves into the
    // task are the ones that go on the wire, and `Value` is not `Copy`.
    let expected_marker = small.clone();
    let sender = tokio::spawn(async move {
        for i in 0..FILLERS {
            writer
                .send_frame(&json!({ "filler": i }))
                .await
                .expect("queue a filler (the peer's channel is not full yet)");
        }
        for _ in 0..BIG_FRAMES {
            writer
                .send_frame(&big)
                .await
                .expect("a big frame drains once the peer reads again");
        }
        writer
            .send_frame(&small)
            .await
            .expect("the marker follows them, it does not overtake them");
    });

    // Let the write loop park with a tail in `pending`. The park is then
    // CHECKED, not assumed: a send that already finished means nothing was
    // partial and the test would be vacuous.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !sender.is_finished(),
        "the writer should still be PARKED inside `send_frame`'s write loop with \
         a tail in `pending`: the peer accepted but never called `recv`, its \
         {INBOUND_QUEUE_DEPTH}-slot inbound channel is full, and \
         {BIG_FRAMES}x{OVERWHELM} bytes is more than this platform can swallow \
         unread. It finished, which means the drain path was never exercised — \
         raise BIG_FRAMES/OVERWHELM or the test asserts nothing."
    );

    // NOW somebody reads. Every frame must land, whole, in the order it was
    // handed to `send_frame`.
    start_tx
        .send(())
        .expect("release the drain (the peer task is alive)");
    let (fillers, bigs, marker) = server
        .await
        .expect("the peer read every frame without a decode error");
    assert_eq!(
        fillers, FILLERS,
        "the fillers that parked the peer's reader all arrive first, so the \
         queue order the drain is asserted against is the real one"
    );
    assert_eq!(
        bigs.len(),
        BIG_FRAMES,
        "every big frame arrives as its OWN frame — a line glued to another \
         frame's bytes would not decode, and would show up here as a missing one"
    );
    for (at, payload) in bigs.iter().enumerate() {
        assert_eq!(
            payload.len(),
            OVERWHELM,
            "big frame #{at} arrives COMPLETE, not truncated and not glued to a \
             neighbour (a glued line would not decode at all)"
        );
        assert!(
            payload.bytes().all(|byte| byte == b'a'),
            "and every byte of big frame #{at} is its own payload"
        );
    }
    assert_eq!(
        marker, expected_marker,
        "the marker is a SEPARATE line, after the last big frame's tail"
    );

    sender
        .await
        .expect("every send completed once the peer started reading");
}

/// Two tasks driving ONE writer never interleave a frame.
///
/// # Exactly which regression shapes this reddens — and which it cannot see
///
/// This body holds the mutex guard across the WHOLE of `send_frame`'s await, so
/// the two sends are fully serialised — which means the design this file worries
/// about most is the one it CANNOT fail on: a `Mutex<ConnectionWriter>` plus a
/// re-added `Connection::send_frame`, whose lock covers each send end to end,
/// produces precisely the whole-send serialisation this test already implements.
/// Its 40 frames arrive clean and this test stays GREEN under that regression.
///
/// What it does redden is a change that lets two tasks reach one `pending`
/// WITHOUT whole-send serialisation, because then two `write` calls are live on
/// one stream at once, each may take only part of its frame, and the peer's line
/// reader glues the halves into one non-JSON line:
///   * a guard dropped before the await (or the lock re-acquired per `write`), so
///     the lock no longer spans the write loop;
///   * `pending` hoisted onto a `Clone`/`Sync` handle, so each writer holds its
///     OWN view of the tail and no lock serialises them at all.
///
/// The serialising shape is kept out of reach by the types, not by a behavioural
/// test, and that is the right division of labour: `ConnectionWriter` is neither
/// `Clone` nor otherwise shareable and is handed out only by
/// `Connection::split_writer`; `send_frame` takes `&mut self`, so two tasks
/// cannot both be inside it on one handle; and `Connection` has no write method
/// left to reach `pending` with (T4b deleted the delegator). A
/// `Mutex<ConnectionWriter>` + `Connection::send_frame` is therefore not a change
/// this crate can be edited into while keeping those types — it requires
/// re-widening them, and the type-level guarantee is what stops it.
/// `a_second_split_writer_panics` is the runtime pin on the one mistake that
/// remains expressible — asking for the writer twice — and it stays GREEN under
/// the mutex design, which never calls `split_writer` twice.
///
/// So: this test pins the frame-whole property of the mutex-shared shape a future
/// change might reach for; its GREEN says nothing about the current design's
/// exclusivity, which no behavioural test can prove. The three serial pins above
/// it stay green under any mutex-shared design too — the mutex serialises them.
/// This is the only behavioural test here that can see interleaving at all, and
/// it can see only the UNSERIALISED kind.
///
/// # Why it is driven with `tokio::join!` and not two `spawn`s
///
/// Convenience, and nothing more: two `async move` blocks joined in the test body
/// share the `Arc` without cloning it into each task and still run CONCURRENTLY on
/// the same writer. It is NOT a compile constraint any more. When this test held a
/// `std::sync::Mutex` the guard was `!Send`, so `tokio::spawn` was unavailable and
/// `join!` was forced; with `tokio::sync::Mutex` the guard is `Send` and two
/// `spawn`s would compile. See the deadlock note below for why the std lock was
/// dropped — it was never the right tool for a guard held across an `.await`.
///
/// # The deadlock this test used to claim it had avoided — and had not
///
/// The previous version of this note said holding a `std` guard across
/// `send_frame`'s write-loop `.await` "can DEADLOCK" if the peer stops draining,
/// and that the test "deliberately stays out of it" by keeping the frames small and
/// the peer draining. That was wrong twice over.
///
/// It deadlocks WITHOUT backpressure. `tokio::join!` polls both futures on the one
/// thread of this task, so the moment task A parks inside `write().await` while
/// owning a `std` lock, task B's blocking `lock()` parks that same thread — and
/// task A can only be resumed by that thread. Small frames and a draining peer do
/// not prevent it; they only make it a race. On `x86_64-pc-windows-gnu` a named
/// pipe parks a write often enough to lose that race every time: the test hung
/// there while passing on Linux, which is the asymmetry that exposed the claim.
/// Switching to `tokio::sync::Mutex` fixes it because a parked task yields a
/// pending future instead of consuming the thread.
///
/// Backpressure is still out of scope, for a DIFFERENT and honest reason: if the
/// peer stops draining mid-frame, task A legitimately waits for room while holding
/// the lock and task B legitimately waits for the lock. That is the design working,
/// not wedging — it unblocks the instant the peer reads. So the assertion to watch
/// remains "40 whole lines in the right per-writer order", not throughput under
/// backpressure; it is just no longer defended by a claim that the shape is safe
/// under it.
#[tokio::test]
// No `#[allow(clippy::await_holding_lock)]` here, deliberately. The lint exists for
// non-Send guards (`std::sync::Mutex` and friends) held across an `.await`, which is
// exactly what this test used to do and what caused the deadlock above. A
// `tokio::sync::MutexGuard` is designed to be held across an await, so the lint does
// not fire and an `allow` would be a comment claiming a suppression that is not
// happening. If this test ever goes back to a std lock, the lint will come back with
// it — and that is the signal, not an annoyance.
async fn a_shared_writer_under_a_mutex_never_interleaves_frames() {
    /// Frames per task. Small on purpose (see the deadlock note above); 20×2 is
    /// still 40 chances to glue a line.
    const ROUNDS: usize = 20;

    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");

    let server = tokio::spawn(async move {
        let mut conn = accept_one(&listener).await;
        let mut got = Vec::with_capacity(2 * ROUNDS);
        for _ in 0..(2 * ROUNDS) {
            let item = recv_checked(&mut conn)
                .await
                .expect("a line arrives (EOF means the writer died mid-run)");
            got.push(item.expect(
                "every line is a whole frame: a half-frame glued to another \
                 frame's bytes is not valid JSON, and that is the interleave",
            ));
        }
        got
    });

    let client = dial(endpoint).await;
    let (_reader, writer) = client.split_writer();
    // The shared handle. NOTE this is the SHAPE the design forbids — it is
    // built here precisely so the forbidden shape is exercised, not certified.
    //
    // `tokio::sync::Mutex` and NOT `std::sync::Mutex`, and the reason is a
    // deadlock this file used to claim it had avoided by keeping the frames
    // small (see the note below). `tokio::join!` polls BOTH futures on the ONE
    // thread of this task, so a `std` guard held across `send_frame`'s `.await`
    // parks the writer mid-frame while it owns the lock, and the second task's
    // BLOCKING `lock()` then parks the same thread that has to poll the first
    // task to release it. Nothing is left to run. An async lock parks the second
    // task by RETURNING a pending future instead of consuming the thread, which
    // is the whole difference.
    //
    // Measured, not inferred: on `x86_64-pc-windows-gnu` the `std` shape hangs
    // this test outright (8 of the 9 transport tests pass, this one never
    // returns) while this shape finishes in ~10 ms. `flavor = "multi_thread"`
    // does NOT rescue the `std` shape, because the single `join!` task is
    // confined to one worker no matter how many workers exist.
    let shared = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    let task_a = {
        let shared = std::sync::Arc::clone(&shared);
        async move {
            for i in 0..ROUNDS {
                // The guard is bound so it visibly OUTLIVES the `await`: whole-
                // send serialisation is the property, and dropping the guard
                // before the await would test nothing. ASYNC lock, and it must
                // stay async — see the deadlock note on this test.
                let mut guard = shared.lock().await;
                guard
                    .send_frame(&json!({ "writer": "a", "i": i }))
                    .await
                    .expect("send from task a");
            }
        }
    };
    let task_b = {
        let shared = std::sync::Arc::clone(&shared);
        async move {
            for i in 0..ROUNDS {
                let mut guard = shared.lock().await;
                guard
                    .send_frame(&json!({ "writer": "b", "i": i }))
                    .await
                    .expect("send from task b");
            }
        }
    };
    // Both loops run CONCURRENTLY against the one writer.
    tokio::join!(task_a, task_b);
    // The writer goes away with the last task's guard; the server has already
    // taken all 40 lines by the time it returns.
    drop(shared);

    let got = server
        .await
        .expect("the peer read 40 frames without a decode error");
    assert_eq!(
        got.len(),
        2 * ROUNDS,
        "exactly 40 frames arrive — a glued pair would have arrived as 39 lines \
         (and one of them would not have decoded)"
    );

    // Per writer, the sequence is complete and in order. The INTERLEAVING of
    // `a` and `b` is a schedule and is deliberately not asserted: what this test
    // pins is that every frame is a whole line, whichever order they land in.
    for writer_name in ["a", "b"] {
        let seen: Vec<u64> = got
            .iter()
            .filter(|frame| frame["writer"] == json!(writer_name))
            .map(|frame| {
                frame["i"]
                    .as_u64()
                    .unwrap_or_else(|| panic!("`{writer_name}`'s frame index is a number: {frame}"))
            })
            .collect();
        let expected: Vec<u64> = (0..ROUNDS as u64).collect();
        assert_eq!(
            seen, expected,
            "writer `{writer_name}`'s frames all arrive, whole, in order"
        );
    }
}

/// The direction close (A1b) on a UNIX socket: a send against the shut-down
/// connection ERRORS, the peer sees EOF, and the connection's own reader
/// SURVIVES it.
///
/// This is the load-bearing half of the daemon's teardown contract (T4 D3): the
/// writer's exit tells the peer "this direction is over" without taking the
/// socket down under the reader, and it is the only place the split reaches
/// `poll_shutdown`.
///
/// It also pins what shutdown is NOT: it does not close the socket (the read
/// direction still delivers), which is why `tokio::io::split`'s shared-`Arc`
/// semantics — dropping a half closes nothing — are not something the daemon may
/// lean on for teardown.
///
/// # Why this is `#[cfg(unix)]`, with the Windows twin below
///
/// `shutdown(2)` on a unix socket closes ONE direction of a duplex fd, which is
/// the entire mechanism A1b claims. On Windows the duplex is a named pipe, and
/// tokio 1.54 implements `poll_shutdown` for both `NamedPipeClient` and
/// `NamedPipeServer` as `poll_flush` (`net/windows/named_pipe.rs:926` and
/// `:1717`) — a pipe has no per-direction close at all. Asserting the unix
/// semantics there would be asserting a fiction, and CI RUNS this binary on
/// Windows, so it would fail. The Windows twin states what is true instead.
#[cfg(unix)]
#[tokio::test]
async fn a_send_against_a_shut_down_connection_errors_that_the_writer_survives() {
    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");
    // The accept runs on a task BEFORE the dial (the file's pattern): a
    // single-threaded test runtime cannot await an accept that only completes
    // once this same task dials.
    let peer = tokio::spawn(async move { accept_one(&listener).await });
    let client = dial(endpoint).await;
    let server = peer
        .await
        .expect("the listener accepted the dial it was waiting for");

    let (mut client_reader, mut client_writer) = client.split_writer();
    let (mut server_reader, mut server_writer) = server.split_writer();

    // Setup first: a frame crosses while BOTH directions are live, so a later
    // failure below cannot be blamed on a connection that never worked.
    client_writer
        .send_frame(&json!({ "before": "shutdown" }))
        .await
        .expect("the write direction is open at the start");
    assert_eq!(
        recv_one(&mut server_reader).await["before"],
        json!("shutdown"),
        "and the peer reads it"
    );

    // The direction close (A1b).
    client_writer.shutdown().await;

    // CLAIM 1: a FUTURE write on this direction fails. On a socket the OS
    // answers `EPIPE`; the exact kind is platform detail, so the claim is the
    // `Err` and the fact that it is an I/O error rather than a framing one.
    let err = client_writer
        .send_frame(&json!({ "after": "shutdown" }))
        .await
        .expect_err("a write on a shut-down write direction must fail, not silently vanish");
    assert!(
        matches!(err, TransportError::Io(_)),
        "the refusal is the OS refusing the direction: {err}"
    );

    // CLAIM 1b: the PEER sees that direction as EOF — the other half of the same
    // fact, and the reason the daemon's writer can use shutdown as a goodbye.
    // (`recv_checked` already applies the budget and hands back the stream's own
    // `Option`, so `None` here IS the EOF answer.)
    let peer_view = recv_checked(&mut server_reader).await;
    assert!(
        peer_view.is_none(),
        "the peer sees EOF on the direction the writer shut down, got {peer_view:?}"
    );

    // CLAIM 2: the READ half survives it. The peer's write direction is still
    // open, so a frame it sends still arrives.
    server_writer
        .send_frame(&json!({ "still": "readable" }))
        .await
        .expect("the peer's write direction was not shut down");
    assert_eq!(
        recv_one(&mut client_reader).await["still"],
        json!("readable"),
        "shutting the write direction must not close the read direction"
    );

    // And the socket's full close is the LAST HANDLE dropping, not the shutdown:
    // with the server's halves gone, the client's still-live reader sees EOF.
    drop(server_reader);
    drop(server_writer);
    let eof = recv_checked(&mut client_reader).await;
    assert!(
        eof.is_none(),
        "the peer's last handle closing ends this side's stream, got {eof:?}"
    );
}

/// The Windows twin of
/// [`a_send_against_a_shut_down_connection_errors_that_the_writer_survives`],
/// stating what a named pipe actually guarantees.
///
/// A named pipe has no per-direction close: tokio 1.54 implements
/// `poll_shutdown` on both `NamedPipeClient` and `NamedPipeServer` as
/// `poll_flush` (`net/windows/named_pipe.rs:926`, `:1717`), so
/// `ConnectionWriter::shutdown` on this platform FLUSHES and nothing more. That
/// is not a bug to paper over here — it is the reason the daemon's teardown must
/// NOT depend on shutdown to end a connection (T4 D3: the writer's parked write
/// is broken by the PEER'S CLOSE, and the socket's full close rides the drop of
/// the last handle; the reader-first path is closed by `serve_client`'s
/// `writer_task.abort()` plus that drop). What IS asserted is the half the
/// Windows design does rely on: shutdown is harmless (it does not tear the
/// connection down), and both directions still work after it.
///
/// Compile-and-run on the Windows CI leg; this file's platform-neutral framing
/// tests stay the main proof for the codec itself.
#[cfg(not(unix))]
#[tokio::test]
async fn a_send_against_a_shut_down_connection_errors_that_the_writer_survives() {
    let (_keepalive, endpoint) = test_endpoint();
    let listener = bind(endpoint.clone()).expect("bind");
    let peer = tokio::spawn(async move { accept_one(&listener).await });
    let client = dial(endpoint).await;
    let server = peer
        .await
        .expect("the listener accepted the dial it was waiting for");

    let (mut client_reader, mut client_writer) = client.split_writer();
    let (mut server_reader, mut server_writer) = server.split_writer();

    client_writer
        .send_frame(&json!({ "before": "shutdown" }))
        .await
        .expect("the write direction is open at the start");
    assert_eq!(
        recv_one(&mut server_reader).await["before"],
        json!("shutdown"),
        "and the peer reads it"
    );

    client_writer.shutdown().await;

    // What shutdown means on a pipe: a flush. The write direction is NOT closed,
    // so writes keep working — the honest statement of the platform's limit, and
    // the reason the daemon's teardown rides the peer's close and the final
    // handle drop instead (see the doc above).
    client_writer
        .send_frame(&json!({ "after": "shutdown" }))
        .await
        .expect("a named pipe has no per-direction close, so the write still works");
    server_writer
        .send_frame(&json!({ "still": "readable" }))
        .await
        .expect("the peer's direction is unaffected either");
    assert_eq!(
        recv_one(&mut server_reader).await["after"],
        json!("shutdown"),
        "the post-shutdown write arrives"
    );
    assert_eq!(
        recv_one(&mut client_reader).await["still"],
        json!("readable"),
        "and so does the peer's"
    );

    // The full close still rides the last handle, on both platforms.
    drop(server_reader);
    drop(server_writer);
    let eof = recv_checked(&mut client_reader).await;
    assert!(
        eof.is_none(),
        "the peer's last handle closing ends this side's stream, got {eof:?}"
    );
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
        let client = dial(endpoint).await;
        let (mut client, mut client_writer) = client.split_writer();
        client_writer
            .send_frame(&json!({"probe": "after the chmod"}))
            .await
            .expect("the owner can still dial a 0600 socket");
        let echoed = recv_one(&mut client).await;
        assert_eq!(
            echoed["echoed"]["probe"], "after the chmod",
            "a 0600 socket must still serve its owner"
        );
        // Hold the server's writer half alive until the exchange is done, then
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
        let client = dial(endpoint.clone()).await;
        let (_client_reader, mut client_writer) = client.split_writer();
        client_writer
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
            // Read on the whole connection FIRST, then split — the same order
            // `accept_one_and_echo` uses, and required: `split_writer` consumes
            // the `Connection`. The reader half drops here (its reader task with
            // it); the writer half goes back to the caller, so the socket stays
            // open for the echo until the caller drops it.
            let (conn, mut writer) = conn.split_writer();
            writer
                .send_frame(&json!({"echoed": frame}))
                .await
                .expect("send: the peer is still reading");
            drop(conn);
            writer
        });

        let client = dial(endpoint).await;
        let (mut client, mut client_writer) = client.split_writer();
        client_writer
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
    /// `sudo cargo test -p fulcrum-harness-daemon --test transport -- --ignored`.
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
    /// naming [`child_dials_as_a_foreign_uid`] plus three env vars it reads —
    /// which keeps the proof inside one binary instead of reaching for `setpriv`,
    /// `sudo -u nobody`, `socat` or a shell that can dial a unix socket, none of
    /// which a CI image is obliged to provide.
    ///
    /// # Why the child's success is a SENTINEL and not an exit code
    ///
    /// `--exact` selects the child by NAME, and libtest exits 0 when a filter
    /// matches nothing. So a re-exec whose name no longer resolves — the child
    /// entry point renamed, moved to another module, or deleted — runs ZERO
    /// tests, reports success, and an exit-code-only parent would be satisfied by
    /// a child that did nothing at all: the reject-path proof would evaporate
    /// while this test stayed green. That is the same vacuity the file's bind
    /// guard argues about from the other side (an absence-check whose needle
    /// matches nothing asserts nothing).
    ///
    /// So the child's success is an OBSERVED fact instead: after a successful dial
    /// it prints a line naming the nonce this run handed it, the uid it dialed as
    /// and the descriptor it dialed on (see [`PEER_DIAL_SENTINEL`]), and the parent
    /// requires that line in the child's captured stdout. "Ran nothing" cannot
    /// produce it, and neither can "ran as the wrong uid" — the uid in the needle is
    /// the one this test chose. The exit code is still asserted too (a child that
    /// could not dial at all says so by failing), it is simply no longer
    /// load-bearing on its own.
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
                 running as uid {} — run it via `sudo cargo test -p fulcrum-harness-daemon --test \
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

        // Per-run nonce, so the sentinel the parent hunts for cannot be a stale
        // line from an earlier run or a coincidental match in libtest's summary —
        // only a child spawned by THIS run knows it.
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("a wall clock past the epoch")
                .as_nanos()
        );

        let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--ignored",
                "--exact",
                // Without this the sentinel never arrives: libtest CAPTURES a
                // passing test's stdout and prints it only on failure, so the
                // child's green run — the only run whose output is the proof —
                // would print nothing and this test would fail for the wrong
                // reason.
                "--nocapture",
                concat!("unix_specific::", "child_dials_as_a_foreign_uid"),
            ])
            .env("FULCRUM_PEER_DIAL_SOCKET", &path)
            .env("FULCRUM_PEER_DIAL_UID", target.to_string())
            .env("FULCRUM_PEER_DIAL_NONCE", &nonce)
            // The child's stdout is captured rather than nulled OR inherited: it
            // carries the sentinel that proves the child ran at all (see
            // [`PEER_DIAL_SENTINEL`]). Its libtest summary lands here too, which is
            // fine — the parent scans it for one line, and the summary only shows
            // up in a failure message. Its FAILURE, which is the thing worth seeing
            // as it happens, still goes to stderr unbuffered.
            .stdout(std::process::Stdio::piped())
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
        // what the stray-process census fails the run for. `wait_with_output` both
        // reaps and takes the pipe, and doing it in this order (rather than
        // reading the pipe while the child still runs) is what keeps a parent from
        // waiting on output a dead child will never send. It relies on the child's
        // whole stdout fitting the OS pipe buffer — true for one libtest summary
        // plus one sentinel line, and if this child ever grew a chatty stdout the
        // read would have to move before the reap.
        let output = child
            .wait_with_output()
            .expect("reap the dialing child and take its stdout");
        let child_ok = output.status.success();
        let child_stdout = String::from_utf8_lossy(&output.stdout);
        // The exit code FIRST, because it is the specific diagnosis when the child
        // ran and failed; the sentinel below is what the exit code CANNOT say.
        assert!(
            child_ok,
            "the child reported it could NOT dial — the mode widening above did not \
             take effect, so nothing ever reached the credential check"
        );
        // The expected line is built from the SAME helper family the child calls,
        // so the parent's needle cannot drift from the child's print. The fd VALUE
        // is the child's fact, so the needle ends at `fd=` — and the fd must still
        // be there, and still be a number the child actually named.
        let needle = peer_dial_sentinel_prefix(&nonce, target);
        let dialed = child_stdout.lines().map(str::trim_end).any(|line| {
            line.strip_prefix(needle.as_str())
                .is_some_and(|fd| !fd.is_empty() && fd.bytes().all(|byte| byte.is_ascii_digit()))
        });

        assert!(
            dialed,
            "the child never printed its dial sentinel `{needle}<fd>` — `{child_stdout}` \
             has no such line. An exit code of 0 without it means the `--exact` filter \
             matched ZERO tests (the child entry point was renamed, moved or deleted), \
             so nothing dialed, so the refusal asserted below came from somewhere other \
             than a foreign-uid peer and this test proved nothing."
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

    /// Every sentinel line starts with this: the marker [`child_dials_as_a_foreign_uid`]
    /// prints once it has genuinely dialed, and which
    /// [`foreign_uid_peer_is_rejected_under_root`] requires in the captured stdout.
    ///
    /// It exists because the parent selects the child by NAME, as a STRING
    /// (`--exact unix_specific::child_dials_as_a_foreign_uid`). A rename, a move
    /// to another module, or a deletion of the child therefore cannot be caught at
    /// compile time, and libtest exits 0 when a filter matches ZERO tests — so an
    /// exit code alone lets "ran nothing" look exactly like "ran and succeeded",
    /// and the reject-path proof would evaporate silently. Requiring a line only
    /// the running child can write makes its success an observed fact instead.
    /// `nonce=` carries a per-run value so the line cannot be another run's
    /// leftover or a coincidental match in libtest's own summary, `uid=` pins that
    /// it dialed as the uid THIS run chose, and `fd=` names the descriptor it
    /// dialed on.
    #[cfg(target_os = "linux")]
    const PEER_DIAL_SENTINEL: &str = "FULCRUM-PEER-DIAL-SENTINEL";

    /// The one place the sentinel's shape is written — the pair
    /// [`peer_dial_sentinel`] / [`peer_dial_sentinel_prefix`]. The child prints the
    /// first, the parent hunts for the second, so the two sides cannot drift into a
    /// parent that accepts a line no child ever prints.
    ///
    /// [`peer_dial_sentinel`] is what the child prints;
    /// [`peer_dial_sentinel_prefix`] is what the parent hunts for — the same line
    /// up to the `fd=` VALUE, which is the child's own fact (whichever descriptor
    /// its `connect` got) and is not the parent's to guess. Requiring `fd=` to be
    /// present still forces the child to name the descriptor it dialed on.
    #[cfg(target_os = "linux")]
    fn peer_dial_sentinel(nonce: &str, uid: u32, fd: i32) -> String {
        format!("{}fd={fd}", peer_dial_sentinel_prefix(nonce, uid))
    }

    /// The parent's needle — see [`peer_dial_sentinel`].
    #[cfg(target_os = "linux")]
    fn peer_dial_sentinel_prefix(nonce: &str, uid: u32) -> String {
        format!("{PEER_DIAL_SENTINEL} nonce={nonce} uid={uid} fd=")
    }

    /// The child half of [`foreign_uid_peer_is_rejected_under_root`]: drop to the
    /// uid the parent named, dial, hang up.
    ///
    /// `#[ignore]`d so a normal pass never runs it — alone it tests nothing, and
    /// panicking for want of its env vars is the honest failure rather than a
    /// silent skip. The parent reaches it through `--ignored --exact`, and reads
    /// [`peer_dial_sentinel`] on its stdout as the proof this entry point actually
    /// ran (see that constant for why an exit code is not the proof).
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "child entry point: only the peer-credential test spawns it"]
    fn child_dials_as_a_foreign_uid() {
        use std::os::unix::io::AsRawFd as _;

        let path = std::env::var("FULCRUM_PEER_DIAL_SOCKET")
            .expect("the parent must name the socket to dial");
        let target: u32 = std::env::var("FULCRUM_PEER_DIAL_UID")
            .expect("the parent must name the uid to drop to")
            .parse()
            .expect("the target uid is a number");
        // Echoed back in the sentinel, so the parent's needle cannot be satisfied
        // by a line an earlier run left behind.
        let nonce = std::env::var("FULCRUM_PEER_DIAL_NONCE")
            .expect("the parent must name the sentinel nonce to echo");

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
            Ok(peer) => {
                // The proof-of-life, printed AFTER the dial so a child that
                // dropped its uid but never reached the socket cannot print it.
                // Names the uid it dialed as and the descriptor it dialed on, plus
                // the parent's nonce. `println!` reaches the parent's pipe because
                // the parent passes `--nocapture` — without that flag libtest
                // captures a PASSING test's stdout and the sentinel would be
                // swallowed by exactly the green run this test is meant to observe.
                println!("{}", peer_dial_sentinel(&nonce, target, peer.as_raw_fd()));
                // Dropping the stream closes it. The parent was already woken when
                // the connection was queued, and the parent asserts the refusal;
                // success here means only "I got as far as dialing".
                drop(peer);
            }
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

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
    let listener = Listener::bind(endpoint.clone()).expect("bind");
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
    let listener = Listener::bind(endpoint.clone()).expect("bind");

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

    let bind_err = Listener::bind(foreign.clone())
        .expect_err("the other platform's endpoint must not bind here");
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
    use fulcrum::transport::{Endpoint, Listener, TransportError};
    use serde_json::json;
    use tokio::io::AsyncWriteExt as _;
    use tokio::net::UnixStream;

    use super::{accept_one, dial, recv_checked, recv_one, FRAME_BUDGET};

    /// `sockaddr_un::sun_path` is a fixed array; a path that overruns it fails
    /// inside `bind()` with `EINVAL`, which says nothing useful and does not name
    /// the path. The transport checks the length first and reports BOTH numbers.
    /// This is the macOS footgun (104 bytes) — the default data dir must never be
    /// trusted to be short enough.
    #[tokio::test]
    async fn a_socket_path_too_long_for_sockaddr_un_reports_the_length_and_the_path() {
        let dir = tempfile::tempdir().expect("a temp dir for the long socket path");
        let long = dir.path().join("f".repeat(300));
        let err = Listener::bind(Endpoint::socket(long.clone()))
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
        Listener::bind(Endpoint::socket(dir.path().join("ok.sock")))
            .expect("a short socket path binds");
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
        let listener = Listener::bind(Endpoint::socket(path.clone())).expect("bind");

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
        let listener = Listener::bind(Endpoint::socket(path.clone())).expect("bind");

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

        let first = Listener::bind(endpoint.clone()).expect("bind");
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

        Listener::bind(endpoint.clone()).expect("rebinding over a stale socket must work");
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

        let first = Listener::bind(endpoint.clone()).expect("bind the first daemon");
        let err = Listener::bind(endpoint.clone())
            .expect_err("a second daemon must not steal a live socket");
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
        let listener = Listener::bind(Endpoint::socket(path.clone())).expect("bind");

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
}

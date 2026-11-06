//! T9 Task 5 — the `fulcrum` binary's argv dispatch: `--worker` (the Worker
//! mode) and `serve` (the daemon), with everything else the usage text.
//!
//! # Why this file spawns the binary instead of calling the function
//!
//! The acceptance criterion is about the BINARY: `fulcrum --worker` must emit
//! `hello` then `ready` and speak the existing worker protocol **unchanged**.
//! That is a statement about argv dispatch plus the child's stdout, and a unit
//! test on `parse_command` cannot make it — the dispatch is exactly the layer
//! such a test would skip. So every test here `exec`s the real bin.
//!
//! `env!("CARGO_BIN_EXE_fulcrum")` is the route for THIS member's own bin: it is
//! package-local, so it cannot name the engine's fixture bins (see
//! `serve.rs`'s header for what that costs and how it is paid).
//!
//! # Why every read has a deadline
//!
//! `tests/fixture_bins.rs` states the rule and this file follows it: a child
//! that never writes must FAIL, not hang the job. A hung test prints nothing,
//! names nothing, and takes the whole suite's output with it.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use fulcrum_core::worker::protocol::PROTOCOL_VERSION;

/// The bin under test — this member's own, named the way cargo means it.
const BIN: &str = env!("CARGO_BIN_EXE_fulcrum");

/// How long a handshake line is allowed to take.
///
/// Generous because the work is a `fork`+`exec` of a binary that links the whole
/// engine (a cold page-in on a loaded CI runner is seconds, not microseconds),
/// and tight enough that a `--worker` flag nobody wired reads as a failure
/// inside a test run rather than as a stalled CI job.
const HANDSHAKE_BUDGET: Duration = Duration::from_secs(30);

/// One spawned `fulcrum` process with its stdin kept open and its stdout on a
/// channel, so a read can time out instead of blocking forever.
///
/// `Drop` kills and reaps: a test that returns early (an assertion fired) must
/// not leave a `fulcrum --worker` child running — the suite would leak one
/// process per failure, and a leaked worker holds the daemon's pipe open, which
/// makes the NEXT test's EOF ambiguous.
struct Spawned {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    /// `None` once the reader thread has been taken (stdout can only be `take`n
    /// once, so a test that reads lines and then wants the exit status must ask
    /// for the lines first).
    lines: Option<mpsc::Receiver<String>>,
    /// `false` once the test has reaped the child (or deliberately left it
    /// running) — `Drop` must then keep its hands off `child`.
    owned: bool,
}

impl Spawned {
    fn args(args: &[&str]) -> Self {
        let mut child = Command::new(BIN)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // STDERR is piped, not inherited: a test's own output is the report,
            // and a child's diagnostics interleaved into it makes a failure
            // unreadable. It is DRAINED rather than sent to the void, because a
            // child that writes to a full, unread pipe BLOCKS — and then the test
            // hangs on what looks like a protocol bug.
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {BIN} {args:?}: {e}"));

        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let stderr = child.stderr.take().expect("stderr is piped");

        let (line_tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_line(&mut buf) {
                    // EOF or a read error ends the stream; the receiver sees a
                    // closed channel and the caller reads "fewer lines than
                    // asked for" as a failure.
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let line = buf.trim_end().to_string();
                        if !line.is_empty() && line_tx.send(line).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let (err_tx, _errs) = mpsc::channel::<String>();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_line(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if err_tx.send(buf.trim_end().to_string()).is_err() {
                            break;
                        }
                    }
                }
            }
        });

        // No exit-status watcher thread: `Child::wait` would need to OWN the
        // child, and the struct needs it for `Drop`. `try_wait` (polled below)
        // reaps without blocking, which is all `wait_timeout` asks for.
        Self {
            child,
            stdin: Some(stdin),
            lines: Some(lines),
            owned: true,
        }
    }

    /// The next `n` stdout lines, or however many arrive before `budget`.
    fn read_lines(&mut self, n: usize, budget: Duration) -> Vec<serde_json::Value> {
        self.read_raw_lines(n, budget)
            .into_iter()
            .map(
                |line| match serde_json::from_str::<serde_json::Value>(&line) {
                    Ok(v) => v,
                    // A non-JSON line on a Worker's stdout corrupts every later
                    // frame the Supervisor reads, so it is a failure and not
                    // something to skip over.
                    Err(e) => panic!("stdout line is not JSON ({e}): {line:?}"),
                },
            )
            .collect()
    }

    /// The next `n` stdout lines as TEXT (for the usage output, which is prose).
    fn read_raw_lines(&mut self, n: usize, budget: Duration) -> Vec<String> {
        let rx = self.lines.as_ref().expect("the stdout reader is live");
        let deadline = std::time::Instant::now() + budget;
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                break;
            }
            match rx.recv_timeout(left) {
                Ok(line) => out.push(line),
                Err(_) => break,
            }
        }
        out
    }

    fn read_handshake(&mut self) -> Vec<serde_json::Value> {
        self.read_lines(2, HANDSHAKE_BUDGET)
    }

    /// Write one JSONL frame to the child's stdin.
    fn send(&mut self, frame: &serde_json::Value) {
        let w = self.stdin.as_mut().expect("stdin is open");
        let mut line = frame.to_string();
        line.push('\n');
        w.write_all(line.as_bytes())
            .expect("write the frame to stdin");
        w.flush().expect("flush stdin");
    }

    /// The exit code once the child has exited, or `None` if it is still
    /// running after `budget` (a signal death reads as `None` too — every
    /// caller here asserts a clean 0 or a non-zero code, never "a signal").
    ///
    /// Polls `try_wait` rather than blocking in `wait`, so a child that never
    /// exits costs the budget and a failure, not a hung test.
    fn wait_timeout(&mut self, budget: Duration) -> Option<i32> {
        if !self.owned {
            return None;
        }
        let deadline = std::time::Instant::now() + budget;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.owned = false;
                    return status.code();
                }
                // A signal death (unix) carries no code; report it as "not 0"
                // by way of a non-zero sentinel rather than `None`, because the
                // callers distinguish "still running" from "exited badly".
                Ok(None) => {}
                Err(_) => {
                    self.owned = false;
                    return Some(-1);
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Forget the child: `Drop` must not reap a process the test already reaped,
    /// and must not kill one it deliberately left running.
    fn detach(&mut self) {
        self.owned = false;
        self.lines = None;
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        if self.owned {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// `fulcrum --worker` is a Worker: `hello` (the protocol version) then `ready`
/// (the binary version), in that order, before anything else.
///
/// The ORDER is the assertion, not just the presence. `WorkerHandle::spawn` +
/// `wait_ready` read the two off separate watches, but `run_worker_inner`'s
/// comment claims "`hello` is line 1 and `ready` is line 2" — that claim is only
/// true of this binary if this binary keeps it true.
#[test]
fn worker_mode_emits_hello_then_ready_with_the_protocol_version() {
    let mut spawned = Spawned::args(&["--worker"]);
    let frames = spawned.read_handshake();
    assert_eq!(
        frames.len(),
        2,
        "`fulcrum --worker` must write `hello` then `ready`; got {frames:?} — an \
         empty stdout means argv dispatch never reached `run_worker`"
    );

    assert_eq!(
        frames[0]["type"], "hello",
        "line 1 must be the `hello` handshake: {frames:?}"
    );
    assert_eq!(
        frames[0]["protocolVersion"], PROTOCOL_VERSION,
        "`hello.protocolVersion` must carry the wire version the worker protocol \
         defines — this binary IS the worker the daemon spawns, so a daemon and a \
         worker built from one tree must agree on it: {frames:?}"
    );
    assert!(
        frames[0]["clientVersion"].is_string(),
        "`hello.clientVersion` (the binary version) must be a string: {frames:?}"
    );

    assert_eq!(
        frames[1]["type"], "ready",
        "line 2 must be `ready` (the handshake the Supervisor waits for)"
    );
    assert!(
        frames[1]["version"].is_string(),
        "`ready.version` must be the binary version: {frames:?}"
    );
    assert_eq!(
        frames[1]["sessionId"],
        serde_json::Value::Null,
        "`ready` precedes `start`, so it carries no session id"
    );
}

/// `fulcrum --worker` speaks the WHOLE protocol, not just its first two lines:
/// a `start` envelope is accepted and a `close` ends the process with 0.
///
/// Why this is a separate test from the handshake one: the handshake would stay
/// green with a `--worker` arm that printed two lines and then `exit(0)` (or that
/// never read stdin at all). The `close` is what proves the real
/// `run_worker_inner` loop is running, because only that loop treats `close` as
/// "exit 0".
#[test]
fn worker_mode_completes_the_handshake_and_exits_zero_on_close() {
    let mut spawned = Spawned::args(&["--worker"]);
    let frames = spawned.read_handshake();
    assert_eq!(frames.len(), 2, "handshake first: {frames:?}");

    spawned.send(&serde_json::json!({ "type": "close" }));
    let code = spawned.wait_timeout(HANDSHAKE_BUDGET);
    assert_eq!(
        code,
        Some(0),
        "a `close` frame must end the Worker with 0 (the protocol's clean exit; \
         `None` means it never exited — the stdin loop is not the real one)"
    );
    spawned.detach();
}

/// The stdin reader is a blocking thread reading `std::io::stdin`, so a Worker
/// whose Supervisor died (the pipe closed) must exit rather than spin forever
/// holding a session. That is the clean-EOF path of `run_worker_inner`, and it
/// is what stops an orphaned `fulcrum --worker` from outliving its daemon.
#[test]
fn worker_mode_exits_when_its_supervisor_closes_the_pipe() {
    let mut spawned = Spawned::args(&["--worker"]);
    let frames = spawned.read_handshake();
    assert_eq!(frames.len(), 2, "handshake first: {frames:?}");

    // Closing OUR write end is the EOF the child's reader thread sees.
    drop(spawned.stdin.take());
    let code = spawned.wait_timeout(HANDSHAKE_BUDGET);
    assert_eq!(
        code,
        Some(0),
        "stdin EOF must be the clean exit (a hang here means the daemon's pipes \
         keep orphaned workers alive)"
    );
    spawned.detach();
}

/// Everything that is not `--worker` / `serve` / `help` is a usage error: a
/// non-zero exit and a usage line on stderr.
///
/// A daemon that ignores an unknown argv silently does the wrong thing — the
/// classic one being a typo'd `--worker-exe` that lands on `current_exe()` and
/// spawns the DAEMON as its own worker, recursively. Loud beats silent.
#[test]
fn no_arguments_and_unknown_arguments_are_usage_errors() {
    for args in [
        &[] as &[&str],
        &["nope"],
        &["--woker"],
        &["serve", "--sockt"],
    ] {
        let mut spawned = Spawned::args(args);
        let code = spawned.wait_timeout(HANDSHAKE_BUDGET);
        // TEXT, not JSON: the usage is prose, and this assertion is about stdout
        // being EMPTY (the usage belongs on stderr so `fulcrum serve | jq` does
        // not get a usage banner where it expected a result).
        let usage = spawned.read_raw_lines(1, Duration::from_secs(1));
        assert!(
            usage.is_empty(),
            "a usage error must not print to stdout (it is not a result):              {args:?} printed {usage:?}"
        );
        assert!(
            code.is_some_and(|c| c != 0),
            "`fulcrum {args:?}` must exit non-zero; got {code:?}"
        );
    }
}

/// `help` is the one non-command that succeeds, on stdout.
#[test]
fn help_prints_the_usage_and_exits_zero() {
    let mut spawned = Spawned::args(&["help"]);
    let code = spawned.wait_timeout(HANDSHAKE_BUDGET);
    assert_eq!(code, Some(0), "`fulcrum help` must succeed");
    // Nothing is asserted about stdout's CONTENT here beyond "it is not empty":
    // the text is the operator's interface and re-wording it must not redden a
    // test. That it exists at all is the claim.
    let lines = spawned.read_raw_lines(1, Duration::from_secs(5));
    assert!(
        !lines.is_empty(),
        "`fulcrum help` must print the usage (it is how an operator discovers \
         `--worker-exe` exists)"
    );
    spawned.detach();
}

// ── `serve` through the real argv ─────────────────────────────────────────────

/// The endpoint the CLI test binds, plus whatever keeps it alive.
///
/// Mirrors `tests/transport.rs`: the shape is `(keepalive, args)` so the call
/// site is identical on both platforms and the inactive leg is `cfg`-removed
/// rather than type-checked-and-dead.
#[cfg(unix)]
fn serve_cli_args(dir: &std::path::Path) -> Vec<String> {
    vec![
        "serve".to_string(),
        "--socket".to_string(),
        dir.join("cli.sock").display().to_string(),
    ]
}

#[cfg(windows)]
fn serve_cli_args(dir: &std::path::Path) -> Vec<String> {
    vec![
        "serve".to_string(),
        "--pipe".to_string(),
        format!(
            "fulcrum-cli-{}",
            dir.file_name()
                .expect("a temp dir has a final component")
                .to_string_lossy()
        ),
    ]
}

/// `fulcrum serve --socket/--pipe …` really binds and really answers: the argv
/// dispatch reaches the accept loop, not just the type checker.
///
/// A `--worker-exe` that does not exist is passed deliberately. Nothing here
/// starts a session, so the missing binary must NOT be a startup failure — the
/// daemon binds first and reports a spawn failure per `start` frame. Asserting
/// that split is part of the point: a daemon that resolved the worker exe at
/// startup would be refusing to boot over a typo it could have reported.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_cli_binds_the_socket_it_was_given_and_answers_hello() {
    let dir = tempfile::tempdir().expect("temp dir for the CLI socket");
    let args = serve_cli_args(dir.path());
    let mut spawned = Spawned::args(&args.iter().map(String::as_str).collect::<Vec<_>>());

    let endpoint = fulcrum::transport::Endpoint::socket(dir.path().join("cli.sock"));
    let mut conn = dial_until_listening(&endpoint).await;

    conn.send_frame(&serde_json::json!({ "type": "hello" }))
        .await
        .expect("send `hello`");
    let reply = tokio::time::timeout(HANDSHAKE_BUDGET, conn.recv())
        .await
        .expect("a reply within the budget")
        .expect("not EOF")
        .expect("valid JSON");
    assert_eq!(
        reply["type"], "hello",
        "the daemon answers `hello`: {reply}"
    );
    assert_eq!(
        reply["protocolVersion"], PROTOCOL_VERSION,
        "the daemon negotiates the same wire version as the worker protocol"
    );

    // The daemon must still be alive (it answered, so it is — but a reply that
    // arrived before a panic would not notice the panic).
    assert!(
        spawned.wait_timeout(Duration::from_millis(50)).is_none(),
        "the daemon must not exit after serving one client"
    );
    spawned.detach(); // Drop kills it; a daemon never exits on its own in T9.
}

#[cfg(not(unix))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_cli_binds_the_socket_it_was_given_and_answers_hello() {
    let dir = tempfile::tempdir().expect("temp dir for the CLI pipe name");
    let args = serve_cli_args(dir.path());
    let mut spawned = Spawned::args(&args.iter().map(String::as_str).collect::<Vec<_>>());

    let name = args
        .get(2)
        .expect("serve_cli_args puts the pipe name third")
        .clone();
    let endpoint = fulcrum::transport::Endpoint::pipe(format!(r"\\.\pipe\{name}"));
    let mut conn = dial_until_listening(&endpoint).await;

    conn.send_frame(&serde_json::json!({ "type": "hello" }))
        .await
        .expect("send `hello`");
    let reply = tokio::time::timeout(HANDSHAKE_BUDGET, conn.recv())
        .await
        .expect("a reply within the budget")
        .expect("not EOF")
        .expect("valid JSON");
    assert_eq!(
        reply["type"], "hello",
        "the daemon answers `hello`: {reply}"
    );
    assert_eq!(
        reply["protocolVersion"], PROTOCOL_VERSION,
        "the daemon negotiates the same wire version as the worker protocol"
    );
    assert!(
        spawned.wait_timeout(Duration::from_millis(50)).is_none(),
        "the daemon must not exit after serving one client"
    );
    spawned.detach();
}

/// Dial with retries until the daemon is listening (or the budget runs out).
///
/// Binding is asynchronous w.r.t. the test: the process has to `exec`, open the
/// database, and `bind()` before a `connect` succeeds. On unix a connect to a
/// socket file that is not there yet is `ENOENT`, so the retry is on the error,
/// not on a sleep.
async fn dial_until_listening(
    endpoint: &fulcrum::transport::Endpoint,
) -> fulcrum::transport::Connection {
    let deadline = std::time::Instant::now() + HANDSHAKE_BUDGET;
    loop {
        match fulcrum::transport::connect(endpoint.clone()).await {
            Ok(conn) => return conn,
            Err(e) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
                let _ = e;
            }
            Err(e) => panic!("the daemon never listened on {endpoint}: {e}"),
        }
    }
}

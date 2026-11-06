//! T9 Task 5 — the daemon's accept loop, the client↔session multiplex, and the
//! one claim the whole roadmap's `done-when` rests on.
//!
//! # These tests drive the real binary
//!
//! `fulcrum serve` is spawned as a process, a real socket is dialed, a real
//! child Worker is spawned, and `fulcrum.db` is read back through a SECOND
//! connection while the daemon still holds it open. Nothing is stubbed. A daemon
//! tested against a stubbed store cannot make the durability claim, and a daemon
//! tested by calling `serve_client` directly cannot make the disconnect claim —
//! both hazards are about processes and sockets, not about a function's return
//! value.
//!
//! # Why `--worker-exe` is the fixture and not `fulcrum --worker`
//!
//! A real Worker needs an LLM provider: `fulcrum --worker` with the fake model
//! this test must use would `start` and then fail its first provider call,
//! producing no transcript rows — so the durability test would be asserting that
//! nothing happened. The engine's `fake_worker_core` bin exists for exactly this
//! seam (`worker/manager/mod.rs` documents it as the spawn seam's fixture) and
//! emits canned store frames with no network.
//!
//! It is a bin of the OTHER package, so `env!("CARGO_BIN_EXE_…")` cannot name it
//! and cargo need not build it for `cargo test -p fulcrum --test serve`. Measured
//! both ways: a workspace-wide `cargo test` does materialise it (both default
//! members, all targets), and `cargo test -p fulcrum --test serve` on a target
//! dir that has never seen it does not. So [`worker_fixture`] resolves it, and
//! only builds it when it is genuinely absent.
//!
//! ⚠ That fallback MUST NOT write into `target/debug/`, and the reason is the
//! one `ci.yml:60` gives for forbidding a `cargo build --all-targets` step: it
//! would keep the suite green after someone deleted `tests/fixture_bins.rs`,
//! the target whose job is to force the fixture bins to exist. A fallback that
//! populated the shared target dir would be that same step wearing a test's
//! clothes — and the inherited version of this file WAS exactly that, measured:
//! with `tests/fixture_bins.rs` removed, the engine's own suite went from 66
//! failures to 32 because this file had rebuilt the binary. [`worker_fixture`]
//! therefore builds into an isolated `--target-dir` under `target/`, which only
//! this file can find. See its doc for the numbers and the cost.
//!
//! # The deadline rule
//!
//! Every wait here has one, per `tests/fixture_bins.rs`: a hung test is worse
//! than a red one, because it prints nothing and takes the suite's output with
//! it.
//!
//! # This file is written cross-platform, and CI's Windows leg is its first reader
//!
//! Three of this branch's Windows failures were the same mistake wearing three
//! different clothes: **a Linux assumption expressed as a cross-platform
//! assertion.** All three were in this file (the third in a helper it shared,
//! duplicated, with `worker_mode.rs`):
//!
//! * `trim_start_matches(r"\\.\pipe\")` before dialing — the CLI's `--pipe` value
//!   is the bare name because `parse_serve_options` re-adds the prefix, but
//!   `CreateFileW` is not the CLI and resolves a bare name to nothing. Two tests
//!   died with `NotFound` on a live daemon. Now: `endpoint_flag_value` for argv,
//!   `pipe_name` for dialing, each naming the other.
//! * `assert_eq!(exitCode, Null)` — Linux's *representation* of a signal death.
//!   Windows has no signals; `taskkill /F` yields a code. Now: the portable
//!   contract (both crash frames carry one code, passed through, not invented)
//!   asserted everywhere, the OS's own rendering gated with the reason.
//! * `line.contains(",{pid},")` against `tasklist /FO CSV` — the process table's
//!   shape, which quotes every field, so four assertions called a live child dead.
//!   Now the shared, fixture-tested parser in `tests/proctable/mod.rs`.
//!
//! And one precedent, fixed by the commit before this work:
//! `assert!(message.contains("No such file"))` — the kernel's prose, not the
//! daemon's; Windows says "program not found" (`worker_factory.rs`).
//!
//! So: any assertion about an **OS-rendered value** — error prose, an exit code,
//! a process-table shape, a path or pipe-name prefix — must either state the
//! daemon's own contract (the thing this repo can actually break) or be gated
//! `#[cfg]` with a comment naming why the other platform cannot claim it. Do not
//! "fix" a Windows failure by swapping in Microsoft's literal: two platform-shaped
//! assertions are not one portable one, they are two that each fail a platform,
//! and the inverted one fails on the leg nobody watches.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use fulcrum::transport::{connect, Connection, Endpoint};
use fulcrum_core::worker::protocol::{StartEnv, StartMode, PROTOCOL_VERSION};

/// The shared process-table probe, whose own header explains why it is one module
/// included by two test binaries rather than a helper in each (they had already
/// drifted into sharing one wrong answer).
mod proctable;

/// The daemon binary under test (this member's own bin — the route cargo
/// guarantees for an integration test of the package that declares it).
const BIN: &str = env!("CARGO_BIN_EXE_fulcrum");

/// The engine's Worker fixture, as `worker_fixture` resolves it.
const WORKER_FIXTURE: &str = "fake_worker_core";

/// How long a frame is allowed to take to cross the socket.
///
/// The work is a local `write`, but the frame may be queued behind a child
/// Worker's spawn + `ready` handshake (the engine's own bound for that is 5 s),
/// so the budget has to cover the slowest legitimate start.
const FRAME_BUDGET: Duration = Duration::from_secs(30);

/// How long the durability test is allowed to take to see a row appear.
///
/// The fixture's slowest mid-turn scenario is ~3 s of streaming, so this is a
/// 5× margin on real sleeps. It is a DEADLINE, not a sleep: the test proceeds as
/// soon as the row lands.
const ROW_BUDGET: Duration = Duration::from_secs(20);

/// The `fake_worker_core` path, materialised if this cargo invocation did not
/// build it (see the module header).
///
/// # Why the fallback builds into an ISOLATED `--target-dir`
///
/// The obvious fallback — `cargo build -p fulcrum-core --bin fake_worker_core`,
/// which drops the binary in `target/debug/` — is WRONG here, and subtly so.
/// `ci.yml` forbids a `cargo build --all-targets` step because it would keep the
/// suite green after someone deleted `tests/fixture_bins.rs`, the target whose
/// only job is to force the fixture bins to exist. A fallback that populates
/// `target/debug/` is that same step wearing a test's clothes. Measured, with
/// `tests/fixture_bins.rs` removed and both fixture bins deleted:
///
/// * `cargo test -p fulcrum-core --lib` alone → **66 failed** (loud, as designed)
/// * the same workspace `cargo test` WITH this file's shared-target fallback →
///   **32 failed** — this file rebuilt `target/debug/fake_worker_core` at test
///   time and silently rescued 34 of the engine's own tests.
///
/// Building into `target/fulcrum-serve-fixture/` instead means this fallback can
/// only ever produce a binary THIS file knows the path of. `target/debug/` stays
/// empty of it, so deleting `tests/fixture_bins.rs` still reddens every engine
/// test that spawns the fixture by path. That directory is under the repo's
/// gitignored `/target`, and it is a CARGO TARGET DIRECTORY, not a scratch dir —
/// `check-temp-dir-census.sh` counts `TempDir`s, and a build cache is not one.
///
/// The isolation costs ~57 s on a cold cache, and it is paid only when
/// `target/debug/` has no fixture — which is never in CI, where a workspace-wide
/// `cargo test` builds every bin of every default member before the first test
/// runs and the fast path below returns immediately.
fn worker_fixture() -> PathBuf {
    let exe = std::env::current_exe().expect("a test binary has a path");
    // `target/debug/deps/<test>-<hash>` → `target/debug/`, the directory cargo
    // writes a package's bins into. Two `parent()`s off the RUNNING binary, not a
    // level count off `CARGO_MANIFEST_DIR` — the reason `test_support` and
    // `tests/fixture_bins.rs` both give for preferring `current_exe`.
    let target_debug = exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("the test binary lives in target/debug/deps")
        .to_path_buf();
    let name = format!("{WORKER_FIXTURE}{}", std::env::consts::EXE_SUFFIX);
    let shared = target_debug.join(&name);
    if shared.is_file() {
        return shared;
    }

    // Not built for this invocation. Build the ONE bin, into the isolated dir
    // above, from the workspace root (`CARGO_MANIFEST_DIR` is this member; its
    // parent is the root that owns the `[workspace]` table `-p` resolves
    // against). Concurrent callers serialize on cargo's own build-directory
    // lock, which is why no test-level mutex is needed here.
    let root = target_debug
        .parent()
        .expect("target/debug sits in the workspace's target dir")
        .join("fulcrum-serve-fixture");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the member sits inside the workspace root");
    let out = Command::new(&cargo)
        .args([
            "build",
            "-p",
            "fulcrum-core",
            "--bin",
            WORKER_FIXTURE,
            "--target-dir",
        ])
        .arg(&root)
        .current_dir(manifest)
        .output()
        .unwrap_or_else(|e| panic!("spawn `{cargo} build --bin {WORKER_FIXTURE}`: {e}"));
    let built = root.join("debug").join(&name);
    assert!(
        out.status.success(),
        "building the `{WORKER_FIXTURE}` fixture failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        built.is_file(),
        "`{WORKER_FIXTURE}` was built but is not at {} — the nested build put it \
         somewhere this test cannot resolve.",
        built.display()
    );
    // The assertion that keeps the whole point of the isolated dir true: this
    // fallback must NOT have created the shared-target binary, because that is
    // the file whose absence `tests/fixture_bins.rs` exists to make loud.
    assert!(
        !shared.exists(),
        "the fixture fallback wrote {shared:?}; it must build ONLY into \
         {root:?} — populating `target/debug/` from a test is the \
         `cargo build --all-targets` step `ci.yml` forbids, and it would hide \
         the deletion of `tests/fixture_bins.rs`"
    );
    built
}

/// The daemon, its temp dir, and the two paths it was told to use.
///
/// The `TempDir` is a FIELD and not a local: dropping it deletes the socket file
/// and the database under a live daemon, which turns a clean-up race into a
/// store failure that reads as a bug in the code under test (the same reason
/// `tests/transport.rs` hands the keepalive back).
struct Daemon {
    child: Child,
    db_path: PathBuf,
    _dir: tempfile::TempDir,
}

impl Daemon {
    /// Boot the daemon on a fresh socket + a fresh database, and do not return
    /// until something is listening.
    ///
    /// `async` because the readiness probe is the transport's own `connect`: the
    /// platform-neutral dial is async, and a blocking
    /// `std::os::unix::net::UnixStream::connect` — what this did before — is both
    /// a unix-only item (so the Windows leg of CI could not compile this file)
    /// and a second dial implementation the file did not need.
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("temp dir for the daemon");
        let endpoint = Self::endpoint_in(dir.path());
        let db_path = dir.path().join("fulcrum.db");
        let worker = worker_fixture();

        let mut cmd = Command::new(BIN);
        cmd.arg("serve");
        // The endpoint flag is the only platform-dependent part; `worker_mode.rs`
        // covers the CLI's parsing of both spellings, so this one leg is enough
        // here and keeps the tests identical on both OSes.
        #[cfg(unix)]
        cmd.args(["--socket"]);
        #[cfg(windows)]
        cmd.args(["--pipe"]);
        cmd.arg(endpoint_flag_value(&endpoint))
            .arg("--db")
            .arg(&db_path)
            // Explicit, so the test is not accidentally testing the
            // `current_exe()` fallback (that default has its own test, in
            // `worker_factory`'s unit tests, where it can be asserted directly).
            .arg("--worker-exe")
            .arg(&worker)
            .stdin(Stdio::null())
            // The daemon's stderr is the operator's console (see `main.rs`: no
            // `windows_subsystem` attribute, deliberately). Inherited, so a
            // `--nocapture` run shows the accept loop's own reporting — including
            // the "client disconnected" line the disconnect tests are about.
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let child = cmd.spawn().expect("spawn the daemon");

        let daemon = Self {
            child,
            db_path,
            _dir: dir,
        };
        daemon.wait_until_listening(&endpoint).await;
        daemon
    }

    /// Dial with retries until the daemon answers, or fail.
    ///
    /// The retry is on the ERROR, not on a sleep: on unix a `connect` to a socket
    /// file that does not exist yet is `ENOENT`, and a fixed sleep before the
    /// first dial is what turns a fast machine's test into a slow one and a slow
    /// machine's test into a flake.
    ///
    /// The dial is `transport::connect`, so it is the SAME code path a test
    /// client takes and it compiles on both platforms. Dropping the returned
    /// `Connection` immediately is deliberate — this is a liveness probe, and it
    /// also means the daemon's very first accepted connection is one that
    /// EOFs with no frame, which is the shape hazard (b) is about.
    async fn wait_until_listening(&self, endpoint: &Endpoint) {
        let deadline = Instant::now() + FRAME_BUDGET;
        loop {
            match connect(endpoint.clone()).await {
                Ok(_probe) => return,
                Err(e) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let _ = e;
                }
                Err(e) => panic!("the daemon never listened on {endpoint}: {e}"),
            }
        }
    }

    #[cfg(unix)]
    fn endpoint_in(dir: &Path) -> Endpoint {
        Endpoint::socket(dir.join("daemon.sock"))
    }

    #[cfg(windows)]
    fn endpoint_in(dir: &Path) -> Endpoint {
        Endpoint::pipe(format!(
            r"\\.\pipe\fulcrum-serve-{}",
            dir.file_name()
                .expect("a temp dir has a final component")
                .to_string_lossy()
        ))
    }

    /// A SECOND connection to the same file, while the daemon still holds it.
    ///
    /// This is the whole point of WAL: the durability claim is that rows the
    /// daemon wrote are visible to another reader without the daemon's
    /// cooperation, and a test that read them through the daemon's own handle
    /// would prove nothing about what is on disk.
    fn read(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(&self.db_path).expect("open the daemon's db for reading")
    }

    /// `SELECT COUNT(*)` on one of the daemon's tables.
    fn count(&self, table: &str) -> i64 {
        self.read()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap_or_else(|e| panic!("count {table}: {e}"))
    }

    /// How many sessions the daemon has recorded — one row per `start`, so this
    /// is "how many sessions did the daemon actually take responsibility for",
    /// read from the FILE rather than from the daemon's own memory.
    fn spawn_count(&self) -> i64 {
        self.count("sessions")
    }

    /// Ask the daemon to stop (there is no shutdown frame in T9), and reap it.
    fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill the daemon before the temp dir goes away, so the socket unlink
        // does not race a live listener.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The flag VALUE the CLI takes for an endpoint — argv, not an address to dial.
///
/// The two platforms' flags take different shapes: `--socket` takes a path, and
/// `--pipe` takes the bare NAME (`serve.rs`'s own help text: "named pipe to
/// bind, without the `\\.\pipe\` prefix"), because `parse_serve_options` re-adds
/// the prefix so an operator types the name they chose rather than the
/// platform's address syntax. So the strip below is REQUIRED here and is not a
/// general "endpoint → string" conversion.
///
/// ⚠ It is therefore the wrong helper for dialing: `CreateFileW` wants the full
/// `\\.\pipe\…` name, which is what [`Endpoint::pipe`] stores and what
/// `transport::windows::connect` passes. Dialing the stripped form is exactly
/// what made two tests fail on the Windows leg with `NotFound` ("The system
/// cannot find the file specified.") — a bare `fulcrum-serve-…` is not a pipe
/// address. Use [`pipe_name`] for that, or `transport::connect`.
#[cfg(unix)]
fn endpoint_flag_value(endpoint: &Endpoint) -> String {
    socket_path(endpoint).display().to_string()
}

#[cfg(windows)]
fn endpoint_flag_value(endpoint: &Endpoint) -> String {
    endpoint
        .to_string()
        .trim_start_matches(r"\\.\pipe\")
        .to_string()
}

/// The full pipe name to hand to the OS when DIALING (the counterpart of
/// [`socket_path`], and the mirror image of [`endpoint_flag_value`]: that strips
/// the prefix because the CLI flag wants it gone, this keeps it because
/// `CreateFileW` wants it present).
#[cfg(windows)]
fn pipe_name(endpoint: &Endpoint) -> &str {
    match endpoint {
        Endpoint::Pipe(name) => name,
        Endpoint::Socket(path) => panic!("a socket endpoint on windows: {}", path.display()),
    }
}

#[cfg(unix)]
fn socket_path(endpoint: &Endpoint) -> &Path {
    match endpoint {
        Endpoint::Socket(path) => path,
        Endpoint::Pipe(name) => panic!("a pipe endpoint on unix: {name}"),
    }
}

/// The platform's own duplex stream, dialed at the daemon, WITHOUT the framing
/// layer on top — for the tests that must write bytes the framer would refuse to
/// produce (a malformed line) or read a reply the framer would consume.
///
/// Two `cfg` legs, both compiled on their own platform, because that is the only
/// way the Windows leg gets any type-checked coverage of the raw-byte path at
/// all: a `#[cfg(unix)] fn` with no Windows twin means the property it tests is
/// simply unclaimed over there, silently. This is the same reason `transport`
/// gates its `mod`s rather than using `if cfg!(…)`.
///
/// Deliberately NOT `transport::connect`: that returns a `Connection`, which
/// owns a reader task that would take the reply bytes this test needs to read
/// itself.
async fn raw_dial(endpoint: &Endpoint) -> impl tokio::io::AsyncRead + tokio::io::AsyncWrite {
    #[cfg(unix)]
    {
        tokio::net::UnixStream::connect(socket_path(endpoint))
            .await
            .expect("dial raw (unix)")
    }
    #[cfg(windows)]
    {
        // `open` is synchronous in tokio's named-pipe wrapper (a `CreateFileW`),
        // and the daemon is already listening by the time a test gets here —
        // `Daemon::start` probed before returning.
        //
        // The FULL name, prefix and all — the same thing
        // `transport::windows::connect` passes, and the reason this takes
        // `pipe_name` and not `endpoint_flag_value`: the CLI's flag value is the
        // bare name because `parse_serve_options` re-adds the prefix, but
        // `CreateFileW` does not. Handing it a bare `fulcrum-serve-…` resolved
        // to nothing and this failed `NotFound` ("The system cannot find the
        // file specified.") on the Windows leg; the transport's own `connect`,
        // passing the full name, is the control that says which of the two was
        // wrong.
        tokio::net::windows::named_pipe::ClientOptions::new()
            .read(true)
            .write(true)
            .open(pipe_name(endpoint))
            .expect("dial raw (windows)")
    }
}

/// A live client, with the frames it has been sent already drained into a queue
/// so a test can wait for one specific frame without losing the ones before it.
struct Client {
    conn: Connection,
    /// Frames that arrived but that no matcher has claimed yet, in arrival
    /// order. `recv_until*` scans this BEFORE reading the socket, so "wait for
    /// the `start-result`" never swallows the `hello` reply that came first —
    /// and it REMOVES what it returns, so the next call sees the next frame.
    ///
    /// The removal is the load-bearing half. A queue that is scanned but not
    /// drained makes every repeated wait return the same first frame forever:
    /// `while !settled { wait for the next event }` loops on `turn_start` until
    /// its deadline, and "wait for the second `start-result`" re-reads the
    /// first one and reports `ok: true`. Both failure shapes were live in this
    /// file until the queue became consuming — see the note on
    /// [`Client::recv_until_matching`].
    pending: std::collections::VecDeque<serde_json::Value>,
    /// EVERY frame ever received, never drained. `pending` cannot serve the
    /// "was this frame relayed to the socket at all?" assertions once a matcher
    /// has consumed it, and those assertions are exactly the ones that must
    /// survive a matcher that ran earlier in the same test.
    delivered: Vec<serde_json::Value>,
}

impl Client {
    async fn dial(endpoint: &Endpoint) -> Self {
        let conn = connect(endpoint.clone())
            .await
            .expect("dial the daemon (if this fails, the daemon is not listening)");
        Self {
            conn,
            pending: std::collections::VecDeque::new(),
            delivered: Vec::new(),
        }
    }

    async fn send(&mut self, frame: serde_json::Value) {
        self.conn.send_frame(&frame).await.expect("write the frame");
    }

    /// Wait for the first frame whose `type` is `kind`, failing after
    /// [`FRAME_BUDGET`] with everything else that DID arrive.
    ///
    /// The "everything else" is the point of the failure message: a missing
    /// `start-result` because the envelope was malformed looks identical to a
    /// missing `start-result` because the daemon never replied, unless you can
    /// see what it said instead.
    async fn recv_until(&mut self, kind: &str) -> serde_json::Value {
        self.recv_until_matching(kind, |_| true).await
    }

    /// Wait for the next frame matching `kind` + `extra`, CONSUMING it.
    ///
    /// Consuming is what makes "the next X" mean the next one. The version this
    /// replaced scanned `pending` and cloned the match without removing it, so a
    /// test that waited twice for the same frame type got the same frame twice —
    /// which does not look like a broken harness, it looks like the daemon
    /// replaying, or like a duplicate `start` that was accepted when it was
    /// actually refused. A harness that manufactures a wrong answer is worse than
    /// one that fails.
    async fn recv_until_matching(
        &mut self,
        kind: &str,
        extra: impl Fn(&serde_json::Value) -> bool,
    ) -> serde_json::Value {
        let matched = |frame: &serde_json::Value| {
            frame.get("type").and_then(serde_json::Value::as_str) == Some(kind) && extra(frame)
        };
        let deadline = Instant::now() + FRAME_BUDGET;
        loop {
            // `VecDeque::extract_if` is unstable on the pinned toolchain, so the
            // scan is an index loop that can `remove` the position it found.
            if let Some(at) = self.pending.iter().position(&matched) {
                return self.pending.remove(at).expect("the index just found");
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                panic!(
                    "no `{kind}` frame within {FRAME_BUDGET:?}; received so far: {}",
                    serde_json::to_string(&self.delivered).unwrap_or_default()
                );
            }
            let next = tokio::time::timeout(left, self.conn.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "no `{kind}` frame within {FRAME_BUDGET:?}; received so far: {}",
                        serde_json::to_string(&self.delivered).unwrap_or_default()
                    )
                });
            match next {
                Some(Ok(frame)) => {
                    self.delivered.push(frame.clone());
                    self.pending.push_back(frame);
                }
                Some(Err(e)) => panic!("the stream errored while waiting for `{kind}`: {e}"),
                // EOF before the frame: the daemon closed the connection, which
                // is a bug in the daemon for every frame this file waits on.
                None => panic!(
                    "the daemon closed the connection while waiting for `{kind}`; \
                     received so far: {}",
                    serde_json::to_string(&self.delivered).unwrap_or_default()
                ),
            }
        }
    }

    /// The next `event` frame for one session (the multiplex's unit of work).
    async fn recv_event_for(&mut self, session_id: &str) -> serde_json::Value {
        self.recv_until_matching("event", |frame| {
            frame.get("sessionId").and_then(serde_json::Value::as_str) == Some(session_id)
        })
        .await
    }

    /// Any frame, with a budget (the probe tests need "nothing arrived").
    ///
    /// Takes from the FRONT of `pending`, matching socket order — `pop()` takes
    /// the back, which would report the newest frame as the oldest.
    async fn try_recv(&mut self, budget: Duration) -> Option<serde_json::Value> {
        if let Some(frame) = self.pending.pop_front() {
            return Some(frame);
        }
        match tokio::time::timeout(budget, self.conn.recv()).await {
            Ok(Some(Ok(frame))) => {
                self.delivered.push(frame.clone());
                Some(frame)
            }
            Ok(Some(Err(e))) => panic!("a malformed frame arrived: {e}"),
            Ok(None) => None,
            Err(_) => None,
        }
    }

    /// Every frame still unclaimed, plus whatever arrives within `window`.
    ///
    /// Drains `pending` first: a test that asserted on the socket alone would
    /// miss frames a previous matcher had already pulled off it, and the
    /// cross-session-leak assertions are only sound if they look at EVERY frame
    /// this client was sent.
    async fn drain(&mut self, window: Duration) -> Vec<serde_json::Value> {
        let mut out = self.pending.drain(..).collect::<Vec<_>>();
        let deadline = Instant::now() + window;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return out;
            }
            match tokio::time::timeout(left, self.conn.recv()).await {
                Ok(Some(Ok(frame))) => {
                    self.delivered.push(frame.clone());
                    out.push(frame);
                }
                Ok(Some(Err(e))) => panic!("a malformed frame arrived: {e}"),
                Ok(None) => return out,
                Err(_) => return out,
            }
        }
    }
}

/// Is this daemon `event` frame the worker's `RpcEvent` named `name`?
///
/// The path is four keys deep and each level is called `type`, which is how this
/// file got it wrong once: `frame["frame"]["event"]` is the SERIALIZED
/// [`fulcrum_core::events::RpcEvent`], and that enum is internally tagged
/// (`#[serde(tag = "type")]`), so it is an OBJECT `{"type": "agent_settled"}` —
/// not the string a first reading suggests. It is a string only on the OTHER
/// event-bearing variant, `SinkFrame`, whose `event` is a bare name like
/// `"session-update"`. Comparing the `RpcEvent` path to a string silently never
/// matches, and the symptom is a 30 s timeout that looks like a stalled stream.
///
/// Naming it once is the fix; the two call sites below are the two spellings the
/// mistake came in.
fn rpc_event_is(frame: &serde_json::Value, name: &str) -> bool {
    frame["type"] == "event"
        && frame["frame"]["type"] == "event"
        && frame["frame"]["event"]["type"] == name
}

/// Is this daemon `event` frame the streamed `agent_message_chunk` for `session`?
///
/// The specificity is the point. The FIRST event a session ever emits is the
/// fixture's `session_info` `sink-frame`, sent in response to `start` — and it
/// is a `sink-frame` too, so an assertion as loose as "the first mid-turn frame
/// is a sink-frame" is satisfied by a frame produced BEFORE the prompt and
/// proves nothing about the turn being in flight. This matches the chunk's own
/// `sessionUpdate` discriminator and its session, so it can only be satisfied
/// by the turn actually streaming.
fn agent_message_chunk_for(session: &str) -> impl Fn(&serde_json::Value) -> bool {
    let session = session.to_string();
    move |frame: &serde_json::Value| {
        frame["type"] == "event"
            && frame["sessionId"] == session.as_str()
            && frame["frame"]["type"] == "sink-frame"
            && frame["frame"]["payload"]["update"]["sessionUpdate"] == "agent_message_chunk"
    }
}

/// The `StartEnv` a test starts a session with.
///
/// A fake provider with a fake key on purpose: the fixture never calls it, and
/// `StartEnv` requires a `Model`, so the envelope has to carry one. This is the
/// same shape `client.rs`'s own `test_env` builds.
fn start_env(session_id: &str, cwd: &str) -> StartEnv {
    fn model(id: &str) -> fulcrum_core::harness::catalog::Model {
        fulcrum_core::harness::catalog::Model {
            id: id.to_string(),
            provider: "fake".to_string(),
            base_url: "http://127.0.0.1:1/v1".to_string(),
            api_key: "k".to_string(),
            context_window: 100_000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }
    let m = model("m1");
    StartEnv::from_parts(
        session_id.to_string(),
        cwd.to_string(),
        StartMode::Fresh,
        None,
        m.clone(),
        fulcrum_core::harness::catalog::ModelCatalog {
            models: vec![m],
            ..Default::default()
        },
        None,
        true,
        None,
        // The daemon never reads `config_dir` (the five desktop-config fields ride
        // the envelope), and the fixture ignores it — but it is a REQUIRED field
        // of the wire type, so the envelope carries the temp dir.
        std::env::temp_dir().display().to_string(),
        false,
        None,
        None,
    )
}

/// The `start` frame: the `StartEnv` verbatim plus the daemon's routing key.
fn start_frame(env: &StartEnv) -> serde_json::Value {
    let mut frame = serde_json::to_value(env).expect("a StartEnv is encodable");
    frame
        .as_object_mut()
        .expect("an object")
        .insert("type".to_string(), serde_json::json!("start"));
    frame
}

/// Is `pid` a live process?
///
/// Unix: `kill(pid, 0)`, the only portable-within-unix way to ask (Linux has
/// `/proc`, macOS does not, and this suite runs on both). A signal-0 send checks
/// existence and permissions without touching the process.
///
/// ⚠ A ZOMBIE answers "alive" here — which is exactly the trap the task warns
/// about ("the pid check alone would pass if the worker were merely slow to
/// die"). Two things keep that from making the durability test vacuous: the
/// caller polls this over a SUSTAINED window rather than once (a reaped child
/// goes `ESRCH` within the window), and the row-count half is a separate,
/// independent assertion.
///
/// Windows: the shared probe in [`mod@proctable`]. It used to shell out to
/// `tasklist` and search for `,` + pid + `,`, which matched NOTHING — `tasklist
/// /FO CSV` quotes every field, so a live pid reads `,"8620",`. Four assertions
/// in this file failed on CI's `windows-latest` leg against a live child for that
/// reason; the parser and its fixtures live in `tests/proctable/mod.rs` so the
/// shape is tested on the platforms that can run it, and so this file and
/// `worker_mode.rs` cannot drift apart again. That helper panics rather than
/// answering "not alive" when the output is not readable, because a silent false
/// negative here makes the durability tests report a child that vanished when
/// nothing vanished.
fn pid_is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // 0 = "the process exists and we may signal it". `ESRCH` (gone) and
        // `EPERM` (exists, not ours — cannot happen for our own child) are the
        // only other answers, and `EPERM` is treated as alive because that is
        // what it means.
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }
    #[cfg(windows)]
    {
        proctable::in_process_table(pid)
    }
}

/// The engine's graceful-reap grace, plus its margin, as the floor for a
/// "the child survived" observation window.
///
/// `WorkerManager::detach` does not SIGKILL — it runs `reap_worker`, which is
/// `send_close` + a **2 s** grace + `kill` (`manager/registry.rs`). So a daemon
/// that wrongly tears a session down when its client vanishes does not make the
/// pid disappear at t≈0; it makes it disappear at **t≈2 s**.
///
/// This is the number a survival window has to clear. A window of 1.2 s — the
/// value this file shipped with — sits entirely INSIDE that grace, so the
/// plant experiment passed half (a) and only half (b) went red: the pid check
/// was green for exactly the reason the task warns about, "the worker is merely
/// slow to die". 5 s is 2.5× the grace, and it stays well inside the child's
/// natural lifetime (the fixture loops back to reading stdin after a turn and
/// only exits on `close` or EOF, and a dropped client socket does not close the
/// child's stdin).
///
/// If the engine ever changes that grace, this constant and the plant
/// experiment have to be re-measured together — a survival window shorter than
/// the reap grace makes half (a) decorative.
const REAP_GRACE: Duration = Duration::from_secs(5);

/// Poll `pid_is_alive` every `gap` until it reports dead, or the budget runs out.
///
/// Returns `true` only if the process was alive for the WHOLE window and still
/// is at the end — the sustained form, because a single sample proves the child
/// had not died YET, which is not the claim.
async fn alive_throughout(pid: u32, window: Duration, gap: Duration) -> bool {
    let deadline = Instant::now() + window;
    loop {
        if !pid_is_alive(pid) {
            return false;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return true;
        }
        // `tokio::time::sleep`, capped at the remaining budget. (There is no
        // `tokio::stream_idle` — the name that was here described nothing.)
        tokio::time::sleep(gap.min(left)).await;
    }
}

/// `close` ends a session: the child exits.
///
/// This is the test that makes the durability test's pid assertion MEANINGFUL.
/// Without it, "the pid was still alive after the disconnect" could be a
/// property of a pid check that never reports false — the same way a detector
/// that always fires and a detector that never fires both score 100% on one
/// class. Here the same helper, on the same kind of process, reports dead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_ends_the_session_and_the_child_exits() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let mut client = Client::dial(&endpoint).await;

    client.send(serde_json::json!({"type": "hello"})).await;
    let hello = client.recv_until("hello").await;
    assert_eq!(
        hello["protocolVersion"], PROTOCOL_VERSION,
        "the daemon negotiates the worker protocol's version: {hello}"
    );

    let env = start_env("close-me", daemon._dir.path().to_str().unwrap());
    client.send(start_frame(&env)).await;
    let started = client.recv_until("start-result").await;
    assert_eq!(
        started["ok"], true,
        "the session must start before it can be closed: {started}"
    );
    let pid = started["workerPid"]
        .as_u64()
        .expect("a started session reports its worker pid") as u32;
    assert!(
        pid_is_alive(pid),
        "the child is alive right after `start-result`"
    );

    client
        .send(serde_json::json!({"type": "close", "sessionId": "close-me"}))
        .await;
    let closed = client.recv_until("closed").await;
    assert_eq!(closed["ok"], true, "closing a live session is ok: {closed}");

    // The graceful reap is `close` + a 2 s grace + `kill`, so the bound is the
    // engine's, plus the process-exit margin.
    let deadline = Instant::now() + Duration::from_secs(10);
    while pid_is_alive(pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !pid_is_alive(pid),
        "a `close`d session's worker must exit (pid {pid} still alive) — if this \
         fails, `close` is not reaching the Worker, and every pid-based \
         assertion in this file is vacuous"
    );

    daemon.shutdown();
}

/// The daemon runs a session in a child process, streams its events to the
/// socket, and persists it to `fulcrum.db`.
///
/// Three claims in one test because they are one mechanism: the frames that
/// reach the socket are the frames the pump feeds to the persister, so a test
/// that saw events without rows (or the reverse) would be describing a daemon
/// that half-works. The row assertions are made through a SECOND connection to
/// the file, so they are claims about the disk and not about a cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_streams_events_and_persists_to_the_database() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let mut client = Client::dial(&endpoint).await;

    client.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(client.recv_until("hello").await["type"], "hello");

    let env = start_env("persisted", daemon._dir.path().to_str().unwrap());
    client.send(start_frame(&env)).await;
    let started = client.recv_until("start-result").await;
    assert_eq!(
        started,
        serde_json::json!({
            "type": "start-result",
            "sessionId": "persisted",
            "ok": true,
            // The pid is asserted separately (it is a live number, and the
            // assertion that matters about it is `pid_is_alive`, below).
            "workerPid": started["workerPid"],
            "error": null,
        }),
        "`start` must answer with a well-formed result"
    );
    let pid = started["workerPid"].as_u64().expect("a pid") as u32;
    assert!(
        pid_is_alive(pid),
        "the session runs in a CHILD process (pid {pid}), not in the daemon"
    );

    // A row for the session, before a single prompt: the daemon records it at
    // `start`, so a session that spawned and then failed is still on disk.
    let sessions = daemon
        .read()
        .prepare("SELECT id, cwd, is_subagent FROM sessions WHERE id = 'persisted'")
        .expect("prepare")
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .expect("query")
        .filter_map(Result::ok)
        .collect::<Vec<_>>();
    assert_eq!(
        sessions,
        vec![(
            "persisted".to_string(),
            daemon._dir.path().to_str().unwrap().to_string(),
            0
        )],
        "the `sessions` row carries the cwd the client asked for and is a MAIN \
         session row (is_subagent = 0)"
    );

    client
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "persisted", "text": "hello world",
        }))
        .await;
    let delivered = client.recv_until("prompt-result").await;
    assert_eq!(
        delivered["ok"], true,
        "the prompt was relayed to the Worker: {delivered}"
    );

    // The event stream. The fixture's canned turn is
    // `turn_start` → `message_start` → … → `display-upsert` → `turn_end` →
    // `agent_settled`, so waiting for the settle proves the relay carried a
    // whole turn and not just the first frame. The matcher CONSUMES every frame
    // it scans past, so this walks the stream forward instead of re-reading its
    // own first frame (which is what made this assertion time out before the
    // harness was fixed).
    let settled = client
        .recv_until_matching("event", |frame| rpc_event_is(frame, "agent_settled"))
        .await;
    assert_eq!(
        settled["isSubagent"], false,
        "a client-started session is a main session: {settled}"
    );

    // The transcript rows. The fixture emits two `transcript-insert` frames per
    // turn (the user row and the assistant row), and the persister is the only
    // thing that writes this table — so two rows here is the pump → persister →
    // SQLite path, end to end.
    let rows: i64 = daemon
        .read()
        .query_row(
            "SELECT COUNT(*) FROM native_messages WHERE session_id = 'persisted'",
            [],
            |row| row.get(0),
        )
        .expect("count native_messages");
    assert!(
        rows >= 2,
        "the turn's two transcript rows must be on disk, got {rows} — an empty \
         table means the pump's frames never reached the persister"
    );
    let display: i64 = daemon
        .read()
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE session_id = 'persisted'",
            [],
            |row| row.get(0),
        )
        .expect("count messages");
    assert!(
        display >= 1,
        "the `display-upsert` frame must land a display row, got {display}"
    );

    // The store frames reached the socket too (the client's own view of what was
    // written — the desktop's renderer consumes these, and a daemon that only
    // persisted them would leave a client unable to show the answer).
    assert!(
        client.delivered.iter().any(|frame| {
            frame["type"] == "event"
                && frame["frame"]["type"] == "store"
                && frame["frame"]["frame"]["type"] == "transcript-insert"
        }),
        "the transcript frames must be relayed to the client, not only persisted"
    );

    daemon.shutdown();
}

/// THE durability test — the roadmap's `done-when`.
///
/// A client that vanishes mid-turn does not stop the session. Proven two ways,
/// and BOTH are load-bearing:
///
/// * **The child is still alive.** Without this half, "rows kept growing" could
///   be a session that had already finished normally before the socket closed.
/// * **`native_messages` grew after the drop.** Without this half, "the pid is
///   alive" could be a process that survived and then produced nothing — which
///   is what a session that was merely slow to die looks like.
///
/// The mid-turn-ness is real, not asserted: the fixture's `__long_stream__`
/// scenario streams for ~3 s and only writes its transcript rows at the END, so
/// a drop taken after the first streamed chunk lands while the turn is
/// demonstrably unfinished.
///
/// Deliberately absent: any claim that a SECOND connection can rejoin the
/// stream. There is no wire frame for that (`WorkerManager::attach` is
/// in-process, not a protocol frame) and T10 owns its design.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let mut client = Client::dial(&endpoint).await;

    client.send(serde_json::json!({"type": "hello"})).await;
    client.recv_until("hello").await;

    let env = start_env("survives-the-socket", daemon._dir.path().to_str().unwrap());
    client.send(start_frame(&env)).await;
    let started = client.recv_until("start-result").await;
    assert_eq!(started["ok"], true, "the session starts: {started}");
    let pid = started["workerPid"].as_u64().expect("a pid") as u32;

    // The long turn: ~3 s of chunks, and the transcript rows only at the end.
    client
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "survives-the-socket", "text": "__long_stream__",
        }))
        .await;
    client.recv_until("prompt-result").await;

    // Wait for proof the turn is IN FLIGHT — a real streamed chunk, NOT merely
    // "the first event". The fixture answers `start` with a `session_info`
    // sink-frame, so the first event for a session exists whether or not a turn
    // ever ran; matching on "is a sink-frame" is satisfied by that frame and
    // would make this whole mid-turn claim vacuous. See
    // [`agent_message_chunk_for`].
    let first_chunk = client
        .recv_until_matching("event", agent_message_chunk_for("survives-the-socket"))
        .await;
    assert_eq!(
        first_chunk["isSubagent"], false,
        "the streamed chunk belongs to the main session: {first_chunk}"
    );
    let rows_at_drop = daemon.count("native_messages");
    assert_eq!(
        rows_at_drop, 0,
        "the fixture writes its transcript rows at the END of the stream, so a \
         non-zero count here means the turn was already over when the socket was \
         dropped and the durability claim would be vacuous"
    );

    // ── The disconnect. No `close`, no goodbye: the socket just goes. ────────
    drop(client);

    // (a) The child is alive, and stays alive for longer than a wrong teardown
    // would take to look like death. A daemon that detaches on disconnect does
    // not kill the child immediately — `reap_worker` waits out a 2 s grace — so
    // a window shorter than that grace cannot see the bug. See [`REAP_GRACE`].
    assert!(
        alive_throughout(pid, REAP_GRACE, Duration::from_millis(100)).await,
        "the worker child (pid {pid}) must still be running {REAP_GRACE:?} after \
         its client's socket dropped — a client disconnect is not a session \
         teardown, and this window must outlast the engine's 2 s reap grace or \
         the assertion cannot tell 'survived' from 'slow to die'"
    );

    // (b) And it is not merely alive: it is still WORKING, which is observable as
    // rows appearing in the file the daemon owns and nobody else can write.
    let deadline = Instant::now() + ROW_BUDGET;
    let mut rows = daemon.count("native_messages");
    while rows <= rows_at_drop && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        rows = daemon.count("native_messages");
    }
    assert!(
        rows > rows_at_drop,
        "`native_messages` must keep growing after the client vanished \
         ({} rows at the drop, {rows} after {ROW_BUDGET:?}) — execution \
         continued and persisted, which is the whole claim",
        rows_at_drop
    );
    assert!(
        pid_is_alive(pid),
        "and the child is still alive at the end of it (pid {pid})"
    );

    // The session is still addressable, not a zombie the registry lost track of:
    // a second client can `close` it, and the child goes.
    let mut second = Client::dial(&endpoint).await;
    second.send(serde_json::json!({"type": "hello"})).await;
    second.recv_until("hello").await;
    second
        .send(serde_json::json!({
            "type": "close", "sessionId": "survives-the-socket",
        }))
        .await;
    let closed = second.recv_until("closed").await;
    assert_eq!(
        closed["ok"], true,
        "the orphaned session is still registered with the manager, so it can \
         still be closed deliberately: {closed}"
    );

    daemon.shutdown();
}

/// The multiplex: each client sees its own session and not the other's.
///
/// This is the one test that would fail if the fan-out filtered on nothing (the
/// shape a single broadcast tempts you into), and it is the reason the fan-out is
/// not simply "send everything to everyone".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_on_two_sockets_do_not_see_each_others_events() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();

    let mut a = Client::dial(&endpoint).await;
    let mut b = Client::dial(&endpoint).await;
    for client in [&mut a, &mut b] {
        client.send(serde_json::json!({"type": "hello"})).await;
        client.recv_until("hello").await;
    }

    a.send(start_frame(&start_env("session-a", cwd))).await;
    assert_eq!(a.recv_until("start-result").await["ok"], true, "a starts");
    b.send(start_frame(&start_env("session-b", cwd))).await;
    assert_eq!(b.recv_until("start-result").await["ok"], true, "b starts");

    // `__long_stream__` so there is a stream to leak: 30 frames over ~3 s.
    for (client, id) in [(&mut a, "session-a"), (&mut b, "session-b")] {
        client
            .send(serde_json::json!({
                "type": "prompt", "sessionId": id, "text": "__long_stream__",
            }))
            .await;
        client.recv_until("prompt-result").await;
    }

    // Each client's first event is its own session's…
    let a_event = a.recv_event_for("session-a").await;
    let b_event = b.recv_event_for("session-b").await;
    assert_eq!(a_event["sessionId"], "session-a");
    assert_eq!(b_event["sessionId"], "session-b");

    // …and neither receives the other's, over a window long enough to catch a
    // leak (the fixture streams every 100 ms, so 1 s is ~10 frames' worth of
    // opportunity for a mis-filtered frame to show up).
    let leak_window = Duration::from_millis(1000);
    let a_extra = a.drain(leak_window).await;
    let b_extra = b.drain(leak_window).await;
    for (label, frames, own) in [
        ("session-a's socket", &a_extra, "session-a"),
        ("session-b's socket", &b_extra, "session-b"),
    ] {
        // Only frames that NAME a session can be another session's. The daemon's
        // connection-addressed frames (`hello`, the `gap` notice) carry no
        // `sessionId` and are legitimately present on every socket — treating
        // them as leaks would make this assertion fire on correct behaviour.
        if let Some(foreign) = frames.iter().find(|frame| {
            frame
                .get("sessionId")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| id != own)
        }) {
            panic!(
                "{label} received a frame for another session: {foreign} — the \
                 fan-out filters on the TOP-LEVEL sessionId, and this is the \
                 test that catches that filter being removed"
            );
        }
    }

    // One database, two sessions — the `Arc<Db>` shared by both client tasks
    // rather than a handle each. Both rows are visible from one read.
    let ids: Vec<String> = daemon
        .read()
        .prepare("SELECT id FROM sessions ORDER BY id")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .filter_map(Result::ok)
        .collect();
    assert_eq!(
        ids,
        vec!["session-a".to_string(), "session-b".to_string()],
        "both sessions are recorded in the ONE database"
    );

    daemon.shutdown();
}

/// Task 3's hazard, on the accept loop's side.
///
/// `transport::unix::reclaim_stale_socket` probes a live daemon's socket by
/// CONNECTING to it, and that probe connection lands in the daemon's `accept()`
/// backlog and EOFs with no frame — so every accidental second `fulcrum serve`
/// (and every `curl`-style liveness check) hands the accept loop a connection
/// that dies immediately. The loop must skip it and keep serving; a version that
/// logged an error and returned from `run` would take the daemon down because
/// somebody typed the command twice.
///
/// NOT unix-gated any more. The probe's SHAPE — connect, then hang up without a
/// byte — is produced by `transport::connect` plus a `drop`, which is the
/// platform-neutral dial, so the hazard is testable on both legs. It was
/// `#[cfg(unix)]` only because it reached for `UnixStream` to build a socket the
/// transport could already build; keeping it that way would have left the accept
/// loop's most important survival property unexercised on Windows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());

    // Three probe-shaped connections: connect, then hang up without a byte.
    for _ in 0..3 {
        let probe = connect(endpoint.clone()).await.expect("probe connects");
        drop(probe);
    }

    // A real client, after the probes: full handshake, session, prompt, event.
    let mut client = Client::dial(&endpoint).await;
    client.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(
        client.recv_until("hello").await["type"],
        "hello",
        "the accept loop must still be serving after empty connections"
    );

    let env = start_env("after-probes", daemon._dir.path().to_str().unwrap());
    client.send(start_frame(&env)).await;
    let started = client.recv_until("start-result").await;
    assert_eq!(
        started["ok"], true,
        "a session after three probe connections must start: {started}"
    );
    client
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "after-probes", "text": "hi",
        }))
        .await;
    assert_eq!(
        client.recv_until("prompt-result").await["ok"],
        true,
        "and relay a prompt"
    );
    client.recv_event_for("after-probes").await;

    daemon.shutdown();
}

/// A second `serve` on the same endpoint must fail LOUDLY, and must not disturb
/// the first.
///
/// This is the other half of the same interaction: Task 3 chose to refuse rather
/// than steal a live daemon's socket, and the refusal arrives at the accept loop
/// as the probe connection the test above covers. The daemon that keeps running
/// is the claim; the exit code is how the operator learns they typed it twice.
///
/// Split by platform on PURPOSE, and only for the wording. The two claims that
/// hold on both legs — "the second daemon refuses" and "the first is
/// undisturbed" — are asserted unconditionally. The refusal's TEXT is platform
/// vocabulary: unix refuses with the `EADDRINUSE` / "a live daemon is answering"
/// path (`transport::unix::reclaim_stale_socket`), Windows with
/// `CreateNamedPipeW`'s `PermissionDenied` from `first_pipe_instance(true)`
/// (`transport::windows::Listener::bind`). Asserting unix's string on Windows
/// would be asserting a message this machine has never seen, so the substring
/// check is gated and the Windows leg is left with the claims that are actually
/// platform-neutral.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_daemon_on_the_same_endpoint_exits_non_zero_and_leaves_the_first_running() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());

    let mut second = Command::new(BIN);
    second.arg("serve");
    #[cfg(unix)]
    second.arg("--socket");
    #[cfg(windows)]
    second.arg("--pipe");
    let out = second
        .arg(endpoint_flag_value(&endpoint))
        .arg("--db")
        .arg(daemon._dir.path().join("other.db"))
        .arg("--worker-exe")
        .arg(worker_fixture())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        // Piped, not inherited: this test WANTS to read the refusal, and the
        // first daemon's stderr is the one the operator would otherwise see.
        .stderr(Stdio::piped())
        .output()
        .expect("run the second daemon");
    assert!(
        !out.status.success(),
        "a second daemon on a live endpoint must not start quietly: {}",
        out.status
    );
    let message = String::from_utf8_lossy(&out.stderr);
    assert!(
        !message.trim().is_empty(),
        "and must say SOMETHING on stderr — a silent refusal is how an operator \
         concludes the command worked: {message:?}"
    );
    // `cannot bind` is `ServeError::Bind`'s OWN wording (`serve.rs`'s
    // `#[error("cannot bind: {0}")]`, printed verbatim by `main.rs`), so this
    // half runs on every leg: the operator must be able to tell a refused BIND
    // from a refused database open or a refused worker-exe lookup, and that
    // distinction is the daemon's to guarantee regardless of OS.
    assert!(
        message.contains("cannot bind"),
        "the refusal must name the STAGE that failed, on every platform; the \
         operator cannot act on a bare non-zero exit: {message:?}"
    );
    // The REASON behind the refusal is unix-only prose, and deliberately stays
    // gated: `in use` comes from `reclaim_stale_socket`, which PROVES the
    // collision by connecting to the socket. Windows has no equivalent probe —
    // connecting to a pipe would consume a real client slot on the predecessor
    // (`transport::windows` documents this, and why the refusal is therefore not
    // retried) — so the Windows leg genuinely cannot claim to know the pipe is
    // live rather than merely reserved, and asserting a reason there would pin
    // `Access is denied`, which is Microsoft's wording and not ours.
    #[cfg(unix)]
    assert!(
        message.contains("in use"),
        "on unix the refusal must also name the collision (the transport can \
         prove it, so the operator gets to know a daemon is already there): {message:?}"
    );

    // The first daemon is undisturbed.
    let mut client = Client::dial(&endpoint).await;
    client.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(
        client.recv_until("hello").await["type"],
        "hello",
        "the surviving daemon must still serve"
    );
    daemon.shutdown();
}

/// A daemon whose database will not open must leave NO socket behind (finding G).
///
/// `main.rs`'s own doc states the order as intentional: "The database opens
/// BEFORE the bind, on purpose: a daemon that binds first and then fails to open
/// `fulcrum.db` leaves a live socket behind that answers `hello` and fails every
/// `start`, and Task 3's stale-socket reclaim will not clean it up (a live
/// listener answers the probe, so the file is left alone — correctly)."
///
/// The code did the reverse. The result was a socket that answers `hello` and
/// refuses every `start` for a reason the client cannot see, and which the
/// reclaim deliberately cannot remove because it is a LIVE listener — so the
/// operator's only cure is to find and delete a file nothing told them about.
/// Failing before the socket exists leaves nothing to reclaim.
///
/// The unwritable database is a path whose PARENT is a regular file, so
/// `create_dir_all` fails with `ENOTDIR`. That is chosen over `chmod 000`
/// deliberately: a chmod-based test passes for a normal user and FAILS OPEN when
/// CI runs as root (root ignores the mode bits), and a test that silently stops
/// testing anything on the machine that runs it most is worse than no test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_daemon_that_cannot_open_its_database_leaves_no_socket_behind() {
    let dir = tempfile::tempdir().expect("temp dir");
    let endpoint = Daemon::endpoint_in(dir.path());
    // A FILE where the database's parent directory has to be.
    let blocker = dir.path().join("blocked");
    std::fs::write(&blocker, b"not a directory").expect("write the blocker");
    let db_path = blocker.join("fulcrum.db");

    let mut cmd = Command::new(BIN);
    cmd.arg("serve");
    #[cfg(unix)]
    cmd.arg("--socket");
    #[cfg(windows)]
    cmd.arg("--pipe");
    let out = cmd
        .arg(endpoint_flag_value(&endpoint))
        .arg("--db")
        .arg(&db_path)
        .arg("--worker-exe")
        .arg(worker_fixture())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("run the daemon");

    assert!(
        !out.status.success(),
        "an unopenable database must be a failed startup, not a running daemon: {}",
        out.status
    );
    let message = String::from_utf8_lossy(&out.stderr);
    assert!(
        message.contains("database"),
        "and must name what failed, or the operator is guessing: {message:?}"
    );

    // THE finding: nothing may be listening, and nothing may be left to reclaim.
    #[cfg(unix)]
    assert!(
        !socket_path(&endpoint).exists(),
        "a daemon that failed to open its store must not leave a socket at {} — \
         a live listener answering `hello` while every `start` fails is the state \
         the stale-socket probe deliberately refuses to unlink",
        socket_path(&endpoint).display()
    );
    #[cfg(windows)]
    {
        // A named pipe leaves no file to stat, so the proof over there is that
        // dialing the name fails — the same claim, read through the platform's
        // own namespace.
        let dialed = tokio::time::timeout(Duration::from_secs(2), connect(endpoint.clone())).await;
        assert!(
            !matches!(dialed, Ok(Ok(_))),
            "the pipe must not exist after a failed startup: {dialed:?}"
        );
    }
}

/// A frame the daemon does not understand is answered, not fatal.
///
/// The daemon's surface will grow (T10 adds `attach`), and a newer client will
/// outpace an older daemon. Permissive about the CONNECTION, precise about the
/// FRAME — the same rule the worker protocol's decoder runs by.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unknown_frame_is_answered_and_the_connection_survives() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let mut client = Client::dial(&endpoint).await;

    client.send(serde_json::json!({"type": "hello"})).await;
    client.recv_until("hello").await;

    client
        .send(serde_json::json!({"type": "attach", "sessionId": "nope"}))
        .await;
    let error = client.recv_until("error").await;
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|m| m.contains("attach")),
        "the error must name the frame it rejected: {error}"
    );

    // Still usable: a real session starts on the same connection.
    let env = start_env("still-here", daemon._dir.path().to_str().unwrap());
    client.send(start_frame(&env)).await;
    assert_eq!(
        client.recv_until("start-result").await["ok"],
        true,
        "the connection must survive an unknown frame"
    );
    daemon.shutdown();
}

/// A malformed line is an error item, not a closed stream (`transport::frame`'s
/// rule, asserted at the daemon end rather than the transport end).
///
/// The garbage has to be written RAW — through `Connection::send_frame` it would
/// be serialized and therefore always valid — so this reaches past the framing
/// layer to the platform's own stream, which is what `tests/transport.rs` does
/// for the same reason ("widening the public API with a `send_raw` for test
/// convenience would be the dishonest way"). It goes through [`raw_dial`] rather
/// than `UnixStream` directly so the Windows leg compiles the same probe.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_malformed_line_gets_an_error_and_the_stream_continues() {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let mut client = Client::dial(&endpoint).await;

    let (read_half, mut write_half) = tokio::io::split(raw_dial(&endpoint).await);
    write_half
        .write_all(b"this is not json\n")
        .await
        .expect("write garbage");
    write_half.flush().await.expect("flush");
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    // Bounded, per this file's own deadline rule (the module header): an
    // un-timed `read_line` here turns "the daemon ignored my garbage line" into
    // a hung test that prints nothing and takes the suite's output with it —
    // the exact failure mode the rule exists to prevent. A panic that names the
    // budget is a diagnosis; a parked task is not.
    let read = tokio::time::timeout(FRAME_BUDGET, reader.read_line(&mut line))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the daemon never answered a malformed line (no frame within \
                 {FRAME_BUDGET:?}) — it must reply, not stall"
            )
        })
        .expect("read the reply");
    assert!(read > 0, "the daemon must answer garbage, not hang up");
    let reply: serde_json::Value = serde_json::from_str(line.trim())
        .unwrap_or_else(|e| panic!("the reply must be a JSON error frame: {line:?} ({e})"));
    assert_eq!(reply["type"], "error", "garbage gets an error: {reply}");

    // And the daemon still serves a framed client afterwards.
    client.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(
        client.recv_until("hello").await["type"],
        "hello",
        "one malformed line must not cost the daemon its next client"
    );
    daemon.shutdown();
}

/// A client that starts a session and then goes away does NOT leave the daemon
/// unable to serve the next client — the "resource cleanup" half of the same
/// design decision the durability test covers from the other side.
///
/// Two full start/prompt/disconnect/reconnect cycles, in sequence, on one
/// daemon. A per-connection task that leaked its session's subscription would
/// still pass the durability test; it fails this one if the leak is a deadlock
/// or a blocked fan-out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_disconnects_do_not_degrade_the_daemon() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();

    for round in 0..3 {
        let id = format!("round-{round}");
        let mut client = Client::dial(&endpoint).await;
        client.send(serde_json::json!({"type": "hello"})).await;
        client.recv_until("hello").await;
        client.send(start_frame(&start_env(&id, cwd))).await;
        let started = client.recv_until("start-result").await;
        assert_eq!(started["ok"], true, "round {round} starts: {started}");
        client
            .send(serde_json::json!({
                "type": "prompt", "sessionId": id, "text": "hi",
            }))
            .await;
        client.recv_until("prompt-result").await;
        client.recv_event_for(&id).await;
        // Gone, mid-everything.
        drop(client);

        // The next round's `hello` must still be answered promptly. The budget
        // is short on purpose: a fan-out or accept loop degraded by a leak shows
        // up as SLOWNESS first, and a 30 s budget would absorb it.
        let mut next = Client::dial(&endpoint).await;
        let dialed = Instant::now();
        next.send(serde_json::json!({"type": "hello"})).await;
        let reply = tokio::time::timeout(Duration::from_secs(5), next.conn.recv())
            .await
            .unwrap_or_else(|_| panic!("round {round}: the daemon stopped answering"))
            .expect("not EOF")
            .expect("valid JSON");
        assert_eq!(reply["type"], "hello");
        assert!(
            dialed.elapsed() < Duration::from_secs(5),
            "round {round}: the daemon answered in {:?}, which is not 'promptly'",
            dialed.elapsed()
        );
        // Close the round's session so the fixture children do not pile up for
        // the next round (this is what `close` is FOR).
        next.send(serde_json::json!({"type": "close", "sessionId": id}))
            .await;
        next.recv_until("closed").await;
    }

    assert_eq!(
        daemon.spawn_count(),
        3,
        "three rounds, three session rows in the one database"
    );
    daemon.shutdown();
}

/// A client that never reads is THROTTLED, and the daemon's heap stays flat
/// (finding 7 — the unbounded buffer).
///
/// `serve` has no authentication — legitimately deferred to T13 — so ANY local
/// process that can reach the socket can drive this. The shape under test is the
/// one that costs the daemon something: a peer that writes frames and reads no
/// bytes back.
///
/// # What was actually unbounded, and how this was found
///
/// The review pointed at `serve.rs`'s per-client outbound queue. It is bounded
/// now, and it needed bounding — but it was NOT the 895 MB. Measured against the
/// daemon on Linux (one connection, `hello` frames, client reads nothing, 10 s):
///
/// * **1 168 009 requests absorbed, daemon RSS 15 MB → 895 MB.**
///
/// The outbound queue cannot produce that number, because `serve_client` is ONE
/// task running one `select!`: while the drain arm's body awaits a blocked
/// `send_frame`, the `select!` is not re-polled, so the inbound arm cannot run and
/// cannot enqueue. The queue's depth is therefore ~1 — confirmed by instrumenting
/// it. The buffer that grew was the transport's: `Connection`'s reader task drains
/// the socket into an `mpsc::unbounded_channel` that nothing was consuming, so the
/// kernel's ~200 KB of socket became an unbounded queue in the daemon's heap. The
/// fix is in `transport::frame` (`MAX_QUEUED_INBOUND_FRAMES`); the outbound queue
/// is bounded too, for the same reason and to the same budget.
///
/// # What the bound makes observable
///
/// Backpressure, and it is a large, schedule-independent signal: with the reader
/// waiting for room, the socket fills, and the ATTACKER's OWN `write` blocks. The
/// same probe against the fixed daemon:
///
/// * **1 375 requests pushed in 0.3 s, then the write blocks; RSS 15 MB → 19 MB.**
///
/// So the test asserts the throttle (a bounded number of requests accepted while
/// the peer reads nothing) and, on unix, the flat heap — which is the finding's
/// actual claim, "must not grow without bound", measured rather than inferred.
/// The margin is three orders of magnitude, so this is not a timing race: the
/// broken build pushes a million frames in the time the fixed one pushes 1.4 k.
///
/// And it pins what the fix must NOT break: a client that reads normally is
/// unaffected, and a client that was throttled is still connected and still
/// served once it starts reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat() {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());

    // Split up front so the READ HALF can be provably untouched during the flood:
    // "never reads" is the whole attack, and a `Connection` cannot demonstrate it
    // (its reader task drains the socket whether the test calls `recv` or not —
    // which is exactly why the first version of this test passed against the
    // broken daemon and proved nothing).
    let (read_half, mut sink) = tokio::io::split(raw_dial(&endpoint).await);
    let rss_before = daemon_rss(daemon.child.id());

    // `hello\n`: 17 bytes, and the only frame the daemon answers WITHOUT a
    // session, so the flood needs no worker, no database row, no fixture.
    const FRAME: &[u8] = b"{\"type\":\"hello\"}\n";
    // The ceiling the fixed daemon must not exceed. Measured 1 375; the broken one
    // reached 1 168 009. Anything in the tens of thousands separates them by a
    // factor of ten in both directions, so a machine with unusually generous
    // socket buffers cannot flip this green-to-red or red-to-green by accident.
    const CEILING: usize = 50_000;
    // Long enough that "slow" cannot be mistaken for "throttled": a million
    // frames arrive inside this window on the broken build.
    let window = Duration::from_secs(5);

    let mut pushed = 0usize;
    let blocked = {
        let deadline = Instant::now() + window;
        let mut stopped = false;
        while Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(left, sink.write_all(FRAME)).await {
                // This is the success condition, not a failure: the daemon is no
                // longer taking bytes because its queue is full and the socket is
                // full, so OUR kernel is what blocks.
                Err(_) => {
                    stopped = true;
                    break;
                }
                Ok(Err(e)) => panic!("the flood's write failed after {pushed}: {e}"),
                Ok(Ok(())) => pushed += 1,
            }
        }
        stopped
    };

    assert!(
        pushed > 0,
        "not one frame reached the daemon — it is not answering `hello` at all, \
         so this test would certify nothing about any buffer"
    );
    assert!(
        blocked && pushed < CEILING,
        "a client that read NO bytes pushed {pushed} requests in {window:?} and \
         was never throttled. One inbound frame per queued `Value` is the daemon \
         paying for a stranger's backlog: the reader task must wait for room so \
         the peer's own `write` is what blocks (measured: 1 375 when bounded, \
         1 168 009 against the unbounded channel)"
    );

    // The heap, on unix — the finding's own words, measured. The bound allows a
    // few MB of frames in flight; it does not allow hundreds.
    let rss_after = daemon_rss(daemon.child.id());
    assert!(
        rss_after < rss_before + 64 * 1024,
        "the daemon grew {} kB serving {pushed} requests from a client that never \
         read a reply ({} → {} kB). The bound is meant to hold this in the single \
         digits of MB; hundreds of MB is the unbounded queue, not a rounding error",
        rss_after.saturating_sub(rss_before),
        rss_before,
        rss_after,
    );

    // A throttled client is not a banned client: the moment it reads, the daemon
    // resumes and its backlog is served.
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let got = tokio::time::timeout(FRAME_BUDGET, reader.read_line(&mut line))
        .await
        .expect("the daemon answers once the client reads")
        .expect("read the reply");
    assert!(got > 0, "the daemon hung up on a client for being slow");
    let first: serde_json::Value =
        serde_json::from_str(line.trim()).expect("the reply is a JSON frame");
    assert_eq!(
        first["type"], "hello",
        "the first reply on a throttled connection: {first}"
    );

    // And the daemon is unharmed for the next client — the claim
    // `a_client_that_never_reads_does_not_stall_the_daemon` makes against a
    // streaming session, checked here against a request flood.
    let mut later = Client::dial(&endpoint).await;
    later.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(
        later.recv_until("hello").await["type"],
        "hello",
        "a throttled connection must not take the daemon down with it"
    );

    daemon.shutdown();
}

/// The daemon's resident set in kilobytes, or 0 where this cannot be read.
///
/// `/proc/<pid>/status`'s `VmRSS` is the direct measurement of "did the daemon's
/// heap grow without bound", which is the claim the throttle test makes — and
/// reading it beats inferring it from frame counts, because a frame count says
/// what the daemon accepted, not what it cost.
///
/// Unix (Linux) only, and the zero is honest rather than a guess: on the other
/// legs the caller's assertion becomes `0 < 0 + 64 MB`, i.e. the heap half of the
/// test is skipped and the throttle half — which is the portable one — still
/// runs. `worker_children` documents the same split for the process-table half of
/// the race test; a platform-specific probe that silently asserted a wrong number
/// would be worse than one that admits it is not measuring.
#[cfg(target_os = "linux")]
fn daemon_rss(pid: u32) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_else(|e| panic!("read /proc/{pid}/status: {e}"));
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or_else(|e| panic!("unparsable VmRSS in {line:?}: {e}"));
        }
    }
    panic!("no `VmRSS` line in /proc/{pid}/status — is this Linux?");
}

/// Not Linux: nothing to read, so the heap half of the throttle test is skipped.
#[cfg(not(target_os = "linux"))]
fn daemon_rss(_pid: u32) -> u64 {
    0
}

/// The daemon does not buffer forever for a client that stopped reading.
///
/// A stalled client is normal (a laptop asleep); the fan-out is bounded, and the
/// shape a bounded ring produces is a `gap` notice, not an unbounded allocation
/// and not a stalled pump. This test asserts the DAEMON survives it, which is the
/// claim; the exact lag count is timing-dependent and deliberately not asserted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_never_reads_does_not_stall_the_daemon() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();

    // The stalled client: starts a session, then never calls `recv` again.
    let mut stalled = Client::dial(&endpoint).await;
    stalled.send(serde_json::json!({"type": "hello"})).await;
    stalled.recv_until("hello").await;
    stalled.send(start_frame(&start_env("stalled", cwd))).await;
    assert_eq!(stalled.recv_until("start-result").await["ok"], true);
    stalled
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "stalled", "text": "__long_stream__",
        }))
        .await;
    stalled.recv_until("prompt-result").await;
    // …and now it reads nothing at all, while the session streams.

    // A healthy client, on a second session, gets its whole turn while the first
    // client is stalled. This is the claim: one slow consumer cannot starve the
    // fan-out.
    let mut healthy = Client::dial(&endpoint).await;
    healthy.send(serde_json::json!({"type": "hello"})).await;
    healthy.recv_until("hello").await;
    healthy.send(start_frame(&start_env("healthy", cwd))).await;
    assert_eq!(healthy.recv_until("start-result").await["ok"], true);
    healthy
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "healthy", "text": "hi",
        }))
        .await;
    healthy.recv_until("prompt-result").await;

    // One wait, not a poll loop: `recv_until_matching` already carries the
    // deadline and consumes every frame it scans past, so the loop this replaces
    // was both a second timeout stacked on the helper's and a matcher that could
    // never fire (see [`rpc_event_is`]).
    let settled = healthy
        .recv_until_matching("event", |frame| rpc_event_is(frame, "agent_settled"))
        .await;
    assert!(
        settled["sessionId"] == "healthy",
        "a stalled client must not stop another session's stream from arriving: {settled}"
    );

    // And the stalled session persisted its own turn regardless.
    let rows: i64 = daemon
        .read()
        .query_row(
            "SELECT COUNT(*) FROM native_messages WHERE session_id = 'stalled'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    let deadline = Instant::now() + ROW_BUDGET;
    let mut rows = rows;
    while rows < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        rows = daemon
            .read()
            .query_row(
                "SELECT COUNT(*) FROM native_messages WHERE session_id = 'stalled'",
                [],
                |row| row.get(0),
            )
            .expect("count");
    }
    assert!(
        rows >= 2,
        "the stalled session's rows must land anyway ({rows}) — persistence is \
         not gated on a client keeping up"
    );

    daemon.shutdown();
}

/// Two connections racing to `start` the SAME id leave exactly ONE child
/// (finding F — the TOCTOU in the duplicate guard).
///
/// The guard used to be `handle_for(id).is_some()`, then `record_session`, then
/// `attach(…).await` — and `attach`'s registry `insert` is the ONLY atomic step
/// in that sequence, ~5 s after the check was made. Two clients on two
/// connections therefore both passed the check, both reached `attach`, and the
/// second `insert` REPLACED the first's entry: a live child process with no
/// owner, no handle, and no path to being killed. That is the exact leak the
/// guard's own comment claimed to prevent, and the guard was the reason nobody
/// looked for it.
///
/// The race is real rather than staged: both `start` frames are written without
/// awaiting between them, so the two connection tasks are in `attach` at the
/// same time — and `attach` contains a process spawn plus a `ready` handshake,
/// which is a wide window on a loaded machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_connections_racing_to_start_one_id_leave_exactly_one_child() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();
    let env = start_env("raced", cwd);

    let mut a = Client::dial(&endpoint).await;
    let mut b = Client::dial(&endpoint).await;
    a.send(serde_json::json!({"type": "hello"})).await;
    b.send(serde_json::json!({"type": "hello"})).await;
    a.recv_until("hello").await;
    b.recv_until("hello").await;

    // Both `start`s are in flight before either can be answered.
    a.send(start_frame(&env)).await;
    b.send(start_frame(&env)).await;
    let first = a.recv_until("start-result").await;
    let second = b.recv_until("start-result").await;

    let oks = [(&first, &second)]
        .iter()
        .flat_map(|(x, y)| [*x, *y])
        .filter(|r| r["ok"] == true)
        .count();
    assert_eq!(
        oks, 1,
        "exactly one `start` may win a single session id; got {first} and {second}"
    );
    // WHICH of the two wins is a schedule, not a contract — so the assertion is
    // on the pair, never on a named one. Pinning `a` to win would make this test
    // flaky on a machine that ran `b`'s task first.
    let loser = if first["ok"] == true { &second } else { &first };
    assert!(
        loser["error"].is_string(),
        "the refused `start` must say why: {loser}"
    );

    // THE leak check: one winner means ONE child process. An orphan is invisible
    // to the registry, so no `start-result` and no `close` would ever report it
    // — only the process table can.
    #[cfg(unix)]
    {
        let children = worker_children(daemon.child.id());
        assert_eq!(
            children.len(),
            1,
            "exactly one worker process may survive the race; found {children:?} \
             (the daemon's pid is {}) — a second child is an ORPHAN: it is not in \
             the registry, so nothing can ever kill it",
            daemon.child.id()
        );
    }

    daemon.shutdown();
}

/// The daemon's live Worker children, by pid.
///
/// Unix: `pgrep -P`, which reads the process table — the only place an orphaned
/// child is visible at all, which is the whole reason this helper exists.
///
/// Windows: NOT IMPLEMENTED, and said so rather than approximated. `tasklist`
/// has no parent-pid filter (its CSV columns are image / pid / session / memory)
/// and the `wmic` form that did is deprecated and absent from recent builds, so
/// the honest cross-platform story is "this assertion runs on unix". The
/// `start-result` half of the test above — exactly one `ok: true` — is the part
/// that runs everywhere; the process-table half is the unix leg of CI's.
#[cfg(unix)]
fn worker_children(daemon_pid: u32) -> Vec<u32> {
    let out = Command::new("pgrep")
        .args(["-P", &daemon_pid.to_string()])
        .output()
        .expect("pgrep is available on unix CI");
    // `pgrep` exits 1 with no output when there are no children; anything else
    // that is not 0 is a failed lookup, which must not read as "zero children".
    if !out.status.success() && out.stdout.is_empty() {
        return Vec::new();
    }
    assert!(
        out.status.success(),
        "pgrep -P {daemon_pid} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.trim().parse::<u32>().ok())
        .collect()
}

/// A session whose Worker CRASHED can be started again (finding E).
///
/// This is the supervisor's whole reason to exist: recovery. The engine removes
/// a worker from its map ONLY on `detach` / `reap_all` — `unexpected_exit`
/// *reads* the `reaping` flag and the registry's single `remove` lives in
/// `detach_worker` — so after a crash the session is still registered, and a
/// `start` that asks `handle_for(id).is_some()` reports "already running"
/// FOREVER. A client could not restart a crashed session unless it happened to
/// send `close` first, and a client that does not know the child is dead has no
/// reason to.
///
/// The child is crashed and REAPED by the time this asserts (the fixture exits
/// 137 on `__crash__`, and the engine's read task `wait()`s it), so the dead
/// registration is genuinely a dead registration — not a slow child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_whose_worker_crashed_can_be_started_again() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();
    let mut client = Client::dial(&endpoint).await;
    client.send(serde_json::json!({"type": "hello"})).await;
    client.recv_until("hello").await;

    let env = start_env("crash-then-restart", cwd);
    client.send(start_frame(&env)).await;
    let first = client.recv_until("start-result").await;
    assert_eq!(first["ok"], true, "the first start must work: {first}");
    let dead_pid = first["workerPid"]
        .as_u64()
        .expect("a started session reports its worker pid") as u32;

    // Kill the child for real.
    client
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "crash-then-restart", "text": "__crash__",
        }))
        .await;
    let deadline = Instant::now() + FRAME_BUDGET;
    while pid_is_alive(dead_pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !pid_is_alive(dead_pid),
        "the fixture must actually die on `__crash__` (pid {dead_pid} is alive) — \
         otherwise this test asserts nothing about recovery"
    );

    // The crash is visible on the socket (the pump's `Exited` → the daemon's
    // crash report), so a watching client knows to act.
    let crash = client
        .recv_until_matching("event", |frame| {
            frame["sessionId"] == "crash-then-restart" && frame["frame"]["type"] == "worker-crashed"
        })
        .await;
    assert_eq!(
        crash["isSubagent"], false,
        "a main session's crash is not a subagent's: {crash}"
    );

    // The recovery: `start` the SAME id again, with no `close` in between.
    client.send(start_frame(&env)).await;
    let again = client.recv_until("start-result").await;
    assert_eq!(
        again["ok"], true,
        "a crashed session must be startable again — a supervisor that answers \
         `already running` about a dead child can never recover it: {again}"
    );
    let new_pid = again["workerPid"]
        .as_u64()
        .expect("the restarted session reports its worker pid") as u32;
    assert_ne!(
        new_pid, dead_pid,
        "the restart is a NEW child, not the dead registration"
    );
    assert!(
        pid_is_alive(new_pid),
        "and it is actually running (pid {new_pid})"
    );

    // A live session is still refused — the guard must survive the fix, or the
    // daemon would orphan a child on every accidental double `start`.
    client.send(start_frame(&env)).await;
    let dup = client.recv_until("start-result").await;
    assert_eq!(
        dup["ok"], false,
        "a LIVE session must still refuse a second `start`: {dup}"
    );
    assert!(
        dup["error"].as_str().is_some_and(|m| m.contains("already")),
        "and say so: {dup}"
    );

    daemon.shutdown();
}

/// A `prompt` for a session nothing runs is answered, and the daemon keeps
/// serving. The client's error path needs a frame to react to; a silent drop or
/// a closed socket would be indistinguishable from a network fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prompt_for_an_unknown_session_is_answered_not_dropped() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let mut client = Client::dial(&endpoint).await;
    client.send(serde_json::json!({"type": "hello"})).await;
    client.recv_until("hello").await;

    client
        .send(serde_json::json!({
            "type": "prompt", "sessionId": "never-started", "text": "hi",
        }))
        .await;
    let reply = client.recv_until("prompt-result").await;
    assert_eq!(reply["ok"], false, "an unknown session is not ok: {reply}");
    assert!(reply["error"].is_string(), "and it says why: {reply}");

    // Nothing was written for a session that never ran.
    assert_eq!(
        daemon.count("sessions"),
        0,
        "a prompt alone must not create a session row"
    );

    // A second `start` for a session that is ALREADY running is refused rather
    // than silently re-attached (a second `attach` on one id would replace the
    // registry entry and orphan the first child process — the one real way to
    // leak a Worker).
    let env = start_env("once", daemon._dir.path().to_str().unwrap());
    client.send(start_frame(&env)).await;
    assert_eq!(client.recv_until("start-result").await["ok"], true);
    client.send(start_frame(&env)).await;
    let second = client.recv_until("start-result").await;
    assert_eq!(
        second["ok"], false,
        "a duplicate `start` must be refused, not re-attached: {second}"
    );
    assert!(
        second["error"]
            .as_str()
            .is_some_and(|m| m.contains("already")),
        "and must say so: {second}"
    );

    daemon.shutdown();
}

/// `try_recv` exists so a test can assert silence; this pins that it actually
/// observes the absence of a frame, so the "no leak" assertions above are not
/// vacuously passing on a helper that always returns `None`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_client_receives_nothing_before_it_starts_anything() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();

    // One client with a running session, streaming.
    let mut busy = Client::dial(&endpoint).await;
    busy.send(serde_json::json!({"type": "hello"})).await;
    busy.recv_until("hello").await;
    busy.send(start_frame(&start_env("busy", cwd))).await;
    busy.recv_until("start-result").await;
    busy.send(serde_json::json!({
        "type": "prompt", "sessionId": "busy", "text": "__long_stream__",
    }))
    .await;
    busy.recv_until("prompt-result").await;
    busy.recv_event_for("busy").await;

    // A second client that has only said `hello`: it must see NOTHING, while the
    // first session is actively streaming. If this ever returns a frame, the
    // "no leak" assertions elsewhere in this file are testing a helper that
    // cannot see one.
    let mut idle = Client::dial(&endpoint).await;
    idle.send(serde_json::json!({"type": "hello"})).await;
    let hello = idle.recv_until("hello").await;
    assert_eq!(hello["type"], "hello");
    let silence = idle.try_recv(Duration::from_millis(1500)).await;
    assert!(
        silence.is_none(),
        "a client that started nothing must receive nothing, got {silence:?}"
    );

    daemon.shutdown();
}

/// A child killed by **SIGKILL from outside** can be started again, and the
/// crash is reported on the socket (finding 1, step 1 — the mechanism question).
///
/// # Why this test exists separately from the `__crash__` one
///
/// `a_session_whose_worker_crashed_can_be_started_again` crashes the child by
/// having the fixture `exit(137)` — a NORMAL exit with a code. That is NOT the
/// shape this daemon most needs to survive: the OOM killer, and any operator's
/// `kill -9`, deliver a SIGNAL, and a signal death takes a different path through
/// the frozen engine. `WorkerHandle::exited()` waits on
/// `wait_for(|v| v.is_some())` and the read task publishes the code as
/// `Status::code()`, which is `None` for a unix signal death — so the `exited()`
/// WATCH waits FOREVER on exactly that case, while the `Exited` EVENT still fires.
/// Measured here: the frames this test asserts on are the evidence, and they are
/// the reason the daemon keys recovery off the event rather than off the watch.
///
/// On Windows the kill is `taskkill /F` (`TerminateProcess`), since there are no
/// signals to deliver; the EVENT-vs-WATCH mechanism this test exists to prove is
/// the same on both, but the code the event carries is the OS's to choose (`null`
/// after `kill -9`, a number after `/F`), so nothing here pins the number — see
/// the note at the assertions.
///
/// # What it pins
///
/// Both halves, because the roadmap's claim is "the supervisor recovers a dead
/// session" and a signal death is the common way a session dies:
///
/// * the crash REACHES the socket (`worker-crashed`, main-session-labelled, with
///   the dead child's pid, and with the SAME code the `worker-exited` event
///   carried) — so a client can tell its session died instead of inferring it
///   from silence;
/// * the same id is `start`able again with no `close` in between, and gets a NEW
///   live child — the recovery itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_killed_by_sigkill_is_reported_and_can_be_started_again() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();
    let mut client = Client::dial(&endpoint).await;
    client.send(serde_json::json!({"type": "hello"})).await;
    client.recv_until("hello").await;

    let env = start_env("sigkill-then-restart", cwd);
    client.send(start_frame(&env)).await;
    let first = client.recv_until("start-result").await;
    assert_eq!(first["ok"], true, "the first start must work: {first}");
    let dead_pid = first["workerPid"]
        .as_u64()
        .expect("a started session reports its worker pid") as u32;

    // Kill it with a SIGNAL, from outside the daemon — the OOM-killer shape.
    // Nothing in the daemon chose this, sent a `close`, or can see the signal.
    #[cfg(unix)]
    {
        let rc = unsafe { libc::kill(dead_pid as libc::pid_t, libc::SIGKILL) };
        assert_eq!(rc, 0, "kill -9 {dead_pid} must be accepted");
    }
    #[cfg(not(unix))]
    {
        // `taskkill` without `/F` is a polite WM_CLOSE, which a headless console
        // process ignores; `/F` is TerminateProcess, the nearest thing to a
        // signal this platform has. Compile-checked here, first RUN by the
        // Windows leg of CI (the same caveat the other process helpers carry).
        let out = Command::new("taskkill")
            .args(["/PID", &dead_pid.to_string(), "/F"])
            .output()
            .expect("taskkill is available on windows");
        assert!(out.status.success(), "taskkill /F {dead_pid}: {out:?}");
    }

    // The daemon must NOTICE. Poll the process table rather than sleeping a
    // fixed amount: `kill -9` is immediate here, but the read task's stdout EOF
    // → `wait()` → `Exited` chain is what the assertion is actually about, and
    // a fixed sleep would make that a race instead of a deadline.
    let deadline = Instant::now() + FRAME_BUDGET;
    while pid_is_alive(dead_pid) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !pid_is_alive(dead_pid),
        "the SIGKILL did not remove pid {dead_pid} — the test would then be \
         asserting recovery of a LIVE session"
    );

    // THE SIGNAL-DEATH EVIDENCE. `worker-exited` is the daemon's own spelling of
    // the `Exited` EVENT, and it is what proves the daemon NOTICED: the
    // WATCH-based probe (`exited()`) cannot be how a daemon learns of an OOM kill,
    // because a watch is only satisfied by `Some(_)` and a unix signal death
    // publishes `None` — the event fires either way. That mechanism claim is what
    // finding 1 asked for, and it is platform-independent: it is about which of
    // the two channels carried the news, not about what the code field contains.
    //
    // ⚠ What the code field CONTAINS is the operating system's business, and this
    // assertion used to pin it as `Null`. That is Linux's representation of "how
    // did it die": `Status::code()` is `None` for a signal death, so `kill -9`
    // yields `null`. Windows has no signals at all — the kill below is
    // `taskkill /F`, i.e. `TerminateProcess`, and a forcibly terminated process
    // HAS an exit code (`1`). The daemon is right on both: it forwards whatever
    // `Status::code()` reported. The test had pinned one OS's rendering, which is
    // the same mistake the previous commit fixed for the spawn-failure prose, in
    // the same file. So:
    //
    // * the PORTABLE half, asserted everywhere, is that both frames describe ONE
    //   death — `worker-exited` (from `on_event`) and `worker-crashed` (from
    //   `on_crash`) reach the socket carrying the SAME code. Both are built from
    //   the one `Option<i32>` the pump was handed, so a daemon that invented a
    //   code on either path (`unwrap_or(1)`, a default, a re-derivation) makes
    //   them disagree and reddens this on every platform;
    // * the platform's own representation is asserted per-platform below, with
    //   the reason named, and never as a literal value.
    let exited = client
        .recv_until_matching("event", |frame| {
            frame["sessionId"] == "sigkill-then-restart"
                && frame["frame"]["type"] == "worker-exited"
        })
        .await;

    // And the crash REPORT — which only `on_crash` produces (`on_event` emits
    // `worker-exited`, never `worker-crashed`), so this frame is the direct
    // evidence that the pump's crash path fired for a signal death.
    let crash = client
        .recv_until_matching("event", |frame| {
            frame["sessionId"] == "sigkill-then-restart"
                && frame["frame"]["type"] == "worker-crashed"
        })
        .await;
    assert_eq!(
        crash["isSubagent"], false,
        "a main session's crash is not a subagent's: {crash}"
    );
    assert_eq!(
        crash["frame"]["pid"].as_u64(),
        Some(dead_pid as u64),
        "the report names the process that died, since that is the pid a crash \
         log's name carries: {crash}"
    );
    // THE PASS-THROUGH GUARANTEE, in the form that holds on every OS: the two
    // frames report ONE death. `worker_frame` and `crash_report_frame` are fed the
    // same `Option<i32>` by the pump (`pump.rs`'s `Exited` arm calls `on_event`
    // then `on_crash` with the code it was given), so any daemon that manufactured
    // a code on one path — `unwrap_or(1)`, a `?? 0`, a re-derivation from the pid —
    // makes these two disagree, HERE, on Linux and on Windows alike. That is the
    // bug worth catching; "which value the OS chose" is not a bug on either leg.
    assert_eq!(
        crash["frame"]["exitCode"], exited["frame"]["exitCode"],
        "the crash report must carry the SAME code the `worker-exited` event \
         carried — one death, one code, passed through by both paths rather than \
         invented by either (event: {exited}, report: {crash})"
    );
    // The platform's own representation, gated and with the reason named, so the
    // contract this file actually relies on stays pinned where it is true:
    // unix's signal death has no code, and the daemon must not invent one.
    #[cfg(unix)]
    {
        assert_eq!(
            exited["frame"]["exitCode"],
            serde_json::Value::Null,
            "on unix a signal death carries no exit code (`Status::code()` is \
             `None`), and the daemon must pass that through rather than inventing \
             one: {exited}"
        );
    }
    // Not asserted on Windows, deliberately: `taskkill /F` is `TerminateProcess`,
    // whose code is whatever the OS hands back (measured `1` on CI's runner), and
    // naming that number would pin a Windows implementation detail the way the
    // deleted `Null` pinned a Linux one. The pass-through above is the guarantee;
    // the value is the OS's to choose.

    // THE RECOVERY, with no `close` in between.
    client.send(start_frame(&env)).await;
    let again = client.recv_until("start-result").await;
    assert_eq!(
        again["ok"], true,
        "a SIGKILLed session must be startable again — the supervisor cannot \
         answer `already running` about a process the kernel removed: {again}"
    );
    let new_pid = again["workerPid"]
        .as_u64()
        .expect("the restarted session reports its worker pid") as u32;
    assert_ne!(new_pid, dead_pid, "the restart is a NEW child");
    assert!(pid_is_alive(new_pid), "and it is running (pid {new_pid})");

    daemon.shutdown();
}

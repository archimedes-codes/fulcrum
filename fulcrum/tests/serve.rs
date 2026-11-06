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

use fulcrum::transport::{connect, Connection, ConnectionWriter, Endpoint};
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

// ── The `ready`-gate budgets for the slow-start relay test ───────────────────
//
// One test reads these:
// `a_slow_start_does_not_stall_that_clients_own_event_relay`. It holds a worker
// fixture's `ready` frame (see `src/fixtures/worker.rs`'s
// `hold_ready_until_released`) so the daemon's reader is PARKED inside
// `WorkerManager::attach` for a span the test chooses, and it takes its verdict on
// the socket while that hold still stands.
//
// The three real budgets below are DERIVED from the engine's own `ready` ceiling,
// not chosen freely, and the const assert at the end of this block is what makes
// that derivation hold rather than merely documented.

/// The ceiling the test's park span must clear: the engine's OWN `ready` bound
/// inside `attach` — `handle.wait_ready(Duration::from_secs(5))`, at
/// `src/worker/manager/mod.rs` (in `WorkerManager::attach`).
///
/// # Why this const exists, and why the budgets must sum below it
///
/// The whole proof rests on the daemon's reader staying PARKED inside `attach`
/// until the test has taken its verdict, and `wait_ready` implements that park. It
/// is BOUNDED: at this duration it gives up, `handle_frame` returns an error
/// instead of a `start-result`, and a single-task `serve_client` would then RESUME
/// its `select!` — whose drain arm pours the frames it buffered onto the socket in
/// one go. A probe running after that point sees a chunk arrive and reports that
/// the relay works, ON CODE THAT HAS THE BUG. Exceeding this ceiling therefore does
/// not produce a red, it produces the exact false green this test exists to rule
/// out — which is why the sum of the three budgets is asserted at compile time
/// rather than trusted to the reader of a comment.
const ATTACH_READY_CEILING: Duration = Duration::from_secs(5);

/// Mirror of the fixture's `__long_stream__` cadence (`LONG_STREAM_GAP` in
/// `src/fixtures/worker.rs`).
///
/// A mirror and not an import: the fixture's constants are private to
/// `fulcrum-core`'s fixture module, which a `tests/` crate of the OTHER package
/// cannot name. If the fixture's cadence ever changes, this number and
/// [`CHUNK_SILENCE`] have to be re-derived from it — the slow-start test would fail
/// LOUDLY (its silence detector would fire while the stream is still flowing), not
/// silently weaken, so the drift cost is a red here rather than a wrong answer.
const LONG_STREAM_GAP_MS: u64 = 100;

/// How long no chunk has to arrive for to count as "the stream went quiet" — three
/// cadences, so one late chunk on a loaded runner is not read as the end of the
/// stream.
const CHUNK_SILENCE: Duration = Duration::from_millis(3 * LONG_STREAM_GAP_MS);

/// How long the slow-start test waits for a held fixture to write its arrival
/// stamp.
///
/// The stamp is written by the FIXTURE at its own startup, in milliseconds, so this
/// budget is a broken-fixture PANIC timer and not an expected wait — which is why it
/// can be generous inside the ceiling without touching the proof. If the stamp never
/// appears within it then the gate is not working, and the test panics rather than
/// falling through to a verdict: a missing stamp is a broken instrument, not a red
/// on the code under test.
const READY_GATE_STAMP_BUDGET: Duration = Duration::from_millis(1500);

/// The FORWARD bound on a session-A chunk arriving while session B's reader is
/// provably parked: ~100 ms cadence, so this is ~15× the real latency.
///
/// It is a FORWARD bound on the good outcome and NOT a window the bad design races:
/// once the absorb step has drained the pre-park residue, the single-task design has
/// no writer at all during the held park, so its silence there does not depend on
/// this number.
const RELAY_ARRIVAL_BUDGET: Duration = Duration::from_millis(1500);

/// Upper bound on the absorb step (drain the pre-park residue until quiet).
/// ≈ 7 cadences: the residue is a FINITE set of bytes already written to the socket
/// before the park, so it drains in milliseconds; the bound is a ceiling for a
/// loaded CI runner, not a margin the old design can win.
const ABSORB_BUDGET: Duration = Duration::from_millis(700);

/// The proof's hard invariant, checked at COMPILE TIME (the same class of guard as
/// `serve.rs`'s `CLIENT_QUEUE_CAPACITY > 0` / `< FAN_OUT_CAPACITY` asserts):
///
/// ```text
/// READY_GATE_STAMP_BUDGET + ABSORB_BUDGET + RELAY_ARRIVAL_BUDGET < ATTACH_READY_CEILING
///      1500 ms            +    700 ms      +      1500 ms         <      5 000 ms
/// ```
///
/// 3.7 s against a 5 s ceiling, ~1.3 s of headroom. WHY IT MUST HOLD: all three
/// steps run while the reader is parked, and if their sum reached the ceiling then
/// `wait_ready`'s own timeout would fire FIRST — the reader unparks, the buffered
/// frames are drained onto the socket, and the probe measures the daemon's backlog
/// flush instead of its relay. That is a manufactured green on the buggy design, so
/// the inequality is a compile error rather than a convention: a one-number retune
/// of any budget can no longer silently outgrow the engine's bound.
///
/// The spelling matters: `Duration`'s `Add` is NOT a const trait on this toolchain,
/// so `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` does not compile in const
/// context. `as_millis()` IS const, so the millisecond-sum form is the form that
/// evaluates here.
const _: () = assert!(
    READY_GATE_STAMP_BUDGET.as_millis()
        + ABSORB_BUDGET.as_millis()
        + RELAY_ARRIVAL_BUDGET.as_millis()
        < ATTACH_READY_CEILING.as_millis(),
    "READY_GATE_STAMP_BUDGET + ABSORB_BUDGET + RELAY_ARRIVAL_BUDGET must stay below \
     ATTACH_READY_CEILING: that ceiling is `attach`'s own wait_ready(5s) \
     (src/worker/manager/mod.rs), and a park span longer than it would let that \
     timeout unpark the reader mid-verdict — the single-task design would then drain \
     its buffered frames onto the socket and the slow-start test would pass on code \
     that has the bug"
);

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
    /// The `ready`-gate marker this daemon's workers were told to watch, when the
    /// test asked for a gated daemon (see [`Daemon::start_gated`]).
    gate_marker: Option<PathBuf>,
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
        Self::start_inner(None).await
    }

    /// [`Daemon::start`], but with the fixture's `ready` gate POINTED AT — a
    /// marker path inside this daemon's own temp dir that does not exist yet
    /// (T4b's slow-start test).
    ///
    /// # Why the marker is a path handed back rather than a file created here
    ///
    /// The gate's polarity is PRESENT = hold (`src/fixtures/worker.rs`'s
    /// `hold_ready_until_released`), so the daemon must BOOT with the marker
    /// ABSENT — otherwise the very first worker it spawns would be held and the
    /// test could never start its first session at all. This therefore only tells
    /// the daemon where to look; the test creates the marker (arm) and removes it
    /// (release), via [`Daemon::arm_gate`] / [`Daemon::disarm_gate`].
    ///
    /// Inside the temp dir is the point: the marker is cleaned up with the daemon
    /// and no other test — running in parallel, in this process or another — can
    /// name it. A marker some other test left behind would hold THIS daemon's
    /// workers' `ready` hostage, which is the cross-test coupling the fixture's
    /// polarity note is written against.
    ///
    /// # Why the variable reaches the worker at all
    ///
    /// The test cannot reach a worker process directly: the daemon spawns its own
    /// workers, and the only control surface into a process the test never spawned
    /// is its inherited environment plus the filesystem. The `env` set below
    /// reaches the fixture by plain fork-time inheritance — nothing on either spawn
    /// step clears or rewrites the env (`Daemon`'s own spawn has no `env_clear`, and
    /// the engine's `build_command_with` applies only the AppImage scrub, which is
    /// EMPTY under `cargo test`; see `src/worker/client.rs`).
    async fn start_gated() -> Self {
        // The extension is load-bearing: the fixture writes its arrival stamp to
        // `marker.with_extension("arrived")`, so the marker must carry one (`.hold`)
        // for the stamp to be a DIFFERENT file rather than an overwrite of itself.
        Self::start_inner(Some("ready-gate.hold")).await
    }

    async fn start_inner(gate_marker: Option<&str>) -> Self {
        let dir = tempfile::tempdir().expect("temp dir for the daemon");
        let endpoint = Self::endpoint_in(dir.path());
        let db_path = dir.path().join("fulcrum.db");
        let worker = worker_fixture();
        let marker_path = gate_marker.map(|name| dir.path().join(name));

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
        if let Some(marker) = &marker_path {
            // Inside this daemon's own temp dir, so it is cleaned up with the
            // daemon and cannot be named by another test running in parallel.
            cmd.env("FULCRUM_FIXTURE_READY_GATE", marker);
        }
        let child = cmd.spawn().expect("spawn the daemon");

        let daemon = Self {
            child,
            db_path,
            _dir: dir,
            gate_marker: marker_path,
        };
        daemon.wait_until_listening(&endpoint).await;
        daemon
    }

    /// Arm the `ready` gate: create the marker, so every worker spawned from now
    /// on holds its `ready` frame until [`Daemon::disarm_gate`].
    fn arm_gate(&self) {
        let marker = self.gate_marker.as_ref().expect("a gated daemon");
        std::fs::write(marker, b"hold").unwrap_or_else(|e| panic!("create {marker:?}: {e}"));
    }

    /// Release the `ready` gate: remove the marker, so a held fixture emits `ready`
    /// on its next 10 ms poll tick and the parked `attach` completes.
    ///
    /// Idempotent on purpose: a test that has taken its verdict MUST be able to
    /// release, and a release that panicked would leave a worker parked until
    /// `wait_ready`'s own ceiling and turn a verdict into a teardown timeout.
    fn disarm_gate(&self) {
        let Some(marker) = self.gate_marker.as_ref() else {
            return;
        };
        match std::fs::remove_file(marker) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("remove {marker:?}: {e}"),
        }
    }

    /// The fixture's arrival stamp for the current hold (see the fixture's
    /// `hold_ready_until_released`): the proof that a worker process really REACHED
    /// the gate and is holding, rather than an inference from having armed it.
    fn gate_arrived(&self) -> PathBuf {
        self.gate_marker
            .as_ref()
            .expect("a gated daemon")
            .with_extension("arrived")
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

/// `ERROR_PIPE_BUSY` (231) — "every instance of this pipe name is connected".
///
/// **Re-hardcoded, deliberately.** The transport already owns this predicate:
/// `transport::windows::is_pipe_busy` (`fulcrum/src/transport/windows.rs`, the
/// `fn` next to its `raw_os_error() == Some(231)` body, reached from `connect`
/// and from `is_pipe_busy_err`). It is a PRIVATE `fn` — the module is `pub`, the
/// function is not — so a test target cannot name it, and the task forbids
/// widening a `src/` API for a test's convenience. So the constant lives twice,
/// and the cost of that drift is checked by the Windows leg of CI rather than by
/// the compiler: if the transport's code ever changed, this arm would retry the
/// wrong error and the malformed-line test would go red again with a 231 in its
/// output, which is a loud failure and not a silent one.
///
/// `231` and not a `windows_sys` constant, for the reason the transport gives
/// for the same choice: a literal is cheaper than a dependency.
#[cfg(windows)]
fn is_pipe_busy(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(231)
}

/// How long [`raw_dial`] waits out a busy pipe before calling it a failure.
///
/// The transport's `connect` retries this error unbounded; a test may not — a
/// wait with no ceiling is the hung-test failure mode this file's deadline rule
/// exists to prevent, and it would also be the "retry until it passes" lie in
/// its slowest form. 2 s matches the transport's own `BIND_BUSY_BUDGET` for the
/// same code, and it is generous by three orders of magnitude for what it is
/// actually waiting for: an armed instance is refilled the moment `accept`
/// swaps it, so the window this clears is a single `CreateFileW` + handoff, not
/// a queue drain. A dial that exhausts this budget is not transient — it means
/// no instance ever became free, and the panic says so rather than looping.
#[cfg(windows)]
const PIPE_BUSY_BUDGET: Duration = Duration::from_secs(2);

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
///
/// # Why only the Windows arm retries
///
/// `raw_dial` is a second dial path that bypasses the transport, and with it
/// the transport's `ERROR_PIPE_BUSY` retry — the Windows arm below explains the
/// failure that produced. The unix arm needs no twin and has deliberately not
/// been given one: `AF_UNIX` `connect` has no "every instance is busy" answer.
/// Its failure shapes are `ENOENT` (the socket file is not there yet — already
/// retried, by `Daemon::wait_until_listening`) and `ECONNREFUSED` (a stale
/// socket file with nothing listening behind it) — and both of those mean the
/// dial should FAIL, loudly, because there is no daemon worth waiting for.
/// Retrying them here would turn "nothing is listening" into a two-second stall
/// ending in a vaguer message, and would paper over exactly the class of bug
/// the `ENOENT` note above exists to surface. The asymmetry is the OS's, not an
/// inconsistency in this file.
async fn raw_dial(endpoint: &Endpoint) -> impl tokio::io::AsyncRead + tokio::io::AsyncWrite {
    #[cfg(unix)]
    {
        tokio::net::UnixStream::connect(socket_path(endpoint))
            .await
            .expect("dial raw (unix)")
    }
    #[cfg(windows)]
    {
        // `open` is synchronous in tokio's wrapper (a `CreateFileW`), and the
        // daemon is already listening by the time a test gets here —
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
        //
        // # The busy retry, and the line it must not cross
        //
        // Bypassing the transport also bypassed the one thing in it that makes
        // dialing a live daemon reliable: `transport::windows::connect` retries
        // `ERROR_PIPE_BUSY` (231) because "every instance of this name is
        // currently connected" is a momentary condition, not a refusal. The
        // first version of this arm had no retry and CI proved what that costs:
        // `a_malformed_line_gets_an_error_and_the_stream_continues` died with
        // `Os { code: 231, …"All pipe instances are busy." }` on both Windows
        // jobs, reproducibly, because it is the one test that dials raw WHILE
        // the daemon is already servicing another connection (it needs a live
        // daemon to hand garbage to, and holds a framed client open to prove
        // the stream survives) — so it is the one that can land in the
        // all-instances-busy window. The raw dial in
        // `a_client_that_never_reads_is_throttled…` has no competing client and
        // never landed there, and the tests that dial through the transport are
        // immune because it retries — which is why exactly one of 37 failed.
        //
        // Retry 231 and NOTHING ELSE. That distinction is the whole content of
        // this arm: a wrong pipe name answers `NotFound`/`ERROR_FILE_NOT_FOUND`,
        // and the previous round's bug was precisely a dial at a name that
        // resolved to nothing (the stripped prefix). That failure was CORRECT —
        // the fix was to dial the right name, not to wait longer for a pipe that
        // will never appear. A retry keyed on "any error", or on a plain
        // not-found, would have swallowed it and re-hidden that bug behind a
        // timeout, converting a red test that named the cause into a red test
        // that names only its own budget. So the guard below matches one error
        // code; every other error leaves the loop on the first turn and panics
        // with the OS's own rendering.
        //
        // And the loop is BOUNDED, unlike the transport's (whose unbounded retry
        // is right for a client that wants to reach a daemon a second daemon is
        // still binding, and wrong for a test, where an unbounded wait on a
        // genuinely occupied pipe is the hung-test failure mode this file's
        // deadline rule exists to prevent). See [`PIPE_BUSY_BUDGET`] for the
        // number and for what exceeding it means.
        let deadline = Instant::now() + PIPE_BUSY_BUDGET;
        let mut attempts = 0u32;
        let client = loop {
            attempts += 1;
            match tokio::net::windows::named_pipe::ClientOptions::new()
                .read(true)
                .write(true)
                .open(pipe_name(endpoint))
            {
                Ok(client) => break client,
                Err(e) if is_pipe_busy(&e) && Instant::now() < deadline => {
                    // Async sleep: unlike the transport's sync `bind`, this arm
                    // runs inside the test's runtime, so it can yield instead of
                    // parking a worker thread — the same thing
                    // `transport::windows::connect` does for the same code.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // Busy, and the budget is gone: no instance ever came free.
                Err(e) if is_pipe_busy(&e) => panic!(
                    "dial raw (windows) at {} still busy after {PIPE_BUSY_BUDGET:?} \
                     ({attempts} attempts): {e:?} — no pipe instance ever came free",
                    pipe_name(endpoint)
                ),
                // Everything else — `NotFound` above all — fails on the FIRST
                // attempt, with the OS's own error, no waiting.
                Err(e) => {
                    panic!("dial raw (windows) at {}: {e:?}", pipe_name(endpoint))
                }
            }
        };
        client
    }
}

/// A live client, with the frames it has been sent already drained into a queue
/// so a test can wait for one specific frame without losing the ones before it.
struct Client {
    conn: Connection,
    /// This client's half of the framing split (T4b).
    ///
    /// `dial` calls [`Connection::split_writer`] exactly ONCE, which is what lets
    /// every `client.send(..)` in this file keep working unchanged: the writer
    /// lives on the struct instead of being taken per call, because `split_writer`
    /// consumes the `Connection` and may only be called once. Holding both halves
    /// in ONE struct also means the whole socket closes when a test drops its
    /// `Client` — the same lifetime a single `Connection` used to have, which the
    /// disconnect tests depend on.
    writer: ConnectionWriter,
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
        // Split once, here, so the struct owns both halves (see the field). This
        // is the ONLY place in this file that reaches for a writer, and it is why
        // `send` below needs no `split_writer` of its own.
        let (conn, writer) = conn.split_writer();
        Self {
            conn,
            writer,
            pending: std::collections::VecDeque::new(),
            delivered: Vec::new(),
        }
    }

    async fn send(&mut self, frame: serde_json::Value) {
        // The error path is spelled out rather than a bare `expect`, because after
        // T4b a `send` can fail for a NEW reason that is not a test bug: the
        // daemon's writer task exits on its own (its `send_frame` errors on the
        // peer's close, or `serve_client` `abort()`s it once its reader half is
        // gone), and on the reader-first path the socket's final `Arc` drops with
        // that abort — so a test still mid-`send` sees `Err` where the old
        // single-task design, which dropped the whole `Connection` at once, simply
        // could not. That is the split's EOF semantics, and a message that only
        // said "write the frame" would send the next reader to the wrong file.
        self.writer.send_frame(&frame).await.unwrap_or_else(|e| {
            panic!(
                "the daemon accepted the write; it refused it instead ({e}). \
                     If the frame is well-formed, the connection is already going \
                     away: the daemon's writer task exits on the peer's close or on \
                     `serve_client`'s abort, so a `send` that races a teardown sees \
                     this error — check whether the daemon closed this connection \
                     (or was refused by the handshake / rate gate) before this frame"
            )
        });
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

/// Issue #3's acceptance criterion: a client that starts a session and streams
/// sees its OWN event frames arrive **while a second `start` is still in flight**.
///
/// # The bug this exists for
///
/// `serve_client` used to be ONE task running ONE `select!`, with `handle_frame`
/// awaited INLINE in the inbound arm. A `start` inside it awaits
/// `WorkerManager::attach` — the engine's 5 s `ready` bound — so for that window the
/// task was not in its `select!` at all: this connection's fan-out arm could not
/// run, and a client that started a session while its other session was streaming
/// saw its own events only after the start finished. No frame was lost; the delay
/// was the defect.
///
/// # Why the gate makes the old design red BY CONSTRUCTION, not by margin
///
/// A fixed fixture delay cannot prove this. `attach` returns on its timer and the
/// single task then drains everything it buffered in one burst, so any measurement
/// window placed by a clock guess is one the buggy design can win by being fast.
///
/// Instead the fixture's `ready` frame is HELD until this test releases it
/// (`src/fixtures/worker.rs`'s `hold_ready_until_released`, armed through
/// [`Daemon::start_gated`] / [`Daemon::arm_gate`]). So:
///
/// * session B's `start` parks the daemon's reader inside `attach`, and it CANNOT
///   return until this test removes the marker;
/// * the fixture writes an arrival stamp on entering the hold, so "the park
///   happened" is a fact read off the filesystem rather than an inference;
/// * the verdict is taken on the socket **while the marker still exists** — and in
///   the single-task design there is then nothing that can write this socket at all:
///   the one task is parked in `handle_frame`, its fan-out arm is unpolled, and
///   there is no second task. Silence there is structural, so a chunk arriving in
///   that span can only come from a relay that runs independently of the reader —
///   which is precisely what the reader/writer split delivers.
///
/// The budgets that bound this span are in the [`ATTACH_READY_CEILING`] block, and
/// the compile-time assert there is what keeps the span inside `attach`'s own
/// timeout — a hold that outlived it would let the timeout unpark the reader, the
/// backlog would drain, and the probe would measure a flush instead of a relay: a
/// false green.
///
/// # The two steps of the verdict, and why both are needed
///
/// 1. **Absorb.** The old design may have written a small RESIDUE before it parked —
///    the chunks its `select!` relayed between the marker being created and the
///    reader actually parking. The pre-`start` barrier below (consuming the first
///    chunk) bounds that to a couple of frames, so the absorb drains it to quiet in
///    milliseconds. Chunks STILL arriving at the end of [`ABSORB_BUDGET`] (~7
///    cadences of continuous production) cannot be residue: that is already the
///    good outcome.
/// 2. **Probe.** Once quiet, the socket is structurally silent for the old design for
///    as long as the marker holds, so ONE chunk arriving within
///    [`RELAY_ARRIVAL_BUDGET`] is conclusive.
///
/// # What the teardown half adds
///
/// After the release, B's `start-result` must still arrive (the park really ended
/// rather than timing out), and dropping this test's `Client` must tear the
/// connection down without wedging the daemon: a fresh dial gets its `hello` reply.
/// That is the integration-level home of "no task outlives `serve_client`" — the
/// writer half is `abort()`ed by `serve_client` rather than joined, because joining
/// a writer parked inside `send_frame` can never resolve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_start_does_not_stall_that_clients_own_event_relay() {
    // A gated daemon: the fixture's `ready` is held whenever the marker exists, and
    // the marker does NOT exist yet — session A must start normally.
    let mut daemon = Daemon::start_gated().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();
    let mut client = Client::dial(&endpoint).await;

    client.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(client.recv_until("hello").await["type"], "hello");

    // ── Session A: started, and streaming of its own accord. ─────────────────
    let parkable = "relay-parked";
    let streaming = "relay-streaming";
    client.send(start_frame(&start_env(streaming, cwd))).await;
    let started = client.recv_until("start-result").await;
    assert_eq!(
        started["ok"], true,
        "the streaming session starts: {started}"
    );

    client
        .send(serde_json::json!({
            "type": "prompt", "sessionId": streaming, "text": "__long_stream__",
        }))
        .await;
    let delivered = client.recv_until("prompt-result").await;
    assert_eq!(delivered["ok"], true, "the prompt is relayed: {delivered}");

    // THE BARRIER: consume the first streamed chunk. Without it, chunks that were
    // already in flight before the park could satisfy the probe and the whole test
    // would pass on the buggy design. Matching on a real `agent_message_chunk` (and
    // not merely "an event") is what makes this a claim about a TURN IN FLIGHT —
    // `start` itself answers with a `session_info` sink-frame.
    let first_chunk = client
        .recv_until_matching("event", agent_message_chunk_for(streaming))
        .await;
    assert_eq!(
        first_chunk["sessionId"], streaming,
        "the barrier chunk belongs to the streaming session: {first_chunk}"
    );

    // ── Arm the gate, then start B: its `attach` parks the reader. ───────────
    daemon.arm_gate();
    let armed = Instant::now();
    client.send(start_frame(&start_env(parkable, cwd))).await;

    // The park must be OBSERVED, not assumed: the fixture writes the stamp when it
    // enters the hold. A stamp that never arrives means the gate is broken, and that
    // is a broken instrument rather than a red on the daemon — so this panics rather
    // than falling through to a verdict.
    let stamp_deadline = Instant::now() + READY_GATE_STAMP_BUDGET;
    let stamp = daemon.gate_arrived();
    let stamp_at = loop {
        if stamp.exists() {
            break Instant::now();
        }
        if Instant::now() >= stamp_deadline {
            panic!(
                "the second worker never reached the `ready` gate (no {:?} within \
                 {READY_GATE_STAMP_BUDGET:?}) — the reader is therefore NOT provably \
                 parked and any verdict below would be meaningless. Is \
                 FULCRUM_FIXTURE_READY_GATE reaching the worker (see \
                 `Daemon::start_gated`)?",
                stamp
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let park_observed_at = stamp_at - armed;

    // The reader is parked now, and stays parked until THIS test releases it.
    // Everything between here and the release is inside that park.
    let is_streaming_chunk = agent_message_chunk_for(streaming);

    // ── Step 1: absorb the pre-park residue. ─────────────────────────────────
    let absorb_started = Instant::now();
    let absorb_deadline = absorb_started + ABSORB_BUDGET;
    let mut residue = 0usize;
    let mut quiet = false;
    loop {
        let left = absorb_deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break; // still producing at the ceiling → not residue, i.e. the relay
        }
        // Only a full `CHUNK_SILENCE` window of silence counts as quiet; a truncated
        // final window means the absorb ran out of budget, which is the other branch.
        let window = left.min(CHUNK_SILENCE);
        match client.try_recv(window).await {
            Some(frame) if is_streaming_chunk(&frame) => residue += 1,
            Some(_) => {} // a non-chunk frame is not the relay flowing
            None if window >= CHUNK_SILENCE => {
                quiet = true;
                break;
            }
            None => break,
        }
    }
    let absorb_elapsed = absorb_started.elapsed();

    // ── Step 2: the probe, in the held park. ─────────────────────────────────
    let mut probe_elapsed = Duration::ZERO;
    let mut relayed_while_parked = !quiet && residue > 0;
    if !relayed_while_parked {
        let probed = Instant::now();
        let probe_deadline = probed + RELAY_ARRIVAL_BUDGET;
        relayed_while_parked = loop {
            let left = probe_deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break false;
            }
            match client.try_recv(left).await {
                Some(frame) if is_streaming_chunk(&frame) => break true,
                // Any other frame is not the relay flowing; keep waiting out the
                // budget so a stall is reported as a stall.
                Some(_) => {}
                // `try_recv` says `None` for BOTH "the budget expired" and "the
                // stream ended". Either way no chunk arrived, and a torn-down stream
                // must not be spun on for the rest of the budget.
                None => break false,
            }
        };
        probe_elapsed = probed.elapsed();
    }

    // The verdict's precondition, asserted rather than trusted: the hold that makes
    // it conclusive must still be in force.
    assert!(
        daemon
            .gate_marker
            .as_ref()
            .expect("a gated daemon")
            .exists(),
        "the verdict must be taken while the `ready` gate still holds — the marker \
         vanished before the probe finished, so the reader may have unparked on \
         `wait_ready`'s timeout and drained its backlog (the false green \
         ATTACH_READY_CEILING exists to rule out)"
    );

    eprintln!(
        "slow-start relay: park observed after {park_observed_at:?}; absorb \
         {absorb_elapsed:?} saw {residue} chunk(s) and reached quiet = {quiet}; \
         probe waited {probe_elapsed:?} → relayed while parked = {relayed_while_parked}"
    );

    assert!(
        relayed_while_parked,
        "`{streaming}`'s chunks stopped arriving while `{parkable}`'s `start` was \
         parked in `attach` (park observed {park_observed_at:?} after the `start` \
         was written; absorb {absorb_elapsed:?} drained {residue} residue chunk(s) \
         to quiet = {quiet}; then the probe waited {probe_elapsed:?} for one chunk \
         and got none within {RELAY_ARRIVAL_BUDGET:?}). While the marker held, the \
         daemon's reader could not return, so this connection's relay must be run by \
         ANOTHER task — that is the reader/writer split. A single `serve_client` task \
         with the fan-out as one of its `select!` arms is silent here by construction."
    );

    // ── Release, and show the parked `start` really completes. ───────────────
    daemon.disarm_gate();
    let started_b = client
        .recv_until_matching("start-result", |frame| frame["sessionId"] == parkable)
        .await;
    assert_eq!(
        started_b["ok"], true,
        "releasing the gate must let the parked `attach` finish and answer: {started_b}"
    );

    // ── The teardown half: no task of this connection outlives it. ───────────
    //
    // The `Client` owns BOTH halves of its connection (the reader `conn` and the
    // writer `writer`), so dropping it closes the socket outright — the reader-first
    // path, the one where the daemon's writer may be parked inside `send_frame` and
    // where nothing but `serve_client`'s `abort()` ends it. A wedged writer would
    // not wedge the ACCEPT loop, so this assertion is the cheap smoke test for "the
    // daemon is still healthy"; the assertion that actually catches a leaked task is
    // the process census T1 runs on this branch over the whole suite.
    drop(client);
    daemon.wait_until_listening(&endpoint).await;
    let mut after = Client::dial(&endpoint).await;
    after.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(
        after.recv_until("hello").await["type"],
        "hello",
        "the daemon still answers a fresh client after the parked start and the \
         abrupt disconnect — the torn-down connection must leave nothing behind"
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
/// The garbage has to be written RAW — through a `ConnectionWriter::send_frame` it
/// would be serialized and therefore always valid — so this reaches past the framing
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

/// A connection that sends frames BEFORE saying `hello` is refused, not served
/// (issue #4 — the required handshake).
///
/// The `hello` frame already existed and `handle_frame` already answered it; what
/// was missing was that NOTHING REQUIRED IT. Any local process that could reach
/// the socket could go straight to `start` — the greeting was decoration, so a
/// stray connection was implicitly trusted to know the protocol. This test pins
/// the opposite: frames before `hello` get no service at all, and after
/// [`MAX_PRE_HELLO_FRAMES`] of them the peer is told once and hung up on.
///
/// # Why the frames are written RAW
///
/// Same reason as `a_malformed_line_gets_an_error_and_the_stream_continues`: this
/// peer must not be a well-behaved `Client`, and it must not read through a
/// `Connection` whose reader task would consume the very reply this test asserts
/// on. So: `raw_dial`, split, `write_all` the offending frames, read the raw lines
/// back.
///
/// # What is asserted, in order
///
/// 1. NO `start-result` ever arrives — a pre-`hello` `start` is DROPPED, not
///    answered. This is the load-bearing half: "reject rather than serve" is the
///    issue's wording, and a daemon that answered the pre-`hello` `start` and THEN
///    complained would pass an assertion written only on the error frame.
/// 2. Exactly ONE `error` frame arrives, and it names the handshake requirement.
///    One, not three: the peer is told once and closed, not lectured per frame.
/// 3. The stream then EOFs — the daemon closed the connection rather than leaving
///    a hostile peer's socket open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_that_sends_frames_before_hello_is_rejected_not_served() {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();

    let (read_half, mut write_half) = tokio::io::split(raw_dial(&endpoint).await);
    // Three `start` frames and not one `hello`. Written in one write so they are
    // three queued frames the moment the daemon's reader gets to them — the
    // pipelined shape, which is the shape a real client that skipped the greeting
    // would actually produce.
    let mut flood = String::new();
    for _ in 0..3 {
        flood.push_str(&serde_json::to_string(&start_frame(&start_env("pre-hello", cwd))).unwrap());
        flood.push('\n');
    }
    write_half
        .write_all(flood.as_bytes())
        .await
        .expect("write three pre-hello frames");
    write_half.flush().await.expect("flush");

    // Read whatever the daemon sends, until it hangs up or the budget expires.
    let mut reader = BufReader::new(read_half);
    let mut lines: Vec<String> = Vec::new();
    let saw_eof = {
        let deadline = Instant::now() + FRAME_BUDGET;
        let mut hit_eof = false;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let mut line = String::new();
            let read = tokio::time::timeout(left, reader.read_line(&mut line))
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "the daemon neither refused the pre-hello peer nor closed it \
                         within {FRAME_BUDGET:?} (frames so far: {lines:?}) — a peer \
                         that never greets must be told once and hung up on, not \
                         held open"
                    )
                })
                .expect("read a reply line");
            if read == 0 {
                hit_eof = true;
                break;
            }
            lines.push(line);
        }
        hit_eof
    };

    let frames: Vec<serde_json::Value> = lines
        .iter()
        .map(|l| {
            serde_json::from_str(l.trim())
                .unwrap_or_else(|e| panic!("the reply line is not a JSON frame: {l:?} ({e})"))
        })
        .collect();

    // (1) Not one pre-`hello` frame was SERVED. A `start-result` here would mean
    //     the daemon spawned a child for a peer it is refusing.
    assert!(
        frames.iter().all(|f| f["type"] != "start-result"),
        "a `start` sent before `hello` must be dropped, not answered: {frames:?}"
    );
    // (2) One refusal, naming the requirement.
    let refusals: Vec<&serde_json::Value> =
        frames.iter().filter(|f| f["type"] == "error").collect();
    assert_eq!(
        refusals.len(),
        1,
        "the pre-hello peer must be told ONCE and then closed; got {frames:?}"
    );
    let message = refusals[0]["message"]
        .as_str()
        .unwrap_or_else(|| panic!("the refusal carries a message: {}", refusals[0]));
    assert!(
        message.contains("hello"),
        "the refusal must name what the peer failed to send, so a client with a \
         wrong first frame knows what to fix; got: {message}"
    );
    // (3) And the connection is closed.
    assert!(
        saw_eof,
        "the daemon must CLOSE a peer it refused for skipping the handshake, not \
         leave its socket open; frames seen: {frames:?}"
    );

    // The daemon is unharmed for a client that does greet — the same "one bad peer
    // costs nothing" claim every other hardening test in this file makes.
    let mut later = Client::dial(&endpoint).await;
    later.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(
        later.recv_until("hello").await["type"],
        "hello",
        "a refused pre-hello peer must not take the daemon down with it"
    );

    daemon.shutdown();
}

/// A peer that floods NON-GREETING frames is refused and closed, and the daemon
/// stays healthy for the next client (issue #4 — the inbound frame-rate bound).
///
/// # Why the flood is `prompt` and not `hello`
///
/// The existing flood test,
/// `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat`, floods
/// `hello` and pins the numbers the inbound QUEUE produces (blocked write, RSS
/// flat). This gate is a different bound on a different axis — the RATE at which
/// decoded frames are served — and it deliberately counts non-greeting frames
/// only, because `start`/`prompt` cost the daemon a syscall each while `hello`
/// costs one `json!`. So this test floods `prompt` and that one keeps flooding
/// `hello`; neither is a stand-in for the other.
///
/// `prompt` for a session that does not exist is answered (`prompt-result` with
/// `ok: false`) and needs no worker, no fixture, no database row — which makes
/// this a test of the gate rather than of session machinery.
///
/// # Why the writes are INTERLEAVED with the reads
///
/// The first version wrote a 5-second flood without reading, and the assertion
/// then died on `ECONNRESET` instead of seeing the refusal: when a socket is
/// closed with data still unread in the receive buffer, Linux sends RST, and RST
/// discards what the daemon had already written. That is the kernel's behaviour,
/// not a daemon bug — but it makes the refusal frame unobservable, and the
/// refusal is the contract being pinned.
///
/// So the writer stops as soon as the refusal appears, and writes in small chunks
/// between reads. The daemon's reader keeps up, the buffer is empty at close, and
/// what the test observes is the daemon's own answer rather than a kernel artefact.
///
/// # What is asserted, and what is deliberately NOT
///
/// Asserted: ONE `error` frame naming the frame-rate limit, then the connection
/// ends (clean EOF or a reset — both mean "the daemon closed it"); and a
/// subsequent healthy client gets the full hello → start → prompt exchange.
///
/// NOT asserted: any frame count, and no RSS number. The bucket drains at
/// [`FRAME_RATE`] tokens/s after a `FRAME_RATE_BURST` burst, so "how many
/// `prompt-result` frames escaped before the refusal" is a schedule on a loaded CI
/// box. A test that pinned it would be a flake that reads like a regression. The
/// BOUNDARY — told once, then closed — is schedule-independent, and that is the
/// contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flood_of_non_greeting_frames_is_refused_and_the_daemon_survives() {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());

    let (read_half, mut write_half) = tokio::io::split(raw_dial(&endpoint).await);
    // Greet first, so the refusal this test waits for can ONLY be the rate gate.
    // Without the `hello` the same stream would be refused by the handshake gate
    // after three frames and this would be testing the wrong bound.
    write_half
        .write_all(b"{\"type\":\"hello\"}\n")
        .await
        .expect("write the greeting");

    // `prompt` for an id nothing is running: answered cheaply, no child, no row.
    const FRAME: &[u8] = b"{\"type\":\"prompt\",\"sessionId\":\"nobody\",\"text\":\"x\"}\n";
    // Small enough that the daemon's reader has drained the socket by the time the
    // gate fires — see "Why the writes are INTERLEAVED with the reads".
    const CHUNK: usize = 32;

    let mut reader = BufReader::new(read_half);
    let mut refusal: Option<serde_json::Value> = None;
    // How the connection ended, if it did: `true` for a clean EOF, `false` for a
    // reset, and neither is a failure — see the read arm below.
    let mut closed = false;
    let mut written = 0usize;
    let deadline = Instant::now() + FRAME_BUDGET;

    'flood: while Instant::now() < deadline {
        // Write a burst-sized chunk. A blocked write means the socket is full,
        // which is the OTHER bound (the inbound queue) doing its job; stop and
        // read, because the gate's answer arrives on the read half either way.
        let mut chunk = Vec::with_capacity(CHUNK * FRAME.len());
        for _ in 0..CHUNK {
            chunk.extend_from_slice(FRAME);
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        // A blocked or failed write is not this loop's business: the first means
        // the socket is full (the inbound queue's bound doing its job) and the
        // second means the daemon closed the connection. Either way the answer this
        // test asserts on arrives on the READ half, which the loop turns to next.
        if let Ok(Ok(())) = tokio::time::timeout(left, write_half.write_all(&chunk)).await {
            written += CHUNK;
        }

        // Then read whatever the daemon has to say about it, with a short budget:
        // this is a POLL, not a wait. The refusal only appears after the burst is
        // spent, so most turns of this loop see `prompt-result` frames.
        loop {
            let left = Duration::from_millis(20);
            let mut line = String::new();
            match tokio::time::timeout(left, reader.read_line(&mut line)).await {
                Err(_) => continue 'flood, // nothing yet; go flood some more
                Ok(Ok(0)) => {
                    closed = true;
                    break 'flood;
                }
                Ok(Ok(_)) => {
                    let frame: serde_json::Value = serde_json::from_str(line.trim())
                        .unwrap_or_else(|e| {
                            panic!("the reply line is not a JSON frame: {line:?} ({e})")
                        });
                    if frame["type"] == "error" {
                        assert!(
                            refusal.is_none(),
                            "the peer was refused twice ({refusal:?} and {frame}) — \
                             the contract is told ONCE and then closed"
                        );
                        refusal = Some(frame);
                    }
                }
                // A reset is the daemon closing a socket that still held unread
                // bytes; Linux turns that into RST. It IS the close this contract
                // requires, so it ends the loop rather than failing the test — but
                // it is only acceptable AFTER the refusal, because a reset instead
                // of a refusal would mean the daemon hung up without saying why.
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                    closed = true;
                    break 'flood;
                }
                Ok(Err(e)) => panic!("the stream failed while reading the refusal: {e}"),
            }
        }
    }

    assert!(
        written > 0,
        "not one frame reached the daemon, so this test would certify nothing about \
         the rate gate"
    );
    let refusal = refusal.unwrap_or_else(|| {
        panic!(
            "a peer that sent {written} non-greeting frames was never refused by the \
             rate gate (no `error` frame within {FRAME_BUDGET:?}, connection closed \
             {closed}) — the daemon is still serving a flood at whatever rate it \
             arrives, which is the unbounded-cost case issue #4 is about"
        )
    });
    let message = refusal["message"]
        .as_str()
        .unwrap_or_else(|| panic!("the refusal carries a message: {refusal}"));
    assert!(
        message.contains("rate"),
        "the refusal must name the limit it hit, so a merely badly-written client \
         can tell this from the handshake refusal; got: {message}"
    );
    assert!(
        closed,
        "the daemon told the flooding peer and then left its socket open — the \
         refusal must be followed by a close, or the peer keeps a daemon-side \
         connection for free"
    );

    // And the daemon is unharmed: a healthy client gets the full exchange.
    let mut healthy = Client::dial(&endpoint).await;
    healthy.send(serde_json::json!({"type": "hello"})).await;
    assert_eq!(healthy.recv_until("hello").await["type"], "hello");
    let cwd = daemon._dir.path().to_str().unwrap();
    healthy
        .send(start_frame(&start_env("after-flood", cwd)))
        .await;
    assert_eq!(
        healthy.recv_until("start-result").await["ok"],
        true,
        "a rate-limit flood must not cost the daemon its next session"
    );

    daemon.shutdown();
}

/// Concurrent sessions are bounded, and the bound RELEASES on `close` (issue #4).
///
/// # The hole
///
/// Every local process that can reach the socket can `start`, and `start` spawns a
/// child process. Before this bound the only ceiling on how many children one
/// daemon would run was the process table — so a buggy client in a retry loop, or
/// a hostile local one, could take the machine's ability to fork away from
/// everything else running as this user. The outbound and inbound buffers were
/// bounded long ago (see `CLIENT_QUEUE_CAPACITY` and `frame::MAX_QUEUED_INBOUND_FRAMES`
/// for those measurements); the session COUNT was not.
///
/// # Why the test loops to the bound instead of hardcoding it
///
/// The limit is a private const in `serve.rs` (`SESSION_LIMIT`), and the task
/// forbids widening `Daemon::new` or adding a test-only setter to make it
/// injectable. So the test discovers the number the only honest way: start
/// sessions until one is refused. That makes the test outlive a change to the
/// constant, which a hardcoded `for i in 0..32` would not — a bumped limit would
/// turn a hardcoded version green-while-testing-nothing, the failure shape this
/// file's header exists to refuse.
///
/// # The three claims, in order
///
/// 1. The refusal happens, and its message names the bound and says what to do
///    about it (`close` a session).
/// 2. `close` RELEASES a slot: after closing one session, a NEW id starts fine.
///    Without this half, a counter that never decrements would pass claim 1
///    forever and quietly brick the daemon for its whole uptime.
/// 3. Nothing leaked: after closing every session that was opened, another `start`
///    still succeeds. This is the counter-faithfulness pin — the same class of
///    leak `Daemon::dead` used to have, and the reason `close`'s doc calls the
///    release-everything rule an invariant.
///
/// Every session this opens is CLOSED before it returns: the worker children are
/// real processes, and the post-suite stray-process census fails the run for one
/// that outlives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_session_limit_refuses_one_past_the_bound_and_releases_on_close() {
    let mut daemon = Daemon::start().await;
    let endpoint = Daemon::endpoint_in(daemon._dir.path());
    let cwd = daemon._dir.path().to_str().unwrap();

    let mut client = Client::dial(&endpoint).await;
    client.send(serde_json::json!({"type": "hello"})).await;
    client.recv_until("hello").await;

    // Start sessions until one is refused, remembering every id that got in.
    let mut started: Vec<String> = Vec::new();
    let refusal = loop {
        let id = format!("bounded-{}", started.len());
        client.send(start_frame(&start_env(&id, cwd))).await;
        let result = client.recv_until("start-result").await;
        if result["ok"] == true {
            started.push(id);
            // A ceiling on the loop, so a daemon with NO bound fails this test
            // instead of filling the process table and taking the runner with it.
            assert!(
                started.len() <= 256,
                "the daemon accepted {} sessions without refusing any — there is no \
                 session bound, which is the hole issue #4 exists to close",
                started.len()
            );
        } else {
            break result;
        }
    };

    assert!(
        !started.is_empty(),
        "the daemon refused its FIRST session, so it is not bounding the count, it \
         is refusing everything: {refusal}"
    );
    // The bound is `serve.rs`'s private `SESSION_LIMIT`; the test discovers it
    // rather than duplicating it (see the header note), but the NUMBER is part of
    // the issue's acceptance criteria, so a change to it must be a deliberate one
    // that lands here too.
    assert_eq!(
        started.len(),
        32,
        "the daemon accepted {} sessions before refusing — the bound is documented \
         as 32 in `serve.rs` (`SESSION_LIMIT`); if that constant moved, move this \
         number and the doc in the same change",
        started.len()
    );
    let message = refusal["error"]
        .as_str()
        .unwrap_or_else(|| panic!("the refused `start` says why: {refusal}"));
    assert!(
        message.contains("session limit"),
        "the refusal must name the bound that was hit (not a generic failure) so a \
         client can tell this from a broken worker exe; got: {message}"
    );

    // (2) `close` releases a slot — TWO slots, by closing two sessions, so claim
    // (3) below can spend both and the arithmetic is exact. One `close` frees one
    // slot; the first draft of this test closed one and started two, and the
    // failure it produced was the test's bug, not the daemon's.
    for _ in 0..2 {
        let released = started
            .pop()
            .expect("at least two sessions were started above");
        client
            .send(serde_json::json!({"type": "close", "sessionId": released}))
            .await;
        let closed = client.recv_until("closed").await;
        assert_eq!(
            closed["ok"], true,
            "the session that was started must be closable: {closed}"
        );
    }
    client
        .send(start_frame(&start_env("after-release", cwd)))
        .await;
    let restarted = client.recv_until("start-result").await;
    assert_eq!(
        restarted["ok"], true,
        "closing one session must free one slot — a counter that only ever grows \
         would pass the refusal half of this test and then brick the daemon for its \
         whole uptime: {restarted} (limit message was {message:?})"
    );

    // (3) Nothing leaked: a SECOND round, spending the other freed slot. One round
    //     is not enough to tell a faithful counter from one that releases only
    //     once.
    client
        .send(start_frame(&start_env("second-after-release", cwd)))
        .await;
    assert_eq!(
        client.recv_until("start-result").await["ok"],
        true,
        "a second start past the bound (after two closes) must also be accepted"
    );

    for id in started {
        client
            .send(serde_json::json!({"type": "close", "sessionId": id}))
            .await;
        client.recv_until("closed").await;
    }

    // Below the bound again, so the census has nothing to find: every child this
    // test spawned is reaped by its `close`.
    let mut remaining = 0usize;
    for id in ["after-release", "second-after-release"] {
        client
            .send(serde_json::json!({"type": "close", "sessionId": id}))
            .await;
        let frame = client.recv_until("closed").await;
        if frame["ok"] == true {
            remaining += 1;
        }
    }
    assert_eq!(
        remaining, 2,
        "both sessions started after the release must still be running to be closed \
         (a `false` here means one died on its own and the count this test measured \
         was not the count it thought)"
    );

    daemon.shutdown();
}

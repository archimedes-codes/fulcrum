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

// ── reaping: why a child cannot survive its test ─────────────────────────────

// `fulcrum serve` has no shutdown frame in T9 and `serve::run` never returns, so
// a daemon that is not killed **runs for the life of the machine**. This file
// shipped with exactly that leak: `Spawned::detach()` set an `owned` flag to
// `false`, which switched `Drop`'s kill+wait off, and the one call site that
// used it carried the comment "Drop kills it" — while calling the method that
// disabled `Drop`. A day of green development runs left **34 live daemons** from
// this repo's `target/debug/`, some 5.5 hours old.
//
// Three things make that shape of mistake impossible rather than repaired at one
// call site:
//
// 1. **`detach()` is deleted.** `Spawned::drop` kills and reaps unconditionally,
//    and `Drop` runs on every exit path a test can take — return, `?`, and
//    panic/unwind (the workspace root sets `panic = "unwind"`, which
//    `fulcrum/Cargo.toml` documents as load-bearing for this member).
// 2. **[`no_opt_out_of_reaping_can_come_back_in_this_file`] fails if the method returns**, so
//    the next test written "the same way" cannot reach an opt-out — not without
//    deleting a test whose name says exactly what it is protecting.
// 3. **The daemon test asserts its own pid is gone** after dropping its handle,
//    so a `Drop` that quietly stopped killing reddens a test instead of quietly
//    leaking a process.
//
// # What was tried first, and why it is NOT here
//
// A process-wide registry (`static LIVE_CHILDREN`) plus a guard that swept and
// killed anything unreaped at the end of each test. Measured, it killed five
// OTHER tests' children — cargo runs these tests in parallel, and the registry
// was shared — so five tests failed with `None` exit codes and "the daemon never
// listened", and the guard's own `assert!` fired inside a `Drop` during unwinding:
// `panic in a destructor during cleanup` → SIGABRT, so the binary died rather
// than reporting. A leak detector that kills its neighbours' daemons and then
// aborts the run is worse than no leak detector. Per-test ownership (this design)
// needs no shared state at all, so there is nothing to scope and nothing to
// race.
//
/// Is `pid` gone — killed AND reaped?
///
/// `kill(pid, 0)` answers `ESRCH` once a process has been REAPED; a child that
/// was killed but never waited on is a zombie and still answers "alive", which is
/// the distinction this needs — the leak this file fixed was neither killed nor
/// reaped. So a "is it dead?" check that a zombie passes would let the exact
/// original defect through: `detach()` skipped both the `kill` and the `wait`.
///
/// Both platforms, because both daemon tests assert with it and the Windows leg
/// gets no other coverage of the claim. The Windows branch is `tasklist` filtered
/// on the pid — the same instrument `tests/serve.rs`'s `pid_is_alive` uses, and
/// with the same caveat: compile-checked here, first run by the Windows leg of CI.
fn pid_is_reaped(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // 0 means "exists and we may signal it", so any other answer is gone.
        // `EPERM` cannot arise for our own child.
        unsafe { libc::kill(pid as libc::pid_t, 0) != 0 }
    }
    #[cfg(windows)]
    {
        let Ok(out) = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
            .output()
        else {
            // Cannot ask; do not claim. `true` would silence the leak assertion
            // on the platform that most needs it to be honest.
            return false;
        };
        let text = String::from_utf8_lossy(&out.stdout);
        // A CSV row naming the pid means it is still there.
        !text.lines().any(|line| line.contains(&format!(",{pid},")))
    }
}

/// The guard behind point 2 above: the opt-out must stay deleted, in EVERY file
/// that spawns a `fulcrum` process.
///
/// That is a lint standing where a CI grep would otherwise have to go, and it is
/// in the spirit of `tests/fixture_bins.rs` — a target whose only job is to keep a
/// structural property loud. It is deliberately a TEXT check and not a
/// compile-time one: the property at risk is "someone re-added the method and
/// called it", which the compiler cannot see as wrong, because a private
/// `fn detach(&mut self)` is perfectly valid Rust.
///
/// # Why it scans more than this file
///
/// It used to scan `worker_mode.rs` alone, and the defect it guards against did
/// not respect that boundary: the leak it prevents is a `fulcrum` process left
/// running, and any file holding a `Child` can produce one. `tests/serve.rs`
/// spawns the daemon in six places (`Daemon`, plus the two-daemon and
/// bad-database tests) and has its own `Drop for Daemon`; the day someone adds a
/// `fn stop_without_reaping` there, a guard that reads only `worker_mode.rs`
/// reports green. Its own module comment said the defect shipped in exactly this
/// shape — a helper method that looked reasonable — so scoping the check to one
/// file was scoping it to the file that already got fixed.
///
/// # What this CAN catch, and what it cannot
///
/// CAN: a definition or a call of a reaping-opt-out by any of the names below
/// (`detach`, `abandon`, `orphan`, `leak`, `disown`, `release`, `forget`,
/// `keep_alive`), in any scanned file; and the two name-INDEPENDENT ways to skip
/// a `Drop` — `std::mem::forget` and `ManuallyDrop` — which is the point, since
/// those are not defeated by renaming.
///
/// CANNOT, and a reader should not assume otherwise:
///
/// * **A renamed opt-out that is not on the list.** `fn shrug()` or `fn park()`
///   passing the `Child` out of the struct walks straight past a name-based check.
///   The list is a net with known holes, not a proof. The structural needles above
///   close the two holes that matter most; they do not close this one.
/// * **A file that is not in the list below.** A new test file that spawns a
///   daemon is invisible until it is added here. That is the maintenance
///   obligation this design accepts in exchange for being able to catch anything
///   at all, and `the_spawn_sites_are_all_covered` below is what makes adding a
///   file a loud step rather than a silent omission.
/// * **Anything the compiler would catch anyway** — moving a `Child` out with
///   `into_inner`, `mem::replace`, or a plain field read is not a name, and a
///   text scan does not see the type.
///
/// So: the real guarantee is `Drop` being unconditional (which IS compiler-
/// enforced, and which these needles protect from the one thing that can undo
/// them — a second, optional path out). The needles make the reintroduction of
/// that second path expensive and visible. They do not make it impossible.
// No `cfg` gate: the property is about these files' SOURCE, which is the same
// source on every platform — gating it to unix would leave the Windows leg of CI
// free to reintroduce the opt-out.
#[test]
fn no_opt_out_of_reaping_can_come_back_in_any_spawn_site() {
    // Every file in this crate that holds a `Child` it is responsible for.
    // `main.rs` is scanned too rather than skipped: it does not spawn today
    // (`WorkerHandle::spawn` in the engine does), and if that changes, the guard
    // should already be looking.
    let files = [
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/worker_mode.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/serve.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"),
    ];
    // The needles are assembled so this test cannot match ITS OWN SOURCE LINES.
    // Written as literals they did, and the guard failed for containing the
    // strings it looks for. `concat!` is compile-time concatenation, so the
    // comparison is exact while the source line reads `concat!("fn ", "detach")`
    // — which is not the needle. Do not "simplify" these back to literals.
    //
    // The name list is the honest response to the guard's own weakness: the
    // original checked ONE name, and "call it something else" is the first thing
    // anyone reintroducing this would try. Each entry is a word a future author
    // would plausibly reach for when they want a child to survive a `Drop`.
    let opt_out_names = [
        concat!("de", "tach"),
        concat!("aban", "don"),
        concat!("or", "phan"),
        concat!("le", "ak"),
        concat!("diso", "wn"),
        concat!("keep_", "alive"),
    ];
    let mut offenders: Vec<(&str, usize, String)> = Vec::new();
    for file in files {
        let source = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {file} for the reaping guard: {e}"));
        let name = file.rsplit('/').next().unwrap_or(file);
        for (at, line) in source.lines().enumerate() {
            let trimmed = line.trim_start();
            // Comments naming the old method are fine and expected — this very
            // file explains what `detach()` did, in comments. Only executable
            // lines are checked.
            if trimmed.starts_with("//") {
                continue;
            }
            // Name-independent first: skipping a `Drop` does not require a method.
            // Assembled needles, for the same reason as `opt_out_names` — written
            // as literals this line matches itself and the guard fails for
            // containing what it looks for.
            let forget = concat!("mem", "::forget");
            let manual_drop = concat!("Manually", "Drop");
            let structural = trimmed.contains(forget) || trimmed.contains(manual_drop);
            let by_name = opt_out_names.iter().any(|n| {
                trimmed.contains(&format!("fn {n}")) || trimmed.contains(&format!(".{n}()"))
            });
            if structural || by_name {
                offenders.push((name, at + 1, trimmed.to_string()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a spawn site grew a way to skip reaping: {offenders:?}. \
         `fulcrum serve` never exits on its own (no shutdown frame in T9), so any \
         handle that can opt out of `Drop` is a way to leak a daemon for the life \
         of the machine — which is the defect `worker_mode.rs` had, and it measured \
         34 live processes after a day. If a child genuinely must outlive its test, \
         that needs a deliberate decision, not a helper method: say so in the \
         commit and reap it explicitly."
    );
}

/// The guard's own coverage is asserted, not assumed.
///
/// Every test in this family asserts an ABSENCE, so every one of them passes on a
/// file that contains nothing — and one that reads the wrong path passes just as
/// happily. `tests/boundary.rs` builds its whole vacuity-floor machinery around
/// exactly this; this is the same idea at the scale of one list. Two things are
/// checked: that each scanned file is real and non-trivial, and that at least one
/// scanned file still contains a `Drop` implementation (the mechanism the guard
/// exists to protect — if every `Drop` left the list, the guard would be policing
/// nothing).
#[test]
fn the_spawn_sites_are_all_covered() {
    let files = [
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/worker_mode.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/serve.rs"),
        concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"),
    ];
    let mut drops = 0usize;
    for file in files {
        let text = std::fs::read_to_string(file).unwrap_or_else(|e| {
            panic!("{file} is in the guard's file list but cannot be read: {e}")
        });
        assert!(
            text.len() > 1_000,
            "{file} is in the guard's list but is {len} bytes — a file that small \
             is a stub, and scanning a stub proves nothing about the real spawn \
             sites",
            len = text.len()
        );
        if text.contains("impl Drop for") {
            drops += 1;
        }
    }
    assert!(
        drops >= 2,
        "at least two of the scanned files must carry an `impl Drop` (the \
         unconditional reaper this guard protects); found {drops} — if the `Drop` \
         impls moved elsewhere, this guard is scanning files that no longer own a \
         child and its file list needs updating, not its needles"
    );
}

/// One spawned `fulcrum` process with its stdin kept open and its stdout on a
/// channel, so a read can time out instead of blocking forever.
///
/// # There is no way to leave a child running
///
/// `Drop` kills and reaps, unconditionally and on the panic path too. The child
/// sits in an `Option` rather than directly in the struct so a test that already
/// reaped it (via [`Spawned::wait_timeout`]) does not reap it twice — that is the
/// whole reason the `Option` exists, and it replaces the `owned: bool` flag the
/// old `detach()` flipped, which was a leak switch wearing the same clothes.
struct Spawned {
    child: Option<Child>,
    stdin: Option<std::process::ChildStdin>,
    /// `None` once the reader thread has been taken (stdout can only be `take`n
    /// once, so a test that reads lines and then wants the exit status must ask
    /// for the lines first).
    lines: Option<mpsc::Receiver<String>>,
}

/// How long a handshake line is allowed to take.
///
/// Generous because the work is a `fork`+`exec` of a binary that links the whole
/// engine (a cold page-in on a loaded CI runner is seconds, not microseconds),
/// and tight enough that a `--worker` flag nobody wired reads as a failure
/// inside a test run rather than as a stalled CI job.
const HANDSHAKE_BUDGET: Duration = Duration::from_secs(30);

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
        // child, and `Drop` needs it for the reap. `try_wait` (polled below)
        // reaps without blocking, which is all `wait_timeout` asks for.
        Self {
            child: Some(child),
            stdin: Some(stdin),
            lines: Some(lines),
        }
    }

    /// This child's pid, for the leak assertion in the daemon test.
    fn pid(&self) -> u32 {
        self.child
            .as_ref()
            .expect("the child is live until a reaper takes it")
            .id()
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
        let deadline = std::time::Instant::now() + budget;
        loop {
            // `None` = already reaped by an earlier call, so there is nothing
            // left to poll — and re-waiting a pid that could have been recycled
            // is worse than reporting no code.
            let child = self.child.as_mut()?;
            match child.try_wait() {
                Ok(Some(status)) => {
                    // REAPED: the `Option` empties, so neither `Drop` nor a
                    // second `wait_timeout` touches this pid again. This is what
                    // replaces the old `owned` flag — the state lives in the one
                    // place both reapers have to look.
                    self.child = None;
                    return status.code();
                }
                // A signal death (unix) carries no code; report it as "not 0"
                // by way of a non-zero sentinel rather than `None`, because the
                // callers distinguish "still running" from "exited badly".
                Ok(None) => {}
                Err(_) => {
                    self.child = None;
                    return Some(-1);
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        // Unconditional: `Spawned` has no way to opt out, because `fulcrum serve`
        // never exits on its own and a daemon released here outlives the machine.
        // A child the test already reaped reads as `None` and is left alone.
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            // `wait` is what REAPS it: `kill` alone would leave a zombie, and a
            // zombie still holds the pid — the half of the leak the original
            // `detach()` also got wrong.
            let _ = child.wait();
        }
        self.child = None;
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
    // No `detach()`: `wait_timeout` already reaped the child and emptied the
    // slot, so `Drop` has nothing to do. The old call here existed only to
    // suppress a double-reap that the `Option` in the slot now handles.
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
    // No `detach()` — see the note on `Spawned`: reaping is not optional, and
    // `wait_timeout` already took the child out of the slot.
}

// ── `serve` through the real argv ─────────────────────────────────────────────

/// The endpoint the CLI test binds, plus the store it is told to use.
///
/// Mirrors `tests/transport.rs`: the shape is `(keepalive, args)` so the call
/// site is identical on both platforms and the inactive leg is `cfg`-removed
/// rather than type-checked-and-dead.
///
/// # `--db` is not optional, and its absence was a real defect
///
/// `serve` falls back to [`fulcrum::serve::default_db_path`] — `<data_dir>/fulcrum/fulcrum.db`
/// — when `--db` is missing, and a test's `data_dir` is the DEVELOPER'S real one
/// (`~/.local/share/fulcrum/`). This file shipped without `--db` and so opened,
/// created and migrated a database in the developer's home directory on every
/// run: measured as `fulcrum.db` + `-wal` + `-shm` in `~/.local/share/`, created
/// fresh by a single `cargo test -p fulcrum --test worker_mode`. Compounding it,
/// the daemon's store holds full conversation transcripts, and until the mode
/// fix it was created 0644 — world-readable. A test must not write to a real
/// user path regardless of the mode bits.
///
/// `tests/serve.rs`'s `Daemon::start` has always passed an explicit `--db` into
/// its own `TempDir`; this is the same pattern, and it is why the leak and this
/// defect were found in the same file — one helper threaded the temp dir through,
/// the other invented its own argv.
#[cfg(unix)]
fn serve_cli_args(dir: &std::path::Path) -> Vec<String> {
    vec![
        "serve".to_string(),
        "--socket".to_string(),
        dir.join("cli.sock").display().to_string(),
        "--db".to_string(),
        dir.join("cli.db").display().to_string(),
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
        "--db".to_string(),
        dir.join("cli.db").display().to_string(),
    ]
}

/// The assertion behind the `--db` above: no invocation this file spawns may
/// resolve its store through `default_db_path()`.
///
/// Checked on the ARGS rather than by watching the filesystem, because the
/// filesystem check is the one that only fails on the machine that runs the test
/// a second time (the first run creates `~/.local/share/fulcrum`, so a
/// "did you create it?" probe has to know it wasn't already there). Argv is
/// deterministic.
// No `cfg` gate: `serve_cli_args` is `cfg`-SPLIT, so a `--db` that was added to
// only one arm is exactly what this catches — and gating it to unix would leave
// the arm nobody runs here unpoliced.
#[test]
fn every_serve_invocation_names_its_own_database() {
    // A path that is never created — this asserts on the ARGV SHAPE only, so it
    // touches no filesystem and needs no `TempDir`.
    let dir = &std::env::temp_dir().join("fulcrum-argv-shape-check");
    let args = serve_cli_args(dir);
    let db_at = args.iter().position(|a| a == "--db").expect(
        "`serve_cli_args` must pass `--db` — without it the daemon opens \
                 <data_dir>/fulcrum/fulcrum.db, i.e. the DEVELOPER'S real store",
    );
    let value = args
        .get(db_at + 1)
        .expect("`--db` must be followed by a path");
    assert!(
        std::path::Path::new(value).starts_with(dir),
        "`--db` must point INSIDE the test's own temp dir; got {value:?}"
    );
}

/// `fulcrum serve --socket/--pipe …` really binds and really answers: the argv
/// dispatch reaches the accept loop, not just the type checker.
///
/// No `--worker-exe` is passed, so the daemon falls back to `current_exe()` —
/// harmless here because nothing in this test starts a session, so no worker is
/// ever spawned. (The doc here used to claim a non-existent `--worker-exe` was
/// passed "deliberately"; it never was, and the drift mattered because the flag
/// whose ABSENCE caused defect B — `--db` — is documented right next to it.)
/// `--db` is passed, and must stay: without it the daemon opens the developer's
/// real `<data_dir>/fulcrum/fulcrum.db`. See [`serve_cli_args`].
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_cli_binds_the_socket_it_was_given_and_answers_hello() {
    let dir = tempfile::tempdir().expect("temp dir for the CLI socket");
    let args = serve_cli_args(dir.path());
    let mut spawned = Spawned::args(&args.iter().map(String::as_str).collect::<Vec<_>>());
    let daemon_pid = spawned.pid();

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

    // Defect B, asserted rather than measured: the store landed where THIS test
    // told it to. `serve` opens the database before it binds, so by the time a
    // `hello` has been answered the file must exist — and if it exists here, it
    // did not go to `default_db_path()` (the developer's real
    // `~/.local/share/fulcrum/fulcrum.db`, which is where this test used to
    // write, WAL and all).
    let db = dir.path().join("cli.db");
    assert!(
        db.is_file(),
        "the daemon must have opened the database it was given at {} — it is not \
         there, so `--db` was not honoured and the daemon fell through to \
         `default_db_path()`, i.e. the developer's real store",
        db.display()
    );

    // The daemon must still be alive (it answered, so it is — but a reply that
    // arrived before a panic would not notice the panic).
    assert!(
        spawned.wait_timeout(Duration::from_millis(50)).is_none(),
        "the daemon must not exit after serving one client"
    );
    // THE leak assertion, and the reason `detach()` is gone from this file.
    //
    // `fulcrum serve` has no shutdown frame in T9 and `serve::run` never
    // returns, so this daemon will NEVER exit by itself: dropping the handle is
    // the only thing standing between this test and a process that lives until
    // the machine is rebooted. The original released the child here with a
    // comment saying "Drop kills it" — while calling the method that switched
    // `Drop` off — and a day of green runs left 34 daemons live.
    //
    // Checked through the OS rather than through the handle, so a `Drop` that
    // quietly stopped killing fails HERE instead of leaking silently. The pid is
    // checked for GONE, not for dead: a killed-but-unwaited child is a zombie and
    // would still answer `kill(pid, 0)`, which is precisely the half of the
    // original defect that a "did it stop?" check would have missed.
    //
    // `spawned` is dropped explicitly rather than at the `}`, so the daemon is
    // gone before its `TempDir` is unlinked — the ordering `tests/serve.rs`'s
    // `Daemon` keeps for the same reason.
    drop(spawned);
    assert!(
        pid_is_reaped(daemon_pid),
        "the daemon (pid {daemon_pid}) SURVIVED its own handle: `fulcrum serve` \
         never exits on its own, so this is a process leak. It would outlive the \
         test binary and the login session — which is exactly what happened here \
         before `Drop` was made unconditional."
    );
}

#[cfg(not(unix))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_cli_binds_the_socket_it_was_given_and_answers_hello() {
    let dir = tempfile::tempdir().expect("temp dir for the CLI pipe name");
    let args = serve_cli_args(dir.path());
    let mut spawned = Spawned::args(&args.iter().map(String::as_str).collect::<Vec<_>>());
    let daemon_pid = spawned.pid();

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

    // Defect B, on the Windows leg too — the argv helper is `cfg`-split, so a
    // `--db` added only to the unix arm would leave this one writing to the real
    // `%APPDATA%` store, and nothing on this machine would notice.
    let db = dir.path().join("cli.db");
    assert!(
        db.is_file(),
        "the daemon must have opened the database it was given at {} — it is not \
         there, so `--db` was not honoured and the daemon fell through to \
         `default_db_path()`, i.e. the user's real store",
        db.display()
    );

    assert!(
        spawned.wait_timeout(Duration::from_millis(50)).is_none(),
        "the daemon must not exit after serving one client"
    );
    // See the unix leg: the pipe twin leaks exactly the same way, which is why
    // the fix is in the helper and not at one call site.
    drop(spawned);
    assert!(
        pid_is_reaped(daemon_pid),
        "the daemon (pid {daemon_pid}) SURVIVED its own handle: `fulcrum serve` \
         never exits on its own, so this is a process leak. It would outlive the \
         test binary and the login session — which is exactly what happened here \
         before `Drop` was made unconditional."
    );
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

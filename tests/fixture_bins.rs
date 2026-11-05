//! The crate's fixture binaries must exist and must be the RIGHT binaries.
//!
//! # Why this file exists at all — read this before deleting anything here
//!
//! Sixty-six of this crate's tests spawn `target/debug/fake_worker_core` (and
//! `fake_mcp_stdio_core`) by PATH, because a lib's unit tests get no
//! `CARGO_BIN_EXE_*` — a measured cargo limitation, recorded in the roadmap's
//! status file. Cargo only writes a package's bins to `target/debug/<name>`
//! (rather than just `target/debug/deps/<name>-<hash>`) **when the package has
//! an integration-test target**. This crate had none, so on a cold target dir
//! the path every one of those tests resolves named a file that was never
//! written, and all 66 failed with `ENOENT`.
//!
//! A fresh CI checkout always has a cold target dir, so CI caught it while a
//! local run — which had built `--all-targets` at some point — could not.
//!
//! **The existence of THIS file is therefore part of the build contract, not a
//! test that happens to be here.** Delete it and the 66 path-resolving tests
//! go red on any clean checkout. That failure is loud rather than silent, which
//! is the only reason the coupling is survivable; the assertions below are what
//! make it loud.
//!
//! # What is asserted, and why each half earns its place
//!
//! Existence alone would be a weak pin — it says a file is there, not that it is
//! the fixture. So each bin is also SPAWNED and asked to behave: the worker
//! performs the handshake unprompted, the MCP server drains stdin and exits 0.
//! A stale or wrong binary at that path fails those, not just the path check.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// The platform's executable suffix ("" on unix, `.exe` on Windows).
///
/// Load-bearing rather than cosmetic: `Path::is_file()` does NOT append it,
/// while `CreateProcessW` does append it when spawning. A check written without
/// the suffix therefore passes or fails for reasons unrelated to the thing it
/// claims to check — this cost a Windows CI round in T8.
const EXE_SUFFIX: &str = std::env::consts::EXE_SUFFIX;

/// The two fixture bins this package builds, and the crate-side names they are
/// known by.
///
/// The `_core` suffix is NOT cosmetic: two workspace members declaring a bin of
/// the same name collide on the OUTPUT FILENAME in the shared `target/debug/`,
/// cargo only warns, and the last writer silently wins — so a suite could spawn
/// the other package's fixture and never know. The desktop keeps the unsuffixed
/// names because its integration tests resolve them through
/// `CARGO_BIN_EXE_<name>`, which is package-local.
const WORKER_BIN: &str = "fake_worker_core";
const MCP_BIN: &str = "fake_mcp_stdio_core";

/// Where cargo put the binaries for the test currently running.
///
/// Derived from `current_exe()` (this test binary lives in
/// `target/debug/deps/`), NOT by counting levels above `CARGO_MANIFEST_DIR`:
/// the crate sits at `<root>/crates/fulcrum-core` inside the workspace and at
/// `<root>` as a standalone package, so a level count names a directory that
/// only one of the two layouts ever writes.
fn target_debug_dir() -> PathBuf {
    let exe = std::env::current_exe().expect("a test binary has a path");
    exe.parent() // target/debug/deps
        .and_then(|deps| deps.parent()) // target/debug
        .expect("the test binary lives in target/debug/deps")
        .to_path_buf()
}

fn bin_path(name: &str) -> PathBuf {
    target_debug_dir().join(format!("{name}{EXE_SUFFIX}"))
}

/// Read at most `n` lines from a child's stdout, with a deadline.
///
/// The deadline is the point: a fixture that never writes must FAIL this test,
/// not hang the whole CI job. Reading on the calling thread would block
/// indefinitely, and a hung test is strictly worse than a red one.
fn read_lines(child: &mut Child, n: usize, wait: Duration) -> Vec<String> {
    let stdout = child
        .stdout
        .take()
        .expect("the test asked for a piped stdout");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut buf = String::new();
        loop {
            buf.clear();
            match reader.read_line(&mut buf) {
                // EOF or a read error ends the stream; the thread exits and the
                // receiver sees a closed channel, which the caller treats as
                // "fewer lines than asked for".
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = buf.trim_end().to_string();
                    if !line.is_empty() && tx.send(line).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut lines = Vec::with_capacity(n);
    let deadline = std::time::Instant::now() + wait;
    while lines.len() < n {
        let budget = deadline.saturating_duration_since(std::time::Instant::now());
        if budget.is_zero() {
            break;
        }
        match rx.recv_timeout(budget) {
            Ok(line) => lines.push(line),
            Err(_) => break,
        }
    }
    lines
}

/// Reap a child without waiting forever on it.
fn kill_and_reap(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn both_fixture_bins_are_built_where_the_crate_resolves_them() {
    let dir = target_debug_dir();
    for name in [WORKER_BIN, MCP_BIN] {
        let path = bin_path(name);
        assert!(
            path.is_file(),
            "`{name}{EXE_SUFFIX}` is not at {} — the crate's 66 path-resolving \
             fixture tests would all fail with ENOENT. `target/debug/` for this \
             run is {dir:?}. Cargo only materialises a package's bins at the top \
             of `target/debug/` when the package HAS an integration-test target, \
             so if this file (`tests/fixture_bins.rs`) was deleted, or no test \
             target of this package is being built, that is the cause — see this \
             file's module docs.",
            path.display(),
        );
    }
}

/// Are these two paths the same FILE? (unix only — `dev`/`ino` have no
/// cross-platform equivalent.)
///
/// `#[cfg(unix)]` on the FUNCTION, not `if cfg!(unix)` around a closure: the
/// `std::os::unix` module does not exist on Windows, so a `cfg!` *boolean* still
/// fails to compile there. `cfg!` is a compile-time constant, not conditional
/// compilation — the branch is type-checked either way.
#[cfg(unix)]
fn same_inode(a: &std::path::Path, b: &std::path::Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(x), Ok(y)) => (x.dev(), x.ino()) == (y.dev(), y.ino()),
        _ => false,
    }
}

#[test]
fn the_two_fixture_bins_are_distinct_files() {
    // Guards the reason the names differ at all. If someone "tidies" the crate's
    // bins to the desktop's names, cargo's output-filename collision makes one
    // silently overwrite the other — and this is the assertion that would notice.
    let worker = bin_path(WORKER_BIN);
    let mcp = bin_path(MCP_BIN);
    assert_ne!(
        worker, mcp,
        "the two fixture bins must not resolve to one path"
    );

    #[cfg(unix)]
    {
        assert!(
            !same_inode(&worker, &mcp),
            "`{WORKER_BIN}` and `{MCP_BIN}` are the SAME file — that is cargo's \
             output-filename collision, which it only WARNS about while letting \
             the last writer win."
        );
    }

    // On Windows there is no inode to compare, so the collision guard there is
    // the two BEHAVIOURAL tests below: the worker must hand-shake and the MCP
    // server must exit 0 on stdin EOF. If one binary had overwritten the other,
    // exactly one of those would fail. A path/identity check is the weak form of
    // this pin; behaviour is the strong form, and it is portable.
    #[cfg(not(unix))]
    {
        assert!(
            std::fs::metadata(&worker).is_ok() && std::fs::metadata(&mcp).is_ok(),
            "both fixture bins must exist to be compared"
        );
    }
}

#[test]
fn the_worker_fixture_hands_shakes_before_any_start() {
    // The fixture emits `hello` then `ready` UNPROMPTED, both unstamped, exactly
    // like the real Worker. This is what proves the file at that path is the
    // worker fixture and not merely some executable.
    let mut child = Command::new(bin_path(WORKER_BIN))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawning {WORKER_BIN}: {e}"));

    let lines = read_lines(&mut child, 2, Duration::from_secs(10));
    kill_and_reap(child);

    assert_eq!(
        lines.len(),
        2,
        "expected the `hello` then `ready` handshake, got {} line(s): {lines:?}",
        lines.len()
    );

    let hello: serde_json::Value = serde_json::from_str(&lines[0])
        .unwrap_or_else(|e| panic!("`hello` is not JSON ({e}): {:?}", lines[0]));
    assert_eq!(
        hello.get("type").and_then(|t| t.as_str()),
        Some("hello"),
        "the FIRST frame must be `hello`, got {:?}",
        lines[0]
    );
    assert_eq!(
        hello.get("protocolVersion").and_then(|v| v.as_u64()),
        Some(1),
        "`hello` must carry protocolVersion 1, got {:?}",
        lines[0]
    );

    let ready: serde_json::Value = serde_json::from_str(&lines[1])
        .unwrap_or_else(|e| panic!("`ready` is not JSON ({e}): {:?}", lines[1]));
    assert_eq!(
        ready.get("type").and_then(|t| t.as_str()),
        Some("ready"),
        "the SECOND frame must be `ready`, got {:?}",
        lines[1]
    );
}

#[test]
fn the_mcp_fixture_drains_stdin_and_exits_zero() {
    // No protocol needed: the fixture's read loop ends at stdin EOF and it
    // exits 0. So closing stdin is a deterministic way to ask "is this the MCP
    // fixture, and does it shut down cleanly" without depending on its
    // JSON-RPC responses.
    let mut child = Command::new(bin_path(MCP_BIN))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawning {MCP_BIN}: {e}"));

    // Drop stdin => EOF => the fixture's `for line in stdin.lock().lines()` ends.
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.flush();
        drop(stdin);
    }

    let status = child
        .wait()
        .expect("the MCP fixture exits once stdin closes");
    assert!(
        status.success(),
        "the MCP fixture exited {status:?} on stdin EOF; the tests that spawn it \
         rely on a clean exit"
    );
}

#[test]
fn the_worker_fixture_exits_zero_on_close_and_137_on_crash() {
    // The two exit-code contracts the Supervisor's tests assert. Spawning the
    // real bin and observing the codes is the cheapest way to know the crate's
    // bin is wired to the same body the desktop's bin is.
    let codes = |args: &[&str], feed: &str| -> Option<i32> {
        let mut child = Command::new(bin_path(WORKER_BIN))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .ok()?;
        // Consume the handshake first, or the fixture's own writes can block.
        let _ = read_lines(&mut child, 2, Duration::from_secs(10));
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(feed.as_bytes());
            let _ = stdin.write_all(b"\n");
            let _ = stdin.flush();
        }
        child.wait().ok().and_then(|s| s.code())
    };

    assert_eq!(
        codes(&[], r#"{"type":"close"}"#),
        Some(0),
        "`close` must exit 0 — the Supervisor treats a clean worker exit as success"
    );
    // `__crash__` is not a frame TYPE: it is the TEXT of an ordinary `prompt`,
    // which the fixture matches on and answers by exiting 137 (the SIGKILL
    // convention). `images` has no `serde(default)`, so it must be present.
    assert_eq!(
        codes(&[], r#"{"type":"prompt","text":"__crash__","images":[]}"#),
        Some(137),
        "a `prompt` whose text is `__crash__` must exit 137 — the crash-path \
         tests assert exactly this"
    );
}

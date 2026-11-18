//! The `ARCHIMEDES_DEBUG` protocol log (ADR 0025 Task 6 — the
//! Supervisor finally has logs): when `ARCHIMEDES_DEBUG=1` (read ONCE
//! at startup, cached — the check is a single atomic read, so `log` is
//! a free no-op when the var is unset and NO file is created), the
//! Supervisor logs to `<data_dir>/archimedes/supervisor.log`: every
//! Worker lifecycle transition (spawn/ready/exit/crash, with the
//! session id + exit code) + every IPC line (both directions — the
//! `WorkerHandle`'s stdin writer + stdout reader, truncated to 2 KB
//! per line — a streaming token stream would otherwise be unbounded).
//!
//! Rotation: on write, if the file exceeds 5 MB, it is renamed to
//! `supervisor.log.old` (the old one overwritten) — a simple size
//! check, no background task.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// The rotation threshold (5 MB).
pub const MAX_LOG_BYTES: u64 = 5 * 1024 * 1024;
/// The per-line truncation bound (2 KB — a streaming token stream would
/// otherwise be unbounded).
pub const MAX_LINE_BYTES: usize = 2048;

/// The cached debug flag (set once by `init` / `init_from_env`).
static ENABLED: AtomicBool = AtomicBool::new(false);
/// The log dir (`None` = disabled).
static DIR: Mutex<Option<PathBuf>> = Mutex::new(None);

/// The init seam (production: `init_from_env`; tests: an explicit
/// `enabled` + `dir` — a temp dir; `None` dir = disabled).
pub fn init(enabled: bool, dir: Option<PathBuf>) {
    ENABLED.store(enabled && dir.is_some(), Ordering::Relaxed);
    let mut g = DIR.lock().unwrap_or_else(|p| p.into_inner());
    *g = if enabled { dir } else { None };
}

/// Read `ARCHIMEDES_DEBUG` once (the cached check — `log` is a free
/// no-op when the var is unset; `=1` enables, anything else disables).
/// A `None` data dir (an undeterminable platform home) disables too.
pub fn init_from_env() {
    let enabled = std::env::var("ARCHIMEDES_DEBUG")
        .as_deref()
        .is_ok_and(|v| v == "1");
    if !enabled {
        init(false, None);
        return;
    }
    let Some(base) = dirs::data_dir() else {
        init(false, None);
        return;
    };
    init(true, Some(base.join("archimedes")));
}

/// Append one line to the configured log (a no-op when disabled — the
/// cached check makes this free; no file is created). Reads the global
/// dir, then delegates to [`append_to`].
pub fn log(line: &str) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let dir = DIR.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let Some(dir) = dir else {
        return;
    };
    append_to(&dir, line);
}

/// The core append + rotation (PURE w.r.t. the global state — takes the
/// target dir explicitly, so tests exercise it with their OWN dir and
/// never race the global `DIR` that the Worker tests' `log` calls use):
/// `create_dir_all`, the rotation (rename the over-threshold file to
/// `supervisor.log.old`, overwriting the old), then an append write with
/// a timestamp prefix (epoch millis — no `chrono` dep; the lines stay
/// orderable).
pub fn append_to(dir: &std::path::Path, line: &str) {
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    let path = dir.join("supervisor.log");
    // The rotation (the simple size check — no background task):
    // the current file PAST the threshold is renamed to `.old` (the
    // old one overwritten), then a fresh file starts.
    if let Ok(meta) = std::fs::metadata(&path) {
        if meta.len() >= MAX_LOG_BYTES {
            let _ = std::fs::rename(&path, dir.join("supervisor.log.old"));
        }
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let Some(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .ok()
    else {
        return;
    };
    // ONE `write_all`, not a `writeln!` — the [`crate::diagnostics::append_to`]
    // precedent, and the same bug.
    //
    // `writeln!(f, "{ts} {line}")` formats in PIECES — the timestamp, the space,
    // the payload, the newline — and `write_fmt` can issue a separate `write`
    // syscall for each. This file is opened `O_APPEND`, so every one of those
    // writes seeks to the current end on its own and a second writer's pieces
    // land BETWEEN this writer's, producing a file whose lines are not lines at
    // all: two epoch-millis prefixes side by side and both payloads concatenated.
    // That makes EVERY whole-line property of this log unsound for a reader — the
    // `ts` prefix, "one line per record", "a payload cannot forge a line" — and
    // this log is the artifact a supporter reads when a Worker dies. A single
    // `write_all` is one syscall, and `O_APPEND` makes the seek-to-end plus write
    // atomic per call, so a record is whole or absent, never half of two.
    //
    // Exposure is lower than `diagnostics`' (this whole module is gated behind
    // `ARCHIMEDES_DEBUG`, and `log_truncated` already caps each frame at 2 KB),
    // but the failure mode is identical and several Workers write this ONE file.
    let record = format!("{ts} {line}\n");
    let _ = f.write_all(record.as_bytes());
}

/// Log a raw IPC line (both directions — the `WorkerHandle`'s stdin
/// writer + stdout reader): `prefix` = `"→"` (outbound) / `"←"`
/// (inbound); the line is truncated to 2 KB with a `"…"` marker.
pub fn log_truncated(prefix: &str, line: &str) {
    log(&format!("{prefix} {}", truncate_line(line)));
}

/// Truncate a line to `MAX_LINE_BYTES` with the `"…"` marker (a no-op
/// for short lines — the marker is appended only when a cut happens).
pub fn truncate_line(line: &str) -> String {
    if line.len() <= MAX_LINE_BYTES {
        return line.to_string();
    }
    // Cut at a char boundary (the bound may split a multi-byte char).
    let mut end = MAX_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &line[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize the global-state tests (the `test_support` `ENV_LOCK`
    /// pattern — the statics are process-global; a sibling test's
    /// `init` would otherwise race this test's assertions).
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        crate::test_support::env_lock()
    }

    /// A unique temp dir, OWNED (#96 task 08) — removed when the guard drops.
    /// Declared after the `lock()` guard in every caller, so it drops BEFORE the
    /// env lock is released; and every test ends with `init(false, None)`, which
    /// clears the process-global `DIR` before the guard drops (that statement is
    /// a TEARDOWN, not a cleanup — kept).
    ///
    /// HONEST SIZING: 4 mints, 4 `remove_dir_all` — green delta 0 before and
    /// after; the win is the panic path. (D14: `grep -c 'temp_dir()'` reads 6
    /// here because it counts the definition line and the helper's inner
    /// `std::env::temp_dir()`; the CALL form `= temp_dir();` reads 4.)
    fn temp_dir() -> crate::test_support::ScratchDir {
        crate::test_support::ScratchDir::in_env_temp("debuglog-test-", "dir")
    }

    /// The rotation: when the current file exceeds the 5 MB threshold,
    /// the next write renames it to `supervisor.log.old` (the old one
    /// overwritten) and starts a fresh file. Exercises `append_to` with
    /// an EXPLICIT dir (the pure core — NOT the global `DIR`, which the
    /// Worker tests' `log` calls use concurrently; a global-state
    /// rotation test would race those writers mid-test).
    #[test]
    fn rotation_renames_to_old_at_the_size_threshold() {
        let dir = temp_dir();
        // Pre-fill the log PAST the threshold (the rotation fires on
        // the next write).
        std::fs::write(
            dir.join("supervisor.log"),
            vec![b'x'; (MAX_LOG_BYTES + 1) as usize],
        )
        .unwrap();
        // A stale `.old` (the rename overwrites it).
        std::fs::write(dir.join("supervisor.log.old"), b"stale").unwrap();
        append_to(&dir, "after rotation");
        let old = std::fs::read_to_string(dir.join("supervisor.log.old"))
            .expect("the rotation renamed the file");
        assert!(
            old.starts_with("xxx"),
            "the OLD content is in the .old file"
        );
        assert!(!old.contains("stale"));
        let fresh =
            std::fs::read_to_string(dir.join("supervisor.log")).expect("the fresh file is created");
        assert!(
            fresh.contains("after rotation"),
            "the new line is in the fresh file:\n{fresh}"
        );
    }

    /// The `log` → `append_to` delegation (the global-state wiring — the
    /// `contains` assertion is race-tolerant: a concurrent Worker test's
    /// `log` call may add lines to the same file, but the marker holds).
    #[test]
    fn log_delegates_to_append_to_when_enabled() {
        let _g = lock();
        let dir = temp_dir();
        init(true, Some(dir.to_path_buf()));
        log("delegation marker");
        let content =
            std::fs::read_to_string(dir.join("supervisor.log")).expect("the log file is created");
        assert!(
            content.contains("delegation marker"),
            "the line is in the log:\n{content}"
        );
        init(false, None);
    }

    /// With the flag off, `log` is a no-op (no file is created).
    #[test]
    fn log_is_a_noop_when_disabled() {
        let _g = lock();
        let dir = temp_dir();
        init(false, None);
        log("never written");
        assert!(
            !dir.join("supervisor.log").exists(),
            "no file is created when the flag is off"
        );
    }

    /// `init_from_env`: `ARCHIMEDES_DEBUG=1` enables (the `ENV_LOCK`
    /// pattern — the env var is process-global); unset / other values
    /// disable.
    #[test]
    fn init_from_env_reads_the_var_once() {
        let _g = lock();
        std::env::remove_var("ARCHIMEDES_DEBUG");
        init_from_env();
        let dir = temp_dir();
        log("x");
        assert!(!dir.join("supervisor.log").exists());
        std::env::set_var("ARCHIMEDES_DEBUG", "1");
        init_from_env();
        init(true, Some(dir.to_path_buf())); // the data dir is not testable here — re-point.
        log("y");
        assert!(dir.join("supervisor.log").exists());
        std::env::remove_var("ARCHIMEDES_DEBUG");
        init(false, None);
    }

    /// `truncate_line`: a short line is unchanged; a long line is cut
    /// to 2 KB with the `"…"` marker.
    #[test]
    fn truncate_line_cuts_long_lines_with_the_marker() {
        let short = "hello";
        assert_eq!(truncate_line(short), short);
        let long = "a".repeat(MAX_LINE_BYTES + 100);
        let t = truncate_line(&long);
        assert!(t.ends_with('…'), "the marker is at the end");
        let body = t.strip_suffix('…').unwrap();
        assert!(
            body.len() <= MAX_LINE_BYTES,
            "the body is at most 2 KB (got {} bytes)",
            body.len()
        );
    }

    /// The [`crate::diagnostics`] precedent, applied here: ONE `write_all` per
    /// record, so a second writer cannot interleave WITHIN a line.
    ///
    /// `writeln!(f, "{ts} {line}")` formatted in pieces and `write_fmt` may issue
    /// a `write` syscall per piece; on an `O_APPEND` file each piece takes its own
    /// offset, so a concurrent writer's pieces land between this writer's and the
    /// result is a file whose "lines" are two timestamps and two half payloads
    /// welded together. The race needs scheduler luck to observe, so this does not
    /// wait for it — it manufactures the condition, exactly as
    /// `a_sibling_writer_does_not_break_the_timestamp_assertions` does in
    /// `diagnostics`: a thread with NO lock (as a real Worker's `log` call has
    /// none) hammers the same dir through `append_to`, the pure seam.
    ///
    /// The property asserted is the one the fix claims: every line is a WHOLE
    /// record — one `ts` prefix, then the payload, and a payload can never be seen
    /// sharing a line with a second timestamp.
    #[test]
    fn a_sibling_writer_cannot_interleave_within_a_line() {
        // The dir is what the assertions read, so the global `DIR` must not be
        // pointed at it by a concurrent global-state test — but `append_to` takes
        // the dir explicitly and needs no lock for its OWN writes. The lock is
        // taken because a sibling global-state test could otherwise rotate THIS
        // file out from under the read below.
        let _g = lock();
        let dir = temp_dir();

        append_to(&dir, "owned-first");
        // The thread gets its OWN handle (a `PathBuf`, not the `ScratchDir` guard —
        // the guard must stay here so the dir outlives the assertions below).
        let sibling_dir = dir.to_path_buf();
        let sibling = std::thread::spawn(move || {
            for i in 0..200 {
                append_to(&sibling_dir, &format!("sibling payload {i}"));
            }
        });
        // THE OVERLAP. A sibling that is merely busy is not a concurrent writer:
        // joining before writing again would leave this test with ONE writer at a
        // time and a green result over the broken `writeln!` form (measured — it
        // passed). So the owned records are written WHILE the sibling is mid-loop,
        // which is the only condition under which a piece-wise `write_fmt` can be
        // caught between another writer's pieces.
        let mut owned = Vec::new();
        for i in 0..50 {
            append_to(&dir, &format!("owned payload {i}"));
            owned.push(format!("owned payload {i}"));
        }
        sibling.join().expect("the sibling writer finishes");
        append_to(&dir, "owned-last");
        owned.push("owned-first".to_string());
        owned.push("owned-last".to_string());

        let content = std::fs::read_to_string(dir.join("supervisor.log")).expect("the log");
        let lines: Vec<&str> = content.lines().collect();

        // Non-vacuity: the interference landed, or the assertions below are free.
        assert!(
            lines.len() > 2,
            "the sibling writer reached this file (otherwise this test proves \
             nothing): {content}"
        );

        for (index, line) in lines.iter().enumerate() {
            let (ts, rest) = line
                .split_once(' ')
                .unwrap_or_else(|| panic!("line {index} has no `ts ` prefix: {line}"));
            assert!(
                ts.bytes().all(|b| b.is_ascii_digit()) && !ts.is_empty(),
                "the prefix of line {index} is ONE epoch-millis number, not two \
                 welded together by an interleaved write: {line}"
            );
            assert!(
                !rest.contains("supervisor payload sibling payload")
                    && !rest.contains("owned-firstowned")
                    && !rest.contains("owned-lastowned"),
                "no line carries two payloads concatenated: {line}"
            );
        }

        // Both owned records are present and WHOLE, in order.
        let first = lines
            .iter()
            .position(|l| l.ends_with("owned-first"))
            .expect("the first owned line is intact");
        let last = lines
            .iter()
            .position(|l| l.ends_with("owned-last"))
            .expect("the last owned line is intact");
        assert!(first < last, "the owned lines stay in order");
        // EVERY owned record — the 50 written WHILE the sibling was mid-loop
        // included — reads back as exactly one whole line. This is the property
        // the one-write form buys and the `writeln!` form cannot make true.
        for payload in &owned {
            let hits = lines
                .iter()
                .filter(|l| l.ends_with(payload.as_str()))
                .count();
            assert_eq!(
                hits, 1,
                "`{payload}` is the tail of exactly ONE line (found {hits}) — a \
                 second copy means a payload was duplicated by a split write, and \
                 zero means it was cut"
            );
        }
        // Every sibling line is whole too — the count is the strongest witness,
        // since a split line would merge two records into one.
        let siblings = lines
            .iter()
            .filter(|l| {
                l.split_once(' ')
                    .is_some_and(|(_, rest)| rest.starts_with("sibling payload"))
            })
            .count();
        assert_eq!(
            siblings, 200,
            "all 200 sibling records are WHOLE lines — a single intra-line \
             interleave would merge two of them and read 199"
        );
    }
}

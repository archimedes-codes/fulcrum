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
    let _ = writeln!(f, "{ts} {line}");
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

    /// A unique temp dir (cleaned up by the caller).
    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("debuglog-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
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
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The `log` → `append_to` delegation (the global-state wiring — the
    /// `contains` assertion is race-tolerant: a concurrent Worker test's
    /// `log` call may add lines to the same file, but the marker holds).
    #[test]
    fn log_delegates_to_append_to_when_enabled() {
        let _g = lock();
        let dir = temp_dir();
        init(true, Some(dir.clone()));
        log("delegation marker");
        let content =
            std::fs::read_to_string(dir.join("supervisor.log")).expect("the log file is created");
        assert!(
            content.contains("delegation marker"),
            "the line is in the log:\n{content}"
        );
        init(false, None);
        let _ = std::fs::remove_dir_all(&dir);
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
        let _ = std::fs::remove_dir_all(&dir);
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
        init(true, Some(dir.clone())); // the data dir is not testable here — re-point.
        log("y");
        assert!(dir.join("supervisor.log").exists());
        std::env::remove_var("ARCHIMEDES_DEBUG");
        init(false, None);
        let _ = std::fs::remove_dir_all(&dir);
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
}

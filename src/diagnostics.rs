//! The ALWAYS-ON diagnostics channel — deliberately NOT the `ARCHIMEDES_DEBUG`
//! protocol log (see the module's own reasoning below).
//!
//! ## Why this exists
//!
//! The engine's failure paths are invisible in a shipped build. `debuglog::log`
//! — the only sink they had — returns at its FIRST statement unless
//! `ARCHIMEDES_DEBUG=1`, so a user who launches the app from a desktop entry
//! (where no env var is set and `eprintln!` has no reader at all) gets nothing,
//! and a supporter asking "why did it not do the thing?" has no file to ask for.
//! The session-titling failure is the case that forced this: a title request that
//! fails three times simply stops, silently, for the rest of the session's life.
//!
//! ## Why this is a SECOND channel and not the existing one switched on
//!
//! `debuglog` is a full protocol transcript. `worker/client.rs` writes every
//! frame in both directions through `log_truncated("→", …)` /
//! `("←", …)`, and `Inbound::Prompt { text }` is one of those frames — so the
//! gated log carries the user's conversation and their model's token stream,
//! truncated only to 2 KB per line. That is exactly what makes it safe to leave
//! behind an opt-in switch, and exactly why the switch must not be flipped to
//! make a handful of error lines visible. Turning it on by default would record
//! every conversation to disk without asking.
//!
//! So this module is separate: a curated, low-volume channel for the handful of
//! lines that explain a refusal or a silent drop, whose call sites are reviewed
//! for user data one at a time. The filename is different
//! ([`FILE_NAME`](crate::diagnostics::FILE_NAME)) so the two can never be
//! confused and so a supporter collecting one never collects the other.
//!
//! ## The rule for adding a line here
//!
//! **A `record` call site may not contain user content.** Not a transcript, not a
//! prompt, not a model answer, not a token, not a bearer, not a file body. A
//! model id, a session id, a provider id, a frame count, an error sentence from
//! our own code: those are what belong. `debuglog` is where the transcript goes
//! because somebody opted into it; this file is where the transcript must never
//! go because nobody opted into it. Every call site is therefore a reviewed
//! decision, not a convenience — which is the whole reason this is a new module
//! rather than a new level on the old one.
//!
//! ## Volume and rotation
//!
//! Call sites are error and refusal paths, so the volume is a handful of lines
//! per session, and the file rotates at [`MAX_DIAGNOSTIC_BYTES`] by renaming to
//! the `.old` sibling (the `debuglog` shape: a size check on write, no background
//! task). It is smaller than the protocol log's cap on purpose — this channel is
//! meant to be readable end to end, not tailed.
//!
//! ## Test and host control
//!
//! The target starts **unarmed**: `record` writes nothing until a host calls
//! [`init`] with a directory. That is not the env var returning by another name —
//! the env var is never read here — it is the host saying where its data lives,
//! once, beside the `debuglog::init_from_env()` call it already makes. A host that
//! never calls `init` gets the old silence and nothing else.
//!
//! Why the module refuses to resolve `dirs::data_dir()` itself: the pump's
//! frame-drop line is reached by an existing manager test that inits nothing, so a
//! lazy default would make `cargo test` write into the developer's real
//! `~/.local/share/archimedes/`. `debuglog` is test-safe because its default is
//! off and its tests arm it against a scratch dir; this module keeps that property
//! rather than trading it for one fewer startup line. It is also what invariant D
//! in `childproc` argues for — the crate resolves the base dir at install time and
//! passes it across, and a second module independently resolving it is the drift
//! that pin exists to catch.
//!
//! **A test must therefore call `init(Some(temp_dir))` before its first
//! `record`**, under `test_support::env_lock()` (the target is process-global, so
//! a sibling test's `init` would otherwise race these assertions).

use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

/// The diagnostics file's name, beside `supervisor.log` and NOT the same file.
///
/// Naming it is the point: the separation from the protocol log is the load-bearing
/// property of this module, and a test can only pin "not the transcript file" if
/// the transcript file's name is a fact in the code rather than a string in a doc.
pub const FILE_NAME: &str = "diagnostics.log";

/// The rotation threshold. Smaller than the protocol log's 5 MB because this
/// channel is meant to be read whole.
pub const MAX_DIAGNOSTIC_BYTES: u64 = 1024 * 1024;

/// The target dir. `None` = DISABLED, and that is the DEFAULT.
///
/// ## Why the default is off, when the whole point is that this is always on
///
/// "Always on" means **no env var is ever consulted** — it does not mean the
/// module resolves its own home. Two independent reasons force the host to inject
/// the directory:
///
/// 1. **Test safety.** A lazy `dirs::data_dir()` default means any test that
///    reaches `record` writes into the REAL user data dir. The pump's
///    frame-drop line is reached by `lagged_frames_log_continue_and_preserve_partial_salvage`,
///    which inits nothing — so a lazy default would make `cargo test` litter the
///    developer's `~/.local/share/archimedes/`. `debuglog` is test-safe for
///    exactly this reason: its default is off, and its own tests point it at a
///    scratch dir. This module keeps that property.
/// 2. **The repo already decided this.** Invariant D in `childproc` exists
///    because a crash-log module resolving its own data dir is the shape that
///    produced the 862-file bug; the crate resolves the base dir at install time
///    and passes it across (`worker/mod.rs`), owning no slot itself. A second
///    module independently resolving `dirs::data_dir()` is the drift that
///    invariant argues against.
///
/// So a host turns this on with ONE line at startup —
/// `diagnostics::init(Some(data_dir.join("archimedes")))`, beside where it calls
/// `debuglog::init_from_env()` — and every `record` after that is unconditional.
static DIR: Mutex<Option<PathBuf>> = Mutex::new(None);

fn slot() -> &'static Mutex<Option<PathBuf>> {
    &DIR
}

/// Set the diagnostics target. `Some(dir)` writes there from now on; `init(None)`
/// disables the channel process-wide (the test teardown, and a host's off switch).
///
/// Last call wins, which is what lets each test point the channel at its own
/// scratch dir without a reset hook.
pub fn init(dir: Option<PathBuf>) {
    *slot().lock().unwrap_or_else(|p| p.into_inner()) = dir;
}

/// The effective target (`None` until a host calls [`init`]).
fn effective_dir() -> Option<PathBuf> {
    slot().lock().unwrap_or_else(|p| p.into_inner()).clone()
}

/// Append one diagnostics line. **Always on** — no env var, no feature flag, no
/// caller opt-in.
///
/// Never panics and never blocks anything user-facing beyond one small append:
/// every failure (unresolvable dir, unwritable file, poisoned lock recovered) is
/// swallowed, because a diagnostic channel that can fail loudly is worse than one
/// that quietly drops its own line — the thing it was reporting is still reported.
///
/// Prefer this over `debuglog::log` ONLY for the always-visible subset, and read
/// the module's rule above about user content before adding a call site.
pub fn record(line: &str) {
    let Some(dir) = effective_dir() else {
        return;
    };
    append_to(&dir, line);
}

/// The core append + rotation, PURE w.r.t. the global target (takes the dir
/// explicitly, so tests exercise it against their own scratch dir and never race
/// the process-global one — the `debuglog::append_to` shape, and for the same
/// reason).
pub fn append_to(dir: &std::path::Path, line: &str) {
    if std::fs::create_dir_all(dir).is_err() {
        return;
    }
    let path = dir.join(FILE_NAME);
    // The rotation: the over-threshold file is renamed to `.old` (the old `.old`
    // overwritten), then a fresh file starts.
    if let Ok(meta) = std::fs::metadata(&path) {
        if meta.len() >= MAX_DIAGNOSTIC_BYTES {
            let _ = std::fs::rename(&path, dir.join(format!("{FILE_NAME}.old")));
        }
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    else {
        return;
    };
    let _ = writeln!(f, "{ts} {line}");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serialize the process-global target (the `debuglog` / `ENV_LOCK` pattern).
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        crate::test_support::env_lock()
    }

    fn temp_dir() -> crate::test_support::ScratchDir {
        crate::test_support::ScratchDir::in_env_temp("diagnostics-test-", "dir")
    }

    /// THE claim this module exists for: with NO env var set, the line lands.
    ///
    /// This is the whole difference from `debuglog::log`, whose own test
    /// (`log_is_a_noop_when_disabled`) asserts the opposite for the same
    /// circumstances. Removing the env gate reddens this test; re-adding it
    /// anywhere — including a well-meaning "make it opt-in for now" — cannot pass.
    #[test]
    fn a_line_lands_with_no_env_var_set() {
        let _g = lock();
        std::env::remove_var("ARCHIMEDES_DEBUG");
        let dir = temp_dir();
        init(Some(dir.to_path_buf()));
        record("session-name: naming failed 3 times for arch_x; check the model");
        let content = std::fs::read_to_string(dir.join(FILE_NAME))
            .expect("the diagnostics file is created with no env var set");
        assert!(
            content.contains("naming failed 3 times"),
            "the line is in the file:\n{content}"
        );
        init(None);
    }

    /// `init(None)` DISABLES the channel (the teardown, and a host's off switch).
    #[test]
    fn init_with_none_disables_the_channel() {
        let _g = lock();
        let dir = temp_dir();
        init(None);
        record("never written");
        assert!(
            !dir.join(FILE_NAME).exists(),
            "a disabled channel creates no file in a dir it was never pointed at"
        );
        init(Some(dir.to_path_buf()));
        record("written after re-enabling");
        assert!(dir.join(FILE_NAME).exists(), "and it re-enables");
        init(None);
    }

    /// The separation from the protocol log, pinned as a fact rather than prose:
    /// the diagnostics channel must never be the file that carries the transcript.
    #[test]
    fn the_diagnostics_file_is_not_the_protocol_log() {
        let _g = lock();
        assert_eq!(FILE_NAME, "diagnostics.log");
        assert_ne!(
            FILE_NAME, "supervisor.log",
            "`supervisor.log` is the `ARCHIMEDES_DEBUG` transcript (every `→`/`←` \
             frame, prompts included) — this channel must not write there, and a \
             supporter collecting diagnostics must not collect the transcript"
        );
        let dir = temp_dir();
        init(Some(dir.to_path_buf()));
        record("separation marker");
        assert!(dir.join(FILE_NAME).exists());
        assert!(
            !dir.join("supervisor.log").exists(),
            "recording a diagnostic creates no protocol-log file"
        );
        init(None);
    }

    /// Rotation at the cap: the over-threshold file moves aside, and the fresh
    /// file holds only the newest line.
    #[test]
    fn rotation_renames_the_over_threshold_file() {
        let _g = lock();
        let dir = temp_dir();
        init(Some(dir.to_path_buf()));
        let path = dir.join(FILE_NAME);
        std::fs::write(&path, "x".repeat(MAX_DIAGNOSTIC_BYTES as usize))
            .expect("seed the over-threshold file");
        record("the line after rotation");
        let old = dir.join(format!("{FILE_NAME}.old"));
        assert!(old.exists(), "the over-threshold file was renamed to .old");
        let content = std::fs::read_to_string(&path).expect("a fresh file");
        assert!(
            content.contains("the line after rotation") && !content.contains('x'),
            "the fresh file holds only the new line:\n{content}"
        );
        init(None);
    }

    /// A line is timestamped and newline-terminated, so lines stay orderable —
    /// the property that makes the file worth asking a user for.
    #[test]
    fn a_line_is_timestamped_and_terminated() {
        let _g = lock();
        let dir = temp_dir();
        init(Some(dir.to_path_buf()));
        record("first");
        record("second");
        let content = std::fs::read_to_string(dir.join(FILE_NAME)).expect("the file");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "one line per record:\n{content}");
        for line in &lines {
            let (ts, rest) = line
                .split_once(' ')
                .unwrap_or_else(|| panic!("every line carries an epoch-millis prefix: {line}"));
            assert!(
                ts.parse::<u128>().is_ok() && ts.len() >= 10,
                "the prefix is epoch millis, not prose: {line}"
            );
            assert!(!rest.is_empty(), "the payload survives the prefix: {line}");
        }
        assert!(lines[0].ends_with("first"), "the payload is verbatim");
        init(None);
    }

    /// THE test-safety property: unarmed, the channel writes NOWHERE — in
    /// particular not into the real user data dir.
    ///
    /// This is the witness for choosing an unarmed default over a lazy
    /// `dirs::data_dir()` one. The pump's frame-drop line is reached by
    /// `worker::manager::tests::lagged_frames_log_continue_and_preserve_partial_salvage`,
    /// which inits nothing, so a lazy default would make `cargo test` create
    /// `~/.local/share/archimedes/diagnostics.log` on every developer machine and
    /// CI runner. Give `record` a lazy default and this test reddens while the
    /// suite stays green — which is the point: the suite cannot see its own
    /// side effects on the home directory, so the assertion lives here.
    #[test]
    fn unarmed_it_writes_nothing_anywhere() {
        let _g = lock();
        init(None);
        // A dir the test owns and never arms: the proof is that NOTHING appears
        // in it, which a real-data-dir write would not produce either — so the
        // load-bearing half is the next assertion, that no path is even chosen.
        let dir = temp_dir();
        record("must not be written anywhere");
        assert!(
            !dir.join(FILE_NAME).exists(),
            "an unarmed channel writes no file"
        );
        assert!(
            effective_dir().is_none(),
            "an unarmed channel resolves NO target directory at all — a lazy \
             `dirs::data_dir()` default would satisfy the assertion above while \
             writing into the real user data dir"
        );
    }

    /// The pure seam is testable against an explicit dir with the global target
    /// untouched — the reason `append_to` takes a path rather than reading the
    /// global (the `debuglog` precedent, and the same race it avoids).
    #[test]
    fn append_to_takes_its_target_explicitly() {
        let _g = lock();
        let dir = temp_dir();
        init(None); // the global channel is OFF…
        append_to(dir.as_ref(), "written through the pure seam");
        assert!(
            dir.join(FILE_NAME).exists(),
            "…yet the explicit-dir seam still writes, so no test has to arm the global"
        );
    }
}

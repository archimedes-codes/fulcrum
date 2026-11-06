//! The daemon's `WorkerFactory` (T9 Task 5) — the one place the daemon decides
//! WHICH executable a session's Worker is.
//!
//! # Why this is a struct and not a closure
//!
//! [`WorkerFactory`] is the engine's spawn seam and the engine already owns
//! everything about spawning that is not the path: `WorkerHandle::spawn` builds
//! the argv (`--worker`), scrubs the AppImage env patch, pipes the stdio trio,
//! puts the child in its own process group, and starts the reader tasks. So the
//! daemon's whole contribution is `exe`, and the reason it is a named type
//! rather than an `Arc<dyn Fn>` is that the path has a provenance the operator
//! can get wrong — a typo'd `--worker-exe` that silently falls back to
//! `current_exe()` makes the daemon spawn ITSELF as its own worker, recursively.
//! A type with a constructor is where that decision, and its test, live.
//!
//! # Where `exe` comes from
//!
//! `--worker-exe <path>` if the operator gave one, else
//! [`std::env::current_exe`]. The fallback is correct and not a convenience:
//! `WorkerHandle::spawn`'s own documented production caller is
//! `current_exe() + --worker` — the self-exec — and the daemon's `--worker` arm
//! is in the same binary, so the default is "spawn a copy of me", which is
//! exactly what the desktop does.
//!
//! The Tauri **sidecar** case (the Worker is a differently-named sibling binary
//! next to the app) is T12's problem and needs no code here: it is what
//! `--worker-exe` is for.

use std::path::PathBuf;

use fulcrum_core::worker::client::{WorkerError, WorkerHandle};
use fulcrum_core::worker::WorkerFactory;

/// The daemon's factory: `spawn` is `WorkerHandle::spawn(&self.exe)`, nothing else.
///
/// One instance per daemon, shared as `Arc<dyn WorkerFactory>` into
/// [`WorkerManager::new`](fulcrum_core::worker::WorkerManager::new). It is
/// `Send + Sync` by construction (`PathBuf`), and it holds no state — the
/// registry of live Workers is `WorkerManager`'s, and there is deliberately no
/// second one here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonWorkerFactory {
    exe: PathBuf,
}

/// The one error [`DaemonWorkerFactory::resolve`] can raise: the environment
/// could not name its own executable.
#[derive(Debug, thiserror::Error)]
pub enum WorkerExeError {
    /// No `--worker-exe` was given and `current_exe()` failed.
    ///
    /// Reported rather than defaulted: the alternative is spawning a Worker from
    /// a path that does not exist and surfacing it as a per-`start` spawn failure,
    /// which reads as "the worker is broken" when the actual problem is that the
    /// daemon cannot find itself.
    #[error("no --worker-exe was given and the daemon cannot locate its own executable: {0}")]
    NoCurrentExe(String),
}

impl DaemonWorkerFactory {
    /// The explicit form (the test seam, and what
    /// [`resolve`](Self::resolve) lands on once the path is decided).
    pub fn new(exe: impl Into<PathBuf>) -> Self {
        Self { exe: exe.into() }
    }

    /// The executable this factory spawns. The daemon logs it at startup: a
    /// session that "will not start" is usually this path being wrong.
    pub fn exe(&self) -> &std::path::Path {
        &self.exe
    }

    /// Decide the Worker executable from the `--worker-exe` value.
    ///
    /// The `Option` is the CLI's: `Some` = the operator named one, and it is
    /// used VERBATIM — not canonicalized, not checked for existence. Two reasons
    /// a check here would be wrong:
    ///
    /// * a path that does not exist yet can become valid (a sidecar unpacked
    ///   after the daemon starts — the desktop's T12 flow), and refusing to boot
    ///   over it is worse than reporting the failed spawn where it belongs, in
    ///   the `start-result` of the session that asked for it;
    /// * `Path::canonicalize` on Windows prepends a `\\?\` verbatim prefix, and
    ///   this string ends up in spawn errors an operator reads — re-writing it
    ///   here would put a prefix nobody typed into their log line.
    ///
    /// `None` falls back to `current_exe()` (see the module docs).
    pub fn resolve(explicit: Option<PathBuf>) -> Result<Self, WorkerExeError> {
        if let Some(exe) = explicit {
            return Ok(Self::new(exe));
        }
        std::env::current_exe()
            .map(Self::new)
            .map_err(|e| WorkerExeError::NoCurrentExe(e.to_string()))
    }
}

impl WorkerFactory for DaemonWorkerFactory {
    /// Spawn `<exe> --worker` for one session.
    ///
    /// Deliberately a one-line delegation. `WorkerHandle::spawn` is the engine's
    /// only child constructor and the census in `childproc.rs` exists to keep it
    /// the only one; a daemon-side `Command::new(exe).arg("--worker")` here would
    /// silently skip the env scrub, the process-group `pre_exec`, and the stderr
    /// drain, and nothing outside this line would notice for weeks.
    fn spawn(&self) -> Result<WorkerHandle, WorkerError> {
        WorkerHandle::spawn(&self.exe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An explicit `--worker-exe` wins, VERBATIM.
    ///
    /// The verbatim half is the assertion: a factory that canonicalized would
    /// pass a "returns the path I gave it" test written with a relative path and
    /// then hand the operator a `\\?\`-prefixed Windows path in every spawn error.
    #[test]
    fn an_explicit_worker_exe_is_used_untouched() {
        let explicit = PathBuf::from("./somewhere/else/not-a-real-binary");
        let factory = DaemonWorkerFactory::resolve(Some(explicit.clone())).expect("resolves");
        assert_eq!(
            factory.exe(),
            explicit.as_path(),
            "`--worker-exe` must reach the spawn call unmodified"
        );
        // A path that does not exist is NOT a resolution failure — see
        // `resolve`'s doc: the failure belongs in the `start-result`.
        assert!(
            !factory.exe().exists(),
            "the fixture path must stay nonexistent for this test to mean anything"
        );
    }

    /// With no flag, the factory spawns THIS executable — the self-exec, which is
    /// what makes `fulcrum --worker` a valid `--worker-exe` value.
    ///
    /// The assertion is equality with `current_exe()`, not "some path exists":
    /// the claim is that the default is the daemon's own binary, and a default
    /// that quietly became `PathBuf::from("fulcrum")` (PATH-resolved, i.e. a
    /// DIFFERENT build, or none) would still "exist" under the latter spelling.
    #[test]
    fn no_flag_falls_back_to_the_daemons_own_executable() {
        let factory = DaemonWorkerFactory::resolve(None).expect("current_exe is available here");
        assert_eq!(
            factory.exe(),
            std::env::current_exe()
                .expect("a test binary has a path")
                .as_path(),
            "the default Worker is a copy of the daemon itself (`--worker` self-exec)"
        );
        assert!(factory.exe().exists());
    }

    /// The factory is usable as the engine's trait object (the type
    /// `WorkerManager::new` takes), and a bad `exe` surfaces as an ERROR.
    ///
    /// A compile-time check that the trait object forms, plus the runtime claim
    /// that matters to the daemon: `spawn` on a nonexistent executable is an
    /// `Err`, not a panic and not a silent success — `serve::start` turns exactly
    /// that `Err` into a `start-result` with `ok: false`, so a typo'd
    /// `--worker-exe` is one clear frame to the client instead of a session that
    /// never produces anything.
    ///
    /// ⚠ The variant is `Io`, NOT `SpawnFailed`. The WIP this task inherited
    /// asserted `SpawnFailed`, and that claim is false of the engine:
    /// `WorkerHandle::spawn` maps a failed `Command::spawn` to
    /// `WorkerErrorKind::Io(format!("spawn the worker: {e}"))`
    /// (`worker/client.rs`), and `SpawnFailed` is constructed ONLY by hand in
    /// test doubles (the `NeverSpawns` factory in
    /// `worker/manager/subagent_flow.rs`'s tests). Matching on it here would
    /// have been an assertion about a variant the production path never returns.
    /// What the daemon can actually rely on is the `Display` text, which is what
    /// `start_result` forwards to the client — so that is what is asserted.
    #[test]
    fn the_factory_is_the_engines_spawn_seam() {
        let factory: std::sync::Arc<dyn WorkerFactory> =
            std::sync::Arc::new(DaemonWorkerFactory::new("definitely-not-a-binary"));
        // `.err()` rather than `.expect_err()`: `WorkerHandle` holds the child's
        // stdio pipes and is deliberately not `Debug`, so `expect_err`'s
        // `T: Debug` bound is not satisfiable — and turning that into a
        // `Debug` impl on the engine's handle to make a daemon test compile is
        // editing a frozen file for a test-harness convenience.
        let err = factory
            .spawn()
            .err()
            .expect("a nonexistent executable cannot be spawned");
        let text = err.to_string();
        assert!(
            text.contains("spawn the worker") && text.contains("No such file"),
            "the error the client sees must name the spawn failure and why, \
             got {text:?} — `serve::start` forwards this string verbatim into \
             the `start-result`'s `error`"
        );
    }
}

//! The ONE place a session's root sets are computed (ADR 0030).
//!
//! A boundary is a SET of directories, not a single `cwd`. It has to be:
//! `skills::space_roots` walks UP from `cwd` to the repo root, so for
//! `cwd = /repo/packages/app` the root `/repo/.agents/skills` is outside the
//! session `cwd` yet must be readable. Likewise the user-level discovery
//! dirs (`~/.agents/skills`, …) are by definition outside every session.
//!
//! Two consumers, one source of truth:
//! * the GATE (Tasks 3–5) asks "is this path beyond the boundary?", and
//! * the EXECUTOR ([`crate::agent::fs_backend::FsBackend`]) enforces the
//!   `Sandboxed`-tier floor.
//!
//! The check itself is NOT reimplemented here — it is ADR 0029's
//! canonicalize-then-check logic in `FsBackend` (proved sound by the attack
//! matrix in `tests/skill_tools_boundary.rs`); this module only decides
//! WHICH roots go into it.

use std::path::{Path, PathBuf};

use crate::agent::policy::AccessPolicy;

/// The single root-entry meaning "no containment" (the `Ask`/`Allow`
/// floor): every canonical absolute path is under `/`. Harness is
/// Linux-only (exec.rs:9); on other targets the code compiles but the
/// harness never runs there.
pub const UNRESTRICTED: &str = "/";

/// Canonicalize `p`, dropping it when it fails (a missing dir is simply not
/// part of the boundary — the boundary is allowed to name dirs that do not
/// exist, and a nonexistent dir contains nothing).
fn canon(p: &Path) -> Option<PathBuf> {
    p.canonicalize().ok()
}

/// Append the canonicalized paths of a `user_roots` / `space_roots` result
/// (the scope tag is dropped — the boundary is a set of paths), skipping
/// duplicates and dirs that do not exist. Generic over the scope enum
/// (`SkillScope` vs `AgentScope`).
fn push_roots<T>(out: &mut Vec<PathBuf>, roots: Vec<(PathBuf, T)>) {
    for (path, _scope) in roots {
        if let Some(c) = canon(&path) {
            if !out.contains(&c) {
                out.push(c);
            }
        }
    }
}

/// The read boundary: `cwd` FIRST, then the discovery roots (user + space
/// level, skills + agents). Every root is canonicalized; a root that fails
/// to canonicalize (a missing dir) is simply not part of the boundary.
/// `roots[0]` is always the canonical `cwd` — the relative-path join base.
pub fn read_roots(cwd: &Path) -> Vec<PathBuf> {
    // The `cwd` goes FIRST and alone decides the join base: if it cannot be
    // canonicalized there is no boundary to speak of, so return empty (the
    // `FsBackend` treats an empty root set as an error — fail closed).
    let base = match canon(cwd) {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut out = vec![base.clone()];
    push_roots(&mut out, crate::skills::user_roots());
    push_roots(&mut out, crate::agents::user_roots());
    push_roots(&mut out, crate::skills::space_roots(&base));
    push_roots(&mut out, crate::agents::space_roots(&base));
    out
}

/// The write boundary (the gate's notion of "beyond" for writes, and the
/// `Sandboxed` write floor): the canonical `cwd` ONLY. Space-level agent
/// dirs are repo content and stay writable; the user-level dirs are
/// handled by [`protected_dirs`].
pub fn write_roots(cwd: &Path) -> Vec<PathBuf> {
    canon(cwd).map(|c| vec![c]).unwrap_or_default()
}

/// The write DENY-LIST (ADR 0030, Deviation 3): the four user-level
/// discovery dirs — `~/.agents/skills`, `~/.pi/agent/skills`,
/// `~/.agents/agents`, `~/.pi/agent/agents`. A write whose resolved path
/// is under one of these is refused in EVERY policy (a deny, never a
/// prompt, never widened by Trust): a model that can rewrite
/// `~/.agents/skills/*/SKILL.md` rewrites its own instructions for every
/// future session. Canonicalized; a missing dir is dropped.
pub fn protected_dirs() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    push_roots(&mut out, crate::skills::user_roots());
    push_roots(&mut out, crate::agents::user_roots());
    out
}

/// The executor's root set for one direction, derived from the policy.
/// `Sandboxed` → the direction's boundary (the gate already denied
/// beyond-boundary accesses; the executor is the fail-closed backstop).
/// `Ask`/`Allow` → `[cwd, UNRESTRICTED]`: the gate is the only decision
/// point, and an approved `Ask` must be able to land. `roots[0]` is the
/// relative-path join base in both cases.
pub fn executor_roots(cwd: &Path, boundary: &[PathBuf], policy: AccessPolicy) -> Vec<PathBuf> {
    match policy {
        AccessPolicy::Sandboxed => boundary.to_vec(),
        // The widening: the gate has already decided, so the executor must
        // not re-decide. `/` is a root like any other — the canonicalization
        // and the symlink / `..` slow paths still run, they just never
        // reject on containment.
        AccessPolicy::Ask | AccessPolicy::Allow => {
            let base = canon(cwd).unwrap_or_else(|| cwd.to_path_buf());
            vec![base, PathBuf::from(UNRESTRICTED)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::policy::AccessPolicy::*;

    /// A fresh temp dir (removed on drop).
    struct Tmp(PathBuf);
    impl Tmp {
        fn new(tag: &str) -> Self {
            let d = std::env::temp_dir().join(format!("boundary-{tag}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&d).unwrap();
            Self(d)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// `HOME` restore guard (the `test_support` `ENV_LOCK` pattern — copy of
    /// the private one in loop.rs's test module; the restore happens even
    /// when an assertion panics).
    struct RestoreHome(Option<std::ffi::OsString>);
    impl Drop for RestoreHome {
        fn drop(&mut self) {
            match self.0.take() {
                // SAFETY: the `ENV_LOCK` is still held at drop time (this
                // guard is declared after the `_lock` guard and outlives it
                // in reverse); no other thread mutates HOME concurrently.
                Some(v) => unsafe { std::env::set_var("HOME", v) },
                // SAFETY: as above.
                None => unsafe { std::env::remove_var("HOME") },
            }
        }
    }

    /// Pin `$HOME` to a fresh temp dir holding the four user-level discovery
    /// dirs. Returns (env lock, restore guard, temp home) — bind ALL THREE
    /// to non-underscore names so they live until the end of the test (a
    /// name starting with `_` is dropped immediately, which would release
    /// the lock and skip the restore).
    fn pin_home(tag: &str) -> (std::sync::MutexGuard<'static, ()>, RestoreHome, Tmp) {
        let lock = crate::test_support::env_lock();
        let home = Tmp::new(tag);
        for dir in [
            ".agents/skills",
            ".pi/agent/skills",
            ".agents/agents",
            ".pi/agent/agents",
        ] {
            std::fs::create_dir_all(home.path().join(dir)).unwrap();
        }
        let original = std::env::var_os("HOME");
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span; no other thread mutates HOME concurrently.
        unsafe { std::env::set_var("HOME", home.path()) };
        (lock, RestoreHome(original), home)
    }

    /// A temp `$HOME` with NO discovery dirs at all.
    fn pin_empty_home(tag: &str) -> (std::sync::MutexGuard<'static, ()>, RestoreHome, Tmp) {
        let lock = crate::test_support::env_lock();
        let home = Tmp::new(tag);
        let original = std::env::var_os("HOME");
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span; no other thread mutates HOME concurrently.
        unsafe { std::env::set_var("HOME", home.path()) };
        (lock, RestoreHome(original), home)
    }

    #[test]
    fn read_roots_puts_the_canonical_cwd_first_and_includes_the_user_dirs() {
        let (lock, restore, home) = pin_home("read-roots");
        let cwd = Tmp::new("read-roots-cwd");
        let roots = read_roots(cwd.path());
        assert!(
            !roots.is_empty(),
            "a real cwd always yields at least itself"
        );
        // `roots[0]` is the relative-path join base — the canonical `cwd`.
        assert_eq!(roots[0], cwd.path().canonicalize().unwrap());
        // The four user-level dirs are all in the read boundary.
        for dir in [
            ".agents/skills",
            ".pi/agent/skills",
            ".agents/agents",
            ".pi/agent/agents",
        ] {
            let want = home.path().join(dir).canonicalize().unwrap();
            assert!(roots.contains(&want), "missing {dir:?} in {roots:?}");
        }
        // And NOTHING else leaked in — the boundary is a set of discovery
        // dirs, not `$HOME` itself (a root of `$HOME` would make the whole
        // home readable).
        let home_canon = home.path().canonicalize().unwrap();
        assert!(
            !roots.contains(&home_canon),
            "the boundary must never be $HOME itself: {roots:?}"
        );
        drop((restore, lock));
    }

    /// The ADR 0030 motivation: a space-level skill dir at the REPO ROOT is
    /// outside the session `cwd` yet inside the read boundary.
    #[test]
    fn read_roots_includes_a_repo_root_skill_dir_outside_the_cwd() {
        let (lock, restore, _home) = pin_home("space-roots");
        let repo = Tmp::new("space-repo");
        std::fs::create_dir_all(repo.path().join(".git")).unwrap();
        let pkg = repo.path().join("packages/app");
        std::fs::create_dir_all(&pkg).unwrap();
        std::fs::create_dir_all(repo.path().join(".agents/skills")).unwrap();
        std::fs::create_dir_all(repo.path().join(".pi/agents")).unwrap();
        let roots = read_roots(&pkg);
        assert!(
            roots.contains(&repo.path().join(".agents/skills").canonicalize().unwrap()),
            "the repo-root skill dir must be readable from a package cwd: {roots:?}"
        );
        assert!(
            roots.contains(&repo.path().join(".pi/agents").canonicalize().unwrap()),
            "the repo-root agent dir too: {roots:?}"
        );
        drop((restore, lock));
    }

    #[test]
    fn a_nonexistent_root_is_dropped() {
        let (lock, restore, home) = pin_empty_home("dropped");
        let cwd = Tmp::new("dropped-cwd");
        // The four user dirs do not exist, so the boundary is JUST the cwd.
        let roots = read_roots(cwd.path());
        assert_eq!(roots, vec![cwd.path().canonicalize().unwrap()]);
        // And a cwd that does not exist yields NO boundary at all (the
        // `FsBackend` then fails closed rather than joining to a phantom).
        let ghost = home.path().join("never-created");
        assert!(read_roots(&ghost).is_empty());
        drop((restore, lock));
    }

    #[test]
    fn write_roots_is_only_the_canonical_cwd() {
        let (lock, restore, _home) = pin_home("write-roots");
        let cwd = Tmp::new("write-roots-cwd");
        assert_eq!(
            write_roots(cwd.path()),
            vec![cwd.path().canonicalize().unwrap()]
        );
        // A nested cwd's write boundary is JUST itself — the repo-level
        // `.agents/skills` is NOT a write root of a package cwd.
        let nested = cwd.path().join("packages/app");
        std::fs::create_dir_all(cwd.path().join(".agents/skills")).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(write_roots(&nested), vec![nested.canonicalize().unwrap()]);
        drop((restore, lock));
    }

    #[test]
    fn protected_dirs_are_the_four_user_level_dirs() {
        let (lock, restore, home) = pin_home("protected");
        let dirs = protected_dirs();
        let want: Vec<PathBuf> = [
            ".agents/skills",
            ".pi/agent/skills",
            ".agents/agents",
            ".pi/agent/agents",
        ]
        .iter()
        .map(|d| home.path().join(d).canonicalize().unwrap())
        .collect();
        assert_eq!(dirs, want);
        drop((restore, lock));
    }

    /// The deny-list is USER-level only: a repo's `.agents/skills` is repo
    /// content and stays writable in every policy.
    #[test]
    fn protected_dirs_never_include_a_space_level_dir() {
        let (lock, restore, _home) = pin_home("protected-space");
        let repo = Tmp::new("protected-space-repo");
        std::fs::create_dir_all(repo.path().join(".agents/skills")).unwrap();
        let dirs = protected_dirs();
        assert!(
            !dirs
                .iter()
                .any(|p| p.starts_with(repo.path().canonicalize().unwrap())),
            "a repo dir must never be on the deny-list: {dirs:?}"
        );
        drop((restore, lock));
    }

    #[test]
    fn protected_dirs_drop_a_missing_dir() {
        let (lock, restore, _home) = pin_empty_home("protected-missing");
        assert!(protected_dirs().is_empty());
        drop((restore, lock));
    }

    #[test]
    fn executor_roots_uses_the_boundary_only_when_sandboxed() {
        let cwd = Tmp::new("exec-cwd");
        let other = Tmp::new("exec-other");
        let base = cwd.path().canonicalize().unwrap();
        let boundary = vec![base.clone(), other.path().canonicalize().unwrap()];

        // `Sandboxed` → exactly the boundary (the fail-closed backstop).
        assert_eq!(executor_roots(cwd.path(), &boundary, Sandboxed), boundary);

        // `Ask` / `Allow` → `[cwd, "/"]`: the gate is the only decision
        // point and `roots[0]` stays the join base.
        for policy in [Ask, Allow] {
            let roots = executor_roots(cwd.path(), &boundary, policy);
            assert_eq!(
                roots,
                vec![base.clone(), PathBuf::from(UNRESTRICTED)],
                "{policy:?}"
            );
        }
    }

    /// The unrestricted floor really is unrestricted: every canonical
    /// absolute path is under `/`, so an approved `Ask` can land — while the
    /// SAME path is rejected by the `Sandboxed` boundary (the contrast that
    /// keeps this test from passing vacuously).
    #[test]
    fn the_unrestricted_root_contains_every_absolute_path() {
        use crate::agent::fs_backend::FsBackend;
        let a = Tmp::new("floor-a");
        let b = Tmp::new("floor-b");
        let target = b.path().join("x.txt");
        std::fs::write(&target, "x").unwrap();
        // `b` is NOT under `a` (both are siblings under the temp dir).
        let sandboxed = FsBackend {
            roots: vec![a.path().to_path_buf()],
        };
        assert!(
            sandboxed.validate(&target).is_err(),
            "premise: `b` is outside `a`"
        );
        let unrestricted = FsBackend {
            roots: vec![a.path().to_path_buf(), PathBuf::from(UNRESTRICTED)],
        };
        assert_eq!(
            unrestricted.validate(&target).unwrap(),
            target.canonicalize().unwrap(),
            "`/` must contain every absolute path"
        );
    }
}

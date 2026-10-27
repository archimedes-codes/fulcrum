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

/// The root-entry meaning "no containment" on UNIX (one floor of the
/// `Ask`/`Allow` widening): every canonical absolute path is under `/`.
///
/// This is NOT a portable floor, and the "harness is Linux-only, so it
/// never matters" claim that used to live here is RETIRED: the harness runs
/// on Windows and macOS now (CI runs it there). On Windows a canonical path
/// is always under a DRIVE root in the verbatim form `canonicalize()`
/// produces (`\\?\C:\…`), while `canonicalize("/")` resolves to the CURRENT
/// drive's root only — so a lone `/` mismatches any path on another drive
/// (CI's evidence: `read: path escapes the session sandbox:
/// \\?\C:\Users\runneradmin\AppData\Local\Temp\…\secret.txt`). Use
/// [`unrestricted_floor`] for the floor; this const is its unix half, and is
/// kept because it names the concept the ADR text and `fs_backend` docs use.
pub const UNRESTRICTED: &str = "/";

/// The `Ask`/`Allow` floor as a ROOT SET: "no containment", honestly, on
/// every target the harness runs on.
///
/// * unix — `["/"]` ([`UNRESTRICTED`]): one root contains every path.
/// * Windows — one entry per MOUNTED DRIVE ROOT (`C:\`, `D:\`, …), each
///   canonicalized, so every entry is in the verbatim `\\?\C:\` form and
///   `FsBackend::check_under_roots`'s `starts_with` compares like-for-like
///   against a canonicalized candidate. `FsBackend` treats an EMPTY root set
///   as an error rather than as "no limits", so a floor has to name real,
///   existing dirs: an unmounted drive fails its stat and is dropped.
///
/// Why this shape, and not the two alternatives:
/// * `std::fs::roots()` is the obvious single-call source of exactly these
///   roots and is often cited as "stable since 1.80" — it is NOT callable
///   here: on the pinned 1.99.0 toolchain it is still feature-gated (`std::fs`
///   has no `roots`). If it stabilizes on a future pinned toolchain this
///   function is a one-line swap.
/// * deriving the drive root from the CANDIDATE path's `Prefix` component at
///   check time would need no enumeration, but it means teaching
///   `FsBackend::check_under_roots` about floors — that is the containment
///   code the ADR 0030 attack matrix pins, and loosening it is exactly what
///   must not happen. Keeping the widening here means the floor stays "just
///   another root" and the `Sandboxed` path is untouched by construction.
///
/// Cost: up to 24 `metadata` probes + one canonicalize per mounted drive, on
/// the widened (`Ask`/`Allow`) path only. Not cached on purpose — a cached
/// list would go stale the moment a user mounts a drive and silently
/// re-break the floor.
///
/// Known gap, stated rather than papered over: a UNC share (`\\host\share\…`)
/// is under no drive root, so an `Ask`-approved access to a UNC path still
/// hits this floor and is refused on Windows. A share-level root list is out
/// of scope here.
fn unrestricted_floor() -> Vec<PathBuf> {
    #[cfg(not(windows))]
    {
        vec![PathBuf::from(UNRESTRICTED)]
    }
    #[cfg(windows)]
    {
        // `A:`/`B:` are skipped: legacy floppy designations no supported
        // Windows install mounts, and the probes with a history of stalling.
        (b'C'..=b'Z')
            .filter_map(|letter| {
                let drive = PathBuf::from(format!("{}:\\", letter as char));
                // Probe before canonicalizing: `metadata` is the cheap
                // attribute query, and only drives that answer get a handle
                // opened for the verbatim path.
                drive.metadata().ok().map(|_| drive)
            })
            .filter_map(|drive| canon(&drive))
            .collect()
    }
}

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

/// Append the paths of a `user_roots` result to the write DENY-LIST,
/// canonicalized LENIENTLY: a directory that does not exist yet is KEPT and
/// contributes its canonical nearest-existing ancestor plus the components
/// that lead to it. This is the reverse of [`push_roots`], deliberately, and
/// it is a separate function rather than a flag on that one because the flag
/// would select a SECURITY policy: a lenient READ root silently WIDENS what
/// is readable, and a seventh `push_roots` call could opt into it by
/// accident. Here the leniency only ever DENIES more. A path that cannot be
/// resolved at all (`$HOME` relative and absent, a protected dir on an
/// unmounted drive) comes back with its raw spelling — see
/// `protected_dirs_keeps_an_unresolvable_entry_raw`.
fn push_protected<T>(out: &mut Vec<PathBuf>, roots: Vec<(PathBuf, T)>) {
    for (path, _scope) in roots {
        let c = crate::agent::fs_backend::canonicalize_lenient(&path);
        if !out.contains(&c) {
            out.push(c);
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
/// future session.
///
/// Entries are canonicalized LENIENTLY (see `push_protected`), which
/// REPLACES the sentence this task reversed — *"Canonicalized; a missing dir
/// is dropped."* (issue #92). A missing dir is now KEPT as its canonical
/// nearest-existing ancestor plus the components that lead to it, because
/// this list is frozen for the Session's life (`AgentLoop::new`) and a
/// dropped entry silently unprotects the agent's own instructions until
/// restart. The READ roots keep the opposite, strict rule on purpose —
/// leniency there would widen what is readable; see `read_roots` and
/// `a_nonexistent_root_is_dropped`.
///
/// The leniency is only half the protection: the comparison that consumes
/// these entries must canonicalize its ENTRY side leniently too, or a
/// still-missing entry matches nothing and this becomes a silent no-op — see
/// `crate::agent::tools::exec::protected_refusal_text` and
/// `a_write_into_a_still_missing_protected_dir_is_refused`.
pub fn protected_dirs() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    push_protected(&mut out, crate::skills::user_roots());
    push_protected(&mut out, crate::agents::user_roots());
    out
}

/// The executor's root set for one direction, derived from the policy.
/// `Sandboxed` → the direction's boundary (the gate already denied
/// beyond-boundary accesses; the executor is the fail-closed backstop).
/// `Ask`/`Allow` → `[cwd, unrestricted_floor()]`: the gate is the only
/// decision point, and an approved `Ask` must be able to land. `roots[0]` is
/// the relative-path join base in both cases.
pub fn executor_roots(cwd: &Path, boundary: &[PathBuf], policy: AccessPolicy) -> Vec<PathBuf> {
    match policy {
        AccessPolicy::Sandboxed => boundary.to_vec(),
        // The widening: the gate has already decided, so the executor must
        // not re-decide. The floor roots are roots like any other — the
        // canonicalization and the symlink / `..` slow paths still run, they
        // just never reject on containment. On unix the floor is one `/`; on
        // Windows it is the mounted drive roots, because a lone `/` there is
        // only the current drive (see [`unrestricted_floor`]). ONLY this arm
        // widens — the `Sandboxed` arm hands back the caller's real boundary
        // untouched, so the ADR 0030 containment strictness is unchanged.
        AccessPolicy::Ask | AccessPolicy::Allow => {
            let base = canon(cwd).unwrap_or_else(|| cwd.to_path_buf());
            let mut roots = vec![base];
            roots.extend(unrestricted_floor());
            roots
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::policy::AccessPolicy::*;

    /// A fresh temp dir (removed on drop — the removal belongs to the shared
    /// `test_support` guard; this only fixes the BASE and the PREFIX, so #96's
    /// per-prefix census keeps attributing these dirs here).
    struct Tmp(crate::test_support::ScratchDir);
    impl Tmp {
        fn new(tag: &str) -> Self {
            Self(crate::test_support::ScratchDir::in_env_temp(
                "boundary-",
                tag,
            ))
        }
        fn path(&self) -> &Path {
            self.0.path()
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

    /// (issue #92, D3 as amended) The DENY-LIST KEEPS a directory that does
    /// not exist yet — this test REVERSES the sentence deleted from
    /// `protected_dirs`'s doc (*"Canonicalized; a missing dir is dropped."*).
    ///
    /// Why the direction changed: a dropped entry silently UNPROTECTS the
    /// agent's own instructions for the whole life of the Session, because
    /// the list is frozen at `AgentLoop::new` — a user who recreates
    /// `~/.pi/agent/agents` mid-session could write into it until restart,
    /// which is exactly what #92 was filed for.
    ///
    /// The expectation is spelled through `canonicalize()` (the canonical
    /// NEAREST EXISTING ANCESTOR, here `$HOME`, plus the re-applied trailing
    /// components), never through the raw temp path: on macOS `temp_dir()`
    /// lives under `/var` → `/private/var`, so a raw expectation would be a
    /// platform accident. Same reason
    /// `protected_dirs_are_the_four_user_level_dirs` canonicalizes its `want`.
    ///
    /// The READ side deliberately keeps the opposite rule — see
    /// `a_nonexistent_root_is_dropped`, which pins that a missing root is
    /// still dropped from the boundary because a lenient read root would
    /// WIDEN what is readable.
    #[test]
    fn protected_dirs_keep_a_missing_dir() {
        let (lock, restore, home) = pin_empty_home("protected-missing");
        let home_canon = home.path().canonicalize().unwrap();
        let protected = protected_dirs();
        let want: Vec<PathBuf> = [
            ".agents/skills",
            ".pi/agent/skills",
            ".agents/agents",
            ".pi/agent/agents",
        ]
        .iter()
        .map(|d| home_canon.join(d))
        .collect();
        assert_eq!(
            protected, want,
            "a not-yet-existing protected dir is KEPT as the canonical nearest \
             existing ancestor plus its trailing components"
        );
        // The direction of the change, on its own: every entry stays INSIDE
        // the pinned home, so a lenient peel can never leak an entry out of
        // the test's own tree (and never out of the user's `$HOME`).
        assert!(
            protected.iter().all(|p| p.starts_with(&home_canon)),
            "the entries must stay inside $HOME: {protected:?}"
        );
        drop((restore, lock));
    }

    /// (issue #92, D3's RECORDED RESIDUE) When nothing on a protected dir's
    /// path can be resolved at all, `canonicalize_lenient` hands the path
    /// back UNCHANGED — and the entry is still KEPT, which is the "strictly
    /// better than today's outright drop" half of the residue.
    ///
    /// WHAT THIS PROVES (the RED half, before the fix): the four entries
    /// SURVIVE with their RAW spelling where no ancestor resolves, so the
    /// Session still carries a deny-list where today it carried NONE.
    ///
    /// WHAT IT DOES NOT PROVE (the GREEN half, before AND after the fix — a
    /// pin, deliberately not a failure, so the next reader does not "fix" it
    /// into one): a RAW entry cannot prefix-match a CANONICAL resolved path,
    /// so a write into such a dir is NOT refused. That is the residue D3
    /// names, stated as an assertion so it cannot be quietly forgotten. It is
    /// not fixed here because there is nothing to peel to.
    ///
    /// HOW THE MECHANISM IS REACHED: two ways in production — a `$HOME` set
    /// to a RELATIVE path that does not exist relative to the process `cwd`,
    /// and on Windows a `USERPROFILE` on a drive that is not currently
    /// mounted (`Path::pop` on `Z:\` returns `false`). On unix an ABSOLUTE
    /// `$HOME` always peels to at least `/`, so the "no ancestor at all" `Err`
    /// arm is unreachable from it — this relative fixture is how the
    /// raw-return path gets exercised at all. Note the fixture asserts the
    /// RAW spelling rather than asking "did the result come back equal to the
    /// input?": with an absent `~/.pi` and a present absolute `$HOME` the peel
    /// SUCCEEDS at `$HOME` and re-appends three components, and on Linux that
    /// re-applied spelling is textually identical to the input, so such a
    /// check would read as "raw" and test nothing.
    ///
    /// WHY A RELATIVE `$HOME` IS SAFE HERE, and what would break it: the
    /// unlocked readers of `$HOME` in this suite are `skills::home_dir`
    /// (production, and the `user_roots` feeder), `sandbox.rs::git_config_files`,
    /// `sandbox.rs::private_dir_outside_the_read_allow_list`, and
    /// `tests/attention_alert_survives.rs::dirs_home`. Every one either
    /// canonicalizes and discards on failure (the two `sandbox.rs` readers — a
    /// relative, absent `$HOME` just drops one candidate from a candidate
    /// list) or uses the value for a file-existence probe whose answer is
    /// already "absent" in CI (`attention_alert_survives`, which gates a skip).
    /// A future UNLOCKED reader that JOINS onto `$HOME` and writes would be
    /// the one to break. And no `set_current_dir`: the suite runs
    /// multi-threaded and `cwd` is process-global — this test's assertion
    /// needs no `cwd`, because an absent relative path fails to canonicalize
    /// from any working directory.
    #[test]
    fn protected_dirs_keeps_an_unresolvable_entry_raw() {
        let lock = crate::test_support::env_lock();
        // A relative `$HOME` that exists NOWHERE (no dir is created — that is
        // the point). The name only has to be absent; a uuid keeps it from
        // colliding with anything a developer actually has in their cwd.
        let ghost = format!("archimedes-t06-ghost-{}", uuid::Uuid::new_v4());
        let original = std::env::var_os("HOME");
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span; no other thread mutates HOME concurrently.
        unsafe { std::env::set_var("HOME", &ghost) };
        let restore = RestoreHome(original);

        let protected = protected_dirs();
        let want: Vec<PathBuf> = [
            ".agents/skills",
            ".pi/agent/skills",
            ".agents/agents",
            ".pi/agent/agents",
        ]
        .iter()
        .map(|d| std::path::PathBuf::from(ghost.as_str()).join(d))
        .collect();
        assert_eq!(
            protected, want,
            "an entry whose whole path fails to resolve is KEPT with its raw \
             spelling — the session still gets a deny-list (today it gets none)"
        );

        // THE RESIDUE (green before and after the fix): a raw entry cannot
        // prefix-match a CANONICAL write target, so `protected_refusal_text`
        // answers `None` for a canonical path elsewhere. On macOS
        // (`/var` → `/private/var`) or Windows (`\\?\C:\…`) a write into such
        // a dir is therefore NOT refused. Recorded, not hidden.
        let target = Tmp::new("unresolvable-target");
        let canonical_target = target.path().canonicalize().unwrap().join("SKILL.md");
        assert_eq!(
            crate::agent::tools::exec::protected_refusal_text(
                &protected,
                "write",
                &canonical_target
            ),
            None,
            "the recorded residue: a raw entry does not refuse a canonical \
             target — {canonical_target:?} vs {protected:?}"
        );

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

        // `Ask` / `Allow` → `[cwd, <the floor>]`: the gate is the only
        // decision point and `roots[0]` stays the join base. The floor's
        // CONTENT is platform-shaped by design (one `/` on unix, the mounted
        // drive roots on Windows), so the pin is: the base, plus exactly
        // whatever [`unrestricted_floor`] contributes — nothing else.
        for policy in [Ask, Allow] {
            let roots = executor_roots(cwd.path(), &boundary, policy);
            assert_eq!(roots[0], base, "{policy:?}: roots[0] is the join base");
            assert_eq!(&roots[1..], unrestricted_floor().as_slice(), "{policy:?}");
            assert!(
                roots.len() >= 2,
                "{policy:?}: the floor is explicit, never an empty set"
            );
        }
    }

    /// The unrestricted floor really is unrestricted: every canonical
    /// absolute path is under it, so an approved `Ask` can land — while the
    /// SAME path is rejected by the `Sandboxed` boundary (the contrast that
    /// keeps this test from passing vacuously). The floor comes from
    /// [`unrestricted_floor`], so this asserts the CLAIM cross-platform
    /// rather than a hardcoded unix `/`.
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
        let mut floor_roots = vec![a.path().to_path_buf()];
        floor_roots.extend(unrestricted_floor());
        let unrestricted = FsBackend { roots: floor_roots };
        assert_eq!(
            unrestricted.validate(&target).unwrap(),
            target.canonicalize().unwrap(),
            "the floor must contain every absolute path"
        );
    }

    /// The Windows half of that claim, pinned at the seam that broke: CI's
    /// `read: path escapes the session sandbox:
    /// \\?\C:\Users\runneradmin\AppData\Local\Temp\…\secret.txt`. The temp
    /// dir lives under a drive root, and the `Ask` floor must contain it —
    /// in the VERBATIM form `canonicalize()` produces, so `starts_with`
    /// compares like-for-like. `#[cfg(windows)]` because a Linux CI must not
    /// even compile a claim about drive letters (the unix floor is `/` and is
    /// covered by the tests above).
    #[cfg(windows)]
    #[test]
    fn the_ask_floor_contains_the_temp_dirs_drive_root() {
        use crate::agent::fs_backend::FsBackend;
        let cwd = Tmp::new("floor-drive-cwd");
        // Where CI's refused file lived: a file under the temp dir.
        let secret = std::env::temp_dir().join(format!("boundary-secret-{}", uuid::Uuid::new_v4()));
        std::fs::write(&secret, "s").unwrap();
        let secret_canon = secret.canonicalize().unwrap();

        let roots = executor_roots(cwd.path(), &[], Ask);
        let floor = &roots[1..];
        assert!(
            floor.iter().any(|r| secret_canon.starts_with(r)),
            "the Ask floor must contain the temp dir's drive root: {floor:?}"
        );
        // And each floor entry is already canonical (the `\\?\C:\` form) — a
        // bare `C:\` here would silently never match a canonicalized path.
        for root in floor {
            assert_eq!(
                root,
                &root.canonicalize().unwrap(),
                "floor roots must be verbatim-canonical"
            );
        }
        // The regression itself: an approved `Ask` read of that file lands,
        // while the same path is refused by the `Sandboxed` boundary that is
        // `roots[0]` alone (the non-vacuity contrast).
        assert_eq!(
            FsBackend {
                roots: roots.clone()
            }
            .validate(&secret)
            .unwrap(),
            secret_canon,
            "an approved read beyond the cwd must land on Windows"
        );
        assert!(FsBackend {
            roots: vec![roots[0].clone()]
        }
        .validate(&secret)
        .is_err());
        let _ = std::fs::remove_file(&secret);
    }
}

//! Crate-scoped test helpers for `fulcrum-core` (T5 plan, "Create files":
//! `lib.rs` — "module tree + re-export block + crate-scoped test helpers").
//!
//! WHY THIS EXISTS: the S1/S2 modules moved WITH their `#[cfg(test)]` blocks
//! (Step A preserves them verbatim), and those blocks build their fixtures out of
//! `crate::test_support::ScratchDir` / `crate::test_support::env_lock` /
//! `crate::test_support::filesystem_root`. `#[cfg(test)]` is PER-CRATE: the
//! desktop's `src-tauri/src/test_support.rs` does not exist inside this crate's
//! test build, and this crate may not depend on `archimedes_lib` to reach it
//! (that edge is the package cycle T5's ruling severs). So the helpers the moved
//! tests need live here, and the moved test bodies stay byte-identical — their
//! `crate::test_support::…` paths now resolve HERE instead.
//!
//! The API is the subset the moved tests actually touch, kept SIGNATURE-IDENTICAL
//! to the desktop's copy so a fixture reads the same on both sides of the seam:
//! `env_lock`, `filesystem_root`, and `ScratchDir::{in_dir, in_env_temp, path,
//! display, to_path_buf}` plus the `Deref`/`AsRef<Path>` pair. The desktop's copy
//! keeps the rest (`in_tmpfs`/`tmpfs_base_candidates` — no moved test calls them,
//! measured — and the tauri accessors, which this crate must not name).
//!
//! # The one lock per process, and why two copies are safe
//!
//! `ENV_LOCK` serialises tests that mutate process-global state (`$HOME`, the
//! web API-key vars). S2 leaves TWO copies of that static in the workspace: this
//! one and the desktop's. That is safe, and the reason is load-bearing enough to
//! pin here rather than leave to luck:
//!
//!   * This module is `#[cfg(test)]`, so it is compiled into `fulcrum-core`'s own
//!     unit-test binary and is NOT in the rlib. A desktop test therefore CANNOT
//!     reach it (measured: zero `fulcrum_core::test_support` references under
//!     `src-tauri/tests/`), so the two locks can never both be held in one
//!     process — which is what an `ENV_LOCK` self-deadlock would require.
//!   * `cargo test` runs each test BINARY to completion before starting the next
//!     (cargo parallelises *within* a binary via the libtest harness, and
//!     *across* builds, never across two running test binaries; only `nextest`
//!     does that, and this repo runs plain `cargo test`). So the crate's
//!     `$HOME`-mutating tests and the desktop's never overlap in wall-clock time
//!     either.
//!
//! The hazard this note exists for is real and was hit by S3's predecessor (an
//! `ENV_LOCK` self-deadlock): if this module is ever ungated from `#[cfg(test)]`
//! so an integration test CAN import it, the first bullet above stops holding and
//! the two locks must be unified into one static that both crates reference.
//!
//! # The leak this guard exists for (#96)
//!
//! A fixture that hands back a bare `PathBuf` leaks its directory the moment an
//! assertion panics; one green run leaked 432 dirs into the temp dir until the
//! per-user quota exhausted (#96). Ownership is the fix. The `Drop` impl below is
//! the mechanism, and `a_scratch_dir_is_gone_when_its_owner_is_dropped` pins it —
//! the crate's own copy needs its own pin, because a duplicated helper that is
//! only tested in its ORIGINAL copy is untested where it is used. The run-level
//! proof stays where it was: `scripts/check-temp-dir-census.sh` counts the temp
//! dir after the whole suite, across both crates.

/// The shared lock serializing the global-state tests (the parallel test harness
/// runs all module tests concurrently; a process-global read by one test
/// mid-mutation by another would see the wrong value). Mirrors the desktop's
/// `ENV_LOCK`; see the module docs for why two copies do not deadlock.
pub static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire the env-mutation lock (poison-tolerant — a sibling test panicking
/// while holding it must not turn this test's failure into an opaque
/// `PoisonError` panic). `pub(crate)` + `#[cfg(test)]` for the same reason the
/// desktop's copy is: it is test-only, and either half of that gate missing on
/// its own would leave the other half reporting `dead_code`.
#[cfg(test)]
pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    std::sync::Mutex::lock(&ENV_LOCK).unwrap_or_else(|p| p.into_inner())
}

/// The shared cargo `target/debug` directory — the ONE place in this crate that
/// resolves it, and the reason T5/S2 put it here was so T5/S3 would not have to
/// resolve it eight times.
///
/// # The trap
///
/// T4 re-rooted every fixture path to `env!("CARGO_MANIFEST_DIR")`, then its
/// `.parent()`, then `target/debug/<bin>`. That was correct while the files lived
/// in `src-tauri/` (parent = the repo root). Once a file moves to
/// `crates/fulcrum-core`, `CARGO_MANIFEST_DIR` is `…/crates/fulcrum-core` and
/// `.parent()` is `…/crates`, so the path names `crates/target/debug/<bin>` —
/// which does not exist. A test that spawns it fails with ENOENT; a `let _ =`
/// site fails quietly.
///
/// # Why the desktop's own pin cannot catch this
///
/// When this trap was written, T4's
/// `fixture_path_resolves_to_the_workspace_root_target` lived in the DESKTOP's
/// `worker/client.rs`, and a desktop-side pin keeps passing while every moved
/// fixture path is broken — it cannot see the crate's module tree. **T8/S8.3
/// moved that file into this crate**, so the original asymmetry is gone; what
/// replaced it is two witnesses, not one:
///   * `childproc::target_debug_dir_is_where_cargo_actually_builds` (this
///     helper's own pin) compares the resolution against `current_exe()`'s real
///     location, so it cannot go green on a wrong level count; and
///   * `worker/client.rs` and `worker/manager/tests/mod.rs` each assert
///     EQUALITY against this helper plus a `current_exe()` comparison — because
///     S8.3 measured that the old two-`starts_with` form PASSED on the broken
///     path, which is how the trap stayed invisible in the first place.
///
/// # Consumers (re-derived by grep at T8/S8.5 — never transcribe a list like
/// this; the T5/S3 version of it had already drifted twice)
///
/// Two fixture binaries. This helper resolves the DIRECTORY; `fixture_bin`
/// resolves the binary itself by adding the platform exe suffix:
///   * `fake_mcp_stdio_core` — `mcp/stdio.rs`, `mcp/tool.rs`, four sites in
///     `mcp/manager.rs`'s tests, and two in
///     `harness/loop/tests/turn_lifecycle.rs`;
///   * `fake_worker_core` — `worker/client.rs::fixture_path` and
///     `worker/manager/tests/mod.rs::fixture_path` (both re-pointed here in
///     T8/S8.3; before that they did their own manifest-parent arithmetic and
///     resolved to a directory no build writes to).
///
/// BOTH NAMES CARRY `_core` since S8b.1, and that suffix is what makes the
/// crate buildable alone. These are the crate's OWN bins (`src/bin/`, declared
/// in `Cargo.toml`), whose bodies live in `crate::fixtures`; the DESKTOP keeps
/// bins named `fake_worker` / `fake_mcp_stdio` over the same bodies, because
/// `CARGO_BIN_EXE_<name>` is package-local and 7 desktop integration tests name
/// `fake_worker` exactly. Two members may NOT declare the same bin name — they
/// share one output file in the workspace `target/debug/`, cargo only warns, and
/// the last writer silently wins (cargo#6313). Full measurements:
/// `crate::fixtures`.
///
/// The `mcp` spawners still carry no vacuity guard of their own, which is why
/// the `childproc` pin above is what stands between a wrong root and a
/// silently-green suite; the two `worker/` spawners now do carry equality +
/// `current_exe()` guards, so a wrong LEVEL count there fails in two places.
///
/// # Why a binary name is a SEPARATE helper
///
/// `target_debug_dir()` is suffix-free on purpose — it names a directory, and
/// directories have no exe suffix on any platform. Appending `EXE_SUFFIX` here
/// would break every caller that joins a directory and then a file name of its
/// own. The suffix belongs to the FILE, so it belongs to `fixture_bin`.
///
/// The `mcp` sites above hand their path to a `Command` and never OBSERVE the
/// file, so the missing suffix is harmless there — `CreateProcess` fills it in.
/// The two `fake_worker` sites also assert `is_file()`, and that is where the
/// suffix became load-bearing; see `fixture_bin` for the full story.
/// # Why it resolves from `current_exe()` and not from the manifest (S8b.1)
///
/// Until S8b.1 this walked `CARGO_MANIFEST_DIR` up TWO levels and appended
/// `target/debug`. That was right for exactly one checkout shape — this crate as
/// a MEMBER of a two-package workspace, where the grandparent IS the root that
/// owns `target/` — and it is WRONG for the shape T8b creates: a standalone
/// `cargo test` inside this package builds into `<package>/target/debug/`, so the
/// two-levels-up expression names a directory no build ever writes. Measured at
/// S8b.1 by copying the crate out of the workspace AFTER the fixture bins had
/// been moved in and the spawner names re-pointed: still **71 failed**, because
/// every one of the 66 fixture spawns resolved past the package into a
/// non-existent grandparent. Renaming the bins could not fix that; only the root
/// can.
///
/// `current_exe()` is the authoritative answer, and `childproc` already called it
/// "the ground truth": cargo put the test binary at `<target>/debug/deps/<name>`
/// and it puts the crate's bins at `<target>/debug/<name>` — SIBLINGS of `deps/`.
/// Two `parent()` steps from the running binary therefore names the directory the
/// fixtures are in BY DEFINITION, in every layout: workspace member, standalone
/// package, `--target-dir` relocation, or a cross-compiled `target/<triple>/debug`
/// (which manifest arithmetic gets wrong too, and which the census-side sibling
/// `childproc::target_debug_dir_is_where_cargo_actually_builds` still asserts).
/// There is no LEVEL COUNT to get right, which is the point: the trap this
/// function's own docs were written about — manifest arithmetic going stale when
/// a file moves depth — stops being expressible.
///
/// It is `#[cfg(test)]` for the pre-existing reason: the only callers are test
/// modules, and an ungated `pub(crate)` fn with no production caller is
/// `dead_code`, which the gate's `-D warnings` rejects.
#[cfg(test)]
pub(crate) fn target_debug_dir() -> std::path::PathBuf {
    let exe = std::env::current_exe()
        .expect("a test binary has a path — this test-only resolver has no production caller");
    exe.parent() // target/debug/deps
        .and_then(|deps| deps.parent()) // target/debug
        .expect("the test binary lives in target/debug/deps")
        .to_path_buf()
}

/// The file name cargo gives the bin target `name` on the platform whose
/// `EXE_SUFFIX` is `suffix` (the suffix is a PARAMETER so the rule is testable
/// on every OS — see `the_fixture_binary_name_carries_the_platform_exe_suffix`).
#[cfg(test)]
pub(crate) fn exe_file_name(name: &str, suffix: &str) -> String {
    format!("{name}{suffix}")
}

/// The built bin target `name` under `target_debug_dir()` — the crate's ONE
/// fixture-binary resolver, exe-suffix aware.
///
/// **Why the suffix is load-bearing.** `target_debug_dir()` names a DIRECTORY,
/// so it is correct on every platform; but a fixture binary is a FILE, and on
/// Windows cargo writes `fake_worker.exe`. Spawning still worked, because
/// `CreateProcessW` appends `.exe` to a suffix-less name — which is exactly why
/// this stayed invisible until a test tried to OBSERVE the file:
/// `std::path::Path::is_file` performs no such appending, so the existence
/// witness in `worker/client.rs` and `worker/manager/tests/mod.rs` failed on
/// `…\target\debug\fake_worker` while the spawn three lines away succeeded.
/// Deriving the name here means the path the tests assert on and the path the
/// spawner execs are the same string on all three OS legs.
#[cfg(test)]
pub(crate) fn fixture_bin(name: &str) -> std::path::PathBuf {
    target_debug_dir().join(exe_file_name(name, std::env::consts::EXE_SUFFIX))
}

/// Is `s` EXACTLY SIX LOWERCASE hex chars — the shape
/// `crate::types::mint_incarnation` guarantees for a **Session incarnation**
/// stamp (ADR 0049)?
///
/// Lifted into this crate in T8/S8.1 (the grounding's §7.4 "lift the stamp
/// helper into the crate" option) because `worker/protocol.rs` moved with its
/// `#[cfg(test)]` block, and that block calls this predicate: `crate::test_support`
/// is PER-CRATE, so the desktop's copy does not exist inside this crate's test
/// build and this crate may not reach it through `archimedes_lib` (that edge is
/// the package cycle T5's ruling severs). The BODY is character-for-character the
/// desktop's — the two copies are the same predicate, and the crate's own pin
/// below (`the_stamp_shape_rejects_everything_a_radix_parse_admits`) carries the
/// same negative cases, so a drift between them reddens on one side or the other.
/// The desktop's copy STAYS (its `tests/session_native.rs` names it) — this is a
/// lift for the crate's test build, not a relocation.
///
/// The shared pin for the `protocol` unit test and the two-process e2e, because
/// the obvious check is half blind: `len() == 6` + `u32::from_str_radix(s, 16)`
/// also accepts `A3F9C1` (and a radix parse accepts a leading `+`, so it is not
/// even an alphabet check on its own). The BYTE SET is the whole argument: a
/// stamp made only of `[0-9a-f]` is lowercase by construction, which is what
/// makes the `to_lowercase` clause implied rather than load-bearing — it is kept
/// because ADR 0049 names lowercase at the seam, and because it is the clause
/// that fails LOUDEST (`A3F9C1` trips it first) when a stamp source ever grows
/// upper-case letters. Case is not what keeps a stamp from equaling a
/// bookkeeping word, either: `system` and `retract` are already lowercase, and
/// what they are not is hex-spelled (`system` carries `s`/`y`/`t`/`m`, `retract`
/// is seven chars and carries `r`/`t`) — see `normalize::is_bookkeeping_message_id`.
pub fn is_incarnation_stamp(s: &str) -> bool {
    let lower_hex = |b: u8| b.is_ascii_digit() || (b'a'..=b'f').contains(&b);
    s.len() == 6 && s == s.to_lowercase() && s.bytes().all(lower_hex)
}

/// (test only) The broadest rule path a test can name — the one root that
/// really does contain every canonical absolute path ON THIS PLATFORM.
///
/// Moved verbatim from the desktop's copy in T5/S2 because two moved tests call
/// it (`policy.rs`'s and `tools/exec/tests/policy.rs`'s maximum-reach rule
/// fixtures). The reason it exists is a platform fact, restated here so the crate
/// copy carries its own justification: a test that wants "a rule with maximum
/// reach" reaches for the literal `"/"`, and that is a Unix fact, not a platform
/// one. On Windows a lone `/` is only the CURRENT DRIVE'S root in the current
/// working directory's spelling, and it is not a component prefix of the verbatim
/// (`\\?\C:\…`) form that `canonicalize` hands back — so a `"/"` rule matches
/// NOTHING and a test that asserts it refuses something reddens there while
/// staying green on Linux and macOS.
///
/// `for_path` is a path the test is actually working with (its scratch home or
/// `cwd`); the root is taken from ITS OWN components, so the two land on the same
/// drive AND on the same verbatim spelling the shared canonicalizer
/// (`crate::fs_backend::canonicalize_lenient`) produces for it — which is the
/// property the caller needs for a component-prefix compare to hold.
///
/// Strict `canonicalize()` (not the shared lenient one) is deliberately the only
/// resolver used, exactly as in the desktop's copy: a drive root derived from a
/// real path always EXISTS, and on Windows the drive root spelled `C:\` carries a
/// `Disk` prefix component while the canonicalized target carries a
/// `VerbatimDisk` one, so the raw spelling would fail the very compare this
/// exists to satisfy. Canonicalizing lands both sides in the verbatim form.
#[cfg(test)]
pub(crate) fn filesystem_root(for_path: &std::path::Path) -> std::path::PathBuf {
    #[cfg(not(windows))]
    {
        // On unix the root is the one true `/`, and every canonical absolute
        // path is under it. `for_path` is irrelevant here, but the signature
        // stays uniform so the call sites do not need a `cfg`.
        let _ = for_path;
        std::path::PathBuf::from("/")
    }
    #[cfg(windows)]
    {
        // `Prefix`(e.g. `C:`) then `RootDir`(`\`) is the drive root — the
        // component-wise root, not a string slice, so a UNC path yields its
        // own server/share root rather than a mangled `C:\`.
        let mut root = std::path::PathBuf::new();
        for component in for_path.components() {
            match component {
                std::path::Component::Prefix(prefix) => root.push(prefix.as_os_str()),
                std::path::Component::RootDir => {
                    root.push(component.as_os_str());
                    break;
                }
                // Anything else is below the root: stop.
                _ => break,
            }
        }
        assert!(
            !root.as_os_str().is_empty(),
            "`{for_path:?}` is rooted on a drive — a relative path has no root to name"
        );
        root.canonicalize().unwrap_or(root)
    }
}

/// A scratch dir for ONE test, REMOVED when it is dropped. See the module docs
/// for why this is duplicated rather than imported, and #96 for why it exists.
///
/// NOT `Clone`, on purpose: a second owner would `remove_dir_all` the same path
/// twice. [`ScratchDir::to_path_buf`] is the escape hatch for a call site that
/// needs an owned path — but the dir still dies with the guard.
pub struct ScratchDir {
    path: std::path::PathBuf,
}

impl ScratchDir {
    /// A fresh dir directly inside `base`, named `{prefix}{tag}-{uuid}`.
    /// `None` when `base` cannot accept it — the caller decides whether that
    /// means "skip" or "panic". The `tools/sandbox` fixtures are the crate's
    /// reason this constructor exists at all: their base must be a probe-verified
    /// tmpfs OUTSIDE the sandbox write allow-list, and a fallback to
    /// `std::env::temp_dir()` would put them inside it, which makes ~13
    /// confinement tests pass VACUOUSLY. Hence `None`, not a fallback.
    pub fn in_dir(base: &std::path::Path, prefix: &str, tag: &str) -> Option<Self> {
        let path = base.join(format!("{prefix}{tag}-{}", uuid::Uuid::new_v4()));
        // `create_dir_all` IS the probe: a present-but-unwritable base must
        // report `None` rather than panic a fixture (the lesson of the prior art,
        // which checked `is_dir()` first and could still fail the write).
        std::fs::create_dir_all(&path).ok()?;
        Some(Self { path })
    }

    /// The ambient temp dir (`std::env::temp_dir()`) — the base the guards this
    /// replaces already assumed writable. Panics only if `temp_dir()` itself is
    /// unwritable, which every one of those guards has always assumed.
    pub fn in_env_temp(prefix: &str, tag: &str) -> Self {
        Self::in_dir(&std::env::temp_dir(), prefix, tag).expect(
            "`std::env::temp_dir()` accepts a `create_dir_all` — the guards this \
             replaces already assumed it writable, so this is a carried-over \
             precondition rather than an invariant (`TMPDIR` can name a dir that \
             is not writable; that is the condition #96 describes)",
        )
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// The dir as a shell-quotable string (the tags are uuid-derived, so no
    /// quoting character can appear) — the `tools/sandbox` tests' `display()`
    /// needs this on the inner guard, not just on their own `TmpDir` wrapper.
    pub fn display(&self) -> String {
        self.path.display().to_string()
    }

    pub fn to_path_buf(&self) -> std::path::PathBuf {
        self.path.clone()
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl std::ops::Deref for ScratchDir {
    type Target = std::path::Path;
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

/// Load-bearing, not decoration: `std::fs::create_dir_all(&guard)` and the
/// `Some(&home)` → `Option<&Path>` coercion FAIL with
/// `` the trait bound `ScratchDir: AsRef<Path>` is not satisfied `` when only
/// `Deref` is present — the blanket `impl AsRef<U> for &T` needs `T: AsRef<U>`,
/// and `Deref` does not supply it. Both are required; `AsRef` alone does not
/// cover the method calls (`root.join(..)`) that `Deref` does.
impl AsRef<std::path::Path> for ScratchDir {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::{exe_file_name, fixture_bin, is_incarnation_stamp, target_debug_dir, ScratchDir};

    /// The crate's OWN pin for the crate's OWN copy of the stamp predicate —
    /// the rule this module's docs state for `ScratchDir` ("a duplicated helper
    /// that is only tested in its ORIGINAL copy is untested where it is used")
    /// applies exactly as hard to `is_incarnation_stamp`, which T8/S8.1 lifted
    /// here because `worker/protocol.rs`'s moved `#[cfg(test)]` block calls it.
    ///
    /// The negative cases are the point. `every_start_env_gets_its_own_incarnation_stamp`
    /// (in `worker/protocol.rs`) only ever asserts POSITIVE stamps, so without
    /// this pin the copy that the crate actually compiles would never have its
    /// rejections tested at all — and `A3F9C1` is the one case the
    /// `len() == 6` + `u32::from_str_radix(_, 16)` check this replaces was
    /// genuinely BLIND to. The rest are kept so that a future "simplify it back"
    /// that drops EITHER half of this predicate reddens something here — and note
    /// that this is NOT true of the desktop's inherited list, which never contains
    /// a six-char lowercase non-hex string, so dropping the BYTE-SET clause there
    /// reddens nothing. The two cases below are what make the crate's copy
    /// discriminate all three halves; the gap is reported, not silently left.
    #[test]
    fn the_crate_stamp_shape_rejects_everything_a_radix_parse_admits() {
        for ok in ["a3f9c1", "000000", "ffffff", "dead01"] {
            assert!(is_incarnation_stamp(ok), "{ok} is a stamp");
        }
        for bad in [
            "A3F9C1",  // uppercase hex — parses, wrong case
            "+a3f9c1", // a signed number — parses, not an id
            "a3f9c",   // five chars
            "a3f9c10", // seven chars
            "abcdefg", // not hex (`g`) — but 7 chars, so `len` catches it first
            "",        // the EMPTY stamp `TurnState::default()` carries
            // The two cases the inherited list was BLIND to. Every case above is
            // already rejected by `len() == 6` or by `to_lowercase`, so a copy of
            // this predicate that dropped the BYTE-SET clause entirely stayed
            // green — which is precisely the half ADR 0049's byte-set argument is
            // about, and the half the doc above claims is pinned. A six-char
            // LOWERCASE word is the only shape that reaches the byte set, and
            // `system` is the real one: it is a bookkeeping word a stamp must
            // never equal (see `normalize::is_bookkeeping_message_id`).
            "system", // six chars, lowercase, NOT hex (`s`/`y`/`t`/`m`)
            "retrac", // six chars, lowercase, NOT hex — a truncated bookkeeping word
        ] {
            assert!(!is_incarnation_stamp(bad), "{bad} is NOT a stamp");
        }
    }

    /// The mechanism the moved fixtures depend on: the guard removes its dir when
    /// its owner is dropped. A bare `PathBuf` has no `Drop`, so every panicking
    /// assertion leaked one dir (#96). This is the crate's OWN pin for the
    /// crate's own copy of the helper.
    #[test]
    fn a_scratch_dir_is_gone_when_its_owner_is_dropped() {
        let guard = ScratchDir::in_env_temp("fc-guard-", "drop");
        assert!(guard.path().is_dir(), "creating the guard made the dir");
        let captured = guard.to_path_buf();
        drop(guard);
        assert!(
            !captured.exists(),
            "the guard's `Drop` removed its dir — {captured:?} is gone"
        );
    }

    /// `in_dir` is the constructor the sandbox fixtures use because it is the one
    /// that REFUSES rather than falling back. Pinning the refusal is what keeps a
    /// future "simplify it to panic-and-fallback" from silently making ~13
    /// confinement tests vacuous: a sandbox fixture rooted in `/tmp` sits INSIDE
    /// the write allow-list, so a denied write would be denied for the wrong
    /// reason and the test would still be green.
    #[test]
    fn in_dir_reports_none_when_the_base_cannot_accept_it() {
        // A path whose PARENT is a regular file: no `create_dir_all` can succeed
        // under it, on any platform, without touching the ambient temp dir.
        let blocker = ScratchDir::in_env_temp("fc-guard-", "blocker");
        let not_a_dir = blocker.path().join("a-file");
        std::fs::write(&not_a_dir, b"x").unwrap();

        assert!(
            ScratchDir::in_dir(&not_a_dir, "fc-guard-", "unwritable").is_none(),
            "`in_dir` must report `None` for an unusable base — the sandbox \
             fixtures branch on `None` to SKIP loudly, and a fallback or a panic \
             would change what those tests prove"
        );

        // And the happy path, so the `None` above is about the base and not
        // about the constructor being broken.
        let base = ScratchDir::in_env_temp("fc-guard-", "base");
        let inner = ScratchDir::in_dir(base.path(), "fc-guard-", "inside");
        let inner = inner.expect("a writable base accepts a dir");
        assert!(
            inner.path().starts_with(base.path()),
            "the dir lands inside the base it was given: {:?}",
            inner.path()
        );
        assert_eq!(
            inner.display(),
            inner.path().display().to_string(),
            "`display()` is the path's own rendering (the sandbox tests shell-quote it)"
        );
    }

    /// The naming contract the #96 census reads: `{prefix}{tag}-{uuid}`, and the
    /// uuid half must really vary — two tests asking for the SAME tag may not
    /// share a dir, or they delete each other's fixture.
    #[test]
    fn the_guard_names_the_dir_after_the_prefix_and_tag_it_is_given() {
        let a = ScratchDir::in_env_temp("fc-guard-", "naming");
        let name = a
            .path()
            .file_name()
            .expect("a scratch dir has a file name")
            .to_string_lossy()
            .into_owned();
        assert!(
            name.starts_with("fc-guard-naming-"),
            "the name must carry the prefix then the tag: {name}"
        );
        assert!(
            name.strip_prefix("fc-guard-naming-")
                .is_some_and(|uuid| !uuid.is_empty()),
            "the uuid segment must be present: {name}"
        );

        let b = ScratchDir::in_env_temp("fc-guard-", "naming");
        assert_ne!(
            a.path(),
            b.path(),
            "the SAME tag must still mint a DIFFERENT dir"
        );
    }

    /// The exe-suffix rule behind `fixture_bin`.
    ///
    /// CI's `windows-latest` leg ran `fixture_path_resolves_to_the_workspace_root_target`
    /// and `fixture_root_is_where_cargo_actually_builds` red on BOTH runs of the
    /// first CI-enabled SHA, both on their `is_file()` witness:
    /// `the fixture must exist: "D:\\a\\desktop\\desktop\\target\\debug\\fake_worker"`.
    /// The directory half of the path was right and the SPAWN worked — on Windows
    /// `CreateProcessW` appends `.exe` to a suffix-less name — but `is_file()`
    /// performs no such appending, so the existence witness looked at a file that
    /// cargo never wrote. Linux and macOS have an empty `EXE_SUFFIX`, which is why
    /// 2220 local tests and both other legs passed.
    ///
    /// The suffix is a parameter here so BOTH arms of the rule are asserted on
    /// every OS: a Linux run still proves the `.exe` arm, which a test that only
    /// read `std::env::consts::EXE_SUFFIX` could not (it would assert `""` and
    /// pass vacuously off Windows).
    #[test]
    fn the_fixture_binary_name_carries_the_platform_exe_suffix() {
        // The rule, both platforms, on every OS.
        assert_eq!(exe_file_name("fake_worker_core", ""), "fake_worker_core");
        assert_eq!(
            exe_file_name("fake_worker_core", ".exe"),
            "fake_worker_core.exe",
            "a Windows fixture binary is `fake_worker_core.exe` — the name cargo \
             writes for this crate's bin"
        );

        // The resolver applies the rule for the REAL platform, and keeps the
        // directory half untouched (the `current_exe()`-derived root that
        // `target_debug_dir_is_where_cargo_actually_builds` pins).
        let p = fixture_bin("fake_worker_core");
        let want_name = format!("fake_worker_core{}", std::env::consts::EXE_SUFFIX);
        assert_eq!(
            p.file_name().and_then(|s| s.to_str()),
            Some(want_name.as_str()),
            "fixture_bin appends the platform exe suffix to the bin name"
        );
        assert_eq!(
            p.parent(),
            Some(target_debug_dir().as_path()),
            "fixture_bin resolves through the crate's ONE resolver"
        );

        // The existence witness that went red on Windows, now through the
        // resolver — this is the arm that fails if the suffix is dropped.
        assert!(
            p.is_file(),
            "the resolved fixture binary must exist on disk: {p:?} (cargo builds \
             `{want_name}`; `is_file` does NOT append an exe suffix the way \
             `CreateProcess` does, so a suffix-less path is a path that does not exist)"
        );
    }
}

// ── `MemoryStore` — the crate-side double for the harness's `Store` seam ─────
//
// # Why this exists
//
// The moved `harness` fixtures built every `AgentLoop` on a real
// `storage::Db` + the desktop's `SessionStore` (`loop/tests/support.rs`'s
// `build_loop_with_sink`, `compact.rs`'s `temp_store`, `normalize.rs`'s
// `temp_db`). None of that can travel: the crate has no `rusqlite` in its graph
// (Step B-bis) and cannot name `storage::Db` at all. So either ~190 moved tests
// relocate to the desktop — which is where the two-sided `harness_*.rs` targets
// already live, and would leave the crate with no loop tests — or the crate
// gets its own implementation of the seam it now owns. It does the latter.
//
// # What it does NOT pretend to be
//
// A double is not the database, and the honest way to say that is to list what
// only the real thing proves:
//
//   * UPSERT / idempotency keyed on a UNIQUE constraint — the display store's
//     `(session_id, kind, message_key)` collapse and `native_messages`'
//     `(session_id, seq)` collapse;
//   * the ATOMICITY of the compaction rewrite (one SQLite transaction);
//   * the FK from `native_messages` / `messages` to `sessions`;
//   * `payload_json` surviving a round-trip through a TEXT column.
//
// Every moved test whose ASSERTION is one of those four RELOCATED to the desktop
// instead of being re-pointed here — `src-tauri/tests/persist_frames_db.rs`,
// `normalize_display_db.rs`, and the four `SessionStore`-as-a-`Store` tests that
// stayed with the STAYING half in `src-tauri/src/agent/harness/store.rs`. What
// re-pointed here asserts only what a `Store` has to give: which rows the loop
// handed the seam, in what order, with what payload.
//
// # The one thing it must get RIGHT
//
// `load_messages` returns the transcript in `seq` ORDER, and `replace_messages`
// REPLACES the whole transcript (it does not append). Both are assertions the
// moved loop/compaction tests actually make, so both are implemented rather than
// stubbed, and `the_memory_store_is_not_a_noop_store` below is the pin that
// keeps them that way — a double whose `load_messages` always returned `[]`
// would make a dozen compaction and transcript pins pass vacuously.

/// The crate-side [`Store`] double (see the section header for the four
/// properties it deliberately does NOT provide).
#[derive(Default)]
pub struct MemoryStore {
    inner: std::sync::Mutex<MemoryStoreState>,
}

#[derive(Default)]
struct MemoryStoreState {
    /// `(session_id, seq) → (role, content_json)`, read out in `seq` order.
    transcript: Vec<(String, u64, String, String)>,
    /// The DISPLAY rows, in write order, as the `(kind, message_key,
    /// payload_json)` triples the seam was handed.
    display: Vec<(String, String, String, String)>,
    /// The last `record_context_usage` per session.
    context_usage: Vec<(String, u64, u64)>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryStoreState> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The display rows handed to `persist_display`, in write order, as
    /// `(session_id, kind, message_key, payload_json)`.
    pub fn display_rows(&self) -> Vec<(String, String, String, String)> {
        self.lock().display.clone()
    }

    /// The last context-usage write per session.
    pub fn context_usage(&self) -> Vec<(String, u64, u64)> {
        self.lock().context_usage.clone()
    }

    /// The raw transcript rows in `seq` order, as `(seq, role, content_json)`.
    pub fn transcript(&self, session_id: &str) -> Vec<(u64, String, String)> {
        let mut rows: Vec<(u64, String, String)> = self
            .lock()
            .transcript
            .iter()
            .filter(|(sid, _, _, _)| sid == session_id)
            .map(|(_, seq, role, json)| (*seq, role.clone(), json.clone()))
            .collect();
        rows.sort_by_key(|(seq, _, _)| *seq);
        rows
    }
}

impl crate::harness::store::Store for MemoryStore {
    fn insert_message(
        &self,
        session_id: &str,
        seq: u64,
        role: &str,
        content_json: &str,
    ) -> Result<(), crate::seams::StoreError> {
        let mut st = self.lock();
        // The `(session_id, seq)` collapse is reproduced (NOT as a UNIQUE
        // constraint claim — as the "one row per seq" the loop's own reads
        // depend on).
        st.transcript
            .retain(|(sid, s, _, _)| !(sid == session_id && *s == seq));
        st.transcript.push((
            session_id.to_string(),
            seq,
            role.to_string(),
            content_json.to_string(),
        ));
        Ok(())
    }

    fn load_messages(
        &self,
        session_id: &str,
    ) -> Result<Vec<crate::provider_types::ChatMessage>, crate::seams::StoreError> {
        let rows = self.transcript(session_id);
        rows.into_iter()
            .map(|(_, _, json)| serde_json::from_str(&json).map_err(crate::seams::StoreError::Json))
            .collect()
    }

    fn clear_messages(&self, session_id: &str) -> Result<(), crate::seams::StoreError> {
        self.lock()
            .transcript
            .retain(|(sid, _, _, _)| sid != session_id);
        Ok(())
    }

    fn replace_messages(
        &self,
        session_id: &str,
        rows: &[(u64, String, String)],
    ) -> Result<(), crate::seams::StoreError> {
        let mut st = self.lock();
        st.transcript.retain(|(sid, _, _, _)| sid != session_id);
        for (seq, role, content_json) in rows {
            st.transcript.push((
                session_id.to_string(),
                *seq,
                role.clone(),
                content_json.clone(),
            ));
        }
        Ok(())
    }

    fn persist_display(
        &self,
        session_id: &str,
        rows: &[crate::store_rows::DisplayRow],
    ) -> Result<(), crate::seams::StoreError> {
        // NO UPSERT here, on purpose: the `(session_id, kind, message_key)`
        // collapse is the SQLite UNIQUE constraint's behaviour and belongs to
        // the relocated tests. This records EVERY call, which is what the
        // re-pointed loop tests read.
        let mut st = self.lock();
        for row in rows {
            st.display.push((
                session_id.to_string(),
                row.kind.clone(),
                row.message_key.clone(),
                row.payload_json.to_string(),
            ));
        }
        Ok(())
    }

    fn record_context_usage(
        &self,
        session_id: &str,
        used: u64,
        window: u64,
    ) -> Result<(), crate::seams::StoreError> {
        let mut st = self.lock();
        st.context_usage.retain(|(sid, _, _)| sid != session_id);
        st.context_usage
            .push((session_id.to_string(), used, window));
        Ok(())
    }
}

#[cfg(test)]
mod memory_store_tests {
    use super::MemoryStore;
    use crate::harness::store::Store;
    use crate::store_rows::DisplayRow;

    fn row(key: &str, text: &str) -> DisplayRow {
        DisplayRow {
            kind: "agent-text".to_string(),
            message_key: key.to_string(),
            payload_json: serde_json::json!({ "text": text }),
            created_at: 1,
        }
    }

    /// THE non-vacuity pin for the double. `NoopStore` returns an empty
    /// transcript from `load_messages` and drops every write; if `MemoryStore`
    /// degenerated into a second `NoopStore`, a dozen moved compaction and
    /// transcript pins (`load_messages("s1")` after a turn, after a compaction,
    /// after a retraction) would go GREEN on an empty store. So: write, read
    /// back, and prove the read is the write.
    #[test]
    fn the_memory_store_is_not_a_noop_store() {
        let store = MemoryStore::new();
        store
            .insert_message("s1", 0, "user", r#"{"role":"user","content":"hi"}"#)
            .unwrap();
        store
            .insert_message(
                "s1",
                1,
                "assistant",
                r#"{"role":"assistant","content":"yo"}"#,
            )
            .unwrap();
        let loaded = store.load_messages("s1").expect("both rows parse");
        assert_eq!(loaded.len(), 2, "the writes are READ BACK, not dropped");
        use crate::provider_types::MessageContent;
        assert!(
            matches!(&loaded[0].content, MessageContent::Text(s) if s == "hi"),
            "in seq order, got {:?}",
            loaded[0].content
        );
        assert!(
            matches!(&loaded[1].content, MessageContent::Text(s) if s == "yo"),
            "got {:?}",
            loaded[1].content
        );

        // The display half, same claim.
        store.persist_display("s1", &[row("m1", "Hel")]).unwrap();
        store.persist_display("s1", &[row("m1", "Hello")]).unwrap();
        let rows = store.display_rows();
        assert_eq!(rows.len(), 2, "every write is recorded (no UPSERT here)");
        assert_eq!(
            rows[1].3, r#"{"text":"Hello"}"#,
            "the payload rides as the string the seam was handed"
        );

        // `replace_messages` REPLACES (the compaction rewrite) — the property
        // the overflow tests read.
        store
            .replace_messages(
                "s1",
                &[(
                    0,
                    "system".to_string(),
                    r#"{"role":"system","content":"s"}"#.to_string(),
                )],
            )
            .unwrap();
        let after = store.load_messages("s1").expect("the rewrite parses");
        assert_eq!(after.len(), 1, "the rewrite replaced, it did not append");
        assert_eq!(after[0].role, crate::provider_types::ChatRole::System);

        // `clear_messages` clears the transcript and NOT the display rows (the
        // two tables are independent — the reason `native_messages` exists).
        store.clear_messages("s1").unwrap();
        assert!(store.load_messages("s1").unwrap().is_empty());
        assert_eq!(
            store.display_rows().len(),
            2,
            "the display rows survive a transcript clear"
        );

        // And a second session does not see the first one's rows.
        assert!(store.load_messages("other").unwrap().is_empty());
    }

    /// The `seq`-collapse the loop depends on (the same `(session_id, seq)`
    /// re-insert must not double a row — the loop re-persists seq 0 on every
    /// resume). NOTE what this does NOT pin: that the collapse is enforced by a
    /// UNIQUE constraint. That is the desktop's `Db`, and
    /// `tests/harness_store.rs::insert_is_idempotent_on_conflict` pins it there.
    #[test]
    fn a_repeated_seq_collapses_to_one_row() {
        let store = MemoryStore::new();
        for _ in 0..3 {
            store
                .insert_message("s1", 0, "user", r#"{"role":"user","content":"hi"}"#)
                .unwrap();
        }
        assert_eq!(store.transcript("s1").len(), 1);
    }
}

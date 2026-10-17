//! What a child process should inherit, minus the AppImage's own paths.
//!
//! The shipped Linux bundle is an AppImage, and its `AppRun` (a compiled
//! `AppRun.c`, plus the `linuxdeploy-plugin-gtk.sh` hook) `putenv`s a stack of
//! search-path variables — `LD_LIBRARY_PATH`, `PATH`, `PYTHONHOME`,
//! `GI_TYPELIB_PATH`, the `GTK_*` family, … — whose components point inside
//! `/tmp/.mount_archim*`. std materialises the child's `envp` from the parent's
//! env, so EVERY child (`sh`, `fd`, `rg`, a stdio MCP server, the Worker
//! self-spawn, `sudo`) inherits that stack: `node` dies on
//! `version 'OPENSSL_3.4.0' not found` and `git`/`rg`/`grep` degrade to
//! unversioned symbol binding. Evidence and citations:
//! `docs/research/appimage-child-process-env-leak.md`.
//!
//! The fix is per-child and surgical: strip only the components that point
//! inside an AppImage mount, and gate the whole thing OFF unless we are
//! genuinely running from one — a `.deb` install, an `.rpm` install and a
//! `cargo run` dev build must behave byte-identically to before.
//!
//! Two invariants this module exists to hold:
//!
//! * **`scrub` is pure.** It takes the inherited environment, `APPDIR` and the
//!   current exe as *arguments* and returns an [`EnvPatch`]; it reads no
//!   process state and touches no `Command`. That is what lets its tests run in
//!   parallel without the process-env lock (`crate::test_support::env_lock`).
//! * **Set-or-remove, never a map.** `env_remove` is not representable as a
//!   map entry, and "the strip emptied this variable" is the interesting case:
//!   the variable may not have existed before the `AppRun` invented it, and
//!   `env(var, "")` is NOT equivalent to removing it.
//!
//! Never `env_clear()` a child on the strength of this patch: std documents
//! that a child with no `PATH` falls back to `confstr(_CS_PATH)` rather than
//! the parent's, and still exits 0 — a dropped `PATH` is a silent regression,
//! which is exactly why this module filters `PATH` instead of removing it.

use std::ffi::OsString;
use std::path::Path;

// `OsStr` appears only in the Unix-only encoding helper and the Linux-only
// decision procedure, so the import follows their `cfg` — a top-level import
// is an `unused_imports` error under `-D warnings` on `windows-latest`.
#[cfg(unix)]
use std::ffi::OsStr;

/// The variables the bundle's `AppRun` / GTK hook are known to taint. One rule
/// covers both colon-list values (`PATH`, `LD_LIBRARY_PATH`, …) and single-path
/// values (`PYTHONHOME`, `GIO_MODULE_DIR`, …): a single path is a one-component
/// list. Order is the order the patch reports; keep it stable.
///
/// Deliberate omissions, so nobody "fixes" them:
///
/// * **`LD_PRELOAD` is NOT here.** It is **space**-separated, so the one
///   split-on-`:` rule cannot express it, and nothing in this bundle sets it
///   (`AppDir/usr/optional/` does not exist, so there is no `checkrt` hook).
/// * **`APPIMAGE` / `APPDIR` / `ARGV0` / `OWD` are NOT here.** They are the
///   documented AppImage contract, carry no search-path semantics that can
///   break a child, and a user's own packaging script run through the `bash`
///   tool may legitimately read them. `PYTHONDONTWRITEBYTECODE=1` is likewise
///   left alone (not a path), and `GTK_THEME` is the GUI's business, not a path.
/// * **`PYTHONHOME` IS here even though no upstream scrub list has it**
///   (AppImageKit's `run_external()` omits it). It is `$APPDIR/usr/` in this
///   bundle, which breaks any system `python3` child outright
///   (`Fatal Python error: Failed to import encodings module`).
///
/// Not compiled on non-Linux targets, where there is nothing to scrub; the
/// `allow` keeps `cargo clippy --all-targets -- -D warnings` quiet on
/// macOS/Windows, where every consumer below is `cfg`'d out.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const TAINTED_VARS: &[&str] = &[
    "PATH",
    "LD_LIBRARY_PATH",
    "PYTHONHOME",
    "PYTHONPATH",
    "PERLLIB",
    "XDG_DATA_DIRS",
    "GSETTINGS_SCHEMA_DIR",
    "GIO_MODULE_DIR",
    "GIO_EXTRA_MODULES",
    "GI_TYPELIB_PATH",
    "QT_PLUGIN_PATH",
    "GTK_DATA_PREFIX",
    "GTK_PATH",
    "GTK_EXE_PREFIX",
    "GTK_IM_MODULE_FILE",
    "GDK_PIXBUF_MODULE_FILE",
    "GDK_PIXBUF_MODULEDIR",
    "GST_PLUGIN_SYSTEM_PATH",
    "GST_PLUGIN_SYSTEM_PATH_1_0",
    "GST_PLUGIN_PATH",
    "GST_PLUGIN_SCANNER",
];

/// The explicit overrides one child `Command` needs (empty ⇒ apply nothing).
///
/// `Debug` is for the call sites' diagnostics and for test failures: a patch
/// that did the wrong thing is unreadable as just two vectors of opaque bytes
/// otherwise.
#[derive(Debug, Default)]
pub struct EnvPatch {
    set: Vec<(OsString, OsString)>,
    remove: Vec<OsString>,
}

impl EnvPatch {
    /// True when nothing would change (the no-op case every non-AppImage host hits).
    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.remove.is_empty()
    }

    /// Apply the patch. Call this BEFORE the call site's own `env`/`envs`,
    /// so a caller's explicit variable still wins.
    ///
    /// The order of the two loops is NOT an ordering guarantee: `scrub` puts a
    /// name in exactly ONE of `remove` / `set` (never both), so `remove`-then-
    /// `set` and `set`-then-`remove` are the same patch. Do not "fix" it.
    /// What DOES matter is this method's position at the call site — after it,
    /// a caller's own `env` wins; before it, an `env_remove` here discards the
    /// caller's entry (measured against a real `execve`; see the MCP site).
    pub fn apply(&self, cmd: &mut tokio::process::Command) {
        for key in &self.remove {
            cmd.env_remove(key);
        }
        for (key, value) in &self.set {
            cmd.env(key, value);
        }
    }

    /// The [`Self::apply`] for a `std::process::Command` (the updater
    /// relaunch). tokio's `env`/`env_remove` are one-line delegations to std,
    /// so one patch serves both builders.
    ///
    /// Same disjointness note as [`Self::apply`].
    pub fn apply_std(&self, cmd: &mut std::process::Command) {
        for key in &self.remove {
            cmd.env_remove(key);
        }
        for (key, value) in &self.set {
            cmd.env(key, value);
        }
    }
}

/// The cached patch for THIS process (`APPDIR` + `current_exe()` are
/// process-lifetime constants, so this is computed once). `is_empty()` when
/// we are not running from an AppImage.
///
/// ⚠ PRODUCTION-ONLY. Under `cargo test`, `current_exe()` is
/// `target/debug/deps/<hash>`, which is never under `$APPDIR`, so the gate
/// closes and this returns an EMPTY patch on every host — a test that
/// asserts against `for_child()` asserts nothing. Tests must call [`scrub`]
/// with a fabricated `appdir`/`exe` pair.
pub fn for_child() -> &'static EnvPatch {
    static PATCH: std::sync::OnceLock<EnvPatch> = std::sync::OnceLock::new();
    PATCH.get_or_init(|| {
        // A failing `current_exe()` must NOT scrub anything, so pass `/`: the
        // gate rejects it (no `Component::Normal`), which fails toward "do
        // nothing" rather than toward "strip the user's PATH".
        let exe = std::env::current_exe().unwrap_or_else(|_| Path::new("/").to_path_buf());
        scrub(
            std::env::vars_os().collect(),
            std::env::var_os("APPDIR").as_deref().map(Path::new),
            &exe,
        )
    })
}

/// Pure. Production callers go through [`for_child()`]; tests call this
/// directly.
///
/// * `inherited`: the env the child would inherit (prod: `std::env::vars_os()`).
/// * `appdir`:    `std::env::var_os("APPDIR")`.
/// * `exe`:       `std::env::current_exe()`.
///
/// The `appdir`/`exe` inputs are NOT trusted: an empty or root `appdir` must
/// gate the scrub OFF.
#[cfg(target_os = "linux")]
pub fn scrub(inherited: Vec<(OsString, OsString)>, appdir: Option<&Path>, exe: &Path) -> EnvPatch {
    // 1. The gate. Not in an AppImage ⇒ no patch, no `env` call, byte-identical
    //    behaviour for a `.deb`, an `.rpm` and a dev build.
    let Some(appdir) = appimage_mount_root(appdir, exe) else {
        return EnvPatch::default();
    };
    // 2. The mount roots: `$APPDIR`, plus every `.mount_` ancestor found in the
    //    inherited values (the stale generation, see [`mount_roots`]).
    let roots = mount_roots(appdir, &inherited);

    let mut patch = EnvPatch::default();
    // Iterating the const (not `inherited`) is what makes the patch's order
    // stable, and is also what keeps an inherited name that is not on the
    // tainted list entirely untouched.
    for name in TAINTED_VARS {
        // 3. Only names already inherited: a patch never invents a variable.
        let Some(value) = lookup(&inherited, name) else {
            continue;
        };
        // Drop the empty components (the `AppRun` trailing `:` — `ld.so` reads
        // one as the current directory) and the components inside a mount.
        let kept: Vec<OsString> = split_colon_list(value)
            .into_iter()
            .filter(|component| !component.is_empty() && !under_a_root(component, &roots))
            .collect();
        if kept.is_empty() {
            // (a) Nothing of the user's survived ⇒ remove. NEVER `env(name, "")`:
            // the variable may not have existed before the AppRun invented it,
            // and an empty value is not the same as an absent one to every
            // consumer.
            patch.remove.push(OsString::from(*name));
        } else {
            let rejoined = join_colon_list(&kept);
            // (b) Something survived and changed ⇒ re-`set` it, in the original
            // relative order. (c) Byte-identical ⇒ emit NEITHER half, which is
            // what makes the patch idempotent (so it cannot grow across the
            // main → Worker → `bash` hops) and what lets `for_child()` report
            // "nothing to do" on a host with nothing wrong. Note `"/usr/lib:"` →
            // `"/usr/lib"` IS a change, so the trailing-colon case still sets.
            if rejoined.as_os_str() != value {
                patch.set.push((OsString::from(*name), rejoined));
            }
        }
    }
    patch
}

// ── the Linux decision procedure ─────────────────────────────────────────

/// The gate: `Some(appdir)` only when this process is genuinely running from
/// an AppImage bundle rooted there.
///
/// Every condition is load-bearing:
///
/// * **a usable root** — non-empty, with at least one `Component::Normal`.
///   `std::env::var_os` returns `Some("")` for a set-but-empty variable (the
///   `export APPDIR=` "unset-ish" idiom a CI script uses), and `Path::new("")`
///   makes `starts_with` return `true` for EVERY path while the empty mount
///   root byte-prefix-matches every absolute entry: without this, `APPDIR=""`
///   strips `PATH` from every child and loses `~/.cargo/bin`, `~/.local/bin`
///   and `/opt/nvim` silently, exit code 0. The same count rejects `/` (only
///   `RootDir`) and `.`, which have the same property.
/// * **`exe.starts_with(appdir)`** — a user's own shell may legitimately export
///   `APPIMAGE`/`APPDIR`; their real `PATH` must then not be touched.
/// * **[`looks_like_an_appimage`]** — a positive structural *tiebreaker*, never
///   the sole test (the containment test above is the decision; this only
///   confirms the shape). It makes the gate fail closed on a weird layout, e.g.
///   a build directory that happens to contain the exe and exports `APPDIR`.
#[cfg(target_os = "linux")]
fn appimage_mount_root<'a>(appdir: Option<&'a Path>, exe: &Path) -> Option<&'a Path> {
    let appdir = appdir?;
    if appdir.as_os_str().is_empty() {
        return None;
    }
    if !appdir
        .components()
        .any(|component| matches!(component, std::path::Component::Normal(_)))
    {
        return None;
    }
    if !exe.starts_with(appdir) || !looks_like_an_appimage(appdir) {
        return None;
    }
    Some(appdir)
}

/// Positive structural corroboration for the gate — a tiebreaker, and by
/// design not the only one: either the canonical AppDir layout is on disk
/// (`$APPDIR/usr`), or `$APPDIR` has the name the AppImage runtime itself
/// gives a mount point (`build_mount_point` in `runtime.c`). The filesystem
/// half is the one that holds in production (every AppDir has `usr/`); the
/// name half is what keeps the check honest for a fabricated path, so the gate
/// is testable without building a fake tree — and it is never authoritative
/// alone, exactly as the `.mount_` shape rule in [`mount_roots`] requires.
#[cfg(target_os = "linux")]
fn looks_like_an_appimage(appdir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    appdir.join("usr").is_dir()
        || appdir
            .file_name()
            .is_some_and(|name| name.as_bytes().starts_with(b".mount_"))
}

/// The value of `name` in `inherited`, if the variable is present at all.
/// First match wins (a real `envp` has no duplicates).
#[cfg(target_os = "linux")]
fn lookup<'a>(inherited: &'a [(OsString, OsString)], name: &str) -> Option<&'a OsStr> {
    inherited
        .iter()
        .find(|(key, _)| key == OsStr::new(name))
        .map(|(_, value)| value.as_os_str())
}

/// The directories whose contents belong to the bundle, as raw bytes.
///
/// `$APPDIR` alone is not enough: this host measurably carries `PATH` entries
/// under TWO `/tmp/.mount_archim*` dirs while `$APPDIR` names only one, the
/// older generation appearing in no variable of its own (the updater relaunch
/// re-enters `AppRun`, which prepends again — research §3). So the candidate
/// set is widened by an ancestor walk over every component of every tainted
/// variable: any ancestor whose file name starts with `.mount_` is a mount
/// root. That shape test is confined to *candidate discovery* and is never
/// authoritative on its own — `build_mount_point` in AppImage's `runtime.c` is
/// an implementation detail whose truncation length has already drifted
/// between versions, which is why GitButler's hardcoded substrings are cited
/// as a weakness, not a model.
#[cfg(target_os = "linux")]
fn mount_roots(appdir: &Path, inherited: &[(OsString, OsString)]) -> Vec<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;
    let mut roots: Vec<Vec<u8>> = vec![appdir.as_os_str().as_bytes().to_vec()];
    for name in TAINTED_VARS {
        let Some(value) = lookup(inherited, name) else {
            continue;
        };
        for component in split_colon_list(value) {
            if component.is_empty() {
                continue;
            }
            for ancestor in Path::new(&component).ancestors() {
                let shaped = ancestor
                    .file_name()
                    .is_some_and(|name| name.as_bytes().starts_with(b".mount_"));
                if !shaped {
                    continue;
                }
                // `ancestors()` normalises the hook's `//` once it starts
                // trimming, so a root discovered through
                // `$APPDIR//usr/lib/girepository-1.0` is still `$APPDIR`.
                let bytes = ancestor.as_os_str().as_bytes().to_vec();
                if !roots.contains(&bytes) {
                    roots.push(bytes);
                }
            }
        }
    }
    roots
}

/// Whether `candidate` is one of `roots` or lives under one of them.
#[cfg(target_os = "linux")]
fn under_a_root(candidate: &OsStr, roots: &[Vec<u8>]) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let candidate = candidate.as_bytes();
    roots.iter().any(|root| under_root(candidate, root))
}

/// Byte-prefix containment, deliberately NOT `Path::starts_with`: the bytes
/// are what the value actually says, and the two producer shapes this has to
/// match are byte artefacts — `AppRun.c` emits a trailing slash
/// (`/tmp/.mount_x/usr/lib/`) and the GTK hook a DOUBLE slash
/// (`$APPDIR//usr/lib/girepository-1.0`). Both are "the root, then a `/`".
/// A sibling mount (`.mount_archimgAAJCIx` against `.mount_archimgAAJCI`) is
/// rejected because the byte after the prefix is not `/`. The `root` is
/// non-empty by the `appimage_mount_root` gate; the guard here is the
/// defence-in-depth version of it, because an empty root would match every
/// absolute entry.
#[cfg(target_os = "linux")]
fn under_root(candidate: &[u8], root: &[u8]) -> bool {
    !root.is_empty()
        && (candidate == root
            || (candidate.starts_with(root) && candidate.get(root.len()) == Some(&b'/')))
}

/// Rejoin components in their original relative order. `unsafe`-free: like
/// [`split_colon_list`] this is the Unix `ffi` route, so no path is lossy.
#[cfg(target_os = "linux")]
fn join_colon_list(components: &[OsString]) -> OsString {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    let mut bytes = Vec::new();
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            bytes.push(b':');
        }
        bytes.extend_from_slice(component.as_bytes());
    }
    OsString::from_vec(bytes)
}

/// Split a colon-list value on `:` byte-exactly, so every byte of a user's
/// path survives the round-trip.
///
/// `OsStrExt::as_bytes` / `OsString::from_vec` are safe on Unix, which is why
/// this module needs no `unsafe` at all — the alternative
/// (`as_encoded_bytes` + `from_encoded_bytes_unchecked`) would need it. The
/// `use` is inside this `#[cfg(unix)]` scope on purpose: at module top level it
/// is a compile error on `windows-latest` before any other `cfg` can help. The
/// `allow` is because the only caller is Linux-only, so on macOS this is
/// compiled and unused — and CI runs `clippy -D warnings` there.
#[cfg(unix)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn split_colon_list(value: &OsStr) -> Vec<OsString> {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    value
        .as_bytes()
        .split(|byte| *byte == b':')
        .map(|bytes| OsString::from_vec(bytes.to_vec()))
        .collect()
}

/// The non-Linux `scrub`: nothing sets these variables on macOS or Windows,
/// so there is nothing to strip and the patch is always empty.
#[cfg(not(target_os = "linux"))]
pub fn scrub(
    _inherited: Vec<(OsString, OsString)>,
    _appdir: Option<&Path>,
    _exe: &Path,
) -> EnvPatch {
    EnvPatch::default()
}

#[cfg(test)]
mod tests {
    //! The gate tests (1–4, 10, 13) run on every target. The strip tests
    //! (5–9, 11, 12, 14) are `#[cfg(target_os = "linux")]`: off Linux `scrub`
    //! is *defined* to return the empty patch, so writing their assertions
    //! unguarded would make them pass vacuously on macOS and Windows. Gating
    //! them at compile time keeps that honest — `cargo test hostenv` reports
    //! 14 tests on Linux and 6 elsewhere — and mirrors `sandbox.rs`, whose
    //! whole Linux-only test module is `cfg`'d out the same way.
    //!
    //! None of them calls [`for_child()`]: under `cargo test` `current_exe()`
    //! is `target/debug/deps/<hash>`, never under `$APPDIR`, so the cached
    //! patch is empty on every host and such a test would assert nothing.

    use super::*;
    use std::ffi::OsStr;

    /// The mount root the fabricated cases use — the shape the AppImage
    /// runtime actually produces (`build_mount_point` in `runtime.c`).
    const MOUNT: &str = "/tmp/.mount_archimgAAJCI";
    /// The exe of a process running FROM [`MOUNT`]: inside an AppImage
    /// `current_exe()` resolves through the mount (`/proc/self/exe`).
    const EXE_INSIDE: &str = "/tmp/.mount_archimgAAJCI/usr/bin/archimedes";
    /// The exe of a `.deb` / `.rpm` / `cargo run` host: NOT under `$APPDIR`.
    const EXE_OUTSIDE: &str = "/usr/bin/archimedes";
    /// The older mount generation this host actually has (measured: `PATH`
    /// entries under two `.mount_archim*` dirs while `APPDIR` names one).
    const STALE_MOUNT: &str = "/tmp/.mount_archimPfJcAP";

    /// An `inherited` environment from `(name, value)` pairs.
    fn env(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(k, v)| (OsString::from(*k), OsString::from(*v)))
            .collect()
    }

    /// The value the patch would `env()` for `key`, if it would set it at all.
    fn set_of(patch: &EnvPatch, key: &str) -> Option<OsString> {
        patch
            .set
            .iter()
            .find(|(k, _)| k == OsStr::new(key))
            .map(|(_, v)| v.clone())
    }

    /// Whether the patch would `env_remove(key)`.
    fn removes(patch: &EnvPatch, key: &str) -> bool {
        patch.remove.iter().any(|k| k == OsStr::new(key))
    }

    /// Neither half of the patch mentions `key`.
    fn untouched(patch: &EnvPatch, key: &str) -> bool {
        set_of(patch, key).is_none() && !removes(patch, key)
    }

    /// Apply a patch to an in-memory env the way `apply` does to a `Command`,
    /// so the idempotency test can feed the result back through `scrub`.
    /// Removals drop the entry (they do NOT empty it — that is the point).
    #[cfg(target_os = "linux")]
    fn apply_to(target: &mut Vec<(OsString, OsString)>, patch: &EnvPatch) {
        for key in &patch.remove {
            target.retain(|(k, _)| k != key);
        }
        for (key, value) in &patch.set {
            match target.iter_mut().find(|(k, _)| k == key) {
                Some(slot) => slot.1 = value.clone(),
                None => target.push((key.clone(), value.clone())),
            }
        }
    }

    // ── 1–4: the gate ────────────────────────────────────────────────────

    /// Claim: with no `APPDIR` there is no mount, so nothing may be touched.
    /// Asserting `is_empty()` on an empty input env would pass vacuously, so
    /// the env here is the real poisoned shape — and the exe is deliberately
    /// one that IS under a mount, so only `APPDIR`'s absence can close the gate.
    #[test]
    fn outside_an_appimage_nothing_is_touched() {
        let patch = scrub(
            env(&[
                ("PATH", "/tmp/.mount_archimgAAJCI/usr/bin/:/usr/bin"),
                ("LD_LIBRARY_PATH", "/tmp/.mount_archimgAAJCI/usr/lib/:"),
            ]),
            None,
            Path::new(EXE_INSIDE),
        );
        assert!(
            patch.is_empty(),
            "no APPDIR must mean no patch, got {patch:?}"
        );
    }

    /// Claim: `APPDIR` alone is not the test — the exe must be inside it.
    /// A user's own shell may legitimately `export APPDIR=…`; scrubbing then
    /// would strip their REAL `PATH`. A naive "is `APPIMAGE`/`APPDIR` set?"
    /// implementation passes the `appdir: None` case and fails here, which is
    /// why `appdir` is well-formed and mount-shaped rather than absent.
    #[test]
    fn a_foreign_exe_gates_the_scrub_off() {
        let patch = scrub(
            env(&[("PATH", "/tmp/.mount_archimgAAJCI/usr/bin/:/usr/bin")]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_OUTSIDE),
        );
        assert!(
            patch.is_empty(),
            "an exe outside $APPDIR means we are NOT in an AppImage, got {patch:?}"
        );
    }

    /// Claim: `APPDIR=""` — the classic "unset-ish" `export APPDIR=` idiom,
    /// and exactly what a CI script does — must NOT scrub anything.
    /// `Path::new("").starts_with` is `true` for EVERY path and the empty
    /// mount root byte-prefix-matches every absolute entry, so without the
    /// guard every tainted variable is emptied and `env_remove`d: the child
    /// still exits 0 and silently loses `~/.cargo/bin`, `~/.local/bin`,
    /// `/opt/nvim`. That data loss is the bug this test pins.
    #[test]
    fn an_empty_appdir_gates_the_scrub_off() {
        let tainted = env(&[
            (
                "PATH",
                "/tmp/.mount_archimgAAJCI/usr/bin/:/home/u/.cargo/bin",
            ),
            ("LD_LIBRARY_PATH", "/tmp/.mount_archimgAAJCI/usr/lib/"),
        ]);
        let patch = scrub(tainted.clone(), Some(Path::new("")), Path::new(EXE_INSIDE));
        assert!(
            patch.is_empty(),
            "APPDIR=\"\" must gate the scrub off, got {patch:?}"
        );
        // Stated separately from `is_empty` so the data loss is named: it is
        // `env_remove` that silently eats the user's PATH.
        assert!(
            !removes(&patch, "PATH"),
            "APPDIR=\"\" must never remove PATH, got {patch:?}"
        );
        // Positive control: the SAME env with a usable appdir does produce a
        // patch, so the empty-`APPDIR` case is not inert by construction — only
        // the guard tells the two apart. (Off Linux `scrub` is defined to be
        // empty, so the control is a Linux claim.)
        if cfg!(target_os = "linux") {
            let control = scrub(tainted, Some(Path::new(MOUNT)), Path::new(EXE_INSIDE));
            assert_eq!(
                set_of(&control, "PATH").as_deref(),
                Some(OsStr::new("/home/u/.cargo/bin")),
                "the control case must genuinely strip, or the test above proves nothing"
            );
        }
    }

    /// Claim: `APPDIR="/"` must not scrub either. Unlike the empty case this
    /// one cannot be caught by the shape check (`/usr` IS a directory on every
    /// Unix), so it pins the component-count rule on its own: `/` has no
    /// `Component::Normal`, and as a mount root it byte-prefix-matches every
    /// `//`-prefixed entry while the ancestor walk makes it scrub whatever
    /// `.mount_` generations the env happens to mention.
    ///
    /// The input is chosen so a gate that accepts `/` produces BOTH halves of
    /// the damage (a `set` for `PATH`, a `remove` for `GI_TYPELIB_PATH` — that
    /// `//` value is what the GTK hook's substitution yields from an empty
    /// `$APPDIR`), so this test cannot pass for want of something to break.
    #[test]
    fn a_root_appdir_gates_the_scrub_off() {
        let stale_generation = format!("{STALE_MOUNT}/usr/bin/:/home/u/bin");
        let patch = scrub(
            env(&[
                ("PATH", stale_generation.as_str()),
                ("GI_TYPELIB_PATH", "//usr/lib/girepository-1.0"),
            ]),
            Some(Path::new("/")),
            Path::new(EXE_OUTSIDE),
        );
        assert!(
            patch.is_empty(),
            "APPDIR=\"/\" must gate the scrub off, got {patch:?}"
        );
    }

    // ── 5–9: the strip ───────────────────────────────────────────────────

    /// Claim: mount components are stripped and the user's own entries survive
    /// IN ORDER. Asserting only "no `.mount_` substring" would also pass for an
    /// implementation that dropped `PATH` wholesale, so the exact surviving
    /// value — and its order — is what is asserted here.
    #[cfg(target_os = "linux")]
    #[test]
    fn mount_entries_are_stripped_and_the_user_entries_survive_in_order() {
        let patch = scrub(
            env(&[(
                "PATH",
                "/tmp/.mount_archimgAAJCI/usr/bin/:/tmp/.mount_archimgAAJCI/bin/:/home/u/.cargo/bin:/usr/bin",
            )]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert_eq!(
            set_of(&patch, "PATH").as_deref(),
            Some(OsStr::new("/home/u/.cargo/bin:/usr/bin")),
            "the user's entries must survive, in their original relative order"
        );
        assert!(!removes(&patch, "PATH"), "PATH is filtered, never dropped");
    }

    /// Claim: a variable the AppRun INVENTED (nothing of the user's survived
    /// the strip) is `env_remove`d, not set to `""`. `env(v, "")` is not
    /// equivalent to removing it — an empty `LD_LIBRARY_PATH` still joins the
    /// search path and, with the trailing-colon semantics, an empty component
    /// reads as the current directory — and the variable may not have existed
    /// before the AppRun created it. Asserting the `set` half is empty for the
    /// key is the part that fails if `""` is emitted instead.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_variable_the_runtime_invented_is_removed_not_emptied() {
        let patch = scrub(
            env(&[(
                "LD_LIBRARY_PATH",
                "/tmp/.mount_archimgAAJCI/usr/lib/:/tmp/.mount_archimgAAJCI/usr/lib64/",
            )]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert!(
            removes(&patch, "LD_LIBRARY_PATH"),
            "a fully-stripped variable must be removed, got {patch:?}"
        );
        assert!(
            set_of(&patch, "LD_LIBRARY_PATH").is_none(),
            "…and never set to an empty string, got {patch:?}"
        );
    }

    /// Claim: a user component that sits AFTER the mount entries survives —
    /// the loss-freedom guarantee `env_remove` cannot give, and the reason this
    /// module strips instead of unsetting. The prepending (`…:%s`) idiom in
    /// `AppRun.c` is what makes this recoverable.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_user_component_after_the_mount_entries_survives() {
        let patch = scrub(
            env(&[(
                "LD_LIBRARY_PATH",
                "/tmp/.mount_archimgAAJCI/usr/lib/:/opt/vendor/lib",
            )]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert_eq!(
            set_of(&patch, "LD_LIBRARY_PATH").as_deref(),
            Some(OsStr::new("/opt/vendor/lib"))
        );
        assert!(
            !removes(&patch, "LD_LIBRARY_PATH"),
            "a variable with a surviving user component must NOT be removed"
        );
    }

    /// Claim: a STALE generation is stripped too. This host measurably carries
    /// `PATH` entries under two `.mount_archim*` dirs while `$APPDIR` names
    /// only one (the updater relaunch re-prepends; see research §3), so the
    /// mount roots are `$APPDIR` ∪ every `.mount_` ancestor found in the
    /// inherited values. Only asserting `$APPDIR`-prefixed entries are gone
    /// would pass for an implementation that knows just the one generation.
    #[cfg(target_os = "linux")]
    #[test]
    fn stale_generations_are_stripped_too() {
        // `APPDIR` names only the NEWER mount; the older one is named by
        // nothing but this entry — that is the regression, and discovering it
        // needs the ancestor walk, not `$APPDIR`.
        let two_generations = format!("{STALE_MOUNT}/usr/bin/:/home/u/bin:{MOUNT}/usr/bin/");
        let patch = scrub(
            env(&[("PATH", two_generations.as_str())]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        let value = set_of(&patch, "PATH").expect("PATH must be re-set");
        assert_eq!(value, OsStr::new("/home/u/bin"), "{value:?}");
        assert!(
            !value.to_string_lossy().contains(".mount_"),
            "no generation may survive, got {value:?}"
        );
    }

    /// Claim: empty components are DROPPED, and dropping one still counts as a
    /// change (so the patch carries a `set`). `AppRun.c`'s `%s…:%s` always
    /// leaves a trailing `:` when the prior value was empty, and `ld.so`/`PATH`
    /// read an empty component as the CURRENT DIRECTORY (AppImageKit#391; the
    /// same idiom in electron-builder is CVE-2026-54672). Asserting the value
    /// alone would also pass for an implementation that kept the trailing `:`
    /// and reported nothing, hence the `set`-half assertion.
    #[cfg(target_os = "linux")]
    #[test]
    fn empty_components_are_dropped() {
        let patch = scrub(
            env(&[(
                "LD_LIBRARY_PATH",
                "/tmp/.mount_archimgAAJCI/usr/lib/:/usr/lib:",
            )]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert_eq!(
            set_of(&patch, "LD_LIBRARY_PATH").as_deref(),
            Some(OsStr::new("/usr/lib")),
            "the trailing empty component must be dropped"
        );
        assert!(
            !removes(&patch, "LD_LIBRARY_PATH"),
            "a surviving component means the variable stays, got {patch:?}"
        );
    }

    // ── 10–12: patch hygiene ─────────────────────────────────────────────

    /// Claim: a variable that needs no change appears in NEITHER half of the
    /// patch (rule 3c). Without this, `scrub` emits a `set` for every tainted
    /// variable on every host, `for_child().is_empty()` is never true, and the
    /// call sites in Tasks 2–4 would rewrite the env of every child on a
    /// `.deb` install. Off Linux this holds by definition and so is trivially
    /// true — the Linux-meaningful version is the idempotency test below.
    #[test]
    fn a_variable_that_needs_no_change_is_not_in_the_patch_at_all() {
        let patch = scrub(
            env(&[("PATH", "/usr/bin:/bin")]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert!(
            untouched(&patch, "PATH"),
            "an unchanged value must appear in neither half, got {patch:?}"
        );
    }

    /// Claim: a variable absent from the inherited env is never INVENTED —
    /// `scrub` only ever edits what is already there. The control assertion
    /// (the tainted `PATH` in the same input WAS patched) is what stops this
    /// passing because the gate closed instead of because the rule holds.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_variable_absent_from_the_inherited_env_is_never_invented() {
        let patch = scrub(
            env(&[("PATH", "/tmp/.mount_archimgAAJCI/usr/bin/:/usr/bin")]),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert!(
            untouched(&patch, "PYTHONHOME"),
            "no PYTHONHOME was inherited, so none may be invented, got {patch:?}"
        );
        assert!(
            set_of(&patch, "PATH").is_some(),
            "control: the gate must be open for this to prove anything"
        );
    }

    /// Claim: the patch is IDEMPOTENT — applying it and scrubbing again
    /// produces an empty patch. This is what stops the env growing across the
    /// main → Worker → `bash` hops, and it depends on rule 3c (a variable that
    /// needs no change is not re-`set`). Feeding back a hand-written env
    /// instead of the applied result would test nothing about the composition.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_scrub_is_idempotent() {
        let inherited = env(&[
            (
                "PATH",
                "/tmp/.mount_archimgAAJCI/usr/bin/:/home/u/bin:/usr/bin:",
            ),
            (
                "LD_LIBRARY_PATH",
                "/tmp/.mount_archimgAAJCI/usr/lib/:/tmp/.mount_archimgAAJCI/usr/lib64/",
            ),
            (
                "GI_TYPELIB_PATH",
                "/tmp/.mount_archimgAAJCI//usr/lib/girepository-1.0",
            ),
            (
                "XDG_DATA_DIRS",
                "/home/u/.local/share/flatpak/exports/share:/usr/share",
            ),
            ("HOME", "/home/u"),
        ]);
        let first = scrub(
            inherited.clone(),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE),
        );
        assert!(!first.is_empty(), "control: this env is genuinely tainted");
        let mut after = inherited;
        apply_to(&mut after, &first);
        let second = scrub(after, Some(Path::new(MOUNT)), Path::new(EXE_INSIDE));
        assert!(
            second.is_empty(),
            "scrub(scrub(env)) must be a no-op, got {second:?}"
        );
    }

    // ── 13: the derived list ─────────────────────────────────────────────

    /// Claim: `TAINTED_VARS` covers everything the shipped bundle actually
    /// exports. The two literals below are the names DERIVED from the built
    /// bundle at
    /// `target/release/bundle/appimage/Archimedes.AppDir/`, transcribed so a
    /// bundle change that adds a variable reddens this test:
    ///
    /// * `AppRun.wrapped` (the compiled `AppRun.c`):
    ///   `strings AppRun.wrapped | grep -E '^[A-Z0-9_]+=%s' | sed 's/=.*//'`
    ///   — the plan's `^[A-Z_]+=%s` misses `GST_PLUGIN_SYSTEM_PATH_1_0`
    ///   (digits in the name), which this bundle does export, so the class is
    ///   widened to `[A-Z0-9_]`.
    /// * `apprun-hooks/linuxdeploy-plugin-gtk.sh` (the only hook here):
    ///   `grep -oE '^\s*export [A-Z_]+' … | sed 's/.*export //'`
    ///
    /// The relation asserted is `derived ⊆ TAINTED_VARS` — a SUPERSET relation,
    /// NOT equality: `TAINTED_VARS` deliberately carries four more names
    /// nothing in this bundle exports (see `UNBUNDLED_DEFENCE_IN_DEPTH`).
    /// Equality against the derived list alone fails by design.
    #[test]
    fn the_tainted_var_list_covers_everything_the_bundle_exports() {
        /// What `AppRun.wrapped` prepends (the `%s`-templated `putenv` set).
        const APPRUN_VARS: &[&str] = &[
            "GSETTINGS_SCHEMA_DIR",
            "GST_PLUGIN_SYSTEM_PATH",
            "GST_PLUGIN_SYSTEM_PATH_1_0",
            "LD_LIBRARY_PATH",
            "PATH",
            "PERLLIB",
            "PYTHONHOME",
            "PYTHONPATH",
            "QT_PLUGIN_PATH",
            "XDG_DATA_DIRS",
        ];
        /// What `linuxdeploy-plugin-gtk.sh` exports. `APPDIR` and `GTK_THEME`
        /// are in the hook's output and are NOT scrubbed — see
        /// `DELIBERATELY_NOT_SCRUBBED` for why they are listed there rather
        /// than dropped from this literal, which is the hook's raw output.
        const GTK_HOOK_VARS: &[&str] = &[
            "APPDIR",
            "GDK_PIXBUF_MODULE_FILE",
            "GIO_MODULE_DIR",
            "GI_TYPELIB_PATH",
            "GSETTINGS_SCHEMA_DIR",
            "GTK_DATA_PREFIX",
            "GTK_EXE_PREFIX",
            "GTK_IM_MODULE_FILE",
            "GTK_PATH",
            "GTK_THEME",
            "XDG_DATA_DIRS",
        ];
        /// Names the hook exports that are deliberately NOT in `TAINTED_VARS`:
        /// `APPDIR` is the AppImage contract a user's own script may read, and
        /// `GTK_THEME` is not a path (and is the GUI's own business). Pinning
        /// them here keeps "not scrubbed" a decision rather than an oversight.
        const DELIBERATELY_NOT_SCRUBBED: &[&str] = &["APPDIR", "GTK_THEME"];
        /// Defence-in-depth against a future plugin set (this bundle has no
        /// gstreamer/qt/checkrt hook, so nothing exports these — note
        /// `AppDir/usr/optional` does not exist, hence no `checkrt`).
        const UNBUNDLED_DEFENCE_IN_DEPTH: &[&str] = &[
            "GIO_EXTRA_MODULES",
            "GDK_PIXBUF_MODULEDIR",
            "GST_PLUGIN_PATH",
            "GST_PLUGIN_SCANNER",
        ];

        // Every derived, scrub-worthy name must be covered.
        let mut missing: Vec<&str> = Vec::new();
        for name in APPRUN_VARS.iter().chain(GTK_HOOK_VARS.iter()) {
            if DELIBERATELY_NOT_SCRUBBED.contains(name) {
                assert!(
                    !TAINTED_VARS.contains(name),
                    "{name} is excluded by design and must not be scrubbed"
                );
                continue;
            }
            if !TAINTED_VARS.contains(name) {
                missing.push(name);
            }
        }
        assert!(
            missing.is_empty(),
            "the bundle exports {missing:?}, which `scrub` would leave in a child's env"
        );

        // And every name in `TAINTED_VARS` must be justified — either derived
        // from the bundle or listed as deliberate defence-in-depth. This is
        // pinned SORTED, so the order of the const (which is the order the
        // patch reports) stays free.
        let mut justified: Vec<&str> = APPRUN_VARS
            .iter()
            .chain(GTK_HOOK_VARS.iter())
            .chain(UNBUNDLED_DEFENCE_IN_DEPTH.iter())
            .copied()
            .filter(|n| !DELIBERATELY_NOT_SCRUBBED.contains(n))
            .collect();
        justified.sort_unstable();
        justified.dedup();
        assert_eq!(
            sorted(TAINTED_VARS),
            justified,
            "`TAINTED_VARS` gained or lost a name with no matching entry in the \
             derived / defence-in-depth literals above"
        );

        // `LD_PRELOAD` (space-separated — the split-on-`:` rule cannot express
        // it) and `PYTHONDONTWRITEBYTECODE` (not a path) stay out.
        for excluded in ["LD_PRELOAD", "PYTHONDONTWRITEBYTECODE", "OWD", "ARGV0"] {
            assert!(
                !TAINTED_VARS.contains(&excluded),
                "{excluded} must never be in `TAINTED_VARS`"
            );
        }
    }

    /// The names sorted for a stable comparison (the const's order is the
    /// patch's order and must not be pinned by this test).
    fn sorted<'a>(names: &'a [&'a str]) -> Vec<&'a str> {
        let mut names = names.to_vec();
        names.sort_unstable();
        names
    }

    // ── 14: the end-to-end proof ─────────────────────────────────────────

    /// Claim: `apply` really reaches `execve` — a child spawned with
    /// `env_clear()` plus the patch sees the patched value, which is the only
    /// way the `env` calls could matter at all. The paths are fabricated and
    /// need not exist: the gate is lexical (the exe-containment test plus a
    /// mount-shaped `appdir`), so no temp dir is required.
    ///
    /// ⚠ `env_clear()`-then-`apply` is sound for the **`set`** half, which is
    /// all this test uses; it would be VACUOUS for a `remove` assertion, since
    /// removing a variable after clearing the env is a no-op either way. That
    /// is why the case carries a surviving user component.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_patch_actually_reaches_a_real_child() {
        let fake = "/tmp/.mount_archimTESTCASEAAAA";
        let patch = scrub(
            env(&[
                (
                    "LD_LIBRARY_PATH",
                    "/tmp/.mount_archimTESTCASEAAAA/usr/lib/:/usr/lib",
                ),
                ("PATH", "/tmp/.mount_archimTESTCASEAAAA/usr/bin/:/usr/bin"),
            ]),
            Some(Path::new(fake)),
            Path::new("/tmp/.mount_archimTESTCASEAAAA/usr/bin/archimedes"),
        );
        assert_eq!(
            set_of(&patch, "LD_LIBRARY_PATH").as_deref(),
            Some(OsStr::new("/usr/lib")),
            "control: the patch must carry the value the child is asserted to see"
        );

        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.env_clear();
        patch.apply(&mut cmd);
        cmd.arg("-c").arg("printf %s \"$LD_LIBRARY_PATH\"");
        let out = cmd.output().await.expect("spawn /bin/sh");
        assert!(
            out.status.success(),
            "the child failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            "/usr/lib",
            "the patch did not reach the child's environment"
        );
    }
}

/// The fabricated AppImage host the CALL-SITE tests inject into [`scrub`].
///
/// A call-site test cannot use [`for_child`]: under `cargo test` the production
/// gate is CLOSED (`current_exe()` is `target/debug/deps/<hash>`, never under
/// `$APPDIR`), so it hands back the EMPTY patch on every host and an assertion
/// built on it proves nothing — see its doc. These fixtures fabricate the host
/// instead, so a test can ask a spawn site "what does the patch do to this
/// `Command`?" with a patch that is genuinely non-empty:
///
/// * [`MOUNT`] is `appdir`-shaped the way the AppImage runtime names a mount
///   point (`build_mount_point` in `runtime.c`), which is the structural
///   corroboration [`scrub`]'s gate accepts — so NO tree has to exist on disk
///   and the fabrications stay deterministic on every Linux runner.
/// * [`tainted_inherited`] is the taint `AppRun.c` and the GTK hook actually
///   write, with one non-mount entry in it: without a survivor the patch would
///   `remove` every variable and a call site that dropped the user's own paths
///   could not be told from a correct one.
///
/// One place rather than a copy per call site, because these encode THIS
/// module's gate shape: when the gate or the tainted list moves, the fixtures
/// move with it and every consumer's expectations move with them.
///
/// Linux-only: off Linux [`scrub`] is DEFINED to return the empty patch, so
/// nothing there consumes this module, and `clippy --all-targets -D warnings`
/// would report it as `dead_code` on the Windows/macOS CI rows.
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod fixtures {
    use super::*;

    /// The fabricated mount root.
    pub(crate) const MOUNT: &str = "/tmp/.mount_archimTEST";
    /// The `current_exe()` of a process running FROM [`MOUNT`] (in a real
    /// AppImage it resolves through the mount via `/proc/self/exe`).
    pub(crate) const EXE_INSIDE_MOUNT: &str = "/tmp/.mount_archimTEST/usr/bin/archimedes";
    /// The entry that is NOT the bundle's and must survive every scrub, so
    /// "stripped everything" is never mistaken for "stripped the mount".
    pub(crate) const USER_BIN: &str = "/opt/test-unique/bin";
    /// The value the scrub leaves for `LD_LIBRARY_PATH` (the surviving entry).
    pub(crate) const SCRUBBED_LD_LIBRARY_PATH: &str = "/usr/lib";

    /// The taint, in the shapes `AppRun.c` emits: the mount dir at position 0,
    /// a TRAILING SLASH + colon on `LD_LIBRARY_PATH`, and `PYTHONHOME` as a
    /// SINGLE path inside the mount (so the patch must REMOVE it — an empty
    /// value is not an absent one).
    pub(crate) fn tainted_inherited() -> Vec<(OsString, OsString)> {
        vec![
            (
                OsString::from("LD_LIBRARY_PATH"),
                OsString::from(format!("{MOUNT}/usr/lib/:/usr/lib")),
            ),
            (
                OsString::from("PATH"),
                OsString::from(format!("{MOUNT}/bin:{USER_BIN}")),
            ),
            (
                OsString::from("PYTHONHOME"),
                OsString::from(format!("{MOUNT}/usr")),
            ),
        ]
    }

    /// The patch [`tainted_inherited`] produces, asserted non-empty: a test
    /// whose patch is empty cannot fail, so it must not be run at all.
    pub(crate) fn tainted_patch() -> EnvPatch {
        let patch = scrub(
            tainted_inherited(),
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE_MOUNT),
        );
        assert!(
            !patch.is_empty(),
            "the fabricated host produced an EMPTY patch, so every test built on it \
             asserts nothing: {patch:?}"
        );
        patch
    }

    /// The environment a `Command` was TOLD to carry, as a map — the only
    /// honest introspection available, because `tokio::process::Command` has no
    /// `get_envs` (hence `as_std()`), and because `get_envs` reports an
    /// `env_remove` as a `None`-valued entry (it lists the configured changes,
    /// not the inherited env).
    pub(crate) fn configured_env(
        cmd: &tokio::process::Command,
    ) -> std::collections::BTreeMap<OsString, Option<OsString>> {
        cmd.as_std()
            .get_envs()
            .map(|(key, value)| (key.to_os_string(), value.map(|v| v.to_os_string())))
            .collect()
    }

    /// The env a command must carry when the ONLY thing done to it is
    /// `patch.apply` — the expectation, built by `scrub` + the real
    /// [`EnvPatch::apply`], never by reading the patch's internals (they are
    /// private on purpose) and never by `for_child()`.
    ///
    /// Deliberately NO `env_clear()`: std reports `get_envs` as empty after one
    /// and DROPS an `env_remove` issued afterwards (measured), so clearing here
    /// would hide exactly the half of the patch the ordering test is about. A
    /// bare `Command` reports every configured change — and `get_envs` never
    /// lists the INHERITED env, so there is nothing else to clear away.
    pub(crate) fn reference_env(
        patch: &EnvPatch,
    ) -> std::collections::BTreeMap<OsString, Option<OsString>> {
        let mut cmd = tokio::process::Command::new("true");
        patch.apply(&mut cmd);
        configured_env(&cmd)
    }

    /// Whether a colon list names a mount path ANYWHERE in it (an independent
    /// restatement of the shape rule, so a call site that scrubbed via a bug in
    /// this module's own matcher is still caught).
    pub(crate) fn lists_a_mount_path(value: &OsStr) -> bool {
        use std::os::unix::ffi::OsStrExt;
        std::env::split_paths(value).any(|component| {
            Path::new(&component).ancestors().any(|ancestor| {
                ancestor
                    .file_name()
                    .is_some_and(|name| name.as_bytes().starts_with(b".mount_"))
            })
        })
    }
}

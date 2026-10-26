// ── hostenv: the AppImage child-env scrub, at the CALL SITES ────────
//
// `crate::agent::hostenv` pins the RULE (what a scrubbed child env looks
// like). This section pins the three call sites that were leaking it:
// `exec_bash`, and the `fd` / `rg` fast paths of `exec_find` /
// `exec_grep`. Evidence for the defect: the research doc,
// `docs/research/appimage-child-process-env-leak.md`.
//
// Every test here is `#[cfg(target_os = "linux")]`, mirroring `hostenv`'s
// own test module: off Linux `scrub` is DEFINED to return the empty patch,
// so the same assertions would pass VACUOUSLY on macOS and Windows.
// Compile-time gating is the honest form of "not applicable here" — a
// runtime `if` would hide whether anything ran.

#[cfg(target_os = "linux")]
use std::ffi::{OsStr, OsString};
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
use serde_json::json;

#[cfg(target_os = "linux")]
use super::support::*;
#[cfg(target_os = "linux")]
use crate::agent::tools::exec::execute_tool;

/// How many hostenv tests SKIPPED because the scrub was unobservable on
/// this host. Counted, not merely printed —
/// `the_skip_is_a_failure_on_a_capable_host` asserts on the count.
#[cfg(target_os = "linux")]
static SKIPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The fabricated mount root: named the way `build_mount_point` in
/// AppImage's `runtime.c` names a real mount point, which is the shape
/// `hostenv`'s structural corroboration accepts — so the unit-level tests
/// need no tree on disk (and no temp-dir cleanup).
#[cfg(target_os = "linux")]
const MOUNT: &str = "/tmp/.mount_archimTEST";

/// The `current_exe()` of a process running FROM [`MOUNT`].
#[cfg(target_os = "linux")]
const EXE_INSIDE_MOUNT: &str = "/tmp/.mount_archimTEST/usr/bin/archimedes";

/// Set on the re-spawned test binary to tell it that it IS running from
/// inside the fabricated mount (see
/// `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`).
#[cfg(target_os = "linux")]
const FABRICATED: &str = "ARCHIMIDES_HOSTENV_FABRICATED_MOUNT";
/// The dir holding the `fd` / `rg` shims, passed to that child (whose own
/// `PATH` is the scrubbed one, so it cannot be located from inside).
#[cfg(target_os = "linux")]
const SHIM_DIR: &str = "ARCHIMIDES_HOSTENV_SHIM_DIR";
/// Which fast-path binaries the parent found on its `PATH` and shimmed
/// (comma list), so the child knows which assertions it may make.
#[cfg(target_os = "linux")]
const FAST_PATHS: &str = "ARCHIMIDES_HOSTENV_FAST_PATHS";

/// The separator for the `printf` probes (`:` would be ambiguous inside
/// the values themselves, and no path in these tests contains this).
#[cfg(target_os = "linux")]
const SEP: &str = "@HOSTENV@";

/// Printed by the child once every fabricated-mount assertion passed, and
/// by the reporter when it saw a host the scrub was live on. The parent
/// requires BOTH: a child whose filter matched nothing prints
/// `running 0 tests` and exits 0 — a pass that proves nothing.
#[cfg(target_os = "linux")]
const CHILD_RAN: &str = "HOSTENV-CHILD-RAN";
#[cfg(target_os = "linux")]
const COVERAGE_FULL: &str = "HOSTENV-COVERAGE: FULL";

/// Whether `component` IS, or lives under, a directory named like an
/// AppImage mount point (`build_mount_point`, `runtime.c`).
#[cfg(target_os = "linux")]
fn under_a_mount(component: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;
    Path::new(component).ancestors().any(|a| {
        a.file_name()
            .is_some_and(|n| n.as_bytes().starts_with(b".mount_"))
    })
}

/// Whether a colon list names a mount path anywhere in it.
#[cfg(target_os = "linux")]
fn lists_a_mount_path(value: &OsStr) -> bool {
    std::env::split_paths(value).any(|c| under_a_mount(c.as_os_str()))
}

/// What a child SHOULD still see of a colon list: every non-empty
/// component that is not inside a mount. (The bundle's trailing `:`
/// carries no entry, and `ld.so` reads an empty component as the CWD, so
/// dropping it is part of the rule.)
///
/// This ORACLE is written from the rule in the research doc and does NOT
/// call `hostenv`: an expectation computed by the code under test cannot
/// catch the rule itself being wrong.
#[cfg(target_os = "linux")]
fn kept_entries(value: &OsStr) -> Vec<OsString> {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    value
        .as_bytes()
        .split(|byte| *byte == b':')
        .filter(|c| !c.is_empty() && !under_a_mount(OsStr::from_bytes(c)))
        .map(|c| OsString::from_vec(c.to_vec()))
        .collect()
}

/// The value THIS process' child must print for `name`: the inherited
/// value minus the mount entries, rejoined in order. Empty when the patch
/// was right to `env_remove` the variable outright (the real `AppRun` sets
/// `LD_LIBRARY_PATH=$APPDIR/usr/lib/:` and nothing else, so the correct
/// result for THAT variable is often absence, not a shorter value).
#[cfg(target_os = "linux")]
fn expected_child_value(name: &str) -> String {
    let inherited = std::env::var_os(name).unwrap_or_default();
    kept_entries(&inherited)
        .iter()
        .map(|entry| entry.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(":")
}

/// The production gate, recomputed for THIS process: `APPDIR` set, this
/// exe under it, mount shape corroborated. `for_child()` uses exactly
/// this, so `false` means the scrub is INERT in this process — which is
/// the case for every plain `cargo test` (`current_exe()` is
/// `target/debug/deps/<hash>`), and why a test must NOT assert a scrub the
/// production gate would never perform (a dev box that merely exports a
/// stale `APPDIR` would otherwise see a false failure).
#[cfg(target_os = "linux")]
fn running_from_an_appimage_mount() -> bool {
    let Some(appdir) = std::env::var_os("APPDIR") else {
        return false;
    };
    if appdir.is_empty() {
        return false;
    }
    let appdir = Path::new(&appdir);
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/"));
    exe.starts_with(appdir)
        && (appdir.join("usr").is_dir()
            || appdir
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".mount_")))
}

/// Whether the INHERITED env carries mount paths in the variables a child
/// inherits, REGARDLESS of whether this process runs from a mount. A shell
/// opened by `--appimage-run-in-shell`, or one carrying a stale `APPDIR`,
/// looks tainted while `for_child()` is empty BY DESIGN; only the reporter
/// interprets that difference (see its doc).
#[cfg(target_os = "linux")]
fn inherited_env_has_mount_paths() -> bool {
    [
        "PATH",
        "LD_LIBRARY_PATH",
        "GI_TYPELIB_PATH",
        "XDG_DATA_DIRS",
        "PYTHONHOME",
    ]
    .into_iter()
    .filter_map(std::env::var_os)
    .any(|value| lists_a_mount_path(&value))
}

/// Record a skip: the greppable marker AND the count the reporter reads.
#[cfg(target_os = "linux")]
fn record_skip(reason: &str) {
    let n = SKIPPED.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    eprintln!("SKIP-HOSTENV: {reason} — skip #{n}");
}

/// 1 — the unit-level proof, routed through the SAME shell the tool uses.
///
/// Claim: a `sh` child started the way `exec_bash` starts one (a
/// `tokio::process::Command` on `sh` with
/// [`EnvPatch::apply`](crate::agent::hostenv::EnvPatch::apply) on it) sees
/// no mount component in `LD_LIBRARY_PATH` and keeps its own `PATH`
/// entries.
///
/// What this proves, honestly: `scrub` + `apply` + what the shell actually
/// reads after `execve`. It does NOT prove that `exec_bash` CALLS them —
/// applying an EMPTY patch at the call site passes this test vacuously.
/// The call-site tests are
/// `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`
/// (which runs the tool from inside a mount) and the assertion it shares
/// with `a_bash_child_on_an_appimage_host_sees_no_mount_paths`.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_scrubbed_sh_child_sees_no_mount_path_but_keeps_the_user_path() {
    // The two `AppRun.c` shapes: mount dir FIRST with a TRAILING SLASH for
    // `LD_LIBRARY_PATH`, mount dir FIRST for `PATH`, one real entry each.
    let inherited = vec![
        (
            OsString::from("LD_LIBRARY_PATH"),
            OsString::from(format!("{MOUNT}/usr/lib/:/usr/lib")),
        ),
        (
            OsString::from("PATH"),
            OsString::from(format!("{MOUNT}/bin:/opt/test-unique/bin")),
        ),
    ];
    let patch = crate::agent::hostenv::scrub(
        inherited,
        Some(Path::new(MOUNT)),
        Path::new(EXE_INSIDE_MOUNT),
    );
    assert!(
        !patch.is_empty(),
        "the fabricated host must produce a patch or this test asserts nothing: {patch:?}"
    );

    let mut cmd = tokio::process::Command::new("/bin/sh");
    cmd.arg("-c")
        .arg(format!("printf '%s{SEP}%s' \"$LD_LIBRARY_PATH\" \"$PATH\""));
    // `env_clear` so the ONLY thing in the child's env is the patch —
    // otherwise a host whose own env happens to satisfy the expectation
    // could pass this test with a patch that did nothing.
    cmd.env_clear();
    patch.apply(&mut cmd);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let out = cmd.output().await.expect("the scrubbed sh starts");
    assert!(out.status.success(), "the scrubbed sh failed: {out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!("/usr/lib{SEP}/opt/test-unique/bin"),
        "the mount entry must be gone and the user's entry must survive, in order"
    );
}

/// 2 — the integration test through the tool: on a host where the scrub
/// is LIVE, the `bash` child must print no mount path at all.
///
/// Its precondition is the PRODUCTION gate, not `APPDIR.is_set()`: the
/// scrub comes from `for_child()`, which also requires this exe to be
/// under `$APPDIR`. An `APPDIR`-only predicate would assert a scrub that
/// production cannot perform (a dev box that exports a stale `APPDIR` from
/// a previous `--appimage-run-in-shell` would fail spuriously while the
/// code is correct), so it skips — loudly — whenever this process is not
/// itself mounted.
///
/// That would make it skip on every CI runner, so
/// `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`
/// re-executes THIS test from inside a fabricated mount: the assertion
/// then runs on every Linux job, not only on a machine that happens to be
/// running the bundle.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_bash_child_on_an_appimage_host_sees_no_mount_paths() {
    if !running_from_an_appimage_mount() {
        record_skip(
            "this test binary is not running from inside an AppImage mount, so \
             for_child() is empty BY DESIGN and the scrub is unobservable here \
             (the fabricated-mount test runs this test from inside one)",
        );
        return;
    }
    assert_bash_child_env_is_scrubbed().await;
}

/// 3 — the CONTRAST, and a test that can fail: a call site that
/// `env_clear()`s, or a rule that removes `PATH` instead of filtering it,
/// passes every "no `.mount_`" assertion while silently breaking every
/// child — std hands a PATH-less child `confstr(_CS_PATH)`
/// (`/usr/local/bin:/usr/bin`) and it still exits 0, so `git --version`
/// would NOT catch that. The user's own dir must still be in the child's
/// `PATH`, and it must be ALL that is left.
///
/// This is the rule-level half (it drives `scrub` + `apply` directly, so it
/// pins the filter, not a call site); the call-site half of the same
/// contrast — that the `bash` TOOL's child keeps the user's entry, and that
/// the `fd`/`rg` children do too — is asserted inside the fabricated mount
/// (`fabricated_child_half`, `assert_fast_path_children_are_scrubbed`).
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_bash_tool_keeps_the_users_path_entries() {
    let user_bin = temp_cwd("hostenv-path");
    std::fs::write(user_bin.join("marker-of-the-users-own-bin.txt"), "keep me").unwrap();
    let inherited = vec![(
        OsString::from("PATH"),
        OsString::from(format!("{MOUNT}/bin/:{}", user_bin.display())),
    )];
    let patch = crate::agent::hostenv::scrub(
        inherited,
        Some(Path::new(MOUNT)),
        Path::new(EXE_INSIDE_MOUNT),
    );
    assert!(
        !patch.is_empty(),
        "the fabricated host must produce a patch or this test asserts nothing: {patch:?}"
    );

    let mut cmd = tokio::process::Command::new("/bin/sh");
    cmd.arg("-c").arg("printf %s \"$PATH\"");
    cmd.env_clear();
    patch.apply(&mut cmd);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let out = cmd.output().await.expect("the sh starts");
    assert!(out.status.success(), "the sh failed: {out:?}");
    let printed = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        printed.contains(&user_bin.display().to_string()),
        "the child's PATH lost the user's own entry ({}) — it printed {printed:?}",
        user_bin.display()
    );
    assert_eq!(
        printed,
        user_bin.display().to_string(),
        "only the mount entry may be dropped: the child's whole PATH must be \
         the user's entries, in order"
    );
}

/// 4 — the skip reporter, and an ASSERTION rather than a print.
///
/// Why it must assert: a passing test's `println!`/`eprintln!` output is
/// CAPTURED by the harness, so `cargo test | grep -c SKIP-HOSTENV` reads 0
/// even when every hostenv test skipped, and CI runs a bare `cargo test` —
/// a green suite in which the only call-site test skipped would look like
/// a full pass.
///
/// The two views are deliberately DIFFERENT, and the difference is the
/// assertion's teeth:
///
/// * **capable** = [`running_from_an_appimage_mount`] — the production
///   gate is open HERE, the scrub is live, so any skip is a coverage hole
///   ⇒ PANIC.
/// * **merely tainted** = [`inherited_env_has_mount_paths`] — the inherited
///   env names mount paths but this exe is not under `$APPDIR`, so the
///   scrub is inert BY DESIGN: the honest verdict is the printed line, not
///   a false failure.
///
/// Cargo does not order tests, so reading a counter is inherently racy
/// against the test that writes it. It is not the only mechanism: the
/// fabricated-mount test runs the child with `--test-threads=1` (the
/// harness runs tests in name order and every other hostenv test sorts
/// before `the_skip_…`), so this assertion IS enforced on every Linux run
/// — the child that would skip runs first.
///
/// MUST: on an AppImage host `cargo test -- -- --nocapture` prints ZERO
/// `SKIP-HOSTENV` markers; on a non-AppImage host it MUST print the
/// `host is not an AppImage` verdict line.
#[cfg(target_os = "linux")]
#[test]
fn the_skip_is_a_failure_on_a_capable_host() {
    let skips = SKIPPED.load(std::sync::atomic::Ordering::SeqCst);
    if running_from_an_appimage_mount() {
        assert_eq!(
            skips, 0,
            "SKIP-HOSTENV: {skips} hostenv test(s) SKIPPED on a host that runs this \
             exe from inside an AppImage mount — the scrub is LIVE here, so this run \
             may not claim coverage of it"
        );
        eprintln!("{COVERAGE_FULL} — the bash child's env was observed from inside a mount");
    } else if inherited_env_has_mount_paths() {
        eprintln!(
            "SKIP-HOSTENV: host is not an AppImage, the scrub is unobservable here \
             (the inherited env still names mount paths, but this exe is not under \
             $APPDIR, so for_child() is empty by design — a stale AppRun export)"
        );
    } else {
        eprintln!("SKIP-HOSTENV: host is not an AppImage, the scrub is unobservable here");
    }
}

/// 5 — the deterministic call-site test, on ANY Linux host.
///
/// `for_child()` cannot be made non-empty inside a normal test process
/// (its gate needs `current_exe()` under `$APPDIR`), so THIS test builds
/// the missing half: an AppDir-shaped tree containing a HARD LINK to this
/// very test binary, and a re-execution of the binary from that link with
/// the `AppRun` env stack set. The gate then opens for real, the tool runs
/// for real, and the child's env is what is asserted — no fixture of the
/// patch, no host that happens to be tainted.
///
/// Hard-linked, not copied: `target/debug` is by construction the same
/// filesystem as `target/debug/deps` (where the test binary lives), and
/// the lib test binary is far too big to copy per test.
///
/// The child runs three tests (`--test-threads=1`): this one's child half
/// (`fabricated_child_half`), `a_bash_child_on_an_appimage_host_sees_no_mount_paths`
/// — which no longer skips here — and the reporter, whose capable branch
/// therefore runs on every Linux CI job. The parent asserts on the child's
/// exit status AND on both markers, because a filter that matched nothing
/// exits 0.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env() {
    if std::env::var_os(FABRICATED).is_some() {
        fabricated_child_half().await;
        return;
    }

    let appdir = fabricated_mount();
    // The shims live OUTSIDE the mount: a shim inside `$APPDIR` would be
    // scrubbed away with the rest of it and the fast paths would not find
    // it (which is correct behaviour, but proves nothing).
    let shim = temp_cwd("hostenv-shim");
    let mut installed: Vec<String> = Vec::new();
    for binary in ["fd", "rg"] {
        if let Some(real) = real_binary_on_path(binary) {
            write_shim(&shim, binary, &real);
            installed.push(binary.to_string());
        }
    }

    let mut cmd = tokio::process::Command::new(appdir.join("usr/bin/archimedes"));
    cmd.args([
        "agent::tools::exec::tests::bash_hostenv::a_bash_child_on_an_appimage_host_sees_no_mount_paths",
        "agent::tools::exec::tests::bash_hostenv::a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env",
        "agent::tools::exec::tests::bash_hostenv::the_skip_is_a_failure_on_a_capable_host",
        "--test-threads=1",
        "--nocapture",
    ]);
    // The taint `AppRun.c` + `linuxdeploy-plugin-gtk.sh` leave behind, in
    // the byte shapes they actually write (mount dir first, trailing slash,
    // the double slash of the GTK hook).
    cmd.env("APPDIR", &appdir);
    cmd.env("APPIMAGE", appdir.join("archimedes.AppImage"));
    cmd.env(
        "LD_LIBRARY_PATH",
        format!("{}/usr/lib/:/usr/lib", appdir.display()),
    );
    cmd.env(
        "GI_TYPELIB_PATH",
        format!("{}//usr/lib/girepository-1.0", appdir.display()),
    );
    cmd.env("PYTHONHOME", appdir.join("usr"));
    // Mount dir first (with its trailing slash), then the shim dir and
    // `/bin` — the two entries the child must still see.
    cmd.env(
        "PATH",
        format!("{}/usr/bin/:{}:/bin", appdir.display(), shim.display()),
    );
    cmd.env(FABRICATED, "1");
    // `shim.path()` and NOT `&shim` (#96 task 08): `Command::env` wants an
    // `AsRef<OsStr>`, which `ScratchDir` deliberately does not implement (only
    // `AsRef<Path>` + `Deref<Target = Path>`); `&Path` DOES implement
    // `AsRef<OsStr>`, so the accessor is the escape hatch. Same D15 shape as
    // `agents.rs`'s `set_var("HOME", tmp.path())`, one seam over — a CHILD's
    // env rather than the process's.
    cmd.env(SHIM_DIR, shim.path());
    cmd.env(FAST_PATHS, installed.join(","));
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    // So the deadline below cannot leave an orphan: `wait_with_output`
    // CONSUMES the child, so the only way out of a timeout is dropping the
    // future, and dropping the child has to kill it.
    cmd.kill_on_drop(true);

    let child = cmd.spawn().expect("the fabricated-mount child starts");
    let out = match tokio::time::timeout(Duration::from_secs(180), child.wait_with_output()).await {
        Ok(out) => out.expect("the fabricated-mount child is reaped"),
        Err(_) => panic!("the fabricated-mount child did not finish within 180 s"),
    };
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    // KEPT on purpose (#96 task 08): `appdir` comes from
    // `fabricated_mount()`, which builds `.mount_archimTEST-*` under
    // `target/<profile>/` — a DIFFERENT base than the temp dir, so it is
    // outside #96's census and no `ScratchDir` owns it. Deleting this
    // cleanup would be a regression.
    let _ = std::fs::remove_dir_all(&appdir);
    assert!(
        out.status.success(),
        "the run inside the fabricated mount failed (its own assertions are the failure):\n{log}"
    );
    assert!(
        log.contains(CHILD_RAN),
        "the child never ran the fabricated assertions — a filter that matches nothing \
         exits 0, so the marker is the only proof it ran:\n{log}"
    );
    assert!(
        log.contains(COVERAGE_FULL),
        "the reporter did not see a capable host inside the child — the fabricated gate \
         did not open, so nothing here was observed:\n{log}"
    );
    // ── the scrub's VERDICT line (issue #81, residual 2) ─────────────────
    //
    // This is the only place in the tree where a NON-EMPTY patch exists inside
    // a real process, so it is the only honest proof that the line fires where
    // it matters (the unit tests describe a fabricated patch; they cannot show
    // that anything prints it).
    //
    // Exactly ONE line, though the child spawns three tool children (`sh`,
    // `fd`, `rg`): the verdict comes from `for_child()`'s `OnceLock`
    // initializer, which is a per-PROCESS fact. If this ever reads 2 the
    // initializer is not the right place — find out why, do not relax the
    // count. The PARENT's own verdict cannot inflate it: it goes to the
    // parent's stderr, which is not in `log`.
    assert_eq!(
        log.matches("hostenv: scrubbed").count(),
        1,
        "the scrub verdict must be printed exactly once per process (the fabricated \
         child spawns a bash, an fd and an rg child, so a per-CHILD line would read \
         higher and a missing initializer would read 0):\n{log}"
    );
    // And inside a mount the verdict must be the SCRUBBED one — `PYTHONHOME` is
    // set to `$APPDIR/usr` above and the mount owns all of it, so the patch
    // `env_remove`s it and the line must say so. This is the discrimination
    // issue #81 asks for: a run that silently reported `changed nothing` from
    // inside a mount would pass every assertion above.
    assert!(
        log.contains("PYTHONHOME=removed"),
        "inside a fabricated mount the verdict must name the removed variable, not \
         claim nothing was done:\n{log}"
    );
}

/// The AppDir-shaped tree: `$APPDIR/usr/lib`, `$APPDIR/usr/bin` and a hard
/// link of this test binary at `$APPDIR/usr/bin/archimedes` (the path a
/// mounted AppImage's `current_exe()` actually reports).
#[cfg(target_os = "linux")]
fn fabricated_mount() -> PathBuf {
    let exe = std::env::current_exe().expect("a test binary has a path");
    let under_target = exe
        .parent() // target/<profile>/deps
        .and_then(|deps| deps.parent()) // target/<profile>
        .expect("the test binary lives in target/<profile>/deps");
    let appdir = under_target.join(format!(".mount_archimTEST-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(appdir.join("usr/lib")).unwrap();
    std::fs::create_dir_all(appdir.join("usr/bin")).unwrap();
    std::fs::hard_link(&exe, appdir.join("usr/bin/archimedes")).expect(
        "the fabricated AppDir must hard-link the test binary (same filesystem: \
         target/<profile> holds target/<profile>/deps)",
    );
    appdir
}

/// The real `fd` / `rg` on THIS process' `PATH` — the parent's `PATH` is
/// the clean one, and the shim must exec an absolute path because the
/// child's `PATH` no longer contains the real binary's dir by name.
#[cfg(target_os = "linux")]
fn real_binary_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// An `sh` shim that records the env it was started with and then execs
/// the real binary. `exec_find` / `exec_grep` spawn `fd` / `rg` BY NAME
/// and read their stdout, so the record file is the only way to see what
/// those two children were handed.
#[cfg(target_os = "linux")]
fn write_shim(dir: &Path, binary: &str, real: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let record = dir.join(format!("{binary}-env.txt"));
    let script = format!(
        "#!/bin/sh\nprintf '%s{SEP}%s' \"$LD_LIBRARY_PATH\" \"$PATH\" > '{}' 2>/dev/null || true\nexec '{}' \"$@\"\n",
        record.display(),
        real.display()
    );
    let path = dir.join(binary);
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// The assertions that run INSIDE the fabricated mount. [`CHILD_RAN`] is
/// printed only after every one of them passes.
#[cfg(target_os = "linux")]
async fn fabricated_child_half() {
    assert!(
        running_from_an_appimage_mount(),
        "the fabricated mount did not open the production gate — the child would assert nothing"
    );
    let (_ld, path) = assert_bash_child_env_is_scrubbed().await;

    // The user's own entries survive the TOOL hop too — the same contrast
    // as `the_bash_tool_keeps_the_users_path_entries`, but asserted at the
    // call site that is the one capable of getting it wrong (a site that
    // `env_clear()`s, or a rule that removes `PATH`, leaves the child with
    // std's `confstr(_CS_PATH)` fallback and still exits 0).
    let shim = std::env::var_os(SHIM_DIR).expect("the parent passes the shim dir");
    assert_eq!(
        path,
        format!("{}:/bin", shim.to_string_lossy()),
        "the bash child's PATH must be exactly the inherited PATH minus the \
         mount entry, in order (the shim dir is the user's entry, /bin the \
         system's)"
    );

    let installed: Vec<String> = std::env::var(FAST_PATHS)
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    assert_fast_path_children_are_scrubbed(Path::new(&shim), &installed).await;
    println!("{CHILD_RAN}: PATH={path}");
}

/// The shared assertion of tests 2 and 5: run `bash` through the tool and
/// check the child's own `LD_LIBRARY_PATH` and `PATH` against
/// [`expected_child_value`] — the inherited env minus the mount entries.
///
/// Comparing the FULL value (not just "no `.mount_` substring") is what
/// makes a dropped or reordered entry visible, and it is also what makes
/// the correct absence of `LD_LIBRARY_PATH` pass: the real `AppRun` sets it
/// to `$APPDIR/usr/lib/:` and nothing else, so scrubbing it leaves NOTHING,
/// and a "must still be non-empty" assertion here would be a false failure.
#[cfg(target_os = "linux")]
async fn assert_bash_child_env_is_scrubbed() -> (String, String) {
    let cwd = temp_cwd("hostenv-bash");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": format!("printf '%s{SEP}%s' \"$LD_LIBRARY_PATH\" \"$PATH\"") }),
    )
    .await;
    assert!(!r.is_error, "the bash tool failed: {}", text_of(&r));
    let text = text_of(&r).to_string();
    let (ld, path) = text.split_once(SEP).unwrap_or_else(|| {
        panic!("the child printed {text:?}, not the `{SEP}`-separated pair it was asked for")
    });
    for (name, value) in [("LD_LIBRARY_PATH", ld), ("PATH", path)] {
        assert!(
            !lists_a_mount_path(OsStr::new(value)),
            "the bash child inherited a mount path in {name}: {value}"
        );
    }
    let expected = (
        expected_child_value("LD_LIBRARY_PATH"),
        expected_child_value("PATH"),
    );
    assert_eq!(
        (ld, path),
        (expected.0.as_str(), expected.1.as_str()),
        "the bash child's env must be exactly the inherited env minus the mount entries"
    );
    (expected.0, expected.1)
}

/// The `fd` / `rg` fast paths, run through the tools, with the env their
/// shims recorded read back.
#[cfg(target_os = "linux")]
async fn assert_fast_path_children_are_scrubbed(shim: &Path, installed: &[String]) {
    for (tool, binary) in [("find", "fd"), ("grep", "rg")] {
        if !installed.iter().any(|b| b == binary) {
            // A NOTE, not a skip: with the binary absent the fast path
            // never runs (`exec_find` falls back to `walkdir`), so there
            // is no fast-path child env to have scrubbed and nothing here
            // went unobserved. Not a `SKIP-HOSTENV` marker for exactly
            // that reason.
            println!(
                "NOTE-HOSTENV: {binary} is not installed — the `{tool}` fast path does not run here"
            );
            continue;
        }
        let cwd = temp_cwd(&format!("hostenv-{tool}"));
        std::fs::write(cwd.join("needle.txt"), "needle\n").unwrap();
        let params = if tool == "find" {
            json!({ "pattern": "needle.txt" })
        } else {
            json!({ "pattern": "needle" })
        };
        let r = execute_tool(&ctx(&cwd), tool, &params).await;
        let text = text_of(&r).to_string();
        assert!(
            !r.is_error,
            "the `{tool}` fast path must still work on a scrubbed env: {text}"
        );
        assert!(
            text.contains("needle"),
            "the `{tool}` fast path found nothing on a scrubbed env: {text}"
        );
        let record = shim.join(format!("{binary}-env.txt"));
        let seen = std::fs::read_to_string(&record).unwrap_or_else(|e| {
            panic!(
                "the {binary} shim never recorded its env ({e}): the `{tool}` fast path \
                 did not run the shim, so there was no child env to check"
            )
        });
        let (ld, path) = seen.split_once(SEP).unwrap_or_else(|| {
            panic!("unexpected {binary} shim record {seen:?} (no `{SEP}` separator)")
        });
        for (name, value) in [("LD_LIBRARY_PATH", ld), ("PATH", path)] {
            assert!(
                !lists_a_mount_path(OsStr::new(value)),
                "the `{tool}` child inherited a mount path in {name}: {value}"
            );
        }
    }
}

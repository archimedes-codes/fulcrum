use serde_json::json;

use super::support::*;
use crate::agent::hostenv::EnvPatch;
use crate::agent::tools::exec::search::{build_fd_command_with, build_rg_command_with};
use crate::agent::tools::exec::{execute_tool, glob_match};

/// `fd` is available (spawn `fd --version`). Only the gated
/// POSIX-shape fixture below reads it — hence the same `cfg`, or it is
/// dead code on a Windows build.
#[cfg(unix)]
fn fd_available() -> bool {
    std::process::Command::new("fd")
        .arg("--version")
        .output()
        .is_ok()
}

/// The fixture asserts POSIX PATH SHAPE on both halves: the expected
/// result is the literal `"sub/c.txt"`, and the `**/c.txt` pattern's `/`
/// is what `glob_match` compares against the walkdir fallback's
/// platform-relative path — on Windows that is `sub\c.txt`, so `**/…`
/// matches nothing and the run reported `matches: 0`. The separator is
/// `find`'s product behavior, not a fixture bug: gating keeps it honest
/// rather than asserting a shape the tool does not produce on Windows.
#[cfg(unix)]
#[tokio::test]
async fn find_matches_fixture() {
    let cwd = temp_cwd("find");
    std::fs::write(cwd.join("a.txt"), "a").unwrap();
    std::fs::write(cwd.join("b.txt"), "b").unwrap();
    std::fs::create_dir_all(cwd.join("sub")).unwrap();
    std::fs::write(cwd.join("sub/c.txt"), "c").unwrap();
    // Works via `fd` or the `walkdir` fallback.
    let r = execute_tool(&ctx(&cwd), "find", &json!({ "pattern": "a.txt" })).await;
    assert!(!r.is_error, "fd={}", fd_available());
    assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
    assert_eq!(text_of(&r), "a.txt");
    let r = execute_tool(&ctx(&cwd), "find", &json!({ "pattern": "**/c.txt" })).await;
    assert!(!r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
    assert_eq!(text_of(&r), "sub/c.txt");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn find_max_results_truncates() {
    let cwd = temp_cwd("find-max");
    for name in ["a.txt", "b.txt", "c.txt"] {
        std::fs::write(cwd.join(name), name).unwrap();
    }
    let r = execute_tool(
        &ctx(&cwd),
        "find",
        &json!({ "pattern": "*", "max_results": 1 }),
    )
    .await;
    assert!(!r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
    assert_eq!(r.details.as_ref().unwrap()["truncated"], true);
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn find_escape_rejected() {
    let cwd = temp_cwd("find-esc");
    let r = execute_tool(
        &ctx(&cwd),
        "find",
        &json!({ "pattern": "*", "path": "../" }),
    )
    .await;
    assert!(r.is_error, "a sandbox escape must be rejected");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[test]
fn glob_match_basics() {
    assert!(glob_match("a*.txt", "a1.txt"));
    assert!(!glob_match("a*", "a/b.txt"));
    assert!(glob_match("**/c.txt", "sub/c.txt"));
    assert!(!glob_match("**/c.txt", "a.txt"));
    assert!(glob_match("a?c", "abc"));
    assert!(!glob_match("a?c", "ab"));
}

// ── grep ─────────────────────────────────────────────────────────────

/// `ripgrep` is available (spawn `rg --version`).
fn rg_available() -> bool {
    std::process::Command::new("rg")
        .arg("--version")
        .output()
        .is_ok()
}

#[tokio::test]
async fn grep_matches_fixture() {
    // Guard on `rg` availability (NOT `#[ignore]`).
    if !rg_available() {
        return;
    }
    let cwd = temp_cwd("grep");
    std::fs::write(cwd.join("a.txt"), "hello needle here\n").unwrap();
    let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "needle" })).await;
    assert!(!r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
    assert!(text_of(&r).contains("a.txt"));
    assert!(text_of(&r).contains("needle"));
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn grep_option_like_pattern_is_matched_literally() {
    // Guard on `rg` availability (NOT `#[ignore]`). Regression test for
    // the `rg` argument-injection hole: WITHOUT a `--` separator, a
    // pattern like `--pre=sh` is parsed by `rg` as an OPTION (`--pre`
    // runs a program on every input file before searching — arbitrary
    // process execution, un-gated), and the positional shift turns the
    // validated search dir into the pattern and the search path into
    // `.` (the desktop CWD). With `--` the pattern is matched literally.
    if !rg_available() {
        return;
    }
    let cwd = temp_cwd("grep-inj");
    // The option-like pattern as LITERAL text in a file.
    std::fs::write(cwd.join("a.txt"), "--pre=sh needle\n").unwrap();
    let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "--pre=sh" })).await;
    assert!(
        !r.is_error,
        "an option-like pattern must be searched literally, not parsed as an option: {r:?}"
    );
    assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
    assert!(text_of(&r).contains("a.txt"), "the literal text matched");
    let _ = std::fs::remove_dir_all(&cwd);
}

// ── the spawn shape (the `#81` cwd fix, asserted on an unspawned Command) ──

/// Claim: both search children start in the SPACE (`ctx.cwd`), the same
/// promise `exec_bash` has made since it was written.
///
/// Asserted on a `Command` that was never spawned, via
/// `std::process::Command::get_current_dir` — the only honest introspection
/// there is, and the same trick `hostenv::fixtures::configured_env` plays
/// with `as_std().get_envs()` for the env half. Spawning and asking the child
/// `pwd` would work for `bash` but not here: `fd` is not installed on every
/// runner, and the assertion is about what the child is TOLD, not about what
/// a binary chooses to print.
///
/// `EnvPatch::default()` is the right patch for this test: it pins the CWD,
/// not the env — the env is pinned at the call sites by `bash_hostenv.rs`.
#[test]
fn the_search_children_start_in_the_space_cwd() {
    let space = temp_cwd("search-cwd");
    let fd = build_fd_command_with("a.txt", &space, 100, &space, &EnvPatch::default());
    assert_eq!(
        fd.as_std().get_current_dir(),
        Some(space.as_path()),
        "the `fd` child must start in the Space"
    );
    let rg = build_rg_command_with(
        "needle",
        None,
        &space,
        100,
        false,
        &space,
        &EnvPatch::default(),
    );
    assert_eq!(
        rg.as_std().get_current_dir(),
        Some(space.as_path()),
        "the `rg` child must start in the Space (its `-g` globs resolve against it)"
    );
    let _ = std::fs::remove_dir_all(&space);
}

/// Claim: the argument lists are BYTE-IDENTICAL to what they were before the
/// cwd fix, and no `fd`/`rg` flag was added to buy the cwd instead of it.
///
/// This test exists to redden anyone who later reaches for
/// `fd -a`/`--absolute-path` (its output is already absolute, because the
/// search root `validate` hands it is absolute), `fd -C`/`--base-directory`
/// (fd-only, and it would change what the printed paths are relative to), or
/// `--path-separator` — each of which silently un-makes the prefix strip in
/// `exec_find`, which is what keeps `find` output relative to the search dir.
/// So: EXACT vectors, in order, nothing extra.
#[test]
fn the_search_argument_lists_are_unchanged() {
    let space = temp_cwd("search-args");
    let root = space.display().to_string();
    let args_of = |cmd: &tokio::process::Command| -> Vec<String> {
        cmd.as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    };

    let fd = build_fd_command_with("a.txt", &space, 100, &space, &EnvPatch::default());
    assert_eq!(
        args_of(&fd),
        vec!["--max-results", "100", "-g", "a.txt", "--", &root],
        "the `fd` argument list is a byte-for-byte contract"
    );

    let rg = build_rg_command_with(
        "needle",
        None,
        &space,
        100,
        false,
        &space,
        &EnvPatch::default(),
    );
    assert_eq!(
        args_of(&rg),
        vec!["-m", "100", "--", "needle", &root],
        "the `rg` argument list is a byte-for-byte contract"
    );

    // …and the flag-mapping order with both optional pieces present: `-i`,
    // then `--glob`, then `-m`, then `--`, then pattern, then root.
    let rg = build_rg_command_with(
        "needle",
        Some("*.rs"),
        &space,
        5,
        true,
        &space,
        &EnvPatch::default(),
    );
    assert_eq!(
        args_of(&rg),
        vec!["-i", "--glob", "*.rs", "-m", "5", "--", "needle", &root],
        "the `rg` flag order is a byte-for-byte contract"
    );
    let _ = std::fs::remove_dir_all(&space);
}

/// The bug the missing `current_dir` actually costs (`#81`), and the RED
/// half of that fix.
///
/// `rg` matches a `-g` glob that contains a `/` against the candidate path
/// **relative to rg's own cwd**; a basename-only glob (no `/`) is
/// cwd-independent. `exec_grep` handed `rg` an absolute search root but no
/// `current_dir`, so the child ran wherever its parent sat — under
/// `AppRun.c`'s `chdir("$APPDIR/usr")` that is INSIDE the AppImage mount —
/// and every `grep` whose `glob` named a directory returned ZERO matches
/// while reporting success (`is_error: false`, `matches: 0`). A false
/// negative the model reads as "the code does not reference that". Measured
/// on this box (`ripgrep 15.2.0`, Space `/tmp/k81/space` holding
/// `space/sub/x.txt`):
///
/// ```text
/// cd /tmp/k81/space && rg -g 'sub/*'    -- needle /tmp/k81/space → 1 match, exit 0
/// cd /tmp/k81/else  && rg -g 'sub/*'    -- needle /tmp/k81/space → 0 matches, exit 1
/// cd /tmp/k81/else  && rg -g '**/sub/*' -- needle /tmp/k81/space → 1 match, exit 0
/// cd /tmp/k81/else  && rg -g '*.txt'    -- needle /tmp/k81/space → 1 match, exit 0
/// ```
///
/// The CONTROL call (same fixture, same pattern, NO `glob`) is what makes
/// this a test of the glob rather than of the fixture: it must report
/// `matches: 1` both before and after the fix, so the only variable left is
/// the `-g` argument.
#[tokio::test]
async fn a_slash_glob_matches_from_the_space_cwd() {
    // Guard on `rg` availability (NOT `#[ignore]`), same style as
    // `grep_matches_fixture`.
    if !rg_available() {
        return;
    }
    let cwd = temp_cwd("grep-glob");
    std::fs::create_dir_all(cwd.join("sub")).unwrap();
    std::fs::write(cwd.join("sub/needle.txt"), "needle\n").unwrap();

    // Control: no `glob` at all — the file and the pattern are right.
    let control = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "needle" })).await;
    assert!(!control.is_error, "the control call failed: {control:?}");
    assert_eq!(
        control.details.as_ref().unwrap()["matches"],
        1,
        "control: the fixture must match without a glob, or the glob case proves nothing"
    );

    // The case the bug is about: a `glob` that names a directory.
    let r = execute_tool(
        &ctx(&cwd),
        "grep",
        &json!({ "pattern": "needle", "glob": "sub/*.txt" }),
    )
    .await;
    assert!(
        !r.is_error,
        "a directory-naming glob must not be an error: {r:?}"
    );
    assert_eq!(
        r.details.as_ref().unwrap()["matches"],
        1,
        "a `glob` containing a `/` must match inside the Space; the child's cwd \
         is what makes `rg` resolve it (before the fix this is `matches: 0` with \
         `is_error: false` — the silent false negative of #81)"
    );
    // `rg`'s OUTPUT SHAPE did not move: it still carries the absolute search
    // root it was handed. CANONICAL on both sides — `validate` hands `rg` a
    // canonical root, and on macOS the temp dir is `/var/…` behind a
    // `/private/var` symlink, so comparing against the raw `cwd` would be a
    // false failure there (the trap `access.rs`'s "the comparison is
    // CANONICAL on both sides" records).
    let canonical = std::fs::canonicalize(&cwd).unwrap().display().to_string();
    assert!(
        text_of(&r).contains(&canonical),
        "the grep output must still carry the absolute search root, got {:?}",
        text_of(&r)
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

/// `find`'s output stays relative to the SEARCH DIR after the child starts
/// in the Space (the prefix strip in `exec_find` still fires), even when the
/// two differ — the search dir here is `<space>/sub`, the cwd is `<space>`.
///
/// Cross-platform and `fd`-free on purpose: the `fd` arm and the `walkdir`
/// fallback must answer identically, and `fd` is not installed on every
/// runner (this box has no `fd` on `PATH`, so `find` takes the fallback
/// here). The claim is about the strip, which both arms produce.
#[tokio::test]
async fn an_absolute_path_argument_still_reports_paths_relative_to_it() {
    let cwd = temp_cwd("find-abs-dir");
    std::fs::create_dir_all(cwd.join("sub")).unwrap();
    std::fs::write(cwd.join("sub/c.txt"), "c").unwrap();
    let search_dir = cwd.join("sub");
    let r = execute_tool(
        &ctx(&cwd),
        "find",
        &json!({
            "pattern": "c.txt",
            // Absolute, and inside the boundary — `validate` accepts it.
            "path": search_dir.display().to_string(),
        }),
    )
    .await;
    assert!(
        !r.is_error,
        "an absolute in-boundary `path` is valid: {r:?}"
    );
    assert_eq!(
        text_of(&r),
        "c.txt",
        "a result is relative to the SEARCH DIR, not to the Space — and the \
         strip must still fire when the child's cwd is the Space"
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn grep_missing_rg_is_error() {
    // Only the missing-`rg` path (skip when `rg` is installed).
    if rg_available() {
        return;
    }
    let cwd = temp_cwd("grep-missing");
    let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "x" })).await;
    assert!(r.is_error, "a missing rg is a tool error");
    let _ = std::fs::remove_dir_all(&cwd);
}

/// (finding) `exec_grep` has a `DEFAULT_MAX_RESULTS`-style cap (like
/// `find`): a `grep` with no `max_results` is NOT unbounded, and the
/// OUTPUT is bounded to `max_results` lines (`rg -m` is PER-FILE, so
/// the total can exceed it — the executor truncates the output).
#[tokio::test]
async fn grep_output_bounded_by_max_results() {
    // Guard on `rg` availability (NOT `#[ignore]`).
    if !rg_available() {
        return;
    }
    let cwd = temp_cwd("grep-cap");
    for name in ["a.txt", "b.txt", "c.txt"] {
        std::fs::write(cwd.join(name), "needle\n").unwrap();
    }
    // No `max_results` (the default cap) — 3 matches, not truncated.
    let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "needle" })).await;
    assert!(!r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["matches"], 3);
    assert_eq!(r.details.as_ref().unwrap()["truncated"], false);
    // `max_results: 2` — `-m` is PER-FILE (all 3 matches still come
    // back from `rg`), but the OUTPUT is bounded to 2 lines.
    let r = execute_tool(
        &ctx(&cwd),
        "grep",
        &json!({ "pattern": "needle", "max_results": 2 }),
    )
    .await;
    assert!(!r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["matches"], 3);
    assert_eq!(r.details.as_ref().unwrap()["truncated"], true);
    assert_eq!(
        text_of(&r).lines().count(),
        2,
        "the output is bounded to max_results lines"
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

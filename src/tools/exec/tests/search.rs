use serde_json::json;

use super::support::*;
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

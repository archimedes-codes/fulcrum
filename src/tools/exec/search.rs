use std::path::Path;

use serde_json::{json, Value};

use super::access::read_backend;
use crate::agent::tools::types::{ToolCtx, ToolResult};

/// The default `max_results` for `exec_find` / `exec_grep`.
pub(crate) const DEFAULT_MAX_RESULTS: u32 = 100;

/// Find files by glob pattern: shells out to `fd` (`fd -g <pattern>
/// <path> --max-results <n>`) when available, else falls back to a `walkdir`
/// walk with a built-in glob matcher. The search path is validated against
/// `ctx.cwd` via `FsBackend`.
///
/// Deliberately NOT shared with `commands::files::collect_files` (the `?`
/// file picker, ADR 0033/0034), even though both walk a tree. The reason they
/// stay apart is unchanged — sharing would mean either a subprocess spawn per
/// keystroke or a slower agent `find` — so they remain cross-referenced by
/// comment instead (the discipline recorded for the mirrored `parse_value`).
/// Do not "unify" them.
///
/// HOW THEY DIFFER TODAY (as of the picker learning the repo's ignore rules):
///
/// * **Ignore rules.** The picker always applies them (`.gitignore` +
///   `.ignore` + `.git/info/exclude`, repo-scoped, never machine-wide).
///   `exec_find` applies them only as a side effect of `fd`, which honours
///   `.gitignore` by default — the `walkdir` fallback applies NONE. So this
///   tool's result set depends on whether `fd` happens to be installed, while
///   the picker's never does. That fd/no-fd divergence is a KNOWN defect,
///   recorded separately and deliberately not fixed here.
/// * **Dot-naming.** The fallback's dot-skip is per-FILE, so dot-named files
///   (`.gitignore`, `.env`) vanish from its results — the opposite of what the
///   completion list wants — and it descends INTO `.git`, listing paths under
///   it. The picker is the mirror image: it keeps every dot-named FILE and
///   prunes DIRECTORIES, but only the VCS metadata ones (`.git`, `.hg`, `.svn`)
///   and only at any depth below the root (a Space opened AT a directory named
///   `.git` is listed, never pruned) — every other dot-directory is walked and
///   listed unless the repo's own ignore rules exclude it. So
///   `.github/workflows/ci.yml` IS
///   completable in the picker, while this tool's `fd` arm never sees it (`fd`
///   hides hidden names by default, and no `--hidden` is passed) even though
///   the fallback walks right into it — another face of the fd/no-fd defect
///   above. `.git/…` paths run the other way: never in the picker, offered by
///   the fallback.
/// * **Separators.** The fallback builds its relative path with
///   `to_string_lossy()`, which emits `\` on Windows; the picker joins
///   `components()` with `/`, because `\` is outside the composer's token
///   charset.
///
/// `params`: `{ pattern: String, path?: String, max_results?: u32 }`.
pub async fn exec_find(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let pattern = match params.get("pattern").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return ToolResult::fail("find: missing `pattern` parameter".to_string(), None),
    };
    let dir = params
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let max = params
        .get("max_results")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_MAX_RESULTS);
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("find: {e}"), None),
    };

    // Prefer `fd` when it is installed (the spawn fails with NotFound
    // otherwise — that is the availability check, no `which` needed).
    // `--` before the path (the `rg --` hardening — a pattern that looks
    // like a flag is not re-parsed as one).
    //
    // The bound `cmd` (not a fluent chain) exists to carry the SAME env scrub
    // `bash` gets: `fd` and `rg` are system binaries whose dependencies
    // (`libpcre2-8.so.0`, `libcrypto`) the bundle also ships, so on a mounted
    // host they bind the bundle's copies unversioned — `git`'s warning line is
    // the same mechanism, and the poisoned `PATH` is what makes the bare-name
    // lookup work at all (research §7's site table). KNOWN REMAINING GAP,
    // deliberately out of scope here: unlike `exec_bash`, `exec_find` and
    // `exec_grep` pass NO `current_dir`, so this child runs with the INHERITED
    // cwd — which under `AppRun.c`'s `chdir("$APPDIR/usr")` is INSIDE the
    // mount. The env half is fixed; the cwd half is not, and a relative path in
    // a `find`/`grep` result can still surprise because of it (it is not a leak
    // of libraries — do not read the scrub as having fixed that).
    //
    // The constructor is `crate::agent::childproc`'s, which owns the child's
    // Windows console window policy (`CREATE_NO_WINDOW`).
    let mut cmd = crate::agent::childproc::command("fd");
    // A Tool child starts at EOF on stdin — see the note in `exec_bash`: an
    // inherited handle is a live pipe nothing can ever feed, so a `fd` that
    // read stdin would hang to the caller's deadline instead of failing.
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.args([
        "--max-results",
        &max.to_string(),
        "-g",
        &pattern,
        "--",
        &resolved.to_string_lossy(),
    ]);
    crate::agent::hostenv::for_child().apply(&mut cmd);
    let fd = cmd.output().await;
    if let Ok(out) = fd {
        // `fd` prints paths relative to its CWD — since the search dir is
        // passed as an absolute (canonicalized) path, strip the prefix so
        // the results are relative to the search dir (like the `walkdir`
        // fallback).
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let prefix = format!("{}/", resolved.to_string_lossy());
        let stdout: String = stdout
            .lines()
            .map(|l| l.strip_prefix(&prefix).unwrap_or(l))
            .collect::<Vec<_>>()
            .join("\n");
        let matches = stdout.lines().filter(|l| !l.is_empty()).count();
        return ToolResult::ok_text(
            stdout,
            Some(json!({
                "matches": matches,
                "truncated": matches as u32 >= max,
            })),
        );
    }

    // Fallback: walk the tree (skip hidden entries, like `fd`'s default)
    // and glob-match the path relative to the search dir. The walk is
    // BLOCKING (a big tree would stall a tokio worker): it runs on the
    // blocking thread pool.
    let (matches, truncated) = match tokio::task::spawn_blocking(move || {
        let mut matches: Vec<String> = Vec::new();
        let mut truncated = false;
        let walker = walkdir::WalkDir::new(&resolved)
            .into_iter()
            .filter_map(|e| e.ok());
        for entry in walker {
            if entry.file_type().is_dir() {
                continue;
            }
            if entry
                .path()
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('.'))
            {
                continue;
            }
            let rel = entry.path().strip_prefix(&resolved).unwrap_or(entry.path());
            let rel_str = rel.to_string_lossy().to_string();
            if glob_match(&pattern, &rel_str) {
                matches.push(rel_str);
                if matches.len() as u32 >= max {
                    truncated = true;
                    break;
                }
            }
        }
        (matches, truncated)
    })
    .await
    {
        Ok(r) => r,
        Err(_) => {
            return ToolResult::fail(
                "find: the directory walk failed (blocking task join error)".to_string(),
                None,
            )
        }
    };
    ToolResult::ok_text(
        matches.join("\n"),
        Some(json!({ "matches": matches.len(), "truncated": truncated })),
    )
}

/// Match `candidate` against a glob `pattern` (`*` = any run of non-`/`
/// chars, `**` = any run of chars including `/`, `?` = one non-`/` char).
pub(crate) fn glob_match(pattern: &str, candidate: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let c: Vec<char> = candidate.chars().collect();
    glob_match_idx(&p, 0, &c, 0)
}

fn glob_match_idx(p: &[char], i: usize, c: &[char], j: usize) -> bool {
    if i == p.len() {
        return j == c.len();
    }
    match p[i] {
        '*' if p.get(i + 1) == Some(&'*') => {
            // `**` matches any run of chars (including `/`), zero or more.
            let mut k = j;
            loop {
                if glob_match_idx(p, i + 2, c, k) {
                    return true;
                }
                if k == c.len() {
                    return false;
                }
                k += 1;
            }
        }
        '*' => {
            // `*` matches any run of non-`/` chars, zero or more.
            let mut k = j;
            loop {
                if glob_match_idx(p, i + 1, c, k) {
                    return true;
                }
                if k == c.len() || c[k] == '/' {
                    return false;
                }
                k += 1;
            }
        }
        '?' => {
            if j < c.len() && c[j] != '/' {
                glob_match_idx(p, i + 1, c, j + 1)
            } else {
                false
            }
        }
        ch => {
            if j < c.len() && c[j] == ch {
                glob_match_idx(p, i + 1, c, j + 1)
            } else {
                false
            }
        }
    }
}

/// Grep a file tree: shells out to `ripgrep` (`rg <flags> <pattern>
/// <path>`) with flags mapped from the params. The search path is validated
/// against `ctx.cwd` via `FsBackend`. There is no fallback — a missing
/// `rg` is a tool error.
///
/// `params`: `{ pattern: String, path?: String, glob?: String, max_results?: u32, "-i"?: bool }`.
pub async fn exec_grep(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let pattern = match params.get("pattern").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return ToolResult::fail("grep: missing `pattern` parameter".to_string(), None),
    };
    let dir = params
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let glob = params.get("glob").and_then(|v| v.as_str());
    // `max_results` defaults to the cap (like `find` — a grep with no
    // `max_results` is NOT unbounded on a big tree).
    let max = params
        .get("max_results")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_MAX_RESULTS);
    let ignore_case = params.get("-i").and_then(|v| v.as_bool()).unwrap_or(false);
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("grep: {e}"), None),
    };

    let mut args: Vec<String> = Vec::new();
    if ignore_case {
        args.push("-i".to_string());
    }
    if let Some(g) = glob {
        args.push("--glob".to_string());
        args.push(g.to_string());
    }
    // `-m` bounds matches PER FILE (the total can exceed `max` when more
    // files match) — the OUTPUT is additionally truncated below.
    args.push("-m".to_string());
    args.push(max.to_string());
    // `--` terminates option parsing: the pattern is matched LITERALLY
    // (an option-like pattern such as `--pre=sh` — which would otherwise
    // be parsed by `rg` as a `--pre` option and run a program on every
    // input file, un-gated — is treated as the pattern, and the search
    // path stays the validated `resolved` dir, not `.`).
    args.push("--".to_string());
    args.push(pattern.clone());
    args.push(resolved.to_string_lossy().into_owned());

    // Bound `cmd` for the same reason as the `fd` arm of `exec_find`: the env
    // scrub needs a name. Same KNOWN GAP too — no `current_dir` is passed, so
    // the child's cwd stays the inherited one, which on a mounted host is
    // inside the mount (`AppRun.c`'s `chdir("$APPDIR/usr")`); the env is
    // scrubbed, the cwd is not.
    //
    // Same `crate::agent::childproc` constructor as `fd` — it owns the child's
    // Windows console window policy (`CREATE_NO_WINDOW`).
    let mut cmd = crate::agent::childproc::command("rg");
    // Tool child → null stdin, for the same reason as `fd` and `bash` above.
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.args(&args);
    crate::agent::hostenv::for_child().apply(&mut cmd);
    match cmd.output().await {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout).to_string();
            let lines: Vec<&str> = stdout.lines().collect();
            let matches = lines.iter().filter(|l| !l.is_empty()).count();
            // `-m` is PER-FILE, so the total can exceed `max` (one match
            // per file): bound the OUTPUT to `max` lines.
            let truncated = matches as u32 > max;
            let shown: String = lines
                .iter()
                .take(max as usize)
                .copied()
                .collect::<Vec<_>>()
                .join("\n");
            ToolResult::ok_text(
                shown,
                Some(json!({ "matches": matches, "truncated": truncated })),
            )
        }
        Err(e) => ToolResult::fail(format!("grep: ripgrep (rg) is not available: {e}"), None),
    }
}

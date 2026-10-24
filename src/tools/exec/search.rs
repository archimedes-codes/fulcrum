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
    // Both halves of the child's starting conditions are now decided here, and
    // the shape of the call is the reason they are decided together (issue
    // #81):
    //
    // * the ENV — the AppImage scrub, `crate::agent::hostenv`, applied inside
    //   the builder below. `fd` and `rg` are system binaries whose
    //   dependencies (`libpcre2-8.so.0`, `libcrypto`) the bundle also ships,
    //   so on a mounted host they bind the bundle's copies unversioned —
    //   `git`'s warning line is the same mechanism, and the poisoned `PATH` is
    //   what makes the bare-name lookup work at all (research §7's site
    //   table).
    // * the CWD — `ctx.cwd`, the Space, exactly as `exec_bash` passes it.
    //   Without it the child ran wherever its parent sat, which under
    //   `AppRun.c`'s `chdir("$APPDIR/usr")` is INSIDE the mount.
    //
    // What the cwd actually buys is NOT what the comment here used to claim.
    // It never affected `fd`'s OUTPUT: `fd` echoes the search path as it was
    // given (`output.rs` prints `entry.stripped_path(config)`, and
    // `stripped_path` only calls `filesystem::strip_current_dir`, which is
    // `path.strip_prefix(".")`), and `validate` hands every search root to
    // this builder already ABSOLUTE and canonical, so the output is absolute
    // at every cwd and the prefix strip below fires identically wherever the
    // child starts. Measured on `fd` 10.4.2 at five cwds (`/`, `/tmp`, the
    // Space, a directory under it, and a sibling of it): byte-identical output
    // every time. The cwd matters for `rg`'s `-g` GLOBS, which resolve against
    // the child's cwd when they contain a `/` — see the measurement above the
    // `rg` spawn in [`exec_grep`], where the silent false negative lived.
    let mut cmd = build_fd_command(&pattern, &resolved, max, &ctx.cwd);
    let fd = cmd.output().await;
    if let Ok(out) = fd {
        // `fd` echoed the ABSOLUTE search root it was handed (see the note
        // above the spawn: it strips only a literal `.` prefix, so an absolute
        // search path prints absolute paths at every cwd), so strip that prefix
        // to land on paths relative to the SEARCH DIR — the shape the `walkdir`
        // fallback below produces. A relative path out of this tool is
        // therefore relative to the search dir, NOT to the Space, which is
        // still surprising when `path` names a directory below the Space.
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

/// Build the `Command` the `fd` fast path of [`exec_find`] spawns — everything
/// except the `output()` call and the post-processing of its stdout.
///
/// The seam exists for the same reason `agent/mcp/stdio.rs`'s `build_command`
/// and `agent/worker/client.rs`'s exist: the two assertions that matter here —
/// the child starts in the Space, and the argument list did not grow — can only
/// be made on a `Command` that was never spawned (`fd` is not installed on
/// every runner, and a spawned child tells you nothing about what it was
/// TOLD). Production calls it, so it needs no `cfg` and no `allow`.
pub(crate) fn build_fd_command(
    pattern: &str,
    resolved: &Path,
    max: u32,
    cwd: &Path,
) -> tokio::process::Command {
    build_fd_command_with(
        pattern,
        resolved,
        max,
        cwd,
        crate::agent::hostenv::for_child(),
    )
}

/// [`build_fd_command`] with the patch as an argument.
///
/// Production passes exactly [`for_child`](crate::agent::hostenv::for_child);
/// the parameter exists because that cached patch is EMPTY under `cargo test`
/// (its gate needs this exe under `$APPDIR`; under cargo it is
/// `target/debug/deps/<hash>`), so a test driven through the wrapper could not
/// observe the env at all and could not fail.
///
/// `cwd` is the Space (`ctx.cwd`), NOT `resolved`: `resolved` is the search
/// dir, which is `ctx.cwd` or a directory below it, and the two tools'
/// cwd-dependent behaviour is defined against the Space. It is deliberately not
/// canonicalized — `exec_bash` passes `ctx.cwd` raw and these two now agree with
/// it. A `cwd` that does not exist makes the spawn fail: `exec_find` degrades
/// into its `walkdir` fallback (which is cwd-independent) and `exec_grep`
/// reports its existing rg-unavailable error, which for that one cause is a
/// misleading message. Accepted, not overlooked: `exec_bash` has had exactly
/// this property since it was written, and `validate` already refuses a
/// non-existent `ctx.cwd` for the default `path: "."` before anything spawns.
///
/// The argument list is a byte-for-byte contract, pinned by
/// `the_search_argument_lists_are_unchanged`: no `-a`/`--absolute-path` (the
/// output is already absolute, because the search root is), no
/// `-C`/`--base-directory` (fd-only, and it redefines what the printed paths
/// are relative to), no `--path-separator` (it only rewrites separators). Each
/// of those would silently un-make the prefix strip in [`exec_find`].
pub(crate) fn build_fd_command_with(
    pattern: &str,
    resolved: &Path,
    max: u32,
    cwd: &Path,
    patch: &crate::agent::hostenv::EnvPatch,
) -> tokio::process::Command {
    // The constructor is `crate::agent::childproc`'s, which owns the child's
    // Windows console window policy (`CREATE_NO_WINDOW`). It is the ONLY
    // constructor for this tool — the wrapper above delegates rather than
    // building, because `agent/childproc.rs`'s census asserts exactly four
    // `childproc::command(` calls under `agent/tools/exec*`.
    let mut cmd = crate::agent::childproc::command("fd");
    // A Tool child starts at EOF on stdin — see the note in `exec_bash`: an
    // inherited handle is a live pipe nothing can ever feed, so a `fd` that
    // read stdin would hang to the caller's deadline instead of failing.
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.args([
        "--max-results",
        &max.to_string(),
        "-g",
        pattern,
        "--",
        &resolved.to_string_lossy(),
    ]);
    patch.apply(&mut cmd);
    cmd.current_dir(cwd);
    cmd
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

    // The env scrub AND the Space cwd, both decided in the builder (issue
    // #81) — see the note above the `fd` spawn in `exec_find` for the env half
    // and the measured reason the cwd half is not about output shape.
    //
    // The reason THIS tool needed the cwd is `rg`'s `--glob` matching, and it
    // failed SILENTLY: a `-g` glob containing a `/` is matched against the
    // candidate path relative to `rg`'s OWN cwd, so with the child sitting in
    // `$APPDIR/usr` (or anywhere else that is not the Space) every
    // directory-naming glob excluded everything and `rg` exited 1 with no
    // output — which `exec_grep` reports as a successful `matches: 0`. A
    // negated glob is inverted the same way (`-g '!sub/*'` excludes nothing
    // while the cwd is elsewhere, and excludes exactly the `sub/` paths once the
    // cwd IS the Space). A basename-only glob (no `/`) is cwd-independent, which
    // is why the existing tests never saw it.
    // Measured on `ripgrep 15.2.0` with a Space holding `space/sub/x.txt`:
    //
    // ```text
    // cd <space> && rg -g 'sub/*' -- needle <space> → 1 match, exit 0
    // cd <else>  && rg -g 'sub/*' -- needle <space> → 0 matches, exit 1
    // cd <else>  && rg -g '**/sub/*' -- needle <space> → 1 match, exit 0
    // cd <else>  && rg -g '*.txt' -- needle <space> → 1 match, exit 0
    // ```
    //
    // `rg`'s ignore-file discovery was checked too and is NOT cwd-dependent in
    // these versions (an ignore file in a PARENT of the search root is honoured
    // from any cwd — `--no-ignore-parent` is what moves it, not the cwd — and
    // one in a cwd ancestor that is not a search-root ancestor is honoured by
    // neither tool from anywhere), so adding `current_dir` changes no ignore
    // semantics.
    let mut cmd = build_rg_command(&pattern, glob, &resolved, max, ignore_case, &ctx.cwd);
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

/// Build the `Command` [`exec_grep`] spawns — everything except the `output()`
/// call and the line-counting/truncation of its stdout.
///
/// The same unspawned-`Command` seam as [`build_fd_command`] (and the reason is
/// sharper here: the bug this file's cwd fix kills is a FALSE NEGATIVE, so an
/// end-to-end test cannot distinguish "no matches" from "the glob was never
/// applied" without a control call — whereas `get_current_dir` and `get_args`
/// read the two facts straight off the `Command`).
pub(crate) fn build_rg_command(
    pattern: &str,
    glob: Option<&str>,
    resolved: &Path,
    max: u32,
    ignore_case: bool,
    cwd: &Path,
) -> tokio::process::Command {
    build_rg_command_with(
        pattern,
        glob,
        resolved,
        max,
        ignore_case,
        cwd,
        crate::agent::hostenv::for_child(),
    )
}

/// [`build_rg_command`] with the patch as an argument — see
/// [`build_fd_command_with`] for why the parameter exists (the cached patch is
/// EMPTY under `cargo test`).
///
/// Seven parameters, which is the `clippy::too_many_arguments` limit, not over
/// it. If a later edit pushes it to eight, group them — do not add a flag to
/// dodge the cwd, which is the mistake this whole task exists to prevent.
pub(crate) fn build_rg_command_with(
    pattern: &str,
    glob: Option<&str>,
    resolved: &Path,
    max: u32,
    ignore_case: bool,
    cwd: &Path,
    patch: &crate::agent::hostenv::EnvPatch,
) -> tokio::process::Command {
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
    args.push(pattern.to_string());
    args.push(resolved.to_string_lossy().into_owned());

    // Same `crate::agent::childproc` constructor as `fd` — it owns the child's
    // Windows console window policy (`CREATE_NO_WINDOW`) — and the ONLY one for
    // this tool, because `agent/childproc.rs`'s census asserts exactly four
    // constructors under `agent/tools/exec*`.
    let mut cmd = crate::agent::childproc::command("rg");
    // Tool child → null stdin, for the same reason as `fd` and `bash` above.
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.args(&args);
    patch.apply(&mut cmd);
    // BEFORE this line `rg`'s `-g` globs were resolved against wherever the
    // parent happened to be, and every directory-naming glob matched nothing
    // (see the measurement above the spawn in `exec_grep`). No new flag:
    // `--glob`'s own semantics are fine once the child starts where the user
    // thinks they are, and the output keeps its absolute-root shape because the
    // search root is absolute.
    cmd.current_dir(cwd);
    cmd
}

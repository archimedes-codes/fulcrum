//! The Rust tool executors (native-agent-harness Task 1): one per built-in
//! tool, each sandboxed to the session's `cwd` (via the existing
//! [`FsBackend`]) and returning a pi-shaped [`ToolResult`] — `content` is
//! what the LLM sees, `details` is what the UI sees (matching pi's
//! built-in renderers). Both the Phase 1 bridge round-trip (external
//! sessions) and the Phase 2 native harness (in-process) call
//! [`execute_tool`].
//!
//! `powershell` is deliberately absent (Windows-only; the bridge is
//! Linux-only) — it lands with the Windows native sessions (Task 6).
//!
//! Security note: `bash` is GATED, not sandboxed — `sh -c` can read/write
//! anywhere (the permission gate is the control, exactly as in pi's own
//! `bash`). Only the path-param tools (`read`/`write`/`edit`/`find`/
//! `grep`/`ls`) are sandbox-validated via `FsBackend`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::agent::fs_backend::FsBackend;

/// The default `bash` deadline when no `timeout_ms` is given (300 s —
/// matching the bridge's `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`: the native
/// `dispatch_tool` has no outer timeout of its own, so the executor must
/// bound the run itself — a no-`timeout_ms` `bash` is NOT unbounded).
const DEFAULT_BASH_TIMEOUT: Duration = Duration::from_secs(300);

/// The cap on a model-supplied `timeout_ms` (15 min — the model controls
/// the timeout: an unclamped `timeout_ms: 10^9` would run ~11 days. The
/// cap bounds a model-supplied timeout without affecting the default).
const MAX_BASH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Clamp a model-supplied `timeout_ms` to [`MAX_BASH_TIMEOUT`] (the model
/// cannot run a command unbounded — see the cap's doc).
fn clamp_timeout_ms(ms: u64) -> Duration {
    Duration::from_millis(ms).min(MAX_BASH_TIMEOUT)
}

/// The max bytes of combined stdout+stderr `exec_bash` captures (100 KB —
/// overflow is drained but capped, flagged `truncated: true`).
const MAX_OUTPUT_BYTES: usize = 100 * 1024;

/// The max lines `exec_read` returns for a text file with no `limit`.
const MAX_READ_LINES: usize = 2000;

/// The grace deadline for draining a killed `bash` child's pipes (2 s —
/// the pipes may never close if a grandchild escaped the process group;
/// a bounded wait, not an unbounded wait for EOF).
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The default `max_results` for `exec_find` / `exec_grep`.
const DEFAULT_MAX_RESULTS: u32 = 100;

/// One element of a tool result's `content` (pi's `AgentToolResult.content`
/// = `(Text | Image)[]`): a text block or a base64 image block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// `{ "type": "text", "text": "..." }`.
    Text {
        /// The text.
        text: String,
    },
    /// `{ "type": "image", "data": <base64>, "mimeType": "..." }` (pi's
    /// `ImageContent` shape).
    Image {
        /// The base64 image data (no `data:` prefix) + MIME type.
        image: ImageRef,
    },
}

/// A base64 image reference (pi's `ImageContent` payload).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageRef {
    /// base64 (WITHOUT a `data:` prefix).
    pub data: String,
    /// The MIME type (`image/png`, …).
    #[serde(rename = "mimeType")]
    pub mime_type: String,
}

/// A pi-shaped tool result: `content` (the LLM sees this), `details` (the
/// UI sees it), `is_error` (the Rust field) — serialized as **`isError`**
/// (camelCase, the wire contract every bridge handler + pi's
/// `tool_execution_end` use; a snake_case wire name would make pi render
/// every failure as a success).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResult {
    /// The content blocks the LLM sees.
    pub content: Vec<ContentBlock>,
    /// Structured details the UI renders (omitted when there are none).
    pub details: Option<Value>,
    /// Whether the tool call failed (serialized as `isError`).
    pub is_error: bool,
}

impl ToolResult {
    /// A successful single-text-block result.
    fn ok_text(text: String, details: Option<Value>) -> Self {
        Self {
            content: vec![ContentBlock::Text { text }],
            details,
            is_error: false,
        }
    }

    /// A failed single-text-block result.
    fn fail(text: String, details: Option<Value>) -> Self {
        Self {
            content: vec![ContentBlock::Text { text }],
            details,
            is_error: true,
        }
    }
}

/// The execution context for a tool call: the session's `cwd` (the sandbox
/// root for the path-param tools) + the cancellation token wired to the
/// tool's AbortSignal.
#[derive(Clone)]
pub struct ToolCtx {
    /// The session's `cwd` — the sandbox boundary.
    pub cwd: PathBuf,
    /// Cancelled when the tool's AbortSignal fires (kills the `bash` child).
    pub cancel: CancellationToken,
}

/// Run a shell command (`sh -c` on Unix; the command is NOT re-parsed) in
/// `ctx.cwd`, capturing interleaved stdout+stderr (capped at
/// [`MAX_OUTPUT_BYTES`]). On `ctx.cancel` or `timeout_ms` elapsing (no
/// `timeout_ms` = the [`DEFAULT_BASH_TIMEOUT`] 300 s default) the child's
/// WHOLE process group is killed (on Unix the child is spawned in its
/// own group via `process_group(0)`, so backgrounded grandchildren are
/// reaped and the pipes close) and the pipes are drained with a bounded
/// [`DRAIN_GRACE`] deadline — never an unbounded wait for EOF.
///
/// `params`: `{ command: String, timeout_ms?: u64 }`.
pub async fn exec_bash(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let command = match params.get("command").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => return ToolResult::fail("bash: missing `command` parameter".to_string(), None),
    };
    let timeout = params
        .get("timeout_ms")
        .and_then(|v| v.as_u64())
        .map(clamp_timeout_ms);

    // `sh -c` takes the command verbatim (no re-parsing). On Unix the
    // child is spawned in its OWN process group (`process_group(0)`), so
    // a backgrounded grandchild (`sh -c 'sleep 1000 & ...'`) can be
    // reaped with the group — `child.kill()` alone kills only `sh` and
    // leaves the grandchild holding the pipes (the read loop hangs until
    // cancel; the orphaned grandchild outlives the call).
    #[cfg(unix)]
    let mut cmd = {
        let mut c = tokio::process::Command::new("sh");
        c.arg("-c");
        c.process_group(0);
        c
    };
    #[cfg(not(unix))]
    let mut cmd = {
        let mut c = tokio::process::Command::new("cmd");
        c.arg("/C");
        c
    };
    cmd.arg(&command);
    cmd.current_dir(&ctx.cwd);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return ToolResult::fail(format!("bash: failed to start: {e}"), None),
    };

    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    let mut out: Vec<u8> = Vec::new();
    let mut truncated = false;
    let mut cancelled = false;
    let mut stdout_open = true;
    let mut stderr_open = true;
    // One buffer per stream (both select arms borrow them at once).
    let mut out_buf = [0u8; 8192];
    let mut err_buf = [0u8; 8192];
    // Deadline for the timeout. No `timeout_ms` = the 300 s default
    // ([`DEFAULT_BASH_TIMEOUT`] — the bridge's
    // `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`; the native `dispatch_tool` has no
    // outer timeout, so the executor bounds the run itself). Re-armed
    // each loop iteration (`Sleep` is not `Unpin`, so it cannot be a
    // reused `&mut` future).
    let deadline = std::time::Instant::now() + timeout.unwrap_or(DEFAULT_BASH_TIMEOUT);

    loop {
        // NOTE: the LAST arm of `tokio::select!` may not carry an `if`
        // guard (macro limitation) — the unguarded arms come last.
        tokio::select! {
            r = stdout.read(&mut out_buf), if stdout_open => {
                match r {
                    Ok(0) => stdout_open = false,
                    Ok(n) => {
                        let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                        out.extend_from_slice(&out_buf[..take]);
                        if out.len() >= MAX_OUTPUT_BYTES {
                            truncated = true;
                        }
                    }
                    Err(_) => stdout_open = false,
                }
            },
            r = stderr.read(&mut err_buf), if stderr_open => {
                match r {
                    Ok(0) => stderr_open = false,
                    Ok(n) => {
                        let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                        out.extend_from_slice(&err_buf[..take]);
                        if out.len() >= MAX_OUTPUT_BYTES {
                            truncated = true;
                        }
                    }
                    Err(_) => stderr_open = false,
                }
            },
            _ = ctx.cancel.cancelled() => {
                cancelled = true;
                break;
            },
            _ = tokio::time::sleep_until(deadline.into()) => break,
        }
        if !stdout_open && !stderr_open {
            break;
        }
    }

    // The loop broke on cancel/timeout while the pipes were still open
    // (a backgrounded grandchild may hold them): kill the WHOLE process
    // group (on Unix the child is in its own group via `process_group(0)`)
    // so the grandchildren are reaped and the pipes close.
    if stdout_open || stderr_open {
        kill_bash_process_group(&mut child).await;
        // Drain what the group's death frees, bounded by a short grace
        // deadline — NOT an unbounded wait for EOF (a grandchild that
        // escaped the group could hold the pipes forever).
        let grace = std::time::Instant::now() + DRAIN_GRACE;
        loop {
            tokio::select! {
                r = stdout.read(&mut out_buf), if stdout_open => {
                    match r {
                        Ok(0) => stdout_open = false,
                        Ok(n) => {
                            let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                            out.extend_from_slice(&out_buf[..take]);
                            if out.len() >= MAX_OUTPUT_BYTES {
                                truncated = true;
                            }
                        }
                        Err(_) => stdout_open = false,
                    }
                },
                r = stderr.read(&mut err_buf), if stderr_open => {
                    match r {
                        Ok(0) => stderr_open = false,
                        Ok(n) => {
                            let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                            out.extend_from_slice(&err_buf[..take]);
                            if out.len() >= MAX_OUTPUT_BYTES {
                                truncated = true;
                            }
                        }
                        Err(_) => stderr_open = false,
                    }
                },
                _ = tokio::time::sleep_until(grace.into()) => break,
            }
            if !stdout_open && !stderr_open {
                break;
            }
        }
    }

    // `kill` is a no-op when the child already exited; on a kill the exit
    // `code()` is `None` (signal) → reported as -1. `wait` is bounded by
    // the same grace deadline (a reaped pid's `kill` is a bare no-op, but
    // the reap must not be allowed to hang the call either).
    let _ = child.kill().await;
    let exit_code = match tokio::time::timeout(DRAIN_GRACE, child.wait()).await {
        Ok(Ok(s)) => s.code().unwrap_or(-1),
        _ => -1,
    };

    ToolResult {
        content: vec![ContentBlock::Text {
            text: String::from_utf8_lossy(&out).into_owned(),
        }],
        details: Some(json!({
            "exitCode": exit_code,
            "truncated": truncated,
            "cancelled": cancelled,
        })),
        is_error: exit_code != 0 || cancelled,
    }
}

/// Kill a `bash` child's WHOLE process group: on Unix the child was
/// spawned in its own group via `process_group(0)`, so a negative-pid
/// `kill` SIGKILLs the group (including backgrounded grandchildren that
/// hold the pipes open); elsewhere fall back to killing the child only.
async fn kill_bash_process_group(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        // A negative pid SIGKILLs the WHOLE process group (the child is
        // the group leader via `process_group(0)`); `None` when the child
        // was already reaped (then the kill below is a no-op too).
        if let Some(pid) = child.id() {
            let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        }
        // Fallback: kill the direct child as well.
        let _ = child.kill().await;
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill().await;
    }
}

/// Read a file (text, with a 1-indexed `offset`/`limit` line range capped at
/// [`MAX_READ_LINES`]) or return an image `ContentBlock` (image extensions).
/// The path is validated against `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path: String, offset?: u64, limit?: u64 }`.
pub async fn exec_read(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let path = match params.get("path").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return ToolResult::fail("read: missing `path` parameter".to_string(), None),
    };
    let offset = params.get("offset").and_then(|v| v.as_u64()).unwrap_or(1);
    let limit = params.get("limit").and_then(|v| v.as_u64());
    let backend = FsBackend {
        root: ctx.cwd.clone(),
    };
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("read: {e}"), None),
    };

    if let Some(mime) = image_mime(resolved.extension().and_then(|e| e.to_str())) {
        let bytes = match backend.read_bytes(&resolved) {
            Ok(b) => b,
            Err(e) => return ToolResult::fail(format!("read: {e}"), None),
        };
        return ToolResult {
            content: vec![ContentBlock::Image {
                image: ImageRef {
                    data: base64::engine::general_purpose::STANDARD.encode(&bytes),
                    mime_type: mime.to_string(),
                },
            }],
            details: Some(json!({ "path": path, "lines": 0 })),
            is_error: false,
        };
    }

    let text = match backend.read(&resolved) {
        Ok(t) => t,
        Err(e) => return ToolResult::fail(format!("read: {e}"), None),
    };
    let lines: Vec<&str> = text.lines().collect();
    let start = (offset.saturating_sub(1)) as usize;
    let end = limit
        .map(|l| (start + l as usize).min(lines.len()))
        .unwrap_or_else(|| (start + MAX_READ_LINES).min(lines.len()));
    let slice: Vec<String> = lines
        .get(start..end)
        .map(|s| s.iter().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    ToolResult::ok_text(
        slice.join("\n"),
        Some(json!({ "path": path, "lines": slice.len() })),
    )
}

/// The MIME type for an image file extension (`None` for non-images).
fn image_mime(ext: Option<&str>) -> Option<&'static str> {
    match ext {
        Some("png") => Some("image/png"),
        Some("jpg") | Some("jpeg") => Some("image/jpeg"),
        Some("gif") => Some("image/gif"),
        Some("webp") => Some("image/webp"),
        Some("bmp") => Some("image/bmp"),
        _ => None,
    }
}

/// Write a file (overwrite; parent directories are created, still inside the
/// sandbox). The path is validated against `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path: String, content: String }`.
pub async fn exec_write(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let (path, content) = match (
        params.get("path").and_then(|v| v.as_str()),
        params.get("content").and_then(|v| v.as_str()),
    ) {
        (Some(p), Some(c)) => (p.to_string(), c.to_string()),
        _ => {
            return ToolResult::fail(
                "write: missing `path` or `content` parameter".to_string(),
                None,
            )
        }
    };
    let backend = FsBackend {
        root: ctx.cwd.clone(),
    };
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("write: {e}"), None),
    };
    let bytes = content.len();
    if let Err(e) = backend.write(&resolved, &content) {
        return ToolResult::fail(format!("write: {e}"), None);
    }
    ToolResult::ok_text(
        format!("Wrote {bytes} bytes to {path}"),
        Some(json!({ "path": path, "bytes": bytes })),
    )
}

/// Replace `old_text` with `new_text` in a file (the first occurrence, or all
/// when `replace_all`), writing back a unified diff in `details`. The path is
/// validated against `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path: String, old_text: String, new_text: String, replace_all?: bool }`.
pub async fn exec_edit(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let (path, old_text, new_text) = match (
        params.get("path").and_then(|v| v.as_str()),
        params.get("old_text").and_then(|v| v.as_str()),
        params.get("new_text").and_then(|v| v.as_str()),
    ) {
        (Some(p), Some(o), Some(n)) => (p.to_string(), o.to_string(), n.to_string()),
        _ => {
            return ToolResult::fail(
                "edit: missing `path`, `old_text`, or `new_text` parameter".to_string(),
                None,
            )
        }
    };
    // `old_text: ""` passes `contains("")` and `replacen`/`replace` misbehave
    // on it (pi rejects an empty `old_text`) — a distinct guard.
    if old_text.is_empty() {
        return ToolResult::fail("edit: `old_text` must not be empty".to_string(), None);
    }
    let replace_all = params
        .get("replace_all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let backend = FsBackend {
        root: ctx.cwd.clone(),
    };
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("edit: {e}"), None),
    };
    let old = match backend.read(&resolved) {
        Ok(t) => t,
        Err(e) => return ToolResult::fail(format!("edit: {e}"), None),
    };
    if !old.contains(&old_text) {
        return ToolResult::fail(format!("old_text not found in {path}"), None);
    }
    let (new, occurrences) = if replace_all {
        (
            old.replace(&old_text, &new_text),
            old.matches(&old_text).count(),
        )
    } else {
        (old.replacen(&old_text, &new_text, 1), 1)
    };
    if let Err(e) = backend.write(&resolved, &new) {
        return ToolResult::fail(format!("edit: {e}"), None);
    }
    ToolResult::ok_text(
        format!("Edited {path}"),
        Some(json!({
            "path": path,
            "occurrences": occurrences,
            "diff": unified_diff(&old, &new, &path),
        })),
    )
}

/// A minimal unified diff (a single hunk: the unchanged prefix/suffix lines
/// are elided, the changed middle is emitted as `-`/`+` lines).
fn unified_diff(old: &str, new: &str, path: &str) -> String {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();
    let mut prefix = 0;
    while prefix < old_lines.len().min(new_lines.len()) && old_lines[prefix] == new_lines[prefix] {
        prefix += 1;
    }
    let max_suffix = (old_lines.len() - prefix).min(new_lines.len() - prefix);
    let mut suffix = 0;
    while suffix < max_suffix
        && old_lines[old_lines.len() - 1 - suffix] == new_lines[new_lines.len() - 1 - suffix]
    {
        suffix += 1;
    }
    let old_start = prefix;
    let old_end = old_lines.len() - suffix;
    let new_start = prefix;
    let new_end = new_lines.len() - suffix;
    let mut d = format!("--- a/{path}\n+++ b/{path}\n");
    d.push_str(&format!(
        "@@ -{},{} +{},{} @@\n",
        old_start + 1,
        old_end - old_start,
        new_start + 1,
        new_end - new_start
    ));
    for line in &old_lines[old_start..old_end] {
        d.push_str(&format!("-{line}\n"));
    }
    for line in &new_lines[new_start..new_end] {
        d.push_str(&format!("+{line}\n"));
    }
    d
}

/// Find files by glob pattern: shells out to `fd` (`fd -g <pattern>
/// <path> --max-results <n>`) when available, else falls back to a `walkdir`
/// walk with a built-in glob matcher. The search path is validated against
/// `ctx.cwd` via `FsBackend`.
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
    let backend = FsBackend {
        root: ctx.cwd.clone(),
    };
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("find: {e}"), None),
    };

    // Prefer `fd` when it is installed (the spawn fails with NotFound
    // otherwise — that is the availability check, no `which` needed).
    // `--` before the path (the `rg --` hardening — a pattern that looks
    // like a flag is not re-parsed as one).
    let fd = tokio::process::Command::new("fd")
        .args([
            "--max-results",
            &max.to_string(),
            "-g",
            &pattern,
            "--",
            &resolved.to_string_lossy(),
        ])
        .output()
        .await;
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
fn glob_match(pattern: &str, candidate: &str) -> bool {
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
    let backend = FsBackend {
        root: ctx.cwd.clone(),
    };
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

    match tokio::process::Command::new("rg")
        .args(&args)
        .output()
        .await
    {
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

/// List a directory (`read_dir`; entries sorted, directories get a `/`
/// suffix; `long` prefixes the file size). The path is validated against
/// `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path?: String, long?: bool }`.
pub async fn exec_ls(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let dir = params
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let long = params
        .get("long")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let backend = FsBackend {
        root: ctx.cwd.clone(),
    };
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("ls: {e}"), None),
    };
    // `read_dir` is BLOCKING (a big directory would stall a tokio
    // worker): it runs on the blocking thread pool.
    let (names, error) = match tokio::task::spawn_blocking(move || {
        let entries = match std::fs::read_dir(&resolved) {
            Ok(e) => e,
            Err(e) => return (Vec::new(), Some(format!("ls: {e}"))),
        };
        let mut names: Vec<String> = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => return (names, Some(format!("ls: {e}"))),
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            let suffix = if is_dir { "/" } else { "" };
            names.push(if long {
                format!("{size} {name}{suffix}")
            } else {
                format!("{name}{suffix}")
            });
        }
        names.sort();
        (names, None)
    })
    .await
    {
        Ok(r) => r,
        Err(_) => (
            Vec::new(),
            Some("ls: the directory listing failed (blocking task join error)".to_string()),
        ),
    };
    if let Some(e) = error {
        return ToolResult::fail(e, None);
    }
    ToolResult::ok_text(names.join("\n"), Some(json!({ "entries": names.len() })))
}

/// Dispatch a tool call to its executor (the entry point both the Phase 1
/// bridge round-trip and the Phase 2 native harness call).
pub async fn execute_tool(ctx: &ToolCtx, tool: &str, params: &Value) -> ToolResult {
    match tool {
        "bash" => exec_bash(ctx, params).await,
        "read" => exec_read(ctx, params).await,
        "write" => exec_write(ctx, params).await,
        "edit" => exec_edit(ctx, params).await,
        "find" => exec_find(ctx, params).await,
        "grep" => exec_grep(ctx, params).await,
        "ls" => exec_ls(ctx, params).await,
        other => ToolResult::fail(format!("unknown tool: {other}"), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh temp dir as the sandbox `cwd`.
    fn temp_cwd(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("exec-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ctx(cwd: &Path) -> ToolCtx {
        ToolCtx {
            cwd: cwd.to_path_buf(),
            cancel: CancellationToken::new(),
        }
    }

    /// The first text block's text (panics if the shape is wrong).
    fn text_of(r: &ToolResult) -> &str {
        match &r.content[0] {
            ContentBlock::Text { text } => text,
            other => panic!("expected a text block, got {other:?}"),
        }
    }

    // ── serialization ────────────────────────────────────────────────────

    #[test]
    fn tool_result_serializes_is_error_camel_case() {
        let r = ToolResult {
            content: vec![ContentBlock::Text {
                text: "x".to_string(),
            }],
            details: Some(json!({ "exitCode": 0 })),
            is_error: true,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["isError"], true, "the wire name is `isError`");
        assert!(
            v.get("is_error").is_none(),
            "no snake_case `is_error` on the wire"
        );
        // Round-trip.
        let back: ToolResult = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    // ── bash ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn bash_happy_path_serializes_camel_case() {
        let cwd = temp_cwd("bash");
        let r = execute_tool(&ctx(&cwd), "bash", &json!({ "command": "echo hi" })).await;
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["content"][0]["type"], "text");
        assert_eq!(v["content"][0]["text"], "hi\n");
        assert_eq!(v["details"]["exitCode"], 0);
        assert_eq!(v["details"]["truncated"], false);
        assert_eq!(v["details"]["cancelled"], false);
        assert_eq!(v["isError"], false);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn bash_nonzero_exit_is_error() {
        let cwd = temp_cwd("bash-err");
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({ "command": "echo oops 1>&2; exit 3" }),
        )
        .await;
        assert!(r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["exitCode"], 3);
        assert!(text_of(&r).contains("oops"), "stderr is captured");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn bash_cancel_kills_child() {
        let cwd = temp_cwd("bash-cancel");
        let token = CancellationToken::new();
        let c = ToolCtx {
            cwd: cwd.clone(),
            cancel: token.clone(),
        };
        let handle = tokio::spawn(async move {
            let start = std::time::Instant::now();
            let r = execute_tool(&c, "bash", &json!({ "command": "sleep 5" })).await;
            (r, start.elapsed())
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        token.cancel();
        let (r, elapsed) = handle.await.unwrap();
        assert_eq!(r.details.as_ref().unwrap()["cancelled"], true);
        assert!(r.is_error);
        assert!(
            elapsed < Duration::from_secs(4),
            "the child was killed (took {elapsed:?})"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn bash_timeout_kills_child() {
        let cwd = temp_cwd("bash-timeout");
        let start = std::time::Instant::now();
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({ "command": "sleep 5", "timeout_ms": 300 }),
        )
        .await;
        assert!(r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
        assert!(
            start.elapsed() < Duration::from_secs(4),
            "the timeout killed the child"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn bash_backgrounded_grandchild_killed_with_group() {
        // Regression test for the unbounded-hang / orphaned-grandchild hole:
        // the `sh` parent exits immediately, but the backgrounded subshell
        // holds the pipes. Without a process-group kill, the grandchild
        // outlives `exec_bash` and writes the marker at the 3 s mark (and
        // a no-`timeout_ms` run used to wait ~584 years for the pipes' EOF).
        let cwd = temp_cwd("bash-bg");
        let start = std::time::Instant::now();
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({
                "command": "( sleep 3; echo alive >> marker.txt ) & echo done",
                "timeout_ms": 1000,
            }),
        )
        .await;
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the deadline ended the run (took {:?})",
            start.elapsed()
        );
        assert!(
            text_of(&r).contains("done"),
            "the captured output is returned: {:?}",
            text_of(&r)
        );
        // Wait past the moment an ORPHANED grandchild would have written the
        // marker (3 s) — with the process-group kill it was SIGKILLed.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            !cwd.join("marker.txt").exists(),
            "the backgrounded grandchild was killed with the process group"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The native `bash` deadline without a `timeout_ms` is 300 s (matching
    /// the bridge's `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`) — NOT unbounded.
    #[test]
    fn bash_default_deadline_is_300s() {
        assert_eq!(DEFAULT_BASH_TIMEOUT, Duration::from_secs(300));
    }

    /// A model-supplied `timeout_ms` is CLAMPED to the 15 min max (the
    /// model controls the timeout — an unclamped `timeout_ms: 10^9` would
    /// run ~11 days); a `timeout_ms` under the cap is unchanged.
    #[test]
    fn bash_timeout_ms_is_clamped_to_the_max() {
        assert_eq!(clamp_timeout_ms(10u64.pow(9)), MAX_BASH_TIMEOUT);
        assert_eq!(MAX_BASH_TIMEOUT, Duration::from_secs(15 * 60));
        assert_eq!(clamp_timeout_ms(1000), Duration::from_millis(1000));
        assert_eq!(clamp_timeout_ms(15 * 60 * 1000), MAX_BASH_TIMEOUT);
    }

    #[tokio::test]
    async fn bash_output_capped_at_max() {
        let cwd = temp_cwd("bash-cap");
        // 300 KB of `x` — well over the 100 KB cap.
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({ "command": "head -c 300000 /dev/zero | tr '\\0' 'x'" }),
        )
        .await;
        assert!(
            !r.is_error,
            "a truncated-but-successful run is not an error"
        );
        assert_eq!(r.details.as_ref().unwrap()["truncated"], true);
        assert!(
            text_of(&r).len() <= MAX_OUTPUT_BYTES,
            "output is capped at {MAX_OUTPUT_BYTES} bytes (got {})",
            text_of(&r).len()
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── read ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn read_happy_and_offset_limit() {
        let cwd = temp_cwd("read");
        std::fs::write(cwd.join("f.txt"), "line1\nline2\nline3\nline4\nline5").unwrap();
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "f.txt" })).await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "line1\nline2\nline3\nline4\nline5");
        assert_eq!(r.details.as_ref().unwrap()["lines"], 5);
        // 1-indexed offset + limit.
        let r = execute_tool(
            &ctx(&cwd),
            "read",
            &json!({ "path": "f.txt", "offset": 2, "limit": 2 }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "line2\nline3");
        assert_eq!(r.details.as_ref().unwrap()["lines"], 2);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_image_returns_image_block() {
        let cwd = temp_cwd("read-img");
        // A minimal 1×1 transparent PNG.
        let png: &[u8] = &[
            137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1,
            8, 6, 0, 0, 0, 195, 2, 217, 66, 0, 0, 0, 6, 73, 68, 65, 84, 78, 97, 220, 8, 207, 1,
            100, 0, 24, 156, 7, 150, 4, 1, 0, 0, 0, 0, 71, 69, 78, 67, 81, 209, 169, 190,
        ];
        std::fs::write(cwd.join("p.png"), png).unwrap();
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "p.png" })).await;
        assert!(!r.is_error);
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["content"][0]["type"], "image");
        assert_eq!(v["content"][0]["image"]["mimeType"], "image/png");
        assert_eq!(
            v["content"][0]["image"]["data"],
            base64::engine::general_purpose::STANDARD.encode(png)
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_escape_rejected() {
        let cwd = temp_cwd("read-esc");
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "../etc/passwd" })).await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── write ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn write_happy() {
        let cwd = temp_cwd("write");
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "a.txt", "content": "hello" }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "Wrote 5 bytes to a.txt");
        assert_eq!(r.details.as_ref().unwrap()["bytes"], 5);
        assert_eq!(std::fs::read_to_string(cwd.join("a.txt")).unwrap(), "hello");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn write_creates_parent_dirs() {
        let cwd = temp_cwd("write-nested");
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "nested/deep/a.txt", "content": "x" }),
        )
        .await;
        assert!(!r.is_error);
        assert!(cwd.join("nested/deep/a.txt").exists());
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn write_escape_rejected() {
        let cwd = temp_cwd("write-esc");
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "../evil.txt", "content": "x" }),
        )
        .await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── edit ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn edit_replaces_first_occurrence() {
        let cwd = temp_cwd("edit");
        std::fs::write(cwd.join("e.txt"), "X mid X").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "X", "new_text": "Y" }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "Edited e.txt");
        assert_eq!(r.details.as_ref().unwrap()["occurrences"], 1);
        let diff = r.details.as_ref().unwrap()["diff"].as_str().unwrap();
        assert!(
            diff.contains("-X mid X"),
            "the diff has the old line: {diff}"
        );
        assert!(
            diff.contains("+Y mid X"),
            "the diff has the new line: {diff}"
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("e.txt")).unwrap(),
            "Y mid X"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_replace_all() {
        let cwd = temp_cwd("edit-all");
        std::fs::write(cwd.join("e.txt"), "X mid X").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "X", "new_text": "Y", "replace_all": true }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["occurrences"], 2);
        assert_eq!(
            std::fs::read_to_string(cwd.join("e.txt")).unwrap(),
            "Y mid Y"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_empty_old_text_rejected() {
        // `old_text: ""` passes `contains("")` and `replacen`/`replace`
        // misbehave on it (pi rejects it) — a distinct guard.
        let cwd = temp_cwd("edit-empty");
        std::fs::write(cwd.join("e.txt"), "X mid X").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "", "new_text": "Y" }),
        )
        .await;
        assert!(r.is_error);
        assert!(text_of(&r).contains("must not be empty"));
        // The file is untouched.
        assert_eq!(
            std::fs::read_to_string(cwd.join("e.txt")).unwrap(),
            "X mid X"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_not_found_is_error() {
        let cwd = temp_cwd("edit-miss");
        std::fs::write(cwd.join("e.txt"), "no match here").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "Z", "new_text": "W" }),
        )
        .await;
        assert!(r.is_error);
        assert!(text_of(&r).contains("old_text not found in e.txt"));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_escape_rejected() {
        let cwd = temp_cwd("edit-esc");
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "../e.txt", "old_text": "X", "new_text": "Y" }),
        )
        .await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── find ─────────────────────────────────────────────────────────────

    /// `fd` is available (spawn `fd --version`).
    fn fd_available() -> bool {
        std::process::Command::new("fd")
            .arg("--version")
            .output()
            .is_ok()
    }

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

    // ── ls ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ls_lists_entries() {
        let cwd = temp_cwd("ls");
        std::fs::write(cwd.join("a.txt"), "a").unwrap();
        std::fs::write(cwd.join("b.txt"), "b").unwrap();
        std::fs::create_dir_all(cwd.join("d")).unwrap();
        let r = execute_tool(&ctx(&cwd), "ls", &json!({})).await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["entries"], 3);
        let lines: Vec<&str> = text_of(&r).lines().collect();
        assert_eq!(lines, vec!["a.txt", "b.txt", "d/"]);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn ls_escape_rejected() {
        let cwd = temp_cwd("ls-esc");
        let r = execute_tool(&ctx(&cwd), "ls", &json!({ "path": "../" })).await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── dispatch ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn unknown_tool_is_error() {
        let cwd = temp_cwd("unknown");
        let r = execute_tool(&ctx(&cwd), "nope", &json!({})).await;
        assert!(r.is_error);
        assert_eq!(text_of(&r), "unknown tool: nope");
        let _ = std::fs::remove_dir_all(&cwd);
    }
}

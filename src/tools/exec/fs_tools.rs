use std::path::Path;

use base64::Engine;
use serde_json::{json, Value};

use super::access::{read_backend, read_rule_refusal, write_backend, write_rule_refusal};
use crate::agent::tools::types::{ContentBlock, ImageRef, ToolCtx, ToolResult};

/// The max lines `exec_read` returns for a text file with no `limit`.
pub(crate) const MAX_READ_LINES: usize = 2000;

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
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("read: {e}"), None),
    };
    // (T03 rows 1–2, read direction) A rule `deny` refuses a READ too — the
    // row is about the access, not the direction. There is NO read-side guard
    // ask anywhere (the gate's `is_guard_path` is write-only), so this can only
    // refuse or stay out of the way.
    if let Some(msg) = read_rule_refusal(ctx, "read", &resolved) {
        return ToolResult::fail(msg, None);
    }

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
pub(crate) fn image_mime(ext: Option<&str>) -> Option<&'static str> {
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
    let backend = write_backend(ctx);
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("write: {e}"), None),
    };
    // (T03 rows 1–2) The rules, consulted through the ONE resolver BEFORE any
    // guard-membership wording, on the SAME `&resolved` the write below uses.
    // A rule `deny` refuses (with the guard-membership sentence when the target
    // is under one of the four dirs); a rule `allow` proceeds; NO rule matching
    // also proceeds — the guard's row-3 ASK is the GATE's decision and the user
    // has already answered it (or the Space is trusted and the spec says the
    // write lands silently). Refusing here would make the prompt a lie.
    if let Some(msg) = write_rule_refusal(ctx, "write", &resolved) {
        return ToolResult::fail(msg, None);
    }
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
    let backend = write_backend(ctx);
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("edit: {e}"), None),
    };
    // (T03 rows 1–2) `edit` is a write, so it consults the SAME resolver on
    // the SAME resolved target — see `exec_write` for the row semantics.
    if let Some(msg) = write_rule_refusal(ctx, "edit", &resolved) {
        return ToolResult::fail(msg, None);
    }
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
pub(crate) fn unified_diff(old: &str, new: &str, path: &str) -> String {
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
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("ls: {e}"), None),
    };
    // (T03 rows 1–2, read direction) See `exec_read`. An absent `path`
    // defaulted to `"."` above, so it resolves inside the `cwd` and matches no
    // rule — an absent-path call never reaches a rule refusal, let alone a
    // guard ask.
    if let Some(msg) = read_rule_refusal(ctx, "ls", &resolved) {
        return ToolResult::fail(msg, None);
    }
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

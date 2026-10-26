use base64::Engine;
use serde_json::json;

use super::support::*;
use crate::agent::tools::exec::execute_tool;

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
}

#[tokio::test]
async fn read_image_returns_image_block() {
    let cwd = temp_cwd("read-img");
    // A minimal 1×1 transparent PNG.
    let png: &[u8] = &[
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6,
        0, 0, 0, 195, 2, 217, 66, 0, 0, 0, 6, 73, 68, 65, 84, 78, 97, 220, 8, 207, 1, 100, 0, 24,
        156, 7, 150, 4, 1, 0, 0, 0, 0, 71, 69, 78, 67, 81, 209, 169, 190,
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
}

#[tokio::test]
async fn read_escape_rejected() {
    let cwd = temp_cwd("read-esc");
    let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "../etc/passwd" })).await;
    assert!(r.is_error, "a sandbox escape must be rejected");
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
}

#[tokio::test]
async fn ls_escape_rejected() {
    let cwd = temp_cwd("ls-esc");
    let r = execute_tool(&ctx(&cwd), "ls", &json!({ "path": "../" })).await;
    assert!(r.is_error, "a sandbox escape must be rejected");
}

// ── dispatch ─────────────────────────────────────────────────────────

#[tokio::test]
async fn unknown_tool_is_error() {
    let cwd = temp_cwd("unknown");
    let r = execute_tool(&ctx(&cwd), "nope", &json!({})).await;
    assert!(r.is_error);
    assert_eq!(text_of(&r), "unknown tool: nope");
}

#[tokio::test]
async fn web_search_dispatches_via_execute_tool() {
    let cwd = temp_cwd("web-search");
    let r = execute_tool(&ctx(&cwd), "web_search", &json!({})).await;
    // Missing query returns error result from exec_web_search, not "unknown tool"
    assert!(r.is_error);
    assert!(text_of(&r).contains("web_search requires 'query' or 'queries'"));
}

#[tokio::test]
async fn fetch_content_and_get_search_content_dispatch_via_execute_tool() {
    let cwd = temp_cwd("web-fetch");
    let r1 = execute_tool(&ctx(&cwd), "fetch_content", &json!({})).await;
    assert!(r1.is_error);
    assert!(text_of(&r1).contains("fetch_content: missing `url` parameter"));

    let r2 = execute_tool(&ctx(&cwd), "get_search_content", &json!({})).await;
    assert!(r2.is_error);
    assert!(text_of(&r2).contains("get_search_content: missing `response_id` parameter"));
}

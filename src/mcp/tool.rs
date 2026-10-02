//! The `mcp` proxy tool handler (ADR 0018): the param dispatch mirroring the
//! retired `@pi-archimedes/mcp` proxy tool's `buildProxyToolExecute` —
//! `status` (the default) / `server` / `search` / `describe` / `tool`
//! (+ `args`) / `connect` / `action: "auth"`.

use std::time::Duration;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::manager::McpManager;
use super::types::ToolCallResult;
use crate::agent::tools::{ContentBlock, ToolResult};

/// The `mcp` tool's per-request timeout (a stdio / HTTP request bound).
const TIMEOUT: Duration = Duration::from_secs(60);

/// A successful single-text-block `ToolResult`.
fn ok(text: String) -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text { text }],
        details: None,
        is_error: false,
    }
}

/// A failed single-text-block `ToolResult`.
fn fail(text: String) -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text { text }],
        details: None,
        is_error: true,
    }
}

/// Map a `tools/call` outcome to a `ToolResult`: a `ToolCallResult` with
/// `is_error` → a FAILED `ToolResult` with the text (the `isError` mapping);
/// a transport error → a failed `ToolResult` with the error text.
fn map_call_outcome(outcome: Result<ToolCallResult, String>) -> ToolResult {
    match outcome {
        Ok(r) if r.is_error => fail(r.text),
        Ok(r) => ok(r.text),
        Err(e) => fail(e),
    }
}

/// The `mcp` tool's `search` / `describe` / call-tool tool source: lazily
/// connect every server (the `tools/list` is cached per connection) and
/// collect `(server, tool)` pairs. A server that can't connect (`NeedsAuth`
/// / `Error` — not auto-retried) is skipped (its tools are simply absent).
async fn all_tools(
    manager: &mut McpManager,
    cancel: &CancellationToken,
) -> Vec<(String, super::types::ToolInfo)> {
    let mut out = Vec::new();
    for name in manager.server_names() {
        if let Ok(tools) = manager.list_tools(&name, cancel, TIMEOUT).await {
            for t in tools {
                out.push((name.clone(), t));
            }
        }
    }
    out
}

/// The `mcp` proxy tool (the param dispatch — the `@pi-archimedes/mcp`
/// `buildProxyToolExecute` mirrored):
/// - `status` (no `tool`/`search`/`describe`/`connect`/`server`, or
///   `action: "status"`) — the `mcp({})` default: the server status lines.
/// - `connect` — eagerly connect a server (the outcome line).
/// - `action: "auth"` + `server` — run the interactive OAuth flow.
/// - `search` — matching tools across servers (`[server] name — description`).
/// - `describe` — a tool's full `inputSchema` (JSON).
/// - `server` (no `tool`) — that server's tools (name + description, one per
///   line).
/// - `tool` (+ `args` string-or-object) — `tools/call` (the result text = the
///   content joined; `isError` → a failed `ToolResult`). A tool-name
///   disambiguation: an exact raw-name match across servers wins; a tie →
///   the `server` param is required (an error naming the candidates).
/// - an unknown `action` — an error listing the valid ones.
pub async fn mcp_tool(
    manager: &mut McpManager,
    args: &Value,
    cancel: &CancellationToken,
) -> ToolResult {
    let tool = args.get("tool").and_then(Value::as_str).map(str::to_string);
    let args_param = args.get("args").cloned();
    let search = args
        .get("search")
        .and_then(Value::as_str)
        .map(str::to_string);
    let describe = args
        .get("describe")
        .and_then(Value::as_str)
        .map(str::to_string);
    let connect = args
        .get("connect")
        .and_then(Value::as_str)
        .map(str::to_string);
    let server = args
        .get("server")
        .and_then(Value::as_str)
        .map(str::to_string);
    let action = args
        .get("action")
        .and_then(Value::as_str)
        .map(str::to_string);

    // ── status (no tool/search/describe/connect/server, or action: "status") ──
    if tool.is_none()
        && search.is_none()
        && describe.is_none()
        && connect.is_none()
        && server.is_none()
        && action.as_deref().map(|a| a == "status").unwrap_or(true)
    {
        return ok(manager.status_lines().join("\n"));
    }

    // ── connect ──
    if let Some(name) = &connect {
        return match manager.connect(name, cancel, TIMEOUT).await {
            Ok(outcome) => ok(outcome),
            Err(e) => fail(e),
        };
    }

    // ── action: "auth" + server (the interactive OAuth flow) ──
    if action.as_deref() == Some("auth") {
        let Some(name) = &server else {
            return fail("the `auth` action requires a `server` param".to_string());
        };
        return match manager.authenticate(name, cancel).await {
            Ok(outcome) => ok(outcome),
            Err(e) => fail(e),
        };
    }

    // ── unknown action ──
    if let Some(a) = &action {
        if a != "status" {
            return fail(format!("Unknown action: {a} (valid: status, auth)"));
        }
    }

    // ── search ──
    if let Some(q) = &search {
        let all = all_tools(manager, cancel).await;
        let q_lower = q.to_lowercase();
        let results: Vec<String> = all
            .iter()
            .filter(|(s, t)| {
                server
                    .as_deref()
                    .map(|srv| srv == s.as_str())
                    .unwrap_or(true)
                    && (t.name.to_lowercase().contains(&q_lower)
                        || t.description
                            .as_deref()
                            .unwrap_or("")
                            .to_lowercase()
                            .contains(&q_lower))
            })
            .map(|(s, t)| {
                format!(
                    "[{s}] {} — {}",
                    t.name,
                    t.description.as_deref().unwrap_or("(no description)")
                )
            })
            .collect();
        if results.is_empty() {
            return ok(format!("No tools matching \"{q}\""));
        }
        return ok(results.join("\n"));
    }

    // ── describe ──
    if let Some(name) = &describe {
        let all = all_tools(manager, cancel).await;
        let Some((s, t)) = all.iter().find(|(_, ti)| ti.name == name.as_str()) else {
            return ok(format!("Tool not found: {name}"));
        };
        let schema =
            serde_json::to_string_pretty(&t.input_schema).unwrap_or_else(|_| "{}".to_string());
        return ok(format!(
            "{name} ({s})\n{}\n\nSchema:\n{}",
            t.description.as_deref().unwrap_or(""),
            schema
        ));
    }

    // ── list server (server param only, no tool) ──
    if let Some(name) = &server {
        if tool.is_none() {
            return match manager.list_tools(name, cancel, TIMEOUT).await {
                Ok(tools) => {
                    let lines: Vec<String> = tools
                        .iter()
                        .map(|t| {
                            format!(
                                "{}: {}",
                                t.name,
                                t.description.as_deref().unwrap_or("(no description)")
                            )
                        })
                        .collect();
                    let joined = lines.join("\n");
                    ok(if joined.is_empty() {
                        "(no tools)".to_string()
                    } else {
                        joined
                    })
                }
                Err(e) => fail(e),
            };
        }
    }

    // ── call tool ──
    if let Some(t) = &tool {
        // Resolve the server (an explicit `server`, or an exact raw-name
        // match across servers; a tie → the `server` param is required).
        let (server_name, raw_tool) = match &server {
            Some(s) => (s.clone(), t.clone()),
            None => {
                let all = all_tools(manager, cancel).await;
                let matches: Vec<String> = all
                    .iter()
                    .filter(|(_, ti)| ti.name == t.as_str())
                    .map(|(s, _)| s.clone())
                    .collect();
                if matches.is_empty() {
                    return ok(format!("Tool not found: {t}"));
                }
                if matches.len() == 1 {
                    (matches[0].clone(), t.clone())
                } else {
                    return fail(format!(
                        "Tool '{t}' is ambiguous (servers: {}); use the `server` param",
                        matches.join(", ")
                    ));
                }
            }
        };
        // Parse `args` (a JSON string → parse; an object → as-is; absent →
        // `{}`). A JSON-string parse failure is a tool error.
        let tool_args = match &args_param {
            Some(Value::String(s)) => match serde_json::from_str::<Value>(s) {
                Ok(v) => v,
                Err(e) => return fail(format!("Invalid JSON in args: {e}")),
            },
            Some(v) => v.clone(),
            None => Value::Object(Default::default()),
        };
        return map_call_outcome(
            manager
                .call_tool(&server_name, &raw_tool, &tool_args, cancel, TIMEOUT)
                .await,
        );
    }

    // ── fallback (the status default was already handled above) ──
    fail("Unknown action (valid: status, auth)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp `McpManager` over a temp config (the servers point at the fake
    /// stdio binary — `target/debug/fake_mcp_stdio`).
    fn manager_with(servers_json: &Value) -> (McpManager, tempfile::TempDir) {
        let bin = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ));
        let dir = tempfile::tempdir().expect("a temp dir");
        let home = dir.path().to_path_buf();
        let project = dir.path().to_path_buf();
        let global = home.join(".pi/agent/mcp.json");
        std::fs::create_dir_all(global.parent().expect("a parent")).expect("creates the dir");
        let mut mcp = serde_json::Map::new();
        for (name, def) in servers_json.as_object().expect("an object").iter() {
            // A bare string def → the fake stdio binary (a `command` server);
            // an object def → used as-is.
            let def = if def.is_string() {
                serde_json::json!({ "command": bin.display().to_string() })
            } else {
                def.clone()
            };
            mcp.insert(name.clone(), def);
        }
        std::fs::write(
            &global,
            serde_json::json!({ "mcpServers": mcp }).to_string(),
        )
        .expect("writes the mcp.json");
        (McpManager::new(home, project, None), dir)
    }

    fn cancel() -> CancellationToken {
        CancellationToken::new()
    }

    fn text(r: &ToolResult) -> String {
        match &r.content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => panic!("an unexpected non-text content block"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_no_servers() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({}));
        let r = mcp_tool(&mut manager, &serde_json::json!({}), &cancel()).await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "No MCP servers configured.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn status_lists_the_servers() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(&mut manager, &serde_json::json!({}), &cancel()).await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "a: Disconnected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connect_action() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "connect": "a" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "a: connected");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn search_finds_the_tool() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "echo" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "[a] echo — echo its text argument");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn search_no_match() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "zzz" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "No tools matching \"zzz\"");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn describe_shows_the_schema() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "echo" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        let t = text(&r);
        assert!(t.contains("echo (a)"), "the tool + server: {t}");
        assert!(t.contains("\"type\": \"object\""), "the schema: {t}");
        assert!(t.contains("\"text\""), "the schema's property: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn server_lists_its_tools() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "server": "a" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        // The fake server's three tools (one per line).
        let t = text(&r);
        assert!(t.contains("echo: echo its text argument"), "{t}");
        assert!(t.contains("hang: never answers"), "{t}");
        assert!(t.contains("err: always errors"), "{t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn call_tool_with_an_object_args() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo", "args": { "text": "hi" } }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "hi");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn call_tool_with_a_json_string_args() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo", "args": "{\"text\": \"hi\"}" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "hi");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn call_tool_with_an_invalid_json_string_args_is_an_error() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo", "args": "not json" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error, "a JSON-string parse failure is a tool error");
        assert!(text(&r).contains("Invalid JSON in args"), "{:?}", text(&r));
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_ambiguous_tool_name_requires_the_server_param() {
        // Two servers (both the fake binary) — both have an `echo` tool.
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake", "b": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error, "an ambiguous tool name is an error");
        let t = text(&r);
        assert!(t.contains("ambiguous"), "{t}");
        assert!(
            t.contains("a") && t.contains("b"),
            "the candidates are named: {t}"
        );
        // With the `server` param, the ambiguity is resolved.
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo", "server": "a", "args": { "text": "x" } }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text(&r), "x");
        manager.close_all();
    }

    #[test]
    fn the_is_error_mapping_is_a_failed_tool_result() {
        // A `ToolCallResult` with `is_error: true` → a failed `ToolResult`
        // with the text.
        let r = map_call_outcome(Ok(ToolCallResult {
            text: "tool failed".to_string(),
            is_error: true,
            non_text_count: 0,
        }));
        assert!(r.is_error);
        assert_eq!(text(&r), "tool failed");
        // A successful `ToolCallResult` → a successful `ToolResult`.
        let r = map_call_outcome(Ok(ToolCallResult {
            text: "ok".to_string(),
            is_error: false,
            non_text_count: 0,
        }));
        assert!(!r.is_error);
        assert_eq!(text(&r), "ok");
        // A transport error → a failed `ToolResult` with the error text.
        let r = map_call_outcome(Err("boom".to_string()));
        assert!(r.is_error);
        assert_eq!(text(&r), "boom");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_auth_action_on_a_non_oauth_server_is_an_error() {
        // A stdio server is not an OAuth server → the `auth` action is an
        // error.
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "action": "auth", "server": "a" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error);
        assert!(text(&r).contains("OAuth"), "{:?}", text(&r));
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unknown_action_is_an_error() {
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "action": "bogus" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error);
        assert!(text(&r).contains("Unknown action"), "{:?}", text(&r));
        manager.close_all();
    }
}

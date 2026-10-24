//! The `mcp` proxy tool handler (ADR 0018): the param dispatch mirroring the
//! retired `@pi-archimedes/mcp` proxy tool's `buildProxyToolExecute` —
//! `status` (the default) / `server` / `search` / `describe` / `tool`
//! (+ `args`) / `connect` / `action: "auth"`.
//!
//! Issue #68: a `search` / `describe` / name-resolution miss computed while at
//! least one server was UNAVAILABLE (a `NeedsAuth` / `Error` state, a failed
//! connect) is a FAILURE naming each skipped server and its state — not a
//! successful "no such tool" the model reads as "it doesn't exist". With every
//! server reachable the wording is unchanged and stays a success. The partiality
//! is judged over the set the call SCOPED to: with a `server` param naming a
//! server that answered, a skipped OTHER server does not bear on the answer
//! (only a skip of the named one does).
//!
//! The `server` param is a SCOPE, and it is honoured on both halves of an
//! answer:
//! - it filters the RESULT of `search` and `describe` alike — a `describe`
//!   scoped to a server that does not serve the name is a miss scoped to that
//!   server, never another server's tool;
//! - a `server` name the config does not contain is a FAILURE naming that name
//!   (and the servers that do exist), for every scoped path. "The scope named
//!   nothing" must not be reported as the clean negative "the tool is not
//!   there"; it is distinct from "the named server exists but was skipped",
//!   which the issue #68 rule above decides.

use std::time::Duration;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::manager::McpManager;
use super::types::{McpState, ToolCallResult};
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

/// What `collect_tools` gathered: the `(server, tool)` pairs that answered,
/// plus the servers that contributed NOTHING and the reason to show the model
/// (`(name, reason)`).
struct Collected {
    tools: Vec<(String, super::types::ToolInfo)>,
    skipped: Vec<(String, String)>,
}

/// Why a server was skipped — the STATE when the state explains it (the issue's
/// ask: `atlassian (NeedsAuth — use the auth action)`), else the transport
/// error, because a failed STDIO connect never leaves `Error` (it propagates
/// with `?` and the entry stays `Connecting`), so the state alone would say
/// nothing useful.
fn skip_reason(state: &McpState, err: &str) -> String {
    match state {
        McpState::NeedsAuth => "NeedsAuth — use the auth action".to_string(),
        McpState::Error { text } => format!("error: {text}"),
        other => format!("not usable ({other:?}): {err}"),
    }
}

/// The `Skipped servers: …` suffix (`""` when nothing was skipped) — appended
/// to a message that has no trailing period, so the result reads
/// `Tool not found: X. Skipped servers: atlassian (NeedsAuth — use the auth action)`.
fn skipped_note(skipped: &[(String, String)]) -> String {
    if skipped.is_empty() {
        return String::new();
    }
    let parts: Vec<String> = skipped
        .iter()
        .map(|(name, reason)| format!("{name} ({reason})"))
        .collect();
    format!(". Skipped servers: {}", parts.join(", "))
}

/// The "nothing matched" answer: a SUCCESS with the plain `message` when every
/// server answered (the wording is byte-identical to before this rule existed),
/// a FAILURE with the same message + the `Skipped servers: …` note when the
/// answer was computed over a PARTIAL server set (issue #68 — "I searched
/// everywhere and it isn't there" is a lie when a server was skipped).
fn miss_result(message: String, skipped: &[(String, String)]) -> ToolResult {
    if skipped.is_empty() {
        ok(message)
    } else {
        fail(format!("{message}{}", skipped_note(skipped)))
    }
}

/// The `mcp` tool's `search` / `describe` / call-tool tool source: lazily
/// connect every server (the `tools/list` is cached per connection) and
/// collect `(server, tool)` pairs. A server that can't connect (`NeedsAuth` /
/// `Error` — not auto-retried) contributes nothing, and IS reported: the
/// caller must not answer "no such tool" when the answer was computed over a
/// partial server set (issue #68).
async fn collect_tools(manager: &mut McpManager, cancel: &CancellationToken) -> Collected {
    let mut out = Collected {
        tools: Vec::new(),
        skipped: Vec::new(),
    };
    for name in manager.server_names() {
        let outcome = manager.list_tools(&name, cancel, TIMEOUT).await;
        // NOTE: bind the `Result` first — in a `let … else` the scrutinee's
        // mutable borrow of `manager` is still live inside the `else` block, so
        // reading `manager.state_of(&name)` there is a borrow error.
        match outcome {
            Ok(tools) => out
                .tools
                .extend(tools.into_iter().map(|t| (name.clone(), t))),
            Err(err) => {
                let reason = match manager.state_of(&name) {
                    Some(state) => skip_reason(state, &err),
                    // An unknown name (it was just in `server_names()`) — defensive.
                    None => format!("not usable: {err}"),
                };
                out.skipped.push((name, reason));
            }
        }
    }
    out
}

/// The servers the call actually scoped to: the `server` param names ONE, so a
/// skip of any OTHER server is not this call's business; unscoped, every server
/// is in scope and the whole skipped list stands (issue #68 — the partiality is
/// judged over the set the call scoped to, not over every declared server).
fn scoped_skipped(skipped: &[(String, String)], server: Option<&str>) -> Vec<(String, String)> {
    match server {
        None => skipped.to_vec(),
        Some(srv) => skipped
            .iter()
            .filter(|(name, _)| name == srv)
            .cloned()
            .collect(),
    }
}

/// Whether a `(server, tool)` pair is inside the call's `server` scope (no
/// `server` param → every server is in scope).
///
/// `search` and `describe` MUST scope through here, identically: the `server`
/// param narrows the ANSWER, not merely the wording of a failure. A lookup that
/// filters only its FAILURE text by `server` while returning whatever server
/// owns the name would make the `server` param a lie on its success path — it
/// would answer with a tool the caller explicitly excluded.
fn in_scope(server: Option<&str>, server_name: &str) -> bool {
    server.is_none_or(|srv| srv == server_name)
}

/// Reject a `server` param naming a server the manager does not HOLD, as
/// opposed to one it holds and merely could not reach.
///
/// The `server` param can name three different things and the model must be
/// able to tell them apart — this is the same credibility bug class as the
/// `no such tool` of issue #68:
/// - NOT HELD BY THE MANAGER → the scope named NOTHING, so nothing was actually
///   searched: a `No tools matching …` / `Tool not found: …` computed over it
///   would read as "the tool does not exist" and the model would stop, when the
///   real problem is that the scope was empty. This FAILS, naming the unknown
///   name and the servers that ARE held.
/// - HELD BUT UNUSABLE (`NeedsAuth` / `Error` / a failed connect) → it is in
///   [`Collected::skipped`] and the partial-set rule decides, exactly as before.
/// - HELD AND ANSWERING → an ordinary scoped call.
///
/// WHAT THE LIST IS. [`McpManager::server_names`] is the manager's own server
/// map, i.e. the entries `config::load_servers` KEPT after `types::classify_server`
/// — which DROPS an entry before the manager ever sees it when it is
/// `disabled: true`, a legacy `sse` server, or malformed (no `url`, no `command`).
/// So "not in this list" means "not HELD", not "not in the user's JSON": a server
/// the user configured and switched off is reported here as unknown (measured: a
/// `disabled: true` entry answers `Unknown MCP server: switched-off`). That is not
/// a regression — the pre-guard manager failed an unreachable name the same way.
/// A fourth arm saying "configured but disabled" would need the RAW settings layers,
/// which this tool is not given (it gets `&McpManager` only, and the dropped entry
/// is not in it), so there is deliberately no such arm. What is cheap and is done:
/// the requirement clause of the message says the param must name a server the
/// session HOLDS and names the drop reasons (`disabled` / legacy `sse` / malformed),
/// so a user reading it knows to switch the server on rather than invent a name.
/// The candidate LIST keeps its "configured servers" wording (and the empty-config
/// case its "no MCP servers are configured" sentence, pinned) — both name the set
/// the manager holds, which for a healthy config is the config's own set.
///
/// [`Collected::skipped`]: Collected::skipped
fn unknown_server_scope(manager: &McpManager, server: &str) -> Option<ToolResult> {
    let configured = manager.server_names();
    if configured.iter().any(|name| name == server) {
        return None;
    }
    let known = if configured.is_empty() {
        "no MCP servers are configured".to_string()
    } else {
        format!("configured servers: {}", configured.join(", "))
    };
    // The REQUIREMENT clause says "a server this session holds" and names the
    // reasons an entry can be missing from that set, because the list below is
    // what `config::load_servers` KEPT — not the user's JSON — and a clause
    // claiming the name was never CONFIGURED would contradict the user's own
    // settings file for a server they merely switched off. Naming the drop reasons
    // is free (no settings read) and is the actionable half: the fix is to switch
    // the server on, not to invent a name. The candidate list keeps its
    // `configured servers:` wording, which a healthy config makes true.
    Some(fail(format!(
        "Unknown MCP server: {server} ({known}) — the `server` param must name a server this session holds, and an entry the config marks `disabled` (or a legacy `sse` / malformed entry) is not held"
    )))
}

/// The `mcp` proxy tool (the param dispatch — the `@pi-archimedes/mcp`
/// `buildProxyToolExecute` mirrored):
/// - `status` (no `tool`/`search`/`describe`/`connect`/`server`, or
///   `action: "status"`) — the `mcp({})` default: the server status lines.
/// - `connect` — eagerly connect a server (the outcome line).
/// - `action: "auth"` + `server` — run the interactive OAuth flow.
/// - `search` — matching tools, within the `server` scope when one is given
///   (`[server] name — description`).
/// - `describe` — a tool's full `inputSchema` (JSON), within the `server`
///   scope when one is given.
/// - `server` (no `tool`) — that server's tools (name + description, one per
///   line).
/// - `tool` (+ `args` string-or-object) — `tools/call` (the result text = the
///   content joined; `isError` → a failed `ToolResult`). A tool-name
///   disambiguation: an exact raw-name match across servers wins; a tie →
///   the `server` param is required (an error naming the candidates).
/// - an unknown `action` — an error listing the valid ones.
///
/// A `search` / `describe` / bare-`tool` miss while a server IN SCOPE was
/// skipped (`NeedsAuth` / `Error` / a failed connect) is a FAILED result naming
/// the skipped servers; a successful call is never downgraded by an unrelated
/// dead server, and neither is a miss scoped by the `server` param to a server
/// that answered (issue #68).
///
/// A `server` param naming a server the manager does not HOLD FAILS (naming
/// it + the servers it holds) on every scoped path — `search`, `describe`,
/// the server-list path and a `server`-pinned `tool` call — because an answer
/// computed over an empty scope is not an answer. `search` / `describe` filter
/// their ANSWER by `server` too, so a scope never answers with a tool from
/// outside itself.
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

    // ── the `server` scope must name a server the manager HOLDS ──
    //
    // Every path below takes `server` as its SCOPE, so the scope is validated
    // ONCE here: a name the manager does not hold means the scope named
    // NOTHING, and an answer computed over it is not an answer (issue #68's
    // bug class — the model must not read an empty scope as "it doesn't
    // exist"). A held server that is merely unreachable passes here and
    // is reported as a SKIP by `collect_tools`, which is the existing,
    // different rule. Note the asymmetry the guard's doc spells out: an entry
    // `classify_server` DROPPED (`disabled: true`, legacy `sse`, malformed)
    // never reached the manager and so lands on the unknown side. (`connect` /
    // `action: "auth"` returned above; they act ON a server rather than scoping
    // a lookup, and the manager already fails them naming the unknown name.)
    if let Some(name) = &server {
        if let Some(r) = unknown_server_scope(manager, name) {
            return r;
        }
    }

    // ── search ──
    if let Some(q) = &search {
        let all = collect_tools(manager, cancel).await;
        let q_lower = q.to_lowercase();
        let results: Vec<String> = all
            .tools
            .iter()
            .filter(|(s, t)| {
                in_scope(server.as_deref(), s)
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
            // A search computed over a PARTIAL server set is not an answer:
            // it fails and names what was skipped (issue #68) — but only the
            // skips INSIDE the set the call scoped to (a `server` param narrows
            // both the tools and the partiality check).
            return miss_result(
                format!("No tools matching \"{q}\""),
                &scoped_skipped(&all.skipped, server.as_deref()),
            );
        }
        return ok(results.join("\n"));
    }

    // ── describe ──
    if let Some(name) = &describe {
        let all = collect_tools(manager, cancel).await;
        // The `server` scope filters the LOOKUP, not just the failure wording:
        // a describe scoped to a server that does not serve the name is a MISS
        // scoped to that server, never another server's tool. (A describe of a
        // name a DIFFERENT server owns is a miss here too — the caller excluded
        // that server — and the miss keeps the plain `Tool not found:` bytes
        // the scoped rule already promised.)
        let Some((s, t)) = all
            .tools
            .iter()
            .find(|(s, ti)| ti.name == name.as_str() && in_scope(server.as_deref(), s.as_str()))
        else {
            // Nothing matched in scope AND the scoped set was partial → a
            // FAILED result naming the skipped servers (issue #68); a skip
            // OUTSIDE the named `server` does not bear on this call.
            return miss_result(
                format!("Tool not found: {name}"),
                &scoped_skipped(&all.skipped, server.as_deref()),
            );
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
                let all = collect_tools(manager, cancel).await;
                let matches: Vec<String> = all
                    .tools
                    .iter()
                    .filter(|(_, ti)| ti.name == t.as_str())
                    .map(|(s, _)| s.clone())
                    .collect();
                if matches.is_empty() {
                    // Nothing matched AND something was skipped → a FAILED
                    // result naming the skipped servers (issue #68); with every
                    // server reachable the wording is unchanged and stays a
                    // success. This branch only runs UNscoped (a named `server`
                    // resolves above without collecting), so the filter is a
                    // no-op here — applied to keep the rule stated in one
                    // shape: partiality is judged over the scoped set.
                    return miss_result(
                        format!("Tool not found: {t}"),
                        &scoped_skipped(&all.skipped, server.as_deref()),
                    );
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

    /// The `fake_mcp_stdio` test-double binary (built by `cargo test` — the
    /// `CARGO_MANIFEST_DIR`/`target/debug` path).
    fn fake_bin() -> String {
        std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ))
        .display()
        .to_string()
    }

    /// A temp `McpManager` over a temp config (the servers point at the fake
    /// stdio binary — `target/debug/fake_mcp_stdio`).
    fn manager_with(servers_json: &Value) -> (McpManager, tempfile::TempDir) {
        let bin = fake_bin();
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
                serde_json::json!({ "command": bin })
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
        // Compute the credentials path BEFORE `McpManager::new` MOVES `home`,
        // and inject it — no test here can ever read the developer's real
        // `mcp-auth.json` (issue #68).
        let auth_file = home.join("mcp-auth.json");
        let manager = McpManager::new(home, project, None).with_auth_file(auth_file);
        (manager, dir)
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

    // ── issue #68: a skipped server FAILS the call ───────────────────

    /// A `{"dead": {…}}` def pointing at a dead port: NON-OAuth, so no token
    /// logic, so the connect fails instantly and the entry lands in `Error`
    /// (the same loopback-refusal trick as `status_shows_the_error_state`).
    fn dead_server() -> Value {
        serde_json::json!({ "url": "http://127.0.0.1:1/mcp" })
    }

    /// A HEALTHY server that does NOT serve `echo`: the fake binary with its
    /// advertised tool set narrowed to one other tool. The fixture the scoping
    /// tests need — with two healthy servers serving the SAME set, "the scoped
    /// server has no such tool" and "no server has it" are indistinguishable, so
    /// a test could not tell a correctly-scoped miss from an unscoped one.
    fn narrow_server() -> Value {
        serde_json::json!({
            "command": fake_bin(),
            "args": ["--tools=only_here"],
        })
    }

    #[test]
    fn the_skip_reason_names_the_state_and_the_action() {
        assert_eq!(
            skip_reason(&McpState::NeedsAuth, "ignored"),
            "NeedsAuth — use the auth action"
        );
        assert_eq!(
            skip_reason(
                &McpState::Error {
                    text: "boom".to_string()
                },
                "ignored"
            ),
            "error: boom"
        );
        // A failed STDIO connect never leaves `Error` (it propagates with `?`
        // and the entry stays `Connecting`), so the state alone says nothing —
        // the transport error text is what the model needs.
        let reason = skip_reason(&McpState::Connecting, "child exited");
        assert!(reason.contains("not usable"), "{reason}");
        assert!(reason.contains("child exited"), "{reason}");
    }

    #[test]
    fn the_skipped_note_matches_the_issue_example() {
        // The issue's exact sentence.
        assert_eq!(
            format!(
                "Tool not found: X{}",
                skipped_note(&[(
                    "atlassian".to_string(),
                    skip_reason(&McpState::NeedsAuth, "ignored")
                )])
            ),
            "Tool not found: X. Skipped servers: atlassian (NeedsAuth — use the auth action)"
        );
        // Nothing skipped → no suffix at all (the all-healthy wording is
        // byte-identical to before).
        assert_eq!(skipped_note(&[]), "");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_skipped_server_fails_the_bare_call_and_names_it() {
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "nope" }),
            &cancel(),
        )
        .await;
        assert!(
            r.is_error,
            "a partial server set is not a successful 'not found'"
        );
        let t = text(&r);
        assert!(t.contains("Tool not found: nope"), "{t}");
        assert!(t.contains("Skipped servers:"), "{t}");
        assert!(t.contains("dead"), "the skipped server is named: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn describe_fails_and_names_a_skipped_server() {
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "nope" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error, "a describe over a partial server set fails");
        let t = text(&r);
        assert!(t.contains("Tool not found: nope"), "{t}");
        assert!(t.contains("dead"), "the skipped server is named: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn search_fails_and_names_a_skipped_server() {
        // A declared decision: a search computed over a partial server set is
        // not an answer — it fails, keeping the accurate `No tools matching`
        // prefix and naming what was skipped.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "zzz" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error, "a search over a partial server set fails");
        let t = text(&r);
        assert!(t.contains("No tools matching \"zzz\""), "{t}");
        assert!(t.contains("dead"), "the skipped server is named: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_plain_not_found_is_still_a_success_when_every_server_answered() {
        // The all-healthy case is unchanged: the same bytes, still a SUCCESS.
        let (mut manager, _dir) = manager_with(&serde_json::json!({ "a": "fake" }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "nope" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error, "every server answered → a plain not found");
        assert_eq!(text(&r), "Tool not found: nope");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_search_scoped_to_an_answering_server_is_not_poisoned_by_a_dead_sibling() {
        // The finding: `server` scopes the CALL, so it scopes the partiality
        // check. The named server answered → the search is complete over the
        // set the user asked about, so the plain `No tools matching` wording is
        // a SUCCESS (the same bytes as the all-healthy case).
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "zzz", "server": "ok" }),
            &cancel(),
        )
        .await;
        assert!(
            !r.is_error,
            "a scoped search over the one server it scoped to is an answer: {:?}",
            text(&r)
        );
        assert_eq!(text(&r), "No tools matching \"zzz\"");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_describe_scoped_to_an_answering_server_is_not_poisoned_by_a_dead_sibling() {
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "nope", "server": "ok" }),
            &cancel(),
        )
        .await;
        assert!(
            !r.is_error,
            "a scoped describe over the one server it scoped to is an answer: {:?}",
            text(&r)
        );
        assert_eq!(text(&r), "Tool not found: nope");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_search_scoped_to_the_skipped_server_still_fails_and_names_it() {
        // The other half of the rule: when the NAMED server is the one that
        // failed, the scoped set is partial and the call still fails naming it.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "zzz", "server": "dead" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error, "the scoped server itself failed");
        let t = text(&r);
        assert!(t.contains("No tools matching \"zzz\""), "{t}");
        assert!(t.contains("Skipped servers: dead"), "{t}");
        // The healthy sibling answered — it is not a skip, named or not.
        assert!(!t.contains("ok ("), "only the scoped skip is named: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unscoped_search_still_reports_every_skipped_server() {
        // The unscoped call's whole value is naming EVERY dead sibling — the
        // fix must leave those bytes alone.
        let (mut manager, _dir) = manager_with(
            &serde_json::json!({ "ok": "fake", "dead": dead_server(), "dead2": dead_server() }),
        );
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "zzz" }),
            &cancel(),
        )
        .await;
        assert!(
            r.is_error,
            "an unscoped search over a partial server set fails"
        );
        let t = text(&r);
        assert!(t.contains("No tools matching \"zzz\""), "{t}");
        assert!(t.contains("dead"), "the first skip is named: {t}");
        assert!(t.contains("dead2"), "the second skip is named: {t}");
        assert!(
            !t.contains("ok ("),
            "the answering server is not a skip: {t}"
        );
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_call_on_a_healthy_server_is_not_broken_by_a_skipped_one() {
        // The rule must not over-apply: a successful call is never downgraded
        // because an unrelated server is dead.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo", "args": { "text": "hi" } }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error, "a working call stays a success");
        assert_eq!(text(&r), "hi");
        // The same with the server named explicitly: the resolution never
        // consults the dead sibling, so the call is untouched by it.
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "tool": "echo", "server": "ok", "args": { "text": "scoped" } }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error, "a server-scoped call stays a success");
        assert_eq!(text(&r), "scoped");
        manager.close_all();
    }

    // ── the `server` param scopes the ANSWER, not just the failure wording ──

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_describe_scoped_to_a_server_that_lacks_the_tool_is_a_scoped_miss() {
        // `ok` serves `echo`, `narrow` (healthy) does NOT. A describe scoped to
        // `narrow` must answer about `narrow` — the scope the caller named —
        // and never hand back `ok`'s tool. `narrow` ANSWERED, so the scoped miss
        // is the success-shaped "not found" the scoping rule promises.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "narrow": narrow_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "echo", "server": "narrow" }),
            &cancel(),
        )
        .await;
        let t = text(&r);
        assert!(
            !r.is_error,
            "the scoped server answered → a scoped miss, not a partial-set failure: {t}"
        );
        assert_eq!(t, "Tool not found: echo");
        assert!(
            !t.contains("Schema"),
            "another server's tool must not be described into the scope: {t}"
        );
        // The unscoped call still finds it — the tool exists, the SCOPE lacked
        // it (the two facts must stay distinguishable).
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "echo" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        assert!(text(&r).contains("echo (ok)"), "{t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_describe_scoped_to_the_server_that_serves_the_tool_is_unchanged() {
        // The scoping fix must not cost the happy path its bytes.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "narrow": narrow_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "echo", "server": "ok" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error);
        let t = text(&r);
        assert!(t.contains("echo (ok)"), "the tool + server: {t}");
        assert!(t.contains("\"text\""), "the schema's property: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_search_scoped_to_a_never_configured_server_fails_naming_it() {
        // `dead` exists and was skipped; `no-such-server` never existed at all.
        // The second case must NOT read as the clean negative `No tools
        // matching "…"` — the scope named nothing, so nothing was searched.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "echo", "server": "no-such-server" }),
            &cancel(),
        )
        .await;
        assert!(
            r.is_error,
            "a scope that named nothing is not a 'not found'"
        );
        let t = text(&r);
        assert!(t.contains("no-such-server"), "the unknown name: {t}");
        assert!(
            !t.contains("No tools matching"),
            "the clean-negative wording must not be reachable this way: {t}"
        );
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_describe_scoped_to_a_never_configured_server_fails_naming_it() {
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "echo", "server": "no-such-server" }),
            &cancel(),
        )
        .await;
        assert!(
            r.is_error,
            "a scope that named nothing is not a 'not found'"
        );
        let t = text(&r);
        assert!(t.contains("no-such-server"), "the unknown name: {t}");
        assert!(
            !t.contains("Tool not found"),
            "the tool DOES exist — the scope did not: {t}"
        );
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unknown_server_scope_names_the_servers_that_do_exist() {
        // The model's next move is to retry with a real name, so the failure
        // carries the configured names (the authoritative list — the same one
        // `collect_tools` walks).
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "narrow": narrow_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "echo", "server": "typo" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error);
        let t = text(&r);
        assert!(t.contains("Unknown MCP server: typo"), "{t}");
        assert!(t.contains("ok"), "the real names are offered: {t}");
        assert!(t.contains("narrow"), "the real names are offered: {t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_configured_but_unreachable_server_is_never_reported_as_unknown() {
        // THE distinction the guard must not blur: `dead` is in the config (its
        // def is what makes it a server), it simply cannot be reached. It is a
        // SKIP — the issue #68 rule — not an unknown name, and the empty-config
        // case says so in its own words.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "describe": "nope", "server": "dead" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error, "the scoped server itself failed");
        let t = text(&r);
        assert!(
            !t.contains("Unknown MCP server"),
            "a configured server is never 'unknown': {t}"
        );
        assert!(t.contains("Skipped servers: dead"), "{t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_scoped_path_rejects_a_never_configured_server() {
        // The list path and a `server`-pinned call take the SAME scope rule as
        // `search` / `describe` — one vocabulary for "that server does not
        // exist", wherever the model meets it.
        let (mut manager, _dir) =
            manager_with(&serde_json::json!({ "ok": "fake", "dead": dead_server() }));
        for args in [
            serde_json::json!({ "server": "ghost" }),
            serde_json::json!({ "tool": "echo", "server": "ghost", "args": { "text": "x" } }),
        ] {
            let r = mcp_tool(&mut manager, &args, &cancel()).await;
            assert!(r.is_error, "{args} → a failure: {:?}", text(&r));
            let t = text(&r);
            assert!(t.contains("Unknown MCP server: ghost"), "{t}");
            assert!(t.contains("ok"), "the real names are offered: {t}");
        }
        // A CONFIGURED scope is untouched by the guard: the list path keeps its
        // own shape (a server that exists is listed, not rejected).
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "server": "ok" }),
            &cancel(),
        )
        .await;
        assert!(!r.is_error, "a configured scope is untouched");
        assert!(text(&r).contains("echo:"), "{:?}", text(&r));
        // And a configured-but-UNREACHABLE one on the list path keeps the
        // MANAGER's own error text — it exists, so its state is the story, not
        // its name. (The list path is un-collected, so it never reaches the
        // `Skipped servers:` note; this pins that the guard did not swallow it.)
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "server": "dead" }),
            &cancel(),
        )
        .await;
        assert!(
            r.is_error,
            "an unreachable scope still fails, on its merits"
        );
        let t = text(&r);
        assert!(
            !t.contains("Unknown MCP server"),
            "a configured server is never 'unknown': {t}"
        );
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unknown_server_scope_on_an_empty_config_says_so() {
        // No servers at all: the failure still names the scope and states the
        // real reason rather than listing an empty candidate set.
        let (mut manager, _dir) = manager_with(&serde_json::json!({}));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "echo", "server": "anything" }),
            &cancel(),
        )
        .await;
        assert!(r.is_error);
        let t = text(&r);
        assert!(t.contains("Unknown MCP server: anything"), "{t}");
        assert!(t.contains("no MCP servers are configured"), "{t}");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_disabled_server_is_unknown_to_the_manager_and_says_why() {
        // The asymmetry `unknown_server_scope`'s doc states, pinned: an entry the
        // user configured and switched off is DROPPED by `classify_server` before
        // the manager exists, so it is on the unknown side of the guard. The
        // pre-guard manager failed the name the same way — this pins that the
        // message no longer claims the server was never CONFIGURED, which would
        // contradict the user's own settings file, and that it names the drop
        // reasons the manager can see without a settings read.
        let (mut manager, _dir) = manager_with(&serde_json::json!({
            "ok": "fake",
            "switched-off": { "command": fake_bin(), "disabled": true },
        }));
        let r = mcp_tool(
            &mut manager,
            &serde_json::json!({ "search": "echo", "server": "switched-off" }),
            &cancel(),
        )
        .await;
        assert!(
            r.is_error,
            "a scope the manager does not hold names nothing"
        );
        let t = text(&r);
        assert!(t.contains("Unknown MCP server: switched-off"), "{t}");
        assert!(t.contains("ok"), "the held names are still offered: {t}");
        assert!(
            t.contains("servers: ok"),
            "the candidate list keeps its `configured servers:` wording — the doc says why \
             that is acceptable while the requirement clause is not: {t}"
        );
        assert!(
            t.contains("disabled"),
            "and the message says a switched-off entry is not held: {t}"
        );
        manager.close_all();
    }
}

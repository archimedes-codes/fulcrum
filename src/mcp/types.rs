//! The MCP server definition shapes (ADR 0018) — the `mcpServers` entry
//! wire forms the `mcp.json` files carry (the SAME shape pi's built-in MCP
//! reads: an HTTP server = `{ url, headers?, auth?, bearerTokenEnv? }`, a
//! stdio server = `{ command, args?, env?, cwd? }`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A server's authentication (the `auth` field, classified):
/// - `None` — no auth (a local / open server).
/// - `Bearer` — a static `Bearer` token: a literal `token` and/or a
///   `bearerTokenEnv` (an environment-variable NAME — resolved at CONNECT
///   time, not load time).
/// - `OAuth` — OAuth 2.1 (the known `McpOAuthConfig` fields; a `grantType`
///   outside the two known values is dropped, as is an empty string).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthSpec {
    #[default]
    None,
    Bearer {
        token: Option<String>,
        env_var: Option<String>,
    },
    OAuth {
        #[serde(default)]
        grant_type: Option<String>,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        client_secret: Option<String>,
        #[serde(default)]
        scope: Option<String>,
        #[serde(default)]
        redirect_uri: Option<String>,
        #[serde(default)]
        client_name: Option<String>,
        #[serde(default)]
        authorization_server_url: Option<String>,
    },
}

/// An HTTP-based MCP server (the transport is chosen by the def shape:
/// a `url` server connects via streamable HTTP).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpDef {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub auth: AuthSpec,
}

/// A stdio-based MCP server (spawns a child process).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StdioDef {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
}

/// A server definition (an `mcpServers` entry, classified by shape:
/// a `url` = HTTP, a `command` = stdio).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerDef {
    Http(HttpDef),
    Stdio(StdioDef),
}

/// A server's connection state (the manager's per-server state machine):
/// - `Disconnected` — not yet connected (the initial state).
/// - `Connecting` — a connect is in flight.
/// - `Connected` — connected (a client is available).
/// - `NeedsAuth` — an OAuth server that requires a token (the `auth` action
///   is the fix; NOT auto-retried).
/// - `Error { text }` — a failed connect (the `text` is the error; NOT
///   auto-retried — an explicit `connect` is required).
#[derive(Debug, Clone, PartialEq)]
pub enum McpState {
    Disconnected,
    Connecting,
    Connected,
    NeedsAuth,
    Error { text: String },
}

/// A tool advertised by a server (`tools/list`'s `tools[]` entry — the
/// `inputSchema` is the JSON Schema the `mcp` tool's `describe` action
/// shows the model).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolInfo {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

/// A `tools/call` result (the server's `content[]` normalized: the `text`
/// items joined with `\n` + the NON-text items counted — an image /
/// resource item is counted, not inlined; `is_error` from `isError`).
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCallResult {
    pub text: String,
    pub is_error: bool,
    pub non_text_count: u32,
}

/// Extract a `tools/call` result (the shared stdio / HTTP normalization):
/// `content[]` items — `text` joined with `\n`, the other types
/// (`image` / `resource` / …) counted in `non_text_count`; `is_error` from
/// the top-level `isError` (absent = `false`).
pub fn extract_tool_call_result(result: &Value) -> ToolCallResult {
    let mut text = String::new();
    let mut non_text = 0u32;
    if let Some(items) = result.get("content").and_then(Value::as_array) {
        for item in items {
            match item.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(t) = item.get("text").and_then(Value::as_str) {
                        if !text.is_empty() {
                            text.push('\n');
                        }
                        text.push_str(t);
                    }
                }
                Some(_) => non_text += 1,
                None => {} // an item without a type: ignored.
            }
        }
    }
    ToolCallResult {
        text,
        is_error: result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        non_text_count: non_text,
    }
}

/// The known OAuth config fields (the suite's `OAUTH_CONFIG_FIELDS` —
/// an `auth` object carrying ANY of these is an OAuth config; the rest
/// of the object is dropped).
pub const OAUTH_CONFIG_FIELDS: [&str; 7] = [
    "grantType",
    "clientId",
    "clientSecret",
    "scope",
    "redirectUri",
    "clientName",
    "authorizationServerUrl",
];

/// Classify a server's `auth` wire value (the suite's `extractOAuthConfig`
/// + the `Bearer` classification):
/// - `"oauth"` → a default `authorization_code` OAuth config.
/// - `{ token }` (a non-empty string `token`, NO known OAuth field) →
///   `Bearer` (the static-token shape).
/// - an object carrying ≥ 1 known OAuth field (a valid `grantType` ∈
///   `authorization_code` / `client_credentials`; a non-empty string for
///   the rest) → `OAuth` (only the valid known fields — `token` and the
///   unknown fields are dropped).
/// - `null` / any other value → `None`.
pub fn classify_auth(auth: &Value) -> AuthSpec {
    match auth {
        Value::String(s) if s == "oauth" => AuthSpec::OAuth {
            grant_type: Some("authorization_code".into()),
            client_id: None,
            client_secret: None,
            scope: None,
            redirect_uri: None,
            client_name: None,
            authorization_server_url: None,
        },
        Value::Object(map) => {
            // An OAuth config is an object carrying ≥ 1 VALID known
            // OAuth field (a `token` alone is the Bearer shape; an object
            // with only unknown / invalid fields is `None`).
            let mut grant_type: Option<String> = None;
            let mut client_id: Option<String> = None;
            let mut client_secret: Option<String> = None;
            let mut scope: Option<String> = None;
            let mut redirect_uri: Option<String> = None;
            let mut client_name: Option<String> = None;
            let mut authorization_server_url: Option<String> = None;
            let mut any_oauth_field = false;
            for (field, value) in map {
                let Some(s) = value.as_str().filter(|s| !s.is_empty()) else {
                    continue;
                };
                match field.as_str() {
                    "grantType" if s == "authorization_code" || s == "client_credentials" => {
                        grant_type = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    "clientId" => {
                        client_id = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    "clientSecret" => {
                        client_secret = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    "scope" => {
                        scope = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    "redirectUri" => {
                        redirect_uri = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    "clientName" => {
                        client_name = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    "authorizationServerUrl" => {
                        authorization_server_url = Some(s.to_string());
                        any_oauth_field = true;
                    }
                    _ => {} // `token` + unknown fields: not OAuth markers.
                }
            }
            if any_oauth_field {
                return AuthSpec::OAuth {
                    grant_type,
                    client_id,
                    client_secret,
                    scope,
                    redirect_uri,
                    client_name,
                    authorization_server_url,
                };
            }
            match map.get("token").and_then(Value::as_str) {
                Some(t) if !t.is_empty() => AuthSpec::Bearer {
                    token: Some(t.to_string()),
                    env_var: None,
                },
                _ => AuthSpec::None,
            }
        }
        _ => AuthSpec::None,
    }
}

/// A JSON object of string → string (the `headers` / `env` fields — a
/// non-string value drops the entry).
fn string_map(v: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(map) = v.as_object() {
        for (k, value) in map {
            if let Some(s) = value.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

/// Classify ONE `mcpServers` entry (the wire `Value` → a `ServerDef`):
/// - `disabled: true` → `None` (the server is off).
/// - `type: "sse"` (legacy) → `None` (unsupported — pi ≥ 0.99 dropped it).
/// - a `url` (a non-empty string) → `Http` (`headers` a string map, `auth`
///   classified; a `bearerTokenEnv` (a non-empty string) becomes a
///   `Bearer { env_var }` ONLY when `auth` classifies to `None` — the
///   explicit `auth` wins).
/// - a `command` (a non-empty string) → `Stdio` (`args` a string array,
///   `env` a string map, `cwd` a string → `PathBuf`).
/// - neither (or a malformed one) → `None` (a bad entry is skipped, never
///   an error — the best-effort config, ADR 0012 precedent).
pub fn classify_server(entry: &Value) -> Option<ServerDef> {
    let map = entry.as_object()?;
    if map.get("disabled").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    if map.get("type").and_then(Value::as_str) == Some("sse") {
        return None;
    }
    if let Some(url) = map.get("url").and_then(Value::as_str) {
        if !url.is_empty() {
            let auth = classify_auth(map.get("auth").unwrap_or(&Value::Null));
            let auth = if matches!(auth, AuthSpec::None) {
                match map.get("bearerTokenEnv").and_then(Value::as_str) {
                    Some(v) if !v.is_empty() => AuthSpec::Bearer {
                        token: None,
                        env_var: Some(v.to_string()),
                    },
                    _ => AuthSpec::None,
                }
            } else {
                auth
            };
            return Some(ServerDef::Http(HttpDef {
                url: url.to_string(),
                headers: string_map(map.get("headers").unwrap_or(&Value::Null)),
                auth,
            }));
        }
    }
    if let Some(command) = map.get("command").and_then(Value::as_str) {
        if !command.is_empty() {
            let args = map
                .get("args")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            return Some(ServerDef::Stdio(StdioDef {
                command: command.to_string(),
                args,
                env: string_map(map.get("env").unwrap_or(&Value::Null)),
                cwd: map.get("cwd").and_then(Value::as_str).map(PathBuf::from),
            }));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── the auth classification ──────────────────────────────────────

    #[test]
    fn auth_oauth_keyword_is_default_authorization_code() {
        assert_eq!(
            classify_auth(&Value::String("oauth".into())),
            AuthSpec::OAuth {
                grant_type: Some("authorization_code".into()),
                client_id: None,
                client_secret: None,
                scope: None,
                redirect_uri: None,
                client_name: None,
                authorization_server_url: None,
            }
        );
    }

    #[test]
    fn auth_token_object_is_bearer() {
        assert_eq!(
            classify_auth(&serde_json::json!({ "token": "abc" })),
            AuthSpec::Bearer {
                token: Some("abc".into()),
                env_var: None,
            }
        );
    }

    #[test]
    fn auth_oauth_fields_object_is_oauth_with_only_known_fields() {
        assert_eq!(
            classify_auth(&serde_json::json!({
                "clientId": "cid",
                "clientSecret": "sec",
                "scope": "s1 s2",
                "bogusField": "dropped",
                "token": "dropped-too",
            })),
            AuthSpec::OAuth {
                grant_type: None,
                client_id: Some("cid".into()),
                client_secret: Some("sec".into()),
                scope: Some("s1 s2".into()),
                redirect_uri: None,
                client_name: None,
                authorization_server_url: None,
            }
        );
    }

    #[test]
    fn auth_bad_grant_type_drops_the_field_but_keeps_the_config() {
        // A `grantType` outside the two known values is dropped (the
        // config stays an OAuth config — the other fields count).
        assert_eq!(
            classify_auth(&serde_json::json!({
                "grantType": "bogus",
                "clientId": "cid",
            })),
            AuthSpec::OAuth {
                grant_type: None,
                client_id: Some("cid".into()),
                client_secret: None,
                scope: None,
                redirect_uri: None,
                client_name: None,
                authorization_server_url: None,
            }
        );
    }

    #[test]
    fn auth_other_values_are_none() {
        assert_eq!(classify_auth(&Value::Null), AuthSpec::None);
        assert_eq!(
            classify_auth(&Value::String("static-xyz".into())),
            AuthSpec::None
        );
        assert_eq!(classify_auth(&serde_json::json!([1, 2])), AuthSpec::None);
        assert_eq!(
            classify_auth(&serde_json::json!({ "foo": "bar" })),
            AuthSpec::None
        );
    }

    // ── the server-entry classification ──────────────────────────────

    #[test]
    fn http_entry_with_headers_and_bearer_token() {
        let def = classify_server(&serde_json::json!({
            "url": "https://x.example/mcp",
            "headers": { "Authorization": "Bearer k", "X-Bad": 42 },
            "auth": { "token": "tok" },
        }))
        .expect("a url entry classifies");
        assert_eq!(
            def,
            ServerDef::Http(HttpDef {
                url: "https://x.example/mcp".into(),
                headers: {
                    let mut m = BTreeMap::new();
                    m.insert("Authorization".to_string(), "Bearer k".to_string());
                    m
                },
                auth: AuthSpec::Bearer {
                    token: Some("tok".into()),
                    env_var: None,
                },
            })
        );
    }

    #[test]
    fn bearer_token_env_wins_only_when_auth_is_absent() {
        // An explicit `auth` WINS over `bearerTokenEnv`.
        let def = classify_server(&serde_json::json!({
            "url": "https://x.example",
            "auth": { "token": "tok" },
            "bearerTokenEnv": "FOO",
        }))
        .unwrap();
        assert_eq!(
            def,
            ServerDef::Http(HttpDef {
                url: "https://x.example".into(),
                headers: BTreeMap::new(),
                auth: AuthSpec::Bearer {
                    token: Some("tok".into()),
                    env_var: None,
                },
            })
        );
        // No `auth` → the env var.
        let def = classify_server(&serde_json::json!({
            "url": "https://x.example",
            "bearerTokenEnv": "FOO",
        }))
        .unwrap();
        assert_eq!(
            def,
            ServerDef::Http(HttpDef {
                url: "https://x.example".into(),
                headers: BTreeMap::new(),
                auth: AuthSpec::Bearer {
                    token: None,
                    env_var: Some("FOO".into()),
                },
            })
        );
    }

    #[test]
    fn stdio_entry_with_args_env_cwd() {
        let def = classify_server(&serde_json::json!({
            "command": "npx",
            "args": ["-y", "some-server"],
            "env": { "A": "1" },
            "cwd": "/tmp/somewhere",
        }))
        .expect("a command entry classifies");
        assert_eq!(
            def,
            ServerDef::Stdio(StdioDef {
                command: "npx".into(),
                args: vec!["-y".into(), "some-server".into()],
                env: {
                    let mut m = BTreeMap::new();
                    m.insert("A".to_string(), "1".to_string());
                    m
                },
                cwd: Some(PathBuf::from("/tmp/somewhere")),
            })
        );
    }

    #[test]
    fn disabled_and_legacy_sse_entries_are_dropped() {
        assert_eq!(
            classify_server(&serde_json::json!({ "url": "https://x", "disabled": true })),
            None
        );
        assert_eq!(
            classify_server(&serde_json::json!({ "url": "https://x", "type": "sse" })),
            None
        );
        // A malformed entry (neither url nor command) is dropped.
        assert_eq!(classify_server(&serde_json::json!({ "foo": "bar" })), None);
        // An empty-string url is NOT a http def (and no command → dropped).
        assert_eq!(classify_server(&serde_json::json!({ "url": "" })), None);
    }
}

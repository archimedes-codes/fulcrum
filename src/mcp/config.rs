//! The MCP config loading (ADR 0018): the SAME two files pi's built-in
//! MCP reads — `~/.pi/agent/mcp.json` (global) + `<project>/.pi/mcp.json`
//! (project override, highest precedence) — the `mcpServers` shape.
//!
//! Best-effort (the ADR 0018 precedent: the desktop is a read-only
//! CONSUMER of the user's existing pi setup): a missing / unparseable
//! file degrades to "that layer absent" — it never errors, never
//! crashes the session.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use super::types::{classify_server, ServerDef};

/// Read + parse ONE config layer (a missing / unparseable file, or a
/// file with no `mcpServers` object = `None` — the best-effort
/// degradation).
fn read_layer(path: &Path) -> Option<BTreeMap<String, Value>> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    let map = v.get("mcpServers")?.as_object()?;
    Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// Load the effective MCP server definitions: the global layer
/// (`<home>/.pi/agent/mcp.json`) merged with the DESKTOP layer (the
/// `settings.json` `mcpServers`, ADR 0019 — `None` = absent) and the
/// project layer (`<project_cwd>/.pi/mcp.json`). Precedence: project >
/// desktop > global (the most specific layer wins; a desktop entry
/// OVERRIDES a same-named global entry — the ADR 0014 user-wins-on-clash
/// pattern). `disabled` / legacy-`sse` / malformed entries are dropped (the
/// entry classification, `types::classify_server`).
pub fn load_servers(
    home: &Path,
    project_cwd: &Path,
    desktop: Option<&HashMap<String, Value>>,
) -> BTreeMap<String, ServerDef> {
    let mut out: BTreeMap<String, ServerDef> = BTreeMap::new();
    let layers: Vec<BTreeMap<String, Value>> = vec![
        read_layer(&home.join(".pi/agent/mcp.json")),
        desktop.map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        read_layer(&project_cwd.join(".pi/mcp.json")),
    ]
    .into_iter()
    .flatten()
    .collect();
    for layer in layers {
        for (name, entry) in layer {
            if let Some(def) = classify_server(&entry) {
                out.insert(name, def);
            }
        }
    }
    out
}

/// One effective MCP server for the `#`-mention picker (a config read
/// only — NO live connect; the per-session `McpManager` rule,
/// ADR 0018/0019, is untouched).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerInfo {
    pub name: String,
    /// `"http" | "stdio"` (the `ServerDef` classification).
    pub kind: String,
    /// HTTP: the `url`. Stdio: `command` + `args` joined with a space
    /// (e.g. `npx -y x-mcp`; the command alone when there are no args).
    /// NOT length-bounded here: these come from a possibly CLONED repo's
    /// `<cwd>/.pi/mcp.json`, so the injection point caps them
    /// (`capInterpolated` in `src/lib/skills.ts`, the agent description's
    /// 1024-code-point budget) — otherwise one `#name` pick would flood the
    /// prompt AND the persisted message.
    pub summary: String,
}

/// Map the effective set (`load_servers`' output) to the picker shape.
/// Deterministic (a `BTreeMap` input → name-sorted output).
///
/// The `#`-MENTION surface: entries whose name can never equal a mention
/// token (`crate::skills::is_mentionable_name`, which is strictly stronger
/// than tag-safety) are DROPPED here — the one place besides
/// `commands::agents::list_agent_definitions_for_space` where a name reaches
/// the picker. The name is interpolated UNESCAPED into `<mcp name="…">` by
/// `buildMcpBlock`, and `expandMentions` looks it up by
/// `name.toLowerCase() === token` with a `[a-z0-9-]` token, so such a name
/// can NEVER be expanded: offering it would insert a dead token. (The
/// grammar check here is ASCII-only while the TS lookup case-FOLDS, so a
/// name carrying U+212A is hidden here though TS would match it — an
/// accepted LOST-FEATURE divergence, see `crate::skills::is_mentionable_name`.)
/// Filtering here (and NOT in `load_servers`) is deliberate — the session
/// layer keeps connecting to whatever the user configured.
pub fn server_infos(servers: &BTreeMap<String, ServerDef>) -> Vec<McpServerInfo> {
    servers
        .iter()
        .filter(|(name, _)| crate::skills::is_mentionable_name(name))
        .map(|(name, def)| {
            let (kind, summary) = match def {
                ServerDef::Http(h) => ("http".to_string(), h.url.clone()),
                ServerDef::Stdio(s) => (
                    "stdio".to_string(),
                    if s.args.is_empty() {
                        s.command.clone()
                    } else {
                        // A SPACE separator between the command and the
                        // args — the `+` operator concatenates with NO
                        // separator ("npx" + "-y x-mcp" would be
                        // "npx-y x-mcp").
                        format!("{} {}", s.command, s.args.join(" "))
                    },
                ),
            };
            McpServerInfo {
                name: name.clone(),
                kind,
                summary,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::mcp::types::{AuthSpec, HttpDef, StdioDef};
    use std::io::Write;
    use std::path::PathBuf;

    /// Write a file (creating parents).
    fn write_file(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(text.as_bytes()).unwrap();
    }

    /// A temp dir (a fresh one per call).
    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("mcp-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn home_with_mcp_json(mcp_json: &str) -> PathBuf {
        let home = temp_dir("home");
        write_file(&home.join(".pi/agent/mcp.json"), mcp_json);
        home
    }

    #[test]
    fn load_servers_global_only() {
        let home = home_with_mcp_json(
            r#"{ "mcpServers": { "a": { "url": "https://a" }, "b": { "command": "x" } } }"#,
        );
        let project = temp_dir("project");
        let servers = load_servers(&home, &project, None);
        assert_eq!(servers.len(), 2);
        assert!(matches!(servers.get("a"), Some(ServerDef::Http(_))));
        assert!(matches!(servers.get("b"), Some(ServerDef::Stdio(_))));
    }

    #[test]
    fn load_servers_project_overrides_global_by_name() {
        let home = home_with_mcp_json(
            r#"{ "mcpServers": { "a": { "url": "https://global" }, "g": { "url": "https://g" } } }"#,
        );
        let project = temp_dir("project");
        write_file(
            &project.join(".pi/mcp.json"),
            r#"{ "mcpServers": { "a": { "command": "proj" } } }"#,
        );
        let servers = load_servers(&home, &project, None);
        // `a` is the PROJECT's stdio def (the override); `g` survives.
        assert!(matches!(servers.get("a"), Some(ServerDef::Stdio(_))));
        assert_eq!(servers.len(), 2);
    }

    #[test]
    fn load_servers_desktop_overrides_global_by_name() {
        // (ADR 0019) The desktop layer sits BETWEEN global and project:
        // a desktop entry OVERRIDES a same-named global entry (the ADR
        // 0014 user-wins-on-clash pattern).
        let home = home_with_mcp_json(
            r#"{ "mcpServers": { "a": { "url": "https://global" }, "g": { "url": "https://g" } } }"#,
        );
        let project = temp_dir("project");
        let desktop = std::collections::HashMap::from([
            ("a".to_string(), serde_json::json!({ "command": "desktop" })),
            ("d".to_string(), serde_json::json!({ "url": "https://d" })),
        ]);
        let servers = load_servers(&home, &project, Some(&desktop));
        // `a` is the DESKTOP's stdio def (the override); `g` + `d` survive.
        assert!(matches!(servers.get("a"), Some(ServerDef::Stdio(_))));
        assert_eq!(servers.len(), 3);
    }

    #[test]
    fn load_servers_project_overrides_desktop_by_name() {
        // (ADR 0019) Precedence: project > desktop > global — the project
        // layer (the most specific) still wins over a desktop entry.
        let home = temp_dir("home"); // no global file
        let project = temp_dir("project");
        write_file(
            &project.join(".pi/mcp.json"),
            r#"{ "mcpServers": { "a": { "url": "https://project" } } }"#,
        );
        let desktop = std::collections::HashMap::from([(
            "a".to_string(),
            serde_json::json!({ "command": "desktop" }),
        )]);
        let servers = load_servers(&home, &project, Some(&desktop));
        // `a` is the PROJECT's http def (it beats the desktop's stdio def).
        assert!(matches!(servers.get("a"), Some(ServerDef::Http(_))));
    }

    #[test]
    fn load_servers_desktop_disabled_entries_are_dropped() {
        let home = home_with_mcp_json(r#"{ "mcpServers": { "a": { "url": "https://a" } } }"#);
        let project = temp_dir("project");
        let desktop = std::collections::HashMap::from([
            (
                "a".to_string(),
                serde_json::json!({ "url": "https://d-a", "disabled": true }),
            ),
            ("b".to_string(), serde_json::json!({ "url": "https://d-b" })),
        ]);
        let servers = load_servers(&home, &project, Some(&desktop));
        // The disabled desktop entry is dropped — the GLOBAL `a` shows
        // through (a disabled entry is "off", not "shadow"); `b` survives.
        assert!(matches!(servers.get("a"), Some(ServerDef::Http(_))));
        assert_eq!(servers.len(), 2);
    }

    #[test]
    fn load_servers_a_corrupt_project_file_degrades_to_global() {
        let home = home_with_mcp_json(r#"{ "mcpServers": { "a": { "url": "https://a" } } }"#);
        let project = temp_dir("project");
        write_file(&project.join(".pi/mcp.json"), "{ not json");
        let servers = load_servers(&home, &project, None);
        // The corrupt project layer is absent — the global layer loads.
        assert_eq!(servers.len(), 1);
        assert!(matches!(servers.get("a"), Some(ServerDef::Http(_))));
    }

    #[test]
    fn load_servers_no_files_is_empty() {
        let home = temp_dir("home"); // no mcp.json
        let project = temp_dir("project"); // no mcp.json
        assert!(load_servers(&home, &project, None).is_empty());
    }

    #[test]
    fn server_infos_maps_http_to_url_summary() {
        let mut servers = BTreeMap::new();
        servers.insert(
            "a".to_string(),
            ServerDef::Http(HttpDef {
                url: "https://a".into(),
                headers: Default::default(),
                auth: AuthSpec::None,
            }),
        );
        let infos = server_infos(&servers);
        assert_eq!(
            infos,
            vec![McpServerInfo {
                name: "a".into(),
                kind: "http".into(),
                summary: "https://a".into(),
            }]
        );
    }

    #[test]
    fn server_infos_maps_stdio_to_command_plus_args_summary() {
        let mut servers = BTreeMap::new();
        servers.insert(
            "s".to_string(),
            ServerDef::Stdio(StdioDef {
                command: "npx".into(),
                args: vec!["-y".into(), "x-mcp".into()],
                env: Default::default(),
                cwd: None,
            }),
        );
        let infos = server_infos(&servers);
        assert_eq!(infos.len(), 1);
        // A SPACE separator between the command and the args.
        assert_eq!(infos[0].summary, "npx -y x-mcp");
        assert_eq!(infos[0].kind, "stdio");
        assert_eq!(infos[0].name, "s");
        // No args: the command alone.
        let mut bare = BTreeMap::new();
        bare.insert(
            "b".to_string(),
            ServerDef::Stdio(StdioDef {
                command: "solo".into(),
                args: vec![],
                env: Default::default(),
                cwd: None,
            }),
        );
        let infos = server_infos(&bare);
        assert_eq!(infos[0].summary, "solo");
    }

    #[test]
    fn server_infos_is_name_sorted() {
        let mut servers = BTreeMap::new();
        // Insert out of order — the `BTreeMap` keeps them name-sorted.
        servers.insert(
            "b".to_string(),
            ServerDef::Http(HttpDef {
                url: "https://b".into(),
                headers: Default::default(),
                auth: AuthSpec::None,
            }),
        );
        servers.insert(
            "a".to_string(),
            ServerDef::Http(HttpDef {
                url: "https://a".into(),
                headers: Default::default(),
                auth: AuthSpec::None,
            }),
        );
        let infos = server_infos(&servers);
        let names: Vec<&str> = infos.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn server_infos_drops_names_that_can_never_match_a_mention_token() {
        // A3 — the picker round-trip constraint. A name that can never equal a
        // mention token (`skills.ts` looks a token up by
        // `name.toLowerCase() === token`, and a token is `[a-z0-9-]`) is
        // UNNAMEABLE from the composer: offering it would insert a dead token
        // that never expands. The server name is a raw JSON object key
        // (`mcp.json` / the desktop `settings.json` / a CLONED repo's
        // `<cwd>/.pi/mcp.json`), so it is attacker-controlled text — the same
        // `"` that `skills.rs` skips for `<skill name="…">` would break
        // `<mcp name="…">`.
        let mut servers = BTreeMap::new();
        for name in [
            "good",
            "Mixed",          // nameable: matching is case-insensitive
            "x\" onload=\"y", // a `"` → breaks the `<mcp name="…">` attribute
            "has space",      // a token has no whitespace → never matches
            "tab\there",      // a control character
        ] {
            servers.insert(
                name.to_string(),
                ServerDef::Http(HttpDef {
                    url: "https://x".into(),
                    headers: Default::default(),
                    auth: AuthSpec::None,
                }),
            );
        }
        let infos = server_infos(&servers);
        let names: Vec<&str> = infos.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(names, vec!["Mixed", "good"]);
    }

    #[test]
    fn load_servers_keeps_unmentionable_names_for_the_session_layer() {
        // The filter is at the MENTION surface ONLY: `load_servers` (what the
        // session actually connects to) must keep serving an oddly-named
        // entry — dropping it there would silently disable a user's server.
        let home =
            home_with_mcp_json(r#"{ "mcpServers": { "has space": { "url": "https://a" } } }"#);
        let project = temp_dir("project");
        let servers = load_servers(&home, &project, None);
        assert!(servers.contains_key("has space"));
        assert!(server_infos(&servers).is_empty());
    }

    #[test]
    fn load_servers_disabled_entries_are_dropped() {
        let home = home_with_mcp_json(
            r#"{ "mcpServers": { "a": { "url": "https://a", "disabled": true }, "b": { "url": "https://b" } } }"#,
        );
        let project = temp_dir("project");
        let servers = load_servers(&home, &project, None);
        assert_eq!(servers.len(), 1);
        assert!(!servers.contains_key("a"));
        assert!(servers.contains_key("b"));
    }
}

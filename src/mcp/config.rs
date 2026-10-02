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

#[cfg(test)]
mod tests {
    use super::*;
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

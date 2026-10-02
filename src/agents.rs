//! Agent-definition discovery (ADR 0020 — the desktop owns the agent
//! layer).
//!
//! Finds and parses flat `*.md` Agent definition files under a bounded
//! set of roots (pi's agent layout: `*.md` directly in the root, no
//! subdir walk) and parses their YAML frontmatter. Mirrors `skills.rs`
//! (ADR 0013) structurally: the same `space_roots` walk-up, the same
//! `home_dir` reuse, the same first-wins dedupe and deterministic sort.
//!
//! Pure (fs reads only) and total: `discover_agents` never panics and
//! never errors — a missing root, an unreadable file, or malformed
//! frontmatter is silently skipped. Frontmatter is parsed by a
//! hand-rolled tolerant reader (no new crate dependencies): an agent
//! that cannot be parsed is skipped, never a crash.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentScope {
    Space,
    User,
}

impl std::fmt::Display for AgentScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AgentScope::Space => "space",
            AgentScope::User => "user",
        })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentDefinition {
    /// Frontmatter `name`, else the file's STEM name (`scout.md` → `scout`).
    pub name: String,
    /// Frontmatter `description`, else `""`.
    pub description: String,
    /// Frontmatter `model` (`"provider/id"` or `"provider/id:<level>"`), verbatim.
    pub model: Option<String>,
    /// Frontmatter `thinking` (a native extension beyond pi's format).
    pub thinking: Option<String>,
    /// Frontmatter `tools` (both the `read, bash` string and the `[read, bash]`
    /// array spellings normalize to this).
    pub tools: Option<Vec<String>>,
    /// The file body (minus the frontmatter), trimmed. `""` when the body is
    /// empty (no system prompt).
    pub system_prompt: String,
    pub scope: AgentScope,
    /// Absolute (canonical) path to the file.
    pub path: String,
}

/// Discover Agent definitions for `space_path` (its roots + the user-level
/// roots). `space_path: None` → user-level definitions only.
pub fn discover_agents(space_path: Option<&Path>) -> Vec<AgentDefinition> {
    let mut roots: Vec<(PathBuf, AgentScope)> = Vec::new();
    if let Some(p) = space_path {
        roots.extend(space_roots(p));
    }
    roots.extend(user_roots());
    discover_in_roots(&roots)
}

/// The core, testable without env manipulation: discover over EXPLICIT
/// roots. `roots` order defines first-wins dedupe precedence.
pub fn discover_in_roots(roots: &[(PathBuf, AgentScope)]) -> Vec<AgentDefinition> {
    // Shared across ALL roots: a symlink in a later root pointing at an
    // earlier root's file collapses by PATH, not just by name.
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut by_name: HashMap<String, AgentDefinition> = HashMap::new();

    for (root, scope) in roots {
        if !root.is_dir() {
            continue;
        }
        let Ok(entries) = fs::read_dir(root) else {
            continue;
        };
        // Collect the qualifying entries (a `*.md` file or symlink) and
        // SORT THEM BY FILE NAME: `read_dir` order is OS-dependent, and
        // the final `sort_by_cached_key` sorts the OUTPUT, not the
        // insertion order — a same-root case-colliding pair (`a.md` +
        // `A.md`) must dedupe deterministically.
        let mut files: Vec<(String, PathBuf)> = Vec::new();
        for entry in entries.flatten() {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if !file_name.ends_with(".md") {
                continue;
            }
            let is_file_or_symlink = entry.file_type().map(|t| t.is_file() || t.is_symlink());
            if !is_file_or_symlink.unwrap_or(false) {
                continue;
            }
            files.push((file_name, entry.path()));
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        for (file_name, path) in files {
            let Ok(canon) = fs::canonicalize(&path) else {
                continue;
            };
            if !seen.insert(canon.clone()) {
                continue;
            }
            let Ok(content) = fs::read_to_string(&canon) else {
                continue;
            };
            // `file_name` ends in `.md` (3 ASCII bytes), so the byte
            // offset is always on a char boundary.
            let stem = file_name[..file_name.len() - 3].to_string();
            let Some((name, description, model, thinking, tools, system_prompt)) =
                parse_agent_file(&content, &stem)
            else {
                continue;
            };
            // First-wins across the whole run (a Space root shadows a
            // same-named user agent; an innermost ancestor shadows an
            // outer one).
            let key = name.to_lowercase();
            if by_name.contains_key(&key) {
                continue;
            }
            by_name.insert(
                key,
                AgentDefinition {
                    name,
                    description,
                    model,
                    thinking,
                    tools,
                    system_prompt,
                    scope: *scope,
                    path: canon.to_string_lossy().into_owned(),
                },
            );
        }
    }

    let mut out: Vec<AgentDefinition> = by_name.into_values().collect();
    // `sort_by_cached_key`: a single lowercased allocation per element
    // (NOT per comparison), with the `path` as a deterministic tie-break.
    out.sort_by_cached_key(|d| (d.name.to_lowercase(), d.path.clone()));
    out
}

/// Walk UP from `space_path` to the repository root: the first ancestor
/// (including `space_path` itself) that contains a `.git` entry is the
/// repo root; if none exists, `space_path` alone is the only level. At
/// each level (innermost first) collect `<level>/.agents/agents` (first)
/// and `<level>/.pi/agents` (second), both tagged `AgentScope::Space`.
///
/// Do NOT canonicalize the input before walking (the caller passes a
/// canonical path; if `space_path` doesn't exist, return an empty vec).
pub fn space_roots(space_path: &Path) -> Vec<(PathBuf, AgentScope)> {
    if !space_path.exists() {
        return Vec::new();
    }
    // Find the repo root: the first ancestor (including `space_path`
    // itself) that contains a `.git` entry; if none exists, only
    // `space_path` is a level.
    let mut repo_root: Option<PathBuf> = None;
    let mut cur = space_path.to_path_buf();
    loop {
        if cur.join(".git").exists() {
            repo_root = Some(cur.clone());
            break;
        }
        match cur.parent() {
            Some(p) if p != cur => {
                cur = p.to_path_buf();
            }
            _ => break,
        }
    }
    // Levels, innermost (the space itself) first, up to and including
    // the repo root.
    let mut levels = vec![space_path.to_path_buf()];
    if let Some(root) = repo_root {
        let mut cur = space_path.to_path_buf();
        while cur != root {
            let next = cur.parent().unwrap().to_path_buf();
            levels.push(next.clone());
            cur = next;
        }
    }
    let mut out = Vec::new();
    for level in &levels {
        out.push((level.join(".agents/agents"), AgentScope::Space));
        out.push((level.join(".pi/agents"), AgentScope::Space));
    }
    out
}

/// `home_dir().join(".agents/agents")` (first) and
/// `home_dir().join(".pi/agent/agents")` (second), both tagged
/// `AgentScope::User`.
pub fn user_roots() -> Vec<(PathBuf, AgentScope)> {
    let Some(home) = crate::skills::home_dir() else {
        return Vec::new();
    };
    vec![
        (home.join(".agents/agents"), AgentScope::User),
        (home.join(".pi/agent/agents"), AgentScope::User),
    ]
}

/// The tolerant frontmatter reader (no YAML crate): any failure mode
/// (missing closing `---`, …) → `None` (the agent is skipped, never a
/// crash).
///
/// Returns `(name, description, model, thinking, tools, body)`.
#[allow(clippy::type_complexity)] // the 6-tuple IS the spec's signature
fn parse_agent_file(
    content: &str,
    fallback_name: &str,
) -> Option<(
    String,
    String,
    Option<String>,
    Option<String>,
    Option<Vec<String>>,
    String,
)> {
    // 1. Normalize `\r\n` and lone `\r` to `\n`.
    let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();

    // 2. Frontmatter = a block starting at the very beginning: line 1
    //    is exactly `---`, then lines until a line that is exactly
    //    `---`. If line 1 is not `---`: no frontmatter → name =
    //    fallback (the stem), description = "", all Option fields =
    //    None, body = the whole content (trimmed).
    if lines.first() != Some(&"---") {
        return Some((
            fallback_name.to_string(),
            String::new(),
            None,
            None,
            None,
            normalized.trim().to_string(),
        ));
    }
    // The closing `---` is REQUIRED — EOF before it means the
    // frontmatter is malformed → None.
    let close = lines[1..].iter().position(|l| *l == "---")?;
    let fm_end = 1 + close;
    let fm: &[&str] = &lines[1..fm_end];
    let body = lines[fm_end + 1..].join("\n").trim().to_string();

    // 3. Top-level (non-indented) keys only, first occurrence of each
    //    wins (indented lines belong to the previous key's block value;
    //    any other top-level key is ignored).
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;
    let mut model: Option<String> = None;
    let mut thinking: Option<String> = None;
    let mut tools: Option<String> = None;
    let mut i = 0;
    while i < fm.len() {
        let line = fm[i];
        if !line.starts_with(' ') && !line.starts_with('\t') {
            if name.is_none() {
                if let Some(rest) = line.strip_prefix("name:") {
                    name = parse_value(fm, i, rest);
                }
            }
            if description.is_none() {
                if let Some(rest) = line.strip_prefix("description:") {
                    description = parse_value(fm, i, rest);
                }
            }
            if model.is_none() {
                if let Some(rest) = line.strip_prefix("model:") {
                    model = parse_value(fm, i, rest);
                }
            }
            if thinking.is_none() {
                if let Some(rest) = line.strip_prefix("thinking:") {
                    thinking = parse_value(fm, i, rest);
                }
            }
            if tools.is_none() {
                if let Some(rest) = line.strip_prefix("tools:") {
                    tools = parse_value(fm, i, rest);
                }
            }
        }
        i += 1;
    }

    // 4. `name` empty or whitespace-only after parsing (an `""` value,
    //    a `"   "` value, or a block with no indented lines) → fallback
    //    (the stem). NO tag-safety skip (unlike skills: an agent name is
    //    never interpolated unescaped into a prompt tag — it appears only
    //    in the `list_agents` tool RESULT, which the wire JSON-escapes).
    let name = name
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| fallback_name.to_string());
    // `description` left as parsed (may be `""`). NO 1024-char cap
    // (unlike skills: a description is not prompt-injected — it appears
    //    only in `list_agents` output on demand).
    let description = description.unwrap_or_default();
    // `model` / `thinking` empty after parsing → None.
    let model = model.filter(|m| !m.trim().is_empty());
    let thinking = thinking.filter(|t| !t.trim().is_empty());
    // `tools`: normalize the string / array spellings; a list that
    // empties out → None.
    let tools = tools.as_deref().and_then(parse_tools);

    // 5. `body` = the content after the closing `---` (trimmed).
    Some((name, description, model, thinking, tools, body))
}

/// Normalize a parsed `tools` value to a list (or `None`): `trim()`; if
/// it starts with `[` AND ends with `]` → strip both brackets and split
/// on `,`; else split on `,` directly; trim each element; drop empty
/// elements; an empty result → `None`.
fn parse_tools(raw: &str) -> Option<Vec<String>> {
    let raw = raw.trim();
    let inner = raw.strip_prefix('[').and_then(|s| s.strip_suffix(']'));
    let source = inner.unwrap_or(raw);
    let items: Vec<String> = source
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if items.is_empty() {
        None
    } else {
        Some(items)
    }
}

/// Parse the value of a top-level `key: <rest>` line (step 3).
///
/// (A copy of `skills.rs`'s `parse_value` — deliberate per ADR 0020
/// (docs/decisions/0020-native-subagent-agent-definitions.md): "do NOT
/// change `skills.rs`'s visibility — the two frontmatter shapes differ,
/// so the modules stay independent". `parse_value` itself is
/// shape-agnostic: if a bug is found in the value-reading logic (e.g. the
/// NBSP/dedent byte-offset guard documented below), it must be applied to
/// BOTH copies.)
///
/// - inline: the rest of the line, trimmed; strip one pair of matching
///   surrounding quotes; an empty inline value means "no value".
/// - block: the inline value is exactly `|` or `>` (optionally followed
///   by ONE chomping/indent indicator — `-` or `+ — with any further
///   trailing whitespace: the value is the following indented lines
///   until the next non-indented line; `|` joins with `\n`, `>` joins
///   with a single space; the result is dedented and trimmed.
/// - an unquoted inline value containing `: ` (a YAML mapping
///   ambiguity) → no value (tolerant, no crash).
fn parse_value(fm: &[&str], idx: usize, rest: &str) -> Option<String> {
    let inline = rest.trim();
    if inline.is_empty() {
        return None;
    }

    let (style, is_block) = match inline {
        "|" => ('|', true),
        "|-" => ('|', true),
        "|+" => ('|', true),
        ">" => ('>', true),
        ">-" => ('>', true),
        ">+" => ('>', true),
        _ => ('\0', false),
    };
    if is_block {
        let mut collected = Vec::new();
        let mut j = idx + 1;
        while j < fm.len() {
            let l = fm[j];
            if l.starts_with(' ') || l.starts_with('\t') {
                collected.push(l);
                j += 1;
            } else {
                break;
            }
        }
        if collected.is_empty() {
            return Some(String::new());
        }
        // Dedent: strip the common leading whitespace (YAML block
        // semantics — the block's indent is not part of the value).
        // ASCII spaces/tabs ONLY: `trim_start` would also trim UNICODE
        // whitespace (e.g. U+00A0 NBSP, 2 bytes), making the byte offset
        // land mid-character and the `&l[min..]` slice panic — but the
        // discovery must be total (never panic). NBSP is content, not
        // indent.
        let min: usize = collected
            .iter()
            .map(|l| l.len() - l.trim_start_matches([' ', '\t']).len())
            .min()
            .unwrap_or(0);
        let dedented: Vec<&str> = collected.iter().map(|l| &l[min..]).collect();
        let joined = if style == '|' {
            dedented.join("\n")
        } else {
            dedented.join(" ")
        };
        return Some(joined.trim().to_string());
    }

    // Strip one pair of matching surrounding quotes.
    let first = inline.chars().next().unwrap();
    let last = inline.chars().next_back().unwrap();
    if (first == '"' || first == '\'') && first == last && inline.len() >= 2 {
        return Some(inline[1..inline.len() - 1].to_string());
    }
    // Unquoted inline containing `: ` (a YAML mapping ambiguity) → no
    // value (tolerant, no crash).
    if inline.contains(": ") {
        return None;
    }
    Some(inline.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// A scratch root with an EMPTY `.git/` dir so the context walk is
    /// bounded at the scratch (a stray `~/.agents/agents` or `~/.pi`
    /// from a developer's real home must not break the assertions).
    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("archimedes-agents-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::create_dir_all(dir.join(".git")).unwrap();
        dir
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    /// Canonicalize for path comparisons (on macOS `std::env::temp_dir()`
    /// is a symlinked path). Use it ONLY on paths that EXIST — the
    /// fallback to the raw path is a silent no-op otherwise (the root
    /// tests compare raw joined paths instead, as `tests/skills.rs` does).
    fn canon(p: &Path) -> PathBuf {
        fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
    }

    // 1
    #[test]
    fn a_flat_agent_file_is_discovered_with_all_fields() {
        let root = scratch();
        write_file(
            &root.join("scout.md"),
            "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: read, bash\n---\nYou are a scout.\n",
        );
        let out = discover_in_roots(&[(root.clone(), AgentScope::Space)]);
        assert_eq!(out.len(), 1, "one agent expected: {out:?}");
        let d = &out[0];
        assert_eq!(d.name, "scout");
        assert_eq!(d.description, "Fast recon.");
        assert_eq!(d.model.as_deref(), Some("fake/m2"));
        assert_eq!(d.thinking.as_deref(), Some("low"));
        assert_eq!(d.tools, Some(vec!["read".to_string(), "bash".to_string()]));
        assert_eq!(d.system_prompt, "You are a scout.");
        assert_eq!(d.scope, AgentScope::Space);
        assert_eq!(
            d.path,
            canon(&root.join("scout.md")).to_string_lossy().into_owned()
        );
    }

    // 2
    #[test]
    fn a_file_without_frontmatter_uses_the_stem_name() {
        let root = scratch();
        write_file(&root.join("worker.md"), "Do the work.\n");
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(out.len(), 1, "one agent expected: {out:?}");
        assert_eq!(out[0].name, "worker");
        assert_eq!(out[0].description, "");
        assert_eq!(out[0].model, None);
        assert_eq!(out[0].thinking, None);
        assert_eq!(out[0].tools, None);
        assert_eq!(out[0].system_prompt, "Do the work.");
    }

    // 3
    #[test]
    fn a_missing_name_key_falls_back_to_the_stem() {
        let root = scratch();
        write_file(
            &root.join("helper.md"),
            "---\ndescription: A helper.\n---\nHelp.\n",
        );
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(out.len(), 1, "one agent expected: {out:?}");
        assert_eq!(out[0].name, "helper");
        assert_eq!(out[0].description, "A helper.");
        assert_eq!(out[0].system_prompt, "Help.");
    }

    // 4
    #[test]
    fn a_malformed_frontmatter_is_skipped() {
        let root = scratch();
        // An opening `---` with no closing `---` (EOF before it).
        write_file(
            &root.join("broken.md"),
            "---\nname: broken\nmodel: fake/m1\n",
        );
        // A positive control: a well-formed file in the same root.
        write_file(&root.join("good.md"), "---\nname: good\n---\nB.\n");
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(
            out.len(),
            1,
            "only the well-formed file is discovered: {out:?}"
        );
        assert_eq!(out[0].name, "good");
    }

    // 5
    #[test]
    fn tools_accepts_string_and_array_spellings() {
        let root = scratch();
        write_file(
            &root.join("s1.md"),
            "---\nname: s1\ntools: read, bash\n---\nB1.\n",
        );
        write_file(
            &root.join("s2.md"),
            "---\nname: s2\ntools: [read, bash]\n---\nB2.\n",
        );
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(out.len(), 2, "two agents expected: {out:?}");
        let s1 = out.iter().find(|d| d.name == "s1").unwrap();
        let s2 = out.iter().find(|d| d.name == "s2").unwrap();
        assert_eq!(s1.tools, Some(vec!["read".to_string(), "bash".to_string()]));
        assert_eq!(s2.tools, Some(vec!["read".to_string(), "bash".to_string()]));
    }

    // 6
    #[test]
    fn tools_empty_list_is_none() {
        let root = scratch();
        write_file(&root.join("e1.md"), "---\nname: e1\ntools: []\n---\nB.\n");
        write_file(&root.join("e2.md"), "---\nname: e2\ntools: \"\"\n---\nB.\n");
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(out.len(), 2, "two agents expected: {out:?}");
        let e1 = out.iter().find(|d| d.name == "e1").unwrap();
        let e2 = out.iter().find(|d| d.name == "e2").unwrap();
        assert_eq!(e1.tools, None, "`[]` must normalize to None");
        assert_eq!(
            e2.tools, None,
            "an empty string value must normalize to None"
        );
    }

    // 7
    #[test]
    fn a_missing_root_yields_an_empty_vec() {
        let root = scratch();
        let out = discover_in_roots(&[(root.join("missing"), AgentScope::User)]);
        assert!(out.is_empty());
    }

    // 8
    #[test]
    fn a_non_md_file_is_ignored() {
        let root = scratch();
        write_file(&root.join("notes.txt"), "not an agent definition");
        // A positive control: a well-formed `.md` file in the same root.
        write_file(&root.join("good.md"), "---\nname: good\n---\nB.\n");
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(out.len(), 1, "only the `.md` file is discovered: {out:?}");
        assert_eq!(out[0].name, "good");
    }

    // 9
    #[test]
    fn first_wins_dedupe_space_shadows_user() {
        let space_root = scratch();
        let user_root = scratch();
        write_file(
            &space_root.join("scout.md"),
            "---\nname: scout\ndescription: space version\n---\nS.\n",
        );
        write_file(
            &user_root.join("scout.md"),
            "---\nname: scout\ndescription: user version\n---\nU.\n",
        );
        let out = discover_in_roots(&[
            (space_root.clone(), AgentScope::Space),
            (user_root.clone(), AgentScope::User),
        ]);
        assert_eq!(out.len(), 1, "first-wins dedupe: {out:?}");
        assert_eq!(out[0].description, "space version");
        assert_eq!(out[0].scope, AgentScope::Space);
    }

    // 10 — find-by-name (NOT exact-vec equality): the user-level roots may
    // contribute unrelated entries on a developer machine.
    #[test]
    fn innermost_level_shadows_outer() {
        let root = scratch(); // `.git` at the root
        let proj = root.join("proj");
        fs::create_dir_all(&proj).unwrap();
        write_file(
            &root.join(".agents/agents/a.md"),
            "---\nname: a\n---\nouter-body\n",
        );
        write_file(
            &proj.join(".agents/agents/a.md"),
            "---\nname: a\n---\ninner-body\n",
        );
        let out = discover_agents(Some(&proj));
        let a = out
            .iter()
            .find(|d| d.name == "a")
            .expect("an entry named `a`");
        assert_eq!(a.system_prompt, "inner-body");
        assert_eq!(a.scope, AgentScope::Space);
    }

    // 11
    #[test]
    fn a_symlink_collapses_across_roots() {
        let a = scratch();
        write_file(&a.join("scout.md"), "---\nname: scout\n---\nFrom A.\n");
        let b = scratch();
        #[cfg(unix)]
        std::os::unix::fs::symlink(a.join("scout.md"), b.join("scout.md")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink(a.join("scout.md"), b.join("scout.md")).unwrap();
        let out = discover_in_roots(&[
            (a.clone(), AgentScope::Space),
            (b.clone(), AgentScope::User),
        ]);
        assert_eq!(out.len(), 1, "the symlink must collapse: {out:?}");
        assert_eq!(out[0].scope, AgentScope::Space);
        assert_eq!(
            out[0].path,
            canon(&a.join("scout.md")).to_string_lossy().into_owned()
        );
    }

    // 12
    #[test]
    fn a_model_with_a_level_suffix_is_preserved_verbatim() {
        let root = scratch();
        write_file(
            &root.join("m.md"),
            "---\nname: m\nmodel: fake/m2:high\n---\nB.\n",
        );
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        assert_eq!(out.len(), 1, "one agent expected: {out:?}");
        assert_eq!(out[0].model.as_deref(), Some("fake/m2:high"));
    }

    // 13
    #[test]
    fn output_is_deterministically_sorted() {
        let root = scratch();
        write_file(&root.join("zeta.md"), "---\nname: Zeta\n---\nZ.\n");
        write_file(&root.join("alpha.md"), "---\nname: Alpha\n---\nA.\n");
        let out = discover_in_roots(&[(root.clone(), AgentScope::User)]);
        let names: Vec<&str> = out.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["Alpha", "Zeta"], "sorted by lowercased name");
    }

    /// Restores `HOME` on scope exit (even when an assertion panics
    /// mid-test): `Some(v)` → set, `None` → remove.
    struct RestoreHome(Option<std::ffi::OsString>);
    impl Drop for RestoreHome {
        fn drop(&mut self) {
            match self.0.take() {
                // SAFETY: the `ENV_LOCK` is still held at drop time (this
                // guard is declared after the `_lock` guard and outlives
                // it in reverse); no other thread mutates HOME concurrently.
                Some(v) => unsafe {
                    std::env::set_var("HOME", v);
                },
                // SAFETY: the `ENV_LOCK` is still held at drop time (this
                // guard is declared after the `_lock` guard and outlives
                // it in reverse); no other thread mutates HOME concurrently.
                None => unsafe {
                    std::env::remove_var("HOME");
                },
            }
        }
    }

    // 14
    #[test]
    fn user_roots_reads_home() {
        // Hold the shared lock for the WHOLE set→assert→restore span (a
        // `cargo test --lib` runs all module tests concurrently).
        let _lock = crate::test_support::env_lock();
        let original = std::env::var_os("HOME");
        let _restore = RestoreHome(original.clone());
        let tmp = scratch();
        // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
        // span (the `env_lock` guard); no other thread mutates HOME
        // concurrently.
        unsafe {
            std::env::set_var("HOME", &tmp);
        }
        let roots = user_roots();
        // Raw joined paths (NO `canon` — these directories never exist
        // in the test, so `fs::canonicalize` would just fall back to
        // the raw path; the expected values are built with plain
        // `join`, as `tests/skills.rs` does for its root tests).
        assert_eq!(
            roots,
            vec![
                (tmp.join(".agents/agents"), AgentScope::User),
                (tmp.join(".pi/agent/agents"), AgentScope::User),
            ]
        );
    }

    // 15
    #[test]
    fn space_roots_walks_up_to_the_repo_root() {
        let root = scratch(); // `.git` at the root
        let space = root.join("a/b");
        fs::create_dir_all(&space).unwrap();
        let roots = space_roots(&space);
        // Raw joined paths (NO `canon` — the `.agents` / `.pi` dirs
        // never exist in the test, so `fs::canonicalize` would just
        // fall back to the raw path; the expected values are built with
        // plain `join`, as `tests/skills.rs` does for its root tests).
        assert_eq!(
            roots,
            vec![
                (space.join(".agents/agents"), AgentScope::Space),
                (space.join(".pi/agents"), AgentScope::Space),
                (root.join("a/.agents/agents"), AgentScope::Space),
                (root.join("a/.pi/agents"), AgentScope::Space),
                (root.join(".agents/agents"), AgentScope::Space),
                (root.join(".pi/agents"), AgentScope::Space),
            ]
        );
    }
}

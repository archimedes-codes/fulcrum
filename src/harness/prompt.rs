//! The native session's system prompt (ADR 0017): a minimal, pi-shaped
//! prompt for the main session, a reduced message for subagent children,
//! a Rust port of pi's `formatSkillsForPrompt` (metadata only), and a
//! `load_project_context` mirroring pi's candidate list + walk-up.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::agent::harness::provider::ToolSpec;
use crate::skills::SkillInfo;

const PREAMBLE: &str = "You are an expert coding assistant operating inside Archimedes Desktop, a desktop app that connects to coding agents. You help users by reading files, executing commands, editing code, and writing new files.";

const TODO_GUIDANCE: &str = "Use manage_todo_list to track multi-step work — write the plan before starting, mark items completed as you go";

const SUBAGENT_GUIDANCE: &str = "Delegate independent subtasks with subagent — give each a systemPrompt describing its role and constraints (e.g. a read-only researcher, a focused reviewer)";

const CONTEXT_CANDIDATES: &[&str] = &[
    "AGENTS.override.md",
    "AGENTS.md",
    "AGENTS.MD",
    "CLAUDE.md",
    "CLAUDE.MD",
];

/// The inputs for `build_main_prompt` (top-level — no `AgentLoop`
/// involvement).
pub struct PromptContext<'a> {
    pub cwd: &'a Path,
    /// The global context dir — `~/.pi/agent` (the `home_dir` in
    /// skills.rs). Pass `Path::new("")` to skip the global file.
    pub agent_dir: &'a Path,
    /// The tool specs ADVERTISED to the model (the `AgentLoop`'s
    /// `advertised_specs` — single source of truth: the prompt's
    /// `<tools>` section matches the `tools[]` API param exactly).
    pub tools: &'a [ToolSpec],
    /// The discovered skills (the existing `crate::skills::discover_skills`).
    pub skills: &'a [SkillInfo],
}

/// The main session's system prompt (ADR 0017): a minimal, pi-shaped prompt —
/// the one-sentence preamble + the `<tools>` list (matching the `tools[]` API
/// param exactly) + a short `<rules>` list + the `project_context`/`skills`/
/// `cwd` data sections. The sections are joined with a blank line (`"\n\n"`,
/// pi-ai's `getSystemMessageText` rendering); an omitted section leaves NO
/// trace (no empty `<x></x>`, no dangling blank lines).
pub fn build_main_prompt(ctx: &PromptContext) -> String {
    let mut sections: Vec<String> = vec![PREAMBLE.to_string()];

    // `<tools>`: one line per advertised spec, in order; zero specs → `(none)`
    // (a deliberate native simplification: pi's tools body also carries a
    // trailing sentence — the native format omits it).
    let tools_body = if ctx.tools.is_empty() {
        "(none)".to_string()
    } else {
        ctx.tools
            .iter()
            .map(|t| format!("- {}: {}", t.name, t.description))
            .collect::<Vec<_>>()
            .join("\n")
    };
    sections.push(format!("<tools>\n{tools_body}\n</tools>"));

    // `<rules>`: the EXACT order — TODO_GUIDANCE (if the tool is advertised),
    // SUBAGENT_GUIDANCE (if the tool is advertised), then the two pi lines.
    let mut rules: Vec<&str> = Vec::new();
    if ctx.tools.iter().any(|t| t.name == "manage_todo_list") {
        rules.push(TODO_GUIDANCE);
    }
    if ctx.tools.iter().any(|t| t.name == "subagent") {
        rules.push(SUBAGENT_GUIDANCE);
    }
    rules.push("Be concise in your responses");
    rules.push("Show file paths clearly when working with files");
    let rules_body = rules
        .iter()
        .map(|r| format!("- {r}"))
        .collect::<Vec<_>>()
        .join("\n");
    sections.push(format!("<rules>\n{rules_body}\n</rules>"));

    // `<project_context>`: one `<project_instructions path>` block per file, in
    // the order `load_project_context` returns them, separated by a blank line;
    // the content verbatim (BOM already stripped). Omitted when empty.
    let context = load_project_context(ctx.cwd, ctx.agent_dir);
    if !context.is_empty() {
        let blocks: Vec<String> = context
            .iter()
            .map(|(path, content)| {
                // The attribute value is XML-escaped (like the skills
                // section's `<location>` — a path with `"`/`&`/`<` would
                // otherwise produce invalid pseudo-XML).
                format!(
                    "<project_instructions path=\"{}\">\n{}\n</project_instructions>",
                    escape_xml(&path.display().to_string()),
                    content
                )
            })
            .collect();
        let body = format!(
            "Project-specific instructions and guidelines:\n\n{}",
            blocks.join("\n\n")
        );
        sections.push(format!("<project_context>\n{body}\n</project_context>"));
    }

    // `<skills>`: only when skills are present AND a `read` (preferred) or
    // `bash` spec is advertised (pi's `["read", "bash"].find` order).
    let file_read_tool = if ctx.tools.iter().any(|t| t.name == "read") {
        Some("read")
    } else if ctx.tools.iter().any(|t| t.name == "bash") {
        Some("bash")
    } else {
        None
    };
    if !ctx.skills.is_empty() {
        if let Some(tool) = file_read_tool {
            let body = format_skills_for_prompt(ctx.skills, tool);
            sections.push(format!("<skills>\n{body}\n</skills>"));
        }
    }

    // `<cwd>`: backslashes → `/` (pi's `toPosixPath`).
    let cwd = ctx.cwd.to_string_lossy().replace('\\', "/");
    sections.push(format!("<cwd>\n{cwd}\n</cwd>"));

    sections.join("\n\n")
}

/// The subagent child's REDUCED system message (ADR 0017): the `launch`
/// `systemPrompt` (if any — an empty/whitespace-only value counts as `None`,
/// so no leading blank line) + the `manage_todo_list` guidance line (when the
/// child has the tool) — no preamble, no tools list. `None` + no tool →
/// `None` (no system message at all).
pub fn build_child_system_message(
    system_prompt: Option<&str>,
    has_todo_tool: bool,
) -> Option<String> {
    let sp = system_prompt.filter(|s| !s.trim().is_empty());
    match (sp, has_todo_tool) {
        (Some(sp), true) => Some(format!("{sp}\n{TODO_GUIDANCE}")),
        (Some(sp), false) => Some(sp.to_string()),
        (None, true) => Some(TODO_GUIDANCE.to_string()),
        (None, false) => None,
    }
}

/// The project context files (mirrors pi's `loadProjectContextFiles` candidate
/// list + walk-up + global-file-first ordering), with TWO deliberate
/// deviations (documented so a future reader doesn't "fix" them back to pi's):
/// 1. The walk STOPS at the REPO ROOT — the first ancestor (inclusive)
///    containing a `.git` entry (file OR dir); if none, the filesystem root.
///    (Pi walks to the filesystem root — a project's context must not leak
///    from the user's home dir / unrelated ancestors.)
/// 2. Dedup is by CANONICAL path (`fs::canonicalize`, falling back to the raw
///    path on failure). (Pi dedups by raw lexical path.)
///
/// Best-effort: an unreadable file / missing dir is silently skipped.
pub fn load_project_context(cwd: &Path, agent_dir: &Path) -> Vec<(PathBuf, String)> {
    let mut out: Vec<(PathBuf, String)> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    // 1. The global file FIRST (an empty/missing `agent_dir` → skip).
    if !agent_dir.as_os_str().is_empty() {
        if let Some(entry) = load_context_file_from_dir(agent_dir) {
            push_deduped(&mut out, &mut seen, entry);
        }
    }

    // 2. Walk up from `cwd` to the bound in (1); for each dir, outermost
    //    first. (A missing `cwd` → no walk.)
    if cwd.exists() {
        let mut dirs: Vec<PathBuf> = Vec::new();
        let mut cur = cwd.to_path_buf();
        loop {
            dirs.push(cur.clone());
            if cur.join(".git").exists() {
                break;
            }
            match cur.parent() {
                Some(p) if p != cur => cur = p.to_path_buf(),
                _ => break,
            }
        }
        for dir in dirs.iter().rev() {
            if let Some(entry) = load_context_file_from_dir(dir) {
                push_deduped(&mut out, &mut seen, entry);
            }
        }
    }

    out
}

/// For each candidate in `CONTEXT_CANDIDATES`, `dir.join(name)`: the FIRST
/// candidate that `is_file()` AND reads successfully wins — a stat/read
/// failure on an existing candidate CONTINUES to the next candidate (pi's
/// `loadContextFileFromDir` behavior: the read `catch` warns and falls
/// through; a non-file also continues); if none reads, `None`. The BOM is
/// stripped.
fn load_context_file_from_dir(dir: &Path) -> Option<(PathBuf, String)> {
    for name in CONTEXT_CANDIDATES {
        let path = dir.join(name);
        if !path.is_file() {
            continue;
        }
        let Ok(content) = fs::read_to_string(&path) else {
            continue;
        };
        let content = if let Some(stripped) = content.strip_prefix('\u{feff}') {
            stripped.to_string()
        } else {
            content
        };
        return Some((path, content));
    }
    None
}

/// Push `entry` unless its CANONICAL path (falling back to the raw path on
/// failure) was already seen — a symlink to an already-loaded file is
/// deduped, the first (outermost) entry wins.
fn push_deduped(
    out: &mut Vec<(PathBuf, String)>,
    seen: &mut HashSet<PathBuf>,
    entry: (PathBuf, String),
) {
    let (path, content) = entry;
    let canon = fs::canonicalize(&path).unwrap_or(path.clone());
    if seen.insert(canon) {
        out.push((path, content));
    }
}

/// A Rust port of pi's `formatSkillsForPrompt` (metadata only — the native
/// `SkillInfo`/`discover_skills` (ADR 0013) has no `disableModelInvocation`
/// field, so no filtering in v1): the per-skill block is byte-identical to
/// pi's, with pi's leading `"\n\n"` trimmed.
fn format_skills_for_prompt(skills: &[SkillInfo], file_read_tool: &str) -> String {
    let mut lines: Vec<String> = vec![
        "The following skills provide specialized instructions for specific tasks.".to_string(),
        if file_read_tool == "read" {
            "Use the read tool to load a skill's file when the task matches its description."
                .to_string()
        } else {
            "Use bash to load a skill's file when the task matches its description."
                .to_string()
        },
        "When a skill file references a relative path, resolve it against the skill directory (parent of SKILL.md / dirname of the path) and use that absolute path in tool commands."
            .to_string(),
        String::new(),
        "<available_skills>".to_string(),
    ];
    for skill in skills {
        lines.push("  <skill>".to_string());
        lines.push(format!("    <name>{}</name>", escape_xml(&skill.name)));
        lines.push(format!(
            "    <description>{}</description>",
            escape_xml(&skill.description)
        ));
        lines.push(format!(
            "    <location>{}</location>",
            escape_xml(&skill.path)
        ));
        lines.push("  </skill>".to_string());
    }
    lines.push("</available_skills>".to_string());
    lines.join("\n")
}

/// pi's `escapeXml`: `&` `<` `>` `"` `'` → `&amp;` `&lt;` `&gt;` `&quot;`
/// `&apos;` (the `&` first — the replacements compose).
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skills::SkillScope;
    use std::fs;

    fn spec(name: &str, description: &str) -> ToolSpec {
        ToolSpec {
            name: name.to_string(),
            description: description.to_string(),
            parameters: serde_json::Value::Null,
        }
    }

    fn skill(name: &str, description: &str, path: &str) -> SkillInfo {
        SkillInfo {
            name: name.to_string(),
            description: description.to_string(),
            path: path.to_string(),
            dir: path.to_string(),
            scope: SkillScope::User,
            body: "body".to_string(),
        }
    }

    /// A scratch root with an EMPTY `.git/` dir so the context walk is
    /// bounded at the scratch (a stray `AGENTS.md`/`CLAUDE.md` in `/tmp`
    /// or `/` — from another project's test run — must not break the
    /// exact-golden or absence assertions).
    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("archimedes-prompt-{}", uuid::Uuid::new_v4()));
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
    /// is a symlinked path).
    fn canon(p: &Path) -> PathBuf {
        fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
    }

    // 1 — the EXACT full output (golden string, blank-line join, global
    // file first in `<project_context>`).
    #[test]
    fn main_prompt_full_shape() {
        let root = scratch();
        let cwd = root.join("proj");
        write_file(&cwd.join("AGENTS.md"), "Project rules here.");
        let agent_dir = root.join("agent");
        write_file(&agent_dir.join("AGENTS.md"), "Global rules.");
        let skill_md = root
            .join("skills/alpha/SKILL.md")
            .to_string_lossy()
            .into_owned();
        let skills = vec![skill("alpha", "Does alpha.", &skill_md)];
        let tools = vec![
            spec("read", "Read a file."),
            spec("manage_todo_list", "Manage the todo list."),
            spec("subagent", "Launch a subagent."),
        ];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: &agent_dir,
            tools: &tools,
            skills: &skills,
        };
        let prompt = build_main_prompt(&ctx);

        // The production code emits the `path` attribute and the skill
        // `<location>` XML-escaped — the golden applies the SAME escaping
        // to the interpolated paths (a scratch dir containing a char from
        // the escape table, e.g. `TMPDIR=/tmp/a&b`, must still match).
        // The `<cwd>` body is NOT escaped but IS slash-normalized in the
        // production code (`toPosixPath`), so mirror both here.
        let global_path = escape_xml(&agent_dir.join("AGENTS.md").to_string_lossy());
        let project_path = escape_xml(&cwd.join("AGENTS.md").to_string_lossy());
        let skill_md_escaped = escape_xml(&skill_md);
        let cwd_display = cwd.to_string_lossy().replace('\\', "/");
        // The sections joined with the frozen blank-line join ("\n\n");
        // each section is a raw string with the EXACT rendered body (the
        // scratch-dir paths interpolated — they differ per run).
        let expected = [
            PREAMBLE.to_string(),
            r#"<tools>
- read: Read a file.
- manage_todo_list: Manage the todo list.
- subagent: Launch a subagent.
</tools>"#.to_string(),
            format!(
                r#"<rules>
- {TODO_GUIDANCE}
- {SUBAGENT_GUIDANCE}
- Be concise in your responses
- Show file paths clearly when working with files
</rules>"#
            ),
            format!(
                r#"<project_context>
Project-specific instructions and guidelines:

<project_instructions path="{global_path}">
Global rules.
</project_instructions>

<project_instructions path="{project_path}">
Project rules here.
</project_instructions>
</project_context>"#
            ),
            format!(
                r#"<skills>
The following skills provide specialized instructions for specific tasks.
Use the read tool to load a skill's file when the task matches its description.
When a skill file references a relative path, resolve it against the skill directory (parent of SKILL.md / dirname of the path) and use that absolute path in tool commands.

<available_skills>
  <skill>
    <name>alpha</name>
    <description>Does alpha.</description>
    <location>{skill_md_escaped}</location>
  </skill>
</available_skills>
</skills>"#
            ),
            format!(
                r#"<cwd>
{cwd_display}
</cwd>"#
            ),
        ]
        .join("\n\n");
        assert_eq!(prompt, expected);
    }

    // 2 — the `<rules>` order is EXACTLY: TODO_GUIDANCE (if present),
    // SUBAGENT_GUIDANCE (if present), then the two pi lines.
    #[test]
    fn main_prompt_rules_conditional() {
        let root = scratch();
        let cwd = root.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        // WITHOUT manage_todo_list / subagent → both guidance lines absent,
        // the two pi lines present.
        let tools = vec![spec("read", "Read a file.")];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &[],
        };
        let prompt = build_main_prompt(&ctx);
        assert!(
            !prompt.contains("manage_todo_list to track"),
            "TODO_GUIDANCE must be absent without the tool"
        );
        assert!(
            !prompt.contains("Delegate independent subtasks"),
            "SUBAGENT_GUIDANCE must be absent without the tool"
        );
        assert!(
            prompt.contains(
                "<rules>\n\
                 - Be concise in your responses\n\
                 - Show file paths clearly when working with files\n\
                 </rules>"
            ),
            "the two pi lines, alone, in order: {prompt}"
        );

        // WITH both → all 4 lines in the exact order.
        let tools = vec![
            spec("manage_todo_list", "Manage the todo list."),
            spec("subagent", "Launch a subagent."),
            spec("read", "Read a file."),
        ];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &[],
        };
        let prompt = build_main_prompt(&ctx);
        assert!(
            prompt.contains(&format!(
                "<rules>\n\
                 - {TODO_GUIDANCE}\n\
                 - {SUBAGENT_GUIDANCE}\n\
                 - Be concise in your responses\n\
                 - Show file paths clearly when working with files\n\
                 </rules>"
            )),
            "all 4 lines in the exact order: {prompt}"
        );
    }

    // 3 — omitted sections leave NO trace (no empty `<x></x>`, no
    // dangling blank lines).
    #[test]
    fn main_prompt_omitted_sections() {
        let root = scratch();
        let cwd = root.join("proj");
        fs::create_dir_all(&cwd).unwrap();
        let tools = vec![spec("read", "Read a file.")];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &[],
        };
        let prompt = build_main_prompt(&ctx);
        assert!(
            !prompt.contains("<project_context>"),
            "no <project_context>: {prompt}"
        );
        assert!(!prompt.contains("<skills>"), "no <skills>: {prompt}");
        // The `<rules>` block is immediately followed by `<cwd>`.
        assert!(
            prompt.contains(
                "<rules>\n\
                 - Be concise in your responses\n\
                 - Show file paths clearly when working with files\n\
                 </rules>\n\
                 \n\
                 <cwd>"
            ),
            "no dangling blank lines: {prompt}"
        );
    }

    // 4 — `<skills>` requires a `read` or `bash` spec; `read` wins when
    // both are present.
    #[test]
    fn main_prompt_skills_requires_read_or_bash() {
        let root = scratch();
        let cwd = root.join("proj");
        fs::create_dir_all(&cwd).unwrap();
        let skill_md = root.join("s/SKILL.md").to_string_lossy().into_owned();
        let skills = vec![skill("alpha", "Does alpha.", &skill_md)];

        let with_tools = |tools: &[ToolSpec]| -> String {
            let ctx = PromptContext {
                cwd: &cwd,
                agent_dir: Path::new(""),
                tools,
                skills: &skills,
            };
            build_main_prompt(&ctx)
        };

        // Only manage_todo_list → no `<skills>` at all.
        let prompt = with_tools(&[spec("manage_todo_list", "Manage the todo list.")]);
        assert!(
            !prompt.contains("<skills>"),
            "no <skills> without read/bash: {prompt}"
        );

        // bash (no read) → the bash line.
        let prompt = with_tools(&[spec("bash", "Run a command.")]);
        assert!(
            prompt
                .contains("Use bash to load a skill's file when the task matches its description."),
            "bash line: {prompt}"
        );

        // read → the read line.
        let prompt = with_tools(&[spec("read", "Read a file.")]);
        assert!(
            prompt.contains(
                "Use the read tool to load a skill's file when the task matches its description."
            ),
            "read line: {prompt}"
        );

        // both → the read line (pi's ["read", "bash"].find order).
        let prompt = with_tools(&[spec("bash", "Run a command."), spec("read", "Read a file.")]);
        assert!(
            prompt.contains(
                "Use the read tool to load a skill's file when the task matches its description."
            ),
            "read wins over bash: {prompt}"
        );
        assert!(
            !prompt.contains("Use bash to load"),
            "no bash line when read present: {prompt}"
        );
    }

    // 5 — the `<tools>` body: one `- {name}: {description}` line per spec
    // in order; zero specs → `(none)`.
    #[test]
    fn main_prompt_tools_section() {
        let root = scratch();
        let cwd = root.join("proj");
        fs::create_dir_all(&cwd).unwrap();

        let tools = vec![
            spec("read", "Read a file."),
            spec("bash", "Run a command."),
            spec("edit", "Edit a file."),
        ];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &[],
        };
        let prompt = build_main_prompt(&ctx);
        assert!(
            prompt.contains(
                "<tools>\n\
                 - read: Read a file.\n\
                 - bash: Run a command.\n\
                 - edit: Edit a file.\n\
                 </tools>"
            ),
            "one line per spec, in order: {prompt}"
        );

        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &[],
            skills: &[],
        };
        let prompt = build_main_prompt(&ctx);
        assert!(
            prompt.contains("<tools>\n(none)\n</tools>"),
            "zero specs → (none): {prompt}"
        );
    }

    // 6 — the first readable candidate wins; a read failure CONTINUES to
    // the next candidate.
    #[test]
    fn context_candidate_precedence() {
        let root = scratch();

        // AGENTS.override.md + AGENTS.md + CLAUDE.md → only AGENTS.override.md.
        let d = root.join("d");
        write_file(&d.join("AGENTS.override.md"), "override");
        write_file(&d.join("AGENTS.md"), "agents");
        write_file(&d.join("CLAUDE.md"), "claude");
        let res = load_project_context(&d, Path::new(""));
        assert_eq!(res.len(), 1, "only the override: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&d.join("AGENTS.override.md")));
        assert_eq!(res[0].1, "override");

        // Only CLAUDE.md → CLAUDE.md.
        let d2 = root.join("d2");
        write_file(&d2.join("CLAUDE.md"), "claude2");
        let res = load_project_context(&d2, Path::new(""));
        assert_eq!(res.len(), 1, "the CLAUDE.md: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&d2.join("CLAUDE.md")));

        // AGENTS.md UNREADABLE (chmod 000) + CLAUDE.md reads fine →
        // CLAUDE.md (the read failure continues to the next candidate).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let d3 = root.join("d3");
            write_file(&d3.join("AGENTS.md"), "agents3");
            write_file(&d3.join("CLAUDE.md"), "claude3");
            fs::set_permissions(d3.join("AGENTS.md"), fs::Permissions::from_mode(0o000)).unwrap();
            // Root ignores mode bits — if the read unexpectedly SUCCEEDS we
            // can't simulate the unreadable case; skip the sub-assertions
            // (non-root: the read fails and the assertions hold).
            if fs::read_to_string(d3.join("AGENTS.md")).is_err() {
                let res = load_project_context(&d3, Path::new(""));
                assert_eq!(res.len(), 1, "the unreadable AGENTS.md is skipped: {res:?}");
                assert_eq!(canon(&res[0].0), canon(&d3.join("CLAUDE.md")));
                assert_eq!(res[0].1, "claude3");
            }
        }
    }

    // 7 — THE DELIBERATE deviation from pi: the walk STOPS at the repo
    // root (the first ancestor containing a `.git` entry).
    #[test]
    fn context_walk_up_repo_root_bound() {
        let root = scratch();
        let outer = root.join("outer");
        let repo = outer.join("repo");
        fs::create_dir_all(repo.join(".git")).unwrap();
        write_file(&repo.join("AGENTS.md"), "repo rules");
        write_file(&outer.join("AGENTS.md"), "outer rules");
        let cwd = repo.join("sub/nested");
        fs::create_dir_all(&cwd).unwrap();

        let res = load_project_context(&cwd, Path::new(""));
        assert_eq!(res.len(), 1, "only the repo's AGENTS.md: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&repo.join("AGENTS.md")));
        assert!(
            !res.iter()
                .any(|(p, _)| canon(p) == canon(&outer.join("AGENTS.md"))),
            "the ancestor ABOVE the repo root is NOT included"
        );
    }

    // 8 — no `.git` below the scratch root: the walk is bounded at the
    // scratch (its `.git`), but a non-repo ancestor WITHIN the scratch
    // is still included.
    #[test]
    fn context_walk_up_no_git() {
        let root = scratch();
        let a = root.join("a");
        write_file(&a.join("AGENTS.md"), "a rules");
        let cwd = a.join("b/c");
        fs::create_dir_all(&cwd).unwrap();

        let res = load_project_context(&cwd, Path::new(""));
        assert_eq!(res.len(), 1, "a/AGENTS.md: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&a.join("AGENTS.md")));
        assert_eq!(res[0].1, "a rules");
    }

    // 9 — outermost first.
    #[test]
    fn context_order_outermost_first() {
        let root = scratch();
        let a = root.join("a");
        write_file(&a.join("AGENTS.md"), "outer");
        let b = a.join("b");
        write_file(&b.join("AGENTS.md"), "inner");

        let res = load_project_context(&b, Path::new(""));
        assert_eq!(res.len(), 2, "both files: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&a.join("AGENTS.md")));
        assert_eq!(canon(&res[1].0), canon(&b.join("AGENTS.md")));
    }

    // 10 — a symlinked candidate dedups to ONE entry (canonical path).
    #[cfg(unix)]
    #[test]
    fn context_dedup_symlink() {
        use std::os::unix::fs::symlink;
        let root = scratch();
        let a = root.join("a");
        write_file(&a.join("AGENTS.md"), "one file");
        let b = a.join("b");
        fs::create_dir_all(&b).unwrap();
        symlink(a.join("AGENTS.md"), b.join("AGENTS.md")).unwrap();

        let res = load_project_context(&b, Path::new(""));
        assert_eq!(res.len(), 1, "the symlink dedups: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&a.join("AGENTS.md")));
    }

    // 11 — a UTF-8 BOM is stripped.
    #[test]
    fn context_bom_stripped() {
        let root = scratch();
        let d = root.join("d");
        write_file(&d.join("AGENTS.md"), "\u{feff}BOM content");

        let res = load_project_context(&d, Path::new(""));
        assert_eq!(res.len(), 1, "the file: {res:?}");
        assert_eq!(res[0].1, "BOM content");
    }

    // 12 — the global file is pushed FIRST; a global CLAUDE.md counts too.
    #[test]
    fn context_global_first() {
        let root = scratch();
        let agent_dir = root.join("agent");
        write_file(&agent_dir.join("AGENTS.md"), "global");
        let cwd = root.join("proj");
        write_file(&cwd.join("AGENTS.md"), "project");

        let res = load_project_context(&cwd, &agent_dir);
        assert_eq!(res.len(), 2, "global + project: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&agent_dir.join("AGENTS.md")));
        assert_eq!(canon(&res[1].0), canon(&cwd.join("AGENTS.md")));

        // An agent_dir with only CLAUDE.md → the global CLAUDE.md is
        // included (the first candidate that is a file, any candidate).
        let agent_dir2 = root.join("agent2");
        write_file(&agent_dir2.join("CLAUDE.md"), "global claude");
        let cwd2 = root.join("proj2");
        fs::create_dir_all(&cwd2).unwrap();
        let res = load_project_context(&cwd2, &agent_dir2);
        assert_eq!(res.len(), 1, "the global CLAUDE.md: {res:?}");
        assert_eq!(canon(&res[0].0), canon(&agent_dir2.join("CLAUDE.md")));
        assert_eq!(res[0].1, "global claude");
    }

    // 13 — the child-message matrix (exact strings, incl. the empty/
    // whitespace-only `systemPrompt` collapse).
    #[test]
    fn child_message_matrix() {
        assert_eq!(
            build_child_system_message(Some("Do X."), true),
            Some(format!("Do X.\n{TODO_GUIDANCE}"))
        );
        assert_eq!(
            build_child_system_message(Some("Do X."), false),
            Some("Do X.".to_string())
        );
        assert_eq!(
            build_child_system_message(None, true),
            Some(TODO_GUIDANCE.to_string())
        );
        assert_eq!(build_child_system_message(None, false), None);
        // An empty/whitespace-only `systemPrompt` collapses to `None` (no
        // leading blank line in the child's system message).
        assert_eq!(
            build_child_system_message(Some("   "), true),
            Some(TODO_GUIDANCE.to_string())
        );
        assert_eq!(build_child_system_message(Some(""), false), None);
    }

    // 14 — the `<skills>` body is byte-identical to pi's
    // `formatSkillsForPrompt` output with its leading "\n\n" trimmed.
    #[test]
    fn skills_format_matches_pi_golden() {
        let root = scratch();
        let cwd = root.join("proj");
        fs::create_dir_all(&cwd).unwrap();
        let s1 = skill(
            "alpha",
            "Does alpha.",
            &root.join("a/SKILL.md").to_string_lossy(),
        );
        let s2 = skill(
            "beta",
            "Does beta.",
            &root.join("b/SKILL.md").to_string_lossy(),
        );
        let skills = vec![s1.clone(), s2.clone()];
        let tools = vec![spec("read", "Read a file.")];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &skills,
        };
        let prompt = build_main_prompt(&ctx);

        // Extract the `<skills>` body.
        let start = prompt
            .find("<skills>\n")
            .map(|i| i + "<skills>\n".len())
            .unwrap_or_else(|| panic!("the <skills> section: {prompt}"));
        let end = prompt
            .find("\n</skills>")
            .unwrap_or_else(|| panic!("the </skills> close: {prompt}"));
        let body = &prompt[start..end];

        let expected = format!(
            r#"The following skills provide specialized instructions for specific tasks.
Use the read tool to load a skill's file when the task matches its description.
When a skill file references a relative path, resolve it against the skill directory (parent of SKILL.md / dirname of the path) and use that absolute path in tool commands.

<available_skills>
  <skill>
    <name>alpha</name>
    <description>Does alpha.</description>
    <location>{}</location>
  </skill>
  <skill>
    <name>beta</name>
    <description>Does beta.</description>
    <location>{}</location>
  </skill>
</available_skills>"#,
            escape_xml(&s1.path),
            escape_xml(&s2.path)
        );
        assert_eq!(body, expected);
    }
    // 15 — XML-escape name/description per pi's `escapeXml` table.
    #[test]
    fn skills_xml_escaping() {
        let root = scratch();
        let cwd = root.join("proj");
        fs::create_dir_all(&cwd).unwrap();
        let s = skill("a&b", "x<y>\"z'w", "/tmp/loc");
        let tools = vec![spec("read", "Read a file.")];
        let ctx = PromptContext {
            cwd: &cwd,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &[s],
        };
        let prompt = build_main_prompt(&ctx);
        assert!(
            prompt.contains("<name>a&amp;b</name>"),
            "name escaped: {prompt}"
        );
        assert!(
            prompt.contains("<description>x&lt;y&gt;&quot;z&apos;w</description>"),
            "description escaped: {prompt}"
        );
        assert!(
            prompt.contains("<location>/tmp/loc</location>"),
            "a plain path is untouched: {prompt}"
        );
    }

    // 16 — the `<project_instructions path>` attribute is XML-escaped (the
    // golden above applies the same `escape_xml` to the interpolated paths,
    // so it stays an exact match; a path WITH special chars must be escaped,
    // like the skills section's `<location>`).
    #[test]
    fn context_path_xml_escaped() {
        let root = scratch();
        // `&` and `'` are legal in filenames on EVERY platform (a `"` is
        // not — on Windows `create_dir_all` would panic) and both hit the
        // escape table — the portable case.
        let d = root.join("we&ird'dir");
        write_file(&d.join("AGENTS.md"), "x");
        let tools = vec![spec("read", "Read a file.")];
        let ctx = PromptContext {
            cwd: &d,
            agent_dir: Path::new(""),
            tools: &tools,
            skills: &[],
        };
        let prompt = build_main_prompt(&ctx);
        let escaped = escape_xml(&d.join("AGENTS.md").to_string_lossy());
        assert!(
            prompt.contains(&format!("<project_instructions path=\"{}\">", escaped)),
            "the path attribute is escaped: {prompt}"
        );
        assert!(
            prompt.contains("we&amp;ird&apos;dir"),
            "the `&` and `'` are escaped: {prompt}"
        );
        // `"` `<` `>` are illegal in Windows filenames — unix-only (like
        // the symlink and chmod-000 tests above).
        #[cfg(unix)]
        {
            let d = root.join("we\"ird<>&dir");
            write_file(&d.join("AGENTS.md"), "x");
            let ctx = PromptContext {
                cwd: &d,
                agent_dir: Path::new(""),
                tools: &tools,
                skills: &[],
            };
            let prompt = build_main_prompt(&ctx);
            assert!(
                prompt.contains(&format!(
                    "<project_instructions path=\"{}\">",
                    escape_xml(&d.join("AGENTS.md").to_string_lossy())
                )),
                "the path attribute is escaped: {prompt}"
            );
            assert!(
                prompt.contains("we&quot;ird&lt;&gt;&amp;dir"),
                "the `\"` `<` `>` are escaped: {prompt}"
            );
        }
    }
}

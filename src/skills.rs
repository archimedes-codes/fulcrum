//! Skill discovery (ADR 0013 — the desktop owns the skill layer).
//!
//! Finds and parses `SKILL.md` files under a bounded set of roots,
//! mirroring the roots pi scans (so the left pane matches what pi
//! advertises in the common case) and ZCode's bounded-walk +
//! best-effort frontmatter rules.
//!
//! Pure (fs reads only) and total: `discover_skills` never panics and
//! never errors — a missing root, an unreadable file, or an
//! unparseable `SKILL.md` is silently skipped. Frontmatter is parsed by
//! a hand-rolled tolerant reader (no new crate dependencies): a skill
//! that cannot be parsed is skipped, never a crash.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

/// One discovered skill (camelCase over IPC; all fields are lowercase
/// single words).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillInfo {
    /// Frontmatter `name`, else the skill's directory name.
    pub name: String,
    /// Frontmatter `description`, else `""`.
    pub description: String,
    /// Absolute (canonical) path to the `SKILL.md` file.
    pub path: String,
    /// Absolute (canonical) path to the skill's directory (parent of `SKILL.md`).
    pub dir: String,
    /// `"space"` | `"user"`.
    pub scope: SkillScope,
    /// `SKILL.md` content MINUS the frontmatter, trimmed (the injected
    /// block's BODY).
    pub body: String,
    /// Frontmatter `disable-model-invocation: true` — the Skill is
    /// MODEL-HIDDEN: unadvertised and unresolvable by name (ADR 0036).
    pub disable_model_invocation: bool,
}

impl SkillInfo {
    /// The one predicate every model-facing seam filters on (ADR 0036).
    pub fn model_visible(&self) -> bool {
        !self.disable_model_invocation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SkillScope {
    Space,
    User,
}

impl std::fmt::Display for SkillScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SkillScope::Space => "space",
            SkillScope::User => "user",
        })
    }
}

/// Discover skills for `space_path` (its roots + the user-level roots).
/// `space_path: None` → user-level skills only.
///
/// Pure (fs reads only) and total: never panics, never errors — a
/// missing root / unreadable file / unparseable `SKILL.md` is silently
/// skipped.
pub fn discover_skills(space_path: Option<&Path>) -> Vec<SkillInfo> {
    let mut roots: Vec<(PathBuf, SkillScope)> = Vec::new();
    if let Some(p) = space_path {
        roots.extend(space_roots(p));
    }
    roots.extend(user_roots());
    discover_in_roots(&roots)
}

/// The core, testable without env manipulation: discover over EXPLICIT
/// roots. `roots` order defines first-wins dedupe precedence.
pub fn discover_in_roots(roots: &[(PathBuf, SkillScope)]) -> Vec<SkillInfo> {
    // Shared across ALL roots: a symlink in a later root pointing at an
    // earlier root's `SKILL.md` collapses by PATH, not just by name.
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut by_name: HashMap<String, SkillInfo> = HashMap::new();

    for (root, scope) in roots {
        if !root.is_dir() {
            continue;
        }
        for file in find_skill_files(root, &mut seen) {
            let Ok(content) = fs::read_to_string(&file) else {
                continue;
            };
            let parent = file.parent().unwrap_or(&file);
            let fallback = parent
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            let Some((name, description, body, disable_model_invocation)) =
                parse_skill_file(&content, &fallback)
            else {
                continue;
            };
            // First-wins across the whole run (a Space root shadows a
            // same-named user skill; an innermost ancestor shadows an
            // outer one).
            let key = name.to_lowercase();
            if by_name.contains_key(&key) {
                continue;
            }
            // BOTH canonical: `file` is already canonical (the walk
            // canonicalized it); canonicalize the dir too (a `tempdir()`
            // path can be a symlink prefix on some platforms).
            let dir = fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
            let path = file.to_string_lossy().into_owned();
            let dir = dir.to_string_lossy().into_owned();
            // `path`/`dir` are interpolated UNESCAPED into the injected
            // `<skill location="…">` tag / the `References are relative
            // to DIR.` line: a `"` or a control character there is the
            // same malformed-tag class the name check guards against →
            // skip the skill (malformed → skip, never crash).
            if !is_tag_safe_name(&path) || !is_tag_safe_name(&dir) {
                continue;
            }
            by_name.insert(
                key,
                SkillInfo {
                    name,
                    description,
                    path,
                    dir,
                    scope: *scope,
                    body,
                    disable_model_invocation,
                },
            );
        }
    }

    let mut out: Vec<SkillInfo> = by_name.into_values().collect();
    // `sort_by_cached_key`: a single lowercased allocation per element
    // (NOT per comparison), with the `path` as a deterministic tie-break.
    out.sort_by_cached_key(|s| (s.name.to_lowercase(), s.path.clone()));
    out
}

/// Walk UP from `space_path` to the repository root: the first ancestor
/// (including `space_path` itself) that contains a `.git` entry is the
/// repo root; if none exists, `space_path` alone is the only level. At
/// each level (innermost first) collect `<level>/.agents/skills` (first)
/// and `<level>/.pi/skills` (second), both tagged `SkillScope::Space`.
///
/// Do NOT canonicalize the input before walking (the caller passes a
/// canonical path; if `space_path` doesn't exist, return an empty vec).
pub fn space_roots(space_path: &Path) -> Vec<(PathBuf, SkillScope)> {
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
        out.push((level.join(".agents/skills"), SkillScope::Space));
        out.push((level.join(".pi/skills"), SkillScope::Space));
    }
    out
}

/// `home_dir().join(".agents/skills")` (first) and
/// `home_dir().join(".pi/agent/skills")` (second), both tagged
/// `SkillScope::User`. `home_dir()`: `HOME` → else `USERPROFILE`
/// (Windows) → else `std::env::home_dir()`; if none, an empty vec.
pub fn user_roots() -> Vec<(PathBuf, SkillScope)> {
    let Some(home) = home_dir() else {
        return Vec::new();
    };
    vec![
        (home.join(".agents/skills"), SkillScope::User),
        (home.join(".pi/agent/skills"), SkillScope::User),
    ]
}

pub(crate) fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from))
        .or_else(std::env::home_dir)
}

/// Recursive walk for files named exactly `SKILL.md`: ONLY depths 1..=4
/// below `root` (a `SKILL.md` directly at the root — depth 0 — is NOT a
/// skill; depth 1 = `<root>/<name>/SKILL.md`); skip any directory named
/// `node_modules`; skip files whose canonical (symlink-resolved) path
/// was already seen — `seen` is SHARED across all roots; return in
/// deterministic (lexicographic) order.
fn find_skill_files(root: &Path, seen: &mut HashSet<PathBuf>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    // `dir_depth` = the depth of the dir itself (root = 0); files
    // directly inside it are at depth `dir_depth` (a `SKILL.md` at the
    // root is depth 0 → NOT a skill; depth 1 = `<root>/<name>/SKILL.md`).
    walk(root, 0, &mut out, seen);
    out.sort();
    out
}

fn walk(dir: &Path, dir_depth: u32, out: &mut Vec<PathBuf>, seen: &mut HashSet<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        if is_dir {
            if entry.file_name() == "node_modules" {
                continue;
            }
            // Subdirs are at `dir_depth + 1` — only worth walking while
            // that stays within the 1..=4 bound (a dir at depth 4 still
            // holds depth-4 files, but its subdirs' files are depth 5).
            if dir_depth < 4 {
                walk(&path, dir_depth + 1, out, seen);
            }
            continue;
        }
        if entry.file_name() != "SKILL.md" || !(1..=4).contains(&dir_depth) {
            continue;
        }
        // Symlink-resolved; errors → skip.
        if let Ok(canon) = fs::canonicalize(&path) {
            if seen.insert(canon.clone()) {
                out.push(canon);
            }
        }
    }
}

/// The tolerant frontmatter reader (no YAML crate): any failure mode
/// (missing closing `---`, a >1024-char description, …) → `None` (the
/// skill is skipped, never a crash).
///
/// A DUPLICATE top-level key is the clearest place this reader is deliberately
/// LESS strict than pi: pi's YAML parser throws `DUPLICATE_KEY`, its frontmatter
/// parse fails, and pi skips the WHOLE `SKILL.md`; here the first occurrence
/// wins and the skill is kept (ADR 0036). The glued `disable-model-invocation:true`
/// (no space after the colon) is a second such place — pi's `yaml` throws
/// `MISSING_CHAR` and drops the file, where this prefix match reads the key.
fn parse_skill_file(content: &str, fallback_name: &str) -> Option<(String, String, String, bool)> {
    // 1. Normalize `\r\n` and lone `\r` to `\n`.
    let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();

    // 2. Frontmatter = a block starting at the very beginning: line 1
    //    is exactly `---`, then lines until a line that is exactly
    //    `---`. The closing `---` is REQUIRED — EOF before it means the
    //    frontmatter is malformed → None (step 7). If line 1 is not
    //    `---`: no frontmatter → name = fallback, description = "",
    //    body = the whole content (trimmed).
    if lines.first() != Some(&"---") {
        let name = fallback_name.to_string();
        // A `"` or a control character in the directory-name fallback
        // would break the injected `<skill name="…">` tag (see the
        // check in the frontmatter path) → skip.
        if !is_tag_safe_name(&name) {
            return None;
        }
        return Some((name, String::new(), normalized.trim().to_string(), false));
    }
    let close = lines[1..].iter().position(|l| *l == "---")?;
    let fm_end = 1 + close;
    let fm: &[&str] = &lines[1..fm_end];
    let body = lines[fm_end + 1..].join("\n").trim().to_string();

    // 3. Top-level (non-indented) `name:`, `description:` and
    //    `disable-model-invocation:` keys only (indented lines belong to
    //    the previous key's block value; any other top-level key is
    //    ignored).
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;
    let mut disable: Option<bool> = None;
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
            if disable.is_none() {
                if let Some(rest) = line.strip_prefix("disable-model-invocation:") {
                    // pi: `yaml.parse(frontmatter)["disable-model-invocation"]
                    // === true`. The YAML 1.2 CORE SCHEMA that pi's `yaml`
                    // package implements resolves `true`, `True` and `TRUE`
                    // to the boolean `true`, and a plain scalar ENDS at a `#`
                    // that follows whitespace (a comment) — so every one of
                    // those HIDES. `yes` / `on` are plain strings in the core
                    // schema, `1` is a number, a QUOTED "true" is a string,
                    // `true#x` (no space) is ONE plain scalar, and a `>` or
                    // `|` block scalar folds to the string `"true\n"` — none
                    // of those is `=== true`, so none hides. Anything else we
                    // read differently from pi would make the desktop and pi
                    // disagree on the same file (ADR 0036).
                    //
                    // ASCII SPACE AND TAB ONLY, at both trims. YAML separates
                    // a plain scalar on space and tab alone, so `true\u{A0}` /
                    // `true\u{3000}` / `true\u{B}` / `true\u{85}` are STRINGS
                    // to pi and stay model-VISIBLE there; a bare `trim()` /
                    // `trim_end()` removes every `char::is_whitespace` and
                    // would HIDE them — the fail-CLOSED direction, which is
                    // the one place THIS key's branch would silently swallow a
                    // skill pi advertises. It is NOT this reader's only
                    // skip-where-pi-publishes site — step 4b (a tag-unsafe name)
                    // and step 5 (a `description` over 1024 chars) are two
                    // others, each stated on its own step — so keep the claim
                    // per-step rather than widening it to the whole file. The
                    // same rule `parse_value`'s dedent states for the same
                    // reason.
                    let value = rest.trim_matches([' ', '\t']);
                    let value = match value.find('#') {
                        // A `#` only opens a comment when a space or a tab
                        // precedes it; glued to the token it is scalar text.
                        Some(i) if value[..i].ends_with([' ', '\t']) => {
                            value[..i].trim_end_matches([' ', '\t'])
                        }
                        _ => value,
                    };
                    disable = Some(matches!(value, "true" | "True" | "TRUE"));
                    // ASYMMETRY, deliberate and SCOPED OUT: this branch strips
                    // a ` #` comment, `parse_value` (which reads `name` and
                    // `description`) does NOT — so `description: Do the thing #
                    // note` is advertised here WITH the comment pi strips, and
                    // `name: a # note` becomes a literal name no `$` token can
                    // address. Fixing that would change the advertised text of
                    // every commented `SKILL.md` in the wild, which is a larger
                    // and separately-reviewable change; the flag branch strips
                    // because a wrong BOOLEAN here changes ENFORCEMENT, not
                    // prose. Do not "unify" the two readers by reflex.
                }
            }
        }
        i += 1;
    }

    // 4. `name` empty or whitespace-only after parsing (an `""` value,
    //    a `"   "` value, or a block with no indented lines) → fallback.
    //    `description` left as parsed (may be `""`).
    let name = name
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| fallback_name.to_string());
    let description = description.unwrap_or_default();
    let disable_model_invocation = disable.unwrap_or(false);

    // 4b. The final name is interpolated UNESCAPED into the injected
    //     `<skill name="…">` attribute: a `"` there would produce a
    //     malformed tag the agent mis-parses, and a control character
    //     (e.g. a `|` block value joined with newlines) is equally
    //     unsafe → skip the skill (malformed → skip, never crash).
    //     DIVERGENCE, deliberate: pi only WARNS on an invalid name and
    //     "still load[s] the skill even with warnings"
    //     (`dist/core/skills.js`), so this is a place we swallow a skill pi
    //     publishes — tag safety beats parity because the name is injected
    //     UNESCAPED.
    if !is_tag_safe_name(&name) {
        return None;
    }

    // 5. `description` longer than 1024 CHARS → skip the skill
    //    (ZCode's `skill_description_too_long` rule; chars, not bytes —
    //    a CJK description is ≈3 bytes/char and must not be dropped
    //    at ~341 chars). Second skip-where-pi-publishes site (the first is
    //    step 4b): pi's `validateDescription` only WARNS here and still loads
    //    the skill, so a >1024-char `description` is advertised by pi and
    //    absent here.
    if description.chars().count() > 1024 {
        return None;
    }

    // 6. `body` = the content after the closing `---` (trimmed).
    Some((name, description, body, disable_model_invocation))
}

/// The string is interpolated UNESCAPED into the injected `<skill>` tag
/// (the `name` attribute, the `location` attribute, and the `References
/// are relative to DIR.` line): a `"` there would produce a malformed
/// tag the agent mis-parses, and a control character (e.g. a `|` block
/// value joined with newlines) is equally unsafe. A string containing
/// either is NOT tag-safe (the skill is skipped, never listed).
///
/// `pub(crate)` because it is the crate's ONE tag-safety predicate — the
/// same reasoning applies to every resource name interpolated unescaped
/// into a prompt tag, and the MENTION-PICKER surfaces
/// (`agent::mcp::config::server_infos`,
/// `commands::agents::list_agent_definitions_for_space`) build on it via
/// [`is_mentionable_name`] so a resource whose name can never match a
/// mention token is never offered. Mirrors
/// how `agents.rs` already reuses this module's `home_dir` (the shared
/// helper lives here rather than being copy-pasted into a second,
/// divergent copy). ADR 0020's "keep the modules independent" rule is
/// about `parse_value` (the two frontmatter shapes); it does not bind a
/// predicate this module invented and that has exactly one semantics.
///
/// NOT applied at AGENT discovery on purpose: an agent name is dispatchable
/// by the harness's `agentName` param regardless of shape, so skipping it
/// would break existing user files (see the comment in `agents.rs`).
pub(crate) fn is_tag_safe_name(name: &str) -> bool {
    !name.contains('"') && !name.chars().any(char::is_control)
}

/// The MENTION form of [`is_tag_safe_name`]: can this resource name EVER be
/// produced by a mention token? `skills.ts` resolves a mention by exact
/// lookup — `entry.name.toLowerCase() === token` — and a token is
/// `[a-z0-9]+(-[a-z0-9]+)*` (a bare `-` is not allowed at either end or
/// doubled: `[a-z0-9]` must follow every one). So a name over that charset
/// (up to case) is the ONLY kind a picker offer can ever expand; anything
/// else is UNNAMEABLE from the composer, and offering it would insert a dead
/// token that silently never expands.
///
/// "Over that charset (up to case)" is deliberately approximate, and the
/// approximation is ACCEPTED: this predicate is the ASCII grammar, while the
/// TS comparison is a case-FOLD. A name like `fe\u{212A}tch` (U+212A KELVIN
/// SIGN) is hidden here — `is_ascii_alphanumeric` rejects it — yet the TS
/// side WOULD match it from a `#fetch` token, because JS
/// `"fe\u{212A}tch".toLowerCase() === "fetch"`. That is a divergence in the
/// LOST-FEATURE direction (a resource the picker will not offer), never in
/// the UNSAFE one: U+212A is TAG-SAFE (no `"`, no `<`, no control char), so
/// even a hand-typed token cannot malform the tag. Case-folding before the
/// grammar check to close it is not worth the complexity for a character
/// nobody names resources with — do NOT add it; document the divergence.
///
/// This is strictly STRONGER than tag-safety — every name it accepts is
/// tag-safe (no `"`, no control char, and no space either: a space is NOT a
/// control character, so `is_tag_safe_name` alone would keep offering
/// `has space`, whose token can never exist). The two questions are
/// different on purpose:
/// - discovery-time (`skills.rs`): "would this name malFORM the tag?" →
///   `is_tag_safe_name` (skip);
/// - picker-time (`agent::mcp::config::server_infos`,
///   `commands::agents::list_agent_definitions_for_space`): "can this name
///   ever be MENTIONED?" → this predicate (hide, do not skip — the resource
///   stays live for the session and for dispatch);
/// - route-naming-time, for SKILLS only: `exec_read_skill`'s model-hidden
///   refusal and the Skills-dialog tooltip decide whether to PROMISE a `$name`.
///   The `$` skill picker is deliberately NOT filtered by it — the Client's
///   skill catalog stays total (ADR 0036), so the picker can legitimately offer
///   a name the refusal will not promise. Do not describe this as "what the
///   picker filters on" for Skills: no skill code path filters on it.
///
/// MATCHED PAIR: `isMentionableSkillName` in `src/lib/skills.ts` is the
/// TypeScript side of THIS predicate (the Skills-dialog tooltip uses it to
/// decide whether to promise a `$name` route, and `read_skill` here uses it
/// for the same reason). The two must agree on the grammar — change one, the
/// other moves with it, and both sides carry tests for the same name set
/// (`scout` / `fast-recon` / mixed case in; `has space`, `a_b`, `a.b`,
/// `café`, `a--b`, `-lead`, `trail-` out).
pub(crate) fn is_mentionable_name(name: &str) -> bool {
    let mut expect_segment = true; // a segment is due (start, or after a `-`)
    for c in name.chars() {
        if c == '-' {
            if expect_segment {
                return false; // leading `-`, or `--`
            }
            expect_segment = true;
            continue;
        }
        if !c.is_ascii_alphanumeric() {
            return false; // space, `_`, `.`, `"`, a control char, …
        }
        expect_segment = false;
    }
    // Must end on a segment (a trailing `-` matches no token), and must not
    // be empty.
    !expect_segment
}

/// Parse the value of a top-level `key: <rest>` line (step 3).
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
    use super::{is_mentionable_name, is_tag_safe_name};

    #[test]
    fn is_tag_safe_name_rejects_quotes_and_control_chars_only() {
        assert!(is_tag_safe_name("scout"));
        assert!(is_tag_safe_name("has space")); // a SPACE is not a control char
        assert!(!is_tag_safe_name("x\" onload=\"y"));
        assert!(!is_tag_safe_name("a\nb</agent>"));
        assert!(!is_tag_safe_name("tab\there"));
    }

    #[test]
    fn is_mentionable_name_accepts_exactly_the_token_grammar_up_to_case() {
        for ok in [
            "scout",
            "Scout",
            "SCOUT",
            "a",
            "0",
            "fast-recon",
            "a1-b2-c3",
        ] {
            assert!(is_mentionable_name(ok), "{ok} must be mentionable");
        }
        for no in [
            "",
            "has space",
            "x\" onload=\"y",
            "a\nb</agent>",
            "a<b",
            "tab\there",
            "a_b",
            "a.b",
            "a/b",
            "a--b", // the token grammar needs an alnum after every `-`
            "-lead",
            "trail-",
            "café", // a non-ASCII letter lowercases to something no token can hold
        ] {
            assert!(!is_mentionable_name(no), "{no} must NOT be mentionable");
        }
    }

    #[test]
    fn is_mentionable_name_is_strictly_stronger_than_is_tag_safe_name() {
        // Everything mentionable is tag-safe (the picker filter can never
        // weaken the tag invariant), and the converse fails on a space — the
        // case that makes the two predicates DIFFERENT questions rather than
        // one copy.
        for name in ["scout", "a1-b2", "MIXED"] {
            assert!(is_mentionable_name(name) && is_tag_safe_name(name));
        }
        assert!(is_tag_safe_name("has space") && !is_mentionable_name("has space"));
    }
}

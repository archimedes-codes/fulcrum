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
    /// The Skills this name collision EXCLUDED, reported here on the winner
    /// (ADR 0065: a shadow is a REPORT, not an entry — the losers stay
    /// unaddressable by name). Additive, so every existing consumer of this
    /// struct is unaffected.
    ///
    /// **UI-only**: the Skills dialog renders it; it is NEVER interpolated into
    /// a prompt tag, so the advertised name sets cannot drift. Empty unless a
    /// genuinely different `SKILL.md` lost the name (a byte-identical or
    /// CRLF-only copy is a Duplicate and stays silent, and a candidate that
    /// fails a validity check was never eligible, so it is not reported).
    pub shadowed: Vec<Shadowed<SkillScope>>,
}

impl SkillInfo {
    /// The one predicate every model-facing seam filters on (ADR 0036).
    pub fn model_visible(&self) -> bool {
        !self.disable_model_invocation
    }
}

/// One Skill (or Agent definition) excluded by **Shadowing**: another root
/// contributed the same name first and this file is genuinely different.
/// Generic over the scope enum because `skills.rs` and `agents.rs` share the
/// rule with different tags — the shape `boundary::push_roots<T>` uses.
/// ADR 0065. UI-only: never interpolated into a prompt tag.
///
/// `PartialEq` is derived so a DTO CARRYING this can be compared as a whole
/// (`commands::agents::AgentConfigDto` needs it for its tests) — derived HERE
/// rather than mirroring this struct into a second Rust shape, which is the
/// Rust↔Rust twin ADR 0065 exists to forbid. Both users of `Shadowed<T>`
/// benefit from the one derivation.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Shadowed<T> {
    pub name: String,
    /// Canonical path to the shadowed `SKILL.md`.
    pub path: String,
    pub scope: T,
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
    // earlier root's `SKILL.md` collapses by PATH, not just by name. (That is
    // one of TWO link cases — `find_skill_files` also follows symlinked Skill
    // DIRECTORIES, deduped per root by canonical dir; this set stays the
    // canonical-FILE dedupe and must stay shared.)
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut by_name: HashMap<String, SkillInfo> = HashMap::new();
    // Run-local, alongside `by_name` and dropped with it: lowercased name ->
    // the WINNER's NORMALIZED RAW content, so a collision can be told apart
    // from a Duplicate. Compared against the whole file, NOT against
    // `SkillInfo.body` — `body` is frontmatter-stripped AND trimmed, so a
    // body-only comparison calls two Skills duplicates when they differ ONLY in
    // frontmatter (e.g. a model-visible winner and a model-hidden loser with
    // the same body: a real shadow, swallowed).
    let mut content_by_name: HashMap<String, String> = HashMap::new();

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
            // First-wins across the whole run (a Space root shadows a
            // same-named user skill; an innermost ancestor shadows an
            // outer one) — and the collision is RECORDED on the winner as a
            // `shadowed` report (ADR 0065).
            //
            // THE ORDER MATTERS: this check sits BELOW the validity guards on
            // purpose. A loser that was never eligible to be listed is not
            // "hidden by" anything, and recording it would surface a path
            // discovery deliberately refuses to surface. Verified against this
            // loop: the tag-safety guard above is the ONLY rejection that runs
            // after the dedupe — every other one (unparseable frontmatter, a
            // `description` over 1024 chars, an unsafe `name`) lives inside
            // `parse_skill_file`, which already runs before this point.
            let key = name.to_lowercase();
            // Normalized for the Duplicate test ONLY — never compared to
            // `body`, and never compared RAW (a CRLF-vs-LF copy is the same
            // Skill, not a shadow).
            let normalized = normalize_line_endings(&content);
            if let Some(winner) = by_name.get_mut(&key) {
                if content_by_name.get(&key).is_some_and(|w| *w == normalized) {
                    // A Duplicate, not a shadow (CONTEXT.md): the same file
                    // bytes under two roots. Silent.
                    continue;
                }
                // A genuine shadow: the loser rides on the WINNER, keeping its
                // own name, canonical path and REAL scope (a user Skill can
                // lose to another user root — `user_roots()` returns two, and
                // each Space level contributes two — so the dialog derives its
                // wording from the winner's scope, not from an assumption).
                winner.shadowed.push(Shadowed {
                    name,
                    path,
                    scope: *scope,
                });
                continue;
            }
            content_by_name.insert(key.clone(), normalized);
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
                    shadowed: Vec::new(),
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
///
/// DIRECTORY-NESS IS DECIDED THROUGH LINKS: a symlinked Skill directory is
/// descended like the real thing, because a symlink farm (`~/.pi/agent/
/// skills/*` entries that link into `~/.agents/skills`, or a project wiring
/// its Skills in by link) is how one Skill set gets shared across machines.
/// A `SKILL.md` that resolves through a link is still ONE skill: `seen`
/// dedupes it by canonical PATH, and the separate per-root `visited_dirs`
/// set stops a directory reachable by two links from being re-walked.
fn find_skill_files(root: &Path, seen: &mut HashSet<PathBuf>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    // Canonical DIR -> shallowest `dir_depth` it was first walked at. Seeded
    // with `root` at 0 so a link inside the tree pointing back at the root
    // cannot re-walk it (which would otherwise hand a root-level `SKILL.md` —
    // depth 0, not a skill by spec — a depth-1 read).
    let mut visited_dirs: HashMap<PathBuf, u32> = HashMap::new();
    if let Ok(canon) = fs::canonicalize(root) {
        visited_dirs.insert(canon, 0);
    }
    // `dir_depth` = the depth of the dir itself (root = 0); files
    // directly inside it are at depth `dir_depth` (a `SKILL.md` at the
    // root is depth 0 → NOT a skill; depth 1 = `<root>/<name>/SKILL.md`).
    walk(root, 0, &mut out, seen, &mut visited_dirs);
    out.sort();
    out
}

fn walk(
    dir: &Path,
    dir_depth: u32,
    out: &mut Vec<PathBuf>,
    seen: &mut HashSet<PathBuf>,
    visited_dirs: &mut HashMap<PathBuf, u32>,
) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Pruned on the entry NAME, BEFORE any metadata call: a link NAMED
        // `node_modules` is pruned exactly like a real one.
        if entry.file_name() == "node_modules" {
            continue;
        }
        // `entry.file_type()` reports `is_dir() == false` for a link-to-dir —
        // and so does `entry.metadata()`, which does NOT follow links on unix
        // (using it here would silently no-op this whole rule). `fs::metadata`
        // (like `Path::is_dir`) DOES follow it.
        let is_dir = fs::metadata(&path).map(|m| m.is_dir()).unwrap_or(false);
        if is_dir {
            // Subdirs are at `dir_depth + 1` — only worth walking while
            // that stays within the 1..=4 bound (a dir at depth 4 still
            // holds depth-4 files, but its subdirs' files are depth 5).
            if dir_depth < 4 {
                let child_depth = dir_depth + 1;
                // SEPARATE from `seen` (which holds canonical FILE paths and
                // is shared across roots by design — see `discover_in_roots`).
                // Skip only when this dir was already walked at a depth ≤
                // `child_depth`: walking a dir at its SHALLOWEST depth is
                // strictly more capable (the 1..=4 file bound is measured from
                // the walk root), so a deeper second visit is pure duplicate
                // work. Blindly skipping a dir that was first seen DEEPER would
                // drop the Skills whose only path to the root was this shallower
                // link, and make discovery depend on `read_dir` order. So: skip
                // iff the stored depth is shallower-or-equal, else record the
                // minimum and descend.
                //
                // This prevents RE-WALKING a directory reached by two links
                // (duplicate work, duplicate rows) — it is NOT a hang guard: a
                // link cycle already terminated via the `dir_depth < 4` /
                // file-depth `1..=4` bounds, and still does.
                let canon_dir = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
                if visited_dirs
                    .get(&canon_dir)
                    .is_some_and(|seen_at| *seen_at <= child_depth)
                {
                    continue;
                }
                visited_dirs.insert(canon_dir, child_depth);
                walk(&path, child_depth, out, seen, visited_dirs);
            }
            continue;
        }
        if entry.file_name() != "SKILL.md" || !(1..=4).contains(&dir_depth) {
            continue;
        }
        // Symlink-resolved; errors → skip.
        //
        // ACCEPTED RISK (pre-existing, ADR 0065): the canonical `SKILL.md`
        // makes `SkillInfo.dir` — `fs::canonicalize(parent)` — the link's
        // TARGET, and `exec_read_skill` installs that `dir` as its per-skill
        // containment boundary, so `read_skill` on a linked Skill can reach
        // files outside the project. Today's code already does this for a
        // linked `SKILL.md` file (`root/alpha/SKILL.md -> <victim>/secret.txt`
        // discovers as `name="victim"`, `dir="<victim>"`); what following
        // directory links adds is a link to a DIRECTORY that itself holds a
        // `SKILL.md` at depth 1..=4. Requiring links to resolve inside the
        // root would kill the symlink-farm feature, so it is not done here.
        if let Ok(canon) = fs::canonicalize(&path) {
            if seen.insert(canon.clone()) {
                out.push(canon);
            }
        }
    }
}

/// The ONE owner of the line-ending rule: `\r\n` and a lone `\r` become `\n`.
///
/// `pub(crate)` because two callers need the SAME normalization, and a
/// copy-pasted second copy would diverge: `parse_skill_file` (frontmatter
/// parsing) and the **Shadowing** comparison in `discover_in_roots`, which must
/// see a CRLF checkout of an otherwise-identical `SKILL.md` as the Duplicate it
/// is rather than as a shadow.
pub(crate) fn normalize_line_endings(s: &str) -> String {
    s.replace("\r\n", "\n").replace('\r', "\n")
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
    // 1. Normalize `\r\n` and lone `\r` to `\n` (the shared helper — the
    //    shadow comparison in `discover_in_roots` normalizes identically).
    let normalized = normalize_line_endings(content);
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
                    // ASCII SPACE AND TAB ONLY — at the trim and in the shared
                    // comment strip. YAML separates
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
                    let value = strip_trailing_comment(value);
                    disable = Some(matches!(value, "true" | "True" | "TRUE"));
                    // ONE reader, deliberately: this branch and `parse_value`
                    // (which reads `name` and `description`) strip the comment
                    // through the SAME `strip_trailing_comment`, so the same
                    // `SKILL.md` cannot be advertised with different prose by the
                    // two keys. Issue #11 closed the asymmetry this branch used to
                    // document; do not re-split the rule into two copies. What
                    // still differs is the TRIM above (`trim_matches([' ', '\t'])`
                    // here, `trim()` in `parse_value`) and that is deliberate —
                    // see the ASCII-only paragraph above.
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
/// - route-naming-time, for SKILLS only: FOUR places decide whether to PROMISE
///   a `$name` — `exec_read_skill`'s model-hidden refusal, the Skills-dialog
///   tooltip, that dialog's inert ROW AFFORDANCE for an unmentionable row, and
///   `insertSkill`'s guard before it dispatches the token (the last two are
///   issue #10). The `$` skill picker is deliberately NOT filtered by it — the
///   Client's skill catalog stays total (ADR 0036), so the picker can
///   legitimately offer a name the refusal will not promise. Do not describe
///   this as "what the picker filters on" for Skills: no skill code path filters
///   on it.
///
/// MATCHED PAIR: `isMentionableSkillName` in `src/lib/skills.ts` is the
/// TypeScript side of THIS predicate (the Skills-dialog tooltip, its inert row
/// affordance and `insertSkill`'s guard all use it to decide whether to promise
/// a `$name` route, and `read_skill` here uses it for the same reason — the
/// frontend's "where it is applied" paragraph in `skills.ts` is the authoritative
/// count). The two must agree on the grammar — change one, the
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

/// A trailing `#` comment ends an inline frontmatter value, the way pi's `yaml`
/// ends a plain scalar at one (`name: a # note` is `a` to pi, not `a # note`).
///
/// A comment is introduced by an ASCII SPACE or a TAB — `has#nospace` is one plain
/// scalar, and so is anything preceded by U+00A0 / U+3000 / VT / FF / NEL, which
/// are NOT YAML separators. ASCII-only for the same reason the
/// `disable-model-invocation` branch and the block dedent give theirs:
/// `char::is_whitespace` includes NBSP, so a bare `trim()` / `trim_end()` would cut
/// text pi keeps (and, in the dedent, would slice mid-character). A `#` that OPENS
/// the value is a comment too (`description: # internal` is `null` to pi, not the
/// string `# internal`).
///
/// A LEADING QUOTE is respected: the comment only starts after the CLOSING quote,
/// so `"# keep"` survives and `"a # b" # note` is `a # b`. Each quote style then
/// follows ITS OWN YAML escape rule, and they are different rules:
/// - a DOUBLE-quoted scalar escapes with a BACKSLASH, so `\"` is content and does
///   not close the scalar: `"a \" # b"` is one scalar to pi, and the `#` inside it
///   is not a comment. (This scanner needs the escape only for SCANNING; the
///   backslash itself is never un-escaped, so the value keeps `\"` where pi's
///   `yaml` would report `"`. That gap predates the comment strip and is the same
///   "the tolerant reader does not model every YAML shape" residual ADR 0036
///   accepts — it is NOT silently widened here.)
/// - a SINGLE-quoted scalar has no backslash escape at all — its escape is a
///   DOUBLED `''`, so `''` is content and does not close the scalar either:
///   `'it''s fine' # note` closes at the LAST `'`, and a scanner that closed on
///   the second one would test the tail `s fine' # note`, find no space-then-`#`,
///   and strip NOTHING — the silent no-op on exactly the shape this function
///   exists for. The pair is then un-doubled by [`parse_value`], so the value the
///   two keys share is `it's fine`, as pi's `yaml` reports it.
///
/// TOTAL, like every other reader here: every offset it slices at is either the
/// byte index of an ASCII `#` or a boundary advanced by `char::len_utf8()`, so it
/// can never land mid-character. An unterminated quote (pi's `yaml` throws
/// `MISSING_CHAR` / `Missing closing "quote` there and drops the file) is read as
/// an unquoted scalar — fail-open, the tolerant reader's standing direction.
fn strip_trailing_comment(inline: &str) -> &str {
    if let Some(quote) = inline.chars().next().filter(|c| *c == '"' || *c == '\'') {
        let mut skip_next = false;
        // `.skip(1)` starts the scan AFTER the opening quote (`skip` counts
        // items, not bytes, and both quote chars are one byte).
        let mut chars = inline.char_indices().skip(1).peekable();
        while let Some((i, c)) = chars.next() {
            if skip_next {
                skip_next = false;
                continue;
            }
            if quote == '"' && c == '\\' {
                skip_next = true;
                continue;
            }
            if c == quote {
                // The `''` rule above: consume the SECOND quote of the pair and
                // keep scanning for the real close. (`peek`/`next` on the same
                // peekable iterator — the pair is advanced one `char` at a time,
                // so every slice offset below stays a `char_indices` boundary.)
                if quote == '\'' && chars.peek().is_some_and(|(_, p)| *p == '\'') {
                    chars.next();
                    continue;
                }
                let close = i + c.len_utf8();
                let tail = &inline[close..];
                return if tail.starts_with([' ', '\t'])
                    && tail.trim_start_matches([' ', '\t']).starts_with('#')
                {
                    &inline[..close]
                } else {
                    inline
                };
            }
        }
        // No matching close quote: not a quoted scalar. Fall through and read
        // the text as the plain scalar it looks like.
    }
    match inline
        .match_indices('#')
        .find(|(i, _)| *i == 0 || inline[..*i].ends_with([' ', '\t']))
        .map(|(i, _)| i)
    {
        Some(0) => "",
        Some(i) => inline[..i].trim_end_matches([' ', '\t']),
        None => inline,
    }
}

/// Parse the value of a top-level `key: <rest>` line (step 3).
///
/// - inline: the rest of the line, trimmed; a trailing `#` comment introduced
///   by an ASCII space or tab is stripped first, and a `#` that opens the value
///   makes the whole value empty — see [`strip_trailing_comment`]; strip one
///   pair of matching surrounding quotes (a SINGLE-quoted scalar additionally
///   un-doubles its `''` escapes, which is what its escape rule means — see
///   below); an empty inline value means "no value".
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
    // A trailing `#` comment is not part of the value (issue #11: pi strips it,
    // and a `name: a # note` left whole is a name no `$` token can address).
    // BEFORE the block-indicator match, so `description: > # note` still reads
    // as the block scalar pi reads it there.
    let inline = strip_trailing_comment(inline);
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
        let inner = &inline[1..inline.len() - 1];
        // A SINGLE-quoted scalar's `''` is an ESCAPE, not content: YAML resolves
        // the pair to one `'`, so `'it''s fine'` is `it's fine` to pi and NOT
        // `it''s fine`. This belongs to the strip above and is not optional
        // decoration — `strip_trailing_comment` now scans PAST a `''` pair to
        // find the real close, so a strip that left the pair intact would hand
        // back doubled quotes pi never produces.
        //
        // SINGLE QUOTES ONLY, deliberately: inside a DOUBLE-quoted scalar `''`
        // is two literal apostrophes (its escape is the backslash), so
        // `"it''s"` stays `it''s` for pi and stays untouched here.
        //
        // DIVERGENCE from `agents.rs`'s copy of this function, and the "apply
        // shape bugs to BOTH copies" rule it states: that copy mirrors pi's
        // SUBAGENT reader (`pi-subagents/src/agents/frontmatter.ts`), a
        // hand-rolled regex reader that slices one quote pair off and NOTHING
        // ELSE — measured against it, `description: 'it''s'` is `it''s`, quotes
        // stripped, pair left doubled. Un-doubling there would move the desktop
        // away from its only pi-side parity target, exactly as the trailing-`#`
        // strip does (that carve-out's doc names the same reason). So this
        // clause is a SECOND, named exception to the "both copies" rule: the
        // rule binds for the guards both readers share (the NBSP/dedent
        // byte-offset guard, the `: ` guard), and NOT for either of the two
        // places where the two upstream readers genuinely differ.
        if first == '\'' {
            return Some(inner.replace("''", "'"));
        }
        return Some(inner.to_string());
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

    /// A fresh scratch directory (one per call, so tests never share state).
    ///
    /// Deliberately NOT a copy of `agents.rs::scratch()`: that helper also
    /// creates an empty `.git/` dir because `discover_agents` runs a context
    /// walk that stops at a `.git`. `discover_in_roots` takes its roots as
    /// given and never walks up (the upward search lives in `space_roots`),
    /// so such a dir would be inert here and its comment a false claim.
    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("archimedes-skills-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    /// Canonicalize for path comparisons (on macOS `std::env::temp_dir()` is a
    /// symlinked PREFIX, so a raw `join` never string-equals the canonical
    /// `path` `discover_in_roots` stores). Same helper `agents.rs` uses. Use it
    /// ONLY on paths that EXIST — the fallback to the raw path is a silent
    /// no-op otherwise.
    fn canon(p: &Path) -> PathBuf {
        fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
    }

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

    // ---- symlinked Skill DIRECTORIES ------------------------------------
    //
    // Every test here is `#[cfg(unix)]`: creating a DIRECTORY symlink on
    // Windows needs privileges CI does not grant, so a Windows arm would be
    // vacuously green. The `walk` change itself is platform-neutral code and
    // compiles on all three CI OSes. (`read_skill_rejected_symlink_does_not_
    // disclose_its_target` can be dual-arity because it links a *file*.)
    //
    // Assertions are on COUNT / `name` / `scope` — never on a path string:
    // `std::env::temp_dir()` is itself a symlinked prefix on macOS.

    /// A symlinked Skill directory is a Skill farm (`~/.pi/agent/skills/*`
    /// entries are commonly links into `~/.agents/skills`), so it must be
    /// descended, not skipped as "neither a dir nor a `SKILL.md`".
    #[test]
    #[cfg(unix)]
    fn a_symlinked_skill_directory_is_discovered() {
        let root = scratch();
        let home = scratch();
        let real_dir = home.join("real");
        write_file(
            &real_dir.join("SKILL.md"),
            "---\nname: real\ndescription: D.\n---\nB.\n",
        );
        std::os::unix::fs::symlink(&real_dir, root.join("linked")).unwrap();

        let out = discover_in_roots(&[(root, SkillScope::Space)]);
        assert_eq!(out.len(), 1, "the linked Skill must be discovered: {out:?}");
        assert_eq!(out[0].name, "real");
        assert_eq!(out[0].scope, SkillScope::Space);
    }

    /// A link cycle must RETURN (the depth bounds already terminate the walk —
    /// this is not a hang fix) and must not emit the same Skill twice (the
    /// visited set prevents RE-WALKING a dir reached by two links).
    #[test]
    #[cfg(unix)]
    fn a_link_cycle_yields_one_entry_and_terminates() {
        let root = scratch();
        let loopdir = root.join("loopdir");
        write_file(
            &loopdir.join("SKILL.md"),
            "---\nname: looped\ndescription: D.\n---\nB.\n",
        );
        std::os::unix::fs::symlink(&loopdir, loopdir.join("self")).unwrap();

        let out = discover_in_roots(&[(root, SkillScope::Space)]);
        assert_eq!(
            out.iter().filter(|s| s.name == "looped").count(),
            1,
            "a cycle must not duplicate rows: {out:?}"
        );
        assert_eq!(out.len(), 1, "one Skill expected: {out:?}");
    }

    /// The depth-aware rule: skipping a directory merely because it was seen
    /// before is wrong — if it was first seen at a DEEPER walk depth, its
    /// `SKILL.md`s can sit past the `1..=4` file bound on that path and would
    /// be dropped, making discovery depend on `read_dir` order.
    ///
    /// The fixture is shaped so a PLAIN visited-set skip loses `deep_only`
    /// whenever the deeper link is enumerated first: `nest/linked` reaches the
    /// target at `dir_depth` 2, which puts the target's own `SKILL.md` at file
    /// depth 2 and `a/SKILL.md` at 3 — both in bounds — but the `dir_depth < 4`
    /// descent guard stops before `a/b/c`, so `a/b/c/SKILL.md` (file depth 4
    /// through the root-level link) is reachable ONLY at the shallow depth.
    ///
    /// Order-independence is exactly what is being pinned — and `read_dir`
    /// order is unspecified, so a fixture cannot simply be *assumed* to visit
    /// the deep link first. It matters: skipping the DEEPER second visit loses
    /// nothing (a deeper path can only push files past the bound, never bring
    /// new ones in), so a plain visited-set skip is WRONG but only shows itself
    /// when a deep link happens to be enumerated before the shallow one. The
    /// fixture therefore swamps the ONE shallow link with 64 deep ones — the
    /// link that reaches the target FIRST decides the outcome, so the losing
    /// order occurs in 64 of 65 rounds under ANY `read_dir` ordering — and the
    /// test asserts a such a round really happened, so it can never be
    /// vacuously green.
    ///
    /// WHICH ROUND PROBES WHAT. "Enumeration order" is not one property; the
    /// filesystems under CI derive it three different ways, and a round has to
    /// beat the one under which the shallow link happens to come out first.
    /// Varying ONLY creation order over one fixed name does not do that — it
    /// gives 8 identical draws on BOTH hash-ordered runners. Each round
    /// therefore varies BOTH shapes:
    /// - CREATION-ORDERED (or reverse-ordered) filesystems — measured: this
    ///   machine's tmpfs returns strict reverse creation order — are probed by
    ///   `shallow_first`, which flips the creation order of the two link groups.
    /// - NAME-SORTED filesystems (NTFS; and any unix fs that hands back a sorted
    ///   readdir) ignore creation order entirely, so the shallow link's NAME is
    ///   what has to move: `aaa_shallow` and `mid_shallow` sort BEFORE `nest0`,
    ///   `shallow` and `zzz_shallow` sort AFTER it, so two of the four name
    ///   variants put a deep link first no matter when anything was created.
    /// - NAME-HASHED filesystems (ext4 with `dir_index` = the `ubuntu-latest`
    ///   runner, and APFS = `macos-latest`) order by neither creation order nor
    ///   name but by a per-name directory hash, so every round that reuses a
    ///   name re-draws the SAME order. A name-only lottery is why the name is
    ///   load-bearing: with one fixed name all 8 rounds were 8 copies of one
    ///   ~1-in-65 draw (a fresh checkout could deterministically go
    ///   non-vacuously-red, unfixable by rerun); across 4 distinct names the
    ///   chance that every independent draw puts the shallow link first is
    ///   ~(1/65)^4.
    ///
    /// ROUNDS = 4 names × 2 creation orders is the full cross-product, so every
    /// filesystem property is probed under both creation orders, and the
    /// non-vacuity assert at the bottom proves the losing order was really hit.
    #[test]
    #[cfg(unix)]
    fn a_directory_walked_twice_keeps_the_deeper_skills_reachable() {
        const DEEP_LINKS: usize = 64;
        /// 4 shallow-link names × 2 creation orders — see the round-shape table
        /// on the test above: the name varies because hash-ordered and
        /// name-sorted filesystems are blind to creation order.
        const ROUNDS: usize = 8;
        /// Two names sorting before `nest0`, two after it — so name-sorted
        /// filesystems see a deep-first round regardless of creation order.
        const SHALLOW_NAMES: [&str; 4] = ["shallow", "aaa_shallow", "zzz_shallow", "mid_shallow"];

        /// The target holds `SKILL.md` (rel. 0), `a/SKILL.md` (rel. 1) and
        /// `a/b/c/SKILL.md` (rel. 3). Reached at `dir_depth` 2 (through a `nest`
        /// dir) the first two land at file depths 2 and 3 — in bounds — but the
        /// `dir_depth < 4` descent guard stops before `a/b`, so `deep_only`
        /// (file depth 4) is reachable ONLY through a root-level link at
        /// `dir_depth` 1.
        ///
        /// The round returns whether a deep link was enumerated BEFORE the
        /// shallow link, which is the order that a plain visited-set skip loses
        /// `deep_only` in. `shallow_first` flips the CREATION order (probing
        /// creation-ordered filesystems); `shallow_name` moves the shallow link
        /// across the `nest*` block (probing name-sorted filesystems and — since
        /// a hash-ordered fs derives its order from the name — drawing a fresh
        /// order on those). Both vary because each scheme ignores the other.
        fn fixture(shallow_name: &str, shallow_first: bool) -> (PathBuf, bool) {
            let root = scratch();
            let home = scratch();
            let target = home.join("shared");
            write_file(
                &target.join("SKILL.md"),
                "---\nname: shared\ndescription: D.\n---\nB.\n",
            );
            write_file(
                &target.join("a").join("SKILL.md"),
                "---\nname: mid\ndescription: D.\n---\nB.\n",
            );
            write_file(
                &target.join("a").join("b").join("c").join("SKILL.md"),
                "---\nname: deep_only\ndescription: D.\n---\nB.\n",
            );
            // … and ONE (`shallow`) that reaches it at `dir_depth` 1.
            let mk_deep = |i: usize| {
                let nest = format!("nest{i}");
                fs::create_dir_all(root.join(&nest)).unwrap();
                std::os::unix::fs::symlink(&target, root.join(&nest).join("link")).unwrap();
                nest
            };
            let mk_shallow = || {
                std::os::unix::fs::symlink(&target, root.join(shallow_name)).unwrap();
            };
            let mut deeps: Vec<String> = Vec::new();
            if shallow_first {
                mk_shallow();
                for i in 0..DEEP_LINKS {
                    deeps.push(mk_deep(i));
                }
            } else {
                for i in 0..DEEP_LINKS {
                    deeps.push(mk_deep(i));
                }
                mk_shallow();
            }
            // `read_dir` order is stable for a given directory, so probing it
            // here is the order `walk` will see: whichever top-level entry comes
            // first decides whether a PLAIN visited-set skip keeps `deep_only`.
            let order: Vec<String> = fs::read_dir(&root)
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect();
            let first_deep = order
                .iter()
                .position(|n| deeps.iter().any(|d| d == n))
                .unwrap();
            let first_shallow = order.iter().position(|n| n == shallow_name).unwrap();
            (root, first_deep < first_shallow)
        }

        let mut deep_first_rounds = 0;
        for round in 0..ROUNDS {
            // Each name variant gets both creation orders (4 × 2 = ROUNDS).
            let shallow_name = SHALLOW_NAMES[round % SHALLOW_NAMES.len()];
            let shallow_first = (round / SHALLOW_NAMES.len()).is_multiple_of(2);
            let (root, deep_first) = fixture(shallow_name, shallow_first);
            if deep_first {
                deep_first_rounds += 1;
            }
            let out = discover_in_roots(&[(root, SkillScope::Space)]);
            for name in ["shared", "mid", "deep_only"] {
                assert_eq!(
                    out.iter().filter(|s| s.name == name).count(),
                    1,
                    "`{name}` must be found exactly once (round {round}, deep \
                     link enumerated first = {deep_first}): {out:?}"
                );
            }
            assert_eq!(
                out.len(),
                3,
                "all three Skills expected (round {round}, deep link enumerated \
                 first = {deep_first}): {out:?}"
            );
        }
        // The round class that can expose a plain visited-set skip must really
        // have been exercised.
        assert!(
            deep_first_rounds > 0,
            "no round enumerated a deep link first — this test would prove nothing"
        );
    }

    /// Depth rules are untouched by link following: plain nesting at depths 1
    /// and 2 still yields both Skills, nothing more.
    #[test]
    fn following_links_does_not_change_plain_nesting() {
        let root = scratch();
        write_file(
            &root.join("alpha").join("SKILL.md"),
            "---\nname: alpha\ndescription: D.\n---\nB.\n",
        );
        write_file(
            &root.join("group").join("beta").join("SKILL.md"),
            "---\nname: beta\ndescription: D.\n---\nB.\n",
        );

        let out = discover_in_roots(&[(root, SkillScope::Space)]);
        assert_eq!(out.len(), 2, "both nested Skills expected: {out:?}");
    }

    /// The `node_modules` prune is evaluated on the entry NAME before any
    /// metadata call, so a link NAMED `node_modules` is still pruned.
    #[test]
    #[cfg(unix)]
    fn a_link_named_node_modules_is_still_pruned() {
        let root = scratch();
        let home = scratch();
        let hidden = home.join("hidden");
        write_file(
            &hidden.join("SKILL.md"),
            "---\nname: hidden\ndescription: D.\n---\nB.\n",
        );
        std::os::unix::fs::symlink(&hidden, root.join("node_modules")).unwrap();

        let out = discover_in_roots(&[(root, SkillScope::Space)]);
        assert_eq!(out.len(), 0, "node_modules stays pruned: {out:?}");
    }

    // ---- Shadowing: the loser is a REPORT on the winner (ADR 0065) -----
    //
    // `discover_in_roots` already dropped a name collision silently; these pin
    // the disclosure. Assertions are on `name` / `scope` and on `path` compared
    // through `canon` — never on a raw path string (`temp_dir()` is a symlinked
    // prefix on macOS).

    /// A byte-identical `SKILL.md` in two roots is a **Duplicate**, not a
    /// shadow (`CONTEXT.md`), and ADR 0065 keeps it silent: one row, empty
    /// `shadowed`. Reducing this to "any name collision is a shadow" would fill
    /// the dialog with a copy the user made on purpose.
    #[test]
    fn a_byte_identical_copy_across_roots_is_silent() {
        let space = scratch();
        let user = scratch();
        let bytes = "---\nname: discuss\ndescription: D.\n---\nSame body.\n";
        write_file(&space.join("discuss").join("SKILL.md"), bytes);
        write_file(&user.join("discuss").join("SKILL.md"), bytes);

        let out = discover_in_roots(&[(space, SkillScope::Space), (user, SkillScope::User)]);
        assert_eq!(out.len(), 1, "one row for a Duplicate: {out:?}");
        assert!(
            out[0].shadowed.is_empty(),
            "a byte-identical copy is a Duplicate, not a shadow: {out:?}"
        );
    }

    /// The comparison is on NORMALIZED content: a CRLF copy is the same Skill
    /// with a different line ending, and reporting it would bury the real
    /// shadows under Windows-checkout noise. Raw-bytes comparison fails this.
    #[test]
    fn a_crlf_copy_across_roots_is_silent() {
        let space = scratch();
        let user = scratch();
        let lf = "---\nname: discuss\ndescription: D.\n---\nSame body.\n";
        let crlf = lf.replace('\n', "\r\n");
        write_file(&space.join("discuss").join("SKILL.md"), lf);
        write_file(&user.join("discuss").join("SKILL.md"), &crlf);

        let out = discover_in_roots(&[(space, SkillScope::Space), (user, SkillScope::User)]);
        assert_eq!(out.len(), 1, "one row for a CRLF copy: {out:?}");
        assert!(
            out[0].shadowed.is_empty(),
            "a CRLF-only difference is not a shadow: {out:?}"
        );
    }

    /// THE comparison-target pin. `SkillInfo.body` is frontmatter-stripped AND
    /// trimmed, so comparing BODIES calls these two duplicates and swallows a
    /// real shadow — here the loser is model-HIDDEN while the winner is
    /// model-visible, so which file loads is a behavioural difference the user
    /// must be able to see. Compare normalized WHOLE-FILE content, never
    /// `body`. This test goes RED for exactly that substitution.
    #[test]
    fn same_body_with_a_different_disable_model_invocation_is_a_shadow() {
        let space = scratch();
        let user = scratch();
        let visible = "---\nname: discuss\ndescription: D.\n---\nIdentical body.\n";
        let hidden =
            "---\nname: discuss\ndescription: D.\ndisable-model-invocation: true\n---\nIdentical body.\n";
        write_file(&space.join("discuss").join("SKILL.md"), visible);
        write_file(&user.join("discuss").join("SKILL.md"), hidden);

        let out = discover_in_roots(&[(space, SkillScope::Space), (user, SkillScope::User)]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            !out[0].disable_model_invocation,
            "the Space winner stays model-visible: {out:?}"
        );
        assert_eq!(
            out[0].shadowed.len(),
            1,
            "a frontmatter-only difference is a real shadow: {out:?}"
        );
    }

    /// The headline contract: a genuinely different file under the same name is
    /// reported ON THE WINNER, naming the loser with its own name, its
    /// canonical path and its OWN scope — the winner and its scope are
    /// unchanged.
    #[test]
    fn a_different_file_with_the_same_name_records_the_loser() {
        let space = scratch();
        let user = scratch();
        let loser = user.join("discuss").join("SKILL.md");
        write_file(
            &space.join("discuss").join("SKILL.md"),
            "---\nname: discuss\ndescription: space version\n---\nS.\n",
        );
        write_file(
            &loser,
            "---\nname: discuss\ndescription: user version\n---\nU.\n",
        );

        let out = discover_in_roots(&[(space, SkillScope::Space), (user, SkillScope::User)]);
        assert_eq!(out.len(), 1, "the collision stays ONE entry: {out:?}");
        assert_eq!(out[0].name, "discuss");
        assert_eq!(
            out[0].scope,
            SkillScope::Space,
            "the winner's scope is unchanged: {out:?}"
        );
        assert_eq!(out[0].description, "space version", "first-wins: {out:?}");
        assert_eq!(out[0].shadowed.len(), 1, "{out:?}");
        assert_eq!(out[0].shadowed[0].name, "discuss");
        assert_eq!(out[0].shadowed[0].scope, SkillScope::User);
        assert_eq!(
            Path::new(&out[0].shadowed[0].path),
            canon(&loser).as_path(),
            "the loser's path must be canonical (compare via `canon`): {out:?}"
        );
    }

    /// A shadow is NOT necessarily "a project skill hiding your global one":
    /// `user_roots()` returns TWO roots and both are tagged `User` (likewise
    /// each Space level contributes `.agents/skills` then `.pi/skills`), so a
    /// user-scoped Skill can lose to another user-scoped one. The dialog derives
    /// its wording from the WINNER's scope, so this case must exist in the data
    /// rather than being flattened into "space wins".
    #[test]
    fn a_user_skill_shadowed_by_another_user_root_is_recorded_with_a_user_winner() {
        let agents = scratch();
        let pi = scratch();
        write_file(
            &agents.join("discuss").join("SKILL.md"),
            "---\nname: discuss\ndescription: from .agents\n---\nA.\n",
        );
        write_file(
            &pi.join("discuss").join("SKILL.md"),
            "---\nname: discuss\ndescription: from .pi\n---\nP.\n",
        );

        let out = discover_in_roots(&[(agents, SkillScope::User), (pi, SkillScope::User)]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].scope, SkillScope::User, "winner is a user skill");
        assert_eq!(out[0].shadowed.len(), 1, "{out:?}");
        assert_eq!(
            out[0].shadowed[0].scope,
            SkillScope::User,
            "the loser's REAL scope must be carried, not assumed to be space: {out:?}"
        );
    }

    /// A loser that is not ELIGIBLE to be listed is not reported as "hidden":
    /// its path carries a `"` from the directory name, so discovery refuses to
    /// surface that path at all. THIS is the test that exercises the reorder —
    /// recording the shadow at the dedupe point (above the `is_tag_safe_name`
    /// guard on `path`/`dir`) reports it and fails here.
    ///
    /// `name:` comes from frontmatter, so the directory name is NOT the rejection
    /// reason: only the path/dir guard can reject this candidate.
    #[test]
    fn a_loser_with_an_unsafe_directory_name_is_not_reported() {
        // The loser's DIRECTORY holds a `"`; its frontmatter name is clean, so
        // `parse_skill_file` accepts it and only the path/dir guard can stop it.
        //
        // The two cfg blocks are ALTERNATIVES (exactly one compiles per
        // platform, and no early `return` — the pattern, and the clippy reason,
        // stated in `tests/skills.rs`): a `"` component is LEGAL on unix and
        // REFUSED by NTFS, so on Windows the skip is unreachable-by-construction
        // rather than skipped — and the guard RULE itself stays pinned there
        // through the frontmatter route (`a_name_containing_a_quote_is_skipped`),
        // which needs no illegal name.
        #[cfg(windows)]
        {
            let user = scratch();
            let built = fs::create_dir_all(user.join("we\"ird/SKILL.md"));
            assert!(
                built.is_err(),
                "NTFS must REFUSE a `\"` component for this scenario to stay \
                 unreachable — if this ever succeeds, drop the cfg branch and \
                 run the real assertion"
            );
        }
        #[cfg(not(windows))]
        {
            let space = scratch();
            let user = scratch();
            write_file(
                &space.join("discuss").join("SKILL.md"),
                "---\nname: discuss\ndescription: space version\n---\nS.\n",
            );
            write_file(
                &user.join("we\"ird").join("SKILL.md"),
                "---\nname: discuss\ndescription: user version\n---\nU.\n",
            );

            let out = discover_in_roots(&[(space, SkillScope::Space), (user, SkillScope::User)]);
            assert_eq!(out.len(), 1, "{out:?}");
            assert!(
                out[0].shadowed.is_empty(),
                "a loser that could never be listed is not a shadow: {out:?}"
            );
        }
    }

    /// A `description` over 1024 chars is rejected INSIDE `parse_skill_file`,
    /// which runs before the dedupe either way — so unlike the tag-safety guard
    /// above, this test passes on both sides of the reorder. It is kept as a
    /// pin for the ORDER: if the description cap ever moves into the loop (next
    /// to the tag-safety guard), a shadow recorded before it would fail here.
    #[test]
    fn a_loser_over_the_description_limit_is_not_reported() {
        let space = scratch();
        let user = scratch();
        let long = "x".repeat(1025);
        write_file(
            &space.join("discuss").join("SKILL.md"),
            "---\nname: discuss\ndescription: space version\n---\nS.\n",
        );
        write_file(
            &user.join("discuss").join("SKILL.md"),
            &format!("---\nname: discuss\ndescription: {long}\n---\nU.\n"),
        );

        let out = discover_in_roots(&[(space, SkillScope::Space), (user, SkillScope::User)]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(
            out[0].shadowed.is_empty(),
            "a parse-time rejection is not a shadow: {out:?}"
        );
    }

    /// One winner can lose to several roots at once (ADR 0065 rejects the
    /// "annotate the winning row" shape for exactly this reason), so the report
    /// is a LIST, appended in root-precedence order.
    #[test]
    fn several_losers_from_several_roots_all_land_on_the_winner() {
        let space = scratch();
        let second = scratch();
        let third = scratch();
        let second_loser = second.join("discuss").join("SKILL.md");
        let third_loser = third.join("discuss").join("SKILL.md");
        write_file(
            &space.join("discuss").join("SKILL.md"),
            "---\nname: discuss\ndescription: one\n---\n1.\n",
        );
        write_file(
            &second_loser,
            "---\nname: discuss\ndescription: two\n---\n2.\n",
        );
        write_file(
            &third_loser,
            "---\nname: discuss\ndescription: three\n---\n3.\n",
        );

        let out = discover_in_roots(&[
            (space, SkillScope::Space),
            (second, SkillScope::Space),
            (third, SkillScope::User),
        ]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out[0].shadowed.len(), 2, "both losers reported: {out:?}");
        // Root-precedence order, so the dialog reads outermost-last.
        assert_eq!(
            Path::new(&out[0].shadowed[0].path),
            canon(&second_loser).as_path()
        );
        assert_eq!(
            Path::new(&out[0].shadowed[1].path),
            canon(&third_loser).as_path()
        );
        assert_eq!(out[0].shadowed[0].scope, SkillScope::Space);
        assert_eq!(out[0].shadowed[1].scope, SkillScope::User);
    }

    #[test]
    fn parse_value_is_total_over_multibyte_whitespace_and_hashes() {
        // Discovery never panics (the same contract the block-dedent comment
        // protects): every offset `strip_trailing_comment` slices at is an ASCII
        // `#` index or a boundary advanced by `char::len_utf8()`, never a
        // mid-character byte.
        for rest in [
            " a \u{A0}# note",
            " a\u{A0}# note",
            " \u{1F600}#\u{1F600}",
            " a \u{3000}# x",
            " a \u{B}# x",
            " \"a \\\" # b\"",
            " \"unclosed # keep",
            " #",
            " \"",
            " a\t# b",
        ] {
            let _ = parse_value(&[], 0, rest); // totality: whatever it returns, it must not panic
        }
        assert_eq!(parse_value(&[], 0, " a # note").as_deref(), Some("a"));
        assert_eq!(
            parse_value(&[], 0, " \u{1F600}#\u{1F600}").as_deref(),
            Some("\u{1F600}#\u{1F600}")
        );
        assert_eq!(
            parse_value(&[], 0, " \"# keep\"").as_deref(),
            Some("# keep")
        );
        assert_eq!(parse_value(&[], 0, " # note").as_deref(), None);
    }

    /// The two quote-escape rules, pinned at the value level (the comment-strip
    /// side is pinned end-to-end by `tests/skills.rs`'s
    /// `inline_values_strip_the_comment_pi_strips`).
    ///
    /// Expected values were measured against pi's own `yaml@2.9.0`, the parity
    /// target this reader mirrors. The LAST case is the accepted residual: the
    /// reader strips the comment correctly but never un-escapes `\"`, so the
    /// value keeps the backslashes pi would drop. Pinning the divergence here
    /// keeps the doc's claim honest and reddens if the gap is ever closed (in
    /// which case this row moves, and the doc sentence with it).
    #[test]
    fn each_quote_style_follows_its_own_yaml_escape_rule() {
        // Single-quoted: `''` is the escape — it does not close the scalar, and
        // the pair un-doubles in the value.
        assert_eq!(
            parse_value(&[], 0, " 'it''s fine' # note").as_deref(),
            Some("it's fine")
        );
        assert_eq!(
            parse_value(&[], 0, " 'don''t # say' # note").as_deref(),
            Some("don't # say"),
            "a `#` INSIDE the scalar is content; only the comment after the real \
             closing quote is stripped"
        );
        assert_eq!(parse_value(&[], 0, " 'it''s'").as_deref(), Some("it's"));
        assert_eq!(parse_value(&[], 0, " '''").as_deref(), Some("'"));
        // Double-quoted: the BACKSLASH is the escape (the `#` after it is inside
        // the scalar), and a `''` pair inside a double-quoted scalar is TWO LITERAL
        // apostrophes — YAML has no doubling rule there, so nothing un-doubles.
        assert_eq!(
            parse_value(&[], 0, " \"a \\\" # b\"").as_deref(),
            Some("a \\\" # b"),
            "the escaped quote does not close the scalar, so the `#` is scalar text"
        );
        assert_eq!(parse_value(&[], 0, " \"it''s\"").as_deref(), Some("it''s"));
        // The accepted residual: `\"` is never un-escaped (pi gives `a " b`).
        assert_eq!(
            parse_value(&[], 0, " \"a \\\" b\" # note").as_deref(),
            Some("a \\\" b"),
            "the comment is stripped and the quotes removed, but the backslash \
             escape is NOT resolved — the documented fail-open gap"
        );
    }
}

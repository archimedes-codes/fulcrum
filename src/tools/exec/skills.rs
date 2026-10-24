use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::agent::fs_backend::{FsBackend, FsError};
use crate::agent::tools::types::{ToolCtx, ToolResult};

/// The ONE discovery call the skill tools make per tool call (see
/// [`ToolCtx::skill_roots`]): the pinned roots when set (the test seam),
/// otherwise the SAME discovery call the prompt builder makes — ADR 0029's
/// no-drift seam, which is about ADVERTISEMENT: what the tools and the
/// `<skills>` section advertise is exactly the `model_visible` SUBSET of this
/// set (ADR 0036). The set itself is UNFILTERED, and resolution is
/// deliberately WIDER than advertisement: `exec_read_skill` resolves against
/// this FULL set on purpose, so it can tell a model-hidden name from an
/// unknown one and answer each with its own message rather than a lie.
pub(crate) fn skills_for(ctx: &ToolCtx) -> Vec<crate::skills::SkillInfo> {
    match ctx.skill_roots.as_ref() {
        Some(roots) => crate::skills::discover_in_roots(roots),
        None => crate::skills::discover_skills(Some(&ctx.cwd)),
    }
}

/// The skills the tools may ADVERTISE (ADR 0036): `skills_for` minus the
/// model-hidden ones. `read_skill` does NOT use this — it resolves against
/// `skills_for` so it can distinguish hidden from unknown.
pub(crate) fn model_skills_for(ctx: &ToolCtx) -> Vec<crate::skills::SkillInfo> {
    skills_for(ctx)
        .into_iter()
        .filter(|s| s.model_visible())
        .collect()
}

/// List the skills the Agent may use — the catalog the `<skills>` system
/// prompt section advertises, resolved through the SAME discovery call and
/// filtered on the SAME predicate, so the two cannot drift in WHAT they filter
/// (ADR 0029 + ADR 0036): a model-hidden skill is simply ABSENT here. They DO
/// differ in WHEN — this list is re-discovered per call while the prompt
/// section is frozen at seq 0, so a session can list a set its own prompt no
/// longer matches (or does not yet); [`ToolCtx::skill_roots`] and ADR 0036 own
/// that divergence.
///
/// Skills live OUTSIDE the session `cwd` (user skills are in
/// `~/.agents/skills` / `~/.pi/agent/skills`); these tools are the sanctioned
/// route to them — keyed by NAME and contained to the skill's own directory
/// (ADR 0029). Whether a `read` reaches a skill path is NOT decided here: it
/// is the **Boundary**'s call, and ADR 0030 grants the read Boundary the
/// discovery roots. The discovered set is this tool's allowlist.
///
/// `params`: `{}`.
pub async fn exec_list_skills(ctx: &ToolCtx, _params: &Value) -> ToolResult {
    let skills = model_skills_for(ctx);
    if skills.is_empty() {
        return ToolResult::ok_text("none".to_string(), Some(json!({ "skills": 0 })));
    }
    let lines: Vec<String> = skills
        .iter()
        .map(|s| {
            // A block-scalar `description` can hold newlines — flatten it so
            // the one-line-per-skill contract holds (as `list_agents` does).
            let description = s.description.replace('\n', " ");
            format!("{} ({}): {}", s.name, s.scope, description)
        })
        .collect();
    ToolResult::ok_text(lines.join("\n"), Some(json!({ "skills": skills.len() })))
}

/// Read a skill's `SKILL.md` (no `path`) or one of its bundled files
/// (`path`, resolved against the skill's directory).
///
/// The `name` must match a discovered skill — that is the allowlist, so no
/// arbitrary path is ever reachable. A model-hidden skill DOES resolve (so
/// the model is not told a lie) but is REFUSED per hit, before any read, with
/// the sanctioned user route named back at it — the `$name` Mention, but ONLY
/// when the token grammar can carry that name (ADR 0031); otherwise the
/// refusal says the Skill must be loaded explicitly, so it never promises a
/// dead token (ADR 0036) — so neither its `SKILL.md` nor any bundled file is
/// ever read. Unlike `list_skills`, which filters the LIST, this filters the
/// HIT: it needs the unfiltered set to tell hidden
/// apart from unknown. Both filter the same predicate, so the tools' catalog
/// still cannot drift from the prompt's catalog in WHAT is filtered (ADR 0029);
/// the frozen prompt vs the per-call tool (`WHEN`) is the caveat
/// [`ToolCtx::skill_roots`] carries.
///
/// With a `path`, the read is scoped to the skill's OWN directory via a
/// per-skill [`FsBackend`]: `..` traversal, an absolute path, and a symlink
/// out of the dir are all rejected, and a sibling skill's files are NOT
/// reachable (the boundary is the skill dir, not the skill root).
///
/// `params`: `{ name: String, path?: String }`.
pub async fn exec_read_skill(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(name) = params.get("name").and_then(|v| v.as_str()) else {
        return ToolResult::fail("read_skill: missing `name` parameter".to_string(), None);
    };
    let skills = skills_for(ctx);
    let key = name.to_lowercase();
    let Some(skill) = skills.iter().find(|s| s.name.to_lowercase() == key) else {
        return ToolResult::fail(
            format!("read_skill: unknown skill {name:?} — call list_skills for the names"),
            None,
        );
    };
    if !skill.model_visible() {
        // ADR 0029's disclosure rule holds for BOTH shapes: name ONLY what the
        // model sent — no path, no dir, no scope (ADR 0036: this runs BEFORE
        // the `FsBackend` below exists, so nothing is read).
        //
        // The `$mention` route is promised ONLY when the grammar can actually
        // carry the name. The `$` token is `[a-z0-9]+(-[a-z0-9]+)*` (ADR 0031)
        // while discovery screens a name merely for tag-safety, so a legal name
        // like `has space` / `foo_bar` is TYPEABLE NOWHERE: promising
        // `$has space` would send the model (and the user, via the dialog
        // tooltip) to a token that silently expands NOTHING.
        //
        // This predicate filters the `@` and `#` picker catalogs
        // (`commands::agents::list_agent_definitions_for_space`,
        // `agent::mcp::config::server_infos`). It does NOT filter the `$` SKILL
        // catalog: those rows are built from the FULL discovery output with
        // only a name-substring filter (`ChatStream.tsx`), deliberately — the
        // Client's skill catalog must stay total or this flag would delete the
        // user's own route (ADR 0036). So for Skills the predicate is enforced
        // in FOUR places, and this refusal is one of them: the Skills-dialog
        // tooltip (`manualOnlyTooltip`, which promises a `$name` only when the
        // grammar can carry it), that dialog's ROW AFFORDANCE (an unmentionable
        // row renders inert — issue #10), `insertSkill`'s guard before it
        // dispatches the token (issue #10), and here. All three Client sides
        // branch on `isMentionableSkillName` in `src/lib/skills.ts`, whose
        // "where it is applied" paragraph is the count to keep in step with
        // this one. The `$` picker still CAN offer a name this tool refuses.
        // That is the accepted shape, not a drift to fix.
        let route = if crate::skills::is_mentionable_name(&skill.name) {
            // `${key}` is LOWERCASED on purpose: the token grammar is
            // lowercase-only, so `$DeBuG` expands nothing while `$debug` does.
            format!("ask the user to send ${key}")
        } else {
            // NO `$` anywhere in this branch, and no absolute claim. The
            // predicate is the ASCII token grammar, while the Client's match is
            // a JS case-FOLD, so "it cannot be typed" is false for exactly one
            // input (a name holding U+212A lowercases to `k` in JS and WOULD
            // expand, while `is_ascii_alphanumeric` rejects it here) — the
            // accurate statement is that the name is OUTSIDE THE GRAMMAR. And a
            // literal `$` in the sentence is a token the model can still copy.
            "ask the user to load this Skill explicitly (its name is outside the \
             mention token grammar)"
                .to_string()
        };
        return ToolResult::fail(
            format!("read_skill: {name:?} is model-hidden — the Agent may not load it; {route}"),
            None,
        );
    }

    // The per-skill mini-sandbox: the skill's dir is the boundary. NOT the
    // session boundary (ADR 0030): a skill must not be able to read a
    // sibling skill, so this backend sees exactly ONE root and no deny-list.
    let backend = FsBackend {
        roots: vec![PathBuf::from(&skill.dir)],
    };
    let Some(rel) = params.get("path").and_then(|v| v.as_str()) else {
        // No `path` → the `SKILL.md` BODY (discovery already stripped the
        // frontmatter — exactly what progressive disclosure wants).
        return ToolResult::ok_text(
            skill.body.clone(),
            Some(json!({
                "skill": skill.name,
                "scope": skill.scope.to_string(),
                "dir": skill.dir,
            })),
        );
    };

    let resolved = match backend.validate(Path::new(rel)) {
        Ok(p) => p,
        Err(FsError::PathEscape { .. }) => {
            // NEVER surface the `FsError` here: `PathEscape` carries the
            // CANONICALIZED path, which for a symlink INSIDE the skill dir
            // is its target OUTSIDE it — information the model never
            // supplied (it named only `link`). Echoing it turns a
            // repo-shipped skill (`data.txt` -> `~/.ssh/id_rsa`) into a
            // path-disclosure oracle. `read` may echo a path the model
            // itself passed; `read_skill` must never invent one. Name only
            // what the model sent + the boundary it already knows.
            return ToolResult::fail(
                format!(
                    "read_skill: `{rel}` resolves outside the skill directory {} — a skill's `path` must stay inside its own directory (a symlink that leaves it is rejected)",
                    skill.dir
                ),
                None,
            );
        }
        // An `Io` failure (missing file, a directory, bad encoding) discloses
        // nothing the model did not already name — safe to surface verbatim.
        Err(e) => return ToolResult::fail(format!("read_skill: {e}"), None),
    };
    match backend.read(&resolved) {
        Ok(text) => ToolResult::ok_text(
            text,
            Some(json!({
                "skill": skill.name,
                "path": rel,
            })),
        ),
        Err(e) => ToolResult::fail(format!("read_skill: {e}"), None),
    }
}

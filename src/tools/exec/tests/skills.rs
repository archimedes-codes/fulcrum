use std::path::{Path, PathBuf};

use serde_json::json;

use super::support::*;
use crate::agent::tools::exec::execute_tool;
use crate::agent::tools::types::ToolCtx;

// ── list_skills / read_skill ─────────────────────────────────────────

/// A skill root holding `<name>/SKILL.md`; returns the root.
fn skill_root(tag: &str, name: &str, content: &str) -> PathBuf {
    let root = temp_cwd(tag);
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), content).unwrap();
    root
}

/// `roots` as the ONLY discovery roots — the skill tools take their
/// roots from `ToolCtx::skill_roots` so a test never reads the real
/// `~/.agents/skills` (the `discover_in_roots` convention). The policy is
/// the same `Sandboxed` containment tier as [`ctx`], whose `boundary` is
/// the `cwd` ALONE — a FIXTURE choice, not the shipped read Boundary,
/// which ADR 0030 widens with the discovery roots. So a `read` of a skill
/// path is out of bounds HERE only because of this fixture's root set;
/// nothing below is a claim about what `read` does in a real session.
fn ctx_with_roots(cwd: &Path, roots: &[PathBuf]) -> ToolCtx {
    ToolCtx {
        skill_roots: Some(
            roots
                .iter()
                .map(|r| (r.clone(), crate::skills::SkillScope::Space))
                .collect(),
        ),
        ..ctx(cwd)
    }
}

#[tokio::test]
async fn list_skills_lists_name_scope_and_description() {
    let cwd = temp_cwd("skills-list");
    let root = skill_root(
        "skills-list-root",
        "alpha",
        "---\nname: alpha\ndescription: Does alpha.\n---\nBody.\n",
    );
    let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
    assert!(!r.is_error);
    assert_eq!(text_of(&r), "alpha (space): Does alpha.");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn list_skills_empty_is_none_not_error() {
    let cwd = temp_cwd("skills-list-empty");
    let root = temp_cwd("skills-list-empty-root");
    let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
    assert!(!r.is_error, "an empty catalog is not a failure");
    assert_eq!(text_of(&r), "none");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn list_skills_omits_model_hidden_skills() {
    // ADR 0036: to the Agent a model-hidden skill is NOT in the catalog —
    // its name AND its description (the routing hint) are both absent.
    let cwd = temp_cwd("skills-list-hidden");
    let a = skill_root(
        "tag-a",
        "alpha",
        "---\nname: alpha\ndescription: D\n---\nA.\n",
    );
    let o = skill_root(
        "tag-o",
        "omega",
        "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
    );
    let r = execute_tool(&ctx_with_roots(&cwd, &[a, o]), "list_skills", &json!({})).await;
    assert!(!r.is_error, "{}", text_of(&r));
    assert_eq!(text_of(&r), "alpha (space): D");
    assert!(!text_of(&r).contains("omega"), "{}", text_of(&r));
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn list_skills_is_none_when_every_skill_is_model_hidden() {
    // A Space can present an EMPTY catalog to the Agent while the Skills
    // dialog shows a full one — that is the feature, and an empty catalog
    // is still not a failure (cf. `list_skills_empty_is_none_not_error`).
    let cwd = temp_cwd("skills-list-all-hidden");
    let root = skill_root(
        "skills-list-all-hidden-root",
        "omega",
        "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
    );
    let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
    assert!(!r.is_error, "an empty catalog is not a failure");
    assert_eq!(text_of(&r), "none");
    let _ = std::fs::remove_dir_all(&cwd);
}

/// The names the `<skills>` section ADVERTISES, in the prompt (the
/// `<name>` values, inside the section ONLY — the rest of the prompt must
/// not be scanned, or a skill name echoed in another section would count).
fn advertised_in_prompt(prompt: &str) -> std::collections::BTreeSet<String> {
    let section = prompt
        .split("<skills>\n")
        .nth(1)
        .and_then(|rest| rest.split("\n</skills>").next())
        .expect("the `<skills>` section is present (`read_skill` is advertised)");
    section
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("<name>")
                .and_then(|s| s.strip_suffix("</name>"))
                .map(str::to_string)
        })
        .collect()
}

/// The names the `list_skills` output ADVERTISES: one
/// `{name} ({scope}): {description}` line each (`none` → the empty set).
fn advertised_by_tool(text: &str) -> std::collections::BTreeSet<String> {
    if text == "none" {
        return std::collections::BTreeSet::new();
    }
    text.lines()
        .map(|line| {
            line.split_once(" (")
                .map(|(name, _)| name.to_string())
                .unwrap_or_else(|| panic!("unexpected `list_skills` line {line:?}"))
        })
        .collect()
}

/// A `ToolSpec` of the shape `prompt.rs`'s test helper builds (the
/// `parameters` schema is irrelevant here — only the NAME decides whether
/// the `<skills>` section is emitted at all).
fn spec(name: &str) -> crate::agent::harness::provider::ToolSpec {
    crate::agent::harness::provider::ToolSpec {
        name: name.to_string(),
        description: format!("The {name} tool."),
        parameters: serde_json::Value::Null,
    }
}

/// THE anti-drift test (ADR 0029's "the tool's catalog cannot drift from
/// the prompt's catalog" + ADR 0036's "to the Agent a model-hidden Skill
/// is not in the catalog").
///
/// The two CATALOGS are produced by two SEPARATE seams — the prompt's
/// `format_skills_for_prompt` (`harness/prompt.rs`) and the tools'
/// `model_skills_for` → `exec_list_skills` (here) — which today share only
/// the `model_visible` PREDICATE. Every other ADR 0036 test checks ONE seam,
/// so either seam growing its own second filter (a scope test, a count cap,
/// a re-filter of an already-filtered list) would leave the OTHER advertising
/// a skill the model was never told about — or, worse, promise a skill that
/// `read_skill` then refuses. This gives BOTH seams the same discovery ROOTS
/// (the prompt builder is handed the discovered rows directly; the tools
/// RE-DISCOVER from those same roots, so the rows are equal by construction
/// and not by sharing a `Vec`), and asserts the two advertised name sets are
/// EQUAL, so the drift is a test failure and not a transcript surprise. The
/// mixed visible/hidden × Space/User fixture is what gives the equality its
/// teeth: a per-seam scope filter, or a seam that stops filtering on the
/// flag, both break the set equality rather than passing silently.
///
/// It also holds the **Shadowing** case (ADR 0065): a same-named Space+User
/// pair with different bytes, where the loser is model-VISIBLE. A shadow is a
/// report on the winner and must stay out of BOTH catalogs, so this fixture
/// fails if either seam ever advertises a shadowed loser — which is why
/// extending THIS pin beats writing a parallel one.
#[tokio::test]
async fn the_prompt_catalog_and_the_list_skills_catalog_agree() {
    use crate::agent::harness::prompt::{build_main_prompt, PromptContext};
    use crate::skills::{discover_in_roots, SkillScope};

    // The prompt builder walks UP from `cwd` for `AGENTS.md`s; an empty
    // `.git` here bounds that walk at this scratch dir (the `prompt.rs`
    // `scratch()` convention) so a stray `AGENTS.md` in `/tmp` cannot
    // inject a `<project_context>` section (which could mention a skill
    // name and pollute nothing here, but keeps the fixture honest).
    let cwd = temp_cwd("skills-catalog-drift");
    std::fs::create_dir_all(cwd.join(".git")).unwrap();

    // One skill per root (`skill_root` makes exactly one), six roots:
    // visible + hidden, each at BOTH scopes, PLUS a same-named Space/User pair
    // whose bytes DIFFER — the **Shadowing** case (ADR 0065). Its name may
    // appear in NEITHER advertised set below: a shadowed loser stays
    // unaddressable, and the report rides on the winner as data the prompt
    // never interpolates. If a future change advertised `shadowed` (or listed
    // the loser), the `visible` set below stops matching and this fails.
    let roots: Vec<(PathBuf, SkillScope)> = vec![
        (
            skill_root(
                "drift-visible-space",
                "alpha",
                "---\nname: alpha\ndescription: Does alpha.\n---\nA.\n",
            ),
            SkillScope::Space,
        ),
        (
            skill_root(
                "drift-visible-user",
                "beta",
                "---\nname: beta\ndescription: Does beta.\n---\nB.\n",
            ),
            SkillScope::User,
        ),
        (
            skill_root(
                "drift-hidden-space",
                "omega",
                "---\nname: omega\ndescription: Does omega.\ndisable-model-invocation: true\n---\nO.\n",
            ),
            SkillScope::Space,
        ),
        (
            skill_root(
                "drift-hidden-user",
                "zeta",
                "---\nname: zeta\ndescription: Does zeta.\ndisable-model-invocation: true\n---\nZ.\n",
            ),
            SkillScope::User,
        ),
        // The **Shadowing** pair: one name, two roots, DIFFERENT bytes, so the
        // second is a shadowed loser and not a Duplicate. Both are
        // model-HIDDEN, which is what makes this a zero-drift fixture: the
        // winner is hidden and the loser is unaddressable, so `gamma` may appear
        // in NEITHER advertised set — the two catalogs must stay byte-identical
        // to the pre-shadow ones while `skills` grows by one row.
        // The **Shadowing** pair: one name, two roots, DIFFERENT bytes, so the
        // second is a shadowed loser and not a Duplicate. Shaped deliberately:
        // the WINNER is model-HIDDEN and the LOSER is model-VISIBLE. The loser
        // carries an advertisable `description`, so it is the case a naive
        // "make the loser a row" implementation would advertise — and the
        // `visible` set below (UNCHANGED: `gamma` joins neither catalog) is what
        // reddens it, while the winner's own hiddenness keeps `gamma` out of
        // both catalogs legitimately (ADR 0036 + ADR 0065).
        (
            skill_root(
                "drift-shadow-space",
                "gamma",
                "---\nname: gamma\ndescription: Does gamma (space).\ndisable-model-invocation: true\n---\nG.\n",
            ),
            SkillScope::Space,
        ),
        (
            skill_root(
                "drift-shadow-user",
                "gamma",
                "---\nname: gamma\ndescription: Does gamma (user).\n---\nG other.\n",
            ),
            SkillScope::User,
        ),
    ];
    // The SAME rows for both seams: the prompt builder is handed the
    // discovered set directly, and the tool ctx takes the SAME roots (the
    // tools re-discover them through `skills_for` — `skill_roots` is the
    // pinned-root seam, never a pre-filtered list, which is exactly what
    // this test is guarding).
    let skills = discover_in_roots(&roots);
    // Five ROWS, not six: the shadowed loser is a report on the winner
    // (ADR 0065), never an entry of its own.
    assert_eq!(
        skills.len(),
        5,
        "the fixture must discover all five rows (the premise): the four \
         single-root skills plus ONE row for the shadowed pair"
    );
    let paths: Vec<PathBuf> = roots.iter().map(|(p, _)| p.clone()).collect();

    // Seam 1: the prompt's `<skills>` section. `read_skill` is advertised so
    // the section is emitted at all (ADR 0017's skill-loading-tool guard).
    let tool_specs = vec![spec("read_skill"), spec("list_skills")];
    let prompt = build_main_prompt(&PromptContext {
        cwd: &cwd,
        // `Path::new("")` skips the global context file (the documented
        // convention: `build_main_prompt` treats an empty dir as "none").
        agent_dir: std::path::Path::new(""),
        tools: &tool_specs,
        skills: &skills,
    });
    let prompt_names = advertised_in_prompt(&prompt);

    // Seam 2: the `list_skills` output, from the SAME roots. Built from the
    // SAME `ctx` helper as `ctx_with_roots` — but pinned per-root instead of
    // all-`Space`, because the fixture needs BOTH scopes (a per-seam scope
    // filter is one of the drifts this test must catch).
    let ctx = ToolCtx {
        skill_roots: Some(roots.clone()),
        ..ctx(&cwd)
    };
    let r = execute_tool(&ctx, "list_skills", &json!({})).await;
    assert!(!r.is_error, "{}", text_of(&r));
    let tool_names = advertised_by_tool(text_of(&r));

    // THE anti-drift assertion.
    assert_eq!(
        prompt_names, tool_names,
        "the `<skills>` section and `list_skills` advertise DIFFERENT catalogs"
    );
    // And neither may name a model-hidden skill (ADR 0036): the equality
    // above alone would hold for two identically-WRONG catalogs.
    //
    // `gamma` is in NEITHER set on purpose (ADR 0065): its Space row is
    // model-hidden, and the model-VISIBLE user row that lost the name is a
    // shadowed REPORT, not an entry — so a shadow stays unaddressable. Adding
    // `gamma` here would mean the report leaked into a model-facing seam.
    let visible: std::collections::BTreeSet<String> = ["alpha".to_string(), "beta".to_string()]
        .into_iter()
        .collect();
    assert_eq!(
        prompt_names, visible,
        "the advertised catalog is not exactly the model-visible set"
    );
    // The fixture must really contain a SHADOW (not two unrelated skills):
    // otherwise the two assertions above would be pinning nothing about ADR
    // 0065. One row, carrying the loser.
    let gamma = skills
        .iter()
        .find(|s| s.name == "gamma")
        .unwrap_or_else(|| panic!("the shadowed pair must leave one row: {skills:?}"));
    assert_eq!(
        gamma.shadowed.len(),
        1,
        "the user row must be reported on the Space winner: {skills:?}"
    );

    for root in paths {
        let _ = std::fs::remove_dir_all(&root);
    }
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_reads_skill_md_without_touching_the_sandbox() {
    // THE regression: the skill lives OUTSIDE `ctx.cwd` (the real-world
    // case — user skills live in `~/.agents/skills`), so `read` rejects
    // it and `read_skill` must not.
    let cwd = temp_cwd("skill-outside");
    let root = skill_root(
        "skill-outside-root",
        "alpha",
        "---\nname: alpha\ndescription: Does alpha.\n---\nUse the alpha way.\n",
    );
    let skill_md = root.join("alpha/SKILL.md");
    assert!(
        !skill_md.starts_with(&cwd),
        "the fixture must sit outside the session sandbox"
    );
    // The premise: the sandboxed `read` genuinely rejects it.
    let escaped = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read",
        &json!({ "path": skill_md }),
    )
    .await;
    assert!(escaped.is_error, "`read` must still reject the escape");

    let r = execute_tool(
        &ctx_with_roots(&cwd, &[root]),
        "read_skill",
        &json!({ "name": "alpha" }),
    )
    .await;
    assert!(!r.is_error, "{}", text_of(&r));
    assert_eq!(text_of(&r), "Use the alpha way.");
    assert_eq!(r.details.as_ref().unwrap()["skill"], "alpha");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_name_is_case_insensitive_and_flattens_nothing() {
    let cwd = temp_cwd("skill-case");
    let root = skill_root("skill-case-root", "Alpha", "---\nname: Alpha\n---\nBody.\n");
    let r = execute_tool(
        &ctx_with_roots(&cwd, &[root]),
        "read_skill",
        &json!({ "name": "alpha" }),
    )
    .await;
    assert!(!r.is_error);
    assert_eq!(text_of(&r), "Body.");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_bundled_file_resolves_relative_to_the_skill_dir() {
    let cwd = temp_cwd("skill-bundle");
    let root = skill_root(
        "skill-bundle-root",
        "discuss",
        "---\nname: discuss\n---\nSee [adr](./adr-format.md).\n",
    );
    std::fs::write(root.join("discuss/adr-format.md"), "# ADR format\n").unwrap();
    let r = execute_tool(
        &ctx_with_roots(&cwd, &[root]),
        "read_skill",
        &json!({ "name": "discuss", "path": "./adr-format.md" }),
    )
    .await;
    assert!(!r.is_error, "{}", text_of(&r));
    // A bundled file is returned VERBATIM (the file's own bytes, unlike
    // `read`'s line-join) — the whole point is reaching the exact file a
    // skill references.
    assert_eq!(text_of(&r), "# ADR format\n");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_bundled_escape_is_rejected() {
    // A `path` that leaves the SKILL's dir must be rejected even though
    // it stays inside a sibling skill — the boundary is the skill dir,
    // not the skill ROOT.
    let cwd = temp_cwd("skill-esc");
    let root = skill_root("skill-esc-root", "alpha", "---\nname: alpha\n---\nA.\n");
    std::fs::write(root.join("secret.txt"), "top secret").unwrap();
    let r = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read_skill",
        &json!({ "name": "alpha", "path": "../secret.txt" }),
    )
    .await;
    assert!(r.is_error, "an escape from the skill dir must be rejected");
    assert!(
        !text_of(&r).contains("top secret"),
        "the escaped content must not leak: {}",
        text_of(&r)
    );
    // An ABSOLUTE path outside the skill dir is rejected too.
    let outside = std::env::temp_dir().join("definitely-not-in-the-skill.txt");
    let r = execute_tool(
        &ctx_with_roots(&cwd, &[root]),
        "read_skill",
        &json!({ "name": "alpha", "path": outside }),
    )
    .await;
    assert!(
        r.is_error,
        "an absolute path outside the skill dir is rejected"
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_unknown_name_is_error_with_the_catalog() {
    let cwd = temp_cwd("skill-unknown");
    let root = skill_root("skill-unknown-root", "alpha", "---\nname: alpha\n---\nA.\n");
    let r = execute_tool(
        &ctx_with_roots(&cwd, &[root]),
        "read_skill",
        &json!({ "name": "nope" }),
    )
    .await;
    assert!(r.is_error);
    let text = text_of(&r);
    assert!(text.contains("unknown skill"), "{text}");
    // The hint points at the tool that WOULD have listed them.
    assert!(text.contains("list_skills"), "{text}");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_rejects_a_model_hidden_skill_with_the_mention_route() {
    // A hidden name resolves (so the model is not lied to) but is refused,
    // and the failure names the ONE route that works: the user's `$name`
    // Mention. The whole string is pinned — it is the model's only
    // instruction about that route (ADR 0036).
    let cwd = temp_cwd("skill-hidden-route");
    let root = skill_root(
        "skill-hidden-route-root",
        "omega",
        "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
    );
    let r = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read_skill",
        &json!({ "name": "omega" }),
    )
    .await;
    assert!(r.is_error, "a model-hidden skill must be refused");
    assert_eq!(
        text_of(&r),
        "read_skill: \"omega\" is model-hidden — the Agent may not load it; ask the user to send $omega"
    );

    // The `$` Mention token grammar is LOWERCASE-only (ADR 0031), so the
    // route echoed back is the lowercased key: `$DeBuG` expands nothing
    // while `$debug` does.
    let dbg = skill_root(
        "skill-hidden-route-debug",
        "DeBuG",
        "---\nname: DeBuG\ndisable-model-invocation: true\n---\nD.\n",
    );
    let r = execute_tool(
        &ctx_with_roots(&cwd, &[dbg]),
        "read_skill",
        &json!({ "name": "debug" }),
    )
    .await;
    assert!(r.is_error);
    assert!(
        text_of(&r).ends_with("ask the user to send $debug"),
        "{}",
        text_of(&r)
    );
    assert!(
        !text_of(&r).contains(&root.to_string_lossy().into_owned()),
        "the failure must not disclose a path: {}",
        text_of(&r)
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_refuses_a_non_mentionable_hidden_skill_without_a_dead_token() {
    // A skill name is only screened by `is_tag_safe_name`, so `has space`
    // IS discovered (a space is tag-safe) — yet the `$` token grammar
    // cannot carry it (ADR 0031), so the usual refusal would point the
    // model AND the user at a `$has space` token that expands NOTHING.
    // The refusal must therefore stay actionable without promising a
    // token: no `$mention` suggestion at all.
    //
    // (Discovery note: the frontmatter `name` WINS over the
    // directory-name fallback, and `is_tag_safe_name` admits a space, so
    // this frontmatter is the path that actually yields a hidden,
    // non-mentionable skill.)
    let cwd = temp_cwd("skill-hidden-unmentionable");
    let root = skill_root(
        "skill-hidden-unmentionable-root",
        "has-space",
        "---\nname: has space\ndescription: D\ndisable-model-invocation: true\n---\nH.\n",
    );
    let r = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read_skill",
        &json!({ "name": "has space" }),
    )
    .await;
    assert!(r.is_error, "a model-hidden skill must be refused");
    let text = text_of(&r);
    assert!(text.contains("model-hidden"), "{text}");
    assert!(text.contains("has space"), "{text}");
    // The branch is REALLY taken: the refusal carries NO `$` at all — not
    // the pinned `send $<name>` tail, and not a `$mention` mentioned in
    // prose (a `$` in a message whose whole point is "this name has no
    // token" is a dead token the model can still copy).
    assert!(
        !text.contains('$'),
        "must not promise an untypeable $mention token: {text}"
    );
    // And no absolute claim: `is_mentionable_name` is the ASCII grammar, so
    // a name it rejects can still be expandable client-side (U+212A
    // lowercases to `k` in JS), which makes "cannot be typed" false.
    assert!(
        !text.contains("cannot be typed"),
        "the refusal must not state an impossibility: {text}"
    );
    assert!(
        text.contains("outside the mention token grammar"),
        "the refusal must name the real reason without a `$`: {text}"
    );
    assert!(
        text.contains("explicitly"),
        "the refusal must still name a route the user can take: {text}"
    );
    // ADR 0029: name ONLY what the model sent — no path, no dir, no scope.
    assert!(
        !text.contains(&root.to_string_lossy().into_owned()),
        "the failure must not disclose a path: {text}"
    );
    assert!(
        !text.contains("has-space"),
        "the failure must not disclose the directory name: {text}"
    );
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_rejects_a_model_hidden_skill_before_reading_a_bundled_file() {
    // Enforcement lands BEFORE any `FsBackend` is built, so a hidden
    // skill's bundled files are never read — not merely reported as
    // escaped.
    let cwd = temp_cwd("skill-hidden-bundle");
    let hidden = skill_root(
        "skill-hidden-bundle-hidden",
        "omega",
        "---\nname: omega\ndisable-model-invocation: true\n---\nO.\n",
    );
    std::fs::write(hidden.join("omega/ref.md"), "HIDDEN-BUNDLED-BODY\n").unwrap();
    // The visible CONTROL: a visible skill's bundled file still reads, so
    // this test cannot pass merely because the bundled path is broken.
    let visible = skill_root(
        "skill-hidden-bundle-visible",
        "alpha",
        "---\nname: alpha\n---\nA.\n",
    );
    std::fs::write(visible.join("alpha/ref.md"), "CONTROL-BODY\n").unwrap();

    let r = execute_tool(
        &ctx_with_roots(&cwd, &[hidden]),
        "read_skill",
        &json!({ "name": "omega", "path": "ref.md" }),
    )
    .await;
    assert!(r.is_error, "a model-hidden skill must be refused");
    assert_eq!(
        text_of(&r),
        "read_skill: \"omega\" is model-hidden — the Agent may not load it; ask the user to send $omega"
    );
    assert!(
        !text_of(&r).contains("HIDDEN-BUNDLED-BODY"),
        "the bundled body must never be returned: {}",
        text_of(&r)
    );
    assert!(
        !text_of(&r).contains("resolves outside the skill directory"),
        "the hidden branch must run before the containment check: {}",
        text_of(&r)
    );

    let c = execute_tool(
        &ctx_with_roots(&cwd, &[visible]),
        "read_skill",
        &json!({ "name": "alpha", "path": "ref.md" }),
    )
    .await;
    assert!(!c.is_error, "{}", text_of(&c));
    assert_eq!(text_of(&c), "CONTROL-BODY\n");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_missing_name_param_is_error() {
    let cwd = temp_cwd("skill-noparam");
    let r = execute_tool(&ctx(&cwd), "read_skill", &json!({})).await;
    assert!(r.is_error);
    assert!(text_of(&r).contains("`name`"), "{}", text_of(&r));
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_bundled_binary_is_an_error_not_garbage() {
    // `FsBackend::read` is `read_to_string`, so a bundled IMAGE fails
    // (the documented ADR 0029 limitation — unlike `read`'s image path).
    let cwd = temp_cwd("skill-binary");
    let root = skill_root("skill-binary-root", "alpha", "---\nname: alpha\n---\nA.\n");
    std::fs::write(root.join("alpha/pic.png"), [0x89, 0x50, 0x4e, 0x47, 0x00]).unwrap();
    let r = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read_skill",
        &json!({ "name": "alpha", "path": "./pic.png" }),
    )
    .await;
    assert!(r.is_error, "a non-UTF-8 bundled file is an error");
    let _ = std::fs::remove_dir_all(&cwd);
}

#[tokio::test]
async fn read_skill_rejected_symlink_does_not_disclose_its_target() {
    // The model names ONLY `link` (which looks in-bounds). The resolved
    // target is information it does NOT have — echoing it would turn a
    // repo-shipped skill (`data.txt` -> `~/.ssh/id_rsa`) into a
    // path-disclosure oracle. `read` may echo a path the model itself
    // supplied; `read_skill` must not invent one.
    let cwd = temp_cwd("skill-sym-oi");
    let root = skill_root("skill-sym-oi-root", "alpha", "---\nname: alpha\n---\nA.\n");
    let outside = std::env::temp_dir().join(format!("oi-target-{}", uuid::Uuid::new_v4()));
    std::fs::write(&outside, "secret").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, root.join("alpha/link")).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&outside, root.join("alpha/link")).unwrap();

    let r = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read_skill",
        &json!({ "name": "alpha", "path": "link" }),
    )
    .await;
    assert!(r.is_error, "the symlink must be rejected");
    let text = text_of(&r);
    let target = outside.to_string_lossy().into_owned();
    assert!(
        !text.contains(&target),
        "the resolved target must not be disclosed: {text}"
    );
    assert!(
        text.contains("link"),
        "the call must still be diagnosable: {text}"
    );
    let _ = std::fs::remove_dir_all(&cwd);
    let _ = std::fs::remove_file(&outside);
}

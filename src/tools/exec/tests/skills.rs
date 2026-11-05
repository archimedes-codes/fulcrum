use std::path::{Path, PathBuf};

use serde_json::json;

use super::support::*;
use crate::tools::exec::execute_tool;
use crate::tools::types::ToolCtx;

// ── list_skills / read_skill ─────────────────────────────────────────

/// A skill root holding `<name>/SKILL.md`. Returns (GUARD, root path) — the
/// guard FIRST (#96 task 08): destructured locals drop in REVERSE declaration
/// order, so the `PathBuf` dies before the dir it names. The path travels as a
/// separate element because `ctx_with_roots` takes `&[PathBuf]` — which is what
/// turns what would have been 18 `.to_path_buf()` edits into zero: once the
/// bindings are rebound, `&[root]` and `from_ref(&root)` compile unchanged.
fn skill_root(tag: &str, name: &str, content: &str) -> (crate::test_support::ScratchDir, PathBuf) {
    let root = temp_cwd(tag);
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), content).unwrap();
    let path = root.to_path_buf();
    (root, path)
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
    let (_g_root, root) = skill_root(
        "skills-list-root",
        "alpha",
        "---\nname: alpha\ndescription: Does alpha.\n---\nBody.\n",
    );
    let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
    assert!(!r.is_error);
    assert_eq!(text_of(&r), "alpha (space): Does alpha.");
}

#[tokio::test]
async fn list_skills_empty_is_none_not_error() {
    let cwd = temp_cwd("skills-list-empty");
    // The guard is a NAMED local, NOT `temp_cwd(..).to_path_buf()`: a temporary
    // guard would drop at the end of that statement and delete the dir BEFORE
    // `list_skills` ever saw it — the assertion would then pass because the root
    // was gone, not because the catalog was empty. A silent vacuous green.
    let root_guard = temp_cwd("skills-list-empty-root");
    let root = root_guard.to_path_buf();
    let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
    assert!(!r.is_error, "an empty catalog is not a failure");
    assert_eq!(text_of(&r), "none");
}

#[tokio::test]
async fn list_skills_omits_model_hidden_skills() {
    // ADR 0036: to the Agent a model-hidden skill is NOT in the catalog —
    // its name AND its description (the routing hint) are both absent.
    let cwd = temp_cwd("skills-list-hidden");
    let (_g_a, a) = skill_root(
        "tag-a",
        "alpha",
        "---\nname: alpha\ndescription: D\n---\nA.\n",
    );
    let (_g_o, o) = skill_root(
        "tag-o",
        "omega",
        "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
    );
    let r = execute_tool(&ctx_with_roots(&cwd, &[a, o]), "list_skills", &json!({})).await;
    assert!(!r.is_error, "{}", text_of(&r));
    assert_eq!(text_of(&r), "alpha (space): D");
    assert!(!text_of(&r).contains("omega"), "{}", text_of(&r));
}

#[tokio::test]
async fn list_skills_is_none_when_every_skill_is_model_hidden() {
    // A Space can present an EMPTY catalog to the Agent while the Skills
    // dialog shows a full one — that is the feature, and an empty catalog
    // is still not a failure (cf. `list_skills_empty_is_none_not_error`).
    let cwd = temp_cwd("skills-list-all-hidden");
    let (_g_root, root) = skill_root(
        "skills-list-all-hidden-root",
        "omega",
        "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
    );
    let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
    assert!(!r.is_error, "an empty catalog is not a failure");
    assert_eq!(text_of(&r), "none");
}

#[tokio::test]
async fn read_skill_reads_skill_md_without_touching_the_sandbox() {
    // THE regression: the skill lives OUTSIDE `ctx.cwd` (the real-world
    // case — user skills live in `~/.agents/skills`), so `read` rejects
    // it and `read_skill` must not.
    let cwd = temp_cwd("skill-outside");
    let (_g_root, root) = skill_root(
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
}

#[tokio::test]
async fn read_skill_name_is_case_insensitive_and_flattens_nothing() {
    let cwd = temp_cwd("skill-case");
    let (_g_root, root) = skill_root("skill-case-root", "Alpha", "---\nname: Alpha\n---\nBody.\n");
    let r = execute_tool(
        &ctx_with_roots(&cwd, &[root]),
        "read_skill",
        &json!({ "name": "alpha" }),
    )
    .await;
    assert!(!r.is_error);
    assert_eq!(text_of(&r), "Body.");
}

#[tokio::test]
async fn read_skill_bundled_file_resolves_relative_to_the_skill_dir() {
    let cwd = temp_cwd("skill-bundle");
    let (_g_root, root) = skill_root(
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
}

#[tokio::test]
async fn read_skill_bundled_escape_is_rejected() {
    // A `path` that leaves the SKILL's dir must be rejected even though
    // it stays inside a sibling skill — the boundary is the skill dir,
    // not the skill ROOT.
    let cwd = temp_cwd("skill-esc");
    let (_g_root, root) = skill_root("skill-esc-root", "alpha", "---\nname: alpha\n---\nA.\n");
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
}

#[tokio::test]
async fn read_skill_unknown_name_is_error_with_the_catalog() {
    let cwd = temp_cwd("skill-unknown");
    let (_g_root, root) = skill_root("skill-unknown-root", "alpha", "---\nname: alpha\n---\nA.\n");
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
}

#[tokio::test]
async fn read_skill_rejects_a_model_hidden_skill_with_the_mention_route() {
    // A hidden name resolves (so the model is not lied to) but is refused,
    // and the failure names the ONE route that works: the user's `$name`
    // Mention. The whole string is pinned — it is the model's only
    // instruction about that route (ADR 0036).
    let cwd = temp_cwd("skill-hidden-route");
    let (_g_root, root) = skill_root(
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
    let (_g_dbg, dbg) = skill_root(
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
    let (_g_root, root) = skill_root(
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
}

#[tokio::test]
async fn read_skill_rejects_a_model_hidden_skill_before_reading_a_bundled_file() {
    // Enforcement lands BEFORE any `FsBackend` is built, so a hidden
    // skill's bundled files are never read — not merely reported as
    // escaped.
    let cwd = temp_cwd("skill-hidden-bundle");
    let (_g_hidden, hidden) = skill_root(
        "skill-hidden-bundle-hidden",
        "omega",
        "---\nname: omega\ndisable-model-invocation: true\n---\nO.\n",
    );
    std::fs::write(hidden.join("omega/ref.md"), "HIDDEN-BUNDLED-BODY\n").unwrap();
    // The visible CONTROL: a visible skill's bundled file still reads, so
    // this test cannot pass merely because the bundled path is broken.
    let (_g_visible, visible) = skill_root(
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
}

#[tokio::test]
async fn read_skill_missing_name_param_is_error() {
    let cwd = temp_cwd("skill-noparam");
    let r = execute_tool(&ctx(&cwd), "read_skill", &json!({})).await;
    assert!(r.is_error);
    assert!(text_of(&r).contains("`name`"), "{}", text_of(&r));
}

#[tokio::test]
async fn read_skill_bundled_binary_is_an_error_not_garbage() {
    // `FsBackend::read` is `read_to_string`, so a bundled IMAGE fails
    // (the documented ADR 0029 limitation — unlike `read`'s image path).
    let cwd = temp_cwd("skill-binary");
    let (_g_root, root) = skill_root("skill-binary-root", "alpha", "---\nname: alpha\n---\nA.\n");
    std::fs::write(root.join("alpha/pic.png"), [0x89, 0x50, 0x4e, 0x47, 0x00]).unwrap();
    let r = execute_tool(
        &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
        "read_skill",
        &json!({ "name": "alpha", "path": "./pic.png" }),
    )
    .await;
    assert!(r.is_error, "a non-UTF-8 bundled file is an error");
}

#[tokio::test]
async fn read_skill_rejected_symlink_does_not_disclose_its_target() {
    // The model names ONLY `link` (which looks in-bounds). The resolved
    // target is information it does NOT have — echoing it would turn a
    // repo-shipped skill (`data.txt` -> `~/.ssh/id_rsa`) into a
    // path-disclosure oracle. `read` may echo a path the model itself
    // supplied; `read_skill` must not invent one.
    let cwd = temp_cwd("skill-sym-oi");
    let (_g_root, root) = skill_root("skill-sym-oi-root", "alpha", "---\nname: alpha\n---\nA.\n");
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
    let _ = std::fs::remove_file(&outside);
}

use std::path::PathBuf;

use super::*;
use crate::agent::harness::catalog::{CompactionConfig, Model};

/// The three inputs `resolve_launch` needs (the former `AgentLoop`
/// fields the method read — the catalog, the settings dir, and the
/// space cwd), built the same way the old `build_loop` /
/// `build_loop_with_db` helpers built them (a fresh temp-dir space
/// cwd, a `ModelCatalog` over the given `models`).
struct Ctx {
    catalog: ModelCatalog,
    config_dir: Option<PathBuf>,
    /// The scratch Space this fixture MINTED, held by its OWNER rather
    /// than as a `PathBuf` copy of one (#96): holding the fixture and
    /// holding its ownership are then the same act, so a green run cannot
    /// leave the dir behind. Declared LAST so it drops LAST among the
    /// fields; every read site (`&ctx.space_cwd`, `ctx.space_cwd.join(..)`)
    /// still compiles through `Deref<Target = Path>`.
    ///
    /// `config_dir` above stays an `Option<PathBuf>` on purpose: it is a
    /// copy of a path its CALLER owns (the settings scratch dir), and a
    /// second owner would `remove_dir_all` a dir someone else is still
    /// asserting against.
    space_cwd: crate::test_support::ScratchDir,
}

/// A `Model` for a given bare id (a `fake` provider — the unit
/// tests' catalog entry; a single-model catalog is `fake/m1`, so a
/// frontmatter `model: fake/m2` is STALE by construction — exactly
/// the degrade case).
fn fake_model(id: &str) -> Model {
    Model {
        id: id.to_string(),
        provider: "fake".to_string(),
        base_url: "http://fake".to_string(),
        api_key: "k".to_string(),
        context_window: 128000,
        cost_per_mtok_in: 0.0,
        cost_per_mtok_out: 0.0,
        supports_tools: true,
        supports_thinking: false,
        thinking_levels: Vec::new(),
        api: Some("openai-completions".to_string()),
    }
}

/// `HOME` restore guard (a scope-exit `Drop` — the restore happens
/// even when an assertion panics mid-test; the same pattern as the
/// `list_agents_tool` tests in `r#loop`).
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

/// Build the three inputs (the catalog gets the full `models` vec;
/// `config_dir` is the settings dir holding `settings.json`).
fn build_ctx(models: Vec<Model>, config_dir: Option<&std::path::Path>) -> Ctx {
    Ctx {
        catalog: ModelCatalog {
            models,
            default_model: None,
            compaction: CompactionConfig::default(),
        },
        config_dir: config_dir.map(|p| p.to_path_buf()),
        // The constructor owns the name AND the create (#96) — the former
        // bare `join(format!(…-{uuid}))` mint + `create_dir_all` pair had
        // no cleanup anywhere in this file, so every green run leaked one
        // dir per call site. Prefix preserved verbatim (status doc D12:
        // the census attributes leaks by prefix); the tag names the fixture.
        space_cwd: crate::test_support::ScratchDir::in_env_temp("harness-launch-test-", "space"),
    }
}

/// (ADR 0020) `agent_name: ""` → the launch is returned VERBATIM
/// (the label-only, config-less behavior — never an error).
///
/// The tri-state is the point of this test: an empty `agentName` is NOT a
/// miss (config-less dispatch is deliberate), and `NoName` is what keeps
/// the UI silent for it.
#[test]
fn resolve_launch_no_agent_name_returns_the_launch_verbatim() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let launch = LaunchConfig {
        system_prompt: Some("explicit prompt".to_string()),
        model: Some("fake/m1".to_string()),
        thinking: Some("high".to_string()),
        tools: Some(vec!["bash".to_string()]),
        ..Default::default()
    };
    assert_eq!(
        resolve_launch(
            &launch,
            "",
            &ctx.catalog,
            ctx.config_dir.as_deref(),
            &ctx.space_cwd
        ),
        (launch, AgentNameResolution::NoName),
        "an empty agentName returns the launch verbatim AND reports `NoName`"
    );
}

/// (ADR 0020) an `agent_name` matching NO discovered definition →
/// the launch is returned VERBATIM. The user-level roots are
/// ISOLATED first (a developer's real `~/.pi/agent/agents/nope.md`
/// would match and break the "no match" case — the same
/// `ENV_LOCK` / `HOME`-to-empty-scratch drop-guard pattern as the
/// `list_agents_tool` tests).
#[test]
fn resolve_launch_unknown_agent_name_returns_the_launch_verbatim() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let empty_home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "unknown-name",
    );
    // The `env_lock` helper is poison-tolerant (a sibling test
    // panicking while holding the lock must not turn this test's
    // failure into an opaque `PoisonError` panic — see its docs).
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", empty_home.path());
    }
    let launch = LaunchConfig {
        system_prompt: Some("explicit prompt".to_string()),
        model: Some("fake/m1".to_string()),
        thinking: Some("high".to_string()),
        tools: Some(vec!["bash".to_string()]),
        ..Default::default()
    };
    assert_eq!(
        resolve_launch(
            &launch,
            "nope",
            &ctx.catalog,
            ctx.config_dir.as_deref(),
            &ctx.space_cwd
        ),
        (launch.clone(), AgentNameResolution::Unmatched),
        "an unknown agentName returns the launch verbatim AND reports the miss"
    );
    // The ADR 0020 half, pinned on its own so it survives a change to the
    // tuple's shape: the CONFIG is byte-equal to the input.
    assert_eq!(
        resolve_launch(
            &launch,
            "nope",
            &ctx.catalog,
            ctx.config_dir.as_deref(),
            &ctx.space_cwd
        )
        .0,
        launch,
        "an unknown agentName returns the launch verbatim"
    );
}

/// (ADR 0020) a CASE-INSENSITIVE exact match applies the frontmatter
/// (the file's `name: scout` matches `agentName: "Scout"`).
#[test]
fn resolve_launch_case_insensitive_match() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, resolution) = resolve_launch(
        &launch,
        "Scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolution,
        AgentNameResolution::Matched,
        "a different case is a MATCH (eq_ignore_ascii_case), never a miss"
    );
    assert_eq!(resolved.model, Some("fake/m1".to_string()));
    assert_eq!(resolved.frontmatter_thinking, Some("low".to_string()));
    assert_eq!(resolved.tools, Some(vec!["read".to_string()]));
    assert_eq!(resolved.system_prompt, Some("You are a scout.".to_string()));
}

/// (ADR 0020) an all-`None` launch + a file with `model` (IN the
/// catalog), `thinking`, `tools` + a body → ALL FOUR fields are
/// populated from the file (the frontmatter fills the gaps).
#[test]
fn resolve_launch_frontmatter_fills_gaps() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\nthinking: low\ntools: [read, bash]\n---\nYou are a scout.\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved.system_prompt, Some("You are a scout.".to_string()));
    assert_eq!(resolved.model, Some("fake/m1".to_string()));
    assert_eq!(resolved.frontmatter_thinking, Some("low".to_string()));
    assert_eq!(
        resolved.tools,
        Some(vec!["read".to_string(), "bash".to_string()])
    );
}

/// (ADR 0020) a launch with ALL FOUR fields set to values X + a file
/// with DIFFERENT values Y, EXCEPT `launch.thinking` is `None` →
/// the three set fields keep X (the explicit params win) AND
/// `thinking` becomes Y (the `None` field is filled from the file —
/// the second assertion fails under the verbatim stub, which would
/// leave `thinking` `None`).
#[test]
fn resolve_launch_explicit_params_win_over_frontmatter() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: Some("X prompt".to_string()),
        model: Some("fake/m1".to_string()),
        thinking: None,
        tools: Some(vec!["bash".to_string()]),
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    // The explicit params win (the file's `model: fake/m2` is stale
    // anyway — the explicit `fake/m1` is the next layer):
    assert_eq!(resolved.system_prompt, Some("X prompt".to_string()));
    assert_eq!(resolved.model, Some("fake/m1".to_string()));
    assert_eq!(resolved.tools, Some(vec!["bash".to_string()]));
    // The `None` field is filled from the file:
    assert_eq!(resolved.frontmatter_thinking, Some("low".to_string()));
}

/// (ADR 0020) a frontmatter `model` that resolves to NOTHING (not in
/// the catalog — a stale file) degrades to the next layer (the
/// explicit param, else the parent model) — a stale file must not
/// fail the dispatch. `thinking` (unaffected by the model degrade)
/// is still filled from the file — the second assertion fails under
/// the verbatim stub.
#[test]
fn resolve_launch_stale_frontmatter_model_degrades_to_explicit_param() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\n---\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: Some("fake/m1".to_string()),
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "the stale frontmatter model degrades to the explicit param"
    );
    assert_eq!(
        resolved.frontmatter_thinking,
        Some("low".to_string()),
        "the thinking is still filled from the file"
    );
}

/// (ADR 0020) a stale frontmatter `model` + an all-`None` launch →
/// `model` degrades to `None` (the parent model applies downstream)
/// AND `tools` is still filled from the file (the second assertion
/// fails under the verbatim stub).
#[test]
fn resolve_launch_stale_frontmatter_model_degrades_to_none() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\ntools: [read]\n---\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model, None,
        "the stale frontmatter model degrades to `None` (the parent model applies downstream)"
    );
    assert_eq!(
        resolved.tools,
        Some(vec!["read".to_string()]),
        "the tools are still filled from the file"
    );
}

/// (ADR 0020) a stale frontmatter `model` WITH a `:<level>` suffix:
/// the suffix is stripped ONLY for the resolvability check (the bare
/// `fake/m2` is not in the catalog — the suffix does not save it)
/// AND `system_prompt` is still filled from the body (the second
/// assertion fails under the verbatim stub).
#[test]
fn resolve_launch_stale_model_with_level_suffix_degrades() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2:high\n---\nYou are a scout.\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model, None,
        "the bare `fake/m2` is not in the catalog — the suffix does not save it"
    );
    assert_eq!(
        resolved.system_prompt,
        Some("You are a scout.".to_string()),
        "the body is still filled from the file"
    );
}

/// (ADR 0020) unknown tool names in the frontmatter are DROPPED (a
/// file hint, not precise intent) — known names are kept.
#[test]
fn resolve_launch_unknown_tool_names_are_dropped() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\ntools: [read, bogus_tool]\n---\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.tools,
        Some(vec!["read".to_string()]),
        "known names are kept, unknown names are dropped"
    );
}

/// (ADR 0020) a frontmatter `tools` list that empties out after the
/// unknown-name drop is treated as ABSENT (the child is never
/// zeroed out by a stale file) AND `model` (IN the catalog) is
/// still filled from the file (the second assertion fails under the
/// verbatim stub).
#[test]
fn resolve_launch_tools_emptied_out_degrades_to_none() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\ntools: [bogus_tool]\n---\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.tools, None,
        "an emptied-out list degrades to `None` (never an empty allowlist)"
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "the model (in the catalog) is still filled from the file"
    );
}

/// (ADR 0020) a frontmatter `tools` list that holds ONLY the
/// parent-guarded names (`subagent` / `list_agents` — they can never
/// reach the child) is treated as ABSENT (the child is never
/// zeroed out by a stale file) AND `model` (IN the catalog) is
/// still filled from the file (the second assertion fails under a
/// no-op change).
///
/// No `env_lock()` here (intentional): `resolve_launch` reads `HOME`
/// (via `discover_agents` → `user_roots`) concurrently with the
/// `HOME`-mutating tests, but the isolation is safe by construction:
/// (1) the space-level roots are scanned FIRST (first-wins dedupe),
/// so a user-level same-name file can never shadow the space-level
/// definition written below; (2) the mutators' scratch homes contain
/// no agent files. Asserting on a USER-level definition would
/// require `env_lock()` first.
#[test]
fn resolve_launch_guarded_tool_names_in_frontmatter_degrade_to_none() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\ntools: [subagent, list_agents]\n---\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.tools, None,
        "a guarded-only list degrades to `None` (never an empty allowlist)"
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "the model (in the catalog) is still filled from the file"
    );
}

/// (ADR 0020) an EMPTY frontmatter body means "no system prompt" (an
/// `or_else` must not turn it into `Some("")` — `dispatch_native`
/// would prepend an empty message) AND `thinking` is still filled
/// from the file (the second assertion fails under the verbatim
/// stub).
#[test]
fn resolve_launch_empty_body_means_no_system_prompt() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nthinking: low\n---\n",
    )
    .unwrap();
    let launch = LaunchConfig {
        system_prompt: None,
        model: None,
        thinking: None,
        tools: None,
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.system_prompt, None,
        "an empty body means `None` (NOT `Some(\"\")` )"
    );
    assert_eq!(
        resolved.frontmatter_thinking,
        Some("low".to_string()),
        "the thinking is still filled from the file"
    );
}

/// (ADR 0023) a settings `subagentModels` override (RESOLVABLE —
/// in the catalog) beats the frontmatter `model`.
#[test]
fn resolve_launch_settings_override_beats_frontmatter_model() {
    let config_dir = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-settings-",
        "override-beats-file",
    );
    std::fs::write(
        config_dir.join("settings.json"),
        r#"{ "subagentModels": { "scout": "fake/m2" } }"#,
    )
    .unwrap();
    let home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "override-beats-file",
    );
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\nYou are a scout.\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m2".to_string()),
        "the settings override beats the frontmatter model"
    );
}

/// (ADR 0023) the EXPLICIT launch param beats the settings override.
#[test]
fn resolve_launch_explicit_param_beats_settings_override() {
    let config_dir = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-settings-",
        "explicit-beats-override",
    );
    std::fs::write(
        config_dir.join("settings.json"),
        r#"{ "subagentModels": { "scout": "fake/m2" } }"#,
    )
    .unwrap();
    let home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "explicit-beats-override",
    );
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\nYou are a scout.\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
    let launch = LaunchConfig {
        model: Some("fake/m1".to_string()),
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "the explicit param beats the settings override"
    );
}

/// (ADR 0023) a settings override that resolves to NOTHING (NOT in
/// the catalog — a stale override) degrades SOFT to the frontmatter
/// `model` (never an error, never `None`).
#[test]
fn resolve_launch_stale_override_degrades_to_frontmatter() {
    let config_dir = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-settings-",
        "stale-override",
    );
    std::fs::write(
        config_dir.join("settings.json"),
        r#"{ "subagentModels": { "scout": "fake/nope" } }"#,
    )
    .unwrap();
    let home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "stale-override",
    );
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\nYou are a scout.\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "a stale override degrades to the frontmatter model (soft)"
    );
}

/// (ADR 0023) the override key matches CASE-INSENSITIVELY (a capital
/// settings key `"Scout"` matches the file's `name: scout` + the
/// case-insensitive `agentName` `"Scout"`).
#[test]
fn resolve_launch_case_insensitive_override_key() {
    let config_dir = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-settings-",
        "case-insensitive-key",
    );
    std::fs::write(
        config_dir.join("settings.json"),
        r#"{ "subagentModels": { "Scout": "fake/m2" } }"#,
    )
    .unwrap();
    let home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "case-insensitive-key",
    );
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\nYou are a scout.\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "Scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m2".to_string()),
        "the capital settings key matches the lowercase agentName"
    );
}

/// (ADR 0023) a `settings.json` with an EMPTY `subagentModels` map →
/// no override layer → the frontmatter `model` applies (today's
/// behavior).
#[test]
fn resolve_launch_no_override_keeps_the_frontmatter() {
    let config_dir = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-settings-",
        "no-override",
    );
    std::fs::write(
        config_dir.join("settings.json"),
        r#"{ "subagentModels": {} }"#,
    )
    .unwrap();
    let home =
        crate::test_support::ScratchDir::in_env_temp("harness-resolve-launch-home-", "no-override");
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\nYou are a scout.\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "no override → the frontmatter model applies"
    );
}

/// (ADR 0023 thinking split) the frontmatter's `thinking` moves to
/// its OWN field (`frontmatter_thinking`): `thinking` holds the
/// EXPLICIT param only (`None` here) — the dispatch layers the
/// frontmatter's `thinking` LAST.
#[test]
fn resolve_launch_frontmatter_thinking_migrates_to_its_own_field() {
    let home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "thinking-migrates",
    );
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nthinking: low\n---\nYou are a scout.\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.thinking, None,
        "`thinking` holds the EXPLICIT param only"
    );
    assert_eq!(
        resolved.frontmatter_thinking,
        Some("low".to_string()),
        "the frontmatter's `thinking` moves to its own field"
    );
}

// ── (ADR 0066/0067/0068) The override layer: `explicit > override >
//    frontmatter > parent`, each field resolved independently ──────────

/// (ADR 0066 thinking) A fixture model that ADVERTISES thinking levels.
/// `fake_model` sets `supports_thinking: false` with an empty
/// `thinking_levels` — and an empty set SOFT-PASSES
/// (`catalog::supports_thinking_level_empty_set_soft_passes`), which makes
/// the "the winning model does not advertise the pinned level → demote"
/// rule UNTESTABLE against it. Every thinking-level assertion below uses
/// this model instead.
fn levelled_model(id: &str) -> Model {
    Model {
        thinking_levels: vec!["low".to_string(), "high".to_string()],
        supports_thinking: true,
        ..fake_model(id)
    }
}

/// The two HOME-isolation guards plus the `Ctx` over the settings dir —
/// returned TOGETHER, because the guards must outlive the helper: the
/// assertions run in the caller's body, and `discover_agents` reads `HOME`
/// there (via `user_roots`). Dropping them at the end of a helper would
/// restore the real `~/.agents/agents` mid-test.
struct World {
    /// Declared FIRST so it drops FIRST — the same ordering the existing
    /// tests get from their `let _lock` / `let _restore` sequence, which
    /// `RestoreHome`'s SAFETY comment depends on (the restore happens while
    /// the `ENV_LOCK` is still held).
    _restore: RestoreHome,
    _lock: std::sync::MutexGuard<'static, ()>,
    ctx: Ctx,
    /// The isolated `$HOME` this `World` pointed the process at, and the
    /// settings dir `ctx.config_dir` borrows. Fields drop in DECLARATION
    /// order, so these two — declared LAST — drop AFTER `_restore` and
    /// `_lock`: `$HOME` is restored (while the lock is still held) first,
    /// and only then is the dir it named removed. Declaring them before
    /// `_restore` would remove the dir while `$HOME` still pointed at it,
    /// and the lib reads env WITHOUT holding `ENV_LOCK` (see its docs), so
    /// another test could observe that hole.
    _home: crate::test_support::ScratchDir,
    _settings: crate::test_support::ScratchDir,
}

/// Write a `settings.json` holding `agentOverrides` for `scout` (ADR 0066)
/// and an isolated HOME holding the `scout.md` definition, and return the
/// `Ctx` over them. Keeps the ADR 0066 tests below from repeating the
/// `env_lock` / `RestoreHome` / scratch-dir dance.
fn write_override_world(settings_body: &str, definition: &str) -> World {
    let config_dir =
        crate::test_support::ScratchDir::in_env_temp("harness-resolve-launch-settings-", "world");
    std::fs::write(config_dir.join("settings.json"), settings_body).unwrap();
    let home =
        crate::test_support::ScratchDir::in_env_temp("harness-resolve-launch-home-", "world");
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(home.join(".agents/agents/scout.md"), definition).unwrap();
    let lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard the `World` carries); no other thread
    // mutates HOME concurrently.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(
        vec![fake_model("m1"), fake_model("m2"), levelled_model("m3")],
        Some(&config_dir),
    );
    // Both dirs MOVE INTO the `World`: a helper-local guard would
    // `remove_dir_all` the fixture at the end of this helper, while the
    // caller is still asserting against it.
    World {
        _restore: restore,
        _lock: lock,
        ctx,
        _home: home,
        _settings: config_dir,
    }
}

/// So a test can hold the guards and read the `Ctx` through one binding
/// (`let ctx = write_override_world(..); ctx.catalog`).
impl std::ops::Deref for World {
    type Target = Ctx;
    fn deref(&self) -> &Ctx {
        &self.ctx
    }
}

/// (ADR 0066) The override layer beats the frontmatter for ALL THREE
/// fields at once — the chain is `explicit > override > frontmatter >
/// parent`. Today only the `model` arm of the override exists (ADR 0023's
/// `subagentModels`), so the `thinking` and `tools` assertions are the
/// missing behaviour.
#[test]
fn resolve_launch_the_override_beats_the_frontmatter_for_every_field() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "scout": { "model": "fake/m3", "thinking": "high", "tools": ["read"] } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\nthinking: low\ntools: [bash]\n---\nYou are a scout.\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m3".to_string()),
        "the override model beats the frontmatter's"
    );
    assert_eq!(
        resolved.thinking,
        Some("high".to_string()),
        "the override thinking beats the frontmatter's (and lands in `thinking`, \
         the TOP rung of the dispatch's chain, so it outranks the model key's suffix)"
    );
    assert_eq!(
        resolved.tools,
        Some(vec!["read".to_string()]),
        "the override tools beat the frontmatter's"
    );
}

/// (ADR 0066) PER-FIELD INDEPENDENCE: an entry pinning ONLY `model` lets
/// thinking and tools keep coming from the file. Each layer supplies each
/// field on its own — the shape that makes an override that says one thing
/// not silently mute the other two.
#[test]
fn resolve_launch_an_override_pinning_only_model_leaves_the_others_with_the_file() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "scout": { "model": "fake/m3" } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\nthinking: low\ntools: [bash]\n---\nYou are a scout.\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved.model, Some("fake/m3".to_string()));
    assert_eq!(
        resolved.thinking, None,
        "`thinking` holds only the explicit param / the override — a file value \
         never moves into it"
    );
    assert_eq!(
        resolved.frontmatter_thinking,
        Some("low".to_string()),
        "the file's thinking still travels in its OWN field"
    );
    assert_eq!(
        resolved.tools,
        Some(vec!["bash".to_string()]),
        "the file's tools apply — the override does not speak"
    );
}

/// (ADR 0066) The EXPLICIT param still beats the override for all three
/// fields (the override is a layer UNDER the `subagent` tool params, never
/// over them), and the explicit `tools` stay VERBATIM — unfiltered.
#[test]
fn resolve_launch_the_explicit_param_beats_the_override_for_every_field() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "scout": { "model": "fake/m3", "thinking": "high", "tools": ["read"] } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\nYou are a scout.\n",
    );
    let launch = LaunchConfig {
        model: Some("fake/m1".to_string()),
        thinking: Some("low".to_string()),
        tools: Some(vec!["subagent".to_string()]),
        ..Default::default()
    };
    let (resolved, _) = resolve_launch(
        &launch,
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved.model, Some("fake/m1".to_string()));
    assert_eq!(resolved.thinking, Some("low".to_string()));
    assert_eq!(
        resolved.tools,
        Some(vec!["subagent".to_string()]),
        "an explicit `tools` list is NEVER filtered here (its verbatim semantics \
         belong to `subagent_flow.rs`)"
    );
}

/// (ADR 0067) An override NEVER SPEAKS BY BEING EMPTY: `tools: []` and a
/// list that filters down to nothing both fall through to the file, so no
/// override can hand a **Subagent session** zero tools. `subagent` /
/// `list_agents` are stripped at the override layer too — the recursion
/// guard now applies to every layer, not only the frontmatter's.
#[test]
fn resolve_launch_an_empty_or_filtered_empty_override_tools_list_falls_through() {
    for (label, body) in [
        (
            "an empty list",
            r#"{ "agentOverrides": { "scout": { "tools": [] } } }"#,
        ),
        (
            "a list of unknown names",
            r#"{ "agentOverrides": { "scout": { "tools": ["no-such-tool"] } } }"#,
        ),
        (
            "a list of only the guarded names",
            r#"{ "agentOverrides": { "scout": { "tools": ["subagent", "list_agents"] } } }"#,
        ),
    ] {
        let ctx = write_override_world(
            body,
            "---\nname: scout\ndescription: Fast recon.\ntools: [read, bash]\n---\n",
        );
        let (resolved, _) = resolve_launch(
            &LaunchConfig::default(),
            "scout",
            &ctx.catalog,
            ctx.config_dir.as_deref(),
            &ctx.space_cwd,
        );
        assert_eq!(
            resolved.tools,
            Some(vec!["read".to_string(), "bash".to_string()]),
            "{label} must fall through to the file, never zero the child out"
        );
    }
}

/// (ADR 0066) A valid NARROWING override wins (the point of the field), and
/// the guarded names are dropped from a list that survives the filter — so
/// an override can never grant a child a delegation route.
#[test]
fn resolve_launch_a_narrowing_override_wins_and_the_guarded_names_are_stripped() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "scout": { "tools": ["read", "subagent", "no-such-tool"] } } }"#,
        "---\nname: scout\ndescription: Fast recon.\ntools: [read, bash]\n---\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.tools,
        Some(vec!["read".to_string()]),
        "the surviving name wins; `subagent` and the unknown name are dropped"
    );
}

/// (ADR 0066 + the dispatch's validation) An override `thinking` the
/// RESOLVED model does not advertise is not written into `thinking` — the
/// dispatch drops it (`subagent_flow.rs`: `if !model.supports_thinking_level
/// { thinking = None }`), so writing/reporting it would be the ADR 0068 lie.
/// `fake/m3` advertises `low`/`high` only (`levelled_model`), so `xhigh`
/// demotes; the demoted override leaves no trace in `thinking`.
#[test]
fn resolve_launch_an_override_thinking_the_model_lacks_is_demoted() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "scout": { "model": "fake/m3", "thinking": "xhigh" } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved.model, Some("fake/m3".to_string()));
    assert_eq!(
        resolved.thinking, None,
        "the pinned level is not advertised by the winning model — the dispatch \
         would drop it, so the resolver must not promote it either"
    );
}

/// (ADR 0066) The CASE-INSENSITIVE lookup now happens ONCE for three
/// fields: a capital stored key still selects the whole entry (the ADR 0023
/// `resolve_launch_case_insensitive_override_key` guarantee, widened), and
/// the entry's OTHER fields come along with it.
#[test]
fn resolve_launch_a_capital_override_key_selects_the_whole_entry() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "Scout": { "model": "fake/m3", "tools": ["read"] } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\ntools: [bash]\n---\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "Scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m3".to_string()),
        "the capital settings key matches the lowercase agent name"
    );
    assert_eq!(resolved.tools, Some(vec!["read".to_string()]));
}

/// (ADR 0066) `config_dir: None` — no settings file at all — is a layer
/// that CANNOT SPEAK (not an empty map), and an EMPTY `agentOverrides` map
/// behaves identically: both leave the frontmatter in charge.
#[test]
fn resolve_launch_no_settings_and_an_empty_map_are_both_a_silent_layer() {
    // (a) no `config_dir` at all.
    let home = crate::test_support::ScratchDir::in_env_temp(
        "harness-resolve-launch-home-",
        "no-settings-layer",
    );
    std::fs::create_dir_all(home.join(".agents/agents")).unwrap();
    std::fs::write(
        home.join(".agents/agents/scout.md"),
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\ntools: [read]\n---\n",
    )
    .unwrap();
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span.
    unsafe {
        std::env::set_var("HOME", home.path());
    }
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved.model, Some("fake/m1".to_string()));
    assert_eq!(resolved.tools, Some(vec!["read".to_string()]));
    drop(_restore);
    drop(_lock);
    // (b) a file whose map is EMPTY — the same outcome.
    let ctx = write_override_world(
        r#"{ "agentOverrides": {} }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\ntools: [read]\n---\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "an empty map is no layer — the frontmatter applies"
    );
    assert_eq!(resolved.tools, Some(vec!["read".to_string()]));
}

/// (ADR 0023, kept) An override-pinned model missing from the catalog
/// degrades SOFT to the frontmatter (a stale override must never fail the
/// dispatch), while the entry's other fields keep applying — the fields are
/// resolved independently, so a stale model cannot mute a valid tools pin.
#[test]
fn resolve_launch_a_stale_override_model_degrades_and_the_other_fields_survive() {
    let ctx = write_override_world(
        r#"{ "agentOverrides": { "scout": { "model": "fake/nope", "tools": ["read"] } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\ntools: [bash]\n---\n",
    );
    let (resolved, _) = resolve_launch(
        &LaunchConfig::default(),
        "scout",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolved.model,
        Some("fake/m1".to_string()),
        "the stale override model degrades to the frontmatter (soft)"
    );
    assert_eq!(
        resolved.tools,
        Some(vec!["read".to_string()]),
        "the override's tools still apply — per-field independence"
    );
}

// ── The resolver seam itself (`resolve_agent_launch` / `override_layer`) ──
//
// ADR 0068: `list_agent_definition_reports` (Task 2) calls THIS function, so the
// `source` values are the modal's contract, not a debug extra. They are
// asserted here against `AgentDefinition` fixtures — no disk, no `HOME` —
// because the command holds definitions discovery handed it and must be able
// to feed them straight in.

/// A `scout` definition with the given frontmatter fields.
fn def_with(
    model: Option<&str>,
    thinking: Option<&str>,
    tools: Option<&[&str]>,
) -> AgentDefinition {
    AgentDefinition {
        name: "scout".to_string(),
        description: "Fast recon.".to_string(),
        model: model.map(str::to_string),
        thinking: thinking.map(str::to_string),
        tools: tools.map(|t| t.iter().map(|s| s.to_string()).collect()),
        permission: None,
        frontmatter_notes: Vec::new(),
        system_prompt: "You are a scout.".to_string(),
        scope: crate::agents::AgentScope::Space,
        path: "/tmp/scout.md".to_string(),
        shadowed: Vec::new(),
    }
}

/// Settings carrying one `agentOverrides` entry for `scout`.
fn settings_with(entry: AgentOverrideFields) -> Settings {
    Settings {
        agent_overrides: std::collections::HashMap::from([("scout".to_string(), entry)]),
        ..Settings::default()
    }
}

/// `override_layer` keeps "no file", "no entry" and "entry present" as
/// THREE distinguishable states (a caller cannot collapse them): `None`
/// settings is a layer that cannot speak, exactly like an absent name — and
/// both are different from a present entry.
#[test]
fn override_layer_keeps_no_file_and_no_entry_and_an_entry_apart() {
    // (a) no settings file at all.
    assert!(
        override_layer(None, "scout").global.is_none(),
        "`config_dir: None` is a layer that cannot speak"
    );
    // (b) a file with no entry for the name.
    let empty = Settings::default();
    assert!(override_layer(Some(&empty), "scout").global.is_none());
    // (c) an entry, found CASE-INSENSITIVELY (ADR 0023's scan, moved out of
    // the `model` arm because it now has to find the entry ONCE for three
    // fields), and the STORED KEY is not rewritten.
    let settings = settings_with(AgentOverrideFields {
        model: Some("fake/m1".to_string()),
        thinking: None,
        tools: None,
    });
    let layer = override_layer(Some(&settings), "ScOuT");
    assert_eq!(
        layer.global,
        Some(&AgentOverrideFields {
            model: Some("fake/m1".to_string()),
            thinking: None,
            tools: None,
        }),
        "the entry is returned verbatim for a differently-cased name"
    );
    assert_eq!(
        settings.agent_overrides.keys().collect::<Vec<_>>(),
        vec!["scout"],
        "the selector never rewrites the stored key"
    );
}

/// (ADR 0068) `source` per field names the layer that SUPPLIED it:
/// `override` for the store, `file` for the frontmatter, `parent` when
/// nothing below the explicit param spoke.
#[test]
fn resolve_agent_launch_reports_which_layer_supplied_each_field() {
    let catalog = ModelCatalog {
        models: vec![fake_model("m1"), fake_model("m2")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    // (a) the override supplies the model, the file supplies the tools.
    let def = def_with(Some("fake/m1"), None, Some(&["read"]));
    let layer = OverrideLayer {
        global: Some(&AgentOverrideFields {
            model: Some("fake/m2".to_string()),
            thinking: None,
            tools: None,
        }),
    };
    let r = resolve_agent_launch(&LaunchConfig::default(), &def, &layer, &catalog);
    assert_eq!(r.launch.model, Some("fake/m2".to_string()));
    assert_eq!(r.model_source, OverrideSource::Override);
    assert_eq!(r.launch.tools, Some(vec!["read".to_string()]));
    assert_eq!(r.tools_source, OverrideSource::File);
    // A demoted / absent thinking is NOT reported as the override's, and the
    // file's value never moves into `thinking` — so `thinking_source` is
    // `Parent` here even though `frontmatter_thinking` carries `low`.
    assert_eq!(r.launch.thinking, None);
    assert_eq!(r.thinking_source, OverrideSource::Parent);
    // (b) NOTHING speaks (no layer, no frontmatter values) → every field is
    // `Parent` = "no layer below the explicit param spoke" (ADR 0068 — the
    // modal labels it `Inherits parent`, and a dispatch that passes the
    // param still overrides it).
    let bare = def_with(None, None, None);
    let r = resolve_agent_launch(
        &LaunchConfig::default(),
        &bare,
        &OverrideLayer { global: None },
        &catalog,
    );
    assert_eq!(r.launch.model, None);
    assert_eq!(r.model_source, OverrideSource::Parent);
    assert_eq!(r.thinking_source, OverrideSource::Parent);
    assert_eq!(r.tools_source, OverrideSource::Parent);
}

/// (issue #101, task05a) A whole-tool `deny` in the file's `permission:`
/// block is CARRIED verbatim into `launch.permission` (this half carries and
/// reports; the gate consumption is task05b's), and it DROPS the denied tool
/// from the resolver-narrowed `launch.tools` — the one live effect this half
/// ships, because `subagent_flow.rs` re-advertises `launch.tools` VERBATIM,
/// so a dropped name is never offered to the child. `permission_source`
/// reports `File` exactly because the file spoke.
#[test]
fn a_whole_tool_deny_drops_the_tool_from_the_resolved_launch() {
    use crate::agent::definition_permission::{Action, PermissionBlock, Rules, ToolKey};
    let catalog = ModelCatalog {
        models: vec![fake_model("m1")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let block = PermissionBlock {
        tools: vec![(ToolKey::Bash, Rules::whole_tool(Action::Deny))],
    };
    let mut def = def_with(None, None, Some(&["bash", "read"]));
    def.permission = Some(block.clone());
    let r = resolve_agent_launch(
        &LaunchConfig::default(),
        &def,
        &OverrideLayer { global: None },
        &catalog,
    );
    assert_eq!(
        r.launch.tools,
        Some(vec!["read".to_string()]),
        "the whole-tool deny drops `bash` from the advertised set"
    );
    assert_eq!(
        r.tools_source,
        OverrideSource::File,
        "the surviving list is still the FILE's rung"
    );
    assert_eq!(
        r.launch.permission.as_deref(),
        Some(&block),
        "the block is carried VERBATIM (report, not verdict)"
    );
    assert_eq!(
        r.permission_source,
        OverrideSource::File,
        "the file spoke about permissions"
    );
}

/// (issue #101, task05a) A file that does NOT speak about permissions carries
/// `None` and reports `Parent` — and its tool list is untouched by the drop.
#[test]
fn a_definition_without_permission_carries_none_and_reports_parent() {
    let catalog = ModelCatalog {
        models: vec![fake_model("m1")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let def = def_with(None, None, Some(&["bash", "read"]));
    let r = resolve_agent_launch(
        &LaunchConfig::default(),
        &def,
        &OverrideLayer { global: None },
        &catalog,
    );
    assert_eq!(r.launch.permission, None);
    assert_eq!(r.permission_source, OverrideSource::Parent);
    assert_eq!(
        r.launch.tools,
        Some(vec!["bash".to_string(), "read".to_string()]),
        "no block, no drop"
    );
}

/// (issue #101, task05a, the honest limit) Only a WHOLE-TOOL deny acts on the
/// advertised set: a per-pattern ruleset (which announces itself unsupported)
/// changes NOTHING here — no matcher exists to justify dropping a name.
#[test]
fn per_pattern_permission_rules_never_change_the_advertised_tools() {
    use crate::agent::definition_permission::{Action, PermissionBlock, Rule, Rules, ToolKey};
    let catalog = ModelCatalog {
        models: vec![fake_model("m1")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let mut def = def_with(None, None, Some(&["bash", "read"]));
    def.permission = Some(PermissionBlock {
        tools: vec![(
            ToolKey::Bash,
            Rules::per_tool(vec![Rule {
                pattern: "rm *".to_string(),
                action: Action::Deny,
            }]),
        )],
    });
    let r = resolve_agent_launch(
        &LaunchConfig::default(),
        &def,
        &OverrideLayer { global: None },
        &catalog,
    );
    assert_eq!(
        r.launch.tools,
        Some(vec!["bash".to_string(), "read".to_string()]),
        "a per-pattern deny is NOT an advertisement filter"
    );
}

/// (issue #101, the narrowing invariant) A deny that removes the LAST
/// advertised name yields `Some(vec![])` = ZERO tools — NEVER `None`.
///
/// This test used to pin the OPPOSITE (`None`), and that pin was the bug:
/// `subagent_flow.rs`'s three-way rule reads `None` as "the parent's list
/// minus the guard", so a definition whose `permission:` denied its OWN
/// entire `tools:` list handed the child the PARENT'S FULL SET — i.e. adding
/// a deny strictly WIDENED the child's tools, the exact inverse of issue
/// #101's invariant ("a definition can make its own agent stricter … it can
/// never grant past it") and of the acceptance item "drop the tool from the
/// child's `enabled_tools` … so the model is never offered a call it can't
/// make" — precisely in the case where the deny is most total.
///
/// `Some(vec![])` is what the downstream actually means by "no tools" (see
/// the drop site's comment for the three-way rule's quotes), so the empty
/// set is the NARROWER and correct outcome on both rungs tested here: the
/// FILE's list, and the explicit `subagent({tools: […]})` param (where
/// "you may only use bash" + "bash is denied" = nothing, which is still
/// narrower than the parent's set and therefore respects narrowing-only).
#[test]
fn a_deny_that_empties_the_advertised_list_yields_zero_tools_not_the_parent_set() {
    use crate::agent::definition_permission::{Action, PermissionBlock, Rules, ToolKey};
    let catalog = ModelCatalog {
        models: vec![fake_model("m1")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let mut def = def_with(None, None, Some(&["bash"]));
    def.permission = Some(PermissionBlock {
        tools: vec![(ToolKey::Bash, Rules::whole_tool(Action::Deny))],
    });
    let r = resolve_agent_launch(
        &LaunchConfig::default(),
        &def,
        &OverrideLayer { global: None },
        &catalog,
    );
    assert_eq!(
        r.launch.tools,
        Some(vec![]),
        "a deny that empties the list is ZERO tools (`Some(vec![])`), NOT `None` — \
         `None` means 'the parent's list minus the guard', so falling back to it \
         would hand the child MORE tools than the same file without the deny"
    );
    assert_eq!(
        r.launch.permission.as_deref(),
        def.permission.as_ref(),
        "the deny is still CARRIED for task05b's gate"
    );
    // The EXPLICIT-PARAM rung, handled honestly: `subagent({tools: ["bash"]})`
    // + a file `bash: deny` = nothing left. Zero tools here is the correct
    // consequence of the two statements, and it is NARROWER than the parent's
    // set, so it respects narrowing-only (a file may restrict the explicit
    // list, never widen it).
    let explicit = LaunchConfig {
        tools: Some(vec!["bash".to_string()]),
        ..Default::default()
    };
    let r = resolve_agent_launch(&explicit, &def, &OverrideLayer { global: None }, &catalog);
    assert_eq!(
        r.launch.tools,
        Some(vec![]),
        "the explicit `tools: [\"bash\"]` param minus a `bash: deny` is ZERO tools, \
         not a fall-through to the parent's set"
    );
}

/// (issue #101, task05a) The `Box` on `LaunchConfig::permission` (a clippy
/// enum-size measure, documented at the field) is TRANSPARENT to serde: the
/// wire JSON is the B1-pinned block spelling, byte-for-byte what an unboxed
/// field would emit. This matters because the repo has NO wire-contract test
/// and task05b + the TS mirror read these spellings — a box that changed the
/// shape (e.g. a `#[serde(with)]` mishap) would silently hand them a
/// different document.
#[test]
fn the_boxed_permission_field_serializes_as_the_plain_block() {
    use crate::agent::definition_permission::{Action, PermissionBlock, Rules, ToolKey};
    let block = PermissionBlock {
        tools: vec![(ToolKey::Edit, Rules::whole_tool(Action::Ask))],
    };
    let boxed = LaunchConfig {
        permission: Some(Box::new(block.clone())),
        ..Default::default()
    };
    let json = serde_json::to_value(&boxed).unwrap();
    assert_eq!(
        json["permission"],
        serde_json::json!({
            "tools": [["edit", { "patterns": [{ "pattern": "*", "action": "ask" }], "whole_tool": true }]]
        }),
        "the box is a Rust spelling, not a wire name: {json}"
    );
    // And it round-trips back through `Deserialize` (the wire is DECODED in
    // production, so a box that only serialized one way would be a lie).
    let text = serde_json::to_string(&boxed).unwrap();
    let back: LaunchConfig = serde_json::from_str(&text).unwrap();
    assert_eq!(back.permission.as_deref(), Some(&block));
    // An ABSENT key decodes to `None` — the wire-additive promise (a frame
    // written before this field exists still parses). `{}` is the whole
    // promise: every `LaunchConfig` field is `Option`, so a frame missing
    // `permission` entirely is a valid `None`.
    let old: LaunchConfig = serde_json::from_str("{}").unwrap();
    assert_eq!(old.permission, None, "an absent key is `None`");
}

/// (the second half of "only the OVERRIDE value may be written into
/// `thinking`") A `:<level>` SUFFIX on the winning model key is not written
/// into `thinking` either — writing it would promote it above the override
/// and the suffix's own documented position in the dispatch's chain. The
/// suffix stays inside the VERBATIM model string, where `subagent_flow.rs`
/// reads it as the MIDDLE rung.
#[test]
fn resolve_agent_launch_never_writes_the_model_key_suffix_into_thinking() {
    let catalog = ModelCatalog {
        models: vec![levelled_model("m3")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    // The override speaks about the MODEL only; the file carries no
    // thinking; the key carries a `:low` suffix.
    let def = def_with(None, None, None);
    let layer = OverrideLayer {
        global: Some(&AgentOverrideFields {
            model: Some("fake/m3:low".to_string()),
            thinking: None,
            tools: None,
        }),
    };
    let r = resolve_agent_launch(&LaunchConfig::default(), &def, &layer, &catalog);
    assert_eq!(r.launch.model, Some("fake/m3:low".to_string()));
    assert_eq!(
        r.launch.thinking, None,
        "the suffix is a downstream candidate, not `LaunchConfig::thinking`"
    );
    assert_eq!(
        r.thinking_source,
        OverrideSource::Parent,
        "…and not reported as any layer's effective level"
    );
}

/// (ADR 0068, the lie this feature exists to prevent) An override thinking
/// the winning model does NOT advertise is neither written nor REPORTED —
/// `source` must never read `override` for a value the dispatch drops. No
/// `suppressed_thinking` field exists to carry it: the demotion is fully
/// assertable as `thinking == None` + `source != Override`.
#[test]
fn resolve_agent_launch_never_reports_a_demoted_thinking_as_the_override() {
    // `m3` advertises `low`/`high` only; the pinned `xhigh` is dropped
    // downstream, so it must not appear as effective.
    let catalog = ModelCatalog {
        models: vec![levelled_model("m3")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let def = def_with(None, None, None);
    let layer = OverrideLayer {
        global: Some(&AgentOverrideFields {
            model: Some("fake/m3".to_string()),
            thinking: Some("xhigh".to_string()),
            tools: None,
        }),
    };
    let r = resolve_agent_launch(&LaunchConfig::default(), &def, &layer, &catalog);
    assert_eq!(r.launch.model, Some("fake/m3".to_string()));
    assert_eq!(r.launch.thinking, None, "the dropped level is not written");
    assert_ne!(
        r.thinking_source,
        OverrideSource::Override,
        "…and never reported as effective"
    );
    assert_eq!(r.thinking_source, OverrideSource::Parent);
}

/// An advertised override thinking DOES win, and it wins over the model
/// key's `:<level>` SUFFIX — asserted by it landing in `LaunchConfig::
/// thinking`, the TOP rung of the dispatch's chain (`launch.thinking` >
/// suffix > `frontmatter_thinking`, `subagent_flow.rs`). The suffix is left
/// in the VERBATIM model string, untouched.
#[test]
fn resolve_agent_launch_an_advertised_override_thinking_outranks_the_key_suffix() {
    let catalog = ModelCatalog {
        models: vec![levelled_model("m3")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let def = def_with(Some("fake/m3:low"), Some("low"), None);
    let layer = OverrideLayer {
        global: Some(&AgentOverrideFields {
            model: Some("fake/m3:low".to_string()),
            thinking: Some("high".to_string()),
            tools: None,
        }),
    };
    let r = resolve_agent_launch(&LaunchConfig::default(), &def, &layer, &catalog);
    assert_eq!(
        r.launch.model,
        Some("fake/m3:low".to_string()),
        "the VERBATIM ref is kept (the suffix is a downstream thinking candidate)"
    );
    assert_eq!(
        r.launch.thinking,
        Some("high".to_string()),
        "`launch.thinking` outranks the suffix in the dispatch's chain"
    );
    assert_eq!(r.thinking_source, OverrideSource::Override);
    assert_eq!(
        r.launch.frontmatter_thinking,
        Some("low".to_string()),
        "`frontmatter_thinking` carries ONLY the frontmatter's own value"
    );
}

/// The explicit param's thinking is NEVER filtered (its verbatim semantics
/// belong to the dispatch) and always outranks the override — and the
/// override's model still applies beside it.
#[test]
fn resolve_agent_launch_an_explicit_thinking_beats_the_override_and_is_unfiltered() {
    let catalog = ModelCatalog {
        models: vec![levelled_model("m3")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let def = def_with(None, Some("low"), None);
    let layer = OverrideLayer {
        global: Some(&AgentOverrideFields {
            model: Some("fake/m3".to_string()),
            thinking: Some("high".to_string()),
            tools: None,
        }),
    };
    let launch = LaunchConfig {
        thinking: Some("bogus".to_string()),
        ..Default::default()
    };
    let r = resolve_agent_launch(&launch, &def, &layer, &catalog);
    assert_eq!(
        r.launch.thinking,
        Some("bogus".to_string()),
        "an explicit level is passed through unfiltered — the dispatch's own \
         validation owns it, not the resolver"
    );
    assert_eq!(
        r.thinking_source,
        OverrideSource::Parent,
        "ADR 0068's label space has NO word for the explicit param (no modal can \
         observe a live `subagent` call), so a field the explicit param supplied \
         reports `Parent` — which reads, truthfully, as 'nothing BELOW the explicit \
         param spoke'. Task 2's rows pass an EMPTY `LaunchConfig`, so this case \
         never arises on the wire; the pin exists so a future fourth label cannot \
         be invented for it."
    );
    assert_eq!(
        r.model_source,
        OverrideSource::Override,
        "the other fields resolve independently"
    );
}

/// (ADR 0067) `tools: []` speaks not — asserted at the resolver seam too,
/// where the `source` matters: the row must say `file` / `parent`, never
/// `override`, for a stored-but-empty list.
#[test]
fn resolve_agent_launch_an_empty_override_tools_list_reports_the_layer_that_spoke() {
    let catalog = ModelCatalog {
        models: vec![fake_model("m1")],
        default_model: None,
        compaction: CompactionConfig::default(),
    };
    let def = def_with(None, None, Some(&["read"]));
    let layer = OverrideLayer {
        global: Some(&AgentOverrideFields {
            model: None,
            thinking: None,
            tools: Some(Vec::new()),
        }),
    };
    let r = resolve_agent_launch(&LaunchConfig::default(), &def, &layer, &catalog);
    assert_eq!(r.launch.tools, Some(vec!["read".to_string()]));
    assert_eq!(
        r.tools_source,
        OverrideSource::File,
        "an empty stored list is ABSENT (ADR 0067) — the file's list runs"
    );
}

/// An EMPTY `agent_name` short-circuits `resolve_launch` BEFORE any layer
/// or resolver is built — the label-only, config-less case returns the
/// `launch` verbatim, so this test pins that return and NOTHING else.
///
/// Delegation to the shared `resolve_agent_launch` is NOT pinned here (an
/// empty name never reaches it, so this body would stay green with the
/// delegation deleted); it is pinned by the cross-module
/// `commands::agents::tests::agent_definition_reports_and_the_dispatch_resolve_the_same_row`,
/// which calls this real `resolve_launch` and compares its row against the
/// modal's. A same-seam test could not catch seam-internal drift, which is
/// why that pin lives where it does.
#[test]
fn resolve_launch_with_an_empty_agent_name_short_circuits() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let launch = LaunchConfig {
        system_prompt: Some("explicit".to_string()),
        ..Default::default()
    };
    // The verbatim return is the SHORT-CIRCUIT's doing, not the resolver's.
    let (resolved, resolution) = resolve_launch(
        &launch,
        "",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved, launch);
    assert_eq!(
        resolution,
        AgentNameResolution::NoName,
        "an empty name is the DELIBERATE config-less case, never a miss"
    );
}

/// (issue #47) The two whitespace edges of the tri-state, both on the
/// `trim()` short-circuit:
///
/// * a WHITESPACE-ONLY name hits the same short-circuit as `""`, so it is
///   `NoName` and NOT a miss — the UI stays silent for a call that never
///   named anyone;
/// * a PADDED real name still matches its definition, so the trim must not
///   turn a genuine `agentName` into a miss (that would flag a working
///   dispatch).
///
/// Neither case duplicates `resolve_launch_case_insensitive_match`, which
/// pins CASE and not whitespace.
#[test]
fn resolve_launch_a_blank_name_is_no_name_and_a_padded_name_is_a_match() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    // The user-level roots are ISOLATED (the same pattern as
    // `resolve_launch_unknown_agent_name_returns_the_launch_verbatim`): a
    // developer's real `~/.pi/agent/agents/…` must not answer for the
    // whitespace-only case, and the scratch Space below is the ONLY place
    // `scout` is defined.
    let empty_home =
        crate::test_support::ScratchDir::in_env_temp("harness-resolve-launch-home-", "blank-name");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span (the `env_lock` guard); no other thread mutates HOME
    // concurrently. The lock is held across BOTH cases below.
    unsafe {
        std::env::set_var("HOME", empty_home.path());
    }
    let launch = LaunchConfig {
        system_prompt: Some("explicit".to_string()),
        ..Default::default()
    };

    // 1. whitespace-only → `NoName` (the `trim()` short-circuit).
    let (resolved, resolution) = resolve_launch(
        &launch,
        "   ",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(resolved, launch, "a blank name is config-less");
    assert_eq!(
        resolution,
        AgentNameResolution::NoName,
        "a whitespace-only name is the DELIBERATE config-less case, never a miss"
    );

    // 2. a padded real name → `Matched`, with the frontmatter applied.
    let agent_file = ctx.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
    )
    .unwrap();
    let bare = LaunchConfig::default();
    let (resolved, resolution) = resolve_launch(
        &bare,
        "  scout  ",
        &ctx.catalog,
        ctx.config_dir.as_deref(),
        &ctx.space_cwd,
    );
    assert_eq!(
        resolution,
        AgentNameResolution::Matched,
        "the trim must not turn a real name into a miss"
    );
    assert_eq!(resolved.model, Some("fake/m1".to_string()));
    assert_eq!(resolved.tools, Some(vec!["read".to_string()]));
    assert_eq!(resolved.system_prompt, Some("You are a scout.".to_string()));
}

/// #96: the `Ctx` fixture's `space_cwd` must be OWNED, not a bare `PathBuf`.
/// A green run that leaves its dir behind is what exhausted the per-user
/// temp quota; a per-test assertion is the smallest thing that can prove
/// ownership exists AT ALL (the escaped-panic case — a FAILING test whose
/// guard is a plain local — is pinned by task 09's census, per status doc
/// D10, and cannot be claimed from here).
#[test]
fn the_ctx_removes_its_space_cwd_when_it_is_dropped() {
    let ctx = build_ctx(vec![fake_model("m1")], None);
    let captured = ctx.space_cwd.to_path_buf();
    assert!(captured.is_dir(), "build_ctx created the dir");
    drop(ctx);
    assert!(
        !captured.exists(),
        "the fixture owns its scratch dir — {captured:?} is gone with the `Ctx`"
    );
}

/// The same claim for the two dirs `write_override_world` mints. The `home`
/// path is read back from `$HOME` while the `World` still holds the env lock
/// (so this reads `HOME` under the lock, as the `ENV_LOCK` docs require) and
/// the settings path from `ctx.config_dir` — no accessor API is added for the
/// test, and neither path is guessed, so the assertion cannot be confused by
/// another test's live dir of the same prefix.
#[test]
fn write_override_world_removes_both_of_its_scratch_dirs() {
    let world = write_override_world(
        r#"{ "agentOverrides": { "scout": { "model": "fake/m1" } } }"#,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m1\n---\n",
    );
    let home = std::path::PathBuf::from(
        std::env::var_os("HOME").expect("write_override_world points HOME at its scratch dir"),
    );
    let settings = world
        .config_dir
        .clone()
        .expect("the settings dir is ctx.config_dir");
    assert!(
        home.is_dir() && settings.is_dir(),
        "both fixtures exist while the `World` is alive"
    );
    drop(world);
    assert!(
        !home.exists(),
        "`$HOME`'s scratch dir goes with the `World` — {home:?}"
    );
    assert!(
        !settings.exists(),
        "the settings dir goes with the `World` — {settings:?}"
    );
}

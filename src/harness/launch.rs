//! The launch-resolution layer (ADR 0020 / 0023 — extracted from
//! `r#loop` in the loop.rs decomposition): resolve a subagent's
//! `agentName` against the discovered Agent definitions and layer the
//! frontmatter UNDER the explicit launch params. A PURE function of its
//! inputs (the catalog, the settings dir, and the space cwd — no loop
//! state), hence its own module.

use std::path::Path;

use crate::agent::harness::catalog::ModelCatalog;
use crate::agent::harness::tool_specs;
use crate::agent::harness::ModelRef;
use crate::agent::subagent::LaunchConfig;

/// Resolve `agentName` against the discovered Agent definitions and
/// layer the frontmatter UNDER the explicit launch params (ADR 0020
/// — explicit > frontmatter > parent defaults). `agentName` empty /
/// no match → the `launch` is returned VERBATIM (the label-only,
/// config-less behavior — never an error).
pub(crate) fn resolve_launch(
    launch: &LaunchConfig,
    agent_name: &str,
    catalog: &ModelCatalog,
    config_dir: Option<&Path>,
    space_cwd: &Path,
) -> LaunchConfig {
    let name = agent_name.trim();
    if name.is_empty() {
        return launch.clone();
    }
    let Some(def) = crate::agents::discover_agents(Some(space_cwd))
        .into_iter()
        .find(|d| d.name.eq_ignore_ascii_case(name))
    else {
        return launch.clone();
    };
    // A frontmatter `model` that resolves to NOTHING (not in the
    // catalog — a stale file) degrades to the next layer (the
    // explicit param, else the parent model) — a stale file must not
    // fail the dispatch. The `:<level>` suffix is stripped ONLY for
    // the resolvability check (via `ModelRef::parse` — the last-`:`
    // strip, mirroring `dispatch_subagent`'s); the stored value is
    // the VERBATIM
    // frontmatter string (the suffix is a thinking-level candidate
    // handled downstream). An explicit `model` param is NEVER
    // degraded here (it is the model's current intent —
    // `dispatch_native` still fails it when unknown, unchanged).
    // (ADR 0023) The per-agent model override (the settings'
    // `subagentModels` — read at dispatch time: a settings edit takes
    // effect on the NEXT dispatch, no restart; a missing/corrupt file
    // yields the defaults → no override). SOFT, like the frontmatter:
    // a value whose bare key resolves to NOTHING degrades to the next
    // layer (a stale override must not fail the dispatch). CASE-
    // INSENSITIVE by name (a case-insensitive SCAN — the stored key is
    // NOT rewritten: the UI saves `def.name` verbatim, and a hand-edited
    // mixed-case key must still match the case-insensitive `agentName`
    // resolution).
    let override_model = config_dir.and_then(|dir| {
        crate::config::load_settings(dir)
            .subagent_models
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    });
    // ADR 0023 chain — see `resolve_model_ref` (not yet unified).
    // The `:<level>` suffix strip is `ModelRef::parse` (last `:`);
    // a ref whose BARE part is not a valid key (no `/`) parses to
    // `None` — the old code used the whole string as the bare key,
    // which failed `resolve_composed_model` identically (a key
    // without `/` can never be in the catalog) — same degrade.
    let model = launch
        .model
        .clone()
        .or_else(|| {
            override_model.as_ref().and_then(|m| {
                let bare = ModelRef::parse(m)
                    .map(|r| r.key.to_string())
                    .unwrap_or_else(|| m.clone());
                crate::agent::session::resolve_composed_model(catalog, &bare)
                    .is_some()
                    .then_some(m.clone())
            })
        })
        .or_else(|| {
            def.model.as_ref().and_then(|m| {
                let bare = ModelRef::parse(m)
                    .map(|r| r.key.to_string())
                    .unwrap_or_else(|| m.clone());
                crate::agent::session::resolve_composed_model(catalog, &bare)
                    .is_some()
                    .then_some(m.clone())
            })
        });
    // Unknown tool names in the frontmatter are DROPPED (a file
    // hint, not precise intent), as are the parent-guarded
    // `subagent` / `list_agents` (they can never reach the child —
    // `dispatch_native_inner` strips them — so a file `tools:
    // [subagent]` must degrade to ABSENT, not zero the child out).
    // A list that empties out is treated as ABSENT (the child is
    // never zeroed out by a stale file). An explicit `tools` param
    // is NEVER filtered here (its semantics — verbatim,
    // `dispatch_native` minus `subagent`/`list_agents` — are
    // unchanged). `then_some` (NOT `then`) — an absent `tools`
    // stays `None`.
    let specs = tool_specs();
    let tools = launch.tools.clone().or_else(|| {
        def.tools.as_ref().and_then(|t| {
            let known: Vec<String> = t
                .iter()
                .filter(|name| {
                    name.as_str() != "subagent"
                        && name.as_str() != "list_agents"
                        && specs.iter().any(|s| s.name == **name)
                })
                .cloned()
                .collect();
            (!known.is_empty()).then_some(known)
        })
    });
    // An EMPTY frontmatter body means "no system prompt" (the
    // `or_else` must not turn it into `Some("")` — `dispatch_native`
    // would prepend an empty message).
    let system_prompt = launch
        .system_prompt
        .clone()
        .or_else(|| (!def.system_prompt.is_empty()).then(|| def.system_prompt.clone()));
    // The thinking split (the doc-correct order — the dispatch resolves
    // explicit > model-key suffix > frontmatter `thinking`): `thinking`
    // holds the EXPLICIT param only; the frontmatter's `thinking` moves
    // to its own field.
    let frontmatter_thinking = def.thinking.clone();
    LaunchConfig {
        system_prompt,
        model,
        thinking: launch.thinking.clone(),
        frontmatter_thinking,
        tools,
    }
}

#[cfg(test)]
mod tests {
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
        space_cwd: PathBuf,
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
        let space_cwd =
            std::env::temp_dir().join(format!("harness-launch-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&space_cwd).unwrap();
        Ctx {
            catalog: ModelCatalog {
                models,
                default_model: None,
                compaction: CompactionConfig::default(),
            },
            config_dir: config_dir.map(|p| p.to_path_buf()),
            space_cwd,
        }
    }

    /// (ADR 0020) `agent_name: ""` → the launch is returned VERBATIM
    /// (the label-only, config-less behavior — never an error).
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
            launch,
            "an empty agentName returns the launch verbatim"
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
        let empty_home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&empty_home).unwrap();
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
            std::env::set_var("HOME", &empty_home);
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
        let resolved = resolve_launch(
            &launch,
            "Scout",
            &ctx.catalog,
            ctx.config_dir.as_deref(),
            &ctx.space_cwd,
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let resolved = resolve_launch(
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
        let config_dir = std::env::temp_dir().join(format!(
            "harness-resolve-launch-settings-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{ "subagentModels": { "scout": "fake/m2" } }"#,
        )
        .unwrap();
        let home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
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
            std::env::set_var("HOME", &home);
        }
        let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
        let resolved = resolve_launch(
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
        let config_dir = std::env::temp_dir().join(format!(
            "harness-resolve-launch-settings-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{ "subagentModels": { "scout": "fake/m2" } }"#,
        )
        .unwrap();
        let home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
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
            std::env::set_var("HOME", &home);
        }
        let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
        let launch = LaunchConfig {
            model: Some("fake/m1".to_string()),
            ..Default::default()
        };
        let resolved = resolve_launch(
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
        let config_dir = std::env::temp_dir().join(format!(
            "harness-resolve-launch-settings-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{ "subagentModels": { "scout": "fake/nope" } }"#,
        )
        .unwrap();
        let home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
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
            std::env::set_var("HOME", &home);
        }
        let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
        let resolved = resolve_launch(
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
        let config_dir = std::env::temp_dir().join(format!(
            "harness-resolve-launch-settings-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{ "subagentModels": { "Scout": "fake/m2" } }"#,
        )
        .unwrap();
        let home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
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
            std::env::set_var("HOME", &home);
        }
        let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
        let resolved = resolve_launch(
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
        let config_dir = std::env::temp_dir().join(format!(
            "harness-resolve-launch-settings-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(
            config_dir.join("settings.json"),
            r#"{ "subagentModels": {} }"#,
        )
        .unwrap();
        let home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
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
            std::env::set_var("HOME", &home);
        }
        let ctx = build_ctx(vec![fake_model("m1"), fake_model("m2")], Some(&config_dir));
        let resolved = resolve_launch(
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
        let home = std::env::temp_dir().join(format!(
            "harness-resolve-launch-home-{}",
            uuid::Uuid::new_v4()
        ));
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
            std::env::set_var("HOME", &home);
        }
        let ctx = build_ctx(vec![fake_model("m1")], None);
        let resolved = resolve_launch(
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
}

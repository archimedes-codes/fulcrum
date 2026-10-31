//! The launch-resolution layer (ADR 0020 / 0023 / 0066 — extracted from
//! `r#loop` in the loop.rs decomposition): resolve a subagent's
//! `agentName` against the discovered Agent definitions and layer the
//! frontmatter UNDER the explicit launch params, with the global
//! `agentOverrides` layer (ADR 0066) between them. A PURE function of its
//! inputs (the catalog, the settings dir, and the space cwd — no loop
//! state), hence its own module.
//!
//! The chain is `explicit subagent param > override > frontmatter > parent
//! defaults` (ADR 0067), and each layer supplies each of `model` / `thinking`
//! / `tools` INDEPENDENTLY. `resolve_launch` is the dispatch's entry point, and
//! it also REPORTS whether the `agentName` resolved (issue #47 — the tri-state
//! that lets the UI say "no matching Agent" without changing what runs);
//! `resolve_agent_launch` is the seam the Agents modal's command shares
//! (ADR 0068), so the modal cannot print a config the next dispatch will not
//! run.

use std::path::Path;

use serde::Serialize;

use crate::agent::definition_permission::ToolKey;
use crate::agent::harness::catalog::ModelCatalog;
use crate::agent::harness::tool_specs;
use crate::agent::harness::ModelRef;
use crate::agent::subagent::{AgentNameResolution, LaunchConfig};
use crate::agents::AgentDefinition;
use crate::config::{AgentOverrideFields, Settings};

/// The override layer (ADR 0066: ONE global map in `settings.json`, so ONE
/// rung — there is no per-Space layer to merge, and ADR 0066's *Deferred:
/// per-Space* door is a second field on this struct plus one more `or_else`).
///
/// `global: None` covers three causes with one state — no settings file at all
/// (the `config_dir: None` case: a layer that cannot speak, not an empty map),
/// an empty map, and no entry under any casing of the name. All three mean
/// "this layer has no opinion"; none of them may be confused with `Some(entry)`
/// by a caller.
pub(crate) struct OverrideLayer<'a> {
    pub global: Option<&'a AgentOverrideFields>,
}

/// The resolver's output: the config to launch with, plus WHICH LAYER supplied
/// each field (ADR 0068 — the Agents modal prints the source beside the value,
/// so a user can ask "why is scout running that model").
///
/// The three `*_source` fields have TWO readers, and that sharing is the point:
/// `commands::agents::agent_definition_reports` (the modal) and `resolve_launch` (the
/// dispatch) both go through `resolve_agent_launch`, so a layer added to one
/// and forgotten in the other is the drift this struct exists to prevent.
pub(crate) struct ResolvedLaunch {
    pub launch: LaunchConfig,
    /// Which layer supplied `model`; `Parent` when none did.
    pub model_source: OverrideSource,
    /// Which layer supplied `thinking`; `Parent` when none did. A demoted
    /// override level (one the resolved model does not advertise) is NEITHER
    /// written NOR reported, so this never reads `Override` for a level the
    /// child will not use.
    pub thinking_source: OverrideSource,
    /// Which layer supplied `tools`; `Parent` when none did.
    pub tools_source: OverrideSource,
    /// Which layer supplied `permission`; `Parent` when the definition does not
    /// speak about permissions. There is NO settings.json permission layer
    /// (ADR 0066's scope is the file), so this is `File` exactly when
    /// `launch.permission.is_some()`.
    pub permission_source: OverrideSource,
}

/// Which layer supplied a resolved field — the label space of the Agents modal
/// (`override` / `file` / `parent`, ADR 0068) and NOTHING else. A fourth layer
/// (ADR 0066's per-Space door) means adding a variant here, and the exhaustive
/// match in Task 2's test is what makes that a compile error rather than a
/// silent label.
///
/// `pub`, NOT `pub(crate)`: Task 2's `AgentDefinitionReportDto` is `pub` and carries a
/// source struct, and clippy's `private_interfaces` fires transitively down
/// that graph under CI's `-D warnings`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OverrideSource {
    /// `settings.json`'s `agentOverrides` (ADR 0066).
    Override,
    /// The Agent definition's frontmatter.
    File,
    /// Nothing BELOW the explicit `subagent` param spoke, so the child
    /// inherits — which is NOT a promise: a dispatch that passes `model:`
    /// overrides what a `parent` row shows, hence the modal's
    /// `Inherits parent` wording (ADR 0068). The label space has no word for
    /// "the explicit param itself" (no live modal can observe one, and
    /// `list_agent_definition_reports` passes an empty `LaunchConfig`), so a field the
    /// explicit param supplied reports `Parent`.
    Parent,
}

/// Pick the stored entry for `agent_name` out of the settings (ADR 0066).
///
/// THE one place that decides which stored entry applies — the dispatch and
/// Task 2's `list_agent_definition_reports` both call it, so they cannot disagree about
/// what the modal shows versus what runs. The scan is CASE-INSENSITIVE (ADR
/// 0023's rule, moved out of the old `model` arm because it now has to find the
/// entry ONCE for three fields) and the STORED KEY is never rewritten: the UI
/// writes `def.name` verbatim, and a hand-edited mixed-case key still matches.
///
/// The named lifetime is REQUIRED, and `'_` does not substitute for it: with
/// TWO elided reference inputs (the settings AND `agent_name`) rustc cannot
/// tell which one the returned borrow comes from and refuses the signature with
/// E0106 ("missing lifetime specifier") — verified on the pinned toolchain.
pub fn override_layer<'a>(settings: Option<&'a Settings>, agent_name: &str) -> OverrideLayer<'a> {
    OverrideLayer {
        // `None` settings (no file) and "no entry" both give `global: None` —
        // see `OverrideLayer`'s note on the three collapsed causes.
        global: settings.and_then(|settings| {
            settings
                .agent_overrides
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(agent_name))
                .map(|(_, entry)| entry)
        }),
    }
}

/// The FIRST rung that supplies a value, plus the name of the layer that
/// supplied it (`Parent` when none did).
///
/// Every field of the chain runs through here, so the rungs of ONE field are
/// listed in ONE expression — and a new layer (ADR 0066's *Deferred: per-Space*
/// door: one more map folded at load time) joins that list in order, in the one
/// place a reviewer can see it. A layer added to the dispatch and forgotten in
/// the modal is the drift ADR 0068 exists to prevent; a shared list of rungs is
/// the shape that makes it hard to do by accident.
///
/// A rung whose value needs no filtering (the explicit `subagent` params) is
/// passed through UNFILTERED as the first rung; a filtered or soft-degraded
/// candidate arrives as `None` when it does not survive, which is precisely
/// "this layer does not speak" (ADR 0067).
fn first_speaking<T>(
    rungs: impl IntoIterator<Item = (OverrideSource, Option<T>)>,
) -> (Option<T>, OverrideSource) {
    rungs
        .into_iter()
        .find_map(|(source, value)| value.map(|value| (Some(value), source)))
        .unwrap_or((None, OverrideSource::Parent))
}

/// Resolve a launch against the definition + the override layer: per field,
/// the FIRST layer that supplies a value wins (`explicit > override >
/// frontmatter > parent`), each field independently.
///
/// This is the shared seam ADR 0068 asks for — `resolve_launch` (the dispatch)
/// and `list_agent_definition_reports` (the modal) call it, so filtering and soft
/// degradation exist in exactly one place.
pub(crate) fn resolve_agent_launch(
    explicit: &LaunchConfig,
    def: &AgentDefinition,
    layer: &OverrideLayer<'_>,
    catalog: &ModelCatalog,
) -> ResolvedLaunch {
    // ── model ───────────────────────────────────────────────────────────
    // The explicit param is NEVER degraded (it is the model's current intent
    // — `dispatch_native` still fails an unknown one, unchanged). Below it,
    // the override and the frontmatter are SOFT: a value whose bare key is not
    // in the catalog is SKIPPED, never a failed dispatch (ADR 0023, kept by
    // ADR 0066). The `:<level>` suffix is stripped ONLY for the resolvability
    // check (`ModelRef::parse` — the last-`:` strip, mirroring
    // `dispatch_subagent`'s; a ref whose bare part has no `/` parses to `None`
    // and can never be in the catalog, so it degrades identically to the old
    // whole-string behaviour); the value that survives is the VERBATIM string,
    // because the suffix is a thinking-level candidate handled downstream.
    let resolve = |candidate: &str| {
        let bare = ModelRef::parse(candidate)
            .map(|r| r.key.to_string())
            .unwrap_or_else(|| candidate.to_string());
        crate::agent::session::resolve_composed_model(catalog, &bare)
    };
    let soft = |candidate: &str| resolve(candidate).is_some();
    // The rungs, in order: the explicit param (NEVER degraded — see above), the
    // override, the frontmatter.
    let (model, model_source) = first_speaking([
        (OverrideSource::Parent, explicit.model.clone()),
        (
            OverrideSource::Override,
            layer
                .global
                .and_then(|o| o.model.as_ref())
                .filter(|m| soft(m))
                .cloned(),
        ),
        (
            OverrideSource::File,
            def.model.as_ref().filter(|m| soft(m)).cloned(),
        ),
    ]);
    // ── thinking ────────────────────────────────────────────────────────
    // WRITE and REPORT are one decision here, and only the OVERRIDE level may
    // ever enter `LaunchConfig::thinking`: the dispatch's chain is
    // `launch.thinking` > the model key's `:<level>` suffix >
    // `frontmatter_thinking` (`subagent_flow.rs`), so writing the FILE's value
    // or the suffix into that field would promote it to the TOP rung and change
    // behaviour that exists today (`frontmatter_thinking` keeps carrying ONLY
    // the frontmatter's own value, untouched).
    //
    // The candidate is validated against the model the MODEL layer JUST
    // resolved, because the dispatch validates after its own chain and DROPS a
    // level the model does not advertise — so writing/reporting an overridden
    // level the child never uses is exactly the ADR 0068 lie. A DEMOTED level is
    // neither written nor reported, which is why there is no `suppressed_
    // thinking` field: the demotion is fully assertable as `thinking == None`
    // plus `source != Override`. (An explicit param, when present, is never
    // filtered — its own validation belongs to the dispatch.)
    let advertises = |level: &str| {
        // No model layer spoke → the child runs the PARENT's model, which this
        // function cannot see. SOFT-PASS (write + report), exactly as
        // `Model::supports_thinking_level` soft-passes an EMPTY advertised set
        // (`catalog::supports_thinking_level_empty_set_soft_passes`): the
        // dispatch still filters the level against the model it resolved, and
        // demoting here would silently discard a level the parent model almost
        // certainly does advertise.
        model
            .as_ref()
            .and_then(|m| resolve(m))
            .is_none_or(|m| m.supports_thinking_level(level))
    };
    let (thinking, thinking_source) = match explicit.thinking.clone() {
        Some(level) => (Some(level), OverrideSource::Parent),
        None => match layer.global.and_then(|o| o.thinking.as_ref()) {
            Some(level) if advertises(level) => (Some(level.clone()), OverrideSource::Override),
            // Absent, or demoted because the winning model lacks it.
            _ => (None, OverrideSource::Parent),
        },
    };
    // ── tools ───────────────────────────────────────────────────────────
    // An explicit `tools` param is NEVER FILTERED HERE (its semantics —
    // verbatim minus `subagent`/`list_agents`, `dispatch_native` — are
    // unchanged, and it stays the ONLY layer that can hand a child zero tools
    // BY ITSELF; the deny drop below is the other thing that can empty a list).
    // The override and the frontmatter share BOTH rules (ADR 0067): unknown
    // names dropped, `subagent` / `list_agents` stripped so no layer grants a
    // child a delegation route, and a list that empties out treated as ABSENT —
    // so no override can zero out a child by a typo or a stale name. That
    // emptied-means-ABSENT rule stays EXACTLY here (it guards accidental
    // narrowing) and deliberately does NOT extend to the deny drop below, which
    // empties by INTENT and therefore yields `Some(vec![])`.
    let specs = tool_specs();
    let narrow = |names: &[String]| {
        let known: Vec<String> = names
            .iter()
            .filter(|name| {
                name.as_str() != "subagent"
                    && name.as_str() != "list_agents"
                    && specs.iter().any(|s| s.name == **name)
            })
            .cloned()
            .collect();
        (!known.is_empty()).then_some(known)
    };
    let (tools, tools_source) = first_speaking([
        // The explicit list is passed VERBATIM — this layer is the only one
        // that can hand a child zero tools (ADR 0067).
        (OverrideSource::Parent, explicit.tools.clone()),
        (
            OverrideSource::Override,
            layer
                .global
                .and_then(|o| o.tools.as_ref())
                .and_then(|t| narrow(t)),
        ),
        (
            OverrideSource::File,
            def.tools.as_ref().and_then(|t| narrow(t)),
        ),
    ]);
    // (issue #101) A whole-tool `deny` in the file's `permission:` block drops
    // the tool from the child's advertised set BEFORE the wire — the
    // `subagent_flow` three-way rule re-advertises `launch.tools` VERBATIM, so
    // the model is never offered a call the gate would deny. Only whole-tool
    // denies act here (a `Rules::is_deny()` on the Ordered `Vec`); per-command
    // pattern rows announce as unsupported and are NOT advertisement-changed.
    //
    // TWO HONEST LIMITS of this pre-wire filter, one of them closed by the rule
    // below (the gate half, task05b, closes the other; neither is papered over):
    //   * It acts only on a list the resolver PRODUCED. A whole-tool deny where
    //     `tools:` is ABSENT (the `None` rung — "no allowlist, whatever the
    //     parent offers") has no list to filter, so that leg is unhandled here.
    //   * The filter also runs over a list that came from the explicit
    //     `subagent` param. That is deliberate: a file may only ever make a
    //     child MORE restricted, so dropping a denied name from the explicit
    //     list narrows rather than grants — and when it empties that list the
    //     result is ZERO tools, the honest reading of "you may only use bash"
    //     plus "bash is denied" (= nothing). Zero tools is still NARROWER than
    //     the parent's set, so narrowing-only holds.
    //
    // A list the deny empties yields `Some(vec![])` — ZERO tools — and NOT
    // `None`. This is deliberately NOT `narrow`'s ADR 0067 emptied-means-ABSENT
    // rule, and the distinction matters: ADR 0067 exists to stop a TYPO or a
    // STALE NAME in an allowlist from accidentally zeroing a child, i.e. it
    // guards against UNINTENDED narrowing. Here the emptiness is produced by a
    // DELIBERATE `deny`, so that rationale does not apply — and falling back to
    // ABSENT is strictly worse than what ADR 0067 guards against, because
    // `None` is not "no tools": `subagent_flow.rs`'s three-way rule reads it as
    // "the parent's list minus the guard". So the old `None` fallback made the
    // MOST restrictive possible file (every advertised tool denied) the LEAST
    // restrictive outcome — adding a deny GAVE THE CHILD MORE TOOLS than the
    // same file without the deny, the exact inverse of issue #101's invariant
    // ("a definition can make its own agent stricter … it can never grant past
    // it") and of its acceptance item "drop the tool from the child's
    // `enabled_tools` … so the model is never offered a call it can't make".
    // The empty set is what the downstream actually means by "no tools":
    //   * `subagent_flow.rs`'s `Some(tools)` arm filters `subagent` /
    //     `list_agents` and passes the rest VERBATIM, and its comment says so —
    //     "a non-empty set that empties out yields NO tools — NOT re-expanded";
    //   * `worker/core.rs` applies `enabled_tools` VERBATIM — "a legitimately-
    //     empty child tool set must NOT re-expand into all tools — the recursion
    //     guard";
    //   * the `[]` = ALL expansion lives in the `None` arm and reads
    //     `wire.parent_enabled_tools` — a DIFFERENT input (the inherited-parent
    //     list), never this launch field.
    let tools = tools.map(|mut names| {
        if let Some(block) = def.permission.as_ref() {
            for (key, rules) in &block.tools {
                if rules.is_deny() {
                    if let Some(name) = tool_key_name(key) {
                        names.retain(|n| n != name);
                    }
                }
            }
        }
        // `Some` in, `Some` out — an emptied list stays `Some(vec![])` (see
        // above). A `deny` may never convert a list into ABSENT.
        names
    });
    // ── permission ────────────────────────────────────────────────────────
    // A file-declared ruleset (issue #101). NO rung ladder: there is no
    // settings.json `permission` key (the issue's scope is "what a file
    // declares"), and the explicit `subagent` call has no permission param —
    // so the layer is EXACTLY `File` when the file speaks and `Parent`
    // otherwise, reusing the existing three `OverrideSource` variants. Never
    // a `first_speaking` candidate: no `OverrideSource::Permission` variant
    // exists, and `the_source_label_space_is_exactly_three_layers_and_never_space`
    // pins the three-variant enum by a compile-error-exhaustive match.
    let (permission, permission_source) = match def.permission.clone() {
        // BOXED on the way onto the launch (see `LaunchConfig::permission`):
        // the box keeps the wire enums that embed `LaunchConfig` under clippy's
        // `large_enum_variant` threshold. Serde-transparent, so the JSON is the
        // unboxed block the plan specifies.
        Some(block) => (Some(Box::new(block)), OverrideSource::File),
        None => (None, OverrideSource::Parent),
    };
    // An EMPTY frontmatter body means "no system prompt" (the `or_else` must not
    // turn it into `Some("")` — `dispatch_native` would prepend an empty
    // message). The system-prompt layering is unchanged by ADR 0066 (an
    // override cannot speak about prompts).
    let system_prompt = explicit
        .system_prompt
        .clone()
        .or_else(|| (!def.system_prompt.is_empty()).then(|| def.system_prompt.clone()));
    ResolvedLaunch {
        launch: LaunchConfig {
            system_prompt,
            model,
            thinking,
            frontmatter_thinking: def.thinking.clone(),
            tools,
            permission,
        },
        model_source,
        thinking_source,
        tools_source,
        permission_source,
    }
}

/// The advertised `tool_specs()` NAME of a recognised `ToolKey`, `None` for
/// an unrecognised one (they stay parsed + announced; their enforcement has no
/// advertised twin). The name class is the `tool_specs()` set; a key NOT in it
/// has no advertised name to drop.
fn tool_key_name(key: &ToolKey) -> Option<&'static str> {
    match key {
        ToolKey::Bash => Some("bash"),
        ToolKey::Read => Some("read"),
        ToolKey::Write => Some("write"),
        ToolKey::Edit => Some("edit"),
        ToolKey::Find => Some("find"),
        ToolKey::Grep => Some("grep"),
        ToolKey::Ls => Some("ls"),
        ToolKey::ReadSkill => Some("read_skill"),
        ToolKey::Ask => Some("ask"),
        ToolKey::WebSearch => Some("web_search"),
        ToolKey::FetchContent => Some("fetch_content"),
        ToolKey::Unrecognized(_) => None,
    }
}

/// Resolve `agentName` against the discovered Agent definitions and layer the
/// override + the frontmatter UNDER the explicit launch params (ADR 0020 /
/// 0066 — explicit > override > frontmatter > parent defaults). `agentName`
/// empty / no match → the `launch` is returned VERBATIM (the label-only,
/// config-less behavior — never an error).
///
/// The return gained the `AgentNameResolution` (issue #47) because a MISS must
/// be reportable on the wire: the verbatim return made "no name given" and "a
/// name that matched nothing" BYTE-IDENTICAL exits, so a child that loaded no
/// persona, no tool allowlist and no model was displayed under a confident
/// agent label. The BEHAVIOUR is untouched — both non-matched exits still hand
/// back `launch` unchanged and the child still runs.
///
/// `resolve_agent_launch` — the seam ADR 0068 shares with the Agents modal — is
/// deliberately NOT part of that report: the modal resolves a definition it
/// already HAS, so it can never have a NAME miss, only a field miss, which its
/// `source` column already reports. The ADR 0066 layer is added by DELEGATING
/// to that shared resolver, so the `config_dir: None` path goes through the
/// same selector as the modal instead of hand-rolling "no layer".
pub(crate) fn resolve_launch(
    launch: &LaunchConfig,
    agent_name: &str,
    catalog: &ModelCatalog,
    config_dir: Option<&Path>,
    space_cwd: &Path,
) -> (LaunchConfig, AgentNameResolution) {
    let name = agent_name.trim();
    if name.is_empty() {
        // Deliberate config-less dispatch — NOT a miss. Pinned by
        // `resolve_launch_with_an_empty_agent_name_short_circuits`.
        return (launch.clone(), AgentNameResolution::NoName);
    }
    let Some(def) = crate::agents::discover_agents(Some(space_cwd))
        .into_iter()
        .find(|d| d.name.eq_ignore_ascii_case(name))
    else {
        // (issue #47) The verbatim return is ADR 0020's and stays; what was
        // missing was that this exit was BYTE-IDENTICAL to the one above, so
        // nothing downstream could tell a typo from a deliberate nameless call.
        return (launch.clone(), AgentNameResolution::Unmatched);
    };
    // TWO bindings, never one. The one-statement form
    // `override_layer(config_dir.map(load_settings).as_ref(), name)` is E0716
    // ("temporary value dropped while borrowed"): `map` yields an owned
    // `Option<Settings>` temporary, `.as_ref()` borrows it, and the returned
    // `OverrideLayer<'_>` still borrows it when the statement ends. Verified
    // against the pinned toolchain.
    //
    // Read at dispatch time (ADR 0023's property, kept): a settings edit takes
    // effect on the NEXT dispatch, no restart; a missing/corrupt file yields
    // the defaults → no override.
    let settings = config_dir.map(crate::config::load_settings);
    let layer = override_layer(settings.as_ref(), name);
    (
        resolve_agent_launch(launch, &def, &layer, catalog).launch,
        AgentNameResolution::Matched,
    )
}

#[cfg(test)]
mod tests;

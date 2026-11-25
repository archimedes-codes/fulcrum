use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::events::RpcEvent;
use crate::harness::provider::{FinishReason, ProviderEvent, ToolCall};
use crate::harness::retry::RetryPolicy;

/// (finding 13b) `enabled_tools` is wired into `dispatch_tool`:
/// a disabled tool (`bash`, with only `read` enabled) is a
/// tool-result error, NOT executed; an enabled tool (`read`) is
/// still dispatched. A `None` `enabled_tools` (the default) leaves
/// every tool dispatchable (the current behavior).
#[tokio::test]
async fn a_disabled_tool_is_a_tool_result_error_not_execution() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.set_enabled_tools(Some(vec!["read".to_string()]));
    // `bash` is disabled (only `read` is enabled) → a tool-result
    // error, NOT executed.
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert!(r.is_error, "the disabled tool is an error result");
    assert_eq!(
        result_text(&r),
        "tool not enabled: bash",
        "the error names the disabled tool"
    );
    // `read` is enabled → still dispatched (the executor's own
    // result for a missing file — NOT a "tool not enabled" error).
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t2".to_string(),
                name: "read".to_string(),
                arguments: json!({ "path": "/nonexistent-file" }),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert_ne!(
        result_text(&r),
        "tool not enabled: read",
        "an enabled tool is dispatched"
    );
    // A `None` `enabled_tools` (the default) → every tool
    // dispatchable (the current behavior).
    loop_.set_enabled_tools(None);
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t3".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "echo hi" }),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert_ne!(
        result_text(&r),
        "tool not enabled: bash",
        "a `None` `enabled_tools` enables every tool"
    );
}

/// (finding 13b) End-to-end: a native session with
/// `enabled_tools: ["read"]` cannot dispatch `ls` (a non-mutating
/// tool — no permission gate) — the model gets a `tool not enabled`
/// tool-result error and the turn settles.
#[tokio::test]
async fn a_native_session_with_enabled_tools_cannot_dispatch_a_disabled_tool() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "ls".to_string(),
                arguments: json!({ "path": "/tmp" }),
                defect: None,
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    loop_.set_enabled_tools(Some(vec!["read".to_string()]));
    loop_.handle_prompt(&text_prompt("hello")).await;
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `ls`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "the disabled tool is an error result");
    assert_eq!(
        result["content"][0]["text"], "tool not enabled: ls",
        "the model gets a tool-result error (the tool was NOT executed)"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// `dispatch_subagent` is an ALIAS of `subagent` (the `dispatch_tool`
/// `match` routes both to `dispatch_subagent`): the `enabled_tools`
/// gate must know the alias — a parent with
/// `enabled_tools: Some(["subagent"])` must NOT get
/// `tool not enabled: dispatch_subagent` when the model emits the
/// alias (here `subagent: None` → the "not available" dispatch error
/// instead). A parent with `enabled_tools: Some(["read"])` (subagent
/// disabled) still gets the "not enabled" error for the alias.
#[tokio::test]
async fn the_dispatch_subagent_alias_is_gated_by_the_subagent_enabled_tool() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // `subagent` enabled → the alias is enabled too: NOT a
    // "tool not enabled" error (here `subagent: None` → the
    // "not available" dispatch error instead).
    loop_.set_enabled_tools(Some(vec!["subagent".to_string()]));
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "dispatch_subagent".to_string(),
                arguments: json!({ "task": "do the thing" }),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert_eq!(
        result_text(&r),
        "subagent dispatch is not available in this session",
        "the alias is enabled when `subagent` is (the `subagent: None` manager is the \"not available\" error, NOT \"not enabled\")"
    );
    // `subagent` disabled (only `read` enabled) → the alias is
    // still disabled: the "not enabled" error.
    loop_.set_enabled_tools(Some(vec!["read".to_string()]));
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t2".to_string(),
                name: "dispatch_subagent".to_string(),
                arguments: json!({ "task": "do the thing" }),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert_eq!(
        result_text(&r),
        "tool not enabled: dispatch_subagent",
        "the alias is disabled when `subagent` is disabled"
    );
}
/// (ADR 0020) `list_agents` is advertised to a native PARENT: it is in
/// `tool_specs()` with its description, and in the parent's advertised
/// specs (the `enabled_tools` filter `None` = all).
#[test]
fn list_agents_is_in_the_default_tool_specs() {
    let specs = tool_specs();
    let spec = specs
        .iter()
        .find(|s| s.name == "list_agents")
        .expect("`list_agents` in `tool_specs`");
    assert_eq!(
        spec.description,
        "List available subagent configurations (name, description, source, model/tools overrides). Call before dispatching if unsure which agents exist or which fits the task."
    );
    // (ADR 0102 F2) The third argument is the loop's REGISTRY, and `&[]` here is
    // the honest fixture: "no host entries". These pins are about the ENGINE table
    // half of the composed advertised set, and `&[]` is also exactly what a loop
    // that was never given a registry carries — so the assertion reads "with no host
    // tools, the advertised set is `tool_specs()`", which is F2's additivity claim.
    // `the_composed_advertised_set_reaches_its_executors` below is the half with a
    // host entry in it.
    let advertised = AgentLoop::advertised_tool_specs(&None, false, &[]);
    assert!(
        advertised.iter().any(|s| s.name == "list_agents"),
        "a native parent advertises `list_agents`"
    );
}

/// (ADR 0036) the `list_skills` description says which SET it returns. It
/// said "the discoverable Skills", but DISCOVERABLE is this repo's word for
/// the TOTAL set (`CONTEXT.md`'s Skill entry — discovery is total so the
/// `$` Mention route survives; pinned by
/// `discovery_still_returns_model_hidden_skills` in `tests/skills.rs`),
/// while this tool returns the model-VISIBLE subset. The word is the kind of
/// thing a later reader "corrects" back toward discovery, so the description
/// is pinned byte-for-byte like the `read_skill` one.
#[test]
fn list_skills_description_names_the_model_visible_subset() {
    let specs = tool_specs();
    let spec = specs
        .iter()
        .find(|s| s.name == "list_skills")
        .expect("`list_skills` in `tool_specs`");
    assert_eq!(
        spec.description,
        "List the model-visible Skills (name, scope, description) — the catalog the \
         `<skills>` system-prompt section advertises, re-read from disk."
    );
    // "discoverable" is the TOTAL set — it must not return to a sentence
    // about the filtered one.
    assert!(
        !spec.description.contains("discoverable"),
        "`list_skills` returns the model-visible SUBSET, so its description \
         must not use the repo's word for the total set: {}",
        spec.description
    );
}

/// (ADR 0036) the `read_skill` description tells the truth. It used to
/// claim that `read` on a skill path "always fails with `path escapes the
/// session sandbox`" — which died with ADR 0030's `boundary::read_roots`,
/// grants the read Boundary the skill discovery roots, so a `read` on a
/// skill path IS in bounds in every policy. A `ToolSpec.description` is
/// printed VERBATIM into every session's `<tools>` section and the model
/// ACTS on it (it skips a `read` that would work), so the text is pinned
/// byte-for-byte — and the negative guards are what keep the dead lie from
/// creeping back in through a reworded paragraph, alongside the assertion
/// that the model-hidden paragraph covers BOTH name shapes (a `$name`
/// Mention, or an explicit load for a name the token grammar cannot carry).
#[test]
fn read_skill_description_is_honest_about_read_and_model_hidden() {
    let specs = tool_specs();
    let spec = specs
        .iter()
        .find(|s| s.name == "read_skill")
        .expect("`read_skill` in `tool_specs`");
    assert_eq!(
        spec.description,
        "Load a Skill's instructions by NAME — the sanctioned way to read a skill file.\
         \n\
         \n\
         `name` matches a discovered Skill (see list_skills, or the `<available_skills>` section); no `path` returns that skill's `SKILL.md` body. `path` reads a file BUNDLED with that skill, resolved against the skill's directory — it must stay inside it.\
         \n\
         \n\
         A Skill whose frontmatter sets `disable-model-invocation: true` is model-hidden: it is not advertised and this tool refuses it — ask the user to send it as a `$name` Mention, or to load it explicitly if its name is outside the mention token grammar.\
         \n\
         \n\
         Skills live OUTSIDE the session `cwd` (`~/.agents/skills`, `.agents/skills`). The skill tools are the sanctioned route: `read_skill` resolves a NAME against the discovered catalog and confines every read to that skill's own directory."
    );
    // The ADR 0030 lie must not return, in any wording that keeps it.
    assert!(
        !spec.description.contains("always fails"),
        "the description must not claim a `read` on a skill path always fails"
    );
    assert!(
        !spec.description.contains("CANNOT reach"),
        "the description must not claim `read` cannot reach a skill path"
    );
    // The paragraph must cover BOTH name shapes. This text is printed into
    // the `<tools>` section BEFORE any call, so a blanket "send $name" is
    // acted on for a name like `has space`, whose `$has space` token
    // expands NOTHING (`is_mentionable_name`) — the model would send the
    // user on a dead errand instead of loading it explicitly.
    assert!(
        spec.description
            .contains("outside the mention token grammar"),
        "the description must not promise a `$name` route a name outside the \
         token grammar cannot take: {}",
        spec.description
    );
}

/// The DEFAULT child's full tool set, pinned (both halves).
///
/// (ADR 0020) a subagent CHILD gets neither `subagent` (the recursion
/// guard) nor `list_agents` (a child cannot dispatch, so listing
/// dispatch targets is pointless token burn). The CONTRAST assertions
/// (the parent advertises BOTH) are what keep this test from passing
/// VACUOUSLY — without them, "the child drops `list_agents`" is
/// trivially true while the spec is not in `tool_specs()` yet.
///
/// (ADR 0036) The other half of the child story is pinned HERE too, in the
/// same shape: a child holds BOTH skill tools (`read_skill`/`list_skills`).
/// The DEFAULT child's message is `build_child_system_message`'s REDUCED
/// one, which carries no `<skills>` section (ADR 0017), so for THAT shape the
/// TOOLS are the only surface left that filters — which holds only while a
/// child still HAS them. (And the one child shape with no reduced message at
/// all — no launch/definition prompt AND no `manage_todo_list` in the
/// resolved child tools, so `build_child_system_message` returns `None` — now
/// gets NO system message AT ALL: `worker/core.rs`'s `(StartMode::FreshChild,
/// None)` arm prepends nothing (issue #9). So NO child shape carries a
/// `<skills>` section, and the TOOLS are the ONLY filter for EVERY child
/// shape; ADR 0036's subagent bullet (amended) owns that statement.)
/// Drop either spec and the DEFAULT child would have neither a catalog nor
/// the tool that filters, and the ADR's claim would be silently false rather
/// than tested.
///
/// SCOPE: this pins the DEFAULT child — `enabled_tools: None`, i.e. every
/// spec minus the two dispatch tools. An Agent definition that names a
/// `tools:` list may legitimately drop the skill tools (that is ADR 0020's
/// allowlist working), so "a child always has skill tools" is NOT what is
/// claimed here.
#[test]
fn the_child_tool_set_is_pinned() {
    // (F2) `&[]` = the registry half contributes nothing, i.e. no host tools — the
    // state these child-set pins were written against.
    let child = AgentLoop::advertised_tool_specs(&None, true, &[]);
    assert!(
        !child.iter().any(|s| s.name == "subagent"),
        "a child does not advertise `subagent`"
    );
    assert!(
        !child.iter().any(|s| s.name == "list_agents"),
        "a child does not advertise `list_agents`"
    );
    // CONTRAST: the parent advertises BOTH.
    let parent = AgentLoop::advertised_tool_specs(&None, false, &[]);
    assert!(
        parent.iter().any(|s| s.name == "subagent"),
        "a parent advertises `subagent` (contrast)"
    );
    assert!(
        parent.iter().any(|s| s.name == "list_agents"),
        "a parent advertises `list_agents` (contrast)"
    );
    // ADR 0036's PREMISE for the DEFAULT child pinned here: its reduced
    // message (ADR 0017) carries no skill CATALOG, so the skill tools are
    // the ONLY surface left that can filter a model-hidden skill — and a
    // child must still hold both of them, or "children inherit the
    // filtering for free" is an untested claim. (The contrast lives in the
    // assertions above: this set IS reduced — it lost the two dispatch
    // tools — yet neither skill tool went with them.) The `None`
    // fall-through child (no prompt AND no `manage_todo_list`) now gets NO
    // system message at all (issue #9), so it has no section either — the
    // tool-side filter is the whole child story for EVERY child shape, and
    // ADR 0036's subagent bullet (amended) states it; what the assertions here
    // pin is the DEFAULT shape's tool set.
    assert!(
        child.iter().any(|s| s.name == "read_skill"),
        "a child still advertises `read_skill` — the tool that refuses a \
         model-hidden skill by name (ADR 0036's child premise)"
    );
    assert!(
        child.iter().any(|s| s.name == "list_skills"),
        "a child still advertises `list_skills` — the tool that omits a \
         model-hidden skill from the list (ADR 0036's child premise)"
    );
}

/// (ADR 0020) the `list_agents` handler lists the DISCOVERED Agent
/// definitions (one line each: `name (scope): description [+ the
/// model / thinking / tools notes]`). The user-level roots are
/// ISOLATED first (a developer's real `~/.agents/agents` /
/// `~/.pi/agent/agents` files would add lines and break the exact
/// assertion: hold `ENV_LOCK` for the WHOLE set→assert→restore span;
/// `HOME` → an empty scratch; restore via the drop guard).
/// `#[allow(clippy::await_holding_lock)]` is INTENTIONAL: the handler reads
/// `HOME` (via `user_roots`), so the guard must stay held ACROSS the
/// `.await` to serialize against the other HOME-reading tests — dropping
/// it before the await would open a window where a sibling test could
/// flip `HOME` mid-discovery.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn list_agents_tool_lists_discovered_agents() {
    let (provider, _calls) = ScriptedProvider::new(vec![]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (settle_tx, _settle_rx) = watch::channel(0u64);
    let (_space, loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let empty_home =
        crate::test_support::ScratchDir::in_env_temp("harness-list-agents-home-", "lists");
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
    let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\ntools: [read, bash]\n---\nYou are a scout.\n",
    )
    .unwrap();
    let result = loop_.list_agents_tool().await;
    assert_eq!(
        result_text(&result),
        "scout (space): Fast recon. [model: fake/m2, tools: read, bash]"
    );
}

/// (ADR 0020) with NO discovered definitions, the `list_agents` result
/// is `none` (same `ENV_LOCK` + `HOME`-to-empty-scratch isolation —
/// the developer's user-level files must not appear; the guard is held
/// across the `.await` for the same reason as above — `#[allow]` is
/// intentional).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn list_agents_tool_returns_none_when_empty() {
    let (provider, _calls) = ScriptedProvider::new(vec![]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (settle_tx, _settle_rx) = watch::channel(0u64);
    let (_space, loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let empty_home =
        crate::test_support::ScratchDir::in_env_temp("harness-list-agents-home-", "none-empty");
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
    let result = loop_.list_agents_tool().await;
    assert_eq!(result_text(&result), "none");
}

/// (ADR 0020) a block-scalar `description` (`|` with embedded
/// newlines) is FLATTENED to one line in the `list_agents` output
/// (the one-line-per-agent contract holds — the block's internal
/// newlines become single spaces). The agent is written to the
/// space-level `.pi/agents` root (the other space root,
/// `.agents/agents`, is covered by the sibling test above); same
/// `ENV_LOCK` + `HOME`-to-empty-scratch isolation (the developer's
/// user-level files must not appear; the guard is held across the
/// `.await` — `#[allow]` is intentional).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn list_agents_tool_flattens_block_scalar_descriptions() {
    let (provider, _calls) = ScriptedProvider::new(vec![]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (settle_tx, _settle_rx) = watch::channel(0u64);
    let (_space, loop_) = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let empty_home =
        crate::test_support::ScratchDir::in_env_temp("harness-list-agents-home-", "block-scalar");
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
    // The space-level `.pi/agents` root (the other space root,
    // `.agents/agents`, is covered by the sibling test above).
    let agent_file = loop_.space_cwd.join(".pi/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: |\n  line one\n  line two\n---\nYou are a scout.\n",
    )
    .unwrap();
    let result = loop_.list_agents_tool().await;
    let text = result_text(&result);
    assert!(
        text.contains("line one line two"),
        "a block-scalar description is flattened to one line: {text:?}"
    );
    // The one-line-per-agent contract holds: a raw embedded newline
    // leaking from the block scalar would split the output into
    // extra lines (we wrote exactly one agent).
    assert_eq!(
        text.lines().count(),
        1,
        "one line per agent (a leaked block-scalar newline would add lines): {text:?}"
    );
}

/// (issue #180 F1, generalised by ADR 0102 / issue #183 F1) EVERY advertised tool
/// reaches an executor, and what reaches it PARTITIONS the advertised set:
/// **registry set ⊎ loop-internal set = advertised set, and the two are disjoint.**
///
/// # What changed and what did not
///
/// The failure class is unchanged and it is still the one this test exists for: a
/// tool ADVERTISED and UNREACHABLE. `tool_specs()` tells the model the tool exists,
/// the model calls it, and `dispatch_tool` answers `unknown tool` for the whole
/// session — not a compile error, and invisible to the two `tool_specs.rs` param
/// censuses, which compare `tool_specs()` against tables a half-registered tool can
/// join perfectly.
///
/// What changed is the SUBJECT of the set half. It used to be `dispatch_tool`'s
/// built-in OR-list, a list of 19 names written out in the `match`; the disjointness
/// was implicit in nobody having written the same name twice. Now the engine asks a
/// registry, so the reachable set is a VALUE — `loop_.tool_registry()`'s membership —
/// and it can be compared, dropped from, and overlapped with the loop arms. That is
/// why the census is generalised rather than a new test written beside it: the set
/// it needed to reason about did not exist before, and it does now.
///
/// # Why both halves are here and not one
///
/// The set half is cheap and static; the dispatch half is the one that can lie about
/// nothing. A registry entry whose executor exists but whose name never reaches it
/// (a lookup that runs in the wrong place, a name shadowed by a loop arm) is caught
/// only by actually dispatching. So: dispatch every advertised name that is not
/// loop-internal and assert the answer is not the `unknown tool` sentence, THEN
/// assert the partition over the sets.
///
/// # What F3 changed about the sentence this test distinguishes
///
/// F1 wrote the reason for the dispatch half as: "no task store is attached, so a
/// routed task arm answers `no task store` and an unrouted one answers `unknown
/// tool`, and the two sentences are what this test tells apart". That was true at
/// F1 and it is gone now — the task arms do not exist, so neither sentence is
/// reachable from this fixture. What SURVIVES is the distinction itself, with a new
/// pair of sentences: every name this census walks is either a registry entry or a
/// loop arm, so it answers SOMETHING (a param refusal, a cancelled flow, a
/// `tool not enabled` verdict), and only a name in NEITHER half answers `unknown
/// tool`. That is still the one failure this refactor can hide, and it is still only
/// detectable by dispatching.
///
/// What this census CANNOT see is a HOST entry — it iterates `tool_specs()`, which
/// since F3 holds only engine names. That gap is closed by
/// `the_composed_advertised_set_reaches_its_executors` below, which is the same
/// census over the composed registry.
#[tokio::test]
async fn every_advertised_tool_reaches_an_executor() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // The loop-internal names are excluded for the reason their own census gives:
    // their arms are not registry entries, and several would BLOCK on a prompt or
    // dispatch a child rather than answer. `codemode` is excluded because its arm
    // needs a live engine and is covered by `codemode_dispatch_runs_a_script_through_agent_loop`.
    const LOOP_INTERNAL: &[&str] = &[
        "ask",
        "sudo_exec",
        "manage_todo_list",
        "subagent",
        "dispatch_subagent",
        "list_agents",
        "mcp",
        "codemode",
    ];
    let mut checked = 0usize;
    for spec in crate::harness::tool_specs::tool_specs() {
        if LOOP_INTERNAL.contains(&spec.name.as_str()) {
            continue;
        }
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t1".to_string(),
                    name: spec.name.clone(),
                    // `{}` fails every arm's own param validation — which is fine.
                    // An unreachable name produces `unknown tool` instead, and THAT
                    // is the string under test.
                    arguments: json!({}),
                    defect: None,
                },
                &CancellationToken::new(),
            )
            .await
            .0;
        let text = result_text(&r);
        assert!(
            !text.contains("unknown tool"),
            "`{}` is ADVERTISED by `tool_specs()` but `dispatch_tool` routes it to \
             `unknown tool` ({text:?}) — it is in neither the loop's tool registry \
             (`tools/exec/registry.rs`'s `builtins()`) nor the loop-internal arms in \
             `loop/tools.rs`. Before ADR 0102 F1 the message said \"add it to the \
             built-in OR-list\"; there is no OR-list any more.",
            spec.name
        );
        checked += 1;
    }
    assert!(
        checked >= 12,
        "the census must actually walk the advertised set, counted {checked}. The          floor is 12 because since F3 `tool_specs()` carries 19 names and 7 of them          are loop-internal (`ask`, `sudo_exec`, `manage_todo_list`, `subagent`,          `list_agents`, `mcp`, `codemode` — 19 − 7 = 12); `dispatch_subagent` is in          the skip list above but is an ALIAS and not advertised, so it does not          change the arithmetic. Before F3 the floor was 19 against a 26-name table.          A drop here is a name leaving the walked set, not the set changing shape."
    );

    // ── the partition, over the loop's OWN registry ───────────────────────────
    // The registry the loop was built with (`AgentLoop::new` → `builtins()`), read
    // from the loop rather than re-derived — the same reason the dispatch half
    // above drives this loop instead of calling `execute_tool` directly.
    let registry: Vec<&str> = loop_.tool_registry.iter().map(|e| e.name()).collect();
    let advertised: Vec<String> = crate::harness::tool_specs::tool_specs()
        .into_iter()
        .map(|spec| spec.name)
        .collect();

    // Disjoint. A name in BOTH is dispatched by the registry lookup, which runs
    // BEFORE `dispatch_tool`'s `match`, and silently loses its loop arm — the
    // `&mut AgentLoop` half that makes `ask` interactive or `subagent` recursive.
    let shadowed: Vec<&str> = registry
        .iter()
        .filter(|name| LOOP_INTERNAL.contains(name))
        .copied()
        .collect();
    assert!(
        shadowed.is_empty(),
        "{shadowed:?} is BOTH a registry entry and a loop-internal arm — the \
         registry lookup in `dispatch_tool` shadows the loop arm, so the interactive \
         or recursive half of that tool silently stops running",
    );

    // Registry ⊎ loop = advertised, exactly. `dispatch_subagent` is in the
    // `LOOP_INTERNAL` list above but NOT advertised (it is an alias resolved before
    // the `match`), so the union is compared against the advertised set through the
    // advertised names, not by raw set equality.
    let reachable: std::collections::BTreeSet<&str> = registry
        .iter()
        .copied()
        .chain(LOOP_INTERNAL.iter().copied())
        .collect();
    let silent: Vec<&str> = advertised
        .iter()
        .map(String::as_str)
        .filter(|name| !reachable.contains(name))
        .collect();
    assert!(
        silent.is_empty(),
        "advertised and in NEITHER half of the partition: {silent:?} — the loop's \
         registry has {} entries and the loop arms cover {}; every advertised name \
         must be in exactly one of them.",
        registry.len(),
        LOOP_INTERNAL.len(),
    );
    // And the registry's own size is the 12 the split is stated against. A registry
    // that drops a name reddens THIS as well as the walk above and the frozen-name
    // census in `tool_specs.rs` — three independent sentences about the same drop,
    // because `checked >= 12` has exactly zero slack by design.
    //
    // This is the loop's OWN registry, and this fixture was never handed host tools,
    // so it is `builtins()` alone: the 12 engine names. The model still sees 26 in
    // production because `default_build_loop` composes `builtins() ++ host_entries_attached(…)`,
    // and that half is censused by
    // `the_composed_advertised_set_reaches_its_executors` below — not here.
    assert_eq!(
        registry.len(),
        12,
        "the loop's registry carries {} entries, not 12 — {}",
        registry.len(),
        if registry.is_empty() {
            "an EMPTY registry is the loudest possible form of this failure: every \
             built-in the model is advertised now answers `unknown tool`"
                .to_string()
        } else {
            format!("registry: {registry:?}")
        },
    );
}

/// (issue #193, review FIX 5) The defect arm of `unknown_param_error` PRECEDES
/// the "tool not enabled" / "unknown tool" verdicts, and this records that as a
/// decision rather than an accident.
///
/// `run_tool_batch` calls the checker with `advertised_specs()` — the same
/// `enabled_tools` + child filter this module exists to pin — and the checker's
/// head arm runs BEFORE its own spec lookup, so a defective call to a tool the
/// session DISABLED never reaches the enablement verdict. It answers
/// ``invalid arguments for `bash`: …`` where the well-formed call next to it
/// answers ``tool not enabled: bash``.
///
/// Harmless, and deliberately kept: the call is refused either way and NEVER
/// dispatched, and "your arguments did not parse" is the more actionable of the
/// two sentences (a disabled tool's list is not something the model can fix by
/// re-sending, and it may have genuinely mangled the payload too). What this test
/// pins is the PRECEDENCE — the contrast pair below is what makes it a pin and
/// not a tautology, and it lives HERE rather than in `tool_specs`' own module
/// because the faithful fixture is the real `advertised_tool_specs`, which is
/// private to `loop`.
#[test]
fn a_defective_call_to_a_disabled_tool_reports_the_defect_not_the_disablement() {
    // The real filter, exactly as `advertised_specs()` builds it: `read` only.
    let advertised = AgentLoop::advertised_tool_specs(&Some(vec!["read".to_string()]), false, &[]);
    assert!(
        advertised.iter().any(|s| s.name == "read"),
        "the fixture enables `read`"
    );
    assert!(
        !advertised.iter().any(|s| s.name == "bash"),
        "and `bash` is therefore NOT advertised — which is the whole trigger"
    );

    let defective = ToolCall {
        id: "t1".to_string(),
        name: "bash".to_string(),
        arguments: json!({}),
        defect: Some(crate::harness::provider::ArgumentDefect {
            error: "EOF while parsing an object at line 1 column 6".to_string(),
            raw: "{oops".to_string(),
        }),
    };
    let well_formed = ToolCall {
        id: "t2".to_string(),
        name: "bash".to_string(),
        arguments: json!({ "command": "echo hi" }),
        defect: None,
    };

    let defect_verdict = crate::harness::tool_specs::unknown_param_error(&advertised, &defective)
        .expect("a defective call is rejected whatever the tool");
    assert!(
        defect_verdict.starts_with("invalid arguments for `bash`: EOF while parsing"),
        "the DEFECT wins over the disablement verdict: {defect_verdict}"
    );
    assert!(
        !defect_verdict.contains("tool not enabled") && !defect_verdict.contains("unknown tool"),
        "and the two dispatch-stage sentences are exactly what it displaces: \
         {defect_verdict}"
    );
    // THE CONTRAST: the same disabled name, well-formed, is NOT this checker's
    // business at all — rule 1 excuses it and `dispatch_tool` answers
    // `tool not enabled: bash` (pinned by
    // `a_disabled_tool_is_a_tool_result_error_not_execution` above). So it is the
    // defect that changed the answer, not the name.
    assert_eq!(
        crate::harness::tool_specs::unknown_param_error(&advertised, &well_formed),
        None,
        "a well-formed call to a disabled tool stays `dispatch_tool`'s problem"
    );

    // A name nobody ever heard of takes the same precedence for the same reason.
    let invented = ToolCall {
        id: "t3".to_string(),
        name: "frobnicate".to_string(),
        arguments: json!({}),
        defect: Some(crate::harness::provider::ArgumentDefect {
            error: "EOF while parsing an object at line 1 column 6".to_string(),
            raw: "{oops".to_string(),
        }),
    };
    let invented_verdict = crate::harness::tool_specs::unknown_param_error(&advertised, &invented)
        .expect("a defective call to an unknown tool is rejected too");
    assert!(
        invented_verdict.starts_with("invalid arguments for `frobnicate`:"),
        "{invented_verdict}"
    );
    assert_eq!(
        crate::harness::tool_specs::unknown_param_error(
            &advertised,
            &ToolCall {
                id: "t4".to_string(),
                name: "frobnicate".to_string(),
                arguments: json!({ "x": 1 }),
                defect: None,
            },
        ),
        None,
        "and a well-formed unknown tool is `unknown tool:`'s, not this arm's"
    );
}

// ── ADR 0102 / issue #183 F2 — the COMPOSED advertised set ───────────────────
//
// Everything above this line reasons about `tool_specs()`, the ENGINE's own table —
// 26 names through F2, 19 since F3 deleted the 7 board verbs from it. Everything
// below reasons about the set the model is ACTUALLY shown — `advertised_specs()`,
// which F2 made
// `tool_specs() ++ (registry entries the table does not carry)`. Since F3 the two
// are NEVER the same set in production: the composition is where the model's other
// 7 names come from, which is precisely why this half of the file exists.
//
// Why these tests are here and not beside `every_advertised_tool_reaches_an_executor`:
// that census iterates `tool_specs()`, so a host tool is INVISIBLE to it. That is
// correct for the engine half and it is also a gap — a host tool that is advertised
// and unreachable (the exact failure the census exists for) passes it green. These
// tests close the gap by iterating the composed set.

/// The composition F2 performs at the one production call site
/// (`worker/core.rs::default_build_loop`), spelled here against a loop.
///
/// `detached` picks the two `HostTool` shapes and BOTH are needed by the tests
/// below, for opposite reasons:
///   * `false` (attached, the production shape) parks on the frame — so a test
///     using it must cancel the turn, which is also what proves the wired executor
///     is the one that answered (a miss answers `unknown tool`, a detached entry
///     answers `host-owned`, the wired one answers `cancelled`).
///   * `true` (detached, what `host_entries` builds) answers LOUDLY and instantly,
///     which is what lets a census walk a host name without hanging.
fn compose(
    loop_: &mut AgentLoop,
    names: &[&str],
    detached: bool,
) -> Option<mpsc::UnboundedReceiver<crate::worker::protocol::Outbound>> {
    let specs: Vec<crate::harness::provider::ToolSpec> = names
        .iter()
        .map(|name| crate::harness::provider::ToolSpec {
            name: (*name).to_string(),
            description: format!("a host-owned tool ({name})"),
            parameters: json!({"type": "object", "properties": {}}),
        })
        .collect();
    let mut registry = crate::tools::exec::builtins();
    let mut parked_on = None;
    if detached {
        registry.extend(crate::tools::exec::host_entries(specs));
    } else {
        // The outbound RECEIVER is RETURNED and the caller must hold it: an
        // `mpsc::UnboundedSender` whose receiver is dropped FAILS the send, and the
        // wired `HostTool` correctly reports a closed pipe instead of parking. Holding
        // it is what makes the wired fixture park, which is the production shape.
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        let pending: crate::worker::host_tools::PendingHostToolCalls =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        registry.extend(crate::worker::host_tools::host_entries_attached(
            specs,
            crate::worker::host_tools::HostToolChannel::new(Some("s1".into()), out_tx, pending),
        ));
        parked_on = Some(out_rx);
    }
    loop_.set_tool_registry(registry);
    parked_on
}

/// THE CENSUS OVER THE COMPOSED ADVERTISED SET: every name the model is shown
/// reaches an executor, and the set the model is shown is the engine's OWN names
/// (`tool_specs()`, 17 of them for this child fixture) PLUS exactly the host names —
/// no more, no fewer, no duplicates.
///
/// # Why `every_advertised_tool_reaches_an_executor` cannot see this
///
/// It iterates `tool_specs()`. A host tool is not in that table (and must never be
/// — `tool_specs_advertises_the_nineteen_engine_owned_tools` pins the count and
/// `the_composed_advertised_set_is_exactly_the_twenty_six_names` pins the split), so
/// a host tool advertised and UNREACHABLE — the one failure that census exists to
/// catch — walks through it green. This is the same census with the composed set as
/// its subject, and since F3 it is the ONLY in-crate witness that the 7 board verbs
/// still reach an executor at all: their `exec_*` arms are gone, and what answers
/// them now is whatever the desktop hands back over `host_tools`.
///
/// # Why the dispatch half drives a real `dispatch_tool`
///
/// The set half is static and cheap; only dispatching can lie about nothing. A host
/// entry that is in the advertised set but not in the registry (a composition that
/// appended to one and not the other) is caught only by dispatching the name.
#[tokio::test]
async fn the_composed_advertised_set_reaches_its_executors() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    compose(&mut loop_, &["mark_ready", "set_space_icon"], true);

    // ── the set half ─────────────────────────────────────────────────────────
    // NOTE the fixture loop is a CHILD (`build_loop` gives it `subagent: None`), so
    // the engine half of its advertised set is `tool_specs()` minus 2 — `subagent`
    // and `list_agents` are dropped for a child (the rule
    // `the_child_tool_set_is_pinned` pins). Since F3 `tool_specs()` is the 19
    // ENGINE-owned names, so the engine half here is 17 and the arithmetic below is
    // 17 + 2. It is stated as `tool_specs()` minus the two rather than as a bare
    // number so the test says WHY, and the number it pins is what makes the F3 move
    // visible from this side: before F3 it was 26 − 2 = 24, and the 26 included the
    // 7 board verbs that are now the host's half of the composition.
    let advertised: Vec<String> = loop_
        .advertised_specs()
        .into_iter()
        .map(|s| s.name)
        .collect();
    let mut expected: Vec<String> = tool_specs()
        .into_iter()
        .map(|s| s.name)
        .filter(|name| name != "subagent" && name != "list_agents")
        .collect();
    let engine_len = expected.len();
    assert_eq!(
        engine_len, 17,
        "the fixture loop is a CHILD, so the engine half is `tool_specs()`'s 19 − 2 = \
         17 (if this is not 17, either the fixture stopped being a child — in which \
         case the arithmetic below is lying — or `tool_specs()` moved away from the \
         19 engine-owned names F3 set it to, which is the ENGINE half of the split \
         `tool_specs.rs`'s composed census reports)"
    );
    expected.extend(["mark_ready".to_string(), "set_space_icon".to_string()]);
    assert_eq!(
        advertised.len(),
        expected.len(),
        "the composed advertised set has {} names, not the child engine half (17) + \
         the 2 host names (19): {advertised:?}",
        advertised.len()
    );
    for name in &expected {
        assert!(
            advertised.contains(name),
            "`{name}` is not advertised any more — the composition must ADD host names, \
             never displace an engine name"
        );
    }
    for name in &advertised {
        assert!(
            expected.contains(name),
            "`{name}` is advertised and in NEITHER half of the composition — a name the \
             engine never named and no host asked for"
        );
    }
    // NO DUPLICATES. A host spec reusing an engine name would otherwise appear twice,
    // and a provider rejects a request with two tools of one name.
    let unique: std::collections::BTreeSet<&str> = advertised.iter().map(String::as_str).collect();
    assert_eq!(
        unique.len(),
        advertised.len(),
        "the composed advertised set repeats a name: {advertised:?}"
    );

    // ── the dispatch half ────────────────────────────────────────────────────
    let mut host_checked = 0usize;
    for name in ["mark_ready", "set_space_icon"] {
        let r = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "t1".to_string(),
                    name: name.to_string(),
                    arguments: json!({}),
                    defect: None,
                },
                &CancellationToken::new(),
            )
            .await
            .0;
        let text = result_text(&r);
        assert!(
            !text.contains("unknown tool"),
            "`{name}` is ADVERTISED (it is in `advertised_specs()` above) but \
             `dispatch_tool` answered {text:?} — advertised and unreachable, the exact \
             failure `every_advertised_tool_reaches_an_executor` hunts and CANNOT see, \
             because that census iterates `tool_specs()` and a host tool is not in it."
        );
        // And it reached a HOST executor, not the registry's miss sentence and not a
        // loop arm: the loud refusal is `HostTool`'s own.
        assert!(
            text.contains("host-owned"),
            "`{name}` answered {text:?} — expected the `HostTool` refusal (a detached \
             host entry), which is the sentence proving the name reached THAT executor"
        );
        host_checked += 1;
    }
    assert_eq!(host_checked, 2, "the census walked both host names");

    // ── the disjointness half, on the COMPOSED registry ──────────────────────
    // A host spec that names a loop-internal tool (`ask`, `codemode`, …) would be
    // SHADOWED by the registry lookup in `dispatch_tool`, which runs BEFORE the
    // `match` — silently deleting the interactive or recursive half of that tool.
    //
    // (PR #39 review, Finding 1) That shadow is now refused at compose time:
    // `host_entries` drops a spec whose name `tool_specs()` carries. What this
    // assertion checks is therefore the CONSEQUENCE — the registry carries no
    // engine-owned name — and it stays because it is the cheap, whole-set version of
    // that claim. The tests that REDDENED on the hole itself are
    // `worker::host_tools::tests::a_host_spec_named_ask_never_becomes_an_entry` and
    // friends (composition level) and
    // `harness::r#loop::tests::tools_enabled::a_host_spec_named_ask_does_not_capture_the_loop_arm`
    // (dispatch level, driving a real `AgentLoop`).
    const LOOP_INTERNAL: &[&str] = &[
        "ask",
        "sudo_exec",
        "manage_todo_list",
        "subagent",
        "dispatch_subagent",
        "list_agents",
        "mcp",
        "codemode",
    ];
    let registry: Vec<&str> = loop_.tool_registry().iter().map(|e| e.name()).collect();
    let shadowed: Vec<&str> = registry
        .iter()
        .filter(|name| LOOP_INTERNAL.contains(name))
        .copied()
        .collect();
    assert!(
        shadowed.is_empty(),
        "{shadowed:?} is BOTH a registry entry and a loop-internal arm — the registry \
         lookup in `dispatch_tool` shadows the loop arm",
    );
    // The composition's own arithmetic: 12 built-ins + 2 host entries.
    assert_eq!(
        registry.len(),
        14,
        "the composed registry is `builtins()` (12 since F3 deleted the 7 board verbs \
         from it) ++ the host entries (2), got {} entries: {registry:?}",
        registry.len()
    );
    // And every ENGINE name is still there. This assertion was F2's — "F2 is additive
    // and `builtins()` stays whole" — and F3 is the commit that legitimately moved 7
    // names out of `builtins()`, so a test that asserted the pre-F3 whole would be
    // arguing against the design it now guards. Re-anchored to the fact that still
    // holds: the composition ADDS host entries and DISPLACES no engine name, so the
    // 12 the engine still owns are all present. The 7 that left are not forgotten
    // here — they are censused where they now live, in
    // `the_composed_advertised_set_is_exactly_the_twenty_six_names` (`tool_specs.rs`),
    // which asserts the host half arrives and names it as the HOST half when it does not.
    let engine: Vec<String> = crate::tools::exec::builtins()
        .iter()
        .map(|e| e.name().to_string())
        .collect();
    assert_eq!(
        engine.len(),
        12,
        "`builtins()` is no longer the 12 engine-owned names, so the loop below is \
         walking a set that has already moved"
    );
    for name in &engine {
        assert!(
            registry.contains(&name.as_str()),
            "`{name}` left the registry — the composition must ADD host entries and \
             never DISPLACE an engine name (F3 moved the 7 board verbs out of \
             `builtins()` itself; nothing after it may drop an engine name on the way \
             through the composition)"
        );
    }
    // And the two host entries are the ones the fixture asked for — so the loop
    // below cannot be passing on the engine half alone.
    for name in ["mark_ready", "set_space_icon"] {
        assert!(
            registry.contains(&name),
            "the host entry `{name}` is missing from the composed registry"
        );
    }
}

/// THE SCRIPT-REACH PIN for the composed set: a host tool is callable AND
/// discoverable from a codemode script, because `script_access` reads the ENTRY
/// first and `host_entries` marks every entry `Allowed`.
///
/// Mutation note: give the host entries `ScriptAccess::Excluded` and this reddens —
/// and every OTHER test stays green, which is precisely why the field exists. The
/// loss is ADR 0092's motivating case: a `roadmap` run turning an approved backlog
/// into N Tasks is one script with a loop, and it was built as `execute_tool` arms
/// for exactly that reason.
#[test]
fn a_host_tool_is_script_reachable_through_the_composed_registry() {
    let (out_tx, _out_rx) = mpsc::unbounded_channel();
    let pending: crate::worker::host_tools::PendingHostToolCalls =
        Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    // THE NAME MUST NOT BE A BUILT-IN. The first draft of this test used
    // `create_task`, and it PASSED with `host_entries` mutated to hand out
    // `ScriptAccess::Excluded` — because `builtins()` still carries `create_task`
    // and `script_access` finds the FIRST entry of that name, i.e. the built-in's
    // `Allowed`. A pin that passes on the broken thing is worse than no pin, so the
    // fixture uses a name only the host owns. (`script_access`'s first-wins is also
    // what makes the shadowing direction in `tool_specs`/`advertised_tool_specs`
    // consistent: an engine-owned name always answers through the engine's entry.)
    let composed = {
        let mut registry = crate::tools::exec::builtins();
        registry.extend(crate::worker::host_tools::host_entries_attached(
            vec![crate::harness::provider::ToolSpec {
                name: "mark_ready".to_string(),
                description: "a host verb no built-in owns".to_string(),
                parameters: json!({"type": "object", "properties": {}}),
            }],
            crate::worker::host_tools::HostToolChannel::new(None, out_tx, pending),
        ));
        registry
    };
    assert!(
        crate::tools::exec::builtins()
            .iter()
            .all(|e| e.name() != "mark_ready"),
        "the fixture's premise: `mark_ready` is ONLY a host entry, so the verdict \
         below can only come from the host entry's own `ScriptAccess`"
    );

    // The host name is `Allowed`…
    assert!(
        crate::tools::exec::script_access(&composed, "mark_ready").is_callable(),
        "a host entry must be script-callable — `Excluded` here would advertise it to \
         a script and refuse it, silently deleting ADR 0092's `roadmap` case (the \
         mutation this test exists for: every OTHER test in the crate stays green)"
    );
    // …and so is a host name the engine has NEVER heard of, via the `Allowed`
    // fall-through (the second half of the same preservation rule).
    assert!(
        crate::tools::exec::script_access(&composed, "a_verb_nobody_has_named").is_callable(),
        "an unknown name falls through to `Allowed`, which is what keeps a NEW host \
         verb script-reachable the moment its Supervisor ships it"
    );
    // CONTRAST (so the two assertions above are not a vacuous "everything is
    // callable"): a loop-internal name is still excluded through the same function.
    assert!(
        !crate::tools::exec::script_access(&composed, "ask").is_callable(),
        "`ask` is still excluded — the fall-through is not a blanket `Allowed`"
    );
}

/// `enabled_tools` filters the COMPOSED set, so a host tool the session did not
/// name is NOT advertised — and a host tool it DID name is.
///
/// Why this is a decision and not an accident (grounding H2): the wire field IS the
/// harness allowlist and `core.rs` applies it VERBATIM with no mapping. Appending
/// host entries AFTER the filter would advertise a tool the user switched off and
/// then answer `tool not enabled` for the whole session — advertised-and-dead, the
/// class the census above hunts. Filtering the composed set is the ordering
/// consistent with every other tool, and `Some(vec![])` still means NO tools.
#[test]
fn enabled_tools_filters_the_composed_advertised_set() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    compose(&mut loop_, &["mark_ready"], true);

    // A host tool the session named IS advertised.
    loop_.set_enabled_tools(Some(vec!["read".to_string(), "mark_ready".to_string()]));
    let advertised: Vec<String> = loop_
        .advertised_specs()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(
        advertised,
        vec!["read".to_string(), "mark_ready".to_string()],
        "the allowlist applies to host names and engine names alike, in one pass"
    );

    // A host tool the session did NOT name is NOT advertised (and neither is `bash`).
    loop_.set_enabled_tools(Some(vec!["read".to_string()]));
    let advertised: Vec<String> = loop_
        .advertised_specs()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(
        advertised,
        vec!["read".to_string()],
        "a host tool the session switched off is NOT advertised — the alternative \
         ordering would advertise it and then answer `tool not enabled` forever"
    );

    // `Some(vec![])` is still NO tools, host entries included.
    loop_.set_enabled_tools(Some(Vec::new()));
    assert!(
        loop_.advertised_specs().is_empty(),
        "`Some(vec![])` = NO tools, and a host entry must not re-expand it (the \
         recursion guard's shape, applied to host tools)"
    );

    // `None` = all: the engine half + the host's 1. The engine half is 24 here
    // because the fixture loop is a CHILD (see
    // `the_composed_advertised_set_reaches_its_executors` for that note), so the
    // PARENT case is asserted through the free function with `is_child: false`.
    loop_.set_enabled_tools(None);
    assert_eq!(
        loop_.advertised_specs().len(),
        18,
        "with no allowlist the child's composed set is the 17 engine names \
         (`tool_specs()`'s 19 minus the two the child filter drops) + the 1 host name"
    );
    // THE PARENT CASE, through the same filter with `is_child: false` — the shape a
    // main session composes: the FULL 19 engine names + the host name.
    let parent_registry = {
        let mut registry = crate::tools::exec::builtins();
        registry.extend(crate::tools::exec::host_entries(vec![
            crate::harness::provider::ToolSpec {
                name: "mark_ready".to_string(),
                description: "a host verb".to_string(),
                parameters: json!({"type": "object", "properties": {}}),
            },
        ]));
        registry
    };
    let parent = AgentLoop::advertised_tool_specs(&None, false, &parent_registry);
    assert_eq!(
        parent.len(),
        20,
        "a PARENT's composed set is `tool_specs()`'s 19 + the 1 host name — the \
         number THIS fixture's session is shown. The desktop's real session is shown \
         26, because it hands 7 board verbs in `hostTools` rather than the 1 here; the \
         proof this line carries is that the composition ADDS to the surface rather \
         than reshaping it, which is the same proof at 1 host name as at 7."
    );
    assert!(
        parent.iter().any(|s| s.name == "subagent"),
        "and a parent keeps its dispatch tools (the child drop is name-based, so it \
         says nothing about host tools — a host verb is inherited by a child as an \
         ordinary tool)"
    );
    let child = AgentLoop::advertised_tool_specs(&None, true, &parent_registry);
    assert!(
        child.iter().any(|s| s.name == "mark_ready") && !child.iter().any(|s| s.name == "subagent"),
        "a CHILD keeps the host verb and loses the dispatch tools — the inheritance \
         decision `subagent_flow.rs` states explicitly"
    );
}

/// A host tool that is advertised and DISABLED is refused by the enablement gate,
/// exactly as an engine tool is — i.e. a host entry goes through `dispatch_tool`'s
/// existing gates and does not bypass them.
///
/// The pair matters on its own: it is the sentence that proves the composition did
/// not open a route AROUND the gate, which is the invariant ADR 0081 / issue #92
/// exist to protect ("the executor cannot re-decide differently from its gate").
#[tokio::test]
async fn a_disabled_host_tool_is_refused_before_its_executor_runs() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    compose(&mut loop_, &["mark_ready"], true);
    loop_.set_enabled_tools(Some(vec!["read".to_string()]));

    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "mark_ready".to_string(),
                arguments: json!({}),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert_eq!(
        result_text(&r),
        "tool not enabled: mark_ready",
        "the SAME sentence a disabled engine tool gets — not `unknown tool`, and not \
         the host executor's own answer (which would mean the gate was bypassed)"
    );
}

/// THE WIRED-COMPOSITION PIN: the production composition (attached host entries,
/// the shape `default_build_loop` builds) is what answers a host call, and the
/// turn token settles the park.
///
/// The three sentences tell the three outcomes apart and that is the whole point:
/// `unknown tool` = the name never reached a registry entry; `host-owned` = it
/// reached a DETACHED entry (the wrong composition); `cancelled` = it reached the
/// WIRED executor and the park raced the turn token, which is the thing that keeps
/// a Stop from hanging. An already-cancelled token makes this deterministic — the
/// park's select is `biased` and the answer arm can never be Ready here.
#[tokio::test]
async fn the_wired_host_entry_reaches_the_parked_executor_and_the_turn_token_settles_it() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    // HELD for the whole test — see `compose`'s note (a dropped receiver makes the
    // send fail, which is a DIFFERENT correct answer and not the one under test).
    let _out_rx = compose(&mut loop_, &["mark_ready"], false);

    let turn = CancellationToken::new();
    turn.cancel();
    let r = tokio::time::timeout(Duration::from_secs(5), {
        async {
            loop_
                .dispatch_tool(
                    &ToolCall {
                        id: "t1".to_string(),
                        name: "mark_ready".to_string(),
                        arguments: json!({ "note": "hi" }),
                        defect: None,
                    },
                    &turn,
                )
                .await
                .0
        }
    })
    .await
    .expect(
        "a host call on a cancelled turn returns PROMPTLY — if this hangs, the wired \
         executor is not racing the turn token and every host tool hangs on a Stop",
    );
    let text = result_text(&r);
    assert_eq!(
        text, "cancelled",
        "`mark_ready` answered {text:?}. `unknown tool` would mean the composition \
         never reached `dispatch_tool`'s lookup; `host-owned` would mean the loop was \
         given DETACHED entries (F1's shape) instead of the wired ones production \
         builds. `cancelled` is the wired executor answering on the TURN token."
    );
}

/// THE GATE-BEHAVIOUR PIN (ADR 0102 §3, the pin the spec's §3 table did not name):
/// a host tool is gated EXACTLY as the seven board verbs are today, because
/// `ToolKey` recognises neither and `policy_verdict` polices neither.
///
/// # What is being proved, and what deliberately is not
///
/// `ToolKey::from_frontmatter_key` hard-codes eleven names; every other name — the
/// seven verbs included — is retained verbatim as `ToolKey::Unrecognized`, and
/// `gate.rs::tool_key_matches` matches an `Unrecognized` by exact name. So a
/// `permission:` block CAN speak about a host tool (the ruleset stays representable,
/// which is the point of the variant), and it speaks about it the same way it speaks
/// about `list_board`. `ToolKey` is deliberately NOT widened this run — widening it
/// would change per-definition permission matching, which is out of scope.
///
/// The gate half is the honest one and it records a NON-effect: `policy_verdict`
/// returns `None` for any name outside `POLICIED_TOOLS`, so a whole-tool `deny` on a
/// board verb does nothing at that layer TODAY, and a host tool inherits exactly that
/// (non-)behaviour. The contrast pair below is what makes the assertion a pin rather
/// than a tautology: the SAME block on `bash` — a policed name — DOES produce a
/// verdict.
#[test]
fn a_host_tool_is_gated_exactly_as_the_board_verbs_are() {
    use crate::definition_permission::{Action, PermissionBlock, Rules, ToolKey};

    // ── the ToolKey half: identical mapping, name retained verbatim ──────────
    let host_key = ToolKey::from_frontmatter_key("mark_ready");
    let board_key = ToolKey::from_frontmatter_key("list_board");
    assert_eq!(
        host_key,
        ToolKey::Unrecognized("mark_ready".to_string()),
        "a host tool lands in `Unrecognized` (the variant that keeps the name \
         verbatim so the ruleset stays representable) — `ToolKey` is NOT widened this \
         run, on purpose"
    );
    assert_eq!(
        board_key,
        ToolKey::Unrecognized("list_board".to_string()),
        "and so does a board verb, today, unchanged"
    );
    // The structural claim: the two are the SAME VARIANT of the same shape. A widened
    // `ToolKey` (a new variant naming either one) reddens this and is the signal that
    // the "identical gate behaviour" premise needs re-checking.
    assert_eq!(
        std::mem::discriminant(&host_key),
        std::mem::discriminant(&board_key),
        "a host tool and a board verb must reach the gate through the SAME variant"
    );
    // CONTRAST: a recognised name is NOT the same variant (so the two assertions
    // above are not "everything is Unrecognized").
    assert_ne!(
        std::mem::discriminant(&host_key),
        std::mem::discriminant(&ToolKey::from_frontmatter_key("bash")),
        "`bash` IS a recognised key — the fall-through is a real fall-through"
    );

    // ── the gate half: identical verdicts, both `None` ───────────────────────
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    compose(&mut loop_, &["mark_ready"], true);
    // A block that DENIES all three: the host tool, a board verb, and `bash`.
    loop_.set_permission(Some(PermissionBlock {
        tools: vec![
            (
                ToolKey::Unrecognized("mark_ready".to_string()),
                Rules::whole_tool(Action::Deny),
            ),
            (
                ToolKey::Unrecognized("list_board".to_string()),
                Rules::whole_tool(Action::Deny),
            ),
            (ToolKey::Bash, Rules::whole_tool(Action::Deny)),
        ],
    }));

    let verdict = |name: &str| {
        loop_.policy_verdict(&ToolCall {
            id: "t1".to_string(),
            name: name.to_string(),
            arguments: json!({}),
            defect: None,
        })
    };
    assert_eq!(
        verdict("mark_ready"),
        verdict("list_board"),
        "a host tool and a board verb get the SAME gate verdict — that is the whole \
         claim, and it is what makes `ToolKey` not needing to be widened this run a \
         fact rather than a hope"
    );
    assert_eq!(
        verdict("mark_ready"),
        None,
        "and both are `None`: `policy_verdict` starts at `policed_direction(name)?`, \
         which knows only the seven path/shell tools. So a `permission:` block cannot \
         narrow a host tool any more than it can narrow a board verb TODAY — the host \
         tool inherits that, unchanged. (The `enabled_tools` filter IS the narrowing \
         that works, and the census above pins it.)"
    );
    // CONTRAST: the same block on a POLICED name does produce a verdict, so the two
    // `None`s above are a real absence of policing and not a fixture that never
    // reached the gate.
    assert_eq!(
        verdict("bash"),
        Some(crate::policy::Decision::Deny),
        "`bash` is policed and the SAME block denies it — the block is live, it just \
         does not speak to un-policed names"
    );
}

// ── PR #39 review, Finding 1 — a host spec may not take a name the engine owns ───
//
// The composition is `builtins() ++ host_entries_attached(env.host_tools, …)`
// (`worker/core.rs::default_build_loop`) and `lookup` is a plain first-match `find`.
// `advertised_tool_specs` dedupes ENGINE-FIRST, so a host spec named `ask` was never
// shown to the model twice — the advertised surface was safe. The REGISTRY was not:
// `dispatch_tool`'s `lookup` runs BEFORE its `match`, so a host entry named `ask`
// answered the loop's own name and the arm — the `&mut AgentLoop` half that makes
// `ask` interactive, `codemode` recursive, `mcp` turn-raced — never ran. That is
// exactly the failure F1's census comment names, and ADR 0102 lists "Host entries
// shadow built-ins by name" among its REJECTED options.
//
// The fix is compose-time (`host_entries_attached` drops such a spec and records the
// drop), so the reddening witnesses are in `worker/host_tools.rs`. What these
// loop-level tests pin is the CONSEQUENCE a user would feel: the loop arm still
// answers, and the registry carries one entry per name.

/// The composition with EXPLICIT specs (`compose` takes names and invents the
/// description; a shadow fixture has to name a tool the engine owns).
///
/// `detached` means the same thing it means on `compose`: `true` answers loudly and
/// instantly, which is what lets a test tell "a host executor answered" apart from
/// "the loop arm answered" without a Supervisor on the wire.
fn compose_specs(loop_: &mut AgentLoop, specs: &[(&str, &str)]) {
    let registry = {
        let mut registry = crate::tools::exec::builtins();
        registry.extend(crate::tools::exec::host_entries(
            specs
                .iter()
                .map(|(name, description)| crate::harness::provider::ToolSpec {
                    name: (*name).to_string(),
                    description: (*description).to_string(),
                    parameters: json!({"type": "object", "properties": {}}),
                })
                .collect(),
        ));
        registry
    };
    loop_.set_tool_registry(registry);
}

/// THE HOLE, at the dispatch site: a host spec named `ask` must not capture the loop
/// arm. The two sentences are the discriminator — `host-owned` is a host executor
/// answering (the bug), `User cancelled the question.` is `ask_flow` answering on the
/// cancelled turn token (the loop arm, which is the whole point of the tool).
///
/// The turn token is pre-cancelled for determinism: `ask_flow`'s waiter races the
/// turn token and has NO deadline, so an uncancelled token here would park until the
/// test's own timeout — which is itself a pass, because parking means the ARM ran
/// (a host entry on a detached channel answers instantly).
#[tokio::test]
async fn a_host_spec_named_ask_does_not_capture_the_loop_arm() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    compose_specs(&mut loop_, &[("ask", "a host's own `ask`")]);

    let turn = CancellationToken::new();
    turn.cancel();
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "ask".to_string(),
                arguments: json!({
                    "questions": [{
                        "id": "q1",
                        "question": "Which?",
                        "options": [{ "label": "A" }]
                    }]
                }),
                defect: None,
            },
            &turn,
        )
        .await
        .0;
    let text = result_text(&r);
    assert!(
        !text.contains("host-owned"),
        "`ask` was answered by a HOST executor: {text:?}. `dispatch_tool`'s registry \
         `lookup` runs before its `match`, so a host entry with this name turns the \
         interactive tool into a stub — the model asks a question nobody is asked. \
         The engine owns this name (`tool_specs()` carries it) and `ask` is one of the \
         seven names whose executor is `&mut AgentLoop`."
    );
    assert_eq!(
        text, "User cancelled the question.",
        "`ask` must answer through `ask_flow` — the loop arm — and a cancelled turn \
         settles it with the cancelled shape. Anything else means the arm did not run."
    );
}

/// The same hole on the BUILT-IN side (ADR 0102's own rejected option: a host
/// supplying its own `bash`, moving the Boundary judgement host-side). Two halves,
/// because the two failure shapes differ:
///
/// * the registry half reddens on today's code — the composed vec carried TWO
///   entries named `bash`, and `lookup`'s first-wins is the only thing keeping the
///   engine's answer; a composition that appended the host half first, or a
///   `builtins()` that lost the name, and the host's executor answers;
/// * the dispatch half pins the consequence the model sees: the engine's own `bash`
///   answers (`bash: missing \`command\` parameter` is `exec_bash`'s sentence), never
///   the host's.
#[tokio::test]
async fn a_host_spec_named_bash_does_not_replace_the_engine_executor() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    compose_specs(&mut loop_, &[("bash", "a host's own `bash`")]);

    // The registry half: ONE entry per name. A duplicate is the shadow, whatever
    // order the vec happens to be in.
    let names: Vec<&str> = loop_.tool_registry().iter().map(|e| e.name()).collect();
    let bash_count = names.iter().filter(|n| **n == "bash").count();
    assert_eq!(
        bash_count, 1,
        "the composed registry carries {bash_count} entries named `bash` — a host \
         spec took the engine's name. `lookup` is a first-match `find`, so the answer \
         for `bash` depends on the ORDER of a `Vec` two different modules write, and \
         ADR 0102 rejects the option outright (`{names:?}`)."
    );

    // The dispatch half: the engine's executor answers.
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({}),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    assert_eq!(
        result_text(&r),
        "bash: missing `command` parameter",
        "`bash` must answer through the engine's own executor (this sentence is \
         `exec_bash`'s). A `host-owned` answer means the Boundary judgement moved to \
         the host side for the one tool family whose whole safety story is the frozen \
         `ToolCtx`."
    );
}

/// The non-regression half: the fix must not break the REAL desktop payload. The 7
/// board verbs the desktop sends in `hostTools` still compose, still get advertised
/// on top of the engine's 17 child-half names, and still reach a host executor.
#[tokio::test]
async fn the_seven_board_verbs_still_compose_after_the_refusal() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        mpsc::unbounded_channel().0,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let board: Vec<(&str, &str)> = [
        "list_board",
        "get_task",
        "create_task",
        "edit_task",
        "move_task",
        "get_artifact",
        "write_artifact",
    ]
    .iter()
    .map(|name| (*name, "a host-owned board verb"))
    .collect();
    compose_specs(&mut loop_, &board);

    let registry: Vec<&str> = loop_.tool_registry().iter().map(|e| e.name()).collect();
    for name in board.iter().map(|(n, _)| *n) {
        assert!(
            registry.contains(&name),
            "`{name}` — one of the 7 verbs the desktop actually sends — is not in the \
             composed registry: {registry:?}. The engine-name refusal must be \
             name-targeted; a session that lost its board verbs cannot do the work a \
             `roadmap` dispatch was sent to do."
        );
    }
    // 12 built-ins + 7 host verbs. The refusal adds nothing and removes nothing here.
    assert_eq!(
        registry.len(),
        19,
        "the composed registry is `builtins()` (12) ++ the 7 board verbs, got {}: \
         {registry:?}",
        registry.len()
    );

    // And they are advertised: the child fixture's engine half (17) + the 7.
    let advertised: Vec<String> = loop_
        .advertised_specs()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(
        advertised.len(),
        24,
        "the model's surface with the real desktop payload is 17 + 7 = 24 names: \
         {advertised:?}"
    );

    // And one reaches its host executor rather than the miss sentence.
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "list_board".to_string(),
                arguments: json!({}),
                defect: None,
            },
            &CancellationToken::new(),
        )
        .await
        .0;
    let text = result_text(&r);
    assert!(
        text.contains("host-owned") && !text.contains("unknown tool"),
        "`list_board` answered {text:?} — a board verb must reach its host entry."
    );
}

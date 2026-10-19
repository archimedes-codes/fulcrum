use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::agent::events::RpcEvent;
use crate::agent::harness::provider::{FinishReason, ProviderEvent, ToolCall};
use crate::agent::harness::retry::RetryPolicy;

/// (finding 13b) `enabled_tools` is wired into `dispatch_tool`:
/// a disabled tool (`bash`, with only `read` enabled) is a
/// tool-result error, NOT executed; an enabled tool (`read`) is
/// still dispatched. A `None` `enabled_tools` (the default) leaves
/// every tool dispatchable (the current behavior).
#[tokio::test]
async fn a_disabled_tool_is_a_tool_result_error_not_execution() {
    let (provider, _calls) = ScriptedProvider::new(vec![Some(Vec::new())]);
    let mut loop_ = build_loop(
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
            },
            &CancellationToken::new(),
        )
        .await;
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
            },
            &CancellationToken::new(),
        )
        .await;
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
            },
            &CancellationToken::new(),
        )
        .await;
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
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let mut loop_ = build_loop(
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
    let mut loop_ = build_loop(
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
            },
            &CancellationToken::new(),
        )
        .await;
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
            },
            &CancellationToken::new(),
        )
        .await;
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
    let advertised = AgentLoop::advertised_tool_specs(&None, false);
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
/// child still HAS them. (The one child shape with no reduced message at
/// all — no launch/definition prompt AND no `manage_todo_list` in the
/// resolved child tools, so `build_child_system_message` returns `None` —
/// falls through to the MAIN-prompt arm in `worker/core.rs`, whose section
/// is FILTERED the same way; ADR 0036's subagent bullet owns that shape.)
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
    let child = AgentLoop::advertised_tool_specs(&None, true);
    assert!(
        !child.iter().any(|s| s.name == "subagent"),
        "a child does not advertise `subagent`"
    );
    assert!(
        !child.iter().any(|s| s.name == "list_agents"),
        "a child does not advertise `list_agents`"
    );
    // CONTRAST: the parent advertises BOTH.
    let parent = AgentLoop::advertised_tool_specs(&None, false);
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
    // fall-through child (no prompt AND no `manage_todo_list`) does get a
    // section — the main prompt's, FILTERED the same way — so the flag holds
    // for that shape too; ADR 0036's subagent bullet states it, and what the
    // assertions here pin is the DEFAULT shape's tool set.
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
    let loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let empty_home =
        std::env::temp_dir().join(format!("harness-list-agents-home-{}", uuid::Uuid::new_v4()));
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
    let loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let empty_home =
        std::env::temp_dir().join(format!("harness-list-agents-home-{}", uuid::Uuid::new_v4()));
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
    let loop_ = build_loop(
        Box::new(provider),
        events_tx,
        turn_cancel,
        settle_tx,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let empty_home =
        std::env::temp_dir().join(format!("harness-list-agents-home-{}", uuid::Uuid::new_v4()));
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

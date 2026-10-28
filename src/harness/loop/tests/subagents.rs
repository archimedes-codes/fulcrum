use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::support::*;
use crate::agent::events::{EventSink, RpcEvent};
use crate::agent::harness::catalog::{CompactionConfig, Model, ModelCatalog};
use crate::agent::harness::dispatch::{InProcessDispatcher, SubagentDispatcher};
use crate::agent::harness::provider::{
    ChatRole, FinishReason, MessageContent, ProviderEvent, ToolCall,
};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::subagent::{
    AgentNameResolution, LaunchConfig, SubagentOutcome, SubagentSessionManager,
};
use crate::agent::tools::ContentBlock;
use crate::agent::worker::client::{WorkerError, WorkerHandle, WorkerInboundEvent};
use crate::agent::worker::manager::{WorkerFactory, WorkerManager};
use crate::agent::worker::protocol::{StartEnv, StartMode};

/// The `fake_worker` fixture factory (the `WorkerFactory` seam — the
/// `CARGO_MANIFEST_DIR`/`target/debug` convention; `cargo test` builds
/// the bin targets).
struct FakeWorkerFactory;

impl WorkerFactory for FakeWorkerFactory {
    fn spawn(&self) -> Result<WorkerHandle, WorkerError> {
        WorkerHandle::spawn(&std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_worker"
        )))
    }
}

/// A `WorkerManager` with the loop's parent session (`"s1"` — the
/// `AgentLoop`'s `session_id`) `attach`ed (the ADR 0025 Task 5
/// re-plumb — the subagent runs in a `fake_worker` process): the
/// parent's `StartEnv` carries the given `models` catalog (the
/// `dispatch_subagent` flow resolves the child's `model` against it)
/// and the given `config_dir` (the settings' `subagentModels`
/// override is resolved loop-side; the `config_dir` is the
/// parent's). The `sink_for` lookup returns the given sink for the
/// parent (the `subagent-session-started` / `subagent-closed` UI
/// lifecycle events are delivered on the PARENT's sink).
async fn make_worker_manager(
    dead_bound: Duration,
    models: Vec<Model>,
    config_dir: Option<&std::path::Path>,
    sink: Arc<dyn EventSink>,
) -> Arc<WorkerManager> {
    let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
    // The `on_event` closure mirrors the router's `SinkFrame` re-emit
    // (the child's `SinkFrame`s — the `session_info` Start echo with
    // the `systemPrompt` / `trusted` envelope fields — land on the
    // test's sink; the `RpcEvent` stream is a no-op here).
    let on_event_sink = sink.clone();
    let wm = WorkerManager::new(
        factory,
        Arc::new(|_id: String, _code: Option<i32>, _pid: Option<u32>| {}),
        Arc::new(
            move |_id: String, is_subagent: bool, evt: WorkerInboundEvent| {
                if is_subagent {
                    if let WorkerInboundEvent::SinkFrame { event, payload } = &evt {
                        on_event_sink.emit(event, payload.clone());
                    }
                }
            },
        ),
        Arc::new(move |id: &str| (id == "s1").then(|| sink.clone())),
    )
    .with_dead_bound(dead_bound);
    let wm = Arc::new(wm);
    let env = StartEnv::from_parts(
        "s1".to_string(),
        "/tmp/space".to_string(),
        StartMode::Fresh,
        None,
        models[0].clone(),
        ModelCatalog {
            models,
            default_model: None,
            compaction: CompactionConfig::default(),
        },
        None,
        true,
        None,
        config_dir
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "/tmp".to_string()),
        true,
        None,
    );
    wm.attach("s1", &env)
        .await
        .expect("the parent attach completes");
    wm
}

/// A `SubagentSessionManager` on a `WorkerManager` (the ADR 0025
/// Task 5 re-plumb — the `InProcessDispatcher`'s manager): the parent
/// `"s1"` is `attach`ed with the given `models` catalog (the
/// `dispatch_subagent` flow resolves the child's `model` against it).
async fn make_subagent_manager(
    dead_bound: Duration,
    models: Vec<Model>,
    config_dir: Option<&std::path::Path>,
    sink: Arc<dyn EventSink>,
) -> Arc<SubagentSessionManager> {
    let wm = make_worker_manager(dead_bound, models, config_dir, sink).await;
    let manager = Arc::new(SubagentSessionManager::new());
    manager.set_worker_manager(wm);
    manager
}

/// (ADR 0023 thinking-order regression) the `:<level>` suffix of the
/// resolved model key beats the frontmatter's `thinking` (the
/// doc-correct order — explicit > suffix > frontmatter; the OLD code
/// produced the frontmatter's `"low"` here). The ADR 0025 Task 5
/// re-plumb: the subagent runs in a `fake_worker` process (the
/// `WorkerManager`'s `dispatch_subagent` flow) — the
/// `subagent-session-started` frame carries the RESOLVED model +
/// thinking, and the `subagent` tool result is the child's captured
/// final text (the `fake_worker`'s canned `"canned answer"`).
#[tokio::test]
async fn a_native_subagent_with_a_suffix_model_key_uses_the_suffix_thinking() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "recon the auth code", "agentName": "scout" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    // A TWO-model catalog: `fake/m1` (the parent) + `fake/m2` (the
    // frontmatter target — IN the catalog, so it is NOT stale).
    let models = vec![fake_model("m1"), fake_model("m2")];
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
        events: sink_events.clone(),
    });
    let manager =
        make_subagent_manager(Duration::from_secs(30), models.clone(), None, sink.clone()).await;
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
        models,
        None,
    );
    // The Agent definition: `model: fake/m2:high` (IN the catalog —
    // the `:<level>` suffix is stripped for the resolvability check)
    // + `thinking: low` (the LAST rung — the suffix beats it).
    let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2:high\nthinking: low\n---\nYou are a scout.\n",
    )
    .unwrap();
    loop_.handle_prompt(&text_prompt("go")).await;
    // The `subagent` tool result: the child's captured final text
    // (the `fake_worker`'s canned `"canned answer"` — the
    // `SubagentCapture` over the child's `SinkFrame`
    // `agent_message_chunk` stream).
    let end = wait_for_event(
        &mut events_rx,
        SPAWN_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(!is_error, "the child completed");
    assert_eq!(
        result["content"][0]["text"], "canned answer",
        "the tool result is the child's captured final text"
    );
    // The parent turn settled (bounded — the `ScriptedProvider`
    // settles it; the child ran to completion inside the dispatch).
    wait_for_event(&mut events_rx, SPAWN_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the parent turn settled");
    // The `subagent-session-started` frame: the suffix's `high`
    // beats the frontmatter's `low` (the doc-correct order), and
    // the model is the suffix-STRIPPED key.
    let events = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let started = events
        .iter()
        .find(|(name, _)| name == "subagent-session-started")
        .expect("the `subagent-session-started` frame was emitted");
    assert_eq!(
        started.1["model"], "fake/m2",
        "the suffix is stripped for the model resolution"
    );
    assert_eq!(
        started.1["thinkingLevel"], "high",
        "the suffix beats the frontmatter's `thinking` (the order fix)"
    );
}

/// (ADR 0023) end-to-end: a settings `subagentModels` override beats
/// the frontmatter `model` (the child runs the override), and the
/// frontmatter's `thinking` STILL applies (the override only
/// replaces the model). The ADR 0025 Task 5 re-plumb: the subagent
/// runs in a `fake_worker` process — the `subagent-session-started`
/// frame carries the RESOLVED model + thinking + tools, and the
/// `subagent` tool result is the child's captured final text (the
/// `fake_worker`'s canned `"canned answer"`).
#[tokio::test]
async fn a_native_subagent_with_a_settings_override_runs_the_override_model() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "recon the auth code", "agentName": "scout" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    // A THREE-model catalog: `fake/m1` (the parent) + `fake/m2` (the
    // frontmatter target) + `fake/m3` (the override target — IN the
    // catalog, so it is NOT stale).
    let models = vec![fake_model("m1"), fake_model("m2"), fake_model("m3")];
    // #96: a TEST-SCOPE guard, declared BEFORE `make_subagent_manager` and
    // the loop, so it outlives both. Both get `Some(config_dir.path())` — a
    // borrowed path, never the guard. LIMIT, recorded honestly: the value
    // does travel to the child via `StartEnv.config_dir`, but this test's
    // child is `src/bin/fake_worker.rs`, a canned stdin/stdout fixture that
    // performs NO filesystem I/O, so the "read by a real child process"
    // premise is false for it. The dir still must outlive the test because
    // the PARENT-side `AgentLoop` holds the same `config_dir` and re-reads
    // `settings.json` every turn, and the `WorkerManager` owns the child —
    // and this placement provides that.
    let config_dir =
        crate::test_support::ScratchDir::in_env_temp("harness-agent-def-e2e-cfg-", "override");
    std::fs::write(
        config_dir.join("settings.json"),
        r#"{ "subagentModels": { "scout": "fake/m3" } }"#,
    )
    .unwrap();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
        events: sink_events.clone(),
    });
    let manager = make_subagent_manager(
        Duration::from_secs(30),
        models.clone(),
        Some(config_dir.path()),
        sink.clone(),
    )
    .await;
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
        models,
        Some(config_dir.path()),
    );
    // The Agent definition: `model: fake/m2` (IN the catalog — NOT
    // stale) + `thinking: low` (the override replaces ONLY the
    // model — the thinking still comes from the file).
    let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
    )
    .unwrap();
    loop_.handle_prompt(&text_prompt("go")).await;
    // The `subagent` tool result: the child's captured final text
    // (the `fake_worker`'s canned `"canned answer"`).
    let end = wait_for_event(
        &mut events_rx,
        SPAWN_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(!is_error, "the child completed");
    assert_eq!(
        result["content"][0]["text"], "canned answer",
        "the tool result is the child's captured final text"
    );
    // The parent turn settled (bounded — the `ScriptedProvider`
    // settles it; the child ran to completion inside the dispatch).
    wait_for_event(&mut events_rx, SPAWN_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the parent turn settled");
    // The `subagent-session-started` frame: the override beats the
    // frontmatter's model, the frontmatter's `thinking` still
    // applies, and the tools are the frontmatter's allowlist.
    let events = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let started = events
        .iter()
        .find(|(name, _)| name == "subagent-session-started")
        .expect("the `subagent-session-started` frame was emitted");
    assert_eq!(
        started.1["model"], "fake/m3",
        "the settings override beats the frontmatter model"
    );
    assert_eq!(
        started.1["thinkingLevel"], "low",
        "the frontmatter's `thinking` still applies (the override only replaces the model)"
    );
    assert_eq!(
        started.1["enabledTools"],
        json!(["read"]),
        "the child's tools are the frontmatter's allowlist"
    );
}

/// (ADR 0020) End-to-end: a native subagent dispatched with an
/// `agentName` matching a discovered definition runs the FRONTMATTER
/// config (the `dispatch_subagent` → `resolve_launch` layering — the
/// child's `subagent-session-started` payload carries the
/// frontmatter's `model` + `tools`, and the child's `Start` envelope
/// (the `fake_worker`'s `session_info` echo) carries the frontmatter
/// body as the `systemPrompt` + exactly the frontmatter's tools).
/// The ADR 0025 Task 5 re-plumb: the subagent runs in a `fake_worker`
/// process — the `subagent` tool result is the child's captured
/// final text (the `fake_worker`'s canned `"canned answer"`).
///
/// No `env_lock()` here (intentional): `resolve_launch` reads `HOME`
/// (via `discover_agents` → `user_roots`), concurrently with the
/// `HOME`-mutating tests — but the isolation is safe by construction:
/// (1) the space-level roots are scanned FIRST (first-wins dedupe),
/// so a user-level `scout` file can never shadow the space-level
/// `scout` written below; (2) the mutators' scratch homes contain no
/// agent files. If this test ever asserts on a USER-level definition,
/// it must take `env_lock()` (and set `HOME`) first.
#[tokio::test]
async fn a_native_subagent_with_a_matching_agent_name_runs_the_frontmatter_config() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "recon the auth code", "agentName": "scout" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    // A TWO-model catalog: `fake/m1` (the parent) + `fake/m2` (the
    // frontmatter target — IN the catalog, so it is NOT stale).
    let models = vec![fake_model("m1"), fake_model("m2")];
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
        events: sink_events.clone(),
    });
    let manager =
        make_subagent_manager(Duration::from_secs(30), models.clone(), None, sink.clone()).await;
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
        models,
        None,
    );
    // The Agent definition (the loop's `space_cwd` is the Space root
    // — the frontmatter's `model` is IN the catalog, so it is NOT
    // stale).
    let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
    )
    .unwrap();
    loop_.handle_prompt(&text_prompt("go")).await;
    // The parent turn settled (bounded — the `ScriptedProvider`
    // settles it; the child ran to completion inside the dispatch).
    wait_for_event(&mut events_rx, SPAWN_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the parent turn settled");
    // The `subagent-session-started` frame (the `TestSink` cannot
    // see it — the `RecordingSink` records EVERY event): the child's
    // RESOLVED model / tools / thinking carry the frontmatter.
    let events = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let started = events
        .iter()
        .find(|(name, _)| name == "subagent-session-started")
        .expect("the `subagent-session-started` frame was emitted");
    assert_eq!(
        started.1["model"], "fake/m2",
        "the child runs the frontmatter's model (the catalog's `fake/m2`)"
    );
    assert_eq!(
        started.1["enabledTools"],
        json!(["read"]),
        "the child's tools are the frontmatter's allowlist"
    );
    assert_eq!(
        started.1["thinkingLevel"], "low",
        "the child's thinking level is the frontmatter's"
    );
    assert_eq!(
        started.1["nameResolution"], "matched",
        "a discovered definition reports MATCHED end to end"
    );
    // The child's `Start` envelope (the `fake_worker`'s `session_info`
    // echo — re-emitted on the parent's scope by the `on_event`
    // router): the frontmatter body is the `systemPrompt` (the
    // `build_child_system_message` output — the body + the todo
    // instructions), and the `enabledTools` is exactly the
    // frontmatter's.
    let child_info = events
        .iter()
        .find(|(name, payload)| {
            name == "session-update"
                && payload["update"]["sessionUpdate"] == "session_info"
                && payload["update"]["systemPrompt"].is_string()
        })
        .expect("the child's `session_info` Start echo was re-emitted");
    assert!(
        child_info.1["update"]["systemPrompt"]
            .as_str()
            .unwrap()
            .starts_with("You are a scout."),
        "the frontmatter body is the child's system message (the `build_child_system_message` output)"
    );
    assert_eq!(
        child_info.1["update"]["enabledTools"],
        json!(["read"]),
        "the child gets exactly the frontmatter's tool"
    );
}

/// (issue #47) The exact scenario: `subagent({ agentName: "reviewr" })` — a
/// typo in a CORRECTLY-spelled key, so T9's unknown-KEY checker cannot see it.
///
/// Two halves, and the first is the one that matters: the miss NEVER blocks.
/// The `subagent` tool result is still the child's real captured output with
/// `is_error == false`, and the parent turn settles — ADR 0020's deliberate
/// config-less dispatch, unchanged, and the same non-blocking doctrine as T9's
/// warning and ADR 0055's metrics. The second half is what this task adds: the
/// `subagent-session-started` payload says `unmatched` while still carrying the
/// name the model asked for, so the UI can stop presenting a confident label
/// for a child that loaded no persona, no tool allowlist and no model.
///
/// Runs the REAL `resolve_launch` → `dispatch_native` → `subagent_flow` path.
/// NO agent file is written at all, so the name can only miss.
///
/// No `env_lock()` (the neighbour's note applies verbatim): the space-level
/// roots are scanned first and this Space holds no agent file, and the
/// mutators' scratch homes hold none either — and nothing here asserts on a
/// USER-level definition.
#[tokio::test]
async fn a_subagent_named_after_no_definition_still_runs_and_says_so() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "recon the auth code", "agentName": "reviewr" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let models = vec![fake_model("m1")];
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
        events: sink_events.clone(),
    });
    let manager =
        make_subagent_manager(Duration::from_secs(30), models.clone(), None, sink.clone()).await;
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
        models,
        None,
    );
    // NO agent file: `"reviewr"` can only miss.
    loop_.handle_prompt(&text_prompt("go")).await;
    // 1. NON-BLOCKING: the tool result is the child's real output, not an error.
    let end = wait_for_event(
        &mut events_rx,
        SPAWN_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        !is_error,
        "a resolve miss never fails the dispatch — got {result}"
    );
    assert_eq!(
        result["content"][0]["text"], "canned answer",
        "the tool result is the child's captured final text (the child RAN)"
    );
    // 2. The parent turn settled.
    wait_for_event(&mut events_rx, SPAWN_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the parent turn settled");
    // 3. The report: `unmatched`, and the name is still there (additive).
    let events = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let started = events
        .iter()
        .find(|(name, _)| name == "subagent-session-started")
        .expect("the `subagent-session-started` frame was emitted");
    assert_eq!(
        started.1["nameResolution"], "unmatched",
        "a name that matched no definition is visible on the payload"
    );
    assert_eq!(
        started.1["agentName"], "reviewr",
        "the marker never replaces the label — the row keeps the name"
    );
}

/// (issue #47 follow-up) A PADDED `agentName` puts the TRIMMED name on the
/// payload. `resolve_launch` trims before it selects, so `"  scout  "` is a
/// genuine `Matched` — but the label travelled down a separate path, so the
/// wire (and therefore the payload) carried the RAW string while the marker
/// described the trimmed one: the UI printed `"  scout  "` and its row and its
/// badge were about two different names.
///
/// A PAYLOAD-level pin, deliberately not a resolver-level one:
/// `resolve_launch_a_blank_name_is_no_name_and_a_padded_name_is_a_match`
/// already proves the padded name MATCHES, and that test stays green with the
/// label bug in place — nothing below `dispatch_subagent` sees the difference
/// except this emit. Runs the same real path as the two neighbours above
/// (`resolve_launch` → `dispatch_native` → `subagent_flow`).
///
/// No `env_lock()` (both neighbours' note applies verbatim): the space-level
/// roots are scanned first, so a user-level `scout` can never shadow the
/// space-level one written below, and nothing here asserts on a USER-level
/// definition.
#[tokio::test]
async fn a_padded_agent_name_reaches_the_payload_trimmed() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                // The padding is the whole test: a model that emitted the name
                // with surrounding space (a copy out of a prose list) is the
                // realistic producer of this input.
                arguments: json!({ "task": "recon the auth code", "agentName": "  scout  " }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let models = vec![fake_model("m1"), fake_model("m2")];
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let sink: Arc<dyn EventSink> = Arc::new(RecordingSink {
        events: sink_events.clone(),
    });
    let manager =
        make_subagent_manager(Duration::from_secs(30), models.clone(), None, sink.clone()).await;
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        sink,
        Some(Arc::new(InProcessDispatcher::new(manager)) as Arc<dyn SubagentDispatcher>),
        models,
        None,
    );
    let agent_file = loop_.space_cwd.join(".agents/agents/scout.md");
    if let Some(parent) = agent_file.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &agent_file,
        "---\nname: scout\ndescription: Fast recon.\nmodel: fake/m2\nthinking: low\ntools: [read]\n---\nYou are a scout.\n",
    )
    .unwrap();
    loop_.handle_prompt(&text_prompt("go")).await;
    wait_for_event(&mut events_rx, SPAWN_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the parent turn settled");
    let events = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let started = events
        .iter()
        .find(|(name, _)| name == "subagent-session-started")
        .expect("the `subagent-session-started` frame was emitted");
    // THE fix: the label the resolver actually selected by.
    assert_eq!(
        started.1["agentName"], "scout",
        "the payload carries the TRIMMED name — the marker and the label must \
         describe one string, got {:?}",
        started.1["agentName"]
    );
    // And the trim did not COST the dispatch anything: it still matched, and it
    // still ran the frontmatter (so this is not the `Unmatched` path in a
    // different costume).
    assert_eq!(
        started.1["nameResolution"], "matched",
        "a padded real name is still MATCHED"
    );
    assert_eq!(
        started.1["model"], "fake/m2",
        "the padded name still resolved the frontmatter's model"
    );
}

/// `SubagentWait` mechanics (the `MockDispatcher` seam — a QUEUED
/// oneshot): a resolving `Completed` outcome completes the `subagent`
/// tool with the captured output (the `select!`'s outcome arm fires
/// before the turn-cancel arm).
#[tokio::test]
async fn subagent_wait_a_resolving_completed_outcome_completes_the_tool() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = crate::agent::harness::dispatch::MockDispatcher::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::agent::subagent::SubagentCancel::none());
    // The test resolves the QUEUED oneshot BEFORE the turn starts (the
    // oneshot's value is buffered — the loop's `select!` sees it
    // resolved immediately when it pops it at `dispatch`).
    tx.send(SubagentOutcome::Completed {
        output: "the answer".to_string(),
        metrics: crate::agent::subagent::SubagentMetrics {
            output: "the answer".to_string(),
            input_tokens: 10,
            output_tokens: 5,
            cost: Some(0.0),
            duration_ms: 1,
        },
    })
    .unwrap();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    // The `subagent` tool result: the `Completed` output verbatim.
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(!is_error, "a `Completed` outcome is a success result");
    assert_eq!(
        result["content"][0]["text"], "the answer",
        "the tool result is the `Completed` output verbatim"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// (issue #93, narrowing issue #78) A `subagent` call carrying `agent` instead
/// of `agentName` is REJECTED before dispatch: NO child is spawned, and the
/// rejection sentence is the WHOLE tool result.
///
/// WHY THE PREMISE CHANGED. This test was
/// `a_mis_named_subagent_param_still_dispatches_and_announces_itself`. #78
/// pinned `!is_error` because its checker ran AFTER the child had been
/// dispatched: failing the call would have destroyed an expensive, already
/// COMPLETED delegation over a typo, so the only remedy #78 could reach was to
/// dispatch anyway and ANNOUNCE the mis-named key as a second text block. #93
/// moved that same verdict to a PRE-DISPATCH rejection — `run_tool_batch`
/// (`harness/loop/turn.rs`) gates the subagent futures' `.filter()` on it, so
/// the child is never spawned. A rejection therefore spends nothing: there is
/// no completed delegation left to destroy, and #78's subagent exemption has no
/// remaining justification (roadmap D1: "narrowed, not erased"). Hence the
/// inverted `is_error` and the DELETED child-output assertions — there is no
/// child output any more, the rejection IS the result.
///
/// Still on the `MockDispatcher` seam (not the `fake_worker` `WorkerManager`),
/// now for a sharper reason: the mock's queue IS the no-dispatch proof —
/// `dispatch()` pops one entry per child, so a queue that is still full after
/// the turn proves the loop never spawned one. The queued outcome is deliberately
/// RESOLVABLE (the sender is sent to, not forgotten): pre-change the child ran to
/// `Completed { output: "the answer" }`, so this test fails for the right reason
/// — a child that ran — instead of hanging on a dropped sender.
#[tokio::test]
async fn a_mis_named_subagent_param_is_rejected_and_never_dispatches_a_child() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                // `agent` is NOT advertised — the checker's whole trigger.
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task", "agent": "reviewer" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = Arc::new(crate::agent::harness::dispatch::MockDispatcher::new());
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::agent::subagent::SubagentCancel::none());
    // A child that was never dispatched never pops this entry — see the doc
    // comment for why it is resolvable rather than left hanging.
    tx.send(SubagentOutcome::Completed {
        output: "the answer".to_string(),
        metrics: crate::agent::subagent::SubagentMetrics {
            output: "the answer".to_string(),
            input_tokens: 10,
            output_tokens: 5,
            cost: Some(0.0),
            duration_ms: 1,
        },
    })
    .unwrap();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(dispatcher.clone() as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        is_error,
        "a mis-named param REJECTS the call (#93). #78's `!is_error` pinned a \
         warning that could only announce a child it had already paid for; the \
         verdict now lands before dispatch, so there is nothing to be lenient \
         about: {result:?}"
    );
    assert_eq!(
        result["content"].as_array().unwrap().len(),
        1,
        "the rejection is the WHOLE result — no child output to append it to: {result:?}"
    );
    let rejection = result["content"][0]["text"]
        .as_str()
        .expect("the rejection is the first (only) text block");
    assert!(
        rejection.starts_with("unknown param(s) for `subagent`"),
        "the rejection names the tool (no `⚠`, no `— ignored.` — it is no longer \
         a warning riding inside a success): {rejection}"
    );
    assert!(rejection.contains("`agent`"), "{rejection}");
    assert!(
        rejection.contains("agentName"),
        "the suggestion is the useful half — the model needs the key to retry \
         with: {rejection}"
    );
    assert!(
        !result.to_string().contains("the answer"),
        "no child output rides anywhere in a rejected call's result: {result:?}"
    );

    // THE load-bearing line: the child was NEVER DISPATCHED. `dispatch()` pops
    // one queue entry per child, so a still-full queue is the no-spawn proof —
    // the reason #78's "an expensive delegation would be wasted" premise no
    // longer holds (nothing was spent).
    assert_eq!(
        dispatcher.outcomes.lock().unwrap().len(),
        1,
        "a rejected `subagent` call must not consume a dispatch queue entry"
    );

    // The rejection travels into the TRANSCRIPT (the message list the model is
    // re-sent next turn), not just the UI event — and it travels ALONE.
    let tool_message = loop_
        .messages
        .iter()
        .find(|m| m.role == ChatRole::Tool && m.tool_call_id.as_deref() == Some("t1"))
        .expect("the tool message for t1");
    let MessageContent::Blocks(blocks) = &tool_message.content else {
        panic!("the tool message carries content blocks");
    };
    assert_eq!(
        blocks.len(),
        1,
        "the transcript carries the rejection and nothing else — #78's two-block \
         shape (child output + announcement) is gone with the child: {blocks:?}"
    );
    let ContentBlock::Text { text } = &blocks[0] else {
        panic!("the rejection is a text block: {blocks:?}")
    };
    assert_eq!(
        text.as_str(),
        rejection,
        "the model reads the same sentence the UI event carried"
    );

    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// `dispatch` resolves after `delay`) while counting how many subagents
/// are in-flight CONCURRENTLY (the parallel-dispatch assertion — the max
/// in-flight count is N for a concurrent batch, 1 for sequential).
struct ConcurrentCountingDispatcher {
    delay: Duration,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

impl ConcurrentCountingDispatcher {
    fn new(delay: Duration) -> (Self, Arc<std::sync::atomic::AtomicUsize>) {
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            Self {
                delay,
                in_flight: in_flight.clone(),
                max_in_flight: max_in_flight.clone(),
            },
            max_in_flight,
        )
    }
}

impl SubagentDispatcher for ConcurrentCountingDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        _task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::agent::subagent::SubagentCancel,
    ) {
        let in_flight = self.in_flight.clone();
        let max_in_flight = self.max_in_flight.clone();
        let c = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let m = max_in_flight.load(Ordering::SeqCst);
            if c <= m {
                break;
            }
            if max_in_flight
                .compare_exchange(m, c, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let delay = self.delay;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(SubagentOutcome::Completed {
                output: "done".to_string(),
                metrics: crate::agent::subagent::SubagentMetrics {
                    output: "done".to_string(),
                    input_tokens: 1,
                    output_tokens: 1,
                    cost: Some(0.0),
                    duration_ms: 1,
                },
            });
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
        (rx, crate::agent::subagent::SubagentCancel::none())
    }
}

/// ADR 0025 follow-up: the `subagent` tool calls in ONE assistant
/// message run CONCURRENTLY (the model-loop tool execution batches them
/// via `join_all` — N subagents take ~1× the individual duration, not
/// N×). The assertion: the max CONCURRENT in-flight subagent count is
/// ≥ 2 (a sequential loop would peak at 1).
#[tokio::test]
async fn subagent_calls_in_one_message_run_in_parallel() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t3".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "c" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, max_in_flight) = ConcurrentCountingDispatcher::new(Duration::from_millis(50));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
    // CONCURRENT dispatch: ≥ 2 subagents were in-flight at the same
    // time (a sequential loop would peak at 1).
    assert!(
        max_in_flight.load(Ordering::SeqCst) >= 2,
        "the subagent tool calls ran concurrently (max in-flight = {}), \n\
         not sequentially (which would peak at 1)",
        max_in_flight.load(Ordering::SeqCst)
    );
}

/// A test `SubagentDispatcher` for the CANCEL-during-batch test: each
/// `dispatch` returns a NEVER-resolving oneshot (the subagent stays
/// in-flight) + a `SubagentCancel::Flag` whose flip records the cancel.
/// The test stores the watch receivers so it can assert `cancel.cancel()`
/// fired for every in-flight subagent when the turn is cancelled.
struct CancellingDispatcher {
    cancels: Arc<StdMutex<Vec<watch::Receiver<bool>>>>,
}

impl CancellingDispatcher {
    fn new() -> (Self, Arc<StdMutex<Vec<watch::Receiver<bool>>>>) {
        let cancels = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                cancels: cancels.clone(),
            },
            cancels,
        )
    }
}

impl SubagentDispatcher for CancellingDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        _task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::agent::subagent::SubagentCancel,
    ) {
        let (flag_tx, flag_rx) = watch::channel(false);
        self.cancels.lock().unwrap().push(flag_rx);
        // The oneshot sender is FORGOTTEN (never sent, never dropped) so
        // the receiver stays PENDING — the subagent remains in-flight
        // until the turn's `select!` `turn.cancelled()` arm fires.
        let (os_tx, os_rx) = tokio::sync::oneshot::channel();
        std::mem::forget(os_tx);
        (
            os_rx,
            crate::agent::subagent::SubagentCancel::new_flag(flag_tx),
        )
    }
}

/// A test `SubagentDispatcher` for the ORDERING test: a subagent whose
/// `task` contains `"a"` (tool call `t1`) resolves SLOW (300 ms); one
/// whose `task` contains `"b"` (tool call `t2`) resolves FAST (20 ms) —
/// so `t2` completes FIRST. It records the COMPLETION order so the test
/// can prove the append-order reordering is real (not vacuous).
struct OutOfOrderDispatcher {
    completion_order: Arc<StdMutex<Vec<String>>>,
}

impl OutOfOrderDispatcher {
    fn new() -> (Self, Arc<StdMutex<Vec<String>>>) {
        let completion_order = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                completion_order: completion_order.clone(),
            },
            completion_order,
        )
    }
}

impl SubagentDispatcher for OutOfOrderDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::agent::subagent::SubagentCancel,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let delay = if task.contains('a') {
            Duration::from_millis(300)
        } else {
            Duration::from_millis(20)
        };
        let completion_order = self.completion_order.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            completion_order.lock().unwrap().push(task.clone());
            let _ = tx.send(SubagentOutcome::Completed {
                output: task.clone(),
                metrics: crate::agent::subagent::SubagentMetrics {
                    output: task.clone(),
                    input_tokens: 1,
                    output_tokens: 1,
                    cost: Some(0.0),
                    duration_ms: 1,
                },
            });
        });
        (rx, crate::agent::subagent::SubagentCancel::none())
    }
}

/// Parallel-batch CANCEL safety: when the turn is cancelled while N
/// subagents are in-flight, EVERY in-flight subagent's `cancel.cancel()`
/// fires (the `select!`'s `turn.cancelled()` arm) and the turn settles
/// (the `join_all` unwinds through all N cancel arms — no hang on a slow
/// / never-resolving worker).
#[tokio::test]
async fn subagent_batch_a_turn_cancel_cancels_all_in_flight_subagents() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, cancels) = CancellingDispatcher::new();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    // The `join_all` batch blocks (the oneshots never resolve) until the
    // turn is cancelled — so drive it in a task.
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
    // Wait (up to `SETTLE`) for BOTH subagents to be dispatched (in-flight).
    let deadline = tokio::time::Instant::now() + SETTLE;
    while cancels.lock().unwrap().len() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        cancels.lock().unwrap().len(),
        2,
        "both subagents were dispatched (in-flight) before the cancel"
    );
    // Cancel the turn — the in-flight `select!`s must fire.
    turn_cancel.lock().unwrap().cancel();
    // The turn must SETTLE (the `join_all` unwinds through both cancel
    // arms — it does NOT hang on the never-resolving oneshots).
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn settled after the cancel (no hang)");
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("agent_settled");
    // BOTH in-flight subagents were cancelled (`cancel.cancel()` fired for
    // each — the watch flag flipped to `true`).
    let flipped = cancels
        .lock()
        .unwrap()
        .iter()
        .filter(|r| *r.borrow())
        .count();
    assert_eq!(
        flipped, 2,
        "every in-flight subagent's `cancel.cancel()` fired on a turn cancel"
    );
}

/// Parallel-batch ORDERING: a subagent that completes FIRST (t2, fast)
/// must still be appended AFTER a slower one (t1) — the results are
/// appended in the ORIGINAL `tool_calls` order, not completion order.
#[tokio::test]
async fn subagent_batch_results_are_appended_in_original_order_regardless_of_completion_order() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, completion_order) = OutOfOrderDispatcher::new();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    // Collect the `tool_execution_end` order (drain until `agent_settled`).
    let mut order = Vec::new();
    while let Ok(e) = events_rx.try_recv() {
        if let RpcEvent::tool_execution_end {
            ref tool_call_id, ..
        } = e
        {
            order.push(tool_call_id.clone());
        }
        if matches!(e, RpcEvent::agent_settled) {
            break;
        }
    }
    // PROVE the reordering is real: the fast one (t2 / "b") completed
    // FIRST (so appending t1-then-t2 is a genuine reordering, not a
    // vacuous pass where t1 happened to finish first).
    assert_eq!(
        *completion_order.lock().unwrap(),
        vec!["b".to_string(), "a".to_string()],
        "t2 (the fast one) completed before t1 — the append-order test is not vacuous"
    );
    assert_eq!(
        order,
        vec!["t1".to_string(), "t2".to_string()],
        "results appended in the ORIGINAL `tool_calls` order (t1 before t2), even though t2 (the fast one) completed first"
    );
}

/// Parallel-batch MIXED message: a `subagent` call + a non-`subagent`
/// tool (`ls`, ungated) in ONE assistant message — the `subagent` is
/// batched (Phase 1), the `ls` runs sequentially (Phase 2); BOTH results
/// are appended in the original order and the turn settles.
#[tokio::test]
async fn subagent_batch_mixed_with_a_non_subagent_tool_runs_both_in_order() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "ls".to_string(),
                arguments: json!({ "path": "." }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (dispatcher, _max) = ConcurrentCountingDispatcher::new(Duration::from_millis(20));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    let mut order = Vec::new();
    while let Ok(e) = events_rx.try_recv() {
        if let RpcEvent::tool_execution_end {
            ref tool_call_id, ..
        } = e
        {
            order.push(tool_call_id.clone());
        }
        if matches!(e, RpcEvent::agent_settled) {
            break;
        }
    }
    assert_eq!(
        order,
        vec!["t1".to_string(), "t2".to_string()],
        "both the `subagent` (batched) and the `ls` (sequential) ran, in the original `tool_calls` order"
    );
}

/// A test `SubagentDispatcher` for the COMPLETED-result-survives-Stop
/// test: a subagent whose `task` contains `"a"` (t1) resolves FAST (20 ms,
/// `Completed`); one whose `task` contains `"b"` (t2) NEVER resolves
/// (stays in-flight until the turn is cancelled).
struct PartialResolveDispatcher;

impl SubagentDispatcher for PartialResolveDispatcher {
    fn dispatch(
        &self,
        _parent_session_id: &str,
        _parent_cwd: &std::path::Path,
        _parent_model: &Model,
        _parent_enabled_tools: Vec<String>,
        _agent_name: String,
        _name_resolution: AgentNameResolution,
        _launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        crate::agent::subagent::SubagentCancel,
    ) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if task.contains('a') {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = tx.send(SubagentOutcome::Completed {
                    output: "t1-done".to_string(),
                    metrics: crate::agent::subagent::SubagentMetrics {
                        output: "t1-done".to_string(),
                        input_tokens: 1,
                        output_tokens: 1,
                        cost: Some(0.0),
                        duration_ms: 1,
                    },
                });
            });
        } else {
            // t2: the oneshot sender is FORGOTTEN (never sent) so the
            // receiver stays PENDING — t2 stays in-flight until cancelled.
            std::mem::forget(tx);
        }
        (rx, crate::agent::subagent::SubagentCancel::none())
    }
}

/// Parallel-batch CANCEL: a subagent that COMPLETED before a mid-batch
/// Stop keeps its real (`Completed`) result — it is NOT replaced by
/// `"cancelled"` (the fix-#1 core behavior). A still-in-flight subagent
/// IS `"cancelled"`.
#[tokio::test]
async fn subagent_batch_a_completed_result_survives_a_mid_batch_stop() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "a" }),
            }),
            ProviderEvent::ToolCall(ToolCall {
                id: "t2".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "b" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel.clone(),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(PartialResolveDispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    // The `join_all` batch blocks (t2 never resolves) until the turn is
    // cancelled — so drive it in a task.
    let task = tokio::spawn(async move { loop_.handle_prompt(&text_prompt("go")).await });
    // Let t1 complete (its `Completed` result is cached in the batch);
    // t2 stays in-flight.
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Cancel the turn — t2 (in-flight) is cancelled; t1's result is cached.
    turn_cancel.lock().unwrap().cancel();
    let _ = tokio::time::timeout(SETTLE, task)
        .await
        .expect("the turn settled after the cancel");
    // Collect the tool results (drain until `agent_settled` — the
    // `tool_execution_end` events are emitted BEFORE the settle, so a
    // `wait_for_event` first would have consumed them and the drain
    // below would see nothing).
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut results: HashMap<String, Value> = HashMap::new();
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("agent_settled not seen within the settle bound");
        }
        match tokio::time::timeout(Duration::from_millis(100), events_rx.recv()).await {
            Ok(Some(e)) => {
                if let RpcEvent::tool_execution_end {
                    ref tool_call_id,
                    ref result,
                    ..
                } = e
                {
                    results.insert(tool_call_id.clone(), result.clone());
                }
                if matches!(e, RpcEvent::agent_settled) {
                    break;
                }
            }
            Ok(None) | Err(_) => {
                panic!("the event channel closed / timed out before agent_settled");
            }
        }
    }
    // t1 (completed before the Stop) keeps its REAL result, not "cancelled".
    assert_eq!(
        results
            .get("t1")
            .and_then(|v| v["content"][0]["text"].as_str()),
        Some("t1-done"),
        "a completed subagent's result survives a mid-batch Stop (fix #1)"
    );
    // t2 (in-flight at the Stop) is "cancelled".
    assert_eq!(
        results
            .get("t2")
            .and_then(|v| v["content"][0]["text"].as_str()),
        Some("cancelled"),
        "an in-flight subagent is 'cancelled' on a mid-batch Stop"
    );
}

/// `SubagentWait` mechanics: a `Failed` outcome is an ERROR tool
/// result (the `subagent failed: {error}` text — the parent can
/// retry / reword the task).
#[tokio::test]
async fn subagent_wait_a_failed_outcome_is_an_error_result() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "subagent".to_string(),
                arguments: json!({ "task": "the task" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let dispatcher = crate::agent::harness::dispatch::MockDispatcher::new();
    let (tx, rx) = tokio::sync::oneshot::channel();
    dispatcher.queue(rx, crate::agent::subagent::SubagentCancel::none());
    // The test resolves the QUEUED oneshot BEFORE the turn starts
    // (the oneshot's value is buffered — the loop's `select!` sees
    // it resolved immediately when it pops it at `dispatch`).
    tx.send(SubagentOutcome::Failed {
        reason: crate::agent::subagent::SubagentFailureReason::Other,
        error: "boom".to_string(),
        output: String::new(),
        metrics: crate::agent::subagent::SubagentMetrics::default(),
    })
    .unwrap();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    loop_.handle_prompt(&text_prompt("go")).await;
    // The `subagent` tool result: the `Failed` error, verbatim.
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "a `Failed` outcome is an error result");
    assert_eq!(
        result["content"][0]["text"], "subagent failed: boom",
        "the tool result carries the `Failed` error verbatim"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled");
}

/// `SubagentWait` mechanics: an UNRESOLVED outcome (the `MockDispatcher`
/// empty queue — a never-resolving oneshot) + a turn cancel → the
/// `select!`'s turn-cancel arm wins (the `SubagentCancel` is flipped
/// — a no-op here — and the tool result is `cancelled`).
#[tokio::test]
async fn subagent_wait_a_turn_cancel_cancels_the_dispatch() {
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::ToolCall(ToolCall {
            id: "t1".to_string(),
            name: "subagent".to_string(),
            arguments: json!({ "task": "the task" }),
        }),
        ProviderEvent::Done(FinishReason::ToolCalls),
    ])]);
    // The EMPTY queue — the `dispatch` returns a never-resolving
    // oneshot (the outcome arm never fires).
    let dispatcher = crate::agent::harness::dispatch::MockDispatcher::new();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let turn_cancel_c = turn_cancel.clone();
    let (_space, mut loop_) = build_loop_with_dispatcher(
        Box::new(provider),
        events_tx,
        turn_cancel,
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        Arc::new(TestSink {
            updates: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(Arc::new(dispatcher) as Arc<dyn SubagentDispatcher>),
        vec![fake_model("m1")],
        None,
    );
    // The turn runs in a task (`handle_prompt` awaits the WHOLE turn
    // — the `SubagentWait` select blocks on the never-resolving
    // oneshot until the turn-cancel arm fires).
    let turn = tokio::spawn(async move {
        loop_.handle_prompt(&text_prompt("go")).await;
    });
    // Give the turn time to reach the `SubagentWait` select, then
    // cancel (the `select!`'s turn-cancel arm fires — the
    // `SubagentCancel` is flipped + the `Cancelled` tool result).
    tokio::time::sleep(Duration::from_millis(100)).await;
    turn_cancel_c
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel();
    turn.await.expect("the turn task completed");
    let end = wait_for_event(
        &mut events_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the `subagent`'s tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(is_error, "a cancelled dispatch is an error result");
    assert_eq!(
        result["content"][0]["text"], "cancelled",
        "the `Cancelled` tool result"
    );
    wait_for_event(&mut events_rx, SETTLE_MS, |e| {
        matches!(e, RpcEvent::agent_settled)
    })
    .await
    .expect("the turn settled after the cancel");
}

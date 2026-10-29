use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::support::*;
use crate::agent::events::{EventSink, RpcEvent};
use crate::agent::harness::provider::{FinishReason, Provider, ProviderEvent, ToolCall};
use crate::agent::harness::retry::RetryPolicy;
use crate::agent::permission::PermissionOutcome;
use crate::agent::policy::Decision;
use crate::storage::Db;

// ── (ADR 0030 Task 3) the file-access policy gate ───────────────────

/// `handle_prompt` under a hard bound. Every policy test uses it: a
/// gate that regressed into prompting on a turn nobody answers would
/// otherwise hang the suite until the test harness timeout (a FAIL
/// pointing at the gate is the point — not a stall).
async fn bounded_prompt(loop_: &mut AgentLoop, text: &str) {
    tokio::time::timeout(
        Duration::from_secs(30),
        loop_.handle_prompt(&text_prompt(text)),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("the turn never settled — a prompt nobody answered, or a tool that hung")
    });
}

/// The `settings.json` shape the policy tests pin (the wire key is
/// `filePolicy` — the same one the Settings UI writes). No rules (the
/// three-direction shape) — the rule pins use [`policy_settings_with_rules`].
fn policy_settings(reads: &str, writes: &str, shell: &str) -> String {
    format!(
        r#"{{ "filePolicy": {{ "reads": "{reads}", "writes": "{writes}", "shell": "{shell}" }} }}"#
    )
}

/// The same shape PLUS a `"rules"` array (`rules` is raw JSON — a list of
/// `{path, direction, decision}` objects, the wire shape `AccessRule` defines:
/// `direction` camelCase, `decision` lowercase). (T03) This is the seam every
/// rule-row pin drives through, because with `default_rules()` EMPTY a refusal
/// needs a rule the USER wrote.
fn policy_settings_with_rules(reads: &str, writes: &str, shell: &str, rules: &str) -> String {
    format!(
        r#"{{ "filePolicy": {{ "reads": "{reads}", "writes": "{writes}", "shell": "{shell}", "rules": {rules} }} }}"#
    )
}

/// One `deny` rule on `path` (the direction the write pins need).
fn deny_rule(path: &str) -> String {
    format!(r#"[{{ "path": "{path}", "direction": "write", "decision": "deny" }}]"#)
}

/// One `allow` rule on `path`.
fn allow_rule(path: &str) -> String {
    format!(r#"[{{ "path": "{path}", "direction": "write", "decision": "allow" }}]"#)
}

/// The policy-gate fixture: an `AgentLoop` whose `config_dir` holds a
/// `settings.json` with the given policy JSON (`None` = NO `config_dir`
/// at all — the no-settings-layer case), on a fresh `space_cwd`, with a
/// RECORDING sink (the `TestSink` cannot see `permission-request`) and
/// `trust: None` (untrusted — fail-closed, so an `Ask` really prompts).
/// The `config_dir` is returned WITH the loop: it must outlive it (the
/// loop re-reads the file every turn, and the temp dir is removed on
/// drop — dropping it early would silently degrade every test to the
/// all-`Allow` default).
///
/// #96: the `space_cwd` scratch guard is the FIRST element (it is a
/// production `PathBuf` field on `AgentLoop` and cannot live in the loop),
/// so the caller owns it. First, because destructured locals drop in
/// REVERSE declaration order: guard-first means the `Db` — a live SQLite
/// file inside that dir — closes before `remove_dir_all` runs.
///
/// The caller pins `$HOME` FIRST (the `RestoreHome` + `env_lock`
/// pattern) so `AgentLoop::new`'s `read_roots` / `protected_dirs` see an
/// empty scratch home: the read boundary is then EXACTLY the `cwd` and
/// the deny-list is whatever the test created, never whatever happens to
/// be in the developer's real `~/.agents`.
fn build_policy_loop(
    provider: Box<dyn Provider>,
    events: mpsc::UnboundedSender<RpcEvent>,
    sink: Arc<dyn EventSink>,
    settings: Option<&str>,
) -> (crate::test_support::ScratchDir, AgentLoop, Option<Tmp>) {
    let config_dir = settings.map(|_| Tmp::new("cfg"));
    if let (Some(dir), Some(json)) = (config_dir.as_ref(), settings) {
        std::fs::write(dir.path().join("settings.json"), json).unwrap();
    }
    // `_cfg_space` is MOVED OUT into the return tuple below: dropping it
    // here would remove the config dir while the caller still asserts
    // against it.
    let (_cfg_space, mut loop_, _db) = build_loop_with_db(
        provider,
        events,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        config_dir
            .as_ref()
            .map(|d| d.path().to_path_buf())
            .as_deref(),
    );
    // Swap the `TestSink` for the recording one (the field is `pub`).
    loop_.sink = sink;
    (_cfg_space, loop_, config_dir)
}

/// The tool call the policy tests share: a `read` of `path`.
fn read_call(id: &str, path: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        name: "read".to_string(),
        arguments: json!({ "path": path }),
    }
}

/// Await the loop's pending-permission entry and answer it (the mock
/// handle-free waiter — the `tests/harness_loop.rs` pattern, the
/// `pending` map being a `pub` field). Spawned BEFORE the prompt so a gate
/// that must NOT prompt can never hang the test.
///
/// `value` is the option's payload (`path` for `allow-directory` — the
/// frontend copies the emitted value back verbatim, so the guard pins pass the
/// string they read out of the recorded `permission-request`, NEVER a
/// fabricated one). The boundary clicks (`allow` / `trust-space`) pass `None`.
async fn answer_permission(
    pending: PendingPermissions,
    session: &str,
    option: &str,
    value: Option<String>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut map = pending.lock().await;
        if let Some(key) = map
            .keys()
            .find(|k| k.starts_with(&format!("{session}/")))
            .cloned()
        {
            let sender = map.remove(&key).expect("the entry is in the map");
            let _ = sender.send(PermissionOutcome::Selected {
                option_id: option.to_string(),
                value,
            });
            return;
        }
        drop(map);
        if tokio::time::Instant::now() >= deadline {
            return; // the test asserts no prompt arrived
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The `$HOME` pin every guard/rule test needs FIRST (so `AgentLoop::new`'s
/// `protected_dirs()` snapshot describes the scratch home, not the developer's
/// real `~/.agents`). Returns the lock, the restore guard, and the scratch home
/// — bind ALL THREE to non-underscore names so they live to the end of the
/// test (an `_`-prefixed binding drops immediately, releasing the lock and
/// skipping the restore).
///
/// `.pi/agent` is created so a write into `.pi/agent/agents` (which is NOT
/// created — the not-yet-existing shape the guard exists for) resolves to a
/// CANONICAL nearest-existing ancestor rather than a raw peel: the loud pin and
/// the `~` seam pin both assert on the emitted ancestor's spelling.
fn pin_guard_home(tag: &str) -> (std::sync::MutexGuard<'static, ()>, RestoreHome, Tmp) {
    let lock = crate::test_support::env_lock();
    let home = Tmp::new(tag);
    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let original = std::env::var_os("HOME");
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span;
    // no other thread mutates HOME concurrently.
    unsafe { std::env::set_var("HOME", home.path()) };
    (lock, RestoreHome(original), home)
}

/// The scripted two-turn script every guard pin uses: turn 1 is the `write`
/// tool call, turn 2 the terminal text (`ScriptedProvider` repeats its LAST
/// entry, so the terminal entry must be last).
fn write_scripts(path: &str, content: &str) -> Vec<Option<Vec<ProviderEvent>>> {
    vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "write".to_string(),
                arguments: json!({ "path": path, "content": content }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]
}

/// The script for a `read` of `path` (the read-not-ask pin).
fn read_scripts(path: &str) -> Vec<Option<Vec<ProviderEvent>>> {
    vec![
        Some(vec![
            ProviderEvent::ToolCall(read_call("t1", path)),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]
}

/// Mark the loop's own Space trusted (the existing
/// `a_trusted_space_suppresses_the_prompt_the_policy_would_raise` pattern: a
/// fresh `Db` under the scratch home, the Space row marked trusted, and the
/// swap done BEFORE the first prompt so the very first verdict sees it).
fn trust_the_space(loop_: &mut AgentLoop, home: &std::path::Path) {
    let db = Arc::new(Db::open(&home.join("trust.db")).unwrap());
    let cwd = loop_.space_cwd.clone();
    db.upsert_space(&cwd.display().to_string(), false).unwrap();
    db.set_space_trusted(&cwd.display().to_string(), true)
        .unwrap();
    loop_.trust = Some(Arc::new(
        crate::agent::harness::trust::SqliteTrustSource::new(db),
    ));
}

/// The `permission-request` frames a sink recorded.
fn permission_requests(events: &[(String, Value)]) -> Vec<Value> {
    events
        .iter()
        .filter(|(name, _)| name == "permission-request")
        .map(|(_, p)| p.clone())
        .collect()
}

/// Drain every event the turn emitted and return the
/// `tool_execution_end` results keyed by tool-call id (a turn may batch
/// several calls, so ONE drain collects them all — draining per id would
/// consume the later calls' events while looking for an earlier one).
fn tool_ends(events: &mut mpsc::UnboundedReceiver<RpcEvent>) -> HashMap<String, (Value, bool)> {
    let mut out = HashMap::new();
    while let Ok(ev) = events.try_recv() {
        if let RpcEvent::tool_execution_end {
            tool_call_id,
            result,
            is_error,
            ..
        } = ev
        {
            out.insert(tool_call_id, (result, is_error));
        }
    }
    out
}

/// (ADR 0030) `reads: Ask` + an out-of-boundary `read` on an UNTRUSTED
/// Space: a `permission-request` naming THE PATH (not a JSON blob), and
/// after the user allows, the read really lands.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn ask_beyond_the_boundary_prompts_naming_the_path_then_runs() {
    let home = Tmp::new("ask-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore
    // span; no other thread mutates HOME concurrently.
    unsafe { std::env::set_var("HOME", home.path()) };

    let outside = Tmp::new("ask-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(read_call("t1", &path.display().to_string())),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("ask", "ask", "ask")),
    );
    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow", None));
    bounded_prompt(&mut loop_, "go").await;
    answer.await.unwrap();

    let prompts = permission_requests(&sink_events.lock().unwrap());
    assert_eq!(prompts.len(), 1, "exactly one prompt, got {prompts:?}");
    assert_eq!(prompts[0]["requestId"], "t1");
    assert_eq!(
        prompts[0]["request"]["toolCall"]["title"],
        format!("read {}", path.display()),
        "the prompt names the path in plain form"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
    assert!(!is_error, "the allowed read succeeded: {result:?}");
    assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
}

/// (ADR 0030) `reads: Sandboxed` + an out-of-boundary `read`: a
/// tool-result error worded like the executor's own rejection, and NO
/// `permission-request` (a Deny never reaches the UI — Trust included:
/// this loop is untrusted, and the trusted case is the sibling test).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn sandboxed_beyond_the_boundary_errors_with_no_prompt() {
    let home = Tmp::new("deny-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", home.path()) };

    let outside = Tmp::new("deny-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(read_call("t1", &path.display().to_string())),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("sandboxed", "sandboxed", "allow")),
    );
    bounded_prompt(&mut loop_, "go").await;

    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a Deny emits no permission-request"
    );
    assert!(
        loop_.pending_permissions.try_lock().unwrap().is_empty(),
        "and registers no oneshot"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the denied read is an error");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert_eq!(
        text,
        format!(
            "read: path escapes the session sandbox: {}",
            path.canonicalize().unwrap().display()
        ),
        "worded EXACTLY like the executor's rejection, so the model cannot tell which layer said no"
    );
}

/// (ADR 0030, Deviation 2 — the intended widening) the DEFAULT policy
/// (all `Allow`, and the no-`config_dir` case that behaves the same):
/// an out-of-boundary `read` AND an out-of-boundary `write` both
/// dispatch silently — no prompt, no error — and the write LANDS. This
/// is the behavior the feature deliberately widens (pre-feature a write
/// prompted); it is asserted as the widening, so a regression that
/// re-tightens the default reddens it.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_default_policy_reads_and_writes_beyond_the_boundary_in_silence() {
    let home = Tmp::new("allow-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", home.path()) };

    let outside = Tmp::new("allow-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let read_path = outside.path().join("secret.txt");
    let write_path = outside.path().join("written-by-agent.txt");
    for settings in [Some(&policy_settings("allow", "allow", "allow")), None] {
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(read_call("t1", &read_path.display().to_string())),
                ProviderEvent::ToolCall(ToolCall {
                    id: "t2".to_string(),
                    name: "write".to_string(),
                    arguments: json!({
                        "path": write_path.display().to_string(),
                        "content": "WROTE-IT",
                    }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
        let (_space, mut loop_, _cfg) = build_policy_loop(
            Box::new(provider),
            events_tx,
            Arc::new(RecordingSink {
                events: sink_events.clone(),
            }),
            settings.map(|s| s.as_str()),
        );
        bounded_prompt(&mut loop_, "go").await;
        assert!(
            permission_requests(&sink_events.lock().unwrap()).is_empty(),
            "the default policy prompts for nothing"
        );
        // The `read` AND the `write` are separate tool calls in one
        // batch: the script emits them as two `ToolCall` events (the
        // provider stream carries one call per event — see the
        // `read`-then-`write` batch this mirrors).
        let ends = tool_ends(&mut events_rx);
        let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
        assert!(!is_error, "the default policy never errors: {result:?}");
        assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
        let (result, is_error) = ends.get("t2").expect("the write dispatched").clone();
        assert!(
            !is_error,
            "the default policy writes beyond the cwd: {result:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&write_path).unwrap_or_default(),
            "WROTE-IT",
            "the widened write actually landed"
        );
        std::fs::remove_file(&write_path).unwrap();
    }
}

/// (ADR 0030 Deviation 4 — "affects an already-running session") the
/// policy is re-read from `settings.json` at the top of EVERY prompt:
/// a settings edit BETWEEN two prompts changes the SECOND prompt's
/// gating, with no restart and no relaunch.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_settings_edit_between_prompts_changes_the_next_turns_gating() {
    let home = Tmp::new("fresh-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", home.path()) };

    let outside = Tmp::new("fresh-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt").display().to_string();
    // A THREE-call script: turn 1's `read`, the summary-free turn 2's
    // `read`, then the final text (the provider repeats its LAST entry,
    // so the third script entry must be the terminal one).
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(read_call("t1", &path)),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("first done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
        Some(vec![
            ProviderEvent::ToolCall(read_call("t2", &path)),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("second done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let settings = policy_settings("ask", "ask", "ask");
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&settings),
    );
    // The loop's own `config_dir` (the fixture's temp dir): re-read
    // between the prompts, exactly as a user editing Settings would.
    let config_dir = loop_.config_dir.clone().expect("a settings dir");

    // Turn 1: `reads: ask` → the prompt fires (answered `allow`).
    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow", None));
    bounded_prompt(&mut loop_, "one").await;
    answer.await.unwrap();
    assert_eq!(
        permission_requests(&sink_events.lock().unwrap()).len(),
        1,
        "turn 1 prompts under `ask`"
    );

    // The user edits the file BETWEEN the prompts: `ask` → `sandboxed`.
    std::fs::write(
        config_dir.join("settings.json"),
        policy_settings("sandboxed", "sandboxed", "allow"),
    )
    .unwrap();

    // Turn 2: the SAME out-of-boundary read is now a Deny — no prompt,
    // an error result. A per-session (or per-process) snapshot of the
    // policy would still prompt here, which is what this asserts against.
    bounded_prompt(&mut loop_, "two").await;
    assert_eq!(
        permission_requests(&sink_events.lock().unwrap()).len(),
        1,
        "the second turn must NOT prompt after the file changed to `sandboxed`"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t2").expect("a tool result for t2").clone();
    assert!(is_error, "the now-Sandboxed read is refused: {result:?}");
}

/// (ADR 0030 Task 5) `shell: Sandboxed` means CONFINE: the gate never
/// prompts, the command runs INSIDE the Landlock ruleset, and a kernel
/// that cannot confine FAILS CLOSED with a visible error instead of
/// running unsandboxed. Both branches assert "no prompt".
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_sandboxed_shell_dispatches_confined_with_no_prompt() {
    let home = Tmp::new("shell-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", home.path()) };

    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: json!({ "command": "touch confined_ran.txt" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    // #96: the `space_cwd` guard is a TEST-SCOPE local, not a block local.
    // This test asserts AFTER the block on a file inside `space_cwd`
    // (`cwd.join("confined_ran.txt")`), and today that only works because
    // the dir leaked. With the guard owning the dir, a guard declared inside
    // the block would remove it at the block's closing brace and redden the
    // `confined_here` arm on any Landlock machine. Same rule the sibling
    // `a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording`
    // records for its `link` dir: the thing the post-block assertions read must
    // OUTLIVE the block, so it is bound here. The BLOCK stays — it scopes the
    // TURN — and no assertion moves.
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "sandboxed")),
    );
    let cwd = {
        let cwd = loop_.space_cwd.clone();
        bounded_prompt(&mut loop_, "go").await;
        cwd
    };
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "`shell: sandboxed` asks nothing — it confines (here: refuses to run unsandboxed)"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    // Can THIS machine confine? `cfg`-gated (not `cfg!`) so a non-Linux
    // build does not name the sandbox module, which is not compiled
    // there at all.
    #[cfg(target_os = "linux")]
    let confined_here = crate::agent::tools::sandbox::landlock_available();
    #[cfg(not(target_os = "linux"))]
    let confined_here = false;
    if confined_here {
        // The kernel confines → the command RUNS (inside the ruleset)
        // and its write inside the boundary lands.
        assert!(!is_error, "a confined run succeeds: {result:?}");
        assert!(
            cwd.join("confined_ran.txt").exists(),
            "the confined command ran inside the boundary"
        );
    } else {
        // No Landlock / not Linux → a VISIBLE error and NO run.
        assert!(is_error, "an unavailable sandbox is an error: {result:?}");
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .contains("Sandboxed shell is unavailable"),
            "the error names the reason: {result:?}"
        );
        assert!(
            !cwd.join("confined_ran.txt").exists(),
            "fail CLOSED: the command must not have run"
        );
    }
}

/// (T03 row 1, the GATE half of the churned pair — RENAMED from
/// `a_gate_denial_of_a_protected_dir_uses_the_protected_wording`) a `deny` RULE
/// naming a user-level agent-definition dir is refused by the GATE in the
/// deny-list's own words (the same text the executor produces).
///
/// WHY A RULE IS NOW PART OF THE FIXTURE: `default_rules()` is EMPTY (the
/// permissions spec's round-3 correction — the four dirs are the
/// guard-membership set, not default deny rules), so a NO-RULE protected write
/// is the row-3 ASK, not a deny (pinned by
/// `default_posture_agent_dir_write_is_an_ask_not_a_silent_deny`). The refusal
/// asserted here is precedence row 1: a USER deny. The WORDING — what this
/// fixture exists to pin — is unchanged, and it is still the deny-list sentence
/// rather than "escapes the sandbox", because a denied write into one of the
/// four dirs must name the real reason. The no-prompt half is load-bearing too:
/// a MATCHING rule means row 3 does not apply, so the guard must stay silent.
///
/// `$HOME` is spelled through a SYMLINK on unix (the 3fd702f fixture trick
/// at the GATE seam): `boundary::protected_dirs()` hands the gate the
/// CANONICAL entries (through `real`) while the model's argument is the RAW
/// `$HOME` spelling (through `link`), and the target does not exist yet, so
/// the argument is exactly the shape `canonicalize_lenient` resolves. Linux
/// `temp_dir()` is already canonical, hence the manufactured symlink: without
/// the argument-side canonicalization this test is RED on all three
/// platforms (pre-fix it was green on Linux by luck and red on macOS CI —
/// `/var` → `/private/var` — and Windows, raw `C:\…` vs verbatim `\\?\C:\…`).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording() {
    let real = Tmp::new("protected-home");
    // The `link` dir must OUTLIVE the test (it holds the `$HOME` symlink),
    // so it is bound here rather than inside the `cfg` arm.
    #[cfg(unix)]
    let link = Tmp::new("protected-home-link");
    #[cfg(unix)]
    let home = {
        // `link -> real`, and `$HOME` is the LINK: the deny-list entries
        // come back canonical (`real/…`), the model's path is raw (`link/…`).
        std::os::unix::fs::symlink(real.path(), link.path().join("home")).unwrap();
        let home = link.path().join("home");
        assert_ne!(
            home.canonicalize().unwrap(),
            home,
            "the fixture must hand the gate a RAW `$HOME`"
        );
        home
    };
    #[cfg(not(unix))]
    let home = {
        // No symlink privileges: `temp_dir()`'s own spelling is already the
        // raw one `canonicalize` rewrites (on Windows into the verbatim
        // `\\?\…` form), so the raw-vs-canonical split is the platform's.
        real.path().to_path_buf()
    };
    for dir in [
        ".agents/skills",
        ".pi/agent/skills",
        ".agents/agents",
        ".pi/agent/agents",
    ] {
        std::fs::create_dir_all(home.join(dir)).unwrap();
    }
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", &home) };

    let target = home.join(".agents/skills/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        // `writes: allow` on purpose: the rule deny must win over the WIDENING,
        // so the refusal below can only come from the rule row (row 1) and not
        // from the posture select.
        Some(&policy_settings_with_rules(
            "allow",
            "allow",
            "allow",
            &deny_rule("~/.agents/skills"),
        )),
    );
    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a rule deny is a Deny, never a prompt — and the guard ask must NOT fire \
         either, because a MATCHING rule means row 3 does not apply"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the protected write is refused: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the refusal names the real reason, got {text:?}"
    );
    // (issue #33) The route is part of the sentence the two layers must agree
    // on: a gate denial that refused without pointing anywhere is exactly the
    // drift this shared text exists to prevent. Asserted against the SAME const
    // the executor-side test uses, by its full path (no re-export).
    assert!(
        text.ends_with(crate::agent::tools::exec::access::PROTECTED_WRITE_ROUTE),
        "the gate's refusal must end with the sanctioned route, got {text:?}"
    );
    assert!(
        !target.exists(),
        "and nothing was written (the gate stopped it before the executor)"
    );
}

/// (issue #92 + T03 row 1, the GATE half of the session-snapshot pair) The GATE
/// seam, driven through the REAL `AgentLoop::new` snapshot: a protected dir that
/// was missing when the Session started and was recreated mid-session must be
/// refused by the gate WITHOUT a restart, and worded as the guard-membership
/// refusal.
///
/// No symlink fixture here, unlike
/// [`a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording`]:
/// that test's bug was on the ARGUMENT side (a raw `$HOME` vs canonical
/// entries), which is what the `link -> real` dance manufactures. This test's
/// bug is on the ENTRY side — the entry is dropped from the list entirely — so a
/// plain `$HOME` spelling is the right fixture, and the dance was not forgotten.
///
/// (T03) The refusal needs a USER `deny` rule: with `default_rules()` EMPTY a
/// no-rule protected write is the row-3 ASK (`writes: sandboxed` used to be part
/// of this fixture precisely because the posture had to be the thing that did NOT
/// refuse; now the RULE is, so the posture is `allow` and the deny can only come
/// from row 1).
///
/// WHICH ASSERTION DISCRIMINATES, stated because it is not the obvious one:
/// `is_error` and `!target.exists()` are NOT enough — a snapshot that dropped the
/// entry still refuses, as a BOUNDARY escape, and an `is_error`-only test stays
/// green through the bug. The discriminating assertions are the WORDING ones:
/// the refusal must come back as the guard-membership sentence, not
/// `write: path escapes the session sandbox: …`. The wording is the bug's second
/// half — a model sent hunting for a boundary that is not the problem.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_recreated_protected_dir_is_refused_by_the_gate_without_a_new_session() {
    let home = Tmp::new("gate-recreated-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span.
    unsafe { std::env::set_var("HOME", home.path()) };

    // `.pi/agent` exists, `.pi/agent/agents` does NOT — the snapshot the
    // Session starts with.
    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let target = home.path().join(".pi/agent/agents/SKILL.md");

    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    // The snapshot is computed HERE, inside `AgentLoop::new`, while the dir
    // is still missing.
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        // `writes: allow` — the deny can only come from the rule (row 1).
        Some(&policy_settings_with_rules(
            "allow",
            "allow",
            "allow",
            &deny_rule("~/.pi/agent/agents"),
        )),
    );

    // The user recreates the dir AFTER the snapshot exists. No restart.
    std::fs::create_dir_all(home.path().join(".pi/agent/agents")).unwrap();

    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a rule deny is a Deny, never a prompt (not even the guard ask)"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the protected write is refused: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the refusal names the real reason, got {text:?}"
    );
    assert!(
        text.ends_with(crate::agent::tools::exec::access::PROTECTED_WRITE_ROUTE),
        "the gate's refusal must end with the sanctioned route, got {text:?}"
    );
    assert!(
        !target.exists(),
        "and nothing was written (the gate stopped it before the executor)"
    );
}

/// (ADR 0030) A TRUSTED Space suppresses an `Ask` at the GATE: the
/// verdict itself is `Allow` (so no `PendingPermissions` entry is ever
/// registered), and the out-of-boundary `read` runs. Asserted on
/// `policy_verdict` DIRECTLY as well as end-to-end — the end-to-end half
/// alone cannot tell the gate's own trust check apart from
/// `decision_for`'s (both suppress, by design), and the precedence rule
/// is worth pinning at the layer that owns it.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_trusted_space_suppresses_the_prompt_the_policy_would_raise() {
    let home = Tmp::new("trust-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", home.path()) };

    let outside = Tmp::new("trust-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(read_call("t1", &path.display().to_string())),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        // `ask` — the policy WOULD prompt; Trust is what stops it.
        Some(&policy_settings("ask", "ask", "ask")),
    );
    // The Space the loop's `space_cwd` points at, marked trusted (the
    // `Db` the fixture built is reachable through `loop_.store`, so the
    // trust source is wired to the same rows).
    let db = Arc::new(Db::open(&home.path().join("trust.db")).unwrap());
    let cwd = loop_.space_cwd.clone();
    db.upsert_space(&cwd.display().to_string(), false).unwrap();
    db.set_space_trusted(&cwd.display().to_string(), true)
        .unwrap();
    loop_.trust = Some(Arc::new(
        crate::agent::harness::trust::SqliteTrustSource::new(db),
    ));

    // The gate's OWN verdict, read AFTER the turn (so `file_policy` is
    // the `ask` the settings file carries — before the first prompt the
    // field is still the all-`Allow` default and any verdict would
    // pass). `Allow`, not `Ask`: Trust suppressed it before any prompt
    // machinery was reached.
    bounded_prompt(&mut loop_, "go").await;
    let tc = read_call("t1", &path.display().to_string());
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Allow),
        "Trust suppresses the `Ask` at the decision point itself"
    );
    // CONTRAST (so the assertion above cannot pass vacuously on a
    // policy that was never `ask`): the SAME loop and call with the
    // trust removed is an `Ask`.
    loop_.trust = None;
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Ask),
        "without Trust the very same access prompts"
    );

    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a trusted Space is never prompted (Trust suppresses `Ask`)"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
    assert!(!is_error, "and the read runs: {result:?}");
    assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
}

/// (ADR 0030) A tool the policy does NOT judge stays ungated:
/// `read_skill` takes a skill NAME and carries its own per-skill
/// containment (ADR 0029), so a `Sandboxed` read policy must not touch
/// it — and it must not gain a prompt either.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_skill_tools_stay_outside_the_policy_gate() {
    let home = Tmp::new("skill-home");
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let _restore = RestoreHome(original.clone());
    // SAFETY: as above.
    unsafe { std::env::set_var("HOME", home.path()) };

    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "read_skill".to_string(),
                arguments: json!({ "name": "nope" }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("sandboxed", "sandboxed", "sandboxed")),
    );
    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "the skill tools never prompt"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, _is_error) = ends.get("t1").expect("a tool result").clone();
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("unknown skill"),
        "the executor's OWN error (the gate stayed out of it), got {text:?}"
    );
    assert!(
        !text.contains("escapes the session sandbox"),
        "the policy must not re-word the skill tool's failure: {text:?}"
    );
}

// ── (T03) the precedence rows 1–3 and the guard prompt ────────────────
//
// Every pin below drives the REAL `AgentLoop` (scripted provider, real
// `handle_prompt`, real `gate_tool`) with `$HOME` pinned to a scratch dir, so
// `AgentLoop::new`'s frozen `protected_dirs()` snapshot describes the fixture.
// `default_rules()` is EMPTY, so:
//   * row 3 (no rule + under an agent dir) is the ORDINARY default posture —
//     no override arrangement is needed to reach it, which is the whole point
//     of the correction (0 of 16 cases reached it before);
//   * every REFUSAL pin needs a USER rule, because there is no default deny
//     left to refuse with.

/// Await the loop's `permission-request` on the RECORDING sink, take the
/// `allow-directory` option's `path` from it VERBATIM (never a fabricated
/// string — the point is that the value the user's click carries is the one the
/// gate emitted), and answer the prompt with it.
///
/// Returns the value it clicked, so the caller can assert the click matched the
/// emission. Times out loudly if no prompt arrives — a guard pin whose prompt
/// never fired must fail, not hang.
async fn answer_with_recorded_directory(
    pending: PendingPermissions,
    sink_events: Arc<StdMutex<Vec<(String, Value)>>>,
    session: &str,
    option: &str,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let recorded = {
            let events = sink_events.lock().unwrap_or_else(|p| p.into_inner());
            events
                .iter()
                .find(|(name, _)| name == "permission-request")
                .map(|(_, payload)| payload.clone())
        };
        if let Some(payload) = recorded {
            let path = payload["request"]["options"][1]["path"]
                .as_str()
                .unwrap_or_else(|| panic!("the guard prompt has no `path` on option 1: {payload}"))
                .to_string();
            let mut map = pending.lock().await;
            if let Some(key) = map
                .keys()
                .find(|k| k.starts_with(&format!("{session}/")))
                .cloned()
            {
                let sender = map.remove(&key).expect("the entry is in the map");
                let _ = sender.send(PermissionOutcome::Selected {
                    option_id: option.to_string(),
                    value: Some(path.clone()),
                });
                return path;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("the guard prompt never arrived to answer");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// **THE HEADLINE CHURN PIN.** At the DEFAULT posture (`writes: allow`, NO user
/// rules), a write into one of the four agent-definition dirs is
/// `Some(Decision::Ask)` — the guard prompt — and NOT the silent hardcoded deny
/// ADR 0030 shipped.
///
/// This is the assertion that was FALSE at the round-2 build (0 of 16 measured
/// cases across the four dirs and four nesting depths reached row 3, because the
/// guard-membership set and the default-deny set were the same paths under the
/// same prefix predicate). It is the spec's single behavioural change: "today's
/// hardcoded deny becomes an ask".
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn default_posture_agent_dir_write_is_an_ask_not_a_silent_deny() {
    let (_lock, _restore, home) = pin_guard_home("row3-default");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        // The plain default posture — three `allow`s and NO `rules` key.
        Some(&policy_settings("allow", "allow", "allow")),
    );

    // THE REACHABILITY PRECONDITION, asserted FIRST and on the verdict itself:
    // the guard arm must actually be reached. This is the line that was red for
    // every one of the 16 pre-correction probes.
    let tc = ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
    };
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Ask),
        "a default-posture write into an agent-definition dir is the ROW-3 ASK"
    );

    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow", None));
    bounded_prompt(&mut loop_, "go").await;
    answer.await.unwrap();

    let prompts = permission_requests(&sink_events.lock().unwrap());
    assert_eq!(
        prompts.len(),
        1,
        "the ASK really prompts — a `Deny` would emit nothing: {prompts:?}"
    );
    // And it is the GUARD list, not the boundary list (the guard is what asks).
    let ids: Vec<&str> = prompts[0]["request"]["options"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["optionId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["allow", "allow-directory", "reject"]);
    let ends = tool_ends(&mut events_rx);
    let (_result, is_error) = ends.get("t1").expect("the write dispatched").clone();
    assert!(!is_error, "and the answered write runs: {_result:?}");
}

/// **THE LOUD PIN.** A protected write prompts the guard, and clicking
/// `allow-directory` (with the `value` taken VERBATIM from the recorded
/// request's `options[1]["path"]`) makes the tool RUN and the file LAND.
///
/// It fails loudly on every wiring it covers:
/// * an un-extended approve predicate turns the click into `"permission
///   denied"` (the boring `else` arm) — the `!is_error` + landed-file
///   assertions redden;
/// * a guard arm that never fires means no prompt to answer — the answer helper
///   panics on its deadline;
/// * a placeholder `""` in the emitted option would be clicked verbatim and is
///   caught by the non-empty assertion below.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_guard_allow_directory_runs_the_tool() {
    let (_lock, _restore, home) = pin_guard_home("guard-loud");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );

    // THE REACHABILITY PRECONDITION FIRST: the verdict must BE the guard ask
    // before any click is meaningful.
    let tc = ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
    };
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Ask),
        "precondition: the write is the row-3 guard ask"
    );

    let pending = loop_.pending_permissions.clone();
    let recorded = sink_events.clone();
    let answer = tokio::spawn(answer_with_recorded_directory(
        pending,
        recorded,
        "s1",
        "allow-directory",
    ));
    bounded_prompt(&mut loop_, "go").await;
    let clicked = answer.await.unwrap();

    // The clicked value is the EMITTED path (read from the recorded request, so
    // this is a tautology by construction — which is the point: nothing here
    // fabricates the value the way a hand-written `Some("…")` would) and it is a
    // REAL path, not T02's `""` placeholder.
    assert!(
        !clicked.is_empty(),
        "the emitted `allow-directory` path must be the real ancestor, not the \
         `\"\"` placeholder"
    );
    let prompts = permission_requests(&sink_events.lock().unwrap());
    assert_eq!(prompts.len(), 1, "exactly one prompt: {prompts:?}");
    assert_eq!(
        prompts[0]["request"]["options"][1]["path"]
            .as_str()
            .unwrap(),
        clicked,
        "the click carried the emitted value verbatim"
    );

    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(
        !is_error,
        "the `allow-directory` click must RUN the tool (an un-extended approve \
         predicate would answer `permission denied`): {result:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&target).unwrap_or_default(),
        "PWNED",
        "the write LANDED"
    );
}

/// The `value` is T04's business (the Supervisor persists it); the PREDICATE
/// must approve the id regardless of it. The same drive as the loud pin,
/// clicking `allow-directory` with `value: None` — the tool still RUNS.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_guard_allow_directory_runs_the_tool_without_a_value_payload() {
    let (_lock, _restore, home) = pin_guard_home("guard-no-value");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow-directory", None));
    bounded_prompt(&mut loop_, "go").await;
    answer.await.unwrap();
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(
        !is_error,
        "the predicate approves the ID; the payload only matters to T04: {result:?}"
    );
}

/// The TRUSTED-Space VARIANT of the loud pin: the same default-posture
/// protected write emits NO prompt at all and the write LANDS with no click.
/// ("Trust suppresses the guard. A trusted Space's agent can also write its own
/// instruction directories without asking.") It proves the click is not what
/// makes the guard work on a trusted Space.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_guard_allow_directory_runs_the_tool_on_a_trusted_space() {
    let (_lock, _restore, home) = pin_guard_home("guard-trusted-click");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    // Trust BEFORE the first prompt (the loop's `trust` is `pub`).
    trust_the_space(&mut loop_, home.path());

    let ends = {
        bounded_prompt(&mut loop_, "go").await;
        tool_ends(&mut events_rx)
    };
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a trusted Space emits NO permission-request for the guard"
    );
    assert!(
        loop_.pending_permissions.try_lock().unwrap().is_empty(),
        "and registers no oneshot"
    );
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(
        !is_error,
        "the write lands with NO click and NO prompt: {result:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&target).unwrap_or_default(),
        "PWNED",
        "the trusted Space's silent self-modification really wrote the file"
    );
}

/// Trust suppresses the guard ask. The LOAD-BEARING half is the CONTRAST: the
/// same call on the same loop with trust removed IS an ask — otherwise "does
/// not fire" would also describe a guard that never worked.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_guard_ask_does_not_fire_on_a_trusted_space() {
    let (_lock, _restore, home) = pin_guard_home("guard-trust-suppress");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    let tc = ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
    };
    // UNTRUSTED FIRST: the ask WOULD fire (this is what makes the trusted half
    // below load-bearing rather than vacuous).
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Ask),
        "precondition: the same call asks on an untrusted Space"
    );
    trust_the_space(&mut loop_, home.path());
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Allow),
        "Trust suppresses the guard ask at the verdict itself"
    );

    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "no prompt on a trusted Space"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(!is_error, "and the write runs: {result:?}");
}

/// Row 1 is TRUST-IMMUNE: a USER `deny` rule refuses on a TRUSTED Space too,
/// with no prompt (Trust suppresses prompts and never widens a deny — the same
/// precedence rule `decision_for` pins for the posture tiers).
///
/// This is the GATE's row-1 path (`deny_tool_result`'s wording). The EXECUTOR's
/// refusal is a different site and is pinned in `exec/tests/policy.rs`.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_rule_deny_refuses_on_a_trusted_space() {
    let (_lock, _restore, home) = pin_guard_home("rule-deny-trusted");
    std::fs::create_dir_all(home.path().join(".agents/skills")).unwrap();
    let target = home.path().join(".agents/skills/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings_with_rules(
            "allow",
            "allow",
            "allow",
            &deny_rule("~/.agents/skills"),
        )),
    );
    trust_the_space(&mut loop_, home.path());

    // The verdict, read AFTER the first prompt so `self.rules` holds the
    // concatenated list (before it the field is the inert `Vec::new()`).
    let tc = ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
    };
    bounded_prompt(&mut loop_, "go").await;
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Deny),
        "a rule deny is trust-IMMUNE (Trust suppresses prompts, never widens a deny)"
    );
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "row 1 never prompts — not even on a trusted Space"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the refusal reaches the model: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the refusal names the guard-membership reason, got {text:?}"
    );
    assert!(!target.exists(), "nothing was written");
}

/// The guard prompt's END-TO-END SHAPE through the real loop: exactly
/// `allow` / `allow-directory` / `reject` in THAT order, a real non-empty
/// canonical `path` on `allow-directory`, and NO `trust-space` (T02 removed it
/// deliberately — the end-to-end pin keeps it from creeping back).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_guard_prompt_end_to_end_carries_exactly_three_options_a_real_path_and_no_trust_space()
{
    let (_lock, _restore, home) = pin_guard_home("guard-shape");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "reject", None));
    bounded_prompt(&mut loop_, "go").await;
    answer.await.unwrap();

    let prompts = permission_requests(&sink_events.lock().unwrap());
    assert_eq!(prompts.len(), 1, "exactly one prompt: {prompts:?}");
    let options = prompts[0]["request"]["options"]
        .as_array()
        .expect("the prompt carries an options array");
    let ids: Vec<&str> = options
        .iter()
        .map(|o| o["optionId"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec!["allow", "allow-directory", "reject"],
        "the guard list, in order"
    );
    assert!(
        ids.iter().all(|id| *id != "trust-space"),
        "the guard prompt NEVER offers `trust-space`: {ids:?}"
    );
    let emitted = options[1]["path"].as_str().expect("`path` is a string");
    assert!(
        !emitted.is_empty(),
        "the `\"\"` placeholder must be replaced with the real ancestor"
    );
    // It is the CANONICAL nearest-existing ancestor DIRECTORY of the resolved
    // target. The expectation goes through the SAME canonicalizer production
    // uses, NOT strict `canonicalize()`: `pwn` does not exist, so strict
    // canonicalization FAILS on the parent and an `unwrap_or_else` fallback
    // silently hands back the RAW spelling. On Linux `temp_dir()` is already
    // canonical so raw == canonical and that fallback is invisible; on macOS
    // the temp dir is a symlink (`/var` → `/private/var`) and on Windows
    // `canonicalize` rewrites it into the verbatim `\\?\` form, so the raw
    // fallback and the emitted canonical value disagree and this reddens on
    // both platforms while staying green here. `canonicalize_lenient` peels to
    // the deepest EXISTING ancestor and re-appends `pwn`, which is exactly the
    // shape `guard_ancestor` produces from the resolved target.
    let want = crate::agent::fs_backend::canonicalize_lenient(
        target.parent().expect("the target is a file under a dir"),
    );
    assert_eq!(
        std::path::Path::new(emitted),
        want.as_path(),
        "the emitted path is the target's nearest existing ancestor"
    );
    assert!(
        !emitted.contains('~'),
        "a `~` residue would persist a rule `rule_for` can never match: {emitted}"
    );
}

/// The gate-side mirror of T01's
/// `a_rule_naming_a_directory_that_does_not_exist_yet_matches_leniently`: a rule
/// naming a NOT-YET-EXISTING dir matches a write into it, because both sides go
/// through the lenient canonicalizer. A strict `canonicalize()` on the rule path
/// would fail, match nothing, and silently fall through to the guard ask.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_rule_naming_a_not_yet_existing_dir_matches_at_the_gate() {
    // NOTE: `.pi/agent/agents` is deliberately NEVER created.
    let (_lock, _restore, home) = pin_guard_home("gate-lenient-rule");
    let target = home.path().join(".pi/agent/agents/new/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings_with_rules(
            "allow",
            "allow",
            "allow",
            &deny_rule("~/.pi/agent/agents"),
        )),
    );
    let tc = ToolCall {
        id: "t1".to_string(),
        name: "write".to_string(),
        arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
    };
    bounded_prompt(&mut loop_, "go").await;
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Deny),
        "the rule matched a dir that does not exist yet"
    );
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a rule deny is not a prompt (and it beat the guard, which would have \
         asked — so a non-matching rule shows up HERE as a prompt)"
    );
    let ends = tool_ends(&mut events_rx);
    let (_, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the write is refused");
    assert!(!target.exists(), "nothing was written");
}

/// The direction split: a `read` of a file under one of the four dirs with NO
/// matching rule is NOT an ask — the guard is WRITE-only, so the read falls to
/// row 4 and the posture select, which under the default `reads: allow` proceeds
/// silently. The same path via `write` IS the row-3 ask, asserted as the
/// contrast so neither half can be dropped.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_read_of_an_agent_definition_file_is_not_an_ask() {
    let (_lock, _restore, home) = pin_guard_home("read-not-ask");
    let dir = home.path().join(".pi/agent/agents");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), "THE-BODY").unwrap();
    let path = dir.join("SKILL.md").display().to_string();
    let (provider, _calls) = ScriptedProvider::new(read_scripts(&path));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    assert_eq!(
        loop_.policy_verdict(&read_call("t1", &path)),
        Some(Decision::Allow),
        "row 4 for a read: the posture select decides, and `reads: allow` allows"
    );
    // CONTRAST: the SAME path via `write` is the ask — the direction split this
    // pin exists to state.
    assert_eq!(
        loop_.policy_verdict(&ToolCall {
            id: "t1".to_string(),
            name: "write".to_string(),
            arguments: json!({ "path": path.clone(), "content": "X" }),
        }),
        Some(Decision::Ask),
        "the write direction of the same path IS the row-3 ask"
    );

    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a read of an agent-definition file never prompts"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("the read ran").clone();
    assert!(!is_error, "and the read lands: {result:?}");
    assert_eq!(result["content"][0]["text"], "THE-BODY");
}

/// THE SEAM PIN (the `~`-residue hazard): the path an `allow-directory` click
/// carries must be something `rule_for` can MATCH, or the click that is supposed
/// to stop the prompting silently does nothing. Driven end-to-end: the guard
/// emits a path → that path becomes the rule the user persisted → `rule_for`
/// matches the canonicalized target with it, so the next write is row 2 (silent)
/// instead of a repeat prompt.
///
/// WHAT THE PIN CANNOT DRIVE THROUGH THE LOOP, recorded rather than papered
/// over: a `~`-SPELLED `path` ARGUMENT never reaches the guard at all, and that
/// is correct behaviour, not a gap. `FsBackend::resolve` (the executor's binding
/// resolution — the one the gate mirrors) treats `~/x` as a RELATIVE path and
/// joins it to the session `cwd`, so `write {"path": "~/.pi/agent/…"}` creates a
/// literal directory named `~` INSIDE the Space. It is genuinely not under the
/// agent dirs, and a gate that expanded `~` here would resolve the same argument
/// to a DIFFERENT path than the executor writes — the exact gate-vs-executor
/// split-brain ADR 0029/0030's one-canonicalizer rule exists to prevent. The
/// `~` spelling that IS load-bearing is the one in a RULE, which `rule_for`
/// expands against `ToolCtx::home` / `AgentLoop::rules_home`, and that is the
/// second half of this test.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_tilde_spelled_guarded_write_yields_an_ancestor_rule_for_matches() {
    let (_lock, _restore, home) = pin_guard_home("tilde-seam");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    let pending = loop_.pending_permissions.clone();
    let recorded = sink_events.clone();
    let answer = tokio::spawn(answer_with_recorded_directory(
        pending,
        recorded,
        "s1",
        "allow-directory",
    ));
    bounded_prompt(&mut loop_, "go").await;
    let emitted = answer.await.unwrap();

    // HALF ONE: the emitted ancestor carries NO literal `~` (the residue the
    // pre-fix code would have persisted from a raw argument).
    assert!(
        !emitted.contains('~'),
        "the emitted ancestor must be canonical: {emitted}"
    );

    // HALF TWO: feed that emitted path BACK as the rule T04 would persist, and
    // `rule_for` MATCHES the canonicalized target with it — the click stops the
    // next prompt. Spelled with a leading `~` too, via the session's own home,
    // because that is the other spelling a persisted rule can arrive in.
    let canonical_target = crate::agent::fs_backend::canonicalize_lenient(&target);
    let home_canon = home.path().canonicalize().unwrap();
    let as_written = crate::agent::policy::AccessRule {
        path: emitted.clone(),
        direction: crate::agent::policy::RuleDirection::Write,
        decision: crate::agent::policy::RuleDecision::Allow,
    };
    let rules = crate::agent::policy::default_rules()
        .into_iter()
        .chain(std::iter::once(as_written.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        crate::agent::policy::rule_for(
            &rules,
            crate::agent::policy::RuleDirection::Write,
            &canonical_target,
            Some(&home_canon),
        ),
        Some(&as_written),
        "the emitted ancestor is a rule `rule_for` matches against the \
         canonicalized target (the seam the `~` residue broke)"
    );

    // HALF THREE: the `~` RULE spelling resolves too, against the session's
    // home — the spelling a hand-written `settings.json` carries.
    let tilde_rule = crate::agent::policy::AccessRule {
        path: "~/.pi/agent/agents".to_string(),
        direction: crate::agent::policy::RuleDirection::Write,
        decision: crate::agent::policy::RuleDecision::Allow,
    };
    let rules = crate::agent::policy::default_rules()
        .into_iter()
        .chain(std::iter::once(tilde_rule.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        crate::agent::policy::rule_for(
            &rules,
            crate::agent::policy::RuleDirection::Write,
            &canonical_target,
            loop_.rules_home.as_deref(),
        ),
        Some(&tilde_rule),
        "a `~` rule expands against the loop's frozen `rules_home`"
    );
    // And with NO home the same rule matches nothing (the pinned consequence,
    // asserted at the loop's own field so the two halves share one fixture).
    assert_eq!(
        crate::agent::policy::rule_for(
            &rules,
            crate::agent::policy::RuleDirection::Write,
            &canonical_target,
            None,
        ),
        None,
        "`None` home = a `~` rule is a silent no-op"
    );
}

/// Deviation 4 for the RULES layer: a rule added to `settings.json` BETWEEN
/// prompts is enforced at the NEXT prompt, with no restart. This is the
/// per-turn concat's promise (mirror of
/// `a_settings_edit_between_prompts_changes_the_next_turns_gating`, which pins
/// the posture half).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_settings_rule_edit_between_prompts_changes_the_next_turns_gating() {
    let (_lock, _restore, home) = pin_guard_home("rule-per-turn");
    let dir = home.path().join(".agents/skills");
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("pwn/SKILL.md").display().to_string();
    let write = |id: &str| {
        ProviderEvent::ToolCall(ToolCall {
            id: id.to_string(),
            name: "write".to_string(),
            arguments: json!({ "path": target.clone(), "content": "PWNED" }),
        })
    };
    let done = |text: &str| {
        Some(vec![
            ProviderEvent::TextDelta(text.to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])
    };
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            write("t1"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        done("first done"),
        Some(vec![
            write("t2"),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        done("second done"),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        // Turn 1: NO rules → the guard asks.
        Some(&policy_settings("allow", "allow", "allow")),
    );
    let config_dir = loop_.config_dir.clone().expect("a settings dir");

    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "reject", None));
    bounded_prompt(&mut loop_, "one").await;
    answer.await.unwrap();
    assert_eq!(
        permission_requests(&sink_events.lock().unwrap()).len(),
        1,
        "turn 1 prompts (row 3, no rules)"
    );

    // The user edits Settings BETWEEN the prompts: a deny rule appears.
    std::fs::write(
        config_dir.join("settings.json"),
        policy_settings_with_rules("allow", "allow", "allow", &deny_rule("~/.agents/skills")),
    )
    .unwrap();

    bounded_prompt(&mut loop_, "two").await;
    assert_eq!(
        permission_requests(&sink_events.lock().unwrap()).len(),
        1,
        "turn 2 must NOT prompt: the new rule moved the call from row 3 to row 1"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t2").expect("a tool result for t2").clone();
    assert!(is_error, "the new deny rule refuses: {result:?}");
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("protected agent-definition directory"),
        "with the guard-membership wording: {:?}",
        result["content"][0]
    );
}

/// DD1's freeze matrix, pinned at the field it freezes: the per-turn list is
/// `default_rules()` FIRST and the user rules SECOND. The order is the
/// CONCATENATION CONTRACT — `rule_for` breaks an exact tie by list index
/// (last wins), so an inverted concat would make every appended
/// `allow-directory` answer silently no-op. Pinned here (the ORDER of the field
/// the executors receive) because `policy.rs` can only pin the resolver's
/// half of it.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_per_turn_rule_list_is_defaults_first_then_the_user_rules() {
    let (_lock, _restore, _home) = pin_guard_home("concat-order");
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("done".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings_with_rules(
            "allow",
            "allow",
            "allow",
            &allow_rule("~/.agents/skills"),
        )),
    );
    // Before the FIRST prompt the concat has not run: the field is the inert
    // `Vec::new()` (the documented initial value — there is no tool call before
    // a prompt, so it cannot decide anything).
    assert!(
        loop_.rules.clone().is_empty(),
        "the pre-first-turn rule list is EMPTY by design"
    );
    assert!(
        loop_.default_rules.clone().is_empty(),
        "the default half is EMPTY (the round-3 correction)"
    );
    assert!(
        loop_.rules_home.is_some(),
        "the `~`-expansion home is captured once at construction"
    );

    bounded_prompt(&mut loop_, "go").await;
    let after = loop_.rules.clone();
    // The defaults are empty today, so the concat's CONTENT is the user rules —
    // but the assertion that earns its place is the ORDER: every default sits
    // before every user rule, which is what the contract means and what stays
    // true when the default half is no longer empty.
    let defaults = loop_.default_rules.clone();
    assert_eq!(
        after.len(),
        defaults.len() + 1,
        "one user rule appended after the defaults: {after:?}"
    );
    assert_eq!(
        &after[..defaults.len()],
        defaults.as_slice(),
        "defaults FIRST"
    );
    assert_eq!(
        after[defaults.len()].path,
        "~/.agents/skills",
        "the user rule is SECOND"
    );
}

/// Acceptance 9 at the GATE layer: a rule `deny` on a NON-protected dir refuses
/// with the rule-deny sentence, NOT with "escapes the session sandbox".
///
/// Why this is a GATE pin and not only an executor one: `deny_tool_result`'s
/// founding claim is that a gate denial and an executor denial read IDENTICALLY
/// to the model, so its recovery behaviour does not depend on which layer said
/// no. Before this change the gate had only two arms (guard wording / boundary
/// escape), so a rule deny on an ordinary dir reached the escape arm and told
/// the model about a boundary it never crossed — the drift that design exists to
/// prevent, and the reason the gate needed a third arm.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_rule_deny_on_an_ordinary_dir_uses_the_rule_wording_not_an_escape_claim() {
    let (_lock, _restore, home) = pin_guard_home("rule-deny-wording-gate");
    // The target is OUTSIDE every guard dir, so the guard wording must NOT
    // appear, and inside… let's put it in the loop's own Space so the boundary
    // would not refuse it either.
    let (provider, _calls) = {
        // The `space_cwd` is minted by the fixture, so the path is built AFTER
        // the loop exists — the scripted provider only needs the id/name, and
        // the arguments can point at a path the test creates first.
        ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::ToolCall(ToolCall {
                    id: "t1".to_string(),
                    name: "write".to_string(),
                    // A placeholder replaced below is not possible (the script
                    // is built before the loop), so the fixture uses a path the
                    // test creates under the pinned `$HOME` — which is neither
                    // the `cwd` nor a guard dir.
                    arguments: json!({
                        "path": format!("{}/secrets/hidden.txt", home.path().display()),
                        "content": "PWNED",
                    }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ])
    };
    std::fs::create_dir_all(home.path().join("secrets")).unwrap();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings_with_rules(
            "allow",
            // `sandboxed` on purpose: the boundary WOULD refuse this path (it is
            // outside the `cwd`), so the wording below can only come from the
            // rule arm — the escape arm is the wrong answer being ruled out.
            "sandboxed",
            "allow",
            &deny_rule("~/secrets"),
        )),
    );
    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a rule deny never prompts"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the write is refused: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("denied by the access rule"),
        "the refusal names the RULE the user wrote: {text}"
    );
    assert!(
        !text.contains("escapes the session sandbox"),
        "a path the user explicitly blocked must not be reported as a boundary \
         escape (the model would go looking for a boundary that is not the \
         problem): {text}"
    );
    assert!(
        !text.contains("protected agent-definition directory"),
        "and it is not a guard dir either: {text}"
    );
}

// ── (issue #101 task05b) the per-agent `permission:` layer in the gate ─────
//
// The block is the DEFINITION's declared ruleset (the file's `permission:`
// frontmatter, carried by `LaunchConfig.permission` → `StartEnv.permission` →
// `AgentLoop::set_permission`). Three properties every test below pins:
//
//   * NARROWING ONLY — `Allow` is the identity of the merge, so a definition can
//     never widen a posture `Deny`/`Confine`, a row-1 rule `Deny` or the row-3
//     guard `Ask` (ADR 0030's "trust suppresses prompts but never widens a deny"
//     applied to definitions);
//   * `Confine` is NOT a midpoint — it is a RUNNING sandbox, ranked
//     `Allow < Confine < Ask < Deny`, so a per-agent `Allow` cannot un-confine a
//     `Sandboxed` shell and a per-agent `Ask` still tightens one;
//   * a per-agent `Deny` renders through `deny_tool_result` UNCHANGED, so the
//     model's recovery cannot tell which layer said no.
use crate::agent::definition_permission::{Action, PermissionBlock, Rule, Rules, ToolKey};

/// A one-tool whole-tool ruleset (the bare `read: deny` spelling — the shape
/// the gate can decide WITHOUT any pattern matcher).
fn whole_tool_block(tool: ToolKey, action: Action) -> PermissionBlock {
    PermissionBlock {
        tools: vec![(tool, Rules::whole_tool(action))],
    }
}

/// A `bash` tool call (the shell direction has no `path`).
fn bash_call(id: &str, command: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        name: "bash".to_string(),
        arguments: json!({ "command": command }),
    }
}

/// A `write` tool call.
fn write_call(id: &str, path: &str) -> ToolCall {
    ToolCall {
        id: id.to_string(),
        name: "write".to_string(),
        arguments: json!({ "path": path, "content": "PWNED" }),
    }
}

/// (issue #101) A definition `read: ask` on a file whose `filePolicy` is all
/// `Allow`: the definition alone creates the prompt. Without the block the very
/// same access is silent (the widening ADR 0030 intends), so the prompt below
/// can only come from the definition.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_narrowing_an_allow_to_an_ask_prompts() {
    let (_lock, _restore, _home) = pin_guard_home("def-ask");
    let outside = Tmp::new("def-ask-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt");
    let (provider, _calls) = ScriptedProvider::new(read_scripts(&path.display().to_string()));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        // The user's posture WOULD allow this read.
        Some(&policy_settings("allow", "allow", "allow")),
    );
    loop_.set_permission(Some(whole_tool_block(ToolKey::Read, Action::Ask)));
    let pending = loop_.pending_permissions.clone();
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow", None));
    bounded_prompt(&mut loop_, "go").await;
    answer.await.unwrap();

    let tc = read_call("t1", &path.display().to_string());
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Ask),
        "the definition narrowed the posture `Allow` to an `Ask`"
    );
    let prompts = permission_requests(&sink_events.lock().unwrap());
    assert_eq!(
        prompts.len(),
        1,
        "the definition is what made the prompt exist: {prompts:?}"
    );
    assert_eq!(prompts[0]["requestId"], "t1");
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
    assert!(!is_error, "the answered read runs: {result:?}");
    assert_eq!(result["content"][0]["text"], "OUTSIDE-CONTENT");
}

/// (issue #101) A definition `write: deny` refuses with the EXISTING
/// `deny_tool_result` wording — the protected-agent-directory sentence, because
/// the target is under `$HOME/.pi/agent/agents`. No new renderer, no new
/// sentence: a definition denial and a user-rule denial are byte-identical to
/// the model (ADR 0030).
///
/// WHY THE SPACE IS TRUSTED HERE (and untrusted in the guard pins): the row-3
/// guard `Ask` returns EARLY, so on an untrusted Space a no-rule protected write
/// is the guard's prompt and the per-agent layer is never consulted (pinned by
/// [`the_row_precedence_cells_are_untouched_by_the_definition_layer`]). Trust
/// suppresses the guard, which is what lets the verdict fall to row 4 — where
/// ONLY the definition refuses. So the refusal below cannot come from the guard
/// or the posture (both allow); it can only come from the block.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_deny_refuses_with_the_rule_or_protected_wording() {
    let (_lock, _restore, home) = pin_guard_home("def-deny-protected");
    let target = home.path().join(".pi/agent/agents/pwn/SKILL.md");
    let (provider, _calls) =
        ScriptedProvider::new(write_scripts(&target.display().to_string(), "PWNED"));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    trust_the_space(&mut loop_, home.path());
    loop_.set_permission(Some(whole_tool_block(ToolKey::Write, Action::Deny)));
    bounded_prompt(&mut loop_, "go").await;

    let tc = write_call("t1", &target.display().to_string());
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Deny),
        "the definition's `deny` beats the posture's `Allow` (a Deny never prompts)"
    );
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a Deny emits no permission-request"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the denied write is refused: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the refusal is the EXISTING protected-dir sentence, not an invented one: {text:?}"
    );
    assert!(
        !target.exists(),
        "and nothing was written (the gate stopped it before the executor)"
    );
}

/// (issue #101) The `Allow` identity, posture half: `reads: sandboxed` denies an
/// out-of-boundary `read` and a definition `read: allow` cannot widen it back.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_cannot_widen_a_user_deny() {
    let (_lock, _restore, _home) = pin_guard_home("def-no-widen");
    let outside = Tmp::new("def-no-widen-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt");
    let (provider, _calls) = ScriptedProvider::new(read_scripts(&path.display().to_string()));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("sandboxed", "sandboxed", "sandboxed")),
    );
    loop_.set_permission(Some(whole_tool_block(ToolKey::Read, Action::Allow)));
    bounded_prompt(&mut loop_, "go").await;

    assert_eq!(
        loop_.policy_verdict(&read_call("t1", &path.display().to_string())),
        Some(Decision::Deny),
        "the posture `Deny` beats the definition's `Allow` — the layer narrows, never widens"
    );
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "and no prompt appears on the way"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the read stays refused: {result:?}");
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("escapes the session sandbox"),
        "the refusal is the boundary sentence the executor would produce: {text:?}"
    );
}

/// (issue #101) The precedence rows 1–3 are UNTOUCHED by the per-agent layer —
/// both directions of the non-bypass claim, in one place.
///
/// * row 1 (a user `deny` rule) + a definition `write: allow` → still `Deny`;
/// * row 3 (the protected-dir guard) + a definition `write: allow` → still `Ask`
///   (a definition can never silence the guard — the widening the whole design
///   forbids);
/// * row 3 + a definition `write: deny` → still `Ask`. The guard returns EARLY,
///   so the per-agent layer (row 4) never sees it: a definition's `deny` cannot
///   turn the guard's prompt into a silent refusal either. RECORDED, not silent:
///   the user still approves that write by hand, so nothing is granted that the
///   guard would have stopped;
/// * row 2 (a user `allow` rule) + a definition `write: deny` → `Allow`. The
///   user's own explicit rule is the WIDER layer and rows 1–2 return before the
///   per-agent consult. This is the layer's honest boundary: the definition sits
///   BELOW the user's rule list, above the posture.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_row_precedence_cells_are_untouched_by_the_definition_layer() {
    let (_lock, _restore, home) = pin_guard_home("def-rows");
    std::fs::create_dir_all(home.path().join(".agents/skills")).unwrap();
    let ordinary = home.path().join("work");
    std::fs::create_dir_all(&ordinary).unwrap();
    let protected_target = home.path().join(".pi/agent/agents/pwn/SKILL.md");

    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("done".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings_with_rules(
            "allow",
            "allow",
            "allow",
            &deny_rule("~/.agents/skills"),
        )),
    );
    // ONE turn first: `rules` / `file_policy` are freshened at the top of
    // `handle_prompt`, so a verdict probed before any prompt would read the
    // construction defaults and the rule below would not be in the list at all
    // (the existing verdict-pin pattern). The turn is TEXT-ONLY so no tool is
    // gated while the verdicts are probed by hand.
    bounded_prompt(&mut loop_, "go").await;

    // Row 1: a USER `deny` rule vs a definition `allow`.
    let rule_target = home.path().join(".agents/skills/pwn/SKILL.md");
    loop_.set_permission(Some(whole_tool_block(ToolKey::Write, Action::Allow)));
    assert_eq!(
        loop_.policy_verdict(&write_call("t1", &rule_target.display().to_string())),
        Some(Decision::Deny),
        "a definition `allow` cannot bypass a row-1 user `deny`"
    );

    // Row 3: the guard vs a definition `allow` (untrusted — the guard's home).
    assert_eq!(
        loop_.policy_verdict(&write_call("t2", &protected_target.display().to_string())),
        Some(Decision::Ask),
        "a definition `allow` cannot bypass the row-3 protected-dir guard"
    );
    // Row 3, the OTHER direction: a definition `deny` cannot rewrite the guard's
    // prompt into a silent refusal either (row 4 is never consulted).
    loop_.set_permission(Some(whole_tool_block(ToolKey::Write, Action::Deny)));
    assert_eq!(
        loop_.policy_verdict(&write_call("t3", &protected_target.display().to_string())),
        Some(Decision::Ask),
        "the guard's `Ask` survives a definition `deny` — the layer enters at row 4"
    );

    // Row 2: a USER `allow` rule vs a definition `deny` (the recorded boundary).
    let allow_rule_dir = ordinary.join("allowed");
    std::fs::create_dir_all(&allow_rule_dir).unwrap();
    let loop2_settings =
        policy_settings_with_rules("allow", "allow", "allow", &allow_rule("~/.agents/skills"));
    let (provider2, _c2) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("done".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (events_tx2, _rx2) = mpsc::unbounded_channel();
    let (_space2, mut loop2, _cfg2) = build_policy_loop(
        Box::new(provider2),
        events_tx2,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&loop2_settings),
    );
    loop2.set_permission(Some(whole_tool_block(ToolKey::Write, Action::Deny)));
    // The same turn-first requirement as `loop_` above: the rule list is
    // freshened at the top of a prompt, and this turn is text-only.
    bounded_prompt(&mut loop2, "go").await;
    assert_eq!(
        loop2.policy_verdict(&write_call("t4", &rule_target.display().to_string())),
        Some(Decision::Allow),
        "a row-2 user `allow` rule is the WIDER layer and returns before row 4 \
         (the per-agent layer sits below the user's rule list)"
    );
}

/// (issue #101) **`Confine` is not a midpoint.** `shell: sandboxed` + a
/// definition `bash: allow` → `Confine`, not `Allow`: a definition cannot
/// un-confine a `Sandboxed` shell policy. This is the pin a naive `min` over the
/// three per-agent actions would fail (it ranks `Allow` below everything and
/// would let the `allow` win by being "less restrictive than `Ask`").
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_cannot_unconfine_a_sandboxed_shell() {
    let (_lock, _restore, _home) = pin_guard_home("def-confine");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(bash_call("t1", "true")),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "sandboxed")),
    );
    loop_.set_permission(Some(whole_tool_block(ToolKey::Bash, Action::Allow)));
    bounded_prompt(&mut loop_, "go").await;

    assert_eq!(
        loop_.policy_verdict(&bash_call("t1", "true")),
        Some(Decision::Confine),
        "`Allow` is the identity of `Allow < Confine < Ask < Deny` — it cannot un-confine"
    );
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "and confining asks nothing"
    );
}

/// (issue #101) The other half of the ordering: `Confine` ranks BELOW `Ask`, so a
/// definition `bash: ask` tightens a confined shell into a prompt (a per-agent
/// `Ask` beats the posture's `Confine`).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_ask_tightens_a_confined_shell() {
    let (_lock, _restore, _home) = pin_guard_home("def-confine-ask");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(bash_call("t1", "true")),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "sandboxed")),
    );
    loop_.set_permission(Some(whole_tool_block(ToolKey::Bash, Action::Ask)));
    let pending = loop_.pending_permissions.clone();
    // The turn is answered (with a `reject`, so nothing runs): the tightening is
    // asserted BOTH on the verdict and on the prompt the turn really raised.
    let answer = tokio::spawn(answer_permission(pending, "s1", "reject", None));
    bounded_prompt(&mut loop_, "go").await;
    answer.await.unwrap();
    assert_eq!(
        loop_.policy_verdict(&bash_call("t1", "true")),
        Some(Decision::Ask),
        "`Ask` outranks `Confine` — a confined shell can be tightened to a prompt"
    );
    assert_eq!(
        permission_requests(&sink_events.lock().unwrap()).len(),
        1,
        "and the tightening is LIVE: the turn raised the prompt the sandbox alone never would"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(is_error, "the rejected command did not run: {result:?}");
}

/// (issue #101) A definition `ask`/`deny` still tightens a TRUST-widened
/// `Allow`: Trust suppresses the posture's prompt, but it is not a grant — the
/// definition's `read: ask` produces the `Ask` again on the trusted Space.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_ask_tightens_a_trust_widened_allow() {
    let (_lock, _restore, home) = pin_guard_home("def-trust");
    let outside = Tmp::new("def-trust-outside");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE-CONTENT").unwrap();
    let path = outside.path().join("secret.txt");
    let (provider, _calls) = ScriptedProvider::new(read_scripts(&path.display().to_string()));
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    trust_the_space(&mut loop_, home.path());
    let tc = read_call("t1", &path.display().to_string());
    // The contrast that keeps the assertion below from passing vacuously: with
    // NO block the trusted Space runs this read silently.
    assert_eq!(loop_.policy_verdict(&tc), Some(Decision::Allow));
    loop_.set_permission(Some(whole_tool_block(ToolKey::Read, Action::Ask)));
    bounded_prompt(&mut loop_, "go").await;
    assert_eq!(
        loop_.policy_verdict(&tc),
        Some(Decision::Ask),
        "the definition's `Ask` beats the `Allow` Trust produced (Trust suppresses \
         prompts, it does not grant past a definition)"
    );
    // The trusted gate auto-approves the `Ask` (ADR 0010) — no prompt, and the
    // read still lands, so the tightening is a DECISION and not a stall.
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a trusted Space auto-approves the `Ask` (no prompt event)"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("the read dispatched").clone();
    assert!(!is_error, "and the read runs: {result:?}");
}

/// (issue #101, THE motivating shape) A definition with NO `tools:` key and a
/// whole-tool `bash: deny`: the advertisement filter has no list to narrow, so
/// `bash` is still OFFERED, and the GATE is the only layer that can refuse it.
/// The refusal is a tool-result error in the shell's own words, with no prompt.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_whole_tool_bash_deny_refuses_at_the_gate_when_the_tool_is_still_offered() {
    let (_lock, _restore, _home) = pin_guard_home("def-bash-deny");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(bash_call("t1", "touch should-not-exist.txt")),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    // `enabled_tools: None` (the fixture's default) — the ABSENT-list leg: the
    // tool is advertised, the gate is the only refusal available.
    assert_eq!(loop_.enabled_tools(), None, "nothing was filtered out");
    loop_.set_permission(Some(whole_tool_block(ToolKey::Bash, Action::Deny)));
    bounded_prompt(&mut loop_, "go").await;

    assert_eq!(
        loop_.policy_verdict(&bash_call("t1", "true")),
        Some(Decision::Deny),
        "the definition denies the shell even at an all-`Allow` posture"
    );
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a deny never prompts"
    );
    let ends = tool_ends(&mut events_rx);
    let (result, is_error) = ends.get("t1").expect("a tool result").clone();
    assert!(
        is_error,
        "the still-offered bash call is refused: {result:?}"
    );
    let text = result["content"][0]["text"].as_str().unwrap().to_string();
    assert!(
        text.contains("refused by the file-access policy"),
        "the EXISTING shell refusal wording, unchanged: {text:?}"
    );
    assert!(
        !_space.path().join("should-not-exist.txt").exists(),
        "and the command never ran"
    );
}

/// (issue #101, the NAMED LIMITATION, pinned so it cannot regress into a panic
/// or a silent drop) A recognized-but-un-policed tool key (`web_search`,
/// `read_skill`, `subagent`, …) has no `policed_direction` twin: the gate
/// early-returns `None` for it, so a `deny` there parses + announces (B1) and is
/// NOT enforced here. `None` — not a panic, not a `Deny`, not a widened `Allow`.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_definition_ask_on_an_unpoliced_tool_stays_as_self() {
    let (_lock, _restore, _home) = pin_guard_home("def-unpoliced");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(read_call("t1", "inside-the-boundary.txt")),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("sandboxed", "sandboxed", "sandboxed")),
    );
    loop_.set_permission(Some(PermissionBlock {
        tools: vec![
            (ToolKey::WebSearch, Rules::whole_tool(Action::Deny)),
            (ToolKey::ReadSkill, Rules::whole_tool(Action::Ask)),
            // An UNRECOGNISED key (opencode's `task`) — retained, and equally
            // unenforced (it has no advertised twin to police).
            (
                ToolKey::Unrecognized("task".to_string()),
                Rules::whole_tool(Action::Deny),
            ),
        ],
    }));
    bounded_prompt(&mut loop_, "go").await;

    for name in ["web_search", "read_skill", "task"] {
        let tc = ToolCall {
            id: "t1".to_string(),
            name: name.to_string(),
            arguments: json!({}),
        };
        assert_eq!(
            loop_.policy_verdict(&tc),
            None,
            "`{name}` has no policed twin — the gate stays out of it (no panic, no verdict)"
        );
    }
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "and nothing prompts on the way"
    );
}

/// (issue #101) A per-pattern ruleset (`bash: {"*": allow, "rm *": deny}`) has
/// NO tool-wide verdict — the matcher that could read those rows does not exist
/// (B1 announces it unsupported) — so it is `AsSelf`: the posture decides,
/// unchanged. A pattern row is never silently read as a whole-tool verdict.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn per_pattern_rules_leave_the_verdict_untouched() {
    let (_lock, _restore, _home) = pin_guard_home("def-patterns");
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(bash_call("t1", "true")),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let sink_events = Arc::new(StdMutex::new(Vec::<(String, Value)>::new()));
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: sink_events.clone(),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    loop_.set_permission(Some(PermissionBlock {
        tools: vec![(
            ToolKey::Bash,
            Rules::per_tool(vec![
                Rule {
                    pattern: "*".to_string(),
                    action: Action::Allow,
                },
                Rule {
                    pattern: "rm *".to_string(),
                    action: Action::Deny,
                },
            ]),
        )],
    }));
    bounded_prompt(&mut loop_, "go").await;
    assert_eq!(
        loop_.policy_verdict(&bash_call("t1", "true")),
        Some(Decision::Allow),
        "a per-pattern block decides nothing (no matcher) — the posture stands"
    );
}

/// (issue #101, THE evidence file) The REAL `~/.agents/agents/researcher.md`
/// block — verbatim in shape, including its comments and its `"*": allow` bash
/// catch-all — run through the REAL gate. This is the file issue #101's Evidence
/// section names, and it declares NO `tools:` key (`grep -c '^tools:'` = 0), so
/// the advertisement filter cannot reach it and the gate is the ONLY layer that
/// could act on it. What the gate actually does with it is pinned here, in both
/// directions, so nobody has to re-derive it:
///
/// * `task: deny` is a WHOLE-TOOL deny and reaches no verdict — `task` has no
///   policed `policed_direction` twin (the named limitation);
/// * `edit:` and `bash:` are PATTERN objects (`whole_tool: false`), so they carry
///   NO tool-wide verdict and the posture stands. Enforcing those rows needs a
///   command/path matcher nothing has validated — B1 parses them and announces
///   `UnsupportedUnvalidatedMatcher(BashPatternRules | PathPatternRules)` for
///   exactly this reason (the bash rows and the `edit:` path rows each announce
///   their own class — #124), and this task's scope line keeps that decision (the
///   redirect trap:
///   `grep x file 2>/dev/null` contains a `>`). Note the file's own bash catch-all
///   is `"*": allow`, so even a perfect matcher would ALLOW a read-only command.
///
/// So this file's block is NOT enforced by the gate, and saying so IS the answer
/// to "is #101 fixed for its own cited evidence?" — the ENFORCEABLE whole-tool
/// class is (see the tests above and the real-worker e2e), the per-command bash
/// class is announced-unsupported and needs its own matcher task. The pin below
/// is what keeps that from ever being described as a silent drop by accident:
/// every row of the real block is read, and the gate's `AsSelf` is a DECISION
/// recorded here, not an oversight.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn the_real_researcher_block_is_read_and_its_verdicts_are_the_scoped_ones() {
    use crate::agent::definition_permission::{FrontmatterNote, UnsupportedReason};

    let fm = "name: researcher\nmode: subagent\npermission:\n  \
              # Markdown only. In opencode `edit` gates write / edit / patch, patterns match the\n  \
              # worktree-relative path, and the LAST matching rule wins — so the catch-all goes first.\n  \
              edit:\n    \"*\": deny\n    \"*.md\": allow\n    \"*.mdx\": allow\n  \
              task: deny\n  \
              # bash is a write hole (`echo x > src/app.ts`, `sed -i`, `tee`). Deny the mutating\n  \
              # surface, then re-allow the fd-only redirects researchers lean on (`2>/dev/null`,\n  \
              # `2>&1`, `> /dev/null`) — those rules are last because they win by order.\n  \
              # Known gap: a file redirect that also carries an fd suffix (`cat x > f.ts 2>/dev/null`)\n  \
              # re-matches an allow rule. The prompt contract below is the backstop for that.\n  \
              bash:\n    \"*\": allow\n    \"rm *\": deny\n    \"2>/dev/null\": allow\n";
    let lines: Vec<&str> = fm.lines().collect();
    let idx = lines
        .iter()
        .position(|l| l.starts_with("permission:"))
        .unwrap();
    let (block, notes) =
        crate::agent::definition_permission::parse_permission_block(&lines, idx, "");
    let block = block.expect("the file that motivated #101 must parse");
    // The block reader's own announcement is the record that the bash rows are
    // UNSUPPORTED — the gate's `AsSelf` below is that decision, not a drop.
    assert!(
        notes.contains(&FrontmatterNote::UnsupportedUnvalidatedMatcher(
            UnsupportedReason::BashPatternRules
        )),
        "the real block announces its bash pattern rows: {notes:?}"
    );
    // #124: the SAME file's `edit:` map is a per-pattern PATH map, and it now
    // announces its OWN reason. Both reasons are asserted by name so this
    // gate-adjacent record keeps naming the FULL announcement set the real file
    // earns — a regression to bash-only silence for the path rows reddens here.
    assert!(
        notes.contains(&FrontmatterNote::UnsupportedUnvalidatedMatcher(
            UnsupportedReason::PathPatternRules
        )),
        "the real block announces its edit path-pattern rows: {notes:?}"
    );
    assert!(
        block
            .tools
            .iter()
            .any(|(k, _)| *k == ToolKey::Unrecognized("task".into())),
        "`task` is retained, not dropped: {:?}",
        block.tools.iter().map(|(k, _)| k).collect::<Vec<_>>()
    );
    // The bash ruleset is a pattern object with the file's own `allow` catch-all.
    let bash = block
        .tools
        .iter()
        .find(|(k, _)| *k == ToolKey::Bash)
        .map(|(_, r)| r)
        .expect("the file speaks about bash");
    assert!(!bash.whole_tool, "the real bash block is a pattern object");
    assert_eq!(
        bash.patterns.first(),
        Some(&Rule {
            pattern: "*".to_string(),
            action: Action::Allow
        }),
        "the file's own catch-all ALLOWS the shell"
    );

    // …and the GATE's answer for that block, on an all-`Allow` posture.
    let (_lock, _restore, _home) = pin_guard_home("researcher-real");
    let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
        ProviderEvent::TextDelta("done".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]);
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (_space, mut loop_, _cfg) = build_policy_loop(
        Box::new(provider),
        events_tx,
        Arc::new(RecordingSink {
            events: Arc::new(StdMutex::new(Vec::new())),
        }),
        Some(&policy_settings("allow", "allow", "allow")),
    );
    loop_.set_permission(Some(block));
    bounded_prompt(&mut loop_, "go").await;
    // The POLICED tools, whose pattern rows decide nothing → the posture stands.
    for (name, args) in [
        ("bash", json!({ "command": "grep x file 2>/dev/null" })),
        ("bash", json!({ "command": "rm -rf /tmp/scratch" })),
        ("edit", json!({ "path": "src/app.ts" })),
    ] {
        let tc = ToolCall {
            id: "t1".to_string(),
            name: name.to_string(),
            arguments: args,
        };
        assert_eq!(
            loop_.policy_verdict(&tc),
            Some(Decision::Allow),
            "`{name}` under the real researcher block: a pattern object carries no \
             tool-wide verdict, so the posture (`Allow`) stands — announced \
             unsupported, never silently half-enforced"
        );
    }
    // `task` is UN-POLICED: no verdict at all (not an `Allow`, not a `Deny`).
    assert_eq!(
        loop_.policy_verdict(&ToolCall {
            id: "t2".to_string(),
            name: "task".to_string(),
            arguments: json!({ "task": "x" }),
        }),
        None,
        "`task: deny` has no policed twin, so the gate answers `None` (the named \
         limitation) rather than inventing a verdict"
    );
}

/// #124 — minimum bar: if the bash-only guard ever comes back, this test
/// REDDENS. The modal reads the SAME note data this reader produces, so a
/// silent parse here is a silently silent modal there — the exact defect
/// class #101 exists to fix.
#[test]
fn a_path_tool_pattern_map_announces_itself_unsupported() {
    use crate::agent::definition_permission::{FrontmatterNote, UnsupportedReason};

    let fm = "name: p\npermission:\n  edit: {\"*\": deny, \"*.md\": allow}\n";
    let lines: Vec<&str> = fm.lines().collect();
    let idx = lines
        .iter()
        .position(|l| l.starts_with("permission:"))
        .unwrap();
    let (block, notes) =
        crate::agent::definition_permission::parse_permission_block(&lines, idx, "");
    let block = block.expect("a pattern map on a path tool must PARSE");
    assert!(!block.tools[0].1.whole_tool, "an object is per-pattern");
    assert!(
        notes.contains(&FrontmatterNote::UnsupportedUnvalidatedMatcher(
            UnsupportedReason::PathPatternRules
        )),
        "a path-tool pattern map ANNOUNCES itself unsupported: {notes:?}"
    );
    assert!(
        notes.iter().all(|n| !matches!(
            n,
            FrontmatterNote::UnsupportedUnvalidatedMatcher(UnsupportedReason::BashPatternRules)
        )),
        "the path-tool announcement is its OWN reason, never bash's: {notes:?}"
    );
}

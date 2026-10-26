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
/// `filePolicy` — the same one the Settings UI writes).
fn policy_settings(reads: &str, writes: &str, shell: &str) -> String {
    format!(
        r#"{{ "filePolicy": {{ "reads": "{reads}", "writes": "{writes}", "shell": "{shell}" }} }}"#
    )
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
/// `pending` map being a `pub` field). Spawned BEFORE the prompt so a
/// gate that must NOT prompt can never hang the test.
async fn answer_permission(pending: PendingPermissions, session: &str, option: &str) {
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
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow"));
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
    let answer = tokio::spawn(answer_permission(pending, "s1", "allow"));
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
    // `a_gate_denial_of_a_protected_dir_uses_the_protected_wording` records
    // for its `link` dir: the thing the post-block assertions read must
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

/// (ADR 0030 Deviation 3) a write into a user-level agent-definition dir
/// is refused by the GATE in the deny-list's own words (the same text the
/// executor produces), even though `writes: Sandboxed` would otherwise
/// read as an escape — the deny-list is the true reason there.
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
async fn a_gate_denial_of_a_protected_dir_uses_the_protected_wording() {
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
    let (provider, _calls) = ScriptedProvider::new(vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "write".to_string(),
                arguments: json!({ "path": target.display().to_string(), "content": "PWNED" }),
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
        // `writes: allow` on purpose: the deny-list is a FLOOR — it
        // refuses whatever the policy says (the executor owns that
        // case; here the gate never sees it, so `sandboxed` is used to
        // exercise the GATE's wording).
        Some(&policy_settings("allow", "sandboxed", "allow")),
    );
    bounded_prompt(&mut loop_, "go").await;
    assert!(
        permission_requests(&sink_events.lock().unwrap()).is_empty(),
        "a protected write is a Deny, never a prompt"
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

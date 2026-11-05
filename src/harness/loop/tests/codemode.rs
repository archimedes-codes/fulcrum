//! Task C2's harness tests: the `codemode` tool through `AgentLoop`.
//!
//! Three pins, in the order the plan freezes them:
//! 1. the `dispatch_tool` arm routes a `codemode` call to the bridge, and a
//!    non-advertised TOP-LEVEL key is rejected by `unknown_param_error` before any
//!    script runs;
//! 2. a batch of nested calls runs CONCURRENTLY (the `Promise.all` property) and
//!    lands one `details.calls` row per call in COMPLETION order, with live frames
//!    keyed on the PARENT id;
//! 3. a nested call passes the SAME policy gate as a direct call, and a denial is
//!    byte-identical to a direct denial.
//!
//! Every test here runs a REAL QuickJS engine — the bridge is not mocked. The
//! scripted provider only stands in for the model.
//!
//! THE FIXTURE RULE these tests all obey (`policy_gate.rs`'s own discovery, stated
//! at its `policy_verdict` assertions): `PermissionContext::refresh` runs at the top
//! of `handle_prompt`, so a loop that has never handled a prompt still carries the
//! all-`Allow` `file_policy` default no matter what its `settings.json` says. A
//! policy pin therefore drives ONE throwaway prompt first — otherwise a `Deny`
//! expectation would pass vacuously on an `Allow`.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::super::*;
use super::policy_gate::{answer_permission, build_policy_loop};
use super::support::*;
use crate::events::RpcEvent;
use crate::harness::provider::{FinishReason, ProviderEvent, ToolCall};
use crate::harness::retry::RetryPolicy;
use crate::tools::ContentBlock;

/// The text of a [`ToolResult`]'s first block (the `result_text` helper is
/// test-only and lives beside the production dispatcher; this is the same read).
fn text(result: &ToolResult) -> String {
    result
        .content
        .first()
        .map(|b| match b {
            ContentBlock::Text { text } => text.clone(),
            ContentBlock::Image { .. } => String::new(),
        })
        .unwrap_or_default()
}

/// A sink that records EVERY emission (the `TestSink` sees only `session-update`,
/// and the `permission-request` a gate emits rides a different event name).
fn recording_sink() -> Arc<AllEventsSink> {
    Arc::new(AllEventsSink {
        events: Arc::new(StdMutex::new(Vec::new())),
    })
}

/// The recorded payloads for one event name.
fn emitted(sink: &Arc<AllEventsSink>, name: &str) -> Vec<Value> {
    sink.events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(kind, _)| kind == name)
        .map(|(_, payload)| payload.clone())
        .collect()
}

/// The `session-update` frames the sink saw (the live `tool_call_update` frames).
fn frames(sink: &Arc<AllEventsSink>) -> Vec<Value> {
    emitted(sink, "session-update")
        .into_iter()
        .map(|payload| payload["update"].clone())
        .collect()
}

/// A plain terminal turn (throwaway prompt: its only job is to make
/// `PermissionContext::refresh` run, so the settings-driven policy is in force).
fn terminal_scripts() -> Vec<Option<Vec<ProviderEvent>>> {
    vec![Some(vec![
        ProviderEvent::TextDelta("ok".to_string()),
        ProviderEvent::Done(FinishReason::Stop),
    ])]
}

/// The `codemode` turn's script: turn 1 asks for the tool, turn 2 is terminal
/// (`ScriptedProvider` repeats its LAST entry, so the terminal entry is last).
fn codemode_scripts(code: &str) -> Vec<Option<Vec<ProviderEvent>>> {
    vec![
        Some(vec![
            ProviderEvent::ToolCall(ToolCall {
                id: "t1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({ "code": code }),
            }),
            ProviderEvent::Done(FinishReason::ToolCalls),
        ]),
        Some(vec![
            ProviderEvent::TextDelta("done".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ]),
    ]
}

/// A file OUTSIDE the read boundary, holding contents that would prove a leak if a
/// gated nested call ever reached the executor. Bound to the process so parallel
/// test threads cannot collide, and left behind on purpose (`/tmp`).
fn secret_file(tag: &str) -> String {
    let path = std::env::temp_dir().join(format!(
        "archimedes-codemode-{}-{tag}.txt",
        std::process::id()
    ));
    std::fs::write(&path, "SECRET-LEAKED-CONTENT").expect("the temp file writes");
    path.to_string_lossy().to_string()
}

/// TEST 1 — a `codemode` call round-trips through `AgentLoop`.
///
/// Three legs. (a) the dispatch arm is reachable and a well-formed `code` is
/// accepted; (b) a NON-ADVERTISED top-level key is rejected by the batch's
/// `unknown_param_error` pre-pass and never reaches the engine; (c) the whole turn
/// end to end — `handle_prompt` → provider tool call → batch → dispatch arm →
/// QuickJS → a real nested `bash` → `tool_execution_end`.
///
/// `{ "code": 42 }` is deliberately NOT asserted as a rejection:
/// `unknown_param_error` validates top-level KEYS only, so it accepts that call and
/// the BRIDGE reports the missing string (leg (a)'s sibling assertion).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codemode_dispatch_runs_a_script_through_agent_loop() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );

    // (a) The arm routes to the bridge and a real script runs: `1 + 1` needs no
    //     nested call, so this leg is purely "the arm exists and the engine ran".
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({ "code": "1 + 1" }),
            },
            &CancellationToken::new(),
        )
        .await;
    assert!(!r.is_error, "a well-formed script succeeds: {}", text(&r));
    assert_eq!(
        text(&r),
        "2",
        "the script's completion value is the parent result's text block"
    );
    let details = r.details.as_ref().expect("the parent carries `details`");
    assert_eq!(
        details["calls"],
        json!([]),
        "`details.calls` is the ONE key the UI reads, and a script that called \
         nothing reports the empty array: {details}"
    );

    // A MISSING `code` is the bridge's own report, not the checker's — the key is
    // advertised and the checker never inspects value types.
    let r = loop_
        .dispatch_tool(
            &ToolCall {
                id: "t2".to_string(),
                name: "codemode".to_string(),
                arguments: json!({ "code": 42 }),
            },
            &CancellationToken::new(),
        )
        .await;
    assert!(r.is_error, "a non-string `code` is an error result");
    assert!(
        text(&r).contains("`code`") && text(&r).contains("string"),
        "the bridge names the parameter and what it wanted: {}",
        text(&r)
    );

    // (b) The batch pre-pass REJECTS a non-advertised top-level key before dispatch
    //     (`param_rejection.rs`'s pattern: `run_tool_batch` returns the same
    //     `serde_json::to_value(&result)` the `tool_execution_end` carries).
    let results = loop_
        .run_tool_batch(
            &[ToolCall {
                id: "t3".to_string(),
                name: "codemode".to_string(),
                arguments: json!({ "code": "1 + 1", "extra": 1 }),
            }],
            &CancellationToken::new(),
        )
        .await;
    assert_eq!(results.len(), 1, "one result per call");
    let rejected = results[0]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        rejected.contains("unknown param(s) for `codemode`")
            && rejected.contains("`extra`")
            && rejected.contains("the call was rejected"),
        "the shared rejection wording, got {rejected:?}"
    );
    assert_eq!(results[0]["isError"], true, "{:?}", results[0]);
    assert!(
        results[0]["details"].is_null(),
        "a REJECTED call never reached the engine, so it carries no \
         `details.calls` at all (only a RUN script populates it): {:?}",
        results[0]
    );

    // (c) The end-to-end round trip over the WHOLE loop: a real script, a real
    //     engine, a real nested `bash`, a real `tool_execution_end`.
    let sink = recording_sink();
    let (provider, _calls) = ScriptedProvider::new(codemode_scripts(
        "const r = await tools.bash({ command: 'printf codemode-round-trip' });\n\
         r.text.trim();",
    ));
    let (e2e_tx, mut e2e_rx) = mpsc::unbounded_channel();
    let (_space, mut loop_, _db) = build_loop_with_sink(
        Box::new(provider),
        e2e_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
        sink.clone(),
    );
    // No settings layer → the all-`Allow` posture, so `bash` needs no gate at all.
    tokio::time::timeout(
        Duration::from_secs(90),
        loop_.handle_prompt(&text_prompt("go")),
    )
    .await
    .expect("the codemode turn settles");

    let end = wait_for_event(
        &mut e2e_rx,
        SETTLE_MS,
        |e| matches!(e, RpcEvent::tool_execution_end { tool_call_id, .. } if tool_call_id == "t1"),
    )
    .await
    .expect("the codemode call's tool_execution_end");
    let RpcEvent::tool_execution_end {
        result, is_error, ..
    } = end
    else {
        unreachable!()
    };
    assert!(
        !is_error,
        "the script completed: {}",
        serde_json::to_string(&result).unwrap_or_default()
    );
    assert_eq!(
        result["content"][0]["text"].as_str(),
        Some("codemode-round-trip"),
        "the completion value (the nested result's own text) is the parent's text"
    );
    let calls = result["details"]["calls"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(calls.len(), 1, "one nested call, one row: {result}");
    assert_eq!(calls[0]["status"].as_str(), Some("ok"), "{}", calls[0]);
    assert!(
        calls[0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("codemode-round-trip"),
        "the row carries the tool's own output: {}",
        calls[0]
    );
    assert_eq!(
        calls[0]["toolCallId"].as_str(),
        Some("t1/0"),
        "the element's `toolCallId` is `<parent>/<n>`: {}",
        calls[0]
    );
    // The amended contract, in its negative half: a nested id NEVER appears as a
    // FRAME-level `toolCallId` (that would mint a stray top-level transcript row).
    for frame in frames(&sink) {
        assert_ne!(
            frame["toolCallId"].as_str(),
            Some("t1/0"),
            "a frame was keyed on the NESTED id: {frame}"
        );
    }
}

/// TEST 2 — `Promise.all` batches nested calls, in completion order.
///
/// Both halves of the claim are pinned against the REAL engine:
///
/// - *batches* = runs CONCURRENTLY. Three nested `bash` calls that each wait
///   cannot finish in well under the sum of their waits unless they overlap. A
///   bridge that awaited each call inside the JS host closure (the obvious
///   implementation, and the one that deadlocks against the runtime lock) cannot
///   pass this.
/// - *completion order* = the row array is ordered by FINISH, not by issue: the
///   script issues slow → medium → fast, so a completion-ordered array reads fast,
///   medium, slow. A bridge that ordered by issue sequence is RED here.
///
/// The live frames are then asserted against the amended frozen contract: keyed on
/// the PARENT id, `partial_result` a serialized `ToolResult`, the snapshot growing
/// to all three rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promise_all_batches_nested_calls_in_completion_order() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let sink = recording_sink();
    let (_space, mut loop_, _db) = build_loop_with_sink(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
        vec![fake_model("m1")],
        None,
        sink.clone(),
    );
    let turn = CancellationToken::new();

    // Issued slow → medium → fast; completion order is the reverse.
    //
    // The two SLOW commands are built per platform, because the nested `bash`
    // hands its string to the platform shell verbatim (`exec/bash.rs`: `sh -c` on
    // unix, `cmd.exe /d /s /c` on Windows) and the two do not share a grammar:
    //
    // - unix: `sleep 0.7; echo slow` — `sleep` takes fractional seconds and `;`
    //   sequences.
    // - Windows: `ping -n 3 127.0.0.1 > NUL && echo slow`. `sleep` is not a
    //   Windows command, `;` is not a separator (cmd hands the whole
    //   `sleep 0.7; echo slow` to whatever resolves the first token, and GNU
    //   `sleep` on the runner's PATH then rejects `0.7;`, `echo` and `slow` as
    //   three bad intervals — that is the red this replaced), and `&&` is the
    //   sequencing operator. `ping -n` to a loopback address is the wait this tool
    //   DOCUMENTS for itself (`tool_specs::BASH_DESCRIPTION` recommends exactly
    //   this form) and it is proven to wait, exit 0 and need no console input by
    //   `agent::tools::exec::tests::bash::bash_ping_workaround_actually_waits_without_console_input`.
    //   NOT `timeout /t`: that needs console input, which a Tool child never has
    //   (stdin is NULL — see `bash::translate_no_console_input`).
    //
    // Windows' waits are ~2 s and ~1 s rather than 0.7/0.35 because `ping -n`
    // waits one second BETWEEN replies, so `-n 3` is 2 intervals and `-n 2` is 1 —
    // hence 3 and 2, and never `-n 1`, which waits nothing and would let `medium`
    // tie with `fast`. The concurrency claim is a ratio, so the coarser quantum
    // costs nothing; the serial bound below is derived from the same constants the
    // script is built from, so the two cannot drift.
    #[cfg(unix)]
    let (slow_cmd, medium_cmd, serial_ms) =
        ("sleep 0.7; echo slow", "sleep 0.35; echo medium", 1050_u64);
    #[cfg(not(unix))]
    let (slow_cmd, medium_cmd, serial_ms) = (
        "ping -n 3 127.0.0.1 > NUL && echo slow",
        "ping -n 2 127.0.0.1 > NUL && echo medium",
        3000_u64,
    );
    let script = format!(
        "const out = await Promise.all([\n  \
         tools.bash({{ command: '{}' }}),\n  \
         tools.bash({{ command: '{}' }}),\n  \
         tools.bash({{ command: 'echo fast' }}),\n\
         ]);\n\
         out.map(r => r.text.trim()).join(',');",
        slow_cmd, medium_cmd
    );
    let started = std::time::Instant::now();
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "p1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({ "code": script }),
            },
            &turn,
        )
        .await;
    let elapsed = started.elapsed();

    assert!(
        !result.is_error,
        "the batch completed, got: {}",
        text(&result)
    );
    // The script's OWN view is `Promise.all`'s guaranteed ISSUE order — pinned here
    // as the CONTRAST to the row array below, so the two orders are visibly
    // different things and neither assertion can pass by accidentally proving the
    // other. It also proves all three results reached the script.
    assert_eq!(
        text(&result),
        "slow,medium,fast",
        "`Promise.all` resolves in ISSUE order and saw all three results"
    );

    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| panic!("`details.calls` is an array: {:?}", result.details));
    assert_eq!(rows.len(), 3, "one row per nested call: {rows:?}");

    // CONCURRENCY: serial would take `serial_ms` (the two waits summed); the bound
    // is a 15% margin on that. Derived from the same constant the script's waits
    // are built from, so changing a wait cannot silently widen the bound.
    assert!(
        elapsed < Duration::from_millis(serial_ms + serial_ms / 7),
        "the three nested calls ran CONCURRENTLY (that is what `Promise.all` \
         batching means); serial would be ~{}ms and this took {}ms",
        serial_ms,
        elapsed.as_millis()
    );

    // COMPLETION ORDER, not issue order.
    let seen: Vec<&str> = rows
        .iter()
        .map(|r| r["text"].as_str().unwrap_or_default().trim())
        .collect();
    assert_eq!(
        seen,
        vec!["fast", "medium", "slow"],
        "rows are in COMPLETION order (they were issued slow → fast): {rows:?}"
    );
    let mut ids: Vec<&str> = rows
        .iter()
        .map(|r| r["toolCallId"].as_str().unwrap_or_default())
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["p1/0", "p1/1", "p1/2"],
        "every element id is `<parent>/<n>` over the ISSUE sequence: {rows:?}"
    );
    for (idx, row) in rows.iter().enumerate() {
        assert_eq!(row["status"].as_str(), Some("ok"), "row {idx}: {row}");
        assert_eq!(row["isError"].as_bool(), Some(false), "row {idx}: {row}");
        assert_eq!(
            row["toolName"].as_str(),
            Some("bash"),
            "row {idx} names the nested tool: {row}"
        );
        assert!(
            row["args"]["command"]
                .as_str()
                .unwrap_or_default()
                .contains("echo"),
            "row {idx} carries the nested call's own args: {row}"
        );
    }

    // LIVE FRAMES: every frame the bridge streamed is keyed on the PARENT id and
    // carries a serialized `ToolResult` whose `details.calls` is the snapshot.
    let live: Vec<Value> = frames(&sink)
        .into_iter()
        .filter(|f| {
            f["sessionUpdate"] == "tool_call_update"
                && f["rawOutput"]["details"]["calls"].is_array()
        })
        .collect();
    assert!(
        !live.is_empty(),
        "the bridge streamed live `tool_execution_update` frames carrying \
         `details.calls`"
    );
    let mut longest = 0usize;
    for frame in &live {
        assert_eq!(
            frame["toolCallId"].as_str(),
            Some("p1"),
            "a live frame's `toolCallId` is the PARENT call's id, never a nested id \
             (the amended contract — a nested-keyed frame mints a stray top-level \
             transcript row): {frame}"
        );
        // The serialized `ToolResult` shape, not a bespoke `{calls}` envelope.
        assert!(
            frame["rawOutput"]["content"].is_array(),
            "`partial_result` is a serialized `ToolResult` (it carries `content`): \
             {frame}"
        );
        assert!(
            frame["rawOutput"]["isError"].is_boolean(),
            "`partial_result` carries `isError`, the same key the final \
             `tool_execution_end` writes: {frame}"
        );
        let snapshot = frame["rawOutput"]["details"]["calls"]
            .as_array()
            .expect("`details.calls` is an array");
        for row in snapshot {
            assert!(
                row["toolCallId"]
                    .as_str()
                    .unwrap_or_default()
                    .starts_with("p1/"),
                "each element's `toolCallId` is `<parent>/<n>`: {row}"
            );
        }
        longest = longest.max(snapshot.len());
    }
    assert_eq!(
        longest, 3,
        "the snapshot grows to all three rows while the script runs: {live:?}"
    );
}

/// TEST 3 — a nested call passes the SAME gate as a direct call, and a denial is
/// indistinguishable from a direct denial.
///
/// Four legs on `reads: sandboxed` (the posture that answers `Deny` for a
/// beyond-boundary path tool):
/// - the gate is not merely consulted, it PREVENTS: a nested `read` of a file that
///   exists on disk must not reach the executor, so its row text is the refusal and
///   NOT the file's contents;
/// - the wording is byte-identical to `deny_tool_result` — the very function the
///   direct route calls for a `Deny`;
/// - and to the DIRECT call's own result, same loop, same path;
/// - a `Deny` never prompts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_gate_denial_returns_the_identical_denial_wording() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let sink = recording_sink();
    let (_cfg, mut loop_, _tmp) = build_policy_loop(
        Box::new(provider),
        events_tx,
        sink.clone(),
        Some(r#"{ "filePolicy": { "reads": "sandboxed", "writes": "allow", "shell": "allow" } }"#),
    );
    let outside = secret_file("deny");

    // THE FIXTURE RULE: `file_policy` is refreshed per PROMPT, so a loop that never
    // prompted still carries the all-`Allow` default. One throwaway turn first —
    // without it every assertion below would pass against an `Allow` policy.
    loop_.handle_prompt(&text_prompt("load the policy")).await;

    let nested = ToolCall {
        id: "d1/0".to_string(),
        name: "read".to_string(),
        arguments: json!({ "path": &outside }),
    };
    // The fixture is not vacuous: this really is a `Deny`, and the direct route
    // renders it through `deny_tool_result`.
    assert_eq!(
        loop_.policy_verdict(&nested),
        Some(crate::policy::Decision::Deny),
        "the pin needs a real `Deny` — a read INSIDE the boundary would be `Allow`"
    );
    let direct_text = result_text(&loop_.deny_tool_result(&nested));
    assert!(
        !direct_text.contains("SECRET-LEAKED-CONTENT"),
        "the direct refusal is a refusal: {direct_text}"
    );

    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "d1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": format!("const r = await tools.read({{ path: {outside:?} }});\n r.isError;")
                }),
            },
            &CancellationToken::new(),
        )
        .await;

    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| panic!("a denial still reports its rows: {:?}", result.details));
    assert_eq!(rows.len(), 1, "one nested call, one row: {rows:?}");
    let row = &rows[0];
    assert_eq!(row["status"].as_str(), Some("denied"), "{row}");
    assert_eq!(row["isError"].as_bool(), Some(true), "{row}");
    assert_eq!(
        row["toolCallId"].as_str(),
        Some("d1/0"),
        "the element's `toolCallId` is the SAME string the gate received as its \
         requestId, so a row and its prompt correlate: {row}"
    );
    let row_text = row["text"].as_str().unwrap_or_default();
    assert_eq!(
        row_text, direct_text,
        "BYTE-IDENTICAL to the direct denial — the model cannot tell a nested call \
         from a direct one"
    );
    assert!(
        !row_text.contains("SECRET-LEAKED-CONTENT"),
        "the gate PREVENTED the read (the row is the refusal, not the file): {row_text}"
    );
    // A DENY never prompts — the Ask arm's absence is part of the contract.
    assert!(
        emitted(&sink, "permission-request").is_empty(),
        "a deny must not raise a permission prompt"
    );

    // The equality in the other direction, against the DIRECT wire result.
    let direct_batch = loop_
        .run_tool_batch(&[nested], &CancellationToken::new())
        .await
        .into_iter()
        .next()
        .expect("one result");
    assert_eq!(
        direct_batch["content"][0]["text"].as_str(),
        Some(row_text),
        "the direct call's wire text equals the nested row's text"
    );
}

/// TEST 3b — the `Ask` arm really prompts, on the NESTED `requestId`, and a `reject`
/// settles the row `denied` with the direct arm's own wording.
///
/// The `requestId` is the ONE place a nested id legitimately appears: it is the
/// correlation key between a row and its prompt. The amended contract constrains
/// transcript FRAMES, not the gate's request id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nested_ask_prompts_on_the_nested_request_id_and_a_reject_denies_it() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let sink = recording_sink();
    let (_cfg, mut loop_, _tmp) = build_policy_loop(
        Box::new(provider),
        events_tx,
        sink.clone(),
        Some(r#"{ "filePolicy": { "reads": "ask", "writes": "allow", "shell": "allow" } }"#),
    );
    let outside = secret_file("ask");
    loop_.handle_prompt(&text_prompt("load the policy")).await;

    let nested = ToolCall {
        id: "a1/0".to_string(),
        name: "read".to_string(),
        arguments: json!({ "path": &outside }),
    };
    assert_eq!(
        loop_.policy_verdict(&nested),
        Some(crate::policy::Decision::Ask),
        "the pin needs a real `Ask` (untrusted Space, no trust source)"
    );
    let denial_wording = "permission denied".to_string();

    // The answer-waiter is spawned BEFORE the dispatch: a gate that prompts must be
    // answered or it waits for the turn, and a gate that must NOT prompt can then
    // never hang the test either.
    let pending = loop_.pending_permissions.clone();
    let answered = tokio::spawn(answer_permission(pending, "s1", "reject", None));
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "a1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": format!("await tools.read({{ path: {outside:?} }});")
                }),
            },
            &CancellationToken::new(),
        )
        .await;
    answered.await.expect("the answer task ran");

    let prompts = emitted(&sink, "permission-request");
    assert_eq!(
        prompts.len(),
        1,
        "an Ask of a nested call emits exactly one `permission-request`"
    );
    assert_eq!(
        prompts[0]["requestId"].as_str(),
        Some("a1/0"),
        "the prompt's `requestId` is the NESTED id `<parent>/<n>` — the gate's \
         correlation key with the row's `toolCallId`"
    );
    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| panic!("the asked call reports its row: {:?}", result.details));
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(
        rows[0]["status"].as_str(),
        Some("denied"),
        "a rejected prompt settles the row `denied`: {}",
        rows[0]
    );
    assert_eq!(
        rows[0]["text"].as_str(),
        Some(denial_wording.as_str()),
        "the Ask-rejection wording is the direct arm's own sentence: {}",
        rows[0]
    );
    assert!(
        !rows[0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("SECRET-LEAKED-CONTENT"),
        "a rejected prompt never reached the executor: {}",
        rows[0]
    );
}

/// TEST 3c — the `Ask` arm races the turn token: a prompt nobody answers is settled
/// by a Stop rather than hanging the script.
///
/// The row outcome is the FROZEN CONTRACT's, not a nicer one: "A nested call that
/// starts but does not finish (cancel / dropped promise) gets `status: "running"` in
/// its row". `race_turn` drops the script future when the token fires, so the call in
/// flight never reaches its `settle`, and the contract's `running` is what the row
/// must show. What this test pins is the two things that must NOT happen: the script
/// must not HANG on a prompt nobody answers, and the row must not be LOST — the
/// parent still reports the call it started, with the id the gate was given.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nested_ask_raced_by_a_stop_ends_the_script_and_keeps_the_row_running() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let sink = recording_sink();
    let (_cfg, mut loop_, _tmp) = build_policy_loop(
        Box::new(provider),
        events_tx,
        sink.clone(),
        Some(r#"{ "filePolicy": { "reads": "ask", "writes": "allow", "shell": "allow" } }"#),
    );
    let outside = secret_file("cancel");
    loop_.handle_prompt(&text_prompt("load the policy")).await;

    let turn = CancellationToken::new();
    let code = format!("await tools.read({{ path: {outside:?} }});");
    let driver = {
        let turn = turn.clone();
        tokio::spawn(async move {
            let mut loop_ = loop_;
            loop_
                .dispatch_tool(
                    &ToolCall {
                        id: "c1".to_string(),
                        name: "codemode".to_string(),
                        arguments: json!({ "code": code }),
                    },
                    &turn,
                )
                .await
        })
    };

    // Wait for the prompt to EXIST, then Stop the turn (nobody answers it).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while emitted(&sink, "permission-request").is_empty() {
        if tokio::time::Instant::now() >= deadline {
            panic!("the nested Ask never emitted a permission-request");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    turn.cancel();

    let result = tokio::time::timeout(Duration::from_secs(60), driver)
        .await
        .expect("a cancelled prompt must not hang the script")
        .expect("the driver task ran");

    // WHICH of the two outcomes arrives IS a race, and both are conformant, so this
    // test pins the invariant pair rather than a coin-flip: `race_turn`'s `select!`
    // polls the script future and the token arm in random order, and BOTH can win.
    //   · the token arm wins → the script future is dropped → the parent reports
    //     `codemode: cancelled` and the in-flight row keeps `status: "running"`
    //     (the contract's own wording for "started but did not finish");
    //   · the script arm wins → `gate_tool`'s own token arm had already resolved the
    //     prompt to a denial, the script saw `{isError: true}`, and it finished → the
    //     parent is a normal completion and the row is `denied`.
    // What must NEVER happen is a hang, a lost row, or an id that stopped matching the
    // gate's requestId — and those are what the three assertions below catch.
    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "the cancelled call still reports its row: {:?}",
                result.details
            )
        });
    assert_eq!(
        rows.len(),
        1,
        "the started call is NOT lost from `details.calls`: {rows:?}"
    );
    assert_eq!(
        rows[0]["toolCallId"].as_str(),
        Some("c1/0"),
        "the row keeps the id the gate received as its requestId: {}",
        rows[0]
    );
    let status = rows[0]["status"].as_str().unwrap_or_default();
    assert!(
        matches!(status, "running" | "denied"),
        "a Stop leaves the row either unsettled (`running`, the contract's word) or \
         settled as the gate's denial — never `ok`, never an executor error: {}",
        rows[0]
    );
    if status == "denied" {
        assert_eq!(
            rows[0]["text"].as_str(),
            Some("permission denied"),
            "the settled arm carries the direct route's own sentence: {}",
            rows[0]
        );
    } else {
        assert_eq!(
            text(&result),
            "codemode: cancelled",
            "the dropped-script arm reports the Stop: {}",
            text(&result)
        );
    }
}

/// The nested-call exclusion (spec §7.1 + the recursion the spec could not have
/// listed): an interactive or recursive name is NOT installed as `tools.<name>`, so
/// the script sees a JS `TypeError` and the parent result still reports.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_excluded_tool_is_not_installed_as_a_script_global() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let turn = CancellationToken::new();

    for name in [
        "ask",
        "sudo_exec",
        "manage_todo_list",
        "subagent",
        "list_agents",
        "codemode",
    ] {
        let result = loop_
            .dispatch_tool(
                &ToolCall {
                    id: format!("x-{name}"),
                    name: "codemode".to_string(),
                    arguments: json!({ "code": format!("typeof tools.{name};") }),
                },
                &turn,
            )
            .await;
        assert!(
            !result.is_error,
            "`tools.{name}` is merely absent, so the script still runs: {}",
            text(&result)
        );
        assert_eq!(
            text(&result),
            "undefined",
            "`tools.{name}` must NOT exist for a script (interactive or recursive)"
        );
    }

    // And `ALL_TOOLS` does not list them either (discovery is the other half).
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "x-all".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "ALL_TOOLS.map(t => t.name).filter(n => \
                              ['ask','sudo_exec','manage_todo_list','subagent','list_agents','codemode']\
                              .includes(n)).join(',');"
                }),
            },
            &turn,
        )
        .await;
    assert!(
        !result.is_error,
        "the discovery script ran: {}",
        text(&result)
    );
    assert_eq!(
        text(&result),
        "",
        "no excluded tool is discoverable, got {:?}",
        text(&result)
    );
}

/// The interactive MCP `auth` action is refused BEFORE dispatch, so a script cannot
/// open the user's browser and block on a consent screen. The refusal is
/// argument-level: `mcp` itself stays callable, and the NAME-based list cannot express
/// this case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_interactive_mcp_auth_action_is_refused_from_a_script() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let turn = CancellationToken::new();

    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "auth1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    // A server name this manager does not HOLD, so the refusal is the
                    // ONLY way to get this text: had the call reached `mcp_tool` it
                    // would have answered with the tool's own server-scope error, and
                    // had it reached `authenticate` it would have opened a browser.
                    "code": "const r = await tools.mcp({ action: 'auth', server: 'no-such-server' });\n\
                             r.isError + '|' + r.text;"
                }),
            },
            &turn,
        )
        .await;
    // The SCRIPT succeeds — a refused nested call is a result, not an exception, so
    // the parent's own outcome must not turn red over it.
    assert!(
        !result.is_error,
        "the refusal is a RESULT the script can branch on, not a thrown script: {}",
        text(&result)
    );
    let body = text(&result);
    assert!(
        body.starts_with("true|"),
        "the script saw `isError: true`, got {body:?}"
    );
    assert!(
        body.contains("not callable from a codemode script"),
        "the refusal names itself as the bridge's, so the model is not sent looking \
         for a broken MCP server: {body:?}"
    );
    assert!(
        !body.contains("not found") && !body.contains("Unknown action"),
        "this is the BRIDGE's refusal, not `mcp_tool`'s — the call must never reach \
         the tool: {body:?}"
    );

    // The row records it as the bridge's refusal, NOT a gate denial: `denied` is
    // reserved for what the POLICY GATE refused, and the gate never saw this call.
    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"].as_str(), Some("error"), "{}", rows[0]);
    assert_eq!(rows[0]["toolName"].as_str(), Some("mcp"), "{}", rows[0]);

    // And the NON-interactive actions still dispatch — the refusal is scoped to the
    // one action that blocks on a human, not to the `mcp` tool. `status` is the tool's
    // default arm, so an over-broad refusal would answer differently here.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "auth2".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const r = await tools.mcp({});\n!r.isError;"
                }),
            },
            &turn,
        )
        .await;
    assert!(!result.is_error, "the script ran: {}", text(&result));
    assert_eq!(
        text(&result),
        "true",
        "`mcp({{}})` (the status action) must still dispatch — only `auth` is \
         refused: {}",
        text(&result)
    );
}

/// The discovery globals: `ALL_TOOLS` carries the advertised set, `describeTool`
/// answers a built-in's advertised schema, and `searchTools` matches
/// case-insensitively with an optional `limit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_discovery_globals_describe_the_advertised_set() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let turn = CancellationToken::new();

    // `describeTool` returns the ADVERTISED schema — the same JSON the model saw.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "g1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const d = await describeTool('bash');\n\
                             JSON.stringify(Object.keys(d.parameters.properties).sort());"
                }),
            },
            &turn,
        )
        .await;
    assert!(
        !result.is_error,
        "the discovery script ran: {}",
        text(&result)
    );
    assert_eq!(
        text(&result),
        r#"["command","timeout_ms"]"#,
        "`describeTool('bash')` answers the advertised `bash` schema"
    );

    // `searchTools` matches the description too, case-insensitively, and honours
    // `limit`.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "g2".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const r = await searchTools('SHELL');\n\
                             r.tools.length >= 1 && r.tools[0].name === 'bash';"
                }),
            },
            &turn,
        )
        .await;
    assert_eq!(
        text(&result),
        "true",
        "`searchTools` matches case-insensitively on the description: {}",
        text(&result)
    );

    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "g3".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const r = await searchTools('', { limit: 2 });\n\
                             r.tools.length;"
                }),
            },
            &turn,
        )
        .await;
    assert_eq!(
        text(&result),
        "2",
        "an empty query lists and `limit` caps it: {}",
        text(&result)
    );

    // `ALL_TOOLS` is the callable set: `bash` in it, `ask` not, and the count is
    // advertised-minus-the-excluded-six.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "g4".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "ALL_TOOLS.length + ':' + ALL_TOOLS.some(t => t.name === 'bash');"
                }),
            },
            &turn,
        )
        .await;
    let expected = tool_specs().len() - 6;
    assert_eq!(
        text(&result),
        format!("{expected}:true"),
        "ALL_TOOLS is the advertised set minus the six excluded tools"
    );
}

/// The uniform JS result (frozen contract item 3): every nested call resolves to
/// `{ text, isError, details }`, a FAILED tool sets `isError` without throwing, and
/// `bash` yields ONE combined block — stdout and stderr in the same string.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nested_call_resolves_to_the_uniform_result_and_never_throws() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let turn = CancellationToken::new();

    // A failing `bash` is a RESULT with `isError: true`, not a rejected promise —
    // the script can branch on it. And stdout+stderr arrive in ONE `text`.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "u1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const r = await tools.bash({ command: 'echo to-stdout; echo to-stderr 1>&2' });\n\
                             [typeof r.text, typeof r.isError, typeof r.details, \
                              r.text.includes('to-stdout'), r.text.includes('to-stderr'), r.isError].join(',');"
                }),
            },
            &turn,
        )
        .await;
    assert!(!result.is_error, "the script ran: {}", text(&result));
    assert_eq!(
        text(&result),
        "string,boolean,object,true,true,false",
        "the uniform triple, one combined text block, and a successful bash is \
         NOT an error: {}",
        text(&result)
    );

    // A tool that FAILS (a missing file) still resolves — `isError` is the signal.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "u2".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const r = await tools.read({ path: 'definitely-not-here-42.txt' });\n\
                             r.isError + ':' + (r.text.length > 0);"
                }),
            },
            &turn,
        )
        .await;
    assert_eq!(
        text(&result),
        "true:true",
        "a failed nested call resolves to `{{isError: true}}` rather than rejecting \
         the script: {}",
        text(&result)
    );
    // And the ROW agrees with the script's view of the same call.
    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"].as_str(), Some("error"), "{}", rows[0]);
    assert_eq!(rows[0]["isError"].as_bool(), Some(true), "{}", rows[0]);

    // A malformed argument object is reported, not thrown.
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "u3".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "const circular = {}; circular.self = circular;\n\
                             const r = await tools.bash(circular);\n\
                             r.isError + '|' + r.text;"
                }),
            },
            &turn,
        )
        .await;
    assert!(
        text(&result).starts_with("true|"),
        "a non-serializable argument is a failed result, not a script crash: {}",
        text(&result)
    );
}

/// A script that THROWS ends as a failed parent result naming the JS exception, and
/// the rows it had already produced are still reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_throwing_script_reports_its_message_and_keeps_its_rows() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let result = loop_
        .dispatch_tool(
            &ToolCall {
                id: "f1".to_string(),
                name: "codemode".to_string(),
                arguments: json!({
                    "code": "await tools.bash({ command: 'echo first' });\n\
                             throw new Error('the script gave up');"
                }),
            },
            &CancellationToken::new(),
        )
        .await;
    assert!(result.is_error, "a throwing script is a failed result");
    assert!(
        text(&result).contains("the script gave up"),
        "the JS exception message reaches the model: {}",
        text(&result)
    );
    let rows = result
        .details
        .as_ref()
        .and_then(|d| d.get("calls"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        rows.len(),
        1,
        "the call that HAD completed is still reported: {rows:?}"
    );
    assert_eq!(rows[0]["status"].as_str(), Some("ok"), "{}", rows[0]);
}

/// An empty / whitespace-only `code` is the bridge's own `script is empty` failure,
/// and an empty script produces no rows (no engine is even built).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_empty_script_fails_without_producing_rows() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, mut loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    for code in ["", "   ", "\n\t"] {
        let result = loop_
            .dispatch_tool(
                &ToolCall {
                    id: "e1".to_string(),
                    name: "codemode".to_string(),
                    arguments: json!({ "code": code }),
                },
                &CancellationToken::new(),
            )
            .await;
        assert!(result.is_error, "{code:?} must fail");
        assert_eq!(
            text(&result),
            "codemode: script is empty",
            "the bridge's own wording for the empty program"
        );
        assert_eq!(
            result.details,
            Some(json!({ "calls": [] })),
            "the failure still carries the frozen `details.calls` key"
        );
    }
}

/// A STOP during a script cancels it: the parent reports `codemode: cancelled` and
/// the turn token is the single cancellation authority (the same token a direct call
/// races). Bounded so a regression fails loudly instead of hanging the suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_cancels_a_running_script() {
    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let (provider, _calls) = ScriptedProvider::new(terminal_scripts());
    let (_space, loop_) = build_loop(
        Box::new(provider),
        events_tx,
        Arc::new(StdMutex::new(CancellationToken::new())),
        watch::channel(0u64).0,
        RetryPolicy::new_with(5, Duration::from_millis(1)),
    );
    let turn = CancellationToken::new();
    // A hot loop: no host await, so ONLY the interrupt handler can end it (C1's
    // part 1), which the bridge re-armed to watch the turn token.
    let driver = {
        let turn = turn.clone();
        tokio::spawn(async move {
            // The loop is MOVED into the task (a `&mut` cannot cross the spawn), so
            // the task owns it and drops it when the cancelled call returns.
            let mut loop_ = loop_;
            loop_
                .dispatch_tool(
                    &ToolCall {
                        id: "s1".to_string(),
                        name: "codemode".to_string(),
                        arguments: json!({ "code": "while (true) {}" }),
                    },
                    &turn,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    turn.cancel();
    let result = tokio::time::timeout(Duration::from_secs(60), driver)
        .await
        .expect("the interrupt handler ends the hot loop — it must not hang")
        .expect("the driver task ran");
    assert!(result.is_error, "a cancelled script is a failed result");
    assert_eq!(
        text(&result),
        "codemode: cancelled",
        "the cancellation wording (C1's `ScriptEnd::Cancelled`), not a JS exception"
    );
    assert_eq!(
        result.details,
        Some(json!({ "calls": [] })),
        "a cancelled script still carries the frozen key"
    );
}

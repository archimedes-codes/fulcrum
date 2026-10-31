//! The in-process interactive channel: the ask / sudo / todo cores the
//! native `AgentLoop` calls in-process (NOT over a socket) — the
//! user-paced oneshots (`PendingInteractive` / `PendingSudo`), the sudo
//! run flow (`SudoRunner` / `RealSudoRunner`), the todo apply, and the
//! `SudoPromptCleanup` modal-close guard.
//!
//! The `ask` / `confirm` / `password` (interactive request), `todo_update`,
//! and `sudo_exec` flows the native `AgentLoop`'s `ToolRegistry` calls
//! directly (the desktop is the Client of the interactive channel, ADR
//! 0022 — the desktop is native-only, so there is no external socket:
//! the cores are called in-process, and the desktop answers the prompts
//! itself).
//!
//! **The request flow** (the `ask` / `confirm` / `password` core, ADR 0022):
//! register a oneshot in `pending_bridge` (keyed `"{session_id}/{request_id}"`,
//! the `permission` convention) **before** emitting the `interactive-request`
//! event (the UI's `respond_interactive_request` may race the emission and
//! must find the entry), then await the answer — with NO deadline: a prompt
//! ends when the PERSON answers or the session is cancelled (the `cancel`
//! token), never when a clock runs out.
//!
//! **The `sudo_exec` core** (Phase 2): the same flow, but the desktop
//! ANSWERS the `confirm` / `password` sub-prompts itself (the `pending_sudo`
//! oneshots, the per-session `sudo_password` credential cache — the suite's
//! `credentialCache`: in-memory only, keyed by session id, cleared at every
//! session boundary) and runs `sudo -S` through the `SudoRunner` seam (the
//! production `RealSudoRunner`; tests inject a fake).

pub mod sudo_argv;
pub mod sudo_exec;

pub use sudo_exec::{RealSudoRunner, SudoRunner};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::subagent::LaunchConfig;
use crate::agent::todo::{TodoItem, TodoStatus, TodoStore};
use crate::agent::tools::exec::{ContentBlock, ToolResult};

use sudo_argv::{build_sudo_argv, is_sudo_auth_failure, scrub_secret};
use sudo_exec::SudoPromptCleanup;

/// The manager's map of pending interactive-request senders.
///
/// Keyed by `"{session_id}/{request_id}"` so the driver-task cleanup can
/// drain all entries for a closing session at once (dropping the senders
/// cancels the spawned waiters). The oneshot carries the response **`result`
/// `Value` verbatim** — no wrapper (for `password`, `{password}`; for
/// `confirm`, `{confirmed}`; for `ask`, the `AskResponsePayload`).
pub type PendingInteractive = Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>;

/// Build the compound key for a pending interactive request.
pub fn interactive_key(session_id: &str, request_id: &str) -> String {
    format!("{session_id}/{request_id}")
}

/// The key prefix of all pending-interactive keys that belong to `session_id`.
/// Keys are `"{session_id}/{request_id}"`, so the prefix carries the trailing
/// slash: a session id that is a plain prefix of another ("s1" vs "s10")
/// must not drain the other session's requests.
pub fn session_key_prefix(session_id: &str) -> String {
    format!("{session_id}/")
}

/// The manager's map of pending `sudo_exec` SUB-prompts (Phase 2, Task 1).
///
/// One entry per sub-prompt, keyed `"{session_id}/{request_id}:confirm"` /
/// `"{session_id}/{request_id}:password"` (the `requestId` the modals echo
/// VERBATIM into `respond_interactive_request`). Each oneshot carries the raw
/// `respond_interactive_request` `result` `Value` (for `:confirm`, `{confirmed}`;
/// for `:password`, `{password}` — an empty `""` is a cancel).
///
/// The driver-task cleanup drains all entries for a closing session at once
/// (dropping the senders cancels the in-flight sub-prompt waiters).
pub type PendingSudo = Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>;

/// The per-session sudo credential (the suite's `CachedCredential`,
/// `packages/sudo/src/cache.ts` — in-memory only, never disk/keyring).
#[derive(Debug, Clone)]
pub struct CachedPassword {
    pub password: String,
    pub expires_at: std::time::Instant,
}

/// The sudo password-cache TTL (the suite's `DEFAULT_SUDO_CONFIG.ttlMs` —
/// 15 min, config key `archimedes.sudo.ttlMs`).
pub const SUDO_TTL: Duration = Duration::from_millis(900_000);

/// The `sudo_exec` default command timeout (the suite's
/// `DEFAULT_SUDO_CONFIG.defaultTimeoutMs` — 120 s): the RUN is bounded by
/// the caller's `timeoutMs` (absent = this). The `confirm` / `password`
/// sub-prompts are NOT bounded — they wait for the person.
pub const SUDO_DEFAULT_TIMEOUT_MS: u64 = 120_000;

/// The `tool_exec` per-method timeout (native-agent-harness Task 2): fast
/// tools are capped at 30 s; `bash` is capped by its `timeout_ms` param
/// (absent = this 5-min default — the RUN is bounded by the caller's
/// `timeout_ms`, mirroring the `sudo_exec` run cap).
pub const TOOL_EXEC_FAST_TIMEOUT: Duration = Duration::from_secs(30);
pub const TOOL_EXEC_BASH_DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// The `SudoRunner`'s outcome. The three verdict fields are the SHARED exit
/// classifier's (`crate::agent::tools::exec::bash::bash_exit_report`, ADR
/// 0080 + its amendment), so the elevated path reports the same shape a `bash`
/// run does:
///
/// * `exit_code` is the exit code that was actually OBSERVED — `None` when no
///   code was (a signal death, a child that was never reaped, a process-level
///   failure, a non-run). NEVER a fabricated number: `-1` was
///   indistinguishable from a real code, and `124` is a real `timeout(1)` exit
///   code, so a sentinel could never be told apart from the event it faked.
/// * `is_error` / `note` are the classifier's verdict and note — the wire
///   `isError` / error text the render turns into what the model reads. A
///   pipe-held exit 0 (`timed_out: true`, pipes still held at the deadline) is
///   therefore `is_error: false` with a `DO NOT RE-RUN` note, exactly as in
///   `exec_bash`.
/// * `timed_out` stays the RUN's own fact (did the deadline fire) and feeds
///   the classifier inside `run_sudo_real`; the render no longer keys off it —
///   a pipe-held completion has `timed_out: true` and is not an error.
/// * `error: Some(…)` is a PROCESS-level failure (spawn error, transport
///   failure, stdin-write timeout). It is never an auth failure, so the cached
///   credential is kept.
#[derive(Debug, Clone)]
pub struct SudoRun {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    pub is_error: bool,
    pub note: Option<String>,
    pub error: Option<String>,
}

/// Parse + validate the `dispatch_subagent` params: `task` (required,
/// non-empty) + the optional `launch` config (`model` / `systemPrompt` /
/// `tools` — `tools: []` is treated as `None`). `pub(crate)` so the native
/// `AgentLoop`'s `ToolRegistry` (Task 6) reuses the SAME validation.
pub(crate) fn dispatch_params(params: &Value) -> Option<(String, LaunchConfig)> {
    let task = params
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if task.trim().is_empty() {
        return None;
    }
    Some((
        task.to_string(),
        LaunchConfig {
            system_prompt: params
                .get("systemPrompt")
                .and_then(Value::as_str)
                .map(str::to_string),
            model: params
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string),
            thinking: params
                .get("thinking")
                .and_then(Value::as_str)
                .map(str::to_string),
            tools: params
                .get("tools")
                .and_then(Value::as_array)
                .and_then(|arr| {
                    let tools: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect();
                    (!tools.is_empty()).then_some(tools)
                }),
            ..Default::default()
        },
    ))
}

/// The `todo_update` core (method-aware — the desktop answers it itself,
/// in-process): apply the `write` / `replace` / `toggle` / `clear` /
/// `add` / `remove` operations to the shared `TodoStore` (the `todos`
/// table) and emit a `interactive-event` (`todo_update` — the EXISTING
/// `useInteractive.applyTodoUpdate` / `TodoBoardPanel` consume the
/// `InteractiveEventPayload` shape, `sessionId` MANDATORY). A cancelled session
/// aborts the core (BEFORE the store write / the push emit); the `cancel`
/// token is the session's teardown token (the native `AgentLoop` passes
/// its `cancel`; a session close cancels the in-flight core).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn todo_apply(
    store: &TodoStore,
    sid: &str,
    source: &str,
    params: &Value,
    sink: &Arc<dyn EventSink>,
    cancel: &CancellationToken,
) -> ToolResult {
    let operation = params
        .get("operation")
        .and_then(Value::as_str)
        .unwrap_or("");

    // A cancelled session aborts the core (BEFORE the store write / the
    // push emit — a cancelled session's agent is gone).
    if cancel.is_cancelled() {
        return ToolResult {
            content: vec![],
            details: None,
            is_error: true,
        };
    }

    if operation == "read" {
        let todos = store.get(sid);
        // The suite's read text (`tool.ts:75-84`): `JSON.stringify(todos,
        // null, 2)` when non-empty, else the "No todos" text.
        let text = if todos.is_empty() {
            "No todos. Use write operation to create a todo list.".to_string()
        } else {
            serde_json::to_string_pretty(&todos).unwrap_or_default()
        };
        return ToolResult {
            content: vec![ContentBlock::Text { text }],
            details: Some(json!({ "operation": "read", "todos": todos })),
            is_error: false,
        };
    }
    // write (the default for a missing/unknown operation — the suite's
    // schema requires `operation` in {"write","read"}, so anything
    // else is a malformed write).
    match params.get("todoList").and_then(Value::as_array) {
        None => {
            // The suite's text + flag (`tool.ts:89-94`): `todos` is
            // the CURRENT list, `error` is the marker.
            let current = store.get(sid);
            ToolResult {
                content: vec![ContentBlock::Text {
                    text: "Error: todoList is required for write operation.".to_string(),
                }],
                details: Some(
                    json!({ "operation": "write", "todos": current, "error": "todoList required" }),
                ),
                is_error: true,
            }
        }
        Some(arr) => {
            // The suite's `state.validate` (`state-manager.ts:36-59`): the
            // TypeBox schema lets an empty/whitespace `content` through, so
            // the validation is the guard here. (The `status` is checked
            // against the wire casing; a non-string `description` is an
            // error — the schema would have rejected it, but a
            // hand-rolled frame must not blow up the handler.)
            let valid_statuses = ["pending", "in_progress", "completed"];
            let mut errors: Vec<String> = Vec::new();
            let mut items: Vec<TodoItem> = Vec::new();
            for (i, item) in arr.iter().enumerate() {
                let prefix = format!("Item {}", i + 1);
                if item.is_null() {
                    errors.push(format!("{prefix}: undefined item"));
                    continue;
                }
                let content = item.get("content").and_then(Value::as_str);
                let status = item.get("status").and_then(Value::as_str);
                if content.is_none_or(|c| c.trim().is_empty()) {
                    errors.push(format!("{prefix}: missing or invalid 'content'"));
                }
                if !status.is_some_and(|s| valid_statuses.contains(&s)) {
                    errors.push(format!(
                        "{prefix}: 'status' must be one of: pending, in_progress, completed"
                    ));
                }
                if item.get("description").is_some()
                    && item.get("description").and_then(Value::as_str).is_none()
                {
                    errors.push(format!("{prefix}: 'description' must be a string"));
                }
                if let (Some(content), Some(status)) = (content, status) {
                    items.push(TodoItem {
                        content: content.to_string(),
                        status: match status {
                            "pending" => TodoStatus::Pending,
                            "in_progress" => TodoStatus::InProgress,
                            _ => TodoStatus::Completed,
                        },
                        description: item
                            .get("description")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    });
                }
            }
            if !errors.is_empty() {
                // The suite's validation text (`tool.ts:84-93`): the errors
                // prefixed `  - ` and joined with newlines; `details.error`
                // is the errors joined with `; `.
                let text = format!(
                    "Validation failed:\n{}",
                    errors
                        .iter()
                        .map(|e| format!("  - {e}"))
                        .collect::<Vec<_>>()
                        .join("\n")
                );
                let current = store.get(sid);
                ToolResult {
                    content: vec![ContentBlock::Text { text }],
                    details: Some(
                        json!({ "operation": "write", "todos": current, "error": errors.join("; ") }),
                    ),
                    is_error: true,
                }
            } else {
                let stored = store.set(sid, items.clone());
                // Emit the `todos_update` push via the EXISTING
                // `interactive-event` sink path (the `InteractiveEventPayload`
                // shape — the EXISTING `useInteractive.applyTodoUpdate` +
                // `TodoBoardPanel` consume it; do NOT invent a new
                // `todo-update` event).
                sink.emit(
                    "interactive-event",
                    json!({
                        "sessionId": sid,
                        "seq": 0,
                        "event": "todos_update",
                        "payload": { "source": source, "todos": stored }
                    }),
                );
                // The suite's write text (`tool.ts:138`): the stats from
                // the stored items, + a warning appended when the list has
                // <3 items.
                let completed = stored
                    .iter()
                    .filter(|t| t.status == TodoStatus::Completed)
                    .count();
                let total = stored.len();
                let mut message = format!(
                    "Todos have been modified all. {completed}/{total} completed. Ensure that you continue to use the todo list to track your progress. Please proceed with the current tasks if applicable."
                );
                if stored.len() < 3 {
                    message.push_str(
                        "\n\nWarning: Small todo list (<3 items). This task might not need a todo list.",
                    );
                }
                ToolResult {
                    content: vec![ContentBlock::Text { text: message }],
                    details: Some(json!({ "operation": "write", "todos": stored })),
                    is_error: false,
                }
            }
        }
    }
}

/// The `sudo_exec` core (method-aware — the desktop answers it itself,
/// in-process): the `confirm` / `password` sub-prompts (the `pending_sudo`
/// oneshots, the `interactive-request` events via `sink`), the per-session
/// `sudo_password` credential cache (the suite's `credentialCache` —
/// in-memory only, cleared at every session boundary), and the `sudo -S`
/// execution through the `SudoRunner` seam (the production
/// `RealSudoRunner`; tests inject a fake). A cancelled session aborts the
/// core (the `cancel` token — the native `AgentLoop`'s session teardown
/// token); the `SudoPromptCleanup` guard emits the terminal
/// `interactive-request-close` on a DROPPED (unanswered) prompt.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sudo_run_flow(
    sid: &str,
    request_id: &str,
    source: &str,
    params: &Value,
    sink: &Arc<dyn EventSink>,
    runner: &Arc<dyn SudoRunner>,
    pending_sudo: &PendingSudo,
    sudo_password: &Arc<Mutex<HashMap<String, CachedPassword>>>,
    cancel: &CancellationToken,
) -> ToolResult {
    let command = params
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let reason = params
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();

    // The suite's empty-command rejection (`tool.ts:404-411`) — BEFORE any
    // sub-prompt (no runner call, no modal).
    if command.is_empty() {
        return ToolResult {
            content: vec![ContentBlock::Text {
                text: "The command is empty — provide the exact command to run with elevated privileges.".to_string(),
            }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": null, "stdout": "", "stderr": "", "error": "empty command" }),
            ),
            is_error: true,
        };
    }

    // The DROPPED-flow cleanup guard (finding 3): a `tokio::select!` dropping
    // this future (the `dispatch_tool` turn-cancel arm) skips the exit-path
    // cleanup below, so the guard removes the `pending_sudo` key(s) + closes
    // the modal on `Drop` (a normal completion removes the keys first — the
    // guard's removal is then a no-op, so it is idempotent).
    let mut cleanup = SudoPromptCleanup::new(pending_sudo, sink, sid);

    // ── the confirm sub-prompt (BEFORE any credential is acquired) ──
    // Register the oneshot BEFORE the event is emitted (the UI may answer
    // the instant it sees the event — the same race class the generic path
    // guards, `b3c920a`). The `requestId` is the DERIVED `"{id}:confirm"`
    // (NOT the bare frame `id` — a bare id would miss the
    // `respond_interactive_request` lookup and leave the user's answer
    // unrouted, which now hangs the prompt instead of timing it out).
    let confirm_key = interactive_key(sid, &format!("{request_id}:confirm"));
    let (confirm_tx, confirm_rx) = oneshot::channel();
    {
        let mut map = pending_sudo.lock().await;
        map.insert(confirm_key.clone(), confirm_tx);
    }
    cleanup.track(&confirm_key);
    sink.emit(
        "interactive-request",
        json!({
            "sessionId": sid,
            "requestId": format!("{request_id}:confirm"),
            "method": "confirm",
            "source": source,
            "toolCallId": Value::Null,
            "params": { "command": command, "reason": reason }
        }),
    );
    let r: Result<Value, ()> = tokio::select! {
        r = confirm_rx => r.map_err(|_| ()),
        _ = cancel.cancelled() => Err(()),
    };
    // EVERY exit path (answered, cancel — the entry was inserted
    // BEFORE the select, so it must be removed on all of them) removes the
    // entry (no leaked dead-receiver entries — mirrors the `pending_bridge`
    // remove in the generic path). A cancel resolves to `false` (a
    // missing/`false` `confirmed` is a cancel). The key is UNTRACKED too
    // (the `SudoPromptCleanup` guard's `interactive-request-close` is then a
    // no-op — a completed flow, after the user answered, must not emit a
    // stale close; only a genuinely dropped flow — the entry NOT removed —
    // emits the close).
    pending_sudo.lock().await.remove(&confirm_key);
    cleanup.untrack(&confirm_key);
    let confirmed = r
        .ok()
        .and_then(|v| v.get("confirmed").and_then(Value::as_bool))
        .unwrap_or(false);
    if !confirmed {
        // The suite's result (`tool.ts:429-434`) — on a user cancel, a
        // timeout, OR a session close: no password prompt, no execution.
        return ToolResult {
            content: vec![ContentBlock::Text {
                text: "Command not confirmed — not executed, and no password was requested."
                    .to_string(),
            }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": null, "stdout": "", "stderr": "", "error": "command not confirmed" }),
            ),
            is_error: true,
        };
    }

    // ── the password (CACHE-HIT FIRST — the suite's `credentialCache.get()`,
    // `tool.ts:442-456`) ── a valid entry (TTL in the future) skips the
    // prompt entirely (no `:password` event, no re-prompt); on expiry the
    // entry is dropped and the prompt fires.
    let cached: Option<String> = {
        let mut cache = sudo_password.lock().await;
        if let Some(entry) = cache.get(sid) {
            if entry.expires_at > std::time::Instant::now() {
                Some(entry.password.clone())
            } else {
                cache.remove(sid);
                None
            }
        } else {
            None
        }
    };
    let mut prompted = false;
    let mut prompted_value: Option<Value> = None;
    if cached.is_none() {
        let password_key = interactive_key(sid, &format!("{request_id}:password"));
        let (pw_tx, pw_rx) = oneshot::channel();
        {
            let mut map = pending_sudo.lock().await;
            map.insert(password_key.clone(), pw_tx);
        }
        cleanup.track(&password_key);
        sink.emit(
            "interactive-request",
            json!({
                "sessionId": sid,
                "requestId": format!("{request_id}:password"),
                "method": "password",
                "source": source,
                "toolCallId": Value::Null,
                "params": { "command": command, "reason": reason }
            }),
        );
        let r: Result<Value, ()> = tokio::select! {
            r = pw_rx => r.map_err(|_| ()),
            _ = cancel.cancelled() => Err(()),
        };
        pending_sudo.lock().await.remove(&password_key);
        // Untrack the key (the `SudoPromptCleanup` guard's `interactive-request-close`
        // is then a no-op — a completed flow, after the user answered, must
        // not emit a stale close; only a genuinely dropped flow emits it).
        cleanup.untrack(&password_key);
        prompted = true;
        prompted_value = r.ok();
    }
    let password = cached.clone().or_else(|| {
        prompted_value
            .as_ref()
            .and_then(|v| v.get("password").and_then(Value::as_str))
            .map(str::to_string)
            .filter(|p| !p.is_empty()) // an EMPTY `""` password is a cancel
    });
    let Some(password) = password else {
        // Cancelled (an empty password, a session close, or a Stop) — the
        // suite's result (`tool.ts:437-447`): nothing was cached, nothing
        // was run.
        return ToolResult {
            content: vec![ContentBlock::Text {
                text: "Password entry cancelled — not executed, and nothing was cached."
                    .to_string(),
            }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": null, "stdout": "", "stderr": "", "error": "password entry cancelled" }),
            ),
            is_error: true,
        };
    };
    if prompted {
        // Cache the password with the suite's TTL (15 min — `config.ts`;
        // the per-session slot, cleared in the driver-task teardown).
        let mut cache = sudo_password.lock().await;
        cache.insert(
            sid.to_string(),
            CachedPassword {
                password: password.clone(),
                expires_at: std::time::Instant::now() + SUDO_TTL,
            },
        );
    }

    // ── run sudo on the DESKTOP host via the INJECTABLE `SudoRunner` ──
    // `timeout` = `params.timeoutMs` (or the 120 s default) — the RUN is
    // bounded by the caller's `timeoutMs`; the sub-prompts above are not
    // bounded at all (they wait for the person).
    let timeout_ms = params
        .get("timeoutMs")
        .and_then(Value::as_u64)
        .filter(|ms| *ms > 0)
        .unwrap_or(SUDO_DEFAULT_TIMEOUT_MS);
    let argv = build_sudo_argv(&command);
    let run = runner
        .run(argv, password.clone(), Duration::from_millis(timeout_ms))
        .await;

    // Secret scrubbing (the suite's `scrubSecret`, applied PER STREAM — the
    // password must not appear in the captured output that is persisted in
    // tool results). The auth-failure check runs on the RAW stderr (the
    // suite computes it inside `runSudo`, before any scrub).
    let auth_failed = is_sudo_auth_failure(&run.stderr);
    let stdout = scrub_secret(&run.stdout, &password);
    let stderr = scrub_secret(&run.stderr, &password);

    // The render's arm ORDER is load-bearing (ADR 0080's amendment). (1) the
    // PROCESS-level latch first, (2) the auth signature, (3) the classifier's
    // error verdict, (4) the success arm. A process-level failure carries
    // `exit_code: None` with `is_error` FALSE — `run.error` is the ONLY signal
    // it renders from — so it must be checked before any verdict-derived arm,
    // or the success branch would report a spawn failure as a completed run:
    // the exact class of misreport this seam exists to kill.
    if let Some(error) = &run.error {
        // Process-level failure (spawn error, transport failure, stdin-write
        // timeout) — the suite's `tool.ts:483-490`: a clean tool failure with
        // the `error` + `isError`, NEVER an auth failure — the cached
        // credential is KEPT (a transport glitch is transient relative to
        // the credential itself). `exitCode` is whatever was observed (a
        // process-level failure observed none, so the wire says `null` —
        // the bash `ToolResult::fail(…, None)` shape, never a fabricated
        // `-1`).
        ToolResult {
            content: vec![ContentBlock::Text {
                text: format!("failed to run privileged command: {error}"),
            }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": run.exit_code, "stdout": stdout, "stderr": stderr, "error": error }),
            ),
            is_error: true,
        }
    } else if auth_failed {
        // The suite's auth-failure handling (`tool.ts:225-228`): CLEAR the
        // cached password (a mistyped password must not stick for the TTL —
        // the next `sudo_exec` re-prompts) and report the failure. The
        // two-condition auth signature only ever follows a REAL `sudo` exit,
        // so a code exists to name — no fallback number is invented for it.
        sudo_password.lock().await.remove(sid);
        let code = run
            .exit_code
            .expect("an auth failure has an observed sudo exit code");
        ToolResult {
            content: vec![ContentBlock::Text {
                text: format!("sudo authentication failed (incorrect password) — exit code {code}. The in-memory credential cache was cleared; the user will be re-prompted on the next attempt."),
            }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": run.exit_code, "stdout": stdout, "stderr": stderr, "error": "authentication failed" }),
            ),
            is_error: true,
        }
    } else if run.is_error {
        // The classifier's error verdict (R2/R3/R4/R5 — ADR 0080's truth
        // table, reached through the SHARED `bash_exit_report`). The NOTE is
        // the report: it is preferred over any generic text whenever it
        // exists, because it is the one that knows whether this was a
        // deadline, a signal death, an unknown status, or a command's OWN
        // exit code that merely coincided with a held pipe (R2 — the row
        // whose whole purpose is not to read like a hang). The generic
        // "command failed with exit code N" survives only for a noted-less
        // non-zero exit, which is the classifier's normal-exit row.
        let error = match (&run.note, run.exit_code) {
            (Some(note), _) => note.clone(),
            (None, Some(n)) => format!("command failed with exit code {n}"),
            // An `is_error` with no code AND no note is unreachable by
            // construction (the classifier notes every code-less error); the
            // conservative fallback keeps the arm total honest rather than
            // panicking on an unreachable row.
            (None, None) => "the command's exit status is unknown".to_string(),
        };
        ToolResult {
            content: vec![ContentBlock::Text {
                text: stdout.clone(),
            }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": run.exit_code, "stdout": stdout, "stderr": stderr, "error": error }),
            ),
            is_error: true,
        }
    } else {
        // Success (R1 and the plain exit-0 row): the SCRUBBED stdout VERBATIM
        // (`content[0].text` is what the LLM sees — NOT a summary; `details`
        // is not LLM-visible), plus the classifier's note when it has one.
        // R1 arrives HERE: `timed_out: true`, `is_error: false`, and a note
        // that says the command COMPLETED and forbids re-running it. The
        // render deliberately does not consult `run.timed_out` — a pipe-held
        // completion has it set and is not an error (keying off the flag was
        // the original bug).
        let mut text = stdout.clone();
        if let Some(note) = &run.note {
            // Byte-shaped like `exec_bash`'s note append: the note is a LINE
            // of what the model reads, never a suffix glued onto the last
            // line of output.
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            text.push_str(note);
            text.push('\n');
        }
        ToolResult {
            content: vec![ContentBlock::Text { text }],
            details: Some(
                json!({ "command": command, "reason": reason, "exitCode": run.exit_code, "stdout": stdout, "stderr": stderr }),
            ),
            is_error: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::watch;

    /// An `EventSink` that captures emissions (the test double for
    /// `TauriSink`).
    #[derive(Default)]
    struct InteractiveTestSink(StdMutex<Vec<(String, Value)>>);

    impl EventSink for InteractiveTestSink {
        fn emit(&self, event: &str, payload: Value) {
            self.0.lock().unwrap().push((event.to_string(), payload));
        }
    }

    impl InteractiveTestSink {
        fn events_named(&self, name: &str) -> Vec<Value> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|(n, _)| n == name)
                .map(|(_, p)| p.clone())
                .collect()
        }
    }

    #[test]
    fn interactive_key_carries_the_trailing_slash_prefix() {
        assert_eq!(interactive_key("s1", "r1"), "s1/r1");
        assert_eq!(session_key_prefix("s1"), "s1/");
        // Closing "s1" must not drain "s10"'s pending entry (the same
        // trailing-slash-prefix invariant as `permission`).
        let mut map: HashMap<String, oneshot::Sender<Value>> = HashMap::new();
        let (tx_s1, _rx_s1) = oneshot::channel();
        let (tx_s10, _rx_s10) = oneshot::channel();
        map.insert(interactive_key("s1", "r1"), tx_s1);
        map.insert(interactive_key("s10", "r1"), tx_s10);
        map.retain(|key, _| !key.starts_with(&session_key_prefix("s1")));
        assert!(
            map.contains_key(&interactive_key("s10", "r1")),
            "closing s1 must not drain s10's pending entry"
        );
        assert!(
            !map.contains_key(&interactive_key("s1", "r1")),
            "s1's own pending entry must be drained"
        );
    }

    // -------------------------------------------------------------------
    // `dispatch_subagent` param validation (the review finding: a missing
    // `task` must not dispatch an empty-prompt subagent session; `tools: []`
    // must not produce `--tools ''`).
    // -------------------------------------------------------------------

    /// A missing `task` is rejected (no dispatch is spawned).
    #[test]
    fn dispatch_params_rejects_a_missing_task() {
        assert!(dispatch_params(&json!({})).is_none());
        assert!(dispatch_params(&json!({ "agentName": "fake" })).is_none());
    }

    /// An empty (or blank) `task` is rejected (no dispatch is spawned).
    #[test]
    fn dispatch_params_rejects_an_empty_task() {
        assert!(dispatch_params(&json!({ "task": "" })).is_none());
        assert!(dispatch_params(&json!({ "task": "   " })).is_none());
    }

    /// `tools: []` is treated as `None` (an empty allowlist is malformed, not
    /// an allowlist — it would produce `--tools ''`); a non-empty list is
    /// kept verbatim; a missing `tools` is `None`.
    #[test]
    fn dispatch_params_treats_an_empty_tools_list_as_none() {
        let (task, launch) = dispatch_params(&json!({ "task": "t", "tools": [] })).unwrap();
        assert_eq!(task, "t");
        assert!(
            launch.tools.is_none(),
            "tools: [] is malformed, not an allowlist (it would produce `--tools ''`)"
        );
        let (_, launch) =
            dispatch_params(&json!({ "task": "t", "tools": ["read", "bash"] })).unwrap();
        assert_eq!(
            launch.tools.as_deref(),
            Some(&["read".to_string(), "bash".to_string()][..])
        );
        let (_, launch) = dispatch_params(&json!({ "task": "t" })).unwrap();
        assert!(launch.tools.is_none());
    }

    // -------------------------------------------------------------------
    // `todo_update` / `sudo_exec` (Phase 2, Task 1): the desktop-side
    // handlers (method-aware — the desktop answers them itself; the user
    // answers the `sudo_exec` sub-prompts via the existing
    // `respond_interactive_request`). Driven through a fake `SudoRunner` (no
    // real `sudo` in `cargo test`).
    // -------------------------------------------------------------------

    use std::future::Future;
    use std::time::Instant;

    use crate::agent::todo::{TodoItem, TodoStatus, TodoStore};

    /// A `SudoRunner` for tests: records the call and returns the preset
    /// result (no real `sudo` in `cargo test`).
    #[derive(Default)]
    struct FakeRunner {
        calls: StdMutex<Vec<(Vec<String>, String, Duration)>>,
        result: StdMutex<Option<SudoRun>>,
    }

    impl FakeRunner {
        fn with_result(r: SudoRun) -> Self {
            Self {
                calls: StdMutex::new(Vec::new()),
                result: StdMutex::new(Some(r)),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
        fn last_call(&self) -> Option<(Vec<String>, String, Duration)> {
            self.calls.lock().unwrap().last().cloned()
        }
    }

    impl SudoRunner for FakeRunner {
        fn run(
            &self,
            argv: Vec<String>,
            password: String,
            timeout: Duration,
        ) -> Pin<Box<dyn Future<Output = SudoRun> + Send + 'static>> {
            self.calls
                .lock()
                .unwrap()
                .push((argv.clone(), password.clone(), timeout));
            let r = self.result.lock().unwrap().clone().unwrap_or(SudoRun {
                exit_code: Some(0),
                stdout: String::new(),
                stderr: String::new(),
                timed_out: false,
                is_error: false,
                note: None,
                error: None,
            });
            Box::pin(async move { r })
        }
    }

    /// Wait (up to ~2 s) until `key` appears in `pending_sudo`.
    async fn wait_for_sudo_key(pending: &PendingSudo, key: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if pending.lock().await.contains_key(key) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        false
    }

    /// Resolve a pending sudo sub-prompt (mirrors the manager's
    /// `respond_interactive_request` lookup — the key is `"{sid}/{id}:{phase}"`).
    async fn respond_sudo(pending: &PendingSudo, id: &str, phase: &str, value: Value) -> bool {
        let key = interactive_key("sid1", &format!("{id}:{phase}"));
        pending
            .lock()
            .await
            .remove(&key)
            .map(|tx| tx.send(value))
            .is_some()
    }

    /// Serialize a `ToolResult` the way the interactive response envelope
    /// does (`content` + `details` (omitted when absent) + `isError` —
    /// present ONLY on a failure; a success omits it) — the test
    /// helpers build the response envelope around this.
    fn tool_result_response_value(result: &ToolResult) -> Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "content".into(),
            serde_json::to_value(&result.content).unwrap_or(Value::Array(Vec::new())),
        );
        if let Some(details) = &result.details {
            obj.insert("details".into(), details.clone());
        }
        if result.is_error {
            obj.insert("isError".into(), Value::Bool(true));
        }
        Value::Object(obj)
    }

    /// Run the `todo_apply` core inline (it is fast — no user
    /// interaction) and return the response envelope it would write
    /// (`{type, id, result}` — the `ToolResult` serialized through
    /// [`tool_result_response_value`]).
    async fn run_todo(params: Value, store: Arc<TodoStore>, sink: Arc<dyn EventSink>) -> Value {
        let cancel = CancellationToken::new();
        let result = todo_apply(&store, "sid1", "main", &params, &sink, &cancel).await;
        json!({
            "type": "response",
            "id": "id-t",
            "result": tool_result_response_value(&result),
        })
    }

    /// Run the `sudo_run_flow` core on a worker task (the core may block on
    /// a user sub-prompt) and return the response envelope it would write
    /// (a `Value::Null` sentinel when the session close wins the race —
    /// the core is dropped, the write is skipped). The close flag is
    /// wired into the core's `cancel` token (a session close cancels the
    /// in-flight core — the native `AgentLoop` passes its `cancel` token
    /// directly).
    async fn run_sudo(
        id: &str,
        params: Value,
        runner: Arc<dyn SudoRunner>,
        pending_sudo: PendingSudo,
        sudo_password: Arc<Mutex<HashMap<String, CachedPassword>>>,
        close_tx: Arc<watch::Sender<bool>>,
        sink: Arc<dyn EventSink>,
    ) -> tokio::task::JoinHandle<Value> {
        let id = id.to_string();
        tokio::spawn(async move {
            let cancel = CancellationToken::new();
            let core = sudo_run_flow(
                "sid1",
                &id,
                "main",
                &params,
                &sink,
                &runner,
                &pending_sudo,
                &sudo_password,
                &cancel,
            );
            let mut close_rx = close_tx.subscribe();
            tokio::select! {
                result = core => {
                    json!({
                        "type": "response",
                        "id": id,
                        "result": tool_result_response_value(&result),
                    })
                }
                _ = close_rx.wait_for(|v| *v) => {
                    // A session close wins the race — the core is dropped
                    // (its `SudoPromptCleanup` guard removes the in-flight
                    // sub-prompt entries) and NO response is written.
                    cancel.cancel();
                    Value::Null
                }
            }
        })
    }

    /// The shared sudo-test fixtures (a fresh pending map / password cache
    /// / close flag / capturing sink).
    async fn sudo_fixtures(
        runner: Arc<FakeRunner>,
    ) -> (
        PendingSudo,
        Arc<Mutex<HashMap<String, CachedPassword>>>,
        Arc<watch::Sender<bool>>,
        Arc<InteractiveTestSink>,
    ) {
        let pending_sudo: PendingSudo = Arc::new(Mutex::new(HashMap::new()));
        let sudo_password: Arc<Mutex<HashMap<String, CachedPassword>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (close_tx, _close_rx) = watch::channel(false);
        let sink = Arc::new(InteractiveTestSink::default());
        let _ = &runner;
        (pending_sudo, sudo_password, Arc::new(close_tx), sink)
    }

    #[tokio::test]
    async fn todo_update_write_emits_the_push_and_returns_the_stored_todos() {
        let store = Arc::new(TodoStore::new());
        let sink = Arc::new(InteractiveTestSink::default());
        let params = json!({
            "operation": "write",
            "todoList": [
                { "content": "a", "status": "pending" },
                { "content": "b", "status": "in_progress", "description": "d" },
                { "content": "c", "status": "completed" }
            ]
        });
        let response = run_todo(params, store.clone(), sink.clone()).await;
        assert_eq!(response["type"], "response");
        assert_eq!(response["id"], "id-t");
        let result = &response["result"];
        // The suite's write text VERBATIM (`tool.ts:138`).
        assert_eq!(
            result["content"][0]["text"],
            "Todos have been modified all. 1/3 completed. Ensure that you continue to use the todo list to track your progress. Please proceed with the current tasks if applicable."
        );
        assert_eq!(result["details"]["operation"], "write");
        assert_eq!(result["details"]["todos"][0]["content"], "a");
        assert_eq!(result["details"]["todos"][1]["description"], "d");
        assert!(result.get("isError").is_none());
        assert_eq!(store.get("sid1").len(), 3);

        // The `todos_update` push (the EXISTING `useInteractive.applyTodoUpdate`
        // shape — `sessionId` MANDATORY, `payload.{source,todos}`).
        let events = sink.events_named("interactive-event");
        assert_eq!(events.len(), 1, "one todos_update push");
        assert_eq!(events[0]["sessionId"], "sid1");
        assert_eq!(events[0]["event"], "todos_update");
        assert_eq!(events[0]["payload"]["source"], "main");
        assert_eq!(events[0]["payload"]["todos"][0]["content"], "a");
        assert!(
            events[0]["payload"]["todos"][0]
                .get("description")
                .is_none(),
            "an absent description is omitted in the push too"
        );
    }

    #[tokio::test]
    async fn todo_update_write_without_a_todo_list_gets_the_error_result() {
        let store = Arc::new(TodoStore::new());
        store.set(
            "sid1",
            vec![TodoItem {
                content: "cur".to_string(),
                status: TodoStatus::Pending,
                description: None,
            }],
        );
        let sink = Arc::new(InteractiveTestSink::default());
        let response = run_todo(json!({ "operation": "write" }), store.clone(), sink.clone()).await;
        let result = &response["result"];
        // The suite's text + flag (`tool.ts:89-94`).
        assert_eq!(
            result["content"][0]["text"],
            "Error: todoList is required for write operation."
        );
        assert_eq!(result["details"]["error"], "todoList required");
        assert_eq!(result["details"]["todos"][0]["content"], "cur");
        assert_eq!(result["isError"], true);
        assert!(
            sink.events_named("interactive-event").is_empty(),
            "no push on a failed write"
        );
    }

    #[tokio::test]
    async fn todo_update_write_with_an_empty_content_item_fails_validation() {
        let store = Arc::new(TodoStore::new());
        let sink = Arc::new(InteractiveTestSink::default());
        let response = run_todo(
            json!({
                "operation": "write",
                "todoList": [ { "content": "   ", "status": "pending" } ]
            }),
            store.clone(),
            sink.clone(),
        )
        .await;
        let result = &response["result"];
        // The suite's validation text (`tool.ts:84-93`).
        assert_eq!(
            result["content"][0]["text"],
            "Validation failed:\n  - Item 1: missing or invalid 'content'"
        );
        assert_eq!(
            result["details"]["error"],
            "Item 1: missing or invalid 'content'"
        );
        assert_eq!(result["isError"], true);
        assert!(
            store.get("sid1").is_empty(),
            "a failed write stores nothing"
        );
    }

    #[tokio::test]
    async fn todo_update_read_returns_the_stored_todos() {
        let store = Arc::new(TodoStore::new());
        let items = vec![
            TodoItem {
                content: "a".to_string(),
                status: TodoStatus::Pending,
                description: None,
            },
            TodoItem {
                content: "b".to_string(),
                status: TodoStatus::Completed,
                description: Some("x".to_string()),
            },
        ];
        store.set("sid1", items.clone());
        let sink = Arc::new(InteractiveTestSink::default());
        let response = run_todo(json!({ "operation": "read" }), store, sink.clone()).await;
        let result = &response["result"];
        // The suite's read text: `JSON.stringify(todos, null, 2)`.
        assert_eq!(
            result["content"][0]["text"],
            serde_json::to_string_pretty(&items).unwrap()
        );
        assert_eq!(result["details"]["operation"], "read");
        assert_eq!(result["details"]["todos"][0]["content"], "a");
        assert!(result.get("isError").is_none());

        // An empty store gets the suite's "No todos" text.
        let response = run_todo(
            json!({ "operation": "read" }),
            Arc::new(TodoStore::new()),
            sink,
        )
        .await;
        assert_eq!(
            response["result"]["content"][0]["text"],
            "No todos. Use write operation to create a todo list."
        );
    }

    #[tokio::test]
    async fn sudo_exec_with_an_empty_command_gets_the_error_result_without_a_run() {
        let runner = Arc::new(FakeRunner::default());
        let (pending_sudo, _password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let response = run_sudo(
            "id1",
            json!({ "command": "  ", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            _password,
            close_tx,
            sink.clone(),
        )
        .await
        .await
        .unwrap();
        let result = &response["result"];
        // The suite's text + details (`tool.ts:404-411`).
        assert_eq!(
            result["content"][0]["text"],
            "The command is empty — provide the exact command to run with elevated privileges."
        );
        assert_eq!(result["details"]["error"], "empty command");
        // A NON-RUN reports no exit code (ADR 0080's amendment): the bash
        // missing-command analogue is `ToolResult::fail(…, None)`, so the
        // wire says `null` — the fabricated `-1` is deleted.
        assert!(
            result["details"]["exitCode"].is_null(),
            "a rejection that never ran a command reports NO exit code: {:?}",
            result["details"]
        );
        assert_eq!(result["isError"], true);
        assert_eq!(runner.call_count(), 0, "no runner call");
        assert!(
            sink.events_named("interactive-request").is_empty(),
            "no sub-prompt was emitted"
        );
        assert!(pending_sudo.lock().await.is_empty());
    }

    #[tokio::test]
    async fn sudo_exec_without_confirmation_does_not_prompt_or_run() {
        let runner = Arc::new(FakeRunner::default());
        let (pending_sudo, _password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            _password,
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(
            wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await,
            "the confirm sub-prompt is registered before the event"
        );
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": false })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        // The suite's text + details (`tool.ts:429-434`).
        assert_eq!(
            result["content"][0]["text"],
            "Command not confirmed — not executed, and no password was requested."
        );
        assert_eq!(result["details"]["error"], "command not confirmed");
        // A cancel that never ran a command reports NO exit code (ADR 0080's
        // amendment, Decision 2 — this literal was the reviewer's untabled
        // `exitCode: -1`, now `null` like the bash missing-command shape).
        assert!(
            result["details"]["exitCode"].is_null(),
            "a non-run reports no exit code, never a fabricated -1: {:?}",
            result["details"]
        );
        assert_eq!(result["isError"], true);
        assert_eq!(runner.call_count(), 0, "no runner call");
        assert!(
            !pending_sudo.lock().await.contains_key("sid1/id1:password"),
            "no password prompt after a cancel"
        );
        assert!(
            pending_sudo.lock().await.is_empty(),
            "the confirm entry was removed on every exit path"
        );
        // The confirm event carries the FULL modal payload (`SudoConfirmModal`
        // reads `request.method` + `params.{command,reason}` and echoes the
        // `requestId` VERBATIM into `respondInteractiveRequest`).
        let reqs = sink.events_named("interactive-request");
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0]["sessionId"], "sid1");
        assert_eq!(reqs[0]["requestId"], "id1:confirm");
        assert_eq!(reqs[0]["method"], "confirm");
        assert_eq!(reqs[0]["source"], "main");
        assert_eq!(reqs[0]["params"]["command"], "apt update");
        assert_eq!(reqs[0]["params"]["reason"], "r");
    }

    #[tokio::test]
    async fn sudo_exec_with_a_confirmed_password_runs_and_scrubs_the_secret() {
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(0),
            stdout: "out line\nhunter2\nthird".to_string(),
            stderr: "prompt\nhunter2".to_string(),
            timed_out: false,
            is_error: false,
            note: None,
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "need it" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "hunter2" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        // The suite's result: the SCRUBBED stdout VERBATIM (the LLM sees it);
        // the password is scrubbed PER STREAM.
        assert_eq!(result["content"][0]["text"], "out line\n[redacted]\nthird");
        assert_eq!(result["details"]["stdout"], "out line\n[redacted]\nthird");
        assert_eq!(result["details"]["stderr"], "prompt\n[redacted]");
        assert_eq!(result["details"]["exitCode"], 0);
        assert_eq!(result["details"]["command"], "apt update");
        assert_eq!(result["details"]["reason"], "need it");
        assert!(result.get("isError").is_none());
        // The runner got `buildSudoArgv` (quoted-arg split, leading-`sudo`
        // strip, the `--` options terminator, NO `-p`) + the password + the
        // 120 s default timeout.
        assert_eq!(runner.call_count(), 1);
        let (argv, pw, timeout) = runner.last_call().unwrap();
        assert_eq!(
            argv,
            vec![
                "sudo".to_string(),
                "-S".to_string(),
                "--".to_string(),
                "apt".to_string(),
                "update".to_string()
            ]
        );
        assert_eq!(pw, "hunter2");
        assert_eq!(timeout, Duration::from_millis(120_000));
        // The password was cached (the suite's 15 min TTL) and no pending
        // entry leaked.
        assert!(password.lock().await.contains_key("sid1"));
        let entry = password.lock().await.get("sid1").unwrap().clone();
        assert!(entry.expires_at > std::time::Instant::now());
        assert!(pending_sudo.lock().await.is_empty());
    }

    #[tokio::test]
    async fn sudo_exec_with_an_empty_password_is_a_cancel() {
        let runner = Arc::new(FakeRunner::default());
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        // An EMPTY password is a cancel (the `InteractiveResponseDto` convention).
        assert!(respond_sudo(&pending_sudo, "id1", "password", json!({ "password": "" })).await);
        let response = task.await.unwrap();
        let result = &response["result"];
        // The suite's text + details (`tool.ts:437-447`).
        assert_eq!(
            result["content"][0]["text"],
            "Password entry cancelled — not executed, and nothing was cached."
        );
        assert_eq!(result["details"]["error"], "password entry cancelled");
        // The second previously-untabled non-run literal (`exitCode: -1` →
        // `null`): nothing ran, so no code may be claimed.
        assert!(
            result["details"]["exitCode"].is_null(),
            "a non-run reports no exit code, never a fabricated -1: {:?}",
            result["details"]
        );
        assert_eq!(result["isError"], true);
        assert_eq!(runner.call_count(), 0, "no runner call");
        assert!(
            !password.lock().await.contains_key("sid1"),
            "nothing was cached"
        );
    }

    #[tokio::test]
    async fn sudo_exec_aborts_when_the_session_closes_mid_flow() {
        let runner = Arc::new(FakeRunner::default());
        let (pending_sudo, _password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            _password,
            close_tx.clone(),
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        close_tx.send(true).expect("close flag");
        let response = task.await.unwrap();
        // A session close maps to a cancel — and the response write is
        // SKIPPED (the agent is gone: the close flag wins the race in the
        // `run_sudo` helper): no frame is written (the `Value::Null`
        // sentinel). No run, no password prompt, no leak.
        assert!(response.is_null(), "no response frame on a closed session");
        assert_eq!(runner.call_count(), 0, "no runner call");
        assert!(
            pending_sudo.lock().await.is_empty(),
            "the in-flight sub-prompt entry was removed"
        );
    }

    #[tokio::test]
    async fn sudo_exec_auth_failure_clears_the_cached_password() {
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(1),
            stdout: String::new(),
            // The suite's EXACT two-condition auth-failure signature
            // (`tool.ts:225-228`): the sudo-prompt precondition + the
            // "incorrect password" marker.
            stderr: "[sudo] password for daniel\n3 incorrect password attempts".to_string(),
            timed_out: false,
            is_error: true,
            note: None,
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "wrong" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        // The suite's text (`tool.ts` auth-failure branch).
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("sudo authentication failed (incorrect password) — exit code 1."),
            "got: {}",
            result["content"][0]["text"]
        );
        assert_eq!(result["details"]["error"], "authentication failed");
        assert_eq!(result["isError"], true);
        // A mistyped password must NOT stick for the TTL.
        assert!(
            !password.lock().await.contains_key("sid1"),
            "the cached password was cleared on an auth failure"
        );
    }

    #[tokio::test]
    async fn a_second_sudo_exec_within_the_ttl_skips_the_password_prompt() {
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(0),
            stdout: "ok".to_string(),
            stderr: String::new(),
            timed_out: false,
            is_error: false,
            note: None,
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        // First run: confirm + password (caches "pw1").
        let task = run_sudo(
            "id1",
            json!({ "command": "ls", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx.clone(),
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw1" })
            )
            .await
        );
        task.await.unwrap();
        assert!(
            password.lock().await.contains_key("sid1"),
            "the first run cached the password"
        );

        // Second run (a fresh id): the confirm STILL fires, the password
        // prompt is SKIPPED (the suite's `credentialCache.get()` hit).
        let task = run_sudo(
            "id2",
            json!({ "command": "ls", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id2:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id2",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        let response = task.await.unwrap();
        assert_eq!(response["result"]["details"]["exitCode"], 0);
        // The run used the CACHED password (no re-prompt).
        assert_eq!(runner.call_count(), 2);
        let (_, pw, _) = runner.last_call().unwrap();
        assert_eq!(pw, "pw1", "the cached password was reused");
        let reqs = sink.events_named("interactive-request");
        // Run 1 emitted confirm + password (the cache was empty); run 2
        // emitted ONLY the confirm (the cache hit skipped the prompt).
        assert_eq!(
            reqs.len(),
            3,
            "run 1: confirm + password; run 2: confirm only (the password prompt was skipped)"
        );
        assert!(reqs.iter().all(|r| r["requestId"] != "id2:password"));
        assert!(pending_sudo.lock().await.is_empty());
    }

    /// The classifier verdict is what the render reports — for BOTH of the
    /// rows a deadline run can land on. Renamed from
    /// `sudo_exec_timeout_returns_the_partial_output_with_the_error`: the old
    /// name promised "the error", and after ADR 0080's amendment a
    /// deadline-reaching run is NOT necessarily an error at all.
    ///
    /// Case 1 (R5 — a genuine hang): the classifier's `DEADLINE` note IS the
    /// report, the code is `null` (never the fabricated `124`), `isError` is
    /// true, the partial stdout survives, and the credential cache is kept.
    /// The retired `{timeout_ms}ms` kill template is gone from the render (it
    /// claimed a fate the run had not shown), so the classifier's note text is
    /// what the model reads.
    ///
    /// Case 2 (R1 — a pipe-held completion): `is_error: false` with
    /// `timed_out: true`, `exitCode: 0`, and the `DO NOT RE-RUN` note in the
    /// text the model reads — the arm-ordering proof that the render no longer
    /// keys off `timed_out`.
    #[tokio::test]
    async fn sudo_exec_timeout_reports_the_classifier_note_and_partial_output() {
        const DEADLINE: &str = "The command timed out and was killed at its deadline.";
        // ── Case 1: R5, a genuine hang (signal death, no code). ──
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: None,
            stdout: "partial".to_string(),
            stderr: String::new(),
            timed_out: true,
            is_error: true,
            note: Some(format!(
                "{DEADLINE} The command died on signal 9 rather than exiting with a code."
            )),
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        // The (partial) stdout survives, and the classifier's note IS the
        // error text — the retired template is not reconstructed from
        // `timeout_ms` anywhere.
        assert_eq!(result["content"][0]["text"], "partial");
        assert_eq!(
            result["details"]["error"],
            "The command timed out and was killed at its deadline. The command died on signal 9 rather than exiting with a code."
        );
        assert_eq!(result["isError"], true);
        assert!(
            result["details"]["exitCode"].is_null(),
            "a signal death names NO code — never the old `124` sentinel: {:?}",
            result["details"]
        );
        // A timeout is NOT an auth failure: the cache is kept.
        assert!(password.lock().await.contains_key("sid1"));

        // ── Case 2: R1, a pipe-held COMPLETION reaching the deadline. ──
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(0),
            stdout: "installed\n".to_string(),
            stderr: String::new(),
            timed_out: true,
            is_error: false,
            note: Some(
                "The command COMPLETED and was observed exiting with code 0. \
                 DO NOT RE-RUN this command — it already succeeded and its side \
                 effects have landed."
                    .to_string(),
            ),
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        assert!(
            result.get("isError").is_none(),
            "R1: a pipe-held exit 0 is a completion, not a timeout (the wire \
             OMITS `isError` when it is false): {result}"
        );
        assert_eq!(result["details"]["exitCode"], 0);
        let text = result["content"][0]["text"]
            .as_str()
            .expect("the model text is a string");
        assert!(
            text.contains("DO NOT RE-RUN"),
            "the note must ride along the success text: {text:?}"
        );
        assert!(
            result["details"]["error"].is_null(),
            "the error details field must carry no \"timed out\" claim for a \
             completion (there is no error at all): {:?}",
            result["details"]
        );
    }

    #[tokio::test]
    async fn sudo_exec_nonzero_exit_is_an_error_and_a_spawn_failure_keeps_the_cache() {
        // Non-zero exit (no auth markers) → `isError` + the failure text.
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(3),
            stdout: "some output".to_string(),
            stderr: "some error".to_string(),
            timed_out: false,
            is_error: true,
            note: None,
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        assert_eq!(result["content"][0]["text"], "some output");
        assert_eq!(
            result["details"]["error"],
            "command failed with exit code 3"
        );
        assert_eq!(result["isError"], true);

        // A spawn failure (`error: Some(…)`) → a clean failure, NOT an auth
        // failure: the cached credential is KEPT.
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
            is_error: false,
            note: None,
            error: Some("failed to spawn sudo: ENOENT".to_string()),
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        assert_eq!(
            result["content"][0]["text"],
            "failed to run privileged command: failed to spawn sudo: ENOENT"
        );
        assert!(
            result["details"]["exitCode"].is_null(),
            "a process-level failure observed no exit code — never a fabricated `-1` \
             (the bash `ToolResult::fail(…, None)` shape): {:?}",
            result["details"]
        );
        assert_eq!(result["isError"], true);
        assert!(
            password.lock().await.contains_key("sid1"),
            "a spawn failure is not an auth failure — the cache is kept"
        );
    }

    /// The render's arm ORDER is load-bearing, and this is the pin (ADR 0080's
    /// amendment). A PROCESS-level failure carries `exit_code: None` with
    /// `is_error` FALSE — the classifier never ran, so the verdict fields are
    /// untouched and `run.error` is the ONLY signal it has. If the
    /// `is_error`-derived arms were evaluated first, this run would fall to the
    /// success arm and a failed spawn would be reported as a completed
    /// privileged command — the exact class of misreport this task exists to
    /// kill, so the `run.error` arm comes FIRST.
    #[tokio::test]
    async fn a_process_level_sudo_failure_renders_before_any_classifier_arm() {
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
            // Deliberately FALSE: a process-level failure never reaches the
            // classifier, so this is the only shape the first arm can fire on.
            is_error: false,
            note: None,
            error: Some("failed to spawn sudo: Permission denied (os error 13)".to_string()),
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        assert_eq!(
            result["isError"], true,
            "the process-level arm fires even though `is_error` is false — a \
             success-shaped verdict must never swallow it: {result}"
        );
        assert_eq!(
            result["details"]["error"], "failed to spawn sudo: Permission denied (os error 13)",
            "the process-level error text is what the model reads"
        );
        assert!(
            result["details"]["exitCode"].is_null(),
            "no code was observed: {:?}",
            result["details"]
        );
        assert!(
            result["content"][0]["text"]
                .as_str()
                .is_some_and(|t| t.starts_with("failed to run privileged command:")),
            "the model text names the process-level failure: {:?}",
            result["content"]
        );
        // A process-level failure is NOT an auth failure: the cache is kept.
        assert!(password.lock().await.contains_key("sid1"));
    }

    /// The classifier's NOTE wins over the generic failure text whenever it
    /// exists — the R2 row is why. R2 is a pipe-held run whose command exited
    /// NON-ZERO on its own: the classifier says so in words ("that is the
    /// command's own exit code, not a deadline") precisely so the model does not
    /// retry a command that genuinely failed. If the render preferred the
    /// generic `"command failed with exit code N"`, the note — the whole point
    /// of R2 — would never reach the model. `bash.rs`'s render is the reference:
    /// the note always rides along the verdict.
    #[tokio::test]
    async fn a_classifier_note_is_preferred_over_the_generic_failure_text() {
        const R2_NOTE: &str = "The command FAILED with exit code 3 — that is the command's own exit code, not a deadline. The run only reached its deadline because a background process still held the output pipe open after the command had exited.";
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(3),
            stdout: "some output".to_string(),
            stderr: String::new(),
            timed_out: true,
            is_error: true,
            note: Some(R2_NOTE.to_string()),
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "pw" })
            )
            .await
        );
        let response = task.await.unwrap();
        let result = &response["result"];
        assert_eq!(result["isError"], true);
        assert_eq!(result["details"]["exitCode"], 3, "the REAL code survives");
        assert_eq!(
            result["details"]["error"], R2_NOTE,
            "the classifier's note IS the report — the generic text must not win"
        );
        assert!(
            password.lock().await.contains_key("sid1"),
            "an R2 failure is not an auth failure — the cache is kept"
        );
    }

    #[tokio::test]
    async fn a_sudo_exec_on_an_already_closed_session_leaks_no_pending_entry() {
        let runner = Arc::new(FakeRunner::default());
        let (pending_sudo, _password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        // The session closed BEFORE the handler ran: the `close_already`
        // pre-check path (the select! is skipped entirely — the entry is
        // still inserted before the pre-check, so it must be removed here
        // too, not only on the select! exit paths). A keep-alive receiver
        // so the pre-close `send` does not `SendError` (the handler's own
        // subscription comes later).
        let _keepalive_rx = close_tx.subscribe();
        close_tx.send(true).expect("close flag");
        let response = run_sudo(
            "id1",
            json!({ "command": "apt update", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            _password,
            close_tx,
            sink.clone(),
        )
        .await
        .await
        .unwrap();
        // The close maps to a cancel — and the response write is SKIPPED
        // (the agent is gone: the write races the close flag, the
        // `handle_todo_update` posture): no frame is written (the
        // `Value::Null` sentinel). No run, no password prompt, no leak.
        assert!(response.is_null(), "no response frame on a closed session");
        assert_eq!(runner.call_count(), 0, "no runner call");
        assert!(
            pending_sudo.lock().await.is_empty(),
            "the confirm entry is removed on the close_already path too"
        );
    }

    #[tokio::test]
    async fn an_expired_cached_password_re_prompts_instead_of_reusing() {
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(0),
            stdout: "ok".to_string(),
            stderr: String::new(),
            timed_out: false,
            is_error: false,
            note: None,
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        // Pre-seed an ALREADY-EXPIRED entry (the `CachedPassword` fields are
        // `pub`, so the test seeds it directly — `expires_at` in the past;
        // the TTL test above covers only the HIT/future path).
        password.lock().await.insert(
            "sid1".to_string(),
            CachedPassword {
                password: "stale".to_string(),
                expires_at: std::time::Instant::now() - Duration::from_secs(1),
            },
        );
        let task = run_sudo(
            "id1",
            json!({ "command": "ls", "reason": "r" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        // The prompt FIRES (the expired entry is dropped, NOT reused — a
        // resumed session must not silently reuse a stale credential).
        assert!(
            wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await,
            "an expired credential re-prompts"
        );
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "fresh" })
            )
            .await
        );
        let response = task.await.unwrap();
        assert_eq!(response["result"]["details"]["exitCode"], 0);
        assert_eq!(runner.call_count(), 1);
        let (_, pw, _) = runner.last_call().unwrap();
        assert_eq!(pw, "fresh", "the expired credential was NOT reused");
        // The fresh password is cached with a new (future) TTL.
        assert!(password.lock().await.contains_key("sid1"));
        assert!(password.lock().await.get("sid1").unwrap().expires_at > std::time::Instant::now());
    }

    /// (finding 3) A `sudo_run_flow` future DROPPED while blocked on its
    /// confirm sub-prompt (the `dispatch_tool` `select!`'s turn-cancel arm)
    /// must clean up: the `pending_sudo` `:confirm` entry is removed (no
    /// leaked dead-oneshot entry) AND a `interactive-request-close` event is
    /// emitted (the UI closes the modal — pre-fix the modal stayed open
    /// with no pending response, and a late answer got `Ok(true)` with the
    /// send silently failing). The `SudoPromptCleanup` drop guard does it.
    #[tokio::test]
    async fn a_dropped_sudo_flow_cleans_up_its_pending_entry_and_closes_the_modal() {
        let capturing = Arc::new(InteractiveTestSink::default());
        let pending_sudo: PendingSudo = Arc::new(Mutex::new(HashMap::new()));
        let sudo_password: Arc<Mutex<HashMap<String, CachedPassword>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let runner: Arc<dyn SudoRunner> = Arc::new(FakeRunner::default());
        let cancel = CancellationToken::new(); // the SESSION teardown token (NOT cancelled)
        let params = json!({ "command": "echo hi", "reason": "test" });
        // SPAWN the flow (it inserts the `:confirm` entry + emits
        // `interactive-request`, then blocks on the confirm sub-prompt — never
        // answered in the test). A `Box::pin` would not poll the future, so
        // the spawn is what actually runs it. The `Arc`s are CLONED for the
        // closure (the originals are kept for the assertions below).
        let sink: Arc<dyn EventSink> = capturing.clone();
        let runner_task = runner.clone();
        let pending_sudo_task = pending_sudo.clone();
        let sudo_password_task = sudo_password.clone();
        let cancel_task = cancel.clone();
        let flow_task = tokio::spawn(async move {
            sudo_run_flow(
                "s1",
                "r1",
                "native",
                &params,
                &sink,
                &runner_task,
                &pending_sudo_task,
                &sudo_password_task,
                &cancel_task,
            )
            .await
        });
        // Let the flow insert the entry (it then blocks on the confirm).
        tokio::time::sleep(Duration::from_millis(100)).await;
        {
            let map = pending_sudo.lock().await;
            assert!(
                map.contains_key("s1/r1:confirm"),
                "the :confirm entry is present while the flow is blocked"
            );
        }
        // DROP the flow mid-confirm (abort the task — the `select!`'s
        // turn-cancel arm; the `SudoPromptCleanup` guard's `Drop` runs).
        flow_task.abort();
        // The modal-closing event is emitted SYNCHRONOUSLY by the guard's
        // `Drop` (assert after a short poll — the abort is a request).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let close_events = capturing.events_named("interactive-request-close");
        assert!(
            close_events.iter().any(|p| p["requestId"] == "r1:confirm"),
            "the interactive-request-close event was emitted (the modal closes)"
        );
        // The entry is removed (the guard's spawned cleanup task) — wait for
        // it to run.
        tokio::time::sleep(Duration::from_millis(100)).await;
        {
            let map = pending_sudo.lock().await;
            assert!(
                !map.contains_key("s1/r1:confirm"),
                "the :confirm entry is removed after the drop (no leaked dead-oneshot)"
            );
        }
    }

    /// (finding 3) A COMPLETED (answered) `sudo_run_flow` must NOT emit a
    /// `interactive-request-close` (the `SudoPromptCleanup` guard's
    /// `interactive-request-close` is only for a genuinely DROPPED flow — the user
    /// already answered both sub-prompts, so a stale close would promise
    /// "close this open modal" for a modal that is already closed). The mirror
    /// of `a_dropped_sudo_flow_cleans_up_its_pending_entry_and_closes_the_modal`
    /// (a dropped flow DOES emit the close; a completed flow does NOT).
    #[tokio::test]
    async fn a_completed_sudo_flow_does_not_emit_a_stale_request_close() {
        // A runner that succeeds (the flow completes — the user answers both
        // sub-prompts, the command runs, and the flow returns a success result).
        let runner = Arc::new(FakeRunner::with_result(SudoRun {
            exit_code: Some(0),
            stdout: "ok\n".to_string(),
            stderr: String::new(),
            timed_out: false,
            is_error: false,
            note: None,
            error: None,
        }));
        let (pending_sudo, password, close_tx, sink) = sudo_fixtures(runner.clone()).await;
        let task = run_sudo(
            "id1",
            json!({ "command": "echo hi", "reason": "test" }),
            runner.clone(),
            pending_sudo.clone(),
            password.clone(),
            close_tx,
            sink.clone(),
        )
        .await;
        // The user answers BOTH sub-prompts (the flow completes — NOT dropped).
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:confirm").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "confirm",
                json!({ "confirmed": true })
            )
            .await
        );
        assert!(wait_for_sudo_key(&pending_sudo, "sid1/id1:password").await);
        assert!(
            respond_sudo(
                &pending_sudo,
                "id1",
                "password",
                json!({ "password": "hunter2" })
            )
            .await
        );
        // The flow COMPLETES (a success result — the command ran).
        let response = task.await.unwrap();
        let result = &response["result"];
        assert!(
            result.get("isError").is_none(),
            "the flow completed (a success result), got {result:?}"
        );
        // The flow completed — the `SudoPromptCleanup` guard's `Drop` must NOT
        // emit a `interactive-request-close` (the user already answered both
        // sub-prompts; a stale close would promise "close this open modal" for
        // a modal that is already closed).
        let close_events = sink.events_named("interactive-request-close");
        assert!(
            close_events.is_empty(),
            "a COMPLETED sudo flow must NOT emit a interactive-request-close (the user already answered both sub-prompts), got {close_events:?}"
        );
        // The `pending_sudo` map is empty (the entries were removed on the
        // normal exit — no leaked dead-oneshot entries).
        assert!(
            pending_sudo.lock().await.is_empty(),
            "the pending_sudo map is empty after the completion (no leaked dead-oneshot)"
        );
    }

    // ---------------------------------------------------------------------
    // (Finding 5) The `details` wire vocabulary — PRODUCER side.
    //
    // `details` is a VALUE vocabulary inside one frame (not an event *name*),
    // which `src/lib/ipc/events.ts` (:21-31) explicitly endorses pinning by
    // tests on each side. This is the Rust half; the TS half is task07b's
    // `src/lib/toolDetailsRegistry.test.ts`. BOTH halves agree on ONE list —
    // the frozen 34-key producer literal below, copied VERBATIM from
    // task07b's plan section "The frozen producer list" (re-derived from the
    // tree ONCE at HEAD 29b16df, vet round 2; re-verified against this tree at
    // HEAD 1483d94 by the harness-loop grounding, indexed-assignment emission
    // included). DO NOT re-derive it: a new emitter is a live wire change to
    // report, never a silent edit of this constant.
    // ---------------------------------------------------------------------

    // THE details wire vocabulary — producer side. 34 keys. FROZEN in
    // task07b's plan §"The frozen producer list" (re-derived from the tree
    // ONCE at HEAD 29b16df, vet round 2); re-verified against this tree at
    // HEAD 1483d94 by the harness-loop grounding (indexed-assignment
    // emission included: web/tools.rs:314,317 add `renderer`/`rendererNote`
    // post-build). Copy VERBATIM — drift between this Rust copy and the TS
    // registry's PRODUCER_KEYS is the guard's own first test subject.
    const PRODUCER_KEYS: [&str; 34] = [
        "bytes",
        "cancelled",
        "command",
        "customInput",
        "description",
        "diff",
        "dir",
        "entries",
        "error",
        "exitCode",
        "extractor",
        "lines",
        "matches",
        "occurrences",
        "operation",
        "path",
        "provider",
        "reason",
        "renderer",
        "rendererNote",
        "responseId",
        "resultCount",
        "results",
        "scope",
        "skill",
        "skills",
        "status",
        "stderr",
        "stdout",
        "title",
        "todos",
        "truncated",
        "url",
        "wordCount",
    ];

    /// One producer `details` shape: `(shape name, the details object,
    /// whether the producing `ToolResult` is a failure)`.
    ///
    /// One fixture per shape GROUP from the frozen register — the shapes the
    /// 39 production emission sites emit (`details: Some(json!({...}))` and
    /// the `ok_text(.., Some(json!(..)))` second arg, across `web/tools.rs`,
    /// `interactive/mod.rs`, `harness/ask.rs`, `tools/exec/bash.rs`,
    /// `tools/exec/fs_tools.rs`, `tools/exec/search.rs`, `tools/exec/skills.rs`).
    /// Each fixture is the WIRE shape, not an invented one: the sudo arms carry
    /// `exitCode: null` on the never-ran paths (ADR 0080's amendment — never a
    /// fabricated `-1`), and `renderer`/`rendererNote` are the OPTIONAL keys
    /// `web/tools.rs` adds by indexed assignment only when a renderer was used.
    fn producer_details_fixtures() -> Vec<(&'static str, Value, bool)> {
        vec![
            // The single-key failure shape (`web/tools.rs`'s seven error arms).
            ("error", json!({ "error": "boom" }), true),
            // The todo shapes (`interactive/mod.rs`'s todo_apply arms).
            (
                "todo read",
                json!({ "operation": "read", "todos": [] }),
                false,
            ),
            (
                "todo write failure",
                json!({
                    "operation": "write",
                    "todos": [{ "content": "a", "status": "pending" }],
                    "error": "todoList required",
                }),
                true,
            ),
            // The skills shapes (`tools/exec/skills.rs`).
            ("skill list", json!({ "skills": 2 }), false),
            (
                "skill dir",
                json!({ "skill": "tester", "scope": "user", "dir": "/tmp/s" }),
                false,
            ),
            (
                "skill file",
                json!({ "skill": "tester", "path": "SKILL.md" }),
                false,
            ),
            // The file shapes (`tools/exec/fs_tools.rs`).
            (
                "read text",
                json!({ "path": "/tmp/a.rs", "lines": 12 }),
                false,
            ),
            (
                "read image",
                json!({ "path": "/tmp/a.png", "bytes": 2048 }),
                false,
            ),
            (
                "edit",
                json!({
                    "path": "/tmp/a.rs",
                    "occurrences": 1,
                    "diff": "--- a\n+++ b\n",
                }),
                false,
            ),
            ("ls", json!({ "entries": 3 }), false),
            // The search shapes (`tools/exec/search.rs`).
            (
                "find / grep",
                json!({ "matches": 40, "truncated": true }),
                false,
            ),
            // The bash shape (`tools/exec/bash.rs`).
            (
                "bash",
                json!({ "exitCode": 0, "truncated": false, "cancelled": false }),
                false,
            ),
            // The sudo shapes (`interactive/mod.rs`'s eight sudo_exec arms):
            // the ran arm (no `error`) and the failed arm.
            (
                "sudo ran",
                json!({
                    "command": "apt update",
                    "reason": "install deps",
                    "exitCode": 0,
                    "stdout": "ok",
                    "stderr": "",
                }),
                false,
            ),
            (
                "sudo rejected (never ran: exitCode is null, not -1)",
                json!({
                    "command": "  ",
                    "reason": "r",
                    "exitCode": Value::Null,
                    "stdout": "",
                    "stderr": "",
                    "error": "empty command",
                }),
                true,
            ),
            // The web shapes (`web/tools.rs`).
            (
                "web_search",
                json!({
                    "responseId": "r1",
                    "provider": "duckduckgo",
                    "resultCount": 1,
                    "results": [{ "title": "t", "url": "u" }],
                }),
                false,
            ),
            (
                "fetch_content (renderer keys present — the indexed-assignment pair)",
                json!({
                    "responseId": "r2",
                    "url": "https://example.com",
                    "title": "Example",
                    "wordCount": 100,
                    "status": 200,
                    "extractor": "readable",
                    "truncated": false,
                    "renderer": "webview",
                    "rendererNote": "note",
                }),
                false,
            ),
            (
                "get_search_content matches",
                json!({ "responseId": "r3", "matches": [] }),
                false,
            ),
            (
                "get_search_content cached",
                json!({ "responseId": "r4" }),
                false,
            ),
            // The ask shape (`harness/ask.rs`) — the two keys the old 32-key
            // list missed, AND the `results` spanning key.
            (
                "ask",
                json!({
                    "results": [{ "question": "q", "selectedOptions": [] }],
                    "customInput": Value::Null,
                    "description": Value::Null,
                }),
                false,
            ),
        ]
    }

    /// (Finding 5 (b)) Every producer `details` shape round-trips through
    /// [`tool_result_response_value`] UNCHANGED — byte-identical `Value`
    /// equality — and the envelope's two documented `isError` behaviours hold
    /// (a success omits it, a failure carries it). This is the gap the
    /// pre-existing todo/sudo/ask round-trip pins never covered: the WEB and
    /// exec shapes had no interactive-module round-trip at all.
    #[test]
    fn details_producer_shapes_round_trip_through_tool_result_response_value() {
        let fixtures = producer_details_fixtures();
        assert!(
            !fixtures.is_empty(),
            "the fixture collection is non-empty (the vacuity guard — a collapsed \
             list must fail, not pass silently)"
        );

        for (name, details, is_error) in &fixtures {
            let result = ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("the `{name}` shape"),
                }],
                details: Some(details.clone()),
                is_error: *is_error,
            };
            let envelope = tool_result_response_value(&result);

            // The `details` value survives VERBATIM (no key renamed, dropped,
            // added, or re-cased on the way through the envelope).
            assert_eq!(
                &envelope["details"], details,
                "the `{name}` details shape round-trips byte-identically through \
                 tool_result_response_value"
            );

            // The envelope's documented `isError` behaviour.
            if *is_error {
                assert_eq!(
                    envelope["isError"],
                    Value::Bool(true),
                    "a failure carries `isError` (`{name}`)"
                );
            } else {
                assert!(
                    envelope.get("isError").is_none(),
                    "a success OMITS `isError` entirely (`{name}`) — not a false, \
                     not a null"
                );
            }

            // The `content` key is always there (the envelope's own shape pin).
            assert!(
                envelope["content"].is_array(),
                "the envelope carries the `content` array (`{name}`)"
            );
        }
    }

    /// (Finding 5 (c)) The fixture union's key set EQUALS the frozen 34-key
    /// producer list — set-equality BOTH ways, so a producer key with no
    /// reader-side fixture and a fixture key that is not on the frozen list
    /// both fail the same test. Plus the repo-census vacuity guards.
    #[test]
    fn details_producer_key_union_equals_the_frozen_list() {
        use std::collections::BTreeSet;

        // Vacuity guard 1: the frozen list itself is non-empty and is the
        // declared width (a collapsed/edited-down array reddens here, not
        // silently).
        assert!(
            !PRODUCER_KEYS.is_empty(),
            "PRODUCER_KEYS is non-empty (the census vacuity guard)"
        );
        assert_eq!(
            PRODUCER_KEYS.len(),
            34,
            "the frozen producer list is the declared 34 keys — a member added or \
             removed here is a WIRE change to report, not a list edit"
        );
        let frozen: BTreeSet<&str> = PRODUCER_KEYS.into_iter().collect();
        assert_eq!(
            frozen.len(),
            PRODUCER_KEYS.len(),
            "the frozen list carries no duplicate key"
        );

        let fixtures = producer_details_fixtures();
        assert!(!fixtures.is_empty(), "the fixture groups are non-empty");

        // The union of every fixture's keys.
        let mut union: BTreeSet<&str> = BTreeSet::new();
        for (name, details, _) in &fixtures {
            let obj = details
                .as_object()
                .unwrap_or_else(|| panic!("the `{name}` fixture is a details OBJECT"));
            assert!(
                !obj.is_empty(),
                "the `{name}` fixture carries at least one key"
            );
            for key in obj.keys() {
                union.insert(key.as_str());
            }
        }

        // Set-equality BOTH ways (the finding's (c): a key on one side and not
        // the other is drift between the two halves' copies of the agreement).
        let missing_from_fixtures: Vec<&&str> =
            frozen.iter().filter(|k| !union.contains(*k)).collect();
        let not_on_the_frozen_list: Vec<&&str> =
            union.iter().filter(|k| !frozen.contains(*k)).collect();
        assert!(
            missing_from_fixtures.is_empty(),
            "every frozen PRODUCER_KEYS key appears in >= 1 fixture group — these \
             do not: {missing_from_fixtures:?} (the cross-coverage guard against \
             silent list <-> fixture drift)"
        );
        assert!(
            not_on_the_frozen_list.is_empty(),
            "no fixture emits a key that is NOT on the frozen producer list — \
             these are unregistered: {not_on_the_frozen_list:?} (a new emitter is a \
             live wire change to REPORT, never a silent edit of PRODUCER_KEYS)"
        );
        assert_eq!(
            union.len(),
            34,
            "the fixture union is exactly the 34-key producer vocabulary, got {}",
            union.len()
        );
    }
}

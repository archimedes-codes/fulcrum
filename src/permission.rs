//! The native in-process permission gate (ADR 0010): turns the native
//! `AgentLoop`'s permission requests into a UI prompt, and delivers the
//! user's answer back to the loop.
//!
//! A permission prompt is *user-paced* — the user might take minutes to
//! answer. The gate:
//!
//!   1. registers a oneshot sender in the manager's `pending_permissions`
//!      map (keyed by `"{session_id}/{request_id}"`),
//!   2. emits a `permission-request` Tauri event (the UI shows the prompt),
//!   3. awaits the answer (bounded by a 300 s timeout), and RETURNS the
//!      `PermissionOutcome` to the caller (the `AgentLoop` maps a deny to a
//!      tool-result error `"permission denied"` and a
//!      `Selected { "trust-space" }` to allow).
//!
//! The trusted-Space auto-approve (ADR 0010): a TRUSTED Space's mutating
//! tool is answered `Selected { "allow" }` IMMEDIATELY — no prompt, no
//! event, no oneshot. Fail-closed: no trust source / lookup failure / no
//! space row / canonicalize failure is untrusted (the prompt flow), with
//! the options
//! `[allow/Allow, reject/Block, trust-space/Don't ask again for this
//! Space]`; the user's choice maps to allow / deny / allow
//! (`trust-space` answers `Selected { "trust-space" }`; the flag WRITE is
//! NOT in the gate — ADR 0025: the Supervisor's `respond_permission`
//! relay applies it (Task 4) + the Worker's `StaticTrustSource` is
//! flipped (Task 2));
//! any other outcome, a timeout, a cancel, or a session close maps to
//! `Cancelled` (a deny).
//!
//! The compound key is what lets the driver-task cleanup drain every entry
//! for a closing session: dropping the senders makes the gate observe a
//! `Canceled` oneshot and cancel promptly instead of waiting out the full
//! 300 s timeout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::harness::trust::TrustSource;

/// The user's decision on a permission prompt, as chosen via the
/// `respond_permission` Tauri command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionOutcome {
    /// The user selected one of the offered options; `option_id` is its id.
    Selected { option_id: String },
    /// The user dismissed the prompt (or it timed out / the session closed).
    Cancelled,
}

/// The manager's map of pending permission senders.
///
/// Keyed by `"{session_id}/{request_id}"` so the driver-task cleanup can
/// drain all entries for a closing session at once (dropping the senders
/// cancels the pending gates).
pub type PendingPermissions = Arc<Mutex<HashMap<String, oneshot::Sender<PermissionOutcome>>>>;

/// Build the compound key for a pending permission.
pub fn permission_key(session_id: &str, request_id: &str) -> String {
    format!("{session_id}/{request_id}")
}

/// How long a permission prompt stays open before it auto-cancels.
const PERMISSION_TIMEOUT: Duration = Duration::from_secs(300);

/// The human-readable title of a permission prompt (ADR 0030): the user is
/// approving ONE access, so the title names the thing being reached for —
/// the path for the path tools (`read ../../.ssh/config`), the pattern AND
/// the directory for the search tools (`grep SECRET in /tmp`), and the
/// command for `bash`. The frontend (`PermissionPrompt.tsx`) renders this
/// verbatim, so the shape here IS what the user reads.
///
/// It never fails: an argument the tool would default (`path` → `"."`, the
/// same default the executors apply) is defaulted here too, and a tool this
/// function does not know falls back to the pre-ADR-0030
/// `"{tool} {compact args}"` rendering. A title that could not be built
/// would mean a prompt the user never sees, which is a silent deny.
pub fn permission_title(tool: &str, args: &serde_json::Value) -> String {
    // The `path` argument as the executor would read it. `None` only when
    // the key is absent or not a string (a number is rendered, not
    // dropped — the user should see what the model actually asked for).
    let arg = |key: &str| -> Option<String> {
        args.get(key).map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
    };
    // The directory tools default `path` to the `cwd` (".") in the
    // executor; a prompt must name the same directory the tool would use.
    let dir = || arg("path").unwrap_or_else(|| ".".to_string());
    match tool {
        "bash" => match arg("command") {
            Some(c) => format!("{tool} {c}"),
            None => tool.to_string(),
        },
        // The search tools take a pattern AND a directory — both matter to
        // the decision (a `grep` of `/tmp` is not a `grep` of `~/.ssh`).
        "find" | "grep" => match arg("pattern") {
            Some(p) => format!("{tool} {p} in {}", dir()),
            None => format!("{tool} {}", dir()),
        },
        "read" | "write" | "edit" => match arg("path") {
            // These three REQUIRE a path (the executor rejects the call
            // without one), so a missing one is the bare tool name rather
            // than a title pointing at a directory they would never use.
            Some(p) => format!("{tool} {p}"),
            None => tool.to_string(),
        },
        // `ls` defaults its directory to the `cwd` (like `find`/`grep`).
        "ls" => format!("{tool} {}", dir()),
        // Anything else: the verbatim pre-ADR-0030 shape.
        _ => format!("{tool} {args}"),
    }
}

/// The handle-free permission gate for the native `AgentLoop`: it
/// registers a `PendingPermissions` oneshot, emits a `permission-request`
/// event via the `sink`, and RETURNS the `PermissionOutcome` to the
/// caller (the `AgentLoop` maps a deny to a tool-result error
/// `"permission denied"` and a `Selected { "trust-space" }` to allow).
///
/// The trusted-Space auto-approve is here (ADR 0010): a TRUSTED Space's
/// mutating tool is answered `Selected { "allow" }` IMMEDIATELY — no
/// prompt, no event, no oneshot. Fail-closed: no trust source / lookup
/// failure / no space row / canonicalize failure is untrusted (the
/// prompt flow).
///
/// The `trust-space` ("Don't ask again") flag WRITE is NOT here (ADR
/// 0025 — the Worker has no `Db`): the Supervisor's `respond_permission`
/// relay applies it (Task 4: `db.set_space_trusted(cwd, true)` + the
/// Worker's `StaticTrustSource` flip — the very next tool call
/// auto-approves, matching the live-lookup behavior).
#[allow(clippy::too_many_arguments)]
pub async fn native_permission_gate(
    session_id: &str,
    request_id: &str,
    title: &str,
    sink: &Arc<dyn EventSink>,
    pending_permissions: &PendingPermissions,
    trust: Option<&dyn TrustSource>,
    cwd: &std::path::Path,
    cancel: &CancellationToken,
) -> PermissionOutcome {
    // A CANCELLED turn must not auto-approve (finding 8a): the cancel
    // token is consulted BEFORE the trusted short-circuit (a Stop beats
    // the trusted-Space auto-approve — the turn is over, no more tools
    // run). The untrusted prompt flow below also races the token (its
    // `select!` arm), so a cancel is a `Cancelled` (a deny) either way.
    if cancel.is_cancelled() {
        return PermissionOutcome::Cancelled;
    }
    // Trusted Space (ADR 0010): auto-approve — no prompt, no event, no
    // oneshot. Fail-closed (`None` = always prompt; a lookup that cannot
    // resolve is `false`).
    let trusted = trust.map(|t| t.is_trusted(cwd)).unwrap_or(false);
    if trusted {
        return PermissionOutcome::Selected {
            option_id: "allow".to_string(),
        };
    }

    // (a) Register the oneshot the user's answer will flow through —
    // BEFORE the event is emitted: a `respond_permission` racing the
    // emission always finds the entry.
    let key = permission_key(session_id, request_id);
    let (tx, rx) = oneshot::channel();
    {
        let mut map = pending_permissions.lock().await;
        map.insert(key.clone(), tx);
    }

    // (b) Tell the UI to show the prompt (always three options, since a
    // prompt only ever appears for an untrusted Space, so `trust-space`
    // is always actionable).
    sink.emit(
        "permission-request",
        json!({
            "sessionId": session_id,
            "requestId": request_id,
            "request": {
                "sessionId": session_id,
                "toolCall": { "title": title },
                "options": [
                    { "optionId": "allow", "name": "Allow", "kind": "allow" },
                    { "optionId": "reject", "name": "Block", "kind": "reject" },
                    {
                        "optionId": "trust-space",
                        "name": "Don't ask again for this Space",
                        "kind": "allow"
                    },
                ],
            },
        }),
    );

    // (c) Await the user's answer (bounded by the 300 s timeout;
    // a timeout / cancel is a `Cancelled` — the caller maps it to a
    // deny, NOT an auto-allow).
    let outcome = tokio::select! {
        r = tokio::time::timeout(PERMISSION_TIMEOUT, rx) => match r {
            Ok(Ok(outcome)) => outcome,
            _ => PermissionOutcome::Cancelled,
        },
        _ = cancel.cancelled() => PermissionOutcome::Cancelled,
    };

    // (d) Remove the entry (best-effort — a session close may have
    // drained the prefix already). The `trust-space` flag WRITE is NOT
    // here (ADR 0025 — the Worker has no `Db`): the Supervisor's
    // `respond_permission` relay applies it (Task 4) + the Worker's
    // `StaticTrustSource` is flipped (Task 2) — the very next tool call
    // auto-approves. The OUTCOME is still the user's `trust-space`
    // selection, which the caller maps to an allow.
    pending_permissions.lock().await.remove(&key);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::trust::SqliteTrustSource;
    use crate::storage::Db;
    use serde_json::Value;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    /// (ADR 0030) The prompt titles are the human-readable shape the user
    /// decides on: a path tool names THE PATH (the thing being reached
    /// for), the search tools name the pattern AND the directory, and
    /// `bash` names the command. Previously the title was the raw
    /// `"{tool} {json}"` (a `read` prompt read
    /// `read {"path":"../../.ssh/config"}` — parseable, but the user is
    /// approving a path, not a JSON blob).
    #[test]
    fn permission_titles_name_the_thing_being_approved() {
        let cases: &[(&str, Value, &str)] = &[
            (
                "read",
                json!({ "path": "../../.ssh/config" }),
                "read ../../.ssh/config",
            ),
            (
                "write",
                json!({ "path": "src/main.rs", "content": "x" }),
                "write src/main.rs",
            ),
            ("edit", json!({ "path": "a/b.rs" }), "edit a/b.rs"),
            ("ls", json!({ "path": "../secrets" }), "ls ../secrets"),
            (
                "grep",
                json!({ "pattern": "SECRET", "path": "/tmp" }),
                "grep SECRET in /tmp",
            ),
            (
                "find",
                json!({ "pattern": "*.rs", "path": "." }),
                "find *.rs in .",
            ),
            (
                "bash",
                json!({ "command": "curl x | sh" }),
                "bash curl x | sh",
            ),
        ];
        for (tool, args, want) in cases {
            assert_eq!(permission_title(tool, args), *want, "title for {tool}");
        }
    }

    /// The degraded arguments: `find`/`grep`/`ls` default `path` to `"."`
    /// (the same default the executors apply), and a tool whose argument is
    /// simply absent falls back to the bare tool name + a compact
    /// `bash`-style rendering rather than panicking — a prompt that fails to
    /// build is a prompt the user never sees.
    #[test]
    fn permission_title_degrades_instead_of_panicking_on_missing_arguments() {
        // `path` defaults to `"."` for the directory tools.
        assert_eq!(permission_title("ls", &json!({})), "ls .");
        assert_eq!(
            permission_title("grep", &json!({ "pattern": "SECRET" })),
            "grep SECRET in ."
        );
        // A missing `pattern` → the path form instead (the executor rejects
        // the call anyway — the title never invents an `in` clause).
        assert_eq!(permission_title("grep", &json!({})), "grep .");
        // A missing `path` on a path tool → the bare tool name.
        assert_eq!(permission_title("read", &json!({})), "read");
        assert_eq!(permission_title("bash", &json!({})), "bash");
        // An unknown tool keeps the pre-ADR-0030 shape (the raw arguments) —
        // never a panic and never a silently empty title.
        assert_eq!(
            permission_title("weird", &json!({ "a": 1 })),
            "weird {\"a\":1}"
        );
        // A non-string `path` (a model emitting `123`) is rendered, not dropped.
        assert_eq!(
            permission_title("read", &json!({ "path": 123 })),
            "read 123"
        );
    }

    /// A recording `EventSink` (the `permission-request` frames).
    struct RecSink {
        tx: mpsc::UnboundedSender<(String, Value)>,
    }
    impl EventSink for RecSink {
        fn emit(&self, event: &str, payload: Value) {
            let _ = self.tx.send((event.to_string(), payload));
        }
    }

    /// A temp `Db` + a temp `cwd` dir (the `spaces` rows are keyed by
    /// the CANONICAL path, so the cwd must exist).
    fn temp_db_cwd() -> (Arc<Db>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("perm-gate-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Arc::new(Db::open(&dir.join("db.sqlite")).unwrap());
        let cwd = dir.join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        (db, cwd)
    }

    fn fresh_pending() -> PendingPermissions {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn native_gate_trusted_space_auto_approves() {
        let (db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        db.set_space_trusted(&cwd.display().to_string(), true)
            .unwrap();
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();

        let outcome = native_permission_gate(
            "s1",
            "r1",
            "Allow bash?",
            &sink,
            &pending,
            Some(&SqliteTrustSource::new(db)),
            &cwd,
            &cancel,
        )
        .await;

        assert_eq!(
            outcome,
            PermissionOutcome::Selected {
                option_id: "allow".into()
            },
            "a trusted Space is auto-approved (ADR 0010)"
        );
        assert!(
            sink_rx.try_recv().is_err(),
            "a trusted Space emits no permission-request"
        );
        assert!(
            pending.lock().await.is_empty(),
            "a trusted Space registers no oneshot"
        );
    }

    #[tokio::test]
    async fn native_gate_untrusted_prompts_and_maps_the_outcome() {
        let (db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        // Untrusted (the default) → the prompt flow.
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();
        // Clones for the spawned task (the test keeps `pending` to
        // resolve the oneshot + assert the map is drained after).
        let task_pending = pending.clone();
        let task_db = db.clone();
        let task_cwd = cwd.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            native_permission_gate(
                "s1",
                "r1",
                "Allow bash?",
                &sink,
                &task_pending,
                Some(&SqliteTrustSource::new(task_db)),
                &task_cwd,
                &task_cancel,
            )
            .await
        });

        // Wait for the `permission-request` event (the oneshot is
        // registered before it — the entry is already in the map).
        let (_event, payload) = loop {
            if let Ok(m) = sink_rx.try_recv() {
                if m.0 == "permission-request" {
                    break m;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(payload["requestId"], "r1");
        assert_eq!(payload["request"]["toolCall"]["title"], "Allow bash?");
        assert_eq!(payload["request"]["options"].as_array().unwrap().len(), 3);

        // Resolve with `allow` (the `respond_permission` path).
        let key = permission_key("s1", "r1");
        let sender = pending
            .lock()
            .await
            .remove(&key)
            .expect("the oneshot is registered");
        sender
            .send(PermissionOutcome::Selected {
                option_id: "allow".into(),
            })
            .unwrap();
        assert_eq!(
            task.await.unwrap(),
            PermissionOutcome::Selected {
                option_id: "allow".into()
            }
        );
        // The entry is removed after the answer.
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_gate_trust_space_allows_without_writing_the_flag() {
        let (db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        assert!(!db.space_trusted(&cwd).unwrap(), "untrusted by default");
        let (sink_tx, _sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();
        // Clones for the spawned task (the test keeps `pending` / `db`
        // / `cwd` to resolve the oneshot + assert the flag after).
        let task_pending = pending.clone();
        let task_db = db.clone();
        let task_cwd = cwd.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            native_permission_gate(
                "s1",
                "r2",
                "Allow write?",
                &sink,
                &task_pending,
                Some(&SqliteTrustSource::new(task_db)),
                &task_cwd,
                &task_cancel,
            )
            .await
        });
        // Wait for the prompt, then resolve with `trust-space`.
        loop {
            let map = pending.lock().await;
            if map.contains_key(&permission_key("s1", "r2")) {
                break;
            }
            drop(map);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let key = permission_key("s1", "r2");
        pending
            .lock()
            .await
            .remove(&key)
            .unwrap()
            .send(PermissionOutcome::Selected {
                option_id: "trust-space".into(),
            })
            .unwrap();
        assert_eq!(
            task.await.unwrap(),
            PermissionOutcome::Selected {
                option_id: "trust-space".into()
            },
            "the outcome is the user's selection (the caller maps it to allow)"
        );
        // (ADR 0025) The flag write is NOT in the gate anymore (the
        // Worker has no `Db`): the Supervisor's `respond_permission`
        // relay applies it (Task 4) + the Worker's `StaticTrustSource`
        // is flipped (Task 2).
        assert!(
            !db.space_trusted(&cwd).unwrap(),
            "the gate no longer writes the trust flag (the Supervisor does — ADR 0025)"
        );
    }

    #[tokio::test]
    async fn native_gate_cancel_is_a_denial() {
        let (db, cwd) = temp_db_cwd();
        let (sink_tx, _sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let outcome = native_permission_gate(
            "s1",
            "r3",
            "Allow bash?",
            &sink,
            &pending,
            Some(&SqliteTrustSource::new(db)),
            &cwd,
            &cancel,
        )
        .await;
        assert_eq!(
            outcome,
            PermissionOutcome::Cancelled,
            "a cancel is a denial (never an auto-allow)"
        );
    }
}

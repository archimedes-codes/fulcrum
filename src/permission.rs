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

use crate::agent::harness::trust::TrustSource;
use crate::agent::session::EventSink;

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

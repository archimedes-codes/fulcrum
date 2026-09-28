//! The permission bridge: turns the agent's `extension_ui_request` dialog
//! into a UI prompt, and delivers the user's answer back to the agent.
//!
//! The SDK runs every handler callback on a single event-loop task, and while a
//! handler is running no new messages are processed. A permission prompt is
//! *user-paced* — the user might take minutes to answer — so the handler must
//! **not** await it. Instead the handler:
//!
//!   1. registers a oneshot sender in the manager's `pending_permissions` map
//!      (keyed by `"{session_id}/{request_id}"`),
//!   2. emits a `permission-request` Tauri event (the UI shows the prompt), and
//!   3. spawns a task that owns the oneshot receiver, awaits the answer
//!      (bounded by a 300 s timeout), and responds to the agent via
//!      `PiRpcHandle::respond_extension_ui`.
//!
//! The handler then returns immediately.
//!
//! Which dialogs become prompts (and which responses they map to):
//!
//! - `confirm` → a TRUSTED Space (ADR 0010, looked up via the `Db`
//!   `space_trusted` — fail-closed: no db / db error / no space row /
//!   canonicalize failure is untrusted) is answered `confirmed: true`
//!   IMMEDIATELY — no prompt, no event, no oneshot. An UNTRUSTED Space gets
//!   a prompt with the options `[allow/Allow, reject/Block,
//!   trust-space/Don't ask again for this Space]`; the user's choice maps
//!   to `confirmed: true / false / true` (`trust-space` answers
//!   `confirmed: true` FIRST, then sets the flag best-effort — a failed
//!   write is logged and the Space stays untrusted) — any other choice,
//!   a timeout, or a session close maps to `cancelled`.
//! - `select` → a prompt whose options are the request's option labels;
//!   the user's choice maps to the `value` response. `select` is NEVER
//!   auto-answered, trusted or not (the `ask` tool's questions are the
//!   user's to answer).
//! - `input` / `editor` → NO prompt: the desktop cannot collect free-form
//!   input in this shape, so the request is answered `cancelled` IMMEDIATELY
//!   (the agent's `createDialogPromise` resolves `undefined` and the
//!   extension proceeds; an unanswered `input` / `editor` would hang the
//!   agent's event loop, since those requests are awaited by pi).
//! - `notify` / `set_status` / `set_widget` / `set_title` /
//!   `set_editor_text` → IGNORED (fire-and-forget on the pi side — pi does
//!   not register a pending response for them, so there is nothing to
//!   answer and nothing to hang on).
//!
//! The compound key is what lets the driver-task cleanup (Task 2) drain every
//! entry for a closing session: dropping the senders makes the spawned tasks
//! observe a `Canceled` oneshot and cancel promptly instead of waiting out the
//! full 300 s timeout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::agent::rpc::{ExtensionUiRequest, ExtensionUiResponse, PiRpcHandle};
use crate::agent::session::EventSink;
use crate::storage::Db;

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
/// Keyed by `"{session_id}/{request_id}"` so the driver-task cleanup can drain
/// all entries for a closing session at once (dropping the senders cancels the
/// spawned tasks).
pub type PendingPermissions = Arc<Mutex<HashMap<String, oneshot::Sender<PermissionOutcome>>>>;

/// Build the compound key for a pending permission.
pub fn permission_key(session_id: &str, request_id: &str) -> String {
    format!("{session_id}/{request_id}")
}

/// The key prefix of all pending-permission keys that belong to
/// `session_id`. Keys are `"{session_id}/{request_id}"`, so the prefix
/// carries the trailing slash: a session id that is a plain prefix of
/// another ("s1" vs "s10") must not drain the other session's prompts.
pub fn session_key_prefix(session_id: &str) -> String {
    format!("{session_id}/")
}

/// How long a permission prompt stays open before it auto-cancels.
const PERMISSION_TIMEOUT: Duration = Duration::from_secs(300);

/// Handle an incoming `extension_ui_request` without blocking the event
/// loop. See the module docs for the full flow.
pub async fn handle_extension_ui_request(
    session_id: &str,
    req: ExtensionUiRequest,
    handle: &PiRpcHandle,
    sink: &Arc<dyn EventSink>,
    pending_permissions: &PendingPermissions,
    db: Option<&Arc<Db>>,
    cwd: &std::path::Path,
) {
    match req {
        ExtensionUiRequest::Confirm { id, title, .. } => {
            // Trusted Space (ADR 0010): auto-approve — no prompt, no event,
            // no oneshot. Fail-closed: no db / db error / no space row /
            // canonicalize failure = untrusted = today's flow.
            let trusted = match db {
                Some(d) => d.space_trusted(cwd).unwrap_or(false),
                None => false,
            };
            if trusted {
                let handle = handle.clone();
                tokio::spawn(async move {
                    let _ = handle
                        .respond_extension_ui(ExtensionUiResponse::Confirmed {
                            id,
                            confirmed: true,
                        })
                        .await;
                });
                return;
            }
            // Untrusted: today's flow, with the third option (always present
            // — the `db` here is the driver's `trust_db` (fail-closed for
            // main AND subagent Sessions — ADR 0010), so a prompt only ever
            // appears for an untrusted Space and the option is always
            // actionable).
            spawn_permission_waiter(
                session_id,
                id,
                json!({
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
                }),
                ResponseKind::Confirm,
                handle,
                sink,
                pending_permissions,
                db.cloned(),
                cwd.display().to_string(),
            )
            .await;
        }
        ExtensionUiRequest::Select {
            id, title, options, ..
        } => {
            // The options are the request's option labels (the `ask`
            // extension's select dialogs).
            let opts: Vec<Value> = options
                .iter()
                .map(|label| json!({ "optionId": label, "name": label }))
                .collect();
            spawn_permission_waiter(
                session_id,
                id,
                json!({
                    "sessionId": session_id,
                    "toolCall": { "title": title },
                    "options": opts,
                }),
                ResponseKind::Select,
                handle,
                sink,
                pending_permissions,
                db.cloned(),
                cwd.display().to_string(),
            )
            .await;
        }
        // `input` / `editor`: answer `cancelled` IMMEDIATELY (a prompt is
        // not possible in this shape, and an unanswered request would hang
        // the agent — pi awaits these two).
        ExtensionUiRequest::Input { id, .. } | ExtensionUiRequest::Editor { id, .. } => {
            let _ = handle
                .respond_extension_ui(ExtensionUiResponse::Cancelled { id })
                .await;
        }
        // `notify` / `set_status` / `set_widget` / `set_title` /
        // `set_editor_text`: fire-and-forget on the pi side — ignore.
        ExtensionUiRequest::Notify { .. }
        | ExtensionUiRequest::SetStatus { .. }
        | ExtensionUiRequest::SetWidget { .. }
        | ExtensionUiRequest::SetTitle { .. }
        | ExtensionUiRequest::SetEditorText { .. } => {}
    }
}

/// The handle-free permission gate for the native `AgentLoop`
/// (native-agent-harness Task 6, reviewer-corrected Major #14): the
/// existing gate is agent-side (`gate.ts` + `handle_extension_ui_request`),
/// whose response path (`spawn_permission_waiter`) completes via
/// `handle.respond_extension_ui(...)` — coupled to `PiRpcHandle`, which a
/// native `AgentLoop` has none of. This waiter is handle-free: it registers
/// a `PendingPermissions` oneshot, emits a `permission-request` event via
/// the `sink`, and RETURNS the `PermissionOutcome` to the caller (the
/// `AgentLoop` maps a deny to a tool-result error `"permission denied"` and
/// a `Selected { "trust-space" }` to allow — the flag write below mirrors
/// the existing `spawn_permission_waiter` Confirm mapping, `268-288`).
///
/// The trusted-Space auto-approve is REPLICATED here (ADR 0010): the native
/// path has no `handle_extension_ui_request` to do it, so a TRUSTED Space's
/// mutating tool is answered `Selected { "allow" }` IMMEDIATELY — no prompt,
/// no event, no oneshot. Fail-closed: no db / db error / no space row /
/// canonicalize failure is untrusted (the prompt flow).
#[allow(clippy::too_many_arguments)]
pub async fn native_permission_gate(
    session_id: &str,
    request_id: &str,
    title: &str,
    sink: &Arc<dyn EventSink>,
    pending_permissions: &PendingPermissions,
    db: Option<&Arc<Db>>,
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
    // oneshot. Fail-closed (the same rule as `handle_extension_ui_request`).
    let trusted = match db {
        Some(d) => d.space_trusted(cwd).unwrap_or(false),
        None => false,
    };
    if trusted {
        return PermissionOutcome::Selected {
            option_id: "allow".to_string(),
        };
    }

    // (a) Register the oneshot the user's answer will flow through —
    // BEFORE the event is emitted (the same race guard as
    // `spawn_permission_waiter`): a `respond_permission` racing the
    // emission always finds the entry.
    let key = permission_key(session_id, request_id);
    let (tx, rx) = oneshot::channel();
    {
        let mut map = pending_permissions.lock().await;
        map.insert(key.clone(), tx);
    }

    // (b) Tell the UI to show the prompt (the same options as the
    // external `confirm` flow — always three, since `db` here is the
    // trust lookup and a prompt only ever appears for an untrusted
    // Space, so `trust-space` is always actionable).
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

    // (c) Await the user's answer (bounded by the SAME 300 s timeout;
    // a timeout / cancel is a `Cancelled` — the caller maps it to a
    // deny, NOT an auto-allow).
    let outcome = tokio::select! {
        r = tokio::time::timeout(PERMISSION_TIMEOUT, rx) => match r {
            Ok(Ok(outcome)) => outcome,
            _ => PermissionOutcome::Cancelled,
        },
        _ = cancel.cancelled() => PermissionOutcome::Cancelled,
    };

    // (d) `trust-space`: the flag write is best-effort and happens BEFORE
    // the outcome is returned (a failed write is logged and the Space
    // stays untrusted — the next call prompts again; the outcome is
    // still the user's `trust-space` selection, which the caller maps to
    // an allow). Mirrors the existing `spawn_permission_waiter` Confirm
    // mapping.
    if let PermissionOutcome::Selected { option_id } = &outcome {
        if option_id == "trust-space" {
            if let Some(d) = db {
                match d.set_space_trusted(&cwd.display().to_string(), true) {
                    Ok(true) => {}
                    Ok(false) => eprintln!(
                        "trust-space: no space row for {}; trust not persisted",
                        cwd.display()
                    ),
                    Err(e) => {
                        eprintln!(
                            "trust-space: failed to set trusted for {}: {e}",
                            cwd.display()
                        )
                    }
                }
            }
        }
    }

    // (e) Remove the entry (best-effort — a session close may have
    // drained the prefix already).
    pending_permissions.lock().await.remove(&key);
    outcome
}

/// The response shape a pending dialog maps to (decided by the request
/// method: `confirm` frames answer `confirmed`, `select` frames answer
/// `value`).
#[derive(Clone, Copy)]
enum ResponseKind {
    Confirm,
    Select,
}

/// Register the oneshot the user's answer will flow through, emit the
/// `permission-request` event, and spawn the waiter. See the module docs for
/// the full flow.
#[allow(clippy::too_many_arguments)]
async fn spawn_permission_waiter(
    session_id: &str,
    request_id: String,
    request_payload: Value,
    kind: ResponseKind,
    handle: &PiRpcHandle,
    sink: &Arc<dyn EventSink>,
    pending_permissions: &PendingPermissions,
    db: Option<Arc<Db>>,
    cwd: String,
) {
    let key = permission_key(session_id, &request_id);

    // (a) Register the oneshot the user's answer will flow through — BEFORE
    // the event is emitted. The UI (and the rpc_flow test) calls
    // `respond_permission` the instant it sees the event; if the entry were
    // not in the map yet, that call would miss and be a no-op, and the agent
    // would block on its response until the 300 s timeout. Registering first
    // makes the lookup total: by the time the event is observed, the entry
    // is already there. (This ordering is a load-dependent microsecond race
    // to test deterministically, so it is verified by running the rpc_flow
    // integration test repeatedly rather than a unit test.)
    let (tx, rx) = oneshot::channel();
    {
        let mut map = pending_permissions.lock().await;
        map.insert(key.clone(), tx);
    }

    // (b) Tell the UI to show the prompt — now that the answer path is in
    // place, so a `respond_permission` racing the emission always finds the
    // entry.
    let payload = json!({
        "sessionId": session_id,
        "requestId": request_id,
        "request": request_payload,
    });
    sink.emit("permission-request", payload);

    // (c) Spawn the waiter. It owns the receiver.
    let key_owned = key.clone();
    let handle = handle.clone();
    let pp = pending_permissions.clone();

    tokio::spawn(async move {
        // Await the user's answer, a timeout, or a Canceled oneshot (the
        // session closed before the user answered).
        let outcome = match tokio::time::timeout(PERMISSION_TIMEOUT, rx).await {
            Ok(Ok(outcome)) => outcome,
            _ => PermissionOutcome::Cancelled,
        };

        // Map to the extension-UI response.
        let response = match (kind, outcome) {
            (ResponseKind::Confirm, PermissionOutcome::Selected { option_id }) => {
                match option_id.as_str() {
                    "allow" => ExtensionUiResponse::Confirmed {
                        id: request_id,
                        confirmed: true,
                    },
                    "reject" => ExtensionUiResponse::Confirmed {
                        id: request_id,
                        confirmed: false,
                    },
                    // Trust this Space: the flag write is best-effort and
                    // happens BEFORE the response is sent (the match arm
                    // runs, then the tail ships the returned value) — so a
                    // failed respond can never lose the trust decision (a
                    // db ERROR is logged and the response still ships; the
                    // Space stays untrusted and the next call prompts again).
                    // (A poisoned-mutex panic inside the db is the
                    // pre-existing db.rs pattern — out of scope here.)
                    "trust-space" => {
                        if let Some(d) = &db {
                            match d.set_space_trusted(&cwd, true) {
                                Ok(true) => {}
                                Ok(false) => eprintln!(
                                    "trust-space: no space row for {cwd}; trust not persisted"
                                ),
                                Err(e) => {
                                    eprintln!("trust-space: failed to set trusted for {cwd}: {e}")
                                }
                            }
                        }
                        ExtensionUiResponse::Confirmed {
                            id: request_id,
                            confirmed: true,
                        }
                    }
                    // A non-allow/reject/trust-space selection is a
                    // dismissal.
                    _ => ExtensionUiResponse::Cancelled { id: request_id },
                }
            }
            (ResponseKind::Confirm, PermissionOutcome::Cancelled) => {
                ExtensionUiResponse::Cancelled { id: request_id }
            }
            (ResponseKind::Select, PermissionOutcome::Selected { option_id }) => {
                ExtensionUiResponse::Value {
                    id: request_id,
                    value: option_id,
                }
            }
            (ResponseKind::Select, PermissionOutcome::Cancelled) => {
                ExtensionUiResponse::Cancelled { id: request_id }
            }
        };

        // Remove the entry (best-effort; it may already have been drained by a
        // session close).
        {
            let mut map = pp.lock().await;
            map.remove(&key_owned);
        }

        // Respond exactly once, best-effort. A spawned task must return
        // Ok(()) on every path: returning Err would tear down the whole
        // connection. Dropping the sender instead would leave the agent's
        // request hanging forever.
        let _ = handle.respond_extension_ui(response).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
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
        db.upsert_space(&cwd.display().to_string()).unwrap();
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
            Some(&db),
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
        db.upsert_space(&cwd.display().to_string()).unwrap();
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
                Some(&task_db),
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
    async fn native_gate_trust_space_allows_and_sets_the_flag() {
        let (db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string()).unwrap();
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
                Some(&task_db),
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
        assert!(
            db.space_trusted(&cwd).unwrap(),
            "trust-space sets the flag best-effort"
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
            Some(&db),
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

    #[test]
    fn key_prefix_carries_the_trailing_slash() {
        assert_eq!(session_key_prefix("s1"), "s1/");
    }

    #[test]
    fn closing_s1_does_not_drain_s10_pending_entries() {
        // Simulate the driver-task cleanup `retain` for closing session
        // "s1" while a prompt from the longer session "s10" is pending.
        let mut map: HashMap<String, oneshot::Sender<PermissionOutcome>> = HashMap::new();
        let (tx_s1, _rx_s1) = oneshot::channel();
        let (tx_s10, _rx_s10) = oneshot::channel();
        map.insert(permission_key("s1", "r1"), tx_s1);
        map.insert(permission_key("s10", "r1"), tx_s10);

        map.retain(|key, _| !key.starts_with(&session_key_prefix("s1")));

        assert!(
            map.contains_key(&permission_key("s10", "r1")),
            "closing s1 must not drain s10's pending entry"
        );
        assert!(
            !map.contains_key(&permission_key("s1", "r1")),
            "s1's own pending entry must be drained"
        );
    }
}

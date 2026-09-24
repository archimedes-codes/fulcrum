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
//! - `confirm` → a prompt with the fixed options `[allow/Allow,
//!   reject/Block]`; the user's choice maps to `confirmed: true / false`
//!   (any other choice — or a timeout / a session close — maps to
//!   `cancelled`).
//! - `select` → a prompt whose options are the request's option labels;
//!   the user's choice maps to the `value` response.
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

use crate::agent::rpc::{ExtensionUiRequest, ExtensionUiResponse, PiRpcHandle};
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
) {
    match req {
        ExtensionUiRequest::Confirm { id, title, .. } => {
            // The fixed options: `[allow/Allow, reject/Block]` (the gate
            // extension's confirm dialogs are yes/no).
            spawn_permission_waiter(
                session_id,
                id,
                json!({
                    "sessionId": session_id,
                    "toolCall": { "title": title },
                    "options": [
                        { "optionId": "allow", "name": "Allow", "kind": "allow" },
                        { "optionId": "reject", "name": "Block", "kind": "reject" },
                    ],
                }),
                ResponseKind::Confirm,
                handle,
                sink,
                pending_permissions,
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
async fn spawn_permission_waiter(
    session_id: &str,
    request_id: String,
    request_payload: Value,
    kind: ResponseKind,
    handle: &PiRpcHandle,
    sink: &Arc<dyn EventSink>,
    pending_permissions: &PendingPermissions,
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
                    // A non-allow/reject selection is a dismissal.
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

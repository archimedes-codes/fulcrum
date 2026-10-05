//! The Worker's `EventSink` implementation (ADR 0025 Task 2): maps the
//! Tauri event names the `AgentLoop` / gate / interactive machinery
//! emits to `Outbound` frames — `"permission-request"` →
//! `PermissionRequest` (the payload's `requestId` as the `id`, the
//! payload VERBATIM), `"interactive-request"` → `InteractiveRequest`
//! (likewise), EVERY other name → `SinkFrame { event, payload }`
//! verbatim (the `interactive-event` todo frames,
//! `interactive-request-close`, `session-update` — the ENTIRE UI
//! contract; the Supervisor re-emits a `SinkFrame` as the same-named
//! Tauri event verbatim).
//!
//! Writes via the unbounded send (synchronous, never fails) to the
//! core's outbound channel — the sink is synchronous (`emit` is not
//! `async`), so nothing is dropped.

use serde_json::Value;
use tokio::sync::mpsc;

use crate::agent::events::EventSink;
use crate::agent::worker::protocol::Outbound;

/// The Worker's `EventSink` (ADR 0025 Task 2): maps the Tauri event
/// names the `AgentLoop` / gate / interactive machinery emits to
/// `Outbound` frames (the Supervisor re-emits a `SinkFrame` as the
/// same-named Tauri event verbatim — the ENTIRE UI contract).
pub struct IpcEventSink {
    outbound: mpsc::UnboundedSender<Outbound>,
}

impl IpcEventSink {
    pub fn new(outbound: mpsc::UnboundedSender<Outbound>) -> Self {
        Self { outbound }
    }
}

impl EventSink for IpcEventSink {
    fn emit(&self, event: &str, payload: Value) {
        // The unbounded send is synchronous and never fails — the sink
        // is synchronous (`emit` is not `async`), so nothing is dropped.
        let frame = match event {
            // The gate's `permission-request` payload carries `requestId`
            // (the `PendingPermissions` map key component) — the `id` is
            // the payload's `requestId`, the payload VERBATIM (the full
            // `request.options` list — the frontend consumes exactly
            // those fields).
            "permission-request" => {
                let id = payload
                    .get("requestId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Outbound::PermissionRequest { id, payload }
            }
            // The `interactive-request` payloads carry the request key
            // (`requestId` — e.g. `"{id}:confirm"` / `"{id}:password"`) —
            // likewise.
            "interactive-request" => {
                let id = payload
                    .get("requestId")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Outbound::InteractiveRequest { id, payload }
            }
            // EVERY other name → `SinkFrame { event, payload }` verbatim
            // (the `interactive-event` todo frames,
            // `interactive-request-close`, `session-update` — the
            // CATCH-ALL for the ENTIRE UI contract).
            _ => Outbound::SinkFrame {
                event: event.to_string(),
                payload,
            },
        };
        let _ = self.outbound.send(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::events::EventSink;
    use serde_json::json;
    use tokio::sync::mpsc;

    /// A test-observed outbound channel + the `IpcEventSink`.
    fn sink() -> (IpcEventSink, mpsc::UnboundedReceiver<Outbound>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (IpcEventSink::new(tx), rx)
    }

    /// A `permission-request` emission → a `PermissionRequest` frame
    /// with the payload VERBATIM (the `requestId` as the `id` — the
    /// `PendingPermissions` map key component — and the full
    /// `request.options` list preserved field-for-field).
    #[test]
    fn permission_request_maps_to_the_frame_with_the_payload_verbatim() {
        let (sink, mut rx) = sink();
        let payload = json!({
            "sessionId": "s1",
            "requestId": "r1",
            "request": {
                "sessionId": "s1",
                "toolCall": { "title": "Allow bash?" },
                "options": [
                    { "optionId": "allow", "name": "Allow", "kind": "allow" },
                    { "optionId": "reject", "name": "Block", "kind": "reject" },
                    { "optionId": "trust-space", "name": "Don't ask again for this Space", "kind": "allow" }
                ]
            }
        });
        sink.emit("permission-request", payload.clone());
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::PermissionRequest { id, payload: back } => {
                assert_eq!(id, "r1", "the id is the payload's requestId");
                assert_eq!(back, payload, "the payload rides VERBATIM");
                assert_eq!(
                    back["request"]["options"].as_array().unwrap().len(),
                    3,
                    "the full options list survives"
                );
                assert_eq!(back["sessionId"], "s1");
                assert_eq!(back["request"]["toolCall"]["title"], "Allow bash?");
            }
            other => panic!("expected `PermissionRequest`, got {other:?}"),
        }
    }

    /// An `interactive-request` emission (a canned `ask` payload) → an
    /// `InteractiveRequest` frame with the payload VERBATIM.
    #[test]
    fn interactive_request_maps_to_the_frame_with_the_payload_verbatim() {
        let (sink, mut rx) = sink();
        let payload = json!({
            "sessionId": "s1",
            "requestId": "r2",
            "method": "ask",
            "source": "agent",
            "toolCallId": "tc1",
            "params": {
                "question": "Which option?",
                "options": [
                    { "label": "A" },
                    { "label": "B" }
                ]
            }
        });
        sink.emit("interactive-request", payload.clone());
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::InteractiveRequest { id, payload: back } => {
                assert_eq!(id, "r2", "the id is the payload's requestId");
                assert_eq!(back, payload, "the payload rides VERBATIM");
                assert_eq!(back["method"], "ask");
                assert_eq!(back["toolCallId"], "tc1");
                assert_eq!(back["params"]["options"].as_array().unwrap().len(), 2);
            }
            other => panic!("expected `InteractiveRequest`, got {other:?}"),
        }
    }

    /// EVERY other event name → a `SinkFrame` with the same event name +
    /// payload verbatim (the `interactive-event` todo frames,
    /// `interactive-request-close`, `session-update` — the frontend
    /// contract).
    #[test]
    fn every_other_event_is_a_sink_frame_verbatim() {
        let (sink, mut rx) = sink();
        let todo_payload = json!({
            "sessionId": "s1",
            "todoId": "t1",
            "items": [
                { "content": "a", "status": "completed" },
                { "content": "b", "status": "in_progress" }
            ]
        });
        sink.emit("interactive-event", todo_payload.clone());
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::SinkFrame {
                event,
                payload: back,
            } => {
                assert_eq!(event, "interactive-event", "the event name is preserved");
                assert_eq!(back, todo_payload, "the payload rides verbatim");
            }
            other => panic!("expected `SinkFrame`, got {other:?}"),
        }
        // The `interactive-request-close` modal cleanup + the
        // `session-update` context-usage display frames — the same
        // catch-all.
        let close_payload = json!({ "sessionId": "s1", "requestId": "r2" });
        sink.emit("interactive-request-close", close_payload.clone());
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::SinkFrame {
                event,
                payload: back,
            } => {
                assert_eq!(event, "interactive-request-close");
                assert_eq!(back, close_payload);
            }
            other => panic!("expected `SinkFrame`, got {other:?}"),
        }
        let session_update = json!({
            "sessionId": "s1",
            "update": { "sessionUpdate": "context_usage_update", "usedTokens": 5, "windowTokens": 100 }
        });
        sink.emit("session-update", session_update.clone());
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::SinkFrame {
                event,
                payload: back,
            } => {
                assert_eq!(event, "session-update");
                assert_eq!(back, session_update);
            }
            other => panic!("expected `SinkFrame`, got {other:?}"),
        }
    }
}

use std::sync::atomic::Ordering;

use serde_json::{json, Value};

use super::AgentLoop;
use crate::agent::events::RpcEvent;
use crate::agent::harness::provider::{ChatRole, FinishReason, ToolCall, Usage};
use crate::agent::harness::store::role_str;
use crate::agent::normalize::{compute_display_rows, normalize};

impl AgentLoop {
    /// Emit the `context_usage_update` frame (session-scoped bookkeeping:
    /// used tokens vs the model's window, read by the UI context bar).
    /// Emitted via the sink, NOT through `normalize` / display-row persistence
    /// (it is not a transcript row; it is a live session gauge, like
    /// `config_option_update`). Also records the usage to the store so a
    /// reload or restart sees the session's last known usage before any turn
    /// has run (the ADR 0038 resume-banner bug: the banner was missing after
    /// reload). The write is best-effort (a DB error must not break the display).
    pub(super) fn emit_context_usage(&self) {
        self.sink.emit(
            "session-update",
            json!({
                "sessionId": self.session_id,
                "update": {
                    "sessionUpdate": "context_usage_update",
                    "usedTokens": self.last_context_tokens,
                    "windowTokens": self.model.context_window,
                },
            }),
        );
        let _ = self.store.record_context_usage(
            &self.session_id,
            self.last_context_tokens,
            u64::from(self.model.context_window),
        );
    }

    /// Emit one `cost_update` frame (issue #87, T09): the provider's cost for
    /// the model call whose `Usage` event is being handled.
    ///
    /// DIRECT on the sink, like `emit_context_usage` — this is an
    /// **interactive-event**, NOT an `RpcEvent` (D5 + the grounding's §8.5): the
    /// Client routes it off the `interactive-event` payload's `event`
    /// discriminant (`App.tsx`), and an unlisted name is dropped, so an
    /// `events.rs` variant would reach no UI and force a `kind()` arm for a
    /// phantom event. The envelope is the `todos_update` shape.
    ///
    /// THE CONTRACT (shared with the frontend task, pinned in both plans):
    /// - **A `None` cost emits NOTHING at all.** Absence is the wire spelling of
    ///   "no figure"; the phantom `$0.00` is created by *emitting a zero*, so the
    ///   only defence is not to emit. Rust never puts `null` on this bus.
    /// - **The value is ONE call's DELTA, never a running total.** The frontend
    ///   store sums the frames, so re-emitting a total would double-count — and
    ///   a Worker re-incarnation / `Resume` (which rebuilds this loop with no
    ///   memory of the old figure) would make that unrecoverable. Hence there is
    ///   deliberately no accumulator on `AgentLoop`.
    /// - **`-0.0` is normalised here.** `c + 0.0` maps `-0.0 -> +0.0` and leaves
    ///   a real `0.0` untouched, so the bus never carries a signed zero (which
    ///   the frontend would render as `$0.00`, since JS treats `-0 === 0`). This
    ///   is NOT the `!= 0.0` filter that would eat a legitimate `Some(0.0)` — a
    ///   real zero is still a figure and still gets its frame.
    pub(super) fn emit_cost_update(&self, cost: Option<f64>) {
        let Some(cost) = cost else { return };
        let cost = cost + 0.0;
        self.sink.emit(
            "interactive-event",
            json!({
                "sessionId": self.session_id,
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": cost, "source": "main" },
            }),
        );
    }

    /// Re-estimate the context size (the prompt-time / post-tool-batch
    /// metric). If the estimate MOVED, emits `context_usage_update`.
    ///
    /// The guard suppresses identical frames: `Compactor::refresh` returns the
    /// same estimate whenever the transcript has not changed, and the
    /// frame also writes the `sessions` row (an IPC round trip -> an
    /// `ensure_session_row` SELECT + an `UPDATE`), once per iteration, on the
    /// COMMON (usage-reporting) path. On a usage-less endpoint the value
    /// always moves (the reply / the tool batch grew the estimate), so the
    /// emissions that matter are untouched.
    pub(super) fn note_context(&mut self) {
        let next = self.compactor.refresh(&self.messages);
        if next != self.last_context_tokens {
            self.last_context_tokens = next;
            self.emit_context_usage();
        }
    }

    /// The `message_end` / `agent_end` assistant message (the pi wire
    /// `AgentMessage` shape — `content` blocks: `text` / `thinking` /
    /// `toolCall` `{ id, name, arguments }`).
    pub(super) fn assistant_message_value(&self, acc: &TurnAccumulator) -> Value {
        let mut content: Vec<Value> = Vec::new();
        if !acc.text.is_empty() {
            content.push(json!({ "type": "text", "text": acc.text }));
        }
        if !acc.thinking.is_empty() {
            content.push(json!({ "type": "thinking", "thinking": acc.thinking }));
        }
        for tc in &acc.tool_calls {
            content.push(json!({
                "type": "toolCall",
                "id": tc.id,
                "name": tc.name,
                "arguments": tc.arguments,
            }));
        }
        json!({ "role": "assistant", "content": content })
    }

    /// Persist the LAST `messages` entry (idempotent upsert at its `seq`).
    pub(super) fn persist_transcript_message(&self) {
        let len = self.messages.len();
        let m = self
            .messages
            .get(len - 1)
            .cloned()
            .expect("a message was just pushed");
        let content_json = serde_json::to_string(&m).unwrap_or_default();
        if let Err(e) = self.store.insert_message(
            &self.session_id,
            (len - 1) as u64,
            role_str(m.role),
            &content_json,
        ) {
            eprintln!("harness: transcript persist failed: {e}");
        }
    }

    /// Persist the LEADING system message (if any) at seq 0 (an idempotent
    /// upsert — the transcript record of the session's system prompt;
    /// ADR 0017). A resume replays it verbatim via `load_transcript`.
    /// The `len == 1` guard makes `prepend_system`'s "call AT MOST ONCE"
    /// contract explicit: a second call on a non-empty transcript is
    /// LOUDLY skipped (the in-memory prepend happened, but the seq-0
    /// row is left untouched — the eprintln is the only trace).
    pub(super) fn persist_system_message(&self) {
        let m = match self.messages.first() {
            Some(m) if self.messages.len() == 1 => m,
            Some(_) => {
                eprintln!(
                    "harness: system message persist skipped (the transcript already has {} messages — the 'call AT MOST ONCE' contract is broken; the seq-0 row is left untouched)",
                    self.messages.len()
                );
                return;
            }
            None => return, // empty transcript — nothing to persist
        };
        if !matches!(m.role, ChatRole::System) {
            return;
        }
        let content_json = serde_json::to_string(m).unwrap_or_default();
        if let Err(e) =
            self.store
                .insert_message(&self.session_id, 0, role_str(m.role), &content_json)
        {
            eprintln!("harness: system message persist failed: {e}");
        }
    }

    /// The tool call as the RENDERER should see it.
    ///
    /// Only `ask` has a payload whose raw form is not renderable: its options
    /// may be bare strings (no `label` to read, so the card draws blank rows)
    /// and its questions may have no `id` (so every per-question draft
    /// collapses into one bucket). Everything else — the tool name, the id, and
    /// every other tool's arguments — is passed through untouched, and the
    /// CALLER's `ToolCall` (the transcript copy replayed to the provider) is
    /// never mutated: the model is told what the model said.
    ///
    /// `ask_flow` normalizes the raw copy it is dispatched with, applying the
    /// SAME pure function, which is why the `interactive-request` event and the
    /// persisted `tool-call` row this feeds are byte-identical rather than
    /// merely similar.
    pub(super) fn normalized_for_display(&self, tc: &ToolCall) -> ToolCall {
        if tc.name != "ask" {
            return tc.clone();
        }
        ToolCall {
            id: tc.id.clone(),
            name: tc.name.clone(),
            arguments: crate::agent::harness::ask::normalize_ask_params(&tc.arguments),
        }
    }

    /// Emit one `RpcEvent`: through the EXISTING `normalize` +
    /// `compute_display_rows` + `Store::persist_display` pipeline (the
    /// FROZEN `session-update` frames via the sink + the display
    /// persistence) AND onto the `events` channel (the raw `RpcEvent`
    /// values — the UNBOUNDED `send` never fails — ADR 0025: nothing is
    /// dropped; a closed channel (the driver gone) is a silent no-op).
    /// The settle is NOT taken from the (lossy) `events` channel: an
    /// `agent_settled` is ALSO written to the settle watch (a watch send
    /// is NEVER dropped — the driver's settle watch fires regardless, so
    /// the turn settle is never lost).
    pub(super) fn emit(&self, ev: RpcEvent) {
        let updates = normalize(
            &ev,
            &mut self.turn_state.lock().unwrap_or_else(|p| p.into_inner()),
        );
        for u in &updates {
            self.sink.emit(
                "session-update",
                json!({ "sessionId": self.session_id, "update": u }),
            );
            let rows =
                compute_display_rows(u, &self.text_acc, &self.tool_state, &self.thought_state);
            let _ = self.store.persist_display(&self.session_id, &rows);
        }
        if matches!(ev, RpcEvent::agent_settled) {
            let n = self.settle_count.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.settle_tx.send(n);
        }
        let _ = self.events.send(ev);
    }
}

/// The per-model-call accumulator (the streamed `ProviderEvent`s -> the
/// assistant message).
#[derive(Default)]
pub(super) struct TurnAccumulator {
    pub(super) text: String,
    pub(super) thinking: String,
    pub(super) tool_calls: Vec<ToolCall>,
    pub(super) usage: Option<Usage>,
    pub(super) finish: Option<FinishReason>,
}

impl TurnAccumulator {
    /// The `message_update` `usage` field (REQUIRED on the wire — an
    /// object; zeros until a `Usage` event arrives).
    pub(super) fn usage_value(&self) -> Value {
        match self.usage {
            Some(u) => json!({
                "inputTokens": u.input_tokens,
                "outputTokens": u.output_tokens,
            }),
            None => json!({ "inputTokens": 0, "outputTokens": 0 }),
        }
    }
}

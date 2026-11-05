use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::{AgentLoop, PermissionContext};
use crate::events::{EventSink, RpcEvent};
use crate::harness::provider::{ChatRole, FinishReason, ToolCall, Usage};
use crate::harness::store::role_str;
use crate::harness::store::Store;
use crate::normalize::{compute_display_rows, normalize, ThoughtState, TurnState};
use crate::tools::ToolEventSink;

/// The borrowed view of ONE emit pipeline's state (B1).
///
/// The extraction of `AgentLoop::emit`'s body needs eight pieces of state,
/// which is past `clippy::too_many_arguments` as free parameters — so they
/// travel as this view instead. BOTH owners of a pipeline hand one over:
/// `AgentLoop::emit` borrows the loop's own fields, and `LoopEventSinkInner`
/// borrows its owned copies. That is what makes "the same pipeline" a type
/// fact rather than a comment.
struct EmitPipeline<'a> {
    session_id: &'a str,
    sink: &'a Arc<dyn EventSink>,
    store: &'a Arc<dyn Store>,
    turn_state: &'a StdMutex<TurnState>,
    text_acc: &'a StdMutex<HashMap<String, String>>,
    tool_state: &'a StdMutex<HashMap<String, Value>>,
    thought_state: &'a StdMutex<ThoughtState>,
    events: &'a mpsc::UnboundedSender<RpcEvent>,
}

impl EmitPipeline<'_> {
    /// Run the shared emit pipeline (B1): `RpcEvent` -> `normalize` -> one
    /// `session-update` sink frame per update -> `compute_display_rows` ->
    /// `Store::persist_display` -> `events.send`. Extracted VERBATIM from
    /// `AgentLoop::emit` so the `LoopEventSink` handle runs the SAME pipeline
    /// the loop does — a nested call's live rows land in the same display
    /// table and the same raw-event channel BY CONSTRUCTION, not by a second
    /// copy of the logic that can drift.
    ///
    /// The `AgentLoop::emit` settle-watch arm deliberately stays OUTSIDE this
    /// method (it is the loop's own supervision concern, and the handle has no
    /// settle channel).
    fn run(&self, ev: RpcEvent) {
        let updates = normalize(
            &ev,
            &mut self.turn_state.lock().unwrap_or_else(|p| p.into_inner()),
        );
        for u in &updates {
            self.sink.emit(
                "session-update",
                json!({ "sessionId": self.session_id, "update": u }),
            );
            let rows = compute_display_rows(u, self.text_acc, self.tool_state, self.thought_state);
            let _ = self.store.persist_display(self.session_id, &rows);
        }
        let _ = self.events.send(ev);
    }
}

/// (B1) The `ToolEventSink` the loop attaches to every ctx it builds: an
/// `Arc` inner so a CLONE of the ctx shares ONE sink (the bridge clones the
/// ctx it was given).
///
/// Owns a FRESH `TurnState` + accumulators — deliberately NOT the loop's:
/// nested-call ids (`<parentCallId>/<n>`) are minted by the bridge, never by
/// this state, and `normalize`'s `tool_execution_update` arm mutates NO turn
/// state (it returns a frame reading only `tool_call_id` / `partial_result`),
/// so a desync between the two states is impossible; `compute_display_rows`'s
/// `tool_call_update` arm works from an EMPTY map (it inserts, then
/// `merge_json`s into the fresh entry).
struct LoopEventSink {
    inner: Arc<LoopEventSinkInner>,
}

struct LoopEventSinkInner {
    session_id: String,
    sink: Arc<dyn EventSink>,
    store: Arc<dyn Store>,
    events: mpsc::UnboundedSender<RpcEvent>,
    turn_state: StdMutex<TurnState>,
    text_acc: StdMutex<HashMap<String, String>>,
    tool_state: StdMutex<HashMap<String, Value>>,
    thought_state: StdMutex<ThoughtState>,
}

impl LoopEventSinkInner {
    /// The same borrowed view `AgentLoop::emit` builds from its own fields —
    /// one `run` body serves both owners.
    fn pipeline(&self) -> EmitPipeline<'_> {
        EmitPipeline {
            session_id: &self.session_id,
            sink: &self.sink,
            store: &self.store,
            turn_state: &self.turn_state,
            text_acc: &self.text_acc,
            tool_state: &self.tool_state,
            thought_state: &self.thought_state,
            events: &self.events,
        }
    }
}

impl ToolEventSink for LoopEventSink {
    fn emit_tool_execution_update(
        &self,
        tool_call_id: String,
        tool_name: String,
        args: Value,
        partial_result: Value,
    ) {
        self.inner.pipeline().run(RpcEvent::tool_execution_update {
            tool_call_id,
            tool_name,
            args,
            partial_result,
        });
    }
}

impl PermissionContext {
    /// (B1) The ONE seeding step: `AgentLoop::new` calls this right after
    /// `PermissionContext::new` (which cannot see the loop's sink/store/events
    /// — they are constructor parameters that do not exist as the bundle is
    /// built). `pub(super)` (`loop.rs` is the `super` here) so `to_tool_ctx`,
    /// defined in `loop.rs`, can read the field it populates.
    pub(super) fn attach_tool_event_sink(
        &mut self,
        session_id: String,
        sink: Arc<dyn EventSink>,
        store: Arc<dyn Store>,
        events: mpsc::UnboundedSender<RpcEvent>,
    ) {
        self.tool_event_sink = Some(Arc::new(LoopEventSink {
            inner: Arc::new(LoopEventSinkInner {
                session_id,
                sink,
                store,
                events,
                turn_state: StdMutex::new(TurnState::default()),
                text_acc: StdMutex::new(HashMap::new()),
                tool_state: StdMutex::new(HashMap::new()),
                thought_state: StdMutex::new(ThoughtState::default()),
            }),
        }));
    }
}

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

    /// Emit one `cost_update` frame (issue #87, T09; the VALUE's meaning is
    /// Fulcrum T2's): the session's CUMULATIVE cost in USD after this call.
    ///
    /// DIRECT on the sink, like `emit_context_usage` — this is an
    /// **interactive-event**, NOT an `RpcEvent` (D5 + the grounding's §8.5): the
    /// Client routes it off the `interactive-event` payload's `event`
    /// discriminant (`App.tsx`), and an unlisted name is dropped, so an
    /// `events.rs` variant would reach no UI and force a `kind()` arm for a
    /// phantom event. The envelope is the `todos_update` shape.
    ///
    /// THE CONTRACT (shared with the frontend task, pinned in both plans):
    /// - **A `None` cost emits NOTHING at all, and adds nothing.** Absence is
    ///   the wire spelling of "no figure"; the phantom `$0.00` is created by
    ///   *emitting a zero*, so the only defence is not to emit. Rust never puts
    ///   `null` on this bus. The accumulator is left untouched in that case, so
    ///   an unpriced call cannot dilute the total the next frame carries.
    /// - **The value is the session's RUNNING TOTAL** (Fulcrum T2, reversing
    ///   ADR 0082's "deltas, not totals"). This method owns the accumulator: it
    ///   adds `cost` to `self.cost_acc` and emits the NEW total. Every consumer
    ///   — the frontend store (`combineSessionCost`, now last-frame-wins) and the
    ///   Supervisor's `Db::record_session_cost` (now an overwrite) — replaces
    ///   rather than sums, so a re-delivered frame cannot double the figure.
    ///   That is what closes ADR 0082's **limit 7**, whose stated blocker was
    ///   "a frame carries no identity to dedupe on": a snapshot needs no
    ///   identity. The `Resume` objection that ADR 0082 used to reject a loop-side
    ///   total is answered by [`AgentLoop::seed_cost`], which reloads the stored
    ///   figure into `cost_acc` when the loop is rebuilt.
    /// - **`-0.0` is normalised here.** `c + 0.0` maps `-0.0 -> +0.0` and leaves
    ///   a real `0.0` untouched, so the bus never carries a signed zero (which
    ///   the frontend would render as `$0.00`, since JS treats `-0 === 0`). This
    ///   is NOT the `!= 0.0` filter that would eat a legitimate `Some(0.0)` — a
    ///   real zero is still a figure and still gets its frame. The normalisation
    ///   is applied to the TOTAL, so a `-0.0` figure cannot leak a signed zero
    ///   through the accumulator either, and a session whose first figure is
    ///   `-0.0` still gets its `+0.0` frame.
    pub(super) fn emit_cost_update(&mut self, cost: Option<f64>) {
        // The frame-existence gate and the accumulator's `None` arm are the
        // SAME decision: no figure reported, nothing to add, no frame.
        let Some(cost) = cost else { return };
        self.cost_acc += cost;
        let total = self.cost_acc + 0.0;
        self.sink.emit(
            "interactive-event",
            json!({
                "sessionId": self.session_id,
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": total, "source": "main" },
            }),
        );
    }

    /// Fold a CHILD's reported cost into THIS loop's total and emit the new
    /// total (Fulcrum T2's one-writer fix; the money-loss defect's cure).
    ///
    /// WHY THE PARENT'S LOOP IS THE ONLY LEGAL WRITER OF ITS OWN KEY. Issue
    /// #105 requires a parent's STORED total to include its children. Since the
    /// frame became a snapshot, two writers on one key cannot both satisfy
    /// that: the Supervisor's roll-up (`subagent_flow.rs`) and this loop's own
    /// frames are both keyed to the parent, and the store OVERWRITES, so the
    /// last frame won — and this loop's accumulator had never seen the child,
    /// so its next frame erased the child's money (parent 0.003 → child 0.002 →
    /// parent 0.006 stored `0.009`, not `0.011`). The delta wire ADDED, so it
    /// hid this; the snapshot wire made it structural. One writer per key is
    /// the only shape that is correct AND idempotent, and only the parent's
    /// loop can be that writer: it is the only party that sees both its own
    /// calls and its children's outcomes.
    ///
    /// WHY IT EMITS AND DOES NOT MERELY ADD: a parent whose last act is a
    /// delegation makes no further priced call, so an add-only fold would keep
    /// the child's money in memory and out of the row — #105's requirement
    /// would hold only for parents that happen to spend again afterwards.
    ///
    /// The gate is the emitter's own: `None` (a child that never reported a
    /// figure, or a dispatch that was cancelled before any outcome arrived)
    /// adds nothing and emits nothing — never a phantom `$0.00` (D5). A child's
    /// `Some(0.0)` IS a figure and still folds. The `source` stays `"main"`
    /// because the frame states the PARENT's total, and the label is inert
    /// anyway (ADR 0041) — a second label on this key would just be a second
    /// writer wearing a different hat.
    pub(super) fn fold_child_cost(&mut self, child_cost: Option<f64>) {
        self.emit_cost_update(child_cost);
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
            arguments: crate::harness::ask::normalize_ask_params(&tc.arguments),
        }
    }

    /// Emit one `RpcEvent`: through the EXISTING `normalize` +
    /// `compute_display_rows` + `Store::persist_display` pipeline (the
    /// FROZEN `session-update` frames via the sink + the display
    /// persistence) AND onto the `events` channel (the raw `RpcEvent`
    /// values — the UNBOUNDED `send` never fails — ADR 0025: nothing is
    /// dropped; a closed channel (the driver gone) is a silent no-op).
    /// The pipeline itself is the shared [`EmitPipeline::run`] (B1) — the SAME
    /// code the `LoopEventSink` handle runs, so a nested call's live rows
    /// reach the identical sink + display table + raw-event channel. Only the
    /// settle-watch arm stays here: it is this loop's supervision concern
    /// (the handle has no settle channel).
    ///
    /// The settle is NOT taken from the (lossy) `events` channel: an
    /// `agent_settled` is ALSO written to the settle watch (a watch send
    /// is NEVER dropped — the driver's settle watch fires regardless, so
    /// the turn settle is never lost).
    ///
    /// (B1) One behavioural consequence of the extraction, accepted: the old
    /// body wrote the settle watch BEFORE `events.send`; now the shared
    /// pipeline sends the raw event first and the watch write follows. Those
    /// are two INDEPENDENT bus endpoints (the unbounded `events` channel vs
    /// the `watch::Sender`) and every consumer in the tree reads one or the
    /// other, never one before the other (`worker/client.rs` +
    /// `worker/core.rs` detect the settle off the channel; the loop tests
    /// await the watch) — so no consumer can observe the swap.
    pub(super) fn emit(&self, ev: RpcEvent) {
        EmitPipeline {
            session_id: &self.session_id,
            sink: &self.sink,
            store: &self.store,
            turn_state: &self.turn_state,
            text_acc: &self.text_acc,
            tool_state: &self.tool_state,
            thought_state: &self.thought_state,
            events: &self.events,
        }
        // `ev.clone()` because the settle arm below matches on `ev` AFTER the
        // move into the pipeline (which sends the raw event on the channel).
        .run(ev.clone());
        if matches!(ev, RpcEvent::agent_settled) {
            let n = self.settle_count.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = self.settle_tx.send(n);
        }
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

/// B1's acceptance test.
///
/// WHY THIS LIVES HERE and not in `src-tauri/tests/skill_tools_boundary.rs`:
/// that file is an INTEGRATION test — it links against `archimedes_lib`'s
/// `pub` surface only, and the production population chain this test must
/// drive (`PermissionContext::new` → `attach_tool_event_sink` →
/// `to_tool_ctx`) is `pub(crate)` / module-private. A child module sees its
/// ancestors' private items: `emit.rs` is `mod emit;` inside the `loop`
/// module, so `PermissionContext` and its private methods are visible here —
/// exactly the shape `loop/tests/mod.rs` already uses. A test that built the
/// sink handle directly instead would pass even with the production wiring
/// broken, which is the failure this placement exists to prevent.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventSink;
    use crate::harness::store::NoopStore;
    use crate::test_support::env_lock;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio_util::sync::CancellationToken;

    /// Records EVERY emission, `(event, payload)` (the `AllEventsSink`
    /// pattern from `loop/tests/support.rs`).
    #[derive(Default)]
    struct TestSink {
        frames: Arc<StdMutex<Vec<(String, Value)>>>,
    }

    impl EventSink for TestSink {
        fn emit(&self, event: &str, payload: Value) {
            self.frames
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((event.to_string(), payload));
        }
    }

    /// B1 acceptance: code holding ONLY a CLONED `&ToolCtx` (the codemode
    /// bridge's shape) produces a `tool_execution_update` that a subscriber
    /// on the EXISTING `events` channel receives, AND the matching
    /// `session-update` frame reaches the `EventSink` — through the SAME
    /// normalize → sink → `persist_display` → `events.send` pipeline
    /// `AgentLoop::emit` runs.
    #[test]
    fn a_cloned_tool_ctx_streams_tool_execution_update_through_the_loop_pipeline() {
        // `PermissionContext::new` reads `$HOME` (boundary::read_roots) —
        // the same env lock every HOME-reading test in the crate takes.
        let _lock = env_lock();
        let space = crate::test_support::ScratchDir::in_env_temp("b1-sink-", "space");
        let cwd = space.path();
        let (events_tx, mut events_rx) = mpsc::unbounded_channel::<RpcEvent>();
        let frames = Arc::new(StdMutex::new(Vec::new()));
        let sink = Arc::new(TestSink {
            frames: frames.clone(),
        });
        let store = Arc::new(NoopStore);

        // THE PRODUCTION POPULATION CHAIN — no hand-built handle:
        let mut permissions = PermissionContext::new(None, cwd);
        permissions.attach_tool_event_sink(
            "s1".to_string(),
            sink.clone(),
            store.clone(),
            events_tx.clone(),
        );
        let ctx = permissions.to_tool_ctx(cwd.to_path_buf(), CancellationToken::new());

        // THE BRIDGE SHAPE: clone the ctx, hold ONLY the clone, emit through it.
        let cloned = ctx.clone();
        // The id is the PARENT codemode call's id (the frozen contract: the
        // bridge supplies the parent id, never a nested `<parent>/<n>` one).
        cloned.emit_tool_execution_update(
            "tc-parent-7",
            "bash",
            json!({ "cmd": "ls" }),
            json!({ "exitCode": 0, "stdout": "src" }),
        );

        // 1) the raw RpcEvent on the EXISTING events channel:
        match events_rx.try_recv() {
            Ok(RpcEvent::tool_execution_update {
                tool_call_id,
                tool_name,
                args,
                partial_result,
            }) => {
                assert_eq!(tool_call_id, "tc-parent-7");
                assert_eq!(tool_name, "bash");
                assert_eq!(args, json!({ "cmd": "ls" }));
                assert_eq!(partial_result, json!({ "exitCode": 0, "stdout": "src" }));
            }
            other => panic!("expected tool_execution_update, got {other:?}"),
        }
        assert!(events_rx.try_recv().is_err(), "no other raw event expected");

        // 2) the matching session-update frame on the EventSink:
        let received = frames.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(received.len(), 1, "one session-update frame");
        assert_eq!(received[0].0, "session-update");
        let u = &received[0].1["update"];
        assert_eq!(u["sessionUpdate"], "tool_call_update");
        assert_eq!(u["toolCallId"], "tc-parent-7");
        assert_eq!(u["rawOutput"], json!({ "exitCode": 0, "stdout": "src" }));
    }
}

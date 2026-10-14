//! The event-normalization layer: map the harness's `RpcEvent` stream onto
//! the FROZEN `session-update` JSON the frontend consumes, and compute the
//! display `messages` rows for persistence (ADR 0011 / ADR 0025).
//!
//! Extracted verbatim from `session.rs` (Task 5 of the god-file
//! decomposition) — a TRUE SIBLING of `session/` so the modules that once
//! imported these items from `session` (the harness loop) now import from
//! here instead.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex as StdMutex;

use serde_json::{json, Value};

use crate::agent::events::RpcEvent;
use crate::agent::harness::store::DisplayRow;

/// A cheap, `'static`-safe handle to the native `AgentLoop` task:
/// `prompt_tx` / `control_tx` clone
/// the loop's channels and `cancel` is the loop's cancellation token. The
/// loop's `RpcEvent` `Receiver` is NOT held here (a `tokio` mpsc `Receiver`
/// is not `Clone`) — `drive_native_session` takes it by move (the driver
/// task consumes it to watch `agent_settled`; the loop has ALREADY run the
/// The subagent-close handle: the subagent cancel path (a subagent session
/// passes it to `drive_native_session`; a main session passes `None`).
/// Per-session thinking-persistence state: the accumulated text of the
/// OPEN segment, the segment's message key (or `None` when the last
/// update was not a continuing thought chunk), and the next segment
/// number. `current` is reset when a new segment starts (a closed
/// segment is never re-appended — see the segmentation rule above).
#[derive(Debug, Default)]
pub(crate) struct ThoughtState {
    current: String,
    open_key: Option<String>,
    next: u32,
    /// The segment indices each messageId actually used, in order. A message's
    /// thought rows are `{id}#{n}` for these `n` — and they do NOT start at 1
    /// for any message but the first, because `next` counts segments for the
    /// life of the loop. A Retraction erases exactly this list: the display
    /// store UPSERTs and has no DELETE, so erasing a key that never existed
    /// writes a row that can never be removed.
    segments: HashMap<String, Vec<u32>>,
}

/// Per-session turn state for the event normalizer.
///
/// `msg_counter` is the `messageId` source: it starts at 0 and is advanced
/// ONLY when a `message_start` carries an `assistant` message (the wire's
/// `message_start` carries ANY `AgentMessage` — user and tool-result
/// messages included — so a role-blind counter would number the first
/// assistant chunk `m2` and make the `get_messages` replay numbering
/// irreproducible). `current_message_id` is `format!("m{}", counter)`.
///
/// `toolcall_args` is the accumulating partial-args buffer per tool-call id
/// (the `toolcall_delta` frames are JSON fragments; `toolcall_end` carries
/// the full `arguments` object and the buffer entry is dropped).
///
/// `announced_tool_calls` is the set of ids a `tool_call` (ANNOUNCE) frame
/// has already been emitted for (a `toolcall_start` with a non-empty
/// `toolName`, or the `toolcall_end` / `tool_execution_start` fallbacks).
/// The first frame for an id MUST be a `tool_call` (with a real `title`) —
/// a `tool_call_update` for an id the frontend never saw would create a
/// message with `title = toolCallId` (e.g. `chatcmpl-tool-…`), and a second
/// `tool_call` frame would APPEND a duplicate row.
#[derive(Debug, Default)]
pub struct TurnState {
    pub msg_counter: u64,
    pub current_message_id: Option<String>,
    pub toolcall_args: HashMap<String, String>,
    pub announced_tool_calls: HashSet<String>,
    /// How many Retractions this turn has announced. The discard line needs a
    /// messageId UNIQUE per discard (ADR 0048) — two lines sharing `"system"`
    /// would weld into one bubble.
    pub retract_seq: u64,
}

/// A one-line `agent_message_chunk` carrying a UNIQUE messageId — for a line
/// that must survive being adjacent to another line. `system_chunk`'s shared
/// `"system"` key APPENDS (that is its point), so a second line on it welds.
fn unique_system_chunk(message_id: &str, text: impl Into<String>) -> Value {
    let text = text.into();
    json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text },
        "messageId": message_id,
    })
}

/// Is this messageId a harness bookkeeping line rather than model output?
/// `"system"` is the shared one-liner key; `"retract:N"` is a discard line
/// (ADR 0048). Anything that must capture ONLY what the model said — the
/// subagent capture, whose text becomes the PARENT's tool result — asks this
/// instead of re-deciding the convention.
pub fn is_bookkeeping_message_id(id: &str) -> bool {
    id == "system" || id.starts_with("retract:")
}

/// A one-line `agent_message_chunk` with the dedicated `"system"` messageId
/// (the bookkeeping one-liners — never mixed into a real message's
/// accumulated text).
fn system_chunk(text: impl Into<String>) -> Value {
    let text = text.into();
    json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text },
        "messageId": "system",
    })
}

/// Shallow-merge `patch` into `base`: non-null fields of `patch` win.
fn merge_json(base: &mut Value, patch: &Value) {
    if let (Some(base), Some(patch)) = (base.as_object_mut(), patch.as_object()) {
        for (k, v) in patch {
            if !v.is_null() {
                base.insert(k.clone(), v.clone());
            }
        }
    }
}

/// Map one pi event onto the FROZEN `session-update` JSON the frontend
/// consumes (the ACP-era envelope shapes — `agent_message_chunk` /
/// `agent_thought_chunk` / `tool_call` / `tool_call_update` /
/// `session_info_update` / `config_option_update`). A pure function (no
/// I/O): the driver feeds it the event + the session's `TurnState` and
/// emits / persists the frames it returns. `pub(crate)` so the native
/// `AgentLoop` (Task 6) runs its `RpcEvent`s through the SAME pipeline
/// (the frontend is unchanged — the FROZEN frames are identical).
///
/// No-frame events are bookkeeping (`agent_start` / `agent_end` /
/// `turn_start` / `turn_end` / `queue_update` / `entry_appended` /
/// `bash_execution_update` / `message_end` / `text_end` / `thinking_end` —
/// the delta stream already delivered the content, and the authoritative
/// `message_end` text is NOT re-emitted) or the turn's resolution signal
/// (`agent_settled` — the driver resolves the pending turn, not a frame).
pub(crate) fn normalize(e: &RpcEvent, st: &mut TurnState) -> Vec<Value> {
    match e {
        // `message_start` carries ANY `AgentMessage` (user / assistant /
        // toolResult): advance the counter ONLY for `assistant` messages
        // (a role-blind counter would number the first assistant chunk
        // `m2` and make the `get_messages` replay numbering irreproducible).
        RpcEvent::message_start { message } => {
            if message.get("role").and_then(Value::as_str) == Some("assistant") {
                st.msg_counter += 1;
                st.current_message_id = Some(format!("m{}", st.msg_counter));
            }
            Vec::new()
        }
        RpcEvent::message_update {
            assistant_message_event: ev,
            ..
        } => {
            let mid = st
                .current_message_id
                .clone()
                .unwrap_or_else(|| "default".to_string());
            match ev.get("type").and_then(Value::as_str) {
                Some("text_delta") => vec![json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": ev.get("delta") },
                    "messageId": mid,
                })],
                Some("thinking_delta") => vec![json!({
                    "sessionUpdate": "agent_thought_chunk",
                    "content": { "type": "text", "text": ev.get("delta") },
                    "messageId": mid,
                })],
                // The `*_end` / `*_start` frames carry the authoritative
                // (already streamed) content — no frame (re-emitting would
                // double the text).
                Some("text_start")
                | Some("text_end")
                | Some("thinking_start")
                | Some("thinking_end") => Vec::new(),
                // ANNOUNCE only when the name is known (a non-empty
                // `toolName` — the OpenAI-compatible streaming delivers
                // `function.name` in a LATER delta, so `toolcall_start`
                // often carries `""`). An empty / missing name (or an
                // empty `id`) defers the announcement to `toolcall_end`
                // (where the name is known) — announcing now would make
                // the frontend display the `toolCallId` (e.g.
                // `chatcmpl-tool-…`) instead of the tool name.
                Some("toolcall_start") => {
                    let Some(id) = ev
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    else {
                        return Vec::new();
                    };
                    let Some(name) = ev
                        .get("toolName")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    else {
                        return Vec::new();
                    };
                    st.announced_tool_calls.insert(id.to_string());
                    vec![json!({
                        "sessionUpdate": "tool_call",
                        "toolCallId": id,
                        "title": name,
                        "status": "in_progress",
                        "rawInput": {},
                    })]
                }
                // Accumulate the partial-args JSON fragments. For an
                // ANNOUNCED id a complete object is sent as `rawInput`,
                // an incomplete one as `partialArgs` (the adapter-era
                // behavior, kept for the streaming tool-call frames). For
                // an UNANNOUNCED id the delta is BUFFER ONLY — no frame:
                // a `tool_call_update` for an id the frontend never saw
                // would create a message with `title = toolCallId` (e.g.
                // `chatcmpl-tool-…`); the announcement comes from
                // `toolcall_end` (or `tool_execution_start`).
                Some("toolcall_delta") => {
                    let Some(id) = ev.get("id").and_then(Value::as_str) else {
                        return Vec::new();
                    };
                    let delta = ev.get("delta").and_then(Value::as_str).unwrap_or_default();
                    st.toolcall_args
                        .entry(id.to_string())
                        .or_default()
                        .push_str(delta);
                    if !st.announced_tool_calls.contains(id) {
                        return Vec::new();
                    }
                    let acc = st.toolcall_args.get(id).unwrap();
                    match serde_json::from_str::<Value>(acc) {
                        Ok(v) => vec![json!({
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": id,
                            "rawInput": v,
                        })],
                        Err(_) => vec![json!({
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": id,
                            "partialArgs": acc,
                        })],
                    }
                }
                // The full `arguments` object (the wire `toolCall` field —
                // `{id, name, arguments}`); clear the partial-args buffer.
                // If the id was ALREADY announced (a `toolcall_start` with a
                // name), this is an update. Otherwise (the NATIVE-HARNESS
                // turn — no `toolcall_start` at all — or a
                // `toolcall_start` with an empty name) THIS is the
                // announcement: a `tool_call` frame with the real `title`
                // (NOT a `tool_call_update`, which has no `title`, so the
                // frontend would fall back to displaying the `toolCallId`,
                // e.g. `chatcmpl-tool-…`). A name that never arrived keeps
                // the legacy update (the degenerate case, no worse than
                // before).
                Some("toolcall_end") => {
                    let Some(tc) = ev.get("toolCall") else {
                        return Vec::new();
                    };
                    let Some(id) = tc
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    else {
                        return Vec::new();
                    };
                    st.toolcall_args.remove(id);
                    let Some(name) = tc
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                    else {
                        return vec![json!({
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": id,
                            "rawInput": tc.get("arguments"),
                            "status": "in_progress",
                        })];
                    };
                    if st.announced_tool_calls.contains(id) {
                        vec![json!({
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": id,
                            "rawInput": tc.get("arguments"),
                            "status": "in_progress",
                        })]
                    } else {
                        st.announced_tool_calls.insert(id.to_string());
                        vec![json!({
                            "sessionUpdate": "tool_call",
                            "toolCallId": id,
                            "title": name,
                            "status": "in_progress",
                            "rawInput": tc.get("arguments"),
                        })]
                    }
                }
                _ => Vec::new(),
            }
        }
        // The native `tool_execution_start` CARRIES the tool name + args
        // (the provider already accumulated them). If the id was ALREADY
        // announced (a `toolcall_start` with a name, or `toolcall_end` —
        // the native-harness turn), this is an UPDATE: the frontend applies
        // `title` / `rawInput` / `status` in place (a second `tool_call`
        // frame would APPEND a duplicate row). If it was NOT announced (no
        // `toolcall_*` frames at all — the defensive fallback), THIS is the
        // announcement: a `tool_call` frame with the real `title` (NOT just
        // a `tool_call_update`, which has no `title`, so the frontend would
        // fall back to displaying the `toolCallId`, e.g. `chatcmpl-tool-…`).
        RpcEvent::tool_execution_start {
            tool_call_id,
            tool_name,
            args,
        } => {
            if st.announced_tool_calls.contains(tool_call_id) {
                // A `null` `title` is a no-op for the frontend (`update.title
                // ?? prev.title` keeps the existing title) — never overwrite
                // a good title with an empty one.
                let title = if tool_name.is_empty() {
                    Value::Null
                } else {
                    Value::String(tool_name.clone())
                };
                vec![json!({
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": tool_call_id,
                    "title": title,
                    "rawInput": args,
                    "status": "in_progress",
                })]
            } else {
                st.announced_tool_calls.insert(tool_call_id.clone());
                vec![json!({
                    "sessionUpdate": "tool_call",
                    "toolCallId": tool_call_id,
                    "title": tool_name,
                    "status": "in_progress",
                    "rawInput": args,
                })]
            }
        }
        // `rawOutput` is the tool's result (the RPC `AgentToolResult`): a live
        // partial while the tool runs, final on `tool_execution_end`. It is
        // consumed by the frontend (the `tool-call` `Message` carries it) AND
        // persisted into the tool-call row via `merge_json` (so a resume
        // restores the summary + output).
        RpcEvent::tool_execution_update {
            tool_call_id,
            partial_result,
            ..
        } => vec![json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": tool_call_id,
            "rawOutput": partial_result,
        })],
        RpcEvent::tool_execution_end {
            tool_call_id,
            result,
            is_error,
            ..
        } => vec![json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": tool_call_id,
            "status": if *is_error { "failed" } else { "completed" },
            "rawOutput": result,
        })],
        RpcEvent::session_info_changed { name } => vec![json!({
            "sessionUpdate": "session_info_update",
            "title": name,
        })],
        // Bookkeeping / de-structured one-liners (the adapter's parity; the
        // strings are stable for tests). They carry a `messageId` of
        // `"system"` (a dedicated key — never mixed into a real message's
        // accumulated text).
        //
        // A compaction emits a SECOND frame beside the one-liner: the
        // `compaction_update` STATE frame ("a compaction is running", and the
        // trigger `reason` — `manual` / `threshold` / `overflow`) the
        // renderer's indicator reads. It has no display row (see
        // `compute_display_rows`' catch-all), so the one-liner stays the only
        // durable transcript trace — the summary itself is a `native_messages`
        // `System` row, never a display row.
        RpcEvent::compaction_start { reason } => vec![
            system_chunk("Compacting context…"),
            json!({
                "sessionUpdate": "compaction_update",
                "active": true,
                "reason": reason,
            }),
        ],
        // A `compaction_end` that carries an `error_message` (ADR 0045: the
        // overflow recovery's actionable line — "not enough older context to
        // compact" / "the summary call failed") prints THAT instead of the
        // success line, never both: these one-liners all share
        // `messageId: "system"`, and the frontend appends same-`messageId`
        // chunks with no separator, so two in one batch weld into one bubble.
        // A successful compaction has no `error_message`, so the pinned
        // "Compaction finished" still ships on the success path.
        RpcEvent::compaction_end {
            reason,
            error_message,
            ..
        } => vec![
            match error_message {
                Some(m) => system_chunk(format!("Compaction failed: {m}")),
                None => system_chunk("Compaction finished"),
            },
            json!({
                "sessionUpdate": "compaction_update",
                "active": false,
                "reason": reason,
            }),
        ],
        // (ADR 0048) An Abandoned attempt's reply is retracted. The id is the one
        // the harness is ON — the retried call REUSES it (`message_start` fires
        // once per assistant message), which is precisely why the discard is a
        // protocol event and not something the reader may infer. Then the line
        // says so, on its own id: reusing `"system"` would weld it onto the
        // "Retrying (attempt 1/3…)" line this path emits immediately before.
        RpcEvent::message_retracted => {
            let mid = st
                .current_message_id
                .clone()
                .unwrap_or_else(|| "default".to_string());
            st.retract_seq += 1;
            vec![
                json!({ "sessionUpdate": "message_retracted", "messageId": mid }),
                unique_system_chunk(
                    &format!("retract:{}", st.retract_seq),
                    "Discarded an incomplete reply",
                ),
            ]
        }
        RpcEvent::auto_retry_start {
            attempt,
            max_attempts,
            ..
        } => vec![system_chunk(format!(
            "Retrying (attempt {attempt}/{max_attempts}…)"
        ))],
        RpcEvent::auto_retry_end { success, .. } => {
            vec![if *success {
                system_chunk("Retry succeeded")
            } else {
                system_chunk("Retry failed")
            }]
        }
        RpcEvent::extension_error { error, .. } => {
            vec![system_chunk(format!("Extension error: {error}"))]
        }
        // Bookkeeping (no frame): the turn's resolution signal
        // (`agent_settled` — the driver resolves the pending turn), the
        // turn / message boundaries, the queue / entry / bash bookkeeping,
        // and unknown event types (permissive — debug-logged by the
        // reader, never an error).
        _ => Vec::new(),
    }
}

/// (the `list_sessions` path — NOT `load_history`, which returns raw
/// `MessageRow`s and never touches `capabilities_json`): an envelope that
/// (a) fails to parse as JSON, or (b) parses but lacks the `loadSession`
/// key (a pre-swap ACP row) is normalized to `loadSession: false` — so a
/// legacy row shows NO Resume button (the history-only banner is a
/// FRONTEND-side decision driven by `capabilities.loadSession`; there is no
/// error-kind matching in the frontend, so the normalized capabilities are
/// the honest path).
pub fn normalize_capabilities(raw: &str) -> Value {
    let v: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return json!({ "loadSession": false }),
    };
    if v.get("loadSession").is_some() {
        v
    } else {
        json!({ "loadSession": false })
    }
}

/// Compute the display `messages` rows for one `session-update` (the
/// FROZEN JSON shapes the normalizer emits) — the PURE row-computation
/// half of the ACP-era persistence (the write half is
/// `Store::persist_display` — ADR 0025: the `SqliteStore` writes them via
/// `db.record_message`, the `IpcStore` frames them, the `NoopStore`
/// ignores them).
///
/// The persistence semantics are the ACP-era ones, unchanged: upsert keys
/// (`(session_id, kind, message_key)`), agent-text accumulation per
/// `messageId`, thought segmentation (`{messageId}#{segment}` boundaries),
/// tool-call `merge_json` shallow-merge. (The ACP `content:
/// ToolCallContent[]` diff channel has no RPC input in Phase 1 — pi's tool
/// results carry no diff-structured content — so the `has_diff` branch is
/// gone with the crate types). `pub(crate)` so the native `AgentLoop`
/// (Task 6) computes through the SAME function.
///
/// Returns 0 or more rows (the `_` arm — an update kind without a display
/// row — yields none, and a Retraction returns one row per row it erases).
/// `created_at` is the current time (the write half's
/// `now_ms` stamp — the `SqliteStore`'s `db.record_message` re-stamps it
/// at write time; the `IpcStore` frames it for the Supervisor's writer).
pub(crate) fn compute_display_rows(
    update: &Value,
    agent_text_acc: &StdMutex<HashMap<String, String>>,
    tool_call_state: &StdMutex<HashMap<String, Value>>,
    thought_state: &StdMutex<ThoughtState>,
) -> Vec<DisplayRow> {
    let kind = update.get("sessionUpdate").and_then(Value::as_str);
    match kind {
        Some("agent_thought_chunk") => {
            let Some(text) = update
                .get("content")
                .and_then(|c| c.get("text"))
                .and_then(Value::as_str)
            else {
                return Vec::new();
            };
            if text.is_empty() {
                return Vec::new();
            }
            let key = update
                .get("messageId")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_string();
            let mut state = thought_state.lock().expect("thought state poisoned");
            if state.open_key.as_deref() != Some(key.as_str()) {
                // New thinking segment: a new messageId, or an intervening
                // segmenting update (see the rule above) cleared `open_key`.
                state.next += 1;
                // (Read before the entry's mutable borrow of `state`.)
                let segment = state.next;
                state.segments.entry(key.clone()).or_default().push(segment);
                state.open_key = Some(key);
                state.current = String::new();
            }
            state.current.push_str(text);
            let row_key = format!("{}#{}", state.open_key.as_ref().unwrap(), state.next);
            let payload = json!({ "text": state.current });
            vec![DisplayRow {
                kind: "agent-thought".to_string(),
                message_key: row_key,
                payload_json: payload,
                created_at: now_ms(),
            }]
        }
        Some("agent_message_chunk") => {
            let Some(text) = update
                .get("content")
                .and_then(|c| c.get("text"))
                .and_then(Value::as_str)
            else {
                return Vec::new();
            };
            if text.is_empty() {
                return Vec::new();
            }
            thought_state
                .lock()
                .expect("thought state poisoned")
                .open_key = None;
            let key = update
                .get("messageId")
                .and_then(Value::as_str)
                .unwrap_or("default")
                .to_string();
            let mut acc = agent_text_acc
                .lock()
                .expect("agent-text accumulator poisoned");
            let entry = acc.entry(key.clone()).or_default();
            entry.push_str(text);
            let payload = json!({ "text": entry });
            vec![DisplayRow {
                kind: "agent-text".to_string(),
                message_key: key,
                payload_json: payload,
                created_at: now_ms(),
            }]
        }
        // (ADR 0048) An Abandoned attempt's reply is erased. The display store has
        // no DELETE, so the rows are OVERWRITTEN with empty text and the reader
        // hydrates an empty row as nothing; the accumulators go with the rows,
        // because the retried call REUSES this message id.
        Some("message_retracted") => {
            let Some(id) = update.get("messageId").and_then(Value::as_str) else {
                return Vec::new();
            };
            let mut rows = Vec::new();
            let streamed_text = {
                let mut acc = agent_text_acc
                    .lock()
                    .expect("agent-text accumulator poisoned");
                acc.remove(id).map(|t| !t.is_empty()).unwrap_or(false)
            };
            if streamed_text {
                rows.push(DisplayRow {
                    kind: "agent-text".to_string(),
                    message_key: id.to_string(),
                    payload_json: json!({ "text": "" }),
                    created_at: now_ms(),
                });
            }
            let segments = {
                let mut state = thought_state.lock().expect("thought state poisoned");
                // The abandoned message's thoughts are CLOSED: the retry opens a
                // FRESH segment (a new row key) rather than continuing text that was
                // just discarded.
                state.open_key = None;
                state.current = String::new();
                state.segments.remove(id)
            };
            if let Some(used) = segments {
                rows.extend(used.into_iter().map(|n| DisplayRow {
                    kind: "agent-thought".to_string(),
                    message_key: format!("{id}#{n}"),
                    payload_json: json!({ "text": "" }),
                    created_at: now_ms(),
                }));
            }
            rows
        }
        Some("tool_call") => {
            let Some(key) = update
                .get("toolCallId")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                return Vec::new();
            };
            thought_state
                .lock()
                .expect("thought state poisoned")
                .open_key = None;
            let mut state = tool_call_state.lock().expect("tool-call state poisoned");
            state.insert(key.clone(), update.clone());
            vec![DisplayRow {
                kind: "tool-call".to_string(),
                payload_json: state[&key].clone(),
                message_key: key,
                created_at: now_ms(),
            }]
        }
        Some("tool_call_update") => {
            let Some(key) = update
                .get("toolCallId")
                .and_then(Value::as_str)
                .map(str::to_string)
            else {
                return Vec::new();
            };
            let mut state = tool_call_state.lock().expect("tool-call state poisoned");
            let is_new = !state.contains_key(&key);
            let entry = state
                .entry(key.clone())
                .or_insert_with(|| Value::Object(Default::default()));
            merge_json(entry, update);
            let payload = entry.clone();
            drop(state);
            // A first-seen tool-call update segments the thought stream
            // (the same rule as the ACP `has_diff` branch — a new tool
            // result interrupts the run).
            if is_new {
                thought_state
                    .lock()
                    .expect("thought state poisoned")
                    .open_key = None;
            }
            vec![DisplayRow {
                kind: "tool-call".to_string(),
                message_key: key,
                payload_json: payload,
                created_at: now_ms(),
            }]
        }
        _ => Vec::new(),
    }
}

/// The current unix time in milliseconds (the `DisplayRow` `created_at`
/// stamp — the `Db::now_ms` mirror; the `SqliteStore`'s `db.record_message`
/// re-stamps it at write time, so the two are the same value in practice).
pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod normalize_tests {
    use serde_json::json;

    use super::{normalize, TurnState};
    use crate::agent::events::RpcEvent;

    fn ev(v: serde_json::Value) -> RpcEvent {
        serde_json::from_value(v).expect("event should parse")
    }

    /// The `messageId` role rule: a USER `message_start` does NOT advance
    /// the counter; the following ASSISTANT `message_start` numbers the
    /// first assistant chunk `m1` (a role-blind counter would make it
    /// `m2` and break the replay numbering).
    #[test]
    fn message_id_advances_only_for_assistant_messages() {
        let mut st = TurnState::default();
        let user = ev(json!({
            "type": "message_start",
            "message": { "role": "user", "content": [{ "type": "text", "text": "hi" }] },
        }));
        assert!(normalize(&user, &mut st).is_empty());
        assert_eq!(
            st.msg_counter, 0,
            "a user message_start must NOT advance the counter"
        );
        assert!(st.current_message_id.is_none());

        let assistant = ev(json!({
            "type": "message_start",
            "message": { "role": "assistant", "content": [] },
        }));
        assert!(normalize(&assistant, &mut st).is_empty());
        assert_eq!(st.msg_counter, 1);
        assert_eq!(st.current_message_id.as_deref(), Some("m1"));

        let delta = ev(json!({
            "type": "message_update",
            "usage": null,
            "assistantMessageEvent": { "type": "text_delta", "contentIndex": 0, "delta": "Hel" },
        }));
        let frames = normalize(&delta, &mut st);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["sessionUpdate"], "agent_message_chunk");
        assert_eq!(
            frames[0]["messageId"], "m1",
            "the first assistant chunk keys m1"
        );
        assert_eq!(frames[0]["content"]["text"], "Hel");
    }

    /// A second assistant message advances the counter to `m2`; the
    /// `*_end` / `*_start` frames carry NO frame (the delta stream already
    /// delivered the content — re-emitting would double the text).
    #[test]
    fn end_and_start_frames_emit_nothing() {
        let mut st = TurnState::default();
        let _ = normalize(
            &ev(json!({ "type": "message_start", "message": { "role": "assistant" } })),
            &mut st,
        );
        let _ = normalize(
            &ev(json!({ "type": "message_start", "message": { "role": "assistant" } })),
            &mut st,
        );
        assert_eq!(st.current_message_id.as_deref(), Some("m2"));

        for t in ["text_end", "thinking_end", "text_start", "thinking_start"] {
            let frames = normalize(
                &ev(json!({
                    "type": "message_update",
                    "usage": null,
                    "assistantMessageEvent": { "type": t, "contentIndex": 0, "content": "x", "delta": "x" },
                })),
                &mut st,
            );
            assert!(frames.is_empty(), "{t} must emit no frame");
        }
    }

    /// `thinking_delta` → `agent_thought_chunk` (the same `messageId` keying
    /// as text deltas).
    #[test]
    fn thinking_delta_maps_to_agent_thought_chunk() {
        let mut st = TurnState::default();
        let _ = normalize(
            &ev(json!({ "type": "message_start", "message": { "role": "assistant" } })),
            &mut st,
        );
        let frames = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "thinking_delta", "contentIndex": 0, "delta": "hmm" },
            })),
            &mut st,
        );
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["sessionUpdate"], "agent_thought_chunk");
        assert_eq!(frames[0]["content"]["text"], "hmm");
        assert_eq!(frames[0]["messageId"], "m1");
    }

    /// The tool-call frame sequence: `toolcall_start` → `tool_call` (empty
    /// `rawInput`), `toolcall_delta` → `tool_call_update` (partial args as
    /// `partialArgs` while incomplete, `rawInput` once the accumulated
    /// string parses as JSON), `toolcall_end` → `tool_call_update` with the
    /// full `arguments` object + the buffer cleared.
    #[test]
    fn toolcall_frames_accumulate_args() {
        let mut st = TurnState::default();
        let _ = normalize(
            &ev(json!({ "type": "message_start", "message": { "role": "assistant" } })),
            &mut st,
        );

        let start = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "toolcall_start", "contentIndex": 0, "id": "tc1", "toolName": "bash" },
            })),
            &mut st,
        );
        assert_eq!(start[0]["sessionUpdate"], "tool_call");
        assert_eq!(start[0]["toolCallId"], "tc1");
        assert_eq!(start[0]["title"], "bash");
        assert_eq!(start[0]["rawInput"], json!({}));

        // An incomplete JSON fragment → `partialArgs` (the adapter's
        // behavior — the frontend shows the raw string while parsing).
        let partial = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "toolcall_delta", "contentIndex": 0, "id": "tc1", "delta": "{\"cmd\":" },
            })),
            &mut st,
        );
        assert_eq!(partial[0]["partialArgs"], "{\"cmd\":");
        assert!(partial[0].get("rawInput").is_none());

        // The fragment completes → `rawInput` (the accumulated string
        // parses as JSON).
        let done = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "toolcall_delta", "contentIndex": 0, "id": "tc1", "delta": "\"ls\"}" },
            })),
            &mut st,
        );
        assert_eq!(done[0]["rawInput"], json!({ "cmd": "ls" }));

        // `toolcall_end` → the full `arguments` object + the buffer
        // cleared (a later delta for the same id starts fresh).
        let end = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": {
                    "type": "toolcall_end",
                    "toolCall": { "id": "tc1", "name": "bash", "arguments": { "cmd": "ls" } },
                },
            })),
            &mut st,
        );
        assert_eq!(end[0]["rawInput"], json!({ "cmd": "ls" }));
        assert_eq!(end[0]["status"], "in_progress");
        assert!(
            !st.toolcall_args.contains_key("tc1"),
            "the buffer must be cleared"
        );
    }

    /// `tool_execution_*` → `tool_call` (start, with the real `title` +
    /// `rawInput`) / `tool_call_update` (partial `rawOutput` /
    /// completed-or-failed + `rawOutput`).
    #[test]
    fn tool_execution_frames_map_to_updates() {
        let mut st = TurnState::default();
        // `tool_execution_start` CARRIES the tool name + args (the provider
        // already accumulated them), so it maps to a `tool_call` frame (with
        // the real `title` + `rawInput`) — NOT a `tool_call_update` (no
        // `title` → the frontend would display the `toolCallId`, e.g.
        // `chatcmpl-tool-…`, instead of the tool name).
        let start = normalize(
            &ev(
                json!({ "type": "tool_execution_start", "toolCallId": "tc1", "toolName": "bash", "args": { "cmd": "ls" } }),
            ),
            &mut st,
        );
        assert_eq!(
            start[0],
            json!({ "sessionUpdate": "tool_call", "toolCallId": "tc1", "title": "bash", "status": "in_progress", "rawInput": { "cmd": "ls" } })
        );

        let update = normalize(
            &ev(
                json!({ "type": "tool_execution_update", "toolCallId": "tc1", "toolName": "bash", "args": {}, "partialResult": "out" }),
            ),
            &mut st,
        );
        assert_eq!(update[0]["rawOutput"], "out");

        let end_ok = normalize(
            &ev(
                json!({ "type": "tool_execution_end", "toolCallId": "tc1", "toolName": "bash", "result": "done", "isError": false }),
            ),
            &mut st,
        );
        assert_eq!(end_ok[0]["status"], "completed");
        assert_eq!(end_ok[0]["rawOutput"], "done");

        let end_err = normalize(
            &ev(
                json!({ "type": "tool_execution_end", "toolCallId": "tc1", "toolName": "bash", "result": "boom", "isError": true }),
            ),
            &mut st,
        );
        assert_eq!(end_err[0]["status"], "failed");
    }

    /// The NATIVE-HARNESS flow (no `toolcall_start` at all — the harness
    /// emits `toolcall_delta` + `toolcall_end` only): the FIRST frame for
    /// an id must be a `tool_call` with the real `title` (a
    /// `tool_call_update` for an id the frontend never saw would create a
    /// message with `title = toolCallId`, e.g. `chatcmpl-tool-…`), and
    /// `tool_execution_start` must NOT re-announce (the frontend's
    /// `tool_call` case always APPENDS — a second `tool_call` frame would
    /// duplicate the row).
    #[test]
    fn native_harness_toolcall_end_announces_single_row() {
        let mut st = TurnState::default();
        // `toolcall_delta` (the harness's first frame for the id):
        // buffer only — NO frame.
        for delta in ["{\"cmd\":", "\"ls\"}"] {
            let frames = normalize(
                &ev(json!({
                    "type": "message_update",
                    "usage": null,
                    "assistantMessageEvent": { "type": "toolcall_delta", "id": "chatcmpl-tool-x", "delta": delta },
                })),
                &mut st,
            );
            assert!(
                frames.is_empty(),
                "an unannounced delta must not emit a frame"
            );
        }

        // `toolcall_end` (the name is known here): the ANNOUNCEMENT — a
        // `tool_call` with the real `title` + the full `rawInput`.
        let end = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": {
                    "type": "toolcall_end",
                    "toolCall": { "id": "chatcmpl-tool-x", "name": "bash", "arguments": { "cmd": "ls" } },
                },
            })),
            &mut st,
        );
        assert_eq!(
            end[0],
            json!({ "sessionUpdate": "tool_call", "toolCallId": "chatcmpl-tool-x", "title": "bash", "status": "in_progress", "rawInput": { "cmd": "ls" } })
        );

        // `tool_execution_start` for the ANNOUNCED id: an UPDATE (the
        // frontend applies `title` / `rawInput` / `status` in place), NOT
        // a re-announcement.
        let start = normalize(
            &ev(
                json!({ "type": "tool_execution_start", "toolCallId": "chatcmpl-tool-x", "toolName": "bash", "args": { "cmd": "ls" } }),
            ),
            &mut st,
        );
        assert_eq!(start[0]["sessionUpdate"], "tool_call_update");
        assert_eq!(start[0]["title"], "bash");
        assert_eq!(start[0]["rawInput"], json!({ "cmd": "ls" }));
        assert_eq!(start[0]["status"], "in_progress");

        // `tool_execution_end` completes the single row.
        let done = normalize(
            &ev(
                json!({ "type": "tool_execution_end", "toolCallId": "chatcmpl-tool-x", "toolName": "bash", "result": "done", "isError": false }),
            ),
            &mut st,
        );
        assert_eq!(done[0]["sessionUpdate"], "tool_call_update");
        assert_eq!(done[0]["status"], "completed");
        assert_eq!(done[0]["rawOutput"], "done");
    }

    /// An external-pi `toolcall_start` with an EMPTY `toolName` (the
    /// OpenAI-compatible streaming delivers `function.name` in a LATER
    /// delta) must NOT announce — the announcement is deferred to
    /// `toolcall_end` (where the name is known). A `tool_call` frame with
    /// the empty title would make the frontend display the `toolCallId`
    /// (e.g. `chatcmpl-tool-…`).
    #[test]
    fn empty_toolname_defers_announcement_to_toolcall_end() {
        let mut st = TurnState::default();
        let start = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "toolcall_start", "contentIndex": 0, "id": "tc1", "toolName": "" },
            })),
            &mut st,
        );
        assert!(start.is_empty(), "an empty toolName must not announce");

        let delta = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "toolcall_delta", "contentIndex": 0, "id": "tc1", "delta": "{\"cmd\":\"ls\"}" },
            })),
            &mut st,
        );
        assert!(delta.is_empty(), "an unannounced id must buffer only");

        let end = normalize(
            &ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": {
                    "type": "toolcall_end",
                    "toolCall": { "id": "tc1", "name": "bash", "arguments": { "cmd": "ls" } },
                },
            })),
            &mut st,
        );
        assert_eq!(
            end[0],
            json!({ "sessionUpdate": "tool_call", "toolCallId": "tc1", "title": "bash", "status": "in_progress", "rawInput": { "cmd": "ls" } })
        );

        // `tool_execution_start` for the announced id: an update, not a
        // re-announcement (no duplicate row).
        let exec_start = normalize(
            &ev(
                json!({ "type": "tool_execution_start", "toolCallId": "tc1", "toolName": "bash", "args": { "cmd": "ls" } }),
            ),
            &mut st,
        );
        assert_eq!(exec_start[0]["sessionUpdate"], "tool_call_update");
    }

    /// The bookkeeping one-liners (stable strings for tests) + the
    /// no-frame bookkeeping events (`agent_settled` / `turn_end` / …).
    ///
    /// A compaction event carries TWO frames: the durable transcript
    /// one-liner (the only trace of the compaction in the display rows —
    /// the summary itself is a `native_messages` `System` row) AND the
    /// `compaction_update` STATE frame ("a compaction is running", with
    /// the trigger `reason` — `manual` / `threshold` / `overflow`), which
    /// the renderer's indicator reads and which persists no display row.
    #[test]
    fn bookkeeping_events_and_one_liners() {
        let mut st = TurnState::default();
        let compacting = normalize(
            &ev(json!({ "type": "compaction_start", "reason": "manual" })),
            &mut st,
        );
        assert_eq!(compacting.len(), 2, "the one-liner + the state frame");
        assert_eq!(compacting[0]["content"]["text"], "Compacting context…");
        assert_eq!(
            compacting[0]["messageId"], "system",
            "the one-liners key the dedicated system id"
        );
        assert_eq!(
            compacting[1],
            json!({ "sessionUpdate": "compaction_update", "active": true, "reason": "manual" }),
            "the state frame carries the trigger reason"
        );

        let done = normalize(
            &ev(
                json!({ "type": "compaction_end", "reason": "manual", "aborted": false, "willRetry": false }),
            ),
            &mut st,
        );
        assert_eq!(done.len(), 2, "the one-liner + the state frame");
        assert_eq!(done[0]["content"]["text"], "Compaction finished");
        assert_eq!(
            done[1],
            json!({ "sessionUpdate": "compaction_update", "active": false, "reason": "manual" }),
            "the state frame closes the indicator, with the same reason"
        );

        let retry = normalize(
            &ev(
                json!({ "type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 1000, "errorMessage": "x" }),
            ),
            &mut st,
        );
        assert_eq!(retry[0]["content"]["text"], "Retrying (attempt 1/3…)");

        for t in [
            "agent_start",
            "agent_end",
            "agent_settled",
            "turn_start",
            "queue_update",
            "bash_execution_update",
        ] {
            let v = match t {
                "turn_start" => json!({ "type": t }),
                "agent_end" => json!({ "type": t, "messages": [], "willRetry": false }),
                _ => {
                    json!({ "type": t, "toolCallId": "x", "toolName": "bash", "args": {}, "partialResult": "p", "result": "r", "isError": false, "steering": [], "followUp": [], "entry": {}, "id": "x", "delta": "d", "message": { "role": "assistant" }, "toolResults": [] })
                }
            };
            assert!(
                normalize(&ev(v), &mut st).is_empty(),
                "{t} must emit no frame"
            );
        }
    }
    /// A `compaction_end` that CARRIES an `error_message` prints it INSTEAD
    /// of the success line — and not in addition to it. Both lines are
    /// `system_chunk`s, i.e. they share `messageId: "system"`, and the
    /// frontend's `applySessionUpdate` APPENDS same-`messageId` chunks with NO
    /// separator: two of them in one batch weld into one bubble reading
    /// `Compaction finishedCompaction failed: …` (pinned at the store level in
    /// `src/store/sessions.test.ts`). Replacing the line is what keeps the
    /// transcript readable — and a SUCCESSFUL compaction has no `error_message`,
    /// so the pinned `"Compaction finished"` string still ships on that path.
    ///
    /// The scope is any `error_message`, not just the overflow ones: this is
    /// the first time a provider-side compaction failure is visible to the user
    /// at all (see `a_failed_summary_does_not_rewrite_the_transcript`).
    #[test]
    fn a_compaction_error_message_replaces_the_success_line() {
        let mut st = TurnState::default();
        let failed = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "overflow",
                "aborted": false,
                "willRetry": true,
                "errorMessage": "Context overflow: not enough older context to compact — try a larger-context model.",
            })),
            &mut st,
        );
        assert_eq!(failed.len(), 2, "the one-liner + the state frame");
        assert_eq!(
            failed[0]["content"]["text"],
            "Compaction failed: Context overflow: not enough older context to compact — try a larger-context model.",
            "the actionable message is the line the user reads"
        );
        assert!(
            !failed[0]["content"]["text"]
                .as_str()
                .unwrap_or_default()
                .contains("Compaction finished"),
            "the success line is REPLACED, not followed (the weld)"
        );
        assert_eq!(
            failed[0]["messageId"], "system",
            "the failure line keeps the system id"
        );
        assert_eq!(
            failed[1],
            json!({ "sessionUpdate": "compaction_update", "active": false, "reason": "overflow" }),
            "the frame order is unchanged: [one-liner, state frame]"
        );

        // A `Failed` compaction (aborted) prints the same way.
        let aborted = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "overflow",
                "aborted": true,
                "willRetry": true,
                "errorMessage": "Context overflow: the summary call failed (boom) — the transcript was left unchanged.",
            })),
            &mut st,
        );
        assert!(
            aborted[0]["content"]["text"]
                .as_str()
                .unwrap_or_default()
                .starts_with("Compaction failed: Context overflow: the summary call failed (boom)"),
            "an aborted compaction prints its error too, got {:?}",
            aborted[0]["content"]["text"]
        );
    }

    /// (ADR 0048) A real discard is both the erase AND a line saying so, and
    /// the line carries its OWN messageId — the shared `"system"` key would weld
    /// it onto the retry line emitted immediately before it.
    #[test]
    fn a_retraction_erases_the_current_message_and_says_so() {
        let mut st = TurnState::default();
        normalize(
            &RpcEvent::message_start {
                message: json!({ "role": "assistant" }),
            },
            &mut st,
        );
        let frames = normalize(&RpcEvent::message_retracted, &mut st);
        assert_eq!(
            frames,
            vec![
                json!({ "sessionUpdate": "message_retracted", "messageId": "m1" }),
                json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": "Discarded an incomplete reply" },
                    "messageId": "retract:1",
                }),
            ]
        );
        assert_eq!(
            st.current_message_id.as_deref(),
            Some("m1"),
            "the retried call REUSES the id: a Retraction is not a new message"
        );
    }

    /// Two discards in one turn must not weld (ADR 0048), so the line's id
    /// increments per Retraction — the ids are unique, not shared.
    #[test]
    fn a_second_retraction_gets_its_own_line_id() {
        let mut st = TurnState::default();
        normalize(
            &RpcEvent::message_start {
                message: json!({ "role": "assistant" }),
            },
            &mut st,
        );
        let first = normalize(&RpcEvent::message_retracted, &mut st);
        let second = normalize(&RpcEvent::message_retracted, &mut st);
        assert_eq!(first[1]["messageId"], "retract:1");
        assert_eq!(second[1]["messageId"], "retract:2");
    }

    /// A Retraction with no message announced yet is still well-formed (the
    /// `default` id every other chunk falls back to).
    #[test]
    fn a_retraction_before_any_message_start_says_default() {
        let mut st = TurnState::default();
        let frames = normalize(&RpcEvent::message_retracted, &mut st);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["sessionUpdate"], "message_retracted");
        assert_eq!(frames[0]["messageId"], "default");
    }
}

#[cfg(test)]
mod compute_display_rows_tests {
    use serde_json::json;

    use super::{compute_display_rows, ThoughtState};
    use crate::agent::harness::store::{SessionStore, Store};
    use crate::storage::Db;
    use crate::types::SessionInfo;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};

    /// A temp-dir `Db` with a recorded `sessions` row (the `messages`
    /// rows FK to `sessions`).
    fn temp_db() -> Arc<Db> {
        let dir =
            std::env::temp_dir().join(format!("harness-display-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Arc::new(Db::open(&dir.join("t.db")).expect("db should open"));
        db.record_session(&SessionInfo {
            session_id: "s1".to_string(),
            cwd: std::path::PathBuf::from("/tmp"),
            capabilities: serde_json::json!({}),
            config_options: None,
            archived: false,
            title: None,
            context_usage: None,
            is_subagent: false,
        })
        .expect("record_session");
        db
    }

    /// The `persist_update` split (ADR 0025) GOLDEN test:
    /// `compute_display_rows` on a canned normalized update + canned
    /// accumulators yields the same `Vec<DisplayRow>` the old
    /// `persist_update` wrote (the normalize + accumulator logic moved
    /// verbatim — `SqliteStore::persist_display` applies the rows via
    /// `db.record_message`, so the `messages` rows are identical).
    #[test]
    fn compute_display_rows_matches_the_legacy_persist_update_writes() {
        let db = temp_db();
        let store = SessionStore::new(db.clone());
        let text_acc = StdMutex::new(HashMap::new());
        let tool_state = StdMutex::new(HashMap::new());
        let thought_state = StdMutex::new(ThoughtState::default());

        // The canned normalized update sequence (the FULL
        // `session-update` frames the `normalize` emits):
        let updates = [
            // The assistant message m1: two chunks (the accumulator grows).
            json!({ "sessionUpdate": "agent_message_chunk", "messageId": "m1", "content": { "text": "Hel" } }),
            json!({ "sessionUpdate": "agent_message_chunk", "messageId": "m1", "content": { "text": "lo" } }),
            // A thought segment for m1 (one chunk).
            json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m1", "content": { "text": "think" } }),
            // A tool-call announcement + update (the `merge_json` shallow-merge).
            json!({ "sessionUpdate": "tool_call", "toolCallId": "t1", "title": "run" }),
            json!({ "sessionUpdate": "tool_call_update", "toolCallId": "t1", "status": { "type": "completed" } }),
            // A second assistant message m2 (segments the thought stream).
            json!({ "sessionUpdate": "agent_message_chunk", "messageId": "m2", "content": { "text": "x" } }),
            // An unknown kind (NO row — the `_` arm).
            json!({ "sessionUpdate": "usage_update" }),
        ];
        let mut all_rows = Vec::new();
        for u in &updates {
            let rows = compute_display_rows(u, &text_acc, &tool_state, &thought_state);
            if !rows.is_empty() {
                assert_eq!(rows.len(), 1, "at most one row per update");
            }
            all_rows.extend(rows);
        }
        // 6 rows (the 7th update — the unknown kind — yields none).
        assert_eq!(all_rows.len(), 6);
        // The per-row golden (what the old `persist_update` wrote):
        let (r0, r1, r2, r3, r4, r5) = (
            &all_rows[0],
            &all_rows[1],
            &all_rows[2],
            &all_rows[3],
            &all_rows[4],
            &all_rows[5],
        );
        assert_eq!(
            (r0.kind.as_str(), r0.message_key.as_str()),
            ("agent-text", "m1")
        );
        assert_eq!(r0.payload_json, json!({ "text": "Hel" }), "the first chunk");
        assert_eq!(
            r1.payload_json,
            json!({ "text": "Hello" }),
            "the accumulator grew"
        );
        assert_eq!(
            (r2.kind.as_str(), r2.message_key.as_str()),
            ("agent-thought", "m1#1"),
            "the thought segment key"
        );
        assert_eq!(r2.payload_json, json!({ "text": "think" }));
        assert_eq!(
            (r3.kind.as_str(), r3.message_key.as_str()),
            ("tool-call", "t1")
        );
        assert_eq!(r3.payload_json["title"], "run", "the announcement row");
        assert_eq!(
            r4.payload_json["title"], "run",
            "the merged row keeps the title"
        );
        assert_eq!(
            r4.payload_json["status"]["type"], "completed",
            "the merge added the status"
        );
        assert_eq!(
            (r5.kind.as_str(), r5.message_key.as_str()),
            ("agent-text", "m2")
        );
        assert_eq!(r5.payload_json, json!({ "text": "x" }));
        // `created_at` is the current time (the write half's `now_ms`).
        assert!(r0.created_at > 0);

        // The WRITE half (`SqliteStore::persist_display` via
        // `db.record_message`): the rows land as `messages` rows — the
        // FINAL golden values (the last row per key wins, the upsert
        // idempotency — the old `persist_update`'s semantics,
        // unchanged).
        store.persist_display("s1", &all_rows).unwrap();
        let db_rows = db.messages_for("s1").unwrap();
        assert_eq!(db_rows.len(), 4, "four distinct rows landed");
        let text = db_rows
            .iter()
            .find(|r| r.kind == "agent-text" && r.message_key.as_deref() == Some("m1"))
            .unwrap();
        assert_eq!(
            text.payload_json, r#"{"text":"Hello"}"#,
            "the m1 row holds the GROWN accumulator (the last upsert wins)"
        );
        let thought = db_rows.iter().find(|r| r.kind == "agent-thought").unwrap();
        assert_eq!(thought.message_key.as_deref(), Some("m1#1"));
        assert_eq!(thought.payload_json, r#"{"text":"think"}"#);
        let tool = db_rows
            .iter()
            .find(|r| r.kind == "tool-call" && r.message_key.as_deref() == Some("t1"))
            .unwrap();
        let tool: serde_json::Value = serde_json::from_str(&tool.payload_json).unwrap();
        assert_eq!(tool["title"], "run");
        assert_eq!(tool["status"]["type"], "completed");
        // A re-send of the SAME rows is an idempotent upsert (no
        // duplicates — the old `persist_update`'s semantics).
        store.persist_display("s1", &all_rows).unwrap();
        let db_rows = db.messages_for("s1").unwrap();
        assert_eq!(db_rows.len(), 4, "the upsert never duplicates");
    }

    /// The `compaction_update` STATE frame (the renderer's "a compaction is
    /// running" indicator) is bookkeeping, NOT transcript content: it persists
    /// NO display row and leaves the thinking-segment state untouched (a
    /// compaction that lands mid-stream must not split the thought segment
    /// that straddles it). Pinned because the frame falls through
    /// `compute_display_rows`' catch-all `_ => Vec::new()` — a future change
    /// to that catch-all would make it replay as a chat bubble on resume.
    #[test]
    fn a_compaction_update_frame_persists_no_display_row() {
        let text_acc = StdMutex::new(HashMap::new());
        let tool_state = StdMutex::new(HashMap::new());
        let thought_state = StdMutex::new(ThoughtState::default());

        // An OPEN thought segment first, so the no-side-effect half is
        // asserted on live state rather than on a fresh one.
        let opening = compute_display_rows(
            &json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m1", "content": { "text": "think" } }),
            &text_acc,
            &tool_state,
            &thought_state,
        );
        assert_eq!(opening.len(), 1, "the thought chunk writes a row");

        for active in [true, false] {
            let rows = compute_display_rows(
                &json!({ "sessionUpdate": "compaction_update", "active": active, "reason": "threshold" }),
                &text_acc,
                &tool_state,
                &thought_state,
            );
            assert!(
                rows.is_empty(),
                "the compaction_update frame (active: {active}) persists no display row, got {rows:?}"
            );
        }

        // The thinking-segment state is UNTOUCHED: the next chunk continues
        // the SAME segment (`m1#1`, the text grows) instead of starting #2.
        let after = compute_display_rows(
            &json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m1", "content": { "text": "ing" } }),
            &text_acc,
            &tool_state,
            &thought_state,
        );
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].message_key, "m1#1", "the segment was not split");
        assert_eq!(after[0].payload_json, json!({ "text": "thinking" }));
    }

    /// (ADR 0048) A Retraction empties the abandoned message's rows — text AND
    /// thoughts — and empties the accumulators, so the retried call that REUSES
    /// the id starts from nothing instead of welding on. The display store has
    /// no DELETE, so "erased" means OVERWRITTEN with empty text.
    #[test]
    fn a_retraction_empties_the_abandoned_rows() {
        let text_acc = StdMutex::new(HashMap::new());
        let tool_state = StdMutex::new(HashMap::new());
        let thought_state = StdMutex::new(ThoughtState::default());

        // The abandoned attempt: text, a thought segment, a tool-call
        // announcement (which CLOSES that segment), then a second segment.
        let drive = [
            json!({ "sessionUpdate": "agent_message_chunk", "messageId": "m1", "content": { "text": "parti" } }),
            json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m1", "content": { "text": "th" } }),
            json!({ "sessionUpdate": "tool_call", "toolCallId": "t1", "title": "run" }),
            json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m1", "content": { "text": "second" } }),
        ];
        for u in &drive {
            compute_display_rows(u, &text_acc, &tool_state, &thought_state);
        }

        let rows = compute_display_rows(
            &json!({ "sessionUpdate": "message_retracted", "messageId": "m1" }),
            &text_acc,
            &tool_state,
            &thought_state,
        );
        let erased: std::collections::BTreeSet<(&str, &str)> = rows
            .iter()
            .map(|r| (r.kind.as_str(), r.message_key.as_str()))
            .collect();
        assert_eq!(
            erased,
            [
                ("agent-text", "m1"),
                ("agent-thought", "m1#1"),
                ("agent-thought", "m1#2"),
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
            "the erased key set is EXACTLY the rows the abandoned message wrote, got {rows:?}"
        );
        for row in &rows {
            assert_eq!(
                row.payload_json,
                json!({ "text": "" }),
                "{} row {} is overwritten empty, not deleted",
                row.kind,
                row.message_key
            );
        }

        assert!(
            !text_acc
                .lock()
                .expect("accumulator poisoned")
                .contains_key("m1"),
            "the text accumulator no longer holds the abandoned id"
        );
        let state = thought_state.lock().expect("thought state poisoned");
        assert_eq!(
            state.open_key.as_deref(),
            None,
            "the abandoned message's thoughts are CLOSED"
        );
        assert_eq!(
            state.current, "",
            "the open-segment buffer does not carry the discarded text"
        );
        drop(state);

        // The retried call REUSES the id, and now writes its own text alone:
        // `done`, never `partidone` (the weld ADR 0048 exists to stop).
        let retried = compute_display_rows(
            &json!({ "sessionUpdate": "agent_message_chunk", "messageId": "m1", "content": { "text": "done" } }),
            &text_acc,
            &tool_state,
            &thought_state,
        );
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].kind, "agent-text");
        assert_eq!(retried[0].message_key, "m1");
        assert_eq!(
            retried[0].payload_json,
            json!({ "text": "done" }),
            "the retry streams into the cleared id from nothing"
        );
    }

    /// (ADR 0048) The thought run does NOT start at `#1`: `next` counts
    /// segments for the life of the loop, so a second message's thoughts are
    /// `m2#2`, `m2#3`. Erasing a range from `#1` would UPSERT rows that never
    /// existed, and the UPSERT has no undo — so the erased set is exactly the
    /// keys this message actually used.
    #[test]
    fn a_retraction_erases_exactly_the_keys_that_exist() {
        let text_acc = StdMutex::new(HashMap::new());
        let tool_state = StdMutex::new(HashMap::new());
        let thought_state = StdMutex::new(ThoughtState::default());

        let drive = [
            // The loop's FIRST segment belongs to the FIRST message.
            json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m1", "content": { "text": "first" } }),
            json!({ "sessionUpdate": "tool_call", "toolCallId": "t1", "title": "run" }),
            // m2's thoughts therefore open at #2, not #1.
            json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m2", "content": { "text": "second" } }),
            json!({ "sessionUpdate": "tool_call", "toolCallId": "t2", "title": "again" }),
            json!({ "sessionUpdate": "agent_thought_chunk", "messageId": "m2", "content": { "text": "third" } }),
        ];
        let driven: Vec<String> = drive
            .iter()
            .flat_map(|u| compute_display_rows(u, &text_acc, &tool_state, &thought_state))
            .map(|r| format!("{}@{}", r.kind, r.message_key))
            .collect();
        assert_eq!(
            driven,
            vec![
                "agent-thought@m1#1",
                "tool-call@t1",
                "agent-thought@m2#2",
                "tool-call@t2",
                "agent-thought@m2#3",
            ],
            "m2's run is #2..#3 — it does NOT start at #1"
        );

        let rows = compute_display_rows(
            &json!({ "sessionUpdate": "message_retracted", "messageId": "m2" }),
            &text_acc,
            &tool_state,
            &thought_state,
        );
        let erased: std::collections::BTreeSet<(&str, &str)> = rows
            .iter()
            .map(|r| (r.kind.as_str(), r.message_key.as_str()))
            .collect();
        assert_eq!(
            erased,
            [("agent-thought", "m2#2"), ("agent-thought", "m2#3")]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "exactly the keys that existed are erased (no `m2#1`, no other \
             message's row), got {rows:?}"
        );
    }

    /// (ADR 0048) Nothing streamed under this id, so nothing is written: an
    /// empty Retraction must not litter a table the UPSERT can never clean.
    #[test]
    fn a_retraction_of_a_message_that_never_streamed_writes_nothing() {
        let text_acc = StdMutex::new(HashMap::new());
        let tool_state = StdMutex::new(HashMap::new());
        let thought_state = StdMutex::new(ThoughtState::default());

        let rows = compute_display_rows(
            &json!({ "sessionUpdate": "message_retracted", "messageId": "m7" }),
            &text_acc,
            &tool_state,
            &thought_state,
        );
        assert!(
            rows.is_empty(),
            "a Retraction for an id that streamed nothing writes no row, got {rows:?}"
        );
    }
}

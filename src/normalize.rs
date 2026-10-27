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
/// irreproducible). `current_message_id` is
/// `format!("{incarnation}:m{counter}")` (ADR 0049 — the stamp is what keeps
/// a resumed Worker's counter restart out of its predecessor's rows).
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
    /// messageId UNIQUE per discard (ADR 0048) — two lines on the shared
    /// `system` key (today `{stamp}:system`) would weld into one bubble.
    pub retract_seq: u64,
    /// The card id of the compaction currently in flight, set by
    /// `compaction_start` and taken by `compaction_end` (issue #90: a
    /// compaction renders as a `ToolCallCard`, so the settle must land on the
    /// card the announcement opened — a fresh id per compaction, because a long
    /// session compacts many times and one card per session would merge them).
    pub open_compaction_id: Option<String>,
    /// How many compaction cards this turn has announced (the per-compaction
    /// id counter, same role as `retract_seq`).
    pub compaction_seq: u64,
    /// The stamp for this Worker's life (ADR 0049), prefixed onto every id
    /// this state mints. An EMPTY stamp is a test artifact — production can
    /// only build a `TurnState` through `AgentLoop::new`, which requires one.
    pub incarnation: String,
}

impl TurnState {
    /// A state whose ids are namespaced by `incarnation` (ADR 0049).
    pub fn stamped(incarnation: impl Into<String>) -> Self {
        Self {
            incarnation: incarnation.into(),
            ..Self::default()
        }
    }

    /// Namespace one of this state's ids: `a3f9c1:m1`, `a3f9c1:system`.
    fn id(&self, suffix: &str) -> String {
        format!("{}:{}", self.incarnation, suffix)
    }
}

/// A one-line `agent_message_chunk` carrying a UNIQUE messageId — for a line
/// that must survive being adjacent to another line. `system_chunk`'s shared
/// key (the stamped `system` key, `{stamp}:system`) APPENDS (that is its
/// point), so a second line on it welds.
fn unique_system_chunk(message_id: &str, text: impl Into<String>) -> Value {
    let text = text.into();
    json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text },
        "messageId": message_id,
    })
}

/// Is this messageId a harness bookkeeping line rather than model output?
/// The bookkeeping WORDS are `system` (the shared one-liner key) and `retract`
/// (a discard line, ADR 0048). The predicate matches either as a `:`-separated
/// word ANYWHERE in the id — deliberately a word-set, not a position: the id
/// `normalize` mints today is `{stamp}:system` / `{stamp}:retract:N` (ADR
/// 0049), and the BARE shape (`system`, `retract:1`) is accepted as well
/// because that is the id shape WITH NO stamp — the test fixtures that
/// hand-write these ids, and `TurnState::default()`, whose EMPTY stamp mints
/// `:system` / `:retract:1` (a "strip a leading 6-hex field, then match" rule
/// would misclassify those). A stamp is SIX HEX CHARS
/// (`types::mint_incarnation`), and neither word is spelled from the hex
/// alphabet (`system` carries `s`/`y`/`t`/`m`; `retract` is seven chars and
/// carries `r`/`t`), so a stamp can never EQUAL one. `default` is deliberately NOT here: it is the
/// no-id-open fallback over REAL model text, and the subagent capture must
/// still hand it to the parent. Anything that must capture ONLY what the model
/// said — the subagent capture, whose text becomes the PARENT's tool result —
/// asks this instead of re-deciding the convention.
pub fn is_bookkeeping_message_id(id: &str) -> bool {
    id.split(':')
        .any(|field| field == "system" || field == "retract")
}

/// The transcript title of a compaction's card (issue #90 item 2). Not a real
/// tool: nothing is dispatched under this name, and it is deliberately NOT in
/// `tool_specs()` — it exists only as a display title, so the model can never
/// call it.
pub(crate) const COMPACTION_TOOL: &str = "compaction";

/// A one-line `agent_message_chunk` with the dedicated `system` messageId
/// (the bookkeeping one-liners — never mixed into a real message's
/// accumulated text). The id is the stamped shared key (`{stamp}:system`) so a
/// resumed Worker's one-liner does not overwrite its predecessor's (ADR 0049).
fn system_chunk(message_id: &str, text: impl Into<String>) -> Value {
    let text = text.into();
    json!({
        "sessionUpdate": "agent_message_chunk",
        "content": { "type": "text", "text": text },
        "messageId": message_id,
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
                st.current_message_id = Some(st.id(&format!("m{}", st.msg_counter)));
            }
            Vec::new()
        }
        RpcEvent::message_update {
            assistant_message_event: ev,
            ..
        } => {
            // LAZY on purpose (ADR 0049's id stamp makes the fallback a
            // `format!`): this arm runs for EVERY streamed token, and the
            // `default` id is built ONLY when no assistant `message_start` has
            // opened a message yet — hoisting it above the `match` would pay
            // one allocation per token for the id of that one edge case.
            let mid = match st.current_message_id.clone() {
                Some(id) => id,
                None => st.id("default"),
            };
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
        // (issue #90 item 2) A compaction renders as a TOOL CARD, like `bash`
        // and the rest, because that is the transcript's existing shape for
        // "something ran, here is what it produced" — and what a compaction
        // produces (the summary) was previously shown NOWHERE: it lived only in
        // the `native_messages` `System` row the model is replayed, and the
        // transcript's only trace was the one-liner.
        //
        // So the two `system_chunk` one-liners are GONE, replaced by the
        // announce/settle pair a `ToolCallCard` renders. Removing them also
        // removes the weld they were engineered to dodge: every `system_chunk`
        // shares `{stamp}:system` and the frontend APPENDS same-`messageId`
        // chunks with no separator, so a session that compacts repeatedly reads
        // as one bubble — measured in a real database:
        // `Compacting context…Compaction finishedCompacting context…Compaction
        // finishedCompaction failed: …`. Each compaction now gets its own card.
        //
        // The `compaction_update` STATE frame SURVIVES both events: the
        // indicator, `composerLocked` and the Esc-to-cancel gate read that, and
        // it is session state rather than transcript content (it persists no
        // display row).
        RpcEvent::compaction_start { reason } => {
            st.compaction_seq += 1;
            // Its OWN id per compaction (a long session compacts many times; one
            // id per session would merge every card into one). Stamped like every
            // other id this state mints (ADR 0049), and shaped so no provider
            // `toolCallId` (`call_…`, `chatcmpl-tool-…`) can ever collide with it.
            let id = st.id(&format!("compact:{}", st.compaction_seq));
            st.open_compaction_id = Some(id.clone());
            st.announced_tool_calls.insert(id.clone());
            vec![
                json!({
                    "sessionUpdate": "tool_call",
                    "toolCallId": id,
                    "title": COMPACTION_TOOL,
                    "status": "in_progress",
                    // The trigger, as the card's summary line: which of
                    // `manual` / `threshold` / `overflow` opened this card is
                    // information the one-liner never carried.
                    "rawInput": { "reason": reason },
                }),
                json!({
                    "sessionUpdate": "compaction_update",
                    "active": true,
                    "reason": reason,
                }),
            ]
        }
        // The settle lands on the announced card when the pair is intact. A
        // `compaction_end` whose announcement was never seen (a resume, or a
        // Worker that started mid-compaction) still settles SOMETHING:
        // `compute_display_rows` and the reducer both treat an unknown id as a
        // new card, so the outcome is never silently dropped.
        RpcEvent::compaction_end {
            reason,
            result,
            aborted,
            error_message,
            ..
        } => {
            let id = st.open_compaction_id.take().unwrap_or_else(|| {
                // No announcement was seen (a resume, or a Worker that started
                // mid-compaction). Still a UNIQUE id — reusing one bare key for
                // every orphan settle would merge unrelated compactions into one
                // card, the very defect the per-compaction id exists to avoid.
                st.compaction_seq += 1;
                st.id(&format!("compact:{}", st.compaction_seq))
            });
            // `failed` on any error, exactly as a real tool settles; the reason
            // goes where `failureText` reads first (`details.error`), so the
            // header's Failed chip carries the actionable line ADR 0045 composes.
            let raw_output = match error_message {
                Some(m) => json!({
                    "content": [{ "type": "text", "text": m }],
                    "details": { "error": m },
                }),
                None => match result {
                    // pi's `CompactionResult.summary`, in the `AgentToolResult`
                    // shape `normalizeToolOutput` joins.
                    Some(r) => json!({
                        "content": [
                            { "type": "text", "text": r.get("summary").and_then(Value::as_str).unwrap_or_default() }
                        ]
                    }),
                    // `Skipped` — nothing old enough to summarize. Nothing
                    // failed, so it settles `completed`; an EMPTY body would
                    // render as "(no output)", which reads as a tool that ran
                    // and printed nothing rather than "there was nothing to do".
                    None => json!({
                        "content": [{
                            "type": "text",
                            "text": "Nothing old enough to summarize — the transcript is unchanged."
                        }]
                    }),
                },
            };
            vec![
                json!({
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": id,
                    "status": if *aborted || error_message.is_some() { "failed" } else { "completed" },
                    "rawOutput": raw_output,
                }),
                json!({
                    "sessionUpdate": "compaction_update",
                    "active": false,
                    "reason": reason,
                }),
            ]
        }
        // (ADR 0048) An Abandoned attempt's reply is retracted. The id is the one
        // the harness is ON — the retried call REUSES it (`message_start` fires
        // once per assistant message), which is precisely why the discard is a
        // protocol event and not something the reader may infer. Then the line
        // says so, on its own id: reusing the shared `system` key would weld it
        // onto the "Retrying (attempt 1/3…)" line this path emits immediately
        // before.
        RpcEvent::message_retracted => {
            // The SAME lazy form as the `message_update` arm (symmetry, not a
            // hot path — this runs once per discarded attempt).
            let mid = match st.current_message_id.clone() {
                Some(id) => id,
                None => st.id("default"),
            };
            st.retract_seq += 1;
            vec![
                json!({ "sessionUpdate": "message_retracted", "messageId": mid }),
                unique_system_chunk(
                    &st.id(&format!("retract:{}", st.retract_seq)),
                    "Discarded an incomplete reply",
                ),
            ]
        }
        RpcEvent::auto_retry_start {
            attempt,
            max_attempts,
            ..
        } => vec![system_chunk(
            &st.id("system"),
            format!("Retrying (attempt {attempt}/{max_attempts}…)"),
        )],
        RpcEvent::auto_retry_end { success, .. } => {
            vec![if *success {
                system_chunk(&st.id("system"), "Retry succeeded")
            } else {
                system_chunk(&st.id("system"), "Retry failed")
            }]
        }
        RpcEvent::extension_error { error, .. } => {
            vec![system_chunk(
                &st.id("system"),
                format!("Extension error: {error}"),
            )]
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
use std::sync::Arc;

#[cfg(test)]
use crate::storage::Db;

/// A temp-dir `Db` with a recorded `sessions` row (the `messages` rows FK to
/// `sessions`) — the fixture BOTH of this module's test suites need (one
/// `session_id`, one table, the display rows written through `SessionStore`).
///
/// The `TempDir` GUARD travels back with the `Db` and the caller keeps it
/// alive for the whole test (`let (db, _dir) = temp_db();` — the `mcp` tool
/// tests' `(T, tempfile::TempDir)` pattern). It used to be a hand-rolled
/// `temp_dir()/harness-display-test-<uuid>` that NOTHING ever removed, so every
/// run of either suite left two directories behind; `TempDir` deletes its dir on
/// drop, which only happens if a binding still holds it when the test ends.
#[cfg(test)]
fn temp_db() -> (Arc<Db>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temp dir");
    let db = Arc::new(Db::open(&dir.path().join("t.db")).expect("db should open"));
    db.record_session(&crate::types::SessionInfo {
        session_id: "s1".to_string(),
        cwd: std::path::PathBuf::from("/tmp"),
        capabilities: serde_json::json!({}),
        config_options: None,
        archived: false,
        title: None,
        context_usage: None,
        cost_usd: None,
        is_subagent: false,
    })
    .expect("record_session");
    (db, dir)
}

#[cfg(test)]
mod normalize_tests {
    use serde_json::{json, Value};

    use super::temp_db;
    use super::{
        compute_display_rows, is_bookkeeping_message_id, normalize, ThoughtState, TurnState,
        COMPACTION_TOOL,
    };
    use crate::agent::events::RpcEvent;
    use crate::agent::harness::store::{SessionStore, Store};
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    fn ev(v: serde_json::Value) -> RpcEvent {
        serde_json::from_value(v).expect("event should parse")
    }

    /// One frame's whole downstream pipeline: `compute_display_rows` + the
    /// write half (`SessionStore::persist_display` → `db.record_message`) —
    /// what the loop's driver does per frame (ADR 0025's split).
    fn persist_frames(
        store: &SessionStore,
        session_id: &str,
        frames: Vec<Value>,
        text_acc: &StdMutex<HashMap<String, String>>,
        tool_state: &StdMutex<HashMap<String, Value>>,
        thought_state: &StdMutex<ThoughtState>,
    ) {
        for frame in frames {
            let rows = compute_display_rows(&frame, text_acc, tool_state, thought_state);
            store
                .persist_display(session_id, &rows)
                .expect("display rows persist");
        }
    }

    /// The `messageId` role rule: a USER `message_start` does NOT advance
    /// the counter; the following ASSISTANT `message_start` numbers the
    /// first assistant chunk `m1` (a role-blind counter would make it
    /// `m2` and break the replay numbering).
    #[test]
    fn message_id_advances_only_for_assistant_messages() {
        let mut st = TurnState::stamped("t");
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
        assert_eq!(st.current_message_id.as_deref(), Some("t:m1"));

        let delta = ev(json!({
            "type": "message_update",
            "usage": null,
            "assistantMessageEvent": { "type": "text_delta", "contentIndex": 0, "delta": "Hel" },
        }));
        let frames = normalize(&delta, &mut st);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["sessionUpdate"], "agent_message_chunk");
        assert_eq!(
            frames[0]["messageId"], "t:m1",
            "the first assistant chunk keys the stamped m1"
        );
        assert_eq!(frames[0]["content"]["text"], "Hel");
    }

    /// A second assistant message advances the counter to `m2`; the
    /// `*_end` / `*_start` frames carry NO frame (the delta stream already
    /// delivered the content — re-emitting would double the text).
    #[test]
    fn end_and_start_frames_emit_nothing() {
        let mut st = TurnState::stamped("t");
        let _ = normalize(
            &ev(json!({ "type": "message_start", "message": { "role": "assistant" } })),
            &mut st,
        );
        let _ = normalize(
            &ev(json!({ "type": "message_start", "message": { "role": "assistant" } })),
            &mut st,
        );
        assert_eq!(st.current_message_id.as_deref(), Some("t:m2"));

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
        let mut st = TurnState::stamped("t");
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
        assert_eq!(frames[0]["messageId"], "t:m1");
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
    /// A compaction event carries TWO frames (issue #90 item 2): the
    /// `ToolCallCard` half of the pair (`tool_call` announce / `tool_call_update`
    /// settle) AND the `compaction_update` STATE frame ("a compaction is
    /// running", with the trigger `reason` — `manual` / `threshold` /
    /// `overflow`), which the renderer's indicator reads and which persists no
    /// display row. The `system_chunk` one-liners these assertions used to pin
    /// are GONE — see the normalize arms for why (the weld).
    #[test]
    fn bookkeeping_events_and_one_liners() {
        let mut st = TurnState::stamped("t");
        let compacting = normalize(
            &ev(json!({ "type": "compaction_start", "reason": "manual" })),
            &mut st,
        );
        assert_eq!(compacting.len(), 2, "the card announce + the state frame");
        assert_eq!(compacting[0]["sessionUpdate"], "tool_call");
        assert_eq!(compacting[0]["title"], COMPACTION_TOOL);
        assert_eq!(
            compacting[0]["toolCallId"], "t:compact:1",
            "the card id is stamped AND unique per compaction (ADR 0049)"
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
        assert_eq!(done.len(), 2, "the card settle + the state frame");
        assert_eq!(done[0]["sessionUpdate"], "tool_call_update");
        assert_eq!(
            done[0]["toolCallId"], "t:compact:1",
            "the settle lands on the announced card"
        );
        assert_eq!(
            done[1],
            json!({ "sessionUpdate": "compaction_update", "active": false, "reason": "manual" }),
            "the state frame closes the indicator, with the same reason"
        );

        // A SECOND compaction of the same turn opens its OWN card — the defect
        // the counter exists for (one shared id would merge every card, which is
        // what the shared `system` key did to the one-liners).
        let second = st.id(&format!("compact:{}", st.compaction_seq + 1));
        normalize(
            &ev(json!({ "type": "compaction_start", "reason": "threshold" })),
            &mut st,
        );
        assert_eq!(
            st.open_compaction_id.as_deref(),
            Some(second.as_str()),
            "each compaction gets its own card id, got {:?}",
            st.open_compaction_id
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
    /// A `compaction_end` whose `compaction_start` was never seen still lands
    /// (a resume, or a Worker that started mid-compaction): the settle becomes a
    /// card of its own rather than being dropped, because the outcome —
    /// especially a FAILED one carrying ADR 0045's actionable line — is the
    /// thing the user needs and there is no announcement to attach it to.
    ///
    /// The old version of this test pinned the opposite contract: that an
    /// `error_message` REPLACED the `"Compaction finished"` one-liner so the two
    /// `system_chunk`s could not weld into one bubble. The weld is now
    /// unreachable — a compaction emits no `system_chunk` at all — and each
    /// compaction gets its own card id, so repeated compactions cannot merge
    /// either (issue #90 item 2).
    #[test]
    fn a_compaction_end_with_no_announcement_still_settles_its_own_card() {
        let mut st = TurnState::stamped("t");
        let failed = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "overflow",
                "aborted": false,
                "willRetry": false,
                "errorMessage": "Context overflow: not enough older context to compact — try a larger-context model.",
            })),
            &mut st,
        );
        assert_eq!(failed.len(), 2, "the card settle + the state frame");
        assert_eq!(failed[0]["sessionUpdate"], "tool_call_update");
        assert_eq!(failed[0]["status"], "failed");
        assert_eq!(
            failed[0]["toolCallId"], "t:compact:1",
            "an orphan settle mints its OWN stamped id, not a shared bare key"
        );

        // A second orphan in the same turn must NOT land on the first card —
        // that is the merge the per-compaction counter exists to prevent.
        let second = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "threshold",
                "aborted": false,
                "willRetry": false,
            })),
            &mut st,
        );
        assert_eq!(
            second[0]["toolCallId"], "t:compact:2",
            "each compaction gets its own card, got {}",
            second[0]["toolCallId"]
        );
        assert_eq!(
            failed[0]["rawOutput"]["details"]["error"]
                .as_str()
                .unwrap_or_default(),
            "Context overflow: not enough older context to compact — try a larger-context model.",
            "the actionable message is the card's failure text, where failureText reads it"
        );
        assert_eq!(
            failed[1],
            json!({ "sessionUpdate": "compaction_update", "active": false, "reason": "overflow" }),
            "the state frame still closes the indicator"
        );
        assert!(
            !failed
                .iter()
                .any(|f| f["sessionUpdate"] == "agent_message_chunk"),
            "no system one-liner is emitted on any compaction path, got {failed:?}"
        );
    }

    /// (ADR 0048) A real discard is both the erase AND a line saying so, and
    /// the line carries its OWN messageId — the shared `system` key would weld
    /// it onto the retry line emitted immediately before it.
    #[test]
    fn a_retraction_erases_the_current_message_and_says_so() {
        let mut st = TurnState::stamped("t");
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
                json!({ "sessionUpdate": "message_retracted", "messageId": "t:m1" }),
                json!({
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": "Discarded an incomplete reply" },
                    "messageId": "t:retract:1",
                }),
            ]
        );
        assert_eq!(
            st.current_message_id.as_deref(),
            Some("t:m1"),
            "the retried call REUSES the id: a Retraction is not a new message"
        );
    }

    /// Two discards in one turn must not weld (ADR 0048), so the line's id
    /// increments per Retraction — the ids are unique, not shared.
    #[test]
    fn a_second_retraction_gets_its_own_line_id() {
        let mut st = TurnState::stamped("t");
        normalize(
            &RpcEvent::message_start {
                message: json!({ "role": "assistant" }),
            },
            &mut st,
        );
        let first = normalize(&RpcEvent::message_retracted, &mut st);
        let second = normalize(&RpcEvent::message_retracted, &mut st);
        assert_eq!(first[1]["messageId"], "t:retract:1");
        assert_eq!(second[1]["messageId"], "t:retract:2");
    }

    /// A Retraction with no message announced yet is still well-formed (the
    /// `default` id every other chunk falls back to).
    #[test]
    fn a_retraction_before_any_message_start_says_default() {
        let mut st = TurnState::stamped("t");
        let frames = normalize(&RpcEvent::message_retracted, &mut st);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["sessionUpdate"], "message_retracted");
        assert_eq!(frames[0]["messageId"], "t:default");
    }

    /// A stamp is a NAMESPACE, not a suffix to sort by: the same counter value
    /// in two incarnations must produce two different ids, or a resumed Worker
    /// is back in its predecessor's rows (ADR 0049 — the Resume restarts the
    /// loop's counters, so an unstamped `m1` overwrites the first Worker's
    /// `m1`).
    #[test]
    fn two_incarnations_mint_disjoint_ids_for_the_same_counter() {
        let mut st_a = TurnState::stamped("aaaaaa");
        let mut st_b = TurnState::stamped("bbbbbb");
        let start = || {
            ev(json!({
                "type": "message_start",
                "message": { "role": "assistant", "content": [] },
            }))
        };
        assert!(normalize(&start(), &mut st_a).is_empty());
        assert!(normalize(&start(), &mut st_b).is_empty());
        assert_eq!(
            st_a.current_message_id.as_deref(),
            Some("aaaaaa:m1"),
            "the counter is NAMED by the stamp, not sequenced by it"
        );
        assert_eq!(st_b.current_message_id.as_deref(), Some("bbbbbb:m1"));
        assert_eq!(
            st_a.msg_counter, st_b.msg_counter,
            "both incarnations are on counter 1 — the collision is the point"
        );
        assert_ne!(
            st_a.current_message_id, st_b.current_message_id,
            "the same counter value in two incarnations must not collide"
        );
    }

    /// (ADR 0049) TWO Worker lives for ONE session — the Resume case, and the
    /// defect this exists to stop. A Resume spawns a new Worker with a new
    /// `AgentLoop`, so `TurnState`'s counters restart at 0 and the resumed
    /// session's first reply is the `m1` its first Worker already used. The
    /// display store UPSERTs on `UNIQUE(session_id, kind, message_key)` and has
    /// no DELETE, so an unstamped second `m1` silently overwrote the first
    /// incarnation's rows, and ADR 0048's Retraction — which erases BY
    /// `messageId` — could erase history that is not its own.
    ///
    /// The claim, in words: for ONE `session_id` and ONE `Db`, every id the
    /// harness mints is namespaced by its incarnation stamp, so the same
    /// counter value in two incarnations is two display rows. A Retraction in
    /// incarnation B erases B's OWN text and thought rows and leaves
    /// incarnation A's text, thought AND system rows untouched — and nothing
    /// unstamped (`m1`, `m1#1`, `system`) is ever written.
    ///
    /// This is the seam where two Worker lives meet: two `TurnState`s with
    /// different stamps, two FRESH accumulator sets (a new Worker has new ones),
    /// one session id, one database.
    #[test]
    fn two_incarnations_of_one_session_never_share_a_display_row() {
        let (db, _dir) = temp_db();
        let store = SessionStore::new(db.clone());

        // ── incarnation A: the first Worker ────────────────────────────────
        let mut st_a = TurnState::stamped("aaaaaa");
        let a_text = StdMutex::new(HashMap::new());
        let a_tools = StdMutex::new(HashMap::new());
        let a_thoughts = StdMutex::new(ThoughtState::default());
        let events_a = [
            // Opens `aaaaaa:m1` (the counter's FIRST value).
            ev(json!({
                "type": "message_start",
                "message": { "role": "assistant", "content": [] },
            })),
            ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "text_delta", "contentIndex": 0, "delta": "history" },
            })),
            ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "thinking_delta", "contentIndex": 0, "delta": "think" },
            })),
            // The retry one-liner — the id family with NO counter to seed
            // (`system`), which a counter-seeding design would have left
            // unstamped and therefore shared.
            ev(json!({
                "type": "auto_retry_start",
                "attempt": 1,
                "maxAttempts": 3,
                "delayMs": 100,
                "errorMessage": "boom",
            })),
        ];
        for e in &events_a {
            persist_frames(
                &store,
                "s1",
                normalize(e, &mut st_a),
                &a_text,
                &a_tools,
                &a_thoughts,
            );
        }

        // ── incarnation B: the resumed Worker ──────────────────────────────
        // A SECOND, FRESH set of accumulators (what a new process has), the
        // SAME session id, the SAME database.
        let mut st_b = TurnState::stamped("bbbbbb");
        let b_text = StdMutex::new(HashMap::new());
        let b_tools = StdMutex::new(HashMap::new());
        let b_thoughts = StdMutex::new(ThoughtState::default());
        let events_b = [
            // The SAME counter value A used — the collision is the point.
            ev(json!({
                "type": "message_start",
                "message": { "role": "assistant", "content": [] },
            })),
            ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "text_delta", "contentIndex": 0, "delta": "parti" },
            })),
            // REQUIRED: the retraction arm erases only the segments recorded in
            // `ThoughtState.segments`, so without a thought there is no
            // `bbbbbb:m1#1` row to assert.
            ev(json!({
                "type": "message_update",
                "usage": null,
                "assistantMessageEvent": { "type": "thinking_delta", "contentIndex": 0, "delta": "partthink" },
            })),
            ev(json!({ "type": "message_retracted" })),
        ];
        for e in &events_b {
            persist_frames(
                &store,
                "s1",
                normalize(e, &mut st_b),
                &b_text,
                &b_tools,
                &b_thoughts,
            );
        }

        // ── what the one table actually holds ─────────────────────────────
        let rows: Vec<(String, String, String)> = db
            .messages_for("s1")
            .unwrap()
            .into_iter()
            .map(|m| (m.kind, m.message_key.unwrap_or_default(), m.payload_json))
            .collect();
        let row = |kind: &str, key: &str| -> (String, String, String) {
            rows.iter()
                .find(|(k, m, _)| k == kind && m == key)
                .cloned()
                .unwrap_or_else(|| panic!("no ({kind}, {key}) display row; the table is {rows:?}"))
        };

        // A's rows survived B's arrival entirely.
        assert!(
            row("agent-text", "aaaaaa:m1").2.contains("history")
                && row("agent-thought", "aaaaaa:m1#1").2.contains("think"),
            "incarnation A's text and thought rows are intact, the table is {rows:?}"
        );
        assert!(
            row("agent-text", "aaaaaa:system").2.contains("Retrying"),
            "the shared one-liner key is stamped too (it has no counter to \
             seed, so it is the family a counter-only design would leave \
             shared), the table is {rows:?}"
        );

        // B erased its OWN rows — and nothing else.
        assert_eq!(
            row("agent-text", "bbbbbb:m1").2,
            r#"{"text":""}"#,
            "B's Retraction emptied B's own text row"
        );
        assert_eq!(
            row("agent-thought", "bbbbbb:m1#1").2,
            r#"{"text":""}"#,
            "B's Retraction emptied B's own thought row"
        );
        // The discard line is a display row too — asserted so nobody later
        // "cleans it up".
        assert!(
            row("agent-text", "bbbbbb:retract:1")
                .2
                .contains("Discarded"),
            "the discard line is persisted under B's own stamp, the table is {rows:?}"
        );

        // Nothing unstamped was ever written.
        for unstamped in ["m1", "m1#1", "system"] {
            assert!(
                !rows.iter().any(|(_, k, _)| k == unstamped),
                "an unstamped id reached the display store: {unstamped}"
            );
        }

        // The two first-assistant-message rows COEXIST as TWO rows: both
        // incarnations counted to `m1` for one `session_id`, so the stamp is
        // the only thing keeping them apart.
        //
        // The EXACT TOTAL is the proof. Drop the stamp and `m1` / `m1#1`
        // collapse into ONE row each, leaving FOUR rows (`m1`, `m1#1`,
        // `system`, `retract:1`) where these SIX live side by side. Order of
        // failure when the stamp is dropped: the
        // `row("agent-text", "aaaaaa:m1")` LOOKUP above is the FIRST thing to
        // fail (`row` panics on an absent key), this count is the second.
        //
        // And deliberately NO `assert_ne!(A's payload, B's payload)` beside
        // it: rows are keyed by `(kind, message_key)`, so two rows carrying
        // `aaaaaa:m1` and `bbbbbb:m1` CANNOT be one overwritten row — the
        // count already states that claim — and these two payloads differ
        // only incidentally (A streamed `history`; B streamed `parti` and then
        // erased itself), so the assertion could not fire for the overwrite it
        // advertises. What each life's payload has to BE is asserted per key
        // ABOVE (`aaaaaa:m1` holds A's text, `bbbbbb:m1` is emptied by B's own
        // Retraction), which is the assertion that can actually fail.
        assert_eq!(
            rows.len(),
            6,
            "A's `m1` + `m1#1` + `system` PLUS B's `m1` + `m1#1` + `retract:1` \
             — six coexisting rows, two Worker lives that never touched each \
             other's keys; the table is {rows:?}"
        );
    }

    /// The bookkeeping set is `system` and `retract`, in either id shape — and
    /// `default` is NOT in it: that id carries real model text (a
    /// `message_update` before any assistant `message_start`), and the subagent
    /// capture must still hand it to the parent (ADR 0048's fourth consumer).
    #[test]
    fn the_bookkeeping_predicate_reads_both_id_shapes() {
        for id in ["system", "retract:1", "a3f9c1:system", "a3f9c1:retract:1"] {
            assert!(is_bookkeeping_message_id(id), "{id} is bookkeeping");
        }
        for id in ["a3f9c1:m1", "a3f9c1:m2#3", "default", "a3f9c1:default"] {
            assert!(
                !is_bookkeeping_message_id(id),
                "{id} carries model output, not bookkeeping"
            );
        }
        // The LOOSENESS is chosen, not accidental: the predicate scans the id's
        // `:`-separated fields for a WORD, it does not decode a POSITION. No
        // reachable id tells the two readings apart (`normalize` is the only
        // `messageId` writer and every id it mints is `{6 hex}:…`, and a stamp
        // can never BE one of the words — they contain non-hex characters), and
        // a stricter "strip a leading 6-hex field, then match" rule would
        // MISCLASSIFY the EMPTY-stamp ids `TurnState::default()` still mints
        // (`:system`, `:retract:1`). The pins below say so.
        assert!(
            is_bookkeeping_message_id("retract"),
            "the bare pre-stamp discard id is bookkeeping"
        );
        assert!(
            is_bookkeeping_message_id("anything:retract:9"),
            "the predicate matches a WORD, not a position"
        );
        assert!(
            is_bookkeeping_message_id(":retract:1"),
            "the EMPTY-stamp shape (`TurnState::default()`) is bookkeeping too \
             — a position-based rule would miss it"
        );
    }
    /// (issue #90 item 2) The DURABLE half: what a reload reads back. A frame
    /// the reducer renders but `compute_display_rows` never persists would show
    /// the summary live and lose it on reopen, so this drives the whole
    /// downstream pipeline (`normalize` → `compute_display_rows` →
    /// `persist_display`) and reads the `messages` table.
    ///
    /// The second compaction is the point of the test: the announce/settle pair
    /// must write a SECOND row, not UPSERT over the first. The one-liners this
    /// replaced shared `{stamp}:system` and appended, so a real database held
    /// every compaction of a session in one welded bubble.
    #[test]
    fn a_compaction_pair_persists_one_card_per_compaction_with_its_summary() {
        let (db, _dir) = temp_db();
        let store = SessionStore::new(db.clone());
        let mut st = TurnState::stamped("t");
        let text = StdMutex::new(HashMap::new());
        let tools = StdMutex::new(HashMap::new());
        let thoughts = StdMutex::new(ThoughtState::default());

        for e in [
            ev(json!({ "type": "compaction_start", "reason": "threshold" })),
            ev(json!({
                "type": "compaction_end",
                "reason": "threshold",
                "result": { "summary": "first summary text" },
                "aborted": false,
                "willRetry": false,
            })),
            ev(json!({ "type": "compaction_start", "reason": "overflow" })),
            ev(json!({
                "type": "compaction_end",
                "reason": "overflow",
                "result": { "summary": "second summary text" },
                "aborted": false,
                "willRetry": true,
            })),
        ] {
            persist_frames(
                &store,
                "s1",
                normalize(&e, &mut st),
                &text,
                &tools,
                &thoughts,
            );
        }

        let rows: Vec<(String, String, String)> = db
            .messages_for("s1")
            .unwrap()
            .into_iter()
            .map(|m| (m.kind, m.message_key.unwrap_or_default(), m.payload_json))
            .collect();
        let cards: Vec<&(String, String, String)> =
            rows.iter().filter(|(k, _, _)| k == "tool-call").collect();
        assert_eq!(
            cards.len(),
            2,
            "one row PER compaction (the welded single-row defect), got {rows:?}"
        );
        assert_eq!(cards[0].1, "t:compact:1");
        assert_eq!(cards[1].1, "t:compact:2");
        // The settle MERGED into the announced row (same key), and the summary
        // is the payload `rowToMessages` reads back into `rawOutput`.
        assert!(
            cards[0].2.contains("first summary text") && cards[0].2.contains("\"completed\""),
            "the first card holds its summary and settled status, got {}",
            cards[0].2
        );
        assert!(
            cards[1].2.contains("second summary text") && cards[1].2.contains("\"overflow\""),
            "the second card holds its OWN summary and its own trigger, got {}",
            cards[1].2
        );
        // And no welded system bubble exists at all.
        assert!(
            !rows
                .iter()
                .any(|(k, key, _)| k == "agent-text" && key.ends_with(":system")),
            "a compaction writes no system one-liner row, got {rows:?}"
        );
    }

    /// (issue #90 item 2) A compaction is presented like a tool: a card the user
    /// can expand to read what the compaction actually produced. Pre-fix the
    /// only trace was the welded one-liner (`Compacting context…Compaction
    /// finished`) and the summary text — the one thing worth reading — was
    /// NEVER rendered: `run_compaction` hardcoded `result: None` and the
    /// summary lived only in `native_messages` (the model's transcript), which
    /// no display row is built from.
    ///
    /// So a successful `compaction_end` carries its summary in `result` and the
    /// normalizer emits the two frames a `ToolCallCard` already knows how to
    /// render: the `tool_call` announcement (pending) and the
    /// `tool_call_update` that settles it with the summary as `rawOutput`.
    #[test]
    fn a_successful_compaction_emits_a_tool_card_carrying_the_summary() {
        let mut st = TurnState::stamped("t");
        let start = normalize(
            &ev(json!({ "type": "compaction_start", "reason": "threshold" })),
            &mut st,
        );
        // The announcement: a `tool_call` (the FIRST frame for an id must be the
        // announcement with a real title — see `announced_tool_calls`).
        let announce = start
            .iter()
            .find(|f| f["sessionUpdate"] == "tool_call")
            .expect("compaction_start announces a tool card, got {start:?}");
        // The title is the stable tool-ish NAME, not prose — the same contract
        // every real tool honours, so `ToolCallCardHeader` renders its own verb
        // ("Compacting" / "Compacted") and the resume merge key
        // (`tool-call|<id>|<title>|<status>`) stays byte-stable.
        assert_eq!(announce["title"], COMPACTION_TOOL);
        assert_eq!(announce["status"], "in_progress");
        // The trigger rides as the card's summary input.
        assert_eq!(announce["rawInput"]["reason"], "threshold");
        let card_id = announce["toolCallId"]
            .as_str()
            .expect("the card has an id")
            .to_string();
        assert!(
            card_id.starts_with("t:"),
            "the id is stamped like every other id this state mints (ADR 0049), got {card_id}"
        );
        // The STATE frame still ships — the indicator is a separate surface from
        // the card, and `composerLocked` reads it.
        assert!(
            start
                .iter()
                .any(|f| f["sessionUpdate"] == "compaction_update" && f["active"] == true),
            "the indicator frame survives, got {start:?}"
        );

        let summary = "User asked for X. Decided Y. Open task: Z.";
        let end = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "threshold",
                "result": { "summary": summary },
                "aborted": false,
                "willRetry": false,
            })),
            &mut st,
        );
        let settle = end
            .iter()
            .find(|f| f["sessionUpdate"] == "tool_call_update")
            .expect("compaction_end settles the card, got {end:?}");
        assert_eq!(
            settle["toolCallId"], card_id,
            "the update lands on the announced card (a new id would append a second one)"
        );
        assert_eq!(settle["status"], "completed");
        // The summary is the card's OUTPUT, in the `AgentToolResult` shape the
        // frontend's `normalizeToolOutput` joins (`content[]` text items).
        assert_eq!(settle["rawOutput"]["content"][0]["text"], summary);
        assert!(
            end.iter()
                .any(|f| f["sessionUpdate"] == "compaction_update" && f["active"] == false),
            "the indicator frame still closes, got {end:?}"
        );
    }

    /// A FAILED compaction settles the same card as `failed` and puts the
    /// actionable line in it, so a collapsed card shows the failure the way every
    /// other failed tool does — the reason reaches the header's Failed tooltip
    /// (`failureText` reads `details.error` first) and the expanded body.
    #[test]
    fn a_failed_compaction_settles_its_card_as_failed_with_the_reason() {
        let mut st = TurnState::stamped("t");
        let start = normalize(
            &ev(json!({ "type": "compaction_start", "reason": "overflow" })),
            &mut st,
        );
        let card_id = start
            .iter()
            .find(|f| f["sessionUpdate"] == "tool_call")
            .and_then(|f| f["toolCallId"].as_str())
            .expect("the card id")
            .to_string();

        let end = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "overflow",
                "aborted": true,
                "willRetry": true,
                "errorMessage": "the summary hit the output token limit",
            })),
            &mut st,
        );
        let settle = end
            .iter()
            .find(|f| f["sessionUpdate"] == "tool_call_update")
            .expect("the card is settled even on failure, got {end:?}");
        assert_eq!(settle["toolCallId"], card_id);
        assert_eq!(settle["status"], "failed");
        // `failureText` reads `details.error` first — that is where the reason goes,
        // so the header's Failed chip carries ADR 0045's actionable line, exactly
        // as every other failed tool does. It is no longer a `system_chunk`
        // one-liner: those welded into one bubble across repeated compactions.
        assert_eq!(
            settle["rawOutput"]["details"]["error"],
            "the summary hit the output token limit"
        );
        // The one-liners are gone: no `agent_message_chunk` at all.
        assert!(
            !end.iter()
                .any(|f| f["sessionUpdate"] == "agent_message_chunk"),
            "the compaction no longer writes a system one-liner, got {end:?}"
        );
    }

    /// A compaction with NO summary to show (a `Skipped` — nothing old enough to
    /// summarize) still settles the card it announced: an unsettled card is a
    /// spinner forever. It settles `completed` with no output, not `failed` —
    /// nothing failed.
    #[test]
    fn a_skipped_compaction_still_settles_its_card() {
        let mut st = TurnState::stamped("t");
        normalize(
            &ev(json!({ "type": "compaction_start", "reason": "manual" })),
            &mut st,
        );
        let end = normalize(
            &ev(json!({
                "type": "compaction_end",
                "reason": "manual",
                "aborted": false,
                "willRetry": false,
            })),
            &mut st,
        );
        let settle = end
            .iter()
            .find(|f| f["sessionUpdate"] == "tool_call_update")
            .expect("a skipped compaction closes its card, got {end:?}");
        assert_eq!(
            settle["status"], "completed",
            "nothing failed — there was simply nothing old enough to summarize"
        );
        assert_eq!(
            settle["rawOutput"]["content"][0]["text"]
                .as_str()
                .unwrap_or_default(),
            "Nothing old enough to summarize — the transcript is unchanged.",
            "the body says why, rather than rendering as '(no output)'"
        );
        assert!(
            !end.iter()
                .any(|f| f["sessionUpdate"] == "agent_message_chunk"),
            "no one-liner on the skip path either, got {end:?}"
        );
        // The indicator still closes — that is what unlocks the composer.
        assert!(
            end.iter()
                .any(|f| f["sessionUpdate"] == "compaction_update" && f["active"] == false),
            "the state frame still closes the indicator, got {end:?}"
        );
    }
}

#[cfg(test)]
mod compute_display_rows_tests {
    use serde_json::json;

    use super::temp_db;
    use super::{compute_display_rows, ThoughtState};
    use crate::agent::harness::store::{SessionStore, Store};
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// The `persist_update` split (ADR 0025) GOLDEN test:
    /// `compute_display_rows` on a canned normalized update + canned
    /// accumulators yields the same `Vec<DisplayRow>` the old
    /// `persist_update` wrote (the normalize + accumulator logic moved
    /// verbatim — `SqliteStore::persist_display` applies the rows via
    /// `db.record_message`, so the `messages` rows are identical).
    #[test]
    fn compute_display_rows_matches_the_legacy_persist_update_writes() {
        let (db, _dir) = temp_db();
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

//! The OpenAI Responses stack: `OpenAiResponsesProvider`, its request-body
//! / `input` mappers, and the `ResponsesStream` `event:`-tagged SSE parser.
//!
//! The wire is frozen (ADR 0024); the transport discipline (client,
//! status map, LiteLLM headers) lives in [`super::transport`].

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use serde_json::json;
use serde_json::Value;

use super::openai::{
    emit_harness_line, parse_arguments, tool_call_dropped_with_no_name,
    tool_calls_dropped_at_stream_error, LineAssembler,
};
use super::transport::{build_client, litellm_headers, map_response};
use super::types::{
    cost_usd_from_usage, parse_error_chunk, ChatRole, FinishReason, MessageContent, ModelRequest,
    Provider, ProviderError, ProviderEvent, ToolCall, ToolCallDelta, Usage,
};
use crate::tools::ContentBlock;

/// The accumulated per-item tool-call state for the OpenAI Responses
/// wire (a `function_call` output item: the `id` + `name` arrive at
/// `output_item.added`, the `arguments` JSON text accumulates as
/// `function_call_arguments.delta` fragments, and the
/// `output_item.done` item carries the COMPLETE `arguments` —
/// authoritative, overwriting the accumulated text).
#[derive(Default, Clone)]
struct ResponsesToolAcc {
    id: Option<String>,
    name: Option<String>,
    /// The argument fragments concatenated (JSON text — the
    /// `output_item.done` item OVERWRITES it with the complete value).
    arguments: String,
    /// The `output_item.done` item's `ToolCall` was emitted already (the
    /// stream-end flush skips `done` accs — no duplicate `ToolCall` — and
    /// a `done` acc suppresses the stream-end synthetic `Done(Error)`:
    /// the call is complete and actionable, so the `AgentLoop` continues
    /// the turn with it instead of retrying the model call).
    done: bool,
}

/// The OpenAI Responses SSE parser (ADR 0024): turns raw response bytes
/// into [`ProviderEvent`]s. The wire is `event:`-TAGGED like Anthropic
/// (every `data:` line is preceded by an `event: <name>` line), but the
/// event names + payload shapes are the Responses API's:
/// `response.output_text.delta` (the text fragments),
/// `response.reasoning_summary_text.delta` (the reasoning fragments),
/// `response.output_item.added` (a `function_call` item — the `id` +
/// `name`), `response.function_call_arguments.delta` (the `arguments`
/// fragments), `response.output_item.done` (the complete `function_call`
/// item — authoritative `arguments`), `response.completed` (the
/// `usage` and `status: "completed"`), `response.incomplete` (the `usage`
/// and `incomplete_details.reason`), `response.failed` (the `usage`), and
/// an in-stream `error` event (terminal).
struct ResponsesStream {
    /// The raw byte stream (`reqwest::bytes_stream`, mapped to `Vec<u8>`).
    raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>>,
    /// The raw-bytes line assembler (the multi-byte-codepoint safety —
    /// finding 9).
    lines: LineAssembler,
    /// Events parsed but not yet delivered.
    pending: VecDeque<ProviderEvent>,
    /// The current SSE `event:` name (a `data:` line without one is
    /// dispatched on its `type` field instead — the Responses events
    /// carry a `type` matching the event name).
    event: Option<String>,
    /// Per-item tool-call accumulation (the `output_index` of a
    /// `function_call` item; the `output_item.done` emits the `ToolCall`
    /// directly, the stream end flushes the not-`done` accs).
    tool_acc: BTreeMap<u32, ResponsesToolAcc>,
    /// A terminal event was seen (`response.completed` /
    /// `response.incomplete` / `response.failed` / `error` — no synthetic
    /// `Done` at stream end). Also the latch the three TOOL-CALL
    /// ACCUMULATION arms test: a terminal event has already flushed this
    /// response's calls and emptied the map, so an item event after it is
    /// dropped rather than accumulated into a call nobody will flush (or
    /// flushed a second time — a duplicate dispatch).
    terminated: bool,
    /// A combined `Usage` was emitted (exactly once — the first terminal
    /// event to carry a `response.usage` with at least one token count).
    usage_emitted: bool,
    /// A terminal `Done` carrying a NON-`Stop` disposition (`Length` or
    /// `Error`) was already delivered, so NO later terminal's `Done` may
    /// follow it (see [`Self::push_terminal_done`] — a second `Done`
    /// overwrites the first downstream, which makes a never-retried
    /// truncation retryable and makes a FAILED response report success).
    sticky_done_emitted: bool,
    /// The raw stream is exhausted (the stream ends after `pending`).
    exhausted: bool,
}

/// The `item.type` of an item event, for the post-terminal discard line.
///
/// A protocol discriminator (`"function_call"` / `"message"` /
/// `"reasoning"`) that says which kind of call was lost; `"<none>"` when
/// the event carries no readable `item.type` at all, which is itself the
/// interesting answer. It is provider-supplied, so it is a string and not
/// an enum — but the line it feeds goes to stderr and thence to
/// `debuglog::log_truncated`, which caps every line at its own
/// `MAX_LINE_BYTES`, so an absurd value is bounded by the sink rather than
/// needing a second cap here.
fn item_type_label(v: &Value) -> String {
    v.get("item")
        .and_then(|i| i.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("<none>")
        .to_string()
}

impl ResponsesStream {
    fn new(raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>>) -> Self {
        Self {
            raw,
            lines: LineAssembler::new(),
            pending: VecDeque::new(),
            event: None,
            tool_acc: BTreeMap::new(),
            terminated: false,
            usage_emitted: false,
            sticky_done_emitted: false,
            exhausted: false,
        }
    }

    /// Append a raw chunk; process every complete line it contains.
    fn push(&mut self, chunk: &[u8]) {
        for line in self.lines.push(chunk) {
            self.handle_line(&line);
        }
    }

    /// Handle one line (an `event:` line sets the current event name —
    /// it PRECEDES its `data:` line; a `data:` line dispatches on it.
    /// `:`-comment lines and other fields are ignored.)
    fn handle_line(&mut self, line: &str) {
        let line = line.trim();
        if let Some(event) = line.strip_prefix("event:") {
            let event = event.trim();
            if !event.is_empty() {
                self.event = Some(event.to_string());
            }
            return;
        }
        let Some(payload) = line.strip_prefix("data:") else {
            return;
        };
        let payload = payload.trim();
        if payload.is_empty() {
            return;
        }
        let Ok(v) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        self.handle_chunk(&v);
    }

    /// Handle one parsed event (the `event:` tag is the discriminator;
    /// the `type` field is the fallback when the tag is absent — every
    /// Responses event carries a `type` matching its event name).
    fn handle_chunk(&mut self, v: &Value) {
        let kind = self
            .event
            .as_deref()
            .or_else(|| v.get("type").and_then(|t| t.as_str()));
        match kind {
            Some("response.output_text.delta") => {
                if let Some(delta) = v.get("delta").and_then(|d| d.as_str()) {
                    self.pending
                        .push_back(ProviderEvent::TextDelta(delta.to_string()));
                }
            }
            Some("response.reasoning_summary_text.delta") => {
                if let Some(delta) = v.get("delta").and_then(|d| d.as_str()) {
                    self.pending
                        .push_back(ProviderEvent::ThinkingDelta(delta.to_string()));
                }
            }
            Some("response.output_item.added") => {
                // A terminal event has already flushed this response's
                // tool calls and emptied the accumulator map, so an item
                // event AFTER the terminal cannot contribute to a call
                // anyone will ever see: it would build a FRESH acc on the
                // emptied map and either be flushed a second time (a
                // duplicate `ToolCall` → a duplicate dispatch — no `id`
                // dedupe downstream) or never flushed at all. The
                // terminal ENDS the response; nothing after it is
                // accumulated. (UNVERIFIED upstream — per the Responses
                // spec `response.completed` is final and every
                // `output_item.done` precedes it; this gate is defensive
                // hardening, not a fix for an observed wire.)
                //
                // The discard is LOUD, and that is load-bearing. What this
                // gate prevents is a duplicate `ToolCall` for a call the
                // terminal already delivered, and `run_tool_batch` has no `id`
                // dedupe — the failure mode it guards is a SECOND side effect
                // on a payment path. If the gate ever engages in the field we
                // have to be able to tell, and a bare `return` leaves no trace
                // anywhere: it returns BEFORE touching `tool_acc`, so there is
                // no accumulator residue to find downstream either. Silent,
                // this hardening can regress from "loud duplicate write" to
                // "silently lost call" — the softer failure, not the safer one.
                //
                // `eprintln!`, deliberately, and not `debuglog::log` /
                // `diagnostics::record` (the asymmetry `provider/mod.rs`'s
                // "Why two sinks" documents in full): the Worker's stderr is
                // drained by a task in `worker/client.rs` into
                // `debuglog::log_truncated`, which makes this line the only
                // Worker-side sink that survives at all — and that sink is
                // itself gated behind `ARCHIMEDES_DEBUG`, so in a DEFAULT
                // launch the Worker drops it. That is accepted. `diagnostics`
                // is the always-on channel and is NOT used here: its rule is
                // reviewed call sites at low volume, and one line per dropped
                // item on a stream is not always-on-worthy volume for a shape
                // that has never been observed on a real wire.
                if self.terminated {
                    eprintln!(
                        "harness: responses post-terminal item dropped: \
                         `response.output_item.added` after a terminal event, \
                         item type {} — the terminal already flushed this \
                         response's calls",
                        item_type_label(v)
                    );
                    return;
                }
                let Some(item) = v.get("item") else {
                    return;
                };
                if item.get("type").and_then(|t| t.as_str()) == Some("function_call") {
                    // The `output_index` keys the acc (`0` when absent —
                    // a well-formed stream always carries it).
                    let index = v
                        .get("output_index")
                        .and_then(|i| i.as_u64())
                        .map(|i| i as u32)
                        .unwrap_or(0);
                    let mut acc = ResponsesToolAcc::default();
                    if let Some(id) = item.get("id").and_then(|i| i.as_str()) {
                        acc.id = Some(id.to_string());
                    }
                    if let Some(name) = item.get("name").and_then(|n| n.as_str()) {
                        acc.name = Some(name.to_string());
                    }
                    // The `arguments` (a JSON string, usually `""`) seed
                    // the accumulator — the `output_item.done` item
                    // overwrites it with the complete value.
                    if let Some(args) = item.get("arguments").and_then(|a| a.as_str()) {
                        acc.arguments = args.to_string();
                    }
                    self.tool_acc.insert(index, acc.clone());
                    // The `AgentLoop` renders the tool call as soon as
                    // its `name` is known (the `arguments` JSON still
                    // streams) — the `ToolCallDelta` carries the `id` +
                    // `name` (+ the `arguments` when non-empty).
                    self.pending
                        .push_back(ProviderEvent::ToolCallDelta(ToolCallDelta {
                            index,
                            id: acc.id.clone(),
                            name: acc.name.clone(),
                            arguments: if acc.arguments.is_empty() {
                                None
                            } else {
                                Some(acc.arguments.clone())
                            },
                        }));
                }
                // `message` / `reasoning` items: their content streams via
                // the `*_delta` events — nothing to record at `added`.
            }
            Some("response.function_call_arguments.delta") => {
                // Gate as above: after a terminal the accumulator map is
                // flushed and empty, so a late fragment would open a
                // fresh acc whose `arguments` can never reach a `ToolCall`
                // (and the `ToolCallDelta` would render a card for a call
                // that was already delivered — or, with a name in the
                // item, be flushed a second time).
                //
                // Loud for the same reason as the arm above, and this is the
                // arm most likely to fire in the field — a proxy that
                // re-orders the tail of the stream sends fragments after the
                // terminal without ever re-sending the item. There is no item
                // type on this event, so the line names the `output_index`
                // instead: which call the provider was still streaming into is
                // the one fact that identifies the loss.
                if self.terminated {
                    eprintln!(
                        "harness: responses post-terminal item dropped: \
                         `response.function_call_arguments.delta` after a \
                         terminal event, output_index {} — the terminal \
                         already flushed this response's calls",
                        v.get("output_index")
                            .and_then(|i| i.as_u64())
                            .map(|i| i.to_string())
                            .unwrap_or_else(|| "<none>".to_string())
                    );
                    return;
                }
                let Some(index) = v
                    .get("output_index")
                    .and_then(|i| i.as_u64())
                    .map(|i| i as u32)
                else {
                    return;
                };
                let Some(fragment) = v.get("delta").and_then(|d| d.as_str()) else {
                    return;
                };
                self.tool_acc
                    .entry(index)
                    .or_default()
                    .arguments
                    .push_str(fragment);
                self.pending
                    .push_back(ProviderEvent::ToolCallDelta(ToolCallDelta {
                        index,
                        id: None,
                        name: None,
                        arguments: Some(fragment.to_string()),
                    }));
            }
            Some("response.output_item.done") => {
                // The third accumulation arm, gated for the same reason
                // and the one that actually bit: post-terminal the acc map
                // is EMPTY, so the done item no longer upgrades an
                // in-flight acc — it CREATES one. With `id` + `name` on
                // the item (or falling back to nothing) that emits a
                // SECOND `ToolCall` for a call the terminal already
                // delivered — same `id`, or an empty one — and
                // `run_tool_batch` has no `id` dedupe, so a `write` runs
                // twice. (UNVERIFIED upstream — the spec orders every
                // `output_item.done` BEFORE `response.completed`; this is
                // defensive.)
                //
                // Loud, and this is the gate the whole hardening was written
                // for: the duplicate `write` on a payment path. A suppressed
                // call that leaves no trace is the worse outcome, because the
                // duplicate we would have caught becomes a call the user never
                // sees and never learns was dropped. Same `eprintln!`-not-
                // `diagnostics` reasoning as the two arms above.
                if self.terminated {
                    eprintln!(
                        "harness: responses post-terminal item dropped: \
                         `response.output_item.done` after a terminal event, \
                         item type {} — this is the arm that duplicates a \
                         dispatch; the call was already delivered by the \
                         terminal",
                        item_type_label(v)
                    );
                    return;
                }
                let Some(item) = v.get("item") else {
                    return;
                };
                if item.get("type").and_then(|t| t.as_str()) == Some("function_call") {
                    let index = v
                        .get("output_index")
                        .and_then(|i| i.as_u64())
                        .map(|i| i as u32)
                        .unwrap_or(0);
                    let acc = self.tool_acc.entry(index).or_default();
                    // The done item carries the COMPLETE `arguments` (a
                    // JSON string) — AUTHORITATIVE: overwrite the
                    // accumulated text (a proxy that sent no
                    // `arguments.delta` events still yields a complete
                    // call), and mark the acc `done` (the stream-end
                    // flush skips it — no duplicate `ToolCall`; and a
                    // `done` acc suppresses the stream-end synthetic
                    // `Done(Error)` — the call is complete and actionable,
                    // so the `AgentLoop` continues the turn with it
                    // instead of retrying the model call).
                    if let Some(args) = item.get("arguments").and_then(|a| a.as_str()) {
                        acc.arguments = args.to_string();
                    }
                    // `id` / `name` fall back to the acc's (a proxy may
                    // omit them on the done item); a call with NO `name`
                    // cannot be dispatched — skip the item entirely (the
                    // acc stays not-`done`: the item never completed a
                    // dispatchable call, so a stream end without a
                    // terminal event still gets the synthetic
                    // `Done(Error)`).
                    let id = item
                        .get("id")
                        .and_then(|i| i.as_str())
                        .map(String::from)
                        .or_else(|| acc.id.clone());
                    let name = item
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(String::from)
                        .or_else(|| acc.name.clone());
                    if let Some(name) = name {
                        acc.done = true;
                        // (issue #193) The defect rides the call — see
                        // `parse_arguments`.
                        let (arguments, defect) = parse_arguments(&acc.arguments);
                        self.pending.push_back(ProviderEvent::ToolCall(ToolCall {
                            id: id.unwrap_or_default(),
                            name,
                            arguments,
                            defect,
                        }));
                    } else {
                        // … and it now SAYS so. The comment above has always
                        // known a `function_call` item with no `name` cannot be
                        // dispatched; the arm just skipped it in silence, which
                        // is the exact shape this change exists to close.
                        //
                        // The PARSE site is not redundant with the flush-time
                        // line. They name two different facts a supporter has to
                        // be able to tell apart: here the PROVIDER sent a
                        // COMPLETED item with no `name` in it (a proxy that
                        // stripped it, a gateway's schema mismatch), where the
                        // flush line says the acc never had one at all. `stage=`
                        // is what separates them — and a healthy turn's done item
                        // takes the `name`-present branch above, so this `else`
                        // never fires on one.
                        emit_harness_line(&tool_call_dropped_with_no_name(
                            "responses",
                            "output-item-done",
                            index,
                            id.as_deref(),
                            &acc.arguments,
                        ));
                    }
                }
                // `message` / `reasoning` items: nothing (their content
                // already streamed via the `*_delta` events).
            }
            Some("response.completed") => {
                // The terminal event: the `usage` (when at least one side
                // is present) PRECEDES the `Done`.
                self.emit_terminal_usage(v);
                // The tool calls flush BEFORE the `Done` — the ordering
                // invariant the `openai` and `anthropic` wires already
                // hold (`Usage` → tool calls → `Done`). Without this the
                // stream-end flush is UNREACHABLE (it sits behind
                // `!this.terminated`, set here), so a `function_call` in
                // flight at the terminal was lost on every healthy
                // `completed`. The order between this and the
                // `terminated` write below is not load-bearing —
                // `flush_tool_calls` does not consult the flag — it is
                // readability only; what matters is that the flush
                // precedes the `Done`.
                self.flush_tool_calls();
                self.terminated = true;
                let status = v
                    .get("response")
                    .and_then(|r| r.get("status"))
                    .and_then(|s| s.as_str());
                if status == Some("completed") {
                    self.push_terminal_done(FinishReason::Stop);
                } else {
                    // A `terminated` stream always carries a finish
                    // reason. A `response.completed` whose `status` is
                    // absent or anything else is NOT a success: pushing
                    // nothing left `acc.finish` at `None`, bypassed the
                    // `Length` gate and the fabrication guard, and let
                    // the turn fall through into `note_success` and the
                    // success triple — a SILENT SUCCESS with a vanished
                    // tool call. (Defensive/proxy shape: no captured
                    // payload shows a real backend sending a `completed`
                    // without `status: "completed"`.)
                    self.push_terminal_done(FinishReason::Error);
                }
            }
            Some("response.incomplete") => {
                self.emit_terminal_usage(v);
                // Same in-arm flush: a truncated response still delivers
                // the call it accumulated (the truncation notice must not
                // arrive without the tool card).
                self.flush_tool_calls();
                self.terminated = true;
                // `max_output_tokens` → `Length`; any other reason (or an
                // absent one) → `Error`.
                let reason = v
                    .get("response")
                    .and_then(|r| r.get("incomplete_details"))
                    .and_then(|d| d.get("reason"))
                    .and_then(|r| r.as_str());
                let finish = if reason == Some("max_output_tokens") {
                    FinishReason::Length
                } else {
                    FinishReason::Error
                };
                self.push_terminal_done(finish);
            }
            Some("response.failed") => {
                self.emit_terminal_usage(v);
                // … and a failed response is not a reason to swallow the
                // call the model had already committed to.
                self.flush_tool_calls();
                self.terminated = true;
                self.push_terminal_done(FinishReason::Error);
            }
            Some("error") => {
                // An in-stream `error` event (the HTTP status was 200 —
                // the error is IN the stream). Terminal for an
                // UNCLASSIFIABLE body: the Responses `error` object has no
                // stable `type` taxonomy, and a mid-stream error after a
                // partial response must NOT be blindly retried — the
                // `AgentLoop` has already consumed the partial deltas, so
                // the `RetryPolicy` sees a non-retryable end. A `None`
                // (no `error` object the classifier can read at all), a
                // `Retryable` or a `Fatal` therefore stay the bare terminal
                // `Done(Error)`: pushing an `Error` for those would let the
                // ladder re-issue a turn whose deltas are already spent, or
                // settle a tier this wire cannot stand behind.
                //
                // EXCEPT the two tiers `parse_error_chunk` (types.rs)
                // recognises with confidence — ADR 0045's context overflow
                // and auth — which upgrade to a REAL
                // `ProviderEvent::Error` so the loop's mid-stream recovery
                // works on the Responses wire too: an overflow is compacted
                // IN the turn and never ladder-retried
                // (`context_overflow_is_never_retried`), and an auth failure
                // is returned verbatim in one call and keys the settings UI
                // (ADRs 0081 / 0082 / 0045). Exactly one event is pushed
                // either way — never both an `Error` and a `Done`.
                self.exhausted = true;
                self.terminated = true;
                // The flush runs here too, BEFORE the match: this arm can
                // push a `ProviderEvent::Error` instead of a `Done`, and
                // its `terminated = true` (written above, so it covers
                // BOTH branches of the match) makes the stream-end flush
                // unreachable — without this line an in-flight call
                // vanished on the error path as well.
                self.flush_tool_calls();
                match parse_error_chunk(v) {
                    Some(e @ (ProviderError::ContextOverflow(_) | ProviderError::Auth(_))) => {
                        self.pending.push_back(ProviderEvent::Error(e));
                    }
                    _ => self.push_terminal_done(FinishReason::Error),
                }
            }
            _ => {
                // `response.created` / `response.output_item.added`
                // (non-`function_call`) / unknown: no event to deliver.
            }
        }
        // Consume the `event:` tag (a `data:` line belongs to the
        // PRECEDING `event:` line; the next `event:` line starts a new
        // event — the tags and the `data:` lines alternate on the wire).
        self.event = None;
    }

    /// Emit the terminal `Usage` ONCE — a doubled terminal event (a proxy
    /// re-sending `response.completed`, or an `incomplete` followed by a
    /// `completed`) must not push a second frame the roll-up would sum as
    /// money that was not spent. The `usage_emitted` flag guards all THREE
    /// terminal arms (`response.completed` / `response.incomplete` /
    /// `response.failed`) — `terminated` does not: it is consulted only at
    /// stream end, and it is set AFTER this push (the order is
    /// coincidental, not protective). The counts gate is UNCHANGED:
    /// `response_usage`'s `(None, None) => None` still drops a cost-only
    /// `response.usage` (pinned by
    /// `responses_sse_cost_with_no_token_counts_emits_no_usage_event`), so
    /// this helper is NOT a second `Usage` producer.
    ///
    /// The early `return`s are safe here (unlike the openai parser, whose
    /// guard WRAPS its inline push): this is a standalone fn whose entire
    /// body is the emission, so returning skips nothing the caller must
    /// still do — each terminal arm keeps its own `terminated = true` and
    /// its `Done` logic after this call.
    fn emit_terminal_usage(&mut self, v: &Value) {
        if self.usage_emitted {
            return;
        }
        let Some(usage) = self.response_usage(v) else {
            return;
        };
        // The cost rides the SAME terminal `response.usage` object the
        // counts came from — one more element on this push, no new
        // emission path.
        self.usage_emitted = true;
        self.pending
            .push_back(ProviderEvent::Usage(usage, self.response_cost(v)));
    }

    /// The `usage` of a terminal event's `response` object (the wire
    /// `u64` counts saturate to `u32::MAX — a silent `as u32` would
    /// TRUNCATE); a `None` side becomes `0`; `None` when NEITHER side is
    /// present (no `Usage` event — a zero-filled `Usage` would
    /// misreport).
    fn response_usage(&self, v: &Value) -> Option<Usage> {
        let usage = v.get("response")?.get("usage")?;
        let input = usage
            .get("input_tokens")
            .and_then(|t| t.as_u64())
            .map(|t| u32::try_from(t).unwrap_or(u32::MAX));
        let output = usage
            .get("output_tokens")
            .and_then(|t| t.as_u64())
            .map(|t| u32::try_from(t).unwrap_or(u32::MAX));
        match (input, output) {
            (Some(input), Some(output)) => Some(Usage {
                input_tokens: input,
                output_tokens: output,
            }),
            (Some(input), None) => Some(Usage {
                input_tokens: input,
                output_tokens: 0,
            }),
            (None, Some(output)) => Some(Usage {
                input_tokens: 0,
                output_tokens: output,
            }),
            (None, None) => None,
        }
    }

    /// The per-call cost (USD) of a terminal event's `response` object
    /// (D4, AMENDED: the money rides the BODY's usage object, not a
    /// header). A SIBLING of [`Self::response_usage`] rather than folded
    /// into it, so that helper's ADR 0046 arithmetic — including its
    /// "absent side becomes 0" arms and its `(None, None) => None` — stays
    /// textually untouched. `None` when the provider reported no figure:
    /// not zero, never a clamp.
    fn response_cost(&self, v: &Value) -> Option<f64> {
        v.get("response")
            .and_then(|r| r.get("usage"))
            .and_then(cost_usd_from_usage)
    }

    /// Push a terminal event's `Done`, ONCE per disposition: a NON-`Stop`
    /// disposition is sticky, so a later terminal's `Done` is dropped rather
    /// than overwriting it.
    ///
    /// Why the latch exists at all: the finish reason is not merged
    /// downstream, it is REWRITTEN (`acc.finish = Some(f)` is unconditional),
    /// so last-terminal-won. A `response.incomplete{max_output_tokens}`
    /// followed by a statusless `response.completed` therefore turned
    /// `Length` into `Error` — and `Error` routes through the fabrication
    /// guard to a `Retryable` disposition, so `retry_abandoned_attempt`
    /// RE-ISSUED a request whose response was already truncated. ADR 0108
    /// Decision 1 forbids replaying a truncated response, and a truncation is
    /// strictly more specific than a generic error: the first terminal knew
    /// WHY the turn ended.
    ///
    /// Why the latch covers `Error` and not only `Length`: the same
    /// rewrite, read the other way round, is worse.
    /// `response.failed` followed by `response.completed{status:"completed"}`
    /// yielded `Error → Stop`, and `Stop` is the SUCCESS disposition — the
    /// turn takes the success triple and a response the provider called a
    /// FAILURE reports as a completed turn. That is precisely the fabrication
    /// the fabrication guard on `acc.finish == Some(Error)` exists to refuse,
    /// reached by a doubled terminal rather than by a lying provider. The rule
    /// is therefore about specificity, not about truncation: `Length` and
    /// `Error` are assertions about WHY the turn ended, `Stop` is the absence
    /// of one, and a later event cannot assert an absence over an earlier
    /// event's finding.
    ///
    /// The latch stays deliberately narrow in the two directions that are not
    /// lies. A doubled `Done(Stop)` (the residue `sse_single_usage_chunk`
    /// documents) is a benign stop-reason duplication and stays as it was —
    /// collapsing it would change a vector a test records on purpose. And
    /// `Error → Error` still passes through: the second terminal repeats the
    /// first's disposition, so nothing is overwritten and the event vector
    /// keeps describing what the wire actually sent. Nor is the `error` arm's
    /// `ProviderEvent::Error` push routed through here at all: an
    /// `Auth` / `ContextOverflow` tier is not a finish reason and must reach
    /// the loop even after a truncation was reported.
    fn push_terminal_done(&mut self, finish: FinishReason) {
        if self.sticky_done_emitted {
            return;
        }
        if !matches!(finish, FinishReason::Stop) {
            self.sticky_done_emitted = true;
        }
        self.pending.push_back(ProviderEvent::Done(finish));
    }

    /// Emit the accumulated tool calls as complete `ToolCall` events
    /// (dropping any acc that never arrived with a `name` — a
    /// `function_call` item without a `name` cannot be dispatched — and
    /// any `done` acc: its `output_item.done` already emitted the
    /// `ToolCall`, flushing it again would duplicate it).
    ///
    /// Runs at EVERY terminal arm (`response.completed` /
    /// `response.incomplete` / `response.failed` / `error`) as well as at
    /// stream end — meaning every PARSED terminal event, not every way
    /// this stream can end: a mid-stream TRANSPORT failure also ends it
    /// and does NOT flush (`poll_next`'s `Err` arm sets `exhausted`, so
    /// the `Ready(None)` arm — the only other caller — is never reached),
    /// and the in-flight call is dropped with it. That is survivable
    /// rather than a leak: the retry re-issues the whole request, and a
    /// call dropped here was never ANNOUNCED, so nothing dangles waiting
    /// for a `function_call_output`.
    ///
    /// The in-arm call is what the `openai` and `anthropic` parsers
    /// already did and this one did not: `terminated` is consulted only
    /// by the stream-end `if !this.terminated` block, every terminal arm
    /// sets it, so the stream-end flush never ran on a terminal event and
    /// a `function_call` in flight at one was LOST. The in-arm flush makes
    /// each terminal event deliver `Usage` → tool calls → `Done` WITHIN
    /// itself, the same order the other two wires hold. Per terminal
    /// EVENT, not per stream: the `error` arm never calls
    /// `emit_terminal_usage`, so an `error`-then-`completed` stream emits
    /// its `Done` BEFORE its `Usage`.
    fn flush_tool_calls(&mut self) {
        for (index, acc) in std::mem::take(&mut self.tool_acc).into_iter() {
            // Deliberate DE-DUPLICATION, not a loss: the `output_item.done` arm
            // already emitted this acc's `ToolCall`. Left silent on purpose —
            // this branch fires on EVERY healthy tool call on this wire, and the
            // `a_healthy_tool_call_turn_records_no_drop_line` test reddens if it
            // is ever instrumented.
            if acc.done {
                continue;
            }
            // The drop is LOUD. It was a bare `continue` — the silent-discard
            // shape this whole change exists to close: the model made a call, its
            // `arguments` fragments streamed, and the turn settled as a success
            // having dispatched nothing, with no line anywhere.
            let Some(name) = acc.name else {
                emit_harness_line(&tool_call_dropped_with_no_name(
                    "responses",
                    "flush",
                    index,
                    acc.id.as_deref(),
                    &acc.arguments,
                ));
                continue;
            };
            // (issue #193) The defect rides the call — see `parse_arguments`.
            let (arguments, defect) = parse_arguments(&acc.arguments);
            self.pending.push_back(ProviderEvent::ToolCall(ToolCall {
                id: acc.id.unwrap_or_default(),
                name,
                arguments,
                defect,
            }));
        }
    }
}

impl Stream for ResponsesStream {
    type Item = ProviderEvent;

    fn poll_next(
        self: Pin<&mut ResponsesStream>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ProviderEvent>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.pending.pop_front() {
                return Poll::Ready(Some(event));
            }
            if this.exhausted {
                return Poll::Ready(None);
            }
            match this.raw.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(chunk))) => this.push(&chunk),
                Poll::Ready(Some(Err(e))) => {
                    // A mid-stream transport failure is retryable (the
                    // `AgentLoop`'s `RetryPolicy` re-issues `complete()`).
                    //
                    // THE FLUSH-SKIP, reported at the latch — and on this wire
                    // this is the ONLY site of its class. The four TERMINAL arms
                    // (`response.completed` / `incomplete` / `failed` and the
                    // in-stream `error`) each call `flush_tool_calls()` in-arm,
                    // BEFORE setting `terminated`, precisely because the stream-end
                    // flush is unreachable once they have; a line at one of them
                    // would be a provable no-op AND would tell a supporter a call
                    // was lost when the call was delivered. A transport failure is
                    // different: `exhausted` here makes `poll_next`'s early
                    // `if this.exhausted` return `Ready(None)`, the `Ready(None)`
                    // block is never reached, `terminated` is still false, and the
                    // flush with it. The gate is a non-empty `tool_acc` — the only
                    // condition under which a real call is being lost, and what
                    // keeps a healthy turn silent.
                    if !this.tool_acc.is_empty() {
                        emit_harness_line(&tool_calls_dropped_at_stream_error(
                            "responses",
                            "transport-error",
                            this.tool_acc.len(),
                            this.tool_acc.values().any(|acc| acc.name.is_some()),
                        ));
                    }
                    this.exhausted = true;
                    this.pending
                        .push_back(ProviderEvent::Error(ProviderError::Retryable(format!(
                            "stream error: {e}"
                        ))));
                }
                Poll::Ready(None) => {
                    this.exhausted = true;
                    // The stream ended: process the last (un-`\n`ed) line,
                    // flush any not-`done` tool calls, and synthesize a
                    // `Done` when no terminal event was seen. (A tail
                    // that ends mid-codepoint is truncated on the wire —
                    // the lossy conversion is the only option there.)
                    //
                    // NO `Usage` is synthesized here (unlike Anthropic —
                    // a Responses `usage` arrives only on the terminal
                    // events; a missing terminal event means missing
                    // usage, and a zero-filled `Usage` would misreport).
                    //
                    // With the terminal arms flushing in-arm, this
                    // block's flush is provably a NO-OP on every
                    // terminated path, for two INDEPENDENT reasons:
                    //   1. `flush_tool_calls` does
                    //      `std::mem::take(&mut self.tool_acc)`, so the
                    //      in-arm flush EMPTIES the map — there is nothing
                    //      left to emit here; and
                    //   2. the whole block sits inside `if
                    //      !this.terminated` and all four terminal arms
                    //      set `terminated = true` — including the `error`
                    //      arm, whose write PRECEDES its `parse_error_chunk`
                    //      match, so the `ProviderEvent::Error` branch
                    //      (which pushes an `Error`, not a `Done`) is
                    //      covered too.
                    // The block still does real work on the stream-ended-
                    // with-no-terminal-event path, which is what it is for.
                    if let Some(tail) = this.lines.finish() {
                        this.handle_line(&tail);
                    }
                    if !this.terminated {
                        // A `done` acc (its `output_item.done` emitted a
                        // complete, actionable `ToolCall`) SUPPRESSES the
                        // synthetic `Done(Error)`: the `AgentLoop` sees a
                        // stream end without a `Done` and continues the
                        // turn with the tool call instead of retrying
                        // the model call (a `Done(Error)` would be
                        // retryable — the partial text would be dropped
                        // and the turn re-issued from scratch). A not-
                        // `done` acc (or none at all) means the response
                        // was cut off — the synthetic `Done(Error)`
                        // applies.
                        let completed = this.tool_acc.values().any(|acc| acc.done);
                        this.flush_tool_calls();
                        if !completed {
                            this.pending
                                .push_back(ProviderEvent::Done(FinishReason::Error));
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The OpenAI Responses WIRE shape of a content block: `Text` →
/// `{ "type": "input_text", "text" }`; `Image` →
/// `{ "type": "input_image", "image_url": "data:<mime>;base64,<data>"
/// }` (the OpenAI data-URI composition — the `ContentBlock::Image`
/// `data` field is base64 WITHOUT a `data:` prefix, so the URI is
/// composed here; the part `type` is `input_image`, NOT `image_url` —
/// the OpenAI chat-completions part type from
/// [`MessageContent::to_wire`]).
fn responses_block_wire(b: &ContentBlock) -> Value {
    match b {
        ContentBlock::Text { text } => json!({ "type": "input_text", "text": text }),
        ContentBlock::Image { image } => json!({
            "type": "input_image",
            "image_url": format!("data:{};base64,{}", image.mime_type, image.data),
        }),
    }
}

/// The `content` of a Responses wire item: `Text` → a plain string;
/// `Blocks` → an array of Responses parts ([`responses_block_wire`]).
fn responses_content_wire(c: &MessageContent) -> Value {
    match c {
        MessageContent::Text(t) => Value::String(t.clone()),
        MessageContent::Blocks(blocks) => {
            Value::Array(blocks.iter().map(responses_block_wire).collect())
        }
    }
}

/// The Responses `input` array: the non-system messages as items (NO
/// same-role merging — the Responses API accepts consecutive same-role
/// items): `user` / `assistant` → `{ role, content }` (the content in
/// the Responses wire form); a `tool` message → a `function_call_output`
/// item (`{ type, call_id, output }` — NO `role` field, the `item` type
/// IS the role); and an `assistant` message with `tool_calls` → ONE
/// `function_call` item PER CALL (in call order — the API validates that
/// each `function_call_output`'s `call_id` references a `function_call`
/// item in `input`, so the `function_call` items are REQUIRED: dropping
/// them makes the second turn of every tool-using session 400), preceded
/// by a `message` item for the text content (an EMPTY `Text` →
/// `function_call` items ONLY, no `message` item).
fn responses_input(req: &ModelRequest) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    for m in req.messages.iter().filter(|m| m.role != ChatRole::System) {
        if m.role == ChatRole::Tool {
            out.push(json!({
                "type": "function_call_output",
                "call_id": m.tool_call_id.clone().unwrap_or_default(),
                "output": responses_content_wire(&m.content),
            }));
            continue;
        }
        let role = if m.role == ChatRole::Assistant {
            "assistant"
        } else {
            "user"
        };
        if let Some(calls) = &m.tool_calls {
            // The assistant's TEXT content (if any) is a SEPARATE
            // `message` item that PRECEDES the `function_call` items: an
            // EMPTY `Text` / `Blocks` → `function_call` items ONLY.
            let message_item = match &m.content {
                MessageContent::Text(t) if !t.is_empty() => Some(json!({
                    "role": role,
                    "content": Value::String(t.clone()),
                })),
                MessageContent::Text(_) => None,
                MessageContent::Blocks(blocks) if !blocks.is_empty() => Some(json!({
                    "role": role,
                    "content": Value::Array(blocks.iter().map(responses_block_wire).collect()),
                })),
                MessageContent::Blocks(_) => None,
            };
            if let Some(item) = message_item {
                out.push(item);
            }
            // ONE `function_call` item PER CALL, in call order. The
            // `arguments` is a JSON **string** (the same wire form as
            // the OpenAI `function` object's `arguments` — re-serialized
            // from the parsed `Value`; an empty-arguments call →
            // `"{}"`).
            for tc in calls {
                let args =
                    serde_json::to_string(&tc.arguments).expect("JSON serialization cannot fail");
                out.push(json!({
                    "type": "function_call",
                    "call_id": tc.id,
                    "name": tc.name,
                    "arguments": args,
                }));
            }
            continue;
        }
        out.push(json!({ "role": role, "content": responses_content_wire(&m.content) }));
    }
    out
}

/// A client for the OpenAI Responses API (`POST {base_url}/responses`,
/// the `event:`-tagged SSE response parsed by [`ResponsesStream`]; ADR
/// 0024). The auth header is `Authorization: Bearer` (the
/// `OpenAiCompatibleProvider` pattern — the Responses API is OpenAI's,
/// unlike Anthropic's `x-api-key`).
#[derive(Debug, Clone)]
pub struct OpenAiResponsesProvider {
    /// The API base (e.g. `https://api.openai.com/v1` — the client
    /// appends `/responses`).
    pub base_url: String,
    /// The `Authorization: Bearer` key.
    pub api_key: String,
    /// The provider's expanded + validated CUSTOM headers (R1, issue #129) —
    /// applied by `build_client` as `default_headers` on every completion
    /// request. `None` = no custom headers (the byte-identical default).
    pub custom_headers: Option<std::collections::BTreeMap<String, String>>,
}

/// Build the OpenAI Responses request body from a [`ModelRequest`]
/// (`max_output_tokens` is included ONLY when `Some` — unlike Anthropic,
/// NOT required; `instructions` is present only when non-empty; the
/// `reasoning` `effort` is passed through VERBATIM — the desktop's level
/// vocabulary for an OpenAI model comes from the provider's own
/// discovery, so it is already in the provider's vocabulary). The
/// message `content` uses the Responses WIRE form (a plain string for
/// `Text`, an array of Responses content parts for `Blocks` —
/// [`responses_input`]).
fn responses_request_body(req: &ModelRequest) -> Value {
    // `instructions`: the `System` messages' text, folded — the same
    // collection as Anthropic's `system` ([`ModelRequest::system_text`]).
    let instructions = req.system_text();
    let mut body = json!({
        "model": req.model,
        "input": Value::Array(responses_input(req)),
        "stream": req.options.stream,
    });
    if !instructions.is_empty() {
        body["instructions"] = json!(instructions);
    }
    if !req.tools.is_empty() {
        // Flat `{ type: "function", name, description, parameters }` —
        // the `function` nesting does NOT apply (the `name` /
        // `description` / `parameters` are at the TOP level plus the
        // `type` tag).
        body["tools"] = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
                })
            })
            .collect::<Vec<_>>()
            .into();
    }
    if let Some(t) = req.options.temperature {
        body["temperature"] = json!(t);
    }
    // `max_output_tokens` (the Responses name) — included ONLY when
    // `Some` (unlike Anthropic's `max_tokens`, NOT required).
    if let Some(m) = req.options.max_tokens {
        body["max_output_tokens"] = json!(m);
    }
    if let Some(level) = &req.options.reasoning_effort {
        if !level.is_empty() {
            body["reasoning"] = json!({ "effort": level });
        }
    }
    body
}

#[async_trait]
impl Provider for OpenAiResponsesProvider {
    async fn complete(
        &self,
        req: &ModelRequest,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let url = format!("{}/responses", self.base_url.trim_end_matches('/'));
        // The shared idle-based bounds (the `OpenAiCompatibleProvider`
        // values — `build_client`'s docs: a generous CONNECT bound + a
        // `read_timeout` (the time BETWEEN BYTES on the body)).
        let client = build_client(
            Duration::from_secs(30),      // a generous CONNECT bound
            Duration::from_secs(5 * 60),  // 5 min of SILENCE = a stalled provider
            self.custom_headers.as_ref(), // R1 (issue #129): the ONE completion seam
        );
        let mut builder = client.post(&url).bearer_auth(&self.api_key);
        for (name, value) in litellm_headers(req.session_id.as_deref()) {
            builder = builder.header(name, value);
        }
        let resp = builder
            .json(&responses_request_body(req))
            .send()
            .await
            .map_err(|e| ProviderError::Retryable(format!("request failed: {e}")))?;

        // Map the HTTP status to a `ProviderError` BEFORE the SSE body is
        // consumed (`transport::map_response`: the error payload is read ONLY
        // for a 4xx/5xx — 401/403 → `Auth`, 429/5xx → `Retryable`, other
        // 4xx → `Fatal`; any other non-success (1xx/3xx) → the terminal
        // `Fatal` with NO body suffix).
        let resp = map_response(resp).await?;

        // `bytes_stream` yields `bytes::Bytes`; map to `Vec<u8>` so the
        // stream type does not name the `bytes` crate.
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            resp.bytes_stream().map(|r| r.map(|b| b.to_vec())).boxed();
        Ok(Box::pin(ResponsesStream::new(raw)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{chat_message, collect, keys_of_names};
    use super::super::types::{ModelOptions, ToolSpec};
    use super::*;
    use crate::tools::ImageRef;

    // ── the OpenAI Responses wire (`event:`-tagged SSE, ADR 0024) ──────

    /// Build a `ResponsesStream` over RAW byte chunks (the `&str`
    /// helper below cannot split a multi-byte codepoint mid-byte —
    /// `&str` is valid UTF-8; this one can).
    fn responses_stream_bytes(chunks: &[Vec<u8>]) -> ResponsesStream {
        let data: Vec<Vec<u8>> = chunks.to_vec();
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            futures_util::stream::iter(data.into_iter().map(Ok)).boxed();
        ResponsesStream::new(raw)
    }

    /// Build a `ResponsesStream` over canned raw chunks (no HTTP).
    fn responses_stream(chunks: &[&str]) -> ResponsesStream {
        // Owned data so the stream is `'static` (independent of `chunks`).
        let data: Vec<Vec<u8>> = chunks.iter().map(|c| c.as_bytes().to_vec()).collect();
        responses_stream_bytes(&data)
    }

    /// Exactly-once `Usage` on the Responses wire (#108): `terminated`
    /// does NOT protect the usage push — it is consulted only at stream
    /// end to decide whether to synthesise a `Done`, and it is set AFTER
    /// the push. A doubled `response.completed` (a proxy re-sending the
    /// final event) therefore pushes TWO `Usage` frames today, and the
    /// roll-up would read that as money that was not spent.
    ///
    /// The expected vector is `[Usage, Done(Stop), Done(Stop)]` — ONE
    /// `Usage` (the thing #108 guards) but TWO `Done(Stop)` frames,
    /// because the guard covers the `Usage` push ONLY. A doubled
    /// terminal still yields a doubled `Done`: that is a pre-existing,
    /// separate stop-reason concern, deliberately OUT of #108's scope,
    /// and the vector documents that residue rather than hiding it.
    #[tokio::test]
    async fn sse_single_usage_chunk() {
        let events = collect(responses_stream(&[
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                    },
                    None
                ),
                // The SECOND terminal's `Done` still arrives — the guard
                // is about the cost frame, not the stop reason.
                ProviderEvent::Done(FinishReason::Stop),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a doubled terminal emits ONE Usage (the doubled `Done` is the recorded residue) (got {events:?})"
        );
    }

    #[tokio::test]
    async fn responses_sse_text_and_completed() {
        // The full happy path: one `event:` line preceding each `data:`
        // line. A `message` `output_item.done` carries no content of its
        // own (the text streamed via the `*_delta` events) → nothing.
        let events = collect(responses_stream(&[
            "event: response.created\n\
             data: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\n\
             event: response.output_text.delta\n\
             data: {\"type\":\"response.output_text.delta\",\"delta\":\"He\"}\n\n\
             event: response.output_text.delta\n\
             data: {\"type\":\"response.output_text.delta\",\"delta\":\"llo\"}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"id\":\"msg_1\",\"status\":\"completed\",\"content\":[]}}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n",
        ]))
        .await;
        // The `Usage` PRECEDES the `Done` (the `response.completed`
        // handler emits it first).
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("He".to_string()),
                ProviderEvent::TextDelta("llo".to_string()),
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// (ADR 0046) The ANTI-DOUBLE-COUNT pin for the Responses wire. Its
    /// `usage.input_tokens` is ALREADY the whole prompt — cached tokens
    /// included — and `input_tokens_details.cached_tokens` is a SUBSET of
    /// it, so the count passes through UNCHANGED and the detail is NOT
    /// read at all (summing it "for consistency" with the Anthropic wire
    /// would double the cached prefix and make compaction arrive EARLY).
    /// An INPUT-ONLY usage is still a report here (`response_usage`
    /// accepts one side), so the `0` output is the documented rule, not a
    /// dropped count.
    #[tokio::test]
    async fn responses_usage_does_not_add_the_cached_tokens_detail() {
        let events = collect(responses_stream(&[
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1000,\"input_tokens_details\":{\"cached_tokens\":800}}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 1000,
                        output_tokens: 0,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    #[tokio::test]
    async fn responses_sse_reasoning_summary() {
        // The reasoning streams as `reasoning_summary_text.delta` fragments
        // (the `ThinkingDelta` events), then the text, then the terminal.
        let events = collect(responses_stream(&[
            "event: response.reasoning_summary_text.delta\n\
             data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"think\"}\n\n\
             event: response.reasoning_summary_text.delta\n\
             data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"ing\"}\n\n\
             event: response.output_text.delta\n\
             data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":3}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ThinkingDelta("think".to_string()),
                ProviderEvent::ThinkingDelta("ing".to_string()),
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 3,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// The same, with a TRANSPORT failure as the LAST item of the raw stream —
    /// the shape `poll_next`'s `Poll::Ready(Some(Err(e)))` arm sees (a
    /// `read_timeout` on a stalled provider, a reset connection). The helper
    /// above cannot express it: it maps every chunk to `Ok`.
    fn responses_stream_failing(chunks: &[&str]) -> ResponsesStream {
        let mut data: Vec<Result<Vec<u8>, reqwest::Error>> =
            chunks.iter().map(|c| Ok(c.as_bytes().to_vec())).collect();
        data.push(Err(transport_error()));
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            futures_util::stream::iter(data).boxed();
        ResponsesStream::new(raw)
    }

    /// A real `reqwest::Error`, built the only way a test can without a socket:
    /// `http://` has an empty host, so `RequestBuilder::build` answers `Err` with
    /// a `Builder`-kind error. All the arm under test does with the value is
    /// format `{e}` into its own `Retryable`, so the KIND is irrelevant — what the
    /// fixture needs is an `Err` item AFTER a chunk was consumed.
    fn transport_error() -> reqwest::Error {
        reqwest::Client::new()
            .get("http://")
            .build()
            .expect_err("`http://` has no host, so the request cannot be built")
    }

    // ── (supervisor-log diagnostics) the tool call the parser discarded ─────
    //
    // Three DISCARD paths on this wire were silent: the nameless acc the flush
    // drops, the nameless `output_item.done` the PARSE site skips, and the call
    // in flight when the TRANSPORT error makes the stream-end flush unreachable.
    // (The four TERMINAL arms are not among them — each already calls
    // `flush_tool_calls` in-arm before setting `terminated`, so nothing is lost
    // there and a line at one would report a loss that did not happen.)
    //
    // Asserted on the `diagnostics::record` half. The `eprintln!` half is not
    // assertable from a lib unit test — `provider/mod.rs`'s
    // `the_chosen_wire_is_logged_at_construction` limit, restated: its only
    // production reader is the Worker-stderr drain task, which needs a spawned
    // Worker. It is covered here by carrying the SAME payload.

    /// The stems of EVERY line this work adds, on ANY of the three wires. The
    /// healthy-turn test asserts none appears, so its empty assertion guards
    /// against instrumenting a site that fires on a GOOD turn — and it
    /// specifically covers `if acc.done { continue; }`, the deliberate
    /// de-duplication this change must NOT instrument (it fires on every healthy
    /// tool call on this wire).
    const DISCARD_STEMS: [&str; 2] = [
        "tool call dropped with no name",
        "tool calls dropped at a stream error",
    ];

    /// The recorded lines carrying one of those stems — the assertion surface.
    /// Filtered by stem rather than counted whole, because `diagnostics` is
    /// process-global: a sibling test that reaches `record` without the env lock
    /// can append its own line here (the `provider wire=` construction line,
    /// e.g.), and it must neither satisfy nor spoil these.
    fn discard_lines(lines: &[String]) -> Vec<String> {
        lines
            .iter()
            .filter(|l| DISCARD_STEMS.iter().any(|stem| l.contains(stem)))
            .cloned()
            .collect()
    }

    /// How many of `lines` END with `want` — `record` prefixes `<epoch-millis> `,
    /// so the match is a suffix match (as `settle_diagnostics.rs` and
    /// `provider/mod.rs` both do it).
    fn ending_with(lines: &[String], want: &str) -> usize {
        lines.iter().filter(|l| l.ends_with(want)).count()
    }

    /// Arms the process-global diagnostics channel at a fresh scratch dir for ONE
    /// test and disables it again on the way out — the
    /// `loop/tests/settle_diagnostics.rs` pair, folded into ONE guard. The fold is
    /// the point: as two locals the dir can drop BEFORE the global is cleared, and
    /// the next `record` anywhere then `create_dir_all`s the removed dir back into
    /// existence and writes into it for the rest of the run.
    struct ArmedDiagnostics {
        dir: crate::test_support::ScratchDir,
    }
    impl Drop for ArmedDiagnostics {
        fn drop(&mut self) {
            crate::diagnostics::init(None);
        }
    }

    /// Arm the channel. Every caller holds `test_support::env_lock()` across the
    /// call AND its assertions — the contract `diagnostics` states for its
    /// process-global target. The lock is taken ONCE per test: `env_lock` is
    /// non-reentrant and a second call inside one test self-deadlocks.
    fn arm_diagnostics(tag: &str) -> ArmedDiagnostics {
        let dir = crate::test_support::ScratchDir::in_env_temp("responses-discard-diag-", tag);
        crate::diagnostics::init(Some(dir.to_path_buf()));
        ArmedDiagnostics { dir }
    }

    /// The armed file's lines. A missing file means nothing was recorded, which is
    /// a legitimate answer for the negative tests (`record` creates the file on its
    /// first write).
    fn recorded_lines(dir: &std::path::Path) -> Vec<String> {
        let path = dir.join(crate::diagnostics::FILE_NAME);
        match std::fs::read_to_string(&path) {
            Ok(text) => text.lines().map(str::to_string).collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => panic!("the scratch diagnostics file is readable: {e}"),
        }
    }

    /// Truncate the armed file so each assertion is proven by ITS OWN line (the
    /// `provider/mod.rs::reset_diagnostics_file` precedent).
    fn reset_diagnostics_file(dir: &std::path::Path) {
        let path = dir.join(crate::diagnostics::FILE_NAME);
        match std::fs::write(&path, "") {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("the scratch diagnostics file can be truncated: {e}"),
        }
    }

    /// One `response.output_item.added` event for a `function_call` item. Built
    /// with `json!` rather than the hand-escaped literals the neighbouring fixtures
    /// use, for ONE reason: the expected lines name `args_bytes`, and an escaped
    /// literal makes that number a claim about my escaping rather than the data.
    fn output_item_added(index: u32, id: Option<&str>, name: Option<&str>) -> String {
        let mut item = serde_json::Map::new();
        item.insert("type".to_string(), json!("function_call"));
        if let Some(id) = id {
            item.insert("id".to_string(), json!(id));
        }
        if let Some(name) = name {
            item.insert("name".to_string(), json!(name));
        }
        item.insert("arguments".to_string(), json!(""));
        item.insert("status".to_string(), json!("in_progress"));
        format!(
            "event: response.output_item.added\n\
             data: {}\n\n\
             ",
            json!({ "type": "response.output_item.added", "output_index": index,
                    "item": Value::Object(item) })
        )
    }

    /// One `response.output_item.done` event (the authoritative complete item).
    fn output_item_done(index: u32, id: Option<&str>, name: Option<&str>, args: &str) -> String {
        let mut item = serde_json::Map::new();
        item.insert("type".to_string(), json!("function_call"));
        if let Some(id) = id {
            item.insert("id".to_string(), json!(id));
        }
        if let Some(name) = name {
            item.insert("name".to_string(), json!(name));
        }
        item.insert("arguments".to_string(), json!(args));
        item.insert("status".to_string(), json!("completed"));
        format!(
            "event: response.output_item.done\n\
             data: {}\n\n\
             ",
            json!({ "type": "response.output_item.done", "output_index": index,
                    "item": Value::Object(item) })
        )
    }

    /// One `response.function_call_arguments.delta` event (an argument fragment).
    fn arguments_delta(index: u32, fragment: &str) -> String {
        format!(
            "event: response.function_call_arguments.delta\n\
             data: {}\n\n\
             ",
            json!({ "type": "response.function_call_arguments.delta",
                    "output_index": index, "delta": fragment })
        )
    }

    /// The argument text the fixtures stream. Its BYTE length is what the expected
    /// lines name.
    const ARG_TEXT: &str = "{\"path\":\"/x\"}";

    /// The EXACT payload for a nameless acc dropped at the FLUSH — spelled out in
    /// full, deliberately NOT re-derived from the message helper. A test that
    /// called the helper to build its own expectation would pass whatever the
    /// helper chose to say; a literal reddens when the sentence changes.
    const WANT_FLUSH_NAMELESS: &str = "tool call dropped with no name: parser=responses \
         stage=flush index=2 id=\"fc_7\" args_bytes=13 — a call with no name cannot \
         be dispatched, so the model's call is lost and the turn settles without it";

    /// The EXACT payload for the nameless `output_item.done` the PARSE site
    /// skips. A different fact from the one above (the provider sent a COMPLETED
    /// item with no name in it) and a different `stage`, so a reader can tell
    /// which of the two the wire did.
    const WANT_DONE_NAMELESS: &str = "tool call dropped with no name: \
         parser=responses stage=output-item-done index=2 id=\"fc_7\" args_bytes=13 \
         — a call with no name cannot be dispatched, so the model's call is lost \
         and the turn settles without it";

    /// The EXACT payload for the call in flight at the TRANSPORT error. The four
    /// terminal arms already flush in-arm, so this is the only latch on this wire.
    const WANT_LATCH_TRANSPORT: &str = "tool calls dropped at a stream error: \
         parser=responses stage=transport-error in_flight=1 named_any=true — the \
         stream-end flush is unreachable once the stream is exhausted, so a call \
         the model had already streamed was lost";

    /// A nameless acc dropped at the flush: `output_item.added` with no `name`,
    /// the argument fragments, then the `response.completed` terminal — whose
    /// in-arm flush is where the acc dies. NO `output_item.done` in this fixture,
    /// so the line below is proven to come from the FLUSH and not from the parse
    /// site.
    ///
    /// The "no `ToolCall`" half is a PIN of pre-existing behaviour (a nameless
    /// call could never be dispatched) and its witness is the two deltas.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_nameless_function_call_is_dropped_at_the_flush_and_recorded() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("flush-nameless");
        reset_diagnostics_file(armed.dir.path());

        assert_eq!(ARG_TEXT.len(), 13, "the number the expected line quotes");
        let events = collect(responses_stream(&[
            &output_item_added(2, Some("fc_7"), None),
            &arguments_delta(2, "{\"pa"),
            &arguments_delta(2, "th\":\"/x\"}"),
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"\
             completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n",
        ]))
        .await;

        let deltas = events
            .iter()
            .filter(|e| matches!(e, ProviderEvent::ToolCallDelta(_)))
            .count();
        assert_eq!(
            deltas, 3,
            "the `added` delta and both argument fragments reached the acc (the \
             drop is downstream of them), got {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "a call with no name cannot be dispatched, so it is dropped (and \
             always was), got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&lines, WANT_FLUSH_NAMELESS),
            1,
            "the flush records the discard EXACTLY ONCE, with no `harness: ` \
             prefix (the `log_wire` convention) and naming the parser, the stage, \
             the index, the id and the argument-buffer length; want a line ending \
             with {WANT_FLUSH_NAMELESS:?}, got:\n{lines:#?}"
        );
        assert_eq!(
            ending_with(&lines, WANT_DONE_NAMELESS),
            0,
            "no `output_item.done` arrived, so the PARSE-site line must not fire \
             here — the two sites name different facts; got:\n{lines:#?}"
        );
    }

    /// THE site the flush-time line alone cannot cover: a
    /// `response.output_item.done` whose `item` carries no `name` (and whose acc
    /// has none either, so the `or_else` fallback finds nothing). The arm's own
    /// comment already says such a call "cannot be dispatched — skip the item
    /// entirely"; the `if let Some(name) = name` had no `else`, so it never said
    /// so out loud.
    ///
    /// The parse-time line fires HERE, and the flush-time line also fires later
    /// (the acc is left not-`done` on purpose, so the stream-end flush still sees
    /// it — that is what keeps the synthetic `Done(Error)`). TWO lines, for two
    /// distinct facts, distinguishable by `stage=`; both are asserted so neither
    /// can silently disappear.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_nameless_output_item_done_is_recorded_at_parse_time() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("done-nameless");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(responses_stream(&[
            &output_item_added(2, Some("fc_7"), None),
            // The done item is AUTHORITATIVE for `arguments`: it supplies the full
            // 13-byte text the acc then holds, so `args_bytes` is 13 whatever the
            // `added` item seeded.
            &output_item_done(2, Some("fc_7"), None, ARG_TEXT),
        ]))
        .await;

        // The witness: the item was PARSED (its `arguments` overwrote the acc —
        // proven by the flush line's `args_bytes=13` with no `arguments.delta` in
        // the fixture at all) and it dispatched nothing.
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "a done item with no name dispatches nothing, got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&lines, WANT_DONE_NAMELESS),
            1,
            "the PARSE site records the skip EXACTLY ONCE — the flush-time line \
             alone would not say the provider sent a COMPLETED item with no name \
             in it; want a line ending with {WANT_DONE_NAMELESS:?}, got:\n\
             {lines:#?}"
        );
        assert_eq!(
            ending_with(&lines, WANT_FLUSH_NAMELESS),
            1,
            "the acc is left not-`done` on purpose, so the stream-end flush sees \
             it too and records its own line — asserted so a future change that \
             quiets one site cannot silently drop both; got:\n{lines:#?}"
        );
    }

    /// The transport-error arm — the ONLY latch on this wire. The four terminal
    /// arms (`response.completed` / `incomplete` / `failed` / the in-stream
    /// `error`) each call `flush_tool_calls()` in-arm before setting
    /// `terminated`, so nothing is lost at one and a line there would report a
    /// loss that did not happen. A TRANSPORT failure is different: `poll_next`'s
    /// `Err` arm sets `exhausted`, the early `if this.exhausted` returns
    /// `Ready(None)`, and the stream-end flush is never reached.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_transport_failure_with_a_call_in_flight_records_the_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("transport-error");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(responses_stream_failing(&[
            &output_item_added(2, Some("fc_7"), Some("write")),
            &arguments_delta(2, ARG_TEXT),
        ]))
        .await;

        assert!(
            matches!(
                &events[events.len() - 1],
                ProviderEvent::Error(ProviderError::Retryable(m)) if m.contains("stream error")
            ),
            "the fixture ends on the transport error, got {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "the transport arm does not flush — the call is lost, got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&lines, WANT_LATCH_TRANSPORT),
            1,
            "the transport latch reports the call it is about to lose, naming the \
             count; want a line ending with {WANT_LATCH_TRANSPORT:?}, got:\n\
             {lines:#?}"
        );
    }

    /// THE GUARD ITSELF, on the latch: a transport failure with NO call in flight
    /// records nothing. This is the assertion that makes
    /// `!this.tool_acc.is_empty()` load-bearing rather than decorative — a
    /// transport failure is an ORDINARY failure the loop retries, and every one of
    /// them would otherwise write a "tool calls dropped" sentence into the
    /// always-on file for a turn that had no call to lose.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_transport_failure_with_nothing_in_flight_records_no_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("transport-empty-acc");
        reset_diagnostics_file(armed.dir.path());

        // Text only, then the transport failure — no `function_call` item, so the
        // acc map is NEVER touched and `tool_acc.len()` is 0 at the latch.
        let events = collect(responses_stream_failing(&[
            "event: response.output_text.delta\n\
             data: {\"type\":\"response.output_text.delta\",\"delta\":\"thinking\"}\n\n",
        ]))
        .await;

        // The witness: the failure really did land (the latch WAS reached) and no
        // tool-call fragment ever arrived — so the empty assertion below measures
        // the gate and not a path that never ran.
        assert!(
            matches!(
                &events[events.len() - 1],
                ProviderEvent::Error(ProviderError::Retryable(_))
            ),
            "the fixture reached the transport latch, got {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCallDelta(_))),
            "no tool-call fragment arrived, so `tool_acc` is empty at the latch, \
             got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert!(
            lines.is_empty(),
            "a transport failure with NO call in flight lost nothing, so the gate \
             must hold it shut; got:\n{lines:#?}"
        );
    }

    /// The guard, on the wire that needs it most: a healthy tool call on this
    /// parser walks FOUR sites that must stay silent — the `done` de-duplication
    /// in `flush_tool_calls` (`if acc.done { continue; }`, which fires on every
    /// healthy call here), the `output_item.done` arm's `name`-present branch,
    /// and both placeholder-free text arms. The assertion is over BOTH stems, so
    /// instrumenting any of them reddens this.
    ///
    /// WELL-FORMED arguments: `parse_arguments`'s `harness: malformed tool-call
    /// arguments` line predates this task and would redden the assertion for an
    /// unrelated reason.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_healthy_tool_call_turn_records_no_drop_line() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("healthy");
        reset_diagnostics_file(armed.dir.path());

        // The FULL healthy lifecycle, terminal included: `added` → fragments →
        // the authoritative `done` → `response.completed`. The `done` marks the
        // acc `done`, so the terminal's in-arm flush walks the
        // `if acc.done { continue; }` de-duplication — the site this change must
        // NOT instrument.
        let events = collect(responses_stream(&[
            &output_item_added(2, Some("fc_7"), Some("read")),
            &arguments_delta(2, "{\"pa"),
            &arguments_delta(2, "th\":\"/x\"}"),
            &output_item_done(2, Some("fc_7"), Some("read"), ARG_TEXT),
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"\
             completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}}\n\n",
        ]))
        .await;

        // The witness: this IS a healthy turn — exactly one complete call,
        // dispatched, no defect, and the terminal `Done` after it.
        let calls: Vec<&ToolCall> = events
            .iter()
            .filter_map(|e| match e {
                ProviderEvent::ToolCall(t) => Some(t),
                _ => None,
            })
            .collect();
        assert_eq!(
            calls.len(),
            1,
            "exactly ONE call dispatched — the de-duplication held, so the \
             de-dup branch was walked (which is what makes the empty assertion \
             below measure a guard), got {events:?}"
        );
        assert_eq!(
            calls[0].arguments,
            json!({ "path": "/x" }),
            "the `done` item's authoritative arguments parsed cleanly"
        );
        assert!(
            calls[0].defect.is_none(),
            "well-formed arguments carry no defect — otherwise the pre-existing \
             malformed-arguments line would fire here too"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert!(
            lines.is_empty(),
            "a turn that streamed and dispatched its call lost nothing, so no \
             discard line may be written; got:\n{lines:#?}"
        );

        // The POSITIVE CONTROL, so the assertion above cannot be vacuous by way
        // of a channel that records nothing at all: the same armed channel takes
        // the same sentence when the guard IS satisfied. If this half stops
        // writing, the empty assertion above has stopped meaning anything.
        crate::diagnostics::record(WANT_FLUSH_NAMELESS);
        let after = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&after, WANT_FLUSH_NAMELESS),
            1,
            "the armed channel DOES take this line — so the empty assertion above \
             measured the guard, not a dead sink; got:\n{after:#?}"
        );
    }

    /// The terminal arms are NOT a loss site and must stay silent — the four of
    /// them already call `flush_tool_calls()` in-arm, which is exactly what
    /// `responses_sse_error_event_flushes_a_partial_function_call` and its
    /// sibling pin. Adding a latch line at one of them would be a provable no-op
    /// AND would tell a supporter a call was lost when the call was delivered.
    ///
    /// The in-stream `error` arm is the strongest of the four (it can push an
    /// `Error` instead of a `Done`, and its `terminated = true` is written before
    /// the `parse_error_chunk` match so it covers both branches).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_terminal_arm_that_flushes_records_no_drop_line() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("terminal-flush");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(responses_stream(&[
            &output_item_added(1, Some("fc_9"), Some("write")),
            &arguments_delta(1, ARG_TEXT),
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"code\":\"server_error\",\
             \"message\":\"boom\"}}\n\n",
        ]))
        .await;

        // The witness: the arm FLUSHED — the call is on the event stream, ahead
        // of the terminal `Done`. So the absence below is "nothing was lost", not
        // "the code was never reached".
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "the terminal arm flushed the in-flight call (the pre-existing fix), \
             got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert!(
            lines.is_empty(),
            "a terminal arm that FLUSHED lost nothing, so no latch line may be \
             written for it — that is why the responses latch exists only at the \
             transport arm; got:\n{lines:#?}"
        );
    }

    #[tokio::test]
    async fn responses_sse_function_call_full_lifecycle() {
        // `output_item.added` (the `id` + `name` + the empty `arguments`),
        // the `arguments` fragments, then the `output_item.done` with the
        // COMPLETE `arguments` (the done item is authoritative).
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":2,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":2,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"read\",\"arguments\":\"{\\\"path\\\":\\\"/x\\\"}\",\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        // The `output_item.done` emits the `ToolCall`; the stream ends
        // WITHOUT a terminal event, but the (already-`done`) acc is NOT
        // flushed again — no duplicate `ToolCall`.
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 2,
                    id: Some("fc_1".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 2,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 2,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
            ]
        );
    }

    #[tokio::test]
    async fn responses_sse_function_call_done_item_authoritative() {
        // NO `arguments.delta` events: the `output_item.done` carries the
        // COMPLETE `arguments` — the done item is authoritative (a proxy
        // that sent no `arguments.delta` events still yields a complete
        // call).
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"name\":\"read\",\"arguments\":\"{\\\"path\\\":\\\"/y\\\"}\",\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        // The done item's `arguments` WIN (over the empty `added` one).
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 2,
                    id: Some("fc_1".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/y" }),
                    defect: None,
                }),
            ]
        );
    }

    #[tokio::test]
    async fn responses_sse_incomplete_max_output_tokens() {
        // `incomplete_details.reason: "max_output_tokens"` → `Done(Length)`.
        let events = collect(responses_stream(&[
            "event: response.incomplete\n\
             data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":32000}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 32000,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Length),
            ]
        );
    }

    #[tokio::test]
    async fn responses_sse_failed() {
        // NO `usage` in the fixture → NO `Usage` event; just `Done(Error)`.
        let events = collect(responses_stream(&[
            "event: response.failed\n\
             data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}}\n\n",
        ]))
        .await;
        assert_eq!(events, vec![ProviderEvent::Done(FinishReason::Error)]);
    }

    /// A `function_call` IN FLIGHT at a `response.incomplete` is flushed
    /// before the `Done` (the truncation notice must not arrive without the
    /// tool card the stream already carried — the `openai` / `anthropic`
    /// wires flush in the terminal arm; this one did not).
    #[tokio::test]
    async fn responses_sse_incomplete_flushes_a_partial_function_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.incomplete\n\
             data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n",
        ]))
        .await;
        // The flush precedes the `Done` (the ordering invariant:
        // `Usage` → tool calls → `Done`).
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Length),
            ],
            "a truncated stream still delivers the tool call it accumulated \
             (got {events:?})"
        );
    }

    /// The flush is NOT tied to the truncation reason: a
    /// `content_filter` incomplete (→ `Done(Error)`, not `Done(Length)`)
    /// still yields the call in flight.
    #[tokio::test]
    async fn responses_sse_incomplete_non_token_reason_still_flushes() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.incomplete\n\
             data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"content_filter\"}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Error),
            ],
            "a non-token incomplete reason still flushes (got {events:?})"
        );
    }

    /// … and so does a FAILED terminal (the call the model had already
    /// committed to is not paid for and then swallowed).
    #[tokio::test]
    async fn responses_sse_failed_flushes_a_partial_function_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.failed\n\
             data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Error),
            ],
            "a failed terminal still flushes the call in flight (got {events:?})"
        );
    }

    /// The sharpest of the four: `response.completed` sets `terminated`
    /// UNCONDITIONALLY but pushed `Done(Stop)` only for
    /// `status == "completed"`, so an absent/other `status` emitted NO
    /// finish reason at all — `acc.finish` stayed `None`, the `Length`
    /// gate and the fabrication guard were bypassed, and the turn fell
    /// through into `note_success` and the success triple: a SILENT
    /// SUCCESS with a vanished call. (UNVERIFIED upstream: no captured
    /// payload shows a real backend doing this — this arm is the
    /// defensive/proxy shape.)
    ///
    /// The fixture carries NO `usage` key on purpose: `response_usage`
    /// returns `None` for an absent `response.usage`, so
    /// `emit_terminal_usage` pushes nothing and no `Usage` lands between
    /// the deltas and the `ToolCall`.
    #[tokio::test]
    async fn responses_sse_completed_without_status_flushes_and_errors() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                // A `terminated` stream always carries a finish reason —
                // a `completed` whose status is not `"completed"` is an
                // ERROR, never a silent success.
                ProviderEvent::Done(FinishReason::Error),
            ],
            "a `completed` without a `completed` status flushes AND reports \
             a finish reason (got {events:?})"
        );
    }

    /// Even a HEALTHY `completed` loses a call that never got its
    /// `output_item.done` (the terminal is the last chance to flush —
    /// the stream-end flush is unreachable once `terminated` is set).
    ///
    /// The fixture carries a `usage` on purpose: this is the wire's only
    /// pin on the FULL `Usage` → tool calls → `Done` order the commit
    /// exists to establish. Swapping the `flush_tool_calls()` and
    /// `emit_terminal_usage(v)` calls in the arm reddens it. (The
    /// Anthropic wire pins the same order in
    /// `anthropic_sse_tool_use_block`; this wire had no equivalent, so
    /// every other fixture here omits `usage` and `Usage` never enters
    /// their expected vectors.)
    #[tokio::test]
    async fn responses_sse_completed_with_status_flushes_a_never_done_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"read\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                // `Usage` BETWEEN the deltas and the `ToolCall`: within a
                // terminal event the order is `emit_terminal_usage` →
                // `flush_tool_calls` → `Done`, the same order the `openai`
                // and `anthropic` wires hold.
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                    },
                    None
                ),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a `completed` with no `output_item.done` still flushes the \
             call it accumulated, after its `Usage` and before the `Done` \
             (got {events:?})"
        );
    }

    /// The terminal event ENDS the response: an `output_item.done` that
    /// arrives AFTER it must not resurrect the accumulator the terminal
    /// already flushed and emptied. Without the `terminated` gate on the
    /// accumulation arms the late done item builds a FRESH acc (the map
    /// was `std::mem::take`n by the flush) and emits a SECOND `ToolCall`
    /// with the SAME `id`.
    ///
    /// Why that is the expensive one: `consume_stream` pushes both into
    /// `acc.tool_calls`, `run_tool_batch` has no `id` dedupe, and
    /// `tool_execution_start` is emitted twice — TWO executions of the
    /// same `write`. `normalize`'s `announced_tool_calls` set collapses
    /// the duplicate `ToolCallStart` FRAME, not the dispatch.
    ///
    /// (UNVERIFIED upstream: per the Responses spec `response.completed`
    /// is final and every `output_item.done` precedes it, so no real
    /// backend is known to send this order. This is defensive hardening
    /// against a duplicated side effect on a payment path, NOT a fix for
    /// an observed wire.)
    #[tokio::test]
    async fn responses_sse_done_item_after_completed_does_not_duplicate_the_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"write\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"write\",\"arguments\":\"{\\\"path\\\":\\\"/x\\\"}\",\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("write".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "write".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a done item after the terminal must not emit a second ToolCall \
             for the call the terminal already flushed (got {events:?})"
        );
    }

    /// The same gate, the shape the arm explicitly supports — "a proxy
    /// may omit them on the done item": `id` / `name` fall back to the
    /// accumulator's, and after the flush emptied the map there is no
    /// `id` to fall back to. Measured on the current tree this late item
    /// emits a SECOND `ToolCall` with `id: ""` — not merely a duplicate,
    /// a call whose `call_id` matches no `function_call` item, so the
    /// `function_call_output` the loop sends back for it references
    /// nothing (and on the `write`/`bash` tools it is still a second
    /// dispatch).
    ///
    /// (UNVERIFIED upstream, as above.)
    #[tokio::test]
    async fn responses_sse_done_item_without_id_after_completed_does_not_duplicate_the_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"write\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"name\":\"write\",\"arguments\":\"{\\\"path\\\":\\\"/x\\\"}\",\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("write".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "write".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a done item that omits `id` after the terminal must not emit a \
             second, `id`-less ToolCall (got {events:?})"
        );
    }

    /// The third shape, and the one that is ALREADY correct: the late
    /// done item carries NEITHER `id` NOR `name`. It can emit nothing
    /// here (a nameless call cannot be dispatched) and it can never be
    /// flushed later (`flush_tool_calls` drops a nameless acc), so the
    /// single `ToolCall` the caller sees is the one the terminal flushed —
    /// with the authoritative arguments, because on a well-formed stream
    /// the accumulated fragments ARE the complete `arguments`. GREEN
    /// before the gate as well as after: this test is a PRESERVATION
    /// guard, pinning that the gate keeps it that way and that the late
    /// item adds no event of any kind.
    ///
    /// The residue this test does NOT claim to fix, measured: when the
    /// `arguments.delta` fragments were themselves truncated AND the late
    /// done item is the only carrier of the complete `arguments`, the
    /// flushed call keeps the stale fragment and its `defect` (the "your
    /// JSON arrived truncated" refusal). The gate cannot repair that —
    /// resurrecting the flushed acc to adopt a post-terminal `arguments`
    /// is what re-creates the duplicate dispatch the two tests above
    /// exist to prevent. Logging that dropped late item is another card's
    /// job (see the untouched `continue`s in `flush_tool_calls`).
    #[tokio::test]
    async fn responses_sse_done_item_without_id_or_name_after_completed_adds_no_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"write\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\"}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"arguments\":\"{\\\"path\\\":\\\"/x\\\"}\",\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("write".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "write".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a nameless done item after the terminal adds no event at all \
             (got {events:?})"
        );
    }

    /// A doubled terminal must not downgrade a TRUNCATION to an `Error`.
    ///
    /// `flush_tool_calls` is idempotent, but the `Done`s were not:
    /// `acc.finish = Some(f)` is written unconditionally downstream, so
    /// last-terminal-won and `response.incomplete{max_output_tokens}`
    /// followed by a statusless `response.completed` yielded
    /// `[Done(Length), Done(Error)]` — `acc.finish` became `Error`, the
    /// fabrication guard fired, the disposition was `Retryable`, and
    /// `retry_abandoned_attempt` RE-ISSUED the request. ADR 0108 Decision
    /// 1 forbids replaying a truncated response, and the neighbouring
    /// code is emphatic about it. Nothing dispatches here (no tool call in
    /// flight), so no side effect duplicates — but a never-retried
    /// truncation became a retried attempt, which is the one thing that
    /// must not happen.
    ///
    /// The first terminal's `Done(Length)` now stands; the second
    /// terminal's `Done` is dropped. (A doubled `Done(Stop)` — the residue
    /// `sse_single_usage_chunk` documents — is a separate, benign
    /// stop-reason concern and deliberately NOT collapsed by this latch.)
    #[tokio::test]
    async fn responses_sse_incomplete_length_then_completed_keeps_the_length_disposition() {
        let events = collect(responses_stream(&[
            "event: response.incomplete\n\
             data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Done(FinishReason::Length)],
            "a later terminal must not overwrite a `Done(Length)` with \
             `Done(Error)` — a truncated response is never re-issued \
             (got {events:?})"
        );
    }

    /// A doubled terminal must not turn a FAILED response into a SUCCESS.
    ///
    /// The `Length` latch above is narrower than the defect it guards. The
    /// finish reason is still REWRITTEN downstream (`acc.finish = Some(f)`
    /// is unconditional in `consume_stream`), so with only `Length` sticky a
    /// `response.failed` followed by `response.completed{status:"completed"}`
    /// yielded `[Done(Error), Done(Stop)]` and `acc.finish` ended at `Stop` —
    /// **a failed response reporting success**. That is the exact fabrication
    /// the fabrication guard exists to refuse, reached by a doubled terminal
    /// instead of by a lying provider: the turn takes the success triple, the
    /// composer unlocks, and the user is told a response the provider called a
    /// failure completed. Nothing dispatches here either, so this is a
    /// disposition lie rather than a duplicated side effect — but it is the
    /// lie on the path that decides whether a turn is a success.
    ///
    /// The general rule the latch now implements: a non-`Stop` disposition is
    /// STICKY. `Length` and `Error` are what a terminal knew about WHY the
    /// turn ended; `Stop` is the absence of a problem, and the absence cannot
    /// be asserted by a later event over an earlier event that found one. A
    /// doubled `Done(Stop)` is still deliberately NOT collapsed (see
    /// [`Self::push_terminal_done`] and the residue `sse_single_usage_chunk`
    /// records on purpose), and `Error → Error` still passes through.
    #[tokio::test]
    async fn responses_sse_failed_then_completed_keeps_the_error_disposition() {
        let events = collect(responses_stream(&[
            "event: response.failed\n\
             data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}}\n\n\
             event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Done(FinishReason::Error)],
            "a later `completed` must not overwrite a `Done(Error)` with \
             `Done(Stop)` — a failed response cannot be reported as a \
             success (got {events:?})"
        );
    }

    /// The other two post-terminal gates: an `output_item.added` and a
    /// `function_call_arguments.delta` that arrive AFTER `response.completed`
    /// are dropped, and the event vector carries nothing for them.
    ///
    /// The three accumulation arms are gated together, but only the
    /// `output_item.done` arm has a test of its own (the three
    /// `*_after_completed_*` cases). This pins the other two, which are the
    /// arms a proxy that re-orders the tail of the stream would actually
    /// exercise — it re-sends fragments and items without re-sending the
    /// terminal.
    ///
    /// The assertion is the observable consequence, and it is the honest shape
    /// for this: an ungated `added` would push a `ToolCallDelta` carrying
    /// `id` + `name`, and an ungated `delta` would push one per fragment, so
    /// the ABSENCE of every `ToolCallDelta` after the `Done` is exactly what
    /// "the gate engaged" looks like from inside the process. The accumulator
    /// residue the gate also prevents is not observable here — the gate
    /// returns before touching `tool_acc`, which is the point.
    ///
    /// **What this does NOT pin: the `eprintln!` itself.** A lib unit test
    /// cannot capture the process's stderr — it runs inside cargo's test
    /// harness, whose stderr goes to the harness, and the only production
    /// reader of a Worker's stderr is the drain task in `worker/client.rs`,
    /// which needs a real spawned Worker. So the log line is unpinned, the
    /// same way `the_chosen_wire_is_logged_at_construction` records its own
    /// inability to reach the `eprintln!` half. What is pinned is that the
    /// DISCARD happens; the line is there so that when it does engage in the
    /// field somebody can find out.
    #[tokio::test]
    async fn responses_sse_item_events_after_completed_are_dropped_not_accumulated() {
        let events = collect(responses_stream(&[
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
             event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_late\",\"name\":\"write\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":2,\"delta\":\"{\\\"path\\\":\\\"/receipts/totals.csv\\\"}\"}\n\n\
             event: response.output_item.done\n\
             data: {\"type\":\"response.output_item.done\",\"output_index\":2,\"item\":{\"type\":\"function_call\",\"id\":\"fc_late\",\"name\":\"write\",\"arguments\":\"{\\\"path\\\":\\\"/receipts/totals.csv\\\"}\",\"status\":\"completed\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Done(FinishReason::Stop)],
            "a terminal ENDS the response: no `ToolCallDelta` and no second \
             `ToolCall` may follow the `Done` — an ungated `added` or `delta` \
             would push a `ToolCallDelta` right here (got {events:?})"
        );
    }

    // ── the per-call cost off the BODY's usage object (D4, AMENDED) ─────

    /// The cost rides the terminal event's `response.usage` — confirmed
    /// live on this wire by probe P3 (`cost` AND an equal
    /// `cost_details.upstream_inference_cost`, WITHOUT the client asking:
    /// the Responses wire sends usage unconditionally).
    #[tokio::test]
    async fn responses_sse_cost_rides_the_completed_response_usage() {
        let events = collect(responses_stream(&[
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5,\"cost\":2.122e-05}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                    },
                    Some(2.122e-05)
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// An INCOMPLETE terminal still costed its tokens (the model was
    /// called and the provider priced it) — the cost rides all three
    /// terminal arms, not just the happy one.
    #[tokio::test]
    async fn responses_sse_cost_rides_the_incomplete_terminal() {
        let events = collect(responses_stream(&[
            "event: response.incomplete\n\
             data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":32000,\"cost\":0.25}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 32000,
                    },
                    Some(0.25)
                ),
                ProviderEvent::Done(FinishReason::Length),
            ]
        );
    }

    /// … and so does a FAILED one (a failed call is still a call the
    /// provider may have priced).
    #[tokio::test]
    async fn responses_sse_cost_rides_the_failed_terminal() {
        let events = collect(responses_stream(&[
            "event: response.failed\n\
             data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":1,\"cost\":0.003}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 1,
                    },
                    Some(0.003)
                ),
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    /// A terminal `usage` with counts and no money is `None`, not `0.0`
    /// (D5: an unpriced call renders NOTHING, not a free `$0.00`).
    #[tokio::test]
    async fn responses_sse_absent_cost_is_none_not_zero() {
        let events = collect(responses_stream(&[
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// A malformed money value is `None` — no panic, no clamp — and the
    /// token counts still arrive.
    #[tokio::test]
    async fn responses_sse_malformed_cost_is_none() {
        for malformed in [json!("abc"), Value::Null, json!({}), json!(-0.5)] {
            let payload = json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "usage": { "input_tokens": 10, "output_tokens": 5, "cost": malformed },
                },
            });
            let events = collect(responses_stream(&[&format!(
                "event: response.completed\ndata: {payload}\n\n"
            )]))
            .await;
            assert_eq!(
                events,
                vec![
                    ProviderEvent::Usage(
                        Usage {
                            input_tokens: 10,
                            output_tokens: 5,
                        },
                        None
                    ),
                    ProviderEvent::Done(FinishReason::Stop),
                ],
                "a malformed cost ({malformed}) is None, not a panic"
            );
        }
    }

    /// REGRESSION GUARD (a recorded residue, not a RED pin): a terminal
    /// event whose `response.usage` carries a cost and NEITHER token count
    /// emits NO `Usage` — `response_usage`'s `(None, None) => None` arm
    /// still returns `None`, so the money has no event to ride. This is
    /// this wire's share of the "a cost with no token counts is dropped"
    /// residue; `responses_sse_failed` does NOT cover it (that fixture has
    /// no `usage` object at all, so it exercises the missing-`usage` early
    /// return, not the count arm). Asserts an ABSENCE, so it is green
    /// before the change as well as after.
    #[tokio::test]
    async fn responses_sse_cost_with_no_token_counts_emits_no_usage_event() {
        let events = collect(responses_stream(&[
            "event: response.completed\n\
             data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"cost\":0.003}}}\n\n",
        ]))
        .await;
        assert!(
            !events.iter().any(|e| matches!(e, ProviderEvent::Usage(..))),
            "a cost with no counts emits NO Usage (got {events:?})"
        );
    }

    #[tokio::test]
    async fn responses_sse_error_event() {
        // An in-stream `error` event whose body the classifier cannot read
        // stays the TERMINAL `Done(Error)` — deliberately. The Responses
        // `error` object has no stable `type` taxonomy, and this
        // `server_error` body carries NEITHER a classifier wire key nor an
        // overflow phrase, so `parse_error_chunk` answers on its `None` side
        // and the upgrade keeps it terminal: only ADR 0045-classified
        // overflow / auth bodies upgrade to a real tier (see
        // `responses_sse_context_overflow_error_event` /
        // `responses_sse_auth_error_event`). Pushing an `Error` for an
        // unclassifiable body would hand the `RetryPolicy` a tier this wire
        // cannot stand behind, on a turn whose partial deltas the
        // `AgentLoop` has ALREADY consumed.
        let events = collect(responses_stream(&[
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events.len(),
            1,
            "an `error` event is terminal (the stream ends)"
        );
        assert_eq!(
            events[0],
            ProviderEvent::Done(FinishReason::Error),
            "an unclassifiable `error` body stays the bare terminal — the \
             divergence from the Anthropic wire is deliberate"
        );
    }

    /// (ADR 0045) The upgrade: an over-window prompt reported INSIDE the
    /// Responses stream is a real `ContextOverflow` tier, so the loop's
    /// mid-stream recovery (compact in the turn, never ladder-retry —
    /// `context_overflow_is_never_retried`) is reachable on this wire.
    #[tokio::test]
    async fn responses_sse_context_overflow_error_event() {
        let events = collect(responses_stream(&[
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your input exceeds the context window of this model\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Error(ProviderError::ContextOverflow(
                "invalid_request_error: Your input exceeds the context window of this model"
                    .to_string()
            ))],
            "an in-stream overflow upgrades to the real tier (exactly one \
             event — no `Done` alongside it)"
        );
        match &events[0] {
            ProviderEvent::Error(ProviderError::ContextOverflow(msg)) => {
                assert!(msg.contains("Your input exceeds"), "got {msg:?}");
            }
            other => panic!("expected a ContextOverflow error, got {other:?}"),
        }
    }

    /// (ADRs 0081 / 0082) The other upgraded tier: a bad key reported inside
    /// the Responses stream is the `Auth` tier — returned verbatim in ONE
    /// call (no doomed ladder retries) and the tier the settings UI keys on.
    /// The terminal path for unclassifiable bodies is pinned by
    /// `responses_sse_error_event`, and the stream-end synthesis by
    /// `responses_sse_stream_end_without_terminal` — both stay green.
    #[tokio::test]
    async fn responses_sse_auth_error_event() {
        let events = collect(responses_stream(&[
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"Bad key\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Error(ProviderError::Auth(
                "authentication_error: Bad key".to_string()
            ))],
            "an in-stream auth failure upgrades to the real tier (exactly \
             one event — no `Done` alongside it)"
        );
        match &events[0] {
            ProviderEvent::Error(ProviderError::Auth(msg)) => {
                assert!(msg.contains("Bad key"), "got {msg:?}");
            }
            other => panic!("expected an Auth error, got {other:?}"),
        }
    }

    /// The `error` arm flushes too — the fourth of the four terminal arms
    /// `flush_tool_calls` was added to, and the one with no red-test
    /// evidence until now (every pre-existing `event: error` fixture
    /// carries no `function_call`, so deleting the flush in this arm left
    /// the suite green).
    ///
    /// The unclassifiable body: the in-flight call is flushed BEFORE the
    /// terminal `Done(Error)`, so the loop sees the card it was shown and
    /// the turn settles with a finish reason rather than a vanished call.
    #[tokio::test]
    async fn responses_sse_error_event_flushes_a_partial_function_call() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"write\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: response.function_call_arguments.delta\n\
             data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{\\\"path\\\":\\\"/x\\\"}\"}\n\n\
             event: error\n\
             data: {\"type\":\"error\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("write".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{\"path\":\"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "write".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
                }),
                ProviderEvent::Done(FinishReason::Error),
            ],
            "an in-stream error still flushes the call in flight, before the \
             terminal Done (got {events:?})"
        );
    }

    /// … and on the branch that pushes a real `ProviderEvent::Error`
    /// instead of a `Done` — the tier the arm's own docblock singles out.
    /// This is the branch the flush most needed to cover: its
    /// `terminated = true` is written BEFORE the `parse_error_chunk`
    /// match, so it covers BOTH arms, and the whole point of the flush
    /// line is that it runs BEFORE the match. The vector therefore pins
    /// the `ToolCall` PRECEDING the `Error` (an `Error` first would put
    /// the recovery tier ahead of the card the loop has to render).
    #[tokio::test]
    async fn responses_sse_auth_error_flushes_the_call_before_the_error() {
        let events = collect(responses_stream(&[
            "event: response.output_item.added\n\
             data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_9\",\"name\":\"write\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n\
             event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"Bad key\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("fc_9".to_string()),
                    name: Some("write".to_string()),
                    arguments: None,
                }),
                // No `arguments.delta` arrived, so the flushed call
                // carries the `added` item's empty `arguments` → `{}`
                // with NO defect (an empty string is not malformed).
                ProviderEvent::ToolCall(ToolCall {
                    id: "fc_9".to_string(),
                    name: "write".to_string(),
                    arguments: json!({}),
                    defect: None,
                }),
                ProviderEvent::Error(ProviderError::Auth(
                    "authentication_error: Bad key".to_string()
                )),
            ],
            "the flush precedes the upgraded `Error` tier too (got {events:?})"
        );
    }

    #[tokio::test]
    async fn responses_sse_stream_end_without_terminal() {
        // One `output_text.delta` only (no terminal event): the stream end
        // synthesizes a `Done(Error)`. NO `Usage` — a Responses `usage`
        // arrives only on the terminal events, and a zero-filled `Usage`
        // would misreport.
        let events = collect(responses_stream(&["event: response.output_text.delta\n\
             data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n"]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("x".to_string()),
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    /// (finding 9, the Responses wire) The raw bytes may split a
    /// MULTI-BYTE codepoint between chunks (CJK is 3 bytes, emoji is 4).
    /// The buffer must hold RAW bytes and convert only COMPLETE lines: the
    /// reassembled text is byte-identical at every possible split point.
    #[tokio::test]
    async fn responses_multibyte_chars_survive_chunk_boundaries() {
        // CJK (3-byte) + emoji (4-byte) content in a Responses
        // `output_text.delta` line.
        let text = "你好🌍世界🚀";
        let payload = format!(
            "event: response.output_text.delta\n\
             data: {{\"type\":\"response.output_text.delta\",\"delta\":\"{text}\"}}\n\n"
        );
        let bytes = payload.as_bytes();
        // Split the raw bytes at EVERY offset — including the middle of a
        // 3-byte and a 4-byte codepoint. Each reassembly must be
        // byte-identical (no U+FFFD).
        for split_at in 1..bytes.len() {
            let events = collect(responses_stream_bytes(&[
                bytes[..split_at].to_vec(),
                bytes[split_at..].to_vec(),
            ]))
            .await;
            let reassembled: String = events
                .iter()
                .filter_map(|e| match e {
                    ProviderEvent::TextDelta(t) => Some(t.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(
                reassembled, text,
                "split at byte {split_at} corrupts the text (got {reassembled:?})"
            );
            assert!(
                !reassembled.contains('\u{fffd}'),
                "split at byte {split_at} yields a replacement character"
            );
        }
        // A 4-byte emoji split across THREE chunks (1+1+2 bytes) — the
        // worst case: two codepoint boundaries inside one codepoint.
        let eb = bytes;
        let start = eb
            .iter()
            .position(|&b| b == 0xF0)
            .expect("the emoji's lead byte is present");
        let events = collect(responses_stream_bytes(&[
            eb[..start + 1].to_vec(),
            eb[start + 1..start + 2].to_vec(),
            eb[start + 2..].to_vec(),
        ]))
        .await;
        let reassembled: String = events
            .iter()
            .filter_map(|e| match e {
                ProviderEvent::TextDelta(t) => Some(t.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(reassembled, text, "the 1+1+2 split reassembles identically");
    }

    // ── the OpenAI Responses provider (ADR 0024) ──────────────────────

    /// The `OpenAiResponsesProvider` is object-safe (a `Box<dyn
    /// Provider>` compiles — the `SessionManager`'s factory dispatches on
    /// it).
    #[test]
    fn openai_responses_provider_is_object_safe() {
        let provider: Box<dyn Provider> = Box::new(OpenAiResponsesProvider {
            base_url: "https://api.openai.com/v1".to_string(),
            api_key: "k".to_string(),
            custom_headers: None,
        });
        let _ = provider;
    }

    #[test]
    fn responses_request_body_maps_the_request() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                chat_message(
                    ChatRole::System,
                    MessageContent::Text("sys".to_string()),
                    None,
                    None,
                ),
                chat_message(
                    ChatRole::User,
                    MessageContent::Text("hi".to_string()),
                    None,
                    None,
                ),
                chat_message(
                    ChatRole::Assistant,
                    MessageContent::Text("out".to_string()),
                    None,
                    None,
                ),
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text("result".to_string()),
                    Some("fc_1"),
                    None,
                ),
            ],
            tools: vec![ToolSpec {
                name: "read".to_string(),
                description: "Read a file".to_string(),
                parameters: json!({ "type": "object" }),
            }],
            options: ModelOptions {
                temperature: Some(0.2),
                max_tokens: Some(64),
                reasoning_effort: Some("high".to_string()),
                stream: true,
            },
            session_id: None,
        };
        let body = responses_request_body(&req);
        assert_eq!(body["model"], "m");
        assert_eq!(body["instructions"], "sys");
        // The non-system messages → `input` items (NO same-role merging —
        // the Responses API accepts consecutive same-role items): the
        // `user` string content, the `assistant` string content, and the
        // `function_call_output` item (the `tool` message — the `item`
        // type IS the role: NO `role` field).
        let input = body["input"].as_array().expect("input is present");
        assert_eq!(input.len(), 3);
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"], "hi");
        assert_eq!(input[1]["role"], "assistant");
        assert_eq!(input[1]["content"], "out");
        assert_eq!(input[2]["type"], "function_call_output");
        assert_eq!(input[2]["call_id"], "fc_1");
        assert_eq!(input[2]["output"], "result");
        assert!(
            input[2].get("role").is_none(),
            "a `function_call_output` item has NO `role` field"
        );
        // `tools`: flat `{ type: "function", name, description,
        // parameters }` — NO `function` nesting.
        assert_eq!(
            body["tools"][0],
            json!({
                "type": "function",
                "name": "read",
                "description": "Read a file",
                "parameters": { "type": "object" },
            })
        );
        assert!(
            body["tools"][0].get("function").is_none(),
            "the Responses `tools` entry has NO `function` nesting"
        );
        // `max_output_tokens` (the Responses name — unlike Anthropic's
        // required `max_tokens`, included only when `Some`).
        assert_eq!(body["max_output_tokens"], 64);
        let temperature = body["temperature"]
            .as_f64()
            .expect("temperature is a number");
        assert!(
            (temperature - 0.2).abs() < 1e-6,
            "temperature is passed through (got {temperature})"
        );
        assert_eq!(body["stream"], true);
        // `reasoning`: the level passed through VERBATIM.
        assert_eq!(body["reasoning"], json!({ "effort": "high" }));
    }

    #[test]
    fn responses_request_body_blocks_and_image() {
        // `Blocks` content → an array of Responses parts: a `Text` block →
        // `{ type: "input_text", text }`, an `Image` block →
        // `{ type: "input_image", image_url: "data:<mime>;base64,<data>"
        // }` (the OpenAI data-URI — WITH the `data:` prefix, unlike the
        // Anthropic verbatim-base64 shape; the part `type` is
        // `input_image`, NOT `image_url`).
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![chat_message(
                ChatRole::User,
                MessageContent::Blocks(vec![
                    ContentBlock::Text {
                        text: "a".to_string(),
                    },
                    ContentBlock::Image {
                        image: ImageRef {
                            data: "B64".to_string(),
                            mime_type: "image/png".to_string(),
                        },
                    },
                ]),
                None,
                None,
            )],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        let body = responses_request_body(&req);
        let content = body["input"][0]["content"]
            .as_array()
            .expect("the content is a parts array");
        assert_eq!(content[0], json!({ "type": "input_text", "text": "a" }));
        assert_eq!(
            content[1],
            json!({
                "type": "input_image",
                "image_url": "data:image/png;base64,B64",
            })
        );
    }

    #[test]
    fn responses_request_body_omits_none_options() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        let body = responses_request_body(&req);
        assert!(body.get("temperature").is_none());
        assert!(body.get("max_output_tokens").is_none());
        assert!(body.get("reasoning").is_none());
        assert!(body.get("tools").is_none());
        // `messages: []` → an empty `input` array (the chosen shape).
        let input = body["input"].as_array().expect("input is present");
        assert!(input.is_empty(), "no messages → an empty `input` array");
        // `instructions` MUST be absent when there are no system messages.
        assert!(
            body.get("instructions").is_none(),
            "no system messages → NO `instructions` field"
        );
    }

    /// An `Assistant` message with `tool_calls` emits ONE `function_call`
    /// item PER CALL (in call order — `arguments` as a JSON **string**,
    /// the Responses wire form), preceded by a `message` item for the
    /// text content (an EMPTY `Text` → `function_call` items ONLY). This
    /// is REQUIRED: the Responses API validates that each
    /// `function_call_output`'s `call_id` references a `function_call`
    /// item in `input` — dropping the `function_call` items makes the
    /// second turn of every tool-using `openai-responses` session 400.
    #[test]
    fn responses_request_body_serializes_tool_calls_as_function_call_items() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                chat_message(
                    ChatRole::Assistant,
                    MessageContent::Text(String::new()),
                    None,
                    Some(vec![
                        ToolCall {
                            id: "fc_1".to_string(),
                            name: "read".to_string(),
                            arguments: json!({ "path": "/x" }),
                            defect: None,
                        },
                        ToolCall {
                            id: "fc_2".to_string(),
                            name: "bash".to_string(),
                            arguments: Value::Object(Default::default()),
                            // (issue #193) DEFECT-CARRYING, so the exact-key-set
                            // pin on `input[1]` below is the leak guard for a
                            // call that actually HAS a defect (see the pin).
                            defect: Some(crate::harness::provider::ArgumentDefect {
                                error: "expected value at line 1 column 1".to_string(),
                                raw: "{not json".to_string(),
                            }),
                        },
                    ]),
                ),
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text("out".to_string()),
                    Some("fc_1"),
                    None,
                ),
            ],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        let body = responses_request_body(&req);
        let input = body["input"].as_array().expect("input is present");
        // In order: the `function_call` items (per call, call order),
        // then the `function_call_output` item. NO `message` item for the
        // assistant (EMPTY `Text`).
        assert_eq!(
            input.len(),
            3,
            "two `function_call` items + one `function_call_output` (no `message` item for the empty `Text`), got {input:?}"
        );
        // `fc_1`: `arguments` as a JSON **string** (not an object).
        assert_eq!(input[0]["type"], "function_call");
        assert_eq!(input[0]["call_id"], "fc_1");
        assert_eq!(input[0]["name"], "read");
        assert_eq!(input[0]["arguments"], "{\"path\":\"/x\"}");
        assert!(
            input[0]["arguments"].is_string(),
            "the `arguments` is a JSON **string**, not an object"
        );
        // `fc_2`: an empty-arguments call serializes to `"{}"`.
        assert_eq!(input[1]["type"], "function_call");
        assert_eq!(input[1]["call_id"], "fc_2");
        assert_eq!(input[1]["name"], "bash");
        assert_eq!(input[1]["arguments"], "{}");
        // The `function_call_output` item: the `call_id` references the
        // `function_call` items' `call_id`s (the API's validation
        // target); NO `role` field.
        assert_eq!(input[2]["type"], "function_call_output");
        assert_eq!(input[2]["call_id"], "fc_1");
        assert_eq!(input[2]["output"], "out");
        assert!(input[2].get("role").is_none());
        // NO `message` item anywhere (the assistant's `Text` was empty).
        assert!(
            input.iter().all(|item| item.get("role").is_none()),
            "an EMPTY `Text` → `function_call` items ONLY, no `message` item: {input:?}"
        );

        // ── EXACT-FIELD-SET PINS (the leak guard) ───────────────────────
        // `responses_input` is a field-by-field WHITELIST, but nothing tested
        // that. `ChatMessage` is BOTH the transcript type AND the type this
        // body is built from, so a transcript-only field (e.g. a future
        // `finish_reason`) must NEVER appear here. On THIS wire an assistant
        // with calls has no `message` item at all — the pin goes on the
        // `function_call` / `function_call_output` items it became.
        assert_eq!(
            keys_of_names(&body),
            vec!["input", "model", "stream"],
            "the Responses body carries exactly these keys (no `instructions` \
             — no system text; no `tools` / `temperature` / `max_output_tokens` \
             / `reasoning` — this fixture sets none of them)"
        );
        assert_eq!(
            keys_of_names(&input[0]),
            vec!["arguments", "call_id", "name", "type"],
            "a `function_call` item is exactly `type` + `call_id` + `name` + \
             `arguments` — a transcript-only field must not appear here"
        );
        assert_eq!(
            keys_of_names(&input[1]),
            vec!["arguments", "call_id", "name", "type"],
            "the second `function_call` item carries the same exact set — and \
             it is the DEFECT-CARRYING call, so `defect` demonstrably has no \
             path to this wire either"
        );
        assert_eq!(
            keys_of_names(&input[2]),
            vec!["call_id", "output", "type"],
            "a `function_call_output` item is exactly `type` + `call_id` + \
             `output` — a transcript-only field must not appear here"
        );
    }
}

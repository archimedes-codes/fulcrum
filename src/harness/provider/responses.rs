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

use super::openai::{parse_arguments, LineAssembler};
use super::transport::{build_client, litellm_headers, map_response};
use super::types::{
    ChatRole, FinishReason, MessageContent, ModelRequest, Provider, ProviderError, ProviderEvent,
    ToolCall, ToolCallDelta, Usage,
};
use crate::agent::tools::ContentBlock;

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
    /// `Done` at stream end).
    terminated: bool,
    /// The raw stream is exhausted (the stream ends after `pending`).
    exhausted: bool,
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
                        self.pending.push_back(ProviderEvent::ToolCall(ToolCall {
                            id: id.unwrap_or_default(),
                            name,
                            arguments: parse_arguments(&acc.arguments),
                        }));
                    }
                }
                // `message` / `reasoning` items: nothing (their content
                // already streamed via the `*_delta` events).
            }
            Some("response.completed") => {
                // The terminal event: the `usage` (when at least one side
                // is present) PRECEDES the `Done`.
                if let Some(usage) = self.response_usage(v) {
                    self.pending.push_back(ProviderEvent::Usage(usage));
                }
                self.terminated = true;
                if v.get("response")
                    .and_then(|r| r.get("status"))
                    .and_then(|s| s.as_str())
                    == Some("completed")
                {
                    self.pending
                        .push_back(ProviderEvent::Done(FinishReason::Stop));
                }
            }
            Some("response.incomplete") => {
                if let Some(usage) = self.response_usage(v) {
                    self.pending.push_back(ProviderEvent::Usage(usage));
                }
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
                self.pending.push_back(ProviderEvent::Done(finish));
            }
            Some("response.failed") => {
                if let Some(usage) = self.response_usage(v) {
                    self.pending.push_back(ProviderEvent::Usage(usage));
                }
                self.terminated = true;
                self.pending
                    .push_back(ProviderEvent::Done(FinishReason::Error));
            }
            Some("error") => {
                // An in-stream `error` event (the HTTP status was 200 —
                // the error is IN the stream). Terminal: the turn cannot
                // continue (deliberately simpler than the Anthropic
                // `error` mapping — the Responses `error` object has no
                // stable `type` taxonomy, and a mid-stream error after a
                // partial response must NOT be blindly retried: the
                // `AgentLoop` has already consumed the partial deltas,
                // so the `RetryPolicy` sees a non-retryable end).
                self.exhausted = true;
                self.terminated = true;
                self.pending
                    .push_back(ProviderEvent::Done(FinishReason::Error));
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

    /// Emit the accumulated tool calls as complete `ToolCall` events
    /// (dropping any acc that never arrived with a `name` — a
    /// `function_call` item without a `name` cannot be dispatched — and
    /// any `done` acc: its `output_item.done` already emitted the
    /// `ToolCall`, flushing it again would duplicate it).
    fn flush_tool_calls(&mut self) {
        for (_index, acc) in std::mem::take(&mut self.tool_acc).into_iter() {
            if acc.done {
                continue;
            }
            let Some(name) = acc.name else {
                continue;
            };
            self.pending.push_back(ProviderEvent::ToolCall(ToolCall {
                id: acc.id.unwrap_or_default(),
                name,
                arguments: parse_arguments(&acc.arguments),
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
    // `instructions`: the `System` messages' text (the same collection as
    // Anthropic's `system` — `Text` verbatim; `Blocks` → the `Text`
    // block texts joined with `"\n\n"`; the messages joined with
    // `"\n\n"`).
    let instructions = req
        .messages
        .iter()
        .filter(|m| m.role == ChatRole::System)
        .map(|m| match &m.content {
            MessageContent::Text(t) => t.clone(),
            MessageContent::Blocks(blocks) => blocks
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text { text } => Some(text.clone()),
                    ContentBlock::Image { .. } => None,
                })
                .collect::<Vec<_>>()
                .join("\n\n"),
        })
        .collect::<Vec<_>>()
        .join("\n\n");
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
            Duration::from_secs(30),     // a generous CONNECT bound
            Duration::from_secs(5 * 60), // 5 min of SILENCE = a stalled provider
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
    use super::super::test_util::{chat_message, collect};
    use super::super::types::{ModelOptions, ToolSpec};
    use super::*;
    use crate::agent::tools::ImageRef;

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
                ProviderEvent::Usage(Usage {
                    input_tokens: 10,
                    output_tokens: 5,
                }),
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
                ProviderEvent::Usage(Usage {
                    input_tokens: 10,
                    output_tokens: 3,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ]
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
                ProviderEvent::Usage(Usage {
                    input_tokens: 10,
                    output_tokens: 32000,
                }),
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

    #[tokio::test]
    async fn responses_sse_error_event() {
        // An in-stream `error` event → `Done(Error)` (terminal — the turn
        // cannot continue; deliberately simpler than the Anthropic
        // `error` mapping: the Responses `error` object has no stable
        // `type` taxonomy, and a mid-stream error after a partial
        // response must not be blindly retried).
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
        assert_eq!(events[0], ProviderEvent::Done(FinishReason::Error));
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
                        },
                        ToolCall {
                            id: "fc_2".to_string(),
                            name: "bash".to_string(),
                            arguments: Value::Object(Default::default()),
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
    }
}

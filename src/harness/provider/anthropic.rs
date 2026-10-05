//! The Anthropic Messages stack: `AnthropicProvider`, its request-body
//! mappers, and the `AnthropicStream` `event:`-tagged SSE parser.
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
use super::transport::{build_client, litellm_headers, map_response, ANTHROPIC_API_VERSION};
use super::types::{
    ChatRole, FinishReason, MessageContent, ModelRequest, Provider, ProviderError, ProviderEvent,
    ToolCall, ToolCallDelta, Usage,
};
use crate::agent::tools::ContentBlock;

/// Map an Anthropic `stop_reason` to a [`FinishReason`].
fn map_anthropic_stop_reason(reason: &str) -> FinishReason {
    match reason {
        "end_turn" => FinishReason::Stop,
        "tool_use" => FinishReason::ToolCalls,
        "max_tokens" => FinishReason::Length,
        // `stop_sequence` (a custom stop sequence hit) is a normal stop.
        "stop_sequence" => FinishReason::Stop,
        _ => {
            eprintln!("harness: unknown anthropic stop_reason {reason:?} — treating as a stop");
            FinishReason::Stop
        }
    }
}

/// Normalize an Anthropic-wire base URL to include the `/v1` API prefix
/// (ADR 0024 — mirroring ZCode's `normalizeAnthropicBaseURL`, applied at
/// the adapter boundary). The Anthropic client appends `/messages` (and
/// discovery appends `/models`) to the base, so a GATEWAY ROOT that does
/// not already end in `/v1` gets it appended: `https://openrouter.ai/api`
/// → `…/api/v1`. Idempotent (a base whose path already ends in `/v1` is
/// left as-is, trailing slash dropped).
///
/// Operates on the URL PATH (not the full string, so a query string is
/// not corrupted — ZCode's exact behavior); an unparseable base falls
/// back to a plain suffix check (ZCode's `catch` arm).
pub fn normalize_anthropic_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(mut url) = url::Url::parse(trimmed) {
        // An OWNED `String` (NOT a `&str` borrowing from `url` — the
        // `set_path` below mutates `url`, so the borrow must not outlive it).
        let pathname = url.path().trim_end_matches('/').to_string();
        if pathname.to_lowercase().ends_with("/v1") {
            url.set_path(&pathname); // drop any trailing slash
            return url.to_string();
        }
        let with_v1 = format!("{pathname}/v1");
        url.set_path(&with_v1);
        return url.to_string();
    }
    // Fallback (unparseable): a plain suffix check on the trimmed string.
    let without_trailing = trimmed.trim_end_matches('/');
    if without_trailing.to_lowercase().ends_with("/v1") {
        without_trailing.to_string()
    } else {
        format!("{without_trailing}/v1")
    }
}

/// Map an Anthropic `error` event's `type` to a [`ProviderError`] (the
/// wire `type` is the classification: `overloaded_error` / `api_error`
/// are transient, `authentication_error` / `permission_error` are auth,
/// the rest are permanent; an UNKNOWN type is conservative — retryable,
/// a new error type is probably transient, and the `RetryPolicy`'s
/// budget bounds the damage).
fn map_anthropic_error(v: &Value) -> ProviderError {
    let message = v
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("(no message)");
    let kind = v
        .get("error")
        .and_then(|e| e.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    match kind {
        "overloaded_error" | "api_error" => ProviderError::Retryable(format!("{kind}: {message}")),
        "authentication_error" | "permission_error" => {
            ProviderError::Auth(format!("{kind}: {message}"))
        }
        "invalid_request_error" | "not_found_error" => {
            ProviderError::Fatal(format!("{kind}: {message}"))
        }
        _ => {
            let label = if kind.is_empty() { "unknown" } else { kind };
            ProviderError::Retryable(format!("{label}: {message}"))
        }
    }
}

/// The accumulated per-block tool-call state for the Anthropic wire (a
/// `tool_use` content block: the `id` + `name` arrive at
/// `content_block_start`, the `input` JSON text accumulates as
/// `input_json_delta` fragments).
#[derive(Default, Clone)]
struct AnthropicToolAcc {
    id: Option<String>,
    name: Option<String>,
    /// The argument fragments concatenated (JSON text).
    arguments: String,
}

/// The Anthropic Messages SSE parser: turns raw response bytes into
/// [`ProviderEvent`]s. The wire is `event:`-TAGGED (every `data:` line
/// is preceded by an `event: <name>` line — the OpenAI wire carries no
/// `event:` tags, so this parser tracks the current event name and
/// dispatches on it): `message_start` (the `usage.input_tokens`),
/// `content_block_start` (a `text` / `thinking` / `tool_use` block —
/// the `tool_use` block carries the `id` + `name`), `content_block_delta`
/// (`text_delta` / `thinking_delta` / `input_json_delta` fragments),
/// `content_block_stop`, `message_delta` (the `stop_reason` + the
/// `usage.output_tokens`), `message_stop`, `error`.
struct AnthropicStream {
    /// The raw byte stream (`reqwest::bytes_stream`, mapped to `Vec<u8>`).
    raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>>,
    /// The raw-bytes line assembler (the multi-byte-codepoint safety —
    /// finding 9).
    lines: LineAssembler,
    /// Events parsed but not yet delivered.
    pending: VecDeque<ProviderEvent>,
    /// The current SSE `event:` name (a `data:` line without one is
    /// dispatched on its `type` field instead — the Anthropic wire
    /// always tags its events, but a proxy may strip the tags; every
    /// Anthropic event carries a `type` matching its event name).
    event: Option<String>,
    /// Per-block tool-call accumulation (the `content_block.index` of a
    /// `tool_use` block; flushed as `ToolCall`s at the `message_delta`
    /// `stop_reason` / stream end).
    tool_acc: BTreeMap<u32, AnthropicToolAcc>,
    /// The `message_start` input tokens (the `usage` there carries the
    /// INPUT count only; the output comes from `message_delta`).
    input_tokens: Option<u32>,
    /// The last `message_delta` `usage.output_tokens` seen (cumulative on
    /// the wire — the FINAL `message_delta` carries the full count).
    last_output_tokens: Option<u32>,
    /// A `message_delta` with a `stop_reason` was seen (no synthetic
    /// `Done` at end).
    finished: bool,
    /// A combined `Usage` was emitted (exactly once — the `message_delta`
    /// `stop_reason` event, or the stream end when it never arrived).
    usage_emitted: bool,
    /// The raw stream is exhausted (the stream ends after `pending`).
    exhausted: bool,
}

impl AnthropicStream {
    fn new(raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>>) -> Self {
        Self {
            raw,
            lines: LineAssembler::new(),
            pending: VecDeque::new(),
            event: None,
            tool_acc: BTreeMap::new(),
            input_tokens: None,
            last_output_tokens: None,
            finished: false,
            usage_emitted: false,
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
    /// Anthropic event carries a `type` matching its event name).
    fn handle_chunk(&mut self, v: &Value) {
        let kind = self
            .event
            .as_deref()
            .or_else(|| v.get("type").and_then(|t| t.as_str()));
        match kind {
            Some("message_start") => {
                if let Some(t) = v
                    .get("message")
                    .and_then(|m| m.get("usage"))
                    .and_then(|u| u.get("input_tokens"))
                    .and_then(|t| t.as_u64())
                {
                    self.input_tokens = Some(u32::try_from(t).unwrap_or(u32::MAX));
                }
            }
            Some("content_block_start") => {
                let Some(index) = v.get("index").and_then(|i| i.as_u64()).map(|i| i as u32) else {
                    return;
                };
                let block = v.get("content_block");
                if block.and_then(|b| b.get("type")).and_then(|t| t.as_str()) == Some("tool_use") {
                    let mut acc = AnthropicToolAcc::default();
                    if let Some(id) = block.and_then(|b| b.get("id")).and_then(|i| i.as_str()) {
                        acc.id = Some(id.to_string());
                    }
                    if let Some(name) = block.and_then(|b| b.get("name")).and_then(|n| n.as_str()) {
                        acc.name = Some(name.to_string());
                    }
                    self.tool_acc.insert(index, acc.clone());
                    // The `AgentLoop` renders the tool call as soon as its
                    // `name` is known (the `input` JSON still streams) —
                    // the `ToolCallDelta` carries the `id` + `name`.
                    self.pending
                        .push_back(ProviderEvent::ToolCallDelta(ToolCallDelta {
                            index,
                            id: acc.id.clone(),
                            name: acc.name.clone(),
                            arguments: None,
                        }));
                }
                // `text` / `thinking` blocks: their deltas carry the
                // fragments — nothing to record at `start`.
            }
            Some("content_block_delta") => {
                let Some(index) = v.get("index").and_then(|i| i.as_u64()).map(|i| i as u32) else {
                    return;
                };
                let delta = v.get("delta");
                match delta.and_then(|d| d.get("type")).and_then(|t| t.as_str()) {
                    Some("text_delta") => {
                        if let Some(text) =
                            delta.and_then(|d| d.get("text")).and_then(|t| t.as_str())
                        {
                            self.pending
                                .push_back(ProviderEvent::TextDelta(text.to_string()));
                        }
                    }
                    Some("thinking_delta") => {
                        if let Some(thinking) = delta
                            .and_then(|d| d.get("thinking"))
                            .and_then(|t| t.as_str())
                        {
                            self.pending
                                .push_back(ProviderEvent::ThinkingDelta(thinking.to_string()));
                        }
                    }
                    Some("input_json_delta") => {
                        if let Some(fragment) = delta
                            .and_then(|d| d.get("partial_json"))
                            .and_then(|t| t.as_str())
                        {
                            let acc = self.tool_acc.entry(index).or_default();
                            acc.arguments.push_str(fragment);
                            self.pending
                                .push_back(ProviderEvent::ToolCallDelta(ToolCallDelta {
                                    index,
                                    id: None,
                                    name: None,
                                    arguments: Some(fragment.to_string()),
                                }));
                        }
                    }
                    _ => {}
                }
            }
            Some("message_delta") => {
                // The `usage.output_tokens` (cumulative ��� the FINAL
                // `message_delta` carries the full count; an intermediate
                // one's value is a lower bound, so the combined `Usage`
                // is emitted ONCE, at the `stop_reason` event / stream
                // end, from the LAST value seen).
                if let Some(t) = v
                    .get("usage")
                    .and_then(|u| u.get("output_tokens"))
                    .and_then(|t| t.as_u64())
                {
                    self.last_output_tokens = Some(u32::try_from(t).unwrap_or(u32::MAX));
                }
                if let Some(reason) = v
                    .get("delta")
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(|r| r.as_str())
                {
                    self.emit_usage();
                    self.flush_tool_calls();
                    self.pending
                        .push_back(ProviderEvent::Done(map_anthropic_stop_reason(reason)));
                    self.finished = true;
                }
            }
            Some("error") => {
                // A mid-stream `error` event (the HTTP status was 200 —
                // the error is IN the stream). The `AgentLoop` records
                // it (a `Done` may still follow on a healthy stream end;
                // the error takes precedence in the loop's accounting).
                self.exhausted = true;
                self.pending
                    .push_back(ProviderEvent::Error(map_anthropic_error(v)));
            }
            _ => {
                // `message_stop` / `content_block_stop` / unknown: no
                // event to deliver (a `message_stop` after a
                // `message_delta` is the healthy end — the `Done` was
                // already emitted; a `message_stop` WITHOUT a
                // `message_delta` hits the stream-end synthetic `Done`).
            }
        }
        // Consume the `event:` tag (a `data:` line belongs to the
        // PRECEDING `event:` line; the next `event:` line starts a new
        // event — the tags and the `data:` lines alternate on the wire).
        self.event = None;
    }

    /// Emit the combined `Usage` ONCE (the input from `message_start` +
    /// the last `output_tokens` seen; a `None` side becomes `0` — an
    /// absent count is reported as zero, NOT dropped: a `Usage` with one
    /// real side is more useful than none).
    fn emit_usage(&mut self) {
        if self.usage_emitted {
            return;
        }
        if self.input_tokens.is_none() && self.last_output_tokens.is_none() {
            return;
        }
        self.usage_emitted = true;
        self.pending.push_back(ProviderEvent::Usage(Usage {
            input_tokens: self.input_tokens.unwrap_or(0),
            output_tokens: self.last_output_tokens.unwrap_or(0),
        }));
    }

    /// Emit the accumulated tool calls as complete `ToolCall` events
    /// (dropping any block that never arrived with a `name` — a
    /// `tool_use` block without a `name` cannot be dispatched).
    fn flush_tool_calls(&mut self) {
        for (_index, acc) in std::mem::take(&mut self.tool_acc).into_iter() {
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

impl Stream for AnthropicStream {
    type Item = ProviderEvent;

    fn poll_next(
        self: Pin<&mut AnthropicStream>,
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
                    // emit any pending `Usage`, flush the tool calls, and
                    // synthesize a `Done` if the provider never sent a
                    // `stop_reason`. (A tail that ends mid-codepoint is
                    // truncated on the wire — the lossy conversion is the
                    // only option there.)
                    if let Some(tail) = this.lines.finish() {
                        this.handle_line(&tail);
                    }
                    this.emit_usage();
                    if !this.finished {
                        this.flush_tool_calls();
                        this.pending
                            .push_back(ProviderEvent::Done(FinishReason::Error));
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The default `max_tokens` for the Anthropic wire (Anthropic REQUIRES
/// `max_tokens` on every request — a generous default when the caller did
/// not set one).
pub const ANTHROPIC_DEFAULT_MAX_TOKENS: u32 = 32768;

/// A client for the Anthropic Messages API (`POST {base_url}/messages`,
/// the `event:`-tagged SSE response parsed by [`AnthropicStream`]; ADR
/// 0024). The auth header is `x-api-key` (NOT `Authorization: Bearer` —
/// the Anthropic API's header), plus the required `anthropic-version`.
#[derive(Debug, Clone)]
pub struct AnthropicProvider {
    /// The API base (e.g. `https://api.anthropic.com/v1` — the client
    /// appends `/messages`).
    pub base_url: String,
    /// The `x-api-key` header value.
    pub api_key: String,
}

/// The Anthropic WIRE shape of a content block: `Text` →
/// `{ "type": "text", "text" }`; `Image` →
/// `{ "type": "image", "source": { "type": "base64", "media_type",
/// "data" } }` (the pi-shaped `data` is base64 WITHOUT a `data:` prefix —
/// sent VERBATIM, unlike the OpenAI data-URI composition in
/// [`MessageContent::to_wire`]).
fn anthropic_block_wire(b: &ContentBlock) -> Value {
    match b {
        ContentBlock::Text { text } => json!({ "type": "text", "text": text }),
        ContentBlock::Image { image } => json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": image.mime_type,
                "data": image.data,
            },
        }),
    }
}

/// The `content` of a `tool_result` block: the `Text` string verbatim;
/// `Blocks` → an array of the Anthropic block shapes (a `Text` block →
/// `{ type: "text", text }`, an `Image` block → the image shape above).
fn anthropic_tool_result_content(c: &MessageContent) -> Value {
    match c {
        MessageContent::Text(t) => Value::String(t.clone()),
        MessageContent::Blocks(blocks) => {
            Value::Array(blocks.iter().map(anthropic_block_wire).collect())
        }
    }
}

/// Merge `next` into `existing` (the consecutive same-role merge —
/// Anthropic requires strict user/assistant alternation): string+string →
/// joined with `"\n"`; array+array → concatenated; string+array → the
/// string as a `text` block prepended; array+string → the `text` block
/// appended.
fn merge_content(existing: &mut Value, next: &Value) {
    if let (Value::String(a), Value::String(b)) = (&mut *existing, next) {
        a.push('\n');
        a.push_str(b);
        return;
    }
    if existing.is_array() && next.is_array() {
        existing
            .as_array_mut()
            .unwrap()
            .extend(next.as_array().unwrap().iter().cloned());
        return;
    }
    // The mixed `Text`+`Blocks` shapes: the text becomes a `text` block
    // (PREPENDED when it is the first message, APPENDED when the second).
    if existing.is_string() && next.is_array() {
        let text = std::mem::take(existing);
        let mut merged = vec![json!({ "type": "text", "text": text })];
        merged.extend(next.as_array().unwrap().iter().cloned());
        *existing = Value::Array(merged);
        return;
    }
    if existing.is_array() && next.is_string() {
        existing
            .as_array_mut()
            .unwrap()
            .push(json!({ "type": "text", "text": next.as_str().unwrap() }));
    }
}

/// The Anthropic `messages` array: the non-system messages with
/// CONSECATIVE-SAME-ROLE MERGING (Anthropic requires strict
/// user/assistant alternation — adjacent same-role messages merge per
/// [`merge_content`]), a `tool` message as a `user` `tool_result` block
/// (a run of `tool` messages merges into ONE `user` message), and an
/// assistant `tool_calls` as `tool_use` blocks (the `input` is a JSON
/// **object** — the OpenAI wire's string form does not apply; an assistant
/// message with `tool_calls` and EMPTY `Text` content → `tool_use` blocks
/// ONLY, no empty `text` block).
fn anthropic_messages(req: &ModelRequest) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    for m in req.messages.iter().filter(|m| m.role != ChatRole::System) {
        if m.role == ChatRole::Tool {
            let block = json!({
                "type": "tool_result",
                "tool_use_id": m.tool_call_id.clone().unwrap_or_default(),
                "content": anthropic_tool_result_content(&m.content),
            });
            // A run of `tool` messages merges into the previous `user`
            // message (its content is a block array — a `tool_result` is
            // always a block).
            if let Some(last) = out.last_mut() {
                if last.get("role").and_then(|r| r.as_str()) == Some("user")
                    && last.get("content").is_some_and(|c| c.is_array())
                {
                    last["content"].as_array_mut().unwrap().push(block);
                    continue;
                }
            }
            out.push(json!({ "role": "user", "content": Value::Array(vec![block]) }));
            continue;
        }
        let role = if m.role == ChatRole::Assistant {
            "assistant"
        } else {
            "user"
        };
        let mut content = match &m.content {
            MessageContent::Text(t) => Value::String(t.clone()),
            MessageContent::Blocks(blocks) => {
                Value::Array(blocks.iter().map(anthropic_block_wire).collect())
            }
        };
        if let Some(calls) = &m.tool_calls {
            let tool_blocks: Vec<Value> = calls
                .iter()
                .map(|tc| {
                    json!({
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.name,
                        "input": tc.arguments,
                    })
                })
                .collect();
            if content.is_string() {
                // An EMPTY `Text` → `tool_use` blocks ONLY (no empty `text`
                // block); a non-empty `Text` → a `text` block followed by
                // the `tool_use` blocks.
                let text = std::mem::take(&mut content);
                let mut blocks = Vec::new();
                if text.as_str().is_some_and(|s| !s.is_empty()) {
                    blocks.push(json!({ "type": "text", "text": text }));
                }
                blocks.extend(tool_blocks);
                content = Value::Array(blocks);
            } else if let Some(arr) = content.as_array_mut() {
                arr.extend(tool_blocks);
            }
        }
        // The consecutive same-role merge (the previous message's role
        // must differ — `tool` runs already merged into `user`).
        if let Some(last) = out.last_mut() {
            if last.get("role").and_then(|r| r.as_str()) == Some(role) {
                merge_content(&mut last["content"], &content);
                continue;
            }
        }
        out.push(json!({ "role": role, "content": content }));
    }
    out
}

/// The Anthropic `thinking` mode for a `reasoning_effort` level (the
/// desktop's level vocabulary → a `budget_tokens` value; an unrecognized
/// non-empty level → the `adaptive` mode — no budget, the provider decides).
fn anthropic_thinking(level: &str) -> Value {
    let budget: u64 = match level {
        "low" => 4096,
        "medium" => 10000,
        "high" => 20000,
        "xhigh" => 40000,
        "max" => 128000,
        _ => return json!({ "type": "adaptive" }),
    };
    json!({ "type": "enabled", "budget_tokens": budget })
}

/// Build the Anthropic Messages request body from a [`ModelRequest`]
/// (`max_tokens` is ALWAYS present — Anthropic requires it; `system` is
/// present only when non-empty; `thinking` is present only when a
/// `reasoning_effort` is set). The message `content` uses the Anthropic
/// WIRE form (a plain string for `Text`, an array of Anthropic content
/// parts for `Blocks` — [`anthropic_messages`]).
fn anthropic_request_body(req: &ModelRequest) -> Value {
    // `system`: the `System` messages' text (`Text` verbatim; `Blocks` →
    // the `Text` block texts joined with `"\n\n"`), the messages joined
    // with `"\n\n"`.
    let system = req
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
        "messages": Value::Array(anthropic_messages(req)),
        "stream": req.options.stream,
    });
    if !system.is_empty() {
        body["system"] = json!(system);
    }
    if !req.tools.is_empty() {
        // Flat `{ name, description, input_schema }` — the OpenAI
        // `function` nesting + `parameters` key do NOT apply.
        body["tools"] = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.parameters,
                })
            })
            .collect::<Vec<_>>()
            .into();
    }
    if let Some(t) = req.options.temperature {
        body["temperature"] = json!(t);
    }
    // `max_tokens` is ALWAYS present (Anthropic requires it): the caller's
    // value or the default, clamped UP over the `thinking` budget (the
    // API requires `max_tokens > budget_tokens`).
    let mut max_tokens = req
        .options
        .max_tokens
        .unwrap_or(ANTHROPIC_DEFAULT_MAX_TOKENS);
    if let Some(level) = &req.options.reasoning_effort {
        let thinking = anthropic_thinking(level);
        // The API requires `max_tokens > budget_tokens` — clamp UP over
        // the budget (the `adaptive` mode has no budget — no clamp).
        if let Some(budget) = thinking.get("budget_tokens").and_then(|b| b.as_u64()) {
            max_tokens = max_tokens.max(budget as u32 + 1024);
        }
        body["thinking"] = thinking;
    }
    body["max_tokens"] = json!(max_tokens);
    body
}

#[async_trait]
impl Provider for AnthropicProvider {
    async fn complete(
        &self,
        req: &ModelRequest,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        // Normalize the base to include the `/v1` API prefix (ADR 0024 —
        // mirroring ZCode's adapter-boundary normalization: a gateway root
        // like `https://openrouter.ai/api` → `…/api/v1` before `/messages`
        // is appended; idempotent for a base that already ends in `/v1`).
        let url = format!("{}/messages", normalize_anthropic_base_url(&self.base_url));
        // The shared idle-based bounds (the `OpenAiCompatibleProvider`
        // values — `build_client`'s docs: a generous CONNECT bound + a
        // `read_timeout` (the time BETWEEN BYTES on the body)).
        let client = build_client(
            Duration::from_secs(30),     // a generous CONNECT bound
            Duration::from_secs(5 * 60), // 5 min of SILENCE = a stalled provider
        );
        // `x-api-key` is ALWAYS sent (a keyless Anthropic endpoint is a
        // misconfiguration, but sending an empty value is harmless and
        // keeps one code path) + the required `anthropic-version` (the
        // `User-Agent` comes from `build_client`).
        let mut builder = client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_API_VERSION);
        for (name, value) in litellm_headers(req.session_id.as_deref()) {
            builder = builder.header(name, value);
        }
        let resp = builder
            .json(&anthropic_request_body(req))
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
        Ok(Box::pin(AnthropicStream::new(raw)))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{chat_message, collect};
    use super::super::types::{ChatMessage, ModelOptions, ToolSpec};
    use super::*;
    use crate::agent::tools::ImageRef;

    /// Build an `AnthropicStream` over RAW byte chunks (the `&str`
    /// helper below cannot split a multi-byte codepoint mid-byte —
    /// `&str` is valid UTF-8; this one can).
    fn anthropic_stream_bytes(chunks: &[Vec<u8>]) -> AnthropicStream {
        let data: Vec<Vec<u8>> = chunks.to_vec();
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            futures_util::stream::iter(data.into_iter().map(Ok)).boxed();
        AnthropicStream::new(raw)
    }

    /// Build an `AnthropicStream` over canned raw chunks (no HTTP).
    fn anthropic_stream(chunks: &[&str]) -> AnthropicStream {
        // Owned data so the stream is `'static` (independent of `chunks`).
        let data: Vec<Vec<u8>> = chunks.iter().map(|c| c.as_bytes().to_vec()).collect();
        anthropic_stream_bytes(&data)
    }

    #[test]
    fn map_anthropic_stop_reason_mapping() {
        assert_eq!(map_anthropic_stop_reason("end_turn"), FinishReason::Stop);
        assert_eq!(
            map_anthropic_stop_reason("tool_use"),
            FinishReason::ToolCalls
        );
        assert_eq!(
            map_anthropic_stop_reason("max_tokens"),
            FinishReason::Length
        );
        // `stop_sequence` (a custom stop sequence hit) is a normal stop.
        assert_eq!(
            map_anthropic_stop_reason("stop_sequence"),
            FinishReason::Stop
        );
        // An unknown value is a normal stop (logged).
        assert_eq!(
            map_anthropic_stop_reason("something_new"),
            FinishReason::Stop
        );
    }

    #[test]
    fn map_anthropic_error_mapping() {
        fn err(kind: Option<&str>) -> ProviderError {
            let mut error = serde_json::Map::new();
            error.insert("message".to_string(), json!("boom"));
            if let Some(k) = kind {
                error.insert("type".to_string(), json!(k));
            }
            map_anthropic_error(&json!({ "type": "error", "error": error }))
        }
        // `overloaded_error` / `api_error` are transient.
        for kind in ["overloaded_error", "api_error"] {
            match err(Some(kind)) {
                ProviderError::Retryable(_) => {}
                e => panic!("{kind} is retryable, got {e:?}"),
            }
        }
        // `authentication_error` / `permission_error` are auth.
        for kind in ["authentication_error", "permission_error"] {
            match err(Some(kind)) {
                ProviderError::Auth(_) => {}
                e => panic!("{kind} is auth, got {e:?}"),
            }
        }
        // `invalid_request_error` / `not_found_error` are permanent.
        for kind in ["invalid_request_error", "not_found_error"] {
            match err(Some(kind)) {
                ProviderError::Fatal(_) => {}
                e => panic!("{kind} is fatal, got {e:?}"),
            }
        }
        // An UNKNOWN type is conservative — retryable.
        match err(Some("mystery_error")) {
            ProviderError::Retryable(_) => {}
            e => panic!("an unknown type is retryable, got {e:?}"),
        }
        // An absent / empty `type` is retryable, labeled `unknown`.
        for kind in [None, Some("")] {
            match err(kind) {
                ProviderError::Retryable(msg) => {
                    assert!(
                        msg.contains("unknown"),
                        "an absent/empty type is labeled `unknown` (got {msg:?})"
                    );
                }
                other => panic!("an absent/empty type is retryable, got {other:?}"),
            }
        }
    }

    #[test]
    fn normalize_anthropic_base_url_appends_v1_at_the_adapter_boundary() {
        // (ADR 0024 — mirroring ZCode's `normalizeAnthropicBaseURL`.)
        // A base that ALREADY ends in `/v1` is left as-is (idempotent).
        assert_eq!(
            normalize_anthropic_base_url("https://api.anthropic.com/v1"),
            "https://api.anthropic.com/v1"
        );
        // A trailing slash is dropped (ZCode sets the stripped pathname).
        assert_eq!(
            normalize_anthropic_base_url("https://api.anthropic.com/v1/"),
            "https://api.anthropic.com/v1"
        );
        // A GATEWAY ROOT that does not end in `/v1` gets it appended —
        // the OpenRouter case (the user's finding: `…/api` → `…/api/v1`).
        assert_eq!(
            normalize_anthropic_base_url("https://openrouter.ai/api"),
            "https://openrouter.ai/api/v1"
        );
        // A trailing slash on a gateway root is dropped + `/v1` appended.
        assert_eq!(
            normalize_anthropic_base_url("https://openrouter.ai/api/"),
            "https://openrouter.ai/api/v1"
        );
        // A BARE domain (no path) gets `/v1` (the official-URL discovery
        // win: `https://api.anthropic.com` → `…/v1`).
        assert_eq!(
            normalize_anthropic_base_url("https://api.anthropic.com"),
            "https://api.anthropic.com/v1"
        );
        // A deeper gateway root (Z.ai / BigModel / DeepSeek) — `/v1`
        // appended to the path (ZCode's exact behavior).
        assert_eq!(
            normalize_anthropic_base_url("https://api.z.ai/api/anthropic"),
            "https://api.z.ai/api/anthropic/v1"
        );
        assert_eq!(
            normalize_anthropic_base_url("https://open.bigmodel.cn/api/anthropic"),
            "https://open.bigmodel.cn/api/anthropic/v1"
        );
        assert_eq!(
            normalize_anthropic_base_url("https://api.deepseek.com/anthropic"),
            "https://api.deepseek.com/anthropic/v1"
        );
        // Case-insensitive: an uppercase `/V1` is recognized (left as-is).
        assert_eq!(
            normalize_anthropic_base_url("https://api.anthropic.com/V1"),
            "https://api.anthropic.com/V1"
        );
        // A query string is NOT corrupted: `/v1` is appended to the PATH,
        // not after the query (ZCode operates on `url.pathname`).
        assert_eq!(
            normalize_anthropic_base_url("https://openrouter.ai/api?x=1"),
            "https://openrouter.ai/api/v1?x=1"
        );
        // An unparseable base falls back to a plain suffix check (ZCode's
        // `catch` arm) — still appends `/v1`.
        assert_eq!(normalize_anthropic_base_url("not-a-url"), "not-a-url/v1");
        // Empty / whitespace → empty (no `/v1` appended to nothing).
        assert_eq!(normalize_anthropic_base_url(""), "");
        assert_eq!(normalize_anthropic_base_url("   "), "");
    }

    // ── the Anthropic Messages wire (`event:`-tagged SSE) ──────────────

    #[tokio::test]
    async fn anthropic_sse_text_and_finish_reason() {
        // The full happy path: one `event:` line preceding each `data:` line.
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n\
             event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"He\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"llo\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n\
             event: message_stop\n\
             data: {\"type\":\"message_stop\"}\n\n",
        ]))
        .await;
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
    async fn anthropic_sse_thinking_deltas() {
        // A `thinking` block, then a `text` block, then the `message_delta`.
        let events = collect(anthropic_stream(&[
            "event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"think\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
             event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":1}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ThinkingDelta("think".to_string()),
                ProviderEvent::TextDelta("done".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    #[tokio::test]
    async fn anthropic_sse_tool_use_block() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n\
             event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
             event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"read\",\"input\":{}}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"pat\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"h\\\":\\\"/x\\\"}\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":1}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n",
        ]))
        .await;
        // The `message_delta` handler runs `emit_usage()` BEFORE
        // `flush_tool_calls()` — the `Usage` PRECEDES the `ToolCall`
        // (matching the OpenAI wire's trailing-usage semantics).
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("tu_1".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{\"pat".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("h\":\"/x\"}".to_string()),
                }),
                ProviderEvent::Usage(Usage {
                    input_tokens: 10,
                    output_tokens: 7,
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "tu_1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]
        );
    }

    #[tokio::test]
    async fn anthropic_sse_multiple_tool_use_blocks() {
        let events = collect(anthropic_stream(&[
            "event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
             event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_0\",\"name\":\"read\",\"input\":{}}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"p\\\":\\\"x\\\"}\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":0}\n\n\
             event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"bash\",\"input\":{}}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"c\\\":\\\"y\\\"}\"}}\n\n\
             event: content_block_stop\n\
             data: {\"type\":\"content_block_stop\",\"index\":1}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n",
        ]))
        .await;
        // Two `tool_use` blocks → two `ToolCall`s in index order (`read`
        // then `bash`); NO `Usage` (no `message_start` / `usage` in the
        // fixture).
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 0,
                    id: Some("tu_0".to_string()),
                    name: Some("read".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 0,
                    id: None,
                    name: None,
                    arguments: Some("{\"p\":\"x\"}".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: Some("tu_1".to_string()),
                    name: Some("bash".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 1,
                    id: None,
                    name: None,
                    arguments: Some("{\"c\":\"y\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "tu_0".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "p": "x" }),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "tu_1".to_string(),
                    name: "bash".to_string(),
                    arguments: json!({ "c": "y" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]
        );
    }

    #[tokio::test]
    async fn anthropic_sse_error_event() {
        let events = collect(anthropic_stream(&[
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events.len(),
            1,
            "an `error` event is terminal (the stream ends)"
        );
        match &events[0] {
            ProviderEvent::Error(ProviderError::Retryable(msg)) => {
                assert!(
                    msg.contains("Overloaded"),
                    "the error carries the wire message (got {msg:?})"
                );
            }
            other => panic!("expected a Retryable error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn anthropic_sse_auth_error_event() {
        let events = collect(anthropic_stream(&[
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"Bad key\"}}\n\n",
        ]))
        .await;
        match &events[0] {
            ProviderEvent::Error(ProviderError::Auth(msg)) => {
                assert!(
                    msg.contains("Bad key"),
                    "the error carries the wire message (got {msg:?})"
                );
            }
            other => panic!("expected an Auth error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn anthropic_sse_stream_end_without_message_delta() {
        // `message_start` + one `text_delta` only (no `message_delta`).
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\"}}\n\n",
        ]))
        .await;
        // The `Usage` is emitted at stream end (the input is known — the
        // `0` output is the documented absent-side rule) and a synthetic
        // `Done(Error)` closes the turn.
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("x".to_string()),
                ProviderEvent::Usage(Usage {
                    input_tokens: 10,
                    output_tokens: 0,
                }),
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    #[tokio::test]
    async fn anthropic_sse_usage_emitted_once_from_the_final_message_delta() {
        // An intermediate `message_delta` (NO `stop_reason`) does NOT emit
        // the `Usage` — exactly ONE, from the LAST value seen.
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(Usage {
                    input_tokens: 10,
                    output_tokens: 7,
                }),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// (finding 9, the Anthropic wire) The raw bytes may split a
    /// MULTI-BYTE codepoint between chunks (CJK is 3 bytes, emoji is 4).
    /// The buffer must hold RAW bytes and convert only COMPLETE lines: the
    /// reassembled text is byte-identical at every possible split point.
    #[tokio::test]
    async fn anthropic_multibyte_chars_survive_chunk_boundaries() {
        // CJK (3-byte) + emoji (4-byte) content in an Anthropic
        // `content_block_delta` `text_delta` line.
        let text = "你好🌍世界🚀";
        let payload = format!(
            "event: content_block_delta\n\
             data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":\"{text}\"}}}}\n\n"
        );
        let bytes = payload.as_bytes();
        // Split the raw bytes at EVERY offset — including the middle of a
        // 3-byte and a 4-byte codepoint. Each reassembly must be
        // byte-identical (no U+FFFD).
        for split_at in 1..bytes.len() {
            let events = collect(anthropic_stream_bytes(&[
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
        let events = collect(anthropic_stream_bytes(&[
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

    // ── the Anthropic provider (ADR 0024) ──────────────────────────────

    /// The `AnthropicProvider` is object-safe (a `Box<dyn Provider>`
    /// compiles — the `SessionManager`'s factory dispatches on it).
    #[test]
    fn anthropic_provider_is_object_safe() {
        let provider: Box<dyn Provider> = Box::new(AnthropicProvider {
            base_url: "https://api.anthropic.com/v1".to_string(),
            api_key: "k".to_string(),
        });
        let _ = provider;
    }

    #[test]
    fn anthropic_request_body_maps_the_request() {
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
                    MessageContent::Text(String::new()),
                    None,
                    Some(vec![ToolCall {
                        id: "tu_1".to_string(),
                        name: "read".to_string(),
                        arguments: json!({ "path": "/x" }),
                    }]),
                ),
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text("out".to_string()),
                    Some("tu_1"),
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
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        let body = anthropic_request_body(&req);
        assert_eq!(body["model"], "m");
        assert_eq!(body["system"], "sys");
        // The non-system messages: `user`, `assistant`, `user` (the `tool`
        // message became a `user` `tool_result`).
        let messages = body["messages"].as_array().expect("messages are present");
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "hi");
        assert_eq!(messages[1]["role"], "assistant");
        // The assistant's `tool_calls` → `tool_use` blocks: the `input` is
        // a JSON **object** (NOT the OpenAI wire's JSON string), and an
        // EMPTY `Text` content → `tool_use` blocks ONLY.
        let content = messages[1]["content"]
            .as_array()
            .expect("assistant content is an array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "tool_use");
        assert_eq!(content[0]["id"], "tu_1");
        assert_eq!(content[0]["name"], "read");
        assert_eq!(content[0]["input"], json!({ "path": "/x" }));
        assert!(
            content[0]["input"].is_object(),
            "the `input` is an OBJECT, not a string"
        );
        // The `tool` message → a `user` `tool_result` block.
        assert_eq!(messages[2]["role"], "user");
        let tool_result = messages[2]["content"]
            .as_array()
            .expect("tool_result content is an array")
            .iter()
            .next()
            .expect("one tool_result block");
        assert_eq!(tool_result["type"], "tool_result");
        assert_eq!(tool_result["tool_use_id"], "tu_1");
        assert_eq!(tool_result["content"], "out");
        // `tools`: flat `{ name, description, input_schema }` — NO
        // `function` nesting, NO `type` field (unlike the OpenAI wire).
        assert_eq!(
            body["tools"][0],
            json!({
                "name": "read",
                "description": "Read a file",
                "input_schema": { "type": "object" },
            })
        );
        assert!(body["tools"][0].get("function").is_none());
        assert!(body["tools"][0].get("type").is_none());
        assert_eq!(body["max_tokens"], 64);
        let temperature = body["temperature"]
            .as_f64()
            .expect("temperature is a number");
        assert!(
            (temperature - 0.2).abs() < 1e-6,
            "temperature is passed through (got {temperature})"
        );
        assert_eq!(body["stream"], true);
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn anthropic_request_body_consecutive_same_role_merge() {
        fn req(messages: Vec<ChatMessage>) -> Value {
            let r = ModelRequest {
                model: "m".to_string(),
                messages,
                tools: vec![],
                options: ModelOptions {
                    temperature: None,
                    max_tokens: None,
                    reasoning_effort: None,
                    stream: true,
                },
                session_id: None,
            };
            anthropic_request_body(&r)
        }
        // Two consecutive `User` `Text` messages → ONE `user` message with
        // the texts joined.
        let body = req(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("a".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::User,
                MessageContent::Text("b".to_string()),
                None,
                None,
            ),
        ]);
        let messages = body["messages"].as_array().expect("messages are present");
        assert_eq!(messages.len(), 1, "the same-role pair merges into one");
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[0]["content"], "a\nb");

        // A `User` `Text` then a `User` `Blocks` → ONE message with a
        // 2-block array (the text as a `text` block prepended).
        let body = req(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("a".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::User,
                MessageContent::Blocks(vec![ContentBlock::Text {
                    text: "b".to_string(),
                }]),
                None,
                None,
            ),
        ]);
        let messages = body["messages"].as_array().expect("messages are present");
        assert_eq!(messages.len(), 1, "the Text+Blocks pair merges into one");
        assert_eq!(
            messages[0]["content"],
            json!([
                { "type": "text", "text": "a" },
                { "type": "text", "text": "b" },
            ])
        );
    }

    #[test]
    fn anthropic_request_body_multiple_tool_results_merge() {
        // A run of `tool` messages merges into ONE `user` message with
        // multiple `tool_result` blocks.
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text("r1".to_string()),
                    Some("t1"),
                    None,
                ),
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text("r2".to_string()),
                    Some("t2"),
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
        let body = anthropic_request_body(&req);
        let messages = body["messages"].as_array().expect("messages are present");
        assert_eq!(
            messages.len(),
            1,
            "the tool run merges into ONE user message"
        );
        assert_eq!(messages[0]["role"], "user");
        let content = messages[0]["content"]
            .as_array()
            .expect("the merged content is a block array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "tool_result");
        assert_eq!(content[0]["tool_use_id"], "t1");
        assert_eq!(content[0]["content"], "r1");
        assert_eq!(content[1]["type"], "tool_result");
        assert_eq!(content[1]["tool_use_id"], "t2");
        assert_eq!(content[1]["content"], "r2");
    }

    #[test]
    fn anthropic_request_body_image_block_wire_shape() {
        // The Anthropic image wire shape: `source.data` is the base64
        // VERBATIM (NO `data:` prefix — unlike the OpenAI data-URI
        // composition in `MessageContent::to_wire`).
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![chat_message(
                ChatRole::User,
                MessageContent::Blocks(vec![ContentBlock::Image {
                    image: ImageRef {
                        data: "BASE64DATA".to_string(),
                        mime_type: "image/png".to_string(),
                    },
                }]),
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
        let body = anthropic_request_body(&req);
        let content = body["messages"][0]["content"]
            .as_array()
            .expect("the content is a block array");
        assert_eq!(
            content[0],
            json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/png",
                    "data": "BASE64DATA",
                },
            })
        );
    }

    #[test]
    fn anthropic_request_body_max_tokens_default_and_clamp() {
        fn req(max_tokens: Option<u32>, reasoning_effort: Option<&str>) -> Value {
            let r = ModelRequest {
                model: "m".to_string(),
                messages: vec![],
                tools: vec![],
                options: ModelOptions {
                    temperature: None,
                    max_tokens,
                    reasoning_effort: reasoning_effort.map(|s| s.to_string()),
                    stream: true,
                },
                session_id: None,
            };
            anthropic_request_body(&r)
        }
        // `max_tokens: None` + a `max` budget (128000) → clamped to
        // `budget + 1024` (Anthropic requires `max_tokens > budget_tokens`).
        assert_eq!(req(None, Some("max"))["max_tokens"], 129024);
        // `max_tokens: Some(64)` + a `low` budget (4096) → clamped UP to
        // `4096 + 1024`.
        assert_eq!(req(Some(64), Some("low"))["max_tokens"], 5120);
        // NO `reasoning_effort` + NO `max_tokens` → the default, NO
        // `thinking` field.
        let body = req(None, None);
        assert_eq!(body["max_tokens"], ANTHROPIC_DEFAULT_MAX_TOKENS);
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn anthropic_request_body_thinking_map() {
        fn req(level: Option<&str>) -> Value {
            let r = ModelRequest {
                model: "m".to_string(),
                messages: vec![],
                tools: vec![],
                options: ModelOptions {
                    temperature: None,
                    max_tokens: None,
                    reasoning_effort: level.map(|s| s.to_string()),
                    stream: true,
                },
                session_id: None,
            };
            anthropic_request_body(&r)
        }
        // The desktop's level vocabulary → the `budget_tokens` values.
        assert_eq!(
            req(Some("low"))["thinking"],
            json!({ "type": "enabled", "budget_tokens": 4096 })
        );
        assert_eq!(
            req(Some("medium"))["thinking"],
            json!({ "type": "enabled", "budget_tokens": 10000 })
        );
        assert_eq!(
            req(Some("high"))["thinking"],
            json!({ "type": "enabled", "budget_tokens": 20000 })
        );
        assert_eq!(
            req(Some("xhigh"))["thinking"],
            json!({ "type": "enabled", "budget_tokens": 40000 })
        );
        assert_eq!(
            req(Some("max"))["thinking"],
            json!({ "type": "enabled", "budget_tokens": 128000 })
        );
        // An unrecognized non-empty level → the `adaptive` mode (no budget).
        assert_eq!(
            req(Some("weird"))["thinking"],
            json!({ "type": "adaptive" })
        );
        // `None` → the field is ABSENT.
        assert!(req(None).get("thinking").is_none());
    }
}

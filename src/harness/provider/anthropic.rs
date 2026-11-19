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

use super::openai::{
    content_is_effectively_empty, emit_harness_line, parse_arguments,
    tool_call_dropped_with_no_name, tool_calls_dropped_at_stream_error, LineAssembler,
    TOOL_EMPTY_OUTPUT_PLACEHOLDER,
};
use super::transport::{build_client, litellm_headers, map_response, ANTHROPIC_API_VERSION};
use super::types::{
    cost_usd_from_usage, parse_error_chunk, ChatRole, FinishReason, MessageContent, ModelRequest,
    Provider, ProviderError, ProviderEvent, ToolCall, ToolCallDelta, Usage,
};
use crate::harness::WireApi;
use crate::tools::ContentBlock;

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

/// Normalize a wire base URL per the model's API (T3b / #131, ADR 0026
/// amendment; interface §9 — do NOT overload `normalize_anthropic_base_url`:
/// that helper can only ever PRODUCE a `/v1`-suffixed path, it never strips,
/// so it cannot express the Messages rule).
///
/// - [`WireApi::AnthropicMessages`] → the ROOT: a trailing `/v1` is stripped
///   (the Anthropic client appends its OWN `/v1/messages`, so a base of
///   `{root}/v1` would produce `{root}/v1/v1/messages`);
/// - [`WireApi::OpenAiCompletions`] / [`WireApi::OpenAiResponses`] →
///   `{root}/v1` (appended idempotently — the shape
///   `normalize_anthropic_base_url` produces);
/// - [`WireApi::LiteLLM`] → `{root}/v1` (a discovery MODE, not a wire — its
///   completion wire IS `openai-completions`, ADR 0026).
///
/// Operates on the URL PATH (a query string is never corrupted — the
/// `url::Url` surgery the sibling helper uses); an unparseable base falls back
/// to a plain suffix strip/append on the trimmed string.
///
/// IDEMPOTENCY CONTRACT (part of this helper's public spec): applying the
/// helper twice yields the same URL as applying it once, for EVERY wire —
/// `n(n(u)) == n(u)`. That is what lets the helper and its `build_provider`
/// call site land in either order (the second pass is always a no-op), and the
/// tests pin it directly (the `idempotent` cases) as well as at the call site.
pub fn normalize_wire_base_url(base_url: &str, api: WireApi) -> String {
    let trimmed = base_url.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    // The Messages rule strips, every other wire appends.
    let strip = api == WireApi::AnthropicMessages;
    if let Ok(mut url) = url::Url::parse(trimmed) {
        // An OWNED `String` (NOT a `&str` borrowing from `url` — `set_path`
        // below mutates `url`, so the borrow must not outlive it).
        let pathname = url.path().trim_end_matches('/').to_string();
        let has_v1 = pathname.to_lowercase().ends_with("/v1");
        if strip {
            // Strip AT MOST one trailing `/v1` (never more — `n(n(u)) == n(u)`).
            let stripped = if has_v1 {
                pathname[..pathname.len() - 3].to_string()
            } else {
                pathname
            };
            url.set_path(if stripped.is_empty() { "/" } else { &stripped });
            return match stripped.as_str() {
                // A root whose ENTIRE path was `/v1` becomes the BARE root:
                // the `url::Url` display of an empty path is `/`
                // (`https://x/`, and `https://x/?q=1` with a query), so the
                // authority slash is removed by hand — the helper's contract
                // is the root (`https://x`, `https://x?q=1`).
                "" => strip_authority_slash(url.to_string()),
                _ => url.to_string(),
            };
        }
        if has_v1 {
            url.set_path(&pathname); // drop any trailing slash
        } else {
            url.set_path(&format!("{pathname}/v1"));
        }
        return url.to_string();
    }
    // Fallback (unparseable): a plain suffix check on the trimmed string.
    let without_trailing = trimmed.trim_end_matches('/');
    let has_v1 = without_trailing.to_lowercase().ends_with("/v1");
    if strip {
        if has_v1 {
            // `x/v1` → `x`; a bare `/v1` → the empty root.
            without_trailing[..without_trailing.len() - 3].to_string()
        } else {
            without_trailing.to_string()
        }
    } else if has_v1 {
        without_trailing.to_string()
    } else {
        format!("{without_trailing}/v1")
    }
}

/// Remove the authority slash the `url::Url` display inserts when the path is
/// empty/root: `https://x/` → `https://x`, `https://x/?q=1` → `https://x?q=1`.
/// Only the FIRST `/` after the scheme separator is touched, and only when
/// nothing but a query / fragment follows it (a real path segment survives).
fn strip_authority_slash(url: String) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        if let Some(slash) = url[after..].find('/') {
            let idx = after + slash;
            let rest = &url[idx + 1..];
            if rest.is_empty() || rest.starts_with('?') || rest.starts_with('#') {
                let mut out = String::with_capacity(url.len());
                out.push_str(&url[..idx]);
                out.push_str(rest);
                return out;
            }
        }
    }
    url
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
    /// The `message_start` WHOLE-PROMPT input count — the wire's
    /// `input_tokens` PLUS both cache fields (ADR 0046: on this wire the
    /// cached prefix is NOT in `input_tokens`). The `usage` there carries
    /// the INPUT count only; the output comes from `message_delta`.
    /// `None` when the wire reported no input-side count at all.
    input_tokens: Option<u32>,
    /// The last `message_delta` `usage.output_tokens` seen (cumulative on
    /// the wire — the FINAL `message_delta` carries the full count).
    last_output_tokens: Option<u32>,
    /// The last usage object's cost (USD) seen — read at BOTH places this
    /// parser reads a usage object (`message_start`'s input side and
    /// `message_delta`'s output side), LAST-NON-`None`-WINS: a later usage
    /// object that PRICES the call supersedes the earlier figure (the
    /// final `message_delta` carries the final number, as
    /// `last_output_tokens` does), and one that omits the cost does NOT
    /// erase an earlier figure — an absent field is neither a zero nor a
    /// retraction. `None` means the provider reported NO money, which is
    /// not zero (D5).
    cost_usd: Option<f64>,
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
            cost_usd: None,
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
                // ADR 0046: `Usage.input_tokens` is the WHOLE prompt the
                // model was shown, cached prefix included — and on THIS
                // wire the cached prefix is NOT in `input_tokens`: it
                // lives in `cache_read_input_tokens` (read) and
                // `cache_creation_input_tokens` (written), which are
                // DISJOINT parts of the prompt here. (On the OpenAI-shaped
                // wires the cached count is a SUBSET of the input total
                // and must never be added — pinned there.) So the sum is
                // computed HERE, at the seam that knows the wire's
                // vocabulary, leaving `Usage` two fields and the Anchor
                // formula (ADR 0028) untouched.
                //
                // Stored ONLY when at least one of the three counts is
                // present: an absent `usage` must leave `input_tokens`
                // `None`, or `emit_usage`'s guard could not tell "the
                // stream reported nothing" from a genuine zero and would
                // emit a spurious `Usage(0, n)`.
                //
                // No early `return` in this arm: `handle_chunk` clears the
                // `event:` tag at its END, so bailing out here would leave
                // a stale tag to mis-dispatch the NEXT tagless `data:`
                // line (a proxy may strip the tags).
                if let Some(u) = v.get("message").and_then(|m| m.get("usage")) {
                    // Per-field clamp stays `u32::MAX` (a silent `as u32`
                    // would TRUNCATE); the sum SATURATES rather than wrapping.
                    let mut whole_prompt: Option<u32> = None;
                    for key in [
                        "input_tokens",
                        "cache_read_input_tokens",
                        "cache_creation_input_tokens",
                    ] {
                        let Some(n) = u
                            .get(key)
                            .and_then(|t| t.as_u64())
                            .map(|t| u32::try_from(t).unwrap_or(u32::MAX))
                        else {
                            continue;
                        };
                        whole_prompt = Some(whole_prompt.unwrap_or(0).saturating_add(n));
                    }
                    self.input_tokens = whole_prompt;
                    // The same usage object may carry the money (D4,
                    // AMENDED: the cost rides the BODY's usage object, not
                    // a header). Read here too — which usage object a
                    // gateway prices is deployment-dependent, so both
                    // places this parser reads a usage object are read.
                    self.record_cost(cost_usd_from_usage(u));
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
                // The same usage object may carry the money (D4, AMENDED).
                // The FINAL `message_delta` is where a gateway that prices
                // after streaming reports, so this is the Anthropic
                // analogue of the OpenAI-shaped final usage chunk.
                if let Some(u) = v.get("usage") {
                    self.record_cost(cost_usd_from_usage(u));
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
                // Classified by `parse_error_chunk` (types.rs) — the ONE
                // classifier of the three wires. The `.expect` is the
                // load-bearing guarantee that a `None` can never silently
                // drop an Anthropic error: this arm only runs for an
                // `error` EVENT, and an `error` event always carries an
                // `error` object, so the classifier is always `Some` here
                // (a `None` would be a wire violation and must panic loudly
                // rather than fall through to the non-error path).
                //
                // THE FLUSH-SKIP, reported at the latch — BEFORE `exhausted` is
                // written, so the line is out even if the `.expect` below panics.
                // `exhausted` here makes `poll_next`'s early `if this.exhausted`
                // return `Ready(None)` without ever reaching the
                // `Poll::Ready(None)` block, and the stream-end flush sits inside
                // that block behind `if !this.finished` — so a call in flight at
                // this moment is never flushed. The line is gated on a NON-EMPTY
                // `tool_acc` because that is the only condition under which a real
                // call is being lost, and it is what keeps a healthy turn silent
                // (an in-stream error with no call in flight is an ordinary failure
                // the loop retries, not a lost call).
                if !self.tool_acc.is_empty() {
                    emit_harness_line(&tool_calls_dropped_at_stream_error(
                        "anthropic",
                        "in-stream-error",
                        self.tool_acc.len(),
                        self.tool_acc.values().any(|acc| acc.name.is_some()),
                    ));
                }
                self.exhausted = true;
                self.pending
                    .push_back(ProviderEvent::Error(parse_error_chunk(v).expect(
                        "an anthropic `error` event always carries an `error` object",
                    )));
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

    /// Record a cost seen on a usage object: LAST-NON-`None`-WINS. A
    /// `None` (absent, malformed or negative — the reader's verdict) never
    /// overwrites a figure an earlier usage object carried, because an
    /// absent field is not a retraction of the money.
    fn record_cost(&mut self, cost: Option<f64>) {
        if cost.is_some() {
            self.cost_usd = cost;
        }
    }

    /// Emit the combined `Usage` ONCE (the input from `message_start` —
    /// already the WHOLE prompt, cached prefix included, ADR 0046 — plus
    /// the last `output_tokens` seen; a `None` side becomes `0` — an
    /// absent count is reported as zero, NOT dropped: a `Usage` with one
    /// real side is more useful than none).
    ///
    /// The guard below is deliberately **counts-only**: it must NOT gain a
    /// `|| self.cost_usd.is_some()` term. A cost with no token counts on
    /// either side would push `Usage(0, 0, Some(money))`, and
    /// `Compactor::record_usage` would write a ZERO **Anchor** — a
    /// compaction regression bought for a figure (pinned by
    /// `anthropic_sse_cost_with_no_token_counts_emits_no_usage_event`).
    /// The cost is one more argument on this ONE push, so the
    /// `usage_emitted` exactly-once property is unchanged by it.
    fn emit_usage(&mut self) {
        if self.usage_emitted {
            return;
        }
        if self.input_tokens.is_none() && self.last_output_tokens.is_none() {
            return;
        }
        self.usage_emitted = true;
        self.pending.push_back(ProviderEvent::Usage(
            Usage {
                input_tokens: self.input_tokens.unwrap_or(0),
                output_tokens: self.last_output_tokens.unwrap_or(0),
            },
            self.cost_usd,
        ));
    }

    /// Emit the accumulated tool calls as complete `ToolCall` events
    /// (dropping any block that never arrived with a `name` — a
    /// `tool_use` block without a `name` cannot be dispatched).
    ///
    /// The drop is LOUD. It was a bare `continue` — the silent-discard shape this
    /// whole change exists to close: the model made a call, its `input` fragments
    /// streamed, and the turn settled as a success having dispatched nothing, with
    /// no line anywhere. This is the site the settle-site line's "where the call
    /// went is the provider parser's account" resolves to.
    fn flush_tool_calls(&mut self) {
        for (index, acc) in std::mem::take(&mut self.tool_acc).into_iter() {
            let Some(name) = acc.name else {
                emit_harness_line(&tool_call_dropped_with_no_name(
                    "anthropic",
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
                    //
                    // THE FLUSH-SKIP, reported at the latch — see the in-stream
                    // `error` arm for why `exhausted` makes the stream-end flush
                    // unreachable, and why the gate is a non-empty `tool_acc`.
                    if !this.tool_acc.is_empty() {
                        emit_harness_line(&tool_calls_dropped_at_stream_error(
                            "anthropic",
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

/// The hard CEILING on `max_tokens` for this wire — requested
/// UNCONDITIONALLY, never gated on the Claude-family model prefix.
///
/// Anthropic's own synchronous ceiling is 128,000 on current models and
/// 64,000 on the 4.5 generation, and an over-cap value is a **400, NOT a
/// silent clamp** — so the conservative end of that range is the only end
/// that cannot 400. A gateway with a LOWER cap silently clamps down rather
/// than rejecting, so asking for 64,000 is safe behind one. A family-prefix
/// gate would silently disable the ceiling for LiteLLM-routed Claude traffic
/// (issue #196's whole finding: the model id survives LiteLLM routing but the
/// routing changes what the far end will accept, so the prefix identifies the
/// family and NOT the ceiling that applies to it).
///
/// Asking for the full 64,000 on EVERY turn — the no-cap arm is what every
/// loop turn takes, and titling reaches it too whenever a naming reasoning
/// level is configured — costs nothing: Anthropic documents that `max_tokens`
/// does NOT factor into the OTPM rate-limit calculation (OTPM is evaluated in
/// real time on the tokens actually generated), and output is billed on the
/// tokens generated, not on the cap requested. The ask therefore spends none
/// of the bucket a smaller cap would have reserved. The contrast is the trap:
/// on the OpenAI side the same field is NOT free — its rate limit is the max
/// of `max_tokens` and the estimated prompt size, and a LiteLLM gateway doing
/// budget reservation estimates from the value — which is why the OpenAI
/// builders (`openai.rs`, `responses.rs`) still OMIT the field when it is
/// `None`. This ceiling is Anthropic-wire-only for exactly that reason.
const ANTHROPIC_OUTPUT_CEILING_TOKENS: u32 = 64_000;

/// The answer's FLOOR: the minimum output left for visible text PLUS the
/// whole `tool_use` block once a `thinking` budget has been clamped.
///
/// The number this replaces is 1,024 — identical in goose and pi — and our
/// tokenized traffic exceeds it outright: 62% of final reports and 65% of
/// writes are longer than 1,024 tokens, so that reservation truncates the
/// deliverable at the point where the model is emitting it. 16,384 is the
/// SMALLEST power of two clearing both measurements. It is affordable: at
/// [`ANTHROPIC_OUTPUT_CEILING_TOKENS`] it still leaves 47,616 tokens for
/// thinking, which is MORE than the `xhigh` level (40,000) spends — so
/// `low`/`medium`/`high`/`xhigh` keep their budgets untouched and only `max`
/// (128,000) shrinks, which is precisely the level whose request is a 400
/// today.
///
/// PROVENANCE of those two percentages, carried here because this comment is
/// the ONLY surviving copy of them: they come from a live-tokenizer
/// measurement over real database traffic that is NOT recorded in this repo
/// (the plan that chose 16,384 flagged it as unverified and its source report
/// had no tokenizer installed, deriving its own figures at bytes ÷ 3.7 ±20%).
/// Treat them as REPORTED, not as reproducible here. The load-bearing
/// decision is the FLOOR — 16,384 is the smallest power of two clearing both
/// populations while still leaving more room than `xhigh` spends — NOT the
/// two percentages, which only justify the size. If the measurement is ever
/// re-taken and disagrees, `MIN_ANSWER_TOKENS` is the number to re-open, not
/// the SHAPE of the ceiling/clamp design above.
const MIN_ANSWER_TOKENS: u32 = 16_384;

/// Anthropic's own minimum legal `budget_tokens` (below it the request is a
/// 400).
///
/// This is the **DROP THRESHOLD**, not a floor on the budget: when the room
/// the ceiling leaves after [`MIN_ANSWER_TOKENS`] falls under it there is NO
/// legal budget at all, and `thinking` is omitted entirely rather than
/// emitting a `budget_tokens` the API would reject. It never raises a budget
/// toward this value — see [`clamp_thinking_budget`].
const MIN_THINKING_BUDGET_TOKENS: u32 = 1_024;

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
    /// The provider's expanded + validated CUSTOM headers (R1, issue #129) —
    /// applied by `build_client` as `default_headers` on every completion
    /// request. `None` = no custom headers (the byte-identical default).
    pub custom_headers: Option<std::collections::BTreeMap<String, String>>,
}

/// The text sent for an effectively-empty `Text` or all-empty `Blocks`
/// user/assistant message with no `tool_calls` to carry (where no usable
/// content is left once empty text blocks are dropped). Anthropic rejects
/// an empty string, an empty `content` array, and empty text blocks, and
/// dropping the whole message would break the strict user/assistant
/// alternation, so a visible stand-in is sent instead.
pub(crate) const EMPTY_USER_MESSAGE_PLACEHOLDER: &str = "(empty)";

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

/// The Anthropic wire blocks for a `Blocks` list with every effectively-empty
/// `Text` block DROPPED (Anthropic rejects empty text blocks). `Image` blocks
/// are always kept. May return an empty `Vec` — the callers decide what to do then.
fn anthropic_nonempty_blocks(blocks: &[ContentBlock]) -> Vec<Value> {
    blocks
        .iter()
        .filter(|b| match b {
            ContentBlock::Text { text } => !content_is_effectively_empty(text),
            ContentBlock::Image { .. } => true,
        })
        .map(anthropic_block_wire)
        .collect()
}

/// The `content` of a `tool_result` block: a `Text` string is sent verbatim
/// unless effectively empty, in which case it is replaced by
/// [`TOOL_EMPTY_OUTPUT_PLACEHOLDER`] (`"(no output)"`). A `Blocks` list is an
/// array of the Anthropic block shapes with effectively-empty text blocks
/// dropped (Anthropic rejects empty text blocks). If nothing is left, it is
/// the [`TOOL_EMPTY_OUTPUT_PLACEHOLDER`] string. An image-only result stays
/// an array containing the image.
fn anthropic_tool_result_content(c: &MessageContent) -> Value {
    match c {
        MessageContent::Text(t) => {
            if content_is_effectively_empty(t) {
                Value::String(TOOL_EMPTY_OUTPUT_PLACEHOLDER.to_string())
            } else {
                Value::String(t.clone())
            }
        }
        MessageContent::Blocks(blocks) => {
            let wire = anthropic_nonempty_blocks(blocks);
            if wire.is_empty() {
                Value::String(TOOL_EMPTY_OUTPUT_PLACEHOLDER.to_string())
            } else {
                Value::Array(wire)
            }
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
/// ONLY, no empty `text` block). Effectively-empty text blocks in `Blocks`
/// content are dropped; an effectively-empty `Text` or all-empty `Blocks`
/// message with no `tool_calls` is sent as `"(empty)"` (string or single
/// text block).
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
            MessageContent::Text(s) => {
                if content_is_effectively_empty(s)
                    && m.tool_calls.as_ref().is_none_or(|c| c.is_empty())
                {
                    Value::String(EMPTY_USER_MESSAGE_PLACEHOLDER.to_string())
                } else {
                    Value::String(s.clone())
                }
            }
            MessageContent::Blocks(blocks) => {
                let wire = anthropic_nonempty_blocks(blocks);
                if wire.is_empty() && m.tool_calls.as_ref().is_none_or(|c| c.is_empty()) {
                    Value::Array(vec![
                        json!({ "type": "text", "text": EMPTY_USER_MESSAGE_PLACEHOLDER }),
                    ])
                } else {
                    // Non-empty, OR empty but `tool_calls` follow: the `tool_use`
                    // blocks are appended below and carry the message.
                    Value::Array(wire)
                }
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
                // An effectively-empty `Text` (empty, whitespace-only, or the LiteLLM banner)
                // → `tool_use` blocks ONLY (no empty `text` block); a non-empty `Text` → a `text`
                // block followed by the `tool_use` blocks.
                let text = std::mem::take(&mut content);
                let mut blocks = Vec::new();
                if text
                    .as_str()
                    .is_some_and(|s| !content_is_effectively_empty(s))
                {
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

/// Fit a thinking budget under the output ceiling, leaving
/// [`MIN_ANSWER_TOKENS`] for the answer.
///
/// `Some(b)` = use this budget (possibly shrunk). `None` = emit NO `thinking`
/// key: no legal budget fits, because Anthropic's minimum will not fit in the
/// room the floor leaves.
///
/// ONLY EVER REDUCES — it never raises a budget toward the minimum. Three
/// outcomes: SHRINK (at the current 64,000 ceiling, `128000 → 47616` — the
/// `max` level is shrunk, not dropped), PASS THROUGH (`4096 → Some(4096)`;
/// exactly at room is inclusive, so `47616 → Some(47616)` and `47617` clamps
/// to `Some(47616)`), and DROP — which happens **only** when the room itself
/// is under [`MIN_THINKING_BUDGET_TOKENS`].
///
/// Deliberately NOT `room.max(MIN_THINKING_BUDGET_TOKENS)`: that looks like
/// the defensive move and it is dead code with a trap in it. Applied first it
/// lifts `room` to exactly 1,024 at any ceiling under 17,408, which makes the
/// `room < MIN_THINKING_BUDGET_TOKENS` test unreachable forever — so a
/// low-ceiling model would emit a 1,024-token budget where the design says
/// drop `thinking`, and at a ceiling of 1,024 or lower it would emit
/// `budget_tokens >= max_tokens`, a 400. [`MIN_THINKING_BUDGET_TOKENS`] has
/// exactly one job: the drop threshold.
fn clamp_thinking_budget(budget: u32) -> Option<u32> {
    let room = ANTHROPIC_OUTPUT_CEILING_TOKENS.saturating_sub(MIN_ANSWER_TOKENS);
    if room < MIN_THINKING_BUDGET_TOKENS {
        None
    } else {
        Some(budget.min(room))
    }
}

/// The (`max_tokens`, `thinking`) pair for a request.
///
/// `max_tokens` is the CEILING and the budget is what moves to fit under it —
/// the inverse of the `max_tokens.max(budget + 1024)` clamp this replaced.
/// `thinking` in is the [`anthropic_thinking`] output (or `None` for no
/// level); `thinking` out is what goes on the wire: passed through when it
/// carries no budget (the `adaptive` mode), rebuilt with the clamped budget
/// when it does, or dropped when the clamp returns `None`.
///
/// The clamp runs BEFORE the arms split, and it feeds BOTH arms — the
/// caller-cap arm does not leave the budget alone. The naive transcription of
/// the upstream reference shape (`min(caller_cap + budget, ceiling)` with the
/// RAW budget) is illegal in 23 of the 144 reachable inputs, because our
/// callers' caps — `Some(100)` from the desktop's titling request, and
/// `min(0.8 × reserve_tokens, 8_192).max(1)` from the summariser, i.e.
/// anything in `1..=8_192` including `Some(1)` at a zero reserve — sit far
/// below the level table's budgets, and Anthropic requires **strict**
/// `max_tokens > budget_tokens` (`cap = Some(0)` with any positive budget
/// gives `max_tokens == budget_tokens`, a 400 — and `Some(1)` is a value a
/// real caller produces TODAY, so the guard below it is not paranoia). The
/// sum therefore adds the CLAMPED budget, and `c.max(1)` is the same class of
/// guard one size lower (a caller sending `Some(0)` with a 1,024 budget would
/// otherwise tie) — `compact.rs:617` already writes `cap.max(1)` for this
/// reason, but the wire builder cannot rely on every caller doing so, so it
/// applies its own.
///
/// The DROP arm (`(Some(_), None)`) fires only when the ceiling is below
/// [`MIN_ANSWER_TOKENS`] + [`MIN_THINKING_BUDGET_TOKENS`] = 17,408, and it is
/// a function of the ceiling ALONE — no caller cap and no thinking level can
/// trigger it, because the clamp has already run by the time the arms split.
/// At today's 64,000 it is **unreachable from every input**, and it is
/// implemented anyway because it is the clause that keeps the shape legal once
/// per-model ceilings land. Even a 32,000-ceiling model does not reach it: it
/// has 15,616 of room, so `xhigh` would merely be SHRUNK to that.
fn anthropic_output_budget(
    caller_cap: Option<u32>,
    thinking: Option<Value>,
) -> (u32, Option<Value>) {
    // Read the budget back out of the object with the existing idiom rather
    // than duplicating `anthropic_thinking`'s five-arm match: a second match
    // is a second place the level table lives and the two would drift. An
    // `adaptive` object simply has no `budget_tokens` key, so it reads as
    // `None` and passes through untouched.
    let budget = thinking
        .as_ref()
        .and_then(|t| t.get("budget_tokens"))
        .and_then(|b| b.as_u64())
        .map(|b| b as u32);
    let clamped = budget.and_then(clamp_thinking_budget);
    let max_tokens = match caller_cap {
        None => ANTHROPIC_OUTPUT_CEILING_TOKENS,
        Some(c) => c
            .max(1)
            .saturating_add(clamped.unwrap_or(0))
            .min(ANTHROPIC_OUTPUT_CEILING_TOKENS),
    };
    let thinking = match (budget, clamped) {
        // `adaptive` (or no level at all) passes through UNTOUCHED — this arm
        // is what keeps that mode byte-identical.
        (None, _) => thinking,
        // Rebuilt rather than mutated in place: a `budget_tokens` key exists
        // ONLY on the `enabled` object (that is all `anthropic_thinking` can
        // produce), so these arms can name `type: "enabled"` without reading
        // the mode back.
        (Some(_), Some(b)) => Some(json!({ "type": "enabled", "budget_tokens": b })),
        // No legal budget fits under the ceiling — drop `thinking` entirely
        // (unreachable at today's ceiling; see the docs above).
        (Some(_), None) => None,
    };
    (max_tokens, thinking)
}

/// The Claude-family model-id prefix (T3b / #131): a Claude model keeps its
/// model id when it is routed through LiteLLM, so the prefix alone identifies
/// the family (the `anthropic-messages` WIRE is gated at the caller — this
/// builder is only reachable from `AnthropicProvider::complete`).
fn is_claude_model(model: &str) -> bool {
    model.to_ascii_lowercase().starts_with("claude-")
}

/// The CAP on cached system blocks (T3b / #131): a long system prompt is
/// chunked on paragraph boundaries, but the block count never grows past this
/// number — the excess is FOLDED INTO THE LAST block (no text is dropped) and
/// the cache point still attaches to that last block.
const MAX_SYSTEM_BLOCKS: usize = 10;

/// The Anthropic prompt-caching marker (body-level, ADR 0026's wave-4
/// amendment): a `cache_control` on the LAST system block + on every tool
/// block makes the system + tool definitions a cached prefix.
fn ephemeral_cache_control() -> Value {
    json!({ "type": "ephemeral" })
}

/// The system text → the CACHED block array: chunked on paragraph boundaries
/// (the natural block shape Anthropic's own examples use), capped at
/// [`MAX_SYSTEM_BLOCKS`] by folding the remainder into the LAST chunk (nothing
/// dropped), every block a `{ "type": "text", "text" }` and the LAST carrying
/// `cache_control: { "type": "ephemeral" }`.
fn cached_system_blocks(system: &str) -> Vec<Value> {
    // The chunking: split on the paragraph separator the folding in
    // [`ModelRequest::system_text`] itself uses, so a multi-message system
    // becomes one block per message.
    let mut chunks: Vec<String> = system
        .split("\n\n")
        .filter(|c| !c.trim().is_empty())
        .map(str::to_string)
        .collect();
    if chunks.is_empty() {
        chunks.push(system.to_string());
    }
    if chunks.len() > MAX_SYSTEM_BLOCKS {
        // Fold EVERYTHING past the cap into the last kept chunk (the block
        // count caps, the text survives verbatim — joined back with the same
        // separator it was split on).
        let head: Vec<String> = chunks[..MAX_SYSTEM_BLOCKS - 1].to_vec();
        let tail = chunks[MAX_SYSTEM_BLOCKS - 1..].join("\n\n");
        chunks = head;
        chunks.push(tail);
    }
    let last = chunks.len() - 1;
    chunks
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            let mut block = json!({ "type": "text", "text": chunk });
            if i == last {
                block["cache_control"] = ephemeral_cache_control();
            }
            block
        })
        .collect()
}

/// Build the Anthropic Messages request body from a [`ModelRequest`]
/// (`max_tokens` is ALWAYS present — Anthropic requires it; `system` is
/// present only when non-empty; `thinking` is present only when a
/// `reasoning_effort` is set). The message `content` uses the Anthropic
/// WIRE form (a plain string for `Text`, an array of Anthropic content
/// parts for `Blocks` — [`anthropic_messages`]).
fn anthropic_request_body(req: &ModelRequest) -> Value {
    // `system`: the `System` messages' text, folded —
    // [`ModelRequest::system_text`].
    let system = req.system_text();
    let mut body = json!({
        "model": req.model,
        "messages": Value::Array(anthropic_messages(req)),
        "stream": req.options.stream,
    });
    // Prompt caching (T3b / #131, acceptance 1). The EXACT condition (no
    // hidden gate): a Claude model AND (tools OR a non-empty system). The
    // three no-caching branches exhaust the rest:
    //   (a) a non-Claude model → the plain-string `system` + flat tools
    //       (byte-identical to today);
    //   (b) a Claude model with NO tools AND no system text → no `system` key,
    //       no `tools` key, no `cache_control` anywhere (byte-identical);
    //   (c) a Claude model with an EMPTY tools list but NON-EMPTY system text →
    //       the cached system array (the tools branch below simply stays out).
    let cache = is_claude_model(&req.model) && (!req.tools.is_empty() || !system.is_empty());
    if cache {
        if !system.is_empty() {
            // The ARRAY form with the cache point on the LAST block (the
            // prefix covering the system text).
            body["system"] = Value::Array(cached_system_blocks(&system));
        }
    } else if !system.is_empty() {
        body["system"] = json!(system);
    }
    if !req.tools.is_empty() {
        // Flat `{ name, description, input_schema }` — the OpenAI
        // `function` nesting + `parameters` key do NOT apply.
        let mut tools: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.parameters,
                })
            })
            .collect();
        if cache {
            // The tool definitions are part of the cached prefix: EVERY tool
            // block carries the same ephemeral `cache_control` (a bounded set —
            // the tools list is the harness's own, never user-grown).
            for t in &mut tools {
                t["cache_control"] = ephemeral_cache_control();
            }
        }
        body["tools"] = tools.into();
    }
    if let Some(t) = req.options.temperature {
        body["temperature"] = json!(t);
    }
    // `max_tokens` is ALWAYS present (Anthropic requires it) and it is the
    // CEILING: the caller's cap or [`ANTHROPIC_OUTPUT_CEILING_TOKENS`], with
    // the `thinking` budget clamped DOWN to fit beneath it (the API requires
    // `max_tokens > budget_tokens`).
    let thinking = req
        .options
        .reasoning_effort
        .as_deref()
        .map(anthropic_thinking);
    let (max_tokens, thinking) = anthropic_output_budget(req.options.max_tokens, thinking);
    if let Some(thinking) = thinking {
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
            Duration::from_secs(30),      // a generous CONNECT bound
            Duration::from_secs(5 * 60),  // 5 min of SILENCE = a stalled provider
            self.custom_headers.as_ref(), // R1 (issue #129): the ONE completion seam
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
    use super::super::test_util::{chat_message, collect, keys_of_names};
    use super::super::types::{ChatMessage, ModelOptions, ToolSpec};
    use super::*;
    use crate::tools::ImageRef;

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

    /// The same, with a TRANSPORT failure as the LAST item of the raw stream —
    /// the shape `poll_next`'s `Poll::Ready(Some(Err(e)))` arm sees (a
    /// `read_timeout` on a stalled provider, a reset connection). The helper
    /// above cannot express it: it maps every chunk to `Ok`.
    fn anthropic_stream_failing(chunks: &[&str]) -> AnthropicStream {
        let mut data: Vec<Result<Vec<u8>, reqwest::Error>> =
            chunks.iter().map(|c| Ok(c.as_bytes().to_vec())).collect();
        data.push(Err(transport_error()));
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            futures_util::stream::iter(data).boxed();
        AnthropicStream::new(raw)
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
    // Both tool-call DISCARD paths on this wire were silent — the nameless acc
    // the flush drops, and the call in flight when the error latch makes the
    // stream-end flush unreachable. The settle-site line can only say a call was
    // STREAMED and never dispatched; these lines say WHERE it went.
    //
    // Asserted on the `diagnostics::record` half. The `eprintln!` half is not
    // assertable from a lib unit test — `provider/mod.rs`'s
    // `the_chosen_wire_is_logged_at_construction` limit, restated: its only
    // production reader is the Worker-stderr drain task, which needs a spawned
    // Worker. It is covered here by carrying the SAME payload.

    /// The stems of EVERY line this work adds, on ANY of the three wires. The
    /// healthy-turn test asserts none appears, so its empty assertion guards
    /// against instrumenting a site that fires on a GOOD turn — including one in a
    /// sibling parser whose code this fixture happens to reach.
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
        let dir = crate::test_support::ScratchDir::in_env_temp("anthropic-discard-diag-", tag);
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

    /// One `content_block_start` event for a `tool_use` block. Built with `json!`
    /// rather than the hand-escaped literals the neighbouring fixtures use, for ONE
    /// reason: the expected line names `args_bytes`, and an escaped literal makes
    /// that number a claim about my escaping rather than about the data.
    fn content_block_start(index: u32, id: Option<&str>, name: Option<&str>) -> String {
        let mut block = serde_json::Map::new();
        block.insert("type".to_string(), json!("tool_use"));
        if let Some(id) = id {
            block.insert("id".to_string(), json!(id));
        }
        if let Some(name) = name {
            block.insert("name".to_string(), json!(name));
        }
        block.insert("input".to_string(), json!({}));
        format!(
            "event: content_block_start\n\
             data: {}\n\n\
             ",
            json!({ "type": "content_block_start", "index": index,
                    "content_block": Value::Object(block) })
        )
    }

    /// One `input_json_delta` event (an argument fragment).
    fn input_json_delta(index: u32, fragment: &str) -> String {
        format!(
            "event: content_block_delta\n\
             data: {}\n\n\
             ",
            json!({ "type": "content_block_delta", "index": index,
                    "delta": { "type": "input_json_delta",
                               "partial_json": fragment } })
        )
    }

    /// The terminal `message_delta` (the arm that flushes on a healthy stream).
    const MESSAGE_DELTA: &str = "event: message_delta\n\
         data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"}}\n\n";

    /// The argument text every fixture below streams. The nameless fixture splits
    /// it across two fragments; its BYTE length is what the expected line names.
    const ARG_TEXT: &str = "{\"path\":\"/x\"}";

    /// The EXACT payload for a nameless acc dropped at the flush — spelled out in
    /// full, deliberately NOT re-derived from the message helper. A test that
    /// called the helper to build its own expectation would pass whatever the
    /// helper chose to say; a literal reddens when the sentence changes.
    const WANT_NAMELESS: &str = "tool call dropped with no name: parser=anthropic \
         stage=flush index=1 id=\"tu_9\" args_bytes=13 — a call with no name \
         cannot be dispatched, so the model's call is lost and the turn settles \
         without it";

    /// The EXACT payload for the call in flight when the stream dies on the error
    /// latch. `stage` names the arm, so the two latch sites stay distinguishable in
    /// a file a supporter reads end to end.
    const WANT_LATCH_INSTREAM: &str = "tool calls dropped at a stream error: \
         parser=anthropic stage=in-stream-error in_flight=1 named_any=true — the \
         stream-end flush is unreachable once the stream is exhausted, so a call \
         the model had already streamed was lost";

    const WANT_LATCH_TRANSPORT: &str = "tool calls dropped at a stream error: \
         parser=anthropic stage=transport-error in_flight=1 named_any=true — the \
         stream-end flush is unreachable once the stream is exhausted, so a call \
         the model had already streamed was lost";

    /// THE defect on this wire: a `tool_use` block whose `content_block_start`
    /// carries an `id` and NO `name`. The acc is assembled, the argument fragments
    /// stream into it, the `message_delta` flushes — and the flush drops the call
    /// in silence, because `acc.name` is `None` and the `continue` swallows it.
    ///
    /// The `record` half is the assertion that reddens. The "no `ToolCall`" half is
    /// a PIN of the pre-existing behaviour the new line is bolted to (a call with
    /// no name could never be dispatched, and still cannot), and its witness is the
    /// deltas — which prove the block opened and its fragments arrived, so the
    /// recorded line is credited to a DISCARD and not to a fixture that streamed
    /// nothing.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_tool_use_block_with_no_name_is_dropped_and_the_drop_is_recorded() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("nameless");
        reset_diagnostics_file(armed.dir.path());

        assert_eq!(ARG_TEXT.len(), 13, "the number the expected line quotes");
        let events = collect(anthropic_stream(&[
            &content_block_start(1, Some("tu_9"), None),
            &input_json_delta(1, "{\"pa"),
            &input_json_delta(1, "th\":\"/x\"}"),
            MESSAGE_DELTA,
        ]))
        .await;

        // The witness: the block opened and BOTH fragments were accumulated — the
        // acc existed with arguments in it, so the absence below is the DISCARD.
        let deltas = events
            .iter()
            .filter(|e| matches!(e, ProviderEvent::ToolCallDelta(_)))
            .count();
        assert_eq!(
            deltas, 3,
            "the `content_block_start` delta and both argument fragments reached \
             the acc (the drop is downstream of them), got {events:?}"
        );
        // The PIN: the nameless call is not dispatched — unchanged behaviour.
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "a call with no name cannot be dispatched, so it is dropped (and \
             always was), got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&lines, WANT_NAMELESS),
            1,
            "the discard is recorded EXACTLY ONCE, with no `harness: ` prefix on \
             the `record` half (the `log_wire` convention) and naming the parser, \
             the stage, the index, the id and the argument-buffer length; want a \
             line ending with {WANT_NAMELESS:?}, got:\n{lines:#?}"
        );
    }

    /// The in-stream `error` arm: the HTTP status was 200 and the error is IN the
    /// stream, so the arm sets `exhausted` and pushes the `Error` — and the
    /// stream-end flush sits inside the `Poll::Ready(None)` block behind
    /// `if !this.finished`, which `exhausted` makes UNREACHABLE. A call in flight
    /// at that moment is lost, and until now it was lost silently.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn an_in_stream_error_with_a_call_in_flight_records_the_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("instream-error");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(anthropic_stream(&[
            &content_block_start(1, Some("tu_9"), Some("read")),
            &input_json_delta(1, ARG_TEXT),
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\
             \"message\":\"Overloaded\"}}\n\n",
        ]))
        .await;

        // The witness: the call streamed (name and all) and the stream really did
        // end on the latch this site logs at.
        assert!(
            matches!(
                &events[events.len() - 1],
                ProviderEvent::Error(ProviderError::Retryable(m)) if m.contains("Overloaded")
            ),
            "the fixture ends on the in-stream error, got {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "the flush never runs on this path — that IS the loss being reported, \
             got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&lines, WANT_LATCH_INSTREAM),
            1,
            "the error latch reports the call it is about to lose, naming the \
             count; want a line ending with {WANT_LATCH_INSTREAM:?}, got:\n\
             {lines:#?}"
        );
    }

    /// … and the transport-error arm: the same latch, set from `poll_next` (a
    /// `read_timeout` on a stalled provider, a reset connection).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_transport_failure_with_a_call_in_flight_records_the_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("transport-error");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(anthropic_stream_failing(&[
            &content_block_start(1, Some("tu_9"), Some("read")),
            &input_json_delta(1, ARG_TEXT),
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
            "the transport latch reports the call it is about to lose; want a line \
             ending with {WANT_LATCH_TRANSPORT:?}, got:\n{lines:#?}"
        );
    }

    /// THE GUARD ITSELF, on the latch: an error with NO call in flight records
    /// nothing. This is the assertion that makes `!self.tool_acc.is_empty()`
    /// load-bearing rather than decorative — an in-stream error is an ORDINARY
    /// failure the loop retries, and every one of them would otherwise write a
    /// "tool calls dropped" sentence into the always-on file for a turn that had
    /// no call to lose.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_stream_error_with_nothing_in_flight_records_no_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("error-empty-acc");
        reset_diagnostics_file(armed.dir.path());

        // Text only, then the error — no `tool_use` block, so the acc map is
        // NEVER touched and `tool_acc.len()` is 0 when the latch is set.
        let events = collect(anthropic_stream(&[
            "event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"\
             content_block\":{\"type\":\"text\"}}\n\n",
            "event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"\
             delta\":{\"type\":\"text_delta\",\"text\":\"thinking out loud\"}}\n\n",
            "event: error\n\
             data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\
             \"message\":\"Overloaded\"}}\n\n",
        ]))
        .await;

        // The witness: the error really did land (the latch WAS reached) and no
        // tool-call fragment ever arrived — so the empty assertion below measures
        // the gate and not a path that never ran.
        assert!(
            matches!(
                &events[events.len() - 1],
                ProviderEvent::Error(ProviderError::Retryable(_))
            ),
            "the fixture reached the error latch, got {events:?}"
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
            "an error with NO call in flight lost nothing, so the gate must hold \
             it shut; got:\n{lines:#?}"
        );
    }

    /// The other half of the guard: the SAME call, the stream ending CLEAN. Here
    /// the flush runs and the call is dispatched, so nothing was lost and the latch
    /// line must not appear. Without this, the new site would fire at the ordinary
    /// end of every stream and the always-on file would carry a failure sentence
    /// for a turn that worked.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_clean_stream_end_with_a_call_in_flight_records_no_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("clean-end");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(anthropic_stream(&[
            &content_block_start(1, Some("tu_9"), Some("read")),
            &input_json_delta(1, ARG_TEXT),
            // NO `message_delta`: the stream ends with the call still in the acc,
            // and the stream-end block flushes it and synthesizes the `Done`.
        ]))
        .await;

        // The witness: the stream ended with no `stop_reason` and the flush ran
        // anyway — the call was dispatched, so there is nothing to report.
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::ToolCall(_))),
            "a clean end flushes the call — the path the latch line must stay off, \
             got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert!(
            lines.is_empty(),
            "a stream that ended clean dispatched its call, so no latch line may \
             fire; got:\n{lines:#?}"
        );
    }

    /// … and the same gate at the TRANSPORT latch, whose `tool_acc` is reached by
    /// a different fixture (the `Err` item arrives from `poll_next`, not from
    /// `handle_chunk`) — so the in-stream test above cannot pin it.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_transport_failure_with_nothing_in_flight_records_no_drop() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("transport-empty-acc");
        reset_diagnostics_file(armed.dir.path());

        // Text only, then the transport failure — no `tool_use` block, so the acc
        // map is NEVER touched and `tool_acc.len()` is 0 at the latch.
        let events = collect(anthropic_stream_failing(&[
            "event: content_block_start\n\
             data: {\"type\":\"content_block_start\",\"index\":0,\"\
             content_block\":{\"type\":\"text\"}}\n\n",
            "event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"\
             delta\":{\"type\":\"text_delta\",\"text\":\"thinking out loud\"}}\n\n",
        ]))
        .await;

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

    /// The healthy-turn guard, and the one that keeps this change from becoming
    /// noise: a well-formed `tool_use` block, streamed and dispatched, records
    /// NOTHING.
    ///
    /// The fixture uses WELL-FORMED arguments on purpose. `parse_arguments`'s
    /// `harness: malformed tool-call arguments` line predates this task and fires
    /// on bad JSON, so a malformed fixture would redden this assertion for a reason
    /// unrelated to the guard. (A NAMELESS call can never double-fire with it — a
    /// nameless call is dropped BEFORE `parse_arguments` is reached.)
    ///
    /// The assertion covers BOTH stems, so it also reddens if the placeholder
    /// filters are ever instrumented — they fire on a good turn.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn a_healthy_tool_call_turn_records_no_drop_line() {
        let _guard = crate::test_support::env_lock();
        let armed = arm_diagnostics("healthy");
        reset_diagnostics_file(armed.dir.path());

        let events = collect(anthropic_stream(&[
            &content_block_start(1, Some("tu_9"), Some("read")),
            &input_json_delta(1, ARG_TEXT),
            MESSAGE_DELTA,
        ]))
        .await;

        // The witness: this IS a healthy turn — the call was assembled and
        // dispatched with well-formed arguments, so the empty assertion below
        // measures a guard and not a dead path.
        assert!(
            events.iter().any(|e| matches!(
                e,
                ProviderEvent::ToolCall(t) if t.name == "read" && t.defect.is_none()
            )),
            "the fixture dispatched a complete, well-formed call, got {events:?}"
        );

        let lines = discard_lines(&recorded_lines(armed.dir.path()));
        assert!(
            lines.is_empty(),
            "a turn that streamed and dispatched its call lost nothing, so no \
             discard line may be written; got:\n{lines:#?}"
        );

        // The POSITIVE CONTROL, so the assertion above cannot be vacuous by way of
        // a channel that records nothing at all: the same armed channel takes the
        // same sentence when the guard IS satisfied. If this half stops writing,
        // the empty assertion above has stopped meaning anything.
        crate::diagnostics::record(WANT_NAMELESS);
        let after = discard_lines(&recorded_lines(armed.dir.path()));
        assert_eq!(
            ending_with(&after, WANT_NAMELESS),
            1,
            "the armed channel DOES take this line — so the empty assertion above \
             measured the guard, not a dead sink; got:\n{after:#?}"
        );
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

    // ── `normalize_wire_base_url` (T3b / #131 — the per-wire rule) ─────

    /// The Messages rule: the ROOT (a trailing `/v1` is stripped, never
    /// stripped further — the idempotency contract).
    #[test]
    fn normalize_wire_base_url_root_for_messages() {
        let messages = WireApi::AnthropicMessages;
        assert_eq!(
            normalize_wire_base_url("https://x/v1", messages),
            "https://x",
            "a `{{root}}/v1` base becomes the root (the client appends `/v1/messages`)",
        );
        assert_eq!(
            normalize_wire_base_url("https://x/v1/", messages),
            "https://x",
            "the trailing slash is dropped before the strip"
        );
        assert_eq!(normalize_wire_base_url("https://x", messages), "https://x");
        // IDEMPOTENCY: applying the helper twice == applying it once.
        let once = normalize_wire_base_url("https://x/v1", messages);
        assert_eq!(normalize_wire_base_url(&once, messages), once);
        // A query string survives the path surgery.
        assert_eq!(
            normalize_wire_base_url("https://x/v1?q=1", messages),
            "https://x?q=1"
        );
        // An unparseable base → the plain suffix strip.
        assert_eq!(normalize_wire_base_url("x/v1", messages), "x");
        assert_eq!(normalize_wire_base_url("x", messages), "x");
        let once = normalize_wire_base_url("x/v1", messages);
        assert_eq!(normalize_wire_base_url(&once, messages), once);
        // Empty / whitespace → empty (nothing to normalize).
        assert_eq!(normalize_wire_base_url("", messages), "");
    }

    /// The Completions / Responses rule: `{root}/v1` (appended
    /// idempotently — the shape `normalize_anthropic_base_url` produces).
    #[test]
    fn normalize_wire_base_url_v1_for_completions_and_responses() {
        for api in [WireApi::OpenAiCompletions, WireApi::OpenAiResponses] {
            assert_eq!(
                normalize_wire_base_url("https://x", api),
                "https://x/v1",
                "{api:?}: a bare root gains `/v1`"
            );
            assert_eq!(
                normalize_wire_base_url("https://x/", api),
                "https://x/v1",
                "{api:?}: the trailing slash is dropped, `/v1` appended"
            );
            assert_eq!(
                normalize_wire_base_url("https://x/v1", api),
                "https://x/v1",
                "{api:?}: an already-normalized base is left alone (no double-append)"
            );
            // IDEMPOTENCY at the helper level.
            let once = normalize_wire_base_url("https://x", api);
            assert_eq!(normalize_wire_base_url(&once, api), once);
            // A query string survives.
            assert_eq!(
                normalize_wire_base_url("https://x?a=1", api),
                "https://x/v1?a=1"
            );
            // An unparseable base → the plain suffix append.
            assert_eq!(normalize_wire_base_url("x", api), "x/v1");
            let once = normalize_wire_base_url("x", api);
            assert_eq!(normalize_wire_base_url(&once, api), once);
        }
    }

    /// `litellm` is a DISCOVERY MODE whose completion wire IS
    /// `openai-completions` (ADR 0026) — so its base normalizes as
    /// Completions.
    #[test]
    fn normalize_wire_base_url_litellm_uses_v1() {
        assert_eq!(
            normalize_wire_base_url("https://x", WireApi::LiteLLM),
            "https://x/v1"
        );
        assert_eq!(
            normalize_wire_base_url("https://x/v1", WireApi::LiteLLM),
            "https://x/v1"
        );
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
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 7,
                    },
                    None
                ),
                ProviderEvent::ToolCall(ToolCall {
                    id: "tu_1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                    defect: None,
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
                    defect: None,
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "tu_1".to_string(),
                    name: "bash".to_string(),
                    arguments: json!({ "c": "y" }),
                    defect: None,
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
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 0,
                    },
                    None
                ),
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
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 7,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// (ADR 0046) The Anthropic Messages wire reports `input_tokens` as
    /// the UNCACHED remainder — the cached prefix lives in
    /// `cache_read_input_tokens` / `cache_creation_input_tokens`. The
    /// parser seam is where that is reconciled: `Usage.input_tokens` IS
    /// the whole prompt the model was shown, so the cached prefix is
    /// SUMMED IN here and the **Anchor** (ADR 0028) sees the real context
    /// size instead of under-reading it and letting compaction arrive
    /// late (a Provider 400).
    #[tokio::test]
    async fn anthropic_sse_usage_sums_the_cached_prefix_into_input_tokens() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":50,\"cache_creation_input_tokens\":25}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 175,
                        output_tokens: 7,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// The other half of the seam: with no cache fields on the wire (an
    /// endpoint that never caches), `input_tokens` passes through
    /// UNCHANGED — the sum is over the fields that are PRESENT.
    #[tokio::test]
    async fn anthropic_sse_usage_without_cache_fields_passes_input_tokens_through() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":100}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 100,
                        output_tokens: 7,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// The `emit_usage` guard survives the sum: a stream that reports
    /// NOTHING (no `usage` on `message_start`, no `output_tokens` on the
    /// `message_delta`) emits NO `Usage` at all — storing a sum
    /// unconditionally would turn the absent counts into `Some(0)` and
    /// emit a spurious `Usage(0, 0)`. (A `message_delta` that DOES carry
    /// `output_tokens` emits `Usage(0, n)` by the documented
    /// absent-side-becomes-zero rule — that is NOT the case under test.)
    #[tokio::test]
    async fn anthropic_sse_no_usage_anywhere_emits_no_usage_event() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\"}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("x".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
        assert!(
            !events.iter().any(|e| matches!(e, ProviderEvent::Usage(..))),
            "a stream with no counts on either side emits NO Usage (got {events:?})"
        );
    }

    // ── the per-call cost off the BODY's usage object (D4, AMENDED) ─────

    /// The cost rides the `message_delta` `usage` — the object that
    /// carries the FINAL output count, and the Anthropic analogue of the
    /// OpenAI-shaped final usage chunk where LiteLLM injects.
    #[tokio::test]
    async fn anthropic_sse_cost_rides_the_message_delta_usage() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7,\"cost\":0.002}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 7,
                    },
                    Some(0.002)
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// … and it rides the `message_start` `usage` too (the input side):
    /// both places the parser already reads a usage object are read for a
    /// cost, because which one a gateway prices is deployment-dependent.
    #[tokio::test]
    async fn anthropic_sse_cost_rides_the_message_start_usage() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cost\":0.001}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 7,
                    },
                    Some(0.001)
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// LAST-NON-`None`-WINS, in both directions of "later": a later usage
    /// object that PRICES the call supersedes the earlier figure (the
    /// final `message_delta` carries the final number, exactly as
    /// `last_output_tokens` does), and a later usage object that omits
    /// cost does NOT erase an earlier figure — an absent field is not a
    /// zero and not a retraction.
    #[tokio::test]
    async fn anthropic_sse_cost_is_the_last_non_none_usage_object() {
        // A later cost supersedes.
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cost\":0.001}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3,\"cost\":0.004}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7,\"cost\":0.009}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 7,
                    },
                    Some(0.009)
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );

        // A later usage object WITHOUT a cost keeps the earlier figure.
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cost\":0.001}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 7,
                    },
                    Some(0.001)
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// Exactly-once survives the cost: a figure present on BOTH usage
    /// objects still yields ONE `Usage` event — the cost is one more
    /// argument on the single push behind the unchanged `usage_emitted`
    /// guard, so exactly-once holds by construction rather than by a new
    /// invariant.
    #[tokio::test]
    async fn anthropic_sse_cost_is_emitted_exactly_once() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cost\":0.001}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3,\"cost\":0.004}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7,\"cost\":0.009}}\n\n",
        ]))
        .await;
        let with_cost: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, ProviderEvent::Usage(..)))
            .collect();
        assert_eq!(
            with_cost.len(),
            1,
            "a cost seen twice still emits ONE Usage (got {events:?})"
        );
    }

    /// The honest live case (probe P4: this proxy's `/v1/messages` stream
    /// reports token counts and NO money). Absent is `None` — NOT `0.0`,
    /// which the UI would render as a free `$0.00`.
    #[tokio::test]
    async fn anthropic_sse_absent_cost_is_none_not_zero() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":91}}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":91,\"output_tokens\":8}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 91,
                        output_tokens: 8,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// A provider that sends a cost that is not a number gets NO figure:
    /// `None`, not a panic and not a clamp.
    #[tokio::test]
    async fn anthropic_sse_malformed_cost_is_none() {
        // Built with `json!` so the malformed value is inserted VERBATIM
        // (a hand-escaped format! string is how this pin first shipped a
        // truncated fixture and reddened for the wrong reason).
        for malformed in [json!("abc"), Value::Null, json!({}), json!(-0.5)] {
            let payload = json!({
                "type": "message_delta",
                "delta": { "stop_reason": "end_turn" },
                "usage": { "output_tokens": 7, "cost": malformed },
            });
            let events = collect(anthropic_stream(&[
                "event: message_start\n\
                 data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10}}}\n\n",
                &format!("event: message_delta\ndata: {payload}\n\n"),
            ]))
            .await;
            assert_eq!(
                events,
                vec![
                    ProviderEvent::Usage(
                        Usage {
                            input_tokens: 10,
                            output_tokens: 7,
                        },
                        None
                    ),
                    ProviderEvent::Done(FinishReason::Stop),
                ],
                "a malformed cost ({malformed}) is None, not a panic"
            );
        }
    }

    /// REGRESSION GUARD (a recorded residue, not a RED pin): the
    /// `emit_usage` guard must NOT gain a cost term. A cost with no token
    /// counts on either side emits NO `Usage` — inventing a cost-only
    /// emission would push `Usage(0, 0, Some(money))`, and
    /// `Compactor::record_usage` would then write a ZERO **Anchor**: a
    /// compaction regression bought for a figure. This asserts the ABSENCE
    /// of an event, so it is green before the change as well as after.
    #[tokio::test]
    async fn anthropic_sse_cost_with_no_token_counts_emits_no_usage_event() {
        let events = collect(anthropic_stream(&[
            "event: message_start\n\
             data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"cost\":0.001}}}\n\n\
             event: content_block_delta\n\
             data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"x\"}}\n\n\
             event: message_delta\n\
             data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"cost\":0.002}}\n\n",
        ]))
        .await;
        assert!(
            !events.iter().any(|e| matches!(e, ProviderEvent::Usage(..))),
            "a cost with no counts on either side emits NO Usage (got {events:?})"
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
            custom_headers: None,
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
                        // (issue #193) A DEFECT-CARRYING call sits in this
                        // fixture on purpose: the `keys_of_names` pin on the
                        // `tool_use` block below is the leak guard, and it only
                        // proves anything if a call that HAS a defect is the one
                        // being rendered. `anthropic_messages` builds the block
                        // field by field, so `defect` has no path to the wire.
                        defect: Some(crate::harness::provider::ArgumentDefect {
                            error: "EOF while parsing an object".to_string(),
                            raw: r#"{"path": "/x"#.to_string(),
                        }),
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

        // ── EXACT-FIELD-SET PINS (the leak guard) ───────────────────────
        // `anthropic_messages` is a field-by-field WHITELIST, but nothing
        // tested that. `ChatMessage` is BOTH the transcript type AND the type
        // this body is built from, so a transcript-only field (e.g. a future
        // `finish_reason`) must NEVER appear here — a strict proxy rejects the
        // unknown field with a 400, which is a Fatal on EVERY model call.
        // Pinned at BOTH levels: the message object AND its content blocks
        // (on this wire `tool_calls` became `tool_use` BLOCKS, not a message key).
        assert_eq!(
            keys_of_names(&body),
            vec![
                "max_tokens",
                "messages",
                "model",
                "stream",
                "system",
                "temperature",
                "tools"
            ],
            "the Anthropic body carries exactly these keys (`max_tokens` is \
             always present; no `thinking` — no `reasoning_effort`)"
        );
        assert_eq!(
            keys_of_names(&messages[1]),
            vec!["content", "role"],
            "an assistant message object is exactly `role` + `content` on this \
             wire — a transcript-only field must not appear here"
        );
        assert_eq!(
            keys_of_names(&content[0]),
            vec!["id", "input", "name", "type"],
            "a `tool_use` block is exactly `type` + `id` + `name` + `input` \
             — a transcript-only field must not appear here (and the call behind \
             this block CARRIES a `defect`, so the field is proven absent rather \
             than merely never set: issue #193)"
        );
        assert_eq!(
            keys_of_names(&messages[2]),
            vec!["content", "role"],
            "the merged `user` (tool_result) message is exactly `role` + `content`"
        );
        assert_eq!(
            keys_of_names(tool_result),
            vec!["content", "tool_use_id", "type"],
            "a `tool_result` block is exactly `type` + `tool_use_id` + `content`"
        );
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
    fn anthropic_request_body_consecutive_tool_messages_empty_and_real_merge() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text(String::new()),
                    Some("t1"),
                    None,
                ),
                chat_message(
                    ChatRole::Tool,
                    MessageContent::Text("real output".to_string()),
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
            "two consecutive tool messages merge into ONE user message"
        );
        assert_eq!(messages[0]["role"], "user");
        let content = messages[0]["content"]
            .as_array()
            .expect("the merged content is a block array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "tool_result");
        assert_eq!(content[0]["tool_use_id"], "t1");
        assert_eq!(content[0]["content"], "(no output)");
        assert_eq!(content[1]["type"], "tool_result");
        assert_eq!(content[1]["tool_use_id"], "t2");
        assert_eq!(content[1]["content"], "real output");
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

    fn req_body(messages: Vec<ChatMessage>) -> Value {
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

    fn img() -> ContentBlock {
        ContentBlock::Image {
            image: ImageRef {
                data: "BASE64DATA".to_string(),
                mime_type: "image/png".to_string(),
            },
        }
    }

    fn text_block(t: &str) -> ContentBlock {
        ContentBlock::Text {
            text: t.to_string(),
        }
    }

    fn one_call() -> Option<Vec<ToolCall>> {
        Some(vec![ToolCall {
            id: "tu_1".to_string(),
            name: "read".to_string(),
            arguments: json!({}),
            defect: None,
        }])
    }

    #[test]
    fn anthropic_tool_result_content_replaces_empty_text_with_no_output_placeholder() {
        for empty in [
            "",
            "   ",
            "\n",
            "[System: Empty message content sanitised to satisfy protocol]",
        ] {
            assert_eq!(
                anthropic_tool_result_content(&MessageContent::Text(empty.to_string())),
                Value::String("(no output)".to_string()),
                "empty tool text {empty:?} must be replaced by \"(no output)\""
            );
        }
    }

    #[test]
    fn anthropic_tool_result_content_replaces_empty_blocks_with_no_output_placeholder() {
        for blocks in [
            vec![],
            vec![text_block("")],
            vec![text_block("  "), text_block("")],
        ] {
            assert_eq!(
                anthropic_tool_result_content(&MessageContent::Blocks(blocks)),
                Value::String("(no output)".to_string()),
                "empty blocks must be replaced by \"(no output)\""
            );
        }
    }

    #[test]
    fn anthropic_tool_result_content_preserves_real_text_and_images() {
        assert_eq!(
            anthropic_tool_result_content(&MessageContent::Text("hello".to_string())),
            Value::String("hello".into())
        );
        assert_eq!(
            anthropic_tool_result_content(&MessageContent::Text("  padded  ".to_string())),
            Value::String("  padded  ".into())
        );
        assert_eq!(
            anthropic_tool_result_content(&MessageContent::Blocks(vec![text_block(""), img()])),
            json!([
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "BASE64DATA",
                    },
                }
            ])
        );
        let mixed = anthropic_tool_result_content(&MessageContent::Blocks(vec![
            text_block("a"),
            text_block(""),
            img(),
        ]));
        let arr = mixed.as_array().expect("array of blocks");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], json!({ "type": "text", "text": "a" }));
        assert_eq!(
            arr[1],
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
    fn anthropic_messages_drops_empty_text_block_in_mixed_image_message() {
        let body = req_body(vec![chat_message(
            ChatRole::User,
            MessageContent::Blocks(vec![img(), text_block("")]),
            None,
            None,
        )]);
        let content = body["messages"][0]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "image");

        let body_rev = req_body(vec![chat_message(
            ChatRole::User,
            MessageContent::Blocks(vec![text_block(""), img()]),
            None,
            None,
        )]);
        let content_rev = body_rev["messages"][0]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content_rev.len(), 1);
        assert_eq!(content_rev[0]["type"], "image");
    }

    #[test]
    fn anthropic_messages_all_empty_blocks_without_tool_calls_become_empty_placeholder() {
        let body1 = req_body(vec![chat_message(
            ChatRole::User,
            MessageContent::Blocks(vec![]),
            None,
            None,
        )]);
        assert_eq!(
            body1["messages"][0]["content"],
            json!([{ "type": "text", "text": "(empty)" }])
        );

        let body2 = req_body(vec![chat_message(
            ChatRole::User,
            MessageContent::Blocks(vec![text_block("")]),
            None,
            None,
        )]);
        assert_eq!(
            body2["messages"][0]["content"],
            json!([{ "type": "text", "text": "(empty)" }])
        );
    }

    #[test]
    fn anthropic_messages_empty_text_without_tool_calls_becomes_empty_placeholder() {
        for empty in [
            "",
            "   ",
            "\n\t ",
            "[System: Empty message content sanitised to satisfy protocol]",
        ] {
            let body_user = req_body(vec![chat_message(
                ChatRole::User,
                MessageContent::Text(empty.to_string()),
                None,
                None,
            )]);
            assert_eq!(
                body_user["messages"][0]["content"],
                json!("(empty)"),
                "empty user text {empty:?} must emit placeholder"
            );

            let body_assistant = req_body(vec![
                chat_message(
                    ChatRole::User,
                    MessageContent::Text("hello".to_string()),
                    None,
                    None,
                ),
                chat_message(
                    ChatRole::Assistant,
                    MessageContent::Text(empty.to_string()),
                    None,
                    None,
                ),
            ]);
            assert_eq!(
                body_assistant["messages"][1]["content"],
                json!("(empty)"),
                "empty assistant text {empty:?} must emit placeholder"
            );
        }
    }

    #[test]
    fn anthropic_messages_assistant_tool_use_with_empty_blocks_omits_text_block() {
        let body = req_body(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::Assistant,
                MessageContent::Blocks(vec![text_block("")]),
                None,
                one_call(),
            ),
        ]);
        let content = body["messages"][1]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "tool_use");
        assert_eq!(content[0]["id"], "tu_1");
        assert!(
            !content.iter().any(|b| b["type"] == "text"),
            "no text block should be present"
        );

        // ── EXACT-FIELD-SET PINS (the leak guard) ───────────────────────
        // Both levels of this wire: the assistant MESSAGE object and its
        // `tool_use` BLOCK. A transcript-only field (e.g. a future
        // `finish_reason` on `ChatMessage`) must never appear in either.
        assert_eq!(
            keys_of_names(&body["messages"][1]),
            vec!["content", "role"],
            "an assistant message object is exactly `role` + `content` on this \
             wire (its `tool_calls` became BLOCKS, never a message key) — a \
             transcript-only field must not appear here"
        );
        assert_eq!(
            keys_of_names(&content[0]),
            vec!["id", "input", "name", "type"],
            "a `tool_use` block is exactly `type` + `id` + `name` + `input` — \
             a transcript-only field must not appear here"
        );
    }

    #[test]
    fn anthropic_messages_assistant_whitespace_text_with_tool_calls_omits_text_block() {
        let body = req_body(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::Assistant,
                MessageContent::Text("  ".to_string()),
                None,
                one_call(),
            ),
        ]);
        let content = body["messages"][1]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "tool_use");

        // Keep side: non-empty thinking text
        let body_keep = req_body(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::Assistant,
                MessageContent::Text("thinking".to_string()),
                None,
                one_call(),
            ),
        ]);
        let content_keep = body_keep["messages"][1]["content"]
            .as_array()
            .expect("content array");
        assert_eq!(content_keep.len(), 2);
        assert_eq!(
            content_keep[0],
            json!({ "type": "text", "text": "thinking" })
        );
        assert_eq!(content_keep[1]["type"], "tool_use");
    }

    #[test]
    fn anthropic_request_body_tool_result_never_contains_empty_text() {
        // Text("") case
        let body_text = req_body(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::Assistant,
                MessageContent::Text("".to_string()),
                None,
                one_call(),
            ),
            chat_message(
                ChatRole::Tool,
                MessageContent::Text("".to_string()),
                Some("tu_1"),
                None,
            ),
        ]);
        assert_eq!(
            body_text["messages"][2]["content"][0]["type"],
            "tool_result"
        );
        assert_eq!(
            body_text["messages"][2]["content"][0]["content"],
            "(no output)"
        );

        // Blocks([text_block("")]) case
        let body_blocks = req_body(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::Assistant,
                MessageContent::Text("".to_string()),
                None,
                one_call(),
            ),
            chat_message(
                ChatRole::Tool,
                MessageContent::Blocks(vec![text_block("")]),
                Some("tu_1"),
                None,
            ),
        ]);
        assert_eq!(
            body_blocks["messages"][2]["content"][0]["type"],
            "tool_result"
        );
        assert_eq!(
            body_blocks["messages"][2]["content"][0]["content"],
            "(no output)"
        );

        assert!(
            !body_text.to_string().contains("\"text\":\"\""),
            "body_text must not contain empty text: {body_text}"
        );
        assert!(
            !body_blocks.to_string().contains("\"text\":\"\""),
            "body_blocks must not contain empty text: {body_blocks}"
        );
    }

    #[test]
    fn anthropic_request_body_tool_result_keeps_image_only_blocks() {
        let body = req_body(vec![
            chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            ),
            chat_message(
                ChatRole::Assistant,
                MessageContent::Text("".to_string()),
                None,
                one_call(),
            ),
            chat_message(
                ChatRole::Tool,
                MessageContent::Blocks(vec![img()]),
                Some("tu_1"),
                None,
            ),
        ]);
        let tool_content = body["messages"][2]["content"][0]["content"]
            .as_array()
            .expect("array content");
        assert_eq!(tool_content.len(), 1);
        assert_eq!(tool_content[0]["type"], "image");
        assert!(
            !body.to_string().contains("(no output)"),
            "image-only result should not contain '(no output)': {body}"
        );
    }

    /// `max_tokens` on the wire is a CEILING the thinking budget fits under —
    /// NOT the sum of the budget and an answer reservation. The shape this
    /// replaced (`max_tokens.max(budget + 1024)`) grew the request with the
    /// budget, which is exactly what Anthropic rejects.
    #[test]
    fn anthropic_request_body_max_tokens_is_a_ceiling_not_a_budget_sum() {
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
        // ── no caller cap: the ceiling is what goes on the wire ──────────
        // `max_tokens: None` + a `max` budget: the CEILING, not the old
        // `budget + 1024` = `129024` — a number Anthropic answers with a 400
        // (its own synchronous ceiling is 64,000 on the 4.5 generation, and an
        // over-cap value is a rejection, not a silent clamp).
        assert_eq!(
            req(None, Some("max"))["max_tokens"],
            ANTHROPIC_OUTPUT_CEILING_TOKENS,
            "`max_tokens` must be the ceiling; the old value 129024 was a 400 \
             from Anthropic (it is `128000 + 1024`, the sum this replaced)"
        );
        // The level that matters most in practice: all 90 thinking-level
        // sessions in our DB are `xhigh`, and `xhigh` keeps its FULL budget,
        // because 40,000 ≤ 47,616 — the room the ceiling leaves after the
        // answer's floor. The ceiling costs this level nothing.
        assert_eq!(
            req(None, Some("xhigh"))["max_tokens"],
            ANTHROPIC_OUTPUT_CEILING_TOKENS
        );
        assert_eq!(req(None, Some("xhigh"))["thinking"]["budget_tokens"], 40000);
        // `max` is the one level that SHRINKS: 128,000 → exactly the room the
        // ceiling leaves after the answer's floor (64,000 − 16,384 = 47,616).
        assert_eq!(
            req(None, Some("max"))["thinking"]["budget_tokens"],
            ANTHROPIC_OUTPUT_CEILING_TOKENS - MIN_ANSWER_TOKENS
        );

        // ── a caller cap is HONOURED, never raised to the ceiling ─────────
        // The desktop's titling request sends `Some(100)` and the summariser
        // sends `min(0.8 × reserve_tokens, 8_192).max(1)` — anything in
        // `1..=8_192`, `Some(8192)` at a roomy reserve and `Some(1)` at a zero
        // one; both want a SHORT answer. This is the assertion
        // that stops a future reader "simplifying" the ceiling into an
        // unconditional write of `ANTHROPIC_OUTPUT_CEILING_TOKENS`, which
        // would make titling pay for 64,000 tokens of title.
        assert_eq!(
            req(Some(64), None)["max_tokens"],
            64,
            "a caller cap is honoured, not raised to the ceiling — titling \
             (Some(100)) and the summariser (anything in 1..=8192) both cap \
             on purpose"
        );
        // The caller-cap ARM: the cap plus the CLAMPED budget. 64 + 4096 =
        // 4160. This pin CHANGES from the old `5120`, and deliberately so: the
        // old clamp forced the sum UP to `budget + 1024` = 5,120, i.e. it
        // overrode the caller's cap to buy the answer 1,024 tokens. In the
        // caller-cap arm the caller's own cap IS the answer room, so the
        // answer gets 64 and the number comes DOWN. Keep reading the arm
        // before "fixing" this back to 5120.
        assert_eq!(req(Some(64), Some("low"))["max_tokens"], 4160);

        // ── overflow and the no-thinking baseline ─────────────────────────
        // `u32::MAX` saturates into the ceiling rather than wrapping (the
        // `saturating_add` in the caller-cap arm) and does not panic at any
        // level.
        assert_eq!(
            req(Some(u32::MAX), None)["max_tokens"],
            ANTHROPIC_OUTPUT_CEILING_TOKENS
        );
        let maxed = req(Some(u32::MAX), Some("max"));
        assert_eq!(maxed["max_tokens"], ANTHROPIC_OUTPUT_CEILING_TOKENS);
        assert_eq!(maxed["thinking"]["budget_tokens"], 47616);
        // NO `reasoning_effort` + NO `max_tokens` → the ceiling, NO
        // `thinking` field.
        let body = req(None, None);
        assert_eq!(body["max_tokens"], ANTHROPIC_OUTPUT_CEILING_TOKENS);
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
        // `max` is the ONE assertion here that moved: the level table says
        // 128,000 and the WIRE says 47,616. The clamp lives in the body
        // builder (via `anthropic_output_budget`), not in `anthropic_thinking`,
        // so this test is where the difference between the two becomes
        // visible — the table keeps the family's own number, the ceiling
        // decides what is actually sent.
        assert_eq!(
            req(Some("max"))["thinking"],
            json!({ "type": "enabled", "budget_tokens": 47616 })
        );
        // An unrecognized non-empty level → the `adaptive` mode (no budget).
        assert_eq!(
            req(Some("weird"))["thinking"],
            json!({ "type": "adaptive" })
        );
        // `None` → the field is ABSENT.
        assert!(req(None).get("thinking").is_none());
    }

    /// The FLOOR is the point of the change, so this pins the *invariant*
    /// (the answer's room) driven over the level list, rather than five
    /// hand-copied numbers.
    ///
    /// Scope note, deliberately narrow: this is the **no-caller-cap** arm
    /// only. In the caller-cap arm the caller's own cap IS the answer room, so
    /// the floor is not a claim about that arm at all — titling's `Some(100)`
    /// is correct and must not become 16,384, or titling would be asked to
    /// write 16,384 tokens of title. The invariant that spans BOTH arms is
    /// [`the_wire_never_asks_for_more_than_the_ceiling_and_always_outranks_its_budget`].
    #[test]
    fn the_answer_floor_holds_under_every_level() {
        // Driven over the levels, not five copies of the same assertion.
        for level in ["low", "medium", "high", "xhigh", "max"] {
            let (max_tokens, thinking) =
                anthropic_output_budget(None, Some(anthropic_thinking(level)));
            let budget = thinking
                .as_ref()
                .and_then(|t| t.get("budget_tokens"))
                .and_then(|b| b.as_u64())
                .expect("every named level carries a budget at this ceiling");
            // `saturating_sub` so a broken clamp fails the ASSERTION with this
            // message instead of panicking on u32 underflow — the failure names
            // the level and both numbers, which is what a reader needs.
            let answer_room = max_tokens.saturating_sub(budget as u32);
            assert!(
                answer_room >= MIN_ANSWER_TOKENS,
                "level {level:?}: max_tokens {max_tokens} minus budget {budget} \
                 leaves only {answer_room} answer tokens, under the \
                 {MIN_ANSWER_TOKENS}-token floor"
            );
        }
    }

    /// The ONE invariant that holds across **both** arms of
    /// [`anthropic_output_budget`]: the wire never asks for more than the
    /// ceiling, and it always outranks its own budget (Anthropic requires
    /// **strict** `max_tokens > budget_tokens` — a tie is a 400).
    ///
    /// Driven through the pure switch (`anthropic_output_budget(cap, ...)`)
    /// rather than the body builder, so no request shape is involved, and over
    /// **both** driver shapes — they do not reach the same inputs:
    ///
    /// **16 caps × 9 budgets = 144** — the plan's exhaustive legality set. The
    /// numeric budgets are turned into `thinking` objects DIRECTLY here, not
    /// driven through [`anthropic_thinking`]: the level table can only ever
    /// produce five budgets (`{4096, 10000, 20000, 40000, 128000}`), so the
    /// clamp's own boundaries are unreachable from a level name. `1024`
    /// (exactly Anthropic's legal minimum), `47616` (exactly `room`) and
    /// `47617` (one over `room`) are three of the nine and none of the five.
    ///
    /// **16 caps × 7 levels = 112** — the set reachable from a level NAME, kept
    /// because it is the only shape that produces the `adaptive` object (an
    /// unrecognized level → `{"type":"adaptive"}` with **no** `budget_tokens`),
    /// the case that must pass through untouched rather than be mistaken for a
    /// zero budget.
    ///
    /// The `>` half is read off the returned object's `budget_tokens` KEY in
    /// both loops: the `adaptive` mode carries no such key, so it is exempt
    /// **by construction, not by a special case** — there is no
    /// `if level == "weird"` here, and adding a budget to `adaptive` would
    /// automatically bring it under the assertion.
    ///
    /// Deliberately NOT the floor: see the scope note on
    /// [`the_answer_floor_holds_under_every_level`].
    ///
    /// What this invariant CANNOT see, so nobody mistakes 256 inputs for full
    /// coverage of the clamp: both halves constrain only the PAIR's ordering
    /// and the ceiling, never the budget's VALUE. Enumerating every shipped
    /// budget the code could emit against every cap, the two halves are
    /// violated **iff a budget ≥ the ceiling reaches the wire** — so a clamp
    /// that returns `min(budget, MIN_ANSWER_TOKENS)` instead of
    /// `min(budget, room)`, one that returns `min(budget, room - 1)`, or one
    /// that raises small budgets to 1,024 all stay legal at every one of these
    /// 256 inputs and pass here. That is not a gap to plug by weakening an
    /// assertion; it is why [`the_clamp_shrinks_and_only_drops_when_no_legal_budget_fits`]
    /// and the two body-builder tests pin the shipped VALUES (47,616 and the
    /// 128,000 → 47,616 shrink) directly. What this sweep owns is legality
    /// across the cross-product, and the 144 half is what proves no cap/budget
    /// combination is illegal — including the three the level vocabulary can
    /// never generate.
    #[test]
    fn the_wire_never_asks_for_more_than_the_ceiling_and_always_outranks_its_budget() {
        // The caps span both arms' interesting edges: `None` (the ceiling
        // arm), 0 and 1 (the `c.max(1)` guard), the real caller caps
        // (`64`-ish titling, `100`, `8192`-or-less summarising), the values
        // the constants derive to (13,312 / 17,408 / 47,616), the ceiling
        // itself, and values above it including `u32::MAX` (the saturating
        // add).
        let caps: [Option<u32>; 16] = [
            None,
            Some(0),
            Some(1),
            Some(64),
            Some(100),
            Some(4096),
            Some(5000),
            Some(8192),
            Some(10000),
            Some(13312),
            Some(17408),
            Some(20000),
            Some(47616),
            Some(64000),
            Some(100000),
            Some(u32::MAX),
        ];
        // The two halves, shared by both sweeps so neither loop can drift
        // away from the other. `driver` names what fed the switch so a failure
        // points at the input, not just at the numbers.
        let check_legal =
            |cap: Option<u32>, driver: String, max_tokens: u32, thinking: &Option<Value>| {
                // Half one: the ceiling is never exceeded, in either arm.
                assert!(
                    max_tokens <= ANTHROPIC_OUTPUT_CEILING_TOKENS,
                    "cap {cap:?} {driver}: max_tokens {max_tokens} exceeds \
                 the ceiling {ANTHROPIC_OUTPUT_CEILING_TOKENS}"
                );
                // Half two: strictly greater than the budget it ships. The
                // `if let` on the KEY is the whole exemption story.
                if let Some(budget) = thinking
                    .as_ref()
                    .and_then(|t| t.get("budget_tokens"))
                    .and_then(|b| b.as_u64())
                {
                    assert!(
                        max_tokens as u64 > budget,
                        "cap {cap:?} {driver}: max_tokens {max_tokens} does \
                     not outrank budget {budget} (Anthropic requires strict \
                     `max_tokens > budget_tokens`; a tie is a 400)"
                    );
                }
            };

        // ── Sweep one: the plan's exhaustive budget cross-product ─────────
        //
        // The numeric budgets are turned into `thinking` objects HERE, not
        // driven through `anthropic_thinking`, because the level table can only
        // produce five budgets — `{4096, 10000, 20000, 40000, 128000}` — and so
        // can never reach the clamp's own boundaries: `1024` (exactly
        // Anthropic's legal minimum), `47616` (exactly `room`, the inclusive
        // edge) and `47617` (one over `room`, the shrink edge). A sweep driven
        // by level name silently omits all three.
        let budgets: [Option<u32>; 9] = [
            None,
            Some(1024),
            Some(4096),
            Some(10000),
            Some(20000),
            Some(40000),
            Some(47616),
            Some(47617),
            Some(128000),
        ];
        let mut budget_inputs = 0usize;
        for cap in caps {
            for budget in budgets {
                budget_inputs += 1;
                // `None` is no `thinking` at all; a number is the `enabled`
                // object the wire would carry, built directly.
                let thinking = budget.map(|b| json!({ "type": "enabled", "budget_tokens": b }));
                let (max_tokens, thinking) = anthropic_output_budget(cap, thinking);
                check_legal(cap, format!("budget {budget:?}"), max_tokens, &thinking);
            }
        }
        // 16 caps × 9 budgets = 144: the plan's exhaustive legality set, the
        // one its claim rests on. Pinned so a dropped cap or budget is visible
        // rather than silently narrowing coverage.
        assert_eq!(budget_inputs, caps.len() * budgets.len());
        assert_eq!(budget_inputs, 144);

        // ── Sweep two: the real level vocabulary ──────────────────────────
        // The full level vocabulary, `None` (no `thinking` at all) and the
        // unrecognized level (→ `adaptive`) included. Kept even though it is
        // narrower than sweep one on budgets, because it is the ONLY shape that
        // produces the `adaptive` object — an unrecognized level yields
        // `{"type":"adaptive"}` with no `budget_tokens` key, the case that must
        // pass through untouched rather than be read as a zero budget.
        let levels: [Option<&str>; 7] = [
            None,
            Some("low"),
            Some("medium"),
            Some("high"),
            Some("xhigh"),
            Some("max"),
            Some("weird"),
        ];
        let mut level_inputs = 0usize;
        for cap in caps {
            for level in levels {
                level_inputs += 1;
                let thinking = level.map(anthropic_thinking);
                let (max_tokens, thinking) = anthropic_output_budget(cap, thinking);
                check_legal(cap, format!("level {level:?}"), max_tokens, &thinking);
            }
        }
        // 16 caps × 7 levels = 112: the set reachable from a level NAME. Also
        // pinned — sweep one does not subsume this one (it cannot make
        // `adaptive`), and this one does not subsume sweep one (it cannot make
        // 1024 / 47616 / 47617).
        assert_eq!(level_inputs, caps.len() * levels.len());
        assert_eq!(level_inputs, 112);
    }

    /// `clamp_thinking_budget` pinned DIRECTLY, separate from the body's view
    /// of it. It is a pure `u32 → Option<u32>`, so every interesting case is
    /// reachable here without a `ModelRequest` — and pinning the helper apart
    /// from the wire is what stops a future reader "fixing" the level table
    /// (which still says 128,000 for `max`) instead of reading the clamp.
    #[test]
    fn the_clamp_shrinks_and_only_drops_when_no_legal_budget_fits() {
        // PASS THROUGH, under the room: nothing to do.
        assert_eq!(clamp_thinking_budget(40_000), Some(40_000));
        // PASS THROUGH, EXACTLY at the room — the comparison is inclusive.
        assert_eq!(clamp_thinking_budget(47_616), Some(47_616));
        // SHRINK, one token over.
        assert_eq!(clamp_thinking_budget(47_617), Some(47_616));
        // SHRINK, not DROP: the `max` level's 128,000 comes down to the room.
        assert_eq!(clamp_thinking_budget(128_000), Some(47_616));

        // THE DROP ARM HAS NO ASSERTION, AND MUST NOT GET ONE. It fires only
        // when `room` = ceiling − MIN_ANSWER_TOKENS falls under
        // MIN_THINKING_BUDGET_TOKENS, i.e. when the ceiling is below
        // 16,384 + 1,024 = 17,408. It is a function of the CEILING ALONE — no
        // caller cap and no thinking level can reach it, because the clamp has
        // already run by the time the arms split. `room` derives from two
        // `const`s, so the ceiling is not a parameter and the arm cannot be
        // made reachable from a test either. It exists for the day per-model
        // ceilings land. Do NOT fake liveness here: asserting
        // `40_000 → None` at a 64,000 ceiling fails, and weakening such a test
        // until it passes would make the drop look real when it is not.
        //
        // The mode that SHARES the `None`-budget path (a `thinking` object
        // with no `budget_tokens`) does have a live witness, and it is the one
        // in `anthropic_request_body_thinking_map`:
        // `req(Some("weird"))["thinking"] == json!({"type":"adaptive"})`. It
        // proves a budget-less `thinking` object passes through the switch
        // untouched rather than being mistaken for a zero budget and dropped.
        //
        // That witness runs with `max_tokens: None`, so it only covers the
        // no-cap arm. This is the CALLER-CAP half of the pass-through the
        // sweep's docblock claims but cannot assert (the sweep checks only
        // legality — ordering and the ceiling — never pass-through): an
        // `adaptive` object under a real cap must come back byte-identical,
        // not read as a zero budget and dropped.
        assert_eq!(
            anthropic_output_budget(Some(8_192), Some(json!({ "type": "adaptive" }))),
            (8_192, Some(json!({ "type": "adaptive" })),),
            "a caller cap must not disturb the `adaptive` pass-through"
        );
    }

    // ── prompt caching on Claude models (T3b / #131) ────────────────────

    /// A request for ONE model with an optional system + optional tools.
    fn cache_request(model: &str, system: Option<&str>, tools: usize) -> Value {
        let mut messages = Vec::new();
        if let Some(s) = system {
            messages.push(chat_message(
                ChatRole::System,
                MessageContent::Text(s.to_string()),
                None,
                None,
            ));
        }
        messages.push(chat_message(
            ChatRole::User,
            MessageContent::Text("hi".to_string()),
            None,
            None,
        ));
        let r = ModelRequest {
            model: model.to_string(),
            messages,
            tools: (0..tools)
                .map(|i| ToolSpec {
                    name: format!("tool_{i}"),
                    description: "d".to_string(),
                    parameters: json!({ "type": "object" }),
                })
                .collect(),
            options: ModelOptions {
                temperature: None,
                max_tokens: Some(64),
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        anthropic_request_body(&r)
    }

    /// (acceptance 1) A Claude model with tools + a system: the `system`
    /// becomes an ARRAY of text blocks with `cache_control` on the LAST block,
    /// and EVERY tool block carries `cache_control`.
    #[test]
    fn anthropic_request_body_claude_model_caches_system_and_tools() {
        let body = cache_request("claude-3-5-sonnet", Some("sys one\n\nsys two"), 2);
        let system = body["system"]
            .as_array()
            .expect("the cached system is an ARRAY of blocks");
        assert_eq!(system.len(), 2, "one block per paragraph chunk");
        assert_eq!(system[0]["text"], "sys one");
        assert!(
            system[0].get("cache_control").is_none(),
            "only the LAST block carries the cache point"
        );
        assert_eq!(system[1]["text"], "sys two");
        assert_eq!(system[1]["cache_control"], json!({ "type": "ephemeral" }));
        for i in 0..2 {
            assert_eq!(
                body["tools"][i]["cache_control"],
                json!({ "type": "ephemeral" }),
                "every tool block is part of the cached prefix"
            );
            assert_eq!(body["tools"][i]["name"], format!("tool_{i}"));
        }
    }

    /// The cap: a system beyond `MAX_SYSTEM_BLOCKS` chunks caps the BLOCK
    /// COUNT at the const, the exceeding text is FOLDED into the last block
    /// (nothing dropped), and the last block still carries the cache point.
    #[test]
    fn anthropic_request_body_max_system_blocks_cap_holds() {
        // MAX_SYSTEM_BLOCKS + 5 paragraphs (each distinct so the fold is
        // observable in the text).
        let paragraphs: Vec<String> = (0..(MAX_SYSTEM_BLOCKS + 5))
            .map(|i| format!("para-{i}"))
            .collect();
        let system = paragraphs.join("\n\n");
        let body = cache_request("claude-3-5-sonnet", Some(&system), 0);
        let blocks = body["system"]
            .as_array()
            .expect("the cached system is an array");
        assert_eq!(blocks.len(), MAX_SYSTEM_BLOCKS, "the block count caps");
        // NOTHING dropped: every paragraph survives, the tail folded into the
        // last block (joined back with the separator it was split on).
        let joined = blocks
            .iter()
            .map(|b| b["text"].as_str().expect("a text block"))
            .collect::<Vec<_>>()
            .join("\n\n");
        assert_eq!(joined, system, "no text is dropped by the fold");
        assert_eq!(
            blocks[MAX_SYSTEM_BLOCKS - 1]["cache_control"],
            json!({ "type": "ephemeral" })
        );
        // The fold holds the tail: the LAST kept block starts at the first
        // paragraph the cap cut, and holds every paragraph after it.
        let last = blocks[MAX_SYSTEM_BLOCKS - 1]["text"]
            .as_str()
            .expect("a text block");
        assert!(
            last.starts_with(&format!("para-{}", MAX_SYSTEM_BLOCKS - 1)),
            "the last kept block starts at the cap boundary: {last}"
        );
        assert!(
            last.contains(&format!("para-{}", MAX_SYSTEM_BLOCKS + 4)),
            "the last kept block folds EVERY paragraph past the cap: {last}"
        );
    }

    /// (branch (a)) A NON-Claude model keeps today's shape byte-identically:
    /// the plain-string `system`, flat tools, NO `cache_control` anywhere.
    #[test]
    fn anthropic_request_body_non_claude_model_is_byte_identical() {
        let body = cache_request("gpt-4o", Some("sys"), 1);
        assert_eq!(
            body["system"],
            json!("sys"),
            "the system stays a plain string"
        );
        assert!(body["tools"][0].get("cache_control").is_none());
        // The whole body carries NO cache_control anywhere.
        assert!(
            !body.to_string().contains("cache_control"),
            "no caching on a non-Claude model: {body}"
        );
    }

    /// (branch (b)) A Claude model with NO tools AND NO system text: the
    /// pre-existing shape (no `system` key at all, no `tools`, no
    /// `cache_control`) — byte-identical to today.
    #[test]
    fn anthropic_request_body_claude_without_tools_keeps_string_system() {
        let body = cache_request("claude-3-5-sonnet", None, 0);
        assert!(body.get("system").is_none(), "an empty system emits no key");
        assert!(body.get("tools").is_none());
        assert!(
            !body.to_string().contains("cache_control"),
            "the no-tools empty-system case caches nothing: {body}"
        );
    }

    /// (branch (c)) A Claude model with NO tools but NON-EMPTY system text:
    /// the system IS the cached array (the caching condition is tools OR
    /// system — no hidden second gate).
    #[test]
    fn anthropic_request_body_claude_system_only_caches() {
        let body = cache_request("claude-3-5-sonnet", Some("only system"), 0);
        let system = body["system"]
            .as_array()
            .expect("the system-only branch still caches");
        assert_eq!(system.len(), 1);
        assert_eq!(system[0]["cache_control"], json!({ "type": "ephemeral" }));
        assert!(body.get("tools").is_none());
    }

    /// The family predicate's truth table (a `claude-` prefix check — the
    /// model id survives LiteLLM routing verbatim).
    #[test]
    fn is_claude_model_matches_the_family() {
        for model in [
            "claude-3-5-sonnet",
            "claude-opus-4",
            "Claude-3-Haiku",
            "claude-3-7-sonnet",
        ] {
            assert!(is_claude_model(model), "{model}");
        }
        for model in ["gpt-4o", "m", "sonnet-3-claude", "claude", "lit/claude-x"] {
            assert!(!is_claude_model(model), "{model} is NOT the family");
        }
    }
}

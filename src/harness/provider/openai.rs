//! The OpenAI-compatible chat-completions stack: `OpenAiCompatibleProvider`,
//! its request-body mapper, the `SseStream` line/chunk parser, and the shared
//! raw-bytes `LineAssembler` (`pub(super)` so the other SSE parsers in
//! this module still use it).
//!
//! The wire is frozen (ADR 0024); the transport discipline (client,
//! status map, LiteLLM headers) lives in [`super::transport`].

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use serde_json::json;
use serde_json::Value;

use super::transport::{build_client, litellm_headers, map_response};
use super::types::{
    cost_usd_from_usage, parse_error_chunk, ChatRole, FinishReason, MessageContent, ModelRequest,
    Provider, ProviderError, ProviderEvent, ToolCall, ToolCallDelta, Usage, WireToolCall,
};

// ── (issue #122) the shared sanitisation-placeholder vocabulary ───────────
//
// ONE definition of the upstream literal, read by the inbound SSE filter below,
// by the outbound `request_body` guard, and — through their own imports — by
// the loop's `consume_stream` bolt and the Worker's three guards, so no copy
// can drift.
//
// This module is the home for it (not `harness/loop/turn.rs`, which the plan
// proposed): `mod turn` is PRIVATE to `harness::loop`, so a definition there is
// unreachable from the wire and the Worker without widening `loop.rs` — a file
// outside this task's file set. `openai.rs` is the crate-visible seam the other
// SSE parsers already import from (`LineAssembler`, `parse_arguments`), and
// ADR 0054 puts the wire vocabulary on the wire anyway.

/// (issue #122) The literal placeholder LiteLLM's `_sanitize_empty_text_content`
/// (`factory.py`, `modify_params=True`) substitutes for empty/whitespace string
/// content. It is PRODUCED UPSTREAM — this repo's transcripts only ever *echo*
/// it, and grep across `src/` and `src-tauri/src/` finds zero producers — so
/// both the inbound stream filter and the outbound `request_body` guard key on
/// this ONE definition.
pub(crate) const EMPTY_CONTENT_SANITISE_PLACEHOLDER: &str =
    "[System: Empty message content sanitised to satisfy protocol]";

/// True when `text` IS the upstream sanitisation placeholder and nothing else —
/// optionally wrapped in whitespace or in mention-ish `*` / `` ` `` decoration (a
/// client that renders the banner as markdown emphasis).
///
/// EXACT-MATCH ONLY, never a substring: a longer line that merely *quotes* the
/// banner ("what does `[System: … protocol]` mean?") is real text and must
/// stream untouched.
///
/// This is the predicate the two STREAMING drop sites use — the loop's
/// `consume_stream` and the SSE parser's accumulator. Those sites deliberately
/// do NOT use [`content_is_effectively_empty`]: a whitespace-only delta (`" "`,
/// `"\n"`) is a legitimate chunk that providers really send between words, and
/// dropping it would silently weld words and paragraphs together in the
/// transcript. Only the placeholder is dropped; empty stays empty.
pub(crate) fn content_is_the_sanitise_placeholder(text: &str) -> bool {
    let trimmed = text.trim();
    !trimmed.is_empty()
        && trimmed.replace(['*', '`'], "").trim() == EMPTY_CONTENT_SANITISE_PLACEHOLDER
}

/// True when a WHOLE content value is nothing anyone ever meant to say:
/// empty / whitespace-only, or the upstream sanitisation placeholder.
///
/// Used where the value is a message's entire content and "there is no content
/// here" is the decision to make — the outbound `request_body` guard (an empty
/// assistant `content` is exactly what LiteLLM rewrites, so it must never be
/// sent) and the subagent guards (a child that produced nothing is not a
/// `Completed`). NOT for the streaming drop sites — see
/// [`content_is_the_sanitise_placeholder`].
pub(crate) fn content_is_effectively_empty(text: &str) -> bool {
    text.trim().is_empty() || content_is_the_sanitise_placeholder(text)
}

/// A client for the OpenAI-compatible chat-completions API (OpenAI,
/// OpenRouter, `tama`, …).
#[derive(Debug)]
pub struct OpenAiCompatibleProvider {
    /// The API base (e.g. `https://openrouter.ai/api/v1` — the client
    /// appends `/chat/completions`).
    pub base_url: String,
    /// The `Authorization: Bearer` key.
    pub api_key: String,
    /// The `stream_options.include_usage` LATCH (the `stream_options`
    /// retry-and-latch): `true` until a request carrying the field is
    /// REJECTED for mentioning it — from then on every request omits it.
    /// Internal per-instance state (NOT a knob), hence not `pub`.
    accepts_stream_options: AtomicBool,
    /// The provider's expanded + validated CUSTOM headers (R1, issue #129) —
    /// applied by `build_client` as `default_headers` on every completion
    /// request. `None` = no custom headers (the byte-identical default).
    pub custom_headers: Option<std::collections::BTreeMap<String, String>>,
}

impl OpenAiCompatibleProvider {
    /// The production constructor (a fresh client — usage reporting ON
    /// until this instance learns otherwise; the latch is PER INSTANCE, so
    /// one hostile proxy does not disable usage for the whole process).
    /// The `None`-headers shorthand: no custom headers (issue #129).
    pub fn new(base_url: String, api_key: String) -> Self {
        Self {
            base_url,
            api_key,
            accepts_stream_options: AtomicBool::new(true),
            custom_headers: None,
        }
    }

    /// The header-carrying constructor (issue #129, R1): the same client as
    /// [`Self::new`], but the provider's EXPANDED + VALIDATED custom-header
    /// map rides every completion request via `build_client`'s
    /// `default_headers`. A struct literal cannot replace this — the private
    /// `accepts_stream_options` latch is set only by these constructors.
    pub fn with_custom_headers(
        base_url: String,
        api_key: String,
        custom_headers: Option<std::collections::BTreeMap<String, String>>,
    ) -> Self {
        Self {
            base_url,
            api_key,
            accepts_stream_options: AtomicBool::new(true),
            custom_headers,
        }
    }

    /// The `stream_options` retry-and-latch (the `complete()`'s status
    /// gate): the FIRST streaming call asks for `stream_options.include_usage`
    /// (an OpenAI-compatible stream carries NO `usage` chunk unless asked —
    /// and a usage-less session freezes the context bar and can never trip
    /// compaction). A FEW strict proxies reject the unknown field with a
    /// 400, which would otherwise be a `Fatal` on EVERY model call — so on
    /// a rejection that MENTIONS the field, re-issue once WITHOUT it and
    /// latch the omission for this instance's lifetime.
    ///
    /// The gate is the error's MESSAGE as well as its tier (the body, which
    /// `map_response` embeds verbatim): an unrelated 400 (a bad model, a
    /// malformed body) stays the `Fatal` it always was and is NOT re-tried.
    /// A false positive — a 400 whose text happens to name the field for
    /// another reason — costs one extra request and then loses usage
    /// reporting for this session, where the loop's local estimate takes
    /// over. The ONE tier that is never eligible is `ContextOverflow` (ADR
    /// 0045): its body names `stream_options` as often as not (a proxy that
    /// echoes the request it rejected), and re-issuing the oversized prompt
    /// cannot succeed.
    async fn send_model_request(
        &self,
        client: &reqwest::Client,
        url: &str,
        req: &ModelRequest,
    ) -> Result<reqwest::Response, ProviderError> {
        let builder = |body: &Value| {
            let mut b = client.post(url).bearer_auth(&self.api_key);
            for (name, value) in litellm_headers(req.session_id.as_deref()) {
                b = b.header(name, value);
            }
            b.json(body)
        };
        let stream_options =
            req.options.stream && self.accepts_stream_options.load(Ordering::Relaxed);
        let resp = builder(&request_body(req, stream_options))
            .send()
            .await
            .map_err(|e| ProviderError::Retryable(format!("request failed: {e}")))?;
        let err = match map_response(resp).await {
            Ok(resp) => return Ok(resp),
            Err(e) => e,
        };
        // The retry is gated on BOTH halves: a client error (a 4xx →
        // `Fatal` — a 429/5xx is the `Retryable` the `RetryPolicy` already
        // handles, and re-issuing here would skip its backoff) that NAMES
        // the field. The overflow tier is excluded BY NAME (ADR 0045), not
        // merely by not being a `Fatal`: re-issuing an oversized prompt just
        // re-sends the oversized bytes, and latching would drop usage
        // reporting for the whole session — the very signal the context bar
        // and the compaction threshold read. Naming the tier here keeps that
        // true whatever the tier gate becomes.
        if !stream_options
            || matches!(err, ProviderError::ContextOverflow(_))
            || !matches!(err, ProviderError::Fatal(_))
            || !is_stream_options_rejection(&err)
        {
            return Err(err);
        }
        // A usage-request rejection is permanent for this endpoint — latch
        // it BEFORE the retry (the retry itself carries no field, so a
        // SECOND rejection cannot re-latch from here).
        self.accepts_stream_options.store(false, Ordering::Relaxed);
        let resp = builder(&request_body(req, false))
            .send()
            .await
            .map_err(|e| ProviderError::Retryable(format!("request failed: {e}")))?;
        map_response(resp).await
    }
}

/// The accumulated per-`index` tool-call state (fragments arrive as
/// `id` / `name` / `arguments` pieces).
#[derive(Default)]
struct ToolAcc {
    id: Option<String>,
    name: Option<String>,
    /// The argument fragments concatenated (JSON text).
    arguments: String,
}

/// Assembles raw response bytes into complete `\n`-terminated lines
/// (shared by ALL the SSE parsers — the OpenAI [`SseStream`] and the two
/// `event:`-tagged ones, [`AnthropicStream`] / [`ResponsesStream`]). The
/// buffer holds RAW BYTES (a network chunk may split a multi-byte
/// codepoint; a per-chunk `from_utf8_lossy` would replace each half with
/// U+FFFD before line assembly — only COMPLETE lines are converted;
/// finding 9).
pub(super) struct LineAssembler {
    /// The not-yet-`\n`-terminated tail of the response (RAW bytes — an
    /// incomplete trailing multi-byte sequence is held back between
    /// chunks and only converted once its line is complete).
    buf: Vec<u8>,
}

impl LineAssembler {
    pub(super) fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Append a raw chunk; return every complete line it contains.
    ///
    /// (`drain(..=pos)`, NOT `split_off`: `split_off(at)` returns the
    /// portion AFTER `at` and keeps the before-portion in `self` — the
    /// exact opposite of what is needed here.)
    pub(super) fn push(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(chunk);
        let mut lines = Vec::new();
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=pos).collect();
            lines.push(String::from_utf8_lossy(&line).into_owned());
        }
        lines
    }

    /// The not-yet-`\n`-terminated tail (a stream end — a tail that ends
    /// mid-codepoint is truncated on the wire; the lossy conversion is
    /// the only option there). `None` when nothing is buffered.
    pub(super) fn finish(&mut self) -> Option<String> {
        if self.buf.is_empty() {
            return None;
        }
        // `mem::take`, not `drain(..).collect()` — same result (the whole
        // buffer handed over, `buf` left empty) without the extra
        // allocation (`clippy::drain_collect`, rustc 1.99).
        let line = std::mem::take(&mut self.buf);
        Some(String::from_utf8_lossy(&line).into_owned())
    }
}

/// The SSE line/chunk parser: turns raw response bytes into
/// [`ProviderEvent`]s (content / reasoning / tool-call deltas,
/// `finish_reason` → `Done`, `usage` → `Usage`).
pub(super) struct SseStream {
    /// The raw byte stream (`reqwest::bytes_stream`, mapped to `Vec<u8>`).
    raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>>,
    /// The raw-bytes line assembler (the multi-byte-codepoint safety —
    /// finding 9).
    lines: LineAssembler,
    /// Events parsed but not yet delivered.
    pending: VecDeque<ProviderEvent>,
    /// Per-`index` tool-call accumulation (flushed as `ToolCall`s at
    /// `finish_reason` / stream end).
    tool_acc: BTreeMap<u32, ToolAcc>,
    /// A `finish_reason` chunk was seen (no synthetic `Done` at end).
    finished: bool,
    /// A combined `Usage` was emitted (exactly once — the trailing
    /// `usage` chunk, the first one to carry BOTH token counts).
    usage_emitted: bool,
    /// The raw stream is exhausted (the stream ends after `pending`).
    exhausted: bool,
    /// (issue #122) The run of `delta.content` text held back from the event
    /// stream because it is still a STRICT PREFIX of the upstream sanitisation
    /// placeholder — it might be the placeholder arriving split across chunks.
    ///
    /// Bounded by construction: it only ever holds a strict prefix of the
    /// placeholder (at most `EMPTY_CONTENT_SANITISE_PLACEHOLDER.len() - 1`
    /// bytes) plus the ONE chunk that either completes it (→ dropped) or
    /// diverges from it (→ released as a single `TextDelta`).
    ///
    /// It is flushed (released or dropped) at EVERY site that ends the text
    /// run — a `finish_reason`, a transport error, a non-text event that must
    /// keep its order, and the stream tail — so `poll_next` never returns
    /// `None` while text is held, and nothing held is ever LOST.
    stream_text_accum: String,
}

impl SseStream {
    pub(super) fn new(raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>>) -> Self {
        Self {
            raw,
            lines: LineAssembler::new(),
            pending: VecDeque::new(),
            tool_acc: BTreeMap::new(),
            finished: false,
            usage_emitted: false,
            exhausted: false,
            stream_text_accum: String::new(),
        }
    }

    /// Append a raw chunk; process every complete line it contains.
    fn push(&mut self, chunk: &[u8]) {
        for line in self.lines.push(chunk) {
            self.handle_line(&line);
        }
    }

    /// Handle one SSE line (`data: <json>`; `:`-comments and other
    /// fields are ignored, as is `data: [DONE]`).
    fn handle_line(&mut self, line: &str) {
        let Some(payload) = line.trim().strip_prefix("data:") else {
            return;
        };
        let payload = payload.trim();
        if payload.is_empty() || payload == "[DONE]" {
            return;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        self.handle_chunk(&chunk);
    }

    /// Handle one parsed chunk (a `choices[0].delta` + optional
    /// top-level `usage`; an `error` chunk is classified first — see the
    /// error arm below).
    fn handle_chunk(&mut self, v: &Value) {
        // (issue #122) The ONLY chunk that may extend the held run is one that
        // carries `delta.content`. Any OTHER chunk (`usage`, a `reasoning`
        // fragment, `tool_calls`, a `finish_reason`) ends the text run, so the
        // held prefix is released (or dropped) BEFORE it is handled — otherwise
        // a held `TextDelta` would overtake events that arrived after it. For
        // the ordinary stream this is a no-op: nothing is ever held unless the
        // run so far is a strict prefix of the sanitisation placeholder.
        //
        // The `content` chunk itself is NOT flushed here — that is the whole
        // point of the hold (the chunk that COMPLETES a split placeholder
        // usually also carries the `finish_reason`, so flushing first would
        // emit the placeholder before it could be recognised).
        let delta_text = v
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("delta"))
            .and_then(|d| d.get("content"))
            .and_then(Value::as_str);
        if delta_text.is_none() {
            self.flush_stream_text();
        }
        // A trailing chunk may carry `usage` with an empty `choices`.
        // The wire `u64` token counts saturate to `u32::MAX` (a silent
        // `as u32` would TRUNCATE — a 4 GB token count would wrap to a
        // small one).
        if let Some(u) = v.get("usage").and_then(|u| {
            u.get("prompt_tokens")
                .and_then(|t| t.as_u64())
                .zip(u.get("completion_tokens").and_then(|t| t.as_u64()))
        }) {
            // The GATE above is unchanged and deliberate: both counts must
            // be present, so a chunk carrying a cost and no token pair
            // emits NO event — a cost-only emission path would make this a
            // SECOND `Usage` producer and hand T09 money detached from the
            // counts D4 pairs it with (pinned by
            // `sse_cost_only_usage_chunk_emits_no_usage_event`).
            // The money rides the SAME usage object the counts were read
            // from (D4, AMENDED: the body, not a header). Purely ADDITIVE
            // — the token arithmetic above is untouched.
            //
            // Exactly-once (the same `#108` defect class the anthropic
            // `usage_emitted` guard fixes): the trailing `usage` chunk is
            // emitted ONCE — a proxy re-sending it must not push a second
            // frame the roll-up would sum as money that was not spent. This
            // guard WRAPS the push, it does NOT early-return:
            // `handle_chunk` continues past this block to the
            // `choices[0].delta` handling and the `finish_reason`
            // flush+`Done`, and a chunk that carries BOTH `usage` and a
            // delta/finish_reason must still deliver them (pinned by
            // `sse_usage_emitted_once_from_two_chunks`).
            if !self.usage_emitted {
                self.usage_emitted = true;
                self.pending.push_back(ProviderEvent::Usage(
                    Usage {
                        input_tokens: u32::try_from(u.0).unwrap_or(u32::MAX),
                        output_tokens: u32::try_from(u.1).unwrap_or(u32::MAX),
                    },
                    v.get("usage").and_then(cost_usd_from_usage),
                ));
            }
        }
        // (Finding 1) An in-stream `error` chunk (the HTTP status was 200 —
        // the error is IN the stream: `data: {"error":{…}}`). Checked HERE,
        // BEFORE the `choices` deref below, because an error chunk carries no
        // `choices` at all: the deref used to `return` and the chunk was
        // discarded in silence, the stream end synthesized
        // `Done(FinishReason::Error)`, and `turn.rs` fabricated a `Retryable`
        // from it — 4 doomed ladder retries against a bad key, and ADR 0045's
        // mid-stream overflow recovery unreachable on this wire. Classified by
        // `parse_error_chunk` (types.rs) — the ONE classifier of the three
        // wires, so an auth body is the `Auth` tier here exactly as it is on
        // Anthropic, and the settings UI's auth keying works on every provider.
        //
        // AFTER the usage block ON PURPOSE: a chunk carrying BOTH `usage` and
        // `error` still delivers its `Usage` (the usage gate WRAPS its push, it
        // does not early-return, so the `usage_emitted` latch is not skipped).
        //
        // FLUSH SEAM (issue #122): the error ends the text run, so anything the
        // placeholder-prefix hold is keeping is released BEFORE the push —
        // mirroring the transport-error arm and the `finish_reason` flush seam:
        // nothing the hold keeps may overtake the error event.
        if let Some(e) = parse_error_chunk(v) {
            self.flush_stream_text();
            self.exhausted = true;
            self.pending.push_back(ProviderEvent::Error(e));
            return;
        }
        let Some(choice) = v.get("choices").and_then(|c| c.get(0)) else {
            return;
        };
        let Some(delta) = choice.get("delta") else {
            return;
        };
        // A single chunk can carry BOTH a `reasoning` fragment and a
        // `content` fragment (some providers batch the thinking's tail
        // with the text's head — the transition chunk). The thinking
        // block PRECEDES the text block in the model's output, so the
        // `ThinkingDelta` must be emitted BEFORE the `TextDelta` — the
        // reverse order would make the frontend start the text block
        // first and file the thinking's tail under a SECOND thinking
        // block (the "missing last word" bug).
        if let Some(thinking) = delta
            .get("reasoning")
            .or_else(|| delta.get("reasoning_content"))
            .and_then(|c| c.as_str())
        {
            // (issue #122) A `reasoning` fragment ends the text run: release
            // (or drop) anything held BEFORE pushing it, so the held text —
            // which arrived earlier on the wire — keeps its order.
            self.flush_stream_text();
            self.pending
                .push_back(ProviderEvent::ThinkingDelta(thinking.to_string()));
        }
        if let Some(text) = delta.get("content").and_then(|c| c.as_str()) {
            // (issue #122) THE FILTER OF RECORD for the upstream sanitisation
            // placeholder. This parser is the only seam that sees the WHOLE
            // `delta.content` stream, so it is the one place a placeholder
            // SPLIT across chunks can be reassembled and recognised — which is
            // what lets every other guard in this fix be an exact match
            // instead of a substring scan.
            //
            // The rule is a PREFIX HOLD, not a suffix guess (`ends_with(']')`
            // would hold legitimate streaming text): accumulate, and
            //   * if the run IS the placeholder → drop it (silenced at the
            //     parse seam: it never becomes a `TextDelta`, so the loop, the
            //     transcript and the UI never see it);
            //   * else if the run is still a STRICT PREFIX of the placeholder →
            //     hold it for the next chunk (bounded: at most
            //     `EMPTY_CONTENT_SANITISE_PLACEHOLDER.len() - 1` bytes);
            //   * else → release the whole run as ONE `TextDelta`, exactly as if
            //     it had streamed in a single chunk.
            //
            // Why this cannot eat real content: a genuine message either
            // diverges at the first character (released immediately, so the
            // common path is byte-identical to before) or diverges at the first
            // character that cannot continue the placeholder. Every delta is
            // released in arrival order; the only observable difference is that
            // a few leading chunks of a message that STARTS like the
            // placeholder coalesce into one `TextDelta` — and downstream only
            // appends to `acc.text`, so chunk granularity is invisible.
            //
            // RECORDED EXACT-MATCH TRADE-OFF: a text whose ENTIRE accumulated
            // run equals the placeholder is dropped here. That covers the
            // upstream notice, and also a model echoing the literal verbatim or
            // a proxied transcript replay of it. A user typing the placeholder
            // as their PROMPT is unaffected by this inbound seam (a prompt
            // travels outbound, out of the transcript), and a user QUOTING the
            // banner inside a longer line is untouched — such a run cannot be a
            // strict prefix of the placeholder, so it is never held.
            self.stream_text_accum.push_str(text);
            if content_is_the_sanitise_placeholder(&self.stream_text_accum) {
                self.stream_text_accum.clear();
            } else if self.stream_text_accum.len() < EMPTY_CONTENT_SANITISE_PLACEHOLDER.len()
                && EMPTY_CONTENT_SANITISE_PLACEHOLDER.starts_with(&self.stream_text_accum)
            {
                // A strict prefix — might be the split placeholder's head.
            } else {
                let held = std::mem::take(&mut self.stream_text_accum);
                self.pending.push_back(ProviderEvent::TextDelta(held));
            }
        }
        if let Some(calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
            // (issue #122) A tool call ends the text run (a chunk may carry
            // BOTH the tail of a held text prefix and its first `tool_calls`
            // fragment): release the held text BEFORE the tool-call events so
            // the arrival order survives.
            self.flush_stream_text();
            for call in calls {
                let Some(index) = call.get("index").and_then(|i| i.as_u64()) else {
                    continue;
                };
                let index = index as u32;
                let acc = self.tool_acc.entry(index).or_default();
                let mut partial = ToolCallDelta {
                    index,
                    id: None,
                    name: None,
                    arguments: None,
                };
                if let Some(id) = call.get("id").and_then(|i| i.as_str()) {
                    acc.id = Some(id.to_string());
                    partial.id = Some(id.to_string());
                }
                let function = call.get("function");
                if let Some(name) = function
                    .and_then(|f| f.get("name"))
                    .and_then(|n| n.as_str())
                {
                    // `name` fragments accumulate (the provider may split
                    // the name across chunks); `id` is set once.
                    match &mut acc.name {
                        Some(existing) => existing.push_str(name),
                        None => acc.name = Some(name.to_string()),
                    }
                    partial.name = Some(name.to_string());
                }
                if let Some(args) = function
                    .and_then(|f| f.get("arguments"))
                    .and_then(|a| a.as_str())
                {
                    acc.arguments.push_str(args);
                    partial.arguments = Some(args.to_string());
                }
                self.pending
                    .push_back(ProviderEvent::ToolCallDelta(partial));
            }
        }
        // A `finish_reason` closes the turn: flush the accumulated tool
        // calls first (the `AgentLoop` needs them before it sees the
        // `Done`), then the `Done` itself.
        if let Some(reason) = choice.get("finish_reason").and_then(|f| f.as_str()) {
            // (issue #122) FLUSH SEAM: a `finish_reason` ends the text run, so
            // anything the placeholder-prefix hold is keeping is released (or
            // dropped) HERE — BEFORE the tool calls and the `Done`, so the held
            // text keeps its position and nothing is lost when the stream ends
            // on this chunk. (A chunk that carries BOTH the completing content
            // fragment and the `finish_reason` reached this point with the run
            // already recognised — a completed placeholder was dropped by the
            // content block above, and a diverged run was already released.)
            self.flush_stream_text();
            self.flush_tool_calls();
            self.pending
                .push_back(ProviderEvent::Done(map_finish_reason(reason)));
            self.finished = true;
        }
    }

    /// FLUSH SEAM (issue #122): release or drop whatever the placeholder-prefix
    /// hold is keeping. Called at EVERY point that ends the text run — a chunk
    /// that carries no `delta.content` (so: `usage`, `reasoning`, `tool_calls`,
    /// a `finish_reason`), a transport error, and the stream tail — because a
    /// strict prefix of the placeholder can otherwise be held until the stream
    /// ends, and:
    ///
    /// * a real message that merely STARTS like the placeholder (e.g. `"["`)
    ///   must not lose its head — it is RELEASED here as one `TextDelta`;
    /// * a run that is the placeholder (or only whitespace around it) is DROPPED;
    /// * `poll_next` must never return `None` while text is held, and must never
    ///   park on `Poll::Pending` while the raw stream is done.
    ///
    /// The known trade-off is DELAY, never loss: a provider that stalls
    /// mid-prefix holds that run's text until the next chunk diverges it or the
    /// stream ends. The hold's SIZE is bounded by the placeholder's own length
    /// plus one chunk (see `stream_text_accum`).
    fn flush_stream_text(&mut self) {
        let held = std::mem::take(&mut self.stream_text_accum);
        if held.is_empty() || content_is_the_sanitise_placeholder(&held) {
            return;
        }
        self.pending.push_back(ProviderEvent::TextDelta(held));
    }

    /// Emit the accumulated tool calls as complete `ToolCall` events
    /// (dropping any index that never arrived with a `name`).
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

/// Map a wire `finish_reason` string to a [`FinishReason`] (an unknown
/// value is a normal stop — some providers use `content_filter`, …;
/// it is logged so a new provider-side value is diagnosable, not
/// silent).
fn map_finish_reason(reason: &str) -> FinishReason {
    match reason {
        "stop" => FinishReason::Stop,
        "tool_calls" => FinishReason::ToolCalls,
        "length" => FinishReason::Length,
        "error" => FinishReason::Error,
        _ => {
            eprintln!("harness: unknown finish_reason {reason:?} — treating as a stop");
            FinishReason::Stop
        }
    }
}

/// Parse accumulated argument text as JSON (an empty payload becomes
/// `{}` — a tool with no arguments; a MALFORMED payload also becomes
/// `{}`, but is logged — a silent `{}` would make the tool run with
/// no arguments and hide the provider-side corruption).
pub(super) fn parse_arguments(args: &str) -> Value {
    if args.trim().is_empty() {
        return Value::Object(Default::default());
    }
    match serde_json::from_str(args) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("harness: malformed tool-call arguments — using {{}}: {e}");
            Value::Object(Default::default())
        }
    }
}

impl Stream for SseStream {
    type Item = ProviderEvent;

    fn poll_next(self: Pin<&mut SseStream>, cx: &mut Context<'_>) -> Poll<Option<ProviderEvent>> {
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
                    // (issue #122) FLUSH SEAM: the failure ends the text run,
                    // so release (or drop) anything the placeholder-prefix hold
                    // is keeping BEFORE the `Error` event — the retry re-issues
                    // the call, but text that was already delivered must not be
                    // swallowed by the hold.
                    this.flush_stream_text();
                    this.exhausted = true;
                    this.pending
                        .push_back(ProviderEvent::Error(ProviderError::Retryable(format!(
                            "stream error: {e}"
                        ))));
                }
                Poll::Ready(None) => {
                    this.exhausted = true;
                    // The stream ended: process the last (un-`\n`ed) line,
                    // flush any tool calls, and synthesize a `Done` if the
                    // provider never sent a `finish_reason`. (A tail that
                    // ends mid-codepoint is truncated on the wire — the
                    // lossy conversion is the only option there.)
                    if let Some(tail) = this.lines.finish() {
                        this.handle_line(&tail);
                    }
                    // (issue #122) FLUSH SEAM: the raw stream is over, so this
                    // is the LAST chance for text the placeholder-prefix hold
                    // is keeping. A strict prefix that never became the
                    // placeholder (a real message that merely starts like it —
                    // `"["`) is released HERE as one `TextDelta`; a run that IS
                    // the placeholder is dropped. Without this the held head of
                    // a legitimate message would be silently LOST at stream end,
                    // and `poll_next` would return `None` with text still held.
                    this.flush_stream_text();
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

/// The accumulated per-block tool-call state for the Anthropic wire (a
/// `tool_use` content block: the `id` + `name` arrive at
/// `content_block_start`, the `input` JSON text accumulates as
/// `input_json_delta` fragments).

#[async_trait]
impl Provider for OpenAiCompatibleProvider {
    async fn complete(
        &self,
        req: &ModelRequest,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        // The request-level bounds (finding 4 — an IDLE-based bound, NOT a
        // TOTAL request bound): a `reqwest` `.timeout()` applies "from when
        // the request starts connecting until the response body has
        // finished" — it kills HEALTHY slow streams too (a legitimately
        // long streaming call — e.g. a 4k-token compaction summary at
        // 5-10 tok/s on a slow local OpenAI-compatible endpoint, the
        // harness's target deployment per ADR 0014 ≈ 7-14 min — is cut
        // mid-stream → `Retryable` → re-issued from scratch up to 5× →
        // compaction/turn permanently fails). Instead:
        // - `connect_timeout`: a generous CONNECT bound (the TCP connect +
        //   TLS handshake — a stalled connect ERRORS `Retryable`, it does
        //   not hang the turn until the user Stops).
        // - `read_timeout`: the time BETWEEN BYTES on the body (5 min of
        //   SILENCE = a stalled provider ERRORS `Retryable` — the
        //   `RetryPolicy`'s backoff; a healthy slow stream that dribbles
        //   bytes keeps flowing, no matter how long it runs).
        let client = build_client(
            Duration::from_secs(30),      // a generous CONNECT bound
            Duration::from_secs(5 * 60),  // 5 min of SILENCE = a stalled provider
            self.custom_headers.as_ref(), // R1 (issue #129): the ONE completion seam
        );
        // The request + status gate (the `stream_options` retry-and-latch
        // lives in `send_model_request`).
        let resp = self.send_model_request(&client, &url, req).await?;

        // `bytes_stream` yields `bytes::Bytes`; map to `Vec<u8>` so the
        // stream type does not name the `bytes` crate.
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            resp.bytes_stream().map(|r| r.map(|b| b.to_vec())).boxed();
        Ok(Box::pin(SseStream::new(raw)))
    }
}

/// Does a `ProviderError` say the endpoint REJECTED the request for its
/// `stream_options` field? The `map_status` message embeds the error body
/// verbatim (`status {status}: {body}`), so a strict proxy's complaint —
/// "Unrecognized field: stream_options", "Unsupported parameter:
/// stream_options", … — is in there. Matched on the lowercased `usage`-
/// request marker: any wording that names the field, on any tier. A
/// genuine `Auth` error never names it, so the tiers stay intact.
fn is_stream_options_rejection(err: &ProviderError) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("stream_options")
}

/// Build the OpenAI-compatible request body from a [`ModelRequest`]
/// (`stream: true` is always sent; `None` options are omitted). The
/// message `content` goes through the OpenAI WIRE form
/// ([`MessageContent::to_wire`]) — a plain string for `Text`, an array
/// of OpenAI content parts (`text` / `image_url` data-URIs) for
/// `Blocks` — and an assistant `tool_calls` is serialized in the
/// OpenAI shape (`{ id, type: "function", function: { name, arguments:
/// <string> } }`).
///
/// `stream_options.include_usage` is sent when `ask_usage` is set (the
/// provider's latch — see `send_model_request`), on TOP of the request's
/// own `stream` flag.
fn request_body(req: &ModelRequest, ask_usage: bool) -> Value {
    // The `System` messages NEVER travel in the position the transcript put
    // them: they fold into ONE leading `system` message (see
    // [`ModelRequest::system_text`]), because the ChatML-family templates
    // many OpenAI-compatible servers run reject a `system` message anywhere
    // else outright. Everything else keeps its order — a `System` message is
    // never a link in a `tool_calls` / `tool_call_id` pair, so hoisting it
    // cannot break a tool turn.
    let system = req.system_text();
    let mut messages: Vec<Value> = Vec::with_capacity(req.messages.len() + 1);
    if !system.is_empty() {
        messages.push(json!({ "role": "system", "content": system }));
    }
    messages.extend(
        req.messages
            .iter()
            .filter(|m| m.role != ChatRole::System)
            .map(|m| {
                // The content is the OpenAI WIRE form (`MessageContent::to_wire`
                // — `Text` → a string, `Blocks` → an array of OpenAI content
                // parts with `image_url` data-URIs), NOT the `ContentBlock`'s
                // own pi-shaped serialization (the DB / transcript / tool-result
                // shape). The remaining fields keep their wire shape.
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "role".to_string(),
                    serde_json::to_value(m.role).expect("role serialization cannot fail"),
                );
                // (issue #122) THE OUTBOUND ROOT-CAUSE GUARD. An empty/whitespace
                // assistant message must never send `"content": ""` on the
                // OpenAI-compatible wire: LiteLLM's `_sanitize_empty_text_content`
                // rewrites it into the placeholder and SAVES that back into
                // history — so the placeholder would be authored by OUR shape,
                // re-read on the next turn, and every inbound filter in this repo
                // would sit permanently downstream of a self-perpetuating
                // history. `isinstance(content, str)` is false for an OMITTED
                // field, so omitting it (the OpenAI contract does not require
                // `content` on an assistant message that carries `tool_calls`)
                // makes the upstream sanitizer a no-op.
                //
                // Shaping HERE, not in the transcript — ADR 0054: the wire's shape
                // is corrected where the request is built.
                //
                // Deliberately narrow, and pinned by the three tests above:
                //   * only a `Text` that is empty-ish / the placeholder, AND only
                //     on a message that carries `tool_calls`, is omitted;
                //   * a tool-call-less assistant message keeps its `"content": ""`
                //     (that shape is the transcript's, and issue #64's
                //     `handle_empty_turn` already retries such a turn instead of
                //     letting it round-trip);
                //   * `Blocks` is untouched (an array fails LiteLLM's `str` check);
                //   * a `tool` message's `"content"` is REQUIRED to pair with its
                //     `tool_call_id`, so it is never dropped.
                let empty_text_with_calls = m.tool_calls.is_some()
                    && match &m.content {
                        MessageContent::Text(t) => content_is_effectively_empty(t),
                        MessageContent::Blocks(_) => false,
                    };
                if !empty_text_with_calls {
                    obj.insert("content".to_string(), m.content.to_wire());
                }
                if let Some(id) = &m.tool_call_id {
                    obj.insert("tool_call_id".to_string(), Value::String(id.clone()));
                }
                if let Some(calls) = &m.tool_calls {
                    obj.insert(
                        "tool_calls".to_string(),
                        Value::Array(
                            calls
                                .iter()
                                .map(|tc| {
                                    serde_json::to_value(WireToolCall {
                                        id: &tc.id,
                                        name: &tc.name,
                                        arguments: &tc.arguments,
                                    })
                                    .expect("JSON serialization cannot fail")
                                })
                                .collect(),
                        ),
                    );
                }
                Value::Object(obj)
            }),
    );
    let mut body = json!({
        "model": req.model,
        "messages": Value::Array(messages),
        "stream": req.options.stream,
    });
    // An OpenAI-compatible stream carries NO `usage` chunk unless the
    // request asks for it — and a usage-less session is worse than a
    // cosmetic problem: the `AgentLoop`'s `last_context_tokens` and the
    // `Compactor`'s usage ANCHOR are BOTH fed by the `Usage` event, so
    // without it the context bar freezes at its first estimate and
    // auto-compaction can NEVER trip (an unbounded transcript until the
    // provider 400s). Streaming-only: OpenAI rejects `stream_options`
    // with `stream: false`.
    if req.options.stream && ask_usage {
        body["stream_options"] = json!({ "include_usage": true });
    }
    if !req.tools.is_empty() {
        body["tools"] = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    },
                })
            })
            .collect::<Vec<_>>()
            .into();
    }
    if let Some(t) = req.options.temperature {
        body["temperature"] = json!(t);
    }
    if let Some(m) = req.options.max_tokens {
        body["max_tokens"] = json!(m);
    }
    // Vendor quirk (GPT-5.5+ over Chat Completions — the OpenAI API rejects
    // `reasoning_effort` alongside function tools: "Function tools with
    // reasoning_effort are not supported"). Suppressed for the `gpt-5.5`
    // family WHEN THE REQUEST CARRIES TOOLS; the Responses wire accepts both
    // (which is exactly why responses-mode models derive there — see
    // `is_gpt_5_5_or_newer_completions_quirk`). A suppressed level means
    // "provider default" for that turn — what the 400 would force anyway.
    //
    // THE GATE IS EXACT (plan VF#5b): it fires ONLY on the Chat Completions
    // wire — this is `OpenAiCompatibleProvider`'s ONE body builder and
    // `responses.rs` has its own `responses_request_body` (it never calls
    // this function), so `reasoning` on the Responses wire is untouched.
    // Predicate: `is_gpt_5_5_or_newer_completions_quirk(&req.model)
    // && !req.tools.is_empty()`.
    let suppress_reasoning =
        is_gpt_5_5_or_newer_completions_quirk(&req.model) && !req.tools.is_empty();
    if let Some(r) = &req.options.reasoning_effort {
        if !suppress_reasoning {
            body["reasoning_effort"] = json!(r);
        }
    }
    body
}

/// The GPT-5.5+ Chat-Completions quirk family (T3b / #131): a CONSERVATIVE
/// model-name prefix list. `req.model` is the catalog's BARE id (the loop's
/// `model_request` uses `model.id`), matched case-insensitively.
///
/// Deliberately narrow: the `gpt-5.5` family (plus the `gpt-5.6` / `gpt-5.7`
/// minors if they appear) and NOTHING else — a future `gpt-6` or a renamed
/// family is NOT blindly matched; extending the list is this one line. No other
/// model-name branching exists in this file (the wire is derived, not guessed).
fn is_gpt_5_5_or_newer_completions_quirk(model: &str) -> bool {
    const PREFIXES: [&str; 3] = ["gpt-5.5", "gpt-5.6", "gpt-5.7"];
    let lower = model.to_ascii_lowercase();
    PREFIXES.iter().any(|p| lower.starts_with(p))
}

#[cfg(test)]
mod tests {
    use super::super::test_util::{chat_message, collect};
    use super::super::types::{ChatMessage, ChatRole, MessageContent, ModelOptions, ToolSpec};
    use super::*;
    use crate::agent::tools::{ContentBlock, ImageRef};
    // Test-only (the raw-server request counter): importing it at the module
    // head would be an unused import in the non-test build.
    use std::sync::atomic::AtomicU32;

    /// Build an `SseStream` over RAW byte chunks (the `&str` helper below
    /// cannot split a multi-byte codepoint mid-byte — `&str` is valid
    /// UTF-8; this one can).
    fn sse_stream_bytes(chunks: &[Vec<u8>]) -> SseStream {
        let data: Vec<Vec<u8>> = chunks.to_vec();
        let raw: BoxStream<'static, Result<Vec<u8>, reqwest::Error>> =
            futures_util::stream::iter(data.into_iter().map(Ok)).boxed();
        SseStream::new(raw)
    }

    /// Build an `SseStream` over canned raw chunks (no HTTP).
    fn sse_stream(chunks: &[&str]) -> SseStream {
        // Owned data so the stream is `'static` (independent of `chunks`).
        let data: Vec<Vec<u8>> = chunks.iter().map(|c| c.as_bytes().to_vec()).collect();
        sse_stream_bytes(&data)
    }

    // ── object safety / the seam ───────────────────────────────────────

    /// The `Provider` trait is object-safe (a `Box<dyn Provider>` compiles)
    /// so a second provider (Anthropic) can be added later.
    #[test]
    fn provider_trait_is_object_safe() {
        let provider: Box<dyn Provider> = Box::new(OpenAiCompatibleProvider::new(
            "https://example.com/v1".to_string(),
            "k".to_string(),
        ));
        let _ = provider;
    }

    // ── SSE parsing ────────────────────────────────────────────────────

    // ── (issue #122) the upstream sanitisation placeholder, inbound ─────

    /// (issue #122) The SSE parser is the FILTER OF RECORD: it is the only seam
    /// that sees the whole `delta.content` stream, so a placeholder LiteLLM
    /// SPLIT mid-token across two `delta.content` fragments is reassembled by
    /// the prefix hold and SILENCED before it can become a `TextDelta` — the
    /// loop, the transcript and the UI never see it.
    #[tokio::test]
    async fn sse_streamed_placeholder_content_is_silenced() {
        let events = collect(sse_stream(&[
            // LiteLLM splits the string mid-token across TWO `delta.content`
            // fragments, and the second one carries the `finish_reason`.
            "data: {\"choices\":[{\"delta\":{\"content\":\"[System: Empty message \"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"content sanitised to satisfy protocol]\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]))
            .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Done(FinishReason::Stop)],
            "the split placeholder is reconstructed and SILENCED (no `TextDelta`)"
        );
    }

    /// (issue #122) The guard rail of the prefix hold: legitimate streaming text
    /// is untouched. `"he"` is not a prefix of the placeholder, so it is
    /// released on its own chunk exactly as it was before this fix — the split
    /// terminal deltas still reach the UI (the byte-identical common path).
    #[tokio::test]
    async fn sse_does_not_silence_legitimate_content() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"llo\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]))
            .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("he".to_string()),
                ProviderEvent::TextDelta("llo".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a run that cannot be the placeholder is released per chunk, \
             byte-identical to the pre-fix stream (the `he`/`llo` split reaches the UI)"
        );
    }

    /// (issue #122) The FLUSH SEAM, loss half: a legitimate message that merely
    /// STARTS like the placeholder (here, a bare `"["`) is held as a strict
    /// prefix, and the stream then ends WITHOUT ever diverging it. The held head
    /// is RELEASED at the stream tail — never silently dropped — and the
    /// synthesized `Done` follows it (so `poll_next` never returns `None` with
    /// text still held).
    #[tokio::test]
    async fn a_held_placeholder_prefix_is_released_at_the_stream_tail() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"[\"}}]}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("[".to_string()),
                // No `finish_reason` ever arrived, so the tail synthesizes the
                // `Done(Error)` (the pre-existing behaviour) — AFTER the text.
                ProviderEvent::Done(FinishReason::Error),
            ],
            "the held head of a legitimate `[`-starting message is released at \
             stream end, not lost (the recorded trade-off is DELAY, never loss)"
        );
    }

    /// (issue #122) The FLUSH SEAM, diverge half, plus the ordering rule: a run
    /// that starts like the placeholder and then diverges is released as ONE
    /// `TextDelta` (the coalesced head) BEFORE the events that arrived after it
    /// — here the `finish_reason` chunk's `Done`, and a `usage`-only chunk that
    /// cannot carry content. The banner QUOTED inside a longer line is the
    /// infinitely-likely user case this pins: it can never be a strict prefix of
    /// the placeholder, so it streams untouched.
    #[tokio::test]
    async fn a_diverged_held_prefix_is_released_in_arrival_order() {
        let events = collect(sse_stream(&[
            // A strict prefix — held.
            "data: {\"choices\":[{\"delta\":{\"content\":\"[System: Empty\"}}]}\n\n\
             // A `usage`-only chunk: it ends the text run, so the held head is
             // released BEFORE the `Usage` (arrival order preserved).
             data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n\
             // The line that turns it into a quoted banner: released as-is.
             data: {\"choices\":[{\"delta\":{\"content\":\" message content sanitised to satisfy protocol] means what?\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]))
            .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("[System: Empty".to_string()),
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 1,
                        output_tokens: 1
                    },
                    None
                ),
                ProviderEvent::TextDelta(
                    " message content sanitised to satisfy protocol] means what?".to_string()
                ),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "a banner quoted inside a longer line is NEVER silenced, and held \
             text keeps its position in the event order (got {events:?})"
        );
    }

    /// (issue #122) The third split shape: the placeholder arrives in THREE
    /// fragments, and a real message's tail follows the silenced run in the same
    /// stream. The placeholder is dropped, the real text survives, in order.
    #[tokio::test]
    async fn a_three_way_split_placeholder_is_silenced_and_the_real_tail_survives() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"[Sys\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"tem: Empty message content sa\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"nitised to satisfy protocol]\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"the real answer\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]))
            .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("the real answer".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "the three-way split placeholder is silenced; the text after it \
             survives (got {events:?})"
        );
    }

    /// (issue #122) The DON'T-FILTER-TOO-MUCH rail at the byte level: a
    /// whitespace-only delta is a chunk providers really send between words, so
    /// the SSE seam releases it rather than dropping it (the streaming drop sites
    /// key on the PLACEHOLDER, never on emptiness — see
    /// `content_is_the_sanitise_placeholder`). Dropping `" "` would weld two
    /// words into one in the transcript.
    #[tokio::test]
    async fn a_whitespace_only_delta_is_released_not_dropped() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\" \"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}]}\n\n",
        ]))
            .await;
        let text: String = events
            .iter()
            .filter_map(|e| match e {
                ProviderEvent::TextDelta(t) => Some(t.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            text, "hello world",
            "the space between the words survives the filter (got {events:?})"
        );
    }

    /// (issue #122) The predicate pair, pinned directly: the exact placeholder
    /// (bare, padded, or markdown-decorated) is the placeholder; a longer line
    /// that quotes it, a strict PREFIX of it, and an empty string are not. The
    /// prefix case is the whole reason the SSE seam holds instead of matching
    /// per-chunk, and the empty case is the reason the streaming sites must not
    /// use `content_is_effectively_empty`.
    #[test]
    fn the_placeholder_predicate_is_exact_and_never_matches_a_prefix() {
        assert!(content_is_the_sanitise_placeholder(
            EMPTY_CONTENT_SANITISE_PLACEHOLDER
        ));
        assert!(
            content_is_the_sanitise_placeholder(&format!(
                "  {EMPTY_CONTENT_SANITISE_PLACEHOLDER}\n"
            )),
            "surrounding whitespace is decoration"
        );
        assert!(
            content_is_the_sanitise_placeholder(&format!("*{EMPTY_CONTENT_SANITISE_PLACEHOLDER}*")),
            "markdown emphasis around the banner is decoration"
        );
        // NOT the placeholder: a strict prefix (the split head — the SSE seam
        // HOLDS this rather than matching or dropping it), a longer line that
        // quotes it, and emptiness.
        assert!(!content_is_the_sanitise_placeholder("[Sys"));
        assert!(!content_is_the_sanitise_placeholder(&format!(
            "what does {EMPTY_CONTENT_SANITISE_PLACEHOLDER} mean?"
        )));
        assert!(!content_is_the_sanitise_placeholder(""));
        assert!(!content_is_the_sanitise_placeholder("   "));
        // `content_is_effectively_empty` is the WHOLE-VALUE predicate: it adds
        // emptiness (the outbound guard and the subagent guards) and keeps the
        // exact-match property.
        assert!(content_is_effectively_empty(""));
        assert!(content_is_effectively_empty(" \n "));
        assert!(content_is_effectively_empty(
            EMPTY_CONTENT_SANITISE_PLACEHOLDER
        ));
        assert!(!content_is_effectively_empty("[Sys"));
        assert!(!content_is_effectively_empty("real text"));
    }

    #[tokio::test]
    async fn sse_content_and_finish_reason() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
             data: [DONE]\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("He".to_string()),
                ProviderEvent::TextDelta("llo".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    /// (Finding 1) An in-stream `error` chunk on the chat-completions wire
    /// is a REAL tier, not a fabricated one. Until the parser had an error
    /// arm this chunk was discarded at the `choices` deref (an `error`
    /// chunk carries no `choices`), the stream end synthesized
    /// `Done(FinishReason::Error)`, and `turn.rs` turned that into
    /// `Retryable("the model stream ended without a finish_reason")` — 4
    /// doomed ladder retries against a key that cannot become valid. The
    /// Anthropic wire reports the same body as `Auth` in ONE call, and the
    /// settings UI keys off the auth tier.
    #[tokio::test]
    async fn sse_mid_stream_auth_error_is_reported() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"error\":{\"type\":\"authentication_error\",\"message\":\"Bad key\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Error(ProviderError::Auth(
                "authentication_error: Bad key".to_string()
            ))],
            "an in-stream `error` chunk is a real `Auth` tier, terminal \
             (no synthesized `Done`, no retryable fabrication)"
        );
        // The message survives the classification (the UI shows it).
        match &events[0] {
            ProviderEvent::Error(ProviderError::Auth(msg)) => {
                assert!(msg.contains("Bad key"), "got {msg:?}");
            }
            other => panic!("expected an Auth error, got {other:?}"),
        }
    }

    /// (ADR 0045) The overflow half of the same arm: an over-window prompt
    /// reported INSIDE the stream must reach the loop as
    /// [`ProviderError::ContextOverflow`], which `retry.rs`'s
    /// `context_overflow_is_never_retried` forbids the ladder from touching
    /// (the loop compacts in the turn instead). The `is_context_overflow_text`
    /// check runs BEFORE the `kind` match, so a `Fatal`-looking
    /// `invalid_request_error` kind keeps the overflow tier.
    #[tokio::test]
    async fn sse_mid_stream_context_overflow_is_reported() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"error\":{\"type\":\"invalid_request_error\",\"message\":\"prompt is too long: 213462 tokens > 200000 maximum\"}}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![ProviderEvent::Error(ProviderError::ContextOverflow(
                "invalid_request_error: prompt is too long: 213462 tokens > 200000 maximum"
                    .to_string()
            ))],
            "an in-stream overflow is the overflow tier (ADR 0045) — the \
             loop compacts it in the turn, the ladder never re-issues it"
        );
        match &events[0] {
            ProviderEvent::Error(ProviderError::ContextOverflow(msg)) => {
                assert!(msg.contains("prompt is too long"), "got {msg:?}");
            }
            other => panic!("expected a ContextOverflow error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sse_reasoning_deltas() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"done\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
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

    /// A single chunk can carry BOTH a `reasoning` fragment and a
    /// `content` fragment (some providers batch the thinking's tail with
    /// the text's head — the transition chunk). The thinking block
    /// PRECEDES the text block in the model's output, so the
    /// `ThinkingDelta` must be emitted BEFORE the `TextDelta` — the
    /// reverse order makes the frontend start the text block first and
    /// file the thinking's tail under a SECOND thinking block (the
    /// "missing last word" bug).
    #[tokio::test]
    async fn sse_chunk_with_reasoning_and_content_orders_thinking_first() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"Fedora.\",\"content\":\"This project\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\" builds\"}}]}\n\n\
             data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::ThinkingDelta("Fedora.".to_string()),
                ProviderEvent::TextDelta("This project".to_string()),
                ProviderEvent::TextDelta(" builds".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]
        );
    }

    #[tokio::test]
    async fn sse_tool_call_fragments_accumulate() {
        // `name` and `arguments` split across chunks (as the real API
        // streams them); the `id` arrives only in the first fragment.
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"re\"}}]}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"ad\",\"arguments\":\"{\"}}]}}]}\n\n\
             data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"path\\\": \\\"/x\\\"}\"}}]}}]}\n\n\
             data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
        ]))
        .await;
        // Three `ToolCallDelta`s, then the accumulated `ToolCall`, then
        // `Done(ToolCalls)` — the `ToolCall` precedes the `Done`.
        assert_eq!(
            events,
            vec![
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 0,
                    id: Some("call_1".to_string()),
                    name: Some("re".to_string()),
                    arguments: None,
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 0,
                    id: None,
                    name: Some("ad".to_string()),
                    arguments: Some("{".to_string()),
                }),
                ProviderEvent::ToolCallDelta(ToolCallDelta {
                    index: 0,
                    id: None,
                    name: None,
                    arguments: Some("\"path\": \"/x\"}".to_string()),
                }),
                ProviderEvent::ToolCall(ToolCall {
                    id: "call_1".to_string(),
                    name: "read".to_string(),
                    arguments: json!({ "path": "/x" }),
                }),
                ProviderEvent::Done(FinishReason::ToolCalls),
            ]
        );
    }

    /// Exactly-once `Usage` on the openai wire (#108): a proxy that
    /// re-sends the trailing `usage` chunk must not push a second frame
    /// the cost roll-up would sum as money that was not spent.
    ///
    /// The SECOND `usage` chunk ALSO carries a `choices[0].delta.content`
    /// and a `finish_reason` — so a guard written as an early `return`
    /// from `handle_chunk` (which would exit PAST the `choices[0].delta`
    /// handling and the `finish_reason` flush + `Done` that follow the
    /// usage block) fails this pin on the MISSING `TextDelta` / `Done`,
    /// not merely on the `Usage` count. A `Usage`-count-only pin passes
    /// against that buggy guard; the full-vector assert is what makes
    /// this a real pin.
    #[tokio::test]
    async fn sse_usage_emitted_once_from_two_chunks() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n\
             data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n",
        ]))
        .await;
        // ONE `Usage` frame, then the text, then the `Done` — nothing
        // dropped, nothing doubled.
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
                ProviderEvent::TextDelta("hi".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ],
            "two `usage` chunks emit ONE Usage and lose no delta/`Done` (got {events:?})"
        );
    }

    #[tokio::test]
    async fn sse_usage_chunk() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n\
             data: [DONE]\n\n",
        ]))
        .await;
        // No `finish_reason` on the wire: a synthetic `Done(Error)` closes
        // the stream (the `Usage` still arrives).
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("x".to_string()),
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 10,
                        output_tokens: 5,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    /// (ADR 0046) The ANTI-DOUBLE-COUNT pin. On this wire `prompt_tokens`
    /// is ALREADY the whole prompt — cached tokens included — and
    /// `prompt_tokens_details.cached_tokens` is a SUBSET of it, so the
    /// count passes through UNCHANGED and the detail is NOT read at all.
    /// (The Anthropic wire reports its cache fields as DISJOINT parts and
    /// DOES sum them; copying that "for consistency" here would add the
    /// entire cached prefix a second time, over-read the **Anchor** and
    /// make compaction arrive EARLY — that tempting edit is exactly what
    /// this test exists to break.)
    #[tokio::test]
    async fn sse_usage_does_not_add_the_cached_tokens_detail() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1000,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":800}}}\n\n\
             data: [DONE]\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                // A `usage`-only chunk carries an EMPTY `choices`, so the
                // only events are the `Usage` and the synthetic `Done`
                // (no `finish_reason` arrived).
                ProviderEvent::Usage(
                    Usage {
                        input_tokens: 1000,
                        output_tokens: 5,
                    },
                    None
                ),
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    /// `LineAssembler::finish` is shared by all three SSE parsers, so its
    /// contract is pinned directly: a non-empty tail is returned ONCE as
    /// lossy text and the buffer is left empty (a second call yields `None`),
    /// and a never-written assembler reports `None`.
    #[test]
    fn line_assembler_finish_returns_tail_once_then_none() {
        let mut a = LineAssembler::new();
        assert_eq!(a.finish(), None, "empty assembler has no tail");

        // An unterminated tail (no trailing '\n') is flushed as-is.
        assert!(a.push(b"data: partial").is_empty(), "no newline, no line");
        assert_eq!(a.finish(), Some("data: partial".to_string()));
        assert_eq!(a.finish(), None, "the tail is consumed, not replayed");

        // A complete line is returned by push (terminator included —
        // `handle_line` strips it), leaving nothing to flush.
        assert_eq!(a.push(b"data: whole\n"), vec!["data: whole\n".to_string()]);
        assert_eq!(a.finish(), None, "complete lines leave no tail");

        // A tail that ends mid-codepoint is truncated lossily (the only
        // option once the wire is done).
        assert!(a.push("你好".as_bytes()[..4].as_ref()).is_empty());
        let tail = a.finish().expect("split codepoint still flushes a tail");
        assert!(tail.contains('\u{FFFD}'), "got {tail:?}");
    }

    #[tokio::test]
    async fn sse_chunks_split_mid_line() {
        // The raw bytes may split a `data:` line anywhere — the line
        // buffer must reassemble it.
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"de",
            "lta\":{\"content\":\"ab\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"fin",
            "ish_reason\":\"length\"}]}\n\n",
        ]))
        .await;
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextDelta("ab".to_string()),
                ProviderEvent::Done(FinishReason::Length),
            ]
        );
    }

    /// (finding 9) The raw bytes may split a MULTI-BYTE codepoint between
    /// chunks (CJK is 3 bytes, emoji is 4). A per-chunk `from_utf8_lossy`
    /// would replace each half with U+FFFD BEFORE line assembly, corrupting
    /// the reassembled text. The buffer must hold RAW bytes and convert
    /// only COMPLETE lines: the reassembled text is byte-identical at
    /// every possible split point.
    #[tokio::test]
    async fn sse_multibyte_chars_survive_chunk_boundaries() {
        // CJK (3-byte) + emoji (4-byte) content.
        let text = "你好🌍世界🚀";
        let payload =
            format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\n");
        let bytes = payload.as_bytes();
        // Split the raw bytes at EVERY offset — including the middle of a
        // 3-byte and a 4-byte codepoint. Each reassembly must be
        // byte-identical (no U+FFFD).
        for split_at in 1..bytes.len() {
            let events = collect(sse_stream_bytes(&[
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
        let events = collect(sse_stream_bytes(&[
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

    #[test]
    fn finish_reason_mapping() {
        assert_eq!(map_finish_reason("stop"), FinishReason::Stop);
        assert_eq!(map_finish_reason("tool_calls"), FinishReason::ToolCalls);
        assert_eq!(map_finish_reason("length"), FinishReason::Length);
        assert_eq!(map_finish_reason("error"), FinishReason::Error);
        // An unknown value is a normal stop.
        assert_eq!(map_finish_reason("content_filter"), FinishReason::Stop);
    }

    #[test]
    fn parse_arguments_handles_empty_and_malformed() {
        assert_eq!(parse_arguments(""), Value::Object(Default::default()));
        assert_eq!(parse_arguments("  "), Value::Object(Default::default()));
        assert_eq!(
            parse_arguments("{not json"),
            Value::Object(Default::default())
        );
        assert_eq!(parse_arguments(r#"{"a":1}"#), json!({ "a": 1 }));
    }

    /// A wire `usage` token count that exceeds `u32::MAX` SATURATES
    /// (a silent `as u32` would truncate — 4 GB of tokens would wrap to
    /// a small count).
    #[tokio::test]
    async fn sse_usage_saturates_on_u32_overflow() {
        let events = collect(sse_stream(&[
            &format!(
                "data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":{},\"completion_tokens\":5}}}}\n\n",
                u32::MAX as u64 + 1
            ),
        ]))
        .await;
        match &events[0] {
            ProviderEvent::Usage(u, _) => {
                assert_eq!(u.input_tokens, u32::MAX, "saturated, not truncated");
                assert_eq!(u.output_tokens, 5);
            }
            other => panic!("expected a Usage event, got {other:?}"),
        }
    }

    // ── the per-call cost off the BODY's usage object (D4, AMENDED) ─────

    /// The cost rides the SAME trailing usage chunk the token counts come
    /// from — the field LiteLLM injects when the client asks for the usage
    /// chunk (`stream_options.include_usage`, which this client already
    /// sends), confirmed live by probe P1.
    #[tokio::test]
    async fn sse_usage_chunk_carries_the_provider_cost() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"cost\":0.000011662}}\n\n\
             data: [DONE]\n\n",
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
                    Some(0.000011662)
                ),
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    /// A usage chunk with counts and NO money is `None` — the ordinary
    /// case on a direct OpenAI endpoint, and on a LiteLLM CACHE HIT even
    /// on a cooperative proxy (probe P6). Absence is per-CALL and is not
    /// zero.
    #[tokio::test]
    async fn sse_usage_chunk_without_cost_reports_none() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\n\
             data: [DONE]\n\n",
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
                ProviderEvent::Done(FinishReason::Error),
            ]
        );
    }

    /// A malformed money value is `None` — never a panic, never a clamp,
    /// and never a `Some(0.0)` that the UI would render as a free `$0.00`.
    /// The token counts still arrive: a provider that lies about money
    /// still told the truth about tokens.
    #[tokio::test]
    async fn sse_usage_malformed_cost_is_none_not_a_panic() {
        for malformed in [json!("abc"), Value::Null, json!({}), json!(-0.5)] {
            let payload = json!({
                "choices": [],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "cost": malformed,
                },
            });
            let events = collect(sse_stream(&[&format!("data: {payload}\n\n")])).await;
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
                    ProviderEvent::Done(FinishReason::Error),
                ],
                "a malformed cost ({malformed}) is None, not a panic"
            );
        }
    }

    /// REGRESSION GUARD (a recorded residue, not a RED pin): the existing
    /// `prompt_tokens` × `completion_tokens` gate is UNCHANGED, so a usage
    /// chunk carrying a cost and no token pair emits NO `Usage` event.
    /// Inventing a cost-only emission would make this parser a SECOND
    /// `Usage` producer and feed T09 money detached from the counts D4
    /// pairs it with. Asserts an ABSENCE, so it is green before the change
    /// as well as after.
    #[tokio::test]
    async fn sse_cost_only_usage_chunk_emits_no_usage_event() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[],\"usage\":{\"cost\":0.000011662}}\n\n\
             data: [DONE]\n\n",
        ]))
        .await;
        assert!(
            !events.iter().any(|e| matches!(e, ProviderEvent::Usage(..))),
            "a cost with no token pair emits NO Usage (got {events:?})"
        );
    }

    /// The honest non-finite case, pinned where it is OBSERVABLE (the
    /// wire, not the reader): a `1e999` cost is rejected by serde_json at
    /// the CHUNK level (`number out of range`), the parser's `let Ok(…)
    /// else` swallows it, and the effect is NO event at all — not
    /// `Some(inf)`. A reader-level "non-finite → None" pin would be a
    /// tautology (a non-finite cannot exist as a `serde_json::Number`),
    /// which is why the claim lives here instead.
    #[tokio::test]
    async fn sse_usage_chunk_with_an_unparsable_number_emits_no_usage_event() {
        let events = collect(sse_stream(&[
            "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n\
             data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"cost\":1e999}}\n\n\
             data: [DONE]\n\n",
        ]))
        .await;
        assert!(
            !events.iter().any(|e| matches!(e, ProviderEvent::Usage(..))),
            "the whole chunk fails to parse, so NO Usage is emitted (got {events:?})"
        );
        // The chunk that DID parse still delivered its text — the failure
        // is scoped to the one bad line, not to the stream.
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::TextDelta(t) if t == "x")),
            "the good chunk is unaffected (got {events:?})"
        );
    }

    // ── request-body mapping ───────────────────────────────────────────

    #[test]
    fn request_body_maps_the_request() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                ChatMessage {
                    role: ChatRole::System,
                    content: MessageContent::Text("sys".to_string()),
                    tool_call_id: None,
                    tool_calls: None,
                },
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: MessageContent::Text(String::new()),
                    tool_call_id: None,
                    tool_calls: Some(vec![ToolCall {
                        id: "call_1".to_string(),
                        name: "read".to_string(),
                        arguments: json!({ "path": "/x" }),
                    }]),
                },
                ChatMessage {
                    role: ChatRole::Tool,
                    content: MessageContent::Blocks(vec![ContentBlock::Text {
                        text: "file".to_string(),
                    }]),
                    tool_call_id: Some("call_1".to_string()),
                    tool_calls: None,
                },
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
        let body = request_body(&req, true);
        assert_eq!(body["model"], "m");
        assert_eq!(body["stream"], true);
        let temperature = body["temperature"]
            .as_f64()
            .expect("temperature is a number");
        assert!(
            (temperature - 0.2).abs() < 1e-6,
            "temperature is passed through (got {temperature})"
        );
        assert_eq!(body["max_tokens"], 64);
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "read");
        // The message roles + the tool-result `tool_call_id`.
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][2]["role"], "tool");
        assert_eq!(body["messages"][2]["tool_call_id"], "call_1");
        // `Blocks` content serializes as an array of OpenAI parts (the
        // `to_wire` shape — `ContentBlock::Text` verbatim).
        assert_eq!(body["messages"][2]["content"][0]["type"], "text");
        // `stream_options.include_usage` — an OpenAI-compatible stream
        // carries NO `usage` chunk unless the request asks for it, and a
        // usage-less session freezes the context bar and NEVER trips the
        // compaction threshold.
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    /// A `Text`-content `ChatMessage` test literal (the request-body
    /// tests' shape; `naming.rs`'s helper of the same name).
    fn text(role: ChatRole, t: &str) -> ChatMessage {
        ChatMessage {
            role,
            content: MessageContent::Text(t.to_string()),
            tool_call_id: None,
            tool_calls: None,
        }
    }

    /// A request whose messages put a `System` message ANYWHERE BUT index 0
    /// still gets ONE leading `system` message on the wire.
    ///
    /// This is not cosmetic. The harness's compaction summary is a `System`
    /// message and it sits at index 1 — right after the preserved leading
    /// system prompt (`compact::run_compaction`) — so every compacted
    /// session's SECOND-plus request has a non-leading `system` message. The
    /// ChatML-family chat templates vLLM / SGLang serve for Qwen, DeepSeek
    /// and Hermes models ASSERT against that shape:
    /// `{%- if not loop.first %}{{- raise_exception('System message must be
    /// at the beginning.') }}` — and because vLLM EXECUTES the template to
    /// auto-generate its response parser, the refusal is a 400
    /// `invalid_request_error` naming the TEMPLATE, not the messages, before
    /// the model ever runs. The completions wire therefore folds every
    /// `System` message into one `system` message at index 0, exactly as the
    /// Anthropic (`system`) and Responses (`instructions`) wires already do
    /// — the transcript keeps its shape, only the request is re-shaped.
    #[test]
    fn request_body_folds_every_system_message_into_one_leading_message() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                text(ChatRole::System, "sys"),
                // The real shape: index 1 is a compaction summary.
                text(ChatRole::System, "Summary of previous conversation:\nabc"),
                text(ChatRole::User, "hi"),
                text(ChatRole::Assistant, "yo"),
                // A `System` message that arrives mid-conversation is
                // hoisted into the fold, never left where it was.
                text(ChatRole::System, "late"),
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
        let body = request_body(&req, false);
        let msgs = body["messages"].as_array().expect("messages is an array");
        // The three conversation messages + ONE folded system message.
        assert_eq!(msgs.len(), 3, "the three System messages fold to one");
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(
            msgs[0]["content"], "sys\n\nSummary of previous conversation:\nabc\n\nlate",
            "the system texts keep their order, joined the way the other two wires join them"
        );
        // The conversation itself is UNTOUCHED, in order.
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"], "hi");
        assert_eq!(msgs[2]["role"], "assistant");
        assert_eq!(msgs[2]["content"], "yo");
        // THE invariant every strict ChatML template asserts.
        assert!(
            msgs[1..].iter().all(|m| m["role"] != json!("system")),
            "no `system` message anywhere but index 0: {msgs:?}"
        );
    }

    /// The fold never INVENTS a `system` message: with no `System` messages
    /// (a child session with no system prompt, a summary at index 0) the
    /// first wire message is the first conversation message.
    #[test]
    fn request_body_adds_no_system_message_when_the_request_has_none() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![text(ChatRole::User, "hi")],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: false,
            },
            session_id: None,
        };
        let body = request_body(&req, false);
        let msgs = body["messages"].as_array().expect("messages is an array");
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
    }

    /// A NON-streaming request carries no `stream_options` (the field is
    /// streaming-only — OpenAI 400s on it with `stream: false`).
    #[test]
    fn request_body_omits_stream_options_when_not_streaming() {
        // The `sample_request` fields, restated (that fn inlines its
        // `messages`); only `stream` differs.
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: MessageContent::Text("hi".to_string()),
                tool_call_id: None,
                tool_calls: None,
            }],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: false,
            },
            session_id: None,
        };
        let body = request_body(&req, true);
        assert_eq!(body["stream"], false);
        assert!(
            body.get("stream_options").is_none(),
            "stream_options is streaming-only"
        );
    }

    /// The model-request wire form is the OpenAI shape: `Text` → a plain
    /// string; `Blocks` → an array of content parts — a `Text` block
    /// verbatim, an `Image` block as an `image_url` **data-URI** (the
    /// pi-shaped `data` (base64, no prefix) + `mimeType` fields composed
    /// into `data:<mime>;base64,<data>` — the model never sees the
    /// pi-shaped `image` block).
    #[test]
    fn to_wire_is_the_openai_request_shape() {
        // `Text` → a plain string.
        assert_eq!(
            MessageContent::Text("hello".to_string()).to_wire(),
            json!("hello")
        );
        // `Blocks` → an array of OpenAI content parts.
        let blocks = MessageContent::Blocks(vec![
            ContentBlock::Text {
                text: "before".to_string(),
            },
            ContentBlock::Image {
                image: ImageRef {
                    data: "BASE64DATA".to_string(),
                    mime_type: "image/png".to_string(),
                },
            },
        ]);
        assert_eq!(
            blocks.to_wire(),
            json!([
                { "type": "text", "text": "before" },
                {
                    "type": "image_url",
                    "image_url": { "url": "data:image/png;base64,BASE64DATA" }
                }
            ])
        );
    }

    /// The assistant `tool_calls` go on the wire in the OpenAI shape:
    /// `{ id, type: "function", function: { name, arguments: <JSON string> } }`
    /// — nested `function`, a `type` tag, and `arguments` as a STRING (a
    /// non-conformant flat shape is a 400 at the second model call of
    /// every tool-using turn).
    #[test]
    fn request_body_serializes_tool_calls_in_the_openai_wire_shape() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: MessageContent::Text(String::new()),
                    tool_call_id: None,
                    tool_calls: Some(vec![
                        ToolCall {
                            id: "call_1".to_string(),
                            name: "read".to_string(),
                            arguments: json!({ "path": "/x", "limit": 10 }),
                        },
                        ToolCall {
                            id: "call_2".to_string(),
                            name: "bash".to_string(),
                            arguments: Value::Object(Default::default()),
                        },
                    ]),
                },
                ChatMessage {
                    role: ChatRole::Tool,
                    content: MessageContent::Text("out".to_string()),
                    tool_call_id: Some("call_1".to_string()),
                    tool_calls: None,
                },
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
        let body = request_body(&req, true);
        let calls = body["messages"][0]["tool_calls"]
            .as_array()
            .expect("assistant tool_calls are present");
        // `call_1`: nested `function`, a `type` tag, `arguments` as a
        // JSON **string** (not an object).
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "read");
        assert_eq!(
            calls[0]["function"]["arguments"],
            "{\"limit\":10,\"path\":\"/x\"}"
        );
        // `call_2`: an empty-arguments call serializes to `"{}"`.
        assert_eq!(calls[1]["id"], "call_2");
        assert_eq!(calls[1]["type"], "function");
        assert_eq!(calls[1]["function"]["name"], "bash");
        assert_eq!(calls[1]["function"]["arguments"], "{}");
        // The `tool` message keeps its `tool_call_id` (no `tool_calls`).
        assert_eq!(body["messages"][1]["tool_call_id"], "call_1");
        assert!(body["messages"][1].get("tool_calls").is_none());
    }

    // ── (issue #122) the outbound empty-content guard ──────────────────

    /// An assistant message that called tools must NEVER carry
    /// `"content": ""` on the OpenAI-compatible wire: LiteLLM's
    /// `_sanitize_empty_text_content` rewrites an empty `str` content into the
    /// placeholder AND SAVES that back into history, which is the root cause of
    /// the leaked `[System: Empty message content sanitised to satisfy
    /// protocol]` banner. `isinstance(content, str)` is false for an OMITTED
    /// field, so omitting it (the OpenAI contract does not require `content` on
    /// an assistant message that carries `tool_calls`) keeps the upstream
    /// sanitizer a no-op. Shaping here, not in the transcript — ADR 0054.
    #[test]
    fn request_body_omits_content_for_an_empty_text_assistant_message_with_tool_calls() {
        // The three empty spellings the sanitizer rewrites: empty, whitespace,
        // and the placeholder itself (a replayed poisoned row).
        for empty in [
            String::new(),
            "   \n\t ".to_string(),
            EMPTY_CONTENT_SANITISE_PLACEHOLDER.to_string(),
        ] {
            let req = ModelRequest {
                model: "m".to_string(),
                messages: vec![ChatMessage {
                    role: ChatRole::Assistant,
                    content: MessageContent::Text(empty.clone()),
                    tool_call_id: None,
                    tool_calls: Some(vec![ToolCall {
                        id: "call_1".to_string(),
                        name: "read".to_string(),
                        arguments: json!({ "path": "/x" }),
                    }]),
                }],
                tools: vec![],
                options: ModelOptions {
                    temperature: None,
                    max_tokens: None,
                    reasoning_effort: None,
                    stream: true,
                },
                session_id: None,
            };
            let body = request_body(&req, true);
            assert!(
                body["messages"][0].get("content").is_none(),
                "an assistant tool-call message with content {empty:?} sends NO \
                 `content` field (never an empty string), got {}",
                body["messages"][0]
            );
            // The `tool_calls` still ride (the guard omits `content`, never the calls).
            assert!(
                body["messages"][0]["tool_calls"].is_array(),
                "the tool_calls survive the guard"
            );
        }
    }

    /// The GUARD RAIL: the guard is about the WIRE, not about the transcript's
    /// shape. An assistant message with no `tool_calls` keeps its `"content": ""`
    /// — such a turn is the empty-turn condition issue #64 already retries via
    /// `handle_empty_turn` (and never persists), so it is not this guard's case,
    /// and silently dropping the field would change a shape the transcript owns.
    #[test]
    fn request_body_keeps_content_for_an_empty_text_assistant_message_without_tool_calls() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text(String::new()),
                tool_call_id: None,
                tool_calls: None,
            }],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        let body = request_body(&req, true);
        assert_eq!(
            body["messages"][0]["content"],
            Value::String(String::new()),
            "the empty `content` of a tool-call-less assistant message is the \
             transcript's shape, not the wire's to rewrite"
        );
    }

    /// The other two guard rails: a `Blocks` content is never an empty `str` on
    /// the wire (LiteLLM's `isinstance(content, str)` is false for an array, so
    /// there is nothing to sanitize), and a `tool` message's `"content"` is
    /// REQUIRED to pair with its `tool_call_id` — even an empty one is kept, and
    /// a real text is never touched.
    #[test]
    fn request_body_keeps_content_for_blocks_tool_messages_and_real_text() {
        let req = ModelRequest {
            model: "m".to_string(),
            messages: vec![
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: MessageContent::Blocks(vec![ContentBlock::Text {
                        text: String::new(),
                    }]),
                    tool_call_id: None,
                    tool_calls: Some(vec![ToolCall {
                        id: "call_1".to_string(),
                        name: "read".to_string(),
                        arguments: Value::Object(Default::default()),
                    }]),
                },
                ChatMessage {
                    role: ChatRole::Tool,
                    content: MessageContent::Text(String::new()),
                    tool_call_id: Some("call_1".to_string()),
                    tool_calls: None,
                },
                ChatMessage {
                    role: ChatRole::Assistant,
                    content: MessageContent::Text("real".to_string()),
                    tool_call_id: None,
                    tool_calls: Some(vec![ToolCall {
                        id: "call_2".to_string(),
                        name: "bash".to_string(),
                        arguments: Value::Object(Default::default()),
                    }]),
                },
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
        let body = request_body(&req, true);
        assert!(
            body["messages"][0]["content"].is_array(),
            "a `Blocks` content is an array on the wire — never an empty `str`, \
             so it is untouched: {}",
            body["messages"][0]["content"]
        );
        assert_eq!(
            body["messages"][1]["content"],
            Value::String(String::new()),
            "a `tool` message keeps its `content` (it pairs with `tool_call_id`)"
        );
        assert_eq!(
            body["messages"][2]["content"],
            Value::String("real".to_string()),
            "real text is never touched"
        );
    }

    /// The display wire keeps the FLAT `ToolCall` shape (`{ id, name,
    /// arguments }` — the `toolcall_end` / `tool_execution_start` events):
    /// the OpenAI wire shape is request-side only.
    #[test]
    fn tool_call_display_wire_stays_flat() {
        let tc = ToolCall {
            id: "call_1".to_string(),
            name: "read".to_string(),
            arguments: json!({ "path": "/x" }),
        };
        let v = serde_json::to_value(&tc).expect("serialization cannot fail");
        assert_eq!(
            v,
            json!({ "id": "call_1", "name": "read", "arguments": { "path": "/x" } })
        );
        assert!(
            v.get("function").is_none(),
            "no nested `function` on the display wire"
        );
        assert!(v.get("type").is_none(), "no `type` tag on the display wire");
    }

    #[test]
    fn request_body_omits_none_options_and_empty_tools() {
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
        let body = request_body(&req, true);
        assert!(body.get("tools").is_none());
        assert!(body.get("temperature").is_none());
        assert!(body.get("max_tokens").is_none());
        assert!(body.get("reasoning_effort").is_none());
    }

    // ── the GPT-5.5+ Chat-Completions quirk (T3b / #131) ────────────────

    /// A request for ONE model, with or without tools, carrying a level.
    fn quirk_request(model: &str, tools: bool) -> ModelRequest {
        ModelRequest {
            model: model.to_string(),
            messages: vec![chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            )],
            tools: if tools {
                vec![ToolSpec {
                    name: "read".to_string(),
                    description: "Read a file".to_string(),
                    parameters: json!({ "type": "object" }),
                }]
            } else {
                vec![]
            },
            options: ModelOptions {
                temperature: None,
                max_tokens: Some(64),
                reasoning_effort: Some("high".to_string()),
                stream: true,
            },
            session_id: None,
        }
    }

    /// The quirk: OpenAI rejects `reasoning_effort` together with function
    /// tools on Chat Completions — the level is SUPPRESSED for the `gpt-5.5`
    /// family when the request carries tools.
    #[test]
    fn request_body_suppresses_reasoning_effort_with_tools_on_gpt_5_5() {
        for model in ["gpt-5.5", "gpt-5.5-mini", "GPT-5.5"] {
            let body = request_body(&quirk_request(model, true), true);
            assert!(
                body.get("reasoning_effort").is_none(),
                "{model} + tools must NOT carry `reasoning_effort` (the provider 400s): {body}"
            );
            // The rest of the body is untouched (tools still ride).
            assert_eq!(body["tools"][0]["function"]["name"], "read");
            assert_eq!(body["model"], model);
        }
    }

    /// No tools → no conflict → the level stays (the quirk is the PAIR).
    #[test]
    fn request_body_keeps_reasoning_effort_without_tools_on_gpt_5_5() {
        let body = request_body(&quirk_request("gpt-5.5", false), true);
        assert_eq!(body["reasoning_effort"], "high");
        assert!(body.get("tools").is_none());
    }

    /// No regression: every OTHER model keeps `reasoning_effort` alongside
    /// tools (the permissive default is unchanged).
    #[test]
    fn request_body_keeps_reasoning_effort_with_tools_on_other_models() {
        for model in [
            "gpt-4o",
            // The near-miss the CONSERVATIVE prefix list must not match: a
            // plain `gpt-5` is not the 5.5 family.
            "gpt-5",
            "gpt-5-mini",
            // A future major version is NOT blindly matched.
            "gpt-6",
            "claude-3-5-sonnet",
            "m",
        ] {
            let body = request_body(&quirk_request(model, true), true);
            assert_eq!(
                body["reasoning_effort"], "high",
                "{model} + tools keeps `reasoning_effort` (no regression)"
            );
        }
    }

    /// The permissive default: an unknown model with no tools still carries the
    /// level (the gate is the model prefix AND the tools, never either alone).
    #[test]
    fn request_body_keeps_reasoning_effort_when_no_tools_and_unknown_model() {
        let body = request_body(&quirk_request("some-unknown-model", false), true);
        assert_eq!(body["reasoning_effort"], "high");
    }

    /// The predicate's truth table (a documented prefix list — extending the
    /// family is one line, NOT a pattern match).
    #[test]
    fn is_gpt_5_5_or_newer_completions_quirk_prefix_truth_table() {
        for model in [
            "gpt-5.5",
            "gpt-5.5-2026-01",
            "gpt-5.6",
            "gpt-5.7",
            "GPT-5.5",
        ] {
            assert!(is_gpt_5_5_or_newer_completions_quirk(model), "{model}");
        }
        for model in [
            "gpt-5",
            "gpt-5-mini",
            "gpt-4o",
            "gpt-6",
            "o3",
            "claude-3-5-sonnet",
            "5.5",
            "gpt-55",
        ] {
            assert!(
                !is_gpt_5_5_or_newer_completions_quirk(model),
                "{model} is NOT the quirk family"
            );
        }
    }

    // ── the `stream_options` latch vs the overflow tier (ADR 0045) ──────

    /// A raw HTTP server that answers EVERY request with `status` + `body` and
    /// counts what it served. The COUNT is the observable: a latch that fires
    /// re-issues the request, so "exactly one request" is what "the latch did
    /// not fire" looks like from outside.
    async fn rejecting_server(
        status: u16,
        body: &'static str,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<AtomicU32>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let hits = std::sync::Arc::new(AtomicU32::new(0));
        let hits_task = hits.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                hits_task.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                let mut data = Vec::new();
                while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&buf[..n]);
                }
                let head = format!(
                    "HTTP/1.1 {status} Nope\r\ncontent-type: application/json\r\ncontent-length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(body.as_bytes()).await;
            }
        });
        (addr, hits, server)
    }

    /// A streaming request (the latch only applies to one — `stream_options`
    /// is what the first streaming call asks for).
    fn streaming_request() -> ModelRequest {
        ModelRequest {
            model: "m/1".to_string(),
            messages: vec![],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: None,
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        }
    }

    /// (ADR 0045) A context-overflow 400 must NEVER trip the
    /// `stream_options` retry-and-latch, even when its body names BOTH the
    /// field and the overflow (a LiteLLM-style proxy echoing the request it
    /// rejected). The re-issue is pointless — the same oversized bytes fail
    /// the same way — and latching silently loses usage reporting for the
    /// rest of the session, which is the one signal the context bar and the
    /// compaction threshold read. The error surfaces untouched so the loop can
    /// compact instead.
    #[tokio::test]
    async fn a_context_overflow_never_trips_the_stream_options_latch() {
        let body = r#"{"error":{"message":"Unrecognized field: stream_options — and prompt is too long: 213462 tokens > 200000 maximum"}}"#;
        let (addr, hits, server) = rejecting_server(400, body).await;
        let provider = OpenAiCompatibleProvider::new(format!("http://{addr}/v1"), "k".to_string());
        let err = match provider.complete(&streaming_request()).await {
            Ok(_) => panic!("a 400 is an error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, ProviderError::ContextOverflow(_)),
            "the overflow body decides the tier, got {err:?}"
        );
        // Give a re-issue (which would be immediate) the chance to land.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "the overflow is returned untouched — no re-issue without `stream_options`"
        );
        assert!(
            provider.accepts_stream_options.load(Ordering::Relaxed),
            "the latch stays closed: usage reporting survives the overflow"
        );
        server.abort();
    }

    /// The OTHER half of the pin: the latch still works for the case it
    /// exists for (a plain `Fatal` 400 that names the field) — so the overflow
    /// exclusion cannot quietly disable it for everything.
    #[tokio::test]
    async fn a_fatal_stream_options_rejection_still_latches() {
        let body = r#"{"error":{"message":"Unrecognized field: stream_options"}}"#;
        let (addr, hits, server) = rejecting_server(400, body).await;
        let provider = OpenAiCompatibleProvider::new(format!("http://{addr}/v1"), "k".to_string());
        let err = match provider.complete(&streaming_request()).await {
            Ok(_) => panic!("both requests are rejected"),
            Err(e) => e,
        };
        assert!(
            matches!(err, ProviderError::Fatal(_)),
            "a field rejection is still Fatal, got {err:?}"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "the rejection is re-issued once, without the field"
        );
        assert!(
            !provider.accepts_stream_options.load(Ordering::Relaxed),
            "and the omission is latched for this instance"
        );
        server.abort();
    }
}

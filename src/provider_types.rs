//! The provider-agnostic wire vocabulary: the request / message / tool /
//! usage types, the `ProviderEvent` stream shape, the `ProviderError`
//! classification, and the [`Provider`] trait seam the `AgentLoop` calls.
//!
//! (Moved from `provider.rs` — the per-provider stacks that implement
//! [`Provider`] live in the sibling modules.)

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;

use crate::tools::ContentBlock;

/// A model request (the `AgentLoop`'s `complete()` argument, Task 6).
#[derive(Debug, Clone)]
pub struct ModelRequest {
    /// The model id (e.g. `tama` / an OpenRouter slug).
    pub model: String,
    /// The conversation so far.
    pub messages: Vec<ChatMessage>,
    /// The tool specs the model may call (empty = no tools).
    pub tools: Vec<ToolSpec>,
    /// Sampling / limits.
    pub options: ModelOptions,
    /// The session id for LiteLLM session / request tracking (if known).
    pub session_id: Option<String>,
}

impl ModelRequest {
    /// The `System` messages' text, FOLDED into ONE string: each message
    /// verbatim for [`Text`](MessageContent::Text), its `Text` block texts
    /// joined for [`Blocks`](MessageContent::Blocks) (an `Image` block has
    /// no text to fold — it is dropped, as on every wire), and the messages
    /// joined with `"\n\n"` in transcript order. Empty when the request has
    /// no `System` message.
    ///
    /// All three wires need this fold, for different reasons. The harness
    /// keeps `System` messages wherever they belong in the transcript, and
    /// index 1 is a legal home: the compaction summary is a `System` message
    /// sitting right after the preserved leading system prompt
    /// (`compact::run_compaction`). Anthropic has a top-level `system` field
    /// and Responses a top-level `instructions` field, so both send the fold
    /// THERE and keep `System` out of `messages` / `input`. Chat-completions
    /// has no such field, so it sends the fold as a single LEADING `system`
    /// message — the ChatML-family templates vLLM / SGLang serve for Qwen,
    /// DeepSeek and Hermes assert that a `system` message appears ONLY at
    /// index 0 (`raise_exception('System message must be at the beginning.')`),
    /// and because vLLM executes the template to auto-generate its response
    /// parser that assertion is a 400 BEFORE the model runs, reported as a
    /// template error rather than a message error.
    pub fn system_text(&self) -> String {
        self.messages
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
            .join("\n\n")
    }
}

/// A message's content: plain text, or a list of content blocks (mirrors
/// the Task 1 [`ContentBlock`] shape — text or a base64 image).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// `{ "content": "..." }` (the common case).
    Text(String),
    /// `{ "content": [{ "type": "text" | "image", ... }] }`.
    Blocks(Vec<ContentBlock>),
}

impl MessageContent {
    /// The OpenAI-compatible WIRE form of the content: `Text` → a plain
    /// string (`{ "content": "..." }`); `Blocks` → an array of OpenAI
    /// content parts (a text part `{ "type": "text", "text" }` — the
    /// `ContentBlock::Text` wire shape verbatim — or an image part
    /// `{ "type": "image_url", "image_url": { "url": "data:<mime>;base64,…"
    /// } }`, the OpenAI data-URI shape).
    ///
    /// This is the ONLY serialization used on the request wire
    /// (`request_body`); the `ContentBlock`'s own (pi-shaped) serialization
    /// is the DB / transcript / tool-result shape, NOT the model request.
    pub fn to_wire(&self) -> Value {
        match self {
            MessageContent::Text(t) => Value::String(t.clone()),
            MessageContent::Blocks(blocks) => {
                let parts: Vec<Value> = blocks
                    .iter()
                    .map(|b| match b {
                        ContentBlock::Text { text } => json!({
                            "type": "text",
                            "text": text,
                        }),
                        // The OpenAI image wire shape: a `data:` URI
                        // (`data:<mime>;base64,<base64>`) in
                        // `image_url.url`. The `ContentBlock::Image` field
                        // is `data` (base64, NO `data:` prefix) + `mimeType`
                        // (renamed from `mime_type`), so compose the URI here.
                        ContentBlock::Image { image } => json!({
                            "type": "image_url",
                            "image_url": {
                                "url": format!("data:{};base64,{}", image.mime_type, image.data),
                            },
                        }),
                    })
                    .collect();
                Value::Array(parts)
            }
        }
    }
}

/// A chat message's role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    User,
    Assistant,
    /// A tool result (paired with `tool_call_id`).
    Tool,
}

/// One chat message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: MessageContent,
    /// Set on a `tool` message: the `tool_call.id` this result answers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Set on an `assistant` message that called tools.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
}

/// A tool the model may call (the OpenAI `tools[]` entry).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    /// The JSON Schema of the tool's parameters.
    pub parameters: Value,
}

/// A complete tool call (accumulated from `ToolCallDelta`s).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// The provider-assigned id (e.g. `call_1`).
    pub id: String,
    /// The tool name.
    pub name: String,
    /// The parsed arguments (an empty object when the model sent none).
    pub arguments: Value,
    /// (issue #193) The arguments arrived but did not parse: the parse error and
    /// the raw text the provider sent. `None` for a well-formed call AND for a
    /// legitimately empty payload — the two are distinguished precisely BY this
    /// field being absent. `{}` stays `{}` either way; nothing about `arguments`
    /// changes. `skip_serializing_if` (not `skip_serializing`) is load-bearing,
    /// and BOTH directions of that choice are pinned, one per fixture, by
    /// `tool_call_display_wire_stays_flat`:
    /// * dropping the attribute entirely puts `"defect": null` on EVERY
    ///   transcript row (serde's derived `Serialize` writes an absent `Option` as
    ///   `null`) — reddens the test's `None` case;
    /// * an unconditional `skip_serializing` passes the `None` case byte-for-byte
    ///   and silently drops a REAL defect, which is the half that matters more:
    ///   `persist_transcript_message` and `run_compaction` both re-serialize the
    ///   message whole into `native_messages.content_json`, so a defect lost there
    ///   cannot be rebuilt on a resume — reddens the test's `Some` case.
    ///
    /// What neither case may gain is the OpenAI request shape (`function` /
    /// `type`) — that is the flat-shape claim this pin is named for. Read the
    /// two fixtures as ONE key, not two shapes: both serialize to a superset of
    /// `{id, name, arguments}`, and the pair's entire content is whether
    /// `defect` is in that set. They are not opposite key sets, and the test's
    /// own key-list assertion says so ("exactly the three wire keys plus
    /// `defect`"). If you want the property rather than the proxy,
    /// `a_transcript_round_trip_rebuilds_the_defect_a_resume_needs` in
    /// `harness/provider/openai.rs` asserts the `content_json` round-trip a
    /// resume actually depends on.
    ///
    /// # Where the defect travels, and where it never goes
    ///
    /// **Disk and daemon-socket clients; never any provider request.** The
    /// provider half is TRUE and pinned: all three request builders are
    /// hand-built whitelists that name `id` / `name` / `arguments` and nothing
    /// else, and none of them derives `Serialize`, so `defect` cannot join a
    /// request body by a derive change. That half is what
    /// `estimate_summary_payload` (`harness/compact.rs`) declines to count.
    ///
    /// The disk half is NOT the only destination, and a previous round of this
    /// docblock said it was. `persist_transcript_message`
    /// (`harness/loop/emit.rs`) serialises the whole `ChatMessage` into
    /// `content_json`, `IpcStore::insert_message` (`worker/store.rs`) forwards
    /// that verbatim as `StoreFrame::Insert`, and the daemon's `store_frame`
    /// (`fulcrum/src/serve.rs`) puts the same `content_json` on the socket as a
    /// `{"type":"transcript-insert","contentJson":…}` frame — so every socket
    /// client receives `defect.raw`. That is why the field is bounded at the
    /// parse site by [`MAX_DEFECT_RAW_BYTES`] rather than left unbounded: the
    /// data is legitimately disk-shaped (a resume rebuilds the refusal sentence
    /// from it) and it is not user content going to a hostile party, so the
    /// store path stays as it is and the value carries a cap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defect: Option<ArgumentDefect>,
}

/// (issue #193) What was wrong with a tool call's arguments, and what actually
/// arrived.
///
/// This exists because collapsing an unparseable payload into `{}` made every
/// downstream refusal a lie: `read` answered ``missing `path` parameter`` for a
/// call whose `path` had arrived truncated, the model read that as "I omitted a
/// required parameter", and re-sent the identical call (one subagent made 303
/// such calls in 19 minutes; 566 rows in this machine's database carry the
/// sentence). Preserving the defect lets the refusal name the real problem and
/// show the raw text, and it repairs NOTHING — the coercion victims are `write`
/// (440 rows) and `edit` (125), and completing truncated JSON into a written
/// file is worse than refusing.
///
/// `raw` is verbatim up to [`MAX_DEFECT_RAW_BYTES`] — not trimmed, not
/// re-serialized, not re-parsed: the only thing the model can act on is the
/// text it sent, and the head of a JSON argument object is where the keys that
/// arrived live. Past the cap the stored value is that head plus a note naming
/// the true length; see the constant for why a bound at all.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArgumentDefect {
    /// The `serde_json` parse error, stringified.
    pub error: String,
    /// The argument text the provider sent, byte-for-byte up to
    /// [`MAX_DEFECT_RAW_BYTES`], then the head plus a note naming the true
    /// length. Written by [`cap_defect_raw`] — never assign to this field
    /// directly from provider text, or the bound is silently gone again.
    pub raw: String,
}

/// The most raw argument text an [`ArgumentDefect`] stores (issue #193, review
/// FIX 4).
///
/// # Why a bound, when the field was called disk-only
///
/// The docblock on `ToolCall::defect` used to say the field never leaves the
/// disk. The provider half of that is TRUE and pinned — all three request
/// builders are hand-built whitelists that name `id` / `name` / `arguments` and
/// nothing else, so `defect` cannot reach a provider request by a `serde` derive
/// change because none of them derives. The disk half was never the whole
/// story:
///
/// * `persist_transcript_message` (`harness/loop/emit.rs`) serialises the WHOLE
///   `ChatMessage` — `defect` included — into `content_json`;
/// * `IpcStore::insert_message` (`worker/store.rs`) forwards that string
///   verbatim as `StoreFrame::Insert`;
/// * the daemon's `store_frame` (`fulcrum/src/serve.rs`) puts the same
///   `content_json` on the socket as `{"type":"transcript-insert",
///   "contentJson": …}`.
///
/// So `defect.raw` — documented as unbounded — reaches every daemon socket
/// client and a transcript row. This is legitimately disk-shaped data (a resume
/// needs it to rebuild the sentence) and it is not user content travelling to a
/// hostile party, so the store path is deliberately NOT redesigned; the field
/// is bounded instead. An 8 MB truncated `write` body should not be broadcast
/// to satisfy a resume that can only ever quote its head.
///
/// # Why 2 KB, and why the number lives HERE
///
/// 2048 is the in-repo precedent for how much of one oversized thing a single
/// line carries: [`crate::debuglog`]`::MAX_LINE_BYTES` cuts every IPC line at
/// exactly that. The on-disk need is bounded by what a resume can usefully
/// re-read, and a resume re-reads a sentence, not a payload.
///
/// It lives next to the field it bounds rather than beside the sentence that
/// quotes it, because the field is now the thing the cap protects.
/// `harness/tool_specs.rs`'s `MAX_DEFECT_RAW_BYTES` — the bound on the
/// interpolation into the refusal sentence, added by an earlier review round —
/// is this same constant, re-exported, so the two bounds cannot drift apart.
pub const MAX_DEFECT_RAW_BYTES: usize = 2048;

/// Bound provider argument text for [`ArgumentDefect::raw`]: byte-exact up to
/// [`MAX_DEFECT_RAW_BYTES`], then the head plus `… (truncated, N bytes total)`.
///
/// The note is part of the stored value on purpose. A silent cut would make a
/// 2 KB head indistinguishable from a complete 2 KB payload, and the whole
/// reason this field exists is that a refusal must not misdescribe what
/// arrived. Naming the true length keeps the truncation honest in the data.
///
/// # The cap is on the STORED VALUE, note included
///
/// The head is shortened so that head + note fits inside
/// [`MAX_DEFECT_RAW_BYTES`] — the cap is what a consumer stores and broadcasts,
/// so that is what must be bounded. It also makes the two bounds on this field
/// COMPOSE instead of fight: `tool_specs`'s `quoted_defect_raw` cuts at the
/// same constant, and if this one produced a value of `cap + note` the sentence
/// would cut the field's own note in half and append a second one naming the
/// **capped** length (2081) where the payload was 8 MB — silently undoing the
/// "the note names the true length" property the earlier round added. A stored
/// value at or under the cap is left alone by the sentence, which then quotes
/// the note this function wrote.
///
/// # Why the cut walks
///
/// To a character boundary at or below the head budget:
/// `str::floor_char_boundary` is still unstable on the pinned toolchain, and a
/// byte-indexed slice would PANIC on a multi-byte character — which a model
/// streaming CJK content into a `write` body eventually hits. Bounding the
/// field must never be able to panic.
pub fn cap_defect_raw(raw: &str) -> String {
    if raw.len() <= MAX_DEFECT_RAW_BYTES {
        return raw.to_string();
    }
    // The note is written FIRST because its own length depends on the true
    // length, and the head budget depends on the note.
    let note = format!("… (truncated, {} bytes total)", raw.len());
    // `MAX_DEFECT_RAW_BYTES` (2048) dwarfs any note this can produce (a
    // `usize`'s 20 digits plus 28 fixed bytes), so the subtraction cannot
    // underflow; `saturating_sub` keeps it safe if the constant ever shrinks.
    let budget = MAX_DEFECT_RAW_BYTES.saturating_sub(note.len());
    // `is_char_boundary` is true at index 0, so this terminates and never
    // underflows.
    let mut end = budget;
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &raw[..end], note)
}

/// The REQUEST-wire shape of a tool call (the OpenAI contract: a nested
/// `function` object, a `type: "function"` tag, and `arguments` as a JSON
/// **string**). Used ONLY when building the `request_body` messages — the
/// display wire (`toolcall_end` / `tool_execution_start`) keeps the flat
/// [`ToolCall`] shape, and `ChatMessage`'s own serialization (the DB /
/// transcript) is untouched.
///
/// `pub` (T5/S4, Step A.3): a package boundary erases `pub(super)`, and the
/// desktop's `provider/openai.rs` — desktop then — constructed these in its
/// `request_body`. T5/S5b moved that file into this crate as
/// `harness/provider/openai.rs`, so the cross-package reason is gone and this is
/// now constructed only inside the crate (grep-verified at T8/S8.5: zero
/// references under `src-tauri/`). `pub` KEPT — see the note on
/// `interactive::dispatch_params`; the same tighten candidate.
pub struct WireToolCall<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub arguments: &'a Value,
}

/// The `function` object of a [`WireToolCall`].
pub struct WireFunction<'a> {
    pub name: &'a str,
    pub arguments: &'a Value,
}

impl Serialize for WireToolCall<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(3))?;
        map.serialize_entry("id", self.id)?;
        map.serialize_entry("type", "function")?;
        map.serialize_entry(
            "function",
            &WireFunction {
                name: self.name,
                arguments: self.arguments,
            },
        )?;
        map.end()
    }
}

impl Serialize for WireFunction<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(Some(2))?;
        map.serialize_entry("name", self.name)?;
        // The arguments as a JSON **string** (the OpenAI contract) —
        // re-serialized from the parsed `Value`.
        let text = serde_json::to_string(self.arguments).map_err(serde::ser::Error::custom)?;
        map.serialize_entry("arguments", &text)?;
        map.end()
    }
}

/// A partial tool call, accumulated across chunks (keyed by `index`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCallDelta {
    /// The `tool_calls[].index` (stable across a call's fragments).
    pub index: u32,
    /// Present when the provider sends the id (usually the first fragment).
    pub id: Option<String>,
    /// Name fragments (the provider may split the name across chunks).
    pub name: Option<String>,
    /// Argument-string fragments (JSON text, accumulated and parsed at
    /// the end).
    pub arguments: Option<String>,
}

/// Sampling / limits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// Passed through if the provider supports it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    /// Always `true` for `complete()` (the stream is the response).
    pub stream: bool,
}

/// Token usage reported by the provider (usually a trailing chunk).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// The WHOLE prompt the model was shown, **cached prefix included**
    /// (ADR 0046) — this is the number ADR 0028's **Anchor** is built
    /// from, so it must be the full context size the Provider held, not
    /// the uncached remainder.
    ///
    /// The wire's cached subsets (`cache_read_input_tokens` /
    /// `cache_creation_input_tokens` on the Anthropic Messages wire,
    /// `prompt_tokens_details.cached_tokens` / `input_tokens_details.cached_tokens`
    /// on the OpenAI-completions / Responses wires) are DELIBERATELY not
    /// separate fields: two of those three wires report them as a SUBSET
    /// of this number, so publishing them as parts invites every consumer
    /// to add them — which DOUBLE-COUNTS the prefix there, over-reads the
    /// Anchor and makes compaction arrive EARLY (on Anthropic, where the
    /// fields are DISJOINT, not adding them UNDER-reads and compaction
    /// arrives LATE, as a Provider 400). Each parser reconciles its own
    /// vocabulary at the seam instead: the Anthropic parser SUMS the three
    /// counts into this field; the OpenAI-shaped parsers pass their count
    /// through untouched and must keep doing so (both pinned in their own
    /// modules).
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Why the model finished its turn.
///
/// Renamed from `StopReason` (reviewer-corrected): `session.rs` already
/// exports `pub enum StopReason` (the ACP wire values). The `AgentLoop`
/// (Task 6) maps this → the session `StopReason`
/// (`Stop` → `EndTurn`, `Length` → `MaxTokens`, `Error` → via the error
/// path).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The model finished its turn normally.
    Stop,
    /// The model called tools (the loop executes them and continues).
    ToolCalls,
    /// The model hit its output token limit.
    Length,
    /// The provider reported an error.
    Error,
}

/// One event of the `complete()` stream.
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderEvent {
    /// A fragment of the assistant's text.
    TextDelta(String),
    /// A fragment of the assistant's reasoning (when the model emits it).
    ThinkingDelta(String),
    /// A partial tool call (accumulated across chunks, `index`-keyed).
    ToolCallDelta(ToolCallDelta),
    /// A complete tool call (accumulated `name` + `arguments`).
    ToolCall(ToolCall),
    /// The token usage (usually a trailing chunk) plus, when the provider
    /// priced the call, its cost in USD. `None` cost means the provider
    /// reported NONE — it is NOT zero and must never be rendered as
    /// `$0.00`; a `Some(0.0)` IS a figure and renders `$0.00` (D5). Cost
    /// rides the EVENT, not the `Usage` struct, so the Anchor type (ADR
    /// 0046) keeps its fields, its `Eq` and its doc comment untouched.
    Usage(Usage, Option<f64>),
    /// The turn is over.
    Done(FinishReason),
    /// A mid-stream error (the stream ends after this).
    Error(ProviderError),
}

/// A provider error. The `AgentLoop`'s `RetryPolicy` (Task 6) retries on
/// [`ProviderError::Retryable`].
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ProviderError {
    /// Transient (HTTP 429 / 5xx, a network failure) — worth retrying.
    #[error("retryable: {0}")]
    Retryable(String),
    /// Permanent (other 4xx, a malformed response) — not worth retrying.
    #[error("fatal: {0}")]
    Fatal(String),
    /// Missing / bad credentials (HTTP 401 / 403).
    #[error("auth: {0}")]
    Auth(String),
    /// The prompt is over the model's context window (ADR 0045) — the FOURTH
    /// tier, and deliberately NOT one the retry ladder owns. Retrying an
    /// oversized request re-sends the identical oversized bytes, so every
    /// attempt fails identically with backoff in between; the tier is
    /// recovered by the `AgentLoop` IN the turn instead (a forced compaction,
    /// then the same model call again), which is why [`Retryable`] must stay
    /// the ladder's only input.
    ///
    /// [`Retryable`]: ProviderError::Retryable
    #[error("context overflow: {0}")]
    ContextOverflow(String),
}

/// A model provider. Object-safe (a `Box<dyn Provider>` compiles) so a
/// second provider (Anthropic) can be added later without touching the
/// `AgentLoop` (Task 6).
#[async_trait]
pub trait Provider: Send + Sync {
    /// Run one model turn: a streaming chat-completions call. Returns a
    /// boxed stream of [`ProviderEvent`]s (the `AgentLoop` consumes it
    /// with `futures_util::StreamExt`).
    async fn complete(
        &self,
        req: &ModelRequest,
    ) -> Result<BoxStream<'static, ProviderEvent>, ProviderError>;
}

/// Read the per-call cost (USD) out of a wire `usage` object (D4,
/// AMENDED: money arrives in the response **BODY**, on the usage object
/// each parser already reads — NOT the `x-litellm-response-cost` header,
/// which commits before a stream ends and so carries a pre-stream value).
///
/// Field order is D4's own, with no invented precedence: `cost` (LiteLLM's
/// injection into the final usage chunk, and OpenRouter's field) first,
/// then the two known variants — `cost_details.upstream_inference_cost`
/// and `estimated_cost` (DeepInfra). A later field is consulted only when
/// the earlier one is ABSENT.
///
/// Anything absent, non-numeric, negative or non-finite is `None` — never
/// `0.0`, never a clamp, never a panic. `None` means the provider reported
/// NO figure and must render as nothing; a real `0` from the provider is
/// `Some(0.0)` and renders `$0.00` (D5).
///
/// `is_finite()` is defence-in-depth, not the load-bearing filter: with
/// `arbitrary_precision` OFF (the resolved feature set here),
/// `as_f64()`'s non-arbitrary arm is a bare `Some(n)` with no finite
/// check, so `>= 0.0` alone would let an `inf` through. Today a non-finite
/// literal never reaches this reader at all — serde_json rejects the
/// WHOLE chunk with `number out of range`, which each parser swallows
/// (pinned as a no-event case on the OpenAI side) — but one comparison is
/// a cheap barrier between a future dependency change and an `$inf` on
/// screen, so it stays.
pub fn cost_usd_from_usage(usage: &Value) -> Option<f64> {
    // The FIRST PRESENT field answers, in D4's order (`.flatten()` skips
    // only the ABSENT ones).
    let value = [
        usage.get("cost"),
        usage
            .get("cost_details")
            .and_then(|d| d.get("upstream_inference_cost")),
        usage.get("estimated_cost"),
    ]
    .into_iter()
    .flatten()
    .next()?;
    // A PRESENT field is answered even when it is unusable: a malformed
    // `cost` is `None`, not a licence to fall through to a different
    // field's figure (which would report a number the primary did not
    // carry).
    match value.as_f64() {
        Some(v) if v.is_finite() && v >= 0.0 => Some(v),
        _ => None,
    }
}

/// Classify an in-stream SSE `error` chunk into a [`ProviderError`] — the
/// ONE classifier of the three wires (the `cost_usd_from_usage` precedent:
/// a shared reader of a `Value` each parser already holds, so no per-wire
/// copy can drift).
///
/// The wire `type` is the classification: `overloaded_error` / `api_error`
/// are transient, `authentication_error` / `permission_error` are auth, the
/// rest are permanent; an UNKNOWN type is conservative — retryable, a new
/// error type is probably transient, and the `RetryPolicy`'s budget bounds
/// the damage. The ONE exception is the context-overflow message (ADR 0045),
/// which is checked FIRST because Anthropic reports an over-window prompt as
/// an ordinary `invalid_request_error`.
///
/// (ADR 0045) An over-window prompt arrives here as an
/// `invalid_request_error` — a `kind` that maps to `Fatal` on its own — so
/// the MESSAGE has to decide the tier, before the `kind` match. This is the
/// only place a mid-stream `ProviderEvent::Error` is built, so without it
/// nothing could ever produce a mid-stream overflow and the loop's
/// mid-stream recovery arm would be dead code. The exclusions run first
/// inside the check, so a throttle stays the tier its `kind` says.
///
/// `None` means NOT an error chunk — the chunk carries no `error` key at
/// all, and the caller proceeds with its normal parsing. An `error` object
/// that is present but empty is NOT `None`: the `"(no message)"` and
/// `""`-kind fallbacks run and yield `Retryable("unknown: (no message)")`,
/// exactly the behaviour the Anthropic wire has always had (the format
/// string `"{kind}: {message}"` is byte-identical to that wire's, so its
/// behaviour is preserved by construction).
pub fn parse_error_chunk(v: &Value) -> Option<ProviderError> {
    let error = v.get("error")?;
    let message = error
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("(no message)");
    let kind = error.get("type").and_then(|t| t.as_str()).unwrap_or("");
    // (ADR 0045) THE ORDER IS LOAD-BEARING: the overflow check runs BEFORE
    // the `kind` match (see the doc above), and the exclusions run first
    // inside the check, so a throttle stays the tier its `kind` says.
    if crate::provider_overflow::is_context_overflow_text(message) {
        return Some(ProviderError::ContextOverflow(format!("{kind}: {message}")));
    }
    Some(match kind {
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
    })
}

#[cfg(test)]
mod tests {
    use super::{
        cost_usd_from_usage, parse_error_chunk, ChatMessage, ChatRole, ProviderError, ToolCall,
    };
    use serde_json::json;

    /// (issue #193) A HISTORICAL `content_json` row — hand-written in the
    /// PRE-CHANGE shape, with no `defect` key anywhere — still deserializes.
    ///
    /// This is the desktop-compatibility acceptance criterion: the desktop reads
    /// these rows out of the same SQLite file, and every one of them predates
    /// the field. The mechanism is serde's own, and deliberately NOT a
    /// `#[serde(default)]`: `serde_derive` emits a `missing_field` path for an
    /// absent field and serde's `MissingFieldDeserializer` answers
    /// `deserialize_option` with `visit_none()`, so a missing `Option` is `None`
    /// for free. The in-crate precedent is `ChatMessage::tool_call_id`,
    /// `tool_calls` and `ModelOptions` — `skip_serializing_if` alone, no
    /// `default`. Adding `#[serde(default)]` here would be a second, redundant
    /// mechanism, and this test is what makes its absence a CHOICE rather than
    /// an oversight.
    ///
    /// The string is a literal on purpose: building it with `json!` from a
    /// current `ToolCall` would let a future change to the struct silently
    /// rewrite the fixture, and the whole point is a document that CANNOT change.
    #[test]
    fn a_pre_change_row_with_no_defect_key_still_deserializes() {
        // Exactly what `emit.rs`'s `serde_json::to_string(&m)` wrote before this
        // field existed: the flat `{id, name, arguments}` tool call, no `defect`.
        const PRE_CHANGE_ROW: &str = concat!(
            r#"{"role":"assistant","content":"let me read that","#,
            r#""tool_calls":[{"id":"call_1","name":"read","#,
            r#""arguments":{"path":"/x"}}]}"#,
        );
        let message: ChatMessage = serde_json::from_str(PRE_CHANGE_ROW)
            .expect("a historical content_json row must still deserialize");
        let calls = message.tool_calls.expect("the row's tool_calls survive");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].name, "read");
        assert_eq!(calls[0].arguments, json!({ "path": "/x" }));
        assert_eq!(
            calls[0].defect, None,
            "a row written before the field existed carries NO defect"
        );

        // The bare `ToolCall` row on its own (the `tool-call` display row and
        // the `native_messages` shape ride this serialization too).
        const PRE_CHANGE_TOOL_CALL: &str =
            r#"{"id":"call_2","name":"bash","arguments":{"command":"ls"}}"#;
        let call: ToolCall = serde_json::from_str(PRE_CHANGE_TOOL_CALL)
            .expect("a pre-change tool-call object must still deserialize");
        assert_eq!(call.defect, None);
        assert_eq!(call.name, "bash");

        // And the round-trip back out is BYTE-IDENTICAL to what went in: the
        // field is `skip_serializing_if`-guarded, so re-serialifying a row that
        // had no `defect` must not ADD one. This is the leak guard at the
        // transcript layer (its display-wire twin is
        // `tool_call_display_wire_stays_flat`).
        let round_tripped = serde_json::to_string(&call).expect("serialization cannot fail");
        assert_eq!(
            round_tripped, PRE_CHANGE_TOOL_CALL,
            "a defect-free call re-serializes to the pre-change bytes — no \
             `\"defect\":null` is added to any row"
        );
        assert!(
            !round_tripped.contains("defect"),
            "the key is absent, not null: {round_tripped}"
        );

        // `ChatRole::Assistant` was actually parsed (the literal above must not
        // be silently answering a default).
        assert_eq!(message.role, ChatRole::Assistant);
    }

    #[test]
    fn cost_usd_from_usage_reads_the_primary_cost_field() {
        // `usage.cost` — LiteLLM's injection into the final usage chunk,
        // and OpenRouter's field (D4's primary).
        let usage = json!({"prompt_tokens": 10, "cost": 0.000011662});
        assert_eq!(cost_usd_from_usage(&usage), Some(0.000011662));
    }

    #[test]
    fn cost_usd_from_usage_falls_back_to_the_upstream_inference_cost_detail() {
        // `usage.cost_details.upstream_inference_cost` — the known
        // variant D4 names, read only when the primary is ABSENT.
        let usage = json!({"cost_details": {"upstream_inference_cost": 0.005}});
        assert_eq!(cost_usd_from_usage(&usage), Some(0.005));
    }

    #[test]
    fn cost_usd_from_usage_reads_the_estimated_cost_variant() {
        // `usage.estimated_cost` — DeepInfra's field (D4's other named
        // variant). Never observed on a wire reachable from here; pinned
        // by canned JSON only (the plan records that).
        let usage = json!({"estimated_cost": 0.00042});
        assert_eq!(cost_usd_from_usage(&usage), Some(0.00042));
    }

    #[test]
    fn cost_usd_from_usage_prefers_the_gateway_cost_over_the_upstream_detail() {
        // Both present → the PRIMARY wins. D4: "Read `cost` first; do not
        // invent a precedence rule between a gateway's figure and an
        // upstream's" — first-present IS D4's order, and the values here
        // are deliberately DIFFERENT so a swapped order cannot pass.
        // (On the live gateway both fields are numerically identical, so
        // only a differing fixture can pin the order.)
        let usage = json!({
            "cost": 0.002,
            "cost_details": {"upstream_inference_cost": 0.009},
            "estimated_cost": 0.007,
        });
        assert_eq!(cost_usd_from_usage(&usage), Some(0.002));
    }

    #[test]
    fn cost_usd_from_usage_returns_none_when_no_cost_field_is_present() {
        // The ordinary case on a direct Anthropic / OpenAI endpoint, and
        // on a LiteLLM cache hit (probe P6). Absence is NOT zero.
        assert_eq!(
            cost_usd_from_usage(&json!({"prompt_tokens": 10, "completion_tokens": 5})),
            None
        );
        assert_eq!(cost_usd_from_usage(&json!({})), None);
    }

    #[test]
    fn cost_usd_from_usage_returns_none_for_a_non_numeric_or_negative_cost() {
        // A provider that is not telling the truth about money gets NO
        // figure: never a panic, never a clamp, never `Some(0.0)`.
        for usage in [
            json!({"cost": "abc"}),                  // a string
            json!({"cost": null}),                   // an explicit null
            json!({"cost": -0.5}),                   // negative
            json!({"cost": {}}),                     // an object
            json!({"cost": true}),                   // a bool
            json!({"cost_details": "nope"}),         // a non-object detail
            json!({"cost_details": {"other": 1.0}}), // the named key absent
            json!({"estimated_cost": "abc"}),        // the variant, non-numeric
        ] {
            assert_eq!(
                cost_usd_from_usage(&usage),
                None,
                "expected None for {usage}"
            );
        }
    }

    #[test]
    fn cost_usd_from_usage_keeps_a_real_zero_as_some_zero() {
        // D5: `0` from the provider IS a figure (it renders `$0.00`);
        // only ABSENCE is `None`. A `0.0` sentinel for absence would make
        // an unpriced call look free.
        assert_eq!(cost_usd_from_usage(&json!({"cost": 0})), Some(0.0));
        assert_eq!(cost_usd_from_usage(&json!({"cost": 0.0})), Some(0.0));
    }

    // ── `parse_error_chunk` (the ONE classifier of the three wires) ──────
    //
    // (Moved from `anthropic.rs`'s `map_anthropic_error_*` tests — the
    // lifted classifier is byte-identical, so the inputs and expectations
    // are textually unchanged, only the call shape gained an `.unwrap()`.)

    #[test]
    fn parse_error_chunk_maps_wire_kind_tiers() {
        fn err(kind: Option<&str>) -> ProviderError {
            let mut error = serde_json::Map::new();
            error.insert("message".to_string(), json!("boom"));
            if let Some(k) = kind {
                error.insert("type".to_string(), json!(k));
            }
            parse_error_chunk(&json!({ "type": "error", "error": error })).expect("an error chunk")
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

    /// (ADR 0045) `parse_error_chunk` is the ONLY producer of mid-stream
    /// `ProviderError`s, so it is the only place a mid-stream SSE `error`
    /// event can become a `ContextOverflow` — without it the loop's
    /// mid-stream recovery arm would be unreachable. Anthropic reports an
    /// over-window prompt as `invalid_request_error` (which maps to `Fatal`
    /// on its own), so the MESSAGE decides: the overflow check runs BEFORE
    /// the `kind` match, and a throttle / rate-limit message is excluded
    /// first (a Bedrock-style throttle behind a gateway stays the tier its
    /// `kind` says).
    #[test]
    fn parse_error_chunk_detects_a_context_overflow_before_the_kind_match() {
        fn err(kind: &str, message: &str) -> ProviderError {
            parse_error_chunk(&json!({
                "type": "error",
                "error": { "type": kind, "message": message },
            }))
            .expect("an error chunk")
        }
        // The real Anthropic wording, behind the `kind` that used to be
        // `Fatal`: the tier is the overflow, message included.
        assert_eq!(
            err(
                "invalid_request_error",
                "prompt is too long: 213462 tokens > 200000 maximum"
            ),
            ProviderError::ContextOverflow(
                "invalid_request_error: prompt is too long: 213462 tokens > 200000 maximum"
                    .to_string()
            )
        );
        // The `kind` does not have to be `invalid_request_error` — the
        // message is what the tier reads (a gateway may wrap it).
        for kind in ["api_error", "mystery_error", ""] {
            assert!(
                matches!(
                    err(kind, "Your input exceeds the context window of this model"),
                    ProviderError::ContextOverflow(_)
                ),
                "{kind} with an overflow message is the overflow tier"
            );
        }
        // An `invalid_request_error` that is NOT about length stays `Fatal`
        // (a bad tool spec, a malformed body — nothing a compaction fixes).
        assert_eq!(
            err("invalid_request_error", "boom"),
            ProviderError::Fatal("invalid_request_error: boom".to_string())
        );
        // The exclusions run first, so a throttle behind an
        // `invalid_request_error` is not read as an overflow.
        assert_eq!(
            err(
                "invalid_request_error",
                "ThrottlingException: Too many tokens, please wait before trying again."
            ),
            ProviderError::Fatal(
                "invalid_request_error: ThrottlingException: Too many tokens, please wait before trying again."
                    .to_string()
            )
        );
    }

    /// The `None`-discipline (the `cost_usd_from_usage` precedent): `None`
    /// means the chunk carries NO `error` key at all, so the caller proceeds
    /// with its normal parsing. An `error` key that is PRESENT is always
    /// `Some` — even an empty object, which falls through the
    /// `"(no message)"` / `""`-kind fallbacks to the `unknown`-labelled
    /// `Retryable` the Anthropic wire has always produced.
    #[test]
    fn parse_error_chunk_returns_none_for_non_error_bodies() {
        for chunk in [
            json!({"choices": []}),
            json!({"choices": [{"delta": {"content": "x"}}]}),
            json!({"usage": {"prompt_tokens": 1, "completion_tokens": 1}}),
            json!({}),
        ] {
            assert_eq!(
                parse_error_chunk(&chunk),
                None,
                "a chunk with no `error` key is not an error chunk: {chunk}"
            );
        }
        // PRESENT-but-empty is NOT `None` (the fallbacks run — today's
        // Anthropic behaviour, byte-identical).
        assert_eq!(
            parse_error_chunk(&json!({"error": {}})),
            Some(ProviderError::Retryable(
                "unknown: (no message)".to_string()
            ))
        );
    }
}

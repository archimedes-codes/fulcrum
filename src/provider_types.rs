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

use crate::agent::tools::ContentBlock;

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
}

/// The REQUEST-wire shape of a tool call (the OpenAI contract: a nested
/// `function` object, a `type: "function"` tag, and `arguments` as a JSON
/// **string**). Used ONLY when building the `request_body` messages — the
/// display wire (`toolcall_end` / `tool_execution_start`) keeps the flat
/// [`ToolCall`] shape, and `ChatMessage`'s own serialization (the DB /
/// transcript) is untouched.
///
/// `pub(super)`: `provider/mod.rs`'s `request_body` constructs these.
pub(super) struct WireToolCall<'a> {
    pub(super) id: &'a str,
    pub(super) name: &'a str,
    pub(super) arguments: &'a Value,
}

/// The `function` object of a [`WireToolCall`].
pub(super) struct WireFunction<'a> {
    pub(super) name: &'a str,
    pub(super) arguments: &'a Value,
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
/// [`Retryable`].
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
pub(crate) fn cost_usd_from_usage(usage: &Value) -> Option<f64> {
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

#[cfg(test)]
mod tests {
    use super::cost_usd_from_usage;
    use serde_json::json;

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
}

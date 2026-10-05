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
    /// The token usage (usually a trailing chunk).
    Usage(Usage),
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

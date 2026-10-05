//! Shared test helpers for the `provider` module's `#[cfg(test)]` mods —
//! the stream collector and the `ChatMessage` test-literal helper, used
//! across the OpenAI / Anthropic / Responses suites. Declared in
//! `provider/mod.rs` (`#[cfg(test)] pub(crate) mod test_util;`); imported
//! by each per-provider `#[cfg(test)]` mod — NO per-file duplicates.
//! (The per-parser `*_stream` / `*_stream_bytes` builders stay local to
//! the module that owns the parser — their return type is private there.)

use futures_util::{Stream, StreamExt};

use super::types::{ChatMessage, ChatRole, MessageContent, ProviderEvent, ToolCall};

/// Collect the whole stream (generic over the parser — the `SseStream`
/// and the `AnthropicStream` suites share it).
pub(crate) async fn collect<S: Stream<Item = ProviderEvent> + Unpin>(s: S) -> Vec<ProviderEvent> {
    s.collect().await
}

/// A `ChatMessage` test-literal helper (the `None` fields default to
/// `None`).
pub(crate) fn chat_message(
    role: ChatRole,
    content: MessageContent,
    tool_call_id: Option<&str>,
    tool_calls: Option<Vec<ToolCall>>,
) -> ChatMessage {
    ChatMessage {
        role,
        content,
        tool_call_id: tool_call_id.map(|s| s.to_string()),
        tool_calls,
    }
}

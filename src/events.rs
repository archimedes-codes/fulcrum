//! The native harness's event vocabulary.
//!
//! The `RpcEvent` enum is the wire-shape vocabulary of a session's event
//! stream: one variant per event in the `agent_settled`/`assistantMessageEvent`
//! catalog, with the exact camelCase wire field names (serde
//! `rename_all = "camelCase"` on the enum renames the variant field names;
//! the `set_editor_text` method keeps its snake_case wire name via an
//! explicit variant `rename`).
//!
//! Deserialization is **permissive of unknown events**: the reader parses
//! each line as `Value` first, routes by `type`, and deserializes known types
//! into their variants; an unknown `type` becomes `Unknown { raw }` (never an
//! error — the surface is unversioned and new events will appear).
// NOTE: the `type` discriminators are snake_case on the wire (only the
// PAYLOAD field names are camelCase) — so the variant names ARE the wire
// names, and `rename_all = "camelCase"` is applied PER-VARIANT (to the
// fields only). An enum-level `rename_all` would rename the tag values too
// (`AgentSettled` -> `agentSettled`) and every event would fall through to
// `unknown`.
// The variant names are intentionally the wire names (snake_case) — the
// `type` discriminators are snake_case on the wire and the variant name IS
// the tag value (an enum-level `rename_all` would corrupt it), so the
// casing lint is suppressed for this type.

use serde_json::Value;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum RpcEvent {
    agent_start,
    #[serde(rename_all = "camelCase")]
    agent_end {
        messages: Vec<Value>,
        will_retry: bool,
    },
    agent_settled,
    turn_start,
    #[serde(rename_all = "camelCase")]
    turn_end {
        message: Value,
        tool_results: Vec<Value>,
    },
    #[serde(rename_all = "camelCase")]
    message_start {
        message: Value,
    },
    #[serde(rename_all = "camelCase")]
    message_end {
        message: Value,
    },
    #[serde(rename_all = "camelCase")]
    message_update {
        usage: Value,
        assistant_message_event: Value,
    },
    #[serde(rename_all = "camelCase")]
    text_start {
        content_index: u64,
    },
    #[serde(rename_all = "camelCase")]
    text_delta {
        content_index: u64,
        delta: String,
    },
    #[serde(rename_all = "camelCase")]
    text_end {
        content_index: u64,
        content: String,
    },
    #[serde(rename_all = "camelCase")]
    thinking_start {
        content_index: u64,
    },
    #[serde(rename_all = "camelCase")]
    thinking_delta {
        content_index: u64,
        delta: String,
    },
    #[serde(rename_all = "camelCase")]
    thinking_end {
        content_index: u64,
        content: String,
    },
    #[serde(rename_all = "camelCase")]
    toolcall_start {
        content_index: u64,
        id: String,
        tool_name: String,
    },
    #[serde(rename_all = "camelCase")]
    toolcall_delta {
        content_index: u64,
        delta: String,
    },
    #[serde(rename_all = "camelCase")]
    toolcall_end {
        tool_call: Value,
    },
    #[serde(rename_all = "camelCase")]
    tool_execution_start {
        tool_call_id: String,
        tool_name: String,
        args: Value,
    },
    #[serde(rename_all = "camelCase")]
    tool_execution_update {
        tool_call_id: String,
        tool_name: String,
        args: Value,
        partial_result: Value,
    },
    #[serde(rename_all = "camelCase")]
    tool_execution_end {
        tool_call_id: String,
        tool_name: String,
        result: Value,
        is_error: bool,
    },
    #[serde(rename_all = "camelCase")]
    queue_update {
        steering: Vec<String>,
        follow_up: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    entry_appended {
        entry: Value,
    },
    #[serde(rename_all = "camelCase")]
    session_info_changed {
        name: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    thinking_level_changed {
        level: String,
    },
    #[serde(rename_all = "camelCase")]
    compaction_start {
        reason: String,
    },
    #[serde(rename_all = "camelCase")]
    compaction_end {
        reason: String,
        result: Option<Value>,
        aborted: bool,
        will_retry: bool,
        error_message: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    auto_retry_start {
        attempt: u64,
        max_attempts: u64,
        delay_ms: u64,
        error_message: String,
    },
    #[serde(rename_all = "camelCase")]
    auto_retry_end {
        success: bool,
        attempt: u64,
        final_error: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    summarization_retry_scheduled {
        attempt: u64,
        max_attempts: u64,
        delay_ms: u64,
        error_message: String,
    },
    #[serde(rename_all = "camelCase")]
    summarization_retry_attempt_start {
        source: String,
        reason: Option<String>,
    },
    summarization_retry_finished,
    #[serde(rename_all = "camelCase")]
    bash_execution_update {
        id: Option<String>,
        delta: String,
    },
    #[serde(rename_all = "camelCase")]
    extension_error {
        extension_path: String,
        event: String,
        error: String,
    },
    /// An event `type` this version of the client doesn't know (permissive
    /// of unknown events — the raw line is preserved for logging/inspection).
    #[serde(rename_all = "camelCase")]
    unknown {
        raw: Value,
    },
}

impl RpcEvent {
    /// The wire `type` discriminator (for tests + the normalizer's match).
    pub fn kind(&self) -> std::borrow::Cow<'static, str> {
        match self {
            RpcEvent::agent_start => std::borrow::Cow::Borrowed("agent_start"),
            RpcEvent::agent_end { .. } => std::borrow::Cow::Borrowed("agent_end"),
            RpcEvent::agent_settled => std::borrow::Cow::Borrowed("agent_settled"),
            RpcEvent::turn_start => std::borrow::Cow::Borrowed("turn_start"),
            RpcEvent::turn_end { .. } => std::borrow::Cow::Borrowed("turn_end"),
            RpcEvent::message_start { .. } => std::borrow::Cow::Borrowed("message_start"),
            RpcEvent::message_end { .. } => std::borrow::Cow::Borrowed("message_end"),
            RpcEvent::message_update { .. } => std::borrow::Cow::Borrowed("message_update"),
            RpcEvent::text_start { .. } => std::borrow::Cow::Borrowed("text_start"),
            RpcEvent::text_delta { .. } => std::borrow::Cow::Borrowed("text_delta"),
            RpcEvent::text_end { .. } => std::borrow::Cow::Borrowed("text_end"),
            RpcEvent::thinking_start { .. } => std::borrow::Cow::Borrowed("thinking_start"),
            RpcEvent::thinking_delta { .. } => std::borrow::Cow::Borrowed("thinking_delta"),
            RpcEvent::thinking_end { .. } => std::borrow::Cow::Borrowed("thinking_end"),
            RpcEvent::toolcall_start { .. } => std::borrow::Cow::Borrowed("toolcall_start"),
            RpcEvent::toolcall_delta { .. } => std::borrow::Cow::Borrowed("toolcall_delta"),
            RpcEvent::toolcall_end { .. } => std::borrow::Cow::Borrowed("toolcall_end"),
            RpcEvent::tool_execution_start { .. } => {
                std::borrow::Cow::Borrowed("tool_execution_start")
            }
            RpcEvent::tool_execution_update { .. } => {
                std::borrow::Cow::Borrowed("tool_execution_update")
            }
            RpcEvent::tool_execution_end { .. } => std::borrow::Cow::Borrowed("tool_execution_end"),
            RpcEvent::queue_update { .. } => std::borrow::Cow::Borrowed("queue_update"),
            RpcEvent::entry_appended { .. } => std::borrow::Cow::Borrowed("entry_appended"),
            RpcEvent::session_info_changed { .. } => {
                std::borrow::Cow::Borrowed("session_info_changed")
            }
            RpcEvent::thinking_level_changed { .. } => {
                std::borrow::Cow::Borrowed("thinking_level_changed")
            }
            RpcEvent::compaction_start { .. } => std::borrow::Cow::Borrowed("compaction_start"),
            RpcEvent::compaction_end { .. } => std::borrow::Cow::Borrowed("compaction_end"),
            RpcEvent::auto_retry_start { .. } => std::borrow::Cow::Borrowed("auto_retry_start"),
            RpcEvent::auto_retry_end { .. } => std::borrow::Cow::Borrowed("auto_retry_end"),
            RpcEvent::summarization_retry_scheduled { .. } => {
                std::borrow::Cow::Borrowed("summarization_retry_scheduled")
            }
            RpcEvent::summarization_retry_attempt_start { .. } => {
                std::borrow::Cow::Borrowed("summarization_retry_attempt_start")
            }
            RpcEvent::summarization_retry_finished => {
                std::borrow::Cow::Borrowed("summarization_retry_finished")
            }
            RpcEvent::bash_execution_update { .. } => {
                std::borrow::Cow::Borrowed("bash_execution_update")
            }
            RpcEvent::extension_error { .. } => std::borrow::Cow::Borrowed("extension_error"),
            RpcEvent::unknown { raw } => raw
                .get("type")
                .and_then(|t| t.as_str())
                // The type string is borrowed from `raw` (which borrows from
                // `&self`) — an owned `String` is needed for the 'static
                // return (the literal arms coerce to `Cow::Borrowed`).
                .map(|s| std::borrow::Cow::Owned(s.to_string()))
                .unwrap_or_else(|| std::borrow::Cow::Borrowed("unknown")),
        }
    }
}

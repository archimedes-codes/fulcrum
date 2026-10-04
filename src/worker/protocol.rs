//! The Worker IPC wire vocabulary (ADR 0025 Task 2): the shared JSONL
//! protocol between the Supervisor (the Tauri app process) and a Worker
//! (`archimedes --worker` — one session's `AgentLoop` over stdio).
//!
//! The conventions mirror `RpcEvent` (`agent/events.rs`): the `type`
//! discriminators are snake_case on the wire (the variant `rename`s ARE
//! the wire names — an enum-level `rename_all` would corrupt them), and
//! the PAYLOAD field names are camelCase (per-variant
//! `rename_all = "camelCase"`).
//!
//! The `Model` / `ModelCatalog` / `PermissionOutcome` / `ChatMessage` /
//! `ImageRef` / `LaunchConfig` / `SubagentOutcome` types already derive
//! serde — they ride the wire VERBATIM (no separate provider wire type:
//! the `Model` carries the `api` / `base_url` / `api_key` the
//! `build_provider` uses).
//!
//! Decoding is **permissive of unknown `type`s**: the decoder parses the
//! line as `Value` first, routes by `type`, and deserializes known types
//! into their variants; an unknown `type` becomes `Unknown { raw }`
//! (never an error — the surface is unversioned).

use serde_json::Value;

/// The session-start mode (the `start` envelope's `mode` field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartMode {
    /// A fresh session: the Worker builds the main system prompt itself
    /// (the seq-0 transcript row).
    Fresh,
    /// A resume: the Supervisor rehydrated the provider transcript (the
    /// `native_messages` re-read — moved from harness to Supervisor) and
    /// it arrives in the envelope's `transcript`.
    Resume,
}

/// Supervisor → Worker.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum Inbound {
    /// The session-start envelope (handshake: spawn → the Worker emits
    /// `Ready` → the Supervisor sends `Start`).
    ///
    /// `Box`ed — the largest variant (keeps the enum small; serde is
    /// transparent to the box). The payload is the `StartEnv` verbatim
    /// (camelCase via `StartEnv`'s own `rename_all`).
    #[serde(rename = "start")]
    Start(Box<StartEnv>),
    /// The existing `Prompt` type's fields verbatim.
    #[serde(rename = "prompt", rename_all = "camelCase")]
    Prompt {
        text: String,
        images: Vec<crate::agent::tools::ImageRef>,
    },
    #[serde(rename = "config", rename_all = "camelCase")]
    Config {
        model: Option<crate::agent::harness::catalog::Model>,
        thinking: Option<String>,
        trusted: Option<bool>,
    },
    #[serde(rename = "abort")]
    Abort,
    #[serde(rename = "close")]
    Close,
    /// The REAL outcome (3-option UI: `Selected { option_id }` ∈
    /// allow / reject / trust-space, or `Cancelled`) — NOT a bool.
    #[serde(rename = "permission-response", rename_all = "camelCase")]
    PermissionResponse {
        id: String,
        outcome: crate::agent::permission::PermissionOutcome,
    },
    #[serde(rename = "interactive-response", rename_all = "camelCase")]
    InteractiveResponse { id: String, value: Value },
    #[serde(rename = "subagent-result", rename_all = "camelCase")]
    SubagentResult {
        id: String,
        outcome: crate::agent::subagent::SubagentOutcome,
    },
    /// An inbound `type` this version of the Worker doesn't know
    /// (permissive — the raw line is preserved; never an error).
    #[serde(rename = "unknown", rename_all = "camelCase")]
    Unknown { raw: Value },
}

/// Worker → Supervisor.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum Outbound {
    /// Emitted at startup, BEFORE `Start` (`session_id: None`).
    #[serde(rename = "ready", rename_all = "camelCase")]
    Ready {
        version: String,
        session_id: Option<String>,
    },
    /// The existing event vocabulary, verbatim — drives the
    /// Supervisor's INTERNAL bookkeeping (settle detection,
    /// `pending_turn` resolution, the debug log) — NOT the UI.
    #[serde(rename = "event", rename_all = "camelCase")]
    Event {
        event: crate::agent::events::RpcEvent,
    },
    // The persistence frames (the `Store` seam over IPC — `IpcStore`
    // emits these; the Supervisor's `TranscriptPersister` applies them).
    /// `Store::insert_message` — the provider transcript rows (system
    /// prompt seq 0, user messages, assistant + tool-role messages).
    #[serde(rename = "transcript-insert", rename_all = "camelCase")]
    TranscriptInsert {
        session_id: String,
        seq: u64,
        role: String,
        content_json: String,
    },
    /// `Store::replace_messages` — the compaction rewrite.
    #[serde(rename = "transcript-replace", rename_all = "camelCase")]
    TranscriptReplace {
        session_id: String,
        rows: Vec<(u64, String, String)>,
    },
    /// `Store::persist_display` — the display `messages` rows.
    #[serde(rename = "display-upsert", rename_all = "camelCase")]
    DisplayUpsert {
        session_id: String,
        rows: Vec<crate::agent::harness::store::DisplayRow>,
    },
    /// `Store::record_context_usage` — the `sessions.context_usage_json`
    /// write.
    #[serde(rename = "context-usage", rename_all = "camelCase")]
    ContextUsage {
        session_id: String,
        used: u64,
        window: u64,
    },
    // The control frames — the FULL original event payloads VERBATIM
    // (the frontend consumes exactly those fields; a lossy restatement
    // cannot reproduce them).
    /// `id` = the payload's `requestId` (the `PendingPermissions` map
    /// key — the same key the loop used); `payload` = the gate's
    /// `permission-request` sink payload VERBATIM.
    #[serde(rename = "permission-request", rename_all = "camelCase")]
    PermissionRequest { id: String, payload: Value },
    /// `id` = the payload's request key; `payload` = the
    /// `interactive-request` sink payload VERBATIM.
    #[serde(rename = "interactive-request", rename_all = "camelCase")]
    InteractiveRequest { id: String, payload: Value },
    /// The RESOLVED model key (the loop's pre-flight resolved it against
    /// the Worker's catalog); the Supervisor resolves the key →
    /// provider config (it owns the catalog + settings) → the child's
    /// `Start`.
    #[serde(rename = "subagent-dispatch", rename_all = "camelCase")]
    SubagentDispatch {
        id: String,
        parent_session_id: String,
        parent_cwd: String,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: crate::agent::subagent::LaunchConfig,
        task: String,
        model_key: String,
    },
    #[serde(rename = "subagent-cancel", rename_all = "camelCase")]
    SubagentCancel { id: String },
    /// The CATCH-ALL for every other `EventSink` emission — the ENTIRE
    /// UI contract (the `interactive-event` todo frames,
    /// `interactive-request-close` modal cleanup, the `session-update`
    /// context-usage display frames — the frontend contract is
    /// preserved by the Supervisor re-emitting `SinkFrame` as the
    /// same-named Tauri event verbatim).
    #[serde(rename = "sink-frame", rename_all = "camelCase")]
    SinkFrame { event: String, payload: Value },
    #[serde(rename = "worker-error", rename_all = "camelCase")]
    WorkerError {
        code: String,
        message: String,
        backtrace: Option<String>,
    },
    /// An outbound `type` this version of the Supervisor doesn't know
    /// (permissive — the raw line is preserved; never an error).
    #[serde(rename = "unknown", rename_all = "camelCase")]
    Unknown { raw: Value },
}

/// The `start` envelope as a standalone struct (Tasks 3/4/7 reference
/// it): the `Inbound::Start` payload fields, verbatim.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartEnv {
    pub session_id: String,
    pub cwd: String,
    pub mode: StartMode,
    pub transcript: Option<Vec<crate::agent::harness::provider::ChatMessage>>,
    pub model: crate::agent::harness::catalog::Model,
    pub catalog: crate::agent::harness::catalog::ModelCatalog,
    pub thinking: Option<String>,
    pub trusted: bool,
    pub enabled_tools: Option<Vec<String>>,
    pub config_dir: String,
    pub subagent_enabled: bool,
    pub system_prompt: Option<String>,
}

impl StartEnv {
    /// The Task-3/4/5 construction site.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        session_id: String,
        cwd: String,
        mode: StartMode,
        transcript: Option<Vec<crate::agent::harness::provider::ChatMessage>>,
        model: crate::agent::harness::catalog::Model,
        catalog: crate::agent::harness::catalog::ModelCatalog,
        thinking: Option<String>,
        trusted: bool,
        enabled_tools: Option<Vec<String>>,
        config_dir: String,
        subagent_enabled: bool,
        system_prompt: Option<String>,
    ) -> Self {
        Self {
            session_id,
            cwd,
            mode,
            transcript,
            model,
            catalog,
            thinking,
            trusted,
            enabled_tools,
            config_dir,
            subagent_enabled,
            system_prompt,
        }
    }
}

/// The `Start` variant from a `StartEnv` (the round-trip helper — the
/// variant IS the boxed `StartEnv`).
impl From<&StartEnv> for Inbound {
    fn from(env: &StartEnv) -> Self {
        Inbound::Start(Box::new(env.clone()))
    }
}

/// The store-frame summary (Task 4's `TranscriptPersister` consumes it —
/// the `Outbound` persistence frames in a single vocabulary).
#[derive(Debug, Clone, PartialEq)]
pub enum StoreFrame {
    Insert {
        session_id: String,
        seq: u64,
        role: String,
        content_json: String,
    },
    Replace {
        session_id: String,
        rows: Vec<(u64, String, String)>,
    },
    Display {
        session_id: String,
        rows: Vec<crate::agent::harness::store::DisplayRow>,
    },
    ContextUsage {
        session_id: String,
        used: u64,
        window: u64,
    },
}

/// The `SubagentDispatch` payload as a standalone struct (Task 3's
/// `dispatch_subagent` consumes it).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentDispatchWire {
    pub id: String,
    pub parent_session_id: String,
    pub parent_cwd: String,
    pub parent_enabled_tools: Vec<String>,
    pub agent_name: String,
    pub launch: crate::agent::subagent::LaunchConfig,
    pub task: String,
    pub model_key: String,
}

/// A decode error (a malformed line, or a KNOWN `type` whose payload
/// doesn't fit — an UNKNOWN `type` is NOT an error: it decodes to the
/// `Unknown` variant).
#[derive(Debug)]
pub enum ProtoError {
    Json(String),
}

impl std::fmt::Display for ProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoError::Json(msg) => write!(f, "protocol json error: {msg}"),
        }
    }
}

impl std::error::Error for ProtoError {}

/// Encode one frame as a single JSON line (`\n`-terminated).
pub fn encode_inbound(msg: &Inbound) -> String {
    encode_line(msg)
}

/// Encode one frame as a single JSON line (`\n`-terminated).
pub fn encode_outbound(msg: &Outbound) -> String {
    encode_line(msg)
}

fn encode_line<T: serde::Serialize>(msg: &T) -> String {
    format!(
        "{}\n",
        serde_json::to_string(msg).expect("the protocol types are always encodable")
    )
}

/// Decode one inbound line. PERMISSIVE of unknown `type`s (the
/// `Unknown { raw }` variant — never an error: the surface is
/// unversioned).
pub fn decode_inbound(line: &str) -> Result<Inbound, ProtoError> {
    let value: Value = serde_json::from_str(line).map_err(|e| ProtoError::Json(e.to_string()))?;
    let ty = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match ty {
        "start"
        | "prompt"
        | "config"
        | "abort"
        | "close"
        | "permission-response"
        | "interactive-response"
        | "subagent-result" => {
            serde_json::from_value(value).map_err(|e| ProtoError::Json(e.to_string()))
        }
        // Unknown `type` (and the literal `unknown`) — permissive.
        _ => Ok(Inbound::Unknown { raw: value }),
    }
}

/// Decode one outbound line. PERMISSIVE of unknown `type`s (the
/// `Unknown { raw }` variant — never an error: the surface is
/// unversioned).
pub fn decode_outbound(line: &str) -> Result<Outbound, ProtoError> {
    let value: Value = serde_json::from_str(line).map_err(|e| ProtoError::Json(e.to_string()))?;
    let ty = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match ty {
        "ready"
        | "event"
        | "transcript-insert"
        | "transcript-replace"
        | "display-upsert"
        | "context-usage"
        | "permission-request"
        | "interactive-request"
        | "subagent-dispatch"
        | "subagent-cancel"
        | "sink-frame"
        | "worker-error" => {
            serde_json::from_value(value).map_err(|e| ProtoError::Json(e.to_string()))
        }
        // Unknown `type` (and the literal `unknown`) — permissive.
        _ => Ok(Outbound::Unknown { raw: value }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::catalog::{Model, ModelCatalog};
    use crate::agent::harness::provider::{ChatMessage, ChatRole, MessageContent};
    use crate::agent::permission::PermissionOutcome;
    use crate::agent::subagent::{LaunchConfig, SubagentMetrics, SubagentOutcome};

    /// A fake OpenAI-compatible model (the wire shapes ride verbatim).
    fn test_model() -> Model {
        Model {
            id: "m1".to_string(),
            provider: "fake".to_string(),
            base_url: "http://fake/v1".to_string(),
            api_key: "k".to_string(),
            context_window: 100_000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }

    fn test_catalog() -> ModelCatalog {
        ModelCatalog {
            models: vec![test_model()],
            ..Default::default()
        }
    }

    fn test_env() -> StartEnv {
        StartEnv::from_parts(
            "s1".to_string(),
            "/tmp/space".to_string(),
            StartMode::Fresh,
            None,
            test_model(),
            test_catalog(),
            Some("low".to_string()),
            true,
            Some(vec!["bash".to_string()]),
            "/tmp/config".to_string(),
            true,
            None,
        )
    }

    /// The `start` envelope as the `Inbound::Start` variant (all 12
    /// fields, the harness `enabled_tools` convention verbatim).
    fn start_inbound() -> Inbound {
        Inbound::Start(Box::new(StartEnv {
            session_id: "s1".to_string(),
            cwd: "/tmp/space".to_string(),
            mode: StartMode::Resume,
            transcript: Some(vec![
                ChatMessage {
                    role: ChatRole::System,
                    content: MessageContent::Text("sp".to_string()),
                    tool_call_id: None,
                    tool_calls: None,
                },
                ChatMessage {
                    role: ChatRole::User,
                    content: MessageContent::Text("hi".to_string()),
                    tool_call_id: None,
                    tool_calls: None,
                },
            ]),
            model: test_model(),
            catalog: test_catalog(),
            thinking: Some("low".to_string()),
            trusted: true,
            // `Some(vec![])` = NO tools (the harness convention — the
            // round-trip must preserve the empty set verbatim).
            enabled_tools: Some(Vec::new()),
            config_dir: "/tmp/config".to_string(),
            subagent_enabled: false,
            system_prompt: Some("child prompt".to_string()),
        }))
    }

    /// Every `Inbound` variant round-trips through `encode_line` +
    /// `decode_inbound` (including the `PermissionResponse` carrying
    /// `Selected { option_id: "trust-space" }` and the
    /// `SubagentResult` carrying each `SubagentOutcome` variant).
    #[test]
    fn every_inbound_variant_round_trips() {
        let cases = vec![
            start_inbound(),
            Inbound::Prompt {
                text: "hello".to_string(),
                images: vec![],
            },
            Inbound::Config {
                model: Some(test_model()),
                thinking: Some("low".to_string()),
                trusted: Some(true),
            },
            Inbound::Abort,
            Inbound::Close,
            Inbound::PermissionResponse {
                id: "r1".to_string(),
                outcome: PermissionOutcome::Selected {
                    option_id: "trust-space".to_string(),
                },
            },
            Inbound::PermissionResponse {
                id: "r2".to_string(),
                outcome: PermissionOutcome::Cancelled,
            },
            Inbound::InteractiveResponse {
                id: "r3".to_string(),
                value: serde_json::json!({ "confirmed": true }),
            },
            Inbound::SubagentResult {
                id: "sub1".to_string(),
                outcome: SubagentOutcome::Completed {
                    output: "done".to_string(),
                    metrics: SubagentMetrics {
                        output: "done".to_string(),
                        input_tokens: 10,
                        output_tokens: 2,
                        cost: 0.001,
                        duration_ms: 42,
                    },
                },
            },
            Inbound::SubagentResult {
                id: "sub2".to_string(),
                outcome: SubagentOutcome::Failed {
                    error: "boom".to_string(),
                },
            },
        ];
        for case in cases {
            let line = encode_inbound(&case);
            assert!(line.ends_with('\n'), "the frame is one JSON line");
            let decoded = decode_inbound(&line).expect("the frame decodes");
            assert_eq!(case, decoded, "the frame round-trips verbatim");
        }
    }

    /// Every `Outbound` variant round-trips through `encode_outbound` +
    /// `decode_outbound` (including the `PermissionRequest` /
    /// `InteractiveRequest` frames carrying full canned gate payloads
    /// verbatim, and the `Event` frame nesting an `RpcEvent`).
    #[test]
    fn every_outbound_variant_round_trips() {
        let permission_payload = serde_json::json!({
            "sessionId": "s1",
            "requestId": "r1",
            "request": {
                "sessionId": "s1",
                "toolCall": { "title": "Allow bash?" },
                "options": [
                    { "optionId": "allow", "name": "Allow", "kind": "allow" },
                    { "optionId": "reject", "name": "Block", "kind": "reject" },
                    { "optionId": "trust-space", "name": "Don't ask again for this Space", "kind": "allow" }
                ]
            }
        });
        let interactive_payload = serde_json::json!({
            "sessionId": "s1",
            "requestId": "r2:confirm",
            "method": "confirm",
            "source": "sudo",
            "toolCallId": null,
            "params": { "command": "apt install ripgrep", "reason": "search" }
        });
        let cases = vec![
            Outbound::Ready {
                version: "0.1.0".to_string(),
                session_id: None,
            },
            Outbound::Ready {
                version: "0.1.0".to_string(),
                session_id: Some("s1".to_string()),
            },
            Outbound::Event {
                event: crate::agent::events::RpcEvent::agent_settled,
            },
            Outbound::TranscriptInsert {
                session_id: "s1".to_string(),
                seq: 0,
                role: "system".to_string(),
                content_json: r#"{"role":"system"}"#.to_string(),
            },
            Outbound::TranscriptReplace {
                session_id: "s1".to_string(),
                rows: vec![(0, "system".to_string(), "{}".to_string())],
            },
            Outbound::DisplayUpsert {
                session_id: "s1".to_string(),
                rows: vec![crate::agent::harness::store::DisplayRow {
                    kind: "agent-text".to_string(),
                    message_key: "m1".to_string(),
                    payload_json: serde_json::json!({ "text": "hi" }),
                    created_at: 1,
                }],
            },
            Outbound::ContextUsage {
                session_id: "s1".to_string(),
                used: 53_760,
                window: 128_000,
            },
            Outbound::PermissionRequest {
                id: "r1".to_string(),
                payload: permission_payload,
            },
            Outbound::InteractiveRequest {
                id: "r2:confirm".to_string(),
                payload: interactive_payload,
            },
            Outbound::SubagentDispatch {
                id: "sub1".to_string(),
                parent_session_id: "s1".to_string(),
                parent_cwd: "/tmp/space".to_string(),
                parent_enabled_tools: vec!["bash".to_string()],
                agent_name: "tester".to_string(),
                launch: LaunchConfig::default(),
                task: "do the thing".to_string(),
                model_key: "fake/m1".to_string(),
            },
            Outbound::SubagentCancel {
                id: "sub1".to_string(),
            },
            Outbound::SinkFrame {
                event: "session-update".to_string(),
                payload: serde_json::json!({ "sessionId": "s1" }),
            },
            Outbound::WorkerError {
                code: "worker_panic".to_string(),
                message: "the loop panicked".to_string(),
                backtrace: Some("frame 1".to_string()),
            },
        ];
        for case in cases {
            let line = encode_outbound(&case);
            assert!(line.ends_with('\n'), "the frame is one JSON line");
            let decoded = decode_outbound(&line).expect("the frame decodes");
            assert_eq!(case, decoded, "the frame round-trips verbatim");
        }
    }

    /// The `RpcEvent`-inside-`Event` nesting round-trips (forces the
    /// `Serialize` derive on `RpcEvent` — the camelCase payload fields +
    /// the snake_case `type` discriminators survive both directions).
    #[test]
    fn rpc_event_inside_event_nesting_round_trips() {
        let events = vec![
            crate::agent::events::RpcEvent::agent_start,
            crate::agent::events::RpcEvent::turn_start,
            crate::agent::events::RpcEvent::text_delta {
                content_index: 0,
                delta: "hi".to_string(),
            },
            crate::agent::events::RpcEvent::message_update {
                usage: serde_json::json!({ "inputTokens": 5 }),
                assistant_message_event: serde_json::json!({ "type": "textDelta" }),
            },
            crate::agent::events::RpcEvent::compaction_end {
                reason: "context_limit".to_string(),
                result: None,
                aborted: false,
                will_retry: false,
                error_message: None,
            },
            crate::agent::events::RpcEvent::unknown {
                raw: serde_json::json!({ "type": "brand-new-event", "x": 1 }),
            },
        ];
        for event in events {
            let line = encode_outbound(&Outbound::Event {
                event: event.clone(),
            });
            let decoded = decode_outbound(&line).expect("the frame decodes");
            match decoded {
                Outbound::Event { event: back } => {
                    assert_eq!(event, back, "the nested RpcEvent round-trips verbatim")
                }
                other => panic!("expected `Event`, got {other:?}"),
            }
        }
    }

    /// An unknown `type` line decodes to the `Unknown` variant (NEVER an
    /// error — the surface is unversioned), on both sides; the raw line
    /// is preserved.
    #[test]
    fn unknown_type_decodes_to_the_unknown_variant_never_an_error() {
        let line = r#"{"type": "brand-new-event", "foo": 1, "bar": "baz"}"#;
        let inbound = decode_inbound(line).expect("an unknown type is never an error");
        match inbound {
            Inbound::Unknown { raw } => {
                assert_eq!(raw["foo"], 1, "the raw line is preserved");
                assert_eq!(raw["bar"], "baz");
            }
            other => panic!("expected `Unknown`, got {other:?}"),
        }
        let outbound = decode_outbound(line).expect("an unknown type is never an error");
        match outbound {
            Outbound::Unknown { raw } => assert_eq!(raw["foo"], 1),
            other => panic!("expected `Unknown`, got {other:?}"),
        }
        // A line with NO `type` at all is `Unknown` too (not an error).
        let no_type = r#"{"foo": 1}"#;
        assert!(matches!(
            decode_inbound(no_type).expect("no type is never an error"),
            Inbound::Unknown { .. }
        ));
    }

    /// A KNOWN `type` with a malformed payload IS an error (permissive
    /// only of UNKNOWN types).
    #[test]
    fn a_known_type_with_a_malformed_payload_is_an_error() {
        // `start` without its payload fields.
        let line = r#"{"type": "start"}"#;
        assert!(
            decode_inbound(line).is_err(),
            "a partial `start` is an error"
        );
        // Not valid JSON at all.
        assert!(decode_inbound("not json at all").is_err());
        assert!(decode_outbound("not json at all").is_err());
    }

    /// `StartEnv::from_parts` + `From<&StartEnv> for Inbound` — the
    /// standalone struct and the `Start` variant are field-identical
    /// (the Task-3/4/7 construction sites build the `StartEnv`, the
    /// `Inbound` is the wire form).
    #[test]
    fn start_env_from_parts_matches_the_inbound_start_variant() {
        let env = test_env();
        let inbound = Inbound::from(&env);
        let line = encode_inbound(&inbound);
        let decoded = decode_inbound(&line).expect("the frame decodes");
        match decoded {
            Inbound::Start(env_dec) => {
                assert_eq!(env_dec.session_id, env.session_id);
                assert_eq!(env_dec.cwd, env.cwd);
                assert_eq!(env_dec.mode, env.mode);
                assert_eq!(env_dec.transcript, env.transcript);
                assert_eq!(env_dec.model, env.model);
                assert_eq!(env_dec.catalog, env.catalog);
                assert_eq!(env_dec.thinking, env.thinking);
                assert_eq!(env_dec.trusted, env.trusted);
                assert_eq!(env_dec.enabled_tools, env.enabled_tools);
                assert_eq!(env_dec.config_dir, env.config_dir);
                assert_eq!(env_dec.subagent_enabled, env.subagent_enabled);
                assert_eq!(env_dec.system_prompt, env.system_prompt);
            }
            other => panic!("expected `Start`, got {other:?}"),
        }
    }
}

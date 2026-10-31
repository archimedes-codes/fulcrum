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

/// The session-start mode (the `start` envelope's `mode` field): WHICH
/// prompt this session starts from, and the FIRST half of every arm of
/// `default_build_loop`'s init `match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartMode {
    /// A fresh MAIN session: the Worker builds the main system prompt itself
    /// (the seq-0 transcript row) — ADR 0017.
    Fresh,
    /// A fresh SUBAGENT child (ADR 0017's REDUCED message): the Supervisor has
    /// already computed `build_child_system_message` into `system_prompt`.
    /// `Some` → that message is the seq-0 row; `None` → the child gets NO
    /// system message at all and NO seq-0 row. A child NEVER reaches the
    /// main-prompt arm — that is the whole reason this variant exists
    /// (issue #9: `(Fresh, None)` used to mean both "main session" and
    /// "child with nothing to say", and the child lost that tie to the
    /// largest prompt in the app).
    FreshChild,
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
    /// `/compact` (ADR 0044 — a Composer command the CLIENT intercepts, so
    /// this frame is the ONLY way a hand-triggered compaction reaches the
    /// harness). An idle-time control frame: the Worker forwards it to the
    /// loop's control queue, which `run()` polls only BETWEEN turns, so a
    /// command sent mid-turn queues behind the turn. `instructions` are the
    /// user's extra summary instructions (`/compact <args>`; `None` for a bare
    /// `/compact`).
    #[serde(rename = "compact", rename_all = "camelCase")]
    Compact { instructions: Option<String> },
    #[serde(rename = "close")]
    Close,
    /// The REAL outcome (the per-prompt option UI: `Selected { option_id,
    /// value }` — the boundary prompt's allow / reject / trust-space, or the
    /// guard prompt's allow / allow-directory / reject, `value` carrying the
    /// option's payload — or `Cancelled`) — NOT a bool.
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
    #[serde(rename = "fetch-render-response", rename_all = "camelCase")]
    FetchRenderResponse { id: String, value: Value },
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
        /// (issue #47) `resolve_launch`'s report on `agent_name` (computed
        /// Worker-side, in the loop's `harness::dispatch`), so the Supervisor's
        /// `subagent-session-started` payload can distinguish "no name given"
        /// from "name given, matched" from "name given, NO match".
        ///
        /// Deliberately NOT `#[serde(default)]`, unlike the `subagent-result`
        /// fields above. Every default for this field is a LIE: `NoName` would
        /// silence the marker for a child that was named and missed, which is
        /// the exact defect the field exists to fix, and the other two would
        /// accuse a genuinely nameless dispatch. A missing field is therefore a
        /// hard `ProtoError::Json` (this `type` is in `decode_outbound`'s
        /// explicit allow-list) — the loud direction — and it cannot occur in
        /// production: the Worker is a self-exec of the SAME binary
        /// (`WorkerHandle::spawn(std::env::current_exe())` + `--worker`), so
        /// Supervisor and Worker are always one build. Pinned by
        /// `an_old_shape_subagent_dispatch_line_without_name_resolution_is_a_decode_error`.
        ///
        /// What the loud direction costs, in writing: the Client's read loop
        /// turns a decode `Err` into `eprintln!` + `continue`, so a dropped
        /// `SubagentDispatch` line leaves the Worker's `IpcDispatcher` waiter
        /// unresolved and the parent's `subagent` tool call hanging until the
        /// turn is cancelled — and that `eprintln!` is visible only under
        /// `ARCHIMEDES_DEBUG`, which is why the self-exec pin above, not the
        /// log, is what keeps this consequence unpayable.
        name_resolution: crate::agent::subagent::AgentNameResolution,
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
    #[serde(rename = "fetch-render-request", rename_all = "camelCase")]
    FetchRenderRequest { id: String, payload: Value },
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
    /// (issue #101) The per-agent `permission:` ruleset as the PARENT's
    /// resolved launch carried it — VERBATIM from
    /// [`crate::agent::subagent::LaunchConfig::permission`], i.e. the
    /// definition's declared NARROWING layer. `None` → the file does not speak
    /// about permissions and the child's gate starts from "no narrowing".
    ///
    /// The field the gate's enforcement rides on: `worker/core.rs` hands this
    /// to `AgentLoop::set_permission` next to `set_enabled_tools`, and without
    /// that line the child's gate consult would see `None` on every dispatch —
    /// unit-tested enforcement that never runs (the confident no-op #101 is
    /// about). Crosses the wire inside the `start` envelope (`camelCase`,
    /// ADDITIVE: an old supervisor's `start` without the key decodes to `None`,
    /// because serde treats a missing `Option` field as absent-by-default).
    pub permission: Option<crate::agent::definition_permission::PermissionBlock>,
    pub config_dir: String,
    pub subagent_enabled: bool,
    pub system_prompt: Option<String>,
    /// The stamp for THIS Worker's life within the Session (ADR 0049). Every
    /// `messageId` the harness mints carries it, so two incarnations of one
    /// Session cannot write the same display row. Minted per envelope — ONE
    /// stamp per `Start`.
    pub incarnation: String,
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
        permission: Option<crate::agent::definition_permission::PermissionBlock>,
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
            permission,
            config_dir,
            subagent_enabled,
            system_prompt,
            incarnation: crate::agent::types::mint_incarnation(),
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
    /// (issue #47) The `subagent-session-started` payload's `nameResolution`,
    /// verbatim — the flow does not re-derive it (a second `discover_agents`
    /// there would be a second name selector). No `#[serde(default)]`, for the
    /// same reason as the `Outbound` variant above: every default would be a
    /// lie about whether a persona loaded.
    pub name_resolution: crate::agent::subagent::AgentNameResolution,
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
        | "compact"
        | "abort"
        | "close"
        | "permission-response"
        | "interactive-response"
        | "subagent-result"
        | "fetch-render-response" => {
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
        | "fetch-render-request"
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
    use crate::agent::subagent::{
        AgentNameResolution, LaunchConfig, SubagentFailureReason, SubagentMetrics, SubagentOutcome,
    };

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
            None,
        )
    }

    /// The `start` envelope as the `Inbound::Start` variant (all 14
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
            // (issue #101) A NON-`None` block ON PURPOSE: this literal is the
            // payload `every_inbound_variant_round_trips` encodes and DECODES
            // through `decode_inbound`, so the `Deserialize` side of the whole
            // `PermissionBlock` aggregate is pinned on every run — including
            // the spellings B1 froze (`Action` lowercase, `ToolKey` camelCase,
            // `Unrecognized` externally tagged, `whole_tool` snake_case with NO
            // `rename_all` on `Rules`). A convenience `rename_all` added to any
            // nested type reddens THAT test instead of silently decoding a real
            // file's block into a different ruleset.
            permission: Some(test_permission_block()),
            incarnation: "a3f9c1".to_string(),
        }))
    }

    /// The `permission` fixture the round-trip pin carries: one whole-tool
    /// verdict, one per-pattern ruleset, and one unrecognised tool key — the
    /// three shapes `Rules` / `ToolKey` can hold, so the decode exercises every
    /// nested type in the aggregate.
    fn test_permission_block() -> crate::agent::definition_permission::PermissionBlock {
        use crate::agent::definition_permission::{Action, Rule, Rules, ToolKey};
        crate::agent::definition_permission::PermissionBlock {
            tools: vec![
                (ToolKey::Bash, Rules::whole_tool(Action::Deny)),
                (
                    ToolKey::Edit,
                    Rules::per_tool(vec![
                        Rule {
                            pattern: "*".to_string(),
                            action: Action::Deny,
                        },
                        Rule {
                            pattern: "*.md".to_string(),
                            action: Action::Allow,
                        },
                    ]),
                ),
                (
                    ToolKey::Unrecognized("task".to_string()),
                    Rules::whole_tool(Action::Ask),
                ),
            ],
        }
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
            Inbound::Compact {
                instructions: Some("keep the tests".to_string()),
            },
            Inbound::Abort,
            Inbound::Close,
            Inbound::PermissionResponse {
                id: "r1".to_string(),
                outcome: PermissionOutcome::Selected {
                    option_id: "trust-space".to_string(),
                    value: None,
                },
            },
            Inbound::PermissionResponse {
                id: "r2".to_string(),
                outcome: PermissionOutcome::Cancelled,
            },
            // The guard prompt's `allow-directory` carries the directory as
            // the outcome's `value` (the Supervisor persists it — Task 4), so
            // the NEW shape is part of the crate-wide round-trip pin.
            Inbound::PermissionResponse {
                id: "r2b".to_string(),
                outcome: PermissionOutcome::Selected {
                    option_id: "allow-directory".to_string(),
                    value: Some("/home/u/.pi/agent/agents".to_string()),
                },
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
                        cost: Some(0.001),
                        duration_ms: 42,
                    },
                },
            },
            Inbound::SubagentResult {
                id: "sub2".to_string(),
                outcome: SubagentOutcome::Failed {
                    reason: SubagentFailureReason::Crashed,
                    error: "boom".to_string(),
                    output: "partial".to_string(),
                    metrics: SubagentMetrics {
                        output: "partial".to_string(),
                        input_tokens: 7,
                        output_tokens: 3,
                        cost: Some(0.0),
                        duration_ms: 99,
                    },
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

    /// (Cross-version guard) An OLD-shape `Failed` payload — the pre-salvage
    /// `{"Failed":{"error":"x"}}`, with NO `reason` / `output` / `metrics` —
    /// still DECODES. This is not a nicety: `subagent-result` is in
    /// `decode_inbound`'s explicit allow-list, so a missing field is a hard
    /// `ProtoError::Json` and NOT the permissive `Unknown` fallback — a stale
    /// Worker binary (one that survived an app update) would fail every
    /// subagent result it delivered. The `#[serde(default)]`s on the new fields
    /// are what keep an old Worker working.
    #[test]
    fn a_failed_subagent_result_in_the_old_shape_still_decodes() {
        let line = concat!(
            "{\"type\":\"subagent-result\",\"id\":\"sub1\",",
            "\"outcome\":{\"Failed\":{\"error\":\"x\"}}}\n"
        );
        let decoded = decode_inbound(line).expect("an OLD-shape `Failed` decodes");
        assert_eq!(
            decoded,
            Inbound::SubagentResult {
                id: "sub1".to_string(),
                outcome: SubagentOutcome::Failed {
                    reason: SubagentFailureReason::Other,
                    error: "x".to_string(),
                    output: String::new(),
                    metrics: SubagentMetrics::default(),
                },
            },
            "the missing fields fall back to the `serde(default)`s"
        );
    }

    /// (Cross-version guard, the `a_failed_subagent_result_in_the_old_shape_still_decodes`
    /// twin) An OLD-shape `permission-response` — the pre-`value`
    /// `{"selected":{"option_id":"allow"}}`, with NO `value` — still DECODES,
    /// to `value: None`.
    ///
    /// `"permission-response"` is in `decode_inbound`'s EXPLICIT allow-list,
    /// so a missing `option_id` (or any structural mismatch) is a hard
    /// `ProtoError::Json`, NOT the permissive `Unknown { raw }` fallback — but
    /// a missing `value` is NOT an error: that is exactly the field
    /// `#[serde(default)]` exists to absorb. A stale Worker binary (one that
    /// survived an app update) keeps answering prompts.
    #[test]
    fn before_value_permission_response_still_decodes() {
        let line = concat!(
            "{\"type\":\"permission-response\",\"id\":\"r1\",",
            "\"outcome\":{\"selected\":{\"option_id\":\"allow\"}}}\n"
        );
        let decoded = decode_inbound(line).expect("an OLD-shape `permission-response` decodes");
        assert_eq!(
            decoded,
            Inbound::PermissionResponse {
                id: "r1".to_string(),
                outcome: PermissionOutcome::Selected {
                    option_id: "allow".to_string(),
                    value: None,
                },
            },
            "the missing `value` falls back to the `serde(default)`"
        );
        // And the decode is byte-stable: re-encoding the decoded frame yields
        // the SAME old line (`skip_serializing_if` keeps `None` off the wire).
        assert_eq!(encode_inbound(&decoded), line);
    }

    /// The `SubagentFailureReason`'s wire spelling (the `subagent-closed`
    /// payload's `reason` field and the card's short line read it). A nested
    /// serde enum does NOT inherit an outer `rename_all`, so the enum carries
    /// its own `snake_case` attribute — this test is what pins it.
    #[test]
    fn the_failure_reason_serialises_in_snake_case() {
        let encode = |r| serde_json::to_value(r).unwrap();
        assert_eq!(
            encode(SubagentFailureReason::Unresponsive),
            serde_json::json!("unresponsive")
        );
        assert_eq!(
            encode(SubagentFailureReason::Crashed),
            serde_json::json!("crashed")
        );
        assert_eq!(
            encode(SubagentFailureReason::Cancelled),
            serde_json::json!("cancelled")
        );
        assert_eq!(
            encode(SubagentFailureReason::Other),
            serde_json::json!("other")
        );
        assert_eq!(
            SubagentFailureReason::default(),
            SubagentFailureReason::Other,
            "an unknown / absent reason is `Other`, never a wrong specific cause"
        );
    }

    /// (issue #47) The `AgentNameResolution`'s WIRE SPELLING. Like
    /// `SubagentFailureReason`, a nested serde enum does NOT inherit an outer
    /// `rename_all`, so the enum carries its own `snake_case` attribute and this
    /// test is what pins it.
    ///
    /// `no_name` is the only TWO-WORD value in the whole subagent vocabulary
    /// (`SubagentFailureReason`'s four values are single words, so snake_case
    /// and camelCase and bare lowercase coincide there — which is exactly why
    /// nobody ever noticed the missing attribute). A "consistency" rename to
    /// `noName` would silently break the frontend's three-way comparison, so
    /// the spelling is pinned at the type.
    #[test]
    fn the_agent_name_resolution_serialises_in_snake_case() {
        let encode = |r| serde_json::to_value(r).unwrap();
        assert_eq!(
            encode(AgentNameResolution::NoName),
            serde_json::json!("no_name"),
            "`no_name` is the one two-word value in this vocabulary"
        );
        assert_eq!(
            encode(AgentNameResolution::Matched),
            serde_json::json!("matched")
        );
        assert_eq!(
            encode(AgentNameResolution::Unmatched),
            serde_json::json!("unmatched")
        );
    }

    /// (issue #47) An OLD-shape `SubagentDispatch` frame — one with NO
    /// `nameResolution` — is a hard decode ERROR, and the CURRENT frame decodes.
    ///
    /// Why loud here when the `subagent-result` fields above are permissive:
    /// `subagent-dispatch` is in `decode_outbound`'s explicit `type` allow-list,
    /// so the permissive `Unknown { raw }` fallback does NOT apply and a missing
    /// field is a `ProtoError::Json`. That is the chosen direction because every
    /// possible `#[serde(default)]` for this field is a LIE about whether a
    /// persona loaded: `NoName` would silence the resolve-miss marker for a
    /// child that WAS named and missed (the exact defect the field exists to
    /// fix), and `Matched` / `Unmatched` would accuse or absolve a genuinely
    /// nameless dispatch. The mix is also UNREACHABLE in production — the Worker
    /// is a self-exec of the SAME binary, so Supervisor and Worker are always
    /// one build — which is what makes a hard error affordable. This is the
    /// trap-pin against someone "tidying" the tolerance in a later task, the
    /// same shape as T10's
    /// `an_unknown_start_mode_is_a_decode_error_never_a_silent_main_prompt`.
    #[test]
    fn an_old_shape_subagent_dispatch_line_without_name_resolution_is_a_decode_error() {
        let case = Outbound::SubagentDispatch {
            id: "sub1".to_string(),
            parent_session_id: "s1".to_string(),
            parent_cwd: "/tmp/space".to_string(),
            parent_enabled_tools: vec!["bash".to_string()],
            agent_name: "tester".to_string(),
            name_resolution: AgentNameResolution::Matched,
            launch: LaunchConfig::default(),
            task: "do the thing".to_string(),
            model_key: "fake/m1".to_string(),
        };
        let line = encode_outbound(&case);

        // The CURRENT frame decodes and the field reads back — so the error
        // below is the MISSING field and not a broken encoder.
        match decode_outbound(&line).expect("the current frame decodes") {
            Outbound::SubagentDispatch {
                name_resolution, ..
            } => assert_eq!(name_resolution, AgentNameResolution::Matched),
            other => panic!("expected `SubagentDispatch`, got {other:?}"),
        }

        let old = line.replace(r#""nameResolution":"matched","#, "");
        assert_ne!(old, line, "the rewrite happened");
        assert!(
            decode_outbound(&old).is_err(),
            "a frame with no `nameResolution` is a loud error, never a \
             silently-defaulted resolution: {old}"
        );
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
                name_resolution: AgentNameResolution::Matched,
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
                reason: crate::agent::events::CompactionReason::Threshold,
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

    /// The compaction reason's WIRE SPELLINGS are frozen (Finding 4).
    ///
    /// The Rust enum and the TS union are ONE contract:
    /// `export type CompactionReason = "manual" | "threshold" | "overflow";`
    /// (`src/lib/types.ts` — `types.ts:143`, landed 9d8687c). The enum's
    /// `#[serde(rename_all = "snake_case")]` is what makes that true; any other
    /// rename strategy changes the wire and breaks every TS reader (the
    /// `compacting` store field, `normalize.rs`'s `compaction_update`
    /// passthrough, `toolOutput.ts`'s exhaustive switch with its `never`
    /// default arm).
    ///
    /// This test is the DRIFT PIN: a fourth member, a renamed spelling, or a
    /// changed rename strategy reddens it. It references the members BY NAME
    /// (never a `&str` literal) so it cannot pass vacuously against a
    /// string-typed field.
    #[test]
    fn compaction_reason_serde_spellings_are_frozen() {
        use crate::agent::events::CompactionReason;

        // (a) The three members serialize to EXACTLY the three TS spellings.
        assert_eq!(
            serde_json::to_string(&CompactionReason::Manual).unwrap(),
            "\"manual\"",
            "the manual trigger's wire spelling is frozen (types.ts:143)"
        );
        assert_eq!(
            serde_json::to_string(&CompactionReason::Threshold).unwrap(),
            "\"threshold\"",
            "the threshold trigger's wire spelling is frozen (types.ts:143)"
        );
        assert_eq!(
            serde_json::to_string(&CompactionReason::Overflow).unwrap(),
            "\"overflow\"",
            "the overflow trigger's wire spelling is frozen (types.ts:143)"
        );
        // And back — the reader side accepts exactly those strings.
        for (wire, member) in [
            ("\"manual\"", CompactionReason::Manual),
            ("\"threshold\"", CompactionReason::Threshold),
            ("\"overflow\"", CompactionReason::Overflow),
        ] {
            let back: CompactionReason =
                serde_json::from_str(wire).expect("the frozen spelling deserializes");
            assert_eq!(back, member, "`{wire}` maps back to its own enum member");
        }

        // (b) A full `RpcEvent` carrying each member round-trips VERBATIM
        // through the real outbound codec (mirrors
        // `rpc_event_inside_event_nesting_round_trips`'s shape).
        let events = vec![
            crate::agent::events::RpcEvent::compaction_start {
                reason: CompactionReason::Overflow,
            },
            crate::agent::events::RpcEvent::compaction_end {
                reason: CompactionReason::Threshold,
                result: None,
                aborted: false,
                will_retry: false,
                error_message: None,
            },
            crate::agent::events::RpcEvent::compaction_start {
                reason: CompactionReason::Manual,
            },
            crate::agent::events::RpcEvent::compaction_end {
                reason: CompactionReason::Manual,
                result: Some(serde_json::json!({ "tokens": 12 })),
                aborted: true,
                will_retry: false,
                error_message: Some("failed: boom".to_string()),
            },
        ];
        for event in events {
            let line = encode_outbound(&Outbound::Event {
                event: event.clone(),
            });
            let decoded = decode_outbound(&line).expect("the frame decodes");
            match decoded {
                Outbound::Event { event: back } => {
                    assert_eq!(event, back, "the compaction RpcEvent round-trips verbatim")
                }
                other => panic!("expected `Event`, got {other:?}"),
            }
        }

        // The `type` discriminator itself is NOT camelCased by the per-variant
        // `rename_all` (the enum-level note in `events.rs`): the frame the TS
        // side routes on is unchanged by the enum.
        let line = encode_outbound(&Outbound::Event {
            event: crate::agent::events::RpcEvent::compaction_start {
                reason: CompactionReason::Manual,
            },
        });
        let raw: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(raw["event"]["type"], "compaction_start");
        assert_eq!(raw["event"]["reason"], "manual");
    }

    /// `decode_inbound` routes a `compact` line to the `Compact` variant.
    /// PINNED on purpose: the decoder matches an EXPLICIT `type` allow-list
    /// and sends everything else to `Unknown { raw }`, so a `compact` frame
    /// that is not in that list is a SILENT no-op over real IPC while the
    /// raw-serde round-trip above still passes.
    #[test]
    fn decode_inbound_routes_a_compact_line_to_the_compact_variant() {
        let line = r#"{"type": "compact", "instructions": "keep the tests"}"#;
        match decode_inbound(line).expect("the `compact` line decodes") {
            Inbound::Compact { instructions } => assert_eq!(
                instructions.as_deref(),
                Some("keep the tests"),
                "the user's extra summary instructions ride the frame"
            ),
            other => panic!(
                "`compact` must decode to `Inbound::Compact`, got {other:?} \
                 (the `decode_inbound` type allow-list is missing `compact`)"
            ),
        }
        // A bare `/compact` carries no extra instructions.
        match decode_inbound(r#"{"type": "compact"}"#).expect("a bare `compact` decodes") {
            Inbound::Compact { instructions } => assert_eq!(instructions, None),
            other => panic!("expected `Compact`, got {other:?}"),
        }
    }

    /// `decode_outbound` routes a `fetch-render-request` line to its variant.
    #[test]
    fn decode_outbound_routes_a_fetch_render_request_line_to_its_variant() {
        let line = r#"{"type": "fetch-render-request", "id": "req-1", "payload": {"sessionId": "s1", "requestId": "req-1", "url": "https://example.com", "timeoutMs": 30000}}"#;
        match decode_outbound(line).expect("the `fetch-render-request` line decodes") {
            Outbound::FetchRenderRequest { id, payload } => {
                assert_eq!(id, "req-1");
                assert_eq!(payload["sessionId"], "s1");
                assert_eq!(payload["requestId"], "req-1");
                assert_eq!(payload["url"], "https://example.com");
                assert_eq!(payload["timeoutMs"], 30000);
            }
            other => panic!(
                "`fetch-render-request` must decode to `Outbound::FetchRenderRequest`, got {other:?} \
                 (the `decode_outbound` type allow-list is missing `fetch-render-request`)"
            ),
        }
    }

    /// `decode_inbound` routes a `fetch-render-response` line to its variant.
    #[test]
    fn decode_inbound_routes_a_fetch_render_response_line_to_its_variant() {
        let line = r#"{"type": "fetch-render-response", "id": "req-1", "value": {"html": "<html></html>", "status": 200, "finalUrl": "https://example.com"}}"#;
        match decode_inbound(line).expect("the `fetch-render-response` line decodes") {
            Inbound::FetchRenderResponse { id, value } => {
                assert_eq!(id, "req-1");
                assert_eq!(value["html"], "<html></html>");
                assert_eq!(value["status"], 200);
                assert_eq!(value["finalUrl"], "https://example.com");
            }
            other => panic!(
                "`fetch-render-response` must decode to `Inbound::FetchRenderResponse`, got {other:?} \
                 (the `decode_inbound` type allow-list is missing `fetch-render-response`)"
            ),
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
                assert_eq!(env_dec.incarnation, env.incarnation);
            }
            other => panic!("expected `Start`, got {other:?}"),
        }
    }

    /// (E) `FreshChild`'s WIRE SPELLING is `fresh_child`, pinned as a raw
    /// substring of the encoded line — not merely a decoded equality: the
    /// spelling IS the contract, and a variant rename (or a stray per-variant
    /// `rename`) must not silently change it.
    #[test]
    fn the_fresh_child_mode_round_trips_as_the_fresh_child_wire_name() {
        let mut env = test_env();
        env.mode = StartMode::FreshChild;
        let line = encode_inbound(&Inbound::from(&env));
        assert!(
            line.contains(r#""mode":"fresh_child""#),
            "the wire spells the child mode `fresh_child`, got {line}"
        );
        match decode_inbound(&line).expect("the frame decodes") {
            Inbound::Start(env_dec) => assert_eq!(
                env_dec.mode,
                StartMode::FreshChild,
                "the child mode round-trips"
            ),
            other => panic!("expected `Start`, got {other:?}"),
        }
    }

    /// (F) An UNREADABLE `mode` is a decode ERROR — never a silent main
    /// prompt. This is the DESIRED direction, and the trap-pin against
    /// "tidying" the tolerance later: the protocol is permissive about
    /// unknown top-level `type`s ONLY (`decode_inbound` routes on `type`,
    /// then deserializes the WHOLE payload), so a bad nested `mode` is a
    /// `ProtoError::Json` the Worker prints as `worker: bad inbound line` on
    /// the stderr the Supervisor pipes into its log — LOUD. The alternative,
    /// a `#[serde(other)]` fallback on `Fresh`, would make an unreadable mode
    /// SILENTLY become the main-prompt arm: the exact defect issue #9 removes.
    #[test]
    fn an_unknown_start_mode_is_a_decode_error_never_a_silent_main_prompt() {
        let mut env = test_env();
        env.mode = StartMode::FreshChild;
        let line = encode_inbound(&Inbound::from(&env));
        let bad = line.replace(r#""mode":"fresh_child""#, r#""mode":"nonsense""#);
        assert_ne!(bad, line, "the rewrite happened");
        assert!(
            decode_inbound(&bad).is_err(),
            "an unreadable `mode` is a loud error, not a silent main prompt: {bad}"
        );
    }

    /// Every `Start` mints its OWN incarnation stamp: the token is what keeps
    /// a resumed Worker out of its predecessor's display rows, so two `Start`s
    /// sharing one token would reproduce the collision ADR 0049 removes. The
    /// stamp is 6 LOWERCASE hex chars — a namespace, never a decoded value
    /// (the shape predicate is `test_support::is_incarnation_stamp`, shared
    /// with the two-process e2e, because `len() == 6` + a radix parse also
    /// accepts `A3F9C1`).
    #[test]
    fn every_start_env_gets_its_own_incarnation_stamp() {
        let a = test_env();
        let b = test_env();
        for env in [&a, &b] {
            assert!(
                crate::test_support::is_incarnation_stamp(&env.incarnation),
                "the stamp is six LOWERCASE hex chars, got {:?}",
                env.incarnation
            );
        }
        assert_ne!(
            a.incarnation, b.incarnation,
            "two `Start`s must not share one stamp"
        );
    }
}

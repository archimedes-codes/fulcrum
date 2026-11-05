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
//! (never an error — a peer may speak a NEWER surface than this build,
//! and forward-compatibility is the whole reason the decoder routes by
//! hand instead of deserializing the enum directly).
//!
//! The surface is nevertheless **negotiated**, not assumed: a Worker
//! opens with `Outbound::Hello`, carrying [`PROTOCOL_VERSION`], before
//! anything else (Fulcrum T1). Permissive decoding and an explicit
//! version are complementary — the version tells a peer WHEN it is
//! out of step, the permissive decoder decides WHETHER a given frame is
//! survivable. An unknown `type` is thus never a silent contract
//! violation on a KNOWN version; it is a future frame on a version this
//! build already agreed to tolerate.

use serde_json::Value;

// ── T5/S4 + T8/S8.1 — this file and its DATA types are now ONE crate ─────────
//
// S4 moved the wire DATA (`StoreFrame`, `SubagentDispatchWire`, `StartEnv`,
// `StartMode`, `DesktopConfig`) into `crate::worker_types` because the crate had
// to NAME them — `persist.rs` consumes `StoreFrame`, `subagent.rs` builds
// `SubagentDispatchWire`, the crate's `Dispatcher` trait takes one — while this
// file was still desktop, and a crate cannot name a type that lives in the
// desktop package. S8.1 moved this file INTO the crate, so the re-export below is
// now crate-internal (`crate::worker_types`, not the package-spelled path — a
// crate cannot import itself by name; that is a cycle, and the compiler says so).
//
// The re-export STAYS a `pub use` for one reason: the surface. Every existing
// spelling of these five names still routes through this module — the desktop's
// re-export shell at `src-tauri/src/agent/worker/mod.rs` re-exports them from
// here, and the two-sided integration tests name them at that path. Dropping the
// `pub` would break them for no gain.
pub use crate::worker_types::{
    DesktopConfig, StartEnv, StartMode, StoreFrame, SubagentDispatchWire,
};

/// The version of THIS stdio wire (the `hello` frame's `protocolVersion`).
///
/// Bumped only on a BREAKING wire change — a frame shape an older
/// Supervisor cannot read. An ADDITIVE frame (a new `type`, a new optional
/// field) does NOT bump it: the decoder is permissive by design, so an
/// older peer ignores what it does not know.
///
/// Distinct from `Outbound::Ready`'s `version` (and `hello`'s own
/// `clientVersion`), which is the BINARY version — `CARGO_PKG_VERSION`.
/// Two different axes: one says "which build", the other says "which
/// protocol".
pub const PROTOCOL_VERSION: u32 = 1;

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
        images: Vec<crate::tools::ImageRef>,
    },
    #[serde(rename = "config", rename_all = "camelCase")]
    Config {
        model: Option<crate::harness::catalog::Model>,
        thinking: Option<String>,
        /// Clear the loop's thinking level to "the model's default" (no
        /// `reasoning_effort` on the request). Distinct from `thinking: None`,
        /// which means "leave the level alone": a native model switch to a
        /// model that advertises NO levels must drop the previous model's
        /// level (issue #146), and `None` alone cannot say that. `serde(default)`
        /// so a frame from a caller that predates the field still decodes as
        /// "no clear".
        #[serde(default)]
        clear_thinking: bool,
        trusted: Option<bool>,
    },
    /// (Fulcrum T3) The live-reload push: the Supervisor re-sends the three
    /// desktop-config fields the Worker used to re-read from `settings.json`
    /// when the user SAVES Settings.
    ///
    /// This frame is WHY the disk read could be removed at all. Freezing those
    /// three into the one `start` envelope would silently delete the
    /// user-visible property "edit Settings, it applies at the next prompt, no
    /// restart": `PermissionContext::refresh` re-read `file_policy` + `rules` +
    /// `web` at the top of EVERY turn (ADR 0030 Deviation 4) and
    /// `agent_overrides` was re-read per subagent dispatch (ADR 0023). The
    /// status file's Decisions log chose the push over accepting that loss;
    /// the rejected alternatives (a silent loss, or keeping a `config_dir` read
    /// inside the crate that T4 must make daemon-side) stay rejected.
    ///
    /// Deliberately a SEPARATE variant from `Config`, not a widening of it:
    /// `Config` is the per-session composer controls (model / thinking /
    /// trusted) and routes to `set_model` / `set_thinking_level`, while this is
    /// the app-level settings snapshot and routes to the loop's packaged
    /// config. Reusing one variant for both would mean one payload describing
    /// two different actors.
    ///
    /// Deliberately MISSING `providers` and `mcp_servers`: those stay
    /// session-construction snapshots (the recorded residual hole in the status
    /// file — MCP already behaved that way, and headers already re-resolved
    /// only on `set_model`).
    ///
    /// EVERY field is an `Option` with `#[serde(default)]`, so a Worker that
    /// predates a field ignores it and a Supervisor that only knows about one
    /// of the three cannot wipe the other two: `None` means "leave it alone".
    #[serde(rename = "config-update", rename_all = "camelCase")]
    ConfigUpdate {
        #[serde(default)]
        file_policy: Option<crate::policy::FilePolicy>,
        #[serde(default)]
        web: Option<crate::web::config::WebSettings>,
        #[serde(default)]
        agent_overrides:
            Option<std::collections::HashMap<String, crate::config_types::AgentOverrideFields>>,
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
        outcome: crate::permission::PermissionOutcome,
    },
    #[serde(rename = "interactive-response", rename_all = "camelCase")]
    InteractiveResponse { id: String, value: Value },
    #[serde(rename = "subagent-result", rename_all = "camelCase")]
    SubagentResult {
        id: String,
        outcome: crate::subagent::SubagentOutcome,
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
    /// The version-negotiation opener — emitted at startup, BEFORE
    /// `Ready` and BEFORE `Start`. The surface was deliberately
    /// unversioned until Fulcrum T1: a Worker and a Supervisor of
    /// different builds could only discover that by failing.
    ///
    /// `protocol_version` is the WIRE version ([`PROTOCOL_VERSION`]);
    /// `client_version` is the BINARY version (the `Ready.version`
    /// analogue, restated here so a peer can identify the build from the
    /// very first line). One-sided by design: the Worker initiates, the
    /// Supervisor (same binary today) needs no reply yet.
    #[serde(rename = "hello", rename_all = "camelCase")]
    Hello {
        protocol_version: u32,
        client_version: String,
    },
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
    Event { event: crate::events::RpcEvent },
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
        rows: Vec<crate::harness::store::DisplayRow>,
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
        name_resolution: crate::subagent::AgentNameResolution,
        launch: crate::subagent::LaunchConfig,
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

/// The `Start` variant from a `StartEnv` (the round-trip helper — the
/// variant IS the boxed `StartEnv`).
impl From<&StartEnv> for Inbound {
    fn from(env: &StartEnv) -> Self {
        Inbound::Start(Box::new(env.clone()))
    }
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

/// Encode one outbound frame AND stamp it with the next `frameSeq` — the
/// per-session transport cursor (Fulcrum T1). Returns the JSONL line.
///
/// WHY THIS LIVES BELOW THE CODEC AND NOT INSIDE THE ENUM (three
/// reasons, all measured, not stylistic):
///
/// 1. `RpcEvent` has 279 construction sites (≈200 of them byte-pinned in
///    round-trip tests). A cursor field on the event type would rewrite
///    all of them and redden every pin. There are 3–4 WRITER sites — this
///    helper is called from those.
/// 2. The `Outbound` variants have ≈100–130 construction sites for the
///    same reason, and the codec round-trip tests deliberately test
///    `encode_outbound`/`decode_outbound` with NO stamping — they stay
///    green because the cursor never enters the enum.
/// 3. The shape is a FLAT SIBLING field, not a nested envelope: `payload`
///    is already a field of `SinkFrame` / `PermissionRequest` /
///    `InteractiveRequest` / `FetchRenderRequest`, so nesting under a new
///    `payload` collides.
///
/// The name is `frameSeq` and NOT `seq`, because `seq` is already taken
/// three ways: the transcript-row `seq` on `TranscriptInsert` /
/// `StoreFrame::Insert`, the inner `"seq": 0` Tauri-payload stub on the
/// `interactive-event` frames, and `SubagentLiveness.seq` (Supervisor-side,
/// never on the wire). This is the FOURTH namespace and T2 depends on the
/// distinction — hence the two pin tests that stamp a `TranscriptInsert`
/// and an `interactive-event` frame.
///
/// `seq` is the shared per-session counter (`fetch_add(1) + 1` → 1-based,
/// monotonic, reset to 0 on each `Start`). The pre-session frames
/// (`hello`, `ready`) go through `encode_outbound` instead and carry NO
/// `frameSeq` — numbering begins with the first post-`Start` frame at 1.
///
/// Injection is `to_value` → insert → `to_string`, so the `type`
/// discriminator and every existing field stay byte-identical. No `Outbound`
/// variant carries a `frameSeq` field, so there is no collision on any
/// variant, and `decode_outbound` ignores the injected key (no
/// `deny_unknown_fields`), which is what keeps the typed reads working.
pub fn stamp_outbound(seq: &std::sync::atomic::AtomicU64, frame: &Outbound) -> String {
    let mut v = serde_json::to_value(frame).expect("the protocol types are always encodable");
    let next = seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    v.as_object_mut()
        .expect("a tagged frame serializes to an object")
        .insert("frameSeq".to_string(), serde_json::json!(next));
    format!(
        "{}\n",
        serde_json::to_string(&v).expect("the stamped frame is always encodable")
    )
}

/// Decode one inbound line. PERMISSIVE of unknown `type`s (the
/// `Unknown { raw }` variant — never an error: a newer Supervisor may
/// send a frame this Worker's build predates, and dropping one line
/// beats killing a session. Negotiation of the OVERALL surface happens
/// once, on the Worker's `hello` frame — permissiveness is what happens
/// INSIDE an agreed version, not a substitute for having one).
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
        | "config-update"
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
/// `Unknown { raw }` variant — never an error: a newer Worker may emit a
/// frame this Supervisor's build predates). The overall surface IS
/// versioned — the Worker's `hello` frame carries [`PROTOCOL_VERSION`]
/// before any other frame — and this permissiveness is deliberately
/// narrow: a `type` in the allow-list below whose payload does NOT fit
/// is a hard error, never a silent `Unknown`. Adding a variant here
/// without adding it to that allow-list is the failure mode
/// `decode_outbound_routes_a_hello_line_to_the_hello_variant` exists to
/// catch.
pub fn decode_outbound(line: &str) -> Result<Outbound, ProtoError> {
    let value: Value = serde_json::from_str(line).map_err(|e| ProtoError::Json(e.to_string()))?;
    let ty = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match ty {
        "hello"
        | "ready"
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
    use crate::harness::catalog::{Model, ModelCatalog};
    use crate::harness::provider::{ChatMessage, ChatRole, MessageContent};
    use crate::permission::PermissionOutcome;
    use crate::subagent::{
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
            // (Fulcrum T2) A NON-`None` seed ON PURPOSE: this literal is the
            // payload `every_inbound_variant_round_trips` encodes and DECODES,
            // so the resume-seed field is on the round-trip in both directions.
            // The camelCase WIRE NAME is pinned separately and byte-wise by
            // `the_start_env_carries_the_stored_cost_as_cost_usd` — a field on a
            // `rename_all = "camelCase"` struct that a hand-written line spells
            // `cost_usd` decodes to `None` SILENTLY, so a round-trip alone does
            // not pin the spelling.
            cost_usd: Some(0.5),
            // (Fulcrum T3) The FIVE packaged desktop-config fields, NON-default
            // ON PURPOSE for the same reason as the two above: this literal is
            // what `every_inbound_variant_round_trips` encodes and DECODES, so
            // the `Deserialize` half of `FilePolicy` / `WebSettings` /
            // `ProviderConfig` / `AgentOverrideFields` is exercised on every
            // run, and the camelCase WIRE NAMES are pinned byte-wise by
            // `the_start_env_packages_the_five_desktop_config_fields_in_camel_case`.
            file_policy: crate::policy::FilePolicy {
                reads: crate::policy::AccessPolicy::Ask,
                writes: crate::policy::AccessPolicy::Sandboxed,
                shell: crate::policy::AccessPolicy::Ask,
                rules: vec![crate::policy::AccessRule {
                    path: "/etc/hosts".to_string(),
                    direction: crate::policy::RuleDirection::ReadWrite,
                    decision: crate::policy::RuleDecision::Deny,
                }],
            },
            web: crate::web::config::WebSettings {
                brave_api_key: "b-key".to_string(),
                tavily_api_key: String::new(),
                perplexity_api_key: String::new(),
                searxng_url: String::new(),
                default_provider: "brave".to_string(),
                proxy: String::new(),
            },
            mcp_servers: std::collections::HashMap::from([(
                "desktop-server".to_string(),
                serde_json::json!({ "command": "npx", "args": ["-y", "srv"] }),
            )]),
            agent_overrides: std::collections::HashMap::from([(
                "scout".to_string(),
                crate::config_types::AgentOverrideFields {
                    model: Some("fake/m3".to_string()),
                    thinking: None,
                    // ADR 0067: an EMPTY list is ABSENT, never `Some(vec![])`.
                    tools: None,
                },
            )]),
            providers: vec![crate::config_types::ProviderConfig {
                id: "corp".to_string(),
                name: "Corp".to_string(),
                base_url: "https://corp.invalid/v1".to_string(),
                api_key: "k".to_string(),
                api: "openai-completions".to_string(),
                key_url: None,
                auth_mode: "api_key".to_string(),
                headers: Some(std::collections::HashMap::from([(
                    "X-Tenant".to_string(),
                    "$CORP_TENANT".to_string(),
                )])),
                allow_insecure_http: false,
                use_gcloud_token_auth: false,
                oidc: None,
            }],
        }))
    }

    /// The `permission` fixture the round-trip pin carries: one whole-tool
    /// verdict, one per-pattern ruleset, and one unrecognised tool key — the
    /// three shapes `Rules` / `ToolKey` can hold, so the decode exercises every
    /// nested type in the aggregate.
    fn test_permission_block() -> crate::definition_permission::PermissionBlock {
        use crate::definition_permission::{Action, Rule, Rules, ToolKey};
        crate::definition_permission::PermissionBlock {
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

    /// Issue #146: a `config` frame that predates `clearThinking` (no field on
    /// the wire) still decodes, with the clear OFF — so an older Supervisor
    /// cannot accidentally clear a level. A `clearThinking: true` frame decodes
    /// to a clear.
    #[test]
    fn config_clear_thinking_defaults_off_and_decodes_when_set() {
        let legacy = r#"{"type":"config","model":null,"thinking":"high","trusted":null}"#;
        match decode_inbound(legacy).expect("a legacy config frame decodes") {
            Inbound::Config {
                thinking,
                clear_thinking,
                ..
            } => {
                assert_eq!(thinking.as_deref(), Some("high"));
                assert!(!clear_thinking, "a frame without the field must not clear");
            }
            other => panic!("expected Config, got {other:?}"),
        }
        let clear =
            r#"{"type":"config","model":null,"thinking":null,"clearThinking":true,"trusted":null}"#;
        match decode_inbound(clear).expect("a clear config frame decodes") {
            Inbound::Config {
                thinking,
                clear_thinking,
                ..
            } => {
                assert_eq!(thinking, None);
                assert!(clear_thinking);
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    /// (Fulcrum T2) The `StartEnv`'s resume-seed field: its WIRE NAME is
    /// `costUsd`, it is ADDITIVE (a line without the key decodes to `None`), and
    /// `with_seed_cost` is the only writer.
    ///
    /// Why a byte-level pin rather than the round-trip alone: `StartEnv` carries
    /// `rename_all = "camelCase"`, but the nested `Model` / `ModelCatalog` it
    /// embeds carry NO `rename_all` — so a hand-written `start` line that spells
    /// its own keys camelCase fails to decode, and BOTH sides of the wire drop a
    /// bad line permissively without an error. A test that hand-wrote such a line
    /// would pass vacuously. The line below is therefore derived from an ENCODED
    /// frame by string surgery, so the only thing this test changes is the key
    /// it is pinning.
    #[test]
    fn the_start_env_carries_the_stored_cost_as_cost_usd() {
        let line = encode_inbound(&start_inbound());
        assert!(
            line.contains(r#""costUsd":0.5"#),
            "the resume seed rides the wire as camelCase `costUsd`: {line}"
        );
        assert!(
            !line.contains("cost_usd"),
            "no snake_case spelling of the seed key ever reaches the wire: {line}"
        );

        // The additive direction: an OLD supervisor's `start` with the key
        // removed still decodes, and reads `None` (=> the loop seeds 0.0).
        let old = line.replace(r#","costUsd":0.5"#, "");
        assert_ne!(old, line, "the rewrite happened");
        match decode_inbound(&old).expect("a `start` without the seed key decodes") {
            Inbound::Start(env) => assert_eq!(
                env.cost_usd, None,
                "a missing seed key is `None`, never a fabricated zero-becomes-figure"
            ),
            other => panic!("expected `Start`, got {other:?}"),
        }

        // The trap, armed: the SAME key spelled snake_case is not an error and
        // not a seed — it is silently `None`, because `StartEnv` is camelCase.
        // This is why the assertion above is on the serialised line.
        let misnamed = line.replace(r#""costUsd":0.5"#, r#""cost_usd":0.5"#);
        match decode_inbound(&misnamed).expect("a misnamed key does not break the decode") {
            Inbound::Start(env) => assert_eq!(
                env.cost_usd, None,
                "a snake_case `cost_usd` is an UNKNOWN key to this struct — the \
                 silent-drop direction, documented so nobody hand-writes one"
            ),
            other => panic!("expected `Start`, got {other:?}"),
        }

        // `from_parts` (every fresh session and every fresh child) seeds `None`;
        // `with_seed_cost` is the resume path's setter.
        assert_eq!(
            test_env().cost_usd,
            None,
            "a constructor-built env carries no stored figure"
        );
        assert_eq!(
            test_env().with_seed_cost(Some(0.125)).cost_usd,
            Some(0.125),
            "`with_seed_cost` sets it verbatim"
        );
        // A snapshot builder SETS, it does not add — a second call cannot
        // compound a seed the way the retired delta-sum could.
        assert_eq!(
            test_env()
                .with_seed_cost(Some(0.125))
                .with_seed_cost(Some(0.25))
                .cost_usd,
            Some(0.25)
        );
    }

    /// (Fulcrum T3) The FIVE packaged desktop-config fields ride the `start`
    /// envelope as camelCase keys, and `from_parts`' defaults are the INERT
    /// ones (an empty map / an empty vec / the all-`Allow` policy / the
    /// env-only web config) — the shape a test fixture and an old-style
    /// Supervisor produce.
    ///
    /// Why a byte-level pin rather than the round-trip alone: `StartEnv` is
    /// camelCase but the types it embeds are NOT uniformly renamed — so a
    /// hand-written camelCase line can decode to nothing on BOTH sides with no
    /// error at all (see [`the_start_env_carries_the_stored_cost_as_cost_usd`]
    /// and the module's casing note). The lines below are therefore derived
    /// from an ENCODED frame by string surgery.
    #[test]
    fn the_start_env_packages_the_five_desktop_config_fields_in_camel_case() {
        let line = encode_inbound(&start_inbound());
        for key in [
            r#""filePolicy":"#,
            r#""web":"#,
            r#""mcpServers":"#,
            r#""agentOverrides":"#,
            r#""providers":"#,
        ] {
            assert!(
                line.contains(key),
                "the `start` envelope carries {key}: {line}"
            );
        }
        // The inner `providers[].headers` is the FIFTH field's payload (issue
        // #129) — the row `set_model` re-resolves. If it did not ride, the
        // Worker would still have to open `settings.json`.
        assert!(
            line.contains(r#""headers":{"X-Tenant":"$CORP_TENANT"}"#),
            "the provider row's raw header TEMPLATE rides the envelope \
             (expansion stays worker-side, against the Worker's own env): {line}"
        );
        // The ADR 0067 tri-state survives the wire: an absent `tools` is
        // ABSENT, not `null` and not `[]`.
        assert!(
            !line.contains(r#""tools":[]"#),
            "an override entry never serializes an empty tool list: {line}"
        );
        assert!(
            !line.contains(r#""file_policy""#) && !line.contains(r#""mcp_servers""#),
            "no snake_case spelling of the packaged keys ever reaches the wire: {line}"
        );

        // And they DECODE — the round-trip direction, on the same literal.
        match decode_inbound(&line).expect("the grown `start` decodes") {
            Inbound::Start(env) => {
                assert_eq!(env.file_policy.reads, crate::policy::AccessPolicy::Ask);
                assert_eq!(
                    env.file_policy.writes,
                    crate::policy::AccessPolicy::Sandboxed
                );
                assert_eq!(env.file_policy.rules.len(), 1);
                assert_eq!(env.web.brave_api_key, "b-key");
                assert_eq!(env.web.default_provider, "brave");
                assert!(env.mcp_servers.contains_key("desktop-server"));
                assert_eq!(
                    env.agent_overrides
                        .get("scout")
                        .and_then(|e| e.model.as_deref()),
                    Some("fake/m3")
                );
                assert_eq!(
                    env.agent_overrides
                        .get("scout")
                        .and_then(|e| e.tools.as_ref()),
                    None,
                    "ADR 0067: an absent `tools` stays absent across the wire"
                );
                assert_eq!(env.providers.len(), 1);
                assert_eq!(env.providers[0].id, "corp");
                assert_eq!(
                    env.providers[0]
                        .headers
                        .as_ref()
                        .and_then(|h| h.get("X-Tenant"))
                        .map(String::as_str),
                    Some("$CORP_TENANT"),
                    "the header template arrives UNEXPANDED"
                );
            }
            other => panic!("expected `Start`, got {other:?}"),
        }
    }

    /// (Fulcrum T3) The ADDITIVE direction, pinned the way `permission` and
    /// `costUsd` pin theirs: an OLD supervisor's `start` — every one of the
    /// five keys REMOVED from the encoded line — still decodes, and each field
    /// lands on its INERT default.
    ///
    /// The inert defaults are not a formality; they are what keeps an old peer
    /// working after this change:
    /// * `file_policy` → `FilePolicy::default()` = all-`Allow` (ADR 0030
    ///   Deviation 2's out-of-the-box posture — the SAME thing today's
    ///   "no `settings.json` in the temp dir" case produces);
    /// * `web` → all-empty = env-only web config;
    /// * `mcp_servers` / `agent_overrides` → the EMPTY map = no desktop MCP
    ///   layer / a silent override layer;
    /// * `providers` → the empty vec = no custom headers, byte-identical to
    ///   today's `config_dir: None` case.
    #[test]
    fn an_old_shape_start_envelope_still_decodes_to_the_five_inert_defaults() {
        let line = encode_inbound(&start_inbound());
        // Strip the five keys by SURGERY on the encoded line, so the only
        // thing this test varies is the presence of the keys themselves.
        let mut old = line.clone();
        for key in [
            "filePolicy",
            "web",
            "mcpServers",
            "agentOverrides",
            "providers",
        ] {
            old = drop_json_key(&old, key);
        }
        assert_ne!(
            old, line,
            "the rewrite happened (a key was actually removed)"
        );
        assert!(
            !old.contains(r#""filePolicy""#) && !old.contains(r#""providers""#),
            "the old-shape line names none of the packaged keys: {old}"
        );
        match decode_inbound(&old).expect("a `start` without the five keys decodes") {
            Inbound::Start(env) => {
                assert_eq!(
                    env.file_policy,
                    crate::policy::FilePolicy::default(),
                    "no packaged policy = the all-`Allow` default"
                );
                assert_eq!(env.web, crate::web::config::WebSettings::default());
                assert!(
                    env.mcp_servers.is_empty(),
                    "no packaged map = no desktop layer"
                );
                assert!(
                    env.agent_overrides.is_empty(),
                    "no packaged map = a layer that says nothing"
                );
                assert!(
                    env.providers.is_empty(),
                    "no packaged rows = no custom headers (the old `config_dir: None` case)"
                );
                // The pre-existing fields are untouched by the strip.
                assert_eq!(env.session_id, "s1");
                assert_eq!(env.cost_usd, Some(0.5));
            }
            other => panic!("expected `Start`, got {other:?}"),
        }

        // `from_parts` (the fixture/old-call shape) builds the SAME inert
        // five: a caller that knows nothing about them gets the inert env, not
        // a panic and not a half-populated one.
        let plain = test_env();
        assert_eq!(plain.file_policy, crate::policy::FilePolicy::default());
        assert_eq!(plain.web, crate::web::config::WebSettings::default());
        assert!(plain.mcp_servers.is_empty());
        assert!(plain.agent_overrides.is_empty());
        assert!(plain.providers.is_empty());
    }

    /// Remove one top-level key (and its value) from a flat-ish JSON object
    /// line by bracket matching. Deliberately dumb and local: a real parser
    /// would let this test pass even if the key were renamed rather than
    /// removed, and the whole point is that the key is ABSENT.
    fn drop_json_key(line: &str, key: &str) -> String {
        let needle = format!(r#""{key}":"#);
        let start = line
            .find(&needle)
            .unwrap_or_else(|| panic!("the encoded line carries `{key}`: {line}"));
        let value_start = start + needle.len();
        let bytes = line.as_bytes();
        let first = bytes[value_start];
        let mut end = value_start;
        if first == b'"' {
            // A string value: run to the closing quote (backslash-escaped
            // quotes are skipped).
            end += 1;
            while end < bytes.len() && !(bytes[end] == b'"' && bytes[end - 1] != b'\\') {
                end += 1;
            }
            end += 1;
        } else if first == b'{' || first == b'[' {
            // An object/array value: depth-match the brackets, skipping the
            // contents of strings (a `{` inside a string value is not a
            // bracket). NOTE the `==` comparisons — a bare `open =>` match arm
            // would be a NEW BINDING that matches every byte, not a
            // comparison, and would silently mis-scan the whole line.
            let close = if first == b'{' { b'}' } else { b']' };
            let mut depth = 0usize;
            let mut in_string = false;
            while end < bytes.len() {
                let b = bytes[end];
                if in_string {
                    if b == b'"' && bytes[end.saturating_sub(1)] != b'\\' {
                        in_string = false;
                    }
                } else {
                    match b {
                        _ if b == b'"' => in_string = true,
                        _ if b == first => depth += 1,
                        _ if b == close => {
                            depth -= 1;
                            if depth == 0 {
                                end += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                end += 1;
            }
        } else {
            panic!("unsupported value shape for `{key}`");
        }
        // Swallow one adjacent comma (either side — the key's position in the
        // encoded line is serde's business, not this helper's).
        let mut from = start;
        let mut to = end;
        if to < line.len() && bytes[to] == b',' {
            to += 1;
        } else if from > 0 && bytes[from - 1] == b',' {
            from -= 1;
        }
        format!("{}{}", &line[..from], &line[to..])
    }

    /// (Fulcrum T3) The `config-update` frame's WIRE NAME is `config-update`
    /// and it is routed by `decode_inbound`'s EXPLICIT allow-list.
    ///
    /// The routing is pinned separately from the round-trip on purpose: the
    /// decoder matches an explicit `type` list and sends everything else to
    /// `Unknown { raw }`, so a `config-update` frame that is not in that list
    /// is a SILENT no-op over real IPC — the live-reload push would look like
    /// it worked (the Supervisor sent a line, nothing errored) while the Worker
    /// dropped it. Same reason `decode_inbound_routes_a_compact_line_to_the_compact_variant`
    /// exists.
    #[test]
    fn decode_inbound_routes_a_config_update_line_to_the_config_update_variant() {
        let line = r#"{"type":"config-update","filePolicy":{"reads":"ask","writes":"ask","shell":"ask"},"web":{"braveApiKey":"b-key","tavilyApiKey":"","perplexityApiKey":"","searxngUrl":"","defaultProvider":"brave","proxy":""},"agentOverrides":{"scout":{"model":"fake/m3"}}}"#;
        match decode_inbound(line).expect("the `config-update` line decodes") {
            Inbound::ConfigUpdate {
                file_policy,
                web,
                agent_overrides,
            } => {
                assert_eq!(
                    file_policy.map(|p| p.reads),
                    Some(crate::policy::AccessPolicy::Ask),
                    "the pushed policy arrives (the top-level keys ARE camelCase \
                     — `StartEnv`'s casing applies here too)"
                );
                assert_eq!(web.map(|w| w.brave_api_key).as_deref(), Some("b-key"));
                assert_eq!(
                    agent_overrides
                        .as_ref()
                        .and_then(|m| m.get("scout"))
                        .and_then(|e| e.model.as_deref()),
                    Some("fake/m3"),
                    "ADR 0023's next-dispatch property rides the push"
                );
            }
            other => panic!(
                "`config-update` must decode to `Inbound::ConfigUpdate`, got {other:?} \
                 (the `decode_inbound` type allow-list is missing `config-update`)"
            ),
        }
    }

    /// (Fulcrum T3) Every field of `config-update` is optional in BOTH
    /// directions: a frame that carries only ONE of the three updates decodes
    /// with the other two `None` — `None` means "leave that field alone",
    /// which is what makes a partial push (and an older Supervisor that only
    /// learned about one of the three) safe rather than destructive.
    #[test]
    fn a_config_update_frame_with_missing_fields_leaves_them_alone() {
        let only_policy = r#"{"type":"config-update","filePolicy":{"reads":"sandboxed","writes":"ask","shell":"ask"}}"#;
        match decode_inbound(only_policy).expect("a policy-only push decodes") {
            Inbound::ConfigUpdate {
                file_policy,
                web,
                agent_overrides,
            } => {
                assert_eq!(
                    file_policy.map(|p| p.reads),
                    Some(crate::policy::AccessPolicy::Sandboxed)
                );
                assert!(web.is_none(), "no web key = leave the web config alone");
                assert!(
                    agent_overrides.is_none(),
                    "no overrides key = leave the override layer alone"
                );
            }
            other => panic!("expected `ConfigUpdate`, got {other:?}"),
        }
        // An entirely empty frame is legal and means "nothing changed" — the
        // permissive direction an OLD worker's decode relies on.
        match decode_inbound(r#"{"type":"config-update"}"#).expect("an empty push decodes") {
            Inbound::ConfigUpdate {
                file_policy,
                web,
                agent_overrides,
            } => {
                assert!(file_policy.is_none() && web.is_none() && agent_overrides.is_none());
            }
            other => panic!("expected `ConfigUpdate`, got {other:?}"),
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
                clear_thinking: false,
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
            // (Fulcrum T3) The live-reload push, with all three fields set.
            Inbound::ConfigUpdate {
                file_policy: Some(crate::policy::FilePolicy {
                    reads: crate::policy::AccessPolicy::Ask,
                    writes: crate::policy::AccessPolicy::Ask,
                    shell: crate::policy::AccessPolicy::Sandboxed,
                    rules: Vec::new(),
                }),
                web: Some(crate::web::config::WebSettings {
                    brave_api_key: "b".to_string(),
                    ..crate::web::config::WebSettings::default()
                }),
                agent_overrides: Some(std::collections::HashMap::from([(
                    "scout".to_string(),
                    crate::config_types::AgentOverrideFields {
                        model: Some("fake/m3".to_string()),
                        thinking: Some("low".to_string()),
                        tools: Some(vec!["read".to_string()]),
                    },
                )])),
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
            // The version-negotiation opener (Fulcrum T1). The list is
            // enumerated BY HAND, so a new variant nobody added here is an
            // untested coverage hole — the codec would round-trip it
            // silently broken (e.g. a `rename` typo that decodes to
            // `Unknown`) and every test would stay green.
            Outbound::Hello {
                protocol_version: PROTOCOL_VERSION,
                client_version: "0.1.0".to_string(),
            },
            Outbound::Ready {
                version: "0.1.0".to_string(),
                session_id: None,
            },
            Outbound::Ready {
                version: "0.1.0".to_string(),
                session_id: Some("s1".to_string()),
            },
            Outbound::Event {
                event: crate::events::RpcEvent::agent_settled,
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
                rows: vec![crate::harness::store::DisplayRow {
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
            crate::events::RpcEvent::agent_start,
            crate::events::RpcEvent::turn_start,
            crate::events::RpcEvent::text_delta {
                content_index: 0,
                delta: "hi".to_string(),
            },
            crate::events::RpcEvent::message_update {
                usage: serde_json::json!({ "inputTokens": 5 }),
                assistant_message_event: serde_json::json!({ "type": "textDelta" }),
            },
            crate::events::RpcEvent::compaction_end {
                reason: crate::events::CompactionReason::Threshold,
                result: None,
                aborted: false,
                will_retry: false,
                error_message: None,
            },
            crate::events::RpcEvent::unknown {
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
        use crate::events::CompactionReason;

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
            crate::events::RpcEvent::compaction_start {
                reason: CompactionReason::Overflow,
            },
            crate::events::RpcEvent::compaction_end {
                reason: CompactionReason::Threshold,
                result: None,
                aborted: false,
                will_retry: false,
                error_message: None,
            },
            crate::events::RpcEvent::compaction_start {
                reason: CompactionReason::Manual,
            },
            crate::events::RpcEvent::compaction_end {
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
            event: crate::events::RpcEvent::compaction_start {
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

    /// `decode_outbound` routes a `hello` line to its variant (the
    /// version-negotiation opener). The failure mode this pins: the
    /// decoder is PERMISSIVE, so a `hello` type missing from the
    /// explicit allow-list is silently dropped as `Unknown { raw }` —
    /// a green suite that never saw the handshake.
    #[test]
    fn decode_outbound_routes_a_hello_line_to_the_hello_variant() {
        // The binary version comes from the crate (the same value the
        // Worker stamps), never a hardcoded copy that drifts on bump.
        let line = format!(
            r#"{{"type":"hello","protocolVersion":{},"clientVersion":"{}"}}"#,
            PROTOCOL_VERSION,
            env!("CARGO_PKG_VERSION")
        );
        match decode_outbound(&line).expect("the `hello` line decodes") {
            Outbound::Hello {
                protocol_version,
                client_version,
            } => {
                assert_eq!(protocol_version, 1);
                assert_eq!(client_version, env!("CARGO_PKG_VERSION"));
            }
            other => panic!(
                "`hello` must decode to `Outbound::Hello`, got {other:?} \
                 (the `decode_outbound` type allow-list is missing `hello`)"
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
    /// error — a peer may emit a frame this build predates; see the
    /// `decode_outbound` doc for how that squares with the `hello`
    /// version negotiation), on both sides; the raw line is preserved.
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

    /// `stamp_outbound` stamps a FLAT sibling `frameSeq` beside the
    /// `type` discriminator and counts monotonically from 1 (the writer's
    /// per-session cursor — Fulcrum T1; T10's `attach { afterSeq }` reads
    /// exactly this field).
    #[test]
    fn stamp_outbound_adds_a_monotonic_frame_seq_beside_the_type() {
        use std::sync::atomic::AtomicU64;
        let seq = AtomicU64::new(0);
        let mut lines = Vec::new();
        for n in 0..3 {
            let line = stamp_outbound(
                &seq,
                &Outbound::SinkFrame {
                    event: format!("session-update-{n}"),
                    payload: serde_json::json!({ "sessionId": "s1" }),
                },
            );
            assert!(line.ends_with('\n'), "one JSONL line per frame");
            lines.push(line);
        }
        for (i, line) in lines.iter().enumerate() {
            let expected = format!(r#""frameSeq":{}"#, i + 1);
            assert!(
                line.contains(&expected),
                "frame {i} carries the 1-based cursor {expected}: {line}"
            );
            assert!(
                line.contains(r#""type":"sink-frame""#),
                "the sibling insert never disturbs the `type` discriminator: {line}"
            );
        }
        // The cursor is FLAT (a sibling of `type`), not nested under the
        // frame's own `payload` field — nesting collides with the
        // `payload` that `SinkFrame` / `PermissionRequest` /
        // `InteractiveRequest` / `FetchRenderRequest` already carry.
        let v: serde_json::Value = serde_json::from_str(lines[0].trim()).expect("valid JSON");
        assert_eq!(v["frameSeq"], 1, "flat sibling, not nested");
        assert_eq!(v["event"], "session-update-0");
        assert_eq!(v["payload"]["sessionId"], "s1");
    }

    /// The cursor does NOT collide with the transcript-row `seq` — the
    /// two namespaces coexist on one frame.
    #[test]
    fn stamp_outbound_does_not_collide_with_the_transcript_row_seq() {
        use std::sync::atomic::AtomicU64;
        let seq = AtomicU64::new(0);
        let line = stamp_outbound(
            &seq,
            &Outbound::TranscriptInsert {
                session_id: "s".to_string(),
                seq: 5,
                role: "user".to_string(),
                content_json: "{}".to_string(),
            },
        );
        let v: serde_json::Value = serde_json::from_str(line.trim()).expect("valid JSON");
        assert_eq!(v["seq"], 5, "the transcript ROW seq is untouched");
        assert_eq!(v["frameSeq"], 1, "the transport cursor is a separate field");
        assert_eq!(v["type"], "transcript-insert");
        // And it still decodes through the typed reader (the injected
        // field is invisible: no `deny_unknown_fields` on `Outbound`).
        match decode_outbound(&line).expect("an injected field does not break the decode") {
            Outbound::TranscriptInsert { seq: row_seq, .. } => assert_eq!(row_seq, 5),
            other => panic!("expected `TranscriptInsert`, got {other:?}"),
        }
    }

    /// The cursor does NOT touch the INNER `"seq": 0` Tauri-payload stub
    /// on an `interactive-event` frame — the third `seq` in the repo, and
    /// the one T2 depends on staying at 0 while the cursor climbs.
    #[test]
    fn stamp_outbound_leaves_the_inner_interactive_event_seq_zero_untouched() {
        use std::sync::atomic::AtomicU64;
        let seq = AtomicU64::new(0);
        let line = stamp_outbound(
            &seq,
            &Outbound::SinkFrame {
                event: "interactive-event".to_string(),
                payload: serde_json::json!({
                    "sessionId": "s",
                    "seq": 0,
                    "event": "todos_update",
                    "payload": { "source": "main", "todos": [] }
                }),
            },
        );
        let v: serde_json::Value = serde_json::from_str(line.trim()).expect("valid JSON");
        assert_eq!(v["payload"]["seq"], 0, "the INNER payload seq stays 0");
        assert_eq!(v["frameSeq"], 1, "the OUTER cursor is a different level");
        assert!(
            line.contains(r#""seq":0"#),
            "the inner stub is byte-identical on the wire: {line}"
        );
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

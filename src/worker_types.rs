//! The Worker IPC wire VOCABULARY — the DATA half of `agent/worker/protocol.rs`.
//!
//! ## Why the types move and the codec stays
//!
//! Plan Step E.1/E.3 (edges E1 and E3): the crate must be able to NAME these
//! types, and it cannot name a type that lives in `archimedes_lib`. Two edges
//! forced it — `agent/persist.rs:56` consumes `StoreFrame` and
//! `agent/subagent.rs:29` builds `SubagentDispatchWire` — and once `StoreFrame`
//! moved, `StartEnv`/`StartMode`/`DesktopConfig` had to come with it, because
//! `Inbound::Start(Box<StartEnv>)` and `Inbound::ConfigUpdate` are the frames the
//! same vocabulary speaks, and splitting one enum's variants across two packages
//! is not a thing serde can do.
//!
//! What the S4 cut left desktop was the Worker's PROCESSING: `worker/**`
//! (`client`, `core`, `dispatch`, `manager`, `sink`, `store`) — all of which T8
//! then moved into this crate (S8.1–S8.3), so what was a split across a package
//! boundary is now a split between two modules of ONE package. The cut's
//! reasoning still holds and no byte of the wire changed: the codec
//! (`encode_*`/`decode_*`/`stamp_outbound`, `PROTOCOL_VERSION`, `ProtoError`,
//! `Inbound`, `Outbound`) stayed here with the data it speaks. `Inbound`/
//! `Outbound` are still NOT wire DATA in the sense this module cares about —
//! nothing else in the crate names them; the crate constructs `StoreFrame` and
//! `worker/protocol.rs` frames it. The cut lands between "what a frame IS" and
//! "how a frame travels", and it was never a cycle: this module depends on
//! nothing for those types.
//!
//! The desktop's `agent/worker/mod.rs` re-export shell still re-exports every
//! item below (and `worker/protocol.rs` re-exports the codec), so all ~200
//! existing `crate::agent::worker::protocol::*` and
//! `archimedes_lib::agent::worker::protocol::*` paths keep resolving with ZERO
//! edits — which, per the S4 ruling, IS the E1/E3 proof. The serde attributes are
//! untouched, so no byte on the wire changes.
//!
//! ## The `DesktopConfig` doc-comment's one honest exception
//!
//! Its text names `crate::config::load_settings`, and this crate has a standing
//! pin that the crate contains no `load_settings` reference (Step C). That is not
//! a contradiction: the pin is about CODE — the crate opening the desktop's
//! `settings.json` — and the S3 precedent for reading it that way is recorded in
//! the crate's own `mcp/manager.rs`, whose doc prose explains why `open.rs`
//! cannot move. The sentence here is prose explaining where the caller builds the
//! value from; it compiles to nothing. `config_types`' module docs carry the
//! matching statement of the boundary.

use crate::catalog::{Model, ModelCatalog};
use crate::config_types::{AgentOverrideFields, ProviderConfig};
use crate::definition_permission::PermissionBlock;
use crate::policy::FilePolicy;
use crate::provider_types::ChatMessage;
use crate::subagent_types::AgentNameResolution;
use crate::web::config::WebSettings;

// ── `StartMode` ─────────────────────────────────────────────────────────────────

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

// ── `StartEnv` ──────────────────────────────────────────────────────────────────

/// The `start` envelope as a standalone struct (Tasks 3/4/7 reference
/// it): the `Inbound::Start` payload fields, verbatim.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartEnv {
    pub session_id: String,
    pub cwd: String,
    pub mode: StartMode,
    pub transcript: Option<Vec<ChatMessage>>,
    pub model: Model,
    pub catalog: ModelCatalog,
    pub thinking: Option<String>,
    pub trusted: bool,
    pub enabled_tools: Option<Vec<String>>,
    /// (issue #101) The per-agent `permission:` ruleset as the PARENT's
    /// resolved launch carried it — VERBATIM from
    /// [`crate::launch_config::LaunchConfig::permission`], i.e. the
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
    pub permission: Option<PermissionBlock>,
    pub config_dir: String,
    pub subagent_enabled: bool,
    pub system_prompt: Option<String>,
    /// The stamp for THIS Worker's life within the Session (ADR 0049). Every
    /// `messageId` the harness mints carries it, so two incarnations of one
    /// Session cannot write the same display row. Minted per envelope — ONE
    /// stamp per `Start`.
    pub incarnation: String,
    /// (Fulcrum T2) The session's ALREADY-STORED cost total in USD, so a resumed
    /// loop continues the figure instead of restarting it at zero.
    ///
    /// The `Resume`-seeding half of the cumulative-cost contract: `cost_update`
    /// frames now carry the session's CUMULATIVE total, which the loop computes
    /// from its own `cost_acc` accumulator. A fresh loop's accumulator starts at
    /// `0.0`, so without this field a resumed session's first frame would read
    /// `0 + first_call` — and since `Db::record_session_cost` is now a SNAPSHOT
    /// overwrite, that smaller figure would REPLACE the stored total (the retired
    /// delta-sum merely added, so a lost base was invisible). ADR 0082 rejected a
    /// loop-side total precisely because "a re-incarnation or `Resume` rebuilds
    /// the loop with no memory of the old figure"; the answer is to hand the
    /// figure back to each incarnation instead of asking it to remember.
    ///
    /// ADDITIVE and camelCase like every other field on this struct
    /// (`costUsd`): `#[serde(default)]` so a Supervisor that predates the field
    /// still decodes — `None` means "no stored figure", which seeds `0.0`. The
    /// `permission` field above is the same additive template.
    ///
    /// Set ONLY on the resume path, via [`StartEnv::with_seed_cost`]; a fresh
    /// session and a fresh child carry `None`.
    #[serde(default)]
    pub cost_usd: Option<f64>,
    /// (Fulcrum T3) The desktop's file-access policy — PACKAGED, so the Worker
    /// never opens `settings.json` to find it.
    ///
    /// THE field whose disk read this task removes: `PermissionContext::refresh`
    /// re-read `file_policy` from `settings.json` at the top of EVERY turn (ADR
    /// 0030 Deviation 4). It now consumes the value that rode the envelope, and
    /// a later edit reaches the loop through the `config-update` push
    /// (`Inbound::ConfigUpdate`, whose codec lives in this crate's
    /// `worker/protocol.rs` since T8/S8.1 — it was the desktop's when this line
    /// was written) instead of through a second disk read — the
    /// live-reload property is preserved by the push, not lost with the read.
    ///
    /// ADDITIVE and camelCase (`filePolicy`) like every other field here:
    /// `#[serde(default)]` so a Supervisor that predates the field still
    /// decodes. The default is `FilePolicy::default()` — all-`Allow` — which is
    /// BYTE-IDENTICAL to what an old Supervisor produced by pointing
    /// `config_dir` at a dir with no `settings.json` in it (ADR 0030 Deviation
    /// 2's out-of-the-box posture), so the additive path cannot change anyone's
    /// gating.
    ///
    /// [`StartEnv::with_desktop_config`] is the construction seam.
    #[serde(default)]
    pub file_policy: FilePolicy,
    /// (Fulcrum T3) The web tools' configuration — PACKAGED (the second of the
    /// five). Same per-turn re-read, same push, same additive default
    /// (all-empty ⇒ the env-only `WebConfig`, exactly what today's
    /// `config_dir: None` case yields).
    #[serde(default)]
    pub web: WebSettings,
    /// (Fulcrum T3) The desktop `mcpServers` layer — PACKAGED (the third).
    /// `None` on the manager side means "no desktop layer"; here the EMPTY map
    /// means the same thing, and that is what an old envelope decodes to.
    ///
    /// RESIDUAL LIMITATION, recorded not fixed (status file Decisions log):
    /// this is a session-CONSTRUCTION snapshot. A user who adds an MCP server
    /// mid-session needs a session restart — which matches today's behaviour,
    /// because the `McpManager`'s server set is built once per `AgentLoop`.
    /// `config-update` deliberately does NOT carry it.
    #[serde(default)]
    pub mcp_servers: std::collections::HashMap<String, serde_json::Value>,
    /// (Fulcrum T3) The ADR 0066 agent-override layer — PACKAGED (the fourth),
    /// and PACKAGED POST-FOLD: the Supervisor builds this from
    /// `crate::config::load_settings`, which folds the retired `subagentModels`
    /// into `agent_overrides` and clears the legacy map BEFORE the envelope is
    /// built. The Worker must NEVER see the raw `subagent_models` map (ADR
    /// 0066).
    ///
    /// ADR 0067's tri-state rides untouched: an entry's `tools` is `None` when
    /// the list is empty — an empty list is ABSENT, never `Some(vec![])` — and
    /// nothing in the packaging collapses or invents a state.
    ///
    /// The ABSENCE-vs-empty distinction lives one level up, on the loop-side
    /// mirror and on `resolve_launch`'s parameter, both `Option<HashMap<…>>`:
    /// `None` = "a layer that cannot speak" (today's `config_dir: None`),
    /// `Some(empty)` = a live layer that says nothing. Both collapse to
    /// `global: None` inside `override_layer`, and both stay distinguishable
    /// from `None` at the seam — pinned by
    /// `packaged_settings_keeps_no_layer_and_an_empty_map_apart`.
    ///
    /// This field rides the `config-update` push, so ADR 0023's "a settings
    /// edit applies at the NEXT dispatch" property survives the removal of the
    /// per-dispatch disk read.
    #[serde(default)]
    pub agent_overrides: std::collections::HashMap<String, AgentOverrideFields>,
    /// (Fulcrum T3) The user's provider rows — PACKAGED (the FIFTH field, the
    /// one the spec's four-field list missed; issue #129).
    ///
    /// The whole `Vec<ProviderConfig>`, not a pre-expanded header map, because
    /// TWO sites need the headers and they need DIFFERENT rows:
    /// `default_build_loop` needs the START model's provider, and
    /// `AgentLoop::set_model` needs the NEW model's provider on every mid-session
    /// switch. A single expanded map cannot answer the second question.
    ///
    /// The `headers` values ride as the RAW templates (`"$CORP_TENANT"`); the
    /// `$VAR` expansion (`provider_auth::headers::expand_map`) stays worker-side
    /// and runs against the WORKER's process env — which is the same env the
    /// disk-read version expanded against, because the Worker is spawned by the
    /// Supervisor and inherits its environment. So packaging the rows changes
    /// where the rows come from, not what the headers resolve to.
    ///
    /// `#[serde(default)]` → the empty vec = no custom headers, byte-identical
    /// to the old `config_dir: None` case. Like `mcp_servers`, this is a
    /// construction snapshot and deliberately NOT on the push (the recorded
    /// hole; a model switch re-resolves from this snapshot, exactly as today).
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
    /// The directory the Worker's ALWAYS-ON diagnostics channel
    /// ([`crate::diagnostics`], `<datadir>/archimedes/diagnostics.log`) writes to —
    /// the value `diagnostics::init` takes, carried across the wire.
    ///
    /// THE field whose absence made the Worker's own diagnostics unreachable.
    /// Every `diagnostics::init` in this crate WAS `#[cfg(test)]` before this field
    /// existed, so `record` was a silent no-op inside a `--worker` child: the
    /// tool-call-drop line, the silent-settle line and `log_wire`'s provider line all
    /// reached NO file in a default launch, which is the card's symptom ("a tool call
    /// dropped or a turn settled without an answer and nothing says so"). The
    /// Supervisor arms the channel in its own `run()`; a Worker never gets there — so
    /// the directory now rides this envelope, and `worker/core.rs`'s `Inbound::Start`
    /// arm is a NON-test `diagnostics::init` that arms what arrives here.
    ///
    /// ADDITIVE and camelCase (`diagnosticsDir`) like every other field here —
    /// `permission`, `cost_usd` and the five packaged config fields are the same
    /// template — so `from_parts` gains NO positional parameter. A 14th parameter
    /// would break EVERY call site in BOTH repos against a registry pin neither repo
    /// can move alone (measured when this field was added: 8 in this repo, 11 in the
    /// desktop — re-measure with `grep -rn 'from_parts('` in each rather than
    /// trusting this number) and every existing caller and fixture is unchanged and
    /// stays **unarmed** (`None` → the old silence, deliberately).
    ///
    /// # The two wire directions, VERIFIED rather than assumed
    ///
    /// Checked at this struct's declaration and at `worker/protocol.rs`:
    /// `StartEnv` derives neither `#[non_exhaustive]` nor
    /// `#[serde(deny_unknown_fields)]`, and `decode_inbound` deliberately ignores
    /// unknown keys. So the field is compatible in BOTH directions —
    /// * old Supervisor → new Worker: the key is absent, `Option` +
    ///   `#[serde(default)]` decodes to `None` → UNARMED, byte-identical to
    ///   today's Worker;
    /// * new Supervisor → old Worker (0.4.1): `diagnosticsDir` is an unknown key,
    ///   the decoder drops it, the frame still decodes and the Worker still runs —
    ///   it just stays unarmed.
    ///
    /// Both are pinned by
    /// [`the_start_env_carries_the_diagnostics_dir_as_diagnostics_dir`](crate::worker::protocol),
    /// not asserted here.
    ///
    /// # Why `Option<PathBuf>` and not the `String` `config_dir` uses
    ///
    /// `config_dir` is a `String` because it is re-parsed as text and compared as
    /// text — it is a key the frontend joins on and a path `load_settings` opens.
    /// This value is NEVER re-parsed: it goes straight into
    /// [`crate::diagnostics::init`], whose parameter IS an `Option<PathBuf>`, so a
    /// `String` here would buy nothing and cost a `.into()` plus the chance of
    /// handing `init` a path-shaped string that was never a path. serde writes a
    /// `PathBuf` as a JSON string and reads one back from a string (its
    /// `PathBufVisitor` takes `visit_str`/`visit_string` — and the `bytes` pair,
    /// which a JSON deserializer never calls), so the WIRE FORM is identical to
    /// `config_dir`'s either way — the type is a statement about the consumer, not
    /// about the bytes.
    ///
    /// `None` (and an absent key, and a JSON `null`) all mean UNARMED. An EMPTY
    /// string is not special-cased and must not become one: `diagnostics::append_to`
    /// calls `create_dir_all("")`, which succeeds, and then writes to a
    /// CWD-relative `diagnostics.log` — the leftover a hand-run once left in this
    /// repo's own working directory. A host that wants the channel off passes
    /// `None`; the Supervisor passes the app data dir it already resolves.
    ///
    /// # The one cost of the `PathBuf`, stated rather than hidden
    ///
    /// serde serializes a `PathBuf` by way of `Path::to_str`, and a path that is
    /// NOT valid UTF-8 is a serialize ERROR ("path contains invalid UTF-8
    /// characters") — and [`crate::worker::protocol::encode_inbound`] `.expect`s on
    /// any encode failure. So a non-UTF-8 directory would PANIC the Supervisor at
    /// `send_start` rather than come back as a `WorkerError`. Measured, not
    /// reasoned: a `StartEnv` carrying `/tmp/bad\xffpath` makes `encode_inbound`
    /// panic with exactly `path contains invalid UTF-8 characters`. A `String`
    /// could not fail that way, because it cannot hold one. Accepted on three
    /// grounds: on the DESKTOP surface the value comes from `dirs::data_dir()` / the
    /// app's own data-dir resolution, which no user types; `SessionInfo::cwd` shows
    /// this crate already carries a `PathBuf` across a process boundary for the
    /// reasons its own doc gives, so the posture is existing and not new (it crosses
    /// TAURI's IPC, NOT the worker boundary, and it has the same non-UTF-8 weakness
    /// there — it is evidence of posture only, not of a solved problem); and the
    /// alternative failure a `String` invites — handing `diagnostics::init` a
    /// path-shaped string that a `PathBuf` would have refused at the door — is the
    /// worse of the two, because it moves the lossy conversion INTO the Worker's
    /// `init(PathBuf::from(s))` instead of failing at the Supervisor's encode.
    ///
    /// # Where the value can come from, stated honestly
    ///
    /// It is NOT true that this value never comes from typed input. On the
    /// `fulcrum serve` surface it is: `serve.rs`'s `start_env` parses the client's
    /// `start` payload with `serde_json::from_value::<StartEnv>` and forwards it
    /// VERBATIM, so a `fulcrum serve` CLIENT can set `diagnosticsDir` and cause a
    /// Worker to `create_dir_all` and append `diagnostics.log` at a path the client
    /// chose. The daemon arms nothing of its own — it just does not strip what
    /// arrived.
    ///
    /// Two mitigations are real, and they are the reason this is documented rather
    /// than closed: (1) a socket peer whose uid is not the daemon's is refused at
    /// accept (`TransportError::PeerIdentity`, `transport::unix`), so the reachable
    /// set is same-uid only — and a same-uid process can already open any file this
    /// field could name, so the field grants no write the peer did not already have;
    /// (2) what lands in that file is only ever counts, a closed wire-enum name and
    /// sanitised ids (`log_wire`, `emit_harness_line`, the settle line, `pump.rs`'s
    /// lag line — each re-verified per site), never prompt text, tool arguments or
    /// model output, so the worst a client achieves is a stray file of numbers (plus
    /// an unbounded-but-rotated `diagnostics.log`, the same rotation `debuglog`
    /// already accepts). What is NOT real is "never from typed input", and this doc
    /// used to claim it. If the residual ever bites, the follow-up is an error around
    /// the `encode_line` `.expect` — not a type change here.
    #[serde(default)]
    pub diagnostics_dir: Option<std::path::PathBuf>,
    /// (ADR 0102 / issue #183 F2) The specs of the tools whose SUBSTANCE lives in
    /// the SUPERVISOR, arriving as DATA. The wire key is camelCase `hostTools`
    /// (`StartEnv`'s own `rename_all`).
    ///
    /// # Why specs cross the boundary and executors never do
    ///
    /// The engine cannot advertise a spec it has never seen, so the spec travels.
    /// The EXECUTOR stays on this side of the wire: a Worker is a second process of
    /// the SAME binary (`WorkerHandle::spawn(current_exe())` + `--worker`, ADR
    /// 0025), so the desktop cannot hand it a Rust object even in principle. The
    /// engine therefore wraps each spec in
    /// [`HostTool`](crate::tools::exec::HostTool) itself —
    /// `builtins() ++ host_entries(env.host_tools)` — and a call to one of them
    /// goes back out over
    /// [`Outbound::HostToolCall`](crate::worker::protocol::Outbound::HostToolCall)
    /// to the Supervisor that owns it. No desktop type crosses the boundary, so ADR
    /// 0091's rule holds verbatim: *the desktop drives; the crate never pulls.*
    ///
    /// # The additive template, verbatim
    ///
    /// `#[serde(default)]` on the LAST field, an inert value in `from_parts`, a
    /// `with_*` builder ([`StartEnv::with_host_tools`]) for the real Supervisor
    /// path, and NO new `from_parts` parameter — `permission` / `cost_usd` /
    /// `file_policy` / `diagnostics_dir` are the same template, for the same reason
    /// (a 13-parameter positional constructor with a call site in every fixture in
    /// BOTH repos). The `Vec`'s `serde(default)` is the EMPTY vec = "no host tools",
    /// byte-identical to an old envelope's absence, so a pre-F2 `start` line decodes
    /// to the surface it spoke. `ToolSpec` needs no new serde attributes: its three
    /// field names are single lowercase words, so camelCase spells them identically,
    /// and `parameters` is a raw `Value` so an arbitrary JSON Schema round-trips.
    ///
    /// # These are NOT part of `DesktopConfig`, and a child gets them explicitly
    ///
    /// `DesktopConfig::from_env` copies exactly the FIVE fields the Worker used to
    /// read out of `settings.json`; a host tool is not one of those, so folding it
    /// in would make that bundle's doc a lie. Subagent inheritance is therefore an
    /// explicit line in `subagent_flow.rs` (`with_host_tools(parent_env.host_tools…)`),
    /// not a serde default doing it by accident.
    ///
    /// # The daemon surface, stated honestly (the reason the loud refusal exists)
    ///
    /// `serve.rs`'s `start_env` parses the client's `start` payload and forwards it
    /// VERBATIM, so a `fulcrum serve` CLIENT can set `hostTools` and have them
    /// ADVERTISED to the model — while the daemon has no way to ANSWER them (its
    /// `on_event` carries no inbound writer). That is exactly the
    /// advertised-but-dead shape, and the same situation `diagnostics_dir`'s doc
    /// records for its own field. Two mitigations are real: a socket peer whose uid
    /// is not the daemon's is refused at accept (`TransportError::PeerIdentity`), so
    /// the reachable set is same-uid only — a peer that can already run this code
    /// itself; and the unhosted case answers a `ToolResult::fail` through the
    /// daemon's `host-tool-unsupported` refusal rather than a silent park, so the
    /// worst a client achieves is a tool that fails with a reason.
    #[serde(default)]
    pub host_tools: Vec<crate::provider_types::ToolSpec>,
}

// ── `DesktopConfig` ─────────────────────────────────────────────────────────────

/// The FIVE desktop-config fields the Worker used to read out of
/// `settings.json` (Fulcrum T3), bundled so they are PACKAGED at one call site
/// with NAMED fields rather than appended to a 13-parameter positional
/// constructor.
///
/// The bundle exists for the same reason [`StartEnv::with_seed_cost`] does:
/// `from_parts` is a positional constructor with a call site in EVERY fixture and
/// production envelope builder in BOTH repos — every one of them would gain five
/// arguments (measured when the diagnostics directory was added: 8 here, 11 in the
/// desktop; the "fourteen" this doc used to state predated the T3/T8 moves, and this
/// census has since been wrong twice more, so the claim that matters is the class:
/// re-run `grep -rn 'from_parts('` in each repo rather than trusting a number here).
/// Here the five types
/// are all DISTINCT, so a swapped slot is a compile error rather than the silent
/// mis-slot T2 avoided — but the builder still keeps the fixtures that
/// need NONE of these fields untouched, and it names the five as the SET they are:
/// the packaged desktop config, one concept, one place to look when a sixth field
/// joins it.
///
/// Every field is the DESKTOP's own type (T3 moves no types — the definitions
/// stay in `crate::config` / `agent::policy` and move in T5–T7).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DesktopConfig {
    pub file_policy: FilePolicy,
    pub web: WebSettings,
    pub mcp_servers: std::collections::HashMap<String, serde_json::Value>,
    pub agent_overrides: std::collections::HashMap<String, AgentOverrideFields>,
    pub providers: Vec<ProviderConfig>,
}

impl DesktopConfig {
    /// The child's five, copied from the PARENT's stored envelope (Fulcrum T3,
    /// the `subagent_flow.rs` hop).
    ///
    /// One named function rather than five hand-copied `.clone()`s because this
    /// line IS the ADR 0023 live property on the child path: a `config-update`
    /// push that overwrites the PARENT's packaged fields propagates to every
    /// NEXT dispatch's child precisely because the child's five are read off
    /// the parent's live envelope here, and nothing else. A field added to the
    /// bundle and forgotten in a hand-written copy would silently freeze one
    /// knob for children while the parent kept live-reloading it; through this
    /// function there is no list to forget one from.
    pub fn from_env(parent: &StartEnv) -> Self {
        Self {
            file_policy: parent.file_policy.clone(),
            web: parent.web.clone(),
            mcp_servers: parent.mcp_servers.clone(),
            agent_overrides: parent.agent_overrides.clone(),
            providers: parent.providers.clone(),
        }
    }
}

impl StartEnv {
    /// The Task-3/4/5 construction site.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        session_id: String,
        cwd: String,
        mode: StartMode,
        transcript: Option<Vec<ChatMessage>>,
        model: Model,
        catalog: ModelCatalog,
        thinking: Option<String>,
        trusted: bool,
        enabled_tools: Option<Vec<String>>,
        config_dir: String,
        subagent_enabled: bool,
        system_prompt: Option<String>,
        permission: Option<PermissionBlock>,
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
            incarnation: crate::types::mint_incarnation(),
            // A constructor-built `StartEnv` is a FRESH session (or a fresh
            // child): nothing is stored yet, so there is nothing to seed. The
            // resume path opts in via [`StartEnv::with_seed_cost`] instead of
            // widening this signature — `from_parts` is a 13-parameter
            // positional constructor with a call site in every fixture and
            // envelope builder in both repos (8 here, 11 in the desktop when the
            // diagnostics directory landed — re-measure with
            // `grep -rn 'from_parts('` rather than trusting it), and T3's five
            // config fields ride [`StartEnv::with_desktop_config`] for the
            // same reason.
            cost_usd: None,
            // (Fulcrum T3) The five packaged desktop-config fields are INERT
            // here on purpose: every one of them is `#[serde(default)]` on the
            // wire, and the inert value is the SAME value an old envelope
            // decodes to — all-`Allow` policy, env-only web, no desktop MCP
            // layer, a silent override layer, no custom headers. A fixture that
            // builds through `from_parts` therefore gets the posture today's
            // "no `settings.json` anywhere" fixture had, unchanged. The real
            // Supervisor paths package the real values through
            // [`StartEnv::with_desktop_config`].
            file_policy: FilePolicy::default(),
            web: WebSettings::default(),
            mcp_servers: std::collections::HashMap::new(),
            agent_overrides: std::collections::HashMap::new(),
            providers: Vec::new(),
            // The diagnostics channel stays UNARMED here, and that is the load-
            // bearing default, not an omission. `from_parts` is what every fixture
            // in both repos calls, and what the desktop's five real-Worker tests
            // end up calling through their hand-built envelopes — none of them
            // isolates `HOME`/`XDG_DATA_HOME`, so an armed default would write into
            // the developer's real `~/.local/share/archimedes/diagnostics.log` on
            // every `cargo test`. The Supervisor opts a Worker in explicitly with
            // [`StartEnv::with_diagnostics_dir`].
            diagnostics_dir: None,
            // (ADR 0102 F2) No host tools, and that is the load-bearing default, not
            // an omission — the same argument `diagnostics_dir` above makes. Every
            // fixture in both repos builds through `from_parts`, and a fixture that
            // advertised a host tool with no Supervisor to answer it would park a
            // real oneshot. The Supervisor opts its session in with
            // [`StartEnv::with_host_tools`]; a child inherits the parent's through
            // the explicit line in `subagent_flow.rs`.
            host_tools: Vec::new(),
        }
    }

    /// Attach the HOST-OWNED tool specs to an already-built `StartEnv` (ADR 0102
    /// F2). The Supervisor's only edit of this field, and the sibling of
    /// [`StartEnv::with_diagnostics_dir`] for the same reason: a builder, not a 14th
    /// `from_parts` parameter.
    ///
    /// REPLACES, never extends — a second call is a new surface, matching
    /// [`AgentLoop::set_tool_registry`](crate::harness::r#loop::AgentLoop)'s rule, so
    /// "what this session can reach" never depends on call order.
    pub fn with_host_tools(mut self, tools: Vec<crate::provider_types::ToolSpec>) -> Self {
        self.host_tools = tools;
        self
    }

    /// Attach the FIVE packaged desktop-config fields to an already-built
    /// `StartEnv` (Fulcrum T3). The Supervisor's only edit of these five.
    ///
    /// A builder and NOT five more `from_parts` parameters, following
    /// [`StartEnv::with_seed_cost`]: the positional constructor has a call site in
    /// every fixture and envelope builder in BOTH repos (8 in this repo, 11 in the
    /// desktop when the diagnostics directory landed — re-measure with
    /// `grep -rn 'from_parts('`; the desktop half compiles against the PUBLISHED
    /// crate, so a positional change there is not even visible from here until the
    /// pin moves), and every one of them would gain five arguments — except the
    /// handful that go on to call this builder anyway. The Supervisor's session-start
    /// site is the ONE place that has a `Settings` in scope, and it calls this.
    ///
    /// The `agent_overrides` half MUST be the POST-FOLD map (ADR 0066) — i.e.
    /// whatever `crate::config::load_settings` produced, never the raw
    /// `subagent_models` map.
    pub fn with_desktop_config(mut self, config: DesktopConfig) -> Self {
        self.file_policy = config.file_policy;
        self.web = config.web;
        self.mcp_servers = config.mcp_servers;
        self.agent_overrides = config.agent_overrides;
        self.providers = config.providers;
        self
    }

    /// Attach the session's stored cost total to an already-built `StartEnv`
    /// (Fulcrum T2). The resume path's only edit of this struct.
    ///
    /// Deliberately a builder and NOT a fourteenth `from_parts` parameter: the
    /// positional constructor has a call site in every fixture and envelope builder
    /// in BOTH repos (8 in this repo, 11 in the desktop when the diagnostics
    /// directory landed — re-measure with `grep -rn 'from_parts('`), and T3 appends
    /// five config fields to that same list — two tasks re-plumbing one signature in
    /// the same wave is how a positional argument lands in the wrong slot. A
    /// field-placeholder builder cannot.
    pub fn with_seed_cost(mut self, total: Option<f64>) -> Self {
        self.cost_usd = total;
        self
    }

    /// Attach the always-on diagnostics directory (`diagnostics::init`'s target)
    /// to an already-built `StartEnv`. The Supervisor's only edit of this field,
    /// and the ONLY way a Worker's diagnostics channel ever gets armed.
    ///
    /// Deliberately a builder and NOT a fourteenth `from_parts` parameter,
    /// following [`StartEnv::with_seed_cost`] and
    /// [`StartEnv::with_desktop_config`]: the positional constructor is called by
    /// every fixture and envelope builder in both repos — 8 call sites in this repo
    /// and 11 in the desktop when this field landed, those 11 spread over 10 files
    /// (`harness_subagent_worker.rs` has two), which is why a per-file count there
    /// reads 10 and a per-call count reads 11 — and every one of them would gain
    /// an argument, against a crates.io pin that is a break neither repo can land
    /// alone. A builder leaves every one of them compiling and leaves every fixture
    /// `None`, i.e. unarmed, which is exactly the posture the unisolated real-Worker
    /// tests require.
    ///
    /// `None` UNARMS (the default, an old envelope, and a host's off switch all
    /// look like this). The value is passed to `diagnostics::init` verbatim by the
    /// Worker's `Start` handler — it is never resolved, joined onto anything, or
    /// fallen back to a platform directory here.
    pub fn with_diagnostics_dir(mut self, dir: Option<std::path::PathBuf>) -> Self {
        self.diagnostics_dir = dir;
        self
    }
}

// ── `StoreFrame` ────────────────────────────────────────────────────────────────

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
        rows: Vec<crate::store_rows::DisplayRow>,
    },
    ContextUsage {
        session_id: String,
        used: u64,
        window: u64,
    },
}

// ── `SubagentDispatchWire` ──────────────────────────────────────────────────────

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
    pub name_resolution: AgentNameResolution,
    pub launch: crate::launch_config::LaunchConfig,
    pub task: String,
    pub model_key: String,
}

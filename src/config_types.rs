//! The desktop settings' VALUE types (T5/S2 §2c's SPLIT, completed in S4).
//!
//! ## Why these three and not `config.rs`
//!
//! The plan's §2c ruling is that `config.rs` SPLITS rather than moves: the value
//! DTOs come here and `load_settings` STAYS desktop, because `load_settings` IS
//! the T3 disk boundary — moving it would put the desktop's `settings.json`
//! loader inside the engine and silently undo T3. S2 executed that split for
//! `WebSettings` (now `crate::web::config`); S4 completes it for the three the
//! packaged `StartEnv` names: `StartEnv.providers` is a `Vec<ProviderConfig>`,
//! `StartEnv.agent_overrides` is a `HashMap<String, AgentOverrideFields>`, and
//! `ProviderConfig.oidc` is an `Option<OidcConfig>`, so all three are embedded in
//! a wire type the crate must own. Leaving any of them desktop would have made a
//! crate type name a desktop type — the package cycle, arriving through a field.
//!
//! Nothing here reads a file. The disk read stays in the desktop's
//! `config.rs::load_settings`, which imports these types back
//! (desktop→crate, the allowed direction) so `crate::config::ProviderConfig` and
//! the other existing spellings keep resolving with zero edits — the same
//! re-export shape S2 used for `WebSettings`.
//!
//! `OidcConfig` is the fourth of the plan's §2c list. It has no crate-side
//! consumer on its own, but it is a FIELD TYPE of `ProviderConfig`, so it cannot
//! be left behind; the same embedded-type logic that pulled `AgentOverrideFields`
//! in applies one level deeper.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::policy::FilePolicy;
use crate::web::config::WebSettings;

// ── `ProviderConfig` ────────────────────────────────────────────────────────────

/// A user-managed LLM provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderConfig {
    /// The provider's id: the slug of its name (the desktop's Settings page
    /// derives it from the name — a name commit re-identifies the provider,
    /// remapping the `default_model` / `default_thinking_levels` references
    /// old → new; a blank name keeps the current id).
    pub id: String,
    pub name: String,
    /// Normalized base URL (…/v1).
    pub base_url: String,
    /// Plaintext (empty = local gateway, no key).
    pub api_key: String,
    /// The wire API (ADR 0024): `"openai-completions"` (the default — a
    /// pre-feature file parses to it, no migration) / `"anthropic-messages"`
    /// / `"openai-responses"` / `"litellm"` (discovery via `GET /model/info`,
    /// wire = openai-completions — ADR 0026).
    #[serde(default = "default_provider_api")]
    pub api: String,
    /// The key-management page URL (set by the known-providers picker — the
    /// row's "Get key" link; `None` for a hand-typed provider).
    #[serde(default)]
    pub key_url: Option<String>,
    /// The provider's auth mode (issue #125): `"api_key"` (the default — a
    /// pre-feature file parses to it) | `"oauth"` | `"oidc"` | `"gcloud_adc"`.
    /// A STRING, not an enum, matching `api`'s permissive-boundary style;
    /// validate at use, not at parse. Additive: pre-feature files parse
    /// untouched via the serde default.
    #[serde(default = "default_provider_auth_mode")]
    pub auth_mode: String,
    /// Custom per-provider request headers (issue #129), written with
    /// environment templates: `$VAR` and `${VAR}` expand against the process
    /// environment SINGLE-PASS at resolve time (never recursively). Examples:
    /// `"CF-Access-Client-Id": "$CLOUDFLARE_SERVICE_TOKEN_ID"` — enterprise
    /// proxies (Cloudflare Access, …) authenticate this way. `None` = no
    /// custom headers. Additive: a pre-feature file parses to `None`.
    #[serde(default)]
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// Explicitly allow plain `http://` provider base URLs (the default of
    /// `false` refuses any non-loopback `http://` base with a descriptive
    /// error). Loopback (`127.0.0.0/8`, `::1`) is ALWAYS allowed (ADR 0057 —
    /// local dev servers are essential).
    #[serde(default)]
    pub allow_insecure_http: bool,
    /// ADC: read the Google Application Default Credentials file and exchange
    /// the refresh token natively (the `gcloud_adc` flow, issue #128). Also
    /// enabled by `auth_mode: "gcloud_adc"`, and overridable by the
    /// `LITELLM_GCLOUD_TOKEN_AUTH` env var — which the RESOLVE arm reads, never
    /// this module (an env read here would cache at parse time and make the
    /// toggle untestable; see the desktop's `provider_auth::gcloud_token_auth_enabled`).
    /// Additive: a pre-feature file parses to `false`, so no existing row
    /// starts reading the user's ADC file on upgrade.
    #[serde(default)]
    pub use_gcloud_token_auth: bool,
    /// Direct OIDC PKCE login against an external identity provider (the
    /// `oidc` flow, issue #127). The validated `id_token` becomes the Bearer
    /// token sent to the LiteLLM proxy.
    ///
    /// Additive: the field is `#[serde(default)]`, so a pre-feature provider
    /// parses to `None` and keeps its existing behaviour. A PRESENT but
    /// malformed block (missing/blank `issuer`/`clientId`, wrong types) FAILS
    /// the whole `Settings` document parse and lands in `load_settings`'s
    /// existing corrupt-file fallback (`Settings::default()` + the corrupt-file
    /// log) — the same whole-document behaviour as any other malformed field
    /// today. There is deliberately NO degrade-to-`None`-and-keep-parsing path:
    /// silently dropping a user's configured login is worse than the honest,
    /// pre-existing fallback.
    #[serde(default)]
    pub oidc: Option<OidcConfig>,
}

/// The OIDC identity-provider configuration (the `oidc` member on a provider
/// row, issue #127).
///
/// `issuer` and `client_id` are REQUIRED (no `#[serde(default)]`): a present
/// `oidc` block that omits them fails the document parse (see the field doc on
/// [`ProviderConfig::oidc`] for the whole-document rule). `scope` and
/// `redirect_ports` are `#[serde(default)]` (absent → `None` / the empty list,
/// and the flow then asks for `openid` alone on one ephemeral callback port).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OidcConfig {
    /// The IdP issuer — the base for
    /// `GET {issuer}/.well-known/openid-configuration`.
    pub issuer: String,
    /// The PRE-REGISTERED public client id (T6 does no dynamic registration):
    /// the `client_id` in the authorize request and the value the `aud`/`azp`
    /// claims are checked against.
    pub client_id: String,
    /// The requested scope. MUST include `openid`; the flow appends `openid`
    /// when absent. `None` → `openid` alone.
    #[serde(default)]
    pub scope: Option<String>,
    /// The sequential callback-port list (corporate IdPs pin redirect URIs to a
    /// fixed port allowlist, so the loopback server tries these in order).
    /// Empty/missing → ONE ephemeral port.
    #[serde(default)]
    pub redirect_ports: Vec<u16>,
}

fn default_provider_api() -> String {
    "openai-completions".to_string()
}

fn default_provider_auth_mode() -> String {
    "api_key".to_string()
}

// ── `AgentOverrideFields` ───────────────────────────────────────────────────────

/// (ADR 0066 / 0067) One agent's override — the VALUE of
/// `Settings::agent_overrides`.
///
/// **Field ABSENCE is the only meaning of "this override does not speak"**
/// (ADR 0067): an unset field is absent from the document
/// (`skip_serializing_if = "Option::is_none"`), never `null` — a `"model":
/// null` would be a third state for the resolver to reconcile, and the whole
/// point of the rule is that the UI's "clear this override" and the
/// resolver's "skip this layer" are literally the same fact. A `tools` list
/// that is EMPTY also speaks not (ADR 0067, enforced in
/// `launch::resolve_agent_launch`), so no override can zero out a child.
///
/// Derived `Default` is safe HERE and only because every field is an
/// `Option` (all-absent = "says nothing") — unlike `FontSettings`, whose
/// hand-written `Default` exists because a derived one would produce
/// `size_px: 0`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AgentOverrideFields {
    /// The model ref (`"provider/id"`, or a hand-edited
    /// `"provider/id:<level>"` — the `:<level>` suffix is a thinking-level
    /// candidate handled downstream, exactly as in ADR 0023).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// A thinking level. Honoured only when the model that actually runs
    /// advertises it (`launch::resolve_agent_launch` demotes it otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    /// The tool allowlist. An EMPTY list is ABSENT (ADR 0067).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<String>>,
}

// ── `Settings` + `FontSettings` (T5/S5b, the S5b RULING's BLOCKER 1) ─────────
//
// These two are the LAST two value types the desktop's `config.rs` owned, and
// they moved for ONE production reason: `harness/launch.rs` names `Settings`
// outside any `#[cfg(test)]` block (`override_layer` takes `Option<&Settings>`,
// `packaged_settings` builds one), and `harness` moved into this crate in S5b.
// A crate file cannot name a desktop type — that edge IS the package cycle T5
// exists to sever — so the TYPE had to come here.
//
// **`FontSettings` moves with it because it is a `pub` FIELD TYPE of
// `Settings`** (`Settings::font`). A `pub` field must be at least as visible as
// the struct that exposes it; leaving it desktop would have made a crate type
// expose a desktop type through a field — the same failure `ProviderConfig.oidc`
// forced S4 to fix one level deeper. It is pure (`u32` + two `Option<String>`s)
// and no moving code names it.
//
// ## MOVING THE TYPE DID NOT MOVE THE DISK
//
// This is the ruling's sentence and the reason `boundary_pins` guards it:
// `load_settings` and every settings read/write STAY DESKTOP
// (`src-tauri/src/config.rs`). The crate gets the SHAPE, never the file. The
// design this serves is the one T3 already built — settings live in both places
// and the desktop DRIVES changes to the harness, which is exactly
// [`crate::worker_types::DesktopConfig`] pushed through
// `AgentLoop::set_desktop_config`. Nothing here opens, reads, or writes a file,
// and `boundary_pins::no_code_reads_the_desktop_settings_file` stays green
// UNCHANGED — if it reddens, the boundary was crossed.
//
// The four `default_*` functions travel with the struct because a
// `#[serde(default = "…")]` attribute names them BY PATH and the attribute is
// compiled where the struct is.

/// Font settings for the UI.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FontSettings {
    /// UI font size in px (default 14).
    #[serde(default = "default_font_size")]
    pub size_px: u32,
    /// `None` = the design system's pinned sans stack (the app default —
    /// the `index.css` stack resolves to `"Noto Sans"` first, with the
    /// system tail as the offline fallback).
    #[serde(default)]
    pub ui_family: Option<String>,
    /// `None` = the design system's pinned mono stack (the app default —
    /// the `index.css` stack resolves to `"Fira Code"` first, with the
    /// system tail as the offline fallback).
    #[serde(default)]
    pub code_family: Option<String>,
}

fn default_font_size() -> u32 {
    14
}

fn default_theme() -> String {
    "dark".to_string()
}

/// The serde missing-field default for a `true`-by-default bool (the shape of
/// `default_theme` / `default_font_size`: a non-`false` default needs a
/// named function, `#[serde(default)]` alone would mean `false`).
fn default_true() -> bool {
    true
}

/// The serde missing-field default for `attention_alert_delay_secs`: 15 s —
/// half of pi-archimedes' 30 s terminal default, affordable here because the
/// desktop also gates the alert on window focus (ADR 0038). SECONDS, not
/// millis: the value crosses the IPC boundary as `delaySecs`.
fn default_attention_delay_secs() -> u64 {
    15
}

/// The serde missing-field default for `subagent_dead_bound_secs`: 1800 s (the
/// 30 minutes the ADR 0055 **Dead bound** shipped with). SECONDS, not millis —
/// the same unit rule as `attention_alert_delay_secs`.
fn default_subagent_dead_bound_secs() -> u64 {
    1800
}

/// The persisted app settings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    /// `"system" | "dark" | "light"` (default `"dark"`).
    #[serde(default = "default_theme")]
    pub theme: String,
    /// (ADR 0027) The color scheme: `"zai" | "dracula"`. `None` = `"zai"` (a
    /// pre-feature file — `#[serde(default)]`, no migration). Orthogonal to
    /// `theme` (the light/dark mode): we ship Dracula's dark variant only (a
    /// scope decision), so `dracula` PINS the app dark and the mode is ignored
    /// — the spec's light reading, Alucard Classic, is deferred, not impossible.
    #[serde(default)]
    pub palette: Option<String>,
    /// Free-form pane layout state (owned by the frontend).
    #[serde(default)]
    pub pane_layout: Value,
    /// Whether to trust new Spaces by default.
    #[serde(default)]
    pub default_trust_new_spaces: bool,
    /// Whether the composer's `#` MCP-server mention is live (ADR 0031's
    /// follow-up). `#[serde(default)]` = `false`, so a pre-feature
    /// `settings.json` ships `#` OFF: `#` is the one mention prefix that
    /// collides with pasted text (`#include`, `#123`, `#hashtag` all match
    /// the token grammar), so it is opt-in while `$` and `@` are always on.
    /// Gates the TRIGGER only — the display side still parses a persisted
    /// `<mcp>` block, and the MCP catalog command stays available.
    #[serde(default)]
    pub mcp_mentions_enabled: bool,
    /// (ADR 0030) The per-direction file-access policies. `#[serde(default)]`
    /// — a pre-feature file parses to `FilePolicy::default()` (all `Allow`),
    /// so no migration runs. NOTE: that default is WIDER than the pre-feature
    /// hardcoded behavior — out of the box a session reads and writes anywhere
    /// and runs `bash` with no prompt, including in an untrusted Space (this
    /// makes ADR 0010's prompt opt-in). Intentional; see ADR 0030 Deviation 2.
    #[serde(default)]
    pub file_policy: FilePolicy,
    /// The web tools' configuration (ADR-less; see `docs/roadmap/web-search-and-fetch.md`).
    /// `#[serde(default)]` — a pre-feature file parses to `WebSettings::default()`
    /// (all empty ⇒ only the keyless `duckduckgo` Search provider is available).
    #[serde(default)]
    pub web: WebSettings,
    /// `"provider/id"`; `None` = system default.
    #[serde(default)]
    pub default_model: Option<String>,
    /// The per-app thinking-level seed (the native session start's last
    /// rung: remembered > stored > this > `None`); `None` = the model's
    /// own default. `#[serde(default)]` — a pre-feature file parses to
    /// `None` (no migration needed).
    #[serde(default)]
    pub default_thinking_level: Option<String>,
    /// The harness-level tool filter: `[]` = all tools enabled (the native
    /// `AgentLoop` maps an empty list to `None` = all). `#[serde(default)]`
    /// — a pre-feature file parses to `[]` (no migration needed).
    #[serde(default)]
    pub enabled_tools: Vec<String>,
    /// User-managed providers (default `[]`).
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
    /// (ADR 0019) The user-managed MCP servers: `name → entry` in pi's
    /// `mcpServers` entry shape VERBATIM (an entry copy-pastes between the
    /// two files). Merged into the harness's effective set at global
    /// precedence (project > desktop > pi-global). `#[serde(default)]` —
    /// a pre-feature file parses to `{}` (no migration).
    #[serde(default)]
    pub mcp_servers: HashMap<String, Value>,
    /// Font settings.
    #[serde(default)]
    pub font: FontSettings,
    /// (ADR 0015) The per-model remembered thinking level: the composed
    /// model key (`"<provider>/<id>"`) → the last EXPLICITLY chosen level.
    /// `#[serde(default)]` — a pre-feature file parses to an empty map.
    #[serde(default)]
    pub default_thinking_levels: HashMap<String, String>,
    /// (ADR 0023 — RETIRED by ADR 0066) The per-agent subagent model map:
    /// agent name → model key. READ-ONLY from here on — `load_settings` folds
    /// its values into [`Settings::agent_overrides`] and clears it, and
    /// `skip_serializing` (UNCONDITIONAL, not `skip_serializing_if =
    /// "HashMap::is_empty"`) is what makes the dead key disappear from the
    /// file on the next save, permanently. The two spellings are equivalent
    /// for anything reaching a save through `load_settings` (the fold clears
    /// the map); the unconditional form also survives a writer that builds a
    /// `Settings` literal by hand, which is the one path where a retired key
    /// could leak back onto the wire.
    ///
    /// Honouring the legacy key on READ is what makes the upgrade a
    /// non-loss: the models a user pinned in Settings keep applying.
    #[serde(default, skip_serializing)]
    pub subagent_models: HashMap<String, String>,
    /// (ADR 0066) Global Agent overrides keyed by agent NAME — the one
    /// override layer between the explicit `subagent` params and an Agent
    /// definition's frontmatter (ADR 0067's chain: explicit > override >
    /// frontmatter > parent). There is NO per-Space map: the Space decides
    /// which definitions get listed, never which overrides apply (ADR 0066's
    /// *Deferred: per-Space* is the only door back to a second map, and it
    /// re-opens additively).
    ///
    /// Serialized camelCase like every other key in this file —
    /// `agentOverrides: { "scout": { "model": … } }` — and matched
    /// CASE-INSENSITIVELY by name, exactly as ADR 0023's map was (the stored
    /// key is never rewritten; the UI writes `def.name` verbatim).
    ///
    /// `#[serde(default)]` and deliberately **NOT** `skip_serializing_if`:
    /// every map field in this struct serializes an EMPTY map as `"key": {}`
    /// (`mcpServers`, `defaultThinkingLevels`), and the TypeScript
    /// `AppSettings` declares each of them required. Dropping the key when
    /// empty would hand the renderer `undefined` for a required field and make
    /// the Agents modal throw for every user who has never pinned anything.
    /// Pinned by
    /// `agent_overrides_default_to_an_empty_map_that_still_serializes`.
    #[serde(default)]
    pub agent_overrides: HashMap<String, AgentOverrideFields>,
    /// The working-indicator spinner style (the `braille-loader` variant name,
    /// e.g. `"typing"` / `"pendulum"`): the animation the chat's top working
    /// indicator runs while the agent is busy. `None` = the frontend's
    /// `typing` default (a pre-feature file — `#[serde(default)]`, no
    /// migration).
    #[serde(default)]
    pub spinner_style: Option<String>,
    /// The **Transcript**'s empty-state animation: `"off"` / `"static"` /
    /// `"animated"`. `None` = the frontend's `"animated"` default (a
    /// pre-feature file — `#[serde(default)]`, no migration). `prefers-reduced-motion`
    /// can pin `"animated"` to `"static"` but can never un-`off` a deliberate
    /// `"off"` (ADR 0070).
    #[serde(default)]
    pub empty_state_animation: Option<String>,
    /// Session naming (ported from pi-archimedes' `session-name`): the master
    /// switch for the post-first-exchange title request. `#[serde(default =
    /// "default_true")]` — pi ships naming ON, so a pre-feature file reads as
    /// enabled (unlike the `#[serde(default)]` bools above, whose default is
    /// `false`).
    #[serde(default = "default_true")]
    pub session_naming_enabled: bool,
    /// The model used for title generation: the composed key
    /// (`"provider/id"`, or a hand-edited `"provider/id:<level>"`). `None` /
    /// blank = the session's own model (pi's `model` setting).
    #[serde(default)]
    pub session_naming_model: Option<String>,
    /// The thinking level for the title request. Honoured ONLY when the chosen
    /// model advertises it; `None` = omit the `reasoning_effort` key (the
    /// default — `ModelOptions.reasoning_effort: None` already means "no
    /// thinking" here, so pi's `minimal` default would ADD cost). JSON-only:
    /// there is no settings row for it.
    #[serde(default)]
    pub session_naming_reasoning: Option<String>,
    /// Attention alerts (ADR 0038/0039): the master switch for the OS
    /// notification raised when a session has been paused on the user
    /// (finished, or waiting on a gate) for longer than
    /// `attention_alert_delay_secs`. `#[serde(default = "default_true")]` — the
    /// feature ships ON, so a pre-feature file reads as enabled (the
    /// `session_naming_enabled` pattern, not the `#[serde(default)]`-false one).
    #[serde(default = "default_true")]
    pub attention_alerts_enabled: bool,
    /// The delay between an attention trigger and the alert, in SECONDS.
    /// `#[serde(default = "default_attention_delay_secs")]` = 15 — half of
    /// pi-archimedes' 30 s terminal default, affordable here because the desktop
    /// also gates on window focus. There is no clamping LAYER on purpose: the
    /// Settings select is the only producer, and the only bound is the
    /// sanity ceiling in `commands::attention`.
    #[serde(default = "default_attention_delay_secs")]
    pub attention_alert_delay_secs: u64,
    /// The ADR 0055 **Dead bound**: SECONDS a **Subagent session** may stay
    /// silent with no tool execution outstanding before it is reaped as Dead.
    /// It bounds UNEXPLAINED SILENCE **as the Supervisor observes it** (the gap
    /// between frames the pump actually read — a parent starved of CPU can
    /// observe a gap a live child never stopped producing into), never the length
    /// of the delegation — an **Active Subagent** (producing frames, or holding an
    /// outstanding `tool_call_id`) has no timeout of any kind, so no value here
    /// can cut a working child. 1800 s: the provider layer already self-bounds a
    /// stalled stream at a 5-minute idle `read_timeout`, so silence past twice
    /// that with nothing in flight is a wedged Worker, not a slow model.
    /// `settings.json`-only (no Settings-UI control, deliberately — the
    /// `session_naming_reasoning` precedent).
    #[serde(default = "default_subagent_dead_bound_secs")]
    pub subagent_dead_bound_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            theme: "dark".to_string(),
            palette: None,
            pane_layout: Value::Object(Default::default()),
            default_trust_new_spaces: false,
            mcp_mentions_enabled: false,
            file_policy: FilePolicy::default(),
            web: WebSettings::default(),
            default_model: None,
            default_thinking_level: None,
            enabled_tools: Vec::new(),
            providers: Vec::new(),
            mcp_servers: HashMap::new(),
            font: FontSettings::default(),
            default_thinking_levels: HashMap::new(),
            subagent_models: HashMap::new(),
            agent_overrides: HashMap::new(),
            spinner_style: None,
            empty_state_animation: None,
            session_naming_enabled: true,
            session_naming_model: None,
            session_naming_reasoning: None,
            attention_alerts_enabled: true,
            attention_alert_delay_secs: default_attention_delay_secs(),
            subagent_dead_bound_secs: default_subagent_dead_bound_secs(),
        }
    }
}

/// HAND-WRITTEN (do NOT `#[derive(Default)]` — a derived `Default` would
/// give `size_px: 0`, a second "default" that diverges from the serde
/// missing-field default of 14):
impl Default for FontSettings {
    fn default() -> Self {
        Self {
            size_px: default_font_size(),
            // The app default (a fresh settings file): Noto Sans for the
            // UI, Fira Code for code — both the `index.html` Google Fonts
            // families (the quoted CSS family names the frontend's
            // `applySettingsFont` splices into the `--font-sans` /
            // `--font-mono` stacks).
            ui_family: Some("\"Noto Sans\"".to_string()),
            code_family: Some("\"Fira Code\"".to_string()),
        }
    }
}

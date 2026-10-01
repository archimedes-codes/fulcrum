//! The model catalog (native-agent-harness Task 5, ADR 0012: v1 adds no
//! new user-facing config surface). The desktop **seeds** its model
//! catalog + provider auth from the user's existing pi config files
//! (`~/.pi/agent/settings.json` / `auth.json` / `models-store.json`)
//! so the user's existing pi setup works with zero reconfiguration.
//!
//! Seeding is **best-effort**: a missing/unparseable file degrades to
//! "no seeded model" (a logged warning), never a crash. The catalog is
//! the source for the model picker (Task 7) and the [`Provider`]
//! construction (Task 4: a [`Model`]'s `provider` + `base_url` +
//! `api_key` → an `OpenAiCompatibleProvider`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A model with no metadata in `models-store.json` gets this window
/// (best-effort). Also the `effective_catalog` fallback for a user
/// provider's discovered model (ADR 0014 — a user model has no static
/// metadata).
pub const DEFAULT_CONTEXT_WINDOW: u32 = 128000;
/// The `Compactor` (Task 6) thresholds when `settings.json` has no
/// `compaction` block (pi's own defaults).
const DEFAULT_RESERVE_TOKENS: u32 = 16384;
const DEFAULT_KEEP_RECENT_TOKENS: u32 = 20000;

/// A model in the catalog (the model picker's entry, Task 7).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Model {
    /// The bare model id (e.g. `Qwen/Qwen3.8-27B` — ids may contain
    /// `/`). The catalog/config-option key is
    /// `format!("{provider}/{id}")`.
    pub id: String,
    /// The provider (e.g. `tama`).
    pub provider: String,
    /// The API base (e.g. `https://tama.wizards.town/v1` — the
    /// `OpenAiCompatibleProvider` appends `/chat/completions`).
    pub base_url: String,
    /// The `Authorization: Bearer` key (empty when the provider needs
    /// none — a local gateway — or is absent from `auth.json`).
    pub api_key: String,
    pub context_window: u32,
    /// USD per 1M input tokens.
    pub cost_per_mtok_in: f64,
    /// USD per 1M output tokens.
    pub cost_per_mtok_out: f64,
    /// `true` when the model speaks the OpenAI-compatible
    /// chat-completions API (`api == "openai-completions"` — the only
    /// tool-carrying wire in v1, ADR 0012).
    pub supports_tools: bool,
    /// `reasoning` + a non-empty `thinkingLevelMap` (non-null values).
    pub supports_thinking: bool,
    /// The `thinkingLevelMap` keys with a non-null value (e.g.
    /// `["low", "medium", "xhigh"]`).
    pub thinking_levels: Vec<String>,
    /// The wire API discriminator (`"openai-completions"` /
    /// `"google-generative-ai"` / …; `None` for a metadata-less model —
    /// an unknown API is not OpenAI-compatible).
    pub api: Option<String>,
}

/// The `Compactor`'s (Task 6) thresholds — seeded from `settings.json`'s
/// `compaction` (`{ enabled, reserveTokens, keepRecentTokens }`),
/// defaulting `true` / `16384` / `20000` when absent (compaction is ON
/// by default — pi's own default; an explicit `compaction.enabled:
/// false` disables it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionConfig {
    pub enabled: bool,
    pub reserve_tokens: u32,
    pub keep_recent_tokens: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            reserve_tokens: DEFAULT_RESERVE_TOKENS,
            keep_recent_tokens: DEFAULT_KEEP_RECENT_TOKENS,
        }
    }
}

/// The desktop's model catalog (seeded from the pi config files, ADR
/// 0012).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelCatalog {
    /// The enabled models (one per `settings.json` `enabledModels`
    /// entry).
    pub models: Vec<Model>,
    /// The default model's composed key (e.g.
    /// `tama/Qwen/Qwen3.8-27B`), or `None` when the settings' default
    /// (`defaultProvider` + `defaultModel`) matches no `enabledModels`
    /// entry. Task 7's built-in `harness.default_model` (`None`) is
    /// resolved from this at session start.
    pub default_model: Option<String>,
    /// The `Compactor` thresholds (Task 6) — seeded from
    /// `settings.json`'s `compaction` (compaction ON by default, `true`
    /// / `16384` / `20000` when absent).
    pub compaction: CompactionConfig,
}

impl ModelCatalog {
    /// Look up a model by its bare id (e.g. `Qwen/Qwen3.8-27B`).
    pub fn get(&self, id: &str) -> Option<&Model> {
        self.models.iter().find(|m| m.id == id)
    }

    /// All the enabled models.
    pub fn all(&self) -> &[Model] {
        &self.models
    }

    /// The v1-selectable set (ADR 0012): the tool-supporting,
    /// OpenAI-compatible models (`api == "openai-completions"`).
    pub fn openai_compatible(&self) -> Vec<&Model> {
        self.models
            .iter()
            .filter(|m| m.supports_tools && m.api.as_deref() == Some("openai-completions"))
            .collect()
    }

    /// Seed the catalog from the user's pi config files (read-only —
    /// ADR 0012: the desktop NEVER writes to pi's config). Best-effort:
    /// a missing/unparseable file degrades (a logged warning), never a
    /// crash.
    ///
    /// `pi_dir` is `~/.pi/agent` in production ([`seed_from_pi_config`])
    /// and a temp dir in tests (pointing at a real `HOME` would be
    /// racy).
    pub fn seed_from(pi_dir: &Path) -> ModelCatalog {
        // The production entry point: the `api_key` is resolved from the
        // process environment (env-first) then `auth.json`.
        Self::seed_from_env(pi_dir, &|name: &str| std::env::var(name).ok())
    }

    /// The testable core of [`seed_from`]: seed from the pi config files,
    /// resolving each provider's `api_key` via `env_lookup` (env-first —
    /// `<PROVIDER>_API_KEY` → `_TOKEN` → `_KEY` — then `auth.json`). In
    /// production `env_lookup` is `std::env::var`; in tests a controlled
    /// table (so the tests never touch the real environment).
    pub fn seed_from_env(
        pi_dir: &Path,
        env_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> ModelCatalog {
        // `settings.json` is the spine (the `enabledModels` list): a
        // missing/unparseable file → an empty catalog (no models, no
        // default, the compaction defaults).
        let Some(settings) = read_json::<Settings>(&pi_dir.join("settings.json")) else {
            return ModelCatalog::default();
        };

        // `auth.json` / `models-store.json` degrade independently (a
        // missing file → empty keys / default metadata).
        let auth: HashMap<String, AuthEntry> =
            read_json(&pi_dir.join("auth.json")).unwrap_or_default();
        let store: HashMap<String, ProviderStore> =
            read_json(&pi_dir.join("models-store.json")).unwrap_or_default();

        let models = settings
            .enabled_models
            .iter()
            .filter_map(|entry| {
                let (provider, id) = split_enabled_entry(entry)?;
                let meta = store
                    .get(provider)
                    .and_then(|p| p.models.iter().find(|m| m.id.as_deref() == Some(id)));
                Some(build_model(provider, id, meta, &auth, pi_dir, env_lookup))
            })
            .collect();

        ModelCatalog {
            models,
            default_model: resolve_default(&settings),
            compaction: settings.compaction.map(Into::into).unwrap_or_default(),
        }
    }
}

/// Merge the seeded catalog with the user-provider models (ADR 0014).
/// `user_models` are the discovered models; `shadowed_provider_ids` are
/// the ids of EVERY user provider configured in `settings.json` (regardless
/// of whether its discovery succeeded — a provider that discovered 0 models
/// STILL shadows the seeded models for its id: user-wins-on-clash, even
/// on failure). For every id in `shadowed_provider_ids` the seeded models
/// for that id are REPLACED by the user models with that id (possibly none).
/// `default_model` is the seeded default unless it belongs to a shadowed
/// provider (then `None` — the caller's resolution chain degrades).
pub fn merge_catalog(
    seeded: &ModelCatalog,
    user_models: &[Model],
    shadowed_provider_ids: &[String],
) -> ModelCatalog {
    let kept_seeded: Vec<Model> = seeded
        .models
        .iter()
        .filter(|m| !shadowed_provider_ids.contains(&m.provider))
        .cloned()
        .collect();
    let mut models = kept_seeded;
    models.extend_from_slice(user_models);
    // The seeded default is kept only when it still EXISTS in the merged
    // result: a shadowed provider's default model was replaced (possibly by
    // nothing) → `None` (the caller's resolution chain degrades). A
    // default pointing at a model absent from `seeded.models` (a stale
    // key) also degrades to `None` (it is not in the merged result either).
    let default_model = seeded.default_model.clone().filter(|key| {
        models
            .iter()
            .any(|m| format!("{}/{}", m.provider, m.id) == key.as_str())
    });
    ModelCatalog {
        models,
        default_model,
        compaction: seeded.compaction,
    }
}

/// Seed from the user's pi config dir (the production entry point — a
/// thin wrapper over [`ModelCatalog::seed_from`] so the seeding logic
/// stays testable on a temp dir).
pub fn seed_from_pi_config() -> ModelCatalog {
    let Some(home) = home_dir() else {
        eprintln!("harness: no $HOME / %USERPROFILE — the model catalog is empty");
        return ModelCatalog::default();
    };
    ModelCatalog::seed_from(&home.join(".pi/agent"))
}

/// `$HOME` (Unix) / `%USERPROFILE` (Windows) — no `dirs` dependency.
fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var("USERPROFILE").ok().map(PathBuf::from)
    }
    #[cfg(not(windows))]
    {
        std::env::var("HOME").ok().map(PathBuf::from)
    }
}

/// Resolve a provider's `api_key` (the `Authorization: Bearer` key).
///
/// **Env first** (matching `pi-provider-litellm`'s ambient auth): for
/// provider `p` (uppercased `P`), try `P_API_KEY` → `P_TOKEN` → `P_KEY`
/// and use the first that is set and **non-empty** (a `tama` gateway is
/// keyed by `TAMA_TOKEN`; `eurouter`/`openrouter`/`google` by
/// `<NAME>_API_KEY`). Env-first means a stored key can be overridden
/// without editing `auth.json`. Then `auth.json[provider].key`. A provider
/// with neither (a local gateway needing no key) → empty.
fn resolve_api_key(
    provider: &str,
    auth: &HashMap<String, AuthEntry>,
    env_lookup: &dyn Fn(&str) -> Option<String>,
) -> String {
    let upper = provider.to_ascii_uppercase();
    for suffix in ["_API_KEY", "_TOKEN", "_KEY"] {
        if let Some(v) = env_lookup(&format!("{upper}{suffix}")) {
            if !v.is_empty() {
                return v;
            }
        }
    }
    auth.get(provider)
        .and_then(|a| a.key.clone())
        .unwrap_or_default()
}

/// Fresh model metadata from a live `GET /v1/models` (the OpenAI endpoint
/// "supplies everything" — the `pi-provider-litellm` `fetchModels` pattern).
/// All fields `Option`: a field absent in the response keeps the static
/// (`models-store.json`) value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiscoveredMeta {
    pub context_window: Option<u32>,
    pub thinking_levels: Option<Vec<String>>,
    pub supports_thinking: Option<bool>,
}

/// The per-provider discovery cache entry: `attempted` (a failed /
/// unreachable endpoint is NOT re-fetched every session) + the discovered
/// models (model id → fresh metadata; empty when the fetch failed or the
/// model is absent from the response).
#[derive(Debug, Clone, Default)]
pub struct ProviderDiscovery {
    pub attempted: bool,
    pub models: HashMap<String, DiscoveredMeta>,
}

/// The `GET /v1/models` response (standard OpenAI discovery — `tama` and
/// OpenAI-compatible gateways expose it; the `data` entries carry `id`,
/// `max_model_len` (the context window), `reasoningLevels`, and
/// `supportsReasoningEffort`).
#[derive(Debug, Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    data: Vec<DiscoveredModel>,
}

#[derive(Debug, Deserialize)]
struct DiscoveredModel {
    id: String,
    #[serde(rename = "max_model_len")]
    max_model_len: Option<u32>,
    #[serde(rename = "reasoningLevels")]
    reasoning_levels: Option<Vec<String>>,
    #[serde(rename = "supportsReasoningEffort")]
    supports_reasoning_effort: Option<bool>,
}

/// Query a provider's `GET /v1/models` for fresh model metadata (bounded,
/// best-effort). `base_url` is `.../v1` (the endpoint is `{base_url}/models`).
/// A non-2xx / network error → `Err` (the caller degrades to the static
/// `models-store.json` metadata). The `api_key` is sent as `Authorization:
/// Bearer` when non-empty (a local gateway needing no key → no header).
pub async fn discover_models(
    base_url: &str,
    api_key: &str,
) -> Result<HashMap<String, DiscoveredMeta>, String> {
    let url = format!("{}/models", base_url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .read_timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let mut req = client.get(&url);
    if !api_key.is_empty() {
        req = req.bearer_auth(api_key);
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("status {status}"));
    }
    let body: ModelsResponse = resp.json().await.map_err(|e| e.to_string())?;
    Ok(body
        .data
        .into_iter()
        .map(|m| {
            (
                m.id,
                DiscoveredMeta {
                    context_window: m.max_model_len,
                    thinking_levels: m.reasoning_levels,
                    supports_thinking: m.supports_reasoning_effort,
                },
            )
        })
        .collect())
}

/// Build one `Model` for an `enabledModels` entry: the metadata (from
/// `models-store.json[provider].models[]` matched by `id` — absent →
/// the defaults), the `api_key` (env-first via [`resolve_api_key`] —
/// `auth.json` fallback — absent → empty), and the `base_url` (the model
/// entry's `baseUrl` → the `pi-provider-<provider>.json` fallback `baseURL`
/// normalized with a `/v1` suffix → a provider-specific default).
fn build_model(
    provider: &str,
    id: &str,
    meta: Option<&StoreModel>,
    auth: &HashMap<String, AuthEntry>,
    pi_dir: &Path,
    env_lookup: &dyn Fn(&str) -> Option<String>,
) -> Model {
    // `api == "openai-completions"` is the OpenAI-compatible
    // discriminator (v1 is OpenAI-compatible only, ADR 0012). A
    // metadata-less model defaults to tool-supporting (best-effort) but
    // an unknown `api` is NOT OpenAI-compatible (it is excluded from
    // `openai_compatible()`).
    let (supports_tools, api) = match meta {
        Some(m) => {
            let openai = m.api.as_deref() == Some("openai-completions");
            (openai, m.api.clone())
        }
        None => (true, None),
    };

    // `reasoning` + the NON-NULL `thinkingLevelMap` keys.
    let (supports_thinking, thinking_levels) =
        match meta.and_then(|m| m.thinking_level_map.as_ref()) {
            Some(map) => {
                let levels: Vec<String> = map
                    .iter()
                    .filter(|(_, v)| v.is_some())
                    .map(|(k, _)| k.clone())
                    .collect();
                let reasoning = meta.and_then(|m| m.reasoning).unwrap_or(false);
                (reasoning && !levels.is_empty(), levels)
            }
            None => (false, Vec::new()),
        };

    let base_url = meta
        .and_then(|m| m.base_url.clone())
        .or_else(|| {
            let path = pi_dir.join(format!("pi-provider-{provider}.json"));
            read_json::<PiProviderFile>(&path).and_then(|f| f.base_url)
        })
        .map(|b| normalize_base_url(&b))
        .or_else(|| provider_default_base_url(provider))
        .unwrap_or_default();

    Model {
        id: id.to_string(),
        provider: provider.to_string(),
        base_url,
        // Env-first (a `TAMA_TOKEN`-style ambient key), then `auth.json`;
        // a provider with neither (a local gateway needing no key) → empty.
        api_key: resolve_api_key(provider, auth, env_lookup),
        context_window: meta
            .and_then(|m| m.context_window)
            .unwrap_or(DEFAULT_CONTEXT_WINDOW),
        cost_per_mtok_in: meta
            .and_then(|m| m.cost.as_ref())
            .and_then(|c| c.input)
            .unwrap_or(0.0),
        cost_per_mtok_out: meta
            .and_then(|m| m.cost.as_ref())
            .and_then(|c| c.output)
            .unwrap_or(0.0),
        supports_tools,
        supports_thinking,
        thinking_levels,
        api,
    }
}

/// The default: the BARE `defaultModel` + `defaultProvider` composed
/// into the `"<provider>/<id>"` key, `None` when it matches no
/// `enabledModels` entry.
fn resolve_default(settings: &Settings) -> Option<String> {
    let (Some(provider), Some(model)) = (&settings.default_provider, &settings.default_model)
    else {
        return None;
    };
    let key = format!("{provider}/{model}");
    settings
        .enabled_models
        .iter()
        .any(|e| e == &key)
        .then_some(key)
}

/// Split an `enabledModels` entry on the FIRST `/` (model ids themselves
/// contain `/` — `tama/Qwen/Qwen3.8-27B` → provider `tama`, id
/// `Qwen/Qwen3.8-27B`). `None` when the entry has no `/` (or an empty
/// side).
fn split_enabled_entry(entry: &str) -> Option<(&str, &str)> {
    let (provider, id) = entry.split_once('/')?;
    if provider.is_empty() || id.is_empty() {
        return None;
    }
    Some((provider, id))
}

/// Normalize a base URL to the OpenAI-compatible form: a trailing
/// `/v1` (the `OpenAiCompatibleProvider` appends `/chat/completions`).
/// The model entry's `baseUrl` already carries the suffix
/// (`https://tama.wizards.town/v1`); the `pi-provider-<provider>.json`
/// fallback `baseURL` does NOT (`https://tama.wizards.town`) → append it.
fn normalize_base_url(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if url.is_empty() || url.ends_with("/v1") {
        url.to_string()
    } else {
        format!("{url}/v1")
    }
}

/// Provider-specific `base_url` defaults (the last fallback in the
/// chain) — `openrouter` is the only one with a well-known default
/// (an unknown provider → `None` → an empty `base_url`, best-effort).
fn provider_default_base_url(provider: &str) -> Option<String> {
    match provider {
        "openrouter" => Some("https://openrouter.ai/api/v1".to_string()),
        _ => None,
    }
}

/// Read + parse one JSON file (best-effort: a missing/unparseable file
/// → `None` + a logged warning, never a crash).
fn read_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("harness: cannot read {}: {e}", path.display());
            return None;
        }
    };
    match serde_json::from_str(&text) {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("harness: cannot parse {}: {e}", path.display());
            None
        }
    }
}

// ── the pi config file shapes (verified against `~/.pi/agent/`) ──────
//
// Only the seeded fields are named — serde ignores the rest.

/// `settings.json` (a bare `defaultModel` + `defaultProvider`, the
/// `enabledModels` list, and the `Compactor` thresholds under
/// `compaction`).
#[derive(Debug, Deserialize)]
struct Settings {
    #[serde(default, rename = "defaultProvider")]
    default_provider: Option<String>,
    #[serde(default, rename = "defaultModel")]
    default_model: Option<String>,
    #[serde(default, rename = "enabledModels")]
    enabled_models: Vec<String>,
    #[serde(default)]
    compaction: Option<SettingsCompaction>,
}

/// `settings.json`'s `compaction` block (`{ enabled, reserveTokens,
/// keepRecentTokens }` — `enabled` defaults `true` (pi's own default;
/// a block without the key still compacts), the numeric fields default
/// when absent).
#[derive(Debug, Deserialize)]
struct SettingsCompaction {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default, rename = "reserveTokens")]
    reserve_tokens: Option<u32>,
    #[serde(default, rename = "keepRecentTokens")]
    keep_recent_tokens: Option<u32>,
}

fn default_true() -> bool {
    true
}

impl From<SettingsCompaction> for CompactionConfig {
    fn from(c: SettingsCompaction) -> Self {
        Self {
            enabled: c.enabled,
            reserve_tokens: c.reserve_tokens.unwrap_or(DEFAULT_RESERVE_TOKENS),
            keep_recent_tokens: c.keep_recent_tokens.unwrap_or(DEFAULT_KEEP_RECENT_TOKENS),
        }
    }
}

/// `auth.json`: `provider → { key, type }` (read `.key` — NOT
/// `api_key`).
#[derive(Debug, Deserialize)]
struct AuthEntry {
    #[serde(default)]
    key: Option<String>,
}

/// `models-store.json`: `provider → { models, checkedAt, etag?,
/// lastModified? }` — the provider-level `etag`/`lastModified` are
/// OPTIONAL (a sparse `tama`-like entry has only `{ models, checkedAt }`
/// — they are not even named here, so the sparse shape parses).
#[derive(Debug, Deserialize)]
struct ProviderStore {
    #[serde(default)]
    models: Vec<StoreModel>,
}

/// A `models-store.json` model entry (`{ id, name, api, baseUrl,
/// provider, reasoning, thinkingLevelMap, input, cost, contextWindow,
/// maxTokens, compat?, inputLimits?, type? }`) — only the seeded fields
/// are named; `compat`/`inputLimits`/`type` are optional and ignored.
#[derive(Debug, Deserialize)]
struct StoreModel {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    api: Option<String>,
    #[serde(default, rename = "baseUrl")]
    base_url: Option<String>,
    #[serde(default)]
    reasoning: Option<bool>,
    #[serde(default, rename = "thinkingLevelMap")]
    thinking_level_map: Option<HashMap<String, Option<String>>>,
    #[serde(default)]
    cost: Option<ModelCost>,
    #[serde(default, rename = "contextWindow")]
    context_window: Option<u32>,
}

/// A model entry's `cost` (`{ input, output, cacheRead, cacheWrite }` —
/// USD per 1M tokens; v1 seeds input/output only).
#[derive(Debug, Deserialize)]
struct ModelCost {
    #[serde(default)]
    input: Option<f64>,
    #[serde(default)]
    output: Option<f64>,
}

/// `pi-provider-<provider>.json` (a FALLBACK only — a *different*
/// schema: top-level `baseURL` WITHOUT the `/v1` suffix, and
/// `context_length`/`tool_call`/`modalities` model entries that do not
/// map cleanly in v1 — only the `baseURL` is used).
#[derive(Debug, Deserialize)]
struct PiProviderFile {
    #[serde(default, rename = "baseURL")]
    base_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── `enabledModels` splitting ─────────────────────────────────────

    #[test]
    fn split_entry_splits_on_the_first_slash_only() {
        // Model ids themselves contain `/` — split on the FIRST only.
        assert_eq!(
            split_enabled_entry("tama/Qwen/Qwen3.8-27B"),
            Some(("tama", "Qwen/Qwen3.8-27B"))
        );
        assert_eq!(
            split_enabled_entry("openrouter/openrouter/free"),
            Some(("openrouter", "openrouter/free"))
        );
        // No `/` / empty sides → `None` (skipped, not a crash).
        assert_eq!(split_enabled_entry("just-a-model"), None);
        assert_eq!(split_enabled_entry("/no-provider"), None);
        assert_eq!(split_enabled_entry("no-model/"), None);
    }

    // ── `base_url` normalization ──────────────────────────────────────

    #[test]
    fn normalize_base_url_appends_v1_when_absent() {
        // The `pi-provider-*.json` fallback `baseURL` lacks the suffix.
        assert_eq!(
            normalize_base_url("https://tama.wizards.town"),
            "https://tama.wizards.town/v1"
        );
        // The model entry's `baseUrl` already carries it (untouched).
        assert_eq!(
            normalize_base_url("https://tama.wizards.town/v1"),
            "https://tama.wizards.town/v1"
        );
        // A trailing `/` is trimmed before the suffix is appended.
        assert_eq!(normalize_base_url("https://x.io/"), "https://x.io/v1");
    }

    // ── the provider-default fallback ─────────────────────────────────

    #[test]
    fn provider_default_base_url_is_openrouter_only() {
        assert_eq!(
            provider_default_base_url("openrouter"),
            Some("https://openrouter.ai/api/v1".to_string())
        );
        assert_eq!(provider_default_base_url("tama"), None);
    }

    // ── `ModelCatalog` accessors ──────────────────────────────────────

    fn model(id: &str, supports_tools: bool, api: Option<&str>) -> Model {
        Model {
            id: id.to_string(),
            provider: "p".to_string(),
            base_url: "https://x/v1".to_string(),
            api_key: String::new(),
            context_window: 128000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: api.map(str::to_string),
        }
    }

    #[test]
    fn get_all_and_openai_compatible_filter() {
        let catalog = ModelCatalog {
            models: vec![
                model("a", true, Some("openai-completions")),
                model("b", false, Some("google-generative-ai")),
                model("c", true, None), // metadata-less: tools OK, api unknown
            ],
            ..Default::default()
        };
        assert_eq!(catalog.all().len(), 3);
        assert!(catalog.get("a").is_some());
        assert!(catalog.get("zzz").is_none());
        // The v1-selectable set: `supports_tools` AND OpenAI-compatible.
        let openai: Vec<&str> = catalog
            .openai_compatible()
            .iter()
            .map(|m| m.id.as_str())
            .collect();
        assert_eq!(openai, vec!["a"]);
    }

    // ── the `CompactionConfig` defaults ───────────────────────────────

    #[test]
    fn compaction_config_defaults() {
        let c = CompactionConfig::default();
        // (finding 13a) Compaction is ON by default (pi's own default —
        // a settings file without a `compaction` block still compacts;
        // an explicit `compaction.enabled: false` disables it).
        assert!(c.enabled);
        assert_eq!(c.reserve_tokens, 16384);
        assert_eq!(c.keep_recent_tokens, 20000);
    }

    // ── API-key resolution (env first, then `auth.json`) ────────────────

    /// A fake `env_lookup` table for the `resolve_api_key` tests.
    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| m.get(name).cloned()
    }

    #[test]
    fn resolve_api_key_env_first_then_auth() {
        let mut auth: HashMap<String, AuthEntry> = HashMap::new();
        auth.insert(
            "p".into(),
            AuthEntry {
                key: Some("auth-key".into()),
            },
        );
        auth.insert(
            "g".into(),
            AuthEntry {
                key: Some("g-auth".into()),
            },
        );
        // `tama`: `TAMA_API_KEY` set → wins (first in the order), even
        // though `TAMA_TOKEN` is also set.
        let env = env_of(&[("TAMA_API_KEY", "env-api"), ("TAMA_TOKEN", "env-token")]);
        assert_eq!(resolve_api_key("tama", &auth, &env), "env-api");
        // `eur`: no `EUR_API_KEY`, `EUR_TOKEN` set → the token wins.
        let env = env_of(&[("EUR_TOKEN", "eur-env-token")]);
        assert_eq!(resolve_api_key("eur", &auth, &env), "eur-env-token");
        // `g`: `G_API_KEY` is EMPTY (skipped) → `G_TOKEN` wins over the
        // `auth.json` key (env-first precedence).
        let env = env_of(&[("G_API_KEY", ""), ("G_TOKEN", "g-env-token")]);
        assert_eq!(resolve_api_key("g", &auth, &env), "g-env-token");
        // `p`: no env var, `auth.json` has a key → the stored key wins.
        let env = env_of(&[]);
        assert_eq!(resolve_api_key("p", &auth, &env), "auth-key");
        // `x`: no env var, no `auth.json` entry → empty (a local gateway).
        assert_eq!(resolve_api_key("x", &auth, &env), "");
        // `k`: `K_KEY` (the last in the order) is the only one set.
        let env = env_of(&[("K_KEY", "k-key")]);
        assert_eq!(resolve_api_key("k", &auth, &env), "k-key");
    }

    // ── end-to-end: `seed_from` resolves the `api_key` from the env ────

    #[test]
    fn seed_from_resolves_the_api_key_from_the_env_when_absent_from_auth() {
        let dir = std::env::temp_dir().join(format!("catalog-seed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("settings.json"),
            r#"{ "defaultProvider": "tama", "defaultModel": "m/1",
                 "enabledModels": ["tama/m/1"] }"#,
        )
        .unwrap();
        std::fs::write(
            dir.join("models-store.json"),
            r#"{ "tama": { "models": [ { "id": "m/1",
                "api": "openai-completions",
                "baseUrl": "https://tama.wizards.town/v1" } ] } }"#,
        )
        .unwrap();
        // NO `auth.json` entry for `tama` — the key comes from the env.
        let catalog = ModelCatalog::seed_from_env(&dir, &env_of(&[("TAMA_TOKEN", "tama-env-key")]));
        assert_eq!(catalog.models.len(), 1);
        assert_eq!(catalog.models[0].api_key, "tama-env-key");
        // The base_url is still seeded from the model entry (untouched by the env).
        assert_eq!(catalog.models[0].base_url, "https://tama.wizards.town/v1");
    }

    // ── live `/v1/models` discovery (the endpoint "supplies everything") ──

    /// A raw HTTP server that serves a fixed `GET` response (the
    /// `discover_models` tests). `status` + `body` are the response.
    async fn raw_json_server(
        listener: tokio::net::TcpListener,
        status: u16,
        body: &str,
    ) -> tokio::task::JoinHandle<()> {
        // Owned (the spawned task needs `'static`; the `&str` borrow would
        // otherwise escape the function).
        let body = body.to_string();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = [0u8; 4096];
            let mut data = Vec::new();
            while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                let Ok(n) = stream.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                data.extend_from_slice(&buf[..n]);
            }
            let reason = if status == 200 { "OK" } else { "Unauthorized" };
            let resp = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        })
    }

    #[tokio::test]
    async fn discover_models_maps_the_live_metadata() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"{"data":[{"id":"m/1","max_model_len":99999,
            "reasoningLevels":["low","medium"],
            "supportsReasoningEffort":true},
            {"id":"m/2"}]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(&format!("http://{addr}/v1"), "test-key")
            .await
            .unwrap();
        // `m/1`: the live metadata is mapped (context window, thinking
        // levels, supports_thinking).
        let m1 = &models["m/1"];
        assert_eq!(m1.context_window, Some(99999));
        assert_eq!(
            m1.thinking_levels,
            Some(vec!["low".into(), "medium".into()])
        );
        assert_eq!(m1.supports_thinking, Some(true));
        // `m/2`: a bare entry (no metadata fields) → all `None` (the static
        // metadata is kept on a refresh).
        let m2 = &models["m/2"];
        assert_eq!(m2.context_window, None);
        assert_eq!(m2.thinking_levels, None);
        assert_eq!(m2.supports_thinking, None);
        server.abort();
    }

    #[tokio::test]
    async fn discover_models_errors_on_a_non_2xx() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = raw_json_server(listener, 401, r#"{"error":"auth"}"#).await;
        // A 401 (a bad/missing key) → `Err` (the caller degrades to the
        // static metadata).
        assert!(discover_models(&format!("http://{addr}/v1"), "bad-key")
            .await
            .is_err());
        server.abort();
    }

    /// (finding 13a) A settings file WITHOUT a `compaction` block seeds
    /// the default — compaction `enabled: true` (pi's behavior; a long
    /// native session compacts instead of hitting a hard context-length
    /// `Fatal`).
    #[test]
    fn a_settings_file_without_a_compaction_block_defaults_to_enabled() {
        let dir = std::env::temp_dir().join(format!("catalog-seed-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("settings.json"),
            r#"{ "defaultProvider": "p", "defaultModel": "m", "enabledModels": ["p/m"] }"#,
        )
        .unwrap();
        let catalog = ModelCatalog::seed_from(&dir);
        assert!(
            catalog.compaction.enabled,
            "compaction is ON by default (a missing `compaction` block)"
        );
        // An explicit `compaction.enabled: false` still disables it.
        std::fs::write(
            dir.join("settings.json"),
            r#"{ "defaultProvider": "p", "defaultModel": "m", "enabledModels": ["p/m"], "compaction": { "enabled": false } }"#,
        )
        .unwrap();
        let catalog = ModelCatalog::seed_from(&dir);
        assert!(
            !catalog.compaction.enabled,
            "an explicit `compaction.enabled: false` disables it"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn settings_compaction_maps_with_per_field_defaults() {
        // A partial block (the numeric fields absent) → the per-field
        // defaults.
        let c: CompactionConfig = SettingsCompaction {
            enabled: true,
            reserve_tokens: None,
            keep_recent_tokens: Some(5000),
        }
        .into();
        assert!(c.enabled);
        assert_eq!(c.reserve_tokens, 16384);
        assert_eq!(c.keep_recent_tokens, 5000);
    }

    // ── `merge_catalog` (ADR 0014: the effective catalog) ─────────────

    /// A full `Model` literal (the `model` helper above pins `provider` to
    /// `"p"` — a `provider` field needs the full literal; `Model` has no
    /// `Default` derive).
    fn full_model(id: &str, provider: &str) -> Model {
        Model {
            id: id.to_string(),
            provider: provider.to_string(),
            base_url: "https://x/v1".to_string(),
            api_key: "k".to_string(),
            context_window: DEFAULT_CONTEXT_WINDOW,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }

    #[test]
    fn merge_catalog_replaces_shadowed_providers_even_with_zero_models() {
        let seeded = ModelCatalog {
            models: vec![full_model("a", "p"), full_model("b", "q")],
            default_model: Some("p/a".to_string()),
            ..Default::default()
        };
        // A discovered model under `p` REPLACES the seeded `p/a` (the user
        // wins on a provider-id clash); the seeded default belongs to the
        // shadowed provider → `None` (the resolution chain degrades).
        let user = vec![full_model("x", "p")];
        let merged = merge_catalog(&seeded, &user, &["p".to_string()]);
        let mut keys: Vec<String> = merged
            .models
            .iter()
            .map(|m| format!("{}/{}", m.provider, m.id))
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["p/x".to_string(), "q/b".to_string()]);
        assert_eq!(merged.default_model, None);

        // ZERO discovered models STILL shadow: the provider's id is in
        // `shadowed_provider_ids` with an empty `user_models` → 0 models
        // for `p` (the stale seeded `p/a` does NOT resurrect — a
        // two-argument signature cannot express this, since `p` would be
        // absent from `user_models` entirely).
        let merged = merge_catalog(&seeded, &[], &["p".to_string()]);
        let keys: Vec<String> = merged
            .models
            .iter()
            .map(|m| format!("{}/{}", m.provider, m.id))
            .collect();
        assert_eq!(keys, vec!["q/b".to_string()]);
        assert_eq!(merged.default_model, None);
        // The `compaction` carries over from the seeded catalog.
        assert_eq!(merged.compaction, seeded.compaction);
    }

    #[test]
    fn merge_catalog_with_no_user_providers_is_the_seeded_catalog() {
        let seeded = ModelCatalog {
            models: vec![full_model("a", "p")],
            default_model: Some("p/a".to_string()),
            compaction: CompactionConfig {
                enabled: false,
                reserve_tokens: 1,
                keep_recent_tokens: 2,
            },
        };
        let merged = merge_catalog(&seeded, &[], &[]);
        assert_eq!(merged, seeded);
    }

    // ── the sparse `models-store.json` shape ──────────────────────────

    #[test]
    fn sparse_provider_entry_parses() {
        // The `tama` entry has only `{ models, checkedAt }` (no
        // `etag`/`lastModified`) and its model entry has no
        // `inputLimits` — the serde structs must accept the sparse
        // shape (or the whole source is dropped).
        let store: HashMap<String, ProviderStore> =
            serde_json::from_str(r#"{ "tama": { "models": [ { "id": "m" } ], "checkedAt": 1 } }"#)
                .unwrap();
        let provider = store.get("tama").expect("the sparse entry parses");
        assert_eq!(provider.models.len(), 1);
        assert_eq!(provider.models[0].id.as_deref(), Some("m"));
        assert_eq!(provider.models[0].api, None);
    }
}

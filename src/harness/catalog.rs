//! The model catalog. The desktop's model source is the Settings'
//! providers list (ADR 0014 — user providers shadow the base catalog on
//! a provider-id clash) merged with live per-provider discovery
//! (`GET /v1/models`). The catalog is the source for the model picker
//! and the [`Provider`] construction (a [`Model`]'s `api` discriminator
//! → `build_provider` (ADR 0024)).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::model_key::ModelKey;
use super::WireApi;

/// A model with no metadata gets this context window (best-effort).
/// Also the `effective_catalog` fallback for a user provider's
/// discovered model (ADR 0014 — a user model has no static metadata).
pub const DEFAULT_CONTEXT_WINDOW: u32 = 128000;
/// The `Compactor`'s (Task 6) thresholds (pi's own defaults).
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
    /// none — a local gateway).
    pub api_key: String,
    pub context_window: u32,
    /// USD per 1M input tokens.
    pub cost_per_mtok_in: f64,
    /// USD per 1M output tokens.
    pub cost_per_mtok_out: f64,
    /// `true` when the model can carry tool calls on its wire (the
    /// selectable set, ADR 0024 — the three wires the harness speaks +
    /// the `litellm` discovery mode (ADR 0026, its wire is
    /// `openai-completions`) all carry tools).
    pub supports_tools: bool,
    /// `reasoning` + a non-empty `thinkingLevelMap` (non-null values).
    pub supports_thinking: bool,
    /// The `thinkingLevelMap` keys with a non-null value (e.g.
    /// `["low", "medium", "xhigh"]`).
    pub thinking_levels: Vec<String>,
    /// The wire API discriminator (`"openai-completions"` /
    /// `"anthropic-messages"` / `"openai-responses"` / `"litellm"` /
    /// other; `None` for a metadata-less model — an unknown API is not
    /// one of the three wires the harness speaks nor the `litellm`
    /// discovery mode (ADR 0026)).
    pub api: Option<String>,
}

impl Model {
    /// The thinking-level membership predicate: `true` when the model
    /// advertises NO levels (the empty set soft-passes — the status quo
    /// is never a mismatch) or the level is in `thinking_levels`.
    pub fn supports_thinking_level(&self, level: &str) -> bool {
        self.thinking_levels.is_empty() || self.thinking_levels.iter().any(|l| l == level)
    }
}

/// The composed key of a catalog model (its `provider` / `id` halves).
impl From<&Model> for ModelKey {
    fn from(m: &Model) -> Self {
        ModelKey::new(m.provider.clone(), m.id.clone())
    }
}

/// The `Compactor`'s (Task 6) thresholds — `{ enabled, reserveTokens,
/// keepRecentTokens }`, defaulting `true` / `16384` / `20000` (compaction
/// is ON by default — pi's own default; an explicit `enabled: false`
/// disables it).
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

/// The desktop's model catalog (the base of the effective catalog —
/// the Settings' providers list is merged on top, ADR 0014).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelCatalog {
    /// The enabled models.
    pub models: Vec<Model>,
    /// The default model's composed key (e.g.
    /// `tama/Qwen/Qwen3.8-27B`), or `None` when no default is set.
    /// The resolution chain (settings `default_model` → this → first
    /// OpenAI-compatible model) is resolved at session start.
    pub default_model: Option<String>,
    /// The `Compactor` thresholds (Task 6) — compaction ON by default
    /// (`true` / `16384` / `20000`).
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

    /// The selectable set (ADR 0024): the tool-supporting models on a wire
    /// the harness speaks (`"openai-completions"` / `"anthropic-messages"`
    /// / `"openai-responses"`) PLUS the `litellm` discovery mode (ADR 0026
    /// — a discovery mode, not a fourth wire: its wire is
    /// `openai-completions`). A `None` / unknown `api` stays
    /// unselectable: an unknown API is not one of the three wires the
    /// harness speaks nor the `litellm` discovery mode.
    pub fn selectable(&self) -> Vec<&Model> {
        self.models
            .iter()
            .filter(|m| {
                m.supports_tools
                    && m.api
                        .as_deref()
                        .and_then(WireApi::parse)
                        .is_some_and(WireApi::is_supported)
            })
            .collect()
    }
}

/// Merge the base catalog with the user-provider models (ADR 0014).
/// `user_models` are the discovered models; `shadowed_provider_ids` are
/// the ids of EVERY user provider configured in `settings.json` (regardless
/// of whether its discovery succeeded — a provider that discovered 0 models
/// STILL shadows the base models for its id: user-wins-on-clash, even
/// on failure). For every id in `shadowed_provider_ids` the base models
/// for that id are REPLACED by the user models with that id (possibly none).
/// `default_model` is the base default unless it belongs to a shadowed
/// provider (then `None` — the caller's resolution chain degrades).
pub fn merge_catalog(
    base: &ModelCatalog,
    user_models: &[Model],
    shadowed_provider_ids: &[String],
) -> ModelCatalog {
    let kept_base: Vec<Model> = base
        .models
        .iter()
        .filter(|m| !shadowed_provider_ids.contains(&m.provider))
        .cloned()
        .collect();
    let mut models = kept_base;
    models.extend_from_slice(user_models);
    // The base default is kept only when it still EXISTS in the merged
    // result: a shadowed provider's default model was replaced (possibly by
    // nothing) → `None` (the caller's resolution chain degrades). A
    // default pointing at a model absent from `base.models` (a stale
    // key) also degrades to `None` (it is not in the merged result either).
    let default_model = base
        .default_model
        .clone()
        .filter(|key| models.iter().any(|m| ModelKey::from(m).to_string() == *key));
    ModelCatalog {
        models,
        default_model,
        compaction: base.compaction,
    }
}

/// The per-provider discovery cache entry: whether the last fetch SUCCEEDED
/// (a successful result is reused; a FAILED one is retried on the next read —
/// #85, so one transient outage cannot blacklist a provider for the process's
/// life) + the discovered models (model id → fresh metadata; empty when the
/// fetch failed or the model is absent from the response).
#[derive(Debug, Clone, Default)]
pub struct ProviderDiscovery {
    /// Whether the last attempt SUCCEEDED — the single cache gate (#85).
    /// Whether the last fetch and whether its result may be reused are one
    /// question: a FAILED fetch must be retried on the next read, or one
    /// transient outage (the app launched before the network was up, a gateway
    /// restarting) blacklists the provider for the process's life —
    /// 0 models, an `unreachable` row, and no recovery but a restart. A SUCCESS
    /// is what the cache exists to reuse; a failure is not an answer.
    ///
    /// A SUCCESSFUL fetch that returned zero models still sets this: "the
    /// endpoint answered and had nothing" is a real answer and caches normally.
    pub succeeded: bool,
    pub models: HashMap<String, DiscoveredMeta>,
}

/// Fresh model metadata from a live `GET /v1/models` (the OpenAI endpoint
/// "supplies everything" — the `pi-provider-litellm` `fetchModels` pattern).
/// All fields `Option`: a field absent in the response keeps the static
/// value.
///
/// The FOUR additive `Option` fields (T3a / #130, ADR 0026 amendment) carry the
/// LiteLLM reduction's conservative truth + the derived wire. They are
/// Default-compatible `Option`s so every NON-LiteLLM discovery arm keeps writing
/// `None` (byte-identical callers), and `DiscoveredMeta` is never serialized to
/// disk (only the in-memory [`ProviderDiscovery`] cache holds it), so the widening
/// is schema-invisible. `None` on `supports_tools` preserves today's hardcoded
/// `true`, `None` on the costs today's `0.0` (interface §10 R5). The terminal
/// consumer that fans them onto `Model` is T3b's `EffectiveCatalog::resolve`
/// write — this task carries the values, it does NOT apply them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiscoveredMeta {
    pub context_window: Option<u32>,
    pub thinking_levels: Option<Vec<String>>,
    pub supports_thinking: Option<bool>,
    /// The derived wire for a LiteLLM route (the pure computation over the
    /// deployment set; `None` for every other discovery shape).
    pub api: Option<WireApi>,
    /// The ALL-deployments `supports_function_calling` gate (`None` = the proxy
    /// advertised nothing → the status-quo default is preserved downstream).
    pub supports_tools: Option<bool>,
    /// USD per 1M input tokens, `max` over the route's deployments.
    pub cost_per_mtok_in: Option<f64>,
    /// USD per 1M output tokens, `max` over the route's deployments.
    pub cost_per_mtok_out: Option<f64>,
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

/// Query a provider's `GET {base_url}/models` for fresh model metadata
/// (bounded, best-effort). `base_url` is `.../v1` (the endpoint is
/// `{base_url}/models`). The `api` decides the wire (ADR 0024 / 0026):
/// - `"anthropic-messages"`: the ANTHROPIC shape (the `x-api-key` +
///   `anthropic-version` headers; the response carries `id` ONLY — a
///   discovered Anthropic model gets the `DEFAULT_CONTEXT_WINDOW`
///   fallback + no advertised thinking levels, the documented v1
///   degradation);
/// - `"litellm"`: the LITELLM shape (ADR 0026 — the LiteLLM proxy's
///   `GET {base}/model/info`; the response carries `model_name` +
///   `model_info.{max_input_tokens,reasoning_effort_levels,
///   supports_reasoning}`);
/// - any other `api` (`openai-completions` / `openai-responses` /
///   anything else): the OpenAI shape (`Authorization: Bearer` when the
///   key is non-empty — a local gateway needing no key → no header —
///   the `max_model_len` / `reasoningLevels` /
///   `supportsReasoningEffort` parsing).
///
/// `custom_headers` (issue #129) is the provider's EXPANDED + VALIDATED custom
/// header map (`provider_auth::headers::expand_map` did the `$VAR`/`${VAR}`
/// expansion and the name/value validation at the resolve call site — the arms
/// here only ATTACH them, via the single
/// [`transport::apply_custom_headers`](super::provider::transport::apply_custom_headers)
/// seam). `None` = a provider with no custom headers, i.e. the pre-feature
/// request shape verbatim.
///
/// A non-2xx / network error → `Err` (the caller degrades to the static
/// metadata).
pub async fn discover_models(
    base_url: &str,
    api_key: &str,
    api: &str,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<HashMap<String, DiscoveredMeta>, String> {
    let api = WireApi::parse(api).unwrap_or(WireApi::OpenAiCompletions);
    match api {
        WireApi::AnthropicMessages => {
            discover_anthropic_models(base_url, api_key, custom_headers).await
        }
        WireApi::LiteLLM => discover_litellm_models(base_url, api_key, custom_headers).await,
        // The OpenAI shape (`OpenAiCompletions` / `OpenAiResponses` /
        // unknown — the current behavior verbatim).
        _ => {
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
            // The provider's custom headers (issue #129), after the bearer so a
            // custom `authorization` entry can still override it (the documented
            // order — the operator's map wins).
            let req = super::provider::transport::apply_custom_headers(req, custom_headers);
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
                            // The four additive fields (T3a) stay `None` for the
                            // OpenAI shape — byte-identical callers.
                            ..Default::default()
                        },
                    )
                })
                .collect())
        }
    }
}

/// The Anthropic `GET {base_url}/models` (ADR 0024 — the SAME endpoint
/// path, since Anthropic's model list lives at `{base}/models`, e.g.
/// `https://api.anthropic.com/v1/models`). Headers (BOTH required):
/// `x-api-key` (NOT `Authorization: Bearer` — the Anthropic API's auth
/// header; a keyless call → NO `x-api-key` header, the existing
/// empty-key rule) + `anthropic-version` (Anthropic requires it on
/// EVERY endpoint — the same value as `AnthropicProvider::complete`'s
/// header; WITHOUT it, real `api.anthropic.com/v1/models` returns 400 →
/// discovery fails → 0 models → every Anthropic provider row shows
/// `unreachable`). The response carries NO `max_model_len` /
/// `reasoningLevels` / `supportsReasoningEffort` — a discovered
/// Anthropic model gets the `DEFAULT_CONTEXT_WINDOW` fallback + no
/// advertised thinking levels (the documented v1 degradation).
async fn discover_anthropic_models(
    base_url: &str,
    api_key: &str,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<HashMap<String, DiscoveredMeta>, String> {
    // Normalize the base to include the `/v1` API prefix (ADR 0024 —
    // mirroring ZCode's adapter-boundary normalization; WITHOUT it a
    // gateway root like `https://openrouter.ai/api` would 404 on
    // `…/api/models` → 0 discovered models → the row shows `unreachable`).
    let url = format!(
        "{}/models",
        super::provider::normalize_anthropic_base_url(base_url)
    );
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .read_timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let mut req = client
        .get(&url)
        // `anthropic-version` is REQUIRED on every Anthropic endpoint
        // (the same value as `AnthropicProvider::complete`'s header).
        .header(
            "anthropic-version",
            super::provider::transport::ANTHROPIC_API_VERSION,
        );
    // `x-api-key` (NOT `Authorization: Bearer` — the Anthropic API's
    // auth header); a keyless call → NO `x-api-key` header (the
    // existing empty-key rule).
    if !api_key.is_empty() {
        req = req.header("x-api-key", api_key);
    }
    // The provider's custom headers (issue #129) — see the OpenAI arm's note.
    let req = super::provider::transport::apply_custom_headers(req, custom_headers);
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("status {status}"));
    }
    let body: AnthropicModelsResponse = resp.json().await.map_err(|e| e.to_string())?;
    // `id` ONLY (the Anthropic list carries NO metadata fields) → an
    // all-`None` meta entry (the caller's `DEFAULT_CONTEXT_WINDOW`
    // fallback + no advertised thinking levels — the documented v1
    // degradation).
    Ok(body
        .data
        .into_iter()
        .map(|m| (m.id, DiscoveredMeta::default()))
        .collect())
}

/// The Anthropic `GET /v1/models` response (ADR 0024 — the entries
/// carry `id` + `display_name`; NO `max_model_len` /
/// `reasoningLevels` / `supportsReasoningEffort` — only `id` is
/// parsed, the rest is the documented v1 degradation).
#[derive(Debug, Deserialize)]
struct AnthropicModelsResponse {
    #[serde(default)]
    data: Vec<AnthropicDiscoveredModel>,
}

#[derive(Debug, Deserialize)]
struct AnthropicDiscoveredModel {
    id: String,
}

/// The LiteLLM discovery arm (ADR 0026 + its 2026-11-01 amendment, #130): a
/// FOUR-RUNG fallback ladder whose TERMINAL rung decides `Ok`/`Err`.
///
/// ```text
/// rung 1  GET {root}/model/info          2xx + non-empty data[] → group → reduce → Ok
/// rung 2  GET {root}/v1/models           the OpenAI shape (a NON-admin key cannot
///                                        read `/model/info`) → ids + wildcard expansion
/// rung 3  GET {root}/v1/model/info       the `/v1`-prefixed model/info (LiteLLM serves both)
/// rung 4  GET {root}/health  (liveness gate) then, per deployment id the earlier
///         rungs mentioned but could not turn into a route,
///         GET {root}/model/info?litellm_model_id={id}
/// ```
///
/// An EMPTY rung falls DOWN (never a terminal success) and only the chain's
/// terminal answer decides `Ok`/`Err` — `ProviderDiscovery.succeeded` is
/// untouched (ADR 0076 rule 3: any answering rung is a success; only an
/// all-empty chain is `Err`, and an `Err` is retried on the next read).
///
/// Rungs 1/3 group the deployment set by `model_name` and write the
/// [`reduce_litellm_route`] row (never last-wins); `model_info.max_input_tokens`
/// (fallback `max_output_tokens`) → `context_window`, `reasoning_effort_levels`
/// → `thinking_levels` VERBATIM (with the `supports_*_reasoning_effort`-boolean
/// fallback), `supports_reasoning` → `supports_thinking`. `default_reasoning_effort`
/// stays parsed-by-omission. Bearer auth when the key is non-empty (empty key →
/// no header) rides EVERY rung; one bounded 5s/10s client is reused across the
/// ladder, so a well-behaved proxy still makes ≤1 request per read. NO base-URL
/// normalization (the caller passes the base as configured; normalization is
/// T3b's).
async fn discover_litellm_models(
    base_url: &str,
    api_key: &str,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<HashMap<String, DiscoveredMeta>, String> {
    let root = base_url.trim_end_matches('/');
    // ONE bounded client across every rung (5s connect / 10s read).
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .read_timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    // A rung's GET with the ladder's standing request shape: Bearer when the
    // key is non-empty (the existing empty-key rule) + the provider's custom
    // headers (issue #129), attached AFTER the bearer so the operator's map
    // still wins (the documented order).
    let get = |path: String| {
        let mut req = client.get(path);
        if !api_key.is_empty() {
            req = req.bearer_auth(api_key);
        }
        super::provider::transport::apply_custom_headers(req, custom_headers)
    };
    // A `model_info`-shaped GET → the parsed entries (`None` = this rung did not
    // ANSWER at all: a transport failure, a non-2xx, or a body that will not
    // parse. A 2xx with an EMPTY `data[]` parses to an empty list and falls DOWN
    // at the caller's "usable routes?" test, never as a terminal success).
    let model_info = |path: String| async {
        let resp = get(path).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<LiteLLMModelInfoResponse>()
            .await
            .ok()
            .map(|body| body.data)
    };

    // ── rung 1: `GET {root}/model/info` ─────────────────────────────────
    let entries = model_info(format!("{root}/model/info"));
    // The deployment ids the rungs MENTIONED but could not turn into a route
    // (rung 4 probes exactly these; a rung that named nothing leaves it empty).
    let mut mentioned: Vec<String> = Vec::new();
    if let Some(data) = entries.await {
        collect_mentions(&data, &mut mentioned);
        let routes = group_and_reduce(&client, root, api_key, custom_headers, &data).await;
        if !routes.is_empty() {
            return Ok(routes);
        }
    }

    // ── rung 2: `GET {root}/v1/models` (the OpenAI shape) ───────────────
    // A NON-admin key cannot read `/model/info` but CAN read the OpenAI-shaped
    // model list; route-advertised ids (`anthropic/claude-…`) become individually
    // selectable here (the wildcard expansion).
    let rung2 = get(format!("{root}/v1/models")).send().await;
    let rung2 = match rung2 {
        Ok(resp) if resp.status().is_success() => {
            resp.json::<ModelsResponse>().await.ok().map(|b| b.data)
        }
        _ => None,
    };
    if let Some(data) = rung2.filter(|d| !d.is_empty()) {
        return Ok(expand_v1_models(&data));
    }

    // ── rung 3: `GET {root}/v1/model/info` ──────────────────────────────
    // LiteLLM serves BOTH `/model/info` and `/v1/model/info`; a proxy in front
    // of it may 403 only the unprefixed path.
    if let Some(data) = model_info(format!("{root}/v1/model/info")).await {
        collect_mentions(&data, &mut mentioned);
        let routes = group_and_reduce(&client, root, api_key, custom_headers, &data).await;
        if !routes.is_empty() {
            return Ok(routes);
        }
    }

    // ── rung 4: `/health` (liveness gate) + the `?litellm_model_id=` probes ──
    // `/health` answers "is this proxy the LiteLLM kind at all", so a provider
    // row that is really some other OpenAI-compatible gateway does not waste one
    // probe per deployment. It is a GATE, never a model source.
    let health = get(format!("{root}/health")).send().await;
    if !matches!(&health, Ok(resp) if resp.status().is_success()) {
        return Err("litellm discovery: no rung answered".to_string());
    }
    // `/health` answered and NO rung ever named a deployment id: the proxy is
    // the LiteLLM kind, it simply has nothing to serve. That is a real (empty)
    // answer — it CACHES (ADR 0076 rule 3), it is not a failure to retry.
    if mentioned.is_empty() {
        return Ok(HashMap::new());
    }
    let mut probed: HashMap<String, DiscoveredMeta> = HashMap::new();
    for id in mentioned.iter().filter(|id| !id.is_empty()) {
        let path = format!("{root}/model/info?litellm_model_id={}", urlencoded(id));
        if let Some(data) = model_info(path).await {
            probed.extend(group_and_reduce(&client, root, api_key, custom_headers, &data).await);
        }
    }
    // Terminal semantics: at least one non-empty probe result is a real answer
    // (cacheable, `succeeded = true`); total failure / all empty is `Err` (the
    // cache retries on the next read).
    if probed.is_empty() {
        return Err("litellm discovery: no rung answered".to_string());
    }
    Ok(probed)
}

/// Percent-encode a deployment id for the `?litellm_model_id=` query (ids carry
/// `/` and `:`; a raw `&`/`#` would otherwise corrupt the query).
fn urlencoded(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The deployment ids a `model_info` payload MENTIONS but cannot turn into a
/// selectable route: an entry whose `model_name` is absent/EMPTY has no key to
/// select it by, yet a modern proxy still carries its per-deployment
/// `litellm_model_id` / `id`. Those are exactly the ids rung 4 probes
/// (`?litellm_model_id={id}`) — the honest, reachable form of the plan's "per
/// deployment whose id is known", since a rung that failed outright can name no
/// id at all.
fn collect_mentions(data: &[LiteLLMModelInfoEntry], out: &mut Vec<String>) {
    for entry in data {
        if !entry.model_name.trim().is_empty() {
            continue;
        }
        if let Some(id) = entry
            .model_info
            .as_ref()
            .and_then(|mi| mi.litellm_model_id.clone().or_else(|| mi.id.clone()))
            .filter(|s| !s.trim().is_empty())
        {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
}

/// Group a `/model/info` payload by `model_name` (the deployment set SURVIVES —
/// this is the seam that used to last-wins-flatten it) and reduce each route
/// conservatively. `messages_available` is the `/v1/messages` probe latch's
/// answer for this proxy (`None` = never probed, i.e. no Claude-adapter route was
/// in play — the Messages rung of the derived wire then cannot fire).
async fn group_and_reduce(
    client: &reqwest::Client,
    root: &str,
    api_key: &str,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
    data: &[LiteLLMModelInfoEntry],
) -> HashMap<String, DiscoveredMeta> {
    // The latch is the ONLY new per-instance state and it probes at most once
    // per root per process, only for a payload that actually names a
    // Claude-family adapter (an all-OpenAI proxy never pays for the probe).
    let claude_route = data.iter().any(|e| {
        route_hint_of(&e.model_name).is_some_and(|h| CLAUDE_ROUTE_HINTS.contains(&h.as_str()))
    });
    let messages_available = if claude_route {
        Some(latch_messages_available(client, root, api_key, custom_headers).await)
    } else {
        None
    };
    // `BTreeMap` so a group's deployments arrive in a stable order (the levels
    // INTERSECTION preserves the first advertiser's order).
    let mut groups: std::collections::BTreeMap<&str, Vec<LiteLLMDeployment>> =
        std::collections::BTreeMap::new();
    for entry in data {
        groups
            .entry(entry.model_name.as_str())
            .or_default()
            .push(entry.deployment(messages_available));
    }
    groups
        .into_iter()
        // An EMPTY `model_name` is not a selectable key — such an entry only
        // contributes its deployment id to rung 4's probe set
        // (`collect_mentions`); it never becomes a route the picker points at.
        .filter(|(name, _)| !name.trim().is_empty())
        .map(|(name, deployments)| {
            // A route whose entries carry NO `model_info` block at all stays the
            // all-`None` meta entry (the documented degradation, byte-identical
            // to the pre-ladder shape) — the reduction fabricates no claims the
            // proxy never made.
            let meta = if deployments.iter().all(|d| d.is_bare()) {
                DiscoveredMeta::default()
            } else {
                aart_to_meta(&reduce_litellm_route(name, &deployments))
            };
            (name.to_string(), meta)
        })
        .collect()
}

/// Rung 2 (`/v1/models`): the OpenAI-shaped ids become selectable routes. An id
/// already carried by a `model_name` route keeps its metadata; a route-advertised
/// id (`<provider>/<id>`, the wildcard form) gets the OpenAI-shape metadata it
/// advertises and `None` for the four reduction fields (the documented v1
/// degradation for the OpenAI shape).
fn expand_v1_models(data: &[DiscoveredModel]) -> HashMap<String, DiscoveredMeta> {
    data.iter()
        .map(|m| {
            (
                m.id.clone(),
                DiscoveredMeta {
                    context_window: m.max_model_len,
                    thinking_levels: m.reasoning_levels.clone(),
                    supports_thinking: m.supports_reasoning_effort,
                    api: None,
                    supports_tools: None,
                    cost_per_mtok_in: None,
                    cost_per_mtok_out: None,
                },
            )
        })
        .collect()
}

/// The LiteLLM `GET /model/info` response (ADR 0026 — `data` entries carry
/// The LiteLLM `GET /model/info` response (ADR 0026 — `data` entries carry
/// `model_name` + a `model_info` metadata block; the rest of the block is
/// ignored for v1).
#[derive(Debug, Deserialize)]
struct LiteLLMModelInfoResponse {
    #[serde(default)]
    data: Vec<LiteLLMModelInfoEntry>,
}

#[derive(Debug, Deserialize)]
struct LiteLLMModelInfoEntry {
    model_name: String,
    // `Option`: an absent OR an explicit `"model_info": null` (both
    // observed shapes on LiteLLM deployments) must map to an all-`None`
    // meta entry (the documented degradation, same as a bare OpenAI
    // entry) — NOT fail the whole response (the `Option` handles both).
    model_info: Option<LiteLLMModelInfo>,
    /// The presence guard (T3a): a proxy ADDING a `model_info` key must never
    /// take the whole route down — the unrecognized remainder lands here and is
    /// simply not read (ADR 0026's "11 of 35 keys" census narrows to 17 + this).
    /// NEVER READ by design: without it serde would drop unknown keys silently
    /// today and a strict deserializer added later would fail the whole route.
    #[allow(dead_code)]
    #[serde(flatten)]
    extras: HashMap<String, serde_json::Value>,
}

impl LiteLLMModelInfoEntry {
    /// The entry → one [`LiteLLMDeployment`] (the raw per-deployment markers the
    /// reduction reads). `thinking_levels` keeps the existing VERBATIM parse with
    /// the `supports_*_reasoning_effort`-boolean fallback; `supports_thinking`
    /// keeps its inference-from-levels rule.
    fn deployment(&self, messages_available: Option<bool>) -> LiteLLMDeployment {
        // The deployment id: `litellm_model_id` / `id` (the per-deployment ids a
        // modern proxy carries), else the `model_name`, else a stable hash of the
        // entry (a route key must never collide).
        let name = self
            .model_info
            .as_ref()
            .and_then(|mi| mi.litellm_model_id.clone().or_else(|| mi.id.clone()))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| self.model_name.clone());
        let name = if name.is_empty() {
            format!("deployment-{}", stable_hash(&self.model_name))
        } else {
            name
        };
        let Some(mi) = &self.model_info else {
            return LiteLLMDeployment {
                name: name.into_boxed_str(),
                ..Default::default()
            };
        };
        let thinking_levels = mi.thinking_levels();
        LiteLLMDeployment {
            name: name.into_boxed_str(),
            context_window: mi.max_input_tokens,
            max_output_tokens: mi.max_output_tokens,
            // Per-TOKEN costs × 1e6 = the USD-per-1M the `Model` seam speaks;
            // absent → `0.0` (the documented degradation, and today's value).
            cost_per_mtok_in: mi.input_cost_per_token.unwrap_or(0.0) * 1e6,
            cost_per_mtok_out: mi.output_cost_per_token.unwrap_or(0.0) * 1e6,
            supports_reasoning: mi.supports_thinking(&thinking_levels),
            thinking_levels,
            supports_function_calling: mi.supports_function_calling,
            mode: mi.mode.clone(),
            // The adapter routing marker (the derived wire's raw input): the
            // `<provider>` half of the LiteLLM `provider/model` id form, which is
            // exactly the adapter name LiteLLM routes on (`anthropic/…`,
            // `bedrock/…`, `vertex_ai/…`).
            route_hint: route_hint_of(&self.model_name),
            messages_available,
            // The route's CURRENT deployment: both per-deployment ids present and
            // equal to the route's own name (the modern shape).
            current: self.model_info.as_ref().is_some_and(|mi| {
                mi.litellm_model_id.is_some()
                    && mi.id.is_some()
                    && mi.litellm_model_id == mi.id
                    && mi.id.as_deref() == Some(self.model_name.as_str())
            }),
        }
    }
}

/// The `<provider>` half of a LiteLLM `provider/model` id (the adapter routing
/// marker); `None` for a plain id (no `/`).
fn route_hint_of(model_name: &str) -> Option<String> {
    let (provider, rest) = model_name.split_once('/')?;
    (!provider.is_empty() && !rest.is_empty()).then(|| provider.to_lowercase())
}

/// The per-process `/v1/messages` probe LATCH (root → answered Messages): the ONE
/// piece of new per-instance state the ladder carries, and it NEVER probes per
/// turn (the `accepts_stream_options` retry-and-latch precedent). A proxy that
/// enables `/v1/messages` later needs a restart — documented, and matching that
/// latch's behavior.
fn messages_probe_latch() -> &'static std::sync::Mutex<HashMap<String, bool>> {
    static LATCH: std::sync::OnceLock<std::sync::Mutex<HashMap<String, bool>>> =
        std::sync::OnceLock::new();
    LATCH.get_or_init(Default::default)
}

/// The one-time `GET {root}/v1/messages` liveness probe for a Claude-adapter
/// route (the derived wire's Messages rung needs proof, not just an adapter
/// name). Latched per root for the process's life. Headers mirror
/// `discover_anthropic_models`: `anthropic-version` is REQUIRED on every
/// Anthropic endpoint (without it a real `api.anthropic.com` 400s) and
/// `x-api-key` carries the key (absent for an empty key). A clean 2xx answers
/// AVAILABLE; anything else (404, a 405 from an adapter that answers POST but not
/// the probe, a transport failure) answers MISS.
async fn latch_messages_available(
    client: &reqwest::Client,
    root: &str,
    api_key: &str,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
) -> bool {
    if let Some(seen) = messages_probe_latch()
        .lock()
        .ok()
        .and_then(|m| m.get(root).copied())
    {
        return seen;
    }
    let mut req = client.get(format!("{root}/v1/messages")).header(
        "anthropic-version",
        super::provider::transport::ANTHROPIC_API_VERSION,
    );
    if !api_key.is_empty() {
        req = req.header("x-api-key", api_key);
    }
    let req = super::provider::transport::apply_custom_headers(req, custom_headers);
    let available = match req.send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    };
    if let Ok(mut m) = messages_probe_latch().lock() {
        m.insert(root.to_string(), available);
    }
    available
}

/// A stable short hash of a string (FNV-1a) — the collision-free fallback
/// deployment name for an entry that carries NO id at all.
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Debug, Deserialize)]
struct LiteLLMModelInfo {
    #[serde(rename = "max_input_tokens")]
    max_input_tokens: Option<u32>,
    #[serde(rename = "max_output_tokens")]
    max_output_tokens: Option<u32>,
    #[serde(rename = "reasoning_effort_levels")]
    reasoning_effort_levels: Option<Vec<String>>,
    #[serde(rename = "supports_reasoning")]
    supports_reasoning: Option<bool>,
    #[serde(rename = "supports_none_reasoning_effort")]
    supports_none_reasoning_effort: Option<bool>,
    #[serde(rename = "supports_minimal_reasoning_effort")]
    supports_minimal_reasoning_effort: Option<bool>,
    #[serde(rename = "supports_low_reasoning_effort")]
    supports_low_reasoning_effort: Option<bool>,
    #[serde(rename = "supports_medium_reasoning_effort")]
    supports_medium_reasoning_effort: Option<bool>,
    #[serde(rename = "supports_high_reasoning_effort")]
    supports_high_reasoning_effort: Option<bool>,
    #[serde(rename = "supports_xhigh_reasoning_effort")]
    supports_xhigh_reasoning_effort: Option<bool>,
    #[serde(rename = "supports_max_reasoning_effort")]
    supports_max_reasoning_effort: Option<bool>,
    // ── the widened census (T3a / #130 — ADR 0026 amendment; 11 → 17 keys) ──
    /// The deployment's API mode (`chat` / `completion` / `responses`) — the raw
    /// marker of the derived-wire computation (its application is T3b's).
    #[serde(rename = "mode")]
    mode: Option<String>,
    /// The tool-support marker feeding the ALL-deployments `supports_tools` gate.
    #[serde(rename = "supports_function_calling")]
    supports_function_calling: Option<bool>,
    /// The per-deployment id a `?litellm_model_id=` probe names.
    #[serde(rename = "litellm_model_id")]
    litellm_model_id: Option<String>,
    /// The `model_info` primary key.
    #[serde(rename = "id")]
    id: Option<String>,
    /// USD per TOKEN in (× 1e6 at the deployment seam). The reduction's `max`
    /// cost is the FIRST reader of these fields.
    #[serde(rename = "input_cost_per_token")]
    input_cost_per_token: Option<f64>,
    /// USD per TOKEN out.
    #[serde(rename = "output_cost_per_token")]
    output_cost_per_token: Option<f64>,
}

impl LiteLLMModelInfo {
    fn thinking_levels(&self) -> Option<Vec<String>> {
        if let Some(ref levels) = self.reasoning_effort_levels {
            if !levels.is_empty() {
                return Some(levels.clone());
            }
        }
        let mut synthesized = Vec::new();
        if self.supports_none_reasoning_effort == Some(true) {
            synthesized.push("none".to_string());
        }
        if self.supports_minimal_reasoning_effort == Some(true) {
            synthesized.push("minimal".to_string());
        }
        if self.supports_low_reasoning_effort == Some(true) {
            synthesized.push("low".to_string());
        }
        if self.supports_medium_reasoning_effort == Some(true) {
            synthesized.push("medium".to_string());
        }
        if self.supports_high_reasoning_effort == Some(true) {
            synthesized.push("high".to_string());
        }
        if self.supports_xhigh_reasoning_effort == Some(true) {
            synthesized.push("xhigh".to_string());
        }
        if self.supports_max_reasoning_effort == Some(true) {
            synthesized.push("max".to_string());
        }
        if synthesized.is_empty() {
            None
        } else {
            Some(synthesized)
        }
    }

    fn supports_thinking(&self, thinking_levels: &Option<Vec<String>>) -> Option<bool> {
        self.supports_reasoning.or_else(|| {
            if thinking_levels.is_some() {
                Some(true)
            } else {
                None
            }
        })
    }
}

// ── the multi-deployment model + the conservative reduction (T3a / #130) ──
//
// A LiteLLM proxy may serve SEVERAL deployments under ONE `model_name` route.
// Flattening that set at the map seam (last-wins) is what this shape replaces:
// the deployment set survives into [`LiteLLMDeployment`], a `model_name` groups
// its deployments, and [`reduce_litellm_route`] folds the group into one
// conservative route row. Pure (no HTTP) so the reduction is testable without a
// server, exactly like `merge_catalog`.

/// One deployment of a `model_name` route (inert, never serialized).
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct LiteLLMDeployment {
    /// The deployment id — `model_info.id` / `litellm_model_id`, else the
    /// `model_name`, else a stable hash of the entry (never collides).
    pub(crate) name: Box<str>,
    /// `model_info.max_input_tokens` (the context window).
    pub(crate) context_window: Option<u32>,
    /// `model_info.max_output_tokens` (an output cap; a real per-deployment
    /// field now, not only the window fallback).
    pub(crate) max_output_tokens: Option<u32>,
    /// USD per 1M input tokens (`input_cost_per_token` × 1e6; absent → `0.0`).
    pub(crate) cost_per_mtok_in: f64,
    /// USD per 1M output tokens (`output_cost_per_token` × 1e6; absent → `0.0`).
    pub(crate) cost_per_mtok_out: f64,
    /// `model_info.supports_reasoning`.
    pub(crate) supports_reasoning: Option<bool>,
    /// The advertised reasoning levels (verbatim, `off`↔`none` never translated).
    pub(crate) thinking_levels: Option<Vec<String>>,
    /// `model_info.supports_function_calling`.
    pub(crate) supports_function_calling: Option<bool>,
    /// `model_info.mode` (`"chat"` / `"completion"` / `"responses"`) — the raw
    /// marker the derived-wire computation reads (its CONSUMER is T3b).
    pub(crate) mode: Option<String>,
    /// The adapter routing string (`"anthropic"` / `"bedrock"` / `"vertex_ai"`)
    /// — the second raw marker for the derived wire.
    pub(crate) route_hint: Option<String>,
    /// The one-time `/v1/messages` probe LATCH (set during discovery, never per
    /// turn). `None` = never probed.
    pub(crate) messages_available: Option<bool>,
    /// The route's own current deployment (`model_info.id == litellm_model_id`).
    pub(crate) current: bool,
}

/// A `model_name` ROUTE with its deployments reduced CONSERVATIVELY (the row the
/// `DiscoveredMeta` is written from).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Aart {
    /// The `model_name`.
    pub(crate) name: String,
    /// `min` over the deployments that advertise a window.
    pub(crate) context_window: Option<u32>,
    /// `min` over the deployments that advertise an output cap.
    pub(crate) max_output_tokens: Option<u32>,
    /// `max` over the deployments (absent → `0.0`).
    pub(crate) cost_per_mtok_in: f64,
    /// `max` over the deployments (absent → `0.0`).
    pub(crate) cost_per_mtok_out: f64,
    /// ALL deployments advertise `supports_reasoning: true` (absent → `false`).
    pub(crate) supports_thinking: bool,
    /// The INTERSECTION over the deployments that advertise levels.
    pub(crate) thinking_levels: Vec<String>,
    /// ALL deployments advertise `supports_function_calling: true` (absent → `false`).
    pub(crate) supports_tools: bool,
    /// The derived wire (the pure computation; consumed by T3b).
    pub(crate) derived_wire: WireApi,
    /// The un-flattened deployment set (grouping survives the reduction).
    pub(crate) deployments: Vec<LiteLLMDeployment>,
}

/// The Claude-family adapters whose deployments ride the Messages wire when the
/// probe latch agrees.
const CLAUDE_ROUTE_HINTS: [&str; 3] = ["anthropic", "bedrock", "vertex_ai"];

/// The derived-wire COMPUTATION (the pure half of T3b's rule, computed where the
/// deployment set lives — T3a does NOT apply it anywhere):
/// 1. `openai-responses` when EVERY deployment's `mode == "responses"`;
/// 2. else `anthropic-messages` when EVERY deployment's `route_hint` names a
///    Claude adapter AND every `messages_available` latch is true;
/// 3. else `openai-completions` (today's behavior).
///
/// Multi-deployment routes require ALL deployments to agree, so a mixed route
/// never sends a wire half its peers reject. The `current` deployment's offer is
/// the TIEBREAK when the route disagrees (secondary to all-agree); without a
/// `current` marker a mixed route falls back.
pub(crate) fn derive_wire_for_route(deployments: &[LiteLLMDeployment]) -> WireApi {
    if deployments.is_empty() {
        return WireApi::OpenAiCompletions;
    }
    let responses = |d: &LiteLLMDeployment| d.mode.as_deref() == Some("responses");
    let messages = |d: &LiteLLMDeployment| {
        let hint = d.route_hint.as_deref().unwrap_or_default();
        CLAUDE_ROUTE_HINTS.contains(&hint) && d.messages_available == Some(true)
    };
    if deployments.iter().all(responses) {
        return WireApi::OpenAiResponses;
    }
    if deployments.iter().all(messages) {
        return WireApi::AnthropicMessages;
    }
    // The route disagrees: the CURRENT deployment's offer wins when it has one,
    // otherwise the fallback (a mixed route must not be guessed).
    if deployments.iter().any(|d| d.current && responses(d)) {
        return WireApi::OpenAiResponses;
    }
    if deployments.iter().any(|d| d.current && messages(d)) {
        return WireApi::AnthropicMessages;
    }
    WireApi::OpenAiCompletions
}

/// The conservative reduction of one route's deployment set. Direction differs
/// per field ON PURPOSE: the windows are `min` (a multi-deployment route must
/// never overestimate what it accepts), the costs are `max` (the most this route
/// could cost), and the two capability gates are ALL-deployments (a route whose
/// peers disagree shows no thinking controls and is not tool-selectable).
pub(crate) fn reduce_litellm_route(name: &str, deployments: &[LiteLLMDeployment]) -> Aart {
    let min_of = |pick: fn(&LiteLLMDeployment) -> Option<u32>| {
        deployments
            .iter()
            .filter_map(pick)
            .min()
            // An absent window contributes `None` — the `DEFAULT_CONTEXT_WINDOW`
            // resolve fallback governs (never a guessed number).
            .map(Some)
            .unwrap_or(None)
    };
    let max_cost =
        |pick: fn(&LiteLLMDeployment) -> f64| deployments.iter().map(pick).fold(0.0_f64, f64::max);
    // The levels INTERSECTION: keep the first advertiser's order, keep a level
    // only when every OTHER advertiser carries it. A deployment advertising no
    // levels does not empty the intersection (the empty-set soft-pass).
    let advertisers: Vec<&LiteLLMDeployment> = deployments
        .iter()
        .filter(|d| d.thinking_levels.as_ref().is_some_and(|l| !l.is_empty()))
        .collect();
    let thinking_levels = advertisers
        .first()
        .map(|first| {
            first
                .thinking_levels
                .clone()
                .unwrap_or_default()
                .into_iter()
                .filter(|level| {
                    advertisers.iter().all(|d| {
                        d.thinking_levels
                            .as_ref()
                            .is_some_and(|l| l.iter().any(|x| x == level))
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Aart {
        name: name.to_string(),
        context_window: min_of(|d| d.context_window),
        max_output_tokens: min_of(|d| d.max_output_tokens),
        cost_per_mtok_in: max_cost(|d| d.cost_per_mtok_in),
        cost_per_mtok_out: max_cost(|d| d.cost_per_mtok_out),
        // The vacuous-ALL trap: an EMPTY deployment set is not "all agree".
        supports_thinking: !deployments.is_empty()
            && deployments
                .iter()
                .all(|d| d.supports_reasoning == Some(true)),
        thinking_levels,
        supports_tools: !deployments.is_empty()
            && deployments
                .iter()
                .all(|d| d.supports_function_calling == Some(true)),
        derived_wire: derive_wire_for_route(deployments),
        deployments: deployments.to_vec(),
    }
}

impl LiteLLMDeployment {
    /// `true` when the payload advertised NOTHING for this deployment (an absent
    /// / null `model_info`): the route then keeps the all-`None` meta entry
    /// rather than a reduction that fabricates claims.
    fn is_bare(&self) -> bool {
        self.context_window.is_none()
            && self.max_output_tokens.is_none()
            && self.cost_per_mtok_in == 0.0
            && self.cost_per_mtok_out == 0.0
            && self.supports_reasoning.is_none()
            && self.thinking_levels.is_none()
            && self.supports_function_calling.is_none()
            && self.mode.is_none()
            && self.route_hint.is_none()
    }
}

/// The route row → the cached metadata. The reduction's conservative truth rides
/// the four additive fields; `max_output_tokens` stays a window FALLBACK only
/// (never the primary window — it is an output cap).
fn aart_to_meta(route: &Aart) -> DiscoveredMeta {
    // `supports_thinking` keeps the pre-ladder shape for a route that advertised
    // NOTHING about reasoning (`None`, not a fabricated `false`) — the gate only
    // answers where the proxy spoke.
    let advertised = route
        .deployments
        .iter()
        .any(|d| d.supports_reasoning.is_some());
    DiscoveredMeta {
        context_window: route.context_window.or(route.max_output_tokens),
        // An empty intersection is `None` (the soft-pass), matching the
        // pre-grouping shape.
        thinking_levels: (!route.thinking_levels.is_empty()).then(|| route.thinking_levels.clone()),
        supports_thinking: advertised.then_some(route.supports_thinking),
        api: Some(route.derived_wire),
        supports_tools: Some(route.supports_tools),
        cost_per_mtok_in: Some(route.cost_per_mtok_in),
        cost_per_mtok_out: Some(route.cost_per_mtok_out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── `Model` ───────────────────────────────────────────────────────

    #[test]
    fn supports_thinking_level_empty_set_soft_passes() {
        let m = model("a", true, Some("openai-completions"));
        assert!(m.supports_thinking_level("high"));
        assert!(m.supports_thinking_level("anything"));
    }

    #[test]
    fn supports_thinking_level_membership() {
        let mut m = model("a", true, Some("openai-completions"));
        m.thinking_levels = vec!["low".to_string(), "high".to_string()];
        assert!(m.supports_thinking_level("high"));
        assert!(!m.supports_thinking_level("bogus"));
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
    fn selectable_includes_the_three_wires() {
        let catalog = ModelCatalog {
            models: vec![
                model("a", true, Some("openai-completions")),
                model("b", true, Some("anthropic-messages")),
                model("c", true, Some("openai-responses")),
                model("lit/1", true, Some("litellm")),
            ],
            ..Default::default()
        };
        assert_eq!(catalog.all().len(), 4);
        assert!(catalog.get("a").is_some());
        assert!(catalog.get("zzz").is_none());
        // The selectable set (ADR 0024): one model per wire + the
        // `litellm` discovery mode (ADR 0026 — its wire is
        // `openai-completions`), all `supports_tools` → all four
        // selectable.
        let ids: Vec<&str> = catalog.selectable().iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c", "lit/1"]);
    }

    #[test]
    fn selectable_excludes_none_and_unknown_api() {
        // A `None` / unknown `api` stays unselectable (an unknown API is
        // not one of the three the harness speaks).
        let catalog = ModelCatalog {
            models: vec![
                model("a", true, Some("openai-completions")),
                model("b", true, Some("google-generative-ai")),
                model("c", true, None), // metadata-less: api unknown
            ],
            ..Default::default()
        };
        let ids: Vec<&str> = catalog.selectable().iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["a"]);
    }

    #[test]
    fn selectable_requires_supports_tools() {
        // A known wire that does NOT support tools stays unselectable.
        let catalog = ModelCatalog {
            models: vec![
                model("a", true, Some("openai-completions")),
                model("b", false, Some("anthropic-messages")),
                model("d", false, Some("litellm")),
            ],
            ..Default::default()
        };
        let ids: Vec<&str> = catalog.selectable().iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["a"]);
    }

    // ── the `CompactionConfig` defaults ───────────────────────────────

    #[test]
    fn compaction_config_defaults() {
        let c = CompactionConfig::default();
        // (finding 13a) Compaction is ON by default (a catalog without an
        // explicit config still compacts; an explicit `enabled: false`
        // disables it).
        assert!(c.enabled);
        assert_eq!(c.reserve_tokens, 16384);
        assert_eq!(c.keep_recent_tokens, 20000);
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
    async fn discover_models_openai_shape_unchanged() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"{"data":[{"id":"m/1","max_model_len":99999,
            "reasoningLevels":["low","medium"],
            "supportsReasoningEffort":true},
            {"id":"m/2"}]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(
            &format!("http://{addr}/v1"),
            "test-key",
            "openai-completions",
            None,
        )
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
    async fn discover_models_responses_uses_the_openai_shape() {
        // `openai-responses` routes through the SAME OpenAI parsing path
        // (the endpoint + the `max_model_len` / `reasoningLevels` /
        // `supportsReasoningEffort` parsing — ADR 0024).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"{"data":[{"id":"r/1","max_model_len":42424,
            "reasoningLevels":["low"],
            "supportsReasoningEffort":true}]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(
            &format!("http://{addr}/v1"),
            "test-key",
            "openai-responses",
            None,
        )
        .await
        .unwrap();
        // The OpenAI parsing path mapped the metadata (not the Anthropic
        // all-`None` shape).
        assert_eq!(models["r/1"].context_window, Some(42424));
        assert_eq!(models["r/1"].thinking_levels, Some(vec!["low".into()]));
        assert_eq!(models["r/1"].supports_thinking, Some(true));
        server.abort();
    }

    #[tokio::test]
    async fn discover_models_anthropic_shape() {
        // (ADR 0024) The Anthropic model list carries `id` +
        // `display_name` ONLY (NO `max_model_len` / `reasoningLevels` /
        // `supportsReasoningEffort`) — a discovered Anthropic model gets
        // the `DEFAULT_CONTEXT_WINDOW` fallback + no advertised thinking
        // levels (the documented v1 degradation): an id-keyed all-`None`
        // meta map.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"{"data":[{"id":"claude-sonnet-4","display_name":"Claude Sonnet 4"},
            {"id":"claude-opus-4"}]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(
            &format!("http://{addr}/v1"),
            "test-key",
            "anthropic-messages",
            None,
        )
        .await
        .unwrap();
        assert_eq!(models.len(), 2);
        // Both entries parse (the `display_name` is ignored) and carry
        // all-`None` metadata.
        for meta in models.values() {
            assert_eq!(meta, &DiscoveredMeta::default());
        }
        server.abort();
    }

    // ── the LiteLLM `GET {base}/model/info` discovery shape (ADR 0026) ──

    /// The `raw_json_server` above ignores the request PATH + HEADERS (it
    /// serves its fixed body to any GET) — these tests pin the `litellm`
    /// DISPATCH ARM + the response parsing. The `{base}/model/info` path
    /// + the Bearer header are verified by the live smoke (Task 5).

    #[tokio::test]
    async fn discover_models_litellm_maps_the_model_info_shape() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // A full entry (`lit/1`) + a bare entry (`lit/2` — an absent
        // `model_info` → an all-`None` meta entry, the documented
        // degradation, the same as a bare OpenAI entry).
        let body = r#"{"data":[
            {"model_name":"lit/1","model_info":{
                "max_input_tokens":262144,
                "max_output_tokens":32768,
                "reasoning_effort_levels":["none","low","medium","xhigh"],
                "supports_reasoning":true}},
            {"model_name":"lit/2"}]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(&format!("http://{addr}/v1"), "test-key", "litellm", None)
            .await
            .unwrap();
        // `model_name` is the map key (NOT an `id` field — the OpenAI
        // shape would fail to parse this body). The keys are sorted —
        // `HashMap` iteration order is nondeterministic.
        let mut keys: Vec<&str> = models.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["lit/1", "lit/2"]);
        // `lit/1`: `max_input_tokens` → `context_window` (the
        // `max_output_tokens` is NOT used when `max_input_tokens` is
        // present); `reasoning_effort_levels` → `thinking_levels`
        // VERBATIM; `supports_reasoning` → `supports_thinking`.
        let lit1 = &models["lit/1"];
        assert_eq!(lit1.context_window, Some(262144));
        assert_eq!(
            lit1.thinking_levels,
            Some(vec![
                "none".into(),
                "low".into(),
                "medium".into(),
                "xhigh".into()
            ])
        );
        assert_eq!(lit1.supports_thinking, Some(true));
        // `lit/2`: absent `model_info` → all-`None` meta.
        let lit2 = &models["lit/2"];
        assert_eq!(lit2, &DiscoveredMeta::default());
        server.abort();
    }

    #[tokio::test]
    async fn discover_models_litellm_falls_back_to_max_output_tokens() {
        // `max_input_tokens` absent → the `max_output_tokens` fallback
        // (a `max_output_tokens`-only entry still gets a window).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"{"data":[{"model_name":"lit/o","model_info":
            {"max_output_tokens":999}}]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(&format!("http://{addr}/v1"), "test-key", "litellm", None)
            .await
            .unwrap();
        assert_eq!(models["lit/o"].context_window, Some(999));
        // No reasoning fields advertised → all `None`.
        assert_eq!(models["lit/o"].thinking_levels, None);
        assert_eq!(models["lit/o"].supports_thinking, None);
        server.abort();
    }

    #[tokio::test]
    async fn discover_models_litellm_falls_back_to_reasoning_effort_booleans() {
        // When `reasoning_effort_levels` is null/absent, but individual
        // `supports_*_reasoning_effort` booleans are set, synthesize the
        // levels in canonical order and infer `supports_thinking: true`.
        // Also test:
        // - explicit non-empty `reasoning_effort_levels` takes precedence
        // - empty `reasoning_effort_levels: []` falls back to booleans
        // - explicit `supports_reasoning: false` is preserved
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = r#"{"data":[
            {"model_name":"lit/b","model_info":{
                "supports_none_reasoning_effort": null,
                "supports_minimal_reasoning_effort": null,
                "supports_low_reasoning_effort": null,
                "supports_xhigh_reasoning_effort": true,
                "supports_max_reasoning_effort": true,
                "reasoning_effort_levels": null,
                "default_reasoning_effort": null
            }},
            {"model_name":"lit/all","model_info":{
                "supports_none_reasoning_effort": true,
                "supports_minimal_reasoning_effort": true,
                "supports_low_reasoning_effort": true,
                "supports_medium_reasoning_effort": true,
                "supports_high_reasoning_effort": true,
                "supports_xhigh_reasoning_effort": true,
                "supports_max_reasoning_effort": true,
                "reasoning_effort_levels": []
            }},
            {"model_name":"lit/precedence","model_info":{
                "supports_none_reasoning_effort": true,
                "supports_xhigh_reasoning_effort": true,
                "reasoning_effort_levels": ["low", "high"]
            }},
            {"model_name":"lit/disabled","model_info":{
                "supports_high_reasoning_effort": true,
                "supports_reasoning": false
            }}
        ]}"#;
        let server = raw_json_server(listener, 200, body).await;
        let models = discover_models(&format!("http://{addr}/v1"), "test-key", "litellm", None)
            .await
            .unwrap();

        // lit/b: subset of booleans
        let meta_b = &models["lit/b"];
        assert_eq!(
            meta_b.thinking_levels,
            Some(vec!["xhigh".to_string(), "max".to_string()])
        );
        assert_eq!(meta_b.supports_thinking, Some(true));

        // lit/all: all 7 booleans in canonical progression, empty array fallback
        let meta_all = &models["lit/all"];
        assert_eq!(
            meta_all.thinking_levels,
            Some(vec![
                "none".to_string(),
                "minimal".to_string(),
                "low".to_string(),
                "medium".to_string(),
                "high".to_string(),
                "xhigh".to_string(),
                "max".to_string(),
            ])
        );
        assert_eq!(meta_all.supports_thinking, Some(true));

        // lit/precedence: explicit array wins
        let meta_prec = &models["lit/precedence"];
        assert_eq!(
            meta_prec.thinking_levels,
            Some(vec!["low".to_string(), "high".to_string()])
        );
        assert_eq!(meta_prec.supports_thinking, Some(true));

        // lit/disabled: supports_reasoning: false preserved
        let meta_dis = &models["lit/disabled"];
        assert_eq!(meta_dis.thinking_levels, Some(vec!["high".to_string()]));
        assert_eq!(meta_dis.supports_thinking, Some(false));

        server.abort();
    }

    #[tokio::test]
    async fn discover_models_litellm_errors_on_a_non_2xx() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = raw_json_server(listener, 404, r#"{"detail":"Not Found"}"#).await;
        // A non-2xx → `Err` (the caller degrades to the static metadata —
        // the row shows `unreachable`, 0 models, still shadows the base id).
        assert!(
            discover_models(&format!("http://{addr}/v1"), "test-key", "litellm", None,)
                .await
                .is_err()
        );
        server.abort();
    }

    #[tokio::test]
    async fn discover_models_errors_on_a_non_2xx() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = raw_json_server(listener, 401, r#"{"error":"auth"}"#).await;
        // A 401 (a bad/missing key) → `Err` (the caller degrades to the
        // static metadata).
        assert!(discover_models(
            &format!("http://{addr}/v1"),
            "bad-key",
            "openai-completions",
            None,
        )
        .await
        .is_err());
        server.abort();
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
        let base = ModelCatalog {
            models: vec![full_model("a", "p"), full_model("b", "q")],
            default_model: Some("p/a".to_string()),
            ..Default::default()
        };
        // A discovered model under `p` REPLACES the base `p/a` (the user
        // wins on a provider-id clash); the base default belongs to the
        // shadowed provider → `None` (the resolution chain degrades).
        let user = vec![full_model("x", "p")];
        let merged = merge_catalog(&base, &user, &["p".to_string()]);
        let mut keys: Vec<String> = merged
            .models
            .iter()
            .map(|m| ModelKey::from(m).to_string())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["p/x".to_string(), "q/b".to_string()]);
        assert_eq!(merged.default_model, None);

        // ZERO discovered models STILL shadow: the provider's id is in
        // `shadowed_provider_ids` with an empty `user_models` → 0 models
        // for `p` (the stale base `p/a` does NOT resurrect — a
        // two-argument signature cannot express this, since `p` would be
        // absent from `user_models` entirely).
        let merged = merge_catalog(&base, &[], &["p".to_string()]);
        let keys: Vec<String> = merged
            .models
            .iter()
            .map(|m| ModelKey::from(m).to_string())
            .collect();
        assert_eq!(keys, vec!["q/b".to_string()]);
        assert_eq!(merged.default_model, None);
        // The `compaction` carries over from the base catalog.
        assert_eq!(merged.compaction, base.compaction);
    }

    #[test]
    fn merge_catalog_with_no_user_providers_is_the_base_catalog() {
        let base = ModelCatalog {
            models: vec![full_model("a", "p")],
            default_model: Some("p/a".to_string()),
            compaction: CompactionConfig {
                enabled: false,
                reserve_tokens: 1,
                keep_recent_tokens: 2,
            },
        };
        let merged = merge_catalog(&base, &[], &[]);
        assert_eq!(merged, base);
    }

    // ── the multi-deployment reduction (T3a / #130, ADR 0026 amendment) ──
    //
    // PURE tests (no HTTP): the reduction is a pure function over the
    // deployment set, exactly like the `merge_catalog` tests above. The
    // terminal consumer of `supports_tools` / `cost_per_mtok_*` is T3b's
    // `EffectiveCatalog::resolve` write (interface §10 R5) — these tests
    // assert at the `AART` / `DiscoveredMeta` level and claim NOTHING about
    // `Model`.

    /// A deployment with only its id set (every marker `None` / `0.0`).
    fn deployment(name: &str) -> LiteLLMDeployment {
        LiteLLMDeployment {
            name: name.into(),
            ..Default::default()
        }
    }

    #[test]
    fn discovered_meta_additive_fields_default_none() {
        // The four additive fields (interface §10 R5 + the derived wire) are
        // Default-compatible `Option`s: every NON-LiteLLM discovery arm keeps
        // writing them `None` (byte-identical callers).
        let m = DiscoveredMeta::default();
        assert_eq!(m.api, None);
        assert_eq!(m.supports_tools, None);
        assert_eq!(m.cost_per_mtok_in, None);
        assert_eq!(m.cost_per_mtok_out, None);
    }

    #[test]
    fn reduce_litellm_route_conservative_min_window_min_output_max_cost() {
        // Windows/output are `min` (a multi-deployment route NEVER
        // overestimates); cost is `max` ("the most this route could cost").
        let mut a = deployment("dep-a");
        a.context_window = Some(128000);
        a.max_output_tokens = Some(16000);
        a.cost_per_mtok_in = 0.5;
        a.cost_per_mtok_out = 1.0;
        let mut b = deployment("dep-b");
        b.context_window = Some(64000);
        b.max_output_tokens = Some(8000);
        b.cost_per_mtok_in = 2.5;
        b.cost_per_mtok_out = 6.0;
        let route = reduce_litellm_route("r", &[a, b]);
        assert_eq!(route.name, "r");

        assert_eq!(route.context_window, Some(64000));
        assert_eq!(route.max_output_tokens, Some(8000));
        assert_eq!(route.cost_per_mtok_in, 2.5);
        assert_eq!(route.cost_per_mtok_out, 6.0);
        // The deployment SET survives the reduction (grouping never last-wins).
        assert_eq!(route.deployments.len(), 2);
    }

    #[test]
    fn reduce_litellm_route_absent_window_contributes_none_not_a_guess() {
        // A deployment advertising NO window contributes `None`; the existing
        // `DEFAULT_CONTEXT_WINDOW` resolve fallback governs.
        let mut a = deployment("dep-a");
        a.context_window = Some(128000);
        let b = deployment("dep-b");
        assert_eq!(
            reduce_litellm_route("r", &[a, b]).context_window,
            Some(128000)
        );
        assert_eq!(
            reduce_litellm_route("r", &[deployment("x")]).context_window,
            None
        );
        // Absent costs are `0.0` (the documented degradation for proxies that
        // omit per-token costs) — the `max` of is then `0.0`.
        let route = reduce_litellm_route("r", &[deployment("x"), deployment("y")]);
        assert_eq!(route.cost_per_mtok_in, 0.0);
        assert_eq!(route.cost_per_mtok_out, 0.0);
    }

    #[test]
    fn reduce_litellm_route_hides_thinking_when_not_all_deployments_reason() {
        // Subissue 6: require ALL deployments of a route to support reasoning
        // before exposing thinking controls.
        let mut a = deployment("dep-a");
        a.supports_reasoning = Some(true);
        let mut b = deployment("dep-b");
        b.supports_reasoning = Some(false);
        assert!(!reduce_litellm_route("r", &[a.clone(), b]).supports_thinking);
        // An absent (`None`) marker is ALSO a gate failure (conservative).
        assert!(!reduce_litellm_route("r", &[a.clone(), deployment("dep-c")]).supports_thinking);
        // ALL-true is the only pass.
        let mut c = deployment("dep-c");
        c.supports_reasoning = Some(true);
        assert!(reduce_litellm_route("r", &[a, c]).supports_thinking);
    }

    #[test]
    fn reduce_litellm_route_intersects_thinking_levels() {
        // The INTERSECTION over the deployments that advertise levels (order
        // follows the first advertiser).
        let mut a = deployment("dep-a");
        a.thinking_levels = Some(vec!["none".into(), "low".into()]);
        let mut b = deployment("dep-b");
        b.thinking_levels = Some(vec!["low".into(), "medium".into()]);
        assert_eq!(
            reduce_litellm_route("r", &[a.clone(), b]).thinking_levels,
            vec!["low".to_string()]
        );
        // A deployment advertising NO levels does not empty the intersection
        // (the empty-set soft-pass stays).
        assert_eq!(
            reduce_litellm_route("r", &[a, deployment("dep-c")]).thinking_levels,
            vec!["none".to_string(), "low".to_string()]
        );
        // All-`None` → empty (the existing soft-pass in
        // `supports_thinking_level` keeps the status quo).
        assert!(reduce_litellm_route("r", &[deployment("x")])
            .thinking_levels
            .is_empty());
    }

    #[test]
    fn reduce_litellm_route_gates_tools_on_all_deployments() {
        // A route ANY deployment lacks `supports_function_calling` on exposes
        // `supports_tools == false` (ALL-deployments gate; `None` → `false`).
        let mut a = deployment("dep-a");
        a.supports_function_calling = Some(true);
        let mut b = deployment("dep-b");
        b.supports_function_calling = Some(false);
        assert!(!reduce_litellm_route("r", &[a.clone(), b]).supports_tools);
        assert!(!reduce_litellm_route("r", &[a.clone(), deployment("dep-c")]).supports_tools);
        let mut c = deployment("dep-c");
        c.supports_function_calling = Some(true);
        assert!(reduce_litellm_route("r", &[a, c]).supports_tools);
    }

    #[test]
    fn derive_wire_responses_when_all_mode_responses() {
        let mut a = deployment("dep-a");
        a.mode = Some("responses".into());
        let mut b = deployment("dep-b");
        b.mode = Some("responses".into());
        assert_eq!(derive_wire_for_route(&[a, b]), WireApi::OpenAiResponses);
    }

    #[test]
    fn derive_wire_messages_when_all_claude_adapters_and_probe() {
        // Every route hint names a Claude adapter AND every probe latch is
        // true → Messages.
        let mut a = deployment("dep-a");
        a.route_hint = Some("anthropic".into());
        a.messages_available = Some(true);
        let mut b = deployment("dep-b");
        b.route_hint = Some("vertex_ai".into());
        b.messages_available = Some(true);
        assert_eq!(derive_wire_for_route(&[a, b]), WireApi::AnthropicMessages);
    }

    #[test]
    fn derive_wire_falls_back_to_completions_on_any_dissent() {
        // One `responses` deployment among a `chat` peer → completions (the
        // route never sends a wire half its peers reject).
        let mut a = deployment("dep-a");
        a.mode = Some("responses".into());
        let mut b = deployment("dep-b");
        b.mode = Some("chat".into());
        assert_eq!(derive_wire_for_route(&[a, b]), WireApi::OpenAiCompletions);
        // No probe info at all → completions (the Messages rung needs the
        // latch, not just the adapter name).
        let mut c = deployment("dep-c");
        c.route_hint = Some("anthropic".into());
        let mut d = deployment("dep-d");
        d.route_hint = Some("openai".into());
        d.messages_available = Some(true);
        assert_eq!(
            derive_wire_for_route(&[c, d.clone()]),
            WireApi::OpenAiCompletions
        );
        // A Messages-capable pair with a 405/MISS latch → completions.
        let mut e = deployment("dep-e");
        e.route_hint = Some("bedrock".into());
        e.messages_available = Some(true);
        let mut f = deployment("dep-f");
        f.route_hint = Some("bedrock".into());
        f.messages_available = Some(false);
        assert_eq!(derive_wire_for_route(&[e, f]), WireApi::OpenAiCompletions);
    }

    #[test]
    fn derive_wire_prefers_current_deployments_offer() {
        // The all-agree rule is PRIMARY; the `current` deployment's offer is
        // the TIEBREAK when the route disagrees.
        let mut a = deployment("dep-a");
        a.mode = Some("responses".into());
        a.current = true;
        let mut b = deployment("dep-b");
        b.mode = Some("chat".into());
        assert_eq!(derive_wire_for_route(&[a, b]), WireApi::OpenAiResponses);
        // The same mixed route WITHOUT a `current` marker → completions.
        let mut a = deployment("dep-a");
        a.mode = Some("responses".into());
        let mut b = deployment("dep-b");
        b.mode = Some("chat".into());
        assert_eq!(derive_wire_for_route(&[a, b]), WireApi::OpenAiCompletions);
        // A `current` Messages offer still needs the probe latch.
        let mut c = deployment("dep-c");
        c.route_hint = Some("anthropic".into());
        c.current = true;
        let mut d = deployment("dep-d");
        d.mode = Some("chat".into());
        assert_eq!(
            derive_wire_for_route(&[c, d.clone()]),
            WireApi::OpenAiCompletions
        );
        let mut e = deployment("dep-e");
        e.route_hint = Some("anthropic".into());
        e.messages_available = Some(true);
        e.current = true;
        assert_eq!(derive_wire_for_route(&[e, d]), WireApi::AnthropicMessages);
    }

    #[test]
    fn reduce_litellm_route_empty_deployment_set_is_all_conservative() {
        // The vacuous-ALL trap: an empty set must NOT read as "all agree".
        let route = reduce_litellm_route("r", &[]);
        assert!(!route.supports_thinking);
        assert!(!route.supports_tools);
        assert_eq!(route.context_window, None);
        assert_eq!(route.derived_wire, WireApi::OpenAiCompletions);
    }

    // ── the fallback LADDER (path-aware; wiremock) ──────────────────────
    //
    // `raw_json_server` above serves its fixed body to ANY request — it is
    // path- and header-blind, so it cannot express a ladder. These tests use
    // wiremock (the repo's convention for path-mocked HTTP, `tests/provider.rs`)
    // and assert WHICH path answered.

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Serve `body` with `status` for one exact request path.
    async fn mount(server: &MockServer, request_path: &str, status: u16, body: &str) {
        Mock::given(method("GET"))
            .and(path(request_path))
            .respond_with(
                ResponseTemplate::new(status).set_body_raw(body.to_string(), "application/json"),
            )
            .mount(server)
            .await;
    }

    /// Serve `body` with `status` for a request carrying `litellm_model_id=id`
    /// (the rung-4 probes — `path` alone strips the query, so the id must be
    /// matched as a query param).
    async fn mount_probe(server: &MockServer, id: &str, status: u16, body: &str) {
        Mock::given(method("GET"))
            .and(path("/model/info"))
            .and(query_param("litellm_model_id", id))
            .respond_with(
                ResponseTemplate::new(status).set_body_raw(body.to_string(), "application/json"),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn litellm_ladder_falls_to_v1_models_on_403() {
        // A NON-admin key cannot read `/model/info`; the OpenAI-shaped
        // `/v1/models` answers instead, and the Bearer header rode BOTH rungs.
        let server = MockServer::start().await;
        mount(&server, "/model/info", 403, r#"{"error":"forbidden"}"#).await;
        mount(
            &server,
            "/v1/models",
            200,
            r#"{"data":[{"id":"gpt-5.5","max_model_len":400000}]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "sk-key", "litellm", None)
            .await
            .unwrap();
        assert_eq!(models["gpt-5.5"].context_window, Some(400000));
        // The ladder's two rungs were BOTH hit (the fall-DOWN happened).
        let requests = server.received_requests().await.unwrap();
        let paths: Vec<&str> = requests.iter().map(|r| r.url.path()).collect();
        assert_eq!(paths, vec!["/model/info", "/v1/models"]);
        for r in &requests {
            let auth = r
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_eq!(
                auth, "Bearer sk-key",
                "the Bearer header must ride every rung"
            );
        }
    }

    #[tokio::test]
    async fn litellm_ladder_treats_empty_model_info_as_fallback() {
        // A 2xx `{"data":[]}` is NOT a terminal success: it falls DOWN (the
        // pre-ladder code cached it as an empty `succeeded = true`).
        let server = MockServer::start().await;
        mount(&server, "/model/info", 200, r#"{"data":[]}"#).await;
        mount(
            &server,
            "/v1/models",
            200,
            r#"{"data":[{"id":"m/1","max_model_len":1000}]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models["m/1"].context_window, Some(1000));
        let paths: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.path().to_string())
            .collect();
        assert_eq!(paths, vec!["/model/info", "/v1/models"]);
    }

    #[tokio::test]
    async fn litellm_ladder_groups_multi_deployment_model_names() {
        // ONE `/model/info` body, TWO entries sharing a `model_name` → ONE
        // reduced route (the old seam last-won; the deployment set must survive).
        let server = MockServer::start().await;
        mount(
            &server,
            "/model/info",
            200,
            r#"{"data":[
                {"model_name":"shared","model_info":{"id":"dep-a","max_input_tokens":128000}},
                {"model_name":"shared","model_info":{"id":"dep-b","max_input_tokens":64000}}
            ]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        assert_eq!(
            models.len(),
            1,
            "one route, not one map entry per deployment"
        );
        // `min` — the route never overestimates its window.
        assert_eq!(models["shared"].context_window, Some(64000));
        // Rung 1 answered → exactly ONE request (a well-behaved proxy costs ≤1).
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn litellm_ladder_uses_v1_model_info_when_unprefixed_is_forbidden() {
        // Rung 3: a proxy that 403s the unprefixed `/model/info` but serves the
        // `/v1`-prefixed one (LiteLLM answers BOTH paths).
        let server = MockServer::start().await;
        mount(&server, "/model/info", 403, r#"{"error":"forbidden"}"#).await;
        mount(&server, "/v1/models", 403, r#"{"error":"forbidden"}"#).await;
        mount(
            &server,
            "/v1/model/info",
            200,
            r#"{"data":[{"model_name":"lit/3","model_info":{"max_input_tokens":3000}}]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        assert_eq!(models["lit/3"].context_window, Some(3000));
        let paths: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.path().to_string())
            .collect();
        assert_eq!(paths, vec!["/model/info", "/v1/models", "/v1/model/info"]);
    }

    #[tokio::test]
    async fn litellm_ladder_probes_health_and_model_id_when_all_lists_fail() {
        // Rungs 1–3 give no selectable route but DO mention a deployment id (an
        // entry with no `model_name` to select it by, carrying a
        // `litellm_model_id`) → `/health` gates, the probe answers → `Ok`.
        let server = MockServer::start().await;
        // Mounted FIRST: wiremock's first-registered match wins and `path`
        // ignores the query, so the plain rung-1 mount below would otherwise
        // swallow the probe request.
        mount_probe(
            &server,
            "dep-7",
            200,
            r#"{"data":[{"model_name":"probed/model","model_info":{"max_input_tokens":7777}}]}"#,
        )
        .await;
        mount(
            &server,
            "/model/info",
            200,
            r#"{"data":[{"model_name":"","model_info":{"litellm_model_id":"dep-7"}}]}"#,
        )
        .await;
        mount(&server, "/v1/models", 200, r#"{"data":[]}"#).await;
        mount(&server, "/v1/model/info", 404, r#"{"detail":"Not Found"}"#).await;
        mount(&server, "/health", 200, r#"{"status":"healthy"}"#).await;
        let models = discover_models(&server.uri(), "sk-key", "litellm", None)
            .await
            .unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models["probed/model"].context_window, Some(7777));
        let requests = server.received_requests().await.unwrap();
        let paths: Vec<&str> = requests.iter().map(|r| r.url.path()).collect();
        assert!(
            paths.contains(&"/health"),
            "the liveness gate must run: {paths:?}"
        );
        assert!(
            requests.iter().any(|r| r.url.path() == "/model/info"
                && r.url.query() == Some("litellm_model_id=dep-7")),
            "the probe must carry the deployment id: {:?}",
            requests
                .iter()
                .map(|r| r.url.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn litellm_ladder_all_empty_chain_is_err() {
        // Every rung (including `/health`) fails → `Err` (the cache-visible
        // failure: `succeeded = false`, retried on the next read).
        let server = MockServer::start().await;
        mount(&server, "/model/info", 500, r#"{"error":"boom"}"#).await;
        mount(&server, "/v1/models", 500, r#"{"error":"boom"}"#).await;
        mount(&server, "/v1/model/info", 500, r#"{"error":"boom"}"#).await;
        mount(&server, "/health", 503, r#"{"error":"down"}"#).await;
        assert!(discover_models(&server.uri(), "k", "litellm", None)
            .await
            .is_err());
        let paths: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.path().to_string())
            .collect();
        assert_eq!(
            paths,
            vec!["/model/info", "/v1/models", "/v1/model/info", "/health"]
        );
    }

    #[tokio::test]
    async fn litellm_health_only_chain_is_an_empty_success_not_an_err() {
        // A proxy answering NOTHING but `/health`, with no deployment id ever
        // mentioned: "the endpoint answered and had nothing" is a real answer
        // (ADR 0076 rule 3) → an EMPTY `Ok` that caches, not a retry forever.
        let server = MockServer::start().await;
        mount(&server, "/model/info", 200, r#"{"data":[]}"#).await;
        mount(&server, "/v1/models", 200, r#"{"data":[]}"#).await;
        mount(&server, "/v1/model/info", 200, r#"{"data":[]}"#).await;
        mount(&server, "/health", 200, r#"{"status":"healthy"}"#).await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .expect("a live proxy with nothing to serve is an empty SUCCESS");
        assert!(models.is_empty());
        // No probe ran (nothing was mentioned to probe for).
        assert!(!server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|r| r.url.query().is_some()));
    }

    #[tokio::test]
    async fn litellm_rung2_wildcard_ids_are_selectable_with_none_reduction_fields() {
        // The wildcard expansion: route-advertised ids (`anthropic/…`) that no
        // `model_name` route carries become individually selectable, with the
        // OpenAI-shape metadata they advertise and `None` for the four
        // reduction fields (the documented v1 degradation).
        let server = MockServer::start().await;
        mount(&server, "/model/info", 403, r#"{"error":"forbidden"}"#).await;
        mount(
            &server,
            "/v1/models",
            200,
            r#"{"data":[
                {"id":"anthropic/claude-3-7-sonnet","max_model_len":200000},
                {"id":"anthropic/claude-opus-4"},
                {"id":"gpt-5.5","max_model_len":400000}
            ]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        let mut keys: Vec<&str> = models.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "anthropic/claude-3-7-sonnet",
                "anthropic/claude-opus-4",
                "gpt-5.5"
            ]
        );
        // The provider-qualified form is preserved (the picker's keying is unchanged).
        assert_eq!(
            models["anthropic/claude-3-7-sonnet"].context_window,
            Some(200000)
        );
        // The reduction's truth is NOT fabricated for the OpenAI shape.
        for meta in models.values() {
            assert_eq!(meta.api, None);
            assert_eq!(meta.supports_tools, None);
            assert_eq!(meta.cost_per_mtok_in, None);
            assert_eq!(meta.cost_per_mtok_out, None);
        }
    }

    #[tokio::test]
    async fn litellm_unknown_keys_do_not_break_the_parse() {
        // The presence guard: a proxy ADDING a key must never take the whole
        // route down — at the entry level the `#[serde(flatten)] extras` map
        // absorbs it, inside `model_info` serde's default-ignorant shape does.
        let server = MockServer::start().await;
        mount(
            &server,
            "/model/info",
            200,
            r#"{"data":[{"model_name":"lit/new","litellm_tag_from_the_future":7,"model_info":{
                "max_input_tokens":5000,"a_key_from_the_future":{"nested":true}
            }}]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        assert_eq!(models["lit/new"].context_window, Some(5000));
    }

    #[tokio::test]
    async fn litellm_derived_wire_and_costs_land_on_the_meta() {
        // The pure reduction's truth rides the meta on the REAL path (rung 1):
        // all-`responses` mode → the derived wire is `openai-responses`, and the
        // `max` cost lands on `cost_per_mtok_out`. T3b's resolve write is the
        // terminal consumer — nothing here asserts a `Model`-level effect.
        let server = MockServer::start().await;
        mount(
            &server,
            "/model/info",
            200,
            r#"{"data":[
                {"model_name":"r/1","model_info":{
                    "id":"d-a","mode":"responses","max_input_tokens":1000,
                    "supports_function_calling":true,
                    "input_cost_per_token":0.0000025,"output_cost_per_token":0.000006}},
                {"model_name":"r/1","model_info":{
                    "id":"d-b","mode":"responses","max_input_tokens":2000,
                    "supports_function_calling":true,"output_cost_per_token":0.000001}}
            ]}"#,
        )
        .await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        let meta = &models["r/1"];
        assert_eq!(meta.api, Some(WireApi::OpenAiResponses));
        assert_eq!(meta.supports_tools, Some(true));
        assert_eq!(meta.cost_per_mtok_in, Some(2.5));
        // `max` over the deployments (0.000006 × 1e6), NOT the last entry's 1.0.
        assert_eq!(meta.cost_per_mtok_out, Some(6.0));
        assert_eq!(meta.context_window, Some(1000));
    }

    #[tokio::test]
    async fn litellm_non_claude_proxy_never_probes_messages() {
        // The `/v1/messages` latch costs ONE request per root per process and
        // only for a payload that names a Claude-family adapter — an all-OpenAI
        // proxy pays nothing for it.
        let server = MockServer::start().await;
        mount(
            &server,
            "/model/info",
            200,
            r#"{"data":[{"model_name":"gpt-5.5","model_info":{"max_input_tokens":100}}]}"#,
        )
        .await;
        discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        let paths: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| r.url.path().to_string())
            .collect();
        assert_eq!(paths, vec!["/model/info"]);
    }

    #[tokio::test]
    async fn litellm_claude_adapter_probe_latches_the_messages_wire() {
        // Every deployment names a Claude adapter AND the probe answers → the
        // derived wire is Messages (COMPUTED + carried; T3b applies it).
        let server = MockServer::start().await;
        mount(
            &server,
            "/model/info",
            200,
            r#"{"data":[{"model_name":"anthropic/claude-3-7-sonnet","model_info":{
                "id":"d-1","supports_function_calling":true}}]}"#,
        )
        .await;
        mount(&server, "/v1/messages", 200, r#"{"error":{}}"#).await;
        let models = discover_models(&server.uri(), "k", "litellm", None)
            .await
            .unwrap();
        assert_eq!(
            models["anthropic/claude-3-7-sonnet"].api,
            Some(WireApi::AnthropicMessages)
        );
        let requests = server.received_requests().await.unwrap();
        let probe = requests
            .iter()
            .find(|r| r.url.path() == "/v1/messages")
            .expect("the Claude-adapter route must be probed");
        // `anthropic-version` is REQUIRED on every Anthropic endpoint (without
        // it a real `api.anthropic.com` 400s the probe).
        assert!(probe.headers.contains_key("anthropic-version"));
        assert!(probe.headers.contains_key("x-api-key"));
    }
}

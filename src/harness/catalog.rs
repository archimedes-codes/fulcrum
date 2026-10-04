//! The model catalog. The desktop's model source is the Settings'
//! providers list (ADR 0014 — user providers shadow the base catalog on
//! a provider-id clash) merged with live per-provider discovery
//! (`GET /v1/models`). The catalog is the source for the model picker
//! and the [`Provider`] construction (a [`Model`]'s `api` discriminator
//! → `build_provider` (ADR 0024)).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
                    && matches!(
                        m.api.as_deref(),
                        Some("openai-completions")
                            | Some("anthropic-messages")
                            | Some("openai-responses")
                            | Some("litellm")
                    )
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
    let default_model = base.default_model.clone().filter(|key| {
        models
            .iter()
            .any(|m| format!("{}/{}", m.provider, m.id) == key.as_str())
    });
    ModelCatalog {
        models,
        default_model,
        compaction: base.compaction,
    }
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

/// Fresh model metadata from a live `GET /v1/models` (the OpenAI endpoint
/// "supplies everything" — the `pi-provider-litellm` `fetchModels` pattern).
/// All fields `Option`: a field absent in the response keeps the static
/// value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DiscoveredMeta {
    pub context_window: Option<u32>,
    pub thinking_levels: Option<Vec<String>>,
    pub supports_thinking: Option<bool>,
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
/// A non-2xx / network error → `Err` (the caller degrades to the static
/// metadata).
pub async fn discover_models(
    base_url: &str,
    api_key: &str,
    api: &str,
) -> Result<HashMap<String, DiscoveredMeta>, String> {
    if api == "anthropic-messages" {
        return discover_anthropic_models(base_url, api_key).await;
    }
    if api == "litellm" {
        return discover_litellm_models(base_url, api_key).await;
    }
    // The OpenAI shape (`openai-completions` / `openai-responses` /
    // anything else — the existing behavior verbatim).
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
        .header("anthropic-version", "2023-06-01");
    // `x-api-key` (NOT `Authorization: Bearer` — the Anthropic API's
    // auth header); a keyless call → NO `x-api-key` header (the
    // existing empty-key rule).
    if !api_key.is_empty() {
        req = req.header("x-api-key", api_key);
    }
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

/// The LiteLLM `GET {base_url}/model/info` (ADR 0026): `model_name` → the
/// map key; `model_info.max_input_tokens` (fallback `max_output_tokens`) →
/// `context_window`; `reasoning_effort_levels` → `thinking_levels`
/// (VERBATIM); `supports_reasoning` → `supports_thinking`.
/// `default_reasoning_effort` / `supports_function_calling` / the cost
/// fields are parsed-by-omission (ignored for v1 — see the ADR's
/// Considered Options). Bearer auth when the key is non-empty (empty key →
/// no header); same 5s/10s bounded client; non-2xx / network error →
/// `Err`. NO base-URL normalization (LiteLLM serves both `/v1/model/info`
/// and `/model/info` — the caller passes the base as configured).
async fn discover_litellm_models(
    base_url: &str,
    api_key: &str,
) -> Result<HashMap<String, DiscoveredMeta>, String> {
    let url = format!("{}/model/info", base_url.trim_end_matches('/'));
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
    let body: LiteLLMModelInfoResponse = resp.json().await.map_err(|e| e.to_string())?;
    Ok(body
        .data
        .into_iter()
        .map(|entry| {
            (
                entry.model_name,
                entry
                    .model_info
                    .map(|mi| DiscoveredMeta {
                        // `max_input_tokens` → the context window (the
                        // `max_output_tokens` fallback keeps a
                        // `max_output_tokens`-only entry from losing its
                        // window entirely). An OUTPUT cap, not a total window
                        // — so it UNDER-sizes the budget (safe; over-sizing
                        // risks provider 400s), for the rare absent shape.
                        context_window: mi.max_input_tokens.or(mi.max_output_tokens),
                        thinking_levels: mi.reasoning_effort_levels,
                        supports_thinking: mi.supports_reasoning,
                    })
                    .unwrap_or_default(),
            )
        })
        .collect())
}

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let models = discover_models(&format!("http://{addr}/v1"), "test-key", "openai-responses")
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
        let models = discover_models(&format!("http://{addr}/v1"), "test-key", "litellm")
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
        let models = discover_models(&format!("http://{addr}/v1"), "test-key", "litellm")
            .await
            .unwrap();
        assert_eq!(models["lit/o"].context_window, Some(999));
        // No reasoning fields advertised → all `None`.
        assert_eq!(models["lit/o"].thinking_levels, None);
        assert_eq!(models["lit/o"].supports_thinking, None);
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
            discover_models(&format!("http://{addr}/v1"), "test-key", "litellm")
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
            "openai-completions"
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
            .map(|m| format!("{}/{}", m.provider, m.id))
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
            .map(|m| format!("{}/{}", m.provider, m.id))
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
}

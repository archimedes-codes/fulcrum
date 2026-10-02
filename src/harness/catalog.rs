//! The model catalog. The desktop's model source is the Settings'
//! providers list (ADR 0014 — user providers shadow the base catalog on
//! a provider-id clash) merged with live per-provider discovery
//! (`GET /v1/models`). The catalog is the source for the model picker
//! and the [`Provider`] construction (a [`Model`]'s `provider` +
//! `base_url` + `api_key` → an `OpenAiCompatibleProvider`).

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
    /// `true` when the model speaks the OpenAI-compatible
    /// chat-completions API (`api == "openai-completions"` — the only
    /// tool-carrying wire in v1, ADR 0014).
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

    /// The selectable set (ADR 0014): the tool-supporting,
    /// OpenAI-compatible models (`api == "openai-completions"`).
    pub fn openai_compatible(&self) -> Vec<&Model> {
        self.models
            .iter()
            .filter(|m| m.supports_tools && m.api.as_deref() == Some("openai-completions"))
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

/// Query a provider's `GET /v1/models` for fresh model metadata (bounded,
/// best-effort). `base_url` is `.../v1` (the endpoint is `{base_url}/models`).
/// A non-2xx / network error → `Err` (the caller degrades to the static
/// metadata). The `api_key` is sent as `Authorization: Bearer` when
/// non-empty (a local gateway needing no key → no header).
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

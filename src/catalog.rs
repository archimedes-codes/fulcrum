//! The model-catalog DATA types (T5/S4 split of the desktop's `harness/catalog.rs`).
//!
//! ## Why this module exists, and why it is not the whole catalog
//!
//! Step E.4 moves `resolve_composed_model` out of the desktop's `session/mod.rs`
//! into the crate, because it is the one crate→desktop edge (E4) that a seam
//! would be silly for: the function is a pure lookup, `ModelKey::parse` plus a
//! `find` over a vec, with no desktop dependency in its body. But a package
//! boundary checks SIGNATURES, not bodies — and its signature is
//! `(catalog: &ModelCatalog, key: &str) -> Option<Model>`. So moving the function
//! moves its two argument types with it, and `Model`/`ModelCatalog` reach for
//! `ModelKey` (their `From` impl) and `WireApi` (the `selectable` predicate),
//! which is what pulled those two files across too.
//!
//! What did NOT come is `discover_models` and the LiteLLM discovery ladder: those
//! call the desktop's provider transport
//! (`super::provider::transport::apply_custom_headers`,
//! `provider::normalize_anthropic_base_url`, `ANTHROPIC_API_VERSION`), so moving
//! them would drag the provider HTTP stack in — S5's business, not S4's. The
//! desktop's `harness/catalog.rs` therefore SURVIVES as the discovery half and
//! imports these types back, exactly the split §2c applied to `config.rs`
//! (data moves, I/O stays). The desktop file re-exports them, so all 48 existing
//! `harness::catalog::*` / `harness::Model` / `harness::WireApi` paths and the
//! `harness/mod.rs` re-export block keep resolving with zero edits.
//!
//! `ProviderDiscovery` and `DiscoveredMeta` are here because `DiscoveredMeta` is
//! the return type of the discovery call that stays desktop and the field type of
//! the cache that stays desktop — a data type either way, and `api: Option<WireApi>`
//! makes it depend on the moved `WireApi` regardless.
//!
//! `merge_catalog` is the one FUNCTION here. It is pure (ADR 0014's shadowing
//! rule over two vecs), it is called by the desktop's `EffectiveCatalog::resolve`
//! through the re-export, and splitting a 40-line pure function from the types it
//! returns would buy nothing.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model_key::ModelKey;
use crate::wire::WireApi;

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
/// Resolve a composed model key (`"<provider>/<id>"` — the catalog / config
/// option key form) to a `Model` (the first `/` split — model ids themselves
/// contain `/`). `None` when the key is malformed or unknown.
///
/// (T5/S4, plan Step E.3 / edge E4) This moved from the desktop's
/// `session/mod.rs`, where it was `pub(crate)`, to sever the crate→desktop edge at
/// `harness/launch.rs:173` — the ONLY harness call site. It is the one seam edge
/// the plan resolved by MOVING rather than by injecting: the body is a parse and a
/// `find`, with no desktop reference anywhere in it, so a trait for it would be an
/// abstraction with one implementation for a dependency that stops existing the
/// moment the function is where its caller is.
///
/// `pub`, not `pub(crate)`: a package boundary erases `pub(crate)` (Step A.3), and
/// the STAYING desktop names this function — `session/mod.rs`'s own resolution
/// chain and `worker/manager/subagent_flow.rs:14` both import it. The desktop
/// re-exports it at its old path, so nothing there changed.
///
/// The `:<level>` suffix is stripped by the CALLER before this is called (`ModelRef::parse`)
/// — this function does not split it.
pub fn resolve_composed_model(catalog: &ModelCatalog, key: &str) -> Option<Model> {
    let key = ModelKey::parse(key)?;
    catalog
        .models
        .iter()
        .find(|m| m.provider == key.provider && m.id == key.id)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::{resolve_composed_model, Model, ModelCatalog};

    fn model(provider: &str, id: &str) -> Model {
        Model {
            id: id.to_string(),
            provider: provider.to_string(),
            base_url: String::new(),
            api_key: String::new(),
            context_window: 1000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }

    fn catalog(models: Vec<Model>) -> ModelCatalog {
        ModelCatalog {
            models,
            ..Default::default()
        }
    }

    /// E4's behaviour pin, kept in the crate so it MOVES with the function (the
    /// S1–S3 lesson: a desktop-side test of moved code keeps passing while the
    /// moved copy is subtly wrong). The load-bearing case is the id that contains
    /// `/` — the reason the split is on the FIRST slash and not the last.
    #[test]
    fn resolve_composed_model_splits_on_the_first_slash_only() {
        let c = catalog(vec![model("tama", "Qwen/Qwen3.8-27B")]);
        let found = resolve_composed_model(&c, "tama/Qwen/Qwen3.8-27B").expect("resolves");
        assert_eq!(found.provider, "tama");
        assert_eq!(found.id, "Qwen/Qwen3.8-27B");
    }

    #[test]
    fn resolve_composed_model_is_none_for_a_malformed_or_unknown_key() {
        let c = catalog(vec![model("tama", "m-1")]);
        // No `/` at all is not a composed key.
        assert!(resolve_composed_model(&c, "m-1").is_none());
        // A well-formed key naming no model.
        assert!(resolve_composed_model(&c, "tama/other").is_none());
        // The provider is part of the identity, not a hint.
        assert!(resolve_composed_model(&c, "other/m-1").is_none());
    }

    /// The `:<level>` suffix is NOT this function's business — the caller strips
    /// it via `ModelRef::parse`. Pinned because `harness/launch.rs:173` relies on
    /// that division of labour: passing the suffixed string here must MISS rather
    /// than half-match, which is what makes the soft rung skip rather than guess.
    #[test]
    fn resolve_composed_model_does_not_strip_a_thinking_level_suffix() {
        let c = catalog(vec![model("tama", "m-1")]);
        assert!(resolve_composed_model(&c, "tama/m-1:high").is_none());
    }
}

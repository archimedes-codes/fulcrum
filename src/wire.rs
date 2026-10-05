//! The provider wire vocabulary (ADR 0024 + ADR 0026).

use serde::{Deserialize, Serialize};

/// The provider wire vocabulary (ADR 0024 + ADR 0026). Four known values:
/// the three wires the harness speaks + the `litellm` DISCOVERY MODE (its
/// wire is `openai-completions`). Unknown strings / `None` (a
/// metadata-less `Model.api`) are NOT representable — they stay
/// permissive `String`s at the boundary and `parse` to `None`.
/// Explicit `#[serde(rename)]` per variant (kebab-case would produce
/// `"open-ai-completions"` — WRONG).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum WireApi {
    /// The default wire.
    #[serde(rename = "openai-completions")]
    #[default]
    OpenAiCompletions,
    #[serde(rename = "anthropic-messages")]
    AnthropicMessages,
    #[serde(rename = "openai-responses")]
    OpenAiResponses,
    /// The LiteLLM DISCOVERY mode (ADR 0026, its wire is
    /// `openai-completions`).
    #[serde(rename = "litellm")]
    LiteLLM,
}

impl WireApi {
    /// All four known values.
    pub const ALL: [WireApi; 4] = [
        WireApi::OpenAiCompletions,
        WireApi::AnthropicMessages,
        WireApi::OpenAiResponses,
        WireApi::LiteLLM,
    ];

    /// Parse a stored/wire value; `None` for unknown strings (the
    /// permissive case — `Model.api` may carry any hand-edited value).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "openai-completions" => Some(WireApi::OpenAiCompletions),
            "anthropic-messages" => Some(WireApi::AnthropicMessages),
            "openai-responses" => Some(WireApi::OpenAiResponses),
            "litellm" => Some(WireApi::LiteLLM),
            _ => None,
        }
    }

    /// The exact wire string for this value.
    pub fn as_str(self) -> &'static str {
        match self {
            WireApi::OpenAiCompletions => "openai-completions",
            WireApi::AnthropicMessages => "anthropic-messages",
            WireApi::OpenAiResponses => "openai-responses",
            WireApi::LiteLLM => "litellm",
        }
    }

    /// Selectable set (ADR 0024 + 0026): all four known values.
    pub fn is_supported(self) -> bool {
        Self::ALL.contains(&self)
    }
}

impl From<WireApi> for String {
    fn from(api: WireApi) -> String {
        api.as_str().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_returns_the_exact_wire_strings() {
        assert_eq!(WireApi::OpenAiCompletions.as_str(), "openai-completions");
        assert_eq!(WireApi::AnthropicMessages.as_str(), "anthropic-messages");
        assert_eq!(WireApi::OpenAiResponses.as_str(), "openai-responses");
        assert_eq!(WireApi::LiteLLM.as_str(), "litellm");
    }

    #[test]
    fn parse_round_trips_all_four() {
        for api in WireApi::ALL {
            assert_eq!(WireApi::parse(api.as_str()), Some(api));
        }
    }

    #[test]
    fn parse_rejects_unknown_and_malformed_strings() {
        assert_eq!(WireApi::parse("google-generative-ai"), None);
        // The wrong kebab form (`rename_all = "kebab-case"` would produce
        // this — it is NOT a wire).
        assert_eq!(WireApi::parse("open-ai-completions"), None);
        assert_eq!(WireApi::parse(""), None);
    }

    #[test]
    fn all_four_are_supported() {
        assert!(WireApi::ALL.iter().all(|api| api.is_supported()));
    }

    #[test]
    fn default_is_openai_completions() {
        assert_eq!(WireApi::default(), WireApi::OpenAiCompletions);
    }

    #[test]
    fn from_wire_api_for_string_yields_as_str() {
        for api in WireApi::ALL {
            let s: String = api.into();
            assert_eq!(s, api.as_str());
        }
    }

    #[test]
    fn serde_uses_the_exact_wire_strings() {
        for api in WireApi::ALL {
            let json = serde_json::to_string(&api).unwrap();
            assert_eq!(json, format!("\"{}\"", api.as_str()));
            let back: WireApi = serde_json::from_str(&json).unwrap();
            assert_eq!(back, api);
        }
        // An unknown string does NOT parse (the permissive `String`
        // storage at the boundary keeps those values; `parse` → `None`).
        assert!(serde_json::from_str::<WireApi>("\"google-generative-ai\"").is_err());
    }
}

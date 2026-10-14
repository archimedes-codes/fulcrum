//! Context-overflow DETECTION (ADR 0045): does this provider error text mean
//! "the prompt is over the model's context window"?
//!
//! The classification is a ported pattern table (pi's
//! `packages/ai/src/utils/overflow.ts`), because a context-length rejection
//! arrives as prose: the HTTP status is a plain 400 / 413 (indistinguishable
//! from any other client error) and a mid-stream SSE `error` event carries
//! only its message. ADOPTED as a tier (`ProviderError::ContextOverflow`)
//! rather than a `match` at the decision site — see ADR 0045.

/// The overflow wordings, one per provider phrasing (ported from pi's
/// `OVERFLOW_PATTERNS`, where each carries the provider it was seen on —
/// kept below as documentation, since the fragment alone does not say who
/// emits it). Matching is a lowercased SUBSTRING test, not a regex: `regex`
/// is only a transitive dependency here, and every pattern pi needs is a
/// literal phrase once the text is lowercased.
///
/// - Anthropic: `prompt is too long` ("prompt is too long: 213462 tokens >
///   200000 maximum") and `request_too_large` (the HTTP 413 byte-size
///   rejection).
/// - Amazon Bedrock: `input is too long for requested model`.
/// - z.ai: `prompt too long` / `prompt exceeds max length` (the CN endpoint)
///   / `model_context_window_exceeded` (a non-standard `finish_reason`
///   surfaced as error text). z.ai ALSO accepts some over-window prompts
///   silently and reports it only in `usage` — that half is ADR 0046's job
///   (the cache-inclusive `input_tokens` at the parser seam), NOT this table.
/// - OpenAI (Completions & Responses): `exceeds the context window`.
/// - OpenAI-compatible proxies (LiteLLM) / OpenRouter / Mistral: `maximum
///   context length`, `maximum context length is`.
/// - Google (Gemini): the paired `input token count` + `exceeds the maximum`.
/// - xAI (Grok): `maximum prompt length is`.
/// - Groq: `reduce the length of the messages`.
/// - OpenRouter/Poolside: `exceeds the maximum allowed input length`.
/// - Together AI: `is longer than the model's context length`.
/// - GitHub Copilot: `exceeds the limit of`.
/// - llama.cpp server: `exceeds the available context size`.
/// - LM Studio: `greater than the context length`.
/// - MiniMax: `context window exceeds limit`.
/// - Kimi For Coding: `exceeded model token limit`.
/// - DS4: `configured context size is`.
/// - Ollama: `prompt too long` ("prompt too long; exceeded max context
///   length by X tokens").
/// - DashScope / Qwen Token Plan: `range of input length should be` ("Range
///   of input length should be [1, X]", the HTTP 400 invalid_parameter_error).
/// - Generic fallbacks: `context_length_exceeded`, `too many tokens`,
///   `token limit exceeded`.
const OVERFLOW_FRAGMENTS: &[&str] = &[
    "prompt is too long",                        // Anthropic (token overflow)
    "prompt too long",                           // z.ai / Ollama
    "prompt exceeds max length",                 // z.ai (CN endpoint)
    "request_too_large",                         // Anthropic (HTTP 413 byte-size overflow)
    "input is too long for requested model",     // Amazon Bedrock
    "exceeds the context window",                // OpenAI
    "maximum context length",                    // LiteLLM / OpenRouter / Mistral
    "context_length_exceeded",                   // generic fallback
    "maximum prompt length is",                  // xAI (Grok)
    "reduce the length of the messages",         // Groq
    "maximum context length is",                 // OpenRouter
    "exceeds the maximum allowed input length",  // OpenRouter/Poolside
    "is longer than the model's context length", // Together AI
    "exceeds the limit of",                      // GitHub Copilot
    "exceeds the available context size",        // llama.cpp server
    "greater than the context length",           // LM Studio
    "context window exceeds limit",              // MiniMax
    "exceeded model token limit",                // Kimi For Coding
    "model_context_window_exceeded",             // z.ai
    "too many tokens",                           // generic fallback (see the exclusions!)
    "token limit exceeded",                      // generic fallback
    "configured context size is",                // DS4 server
    "range of input length should be",           // DashScope / Qwen Token Plan
];

/// Google (Gemini): "The input token count (1196265) exceeds the maximum
/// number of tokens allowed (1048575)" — pi anchors the phrase with `.*`, so
/// BOTH halves must be present for it to be an overflow (either half alone is
/// ordinary prose about token counts).
const OVERFLOW_FRAGMENT_PAIRS: &[(&str, &str)] = &[("input token count", "exceeds the maximum")];

/// Wordings that mean "not an overflow", checked BEFORE
/// [`OVERFLOW_FRAGMENTS`] — see [`is_context_overflow_text`] for why the
/// order is the whole point of the list.
///
/// - `rate limit` / `too many requests`: the generic rate-limit shapes.
/// - `throttling error:` / `service unavailable:`: the two human-readable
///   prefixes AWS's Bedrock wrapper produces.
/// - `throttlingexception`: the AWS SDK's own exception name.
///   **This is a deliberate divergence from pi, and it must not be "restored
///   to parity".** pi anchors its exclusion at `/^(Throttling error|Service
///   unavailable):/i`, which does NOT match AWS's
///   `ThrottlingException: Too many tokens, please wait before trying
///   again.` — one of pi's OWN documented examples. Since our fragments
///   include the `too many tokens` fallback, that string would classify as an
///   overflow there and a Bedrock throttle would trigger a pointless
///   compaction; adding the exception NAME here (unanchored, like every other
///   fragment) closes it.
const NON_OVERFLOW_FRAGMENTS: &[&str] = &[
    "rate limit",
    "too many requests",
    "throttling error:",
    "throttlingexception",
    "service unavailable:",
];

/// True when provider error text means "the prompt is over the model's
/// context window".
///
/// The EXCLUSIONS run FIRST: a Bedrock throttle says "too many tokens", and
/// turning a rate limit into a compaction is worse than missing an overflow —
/// the false positive throws away conversation history the next call still
/// needed, while the false negative leaves the turn exactly as it is today
/// (a settled error the user can continue from).
///
/// Matching is on the LOWERCASED text (every provider capitalizes
/// differently, and the wire embeds the JSON body verbatim), so every
/// fragment below is lowercase.
pub(crate) fn is_context_overflow_text(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    if NON_OVERFLOW_FRAGMENTS.iter().any(|f| text.contains(f)) {
        return false;
    }
    OVERFLOW_FRAGMENTS.iter().any(|f| text.contains(f))
        || OVERFLOW_FRAGMENT_PAIRS
            .iter()
            .any(|(head, tail)| text.contains(head) && text.contains(tail))
}

#[cfg(test)]
mod tests {
    use super::is_context_overflow_text;

    /// Every pi-documented overflow example classifies `true` — the table is
    /// only worth porting if the real provider strings land in it.
    #[test]
    fn every_provider_overflow_wording_is_detected() {
        for text in [
            // Anthropic: token overflow + request byte-size overflow (HTTP 413).
            "prompt is too long: 213462 tokens > 200000 maximum",
            "413 {\"error\":{\"type\":\"request_too_large\",\"message\":\"Request exceeds the maximum size\"}}",
            // OpenAI (Completions & Responses).
            "Your input exceeds the context window of this model",
            // OpenAI / LiteLLM proxy.
            "Requested token count exceeds the model's maximum context length of 131072 tokens",
            // OpenAI-compatible proxy.
            "Input length (265330) exceeds model's maximum context length (262144).",
            // Google (Gemini) — BOTH halves of the phrase are required.
            "The input token count (1196265) exceeds the maximum number of tokens allowed (1048575)",
            // xAI (Grok).
            "This model's maximum prompt length is 131072 but the request contains 537812 tokens",
            // Groq.
            "Please reduce the length of the messages or completion",
            // llama.cpp server.
            "the request exceeds the available context size, try increasing it",
            // z.ai (CN endpoint).
            "Prompt exceeds max length",
            // z.ai.
            "{\"code\":\"1261\",\"message\":\"Prompt too long\"}",
            // z.ai: a non-standard `finish_reason` surfaced as error text.
            "model_context_window_exceeded",
            // GitHub Copilot.
            "prompt token count of 250000 exceeds the limit of 200000",
            // MiniMax.
            "context window exceeds limit",
        ] {
            assert!(
                is_context_overflow_text(text),
                "an overflow error must be detected: {text}"
            );
        }
    }

    /// The fragments the examples above do not reach each get one pi-documented
    /// wording, so no entry in the table is unpinned (an entry nobody tests can
    /// be deleted by accident).
    #[test]
    fn every_table_fragment_has_a_pinning_example() {
        for text in [
            // Generic fallback.
            "{\"error\":{\"code\":\"context_length_exceeded\"}}",
            // OpenRouter / Poolside.
            "Input length 265330 exceeds the maximum allowed input length of 262144 tokens.",
            // Together AI.
            "The input (265330 tokens) is longer than the model's context length (262144 tokens).",
            // LM Studio.
            "tokens to keep from the initial prompt is greater than the context length",
            // Kimi For Coding.
            "Your request exceeded model token limit: 131072 (requested: 200000)",
            // Generic fallback.
            "Token limit exceeded for this model",
            // DS4 server.
            "Prompt has 300000 tokens, but the configured context size is 131072 tokens",
            // OpenRouter (most backends).
            "This endpoint's maximum context length is 131072 tokens. However, you requested about 200000 tokens",
            // Mistral.
            "Prompt contains 300000 tokens, too large for model with 131072 maximum context length",
            // Ollama explicit overflow.
            "prompt too long; exceeded max context length by 1024 tokens",
        ] {
            assert!(
                is_context_overflow_text(text),
                "an overflow error must be detected: {text}"
            );
        }
    }

    /// The two wordings pi attributes to Amazon Bedrock and DashScope/Qwen
    /// (`input is too long for requested model`, `range of input length should
    /// be`) are plain prose, so they need a fragment of their own — the table
    /// is only worth porting if the providers pi documented are all in it.
    #[test]
    fn bedrock_and_dashscope_overflow_wording_is_detected() {
        for text in [
            // Amazon Bedrock.
            "Input is too long for requested model.",
            // DashScope / Qwen (HTTP 400 invalid_parameter_error).
            "Range of input length should be [1, 30000]",
        ] {
            assert!(
                is_context_overflow_text(text),
                "an overflow error must be detected: {text}"
            );
        }
    }

    /// A rate limit / throttle is NEVER an overflow, even when its wording
    /// collides with a fragment — and the exclusions win because they are
    /// checked FIRST. The headline case is AWS's
    /// `ThrottlingException: Too many tokens, please wait before trying
    /// again.`, which says "too many tokens" while meaning "slow down":
    /// misreading it spends a compaction on a request that was perfectly
    /// sized (and destroys context the next call needs).
    #[test]
    fn a_throttle_or_rate_limit_is_never_an_overflow() {
        for text in [
            // Bedrock's throttle wording (the reason `throttlingexception` is
            // in the exclusion list — see the doc comment on the table).
            "ThrottlingException: Too many tokens, please wait before trying again.",
            // The two prefixes pi anchors on.
            "Throttling error: Too many tokens, please wait before trying again.",
            "Service unavailable: too many tokens",
            // Generic rate limiting that also names a fragment.
            "rate limit: token limit exceeded per minute",
            "429 Too Many Requests: too many tokens",
        ] {
            assert!(
                !is_context_overflow_text(text),
                "a throttle / rate limit is not a context overflow: {text}"
            );
        }
    }

    /// Unrelated errors (auth, a malformed request, a 5xx) stay out of the
    /// tier — an overflow triggers a compaction, so a false positive here is
    /// a rewrite of the user's transcript for nothing.
    #[test]
    fn an_unrelated_error_is_not_an_overflow() {
        for text in [
            "invalid api key",
            "{\"detail\":\"field required\"}",
            "500 Internal Server Error",
            "the model call failed",
            // Near-misses on the paired Google rule: one half alone is not
            // enough ("input token count" without "exceeds the maximum").
            "the input token count was 1196265",
        ] {
            assert!(
                !is_context_overflow_text(text),
                "not a context overflow: {text}"
            );
        }
    }
}

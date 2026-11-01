//! The model providers (ADR 0024: one per wire shape) + the shared
//! transport. `OpenAiCompatibleProvider` (in [`openai`]) speaks the
//! OpenAI-compatible chat-completions API: a `POST {base_url}/chat/completions`
//! with `stream: true`, parsing the SSE response (content / reasoning /
//! tool-call deltas, `finish_reason`, `usage`) into a `ProviderEvent`
//! stream.
//!
//! The [`Provider`] trait (in [`types`]) is the seam the `AgentLoop`
//! (Task 6) calls — it is provider-agnostic in shape (a `complete()`
//! returning a boxed, `'static`, `Send` stream) so a second provider
//! (Anthropic) can be added later without touching the loop.

pub mod anthropic;
pub mod openai;
mod overflow;
pub mod responses;
#[cfg(test)]
pub(crate) mod test_util;
pub mod transport;
pub mod types;

pub use anthropic::{normalize_anthropic_base_url, normalize_wire_base_url, AnthropicProvider};
pub use openai::OpenAiCompatibleProvider;
pub use responses::OpenAiResponsesProvider;
pub use types::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ModelOptions, ModelRequest, Provider,
    ProviderError, ProviderEvent, ToolCall, ToolCallDelta, ToolSpec, Usage,
};

/// Build the `Provider` for a `Model` (the `api` discriminator — ADR
/// 0024; the `SessionManager`'s default factory + the tests).
///
/// The `None`-headers shorthand (issue #129): no custom headers on the wire.
/// [`build_provider_with_headers`] is the header-carrying form the two
/// production construction sites use.
pub fn build_provider(m: &crate::agent::harness::catalog::Model) -> Box<dyn Provider> {
    match crate::agent::harness::WireApi::parse(m.api.as_deref().unwrap_or("openai-completions"))
        .unwrap_or_default()
    {
        crate::agent::harness::WireApi::AnthropicMessages => Box::new(AnthropicProvider {
            // T3b (#131): the Messages wire wants the ROOT — the client appends
            // its own `/v1/messages`, so a `{root}/v1` base (the Settings'
            // convention, and what the LiteLLM discovery rows carry) would
            // otherwise produce `{root}/v1/v1/messages`. The pass is idempotent
            // (`n(n(u)) == n(u)`), so re-running it is a no-op.
            base_url: normalize_wire_base_url(
                &m.base_url,
                crate::agent::harness::WireApi::AnthropicMessages,
            ),
            api_key: m.api_key.clone(),
            custom_headers: None,
        }),
        // Completions / Responses keep the configured base VERBATIM
        // (byte-identical to today — see the note above
        // `build_provider_with_headers` for why the append rule is not applied
        // here, and `normalize_wire_base_url` for the rule itself).
        crate::agent::harness::WireApi::OpenAiResponses => Box::new(OpenAiResponsesProvider {
            base_url: m.base_url.clone(),
            api_key: m.api_key.clone(),
            custom_headers: None,
        }),
        // `openai-completions` / `litellm` / `None` / unknown — the
        // current default (an unknown API is unselectable in practice,
        // but a hand-built `Model` with a weird `api` still gets a
        // working client). `litellm` is a DISCOVERY mode, not a wire —
        // its completion wire IS `openai-completions` (ADR 0026).
        _ => Box::new(OpenAiCompatibleProvider::new(
            m.base_url.clone(),
            m.api_key.clone(),
        )),
    }
}

/// Expand + validate ONE provider's custom headers from `settings.json`
/// (R1, issue #129 — the completion-wire path). `config_dir` is the Worker's
/// settings home (`env.config_dir`, ALREADY threaded into `AgentLoop`); the
/// row is the one whose `id == model.provider`. A missing row / a row with no
/// `headers` → `None` (no custom headers — the current behaviour byte-for-
/// byte); an unreadable settings file degrades the same way (`config::
/// load_settings` never fails — it returns the defaults + its own warning), so
/// provider CONSTRUCTION never fails because headers were unreadable.
///
/// The expansion engine is the SAME `provider_auth::headers::expand_map` the
/// discovery spine (T2a) calls — a header value is expanded once and both
/// wires see the identical result. The insecure-HTTP gate is NOT re-checked
/// here: it belongs to the discovery spine (T2a's `validate_base_url`), and
/// this lookup is synchronous by construction (`AgentLoop::set_model` is a
/// sync `&mut self` method).
pub(crate) fn provider_headers_for(
    config_dir: &std::path::Path,
    model: &crate::agent::harness::catalog::Model,
) -> Option<std::collections::BTreeMap<String, String>> {
    let settings = crate::config::load_settings(config_dir);
    settings
        .providers
        .iter()
        .find(|p| p.id == model.provider)
        .and_then(|p| p.headers.as_ref())
        .map(crate::agent::provider_auth::headers::expand_map)
}

/// Build the `Provider` for a `Model` WITH the provider's custom headers
/// (R1, issue #129): the production construction entry —
/// `build_provider_with_headers(&model, provider_headers_for(dir, model).as_ref())`.
/// `custom_headers` is the EXPANDED + VALIDATED map; it rides every completion
/// request through the ONE seam, [`transport::build_client`]'s
/// `default_headers`. `None` is byte-identical to [`build_provider`] (which
/// stays the `None` shorthand for the naming path and the tests).
pub fn build_provider_with_headers(
    m: &crate::agent::harness::catalog::Model,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
) -> Box<dyn Provider> {
    match crate::agent::harness::WireApi::parse(m.api.as_deref().unwrap_or("openai-completions"))
        .unwrap_or_default()
    {
        crate::agent::harness::WireApi::AnthropicMessages => Box::new(AnthropicProvider {
            // T3b (#131): the SAME root-for-Messages pass as `build_provider`
            // (the idempotent strip; the client re-appends `/v1/messages` at
            // its own adapter boundary, so a `{root}/v1` row and a root row
            // both land on `{root}/v1/messages`).
            base_url: normalize_wire_base_url(
                &m.base_url,
                crate::agent::harness::WireApi::AnthropicMessages,
            ),
            api_key: m.api_key.clone(),
            custom_headers: custom_headers.cloned(),
        }),
        // Completions / Responses keep the configured base VERBATIM — see the
        // note on `build_provider` (the append half of
        // `normalize_wire_base_url` is deliberately NOT applied here).
        crate::agent::harness::WireApi::OpenAiResponses => Box::new(OpenAiResponsesProvider {
            base_url: m.base_url.clone(),
            api_key: m.api_key.clone(),
            custom_headers: custom_headers.cloned(),
        }),
        // `openai-completions` / `litellm` / `None` / unknown — the
        // current default (an unknown API is unselectable in practice,
        // but a hand-built `Model` with a weird `api` still gets a
        // working client). `litellm` is a DISCOVERY mode, not a wire —
        // its completion wire IS `openai-completions` (ADR 0026).
        // The CONSTRUCTOR (not a struct literal) — `OpenAiCompatibleProvider`
        // holds the PRIVATE `accepts_stream_options` latch a literal cannot
        // name; `with_custom_headers` is its header-carrying twin.
        _ => Box::new(OpenAiCompatibleProvider::with_custom_headers(
            m.base_url.clone(),
            m.api_key.clone(),
            custom_headers.cloned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::chat_message;
    use super::*;
    use futures_util::StreamExt;

    // ── `build_provider` (ADR 0024: the `api` discriminator) ────────

    /// A `Model` test-literal helper (everything else pinned; `api` is
    /// the parameter — the `catalog.rs` tests' `model` helper pattern,
    /// extended with the `api` parameter).
    fn model(base_url: &str, api: Option<&str>) -> crate::agent::harness::catalog::Model {
        crate::agent::harness::catalog::Model {
            id: "m/1".to_string(),
            provider: "p".to_string(),
            base_url: base_url.to_string(),
            api_key: "k".to_string(),
            context_window: 128000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: api.map(str::to_string),
        }
    }

    /// A minimal `ModelRequest` (a single user message; `max_tokens`
    /// present for the Anthropic wire's required field).
    fn test_request() -> ModelRequest {
        ModelRequest {
            model: "m/1".to_string(),
            messages: vec![chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            )],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: Some(64),
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        }
    }

    /// A recorded mock HTTP server: LOOP-accepts (one request per
    /// connection — the `build_provider` dispatch test performs FOUR
    /// dispatches, so a single-accept server would hang the 2nd/3rd/4th),
    /// reads each request's headers until `\r\n\r\n` (+ the
    /// `Content-Length` body), records the `PATH` line + the
    /// `authorization` / `x-api-key` header presence, and replies the
    /// wire-appropriate minimal 200 SSE body (a `data: {"choices":[…]}`
    /// for OpenAI, a `message_start` + `message_delta` pair for
    /// Anthropic, a `response.completed` for Responses).
    async fn recorded_dispatch_server(
        listener: tokio::net::TcpListener,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::sync::mpsc::Receiver<(String, bool, bool)>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel::<(String, bool, bool)>(8);
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = [0u8; 65536];
                let mut data = Vec::new();
                // Read the headers (until `\r\n\r\n`).
                while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    data.extend_from_slice(&buf[..n]);
                }
                let header_end = data
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .expect("the header terminator")
                    + 4;
                let headers = String::from_utf8_lossy(&data[..header_end]).to_ascii_lowercase();
                let path = headers.lines().next().unwrap_or_default().to_string();
                let has_auth = headers.lines().any(|l| l.starts_with("authorization:"));
                let has_x_api_key = headers.lines().any(|l| l.starts_with("x-api-key:"));
                // Read the body (`Content-Length` bytes — a clean close
                // needs the request fully consumed; the client may send
                // the body in the same or a later segment).
                let content_length = headers
                    .lines()
                    .find(|l| l.starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1).unwrap().trim().parse().ok())
                    .unwrap_or(0) as usize;
                let mut have = data.len();
                while have < header_end + content_length {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&buf[..n]);
                    have = data.len();
                }
                // The wire-appropriate minimal 200 SSE body (the `PATH`
                // decides the wire — one server, four dispatches).
                let body = if path.contains("/messages") {
                    // Anthropic: a `message_start` + `message_delta` pair.
                    "event: message_start\r\n\
                     data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\r\n\
                     \r\n\
                     event: message_delta\r\n\
                     data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\r\n\
                     \r\n"
                } else if path.contains("/responses") {
                    // Responses: a `response.completed`.
                    "event: response.completed\r\n\
                     data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\r\n\
                     \r\n"
                } else {
                    // OpenAI: a `data: {"choices":[…]}` + `[DONE]`.
                    "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\r\n\
                     \r\n\
                     data: [DONE]\r\n\
                     \r\n"
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                if stream.write_all(resp.as_bytes()).await.is_err() {
                    break;
                }
                if tx.send((path, has_auth, has_x_api_key)).await.is_err() {
                    break;
                }
                // DROP the stream (the client sees EOF → the stream ends).
            }
        });
        (server, rx)
    }

    /// (ADR 0024) `build_provider` dispatches on `Model.api`: an
    /// `anthropic-messages` model runs on `AnthropicProvider` (`POST
    /// /messages`, an `x-api-key` header, NO `authorization` header), an
    /// `openai-responses` model on `OpenAiResponsesProvider` (`POST
    /// /responses`, an `authorization` header), and a `None`-api model
    /// on `OpenAiCompatibleProvider` (`POST /chat/completions`, an
    /// `authorization` header — the current default), and a `litellm`
    /// model on `OpenAiCompatibleProvider` too (`POST /chat/completions`,
    /// an `authorization` header — ADR 0026: `litellm` is a DISCOVERY
    /// mode, its wire is `openai-completions`). Each stream is collected
    /// to completion (the recorded mock server replies the
    /// wire-appropriate minimal 200 SSE body; the loop-accept server
    /// handles the FOUR dispatches — one request per connection).
    #[tokio::test]
    async fn build_provider_dispatches_on_the_api() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let base_url = format!("http://{addr}/v1");
        let (server, mut rx) = recorded_dispatch_server(listener).await;
        let req = test_request();

        // (1) `anthropic-messages` → `POST /messages` + `x-api-key` +
        // NO `authorization`.
        let provider = build_provider(&model(&base_url, Some("anthropic-messages")));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the anthropic stream completes: {events:?}"
        );
        let (path, has_auth, has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        // (the method line is lowercase on the wire — `post`)
        assert!(
            path.starts_with("post ") && path.contains("/messages"),
            "got {path:?}"
        );
        assert!(
            has_x_api_key,
            "the anthropic wire sends `x-api-key` ({path:?})"
        );
        assert!(
            !has_auth,
            "the anthropic wire sends NO `authorization` ({path:?})"
        );

        // (2) `openai-responses` → `POST /responses` + `authorization`.
        let provider = build_provider(&model(&base_url, Some("openai-responses")));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the responses stream completes: {events:?}"
        );
        let (path, has_auth, has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        assert!(
            path.starts_with("post ") && path.contains("/responses"),
            "got {path:?}"
        );
        assert!(
            has_auth,
            "the responses wire sends `authorization` ({path:?})"
        );
        assert!(
            !has_x_api_key,
            "the responses wire sends NO `x-api-key` ({path:?})"
        );

        // (3) `None` (the default) → `POST /chat/completions` +
        // `authorization`.
        let provider = build_provider(&model(&base_url, None));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the openai-compatible stream completes: {events:?}"
        );
        let (path, has_auth, _has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        assert!(
            path.starts_with("post ") && path.contains("/chat/completions"),
            "got {path:?}"
        );
        assert!(
            has_auth,
            "the openai-compatible wire sends `authorization` ({path:?})"
        );

        // (4) (ADR 0026) `litellm` → the OpenAI arm: `POST /chat/completions` +
        // `authorization` (it is a discovery mode, NOT a fourth wire).
        let provider = build_provider(&model(&base_url, Some("litellm")));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the litellm (openai-compatible) stream completes: {events:?}"
        );
        let (path, has_auth, has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        assert!(
            path.starts_with("post ") && path.contains("/chat/completions"),
            "got {path:?}"
        );
        assert!(
            has_auth,
            "the litellm api uses the openai-compatible wire: `authorization` ({path:?})"
        );
        assert!(
            !has_x_api_key,
            "the litellm api uses the openai-compatible wire: NO `x-api-key` ({path:?})"
        );

        // The loop-accept server never exits on its own — abort it (an
        // `await` would hang on the `accept()` loop).
        server.abort();
    }

    // ── the per-wire base-URL pass (T3b / #131) ───────────────────────

    /// A `Model` whose `api` carries a wire, for the recorded-path assertions.
    async fn record_paths(base_url: &str, api: &str) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let (server, mut rx) = recorded_dispatch_server(listener).await;
        let base = base_url.replace("{addr}", &addr.to_string());
        let provider = build_provider(&model(&base, Some(api)));
        let events: Vec<ProviderEvent> = provider
            .complete(&test_request())
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the {api} stream completes: {events:?}"
        );
        let (path, _auth, _key) = rx.recv().await.expect("the server recorded the request");
        (path, server)
    }

    /// The Messages rule at the `build_provider` call site: the base is
    /// normalized to the ROOT, so the request lands on `POST /v1/messages` —
    /// NOT `/messages` (a root-only base) and NOT `/v1/v1/messages` (the
    /// double-prefix the un-normalized `{root}/v1` base produced before T3b).
    #[tokio::test]
    async fn build_provider_posts_messages_at_root_normalized_v1() {
        // A ROOT base (no `/v1`): the pass leaves it, the client appends its
        // own `/v1/messages`.
        let (path, server) = record_paths("http://{addr}", "anthropic-messages").await;
        assert!(
            path.starts_with("post ") && path.contains(" /v1/messages"),
            "a root base posts /v1/messages: got {path:?}"
        );
        server.abort();

        // A `{root}/v1` base (the Settings' convention): the STRIP is the new
        // behavior — without it the client's own `/v1` append doubles it.
        let (path, server) = record_paths("http://{addr}/v1", "anthropic-messages").await;
        assert!(
            path.contains(" /v1/messages") && !path.contains("/v1/v1/"),
            "a `/v1` base is stripped to the root, never double-prefixed: got {path:?}"
        );
        server.abort();
    }

    /// Completions / Responses keep the configured base VERBATIM (the
    /// no-regression pin — every existing suite mounts the path its own base
    /// produces, and a root base stays a root base).
    #[tokio::test]
    async fn build_provider_keeps_the_configured_base_for_completions_and_responses() {
        // A root base → the client's own path, no appended `/v1` (byte-identical
        // to the pre-T3b behavior the unowned suites pin).
        let (path, server) = record_paths("http://{addr}", "openai-completions").await;
        assert!(
            path.contains(" /chat/completions") && !path.contains("/v1/chat"),
            "a root base posts /chat/completions verbatim: got {path:?}"
        );
        server.abort();
        let (path, server) = record_paths("http://{addr}", "openai-responses").await;
        assert!(
            path.contains(" /responses") && !path.contains("/v1/responses"),
            "a root base posts /responses verbatim: got {path:?}"
        );
        server.abort();

        // A `/v1` base → `/v1/…` (the configured shape, untouched).
        let (path, server) = record_paths("http://{addr}/v1", "openai-completions").await;
        assert!(
            path.contains(" /v1/chat/completions"),
            "a `/v1` base posts /v1/chat/completions: got {path:?}"
        );
        server.abort();
        let (path, server) = record_paths("http://{addr}/v1", "openai-responses").await;
        assert!(
            path.contains(" /v1/responses"),
            "a `/v1` base posts /v1/responses: got {path:?}"
        );
        server.abort();
    }

    /// The pass is idempotent THROUGH `build_provider`: feeding a base the
    /// helper itself would produce records the SAME path as feeding the raw
    /// base (the call-site half of the `n(n(u)) == n(u)` contract).
    #[tokio::test]
    async fn build_provider_base_pass_is_idempotent_on_v1_bases() {
        // ONE server, two bases (so the recorded paths are literally comparable
        // — the same authority in both).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let (server, mut rx) = recorded_dispatch_server(listener).await;

        /// Send through `build_provider` and return the RECORDED request line
        /// (one call site, so every recorded path is comparable).
        async fn record(
            addr: std::net::SocketAddr,
            api: &str,
            suffix: &str,
            rx: &mut tokio::sync::mpsc::Receiver<(String, bool, bool)>,
        ) -> String {
            let base = format!("http://{addr}{suffix}");
            let provider = build_provider(&model(&base, Some(api)));
            let events: Vec<ProviderEvent> = provider
                .complete(&test_request())
                .await
                .expect("the request sends")
                .collect()
                .await;
            assert!(
                events
                    .iter()
                    .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
                "the {api} stream completes: {events:?}"
            );
            rx.recv().await.expect("the server recorded the request").0
        }

        // Messages: `{root}/v1` (what the helper strips FROM) and the stripped
        // root (what it produces) record the IDENTICAL path.
        let from_v1 = record(addr, "anthropic-messages", "/v1", &mut rx).await;
        let from_root = record(addr, "anthropic-messages", "", &mut rx).await;
        assert_eq!(
            from_v1, from_root,
            "the Messages pass is idempotent through build_provider \
             (`{{root}}/v1` and the root both land on /v1/messages)"
        );
        assert!(
            from_v1.contains(" /v1/messages"),
            "and the shared path is /v1/messages: {from_v1:?}"
        );

        // Completions / Responses: the pass is the identity, so an already-
        // `/v1` base records exactly the path its base names (no double-append).
        let comp = record(addr, "openai-completions", "/v1", &mut rx).await;
        assert!(
            comp.contains(" /v1/chat/completions") && !comp.contains("/v1/v1/"),
            "a `/v1` completions base is never double-appended: {comp:?}"
        );
        let resp = record(addr, "openai-responses", "/v1", &mut rx).await;
        assert!(
            resp.contains(" /v1/responses") && !resp.contains("/v1/v1/"),
            "a `/v1` responses base is never double-appended: {resp:?}"
        );

        server.abort();
    }
}

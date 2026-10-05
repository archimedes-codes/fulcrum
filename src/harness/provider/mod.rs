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
pub mod responses;
#[cfg(test)]
pub(crate) mod test_util;
pub mod transport;
pub mod types;

pub use anthropic::{normalize_anthropic_base_url, AnthropicProvider};
pub use openai::OpenAiCompatibleProvider;
pub use responses::OpenAiResponsesProvider;
pub use types::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ModelOptions, ModelRequest, Provider,
    ProviderError, ProviderEvent, ToolCall, ToolCallDelta, ToolSpec, Usage,
};

/// Build the `Provider` for a `Model` (the `api` discriminator — ADR
/// 0024; the `SessionManager`'s default factory + the tests).
pub fn build_provider(m: &crate::agent::harness::catalog::Model) -> Box<dyn Provider> {
    match crate::agent::harness::WireApi::parse(m.api.as_deref().unwrap_or("openai-completions"))
        .unwrap_or_default()
    {
        crate::agent::harness::WireApi::AnthropicMessages => Box::new(AnthropicProvider {
            base_url: m.base_url.clone(),
            api_key: m.api_key.clone(),
        }),
        crate::agent::harness::WireApi::OpenAiResponses => Box::new(OpenAiResponsesProvider {
            base_url: m.base_url.clone(),
            api_key: m.api_key.clone(),
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
}

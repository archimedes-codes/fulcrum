//! Shared model-call transport: the `reqwest` client, the
//! HTTP-status→[`ProviderError`] map, the LiteLLM session/request headers,
//! and the Anthropic API version.
//!
//! These were TRIPlicated verbatim across the three `complete()` impls
//! (OpenAI-completions / Anthropic / OpenAI-Responses) — they are
//! wire-agnostic (ADR 0024) and live here once now.

use std::time::Duration;

use super::types::ProviderError;

/// The `User-Agent` the model calls identify with (`archimedes/<version>`
/// — the harness is the desktop's own model client; `reqwest` sends NO
/// `User-Agent` by default, so without this the traffic is unattributable
/// in the provider's logs).
pub const USER_AGENT: &str = concat!("archimedes/", env!("CARGO_PKG_VERSION"));

/// The `anthropic-version` header value, REQUIRED on every Anthropic
/// endpoint (used by the `AnthropicProvider` request and the Anthropic
/// discovery call in `catalog.rs`).
pub const ANTHROPIC_API_VERSION: &str = "2023-06-01";

/// Build the `reqwest` client for a model call (finding 4 — the IDLE-based
/// bounds: a generous `connect_timeout` + a `read_timeout` (the time BETWEEN
/// BYTES on the body — a stalled provider errors `Retryable`, a healthy slow
/// stream that dribbles bytes keeps flowing). Extracted so the tests can
/// inject a short `read_timeout` (the production 5 min is too slow to test).
///
/// The client also identifies itself: a `User-Agent` header ([`USER_AGENT`])
/// so the provider's logs can attribute the traffic to the app.
pub(crate) fn build_client(connect_timeout: Duration, read_timeout: Duration) -> reqwest::Client {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::USER_AGENT,
        reqwest::header::HeaderValue::from_static(USER_AGENT),
    );
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .read_timeout(read_timeout)
        .default_headers(headers)
        .build()
        .expect("the reqwest client builds (fixed connect + read timeouts)")
}

/// Map an HTTP status to a [`ProviderError`] (the two-tier message contract
/// of the pre-dedup `complete()`s): 401/403 → `Auth`, 429/5xx → `Retryable`,
/// other 4xx → `Fatal` — all three embed the error body
/// (`status {status}: {body}`); any OTHER non-success status (1xx / 3xx —
/// e.g. a 3xx with no `Location` header, which `reqwest` surfaces instead of
/// following) → the TERMINAL `Fatal(format!("status {status}"))`, with NO
/// body suffix. `None` for 2xx (a success — no error).
///
/// `body` is `Some` only when the caller READ the body — which happens only
/// for a 4xx/5xx (see [`map_response`]); pass `None` for a status whose body
/// was deliberately not read (the terminal arm ignores it either way).
pub(crate) fn map_status(status: reqwest::StatusCode, body: Option<&str>) -> Option<ProviderError> {
    if status.is_success() {
        return None;
    }
    let body = body.unwrap_or_default();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        Some(ProviderError::Auth(format!("status {status}: {body}")))
    } else if status.as_u16() == 429 || status.is_server_error() {
        Some(ProviderError::Retryable(format!("status {status}: {body}")))
    } else if status.is_client_error() {
        Some(ProviderError::Fatal(format!("status {status}: {body}")))
    } else {
        // Terminal (1xx / 3xx): no body read, no `: {body}` suffix.
        Some(ProviderError::Fatal(format!("status {status}")))
    }
}

/// Gate a response on its HTTP status BEFORE the SSE body is consumed: a
/// success passes the [`reqwest::Response`] through untouched; otherwise the
/// error payload is read ONLY for a 4xx / 5xx (a 1xx / 3xx body is NOT read)
/// and mapped by [`map_status`] (byte-identical to the pre-dedup
/// `complete()`s' status guards).
pub(crate) async fn map_response(
    resp: reqwest::Response,
) -> Result<reqwest::Response, ProviderError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = if status.is_client_error() || status.is_server_error() {
        Some(resp.text().await.unwrap_or_default())
    } else {
        None
    };
    Err(map_status(status, body.as_deref()).expect("a non-success status maps to an error"))
}

/// The LiteLLM session / request tracking headers (added when the request
/// carries a session id): `x-litellm-session-id` + `x-request-id`, both set
/// to the session id.
pub(crate) fn litellm_headers(session_id: Option<&str>) -> Vec<(String, String)> {
    match session_id {
        Some(sid) => vec![
            ("x-litellm-session-id".to_string(), sid.to_string()),
            ("x-request-id".to_string(), sid.to_string()),
        ],
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::{ModelOptions, ModelRequest, Provider};
    use super::*;
    use reqwest::StatusCode;

    /// The HTTP-status→`ProviderError` map (the wire-visible behavior):
    /// 401/403 → `Auth`, 429/5xx → `Retryable`, other 4xx → `Fatal`,
    /// 2xx → no error.
    #[test]
    fn map_status_classifies_the_retry_contract() {
        for s in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            assert!(
                matches!(map_status(s, Some("boom")), Some(ProviderError::Auth(_))),
                "{s} should be Auth"
            );
        }
        for s in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(
                matches!(
                    map_status(s, Some("boom")),
                    Some(ProviderError::Retryable(_))
                ),
                "{s} should be Retryable"
            );
        }
        for s in [StatusCode::BAD_REQUEST, StatusCode::NOT_FOUND] {
            assert!(
                matches!(map_status(s, Some("boom")), Some(ProviderError::Fatal(_))),
                "{s} should be Fatal"
            );
        }
        assert!(map_status(StatusCode::OK, Some("")).is_none());
        assert!(map_status(StatusCode::CREATED, Some("")).is_none());
    }

    /// The TERMINAL arm (1xx / 3xx — a status that is neither a client nor a
    /// server error): `Fatal` with the message EXACTLY `status {status}` —
    /// NO `: {body}` suffix, even when a body is supplied (byte-identical to
    /// the pre-dedup `complete()`s, whose final `if !status.is_success()`
    /// guard never read the body). The message is `status {status}` with
    /// `reqwest`'s own `StatusCode` rendering (`301 Moved Permanently`).
    #[test]
    fn map_status_terminal_arm_has_no_body_suffix() {
        let s = StatusCode::MOVED_PERMANENTLY;
        let expected = format!("status {s}");
        let without = map_status(s, None).expect("a non-success status maps to an error");
        assert_eq!(without, ProviderError::Fatal(expected.clone()));
        // A supplied body is IGNORED on the terminal arm.
        let with = map_status(s, Some("nope")).expect("a non-success status maps to an error");
        assert_eq!(with, ProviderError::Fatal(expected));
    }

    // ── the idle-based `read_timeout` (finding 4) ────────────────────

    /// A raw HTTP server: reads the request, writes the `200` headers, then
    /// either DRIBBLES a body byte every 50 ms (a healthy slow stream) or
    /// stays SILENT (a stalled one). The `read_timeout` applies to the time
    /// BETWEEN BYTES on the body, so a dribble under the timeout completes
    /// and a silence over it errors.
    async fn raw_http_server(
        listener: tokio::net::TcpListener,
        dribble: bool,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = [0u8; 4096];
            let mut data = Vec::new();
            // Read the request until the headers end (`\r\n\r\n`).
            while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                let Ok(n) = stream.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                data.extend_from_slice(&buf[..n]);
            }
            if stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
                .await
                .is_err()
            {
                return;
            }
            if dribble {
                // Dribble a body byte every 50 ms for 2 s (a healthy slow
                // stream — each byte is well under the 200 ms `read_timeout`).
                for _ in 0..40 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if stream.write_all(b"x").await.is_err() {
                        return;
                    }
                }
            } else {
                // Silent: HOLD the connection open (no body bytes — the
                // `read_timeout` fires before this sleep ends; the test
                // aborts the task so it does not wait the full duration).
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            // Close (the stream is dropped when the function returns).
        })
    }

    /// (finding 4) A stream that DRIBBLES a byte every 50 ms (under the
    /// `read_timeout`) COMPLETES (a healthy slow stream is not cut — the
    /// pre-fix `.timeout()` was a TOTAL request bound that killed it), while
    /// a fully SILENT stream ERRORS (a stalled provider — the `read_timeout`
    /// fires). A short `read_timeout` (200 ms) is injected via `build_client`
    /// (the production 5 min is too slow to test).
    #[tokio::test]
    async fn a_slow_stream_completes_but_a_silent_stream_errors() {
        // Consume a `bytes_stream`, returning `(bytes, error)`: `error` is
        // `Some` if a chunk failed (the `read_timeout`), `None` if the stream
        // completed (EOF).
        async fn drain(resp: reqwest::Response) -> (Vec<u8>, Option<reqwest::Error>) {
            use futures_util::StreamExt;
            let mut stream = resp.bytes_stream();
            let mut body = Vec::new();
            let mut error: Option<reqwest::Error> = None;
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(b) => body.extend_from_slice(&b),
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
            (body, error)
        }

        // (1) Dribbling: a body byte every 50 ms for 2 s (under the 200 ms
        //     `read_timeout`). The stream COMPLETES (40 bytes — not cut).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let server = raw_http_server(listener, true).await;
        let client = build_client(Duration::from_secs(30), Duration::from_millis(200));
        let resp = client
            .post(format!("http://{addr}/chat/completions"))
            .send()
            .await
            .expect("the request sends");
        let (body, error) = drain(resp).await;
        assert!(
            error.is_none(),
            "the slow (dribbling) stream completes — it is NOT cut, got {error:?}"
        );
        assert_eq!(body.len(), 40, "all 40 dribbled bytes were received");
        let _ = server.await;

        // (2) Silent: no body bytes (over the 200 ms `read_timeout`). The
        //     stream ERRORS (a stalled provider — the `read_timeout` fires).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let server = raw_http_server(listener, false).await;
        let client = build_client(Duration::from_secs(30), Duration::from_millis(200));
        let resp = client
            .post(format!("http://{addr}/chat/completions"))
            .send()
            .await
            .expect("the request sends");
        let (_body, error) = drain(resp).await;
        assert!(
            error.is_some(),
            "the silent stream errors — the `read_timeout` fired"
        );
        server.abort(); // the server holds the connection open (the sleep)
    }

    /// End-to-end: a TERMINAL status (a `301` with NO `Location` header —
    /// reqwest surfaces it instead of erroring) surfaces from a real provider
    /// `complete()` as `Fatal` with the message EXACTLY `status {status}` —
    /// the body is NOT read and NOT appended (byte-identical to main).
    #[tokio::test]
    async fn a_terminal_status_errors_without_a_body_suffix() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let server = tokio::spawn(async move {
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
            // A 3xx with no `Location` header + a body (the terminal arm).
            let _ = stream
                .write_all(
                    b"HTTP/1.1 301 Nope\r\ncontent-type: text/plain\r\nConnection: close\r\n\r\nnope",
                )
                .await;
        });
        let provider = super::super::openai::OpenAiCompatibleProvider::new(
            format!("http://{addr}/v1"),
            "k".to_string(),
        );
        let req = ModelRequest {
            model: "m/1".to_string(),
            messages: Vec::new(),
            tools: Vec::new(),
            options: ModelOptions {
                temperature: None,
                max_tokens: Some(64),
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        };
        let err = match provider.complete(&req).await {
            Ok(_) => panic!("a 301 with no Location is an error"),
            Err(e) => e,
        };
        assert_eq!(
            err,
            ProviderError::Fatal(format!("status {}", StatusCode::MOVED_PERMANENTLY))
        );
        assert!(
            !err.to_string().contains("nope"),
            "the terminal arm never reads/embeds the body: {err}"
        );
        let _ = server.await;
    }

    /// The client identifies itself: the request carries a `User-Agent`
    /// header (`archimedes/<version>`) so the provider's logs can
    /// attribute the traffic to the app (the harness is the desktop's own
    /// OpenAI-compatible client — `reqwest` sends NO `User-Agent` by
    /// default, so without this the traffic is unattributable).
    #[tokio::test]
    async fn the_request_sends_a_user_agent_header() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // A raw server that CAPTURES the request headers (the `reqwest`
        // response is complete — `data: [DONE]` — so the client closes
        // the connection; the captured bytes go to the test via the
        // channel).
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        tokio::spawn(async move {
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
            let _ = tx.send(data).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\ndata: [DONE]\r\n\r\n",
                )
                .await;
            // Hold the socket open until the test is done (the response
            // body is complete — the client closes the connection).
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let client = build_client(Duration::from_secs(30), Duration::from_millis(200));
        let _resp = client
            .post(format!("http://{addr}/chat/completions"))
            .send()
            .await
            .expect("the request sends");
        let raw = rx.recv().await.expect("the server captured the request");
        let request = String::from_utf8(raw).expect("the request is valid UTF-8");
        let header = request
            .lines()
            .find(|l| l.to_lowercase().starts_with("user-agent:"))
            .expect("the request carries a User-Agent header")
            .to_lowercase();
        assert_eq!(
            header,
            format!("user-agent: archimedes/{}", env!("CARGO_PKG_VERSION"))
        );
    }
}

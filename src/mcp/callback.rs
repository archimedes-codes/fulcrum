//! The local OAuth callback server (ADR 0018): binds 127.0.0.1 (a random
//! free port), waits for a single `GET /callback?code=…&state=…` (the
//! browser's redirect after the user authorizes), validates the `state`
//! against the reserved value (a CSRF guard), and returns the `code`.
//!
//! A minimal single-shot HTTP handler (a raw `tokio::net` listener — NO
//! new HTTP dependency). A bind failure is surfaced (`Err`) so the caller
//! can fall back to the manual-URL path (the `authenticate` result carries
//! the authorization URL; the model's `ask` tool can relay it).

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

/// The result of a successful callback (the authorization `code`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackResult {
    pub code: String,
}

/// A bound local callback server (127.0.0.1, a random free port).
pub struct CallbackServer {
    listener: tokio::net::TcpListener,
    /// The redirect URI (the `redirect_uri` the authorization request
    /// carries — `http://127.0.0.1:<port>/callback`).
    redirect_uri: String,
}

impl CallbackServer {
    /// Bind 127.0.0.1 on a random free port. A bind failure is `Err`
    /// (the caller surfaces the manual-URL path).
    pub async fn bind() -> Result<Self, String> {
        // A `std` bind to pick a free port (a `Result` — a failure is
        // surfaced, not a panic), then a tokio `bind` on that port (a
        // `from_std` is unsupported in a runtime; the rebind race is
        // negligible — the port was just freed).
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let port = std_listener.local_addr().map_err(|e| e.to_string())?.port();
        drop(std_listener);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self {
            redirect_uri: format!("http://127.0.0.1:{port}"),
            listener,
        })
    }

    /// The redirect URI (`http://127.0.0.1:<port>/callback`).
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }

    /// Wait for a single `GET /callback?code=…&state=…` (raced against
    /// `timeout` + `cancel`), validating the `state` against `expected_state`
    /// (a CSRF guard — a mismatch is an error). Returns the `code`.
    ///
    /// A single-shot handler: the FIRST request is served (the response is a
    /// "success — you can close this tab" page), and the listener is then
    /// dropped (a second request is not accepted).
    pub async fn wait(
        self,
        expected_state: &str,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<CallbackResult, String> {
        let accepted = tokio::select! {
            r = self.listener.accept() => r,
            _ = tokio::time::sleep(timeout) => {
                return Err("the OAuth callback timed out".to_string());
            }
            _ = cancel.cancelled() => {
                return Err("cancelled".to_string());
            }
        }
        .map_err(|e| format!("the callback listener failed: {e}"))?;
        let (mut stream, _) = accepted;
        // Read the request (we only need the request line — the
        // `?code=…&state=…` query).
        let mut buf = [0u8; 8192];
        let mut data = Vec::new();
        // Read until the headers end (`\r\n\r\n`) — the request line is the
        // first line.
        while !data.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut buf).await.map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            data.extend_from_slice(&buf[..n]);
        }
        let request = String::from_utf8_lossy(&data);
        let request_line = request.lines().next().unwrap_or_default();
        // Parse the query (`GET /callback?code=…&state=…`).
        let Some(query) = request_line.split('?').nth(1).map(str::to_string) else {
            return Err("the callback request has no query".to_string());
        };
        // Strip the ` HTTP/1.1` (the request line is `GET /path?query HTTP/1.1`).
        let query = query
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        let mut code: Option<String> = None;
        let mut state: Option<String> = None;
        for pair in query.split('&') {
            let mut kv = pair.splitn(2, '=');
            let k = kv.next().unwrap_or_default();
            let v = kv.next().unwrap_or_default();
            match k {
                "code" => code = Some(url_decode(v)),
                "state" => state = Some(url_decode(v)),
                _ => {}
            }
        }
        let Some(code) = code else {
            return Err("the callback has no `code`".to_string());
        };
        let Some(state) = state else {
            return Err("the callback has no `state`".to_string());
        };
        if state != expected_state {
            return Err("the callback `state` does not match (a possible CSRF)".to_string());
        }
        // A "success — you can close this tab" page.
        let html = "<!doctype html><meta charset=\"utf-8\"><title>Authenticated</title>\
                   <body><h1>Success</h1><p>You can close this tab and return to Archimedes.</p></body>";
        let _ = stream.write_all(html.as_bytes()).await;
        // Single-shot: drop the listener (a second request is not accepted).
        drop(self.listener);
        Ok(CallbackResult { code })
    }
}

/// A minimal percent-decode (the `code` / `state` may be percent-encoded).
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &s[i + 1..i + 3];
            if let Ok(v) = u8::from_str_radix(hex, 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;

    /// Send a raw HTTP request to a bound `CallbackServer` (a helper for
    /// the tests — a separate connection that issues the `/callback` GET).
    async fn send_callback(port: u16, path_and_query: &str) -> std::io::Result<()> {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        let req = format!("GET {path_and_query} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
        stream.write_all(req.as_bytes()).await?;
        Ok(())
    }

    fn port_of(redirect_uri: &str) -> u16 {
        redirect_uri
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .expect("the redirect URI has a port")
    }

    /// A bound server's port (a helper — bind a UDP socket to find a free
    /// port is not needed; the `CallbackServer` picks its own port).
    #[allow(dead_code)]
    fn _free_port() -> u16 {
        UdpSocket::bind("127.0.0.1:0")
            .expect("binds")
            .local_addr()
            .expect("addr")
            .port()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_matching_state_returns_the_code() {
        let server = CallbackServer::bind().await.expect("binds");
        let port = port_of(server.redirect_uri());
        let handle = tokio::spawn(async move {
            server
                .wait(
                    "the-state",
                    Duration::from_secs(5),
                    &CancellationToken::new(),
                )
                .await
        });
        // Give the `wait` a moment to start listening, then send the callback.
        tokio::time::sleep(Duration::from_millis(50)).await;
        send_callback(port, "/callback?code=the-code&state=the-state")
            .await
            .expect("sends");
        let result = handle.await.expect("joins");
        assert_eq!(
            result,
            Ok(CallbackResult {
                code: "the-code".into()
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_mismatched_state_is_an_error() {
        let server = CallbackServer::bind().await.expect("binds");
        let port = port_of(server.redirect_uri());
        let handle = tokio::spawn(async move {
            server
                .wait(
                    "the-state",
                    Duration::from_secs(5),
                    &CancellationToken::new(),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        send_callback(port, "/callback?code=the-code&state=WRONG")
            .await
            .expect("sends");
        let result = handle.await.expect("joins");
        assert!(
            result.is_err(),
            "a mismatched state is an error: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_timeout_is_an_error() {
        let server = CallbackServer::bind().await.expect("binds");
        let result = server
            .wait(
                "the-state",
                Duration::from_millis(100),
                &CancellationToken::new(),
            )
            .await;
        assert!(result.is_err(), "a timeout is an error: {result:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancel_is_an_error() {
        let server = CallbackServer::bind().await.expect("binds");
        let cancel = CancellationToken::new();
        let inner = cancel.clone();
        let handle = tokio::spawn(async move {
            server
                .wait("the-state", Duration::from_secs(5), &inner)
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        let result = handle.await.expect("joins");
        assert!(result.is_err(), "a cancel is an error: {result:?}");
    }

    #[test]
    fn url_decode_handles_percent_encoding() {
        assert_eq!(url_decode("abc"), "abc");
        assert_eq!(url_decode("ab%20cd"), "ab cd");
        assert_eq!(url_decode("%41%42"), "AB");
    }
}

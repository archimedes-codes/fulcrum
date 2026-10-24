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
    /// A single-shot handler: the FIRST request is served with a framed
    /// `HTTP/1.1 200` "success — you can close this tab" page (a rejection
    /// answers a framed `400 Bad Request` — a browser reading a bare body
    /// reports a protocol error), and the listener is then dropped (a second
    /// request is not accepted).
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
            return Err(reject(&mut stream, "the callback request has no query").await);
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
            return Err(reject(&mut stream, "the callback has no `code`").await);
        };
        let Some(state) = state else {
            return Err(reject(&mut stream, "the callback has no `state`").await);
        };
        if state != expected_state {
            return Err(reject(
                &mut stream,
                "the callback `state` does not match (a possible CSRF)",
            )
            .await);
        }
        // A "success — you can close this tab" page.
        let html = "<!doctype html><meta charset=\"utf-8\"><title>Authenticated</title>\
                   <body><h1>Success</h1><p>You can close this tab and return to Archimedes.</p></body>";
        write_response(&mut stream, "200 OK", html).await;
        // Single-shot: drop the listener (a second request is not accepted).
        drop(self.listener);
        Ok(CallbackResult { code })
    }
}

/// Write a minimal `HTTP/1.1` response (status line + `Content-Type` +
/// `Content-Length` + `Connection: close` + a blank line + the body). A
/// browser reading a bare body reports a protocol error, so the framing is
/// the whole point of the page existing.
async fn write_response(stream: &mut tokio::net::TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    // A failed write is not an error worth failing the auth flow for — the
    // user may have closed the tab the moment it redirected.
    let _ = stream.write_all(response.as_bytes()).await;
}

/// Answer a rejected callback with a `400` (the tab is waiting for a reply —
/// silence reads as a broken server) and hand the SAME error text back to the
/// caller so the surfaced message does not change.
async fn reject(stream: &mut tokio::net::TcpStream, reason: &str) -> String {
    let body = format!(
        "<!doctype html><meta charset=\"utf-8\"><title>Not authorized</title>\
         <body><h1>Not authorized</h1><p>{reason}</p></body>"
    );
    write_response(stream, "400 Bad Request", &body).await;
    reason.to_string()
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

    /// Send a raw callback request AND read the whole reply (until EOF — the
    /// server answers with `Connection: close` and drops the stream). Nothing
    /// observed the response bytes before this helper existed.
    async fn callback_response(port: u16, path_and_query: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connects");
        let req = format!("GET {path_and_query} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
        stream.write_all(req.as_bytes()).await.expect("sends");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("reads to EOF");
        String::from_utf8_lossy(&buf).to_string()
    }

    /// Split a raw reply into (headers, body) at the first `\r\n\r\n`, and
    /// assert the `Content-Length` header equals the body's byte length (the
    /// self-consistency check — the body length is never hardcoded).
    fn assert_framed(response: &str, status: &str) -> String {
        assert!(
            response.starts_with(&format!("HTTP/1.1 {status}\r\n")),
            "the reply starts with the status line {status:?}: {response:?}"
        );
        let Some((headers, body)) = response.split_once("\r\n\r\n") else {
            panic!("the reply has a blank line after the headers: {response:?}");
        };
        assert!(
            headers.contains("Content-Type: text/html; charset=utf-8"),
            "the reply declares its content type: {headers:?}"
        );
        assert!(
            headers.contains("Connection: close"),
            "the reply closes the connection: {headers:?}"
        );
        let declared = headers
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length:").map(|v| v.trim()))
            .expect("a Content-Length header");
        assert_eq!(
            declared.parse::<usize>().expect("a numeric Content-Length"),
            body.len(),
            "the Content-Length matches the body's byte length"
        );
        body.to_string()
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
    async fn the_success_page_is_a_well_formed_http_response() {
        // The browser's tab lands on this reply — a bare body (no status line,
        // no headers) reads to it as a protocol error, so the framing is the
        // whole point of the page existing.
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
        let response = callback_response(port, "/callback?code=the-code&state=the-state").await;
        // The flow still works (the framing is additive).
        let result = handle.await.expect("joins");
        assert_eq!(
            result,
            Ok(CallbackResult {
                code: "the-code".into()
            })
        );
        let body = assert_framed(&response, "200 OK");
        assert!(
            body.contains("You can close this tab"),
            "the success page is rendered: {body:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rejected_callback_answers_http_400() {
        // A rejected callback leaves a browser tab waiting on the connection —
        // silence reads as a broken server, so the rejection is framed too.
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
        let response = callback_response(port, "/callback").await;
        let result = handle.await.expect("joins");
        let Err(e) = result else {
            panic!("a callback with no query is an error: {result:?}");
        };
        assert!(e.contains("no query"), "the error text is unchanged: {e}");
        assert_framed(&response, "400 Bad Request");
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

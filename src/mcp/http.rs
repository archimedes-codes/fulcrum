//! The MCP streamable-HTTP transport client (ADR 0018): one JSON-RPC 2.0
//! request per `POST` to the server `url`; the response is EITHER a JSON
//! object OR a `text/event-stream` SSE stream (`event:` / `data:` lines —
//! a `message` event carries the JSON-RPC response; a `ping` event is
//! ignored); a `Mcp-Session-Id` response header is stored + resent on
//! every subsequent request; `initialize` → `notifications/initialized`;
//! `tools/list`; `tools/call`.
//!
//! The request bound is IDLE-based (the `provider.rs` discipline): a
//! generous `connect_timeout` + a `read_timeout` (the time BETWEEN bytes —
//! a stalled provider ERRORS, a healthy slow stream that dribbles bytes
//! keeps flowing). Every operation races the TURN `CancellationToken`.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::types::{extract_tool_call_result, AuthSpec, HttpDef, ToolCallResult, ToolInfo};

const PROTOCOL_VERSION: &str = "2025-06-18";

/// A token provider for an OAuth server (the Task 5 seam — a closure that
/// returns the current valid token, if any; `None` = no token yet, so the
/// request goes out unauthenticated and a 401 → `needs-auth`).
pub type TokenProvider = Box<dyn Fn() -> Option<String> + Send + Sync>;

/// A connected streamable-HTTP MCP server.
pub struct HttpClient {
    client: reqwest::Client,
    url: String,
    /// The `Mcp-Session-Id` (set by the server's `initialize` response —
    /// resent on every subsequent request).
    session_id: Option<String>,
    /// The static `Authorization` header (a `Bearer` token / env-resolved
    /// token; `None` for an OAuth server — its token comes from the
    /// `get_token` provider per request).
    auth_header: Option<String>,
    /// An OAuth server's token provider (the Task 5 seam; `None` for a
    /// non-OAuth server).
    get_token: Option<TokenProvider>,
    /// An OAuth server (a 401 → the special `needs-auth` error, NOT a
    /// generic HTTP error).
    is_oauth: bool,
    /// The static (non-auth) headers (the `def.headers` — sent with every
    /// request).
    static_headers: BTreeMap<String, String>,
    next_id: std::sync::atomic::AtomicU64,
}

/// Build the `reqwest` client (the `provider.rs` IDLE-based bounds: a
/// generous `connect_timeout` + a `read_timeout` = the time BETWEEN bytes
/// on the body — a stalled server ERRORS `Retryable`-style, a healthy slow
/// stream that dribbles bytes keeps flowing).
fn build_client(read_timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(read_timeout)
        .build()
        .expect("the reqwest client builds")
}

impl HttpClient {
    /// Connect: the `initialize` handshake (with `timeout`, raced against
    /// `cancel`), then `notifications/initialized` (no response expected).
    /// A 401 on an OAuth server → the special `needs-auth` error (the
    /// manager marks the server `NeedsAuth`; the `authenticate` action,
    /// Task 5, is how the token is obtained).
    pub async fn connect(
        def: &HttpDef,
        timeout: Duration,
        cancel: &CancellationToken,
        get_token: Option<TokenProvider>,
    ) -> Result<Self, String> {
        // The static `Authorization` header: a `Bearer` token (literal or
        // env-resolved — a missing / empty env var is a connect error);
        // `None` for an OAuth server (its token comes from `get_token`).
        let auth_header = match &def.auth {
            AuthSpec::None => None,
            AuthSpec::Bearer { token, env_var } => {
                let t = token
                    .clone()
                    .or_else(|| env_var.as_ref().and_then(|v| std::env::var(v).ok()));
                match t {
                    Some(t) if !t.is_empty() => Some(format!("Bearer {t}")),
                    _ => {
                        if env_var.is_some() {
                            return Err(format!(
                                "the {} env var is not set (a Bearer token is required)",
                                env_var.as_deref().unwrap_or_default()
                            ));
                        }
                        None
                    }
                }
            }
            AuthSpec::OAuth { .. } => None,
        };
        let client = build_client(timeout);
        let mut http = HttpClient {
            client,
            url: def.url.clone(),
            session_id: None,
            auth_header,
            get_token,
            is_oauth: matches!(def.auth, AuthSpec::OAuth { .. }),
            static_headers: def.headers.clone(),
            next_id: std::sync::atomic::AtomicU64::new(1),
        };
        // The `initialize` handshake (a 401 on an OAuth server is the
        // special `needs-auth` error — see `send_request`).
        http.send_request(
            "initialize",
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": { "name": "archimedes", "version": env!("CARGO_PKG_VERSION") },
            }),
            cancel,
        )
        .await?;
        // `notifications/initialized` (NO response expected — best-effort;
        // a 4xx/5xx here is not fatal — the server may already be ready).
        let _ = http.send_notification("notifications/initialized").await;
        Ok(http)
    }

    /// One JSON-RPC request (a `POST` with the request body; the response
    /// is a JSON object OR an SSE stream — `read_sse_response` handles the
    /// latter): `Ok(result)` / `Err` (a JSON-RPC `error`, a 401 →
    /// `needs-auth` (OAuth) / HTTP error, a timeout, a cancel).
    async fn send_request(
        &mut self,
        method: &str,
        params: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, String> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let body = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let mut req = self.client.post(&self.url);
        req = req.header("Content-Type", "application/json");
        req = req.header("Accept", "application/json, text/event-stream");
        if let Some(session) = &self.session_id {
            req = req.header("Mcp-Session-Id", session);
        }
        // The `Authorization` header: the static `Bearer` token, else (an
        // OAuth server) the current token from the provider (the Task 5
        // seam — `None` = unauthenticated → a 401 → `needs-auth`).
        let auth = self
            .auth_header
            .clone()
            .or_else(|| self.get_token.as_ref().and_then(|f| f()));
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        for (k, v) in self.static_headers.iter() {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = tokio::select! {
            r = req.json(&body).send() => r,
            _ = cancel.cancelled() => return Err("cancelled".to_string()),
        }
        .map_err(|e| e.to_string())?;
        // The `Mcp-Session-Id` response header (stored + resent on every
        // subsequent request).
        if let Some(sid) = resp
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
        {
            self.session_id = Some(sid.to_string());
        }
        let status = resp.status();
        if status.as_u16() == 401 {
            if self.is_oauth {
                // The special `needs-auth` marker (the manager marks the
                // server `NeedsAuth`; `authenticate`, Task 5, is the fix).
                return Err("needs-auth".to_string());
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("HTTP 401: {text}"));
        }
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(format!("HTTP {status}: {text}"));
        }
        let ct = resp
            .headers()
            .get("Content-Type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if ct.contains("text/event-stream") {
            // An SSE stream: read until a `message` event whose `data` is a
            // JSON-RPC response for this `id`.
            return self.read_sse_response(resp, id, cancel).await;
        }
        // A JSON response.
        let v: Value = resp.json().await.map_err(|e| e.to_string())?;
        Self::response_to_result(&v)
    }

    /// A JSON-RPC response (a `result` → `Ok(result)`; an `error` →
    /// `Err(error.message)`).
    fn response_to_result(v: &Value) -> Result<Value, String> {
        let Some(resp) = v.as_object() else {
            return Err("the response is not a JSON object".to_string());
        };
        if let Some(e) = resp.get("error").and_then(Value::as_object) {
            let code = e.get("code").and_then(Value::as_i64).unwrap_or(-1);
            let message = e.get("message").and_then(Value::as_str).unwrap_or_default();
            return Err(format!("{message} (code {code})"));
        }
        Ok(resp.get("result").cloned().unwrap_or(Value::Null))
    }

    /// Read the response body (a `text/event-stream` SSE stream — `event:` /
    /// `data:` lines; a `message` event carries the JSON-RPC response; a
    /// `ping` / other event is ignored) and return the JSON-RPC `result` for
    /// `id`. The body is read whole (`resp.bytes()` — bounded by the
    /// `read_timeout` / a `Content-Length`; a `ping`-only stream that never
    /// delivers the `message` event times out on the `read_timeout`). Raced
    /// against `cancel`.
    async fn read_sse_response(
        &mut self,
        resp: reqwest::Response,
        id: u64,
        cancel: &CancellationToken,
    ) -> Result<Value, String> {
        // Read the body (raced against `cancel`).
        let body = tokio::select! {
            b = resp.bytes() => b,
            _ = cancel.cancelled() => return Err("cancelled".to_string()),
        }
        .map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&body);
        // Parse the SSE events (`event:` / `data:` lines; an empty line ends
        // an event).
        let mut event: Option<String> = None;
        let mut data: Vec<String> = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                // An empty line: the event is complete.
                let data_text = data.join("\n");
                data.clear();
                if event.as_deref() == Some("message") && !data_text.is_empty() {
                    if let Ok(v) = serde_json::from_str::<Value>(&data_text) {
                        if v.get("id").and_then(Value::as_u64) == Some(id) {
                            return Self::response_to_result(&v);
                        }
                    }
                }
                event = None;
            } else if let Some(rest) = line.strip_prefix("event:") {
                event = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                data.push(rest.trim().to_string());
            }
        }
        // The stream ended without a `message` event for `id`.
        Err("the SSE stream ended before a response".to_string())
    }

    /// A notification (NO `id` — no response expected; best-effort).
    async fn send_notification(&mut self, method: &str) -> Result<(), String> {
        let body = json!({ "jsonrpc": "2.0", "method": method });
        let mut req = self.client.post(&self.url);
        req = req.header("Content-Type", "application/json");
        req = req.header("Accept", "application/json, text/event-stream");
        if let Some(session) = &self.session_id {
            req = req.header("Mcp-Session-Id", session);
        }
        if let Some(a) = self
            .auth_header
            .clone()
            .or_else(|| self.get_token.as_ref().and_then(|f| f()))
        {
            req = req.header("Authorization", a);
        }
        for (k, v) in self.static_headers.iter() {
            req = req.header(k.as_str(), v.as_str());
        }
        let _resp = req.json(&body).send().await.map_err(|e| e.to_string())?;
        // A 2xx/204/202 = success (a notification has no response body); a
        // 4xx/5xx is NOT fatal (best-effort).
        Ok(())
    }

    /// `tools/list` → the server's tools (name + description +
    /// `inputSchema`).
    pub async fn list_tools(
        &mut self,
        cancel: &CancellationToken,
    ) -> Result<Vec<ToolInfo>, String> {
        let result = self.send_request("tools/list", json!({}), cancel).await?;
        Ok(result
            .get("tools")
            .and_then(Value::as_array)
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(|t| {
                        Some(ToolInfo {
                            name: t.get("name")?.as_str()?.to_string(),
                            description: t
                                .get("description")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            input_schema: t.get("inputSchema").cloned().unwrap_or(Value::Null),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// `tools/call` → the normalized result (the `content[]` text joined +
    /// the non-text count + `isError`).
    pub async fn call_tool(
        &mut self,
        name: &str,
        args: &Value,
        cancel: &CancellationToken,
    ) -> Result<ToolCallResult, String> {
        let result = self
            .send_request(
                "tools/call",
                json!({ "name": name, "arguments": args }),
                cancel,
            )
            .await?;
        Ok(extract_tool_call_result(&result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Build a raw HTTP response (a status line + headers + a `Content-Length`
    /// + a body).
    fn http_response(status: &str, headers: &str, body: &str) -> String {
        format!(
            "{status}\r\n{headers}\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    /// A raw HTTP server: reads the request (headers + body), applies the
    /// handler (raw request bytes → raw response bytes + whether to
    /// dribble the body in 50 ms chunks), and records the request headers
    /// (for the session-id test).
    /// The test server's handler (a raw request → a raw response + whether to
    /// dribble the body).
    type TestHandler = Box<dyn Fn(&str, &str) -> (String, bool) + Send>;

    /// Handle the requests on a connection (keep-alive — until the client
    /// closes it).
    async fn handle_connection(
        stream: &mut tokio::net::TcpStream,
        handler: &Mutex<TestHandler>,
        recorded: &Mutex<Vec<String>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = [0u8; 8192];
        loop {
            let mut data = Vec::new();
            // Read the request headers (until `\r\n\r\n`).
            while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                let Ok(n) = stream.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    return; // the client closed the connection.
                }
                data.extend_from_slice(&buf[..n]);
            }
            let header_end = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
            let headers = String::from_utf8_lossy(&data[..header_end + 4]).to_string();
            let content_length = headers
                .lines()
                .find_map(|l| {
                    l.to_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                })
                .unwrap_or(0);
            let mut body = data[header_end + 4..].to_vec();
            while body.len() < content_length {
                let Ok(n) = stream.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&buf[..n]);
            }
            let body_str = String::from_utf8_lossy(&body).to_string();
            recorded.lock().unwrap().push(headers.clone());
            let (response, dribble) = handler.lock().unwrap()(&headers, &body_str);
            if dribble {
                // Dribble ONLY the body (the headers go out immediately — a
                // real SSE server sends the headers at once and then streams
                // the body). Split at the header/body boundary (`\r\n\r\n`).
                let bytes_all = response.as_bytes();
                let sep = bytes_all
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .unwrap_or(0);
                if stream.write_all(&bytes_all[..sep + 4]).await.is_err() {
                    return;
                }
                let bytes = bytes_all[sep + 4..].to_vec();
                let mut i = 0;
                while i < bytes.len() {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    let end = (i + 1).min(bytes.len());
                    if stream.write_all(&bytes[i..end]).await.is_err() {
                        return;
                    }
                    i = end;
                }
            } else if stream.write_all(response.as_bytes()).await.is_err() {
                return;
            }
        }
    }

    fn http_server(
        listener: tokio::net::TcpListener,
        handler: Arc<Mutex<TestHandler>>,
        recorded: Arc<Mutex<Vec<String>>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                handle_connection(&mut stream, &handler, &recorded).await;
            }
        })
    }

    /// A `HttpDef` (a URL + an optional auth).
    fn http_def(url: &str, auth: AuthSpec) -> HttpDef {
        HttpDef {
            url: url.to_string(),
            headers: Default::default(),
            auth,
        }
    }

    fn cancel() -> CancellationToken {
        CancellationToken::new()
    }

    /// A canned `tools/list` / `tools/call` result (a `Value`).
    fn canned_result(method: &str, v: &Value) -> Value {
        match method {
            "initialize" => json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "serverInfo": { "name": "fake-http", "version": "0.1.0" }
            }),
            "tools/list" => json!({
                "tools": [{ "name": "echo", "description": "echo", "inputSchema": { "type": "object" } }]
            }),
            "tools/call" => {
                let t = v["params"]["arguments"]["text"]
                    .as_str()
                    .unwrap_or_default();
                json!({ "content": [{ "type": "text", "text": t }] })
            }
            _ => json!({}),
        }
    }

    /// A handler that answers with a JSON response.
    fn json_handler(extra_headers: &'static str) -> TestHandler {
        Box::new(move |_h, body| {
            let v: Value = serde_json::from_str(body).expect("a JSON body");
            let method = v["method"].as_str().unwrap_or_default();
            let id = v["id"].clone();
            let result = canned_result(method, &v);
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string();
            (
                http_response(
                    "HTTP/1.1 200 OK",
                    &format!("Content-Type: application/json{extra_headers}"),
                    &resp,
                ),
                false,
            )
        })
    }

    /// A handler that answers with an SSE response.
    fn sse_handler(dribble: bool) -> TestHandler {
        Box::new(move |_h, body| {
            let v: Value = serde_json::from_str(body).expect("a JSON body");
            let method = v["method"].as_str().unwrap_or_default();
            let id = v["id"].clone();
            let result = canned_result(method, &v);
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string();
            (
                http_response(
                    "HTTP/1.1 200 OK",
                    "Content-Type: text/event-stream",
                    &format!("event: message\r\ndata: {resp}\r\n\r\n"),
                ),
                dribble,
            )
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_json_response_round_trip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(json_handler("")));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        let mut client = HttpClient::connect(
            &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
            Duration::from_secs(5),
            &cancel(),
            None,
        )
        .await
        .expect("the handshake completes");
        let tools = client
            .list_tools(&cancel())
            .await
            .expect("tools/list answers");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "echo");
        let r = client
            .call_tool("echo", &json!({ "text": "hi" }), &cancel())
            .await
            .expect("the call answers");
        assert_eq!(r.text, "hi");
        assert!(!r.is_error);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_sse_response_round_trip() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(sse_handler(false)));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        let mut client = HttpClient::connect(
            &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
            Duration::from_secs(5),
            &cancel(),
            None,
        )
        .await
        .expect("the handshake completes");
        let r = client
            .call_tool("echo", &json!({ "text": "sse" }), &cancel())
            .await
            .expect("the SSE call answers");
        assert_eq!(r.text, "sse");
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_session_id_is_resent() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        // A JSON handler that ALSO sets a `Mcp-Session-Id` response header.
        let handler: Arc<Mutex<TestHandler>> =
            Arc::new(Mutex::new(json_handler("\r\nMcp-Session-Id: sess-123")));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        let mut client = HttpClient::connect(
            &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
            Duration::from_secs(5),
            &cancel(),
            None,
        )
        .await
        .expect("the handshake completes");
        let _ = client
            .list_tools(&cancel())
            .await
            .expect("tools/list answers");
        // The recorded requests: the 2nd (tools/list) MUST carry the
        // `Mcp-Session-Id: sess-123` header (the 1st — initialize — set it).
        let headers = recorded.lock().unwrap().clone();
        assert!(headers.len() >= 2, "at least 2 requests were recorded");
        assert!(
            headers[1]
                .to_lowercase()
                .contains("mcp-session-id: sess-123"),
            "the 2nd request resends the session id, got:\n{}",
            headers[1]
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_401_on_an_oauth_server_is_needs_auth() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        // A server that answers with a 401 (an OAuth server — the special
        // `needs-auth` marker, NOT a generic HTTP error).
        let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(Box::new(|_h, _body| {
            (
                http_response(
                    "HTTP/1.1 401 Unauthorized",
                    "Content-Type: application/json",
                    "{}",
                ),
                false,
            )
        })));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        let r = HttpClient::connect(
            &http_def(
                &format!("http://{addr}/mcp"),
                AuthSpec::OAuth {
                    grant_type: Some("authorization_code".into()),
                    client_id: None,
                    client_secret: None,
                    scope: None,
                    redirect_uri: None,
                    client_name: None,
                    authorization_server_url: None,
                },
            ),
            Duration::from_secs(5),
            &cancel(),
            None, // no token yet → unauthenticated → the 401.
        )
        .await;
        match r {
            Err(e) => assert_eq!(e, "needs-auth", "a 401 on an OAuth server is `needs-auth`"),
            Ok(_) => panic!("a 401 on an OAuth server should be an error, not Ok"),
        }
        server.abort();
    }

    /// A handler that responds quickly to `initialize` / `tools/list` (JSON)
    /// but DRIBBLES the `tools/call` response (an SSE `message` event in 50 ms
    /// chunks — a healthy slow stream, under the `read_timeout`).
    fn slow_handler() -> TestHandler {
        Box::new(move |_h, body| {
            let v: Value = serde_json::from_str(body).expect("a JSON body");
            let method = v["method"].as_str().unwrap_or_default();
            let id = v["id"].clone();
            let result = canned_result(method, &v);
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string();
            if method == "tools/call" {
                // Dribble the `tools/call` response (an SSE `message` event).
                (
                    http_response(
                        "HTTP/1.1 200 OK",
                        "Content-Type: text/event-stream",
                        &format!("event: message\r\ndata: {resp}\r\n\r\n"),
                    ),
                    true,
                )
            } else {
                // A fast JSON response (the handshake / `tools/list`).
                (
                    http_response("HTTP/1.1 200 OK", "Content-Type: application/json", &resp),
                    false,
                )
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_a_slow_sse_stream_completes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        // A handler that DRIBBLES the `tools/call` response (a `message` event
        // in 50 ms chunks — a healthy slow stream, under the `read_timeout`)
        // but answers the handshake quickly.
        let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(slow_handler()));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        // A short `read_timeout` (1 s) — the dribble (50 ms/byte) is well
        // under it, so the stream completes. 1 s, not the original 200 ms:
        // CI runners under full-suite load produced a single scheduling gap
        // over 200 ms (macOS, run 38750373114 — "error decoding response
        // body" = the read idle bound firing on a healthy stream), and the
        // test's POINT is only that the dribble stays under the bound —
        // 20× headroom proves it as well as 4× did, and stops the coin
        // flip. The bound stays SMALL on purpose: it must stay far below
        // what a genuinely hung stream would take to trip some OTHER test.
        let mut client = HttpClient::connect(
            &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
            Duration::from_secs(1),
            &cancel(),
            None,
        )
        .await
        .expect("the handshake completes");
        let r = client
            .call_tool("echo", &json!({ "text": "slow" }), &cancel())
            .await
            .expect("the slow SSE stream completes (the idle bound does not cut it)");
        assert_eq!(r.text, "slow");
        server.abort();
    }

    /// Poll the harness's request recorder until at least `want` requests have
    /// been RECORDED. A recorded request proves the server READ it (the record
    /// at `handle_connection` precedes the response write), which is the
    /// ARRIVAL PROOF the cancel phases need — a cancel that fires before the
    /// request arrives is the SEND arm (`:164`), never the READ arm (`:235`).
    /// Returns whether the threshold was reached before `limit`, so the caller
    /// FAILS LOUDLY on a miss instead of hanging on an un-armed cancel.
    async fn wait_for_recorded(
        recorded: &Arc<Mutex<Vec<String>>>,
        want: usize,
        limit: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            if recorded.lock().unwrap().len() >= want {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A server for the SEND-arm cancel phase: it answers the `initialize`
    /// handshake and `notifications/initialized` FAST (JSON, via
    /// `canned_result`) — so the client reaches `tools/call` — and then NEVER
    /// ANSWERS the `tools/call`: it records the request and awaits a 6 s sleep
    /// before writing a SINGLE response byte.
    ///
    /// Why this fixture is test-local rather than a `TestHandler` over
    /// `http_server`: the harness handler is a SYNC
    /// `Box<dyn Fn(&str, &str) -> (String, bool) + Send>` — it cannot
    /// `tokio::time::sleep` (a nested runtime panics) and `std::thread::sleep`
    /// is forbidden here (it would occupy one of this runtime's 4 worker
    /// threads for the whole stall). The stall is an `await` inside the spawned
    /// server task, which means the fixture owns its request read (the same
    /// `\r\n\r\n` + `Content-Length` discipline `handle_connection` uses).
    ///
    /// The 6 s stall EXCEEDS the client's hardcoded 5 s `connect_timeout`
    /// (`build_client` :58) on purpose, so the fixture can never turn the
    /// observable into a timeout; the cancel lands ~100 ms after the request is
    /// recorded, so the stall is never waited out — it only has to outlive the
    /// assertion.
    fn unanswered_call_server(
        listener: tokio::net::TcpListener,
        recorded: Arc<Mutex<Vec<String>>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 8192];
            'accept: loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut data = Vec::new();
                loop {
                    // The request headers (until `\r\n\r\n`).
                    while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                        let Ok(n) = stream.read(&mut buf).await else {
                            continue 'accept;
                        };
                        if n == 0 {
                            continue 'accept; // the client closed the connection.
                        }
                        data.extend_from_slice(&buf[..n]);
                    }
                    let header_end = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
                    let headers = String::from_utf8_lossy(&data[..header_end + 4]).to_string();
                    let content_length = headers
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    let mut body = data[header_end + 4..].to_vec();
                    while body.len() < content_length {
                        let Ok(n) = stream.read(&mut buf).await else {
                            continue 'accept;
                        };
                        if n == 0 {
                            break;
                        }
                        body.extend_from_slice(&buf[..n]);
                    }
                    let consumed = (header_end + 4 + content_length).min(data.len());
                    data = data[consumed..].to_vec();
                    let body_str = String::from_utf8_lossy(&body).to_string();
                    recorded.lock().unwrap().push(headers);
                    let v: Value = serde_json::from_str(&body_str).unwrap_or(Value::Null);
                    let method = v["method"].as_str().unwrap_or_default().to_string();
                    if method == "tools/call" {
                        // NEVER answered — not one response byte is written.
                        tokio::time::sleep(Duration::from_secs(6)).await;
                        return;
                    }
                    let resp = json!({
                        "jsonrpc": "2.0",
                        "id": v["id"].clone(),
                        "result": canned_result(&method, &v)
                    })
                    .to_string();
                    let resp =
                        http_response("HTTP/1.1 200 OK", "Content-Type: application/json", &resp);
                    if stream.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            }
        })
    }

    /// The mid-flight cancel observable: the EXACT string `"cancelled"`,
    /// produced by two `tokio::select!` arms — the SEND arm (`:164`, racing
    /// `req.json(&body).send()`) and the SSE READ arm (`:235`, racing
    /// `resp.bytes()` inside `read_sse_response`). Both arms return the
    /// IDENTICAL `Err`, so NO assertion can tell them apart; each phase below
    /// therefore makes its own arm the ONLY reachable one and states the
    /// argument in place. (The stdio sibling pins the same string —
    /// `stdio.rs:222` / the pin at `stdio.rs:484`.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_cancel_mid_request_returns_err_cancelled() {
        // ── phase (a) — the SEND arm (`:164`): the response NEVER starts ──
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the listener binds");
            let addr = listener.local_addr().expect("the listener has an address");
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let server = unanswered_call_server(listener, recorded.clone());
            // The `read_timeout` (the `timeout` arg — `build_client` :59) is
            // 10 s: LONG, so NOTHING but the cancel can end the call (the
            // fixture stalls 6 s, the `connect_timeout` is 5 s — a short read
            // bound here would let a timeout masquerade as the observable).
            let mut client = HttpClient::connect(
                &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
                Duration::from_secs(10),
                &cancel(),
                None,
            )
            .await
            .expect("the handshake completes (the fixture answers it fast)");
            let c = cancel();
            let gate_recorded = recorded.clone();
            let gate_token = c.clone();
            let gate = tokio::spawn(async move {
                // Arrival proof FIRST (a RECORDED request means the server
                // READ the `tools/call`), then 100 ms inside the stall, THEN
                // cancel. The bool is returned, not asserted in this task, so
                // a gate miss fails the TEST instead of hanging it.
                let arrived = wait_for_recorded(&gate_recorded, 3, Duration::from_secs(10)).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                gate_token.cancel();
                arrived
            });
            let started = tokio::time::Instant::now();
            let r = client
                .call_tool("echo", &json!({ "text": "never" }), &c)
                .await;
            let elapsed = started.elapsed();
            let arrived = gate.await.expect("the cancel gate is not cancelled");
            let rec = recorded.lock().unwrap().clone();
            server.abort();
            // THE ARM ARGUMENT (as built): the `tools/call` was RECORDED (it
            // arrived) and this fixture writes NO response byte before its
            // 6 s stall, so `send()` (:162) had nothing to resolve on — and
            // `read_sse_response` (holding the :235 arm) is only ever entered
            // from a RESOLVED response whose `Content-Type` is
            // `text/event-stream` (:195), so the READ arm was structurally
            // unreachable. Only :164 could have returned. A gate miss would
            // mean the cancel fired before arrival, so this FAILS LOUDLY.
            assert!(
                arrived,
                "the `tools/call` was recorded BEFORE the cancel was armed (a \
                 pre-arrival cancel is the READ arm, not the SEND arm); got {} \
                 request(s):\n{}",
                rec.len(),
                rec.join("\n---\n")
            );
            // The observable is the CANCEL, not a timeout: 100 ms of gate +
            // the stall (6 s) and the read bound (10 s) are all far above the
            // elapsed time, and 50 ms is the floor the gate's own poll leaves.
            assert!(
                elapsed < Duration::from_secs(3),
                "the cancel won the race (a timeout would take >= 5 s), got {elapsed:?}"
            );
            match r {
                Err(e) => assert_eq!(
                    e, "cancelled",
                    "a mid-request cancel errors the exact string `cancelled` (the SEND arm, :164)"
                ),
                Ok(v) => panic!("a cancelled tools/call must be an Err, got Ok({v:?})"),
            }
        }

        // ── phase (b) — the SSE READ arm (`:235`): headers in, body mid-flight ──
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the listener binds");
            let addr = listener.local_addr().expect("the listener has an address");
            let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(slow_handler()));
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let server = http_server(listener, handler, recorded.clone());
            // THE `read_timeout` FOR THIS TEST: 20 s (the `timeout` arg IS the
            // reqwest `read_timeout` — `build_client` :59). It must be LONG —
            // LONGER THAN THE WHOLE DRIBBLE (14.95 s, see below) — because the
            // read side of `resp.bytes()` (:234) has to still be PENDING when
            // the cancel fires: a SHORT bound would end the read with a
            // timeout and the cancel arm would never be reached. (The dribble
            // writes a byte every 50 ms, so the IDLE bound never trips on a
            // healthy stream — that is `http_a_slow_sse_stream_completes`'s
            // territory; here the bound only has to stay out of the way.)
            let mut client = HttpClient::connect(
                &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
                Duration::from_secs(20),
                &cancel(),
                None,
            )
            .await
            .expect("the handshake completes");
            let c = cancel();
            let gate_recorded = recorded.clone();
            let gate_token = c.clone();
            let gate = tokio::spawn(async move {
                // GATE 1 — arrival: the `tools/call` request is RECORDED (the
                // 3rd request: initialize, the notification, then the call).
                // A cancel before this point would be the SEND arm (:164)
                // while claiming the READ arm — the false positive this gate
                // exists to rule out.
                let arrived = wait_for_recorded(&gate_recorded, 3, Duration::from_secs(10)).await;
                // GATE 2 — the headers are on the wire, and the client has
                // retired its `send()` future. `handle_connection` writes the
                // SSE headers (:414) in the SAME iteration that recorded the
                // request, before any 50 ms/byte chunk, so the headers are
                // provably written; the 1 s then absorbs that single write AND
                // lets the client observe the headers and enter
                // `read_sse_response` — `tokio::select!` picks RANDOMLY when
                // both arms are ready, so the margin must cover the
                // send()→read transition, not just the server's write.
                tokio::time::sleep(Duration::from_millis(1000)).await;
                gate_token.cancel();
                arrived
            });
            let started = tokio::time::Instant::now();
            // 200 bytes of text → a 299-byte SSE body (`event: message\r\n` +
            // `data: ` + the 95-byte JSON-RPC envelope + the 200-byte text +
            // `\r\n\r\n`) → a 14.95 s DRIBBLE (50 ms/byte, `handle_connection`
            // :417-424 — the harness sleeps BETWEEN bytes). The cancel lands
            // ~1 s in, so ~14 s of body is STILL dribbling: `resp.bytes()`
            // (which resolves only at the END of the body) was provably still
            // pending — the cancel sits ~15× inside the dribble.
            let text = "d".repeat(200);
            let r = client.call_tool("echo", &json!({ "text": text }), &c).await;
            let elapsed = started.elapsed();
            let arrived = gate.await.expect("the cancel gate is not cancelled");
            let rec = recorded.lock().unwrap().clone();
            server.abort();
            assert!(
                arrived,
                "the `tools/call` was recorded BEFORE the cancel was armed (a \
                 pre-arrival cancel is the SEND arm :164, not the READ arm); got \
                 {} request(s):\n{}",
                rec.len(),
                rec.join("\n---\n")
            );
            // THE ARM ARGUMENT (as built): arrival (gate 1) + the SSE headers
            // written and observed with a 1 s margin (gate 2) + ~14 s of body
            // still to dribble ⇒ the ONLY live arm at the cancel is the
            // `resp.bytes()` select at :233-236. The elapsed bounds pin that
            // the cancel was neither premature (>= 900 ms: it could not fire
            // before its own 1 s gate) nor late enough for the read to finish
            // (< 5 s: a COMPLETED read would have returned `Ok`).
            assert!(
                elapsed >= Duration::from_millis(900),
                "the cancel fired only after its 1 s post-arrival gate, got {elapsed:?}"
            );
            // The UPPER bound is deliberately generous, and its number is
            // DERIVED, not guessed: the arrival gate above is itself bounded at
            // 10 s, and the cancel fires 1 s after arrival, so the observable
            // cannot legitimately arrive later than ~11 s. 12 s is that bound
            // plus slack. A TIGHT ceiling (the 5 s this shipped as during
            // development) adds nothing the `match` below does not already
            // prove — a completed read returns `Ok`, which the `match` rejects —
            // and it would flake on a loaded machine, because `arrival` waits
            // for THREE recorded requests (handshake + notification + call) and
            // under full-suite load the arrival alone can exceed a tight
            // ceiling and redden a correct test. That is the wall-clock-flake
            // class AGENTS.md records for `Reasoning.test.tsx` and for this
            // module's own `http_a_slow_sse_stream_completes`.
            assert!(
                elapsed < Duration::from_secs(12),
                "the cancel won a ~15 s dribble (a completed read would be Ok), got {elapsed:?}"
            );
            match r {
                Err(e) => assert_eq!(
                    e, "cancelled",
                    "a cancel during the SSE body read errors the exact string `cancelled` (the READ arm, :235)"
                ),
                Ok(v) => panic!("a cancelled tools/call must be an Err, got Ok({v:?})"),
            }
        }
    }

    /// Every decision `response_to_result` (`:207`) can make, pinned at the
    /// unit level (an in-module test calls the private fn directly — no
    /// listener), then the two server-facing variants pinned THROUGH THE WIRE
    /// (the JSON transport and the SSE transport), so the shape logic is
    /// pinned at both levels without duplicating the round-trip tests.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_response_to_result_shape_branches() {
        // ── the unit-level branches (the function at :207-220) ──
        // A top-level NON-object (`as_object` fails) — the `else` at :208-210.
        for not_object in [
            json!([1, 2]),
            json!("a string"),
            json!(7),
            json!(null),
            json!(true),
        ] {
            assert_eq!(
                HttpClient::response_to_result(&not_object),
                Err("the response is not a JSON object".to_string()),
                "a top-level non-object is the `the response is not a JSON object` error"
            );
        }
        // An `error` object with code + message — the exact `format!` shape at
        // :215 (`"{message} (code {code})"`), byte-exact including the space.
        assert_eq!(
            HttpClient::response_to_result(&json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": -32601, "message": "no such tool" }
            })),
            Err("no such tool (code -32601)".to_string()),
            "an error object formats as `<message> (code <n>)`"
        );
        // An `error` object MISSING `message` — the `unwrap_or_default()`
        // branch at :214: the message is `""`, so the string LEADS with the
        // space the `format!` puts between them.
        assert_eq!(
            HttpClient::response_to_result(&json!({
                "jsonrpc": "2.0", "id": 1, "error": { "code": 1 }
            })),
            Err(" (code 1)".to_string()),
            "an error without a message is the empty-message shape ` (code <n>)`"
        );
        // An `error` object MISSING `code` — the `unwrap_or(-1)` branch at :213.
        assert_eq!(
            HttpClient::response_to_result(&json!({
                "jsonrpc": "2.0", "id": 1, "error": { "message": "b" }
            })),
            Err("b (code -1)".to_string()),
            "an error without a code reports `code -1`"
        );
        // THE FLOAT-`code` QUIRK — PINNED BY DECISION (§1.2 of the plan: the
        // vet record forbids leaving this corner undecided, so it is PINNED,
        // not excluded). A server sending a NON-INTEGER code: `Value::as_i64`
        // FAILS on a JSON float (`serde_json`'s documented semantics), so
        // `unwrap_or(-1)` at :213 kicks in and the code reports `-1` — EXACTLY
        // as if `code` were absent. This tree's decision; if the shape logic is
        // ever reworked to preserve a float code, THIS PIN BREAKS LOUDLY, which
        // is the point of pinning it.
        assert_eq!(
            HttpClient::response_to_result(&json!({
                "jsonrpc": "2.0", "id": 1,
                "error": { "code": 1.5, "message": "b" }
            })),
            Err("b (code -1)".to_string()),
            "a float `code` is not an i64, so it collapses to `code -1` (the pinned quirk)"
        );
        // A `result` present → `Ok(cloned result)` (the `.cloned()` at :217).
        let result = json!({ "tools": [{ "name": "echo" }] });
        assert_eq!(
            HttpClient::response_to_result(&json!({
                "jsonrpc": "2.0", "id": 2, "result": result
            })),
            Ok(result.clone()),
            "a `result` is returned cloned"
        );
        // A `result` ABSENT (and no `error`) → the `unwrap_or(Value::Null)`
        // branch at :217. NOTE: an `error` that is NOT an object (a string)
        // falls through the `.and_then(Value::as_object)` at :212 to here — so
        // it is `Ok(Null)`, NOT an error; pinned as part of the same branch.
        assert_eq!(
            HttpClient::response_to_result(&json!({ "jsonrpc": "2.0", "id": 3 })),
            Ok(Value::Null),
            "a response with no `result` is `Ok(Value::Null)`"
        );
        assert_eq!(
            HttpClient::response_to_result(&json!({
                "jsonrpc": "2.0", "id": 3, "error": "a malformed string error"
            })),
            Ok(Value::Null),
            "a NON-object `error` is not an error at all — it falls through to `Ok(Null)`"
        );

        // ── the two server-facing variants, THROUGH THE WIRE ──
        // A server that answers the `initialize` handshake normally (so the
        // transport itself is proven good) and then answers `tools/list` with a
        // JSON-RPC `error` — the error must surface from `list_tools` in the
        // SAME shape the unit calls pinned. `sse` picks the transport.
        let error_handler = |sse: bool| -> TestHandler {
            Box::new(move |_h: &str, body: &str| {
                let v: Value = serde_json::from_str(body).expect("a JSON body");
                let method = v["method"].as_str().unwrap_or_default();
                let id = v["id"].clone();
                let payload = if method == "initialize" {
                    json!({ "jsonrpc": "2.0", "id": id, "result": canned_result(method, &v) })
                } else {
                    json!({
                        "jsonrpc": "2.0", "id": id,
                        "error": { "code": -32601, "message": "no such tool" }
                    })
                }
                .to_string();
                if sse {
                    (
                        http_response(
                            "HTTP/1.1 200 OK",
                            "Content-Type: text/event-stream",
                            &format!("event: message\r\ndata: {payload}\r\n\r\n"),
                        ),
                        false,
                    )
                } else {
                    (
                        http_response(
                            "HTTP/1.1 200 OK",
                            "Content-Type: application/json",
                            &payload,
                        ),
                        false,
                    )
                }
            })
        };
        for (transport, sse) in [("JSON", false), ("SSE", true)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("the listener binds");
            let addr = listener.local_addr().expect("the listener has an address");
            let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(error_handler(sse)));
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let server = http_server(listener, handler, recorded);
            let mut client = HttpClient::connect(
                &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
                Duration::from_secs(5),
                &cancel(),
                None,
            )
            .await
            .expect("the handshake completes (the server answers `initialize` fine)");
            let r = client.list_tools(&cancel()).await;
            server.abort();
            assert_eq!(
                r,
                Err("no such tool (code -32601)".to_string()),
                "a JSON-RPC `error` arriving over the {transport} transport surfaces in the pinned shape"
            );
        }
    }

    /// The `Bearer` prefix ON THE WIRE for a static-header auth spec. Every
    /// other test here runs `AuthSpec::None` or `AuthSpec::OAuth`, so nothing
    /// pinned that the `format!("Bearer {t}")` built at connect time (`:86`)
    /// actually lands as an `Authorization` request header (`:157` in
    /// `send_request`, `:281` in `send_notification`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_bearer_prefix_on_the_wire_for_a_static_header_spec() {
        // A literal token (NOT the `env_var` branch — that is a connect-time
        // resolution decision, not a wire byte, and is out of scope here; and
        // NOT the OAuth path, which is `http_401_on_an_oauth_server_is_needs_auth`'s
        // territory).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(json_handler("")));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        let mut client = HttpClient::connect(
            &http_def(
                &format!("http://{addr}/mcp"),
                AuthSpec::Bearer {
                    token: Some("s3cr3t-token".to_string()),
                    env_var: None,
                },
            ),
            Duration::from_secs(5),
            &cancel(),
            None,
        )
        .await
        .expect("the handshake completes");
        let _ = client
            .list_tools(&cancel())
            .await
            .expect("tools/list answers");
        let _ = client
            .call_tool("echo", &json!({ "text": "auth" }), &cancel())
            .await
            .expect("tools/call answers");
        let headers = recorded.lock().unwrap().clone();
        server.abort();
        // 3 id-bearing requests + the `notifications/initialized` notification
        // — the pin below covers ALL of them, so a leak on ANY request fails.
        assert!(
            headers.len() >= 3,
            "the handshake + the notification + tools/list + tools/call were recorded, got {}",
            headers.len()
        );
        // The `initialize` request (index 0) carries the prefix. Compared on the
        // LOWERCASED header line (the session-id test's pattern at :605-610):
        // the header NAME's casing on the wire is reqwest's, and the compare
        // pins the `bearer ` PREFIX + the token, not reqwest's spelling.
        let initialize = headers[0].to_lowercase();
        assert!(
            initialize.contains("authorization: bearer s3cr3t-token"),
            "the initialize request carries `Authorization: Bearer <token>` on the wire, got:\n{}",
            headers[0]
        );
        // EVERY request of the connection carries it (the header is fixed at
        // connect time and re-attached per request — `:152-157` and the
        // notification's identical path at `:276-282`).
        for (i, h) in headers.iter().enumerate() {
            assert!(
                h.to_lowercase()
                    .contains("authorization: bearer s3cr3t-token"),
                "request {i} also carries the `Bearer` prefix, got:\n{h}"
            );
        }

        // ── the negative control: `AuthSpec::None` sends NO `Authorization` ──
        // Its OWN server + client (a fresh connection, so the recorder can only
        // hold this client's bytes).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let handler: Arc<Mutex<TestHandler>> = Arc::new(Mutex::new(json_handler("")));
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, handler, recorded.clone());
        let mut client = HttpClient::connect(
            &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
            Duration::from_secs(5),
            &cancel(),
            None,
        )
        .await
        .expect("the handshake completes");
        let _ = client
            .list_tools(&cancel())
            .await
            .expect("tools/list answers");
        let headers = recorded.lock().unwrap().clone();
        server.abort();
        assert!(
            headers.len() >= 2,
            "the unauthenticated connection recorded its requests, got {}",
            headers.len()
        );
        for (i, h) in headers.iter().enumerate() {
            assert!(
                !h.to_lowercase().contains("authorization:"),
                "an `AuthSpec::None` connection sends NO `Authorization` header at all, but request {i} carries one:\n{h}"
            );
        }
    }

    /// The exact BYTES of the requests this client puts on the wire — the
    /// request line, the headers this crate sets, the JSON-RPC body, and the
    /// id sequence. The other tests assert on BEHAVIOUR (a round-trip, a
    /// resend, a 401); nothing pinned the bytes themselves, so a change to the
    /// construction at `:141-160` that kept behaviour intact would have been
    /// invisible.
    ///
    /// **What "byte-exact" claims, and what it does NOT** (the honest scope):
    /// - **Claimed:** the compact JSON body, byte for byte; and NAMED header
    ///   substrings (`POST /mcp HTTP/1.1`, `content-type:`, `accept:`,
    ///   `x-custom:`) matched as substrings of the recorded request.
    /// - **NOT claimed:** the `Host:` header, reqwest's `Content-Length:`, and
    ///   the ORDER of headers on the wire — those are reqwest/hyper's output,
    ///   not this crate's, and may move with a reqwest bump. Nothing here pins
    ///   them.
    /// - **The body IS deterministic in this tree,** because `serde_json`
    ///   1.0.151 (locked) has `preserve_order` OFF, so `Map` is a `BTreeMap`
    ///   and a given `Value` serializes in SORTED key order — byte-identical
    ///   run to run. (Note: that means the bytes follow the MAP's sorted order,
    ///   NOT the `json!` literal order — the keys of the body are written in
    ///   `jsonrpc,id,method,params` order in the source and arrive as
    ///   `id,jsonrpc,method,params`.)
    /// - **The pin is version-coupled:** the first body embeds
    ///   `env!("CARGO_PKG_VERSION")` (the `initialize` `clientInfo.version`),
    ///   so a version bump breaks this test LOUDLY and on purpose — a bump
    ///   forces the re-pin to happen with eyes open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_exact_request_bytes_on_the_wire() {
        // Bodies are NOT what `recorded` holds (it stores the request line +
        // headers — `handle_connection` pushes `headers.clone()`), so the
        // bodies ride out through the handler closure on their own recorder.
        // Still the existing harness — no second server abstraction.
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let bodies_in = bodies.clone();
        let handler: TestHandler = Box::new(move |_headers, body| {
            bodies_in.lock().unwrap().push(body.to_string());
            let v: Value = serde_json::from_str(body).expect("a JSON body");
            let method = v["method"].as_str().unwrap_or_default();
            let id = v["id"].clone();
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": canned_result(method, &v) })
                .to_string();
            (
                http_response("HTTP/1.1 200 OK", "Content-Type: application/json", &resp),
                false,
            )
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let server = http_server(listener, Arc::new(Mutex::new(handler)), recorded.clone());
        // A DIRECT `HttpDef` literal: `http_def` hardcodes EMPTY headers, and
        // this test's whole point includes the static-header loop at `:160`.
        let mut def = http_def(&format!("http://{addr}/mcp"), AuthSpec::None);
        def.headers = BTreeMap::from([("X-Custom".to_string(), "v1".to_string())]);
        let mut client = HttpClient::connect(&def, Duration::from_secs(5), &cancel(), None)
            .await
            .expect("the handshake completes");
        let _ = client
            .list_tools(&cancel())
            .await
            .expect("tools/list answers");
        let _ = client
            .call_tool("echo", &json!({ "text": "bytes" }), &cancel())
            .await
            .expect("tools/call answers");
        let headers = recorded.lock().unwrap().clone();
        let bodies = bodies.lock().unwrap().clone();
        server.abort();

        // initialize, notifications/initialized, tools/list, tools/call.
        assert_eq!(
            headers.len(),
            4,
            "four requests on one connection, got {}:\n{}",
            headers.len(),
            headers.join("\n---\n")
        );
        assert_eq!(bodies.len(), 4, "four bodies captured");

        // ── the request line (the URL path is test-controlled) ──
        for (i, h) in headers.iter().enumerate() {
            assert!(
                h.starts_with("POST /mcp HTTP/1.1\r\n"),
                "request {i} is a `POST` to the def's path, got: {h}"
            );
        }
        // ── the headers THIS crate sets (named substrings, lowercased: the
        // wire casing is reqwest's; the compare pins the value, not the case) ──
        for (i, h) in headers.iter().enumerate() {
            let low = h.to_lowercase();
            assert!(
                low.contains("content-type: application/json"),
                "request {i} declares the JSON content type (:144), got: {h}"
            );
            assert!(
                low.contains("accept: application/json, text/event-stream"),
                "request {i} advertises both response shapes (:145), got: {h}"
            );
            assert!(
                low.contains("x-custom: v1"),
                "request {i} carries the def's static header — the `static_headers` \
                 loop at :160 runs on EVERY request, got: {h}"
            );
        }
        // ── the session id is LEARNED, so the first request cannot carry it ──
        assert!(
            !headers[0].to_lowercase().contains("mcp-session-id:"),
            "the first request cannot resend a session id it has not learned yet, got: {}",
            headers[0]
        );
        // (the resend itself is `http_session_id_is_resent`'s — not duplicated here)

        // ── the id sequence: 1 (initialize), NONE (the notification), 2, 3 ──
        let parsed: Vec<Value> = bodies
            .iter()
            .map(|b| serde_json::from_str(b).expect("a JSON-RPC body"))
            .collect();
        assert_eq!(parsed[0]["method"], "initialize");
        assert_eq!(parsed[0]["id"], json!(1));
        assert_eq!(parsed[1]["method"], "notifications/initialized");
        assert!(
            parsed[1].get("id").is_none(),
            "a notification carries NO id (:269 builds `{{jsonrpc, method}}` only), got: {}",
            bodies[1]
        );
        assert_eq!(
            parsed[1].get("params"),
            None,
            "a notification carries no params"
        );
        assert_eq!(parsed[2]["method"], "tools/list");
        assert_eq!(parsed[2]["id"], json!(2));
        assert_eq!(parsed[3]["method"], "tools/call");
        assert_eq!(parsed[3]["id"], json!(3));

        // ── the body SHAPES (the contract, not just the bytes) ──
        assert_eq!(parsed[0]["jsonrpc"], "2.0");
        assert_eq!(parsed[0]["params"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(parsed[0]["params"]["clientInfo"]["name"], "archimedes");
        assert_eq!(
            parsed[0]["params"]["clientInfo"]["version"],
            env!("CARGO_PKG_VERSION"),
            "the version on the wire is the CRATE's version, read the way production \
             reads it — never a hardcoded literal"
        );
        assert_eq!(
            parsed[2]["params"],
            json!({}),
            "tools/list sends EMPTY params"
        );
        assert_eq!(
            parsed[3]["params"],
            json!({ "name": "echo", "arguments": { "text": "bytes" } }),
            "tools/call params are `{{name, arguments}}` — the arguments are NOT \
             flattened into params (that shape is the contract)"
        );

        // ── the BYTES of the first body, pinned ──
        // Sorted-key order (the `BTreeMap` `Map` — `preserve_order` off), which
        // is why `id` leads. If this breaks, diff the two strings: a version
        // bump changes `clientInfo.version` and is the EXPECTED reason.
        let expected_first = format!(
            "{{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"initialize\",\"params\":{{\"capabilities\":{{}},\"clientInfo\":{{\"name\":\"archimedes\",\"version\":\"{}\"}},\"protocolVersion\":\"{}\"}}}}",
            env!("CARGO_PKG_VERSION"),
            PROTOCOL_VERSION
        );
        assert_eq!(
            bodies[0], expected_first,
            "the initialize body is pinned byte-for-byte (version-coupled — see the \
             test's doc comment)"
        );
    }
}

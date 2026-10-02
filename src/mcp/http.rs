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
        // A short `read_timeout` (200 ms) — the dribble (50 ms/byte) is
        // well under it, so the stream completes.
        let mut client = HttpClient::connect(
            &http_def(&format!("http://{addr}/mcp"), AuthSpec::None),
            Duration::from_millis(200),
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
}

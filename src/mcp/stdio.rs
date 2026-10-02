//! The MCP stdio transport client (ADR 0018): spawn the server process
//! (`tokio::process`, `kill_on_drop`), speak JSON-RPC 2.0 over its
//! stdin/stdout as NEWLINE-DELIMITED lines, do the `initialize` →
//! `notifications/initialized` handshake, then `tools/list` /
//! `tools/call`.
//!
//! Concurrency: a WRITER task (mpsc → stdin lines) + a READER task
//! (stdout lines → routed to the pending request by `id`; a process
//! exit fails every pending request). Every operation races the TURN
//! `CancellationToken` (a Stop cancels the call — the process is
//! killed) and a per-request timeout (a stalled server ERRORS, it does
//! not hang the turn).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::rpc::{parse_notification, parse_response, RpcRequest, RpcResponse};
use super::types::{extract_tool_call_result, StdioDef, ToolCallResult, ToolInfo};

const PROTOCOL_VERSION: &str = "2025-06-18";

/// A connected stdio MCP server (a spawned child process speaking
/// newline-delimited JSON-RPC 2.0).
pub struct StdioClient {
    /// The writer task's line channel (a send failure = the process died).
    tx: mpsc::Sender<String>,
    /// Pending request `id` → its response oneshot (the reader routes
    /// lines by `id`).
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<RpcResponse>>>>,
    next_id: AtomicU64,
    /// The child (killed on drop — `kill_on_drop` — and by `close`).
    child: tokio::process::Child,
    /// Set by the reader when the process exits (EOF on stdout).
    exited_rx: watch::Receiver<bool>,
}

impl StdioClient {
    /// Connect: spawn the process (the def's `env` ADDITIVE over the
    /// inherited env; the def's `cwd` over the session `cwd`), run the
    /// `initialize` handshake (with `timeout`, raced against `cancel`),
    /// then send `notifications/initialized` (no response expected).
    /// A handshake failure KILLS the process (the child is dropped).
    pub async fn connect(
        def: &StdioDef,
        cwd: &Path,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Self, String> {
        let mut cmd = Command::new(&def.command);
        cmd.args(&def.args);
        // The def's `env` is ADDITIVE (the inherited env + the extras —
        // the suite's spawn behavior; `std::process::Command::envs`
        // ADDs, it does not replace).
        cmd.envs(&def.env);
        cmd.current_dir(def.cwd.as_deref().unwrap_or(cwd));
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::null());
        cmd.kill_on_drop(true);
        let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;

        let (tx, mut rx) = mpsc::channel::<String>(32);
        let stdin = child.stdin.take().ok_or_else(|| "no stdin".to_string())?;
        let stdout = child.stdout.take().ok_or_else(|| "no stdout".to_string())?;
        let (exited_tx, exited_rx) = watch::channel(false);
        let exited_for_reader = exited_tx.clone();

        // The WRITER task: mpsc lines → stdin (a write failure ends it —
        // the process is gone; the reader's EOF fails the pendings).
        let writer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut stdin = stdin;
            while let Some(line) = rx.recv().await {
                // Newline-delimited JSON-RPC: the line + a `\n` terminator.
                // The server reads `lines()` — a line WITHOUT the `\n` is
                // never delivered, so it MUST be appended here.
                if stdin.write_all(line.as_bytes()).await.is_err()
                    || stdin.write_all(b"\n").await.is_err()
                    || stdin.flush().await.is_err()
                {
                    break;
                }
            }
        });

        // The READER task: stdout bytes → complete lines (split on `\n`) →
        // the pending map (by `id`); a notification is ignored (v1: no
        // server push); EOF fails every pending + sets `exited`.
        let pending = Arc::new(Mutex::new(
            BTreeMap::<u64, oneshot::Sender<RpcResponse>>::new(),
        ));
        let pending_for_reader = pending.clone();
        let reader = tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut stdout = stdout;
            let mut buf = [0u8; 4096];
            let mut acc: Vec<u8> = Vec::new();
            loop {
                let n = match stdout.read(&mut buf).await {
                    Ok(0) => break, // EOF: the process is gone.
                    Ok(n) => n,
                    Err(_) => break, // a read error: the process is gone.
                };
                acc.extend_from_slice(&buf[..n]);
                // Process every complete line in the accumulator.
                while let Some(pos) = acc.iter().position(|&b| b == b'\n') {
                    let line: String = String::from_utf8_lossy(&acc[..pos]).into_owned();
                    acc.drain(..=pos);
                    if line.trim().is_empty() {
                        continue;
                    }
                    if let Ok(resp) = parse_response(&line) {
                        let mut map = pending_for_reader.lock().unwrap();
                        // A non-numeric `id` (a malformed response) is
                        // dropped; a known `id` routes to its oneshot.
                        if let Some(id) = resp.id.as_u64() {
                            if let Some(tx) = map.remove(&id) {
                                let _ = tx.send(resp);
                            }
                        }
                    } else if parse_notification(&line).is_ok() {
                        // A notification (e.g. `tools/list_changed`):
                        // ignored (v1).
                    }
                }
            }
            // EOF: fail every pending + mark exited.
            let mut map = pending_for_reader.lock().unwrap();
            let ids: Vec<u64> = map.keys().cloned().collect();
            for id in ids {
                if let Some(tx) = map.remove(&id) {
                    let _ = tx.send(err_resp());
                }
            }
            let _ = exited_for_reader.send(true);
        });

        let mut client = StdioClient {
            tx,
            pending,
            next_id: AtomicU64::new(1),
            child,
            exited_rx,
        };

        // The `initialize` handshake (a request failure kills the child —
        // the `client` is dropped, `kill_on_drop` fires).
        let init = client
            .request_inner(
                "initialize",
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": "archimedes", "version": env!("CARGO_PKG_VERSION") },
                }),
                timeout,
                cancel,
            )
            .await;
        let Ok(_resp) = init else {
            client.kill().await;
            let _ = writer.await;
            let _ = reader.await;
            return Err(init.unwrap_err());
        };
        // `notifications/initialized` (no response expected — best-effort;
        // the writer may already be dead if the process died mid-handshake).
        let _ = client
            .tx
            .send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }).to_string())
            .await;
        let _ = (writer, reader); // the tasks run until the process exits.
        Ok(client)
    }

    /// One JSON-RPC request (build the `RpcRequest`, route the response
    /// by `id`, race the timeout + the `cancel` token): `Ok(result)` /
    /// `Err` (a JSON-RPC `error` response, a timeout, a cancel, or the
    /// process exiting).
    async fn request_inner(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.pending.lock().unwrap();
            if *self.exited_rx.borrow() {
                return Err("the server process exited".to_string());
            }
            map.insert(id, tx);
        }
        let req = RpcRequest {
            id,
            method: method.to_string(),
            params,
        };
        if self.tx.send(req.to_json().to_string()).await.is_err() {
            // The writer task is gone (the process died) — the reader's
            // EOF will fail this pending too; fail fast.
            self.pending.lock().unwrap().remove(&id);
            return Err("the server process exited".to_string());
        }
        let outcome = tokio::select! {
            r = tokio::time::timeout(timeout, rx) => match r {
                Ok(Ok(resp)) => Ok(resp),
                Ok(Err(_)) => Err("the response channel closed (the process exited)".to_string()),
                Err(_) => Err(format!("timed out after {timeout:?}")),
            },
            _ = cancel.cancelled() => Err("cancelled".to_string()),
            _ = self.exited_rx.wait_for(|&exited| exited) => Err("the server process exited".to_string()),
        };
        // Drop the pending (a late response is discarded).
        self.pending.lock().unwrap().remove(&id);
        let resp = outcome?;
        if let Some(err) = &resp.error {
            return Err(err.message.clone());
        }
        Ok(resp.result.unwrap_or(Value::Null))
    }

    /// `tools/list` → the server's tools (name + description +
    /// `inputSchema`).
    pub async fn list_tools(
        &mut self,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Vec<ToolInfo>, String> {
        let result = self
            .request_inner("tools/list", json!({}), timeout, cancel)
            .await?;
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
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<ToolCallResult, String> {
        let result = self
            .request_inner(
                "tools/call",
                json!({ "name": name, "arguments": args }),
                timeout,
                cancel,
            )
            .await?;
        Ok(extract_tool_call_result(&result))
    }

    /// Kill + reap the process (idempotent — `close_session` / a drop).
    pub async fn kill(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for StdioClient {
    fn drop(&mut self) {
        // `kill_on_drop` fires (a best-effort kill — `drop` cannot be
        // async, so the reap is racy; `kill()` is the clean path).
    }
}

/// The reader's EOF failure response (a response with an `error` — the
/// `request_inner` maps it to the `error.message`).
fn err_resp() -> RpcResponse {
    RpcResponse {
        id: Value::Null,
        result: None,
        error: Some(super::rpc::SessionError {
            code: -32000,
            message: "the server process exited".to_string(),
            data: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The `fake_mcp_stdio` test-double binary (built by `cargo test` —
    /// the `CARGO_MANIFEST_DIR`/`target/debug` path, the `fake_pi`
    /// pattern).
    fn fake_mcp_bin() -> PathBuf {
        PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ))
    }

    /// A `StdioDef` pointing at the fake binary.
    fn fake_def() -> StdioDef {
        StdioDef {
            command: fake_mcp_bin().to_str().unwrap().to_string(),
            args: vec![],
            env: Default::default(),
            cwd: None,
        }
    }

    fn cancel() -> CancellationToken {
        CancellationToken::new()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connect_list_and_call_round_trip() {
        let mut client = StdioClient::connect(
            &fake_def(),
            Path::new("/tmp"),
            Duration::from_secs(5),
            &cancel(),
        )
        .await
        .expect("the handshake completes");
        let tools = client
            .list_tools(Duration::from_secs(5), &cancel())
            .await
            .expect("tools/list answers");
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["echo", "hang", "err"]);
        // The `echo` tool's description + input schema survive.
        let echo = tools.iter().find(|t| t.name == "echo").unwrap();
        assert_eq!(echo.description.as_deref(), Some("echo its text argument"));
        assert_eq!(echo.input_schema["properties"]["text"]["type"], "string");

        let r = client
            .call_tool(
                "echo",
                &json!({ "text": "hello" }),
                Duration::from_secs(5),
                &cancel(),
            )
            .await
            .expect("the call answers");
        assert_eq!(r.text, "hello");
        assert!(!r.is_error);
        assert_eq!(r.non_text_count, 0);
        client.kill().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_hanging_tool_call_times_out() {
        let mut client = StdioClient::connect(
            &fake_def(),
            Path::new("/tmp"),
            Duration::from_secs(5),
            &cancel(),
        )
        .await
        .expect("the handshake completes");
        // The `hang` tool NEVER answers — the (short) request timeout
        // fires (the call ERRORS, it does not hang the turn).
        let r = client
            .call_tool("hang", &json!({}), Duration::from_millis(300), &cancel())
            .await;
        assert!(
            matches!(r, Err(ref e) if e.contains("timed out")),
            "the hanging call times out, got {r:?}"
        );
        client.kill().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancel_cancels_the_call() {
        let mut client = StdioClient::connect(
            &fake_def(),
            Path::new("/tmp"),
            Duration::from_secs(5),
            &cancel(),
        )
        .await
        .expect("the handshake completes");
        let c = CancellationToken::new();
        // Start a `hang` call (a long timeout) — then CANCEL the turn:
        // the call ERRORS `cancelled` (raced, not waited out).
        let c_spawner = c.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            c_spawner.cancel();
        });
        let r = client
            .call_tool("hang", &json!({}), Duration::from_secs(30), &c)
            .await;
        assert!(
            matches!(r, Err(ref e) if e == "cancelled"),
            "the cancelled call errors `cancelled`, got {r:?}"
        );
        client.kill().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_json_rpc_error_is_an_error() {
        let mut client = StdioClient::connect(
            &fake_def(),
            Path::new("/tmp"),
            Duration::from_secs(5),
            &cancel(),
        )
        .await
        .expect("the handshake completes");
        // The `err` tool answers with a JSON-RPC `error` — the call
        // ERRORS with the server's message.
        let r = client
            .call_tool("err", &json!({}), Duration::from_secs(5), &cancel())
            .await;
        assert!(
            matches!(r, Err(ref e) if e == "boom"),
            "the JSON-RPC error surfaces the message, got {r:?}"
        );
        client.kill().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dead_process_fails_pending_calls() {
        let mut client = StdioClient::connect(
            &fake_def(),
            Path::new("/tmp"),
            Duration::from_secs(5),
            &cancel(),
        )
        .await
        .expect("the handshake completes");
        // Kill the process — a subsequent call ERRORS (the process is
        // gone; the `exited` watch fires).
        client.kill().await;
        let r = client
            .call_tool(
                "echo",
                &json!({ "text": "x" }),
                Duration::from_secs(5),
                &cancel(),
            )
            .await;
        assert!(
            matches!(r, Err(ref e) if e.contains("exited") || e.contains("channel")),
            "a call on a dead process errors, got {r:?}"
        );
    }
}

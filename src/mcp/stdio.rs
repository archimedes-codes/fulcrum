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
use crate::agent::hostenv::EnvPatch;

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
    /// Connect: spawn the process (the inherited env SCRUBBED of the
    /// AppImage's own paths, then the def's `env` ADDITIVE over that — see
    /// [`build_command`]; the def's `cwd` over the session `cwd`), run the
    /// `initialize` handshake (with `timeout`, raced against `cancel`),
    /// then send `notifications/initialized` (no response expected).
    /// A handshake failure KILLS the process (the child is dropped).
    pub async fn connect(
        def: &StdioDef,
        cwd: &Path,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Self, String> {
        let mut cmd = build_command(def, cwd);
        // The transport's own three decisions, deliberately NOT in the seam:
        // piped stdin / piped stdout / NULL stderr (a server's chatter is not
        // the agent's) and `kill_on_drop` (the child dies with the client) —
        // the env policy is [`build_command`]'s, and the order between the two
        // halves of the env is pinned there.
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

/// Build the `Command` a stdio MCP server is spawned with — everything except
/// the `Stdio` trio and `kill_on_drop(true)`, which stay in
/// [`StdioClient::connect`].
///
/// The seam exists because the assertion that matters here — the scrub landed
/// BEFORE the def's `env` — can only be made on a `Command` that was never
/// spawned. It is called by production `connect`, so it needs no `cfg` and no
/// `allow`.
fn build_command(def: &StdioDef, cwd: &Path) -> Command {
    build_command_with(def, cwd, crate::agent::hostenv::for_child())
}

/// [`build_command`] with the patch as an argument.
///
/// Production passes exactly [`for_child`](crate::agent::hostenv::for_child) —
/// the parameter is there because that cached patch is EMPTY under `cargo test`
/// (its gate needs this exe under `$APPDIR`; under cargo it is
/// `target/debug/deps/<hash>`), so a test driven through `build_command` could
/// not observe the ordering at all and could not fail.
fn build_command_with(def: &StdioDef, cwd: &Path, patch: &EnvPatch) -> Command {
    let mut cmd = Command::new(&def.command);
    cmd.args(&def.args);
    // The inherited env is SCRUBBED of the AppImage's own mount paths first
    // (`crate::agent::hostenv`; evidence:
    // `docs/research/appimage-child-process-env-leak.md`). This is the most
    // damaging site after `bash`: a stdio server is overwhelmingly
    // `npx` / `uvx` / `node` / `python` — the frozen-interpreter shapes that
    // `PYTHONHOME`, `PYTHONPATH` and a `libssl` resolved out of the mount kill
    // outright.
    //
    // Both halves of the env decision, and the ORDER between them is the
    // load-bearing one: the def's `env` is ADDITIVE over the SCRUBBED
    // inheritance — `std::process::Command::envs` ADDs, it does not replace —
    // and it runs AFTER the patch, so a variable the user wrote into
    // `mcpServers.env` still wins over the scrub even where the scrub would
    // `env_remove` that name. Move this `apply` below the `envs` and the
    // user's entry is silently discarded (measured against a real `execve`);
    // `an_mcp_server_env_wins_over_the_scrub` is the test that catches it.
    patch.apply(&mut cmd);
    cmd.envs(&def.env);
    cmd.current_dir(def.cwd.as_deref().unwrap_or(cwd));
    cmd
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

    /// The `cwd` the client spawns the fake server in — the argument the
    /// product fills with the SESSION's cwd, i.e. a directory that exists.
    ///
    /// `temp_dir()`, NOT a literal `/tmp`: a literal has no meaning on
    /// Windows, so `CreateProcess` rejects the `current_dir` before the
    /// child starts and every test here died at `connect` with "spawn
    /// failed: The directory name is invalid. (os error 267)". These tests
    /// are about the JSON-RPC transport, not about any one platform's
    /// temp-dir NAME, so the fixture names the platform's own temp dir and
    /// stays a real test on all three.
    fn session_cwd() -> PathBuf {
        std::env::temp_dir()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connect_list_and_call_round_trip() {
        let mut client = StdioClient::connect(
            &fake_def(),
            &session_cwd(),
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
            &session_cwd(),
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
            &session_cwd(),
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
            &session_cwd(),
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
            &session_cwd(),
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

    // ── the AppImage child-env scrub, at the MCP spawn site ──────────────
    //
    // `crate::agent::hostenv` pins the RULE (what a scrubbed child env looks
    // like) and `tools/exec.rs`'s module pins that call site; this section
    // pins the stdio MCP site, which is the most damaging one after `bash`:
    // stdio servers are overwhelmingly `npx` / `uvx` / `node` / `python` —
    // exactly the frozen-interpreter shapes that `PYTHONHOME`, `PYTHONPATH`
    // and a shadowed `libssl` kill (evidence:
    // `docs/research/appimage-child-process-env-leak.md`).
    //
    // ⚠ A seam-built `Command` is NOT the command that spawns. These tests
    // prove the ENV/ARGV policy of [`build_command_with`]; the `Stdio` trio
    // and `kill_on_drop(true)` stay in `connect()` (this site has no
    // `pre_exec`), and they are deliberately NOT asserted here — no child is
    // spawned, so nothing here proves the end-to-end spawn.
    //
    // Both tests are `#[cfg(target_os = "linux")]`: off Linux `scrub` is
    // DEFINED to return the empty patch, so the same assertions would pass
    // VACUOUSLY on macOS and Windows. Compile-time gating is the honest form
    // of "not applicable here" (the `hostenv.rs` / `sandbox.rs` convention).

    #[cfg(target_os = "linux")]
    use std::ffi::{OsStr, OsString};

    /// The value a `StdioDef` sets `LD_LIBRARY_PATH` to: the thing that must
    /// survive a scrub applied BEFORE `envs(&def.env)` and be destroyed by one
    /// applied AFTER it.
    #[cfg(target_os = "linux")]
    const SENTINEL: &str = "/opt/user-said-this/lib";

    /// The ordering test: a user's MCP `env` entry still wins over the scrub.
    ///
    /// Why the order is load-bearing: the patch holds an `env_remove` for a
    /// variable whose every entry was the bundle's, and an `env_remove` issued
    /// AFTER `envs(&def.env)` DISCARDS the user's entry (measured against a
    /// real `execve` — see the note on
    /// [`EnvPatch::apply`](crate::agent::hostenv::EnvPatch::apply)). A
    /// transport that silently drops the environment a user wrote into their
    /// `mcpServers` config is worse than one that never scrubbed.
    ///
    /// The patch is injected through the seam rather than read from
    /// [`for_child`](crate::agent::hostenv::for_child), which is EMPTY under
    /// `cargo test` — its gate needs this exe under `$APPDIR`, and under cargo
    /// it is `target/debug/deps/<hash>`. So a test built on `for_child()`
    /// asserts nothing; the fabricated host in
    /// [`fixtures`](crate::agent::hostenv::fixtures) is what makes the patch
    /// (and therefore the ordering) observable.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_mcp_server_env_wins_over_the_scrub() {
        use crate::agent::hostenv::fixtures;

        let patch = fixtures::tainted_patch();
        let mut def = fake_def();
        def.env
            .insert("LD_LIBRARY_PATH".to_string(), SENTINEL.to_string());

        let cmd = build_command_with(&def, &session_cwd(), &patch);
        let got = fixtures::configured_env(&cmd);

        assert_eq!(
            got.get(&OsString::from("LD_LIBRARY_PATH")),
            Some(&Some(OsString::from(SENTINEL))),
            "the def's `env` must win over the scrub (a scrub applied after \
             `envs` overwrites it, and its `env_remove` would discard it)"
        );
        // …and the scrub IS applied, for the keys the def does not name.
        // Without this conjunct the assertion above passes on a command that
        // never scrubbed at all (an empty env also has no `LD_LIBRARY_PATH` to
        // clobber).
        assert_eq!(
            got.get(&OsString::from("PYTHONHOME")),
            Some(&None),
            "a tainted variable the def does not name must be scrubbed by the \
             same seam — `None` is `env_remove`, NOT an empty value"
        );
        // The ordering, pinned as one equality: scrub FIRST, then the def's
        // `env`. A seam that swaps the two differs HERE, and the difference is
        // the user's config.
        let mut expected = fixtures::reference_env(&patch);
        expected.insert(
            OsString::from("LD_LIBRARY_PATH"),
            Some(OsString::from(SENTINEL)),
        );
        assert_eq!(
            got, expected,
            "the spawn env must be exactly scrub-then-def"
        );
    }

    /// The inheritance test: with no `env` of its own, the server gets the
    /// inherited env MINUS the mount paths — and the user's own entries are
    /// what is left, not an empty or absent variable.
    ///
    /// Same seam caveat as [`an_mcp_server_env_wins_over_the_scrub`]: this is
    /// the command builder, not the spawn.
    #[cfg(target_os = "linux")]
    #[test]
    fn an_mcp_server_inherits_a_scrubbed_library_path() {
        use crate::agent::hostenv::fixtures;

        let patch = fixtures::tainted_patch();
        // `fake_def()`'s `env` is EMPTY — nothing overrides the scrub.
        let cmd = build_command_with(&fake_def(), &session_cwd(), &patch);
        let got = fixtures::configured_env(&cmd);

        let ld = got
            .get(&OsString::from("LD_LIBRARY_PATH"))
            .and_then(|value| value.as_ref())
            .expect(
                "the user's `/usr/lib` survives, so the variable is re-SET, \
                     not removed — its absence would be a `PATH`-style silent \
                     regression for every `npx`/`uvx` server",
            );
        assert_eq!(
            ld.as_os_str(),
            OsStr::new(fixtures::SCRUBBED_LD_LIBRARY_PATH),
            "the surviving entry must be exactly the user's, in order"
        );
        assert_eq!(
            got.get(&OsString::from("PATH")),
            Some(&Some(OsString::from(fixtures::USER_BIN))),
            "the user's own bin dir survives the mount entry's removal"
        );
        assert_eq!(
            got.get(&OsString::from("PYTHONHOME")),
            Some(&None),
            "`PYTHONHOME` pointed ONLY at the mount, so it is removed — an empty \
             value is not an absent one to CPython"
        );
        for (key, value) in &got {
            if let Some(value) = value {
                assert!(
                    !fixtures::lists_a_mount_path(value),
                    "{key:?} still names a mount path: {value:?}"
                );
            }
        }
        assert_eq!(
            got,
            fixtures::reference_env(&patch),
            "with an empty `def.env` the command's env is exactly the patch"
        );
    }
}

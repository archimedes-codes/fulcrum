//! The MCP server manager (ADR 0018): one per `AgentLoop` (constructed with
//! the space `cwd`); `server → (def, state, client)`; lazy connect on first
//! use; `tools/list` cached per connection; `connect` / `status` / `auth` /
//! `close` actions. A failed connect is never silently dropped, but WHERE it
//! lands differs by transport: an HTTP failure becomes `Error { text }` (or
//! `NeedsAuth`) on the entry, so `status` reports it, while a STDIO failure
//! propagates with `?` instead and leaves the entry in `Connecting` — a
//! documented gap, `docs/features/mcp-known-gaps.md` ("A stdio connect failure
//! never reaches `McpState::Error`").

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::config::load_servers;
use super::http::{HttpClient, TokenProvider};
use super::oauth;
use super::stdio::StdioClient;
use super::types::{AuthSpec, McpState, ServerDef, ToolCallResult, ToolInfo};
use crate::config::load_settings;

/// A connected MCP server (a stdio or HTTP client — the unified interface
/// the manager routes `list_tools` / `call_tool` through).
enum Client {
    Stdio(StdioClient),
    Http(HttpClient),
}

impl Client {
    /// `tools/list` (the `timeout` is the stdio bound; an HTTP client uses
    /// its own `read_timeout` — the `timeout` is ignored).
    async fn list_tools(
        &mut self,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Vec<ToolInfo>, String> {
        match self {
            Client::Stdio(c) => c.list_tools(timeout, cancel).await,
            Client::Http(c) => c.list_tools(cancel).await,
        }
    }

    /// `tools/call` (the `timeout` is the stdio bound; an HTTP client uses
    /// its own `read_timeout` — the `timeout` is ignored).
    async fn call_tool(
        &mut self,
        name: &str,
        args: &Value,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<ToolCallResult, String> {
        match self {
            Client::Stdio(c) => c.call_tool(name, args, timeout, cancel).await,
            Client::Http(c) => c.call_tool(name, args, cancel).await,
        }
    }
}

/// A manager's per-server entry (the def + the connection state + the
/// connected client (if any) + the cached `tools/list`).
struct ServerEntry {
    def: ServerDef,
    state: McpState,
    client: Option<Client>,
    tools: Option<Vec<ToolInfo>>,
}

/// The MCP server manager (one per `AgentLoop`).
pub struct McpManager {
    servers: BTreeMap<String, ServerEntry>,
    project_cwd: PathBuf,
    /// The credentials file (`mcp-auth.json`). Production: `oauth::auth_file_path()`.
    /// A field, not a call, so a test can drive the token paths against a temp
    /// file instead of the developer's stored tokens (issue #68 — the read that
    /// T4 left pointed at the real path).
    auth_file: PathBuf,
}

impl McpManager {
    /// Create a manager (load the servers from the config — Task 1's
    /// `load_servers`: global `~/.pi/agent/mcp.json` + the desktop
    /// `settings.json` `mcpServers` (ADR 0019 — `config_dir: None` =
    /// absent) + project `<cwd>/.pi/mcp.json`).
    pub fn new(home_dir: PathBuf, project_cwd: PathBuf, config_dir: Option<&Path>) -> Self {
        // (ADR 0019) The desktop layer: the `settings.json` `mcpServers`
        // (best-effort — a missing / corrupt file is the `load_settings`
        // defaults = an empty map). Merged at global precedence (project
        // > desktop > global).
        let desktop = config_dir.map(|d| load_settings(d).mcp_servers);
        let servers = load_servers(&home_dir, &project_cwd, desktop.as_ref())
            .into_iter()
            .map(|(name, def)| {
                (
                    name,
                    ServerEntry {
                        def,
                        state: McpState::Disconnected,
                        client: None,
                        tools: None,
                    },
                )
            })
            .collect();
        Self {
            servers,
            project_cwd,
            auth_file: oauth::auth_file_path(),
        }
    }

    /// Point the credentials read/write at a specific file (the test seam —
    /// every token test injects a temp path so it never touches the developer's
    /// `mcp-auth.json`).
    #[cfg(test)]
    pub(crate) fn with_auth_file(mut self, auth_file: PathBuf) -> Self {
        self.auth_file = auth_file;
        self
    }

    /// A server's current state (`None` = no such server). For callers that must
    /// report WHY a server was unavailable, not just that it was.
    pub fn state_of(&self, name: &str) -> Option<&McpState> {
        self.servers.get(name).map(|entry| &entry.state)
    }

    /// The server names (for the tests / the `status` action).
    pub fn server_names(&self) -> Vec<String> {
        self.servers.keys().cloned().collect()
    }

    /// The `mcp({})` / `action: "status"` output: `<name>: <state>[
    /// (<error>)]` per server, or `No MCP servers configured.` when empty.
    pub fn status_lines(&self) -> Vec<String> {
        if self.servers.is_empty() {
            return vec!["No MCP servers configured.".to_string()];
        }
        self.servers
            .iter()
            .map(|(name, entry)| match &entry.state {
                McpState::Error { text } => format!("{name}: error ({text})"),
                other => format!("{name}: {other:?}"),
            })
            .collect()
    }

    /// Lazy connect (the first `list_tools` / `call_tool` connects; a
    /// `NeedsAuth` / `Error` state is NOT auto-retried — an explicit
    /// `connect` / `auth` is required).
    async fn ensure_connected(
        &mut self,
        name: &str,
        cancel: &CancellationToken,
        timeout: Duration,
    ) -> Result<(), String> {
        let entry = self
            .servers
            .get_mut(name)
            .ok_or_else(|| format!("unknown MCP server: {name}"))?;
        if entry.client.is_some() {
            return Ok(()); // already connected.
        }
        // A `NeedsAuth` / `Error` state is NOT auto-retried.
        if matches!(entry.state, McpState::NeedsAuth | McpState::Error { .. }) {
            return Err(format!(
                "the server is in a {state:?} state (use the `connect` / `auth` action)",
                state = entry.state
            ));
        }
        entry.state = McpState::Connecting;
        let client = match &entry.def {
            ServerDef::Stdio(def) => {
                Client::Stdio(StdioClient::connect(def, &self.project_cwd, timeout, cancel).await?)
            }
            ServerDef::Http(def) => {
                // An OAuth server: a `TokenProvider` (the stored token, if
                // any; `None` → a 401 → `NeedsAuth`).
                let get_token = if matches!(def.auth, AuthSpec::OAuth { .. }) {
                    usable_token_provider(name, &self.auth_file).await
                } else {
                    None
                };
                match HttpClient::connect(def, timeout, cancel, get_token).await {
                    Ok(c) => Client::Http(c),
                    Err(e) if e == "needs-auth" => {
                        entry.state = McpState::NeedsAuth;
                        return Err("the server requires authentication (use the `auth` action)"
                            .to_string());
                    }
                    Err(e) => {
                        entry.state = McpState::Error { text: e.clone() };
                        return Err(e);
                    }
                }
            }
        };
        entry.client = Some(client);
        entry.state = McpState::Connected;
        Ok(())
    }

    /// `tools/list` (lazy connect; the result is cached per connection).
    pub async fn list_tools(
        &mut self,
        name: &str,
        cancel: &CancellationToken,
        timeout: Duration,
    ) -> Result<Vec<ToolInfo>, String> {
        self.ensure_connected(name, cancel, timeout).await?;
        let entry = self.servers.get_mut(name).expect("the server exists");
        if let Some(tools) = &entry.tools {
            return Ok(tools.clone());
        }
        let Some(client) = entry.client.as_mut() else {
            return Err("the server is not connected".to_string());
        };
        let tools = client.list_tools(timeout, cancel).await?;
        entry.tools = Some(tools.clone());
        Ok(tools)
    }

    /// `tools/call` (lazy connect).
    pub async fn call_tool(
        &mut self,
        name: &str,
        tool: &str,
        args: &Value,
        cancel: &CancellationToken,
        timeout: Duration,
    ) -> Result<ToolCallResult, String> {
        self.ensure_connected(name, cancel, timeout).await?;
        let entry = self.servers.get_mut(name).expect("the server exists");
        let Some(client) = entry.client.as_mut() else {
            return Err("the server is not connected".to_string());
        };
        client.call_tool(tool, args, timeout, cancel).await
    }

    /// Eagerly connect a server (the `connect` action). An OAuth server in
    /// `NeedsAuth` is NOT authenticated here (the `auth` action is).
    pub async fn connect(
        &mut self,
        name: &str,
        cancel: &CancellationToken,
        timeout: Duration,
    ) -> Result<String, String> {
        self.ensure_connected(name, cancel, timeout).await?;
        Ok(format!("{name}: connected"))
    }

    /// Run the interactive OAuth flow for a server (the `auth` action).
    /// Stores the resulting token in `mcp-auth.json` (so a subsequent
    /// `connect` / `call` uses it) and drops the connection so the NEXT use
    /// reconnects with the fresh token — no reconnect happens here.
    pub async fn authenticate(
        &mut self,
        name: &str,
        cancel: &CancellationToken,
    ) -> Result<String, String> {
        // Clone FIRST: `self.authenticate_with(…, &self.auth_file)` would borrow
        // `*self` mutably (the receiver) and immutably (the argument) at once.
        let auth_file = self.auth_file.clone();
        self.authenticate_with(name, cancel, &auth_file).await
    }

    /// The body of `authenticate` with the credentials path injected (the test
    /// seam — a test writes a temp `mcp-auth.json`, never the developer's).
    async fn authenticate_with(
        &mut self,
        name: &str,
        cancel: &CancellationToken,
        auth_file: &Path,
    ) -> Result<String, String> {
        // Check it's an OAuth server + run the flow (the `def` borrow ends
        // at the block's end, so we can mutate `entry` after).
        {
            let entry = self
                .servers
                .get(name)
                .ok_or_else(|| format!("unknown MCP server: {name}"))?;
            let ServerDef::Http(def) = &entry.def else {
                return Err(format!("{name} is not an HTTP server (no OAuth)"));
            };
            if !matches!(def.auth, AuthSpec::OAuth { .. }) {
                return Err(format!("{name} is not an OAuth server"));
            }
            // The auth server URL (the config's `authorizationServerUrl`).
            let auth_server_url = match &def.auth {
                AuthSpec::OAuth {
                    authorization_server_url,
                    ..
                } => authorization_server_url.clone(),
                _ => None,
            };
            // The production `open_browser` (the platform default browser,
            // fire-and-forget; a failure is non-fatal — the URL is in the
            // result).
            let open_browser: Box<dyn Fn(&str) + Send + Sync> = Box::new(production_open_browser);
            let stored =
                oauth::authenticate(def, auth_server_url.as_deref(), cancel, &open_browser)
                    .await
                    .map_err(|e| format!("{name}: {e}"))?;
            // Store the credentials (so a subsequent `connect` / `call` uses
            // the token). Merged into the file as it is on disk NOW — the flow
            // above took a browser round trip, and anything another writer
            // saved meanwhile (another Worker's refresh, the Settings page)
            // must survive.
            oauth::store_credentials(auth_file, name, stored)?;
        }
        // Authenticated: drop the connection AND the state so the next
        // `ensure_connected` reconnects with the stored token (a `NeedsAuth`
        // state is never auto-retried, so leaving it here would keep the
        // server dead for the rest of the session), and drop the cached
        // `tools/list` (the tool set can change with the identity — and
        // `ensure_connected` does not clear it).
        if let Some(entry) = self.servers.get_mut(name) {
            entry.client = None;
            entry.state = McpState::Disconnected;
            entry.tools = None;
        }
        Ok(format!("{name}: authenticated"))
    }

    /// Session teardown: kill the stdio children (dropping a `StdioClient`
    /// kills the child via `kill_on_drop`) + clear the state.
    pub fn close_all(&mut self) {
        for entry in self.servers.values_mut() {
            entry.client = None;
            entry.state = McpState::Disconnected;
            entry.tools = None;
        }
    }
}

/// The stored token for a server, **refreshed first when it is expired**
/// (issue #68). A refresh is async and needs an HTTP client, and the
/// `TokenProvider` seam is a sync closure called per request — so the refresh
/// happens HERE, before the closure is built, and the closure captures the
/// token that survived.
///
/// `None` = nothing usable (no stored entry, or an expired token whose refresh
/// failed or is forbidden by the ADR 0015 public-client guard). The request
/// then goes out unauthenticated and the server's 401 marks the entry
/// `NeedsAuth` — the honest fallback, and the same path a missing token already
/// takes.
///
/// A FREE function (not a method): `ensure_connected` calls it while `entry`
/// (a `self.servers` borrow) and `def` (an `entry.def` borrow) are live, so a
/// `&self` receiver would be a double borrow.
///
/// CANCELLATION: this future sits inside the `mcp` tool arm's `select!` against
/// the TURN token (`harness/loop/tools.rs`), so a Stop drops it wherever it
/// happens to be — including between "the auth server consumed our refresh
/// token" and "we persisted the rotated one". The refresh is therefore run on a
/// DETACHED task that owns its persist (see [`refresh_and_persist`]): dropping
/// this future cannot drop that task, so a completed rotation is always written.
async fn usable_token_provider(server_name: &str, auth_file: &Path) -> Option<TokenProvider> {
    let mut map = oauth::load_credentials(auth_file);
    let stored = map.remove(server_name)?;
    let token = if stored.is_expired(oauth::now_epoch_secs()) {
        eprintln!("mcp: {server_name}'s stored token is expired — trying the refresh_token grant");
        // The ADR 0015 guard lives INSIDE `oauth::refresh` (a client with no
        // secret returns `None` before it touches the network) — this call site
        // must not grow its own copy of that decision.
        refresh_and_persist(stored, server_name.to_string(), auth_file.to_path_buf()).await?
    } else {
        stored.token
    };
    Some(Box::new(move || Some(token.clone())))
}

/// Run the `refresh_token` grant for `stored` and PERSIST the result, on a task
/// the caller cannot cancel — the invariant is that **a completed rotation is
/// never dropped on the floor**.
///
/// Why it has to be a task and not just an `.await`: the caller is cancellable
/// by construction (the `mcp` tool arm races the whole flow against the turn
/// token), so any `await` inside this function is a place a Stop can strand it.
/// A `tokio::spawn`ed task is detached — dropping our `JoinHandle` does not
/// abort it — so the refresh runs to completion and writes its own result even
/// when the tool call that asked for it was cancelled. The caller still awaits
/// the join, because on the normal path it needs the new token for this
/// connection; the detached task is what makes that await safe to abandon.
///
/// The alternative the review offered — "await the refresh uncancellably" — is
/// not available from here: the `select!` is the caller's, and it drops this
/// future on a Stop no matter what this function does. Detaching achieves the
/// same guarantee (the rotation survives) with BETTER stop-latency (a Stop
/// returns immediately; the write lands a moment later). The margin that decides
/// how often this code runs at all is documented on
/// [`oauth::TOKEN_EXPIRY_MARGIN_SECS`], including why the grant is not raced
/// against a cancel token and why "return the stored token on cancel" is not an
/// option.
///
/// Honest limits of the detach:
/// - it needs a runtime that outlives the call (true in production: the Worker's
///   `new_multi_thread` runtime, `run_worker`; the tests use the `multi_thread`
///   flavour), and
/// - the caller no longer sees the token if it was dropped — which is fine, the
///   tool call it served is gone; the NEXT connect reads the persisted token.
///
/// And one the detach makes slightly more likely: the write can land AFTER a
/// newer write for the SAME server (an `auth` that finished while this grant was
/// still in flight). The merge keeps every OTHER server's entry; this server's
/// entry is last-writer-wins, so the worst case is one server holding the older
/// of two valid tokens — which costs a refresh (or, if the auth server revoked
/// it, one `auth`), never a torn file and never another server's credentials.
///
/// And because the task is detached, its FAILURE is invisible by construction:
/// nobody else holds the handle, so `refresh_and_persist` is the one place that
/// can notice the task died, and it reports every way that can happen (see
/// [`refresh_failure_note`]) — a silent `None` here is exactly the "lost a
/// rotation without anyone knowing" the detach was meant to end.
async fn refresh_and_persist(
    stored: oauth::StoredAuth,
    server_name: String,
    auth_file: PathBuf,
) -> Option<String> {
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    else {
        // Not a lost rotation (nothing was spent yet), but it is why this
        // session will not refresh, and that is worth one line.
        eprintln!("mcp: {server_name}: building the refresh client failed — the stored token stays as it is");
        return None;
    };
    // `server_name` moves into the task, so the report gets its own copy.
    let reported_name = server_name.clone();
    let task = tokio::spawn(async move {
        let refreshed = oauth::refresh(&stored, &client).await?;
        let token = refreshed.token.clone();
        // Merge into the LIVE file, not a snapshot taken before this round trip
        // (a concurrent `auth` may have replaced this very entry in the
        // meantime). A failed WRITE must not throw away a token we just earned:
        // use it for this connection and let the next attempt retry the write.
        if let Err(e) = oauth::store_credentials(&auth_file, &server_name, refreshed) {
            eprintln!("mcp: persisting the refreshed token failed: {e}");
        }
        Some(token)
    });
    report_refresh_join(&reported_name, task.await)
}

/// Await the detached refresh task and return its token, printing the reason
/// when there is none. Split from [`refresh_and_persist`] so the three ways an
/// awaited detached task can end are testable without a network round trip.
fn report_refresh_join(
    server_name: &str,
    outcome: Result<Option<String>, tokio::task::JoinError>,
) -> Option<String> {
    if let Some(note) = refresh_failure_note(server_name, &outcome) {
        eprintln!("{note}");
    }
    // The value contract with the caller is unchanged: a token, or `None`.
    // `unwrap_or_default` is that `None` — including for `Err`, where the task
    // died (panicked, or the runtime went away before it finished) and there is
    // nothing to hand back. The REPORT of why is the whole point of this
    // function and is already printed above; `refresh_failure_note` names each
    // arm separately, which is what `ok().flatten()` never did.
    outcome.unwrap_or_default()
}

/// The line explaining a detached refresh that produced no token, or `None`
/// when it produced one (a success is silent).
///
/// Every arm is named separately on purpose. `task.await.ok().flatten()` — the
/// shape this replaced — mapped BOTH the panic and the plain failed refresh to
/// `None` and printed nothing, so a panic in the detached task (the one place
/// the code runs a third party's JSON through `parse_token_response`) was
/// indistinguishable from "the auth server said no", from a log that said
/// nothing at all.
fn refresh_failure_note(
    server_name: &str,
    outcome: &Result<Option<String>, tokio::task::JoinError>,
) -> Option<String> {
    match outcome {
        Ok(Some(_)) => None,
        Ok(None) => Some(format!(
            "mcp: {server_name}'s refresh returned no token (the ADR 0015 public-client \
             guard, or a failed grant) — connecting unauthenticated, so a 401 will mark the \
             server as needing auth"
        )),
        Err(e) if e.is_panic() => Some(format!(
            "mcp: {server_name}'s detached refresh task DIED (a panic — the rotation it may \
             already have earned was not persisted); the stored token is unchanged and the \
             next connect retries"
        )),
        Err(e) => Some(format!(
            "mcp: {server_name}'s detached refresh task never completed ({e}) — the runtime \
             went away before it finished, so nothing was persisted"
        )),
    }
}

/// A one-shot test of a server definition (the Settings page's Test
/// action, ADR 0019): build a FRESH client (bypassing the manager's
/// entries — the manager is per-session, the Settings page is not),
/// `tools/list` (bounded by `timeout`), drop the client (a stdio child is
/// `kill_on_drop`). `Ok` = the tool count; `Err` = the error text (a
/// `needs-auth` / a network failure) surfaced verbatim.
pub async fn test_server(
    server_name: Option<&str>,
    def: &ServerDef,
    project_cwd: &Path,
    timeout: Duration,
) -> Result<usize, String> {
    let cancel = CancellationToken::new();
    let tools = match def {
        ServerDef::Stdio(def) => {
            let mut client = StdioClient::connect(def, project_cwd, timeout, &cancel).await?;
            let tools = client.list_tools(timeout, &cancel).await?;
            drop(client); // kill the child.
            tools
        }
        ServerDef::Http(def) => {
            // An OAuth server: the stored token for this server, if any
            // (refreshed first when expired — the same seam the manager uses;
            // `test_server` is a free function with no manager, so the real
            // credentials path is correct here).
            let get_token = match (server_name, &def.auth) {
                (Some(n), AuthSpec::OAuth { .. }) => {
                    usable_token_provider(n, &oauth::auth_file_path()).await
                }
                _ => None,
            };
            let mut client = HttpClient::connect(def, timeout, &cancel, get_token).await?;
            let tools = client.list_tools(&cancel).await?;
            drop(client);
            tools
        }
    };
    Ok(tools.len())
}

/// Run the interactive OAuth flow for a server from outside an agent session
/// (e.g. from the Settings UI). Stores the resulting token in `mcp-auth.json`.
pub async fn authenticate_server(server_name: &str, def: &ServerDef) -> Result<String, String> {
    let ServerDef::Http(http_def) = def else {
        return Err(format!("{server_name} is not an HTTP server (no OAuth)"));
    };
    if !matches!(http_def.auth, AuthSpec::OAuth { .. }) {
        return Err(format!("{server_name} is not an OAuth server"));
    }
    let auth_server_url = match &http_def.auth {
        AuthSpec::OAuth {
            authorization_server_url,
            ..
        } => authorization_server_url.clone(),
        _ => None,
    };
    let cancel = CancellationToken::new();
    let open_browser: Box<dyn Fn(&str) + Send + Sync> = Box::new(production_open_browser);
    let stored = oauth::authenticate(http_def, auth_server_url.as_deref(), &cancel, &open_browser)
        .await
        .map_err(|e| format!("{server_name}: {e}"))?;
    // Merged into the live file (see `oauth::store_credentials`) — the flow
    // above just spent a browser round trip, and a Worker's token refresh may
    // have written other entries in that window.
    oauth::store_credentials(&oauth::auth_file_path(), server_name, stored)?;
    Ok(format!("{server_name}: authenticated"))
}

/// The production `open_browser` (the platform default browser, fire-and-forget;
/// a failure is non-fatal — the URL is in the result). `crate::open` is the ONE
/// External-open policy, so the per-OS `open`/`cmd /c start`/`xdg-open` block
/// that used to live here is gone: `tauri-plugin-opener` is the only place in
/// this repo that knows what each platform's opener is.
fn production_open_browser(url: &str) {
    let _ = crate::open::open(url, None);
}

#[cfg(test)]
mod tests {
    use super::super::types::{HttpDef, StdioDef};
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A temp `home_dir` + `project_cwd` with a `mcp.json` (a helper for the
    /// tests). Writes to the paths `load_servers` reads: `<home>/.pi/agent/
    /// mcp.json` (global) + `<project_cwd>/.pi/mcp.json` (project).
    fn manager_with(
        home_json: &str,
        project_json: Option<&str>,
    ) -> (McpManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a temp dir");
        let home_dir = dir.path().to_path_buf();
        let project_cwd = dir.path().to_path_buf();
        let global_path = home_dir.join(".pi/agent/mcp.json");
        std::fs::create_dir_all(global_path.parent().expect("a parent dir"))
            .expect("creates the dir");
        std::fs::write(&global_path, home_json).expect("writes the home mcp.json");
        if let Some(p) = project_json {
            let project_path = project_cwd.join(".pi/mcp.json");
            std::fs::create_dir_all(project_path.parent().expect("a parent dir"))
                .expect("creates the dir");
            std::fs::write(&project_path, p).expect("writes the project mcp.json");
        }
        (McpManager::new(home_dir, project_cwd, None), dir)
    }

    /// A `mcp.json` with a single stdio server (the fake binary).
    fn stdio_server_json(bin: &std::path::Path) -> String {
        serde_json::json!({
            "mcpServers": { "a": { "command": bin.display().to_string() } }
        })
        .to_string()
    }

    #[test]
    fn status_no_servers() {
        let (manager, _dir) = manager_with("", None);
        assert_eq!(manager.status_lines(), vec!["No MCP servers configured."]);
    }

    #[test]
    fn status_lists_the_servers_disconnected() {
        let (manager, _dir) = manager_with(
            r#"{ "mcpServers": { "a": { "url": "http://x" }, "b": { "command": "echo" } } }"#,
            None,
        );
        assert_eq!(
            manager.status_lines(),
            vec!["a: Disconnected", "b: Disconnected"]
        );
    }

    #[test]
    fn status_shows_the_error_state() {
        let (mut manager, _dir) = manager_with(
            r#"{ "mcpServers": { "a": { "url": "http://127.0.0.1:1/mcp" } } }"#,
            None,
        );
        // A connect to a dead port → an `Error` state.
        let cancel = CancellationToken::new();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let result = rt
            .block_on(manager.connect("a", &cancel, Duration::from_secs(2)))
            .expect_err("a dead port is an error");
        assert!(!result.is_empty(), "the error is non-empty: {result}");
        // The status is `error (...)`.
        let line = manager.status_lines()[0].clone();
        assert!(line.starts_with("a: error ("), "an error state: {line}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lazy_connect_then_reuse() {
        // A stdio server (the fake binary) — the first `call_tool` connects,
        // the second reuses (no reconnect).
        let bin = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ));
        let (mut manager, _dir) = manager_with(&stdio_server_json(bin), None);
        assert_eq!(manager.server_names(), vec!["a"]);
        // The first `call_tool` connects (lazy).
        let cancel = CancellationToken::new();
        let r = manager
            .call_tool(
                "a",
                "echo",
                &serde_json::json!({ "text": "hi" }),
                &cancel,
                Duration::from_secs(5),
            )
            .await
            .expect("the first call connects + calls");
        assert_eq!(r.text, "hi");
        // The state is now `Connected`.
        assert_eq!(manager.status_lines(), vec!["a: Connected"]);
        // The second `call_tool` reuses (no reconnect) — it still works.
        let r = manager
            .call_tool(
                "a",
                "echo",
                &serde_json::json!({ "text": "again" }),
                &cancel,
                Duration::from_secs(5),
            )
            .await
            .expect("the second call reuses the connection");
        assert_eq!(r.text, "again");
        manager.close_all();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_server_a_stdio_server_reports_the_tool_count() {
        // (ADR 0019) The Settings page's Test action: a one-shot connect +
        // `tools/list` (the fake binary — three canned tools).
        let bin = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ));
        let def = ServerDef::Stdio(StdioDef {
            command: bin.display().to_string(),
            args: vec![],
            env: std::collections::BTreeMap::new(),
            cwd: None,
        });
        let count = test_server(
            None,
            &def,
            std::path::Path::new("."),
            Duration::from_secs(5),
        )
        .await
        .expect("the fake server answers");
        assert_eq!(count, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_server_a_dead_port_is_an_error() {
        let def = ServerDef::Http(HttpDef {
            url: "http://127.0.0.1:1/mcp".to_string(),
            headers: std::collections::BTreeMap::new(),
            auth: AuthSpec::None,
        });
        let result = test_server(
            None,
            &def,
            std::path::Path::new("."),
            Duration::from_secs(2),
        )
        .await;
        assert!(result.is_err(), "a dead port is an error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_all_disconnects() {
        let bin = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/target/debug/fake_mcp_stdio"
        ));
        let (mut manager, _dir) = manager_with(&stdio_server_json(bin), None);
        let cancel = CancellationToken::new();
        manager
            .connect("a", &cancel, Duration::from_secs(5))
            .await
            .expect("connects");
        assert_eq!(manager.status_lines(), vec!["a: Connected"]);
        manager.close_all();
        // `close_all` → `Disconnected` (the client is dropped — the stdio
        // child is killed via `kill_on_drop`).
        assert_eq!(manager.status_lines(), vec!["a: Disconnected"]);
    }

    /// A raw-HTTP server that answers every request with a `401` (an OAuth
    /// server that requires a token — `None` → a 401 → `NeedsAuth`).
    async fn start_401_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    // Drain the request (headers + a bit of the body — a 401
                    // doesn't care about the body).
                    let mut buf = [0u8; 8192];
                    let _ = stream.read(&mut buf).await;
                    let response =
                        "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}";
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}/mcp"), handle)
    }

    /// A minimal OAuth auth server for the `client_credentials` grant: the
    /// well-known metadata (pointing at its own `/token`) + a token endpoint.
    /// `client_credentials` is the one grant that needs NO browser and NO local
    /// callback, so this is all `oauth::authenticate` talks to.
    async fn start_auth_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let base = format!("http://{addr}");
        let base_inner = base.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let base = base_inner.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = stream.read(&mut buf).await;
                    let request = String::from_utf8_lossy(&buf).to_string();
                    let path = request
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    // Route by the request path (the same shape as the
                    // `oauth.rs` mock).
                    let body = if path.starts_with("/.well-known") {
                        serde_json::json!({
                            "authorization_endpoint": format!("{base}/authorize"),
                            "token_endpoint": format!("{base}/token"),
                        })
                        .to_string()
                    } else if path.starts_with("/token") {
                        serde_json::json!({
                            "access_token": "mock-access-token",
                            "expires_in": 3600,
                        })
                        .to_string()
                    } else {
                        "{}".to_string()
                    };
                    let status = if path.starts_with("/.well-known") || path.starts_with("/token") {
                        "200 OK"
                    } else {
                        "404 Not Found"
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (base, handle)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn authenticating_clears_the_needs_auth_state() {
        // Issue #46: after the `auth` action reports `authenticated`, the
        // server must be reconnectable — a `NeedsAuth` state is never
        // auto-retried, so leaving it set keeps the server dead for the rest
        // of the session.
        //
        // Honest caveat: this test injects the credentials FILE path (via
        // `authenticate_with`, so it never writes the developer's real
        // `mcp-auth.json`). The READ side is injected too, since T7:
        // `ensure_connected` goes through `usable_token_provider(name,
        // &self.auth_file)`, so a manager built with `with_auth_file` would
        // read the same temp file. This test does NOT do that — it leaves the
        // manager on the real `oauth::auth_file_path()` and only asserts on what
        // `authenticate_with` WROTE, so step 6 is exact and step 7 asserts only
        // that the guard stopped firing. What this shape still cannot see: the
        // token the reconnect actually SENDS. `HttpClient::send_request` maps a
        // 401 on an OAuth server to `needs-auth` whether or not an
        // `Authorization` header went out, so the branch and its message are the
        // same either way — proving "the token from the injected file reached the
        // wire" needs the `with_auth_file` seam on this manager plus a
        // header-recording server (as `an_expired_token_is_refreshed…` does).
        let (mcp_url, mcp_server) = start_401_server().await;
        let (auth_base, auth_server) = start_auth_server().await;
        let (mut manager, _dir) = manager_with(
            &serde_json::json!({
                "mcpServers": { "a": {
                    "url": mcp_url,
                    "auth": {
                        "grantType": "client_credentials",
                        "clientId": "test-cid",
                        "authorizationServerUrl": auth_base,
                    },
                } }
            })
            .to_string(),
            None,
        );
        let cancel = CancellationToken::new();

        // Drive the server into the stuck state (a 401 → `NeedsAuth`).
        let r = manager
            .call_tool(
                "a",
                "echo",
                &serde_json::json!({}),
                &cancel,
                Duration::from_secs(2),
            )
            .await;
        assert!(r.is_err(), "a 401 → an error: {r:?}");
        assert_eq!(manager.status_lines(), vec!["a: NeedsAuth"]);

        // Authenticate (the injected credentials path — never the real one).
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        let result = manager
            .authenticate_with("a", &cancel, &auth_file)
            .await
            .expect("the client_credentials flow completes");
        assert_eq!(result, "a: authenticated");

        // The stuck state is gone: the entry is reconnectable, not `NeedsAuth`.
        assert_eq!(
            manager.status_lines(),
            vec!["a: Disconnected"],
            "auth leaves the server reconnectable, not stuck in NeedsAuth"
        );

        // The fresh token landed in the injected file.
        assert_eq!(
            oauth::load_credentials(&auth_file)
                .get("a")
                .map(|s| s.token.as_str()),
            Some("mock-access-token")
        );

        // The guard no longer fires: the 401 fake still answers `401` to
        // everything, so this attempt fails — but with a REAL connect attempt,
        // not the "not auto-retried" rejection.
        let r = manager
            .call_tool(
                "a",
                "echo",
                &serde_json::json!({}),
                &cancel,
                Duration::from_secs(2),
            )
            .await;
        let Err(e) = r else {
            panic!("the 401 server still rejects: {r:?}");
        };
        assert!(
            !e.contains("the server is in a"),
            "the not-auto-retried guard must not fire after auth: {e}"
        );
        assert!(
            e.contains("requires authentication"),
            "a real connect attempt was made: {e}"
        );
        assert_eq!(
            manager.status_lines(),
            vec!["a: NeedsAuth"],
            "a rejected reconnect re-marks NeedsAuth honestly"
        );

        mcp_server.abort();
        auth_server.abort();
    }

    /// A minimal auth server for the `refresh_token` grant: the well-known
    /// metadata (its own `/token`) + a token endpoint that answers per
    /// `succeeds` and RECORDS every form field it received (so a test can prove
    /// the `refresh_token` grant was used, and — with the request counter — that
    /// no request was made at all).
    ///
    /// The counter counts EVERY request to the auth server: a successful
    /// `oauth::refresh` makes TWO (the `discover` well-known GET, then the
    /// `/token` POST) — so it is only ever asserted at `0` (proof that nothing
    /// touched the network).
    async fn start_refresh_server(
        succeeds: bool,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        std::sync::Arc<std::sync::atomic::AtomicU32>,
        tokio::task::JoinHandle<()>,
    ) {
        start_refresh_server_with(succeeds, Duration::ZERO).await
    }

    /// `start_refresh_server` with a `delay` on the `/token` answer (the
    /// cancelled-refresh test needs the grant to still be in flight when the
    /// caller is dropped).
    async fn start_refresh_server_with(
        succeeds: bool,
        delay: Duration,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        std::sync::Arc<std::sync::atomic::AtomicU32>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let base = format!("http://{addr}");
        let base_inner = base.clone();
        let recorded: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded_inner = recorded.clone();
        let counter: std::sync::Arc<std::sync::atomic::AtomicU32> =
            std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_inner = counter.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let base = base_inner.clone();
                let recorded = recorded_inner.clone();
                let counter = counter_inner.clone();
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt;
                    // Read the whole request (headers + body — the form body
                    // follows the header block, so a single `read` is not
                    // enough).
                    let mut data = Vec::new();
                    let mut buf = [0u8; 8192];
                    loop {
                        let Ok(n) = stream.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&buf[..n]);
                        if data.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let header_end = match data.windows(4).position(|w| w == b"\r\n\r\n") {
                        Some(p) => p,
                        None => return,
                    };
                    let headers = String::from_utf8_lossy(&data[..header_end + 4]).to_string();
                    let mut body = data[header_end + 4..].to_vec();
                    let content_length = headers
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    while body.len() < content_length {
                        let Ok(n) = stream.read(&mut buf).await else {
                            break;
                        };
                        if n == 0 {
                            break;
                        }
                        body.extend_from_slice(&buf[..n]);
                    }
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let path = headers
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    let (status, body) = if path.starts_with("/.well-known") {
                        (
                            "200 OK",
                            serde_json::json!({
                                "authorization_endpoint": format!("{base}/authorize"),
                                "token_endpoint": format!("{base}/token"),
                            })
                            .to_string(),
                        )
                    } else if path.starts_with("/token") {
                        // Record every decoded `k=v` form field.
                        let body_str = String::from_utf8_lossy(&body).to_string();
                        for pair in body_str.split('&') {
                            let mut kv = pair.splitn(2, '=');
                            let k = kv.next().unwrap_or_default();
                            let v = kv.next().unwrap_or_default();
                            recorded
                                .lock()
                                .unwrap()
                                .push(format!("{}={}", k, url_decode(v)));
                        }
                        if succeeds {
                            if !delay.is_zero() {
                                // Hold the grant open (the auth server HAS
                                // consumed the refresh token by now — the
                                // rotated one is in this pending response).
                                tokio::time::sleep(delay).await;
                            }
                            (
                                "200 OK",
                                serde_json::json!({
                                    "access_token": "refreshed-access-token",
                                    "refresh_token": "rotated-refresh",
                                    "expires_in": 3600,
                                })
                                .to_string(),
                            )
                        } else {
                            ("400 Bad Request", "{}".to_string())
                        }
                    } else {
                        ("404 Not Found", "{}".to_string())
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (base, recorded, counter, handle)
    }

    /// Percent-decode a form field (the `oauth.rs` mock's decoder).
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

    /// An MCP-over-HTTP server that accepts a request only when its
    /// `Authorization` header carries `accepted` — everything else gets a `401`
    /// (which `HttpClient::send_request` maps to `needs-auth` because the def is
    /// OAuth). This is the "the stale token is dead, the fresh one works"
    /// scenario. Records every `Authorization` value it saw.
    async fn start_token_gated_server(
        accepted: &str,
    ) -> (
        String,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let accepted = accepted.to_string();
        let seen: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_inner = seen.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let accepted = accepted.clone();
                let seen = seen_inner.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = stream.read(&mut buf).await;
                    let request = String::from_utf8_lossy(&buf).to_string();
                    let authorization = request
                        .lines()
                        .find(|l| l.to_lowercase().starts_with("authorization:"))
                        .map(|l| l.trim().to_string())
                        .unwrap_or_default();
                    seen.lock().unwrap().push(authorization.clone());
                    // Accept ONLY a header carrying the expected token. (Assert
                    // with `contains` in the tests: the client sends an OAuth
                    // token with no `Bearer ` prefix today — out of scope here.)
                    let (status, body) = if authorization.contains(accepted.as_str()) {
                        (
                            "200 OK",
                            serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "result": {
                                    "protocolVersion": "2025-06-18",
                                    "capabilities": {},
                                    "serverInfo": { "name": "fake", "version": "0" },
                                },
                            })
                            .to_string(),
                        )
                    } else {
                        ("401 Unauthorized", "{}".to_string())
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}/mcp"), seen, handle)
    }

    /// A stored-credentials file holding one entry (the refresh tests' setup).
    fn write_credentials(auth_file: &std::path::Path, entry: oauth::StoredAuth) {
        let mut map: std::collections::BTreeMap<String, oauth::StoredAuth> =
            std::collections::BTreeMap::new();
        map.insert("a".to_string(), entry);
        oauth::save_credentials(auth_file, &map).expect("writes the credentials");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_expired_token_is_refreshed_and_persisted_instead_of_needs_auth() {
        // Issue #68: an `mcp-auth.json` entry whose `expires_at` has passed is
        // refreshed + re-persisted on the next connect, instead of sending the
        // dead token and dropping the server into `NeedsAuth`.
        let (mcp_url, mcp_seen, mcp_server) =
            start_token_gated_server("refreshed-access-token").await;
        let (auth_base, auth_fields, _auth_counter, auth_server) = start_refresh_server(true).await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        write_credentials(
            &auth_file,
            oauth::StoredAuth {
                client_id: "cid".into(),
                client_secret: Some("sec".into()),
                token: "stale-access-token".into(),
                refresh_token: Some("stale-refresh".into()),
                expires_at: Some(oauth::now_epoch_secs() - 10),
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        );
        let (mut manager, _dir) = manager_with(
            &serde_json::json!({
                "mcpServers": { "a": {
                    "url": mcp_url,
                    "auth": { "clientId": "cid", "clientSecret": "sec" },
                } }
            })
            .to_string(),
            None,
        );
        manager = manager.with_auth_file(auth_file.clone());
        let cancel = CancellationToken::new();

        manager
            .connect("a", &cancel, Duration::from_secs(5))
            .await
            .expect("the expired token is refreshed and the connect succeeds");
        assert_eq!(manager.status_lines(), vec!["a: Connected"]);

        // The FRESH token was the one sent (never the stale one). Assert with
        // `contains` — the OAuth header framing is out of this task's scope.
        let seen = mcp_seen.lock().unwrap().join("\n");
        assert!(
            seen.contains("refreshed-access-token"),
            "the refreshed token is the one sent: {seen}"
        );
        assert!(
            !seen.contains("stale-access-token"),
            "the stale token must not be sent: {seen}"
        );

        // Exactly ONE `refresh_token` grant was used (the recorded vec is only
        // written by `/token`, so one occurrence = one grant — the auth-server
        // request counter is 2 here: the `discover` GET + this POST).
        let fields = auth_fields.lock().unwrap().clone();
        assert_eq!(
            fields
                .iter()
                .filter(|f| f.as_str() == "grant_type=refresh_token")
                .count(),
            1,
            "exactly one refresh_token grant: {fields:?}"
        );
        assert!(
            fields.contains(&"refresh_token=stale-refresh".to_string()),
            "the stored refresh token was exchanged: {fields:?}"
        );

        // The refresh was PERSISTED (the next session does not re-run the flow).
        let persisted = oauth::load_credentials(&auth_file);
        let stored = persisted.get("a").expect("the entry is still there");
        assert_eq!(stored.token, "refreshed-access-token");
        assert_eq!(stored.refresh_token.as_deref(), Some("rotated-refresh"));
        assert!(
            stored
                .expires_at
                .is_some_and(|at| at > oauth::now_epoch_secs()),
            "the new expiry is in the future: {:?}",
            stored.expires_at
        );

        mcp_server.abort();
        auth_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_refresh_cancelled_mid_flight_still_persists_the_rotation() {
        // Finding 2: `usable_token_provider` runs inside the `mcp` tool arm's
        // `select!` against the TURN token, so a Stop drops this future
        // wherever it happens to be — including after the auth server has
        // consumed our refresh token and answered with a ROTATED one. Losing
        // that response leaves a dead token on disk and the server in
        // `NeedsAuth` (a full browser flow). The refresh therefore runs on a
        // detached task that writes its own result: dropping the caller cannot
        // drop the task.
        //
        // The auth server holds its `/token` answer open, so the caller is
        // guaranteed to be dropped while the grant is in flight.
        let (auth_base, auth_fields, _counter, auth_server) =
            start_refresh_server_with(true, Duration::from_millis(400)).await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        write_credentials(
            &auth_file,
            oauth::StoredAuth {
                client_id: "cid".into(),
                client_secret: Some("sec".into()),
                token: "stale-access-token".into(),
                refresh_token: Some("stale-refresh".into()),
                expires_at: Some(oauth::now_epoch_secs() - 10),
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        );

        // Cancel the turn the flow is racing against, exactly the way the tool
        // arm does: `select!` on the turn token drops the provider future while
        // the grant is in flight, and the discarded branch's value is gone. The
        // stop is triggered by the auth server RECORDING the `/token` POST (not
        // by a sleep), so the grant is provably in flight when the caller dies.
        let turn = CancellationToken::new();
        let auth_file_for_call = auth_file.clone();
        let stopper = {
            let turn = turn.clone();
            let fields = auth_fields.clone();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            tokio::spawn(async move {
                loop {
                    let sent = fields
                        .lock()
                        .unwrap()
                        .contains(&"grant_type=refresh_token".to_string());
                    if sent {
                        break;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the refresh grant was never attempted"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                turn.cancel();
            })
        };
        let call = async move {
            tokio::select! {
                provider = usable_token_provider("a", &auth_file_for_call) => {
                    let _ = provider;
                    panic!("the refresh finished before the Stop — the drop was not mid-flight")
                }
                _ = turn.cancelled() => {}
            }
        };
        // The caller is dropped at the end of this statement, mid-refresh.
        call.await;
        stopper.await.expect("the stopper ran");
        // The Stop returned immediately (the caller did NOT wait for the grant,
        // which is still being held open), and nothing has been written yet.
        let fields_dropped = auth_fields.lock().unwrap().clone();
        assert!(
            fields_dropped.contains(&"grant_type=refresh_token".to_string()),
            "the refresh grant was in flight when the caller was dropped: {fields_dropped:?}"
        );
        assert_eq!(
            oauth::load_credentials(&auth_file)
                .get("a")
                .map(|s| s.token.as_str()),
            Some("stale-access-token"),
            "nothing is written before the response arrives"
        );

        // The detached task finishes the job the dropped caller could not: the
        // rotation lands, and the file stays a complete document.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let stored = oauth::load_credentials(&auth_file);
            if stored.get("a").map(|s| s.token.as_str()) == Some("refreshed-access-token") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the cancelled-in-flight refresh never persisted its rotation"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let persisted = oauth::load_credentials(&auth_file);
        let stored = persisted.get("a").expect("the entry is still there");
        assert_eq!(stored.refresh_token.as_deref(), Some("rotated-refresh"));
        assert!(
            stored
                .expires_at
                .is_some_and(|at| at > oauth::now_epoch_secs()),
            "the new expiry is in the future: {:?}",
            stored.expires_at
        );

        auth_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_refresh_persist_does_not_clobber_an_entry_another_writer_saved() {
        // Finding 1's lost update, end to end: `map` used to be a snapshot taken
        // BEFORE the refresh round trip, so anything another writer saved during
        // that round trip — the Settings page's `auth`, another Worker's
        // refresh — was renamed away by our save. The persist now merges into
        // the file as it is on disk when the write happens.
        let (auth_base, auth_fields, _counter, auth_server) =
            start_refresh_server_with(true, Duration::from_millis(400)).await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        write_credentials(
            &auth_file,
            oauth::StoredAuth {
                client_id: "cid".into(),
                client_secret: Some("sec".into()),
                token: "stale-access-token".into(),
                refresh_token: Some("stale-refresh".into()),
                expires_at: Some(oauth::now_epoch_secs() - 10),
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        );
        let file_for_call = auth_file.clone();
        let call =
            tokio::spawn(async move { usable_token_provider("a", &file_for_call).await.is_some() });

        // The other writer lands INSIDE the refresh's round trip (the same
        // window the snapshot used to straddle): a fresh token for a DIFFERENT
        // server, written through the same persist path the Settings page uses.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let sent = auth_fields
                .lock()
                .unwrap()
                .contains(&"grant_type=refresh_token".to_string());
            if sent {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the refresh grant was never attempted"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        oauth::store_credentials(
            &auth_file,
            "settings-page",
            oauth::StoredAuth {
                client_id: "cid-b".into(),
                client_secret: Some("sec-b".into()),
                token: "settings-token".into(),
                refresh_token: None,
                expires_at: Some(oauth::now_epoch_secs() + 3600),
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        )
        .expect("the other writer saves");

        assert!(call.await.expect("the provider task runs"));

        let stored = oauth::load_credentials(&auth_file);
        assert_eq!(
            stored.get("a").map(|s| s.token.as_str()),
            Some("refreshed-access-token"),
            "our refresh landed"
        );
        assert_eq!(
            stored.get("settings-page").map(|s| s.token.as_str()),
            Some("settings-token"),
            "the entry another writer saved during the round trip survives"
        );

        auth_server.abort();
    }

    /// A `JoinError` from a task that really panicked (the runtime reports a
    /// panic; `JoinError` has no public constructor, so this is the only honest
    /// way to obtain one).
    async fn a_panicked_task() -> tokio::task::JoinError {
        let task = tokio::spawn(async move {
            panic!("the detached refresh blew up");
            #[allow(unreachable_code)]
            Some("never".to_string())
        });
        task.await.expect_err("a panicking task yields a JoinError")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_died_refresh_task_is_named_rather_than_silently_none() {
        // Finding: `task.await.ok().flatten()` mapped a PANIC in the detached
        // refresh task to `None` — identical to "the auth server said no" — and
        // printed nothing. The detach's whole justification is that a rotation
        // is never lost silently, so a task that dies has to say so.
        //
        // Driven with REAL `JoinError`s rather than a mock: `is_panic()` /
        // `is_cancelled()` are what the code branches on, and only a real error
        // proves the branch is reached.
        let panicked = a_panicked_task().await;
        assert!(panicked.is_panic(), "the fixture is a panic");
        let note = refresh_failure_note("linear", &Err(panicked))
            .expect("a dead task is reported, not swallowed");
        assert!(
            note.contains("linear"),
            "the line names the server it belongs to: {note}"
        );
        assert!(
            note.to_lowercase().contains("panic"),
            "the line says the task DIED rather than failing a grant: {note}"
        );

        // The other way a detached task ends without a token: the runtime went
        // away (an aborted task yields the cancelled `JoinError`).
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Some("never".to_string())
        });
        task.abort();
        let aborted = task.await.expect_err("an aborted task yields a JoinError");
        assert!(aborted.is_cancelled(), "the fixture is a cancellation");
        let note = refresh_failure_note("linear", &Err(aborted))
            .expect("a cancelled task is reported too");
        assert!(note.contains("linear"), "the line names the server: {note}");

        // A refresh that simply returned `None` (the ADR 0015 public-client
        // guard, or a rejected grant) is logged ONCE as well — the caller's
        // `None` is otherwise indistinguishable from all of the above.
        let note =
            refresh_failure_note("linear", &Ok(None)).expect("a no-token refresh is reported");
        assert!(note.contains("linear"), "the line names the server: {note}");
        assert!(
            note.contains("unauthenticated") || note.contains("NeedsAuth"),
            "the line says what happens next: {note}"
        );

        // A SUCCESS stays silent (this runs on the routine connect path).
        assert!(
            refresh_failure_note("linear", &Ok(Some("tok".to_string()))).is_none(),
            "a refresh that produced a token prints nothing"
        );

        // And `report_refresh_join` (the function the spawn site actually calls)
        // returns the token on success and `None` on every failure — the value
        // contract the caller depends on is unchanged by the added logging.
        assert_eq!(
            report_refresh_join("linear", Ok(Some("tok".to_string()))).as_deref(),
            Some("tok")
        );
        let quiet = a_panicked_task().await;
        assert!(report_refresh_join("whisper", Ok(None)).is_none());
        assert!(report_refresh_join("whisper", Err(quiet)).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_spawn_site_reports_through_the_shared_helper() {
        // The WIRING pin. `refresh_failure_note` is only worth anything if the
        // detached task's join goes through it, and that is exactly the line a
        // later "simplification" would restore to `task.await.ok().flatten()` —
        // the tests above would stay green while the panic went back to being
        // silent. So: read the spawn site's own source and require the call.
        //
        // Scoped to `refresh_and_persist`'s body (not the file), because the
        // file legitimately awaits other tasks. A source scan is a blunt
        // instrument: it catches the revert, it does not prove the call is
        // reached at runtime — which is what the real-`JoinError` test above is
        // for. Both halves are here on purpose.
        let src = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/agent/mcp/manager.rs"
        ))
        .expect("the manager source is readable from a test");
        let start = src
            .find("async fn refresh_and_persist(")
            .expect("refresh_and_persist exists");
        // The function ends at the first `\n}` (column 0) after its head.
        let end = src[start..]
            .find("\n}")
            .expect("refresh_and_persist has a closing brace");
        let body = &src[start..start + end];
        // Vacuity guard: finding the `tokio::spawn` proves the captured region
        // really is the function with the detached task (and reaches its join),
        // rather than an empty or mis-sliced slice that would pass any `!`
        // assertion for free.
        assert!(
            body.contains("tokio::spawn"),
            "the scanned region of `refresh_and_persist` ({end} bytes) does not contain the \
             detached task — the slice is wrong, so the checks below would prove nothing"
        );
        assert!(
            body.contains("report_refresh_join"),
            "`refresh_and_persist` must join through `report_refresh_join`, which is where \
             the dead-task line is printed"
        );
        assert!(
            !body.contains("ok().flatten()"),
            "`ok().flatten()` is the shape that discarded the JoinError — if the body still \
             contains it, a panic in the detached task is silent again"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_refresh_lands_in_needs_auth() {
        // A refresh that fails (the auth server rejects it) degrades to the
        // existing fallback: no token → a 401 → `NeedsAuth` → the `auth`
        // action. And a failed refresh WRITES NOTHING (a transient auth-server
        // outage must not destroy a possibly-reusable refresh token).
        let (mcp_url, _mcp_seen, mcp_server) =
            start_token_gated_server("refreshed-access-token").await;
        let (auth_base, auth_fields, _counter, auth_server) = start_refresh_server(false).await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        write_credentials(
            &auth_file,
            oauth::StoredAuth {
                client_id: "cid".into(),
                client_secret: Some("sec".into()),
                token: "stale-access-token".into(),
                refresh_token: Some("stale-refresh".into()),
                expires_at: Some(oauth::now_epoch_secs() - 10),
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        );
        let (mut manager, _dir) = manager_with(
            &serde_json::json!({
                "mcpServers": { "a": {
                    "url": mcp_url,
                    "auth": { "clientId": "cid", "clientSecret": "sec" },
                } }
            })
            .to_string(),
            None,
        );
        manager = manager.with_auth_file(auth_file.clone());
        let cancel = CancellationToken::new();

        let r = manager.connect("a", &cancel, Duration::from_secs(5)).await;
        assert!(r.is_err(), "a failed refresh → an error: {r:?}");
        let e = r.unwrap_err();
        assert!(
            e.contains("requires authentication"),
            "the existing needs-auth message: {e}"
        );
        assert_eq!(
            manager.status_lines(),
            vec!["a: NeedsAuth"],
            "a failed refresh still degrades to NeedsAuth"
        );

        // A refresh WAS attempted (no exact request count — `oauth::refresh`
        // makes two calls: the well-known `discover` + the `/token` POST).
        let fields = auth_fields.lock().unwrap().clone();
        assert!(
            fields.contains(&"grant_type=refresh_token".to_string()),
            "the refresh_token grant was attempted: {fields:?}"
        );
        assert!(
            fields.contains(&"refresh_token=stale-refresh".to_string()),
            "with the stored refresh token: {fields:?}"
        );

        // The credentials file is UNCHANGED (a failed refresh writes nothing).
        let kept = oauth::load_credentials(&auth_file);
        assert_eq!(
            kept.get("a").map(|s| s.token.as_str()),
            Some("stale-access-token"),
            "a failed refresh must not touch the stored entry"
        );

        mcp_server.abort();
        auth_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_token_without_an_expiry_is_never_refreshed() {
        // PASSING BEFORE the implementation too, on purpose: it pins behaviour
        // that must NOT regress — an entry with `expires_at: None` is never
        // refreshed (no round trip, no downgrade of a working token). Do not
        // "fix" it into asserting a refresh.
        let (mcp_url, _mcp_seen, mcp_server) = start_token_gated_server("stale-access-token").await;
        let (auth_base, auth_fields, counter, auth_server) = start_refresh_server(true).await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        write_credentials(
            &auth_file,
            oauth::StoredAuth {
                client_id: "cid".into(),
                client_secret: Some("sec".into()),
                token: "stale-access-token".into(),
                refresh_token: Some("stale-refresh".into()),
                expires_at: None,
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        );
        let (mut manager, _dir) = manager_with(
            &serde_json::json!({
                "mcpServers": { "a": {
                    "url": mcp_url,
                    "auth": { "clientId": "cid", "clientSecret": "sec" },
                } }
            })
            .to_string(),
            None,
        );
        manager = manager.with_auth_file(auth_file.clone());
        let cancel = CancellationToken::new();

        manager
            .connect("a", &cancel, Duration::from_secs(5))
            .await
            .expect("the stored token works");
        assert_eq!(manager.status_lines(), vec!["a: Connected"]);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "expires_at: None never spends a refresh round trip"
        );
        assert!(auth_fields.lock().unwrap().is_empty());

        mcp_server.abort();
        auth_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_public_client_is_never_auto_refreshed_end_to_end() {
        // The ADR 0015 guard at the CALL SITE (`oauth.rs` covers the guard
        // inside `refresh` itself). PASSING BEFORE the implementation too, on
        // purpose — it pins that a public client (no `clientSecret`) makes ZERO
        // auth-server requests: the guard short-circuits before `discover`, so
        // the degradation lands on the interactive flow, never a token POST the
        // auth server would reject with `invalid_client`.
        let (mcp_url, _mcp_seen, mcp_server) =
            start_token_gated_server("refreshed-access-token").await;
        let (auth_base, _auth_fields, counter, auth_server) = start_refresh_server(true).await;
        let dir = tempfile::tempdir().expect("a temp dir");
        let auth_file = dir.path().join("mcp-auth.json");
        write_credentials(
            &auth_file,
            oauth::StoredAuth {
                client_id: "cid".into(),
                client_secret: None,
                token: "stale-access-token".into(),
                refresh_token: Some("stale-refresh".into()),
                expires_at: Some(oauth::now_epoch_secs() - 10),
                auth_server_url: auth_base.clone(),
                scope: None,
            },
        );
        let (mut manager, _dir) = manager_with(
            // No `clientSecret` — the def and the stored entry tell the same
            // story (a pre-registered PUBLIC client).
            &serde_json::json!({
                "mcpServers": { "a": {
                    "url": mcp_url,
                    "auth": { "clientId": "cid" },
                } }
            })
            .to_string(),
            None,
        );
        manager = manager.with_auth_file(auth_file.clone());
        let cancel = CancellationToken::new();

        let r = manager.connect("a", &cancel, Duration::from_secs(5)).await;
        assert!(r.is_err(), "a public client's dead token → an error: {r:?}");
        assert_eq!(manager.status_lines(), vec!["a: NeedsAuth"]);
        assert_eq!(
            counter.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "the ADR 0015 guard short-circuits before discover — zero auth-server calls"
        );

        mcp_server.abort();
        auth_server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_needs_auth_server_is_not_auto_retried() {
        // An OAuth server that answers with a 401 → `NeedsAuth`. A
        // subsequent `call_tool` is NOT auto-retried (an explicit `auth` is
        // required).
        let (url, server) = start_401_server().await;
        let (mut manager, _dir) = manager_with(
            &serde_json::json!({
                "mcpServers": { "a": { "url": url, "auth": "oauth" } }
            })
            .to_string(),
            None,
        );
        let cancel = CancellationToken::new();
        // The first `call_tool` tries to connect (a 401 → `NeedsAuth`).
        let r = manager
            .call_tool(
                "a",
                "echo",
                &serde_json::json!({}),
                &cancel,
                Duration::from_secs(2),
            )
            .await;
        assert!(r.is_err(), "a 401 → an error: {r:?}");
        assert_eq!(manager.status_lines(), vec!["a: NeedsAuth"]);
        // A subsequent `call_tool` is NOT auto-retried (a `NeedsAuth` state).
        let r = manager
            .call_tool(
                "a",
                "echo",
                &serde_json::json!({}),
                &cancel,
                Duration::from_secs(2),
            )
            .await;
        let Err(e) = r else {
            panic!("a `NeedsAuth` state is not auto-retried (should be an error)");
        };
        assert!(
            e.contains("NeedsAuth"),
            "a `NeedsAuth` state is not auto-retried: {e}"
        );
        server.abort();
    }
}

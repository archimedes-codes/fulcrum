# Grounding report — `daemon-hardening` · area `transport-serve`

**Feature:** daemon-hardening. **Area:** `transport-serve` — the local transport (`fulcrum/src/transport/**`) and the accept loop / client multiplex (`fulcrum/src/serve.rs`), plus their integration tests.

**Tasks to be planned from this report:**

- **T3 (issue #4)** — peer-credential check + session/rate limits + required `hello` handshake, on the local socket/pipe.
- **T4 (issue #3)** — split `Connection` and `serve_client` so a slow `attach` cannot stall *that client's own* event relay.

These are the two heaviest tasks of the feature, and **both edit `fulcrum/src/serve.rs`**. That collision is why they run in different waves. This report names the exact regions each needs (`## Will touch`, `## Provides`, `## Constraints`).

**Grounded against:** this tree as read at report time. No `cargo build`/`test`/`clippy` run (a sibling agent owns the build lock); manifests were read as text. The tree is T9-complete: `fulcrum serve` (transport, `serve.rs`, `db`, worker factory, CLI) shipped in PR #2 (`38a9986`), and the T9 plan's roadmap doc was removed as shipped (`958306d`) — so the conventions below ("two decisions already made", the T9 waves) live in git @@history (commit `e44f382`) and in AGENTS.md, not in a current `docs/` file.

---

## 1. Findings

### 1.1 The transport layer (`fulcrum/src/transport/`) — what exists today

**`transport/mod.rs` (181 lines)** — platform-neutral surface, unconditional:

- `pub mod frame;` + `#[cfg(unix)] pub mod unix;` + `#[cfg(windows)] pub mod windows;`
- `pub use frame::Connection;` ; `#[cfg(unix)] pub use unix::{connect, default_endpoint, Listener};` / `#[cfg(windows)] pub use windows::{connect, default_endpoint, Listener};`
- `pub enum Endpoint { Socket(PathBuf), Pipe(String) }` — **both variants exist on every platform** ("the platform decision belongs in `Listener`/`connect`, which ARE gated; `Endpoint` just names an address"). Constructor fns `Endpoint::socket(path)` / `Endpoint::pipe(name)`.
- `pub enum TransportError` with variants: `Io(#[from] std::io::Error)`, `Encode(String)`, `Decode(String)`, `LineTooLong { limit: usize }` (the ONE error item that is also a full stop), `SocketPathTooLong { length, limit, path }`, `WrongPlatform { family, platform, endpoint }`, `NoDataDir`.

**The module header's gating rule — load-bearing and cited repeatedly:** "The gating is load-bearing and is NOT to be replaced by `if cfg!(unix)`: a `cfg!` condition is a runtime `if`, so the inactive platform's code still compiles into the binary and then trips `dead_code`/`unused` under `cargo clippy --all-targets -- -D warnings`. This repo has been caught by that twice. `cfg` on the `mod` items removes the module from the build outright."

**`transport/unix.rs` (287 lines)** — `#[cfg(unix)]`:

- `pub fn default_endpoint() -> Result<Endpoint, TransportError>` — `<data_dir>/fulcrum/daemon.sock`, length-checked (`sun_path_capacity()` = 108 on Linux, 104 on macOS/BSD).
- `pub struct Listener { listener: TokioUnixListener, endpoint: Endpoint }` with:
  - `pub fn bind(endpoint: Endpoint) -> Result<Self, TransportError>` → delegates to `fn bind_socket(path: PathBuf) -> Result<Listener, TransportError>`, which calls **`fn bind_private(path: &Path) -> std::io::Result<TokioUnixListener>`** (umask 0o077 tighten + `chmod 0600`; the "socket is the credential" enforcement) and `fn ensure_private_dir(path)` → **`crate::db::ensure_private_dir`** (the ONE copy), plus `fn reclaim_stale_socket(path: &Path) -> Result<(), TransportError>` on `EADDRINUSE` (blocking `std::os::unix::net::UnixStream::connect` probe: live daemon ⇒ refuse "a live daemon is answering"; `ECONNREFUSED`/error ⇒ `remove_file`).
  - `pub fn endpoint(&self) -> Endpoint`
  - **`pub async fn accept(&self) -> Result<Connection, TransportError>`** — `let (stream, _addr) = self.listener.accept().await?; Ok(Connection::new(stream))`. `_addr` is currently discarded; a peer-credential check hooks HERE (the accepted `stream` is a `tokio::net::UnixStream` in this fn's scope before it is moved into `Connection::new`).
- `pub async fn connect(endpoint: Endpoint) -> Result<Connection, TransportError>`.

**`transport/windows.rs` (297 lines)** — `#[cfg(windows)]`:

- `const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\fulcrum"`; `pub fn default_endpoint()`.
- `pub struct Listener { name: String, armed: StdMutex<Option<NamedPipeServer>> }` with the **one-instance-always invariant**.
  - `pub fn bind(endpoint)` → `fn bind_with_retry(name, first)` → `fn create_pipe(name: &str, first: bool) -> Result<NamedPipeServer, TransportError>` using `ServerOptions::new()` + `first_pipe_instance(true)` + `pipe_mode(Byte)` + `reject_remote_clients(true)`.
  - **`pub async fn accept(&self) -> Result<Connection, TransportError>`** — arms the NEXT instance under the mutex, `server.connect().await?` (completes when a client opens the pipe), `Ok(Connection::new(server))`.
- `pub async fn connect(endpoint)` — `ClientOptions::new().read(true).write(true).open(name)` retrying `ERROR_PIPE_BUSY` (231) — `fn is_pipe_busy(e: &std::io::Error) -> bool { e.raw_os_error() == Some(231) }`, the "no-`windows-sys`" pattern.
- **ACCESS CONTROL, stated in this file's header:** `ServerOptions::create` passes a NULL `lpSecurityAttributes`, so the pipe's DACL is the creating token's default; "there is therefore NO daemon-side equivalent of the unix socket's 0600"; the only real mitigation without a `windows-sys` dependency is T13's protocol-layer auth. **This file documents the `windows`/`windows-sys` absence as deliberate.**

**Manifest facts (`fulcrum/Cargo.toml`, read as text):**

- `tokio = { version = "1", features = ["process", "io-util", "io-std", "sync", "macros", "time", "net", "rt", "rt-multi-thread"] }` — the comment: "`net` alone gives the transport on BOTH platforms … so the `windows` / `windows-sys` crate is deliberately ABSENT and must not be added." `time` is justified as "for the named-pipe connect retry (`windows.rs`), NOT for a shutdown timer: this daemon has no shutdown path".
- `[target.'cfg(unix)'.dependencies] libc = "0.2"` — under "the SAME unix gate the engine uses … the daemon's `kill(-pgid, SIGKILL)` process-group teardown and `setpgid` are `#[cfg(unix)]`, not Linux-only".
- **No Windows-target dependency block exists.** The manifest comment says a Windows crate is a deviation; the precedent in this member is raw constants (`is_pipe_busy` = 231; `childproc.rs`'s `CREATE_NO_WINDOW`).
- `tempfile = "3"` and tokio `test-util` under `[dev-dependencies]`; no `[profile.*]` block (root-only, inherits `panic = "unwind"`).
- `libc` is already used in `unix.rs` integration tests (`tests/transport.rs` umask test), so it is available to the daemon on unix.

**CI matrix (`.github/workflows/ci.yml` + AGENTS.md):** `[ubuntu-latest, windows-latest, macos-latest]`; `cargo clippy --all-targets -- -D warnings` runs on ALL THREE; "an import that is used only inside `#[cfg(unix)]` is an unused-import warning on Windows, which `-D warnings` turns into a build failure here". Also `cargo fmt --all -- --check` and bare `cargo test` (no build step — fixture-bins contract). The only local Windows proof is `cargo check --target x86_64-pc-windows-gnu --all-targets` (unix.rs's own doc demands it for the Windows leg; `windows.rs` was **type-/lint-checked but NEVER RUN** — the dev machine is Linux, and its two behaviours `bind`'s invariant and the busy retry have NO executed test).

### 1.2 `Connection` and `pending` (`fulcrum/src/transport/frame.rs`, 571 lines)

**The framing contract (verbatim from the module header):** "one frame per `\n`-terminated JSON line — identical to the worker protocol in `fulcrum_core::worker::protocol` … a second, different framing on the client side would mean two codecs to keep in step and two sets of framing bugs. Do not 'improve' the client framing."

**Struct and methods (verbatim):**

```rust
pub struct Connection {
    /// The write half, plus the bytes of the current frame the OS has not taken
    /// yet (see `Connection::send_frame`).
    writer: tokio::io::WriteHalf<DuplexBox>,   // Box<dyn Duplex + Unpin + Send>
    /// The tail of a frame whose `write` was only partially accepted. Kept here
    /// so an abandoned `send_frame` cannot desync the stream.
    pending: Vec<u8>,
    /// Decoded inbound lines, in order. `None` (i.e. the channel closing) is EOF.
    /// Bounded — see MAX_QUEUED_INBOUND_FRAMES.
    lines: mpsc::Receiver<Result<serde_json::Value, TransportError>>,
    /// The reader task. Held so `Drop` can abort it.
    reader: tokio::task::JoinHandle<()>,
}

impl Connection {
    pub(crate) fn new<S>(stream: S) -> Self
        where S: AsyncRead + AsyncWrite + Unpin + Send + 'static
    pub async fn send_frame<T>(&mut self, frame: &T) -> Result<(), TransportError>
        where T: Serialize + ?Sized
    pub async fn recv(&mut self) -> Option<Result<serde_json::Value, TransportError>>
}
impl Drop for Connection { fn drop(&mut self) { self.reader.abort(); } }
impl std::fmt::Debug for Connection { /* manual: .field("reader", &self.reader).finish_non_exhaustive() */ }
```

`pending` is **private** and reachable only through `send_frame` — it is the only writer state in the type. Other constants in the file: `const MAX_QUEUED_INBOUND_FRAMES: usize = 1024;` and **`pub const MAX_INBOUND_LINE_BYTES: usize = 1024 * 1024;`**.

**What `pending` protects, precisely:** `send_frame` serializes the frame to a `String`, pushes `\n`, then `self.pending.extend_from_slice(line.as_bytes())` and loops `self.writer.write(&self.pending)` draining what was accepted. If that future is **abandoned mid-write** (it is still documented as "NOT a good idea to `select!` over a send" — the frame you started may still go out), the unwritten tail stays in `pending` and the NEXT `send_frame` drains it before writing anything new. That is the "cancellation-safe in the sense that matters for a framing layer" property. But if TWO writers alternated `send_frame` calls on one `Connection`, each abandoning the other's half-accepted frame, the JSONL stream would hold two interleaved half-lines and **cannot resynchronise** — the desync `frame.rs`'s whole design exists to prevent.

**Where it is reachable from:** exactly two production call sites in the whole tree — `serve_client`'s drain arm only, in `serve.rs`:
- `serve.rs:1235` — `if connection.send_frame(&gap_frame(skipped)).await.is_err()` (the dropped-frames notice), and
- `serve.rs:1241` — `if connection.send_frame(&frame).await.is_err()` (the queue drain).
Both sites are the same single `&mut connection` borrow inside one task.

**Tests:** there are **NO existing partial-write / two-writer tests** — a grep for `pending`/`partial` finds only docs. The frame.rs unit tests cover the line reader (`read_capped_line_under`): `a_line_of_exactly_the_limit_is_accepted`, `one_byte_over_the_limit_is_refused`, `an_unterminated_flood_is_refused_at_the_cap_and_no_further`, `an_unterminated_tail_at_eof_is_still_delivered`, `a_clean_eof_reports_nothing`, `the_cap_counts_bytes_not_characters`, `the_line_too_long_error_names_the_limit`. **Branch coverage of the partial-accept loop is a hole today** — `send_frame` is only exercised against real sockets that never partially accept.

### 1.3 `serve.rs` (2524 lines) — the two tasks' shared file

**Module header (`:1-130`), verbatim facts:**

- "What T9 does NOT do": No `attach`, no replay, no re-join (T10). **"No graceful shutdown frame, so `run` never returns and `WorkerManager::reap_all` is not wired"** (T6's pins and T12's sidecar lifecycle own that; a killed daemon's children exit on their stdin EOF). **No authentication: "the socket is the credential"** — "legitimately deferred to T13".
- **The T4 finding, recorded at `:94-125`** — "⚠ A slow frame on ONE connection stalls THAT connection's own relay … It is a KNOWN LIMITATION awaiting its own ticket": `serve_client` is ONE task running one `select!`; `start` runs inline, `attach` is the engine's ~5 s `ready` bound; the fix "is to split `serve_client` into a reader/reply task and a writer task. It is deliberately NOT done in this pass, and the reason is the hazard `transport::frame` documents: `send_frame` keeps the unwritten tail of a partially-accepted frame in `Connection::pending`, because a JSONL stream has no way to resynchronise once two writers interleave. A safe split therefore needs the `Connection` itself split into an owning reader and an owning writer (`tokio::io::split` over the duplex, with `pending` moving to the writer), and that is a change to the one file whose entire purpose is preventing this class of desync".
- "Every buffer a client can grow is BOUNDED (finding 7) — the fan-out ring (`FAN_OUT_CAPACITY`), one connection's outbound queue (`CLIENT_QUEUE_CAPACITY`), whose full case produces a `gap` notice instead of an unbounded allocation), and in `transport::frame` the inbound line length and the reader's queue."

**The two constants (verbatim):**

```rust
/// How many undelivered frames the fan-out broadcast holds before it starts
/// dropping for the slowest connection.
const FAN_OUT_CAPACITY: usize = 4096;

/// How many undelivered frames ONE connection's outbound queue holds before the
/// daemon starts dropping for it (finding 7).
const CLIENT_QUEUE_CAPACITY: usize = 1024;
```

Plus two compile-time asserts: `CLIENT_QUEUE_CAPACITY > 0` ("tokio's channel panics on a capacity of 0") and `CLIENT_QUEUE_CAPACITY < FAN_OUT_CAPACITY` ("the per-client queue must not be able to buffer more than the shared ring upstream holds"). **Both numeric values are load-bearing:** `4300`-wide fan-out ring, 1024 client queue, 1024 inbound frame queue — the "faster than the slowest" axiom each bound is sized against.

**The `gap`-notice path (verbatim shape — a planner must keep it intact):**

```rust
/// The notice that tells a client it fell behind (finding 7).
fn gap_frame(skipped: u64) -> serde_json::Value {
    serde_json::json!({ "type": "gap", "skipped": skipped })
}
```
- Addressed to the CONNECTION, deliberately no `sessionId`, so `watched` passes it on every socket.
- Emitted from **two** paths: the fan-out `Lagged(skipped)` arm (queued as its own frame, "NOT folded into `dropped`") and the drain arm's `dropped > 0` prelude (built from `std::mem::take(&mut dropped)` and sent with the next queued frame).
- `fn enqueue(tx: &tokio::sync::mpsc::Sender<serde_json::Value>, frame: serde_json::Value, dropped: &mut u64) -> bool` — `try_send`; `TrySendError::Full(_)` ⇒ `*dropped += 1; true` (keep the connection); `TrySendError::Closed(_)` ⇒ `false` (end it).
- `fn watched(frame: &serde_json::Value, watching: &HashSet<String>) -> bool` — top-level `sessionId` in `watching`; absent ⇒ `true`.

**`serve_client` (verbatim signature):**

```rust
async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection) {
    let mut watching: HashSet<String> = HashSet::new();
    let (tx, mut queued) = tokio::sync::mpsc::channel::<serde_json::Value>(CLIENT_QUEUE_CAPACITY);
    let mut dropped: u64 = 0;
    let mut events = daemon.fan_out.subscribe();
    let mut seen = 0usize;
    loop {
        tokio::select! {
            inbound = connection.recv() => { ... }          // decode; enqueue error reply on Err; else handle_frame(...).await INLINE, enqueue reply
            fan = events.recv() => { ... }                  // Ok: watched(&frame, &watching) && enqueue(...) else break; Lagged(skipped): eprintln! + enqueue(gap_frame(skipped)); Closed: break
            queued_frame = queued.recv() => {               // dropped>0 ⇒ send_frame(&gap_frame(...)); then send_frame(&frame)
                ...
            }
        }
    }
    // "The connection is gone." — NO detach, NO kill: dropping the subscription
    // is the whole teardown. If tempted to add teardown, re-read
    // tests::a_client_that_vanishes_mid_turn... and REAP_GRACE.
}
```

`start`'s reply comes from `async fn start(daemon: &Arc<Daemon>, watching: &mut HashSet<String>, frame: &serde_json::Value) -> serde_json::Value`, whose heart is the claim + `daemon.manager.attach(&session_id, &env).await` (the ~5 s bound lives in the engine, `fulcrum-core/src/worker/manager/mod.rs: pub async fn attach(&self, session_id: &str, env: &StartEnv) -> Result<(), WorkerError>`). The starting-set lock is held across `attach` via `SessionClaim` (an RAII guard), but never *inside the await* — the lock is std sync and only the sync work is under it.

**The accept loop (verbatim):**

```rust
/// Run the accept loop forever.
/// Never returns. T9 has no shutdown frame; see the module header.
pub async fn run(listener: Listener, db: Arc<Db>, factory: Arc<dyn WorkerFactory>) {
    let daemon = Daemon::new(db, factory);
    loop {
        match listener.accept().await {
            Ok(connection) => { let daemon = Arc::clone(&daemon);
                let handle = tokio::spawn(serve_client(daemon, connection));
                log_on_join_error(handle); }
            Err(e) => eprintln!("fulcrum serve: accept failed: {e}"),
        }
    }
}
```

- `Daemon::new(db: Arc<Db>, factory: Arc<dyn WorkerFactory>) -> Arc<Self>`; `Daemon`'s fields are `manager: Arc<WorkerManager>`, `persister: Arc<TranscriptPersister>`, `db: Arc<Db>`, `fan_out: tokio::sync::broadcast::Sender<serde_json::Value>`, `subagents: Arc<StdMutex<HashMap<String, SubagentRecord>>>`, `starting: Arc<StdMutex<HashSet<String>>>`.
- **`Daemon::claim_session` returns a `Result<SessionClaim, String>`; the sole routing entry it needs is the `workers` map, whose SOLE maintainer is `daemon.manager`.** `SessionClaim` implements `Drop` that removes the id.
- `handle_frame` returns `Option<serde_json::Value>` and matches `"hello" | "start" | "prompt" | "close"`; unknown kinds ⇒ an `error` frame naming the type. The `hello` arm is one line: `"protocolVersion": PROTOCOL_VERSION, "serverVersion": env!("CARGO_PKG_VERSION")`.
- `start_env(frame)` strips `type` then `serde_json::from_value::<StartEnv>` (the daemon's surface keeps the wire shape of the engine's `StartEnv`, camelCase).
- `Daemon::record_session(&self, env: &StartEnv) -> Result<(), String>` (eager `sessions` row at `start`).
- `eprintln!` is the daemon's operator console (main.rs: stderr inherited; no `windows_subsystem` attribute).

**`StartEnv` permission handling in this file — the precise facts a planner needs:**

1. **Nothing in `serve.rs`, `transport/**`, or the response path ever READS `permission`.** The envelope is handed verbatim to `WorkerManager::attach`, which sends it to the child over the worker protocol. The child's agent gate (`agent_loop` → `set_permission`) is the only consumer.
2. **`serde` default:** `permission: Option<PermissionBlock>` (camelCase) with NO `#[serde(default)]` on the field; missing-key decodes to `None` via the `Option` rule ("an old supervisor's `start` without the key decodes to `None`").
3. **`None` semantics:** "the file does not speak about permissions and the child's gate starts from 'no narrowing'" (i.e., a client `start` carries no permission block unless it wants to impose one).
4. **`start_env`'s stripping is `payload.remove("type")`, nothing else** — `permission` (and every other StartEnv field) rides through as-is.
5. There is a dual-path readiness probe: `tests/serve.rs::wait_until_listening` (platform-neutral `connect` + drop) and the stale-socket probe in `reclaim_stale_socket` (blocking connect). Both land in `run`'s accept path as **EOF-with-no-frames connections**. T3's handshake and rate controls **must treat "connect-and-EOF" as a normal, non-fatal event** (see `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` and the `seen == 0` no-log rule). "A connection that never sent a frame is not even worth a log line."

**Testable-unit surface:** `handle_frame(&daemon, &mut watching, &frame).await -> Option<Value>` already isolates every client-frame routing decision from the socket, and `Daemon::new` builds with no tokio runtime beyond the test's own and no worker executable — so the same harness pattern can unit-test T3's handshake rejection and T4's routing without a live worker.

**Known structural constraint:** `Daemon::new`'s `on_event`/`on_crash` closures are `Arc::new(move |…| …)` that do NOT hold the manager strongly (late-bound `Weak` in `manager_slot`). Any limit counter a plan adds inside `serve_client` must live in `Daemon` (an `Arc`-shared field) or in the client task itself (a local). T3's "session limits" versus "client per-frame rate" differ: the session bound is a daemon-global resource, the frame rate is per-connection local state.

### 1.4 `worker_types.rs` — `StartEnv`, verbatim

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartEnv {
    pub session_id: String,
    pub cwd: String,
    pub mode: StartMode,
    pub transcript: Option<Vec<ChatMessage>>,
    pub model: Model,
    pub catalog: ModelCatalog,
    pub thinking: Option<String>,
    pub trusted: bool,
    pub enabled_tools: Option<Vec<String>>,
    pub permission: Option<PermissionBlock>,   // (issue #101) ... `None` → starts from "no narrowing"
    pub config_dir: String,
    pub subagent_enabled: bool,
    pub system_prompt: Option<String>,
    pub incarnation: String,
    #[serde(default)] pub cost_usd: Option<f64>,
    #[serde(default)] pub file_policy: FilePolicy,
    #[serde(default)] pub web: WebSettings,
    #[serde(default)] pub mcp_servers: std::collections::HashMap<String, serde_json::Value>,
    #[serde(default)] pub agent_overrides: std::collections::HashMap<String, AgentOverrideFields>,
    #[serde(default)] pub providers: Vec<ProviderConfig>,   // tail verified at src/worker_types.rs
}
```

Constructor: `impl StartEnv { #[allow(clippy::too_many_arguments)] pub fn from_parts(session_id, cwd, mode, transcript, model, catalog, thinking, trusted, enabled_tools, config_dir, subagent_enabled, system_prompt, permission) -> Self }` — minted `incarnation`, defaults for the five T3 fields. (The T3-tail marked `(Fulcrum T3)` in the docs is the engine's own desktop-config packaging, an unrelated numbering.) `PROTOCOL_VERSION` and `StartMode` come from `fulcrum_core::worker::protocol`; `PermissionBlock` from `fulcrum_core::definition_permission` (`pub struct PermissionBlock { pub tools: Vec<(ToolKey, Rules)> }`).

### 1.5 Existing integration coverage (what "still passes" means)

**`fulcrum/tests/transport.rs` (1139 lines, loopback only over real sockets):**
- Platform-neutral top three: `a_jsonl_frame_round_trips_in_both_directions`, `the_receive_stream_ends_at_eof_rather_than_hanging`, `an_endpoint_from_the_other_platform_is_rejected_not_mis_bound`.
- `#[cfg(unix)] mod unix_specific`: `a_bound_socket_is_owner_only`, `a_created_socket_directory_is_owner_only`, `binding_does_not_remode_a_directory_it_did_not_create`, `binding_restores_the_umask_it_temporarily_tightened`, `holding_the_bind_lock_blocks_another_bind_from_starting`, `a_socket_path_too_long_for_sockaddr_un_reports_the_length_and_the_path`, `the_default_endpoint_is_under_the_data_dir_and_within_the_sun_path_limit`, `two_frames_written_in_one_syscall_arrive_as_two_frames`, `a_malformed_line_is_an_error_item_and_the_stream_survives_it`, `a_stale_socket_file_is_reclaimed_so_a_restart_can_bind`, `a_live_daemons_socket_is_left_alone_and_the_second_bind_fails`, `a_final_frame_with_no_trailing_newline_is_still_delivered`, `an_oversized_unterminated_line_is_refused_and_ends_the_stream`, `a_line_of_exactly_the_limit_is_still_a_frame`, `connecting_to_a_socket_that_nothing_listens_on_errors_instead_of_hanging`.
- Guards: `every_bind_in_this_file_goes_through_the_serialiser`, `BIND_SERIALIZER` static + `bind`/`bind_holding`, `FRAME_BUDGET = 10s`, `recv_one`/`accept_one`/`dial`/`recv_checked` with the budget.
- **No test today calls `send_frame` twice from two tasks, and none partially accepts a write.** The raw-byte "BAD peer" probe pattern (`tokio::io::split(UnixStream::connect(..))` + `write_all`) is the honest way these tests reach past the framing layer — explicitly cited as "widening the public API with a `send_raw` for test convenience would be the dishonest way".

**`fulcrum/tests/serve.rs` (2411 lines, drives the real binary; `Daemon` harness spawns `CARGO_BIN_EXE_fulcrum serve`, `Client` wraps `Connection` with a consuming pending queue + `delivered` echo):**
- `close_ends_the_session_and_the_child_exits`
- `a_session_streams_events_and_persists_to_the_database`
- `a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting` — THE durability pin. **Do-not-break guards around it:** `REAP_GRACE = 5s` (2.5× the engine's 2 s reap grace — the comment: "a survival window shorter than the reap grace makes half (a) decorative"), `alive_throughout`, `pid_is_alive`, row-count half via a SECOND rusqlite connection.
- `two_sessions_on_two_sockets_do_not_see_each_others_events` — filters on the **TOP-LEVEL** `sessionId`; connection-addressed frames (`hello`, `gap`) legitimately present on every socket.
- `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` — three connect-and-drop probes, then a full session must work. **T3's handshake must keep this green.**
- `a_second_daemon_on_the_same_endpoint_exits_non_zero_and_leaves_the_first_running` — refusal must contain "cannot bind"; on unix also "in use". Note: the REFUSED second daemon exits before `run` — no handshake there.
- `a_daemon_that_cannot_open_its_database_leaves_no_socket_behind`
- `an_unknown_frame_is_answered_and_the_connection_survives` — unknown `type` (e.g. `attach`) gets an `error` frame and the connection survives.
- `a_malformed_line_gets_an_error_and_the_stream_continues` — raw-dial garbage line ⇒ `error` frame; framed client still served.
- `repeated_disconnects_do_not_degrade_the_daemon` — three full start/prompt/drop cycles; 5 s promptness budget; `spawn_count() == 3`.
- `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat` — 1 168 009 requests/895 MB (unbounded) vs 1 375/19 MB (bounded) measurement; `CEILING = 50_000`; RSS `+64 MiB` cap on Linux.
- `a_client_that_never_reads_does_not_stall_the_daemon` — stalled client + healthy client; the stalled session's rows land regardless.
- `two_connections_racing_to_start_one_id_leave_exactly_one_child` — exactly one `ok: true`, unix half asserts one worker child via `pgrep`.
- `a_session_whose_worker_crashed_can_be_started_again` (fixture `__crash__`; live session still refused with "already"), `a_prompt_for_an_unknown_session_is_answered_not_dropped`, `a_fresh_client_receives_nothing_before_it_starts_anything` (silence via `try_recv` 1500 ms), `a_child_killed_by_sigkill_is_reported_and_can_be_started_again` (portable pass-through claim: `worker-exited` and `worker-crashed` carry the SAME code; unix asserts `Null`).
- Harness details a shared-fixture change must not break: `worker_fixture()` (isolated `--target-dir` fallback for `fake_worker_core`), `Daemon::start` (defaults: fresh socket, `--db`, explicit `--worker-exe`, `.stderr(Stdio::inherit())`), `start_env`/`start_frame` (verbatim `StartEnv` + `type:"start"`), `raw_dial` (second, bypassing-the-framer dial path with its own `PIPE_BUSY_BUDGET`).

**Deadline rules:** `FRAME_BUDGET = 30s` (serve tests; must cover worker spawn + 5 s ready), `ROW_BUDGET = 20s`, `PIPE_BUSY_BUDGET = 2s` (Windows).

### 1.6 Security facts T3 needs

- **Already landed (do not redo):** `fulcrum.db` is `0o600` at creation via `make_private` (`db/mod.rs:156`; the `#[cfg(unix)]` copy at 156, the `#[cfg(not(unix))]` no-op stub at 253) with **narrow-an-existing-file** semantics; dirs are `0o700` via `crate::db::ensure_private_dir` (`db/mod.rs:220`, `0o700` at 240); the socket is `0o600`-mode at bind time BEFORE the first `accept` (`unix.rs::bind_private`); sockets' parent dir `0o700`; Windows pipe: `reject_remote_clients(true)` + the one-instance invariant as the only access control, NULL-security-descriptor DACL documented as a T13-standing gap.
- The daemon binds under data-dir permissions; the *unix* socket is bound at 0600, so the honest residual is "the socket is the credential until T13" and **peer-credential verification would make it a second, defense-in-depth check**.

### 1.7 Testable observability facts for a T4 split

- The engine's `WorkerManager` has no observability syscall interface; a session's stream ends with an `agent_settled` event (fixture's `__long_stream__`), reachable through the same code paths the durability test uses, so "events still stream" is meaningfully assertable by the existing helpers.
- The **known-limitation regress test** (the one T4 must make assertable/"still passes"): there is **no test today** named e.g. `a_slow_start_does_not_stall_that_clients_relay`; the "known limitation" regions (module header `:94-125` and `serve_client`'s doc `~:1160-1180`) currently *document* the stall rather than pin it. A T4 plan will want a NEW integration test alongside, not just preservation.

---

## 2. Constraints a planner must not violate

1. **`transport/frame.rs` is the file whose whole purpose is preventing this class of desync** — every one of its doc comments and its reader-task design exists because a JSONL stream that two writers interleave cannot resynchronise. T4 must NOT breach it by adding a `Mutex<Connection>` or an unowned write handle; the designated shape is a `tokio::io::split`-style owning-reader/owning-writer split with `pending` moving to the writer half (module header `:94-125`).
2. **The `gap` notice must stay tested with its numbers intact.** `gap_frame(skipped)` shape (`type:"gap"`, top-level `skipped: u64`, NO `sessionId`), `FAN_OUT_CAPACITY = 4096 > CLIENT_QUEUE_CAPACITY = 1024 > 0` (compile-time-asserted), the two compile-time asserts themselves, `watched`'s absent-`sessionId`-passes rule, and the two emission paths (Lagged arm; drain-arm prelude) are the machine the throttle tests stand on. A T4 split that moves `send_frame` to a writer task changes *which arm announces drops* but must not alter the frame or the "drop, count, announce, keep the connection" semantics; `enqueue`'s `Full ⇒ keep` / `Closed ⇒ end` contract is pinned by unit tests.
3. **The daemon has NO shutdown path today**: `serve::run` never returns, `WorkerManager::reap_all` is unwired, `main.rs::run_serve` dies by process kill, and the child exits on its stdin EOF. T4's split must not create a task that outlives `serve_client`'s returns in a way that needs a join/abort the daemon does not have (frame.rs's dedicated reader task is aborted via `Drop` — the pattern to copy).
4. **`windows`/`windows-sys` absent on purpose.** `fulcrum/Cargo.toml`'s manifest comment says exactly that, and `transport/windows.rs`'s header repeats it; the member's precedent is raw Win32 constants (`is_pipe_busy` = 231). If T3's `GetNamedPipeClientProcessId` "needs a crate", the issue's own needs and the manifest's hard constraint must be reconciled — the constraint wins unless the plan first changes the manifest (and T9's frozen-manifest acceptance criteria say tasks are forbidden from editing it). **The unmodified alternative is raw `GetNamedPipeClientProcessId` via `#[cfg(windows)]` FFI with `unsafe extern "system"` declarations, no crate — which adds its own soundness requirements.**
5. **`#[cfg(unix)]` vs Linux-only gating matters.** CI compiles clippy with `-D warnings` on ubuntu, windows and macos, plus `cargo check --target x86_64-pc-windows-gnu` locally; an import used only under `#[cfg(unix)]` is an unused-import failure on Windows. Platform gates must be `mod`/item-level `#[cfg]`, never `if cfg!(…)` (documented as caught-twice in transport/mod.rs's header). `SO_PEERCRED` is Linux (and the `libc::sockaddr_un` size differs macOS/BSD — same file's `sun_path_capacity` notes 108 vs 104); `getpeereid` covers the BSDs; the plan must state what each platform proves. The tests' own precedent: `daemon_rss` is `#[cfg(target_os = "linux")]` with honest zero elsewhere.
6. **The probe/EOF connections are load-bearing.** `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon`, the stale-socket reclaim's probe side effect, and `wait_until_listening` all create connect-then-EOF peers. T3's handshake must let these connections fail gracefully WITHOUT logging an error or taking the daemon down; the `seen == 0` no-log rule in `serve_client`'s epilogue is the current articulation of that.
7. **T4 depends on T3's post-change shape of `serve_client`.** `serve_client`'s signature (`mut connection: Connection`, the `select!` arms, the `watching` set, `dropped`, `enqueue`, `gap_frame`) is the surface T3's handshake/per-frame-rate controls are drawn on, and T4 splits that same function. Because T3 edits the accept path (`run`, `Listener::accept` return type / a peer-identity type) and T4 edits the per-client loop, the two must land in order (T3's wave before T4's) or on disjoint regions with the plan named at the fn-name level — the wave split exists for this reason.
8. **`cargo fmt --all -- --check` runs in CI** — any new signature/struct must be rustfmt-clean. Also `clippy::doc_lazy_continuation` bites (documented in serve.rs's `close` doc).
9. **Do not widen the crate's public API for test convenience** (`raw_dial` comment, `is_pipe_busy` re-hardcode comment: the transport's private fn "cannot be named by a test target, and the task forbids widening a `src/` API for a test's convenience"). A peer-identity type T3 exposes would be the FIRST genuinely new public item on `transport`.
10. **`fulcrum/Cargo.toml` is effectively frozen** for these tasks (T9's criteria: Tasks 3–5 were "forbidden from editing this file"; `libc` is already declared for unix; `windows`/`windows-sys` absent on purpose; `dirs` and `tokio time` are the only new-ish items and are present).

---

## 3. Will touch

**Shared-fixture note:** BOTH tasks edit tests that spawn the real binary through the same harness (`fulcrum/tests/serve.rs`'s `Daemon`/`Client`/`start_env`/`start_frame`/`raw_dial`, and `fulcrum/tests/transport.rs`'s `bind`/`accept_one`/`dial`/`recv_*`). If a task changes the daemon's greeting semantics (T3), existing tests that wait for a `type == "hello"` reply after sending `hello` must still pass; the `Client::recv_until` helpers require an actual reply frame.

### T3 (issue #4) — peer credential + limits + required handshake

- **Modify `fulcrum/src/transport/unix.rs`** — a peer-credential check at accept time: `Listener::accept` is where the accepted `UnixStream` exists before `Connection::new`; it currently discards `_addr`. (Optionally a new `#[cfg(unix)]` helper fn; the fn is currently `fn`-private — signature changes ripple to callers listed below.)
- **Modify `fulcrum/src/transport/windows.rs`** — the same hook at `Listener::accept` (via the `NamedPipeServer` instance at hand before `Connection::new(server)`), with the platform's documented limits and, if the issue's `GetNamedPipeClientProcessId` is used, a hand-rolled FFI (no crate) — see Constraints 4.
- **Modify `fulcrum/src/transport/frame.rs`** — only if T3 needs to carry peer identity inside `Connection` (a new field or a peer-identity accessor); `Connection::new`'s signature is the seam. If T3 keeps identity out of `Connection`, this file is untouched.
- **Modify `fulcrum/src/serve.rs`** — REGION 1 (accept path): `run`'s loop (the `Ok(connection)` arm — where the check/handshake-before-spawn decision lands, and where the per-connection task's `Arc<Daemon>` access for session limits is already available), plus `serve_client`'s **header region** (before the `select!`): the required-`hello` state, the per-connection frame-rate state, and the session-limit enforcement point (which owns `claim_session`; `Daemon::claim_session`'s `Result<SessionClaim, String>` is the natural seam for a max-session check, best via a counter field on `Daemon` — see §1.3). This is the file's `serve_client` starter region, which T4 will also touch; the wave order resolves the collision.
- **Modify `fulcrum/src/main.rs`** — only if a startup-time counter is passed into `run`; `run(listener, db, factory)` is currently called with three args and no config.
- **Modify `fulcrum/Cargo.toml`** — PREFERRED NOT AT ALL (frozen; Constraint 4).
- **New tests in `fulcrum/tests/transport.rs`** (unix: peer-credential loop variant connecting as the same uid; Windows: compile-level or documented-equivalent) — plus the existing unix_specific module's bind-serialization discipline.
- **New tests in `fulcrum/tests/serve.rs`** — handshake-rejection cases (frames before `hello`), rate-limit-throttle case (same "never reads" flood shape), a max-session case (a second socket's `start` refused once the bound is met), and a probe-connection case proving the probe/EOF shape still does not disturb the daemon.

### T4 (issue #3) — split `Connection` and `serve_client`

- **Modify `fulcrum/src/transport/frame.rs`** — the split itself: `Connection` becomes owning-reader + owning-writer; `pending` moves to the writer half; `Connection::send_frame(&mut self)` (and `Connection::recv(&mut self)`) become the two halves' methods; callers' signatures change. **This is the file whose whole purpose is the desync class; the split must keep the reader-task rule and the `MAX_*` bounds.**
- **Modify `fulcrum/src/serve.rs`** — REGION 2 (`serve_client` and its helpers `enqueue`, `gap_frame`, `watched`, `handle_frame`'s `start` arm): the reader/reply task and writer task replace the single `select!`; `handle_frame(…).await` moves out of the inbound arm so `attach`'s ~5 s no longer blocks the relay. `gap_frame`/`enqueue`/`dropped`'s accounting must move intact. **Region 2 collides with T3's Region 1 only at `serve_client`'s signature border — the plan must state which region each wave owns.**
- **Modify `fulcrum/src/transport/mod.rs`** — re-exports if the halves are public (they live beside `frame::Connection` today); `pub use frame::Connection;` and the `Listener::accept -> Result<Connection, …>` return type are the two ripple points.
- **Modify `fulcrum/src/transport/unix.rs` / `windows.rs`** — only the `Connection::new(stream)` construction sites, if `new`'s signature changed or splits.
- **Modify `fulcrum/tests/transport.rs`** — current tests call `conn.send_frame(&…).await` / `conn.recv()` on one `Connection`; if the split changes the handle shape, the harness helpers (`recv_one`, `accept_one_and_echo`, `dial`) and the two raw-byte probes follow.
- **Modify `fulcrum/tests/serve.rs`** — the `Client` helper uses `conn.send_frame` and `conn.recv`; plus a NEW integration test asserting "a slow `start` (or a stalled relay) no longer stalls that client's own event relay" — this claim is currently documented but unpinned (see §1.7).
- **Shared fixture note:** `tests/serve.rs`'s `Daemon`/`Client`/`start_env`/`start_frame`/`raw_dial` and `tests/transport.rs`'s `bind`/`accept_one`/`dial`/`recv_*` helpers are the shared fixtures both tasks' test changes ride on.

---

## 4. Provides

Existing symbols with verbatim signatures (the seam both tasks build on):

**Transport (`fulcrum/src/transport/…`):**

```rust
// frame.rs
pub struct Connection { /* private */ }
impl Connection {
    pub(crate) fn new<S>(stream: S) -> Self
        where S: AsyncRead + AsyncWrite + Unpin + Send + 'static;
    pub async fn send_frame<T>(&mut self, frame: &T) -> Result<(), TransportError>
        where T: Serialize + ?Sized;
    pub async fn recv(&mut self) -> Option<Result<serde_json::Value, TransportError>>;
}
impl Drop for Connection { /* aborts self.reader */ }
impl std::fmt::Debug for Connection { /* manual */ }
pub const MAX_INBOUND_LINE_BYTES: usize = 1024 * 1024;                    // pub
const MAX_QUEUED_INBOUND_FRAMES: usize = 1024;                           // private

// mod.rs — re-exported platform-neutral surface
pub enum Endpoint { Socket(std::path::PathBuf), Pipe(String) }
impl Endpoint { pub fn socket(path: impl Into<std::path::PathBuf>) -> Self;
                pub fn pipe(name: impl Into<String>) -> Self; }
pub enum TransportError { Io(#[from] std::io::Error), Encode(String), Decode(String),
    LineTooLong { limit: usize }, SocketPathTooLong { length: usize, limit: usize, path: String },
    WrongPlatform { family: &'static str, platform: &'static str, endpoint: Endpoint }, NoDataDir }

// unix.rs  (Config item; note: missing `#[cfg(unix)]` on the sub-fn — the file itself is cfg-gated)
pub fn default_endpoint() -> Result<Endpoint, TransportError>;
pub struct Listener { /* private */ }
impl Listener {
    pub fn bind(endpoint: Endpoint) -> Result<Self, TransportError>;
    pub fn endpoint(&self) -> Endpoint;
    pub async fn accept(&self) -> Result<Connection, TransportError>;   // ← T3 hook point
}
pub async fn connect(endpoint: Endpoint) -> Result<Connection, TransportError>;
// private helpers: bind_socket, bind_private (umask+0600), ensure_private_dir → crate::db::ensure_private_dir,
// reclaim_stale_socket (blocking probe), sun_path_capacity, check_socket_path_len, frame::wrong_platform

// windows.rs
pub fn default_endpoint() -> Result<Endpoint, TransportError>;
pub struct Listener { /* name: String, armed: StdMutex<Option<NamedPipeServer>> */ }
impl Listener {
    pub fn bind(endpoint: Endpoint) -> Result<Self, TransportError>;
    pub fn endpoint(&self) -> Endpoint;
    pub async fn accept(&self) -> Result<Connection, TransportError>;   // ← T3 hook point (on the NamedPipeServer)
}
pub async fn connect(endpoint: Endpoint) -> Result<Connection, TransportError>;
const DEFAULT_PIPE_NAME: &str = r"\\.\pipe\fulcrum";
// private: create_pipe, bind_with_retry, BIND_BUSY_BUDGET (2s), is_pipe_busy_err, is_pipe_busy (raw 231)
```

**serve.rs:**

```rust
pub async fn run(listener: Listener, db: Arc<Db>, factory: Arc<dyn WorkerFactory>);  // never returns
async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection);              // one select! over recv/fan-out/queued
async fn handle_frame(daemon: &Arc<Daemon>, watching: &mut HashSet<String>, frame: &serde_json::Value)
    -> Option<serde_json::Value>;                                                    // hello/start/prompt/close + error
async fn start(daemon: &Arc<Daemon>, watching: &mut HashSet<String>, frame: &serde_json::Value)
    -> serde_json::Value;                                                            // claim_session + manager.attach
fn start_env(frame: &serde_json::Value) -> Result<StartEnv, String>;                 // strips "type" only
fn gap_frame(skipped: u64) -> serde_json::Value;                                     // {"type":"gap","skipped":N}, no sessionId
fn enqueue(tx: &tokio::sync::mpsc::Sender<serde_json::Value>, frame: serde_json::Value,
           dropped: &mut u64) -> bool;                                               // Full⇒count+keep; Closed⇒false
fn watched(frame: &serde_json::Value, watching: &HashSet<String>) -> bool;           // top-level sessionId ∈ watching
const FAN_OUT_CAPACITY: usize = 4096;
const CLIENT_QUEUE_CAPACITY: usize = 1024;                                           // + two const _: () = assert!(...)
struct Daemon { manager: Arc<WorkerManager>, persister: Arc<TranscriptPersister>, db: Arc<Db>,
                fan_out: tokio::sync::broadcast::Sender<serde_json::Value>,
                subagents: Arc<StdMutex<HashMap<String, SubagentRecord>>>,
                starting: Arc<StdMutex<HashSet<String>>> }
impl Daemon { fn new(db: Arc<Db>, factory: Arc<dyn WorkerFactory>) -> Arc<Self>;
              fn claim_session(&self, session_id: &str) -> Result<SessionClaim, String>;
              fn prune_subagents(&self); fn record_session(&self, env: &StartEnv) -> Result<(), String>; }
struct SessionClaim { /* Drop releases the starting-set id */ }
fn crash_report_frame(subagents: &StdMutex<HashMap<String, SubagentRecord>>, session_id: &str,
                      code: Option<i32>, pid: Option<u32>) -> serde_json::Value;
fn event_frame(session_id: &str, is_subagent: bool, evt: &WorkerInboundEvent) -> serde_json::Value;
fn worker_frame(evt: &WorkerInboundEvent) -> serde_json::Value;
fn store_frame(frame: &StoreFrame) -> serde_json::Value;
fn persist_frame(persist: &Arc<TranscriptPersister>, session_id: &str, is_subagent: bool, evt: &WorkerInboundEvent);
fn log_on_join_error(handle: tokio::task::JoinHandle<()>);
fn relay(handle: Option<WorkerHandle>, f: impl FnOnce(&WorkerHandle) -> Result<(), WorkerError>) -> Result<(), String>;
fn relay_result(kind: &str, session_id: &str, result: Result<(), String>) -> serde_json::Value;
// CLI surface: pub enum Command { Worker, Serve(ServeConfig), Help, Usage(String) },
// pub struct ServeConfig { pub endpoint: Endpoint, pub db_path: PathBuf, pub worker_exe: Option<PathBuf> },
// pub fn parse_command(argv: &[String]) -> Command, pub fn defaults(), pub fn default_db_path(),
// pub fn usage() -> &'static str, pub fn open_db(config) -> Result<Db, ServeError>,
// pub enum ServeError { Endpoint(.), NoDataDir, Db(.), Bind(.), Runtime(.), WorkerExe(WorkerExeError) }
```

**main.rs accept-path glue (verbatim):** `runtime.block_on(serve::run(listener, db, factory));` where `let listener = fulcrum::transport::Listener::bind(config.endpoint.clone()).map_err(|e| ServeError::Bind(e.to_string()))?;` and `let db = std::sync::Arc::new(serve::open_db(&config)?);` — the db opens and the factory resolves BEFORE the bind, on purpose (the no-socket-on-failure invariant tests pin).

**Engine side (consumed, not owned):**

```rust
// fulcrum-core/src/worker/manager/mod.rs
pub async fn attach(&self, session_id: &str, env: &StartEnv) -> Result<(), WorkerError>;  // the ~5 s `ready` bound
// fulcrum-core/src/worker/protocol.rs: PROTOCOL_VERSION, StartMode, Inbound::Start(Box<StartEnv>)
// fulcrum-core/src/worker_types.rs:
pub struct StartEnv { /* 21 fields, verbatim in §1.4 */ }
impl StartEnv { #[allow(clippy::too_many_arguments)] pub fn from_parts(/* 13 args; inclusive of permission: Option<PermissionBlock> */) -> Self }
// fulcrum-core/src/definition_permission.rs:
pub struct PermissionBlock { pub tools: Vec<(ToolKey, Rules)> }
```

---

## 5. Needs from other areas

| Need | Producer / file | Notes |
| --- | --- | --- |
| `WorkerManager::attach` (the 5 s `ready` bound) — consumed inline by `start` today; T4 must stop awaiting it inside the relay's task | engine, `fulcrum-core/src/worker/manager/mod.rs` | unchanged API; T4 only changes WHO awaits it |
| The `~5 s` bound NUMBER lives in the engine's `attach` doc, not re-chosen in the daemon | engine (doc) | T4's "slow start no longer stalls the relay" test exploits this bound's existence, not its value |
| `WorkerManager::reap_all` — UNWIRED today | engine fn; wired by T6's pins / T12's sidecar lifecycle | a T4 split must not create a need to call it (no shutdown path, Constraint 3) |
| `StartEnv`/`PermissionBlock` — the daemon never reads `permission`; it rides verbatim to the child | `fulcrum-core/src/worker_types.rs`, `definition_permission.rs` | T3's handshake must keep `permission` untouched in the pass-through |
| Peer identity on unix (`SO_PEERCRED`/`getpeereid`) — nothing exists; `libc` already declared for unix | `libc` crate (`[target.'cfg(unix)'.dependencies]`) | T3 adds the only consumer under `#[cfg(unix)]` |
| Peer identity on Windows (`GetNamedPipeClientProcessId`) — nothing exists; NO `windows`/`windows-sys` crate declared and the manifest forbids adding one | `fulcrum/Cargo.toml` (hard constraint) | T3 must either hand-roll the FFI or scope the platform claim accordingly (Constraint 4) |
| What T9 already landed so T3 does not redo it: `db` 0600 at creation (`db/mod.rs:156`), dirs 0700 (`db/mod.rs:220`/`240`), socket 0600-before-first-`accept` (`unix.rs::bind_private`), pipe `reject_remote_clients(true)` + one-instance invariant (`windows.rs::create_pipe`) | T9 (this tree) | T3 adds peer *identity*, not file modes |
| The `gap` notice contract and its two emission paths (and their numbers) | T9, `serve.rs` (this tree) | T4 must move the machinery intact — the numbers are pinned (Constraints 2) |
| Keepsafe rule for the daemon's prompts / the durability pin: `a_client_that_vanishes_mid_turn…`, `REAP_GRACE`, `two_sessions_on_two_sockets…`, `a_probe_connection…`, `a_client_that_never_reads_is_throttled…` | T9, `fulcrum/tests/serve.rs` | every one of these is the definition of "still passes" after T4's split |
| The raw-byte "bad peer" test pattern (split + `write_all`) | T9, `fulcrum/tests/transport.rs` / `fulcrum/tests/serve.rs` (`raw_dial`) | the honest way to test framing tolerance; also the shape T4's concurrent-writer tests would need |
| **T4 depends on T3's post-change shape of `serve_client` — specifically:** the accept path that hands each connection to a task (`run`'s `Ok(connection)` arm), the `handle_frame`/`start` inline-await that T3's handshake pre-gates, and the session-limit ownership of `claim_session`; T3 edits these regions first, so T4 splits the POST-T3 `serve_client` rather than a shape that is about to move | T3 (same area, earlier wave) | the wave ordering is mandatory; both tasks' plans must name `serve_client` at the fn/region level so the later one rebases on the earlier one's output |
| A NEW regress test for the T4 claim itself — "a slow `attach` does not stall that client's relay" — does not exist today | to be created by T4 (no producer) | the known-limitation docs (`serve.rs:94-125`) are the spec for it |

---

## 6. Sources

- `fulcrum/src/transport/frame.rs` (571 lines) — `Connection`, `pending` (`:123-125`, `:240`, `:265-279`), `send_frame` (`:251-282`), reader-task rationale (`:161-210`), `MAX_QUEUED_INBOUND_FRAMES` (`:43`), `MAX_INBOUND_LINE_BYTES` (`:95`), unit tests (`:381-571`).
- `fulcrum/src/transport/mod.rs` (181 lines) — gating rule (`:15-28`), re-exports, `Endpoint`, `TransportError`.
- `fulcrum/src/transport/unix.rs` (287 lines) — `default_endpoint`, `Listener::bind`, **`Listener::accept` (`:158-169`)**, `bind_private`, `reclaim_stale_socket`, `connect`.
- `fulcrum/src/transport/windows.rs` (297 lines) — access-control disclaimer (`:32-52`), the one-instance invariant, **`Listener::accept`**, `create_pipe`, `connect`, `is_pipe_busy`.
- `fulcrum/src/serve.rs` (2524 lines) — module header incl. the T4 known limitation (`:94-125`) and "no shutdown path" (`:70-92`); `FAN_OUT_CAPACITY` (`:139`), `CLIENT_QUEUE_CAPACITY` (`:249`) + the const asserts; `run` (`:831-858`), `serve_client` (`:1160-1260` incl. the "KNOWN LIMITATION" doc `~:1160-1180` and the send_frame sites `:1235`/`:1241`), `gap_frame`/`enqueue`/`watched` (`:1267-1340`), `start`/`start_env`/`start_result` (`:1408-1500`), `close` (`:1580-1615`), unit tests incl. `a_full_client_queue_drops_counts_and_keeps_the_connection` and `the_gap_notice_carries_its_count_where_the_client_reads_it` (`:2212-2524`).
- `fulcrum/src/main.rs` — `run_serve` ordering (db before bind), `runtime.block_on(serve::run(..))` (`:140`).
- `fulcrum/src/db/mod.rs` — `make_private` (`:156`, `0o600` at `:175`/`:178`; `#[cfg(not(unix))]` stub `:253`), `ensure_private_dir` (`:220`, `0o700` at `:240`).
- `fulcrum/src/worker_factory.rs` — `DaemonWorkerFactory` (the `Arc<dyn WorkerFactory>` argument to `run`/`Daemon::new`).
- `fulcrum/Cargo.toml` — tokio features, `[target.'cfg(unix)'.dependencies] libc`, the "windows-sys deliberately absent" comment, dev-deps (`tempfile`, tokio `test-util`), no `[profile.*]`.
- `src/worker_types.rs` — `StartEnv` (verbatim: fields at `:53-260`), `from_parts` (`:280`), `permission` field doc ("`None` → starts from 'no narrowing'").
- `src/definition_permission.rs` — `PermissionBlock` (`:228`: `pub tools: Vec<(ToolKey, Rules)>`).
- `src/worker/manager/mod.rs` — `pub async fn attach(&self, session_id: &str, env: &StartEnv) -> Result<(), WorkerError>`.
- `fulcrum/tests/transport.rs` (1139 lines) — full fn inventory in §1.5 (`BIND_SERIALIZER`, `every_bind_in_this_file_goes_through_the_serialiser`, `unix_specific` module).
- `fulcrum/tests/serve.rs` (2411 lines) — full fn inventory in §1.5 (`REAP_GRACE` at `:909` region, `FRAME_BUDGET`/`ROW_BUDGET`/`PIPE_BUSY_BUDGET`, `Daemon`/`Client` harness, `worker_fixture`, `raw_dial`).
- `fulcrum/tests/worker_mode.rs`, `fulcrum/tests/db.rs`, `fulcrum/tests/boundary.rs`, `fulcrum/tests/proctable/mod.rs` — adjacent coverage (CLI parsing, db modes incl. `the_database_file_is_owner_readable_only`, daemon boundary pins, process-table parser).
- `.github/workflows/ci.yml` — 3-OS matrix, `clippy --all-targets -- -D warnings`, no build step, allocator guard.
- `AGENTS.md` — validation bar (`cargo check --target x86_64-pc-windows-gnu --all-targets`), fixture-bins contract, `default-members` gate, the "boundary_pins does not see the sibling" gap.
- Git history: `e44f382` (the archived T9 plan — wave structure, frozen-manifest rule, lib+bin rationale) and `38a9986`/`958306d` (the shipped tree).
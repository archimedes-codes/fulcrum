---
status: committed
parent: docs/roadmap/daemon-hardening-status.md
done-when: >-
  `fulcrum serve`'s local socket/pipe accepts a peer only under the platform's
  peer-identity proof, the wire protocol requires a `hello` exchange before any
  other frame is served, concurrent sessions and per-connection inbound frame
  rate are bounded with the bounds named, and the existing `gap`-notice,
  queue-bound and probe/EOF tests still pass with their numbers intact.
---

# daemon-hardening — T3: peer-credential check + rate/session limits + required `hello` handshake (issue #4)

**Goal:** Harden the UNAUTHENTICATED local listener of `fulcrum serve` (unix UDS under the data dir; Windows named pipe). Wire auth is legitimately deferred to T13 — this is the local-level defense that must exist regardless: verify the connecting peer's identity where the platform can prove it, bound how many sessions and how much inbound traffic one buggy/hostile local client can consume, and require a handshake so a stray connection is not implicitly trusted.

**Architecture:** The credential is resolved at **accept time inside `Listener::accept`** on each platform (the accepted `UnixStream` / `NamedPipeServer` is in scope there, before `Connection::new`), and enforced in **`serve::run`'s accept arm** — peer identity is deliberately NOT carried inside `Connection` (constraint: `transport/frame.rs` is T4's file this wave; `Connection::new`'s signature is the seam T4 splits). The handshake and the per-connection rate limit are per-connection state in the header region of `serve_client` (before its `select!`), a shape that moves intact into T4's reader half. The session bound is a counter field on `Daemon`, enforced at the `claim_session` seam.

**Tech Stack:** Tokio (`UnixListener`/`NamedPipeServer`), `libc` (unix peer identity — already a declared `[target.'cfg(unix)'.dependencies]`), serde_json frames, the existing `rusqlite`/`dirs` daemon stack.

**Non-goals (from the issue):** `wss://` and bearer tokens (T13); encrypting transcripts at rest — **recorded as an OPEN QUESTION in `serve.rs`'s module header** (see Step 6), not a task.

**Pre-existing tree state (READ FIRST):** this plan was written against the committed tree `a3f76c0`. At plan time the working tree is NOT clean: `fulcrum/src/transport/frame.rs` has an uncommitted local edit (the `Connection` struct's `writer` and `pending` field declarations removed — an in-flight state of T4's split, not T3's) and `docs/roadmap/daemon-hardening-task02-schema-drift-fingerprint.md` is untracked (another task's plan, not T3's). Neither is in T3's owned file set; **do not touch, revert, or commit either.** Because of that external edit, `cargo check` on the primary tree currently FAILS (E0609/E0560 in `frame.rs`) — that failure is NOT T3's and will be cleared when the external editor resolves its state (or when the run's gate re-bases). T3's test steps below therefore target T3's **owned test files only** (`-p fulcrum --test serve --test transport`), exactly as the wave protocol requires; do not run a full-workspace check and attribute its failures to T3.

---

## Parallelisation

**Verdict:** SERIAL — T3 is a single task that edits five of its six owned files as one change (the credential check, the handshake gate, the rate limit and the session bound are one connected hardening surface, split into the task's sequential steps below). It has nothing to parallelise with: wave 1 is `T1 ∥ T2 ∥ T3` (disjoint owned sets, a wave grouping the roadmap owns), wave 2 is `T4` (serial, depends on T3's post-change `serve_client` shape). Emit no `**Parallel:**` line — one task cannot be a wave.

---

### Task T3

**Context:**
`fulcrum serve` binds a local socket with NO authentication: any local process that can reach it is a client and can issue `start`. T9 already closed "another user reads my DB" (`fulcrum.db` 0o600 via `make_private` at `db/mod.rs:156`; dirs 0o700; the socket 0o600 before the first `accept` via `bind_private` at `transport/unix.rs:169`). It does NOT close "another process as my uid connects to my socket" — same-uid peers pass any file-mode check trivially. T3 adds three defense layers on the local listener:

1. **Peer-credential check** (unix: `SO_PEERCRED`/`getpeereid`; Windows named-pipe: what the platform can and cannot prove, using the blessed documented-limitation route).
2. **Rate and session limits** (bound the inbound: concurrent sessions globally, per-connection frame rate; the outbound side is already bounded by `CLIENT_QUEUE_CAPACITY`).
3. **A required `hello` handshake** before any other frame is served.

**Critical protocol fact (verified in this tree): a `hello` frame type ALREADY EXISTS.** `handle_frame` (`serve.rs:1397`) has a `"hello"` arm that replies `{"type":"hello","protocolVersion":PROTOCOL_VERSION,"serverVersion":CARGO_PKG_VERSION}`, and every integration test already sends `{"type":"hello"}` before `start` (e.g. `tests/serve.rs:1318`). So this task is in the "make an existing exchange MANDATORY and enforced" case, NOT the "invent a second handshake" case. **Do not add a second/overlapping handshake frame; do not rename or reshape the `hello` reply.** The wire already speaks `hello`; the change is that anything sent before `hello` — and anything sent after more than `MAX_PRE_HELLO_FRAMES` non-`hello` frames without a successful `hello` — is rejected, and `hello` itself is not treated as a grant of authority (it stays a pure greeting; session/rate gates remain).

**What must NOT change (hard pins):**
- `transport/frame.rs` is OUT of T3's owned set. Peer identity must NOT ride inside `Connection`; `Connection::new`'s signature is UNCHANGED; do not touch that file.
- `StartEnv.permission` (engine `worker_types.rs`, `Option<PermissionBlock>` field) is OPAQUE WIRE BYTES: nothing in the daemon reads it, `start_env` strips only `type`, and it rides verbatim to the child whose gate is the only consumer. The handshake, the rate limit, and the session bound must NOT rewrite, pre-check, or drop `permission`.
- The `gap`-notice path and its numbers: `gap_frame(skipped)` shape (`{"type":"gap","skipped":N}`, no `sessionId`), `FAN_OUT_CAPACITY = 4096 > CLIENT_QUEUE_CAPACITY = 1024`, both compile-time const asserts, `enqueue`'s `Full ⇒ count+keep` / `Closed ⇒ end`, `watched`'s absent-sessionId-passes rule. Untouched.
- Keep the existing `a_client_that_never_reads_does_not_stall_the_daemon`, `a_client_that_vanishes_mid_turn…`, `repeated_disconnects_do_not_degrade_the_daemon` etc. passing — require that the test suite observes no dangling `fork()` (the fixture worker children exit via the 2 s reap grace); the audit below resolves this.
- The inbound byte/line/queue bounds in `transport/frame.rs` are the transport's; do not add a second inbound queue. The rate limit is distinct: it bounds the RATE at which decoded frames are pulled out of that queue.
- Do not add a reconnect/retry or a backoff in the transport (the peer must not learn "wait 300ms" from an error frame); one `error` frame per offending ratio, then the connection is closed.
- `fulcrum/Cargo.toml` is frozen. `libc` is already declared for unix; do NOT add `windows`/`windows-sys` (manifest comment: "deliberately ABSENT and must not be added"). The Windows route is a documented limitation, NOT raw FFI — see Step 3 and the Windows Capabilities table.
- The daemon has no shutdown path: `serve::run` never returns, `reap_all` unwired; `eprintln!` is the operator console; do not introduce a shutdown or a join/abort that needs one.
- Do not add a second outbound path: `send_frame` stays the ONLY writer of a `Connection` (a second writer is the desync hazard `transport::frame` documents); the handshake rejection and the rate `error` are sent through the existing single `send_frame` owner before `break`.

**Platform gating rule (CI traps):** CI is `[ubuntu-latest, windows-latest, macos-latest]`, runs `cargo clippy --all-targets -- -D warnings` on all three plus `cargo fmt --all -- --check`. An import used only inside `#[cfg(unix)]` is an UNUSED-IMPORT error on Windows (`-D warnings`). Gate every new item at the item/module level with `#[cfg(unix)]` / `#[cfg(windows)]` / `#[cfg(target_os = "linux")]` — NEVER `if cfg!(...)` (the transport module header documents being caught twice). Anything imported solely for a unix-only or linux-only item must itself be `#[cfg(...)]`-imported.

**Windows/tests honesty rule:** `transport/windows.rs` is only ever **type-checked cross-target and never executed locally** (dev machine is Linux; AGENTS.md requires `cargo check --target x86_64-pc-windows-gnu --all-targets`). A Windows test is therefore a **compile gate, not proof**. Do not write a Windows test that claims runtime behavior it cannot prove; the Windows proof is the documented-limitation statement plus the compile gate.

**Windows Capabilities (documented proof, verbatim in `windows.rs` — already partially stated in its header, extend it):**
- What Windows **cannot prove** without a crate: `GetNamedPipeClientProcessId` is export-only via `Kernel32` and would require hand-rolled `#[cfg(windows)]` FFI (no crate is allowed). Raw FFI is rejected here because: (a) the manifest and module header explicitly forbid the crate and the house pattern is raw constants (`is_pipe_busy` = 231), not FFI; (b) FFI would add link-time soundness obligations (`extern "system"`, `GetLastError`, handle lifetimes) that cannot be executed/tested on this Linux dev machine — an untestable unsafe surface is worse than a documented gap; (c) the practical residual is already small: the pipe is created with `reject_remote_clients(true)` and the one-instance invariant, so only LOCAL processes can dial at all, and a local process with your logon token is the same-uid case no credential check ever closes. `GetNamedPipeClientProcessId` can at most prove the client's PID (not integrity, not token, not a secret), and the PID is forgeable by any same-user process. So the honest Windows statement is: **"the named pipe cannot prove the caller's identity beyond 'a local process that passed the pipe's DACL'; the DACL is the creating token's default (NULL `lpSecurityAttributes`); same-session peers are indistinguishable; T13's protocol-layer auth is the only real mitigation on Windows."** (This is the "documented limitation" the issue's own wording permits: "a documented statement of what the platform cannot prove.")
- What Windows **can prove** (and does today): the invariant that the caller is LOCAL (`reject_remote_clients(true)`), which unix gets from the socket family itself.

**Unix Capabilities (documented proof, verbatim in `unix.rs`):**

| Platform | Mechanism | Proves | Doesn't prove | Notes |
|---|---|---|---|---|
| Linux (glibc/musl) | `SO_PEERCRED` via `libc::getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &mut libc::ucred)` | the peer's real uid/gid/pid at connect time, as assigned by the kernel (unforgeable by the peer) | nothing about the peer's token beyond uid; a same-uid process (any process under your account) passes | the accepted stream's send/recv buffers are limited (`SO_SNDBUF`/`SO_RCVBUF` default) so PID reuse is not a practical spoof vector; use `std::os::fd::AsRawFd` |
| macOS (and other BSDs) | `getpeereid(fd, &mut euid, &mut egid)` — **the ONLY documented call; `SO_PEERCRED` does not exist here** (macOS defines `LOCAL_PEERCRED` on a socket option for `LOCAL_PEERCRED`, NOT a SOL_SOCKET `SO_PEERCRED`; `libc` has no `SO_PEERCRED` for apple) | peer euid/egid | pid | `LOCAL_PEERCRED` (the `struct xucred`) IS the alternative on macOS and **DOES** return the pid — see note below; freebsd/dragonfly/netbsd use `getpeereid` |
| FreeBSD/NetBSD/DragonFly/OpenBSD | `getpeereid` | euid/egid | pid | `libc::getpeereid` exists under `unix/bsd/mod.rs:627` for all BSDs; OpenBSD also defines `SO_PEERCRED` (unix cred), but the portable call is `getpeereid` |

- **macOS refinement (verified in `libc` 0.2.191, `unix/bsd/apple/mod.rs:3170`):** `LOCAL_PEERCRED: c_int = 0x001` exists and `getsockopt(fd, LOCAL_PEERCRED, LOCAL_PEERCRED, &mut libc::xucred)` returns `xucred` with `cr_pid` (the CONNECTING pid, unlike `getpeereid`). However, `xucred`'s version field semantics on macOS are fiddly, the struct layout is platform-specific, and the daemon's requirement is uid-identity, not pid — so the portability decision for this task is: use `getpeereid` on ALL BSDs (including macOS), which is the single uniform `libc::getpeereid` call. Document the macOS `LOCAL_PEERCRED`/`xucred` alternative in the module header as "gives the pid too, requires platform struct handling; not used here because uid is sufficient and `getpeereid` is the ONE call that compiles on every BSD". (If a future task needs pid-proof on macOS it has that documented hook.)
- **Android:** `SO_PEERCRED` exists (`libc 0.2.191 unix/linux_like/android/mod.rs:1181`); same Linux shape. The daemon does not target Android, but the code path is the same `#[cfg(any(target_os = "linux", target_os = "android"))]` if you prefer to be exhaustive; simplest correct: `#[cfg(any(target_os = "linux", target_os = "android"))]` for the SO_PEERCRED arm and `#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonfly"))]` for the `getpeereid` arm. `#[cfg(unix)]` covers the file; the two arms inside are per-OS.

**Verification answers (what each platform proves, one line for the docs):** unix-in-general proves the connecting uid is the daemon's uid (`getpeereid`); Linux additionally proves pid via `SO_PEERCRED` (pid unused today, logged only); Windows cannot prove an identity beyond "local + DACL-passing" — documented. Rejection is a `TransportError` (connection refused), never a panic.

**Numeric bounds (named, per the issue):**
- **Session count fact (committed):** the session bound is enforced as `self.attached_sessions.load(SeqCst) >= SESSION_LIMIT` in `claim_session` — a counter field on `Daemon`, not a derived count (see the committed-design paragraph below).
- `MAX_PRE_HELLO_FRAMES: usize = 2` (the greeting exchange is 1 frame; 2 leaves room for a client that pipelined `hello`+`start` in one write as the existing race test does — see `two_connections_racing_to_start_one_id_leave_exactly_one_child` which sends `hello` then immediately `start` without awaiting).
- **The rate limit must not break the existing `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat` flood measurement.** That test floods raw `{"type":"hello"}\n` bytes (the "never reads" flood shape) and asserts `pushed > 0`, `blocked && pushed < CEILING`, and the RSS cap. The test floods `hello` frames — and the rate gate limits NON-greeting frames only (see the "Rate gate placement" note below) — so the flood never trips the gate; the existing queue + socket backpressure produces `blocked` exactly as today, and the test's numbers are untouched. Every frame the gate counts is a DECODED-complete frame at the moment it is pulled from the inbound queue; the gate is a HARD reject (one `error` frame, then close), never a delay, so it cannot slow the drain.
- Probe/EOF connections are not a bound — they must be **accepted, served nothing, and tolerated** (they never reach the rate gate: they send zero frames). The handshake gate treats 0-frame connect-and-drop as normal (the `seen == 0` no-log rule stays).

**State placement (T4-aware):** all of this must live in the `serve_client` header region (before the `select!`) per the constraint in the task text. Concretely: immediate `let` locals for `handshake: HandshakeState` (see the enum below) and the rate-limit gate. Do not restructure the `select!` arms (T4 owns that); ONLY insert the handshake pre-check at the top of the inbound arm and the rate-check just inside it. Keep the inbound arm's `seen += 1;` semantics and the `handle_frame(…)` call reachable only after both gates pass.

**Session-limit seam (committed design):** a counter field `attached_sessions: Arc<std::sync::AtomicUsize>` on `Daemon`, enforced in `claim_session` (`serve.rs:697`, returns `Result<SessionClaim, String>`). Rationale for the counter over any derived count or second registry: `WorkerManager` exposes no `len()` and `handle_for` iterates nothing (a derived count needs an engine change, forbidden this wave), and every prior per-session SET in this file leaked (the `dead` set — `close`'s invariant doc is explicit: "if you add a per-session set to `Daemon`, this function is one of the two places that has to release it"). An atomic counter has exactly two decrement points, both at the exact moments `handle_for(id)` stops returning `Some`, so it stays faithful to "live attached MAIN sessions" with no new cleanup:
- **Increment:** in `start` (`serve.rs:1432`), immediately after `daemon.manager.attach(&session_id, &env).await` returns `Ok(())` and BEFORE `watching.insert(session_id.clone())` (`~:1515-1521`). Only MAIN sessions are counted (a `start` is a MAIN session by definition — subagent rows come from `dispatch_subagent`, never here).
- **Decrement 1 — `close`:** in `close` (`serve.rs:1580-1615`), after `daemon.manager.detach(session_id)` returns `Some(_)` (a live session was detached); on `None` (nothing running) do nothing — matching `ok: false`'s "nothing was running" semantics.
- **Decrement 2 — `on_crash`:** in `Daemon::new`'s `on_crash` closure (`~:605-612`), inside the `if let Some(manager) = … { manager.detach(&session_id); }` body, decrement ONLY when the crashed session is a MAIN session — guard on `crash_subagents.lock().unwrap_or_else(|p| p.into_inner()).get(session_id).is_none()` (a child record means it was a subagent, never counted; decrementing it would drive the counter negative). `on_crash` receives only `(id, code, pid)` — the `subagents` lookup is how the code already distinguishes main from subagent.
- **Enforcement point in `claim_session`:** at the top, BEFORE the `starting.contains` check: if `self.attached_sessions.load(SeqCst) >= SESSION_LIMIT` AND `!starting.contains(session_id)` AND `self.manager.handle_for(session_id).is_none()` → `return Err(format!("session limit of {SESSION_LIMIT} reached (a session already started must close first)"))`. The already-claimed/already-running guards are load-bearing: the limit must NOT fire for an id already in `starting` or already attached, or the existing tests' specific refusals break (`a_session_whose_worker_crashed_can_be_started_again` asserts "already"; `two_connections_racing_to_start_one_id_leave_exactly_one_child` asserts the loser's error is a string — it stays "already starting"). The check is at claim time, before the ~5 s `attach` await; a race (two simultaneous `start`s when the counter is exactly at the bound) is bounded to at most one over-limit session, acceptable for a local hardening bound.

**Session bound numeric (committed):** `const SESSION_LIMIT: usize = 32` — private module-level const in `serve.rs` near `CLIENT_QUEUE_CAPACITY`. Concurrent attached MAIN sessions the daemon will run (a session whose `start` was `ok: true` and whose child is alive). Sizing: one daemon supervising a user's interactive session count; 32 is comfortably above any realistic same-uid concurrent session count and far below the process table / memory ceiling. Exceeding it returns a `start-result` with `ok: false` and message `session limit of 32 reached (a session already started must close first)`.

**Enforcement in `run`'s accept arm:** `Listener::accept` returns `Result<Connection, TransportError>`; on `Err(PeerIdentity)` the arm MUST NOT become a denial-of-service against the accept loop. Malicious locals can connect constantly and be refused constantly — that is fine (each accept is quick) but the error path must NOT `eprintln!` once per refused peer (an attacker could turn the operator's stderr into a DoS surface). Committed choice: a counted variant — a module-level `static REJECTED_FOREIGN_PEERS: AtomicUsize` that logs the first 3 rejections, then every 1024th. (`a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` sends connections that ARE the daemon's own uid locally in tests, so the periodic log path is only exercised by a genuinely foreign attempt.)

**Unix peer-check implementation (exact):**
```rust
/// What this platform can prove about the peer at accept time — resolved ONCE
/// per connection, inside `Listener::accept`, never stored in `Connection`.
/// (Windows: see `windows.rs`'s documented limitation — no proof possible.)
#[derive(Debug, Clone, Copy)]
enum PeerProof {/// unix limbs
    SameUid { uid: libc::uid_t, pid: Option<libc::pid_t> },
}
#[cfg(unix)]
fn peer_uid(fd: std::os::unix::io::RawFd) -> Result<(libc::uid_t, Option<libc::pid_t>), TransportError> {
    #[cfg(any(target_os = "linux", target_os = "android"))] {
        let mut cred = std::mem::MaybeUninit::<libc::ucred>::uninit();
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(fd, libc::SOL_SOCKET, libc::SO_PEERCRED, cred.as_mut_ptr() as *mut libc::c_void, &mut len)
        };
        if rc != 0 { return Err(std::io::Error::last_os_error().into()); }
        let cred = unsafe { cred.assume_init() };
        Ok((cred.uid, Some(cred.pid)))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        // All BSDs + macOS: getpeereid. (macOS ALSO has LOCAL_PEERCRED/xucred,
        // giving pid too; not used — see the module header note.)
        let mut euid: libc::uid_t = 0;
        let mut egid: libc::gid_t = 0;
        let rc = unsafe { libc::getpeereid(fd, &mut euid, &mut egid) };
        if rc != 0 { return Err(std::io::Error::last_os_error().into()); }
        Ok((euid, None))
    }
}
```
- In `Listener::accept` (`unix.rs:107-110`): after `let (stream, _addr) = self.listener.accept().await?;` and BEFORE `Connection::new(stream)`:
```rust
let fd = std::os::unix::io::AsRawFd::as_raw_fd(&stream);
let my_uid = unsafe { libc::geteuid() };
let (peer_uid, peer_pid) = crate::transport::unix::peer_uid(fd)?;   // TransportError::Io on failure
if peer_uid != my_uid {
    return Err(TransportError::PeerIdentity { peer_uid, expected_uid: my_uid });
}
// The uid matches: proceed. (Same-uid peers are the residual the file-mode
// check cannot close either — this is a second, defense-in-depth check.)
Ok(Connection::new(stream))
```
- `TransportError::PeerIdentity { peer_uid: u32, expected_uid: u32 }` — **new variant in `transport/mod.rs`** (hands a structured rejection to `run`'s accept arm).
- **Windows `Listener::accept` (`windows.rs:181-204`):** leave `Ok(Connection::new(server))` untouched; add a module-header paragraph documenting the limitation (Windows capabilities table above). No code change to the accept path beyond docs + comment.
- `run`'s accept arm (`serve.rs:1108-1136`):
```rust
Err(e) => match e {
    TransportError::PeerIdentity { peer_uid, expected_uid } => {
        // Reject a foreign-uid peer. Counted, not per-refusal-logged: a hostile
        // local process can connect repeatedly and must not turn the operator's
        // stderr into the DoS surface. (T9's socket mode 0o600 already stopped
        // most of these at the filesystem; this is the second, defense-in-depth
        // check against the same-uid residual.)
        let n = REJECTED_FOREIGN_PEERS.fetch_add(1, SeqCst) + 1;
        if n % 1024 == 1 || n <= 3 {
            eprintln!("fulcrum serve: rejected a peer (uid {peer_uid}, expected {expected_uid})");
        }
    }
    other => eprintln!("fulcrum serve: accept failed: {other}"),
},
```
  - `REJECTED_FOREIGN_PEERS: AtomicUsize` — module-level `static` in `serve.rs` (never reset; the modulo log stays flat). Log first 3, then every 1024th.
  - Note: the existing refuse path (`reclaim_stale_socket`'s EADDRINUSE / "a live daemon is answering") is NOT affected: a second daemon refuses at BIND time (`reclaim_stale_socket`'s blocking probe), before `run`; its probe connection (connect-then-drop) lands in the accept loop as a normal probe/EOF connection, exactly the shape `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` covers.

**Should a foreign peer be rejected at accept, or accepted-and-closed?** Accept-then-close (return a connection the caller immediately drops) is the alternative; but the credential is available at accept time on unix (the stream is right there), rejection returns `Err(PeerIdentity)`, and `run` already handles per-connection `Err` by continuing the loop. Accept-and-closed would ALSO work but would allocate a reader task + Connection for a peer we're about to refuse — wasted work and a place for a bug (an attacker who connects and never sends keeps a task alive). Reject at accept: the refused peer gets a closed socket immediately (their connect returns or their first write hits EPIPE), nothing is spawned. **Chosen: reject at accept.**

**The `hello` handshake (exact, committed shape):**
- Gate placement: top of the inbound arm of `serve_client`'s `select!`, BEFORE any other handling. Decode FIRST so a malformed line keeps its existing path (an `error` frame, stream continues) and is counted against neither the handshake nor the rate gate:
```rust
// Header region of `serve_client`, before the select!:
let mut handshake = HandshakeState::WaitingHello { pre_hello: 0 };
let mut rate = FrameRateGate::new(Instant::now());

// Inside the inbound arm:
inbound = connection.recv() => {
    let Some(frame) = inbound else { break };
    seen += 1;
    let frame = match frame {
        Ok(frame) => frame,
        // A malformed line is the peer's bug, not our reason to hang up —
        // the EXISTING path, untouched (counted against no gate).
        Err(e) => {
            if !enqueue(&tx, serde_json::json!({
                "type": "error",
                "message": format!("malformed frame: {e}"),
            }), &mut dropped) {
                break;
            }
            continue;
        }
    };
    // ── Required handshake (T3) ────────────────────────────────────────────
    // The peer must greet before any other frame is served. The `hello` frame
    // ALREADY exists (handle_frame's arm); this gate makes the exchange
    // MANDATORY rather than optional. A stray connection that never sends
    // `hello` is refused — it is not implicitly trusted to know the protocol.
    let is_hello = frame.get("type").and_then(Value::as_str) == Some("hello");
    let mut refused_pre_hello = false;
    match &mut handshake {
        HandshakeState::Done => {}
        HandshakeState::WaitingHello { pre_hello } => {
            if is_hello {
                handshake = HandshakeState::Done; // `handle_frame` answers it below
            } else {
                *pre_hello += 1;
                if *pre_hello > MAX_PRE_HELLO_FRAMES {
                    refused_pre_hello = true;
                }
            }
        }
    }
    if refused_pre_hello {
        let _ = connection.send_frame(&serde_json::json!({
            "type": "error",
            "message": "this daemon requires a `hello` frame before any other frame; too many non-hello frames",
        })).await;
        break; // refused pre-hello
    }
    // A non-greeting frame that arrived BEFORE the `hello`: counted, NOT
    // serviced (no reply) — "reject rather than serve" (issue #4).
    if matches!(handshake, HandshakeState::WaitingHello { .. }) && !is_hello {
        continue;
    }
    // ── Inbound frame rate (T3) ─────────────────────────────────────────────
    // NON-greeting frames only, token-bucket, HARD reject (told once, then
    // closed). Frames are DECODED-complete here (the reader queue already
    // bounded the bytes). `hello` frames are free after the handshake — bounded
    // by the existing inbound-queue + socket backpressure so the pinned
    // "never reads" flood test keeps its numbers (see the rate-gate note).
    if !is_hello && !rate.accept(Instant::now()) {
        let _ = connection.send_frame(&serde_json::json!({
            "type": "error",
            "message": "frame rate limit exceeded (this daemon allows at most 200 non-greeting frames/s per connection)",
        })).await;
        break;
    }
    let reply = handle_frame(&daemon, &mut watching, &frame).await;
    if let Some(reply) = reply {
        if !enqueue(&tx, reply, &mut dropped) {
            break;
        }
    }
}
```
- Details:
  - `enum HandshakeState { WaitingHello { pre_hello: usize }, Done }` — defined near `serve_client` (private, module-level or inside fn). `MAX_PRE_HELLO_FRAMES = 2`.
  - **The gate counts `hello` itself as the first allowed frame**; `handle_frame` still answers `hello` (reply unchanged). A client that sends ONLY `hello` and then idles is fine (see `a_fresh_client_receives_nothing_before_it_starts_anything`).
  - Once `Done`, the gate stops counting; the frame rate is the single gate after that.
  - **Non-greeting frames before `hello` are NOT serviced:** the `pre_hello` counter increments for each non-`hello` frame while waiting; only `hello` (or the rejection) exits the wait. A `start` sent before `hello` is counted pre-hello and, if ≤2, is NOT passed to `handle_frame` — it is counted in `pre_hello` and **dropped** (no reply). Rationale: "reject rather than serve" is the issue's demand; sending an `error` for a `start`-before-`hello` would leak routing behavior. Only `hello` is answered while waiting.
  - On the 3rd non-`hello` frame: send ONE `error` frame then `break` (close). The `error` names the requirement.
  - **Probe/EOF connections:** they send ZERO frames; `connection.recv()` returns `None` → `break` immediately, the handshake is never reached, the `seen == 0` epilogue runs (no log line). `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` stays green because the probe drops the connection before the daemon's reader even decodes one frame. **Existing behavior preserved.**
  - **Existing tests all send `hello` first — VERIFIED by reading each frame-sending site:** `close_ends_the_session…` (`:908-913`), `a_session_streams…`, `two_sessions…`, `a_probe_connection…` (sends `hello` AFTER the three probes), `an_unknown_frame…` (`:1526-1530`), `a_malformed_line…` (`:1568-1573`), `repeated_disconnects…` (`:1621-1623`), `a_client_that_never_reads_is_throttled…` (floods `hello` exclusively), `a_client_that_never_reads_does_not_stall…` (`:1866-1868`), `two_connections_racing…` (both `a` and `b` send `hello` then immediately `start` without awaiting — the pipelined case `MAX_PRE_HELLO_FRAMES = 2` tolerates), `a_session_whose_worker_crashed…` (sends `hello` first), `a_prompt_for_an_unknown_session…` (`:2147-2150`), `a_fresh_client_receives_nothing…` (`:2197-2202`; the idle client's silence is checked AFTER its `hello` reply), `a_child_killed_by_sigkill…` (`:2267-2269`), `a_second_daemon_on_the_same_endpoint…` (refused at bind — the probe connection is the only dial), `a_daemon_that_cannot_open_its_database…` (dies before `run`). **Conclusion of the audit: NO existing test sends a non-greeting frame first; NO existing test needs editing.** The only daemon-dialing sites that do not send `hello` are the probe/EOF cases, which is the required behavior. If the implementer's own re-grep finds one, the ONLY permitted edit is adding the `hello` first-frame, named in the commit message.
  - **Rate gate placement (committed: non-greeting frames only):** the existing `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat` flood test floods `hello` frames and asserts `blocked && pushed < CEILING`, where `blocked` means the peer's WRITE timed out (the daemon's socket filled). If the rate gate counted `hello` frames and hard-closed the connection at ~1000 frames, the flood's `write_all` would return EPIPE → `panic!("the flood's write failed after {pushed}…")` — a broken pin. **Therefore the rate gate applies ONLY to non-greeting frames (`start`/`prompt`/`close`/unknown); `hello` frames are free after the handshake (`Done`), bounded by the existing inbound-queue + socket backpressure — the exact measured, pinned throttle.** Rationale: `hello` is cheap (no session, no child); the threat the rate gate exists for is a hostile client firing `start`/`prompt` (child-creation and relay cost). A hostile client can still flood `hello` — bounded by the reader queue (1024 frames) + socket backpressure, the state the flood test proves keeps the daemon flat. Document this in `FRAME_RATE`'s comment.
     - Consequence: `MAX_PRE_HELLO_FRAMES` still rejects pre-hello chatter (bounded, 2 frames); after `Done`, `hello` frames are free (protocol-correct repeated greetings), `start`/`prompt`/`close`/unknown are rate-limited.
     - The new flood test (Step 5) therefore floods `start`/`prompt` frames (any non-hello) and must observe the hard rejection (an `error` frame, then close) — it must NOT flood `hello` (that is the existing test's job).
  - **Session-limit refusal path:** new integration test floods nothing — opens `SESSION_LIMIT` live sessions (via the fixture worker), then a `start` for one more → `start-result` with `ok:false` and error naming the limit. Uses `worker_fixture` + the existing `Daemon` harness; closes all sessions at the end (each `close` decrements the counter) so teardown is clean. Optionally assert the count drops after a `close` (counter faithfulness): after closing one, a new `start` succeeds.
- **Where the counter decrements (`on_crash`):** `Daemon::new`'s `on_crash` closure (`serve.rs` ~:605-612) already does `if let Some(manager) = crash_manager.get().and_then(|w| w.upgrade()) { manager.detach(&session_id); }`. The counter counts MAIN sessions only (the increment happens only for ids a `start` succeeded on). `on_crash` fires for a SUBAGENT too (with the CHILD id, which was never incremented) — decrementing it would drive the counter negative. `on_crash` receives only `(session_id, code, pid)`, no `is_subagent` flag, so guard on the daemon's own `subagents` map: decrement ONLY when `crash_subagents.lock().unwrap_or_else(|p| p.into_inner()).get(&session_id).is_none()` (no child record → it was a main session). A subagent crash leaves the counter untouched; a main crash decrements. Document this in the field doc.
- **`close` decrement:** after `manager.detach(session_id)` returns `Some(_)` (a live session was detached), decrement. `detach` returns `None` for nothing-running — no decrement (matches the "false means nothing was running" semantics). The counter then equals the number of sessions with a live handle exactly, minus the race windows the existing dup-guard already accepts.
- **Thread-safety:** `attached_sessions: Arc<AtomicUsize>` on `Daemon` (clone into closures; `Ordering::SeqCst` for the counter is fine — the increments/decrements are coarse).

**Rate window implementation (exact, simple, no new deps):**
- `FRAME_RATE: usize = 200;` `FRAME_RATE_BURST: usize = FRAME_RATE * 3; // 600` — **token-bucket implementation (committed):**

```rust
/// Per-connection inbound frame-rate gate (T3). Token bucket: capacity
/// `FRAME_RATE_BURST`, replenishing `FRAME_RATE` tokens per second. Counts
/// NON-GREETING frames only (`start`/`prompt`/`close`/unknown) — `hello` frames
/// stay bounded by the existing inbound-queue + socket backpressure so the
/// pinned "never reads" flood test keeps its numbers. The gate is a HARD
/// rejection (the peer is told once, then closed), never a delay — a delayed
/// flood would stress the queue instead of the socket. Pure-local state;
/// moves into T4's reader half as-is.
struct FrameRateGate {
    tokens: f64,          // start at FRAME_RATE_BURST as f64
    last: Instant,
}
impl FrameRateGate {
    fn new(now: Instant) -> Self { Self { tokens: FRAME_RATE_BURST as f64, last: now } }
    fn accept(&mut self, now: Instant) -> bool {
        let dt = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + dt * FRAME_RATE as f64).min(FRAME_RATE_BURST as f64);
        if self.tokens < 1.0 { return false; }
        self.tokens -= 1.0;
        true
    }
}
```
  - `FRAME_RATE = 200` (tokens/sec), `FRAME_RATE_BURST = 600`. Both `const` near `CLIENT_QUEUE_CAPACITY`. `Instant` for timing (monotonic, leak-free). The first frame after `HandshakeState::Done` comes with `rate = FrameRateGate::new(now)`.
  - Note: `now` is `std::time::Instant::now()` at each inbound frame — cheap.
- **Why these numbers preserve the pins:** the existing flood test floods `hello` (unaffected by the non-greeting rate gate); the new flood test (Step 5) floods `start`/`prompt` and must observe: first ~600 in short order pass (burst), then 200/s, then ONE `error` frame, then close. The test asserts: (a) the daemon answers ONE `error` frame naming the rate limit and closes; (b) the daemon stays alive for a subsequent healthy client; (c) it does NOT assert frame counts or RSS — it pins the error-then-close, which is the boundary, and would otherwise flake on scheduling (see Step 5's concrete assertions).

**What about `handle_frame`'s `hello` arm?** Leave it. The gate answers `hello` through it; the reply shape is untouched (pins: `close_ends_the_session…` asserts `hello["protocolVersion"] == PROTOCOL_VERSION`).

**main.rs:** NO change. The task's owned set lists `main.rs`, but the constraint is "only if a startup-time counter is passed into `run`" — it is not: the session counter lives on `Daemon`, built inside `run` (called from `main.rs:140` as `runtime.block_on(serve::run(listener, db, factory))` with three args, unchanged). Leave `main.rs` untouched and say so in the commit message.

**Files (all inside the owned set):**
- Modify: `fulcrum/src/transport/mod.rs` — add `PeerIdentity { peer_uid: u32, expected_uid: u32 }` variant + doc; re-export unchanged.
- Modify: `fulcrum/src/transport/unix.rs` — `peer_uid()` helper (cfg-armed), `verify_peer_uid()`, `Listener::accept` check, module-header platform-proof paragraph, `#[cfg(test)]` unit tests.
- Modify: `fulcrum/src/transport/windows.rs` — module-header limitation paragraph + doc-only change to `Listener::accept`'s comment (NO code change to accept; NO FFI).
- Modify: `fulcrum/src/serve.rs` — `run` accept arm (PeerIdentity handling + `REJECTED_FOREIGN_PEERS`), `serve_client` header (handshake state, rate gate, pre-hello reject), `Daemon` field `attached_sessions: Arc<AtomicUsize>`, `claim_session` session-limit check, `start` increment, `close` decrement, `on_crash` decrement (subagent-guarded), module header OPEN QUESTION paragraph + constants.
- Modify: `fulcrum/tests/serve.rs` — the three NEW tests (pre-hello reject; rate-limit flood; session limit). NO edits to existing tests (audit resolved: all send `hello` first).
- Modify: `fulcrum/tests/transport.rs` — `same_uid_peer_is_accepted` in the `unix_specific` module + the root-gated `#[ignore]` test, following the module's existing `bind`/`BIND_SERIALIZER` discipline.
- `fulcrum/src/main.rs` — **no change** (verify at commit time).
- NOT touched: `fulcrum/src/transport/frame.rs` (T4's), `fulcrum/Cargo.toml` (frozen), engine files, `.github/workflows/ci.yml`.

**Steps (TDD order):**

1. **Add the `PeerIdentity` error variant** in `fulcrum/src/transport/mod.rs` (in `TransportError`) AND construct it in `unix.rs`'s accept path — one commit, so the variant is never dead code. Compile via `cargo check -p fulcrum --offline` → expect only the (pre-existing external) `frame.rs` errors; nothing new from this change. If the change alone introduces warnings, fix them before moving on.

2. **Unix peer check + rejection** in `fulcrum/src/transport/unix.rs` + `serve.rs` accept arm. Write the transport test FIRST:
   - **Testing the reject path honestly (committed shape):** a live foreign-uid process cannot be constructed in this suite — `setuid` to another uid needs root, CI's ubuntu runner is unprivileged, and `tests/` cannot name a `unix.rs`-private fn. So the proof is three pieces: (a) extract the comparison into an internal, unit-testable fn `fn verify_peer_uid(peer_uid: libc::uid_t, expected_uid: libc::uid_t) -> Result<(), TransportError>` in `unix.rs`, unit-test it in `unix.rs`'s own `#[cfg(test)] mod tests` (reachable via `cargo test -p fulcrum --lib`): `verify_peer_uid(1000, 1000) == Ok(())`, `verify_peer_uid(999, 1000) == Err(TransportError::PeerIdentity { .. })`; (b) a portable integration test in `tests/transport.rs`'s `unix_specific` module, `same_uid_peer_is_accepted`: bind a socket (through `bind`/the `BIND_SERIALIZER` discipline), `dial`, and assert `Listener::accept` succeeds — the same-uid path through the REAL socket, same code path the reject guard sits in; (c) a `#[cfg(target_os = "linux")]`, `#[ignore]`-by-default integration test `foreign_uid_peer_is_rejected_under_root` that runs only when `libc::geteuid() == 0` (returns early otherwise): spawn a child that `setuid(65534)` ("nobody") then connects; assert `Listener::accept` returns `Err(TransportError::PeerIdentity { .. })`. Document on (c): "runs only under an operator with root; CI's runner is unprivileged" — the honest `daemon_rss` pattern (`#[cfg(target_os = "linux")]` with an honest zero elsewhere). The criterion "a peer of a different uid is rejected (unix)" is satisfied by (a)+(c) with the guard structurally in `accept`.
   - Commands: `cargo test -p fulcrum --lib --offline` (unit, includes `verify_peer_uid`) and `cargo test -p fulcrum --test transport --offline -- unix_specific::` (integration, includes `same_uid_peer_is_accepted`; `foreign_uid_peer_is_rejected_under_root` is ignored unless run with root).
   - **Note the pre-existing `frame.rs` break:** `cargo test` compiles the whole crate including `frame.rs`, so T3's test commands will FAIL on the external edit. Per the wave protocol the implementer runs tests in its OWN worktree on its OWN branch from the recorded base (clean `a3f76c0`), where `frame.rs` is intact. Add to Step 1: "if `cargo test`/`cargo check` fail ONLY on `frame.rs` errors (E0609/E0560 about `writer`/`pending`), the tree is showing an external T4 edit — stop, do NOT touch `frame.rs`, report it, and switch to a clean worktree/branch from `a3f76c0` (the wave base) before proceeding. If the error is T3's own, fix it."

3. **Windows documented limitation** — doc-only edit to `transport/windows.rs`'s module header (Windows capabilities table) + a doc comment on `Listener::accept`. No code. Gate check: after this step, run `cargo check --target x86_64-pc-windows-gnu --all-targets --offline` ON A CLEAN TREE — expect Windows-compile errors only from the pre-existing external `frame.rs` edit (if the external edit is in the shared tree, this check will fail on it; on a clean base it should pass with the new docs). Do NOT claim runtime proof.

4. **Required handshake + rate gate in `serve_client`** — implement per the exact design above. Test-first:
   - New integration test in `tests/serve.rs` — `a_connection_that_sends_frames_before_hello_is_rejected_not_served`: dial, send `{"type":"start","sessionId":"pre"}`, then a second non-hello frame, then a third → expect an `error` frame naming the handshake, then EOF. `Client`'s `recv_until` panics on EOF, so use the raw pattern (`raw_dial` + split the stream + `write_all` the three frames + read lines) and assert: no `start-result` was ever received, the ONE `error` names the handshake, and the stream EOFs within `FRAME_BUDGET`. (Concretely: first frame → dropped (pre_hello=1, no reply); second → dropped (pre_hello=2, no reply); third → the `error` + close.)
   - The control ("a hello-first client is served") is the EXISTING suite — every existing test sends `hello` first and is fully served. Do NOT add a redundant greeting test.
   - `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` — ALREADY EXISTS; keep green (no edit).
   - The rate/flood test (Step 5) and the session-limit test (Step 6) are separate steps but use the same harness.
   - Implement: `HandshakeState`, `MAX_PRE_HELLO_FRAMES`, the gate in the inbound arm, `FrameRateGate`, `FRAME_RATE`/`FRAME_RATE_BURST`.
   - Commands: `cargo test -p fulcrum --test serve --offline -- a_connection_that_sends_frames_before_hello_is_rejected_not_served` → red first (daemon currently serves them), then green.
   - **Audit every existing test that dials the daemon** (grep `Client::dial\|raw_dial\|connect(` in `tests/serve.rs`): each must send `hello` first, be a probe/EOF, or be a refused-at-bind case. **This plan already did that audit (see "Existing tests all send `hello` first" above) — conclusion: NO existing test needs editing.** Re-run the grep to confirm; if one contradicts this, the ONLY permitted edit is adding the missing `hello` first-frame (name it in the commit message). Then run the full `tests/serve.rs` suite on a clean base and fix regressions.

5. **Rate-limit flood test** in `tests/serve.rs` (new): flood **non-hello** frames (`{"type":"prompt","sessionId":"none","text":"x"}` — answered with `prompt-result ok:false`, cheap, no session needed) from a raw split socket; the flood runs for a short fixed window (e.g. 3–5 s). Assert, in order: (a) within `FRAME_BUDGET` an `error` frame arrives whose message names the frame-rate limit (read the raw stream — the only frames before it are `prompt-result` frames from the burst/sustained budget); (b) the stream then EOFs (the daemon closed the connection); (c) a subsequent healthy `Client` (hello→start→prompt→event, the probe test's shape) is fully served — the daemon is unharmed. Do NOT assert exact frame counts (scheduling-dependent); do NOT reuse the existing test's `CEILING`/RSS assertions (those pin the `hello` flood). Use `FRAME_BUDGET`; keep the file's deadline rules.
   - Note: `prompt` for a missing session is answered (cheap) — the flood gets ~200/s `prompt-result` frames and one `error`, then close. That is the boundary being pinned.
   - Also assert the daemon survives (the healthy client afterwards) — that's criterion 3's "rate-limit flood case … must still not disturb the daemon".

6. **Session limit**:
   - Test-first, `tests/serve.rs`: `the_session_limit_refuses_one_past_the_bound_and_releases_on_close`. Committed design: `SESSION_LIMIT` is a private module-level `const SESSION_LIMIT: usize = 32` in `serve.rs` (NO overloadable `Daemon::new` — its signature is pinned; NO test-only setter — that would be a public-API widening the repo's rule forbids). The test does NOT hardcode 32: it starts sessions with the fixture worker (`worker_fixture` + `start_frame`) in a loop until a `start` is refused, asserts the refusal's error names the limit, then closes exactly the refused-OR-one-open session and asserts a NEW id's `start` succeeds (release-on-close). This bounds the test to exactly the constant's value whichever way the constant changes, and the fixture is a fast fake worker (~quick spawn+ready). If starting to the limit is measurably too slow in practice, run the loop under `FRAME_BUDGET`-per-`start`; 32 fixture sessions is a few seconds at worst — acceptable.
   - Implement: `attached_sessions: Arc<AtomicUsize>` field on `Daemon`, `claim_session` check (only when id not already claimed/running), increment in `start` after successful attach, decrement in `close` on `Some`, decrement in `on_crash` (subagent-guarded via the `subagents` map lookup as described).
   - Commands: run the new test → red (limit not enforced), then green.
   - **Pins re-check:** `two_connections_racing_to_start_one_id_leave_exactly_one_child` must still see one `ok:true` and the loser's error a string (the loser's id is in `starting` → "already starting", the check skips claimed ids); `a_session_whose_worker_crashed_can_be_started_again` must still get "already" for the live session (skips attached ids); `repeated_disconnects_do_not_degrade_the_daemon` (3 cycles) never reaches 32 — but the counter's faithfulness is pinned behaviorally in the new test: after closing sessions back below the bound, a fresh `start` succeeds (release-on-close); and after ALL closes, one more `start` succeeds — proving no leak accumulated across the loop.

7. **Module-header OPEN QUESTION + docs**: in `serve.rs`'s module header (the "what T9 does NOT do" area, ~`:1-130`), add a short paragraph: "Open question (issue #4, non-goal): transcript encryption at rest is NOT addressed here or by T13; if a compliance requirement demands it, it is a separate task." Also document the peer-credential platform proofs and the numeric bounds in the module headers of `unix.rs`/`windows.rs` and next to the constants in `serve.rs`. Update any stale doc that says "the socket is the credential" to "the socket is the credential, and the peer-credential check is the second, defense-in-depth check (T3)" (e.g. `serve.rs`'s module header and `transport/windows.rs`'s access-control disclaimer — name the exact lines you changed in the commit message).

8. **Final verification on a clean base (the wave's own worktree, base `a3f76c0`)** — run, in order:
   - `cargo fmt --all -- --check` (root; passes only on a clean tree — if the external `frame.rs` edit breaks formatting, report it, don't fix it)
   - `cargo clippy -p fulcrum --all-targets -- -D warnings --offline` → must pass with ZERO warnings. On the local Linux box this catches the unix leg and unused-import traps; the Windows/macOS legs are the CI matrix's, reproduced locally only by `cargo check --target x86_64-pc-windows-gnu --all-targets --offline` (already a required step) — note macOS cannot be checked locally (no target installed; the `aarch64-apple-darwin` target IS installed per `rustup target list` → use `cargo check --target aarch64-apple-darwin --all-targets --offline` to type-check the `getpeereid` arm; if the std for that target is missing offline, report it, do not block; CI covers it).
   - `cargo test -p fulcrum --test serve --test transport --offline` (T3's owned test files; on a clean base)
   - `cargo test -p fulcrum --lib --offline` (unit tests incl. `verify_peer_uid`, `handle_frame` hello, queue/gap tests)
   - **Full suite is the run's gate, not T3's** — T3's child proves its own files; the fan-in full suite is the roadmap's step 6.
   - Commit (wave child commits on its OWN task branch `feature/daemon-hardening-task3`, never the feature branch or main): message `feat(daemon): peer-credential check, session/rate limits, and a required hello handshake (issue #4)`. Include only T3's files; the workflow file and `main.rs` (unchanged) are not committed by T3.

9. **Cross-check the numbers stayed**: re-read `CLIENT_QUEUE_CAPACITY`'s two const asserts and `gap_frame`'s tests; confirm no test edited its asserted numbers. Confirm, via `git diff`, that `repeated_disconnects_do_not_degrade_the_daemon` (and every other existing `tests/serve.rs` test) was NOT touched — the audit resolved that no existing test needs an edit.

**Acceptance criteria:**
- [ ] On unix, `Listener::accept` verifies the connecting uid equals `geteuid()` (Linux: `SO_PEERCRED` via `libc::ucred`, giving pid too; BSDs+macOS: `getpeereid`); a mismatched uid returns `TransportError::PeerIdentity` and `run` refuses the peer without spawning a task or flooding stderr (counted logging).
- [ ] Windows `transport/windows.rs` documents exactly what the named pipe can and cannot prove (local-only via `reject_remote_clients(true)`; NO identity proof without a crate; raw FFI rejected on soundness+manifest grounds); the compile gate `cargo check --target x86_64-pc-windows-gnu --all-targets` passes (on a clean base).
- [ ] `handle_frame`'s `hello` reply is unchanged; a connection that sends up to `MAX_PRE_HELLO_FRAMES` non-`hello` frames before any `hello` gets NO service for them, then ONE `error` naming the handshake and the connection is closed — new test `a_connection_that_sends_frames_before_hello_is_rejected_not_served` passes; probe/EOF connections still slide through untouched (`a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon` green).
- [ ] Inbound frame rate is bounded with the bound named (`FRAME_RATE = 200` non-greeting frames/s, `FRAME_RATE_BURST = 600`, token-bucket, hard-reject-once): new flood test observes the `error` + close; the daemon remains healthy for a subsequent client; the existing `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat` (which floods `hello`) still passes with `CEILING = 50_000` intact.
- [ ] Concurrent sessions bounded at `SESSION_LIMIT = 32`: `claim_session` refuses past the bound with a message naming the limit only for ids not already claimed/running; `start`/`close`/`on_crash` maintain the counter (crash decrement subagent-guarded); new limit test proves refusal + release-on-close + no-leak; the duplicate-race and crash-recovery tests still pass.
- [ ] `StartEnv.permission` is byte-for-byte untouched by all gates (no rewrite/pre-check anywhere in T3's code).
- [ ] No edit to `transport/frame.rs`, `fulcrum/Cargo.toml`, engine files, or `.github/workflows/ci.yml`; `Connection::new`'s signature unchanged; the gap-notice numbers (`FAN_OUT 4096`, `CLIENT_QUEUE 1024`, the two const asserts, `gap_frame` shape) unchanged and their tests green.
- [ ] `cargo fmt --all -- --check`, `cargo clippy -p fulcrum --all-targets -- -D warnings`, `cargo test -p fulcrum --lib`, `cargo test -p fulcrum --test serve --test transport` all green on a clean base; `cargo check --target x86_64-pc-windows-gnu --all-targets` and `--target aarch64-apple-darwin --all-targets` green (or reported as unavailable offline with CI as the proof).
- [ ] `main.rs` unchanged (verified at commit time); commit message names any existing-test edit (audit found none — the only possible edits would be `hello`-first fixups, none needed).

**UNVERIFIED / flagged for the caller's vet:**
- `UNVERIFIED: the pre-existing dirty `frame.rs` (external T4 edit) breaks `cargo check`/`test` on the shared primary tree; all T3 commands above assume a clean worktree from base `a3f76c0`.` (The `frame.rs` diff and the untracked `task02` plan doc were present before/were not caused by this planner's read-only session; they predate my `cargo check` — a `cargo check` cannot modify sources. The implementer MUST follow Step 1's clean-worktree instruction. A `Daemon::new` overload carrying the limit was deliberately NOT added — the signature is pinned; the constant `SESSION_LIMIT` + derived behavior is the chosen shape.)
- `UNVERIFIED: whether the local macOS target's std artifacts are present for the offline `--target aarch64-apple-darwin` cross-check; CI covers it if not.`
- `UNVERIFIED: `libc::SO_PEERCRED` resolves on glibc-x86_64-Linux at build time — the lock pins `libc 0.2.191`, where `SO_PEERCRED` (=17) is present in the `new` (linux-uapi) module exported at the crate root on Linux, and `libc::ucred` is type-visible there; the implementer's first compile of the `peer_uid` helper is the confirmation, and the fallback (musl / `getpeereid`) is a one-line `cfg` change documented in `peer_uid`'s body.`
- `UNVERIFIED: the `a_prompt_for_an_unknown_session_is_answered_not_dropped` (`:2147-2150`) and `a_fresh_client_receives_nothing_before_it_starts_anything` (`:2197-2202`) tests were READ during this plan and both send `hello` first — resolved, not a risk. The full "every test sends hello first" audit result is in the "Existing tests all send `hello` first" bullet; Flag for the vet: re-verify the two line references I cite for the frame-sending sites in the other tests (`an_unknown_frame…:1526-1530`, `a_malformed_line…:1568-1573`, `repeated_disconnects…:1621-1623`, `a_client_that_never_reads_does_not_stall…:1866-1868`, `a_child_killed_by_sigkill…:2267-2269`).`
- `UNVERIFIED: the new flood test's exact frame counts (burst/sustain timing) — the test asserts the ERROR+CLOSE, not exact counts, so it cannot flake on scheduling.`

**Sources:** `fulcrum/src/serve.rs` (`run:1108`, `serve_client:1160-1260`, `claim_session:697`, `SessionClaim:783`, `handle_frame:1397`, `hello arm:1399-1419`, `start:1432` & increment site `~1515-1521`, `close:1580-1615`, `on_crash ~605-612`, `unit tests:1800-2524`), `fulcrum/src/transport/unix.rs` (`accept:107-110`, `bind_private:169`), `fulcrum/src/transport/windows.rs` (`accept:181-204`, header access-control disclaimer `:32-52`), `fulcrum/src/transport/mod.rs` (`TransportError:122-172`, gating rule `/mod.rs:15-28`), `fulcrum/src/transport/frame.rs` (READ-ONLY: `Connection:117-130`, `send_frame:249-282`), `fulcrum/Cargo.toml` (tokio features, `libc` under `[target.'cfg(unix)'.dependencies]`, windows-absent comment), `Cargo.lock` (`libc 0.2.191`), `~/.cargo/registry/src/*/libc-0.2.191/src/…` (`new/linux_uapi/asm/socket.rs:29` SO_PEERCRED=17; `unix/bsd/mod.rs:627` getpeereid; `unix/bsd/apple/mod.rs:3170` LOCAL_PEERCRED), `fulcrum/tests/serve.rs` (harness `:100-830`, probe test `:1307`, flood test `:1714-1860`, reaping guards `:900+`, deadline rules), `fulcrum/tests/transport.rs` (`unix_specific` module `:412+`, `bind`/`accept_one`/`dial`/`recv_checked` `:240-330`, guard test `:183`), `.github/workflows/ci.yml` (3-OS matrix, clippy `-D warnings`, fmt check), `docs/roadmap/daemon-hardening-ground-transport-serve.md`, `docs/roadmap/daemon-hardening-status.md` (wave-1 rulings: T3 resolves peer at accept; frame.rs to T4).
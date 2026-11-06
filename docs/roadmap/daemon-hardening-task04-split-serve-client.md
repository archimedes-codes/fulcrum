---
status: committed
done-when: A client that starts a session and streams sees its own event frames arrive WHILE `start` is still in flight; the pre-existing framing suite still passes; the `gap` notice keeps its numbers and its tests; and a new regression test that MUST fail if `pending` ever becomes reachable from two tasks is green.
parent: docs/roadmap/daemon-hardening-status.md
---

# T4 — split `Connection` and `serve_client` (issue #3)

**Goal:** Remove the one-connection stall: today `serve_client` is one task running one `select!`, `start` runs INLINE and awaits `attach` (the engine's ~5 s `ready` bound, `src/worker/manager/mod.rs:223` → `handle.wait_ready(Duration::from_secs(5))`), and while it waits this connection's own fan-out relay arm cannot run — so a client that starts a session and streams sees its first event frames only after `start` completes.

**Architecture:** Split `transport::frame`'s `Connection` into an owning reader half (`Connection`) and an owning writer half (`ConnectionWriter`), moving `pending` into the writer so it is STRUCTURALLY impossible for two tasks to reach it. Then split `serve_client` into a reader/reply task and a writer task sharing the session event subscription, so `handle_frame(…).await` no longer blocks the relay arm. The `gap` accounting (`enqueue`, `gap_frame`, `dropped`, `watched`) moves intact into the writer, which is the sole owner of `send_frame`.

**Tech Stack:** Rust (pinned `rust-toolchain.toml` channel `1.99.0`), tokio 1.54 (`process io-util io-std sync macros time net rt rt-multi-thread`), `tokio::io::split` over a `Box<dyn Duplex + Unpin + Send>` duplex, `tokio::sync::mpsc` for outbound, `tokio::sync::broadcast` for the fan-out. **No new dependencies** (`Cargo.toml` is frozen for this feature; no `windows`/`windows-sys`).

**Rounding (revised in-place after the vet round 1, 🔴; amended again after round 2 — the five round-2 amendments are marked 🔥/🟠/🟡 inline and logged in the Step list):**
- **Round-2 amendment log (this pass, surgical — the architecture is unchanged):** (1) 🔥 `serve_client` holds the writer's `JoinHandle` and **`abort()`s it after `reader_loop` returns** — a writer parked inside `send_frame` never outlives its `serve_client` (D1/D3, tested invariant); (2) 🟠 the shared-writer regression pin that actually goes red, with an honest note on which existing pins false-green on the regression (Step 1); (3) 🟠 the reader-loop-termination property re-scoped as a `tests/serve.rs` integration property — the slow-start test's teardown half (Step 1/Step 9); (4) 🟠 the slow-start engagement gap (Step 9); (5) 🟡 claim precision on `shutdown`'s un-park semantics, the writer-exit list, the gap-interleave unit test, and `Client::send`'s EOF semantics.
- **Teardown is drop-based, and the dropped-write-half claim is corrected to what tokio 1.54 actually does** (CRITICAL 1). `tokio::io::WriteHalf<T>` wraps `Arc<Inner<T>>` (verified in tokio 1.54.1 `src/io/split.rs:20-42`): it has **no `Drop` impl**, and dropping it does NOT close the write direction while the `ReadHalf` is alive — the `Arc` is shared, the stream stays open, and a parked `write` does not error. All three previous teardown variants (join-await, join-timeout, dropped-write-half-closes-socket) are therefore deadlocked or wrong on this tokio. The single primitive that DOES close the write direction while the read half lives is `poll_shutdown` on the shared stream (see A1b/A2b — it makes FUTURE writes fail and the peer sees EOF; it does NOT reliably un-park a write already blocked in the OS, round-2 🟡; the parked write in a real connection is broken by the peer's close). `ReadHalf::unsplit` requires the `WriteHalf` to still be alive (tokio `split.rs:61-75` panics when the pair is unrelated — so no). The shutdown is the honest mechanism, and it is only reachable AFTER the write half has been structurally surrendered: Connection's last send is the close-shutdown (A1b/Part D, MUST), and the `ConnectionWriter`'s own `flush` must also not be awaited mid-select (A1c, MUST). Drop-based reclamation (what drops what, in order) is specified as a tested invariant in D3.
- **The writer is spawned, never joined — and its `JoinHandle` is `abort()`ed by `serve_client` after `reader_loop` returns (round-2 🔥).** The old plan had the reader `await` the writer's `JoinHandle` after a `stop` one-shot; the vet's core objection — a writer parked inside `send_frame`'s write loop cannot poll the stop arm, so the join never resolves — is verified correct (see `frame.rs:274-282`, the `loop { … self.writer.write(&self.pending).await … }` with no timeout; a peer that stops reading parks that loop at the OS send buffer internal, and armed with the corrected `WriteHalf` semantics the join hangs FOREVER). The plan: the writer is a child task whose `JoinHandle` `serve_client` KEEPS and `abort()`s after the reader's task returns (and after the reader's early `break`). Aborting — NOT joining — is compatible with "never joined": it makes a parked write-loop's drop synchronous (no `Drop`-time `flush` is ever awaited) and gives "no task outlives `serve_client`" mechanical enforcement. Shutdown is still announced from the reader through the write half itself (A2b's pre-split shutdown; the writer's own A1b shutdown on its exits), never through a join; the abort is the backstop for the one path no writer exit reaches (peer closed inbound, write direction still open). The prior "handle-dropped, never-joined" claim is retired: dropping the handle alone would orphan a writer parked inside `send_frame`.
- **The slow-ready test now uses a `--slow-ready <ms>` startup-delay hook in `src/fixtures/worker.rs`** (CRITICAL 2, vetted route; round-2 🟠 amendment — its engagement gap is fixed in Step 9). Verified against the repo: `_core` is a naming rule for the TWO existing bins (`Cargo.toml:96-102`, `src/bin/fake_worker_core.rs:9-13`, `src/fixtures/mod.rs:37-50`), `fixture_bins.rs` asserts only on `target/debug/` for those two names (its four tests never look anywhere else); the fixture emits `hello` then `ready` UNPROMPTED at startup (`src/fixtures/worker.rs:549-560`,`worker_main`), and `__long_stream__` chunks stream only AFTER a `start` (they ride `line 552`'s read loop, and no chunk is emitted before `start` — verified). The vetted route is exactly the `--slow-ready` hook; the wrapper-bin and the `fixture-bins-assertions` premises are deleted. The ordering claim is re-traced (see Step 9): a `__long_stream__` prompt's `prompt-result` is enqueued BEFORE the first chunk is relayed to a SOCKET-READING watcher (the reader enqueues the reply through `enqueue_shared` synchronously as soon as the prompt cmd is dispatched, while the chunks must travel worker → pump → on_event → fan-out → writer → drain → socket); the vet's WATCH-EARLY+DELAYED-START discriminator (a session ALREADY watched, streaming, while a SECOND `start` is in flight) is kept, and its boundary is now an EXPLICIT BARRIER — see Step 9's barrier reading and the boundary-status protocol.
- **The `gap`-vs-reply ordering contract is now stated** (CRITICAL 3) — see D4: `gap`s may interleave, they announce from the queue head only when the drain woke with a `dropped > 0`; a `gap` may precede the very reply whose drop it reports. Every existing serve.rs assertion is audited (see the audit block in D4): the two unit tests (queue-full and gap-shape) are unaffected; the integration tests contain **no reply-before-event ordering assumption** — verified by reading them: `a_client_that_never_reads_does_not_stall_the_daemon` (deliberately asserts nothing about lag count), `a_client_that_never_reads_is_throttled…` (asserts one `hello`, non-order), `two_sessions_on_two_sockets…` (asserts absence of foreign frames), `repeated_disconnects…` (`spawn_count`, no interleaving), `a_session_streams_events…` (`recv_until` by matcher), the `starting`-set test (`watching.insert` is on the read side, unaffected), `two_connections_racing…` (pair-wise). The `Client::delivered` echo (`serve.rs:557-745`) is a matcher-driven scan and never depends on delivery order. **No test asserts ordering.**
- **`fulcrum/tests/worker_mode.rs` is in the owned set and its two `send_frame` call sites are enumerated** (SHOULD-FIX 4): `:1006` (unix leg of `serve_cli_binds_the_socket_it_was_given_and_answers_hello`) and `:1087` (windows leg), both via `dial_until_listening` (`:1138`) which returns a `Connection`. Each converts to `let (mut conn, mut cw) = conn.split_writer(); cw.send_frame(&hello)` — the writer is dropped at the end of the test, same as today's `Connection` (dropping both halves still closes the socket; the lease semantics carefully spelled out in B2b/B3).
- **The Step-1 two-writer proof now has a BEHAVIOURAL pin** (SHOULD-FIX 5; round-2 🟠 amendment — a shared-writer regression pin that actually goes red is added in Step 1). The pairwise `send_frame` from two `ConnectionWriter`s on one peer, asserting the bytes arrive un-interleaved. The `#[should_panic]` on a second `split_writer` is kept as a cheap structural pin — it catches a `Mutex<ConnectionWriter>` + re-added `send_frame` — but the compile-error-negative framing is dropped (a test file that fails to compile aborts the whole integration binary; verifiable against `#[should_panic]`'s own semantics). The plan also adds a `split_writer`-returns-pair runtime test (see Step 1).
- **Queue bounds re-derived** (SHOULD-FIX 6): `CLIENT_QUEUE_CAPACITY`'s reasoning now stands on the write-path mutation — see D6. The throttle test's `spawn_count()` assertion (serve.rs:1663) is the sqm: with the two-task split the reader still `recv()`s and the writer still drains; the throttle measurement is still valid, and the `spawn_count()` still tightens to 3.
- **One task, two commits** (SHOULD-FIX 7): the frame split (frame.rs/mod.rs + transport tests) lands first, the `serve.rs` split second; commit message pairs and T4b-serial-on-T4a are spelled out in Step 11.
- All of `gap_frame`/`enqueue`/`watched`/`dropped` and the 4096/1024 constants are preserved; no new dependency; `#[cfg]` discipline under `-D warnings` on three OSes; Windows is compile-gated only, never claimed as runtime proof; `Connection::new`'s signature is UNCHANGED (a change is a `DEVIATION:` report).

---

## Parallelisation

**Verdict:** SERIAL — this plan is a single task (consumed spec mode plans exactly one task; parallelism in this run happens between tasks in the waves `roadmap` owns). This task edits `fulcrum/src/serve.rs`, `fulcrum/src/transport/frame.rs`, `fulcrum/src/transport/mod.rs`, `fulcrum/src/transport/unix.rs`, `fulcrum/src/transport/windows.rs`, `fulcrum/tests/transport.rs`, `fulcrum/tests/serve.rs`, `fulcrum/tests/worker_mode.rs`, `src/fixtures/worker.rs` — all in the same owned set — and its steps build on each other (frame test → frame split → serve split), so it cannot be subdivided into waves.

Emit no `**Parallel:**` line (single task).

---

### Task 1: Split `Connection` so the writer half exclusively owns `pending`, then split `serve_client` into reader and writer tasks

**Parallel:** none — single task.

**Context:**

`transport::frame`'s own module doc and `serve_client`'s "KNOWN LIMITATION" block (serve.rs:1145-1180) document the hazard this work exists to respect: a JSONL stream cannot resynchronise once two writers interleave a half-written line, so `Connection::pending` (the tail of a frame whose `write` was only partially accepted) must be reachable by exactly ONE writer. Today `pending` lives on `Connection` and both production `send_frame` call sites (serve.rs:1273 and :1278 — the gap-notice and the queue drain) are the same single `&mut connection` inside one task. Those two call sites are why `serve_client` runs `handle_frame(…).await` inline and stalls its own relay for up to 5 s: there is no second task that could safely own a second write handle.

This task (1) makes the framing structurally safe to split (writer half owns `pending`), and (2) uses that to split `serve_client` into a reader/reply task and a writer task. The writer task becomes the SOLE `send_frame` owner; the reader task owns the inbound half, the `watching` set, `handle_frame`, and (per T3, see the T3 marker below) T3's handshake/rate/session-limit state.

**⚠ T3 dependency and the split's T3-visible surface.** This task builds on `serve_client` AS IT EXISTS AFTER T3 (wave 1) lands. T3's exact committed shape is not known yet; per the dispatch this plan is written against the CURRENT tree and marks the T3-owned region explicitly "relocate into reader half". When T3 lands, this plan must be re-dispatched (refined in place) against T3's real `serve_client` before implementation. The T3-owned starter region — required-`hello` state, per-connection frame-rate state, session-limit enforcement (the `claim_session` bound) — belongs with the READER half, because the reader is the half that sees inbound frames. The two tasks share only `dropped` and `watching` (both behind an `Arc<StdMutex>`, D2); the writer cannot go anywhere without the reader declaring where the T3 state lives.

**Critical preconditions from the dispatch (verified against this tree):**

1. **There is NO existing behavioural pin to inherit — CONFIRMED.** `grep -rni "partial"` over `fulcrum/src/transport/` and `fulcrum/tests/` matches only doc prose and `serde` derive hits; there is no `pending`-loop test and no two-writer test anywhere. The acceptance-criterion "existing partial-write tests still pass" is **vacuous** as it stands. **Writing the failing-frame test is therefore the FIRST implementation step** (Step 1 below), before any split. A split that ships without a test that fails when `pending` is shared is shipping the hazard (the exact class of desync `frame.rs` exists to prevent) back.
2. **`Connection::new`'s signature is the seam T3 was told not to touch.** This plan keeps `Connection::new`'s signature and its `pub(crate)` visibility **unchanged** (Step 5). If implementation finds the signature must change, that is a `DEVIATION:` to report to the caller's vet, not a silent edit.
3. **The writer task must not leak when the connection dies.** The daemon has NO shutdown path (`serve::run` never returns, `reap_all` unwired; grounding §2 constraint 3). The split must run the writer as a child spawned inside `serve_client` (not a detached peer of `serve_client`), so when the reader exits, `serve_client` reaps it with `writer_task.abort()` (D1/D3 — the reader-first path has no other writer exit). This keeps T1's process census (same branch) green: no task may outlive `serve_client`'s return.
4. **`-D warnings` across three OSes:** any import used only under `#[cfg(unix)]` is an error on the Windows leg. Keep the two platforms' `Connection::new(stream)` construction sites exactly where they are; the `tokio::io::WriteHalf`/`mpsc` types used in the new writer type are all platform-neutral.
5. **Windows cannot run these tests locally** — `transport/windows.rs` is type-checked cross-target, never executed; a Windows-side test is a compile gate, not proof. The frame tests below run on unix (the dev machine is Linux).
6. **Keep the reader-task rule and the `MAX_*` bounds intact.** `MAX_INBOUND_LINE_BYTES` (pub) and `MAX_QUEUED_INBOUND_FRAMES` (private) and the `read_capped_line_under` boundary tests all stay exactly as they are.

**Files:**

- Create: (none)
- Modify:
  - `fulcrum/src/transport/frame.rs`
  - `fulcrum/src/transport/mod.rs`
  - `fulcrum/src/transport/unix.rs` (only if `Connection::new`'s signature changes — it should NOT)
  - `fulcrum/src/transport/windows.rs` (only if `Connection::new`'s signature changes — it should NOT)
  - `fulcrum/src/serve.rs`
  - `fulcrum/tests/transport.rs`
  - `fulcrum/tests/serve.rs`
  - `fulcrum/tests/worker_mode.rs`
  - `src/fixtures/worker.rs` (the `--slow-ready <ms>` hook — now in this task's owned set)
- Test: `fulcrum/tests/transport.rs`, `fulcrum/tests/serve.rs`, `fulcrum/tests/worker_mode.rs` (the new failing-frame test, the slow-start relay test, and the conversions all live in the existing test files; no new test file)

All paths verified on disk at plan time (tree clean at `a3f76c0` on `feature/daemon-hardening`; no external edits).

**What to implement:**

#### Part A — `fulcrum/src/transport/frame.rs`: the writer split

**A1. New public type `ConnectionWriter`** (in `frame.rs`, next to `Connection`):

```rust
/// The WRITE half of a framed connection, with the unwritten tail of a
/// partially-accepted frame. Owns `pending` EXCLUSIVELY — it is the only
/// type whose methods can reach the bytes of a half-sent frame. See
/// [`Connection::split_writer`] for why the two writers may never be
/// shared.
pub struct ConnectionWriter {
    writer: tokio::io::WriteHalf<DuplexBox>,
    pending: Vec<u8>,
}

impl ConnectionWriter {
    pub async fn send_frame<T>(&mut self, frame: &T) -> Result<(), TransportError>
        where T: Serialize + ?Sized
    // body: EXACTLY the current `Connection::send_frame` body; `pending` and
    // `writer` are the struct's own fields. The method keeps today's
    // cancellation-semantics doc inline (the `select!` over a send caveat stays).
}
```

- The `send_frame` body is a verbatim move from the current `Connection::send_frame` (frame.rs:263-282): serialize to `String`, push `\n`, `self.pending.extend_from_slice(line.as_bytes())`, loop `self.writer.write(&self.pending)` draining what was accepted, `Ok(0)` ⇒ clear + `Err(WriteZero)`, `Err(e)` ⇒ `e.into()`, then `self.writer.flush().await?`.
- **A1b (NEW — the teardown load-bearing exit).** Add a `pub(crate)` close method to `ConnectionWriter`:

```rust
/// Ask the OS to close the whole duplex, even while the read half is
/// alive. `tokio::io::split`'s `WriteHalf` holds a SHARED `Arc<Inner<T>>`
/// and has NO `Drop` impl, so dropping it does NOT close a socket while
/// the read half lives (verified in tokio 1.54 `src/io/split.rs`); a
/// writer parked inside `send_frame`'s write loop would stall there
/// forever. `poll_shutdown` on the shared stream is what makes FUTURE
/// `write`s fail and tells the peer that this direction is over (EOF) —
/// it does NOT reliably un-park a `write` already blocked inside the OS
/// (shutdown(2) semantics: the wake latency of a parked send is
/// kernel-dependent), so this method is the direction-closer + the
/// peer-EOF signal, NOT a guaranteed parked-write breaker. For the
/// daemon's connections the writer's parked `write` is broken by the
/// PEER'S CLOSE when a connection ends; see D3.
///
/// MUST be called by the exiting writer AFTER `send_frame` returns (never
/// from inside a `select!`, see A1c); MUST be called by the reader before
/// `split_writer` (Part D's step 1), because it needs the write half still
/// inside this connection. It is a best-effort close: an `Err` is
/// ignored, the callers exit on their own schedules regardless.
pub(crate) async fn shutdown(mut self) {
    let _ = self.writer.shutdown().await;
}
```

This shutdown is the daemon's analogue of dropping a whole `Connection` today — it does not replace the reader's own `recv` EOF (the peer closing inbound still ends the reader). It is the ONLY new method the writer needs for teardown.

- **A1c (NEW — no flush in the select).** Because an await inside the selector arms must never park the writer's loop: the plan requires BOTH of `send_frame`'s awaits (the write loop AND the final `flush`) to be cancellable from the writer's own exit, and it makes the final `flush` a one-liner that a `select!` CAN poll. Since `send_frame`'s body is otherwise a verbatim move (its `flush` is a no-op on a socket), the writer loop's DRAIN arm (D3) will call `send_frame` OUTSIDE the `select!` — via a small wrapper future `send_frame_no_select(&mut writer, frame)` that the `select!` treats as a plain future. See D3's exact shape. This is the one place the plan deviates from a bare move; the deviation is purely mechanical (the flush becomes its own awaitable) and the executable guarantee (a parked block cannot outlive the writer task) is the pin the reader tests in Step 1.

- **No custom `Drop` on `ConnectionWriter`.** Dropping it drops the `WriteHalf` (a no-op on the shared stream — see A1b's correction); there is no reader task to abort. Do not add a `Drop` impl.

**A2. Strip the writer out of `Connection`** — the struct becomes the READER half (plus the dormant writer slot for `split_writer`):

```rust
pub struct Connection {
    /// The write half, held ONLY so `split_writer` can hand it out once.
    /// `None` after `split_writer`; this type has no methods that write.
    writer: Option<tokio::io::WriteHalf<DuplexBox>>,
    lines: mpsc::Receiver<Result<serde_json::Value, TransportError>>,
    reader: tokio::task::JoinHandle<()>,
}
```

- **Removed fields:** `pending: Vec<u8>` moves into `ConnectionWriter` (the structural guarantee — `pending` is reachable ONLY there); `writer: tokio::io::WriteHalf<DuplexBox>` becomes `writer: Option<tokio::io::WriteHalf<DuplexBox>>` on `Connection` (the dormant slot `split_writer` takes, A3).
- `Connection::new<S>(stream: S) -> Self` keeps its signature and `pub(crate)` visibility UNCHANGED. Its body stores the split's write half as `writer: Some(write_half)` (see A3) and otherwise runs the reader unchanged: the `mpsc::channel(MAX_QUEUED_INBOUND_FRAMES)`, the `tokio::spawn` reader with its `read_capped_line` loop and its five `LineRead` arms, the `trim_end`/UTF-8/empty-line handling, the `Drop { reader.abort() }`, the manual `Debug` — all unchanged. The doc comment on `new`'s split step must be updated to say the write half is kept in the struct ready for `split_writer` (NOT spawned anywhere, NOT written from this half).
- **A2b (NEW — teardown / `split_writer` / `take_writer`).** To let the reader shut the socket down BEFORE the writer is extracted (Part D's step 1), add this one method:

```rust
impl Connection {
    /// Best-effort shutdown of the whole duplex, write half still held.
    /// MUST be called before the FIRST `split_writer`, and ONLY once
    /// (a second call's `Option::take` returns `None` — the caller must
    /// not rely on it). See [`ConnectionWriter::shutdown`] for why a
    /// dropped write half alone cannot close the socket.
    pub(crate) async fn shutdown_while_writer_intact(&mut self) {
        if let Some(w) = self.writer.as_mut() {
            let _ = w.shutdown().await;
        }
    }
}
```

The `Option` is what makes "called at most once" a type-level fact. The reader loop calls it BEFORE `self.split_writer()` in the serve split (D1/D3); the reader's own `recv` EOF path does NOT call it (a peer that closed already unwakes the writer's write on its own).

**A3. Recover the writer on demand, once:**

```rust
impl Connection {
    /// Return the connection's write half (and its exclusive `pending`),
    /// leaving this `Connection` owning only the reader.
    ///
    /// May be called at most ONCE. The returned `ConnectionWriter` is the
    /// ONLY way to send frames on this connection; there is no method on
    /// `Connection` that writes. A second call is a caller bug and panics
    /// (this is the same trust `tokio::io::split` gives).
    pub fn split_writer(mut self) -> (Self, ConnectionWriter) { /* … */ }
}
```

- `split_writer` returns both halves; the daemon task keeps the reader half (its `recv` is the connection's inbound) and hands the writer to the writer task; `serve.rs` keeps both (D1).
- **One concrete design, chosen here so the implementer does not improvise:** add a private field `writer: Option<tokio::io::WriteHalf<DuplexBox>>` to `Connection` (replacing today's bare `writer: tokio::io::WriteHalf<DuplexBox>`), and implement:
  ```rust
  pub fn split_writer(mut self) -> (Self, ConnectionWriter) {
      let writer = self.writer.take().expect("the writer was already taken");
      (self, ConnectionWriter { writer, pending: Vec::new() })
  }
  ```
  The `Option` means an abandoned `Connection` still drops its `Some(writer)` and closes that direction when the reader half is dropped (best-effort — the write half drop is a no-op on the shared stream, but the reader task's own `Drop::abort` still stops the task). A SECOND `split_writer` call hits the `expect` and panics — that panic IS the two-writer hazard surfacing structurally (the `#[should_panic]` regression test in Part E pins it). The frame.rs unit-test module must not construct `Connection{ writer: None, … }` directly; tests that need both halves call `split_writer`.
- **API surface note:** this is the ONE genuinely new public item on `transport` (the split's reason for existing). The `boundary.rs` source census (`tests/boundary.rs`, which walks `fulcrum/src` and asserts module allow-lists and a `4`-file `transport/` floor) checks PATHS and declared modules, NOT symbols — `split_writer` and `ConnectionWriter` do NOT trip it (verified: `transport` stays 4 files, `frame.rs` stays one file; boundary.rs has no symbol list that mentions `send_frame`/`pending` — grep confirms). `tests/boundary.rs` is NOT in the owned set and is NOT modified.

#### Part B — `fulcrum/src/transport/mod.rs`: one re-export

- Add one line next to the existing `pub use frame::Connection;` (line 76):
  ```rust
  pub use frame::{Connection, ConnectionWriter};
  ```
- `pub use unix::{connect, default_endpoint, Listener};` / `pub use windows::{connect, default_endpoint, Listener};` are unchanged.
- The `Endpoint` / `TransportError` enums are unchanged.

#### Part C — `fulcrum/src/transport/unix.rs` / `windows.rs` (no change — verify only)

- `Listener::accept -> Result<Connection, TransportError>` keeps its exact shape on both platforms; the construction sites `Ok(Connection::new(stream))` (`unix.rs` accept + connect) and `Ok(Connection::new(server))` / `Ok(Connection::new(client))` (`windows.rs` accept (in `connect`'s body) + connect) are UNCHANGED. **No `Connection::new` call site changes**, so these two files should receive zero edits. If `Connection::new`'s signature were found to need changing, that is a `DEVIATION:` (Step C checks).

#### Part D — `fulcrum/src/serve.rs`: the two-task split

**D1. The reader/reply task and the writer task.**

```rust
async fn serve_client(daemon: Arc<Daemon>, connection: Connection) {
    // The one piece of state both tasks touch: the dropped-frame counter.
    // `enqueue`'s `&mut u64 dropped` is kept; the tasks share the counter
    // through `enqueue_shared` (D2). *The reader never waits on the writer*
    // — the only way to stop a writer parked inside `send_frame` is to
    // close the socket under it (see D3), and the ONLY way to close the
    // socket while the write half is still alive is the shutdown below,
    // BEFORE the split hands the write half to the writer task.
    let mut reader_conn = connection;
    reader_conn.shutdown_while_writer_intact().await;

    let (connection, writer) = reader_conn.split_writer();

    let dropped: Arc<StdMutex<u64>> = Arc::new(StdMutex::new(0));
    let watching: Arc<StdMutex<HashSet<String>>> = Arc::new(StdMutex::new(HashSet::new()));

    // == T3 RELOCATE INTO READER HALF ==
    // T3 (wave 1) inserts its starter region HERE (behind the split): the
    // required-`hello` state, the per-connection frame-rate state, and the
    // session-limit enforcement (owning `claim_session`). All of it belongs
    // with the READER half (the half that sees inbound frames), declared
    // BEFORE the two tasks spawn so the reader can own it. When T3's real
    // shape lands, re-dispatch this plan against it; today this marker is the
    // contract that the T3 state lives in the reader task, never the writer.
    // == /T3 RELOCATE ==

    // The outbound queue (replies + relayed events), BOUNDED — see
    // [`CLIENT_QUEUE_CAPACITY`]. Every push is a `try_send` via `enqueue`.
    // `Sender` is `Clone`: BOTH tasks hold one (the reader for replies, the
    // writer for the relay) — so the channel can NEVER close by dropping a
    // single sender. Teardown is drop-based (D3), never a join: the reader
    // returns when its `recv` EOFs and DROPS its sender; the writer exits
    // when its `queued.recv()` returns `None` (the reader's sender gone) or
    // when a `send_frame` errors (peer gone) or when the fan-out closes; and
    // the one exit NO writer path reaches — the peer closed its INBOUND half
    // but left the write direction open, so no error fires — is reaped by
    // `serve_client`'s `writer_task.abort()` (step 4 below) the moment the
    // reader's task returns.
    let (tx, queued) = tokio::sync::mpsc::channel::<serde_json::Value>(CLIENT_QUEUE_CAPACITY);

    // THE RELAY LIVES IN THE WRITER TASK — this is the whole fix. The reader
    // awaits `handle_frame` (a `start`'s `attach` for up to 5 s); if the
    // fan-out arm shared the reader's `select!` it would still stall for that
    // window. Only the task that never calls `handle_frame` can carry the
    // relay, so the writer does: fan-out → `watched` → `enqueue`, and
    // `queued` → `send_frame`.
    let reader = tokio::spawn(reader_loop(
        Arc::clone(&daemon),
        connection,
        tx.clone(),
        Arc::clone(&dropped),
        Arc::clone(&watching),
    ));
    let writer_task = tokio::spawn(writer_loop(
        writer,
        queued,
        tx,
        daemon.fan_out.subscribe(),
        Arc::clone(&dropped),
        watching,
    ));

    // The reader's whole life-span — `await`, NOT `select!`: the reader's only
    // input is the inbound channel, which is why the split does not need it to
    // share its loop with the relay. This await can park for up to
    // `attach`'s ~5 s (a `start`); that is the bounded per-connection stall
    // issue #3 accepts, and it is exactly why the relay lives in the writer
    // (see D1's relay note).
    reader.await.expect("reader_loop must not panic");

    // ALL of the writer's exits are its own (D3). This abort is the backstop
    // for the ONE exit no writer path reaches: a peer that closes its INBOUND
    // half but leaves the write direction open (so the writer's `send_frame`
    // never errors) parks the writer inside the `send_frame` write loop for as
    // long as the peer's read side stays shut. Abort — NOT join: the writer is
    // never awaited (joining a writer parked inside `send_frame` hangs — D3),
    // and `abort` makes the parked write loop's drop SYNCHRONOUS, so no
    // `Drop`-time `flush` is ever awaited (a flush on a shutdown-locked fd
    // would itself hang). Giving the task's future an async drop is exactly
    // what we must NOT do. The abort fires on EVERY exit from
    // `serve_client`, reader-first or not; for a writer that already terminated
    // it is a no-op (the task is done). This is the round-2 🔥 replacement for
    // the retired "drop the handle, never joined" claim.
    writer_task.abort();

    // `serve_client` returns; its `Arc<Daemon>` refcount drops. No `stop`
    // one-shot, no join, no timeout: every prior teardown shape deadlocks
    // against a writer parked inside `send_frame` on a peer that stopped
    // reading (CRITICAL 1 — see D3's analysis).
}
```

- `serve_client` remains `async fn serve_client(daemon: Arc<Daemon>, connection: Connection)` (non-mut `Connection` — no method needs `&mut` after the split). `run`'s `tokio::spawn(serve_client(daemon, connection))` at `serve.rs:1119` is UNCHANGED.
- **Reader/reply task (`reader_loop`, spawned at D1)** — owns the (now reader-half) `Connection`, `watching`, `seen`, its `tx` clone, and a reference to the shared `dropped`. Its loop is a plain `while let Some(frame) = connection.recv().await` (a single future, no `select!` — the reader's only input is the inbound channel):
  - `seen += 1`.
  - Malformed line (`Err(e)`) ⇒ `enqueue_shared(&tx, error-frame, &dropped)`; on `false` (closed) break.
  - `Ok(frame)` ⇒ `handle_frame(&daemon, &mut *watching.lock().unwrap_or_else(|p| p.into_inner()), &frame).await` INLINE (this await may park for `attach`'s ~5 s — that is fine now, because the RELAY and the SOCKET WRITE moved to the writer; the reader blocking itself is exactly the bounded per-connection stall the issue accepts) ⇒ `enqueue_shared(&tx, reply, &dropped)`, break on `false`.
  - `recv() == None` (EOF — the peer closed its inbound half) ⇒ the raw-bytes drain (next bullet), THEN break.
  - **🔥 Round-2 reader-first-exit detail (the reason `serve_client` holds and aborts the writer's handle):** on `recv() == None` the reader does NOT wait for the peer to tell anyone else — it drains the raw stream to EOF before breaking:
    ```rust
    // The peer closed its INBOUND half (we saw EOF). Convey EOF to the
    // WRITE direction too, so the daemon-side socket shuts down fully:
    // read raw bytes until EOF or error. This is what makes the peer
    // socket's whole duplex close (both `poll_shutdown`es fire), and it is
    // what gives the WRITER its `send_frame` error / `queued.recv()==None`
    // exits on the reader-first path — the writer is NOT left parked in
    // `send_frame` on an open write direction (the 🔥 invariant, Step 1).
    let _ = tokio::io::AsyncReadExt::read_to_end(&mut connection).await;
    ```
    `read_to_end` is the holder of the reader half's `ReadHalf`; the writer is only ever woken through its OWN `send_frame` error or its `queued.recv() == None` — nothing here blocks on the writer. The read half's last owner (the `Connection` in `reader_loop`, plus this drain) lives exactly as long as `reader_loop`; after it, the read half drops, the write half's `Arc` remains (the writer holds it), and the reader HALF'S `poll_shutdown` has already fired from the drain's EOF/error — which is what makes the writer's next `write` fail (a peer socket that fully shut down is a write error, not a hang). Between this drain and the abort, the writer sees the socket die (write error) or the (already-dead) `recv` channel; either way it exits without the abort's help in the common case — the abort is the mechanical backstop the Step-1 test proves.
  - On break: **drop `tx`** (NOT a stop signal — see D3), then the epilogue: the `seen > 0` disconnect line (`"fulcrum serve: client disconnected after {seen} frame(s)"`) and NOTHING else (no `detach`, no `kill`, no join — the write-side EOF is the raw-bytes drain above).
- **Writer task (`writer_loop`, spawned at D1)** — owns `ConnectionWriter` (exclusively), the `queued` receiver, its `tx` clone (for the relay arm's `enqueue`), the fan-out subscription, and the shared `dropped`/`watching`. Runs the RELAY and the DRAIN in one `select!`:

```rust
/// The writer half of the split `serve_client`: the relay and the socket.
///
/// THE ONLY task that calls `send_frame` — by construction
/// (`ConnectionWriter` is not `Clone` and `send_frame` takes `&mut self`).
/// It never calls `handle_frame`, so `attach`'s ~5 s can never park it: while
/// the reader awaits a `start`, this task keeps enqueueing the fan-out and
/// draining the queue.
///
/// # Why the writer is never joined — and what terminates it
///
/// `send_frame` loops `self.writer.write(&self.pending)` with no timeout
/// (frame.rs:274-282). A peer that stops reading parks that loop at the OS
/// send buffer, and nothing in `ConnectionWriter`'s own `select!` — the writer
/// CANNOT poll its other arms while its current `send_frame` future is parked
/// — can wake it. So the reader NEVER awaits this task's `JoinHandle` (a join
/// would hang), and `serve_client` holds the handle and `abort()`s it the
/// moment the reader's task returns (D1). The exits the writer can reach on
/// its own, in order of likelihood:
///
/// 1. `send_frame`/`send_frame_no_select` returns `Err` — the peer is gone
///    (its close, the reader's pre-split shutdown, or the reader-half EOF
///    drain's shutdown unwoke the write; on the reader-first path the peer
///    socket is fully down, so the write errors).
/// 2. `queued.recv() == None` — the channel is closed and empty: the reader's
///    `tx` is gone, and the writer's OWN `tx` (held for the relay arm) is
///    dropped no later than this task's return. (On the abort path this never
///    fires — the task is merely un-parked, see below.)
/// 3. fan-out `Closed` — the [`Daemon::fan_out`] sender's broadcast is gone.
///
/// Every exit path calls `writer.shutdown()`, updates the `dropped` epilogue
/// if needed, and returns; its `tx` and `ConnectionWriter` drop with it.
///
/// The one exit NO writer path reaches is the peer that closed only its
/// INBOUND half and left the write direction open: no error fires in
/// `send_frame` and `recv` never returns `None` for the writer's queue until
/// the reader's sender drops. That path terminates from OUTSIDE: the reader's
/// EOF drain shuts the read half down, and `serve_client` `abort()`s the
/// task's `JoinHandle` when the reader returns. The abort is what makes the
/// parked write-loop's drop SYNCHRONOUS — the task's future is dropped without
/// polling it, so its async `Drop` never runs, so the `flush` its drop would
/// have awaited is never awaited. This is the round-2 🔥 invariant: **a writer
/// parked inside `send_frame` never outlives its `serve_client`** (Step 1's
/// test proves it; D3 spells out who drops what, in what order).
async fn writer_loop(
    mut writer: ConnectionWriter,
    mut queued: tokio::sync::mpsc::Receiver<serde_json::Value>,
    tx: tokio::sync::mpsc::Sender<serde_json::Value>,
    mut events: tokio::sync::broadcast::Receiver<serde_json::Value>,
    dropped: Arc<StdMutex<u64>>,
    watching: Arc<StdMutex<HashSet<String>>>,
) {
    loop {
        tokio::select! {
            // The fan-out relay (moved verbatim from today's fan-out arm).
            fan = events.recv() => match fan {
                Ok(frame) => {
                    let wanted = watched(&frame, &*watching.lock().unwrap_or_else(|p| p.into_inner()));
                    if wanted && !enqueue_shared(&tx, frame, &dropped) {
                        break; // this connection's own queue is gone
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    eprintln!("fulcrum serve: a client lagged {skipped} frames");
                    if !enqueue_shared(&tx, gap_frame(skipped), &dropped) {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    // The fan-out source is gone (only reachable when the
                    // whole `Daemon` is dropped — no shutdown path today);
                    // end the connection like today's Closed arm.
                    break;
                }
            },
            // The queue, drained to the socket — today's drain arm, with the
            // gap-prelude intact. The send is wrapped so the select can poll
            // it: `send_frame`'s write loop is a plain future, and the final
            // flush is a no-op on a socket (A1c).
            queued_frame = queued.recv() => {
                let Some(frame) = queued_frame else {
                    break; // the reader's sender is gone — nothing left to send
                };
                let mut dropped_guard = dropped.lock().unwrap_or_else(|p| p.into_inner());
                if *dropped_guard > 0 {
                    let skipped = std::mem::take(&mut *dropped_guard);
                    drop(dropped_guard); // never hold the guard across an await
                    eprintln!("fulcrum serve: a client lagged {skipped} frames");
                    if send_frame_no_select(&mut writer, &gap_frame(skipped)).await.is_err() {
                        break; // the peer is gone (EPIPE / a closed socket)
                    }
                } else {
                    drop(dropped_guard);
                }
                if send_frame_no_select(&mut writer, &frame).await.is_err() {
                    break;
                }
            }
        }
    }
    // The writer's exit epilogue: it owns `dropped`, so the "disconnected N
    // frame(s) behind (it was never told)" line for a client that never heard
    // its gap logs here (the reader no longer holds the counter).
    let still_dropped = *dropped.lock().unwrap_or_else(|p| p.into_inner());
    if still_dropped > 0 {
        eprintln!("fulcrum serve: a client disconnected {still_dropped} frame(s) behind \
                   (it was never told)");
    }
    // THE teardown close (A1b): the write direction asks the OS to close —
    // FUTURE writes error and the peer sees EOF. (It does NOT reliably
    // un-park a write already blocked in the OS, round-2 🟡; the reader's
    // pre-split shutdown and the peer's close are what un-park in a real
    // connection. The writer calls it as best-effort cleanup on every exit.)
    writer.shutdown().await;
}
```

  - **`send_frame_no_select`** is a tiny private free function in `serve.rs`:
    ```rust
    /// `ConnectionWriter::send_frame`, re-shaped so the writer's `select!`
    /// can poll it: the final `flush` (a no-op on a socket — the write half
    /// is unbuffered) becomes its own await, so the whole call is one
    /// cancellable future instead of two awaits the selector cannot reach
    /// past (A1c).
    async fn send_frame_no_select(writer: &mut ConnectionWriter, frame: &serde_json::Value)
        -> Result<(), TransportError>
    {
        writer.send_frame(frame).await
    }
    ```
    It exists to make the teardown promise executable rather than argued: the writer's ONLY await inside the `select!` is `queued.recv()`/`events.recv()`; `send_frame` is a future the selector can drop when a competing arm fires. (`send_frame_no_select` is new and private; it is not `send_frame`'s replacement — `send_frame` itself stays on `ConnectionWriter`.)
  - **State note — `watching` crosses the tasks (the one place the two halves must not both touch it):** the writer's relay arm filters on `watching`, but `watching` is MUTATED by `handle_frame` (reader side: `start` inserts, `close` removes). If `watching` were a plain `HashSet` in the reader, the writer could not read it. **The split's design decision: the relay filter stays with the reader is WRONG (it would re-stall); so `watching` must be shared read-only from the writer. Use the daemon's existing precedent — an `Arc<StdMutex<HashSet<String>>>`, the same pattern as `Daemon::subagents`/`starting`.** The reader owns it (mutates inside `handle_frame`); the writer's relay arm does a `watched(&frame, &watching.lock())`. Lock hold is contention-free (per-connection, two tasks, microseconds) and never spans an await (read the filter result, drop the guard, then `enqueue`). This is the ONLY shared per-connection state besides `dropped`; the "two tasks may not both touch the write half" invariant is untouched because neither task touches `pending` except the writer via `ConnectionWriter`.
  - **`enqueue`'s signature is a PIN (`&mut u64 dropped`); do not change it.** Both shared counters use a 3-line adapter at each call site:
    ```rust
    // in serve_client's scope
    fn enqueue_shared(
        tx: &tokio::sync::mpsc::Sender<serde_json::Value>,
        frame: serde_json::Value,
        dropped: &StdMutex<u64>,
    ) -> bool {
        enqueue(tx, frame, &mut *dropped.lock().unwrap_or_else(|p| p.into_inner()))
    }
    ```
    `enqueue_shared` is a NEW private helper in `serve.rs`; `enqueue` itself and its two unit tests are byte-identical. (`enqueue` stays reachable — `enqueue_shared` calls it — so no dead-code warning.)
  - **The `gap` notice's TWO emission paths survive intact:** the fan-out `Lagged(skipped)` arm (writer, enqueues `gap_frame(skipped)` as its own frame, NOT folded into `dropped` — same reasoning as today's comment) and the drain-arm prelude (writer, announces `dropped` before the next delivered frame). The frame shape (`{"type":"gap","skipped":N}`, no `sessionId`) is unchanged.

**D2. The two shared counters — `dropped` and `watching` — and why the relay MUST be in the writer.**

- **`dropped`:** shared via `Arc<StdMutex<u64>>` (recommended — the daemon already uses `StdMutex` for `Daemon::subagents`/`starting`, so the pattern is in-repo precedent), accessed only through `enqueue_shared` / explicit short lock-guard scopes that never span an await (the `send_frame` calls sit outside the guard). The channel-based alternative (reader forwards its count to the writer, `enqueue_shared` unused) is acceptable ONLY if it keeps `enqueue`'s signature and both unit tests unchanged; default to the `Arc<StdMutex>` form and note the choice in the commit.
- **`watching`:** `Arc<StdMutex<HashSet<String>>>`, per the state note above. Reader mutates inside `handle_frame` (keep `handle_frame`'s `&mut watching: &mut HashSet<String>` signature — the unit tests call it with a plain `HashSet`); the reader passes `&mut *watching.lock()` where it currently passes `&mut watching`. The writer reads it (`watched(&frame, &watching.lock().unwrap_or_else(|p| p.into_inner()))`).
- **Why the relay is in the writer (the load-bearing placement):** the reader's `handle_frame` await parks the READER for up to ~5 s on `start`. If the fan-out arm lived in the reader's `select!`, the relay would stall for exactly that window — the split would be decorative. The writer never awaits `handle_frame`; its `select!` is over ready-to-run futures (`events.recv()`, `queued.recv()`) plus `send_frame` calls that are cancellation-safe (never abandoned — see D3's no-join rule; the writer owns its own `ConnectionWriter`). The relay therefore runs throughout a slow `start`.
- **T3 marker:** T3's per-connection state (handshake, frame-rate) stays in the READER task (D1 marker) AND — with the relay now in the writer — T3 must be told that the fan-out arm no longer exists in `serve_client`'s single `select!` (it moved to `writer_loop`). T3's per-frame rate limiting applies at the INBOUND edge (reader), which is unaffected. Note this in the re-dispatch.

**D3. Teardown — the honest, deadlock-free, drop-based contract (CRITICAL 1 fix).**

The vet's 🔴 finding, verified against the code, and why every "join the writer" shape is wrong:

1. **A reader `await`-ing the writer's `JoinHandle` after a `stop` one-shot deadlocks.** `ConnectionWriter::send_frame` loops `self.writer.write(&self.pending).await` with NO timeout (`frame.rs:274-282`; verified — no `timeout`, no `select!` inside `send_frame`). A peer that stopped reading parks that loop at the OS send buffer. While the future is parked, NONE of the writer's `select!` arms is pollable, so `stop` is unreachable, the join never resolves, and the `serve_client` task hangs forever. A hung task is indistinguishable from a leaked process to T1's process census running on this same branch — the integrated gate would redden.
2. **The mirror path is equally bad.** Once the fan-out `Receiver` yields `Closed` (only on a full daemon teardown — none today), the writer has no non-error exit, and the same join hangs.
3. **"Dropping the `ConnectionWriter` closes the write direction" is FALSE in this design** — the vet's suggested mechanism, sharpened against tokio 1.54's actual `split` (verified in tokio 1.54.1 `src/io/split.rs:20-42`): `WriteHalf<T>` wraps `Arc<Inner<T>>` with **no `Drop` impl**, so dropping one half does NOT close the duplex while the other half's `Arc` is alive. The parked `write` would NOT error; a dropped `WriteHalf` alone cannot break it. (In today's single-task design, dropping the whole `Connection` drops BOTH halves and the reader task, which is why today's teardown works — the split must not pretend the old shape survives.)
4. **The guaranteed unblocker of a parked `write` is the PEER'S CLOSE — and that is exactly what ending a connection means.** A blocked `self.writer.write(&self.pending)` fails (EPIPE / ECONNRESET) the moment the peer closes its end, which is precisely the event that ends a connection in this daemon. The split introduces NO new way to stall: a peer that neither reads NOR closes can also stall today's single task in the same `send_frame` — the daemon deliberately has no send timeout (backpressure is the design), so an open-but-unreading peer stalling the writer is the SAME behaviour as today, and it is not a teardown case (the connection never ends; the test suite's daemon kill closes every socket, so T1's census sees no leak). For the cases where the connection DOES end, the writer's write unwakes on the peer's close alone. `AsyncWriteExt::shutdown` on the shared `Inner` (`poll_shutdown` in `split.rs:79-82`) closes the WRITE DIRECTION: FUTURE `write`s on this fd fail and the peer sees EOF. It does NOT reliably un-park a `write` already blocked inside the OS — the wake latency of an already-parked send is kernel-dependent, so the plan never relies on shutdown to BREAK a parked write (round-2 🟡 precision; the older "makes ANY pending-or-future write fail" phrasing is retired). The plan reaches shutdown in exactly two places (A1b, A2b): the reader before splitting (Part D1 step 1 — so the write half is still inside the connection and the call is possible; tokio's `unsplit` panics on a consumed half and a dropped half is a no-op, so there is no "close on drop" to fall back on), and the writer on every exit path.
5. **And the hole the four points above do NOT cover — the 🔥 round-2 fix.** A peer that closes ITS inbound half but leaves ITS write direction open (so the daemon's writer never sees a `send_frame` error) and never closes the TCP connection parks the daemon's WRITER inside `send_frame` indefinitely: its write loop is parked (send buffer full), and because the task is inside the future, NONE of its `select!` arms is pollable — so `queued.recv()==None` (the reader's `tx` drops when `reader_loop` returns), the fan-out and every other arm are all unreachable. The reader-HALF EOF drain (D1's `read_to_end`) shuts the read half down, but that is a read-side event — the writer's WRITE side is not awoken by it, only the abort is. So `serve_client` KEEPS the writer's `JoinHandle` and `abort()`s it after `reader_loop` returns (D1): the abort cancels the parked `send_frame` future — its `Drop` is SYNCHRONOUS (the task's future is dropped without polling it), so no `Drop`-time `flush` is ever awaited (a flush on a shutdown-locked fd hangs), and the loop is dead with `serve_client`. This is `abort`, not `join`: joining is what can never resolve; aborting is exactly what makes the parked loop's drop synchronous and gives "no task outlives `serve_client`" MECHANICAL enforcement. (The honest, avoided alternative: a spawn-outside-`serve_client` peer task with an explicit kill tok — rejected, it is quit-on-message choreography the abort makes unnecessary.)

**Who drops what, in what order (the tested invariant):**

- **Reader's exit (peer EOF inbound):** `recv() == None` → the raw-bytes `read_to_end` drain (D1) — the reader-half `Connection`'s last await, shutting the read half down → the reader breaks → drops its `tx` (its `Connection` drop then aborts the read task) → returns from `reader_loop`. The reader does NOT call the writer's `shutdown` (no path to it); its write-side EOF conveyance is solely the raw-bytes drain (the read-half close) plus the abort that follows in `serve_client`.
- **`serve_client` after `reader.await` returns (round-2 🔥):** `writer_task.abort()` — unconditionally, on every path out of `reader.await` (including the reader's early `break` exits, which all funnel through `reader_loop`'s return). The abort is a no-op for a writer that already terminated. `serve_client` then returns; its `Arc<Daemon>` refcount drops. It NEVER awaits the writer's `JoinHandle`.
- **Writer's own exits (any of — the round-2 🟡 trimmed list):** (a) `send_frame`/`send_frame_no_select` returns `Err` — the peer is gone (its full close, the reader's pre-split shutdown, or the read-half drain's shutdown woke the write with an error); (b) `queued.recv() == None` — the reader's `tx` is gone (this cannot fire on the reader-first path: the writer is inside, or has been aborted before, its own parked arm — see D3 point 5); (c) fan-out `Closed` — the [`Daemon::fan_out`] sender's broadcast is gone. **There is no "both senders gone" exit** — the writer's own `tx` never sends to a channel it alone receives from; exit (b) fires on the reader's sender alone, at which point the writer's own `tx` is dropped with the task itself (round-2 🟡 precision — the old phrasing said "both senders gone", which is unreachable). Every exit path then calls `writer.shutdown().await` (A1b), updates the `dropped`-count epilogue if needed, and returns; its `tx` and `ConnectionWriter` drop with it.
- **The no-join / abort rule and T1's census:** the writer is `tokio::spawn`ed INSIDE `serve_client`; `serve_client` holds the handle and `abort()`s it on every reader exit. The writer terminates on its own via one of the three exits above, or is aborted by the backstop — a peer that stops reading WITHOUT closing parks the writer exactly as it parks today's single task (backpressure is the design, not a defect the split introduces), and that is NOT a teardown case (the connection never ends); every REAL teardown un-parks or aborts the writer, so no task outlives `serve_client` — T1's process census (running on this same branch) stays green.
- **Tested, not assumed:** Step 1's frame tests pin (i) that a `send_frame` on a connection whose peer stopped reading errors once the socket is shut down; (ii) that the writer loop terminates without a join when the reader's `tx` is dropped; and (iii) — the round-2 🔥 pin — **a writer parked inside `send_frame` never outlives its `serve_client`**, tested end-to-end through the real `serve` binary (Step 1's recipe + Step 9's teardown half).
- **No timeout, no join, no `stop` one-shot.** Remove every `stop_tx` / `stop_rx` reference from the old plan's design. The `oneshot` channel is gone from this plan.

**D4. The `gap`-vs-reply ORDER contract (CRITICAL 3 fix).**

Today one task serialises `send_frame` calls, so a `gap` notice never interleaves into the reply stream; the reader's replies are enqueued AFTER `handle_frame` returns, and the drain emits them in FIFO order with `gap`s preluding the next frame. Under the split the reader enqueues replies (from the inbound arm) while the writer drains; the writer's drain arm announces `dropped` from its own counter. The explicit new contract, stated here and to be written into `serve_client`'s doc:

> **Post-split ordering contract.** A `gap` notice may interleave with replies — a connection that is both replying and dropping (a transiently-full queue) can see `gap` anywhere in the stream, including before the very reply that overfilled the queue. The `gap`'s count and its `type`/`skipped` shape are unchanged; its *position* is no longer guaranteed. The fan-out `Lagged`-arm `gap` is enqueued as an ordinary frame and is subject to the same queue as replies; the drain-arm prelude `gap` is announced from the queue head when the drain wakes with `dropped > 0` — exactly today's condition, minus the single-task serialisation that kept replies ahead of it. **A `gap` never precedes a frame that predates the drops it reports** (the queue is FIFO; a prelude `gap` leaks only drops that happened before this drain wake), but it may precede the reply whose send raced the drop.

- **Audit of every existing assertion that depends on reply-before-event ordering (result: none do — verified against the tree).** The full list read for this plan:
  - `a_full_client_queue_drops_counts_and_keeps_the_connection` (serve.rs:2453) — a pure `enqueue` unit test; no ordering, no `Connection`.
  - `the_gap_notice_carries_its_count_where_the_client_reads_it` (serve.rs:2502) — shape-only; no ordering.
  - `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat` (serve.rs:1714+): asserts `pushed < CEILING` and ONE `hello` reply after the client resumes — no ordering.
  - `a_client_that_never_reads_does_not_stall_the_daemon` (serve.rs:1821+): `recv_until_matching` on `agent_settled` for a HEALTHY client; the stalled client only has a `start-result` consumed and rows asserted — no ordering.
  - `two_sessions_on_two_sockets_do_not_see_each_others_events` (serve.rs:1227+): asserts the ABSENCE of foreign-session frames (`drain` window), and that each client's `recv_event_for` returns its OWN session's first event — no reply-vs-event order.
  - `repeated_disconnects_do_not_degrade_the_daemon` (serve.rs:1663): `spawn_count() == 3` and per-round `hello` promptness — no ordering.
  - `a_session_streams_events_and_persists_to_the_database` (serve.rs:963+): `recv_until("start-result")` then matcher-based event waits — no ordering.
  - `two_connections_racing_to_start_one_id_leave_exactly_one_child` (serve.rs:1955+): pair-wise `start-result`s, "both in flight" — no ordering.
  - The `starting`-set test: `watching.insert` is on the read side (D1 reader_loop), the relay's `watched()` filter is unchanged — no effect.
  - **Conclusion: the "precise ordering between a reply and an event frame was never contractual", and the new contract's "gap may precede the reply its drop reports" costs no existing test.** Issue #3's "the `gap` notice keeps its tests and its numbers" is still met: `gap_frame`'s shape, `enqueue`'s contract, the two unit tests, and `FAN_OUT_CAPACITY`/`CLIENT_QUEUE_CAPACITY` are all byte-identical to today.
- **New pin worth adding (in `fulcrum/src/serve.rs` unit tests, Part E):** a test that the writer's drain-arm prelude runs with a `dropped > 0` count and produces a `gap` whose `skipped` equals the count even when a reply is enqueued in between (the interleave the split allows). It is unit-testable by calling `enqueue_shared`-paths directly — see Part E.

**D5. Update `serve_client`'s doc comment and the module-header known-limitation note.**

- The module header "⚠ A slow frame on ONE connection stalls THAT connection's own relay" block (`serve.rs:94-125`) and `serve_client`'s doc (`:1145-1172`) currently DOCUMENT the limitation and its fix. After the split, rewrite both to state that the limitation is closed: the reader and writer are separate tasks, `handle_frame(…).await` no longer blocks the relay arm, and the write half's `pending` lives exclusively in `ConnectionWriter` (structural, not structural-via-discipline). Keep the "do not add a second writer / do not `Mutex<Connection>`" warning in the new wording — the reasoning that produced the split is now the invariant that protects it. The "gap" path's numbers (4096/1024) stay in the doc. **Also record the D4 ordering contract in `serve_client`'s doc** (a `gap` may interleave; see D4).

**D6. Queue bounds after the split (SHOULD-FIX 6 fix).**

- `CLIENT_QUEUE_CAPACITY`'s own doc comment (`serve.rs:249`) is *"How many undelivered frames ONE connection's outbound queue holds before the daemon starts dropping for it (finding 7)"*; its `depth stays ~1` reasoning was tied to the single task that both produced and drained the queue. Under the split, the reader enqueues AND the writer relays into the SAME `try_send` queue, so the per-connection queue can be genuinely full (an `enqueue` during an `inbound`-reply while the writer is mid-`send_frame`) — that was ALREADY the post-T9 semantics (the queue is the same `mpsc` today); what changes is that the queue can now be filled from two producers. **The total write-path buffering per connection is `CLIENT_QUEUE_CAPACITY` frames in the queue + `pending`'s in-flight tail + the write half's own OS send buffer — all bounded, all on the same per-connection budget as today.** The 1024 bound and its two compile-time asserts (`> 0`, `< FAN_OUT_CAPACITY`) are unchanged; the fuzzier "depth stays ~1" prose in the doc comment must be re-worded to the two-producer statement above. Add a `// After T4: two producers (reader replies + writer relay)` note.
- **The existing throttle test (`a_client_that_never_reads_is_throttled…`, serve.rs:1714+) still holds — and why.** Its measurement is the INBOUND edge: the reader's `recv()` + the `mpsc` queue's bounded `MAX_QUEUED_INBOUND_FRAMES` backpressure is what stops the flood. The split moves the OUTBOUND side only; the inbound path is byte-identical (`Connection::recv` is unchanged, the reader task is unchanged). The flood is `hello` frames whose replies are enqueued by the reader and drained by the writer — both sides still bounded, so the same 1 375-vs-1 168 009 measurement stands. The RSS cap (`rss_after < rss_before + 64 * 1024`) is Linux-only and measures the daemon's heap; the writer's queue is bounded by 1024 frames, unchanged. No edit to that test is required by the split. (Verify manually during Step 8 that it still passes; if the CI leg flakes on the RSS line, note it as a pre-existing measurement flake, do not weaken the cap.)

**Part E — test harness updates (the ripple points, enumerated)**

**`fulcrum/tests/transport.rs`:**

- The file imports `use fulcrum::transport::{connect, Connection, Endpoint, Listener, TransportError};` — unchanged.
- **Harness helpers that take/call `&mut Connection` and must stay green (they keep working unchanged):**
  - `recv_one(conn: &mut Connection)` (`:258`) — calls `conn.recv()` (reader half) — unchanged.
  - `accept_one(listener: &Listener) -> Connection` (`:267`) — unchanged.
  - `dial(endpoint) -> Connection` (`:275`) — unchanged.
  - `recv_checked(conn: &mut Connection)` (`:284`) — unchanged.
  - `accept_one_and_echo(listener)` (`:297-307`) — this one currently does `conn.send_frame(&json!({ "echoed": frame }))` on the returned `Connection`. **After the split, `Connection` has no `send_frame`.** This helper must change to split inside and send via the writer:
    ```rust
    async fn accept_one_and_echo(listener: Listener) -> ConnectionWriter {
        let mut conn = timeout(FRAME_BUDGET, listener.accept()).await.expect(...).expect(...);
        let frame = recv_one(&mut conn).await;
        let (mut conn, mut writer) = conn.split_writer();
        writer.send_frame(&json!({ "echoed": frame })).await.expect("send: the peer is still reading");
        drop(conn); // the server task is done reading; the reader task aborts on drop
        writer
    }
    ```
    Its only caller (`a_jsonl_frame_round_trips_in_both_directions`) does `server.await.expect(...)` and ignores the returned value, so returning the `ConnectionWriter` and dropping it in the caller closes the server's socket — identical lifetime to today's returned `Connection`. `recv_one(&mut conn)` runs BEFORE the split (the reader half is intact until `split_writer`); note the helper reads one frame first, THEN splits — order matters, keep it.
- **Tests calling `conn.send_frame` directly** (enumerated: every one is a CLIENT-side send and must switch to `split_writer`):
  - `a_jsonl_frame_round_trips_in_both_directions` (`:326`) — `client.send_frame(&hello)`.
  - `the_receive_stream_ends_at_eof_rather_than_hanging` (`:364`) — `client.send_frame(&json!({ "kind": "bye" }))`.
  - `unix_specific::a_bound_socket_is_owner_only` (`:463`) — `probe.send_frame(&json!({"probe": "after the chmod"}))`.
  - `unix_specific::a_live_daemons_socket_is_left_alone_and_the_second_bind_fails` (`:945`) — `client.send_frame(&json!({ "still": "alive" }))`.
  - Additional `send_frame` call sites inside `unix_specific`'s raw-byte probes **already use `tokio::io::split(UnixStream)` + `write_all`** (the BAD-peer pattern) and are UNCHANGED.
  - **Pattern for the client-side conversion (must be used consistently):** `let (mut client, mut client_writer) = client.split_writer();` then `client_writer.send_frame(&json).await` / `recv_one(&mut client).await`. Dropping the `client_writer` closes the peer write direction exactly like dropping a `Connection` did; keeping BOTH halves alive for the frame's duration is required (a dropped writer half closes the socket and the reader sees EOF — for the pre-A1b single-task tests this is the same drop-the-whole-`Connection` lifetime).
- **The new frame regression tests** (the FIRST implementation step) — see Step 1.
- **The Windows compile gate:** the file's platform-neutral tests now exercise `split_writer` on both legs; the `cargo check --target x86_64-pc-windows-gnu --all-targets` gate (AGENTS.md) is the Windows proof.

**`fulcrum/tests/serve.rs`:**

- `use fulcrum::transport::{connect, Connection, Endpoint};` — unchanged.
- **`Client` harness (`:557-745`)** — the only production `Client` methods touching the connection:
  - `Client::dial` (`:581`) builds `Self { conn, pending, delivered }` — unchanged (reader half).
  - `Client::send` (`:593-596`) — `self.conn.send_frame(&frame)` MUST become a `split_writer` call. Because `Client::send` may be called repeatedly, the `Client` struct must hold the writer half across calls: add a field `writer: ConnectionWriter` populated in `dial` (`let (conn, writer) = conn.split_writer();`), and `send` calls `self.writer.send_frame(&frame).await`. **`Client` is the ONE place in `tests/serve.rs` that writes frames; after `dial` splits once, every `client.send(…)` in the file keeps working unchanged.** **Round-2 🟡 (fix 5d) — `send`'s `expect` must follow the NEW EOF semantics:** today's `expect("write the frame")` was written for a `Connection` whose teardown dropped the whole socket. After the split, the daemon-side writer can finish its teardown (abort / socket close) WHILE a test is mid-`send`, so a `send` on a connection whose write half the DAEMON already closed returns `Err` — that is the new EOF semantics and it must NOT panic the harness: `self.writer.send_frame(&frame).await.expect("the daemon accepted the write — if this failed, the connection closed mid-test")` — or, better, map the `Err` to an assertion that names the teardown case (`send_frame(...).await.unwrap_or_else(|e| panic!("send failed; is the daemon tearing down this connection? {e}"))`). The assertion message must make the "writer closed during daemon teardown" reading obvious to a future test author. No production behavior changes — this is the test harness learning the split's teardown semantics (the writer exits on the peer's close, the abort, or the reader-first path).
  - `Client::recv_until_matching` (`:632-660`), `try_recv` (`:671-690`), `drain` (`:693-708`) — all read `self.conn.recv()` (reader half) — unchanged.
  - Line `:1644` (`next.conn.recv()` inside a timeout) — unchanged.
  - The `_probe` returned by `Daemon::wait_until_listening` is a `Connection` that is dropped immediately — unchanged (reader half).
  - `raw_dial` (`:446`) bypasses `Connection` entirely — unchanged.
  - `start_env`/`start_frame` (`:302` region) — unchanged.
- **The NEW slow-start relay integration test** (acceptance criterion 1) — see Step 9.
- **Every existing `serve.rs` integration test** that talks through `Client` (all 20+ listed in the grounding report, incl. `a_session_streams_events_and_persists_to_the_database`, `a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting`, `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon`, `two_sessions_on_two_sockets_do_not_see_each_others_events`, `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat`, `a_client_that_never_reads_does_not_stall_the_daemon`, `repeated_disconnects_do_not_degrade_the_daemon`) exercises the new two-task `serve_client` through the real binary and MUST stay green. **The `REAP_GRACE = 5s` durability pin, the `FRAME_BUDGET = 30s`, `ROW_BUDGET = 20s`, `PIPE_BUSY_BUDGET = 2s` budgets are unchanged.**
- **serve.rs unit tests** (`a_full_client_queue_drops_counts_and_keeps_the_connection` at `:2453`, `the_gap_notice_carries_its_count_where_the_client_reads_it` at `:2502`, and the `handle_frame`-based unit tests `unknown_frames_and_missing_sessions_are_answered_not_fatal` / `a_malformed_start_is_a_start_result_and_keeps_the_connection` / `a_failed_start_releases_its_claim` and the `start`/`close`/`relay` unit tests) are UNCHANGED (they call `enqueue`/`gap_frame`/`watched`/`handle_frame` directly, none touches `Connection`). If the `reader_loop`/`writer_loop` split is implemented with `handle_frame` sharing, these still compile untouched.
- **NEW unit test for the interleave contract (D4, round-2 🟡 re-scoped — in `fulcrum/src/serve.rs`'s `#[cfg(test)]` module):** `the_gap_drain_prelude_uses_the_current_drop_count` — this pins what the drain-arm prelude ACTUALLY does under the split (the old `the_gap_interleave_contract_counts_what_the_drain_announces` name over-claimed an ordering behaviour that is not the writer's property): the prelude builds its `gap` from the CURRENT `dropped` count at drain wake, and it may announce drops that happened between an enqueued reply and its send. Drive it directly, no `Connection` needed, green in the unit harness: call `enqueue(&tx, reply_a, &mut dropped)` and `enqueue(&tx, reply_b, &mut dropped)` on a capacity-1 channel so the second enqueue drops (`dropped == 1`), then simulate the writer's drain arm: `if dropped > 0 { let skipped = take; assert gap_frame(skipped)["skipped"] == 1 }` — the prelude's gap equals the count, and the enqueued `reply_a` is still delivered in FIFO ahead of any later frame. **True reply-vs-gap ORDERING is NOT unit-tested here** — it is a property of two tasks racing in `serve_client`, and it stays where it belongs: the integration relay test (Step 9) and the D4 contract as documented in `serve_client`'s doc (a `gap` may interleave; a `gap` never precedes a frame that predates the drops it reports).

**`fulcrum/tests/worker_mode.rs` (SHOULD-FIX 4 fix — the missed ripple):**

- Both `send_frame` call sites at `:1006` (unix leg of `serve_cli_binds_the_socket_it_was_given_and_answers_hello`) and `:1087` (windows leg) go through `dial_until_listening(endpoint).await` (`:1138-1158`), which returns a `fulcrum::transport::Connection`. After the split, `Connection` has no `send_frame`. **Conversion — both sites identically:**
  ```rust
  let mut conn = dial_until_listening(&endpoint).await;
  let (mut conn, mut cw) = conn.split_writer();
  cw.send_frame(&serde_json::json!({ "type": "hello" }))
      .await
      .expect("send `hello`");
  let reply = tokio::time::timeout(HANDSHAKE_BUDGET, conn.recv())
      .await
      .expect("a reply within the budget")
      .expect("not EOF")
      .expect("valid JSON");
  ```
  The `cw` (writer half) is dropped at the end of the test scope, exactly as the old single `Connection` was — same socket-close lifetime. The reader `conn` is still used for the `hello` reply. These two tests also assert the daemon exits via `pid_is_reaped` AFTER `drop(spawned)` — they are the process-leak tests, so the T4 teardown (D3) must keep those daemons reaped; the split does not affect the daemon's own lifecycle (the daemon is killed by the test's `Drop`), only the per-connection tasks inside it.

**`src/fixtures/worker.rs` (the `--slow-ready <ms>` hook — CRITICAL 2 fix):**

- **The hook.** `worker_main()` (`src/fixtures/worker.rs:549`) currently reads no argv. Add a startup delay between the two `emit_unstamped` calls (after `hello`, before `ready`) that is honoured only when an env var is set:
  ```rust
  // Before the `hello`/`ready` handshake: a configurable startup delay so
  // the daemon tests can hold `attach`'s `wait_ready` open for a measured
  // window (serve.rs's slow-start relay test). The env var is read ONCE at
  // process start; `None` = no delay (today's behaviour, byte-identical for
  // every existing caller). A `__slow-ready__`-style prompt cannot do this:
  // the delay must land BEFORE the `ready` frame, which is emitted before
  // any prompt is ever read.
  if let Ok(ms) = std::env::var("FULCRUM_FIXTURE_SLOW_READY_MS") {
      if let Ok(ms) = ms.parse::<u64>() {
          if ms > 0 {
              std::thread::sleep(std::time::Duration::from_millis(ms));
          }
      }
  }
  // … then `emit_unstamped(hello)`, `emit_unstamped(ready)` as today.
  ```
  - **Why an env var and not an argv flag:** `WorkerHandle::spawn` (`src/worker/client.rs:300-331`) builds the child argv itself — `cmd.arg("--worker")` only (`build_command`), nothing else — so an argv flag would require threading a flag through the engine's spawn and through the fixture's shim bins (`src/bin/fake_worker_core.rs`), touching engine-owned files OUTSIDE this task's owned set. An environment variable is inherited automatically by `Command::new(BIN)` in `Daemon::start` (tests/serve.rs:236-265 — plain `Command::new`, no `.env_clear()`); the fixture is spawned by the daemon which inherits the test's env. Test-only, no production path sets it: `FULCRUM_FIXTURE_SLOW_READY_MS` is never read by the real `fulcrum` binary (grep confirms no other reader).
  - **Critically: the delay must land (a) BEFORE the `ready` frame (that is the `attach`/`wait_ready` gate — `src/worker/manager/mod.rs:223-227`), and (b) AFTER `hello` (the handshake order is asserted by `tests/fixture_bins.rs`'s `the_worker_fixture_hands_shakes_before_any_start` and by the engine's fixture tests — both expect `hello` FIRST).** A delay inserted between `hello` and `ready` keeps both orders intact.
  - The two scope branches (`hello` then `ready`) are emitted in the CURRENT order at lines `:557-560`; keep them byte-identical apart from the inserted delay. No other fixture change.
- **Verify the `ready`-delay semantics against the daemon:** `attach` awaits `wait_ready(5s)` (manager/mod.rs:226). With `FULCRUM_FIXTURE_SLOW_READY_MS=1500`, the daemon's `start` parks the reader for ~1.5 s. That is the window the test needs. The engine's `ready` watch is set by the read task (`spawn_pipe_reader`), and `ReadyTimeout` at 5 s — 1.5 s is comfortably inside (same reasoning as the module header's `FRAME_BUDGET`).

**Steps:**

- [ ] **Step 0 — re-dispatch check:** if T3 (wave 1) has landed since this plan was written, RE-DISPATCH this plan against T3's real `serve_client` before starting. T3 edits `run`'s accept path and `serve_client`'s starter region; this task splits the post-T3 function. The plan's T3 markers (D1) are the contract for where T3's state lands.

- [ ] **Step 1 — write the failing-frame test FIRST (the criterion "existing partial-write tests still pass" is vacuous — there is NO such test today; prove it here).** In `fulcrum/tests/transport.rs` (new `#[tokio::test]`s, platform-neutral, next to `a_jsonl_frame_round_trips_in_both_directions`):
  - **The BEHAVIOURAL proof (SHOULD-FIX 5 — replaces the vacuous compile-error framing):**
    1. `two_frames_from_the_sole_writer_arrive_as_two_clean_lines` — the behaviour the whole framing exists to protect: `dial` + `accept_one` a pair, `split_writer` once, `send_frame(A).await`, `send_frame(B).await`, peer asserts exactly two frames `A` then `B` (never interleaved). This exercises the `pending`-drain path and stays green through the split; it is the regression a future two-writer change would break. **KEPT (round-2 🟠 keep-list, "why" in the note below).**
    2. **The concurrent-writer pin (round-2 🟠, REWRITTEN):** the old `two_concurrent_writers_never_interleave_frames` (two `ConnectionWriter`s racing) is DROPPED — it is not the hazard (two writers on TWO sockets cannot interleave; nothing about `pending` is shared), it does not go red on the reintroduced `Mutex<ConnectionWriter>` regression, and its "compile-error-if-true" framing was already rejected (a failing-to-compile test file aborts the whole integration binary). In its place, the pin that ACTUALLY goes red — **`a_shared_writer_under_a_mutex_never_interleaves_frames`**:
       - **Setup:** `dial`/`accept_one` a pair, `split_writer` once. Wrap the ONE `ConnectionWriter` in a shared mutex — `let shared = Arc::new(std::sync::Mutex::new(writer));`.
       - **Drive (⚠ compiles only this way):** `std::sync::MutexGuard` is `!Send`, so it CANNOT be held across an `await` inside `tokio::spawn` — `shared.lock().unwrap().send_frame(..).await` in a spawned task does not compile. Drive the two concurrent loops with `tokio::join!` on two `async move` blocks INSIDE the test body instead (a `#[tokio::test]` future has no `Send` bound, so a guard held across the `.await` is fine):
         ```rust
         // Weak-driver note: a `std::sync::Mutex` guard held across
         // `send_frame`'s write-loop `.await` can DEADLOCK — if the peer stops
         // draining mid-frame, the first task's `write` parks while it holds
         // the lock, and the second task can never acquire it. Keep the frames
         // SMALL (the peer drains between two `join!`ed yields; each send
         // completes in a few iterations), and have the peer DRAIN continuously
         // with `recv_one`. This matches the honest regression: the deadlock
         // only manifests when the peer stops reading — which is precisely the
         // backpressure case — so the assertion to watch is "40 clean lines".
         let s2 = Arc::clone(&shared);
         let a = async move { for i in 0..20 { s2.lock().unwrap().send_frame(&frame_a(i)).await.unwrap(); } };
         let b = async move { for i in 0..20 { shared.lock().unwrap().send_frame(&frame_b(i)).await.unwrap(); } };
         tokio::join!(a, b); // both loops run CONCURRENTLY on the same writer
         ```
         The peer then asserts it receives exactly the concatenated `a(0..20)` + `b(0..20)` frames **as 40 whole `\n`-terminated lines** — every frame arrives clean, none interleaved mid-line.
       - **Honest mechanism (round-2 🟠 — what makes this go red):** the assertion that every frame arrives as a whole line is precisely what fails in ANY design where two tasks can be inside the write loop of one stream WITHOUT whole-send serialisation — a reintroduced shared `pending` that two calls reach concurrently interleaves BYTES mid-frame (task A's `write` drains only part of its frame, task B's `write` lands in the middle, the peer's `read_line` glues the halves). Whole-send serialisation is the discipline this test pins: each frame must be a single clean line even under concurrent two-task drive. **The honest caveat (ⓘ, in the test doc + in the plan):** this is a regression PIN against a *possible future design*, NOT a proof about the current one — the current design makes the hazard structurally unreachable (`ConnectionWriter` is not `Clone`/`Sync`; `send_frame` takes `&mut self`), and no behavioural test can prove structural impossibility. It exists so that if a future change re-introduces sharing, the failure is a loud red in this file rather than a silent desync. **Which existing pins are kept and why** (round-2 🟠 honesty): `a_second_split_writer_panics` KEPT — cheap structural pin that a second `split_writer` call panics; `two_frames_from_the_sole_writer_arrive_as_two_clean_lines` KEPT — exercises the `pending`-drain path serially; `a_partially_accepted_frame_is_drained_before_any_new_bytes` KEPT — the `pending`-drain regression as it appears in production. The plan states plainly that all three of those stay green beneath the SERIALISED regression (`Mutex<ConnectionWriter>` + whole-send lock: a second `split_writer` call still panics, the serialised sends still arrive as clean lines, the partial-frame pin never observes concurrent writes) — they false-green on it, which is why the NEW concurrent-driver test exists: it is the one that reddens an interleaving design.
    3. **The interleave detector (kept as-is):** `a_partially_accepted_frame_is_drained_before_any_new_bytes` — write a frame whose serialization is larger than the OS write buffer granularity, `send_frame` on a peer whose reader is deliberately NOT reading (so `write` accepts less than the full frame), then `send_frame` a second frame; the peer, once it reads, must see frame 1's bytes COMPLETE before any byte of frame 2. This is the `pending`-drain regression exactly as it appears in production (the writer's own `pending` loop across two `send_frame` calls). It fails if a design ever lets a second writer inject bytes ahead of `pending`. (Note: under a reintroduced shared `pending` this stays green by serialisation — the new `a_shared_writer_under_a_mutex_never_interleaves_frames` is the one that closes that gap.)
  - Also assert **teardown behaviour (D3's tested invariant):**
    - `a_send_against_a_shut_down_connection_errors_that_the_writer_survives` — split once, `writer.shutdown().await` (A1b), then `send_frame` must return `Err` — the write half really does close the duplex while the read half is alive.
    - **🟠 Round-2 replacement:** the frame-level half of `a_writer_whose_tx_drops_terminates_the_reader_loop` is DROPPED as a named test — `ConnectionWriter` has NO `queued` channel (that is a `serve`-level property), so no transport-level test can stand it up. The property it named — reader-EOF terminates the other side — is an integration property of `serve_client`, and it is now covered by the slow-start test's TEARDOWN half in Step 9: when the connection's client half closes, the test asserts the daemon does not hang and (via the abort backstop) no writer task outlives the connection — the daemon spawns no stray task, which is exactly what T1's census on this same branch would catch if it did. The transport-level residue that IS kept: after the reader half is dropped, a `recv()` on the OTHER half returns `None` (peer EOF) — appended to the `a_send_against…` test's body, proving the duplex really is shared.
  - **Run it:** `cargo test -p fulcrum --test transport` — expect the NEW tests to be the only ones red at first because `Connection` no longer has `send_frame` (the conversions in Step 4 are also red). The important first-run property: the frame tests EXIST and would fail against any two-writer design.
  - `Did it fail with the expected shape?` — a compile error on `Connection::send_frame` is EXPECTED at this point (the structural guarantee surfacing). Do NOT proceed past this step into the split with the frame tests absent.

- [ ] **Step 2 — run the existing suite to record the baseline (optional but recommended):** `cargo test -p fulcrum --test transport` and `cargo test -p fulcrum` (or at minimum `cargo test -p fulcrum --test transport --test serve --test worker_mode`) should be GREEN before touching `frame.rs`. If they are NOT green, stop and report (the tree is expected to be T3-post-shape or current-shape; either must build). **In the consume-mode workflow cargo may not be runnable in the planner's sandbox; the IMPLEMENTER runs these. `cargo test -p fulcrum` MUST have at least the discovered serve/transport/worker_mode tests fail loudly if broken — do not accept a green-empty suite as proof (a target that builds no test binaries still exits 0).**

- [ ] **Step 3 — implement Part A in `fulcrum/src/transport/frame.rs`:**
  - Add `ConnectionWriter` with `send_frame` (the sole writer) and the comment block explaining it is the ONLY writer and that `pending` lives ONLY here.
  - Add `ConnectionWriter::shutdown` (A1b) and `Connection::shutdown_while_writer_intact` (A2b) and `Connection::split_writer` (A3).
  - Add the private `Option<WriteHalf<DuplexBox>>` backing + `split_writer` on `Connection` (A3's exact design).
  - Move `pending`/`writer` out of `Connection`; strip `Connection::send_frame` (delete it — anything still calling it is the two-writer hazard).
  - Keep `Drop { reader.abort() }`, the manual `Debug`, `MAX_*`, the reader task, `Connection::recv` (its only read method after the split), `read_capped_line_under`, `decode`, `wrong_platform`, and the `#[cfg(test)]` module's existing tests UNCHANGED (A2). `Connection`'s write side is stripped (A2/A3); tests that need to send use `split_writer`.
  - **`Connection::new`'s signature is UNCHANGED** (`pub(crate) fn new<S>(stream: S) -> Self where S: AsyncRead + AsyncWrite + Unpin + Send + 'static`); its body stores `writer: Some(write_half)` (A2/A3).

- [ ] **Step 4 — implement Part B in `fulcrum/src/transport/mod.rs`:** add `ConnectionWriter` to the `pub use frame::{…}` line (`pub use frame::{Connection, ConnectionWriter};`). (The `pub(crate)` methods `shutdown`/`split_writer` do not need re-export from `mod.rs` — they are on the types and are reachable through the public types.)

- [ ] **Step 5 — compile gate:** `cargo check --target x86_64-pc-windows-gnu --all-targets` (or the repo's documented Windows check) — the new `ConnectionWriter` uses only platform-neutral tokio types; this check must pass on the Windows leg with no `cfg`-leak. Also `cargo check` on the current platform. **If `Connection::new`'s signature had to change, report `DEVIATION:`.**

- [ ] **Step 6 — Part C verification:** confirm `unix.rs`/`windows.rs` builds unchanged (they should — `Connection::new(stream)` is untouched). If `Connection`'s field removal (dropping `writer`/`pending`) broke a `#[cfg(test)]` unit test inside `frame.rs` that constructs `Connection` directly, fix THAT test to use `split_writer` (it is inside the owned file).

- [ ] **Step 7 — Part E, `tests/transport.rs` conversions:** switch the 4 direct `send_frame` call sites (`:326`, `:364`, `:463`, `:945`) and the `accept_one_and_echo` helper to `split_writer`; keep the raw-byte probes as-is. Run `cargo test -p fulcrum --test transport` — all pre-existing tests plus the new frame tests must be green.

- [ ] **Step 7b — Part E, `tests/worker_mode.rs` conversions (SHOULD-FIX 4):** convert the two `send_frame` call sites at `:1006` and `:1087` per the exact `split_writer` pattern above. Run `cargo test -p fulcrum --test worker_mode` — all 31 existing tests must be green (the file's process-leak assertions (`pid_is_reaped`) must still hold).

- [ ] **Step 8 — Part D, `serve.rs` two-task split:** implement `serve_client` per D1, `reader_loop`/`writer_loop` per D2 (use the `Arc<StdMutex>` sharing + `enqueue_shared` — the plan's chosen design), teardown per D3 (drop-based, no join, no stop one-shot, `writer_task.abort()` backstop), the D4 ordering contract in `serve_client`'s doc, the D5 doc/header rewrites, and the D6 queue-bound doc update. Add the NEW unit test `the_gap_drain_prelude_uses_the_current_drop_count` (D4) and the `send_frame_no_select` free fn. `gap_frame`/`enqueue`/`watched` remain byte-identical. `cargo build -p fulcrum` must be clean, and `cargo clippy -p fulcrum --all-targets -- -D warnings` (the CI bar) must not gain warnings.

- [ ] **Step 9 — new integration test (acceptance criterion 1) + harness + fixture-hook changes:**
  - First the harness change in `tests/serve.rs`: `Client` gains a `writer: ConnectionWriter` field; `dial` does `let (conn, writer) = conn.split_writer();`; `send` uses `self.writer.send_frame`. All existing `client.send(…)` / `recv_until_*` / `drain` / `try_recv` calls then keep working unchanged (they go through `Client`'s methods, the only place that touches the connection).
  - **Slow-start machinery — the `--slow-ready` env hook (CRITICAL 2 fix), not a wrapper bin.** `src/fixtures/worker.rs` gains the `FULCRUM_FIXTURE_SLOW_READY_MS` startup delay between `hello` and `ready` (see Part E). This is the ONLY fixture change: no new bin, no `fixture_bins.rs` edit, no `--target-dir` games. The vet's three fixture-bins premises are all confirmed wrong, and the plan no longer relies on them: `_core` is a NAMING rule for the two existing bins (it does not forbid a new distinctly-named bin, but none is needed); `fixture_bins.rs` asserts only on `target/debug/` for those two names (an isolated-`--target-dir` bin would be invisible to it — irrelevant now); and `__long_stream__` streams only AFTER a `start`, so "A's chunks arrive while B's `attach` is in flight" REQUIRES the delayed-`ready` fixture to be reachable at all.
  - **The test's ordering claim, re-traced and stated (what the vet asked to verify):** with the fixture's `ready` delayed by 1.5 s, a SECOND `start` (B) parks the daemon's READER in `attach`→`wait_ready` for ~1.5 s. Session A (already started, already prompter with `__long_stream__`) streams chunks from the SAME fixture — but the fixture is ONE process serving ONE session: B's `start` is attached through the fixture's stdin only AFTER the fixture emits `ready`... **wait. The fixture serves BOTH sessions: the daemon spawns ONE worker per session id (`attach` spawns `factory.spawn()` per call) — B has its OWN worker process.** Verified: `attach` (manager/mod.rs:223) does `(self.state.factory).spawn()` on EVERY call, so A's fixture and B's fixture are two processes, both running `worker_main()`. Each emits `hello` + `ready` at ITS OWN start (subject to the shared `FULCRUM_FIXTURE_SLOW_READY_MS` in A's env — the env var is inherited by both). **The actual trace:**
    - t0: A starts (fast or slow — no delay needed for A if B is the delayed one, but the env var is process-wide so the test sets it for BOTH — the fixture's `hello`/`ready` both delay; that is fine).
    - t1: A's `start-result` ok ⇒ `watching` contains `relay-a` (the READER's insert at D1).
    - t2: A's `prompt(__long_stream__)` → `prompt-result` ok (the daemon's `relay` dispatches to A's fixture; the fixture streams 30 chunks ~100 ms apart). **The ordering claim to trace: does the FIRST A-chunk reach the fan-out BEFORE B's `start-result`?** The `prompt-result` is enqueued by the READER (D1) the instant `relay` returns; the chunks must travel fixture→pump→`on_event`→fan-out→writer→drain→socket. Both are in flight; the 1.5 s attach window gives the chunks a ~15-chunk head start. **The discriminator does NOT need chunks to arrive before `start-result`** — it needs AT LEAST ONE A-chunk to arrive AFTER B's `start` is SENT and BEFORE/AROUND B's `start-result`, which is exactly what the write-side independence (writer away from reader's attach) delivers in the NEW design and CANNOT deliver in the OLD design (the old reader parks the fan-out arm during attach, so A-chunks are buffered in the fan-out ring and only drained after `attach` returns — the B `start-result` is peeked from the inbound arm FIRST).
    - t3: `start(B)` frame sent. `pre_b = client.delivered.len()` — everything the client has READ so far (importantly: `delivered` only grows inside `recv_*` calls — the snapshot is at the moment `start(B)` is written, see the boundary-status protocol below).
    - t4: B's `attach` parks the reader for ~1.5 s (A's chunks CONTINUE to be written by A's worker and relayed by the WRITER — the split's whole point).
    - t5: B's `start-result` (ok) arrives; the writer has also drained A-chunks.
  - **The test itself — `a_slow_start_does_not_stall_that_clients_own_event_relay` (name matches the file's naming):**
    - Spawn a SECOND daemon for this test: `Daemon::start_with_env(env)` — the harness's `Daemon::start` gains an optional env-var parameter (a `#[cfg]`-free `env("FULCRUM_FIXTURE_SLOW_READY_MS", "1500")` on the `Command` when the test asks for it; do NOT change the shared default — all other tests keep the fast fixture). One `Client`; `recv_until("hello")`.
    - Start A: `start_env("relay-a", …)` → `start_frame` → `recv_until_matching("start-result", |f| f["sessionId"] == "relay-a")` → assert `ok == true`. `watching` now contains `relay-a`.
    - Prompt A with `__long_stream__` → `recv_until("prompt-result")` → assert ok. A's fixture is now streaming.
    - **The boundary-status protocol (SHOULD-FIX 5's determinism demand, replacing the old timing-margin reasoning):** after `recv_until("prompt-result")` there may be A-chunks ALREADY in `delivered`. The discriminator must not fail on them (they prove nothing — the old design delivers a backlog once attach returns). The boundary is therefore **an explicit barrier, not an index margin**: the test `drain()`s the client until it has seen the first POST-prompt `agent_message_chunk` for `relay-a` — call it `first_chunk`. THEN it sends `start(B)`. NOW the invariant holds with certainty: *any frame AFTER the `start(B)` write, while the reader is inside `attach`, is a frame the WRITER relayed — and in the OLD design the writer does not exist, so the NEXT A-chunk observed after `start(B)` can only be drained after `attach` returns.* **THE ENGAGEMENT GAP (round-2 🟠 fix):** A's fixture produces a chunk every ~`LONG_STREAM_GAP` (~100 ms, `src/fixtures/worker.rs`), so when `start(B)` is written there are chunks ALREADY in the daemon's fan-out that the UNSplit design drains the moment `attach` parks the reader — the old design could deliver those buffered chunks within the measurement window and false-green. The engagement gap is closed by making B's attach PROVABLY engaged before the measurement starts: after sending `start(B)` and BEFORE awaiting the next A-chunk, sleep `SLOW_READY_MS * 0.6` (~900 ms). By the end of that sleep, B's `attach` is fully engaged (`wait_ready` is parked — the fixture's `ready` is ≥`SLOW_READY_MS` away) AND every chunk that predated `start(B)` has been drained by the old design's fan-out arm, so the next chunk measured is genuinely produced-after-engagement. Only THEN await the NEXT `agent_message_chunk_for("relay-a")` with the short budget; if it arrives within `RELAY_ARRIVAL_BUDGET` → the relay ran during B's attach → GREEN (new design); if it times out → RED (old design: its fan-out arm is unpolled inside attach and the pre-engagement backlog is exhausted, so nothing is drained until `attach` returns ~1.5 s later — the budget closes the window). **This remains a timing test with a scheduling dependency** — the chunk cadence and the sleeps are real (`std::thread::sleep` and `tokio::time::sleep` never return early), and the honest statement is that a loaded runner can stretch the budget (false-RED, never false-GREEN). The named consts (round-2 🟠 fix — next to the existing `FRAME_BUDGET` at the top of `tests/serve.rs`):
      ```rust
      /// The fixture's `ready`-delay under `FULCRUM_FIXTURE_SLOW_READY_MS`
      /// (mirrors the env value the daemon is spawned with). Serves the
      /// slow-start relay test's 0.6× pre-measurement sleep.
      const SLOW_READY_MS: u64 = 1500;
      /// How long the slow-start test waits for the NEXT A-chunk after B's
      /// attach is provably engaged. ~100 ms chunk cadence vs this budget:
      /// 8× headroom on a quiet runner.
      const RELAY_ARRIVAL_BUDGET: Duration = Duration::from_millis(800);
      // fallback values if CI flakes on a loaded runner (all still inside
      // `wait_ready`'s 5 s and `FRAME_BUDGET`): SLOW_READY_MS = 2000,
      // RELAY_ARRIVAL_BUDGET = 1200, and the sleep constant 0.6 tracks it.
      ```
      (A CI flake is a one-line tune: raise both consts — the comment names the fallback pair. Do NOT weaken the budget below ~600 ms — it must stay well under the fixture's scheduled 1.5 s ready delay.)
    - **Cross-check the "cannot false-green the NEW design" direction:** if the runner is so loaded that the writer's relay is delayed past 800 ms, the test would false-RED (flake) but never false-GREEN — the old design cannot produce a chunk within `RELAY_ARRIVAL_BUDGET` of the post-sleep measurement by construction (its fan-out arm is unpolled inside attach, and the pre-engagement backlog is exhausted by the 0.6× sleep). The false-green risk the vet named (a post-B read straddling the boundary) is eliminated by the barrier: `first_chunk` is consumed BEFORE `start(B)` is sent, so nothing measured after the write can be a pre-B artifact.
    - **TEARDOWN half (round-2 🟠 fix 3 — the re-scoped `a_writer_whose_tx_drops_terminates_the_reader_loop` integration property):** after the relay assertion, the test asserts the connection tears down cleanly on the reader-first path without hanging and without leaking a writer task. `Client` holds the reader half (`conn`) and the writer half (`writer`) as two fields of ONE struct; dropping the `Client` closes the whole socket. **The honest teardown assertion:** once the test's `Client` drops (scope exit), the daemon's `serve_client` must terminate BOTH its tasks without hanging: after `drop(client)`, `Daemon::wait_until_listening` returns promptly AND a fresh `Client::dial` + `hello` reply succeeds within the budget — proving the accept loop is not wedged and the old connection's tasks are gone (a writer stuck inside `send_frame` on a closed socket would still let a new dial succeed, so the observable that actually catches the leak is T1's process census on this same branch, run at the suite level; step 10's full suite is where the census reddens if the abort backstop regresses). This is the integration-level home of the property named `a_writer_whose_tx_drops_terminates_the_reader_loop` — the name is deliberately NOT used as a transport test (a frame-level `ConnectionWriter` has no `queued` channel; the property is `serve`-level, so it lives here).
  - **Windows leg:** the env hook and the sleeps are portable; the `#[cfg(windows)]` arm of `Daemon::start_with_env` just sets the env var before spawn — no wrapper, no argv. The Windows CLIENT half is socket-typed: the test's assertions run per-platform identically. Windows CI timing is the only risk (a loaded runner stretching the budget) — the named consts are the one-line tune (raise both; the fallback pair is in the const comment).
  - **`SLOW_READY_MS` must also be set on the DAEMON's `Command`** in `Daemon::start_with_env` (`env("FULCRUM_FIXTURE_SLOW_READY_MS", SLOW_READY_MS.to_string())`) — the fixture inherits the daemon's env, and the const is the single source of truth for BOTH the daemon spawn and the test's 0.6× sleep.
  - **Fallback if the env-hook cannot be built in CI** (e.g. the fixture's env is scrubbed — it is not, verified: `WorkerHandle::spawn` inherits the daemon's env; `Daemon::start` inherits the test's env): drive the window with `__slow__` (the fixture's 10 s settle) instead — but `__slow__` delays the SETTLE, not the `ready`, so it opens a DIFFERENT window (the relay during a long turn) and does not test the `attach` stall. Prefer the env hook; record the exact reason in the final report if the fallback is taken.
  - **UNVERIFIED (flag for the caller's vet):** (1) whether a loaded macOS/Windows CI runner can stretch `RELAY_ARRIVAL_BUDGET` past 800 ms — the plan's named consts make it a one-number tuning change if CI flakes; (2) whether the `Daemon::start_with_env` parameterisation collides with T2's/tests' use of `Daemon::start` (it does not — default is unchanged); (3) whether the env-hook delay interacts with `tests/fixture_bins.rs`'s handshake test (`the_worker_fixture_hands_shakes_before_any_start` reads lines with a 10 s deadline — the env var is NOT set in that test's `Command`, so no delay, verified by the plan's position of the hook AFTER env read).

- [ ] **Step 10 — full verify:** run, in order, on the branch:
  - `cargo fmt --all -- --check` (CI gate — rustfmt-clean)
  - `cargo build`
  - `cargo test -p fulcrum` (the full member suite; the existing serve/transport/worker_mode tests + new ones must pass; `a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting` is the durability pin that must stay green)
  - `cargo clippy -p fulcrum --all-targets -- -D warnings`
  - `cargo check --target x86_64-pc-windows-gnu --all-targets`
  - `Did any of them fail?` Fix and re-run before committing.
- [ ] **Step 11 — commit (serial task, feature branch), TWO commits (SHOULD-FIX 7):**
  - **Commit 1 (the frame split + its tests):** `transport: split Connection so the writer half owns pending (T4a, issue #3)` — frame.rs (ConnectionWriter, split_writer, shutdown, A1c), mod.rs re-export, the new frame tests in `tests/transport.rs` (incl. `a_shared_writer_under_a_mutex_never_interleaves_frames`), and the `tests/transport.rs` + `tests/worker_mode.rs` conversions (they only compile once `Connection::send_frame` is gone). This commit alone must pass `cargo test -p fulcrum --test transport --test worker_mode`.
  - **Commit 2 (the serve split):** `serve: split serve_client into reader and writer tasks (T4b, issue #3)` — serve.rs (reader_loop/writer_loop/enqueue_shared/send_frame_no_select/D3 teardown incl. the `writer_task.abort()` backstop/D4 contract/D5 docs/D6 doc), the `Client` harness (incl. the round-2 🟡 `send`-expect EOF semantics) + the slow-start relay test (with the 🟠 engagement gap and teardown half) + the `SLOW_READY_MS`/`RELAY_ARRIVAL_BUDGET` consts in `tests/serve.rs`, the `FULCRUM_FIXTURE_SLOW_READY_MS` hook in `src/fixtures/worker.rs`, and the new `the_gap_drain_prelude_uses_the_current_drop_count` unit test. **T4b is serial on T4a** (the serve split is only safe because the writer half owns `pending`); never land T4b without T4a, and never land T4a without its frame regression tests.
  - If the reviewer prefers one commit, that is acceptable ONLY if the frame tests remain in the first commit and the serve split in the second — the two-commit shape is the default.

**Acceptance criteria:**
- [ ] `cargo test -p fulcrum --test transport` — ALL pass, including the `transport::frame` line-reader tests (`a_line_of_exactly_the_limit_is_accepted`, `one_byte_over_the_limit_is_refused`, `an_unterminated_flood_is_refused_at_the_cap_and_no_further`, `an_unterminated_tail_at_eof_is_still_delivered`, `a_clean_eof_reports_nothing`, `the_cap_counts_bytes_not_characters`, `the_line_too_long_error_names_the_limit`) and the NEW `a_second_split_writer_panics`, `two_frames_from_the_sole_writer_arrive_as_two_clean_lines`, `a_shared_writer_under_a_mutex_never_interleaves_frames`, `a_partially_accepted_frame_is_drained_before_any_new_bytes`, `a_send_against_a_shut_down_connection_errors_that_the_writer_survives`. **Tested-as-vacuous baseline:** there were NO partial-write/two-writer tests before this task (verified by grep) — the "existing tests still pass" criterion was empty, and this task writes the tests that fail if `pending` is ever shared.
- [ ] A client that starts a session and streams sees event frames arrive WHILE `start` is in flight, not after; the `a_slow_start_does_not_stall_that_clients_own_event_relay` test proves it (RED before the split via the 0.6×-engaged `RELAY_ARRIVAL_BUDGET` probe, GREEN after — the round-2 🟠 engagement gap is closed by the `SLOW_READY_MS * 0.6` pre-measurement sleep), using the `FULCRUM_FIXTURE_SLOW_READY_MS` env hook in `src/fixtures/worker.rs`. The same test's teardown half asserts the daemon reaps both of the connection's tasks on the reader-first path (the re-scoped `a_writer_whose_tx_drops_terminates_the_reader_loop` property — round-2 🟠, `serve`-level only).
- [ ] A regression test exists for the two-writer hazard, and its blind spots are stated honestly (round-2 🟠): `a_second_split_writer_panics` (`#[should_panic(expected = "the writer was already taken")]`), `two_frames_from_the_sole_writer_arrive_as_two_clean_lines`, `a_partially_accepted_frame_is_drained_before_any_new_bytes` — and **`a_shared_writer_under_a_mutex_never_interleaves_frames`, the pin that ACTUALLY goes red on a reintroduced `Mutex<ConnectionWriter>` + re-added `send_frame`** (a regression PIN against a possible future design, not a proof about the current one — stated in the test's doc). The plan states plainly that `a_second_split_writer_panics` stays green on that regression (no second `split_writer` call exists) and that the two serial-driver pins pass trivially (the mutex serialises), which is exactly why the concurrent single-writer driver exists.
- [ ] `cargo fmt --all -- --check`, `cargo clippy -p fulcrum --all-targets -- -D warnings`, `cargo check --target x86_64-pc-windows-gnu --all-targets` all pass (3-OS bar; the Windows leg is a compile gate, not execution proof).
- [ ] `the_gap_notice_carries_its_count_where_the_client_reads_it` and `a_full_client_queue_drops_counts_and_keeps_the_connection` pass UNCHANGED; `gap_frame`'s shape (`type:"gap"`, top-level `skipped`, no `sessionId`) and `FAN_OUT_CAPACITY`/`CLIENT_QUEUE_CAPACITY` (4096/1024) and their compile-time asserts are untouched.
- [ ] **No task leaks and no deadlock:** when a connection dies, BOTH the reader and writer tasks of its `serve_client` terminate; the writer is NEVER joined (its `JoinHandle` is `abort()`ed by `serve_client` after `reader_loop` returns — the round-2 🔥 backstop; the pre-split shutdown, the reader-half EOF drain, and the peer's close are the mechanisms that unwake a parked write — tokens the `a_send_against_a_shut_down_connection_errors_that_the_writer_survives` test proves). **A writer parked inside `send_frame` never outlives its `serve_client`** — the round-2 🔥 tested invariant, covered by the slow-start teardown half (Step 9). T1's process census stays green.
- [ ] `Connection::new`'s signature is unchanged — if it is not, a `DEVIATION:` is recorded in the plan/file before implementation proceeds.
- [ ] All pre-existing `tests/serve.rs` integration tests (distinctly `a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting`, `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon`, `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat`, `a_client_that_never_reads_does_not_stall_the_daemon`, `two_sessions_on_two_sockets_do_not_see_each_others_events`, `repeated_disconnects_do_not_degrade_the_daemon`) pass.
- [ ] `fulcrum/tests/worker_mode.rs`'s `serve_cli_binds_the_socket_it_was_given_and_answers_hello` (both legs) passes with the `split_writer` conversion at `:1006`/`:1087`.
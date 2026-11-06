---
status: committed
done-when: A client that starts a session and streams sees its own event frames arrive WHILE `start` is still in flight; the pre-existing framing suite still passes; the `gap` notice keeps its numbers and its tests; and a new regression test that MUST fail if `pending` ever becomes reachable from two tasks is green.
parent: docs/roadmap/daemon-hardening-status.md
---

# T4 — split `Connection` and `serve_client` (issue #3)

**Goal:** Remove the one-connection stall: today `serve_client` is one task running one `select!`, `start` runs INLINE and awaits `attach` (the engine's ~5 s `ready` bound, `src/worker/manager/mod.rs:223` → `handle.wait_ready(Duration::from_secs(5))`), and while it waits this connection's own fan-out relay arm cannot run — so a client that starts a session and streams sees its first event frames only after `start` completes.

**Architecture:** Split `transport::frame`'s `Connection` into an owning reader half (`Connection`) and an owning writer half (`ConnectionWriter`), moving `pending` into the writer so it is STRUCTURALLY impossible for two tasks to reach it. Then split `serve_client` into a reader/reply task and a writer task sharing the session event subscription, so `handle_frame(…).await` no longer blocks the relay arm. The `gap` accounting (`enqueue`, `gap_frame`, `dropped`, `watched`) moves intact into the writer, which is the sole owner of `send_frame`.

**Tech Stack:** Rust (pinned `rust-toolchain.toml` channel `1.99.0`), tokio 1.54 (`process io-util io-std sync macros time net rt rt-multi-thread`), `tokio::io::split` over a `Box<dyn Duplex + Unpin + Send>` duplex, `tokio::sync::mpsc` for outbound, `tokio::sync::broadcast` for the fan-out. **No new dependencies** (`Cargo.toml` is frozen for this feature; no `windows`/`windows-sys`).

**Reconciled against T3 `6d6887a` (base now `d38423f`) — what the plan changed because the T3 code is now in the tree.** T3 landed in wave 1 and rewrote the exact region this plan's `Builds on:` named; the third-vet-round plan was written against a tree where T3 "was not known yet". Re-read of the committed `serve_client` at `serve.rs:1528` and the T3 diff produced this delta list (each item is expanded at its marker inline):

1. **`serve_client`'s signature is now `async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection)`** (serve.rs:1528) — the `mut` is back. With the split `connection` IS the reader half, so `mut` is the honest spelling; every "non-mut `Connection`" clause in the old plan (Context, D1's signature paragraph) is deleted. `run` spawns it at `serve.rs:1439` (was :1119).
2. **The "T3 RELOCATE INTO READER HALF" marker now names the real state.** T3's header-region locals, placed "before the `select!` … so they move into a reader half as-is" (its own comment), are concrete now: `let mut handshake = Handshake::WaitingHello { pre_hello: 0 };` and `let mut rate = FrameRateGate::new(std::time::Instant::now());` (serve.rs:1569-1570), plus `let mut seen = 0usize;` (serve.rs:1559). All three move into `reader_loop`, with their real type names and the gate/refusal logic (D1).
3. **The exit-path logic survives the split and its owners are named.** `seen` and its `seen > 0` disconnect line / `seen == 0` no-log rule are READER-half (the half that counts inbound); `dropped` and the "N frame(s) behind (it was never told)" line are WRITER-half; T3's counted-schedule rejection log lives in `run`'s `Err(TransportError::PeerIdentity)` arm (serve.rs:1455-1471), which the split never touches. (D3.)
4. **T3 grew `serve_client`'s production `send_frame` sites from two to FOUR**: the pre-hello refusal (serve.rs:1644) and the rate-gate refusal (serve.rs:1668), plus the drain pair (serve.rs:1726/1731). The two refusal sites are READER-side and are the wire-auth path the old plan never accounted for; the split routes them through a **refusal `oneshot` + ack** (not `enqueue`, not a raw reader send — see D1/D3), preserving T3's "told ONCE, then closed" guarantee without giving `Connection` a writer method post-split.
5. **`tests/transport.rs` conversions grew by one test:** T3 added `same_uid_peer_is_accepted` (two `send_frame` sites, :1166/:1174). The `#[ignore]`d `foreign_uid_peer_is_rejected_under_root` re-executes a dialing child that expects refusal and sends nothing — no conversion. The conversion list is now 6 tests + the `accept_one_and_echo` helper (Part E).
6. **`TransportError::PeerIdentity { peer_uid, expected_uid }` is in `mod.rs`** (variant at ~:198) — it is a variant of the EXISTING enum, so Part B's re-export edit is still exactly the one line (`pub use frame::{Connection, ConnectionWriter};` at mod.rs:76) and the re-exports section matches the current tree.
7. **`read_to_end` is deleted from the teardown contract** (the Decision-log correction the dispatch demands): `ReadHalf::poll_read` only does `inner.with_lock(|s| s.poll_read(..))` and never calls `poll_shutdown`, so draining the read half conveys nothing to the write direction. The old plan's "reader-half `poll_shutdown` has fired from the drain's EOF/error" sentence was that false claim and is removed; the unconditional `writer_task.abort()` after `reader_loop` returns is the ONLY mechanism that carries "no task outlives `serve_client`".
8. **The round-1 "pre-split reader shutdown" (`Connection::shutdown_while_writer_intact`, old A2b) is DELETED, and this is the one genuine fix this pass makes to the vetted architecture.** Calling `poll_shutdown` on the shared duplex at `serve_client` START closes the daemon's WRITE direction before the writer task sends anything (tokio `WriteHalf::poll_shutdown` → `shutdown(SHUT_WR)` on the fd), so every reply/relay on every connection would fail. It was a leftover of the retired "reader must close the socket under a parked writer" design; the round-3 🔥 abort design (abort + `ConnectionWriter` drop as the last `Arc`) already closes the socket on the reader-first path, and the writer's own `ConnectionWriter::shutdown()` (A1b) covers its self-reached exits. `poll_shutdown` remains the mechanism — called by the WRITER on its exits, exactly one place, never from inside a `select!` (A1c).
9. **Numbers re-verified on the post-T3 tree:** `FAN_OUT_CAPACITY=4096` :201, `CLIENT_QUEUE_CAPACITY=1024` :275 (+ the two const asserts), `MAX_PRE_HELLO_FRAMES=2` :320, `FRAME_RATE=200` :348, `FRAME_RATE_BURST=600` :358, `SESSION_LIMIT=32` :731, `handle_frame` :1850, `start` :1885, `start_env` :1981, `claim_session` :937, `gap_frame`/`enqueue`/`watched` :1791/:1815/:1838.
10. **T3's `hello` gate vs the slow-start test — verified compatible, numbers stand (see Step 9 "T3 interaction" note):** every `Client` in `tests/serve.rs` sends `hello` and waits for its reply before any `start` (the file-wide pattern, e.g. :905-906), the slow-start test does the same; `hello` frames are NOT charged to the rate gate (serve.rs:353-375, `FRAME_RATE`'s doc) and the 600-token burst dwarfs the test's ~4 non-greeting frames; the `wait_until_listening` connect-and-drop probe sends zero frames and stays the `seen == 0` no-log case; `worker_mode.rs:1006/:1087` both send `hello` first (their `HANDSHAKE_BUDGET` reply wait still holds). No fixture numbers moved.

**Rounding (revised in-place after the vet round 1, 🔴; amended again after round 2 — the five round-2 amendments are marked 🔥/🟠/🟡 inline and logged in the Step list; and reconciled against T3 `6d6887a` as listed above):**
- **Round-2 amendment log (this pass, surgical — the architecture is unchanged):** (1) 🔥 `serve_client` holds the writer's `JoinHandle` and **`abort()`s it after `reader_loop` returns** — a writer parked inside `send_frame` never outlives its `serve_client` (D1/D3, tested invariant); (2) 🟠 the shared-writer regression pin that actually goes red, with an honest note on which existing pins false-green on the regression (Step 1); (3) 🟠 the reader-loop-termination property re-scoped as a `tests/serve.rs` integration property — the slow-start test's teardown half (Step 1/Step 9); (4) 🟠 the slow-start engagement gap (Step 9); (5) 🟡 claim precision on `shutdown`'s un-park semantics, the writer-exit list, the gap-interleave unit test, and `Client::send`'s EOF semantics.
- **Teardown is drop-based, and the dropped-write-half claim is corrected to what tokio 1.54 actually does** (CRITICAL 1). `tokio::io::WriteHalf<T>` wraps `Arc<Inner<T>>` (verified in tokio 1.54.1 `src/io/split.rs:20-42`): it has **no `Drop` impl**, and dropping it does NOT close the write direction while the other half's `Arc` is alive — the `Arc` is shared, the stream stays open, and a parked `write` does not error. All three previous teardown variants (join-await, join-timeout, dropped-write-half-closes-socket) are therefore deadlocked or wrong on this tokio. The primitive that closes the WRITE DIRECTION while the stream lives is `poll_shutdown` on the shared inner (see A1b — it makes FUTURE writes fail and the peer sees EOF; it does NOT reliably un-park a write already blocked in the OS, round-2 🟡; the parked write in a real connection is broken by the peer's close, and the socket's FULL close rides the final `Arc` drop after the abort). `ReadHalf::unsplit` requires the `WriteHalf` to still be alive (tokio `split.rs:61-75` panics when the pair is unrelated — so no). The shutdown is the honest mechanism and it is reachable in exactly ONE place, AFTER `send_frame` returns — the writer's own `ConnectionWriter::shutdown()` on its exits (A1b, MUST; never from inside a `select!`, A1c). The round-1 pre-split reader shutdown (A2b) is DELETED (see the reconciliation list, item 8). Drop-based reclamation (what drops what, in order) is specified as a tested invariant in D3.
- **The writer is spawned, never joined — and its `JoinHandle` is `abort()`ed by `serve_client` after `reader_loop` returns (round-2 🔥).** The old plan had the reader `await` the writer's `JoinHandle` after a `stop` one-shot; the vet's core objection — a writer parked inside `send_frame`'s write loop cannot poll the stop arm, so the join never resolves — is verified correct (see `frame.rs:274-282`, the `loop { … self.writer.write(&self.pending).await … }` with no timeout; a peer that stops reading parks that loop at the OS send buffer internal, and armed with the corrected `WriteHalf` semantics the join hangs FOREVER). The plan: the writer is a child task whose `JoinHandle` `serve_client` KEEPS and `abort()`s after the reader's task returns (and after the reader's early `break`). Aborting — NOT joining — is compatible with "never joined": it makes a parked write-loop's drop synchronous (no `Drop`-time `flush` is ever awaited) and gives "no task outlives `serve_client`" mechanical enforcement. Shutdown on the shared stream is the WRITER's job alone: its own `ConnectionWriter::shutdown()` (A1b) on its exits, never through a join; the abort is the backstop for the one path no writer exit reaches (peer closed inbound, write direction still open). (The round-1 "reader pre-split shutdown" — old A2b — is DELETED in this reconciliation: poll-shutting the shared duplex at `serve_client` start would close the daemon's own write direction before the writer ever sends; see reconciliation item 8.) The prior "handle-dropped, never-joined" claim is retired: dropping the handle alone would orphan a writer parked inside `send_frame`.
31a. **The slow-start proof is RED BY CONSTRUCTION, not by margin (round-3 🔴 — the timing-window fix).** The round-2 `--slow-ready <ms>` fixed-ms delay falses-GREEN on the un-split design (CRITICAL 2 arithmetic, confirmed): `attach` returns at ~`SLOW_READY_MS`, and the old single `select!` then drains the buffered A-chunks INSTANTLY — inside any post-`start` budget — so "sleep + BUDGET < SLOW_READY_MS" (`BUDGET < 0.4 × SLOW_READY_MS`, i.e. `< 600` at 1500, `< 800` at the 2000/1200 fallback) is violated by the plan's 800 — and any ms-delay variant has to place its measurement window by CLOCK GUESSES the old design can win. A timing-margin test is the wrong instrument: the measurement window is closed not by a guessed clock margin but by a **test-gated `ready`** — the fixture's `ready` frame is HELD UNTIL THE TEST RELEASES IT. `start(B)` then parks the reader inside `attach` and CANNOT return until the release; the test observes the A-chunk stream ON THE SOCKET while the gate STILL HOLDS and issues its verdict before releasing (Step 9, Part E + the relay-probe bullet). The old design is red **by construction**: while the reader is provably parked (arrival-stamp observed), its fan-out arm is unpolled AND there is NO other task that can write the socket — the old design is STRUCTURALLY SILENT in the measurement span, regardless of any clock arithmetic; the new design's writer task is the only thing that can place a chunk in the span, which is the whole proof. Verified against the repo: `attach` (manager/mod.rs:225) is `factory.spawn()` + `wait_ready(Duration::from_secs(5))` — the hard 5 s ceiling, inside which the gate park (~stamp-wait + absorb + probe, typically 1-2 s on the new design; for RED runs the probe waits its full `RELAY_ARRIVAL_BUDGET` = 1.5 s but the release still follows immediately after) sits by the round-4 budget-sum guard — sees the round-4 🔴 amendment — then the release to complete B's `start-result`. One bounded fallback pair is named (Step 9, consts), with the corrected inequality `budget < 0.4 × SLOW_READY_MS` spelled out in its comment — for a test that still wants a ms delay; the gated design is the primary route and the fallback is only a documented What-If. See the fixture section for the exact gate plumbing and the tests/serve.rs parts for the red-by-construction traces.
**31b (ROUND-4-🔴 amendment — ONE-LINE-CLASS surgical fix to 31a/Step 9, the consts + their justifying prose ONLY; the gate mechanism, the absorb→probe verdict structure, the fixture changes, and every other sentence of this plan are untouched — the rest of this plan has passed four vet passes and is returned unchanged in substance).** The test-gated `ready` can HOLD LONGER THAN `attach`'s 5 s ceiling, and that fails as a FALSE GREEN — measured in the engine, not assumed: `src/worker/manager/mod.rs:225` calls `handle.wait_ready(Duration::from_secs(5))`. A gated fixture that holds `ready` for more than ~5 s does NOT keep the reader parked forever: `attach` TIMES OUT, `handle_frame` returns an error, the old single-task `serve_client` resumes its `select!`, which then DRAINS the buffered session-A chunks onto the socket — a probe running after that point sees a chunk and reports the split works ON CODE THAT HAS THE BUG, so the whole proof inverts. The pre-fix budgets (`READY_GATE_STAMP_BUDGET = 4 s` + `ABSORB_BUDGET = 700 ms` + `RELAY_ARRIVAL_BUDGET = 2 s` ≈ **6.7 s > 5 s**) violated that ceiling, with prose claiming they sat "comfortably inside" it. Fixed, minimally and completely: (1) **the budgets are now DERIVED from the ceiling** — a single named `ATTACH_READY_CEILING: Duration = Duration::from_secs(5)` mirrors the engine constant (comment cites the `wait_ready` call at manager/mod.rs:225 by symbol, and teaches why the sum must sit below it: exceeding it un-parks the reader via a timeout and manufactures the false green); the stamp budget is 1.5 s (the stamp is written at fixture startup, landing in milliseconds — a broken-fixture panic timer, not an expected wait), absorb 700 ms, probe 1.5 s → **`STAMP + ABSORB + PROBE = 3.7 s < 5 s`**, ~1.3 s of real headroom; (2) **the ceiling is a TESTED invariant, not prose** — a `const _: () = assert!(STAMP.as_millis() + ABSORB.as_millis() + PROBE.as_millis() < ATTACH_READY_CEILING.as_millis(), …)` in the gate-consts block (the same class of guard the repo already has for 4096/1024, `serve.rs:284/:290`), so a one-number retune can no longer silently break the proof. **The `.as_millis()` spelling is mandatory:** `Duration`'s `Add` is NOT a const trait on this toolchain, so the direct `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` form does not compile in const context — the assert MUST be the millisecond-sum form (see the gate-consts block), and if the implementer still cannot make it `const`, the plan marks it `UNVERIFIED` and mandates a runtime `assert!` at test start carrying the same message. (3) **the timeout behaviour is stated honestly** — a stamp that never arrives fails loudly (the existing panic, kept), and a hold that outlives `attach` is a TEST BUG that invalidates the verdict rather than a valid red (stated in the fixture bullet and the stamp-wait + release bullets); (4) the ms-delay fallback AND its `0.4 ×` inequality are left intact (a documented alternative, not the chosen path), and now also state that D inherits the same 5 s ceiling; (5) the loose "seconds at most"/"fits comfortably" prose is fixed to the backed 3.7 s model. See the Step 9 gate-consts bullet for the const block and assert; the "What NOT to change" list at the top is otherwise untouched.
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

`transport::frame`'s own module doc and `serve_client`'s "KNOWN LIMITATION" block (serve.rs:1500-1543) document the hazard this work exists to respect: a JSONL stream cannot resynchronise once two writers interleave a half-written line, so `Connection::pending` (the tail of a frame whose `write` was only partially accepted) must be reachable by exactly ONE writer. Today `pending` lives on `Connection` and the production `send_frame` call sites are split between two regions of the same single task — the T3 refusal pair (serve.rs:1644, :1668) and the drain pair (serve.rs:1726, :1731), all `&mut connection` inside one `serve_client`/`select!`. Those call sites are why `serve_client` runs `handle_frame(…).await` inline and stalls its own relay for up to ~5 s: there is no second task that could safely own a second write handle.

This task (1) makes the framing structurally safe to split (writer half owns `pending`), and (2) uses that to split `serve_client` into a reader/reply task and a writer task. The writer task becomes the SOLE `send_frame` owner; the reader task owns the inbound half, the `watching` set, `handle_frame`, and (per T3, see the T3 marker below) T3's handshake/rate/session-limit state.

**⚠ T3 dependency — T3 HAS LANDED (`6d6887a`), this plan now builds on its committed shape.** T3's wave-1 commit rewrote the exact region this plan's earlier "Builds on:" named; the current plan is reconciled against the committed code (see the "Reconciled against T3 `6d6887a`" list at the top). Concretely: `serve_client` is now `async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection)` (serve.rs:1528) with the T3 hardening state in its header region — `let mut handshake = Handshake::WaitingHello { pre_hello: 0 };`, `let mut rate = FrameRateGate::new(std::time::Instant::now());`, `let mut seen = 0usize;` (serve.rs:1559-1570) — and FOUR production `send_frame` call sites (the pre-hello refusal at :1644, the rate-gate refusal at :1668, and the drain pair at :1726/:1731). The split must carry the handshake/rate/seen state into the READER half (the half that sees inbound frames) and route the two REFUSAL `send_frame`s through a refusal channel (D1), while the drain pair is the WRITER's. The two tasks share only `dropped` and `watching` (both behind an `Arc<StdMutex<…>>`, D2); the writer cannot go anywhere without the reader declaring where the T3 state lives.

**Critical preconditions from the dispatch (verified against this tree):**

1. **There is NO existing behavioural pin to inherit — CONFIRMED.** `grep -rni "partial"` over `fulcrum/src/transport/` and `fulcrum/tests/` (re-ran on the post-T3 tree, `d38423f`) matches only doc prose and `serde` derive hits plus T3's doc-comment mentions; there is no `pending`-loop test and no two-writer test anywhere. The acceptance-criterion "existing partial-write tests still pass" is **vacuous** as it stands. **Writing the failing-frame test is therefore the FIRST implementation step** (Step 1 below), before any split. A split that ships without a test that fails when `pending` is shared is shipping the hazard (the exact class of desync `frame.rs` exists to prevent) back.
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
  - `src/fixtures/worker.rs` (the test-gated `ready` — env-named marker file the fixture waits for between `hello` and `ready` — now in this task's owned set)
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
/// Ask the OS to close the write direction of the whole duplex, even while
/// the read half is still alive. `tokio::io::split`'s `WriteHalf` holds a
/// SHARED `Arc<Inner<T>>` and has NO `Drop` impl, so dropping it does NOT
/// close a socket while any other half's `Arc` is alive (verified in tokio
/// 1.54 `src/io/split.rs`); a writer parked inside `send_frame`'s write loop
/// would stall there forever. `poll_shutdown` on the shared stream is what
/// makes FUTURE `write`s on this direction fail and tells the peer that this
/// direction is over (EOF) — it does NOT reliably un-park a `write` already
/// blocked inside the OS (shutdown(2) semantics: the wake latency of a parked
/// send is kernel-dependent), so this method is the direction-closer + the
/// peer-EOF signal, NOT a guaranteed parked-write breaker. For the daemon's
/// connections the writer's parked `write` is broken by the PEER'S CLOSE or —
/// on the reader-first path — by the socket's full close when the last `Arc`
/// (the reader's `Connection` and the writer task's future) drops; see D3.
///
/// MUST be called by the exiting writer AFTER `send_frame` returns (never
/// from inside a `select!`, see A1c). It is a best-effort close: an `Err` is
/// ignored, the callers exit on their own schedules regardless.
pub(crate) async fn shutdown(mut self) {
    let _ = self.writer.shutdown().await;
}
```

This shutdown is the writer-side analogue of dropping a whole `Connection` today — it does not replace the reader's own `recv` EOF (the peer closing inbound still ends the reader). It is the ONLY new method the writer needs for teardown, and it is the ONLY place `poll_shutdown` is called on the shared duplex in the entire split (the round-1 "reader pre-split shutdown", old A2b, is deleted — see the reconciliation list at the top, item 8).

- **A1c (NEW — no flush in the select).** Because an await inside the selector arms must never park the writer's loop: the plan requires BOTH of `send_frame`'s awaits (the write loop AND the final `flush`) to be cancellable from the writer's own exit, and it makes the final `flush` a one-liner that a `select!` CAN poll. Since `send_frame`'s body is otherwise a verbatim move (its `flush` is a no-op on a socket), the writer loop's DRAIN arm (D3) will call `send_frame` OUTSIDE the `select!` — via a small wrapper future `send_frame_no_select(&mut writer, frame)` that the `select!` treats as a plain future. See D3's exact shape. This is the one place the plan deviates from a bare move; the deviation is purely mechanical (the flush becomes its own awaitable) and the executable guarantee (a parked block cannot outlive the writer task) is the pin the reader tests in Step 1.

- **No custom `Drop` on `ConnectionWriter`.** Dropping it drops the `WriteHalf` (a no-op on the shared stream — see A1b's correction); there is no reader task to abort. Do not add a `Drop` impl.

**A2. Strip the writer out of `Connection`** — the struct becomes the READER half (plus the dormant writer slot for `split_writer` and the transient delegator A3a):

```rust
pub struct Connection {
    /// The write half, held ONLY so `split_writer` can hand it out once.
    /// `None` after `split_writer`. The ONLY write method on this type is
    /// the transient delegator `send_frame` (A3a), which errors once the
    /// slot is `None`; it is deleted in T4b.
    writer: Option<ConnectionWriter>,
    lines: mpsc::Receiver<Result<serde_json::Value, TransportError>>,
    reader: tokio::task::JoinHandle<()>,
}
```

- **Removed fields:** `pending: Vec<u8>` moves into `ConnectionWriter` (the structural guarantee — `pending` is reachable ONLY there); `writer: tokio::io::WriteHalf<DuplexBox>` becomes `writer: Option<ConnectionWriter>` on `Connection` (the dormant slot `split_writer` takes, A3). The slot holds the whole `ConnectionWriter` — not a bare `WriteHalf` — because the convenience delegator `Connection::send_frame` (A3a) must borrow the writer OUT of the `Option` to reach `pending`; a bare `WriteHalf` slot would leave the delegator with no `pending` to delegate to. `ConnectionWriter` is the ONLY type that can name `pending`, so the `Option` being `None` after `split_writer` is exactly what makes the delegator and the split mutually exclusive.
- `Connection::new<S>(stream: S) -> Self` keeps its signature and `pub(crate)` visibility UNCHANGED. Its body stores the split's write half as `writer: Some(ConnectionWriter { writer: write_half, pending: Vec::new() })` (see A3) and otherwise runs the reader unchanged: the `mpsc::channel(MAX_QUEUED_INBOUND_FRAMES)`, the `tokio::spawn` reader with its `read_capped_line` loop and its five `LineRead` arms, the `trim_end`/UTF-8/empty-line handling, the `Drop { reader.abort() }`, the manual `Debug` — all unchanged (verified on the post-T3 tree, frame.rs:161-228). The doc comment on `new`'s split step must be updated to say the write half is kept in the struct ready for `split_writer` (NOT spawned anywhere, NOT written from this half).
- **A2b (DELETE — retired in this reconciliation).** The old round-1 plan added `Connection::shutdown_while_writer_intact(&mut self)` here, to let the READER poll-shut the duplex before `split_writer`. That method is DELETED and must NOT be implemented (see reconciliation item 8): it was a leftover of a retired teardown design, and calling `poll_shutdown` on the shared duplex at `serve_client` start would close the daemon's OWN write direction before the writer ever sends — breaking every reply, relay and `gap` on the connection. The split's ONLY `poll_shutdown` call is `ConnectionWriter::shutdown()` (A1b), called by the writer on its exits. A2b's replacement is the refusal-channel seam described in D1: the reader's T3 refusals go out via a `oneshot` + `ack`, never through a `Connection` writer method (after the split the reader half has no write path at all).

**A3. Recover the writer on demand, once:**

```rust
impl Connection {
    /// Return the connection's write half (and its exclusive `pending`),
    /// leaving this `Connection` owning only the reader.
    ///
    /// May be called at most ONCE. The returned `ConnectionWriter` is the
    /// ONLY way to send frames on this connection after the split; the only
    /// method on `Connection` that writes is the transient delegator
    /// `send_frame` (A3a), which errors once this has been called. A second
    /// call is a caller bug and panics (this is the same trust
    /// `tokio::io::split` gives).
    pub fn split_writer(mut self) -> (Self, ConnectionWriter) { /* … */ }
}
```

- `split_writer` returns both halves; the daemon task keeps the reader half (its `recv` is the connection's inbound) and hands the writer to the writer task; `serve.rs` keeps both (D1).
- **One concrete design, chosen here so the implementer does not improvise:** add a private field `writer: Option<ConnectionWriter>` to `Connection` (replacing today's bare `writer: tokio::io::WriteHalf<DuplexBox>`), and implement:
  ```rust
  pub fn split_writer(mut self) -> (Self, ConnectionWriter) {
      let writer = self.writer.take().expect("the writer was already taken");
      (self, writer)
  }
  ```
  The `Option` means an abandoned `Connection` still carries its `Some(writer)` — dropping the WHOLE connection (reader half + the writer `Arc` slot, i.e. the last two `Arc`s, since the reader and writer halves share the stream) reaches the stream's final `Drop`, which is what closes the socket and stops the reader task (`frame.rs::Connection`'s `Drop { reader.abort() }`). A SECOND `split_writer` call hits the `expect` and panics — that panic IS the two-writer hazard surfacing structurally (the `#[should_panic]` regression test in Part E pins it). The frame.rs unit-test module must not construct `Connection{ writer: None, … }` directly; tests that need both halves call `split_writer`.
- **A3a (NEW — the transient delegator that keeps commit T4a independently landable).** `Connection::send_frame` is NOT deleted in T4a; it is RE-HOMED as a delegator that borrows the writer out of the `Option` slot, so the four production call sites in `serve.rs` (the two T3 refusal paths at :1644/:1668 and the two drain-path sites at :1726/:1731 — all in the `fulcrum` LIB target, which every integration test links) keep compiling until T4b converts them:
  ```rust
  /// Convenience delegator, kept ONLY while the split lands in two commits
  /// (T4a frame split first, T4b serve split second): `serve.rs`'s four
  /// production `send_frame` call sites (the T3 refusal pair and the drain
  /// pair) are converted in T4b, so this method must keep compiling until
  /// then. It borrows the writer out of the `Option` slot, so it is the
  /// SAME `&mut`-exclusive path as `ConnectionWriter::send_frame` — see the
  /// note below for why that keeps the two-writer guarantee intact.
  ///
  /// DELETED in T4b, once `serve.rs`'s four call sites are converted. It
  /// must NOT survive the final state: leaving it would let a future caller
  /// hold a `Connection` AND a `ConnectionWriter` at once — two handles to
  /// `pending` — which is issue #3's hazard. The deletion is load-bearing,
  /// not tidiness (see Step 11).
  pub async fn send_frame<T>(&mut self, frame: &T) -> Result<(), TransportError>
  where
      T: Serialize + ?Sized,
  {
      let Some(writer) = self.writer.as_mut() else {
          // The writer was taken by `split_writer`; this `Connection` is the
          // reader half now and has no write path. Fail loudly rather than
          // silently dropping the frame (pinned by
          // `send_frame_after_split_writer_fails_not_drops`).
          return Err(TransportError::Io(std::io::Error::new(
              std::io::ErrorKind::NotConnected,
              "the writer was taken by split_writer",
          )));
      };
      writer.send_frame(frame).await
  }
  ```
  **Why this keeps the structural guarantee rather than weakening it:** `pending` now lives ONLY inside the `ConnectionWriter`, and BOTH access paths take `&mut` — `send_frame(&mut self)` borrows the writer out of the `Option` (`Self`'s `&mut` is exclusive), and `split_writer(self)` CONSUMES the `Connection` — so two tasks still cannot reach `pending`. The `Option` being `None` after `split_writer` is what makes the convenience method and the split mutually exclusive: a `Connection` that has been split cannot send (the delegator returns `Err`), and a `Connection` that has not been split still has exactly one writer. The error variant is `TransportError::Io(ErrorKind::NotConnected)` with the message `"the writer was taken by split_writer"`, and the new test `send_frame_after_split_writer_fails_not_drops` pins BOTH that it errors AND that it does not silently swallow the frame (Step 1's test list).
  **Why the delegator is acceptable TRANSIENTLY but the FINAL state must not keep it:** issue #3's hazard is two tasks reaching `pending`, and the delegator is only safe while it is the SOLE remaining handle — during T4a the only way to reach `pending` is through the one `Connection`'s slot, so the guarantee holds. Leaving it after T4b would let a future caller hold a `Connection` and a `ConnectionWriter` at once — two independent handles to the same `pending` — which is exactly the interleave hazard `frame.rs` exists to prevent. So T4b's deletion of `Connection::send_frame` is load-bearing, not tidiness, and `a_second_split_writer_panics` plus the `Option`-is-`None` error (`send_frame_after_split_writer_fails_not_drops`) are what pin it.
- **API surface note:** this is the ONE genuinely new public item on `transport` (the split's reason for existing). The `boundary.rs` source census (`tests/boundary.rs`, which walks `fulcrum/src` and asserts module allow-lists and a `4`-file `transport/` floor) checks PATHS and declared modules, NOT symbols — `split_writer` and `ConnectionWriter` do NOT trip it (verified: `transport` stays 4 files, `frame.rs` stays one file; boundary.rs has no symbol list that mentions `send_frame`/`pending` — grep confirms). `tests/boundary.rs` is NOT in the owned set and is NOT modified.

#### Part B — `fulcrum/src/transport/mod.rs`: one re-export (T3-shape confirmed)

- Add one line next to the existing `pub use frame::Connection;` (line 76, post-T3):
  ```rust
  pub use frame::{Connection, ConnectionWriter};
  ```
- **T3-shape check (reconciliation item 6):** T3 added `TransportError::PeerIdentity { peer_uid: u32, expected_uid: u32 }` (~:198) — a NEW VARIANT of the existing enum, matching `run`'s counted-rejection arm (serve.rs:1455-1471). It needs no re-export edit (the enum's `pub use` is unchanged) and it does NOT appear in `mod.rs`'s `pub use unix::{connect, default_endpoint, Listener};` / `pub use windows::{connect, default_endpoint, Listener};` lines, which remain byte-identical. Part B's edit is exactly the one line above and nothing else.
- The `Endpoint` / `TransportError` enums are unchanged (the enum gains a variant, no removal).

#### Part C — `fulcrum/src/transport/unix.rs` / `windows.rs` (no change — verify only)

- `Listener::accept -> Result<Connection, TransportError>` keeps its exact shape on both platforms; the construction sites `Ok(Connection::new(stream))` (`unix.rs` accept + connect) and `Ok(Connection::new(server))` / `Ok(Connection::new(client))` (`windows.rs` accept (in `connect`'s body) + connect) are UNCHANGED. **No `Connection::new` call site changes**, so these two files should receive zero edits. If `Connection::new`'s signature were found to need changing, that is a `DEVIATION:` (Step C checks).

#### Part D — `fulcrum/src/serve.rs`: the two-task split

**D1. The reader/reply task and the writer task.**

```rust
async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection) {
    // The one piece of state the READER carries that the writer never sees:
    // the T3 hardening state (issue #4) plus `seen`. `connection` starts as
    // the full `Connection`, splits into reader-half + writer-half below, and
    // the two tasks are spawned AFTER the split. The maps (`watching`,
    // `dropped`) live in `serve_client`'s scope so both tasks can name them.
    //
    // 1. Required-`hello` handshake state (T3, moves as-is):
    let mut handshake = Handshake::WaitingHello { pre_hello: 0 };
    // 2. Per-connection inbound frame rate (T3, moves as-is):
    let mut rate = FrameRateGate::new(std::time::Instant::now());
    // 3. The frame counter whose exit rule distinguishes "a probe that
    //    connected and hung up" from "a client that talked to us and left"
    //    (T3, serve.rs:1559) — READER-half, see D3.
    let mut seen = 0usize;

    let dropped: Arc<StdMutex<u64>> = Arc::new(StdMutex::new(0));
    let watching: Arc<StdMutex<HashSet<String>>> = Arc::new(StdMutex::new(HashSet::new()));

    // == T3 RELOCATE INTO READER HALF (now CONCRETE — reconciled against
    //    6d6887a) ==
    // T3's header-region locals move into `reader_loop` as-is — they are the
    // one reader-owned piece of per-connection state, and their real type
    // names are `Handshake::WaitingHello { pre_hello }`,
    // `FrameRateGate::new(Instant)`, and `seen: usize`. The reader owns them
    // because it is the half that sees inbound frames; the writer never
    // reads them. The two REFUSAL send_frame calls that T3's gate logic makes
    // (serve.rs:1644, :1668) go through a refusal `oneshot` + `ack` (below) —
    // the reader has no write path after `split_writer`.
    // == /T3 RELOCATE ==

    // The outbound queue (replies + relayed events), BOUNDED — see
    // [`CLIENT_QUEUE_CAPACITY`]. Every push is a `try_send` via `enqueue`.
    // `Sender` is `Clone`: BOTH tasks hold one (the reader for replies, the
    // writer for the relay + the refusal daemon) — so the channel can NEVER
    // close by dropping a single sender. Teardown is drop-based (D3), never a
    // join: the reader returns when its `recv` EOFs and DROPS its sender; the
    // writer exits when its `queued.recv()` returns `None` (the reader's
    // sender gone) or when a `send_frame` errors (peer gone) or when the
    // fan-out closes; and the one exit NO writer path reaches — the peer
    // closed its INBOUND half but left the write direction open, so no error
    // fires — is reaped by `serve_client`'s `writer_task.abort()` the moment
    // the reader's task returns.
    let (tx, queued) = tokio::sync::mpsc::channel::<serde_json::Value>(CLIENT_QUEUE_CAPACITY);

    // T3's wire-auth refusal (the ONLY reader-side write left after the
    // split): TWO one-shots — the refusal frame, and the writer's ACK that it
    // actually sent it. The reader sends the refusal and AWAITS the ack before
    // breaking, which reproduces today's inline `send_frame(...).await; break;`
    // exactly ("told ONCE, then closed" — the refusal is really on the socket
    // before the connection ends). If the writer is already gone (abort path)
    // the refusal `send` returns `Err(frame)` and the reader breaks
    // immediately; if the writer dies between send and ack, the ack sender is
    // dropped and the reader's `ack_rx.await` returns `Err` — same break. A
    // peer that neither reads NOR closes can park the writer's refusal send —
    // that parks the reader's ack await too, which is IDENTICAL to today's
    // inline behaviour (the connection task stalls until the peer leaves; D3's
    // "open-but-unreading peer" case, not a teardown case — the test suite's
    // daemon kill unparks every socket).
    let (refuse_tx, refuse_rx) = tokio::sync::oneshot::channel::<serde_json::Value>();
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();

    // THE RELAY LIVES IN THE WRITER TASK — this is the whole fix. The reader
    // awaits `handle_frame` (a `start`'s `attach` for up to ~5 s); if the
    // fan-out arm shared the reader's `select!` it would still stall for that
    // window. Only the task that never calls `handle_frame` can carry the
    // relay, so the writer does: fan-out → `watched` → `enqueue`, and
    // `queued` → `send_frame`.

    // `split_writer` hands the writer half (and its exclusive `pending`) to
    // `writer_loop`; `connection` becomes the reader half. SPLIT FIRST, then
    // spawn — the writer task must receive its half already moved.
    let (connection, writer) = connection.split_writer();

    let reader = tokio::spawn(reader_loop(
        Arc::clone(&daemon),
        connection,
        tx.clone(),
        Arc::clone(&dropped),
        Arc::clone(&watching),
        refuse_tx,
        ack_rx,
    ));
    let writer_task = tokio::spawn(writer_loop(
        writer,
        queued,
        tx,
        daemon.fan_out.subscribe(),
        Arc::clone(&dropped),
        watching,
        refuse_rx,
        ack_tx,
    ));
    // The reader's whole life-span — `await`, NOT `select!`: the reader's only
    // input is the inbound channel, which is why the split does not need it to
    // share its loop with the relay. This await can park for up to
    // `attach`'s ~5 s (a `start`); that is the bounded per-connection stall
    // issue #3 accepts, and it is exactly why the relay lives in the writer
    // (see D1's relay note). `connection` is `mut` (post-T3 signature) — the
    // reader's `recv` takes `&mut self`.
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

- `serve_client` remains `async fn serve_client(daemon: Arc<Daemon>, mut connection: Connection)` (serve.rs:1528 — the post-T3 signature; `mut` is correct because after the split `connection` IS the reader half). `run`'s `tokio::spawn(serve_client(daemon, connection))` at `serve.rs:1439` is UNCHANGED.
- **Reader/reply task (`reader_loop`, spawned at D1)** — owns the (now reader-half) `Connection`, `watching`, `seen`, `handshake`, `rate`, the shared `dropped`, its `tx` clone, and the refusal `oneshot` sender. Its loop is a plain `while let Some(frame) = connection.recv().await` (a single future, no `select!` — the reader's only input is the inbound channel), and it is the post-T3 `serve_client`'s inbound arm re-embodied:
  - `seen += 1`.
  - Malformed line (`Err(e)`) ⇒ `enqueue_shared(&tx, error-frame, &dropped)`; on `false` (closed) break.
  - `Ok(frame)` ⇒ the T3 handshake gate (verbatim T3 logic: `is_hello`; count `pre_hello`; `refuse_pre_hello` / `drop_pre_hello` transitions on `&mut handshake`); on `refuse_pre_hello` ⇒ eprintln + send the refusal error via `refuse_tx`, then `let _ = ack_rx.await;` + break; on `drop_pre_hello` ⇒ `continue`.
  - The T3 rate gate: `if !is_hello && !rate.accept(now)` ⇒ eprintln + the same refusal send + ack-await + break.
  - `handle_frame(&daemon, &mut *watching.lock().unwrap_or_else(|p| p.into_inner()), &frame).await` INLINE (this await may park for `attach`'s ~5 s — that is fine now, because the RELAY and the SOCKET WRITE moved to the writer; the reader blocking itself is exactly the bounded per-connection stall the issue accepts) ⇒ `enqueue_shared(&tx, reply, &dropped)`, break on `false`.
  - `recv() == None` (EOF — the peer closed its inbound half) ⇒ break (NO raw-bytes drain — see the correction below).
  - On break: **drop `tx` and the `refuse_tx`** (NOT a stop signal — see D3), then the epilogue: the `seen > 0` disconnect line (`"fulcrum serve: client disconnected after {seen} frame(s)"`) and NOTHING else (no `detach`, no `kill`, no join, no reader-side socket close — the close now rides the socket's final `Drop` after the abort; see D3).
- **Writer task (`writer_loop`, spawned at D1)** — owns `ConnectionWriter` (exclusively), the `queued` receiver, its `tx` clone (for the relay arm's `enqueue`), the fan-out subscription, the shared `dropped`/`watching`, and the refusal `oneshot` receiver. Runs the RELAY, the DRAIN and the REFUSAL in one `select!`:

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
///    (its close, or the socket's final `Drop` after the abort; on the
///    reader-first path the last `Arc` of the stream drops when BOTH the
///    reader's `Connection` and this task's `ConnectionWriter` are gone, so
///    the write errors).
/// 2. `queued.recv() == None` — the channel is closed and empty: the reader's
///    `tx` is gone, and the writer's OWN `tx` (held for the relay arm) is
///    dropped no later than this task's return. (On the abort path this never
///    fires — the task is merely un-parked, see below.)
/// 3. The refusal `oneshot` fires (`refuse_rx.await`) — T3's wire-auth path:
///    the reader's gate (pre-hello or rate) closed this connection. Sends the
///    one refusal frame with `send_frame_no_select`, then acks the reader's
///    `ack_rx` (so the reader's exit is ordered after the refusal is on the
///    socket — "told ONCE, then closed"), then breaks.
/// 4. fan-out `Closed` — the [`Daemon::fan_out`] sender's broadcast is gone.
///
/// Every exit path calls `writer.shutdown()`, updates the `dropped` epilogue
/// if needed, and returns; its `tx` and `ConnectionWriter` drop with it.
///
/// The one exit NO writer path reaches is the peer that closed only its
/// INBOUND half and left the write direction open: no error fires in
/// `send_frame` and `recv` never returns `None` for the writer's queue until
/// the reader's sender drops. That path terminates from OUTSIDE: `serve_client`
/// `abort()`s the task's `JoinHandle` when the reader returns. The abort is
/// what makes the parked write-loop's drop SYNCHRONOUS — the task's future is
/// dropped without polling it, so its async `Drop` never runs, so the `flush`
/// its drop would have awaited is never awaited. This is the round-2 🔥
/// invariant: **a writer parked inside `send_frame` never outlives its
/// `serve_client`** (Step 1's test proves it; D3 spells out who drops what,
/// in what order).
async fn writer_loop(
    mut writer: ConnectionWriter,
    mut queued: tokio::sync::mpsc::Receiver<serde_json::Value>,
    tx: tokio::sync::mpsc::Sender<serde_json::Value>,
    mut events: tokio::sync::broadcast::Receiver<serde_json::Value>,
    dropped: Arc<StdMutex<u64>>,
    watching: Arc<StdMutex<HashSet<String>>>,
    mut refuse_rx: tokio::sync::oneshot::Receiver<serde_json::Value>,
    ack_tx: tokio::sync::oneshot::Sender<()>,
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
            },
            // T3's wire-auth refusal (see D1): the ONE reader→writer control
            // message. `send_frame_no_select`, then ack so the reader does not
            // proceed past its break while the frame is still buffered.
            refused = &mut refuse_rx => {
                let Err(frame) = refused else { break }; // sender dropped: reader is gone
                let _ = send_frame_no_select(&mut writer, &frame).await;
                let _ = ack_tx.send(()); // reader's exit is ordered after the refusal is sent
                break;
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
    // un-park a write already blocked in the OS, round-2 🟡; the abort + the
    // peer's close are what un-park in a real connection. The writer calls it
    // as best-effort cleanup on every exit.)
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
    It exists to make the teardown promise executable rather than argued: the writer's ONLY awaits inside the `select!` are `queued.recv()`/`events.recv()`/`refuse_rx`; `send_frame` is a future the selector can drop when a competing arm fires. (`send_frame_no_select` is new and private; it is not `send_frame`'s replacement — `send_frame` itself stays on `ConnectionWriter`.)
  - **The refusal request (not an own helper — the reader does these three statements inline):**
    ```rust
    // Reader-side, where the gate first trips:
    let _ = refuse_tx.send(error_frame);   // the writer's refusal arm picks this up
    let _ = ack_rx.await;                  // ordered EXIT: the refusal is on the socket (or the writer is gone)
    break;
    ```
    The `ack_rx` await returns `Err` (Canceled) when the writer task died between send and ack — same break. **No `refuse_frame` helper is needed**; Step 8's mention of a `refuse_frame` fn refers to this inline shape. `send_frame_no_select` remains the only new free fn.
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
- **T3 marker (reconciled — concrete, no re-dispatch pending):** T3's per-connection state (`Handshake`, `FrameRateGate`, `seen`) lives in the READER task (D1 marker). With the relay now in the writer, T3's fan-out arm no longer exists in `serve_client`'s single `select!` (it moved to `writer_loop`) — T3's wire-auth refusals (the pre-hello and rate-gate `send_frame`s) become D1's refusal `oneshot` + ack arm, and seen/dropped split exactly as D3 states. T3's per-frame rate limiting applies at the INBOUND edge (reader), which is unaffected. No re-dispatch is needed; the reconciliation list at the top is the delta.

**D3. Teardown — the honest, deadlock-free, drop-based contract (CRITICAL 1 fix).**

The vet's 🔴 finding, verified against the code, and why every "join the writer" shape is wrong:

1. **A reader `await`-ing the writer's `JoinHandle` after a `stop` one-shot deadlocks.** `ConnectionWriter::send_frame` loops `self.writer.write(&self.pending).await` with NO timeout (`frame.rs:274-282`; verified — no `timeout`, no `select!` inside `send_frame`). A peer that stopped reading parks that loop at the OS send buffer. While the future is parked, NONE of the writer's `select!` arms is pollable, so `stop` is unreachable, the join never resolves, and the `serve_client` task hangs forever. A hung task is indistinguishable from a leaked process to T1's process census running on this same branch — the integrated gate would redden.
2. **The mirror path is equally bad.** Once the fan-out `Receiver` yields `Closed` (only on a full daemon teardown — none today), the writer has no non-error exit, and the same join hangs.
3. **"Dropping the `ConnectionWriter` closes the write direction" is FALSE in this design** — the vet's suggested mechanism, sharpened against tokio 1.54's actual `split` (verified in tokio 1.54.1 `src/io/split.rs:20-42`): `WriteHalf<T>` wraps `Arc<Inner<T>>` with **no `Drop` impl**, so dropping one half does NOT close the duplex while the other half's `Arc` is alive. The parked `write` would NOT error; a dropped `WriteHalf` alone cannot break it. (In today's single-task design, dropping the whole `Connection` drops BOTH halves and the reader task, which is why today's teardown works — the split must not pretend the old shape survives.)
4. **The guaranteed unblocker of a parked `write` is the PEER'S CLOSE — and that is exactly what ending a connection means.** A blocked `self.writer.write(&self.pending)` fails (EPIPE / ECONNRESET) the moment the peer closes its end, which is precisely the event that ends a connection in this daemon. The split introduces NO new way to stall: a peer that neither reads NOR closes can also stall today's single task in the same `send_frame` — the daemon deliberately has no send timeout (backpressure is the design), so an open-but-unreading peer stalling the writer is the SAME behaviour as today, and it is not a teardown case (the connection never ends; the test suite's daemon kill closes every socket, so T1's census sees no leak). For the cases where the connection DOES end, the writer's write unwakes on the peer's close alone. `AsyncWriteExt::shutdown` on the shared `Inner` (`poll_shutdown` in `split.rs:79-82`) closes the WRITE DIRECTION: FUTURE `write`s on this fd fail and the peer sees EOF. It does NOT reliably un-park a `write` already blocked inside the OS — the wake latency of an already-parked send is kernel-dependent, so the plan never relies on shutdown to BREAK a parked write (round-2 🟡 precision; the older "makes ANY pending-or-future write fail" phrasing is retired). **The split reaches `poll_shutdown` in exactly ONE place: `ConnectionWriter::shutdown()` (A1b), called by the writer on its exits and only ever outside a `select!` (A1c).** (The round-1 plan also reached it from the reader at `serve_client` start via the deleted A2b — that call would have closed the daemon's write direction before the writer ever sent, and is one of the deletions this reconciliation makes; see the top list, item 8.) Tokio's `unsplit` panics on a consumed half and a dropped half is a no-op, so there is no "close on drop" to fall back on — the writer's `shutdown()` plus the final `Arc` drop are the whole close vocabulary.
5. **And the hole the four points above do NOT cover — the 🔥 round-2 fix.** A peer that closes ITS inbound half but leaves ITS write direction open (so the daemon's writer never sees a `send_frame` error) and never closes the TCP connection parks the daemon's WRITER inside `send_frame` indefinitely: its write loop is parked (send buffer full), and because the task is inside the future, NONE of its `select!` arms is pollable — so `queued.recv()==None` (the reader's `tx` drops when `reader_loop` returns), the fan-out, the refusal `oneshot` and every other arm are all unreachable. So `serve_client` KEEPS the writer's `JoinHandle` and `abort()`s it after `reader_loop` returns (D1): the abort cancels the parked `send_frame` future — its `Drop` is SYNCHRONOUS (the task's future is dropped without polling it), so no `Drop`-time `flush` is ever awaited (a flush on a shutdown-locked fd hangs), and the loop is dead with `serve_client`. This is `abort`, not `join`: joining is what can never resolve; aborting is exactly what makes the parked loop's drop synchronous and gives "no task outlives `serve_client`" MECHANICAL enforcement. (The honest, avoided alternative: a spawn-outside-`serve_client` peer task with an explicit kill tok — rejected, it is quit-on-message choreography the abort makes unnecessary.) **The reader does NOT drain bytes on EOF before breaking** — the old `read_to_end` step is deleted (reconciliation item 7: `ReadHalf::poll_read` never calls `poll_shutdown`, so the drain conveyed nothing to the write direction anyway). The abort is the ONLY mechanism that carries "the writer must not outlive `serve_client`", even on the reader-first path; no design decision rides on the drain.

**Who drops what, in what order (the tested invariant):**

- **Reader's exit (peer EOF inbound):** `recv() == None` → break → drops its `tx` and the `refuse_tx` (its `Connection` drop then aborts the read task) → returns from `reader_loop`. The reader does NOT call any shutdown, does NOT drain raw bytes (deleted — see above), and its half of the teardown is complete when its future and its `Connection` drop. The writer's unblocking on this path is the abort's job (D1's backstop).
- **`serve_client` after `reader.await` returns (round-2 🔥):** `writer_task.abort()` — unconditionally, on every path out of `reader.await` (including the reader's early `break` exits, which all funnel through `reader_loop`'s return). The abort is a no-op for a writer that already terminated. `serve_client` then returns; its `Arc<Daemon>` refcount drops. It NEVER awaits the writer's `JoinHandle`.
- **Writer's own exits (any of — the round-2 🟡 trimmed list, plus the new T3 refusal exit):** (a) `send_frame`/`send_frame_no_select` returns `Err` — the peer is gone (its full close, or the socket's final `Drop` when the last stream `Arc` — the reader's `Connection` plus this task's `ConnectionWriter` — is gone); (b) `queued.recv() == None` — the reader's `tx` is gone (this cannot fire on the reader-first path: the writer is inside, or has been aborted before, its own parked arm — see D3 point 5); (c) **the refusal `oneshot` fires** — T3's wire-auth path, added for this reconciliation (send the one refusal frame, ack, break); (d) fan-out `Closed` — the [`Daemon::fan_out`] sender's broadcast is gone. **There is no "both senders gone" exit** — the writer's own `tx` never sends to a channel it alone receives from; exit (b) fires on the reader's sender alone, at which point the writer's own `tx` is dropped with the task itself (round-2 🟡 precision — the old phrasing said "both senders gone", which is unreachable). Every exit path then calls `writer.shutdown().await` (A1b), updates the `dropped`-count epilogue if needed, and returns; its `tx` and `ConnectionWriter` drop with it.
- **The socket's full close (all paths):** the duplex stream's final `Drop` fires when the LAST `Arc<Inner<T>>` is gone — i.e. when BOTH the reader's `Connection` (reader half + the `Option` slot it carried into `reader_loop`) and the writer task's `ConnectionWriter` have dropped. On the reader-first path that is after the abort (the writer's future — and with it its `ConnectionWriter` — is dropped by the abort). A connection whose peer simply closed needs no close: the peer's FIN handled it. **`poll_shutdown` is never the thing that closes the socket; it is only the writer's direction-closer + peer-EOF signal on the writer's own exits (A1b).**
- **The no-join / abort rule and T1's census:** the writer is `tokio::spawn`ed INSIDE `serve_client`; `serve_client` holds the handle and `abort()`s it on every reader exit. The writer terminates on its own via one of the exits above, or is aborted by the backstop — a peer that stops reading WITHOUT closing parks the writer exactly as it parks today's single task (backpressure is the design, not a defect the split introduces), and that is NOT a teardown case (the connection never ends); every REAL teardown un-parks or aborts the writer, so no task outlives `serve_client` — T1's process census (running on this same branch) stays green.
- **Tested, not assumed:** Step 1's frame tests pin (i) that a `send_frame` on a connection whose peer stopped reading errors once the socket is shut down; (ii) that the writer loop terminates without a join when the reader's `tx` is dropped; and (iii) — the round-2 🔥 pin — **a writer parked inside `send_frame` never outlives its `serve_client`**, tested end-to-end through the real `serve` binary (Step 1's recipe + Step 9's teardown half).
- **No timeout, no join, no `stop` one-shot.** Remove every `stop_tx` / `stop_rx` reference from the old plan's design. The `oneshot` channel in this plan is the T3 refusal channel ONLY (D1); there is no general stop channel.

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

- The module header "⚠ A slow frame on ONE connection stalls THAT connection's own relay" block (`serve.rs:94-125`) and `serve_client`'s doc + KNOWN LIMITATION block (`serve.rs:1500-1543`, post-T3) currently DOCUMENT the limitation and its fix. After the split, rewrite both to state that the limitation is closed: the reader and writer are separate tasks, `handle_frame(…).await` no longer blocks the relay arm, and the write half's `pending` lives exclusively in `ConnectionWriter` (structural, not structural-via-discipline). Keep the "do not add a second writer / do not `Mutex<Connection>`" warning in the new wording — the reasoning that produced the split is now the invariant that protects it. The "gap" path's numbers (4096/1024) stay in the doc. **Also record the D4 ordering contract in `serve_client`'s doc** (a `gap` may interleave; see D4), and note in the doc that the T3 handshake/rate/seen state lives in the reader task.

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
  - `accept_one_and_echo(listener)` (`:297-307`) — this one currently does `conn.send_frame(&json!({ "echoed": frame }))` on the returned `Connection`. **After T4b, `Connection` has no `send_frame` (the A3a delegator is deleted then); the conversion is required NOW (T4a) so the helper is already on `split_writer` when the method disappears.** This helper must change to split inside and send via the writer:
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
  - `Client::send` (`:593-596`) — `self.conn.send_frame(&frame)` MUST become a `split_writer` call. Because `Client::send` may be called repeatedly, the `Client` struct must hold the writer half across calls: add a field `writer: ConnectionWriter` populated in `dial` (`let (conn, writer) = conn.split_writer();`), and `send` calls `self.writer.send_frame(&frame).await`. **`Client` is the ONE place in `tests/serve.rs` that writes frames; after `dial` splits once, every `client.send(…)` in the file keeps working unchanged.** **Round-2 🟡 (fix 5d) — `send`'s `expect` must follow the NEW EOF semantics:** today's `expect("write the frame")` was written for a `Connection` whose teardown dropped the whole socket. After the split, the daemon-side writer can finish its teardown (abort / socket close) WHILE a test is mid-`send`, so a `send` on a connection whose write half the DAEMON already closed returns `Err` — that is the new EOF semantics and it must NOT panic the harness: `self.writer.send_frame(&frame).await.expect("the daemon accepted the write — if this failed, the connection closed mid-test")` — or, better, map the `Err` to an assertion that names the teardown case (`send_frame(...).await.unwrap_or_else(|e| panic!("send failed; is the daemon tearing down this connection? {e}"))`). The assertion message must make the "writer closed during daemon teardown" reading obvious to a future test author. No production behavior changes — this is the test harness learning the split's teardown semantics (the writer exits on the peer's close or the abort; the daemon does NOT drain bytes or pre-shut the socket in this design, so the only mid-test close is the abort/peer path).
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

- Both `send_frame` call sites at `:1006` (unix leg of `serve_cli_binds_the_socket_it_was_given_and_answers_hello`) and `:1087` (windows leg) go through `dial_until_listening(endpoint).await` (`:1138-1158`), which returns a `fulcrum::transport::Connection`. **After T4b, `Connection` has no `send_frame` (the A3a delegator is deleted then); the conversion is required NOW (T4a) so the tests are already on `split_writer` when the method disappears.** **Conversion — both sites identically:**
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

**`src/fixtures/worker.rs` (the TEST-GATED `ready` — the round-3 🔴 replacement for the fixed-ms `--slow-ready` hook).**

- **The gate.** `worker_main()` currently emits `hello` then `ready` back-to-back (`src/fixtures/worker.rs:557-560`) and only then starts reading stdin. Insert a gate BETWEEN the two `emit_unstamped` calls that blocks only when an env var names a marker file that EXISTS:
  ```rust
  // The test-gated `ready` (round-3 🔴): hold the `ready` frame until the
  // test releases it — the daemon tests need "the reader is parked inside
  // `attach`" to be a FACT the test controls, not a duration it guesses.
  // The env var is read ONCE at process start; unset (or a marker that
  // does not exist) = today's behaviour, byte-identical for every
  // existing caller. POLARITY: PRESENT = HOLD, ABSENT = PASS — so the
  // gate is re-armed by simply CREATING the marker again, and a marker
  // left on disk by a previous run cannot stall a test that did not ask
  // for the gate (it must be removed, not created, to re-hold).
  if let Ok(gate) = std::env::var("FULCRUM_FIXTURE_READY_GATE") {
      let gate_path = std::path::Path::new(&gate);
      while gate_path.exists() {
          std::thread::sleep(std::time::Duration::from_millis(10));
      }
      // The arrival stamp: the fixture is entering the `ready` hold —
      // the TEST's proof that `wait_ready` is about to park (see the
      // slow-start test's stamp-wait, Step 9). Written AFTER `hello` (the
      // handshake order `tests/fixture_bins.rs` pins is preserved) and
      // while the marker still holds.
      let _ = std::fs::write(gate_path.with_extension("arrived"), b"1");
      while gate_path.exists() {
          std::thread::sleep(std::time::Duration::from_millis(10));
      }
  }
  // … then `emit_unstamped(ready)` as today.
  ```
  The marker polling loop (a 10 ms `sleep` cycle) has no deadline inside the fixture — the bound is the DAEMON side's `wait_ready` (manager/mod.rs:225, `Duration::from_secs(5)`, mirrored as [`ATTACH_READY_CEILING`] in the test); every gate release by the new test happens ~1-3 s in, well inside it (and inside the test's own budget-sum guard below, which must keep `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` — a hold that outlives `attach` is a TEST BUG that invalidates the verdict, not a valid red); a test that hangs releases nothing and fails on the daemon's `wait_ready` timeout rather than forever.
  - **Why an env var and not an argv flag:** `WorkerHandle::spawn` builds the child argv itself — `cmd.arg("--worker")` only (`build_command_with`, `src/worker/client.rs`), nothing else — so an argv flag would require threading a flag through the engine's spawn and through the fixture's shim bins (`src/bin/fake_worker_core.rs`), touching engine-owned files OUTSIDE this task's owned set. An env var set on the daemon's `Command` is inherited by every worker process: the chain is `Daemon::start` (`tests/serve.rs:236-265`, plain `Command::new(BIN)`, no `.env_clear()`) → the daemon's `WorkerHandle::spawn` → `build_command_with` → only the AppImage env patch is applied (empty under `cargo test`, verified in `hostenv.rs`) → the worker. **The gate survives `Daemon::start` → daemon → worker spawn inheritance because nothing on either spawn step clears or rewrites the env** (verified by reading both spawn sites).
  - **Why a marker FILE and not a pipe/fd the test owns:** the daemon spawns its workers itself; the test's only control surface into a worker process is its inherited environment and the filesystem (there is no fd the test can hand the daemon to forward). A marker path is inherited by name, and "present = hold / absent = pass" is re-armed by re-creating the file. The `with_extension("arrived")` stamp is the one extra file: it makes the park observable, which is what Step 9's discriminator needs (the pre-`start-result` stamp-wait).
  - **The gate lives AFTER `hello`** — the `hello`-then-`ready` order asserted by `tests/fixture_bins.rs`'s `the_worker_fixture_hands_shakes_before_any_start` and the engine's fixture tests is preserved (both expect `hello` FIRST, `ready` SECOND, and the delay only widens the gap between them — never reorders).
  - **Why `FULCRUM_FIXTURE_READY_GATE` vs a `__slow-ready__`-style prompt:** the delay must land BEFORE the `ready` frame — `ready` is the `attach`/`wait_ready` gate — and `ready` is emitted before ANY prompt is ever read; a prompt-triggered delay cannot gate it. Note also the fixture emits `ready` with `session_id: None` — before any `Start` — so the gate CANNOT be per-session; it is per-process, set by the environment, and this test has exactly one worker process per session id anyway (`attach` spawns one fixture per call, manager/mod.rs:223).
  - **No other fixture change.** The two scope branches (`hello` then `ready`) keep their current shape apart from the inserted gate; no new bin, no `fixture_bins.rs` edit, no `--target-dir` games; `__long_stream__`/chunk emission is untouched (chunks still stream only after a `start`, and no chunk is emitted before `start` — verified).
- **Verify the gate semantics against the daemon:** the daemon's `start` calls `attach`, which awaits `handle.wait_ready(Duration::from_secs(5))` (manager/mod.rs:225). With `FULCRUM_FIXTURE_READY_GATE` set to an existing marker, the fixture emits `hello`, then blocks in the gate loop and does NOT emit `ready`, so `wait_ready` parks — the reader task is parked inside `handle_frame`'s `attach` for as long as the test holds the marker. The engine's `ready` watch is set by the read task (`spawn_pipe_reader`, `worker/client.rs`), and `ReadyTimeout` is 5 s; every gate hold the test performs is ~1-3 s — comfortably inside (same reasoning as the module header's `FRAME_BUDGET`). **Ceiling discipline (round-4 🔴):** the park can only stay parked if the test's own span — stamp-wait + absorb + probe — ends before those 5 s fire (see [`ATTACH_READY_CEILING`] and the const assert in the gate-consts bullet, Step 9); the budgets in this plan sum to 3.7 s, leaving ~1.3 s of headroom on a nominal run and absorbing a REASONABLE loaded-runner stretch. When CI flakes on load, do NOT open a wide `--slow-ready`-style window — expand `READY_GATE_STAMP_BUDGET`/`RELAY_ARRIVAL_BUDGET` only while the sum stays below the ceiling, or relax the GATE side and keep the assert green.

**Steps:**

- [ ] **Step 0 — T3 is IN the tree (`6d6887a`), so no re-dispatch is needed.** This plan's "Reconciled against T3" list at the top is the delta vs the round-3-vetted text. As a sanity check before starting: `serve_client` at `serve.rs:1528` already has the T3 header state (`handshake`, `rate`, `seen` at :1559-1570) and FOUR `send_frame` sites (:1644/:1668/:1726/:1731); `TransportError::PeerIdentity` is at `transport/mod.rs:~:198`. If any of those are missing the tree is not at the shape this plan was reconciled against — stop and report.

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
    - `a_send_against_a_shut_down_connection_errors_that_the_writer_survives` — split once, `writer.shutdown().await` (A1b), then `send_frame` must return `Err` — the write half really does close the write direction while the read half is alive.
    - **NEW (this pass — the delegator's `None` case):** `send_frame_after_split_writer_fails_not_drops` — `dial`/`accept_one` a pair, `split_writer` once, then call `Connection::send_frame` (the A3a delegator) on the returned reader half and assert it returns `Err` — specifically `TransportError::Io` with `ErrorKind::NotConnected` and the message `"the writer was taken by split_writer"` — and that the peer receives NOTHING (no frame is silently dropped into a void that a later writer would then glue onto; the failure is loud, not silent). This pins the mutual-exclusion property: the `Option` slot is `None` after `split_writer`, so the convenience method and the split cannot both be live. It is a T4a-commit test (it only needs the delegator, which exists in T4a); it stays green through T4b's deletion of the delegator only if the test is deleted together with the method in T4b — Step 8's deletion bullet names this test as the one to remove in the same commit.
    - **🟠 Round-2 replacement:** the frame-level half of `a_writer_whose_tx_drops_terminates_the_reader_loop` is DROPPED as a named test — `ConnectionWriter` has NO `queued` channel (that is a `serve`-level property), so no transport-level test can stand it up. The property it named — reader-EOF terminates the other side — is an integration property of `serve_client`, and it is now covered by the slow-start test's TEARDOWN half in Step 9: when the connection's client half closes, the test asserts the daemon does not hang and (via the abort backstop) no writer task outlives the connection — the daemon spawns no stray task, which is exactly what T1's census on this same branch would catch if it did. The transport-level residue that IS kept: after the reader half is dropped, a `recv()` on the OTHER half returns `None` (peer EOF) — appended to the `a_send_against…` test's body, proving the duplex really is shared.
  - **Run it:** `cargo test -p fulcrum --test transport` — expect the NEW tests to be the only ones red at first (they exercise `split_writer`/`ConnectionWriter`, which do not exist yet — the conversions in Step 4 are also red for the same reason). `Connection::send_frame` still exists as the A3a delegator, so the OLD direct-send tests still compile and pass at this point; the important first-run property is that the frame tests EXIST and would fail against any two-writer design.
  - `Did it fail with the expected shape?` — a compile error on `ConnectionWriter`/`split_writer` is EXPECTED at this point (the structural guarantee surfacing). Do NOT proceed past this step into the split with the frame tests absent.

- [ ] **Step 2 — run the existing suite to record the baseline (optional but recommended):** `cargo test -p fulcrum --test transport` and `cargo test -p fulcrum` (or at minimum `cargo test -p fulcrum --test transport --test serve --test worker_mode`) should be GREEN before touching `frame.rs`. If they are NOT green, stop and report (the tree is expected to be T3-post-shape or current-shape; either must build). **In the consume-mode workflow cargo may not be runnable in the planner's sandbox; the IMPLEMENTER runs these. `cargo test -p fulcrum` MUST have at least the discovered serve/transport/worker_mode tests fail loudly if broken — do not accept a green-empty suite as proof (a target that builds no test binaries still exits 0).**

- [ ] **Step 3 — implement Part A in `fulcrum/src/transport/frame.rs`:**
  - Add `ConnectionWriter` with `send_frame` (the sole writer) and the comment block explaining it is the ONLY writer and that `pending` lives ONLY here.
  - Add `ConnectionWriter::shutdown` (A1b) and `Connection::split_writer` (A3). **Do NOT add `Connection::shutdown_while_writer_intact` or any other `Connection` write method — A2b is deleted in this reconciliation (see the top list, item 8), and after the split the reader half must have no write path at all.**
  - Add the private `Option<WriteHalf<DuplexBox>>` backing + `split_writer` on `Connection` (A3's exact design).
  - Move `pending`/`writer` out of `Connection`; **RE-HOME `Connection::send_frame` as the A3a delegator** (do NOT delete it in this step — `serve.rs`'s four production call sites at :1644/:1668/:1726/:1731 are in the `fulcrum` LIB target and are converted only in T4b, so deleting the method now would break `cargo check -p fulcrum` and every integration test that links the lib; the deletion is T4b's job, Step 8). The delegator borrows the writer out of the `Option` slot and errors with `TransportError::Io(ErrorKind::NotConnected)` / `"the writer was taken by split_writer"` when the slot is `None`.
  - Keep `Drop { reader.abort() }`, the manual `Debug`, `MAX_*`, the reader task, `Connection::recv` (its only read method after the split), `read_capped_line_under`, `decode`, `wrong_platform`, and the `#[cfg(test)]` module's existing tests UNCHANGED (A2). `Connection`'s write side is stripped (A2/A3); tests that need to send use `split_writer`.
  - **`Connection::new`'s signature is UNCHANGED** (`pub(crate) fn new<S>(stream: S) -> Self where S: AsyncRead + AsyncWrite + Unpin + Send + 'static`); its body stores `writer: Some(write_half)` (A2/A3).

- [ ] **Step 4 — implement Part B in `fulcrum/src/transport/mod.rs`:** add `ConnectionWriter` to the `pub use frame::{…}` line (`pub use frame::{Connection, ConnectionWriter};`). (The `pub(crate)` methods `shutdown`/`split_writer` do not need re-export from `mod.rs` — they are on the types and are reachable through the public types.)

- [ ] **Step 5 — compile gate:** `cargo check --target x86_64-pc-windows-gnu --all-targets` (or the repo's documented Windows check) — the new `ConnectionWriter` uses only platform-neutral tokio types; this check must pass on the Windows leg with no `cfg`-leak. Also `cargo check` on the current platform. **If `Connection::new`'s signature had to change, report `DEVIATION:`.**

- [ ] **Step 6 — Part C verification:** confirm `unix.rs`/`windows.rs` builds unchanged (they should — `Connection::new(stream)` is untouched). If `Connection`'s field removal (dropping `writer`/`pending`) broke a `#[cfg(test)]` unit test inside `frame.rs` that constructs `Connection` directly, fix THAT test to use `split_writer` (it is inside the owned file).

- [ ] **Step 7 — Part E, `tests/transport.rs` conversions:** switch the direct `send_frame` call sites to `split_writer` — `a_jsonl_frame_round_trips_in_both_directions` (:326), `the_receive_stream_ends_at_eof_rather_than_hanging` (:364), `unix_specific::a_bound_socket_is_owner_only` (:463), `unix_specific::a_live_daemons_socket_is_left_alone_and_the_second_bind_fails` (:945), **T3's `same_uid_peer_is_accepted` (:1166/:1174 — the positive half of the peer-credential check; do NOT leave this test calling the A3a delegator — it must be on `split_writer` before T4b deletes the method)**, and the `accept_one_and_echo` helper (:297-307). Keep the raw-byte probes as-is. The `#[ignore]`d `foreign_uid_peer_is_rejected_under_root` sends NOTHING from its dialing child (it asserts the refusal at `accept`) — no conversion. Run `cargo test -p fulcrum --test transport` — all pre-existing tests plus the new frame tests must be green.

- [ ] **Step 7b — Part E, `tests/worker_mode.rs` conversions (SHOULD-FIX 4):** convert the two `send_frame` call sites at `:1006` and `:1087` per the exact `split_writer` pattern above. Run `cargo test -p fulcrum --test worker_mode` — all 31 existing tests must be green (the file's process-leak assertions (`pid_is_reaped`) must still hold).

- [ ] **Step 8 — Part D, `serve.rs` two-task split:** implement `serve_client` per D1, `reader_loop`/`writer_loop` per D2 (use the `Arc<StdMutex>` sharing + `enqueue_shared` — the plan's chosen design), teardown per D3 (drop-based, no join, no stop one-shot, `writer_task.abort()` backstop, NO reader-side drain), the D4 ordering contract in `serve_client`'s doc, the D5 doc/header rewrites, and the D6 queue-bound doc update. Add the NEW unit test `the_gap_drain_prelude_uses_the_current_drop_count` (D4) and the `send_frame_no_select` free fn. **Move T3's `handshake`/`rate`/`seen` locals into `reader_loop` as-is and route T3's two refusal `send_frame`s (serve.rs:1644, :1668) through the refusal `oneshot` + ack pair (D1) — the reader sends the refusal and awaits `ack_rx` before breaking, so the refusal is really on the socket before the connection ends.** `gap_frame`/`enqueue`/`watched` remain byte-identical. **This step ALSO deletes the T4a delegator (A3a):** once `serve.rs`'s four call sites are converted to `writer.send_frame` / `send_frame_no_select`, `Connection::send_frame` has no remaining callers and is DELETED from `frame.rs` (the load-bearing deletion — leaving it would let a future caller hold a `Connection` AND a `ConnectionWriter` at once, which is issue #3's hazard; see A3a's "why transient" note). Delete the `send_frame_after_split_writer_fails_not_drops` test in the SAME commit — it tests a method that no longer exists. `cargo build -p fulcrum` must be clean, `cargo check -p fulcrum` must pass (the lib alone — the check that was missing from the old plan's commit split), and `cargo clippy -p fulcrum --all-targets -- -D warnings` (the CI bar) must not gain warnings.

- [ ] **Step 9 — new integration test (acceptance criterion 1) + harness + fixture-hook changes:**
  - First the harness change in `tests/serve.rs`: `Client` gains a `writer: ConnectionWriter` field; `dial` does `let (conn, writer) = conn.split_writer();`; `send` uses `self.writer.send_frame`. All existing `client.send(…)` / `recv_until_*` / `drain` / `try_recv` calls then keep working unchanged (they go through `Client`'s methods, the only place that touches the connection).
  - **T3 interaction — the mandatory `hello` gate vs this test, verified (reconciliation item 10):** this test's `Client` must complete the handshake before `start`, and the file-wide pattern already does exactly that: every existing `tests/serve.rs` test sends `{"type":"hello"}` and waits for the `hello` reply BEFORE its first `start` (e.g. :905-906), and this test follows the same pattern (`recv_until("hello")` after `dial`, then `start(A)`). T3's gate counts pre-`hello` frames and refuses the third — since the test greets first, no frame is ever dropped or refused. The rate gate charges NON-greeting frames only (serve.rs:353-375) and this test sends ~5 of them (2×`start` + 2×`prompt` + `close`) against a 600-token burst — no refusal. `wait_until_listening`'s connect-and-drop probe sends zero frames and remains the `seen == 0` no-log case. **The gate only delays `ready`, which T3's gates never measure (ROUND-3 🔴 wording)** — the `FULCRUM_FIXTURE_READY_GATE` marker lands between `hello` and `ready` (it delays neither `hello` nor any client-side frame, and the client greets before any `start` anyway), so T3's hello/rate/session gates (serve.rs:353-375, the 600-token burst vs the test's ~5 non-greeting frames) are untouched; `wait_until_listening`'s probe sends zero frames and stays the `seen == 0` no-log case; `worker_mode.rs:1006/:1087` both send `hello` first. The old `SLOW_READY_MS`/`RELAY_ARRIVAL_BUDGET` pair is REPLACED by the gate consts (`READY_GATE_STAMP_BUDGET`, `RELAY_ARRIVAL_BUDGET`) — see Step 9. No fixture numbers moved.
  - **Slow-start machinery — the test-gated `ready` (round-3 🔴), a marker FILE not a fixed-ms delay.** `src/fixtures/worker.rs` gains the `FULCRUM_FIXTURE_READY_GATE` hold between `hello` and `ready` (see Part E). This is the ONLY fixture change: no new bin, no `fixture_bins.rs` edit, no `--target-dir` games. The vet's three fixture-bins premises from round 2 are all confirmed irrelevant (no new bin is needed); and `__long_stream__` streams only AFTER a `start`, so "A's chunks arrive while B's `attach` is in flight" REQUIRES the gated-`ready` fixture to be reachable at all.
  - **Why the gate makes the OLD design red BY CONSTRUCTION (round-3 🔴):** the discriminator no longer closes a window with arithmetic. `start(B)` is sent, B's reader parks inside `attach` for as long as the marker exists, and the arrival stamp makes the park an observed fact. While the gate HOLDS, the old single-task design has NO writer task: its fan-out arm is unpolled (the reader is parked in `handle_frame`) and there is no other task that can write the socket — **structurally silent**, with no margin to straddle. The test's verdict is taken ON THE SOCKET WHILE THE GATE HOLDS (the relay-probe bullet below) — a A-chunk arriving in that span cannot be an old-design artifact (the old design cannot write at all in the span), so `RELAY_ARRIVAL_BUDGET` is only a FORWARD bound for the new design, not a window the old one races. The vet's exchange-rate math (`sleep + BUDGET < SLOW_READY_MS → BUDGET < 0.4 × SLOW_READY_MS`, violated by the plan's 800) is MOOT by construction.
  - **The ordering claim, re-traced and stated:** with the fixture's `ready` gated, a SECOND `start` (B) parks the daemon's READER in `attach`→`wait_ready` for as long as the marker holds. Session A (already started, already prompted with `__long_stream__`) streams chunks from ITS OWN fixture process — the daemon spawns ONE worker per session id (`attach` spawns `factory.spawn()` per call, manager/mod.rs:223) — and the WRITER task relays them, independent of B's parked reader. The actual trace:
    - t0: A starts (gate open, A's `ready` passes immediately).
    - t1: A's `start-result` ok ⇒ `watching` contains `relay-a` (the READER's insert at D1).
    - t2: A's `prompt(__long_stream__)` → `prompt-result` ok (the daemon's `relay` dispatches to A's fixture; the fixture streams 30 chunks ~100 ms apart). **The discriminator does NOT need chunks to arrive before `start-result`** — it needs AT LEAST ONE A-chunk to arrive while B's `start` is in flight, which is exactly what the write-side independence (writer away from reader's attach) delivers in the NEW design and CANNOT deliver in the OLD design (the old reader parks the fan-out arm during attach, so A-chunks are buffered in the fan-out ring and only drained after `attach` returns — the B `start-result` is peeked from the inbound arm FIRST).
    - t3: A-chunks are flowing; the test verifies it (the `relay-a` chunk the client has SEEN is consumed before the landing, so the post-`start(B)` measurement never races a pre-`start(B)` artifact).
    - t4: `start(B)` frame sent; B's `attach` parks the READER in `wait_ready` (the marker exists) — A's chunks CONTINUE to be written by A's worker and relayed by the WRITER — the split's whole point.
    - t5: the probe verdict is taken while the marker still holds (an A-chunk on the socket ⇒ GREEN); ONLY THEN the test releases the gate, B's `ready` is emitted, `attach` returns, and B's `start-result` (ok) arrives — the arrival stamp + the release-dependency make "the verdict was taken inside the park" a fact.
  - **The test itself — `a_slow_start_does_not_stall_that_clients_own_event_relay` (name matches the file's naming):**
    - Spawn a SECOND daemon for this test: `Daemon::start_with_env(&marker_path)` — the harness's `Daemon::start` gains an optional gate parameter (a `#[cfg]`-free `env("FULCRUM_FIXTURE_READY_GATE", marker_path)` on the `Command` when the test asks for it; do NOT change the shared default — all other tests run the fixture un-gated). One `Client`; `recv_until("hello")`.
    - Start A: `start_env("relay-a", …)` → `start_frame` → `recv_until_matching("start-result", |f| f["sessionId"] == "relay-a")` → assert `ok == true`. `watching` now contains `relay-a` (A's worker passed its gate — the marker did not exist yet).
    - Prompt A with `__long_stream__` → `recv_until("prompt-result")` → assert ok. A's fixture is now streaming; wait for and consume CLIENT-side chunks until the first POST-prompt `agent_message_chunk` for `relay-a` has been seen (`first_chunk` — the explicit barrier that eliminates any pre-B artifact from the measurement, keeping the round-2 🟠 boundary-status protocol).
    - **Re-arm the gate:** create the marker file (`std::fs::write(&marker_path, b"hold")` — PRESENT = HOLD). The `.arrived` sidecar is NOT to be created by the test — it is written by the gated fixture itself when it enters the hold (see Part E).
    - Send `start_frame(start_env("relay-b", …))`.
    - **Wait for the ARRIVAL STAMP first (the round-3 🔴 discriminator):** `let deadline = Instant::now() + READY_GATE_STAMP_BUDGET; loop { if marker_path.with_extension("arrived").exists() { break; } if Instant::now() > deadline { panic!("B's fixture never reached the ready gate"); } tokio::time::sleep(10ms).await; }` — the gate DOES mean the reader is parked, but the stamp makes the park an OBSERVED FACT rather than an inferred one (the engine's `ready`-watch subscribe can block the worker's stdout, making the stamp lag the true park — see `wait_ready`/`subscribe` in `worker/client.rs`; the stamp is the honest signal). Once the stamp exists (and until the test releases), B's reader is parked and CANNOT return — its park spans the absorb + probe verdict below (longest when the probe times out at the full `RELAY_ARRIVAL_BUDGET` = 1.5 s), which is inside `wait_ready`'s 5 s ceiling by the BUDGET-SUM INEQUALITY — `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` (1500 + 700 + 1500 ms = 3.7 s < 5 s; the release occurs right after the verdict) — enforced by the compile-time const assert in the gate-consts bullet (a one-number retune can no longer outgrow the ceiling and manufacture a false green). **Hold THIS budget at its own failure mode:** if the stamp NEVER arrives within `READY_GATE_STAMP_BUDGET` the test MUST fail loudly — it already panics "B's fixture never reached the ready gate" — never fall through to a verdict on a broken fixture (a missing stamp is a broken instrument, not a valid red). **And hold the OTHER failure mode:** a hold that outlives `attach` — the reader unparks on `wait_ready`'s 5 s timeout, `handle_frame` returns an error, the old single-task `select!` resumes and drains the buffered A-chunks onto the socket, and a probe that runs after that point sees a chunk on CODE THAT HAS THE BUG — a false green. The const assert is exactly what rules that out. **The old design is red by construction in this span** (its fan-out arm is unpolled; nothing but the two-step absorb+probe can drain A-chunks, and that IS the verdict).
    - **The relay probe — ABSORB then READ WHILE THE GATE IS HELD, verdict BEFORE the release (the round-3 🔴 discriminator):** B's reader is provably parked (stamp observed) and the marker STILL EXISTS. Two steps, then the verdict:
      1. **Absorb (drain to quiet):** the OLD design may have written a small pre-park residue to the socket — the A-chunks its `select!` drained in the re-arm→park window (bounded: at ~100 ms cadence, tens of ms of window ≈ a handful of chunks, and the pre-`start(B)` barrier already consumed everything up to the `start(B)` write). Drain the client until no `relay-a` chunk has arrived for `CHUNK_SILENCE`:
         ```rust
         // Drain until quiet, bounded. OLD design: the residue is finite
         // (already-written bytes) and drains in ms → quiet comes quickly.
         // NEW design: the writer task keeps relaying → no quiet within the
         // bound — which is itself the GREEN signal.
         let quiet = drain_until_quiet(CHUNK_SILENCE, ABSORB_BUDGET).await;
         ```
      2. **Verdict probe (in the held park):** if `!quiet` (chunks never stopped within `ABSORB_BUDGET`) → GREEN (`ABSORB_BUDGET ≈ 7` chunks of continuous production is unreachable for an old design whose residue is bounded by the sub-second re-arm→park window). If `quiet` was reached → the socket is now structurally silent for the old design (its reader is parked in `attach`, its fan-out arm is unpolled, and there is NO writer task — after the absorbed residue, NOTHING can write in this span; the park lasts until THIS test releases), so a single-chunk probe is conclusive:
         ```rust
         // The budget is a FORWARD bound for the new design, NOT a window
         // the old design races: the old design has zero writers in this
         // span, so a timeout here is red BY CONSTRUCTION, independent of
         // budget size. The park lasts as long as this test holds the
         // marker — it releases only after the verdict.
         let _relayed = timeout(
             RELAY_ARRIVAL_BUDGET,
             recv_until_matching("agent_message_chunk", |f| f["sessionId"] == "relay-a"),
         ).await.expect("an A-chunk must arrive on the socket while B's start is parked");
         ```
      The release comes AFTER this verdict: `remove_file(&marker_path)` ONLY when the probe has its answer (a chunk, the absorb's no-quiet, or the probe timeout — the timeout IS the answer). Once the marker is removed, await B's `start-result` (ok) — it confirms B's `attach` actually completed and makes `start-result` observable (the dispatch's requirement: "release the gate at the end of the window so the test still completes and B's `start-result` is still observable"). Nothing AFTER the release is used for the relay verdict — the old design's post-release backlog drain (the moment `attach` returns it re-drains the buffered A-chunks) CANNOT inject a false-green chunk, because the verdict was already taken while the gate was held. The chunk cadence is ~100 ms (`LONG_STREAM_GAP`), so on the new design the probe wins in well under a second; `RELAY_ARRIVAL_BUDGET` is a generous forward bound (1.5 s), and the gate hold is at least (stamp-wait + absorb + probe + margin) — the test releases only after the probe's verdict, so the park duration is the test's own choice and needs NO clock guess: **there is no `sleep + budget < slow_ready` arithmetic anywhere in the gated design** (the old 800 ms budget and the `0.4 ×` inequality are what the round-3 🔴 vet arithmetic showed could false-green; they are replaced by "park lasts until the verdict" **plus the round-4 🔴 budget-sum ceiling guard**). **The ONE constraint that is NOT the test's own choice is the ceiling:** the park ends only when the test releases, so the test controls exactly how long `attach` stays parked — but the park cannot exceed `wait_ready`'s 5 s bound however it releases late, and a test that loses that race manufactures a false green (the reader unparks on timeout, the old design's `select!` resumes, and the drain writes the socket). The compile-time assert (gate-consts bullet) keeps all three budgets below the ceiling, so the whole verdict span — the latest the test can release — is inside the park. If the assert formulation cannot be made `const`, `UNVERIFIED:` it as a runtime `assert!` at the test's start with the same message.
      **The honest bound, stated (the ONE remaining timing constant and its one-sided direction):** the only empirical quantity left is the size of the old design's pre-park residue, and the probe's verdict does not depend on bounding it tightly — the residue is a FINITE set of already-written bytes that the absorb fully consumes, and after the absorb the old design has provably ZERO writers during the held park, so the probe's single chunk can only be a real writer. A loaded runner can only DELAY the new design's chunks (flake false-RED if the absorb's quiet detection or the probe budget is exceeded) — it cannot make the old design produce a chunk to false-green, since the old design's post-park silence is structural, not load-dependent.
    - **A bounded fallback IS kept, with the corrected arithmetic (in case the gate cannot be built in CI — e.g. the fixture env is scrubbed, which is NOT the case on this tree, since the spawn chain has no `env_clear` and the patch is empty under cargo test):** a `--slow-ready <ms>`-style ms delay with the inequality spelled out honestly in its comment. This is a DOCUMENTED What-If only — the gated design is the chosen path, so this fallback is NOT covered by the round-4 `const _: () = assert!(…)` (it is off the gated path). Its constants keep the same ceiling discipline as the gate: the fixture delay D is bounded by the engine's `wait_ready(Duration::from_secs(5))` (manager/mod.rs:225), the arrival budget B must satisfy `B < 0.4 × D`, and D must stay under the ceiling with the same headroom:
      ```rust
      /// Bounded fallback (only if the ready-gate cannot be built): a fixed
      /// fixture `ready`-delay D and an arrival budget B. The old design's
      /// `attach` returns at ~D and then drains buffered A-chunks INSTANTLY,
      /// so the measurement window must CLOSE before that: `sleep + B < D`,
      /// i.e. `B < 0.4 × D`. At D = 5000: B < 2000 (600 ms headroom over
      /// the 1400 used here). A non-gated design at D = 1500 would need
      /// B < 600 — below any loaded-runner safety, which is why the gate is
      /// the primary route and this is only a documented fallback.
      ///
      /// TWO bounds hold here, and both inherit from the engine's 5 s
      /// `ready` ceiling (`attach`'s `handle.wait_ready(
      /// Duration::from_secs(5))`, src/worker/manager/mod.rs:225):
      ///   (1) the START of the measurement window (the test's sleep after
      ///       `start(B)`) belongs to the ceiling side: park-start + up to D
      ///       must land inside the 5 s or `wait_ready` fires first and the old
      ///       design wins by timeout — same false-green mechanism as below;
      ///   (2) D itself must sit below the ceiling with margin (D = 5000 IS
      ///       the ceiling, so the real production delay must be smaller —
      ///       see the gated design's budgets, which are derived from this
      ///       ceiling instead of guessed).
      const FALLBACK_SLOW_READY_MS: u64 = 5000;
      const FALLBACK_RELAY_ARRIVAL_BUDGET: Duration = Duration::from_millis(1400);
      ```
      The primary route is the gate; this fallback exists only to say where a number would have to land if the gate cannot be built, and it uses the full `wait_ready(5s)` bound instead of an arbitrary 1.5 s. **Do NOT weaken the fallback's budget below `0.4 × FALLBACK_SLOW_READY_MS`** — that is the exact point where the old design false-greens (the round-3 🔴 vet arithmetic). **Note (round-4 🔴):** this fallback and the `0.4 ×` inequality are kept as the documented alternative; the PRIMARY route's proof no longer rests on any of it — the gated design's verdict window is closed by the held `ready`, and the once-clock-guessed `ms`-delay arithmetic is REPLACED by the budget-sum inequality `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING`, enforced at compile time. Whichever route runs, the failure mode is the same: a hold that outlives `attach` is a TEST BUG that invalidates the verdict — a false green, not a red (see the const assert).
    - **The gate consts (round-3 🔴 + round-4 🔴 ceiling fix — next to the existing `FRAME_BUDGET` at the top of `tests/serve.rs`):** the fixture's chunk cadence and chunk count are engine constants (`LONG_STREAM_GAP` / `LONG_STREAM_CHUNKS`, `src/fixtures/worker.rs`) that a `tests/` crate cannot import (they are `pub` but not re-exported through `fulcrum_core`'s public API), so the test mirrors them as adjacent consts — a `LONG_STREAM_GAP`-mirror and a `LONG_STREAM_CHUNKS`-mirror — and defines the timing budgets. **The three real budgets are DERIVED from the engine's `attach` `ready` ceiling, NOT chosen freely** (round-4 🔴): their sum must sit strictly below the ceiling with margin (see `ATTACH_READY_CEILING` and the const assert), and every retune re-derives from it. NONE of them is a window the old design races (the park lasts until the verdict, so they never encode `sleep + budget < slow_ready`). **Why the sum must stay below the ceiling — stated, and now enforced**: the span the test is parked inside `attach` must end BEFORE `attach`'s own `wait_ready` bound fires; if it exceeded it, the timeout would unpark the reader and manufacture a false green (the `const _: () = assert!(…)` below is what makes that retune-proof):
      ```rust
      /// The ceiling the test's park span must clear: the engine's OWN
      /// `ready` bound inside `attach` — `handle.wait_ready(
      /// Duration::from_secs(5))` at `src/worker/manager/mod.rs:225`.
      ///
      /// WHY THIS CONST EXISTS AND WHY THE BUDGETS MUST SUM BELOW IT: the
      /// whole proof rests on the reader staying PARKED inside `attach`
      /// until the test's verdict is taken. `wait_ready` implements that
      /// park, and it is BOUNDED — it times out at this duration and returns
      /// `Err`, `handle_frame` then returns an error, the single-task
      /// `serve_client` design's `select!` RESUMES, and its drain pours the
      /// buffered session-A chunks onto the socket. A probe that runs after
      /// that point sees a chunk and reports the split works — ON CODE THAT
      /// HAS THE BUG. Exceeding this ceiling manufactures exactly the false
      /// green this test exists to rule out, so the sum of the three real
      /// budgets must be strictly below it, with margin (see the const
      /// assert below).
      const ATTACH_READY_CEILING: Duration = Duration::from_secs(5);
      /// Mirror of the fixture's `__long_stream__` cadence (can't import
      /// it from a `tests/` crate; see `src/fixtures/worker.rs`).
      const LONG_STREAM_GAP_MS: u64 = 100;
      /// A quiet period long enough to mean "the old design's finite
      /// pre-park residue is fully absorbed" (3 × cadence).
      const CHUNK_SILENCE: Duration = Duration::from_millis(300);
      /// How long the slow-start test waits for B's fixture to reach the
      /// ready-gate hold (the `.arrived` stamp). Covers the fixture's
      /// spawn + `hello` + stamp-write. The stamp is WRITTEN BY THE FIXTURE
      /// AT ITS STARTUP, in MILLISECONDS — this budget is a broken-fixture
      /// panic timer, not an expected wait — so it can be sized generously
      /// inside the ceiling (~1.5 s) without touching the proof. If the
      /// stamp never appears within it the fixture is broken, and the test
      /// must fail LOUDLY (it panics "B's fixture never reached the ready
      /// gate") — NEVER fall through and take a verdict: a missing stamp is
      /// not a valid red, it is a broken instrument.
      const READY_GATE_STAMP_BUDGET: Duration = Duration::from_millis(1500);
      /// The FORWARD bound on an A-chunk arriving while B's reader is
      /// provably parked: ~100 ms cadence (`LONG_STREAM_GAP`) → 15×
      /// headroom on the new design. NOT a window the old design races —
      /// once the pre-park residue is absorbed, the old design has zero
      /// writers in the held park (see the relay-probe bullet).
      const RELAY_ARRIVAL_BUDGET: Duration = Duration::from_millis(1500);
      /// Upper bound on the absorb step (the old design's pre-park residue
      /// is a finite, already-written set that drains to quiet in ms; the
      /// bound is a ceiling for CI, not a margin the old design wins).
      const ABSORB_BUDGET: Duration = Duration::from_millis(700); // ≈ 7 × cadence

      // Local aliases so the const assert reads as the inequality above.
      // (Items, declared before the assert that references them.)
      const STAMP: Duration = READY_GATE_STAMP_BUDGET;
      const ABSORB: Duration = ABSORB_BUDGET;
      const PROBE: Duration = RELAY_ARRIVAL_BUDGET;

      /// The proof's hard invariant, checked at COMPILE time (same class of
      /// guard as `serve.rs`'s `CLIENT_QUEUE_CAPACITY > 0` /
      /// `< FAN_OUT_CAPACITY` const asserts). Stated once so a future
      /// one-number retune cannot silently outgrow `attach`'s ceiling:
      ///
      ///     STAMP + ABSORB + PROBE < ATTACH_READY_CEILING
      ///
      /// (1500 ms + 700 ms + 1500 ms = 3.7 s < 5 s, ~1.3 s of headroom.)
      /// WHY IT MUST HOLD: while the stamp-wait, the absorb and the probe
      /// all run, B's reader is parked inside `attach`; if their sum reached
      /// the ceiling, `wait_ready`'s own timeout would FIRE FIRST and unpark
      /// the reader — the `select!` resumes and drains the buffered A-chunks
      /// onto the socket, so a probe that runs after that point sees a chunk
      /// on CODE THAT HAS THE BUG: a manufactured false green. This assert
      /// is what makes that failure class impossible instead of merely
      /// documented. **CONST the two failure modes side-by-side:** (a) the
      /// stamp never arrives — the stamp-wait budget fires and the test
      /// PANICS, and must never fall through to a verdict on a broken fixture
      /// (a missing stamp is a broken instrument, not a valid red); (b) a
      /// hold that outlives `attach` — the ceiling fires first and un-parks
      /// the reader, manufacturing the false green, and this assert is what
      /// makes THAT retune-proof. The assert is spelled with `.as_millis()`
      /// because `Duration`'s `Add` is NOT a const trait on this toolchain —
      /// the direct `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` form does
      /// not compile in const context. `as_millis()` is `const`, so the
      /// millisecond-sum form evaluates in `const` context.
      const _: () = assert!(
          STAMP.as_millis() + ABSORB.as_millis() + PROBE.as_millis()
              < ATTACH_READY_CEILING.as_millis(),
          "READY_GATE_STAMP_BUDGET + ABSORB_BUDGET + RELAY_ARRIVAL_BUDGET must stay \
           below ATTACH_READY_CEILING: match `attach`'s wait_ready(5s) (src/worker/\
           manager/mod.rs:225) and the gate park would exceed it, the timeout would \
           unpark the reader mid-verdict, and the old design would false-green",
      );
      ```
      A CI flake on a loaded runner is a one-line tune: `READY_GATE_STAMP_BUDGET` and `RELAY_ARRIVAL_BUDGET` only need to EXCEED the fixture spawn / the new design's chunk cadence; the GATE holds as long as the marker exists, so raising them cannot weaken the proof (only the old design's post-`attach` backlog drain could — and that CANNOT place a chunk in the held-park verdict, which is taken before the release). **The one-line tune is REFRAMED — never raise a budget past the compile-time assert**: any growth must keep `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING`, so a retune also re-derives from the ceiling (raise `ATTACH_READY_CEILING` first only if the engine's bound ever moves — it is `wait_ready(Duration::from_secs(5))`, manager/mod.rs:225). If implementation finds the const-assert formulation unconst — it should not: the assert is spelled with `.as_millis()` on every term because `Duration`'s `Add` is NOT a const trait on this toolchain — `UNVERIFIED` it at the test's top with a runtime `assert!` at test start carrying the same message.
    - **Cross-check the "cannot false-green the NEW design" direction:** with the gate, the window is inside the park BY construction — there is no sleep between `start(B)` and the measurement that the old design could win. The only ways to false-green are (a) a design that relays while the reader is parked — exactly what the split does and what the old design does not — or (b) a measurement artifact, which the barrier (`first_chunk` consumed pre-`start(B)`) excludes. The false-green risk the round-2 vet named (a post-B read straddling the boundary) is eliminated by the barrier; a released marker that suppresses the `.arrived` sidecar entirely can neither stall nor false-green a test that waits for the stamp and fails on `READY_GATE_STAMP_BUDGET`.
    - **TEARDOWN half (round-2 🟠 fix 3 — the re-scoped `a_writer_whose_tx_drops_terminates_the_reader_loop` integration property):** after the relay assertion AND the release (`remove_file(&marker_path)`), await B's `start-result` (ok) — completing the dispatch's requirement that B's `start-result` be observable. The test then asserts the connection tears down cleanly on the reader-first path without hanging and without leaking a writer task. `Client` holds the reader half (`conn`) and the writer half (`writer`) as two fields of ONE struct; dropping the `Client` closes the whole socket. **The honest teardown assertion:** once the test's `Client` drops (scope exit), the daemon's `serve_client` must terminate BOTH its tasks without hanging: after `drop(client)`, `Daemon::wait_until_listening` returns promptly AND a fresh `Client::dial` + `hello` reply succeeds within the budget — proving the accept loop is not wedged and the old connection's tasks are gone (a writer stuck inside `send_frame` on a closed socket would still let a new dial succeed, so the observable that actually catches the leak is T1's process census on this same branch, run at the suite level; step 10's full suite is where the census reddens if the abort backstop regresses). This is the integration-level home of the property named `a_writer_whose_tx_drops_terminates_the_reader_loop` — the name is deliberately NOT used as a transport test (a frame-level `ConnectionWriter` has no `queued` channel; the property is `serve`-level, so it lives here).
  - **Windows leg:** the gate and the env plumbing are portable (a marker file via `std::fs`, polled on an 10 ms `sleep` cycle — no `cfg`, no wrapper, no argv). The Windows CLIENT half is socket-typed: the test's assertions run per-platform identically. The only Windows-specific risk is the `READY_GATE_STAMP_BUDGET` on a loaded runner (the stamp may appear late) — the consts are the one-line tune (the stamp budget only needs to EXCEED the spawn time, which is sub-second; relax it if CI flakes, and the GATE holds as long as the marker exists regardless).
  - **The marker path + `Daemon::start_with_env`:** the temp dir created by `Daemon::start` is owned by the harness (`tempfile::tempdir()`), so the marker lives INSIDE it and is cleaned up with it — no stray files, nothing shared between concurrently-running tests (tests run in parallel; `FULCRUM_FIXTURE_READY_GATE` is set only on THIS daemon's `Command`; every other test spawns a gate-free daemon). Spawn a SECOND daemon for this test — it needs its own gate.
  - **UNVERIFIED (flag for the caller's vet):** (1) whether a loaded macOS/Windows CI runner can stretch the budget-sum against `ATTACH_READY_CEILING` — the gates that keep the span under the 5 s ceiling are relaxed by tuning `READY_GATE_STAMP_BUDGET`/`RELAY_ARRIVAL_BUDGET` only while `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` stays true (the one-line tune), and a hold that outlives `attach` is a TEST BUG that invalidates the verdict rather than a red; (2) whether the arrival-stamp write can race the marker release (it cannot false-green: the stamp is written BEFORE the marker is removed only if the fixture is genuinely parked; the test waits for the stamp BEFORE releasing, so a stamp suppressed by a previous test-crash residue would only time out the stamp-wait, never false-green); (3) whether the `Daemon::start_with_env` parameterisation collides with T2's/tests' use of `Daemon::start` (it does not — default is unchanged); (4) whether the gate interacts with `tests/fixture_bins.rs`'s handshake test (`the_worker_fixture_hands_shakes_before_any_start` spawns the fixture directly WITHOUT the env var, so the gate is inert for it — verified by the position of the gate AFTER env read); (5) whether the compile-time const assert of the budget-sum inequality is accepted on the build matrix — it is spelled with `.as_millis()` on every term because `Duration`'s `Add` is NOT a const trait on this toolchain (the direct `STAMP + ABSORB + PROBE < ATTACH_READY_CEILING` form does not compile in const context; `as_millis()` is `const`), so the millisecond-sum form should be accepted; if a target or MSRV nuance still rejects it, `UNVERIFIED:` it in the plan and mandate a runtime `assert!` at test start with the same message.

- [ ] **Step 10 — full verify:** run, in order, on the branch:
  - `cargo fmt --all -- --check` (CI gate — rustfmt-clean)
  - `cargo build`
  - `cargo check -p fulcrum` (the lib alone — the check that was missing from the old plan's commit split; it must pass at BOTH commits, T4a via the delegator and T4b after the conversion+deletion)
  - `cargo test -p fulcrum` (the full member suite; the existing serve/transport/worker_mode tests + new ones must pass; `a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting` is the durability pin that must stay green)
  - `cargo clippy -p fulcrum --all-targets -- -D warnings`
  - `cargo check --target x86_64-pc-windows-gnu --all-targets`
  - `Did any of them fail?` Fix and re-run before committing.
- [ ] **Step 11 — commit (serial task, feature branch), TWO commits (SHOULD-FIX 7):**
  - **Commit 1 (T4a — the frame split + its tests):** `transport: split Connection so the writer half owns pending (T4a, issue #3)` — frame.rs (`ConnectionWriter` + `send_frame` + `shutdown` (A1b) + `split_writer` (A3) + the A3a delegator `Connection::send_frame` + `Connection::new` storing `Some(ConnectionWriter { … })`), mod.rs re-export (`pub use frame::{Connection, ConnectionWriter};`), the new frame tests in `tests/transport.rs` (incl. `a_shared_writer_under_a_mutex_never_interleaves_frames` AND `send_frame_after_split_writer_fails_not_drops`), and the `tests/transport.rs` + `tests/worker_mode.rs` conversions (they keep working because the A3a delegator keeps `Connection::send_frame` compiling while the tests move to `split_writer`). **This commit alone must pass `cargo test -p fulcrum --test transport --test worker_mode` AND `cargo check -p fulcrum`** — the lib check is the one the old plan's split was missing: `serve.rs`'s four production `send_frame` call sites (:1644/:1668/:1726/:1731) live in the `fulcrum` LIB target and are still unconverted at T4a, so the delegator is what keeps the lib compiling. Do NOT delete `Connection::send_frame` in this commit.
  - **Commit 2 (T4b — the serve split):** `serve: split serve_client into reader and writer tasks (T4b, issue #3)` — serve.rs (reader_loop/writer_loop/enqueue_shared/send_frame_no_select/D3 teardown incl. the `writer_task.abort()` backstop/D4 contract/D5 docs/D6 doc), the `Client` harness (incl. the round-2 🟡 `send`-expect EOF semantics) + the slow-start relay test (with the round-3 🔴 ready-gate discriminator and teardown half) + the gate consts (`LONG_STREAM_GAP_MS`/`CHUNK_SILENCE`/`READY_GATE_STAMP_BUDGET`/`RELAY_ARRIVAL_BUDGET`/`ABSORB_BUDGET`) in `tests/serve.rs`, and the `FULCRUM_FIXTURE_READY_GATE` gate + arrival stamp in `src/fixtures/worker.rs`, and the new `the_gap_drain_prelude_uses_the_current_drop_count` unit test. **This commit ALSO deletes the A3a delegator** — `Connection::send_frame` is removed from `frame.rs` and `send_frame_after_split_writer_fails_not_drops` is removed from `tests/transport.rs` in the SAME commit, because by now `serve.rs`'s four sites are converted and the method has no remaining callers (the deletion is load-bearing, not tidiness — leaving the delegator would let a future caller hold a `Connection` AND a `ConnectionWriter` at once, which is issue #3's hazard; see A3a's "why transient" note). **T4b is serial on T4a** (the serve split is only safe because the writer half owns `pending`); never land T4b without T4a, and never land T4a without its frame regression tests. **At BOTH commits `cargo check -p fulcrum` must pass** — at T4a the delegator carries the lib's four sites; at T4b the sites are converted and the delegator is gone, so the lib check is satisfiable at every point of the sequence.
  - If the reviewer prefers one commit, that is acceptable ONLY if the frame tests remain in the first commit and the serve split in the second — the two-commit shape is the default.

**Acceptance criteria:**
- [ ] `cargo test -p fulcrum --test transport` — ALL pass, including the `transport::frame` line-reader tests (`a_line_of_exactly_the_limit_is_accepted`, `one_byte_over_the_limit_is_refused`, `an_unterminated_flood_is_refused_at_the_cap_and_no_further`, `an_unterminated_tail_at_eof_is_still_delivered`, `a_clean_eof_reports_nothing`, `the_cap_counts_bytes_not_characters`, `the_line_too_long_error_names_the_limit`) and the NEW `a_second_split_writer_panics`, `two_frames_from_the_sole_writer_arrive_as_two_clean_lines`, `a_shared_writer_under_a_mutex_never_interleaves_frames`, `a_partially_accepted_frame_is_drained_before_any_new_bytes`, `a_send_against_a_shut_down_connection_errors_that_the_writer_survives`, `send_frame_after_split_writer_fails_not_drops` (the A3a delegator's `None` case — a `send_frame` after `split_writer` must fail with `TransportError::Io(ErrorKind::NotConnected)`, not silently drop the frame). **Tested-as-vacuous baseline:** there were NO partial-write/two-writer tests before this task (verified by grep) — the "existing tests still pass" criterion was empty, and this task writes the tests that fail if `pending` is ever shared.
- [ ] A client that starts a session and streams sees event frames arrive WHILE `start` is in flight, not after; the `a_slow_start_does_not_stall_that_clients_own_event_relay` test proves it (RED before the split BY CONSTRUCTION — the `FULCRUM_FIXTURE_READY_GATE` marker holds B's `ready` until the test releases it, so B's reader is provably parked inside `attach`; the OLD single-task design has no writer that can write the socket during the held park, and the verdict is taken while the gate STILL HOLDS — never a timing margin; round-3 🔴). GREEN after the split, using the gate + arrival-stamp machinery in `src/fixtures/worker.rs`. The same test's teardown half asserts the daemon reaps both of the connection's tasks on the reader-first path (the re-scoped `a_writer_whose_tx_drops_terminates_the_reader_loop` property — round-2 🟠, `serve`-level only).
- [ ] A regression test exists for the two-writer hazard, and its blind spots are stated honestly (round-2 🟠): `a_second_split_writer_panics` (`#[should_panic(expected = "the writer was already taken")]`), `two_frames_from_the_sole_writer_arrive_as_two_clean_lines`, `a_partially_accepted_frame_is_drained_before_any_new_bytes` — and **`a_shared_writer_under_a_mutex_never_interleaves_frames`, the pin that ACTUALLY goes red on a reintroduced `Mutex<ConnectionWriter>` + re-added `send_frame`** (a regression PIN against a possible future design, not a proof about the current one — stated in the test's doc). The plan states plainly that `a_second_split_writer_panics` stays green on that regression (no second `split_writer` call exists) and that the two serial-driver pins pass trivially (the mutex serialises), which is exactly why the concurrent single-writer driver exists.
- [ ] `cargo fmt --all -- --check`, `cargo clippy -p fulcrum --all-targets -- -D warnings`, `cargo check --target x86_64-pc-windows-gnu --all-targets` all pass (3-OS bar; the Windows leg is a compile gate, not execution proof).
- [ ] `the_gap_notice_carries_its_count_where_the_client_reads_it` and `a_full_client_queue_drops_counts_and_keeps_the_connection` pass UNCHANGED; `gap_frame`'s shape (`type:"gap"`, top-level `skipped`, no `sessionId`) and `FAN_OUT_CAPACITY`/`CLIENT_QUEUE_CAPACITY` (4096/1024) and their compile-time asserts are untouched.
- [ ] **No task leaks and no deadlock:** when a connection dies, BOTH the reader and writer tasks of its `serve_client` terminate; the writer is NEVER joined (its `JoinHandle` is `abort()`ed by `serve_client` after `reader_loop` returns — the round-2 🔥 backstop, the ONLY mechanism that carries "no task outlives `serve_client`"; the peer's close is what unwakes a writer's parked `send_frame` write, and the writer's own `ConnectionWriter::shutdown()` (A1b) — proven by the `a_send_against_a_shut_down_connection_errors_that_the_writer_survives` test — closes its write direction on every self-reached exit). There is no reader drain of any kind. **A writer parked inside `send_frame` never outlives its `serve_client`** — the round-2 🔥 tested invariant, covered by the slow-start teardown half (Step 9). T1's process census stays green.
- [ ] `Connection::new`'s signature is unchanged — if it is not, a `DEVIATION:` is recorded in the plan/file before implementation proceeds.
- [ ] All pre-existing `tests/serve.rs` integration tests (distinctly `a_client_that_vanishes_mid_turn_leaves_the_session_running_and_persisting`, `a_probe_connection_that_sends_nothing_does_not_disturb_the_daemon`, `a_client_that_never_reads_is_throttled_and_the_daemon_stays_flat`, `a_client_that_never_reads_does_not_stall_the_daemon`, `two_sessions_on_two_sockets_do_not_see_each_others_events`, `repeated_disconnects_do_not_degrade_the_daemon`) pass.
- [ ] `fulcrum/tests/worker_mode.rs`'s `serve_cli_binds_the_socket_it_was_given_and_answers_hello` (both legs) passes with the `split_writer` conversion at `:1006`/`:1087`.
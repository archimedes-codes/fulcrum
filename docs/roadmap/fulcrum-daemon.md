---
status: committed
done-when: "`fulcrum serve` listens on a UDS (unix) / Named Pipe (Windows), runs a session in a spawned worker process, and persists it to a WAL-mode `fulcrum.db`. **A client that disconnects mid-turn does not stop the session: the worker process is still alive afterwards and `fulcrum.db` keeps gaining rows.** `cargo test` is green on all three OSes. *(A reconnecting client rejoining the live stream is T10's `attach { afterSeq }` — see the boundary note under Task 5; it is deliberately NOT claimed here.)*"
---

# T9 — the `fulcrum serve` daemon

**Goal:** Turn this repo into a Cargo workspace and add a `fulcrum` binary crate that supervises session workers and serves them over a local socket, backed by its own SQLite database.

**Architecture:** The daemon is a **process supervisor + transport**, not a new engine. Every engine piece already exists in `fulcrum-core`: `WorkerManager` (session-keyed worker registry, pumps, crash detection, subagent dispatch), `TranscriptPersister` + the `FulcrumPersistence` seam (the persistence writer), `run_worker` (the in-child entry point), and the JSONL `protocol` module. The daemon adds exactly three things that do not exist anywhere today: a **platform-split socket transport** (UDS / Named Pipe), a **WAL-mode `fulcrum.db`** implementing `FulcrumPersistence`, and the **client↔session multiplex**.

**Tech Stack:** Rust 1.99.0 (pinned, `rust-toolchain.toml`), tokio 1 (already carries `net`), rusqlite 0.37 `bundled`, thiserror 2.

---

## Two decisions already made — do not re-litigate

1. **The daemon lives in THIS repo, as a second workspace member** (user decision). Not a third repo, not a second `[[bin]]` in `fulcrum-core`.
2. **It ships to users as a Tauri sidecar via `bundle.externalBin`** (user decision). That wiring is **desktop-repo work and is NOT in this plan** — see *Out of scope*.

## THE LAYOUT DECISION — measured, and it contradicts the grounding report

The workspace can be built two ways. **They are not equivalent, and the obvious one breaks the census.**

- **Layout A — `fulcrum-core` stays at the repo ROOT** (its `[package]` remains in the root `Cargo.toml`, `src/` and `tests/` do not move), plus a new `fulcrum` member. **This is the layout this plan uses.**
- **Layout B — move the lib into `fulcrum-core/`** and put a pure `[workspace]` manifest at the root. This is the cargo-idiomatic shape and it is what the grounding report recommended. **It panics the census.**

Measured by the orchestrator at `8e93496`, reproducible:

```
Layout A  members=[".", "fulcrum"], lib at root      -> childproc census PASSES
Layout B  members=["fulcrum-core","fulcrum"], moved  -> PANIC:
  the census resolved the wrong root: <root>/src-tauri/src is not a directory
  (layout WorkspaceMember { workspace_root: "<root>" })
```

**Why.** `childproc::tests::declaring_workspace_root` walks **ancestors only** — it starts at `manifest_dir().parent()` (`src/childproc.rs:712-714`). A `[workspace]` **at** the manifest dir is therefore never examined, so Layout A keeps resolving `SinglePackage`, which is the correct answer for this checkout: there is no desktop tree to police here. Moving the lib down one level puts the workspace on the ancestor path, the walk finds it, `member_entry_names_us` matches `"fulcrum-core"`, layout becomes `WorkspaceMember`, and `source_roots()` then **hardcodes** `workspace_root.join("src-tauri").join("src")` (`src/childproc.rs:985-988`) — a directory that does not exist in this repo. The census panics loudly rather than policing nothing, which is the right behaviour and the reason this is discoverable at all.

**Consequence for the plan:** do not "tidy" the layout into the idiomatic shape. If a future task wants Layout B, it must first teach `source_roots()` that a workspace root need not hold a desktop tree — a separate change with its own vacuity proof.

Also verified, because it is the premise the user's decision #1 rests on: **a git dependency pinned by `rev` resolves to a workspace member**, not just to a root package. Tested against a local bare repo: `Compiling engine-core v0.1.0 (file:///…?rev=9d7cbe3#9d7cbe3f)`. Cargo addresses a git source by commit and finds the package **by name** anywhere in that commit's tree, so the desktop's existing pin survives this conversion once it bumps the rev.

---

## Parallelisation

**Verdict:** PARALLEL — 4 waves.

- **Wave 1:** Task 1 alone. It converts the workspace and is the only task that touches the root `Cargo.toml`, the member `Cargo.toml`, or `fulcrum/src/lib.rs`, so nothing can join it.
- **Wave 2:** Task 2 (docs) — needs Task 1's layout settled before it can describe it.
- **Wave 3:** Tasks 3 and 4 together. Disjoint file sets (`fulcrum/src/transport/**` + `fulcrum/tests/transport.rs` vs `fulcrum/src/db/**` + `fulcrum/tests/db.rs`), neither imports the other. **Two shared artifacts would otherwise make this wave collide, and Task 1 removes both:** it pre-declares every dependency in the frozen member manifest, and it pre-declares **every module** in `fulcrum/src/lib.rs` with empty stub files, so neither child edits `lib.rs` or `Cargo.toml`. Without that, the wave is serial on a single `mod` line.
- **Wave 4:** Task 5 (multiplex + `serve` + `--worker`) needs both 3 and 4. Then Task 6 (daemon boundary pins) last: it asserts over the finished member.

`Cargo.lock` is rewritten by Task 1 (it gains `rusqlite` and its deps) and by nobody after — Tasks 3–5 add no dependencies.

---

### Task 1: Convert the repo to a workspace (Layout A) without un-policing a single guard

**Parallel:** none — the only task permitted to edit the root `Cargo.toml`; Wave 2/3 children depend on its output.

**Context:** `fulcrum-core` is currently a single `[package]` at the repo root with no `[workspace]` table (verified: `Cargo.toml:1` is `[package]`, `:205` is `[profile.release]`, nothing else). The daemon needs to be a second package in the same repo so it can link the engine by path and never drift from it. This task does the conversion and nothing else — no daemon code. Its real content is **proving the guards still police what they claim**, because this repo's guards are layout-sensitive by design and a layout change is exactly how a guard silently stops working.

**Files:**
- Modify: `Cargo.toml` (add `[workspace]`, move `[profile.release]` to the root scope, leave `[package]`/`[[bin]]`/`[dependencies]`/`[dev-dependencies]` in place)
- Create: `fulcrum/Cargo.toml`, `fulcrum/src/lib.rs`, `fulcrum/src/main.rs`, and the **stub module files** `lib.rs` declares
- Modify: `.gitignore` (confirm `/target` still covers the workspace; it does — the workspace root *is* this directory)
- Test: existing suite, no new test files in this task

**⚠️ THE MEMBER MUST BE A LIB + BIN CRATE, NOT BIN-ONLY. This is the load-bearing detail of the whole plan.** A bin-only crate silently un-gates every later task. Measured by the orchestrator in a scratch package:

- An `src/transport/mod.rs` containing a genuine type error (`let _x: u32 = "…";`) that `main.rs` does not reference → **`cargo check --all-targets` exits 0, prints nothing.** rustc never compiles a file no `mod` declares.
- `tests/t.rs` doing `use binonly::transport::send();` → **`E0433: cannot find module or crate`**, because a bin crate exposes no library path to integration tests.

So under a bin-only member, this plan's own gate (`cargo check --all-targets -- -D warnings`) would certify Tasks 3, 4 and 5 while they were a mass of type errors, and their integration tests would not compile at all. **Adding `fulcrum/src/lib.rs` fixes both** — measured: the same planted error then fails `E0308 mismatched types`, the integration test resolves `fulcrum::transport`, and `CARGO_BIN_EXE_fulcrum` still works on a cold target dir (a lib+bin package still materialises its bin, so the `fixture_bins` concern does not apply to the member's own bin).

**What to implement:**

1. Add at the very top of `Cargo.toml`, before `[package]`:
   ```toml
   [workspace]
   resolver = "2"
   members = ["fulcrum"]
   ```
   **Do not write `members = [".", "fulcrum"]`.** Cargo treats a root `[package]` as an implicit member, so `"."` is unnecessary. It is also *wrong for the census matcher if the walk ever changes*: `member_entry_names_us` (`src/childproc.rs:690-700`) compares `rel == entry || package == entry`, and for a root package `relative_to_manifest` returns the **empty string**, so `"."` matches nothing. It passes today only because the walk never reaches the manifest dir. Write the form that is true, not the form that happens not to be read.
2. `[profile.release]` **must be at the workspace root** (it already is, since the root *is* the crate — just confirm it is not shadowed). Its five lines are load-bearing, especially `panic = "unwind"`: ADR 0025 makes a Worker panic kill the Worker process and leave the app alive, and a `panic = "abort"` daemon would take the whole supervisor down on the first worker panic. **Cargo reads profiles from the workspace root only** — that is why the daemon inherits them and cannot set its own.
3. Create `fulcrum/Cargo.toml` **with every dependency Tasks 3, 4 and 5 will need**, because they are forbidden from editing this file:
   ```toml
   [package]
   name = "fulcrum"
   version = "0.11.0"
   edition = "2021"

   # The lib target is NOT optional — see the warning above. Without it the
   # member's src/ modules are never type-checked and tests/ cannot name them.
   [lib]
   name = "fulcrum"
   path = "src/lib.rs"

   [[bin]]
   name = "fulcrum"
   path = "src/main.rs"

   [dependencies]
   fulcrum-core = { path = ".." }
   tokio = { version = "1", features = ["process", "io-util", "io-std", "sync", "macros", "time", "net", "rt", "rt-multi-thread"] }
   tokio-util = "0.7"
   rusqlite = { version = "0.37", features = ["bundled"] }
   serde = { version = "1", features = ["derive"] }
   serde_json = "1"
   thiserror = "2"
   dirs = "6"

   [target.'cfg(unix)'.dependencies]
   libc = "0.2"

   [dev-dependencies]
   tokio = { version = "1", features = ["process", "io-util", "io-std", "sync", "macros", "time", "net", "rt", "rt-multi-thread", "test-util"] }
   tempfile = "3"
   ```
   `tokio`'s `net` feature is what makes the transport dependency-free: `tokio::net::UnixListener` on unix and `tokio::net::windows::named_pipe::{NamedPipeServer, NamedPipeClient}` on Windows (tokio's `net/mod.rs:62` gates `pub mod windows` behind `cfg_net_windows!`, so **`net` alone is sufficient on Windows too — do not add the `windows` or `windows-sys` crate**). **`dirs = "6"` and `libc = "0.2"` must match the engine** — `Cargo.toml:62` and `:175`, the latter under `[target.'cfg(unix)'.dependencies]`, so mirror that placement. Resolver 2 would unify a lower `dirs` upward, so a mismatch is not a build break; it is a graph that *looks* deliberately different when it isn't. **`rusqlite` mirrors the desktop exactly (`src-tauri/Cargo.toml:98`: `rusqlite = { version = "0.37", features = ["bundled"] }` — `bundled` is pure-C and keeps the "no `openssl-sys`, no `native-tls`" property CI asserts). **A new bin name must never equal `fake_worker_core` or `fake_mcp_stdio_core`**: workspace members share one `target/debug/`, and two members declaring a bin of the same name collide on the output filename — cargo only warns and the last writer wins. `fulcrum` is safe.
4. `fulcrum/src/lib.rs` **declares every module the later tasks will fill**, and each is created now as a compiling stub. This is what makes Wave 3 disjoint — a shared `lib.rs` edited by two children is the classic fan-in collision:
   ```rust
   // fulcrum/src/lib.rs
   pub mod transport;   // Task 3
   pub mod db;          // Task 4
   pub mod serve;       // Task 5
   pub mod worker_factory; // Task 5
   ```
   Exact stub contents — measured in a scratch crate on both the host and `x86_64-pc-windows-gnu`, because two different failures come from getting this wrong:

   ```rust
   // fulcrum/src/transport/mod.rs
   pub mod frame;
   #[cfg(unix)]
   pub mod unix;
   #[cfg(windows)]
   pub mod windows;
   ```
   `frame.rs`, `unix.rs`, `windows.rs`, `db/mod.rs`, `db/schema.rs`, `db/persistence.rs`, `serve.rs`, `worker_factory.rs` are all **empty files**.

   - **`windows.rs` must exist even though it is `#[cfg(windows)]`.** Omitting the file gives `error[E0583]: file not found for module 'windows'` on the Windows target only — invisible to a Linux-only developer, and CI is the first thing to see it.
   - **`pub mod unix;` must be `#[cfg(unix)]`-gated, not bare.** An ungated `pub mod unix;` whose body names `tokio::net::UnixListener` gives `error[E0433]: cannot find 'UnixListener' in 'net'` on the Windows target. The empty stub does not hit this today, but the gate must be right from the start or Task 3 discovers it mid-wave.
   - Stubs stay **empty or comment-only**. A `todo!()` body compiles and would let a later task ship an unimplemented function that nothing notices.

   Verify the whole stub tree on both targets before committing — `cargo check --all-targets` **and** `cargo check --target x86_64-pc-windows-gnu --all-targets` both finish clean with the layout above.
5. `fulcrum/src/main.rs`: `fn main() {}` for now, with a `// T9 Task 5 replaces this stub` comment. **Do NOT copy the desktop's `#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]`** from `archimedes-desktop/src-tauri/src/main.rs:2` — that exists because a GUI app must not show a console; a worker supervisor wants its console in release.

**Steps:**
- [ ] First, **prove the census is green before you touch anything**: `cargo test -p fulcrum-core --lib -- childproc::tests::` — record the count.
- [ ] Make the changes above.
- [ ] `cargo metadata --no-deps --format-version 1 | python3 -c "import json,sys;print(sorted(p['name'] for p in json.load(sys.stdin)['packages']))"` → must print `['fulcrum', 'fulcrum-core']`. If cargo rejects the manifest shape, **stop** — do not switch to Layout B to make it pass.
- [ ] `cargo test -p fulcrum-core --lib -- childproc::tests::the_two_layout_witnesses_agree childproc::tests::production_source_has_exactly_one_child_constructor childproc::tests::production_code_never_appends_a_raw_command_line_itself childproc::tests::production_crashlog_never_resolves_a_data_dir`
  - Did all four pass? **If `the census resolved the wrong root: … src-tauri/src is not a directory` appears, you have accidentally created Layout B.** Move `src/` and `tests/` back to the repo root.
- [ ] `cargo test -p fulcrum-core` → same count as before the change. **A drop means a guard stopped running, not that the conversion is clean.**
- [ ] `cargo test` (workspace-wide now) → the crate's tests **plus** the new member's. Record both.
- [ ] `cargo clippy --all-targets -- -D warnings` **and** `cargo clippy --target x86_64-pc-windows-gnu --all-targets -- -D warnings`
- [ ] `bash scripts/check-rquickjs-allocator.sh` — see the warning below.
- [ ] `cargo build --all-targets && cargo test`
- [ ] `cargo fmt --all -- --check`
- [ ] Commit: `refactor: convert the crate into a workspace with a fulcrum binary member`

**⚠️ The two guard behaviours this conversion genuinely changes — verify both, do not assume:**
1. **`check-rquickjs-allocator.sh` scope grows.** It resolves the **root workspace graph** via `cargo tree -e features`. Today that graph is one package; after this task it includes the daemon's dependencies too. That is the script working as designed, but it means **the daemon's dep choices can now fail a guard that guards the sandbox's 256 MiB memory cap.** Confirm the live check passes and that no `allocator` / `rust-alloc` edge appears on `rquickjs`/`rquickjs-core`.
2. **`boundary_pins` does not cover the new member — at all.** `crate_code()` walks `{CARGO_MANIFEST_DIR}/src` (`src/boundary_pins.rs:166-225`) and the manifest pin reads only `{CARGO_MANIFEST_DIR}/Cargo.toml` (`:371-399`). `fulcrum/src/` is outside that walk and `fulcrum/Cargo.toml` is never read. **So the daemon may import `rusqlite` — and also `tauri` — and nothing in this repo notices.** That is intended for `rusqlite` (ADR 0091 forbids it in the *engine*, and the daemon is the thing that owns the disk) but **not** for `tauri`/`archimedes_lib`. Task 6 adds the daemon's own pins. Do not "fix" `boundary_pins` to walk the sibling: it would then fail on the daemon's legitimate `rusqlite`, and the fix for that failure would be an allowlist that weakens the engine's pin.

**Acceptance criteria:**
- [ ] `cargo metadata --no-deps` lists exactly `fulcrum` and `fulcrum-core`.
- [ ] `src/` and `tests/` have **not** moved; `git show --stat` shows no renames under `src/`.
- [ ] All four layout-sensitive census tests pass, and the crate-side test count is unchanged from before the conversion.
- [ ] `[profile.release]` present exactly once, at the root, with `panic = "unwind"`.
- [ ] rquickjs guard green; clippy green on **both** targets.
- [ ] `fulcrum/Cargo.toml` declares every dep Tasks 3–5 need (they may not edit it).

---

### Task 2: Correct the docs the conversion and one false claim left behind

**Parallel:** none — Wave 2, after Task 1 (it describes the layout Task 1 creates, and edits `README.md` which Task 1's notes also touch).

**Context:** Two doc defects, both verified this session.

**(a) `README.md`'s "Build before you test" box is false.** It says *"On a cold target dir, a bare `cargo test` fails 66 tests with `ENOENT`."* Measured three times:
- `fulcrum` built cold with **no** build step → **1406 passed / 0 failed**
- fresh clone of the pushed repo, cold, no build step → **1406 passed / 0 failed**
- `tests/fixture_bins.rs` **deleted**, cold → **1335 passed / 66 failed**, 50 ENOENT mentions

So the 66 failures are what **deleting the build-contract file** does, not what a cold target dir does. CI agrees and says so explicitly — `.github/workflows/ci.yml:60`: *"⚠ NO `cargo build --all-targets` step before the tests, and do not add one."* **The stale box invites a reader to add exactly the step CI forbids**, which would keep the suite green after `fixture_bins.rs` was deleted and silently re-arm the trap. This is the highest-value sentence in the file to fix.

**(b) The docs describe a single package** and must now describe a workspace with two members, plus say what the `fulcrum` member is for.

**Files:**
- Modify: `README.md` (the box at ~l.51-58, the layout block at ~l.125-140, the "extracted from" intro)
- Modify: `AGENTS.md` (Build & validation bar; add a short "the two members" note)

**What to implement:**
1. Rewrite the box to say: the two fixture binaries are materialised to `target/debug/<name>` because `tests/fixture_bins.rs` is an integration-test target of this package; **`cargo test` alone passes from a cold target dir**; and `fixture_bins.rs` is part of the build contract — deleting it fails 66 tests with ENOENT, which is measured, and **CI deliberately has no `cargo build --all-targets` step because that would hide the deletion**. Keep it as a warning about the *file*, not about coldness.
2. State the workspace layout, and state the Layout A / Layout B finding **in `AGENTS.md`** — it is a trap the next contributor will otherwise walk into, and it is exactly the kind of thing `AGENTS.md` is for: the idiomatic layout panics the census, with the reason (ancestor-only walk + hardcoded `src-tauri/src`) and the reproduction.
3. Note that a **new workspace member's dependencies fall inside the rquickjs allocator guard**, and that `boundary_pins` does **not** cover the sibling member.

**Steps:**
- [ ] Re-run the measurement before writing it: `rm -rf target && cargo test -p fulcrum-core` and confirm green without a build step. **Do not copy the numbers in this plan if your tree disagrees — write what you measured.**
- [ ] Edit both files. `cargo fmt --all -- --check` (docs only, should be untouched).
- [ ] Commit: `docs: correct the build-before-test claim and describe the workspace`

**Acceptance criteria:**
- [ ] No sentence says a bare `cargo test` fails on a cold target dir.
- [ ] No sentence recommends adding a build step.
- [ ] `AGENTS.md` records the Layout A/B finding with its reproduction.

---

### Task 3: The transport — UDS on unix, Named Pipe on Windows

**Parallel:** wave 3 — disjoint from Task 4 (`fulcrum/src/transport/*` + `fulcrum/tests/transport.rs` vs `fulcrum/src/db/*` + `fulcrum/tests/db.rs`); imports nothing from it; **must not edit `fulcrum/Cargo.toml`** — if a dependency is missing, report `DEVIATION:` rather than adding it.

**Context:** There is **no production socket, listener, or accept loop anywhere in this tree** (verified: `UnixListener|NamedPipe|TcpListener|\.sock` hits only `#[cfg(test)]` wiremock stubs in `harness/catalog.rs`). The nearest analogue is the worker stdio transport, and from it take one rule that is easy to get wrong: **`read_line` on a shared reader is not cancellation-safe**, which is why `client.rs:268-307` gives a pipe reader its own dedicated task. A socket reader has the same hazard.

Frame format is **JSONL, one frame per `\n`-terminated line** — identical to the worker protocol, so do not invent a second framing.

**Files:**
- Modify: `fulcrum/src/transport/mod.rs` — the platform-neutral surface. **Task 1 created this and its siblings as empty stubs and declared them in `lib.rs`; do not edit `lib.rs`**
- Modify: `fulcrum/src/transport/unix.rs` — `#[cfg(unix)]`, `tokio::net::UnixListener`
- Modify: `fulcrum/src/transport/windows.rs` — `#[cfg(windows)]`, `tokio::net::windows::named_pipe::NamedPipeServer`
- Modify: `fulcrum/src/transport/frame.rs` — JSONL read/write, shared by both
- Test: `fulcrum/tests/transport.rs`

**What to implement:**
1. A `Listener` type with `bind(endpoint) -> Result<Self>`, `accept() -> Result<Connection>`, and `endpoint() -> Endpoint`. `Endpoint` is an enum or newtype over the socket path / pipe name; the pipe name is `\\.\pipe\fulcrum-<something>`.
2. `Connection`: bidirectional, `send_frame<T: Serialize>(&T)` and a stream of decoded lines. **Give the reader its own task** (mirror `spawn_pipe_reader`'s reasoning) rather than selecting over a `read_line` future.
3. **The `cfg` split must compile with `-D warnings` on all three OSes.** Use this repo's established pattern — `_`-prefixed parameters with an unconditional call site (`childproc.rs:72-175`, `hide`/`hide_tokio`/`append_cmd_c`) — or gate whole modules with `#[cfg(unix)] pub mod unix;` / `#[cfg(windows)] pub mod windows;` and give `mod.rs` a `pub use` for each. `cfg!(unix)` in an `if` does **not** remove the other branch and will produce dead code; that has bitten this repo twice.
4. Unix socket path length: `sockaddr_un` has a ~104-byte path limit on macOS. **Do not put the socket under the default data dir without checking the length**; on failure, report the length and the path in the error.
5. **Do not implement replay, `attach`, `afterSeq`, or a session registry.** That is T10. The protocol already stamps `frameSeq` (`protocol.rs:394-420` (`stamp_outbound`), 1-based, reset per `Start`) and nothing consumes it yet — leave that alone so T10 can build on it.

**Steps:**
- [ ] Write `fulcrum/tests/transport.rs` first: bind, accept in a task, connect, send a JSONL frame each way, assert both arrive. Run `cargo test -p fulcrum --test transport` → **must fail to compile**. The expected error is an **unresolved import for the items** (`Listener`, `Connection`, `Endpoint`), NOT "no `transport` module" — Task 1 already created `transport/` as an empty stub and declared it in `lib.rs`, so the module resolves and only its contents are missing. If you get E0583 "file not found for module", Task 1's stubs are wrong: stop and report `DEVIATION:` rather than creating `lib.rs` edits. Confirm the failure is the unresolved items, not something incidental (a missing dep, a typo).
- [ ] Implement `frame.rs` + `unix.rs`. Re-run → passes on Linux.
- [ ] `cargo check --target x86_64-pc-windows-gnu --all-targets` → the windows half must **type-check**. Then implement `windows.rs` properly. **This machine cannot run Windows tests (no wine, no `lib.exe`): `--all-targets` proves it compiles, never that the pipe works.** Say so in the report.
- [ ] `cargo clippy --all-targets -- -D warnings` and the same for `--target x86_64-pc-windows-gnu`.
- [ ] Prove the test is not vacuous: break the framing deliberately (drop the `\n`, or send to a wrong pipe name) and confirm the test **fails**, then revert. **A transport test that passes both ways proves nothing.**
- [ ] `cargo fmt --all` last. Commit: `feat(daemon): platform-split JSONL transport over UDS and Named Pipe`

**Acceptance criteria:**
- [ ] A loopback test sends and receives a JSONL frame on unix, and the Windows path compiles under `--all-targets`.
- [ ] No `windows`/`windows-sys` dependency added.
- [ ] The reader is on its own task, with a comment naming the cancellation-safety reason.
- [ ] The non-active platform's code produces no `dead_code`/`unused` warning on any of the three legs.
- [ ] Non-vacuity demonstrated by a deliberate break.

---

### Task 4: `fulcrum.db` — WAL, schema, and `impl FulcrumPersistence`

**Parallel:** wave 3 — disjoint from Task 3; **must not edit `fulcrum/Cargo.toml`**.

**Context:** `fulcrum-core` has **zero** rusqlite (verified — every `rusqlite` hit in `src/` is a comment; `boundary_pins` forbids it via an obfuscated needle). ADR 0091's title is literally *"the desktop owning the disk"*, and the daemon is the thing that takes the disk. The engine side already exists: **`FulcrumPersistence`** (`src/seams.rs:149-243`, 16 methods, `pub trait … : Send + Sync` so it is object-safe; also re-exported at `src/lib.rs:295` as `fulcrum_core::FulcrumPersistence`) is the supervisor-side seam, and **`TranscriptPersister`** (`src/persist.rs:77`) is the only consumer of it in the crate — `new(Arc<dyn FulcrumPersistence>)`, `set_cwd(session_id, cwd)`, `apply(is_subagent, &StoreFrame)`, `apply_interactive_event(...)`. The daemon implements the trait; the crate stays pinned.

**The schema lives in the other repo and the daemon must not silently fork it.** The desktop's four tables, verbatim from `src-tauri/src/storage/db.rs:1565-1604`:

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY, cwd TEXT NOT NULL, created_at INTEGER NOT NULL,
    title TEXT, capabilities_json TEXT NOT NULL, archived INTEGER NOT NULL DEFAULT 0,
    context_usage_json TEXT, is_subagent INTEGER NOT NULL DEFAULT 0, todos_json TEXT,
    cost_usd REAL, sort_order INTEGER NOT NULL DEFAULT 0, title_stage INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL, message_key TEXT, payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL, UNIQUE(session_id, kind, message_key)
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);
CREATE TABLE IF NOT EXISTS native_messages (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL, role TEXT NOT NULL, content_json TEXT NOT NULL,
    created_at INTEGER NOT NULL, UNIQUE(session_id, seq)
);
CREATE TABLE IF NOT EXISTS spaces (
    path TEXT PRIMARY KEY, created_at INTEGER NOT NULL, last_opened_at INTEGER NOT NULL,
    trusted INTEGER NOT NULL DEFAULT 0, sort_order INTEGER NOT NULL DEFAULT 0
);
```

**WAL is genuinely new — the desktop has none.** Grep of the desktop's `storage/db.rs` for `journal_mode|WAL|PRAGMA` finds only `foreign_keys` and `pragma_table_info`. So WAL is this task's addition, and **it must not be described as "matching the desktop"**, because nothing would match it.

**Files:**
- Modify: `fulcrum/src/db/mod.rs` — `Db`, open/pragmas/migrations
- Modify: `fulcrum/src/db/schema.rs` — the DDL above, as a `SCHEMA` const
- Modify: `fulcrum/src/db/persistence.rs` — `impl FulcrumPersistence for Db`
- Test: `fulcrum/tests/db.rs`

**What to implement:**
1. `Db { conn: std::sync::Mutex<rusqlite::Connection> }`. **`rusqlite::Connection` is `!Sync`** — the desktop uses exactly this shape (`db.rs:305-311`) and blocks on the mutex inside async handlers. `persist.rs` calls are synchronous, so follow that model; do not reach for `tokio-rusqlite` or an async pool.
2. Pragmas at open, in this order: `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`. **Set `journal_mode` before `foreign_keys`** and assert both took: query `PRAGMA journal_mode` back and **fail if it did not return `wal`** — a filesystem that cannot do WAL (some network mounts) silently leaves `delete`, and a daemon that believes it is in WAL when it is not has a durability story that is false.
3. Schema: the four tables verbatim, `execute_batch(SCHEMA)`. **Keep the `UNIQUE` constraints** — `TranscriptPersister`'s `apply` does idempotent upserts that depend on them.
4. `impl FulcrumPersistence for Db` for **all 16 methods** (`src/seams.rs:149-243`). `store_rows.rs` holds the DTOs it trades in (`SessionRow`, `MessageRow`, `SessionInfo`, `ContextUsage`, `DisplayRow`) — pure data, no rusqlite.
5. **Migrations:** start with a `user_version` pragma and one migration slot. Do not port the desktop's migration chain — those tables already exist on users' disks and this is a **new file** (`fulcrum.db`, not `archimedes.db`).

**Steps:**
- [ ] Write `fulcrum/tests/db.rs` first: open in a temp dir, assert `PRAGMA journal_mode` reports `wal`, insert a `SessionRow`, read it back, `impl FulcrumPersistence` compiles. Run `cargo test -p fulcrum --test db` → **must fail to compile** with unresolved imports for `Db` / `schema` (the `db/` stubs exist from Task 1, their contents do not). Confirm that is the failure and not an incidental one.
- [ ] Implement. Re-run → green.
- [ ] **Prove WAL is real, not assumed:** assert `PRAGMA journal_mode = wal` returns `wal`, **and** assert the sidecar files appear (`<db>-wal`, `<db>-shm`) after a write. If the assertion passes on a filesystem where no `-wal` file is ever created, the test is vacuous — make it fail on purpose (set `journal_mode=delete`) and confirm red.
- [ ] **Prove the FK cascade and the upsert idempotency** the persister relies on: insert the same `StoreFrame` twice through `TranscriptPersister::apply` and assert one row, not two; delete a session and assert its messages go.
- [ ] `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`.
- [ ] Commit: `feat(daemon): fulcrum.db — WAL-mode store implementing FulcrumPersistence`

**Acceptance criteria:**
- [ ] `Db` implements `FulcrumPersistence` and is used through `Arc<dyn FulcrumPersistence>` by `TranscriptPersister` in a test.
- [ ] WAL asserted by query **and** by sidecar file, with a deliberate break shown.
- [ ] Re-applying the same frame is idempotent; session delete cascades.
- [ ] `rusqlite` appears **only** in the `fulcrum` member — `cargo test -p fulcrum-core --lib -- boundary_pins::` still 9/9 green, and `grep -rn "rusqlite" src/*.rs src/*/*.rs` (the engine) still hits comments only.

---

### Task 5: `fulcrum serve` — multiplex clients onto supervised workers, plus `fulcrum --worker`

**Parallel:** none — wave 4, needs Task 3's transport and Task 4's db.

**Context:** The supervision machinery already exists and is host-agnostic by design. **Reuse it; do not re-implement a session registry.**

```rust
// src/worker/manager/mod.rs
pub trait WorkerFactory: Send + Sync {                       // :78
    fn spawn(&self) -> Result<WorkerHandle, WorkerError>;
}
pub type SinkLookup = Arc<dyn Fn(&str) -> Option<Arc<dyn EventSink>> + Send + Sync>;  // :141
pub type OnCrash = Arc<dyn Fn(String, Option<i32>, Option<u32>) + Send + Sync>;       // :149
impl WorkerManager {
    pub fn new(factory: Arc<dyn WorkerFactory>, on_crash: OnCrash,
               on_event: Arc<dyn Fn(String, bool, WorkerInboundEvent) + Send + Sync>,
               sink_for: SinkLookup) -> Self;                                          // :167
    pub async fn attach(&self, session_id: &str, env: &StartEnv) -> Result<(), WorkerError>;  // :223
}
// src/worker/client.rs
pub fn spawn(exe: &std::path::Path) -> Result<Self, WorkerError>                      // :304
// src/persist.rs
impl TranscriptPersister {
    pub fn new(db: Arc<dyn FulcrumPersistence>) -> Self;                              // :94
    pub fn set_cwd(&self, session_id: &str, cwd: &str);                               // :105
    pub fn apply(&self, is_subagent: bool, frame: &StoreFrame);                       // :117
    pub fn apply_interactive_event(&self, session_id: &str, is_subagent: bool, payload: …);  // :206
}
```

`attach` already does the whole lifecycle: `factory.spawn()` → `wait_ready(5s)` → `start_pump` → `send_start(env)` → register under `session_id`. Crash detection, the dead-bound, and subagent dispatch are inside.

**The one structural gap to design around:** `run_worker` (`src/worker/mod.rs:80-95`) reads `stdin` and writes `stdout` behind a mutex — **one process, one session**. The daemon therefore supervises **N child processes** and relays each child's stdout to the socket clients watching that session. That is the multiplex. There is **no** "attach to session X on worker Y" frame in the protocol; `Inbound::Start` is per-process. **The client↔daemon control frames are a new surface the daemon owns, not a change to the worker protocol.**

**Files:**
- Modify: `fulcrum/src/serve.rs` — accept loop, client sessions, multiplex
- Modify: `fulcrum/src/worker_factory.rs` — `impl WorkerFactory` for the daemon
- Modify: `fulcrum/src/main.rs` — argv dispatch: `--worker` → worker mode, `serve` → daemon, else usage
- Test: `fulcrum/tests/serve.rs`, `fulcrum/tests/worker_mode.rs`

**What to implement:**
1. `DaemonWorkerFactory { exe: PathBuf }` implementing `WorkerFactory` by calling `WorkerHandle::spawn(&self.exe)`. **Where `exe` comes from must be explicit and tested**: `--worker-exe <path>` flag, else `std::env::current_exe()` (the sidecar case is the desktop's problem, T12). Do **not** refactor `run_worker_inner` to share code with a generic runner — `production_crashlog_never_resolves_a_data_dir` asserts **exactly** `(1, 1)` for `hook(base.join` and `dirs::data_dir()` in that function's production region, so a second call site in the crate reddens it.
2. `fulcrum --worker` mode calls `fulcrum_core::worker::run_worker(open_requester, install_crash_hook)`. Use **`fulcrum_core::mcp::NoOpenRequester`** — it is production and ungated (`src/mcp/mod.rs:81`, `impl OpenRequester for NoOpenRequester` at `:83`), which is precisely why it exists. Pass `None` for the crash hook: `run_worker`'s own doc says `None` is *"the shape a non-desktop host (the daemon, T9) passes"*.
3. Wire `TranscriptPersister::new(Arc::new(db))` into the `on_event` callback so every frame persists, and `sink_for` to fan frames out to the socket clients for that session.
4. Client control frames — the minimum for T9, **on the daemon's own surface**, JSONL like the rest: `hello` (client → daemon), `start {sessionId, …StartEnv fields}`, `prompt {sessionId, text}`, `event` (daemon → client, per session), `close {sessionId}`. `StartEnv` is `src/worker_types.rs:85` (`session_id`, `cwd`, `mode`, `transcript`, `model`, `catalog`, `thinking`, `trusted`, `enabled_tools`, `permission`, `config_dir`, `subagent_enabled`) and derives `Serialize`/`Deserialize`, so it can be the wire form directly.
5. **`fulcrum.db` must be opened once**, shared as `Arc<Db>`, not per client.

**Steps:**
- [ ] Write `fulcrum/tests/worker_mode.rs` first — spawn the built `fulcrum` binary with `--worker`, read `hello` then `ready`, assert `protocolVersion`. **Spawning a bin by path from a test needs the bin materialised: `fulcrum` is this member's own bin and `env!("CARGO_BIN_EXE_fulcrum")` works in an integration-test target — that is the clean route. Do NOT add a `cargo build --all-targets` step to CI to fix a missing binary** (`ci.yml:60` forbids it); if the bin is absent, the test belongs in `tests/` and that is the fix.
- [ ] Run it → fails. Implement `main.rs` dispatch + `--worker`. Re-run → green.
- [ ] Write `fulcrum/tests/serve.rs`: boot `serve` on a temp socket path, connect, `start`, `prompt`, assert an `event` arrives **and** that `fulcrum.db` gained the session row.
- [ ] Then the durability test, which is the roadmap's `done-when` and the only one that matters — and it must be written so it can actually pass **without** T10's `attach`:
  - **a.** drop the client socket mid-turn (do not send `close`), then assert the worker child's pid is still alive (`try_wait()` returns `None`, or poll `/proc/<pid>` on unix).
  - **b.** after the drop, open `fulcrum.db` **directly** (the test owns the file — no wire surface needed) and assert `native_messages` has **more rows than at the moment of the drop**. Rows growing after the client vanished is the whole claim: execution continued and persisted.
  - **Do not** assert that a second connection can rejoin the stream. There is no wire frame for it yet — `grep -riE '"(attach|resume|subscribe)"' src/worker/protocol.rs` is empty; `WorkerManager::attach` (`manager/mod.rs:223`) is **in-process**, not a protocol frame. Building one here would pre-design T10's `attach { afterSeq }` and force that task to extend a frame it did not design.
- [ ] Implement `serve.rs` + the multiplex. Re-run → green.
- [ ] **Prove the durability test can fail.** A test that passes with the teardown bug present is not a durability test. Plant the bug on purpose — in `serve.rs`, make the client handler call `detach`/`kill` on its sessions when its socket drops — and confirm BOTH halves of the test above go red (the pid check AND the row-count check). Then remove the plant and confirm green. If either half passes with the bug in place, that half is not testing what it claims: the pid assertion alone would pass if the worker were merely slow to die, and the row-count assertion alone would pass if the worker had finished normally rather than survived. **If the daemon tears the session down when the client vanishes, the feature is not built, however green everything else is.**
- [ ] `cargo test` whole workspace; `cargo clippy --all-targets -- -D warnings` + the windows-gnu equivalent; `cargo fmt --all -- --check`.
- [ ] Commit: `feat(daemon): fulcrum serve — supervise session workers over the local socket`

**Acceptance criteria:**
- [ ] `fulcrum --worker` emits `hello` then `ready` and speaks the existing worker protocol unchanged.
- [ ] `fulcrum serve` runs a session in a child process, persists it, and streams events to a socket client.
- [ ] **A client disconnect does not kill the session**, proven BOTH ways: the worker process is alive after the drop, **and** `fulcrum.db` gained `native_messages` rows after the drop. An assertion that only checks the pid proves the process survived, not that it persisted.
- [ ] `WorkerManager` is used, not re-implemented; no second session registry exists.
- [ ] `run_worker_inner`'s production region is untouched (`(1,1)` crashlog pin still green).

---

### Task 6: Give the daemon member its own boundary pins

**Parallel:** none — last. It asserts over the finished member, so it cannot run before Task 5.

**Context:** The engine's `boundary_pins` walk `{CARGO_MANIFEST_DIR}/src` and read only the root `Cargo.toml`, so **the `fulcrum` member is invisible to every existing pin.** Right now the daemon could `use tauri::…` or `use archimedes_lib::…` and CI would be green. This repo's standing rule is that an invariant held by discipline is not held — so pin it. The point is narrow and specific: **the daemon must not become a second desktop.** It is allowed `rusqlite` (it owns the disk) and must be forbidden everything that would make it a Tauri app or a desktop client.

**Files:**
- Create: `fulcrum/tests/boundary.rs`
- Modify: `fulcrum/src/main.rs` only if a diagnostic needs a home (prefer not)

**What to implement:** a source scan over `fulcrum/src/**/*.rs` that (a) strips comments before matching, (b) asserts a **vacuity floor** on files scanned, so a walk that read nothing cannot pass — the engine's own pins use this shape (`found.len() > 40` against 135 files), and every guard added in this roadmap that lacked one has gone silently vacuous — and (c) fails on `tauri`, `tauri_plugin`, `archimedes_lib`, `crate::config::`, `crate::storage::`, and on `rusqlite` appearing in `fulcrum-core`'s tree (a cross-check that the daemon's DB code has not migrated into the engine, which the engine's own pin would then catch loudly — but this pin should name the daemon as the offender, because that is where it would be written).

**⚠️ Two traps this repo has already recorded, both of which will bite here:** a source scanner **cannot distinguish code from prose about code** — a doc comment saying "the daemon may use rusqlite" would trip the needle. `boundary_pins` solves this by obfuscating its own needles (`needle('\u{63}', "rate")`); copy that technique, or scan `code_only` text and keep the literals out of the message. And **the diagnostic message must not contain the needle it reports**, or the guard fails on its own file.

**Steps:**
- [ ] Write `fulcrum/tests/boundary.rs` with the floor first; run `cargo test -p fulcrum --test boundary` → green on an empty tree is **wrong**, so confirm the floor actually fails when pointed at an empty directory before trusting it.
- [ ] **Prove teeth by mutation:** add `use tauri::Url;` to `fulcrum/src/serve.rs`, confirm red naming the file, remove it, confirm green. Repeat for `rusqlite` in `fulcrum-core/src/`. **If you cannot make one go red, say so explicitly rather than reporting green.**
- [ ] `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`.
- [ ] Commit: `test(daemon): boundary pins for the fulcrum member`

**Acceptance criteria:**
- [ ] `fulcrum` cannot import `tauri`/`archimedes_lib` without CI failing, with a message naming the file.
- [ ] The scan has a floor that fails on an empty walk, demonstrated.
- [ ] Both mutations produce red, and the guard does not fire on its own diagnostic text.

---

## Out of scope for this plan (state it in the PR, do not silently drop it)

- **Desktop sidecar wiring.** `bundle.externalBin` is **absent** from the desktop's `src-tauri/tauri.conf.json` (verified) and the desktop spawns its worker via `current_exe()` + `--worker` (`src-tauri/src/main.rs:7`, `lib.rs:44`). Making the desktop ship and locate a separate `fulcrum` binary is desktop-repo work and belongs with **T12** (desktop becomes a daemon client), which is where it is actually needed. This plan produces the binary; it does not ship it.
- **Replay / `attach { afterSeq }` / ring buffer** — T10. `frameSeq` is stamped and unconsumed; leave it.
- **`pending_gates` / park-and-wait** — T11.
- **`wss://` listener + bearer auth** — T13. **`serve` is a local socket with no authentication; do not expose it on a TCP port in this task.**
- **Layout B** (moving the lib into a subdirectory) — blocked by the census finding above; needs its own change to `source_roots()`.
- **Desktop rev bump.** The desktop currently pins `78e5954`, one commit behind this repo's `8e93496`. None of this reaches the desktop until the pin moves — a desktop-side commit.

## Gate (every task, from the repo root)

```bash
cargo build --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
cargo clippy --target x86_64-pc-windows-gnu --all-targets -- -D warnings
cargo check --target x86_64-pc-windows-gnu --all-targets
cargo fmt --all -- --check
bash scripts/check-rquickjs-allocator.sh --self-test && bash scripts/check-rquickjs-allocator.sh
```

Baselines at `8e93496`, **re-measure rather than trusting these**: crate lib **1402 passed**, workspace **1406 / 0 failed / 1 ignored / 5 blocks**; `boundary_pins` **9/9**; clippy clean on both targets. Enumerate test names with `cargo test -- --list`, **never** by grepping sources (a loose grep counts `#[test]` inside doc comments; a strict one misses `#[tokio::test(flavor = …)]`).

**Known flakes — re-run once before investigating, never loosen an assertion:** `childproc::tool_child_reads_eof_on_closed_stdin` (Windows; its own docblock says it passes with or without `closed_stdin` when cargo's stdin is already `/dev/null`); `worker_e2e::subagent_dispatch_end_to_end_real_workers`; `harness::provider::transport::tests::a_slow_stream_completes_but_a_silent_stream_errors` (ubuntu); `provider_gcloud_adc::the_adc_arm_runs_even_with_execute_helpers_false`.

**Environment traps hit during this planning session:** `/tmp` here is a 12 GB tmpfs shared with everything else and it went near quota — a `Disk quota exceeded` / `failed to build archive` error is a **resource** failure; read the text before blaming a test. And `cargo clean -p <pkg>` **does not dirty a git dependency** (nor does `touch` in the checkout) — to force a rebuild, delete `target/…/fingerprint/<pkg>-*`.

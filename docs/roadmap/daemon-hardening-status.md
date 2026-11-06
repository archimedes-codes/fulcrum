---
status: running
mode: autonomous
updated: 2026-11-06
---

# daemon-hardening — STATUS

**READ THIS FIRST on every iteration. This file is the loop's only memory.**

## Mission

Land the four post-T9 hardening items filed against `fulcrum serve` (GitHub issues #3, #4,
#5, #6 in `archimedes-codes/fulcrum`) as one branch → one PR. All four were triaged as
**genuinely open** (filed 2026-11-06T15:03Z, zero comments, zero linked PRs, zero
cross-references) and each carries a full body with acceptance criteria, so no issue needs
re-specifying — the issue bodies ARE the approved block. Finished means: all four tasks
`done` or `blocked`, the repo's validation bar green on the integrated tree, and `review` +
`finish` run.

## Operating mode

The whole wave moves at once — planning fans out, it is not one task at a time.

**On every resume, before dispatching anything:** run `git status --porcelain` **and `git worktree list`.** A wave
child's edits land in its own worktree, so a clean primary tree proves nothing — an `*-taskNN` worktree left over
from a dead iteration means that task died **mid-implementation**: check `git -C <worktree> status --porcelain` for
each one. Such a task is `implemented`, not `todo`, and its siblings may be half-written. Commit or set aside those
edits — in that worktree, on that task's branch; never dispatch a new wave on a dirty tree. Then re-derive every
task's state from the table's Status tests (plan file? changelog? commit?) — the tree outranks whatever value is
written, so correct the column before acting on it. Then rebuild the **session list** (`manage_todo_list`) from the
corrected column — one entry per task row, `W<wave> · <task id>`, replacing whatever entries are there, including an
empty list. The list is session-scoped and does not survive compaction, so on resume it is always wrong: rebuild it
rather than trusting it. Map each row's Status to a list state — `todo`/`planned`/`vetted` → pending,
`implemented` → in-progress, `done` → complete.

Loop protocol — the run's phases, in order (mirrors `skills/roadmap/SKILL.md` Steps 0–8; a resumed run starts at the
first step whose output is missing, never re-dispatches a step whose output already exists):
1. **Decompose & branch** (Step 0): tasks drafted, areas assigned, feature branch exists, session list built. Skip
   if the Tasks table already has rows.
2. **Fan out** (Step 1): one baseline child (`explore`) measures the gate; one `researcher` per area not yet in the
   Grounding table writes its report with `## Will touch`, `## Provides` and `## Needs from other areas`. One response.
3. **Resolve** (Step 2): merge tasks that share a Will-touch file; record each dependency edge with its verbatim
   signature in the graph below; re-label the session list; commit the status file and reports by pathspec.
4. **Plan every task** (Step 3): consumed `specify` via `researcher` for every task with no plan file yet, all in one
   response. Each dispatch carries `Builds on:` with the signatures from step 3's graph.
5. **Vet & commit plans** (Step 4): one `reviewer` per plan, one pass. Only 🔴 findings send a plan back, re-vetted
   alone. Commit the plans by pathspec.
6. **Implement, one wave at a time** (Step 5): mark the wave in-progress in the session list, dispatch its `general`
   children (≤4), each running its own test files only. A signature change is reported as `DEVIATION:` and the later
   plans that build on it are re-planned before the next wave.
7. **Gate** (Step 6): fan in in plan order; run the baseline command once on the integrated tree; only new failures
   count. Green → `done <hash>` here, one Changelog line, session list updated. Red → step 8.
8. **Fix** (Step 7): failures assigned to the task that owns the file; fixers in worktrees; at most 2 rounds.
   Still red → `blocked` with evidence in Blockers, and only the dependent waves stop.
9. **Ship** (Step 8): when no row reads `todo`, `planned`, `vetted` or `implemented`, run `review`, then `finish`.
   Clear the session list after the follow-ups are folded.

Steps 1–9 are the roadmap skill's Steps 0–8, same order, same meaning.

## Gate — establish before task 1, re-run after every wave (and after each serial task)

Run from the repo root (`/home/daniel/Coding/Archimedes/fulcrum`), which is also the `fulcrum-core` package root:

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo check --target x86_64-pc-windows-gnu --all-targets   # CI runs the Windows leg; local cross-target may be absent
bash scripts/check-rquickjs-allocator.sh
bash scripts/check-rquickjs-allocator.sh --self-test
```

`cargo test` and `cargo clippy --all-targets` are the load-bearing pair, and they are **bare on purpose**: the root
manifest's `default-members = [".", "fulcrum"]` is what makes a bare command cover BOTH members. Do not add
`--workspace`.

**Baseline measured at `958306d`: `cargo test` exit 0 — 1570 passed / 0 failed / 1 ignored.**
Breakdown (read the `test result:` lines, do not trust this copy if it disagrees with the run): engine lib 1401 +
`tests/fixture_bins.rs` 5 = 1406 for `fulcrum-core`; daemon member 164 (lib 38, `boundary` 10, `db` 29, `serve` 37,
`transport` 19, `worker_mode` 31). `clippy --all-targets -D warnings` exit 0, `fmt --check` exit 0. The 1 ignored test
is `web::providers::duckduckgo::tests::test_live_ddg_challenging_queries` (live network).

⚠ **The first baseline reading was 19 failures / exit 101 and it was BOGUS — see the Decisions log entry for
2026-11-06 on the stale-`target/` trap.** If a run suddenly shows ~19 red `boundary_pins` / `childproc` /
`source_lints` tests whose panic text names a path that does not exist (`CARGO_MANIFEST_DIR=/home/daniel/Coding/AI/…`),
that is a stale artifact from a moved checkout, NOT a defect: recompile the `fulcrum-core` lib target
(`touch src/lib.rs && cargo test -p fulcrum-core --lib`) before recording anything as a new failure. That recompile
measured 48 s. **Do not reach for `cargo clean`** — one recompile of the affected target is enough, and the full cost
of a clean rebuild has never been measured here.

`cargo check --target x86_64-pc-windows-gnu` is part of the repo's documented bar but was NOT measured here: the
cross-target is not installed in this environment. Record it as unmeasured rather than passing or failing it, and
rely on the CI Windows leg.

## Dependency graph / waves

```
Wave 1: T1, T2, T3   (owned file sets disjoint — verified against each area's Will touch, see rulings below)
Wave 2: T4           (builds on T3's post-change shape of serve_client)
```

**The one real edge, T3 → T4.** T4 does not consume a symbol T3 *creates*; it restructures the code T3 *inserts*.
Specifically, T3 puts required-`hello` state and per-connection frame-rate state at the top of `serve_client`, before
its `select!`, and T4 deletes that `select!` and replaces it with a reader/reply task plus a writer task. T4's plan
must therefore relocate T3's handshake gate and rate-limit state into the reader half, and it cannot be written
against a `serve_client` that no longer looks like the T3 output. The signature it consumes is
`serve_client` itself — read T3's committed shape, not this file's description of it.

**Wave 2 width 1 — both tasks edit `fulcrum/src/serve.rs`, `fulcrum/src/transport/{unix,windows}.rs`,
`fulcrum/tests/serve.rs` and `fulcrum/tests/transport.rs`.** Merging them instead would put ~10 steps and 10 files
(including a 2,524-line `serve.rs`) into one implementer, past the partition ceiling (6 steps / 8 files / one test
tree). This is the honest serial shape; do not re-cut it.

### Wave-1 disjointness rulings (Step 2 merge decisions — both were real collisions)

1. **T2 may not open `.github/workflows/ci.yml`.** The `daemon-store` report listed it as "modified (only if the
   comparison job lives in fulcrum CI)"; T1 modifies that same file. Had the drift check landed as a step in `ci.yml`
   these two tasks could never share a wave. **Ruling: T2 ships its drift check as a NEW workflow file.**
2. **`fulcrum/src/transport/frame.rs` belongs to T4 alone.** The `transport-serve` report listed `frame.rs` as
   conditionally touched by T3 ("if peer identity rides inside `Connection`"). **Ruling: T3 resolves the peer
   credential at accept and enforces it in `run`'s accept arm; peer identity must NOT be carried in `Connection`.**
   That keeps `Connection::new`'s signature out of the wave and removes the ripple.

## Grounding reports

One per code AREA, written once, read by every planner for that area. Re-ground only if the tree moved inside the area.

| Area | Report | Grounded at |
|---|---|---|
| ci-verification (T1, T2's CI half) | `docs/roadmap/daemon-hardening-ground-ci-verification.md` | `958306d` |
| daemon-store (T2's schema half) | `docs/roadmap/daemon-hardening-ground-daemon-store.md` | `958306d` |
| transport-serve (T3, T4) | `docs/roadmap/daemon-hardening-ground-transport-serve.md` | `958306d` |

Note: the reports' required section headings were written as `## \`## Will touch\`` (literal backticks around the
heading text) in two of the three files. Cosmetic only — the sections are complete. Match on the inner string.

The `daemon-store` report is the only one that could read the **other side of the pin**: a desktop checkout exists at
`/home/daniel/Coding/Archimedes/archimedes` (remote `archimedes-codes/desktop`, HEAD `4f9b99dc`). A planner for the
desktop's half must work in that checkout, which is a DIFFERENT REPO — see Blockers.

## Tasks

| # | Task | Area | Plan file | Wave | Owns (file set — disjointness evidence) | Status |
|---|------|------|-----------|------|------------------------------------------|--------|
| T1 | CI process census: fail the run when a daemon or worker outlives the suite (issue #5) | ci-verification | `docs/roadmap/daemon-hardening-task01-stray-process-census.md` | 1 | `scripts/check-no-stray-processes.sh` (new), `.github/workflows/ci.yml` | `vetted` |
| T2 | Cross-repo schema drift fingerprint + comparison workflow (issue #6) | daemon-store | `docs/roadmap/daemon-hardening-task02-schema-drift-fingerprint.md` | 1 | `fulcrum/src/db/schema.rs`, `fulcrum/tests/db.rs`, `schema-sha.txt` (new), `scripts/check-schema-drift.sh` (new), `.github/workflows/schema-drift.yml` (new), `README.md`, `AGENTS.md` | `vetted` |
| T3 | Peer-credential check + rate/session limits + required `hello` handshake (issue #4) | transport-serve | `docs/roadmap/daemon-hardening-task03-socket-hardening.md` | 1 | `fulcrum/src/transport/unix.rs`, `fulcrum/src/transport/windows.rs`, `fulcrum/src/serve.rs`, `fulcrum/src/main.rs`, `fulcrum/tests/serve.rs`, `fulcrum/tests/transport.rs` | `vetted` |
| T4 | Split `Connection` and `serve_client` so a slow attach cannot stall that client's event relay (issue #3) | transport-serve | `docs/roadmap/daemon-hardening-task04-split-serve-client.md` | 2 | `fulcrum/src/transport/frame.rs`, `fulcrum/src/transport/mod.rs`, `fulcrum/src/transport/unix.rs`, `fulcrum/src/transport/windows.rs`, `fulcrum/src/serve.rs`, `fulcrum/tests/serve.rs`, `fulcrum/tests/transport.rs`, `fulcrum/tests/worker_mode.rs`, `src/fixtures/worker.rs` | `vetted` |

T3 and T4 overlap in five files by design — that overlap IS the wave-2 edge, recorded above. T1∥T2∥T3 share nothing.

## Blockers / open questions

1. **T2's second half lives in another repository.** A fingerprint that fulcrum publishes is worthless until the
   desktop publishes one too: the desktop's `src-tauri/src/storage/db.rs` must emit a comparable canonical shape, and
   that file is in `archimedes-codes/desktop` (`/home/daniel/Coding/Archimedes/archimedes`), not this repo. **T2 as
   scoped here ships fulcrum's half plus a check that runs cross-repo against the desktop's pinned rev; if the desktop
   has no fingerprint to read, T2 must land in its honest degraded form — fulcrum's own fingerprint + a self-test + a
   check that FAILS LOUDLY with "the desktop publishes no fingerprint" rather than silently passing.** That is a
   finding for the owner, not a licence to edit the desktop from this branch. Do not open the desktop checkout from a
   wave child.
2. **The pinned rev is not HEAD.** The desktop pins fulcrum at `78e59543`, this branch's base is `958306d`. A
   cross-repo comparison must target the rev the desktop actually pins, or it compares a pair that no build ever
   ships together.
3. **T4 has no behavioural pin to inherit.** `send_frame`'s `pending` loop is branch-uncovered today — there is no
   partial-write / two-writer test in `tests/transport.rs`. T4 must WRITE the test that fails if `pending` becomes
   reachable from two tasks, and it must write it before relying on the split. Issue #3's acceptance criteria already
   demand this.
4. **The Windows half of T3 cannot be proven here.** `windows` / `windows-sys` are deliberately absent from
   `fulcrum/Cargo.toml` (the manifest comment forbids adding them), so `GetNamedPipeClientProcessId` has no crate
   route: hand-rolled `#[cfg(windows)]` FFI or a documented "this platform cannot prove it". `transport/windows.rs`
   is only ever type-checked cross-target and never executed locally, so any Windows test T3 ships is a compile gate,
   not proof. Prefer the documented-limitation route over new FFI; record whichever is chosen in the Decisions log.

## Decisions log

- **2026-11-06 — Entry point.** Backlog came from `check-issue` triage of issues #3/#4/#5/#6, all four
  `genuinely open` at high/medium confidence, none stale, none already fixed, none duplicated; working tree was
  clean at `958306d`. No `discuss` spec and no `research` report exists for this block, so there is no
  `docs/roadmap/daemon-hardening.md` for `specify`'s "No file" branch and no single spec `issue:` key to mirror the
  task breakdown to — the four issues ARE the backlog, one task each. Expected for this entry; `finish` skips the
  missing spec doc.
- **2026-11-06 — Decomposed by owned file set, not by issue.** Four issues → four tasks, but only after two rulings
  that kept wave 1 at width 3 (see the Wave-1 disjointness rulings). Cutting by issue naively would have put
  `ci.yml` in both T1 and T2 and collapsed the first wave to width 1.
- **2026-11-06 — `feature/daemon-hardening` off `main` at `958306d`.** `<feature>` = the branch topic =
  `daemon-hardening`; every artifact is named from it.
- **2026-11-06 — The first baseline was a false red; the gate is exit 0 / 1570 passed.** An `explore` child measured
  `cargo test` at exit 101 with 19 failures, all in `fulcrum-core --lib`, all source-census witnesses
  (`boundary_pins`, `childproc`, `source_lints`, `mcp::manager`) panicking on `CARGO_MANIFEST_DIR=/home/daniel/Coding/AI/fulcrum`
  — a directory that does not exist (`ls` confirms). Cause, measured not guessed: `env!("CARGO_MANIFEST_DIR")` is
  baked at COMPILE time, and `strings target/debug/deps/fulcrum_core-110e2e989c2eb1bd` contains that literal — the
  engine lib had been compiled from a former checkout location and cargo reused the artifact here. Fixing it cost one
  recompile (`touch src/lib.rs`, 48 s), not a rebuild: 6 `boundary_pins` failures went 6 failed → 9 passed. This is
  the AGENTS.md hazard "`env!("CARGO_MANIFEST_DIR")` depth changes between layouts … never write a level-counting
  `.parent()` path", showing up as a red suite rather than a wrong path. **Consequence for T1, and it is load-bearing:
  a `cargo test` that exits 101 means the census step after it never runs.** The census must therefore be a step
  AFTER `cargo test` in `ci.yml` (which is what issue #5 asks for anyway) and must not be written as something that
  replaces or pre-empts the test step. A second consequence: any wave's red gate must be checked against this entry
  before a fixer is dispatched, or a stale artifact gets "fixed" in code that was never wrong.
- **2026-11-06 — The two DDLs agree byte-for-byte today.** The `daemon-store` child extracted both `SCHEMA` consts
  from this repo and from the desktop checkout at `4f9b99dc`: 39 lines each, identical SHA-256 (`f0722539…`), zero
  diff hunks. So T2 starts from agreement — it is building a tripwire, not putting out a fire. It also found the
  daemon ALREADY ships a canonical per-table fingerprint (`TABLE_SHAPES` plus the three `*_TABLE` consts, with a
  self-test proving they match the DDL), so T2 is mostly serialisation plus the cross-repo plumbing, not new schema
  knowledge. Cheapest-correct-enough applies: prefer reusing `TABLE_SHAPES` over inventing a second fingerprint form.
- **2026-11-06 — `StartEnv.permission` is untouched wire bytes for T3.** The `transport-serve` child verified that
  nothing in the daemon reads `StartEnv.permission` — it rides verbatim to `WorkerManager::attach`, and `start_env`
  strips only `type`. A handshake or gate that rewrites or pre-checks `permission` would change what the CHILD's gate
  enforces. T3 treats it as opaque.
- **2026-11-06 — No issue comments, no labels written.** The run mirrors its task breakdown nowhere, because there is
  no spec issue to mirror to (see the entry point decision), and the triage pass wrote no labels. Issues #3–#6 stay
  as triaged: `enhancement`, unassigned.

## Changelog

One line per task: hash, what shipped, gate numbers at that moment. Figures below are historical.

### Vet rounds (Step 4) — these lines are what make the rows above read `vetted`

- **T1** planned → vet R1 **FAIL** (3 🔴) → refined → vet R2 **PASS**, 1 🟡 (prose: say "basename of the FIRST argv element of `args=`", so a `--worker-exe <path>` tail can't be mistaken for the image).
- **T2** planned → vet R1 **PASS**, no 🔴, no 🟠. One implementation-time note: run the added fixtures once to retire the extraction-vacuity gap before merge.
- **T3** planned → vet R1 **PASS**, no 🔴. Stale dirty-tree claim removed from the plan by the orchestrator (see Decisions).
- **T4** planned → vet R1 **pass-with-issues + 3 🔴** → refined → vet R2 **pass-with-issues + 1 new 🔴** (writer can outlive `serve_client`) → refined → vet R3 **PASS**.

### Decisions added by vetting

- **2026-11-06 — `pgrep -x` cannot see the fixture workers; measured, not reasoned.** `pgrep -x fake_worker_core` (16 chars) prints *"pattern that searches for process name longer than 15 characters will result in zero matches"* and **exits 1 regardless of what is running** — `comm` is truncated to 15. I confirmed the truncated form matches: a process at `/tmp/fake_worker_core` was found by `pgrep -x fake_worker_cor` and missed by the 16-char name. `fake_mcp_stdio_core` is 19 → `fake_mcp_stdio_`. So a census written with the real image names is **silently blind to both workers on Linux and reports green**. T1's plan now enumerates `ps -eo pid=,comm=,args=` and matches the FULL basename of the first `args=` element; self-test Case B2 (19-char name must be caught) and Case B3 (`fake_worker_corruptor`, same 15-char comm, must be left alone) pin both halves of that fix.
- **2026-11-06 — My own instruction to T4 was wrong and the planner corrected it.** I told it "drop the writer half; the blocked write then errors." `WriteHalf<T>` is `Arc<Inner<T>>` with **no `Drop` impl** (verified in vendored tokio, and the round-3 vet confirmed it against the pinned 1.54.1), so dropping one half cannot close the write direction while the other half holds its `Arc` clone. The plan's replacement — explicit `poll_shutdown` on the shared inner while the write half is alive, because `unsplit` panics on a consumed pair — is correct. Recorded because the tree outranks the text, including mine.
- **2026-11-06 — T4 IMPLEMENTER MUST KNOW: the `read_to_end`-conveys-write-side-EOF claim in the plan is FALSE and must not be relied on.** `ReadHalf::poll_read` only does `inner.with_lock(|s| s.poll_read(..))` — it never calls `poll_shutdown`, so draining the read half to EOF says nothing about the write direction and cannot un-park a blocked write. The vet judged it load-free because the unconditional `JoinHandle::abort()` after `reader_loop` returns is what actually carries "no task outlives `serve_client`". Delete or correct that prose when implementing; do not build the teardown on it.
- **2026-11-06 — T4's slow-start test is only sound while `RELAY_ARRIVAL_BUDGET` ≤ 0.6 × `SLOW_READY_MS`** (1500/800 today; fallback 2000/1200 preserves the margin). Raise the budget toward `SLOW_READY_MS` and the test can false-GREEN on the unsplit design — i.e. the proof stops proving. There is no compile-time guard for this; it is a comment-guarded invariant.
- **2026-11-06 — T3's plan carried a FALSE blocker and it was deleted, not obeyed.** The planner asserted "the working tree is NOT clean: `frame.rs` has an uncommitted local edit (the `Connection`/`writer`/`pending` fields removed) and `cargo check` fails on it". Measured: `git status --porcelain` listed only untracked plan docs, `git diff -- fulcrum/src/transport/frame.rs` was empty, and `pending`/`send_frame` were intact at `:125`/`:255`. Nothing was edited by any planner. Three sentences repeating that phantom claim were removed from the plan; had they stayed, T3's implementer would have been told to expect a red `cargo check` that does not exist and to hunt an "external edit" that never happened.
- **2026-11-06 — T2's degraded cross-repo case is not hypothetical; the pinned rev has no schema at all.** I verified `git ls-tree 78e5954` (the rev the desktop pins) lists no `fulcrum` member and zero `db/schema.rs`. So today the desktop pins a fulcrum commit with no daemon and no schema to fingerprint, and T2's "refuse loudly with a named reason, never pass vacuously" path is the state that actually ships on day one. **This is an owner decision, not a task:** the desktop must both publish its own fingerprint and re-pin fulcrum, or the cross-repo check stays red by design. Recorded as a finding; no file outside this repo is touched.

*(no tasks shipped yet — current gate figures are in the Gate section)*

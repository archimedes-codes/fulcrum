# daemon-hardening — follow-ups

**This file is a STAGING artifact.** `finish` deletes the whole `docs/roadmap/daemon-hardening-*` family on
ship, including this file, so the durable items below must be folded into a permanent home first. This repo
has **no `docs/features/` and no `adr/` directory** (ADRs 0025/0091 are referenced from `AGENTS.md` but live
in the desktop repo), so the permanent home for items marked **fold** is a new `docs/features/daemon-hardening.md`,
plus the `README.md` sections that already exist (`## Schema-shape contract with the desktop`, `## Tests`).

Statuses: **open work** = someone must do it · **recorded, not open work** = a known limit, deliberately not
fixed · **accepted** = decided, will not change here.

---

## 1. The desktop has not published its half of the schema fingerprint — **open work, and it is an owner decision**

Issue #6 asked for a cross-repo drift check. Fulcrum's half shipped, and it splits in two, so be precise about
which half is green:

- **Green today, in this repo:** `scripts/check-schema-drift.sh` compares `schema-sha.txt` against fulcrum's own
  shipped DDL, and `the_pinned_schema_fingerprint_matches_the_shipped_file` in `fulcrum/tests/db.rs` reddens
  `cargo test` the moment a daemon DDL change lands without its pin. Both DDLs agree byte-for-byte right now, so
  `.github/workflows/schema-drift.yml` is legitimately green — which is exactly why its `--self-test` step runs
  first.
- **Not green, and not running anywhere:** the **cross-repo** comparison. It is designed to run **in the
  desktop's CI** — the same comparator, fed the desktop's live DDL via `--input` and fulcrum's pin from the rev
  the desktop pins. Fulcrum deliberately **never fetches the desktop's DDL at a guessed rev**, because comparing a
  pin against a rev no build ships together is worse than refusing.

So the criterion "a change to the desktop's DDL makes some check fail" is **not yet met by anything that runs**:
it is met by a mechanism whose other half does not exist. Verified with `git ls-tree 78e5954` — the rev the
desktop pins has **no `fulcrum` member and no `db/schema.rs`**, so the desktop-side comparison refuses (exit 2,
named reason) rather than passing on an empty comparison.

Two things are needed, both in `archimedes-codes/desktop`, neither possible from this branch:
- the desktop emits a fingerprint over its `sessions` / `native_messages` DDL in the same canonical form
  (per-table column **name + type + order**, plus index lines — its own `SPACE_COLUMNS` is a 4-name
  name-only subset, so a naive name comparison would false-positive), and
- it re-pins fulcrum to a rev that actually contains the daemon, and wires the comparator step.

**Do not "fix" the guard to pass.** A comparison that succeeds because the other side said nothing is the
failure class this repo refuses elsewhere. `README.md`'s `## Schema-shape contract with the desktop` section
states this degraded condition as shipped behaviour — keep it honest when the desktop half lands.

## 2. Issue #4's "a peer of a different uid is rejected" is not executed by CI — **open work**

`fulcrum/tests/transport.rs`'s `foreign_uid_peer_is_rejected_under_root` is `#[ignore]`d: only root can drop to a
different uid and still dial. It deliberately widens the socket to 0777 / dir 0755 so the **filesystem** cannot
be what makes it pass — against the real 0600 mode a foreign peer is refused by the inode and the test would
pass with the credential check deleted. Its child entry point `child_dials_as_a_foreign_uid` is `#[ignore]`d for
the same reason (2 of the 3 ignored tests in the suite are these two).

So the unix peer check is **structured and reasoned but unproven by any automated run.** Options for whoever
picks this up: a CI job that runs that one test under a non-root uid via `setpriv`/`runuser` (needs a uid to
drop *to*, not root), or a namespace-based test. Do not delete the tests; they are the spec for the behaviour.

## 3. Windows peer credentials are a documented limitation, not enforcement — **recorded, not open work**

`windows` / `windows-sys` are deliberately absent from `fulcrum/Cargo.toml` and its comment forbids adding them,
so `GetNamedPipeClientProcessId` has no crate route. `transport/windows.rs` carries a capabilities table saying
what the platform can and cannot prove. Issue #4's peer-credential criterion is therefore **met on unix only**.
Revisit only if the manifest's ban on the `windows` crate is itself revisited, or if T13's wire auth makes the
question moot.

## 4. T1's census is ADVISORY on the Windows leg — **recorded, not open work**

`scripts/check-no-stray-processes.sh` can enumerate `fulcrum.exe` on Windows but cannot attribute a process to
*this* run (no bash-accessible way to read another process's environment; a parent-PID tree bottoms out in the
runner agent, which also hosts the developer's own `fulcrum.exe`). It reports and labels itself advisory in its
header, its output, and the plan. Issue #5's "fail the run" is **enforced on ubuntu and macOS only**.
The one thing a first Windows CI run must confirm is flagged UNVERIFIED in the script header: MSYS flag mangling
of `tasklist /FI …` arguments.

## 5. macOS has no local compile proof, for anything — **recorded; affects the whole repo, not just this feature**

`aarch64-apple-darwin` is installed but **cannot be checked here**: `ring`'s C build needs a macOS SDK and the
host `cc` has none. `AGENTS.md`'s validation bar names the Windows cross-check as "the only local Windows proof
available" and offers no macOS equivalent, so this is a repo-wide gap rather than a regression.

It bites specifically here: **T3's `#[cfg(target_os = "macos")]` / BSD `getpeereid` path has never been compiled
by anything except CI's macOS leg.** Anyone touching unix peer credentials should treat that leg as the only
witness and expect to iterate in CI.

## 6. T4's writer can still be parked by a peer that neither reads nor closes — **accepted, pre-existing**

Backpressure is the design: `send_frame` has no timeout, deliberately. A peer that stops reading without closing
parks the writer exactly as it parked the single pre-split task, and the only reaper is the connection ending
(the peer's close, or the daemon's death). What the split added is the **mechanical backstop**: `serve_client`
holds the writer's `JoinHandle` and `abort()`s it after `reader_loop` returns, so a parked writer cannot outlive
`serve_client` — witnessed by T1's census running clean against the full suite. Do not "fix" the parking; it is
the same decision `CLIENT_QUEUE_CAPACITY` encodes.

## 7. The `dropped` epilogue can under-report on the aborted path — **accepted, one-line imprecision**

`abort()` drops a task's future without polling it, so the writer's own post-loop epilogue never runs; the
"how many frames you fell behind" line was moved to `serve_client` for that reason. Because abort is a request
rather than an instant kill, that count can be short by whatever the writer dropped in the final microseconds.
The `gap` frames themselves and all their numbers are unaffected — only the log line.

## 8. `cargo doc` emits 2 `private_intra_doc_links` warnings — **recorded, not open work; and NOT a failure**

Measured on this branch: `cargo doc --no-deps --workspace` → **exit 0, 2 warnings** (a warn-by-default lint, not
errors). One is `fulcrum/src/transport/frame.rs:117` — a link to `Connection::new`, which is `pub(crate)` while
`Connection` is public; **that same link is on `main` too**, only its line number moved. The other is
`src/fs_backend.rs:270`, untouched by this feature. `cargo test --doc` passes (0 doc tests), and nothing in the
validation bar runs `cargo doc`. An implementer reported this as "3 errors, `cargo doc` fails" — it is neither.
If `cargo doc` is ever added to CI, decide that deliberately rather than as a side effect.

## 9. Two invariants that are comment-guarded, not compiler-guarded — **recorded**

- **`RELAY_ARRIVAL_BUDGET` must stay under `ATTACH_READY_CEILING` minus the other gate budgets.** Now enforced
  by a `const _: () = assert!(…as_millis() + … < CEIL.as_millis())` in `tests/serve.rs` — this one IS compiler-
  guarded as of T4b. Note the direct `Duration + Duration < Duration` form does **not** compile in const context
  on the pinned toolchain (`Add` is not a const trait); the `.as_millis()` form is required.
- **`Connection::send_frame` must not come back.** It was deleted in T4b on purpose: keeping it alongside
  `split_writer` would let one caller hold a `Connection` and a `ConnectionWriter` and reintroduce two writers
  over `pending` — issue #3's exact hazard. `a_second_split_writer_panics` and
  `a_shared_writer_under_a_mutex_never_interleaves_frames` are the pins; the latter is a regression pin against a
  *possible* future `Mutex<ConnectionWriter>` design, not a proof about the current one.

## 10. Process-hygiene facts worth keeping, because each cost a debugging cycle here

- **`pgrep -x` cannot match a name longer than 15 characters** — it prints
  "pattern that searches for process name longer than 15 characters will result in zero matches" and **exits 1
  regardless of what is running**. `fake_worker_core` (16) and `fake_mcp_stdio_core` (19) are both invisible to
  it; the census enumerates `ps -eo pid=,comm=,args=` and matches the full basename of the first `args=` element.
- **`env!("CARGO_MANIFEST_DIR")` is baked at compile time.** A `target/` built under a since-moved checkout
  produced 19 red `boundary_pins` / `childproc` / `source_lints` tests citing a path that no longer existed. The
  fix was one recompile (`touch src/lib.rs`), not a diagnosis. If those 19 tests go red citing a nonexistent
  directory, suspect a stale artifact before suspecting code.
- **`ps` renders a zombie as `[fake_worker_cor] <defunct>`** — truncated to 15, so an exact-basename rule can
  never fire on one; the census attributes a zombie by its **parent's** marker instead of its name.

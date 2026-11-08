# Daemon hardening — durable notes

Landed in PR #10 (issues #3–#6): the peer-credential check, the session/rate limits and mandatory `hello`
handshake, the `Connection`/`serve_client` task split, the stray-process census, and the cross-repo schema-shape
pin. This file is the permanent residue of that work — the things worth knowing before touching the code again.
The task-by-task plans and the roadmap status file were deleted on ship; `git log` has them.

Statuses: **open work** = someone must do it · **recorded** = a known limit, deliberately not fixed ·
**accepted** = decided, will not change here.

---

## 1. The desktop has not published its half of the schema fingerprint — **open work, owner decision**

The cross-repo drift check splits in two, and only one half runs.

* **Green, in this repo:** `scripts/check-schema-drift.sh` compares `schema-sha.txt` against fulcrum's own shipped
  DDL, and `the_pinned_schema_fingerprint_matches_the_shipped_file` (`fulcrum/tests/db.rs`) reddens `cargo test`
  the moment a daemon DDL change lands without its pin. So `schema-drift.yml` is legitimately green — which is
  why its `--self-test` step runs first.
* **Not running anywhere:** the cross-repo comparison. It is designed to run **in the desktop's CI** — the same
  comparator, fed the desktop's live DDL via `--input` and fulcrum's pin from the rev the desktop pins. Fulcrum
  deliberately never fetches the desktop's DDL at a guessed rev: comparing a pin against a rev no build ships
  together is worse than refusing.

So "a change to the desktop's DDL makes some check fail" is met by a mechanism whose other half does not exist. The
half is **absent, not refusing** — an earlier version of this bullet explained the absence by naming the desktop's pin
(`git ls-tree 78e5954` shows no `fulcrum` member, so there was nothing to fingerprint and the desktop leg had to exit
2). That was true of that rev. The boundary is ours, not the desktop's: the `fulcrum/` member appears at `38a9986` and
`schema-sha.txt` at `26d7648`, so any ref at or after `26d7648` publishes a fingerprint, and for those the only thing
missing is the step. Re-measure before repeating this bullet: `ls <desktop>/.github/workflows/`.

Needed, in `archimedes-codes/archimedes` (renamed from `archimedes-codes/desktop`; the old name still redirects, so a
stale reference there resolves and reads as current): the desktop emits a fingerprint over its `sessions` /
`native_messages` DDL in the same canonical form (per-table column **name + type + order**, plus index lines — its
`SPACE_COLUMNS` is a 4-name name-only subset, so a naive name comparison would false-positive), and it wires the
comparator step into its own CI, fed its live DDL via `--input`.

**Do not "fix" the guard to pass.** A comparison that succeeds because the other side said nothing is the failure
class this repo refuses elsewhere. `README.md`'s `## Schema-shape contract with the desktop` section states the
degraded condition as shipped behaviour — keep it honest when the desktop half lands.

## 2. "A peer of a different uid is rejected" is still not executed by CI — **open work**

`foreign_uid_peer_is_rejected_under_root` (`fulcrum/tests/transport.rs`) is `#[ignore]`d: only root can drop to a
different uid and still dial. It deliberately widens the socket to 0777 / dir 0755 so the **filesystem** cannot be
what makes it pass — against the real 0600 mode a foreign peer is refused by the inode and the test would pass
with the credential check deleted. Its child entry point is `#[ignore]`d for the same reason (2 of the suite's
3 ignored tests are these two). The third ignored test is pre-existing.

So the unix peer check is structured, reasoned, and unit-tested on the uid *comparison* — but **the rejection
itself has never been executed on any runner.** Options: a CI job running that one test under a non-root uid via
`setpriv`/`runuser` (needs a uid to drop *to*, not root), or a namespace-based test. Do not delete the tests; they
are the spec for the behaviour.

## 3. Windows peer credentials are a documented limitation, not enforcement — **recorded**

`windows` / `windows-sys` are deliberately absent from `fulcrum/Cargo.toml` and its comment forbids adding them,
so `GetNamedPipeClientProcessId` has no crate route. `transport/windows.rs` carries a capabilities table saying
what the platform can and cannot prove; issue #4's criterion is **met on unix only**. Revisit only if that ban is
itself revisited, or if T13's wire auth makes the question moot.

## 4. The census's enforcement is uneven across platforms — **recorded, and partly unproven**

| Leg | Enforcement | What is actually proven |
| --- | --- | --- |
| ubuntu | enforced — TERM→KILL | the live plant, the kill, the marker attribution, the lineage skip, the zombie path |
| macOS | enforced on unix | the **live census runs clean**; the plant/kill/lineage/zombie cases **skip** (no `setsid`, no `/proc`) |
| windows | **advisory** — reports, kills nothing | `tasklist` enumeration reads the table and reports clean; attribution is impossible |

Windows cannot attribute a process to *this* run: there is no bash-accessible way to read another process's
environment, and a parent-PID tree bottoms out in the runner agent, which also hosts the developer's own
`fulcrum.exe`. The script says so in its header, its output, and its leak line.

Two holes a reviewer flagged and neither is closed:

* **The `/FI` filter has no positive witness.** A clean Windows leg cannot distinguish "filter honoured, nothing
  matched" from "filter dropped, nothing matched" — both print `clean: … 0 candidate(s)`. Ubuntu proves the shape
  with a live plant; Windows needs the equivalent before the `UNVERIFIED` note in the script header can be retired.
* **`--allow-skips` reason strings are wrong on the Windows leg.** The CI step passes
  `--allow-skips "setsid absent…"` but the script's own NOTEs report the real cause as `ps -eo …` exiting 1 under
  cygwin, and name cases I and M while four (I, L, R, M) actually skipped. The flag's output correctly warns
  `tolerated is NOT proven`, so the guard is honest — but the audit trail it exists to provide is misleading.
  The reason should be per-case, not per-step.

## 5. ~~macOS has no local compile proof~~ — **SUPERSEDED; this claim was false**

The staging note this file replaced said macOS had no compile proof and that the BSD `getpeereid` leg had "never
been compiled by anything except CI's macOS leg". Both were wrong, and the macOS leg corrected them:

* `cargo fmt`, `cargo clippy --all-targets` and `cargo test` all pass on `macos-latest` — the daemon suite
  **executes** there, it does not merely compile.
* `unix_specific::same_uid_peer_is_accepted` passes on macOS, which means the **`getpeereid` arm ran**, not just
  built. So do not treat that leg as unexercised.

What remains true: `aarch64-apple-darwin` cannot be checked **locally** — `ring`'s C build needs a macOS SDK and
the host `cc` has none. `AGENTS.md` names `cargo check --target x86_64-pc-windows-gnu --all-targets` as the only
local cross-target proof and offers no macOS equivalent. The gap is local-only; CI is the macOS witness.

## 6. T4's writer can still be parked by a peer that neither reads nor closes — **accepted, pre-existing**

Backpressure is the design: `send_frame` has no timeout, deliberately. A peer that stops reading without closing
parks the writer exactly as it parked the single pre-split task, and the only reaper is the connection ending.
What the split added is the mechanical backstop: `serve_client` holds the writer's `JoinHandle` and `abort()`s it
after `reader_loop` returns, so a parked writer cannot outlive `serve_client`. Do not "fix" the parking; it is the
same decision `CLIENT_QUEUE_CAPACITY` encodes.

## 7. The `dropped` epilogue can under-report on the aborted path — **accepted, one-line imprecision**

`abort()` drops a task's future without polling it, so the writer's own post-loop epilogue never runs; the
"how many frames you fell behind" line was moved to `serve_client` for that reason. Because abort is a request
rather than an instant kill, that count can be short by whatever the writer dropped in the final microseconds.
The `gap` frames and their numbers are unaffected — only the log line.

## 8. `cargo doc` warns and is not in the validation bar — **recorded; NOT a failure**

`cargo doc --no-deps --workspace` exits **0** with `private_intra_doc_links` warnings (3 in `fulcrum`, 39 in
`fulcrum-core`). Warn-by-default lint, no errors, nothing in the validation bar runs `cargo doc`.

Attribution, measured against the pre-PR commit rather than guessed: pre-PR `main` had **4** in `fulcrum`
(`db/mod.rs:281`, `serve.rs:112`, `transport/frame.rs:116`, `transport/unix.rs:83`). The branch fixed
`serve.rs:112` and introduced one — a public doc on `schema_shape_fingerprint` linking to `schema_shape_sections`
after the latter was narrowed to `pub(crate)`. That link is now prose rather than `[...]`, so `fulcrum` sits at
**3**, one better than `main`. `db/mod.rs`, `frame.rs` and `unix.rs` are all pre-existing, only their line numbers
moved. If `cargo doc` ever joins CI, decide that deliberately rather than as a side effect.

## 9. Invariants that are comment-guarded, not compiler-guarded — **recorded**

* **`Connection::send_frame` must not come back.** Deleted in T4b on purpose: keeping it beside `split_writer`
  would let one caller hold a `Connection` and a `ConnectionWriter` and reintroduce two writers over `pending` —
  issue #3's exact hazard. `a_second_split_writer_panics` is the runtime pin. Note
  `a_shared_writer_under_a_mutex_never_interleaves_frames` does **not** guard this: it holds its guard across the
  whole send, so it stays green under exactly the serialising design it was documented to catch. Its doc states
  which shapes it reddens and which it cannot see.
* **`RELAY_ARRIVAL_BUDGET` must stay under `ATTACH_READY_CEILING` minus the other gate budgets.** Compiler-guarded
  by a `const _: () = assert!(…)` in `tests/serve.rs`. The direct `Duration + Duration < Duration` form does **not**
  compile in const context on the pinned toolchain — the `.as_millis()` form is required.

## 10. Platform facts that cost a debugging cycle each

* **A `std::sync::Mutex` guard held across an `.await` inside `tokio::join!` deadlocks — and `multi_thread` does
  not save you.** `join!` polls both futures on the ONE thread of the task, so a task parked mid-`await` while
  owning the lock blocks the other task's *blocking* `lock()` on that same thread, and that thread is the only
  thing that could resume the first. This hung the Windows CI leg for 3h35m while passing on Linux — small frames
  and a draining peer make it a race, not a fix. `tokio::sync::Mutex` is correct wherever a guard outlives an
  `.await`, because a parked task yields a pending future instead of consuming the thread. **A real runner found
  this; wine reproduced it; Linux never did.**
* **BSD/macOS `ps` prints argv and the entire environment on ONE line.** Any parser that takes
  `BASH_REMATCH[1]` from a `NAME=VALUE` regex gets the *first* variable and silently misses everything later —
  which is how the census "proved" macOS was SIP-blocking a read that had actually succeeded (5443 bytes, marker
  present). Linux's `/proc/<pid>/environ` is NUL-split, one variable per line, so it never shows this. Iterate to
  exhaustion; and when stripping a matched prefix use `${rest#*"$match"}`, never `${rest#"$match"}` — the latter
  strips only a prefix, the match is mid-line, and the loop never terminates.
* **`ps eww -eo` (full table) exits 1 on macOS** — BSD `e` means "all users", not "environment"; `-E` is the env
  flag there and Linux rejects it as `unsupported SysV option`. The census's *per-pid* `ps eww -o pid=,args= -p N`
  form works on both. Do not "fix" the invocation.
* **`setsid` is absent on macOS and on Git-for-Windows bash; `pgrep` is absent on Git-Bash; cygwin `ps -eo` exits
  1.** Any self-test case that plants a process must skip with a reason on those legs, not fail and not silently
  pass.
* **`pgrep -x` cannot match a name longer than 15 characters** — it prints a warning and **exits 1 regardless of
  what is running**. `fake_worker_core` (16) and `fake_mcp_stdio_core` (19) are both invisible to it; the census
  enumerates `ps -eo pid=,comm=,args=` and matches the full basename of the first `args=` element.
* **`ps` renders a zombie as `[fake_worker_cor] <defunct>`** — truncated to 15, so an exact-basename rule can
  never fire on one; the census attributes a zombie by its **parent's** marker instead of its name.
* **`env!("CARGO_MANIFEST_DIR")` is baked at compile time.** A `target/` built under a since-moved checkout
  produced 19 red `boundary_pins` / `childproc` / `source_lints` tests citing a path that no longer existed. The
  fix was one recompile (`touch src/lib.rs`), not a diagnosis.
* **wine (11.20 Staging) is installed on this machine** and reproduces Windows test behaviour faithfully enough
  to have found the deadlock above. Use it as a repro, never as proof: under wine `tasklist` prints nothing, which
  produced a `worker_mode` failure that does not occur on a real Windows runner.

## 11. Guard design rules this feature re-established — **recorded**

* **A behaviour-selecting environment variable on a guard's live path is a vulnerability.** The census honoured
  `CENSUS_MODE=fixture` on the live path, so pointing it at a directory with no process table printed
  `clean: the process table was read` with exit 0 — a guard certifying a table it never read. Harness seams now
  require `--self-test`, and `--allow-skips` was deliberately made a **flag** rather than an env var so the CI log
  always shows which leg tolerated what.
* **A skipped case must never be able to print "all cases behaved."** Skips are ledgered, named, summarised, and
  fatal under `CI=true`; a platform-impossible skip is tolerated only by an explicit per-leg flag with a reason.
  The ubuntu leg runs **without** that flag — that is where the guard's teeth are proven.
* **An assertion about a process's death is meaningless unless its existence was observed first.** "The census
  killed the planted process" passed on macOS for a process that was never planted, because `ps -p <pid>` fails
  identically for a dead pid and a never-born one.
* **`timeout-minutes` on every CI job.** Nothing set it, so the hung Windows leg ran 3h35m against GitHub's
  360-minute default before a human cancelled it. Now 25 min on CI, 10 on schema-drift.

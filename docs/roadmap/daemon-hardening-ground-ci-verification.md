---
title: "daemon-hardening — grounding report: ci-verification"
scope: CI process census (T1, issue #5) and the CI-script/workflow half of cross-repo schema drift detection (T2, issue #6)
date: 2026-11-06
---

# Grounding report — `ci-verification` area of `daemon-hardening`

This report grounds two tasks:

- **T1 (issue #5) — CI process census.** A script that proves no `fulcrum`-family
  process from THIS CI run was lost track of, wired into `.github/workflows/ci.yml`
  after `cargo test` on all three OS legs. The hard requirement from the issue: it
  must distinguish a process from THIS run from an unrelated `fulcrum` a developer
  left running on a self-hosted runner, or it false-positives.
- **T2 (issue #6) — cross-repo schema drift detection, CI half.** The `scripts/*.sh`
  conventions a new guard script must follow, and the shape of a new workflow file.

Everything below is read from the tree, with file:line references, so a planner can
verify each claim. Nothing in this area exists yet: there is no stray-process check,
no env-var run scoping, and no second workflow file anywhere in the repo.

---

## 1. The house guard-script template: `scripts/check-rquickjs-allocator.sh`

This is the ONE existing `scripts/*.sh` in the repo, and it is named by `AGENTS.md`'s
validation bar (`bash scripts/check-rquickjs-allocator.sh` and `+ --self-test`). Its
actual conventions, line by line:

- **Shebang: bash, not sh** — `#!/usr/bin/env bash` (line 1), and it is invoked as
  `bash scripts/check-rquickjs-allocator.sh` in `ci.yml` (lines 81, 84), never as
  `./script` — so the exec bit on the file is cosmetic for CI. **It is not Windows
  native**: CI runs it under the `bash` that `actions/checkout`'s runner guarantees on
  all three legs (`windows-latest` runs `bash` from Git-for-Windows). A new script
  inherits the same pattern: bash-only features are fine, but only through the
  guaranteed `bash` shell.
- **`set -euo pipefail`** (line 53). One strictness line, before anything else.
- **Usage line** — `usage()` prints `usage: $(basename "$0") [repo-root] [--input FILE] [--self-test]`
  (lines 55-58). Three option shapes exist and any new script's flag parsing should
  copy their handling:
  1. `--self-test` (lines 303-307): a plain boolean flag. The template's comment
     (around line 137): "The guard's whole job is to go red on a line shape that is
     NOT in the tree today. That is exactly the condition a live run cannot
     demonstrate, so the fixtures carry a captured [/ planted] dump. Each case
     re-invokes THIS script ... — a second copy of the matching logic here would
     prove nothing about the one that runs in CI."
  2. `--input FILE` (lines 288-302): option-with-value, which the template says
     explicitly is **an OPTION and not a positional**: "an empty or missing value
     must be an error rather than 'read stdin', which would hang a CI step waiting
     on a file that was never passed." Supports both `--input FILE` and `--input=FILE`.
  3. `repo-root` positional (default `.`, lines 318-336): validated to be a
     directory, then `Cargo.toml` checked inside it.
- **What "prove it has teeth" means mechanically** — the `--self-test` mode
  (`run_self_test`, lines 164-262) demonstrates that the guard can still fail, by
  **re-invoking the same script** against planted fixtures via `--input`, and asserting
  exit codes with `expect_exit` (lines 148-163). The `expect_exit` idiom is
  load-bearing and the template records it as the repo's own (line 151):
  `local got=0 output; output="$("$@" 2>&1)" || got=$?` — and NEVER
  `if ! output=$(...)`, because under `if !` the `!` inverts the status so `$?` inside
  the then-branch reads 0, producing a false FAIL.
- **The vacuity floor** — the self-test includes a fixture whose graph "resolved
  nothing"; `run_check` refuses to report clean when the extraction never mentions
  rquickjs (lines 74-95): "reporting 'clean' here would be a guard answering a
  question it was not asked". Any new guard (a stray-process census included) needs
  its analogue: the census must be able to prove it was actually looking.
- **False-positive discipline** — the pattern is anchored (crate name + ` feature "` +
  exact feature name, lines 64-73), with a fixture that proves `allocator-api2`'s
  legitimate `alloc` text does not trip it (lines 227-240, `allocator-api2-only.txt`);
  self-test name: "allocator-api2 alone does NOT false-positive".
- **The vacuous-form witness** — a fixture proving that the *wrong* command form
  (the `-p rquickjs` subtree) would MISS the poison, and is therefore banned
  (lines 242-259, `subtree-form.txt`; expect_exit name "the `-p` subtree form would
  MISS the poison (so we do not use it)").
- **Run locations**: self-test canonicalises itself via
  `cd "$(dirname "${BASH_SOURCE[0]}")" && pwd` so it works from any cwd (line 170);
  the live path `cd "$workspace"` into a repo-rooted `cargo tree` (line 337). A new
  script that must resolve repo-relative paths uses `BASH_SOURCE[0]`, never `$PWD`.
- **`--self-test` exit contract**: 0 all cases behaved, 1 any fixture failed — and
  `ci.yml` runs the self-test for the allocator guard as its OWN step, two steps
  before the guard itself ("Prove the rquickjs allocator guard can fail" then
  "Check the rquickjs build graph", ci.yml lines 80-84). A new guard wired into CI
  inherits the expectation of a paired `--self-test` step.
- **Pass/fail reporting**: pass prints a human sentence to stdout ("rquickjs build
  graph is clean: ..."); failures print `error: ...` blocks to stderr with the
  offending lines indented and remediation hints, and exit 1. `run_check` returns 2
  for "cannot be run honestly" (unreadable dump, `cargo` missing, tree did not
  resolve) — which is also a red CI step. Anything else is cargo's own failure
  (lines 47-52 exit-code contract).

The template is **not** portable across the scripts directory generically: helpers
like `expect_exit` are defined inside the file rather than in a shared shell library,
because this repo has exactly one script. T1's script is only the second; the
convention to copy is the in-file idiom, not to invent a sourced library.

---

## 2. `.github/workflows/ci.yml` — exact structure, ordering, and the two binding constraints

Full step order of the single `lint-test` job (ci.yml, one job, runs on all three OSes):

`on:` — `push` on `branches: [main, feature/*]` (with the comment: "`feature/*`, not
a named branch: the merge bar is a per-branch promise, and a pinned branch name
silently stops making it") and `pull_request:` (lines 3-8). Everything below runs on
every PR and every push to those branches.

Job `lint-test`, `runs-on: ${{ matrix.os }}`, `strategy.fail-fast: false` with
`matrix.os: [ubuntu-latest, windows-latest, macos-latest]` (lines 10-23).
The all-OS choice is justified in the header comment: deliberate
`#[cfg(unix)]` / `#[cfg(target_os = "linux")]` process hygiene that only a Windows
and a macOS compile proves; both of this repo's Windows-only defects so far
(a hand-escaped JSON fixture path; `Path::is_file()` not appending `EXE_SUFFIX`)
were invisible on Linux and caught on the Windows leg (lines 13-18).

Steps, in order:

1. **Checkout** — `actions/checkout@v4` (lines 24-25).
2. **Install the pinned Rust toolchain** — `shell: bash`; no version named; installs
   via repo-root `rust-toolchain.toml`, `rustup default "$(rustup show active-toolchain | cut -d' ' -f1)"`,
   then `rustc --version` (lines 27-38).
3. **Cache the Rust dependencies** — `Swatinem/rust-cache@v2` with `workspaces: .`
   (lines 39-49).
4. **cargo fmt** — `cargo fmt --all -- --check` (lines 50-51).
5. **cargo clippy** — `cargo clippy --all-targets -- -D warnings` (lines 53-58).
   Comment: "`--all-targets` is the gate; without it the integration tests are never
   linted. Runs on all three OSes, and that matters: an import that is used only
   inside `#[cfg(unix)]` is an unused-import warning on Windows, which `-D warnings`
   turns into a build failure here."
6. **⚠ the no-build-step constraint, verbatim** (lines 60-69): the step-slot between
   clippy and `cargo test` is currently a comment block, quoted in full:

   > ⚠ NO `cargo build --all-targets` step before the tests, and do not add one. Cargo
   > materialises a package's binaries at the top of `target/debug/` only when it
   > builds an integration-test target, and 66 of this crate's tests spawn the
   > fixture binaries BY PATH (a lib's unit tests get no `CARGO_BIN_EXE_*`).
   > `tests/fixture_bins.rs` is the target that forces it. Measured on a target dir
   > that had never existed: a bare `cargo test` gives 1397 passed / 0 failed, so no
   > build step is needed — and adding one would re-arm the exact trap it appears to
   > guard, because it would keep the suite green after someone deleted
   > `tests/fixture_bins.rs`. The failure is meant to be loud.

7. **cargo test** — `run: cargo test` (lines 71-72).
8. **Prove the rquickjs allocator guard can fail** — `bash scripts/check-rquickjs-allocator.sh --self-test` (lines 79-81).
9. **Check the rquickjs build graph** — `bash scripts/check-rquickjs-allocator.sh` (lines 83-84).

There are exactly **two** `⚠`-flagged constraint comments in `ci.yml`: the
no-build-step one at line 60 (quoted above) and the toolchain-install comment at
lines 27-31 ("No toolchain version named here on purpose: the repo-root
`rust-toolchain.toml` is the pin, and naming one in this step fights it ...").
The dispatch's "two ⚠ comments" framing maps to: the no-build step (line 60) and the
toolchain-pin rationale (reproducing the pinned toolchain is a binding rule for any
workflow file T2 creates — no version strings; install from the repo file).

**Where a new step goes relative to `cargo test`:** the census must run AFTER
`cargo test` on every leg (that is when leaks would materialise), so *after step 7*,
by the two-step guard pattern: a `--self-test` step then the check itself, mirroring
steps 8-9. Its placement is load-bearing in exactly the same way the ordering of
steps 8-9 is: the self-test must run before the live check so that a broken guard
reddens, not silently passes.

**The second workflow.** The repo has exactly one workflow file (`.github/workflows/ci.yml`).
The only conventions a new file must honour: `on:` restricted to `main` / `feature/*`
pushes plus `pull_request` (this is the repo's stated merge bar: "the merge bar is a
per-branch promise"); the pinned toolchain installed from `rust-toolchain.toml` with
no version named; `actions/checkout@v4`; bash as the explicit `shell:` for anything
non-rust; and `fail-fast: false` so one failing leg does not kill the others.

---

## 3. `fulcrum/tests/proctable/mod.rs` — the existing per-pid probe

One shared module, included by both test binaries that ask "is this pid still
running?" — `serve.rs` declares `mod proctable;` (serve.rs line 36), `worker_mode.rs`
uses the discovery form `#[path = "proctable/mod.rs"] mod proctable;` (worker_mode.rs
lines 31-33). Top-level API:

```rust
pub enum Probe { Present, Absent, Unreadable(String) }                       // mod.rs:25-37

pub fn parse_tasklist_filtered(output: &str, pid: u32) -> Probe               // mod.rs:143
pub fn parse_tasklist_streams(stdout: &str, stderr: &str, pid: u32) -> Probe   // mod.rs:218
#[cfg(windows)]
pub fn in_process_table(pid: u32) -> bool                                     // mod.rs:336
#[cfg(windows)]
fn assert_probe_works(queried: u32)                                           // mod.rs:400
```

Platform behaviour:

- **Windows**: `in_process_table` runs
  `tasklist /FI "PID eq <pid>" /NH /FO CSV`, feeds BOTH streams to the pure
  `parse_tasklist_streams`, and — critically for T1 — `Probe::Absent` first runs
  `assert_probe_works(pid)` (a `Once`-guarded control lookup that proves `tasklist`
  can see a LIVE process: the test binary's own pid), and sits on the guard because
  the old code falsely reported a live process as gone. The parsing is pure and
  fixture-tested on every platform so the Windows arm is not an unverifiable blob,
  and *"unparseable is a PANIC, never 'not alive'"* (module header, lines 29-44):
  a probe that answers "not alive" when it could not read the table turns leak
  guards into liars.
- **Unix**: nothing here — the `kill(pid, 0)` split lives in the two test files'
  thin wrappers (see below), because "alive" for a zombie is what `pid_is_alive`
  wants (`kill(pid,0) == 0`) and "not ESRCH" is what `pid_is_reaped` wants
  (`kill(pid,0) != 0`). Those wrappers deliberately do NOT share this module.

**Reusability by a whole-process-table census (T1):** partial. The pure parsers
are per-pid; a T1 census on Windows is defined by the issue as
`tasklist /FI "IMAGENAME eq fulcrum.exe"`, which `parse_tasklist_streams` cannot
consume because the `tasklist` call carries the `/FI "PID eq …"` filter, not an
image filter — a row's PID column still parses, but `classify_stream`'s pass-2
classification of a *missing* match turns "a different pid's row" into
`Unreadable`, and the "the /FI filter did not filter" verdict depends on the
`/FI PID eq` shape. What IS reusable: the raw techniques the module documents —
the CSV field-unquoting `split_csv` and `pid_column` helpers (private, lines
~300-330), the `INFO:`-marker-not-English matching rule, the quote-every-field
lessons ("both matched `,` + pid + `,` against `tasklist /FO CSV` output ... a live
pid reads `,"8620",`") at lines 3-15, and the fail-UP direction:
"unparseable is a PANIC, never 'not alive'". If T1 lands its Windows probe in Rust
inside a test rather than in the shell script, extending this module (or re-homing
`split_csv`/`pid_column` as `pub(crate)`) is the path; the issue itself specifies a
shell census using `tasklist /FI "IMAGENAME eq ..."`, which keeps the census outside
the Rust tests entirely.

---

## 4. Spawn sites and the ACTUAL image names a census must match

### The `fulcrum` daemon binary — the census's primary target

Spawns of the daemon (`fulcrum serve`), all through `CARGO_BIN_EXE_fulcrum`:

- `fulcrum/tests/serve.rs:36,57` — `mod proctable;`, `const BIN: &str = env!("CARGO_BIN_EXE_fulcrum");`
- `fulcrum/tests/serve.rs:236-265` — `Daemon::start()`, the 17-instance workhorse:
  `Command::new(BIN)` with `serve`, `--socket/--pipe`, `--db`, `--worker-exe`,
  `stdin(Stdio::null())`, `stdout(Stdio::null())`, `stderr(Stdio::inherit())`.
- `fulcrum/tests/serve.rs:1371` — `a_second_daemon_on_the_same_endpoint_exits_non_zero_and_leaves_the_first_running`:
  `Command::new(BIN)` for the competing instance.
- `fulcrum/tests/serve.rs:1465` — `a_daemon_that_cannot_open_its_database_leaves_no_socket_behind`.
- `fulcrum/tests/worker_mode.rs:34,100,582-583,1075-1084` — `const BIN`, the
  `Spawned` harness spawning `fulcrum serve` for the CLI tests; plus
  `worker_mode.rs:1084-1088` the leak assertion `pid_is_reaped(daemon_pid)` — the
  per-test proof that "asserts its own pid is gone".
- `fulcrum/tests/serve.rs:2024` — `pgrep -P` for daemon children (unix only);
  `serve.rs:2291` — `taskkill /PID … /F` (windows only).

Two facts a census must know:

1. **Respawn risk on failure**: `serve.rs:171` runs `std::env::var("CARGO")`
   (default `"cargo"`) to build the `fake_worker_core` fixture when absent — the
   re-spawned processes are the engine's fixture bin, not the daemon, and a census
   that also matches `fake_worker_core` would double-count: see image names below.
2. **The daemon never exits on its own**: `fulcrum serve` has no shutdown frame in
   T9, `run` never returns (`serve.rs:81,88` headers; worker_mode.rs:54-60), so ANY
   `fulcrum` left running after the tests is by construction "lost track of" — the
   census's epsilon on unix (`pgrep -f` matches the daemon) is not a tenth-symptom
   but the whole class of defect (34 live daemons, issue #5's context, some 5.5
   hours old, worker_mode.rs:49-53).

### The fixture binaries — what image names actually exist

Engine member's bins (root `Cargo.toml`, `[[bin]]` blocks, lines designated in the
file as S8b.1):

```toml
[[bin]]
name = "fake_worker_core"
path = "src/bin/fake_worker_core.rs"

[[bin]]
name = "fake_mcp_stdio_core"
path = "src/bin/fake_mcp_stdio_core.rs"
```

Daemon member's bin (`fulcrum/Cargo.toml`): `[[bin]] name = "fulcrum"`.

So the complete set of process image names a THIS-RUN census must consider:

| image name (unix) | Windows (`+ EXE_SUFFIX`) | spawned by | what it must prove absent after tests |
| --- | --- | --- | --- |
| `fulcrum` | `fulcrum.exe` | serve.rs ×17 + worker_mode.rs ×3 (all `CARGO_BIN_EXE_fulcrum`) | THE target — daemon leaks |
| `fake_worker_core` | `fake_worker_core.exe` | spawned *by the daemon* inside serve.rs tests (`--worker-exe`) — the fixture worker a session runs in | ok if reaped with the daemon, but a census matching it catches a leaked worker; the proctable tests already pin this exact CSV row: `"fake_worker_core.exe","8620",...` |
| `fake_mcp_stdio_core` | `fake_mcp_stdio_core.exe` | NOT spawned by the daemon's tests — spawned by `tests/fixture_bins.rs` and the engine's `src/mcp/*` / `tools/exec` tests | a census that matches it would catch an ENGINE-suite leak, but those tests are not the issue's target |

Careful note: `tests/fixture_bins.rs` (line 42 in a hardcoded convention, and
serve.rs:152, `worker_fixture()`) adds `std::env::consts::EXE_SUFFIX` itself:
`format!("{WORKER_FIXTURE}{}", std::env::consts::EXE_SUFFIX)` — so on Windows the
worker image is `fake_worker_core.exe` (cargo's output filename IS the suffixed
name, per `src/test_support.rs`'s `exe_file_name`, `fixture_bin`, and the
"Path::is_file() does not append EXE_SUFFIX; CreateProcessW does" AGENTS.md bullet).
A shell census on Windows must therefore probe `fulcrum.exe` (+ optionally
`fake_worker_core.exe`), which is precisely the issue's
`tasklist /FI "IMAGENAME eq fulcrum.exe"`.

**The name-collision constraint (the `_core` rule):** the bin name `fulcrum` is
forbidden from ever being `fake_worker_core` / `fake_mcp_stdio_core` (fulcrum/Cargo.toml
comment: "The bin name is `fulcrum`. It MUST NEVER equal `fake_worker_core` or
`fake_mcp_stdio_core`: workspace members share one `target/debug/`, and cargo only
WARNS on a bin-name collision — last writer wins"). And the `_core` suffix on the
engine's fixtures is load-bearing: the desktop builds the same bodies under
UNSUFFIXED names (`fake_worker` / `fake_mcp_stdio`), two members with the same bin
name collide on the output filename (cargo#6313), and the desktop keeps its
unsuffixed names forever because `CARGO_BIN_EXE_<name>` is package-local. **The
daemon's own binary (`fulcrum`) has NO `_core` suffix and must never get one** — it
is not a fixture; if a T2 schema-drift workflow shadows the unsuffixed name
mechanism, the `_core` suffix rule still applies to the fixture bins only.

**Pid-image caveat for a whole-process census:** a unix `pgrep -f fulcrum` matches
ANY command line containing `fulcrum` — including the test binaries (whose
`CARGO_BIN_EXE_fulcrum` path is `target/debug/deps/...` containing `fulcrum`),
`cargo`'s own child processes, and (on a self-hosted/developer machine) a real
`fulcrum serve`. The exact-match-by-image unix probe is `pgrep -x fulcrum`
(exact match on the process name, which on Linux is the comm(15) — short names are
safe: `fulcrum`, `fake_worker_cor` — but a name ≥16 chars is truncated by the
kernel's 15-char comm limit: `fake_mcp_stdio_core` → `fake_mcp_stdio_`; that one is
only a candidate image, not the issue's target). The issue names unix `pgrep`
generically; the planner must pin the exact-match form to avoid self-matching, and
that tension is precisely T1's core.

---

## 5. Run-scoping: is there ANY existing run-scoping lesson in the repo?

Searched `src/`, `fulcrum/`, `tests/`, `scripts/`, `.github/`, `Cargo.toml`,
`AGENTS.md` for `GITHUB_`, `RUNNER_`, `self-hosted`, `run id`, `run_id`, and
env-var reads:

- **No code reads `GITHUB_*` or `RUNNER_*` anywhere** — not in `ci.yml` (the only
  dynamic thing is `${{ matrix.os }}`), not in `AGENTS.md`, not in any `.rs`. The
  repo has zero precedent for CI-context env-var scoping.
- **No self-hosted-runner precedent** — all `runs-on` values are
  `ubuntu-latest` / `windows-latest` / `macos-latest` (ci.yml:22); `self-hosted`
  appears nowhere in the tree. The false-positive hazard the issue names (a
  developer's unrelated `fulcrum` on a self-hosted runner) is a THREAT arguably
  unrealised in CI so far — there is nothing to point to, and the issue is asking
  the area to create the first.
- **The one thing the repo DOES say about run scoping is negative, and it is the
  strongest lesson in the entire tree.** `worker_mode.rs:61-88`, header "What was
  tried first, and why it is NOT here": a process-wide registry
  (`static LIVE_CHILDREN`) plus an end-of-test sweep that killed anything unreaped
  **killed five OTHER tests' children** — cargo runs tests in parallel, the registry
  — i.e. any shared "all processes I have seen" state — was shared across concurrent
  tests, five tests failed with "the daemon never listened", and the guard's own
  `assert!` fired inside a `Drop` during unwinding: `panic in a destructor during
  cleanup` → SIGABRT. Conclusion recorded in the header (lines 87-88): "A leak
  detector that kills its neighbours' daemons and then aborts the run is worse than
  no leak detector."
- The fix that landed instead is the **per-test ownership / per-pid self-assert**
  model: `Spawned::drop` kills and reaps unconditionally (worker_mode.rs:562-575,
  `impl Drop for Spawned` lines 743-759), `Daemon`'s `Drop` does the same
  (serve.rs:345-351), `detach()` is deleted and a source-scan guard
  (`no_opt_out_of_reaping_can_come_back_in_any_spawn_site`, worker_mode.rs:294-343)
  makes it impossible to re-add, and each daemon test asserts
  **its own** pid is reaped (`pid_is_reaped(daemon_pid)`, worker_mode.rs:1084-1088).
  This is the "each test now reaps its own children and asserts its own pid is gone"
  state the issue's T1 context describes — and T1 is precisely the hole left: nobody
  asks whether a `fulcrum` is left over that no test claims, because per-test
  ownership deliberately has no shared state to sweep.
- **`/tmp` / `TempDir` usage**: `tempfile::tempdir()` in serve.rs (219, 231, 1458)
  and worker_mode.rs (998, 1075); `std::env::temp_dir()` in serve.rs:794 and
  worker_mode.rs:970 (the `fulcrum-argv-shape-check` dir). Two other guard scripts
  are only MENTIONED by name in the tree — `check-temp-dir-census.sh` (which "counts
  `TempDir`s", serve.rs:135, and is what `src/serve.rs:2427`'s live-reader comment
  exists to catch; "a build cache is not one") and `check-commands.sh` /
  `check-versions.sh` (referenced as the source of the `expect_exit` idiom and the
  `BASH_SOURCE[0]` canonicalisation in check-rquickjs-allocator.sh:151,170) — but all
  three live in the DESKTOP repo, not here. A temp-dir legacy (stale socket/db files
  under `/tmp`) is real but out of scope for T1, which is about live processes.

So for T1's run-scoping decision, the repo contributes: (a) the parallel-child-kill
disaster as the canonical warning against SHARED cross-process state; (b) the
per-pid `pid_is_reaped` self-assert as the model to build on — the census is a
whole-table version of the same per-run question, and it must be scoped so that
legitimate processes (a developer's own `fulcrum`, the engine's fixture bins under
`target/`, another concurrently-running job) can never be confused with THIS run's
leaks; (c) no prior env-var read to lean on — `${GITHUB_*}` would be a NEW
dependency introduced by this area, and whose semantics T1 must define (the issue
names it as the distinguishing mechanism; there is nothing in-repo that already
relies on it).

---

## 6. `AGENTS.md` validation bar and the constraints a planner must not violate

The bar (AGENTS.md, verbatim shapes):

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo check --target x86_64-pc-windows-gnu --all-targets   # the only local Windows proof available
bash scripts/check-rquickjs-allocator.sh                   # + --self-test to prove it has teeth
```

Key items for this area:

- **Available local Windows proof is `cargo check --target x86_64-pc-windows-gnu --all-targets`**
  — the ONLY signal a Windows arm gets locally; proctable's header (mod.rs:44-70)
  warns `#[cfg(windows)]` code is not even type-checked on a Linux build. A T1 shell
  census's Windows half can only be compile- or fixture-proofed locally; run-proof is
  the Windows CI leg.
- **`default-members = [".", "fulcrum"]`** (root Cargo.toml, `[workspace]` section)
  makes bare `cargo test` / `cargo clippy --all-targets` cover BOTH members — that
  is why the 34-daemon leak came from `cargo test` at all, and it is also why a
  census after bare `cargo test` has both members' binaries materialised in one
  shared `target/debug/`.
- **`boundary_pins` "does not see the sibling member"** — `crate_code()` walks
  `{CARGO_MANIFEST_DIR}/src` (src/boundary_pins.rs:166-209), the manifest pin reads
  only `{CARGO_MANIFEST_DIR}/Cargo.toml` (src/boundary_pins.rs:302-328). The daemon
  may import `rusqlite` (intended) and even `tauri` (NOT intended) without this
  repo noticing. T1/T2 touch no `.rs` code, so they cannot trip `boundary_pins`;
  but a planner must not extend `boundary_pins` to police the sibling (explicitly
  forbidden — it would fail on the daemon's legitimate `rusqlite`).
- **The `_core` fixture-name rule** — see section 4: never rename the fixture bins;
  the daemon's own bin must never be named like a fixture; the desktop's unsuffixed
  names are dual obligations.
- **`Cargo.lock`** exists at the workspace root — a T1/T2 area introduces NO cargo
  dependency, so the lockfile is untouched by this area.

---

## `## Will touch`

### T1 (issue #5) — CI process census

- **NEW** `scripts/check-no-stray-processes.sh` — the guard script; follows
  `check-rquickjs-allocator.sh`'s conventions (bash shebang, `set -euo pipefail`,
  `--self-test` flag with planted dummy processes / captured fixture output,
  vacuity floor proving the census was actually looking, exit 0/1/2 contract with
  human pass/fail messages). Implements: unix `pgrep` (exact-match-by-image form,
  section 4's comm(15) notes), windows `tasklist /FI "IMAGENAME eq fulcrum.exe"`,
  and the run-scoping discriminator that separates THIS run from a developer's
  unrelated `fulcrum` (the issue's hard requirement; the only candidate mechanism
  nameable from the repo today is a `GITHUB_*` env-var scope — nothing in the tree
  reads one yet, so the semantics are this script's to define).
- **MODIFIED** `.github/workflows/ci.yml` — one new step pair AFTER `cargo test`
  (ci.yml line 72), mirroring the allocator-guard pair at lines 79-84:
  `bash scripts/check-no-stray-processes.sh --self-test` then
  `bash scripts/check-no-stray-processes.sh`, on all three matrix legs. The no-build
  step rule (ci.yml:60-69) and the order relative to `cargo test` are load-bearing —
  the census must observe the state AFTER the full suite has run.

### T2 (issue #6) — cross-repo schema drift detection (CI half of the area)

- **NEW** `scripts/*.sh` guard over the schema DDL (name left to the planner; the
  repo convention is descriptive lowercase `check-*-*.sh` — only precedent:
  `check-rquickjs-allocator.sh`). Follows the same template: `--self-test` with
  captured-fixture teeth, vacuity floor, exit contract.
- **NEW** `.github/workflows/*.yml` — the repo has exactly one workflow file today;
  a second file must follow: `on:` restricted to `[main, feature/*]` pushes plus
  `pull_request:`; pinned-toolchain install from the repo-root `rust-toolchain.toml`
  with NO version named; `actions/checkout@v4`; `shell: bash` for script steps;
  `fail-fast: false` if a matrix is used. The `db/schema.rs`-side of T2 (the schema
  doc/source the script reads) belongs to another area's report and is NOT modified
  here.

No lockfile, no `.rs`, no `#![cfg]`-bearing file is touched by this area.

## `## Provides`

Existing symbols/mechanisms this area (or other areas) can consume:

- `proctable::in_process_table(pid: u32) -> bool` — `#[cfg(windows)]`, spans
  `tasklist /FI "PID eq <pid>" /NH /FO CSV`, panics on unreadable output,
  `Probe::Absent` gated by the `assert_probe_works` control lookup (proctable/mod.rs:336-396).
- `proctable::parse_tasklist_filtered(output: &str, pid: u32) -> Probe` (mod.rs:143)
  and `proctable::parse_tasklist_streams(stdout: &str, stderr: &str, pid: u32) -> Probe`
  (mod.rs:218) — pure, fixture-tested on every platform; the CSV-unquoting,
  `INFO:`-marker matching, and quote-every-field lessons for any Windows process-table
  work.
- `pid_is_alive(pid: u32) -> bool` (serve.rs:834-855) and `pid_is_reaped(pid: u32) -> bool`
  (worker_mode.rs:104-122) — the per-pid per-platform wrappers (unix `libc::kill(pid,0)`,
  windows via `proctable::in_process_table`); the per-pid "this run's pid is gone"
  proofs T1 generalises to a whole-table form.
- The reaping-opt-out source guard: `no_opt_out_of_reaping_can_come_back_in_any_spawn_site`
  (worker_mode.rs:294), `spawn_sites() -> Vec<std::path::PathBuf>` (worker_mode.rs:199),
  macro `reaping_opt_out_needles!` (worker_mode.rs:127-144) + `NEEDLE_DOC_LIST`.
- Guard-script flag convention (`check-rquickjs-allocator.sh`): `--self-test`,
  `--input FILE`, `repo-root` positional, `expect_exit` idiom
  (`output="$("$@" 2>&1)" || got=$?`, never `if ! …`), vacuity floor returning 2,
  `BASH_SOURCE[0]` canonicalisation, exit 0/1/2 contract.
- CI env precedent: **none currently relied on** — the only dynamic CI values in use
  are `${{ matrix.os }}` and `rustup show`'s active-toolchain. `CARGO` env is read by
  the tests (`std::env::var("CARGO")`, serve.rs:167) and the fixture resolver; any
  `GITHUB_*`/`RUNNER_*` dependency is provided by this task (GitHub defines the
  variables; the repo does not yet consume them).
- Fixture-name consts: `WORKER_FIXTURE: &str = "fake_worker_core"` (serve.rs:60),
  `BIN: &str = env!("CARGO_BIN_EXE_fulcrum")` (serve.rs:57, worker_mode.rs:34),
  `EXE_SUFFIX = std::env::consts::EXE_SUFFIX` (tests/fixture_bins.rs:42).

## `## Needs from other areas`

- **`db/schema.rs`'s schema (T2, other area)** — the drift-check's ground truth: the
  DDL text the guard compares against a remote (desktop) schema. This area's script
  consumes the schema as input; the schema's own producing task is the sibling T2
  `db/schema.rs` half, reported elsewhere. The cross-repo "which remote do we diff
  against" wiring is T2's own decision surface, not provided here.
- **`fulcrum serve`'s runtime (T9 of this same roadmap)** — the daemon whose leaked
  processes T1 counts. Consumed in the form of the `fulcrum` image name and the
  "never exits on its own" property; no API import — the census interrogates the
  process table, which is why a census is possible without touching the daemon.
- **The fixture binaries (`fake_worker_core` / `fake_mcp_stdio_core`, built by
  `tests/fixture_bins.rs`)** — spawned by the daemon inside the serve tests and by
  engine tests; image names the census may optionally match (second-order leaks).
- **`actions/checkout` / runner-provided bash and process tools (`pgrep`,
  `tasklist`)** — CI-step ecosystem that the workflow file and script rely on;
  tasklist's `/FI "IMAGENAME eq"` filter shape is the documented Windows primitive
  the issue already names.
- **`rust-toolchain.toml`** — the channel pin (`1.99.0`) any new workflow file must
  install from rather than naming a version (this area reads it; the file already
  exists).

## `## Constraints a planner must not violate`

1. **No `cargo build --all-targets` step in CI, ever** — ci.yml:60-69, the exact
   `⚠` comment quoted in section 2; the no-build rule is measured (1397/0 cold;
   1397→0 after `tests/fixture_bins.rs` deleted, in the CI sense it stays green).
   A post-test census step must NOT be preceded by a build step.
2. **`tests/fixture_bins.rs` is part of the build contract** — deleting it must stay
   loud; `worker_fixture()`'s isolated `--target-dir` build (serve.rs:108-135) is the
   only permitted fallback and is the model for any "materialise a fixture" logic.
3. **No build/test/check of the Rust graph** — `cargo test` / `cargo clippy ... -D warnings`
   / `cargo fmt --all -- --check` remain the only suite-wide gates; the T1/T2 adds
   are shell guards, and must not run `cargo` for anything except the allocator
   guard's read-only `cargo tree`.
4. **`default-members = [".", "fulcrum"]`** is what makes bare `cargo test` cover
   the daemon — do not "also" add `--workspace` to CI steps, and do not drop the
   `default-members` line.
5. **`boundary_pins` must not be widened to walk the sibling member** — it would
   fail on the daemon's legitimate `rusqlite`; T1/T2 change no `.rs` and must not
   create a reason to loosen the engine's pins.
6. **The `_core` suffix and the bin names are all load-bearing** — never rename
   `fake_worker_core` / `fake_mcp_stdio_core`; never give the daemon a fixture-like
   name; two members share one `target/debug/` and cargo only warns on collisions.
7. **The process-census is a shell guard, and must be honest about what a
   cross-platform census can prove** — `#[cfg(windows)]` arms are not type-checked
   locally (proctable/mod.rs:44-70); the only local Windows proof is
   `cargo check --target x86_64-pc-windows-gnu --all-targets`; the Windows half of a
   census is fixture- or compile-proven locally and run-proven only on
   `windows-latest`.
8. **Do not build shared cross-test process state** (worker_mode.rs:61-88's
   measured disaster — a shared sweep killed five other tests' children and then
   SIGABRT'd in a `Drop` panic); the census's scope is a whole CI JOB's leak-check,
   not a runtime sweep of a test run — and its per-run discriminator must be defined
   (self-hosted/developer processes are the false-positive class the issue names).
9. **`GITHUB_*` / `RUNNER_*` scoping has NO in-repo precedent** — a planner cannot
   claim an existing convention; the semantics are T1's to define and its `--self-test`
   must prove the discriminator actually separates a planted dummy from a real
   process.
10. **`panic = "unwind"` / release profile and toolchain-pin rules** are invariant
    for any new workflow file: install the pinned toolchain from the repo file, never
    name a version; do not add a system-package step that a workflow "needs" beyond
    what the runner provides.

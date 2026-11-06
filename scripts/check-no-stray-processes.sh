#!/usr/bin/env bash
#
# Fail the CI run when a fulcrum daemon or worker outlives the test suite.
#
# ─────────────────────────────────────────────────────────────────────────────
# WHY A CENSUS AND NOT A TEST
# ─────────────────────────────────────────────────────────────────────────────
# Every existing leak guard in this repo is PER-PID and LOCAL: `Spawned::drop`
# kills and reaps its own child, and each daemon test asserts
# `pid_is_reaped(daemon_pid)` for the pid it recorded. Each of those asks "is the
# specific pid I spawned gone?". Nobody asks "are there leftover daemon/worker
# processes from THIS run that nobody is tracking?". A test that crashes before
# recording a pid, or that spawns a grandchild it never tracked, passes every
# existing assertion and still leaks. During T9 development 34 stray
# `fulcrum serve` daemons accumulated on a developer machine, some 5.5 hours old.
# `fulcrum serve` never exits on its own (`serve::run` never returns), so ANY
# `fulcrum` still alive after the suite is by construction a process the run lost.
# This census is the whole-table form of the per-run question, and it must run
# AFTER `cargo test` — that is when leaks materialise.
#
# ─────────────────────────────────────────────────────────────────────────────
# THE RUN-SCOPING DISCRIMINATOR
# (T1 defines this precedent. Nothing else in this repo reads CI context, and this
# script deliberately reads no GITHUB_*/RUNNER_* variable — only its own marker)
# ─────────────────────────────────────────────────────────────────────────────
# The CI job exports `FULCRUM_CI_RUN_MARKER` — a value derived from
# `github.run_id` + `github.run_attempt` — into the environment shared by every
# step of the leg, BEFORE `cargo test` runs. The marker is inherited by every
# process the suite spawns. The spawn paths this was verified against:
#   * `fulcrum/tests/serve.rs` `Daemon::start` (+ the competing-daemon and bad-db
#     `Command::new(BIN)` sites) — no `.env()`/`.envs()`/`env_clear()`;
#   * `fulcrum/tests/worker_mode.rs` `Spawned::args` — same;
#   * daemon→worker: `fulcrum/src/worker_factory.rs` → `WorkerHandle::spawn` →
#     `src/worker/client.rs` `build_command_with` → `childproc::command` (no env
#     mutation) + `EnvPatch::apply` from `hostenv::for_child()`, which is an EMPTY
#     patch under `cargo test` (current_exe is `target/debug/deps/…`, never under
#     `$APPDIR`), and `apply` only ever `env_remove`s/`env`s its OWN keys.
# `env_clear()` exists in the tree only in `hostenv.rs`'s own unit-test fixture and
# in `src/tools/exec/tests/bash_hostenv.rs` — neither on a daemon/worker spawn path.
#
#   A process belongs to THIS run iff its own environment contains the line
#   `FULCRUM_CI_RUN_MARKER=<this run's exact value>`.
#
# The VALUE match (not mere presence) is what makes a locally-exported marker
# useless to a developer's own `fulcrum`, and it is why an unrelated `fulcrum` on a
# self-hosted runner is reported `left-alone` and never killed. The env read is the
# load-bearing proof that a candidate is a daemon/worker of THIS run.
#
# THE ONE MUTATION THAT WOULD SILENTLY BLIND THIS CENSUS: a spawn path that starts
# calling `env_clear()`. A marker-less child is indistinguishable from somebody
# else's process and would be left alone forever. If you add `env_clear()` to a
# daemon or worker spawn path, this guard stops seeing that process — re-grep
# `env_clear` across src/ and fulcrum/ when that area moves.
#
# ─────────────────────────────────────────────────────────────────────────────
# WHY `ps`, AND WHY `pgrep -x` IS NOT USED
# (measured on procps-ng 4.0.6 with kernel TASK_COMM_LEN=16, i.e. comm truncated to
# 15 characters — measured on the machine that wrote this, not hypothesised)
# ─────────────────────────────────────────────────────────────────────────────
# `pgrep -x` matches `comm`, and `comm` is truncated to 15 characters:
#   * `pgrep -x fake_worker_core` (16 chars) prints
#     `pgrep: pattern that searches for process name longer than 15 characters
#     will result in zero matches` and EXITS 1 no matter what is running;
#   * `pgrep -x fake_mcp_stdio_core` (19 chars) does exactly the same;
#   * so a census written from the real image names with `pgrep -x` is SILENTLY
#     BLIND to both fixture workers and reports green forever.
# Matching the TRUNCATED form instead is worse — truncation is a PREFIX COLLISION,
# not a safe cut: `/tmp/fake_worker_core` and `/tmp/fake_worker_corruptor` BOTH
# have `comm=fake_worker_cor` (measured: `pgrep -x fake_worker_cor` returned both
# pids). A census matching the 15-char form would kill an unrelated `…_corruptor`.
# `pgrep -f` is no better: it matches any command line CONTAINING the string, and a
# test-binary path under `target/debug/deps/` contains these names — which is also
# how a `pkill -f` during development can kill the shell that ran it.
#
# THE FIX, AND WHY IT IS EXACT: enumerate with `ps -eo pid=,ppid=,stat=,comm=,args=`
# — whose `args=` column is the UNTRUNCATED argv — and match the FULL BASENAME OF
# THE FIRST ARGV ELEMENT of `args=`. FIRST element, because the daemon's argv ends in
# `--worker-exe /path/to/fake_worker_core` and a basename taken from ANY element
# would classify the daemon's own argument as a second candidate. FULL basename,
# because that restores the exactness `comm` destroyed: `fake_worker_corruptor` is
# rejected, `fake_worker_core` is not. Self-test Case B2 (the 19-char
# `fake_mcp_stdio_core` must be caught) and Case B3 (`fake_worker_corruptor`, same
# 15-char comm, must be left alone) pin both directions of this. DO NOT "simplify"
# this back to `pgrep -x` or to the truncated comm form — those two cases are
# exactly what goes red when you do.
#
# ─────────────────────────────────────────────────────────────────────────────
# IMAGE NAMES THE CENSUS WATCHES
# (full names, matched against ps's untruncated `args=`, NEVER against `comm`)
# ─────────────────────────────────────────────────────────────────────────────
#   unix image            comm (15, truncated)   spawned by
#   fulcrum               fulcrum                serve.rs x17 + worker_mode.rs x3
#   fake_worker_core      fake_worker_cor        the daemon's --worker-exe child
#   fake_mcp_stdio_core   fake_mcp_stdio_        the engine's fixture/MCP tests
# The comm column is documented for readers ONLY; this script never matches on it.
# `fake_mcp_stdio_core` is in the match set but is the census's epsilon, not its
# primary target — the engine's fixture tests spawn it, not the daemon tests — so
# matching it catches an engine-suite leak (second-order) at the price of a larger
# false-positive surface on developer machines. It stays at primary severity; the
# difference is documented, not special-cased.
# The `_core` suffix is load-bearing (workspace members share one target/debug/ and
# cargo only WARNS on a bin-name collision) — never shorten these names.
#
# ─────────────────────────────────────────────────────────────────────────────
# WINDOWS IS ADVISORY — AND IT IS LABELLED ADVISORY IN ITS OWN OUTPUT
# ─────────────────────────────────────────────────────────────────────────────
# No bash-accessible mechanism on Windows can read another process's environment
# (no procfs; the PEB's ProcessParameters->Environment needs a compiled helper with
# process handles, outside this script's file set). A parent-PID-tree discriminator
# (Get-CimInstance Win32_Process) cannot attribute a process to THIS job on a
# self-hosted runner either: a candidate's ancestry bottoms out in the runner agent,
# which also hosts a developer's concurrently-running `fulcrum.exe` — attributing by
# ancestry is a false-positive machine for the exact violation issue #5 names. So on
# Windows EVERY live image match is reported as a leak and NOTHING is ever killed.
# CONSEQUENCE, STATED PLAINLY: issue #5's "fail the run" criterion is ENFORCED ON
# UNIX ONLY. The Windows leg reports and cannot attribute, so on a SELF-HOSTED
# Windows runner a developer's concurrent `fulcrum.exe` could redden this step.
# Accepted because every runner this repo uses today is hosted (a clean image by
# GitHub's contract) and the alternative is no Windows census at all. The seam to
# revisit if a self-hosted Windows runner appears is a compiled env-reading helper.
# The leg runs on the runner's bash, the same shell the existing allocator guard
# runs on. There is NO "is this a native shell" gate: on windows-latest GitHub's
# default shell invokes Git-for-Windows bash, which always sets MSYSTEM, so a gate
# on that would refuse on every leg and the step would report nothing at all.
# UNVERIFIED here (this is a Linux box): whether MSYS mangles the single-slash
# `/FO` / `/FI` flags into Windows paths. `enumerate_windows` exports
# MSYS_NO_PATHCONV=1 and MSYS2_ARG_CONV_EXCL='*' to defuse that, and the fallback
# if the live Windows leg still shows mangling is the double-slash form (`//FO`,
# `//FI`) that proctable's Rust side does not need because Rust does not spawn
# through MSYS. This is the one thing in this script that a first Windows run must
# confirm; if tasklist's output cannot be parsed the leg REFUSES (exit 2) rather
# than report clean.
#
# ─────────────────────────────────────────────────────────────────────────────
# macOS RUN-SCOPING IS UNVERIFIED ON THE MACHINE THAT WROTE THIS — SO THE RULE IS
# "REFUSE, NEVER CERTIFY"
# ─────────────────────────────────────────────────────────────────────────────
# This script was written on Linux; no macOS behaviour was measured here. What it
# does on macOS: it attempts the BSD env read `ps eww -o pid=,args= -p <pid>` (the
# `pid=`-only spelling is broken on EVERY platform — with no args/comm column there
# is no env in the output at all), and before trusting any "no marker here" answer
# it runs a CONTROL PROBE on its own pid, which is marker-bearing by construction:
# if the census cannot read ITS OWN environment then this platform blocks env reads,
# and the census EXITS 2 rather than certify a table it could not read. A candidate
# whose env cannot be read is `unreadable` and also exits 2 — "could not look" is
# never "clean".
# UNVERIFIED here, wants a macOS spot-check: whether `ps eww` prints another
# user-process's env at all under SIP / proc_security; whether BSD `ps` accepts `-e`
# as "all processes"; whether the env suffix is truncated for large environments.
# Reserved fallback spelling: `ps e -o pid=,args=`. The failure class these rules
# exist to make impossible is a macOS leg that PASSES because it could not look.
#
# ─────────────────────────────────────────────────────────────────────────────
# EXIT-CODE CONTRACT
# ─────────────────────────────────────────────────────────────────────────────
#   0  clean — the table WAS read and no live process of THIS run was left; or
#      candidates were found, proven NOT to be this run's, and left alone (the
#      count is reported to stderr and the run still passes)
#   1  leak — a daemon/worker of THIS run outlived the suite (unix: reported AND
#      killed; windows: reported only, advisory) — or a self-test case misbehaved,
#      or a self-test-only option was used outside `--self-test`
#   2  the census could not answer honestly: no FULCRUM_CI_RUN_MARKER, no `ps` / no
#      `tasklist`, an enumeration it could not parse, or a candidate whose
#      PROVENANCE COULD NOT BE READ. Refusing is never the same as clean.
# Anything else is the tool's own failure.
#
# ─────────────────────────────────────────────────────────────────────────────
# UNPARSEABLE IS A REFUSAL, NEVER "CLEAN"
# ─────────────────────────────────────────────────────────────────────────────
# The same rule the repo already encodes for its Rust probe in
# `fulcrum/tests/proctable/mod.rs` ("unparseable is a PANIC, never 'not alive'")
# and scripts/check-rquickjs-allocator.sh's vacuity floor. A census that cannot
# read the table goes RED. `tasklist`'s report line is matched on the `INFO:`
# MARKER and never on its English prose, because that prose is localised.
#
# Usage: check-no-stray-processes.sh [--self-test] [--keep-temp-dir]
#   --self-test      plant fixtures, one LIVE process and one LIVE zombie, then prove
#                    THIS guard can still fail (the cases re-invoke this same script —
#                    never a second implementation of the matching logic)
#   --keep-temp-dir  leave the self-test's scratch directory in place to inspect
#
# The live census takes no input options at all. `--input`/`--fixture-dir`/
# `--platform` exist ONLY inside `--self-test` and are rejected otherwise (exit 1):
# the live path must never read a mock instead of the real process table. That is
# the live half of the vacuity floor, and self-test Case H pins it.

set -euo pipefail

usage() {
  echo "usage: $(basename "$0") [--self-test] [--keep-temp-dir]" >&2
}

# The images the census watches, in FULL (see the header: matched against ps's
# untruncated argv, never against the 15-char comm).
MATCH_IMAGES=(fulcrum fake_worker_core fake_mcp_stdio_core)
MATCH_IMAGES_WINDOWS=(fulcrum.exe fake_worker_core.exe fake_mcp_stdio_core.exe)

# The discriminator's NAME. The value is this run's secret, the name is public.
# Kept in one place so the CI workflow and this script cannot drift apart silently.
MARKER_NAME="FULCRUM_CI_RUN_MARKER"

# The census's own lineage, resolved once.
#
# The plan asserted the census MUST skip itself because this repo's CI checks out
# into a directory named `fulcrum`, so the step's argv would match the image list.
# MEASURED HERE, AND THAT ASSERTION IS WRONG: for a `#!` script the kernel replaces
# argv, so a step running `bash scripts/check-no-stray-processes.sh` shows up in
# `ps` as `bash /path/to/scripts/check-no-stray-processes.sh` with `comm=bash` —
# basename of the first argv element is `bash`, which is not a watched image. The
# census therefore does NOT self-match under its real CI invocation (verified from a
# checkout deliberately named `fulcrum`, and through a wrapper script named
# `fulcrum`, which also shows as `bash`).
#
# The skip stays, for the case that DOES reproduce: if this script is ever run
# through an interpreter or wrapper whose own file name is a watched image (a copy
# or symlink of bash named `fulcrum`, say), the census's ancestor chain contains a
# `fulcrum` carrying the run marker, and without the skip the census would classify
# and KILL ITS OWN PARENT. Self-test Case L plants exactly that and requires the
# census to survive and to name what it skipped. Defensive, but a census that kills
# the step running it is a worse bug than one that reports a phantom.
CENSUS_PID=""
CENSUS_LINEAGE=" "

# Set in fixture mode only: the marker the emulated census environment carries, and
# an optional `--input` stream file. See census()'s fixture arm.
CENSUS_FIXTURE_MARKER=""
CENSUS_INPUT=""

# ---------------------------------------------------------------------------
# marker_from_env — the unexported-marker floor
# ---------------------------------------------------------------------------
marker_from_env() {
  local marker="${!MARKER_NAME:-}"
  if [[ -z "$marker" ]]; then
    {
      echo "error: $MARKER_NAME is not exported, so this census REFUSES (exit 2) — and refusal is not a pass"
      echo "  this guard answers exactly one question: 'did a daemon or worker of THIS run"
      echo "  outlive the test suite?'. The marker is how a candidate is PROVEN to belong to"
      echo "  this run (its own environment must carry this run's exact value), so without a"
      echo "  marker there is no question to ask — reporting 'clean' here would be a census"
      echo "  certifying a table it was never allowed to read."
      echo "  in CI: the job exports FULCRUM_CI_RUN_MARKER before 'cargo test', so every"
      echo "         process the suite spawns inherits it and this step can attribute."
      echo "  locally: export $MARKER_NAME=\$(date +%s)-\$\$   (a per-run value you choose)"
      echo "           — or run  bash $(basename "$0") --self-test  to see the guard fail on"
      echo "             planted processes instead."
    } >&2
    return 2
  fi
  printf '%s' "$marker"
}

# ---------------------------------------------------------------------------
# image-name recognition (unix)
# ---------------------------------------------------------------------------
# Is <name> one of the watched images? EXACT match, full name — never a prefix,
# never a truncated comm. `fake_worker_corruptor` must fail this test even though
# it shares the 15-char comm with `fake_worker_core`.
is_watched_image() {
  local name="$1" image
  for image in "${MATCH_IMAGES[@]}"; do
    if [[ "$name" == "$image" ]]; then
      return 0
    fi
  done
  return 1
}

# The image of a candidate is the FULL basename of the FIRST argv element. First
# element, because the daemon's argv ENDS in `--worker-exe <path>/fake_worker_core`
# and matching any element would double-count the daemon's own argument as a second
# candidate (the plan's round-2 finding). The inline `${args%% *}` + `${…##*/}` at
# the two call sites is that rule; it is deliberately not a function, so there is
# exactly one way to spell it in this file.

# Does <name> collide with a watched image in the 15-char comm space WITHOUT being
# that image? This is the truncation trap made explicit: `fake_worker_corruptor` and
# `fake_worker_core` share `comm=fake_worker_cor`, so a census that classified on
# comm could not tell them apart. The census reports such a process as `left-alone`
# and never kills it — deliberately NOT a silent skip, because "I saw something whose
# truncated name looks like ours and I decided not to touch it" is the diagnostic a
# future reader of a CI log needs. This function can NEVER produce a leak: only the
# exact match in is_watched_image can.
image_truncation_collision() {
  local name="$1" image
  for image in "${MATCH_IMAGES[@]}"; do
    if [[ "$name" != "$image" && "${name:0:15}" == "${image:0:15}" ]]; then
      return 0
    fi
  done
  return 1
}

# ---------------------------------------------------------------------------
# self-lineage (unix)
# ---------------------------------------------------------------------------
# The census must never classify itself or one of its ancestors as a leak: it runs
# from a checkout whose directory is named `fulcrum`, so `bash
# scripts/check-no-stray-processes.sh` has argv[0] `bash` but the ANCESTOR shells'
# argv contains the word `fulcrum` — and, run from a dir named `fulcrum`, a
# `bash`-argv census step would match the image list on its own lineage. The
# census skips its own pid and every ancestor pid.
resolve_lineage() {
  local pid="$1" ppid guard=0
  CENSUS_LINEAGE=" $1 "
  while [[ -r "/proc/$pid/status" && $guard -lt 64 ]]; do
    # `PPid:` — capital P twice. `awk` regexes are case-sensitive, and `/^Ppid:/`
    # silently matches NOTHING, which leaves the lineage as "just my own pid" and
    # lets the census kill its own parent. Self-test Case L is what catches that.
    ppid="$(awk '/^PPid:/{print $2; exit}' "/proc/$pid/status" 2>/dev/null || true)"
    [[ "$ppid" =~ ^[0-9]+$ ]] || break
    [[ "$ppid" == "0" ]] && break
    CENSUS_LINEAGE="$CENSUS_LINEAGE$ppid "
    pid="$ppid"
    guard=$((guard + 1))
  done
}

is_self_pid() {
  case "$CENSUS_LINEAGE" in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

# ---------------------------------------------------------------------------
# env-lookup seam — the one place live and fixture mode differ
# ---------------------------------------------------------------------------
# load_env <pid> populates ENV_LINES with the candidate's `NAME=VALUE` lines and:
#   returns 0  the environment WAS read (possibly empty of our marker)
#   returns 1  the environment COULD NOT BE READ
# It never guesses. A "could not read" answer becomes `unreadable` upstream, which
# the census treats as a REFUSAL (exit 2) — never as "not ours, leave it".
#
#   CENSUS_MODE=fixture   read the candidate's env from $CENSUS_FIXTURE_DIR/env.txt
#                         (a synthetic pid has no /proc entry, so the fixture
#                         carries the env in-memory; a line `<pid> UNREADABLE`
#                         emulates a platform that blocks the read)
#   CENSUS_MODE=live      Linux: /proc/<pid>/environ (readable by a same-user
#                         sibling when ptrace_scope=0; FAILS with ENOENT for a
#                         zombie, which ps still lists — measured).
#                         macOS: `ps eww -o pid=,args= -p <pid>` (UNVERIFIED here —
#                         see the header). A read that yields no NAME=VALUE text at
#                         all is treated as UNREADABLE, i.e. the conservative
#                         direction: refuse, never certify.
ENV_LINES=()
ENV_STATE="unreadable"

env_from_fixture() {
  local pid="$1" env_file="$CENSUS_FIXTURE_DIR/env.txt" name rest
  ENV_LINES=()
  # The census's OWN env in fixture mode: the control probe emulates the census
  # step's environment, which carries the marker by construction (the census refuses
  # outright without one). CENSUS_FIXTURE_CONTROL_UNREADABLE=1 emulates a platform
  # that will not let the census read even itself — the macOS failure shape Case K
  # pins. The LIVE control probe (no fixture) reads the real /proc/<pid>/environ.
  if [[ -n "${CENSUS_FIXTURE_CONTROL_UNREADABLE:-}" && "$pid" == "$CENSUS_PID" ]]; then
    ENV_STATE="unreadable"
    return 1
  fi
  if [[ "$pid" == "$CENSUS_PID" ]]; then
    ENV_STATE="ok"
    ENV_LINES=("$MARKER_NAME=$CENSUS_FIXTURE_MARKER")
    return 0
  fi
  if [[ ! -r "$env_file" ]]; then
    ENV_STATE="unreadable"
    return 1
  fi
  local found=0 line
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
    read -r name rest <<<"$line" || true
    [[ "$name" == "$pid" ]] || continue
    found=1
    if [[ "$rest" == "UNREADABLE" ]]; then
      ENV_STATE="unreadable"
      return 1
    fi
    [[ -n "$rest" ]] && ENV_LINES+=("$rest")
  done <"$env_file"
  if ((found == 0)); then
    # A candidate with no env record at all: the census was not told anything about
    # its provenance. Refuse rather than assume "not ours".
    ENV_STATE="unreadable"
    return 1
  fi
  ENV_STATE="ok"
  return 0
}

env_from_procfs() {
  local pid="$1" raw
  ENV_LINES=()
  if ! raw="$(tr '\0' '\n' <"/proc/$pid/environ" 2>/dev/null)"; then
    # Two different reasons look identical here: the pid exited between listing and
    # reading (a racing exit — not a leak at census time), or the kernel will not
    # hand over the env (a zombie: measured, `/proc/<z>/environ` gives ENOENT while
    # `ps` still lists it and `kill -0` still succeeds). Re-probe with `ps`.
    if ! ps -p "$pid" >/dev/null 2>&1; then
      ENV_STATE="gone"
    else
      ENV_STATE="unreadable"
    fi
    return 1
  fi
  ENV_STATE="ok"
  while IFS= read -r line; do
    [[ -n "$line" ]] && ENV_LINES+=("$line")
  done <<<"$raw"
  return 0
}

env_from_ps_eww() {
  local pid="$1" raw
  ENV_LINES=()
  # The `args=` column is what makes the env visible: `ps eww -o pid= -p <pid>`
  # prints the number and nothing else on every platform (measured on Linux), so a
  # census written with that spelling would find no marker anywhere and certify
  # clean on a look it never took.
  if ! raw="$(ps eww -o pid=,args= -p "$pid" 2>/dev/null)"; then
    if ! ps -p "$pid" >/dev/null 2>&1; then
      ENV_STATE="gone"
    else
      ENV_STATE="unreadable"
    fi
    return 1
  fi
  local lines=()
  while IFS= read -r line; do
    # Only NAME=VALUE-shaped fields count as environment: an argv token such as
    # `--worker-exe` must never be mistaken for an env entry.
    if [[ "$line" =~ [[:space:]]([A-Za-z_][A-Za-z0-9_]*)=([^[:space:]]*) ]]; then
      lines+=("${BASH_REMATCH[1]}=${BASH_REMATCH[2]}")
    fi
  done <<<"$raw"
  if ((${#lines[@]} == 0)); then
    # Nothing env-shaped came back. On macOS that most likely means the platform
    # refused the read (SIP / proc_security) — so this is UNREADABLE, not "empty
    # environment, not ours". Refusing is the honest direction; a census that read
    # nothing must never report clean.
    ENV_STATE="unreadable"
    return 1
  fi
  ENV_LINES=("${lines[@]}")
  ENV_STATE="ok"
  return 0
}

load_env() {
  local pid="$1"
  case "${CENSUS_MODE:-live}" in
    fixture) env_from_fixture "$pid" ;;
    live)
      case "$CENSUS_PLATFORM" in
        linux) env_from_procfs "$pid" ;;
        macos) env_from_ps_eww "$pid" ;;
        *)
          ENV_STATE="unreadable"
          return 1
          ;;
      esac
      ;;
    *)
      ENV_STATE="unreadable"
      return 1
      ;;
  esac
}

# The macOS control probe. A "no marker found" answer is only evidence if the census
# can read environments AT ALL on this platform, so before relying on one it reads
# its OWN — which carries the marker by construction, because the census step runs
# in the same job environment as `cargo test`. If that read fails, the platform is
# blocking env reads and every "not ours" answer in this run would be a guess.
CONTROL_STATE="unprobed"
control_probe() {
  local marker="$1" line
  if load_env "${CENSUS_PID:-$$}"; then
    for line in ${ENV_LINES[@]+"${ENV_LINES[@]}"}; do
      if [[ "$line" == "$MARKER_NAME=$marker" ]]; then
        CONTROL_STATE="ok"
        return 0
      fi
    done
    CONTROL_STATE="no-marker-in-own-env"
  else
    CONTROL_STATE="$ENV_STATE"
  fi
  return 1
}

env_carries_marker() {
  local marker="$1" line
  for line in ${ENV_LINES[@]+"${ENV_LINES[@]}"}; do
    # Exact value match: presence of the NAME is not enough, or a developer who
    # exports the same variable with their own value would be counted as this run.
    if [[ "$line" == "$MARKER_NAME=$marker" ]]; then
      return 0
    fi
  done
  return 1
}

# ---------------------------------------------------------------------------
# parse_candidate_stream — the pure classifier, ONE source of truth
# ---------------------------------------------------------------------------
# Reads candidate lines on stdin and applies the discriminator. Live mode feeds it
# the enumerator's output; the self-test feeds it fixtures through the same call.
#
#   unix candidate format    <pid> <ppid> <stat> <argv…>   (what enumerate_unix emits)
#   windows candidate format `tasklist /FO CSV` rows (header and INFO lines allowed
#                            in the stream and handled explicitly)
#
# Prints one summary line per decision and returns 0 clean / 1 any leak / 2 the
# stream could not be classified.
LEAK_PIDS=()
LEAK_KINDS=()
LEFT_ALONE_COUNT=0
UNREADABLE_COUNT=0
CANDIDATE_COUNT=0
COLLISION_COUNT=0

parse_candidate_stream() {
  local marker="$1" platform="$2"
  LEAK_PIDS=()
  LEAK_KINDS=()
  LEFT_ALONE_COUNT=0
  UNREADABLE_COUNT=0
  CANDIDATE_COUNT=0
  COLLISION_COUNT=0

  local unreadable=0 line pid args image
  local ppid stat rest zombie name_is_exact provenance_pid

  if [[ "$platform" == "windows" ]]; then
    # tasklist's CSV quotes EVERY field (the lesson proctable/mod.rs records), so
    # the image column is read POSITIONALLY (index 0) from a `/FO CSV` row and never
    # by header name — the header text and the INFO prose are localised, the column
    # order is part of the output format.
    local rows=0 saw_info=0
    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -z "${line//[[:space:]]/}" ]] && continue
      if [[ "${line:0:5}" == "INFO:" ]]; then
        saw_info=1
        continue
      fi
      if [[ "${line:0:1}" != "\"" ]]; then
        printf 'unreadable: not a tasklist CSV row: %s\n' "$line" >&2
        unreadable=1
        break
      fi
      rows=$((rows + 1))
      local head="${line%%\",\"*}"
      image="${head#\"}"
      if [[ "$image" == "Image Name" ]]; then
        continue
      fi
      local is_ours=0 want
      for want in "${MATCH_IMAGES_WINDOWS[@]}"; do
        if [[ "$image" == "$want" ]]; then is_ours=1; break; fi
      done
      if ((is_ours == 1)); then
        CANDIDATE_COUNT=$((CANDIDATE_COUNT + 1))
        LEAK_PIDS+=("$image")
        printf 'leak: %s (advisory — windows cannot attribute a process to this run, so nothing is killed)\n' "$image" >&2
      fi
    done
    if ((unreadable == 1)); then
      return 2
    fi
    if ((rows == 0 && saw_info == 0)); then
      printf '%s\n' "error: tasklist produced neither a row nor an INFO: report — an empty process table is not evidence" >&2
      return 2
    fi
  else
    # The control probe first (macOS): a platform that will not let the census read
    # its OWN environment cannot answer "is this one ours?" for anybody else.
    if [[ "$platform" == "macos" ]]; then
      if ! control_probe "$marker"; then
        {
          echo "error: this census cannot read process environments on macOS, so it REFUSES (exit 2) rather than certify clean"
          echo "  control probe on the census's own pid ($CENSUS_PID, which must carry $MARKER_NAME) returned:"
          printf '    %s\n' "$CONTROL_STATE"
          echo "  run-scope on macOS is UNVERIFIED from the machine that wrote this; the census reads env with"
          echo "  'ps eww -o pid=,args= -p <pid>' (fallback: 'ps e -o pid=,args='). If the platform blocks that"
          echo "  read (SIP / proc_security), every 'not this run's process' answer would be a guess, and a"
          echo "  census that passes because it could not look is the exact failure this guard exists to prevent."
        } >&2
        return 2
      fi
    fi

    while IFS= read -r line || [[ -n "$line" ]]; do
      [[ -z "${line//[[:space:]]/}" ]] && continue
      if [[ "${line:0:1}" == "#" ]]; then
        continue
      fi
      read -r pid ppid stat rest <<<"$line" || true
      if [[ ! "${pid:-}" =~ ^[0-9]+$ || ! "${ppid:-}" =~ ^[0-9]+$ || -z "${stat:-}" || -z "${rest:-}" ]]; then
        printf 'unreadable: not a census candidate line: %s\n' "$line" >&2
        unreadable=1
        break
      fi
      # Exact full-basename match on the FIRST argv element. `fake_mcp_stdio_core`
      # (19 chars, comm `fake_mcp_stdio_`) passes this; the 15-char truncated form
      # and `fake_worker_corruptor` do not — Cases J and B3.
      #
      # A ZOMBIE is the exception, and it is forced by `ps` itself: a zombie's argv
      # is printed as `[<name>] <defunct>` with the name truncated to 15 (measured),
      # so the exact rule cannot apply to it. The name then gives SCOPE only; see
      # enumerate_unix for why provenance comes from the PARENT in that case.
      zombie=0
      if [[ "${stat:0:1}" == "Z" ]]; then
        zombie=1
        image="${rest%% *}"
        image="${image#[}"
        image="${image%]}"
      else
        image="${rest%% *}"
        image="${image##*/}"
      fi
      name_is_exact=1
      if is_watched_image "$image"; then
        :
      elif image_truncation_collision "$image"; then
        name_is_exact=0
        # A LIVE process whose name merely collides in the 15-char comm space is NOT
        # one of ours, and the census says so rather than dropping it silently. A
        # ZOMBIE cannot be resolved at all (its name is truncated by `ps`), so the
        # collision is not yet a verdict — the parent's marker decides below.
        if ((zombie == 0)); then
          printf 'left-alone: %s %s (shares a 15-char comm with a watched image but is not one — the census does not match truncated names)\n' "$pid" "$image" >&2
          COLLISION_COUNT=$((COLLISION_COUNT + 1))
          continue
        fi
      else
        continue
      fi
      if is_self_pid "$pid"; then
        printf 'left-alone: %s %s (the census itself — never a candidate)\n' "$pid" "$image" >&2
        LEFT_ALONE_COUNT=$((LEFT_ALONE_COUNT + 1))
        continue
      fi
      CANDIDATE_COUNT=$((CANDIDATE_COUNT + 1))
      # Provenance: the candidate's own env, except for a zombie, whose env the
      # kernel will not hand over (`/proc/<z>/environ` is ENOENT — measured) and whose
      # only reachable provenance is the parent that failed to reap it.
      if ((zombie == 1)); then
        provenance_pid="$ppid"
      else
        provenance_pid="$pid"
      fi
      if ! load_env "$provenance_pid"; then
        if [[ "$ENV_STATE" == "gone" && $zombie == 0 ]]; then
          printf 'gone-before-read: %s %s (exited between listing and reading — not a leak at census time)\n' "$pid" "$image" >&2
          continue
        fi
        printf 'unreadable: %s %s (provenance could not be read from pid %s: %s)\n' "$pid" "$image" "$provenance_pid" "$ENV_STATE" >&2
        UNREADABLE_COUNT=$((UNREADABLE_COUNT + 1))
        unreadable=1
        continue
      fi
      if env_carries_marker "$marker"; then
        LEAK_PIDS+=("$pid")
        if ((zombie == 1)); then
          LEAK_KINDS+=("zombie")
          printf "leak: %s %s (ZOMBIE, stat=%s, parent %s carries this run's %s — its parent never reaped it)\n" \
            "$pid" "$image" "$stat" "$ppid" "$MARKER_NAME" >&2
          if ((name_is_exact == 0)); then
            printf '  note: %s is a zombie whose name `ps` truncates to 15 chars, so the exact image cannot be resolved;\n' "$image" >&2
            printf '        it is reported because it is an unreaped child of THIS run, which is the defect itself.\n' >&2
          fi
        else
          LEAK_KINDS+=("live")
          printf "leak: %s %s (carries this run's %s)\n" "$pid" "$image" "$MARKER_NAME" >&2
        fi
      else
        if ((zombie == 1)); then
          printf "left-alone: %s %s (zombie whose parent %s does not carry this run's %s — not ours to touch)\n" "$pid" "$image" "$ppid" "$MARKER_NAME" >&2
        else
          printf "left-alone: %s %s (does not carry this run's %s — not ours to touch)\n" "$pid" "$image" "$MARKER_NAME" >&2
        fi
        LEFT_ALONE_COUNT=$((LEFT_ALONE_COUNT + 1))
      fi
    done
    if ((unreadable == 1)); then
      return 2
    fi
  fi

  if ((${#LEAK_PIDS[@]} > 0)); then
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# enumerate_unix — thin io, no decision logic (UNIX-ONLY)
# ---------------------------------------------------------------------------
# ONE `ps -eo pid=,ppid=,stat=,comm=,args=` read of the whole table, rather than one
# `ps -p <pid>` call per candidate (each is a process spawn and each races an exit).
# Emits the stable candidate shape the classifier consumes:
#     <pid> <ppid> <stat> <argv…>
# ppid and stat are carried because of ZOMBIES — see below. `comm` is read and
# DISCARDED: it is the truncated field the census must not classify on.
#
# THE ZOMBIE PROBLEM, measured. `ps` does NOT print a zombie's real argv: it prints
# `[<name>] <defunct>`, and the bracketed name is TRUNCATED to 15 (measured:
# `/tmp/zz/fulcrum` → `[fulcrum] <defunct>`, `/tmp/zz/fake_worker_core` →
# `[fake_worker_cor] <defunct>`). So the exact-basename rule that makes the live
# census exact can never fire on a zombie of a >15-char image, and matching the
# bracketed name would be matching a truncated name — the thing Case B3 forbids.
# RESOLUTION: for a zombie the NAME gives SCOPE only (exact or 15-char collision),
# and PROVENANCE comes from the marker on its PARENT — `/proc/<ppid>/environ`, the
# same discriminator, because only the parent can reap it and a zombie of this run
# necessarily has a parent of this run. A zombie whose parent's provenance cannot be
# read is an UNREADABLE refusal, never a pass and never a guess.
enumerate_unix() {
  if ! command -v ps >/dev/null 2>&1; then
    printf '%s\n' "error: ps is missing — the census cannot read the process table, and 'no candidates' is not evidence" >&2
    return 2
  fi
  local table rc=0
  table="$(ps -eo pid=,ppid=,stat=,comm=,args= 2>/dev/null)" || rc=$?
  if [[ $rc != 0 ]]; then
    printf 'error: ps failed (exit %s) — the census refuses to report on a table it could not read\n' "$rc" >&2
    return 2
  fi
  local pid ppid stat comm args image
  while IFS= read -r line; do
    [[ -z "${line//[[:space:]]/}" ]] && continue
    read -r pid ppid stat comm args <<<"$line" || true
    [[ "${pid:-}" =~ ^[0-9]+$ ]] || continue
    [[ "${ppid:-}" =~ ^[0-9]+$ ]] || continue
    [[ -n "${stat:-}" ]] || continue
    [[ -n "${args:-}" ]] || continue
    image="${args%% *}"
    # A zombie's argv is `[name] <defunct>`; the brackets are ps's, not the image's.
    if [[ "${stat:0:1}" == "Z" ]]; then
      image="${image#[}"
      image="${image%]}"
    else
      image="${image##*/}"
    fi
    # Emit exact matches AND 15-char comm collisions: the classifier reports the
    # collisions rather than the enumerator silently dropping them, so a CI log shows
    # the census SAW the near-miss and what it decided about it.
    if is_watched_image "$image" || image_truncation_collision "$image"; then
      printf '%s %s %s %s\n' "$pid" "$ppid" "$stat" "$args"
    fi
  done <<<"$table"
  return 0
}

# ---------------------------------------------------------------------------
# enumerate_windows — thin io (WINDOWS-ONLY, ADVISORY)
# ---------------------------------------------------------------------------
# ⚠ THE GUARD AGAINST A VACUOUS WINDOWS LEG IS HERE, NOT IN A TEST: every call
# passes `/FI "IMAGENAME eq <image>"`. A `tasklist` WITHOUT `/FI` lists every image
# on the machine, and a stream of images the census does not watch reads as clean —
# a guard that answers "no leaks" because it asked for the wrong rows. If you ever
# drop `/FI`, the positional parse still filters by image, so the failure would be
# silent: keep the filter.
# `/FO CSV` is what makes rows parse uniformly. Whether MSYS/Git bash rewrites these
# single-slash flags into Windows paths could not be tested from this Linux box, so
# the call disables path conversion explicitly rather than relying on the flag form
# surviving (see the header's UNVERIFIED note).
enumerate_windows() {
  if ! command -v tasklist >/dev/null 2>&1; then
    printf '%s\n' "error: tasklist is missing — the census cannot read the process table, and 'no candidates' is not evidence" >&2
    return 2
  fi
  local image rc=0 out
  for image in "${MATCH_IMAGES_WINDOWS[@]}"; do
    # MSYS_NO_PATHCONV / MSYS2_ARG_CONV_EXCL: no-ops everywhere else, and on Git
    # bash they stop `/FO CSV` being rewritten to `C:/Program Files/Git/FO`.
    out="$(MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' \
      tasklist /FO CSV /NH /FI "IMAGENAME eq ${image}" 2>/dev/null)" || rc=$?
    if [[ $rc != 0 ]]; then
      printf 'error: tasklist failed for %s (exit %s) — refusing to report on a table it could not read\n' "$image" "$rc" >&2
      return 2
    fi
    printf '%s\n' "$out"
  done
  return 0
}

# ---------------------------------------------------------------------------
# kill_leak — TERM, re-probe, KILL, re-probe (UNIX-ONLY)
# ---------------------------------------------------------------------------
# A census that finds a leak kills it: these are processes of THIS run, proven by
# the marker, and a leaked `fulcrum serve` never exits on its own. Detection is the
# job; a survivor is REPORTED by name and the run still goes red — the census does
# not die trying.
#
# A ZOMBIE needs a different target, and this is not a refinement but a correction:
# a zombie ignores SIGTERM and SIGKILL entirely (there is no task left to deliver to),
# so signalling it burns the whole retry budget and changes nothing. The only thing
# that removes a zombie is its PARENT reaping it — and we already proved the parent
# belongs to this run, because the parent's marker is what classified the zombie. So
# the census signals the PARENT, which is the action that actually clears the leak.
# Case M asserts exactly this: kill the parent, the zombie goes away.
kill_leak() {
  local pid="$1" i state ppid target
  state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ' || true)"
  if [[ "${state:0:1}" == "Z" ]]; then
    ppid="$(awk '/^PPid:/{print $2; exit}' "/proc/$pid/status" 2>/dev/null || true)"
    if [[ ! "${ppid:-}" =~ ^[0-9]+$ ]] || ((ppid <= 1)); then
      printf 'leak: %s is a ZOMBIE (stat=%s) with no reachable parent — only init can reap it now\n' "$pid" "$state" >&2
      return 1
    fi
    printf '  kill: %s is a zombie; signalling its parent %s (a zombie cannot be killed, only reaped)\n' "$pid" "$ppid" >&2
    target="$ppid"
  else
    target="$pid"
  fi

  if ! kill -TERM "$target" 2>/dev/null; then
    printf '  kill: TERM(%s) failed (already gone, or not ours to signal)\n' "$target" >&2
  fi
  for ((i = 0; i < 20; i++)); do
    ps -p "$pid" >/dev/null 2>&1 || { printf '  kill: %s is gone after TERM\n' "$pid" >&2; return 0; }
    sleep 0.1
  done
  printf '  kill: %s survived TERM, escalating to KILL\n' "$pid" >&2
  kill -KILL "$target" 2>/dev/null || true
  for ((i = 0; i < 20; i++)); do
    ps -p "$pid" >/dev/null 2>&1 || { printf '  kill: %s is gone after KILL\n' "$pid" >&2; return 0; }
    sleep 0.1
  done
  state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ' || true)"
  if [[ "${state:0:1}" == "Z" ]]; then
    printf 'leak: %s is STILL a ZOMBIE (stat=%s) after its parent was killed — something else holds it\n' "$pid" "$state" >&2
  else
    printf 'leak: %s is STILL ALIVE after KILL (stat=%s) — the census detected it but could not reap it\n' "$pid" "${state:-unknown}" >&2
  fi
  return 1
}

# ---------------------------------------------------------------------------
# census — enumerate, classify, kill, re-enumerate, report
# ---------------------------------------------------------------------------
census() {
  local marker="$1" platform="$2"
  local stream rc=0 i pid

  stream="$(mktemp)"
  # shellcheck disable=SC2064
  trap "rm -f -- '$stream'" RETURN

  if [[ "${CENSUS_MODE:-live}" == "fixture" ]]; then
    # The control probe reads the census's own pid in every mode, and the fixture
    # env seam needs the marker value it emulates.
    CENSUS_PID="$$"
    CENSUS_FIXTURE_MARKER="$marker"
    local fixture_stream="${CENSUS_INPUT:-$CENSUS_FIXTURE_DIR/stream.txt}"
    if [[ ! -r "$fixture_stream" ]]; then
      printf 'error: fixture stream is not readable: %s\n' "$fixture_stream" >&2
      return 2
    fi
    cp -- "$fixture_stream" "$stream" || return 2
  else
    case "$platform" in
      linux|macos)
        CENSUS_PID="$$"
        resolve_lineage "$CENSUS_PID"
        if ! enumerate_unix >"$stream"; then
          printf '%s\n' "error: the unix process-table enumeration failed — the census refuses (exit 2) rather than report a clean table it never read" >&2
          return 2
        fi
        ;;
      windows)
        # ADVISORY, and it runs on the runner's bash exactly like the allocator
        # guard does. There is deliberately NO "is this a native shell" gate here:
        # on windows-latest the default shell invokes Git-for-Windows bash, which
        # always sets MSYSTEM, so such a gate would refuse on every leg and the step
        # would never report anything. The honest limitation is the banner the leak
        # report prints, not a refusal to look.
        if ! enumerate_windows >"$stream"; then
          printf '%s\n' "error: the windows process-table enumeration failed — the census refuses (exit 2) rather than report a clean table it never read" >&2
          return 2
        fi
        ;;
      *)
        printf 'error: unsupported platform for the census: %s — refusing to guess what "no leaks" means here\n' "$platform" >&2
        return 2
        ;;
    esac
  fi

  parse_candidate_stream "$marker" "$platform" <"$stream" || rc=$?

  if [[ $rc == 2 ]]; then
    printf '%s\n' "error: the census could not classify the process table — REFUSING (exit 2); an unreadable candidate is never counted as clean" >&2
    return 2
  fi

  local leaks=${#LEAK_PIDS[@]}
  if ((leaks == 0)); then
    # The vacuity floor's positive witness: "clean" is only reportable because the
    # census read the table and can say how many candidates it looked at.
    printf "clean: the process table was read; %s candidate(s) of the watched images, 0 of them carrying this run's %s (%s left alone as somebody else's, %s 15-char comm collision(s) declined)\n" \
      "$CANDIDATE_COUNT" "$MARKER_NAME" "$LEFT_ALONE_COUNT" "$COLLISION_COUNT"
    return 0
  fi

  {
    echo "error: $leaks daemon/worker process(es) of THIS run outlived the test suite"
    if [[ "$platform" == "windows" ]]; then
      echo "  ⚠ WINDOWS LEG IS ADVISORY: issue #5's 'fail the run' criterion is ENFORCED ON UNIX"
      echo "    ONLY. Windows cannot read another process's environment from bash, so this leg"
      echo "    cannot prove a match belongs to this run and therefore kills NOTHING. On a"
      echo "    self-hosted Windows runner a developer's own fulcrum.exe can redden this step."
    fi
  } >&2

  if [[ "${CENSUS_MODE:-live}" == "fixture" ]]; then
    # A fixture pid is synthetic; there is nothing to kill. Fixture mode proves the
    # CLASSIFICATION and the exit code — the kill path is proven live by Case I.
    printf '  (fixture mode: classification only — nothing was killed; the kill path is proven by the live planted-process case)\n' >&2
    return 1
  fi

  if [[ "$platform" == "windows" ]]; then
    printf '  advisory: no process was killed on windows\n' >&2
    return 1
  fi

  local survivors=0
  for i in "${!LEAK_PIDS[@]}"; do
    pid="${LEAK_PIDS[$i]}"
    kill_leak "$pid" || survivors=$((survivors + 1))
  done

  # Re-enumerate and re-classify: the census must be able to NAME what it could not
  # reap. A zombie ignores TERM and KILL and only its parent can reap it, so the
  # second pass may still see it — that is a leak by this census's own definition
  # (a process of this run that outlived the suite) and the exit stays 1.
  local residual=0
  if enumerate_unix >"$stream"; then
    local rc2=0
    # stderr stays visible: the reason lines (ZOMBIE / STILL ALIVE) are the part of
    # the report a human needs in order to fix the test that leaked.
    parse_candidate_stream "$marker" "$platform" <"$stream" || rc2=$?
    if [[ $rc2 == 1 ]]; then
      residual=${#LEAK_PIDS[@]}
      printf 'error: after killing, %s process(es) of this run are STILL alive: %s\n' \
        "$residual" "${LEAK_PIDS[*]}" >&2
    elif [[ $rc2 == 2 ]]; then
      printf 'error: after killing, the census could not re-read the table — it will not claim the run is clean\n' >&2
      residual=1
    fi
  else
    printf 'error: after killing, the census could not re-read the table — it will not claim the run is clean\n' >&2
    residual=1
  fi

  {
    echo "error: the suite left $leaks process(es) of this run alive; the run is RED"
    if ((residual == 0)); then
      echo "  the census killed them; nothing of this run is left in the table now."
    fi
    echo "  a leaked 'fulcrum serve' never exits by itself, so each of these would have outlived"
    echo "  the job. Find the test that lost its child: the per-test guards (Spawned::drop,"
    echo "  pid_is_reaped) only ask about the pid each test recorded — this census is the only"
    echo "  guard that asks about the whole table."
    if ((survivors > 0)); then
      printf '  %s of them could NOT be reaped (see the leak: lines above)\n' "$survivors"
    fi
  } >&2
  return 1
}

# ---------------------------------------------------------------------------
# --self-test harness
# ---------------------------------------------------------------------------
# Every case re-invokes THIS script (the allocator guard's idiom — a second copy of
# the matching logic here would prove nothing about the one that runs in CI). The
# census inside --self-test may read fixtures through the seams below; the LIVE
# census cannot, which is Case H's job to pin.
self_test_failures=0
self_test_root=""
self_test_keep_temp_dir=0
self_test_planted=()

self_test_cleanup() {
  local pid
  for pid in ${self_test_planted[@]+"${self_test_planted[@]}"}; do
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  if [[ -n "$self_test_root" && -d "$self_test_root" ]]; then
    if ((self_test_keep_temp_dir == 1)); then
      printf 'self-test: kept scratch dir at %s\n' "$self_test_root"
    else
      rm -rf -- "$self_test_root"
    fi
  fi
}

expect_exit() {
  local what="$1" want="$2"
  shift 2
  # The repo's own idiom (check-rquickjs-allocator.sh:148-163): `|| got=$?` and
  # NEVER `if ! output=$(...)` — under `if !` the `!` inverts the status so `$?`
  # read in the then-branch is 0, which would report a working guard as broken.
  local got=0 output
  output="$("$@" 2>&1)" || got=$?
  if [[ "$got" == "$want" ]]; then
    printf 'ok   %s (exit %s)\n' "$what" "$got"
    LAST_OUTPUT="$output"
  else
    printf 'FAIL %s\n' "$what"
    printf '       expected exit %s, got %s\n' "$want" "$got"
    printf '       output: %s\n' "${output:-<empty>}"
    LAST_OUTPUT="$output"
    self_test_failures=$((self_test_failures + 1))
  fi
}

LAST_OUTPUT=""

# Assert the previous expect_exit case also SAID the thing it must say — an exit
# code alone cannot tell "refused because unreadable" from "refused for another
# reason", and a guard with the right exit code and the wrong reason is still a lie.
expect_output_has() {
  local what="$1" needle="$2"
  if [[ "$LAST_OUTPUT" == *"$needle"* ]]; then
    printf 'ok   %s\n' "$what"
  else
    printf 'FAIL %s\n' "$what"
    printf '       expected output to contain: %s\n' "$needle"
    printf '       output: %s\n' "${LAST_OUTPUT:-<empty>}"
    self_test_failures=$((self_test_failures + 1))
  fi
}

# new_fixture <name> — a fresh fixture dir; echoes its path
new_fixture() {
  local dir="$self_test_root/$1"
  mkdir -p "$dir"
  : >"$dir/stream.txt"
  : >"$dir/env.txt"
  printf '%s' "$dir"
}

run_self_test() {
  local script
  script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  self_test_root="$(mktemp -d)"
  trap self_test_cleanup EXIT

  local marker="selftest-run-$$-$RANDOM"
  export FULCRUM_CI_RUN_MARKER="$marker"

  local d
  # ---- Case A: a table with nothing of ours in it -----------------------------
  d="$(new_fixture clean)"
  {
    printf '%s\n' '# a busy machine, none of it ours'
    printf '%s\n' '4001 1 S /usr/lib/systemd/systemd --system'
    printf '%s\n' '4002 1 S /home/dev/other-project/target/debug/fake_worker_corruptor --serve'
    printf '%s\n' '4003 1 S cargo test --all'
  } >"$d/stream.txt"
  printf '%s\n' '4001 PATH=/usr/bin' '4002 PATH=/usr/bin' '4003 PATH=/usr/bin' >"$d/env.txt"
  expect_exit "A  a table with no candidate of this run passes" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "A  reports that it read the table" "the process table was read"

  # ---- Case B: a marked leak is caught ---------------------------------------
  d="$(new_fixture marked-leak)"
  printf '%s\n' '4100 1 S /tmp/build/fake_worker_core --worker' >"$d/stream.txt"
  printf '%s\n' "4100 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "B  a worker carrying this run's marker is a leak" 1 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "B  names the leak" "leak: 4100 fake_worker_core"

  # ---- Case B2: the TRUNCATION PIN — a 19-char image name must be caught -----
  # This case is unrepresentable in a census that classifies on comm or uses
  # `pgrep -x`: `fake_mcp_stdio_core` has comm `fake_mcp_stdio_`, and `pgrep -x`
  # on a >15-char name exits 1 whatever is running. A census that "simplifies" back
  # to either reports left-alone (exit 0) and this case goes RED.
  d="$(new_fixture long-name-leak)"
  printf '%s\n' '4200 1 S /tmp/build/fake_mcp_stdio_core --stdio' >"$d/stream.txt"
  printf '%s\n' "4200 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "B2 the 19-char fake_mcp_stdio_core IS detected (pgrep -x would be blind)" 1 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "B2 names the 19-char leak" "leak: 4200 fake_mcp_stdio_core"

  # ---- Case B3: the PREFIX-AMBIGUITY PIN — same comm, different image --------
  # `fake_worker_corruptor` (24 chars) shares the 15-char comm `fake_worker_cor`
  # with `fake_worker_core` (measured: pgrep -x fake_worker_cor returns both). It
  # carries THIS run's marker and must still be left alone: the match is the FULL
  # basename of the first argv element, and nothing less. A census matching the
  # truncated form would kill an unrelated process and this case goes RED.
  d="$(new_fixture prefix-collision-alone)"
  printf '%s\n' '4300 1 S /home/dev/victim/fake_worker_corruptor --serve' >"$d/stream.txt"
  printf '%s\n' "4300 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "B3 an unrelated fake_worker_corruptor (same 15-char comm) is left alone" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "B3 reports it as left-alone, not a leak" "left-alone: 4300 fake_worker_corruptor"

  d="$(new_fixture prefix-collision-mixed)"
  {
    printf '%s\n' '4310 1 S /home/dev/victim/fake_worker_corruptor --serve'
    printf '%s\n' '4311 1 S /tmp/build/fake_worker_core --worker'
  } >"$d/stream.txt"
  printf '%s\n' "4310 $MARKER_NAME=$marker" "4311 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "B3 the real leak next to the same-comm decoy is still caught" 1 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "B3 the decoy survived next to the leak" "left-alone: 4310 fake_worker_corruptor"

  # ---- Case C: a DIFFERENT marker value is not this run ----------------------
  d="$(new_fixture other-marker)"
  printf '%s\n' '4400 1 S /tmp/build/fulcrum serve --socket /tmp/x.sock' >"$d/stream.txt"
  printf '%s\n' "4400 $MARKER_NAME=999999-9" >"$d/env.txt"
  expect_exit "C  a daemon carrying a DIFFERENT marker value is left alone" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "C  names it as left-alone" "left-alone: 4400 fulcrum"

  # ---- Case D: no marker at all ----------------------------------------------
  d="$(new_fixture no-marker)"
  printf '%s\n' '4500 1 S /home/dev/checkout/target/debug/fulcrum serve --db /tmp/a.db' >"$d/stream.txt"
  printf '%s\n' '4500 PATH=/usr/bin HOME=/home/dev' >"$d/env.txt"
  expect_exit "D  a daemon with no marker in its env is left alone" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "D  reports it as left-alone" "left-alone: 4500 fulcrum"

  # ---- Case E: no marker exported at all = REFUSAL, never clean --------------
  expect_exit "E  no $MARKER_NAME exported is a refusal, never a pass" 2 \
    env -u "$MARKER_NAME" bash "$script"
  expect_output_has "E  the refusal explains WHY" "not exported"
  expect_output_has "E  the refusal points at the self-test" "--self-test"

  # ---- Case F: an unclassifiable table is a REFUSAL --------------------------
  d="$(new_fixture garbage)"
  printf '%s\n' 'this is not a process table at all' >"$d/stream.txt"
  : >"$d/env.txt"
  expect_exit "F  a candidate stream that cannot be parsed is refused" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "F  says which line it could not read" "not a census candidate line"

  d="$(new_fixture win-garbage)"
  printf '%s\n' 'some localized junk that is neither a CSV row nor an INFO: report' >"$d/stream.txt"
  expect_exit "F  a windows stream with a non-row non-INFO line is refused" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=windows CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census

  # ---- Case G: the real tasklist no-match shape ------------------------------
  d="$(new_fixture win-info)"
  printf '%s\n' 'INFO: No tasks are running which match the specified criteria.' >"$d/stream.txt"
  expect_exit "G  tasklist's INFO: report reads as clean (matched on the INFO: marker, not prose)" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=windows CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census

  d="$(new_fixture win-leak)"
  printf '%s\n' '"Image Name","PID","Session Name","Session#","Mem Usage"' \
    '"fulcrum.exe","8620","Console","1","25,872 K"' >"$d/stream.txt"
  expect_exit "G  a tasklist row for fulcrum.exe is an ADVISORY leak (and nothing is killed)" 1 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=windows CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "G  the windows leak is labelled advisory" "advisory"
  expect_output_has "G  the windows leg states what is enforced where" "ENFORCED ON UNIX"

  # ---- Case H: a mock can never sit between the LIVE census and the table ----
  expect_exit "H  --input outside --self-test is rejected" 1 \
    bash "$script" --input /etc/hostname
  expect_output_has "H  the rejection explains why" "self-test"
  expect_exit "H  --fixture-dir outside --self-test is rejected" 1 \
    bash "$script" --fixture-dir /tmp
  expect_exit "H  --platform outside --self-test is rejected" 1 \
    bash "$script" --platform windows

  # ---- Case I: the LIVE planted process — the enumeration path, for real -----
  # A fixture stream can never prove `enumerate_unix` can SEE a live process, so the
  # self-test plants one: a real exec of a real binary really named
  # `fake_worker_core`, with the marker in its real environment, and the census must
  # find it through `ps`, classify it through /proc/<pid>/environ, report it, and
  # kill it. If the enumeration were `--input`-only, or matched on comm, this fails.
  local live="$self_test_root/live" bin
  mkdir -p "$live"
  for bin in fake_worker_core fulcrum; do
    if cp "$(command -v sleep)" "$live/$bin" 2>/dev/null; then
      chmod +x "$live/$bin"
    fi
  done
  if [[ -x "$live/fake_worker_core" ]]; then
    local live_marker="selftest-live-$$-$RANDOM"
    # setsid + full stdio redirection: the planted process must not hold the
    # self-test's output pipe, and must not die with the census's process group.
    ( CENSUS_LIVE_MARKER="$live_marker" FULCRUM_CI_RUN_MARKER="$live_marker" \
        setsid "$live/fake_worker_core" 300 </dev/null >/dev/null 2>&1 &
      printf '%s' "$!" >"$live/planted.pid" )
    local planted=""
    [[ -r "$live/planted.pid" ]] && planted="$(cat "$live/planted.pid")"
    [[ -n "$planted" ]] && self_test_planted+=("$planted")
    sleep 0.4

    local enum_out="$self_test_root/enumerated.txt"
    expect_exit "I  the live ps enumeration FINDS the planted fake_worker_core" 0 \
      env CENSUS_DUMP_ENUM="$enum_out" CENSUS_PLATFORM=linux bash "$script" --self-test --enumerate-only
    # The enumeration must carry BOTH the planted pid and the full path — the pid
    # proves it saw THIS process, the path proves it read the untruncated argv.
    if [[ -r "$enum_out" ]] && awk -v p="$planted" -v b="$live/fake_worker_core" \
        '$1==p && index($0, b) {found=1} END{exit !found}' "$enum_out"; then
      printf 'ok   I  the enumerated line carries the planted pid %s and its full argv\n' "$planted"
    else
      printf 'FAIL I  the enumerated stream does not contain the planted pid %s\n' "$planted"
      printf '       enumerated: %s\n' "$(tr '\n' '|' <"$enum_out" 2>/dev/null || echo '<missing>')"
      self_test_failures=$((self_test_failures + 1))
    fi

    expect_exit "I  the LIVE census reports the planted process as a leak" 1 \
      env CENSUS_PLATFORM=linux FULCRUM_CI_RUN_MARKER="$live_marker" bash "$script"
    expect_output_has "I  the live leak line names the pid and the image" "leak: $planted fake_worker_core"

    if ps -p "$planted" >/dev/null 2>&1; then
      printf 'FAIL I  the census did not kill the planted process (pid %s is still alive)\n' "$planted"
      self_test_failures=$((self_test_failures + 1))
    else
      printf 'ok   I  the planted process was killed by the census\n'
    fi

    # And the discriminator, live: a DIFFERENT run's marker must not be touched.
    ( FULCRUM_CI_RUN_MARKER="some-other-run-$$" setsid "$live/fake_worker_core" 300 </dev/null >/dev/null 2>&1 &
      printf '%s' "$!" >"$live/other.pid" )
    local other=""
    [[ -r "$live/other.pid" ]] && other="$(cat "$live/other.pid")"
    [[ -n "$other" ]] && self_test_planted+=("$other")
    sleep 0.4
    expect_exit "I  a live process with ANOTHER run's marker is left alone" 0 \
      env CENSUS_PLATFORM=linux FULCRUM_CI_RUN_MARKER="$live_marker" bash "$script"
    if ps -p "$other" >/dev/null 2>&1; then
      printf 'ok   I  the other-run process is still alive (untouched)\n'
      kill -KILL "$other" 2>/dev/null || true
      wait "$other" 2>/dev/null || true
    else
      printf "FAIL I  the census killed a process that was NOT this run's (pid %s)\n" "$other"
      self_test_planted=("${self_test_planted[@]/$other/}")
      self_test_failures=$((self_test_failures + 1))
    fi

    # The census must also survive being run from a checkout whose directory is
    # named `fulcrum` (this repo's own CI): the census skips its own lineage.
    expect_exit "I  a clean live census passes and proves it read the table" 0 \
      env CENSUS_PLATFORM=linux FULCRUM_CI_RUN_MARKER="selftest-clean-$$-$RANDOM" bash "$script"
    expect_output_has "I  the clean census reports the candidate count it looked at" "the process table was read"
  else
    printf 'FAIL I  could not plant a live fake_worker_core (no sleep binary to copy?)\n'
    self_test_failures=$((self_test_failures + 1))
  fi

  # ---- Case J: the census never accepts a truncated name ---------------------
  # If classification ever moved to comm (or to `pgrep -x`'s matching), the
  # 15-char form would become the accepted spelling and this row would become a
  # leak. It must be rejected: the census matches FULL names only.
  d="$(new_fixture truncated-name)"
  printf '%s\n' '4700 1 S /tmp/build/fake_mcp_stdio_ --stdio' >"$d/stream.txt"
  printf '%s\n' "4700 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "J  the 15-char truncated form is NOT a match (comm is never the key)" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census

  # ---- Case K: unreadable provenance is a REFUSAL, never clean ---------------
  d="$(new_fixture unreadable-linux)"
  printf '%s\n' '4800 1 S /tmp/build/fulcrum serve --socket /tmp/s.sock' >"$d/stream.txt"
  printf '%s\n' '4800 UNREADABLE' >"$d/env.txt"
  expect_exit "K  a candidate whose provenance cannot be read makes the census REFUSE (linux)" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "K  it is named as unreadable, not as clean" "unreadable: 4800 fulcrum"

  d="$(new_fixture unreadable-macos)"
  printf '%s\n' '4900 1 S /tmp/build/fulcrum serve --socket /tmp/s.sock' >"$d/stream.txt"
  # The control probe reads the census's own pid, which in fixture mode carries the
  # marker; only the candidate's env is unreadable — that is the macOS shape.
  printf '%s\n' "4900 UNREADABLE" >"$d/env.txt"
  expect_exit "K  an unreadable candidate REFUSES the census (macOS arm)" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=macos CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census

  d="$(new_fixture control-macos)"
  printf '%s\n' '5000 1 S /tmp/build/fulcrum serve --socket /tmp/s.sock' >"$d/stream.txt"
  printf '%s\n' "5000 $MARKER_NAME=$marker" >"$d/env.txt"
  # CENSUS_FIXTURE_CONTROL_UNREADABLE emulates the platform refusing even the
  # census's own env read: the control probe fails and the whole census refuses
  # before it can call anything clean.
  expect_exit "K  macOS refuses when it cannot read even its OWN env (no certification on a look it could not take)" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=macos CENSUS_FIXTURE_CONTROL_UNREADABLE=1 CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "K  the refusal names the platform limit" "cannot read process environments on macOS"

  # ---- Case L: the census must never kill its own lineage -------------------
  # LIVE, and it reproduces the one shape the self-skip exists for: the census is
  # run by a parent process whose OWN image is a watched image and which carries the
  # marker. Here a copy of bash named `fulcrum` runs the census, so `ps` lists the
  # census's parent as `fulcrum` with this run's marker in its environment. Without
  # the lineage skip the census classifies its own parent as a leak and KILLS IT —
  # a guard that kills the step running it. With the skip it names the skip and
  # passes. This case fails (red) if is_self_pid is removed.
  if cp "$(command -v bash)" "$live/fulcrum" 2>/dev/null; then
    chmod +x "$live/fulcrum"
    local lineage_marker="selftest-lineage-$$-$RANDOM"
    local lineage_out="$self_test_root/lineage.txt"
    local lineage_rc=0
    # The parent is really named `fulcrum`; its child is the census.
    FULCRUM_CI_RUN_MARKER="$lineage_marker" CENSUS_PLATFORM=linux \
      "$live/fulcrum" -c "bash '$script' >'$lineage_out' 2>&1; echo \$? >'$lineage_out.rc'" </dev/null >/dev/null 2>&1 || true
    local got_lineage=0
    [[ -r "$lineage_out.rc" ]] && got_lineage="$(cat "$lineage_out.rc")"
    if [[ "$got_lineage" == "0" ]] && grep -q "the census itself" "$lineage_out" 2>/dev/null; then
      printf 'ok   L  the census skipped its own fulcrum-named parent instead of killing it\n'
    else
      printf 'FAIL L  the census did not skip its own lineage (exit %s)\n' "$got_lineage"
      printf '       output: %s\n' "$(tr '\n' '|' <"$lineage_out" 2>/dev/null || echo '<missing>')"
      self_test_failures=$((self_test_failures + 1))
    fi
    # And the parent must still be alive to say so (it already exited by now, but a
    # census that KILLED it would have reported it as a leak, which the check above
    # catches by the absence of the skip line).
  else
    printf 'FAIL L  could not plant a fulcrum-named bash (no bash to copy?)\n'
    self_test_failures=$((self_test_failures + 1))
  fi

  # ---- Case M: a LIVE zombie of a watched image is caught --------------------
  # Fixture mode cannot express this: a synthetic pid has no parent, and `ps` only
  # prints `[<truncated-name>] <defunct>` for a REAL zombie. So the self-test plants
  # one: a parent whose own image is `fake_worker_core` and which carries the marker
  # forks a child that exits and is NEVER reaped. The census must catch it — and the
  # catch cannot come from the name, because `ps` truncates the zombie's bracketed
  # name to 15 chars (`fake_worker_cor`), which the exact rule must refuse. It comes
  # from the PARENT's marker, which is what this case really pins.
  if command -v python3 >/dev/null 2>&1; then
    cat >"$live/mkzombie.py" <<'PYEOF'
import os, sys, time
exe, secs = sys.argv[1], float(sys.argv[2])
if os.fork() == 0:
    os.execv(exe, [exe, "0.1"])
# Parent deliberately never calls wait(): the exited child stays a zombie.
time.sleep(secs)
PYEOF
    local z_marker="selftest-zombie-$$-$RANDOM"
    local z_parent="$live/zparent"
    cp "$live/fake_worker_core" "$z_parent"
    # `( … & )` — a subshell — so the parent is not a job of THIS shell: killing it
    # then produces no "Killed" job-control line in the self-test output.
    ( FULCRUM_CI_RUN_MARKER="$z_marker" setsid python3 "$live/mkzombie.py" "$live/fake_worker_core" 120 \
        </dev/null >/dev/null 2>&1 & )
    sleep 1.2
    # The zombie is the child of the python parent; find it by ppid from ps rather
    # than trusting a shell $! (setsid sits in between).
    local z_parent_pid="" z_pid=""
    z_parent_pid="$(ps -eo pid=,args= | awk -v m="$live/mkzombie.py" 'index($0, m) {print $1; exit}')"
    if [[ -n "$z_parent_pid" ]]; then
      # Record it for the trap: if this case fails part-way the python parent must
      # not linger for its full 120s sleep holding a zombie.
      self_test_planted+=("$z_parent_pid")
      z_pid="$(ps -eo pid=,ppid=,stat= | awk -v p="$z_parent_pid" '$2==p && $3 ~ /^Z/ {print $1; exit}')"
    fi
    if [[ -n "$z_pid" ]]; then
      printf 'ok   M  planted a real zombie (pid %s, parent %s)\n' "$z_pid" "$z_parent_pid"
      expect_exit "M  the census catches the ZOMBIE of a watched image via its parent's marker" 1 \
        env CENSUS_PLATFORM=linux FULCRUM_CI_RUN_MARKER="$z_marker" bash "$script"
      expect_output_has "M  the zombie is named as a leak" "leak: $z_pid"
      expect_output_has "M  the report says ZOMBIE and names the parent" "ZOMBIE"
      # Killing the parent is the ONLY way a zombie is reaped. Do it, then require the
      # zombie to be actually gone — otherwise this case would leave debris behind.
      kill -KILL "$z_parent_pid" 2>/dev/null || true
      sleep 0.5
      if ps -p "$z_pid" >/dev/null 2>&1; then
        printf 'FAIL M  the zombie %s is still listed after its parent was killed\n' "$z_pid"
        self_test_failures=$((self_test_failures + 1))
      else
        printf 'ok   M  reaping the parent removed the zombie\n'
      fi
    else
      printf 'FAIL M  could not plant a zombie (python3 fork/exec failed?)\n'
      self_test_failures=$((self_test_failures + 1))
    fi
  else
    printf 'note M skipped: no python3 to plant a non-reaping parent with\n'
  fi

  if ((self_test_failures > 0)); then
    printf '\nself-test: %s case(s) FAILED — the stray-process census cannot be trusted to fail\n' "$self_test_failures" >&2
    return 1
  fi
  printf '\nself-test: all cases behaved (the census has teeth)\n'
  return 0
}

# ---------------------------------------------------------------------------
# arguments
# ---------------------------------------------------------------------------
want_self_test=0
want_census=0
want_enumerate_only=0
fixture_dir=""
platform_override=""
input_file=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --self-test)
      want_self_test=1
      shift
      ;;
    # INTERNAL to --self-test: run the census once (over the fixture seams when
    # CENSUS_MODE=fixture). The self-test re-invokes THIS file for every case so
    # the logic under test is the logic that runs in CI.
    --census)
      want_census=1
      shift
      ;;
    # INTERNAL to --self-test Case I: dump the live enumeration and exit, so the
    # self-test can prove the enumerator sees a real planted process.
    --enumerate-only)
      want_enumerate_only=1
      shift
      ;;
    # Self-test-only seams. Outside --self-test they are REJECTED: the live census
    # must always read the real process table, so no mock can sit between the
    # census and the table (Case H).
    --input)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --input requires a value (a captured candidate stream, self-test only)" >&2
        usage
        exit 1
      fi
      input_file="$2"
      shift 2
      ;;
    --input=*)
      input_file="${1#--input=}"
      shift
      ;;
    --fixture-dir)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --fixture-dir requires a value (self-test only)" >&2
        usage
        exit 1
      fi
      fixture_dir="$2"
      shift 2
      ;;
    --fixture-dir=*)
      fixture_dir="${1#--fixture-dir=}"
      shift
      ;;
    --platform)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --platform requires a value (self-test only)" >&2
        usage
        exit 1
      fi
      platform_override="$2"
      shift 2
      ;;
    --platform=*)
      platform_override="${1#--platform=}"
      shift
      ;;
    --keep-temp-dir)
      self_test_keep_temp_dir=1
      shift
      ;;
    *)
      echo "error: unexpected argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ((want_self_test == 0)); then
  if [[ -n "$input_file$fixture_dir$platform_override" || $want_census == 1 || $want_enumerate_only == 1 ]]; then
    {
      echo "error: --input / --fixture-dir / --platform / --census / --enumerate-only are for the --self-test harness only"
      echo "  the live census must read the REAL process table. A mock fed to the live path"
      echo "  would let this guard report 'clean' about a table it never looked at — the exact"
      echo "  vacuous green this family of guards exists to refuse."
    } >&2
    usage
    exit 1
  fi
fi

if ((want_self_test == 1)); then
  if ((want_census == 1)); then
    # The census, invoked by the self-test harness. The marker still comes from the
    # environment — the discriminator is never bypassed, even in fixture mode.
    marker_value="$(marker_from_env)"
    CENSUS_PLATFORM="${platform_override:-${CENSUS_PLATFORM:-linux}}"
    CENSUS_MODE="${CENSUS_MODE:-live}"
    CENSUS_FIXTURE_DIR="${fixture_dir:-${CENSUS_FIXTURE_DIR:-}}"
    CENSUS_INPUT="${input_file:-}"
    if [[ "$CENSUS_MODE" == "fixture" && -z "$CENSUS_FIXTURE_DIR" ]]; then
      echo "error: CENSUS_MODE=fixture needs CENSUS_FIXTURE_DIR" >&2
      exit 2
    fi
    census "$marker_value" "$CENSUS_PLATFORM"
    exit $?
  fi
  if ((want_enumerate_only == 1)); then
    CENSUS_PID="$$"
    resolve_lineage "$CENSUS_PID"
    enumerate_unix >"${CENSUS_DUMP_ENUM:-/dev/stdout}"
    exit $?
  fi
  run_self_test
  exit $?
fi

# ---- the live census -------------------------------------------------------
# Platform comes from the environment or from uname, NEVER from a CLI flag (a flag
# could pick a friendlier platform than the one the census is running on — Case H).
# `CENSUS_PLATFORM` is read as a seam so the self-test can exercise the windows and
# macOS arms on one machine, and it is worth stating why that is not a way to fake a
# green run: the census always enumerates the REAL process table and always demands
# the real marker, so naming another platform either fails to find the tools (no
# `tasklist` → exit 2) or reports what is genuinely running. The seam changes which
# reader is used, not whether the table is actually read.
CENSUS_PLATFORM="${CENSUS_PLATFORM:-}"
if [[ -z "$CENSUS_PLATFORM" ]]; then
  case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux) CENSUS_PLATFORM=linux ;;
    Darwin) CENSUS_PLATFORM=macos ;;
    *)
      if command -v tasklist >/dev/null 2>&1; then
        CENSUS_PLATFORM=windows
      else
        printf 'error: cannot identify this platform (uname said %s) and tasklist is absent — the census refuses rather than guess\n' "$(uname -s 2>/dev/null || echo '?')" >&2
        exit 2
      fi
      ;;
  esac
fi

marker_value="$(marker_from_env)"
census "$marker_value" "$CENSUS_PLATFORM"
exit $?

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
# macOS ENV READS WORK. THE SECTION THAT USED TO SAY OTHERWISE WAS DESCRIBING A
# BUG IN THIS SCRIPT'S OWN PARSER, AND THE BUG IS NOW FIXED.
# ─────────────────────────────────────────────────────────────────────────────
# This section used to say macOS run-scoping was UNVERIFIED and that `ps eww`
# probably cannot read another process's environment under SIP / proc_security. That
# is now KNOWN FALSE and the claim has been removed rather than softened. Measured on
# macos-latest, for the census's own marker-bearing pid:
#     ps eww -o pid=,args= -p <pid>   exit=0  bytes=5443  marker PRESENT in raw output
#     single-match extracted 1 var; multi-match found the marker (109 vars)
# The platform answered the read on the first try. What failed was the extraction:
# BSD `ps` prints argv AND THE ENTIRE ENVIRONMENT ON ONE LINE (5443 chars, ZERO
# newlines), and `env_from_ps_eww` ran ONE `[[ =~ ]]` per line, so BASH_REMATCH held
# the first env-shaped token (`GITHUB_JOB=probe`) and never reached the marker ~100
# vars later. See `ps_eww_env_vars` for the fix and Case S for the pin. Linux never
# showed it because env_from_procfs reads /proc/<pid>/environ NUL-split — one var per
# line, no regex at all — and that asymmetry is exactly why it shipped green locally.
#
# WHAT IS STILL TRUE, AND IS THE PART THAT MATTERS: the refusal posture is unchanged.
# The control probe still runs on the census's own pid before any "no marker here"
# answer is trusted, a candidate whose provenance cannot be read is still `unreadable`
# and still exits 2, and a macOS leg still cannot certify a table it could not read.
# What changed is only that "extraction found nothing" and "the platform refused the
# read" are now DIFFERENT states with different messages — the old code printed the
# SIP speech for both, which meant the one failure macOS actually hit was blamed on a
# security policy that was never involved. Fixing a misdiagnosis must not buy a
# weaker guard, so every branch added here still ends in exit 2.
#
# INVOCATION NOTES, MEASURED (do not "simplify" these):
#   * `ps eww -o pid=,args= -p <pid>` — the per-pid form — WORKS on macOS (exit 0)
#     and on Linux. This is the census's spelling and it stays.
#   * `ps eww -eo pid=,args=` (whole table) EXITS 1 with 0 BYTES on macOS: BSD `e`
#     means "all USERS", not "environment". There the env flag is `-E`, which Linux
#     rejects as `unsupported SysV option`. So there is no single whole-table spelling
#     that reads env on both platforms; per-pid is the only shared one.
#   * `ps eww -o pid= -p <pid>` (no `args=`) prints the number and NOTHING else on
#     every platform: no args column, no env in the output at all.
#   * whether the env suffix is TRUNCATED for very large environments is still
#     unmeasured; the extraction bound above turns that into a refusal
#     (`env-truncated`) rather than a silent short answer.
# The failure class these rules exist to make impossible is still a macOS leg that
# PASSES because it could not look.
#
# ─────────────────────────────────────────────────────────────────────────────
# macOS LINEAGE AND ZOMBIE REAPING: IMPLEMENTED ON BOTH ARMS, AND PROVEN ON BOTH
# ─────────────────────────────────────────────────────────────────────────────
# Two mechanisms here used to be Linux-only in disguise. `resolve_lineage` read
# `/proc/<pid>/status`, which does not exist on macOS, so on Darwin the ancestor loop
# never ran one iteration: the census's "lineage" was its own pid alone, and Case L's
# guarantee — never kill a `fulcrum`-named PARENT — did not hold there. `kill_leak`'s
# zombie-parent lookup read the same file, so on macOS it always took the "no
# reachable parent" branch and a zombie of THIS run was reported but never reaped.
# Both now go through `parent_of`, whose second arm is `ps -o ppid= -p` — the one
# spelling BSD and procps share. Self-test Case R forces that arm on Linux
# (--lineage-arm-ps) and re-runs Case L's shape through it, so the arm macOS takes is
# proven somewhere rather than asserted about a platform nobody measured.
# WHICH PROOF IS REAL, PLAINLY: every case in this harness except the ones that probe
# the LIVE `ps eww` read ran on Linux — but the macOS env read is no longer one of the
# unproven things (see the section above: it was measured working on macos-latest, and
# the failure there was this script's parser, not the platform). Case S now feeds the
# SHIPPED parser a captured one-line macOS-shaped read on any box, so the arm macOS
# takes is exercised in CI rather than asserted about a platform nobody tested. The
# lineage/zombie MECHANISM is proven on both arms via Case R. What genuinely remains
# unmeasured is narrower than what this file used to claim: whether a huge environment
# gets TRUNCATED by BSD ps, which the extraction bound turns into a refusal. What
# changed with the parent_of split is that macOS can no longer silently lose the
# parent-protection and zombie-reaping behaviour — it either works or Case R is red.
#
# ─────────────────────────────────────────────────────────────────────────────
# EXIT-CODE CONTRACT
# ─────────────────────────────────────────────────────────────────────────────
#   0  clean — the table WAS read and no live process of THIS run was left; or
#      candidates were found, proven NOT to be this run's, and left alone (the
#      count is reported to stderr and the run still passes); or a self-test whose
#      only gap is a skip its `--allow-skips <reason>` was given a stated reason for
#   1  leak — a daemon/worker of THIS run outlived the suite (unix: reported AND
#      killed; windows: reported only, advisory) — or a self-test case misbehaved,
#      or a self-test-only FLAG was used outside `--self-test` (which includes
#      `--allow-skips`, and `--allow-skips` with an empty reason), or a self-test
#      under CI=true recorded a skip that `--allow-skips <reason>` did not tolerate
#   2  the census could not answer honestly: no FULCRUM_CI_RUN_MARKER, no `ps` / no
#      `tasklist`, an enumeration it could not parse, a candidate whose PROVENANCE
#      COULD NOT BE READ, a table whose candidates ALL vanished before they could be
#      classified, or a LIVE census whose environment set a harness seam
#      (CENSUS_MODE / CENSUS_FIXTURE_DIR / CENSUS_INPUT / CENSUS_PLATFORM) — which
#      would otherwise let it read a mock and report clean. Refusing is never the
#      same as clean.
#      "PROVENANCE COULD NOT BE READ" now names FOUR states, so a refusal can be
#      diagnosed from the log without guessing: `unreadable` (the read itself failed —
#      the only one a blocked platform produces), `gone` (the pid vanished mid-read),
#      `no-env-in-output` (ps answered, and its output held no NAME=VALUE at all —
#      almost always a missing `args=` column, i.e. a bug HERE, not a policy THERE),
#      and `env-truncated` (the read was larger than the extraction bound, so the list
#      is incomplete and the marker could be past the cut). All four refuse; the
#      exit code is identical and has NOT been extended — only the reason printed
#      alongside it got honest. And the macOS control probe's own failure carries the
#      fifth state, `no-marker-in-own-env`, which after the parser fix means what it
#      literally says: the environment was read in full and did not carry the marker.
# Anything else is the tool's own failure.
#
# The seam split in two directions, deliberately: a self-test-only FLAG on the live
# path is CLI misuse (1), while a census that could have read a mock instead of the
# table cannot answer the run-scoped question at all (2).
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
#   --allow-skips <reason>
#                    INSIDE --self-test ONLY: an expected skip is reported loudly and
#                    still counted UNPROVEN, but exits 0 instead of failing under
#                    CI=true. <reason> is REQUIRED and is printed, so the CI log always
#                    says why the gap is tolerated. See the skip section below for why
#                    this is a flag and never an env var.
#
# THE SELF-TEST SKIPS, AND WHAT A SKIP COSTS. Four cases plant real processes and so
# need real tooling: I (ps -eo, a copyable sleep, setsid), M (the same, plus python3),
# L (a renamable bash), R (L's plant, plus /proc). Where a dependency is missing the
# case records itself in the skip ledger and says so on STDERR; it does NOT fail (a
# missing tool is not a census defect) and it does NOT pass (an unproven case is not a
# behaved one). Under CI=true a skip is exit 1 — on a runner an unproven case is
# indistinguishable from a deleted one. Off CI it stays a loud note. The summary line
# `skip summary: N case(s) …` prints EVERY run, including N=0, so "which cases did not
# run" is answerable from any log. Cases are gated INDIVIDUALLY: macOS has no setsid,
# so I and M skip there while L and R still run, and a blanket guard would have thrown
# away the lineage coverage that macOS genuinely can provide.
#
# WHY `--allow-skips` EXISTS, AND WHY IT IS A FLAG. `setsid` is absent BY PLATFORM
# DESIGN on macOS and on Git-for-Windows bash (measured on real runners: macOS has
# /bin/ps and no setsid; Windows has neither setsid nor pgrep). So the CI=true rule
# above would redden the macOS and Windows legs PERMANENTLY, over a gap that is not a
# defect and that the ubuntu leg already covers. That conflates two different claims:
# "the harness is misconfigured" (red, always) and "this platform cannot host this
# case by design, and another leg in the matrix proves it" (loud, tolerated). The
# ubuntu leg is where the census's teeth are proven and runs WITHOUT the flag; the
# other two legs prove their own platform-specific arms.
# It is a FLAG with a mandatory reason, not an environment variable, on purpose: the
# live-path fixture-seam bug this branch fixed (5d08d2c) was exactly a
# behaviour-selecting env var letting a guard fabricate its own verdict, and a var set
# at workflow level is no more auditable than that was. A flag appears in the step's
# own command line, so the CI log shows the tolerance was granted and by what
# argument. Tolerating a gap never proves it: the ledger, the UNPROVEN summary and the
# refusal to print "all cases behaved" are all unchanged. Case T pins both postures.
#
# The live census takes no input options at all. `--input`/`--fixture-dir`/
# `--platform`/`--census`/`--enumerate-only`/`--meta-skip`/`--lineage-arm-ps`/
# `--ps-env-parse`/`--allow-skips` exist ONLY inside `--self-test` and are rejected
# otherwise (exit 1):
# the live path must never read a mock instead of the real process table. That is the
# flag half of the vacuity floor, and self-test Case H pins it (Case S pins the same
# floor for `--ps-env-parse`, which reads a file too).
# The ENVIRONMENT half is the same floor, and it used to be missing: CENSUS_MODE /
# CENSUS_FIXTURE_DIR / CENSUS_INPUT / CENSUS_PLATFORM reach the same fixture seam from
# the environment, and were ungated on the live path — so
# `CENSUS_MODE=fixture CENSUS_FIXTURE_DIR=<an empty dir>` printed
# `clean: the process table was read; 0 candidate(s)` and exited 0 about a table that
# does not exist. The live census now refuses those (exit 2) and names the variable;
# self-test Case H2 pins it, and Case N pins the companion rule that a fixture table
# which is missing or empty is a refusal rather than a clean read.

set -euo pipefail

# Captured BEFORE any assignment below can clobber them. This matters: `CENSUS_INPUT`
# has a top-level initializer (`CENSUS_INPUT=""`) that WIPES the inherited value, so a
# gate that ran after it could not see that the environment had set the seam at all.
# The gate below reads these copies and nothing else.
INHERITED_CENSUS_MODE="${CENSUS_MODE:-}"
INHERITED_CENSUS_FIXTURE_DIR="${CENSUS_FIXTURE_DIR:-}"
INHERITED_CENSUS_INPUT="${CENSUS_INPUT:-}"
INHERITED_CENSUS_PLATFORM="${CENSUS_PLATFORM:-}"

usage() {
  echo "usage: $(basename "$0") [--self-test] [--keep-temp-dir]" >&2
  echo "       $(basename "$0") --self-test [--allow-skips <reason>]   (harness only)" >&2
}

# ---------------------------------------------------------------------------
# the fixture seam is a --self-test-only door — ENVIRONMENT form
# ---------------------------------------------------------------------------
# `--input`/`--fixture-dir`/`--platform` are rejected outside `--self-test` (Case H).
# Those four FLAGS were the only door that got gated: census() and load_env() also
# read CENSUS_MODE / CENSUS_FIXTURE_DIR / CENSUS_INPUT / CENSUS_PLATFORM straight out
# of the environment, so the LIVE census stayed reachable through a mock. Measured
# before this gate existed:
#   FULCRUM_CI_RUN_MARKER=x CENSUS_MODE=fixture CENSUS_FIXTURE_DIR=<an empty dir> \
#     CENSUS_PLATFORM=linux  bash scripts/check-no-stray-processes.sh
#     → clean: the process table was read; 0 candidate(s) …        exit 0
# i.e. a census printing "the process table was read" about a table that does not
# exist. `CENSUS_MODE=live` is the honest spelling — it asks for the real table — so
# it is allowed; any other value, and any value at all for the other three, REFUSES
# and names the variable. Exit 2, not 1: this is the "could not answer honestly"
# class (same as a missing marker), and a census that never looked is not the same
# thing as a leak. The FLAGS keep exit 1 — those are CLI misuse, pinned by Case H.
refuse_self_test_only_env_seams() {
  local offenders="" name value
  for name in CENSUS_MODE CENSUS_FIXTURE_DIR CENSUS_INPUT CENSUS_PLATFORM; do
    # Read the INHERITED_* copies, never the live names: `CENSUS_INPUT` is blanked by
    # a top-level initializer above, so by the time this runs the live name no longer
    # tells you whether the environment set it.
    case "$name" in
      CENSUS_MODE)         value="$INHERITED_CENSUS_MODE" ;;
      CENSUS_FIXTURE_DIR)  value="$INHERITED_CENSUS_FIXTURE_DIR" ;;
      CENSUS_INPUT)        value="$INHERITED_CENSUS_INPUT" ;;
      CENSUS_PLATFORM)     value="$INHERITED_CENSUS_PLATFORM" ;;
    esac
    [[ -z "$value" ]] && continue
    [[ "$name" == "CENSUS_MODE" && "$value" == "live" ]] && continue
    offenders="${offenders}    $name=$value"$'\n'
  done
  if [[ -n "$offenders" ]]; then
    {
      echo "error: the LIVE census refuses (exit 2) — its environment sets a harness seam:"
      printf '%s' "$offenders"
      echo "  these variables choose WHICH reader the census uses — a mock process table, a"
      echo "  chosen platform — instead of whether the real table is read, so they are"
      echo "  honoured ONLY inside the --self-test harness (which re-invokes this script with"
      echo "  --self-test --census). On the live path they are refused, because a census that"
      echo "  claims it 'read the process table' about a fixture directory — or about a"
      echo "  directory that holds no table at all — is the exact unearned green this guard"
      echo "  exists to refuse."
      echo "  to exercise the fixture seam:  bash $(basename "$0") --self-test"
      echo "  to run the real census:         unset CENSUS_MODE CENSUS_FIXTURE_DIR CENSUS_INPUT CENSUS_PLATFORM"
    } >&2
    return 2
  fi
  return 0
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
#
# parent_of <pid> — the parent, by whichever mechanism this platform offers.
# Prints the ppid and returns 0; prints NOTHING and returns 1 when there is no
# parent to name (pid 0, a vanished pid, or a `ps` that would not answer).
#
# TWO ARMS, and the reason for the second is a correctness bug, not portability
# theatre. `/proc/<pid>/status` does not exist on macOS, and this code used to read
# ONLY that file: on Darwin the ancestor loop never ran a single iteration, so the
# "lineage" was just the census's own pid — which means Case L's guarantee (never
# kill a `fulcrum`-named PARENT) did not hold on the platform this repo runs a macOS
# leg on. `ps -o ppid= -p <pid>` is the one spelling both BSD ps and procps accept,
# and it is what the lookup now falls back to. A wrong kill on macOS is worse than a
# slow lineage walk, so the fallback is the default there rather than a documented
# limitation. The procfs arm stays first because it is a single file read where `ps`
# is a process spawn, and because `PPid:` is exact even for a pid `ps` would skip.
#
# Which arm runs is chosen by CENSUS_LINEAGE_ARM below, and ONLY by the self-test flag
# --lineage-arm-ps — never by the environment. A census whose lineage lookup can be
# selected by its own environment is a census that can be told not to protect its own
# parent, which is the exact failure Case L exists to catch.
#
# `procfs` (read /proc/<pid>/status) is the default because it is one file read; `ps`
# forces the portable `ps -o ppid= -p` arm, which is the ONLY arm that exists on
# macOS. Forcing the `ps` arm on Linux is how the self-test proves the arm macOS takes
# really walks ancestors — see Case R.
CENSUS_LINEAGE_ARM="procfs"

parent_of() {
  local pid="$1" ppid
  if [[ "$CENSUS_LINEAGE_ARM" == "procfs" && -r "/proc/$pid/status" ]]; then
    # `PPid:` — capital P twice. `awk` regexes are case-sensitive, and `/^Ppid:/`
    # silently matches NOTHING, which leaves the lineage as "just my own pid" and
    # lets the census kill its own parent. Self-test Case L is what catches that.
    ppid="$(awk '/^PPid:/{print $2; exit}' "/proc/$pid/status" 2>/dev/null || true)"
  else
    ppid="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)"
  fi
  [[ "$ppid" =~ ^[0-9]+$ ]] || return 1
  # 0 means "no parent" (init's own parent). Putting it in the skip set would park a
  # pid that nothing can own in the set that decides what never gets killed.
  ((ppid > 0)) || return 1
  printf '%s' "$ppid"
}

resolve_lineage() {
  local pid="$1" next guard=0
  CENSUS_LINEAGE=" $1 "
  while ((guard < 64)); do
    next="$(parent_of "$pid")" || break
    CENSUS_LINEAGE="$CENSUS_LINEAGE$next "
    pid="$next"
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
#                         emulates a platform that blocks the read, and a line
#                         `<pid> GONE` emulates the pid exiting between being listed
#                         and being read — the two answers are NOT interchangeable:
#                         unreadable is a refusal, gone is a declined candidate, and
#                         without the second spelling the race had no coverage)
#   CENSUS_MODE=live      Linux: /proc/<pid>/environ (readable by a same-user
#                         sibling when ptrace_scope=0; FAILS with ENOENT for a
#                         zombie, which ps still lists — measured).
#                         macOS: `ps eww -o pid=,args= -p <pid>` — MEASURED WORKING on
#                         macos-latest (exit 0, 5443 bytes, marker present); BSD puts
#                         argv AND the whole environment on ONE line, which is what
#                         `ps_eww_env_vars` exists to parse. A read that yields no
#                         NAME=VALUE text at all is `no-env-in-output` and a read past
#                         the extraction bound is `env-truncated`; both are REFUSALS,
#                         i.e. the conservative direction — refuse, never certify —
#                         but neither is reported as a blocked platform any more,
#                         because neither one is.
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
  #
  # CENSUS_FIXTURE_CONTROL_STATE=<state> pins a SPECIFIC control-probe failure
  # instead of the one generic `unreadable`. It exists because the refusal MESSAGE now
  # branches on the state, and each branch carries a different diagnosis: the old text
  # blamed SIP / proc_security for every one of them, including the branch the macOS
  # runner actually hit (`no-marker-in-own-env`, where nothing was blocked). Without
  # this knob no case can reach those branches — the live macOS reader cannot run on
  # Linux — and a message nobody can test is a message that rots.
  if [[ -n "${CENSUS_FIXTURE_CONTROL_STATE:-}" && "$pid" == "$CENSUS_PID" ]]; then
    ENV_STATE="$CENSUS_FIXTURE_CONTROL_STATE"
    return 1
  fi
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
    # A GONE record emulates the race the live arm hits for real: the pid exited
    # between being listed and having its env read (`env_from_procfs` sets
    # ENV_STATE=gone when /proc/<pid>/environ is gone but `ps` had listed it). It is
    # NOT the same answer as UNREADABLE — unreadable is a refusal, gone is a declined
    # candidate — so the fixture needs both spellings or the race has no coverage.
    if [[ "$rest" == "GONE" ]]; then
      ENV_STATE="gone"
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

# How many NAME=VALUE fields one `ps eww` read may yield before the extraction gives
# up. A defensive bound ONLY: a real CI environment is ~100 vars (measured on
# macos-latest: 109). The bound exists so a pathological line cannot occupy the census,
# which runs at the END of a CI job where a hang is worse than a refusal.
# Reaching it is a REFUSAL (`env-truncated`), never a shorter answer — a list that was
# cut off could have held the marker further along, and reporting "not this run's" from
# a truncated list is the same false clean this cap exists to prevent.
ENV_PS_MAX_VARS=4096

# ps_eww_env_vars <text> — pull every `NAME=VALUE` field out of one `ps eww` read.
# Result lands in ENV_PS_VARS (one `NAME=VALUE` per line) and, if the bound was hit
# while fields were still unread, ENV_PS_TRUNCATED=1. Both are GLOBALS on purpose: a
# function cannot return a list and a status at once, and reading the list through
# `$(…)` would run it in a subshell, where the truncation flag could not reach the
# caller — a refusal that silently fails to propagate is the worst kind of lie.
#
# WHY THIS IS A LOOP AND NOT ONE MATCH (the bug this replaces). macOS/BSD `ps` prints
# argv AND THE ENTIRE ENVIRONMENT ON ONE LINE — measured on macos-latest: 5443 chars,
# ZERO newlines, marker present in the raw output. The old body ran a single
# `if [[ "$line" =~ … ]]` per line and read BASH_REMATCH, which holds only the FIRST
# match, so on a 5 KB single-line read it captured `GITHUB_JOB=probe` (the first
# env-shaped token) and never reached the marker ~100 vars later. The census then
# reported `no-marker-in-own-env` for its OWN marker-bearing pid and refused, blaming
# SIP. Linux never showed this because env_from_procfs reads
# `/proc/<pid>/environ` NUL-split — one var per line, no regex at all — and that
# asymmetry is exactly why this shipped green locally.
#
# WHY THE LOOP SPLITS ON WHITESPACE INSTEAD OF RE-MATCHING THE REST. The obvious
# exhaustion loop is `rest="${rest#*"$match"}"` — and NOT `rest="${rest#"$match"}"`,
# which strips only a PREFIX: the match sits mid-line, `rest` never shrinks, and the
# loop spins forever (that is the draft that had to be killed at a 120s timeout). The
# `#*` form is CORRECT — it returns the identical 109-var list — but it is quadratic
# here, because an unanchored `[[:space:]]NAME=` match re-scans the whole remainder on
# every single extraction. Measured on a 52 KB single line, both spellings, same 4096
# bound: strip-with-`#*` 14s, split-then-anchored-test 0.23s. Both agree on the real
# 6.1 KB macOS fixture (109 vars, marker found, 15ms vs 42ms), so this is purely about
# what happens when the bound is the thing being tested — and a bound that costs
# seconds-to-minutes to reach is not the hang-proof it was added to be, on a script
# that runs at the END of a CI job.
#
# THE ARGV HALF OF THE RULE IS PRESERVED, AND IT IS LOAD-BEARING. Field 0 is `ps`'s pid
# column, so the scan starts at 1, and a field only counts when it starts with
# `[A-Za-z_]` and then has its `=`: `--worker`, `--session=deadbeef`, `--exe=/bin/true`
# and `--flag=notenv` all start with `-` and are therefore NOT env, so multi-extraction
# cannot invent environment out of argv. Self-test Case S pins that (an argv-only read
# must yield ZERO vars); a parser that dropped the leading-character rule would start
# reading the daemon's `--worker-exe` argument as an environment entry.
#
# TWO LIMITS THIS SHARES WITH THE CODE IT REPLACES (neither is new, both are worth
# naming rather than discovering later):
#   * a VALUE containing whitespace is cut at the whitespace — `PATH=/a b:/c` yields
#     `PATH=/a`. The old `([^[:space:]]*)` stopped at whitespace too, so this is not a
#     regression, and the marker's value (`run_id` + `run_attempt`) has none.
#   * an argv token that really does look like `NAME=value` (a `make FOO=bar` style
#     command line) is picked up as an env entry, exactly as the old single-match form
#     picked it up when that token came first. So multi-extraction does not widen THIS
#     risk — it widens the list, not the rule. The one way it could matter is a process
#     whose argv spells out `FULCRUM_CI_RUN_MARKER=<this run's exact value>`: it would
#     be treated as ours. That was already true whenever such a token came first, and a
#     process holding this run's marker value in its command line is not obviously
#     somebody else's — but if a spawn path ever builds such a command line, the census
#     would call it a leak, and that is the case to look at first.
ps_eww_env_vars() {
  local text="$1"
  # `ps` may hand back several lines (Linux does; macOS does not — see below). Fold
  # newlines to spaces so ONE split covers both shapes, keeping the tolerance the old
  # line-by-line loop had. `read -ra` stops at the first newline, so this fold is what
  # makes the multi-line (Linux-shaped) read work at all, not a cosmetic normalisation.
  text="${text//$'\n'/ }"
  # `local -a x=()` is a known bash 3.1 wart (the () can be taken as a word); the
  # file's own idiom everywhere else is a plain `local x=()`, which is what macOS's
  # bash 3.2 accepts too.
  local fields=()
  local out=()
  local field
  local guard=0
  local i=1
  local n
  ENV_PS_VARS=""
  ENV_PS_TRUNCATED=0
  IFS=$' \t' read -ra fields <<<"$text"
  n=${#fields[@]}
  while ((i < n)); do
    field="${fields[$i]}"
    if [[ "$field" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
      out+=("${BASH_REMATCH[1]}=${BASH_REMATCH[2]}")
      guard=$((guard + 1))
      if ((guard >= ENV_PS_MAX_VARS)); then
        # Stop, but only call it TRUNCATED if there were still fields after this one
        # to read. Exactly-at-the-bound-with-nothing-left is a complete answer.
        ((i + 1 < n)) && ENV_PS_TRUNCATED=1
        break
      fi
    fi
    i=$((i + 1))
  done
  ENV_PS_VARS="$(printf '%s\n' ${out[@]+"${out[@]}"})"
}
ENV_PS_VARS=""
ENV_PS_TRUNCATED=0

env_from_ps_eww() {
  local pid="$1" raw
  ENV_LINES=()
  ENV_PS_TRUNCATED=0
  # The `args=` column is what makes the env visible: `ps eww -o pid= -p <pid>`
  # prints the number and nothing else on every platform (measured on Linux), so a
  # census written with that spelling would find no marker anywhere and certify
  # clean on a look it never took.
  #
  # DO NOT "fix" this invocation. `ps eww -eo pid=,args=` (the whole table) exits 1
  # with ZERO bytes on macOS, because BSD `e` means "all USERS", not "environment" —
  # there the env flag is `-E`, which Linux rejects as `unsupported SysV option`. The
  # per-pid form below is the spelling that works on BOTH (measured on macOS: exit 0,
  # 5443 bytes, marker present). The bug was in the parsing, never in the call.
  if ! raw="$(ps eww -o pid=,args= -p "$pid" 2>/dev/null)"; then
    if ! ps -p "$pid" >/dev/null 2>&1; then
      ENV_STATE="gone"
    else
      ENV_STATE="unreadable"
    fi
    return 1
  fi
  local extracted
  ps_eww_env_vars "$raw"
  extracted="$ENV_PS_VARS"
  if ((ENV_PS_TRUNCATED == 1)); then
    # The read was bigger than the bound, so the list we hold is INCOMPLETE and the
    # marker could be past the cut. Refuse: "we stopped looking" is not "it was not
    # there", and a census that calls a process not-this-run on a truncated read is
    # reporting clean about a look it did not finish.
    ENV_STATE="env-truncated"
    return 1
  fi
  if [[ -z "$extracted" ]]; then
    # `ps` ANSWERED, and what it answered with held no NAME=VALUE text at all. That is
    # NOT the same event as the platform refusing the read, and saying so matters: the
    # usual cause is the invocation losing the `args=` column (no args column, no env
    # in the output), which is a bug in THIS script, not a macOS security policy. The
    # direction is unchanged — still a refusal, still exit 2 upstream — only the reason
    # it names is now the reason it can act on.
    ENV_STATE="no-env-in-output"
    return 1
  fi
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] && ENV_LINES+=("$line")
  done <<<"$extracted"
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
GONE_COUNT=0

parse_candidate_stream() {
  local marker="$1" platform="$2"
  LEAK_PIDS=()
  LEAK_KINDS=()
  LEFT_ALONE_COUNT=0
  UNREADABLE_COUNT=0
  CANDIDATE_COUNT=0
  COLLISION_COUNT=0
  GONE_COUNT=0

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
        # The reason is spelled OUT per state, and that distinction is a bug fix, not
        # decoration. Every failure of this probe used to print the same SIP /
        # proc_security speech, including the one case where SIP was innocent: the
        # single-match extractor (see ps_eww_env_vars) could only ever see the FIRST
        # NAME=VALUE on BSD's one-line read, so the census's own marker — ~100 vars in
        # — was invisible and the probe answered `no-marker-in-own-env` about an
        # environment it had read perfectly well. Measured on macos-latest: exit 0,
        # 5443 bytes, marker PRESENT in the raw output, 1 var extracted. macOS was
        # never blocking anything, and the message blamed it confidently. "The platform
        # refused the read" and "the read came back and extraction found nothing" are
        # now different sentences with different fixes.
        {
          echo "error: the macOS control probe failed, so this census REFUSES (exit 2) rather than certify clean"
          echo "  it read its OWN pid ($CENSUS_PID, which must carry $MARKER_NAME) and could not confirm the marker."
          echo "  control probe state: $CONTROL_STATE"
          case "$CONTROL_STATE" in
            unreadable)
              echo "  MEANING: the env READ itself failed — 'ps eww -o pid=,args= -p <pid>' exited non-zero while"
              echo "  the pid was still alive. THAT is the shape a blocked read takes (SIP / proc_security is one"
              echo "  candidate cause). Check the invocation before suspecting the OS: 'ps eww -eo pid=,args=' (the"
              echo "  whole-table form) exits 1 with 0 bytes on macOS, because BSD 'e' means ALL USERS, not"
              echo "  environment; the per-pid form is the one that works there."
              ;;
            gone)
              echo "  MEANING: the census's own pid was not in the table at all — the read raced its own process."
              echo "  Nothing was blocked; the census just could not look at itself."
              ;;
            no-env-in-output)
              echo "  MEANING: 'ps' ANSWERED (exit 0) and its output held NO NAME=VALUE text at all. This is NOT a"
              echo "  platform block — it is almost always the invocation losing the 'args=' column, since with no"
              echo "  args column BSD/Linux ps puts NO environment in the output whatsoever. Fix the read; do not"
              echo "  go looking for a security policy that was never involved."
              ;;
            env-truncated)
              echo "  MEANING: the read came back LARGER than the extraction bound ($ENV_PS_MAX_VARS NAME=VALUE"
              echo "  fields) and the scan stopped early, so the marker could sit past the cut. The census will not"
              echo "  call a process not-this-run on a read it did not finish. Raise ENV_PS_MAX_VARS if a real CI"
              echo "  environment ever exceeds it (measured normal size: 109 vars)."
              ;;
            no-marker-in-own-env)
              echo "  MEANING: the environment WAS read, every NAME=VALUE in it was extracted, and none of them was"
              echo "  this run's $MARKER_NAME. With the multi-extraction fix in ps_eww_env_vars this is no longer"
              echo "  what a large macOS environment looks like — it now means the census process genuinely does not"
              echo "  carry the marker it was started with, i.e. the marker was not exported into this step's"
              echo "  environment, or something between the step and this process cleared it."
              ;;
            *)
              echo "  MEANING: unrecognised probe state — treat the whole macOS leg as unproven."
              ;;
          esac
          echo "  either way the direction is unchanged: a census that cannot confirm its OWN marker would be"
          echo "  guessing on every candidate, and a census that passes because it could not look is the exact"
          echo "  failure this guard exists to prevent."
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
          # The race, named rather than swallowed: this pid was one of our images when
          # it was listed, and it was gone by the time its env could be read. Whether
          # it was THIS run's is now unknowable, so it is neither a leak nor proof of
          # cleanliness — it is declined, and counted.
          printf 'gone-before-read: %s %s (exited between listing and reading — not a leak at census time)\n' "$pid" "$image" >&2
          GONE_COUNT=$((GONE_COUNT + 1))
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

  # A stream whose ONLY candidates vanished is not a clean table. Every row of it was
  # one of our images and the census could not resolve a single one, so "0 leaks" here
  # means "nothing was knowable", not "nothing leaked". The census read the table and
  # still cannot answer the question it exists to answer, so it REFUSES — the same
  # direction it takes for an unreadable candidate. A vanished pid next to any
  # resolved row is fine: those rows carry the verdict.
  if ((GONE_COUNT > 0 && LEFT_ALONE_COUNT == 0 && COLLISION_COUNT == 0 && ${#LEAK_PIDS[@]} == 0)); then
    printf 'error: every candidate in this table vanished before its provenance could be read (%s of them) — the census could not classify them, and "I could not tell" is not "nothing leaked"; REFUSING (exit 2)\n' "$GONE_COUNT" >&2
    return 2
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
    # Same two-arm lookup as the lineage (see parent_of). This one used to read
    # /proc/<pid>/status alone, so on macOS it always hit the "no reachable parent"
    # branch — meaning a zombie of THIS run was reported and left in the table on the
    # platform the census claims to enforce on. Signalling the parent is the ONLY
    # action that clears a zombie, so a lookup that cannot answer is not a small gap.
    if ! ppid="$(parent_of "$pid")" || ((ppid <= 1)); then
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
    # An EMPTY fixture table is not a table. `parse_candidate_stream` counts rows, so
    # a zero-byte stream answers "0 candidate(s)" and the census prints clean — a
    # fixture dir that was never populated certifies the run. The windows arm already
    # refuses this shape ("an empty process table is not evidence"); the unix arm must
    # hold the same floor, or Case A's clean verdict is only accidentally real.
    if ! grep -Eqv '^[[:space:]]*(#|$)' -- "$fixture_stream" 2>/dev/null; then
      printf 'error: fixture stream %s holds no rows at all — an empty process table is not evidence, and the census REFUSES (exit 2) rather than report clean about a table that was never planted\n' "$fixture_stream" >&2
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
    printf "clean: the process table was read; %s candidate(s) of the watched images, 0 of them carrying this run's %s (%s left alone as somebody else's, %s 15-char comm collision(s) declined, %s gone before their env could be read)\n" \
      "$CANDIDATE_COUNT" "$MARKER_NAME" "$LEFT_ALONE_COUNT" "$COLLISION_COUNT" "$GONE_COUNT"
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
# Every case that could NOT run is recorded here, never silently forgiven. A skipped
# case is an UNPROVEN case, and "unproven" is not a synonym for "behaved" — see the
# self_test_skip / self_test_verdict pair and Case O.
self_test_skips=""
self_test_root=""
self_test_keep_temp_dir=0
self_test_planted=()

# Are we on a CI runner? GitHub Actions sets CI=true on every runner. Deliberately
# reads CI and nothing else — this script's whole run-scoping stance is that it reads
# no GITHUB_*/RUNNER_* variable, and CI is the one generic signal that carries the
# meaning we need here ("nobody is watching; a silent gap must go red").
is_ci() {
  case "${CI:-}" in
    [Tt][Rr][Uu][Ee] | 1 | [Yy][Ee][Ss]) return 0 ;;
    *) return 1 ;;
  esac
}

# self_test_skip <case> <why> — record a case that could not be run and shout it.
# The note goes to STDERR, never stdout: a note about missing coverage must not
# arrive disguised as another passing line in the case log.
self_test_skip() {
  local case_name="$1" why="$2"
  self_test_skips="${self_test_skips}${case_name}	${why}
"
  printf 'NOTE %s SKIPPED (unproven): %s\n' "$case_name" "$why" >&2
}

# The verdict. Prints the skip ledger whenever anything was skipped, and refuses to
# claim "all cases behaved" while any case is unproven. Under CI a skip is a FAILURE:
# on a runner, an unproven case is indistinguishable from a case that was deleted,
# and the whole point of this harness is that it cannot be quietly disarmed. Off CI it
# stays a loud note so a developer without the tool a case needs can still run it.
# The ONE exception is `--allow-skips <reason>`: a skip the caller has stated a reason
# for under CI exits 0 while still printing the ledger, the UNPROVEN count and the
# reason, and still never printing the all-clear. Tolerated is not proven — see the
# header's WHY `--allow-skips` EXISTS section and Case T.
# Returns the exit code the self-test must have.
self_test_verdict() {
  local skip_count=0 case_name why names=""
  if [[ -n "$self_test_skips" ]]; then
    # The ledger is one "<case>\t<why>" per line. The empty record the trailing
    # newline produces is skipped rather than trimmed with a nested ANSI-C pattern,
    # which is the more portable spelling on bash 3.1 (the macOS runner's shell).
    while IFS=$'\t' read -r case_name why; do
      [[ -z "$case_name" ]] && continue
      skip_count=$((skip_count + 1))
      names="${names:+$names, }$case_name"
      printf 'skipped: %s — %s\n' "$case_name" "$why" >&2
    done <<<"$self_test_skips"
  fi

  # Always printed, including the zero case: "which cases did NOT run" is a question
  # whose answer must be visible in every log, not only when someone remembers to ask.
  if ((skip_count == 0)); then
    printf 'skip summary: 0 case(s) skipped — every case ran\n' >&2
  else
    printf 'skip summary: %s case(s) skipped and therefore UNPROVEN: %s\n' "$skip_count" "$names" >&2
  fi

  if ((self_test_failures > 0)); then
    printf '\nself-test: %s case(s) FAILED — the stray-process census cannot be trusted to fail\n' "$self_test_failures" >&2
    return 1
  fi

  if ((skip_count > 0)); then
    if is_ci && [[ -z "$allow_skips_reason" ]]; then
      printf '\nself-test: %s case(s) SKIPPED and therefore UNPROVEN — FAILING under CI=true\n' "$skip_count" >&2
      printf '  a skipped case is not a passed case: on a runner this is exactly how a disarmed\n' >&2
      printf '  guard looks — the suite is green because the case that could have gone red did\n' >&2
      printf '  not run. Install what the skipped case needs, or run it where it can run.\n' >&2
      printf '  if this platform genuinely CANNOT host the case named above and another matrix\n' >&2
      printf '  leg proves that path, say so out loud: pass --allow-skips "<why>" to the\n' >&2
      printf '  --self-test invocation. It stays in the log; an env var would not.\n' >&2
      return 1
    fi
    if is_ci; then
      # Reached only via --allow-skips. The gap is TOLERATED, never forgiven: the
      # ledger above already printed every skip and the summary above already called
      # them UNPROVEN, and the all-clear line below is still unreachable.
      printf '\nself-test: %s case(s) SKIPPED and therefore UNPROVEN — tolerated by --allow-skips\n' "$skip_count" >&2
      printf '  reason given: %s\n' "$allow_skips_reason" >&2
      printf '  tolerated is NOT proven: the paths named above were not exercised on THIS leg.\n' >&2
      printf '  they must be proven by whichever matrix leg CAN host them; if this is that leg,\n' >&2
      printf '  the flag is wrong here and the guard has just been weakened silently.\n' >&2
      return 0
    fi
    printf '\nself-test: %s case(s) SKIPPED — those paths are UNPROVEN (not a pass; not failing off CI)\n' "$skip_count" >&2
    printf '  the census was NOT shown to be able to fail on the skipped path(s) named above.\n' >&2
    return 0
  fi

  printf '\nself-test: all cases behaved (the census has teeth)\n'
  return 0
}

# plant_was_alive <pid> — has this pid been OBSERVED in the process table?
#
# This exists because of a vacuous pass, not for style. The old Case I asserted "the
# planted process was killed by the census" with `ps -p <pid>` failing — and `ps -p`
# fails just as hard for a pid that NEVER EXISTED. On macOS `setsid` is not installed,
# so the plant never started, `ps -p` failed immediately, and the case reported the
# census had successfully killed the process it had failed to start. A case that never
# started its subject must not be able to grade the subject's death, so every
# "it was killed / it was untouched" assertion below is gated on having seen the thing
# alive FIRST. Same standard Case M's arrival stamp holds: observe the precondition,
# do not infer it from absence.
#
# Polls rather than probing once, because the plant is a fork+exec racing this shell —
# a single probe can miss a process that is genuinely coming.
plant_was_alive() {
  local pid="$1" attempt=0
  [[ -n "$pid" ]] || return 1
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  while ((attempt < 30)); do
    if ps -p "$pid" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 0.1
  done
  return 1
}

# unix_enumeration_works — does the exact `ps` spelling enumerate_unix uses actually
# answer HERE? This is a CAPABILITY probe, not an opinion about the platform: the
# live-census cases (I, L, R, M) are all unix-shaped, and on Git-Bash/MSYS the cygwin
# `ps` exists but `ps -eo …` EXITS 1, so `command -v ps` alone would let every one of
# them run against an empty table and grade a look that was never taken.
unix_enumeration_works() {
  ps -eo pid=,ppid=,stat=,comm=,args= >/dev/null 2>&1
}

# live_census_can_answer — can the LIVE census actually reach a verdict HERE?
# The lineage cases (L, R) assert the census EXITS 0 and names what it skipped. That
# assertion is only gradeable on a platform where the census can read provenance at
# all: on a macOS leg whose env reads really are blocked the census exits 2 — correct
# behaviour for the census, but it would make L red for a reason L has nothing to do
# with, which is exactly the misattribution this branch just fixed for the message.
# So probe it with a throwaway marker (nothing in the table can carry it, so the
# answer is 0 or 2 and never 1) and skip rather than fail when the census cannot look.
live_census_can_answer() {
  local probe_marker="selftest-capability-$$-$RANDOM"
  local rc=0
  env -u CENSUS_MODE -u CENSUS_FIXTURE_DIR -u CENSUS_INPUT -u CENSUS_PLATFORM \
    FULCRUM_CI_RUN_MARKER="$probe_marker" bash "$script" >/dev/null 2>&1 || rc=$?
  ((rc == 0))
}

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

# The negative half of the above. Some claims are about what the census must NEVER
# be able to print — "clean", "all cases behaved" — and an exit code alone cannot
# show that a forbidden sentence was absent.
expect_output_lacks() {
  local what="$1" needle="$2"
  if [[ "$LAST_OUTPUT" == *"$needle"* ]]; then
    printf 'FAIL %s\n' "$what"
    printf '       expected output NOT to contain: %s\n' "$needle"
    printf '       output: %s\n' "${LAST_OUTPUT:-<empty>}"
    self_test_failures=$((self_test_failures + 1))
  else
    printf 'ok   %s\n' "$what"
  fi
}

# Wiring pins, for behaviour a subprocess cannot be made to exhibit on this machine.
# Case M only skips when python3 is ABSENT, so Case O cannot force it by running the
# harness — it pins the wiring instead: the branch that used to print a silent stdout
# note must now register with the ledger, and the old note must be gone.
expect_file_has() {
  local what="$1" file="$2" needle="$3"
  if grep -qF -- "$needle" "$file" 2>/dev/null; then
    printf 'ok   %s\n' "$what"
  else
    printf 'FAIL %s\n' "$what"
    printf '       expected %s to contain: %s\n' "$(basename "$file")" "$needle"
    self_test_failures=$((self_test_failures + 1))
  fi
}

expect_file_lacks() {
  local what="$1" file="$2" needle="$3"
  if grep -qF -- "$needle" "$file" 2>/dev/null; then
    printf 'FAIL %s\n' "$what"
    printf '       expected %s NOT to contain: %s\n' "$(basename "$file")" "$needle"
    self_test_failures=$((self_test_failures + 1))
  else
    printf 'ok   %s\n' "$what"
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

  # ── CAPABILITY PROBES, run FIRST, before any case ──────────────────────────
  # These are hoisted above every case because the need is not limited to the live
  # plant cases. Case H2 asserts `CENSUS_MODE=live` exits 0 — an assertion about what
  # the census ACHIEVES, not about which reader it picked — and on a `ps` that cannot
  # enumerate (Git-Bash) the honest answer is 2, so H2 went red for the machine's
  # limitation. Same vacuity class this branch fixes everywhere else: a case may only
  # assert an OUTCOME on a platform that can produce one.
  local live="$self_test_root/live" bin
  mkdir -p "$live"
  for bin in fake_worker_core fulcrum; do
    if cp "$(command -v sleep 2>/dev/null || echo /nonexistent)" "$live/$bin" 2>/dev/null; then
      chmod +x "$live/$bin"
    fi
  done
  local cap_ps=1 cap_setsid=1 cap_plant=1 cap_live_env=1 cap_proc_env=1 cap_python3=1
  unix_enumeration_works || cap_ps=0
  command -v setsid >/dev/null 2>&1 || cap_setsid=0
  command -v python3 >/dev/null 2>&1 || cap_python3=0
  [[ -x "$live/fake_worker_core" ]] || cap_plant=0
  # Can the LIVE census read provenance here at all? Where it cannot, the live cases
  # must SKIP (the census is right to refuse; the case is just not gradeable) rather
  # than FAIL — see live_census_can_answer.
  live_census_can_answer || cap_live_env=0
  # Case R forces `--platform linux`, so its census reads /proc/<pid>/environ whatever
  # the host really is — a DIFFERENT dependency from cap_live_env (which asks about the
  # host's own reader). With Bug A fixed a macOS leg can now answer for itself, so
  # without this probe R would run its linux-shaped census on a machine with no /proc
  # and go red for having no Linux procfs.
  [[ -r "/proc/$$/environ" ]] || cap_proc_env=0
  # Whether `$live/fulcrum` ends up being a real BASH (Case L's parent) — Case R
  # depends on L's plant, and `-x` alone cannot tell a bash copy from the sleep copy
  # the loop above left there, so L sets this itself.
  local live_fulcrum_is_bash=0
  # A `ps` that cannot enumerate makes every live case ungradeable by construction: the
  # plant is unobservable and the census reads nothing. This is the Windows/Git-Bash
  # shape — cygwin's `ps` EXISTS, so `command -v ps` passes, and `ps -eo …` exits 1.
  local cap_enumerable=1
  ((cap_ps == 0)) && cap_enumerable=0
  ((cap_live_env == 0)) && cap_enumerable=0
  local cap_why=""
  ((cap_ps == 0)) && cap_why="ps -eo pid=,ppid=,stat=,comm=,args= does not answer here (the cygwin ps on Git-Bash exits 1)"
  ((cap_live_env == 0)) && cap_why="${cap_why:+$cap_why; }the live census cannot read process provenance on this platform (it refuses, exit 2), which is correct for the census and ungradeable for a case that asserts a verdict"

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

  # ---- Case H2: the SAME seam reached through the ENVIRONMENT -----------------
  # Case H gates the FLAGS. The env vars are a second door to the same room, and
  # until this case existed they were ungated on the live path:
  #   CENSUS_MODE=fixture CENSUS_FIXTURE_DIR=<an empty dir> → `clean: the process
  #   table was read; 0 candidate(s)` and exit 0, about a table that does not exist.
  # A census that prints "the process table was read" about a table it never read is
  # the exact unearned green this guard exists to refuse, so on the live path every
  # fixture seam is a REFUSAL that names the variable responsible.
  d="$(new_fixture seam-empty-dir)"
  for seam in CENSUS_MODE CENSUS_FIXTURE_DIR CENSUS_PLATFORM CENSUS_INPUT; do
    case "$seam" in
      CENSUS_MODE)         seam_value=fixture ;;
      CENSUS_FIXTURE_DIR)  seam_value="$d" ;;
      CENSUS_PLATFORM)     seam_value=linux ;;
      CENSUS_INPUT)        seam_value="$d/stream.txt" ;;
    esac
    expect_exit "H2 $seam on the LIVE path is a refusal, never a census" 2 \
      env "$seam=$seam_value" bash "$script"
    expect_output_has "H2 the refusal names the offending variable $seam" "$seam"
    expect_output_has "H2 the refusal explains that only --self-test may use $seam" "--self-test"
    expect_output_lacks "H2 $seam never produces a fabricated clean" "clean: the process table was read"
  done
  # CENSUS_MODE=live is the honest spelling and must NOT be refused FOR EXISTING. The
  # claim is "not refused as a seam", not "exits 0": on a machine whose `ps` cannot
  # enumerate, exit 2 is the census being correct, and asserting 0 here punished the
  # platform for H2's own overreach. So the platform-independent half is asserted
  # always (CENSUS_MODE must not appear in the refusal's offender list, and no
  # fabricated clean), and the outcome half only where a census can actually reach one.
  local h2_live_out h2_live_rc=0
  h2_live_out="$(env CENSUS_MODE=live bash "$script" 2>&1)" || h2_live_rc=$?
  LAST_OUTPUT="$h2_live_out"
  # Match the seam-refusal's own signature, not the bare variable name: an unrelated
  # message that merely mentions CENSUS_MODE must not fail this.
  if [[ "$h2_live_out" == *"sets a harness seam"* ]]; then
    printf 'FAIL H2 CENSUS_MODE=live was refused as a harness seam (it is the honest spelling)\n'
    printf '       output: %s\n' "$(printf '%s' "$h2_live_out" | tr '\n' '|')"
    self_test_failures=$((self_test_failures + 1))
  else
    printf 'ok   H2 CENSUS_MODE=live on the live path is accepted (never refused for being a seam)\n'
  fi
  # NOTE: no "never prints clean" assertion belongs here, and the case deliberately
  # does not have one. CENSUS_MODE=live is the REAL census, so on a clean machine it
  # SHOULD print "clean: the process table was read" — that sentence is the honest
  # verdict, not a fabricated one. The fabrications H2 hunts are the fixture seams
  # (CENSUS_MODE=fixture and friends), which the loop above pins one variable at a
  # time; conflating the two would mean this case could only pass by refusing to run a
  # census it is asking for by name.
  if ((cap_enumerable == 1)); then
    if [[ "$h2_live_rc" == 0 ]]; then
      printf 'ok   H2 CENSUS_MODE=live reaches a real verdict on a platform that can read the table (exit 0)\n'
    else
      printf 'FAIL H2 CENSUS_MODE=live should have completed here, got exit %s\n' "$h2_live_rc"
      printf '       output: %s\n' "$(printf '%s' "$h2_live_out" | tr '\n' '|')"
      self_test_failures=$((self_test_failures + 1))
    fi
  else
    printf 'NOTE H2 the exit-code half of CENSUS_MODE=live is UNPROVEN here (%s)\n' "$cap_why"
  fi
  # Anything else the harness reads is refused too — a typo'd mode must not fall
  # through to a census that reads nothing.
  expect_exit "H2 CENSUS_MODE=something-else is refused, not silently treated as live" 2 \
    env CENSUS_MODE=probly-fine bash "$script"
  expect_output_has "H2 the refusal names CENSUS_MODE" "CENSUS_MODE"

  # ---- Case N: a fixture dir is not a process table ---------------------------
  # Fixture mode is allowed to read a mock, but it must not be allowed to read
  # NOTHING and call it clean. A zero-byte stream is `0 candidate(s)` + exit 0 by
  # the arithmetic alone; the same rule the windows arm already applies ("an empty
  # process table is not evidence") applies here.
  d="$(new_fixture missing-table)"
  rm -f -- "$d/stream.txt"
  expect_exit "N  a fixture dir with NO stream.txt refuses instead of reporting clean" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_lacks "N  a missing table is never certified clean" "clean: the process table was read"
  expect_output_has "N  the refusal names the fixture dir it could not read" "fixture"

  d="$(new_fixture empty-table)"
  expect_exit "N  an EMPTY (zero-byte) fixture table refuses instead of reporting clean" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_lacks "N  an empty table is never certified clean" "clean: the process table was read"
  expect_output_has "N  the refusal says an empty table is not evidence" "not evidence"

  # A table with rows but no env file is the same failure one layer down: the census
  # was told which processes exist and nothing about any of them.
  d="$(new_fixture no-env-file)"
  printf '%s\n' '5100 1 S /tmp/build/fake_worker_core --worker' >"$d/stream.txt"
  rm -f -- "$d/env.txt"
  expect_exit "N  a fixture table with no env.txt refuses (provenance was never provided)" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_lacks "N  no env file never certifies clean" "clean: the process table was read"

  # ---- Case I: the LIVE planted process — the enumeration path, for real -----
  # A fixture stream can never prove `enumerate_unix` can SEE a live process, so the
  # self-test plants one: a real exec of a real binary really named
  # `fake_worker_core`, with the marker in its real environment, and the census must
  # find it through `ps`, classify it through /proc/<pid>/environ, report it, and
  # kill it. If the enumeration were `--input`-only, or matched on comm, this fails.
  #
  # THREE THINGS THIS CASE NEEDS, and it used to assume all three: a `ps` whose
  # `-eo` spelling answers (cygwin's `ps` exists on Git-Bash and EXITS 1 on `-eo`),
  # a copyable `sleep` to plant, and `setsid` to detach the plant. `setsid` is not
  # installed on macOS OR on Windows Git-Bash — measured — so on both legs the plant
  # never started, every downstream assertion ran against an empty table, 7
  # assertions went red on a machine that had nothing wrong with its census, and one
  # of them went GREEN for the wrong reason ("the census killed it", from a pid that
  # had never existed). Missing tooling is not a census defect, so it is now a
  # recorded SKIP (loud, and fatal under CI via self_test_verdict); a plant that
  # starts but cannot be observed alive is a FAILURE, because that one IS the
  # enumerator's business.
  # Per-case skip decisions, made ONCE each and in one place (the probes themselves
  # ran at the top of run_self_test). Independent checks rather than one if/elif over
  # capabilities, because an elif chain hides a second missing tool behind the first
  # and the ledger then UNDERSTATES what this machine failed to prove; but exactly one
  # entry per case, because two entries for the same case would double-count one gap
  # as two. Each live case is gated on what IT needs:
  #   I  ps -eo, a copyable sleep, setsid, and a census that can read provenance
  #   M  the same four, plus python3
  #   L  a renamable bash + a census that can read provenance (NOT setsid, NOT sleep)
  #   R  whatever L managed to plant, plus /proc (it forces the linux-shaped reader)
  if ((cap_enumerable == 0)); then
    self_test_skip "I" "$cap_why — the live planted-process path is UNPROVEN"
    self_test_skip "L" "$cap_why — the lineage skip is UNPROVEN"
    self_test_skip "R" "$cap_why — the portable lineage arm is UNPROVEN"
    self_test_skip "M" "$cap_why — the zombie path is UNPROVEN"
  else
    if ((cap_plant == 0)) || ((cap_setsid == 0)); then
      local missing=""
      ((cap_plant == 0)) && missing="no sleep binary to copy as fake_worker_core"
      ((cap_setsid == 0)) && missing="${missing:+$missing, }setsid is not installed (absent on macOS and on Git-Bash)"
      self_test_skip "I" "$missing — the live planted-process path is UNPROVEN"
    fi
    local m_missing=""
    ((cap_plant == 0)) && m_missing="no sleep binary to name as the zombie's parent"
    ((cap_setsid == 0)) && m_missing="${m_missing:+$m_missing, }setsid is not installed (absent on macOS and on Git-Bash)"
    ((cap_python3 == 0)) && m_missing="${m_missing:+$m_missing, }no python3 to act as a non-reaping parent"
    [[ -n "$m_missing" ]] && self_test_skip "M" "$m_missing — the zombie path is UNPROVEN"
    # L and R need neither setsid nor sleep nor python3 — they plant through
    # `env … "$live/fulcrum" -c …` — so on a macOS leg, where Case I cannot run, they
    # still can. Their own guards are at the bottom of each case.
  fi

  # Case I runs only when every one of its dependencies is present; a missing one is
  # already in the ledger above, so the case stays SILENT rather than adding a FAIL
  # that would double-count the same gap as a census defect.
  if ((cap_ps == 1)) && ((cap_plant == 1)) && ((cap_setsid == 1)) && ((cap_live_env == 1)); then
    local live_marker="selftest-live-$$-$RANDOM"
    # setsid + full stdio redirection: the planted process must not hold the
    # self-test's output pipe, and must not die with the census's process group.
    ( CENSUS_LIVE_MARKER="$live_marker" FULCRUM_CI_RUN_MARKER="$live_marker" \
        setsid "$live/fake_worker_core" 300 </dev/null >/dev/null 2>&1 &
      printf '%s' "$!" >"$live/planted.pid" )
    local planted=""
    [[ -r "$live/planted.pid" ]] && planted="$(cat "$live/planted.pid")"
    [[ -n "$planted" ]] && self_test_planted+=("$planted")

    # PRECONDITION, OBSERVED: the plant is in the table before any assertion about it.
    # Everything below is gated on this, so a plant that never started can only be a
    # FAILURE of this line — never a green "it was killed" three lines later.
    if ! plant_was_alive "$planted"; then
      printf 'FAIL I  the planted process was never OBSERVED alive (pid %s) — nothing below this line can be graded\n' "${planted:-<no pid>}"
      self_test_failures=$((self_test_failures + 1))
    else
      printf 'ok   I  the planted process %s was OBSERVED alive before any verdict about it\n' "$planted"

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

      # NOTE: these three invoke the LIVE census, so they must set NO seam variable —
      # not even CENSUS_PLATFORM, which the live path now derives from `uname` alone
      # (see refuse_self_test_only_env_seams). The marker is the only thing a live run
      # is allowed to be told.
      expect_exit "I  the LIVE census reports the planted process as a leak" 1 \
        env -u CENSUS_MODE -u CENSUS_FIXTURE_DIR -u CENSUS_INPUT -u CENSUS_PLATFORM \
        FULCRUM_CI_RUN_MARKER="$live_marker" bash "$script"
      expect_output_has "I  the live leak line names the pid and the image" "leak: $planted fake_worker_core"

      # Reachable ONLY from the observed-alive branch above, which is what makes the
      # negative `ps -p` below evidence of a KILL rather than evidence of a typo.
      if ps -p "$planted" >/dev/null 2>&1; then
        printf 'FAIL I  the census did not kill the planted process (pid %s is still alive)\n' "$planted"
        self_test_failures=$((self_test_failures + 1))
      else
        printf 'ok   I  the planted process %s was alive, and is gone after the census\n' "$planted"
      fi
    fi

    # And the discriminator, live: a DIFFERENT run's marker must not be touched.
    ( FULCRUM_CI_RUN_MARKER="some-other-run-$$" setsid "$live/fake_worker_core" 300 </dev/null >/dev/null 2>&1 &
      printf '%s' "$!" >"$live/other.pid" )
    local other=""
    [[ -r "$live/other.pid" ]] && other="$(cat "$live/other.pid")"
    [[ -n "$other" ]] && self_test_planted+=("$other")
    if plant_was_alive "$other"; then
      expect_exit "I  a live process with ANOTHER run's marker is left alone" 0 \
        env -u CENSUS_MODE -u CENSUS_FIXTURE_DIR -u CENSUS_INPUT -u CENSUS_PLATFORM \
        FULCRUM_CI_RUN_MARKER="$live_marker" bash "$script"
      if ps -p "$other" >/dev/null 2>&1; then
        printf 'ok   I  the other-run process %s is still alive (untouched)\n' "$other"
        kill -KILL "$other" 2>/dev/null || true
        wait "$other" 2>/dev/null || true
        self_test_planted=("${self_test_planted[@]/$other/}")
      else
        printf "FAIL I  the census killed a process that was NOT this run's (pid %s)\n" "$other"
        self_test_planted=("${self_test_planted[@]/$other/}")
        self_test_failures=$((self_test_failures + 1))
      fi
    else
      printf 'FAIL I  the OTHER-run plant was never observed alive (pid %s) — the discriminator is UNPROVEN\n' "${other:-<no pid>}"
      self_test_failures=$((self_test_failures + 1))
    fi

    # The census must also survive being run from a checkout whose directory is
    # named `fulcrum` (this repo's own CI): the census skips its own lineage.
    expect_exit "I  a clean live census passes and proves it read the table" 0 \
      env -u CENSUS_MODE -u CENSUS_FIXTURE_DIR -u CENSUS_INPUT -u CENSUS_PLATFORM \
      FULCRUM_CI_RUN_MARKER="selftest-clean-$$-$RANDOM" bash "$script"
    expect_output_has "I  the clean census reports the candidate count it looked at" "the process table was read"
  fi

  # ---- Case P: gone-before-read is DECLINED, never silently dropped -----------
  # A pid can exit between the enumerator listing it and the classifier reading its
  # env. That race is not a leak at census time, so the census skips it — but it must
  # SKIP IT OUT LOUD. The branch lived at parse_candidate_stream's `gone-before-read`
  # line with no case of any kind behind it, and fixture mode could not express it at
  # all: a synthetic pid has no /proc entry, so `env_from_fixture` had no way to say
  # "this pid vanished" as opposed to "this pid's env is unreadable" (the second is a
  # refusal, the first is a clean skip — conflating them loses either the race's
  # coverage or the refusal). So the fixture arm gains a GONE record, exactly as it
  # already carries an UNREADABLE one.
  d="$(new_fixture gone-before-read)"
  {
    printf '%s\n' '5200 1 S /tmp/build/fake_worker_core --worker'
    printf '%s\n' "5201 1 S /tmp/build/fake_worker_core --worker"
  } >"$d/stream.txt"
  printf '%s\n' '5200 GONE' "5201 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "P  a pid that exited before its env was read is declined, and the real leak beside it is still caught" 1 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "P  the vanished pid is named as gone-before-read" "gone-before-read: 5200 fake_worker_core"
  expect_output_has "P  the reason names the race, not a verdict about the process" "exited between listing and reading"
  expect_output_has "P  the leak next to it is still reported" "leak: 5201 fake_worker_core"

  # The direction that matters: a table whose ONLY candidate vanished is not evidence
  # that nothing leaked. The census saw one of our images and could not tell whether
  # it was ours — it says so, and it does not certify the run.
  d="$(new_fixture gone-only)"
  printf '%s\n' '5300 1 S /tmp/build/fake_worker_core --worker' >"$d/stream.txt"
  printf '%s\n' '5300 GONE' >"$d/env.txt"
  expect_exit "P  a table whose ONLY candidate vanished does not certify the run clean" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "P  the vanished candidate is still named" "gone-before-read: 5300 fake_worker_core"
  expect_output_lacks "P  an all-vanished table is never certified clean" "clean: the process table was read"
  expect_output_has "P  the refusal says the census could not answer" "could not classify"

  # ---- Case J: the census never accepts a truncated name ---------------------
  # If classification ever moved to comm (or to `pgrep -x`'s matching), the
  # 15-char form would become the accepted spelling and this row would become a
  # leak. It must be rejected: the census matches FULL names only.
  d="$(new_fixture truncated-name)"
  printf '%s\n' '4700 1 S /tmp/build/fake_mcp_stdio_ --stdio' >"$d/stream.txt"
  printf '%s\n' "4700 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "J  the 15-char truncated form is NOT a match (comm is never the key)" 0 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=linux CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  # The exit code alone asserted nothing: 0 is ALSO what a census that matched
  # nothing at all prints. Deleting the collision rule outright kept J green (checked
  # by mutation). So assert the census's own declined-collision wording, as B3 does —
  # the difference between "I saw it and declined it" and "I never saw it".
  expect_output_has "J  the truncated form is DECLINED BY NAME, not silently dropped" "left-alone: 4700 fake_mcp_stdio_"
  expect_output_has "J  the reason is the 15-char comm collision, not an unmatched name" "shares a 15-char comm with a watched image"
  expect_output_has "J  the declined collision is counted in the clean report" "1 15-char comm collision(s) declined"

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
  expect_output_has "K  the refusal names the platform limit" "control probe state: unreadable"
  expect_output_has "K  a BLOCKED READ is the only state allowed to mention SIP" "SIP / proc_security"

  # ---- Case K2: the refusal must blame the RIGHT thing -----------------------
  # The macOS leg refused with a message about SIP / proc_security on a platform that
  # was never blocking anything: the single-match extractor could only ever see the
  # first NAME=VALUE on BSD's one-line read, so the census's own marker (~100 vars in)
  # was invisible and the probe answered `no-marker-in-own-env` about an environment it
  # had read FINE (measured: exit 0, 5443 bytes, marker present in the raw output). A
  # confident misattribution costs a day of chasing a security policy that does not
  # exist, so each probe state now carries its own diagnosis — and each branch is
  # pinned here, because the live macOS reader cannot run on this Linux box.
  d="$(new_fixture control-no-marker)"
  printf '%s\n' '5010 1 S /tmp/build/fulcrum serve --socket /tmp/s.sock' >"$d/stream.txt"
  printf '%s\n' "5010 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "K2 the read SUCCEEDED and the marker genuinely was not in it still refuses" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=macos CENSUS_FIXTURE_CONTROL_STATE=no-marker-in-own-env CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "K2 that state says the environment WAS read" "the environment WAS read"
  # The whole point: SIP is innocent here and the message must not say otherwise.
  expect_output_lacks "K2 a successful read does NOT get blamed on SIP" "SIP / proc_security"
  expect_output_has "K2 it points at the real cause (the marker was not exported)" "not exported into this step"

  d="$(new_fixture control-no-env)"
  printf '%s\n' '5020 1 S /tmp/build/fulcrum serve --socket /tmp/s.sock' >"$d/stream.txt"
  printf '%s\n' "5020 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "K2 a read that came back with NO env text at all refuses" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=macos CENSUS_FIXTURE_CONTROL_STATE=no-env-in-output CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "K2 that state names the real cause: a missing args= column" "losing the 'args=' column"
  expect_output_lacks "K2 a broken invocation is NOT blamed on the platform" "SIP / proc_security"

  d="$(new_fixture control-truncated)"
  printf '%s\n' '5030 1 S /tmp/build/fulcrum serve --socket /tmp/s.sock' >"$d/stream.txt"
  printf '%s\n' "5030 $MARKER_NAME=$marker" >"$d/env.txt"
  expect_exit "K2 an extraction that hit its bound refuses rather than answer from a partial list" 2 \
    env CENSUS_MODE=fixture CENSUS_PLATFORM=macos CENSUS_FIXTURE_CONTROL_STATE=env-truncated CENSUS_FIXTURE_DIR="$d" bash "$script" --self-test --census
  expect_output_has "K2 that state says the scan STOPPED EARLY rather than found nothing" "stopped early"

  # ---- Case S: the macOS ONE-LINE env read must be parsed to exhaustion ------
  # BSD `ps eww -o pid=,args= -p <pid>` prints argv AND THE ENTIRE ENVIRONMENT ON ONE
  # LINE. Measured on macos-latest: 5443 chars, ZERO newlines, marker present in the
  # raw bytes, and the old single-match extractor pulled out exactly ONE variable —
  # `GITHUB_JOB=probe`, the first env-shaped token — so the marker ~100 vars later was
  # never seen and the census refused its own platform. Linux could not show this:
  # env_from_procfs reads /proc/<pid>/environ NUL-split, one var per line, no regex.
  #
  # The live macOS reader cannot run here, so --ps-env-parse feeds the SHIPPED parser a
  # captured read (the same trick Case R plays on the lineage arm). Three claims:
  #   1. the marker IS found on a one-line read holding ~109 vars;
  #   2. the count really is ~109, i.e. extraction ran to exhaustion and not to the
  #      first match — asserting the marker alone would pass by luck if the marker
  #      happened to sit first, so the COUNT is the load-bearing half;
  #   3. an ARGV-ONLY read yields ZERO vars. Multi-extraction widens the surface that
  #      could invent environment out of a command line, and the daemon's argv ends in
  #      `--worker-exe /path/to/fake_worker_core`; if a `--flag=value` token ever read
  #      as an env entry, the census would be matching argv it has no business matching.
  local s_line="$self_test_root/ps_eww_macos.txt"
  local s_argv="$self_test_root/ps_eww_argv_only.txt"
  {
    printf ' 12345 /tmp/build/fake_worker_core --worker --session=deadbeef --exe=/bin/true'
    printf ' GITHUB_JOB=probe'
    local s_i=1
    while ((s_i <= 108)); do
      if ((s_i == 42)); then
        printf ' %s=%s' "$MARKER_NAME" "$marker"
      else
        printf ' CI_VAR_%s=value-%s-padding-padding-padding' "$s_i" "$s_i"
      fi
      s_i=$((s_i + 1))
    done
  } >"$s_line"
  printf ' 2222 /tmp/build/fake_worker_core --worker --session=deadbeef --exe=/bin/true -x --flag=notenv' \
    >"$s_argv"

  expect_exit "S  the macOS one-line ps eww read parses to its FULL environment" 0 \
    bash "$script" --self-test --ps-env-parse --input "$s_line"
  # THE MARKER (the thing the census needs).
  expect_output_has "S  the marker IS extracted from a one-line read" "$MARKER_NAME=$marker"
  # THE EXHAUSTION: 109 vars, not 1. `1 var` is precisely what shipped green locally
  # and red on the Mac, so the count is asserted, not just the marker.
  expect_output_has "S  extraction ran to exhaustion (109 vars, not the first match)" "parsed-vars: 109"
  # The first var is still the one the OLD code returned — proof the fixture keeps the
  # shape that broke it (GITHUB_JOB=probe first, marker ~100 in).
  expect_output_has "S  the fixture keeps the shape that broke the old parser" "GITHUB_JOB=probe"
  # And the bound is not what produced that answer.
  expect_output_has "S  a normal-sized environment is NOT reported as truncated" "parsed-truncated: 0"

  expect_exit "S  an ARGV-ONLY ps eww read yields ZERO env vars (argv is not environment)" 0 \
    bash "$script" --self-test --ps-env-parse --input "$s_argv"
  expect_output_has "S  --worker / --session=… / --exe=… / --flag=… are NOT environment" "parsed-vars: 0"
  expect_output_has "S  and therefore nothing was truncated either" "parsed-truncated: 0"

  # The bound is a REFUSAL, not a shorter answer. A parser that quietly returned the
  # first ENV_PS_MAX_VARS vars would let the census call a process "not this run's" on
  # a read it never finished — the same false clean the bound exists to prevent, so
  # hitting it has to be visible to the caller.
  local s_fat="$self_test_root/ps_eww_fat.txt"
  {
    printf ' 3333 /bin/true'
    local s_f=1
    while ((s_f <= 4200)); do
      printf ' Z%s=v' "$s_f"
      s_f=$((s_f + 1))
    done
  } >"$s_fat"
  expect_exit "S  a read past the extraction bound is reported as TRUNCATED, not as a complete answer" 0 \
    bash "$script" --self-test --ps-env-parse --input "$s_fat"
  expect_output_has "S  the bound was hit and says so" "parsed-truncated: 1"
  expect_output_has "S  and it stopped AT the bound rather than running away" "parsed-vars: $ENV_PS_MAX_VARS"

  # The seam is a --self-test door and nothing else: --ps-env-parse reads a FILE, so on
  # the live path it is exactly the mock-in-front-of-the-census shape Case H refuses.
  expect_exit "S  --ps-env-parse outside --self-test is rejected" 1 \
    bash "$script" --ps-env-parse --input "$s_argv"
  expect_output_has "S  the rejection explains that the live census reads the real table" "self-test"

  # ---- Case L: the census must never kill its own lineage -------------------
  # LIVE, and it reproduces the one shape the self-skip exists for: the census is
  # run by a parent process whose OWN image is a watched image and which carries the
  # marker. Here a copy of bash named `fulcrum` runs the census, so `ps` lists the
  # census's parent as `fulcrum` with this run's marker in its environment. Without
  # the lineage skip the census classifies its own parent as a leak and KILLS IT —
  # a guard that kills the step running it. With the skip it names the skip and
  # passes. This case fails (red) if is_self_pid is removed.
  # A REAL SKIP GUARD, not a Linux-only assumption. Case L needs exactly two things: a
  # bash this harness may copy and rename to `fulcrum`, and a census that can read
  # provenance (see cap_live_env — on a macOS leg whose env reads are blocked the
  # census exits 2, which is correct for the census and ungradeable for a case that
  # asserts exit 0). It does NOT need setsid or sleep, so it keeps running on the
  # legs where Case I cannot. The old branch printed FAIL when the copy failed, which
  # punished the census for a missing tool.
  if ((cap_ps == 1)) && ((cap_live_env == 1)) && cp "$(command -v bash 2>/dev/null || echo /nonexistent)" "$live/fulcrum" 2>/dev/null; then
    chmod +x "$live/fulcrum"
    live_fulcrum_is_bash=1
    local lineage_marker="selftest-lineage-$$-$RANDOM"
    local lineage_out="$self_test_root/lineage.txt"
    local lineage_rc=0
    # The parent is really named `fulcrum`; its child is the census. No seam variable
    # may be set — this census runs on the live path.
    FULCRUM_CI_RUN_MARKER="$lineage_marker" \
      env -u CENSUS_MODE -u CENSUS_FIXTURE_DIR -u CENSUS_INPUT -u CENSUS_PLATFORM \
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
    # Not a census failure: one of L's own dependencies is missing. Record it, loudly.
    if ((cap_ps == 0)) || ((cap_live_env == 0)); then
      : # already recorded in the ledger above — do not double-count it
    else
      self_test_skip "L" "no bash available to copy as a fulcrum-named parent — the lineage skip is UNPROVEN"
    fi
  fi

  # ---- Case R: the PORTABLE parent lookup must hold Case L's guarantee --------
  # Case L proves the lineage skip on THIS machine, which is Linux, where
  # /proc/<pid>/status exists. resolve_lineage used to read ONLY that file — and on
  # macOS it does not exist, so the ancestor loop never ran: the "lineage" was just
  # the census's own pid and Case L's guarantee (never kill a `fulcrum`-named PARENT)
  # did not hold on the platform this repo runs a macOS leg on. kill_leak's
  # zombie-parent lookup read the same file and so always fell through to "no
  # reachable parent" there.
  #
  # Both now go through parent_of, which falls back to `ps -o ppid= -p` — the one
  # spelling BSD and procps share. This case forces that arm (--lineage-arm-ps,
  # self-test-only) and re-runs Case L's shape through it, so the macOS arm is proven
  # here rather than asserted about a platform nobody tested on this box.
  if [[ $live_fulcrum_is_bash == 1 ]] && ((cap_ps == 1)) && ((cap_proc_env == 1)); then
    local r_marker="selftest-lineage-ps-$$-$RANDOM"
    local r_out="$self_test_root/lineage-ps.txt"
    local r_rc=0
    FULCRUM_CI_RUN_MARKER="$r_marker" \
      "$live/fulcrum" -c "bash '$script' --self-test --census --lineage-arm-ps --platform linux >'$r_out' 2>&1; echo \$? >'$r_out.rc'" </dev/null >/dev/null 2>&1 || true
    local got_r=0
    [[ -r "$r_out.rc" ]] && got_r="$(cat "$r_out.rc")"
    if [[ "$got_r" == "0" ]] && grep -q "the census itself" "$r_out" 2>/dev/null; then
      printf 'ok   R  the portable ps parent lookup walks ancestors (Case L holds on the macOS arm)\n'
    else
      printf 'FAIL R  the portable ps parent lookup did not protect the census lineage (exit %s)\n' "$got_r"
      printf '       output: %s\n' "$(tr '\n' '|' <"$r_out" 2>/dev/null || echo '<missing>')"
      self_test_failures=$((self_test_failures + 1))
    fi
  else
    # R's own dependencies, named individually: it inherits Case L's bash plant, and it
    # needs /proc/<pid>/environ because it forces the linux-shaped reader. Note that R
    # does NOT need setsid, sleep, or the host's own env reader — so on a macOS leg it
    # still cannot run (no /proc), while on a Linux box without setsid it still can.
    if ((cap_ps == 0)) || ((cap_live_env == 0)); then
      : # already recorded in the ledger above — do not double-count the same gap
    elif [[ $live_fulcrum_is_bash != 1 ]]; then
      self_test_skip "R" "Case L's fulcrum-named bash parent was never planted, so there is no lineage to protect — the portable lineage arm is UNPROVEN"
    else
      self_test_skip "R" "no readable /proc/<pid>/environ on this platform (R forces the linux-shaped reader) — the portable lineage arm is UNPROVEN"
    fi
  fi

  # The flag gate is a CLI-wiring assertion, not a live-planted one: it holds on every
  # platform and must not be skipped along with the plant, or a regression that opens
  # --lineage-arm-ps to the live path could hide behind a skipped case.
  expect_exit "R  --lineage-arm-ps outside --self-test is rejected (runs even when the plant is skipped)" 1 \
    bash "$script" --lineage-arm-ps

  # ---- Case M: a LIVE zombie of a watched image is caught --------------------
  # Fixture mode cannot express this: a synthetic pid has no parent, and `ps` only
  # prints `[<truncated-name>] <defunct>` for a REAL zombie. So the self-test plants
  # one: a parent whose own image is `fake_worker_core` and which carries the marker
  # forks a child that exits and is NEVER reaped. The census must catch it — and the
  # catch cannot come from the name, because `ps` truncates the zombie's bracketed
  # name to 15 chars (`fake_worker_cor`), which the exact rule must refuse. It comes
  # from the PARENT's marker, which is what this case really pins.
  # M's dependency set is decided once, in the capability block above (python3, setsid,
  # a copyable image, a census that can read provenance). The python3 guard already
  # existed there; the others did not, so on macOS M planted nothing and then reported
  # "could not plant" as a CENSUS failure — a missing tool graded as a defect.
  if ((cap_enumerable == 1)) && ((cap_plant == 1)) && ((cap_setsid == 1)) && ((cap_python3 == 1)); then
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
    # OBSERVE THE SUBJECT FIRST. `z_pid` came out of `ps`, so everything below is
    # asserting about a zombie the harness actually saw. The old shape could reach the
    # "reaping the parent removed the zombie" line with z_pid EMPTY on some paths and
    # with the plant never started on others — the same vacuity Case I had.
    if [[ -n "$z_pid" ]]; then
      printf 'ok   M  planted a real zombie (pid %s, parent %s) and OBSERVED it in the table\n' "$z_pid" "$z_parent_pid"
      expect_exit "M  the census catches the ZOMBIE of a watched image via its parent's marker" 1 \
        env -u CENSUS_MODE -u CENSUS_FIXTURE_DIR -u CENSUS_INPUT -u CENSUS_PLATFORM \
        FULCRUM_CI_RUN_MARKER="$z_marker" bash "$script"
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
        printf 'ok   M  reaping the parent removed the zombie %s (it was listed before, and is not now)\n' "$z_pid"
      fi
    else
      # Dependencies were all present and the plant still produced nothing: that IS a
      # finding, and it stays a failure.
      printf 'FAIL M  could not plant a zombie with python3+setsid present (fork/exec failed?)\n'
      self_test_failures=$((self_test_failures + 1))
    fi
  fi

  # ---- Case O: a SKIPPED case can never be reported as "all cases behaved" ----
  # Case M needs `python3` to plant a non-reaping parent. Before this case existed,
  # the python3-less branch printed a `note …` line to STDOUT and incremented nothing,
  # so the run still ended with "self-test: all cases behaved (the census
  # has teeth)" with the zombie path — the exact thing the census's zombie-provenance
  # rule exists to prove — completely unproven. A self-test that grades an unproven
  # case as a pass is the same lie as a census that reports clean about a table it
  # never read.
  #
  # `--meta-skip` is a harness-only probe: it registers one synthetic skip and runs
  # THE SAME verdict code the real run ends with (self_test_verdict, not a second
  # copy of it), so this case tests the verdict that actually gets printed rather
  # than a paraphrase of it.
  expect_exit "O  a skipped case is a FAILURE under CI=true (GitHub Actions sets CI on every runner)" 1 \
    env CI=true bash "$script" --self-test --meta-skip
  expect_output_has "O  the skip summary names the skipped case" "META-SKIP"
  expect_output_has "O  the skip summary says WHY it was skipped" "forced by --meta-skip"
  expect_output_has "O  the skip summary calls the case UNPROVEN" "UNPROVEN"
  expect_output_lacks "O  under CI a skipped run issues NO all-clear verdict" "all cases behaved"

  # Off CI the same skip must stay LOUD but non-fatal: a developer on a machine
  # without python3 can still run the self-test.
  expect_exit "O  the same skip is loud but not fatal off CI (dev boxes may lack python3)" 0 \
    env -u CI bash "$script" --self-test --meta-skip
  expect_output_has "O  off CI the skip is still named" "META-SKIP"
  expect_output_lacks "O  off CI a skipped run issues NO all-clear verdict" "all cases behaved"

  # And the real run carries the same bookkeeping: whatever it skipped, it says so
  # instead of printing an unconditional all-clear. This pins the wiring, not just the
  # verdict — the ledger only protects the run if the cases that can skip actually
  # register with it. Case M is the case that used to print a silent stdout note.
  expect_file_has "O  Case M registers its skip with the ledger (no silent stdout note)" \
    "$script" 'self_test_skip "M"'
  # The needle is assembled at runtime: written literally it would appear in this
  # file as the assertion's own argument and the pin would match itself.
  local old_skip_note="note M sk""ipped"
  expect_file_lacks "O  the old silent stdout skip note is gone from the script" \
    "$script" "$old_skip_note"

  # ---- Case T: an EXPECTED skip may be tolerated, but only by an auditable flag ----
  # Case O's rule (a skip cannot be an all-clear) is right where a case CAN run, and
  # wrong where it structurally cannot: `setsid` is absent BY PLATFORM DESIGN on macOS
  # and on Git-for-Windows bash — measured on real runners — so the macOS and Windows
  # legs would be permanently red over a gap that another matrix leg already covers.
  # That conflates "the harness is misconfigured" with "this platform cannot host this
  # case". `--allow-skips <reason>` separates them: skips stay LOUD and stay counted
  # as UNPROVEN, but exit 0. It is a FLAG whose value lands in the CI log, not an env
  # var — the live-path env-seam bug this branch just fixed (5d08d2c) was exactly a
  # behaviour-selecting env var letting a guard fabricate its own verdict, and a
  # workflow-level env var is no more auditable than that was. A reason string is
  # REQUIRED so the log always says why the gap is tolerated.
  local t_reason="setsid absent on macOS and Git-Bash"

  # WITH the flag, under CI: loud, still UNPROVEN, but not fatal.
  expect_exit "T  --allow-skips tolerates a skip under CI=true (exit 0, not 1)" 0 \
    env CI=true bash "$script" --self-test --meta-skip --allow-skips "$t_reason"
  expect_output_has "T  the tolerated skip still names the skipped case" "META-SKIP"
  expect_output_has "T  the tolerated skip still calls the case UNPROVEN" "UNPROVEN"
  expect_output_has "T  the tolerated skip still counts it in the summary" "1 case(s) skipped"
  # The REASON, not just the exit code: a tolerance that does not say why is as
  # unauditable as the env var this flag deliberately is not (Case J's lesson).
  expect_output_has "T  the tolerance prints the REASON the skip is allowed" "$t_reason"
  expect_output_has "T  the tolerance names the flag it came from" "--allow-skips"
  # Tolerating a gap is NOT passing it. The all-clear sentence must stay absent.
  expect_output_lacks "T  a tolerated skip issues NO all-clear verdict" "all cases behaved"

  # WITHOUT the flag the CI rule is UNCHANGED — this is the ubuntu leg's posture, and
  # the contrast is the whole point of the flag: it must be possible to see, from the
  # log, that the tolerance was granted rather than assumed.
  expect_exit "T  without --allow-skips a skip under CI=true is STILL fatal (ubuntu's posture)" 1 \
    env CI=true bash "$script" --self-test --meta-skip
  expect_output_has "T  the fatal path names the UNPROVEN case it refuses to forgive" "META-SKIP"
  expect_output_has "T  the fatal path says it is failing because of CI" "FAILING under CI=true"
  # The fatal path may MENTION the flag as a hint, but it must not talk as if a
  # tolerance had been granted — asserting the absence of the tolerance's own wording
  # is what distinguishes "here is an opt-in you did not take" from "you are covered".
  expect_output_lacks "T  the fatal path claims NO tolerance it was not given" "tolerated by --allow-skips"
  expect_output_lacks "T  the fatal path prints no reason line (none was given)" "reason given:"

  # A reason is mandatory: `--allow-skips ""` would be a tolerance with no audit trail.
  expect_exit "T  --allow-skips with an EMPTY reason is refused (a reason is the audit trail)" 1 \
    env CI=true bash "$script" --self-test --meta-skip --allow-skips ""
  expect_output_has "T  the empty-reason refusal says a reason is required" "requires a non-empty reason"

  # The `=` form is the same flag and the same rule.
  expect_exit "T  --allow-skips=<reason> behaves identically to the two-word form" 0 \
    env CI=true bash "$script" --self-test --meta-skip --allow-skips="$t_reason"
  expect_output_has "T  the = form prints its reason too" "$t_reason"

  # And the flag is a --self-test-only door, like every other harness seam (Case H).
  # It selects verdict BEHAVIOUR, so on the live path it would let a census grade its
  # own gap away — the same misuse class, so the same refusal and the same exit 1.
  expect_exit "T  --allow-skips outside --self-test is rejected" 1 \
    bash "$script" --allow-skips "$t_reason"
  expect_output_has "T  the rejection names the flag" "--allow-skips"
  expect_output_has "T  the rejection explains that only --self-test may use it" "--self-test"
  expect_output_lacks "T  the rejected live path runs NO census" "clean: the process table was read"

  # Wiring pin, for the half a subprocess cannot show on a box that has setsid: the
  # cases that skip on macOS/Windows must register with the ledger, so the flag has
  # something to tolerate. Case O pins this for M; the flag makes it load-bearing for
  # I as well, because a silently-disarmed plant case would now exit 0 on two legs.
  expect_file_has "T  Case I registers its skip with the ledger (nothing can skip quietly)" \
    "$script" 'self_test_skip "I"'

  self_test_verdict
}

# ---------------------------------------------------------------------------
# arguments
# ---------------------------------------------------------------------------
want_self_test=0
want_census=0
want_enumerate_only=0
want_meta_skip=0
want_lineage_arm_ps=0
want_ps_env_parse=0
allow_skips_reason=""
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
    # INTERNAL to --self-test Case R: force the portable `ps -o ppid= -p` parent
    # lookup — the ONLY arm macOS has — so the lineage guarantee can be proven on a
    # Linux box instead of being asserted about a platform nobody tested. A flag, not
    # an env var: the live census must never be able to be told which lineage lookup
    # to use, since that lookup is what stops it killing its own parent.
    --lineage-arm-ps)
      want_lineage_arm_ps=1
      shift
      ;;
    # INTERNAL to --self-test Case S: run ps_eww_env_vars over --input and report what
    # the extractor made of it, so the macOS one-line env shape can be proven on a
    # Linux box (the same trick Case R plays for the lineage arm). A FLAG, not an env
    # var, and it never reaches the live census: it reads a file, so like --input it is
    # refused outside --self-test.
    --ps-env-parse)
      want_ps_env_parse=1
      shift
      ;;
    # INTERNAL to --self-test Case O: register a synthetic skip and print the verdict
    # the real run ends with, so the "a skip cannot be an all-clear" claim is tested
    # against the code that actually prints the all-clear. Proves the VERDICT, not a
    # paraphrase of it — and it is the verdict path, not a second implementation.
    --meta-skip)
      want_meta_skip=1
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
    # Tolerate an EXPECTED skip instead of failing under CI=true. A flag, not an env
    # var, and it REQUIRES a reason: the whole point is that the tolerance is visible
    # and attributable in the CI log. An env var set at workflow level would be the
    # same mistake this branch just fixed on the live path (5d08d2c), where a
    # behaviour-selecting env var let a guard fabricate its own verdict. Refused
    # outside --self-test, like every other harness-only flag (Case H).
    --allow-skips)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --allow-skips requires a non-empty reason (say WHY the skip is tolerated; self-test only)" >&2
        usage
        exit 1
      fi
      allow_skips_reason="$2"
      shift 2
      ;;
    --allow-skips=*)
      allow_skips_reason="${1#--allow-skips=}"
      if [[ -z "$allow_skips_reason" ]]; then
        echo "error: --allow-skips requires a non-empty reason (say WHY the skip is tolerated; self-test only)" >&2
        usage
        exit 1
      fi
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
  if [[ -n "$input_file$fixture_dir$platform_override$allow_skips_reason" || $want_census == 1 || $want_enumerate_only == 1 || $want_meta_skip == 1 || $want_lineage_arm_ps == 1 || $want_ps_env_parse == 1 ]]; then
    {
      echo "error: --input / --fixture-dir / --platform / --census / --enumerate-only / --meta-skip / --lineage-arm-ps / --ps-env-parse / --allow-skips are for the --self-test harness only"
      echo "  the live census must read the REAL process table. A mock fed to the live path"
      echo "  would let this guard report 'clean' about a table it never looked at — the exact"
      echo "  vacuous green this family of guards exists to refuse."
      echo "  --allow-skips is refused for the same reason from the other side: it chooses how a"
      echo "  VERDICT is graded, so on the live path a guard could forgive its own gap."
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
    if ((want_lineage_arm_ps == 1)); then
      # Case R: prove the portable parent lookup on a platform where /proc exists and
      # would otherwise be used. Only reachable via --self-test --census.
      CENSUS_LINEAGE_ARM="ps"
    fi
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
  if ((want_ps_env_parse == 1)); then
    # Case S's probe: parse a captured `ps eww` read with the REAL extractor. Emits
    # one NAME=VALUE per line plus a trailer stating the count and whether the bound
    # was hit, so the case can assert all three properties (marker found, ~109 vars,
    # zero vars from argv-only) against the shipped parser rather than a copy.
    if [[ -z "$input_file" ]]; then
      echo "error: --ps-env-parse needs --input <a captured ps eww read> (self-test only)" >&2
      exit 2
    fi
    if [[ ! -r "$input_file" ]]; then
      printf 'error: --ps-env-parse cannot read --input %s\n' "$input_file" >&2
      exit 2
    fi
    ps_eww_env_vars "$(cat "$input_file")"
    parsed_count=0
    if [[ -n "$ENV_PS_VARS" ]]; then
      parsed_count="$(printf '%s\n' "$ENV_PS_VARS" | grep -c .)"
      printf '%s\n' "$ENV_PS_VARS"
    fi
    printf 'parsed-vars: %s\n' "$parsed_count"
    printf 'parsed-truncated: %s\n' "$ENV_PS_TRUNCATED"
    exit 0
  fi
  if ((want_meta_skip == 1)); then
    # Case O's probe: one synthetic skip through the REAL verdict path. Nothing else
    # runs — no fixtures, no planted processes — so the exit code is the verdict's
    # own answer to "may a skipped case be called behaved?".
    self_test_skip "META-SKIP" "forced by --meta-skip (a case that cannot run on this machine)"
    self_test_verdict
    exit $?
  fi
  run_self_test
  exit $?
fi

# ---- the live census -------------------------------------------------------
# Platform comes from `uname` and from NOTHING else. It used to also be readable from
# `CENSUS_PLATFORM`, and the argument for that was that the seam only picks which
# reader runs, not whether the table is read. That argument was wrong in a way this
# file must not carry: `CENSUS_PLATFORM` is one of the env vars that reach the same
# fixture seam the CLI flags reach, and leaving it readable meant the live census
# could be told to be a mock census by its environment alone. The harness runs its
# fixture arms through `--self-test --census`, where those vars are honoured; the live
# path refuses them (see refuse_self_test_only_env_seams).
#
# A function rather than bare top-level statements so there is exactly ONE live
# dispatch. The harness must never grow a second copy of "pick platform, read marker,
# run census", because a guard verified through a paraphrase of its own entry point
# proves nothing about the path CI actually takes.
run_live_census() {
  local marker_value
  case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux) CENSUS_PLATFORM=linux ;;
    Darwin) CENSUS_PLATFORM=macos ;;
    *)
      if command -v tasklist >/dev/null 2>&1; then
        CENSUS_PLATFORM=windows
      else
        printf 'error: cannot identify this platform (uname said %s) and tasklist is absent — the census refuses rather than guess\n' "$(uname -s 2>/dev/null || echo '?')" >&2
        return 2
      fi
      ;;
  esac

  marker_value="$(marker_from_env)"
  census "$marker_value" "$CENSUS_PLATFORM"
}

if ((want_self_test == 0)); then
  refuse_self_test_only_env_seams || exit $?
  run_live_census
  exit $?
fi

# Reached only if --self-test dispatched and returned without exiting.
exit 0

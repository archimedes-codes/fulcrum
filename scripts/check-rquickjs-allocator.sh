#!/usr/bin/env bash
#
# Fail if the rquickjs build graph has a custom allocator switched on.
#
# The codemode sandbox's 256 MiB cap is `AsyncRuntime::set_memory_limit`, and
# rquickjs's own doc comment (rquickjs-core-0.14.0/src/runtime/async.rs:204-210)
# says the setter "is a Noop when a custom allocator is being used, as is the
# case for the 'rust-alloc' or 'allocator' features". Those two features are
# TRANSITIVE: nothing in this crate's `Cargo.toml` asks for them, and any dependency
# that does — directly, or through feature unification anywhere in the graph —
# turns the cap into a comment in that same Cargo.toml. The app keeps running;
# the sandbox stops refusing to allocate.
#
# WHY A BUILD-GRAPH CHECK AND NOT A TEST. The no-op happens at COMPILE time,
# before any test runs, so no unit test can observe it as such. The engine does
# now carry a pin (`memory_cap_is_installed_not_merely_requested` reads the limit
# back out of the C runtime through `JS_ComputeMemoryUsage`), but that pin asserts
# the cap is INSTALLED, and measured on this dependency set the limit is still
# installed and still enforced under `--features rquickjs/rust-alloc` — QuickJS's
# cap check lives in `js_malloc_rt` (quickjs.c:2017), above the allocator
# dispatch. So the pin cannot be the thing that catches a poisoned graph. This
# script is: it asks the resolver, which is the only party that knows what got
# unified.
#
# ⚠ THE OBVIOUS COMMAND HERE IS VACUOUS, AND THIS IS THE TRAP.
# `cargo tree --edges features -p rquickjs` does NOT show these features when
# they ARE on. `-p` selects the PACKAGE SUBTREE, and a feature node is an edge
# pointing IN to rquickjs from whoever enabled it, so it lives outside the
# printed subtree. Verified on a throwaway crate whose only dependency is
# `rquickjs` with `allocator` enabled: the `-p rquickjs` output is byte-identical
# to the clean one, and the feature appears only in the FULL tree
# (`├── rquickjs feature "allocator"`). A guard written on the `-p` form would
# print green forever, which is the exact failure class this repo records at
# `src/tools/exec.rs` and in check-commands.sh's D7
# list. So the extraction reads the FULL feature tree.
#
# ⚠ AND THE GREP MUST NOT FALSE-POSITIVE. `allocator-api2` IS legitimately in
# this graph — `hashbrown` pulls it in — and it is a DIFFERENT CRATE from
# rquickjs's `allocator` FEATURE. On the clean tree today the substring "alloc"
# matches three real lines (`allocator-api2 feature "alloc"`,
# `hashbrown feature "allocator-api2"`, `allocator-api2 v0.2.21`), so a
# `grep -i alloc` guard is permanently red and therefore useless. The pattern is
# anchored on the crate name followed by ` feature "` and by the exact feature
# name, which is why it reads `<crate> feature "<name>"` and not a substring.
#
# Usage: check-rquickjs-allocator.sh [repo-root] [--input FILE] [--self-test]
#   repo-root    the checkout whose ROOT workspace is resolved (default: .)
#   --input      read a CAPTURED `cargo tree -e features` dump instead of running
#                cargo — for the fixture harness and for triage offline
#   --self-test  plant fixtures under a throwaway root and prove THIS guard —
#                re-invoked, never a second implementation — can still fail
#
# Exit codes: 0 the graph is clean, 1 a custom-allocator feature is active (or
# the check could not be run honestly), anything else is cargo's own failure.

set -euo pipefail

usage() {
  echo "usage: $(basename "$0") [repo-root] [--input FILE] [--self-test]" >&2
}

# The (crate, feature) pairs that install a custom allocator and so make
# `set_memory_limit` a no-op. BOTH the `rquickjs` facade and `rquickjs-core`
# carry these names — the facade's forwards to core (rquickjs-0.14.0/Cargo.toml:48,119)
# and the resolver prints a node for each level, so matching only the facade
# would miss a dependency that enables `rquickjs-core/rust-alloc` directly.
watched_crates=(rquickjs rquickjs-core)
watched_features=(allocator rust-alloc)

# The one-line shape the pattern is built for: `│   ├── rquickjs feature "allocator"`.
# `cargo tree -e features` always prints a feature node as
# `<indent>├── <crate> feature "<name>"`, so the anchor is the ` feature "`
# literal plus a quoted exact name — never a bare substring of "alloc".
matches_watched() {
  local line="$1" crate feature
  for crate in "${watched_crates[@]}"; do
    for feature in "${watched_features[@]}"; do
      if [[ "$line" == *" $crate feature \"$feature\""* ]]; then
        return 0
      fi
    done
  done
  return 1
}

# Runs the check over a feature-tree dump on stdin. Returns 0 clean, 1 poisoned,
# 2 when the dump is not trustworthy enough to answer with.
run_check() {
  local tree
  if ! tree="$(cat)"; then
    echo "error: could not read the cargo tree output — a failed read is not a clean graph" >&2
    return 2
  fi

  # THE VACUITY FLOOR. If the extraction never mentioned rquickjs at all, then
  # "no forbidden feature found" means nothing — that is the green-that-checked-
  # nothing this family exists to refuse. A renamed dependency or a workspace the
  # resolver cannot read both land here rather than at a pass.
  if ! grep -q 'rquickjs' <<<"$tree"; then
    {
      echo "error: the cargo tree output mentions rquickjs not at all"
      echo "  the codemode engine depends on it, so an output without it means the"
      echo "  graph was never resolved — reporting 'clean' here would be a guard"
      echo "  answering a question it was not asked"
    } >&2
    return 2
  fi

  local hits
  hits="$(grep -F ' feature "' <<<"$tree" | while IFS= read -r line; do
    if matches_watched "$line"; then printf '%s\n' "$line"; fi
  done || true)"

  if [[ -n "$hits" ]]; then
    {
      echo "error: a custom-allocator feature is active in the rquickjs build graph"
      printf '%s\n' "$hits" | sed 's/^/  /'
      echo "  rquickjs's set_memory_limit is a documented NO-OP under these features" >&2
      echo "  (rquickjs-core-0.14.0/src/runtime/async.rs:204-210), so the codemode"
      echo "  sandbox's 256 MiB cap would stop enforcing while still looking"
      echo "  configured — an over-budget script would grow the heap until the"
      echo "  process was OOM-killed instead of throwing."
      echo "  find who enabled it:  cargo tree -i rquickjs -e features   (from the repo root)"
    } >&2
    return 1
  fi

  echo "rquickjs build graph is clean: neither 'allocator' nor 'rust-alloc' is active on rquickjs or rquickjs-core"
  return 0
}

# --- --self-test: the premise mode --------------------------------------------
#
# The guard's whole job is to go red on a line shape that is NOT in the tree
# today. That is exactly the condition a live run cannot demonstrate, so the
# fixtures carry a captured poisoned dump. Each case re-invokes THIS script with
# `--input` — a second copy of the matching logic here would prove nothing about
# the one that runs in CI.
self_test_root=""
self_test_failures=0

self_test_cleanup() {
  if [[ -n "$self_test_root" && -d "$self_test_root" ]]; then
    rm -rf -- "$self_test_root"
  fi
}

expect_exit() {
  local what="$1" want="$2"
  shift 2
  # The repo's own idiom (check-versions.test.sh:run_check): `|| got=$?` and NOT
  # `if ! output=$(...)`. Under `if ! cmd`, the `!` inverts the status, so `$?`
  # read inside the then-branch is 0 — the harness would then report "expected
  # exit 1, got 0" for a script that exited 1, which is a false FAIL that looks
  # like a broken guard rather than a broken test.
  local got=0 output
  output="$("$@" 2>&1)" || got=$?
  if [[ "$got" == "$want" ]]; then
    printf 'ok   %s (exit %s)\n' "$what" "$got"
  else
    printf 'FAIL %s\n' "$what"
    printf '       expected exit %s, got %s\n' "$want" "$got"
    printf '       output: %s\n' "${output:-<empty>}"
    self_test_failures=$((self_test_failures + 1))
  fi
}

run_self_test() {
  # Canonicalised the same way the other two self-testing guards do it
  # (check-commands.sh:697, check-temp-dir-census.sh:972): the fixtures are
  # re-read by THIS file, so the path must not depend on the caller's cwd.
  local script
  script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  self_test_root="$(mktemp -d)"
  trap self_test_cleanup EXIT

  # THE CLEAN FIXTURE, transcribed from the real `cargo tree -e features` output
  # of this workspace. It carries the allocator-api2 / hashbrown lines on
  # purpose: they are the false-positive trap, and a clean fixture without them
  # would let a substring-matching guard pass its own test while being useless
  # against the real tree.
  local clean="$self_test_root/clean.txt"
  cat >"$clean" <<'FIXTURE'
├── rquickjs feature "default"
│   ├── rquickjs v0.14.0
│   │   └── rquickjs-core v0.14.0
│   │       ├── async-lock v3.4.2
│   │       └── hashbrown feature "default"
│   │           ├── hashbrown v0.17.1
│   │           │   └── allocator-api2 feature "alloc"
│   │           │       └── allocator-api2 v0.2.21
│   │           └── hashbrown feature "allocator-api2"
│   │               └── hashbrown v0.17.1 (*)
│   └── rquickjs feature "std"
├── rquickjs feature "futures"
└── rquickjs feature "parallel"
FIXTURE

  # THE POISONED FIXTURE, transcribed from the same command run against a
  # throwaway crate whose only dependency was `rquickjs` with `allocator`
  # enabled — real resolver output, not an invented shape.
  local poisoned="$self_test_root/poisoned.txt"
  cat >"$poisoned" <<'FIXTURE'
├── rquickjs feature "allocator"
│   ├── rquickjs v0.14.0
│   │   └── rquickjs-core v0.14.0
│   │       ├── hashbrown feature "default"
│   │       │   └── allocator-api2 feature "alloc"
│   │       │       └── allocator-api2 v0.2.21
│   │       └── rquickjs-sys v0.14.0
│   └── rquickjs-core feature "allocator"
│       └── rquickjs-core v0.14.0 (*)
└── rquickjs feature "parallel"
FIXTURE

  # The same poison at the CORE level only: a dependency that names
  # `rquickjs-core/rust-alloc` never mentions the facade's feature node, so a
  # guard watching only `rquickjs feature "…"` would call that clean.
  local poisoned_core="$self_test_root/poisoned-core.txt"
  cat >"$poisoned_core" <<'FIXTURE'
├── rquickjs feature "futures"
│   └── rquickjs v0.14.0
└── rquickjs-core feature "rust-alloc"
    └── rquickjs-core v0.14.0
FIXTURE

  # A dump that resolved nothing. Without the floor this is a PASS — no forbidden
  # line present — which is the vacuous green the whole file is written against.
  local empty="$self_test_root/empty.txt"
  printf '%s\n' '└── some-other-crate v1.0.0' >"$empty"

  # The false-positive trap on its own: a tree whose ONLY "alloc" text is the
  # legitimate allocator-api2 / hashbrown feature pair.
  local trap_fixture="$self_test_root/allocator-api2-only.txt"
  cat >"$trap_fixture" <<'FIXTURE'
├── rquickjs feature "default"
│   └── rquickjs v0.14.0
└── hashbrown feature "allocator-api2"
    ├── hashbrown v0.17.1
    └── allocator-api2 feature "alloc"
        └── allocator-api2 v0.2.21
FIXTURE

  expect_exit "a clean graph passes" 0 bash "$script" --input "$clean"
  expect_exit "the facade's allocator feature fails" 1 bash "$script" --input "$poisoned"
  expect_exit "core-level rust-alloc fails" 1 bash "$script" --input "$poisoned_core"
  expect_exit "allocator-api2 alone does NOT false-positive" 0 bash "$script" --input "$trap_fixture"
  expect_exit "a dump without rquickjs is refused, not passed" 2 bash "$script" --input "$empty"

  # The vacuous-form witness. The `-p rquickjs` subtree of a POISONED graph is
  # byte-identical to the clean one, so if anyone rewires this guard onto that
  # form the poisoned fixture above stops matching and this case goes red.
  local subtree="$self_test_root/subtree-form.txt"
  cat >"$subtree" <<'FIXTURE'
rquickjs v0.14.0
└── rquickjs-core v0.14.0
    ├── async-lock v3.4.2
    └── hashbrown feature "default"
        └── allocator-api2 feature "alloc"
            └── allocator-api2 v0.2.21
FIXTURE
  expect_exit "the -p subtree form would MISS the poison (so we do not use it)" 0 bash "$script" --input "$subtree"

  if ((self_test_failures > 0)); then
    printf '\nself-test: %s case(s) FAILED — the rquickjs allocator guard cannot be trusted to fail\n' "$self_test_failures" >&2
    return 1
  fi
  printf '\nself-test: all cases behaved (the guard has teeth)\n'
  return 0
}

# --- arguments ----------------------------------------------------------------

repo_root=""
input_file=""
want_self_test=0

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
    # Like check-versions.sh's `--tag`, this is an OPTION and not a positional:
    # an empty or missing value must be an error rather than "read stdin", which
    # would hang a CI step waiting on a file that was never passed.
    --input)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --input requires a value (a captured cargo tree -e features dump)" >&2
        usage
        exit 1
      fi
      input_file="$2"
      shift 2
      ;;
    --input=*)
      input_file="${1#--input=}"
      if [[ -z "$input_file" ]]; then
        echo "error: --input requires a value (a captured cargo tree -e features dump)" >&2
        usage
        exit 1
      fi
      shift
      ;;
    -*)
      echo "error: unknown argument: $1" >&2
      usage
      exit 1
      ;;
    *)
      if [[ -n "$repo_root" ]]; then
        echo "error: unexpected argument: $1" >&2
        usage
        exit 1
      fi
      repo_root="$1"
      shift
      ;;
  esac
done

if ((want_self_test == 1)); then
  run_self_test
  exit $?
fi

repo_root="${repo_root:-.}"

if [[ ! -d "$repo_root" ]]; then
  echo "error: repo root is not a directory: $repo_root" >&2
  usage
  exit 1
fi

if [[ -n "$input_file" ]]; then
  if [[ ! -f "$input_file" ]]; then
    echo "error: captured cargo tree dump is not a file: $input_file" >&2
    exit 1
  fi
  run_check <"$input_file"
  exit $?
fi

# The live path. Resolve from the REPO ROOT, never from a member: in this repo the
# root manifest IS the package, and in the desktop repo the root manifest is the
# `[workspace]` that unifies features across its members. A member-rooted read
# sees only that member's features, which is the vacuous form — the whole point of
# this guard is that unification happens graph-wide.
workspace="$repo_root"
if [[ ! -f "$workspace/Cargo.toml" ]]; then
  echo "error: no workspace manifest at $workspace/Cargo.toml (pass the repo root)" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required to resolve the build graph (install it, or pass --input with a captured dump)" >&2
  exit 1
fi

# `-e features` is the whole point (plain `cargo tree` prints no feature nodes at
# all, so the guard would match nothing and pass). `2>/dev/null` drops cargo's
# progress lines, which would otherwise land in the dump; a non-zero exit is
# still caught below, because an unresolvable graph must never read as clean.
if ! tree="$(cd "$workspace" && cargo tree -e features 2>/dev/null)"; then
  echo "error: cargo tree failed to resolve the workspace in $workspace — refusing to report a graph it could not read" >&2
  exit 1
fi

run_check <<<"$tree"
exit $?

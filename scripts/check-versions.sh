#!/usr/bin/env bash
#
# Fail if this workspace's packages do not agree on one version, or if a release
# tag does not match that version.
#
# WHY THIS EXISTS. This repo is TWO packages in one workspace — `fulcrum-core` at
# the root and the `fulcrum` daemon member — and nothing in the build ties their
# `version` fields together. Two manifests that disagree still COMPILE (a path
# dependency inside one workspace resolves by path, never by version, so the
# daemon links the engine regardless of what either manifest claims), so the drift
# is invisible until something reads one of the two numbers out loud. Three things
# do: the daemon's `hello` answer carries `serverVersion` from
# `env!("CARGO_PKG_VERSION")` (`fulcrum/src/serve.rs:2309`), the engine puts the
# same macro in its `User-Agent` and in MCP `clientInfo.version`
# (`src/harness/provider/transport.rs:19`, `src/mcp/http.rs:125`), and a git tag
# announces a number to whoever reads the release page. A release where the two
# disagree therefore ships a tree that cannot say which version it is.
#
# WHY IT IS NOT JUST "TWO GREPS". The version is now a real contract, not a label:
# the desktop consumes this engine as a git dependency and the plan is to move that
# pin from a 40-character `rev` to `tag` + `version`, which cargo only honours when
# the tagged manifest actually carries that version. So the tag and the manifests
# have to be proven to agree, in one place, before anything is published — the
# same reason the desktop's copy of this script folds `--tag` into the agreement.
#
# ⚠ THE MEMBER LIST IS DERIVED, NOT HARDED, AND THERE IS A FLOOR. A check that
# names `Cargo.toml` and `fulcrum/Cargo.toml` passes green the day someone adds a
# third member, because the new member is simply not read — the vacuous-pass class
# this repo records at `scripts/check-rquickjs-allocator.sh` (the `-p` subtree
# form) and in `src/childproc.rs`'s blind-census witnesses. So the members are
# enumerated from the root manifest (`[workspace] members`, plus the implicit root
# package), and fewer than two version-bearing members is a REFUSAL (exit 2), not
# a pass.
#
# ⚠ THE `[package]` SECTION IS PARSED, NOT GREPPED. `[workspace.dependencies]` and
# `[workspace.dependencies.<name>]` tables are legal in the root manifest and their
# `version = "…"` lines sit at line start too. A `grep -m1 '^version'` would read
# whichever came FIRST and could bless a workspace dependency's version as the
# crate's own. Both readers here are section-aware for that reason; `--self-test`
# plants that decoy so a regression to a naive grep is caught rather than tolerated.
#
# Usage: check-versions.sh [repo-root] [--tag vX.Y.Z] [--self-test]
#   repo-root    the checkout to read (default: .)
#   --tag        also require the tag's version to equal the agreed version
#   --self-test  plant fixture trees under a throwaway root and prove THIS guard —
#                re-invoked, never a second implementation — can still fail
#
# Exit codes: 0 everything agrees; 1 a disagreement or a malformed argument;
# 2 the check could not be run honestly (a member manifest is missing, carries no
# `[package] version`, or the lock file is absent) — refused, never passed.

set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: check-versions.sh [repo-root] [--tag vX.Y.Z] [--self-test]
USAGE
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit "${2:-1}"
}

repo_root=""
tag=""
self_test=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --tag)
      # A `--tag` with nothing after it must not degrade into "no tag was
      # requested" — the release job would then pass while checking nothing.
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        usage
        die "--tag requires a value (the git tag, e.g. v0.1.0)"
      fi
      tag="$2"
      shift 2
      ;;
    --tag=*)
      tag="${1#--tag=}"
      if [[ -z "$tag" ]]; then
        usage
        die "--tag requires a value (the git tag, e.g. v0.1.0)"
      fi
      shift
      ;;
    --self-test)
      self_test=true
      shift
      ;;
    -*)
      usage
      die "unknown argument: $1"
      ;;
    *)
      if [[ -n "$repo_root" ]]; then
        usage
        die "unexpected argument: $1"
      fi
      repo_root="$1"
      shift
      ;;
  esac
done

repo_root="${repo_root:-.}"

# --- the readers -------------------------------------------------------------

# Reads `version` from the `[package]` table of one Cargo manifest. Section-aware:
# only lines while the current section is exactly `[package]` are candidates, so a
# `[workspace.dependencies.<name>]` version above or below it cannot be mistaken
# for the crate's own. Prints nothing (and the caller reports) when absent.
read_package_version() {
  local manifest="$1"
  [[ -f "$manifest" ]] || return 2
  awk '
    /^[[:space:]]*\[/ { section = $1; next }
    section == "[package]" && /^[[:space:]]*version[[:space:]]*=/ {
      if (match($0, /"[^"]*"/)) { print substr($0, RSTART + 1, RLENGTH - 2); exit }
    }
  ' "$manifest"
}

# Reads the version of ONE named package out of Cargo.lock, by scanning
# `[[package]]` blocks. The lock is not part of the agreement this guard exists to
# police (cargo rewrites it on the next build, so a lagging lock self-heals) but it
# IS part of the tree a tag points at, and a tag whose lock still names the old
# version makes `cargo build --locked` — a step any future CI may add — fail on the
# release commit itself. Checked, with a message that names the fix.
read_lock_version() {
  local lock="$1" want="$2"
  awk -v want="$want" '
    function flush() {
      if (!found && name == want && ver != "") { print ver; found = 1; exit }
    }
    /^[[:space:]]*\[\[/ { flush(); name = ""; ver = ""; inpkg = 1; next }
    /^[[:space:]]*\[/   { flush(); name = ""; ver = ""; inpkg = 0; next }
    inpkg && /^[[:space:]]*name[[:space:]]*=/ {
      if (match($0, /"[^"]*"/)) name = substr($0, RSTART + 1, RLENGTH - 2)
      next
    }
    inpkg && /^[[:space:]]*version[[:space:]]*=/ {
      if (match($0, /"[^"]*"/)) ver = substr($0, RSTART + 1, RLENGTH - 2)
      next
    }
    END { flush() }
  ' "$lock"
}

# The names `[workspace] members = [...]` declares, one per line. Quoted entries,
# stripped of commas and whitespace; `#` comments after the array are dropped.
read_member_entries() {
  local manifest="$1"
  [[ -f "$manifest" ]] || return 2
  awk '
    /^[[:space:]]*members[[:space:]]*=/ {
      line = $0
      sub(/#.*/, "", line)
      if (match(line, /\[[^]]*\]/)) {
        inner = substr(line, RSTART + 1, RLENGTH - 2)
        gsub(/"/, " ", inner)
        gsub(/,/, " ", inner)
        n = split(inner, parts, /[[:space:]]+/)
        for (i = 1; i <= n; i++) if (parts[i] != "") print parts[i]
      }
      exit
    }
  ' "$manifest"
}

# Reads `name` from the `[package]` table of one manifest (section-aware, same
# reason as read_package_version).
read_package_name() {
  local manifest="$1"
  [[ -f "$manifest" ]] || return 2
  awk '
    /^[[:space:]]*\[/ { section = $1; next }
    section == "[package]" && /^[[:space:]]*name[[:space:]]*=/ {
      if (match($0, /"[^"]*"/)) { print substr($0, RSTART + 1, RLENGTH - 2); exit }
    }
  ' "$manifest"
}

# --- the check ---------------------------------------------------------------

run_check() {
  if [[ ! -d "$repo_root" ]]; then
    die "repo root is not a directory: $repo_root" 2
  fi

  local root_manifest="$repo_root/Cargo.toml"
  if [[ ! -f "$root_manifest" ]]; then
    die "no root manifest at $root_manifest" 2
  fi

  # The members: the implicit root package (a root manifest that declares
  # `[package]`) plus every entry of `[workspace] members`. `default-members` is
  # deliberately NOT consulted — cargo hard-errors on a name there that is not a
  # member, so `members` is the complete set.
  local -a manifests=() names=()
  local root_version
  if root_version="$(awk '/^[[:space:]]*\[package\]/ { found = 1; exit } END { exit !found }' "$root_manifest")"; then
    manifests+=("$root_manifest")
  fi

  local entry
  while IFS= read -r entry; do
    [[ -n "$entry" ]] || continue
    # `"."` is accepted by cargo for a root member (and deduped against the
    # implicit one) — see the root manifest's long comment on why `members`
    # forbids it here while `default-members` uses it. Guard both spellings so a
    # future edit cannot double-count the root or resolve it to a missing path.
    if [[ "$entry" == "." || "$entry" == "./" ]]; then
      manifests+=("$root_manifest")
      continue
    fi
    local member_manifest="$repo_root/$entry/Cargo.toml"
    if [[ ! -f "$member_manifest" ]]; then
      die "workspace member '$entry' has no manifest at ${entry}/Cargo.toml — the member list and the tree disagree" 2
    fi
    manifests+=("$member_manifest")
  done < <(read_member_entries "$root_manifest")

  # The anti-vacuity floor. Two packages is this workspace's shape (AGENTS.md), so
  # reading one version means the enumeration stopped working, and a green from here
  # certifies nothing. Refused, not passed.
  if (( ${#manifests[@]} < 2 )); then
    die "found ${#manifests[@]} version-bearing member(s) under $repo_root, expected at least 2 — this workspace is \`fulcrum-core\` plus the \`fulcrum\` member by design, so a check that read one version would be checking nothing" 2
  fi

  # Read every member's version, and its package name (the name is what keys the
  # Cargo.lock lookup, so a renamed member cannot silently drop out of the lock
  # half of the check).
  local -a display=()
  local seen=""
  local i m v name
  for i in "${!manifests[@]}"; do
    m="${manifests[$i]}"
    v="$(read_package_version "$m" || true)"
    if [[ -z "$v" ]]; then
      die "no [package] version found in ${m#"$repo_root"/}" 2
    fi
    name="$(read_package_name "$m" || true)"
    if [[ -z "$name" ]]; then
      die "no [package] name found in ${m#"$repo_root"/} — the lock check needs it" 2
    fi
    display+=("${m#"$repo_root"/}|${name}|${v}")
    seen+="$v"$'\n'
  done

  local distinct
  distinct="$(printf '%s' "$seen" | awk 'NF && !seen[$0]++ { c++ } END { print c + 0 }')"
  local agreed
  agreed="$(printf '%s' "$seen" | awk 'NF { print; exit }')"

  if [[ "$distinct" != "1" ]]; then
    {
      echo "error: the workspace members do not agree on a version"
      for m in "${display[@]}"; do
        printf '  %s\n' "${m//|/: }"
      done
      echo "  a path dependency inside one workspace resolves by PATH, never by version, so this compiles anyway — which is exactly why it needs a check"
    } >&2
    return 1
  fi

  local agreed_version="$agreed"

  # --- Cargo.lock -------------------------------------------------------------
  local lock="$repo_root/Cargo.lock"
  if [[ ! -f "$lock" ]]; then
    die "Cargo.lock is missing — refusing to certify a version agreement it cannot read (this repo commits its lock)" 2
  fi
  local lock_drift=false
  local entry_str pkg_name ver lock_ver
  for entry_str in "${display[@]}"; do
    pkg_name="$(printf '%s' "$entry_str" | cut -d'|' -f2)"
    ver="$(printf '%s' "$entry_str" | cut -d'|' -f3)"
    lock_ver="$(read_lock_version "$lock" "$pkg_name" || true)"
    if [[ -z "$lock_ver" ]]; then
      die "Cargo.lock has no [[package]] entry for '$pkg_name' — the lock and the member list disagree" 2
    fi
    if [[ "$lock_ver" != "$ver" ]]; then
      printf 'error: Cargo.lock pins %s at %s, the manifest says %s\n' "$pkg_name" "$lock_ver" "$ver" >&2
      lock_drift=true
    fi
  done
  if [[ "$lock_drift" == true ]]; then
    printf '  fix: run `cargo check` (or `cargo update --workspace`) in the repo root and commit the lock in the same commit as the bump\n' >&2
    return 1
  fi

  # --- the tag ----------------------------------------------------------------
  # The tag is the version the release ANNOUNCES; the manifests are the version the
  # tree CARRIES. Both are asserted, in this order, so a `v`-less tag is reported as
  # the malformed argument it is rather than as a disagreement.
  if [[ -n "$tag" ]]; then
    if [[ ! "$tag" =~ ^v([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
      die "--tag must be v followed by X.Y.Z (got: $tag)"
    fi
    local tag_version="${BASH_REMATCH[1]}"
    if [[ "$tag_version" != "$agreed_version" ]]; then
      {
        echo "error: the tag does not match the workspace's version"
        printf '  tag %s: %s\n' "$tag" "$tag_version"
        printf '  manifests and lock: %s\n' "$agreed_version"
        echo "  a git dependency pinned with tag + version resolves only if the tagged manifest carries that version, so a tag that outruns the files is a pin nothing can install"
      } >&2
      return 1
    fi
  fi

  printf 'versions match: %s\n' "$agreed_version"
  printf '  %s\n' "${display[@]//|/ }"
  return 0
}

# --- --self-test -------------------------------------------------------------
#
# This guard's job is to go red on a tree that does not exist yet — two members
# disagreeing, a third member nobody added to a hard-coded list, a tag outrunning
# the files. A live run cannot demonstrate any of those, because today everything
# agrees. So each case plants a fixture tree and re-invokes THIS script against it;
# a second copy of the matching logic here would prove nothing about the one that
# runs in CI.
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
  local got=0 output
  # `|| got=$?` and NOT `if ! output=$(...)`: under `if !`, the `!` inverts the
  # status so `$?` inside the branch is 0, and the harness would report
  # "expected 1, got 0" for a script that correctly exited 1 — a false FAIL that
  # looks like a broken guard rather than a broken test.
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

# A two-member workspace in this repo's real shape: root package + one member.
plant_workspace() {
  local root="$1" root_version="$2" member_version="$3" member="${4:-daemon}"
  mkdir -p "$root/$member"
  cat >"$root/Cargo.toml" <<FIXTURE
[workspace]
resolver = "2"
members = ["$member"]
default-members = [".", "$member"]

[package]
name = "engine"
version = "$root_version"
edition = "2021"
FIXTURE
  cat >"$root/$member/Cargo.toml" <<FIXTURE
[package]
name = "$member"
version = "$member_version"
edition = "2021"
FIXTURE
  cat >"$root/Cargo.lock" <<FIXTURE
version = 3

[[package]]
name = "engine"
version = "$root_version"

[[package]]
name = "$member"
version = "$member_version"
FIXTURE
}

run_self_test() {
  local script
  script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  self_test_root="$(mktemp -d)"
  trap self_test_cleanup EXIT

  # THE CLEAN FIXTURE: the real shape, both members and the lock agreeing.
  local clean="$self_test_root/clean"
  plant_workspace "$clean" "0.1.0" "0.1.0"
  expect_exit "two members and the lock agree" 0 bash "$script" "$clean"
  expect_exit "a matching tag passes" 0 bash "$script" "$clean" --tag v0.1.0

  # THE DISAGREEMENT. This is the case the guard exists for, and the case a live
  # run can never show.
  local drift="$self_test_root/drift"
  plant_workspace "$drift" "0.1.0" "0.2.0"
  expect_exit "two members disagreeing fails" 1 bash "$script" "$drift"

  # THE TAG OUTRUNNING THE FILES — the reason --tag exists.
  expect_exit "a tag ahead of the manifests fails" 1 bash "$script" "$clean" --tag v0.2.0
  expect_exit "a tag without the v fails as a bad argument, not as a mismatch" 1 bash "$script" "$clean" --tag 0.1.0
  expect_exit "--tag with no value is refused, not treated as absent" 1 bash "$script" "$clean" --tag

  # THE THIRD-MEMBER WITNESS. A check that hard-coded the two known manifests would
  # read two versions, find them equal, and pass this tree — green over a member
  # nobody compared. The member entry is added to the root manifest, so only a
  # DERIVED member list can see it.
  local third="$self_test_root/third-member"
  plant_workspace "$third" "0.1.0" "0.1.0"
  mkdir -p "$third/extra"
  printf '[package]\nname = "extra"\nversion = "9.9.9"\n' >"$third/extra/Cargo.toml"
  sed -i 's/members = \["daemon"\]/members = ["daemon", "extra"]/' "$third/Cargo.toml"
  printf '\n[[package]]\nname = "extra"\nversion = "9.9.9"\n' >>"$third/Cargo.lock"
  expect_exit "a THIRD member nobody added to a list is still caught" 1 bash "$script" "$third"

  # THE DECOY. `[workspace.dependencies.<name>]` puts a line-start `version` in the
  # root manifest; a naive first-match grep would read whichever appears first and
  # could report the dependency's version as the crate's own. Here the decoy is
  # DIFFERENT from both members on purpose: if the reader ever stops being
  # section-aware, the clean case stops being clean and this goes red.
  local decoy="$self_test_root/decoy"
  plant_workspace "$decoy" "0.1.0" "0.1.0"
  cat >"$decoy/Cargo.toml" <<FIXTURE
[workspace]
resolver = "2"
members = ["daemon"]

[workspace.dependencies.tokio]
version = "1.40.0"

[package]
name = "engine"
version = "0.1.0"
FIXTURE
  expect_exit "a [workspace.dependencies.*] version is NOT read as the crate's" 0 bash "$script" "$decoy"

  # THE STALE LOCK. Cargo self-heals this on the next build, so it is not the drift
  # the guard was born for — but it is drift inside the commit a tag points at, and
  # a `--locked` build step would fail there.
  local stale="$self_test_root/stale-lock"
  plant_workspace "$stale" "0.1.0" "0.1.0"
  sed -i '0,/^version = "0.1.0"/s//version = "0.0.9"/' "$stale/Cargo.lock"
  expect_exit "a lock left behind by the bump fails, naming cargo as the fix" 1 bash "$script" "$stale"

  # THE VACUITY FLOORS: each is "the check could not look", and each must REFUSE
  # rather than pass on an empty comparison.
  local lone="$self_test_root/lone-package"
  mkdir -p "$lone"
  printf '[package]\nname = "only"\nversion = "0.1.0"\n' >"$lone/Cargo.toml"
  printf 'version = 3\n\n[[package]]\nname = "only"\nversion = "0.1.0"\n' >"$lone/Cargo.lock"
  expect_exit "a root with one package is refused, not passed (the floor has teeth)" 2 bash "$script" "$lone"

  local missing="$self_test_root/missing-member"
  plant_workspace "$missing" "0.1.0" "0.1.0"
  rm -rf "$missing/daemon"
  expect_exit "a member named in members with no manifest is refused" 2 bash "$script" "$missing"

  local nover="$self_test_root/no-version"
  plant_workspace "$nover" "0.1.0" "0.1.0"
  sed -i '/^version = "0.1.0"/d' "$nover/daemon/Cargo.toml"
  expect_exit "a member manifest with no [package] version is refused" 2 bash "$script" "$nover"

  local nolock="$self_test_root/no-lock"
  plant_workspace "$nolock" "0.1.0" "0.1.0"
  rm -f "$nolock/Cargo.lock"
  expect_exit "a missing Cargo.lock is refused, not skipped" 2 bash "$script" "$nolock"

  expect_exit "a repo root that is not a directory is refused" 2 bash "$script" "$self_test_root/does-not-exist"

  if ((self_test_failures > 0)); then
    printf '\nself-test: %s case(s) FAILED — the version guard cannot be trusted to fail\n' "$self_test_failures" >&2
    return 1
  fi
  printf '\nself-test: all cases behaved (the guard has teeth)\n'
  return 0
}

if [[ "$self_test" == true ]]; then
  run_self_test
  exit $?
fi

run_check
exit $?

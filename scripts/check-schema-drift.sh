#!/usr/bin/env bash
#
# Fail if a DDL's schema SHAPE has drifted from the pinned fingerprint.
#
# The daemon's four tables (`fulcrum/src/db/schema.rs`) are a TRANSCRIPTION of the
# desktop's (`src-tauri/src/storage/db.rs`), and the daemon opens a DIFFERENT file
# (`fulcrum.db`, not `archimedes.db`), so nothing at runtime can notice the two
# owners moving apart. The mappers read columns BY INDEX
# (`Db::SESSION_COLUMNS` / `Db::MESSAGE_COLUMNS`), so the drift that matters is not
# "a constraint weakened" — it is "slot 7 is not the column my mapper reads out of
# slot 7", whose symptom is an `InvalidColumnType` that `persistence.rs` turns into
# `Ok(None)`: a session that quietly stopped existing. This guard is the thing that
# makes that loud. It compares the shape TEXT of a DDL against the checked-in
# fingerprint `schema-sha.txt` and NAMES the table that disagrees.
#
# WHAT IS COMPARED, and it is a deliberate subset. Per table: every column's NAME
# and DECLARED TYPE, in declaration order, plus the `idx_messages_session` index
# statement. NOT pinned here: `NOT NULL` / `DEFAULT` / `PRIMARY KEY` flags, the two
# `UNIQUE` upsert keys, the two `ON DELETE CASCADE`s (those are pinned by probes
# that ask the DATABASE — `Db::table_has_unique` and the cascade tests in
# `fulcrum/tests/db.rs`), the migration chain, or the SELECT-list subsets
# (`Db::SESSION_COLUMNS` is 10 of `sessions`' 12 columns and the desktop's
# `SPACE_COLUMNS` is 4 of 5 — comparing the subsets would FALSE-POSITIVE on
# `spaces.sort_order`). This is exactly `ColumnShape`'s documented pin philosophy,
# and it is the same fact `TABLE_SHAPES` already holds, so the fingerprint is that
# constant SERIALISED — not a second representation of the same fact, because two
# fingerprints are two things that can drift from each other.
#
# TEXT, NOT A HASH, ON PURPOSE. A hash comparison reports "digests differ" and
# leaves a human to bisect four tables by hand; comparing the per-table `name:type`
# digests names the drifted table in the FIRST line of output. It also makes a
# re-indent a non-event (lines are trimmed, blanks dropped) where a hash would
# scream, and it needs no hashing dependency in either repo.
#
# ⚠ THE VACUITY FLOOR, which is the whole reason this guard is trusted. Every
# "no drift found" here is guarded by a check that there WAS something to compare:
# a missing or unreadable pin, a pin that does not name all five keys exactly once,
# a DDL whose extraction named no table at all, and a missing other side all exit 2
# with a NAMED reason instead of passing. That is not hypothetical: the desktop has no
# comparator step at all, so its half of this comparison never runs and the only leg
# that does is this repo's. A check that finds nothing to compare is a lie, and it is
# refused.
#
# The fulcrum side never fetches the desktop's DDL at a guessed rev — comparing a
# pair that no build ships together judges nothing. Each repo hands this comparator
# its OWN live DDL (the desktop via `--input`, see the README section), and the pin
# is the single artifact both sides agree on.
#
# Usage: check-schema-drift.sh [repo-root] [--input FILE] [--schema FILE] [--self-test]
#   repo-root    the checkout whose DDL and pin are read (default: .)
#   --input      a raw DDL interior (the lines between `r#"` and `"#` of the repo's
#                `SCHEMA` const) instead of extracting this repo's own — the captured
#                form the fixture harness uses, and the form the desktop's CI uses
#   --schema     the fingerprint to compare against (default: <repo-root>/schema-sha.txt)
#   --self-test  plant fixtures under a throwaway root and prove THIS guard —
#                re-invoked, never a second implementation — can still fail
#
# Exit codes: 0 the shape matches the pin, 1 DRIFT (the offending table or index is
# NAMED on the first error line), 2 the check could not be run honestly — bad
# arguments, an unreadable or missing pin, an unextractable DDL, or a fingerprint
# that cannot name all five keys.

set -euo pipefail

usage() {
  echo "usage: $(basename "$0") [repo-root] [--input FILE] [--schema FILE] [--self-test]" >&2
}

# The five keys the fingerprint must name: four tables plus the index. ONE list, so
# the vacuity floor and the comparison loop cannot disagree about what "complete"
# means. Plain array, not an associative one — this script has to run unchanged in
# the desktop's CI, which may carry an older bash.
pin_keys=(sessions messages native_messages spaces idx_messages_session)

# The sectioned text of a DDL interior on stdin: a table's name, then one
# `name:type` line per column line, then the index section LAST (the pin's order,
# and stable regardless of where the index sits in the DDL — the daemon declares the
# index BETWEEN `messages` and `native_messages`, so emitting it in place would make
# the section ORDER an artifact of the DDL's layout rather than of the contract).
#
# EVERY line is trimmed into `line` FIRST and every pattern then matches `line`, so
# the whole thing is whitespace-insensitive including the `CREATE TABLE` /
# `CREATE INDEX` / `);` markers. That ordering is load-bearing and the self-test's
# re-indent case is what caught it: matching `^CREATE TABLE` on the raw record made
# an indented DDL read as "table missing", which is a false POSITIVE for drift — the
# one failure mode worse than a miss, because it makes the guard permanently red and
# therefore ignored. The desktop is a different codebase with its own formatting; it
# must not have to re-indent its DDL to satisfy this comparator.
#
# Column extraction mirrors what `TABLE_SHAPES` pins: the first whitespace token is
# the name, the second is the declared type with any trailing comma stripped (so
# `id TEXT PRIMARY KEY` yields `id:TEXT`, matching `ColumnShape.decl_type`). Table
# bodies end with `);`, and the table-level `UNIQUE(...)` / `PRIMARY KEY(...)`
# constraints are NOT columns — dropping them is what keeps this in agreement with
# the Rust-side parser in `schema.rs`'s own tests, which filters the same two.
extract_sections() {
  awk '
    {
      line = $0
      sub(/^[[:space:]]*/, "", line)
      sub(/[[:space:]]+$/, "", line)
    }
    line == "" { next }
    line ~ /^CREATE TABLE IF NOT EXISTS / {
      name = line
      sub(/^CREATE TABLE IF NOT EXISTS /, "", name)
      sub(/ \(.*/, "", name)
      print name
      next
    }
    line ~ /^CREATE INDEX / { idx = idx line "\n"; next }
    line == ");" { next }
    line ~ /^UNIQUE/ { next }
    line ~ /^PRIMARY KEY/ { next }
    {
      split(line, f, "[[:space:]]+")
      sub(/,+$/, "", f[2])
      print f[1] ":" f[2]
      next
    }
    END { if (idx != "") printf "idx_messages_session\n%s", idx }
  '
}

# The pin file's canonical text: comment lines (`#` at column 0 — the contract's
# prose, not its data) and blank lines dropped. Byte-for-byte the normalisation the
# integration test `the_pinned_schema_fingerprint_matches_the_shipped_file` in
# `fulcrum/tests/db.rs` applies, so `cargo test` and CI ask one question.
canonicalise_pin() {
  grep -v '^#' "$1" | grep -v '^[[:space:]]*$' || true
}

# Which lines of sectioned text on stdin are SECTION HEADERS. A column line always
# carries its `name:type` colon and the index statement always starts with
# `CREATE INDEX `, so anything else is a header. (That is also how the Rust test
# finds the headers, which is the point: ONE definition of "header" on both sides.)
header_lines() {
  awk '
    /^[[:space:]]*$/ { next }
    /:/ { next }
    /^CREATE INDEX / { next }
    {
      line = $0
      sub(/^[[:space:]]*/, "", line)
      sub(/[[:space:]]+$/, "", line)
      print line
      next
    }
  '
}

# The body of one section, read from sectioned text on stdin: every non-header line
# after the header that equals `$1`, stopping at the next header. Bash-3-safe and
# non-associative, so it ports to the desktop's runner unchanged.
section_of() {
  local key="$1"
  awk -v want="$key" '
    /^[[:space:]]*$/ { next }
    {
      line = $0
      sub(/^[[:space:]]*/, "", line)
      sub(/[[:space:]]+$/, "", line)
      if (line !~ /:/ && line !~ /^CREATE INDEX /) { on = (line == want); next }
      if (on) print line
    }
  '
}

# Show the first line where two section bodies differ, one line per side. The
# machinery is free; the requirement is that a human sees WHICH column moved
# without diffing two files by hand.
show_first_difference() {
  local label="$1" pinned="$2" other="$3"
  local -a p_lines=() o_lines=()
  local line
  while IFS= read -r line; do p_lines+=("$line"); done <<<"$pinned"
  while IFS= read -r line; do o_lines+=("$line"); done <<<"$other"

  local max=${#p_lines[@]}
  if (( ${#o_lines[@]} > max )); then max=${#o_lines[@]}; fi
  local i=0 left right
  while (( i < max )); do
    left="${p_lines[$i]:-<absent>}"
    right="${o_lines[$i]:-<absent>}"
    if [[ "$left" != "$right" ]]; then
      printf '    pinned: %s\n' "$left" >&2
      printf '    %s: %s\n' "$label" "$right" >&2
      return 0
    fi
    i=$((i + 1))
  done
  echo "    (the two sections differ only in length)" >&2
}

# The comparison. Args: the canonical pin text, the sectioned DDL text, and a label
# naming the DDL source in the messages. Returns 0 agree / 1 drift / 2 not runnable.
compare() {
  local pin_text="$1" ddl_text="$2" source_label="$3"

  # --- VACUITY FLOOR 1: the pin must name all five keys, each exactly once. -----
  # A pin that LOST a section would compare equal to a DDL that lost the same one:
  # a check that passes because it stopped looking. A pin that GAINED a sixth names
  # a table no build ships. Both are refusals, not comparisons.
  local pin_headers missing extra duplicates
  pin_headers="$(header_lines <<<"$pin_text")"
  if [[ -z "$pin_headers" ]]; then
    {
      echo "error: the schema-shape pin names no table at all — nothing to compare,"
      echo "  and a comparison that finds nothing is a lie"
    } >&2
    return 2
  fi
  missing="$(comm -23 <(printf '%s\n' "${pin_keys[@]}" | sort) <(printf '%s\n' "$pin_headers" | sort))"
  extra="$(comm -13 <(printf '%s\n' "${pin_keys[@]}" | sort) <(printf '%s\n' "$pin_headers" | sort))"
  duplicates="$(printf '%s\n' "$pin_headers" | sort | uniq -d)"
  if [[ -n "$missing" || -n "$extra" || -n "$duplicates" ]]; then
    echo "error: the schema-shape pin does not name the contract it claims to pin" >&2
    if [[ -n "$missing" ]]; then
      printf '  missing section(s):    %s\n' "$(tr '\n' ' ' <<<"$missing")" >&2
    fi
    if [[ -n "$extra" ]]; then
      printf '  unexpected section(s): %s\n' "$(tr '\n' ' ' <<<"$extra")" >&2
    fi
    if [[ -n "$duplicates" ]]; then
      printf '  duplicated section(s): %s\n' "$(tr '\n' ' ' <<<"$duplicates")" >&2
    fi
    echo "  a fingerprint that cannot name every table (and names nothing extra) is" >&2
    echo "  not a comparison — it would pass by omitting the table that drifted" >&2
    return 2
  fi

  # --- VACUITY FLOOR 2: the DDL side must have named SOMETHING. -----------------
  # An extraction that found no table is not a DDL that lost all five tables; it is
  # the extractor failing (a renamed const, a rewritten `CREATE TABLE`, a file that
  # is not the interior at all). Reporting five drifts would send someone hunting a
  # schema bug that does not exist, so this is exit 2, not exit 1.
  local ddl_headers
  ddl_headers="$(header_lines <<<"$ddl_text")"
  if [[ -z "$ddl_headers" ]]; then
    {
      echo "error: the DDL from $source_label declares no table at all"
      echo "  the extraction found no \`CREATE TABLE IF NOT EXISTS\` header, so it"
      echo "  cannot answer a question about shapes — reporting agreement here would"
      echo "  be a guard answering a question it was not asked"
    } >&2
    return 2
  fi

  # A table in the DDL that the pin does not name IS drift — that is the shape a
  # fifth table takes, and it is the one direction the pin's own floor cannot see.
  local unpinne
  for unpinne in $ddl_headers; do
    local known=0 key
    for key in "${pin_keys[@]}"; do
      if [[ "$unpinne" == "$key" ]]; then known=1; break; fi
    done
    if (( known == 0 )); then
      {
        echo "error: schema drift in '$unpinne': the pin describes no '$unpinne' section"
        echo "  $source_label declares a table the fingerprint does not pin, so the"
        echo "  two owners no longer agree on what the store contains"
      } >&2
      return 1
    fi
  done

  # Per-key comparison, which is what lets the FIRST error line name the table.
  local key pinned_here ddl_here drift=0
  for key in "${pin_keys[@]}"; do
    pinned_here="$(section_of "$key" <<<"$pin_text")"
    ddl_here="$(section_of "$key" <<<"$ddl_text")"

    if [[ -z "$ddl_here" ]]; then
      {
        echo "error: schema drift in '$key': $source_label declares no '$key' section — a table or the index went missing"
        echo "  the pin carries $(grep -c . <<<"$pinned_here") line(s) for it; the DDL carries none"
      } >&2
      return 1
    fi
    if [[ -z "$pinned_here" ]]; then
      # Unreachable while floor 1 holds (a named header always yields a body here
      # unless the pin declared a headerless table), but a silent pass is never an
      # acceptable fallback for "one side had nothing to say".
      echo "error: schema drift in '$key': the pin names '$key' but declares no columns for it" >&2
      return 1
    fi

    if [[ "$pinned_here" != "$ddl_here" ]]; then
      {
        echo "error: schema drift in '$key': the pinned fingerprint and $source_label disagree"
        show_first_difference "$source_label" "$pinned_here" "$ddl_here"
        echo "  the mappers read columns BY INDEX, so a renamed, retyped, reordered," >&2
        echo "  added or removed column is corruption here, not a cosmetic diff." >&2
        echo "  update both repos' DDL and this pin in the SAME commit, or revert." >&2
      } >&2
      drift=1
      break
    fi
  done

  if (( drift == 1 )); then
    return 1
  fi

  echo "schema shape matches the pinned fingerprint: sessions, messages, native_messages, spaces and idx_messages_session all agree between $pin_file and $source_label"
  return 0
}

# --- --self-test: the premise mode --------------------------------------------
#
# The guard's whole job is to go red on a shape that is NOT in the tree today — the
# two DDLs are byte-identical right now, so a live run CANNOT demonstrate the
# failure it exists for. Each case therefore plants a fixture and re-invokes THIS
# script through `--input`/`--schema`; a second copy of the comparison logic here
# would prove nothing about the one that runs in CI.
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
  # The repo's own idiom (check-rquickjs-allocator.sh:expect_exit): `|| got=$?` and
  # NOT `if ! output=$(...)`. Under `if ! cmd` the `!` inverts the status, so `$?`
  # read inside the then-branch is 0 — the harness would then report "expected exit
  # 1, got 0" for a script that exited 1, a false FAIL that looks like a broken
  # guard rather than a broken test.
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

# Like expect_exit, but ALSO insists the output names a specific table/index. The
# exit code alone is not enough: a guard that exits 1 while printing "hashes differ"
# is useless to whoever has to fix it, and naming the drifted key is the contract.
expect_exit_naming() {
  local what="$1" want="$2" needle="$3"
  shift 3
  local got=0 output
  output="$("$@" 2>&1)" || got=$?
  if [[ "$got" != "$want" ]]; then
    printf 'FAIL %s\n' "$what"
    printf '       expected exit %s, got %s\n' "$want" "$got"
    printf '       output: %s\n' "${output:-<empty>}"
    self_test_failures=$((self_test_failures + 1))
  elif ! grep -q -- "$needle" <<<"$output"; then
    printf 'FAIL %s\n' "$what"
    printf '       exit %s is right but the output never names `%s`\n' "$got" "$needle"
    printf '       output: %s\n' "${output:-<empty>}"
    self_test_failures=$((self_test_failures + 1))
  else
    printf 'ok   %s (exit %s, names `%s`)\n' "$what" "$got" "$needle"
  fi
}

run_self_test() {
  # Canonicalised the same way the other self-testing guard does it
  # (check-rquickjs-allocator.sh:run_self_test): the fixtures are re-read by THIS
  # file, so the path must not depend on the caller's cwd.
  local script
  script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  local repo_root
  repo_root="$(cd "$(dirname "$script")/.." && pwd)"
  self_test_root="$(mktemp -d)"
  trap self_test_cleanup EXIT

  # THE CLEAN FIXTURE: the real pin, plus a capture of the real DDL interior taken
  # with the SAME extractor live mode uses. It is captured, not transcribed, so the
  # clean case cannot rot into a fixture that agrees only with itself — if the DDL
  # and the pin ever move apart, case 1 goes red and someone re-captures.
  local pin="$self_test_root/pin.txt"
  cp "$repo_root/schema-sha.txt" "$pin"
  local clean="$self_test_root/clean.sql"
  extract_interior "$repo_root/fulcrum/src/db/schema.rs" >"$clean"
  if [[ ! -s "$clean" ]]; then
    echo "error: --self-test could not capture the live DDL interior from $repo_root/fulcrum/src/db/schema.rs" >&2
    return 2
  fi

  expect_exit "the shipped DDL matches the shipped pin (the day-one state)" 0 \
    bash "$script" --input "$clean" --schema "$pin"

  # --- THE MUTATIONS, one per drift direction issue #6 was filed for. -----------

  # A RETYPED column. `cost_usd REAL` → `TEXT` is the case the daemon's own docs
  # call out: SQLite's dynamic typing means this does NOT fail loudly — it coerces,
  # and the daemon stores a value class it did not ask for.
  local retyped="$self_test_root/retyped.sql"
  sed 's/^    cost_usd REAL,$/    cost_usd TEXT,/' "$clean" >"$retyped"
  expect_exit_naming "a retyped sessions.cost_usd is drift" 1 sessions \
    bash "$script" --input "$retyped" --schema "$pin"

  # An ADDED column — the desktop's most likely move (`ADD COLUMN` gates are its
  # whole migration history). The daemon would not read it and would say nothing.
  local added="$self_test_root/added.sql"
  sed 's/^    message_key TEXT,$/    message_key TEXT,\n    extra TEXT,/' "$clean" >"$added"
  expect_exit_naming "an added messages column is drift" 1 messages \
    bash "$script" --input "$added" --schema "$pin"

  # A REORDERED pair of columns — same names, same types, same count, DIFFERENT
  # order. This is the drift an order-insensitive (name-set) comparison cannot see,
  # and it is the exact failure issue #6 was filed for: the mappers read columns BY
  # INDEX, so swapping `trusted` and `sort_order` makes every later slot read the
  # wrong column and the symptom is a coerced value or an `Ok(None)`, not an error.
  # Verified by measurement, not by assertion: the digest is a LINE SEQUENCE.
  local reordered="$self_test_root/reordered.sql"
  awk '
    /^    trusted INTEGER NOT NULL DEFAULT 0,$/ { trust = $0; next }
    trust != "" { print $0; print trust; trust = ""; next }
    { print }
  ' "$clean" >"$reordered"
  if cmp -s "$clean" "$reordered"; then
    printf 'FAIL the reorder fixture is identical to the clean DDL (no mutation planted)\n'
    self_test_failures=$((self_test_failures + 1))
  else
    expect_exit_naming "a reordered column pair is drift (the mappers read by INDEX)" 1 spaces \
      bash "$script" --input "$reordered" --schema "$pin"
  fi

  # A REMOVED table (the renamed-table case: the old name simply stops existing).
  local no_table="$self_test_root/no-sessions.sql"
  awk '
    /^CREATE TABLE IF NOT EXISTS sessions \(/ { skip = 1 }
    skip && /^\);/ { skip = 0; next }
    skip { next }
    { print }
  ' "$clean" >"$no_table"
  expect_exit_naming "a missing sessions table is drift" 1 sessions \
    bash "$script" --input "$no_table" --schema "$pin"

  # A DROPPED INDEX. Nothing in any column list changes, so a fingerprint of columns
  # alone would call this clean — the transcript read becomes a full scan.
  local no_index="$self_test_root/no-index.sql"
  grep -v '^CREATE INDEX ' "$clean" >"$no_index"
  expect_exit_naming "a dropped idx_messages_session is drift" 1 idx_messages_session \
    bash "$script" --input "$no_index" --schema "$pin"

  # A FIFTH TABLE the pin never heard of.
  local extra_table="$self_test_root/extra-table.sql"
  {
    cat "$clean"
    printf 'CREATE TABLE IF NOT EXISTS extra (\n    id TEXT PRIMARY KEY\n);\n'
  } >"$extra_table"
  expect_exit_naming "an unpinned fifth table is drift" 1 extra \
    bash "$script" --input "$extra_table" --schema "$pin"

  # --- THE REFUSALS: exit 2, because these are not comparisons at all. ----------

  # A pin missing a section. This is the vacuity floor: without it, a pin that
  # dropped `native_messages` would agree with a DDL that dropped the same table,
  # and the guard would report green while checking three tables.
  local short_pin="$self_test_root/pin-missing-section.txt"
  awk '
    /^native_messages$/ { skip = 1; next }
    skip && !/:/ && !/^CREATE INDEX / && !/^[[:space:]]*$/ { skip = 0 }
    skip { next }
    { print }
  ' "$pin" >"$short_pin"
  expect_exit "a pin missing a section is REFUSED, not compared" 2 \
    bash "$script" --input "$clean" --schema "$short_pin"

  # A pin that pins a SIXTH header: it describes a table no build ships, so every
  # comparison against a real DDL would report a phantom drift — and the honest
  # answer is "this fingerprint is malformed", not a table-by-table verdict.
  local sixth_pin="$self_test_root/pin-sixth.txt"
  {
    cat "$pin"
    printf 'phantom\nid:TEXT\n'
  } >"$sixth_pin"
  expect_exit "a fingerprint pinning a sixth header is REFUSED" 2 \
    bash "$script" --input "$clean" --schema "$sixth_pin"

  # The MISSING OTHER SIDE — today's real cross-repo state, not a hypothesis: the
  # desktop has no comparator step, so a run that lands here has no pin to read. A
  # guard that exited 0 on "I found no pin to violate" would be the most dangerous
  # version of itself.
  expect_exit_naming "a missing pin file is REFUSED with a named reason" 2 "no schema-shape pin" \
    bash "$script" --input "$clean" --schema "$self_test_root/does-not-exist.txt"

  # The same refusal on the DDL side: a --input that cannot be read is not a DDL
  # that agrees with the pin.
  expect_exit_naming "an unreadable DDL side is REFUSED and names the file" 2 no-such-ddl.sql \
    bash "$script" --input "$self_test_root/no-such-ddl.sql" --schema "$pin"

  # A pin that is not a pin at all (prose plus binary noise): no headers, no
  # comparison, exit 2 rather than "nothing drifted".
  local garbage="$self_test_root/pin-garbage.txt"
  printf 'this file is not a fingerprint\n\001\002garbage\n' >"$garbage"
  expect_exit "a pin with no sections at all is REFUSED" 2 \
    bash "$script" --input "$clean" --schema "$garbage"

  # A DDL that names no table (the extractor failed): exit 2, not five drifts.
  local no_ddl="$self_test_root/empty-ddl.sql"
  printf -- '-- a file with no CREATE TABLE in it\n' >"$no_ddl"
  expect_exit "a DDL declaring no table is REFUSED, not reported as five drifts" 2 \
    bash "$script" --input "$no_ddl" --schema "$pin"

  # --- THE FALSE-POSITIVE TRAP (this family's allocator-api2 case). -------------
  # A re-indent plus extra blank lines is NOT drift: the contract pins names, types
  # and order, and every line is trimmed. Without this case, a guard that compared
  # raw lines would sail through its own tests and then redden CI on a whitespace
  # pass over the DDL — permanently red, therefore useless.
  local reindented="$self_test_root/reindented.sql"
  sed -e 's/^    /        /' -e 's/^CREATE TABLE/  CREATE TABLE/' "$clean" \
    | awk '{ print; if ($0 ~ /^\);/) print "" }' >"$reindented"
  expect_exit "whitespace churn does NOT false-positive" 0 \
    bash "$script" --input "$reindented" --schema "$pin"

  # The comment lines of the REAL pin are prose, not data: the same DDL against a
  # pin with its comments stripped gives the same verdict, which is the shell side
  # of the normalisation the Rust integration test performs.
  local bare_pin="$self_test_root/pin-bare.txt"
  grep -v '^#' "$pin" >"$bare_pin"
  expect_exit "pin comments carry no comparison weight" 0 \
    bash "$script" --input "$clean" --schema "$bare_pin"

  if ((self_test_failures > 0)); then
    printf '\nself-test: %s case(s) FAILED — the schema-drift guard cannot be trusted to fail\n' "$self_test_failures" >&2
    return 1
  fi
  printf '\nself-test: all cases behaved (the guard has teeth)\n'
  return 0
}

# Pull the DDL interior out of a Rust source file: the lines between the `r#"` that
# opens the `SCHEMA` const and the `"#;` that closes it. The same two anchors the
# repo's own source-lint style checks use, and deliberately SED rather than a Rust
# route — this guard has to run in the desktop's CI too, where the const lives in a
# different file and cargo may not even be installed.
extract_interior() {
  local src="$1"
  sed -n '/^pub const SCHEMA: &str = r#"/,/^"#;$/p' "$src" | sed '1d;$d'
}

# --- arguments ----------------------------------------------------------------

repo_root=""
input_file=""
schema_file=""
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
    # Both of these are OPTIONS and not positionals: an empty or missing value must
    # be an error rather than "read stdin", which would hang a CI step waiting on a
    # file that was never passed (the check-rquickjs-allocator.sh `--input` idiom).
    --input)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --input requires a value (a file holding the DDL interior)" >&2
        usage
        exit 2
      fi
      input_file="$2"
      shift 2
      ;;
    --input=*)
      input_file="${1#--input=}"
      if [[ -z "$input_file" ]]; then
        echo "error: --input requires a value (a file holding the DDL interior)" >&2
        usage
        exit 2
      fi
      shift
      ;;
    --schema)
      if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "error: --schema requires a value (the schema-shape pin to compare against)" >&2
        usage
        exit 2
      fi
      schema_file="$2"
      shift 2
      ;;
    --schema=*)
      schema_file="${1#--schema=}"
      if [[ -z "$schema_file" ]]; then
        echo "error: --schema requires a value (the schema-shape pin to compare against)" >&2
        usage
        exit 2
      fi
      shift
      ;;
    -*)
      echo "error: unknown argument: $1" >&2
      usage
      exit 2
      ;;
    *)
      if [[ -n "$repo_root" ]]; then
        echo "error: unexpected argument: $1" >&2
        usage
        exit 2
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
  exit 2
fi

pin_file="${schema_file:-$repo_root/schema-sha.txt}"

# --- THE VACUITY FLOOR ON THE PIN ITSELF ---------------------------------------
# Missing/unreadable is exit 2 with the reason spelled out. The reason deliberately
# does NOT name a fulcrum rev any more. It used to cite
# `78e595436ac5612d8ef87f692bee61f48f6e58e7` as "the rev the desktop pins, which
# ships no schema-sha.txt" — true of that rev, and the reason the desktop leg had to
# refuse when this was written. The desktop's pin has since moved to a rev that DOES
# publish `schema-sha.txt`, so naming a rev here would make this message a claim about
# another repo's manifest: stale the moment it re-pins, and wrong in the common case.
# The condition being reported is about THIS file, so that is what it reports.
if [[ ! -f "$pin_file" || ! -r "$pin_file" ]]; then
  {
    echo "error: no schema-shape pin at $pin_file — nothing to compare, and a comparison that"
    echo "  finds nothing is a lie. Run this from a tree that carries schema-sha.txt (the"
    echo "  fulcrum repo does; a checkout of an older fulcrum rev may not), or pass --schema."
    echo "  Refusing rather than reporting a shape it could not read."
  } >&2
  exit 2
fi

pin_text="$(canonicalise_pin "$pin_file")"

if [[ -n "$input_file" ]]; then
  if [[ ! -f "$input_file" || ! -r "$input_file" ]]; then
    echo "error: the DDL side is not a readable file: $input_file — refusing to report a shape it could not read" >&2
    exit 2
  fi
  interior="$(cat "$input_file")"
  source_label="--input $input_file"
else
  # The live path: THIS repo's own DDL. Never the desktop's at a guessed rev —
  # judging a pair that no build ships together judges nothing.
  db_src="$repo_root/fulcrum/src/db/schema.rs"
  if [[ ! -f "$db_src" || ! -r "$db_src" ]]; then
    echo "error: no daemon schema at $db_src — pass the repo root, or --input with a captured DDL interior" >&2
    exit 2
  fi
  interior="$(extract_interior "$db_src")"
  source_label="$db_src"
  if [[ -z "${interior//[[:space:]]/}" ]]; then
    {
      echo "error: could not extract the SCHEMA const interior from $db_src"
      echo "  the anchors \`pub const SCHEMA: &str = r#\"\` and \`\"#;\` did not both"
      echo "  match, so the DDL was never read — reporting 'no drift' here would be a"
      echo "  guard answering a question it was not asked"
    } >&2
    exit 2
  fi
fi

ddl_text="$(extract_sections <<<"$interior")"
compare "$pin_text" "$ddl_text" "$source_label"
exit $?

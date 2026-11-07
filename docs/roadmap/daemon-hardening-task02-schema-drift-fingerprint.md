---
status: committed
parent: docs/roadmap/daemon-hardening-status.md
issue: 6
done-when: >-
  The repo ships the canonical schema-shape pin `schema-sha.txt` (serialising the
  daemon's existing `TABLE_SHAPES` per-table name+type lists plus the index line),
  the guard `scripts/check-schema-drift.sh` with a `--self-test` that can demonstrably
  go red, and the new workflow `.github/workflows/schema-drift.yml` running
  self-test + live check on `ubuntu-latest`; the module header of
  `fulcrum/src/db/schema.rs` describes the real mechanism (the fingerprint contract),
  not the old "verified against 4f9b99dc by inspection" claim; a change to ONLY the
  daemon's DDL makes `cargo test -p fulcrum` go red naming the drifted table; the
  fingerprint matches the live DDL on this branch's base (first run GREEN, tripwire
  not firefighting); `README.md` and `AGENTS.md` document the contract, the update
  path, and the honest degraded state of the desktop half (its pinned rev
  `78e59543` publishes no fingerprint, so the desktop-side comparison refuses
  loudly rather than passing vacuously); no Cargo dependency added, no
  `boundary_pins` widening, no desktop-repo edit, the two `UNIQUE` constraints and
  both `ON DELETE CASCADE`s untouched.
---

# daemon-hardening — T2: cross-repo schema-drift fingerprint (issue #6)

**Goal:** Turn the daemon's hand-copy of the desktop's DDL — currently a silent,
self-referential guess — into a pinned, compared, named contract: the daemon
publishes a canonical shape fingerprint, a guard script (reusable cross-repo)
compares it against a schema's actual DDL and names the drifted table, and CI runs
the guard plus a self-test proving it can fail. The desktop's half (running the
comparison in its own CI against the pinned rev's fingerprint) is a documented
follow-up in the other repository; until it exists, the comparison FAILS LOUDLY with
a named reason when the other side is missing — never a vacuous pass.

**Architecture:** Serialise what already ships. `table_shapes`/`*_TABLE` in
`fulcrum/src/db/schema.rs` already hold the per-table (name, declared type, order)
fingerprint and a self-test proving it matches the DDL. T2 emits that same fact as a
checked-in text file (`schema-sha.txt`) — five sections: the four tables plus the
`idx_messages_session` index — and verifies the file against the live DDL twice: in
`cargo test` (a new integration test in `fulcrum/tests/db.rs` regenerates the
sections from `TABLE_SHAPES` and diffs the file), and in CI via a new bash guard
(`scripts/check-schema-drift.sh`) that is the reusable cross-repo comparator. The
comparison is by normalised text (whitespace-insensitive), NOT by hash, so a
mismatch names the table directly and a re-indent is not drift. Comparison targets:
the daemon's DDL is compared to `schema-sha.txt` in fulcrum CI (green today, red the
moment a daemon DDL change lands without updating the pin). The desktop's DDL is
compared to the same fingerprint by the desktop's own CI (future, out of scope here);
the desktop's pin rev `78e595436ac5612d8ef87f692bee61f48f6e58e7` ships **no**
fingerprint, so that run — when it exists — refuses with a named reason by design,
which is the honest degraded state this task plants the tripwire for.

**Tech Stack:** Bash (guard + self-test, following `check-rquickjs-allocator.sh`'s
in-file idioms), GitHub Actions YAML (single new workflow), Rust (the fingerprint
builder + integration tests in the daemon member), `grep`/`awk`/`diff` only. **No new
Cargo dependency** (no `sha2`, no `hex` — hashing is deliberately avoided; the file
content IS the fingerprint, so nothing transforms it).

---

## Parallelisation

**Verdict:** SERIAL — this is a consumed-mode plan for exactly ONE task; there is
nothing to parallelise with. The caller (`roadmap`) groups T1∥T2∥T3 into wave 1 by
disjoint file sets; within this plan there is a single task whose files are its own.

---

### Task 1: Ship the schema-shape pin, the drift guard, the workflow, the docs

**Context:**

The daemon's `fulcrum/src/db/schema.rs` transcribes four tables from the desktop's
`src-tauri/src/storage/db.rs`, but nothing keeps the two in lockstep. The existing
guard `schema_text_holds_the_desktops_shape` asserts facts about `SCHEMA` — the
daemon's OWN DDL string — so it verifies the daemon's belief against itself. The
mappers read columns BY INDEX (`Db::SESSION_COLUMNS` / `Db::MESSAGE_COLUMNS`), so
drift surfaces as silent corruption (a session that quietly stops existing), never a
type error. This task adopts the issue's **option 1**: export a canonical fingerprint
from each repo, compare in CI, fail when they disagree — without inventing a fourth
mechanism and without touching the desktop (no shared crate, no desktop migration
chain).

Design decisions already made (do not re-litigate):

- **REUSE the fingerprint that already ships.** `schema.rs` already defines
  `ColumnShape { name, decl_type }`, `TableShape { name, columns }`, the four
  `*_TABLE` consts and `TABLE_SHAPES` (name + declared type, ordered), with
  `the_pinned_shapes_match_the_ddl_they_claim_to_describe` proving they match
  `SCHEMA`. The shipped pin serialises THOSE constants — a second fingerprint form
  would be two things that drift. **Consequently the pin is a text file, not a hash**:
  hashing would add a transform (and, without a manifest edit — which this task does
  not own — a hand-rolled hash) between the one source of truth and the contract.
  The checked-in file carries the canonical `name:type` lines themselves, which is
  the serialisation of `TABLE_SHAPES` + the index line, and a mismatch names the
  table directly. The filename `schema-sha.txt` is the caller's artifact name; its
  content is the shape serialisation (documented in the file's header comment).
- **What the fingerprint compares — say it out loud.** Per table: every column's
  NAME and DECLARED TYPE, in declaration order, plus the `idx_messages_session`
  index line. It does NOT pin `NOT NULL`/`DEFAULT`/`PRIMARY KEY` flags, the `UNIQUE`
  constraints or the `ON DELETE CASCADE`s (those are pinned elsewhere by
  DB-asking probes: `Db::table_has_unique`, `deleting_a_session_cascades_to_its_messages`),
  the migration chain, or the SELECT-list subsets (`Db::SESSION_COLUMNS` is 10 of 12
  `sessions` columns; the desktop's `SPACE_COLUMNS` is 4 of 5 — comparing those
  would FALSE-POSITIVE on `spaces.sort_order`). Whitespace is not drift (each line is
  trimmed, blank lines dropped); a renamed/retyped/reordered/added/removed column or
  a lost index IS drift. This matches `ColumnShape`'s documented pin philosophy
  exactly.
- **The honest degraded case is the DESIGN, not an afterthought.** The desktop
  pins fulcrum at rev `78e595436ac5612d8ef87f692bee61f48f6e58e7` (verified:
  `archimedes/src-tauri/Cargo.toml:47`). That rev is a FULCRUM commit (it is in this
  repo's history, ancestor of the branch base `958306d`) whose tree has **no `fulcrum`
  member and no schema** — so it publishes no fingerprint. The cross-repo comparison
  therefore CANNOT run green against the pin today; when the desktop's half lands, it
  must refuse loudly ("the pinned rev publishes no fingerprint") rather than pass
  because it found nothing. This repo's CI runs the comparison against THIS repo's
  live DDL (which exists and matches), so the first run is GREEN, and the guard's
  `--self-test` plants the missing-other-side case to prove the refusal has teeth.
  The fulcrum side NEVER fetches the desktop's DDL at some guessed rev — comparing
  against anything but the desktop's actual pin would judge a pair no build ships
  together.
- **Constraints and non-goals:** do NOT open `.github/workflows/ci.yml` (T1 owns
  it; the comparison ships as the NEW `.github/workflows/schema-drift.yml`). Do NOT
  modify the desktop checkout (a different repository; out of scope for this branch —
  reading it is fine, editing is not). Do NOT widen `boundary_pins` (the daemon's
  legitimate `rusqlite` is legal, and the engine's pin must not be weakened); the
  fingerprint code lives in the daemon member (`fulcrum/`), so no `.rs` in root
  `src/` changes. Do NOT touch either `UNIQUE` constraint or either `ON DELETE
  CASCADE` (load-bearing: idempotent upserts, cascade delete). No Cargo manifest
  edits of any kind. `schema.rs`'s existing three unit tests stay in substance.

**Owned-file boundary notes (flagged, not acted on):** `fulcrum/src/db/mod.rs`
contains the same stale `tests/db.rs` citations (lines 1075, 1104, 1113) and the
"Task 6's job to pin positively" module-doc sentence (§ line 8), and cites
`tests/db.rs` in `verify_shapes`'s docs (line ~366 region). It is OUTSIDE this
task's owned set (`fulcrum/src/db/schema.rs`, `fulcrum/tests/db.rs`, `schema-sha.txt`,
`scripts/check-schema-drift.sh`, `.github/workflows/schema-drift.yml`, `README.md`,
`AGENTS.md`), so this task does not touch it. The citation fix IS applied at the one
site inside the owned set — `schema.rs` module docs line ~135, "the cascade tests in
`tests/db.rs`" → "`fulcrum/tests/db.rs`". Caller may fold the `db/mod.rs` doc-only
fix into a later wave (T3/T4 do not own it either).

**Files:**
- Create: `schema-sha.txt` (repo root — the canonical shape pin, format below)
- Create: `scripts/check-schema-drift.sh` (guard; full spec below)
- Create: `.github/workflows/schema-drift.yml` (new workflow; ci.yml untouched)
- Modify: `fulcrum/src/db/schema.rs` (module header rewrite + fingerprint builder +
  unit tests; `SCHEMA`/`TABLE_SHAPES`/`migrate`/the three existing tests untouched in
  substance; stale `tests/db.rs` citation fixed)
- Modify: `fulcrum/tests/db.rs` (new integration tests pinning the file against the
  live shapes; nothing existing removed)
- Modify: `README.md` (new "Schema-shape contract with the desktop" section +
  validation-bar lines)
- Modify: `AGENTS.md` (validation-bar lines + a short contract bullet)
- Test: `fulcrum/tests/db.rs` (the file-live pin test) + `fulcrum/src/db/schema.rs`
  (`#[cfg(test)]` module) + the guard's own `--self-test` fixtures

**What to implement:**

#### 1. `schema-sha.txt` (repo root) — create with EXACTLY this content

The five sections are the serialisation of `TABLE_SHAPES` (name, then one
`name:type` line per column, declaration order) plus the `idx_messages_session`
section carrying the trimmed `CREATE INDEX` statement line. Everything below was
verified against the current DDL: the daemon const (schema.rs:84–124) and the
desktop const (`db.rs:1564–1604`) are byte-identical (39 lines each, SHA-256
`f0722539…`), and these sections were derived from `TABLE_SHAPES`/`SCHEMA` directly.
Line-for-line:

```
# Schema-shape pin for the fulcrum↔desktop contract (issue #6).
# Five sections. A section is a header line naming a table (or `idx_messages_session`
# for the index) followed by one `name:type` line per column, in declaration order.
# `name:type` is exactly what the daemon pins in `db/schema.rs`'s `TABLE_SHAPES`;
# NOTHING else is pinned here: not NULL/DEFAULT/PRIMARY KEY flags, not the UNIQUE
# constraints or cascades (pinned elsewhere by DB-asking probes: Db::table_has_unique,
# the cascade tests), not migration history, not the SELECT-list subsets
# (SESSION_COLUMNS / SPACE_COLUMNS are subsets and deliberately absent). Whitespace
# is not drift (lines are trimmed, blanks dropped); a renamed/retyped/reordered/
# added/removed column or a lost index IS. Keep this file's diff reviewable next to
# the DDL diff: change it in the same PR that changes both repos' DDL in lockstep,
# then re-run `bash scripts/check-schema-drift.sh --self-test` and `cargo test -p fulcrum`.
sessions
id:TEXT
cwd:TEXT
created_at:INTEGER
title:TEXT
capabilities_json:TEXT
archived:INTEGER
context_usage_json:TEXT
is_subagent:INTEGER
todos_json:TEXT
cost_usd:REAL
sort_order:INTEGER
title_stage:INTEGER
messages
id:INTEGER
session_id:TEXT
kind:TEXT
message_key:TEXT
payload_json:TEXT
created_at:INTEGER
native_messages
session_id:TEXT
seq:INTEGER
role:TEXT
content_json:TEXT
created_at:INTEGER
spaces
path:TEXT
created_at:INTEGER
last_opened_at:INTEGER
trusted:INTEGER
sort_order:INTEGER
idx_messages_session
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);
```

Comment lines start with `#` at column 0 and are ignored by both consumers; no blank
lines in the file.

#### 2. `fulcrum/src/db/schema.rs` — fingerprint builder + unit tests + header rewrite

Add (module docs first, then code):

```rust
/// Serialise [`TABLE_SHAPES`] — plus the index — into the schema-shape pin sections.
///
/// One section per table: the table's name on its own line, then a `name:type` line
/// per column in declaration order; then a final `idx_messages_session` section
/// carrying the trimmed `CREATE INDEX` statement. The text is produced directly from
/// [`TABLE_SHAPES`] (the single source of truth; [`ColumnShape`] pins exactly name +
/// declared type), parsed from [`SCHEMA`] only for the index line, which the shape
/// constants do not carry. This is the serialisation of the same fact
/// `the_pinned_shapes_match_the_ddl_they_claim_to_describe` proves against `SCHEMA`,
/// not a second representation.
pub fn schema_shape_sections() -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for shape in TABLE_SHAPES {
        out.push((
            shape.name.to_string(),
            shape
                .columns
                .iter()
                .map(|c| format!("{}:{}", c.name, c.decl_type))
                .collect(),
        ));
    }
    let index = SCHEMA
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("CREATE INDEX "))
        .expect("SCHEMA declares the idx_messages_session index");
    out.push(("idx_messages_session".to_string(), vec![index.to_string()]));
    out
}

/// The full pin text: `schema_shape_sections()`, one line per entry, each line
/// `\n`-terminated. Byte-identical to `schema-sha.txt` with comment lines and blank
/// lines stripped (the integration test `the_pinned_schema_fingerprint_matches_the_shipped_file`
/// in `fulcrum/tests/db.rs` asserts exactly that).
pub fn schema_shape_fingerprint() -> String {
    let mut out = String::new();
    for (name, lines) in schema_shape_sections() {
        out.push_str(&name);
        out.push('\n');
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}
```

Unit tests to ADD inside `schema.rs`'s existing `#[cfg(test)] mod tests` (do not
modify the three existing tests):

- `the_shape_sections_name_each_table_and_its_full_column_list` — asserts
  `schema_shape_sections()` has exactly the sections `sessions` (12 lines), `messages`
  (6), `native_messages` (5), `spaces` (5), `idx_messages_session` (1 line), and that
  each table section's lines equal `format!("{}:{}", c.name, c.decl_type)` over the
  matching `*_TABLE` const.
- `the_index_section_is_the_shipped_index_line` — asserts the `idx_messages_session`
  section is exactly `["CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);"]`.

**Module header rewrite (the issue's mandated prose):** in the module docs:

- In `# Where this DDL comes from, and why it is verbatim`, replace the parenthetical
  "(verified against that tree at `4f9b99dc`)" — and the claim that a one-time
  inspection keeps the copy honest — with the true mechanism: the copy is kept honest
  by the **shape-pin contract** (`schema-sha.txt`), checked in CI by
  `.github/workflows/schema-drift.yml` (`scripts/check-schema-drift.sh`) and locally
  by the integration test in `fulcrum/tests/db.rs`; a DDL change in the daemon reddens
  its own suite the moment it lands without updating the pin, and a DDL change in the
  desktop reddens the desktop's comparison against the pinned fingerprint. Keep the
  provenance sentence (the source file and range `archimedes-desktop/src-tauri/src/storage/db.rs:1565-1604`
  are still where the transcription came from — note the desktop repo has since renamed
  to `archimedes-codes/desktop`; the `archimedes-desktop` path is the pre-rename name
  and may be left as-is or updated to the current checkout path `src-tauri/src/storage/db.rs`).
- In `# The two repos' migration mechanisms are unrelated, and what it costs`,
  REPLACE the closing "honest limit" paragraph ("Real cross-repo schema versioning … a follow-up
  ticket; no amount of a reader checking a file against its own expectations
  substitutes for the two owners agreeing on a number") with the real mechanism:
  the two owners now agree on a **number-free but explicit contract** — the
  `schema-shape` pin described above: what it pins (per-table name+declared type in
  order plus the index), what it does not (constraints, defaults, subsets,
  migration history — see the `ColumnShape` doc), that the daemon side is enforced
  in-repo at `cargo test` and in the schema-drift workflow, and that the desktop
  side runs the same comparison in its own CI against the fingerprint of the rev it
  pins, refusing loudly (never a vacuous pass) when that rev publishes no
  fingerprint — which is the current state of the pin rev `78e595436ac5612d8ef87f692bee61f48f6e58e7`.
- Fix the stale citation at line ~135: "the cascade tests in `tests/db.rs`" →
  "the cascade tests in `fulcrum/tests/db.rs`".
- Do NOT touch the "What is NOT here" section, the load-bearing `UNIQUE`/`CASCADE`
  bullets, `SCHEMA`, `SCHEMA_VERSION`, `migrate`, `TABLE_SHAPES` or the three
  existing tests.
- The module docs do NOT mention `boundary_pins`; leave any `boundary_pins` prose
  alone (the hard prohibition is in `db/mod.rs`/`AGENTS.md`, which this task does not
  edit).

#### 3. `fulcrum/tests/db.rs` — the file-live pin (the "immediate red" guarantee)

ADD two integration tests (do not touch existing ones; the file's existing imports
`use fulcrum::db::schema::{SCHEMA, SCHEMA_VERSION};` gain `schema_shape_fingerprint` —
update that import line):

- `the_pinned_schema_fingerprint_matches_the_shipped_file` —
  ```rust
  let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("schema-sha.txt");
  let file = std::fs::read_to_string(&path)
      .unwrap_or_else(|e| panic!("read the repo-root schema-sha.txt ({path:?}): {e}"));
  let canonical: String = file
      .lines()
      .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
      .map(|line| format!("{}\n", line.trim_end()))
      .collect();
  assert_eq!(
      canonical, schema_shape_fingerprint(),
      "`schema-sha.txt` no longer matches the shapes `TABLE_SHAPES` pins — update \
       the pin in the SAME commit as the DDL change, or this repo has drifted from \
       its own contract"
  );
  ```
  (This is the check that goes red the instant a daemon DDL change lands without the
  matching pin update — "in one repo immediately".)
- `the_schema_fingerprint_covers_all_four_tables_and_the_index` — parses the same
  canonical text and asserts exactly the five section headers
  `["sessions", "messages", "native_messages", "spaces", "idx_messages_session"]`
  appear as standalone lines and no other standalone header line exists (the vacuity
  floor: a pin that does not name all five keys is refused, not compared).

Note on the path: `CARGO_MANIFEST_DIR` for the daemon member is
`<repo-root>/fulcrum`, so `../schema-sha.txt` is the repo root's file. Do not use
the process CWD.

#### 4. `scripts/check-schema-drift.sh` — the guard (full spec; write it as specified)

Follow `scripts/check-rquickjs-allocator.sh`'s conventions verbatim: bash shebang,
`set -euo pipefail` as the first statement, `usage()` printing a usage line,
`--self-test` as a boolean flag, `--input FILE` and `--schema FILE` as
option-with-value (both `--flag VALUE` and `--flag=VALUE` shapes; an empty or missing
value is an error, never "read stdin"), a `repo-root` positional defaulting to `.`
validated as a directory, `expect_exit` using the repo's `output="$("$@" 2>&1)" ||
got=$?` idiom (never `if ! …`), `BASH_SOURCE[0]` canonicalisation in the self-test,
exit contract 0 = matches / 1 = drift (with the offending section NAMED) / 2 = the
check could not be run honestly. `--self-test` exit contract: 0 all cases behaved,
1 any case failed.

Behaviour:

- **Live mode (no `--input`):** extract the `SCHEMA` const interior from
  `<repo-root>/fulcrum/src/db/schema.rs`:
  ```bash
  interior="$(sed -n '/^pub const SCHEMA: &str = r#"/,/^"#;$/p' "$db_src" | sed '1d;$d')"
  ```
  If the file is missing or the interior is empty → `error:` + reason + `exit 2`
  (a comparison that cannot read its side of the contract is refused, never passed).
- **`--input FILE`:** `FILE` is a raw DDL interior (the lines between the `r#"` and
  `"#`) — the captured-dump equivalent, used by `--self-test` fixtures and by the
  desktop side (which extracts its own `SCHEMA` the same way and hands it over).
  Missing/unreadable `FILE` → `error:` naming the file + `exit 2`.
- **`--schema FILE`:** the pin to compare against; default
  `<repo-root>/schema-sha.txt`. Missing/unreadable → `error:` naming the expected
  path AND naming why (see the loud-missing-other-side text) + `exit 2`.
- **Normalisation (`extract_sections`, reads the interior on stdin):** the awk below
  (this exact program — the per-table digests it yields equal the pinned file's
  sections on the current DDL; verified):
  ```awk
  # Header: CREATE TABLE IF NOT EXISTS <name> (
  /^CREATE TABLE IF NOT EXISTS / { line=$0; sub(/^CREATE TABLE IF NOT EXISTS /,"",line); sub(/ \(.*/,"",line); print line; next }
  # Header: the index statement (trimmed of leading/trailing whitespace)
  /^CREATE INDEX / { line=$0; sub(/^[[:space:]]*/,"",line); sub(/[[:space:]]+$/,"",line); print "idx_messages_session"; print line; next }
  # Non-column lines
  /^\);/ { next }
  /^[[:space:]]*UNIQUE/ { next }
  /^[[:space:]]*$/ { next }
  # Column lines → name:type (name = first whitespace token, type = second, with any
  # trailing comma stripped and inline `PRIMARY KEY` discarded; matches ColumnShape's
  # decl_type). This exact program, run on the current interior, reproduces the
  # `schema-sha.txt` sections byte-for-byte — verified.
  { line=$0; sub(/^[[:space:]]*/,"",line); sub(/[[:space:]]+$/,"",line); split(line,f,"[[:space:]]+"); sub(/,+$/,"",f[2]); print f[1] ":" f[2]; next }
  ```
  The exact awk can be adjusted for portability (`gawk` not required — plan on the
  POSIX capabilities of the runner's `awk`), but the emission must be: for each
  `CREATE TABLE IF NOT EXISTS <name> (` block, a section `<name>` (header line) then
  one `name:type` line per column line, dropping `);` and `UNIQUE(...)` lines and
  blank lines; then a final section `idx_messages_session` with the trimmed index
  statement line. Everything trimmed of leading/trailing whitespace. `grep -v '^#'`
  on the pin file and `grep -v '^[[:space:]]*$'` give the pinned sectioned text.
- **Comparison (`compare`, per-key so it can NAME the table):**
  - Vacuity floor on the PIN: the pin must contain all five header lines
    `sessions messages native_messages spaces idx_messages_session`, each exactly
    once, and no other header line; otherwise `error:` naming the missing/extra key
    + `exit 2` ("a fingerprint that cannot name every table is not a comparison").
  - For each of the five keys, extract the section body from the pin and from the
    normalised DDL (a small non-associative helper `section_of <key>` reading
    sectioned text on stdin and stopping at the next header line — bash 3-safe, no
    associative arrays, so the script stays portable to the desktop's CI).
  - A key present in the pin but missing from the DDL → DRIFT, `exit 1`, message
    `error: schema drift in '<key>': <source> declares no '<key>' section — a table
    or the index went missing`.
  - A header present in the DDL but not pinned (a fifth table) → DRIFT, `exit 1`,
    naming `'<extra table>'`.
  - A section whose bodies differ → DRIFT, `exit 1`, message of the form
    `error: schema drift in '<table>': the pinned fingerprint and <source> disagree`
    plus a 4-line context showing the first differing line of each side (use
    `diff -u` or a first-difference walk; machinery is free, the NAMED TABLE in the
    first error line is not).
  - All five agree → print a human pass sentence to stdout
    (`schema shape matches the pinned fingerprint: sessions, messages, native_messages, spaces and idx_messages_session all agree between <pin> and <source>`)
    and `exit 0`.
  - A pinned-file parse that cannot be canonicalised (binary garbage, no headers)
    → `exit 2`, named reason.
- **The loud-missing-other-side text:** when `--schema`'s file is missing, the
  `error:` message MUST include both the expected path and the refusal reason, e.g.
  `error: no schema-shape pin at <path> — nothing to compare, and a comparison that
  finds nothing is a lie; the pinned rev of fulcrum publishes no fingerprint (rev
  78e595436ac5612d8ef87f692bee61f48f6e58e7 ships no schema-sha.txt), so this check
  refuses loudly until the desktop half lands`. (Exact wording free; the named
  reason + exit 2 are not.)
- **`--self-test`** plants a throwaway root and re-invokes THIS script (never a
  second implementation) with `--input`/`--schema` fixtures:
  1. clean — the real `schema-sha.txt` copy + a captured fixture of the real interior
     (build it by running the exact `sed` extractor from `fulcrum/src/db/schema.rs`
     when writing the script) → expect `0`;
  2. retyped — fixture with `cost_usd REAL,` mutated to `cost_usd TEXT,` → expect
     `1`, output contains `sessions`;
  3. added column — fixture with a seventh `messages` column added (e.g.
     `    extra TEXT,` after `kind TEXT NOT NULL,`) → expect `1`, output contains
     `messages`;
  4. missing table — fixture without the `sessions` section → expect `1`, output
     contains `sessions`;
  5. dropped index — fixture without the `CREATE INDEX` line → expect `1`, output
     contains `idx_messages_session`;
  6. extra table — fixture with `CREATE TABLE IF NOT EXISTS extra (…);` appended →
     expect `1`, output contains `extra`;
  7. pin missing a section — schema fixture without `native_messages` → expect `2`;
  8. missing other side — `--input` pointing at a nonexistent file → expect `2`,
     output names the file;
  9. whitespace churn — a fixture re-indented (different leading spaces / extra blank
     lines, same names and types) → expect `0` (NO false positive — the analogue of
     the allocator guard's `allocator-api2` trap);
  10. a fingerprint that pins a sixth header → expect `2`.
  Use `expect_exit "…" N bash "$script" --input … --schema …`, seeding each fixture
  with a heredoc like the allocator guard does. Report `self-test: N case(s) FAILED` /
  `self-test: all cases behaved (the guard has teeth)` and exit accordingly.
- The script must NOT run cargo, must NOT write any file in live mode, and must be
  plain bash portable to the desktop's CI (no bash 4 features).

#### 5. `.github/workflows/schema-drift.yml` — new workflow (ci.yml untouched)

```yaml
name: Schema drift

on:
  push:
    # The repo's merge bar — see ci.yml: feature/*, not a named branch.
    branches: [main, feature/*]
  pull_request:

jobs:
  schema-drift:
    name: schema-shape pin vs the shipped DDL
    # Single OS on purpose: the check is text-only (grep/awk/diff). ci.yml's 3-OS
    # matrix exists for the cfg(unix)/cfg(windows) process hygiene, which nothing
    # here touches; this guard's portability is proven by its own --self-test.
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      # No version named on purpose — the repo-root rust-toolchain.toml is the pin
      # (same rationale as ci.yml's Install-the-toolchain step).
      - name: Install the pinned Rust toolchain
        shell: bash
        run: |
          rustup toolchain install
          rustup default "$(rustup show active-toolchain | cut -d' ' -f1)"
          rustc --version

      - name: Prove the schema-drift guard can fail
        shell: bash
        run: bash scripts/check-schema-drift.sh --self-test

      - name: Check the schema-shape pin against the shipped DDL
        shell: bash
        run: bash scripts/check-schema-drift.sh
```

Ordering is load-bearing exactly like ci.yml's guard pair: the self-test runs BEFORE
the live check so a broken guard reddens rather than silently passing. No
`cargo build --all-targets` step anywhere (the repo rule), and this workflow runs no
cargo at all — the Rust-level pin lives in the ordinary `cargo test` suite.

#### 6. `README.md`

- Under `## Tests`, add a bullet for the new guard pair (mirroring how the rquickjs
  guard is described): `bash scripts/check-schema-drift.sh --self-test` + the live
  check.
- Add a short section `## Schema-shape contract with the desktop` (~8-12 lines)
  covering: `schema-sha.txt` at the repo root is the canonical shape pin (four tables
  + index, per-column `name:type` in order); what it does and does not compare (see
  the file header); who enforces it (this repo: the `cargo test` integration test +
  `scripts/check-schema-drift.sh` in `.github/workflows/schema-drift.yml`; the
  desktop: the same comparator in its own CI against the fingerprint of the rev it
  pins); the update path (change both repos' DDL in lockstep, update
  `schema-sha.txt` in the SAME commit, keep the two diffs reviewable together);
  and the current honest-degraded state: the desktop pins `78e59543` which ships no
  fingerprint, so the desktop-side comparison will refuse loudly until the desktop
  half lands — silence here is a bug, never a green.

#### 7. `AGENTS.md`

- Add to the validation-bar code block:
  `bash scripts/check-schema-drift.sh --self-test` and
  `bash scripts/check-schema-drift.sh` (after the rquickjs lines).
- Add one short bullet under `## Things that will bite you` (or the guards section,
  whichever sits with the guard prose): the schema-shape pin is the fulcrum↔desktop
  contract; `schema-sha.txt` is a reviewable artifact updated in the same commit as a
  lockstep DDL change; do NOT regenerate it from a fork (the guard + tests refuse
  nothing-matching or missing-side comparisons); `boundary_pins` is not widened
  (the daemon's `rusqlite` is legal and the engine pin stays put).

**Steps:**

- [ ] 1. Create `schema-sha.txt` at the repo root with the EXACT content in §1, and
      add it.
- [ ] 2. Add the fingerprint builder (`schema_shape_sections` /
      `schema_shape_fingerprint`) and the two unit tests in §2 to
      `fulcrum/src/db/schema.rs`; update the `use` line in
      `fulcrum/tests/db.rs` to import `schema_shape_fingerprint`.
- [ ] 3. Add the integration tests in §3 to `fulcrum/tests/db.rs`.
      Run `cargo test -p fulcrum --lib` (unit tests) and
      `cargo test -p fulcrum --test db` (integration tests).
      - The unit tests should pass against the just-created `schema-sha.txt` the
        first time (the file and the shapes agree); if a test fails, check that the
        file was copied exactly (count the sections: 12+6+5+5 column lines and the
        index line). Run `cargo fmt --all -- --check` before continuing.
- [ ] 4. Write `scripts/check-schema-drift.sh` per §4 with the `--self-test`
      fixtures (build the clean interior fixture with the exact sed extractor).
      `chmod +x scripts/check-schema-drift.sh`.
      Run `bash scripts/check-schema-drift.sh --self-test`.
      - It must print `all cases behaved (the guard has teeth)` and exit 0. Then run
        `bash scripts/check-schema-drift.sh` (live mode) — it must exit 0 and print
        the "shape matches the pinned fingerprint" sentence. **First run GREEN.**
      - Prove the teeth manually once: `sed`-mutate a copy of the interior
        (`cost_usd REAL,` → `cost_usd TEXT,`), run
        `bash scripts/check-schema-drift.sh --input <that file>` → exit 1, output
        names `sessions`.
- [ ] 5. Create `.github/workflows/schema-drift.yml` per §5. (Do NOT open or edit
      `.github/workflows/ci.yml`.)
- [ ] 6. Rewrite the `schema.rs` module header per §2 (mechanism prose + the stale
      `tests/db.rs` citation fix). Do not change `SCHEMA`, `TABLE_SHAPES`, `migrate`
      or the three existing tests.
- [ ] 7. Update `README.md` and `AGENTS.md` per §6 and §7.
- [ ] 8. Full local bar on the task: `cargo test -p fulcrum --lib`,
      `cargo test -p fulcrum --test db`, `cargo fmt --all -- --check`,
      `cargo clippy -p fulcrum --all-targets -- -D warnings`,
      `bash scripts/check-schema-drift.sh --self-test`,
      `bash scripts/check-schema-drift.sh` — all green. (The full two-member bar and
      the Windows cross-check run at fan-in per the status file; nothing here touches
      the engine member, so its tests are unaffected.)
- [ ] 9. Commit with message:
      `feat(daemon): pin the cross-repo schema shape and gate it in CI (#6)`
      — one commit, all owned files together (the `schema-sha.txt` diff is part of
      this commit by construction; it matches the current DDL, so CI is green on the
      base state).

**Acceptance criteria:**
- [ ] A change to the daemon's `sessions`/`native_messages` (or any other) DDL that
      breaks shape compatibility makes SOME check fail in THIS repo immediately —
      `cargo test -p fulcrum` reddens on the stale `schema-sha.txt` pin — and the
      schema-drift workflow's live check reddens identically via the shell guard.
      A change to the DESKTOP's DDL reddens the desktop's comparison against the
      pinned fingerprint once its half lands (documented contract); the guard's
      `--self-test` proves the refusal path for a missing other side.
- [ ] The checks NAME the diverged table or index (`error: schema drift in '<table>'
      …`), not "hashes differ": per-section comparison, plus vacuity floors that
      refuse a pin or extraction that cannot name all five keys (a pass-because-nothing-
      to-compare is impossible: missing pin file, missing section, or unextractable
      DDL all exit 2 with a named reason).
- [ ] `schema.rs`'s module header describes the real mechanism (the shape-pin
      contract + in-repo tests + CI guard + the desktop's loud degraded state at rev
      `78e59543`), replacing the "verified against `4f9b99dc`" inspection claim and
      the "follow-up ticket" sentence; the stale `tests/db.rs` citation inside
      `schema.rs` is corrected to `fulcrum/tests/db.rs`.
- [ ] `--self-test` proves the guard can fail: a retyped `sessions.cost_usd`, an
      added `messages` column, a missing table/index, an extra table, a missing
      section, a missing other side, and a sixth pinned header all produce the
      expected non-zero exits; whitespace churn stays 0 (no false positive).
- [ ] First run is GREEN: `schema-sha.txt`, the guard (live + self-test) and the
      workflow all pass against the base commit, matching the byte-identical state
      of both DDLs today — a tripwire installed, not a fire fought.
- [ ] The fingerprint reuses `TABLE_SHAPES`/`*_TABLE` (single source of truth), no
      crypto/hash code and no Cargo manifest change, `boundary_pins` not widened and
      the engine member untouched, the desktop repository untouched, and the two
      `UNIQUE` constraints + both `ON DELETE CASCADE`s intact (untouched in
      `SCHEMA`, still pinned by `Db::table_has_unique` and the cascade tests).
- [ ] `README.md` and `AGENTS.md` document the contract, the update path (DDL +
      pin in the same commit), and the honest degraded state of the desktop half.

**UNVERIFIED / noted for the caller's vet:**
- `fulcrum/src/db/mod.rs` (lines 8, 1075, 1104, 1113 and the `verify_shapes` docs)
  carries the same stale `tests/db.rs` citations and a "Task 6's job" sentence;
  it is OUTSIDE this task's owned set so it is deliberately not modified — the
  caller may fold the doc-only fix into a later wave.
- The desktop-side implementation (running the comparator in the desktop repo,
  shipping the pinned rev's fingerprint) is out of scope by directive; this plan
  ships the format, the comparator, the loud refusal, and the documentation of the
  degraded state, and it was written against the read-only desktop checkout at
  `4f9b99dc` (pin `78e59543` verified at `archimedes/src-tauri/Cargo.toml:47`).
- All `cargo` commands in the Steps are the standard narrow forms for the daemon
  member; the compiled gate figures (1570 passed / 0 failed / 1 ignored at
  `958306d`) are cited from `docs/roadmap/daemon-hardening-status.md`, not re-measured
  here (this workspace was kept read-only; the implementer's first run is the
  measurement).

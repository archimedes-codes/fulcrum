# GROUNDING REPORT — `daemon-store` (T2, issue #6: cross-repo schema drift detection)

Area: `daemon-store` of feature `daemon-hardening` · Produced by: Researcher subagent
Consumed by: Plan agent when designing T2. This report is **facts and options only** — no implementation is proposed.

## 0. Reading order map

| File | Role |
|---|---|
| `fulcrum/fulcrum/src/db/schema.rs` | The daemon's DDL (`SCHEMA`), the `migrate` slot, `SCHEMA_VERSION`, `TABLE_SHAPES`, the self-referential pin |
| `fulcrum/fulcrum/src/db/mod.rs` | `Db::open`, `verify_shapes`, `Db::SESSION_COLUMNS` / `MESSAGE_COLUMNS`, the by-index mappers, `make_private` / `ensure_private_dir` |
| `fulcrum/fulcrum/src/db/persistence.rs` | The `impl FulcrumPersistence for Db` seam body (T2‑adjacent, not touched by T2) |
| `fulcrum/fulcrum/tests/db.rs` | The daemon's DATABASE tests (the pin the issue's cite counts as "documented") |
| `archimedes/src-tauri/src/storage/db.rs` | The desktop's DDL (`SCHEMA`), its `Db::open` migration chain, its `SESSION_COLUMNS` / `MESSAGE_COLUMNS` / `SPACE_COLUMNS` |
| `archimedes/src-tauri/src/storage/db/tests/mod.rs` | The desktop's own drift-guard tests (the desktop's half of the today-pin) |
| `archimedes/src-tauri/Cargo.toml` | The desktop→fulcrum pins |
| `fulcrum/.github/workflows/ci.yml`, `fulcrum/AGENTS.md`, `fulcrum/Cargo.toml`, `fulcrum/fulcrum/Cargo.toml`, `fulcrum/src/boundary_pins.rs` | Conventions a CROSS-REPO CI check must conform to |

---

## 1. State fact: **the two DDLs agree today — exactly, down to whitespace**

The headline a planner asked for first, so it is first. I extracted the desktop's `SCHEMA` const and the daemon's `SCHEMA` const as text and compared them byte-for-byte:

```
$ sed -n '/const SCHEMA: &str = r#"/,/^"#;$/p' archimedes/src-tauri/src/storage/db.rs | sed '1d;$d' | sha256sum
f072253979a13d4be1b934a64ad33cc06dff332de8ac394df55be5bc654995f1
$ sed -n '/pub const SCHEMA: &str = r#"/,/^"#;$/p' fulcrum/fulcrum/src/db/schema.rs | sed '1d;$d' | sha256sum
f072253979a13d4be1b934a64ad33cc06dff332de8ac394df55be5bc654995f1
```

Both extracts are 39 lines; the SHA-256s are identical. A direct `diff -u` of the two extracts produced **zero hunks**. This is not a vacuous match: I separately verified the extraction ranges exist and are non-empty (39 lines each, containing all four `CREATE TABLE IF NOT EXISTS` statements and the index), and the daemon's `schema.rs` module header claims the transcription is "verified against that tree at `4f9b99dc`". The desktop checkout at `/home/daniel/Coding/Archimedes/archimedes` verifies as:

- `origin git@github.com:archimedes-codes/desktop.git` (SSH) — the module-header claim
  "`archimedes-desktop/src-tauri/src/storage/db.rs`" uses the project's pre-rename name; the repo now resolves as `archimedes-codes/desktop` (AGENTS.md also records "renamed from `archimedes-codes/desktop`").
- `HEAD = 4f9b99dcca13372c7a26e85bec095cd4a14b9405, 2026-11-05 22:10:26 +0100, "T8b/S8b.4-part-3c: restore the desktop-side child-process guard (invariant A only) (#162)"` — **exactly** the commit the daemon's module header names as the verification target.
- The desktop working tree is NOT clean: `git status --porcelain` reports ` M scripts/compose-updater-manifest.test.sh`, ` M src-tauri/tauri.conf.json`, ` M src/lib/paths.test.ts`. None of these three touch `src-tauri/src/storage/db.rs`, so the DDL comparison is unaffected by local desktop edits.

**Conclusion: as of the pinned desktop commit, the four tables' DDL is byte-identical and the module header's claim is true.** No drift. Because the transcription is *verbatim*, the two DDLs also agree on every column name, declared type, the two `UNIQUE` constraints, both `ON DELETE CASCADE` clauses, and the `idx_messages_session` index. A schoolbook `diff` of the two `SCHEMA` texts would pass today — **the problem the issue describes is that nothing keeps it that way; today is the state a fingerprint must freeze.**

Both schema files were double-checked against the actual text (I own the build lock, so nothing here was run; comparison was done with `sed`/`diff`/`sha256sum` on the checked-out sources).

## 2. The daemon side (`fulcrum/fulcrum/src/db/schema.rs`)

### 2.1 `SCHEMA` (the whole const, 39 lines)

```sql
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    cwd TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    title TEXT,
    capabilities_json TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0,
    context_usage_json TEXT,
    is_subagent INTEGER NOT NULL DEFAULT 0,
    todos_json TEXT,
    cost_usd REAL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    title_stage INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    message_key TEXT,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id, kind, message_key)
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);
CREATE TABLE IF NOT EXISTS native_messages (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    content_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id, seq)
);
CREATE TABLE IF NOT EXISTS spaces (
    path TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    last_opened_at INTEGER NOT NULL,
    trusted INTEGER NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0
);
```

`IF NOT EXISTS` throughout — that is the property `migrate` relies on (safe to `execute_batch` against a file that already has the tables).

### 2.2 Version + migration slot

- `pub const SCHEMA_VERSION: i64 = 1;` — stamped into `PRAGMA user_version`; re-exported as `pub use schema::SCHEMA_VERSION` from `db/mod.rs`.
- `pub fn migrate(conn: &mut Connection, from: i64) -> Result<i64, DbError>` — ONE slot: `from > SCHEMA_VERSION` → `Err(DbError::SchemaTooNew { found, supported })`; `from < 1` → one transaction running `execute_batch(SCHEMA)` + `pragma_update(user_version = SCHEMA_VERSION)`. Returns `SCHEMA_VERSION`. Later slots are documented as "one more `if` branch here".
- `Db::open` (`db/mod.rs`) reads `PRAGMA user_version` then `schema::migrate(&mut conn, version)` then `Self::verify_shapes()` — the shape check runs **after** migrate, and the order is documented as load-bearing (before the migration the tables don't exist yet on a fresh file).

### 2.3 The shape-pin machinery (a canonicalisation helper ALREADY present)

T2's issue asks for "a canonical schema fingerprint exported from each repo". **The daemon side of that fingerprint machinery already exists** — `schema.rs` already contains the canonical per-table column lists and the test that proves they agree with the DDL text:

- `pub struct ColumnShape { pub name: &'static str, pub decl_type: &'static str }` — only these two fields are pinned, deliberately: `notnull` / `DEFAULT` / `PRIMARY KEY` are NOT pinned (the "shape" is the positional-read contract plus the quiet-coercion hazard), and the declared type IS pinned because SQLite's dynamic typing coerces a retyped column silently.
- `pub struct TableShape { pub name: &'static str, pub columns: &'static [ColumnShape] }`.
- `pub const SESSION_TABLE: [ColumnShape; 12]` (all 12 `sessions` columns), `pub const MESSAGES_TABLE: [ColumnShape; 6]`, `pub const NATIVE_MESSAGES_TABLE: [ColumnShape; 5]`, `pub const SPACES_TABLE: [ColumnShape; 5]`, `pub const TABLE_SHAPES: [TableShape; 4]`.
- `Db::verify_shapes(&self) -> Result<(), DbError>` (private in `db/mod.rs`) — walks `TABLE_SHAPES`, reads `PRAGMA table_info('<table>')` on each, refuses on missing table / missing column / renamed / reordered / retyped / unexpected-extra column with a per-case sentence inside `DbError::SchemaFork { table, detail }`.
- The three schema.rs tests (the **verbatim signatures** the issue will want in "Provides"):

```rust
#[test]
fn schema_text_holds_the_desktops_shape()                           // schema.rs:384
#[test]
fn the_shape_list_covers_every_table_in_the_schema()
#[test]
fn the_pinned_shapes_match_the_ddl_they_claim_to_describe()
```

**Why the existing pin is self-referential:** `schema_text_holds_the_desktops_shape` asserts facts about `SCHEMA` — the daemon's OWN DDL string. `SCHEMA.matches("CREATE TABLE").count() == 4`, two `ON DELETE CASCADE`, `UNIQUE(session_id, kind, message_key)`, `UNIQUE(session_id, seq)`, the index, `cost_usd REAL,` (and `!` … `"cost_usd REAL NOT NULL"`, the D5/ADR-0082 no-default-on-cost rule). Every one of those is "the daemon's text agrees with the daemon's text"; the desktop's identity is asserted only in a comment. The transcript/drift tests that DO ask a real database live in `fulcrum/fulcrum/tests/db.rs` (see §3.3).

### 2.4 The module header's honest limit (verbatim)

The `schema.rs` module docs already state, in the section "**The honest limit, stated where someone will over-read it:**" (paraphrased, key sentence verbatim):

> that check runs against the daemon's OWN file … It does NOT and cannot detect the desktop forking `archimedes.db`, which this process never opens. Real cross-repo schema versioning (a version the desktop and the daemon negotiate) is a follow-up ticket; no amount of a reader checking a file against its own expectations substitutes for the two owners agreeing on a number.

**This "follow-up ticket" sentence is precisely T2.** The issue's "the module header must be updated to describe the real mechanism" requirement has an exact target: this paragraph (`schema.rs`, module docs, "What is NOT here" / "The two repos' migration mechanisms are unrelated" region). The daemon file `db.rs` and `persistence.rs` also mention "a follow-up ticket" / "Task 6's job", but only `schema.rs`'s header describes the cross-repo mechanism.

## 3. The daemon side — `fulcrum/fulcrum/src/db/mod.rs`

### 3.1 Signatures T2 must not break (verbatim)

```rust
pub struct Db { conn: StdMutex<Connection> }

pub fn open(path: &Path) -> Result<Self, DbError>
   // ensure_private_dir(parent)?; make_private(path)?;
   // Connection::open → pragma_update(journal_mode, "WAL") + read-back check (Err NotWal)
   //            → pragma_update(synchronous, "NORMAL") → pragma_update(foreign_keys, true)
   //            → PRAGMA user_version → schema::migrate(&mut conn, version)?
   //            → Self { conn: StdMutex::new(conn) } .verify_shapes()?

pub enum DbError { Sqlite(#[from] rusqlite::Error), Io(#[from] std::io::Error),
                   Json(#[from] serde_json::Error), NotWal(String),
                   SchemaTooNew { found: i64, supported: i64 },
                   SchemaFork { table: String, detail: String } }
```

The THREE pragmas are an ordered rule and unit-pinned (`a_fresh_store_has_every_pragma_it_claims`, `db/mod.rs` tests): WAL → NORMAL → foreign_keys, with `journal_mode` FIRST and **verified with a read-back** (the only one that can silently fail).

### 3.2 The by-index mappers (verbatim signatures)

```rust
const SESSION_COLUMNS: [&str; 10] = ["id", "cwd", "created_at", "title", "capabilities_json",
                                     "archived", "context_usage_json", "is_subagent", "cost_usd", "title_stage"];
const MESSAGE_COLUMNS: [&str; 6] =  ["id", "session_id", "kind", "message_key", "payload_json", "created_at"];
// Compile-time arity ties in db/mod.rs's test module:
const _: [(); 10] = [(); Db::SESSION_COLUMNS.len()];
const _: [(); 6]  = [(); Db::MESSAGE_COLUMNS.len()];

fn session_row_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<fulcrum_core::store_rows::SessionRow>
    // id(0), cwd(1), created_at(2), title(3), capabilities_json(4), archived get::<_,i64>(5)!=0,
    // context_usage_json(6), is_subagent get::<_,i64>(7)!=0, cost_usd(8), title_stage(9)
fn message_row_from(row: &rusqlite::Row<'_>) -> rusqlite::Result<fulcrum_core::store_rows::MessageRow>
    // id(0), session_id(1), kind(2), message_key(3), payload_json(4), created_at(5)
```

`sessions.todos_json` and `sessions.sort_order` are deliberately ABSENT from `SESSION_COLUMNS` (`SessionRow` has no fields for them) — the SELECT list is a subset of the table, which is exactly why `TABLE_SHAPES`/`verify_shapes` (full shape) exists separately. `native_messages` and `spaces` are read by NAME, not positionally, but are still shape-verified.

### 3.3 Where the daemon's real database tests live — and a DOC LIE in `schema.rs` and `db/mod.rs`

The textually-cited guard, `tests/db.rs`, is **`fulcrum/fulcrum/tests/db.rs`**, and it exists. It contains the acceptance tests: `the_schema_is_exactly_the_desktops_four_tables`, `the_unique_constraints_the_persister_relies_on_exist`, the eight `SchemaFork` mutation tests (`a_missing_column_refuses_the_open_and_names_the_table_and_column`, `an_unexpected_extra_column_refuses_the_open_and_names_it`, `a_renamed_column_refuses_the_open_and_names_both_spellings`, `a_reordered_column_refuses_the_open_and_reports_both_positions`, `a_retyped_column_refuses_the_open_and_names_both_types`, `every_one_of_the_four_tables_is_verified_at_open`, `a_missing_table_refuses_the_open_rather_than_reading_it_as_empty`, `the_check_runs_after_migrate_so_a_fresh_file_still_opens`), and the DB-property pins for WAL / cascades / UNIQUEs / mode bits.

**However:** both `schema.rs` (module docs: "the cascade tests in `tests/db.rs`") and `db/mod.rs` (`verify_shapes` docs: "`deleting_a_session_cascades_to_its_messages`", "the schema test in `tests/db.rs`") are written as if `tests/db.rs` sits beside the workspace-root `tests/` — but the daemon member's tests live under `fulcrum/tests/`, NOT the root `tests/` (root `tests/` holds only `fixture_bins.rs`). This is a doc-location staleness a never-touched-this-area reader will trip on; `fulcrum/fulcrum/tests/db.rs` is the only file matching `find . -name db.rs` under `fulcrum/tests/` or root `tests/`. **If T2 touches any of these doc comments, the path `tests/db.rs` should be corrected to `fulcrum/tests/db.rs`** (or the citation made member-qualified). Not a wiring bug — `fulcrum/fulcrum/tests/` IS the integration-test dir for the `fulcrum` package — purely a doc-fingerprint issue, but it is the same "prose about the mechanism" class the issue's module-header requirement is about.

### 3.4 `make_private` / `ensure_private_dir`

`make_private(path) -> Result<(), DbError>` — unix leg: creates `path` with `mode(0o600)` if absent (never truncates), then `set_permissions(0o600)` unconditionally (narrows pre-existing files; Windows leg is a no-op). `ensure_private_dir(path) -> std::io::Result<()>` — `pub(crate)`, 0700 on only the components THIS call created, documented as THE ONE COPY shared with `transport::unix`. Neither is touched by T2; their unit tests (`the_database_file_is_owner_readable_only`, `the_database_directory_is_owner_only`, `opening_an_existing_world_readable_database_narrows_its_mode`) must stay green.

## 4. The desktop side — `archimedes/src-tauri/src/storage/db.rs` (the counterpart)

### 4.1 Its DDL is `SCHEMA` (private const, lines 1565–1604; cited 1565–1604 in the daemon header — the range matches exactly)

Byte-identical to the daemon's (see §1). It declares **in the same file, right after the const**: `CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);` between `messages` and `native_messages` — same placement in both files.

### 4.2 How the desktop declares its own column lists (and how they differ from the daemon's)

The desktop's lists live in the same module:

```rust
// archimedes/src-tauri/src/storage/db.rs
const SESSION_COLUMNS: [&str; 10] = [ ...same 10 as the daemon... ];
const SESSION_ROW_FIELDS: usize = 10;
const _: [(); SESSION_COLUMNS.len()] = [(); SESSION_ROW_FIELDS];   // arity tie
const MESSAGE_COLUMNS: [&str; 6] = [ ...same 6 as the daemon... ];
const MESSAGE_ROW_FIELDS: usize = 6;
const _: [(); MESSAGE_COLUMNS.len()] = [(); MESSAGE_ROW_FIELDS];
const SPACE_COLUMNS: [&str; 4] = ["path", "created_at", "last_opened_at", "trusted"];  // sort_order deliberately ABSENT, consumed only in ORDER BY
const SPACE_ROW_FIELDS: usize = 4;
const _: [(); SPACE_COLUMNS.len()] = [(); SPACE_ROW_FIELDS];
const LAST_ACTIVITY: &str = "COALESCE((SELECT MAX(m.created_at) FROM messages m WHERE m.session_id = sessions.id), sessions.created_at)";
```

Key facts for designing the desktop side of a fingerprint:

- **The desktop's lists are the same 10/6 but NOT the same shape as the daemon's `TableShape` pins.** The desktop pins COLUMN-NAME arrays + a `*_ROW_FIELDS` usize (arity tie, "cannot see the SELECT statement at all"), and a `SPACE_COLUMNS` of 4 that excludes `sort_order`. **There is no per-column TYPE pin on the desktop side.** The desktop's full-table shape is not pinned anywhere (no `PRAGMA table_info` drift check — only the mapper drift-guard tests).
- **The desktop's real drift guard is a runtime test**: `archimedes/src-tauri/src/storage/db/tests/mod.rs` — `session_row_from_maps_the_select_const_columns_in_order` (line 3484), `message_row_from_maps_the_select_const_columns_in_order` (3574), `space_row_from_maps_the_select_const_columns_in_order` — which assert `stmt.column_count() == *_ROW_FIELDS` BEFORE any value.
- **`SPACE_COLUMNS` differs from the daemon's `SPACES_TABLE`** (4 names vs 5 full columns) — same reason as `SESSION_COLUMNS`: a deliberate subset because `sort_order` is only an ORDER BY key. A naive "desktop list vs daemon list" fingerprint comparison would therefore FALSELY report `spaces` as drifted. The correct fingerprint is per-table FULL DDL (name + type, all columns) — which is exactly what the daemon's `TABLE_SHAPES` already is.
- The desktop has **no `user_version` anywhere** (`grep -cn 'user_version' → 0` confirmed), and **no `journal_mode` / `synchronous` handling** (grep found none) — it only sets `foreign_keys = true` on open and then runs its migration chain.
- The desktop writes `archimedes.db` at `app.path().app_data_dir()/archimedes.db`; the daemon writes a DIFFERENT file (`fulcrum.db` in its own data dir). They never share a file today.

### 4.3 The desktop's migration chain (the non-goal) and its open sequence

`Db::open` (`db.rs` ~line 304): set `foreign_keys=true` → gate: `DROP COLUMN agent_id` (+ delete non-`archimedes` sessions) → gate: `native_messages_migrated` rename recovery → `execute_batch(SCHEMA)` → gated `ADD COLUMN` steps: `spaces.trusted`, `sessions.archived`, `sessions.context_usage_json`, `sessions.is_subagent`, `sessions.todos_json`, `sessions.cost_usd` (deliberately NO default — the D5/ADR-0082 rule), `spaces.sort_order`, `sessions.sort_order`, `sessions.title_stage` → the `native_messages` FK rebuild (a `native_messages_migrated` create + INSERT…SELECT + swap, gated on whether the FK is present). Steps are gated on `PRAGMA table_info` probes (helper `fn gate(conn, table, column, add)`). Every one of these exists because a user's `archimedes.db` predates the column; the daemon's fresh file is born with them. **The daemon's `schema.rs` module docs explicitly refuse to port this chain ("porting the bug history of a file this daemon does not own").**

## 5. The pin from the desktop to fulcrum (what a cross-repo CI check can assume)

`archimedes/src-tauri/Cargo.toml:47`:

```toml
fulcrum-core = { git = "https://github.com/archimedes-codes/fulcrum", rev = "78e595436ac5612d8ef87f692bee61f48f6e58e7" }
```

- Rev-pinned git dep (full 40-char rev), with a comment: "The `rev` is the FULL 40-char commit and MUST stay a full rev, never a branch name … Bump it in the same PR that reviews the engine diff." The engine ALSO lives inside this same repo as `fulcrum` member + `fulcrum-core` root package.
- **Critical consequence for T2's CI design:** the desktop pins by **rev**, and that rev (`78e595436ac5612d8ef87f692bee61f48f6e58e7`) is a commit that is NOT what this fulcrum checkout is at (`958306ddd1a3a35dc79fc84662c91b1f863488b1 — "docs: remove fulcrum-daemon roadmap — shipped (PR #2)"`). The two repos' CI runs against DIFFERENT revisions; a cross-repo check can therefore only assume ONE firm link: **the `rev` string itself** (available as text in the desktop's `Cargo.toml`), plus the soft, comment-only assumption that schema changes move with the engine. Any "export a fingerprint from fulcrum, compare it in the desktop's CI" scheme must fetch the *pinned* fulcrum revision's fingerprint (sparse-checkout the DDL file at that rev, or read the fingerprint file at that rev), or it will judge the daemon against a moving/older target.
- On the NAMING asymmetry: the module header says "verified against `4f9b99dc`" = this desktop checkout's HEAD, while the desktop pins engine rev `78e59543…`. The two "pins" are different directions and different objects; a planner should not assume `78e59543` equals anything in this fulcrum checkout (it likely predates it).
- The rev-pinned dep works today only because the current data contract is byte-stable (§1). It gives no schema-level guarantee at all.

## 6. Conventions a cross-repo workflow must fit (the ci-verification area grounds these; found in this repo's CI)

`fulcrum/.github/workflows/ci.yml` — the single workflow, `name: CI`, triggered `on: push branches: [main, feature/*]` + `pull_request`. Steps: checkout → install toolchain from `rust-toolchain.toml` (`rustup toolchain install`; the file is the pin, CI names no version) → `Swatinem/rust-cache@v2` (workspaces: `.`) → `cargo fmt --all -- --check` → `cargo clippy --all-targets -- -D warnings` → `cargo test` → `bash scripts/check-rquickjs-allocator.sh --self-test` → `bash scripts/check-rquickjs-allocator.sh`. Platform matrix: ubuntu, windows, macOS (all three — deliberate per-file rationale). Explicit "**no** `cargo build --all-targets` step before the tests" (materialising bins would disarm the `tests/fixture_bins.rs` trap). A T2 CI step would be a new `name:` block in this file (or a second workflow) and must not violate the build-step ban / fixture_bins contract.

Repo conventions that hard-carry into any new test file: `cargo fmt` enforced, clippy `-D warnings` over `--all-targets`, "prove the guard can fail" pairs (every vacuity guard is accompanied by a planted-mutation witness), and the file-count vacuity floors in `boundary_pins` (§7). `tests/fixture_bins.rs` (root `tests/`) is the ONLY file in the root `tests/` dir — a T2 unit test would go either in the crate's existing unit-test blocks or in `fulcrum/tests/`; **there is no `fulcrum/tests/` dir yet, and AGENTS.md pins root `src/` + root `tests/` locations** (see §7).

## 7. Constraints a planner must not violate

Confirmed as *stated* (verbatim or near-verbatim from the sources) plus *observed*:

1. **The desktop's migration chain is a non-goal and must stay untouched.** `schema.rs` module docs: the desktop's migration chain (agent_id rip-out, the archived/context_usage_json/todos_json/cost_usd/trusted column adds, the native_messages FK rebuild) is "NOT here — porting the chain would be porting the bug history of a file this daemon does not own." And running in the desktop repo, T2 must not add a migration of its own (the desktop model is gated `ADD COLUMN`s, `user_version` never touched). The daemon's `migrate` slot shape must also stay: one `if` per version, idempotent by construction, refuse `from > SCHEMA_VERSION`.
2. **`rusqlite` is legal in the daemon and illegal in the engine — and `boundary_pins` polices the ENGINE ONLY and must NOT be widened to walk the sibling.** `db/mod.rs` module docs: "`rusqlite` is legal in this member and forbidden in the engine … `boundary_pins` walks `{CARGO_MANIFEST_DIR}/src` and reads only `{CARGO_MANIFEST_DIR}/Cargo.toml`, so it does not police this sibling at all; that is intended for `rusqlite` and is Task 6's job to pin positively." AGENTS.md ("`boundary_pins` does not see the sibling member"): "do not widen `boundary_pins` to walk the sibling, because it would then fail on the daemon's legitimate `rusqlite` and the fix for that would be an allowlist that weakens the engine's pin." The daemon member's own manifest comment says the same ("Do NOT widen `boundary_pins` to walk this sibling in the meantime"). **A cross-repo fingerprint script/test living in the engine crate (root) must read the DDL as a TEXT FILE, not with `rusqlite` — any `rusqlite` use must live in the daemon member (`fulcrum/`), never in engine `src/`.**
3. **The two `UNIQUE` constraints and `ON DELETE CASCADE` are load-bearing and must not be "simplified".** `schema.rs` module docs: `UNIQUE(session_id, kind, message_key)` is the display upsert key and `UNIQUE(session_id, seq)` the transcript upsert key — `fulcrum_core::persist::TranscriptPersister` is idempotent **because** the upsert has something to conflict with; drop either and a replayed frame silently becomes a duplicate row. Both `ON DELETE CASCADE`s: `Db::delete_session` issues ONE `DELETE FROM sessions`; the cascade takes the rows with it, and `foreign_keys=ON` (set per-connection in `Db::open`) is what makes it live. These are pinned by `Db::table_has_unique` + `tests/db.rs` (`the_unique_constraints_the_persister_relies_on_exist`, `deleting_a_session_cascades_to_its_messages`, `re_applying_the_same_frame_stores_one_row`) and by the desktop's identical DDL. A fingerprint that "normalises away" constraints would be a false friend; the daemon's `ColumnShape` deliberately does NOT pin notnull/default/pk — T2 should not try to "tighten" that either.
4. **The layout rule: `src/` and `tests/` at the workspace root must not move.** AGENTS.md "The layout is forced: do not move `src/` and `tests/` into a `fulcrum-core/` directory" — moving them to a `fulcrum-core/` subdir makes the `childproc` census panic (`the census resolved the wrong root … src-tauri/src is not a directory`); five tests redden. Root `Cargo.toml` comment repeats: "Do NOT move them into a `fulcrum-core/` subdirectory." `members = ["fulcrum"]` only, `default-members = [".", "fulcrum"]` (deleting that line silently UN-GATES the daemon member from bare commands). Any doc/prose T2 writes should therefore keep the layout description accurate.
5. **No `cargo build --all-targets` step in CI** (ci.yml + AGENTS.md) — a T2 CI job must not re-add it.
6. **The daemon's own file (`fulcrum.db`) is 0600/0700-guarded and WAL-forced** — any T2 component that opens a database must keep `Db::open`'s pragma order (WAL first, read back; NORMAL; foreign_keys ON) or the `NotWal`/mode tests fail. The free-standing lint conventions: clippy `-D warnings`, `cargo fmt --check`, Cargo.lock untouchable by hand.
7. **`Cargo.toml` dependency-lists are owned by other tasks**: the daemon member's manifest comment says Tasks 3/4/5 "are forbidden from editing this file, so the list must be complete or they stall"; a T2 feature that needs a new dependency (e.g. a hash crate) is NOT a raw manifest edit — it is a cross-area negotiation.
8. **Pin-doc consistency**: the schema.rs header's commit citation (`4f9b99dc`) is currently TRUE of this desktop checkout; a T2 fingerprint that changes the mechanism must also update that header prose (the issue requires it) — and only prose, not the DDL.

## 8. Options space (facts for the planner — no implementation chosen here)

Given the state facts, the candidate directions a cross-repo fingerprint could take (each with its observed friction):

- **Option 1 (the issue's recommendation): export a canonical fingerprint from each repo; compare in CI; name the diverging TABLE.** The daemon's CANONICALISATION ALREADY EXISTS: `schema.rs`'s `TABLE_SHAPES` is order-sensitive, type-typed, and proven to match the DDL by `the_pinned_shapes_match_the_ddl_they_claim_to_describe`; `SESSION_TABLE`/`MESSAGES_TABLE`/`NATIVE_MESSAGES_TABLE`/`SPACES_TABLE` are exactly "the fingerprint" text-nameable per table. The missing halves: (a) a stable, shareable serialisation of the daemon's shapes (or of `SCHEMA`), (b) the desktop's equivalent full-shape source (must be per-table FULL DDL — the desktop's `SESSION_COLUMNS`/`SPACE_COLUMNS` are subsets and cannot be compared directly, §4.2), (c) the comparison job and its trigger. Friction to report: the job must target the desktop's pinned rev `78e59543…` or compare "current desktop ↔ current fulcrum at a defined revision" and say which — the rev pin is the only hard link; DDL text includes the index row (`idx_messages_session`) and `IF NOT EXISTS` wording, so a structural (parsed) fingerprint vs a text hash have different sensitivities (a text hash flags comments/whitespace churn; a parsed fingerprint must replicate the shape extraction `schema.rs` already does for the daemon). `sha256sum` of the two files is byte-identical today (§1) — any canonicalised format is a CHANGE in mechanism, so the issue's "module header must describe the real mechanism" is unavoidable either way.
- **Desktop-side CI job shape (to be grounded by the ci-verification area, but observed here):** the desktop consumes `fulcrum-core` by git rev; the natural fixture is the engine repo's checked-out text at that rev (sparse checkout or `actions/checkout` of the fulcrum repo at `78e59543…`), then a diff/normalised-compare of the two `SCHEMA`s naming the table from the daemon's `TABLE_SHAPES` grouping. Repo-local constraints: fulcrum CI's YAML is one file with a 3-OS matrix; adding a job belongs to the ci-verification area's conventions, and any "can fail" proof must accompany a new guard per the repo's vacuity-guard discipline.
- **Version-number negotiation (rejected-ish by the sources, recorded for completeness):** `schema.rs` itself says "Real cross-repo schema versioning (a version the desktop and the daemon negotiate)" is the follow-up; but the desktop STAMPS NO `user_version` and gates migrations on probes, so a negotiated number would be a new desktop mechanism — squarely in the "desktop migration chain is a non-goal" constraint. A fingerprint is the lighter-weight option the issue points at; the version number is a heavier one blocked by the desktop-touch constraint unless the desktop repo agrees to carry it.

**Risks (facts):** DDL drift is real but LATENT — everything is green today because both sides are byte-identical; the desync surfaces only when a desktop build reads a daemon-written row, and the daemon's own `SchemaFork` at open can't see the desktop's fork; the desktop's engine rev is older than this checkout, so a cross-repo check comparing "current fulcrum" to "desktop's `rev`" will be judging different engine versions unless it reads the fingerprint at the pinned rev; the daemon `tests/db.rs` documents a path (`tests/db.rs`) that isn't where the file lives (§3.3); `boundary_pins`'s floor counts (`> 40` files, `> 30` modules) are measured numbers that a new engine-side test file could shift only at the margins (a new file adds to the walk — harmless), but any new engine-side file adds to the walk `crate_code()` performs, so keep it out of the engine `src/` tree if it doesn't need to be there.

---

## Sources (verification paths)

- `/home/daniel/Coding/Archimedes/fulcrum/fulcrum/src/db/schema.rs` — SCHEMA, SCHEMA_VERSION, migrate, TABLE_SHAPES, the three tests, the module header (incl. the "follow-up ticket" paragraph and the `4f9b99dc` claim).
- `/home/daniel/Coding/Archimedes/fulcrum/fulcrum/src/db/mod.rs` — Db::open (pragma order, migrate, verify_shapes), DbError variants, SESSION_COLUMNS / MESSAGE_COLUMNS + arity ties, session_row_from / message_row_from, make_private / ensure_private_dir, unit tests.
- `/home/daniel/Coding/Archimedes/fulcrum/fulcrum/src/db/persistence.rs` — the `impl FulcrumPersistence for Db` (context for the "why by-index reads are load-bearing" story).
- `/home/daniel/Coding/Archimedes/fulcrum/fulcrum/tests/db.rs` — the daemon's database tests (schema-exactly-four-tables, UNIQUE probes, the eight SchemaFork mutation tests, cascade, WAL, modes).
- `/home/daniel/Coding/Archimedes/archimedes/src-tauri/src/storage/db.rs` (HEAD 4f9b99dc, remote `git@github.com:archimedes-codes/desktop.git`) — SCHEMA (1565–1604), Db::open migration chain (304–~560), SESSION_COLUMNS/MESSAGE_COLUMNS/SPACE_COLUMNS + ROW_FIELDS ties, mappers, DbError 3-variant / StoreError map.
- `/home/daniel/Coding/Archimedes/archimedes/src-tauri/src/storage/db/tests/mod.rs` — desktop mapper drift-guards (3484, 3574, ~3655).
- `/home/daniel/Coding/Archimedes/archimedes/src-tauri/Cargo.toml:47` — the `fulcrum-core` rev pin.
- `/home/daniel/Coding/Archimedes/fulcrum/.github/workflows/ci.yml`, `/home/daniel/Coding/Archimedes/fulcrum/AGENTS.md`, `/home/daniel/Coding/Archimedes/fulcrum/Cargo.toml`, `/home/daniel/Coding/Archimedes/fulcrum/fulcrum/Cargo.toml`, `/home/daniel/Coding/Archimedes/fulcrum/src/boundary_pins.rs` — conventions and constraints (§6, §7).

*Comparison method: `sed` extraction of both `SCHEMA` consts + `sha256sum` + `diff -u`; both hashes `f072253979a13d4be1b934a64ad33cc06dff332de8ac394df55be5bc654995f1` (39-line extracts each). No builds, tests, or git mutations were run anywhere (sibling agent owns the build lock; desktop checkout was read-only).*

---

## Will touch

Files a T2 implementation must edit or create (with current state):

| File | Status | Why |
|---|---|---|
| `fulcrum/fulcrum/src/db/schema.rs` | **modified** | The issue's mandated doc update: the module header's "This is a follow-up ticket" paragraph and the "verified against `4f9b99dc`" transcript note must be rewritten to describe the real (fingerprint/CI) mechanism. If the fingerprint is exported from the daemon side, the exporter (a `pub fn`/const serialising `TABLE_SHAPES` or `SCHEMA` and/or a checked-in fingerprint file + a self-test that it matches the DDL) lives here; `SCHEMA`, `SCHEMA_VERSION`, `TABLE_SHAPES`, the three unit tests §2.3 remain untouched in substance. |
| `fulcrum/fulcrum/src/db/mod.rs` | **modified (doc-only, likely)** | `verify_shapes`'s doc comments cite `tests/db.rs` — the path is `fulcrum/tests/db.rs` (§3.3). Only if T2's prose sweep reaches it. |
| `fulcrum/fulcrum/tests/db.rs` | **modified (likely)** | If the exporter ships a self-test, the daemon's integration suite may gain a piece (e.g. "the exported fingerprint matches what `Db::verify_shapes` enforces"). Optional. |
| `fulcrum/src/boundary_pins.rs` | **modified? — see constraint §7.2** | The module docs claim `boundary_pins` "does not police this sibling at all; that is intended … Task 6's job" and "Do NOT widen `boundary_pins` to walk this sibling". T2 MUST NOT widen it; the doc line may need adjusting only if it misdescribes the new fingerprint mechanism, and even then carefully — the AGENTS.md prohibition is hard. |
| `fulcrum/.github/workflows/ci.yml` | **modified (only if the comparison job lives in fulcrum CI)** | A new job/step comparing the two repos' fingerprints at the desktop's pinned rev. The ci-verification area grounds the workflow conventions; the new step must obey no-build-step / fixture_bins / vacuity-proof rules (§6). |
| `fulcrum/README.md` | **modified (doc)** | If the mechanism is user-visible (it is a cross-repo guarantee); README is the repo's other prose surface. Optional. |
| `fulcrum/AGENTS.md` | **modified? (doc)** | AGENTS.md carries the "boundary_pins does not see the sibling" and layout sections; a new workflow would deserve a bullet here per the file's density of process documentation — but the layout £§7.4 rule itself must NOT be edited away. |
| `archimedes/…` (desktop repo) | **modified — OUT OF SCOPE for this repo** | The desktop's half of the fingerprint (its full-shape source, likely a small const/test in `src-tauri/src/storage/db.rs`) is a change in the OTHER repository. It must exist for the comparison to mean anything; it is not a file this repo can edit. Report as an inter-repo dependency, not a file T2 "will touch". |

## Provides

Symbols other tasks can consume (verbatim signatures):

```rust
// fulcrum/fulcrum/src/db/schema.rs (all pub)
pub const SCHEMA_VERSION: i64 = 1;
pub const SCHEMA: &str = /* the 39-line DDL of §2.1 */;
pub use schema::SCHEMA_VERSION;                       // db/mod.rs

pub struct ColumnShape { pub name: &'static str, pub decl_type: &'static str }
pub struct TableShape  { pub name: &'static str, pub columns: &'static [ColumnShape] }
pub const SESSION_TABLE: [ColumnShape; 12];   // all 12 sessions columns, name+type, in order
pub const MESSAGES_TABLE: [ColumnShape; 6];   // all 6 messages columns
pub const NATIVE_MESSAGES_TABLE: [ColumnShape; 5];
pub const SPACES_TABLE: [ColumnShape; 5];
pub const TABLE_SHAPES: [TableShape; 4];      // one per table, in SCHEMA declaration order

pub fn migrate(conn: &mut rusqlite::Connection, from: i64) -> Result<i64, DbError>;

// The existing test fn (the one the issue cites as self-referential):
#[test] fn schema_text_holds_the_desktops_shape();        // schema.rs:384
#[test] fn the_shape_list_covers_every_table_in_the_schema();
#[test] fn the_pinned_shapes_match_the_ddl_they_claim_to_describe();
```

**The canonicalisation helper already present:** `SchemaFork`-style shape comparison = `Db::verify_shapes` (private, `db/mod.rs`) + the `TABLE_SHAPES` constants + the two "shapes match the DDL" tests — i.e. the daemon already HAS a canonical per-table (name, type, order) fingerprint; **what does not exist anywhere: any export/serialisation of it, any desktop-side full-shape source, and any cross-repo comparison.** A planner can build all three FROM `TABLE_SHAPES` without new schema knowledge.

Also consumable (belt/braces): `Db::table_has_unique(&self, table: &str, columns: &[&str]) -> bool`, `Db::table_names() -> Result<Vec<String>, DbError>`, `DbError::SchemaFork { table, detail }`, and the tests in `fulcrum/tests/db.rs` (list in §3.3).

## Needs from other areas

- **The cross-repo CI job's conventions** (where the workflow file goes, how it triggers, the vacuity-proof discipline, the no-build-step rule) — producer: the **ci-verification** area, which "grounds the conventions for" new workflow files. Without that grounding T2 can design the *data contract* (the fingerprint format + per-table naming) but must not own the workflow's shape.
- **A decision on the desktop's pinned rev as the comparison target** (`78e595436ac5612d8ef87f692bee61f48f6e58e7`, from `archimedes/src-tauri/Cargo.toml:47`) — producer: the **plan agent** (a design decision, not a research fact; the fact T2 provides is that the rev is the only hard link and it is NOT this checkout's HEAD).
- Everything else T2 needs is **self-contained**: `rusqlite` lives legally in the daemon member already (`fulcrum/fulcrum/Cargo.toml:80`, features `["bundled"]`), `serde`/`serde_json` are present, no new dependency is *required* for a text-hash fingerprint (a hash could even be a string compare or a hand-rolled FNV — though if a cryptographic hash is wanted, adding the crate is a manifest edit owned per constraint §7.7). **Nothing — T2 is self-contained except for the new workflow file, which the ci-verification area grounds the conventions for.** (The desktop repo's own fingerprint half is a cross-repo dependency, not an in-repo one.)
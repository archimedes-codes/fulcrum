//! T9 Task 4: the daemon's `fulcrum.db` — WAL, the desktop's four tables, and
//! `impl FulcrumPersistence for Db`.
//!
//! Every test here asserts a DATABASE property, for the reason `persist.rs`
//! records for its own relocated tests: a double that mimics a constraint proves
//! nothing about it. So these drive the real `rusqlite` connection in a temp dir.

use std::path::Path;
use std::sync::Arc;

use fulcrum::db::schema::{schema_shape_fingerprint, SCHEMA, SCHEMA_VERSION};
use fulcrum::db::{Db, DbError};
use fulcrum_harness::persist::TranscriptPersister;
use fulcrum_harness::seams::FulcrumPersistence;
use fulcrum_harness::store_rows::{DisplayRow, SessionInfo};
use fulcrum_harness::worker_types::StoreFrame;

/// A store open in a fresh temp dir, as the `Arc` both shapes need: the tests
/// hold `Arc<Db>` so they can reach the inherent introspection helpers AND let it
/// coerce to `Arc<dyn FulcrumPersistence>` — the same dual use the supervisor
/// makes of it.
struct Harness {
    _dir: tempfile::TempDir,
    db: Arc<Db>,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = Arc::new(Db::open(&dir.path().join("fulcrum.db")).expect("open the daemon store"));
        Self { _dir: dir, db }
    }

    /// The seam view — the ONLY shape `TranscriptPersister::new` accepts.
    fn seam(&self) -> Arc<dyn FulcrumPersistence> {
        Arc::clone(&self.db) as Arc<dyn FulcrumPersistence>
    }

    fn count(&self, table: &str) -> i64 {
        self.db.count_rows(table).expect("count rows")
    }
}

fn session_info(id: &str, cwd: &str, is_subagent: bool) -> SessionInfo {
    SessionInfo {
        session_id: id.to_string(),
        cwd: std::path::PathBuf::from(cwd),
        capabilities: serde_json::json!({ "loadSession": true }),
        config_options: None,
        archived: false,
        title: None,
        context_usage: None,
        cost_usd: None,
        is_subagent,
    }
}

fn insert_frame(sid: &str, seq: u64) -> StoreFrame {
    StoreFrame::Insert {
        session_id: sid.to_string(),
        seq,
        role: "user".to_string(),
        content_json: serde_json::json!({ "role": "user", "content": "hello" }).to_string(),
    }
}

fn display_frame(sid: &str, key: &str) -> StoreFrame {
    StoreFrame::Display {
        session_id: sid.to_string(),
        rows: vec![DisplayRow {
            kind: "agent-text".to_string(),
            message_key: key.to_string(),
            payload_json: serde_json::json!({ "text": "hi" }),
            created_at: 1,
        }],
    }
}

// ── WAL: asserted by query AND by sidecar file ────────────────────────────────

/// `PRAGMA journal_mode` must REPORT `wal`. This is the assertion that fails on a
/// filesystem that cannot do WAL (some network mounts silently keep `delete`),
/// which is the whole reason `Db::open` refuses to pretend otherwise.
#[test]
fn the_store_reports_wal_mode_when_asked() {
    let h = Harness::new();
    let mode = h.db.journal_mode().expect("read journal_mode back");
    assert_eq!(
        mode.to_lowercase(),
        "wal",
        "`PRAGMA journal_mode` must report `wal`, got `{mode}`"
    );
}

/// The sidecar files are the physical proof. A `PRAGMA` that said `wal` while no
/// `-wal` file was ever created would be a durability story that is false, so the
/// query alone is not enough — a write has to leave the `-wal` / `-shm` pair.
#[test]
fn a_write_leaves_the_wal_sidecar_files() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fulcrum.db");
    let db = Db::open(&path).expect("open");

    // A real committed write, not a read.
    db.record_session(&session_info("s1", "/tmp", false))
        .expect("record_session");

    assert!(
        path.exists(),
        "the database file itself must exist at {}",
        path.display()
    );
    assert!(
        sidecar(&path, "-wal").exists(),
        "a WAL database must have its `-wal` sidecar after a write; expected {}",
        sidecar(&path, "-wal").display()
    );
    assert!(
        sidecar(&path, "-shm").exists(),
        "a WAL database must have its `-shm` sidecar after a write; expected {}",
        sidecar(&path, "-shm").display()
    );
}

fn sidecar(path: &Path, suffix: &str) -> std::path::PathBuf {
    let name = path
        .file_name()
        .expect("the db path has a file name")
        .to_string_lossy()
        .into_owned();
    path.with_file_name(format!("{name}{suffix}"))
}

/// `synchronous=NORMAL` is the third pragma and the only one with no sidecar to
/// read, so it is asserted by query.
#[test]
fn the_store_runs_at_synchronous_normal() {
    let h = Harness::new();
    assert_eq!(h.db.synchronous().expect("read synchronous"), "normal");
}

// ── The schema is the desktop's, not a fork of it ─────────────────────────────

/// The four tables the desktop owns, and NOTHING else: a fifth table here would
/// be the daemon silently forking a schema it does not own.
#[test]
fn the_schema_is_exactly_the_desktops_four_tables() {
    let h = Harness::new();
    let mut tables = h.db.table_names().expect("sqlite_master");
    tables.sort();
    assert_eq!(
        tables,
        vec!["messages", "native_messages", "sessions", "spaces"],
        "the daemon's `fulcrum.db` must hold the desktop's four tables and no others"
    );
}

/// The `UNIQUE` constraints are load-bearing — the persister's idempotency IS
/// them — so they are pinned by introspection rather than by trust in the text.
#[test]
fn the_unique_constraints_the_persister_relies_on_exist() {
    let h = Harness::new();
    assert!(
        h.db.table_has_unique("messages", &["session_id", "kind", "message_key"]),
        "`messages` needs UNIQUE(session_id, kind, message_key) — the display upsert key"
    );
    assert!(
        h.db.table_has_unique("native_messages", &["session_id", "seq"]),
        "`native_messages` needs UNIQUE(session_id, seq) — the transcript upsert key"
    );
}

/// The DDL text carries the same promise, so a reader who lands on `schema.rs`
/// sees the four tables without opening sqlite.
#[test]
fn the_schema_const_declares_the_four_tables_and_the_index() {
    for needle in [
        "CREATE TABLE IF NOT EXISTS sessions",
        "CREATE TABLE IF NOT EXISTS messages",
        "CREATE TABLE IF NOT EXISTS native_messages",
        "CREATE TABLE IF NOT EXISTS spaces",
        "CREATE INDEX IF NOT EXISTS idx_messages_session",
        "UNIQUE(session_id, kind, message_key)",
        "UNIQUE(session_id, seq)",
        "ON DELETE CASCADE",
    ] {
        assert!(SCHEMA.contains(needle), "SCHEMA is missing `{needle}`");
    }
}

// ── The schema cross-check: a fork is refused at open (finding 10) ───────────
//
// The daemon reads `sessions` and `messages` POSITIONALLY (`session_row_from`
// indexes columns, it does not name them), so a column the desktop adds is
// invisible and a column it renames silently mis-maps into
// `InvalidColumnType` → `Ok(None)`. Documented-and-undetected is the shape that
// rots, so `Db::open` verifies `PRAGMA table_info` against a pinned shape and
// refuses the file. These tests build the bad shapes for real.
//
// # Why the fixtures are hand-written and not derived from the constant
//
// `SESSIONS_DDL` below is spelled out in this file rather than cut out of
// `schema::SCHEMA`, and the mutations are string surgery on it. A fixture built
// by editing the very constant the check compares against would go blind the day
// that constant is wrong — which is precisely the bug class under test.
// `the_test_fixture_declares_the_pinned_session_shape` is the cross-check that
// keeps the fixture honest: it asserts the hand-written DDL declares exactly the
// columns `schema::SESSION_TABLE` pins.

/// The desktop's `sessions` DDL, hand-written (see the section header).
const SESSIONS_DDL: &str = "CREATE TABLE sessions (
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
);";

/// The desktop's `messages` DDL, hand-written (see the section header).
const MESSAGES_DDL: &str = "CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    message_key TEXT,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id, kind, message_key)
);";

/// The desktop's `native_messages` DDL, hand-written (see the section header).
const NATIVE_MESSAGES_DDL: &str = "CREATE TABLE native_messages (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    content_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id, seq)
);";

/// The desktop's `spaces` DDL, hand-written (see the section header).
const SPACES_DDL: &str = "CREATE TABLE spaces (
    path TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    last_opened_at INTEGER NOT NULL,
    trusted INTEGER NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0
);";

/// A `fulcrum.db` whose `table` has the shape `ddl` spells, everything else real.
///
/// The file is built by running the REAL `SCHEMA` and then dropping and replacing
/// the one table under test, so the other three are byte-identical to the shipped
/// ones and a refusal can only be about the table that was swapped.
///
/// `user_version` is stamped to `SCHEMA_VERSION` deliberately: this simulates a
/// file a legitimate migration already produced, so the refusal cannot be the
/// version gate doing the work and `migrate` cannot paper over the fixture.
fn altered_store(table: &str, ddl: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fulcrum.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("create the fixture file");
        conn.execute_batch(SCHEMA)
            .expect("build the real four tables");
        conn.execute_batch(&format!("DROP TABLE {table};\n{ddl}"))
            .unwrap_or_else(|e| panic!("swap in the altered `{table}`: {e}"));
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)
            .expect("stamp the version this build accepts");
    }
    (dir, path)
}

/// Applies `edit` to `ddl`, and REFUSES to produce an unchanged string.
///
/// The assertion is not ceremony: `str::replace` on a needle that drifted out of
/// the fixture returns the input untouched, so a mutated needle would silently
/// hand back the CORRECT table and the test would pass while asserting nothing.
fn mutated(ddl: &str, from: &str, to: &str) -> String {
    let out = ddl.replacen(from, to, 1);
    assert_ne!(
        out, ddl,
        "the fixture's needle `{from}` no longer appears in the DDL — the \
         mutation is a no-op and the test would pass vacuously"
    );
    out
}

/// The fixture stays a faithful copy of the pinned shape.
///
/// This is the guard against the whole section above drifting: if
/// `schema::SESSION_TABLE` and this file's hand-written DDL ever disagree, one of
/// them is wrong and every mutation test built on it is testing the wrong table.
#[test]
fn the_test_fixture_declares_the_pinned_session_shape() {
    let pinned: Vec<&str> = fulcrum::db::schema::SESSION_TABLE
        .iter()
        .map(|column| column.name)
        .collect();
    let in_fixture: Vec<&str> = SESSIONS_DDL
        .lines()
        .filter_map(|line| {
            let name = line.trim().split([' ', '(']).next().unwrap_or("").trim();
            // Skip the `CREATE TABLE …` line and the table-level constraints.
            (!name.is_empty()
                && !name.starts_with("CREATE")
                && !name.starts_with(')')
                && !name.starts_with("UNIQUE")
                && !name.starts_with("PRIMARY"))
            .then_some(name)
        })
        .collect();
    assert_eq!(
        in_fixture, pinned,
        "this file's hand-written `sessions` DDL must declare exactly the columns \
         `schema::SESSION_TABLE` pins, or the mutation tests below are mutating a \
         different table than the one the check reads"
    );
}

/// The real schema still opens — the check is a guard, not a ban.
///
/// The failure this rules out is a check so strict it rejects the file the daemon
/// just wrote itself, which would take the daemon down on every start. Re-opening
/// a file the daemon created and WROTE to is the exact path a restart takes.
#[test]
fn a_file_the_daemon_wrote_itself_still_opens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fulcrum.db");

    let db = Db::open(&path).expect("first open");
    db.record_session(&session_info("reopen", "/tmp/reopen", false))
        .expect("record_session");
    drop(db);

    let db = Db::open(&path).expect("re-open: the shipped shape must pass the check");
    assert_eq!(
        db.session("reopen")
            .expect("session")
            .expect("the row is still there")
            .cwd,
        "/tmp/reopen",
        "the check must not disturb the data it passes"
    );
    // And every one of the four tables is covered by the shape constants, so a
    // fresh file satisfies all four.
    let mut tables = db.table_names().expect("sqlite_master");
    tables.sort();
    assert_eq!(
        tables,
        vec!["messages", "native_messages", "sessions", "spaces"]
    );
}

/// A column this build reads POSITIONALLY, missing from the file (finding 10).
///
/// `capabilities_json` is deliberately a MIDDLE column, not the tail: dropping
/// the last column is the benign direction (the reader stops early), whereas a
/// hole in the middle shifts every later index and the mapper reads
/// `archived` out of `context_usage_json`'s slot. That mis-map is the defect.
#[test]
fn a_missing_column_refuses_the_open_and_names_the_table_and_column() {
    let (dir, path) = altered_store(
        "sessions",
        &mutated(SESSIONS_DDL, "    capabilities_json TEXT NOT NULL,\n", ""),
    );

    let err = match Db::open(&path) {
        Ok(_) => panic!(
            "a `sessions` table missing `capabilities_json` must NOT open — the \
             positional mapper would read the wrong columns out of every row"
        ),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        matches!(err, DbError::SchemaFork { .. }),
        "must be the dedicated variant, not a wrapped sqlite error; got: {err}"
    );
    assert!(
        message.contains("sessions"),
        "must name the table: {message}"
    );
    assert!(
        message.contains("capabilities_json"),
        "must name the column: {message}"
    );
    // The operator-facing distinction: "this file predates us / is corrupt", not
    // "the file is newer".
    assert!(
        message.contains("TEXT"),
        "must say what was EXPECTED (the declared type): {message}"
    );
    assert!(
        message.to_lowercase().contains("missing"),
        "must say the column is absent rather than merely different: {message}"
    );
    // Refusing must not damage the file — the operator still has the data.
    assert!(path.exists(), "refusing must leave {path:?} intact");
    drop(dir);
}

/// A column the file has and this build does not know (finding 10).
///
/// This is the "the desktop added a column" direction, and it is refused too: a
/// fork is a fork. The daemon must not silently ignore data it does not read,
/// because "ignored" and "lost on the next rewrite" are the same file state from
/// the desktop's side.
#[test]
fn an_unexpected_extra_column_refuses_the_open_and_names_it() {
    let (dir, path) = altered_store(
        "sessions",
        &mutated(
            SESSIONS_DDL,
            "    title_stage INTEGER NOT NULL DEFAULT 0",
            "    title_stage INTEGER NOT NULL DEFAULT 0,\n    \
             from_a_newer_desktop TEXT",
        ),
    );

    let err = match Db::open(&path) {
        Ok(_) => panic!(
            "a `sessions` table carrying a column this build does not know must \
             NOT open silently — the daemon would ignore data it cannot see"
        ),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        matches!(err, DbError::SchemaFork { .. }),
        "must be the dedicated variant; got: {err}"
    );
    assert!(
        message.contains("from_a_newer_desktop"),
        "must name the column it does not know: {message}"
    );
    assert!(
        message.contains("sessions"),
        "must name the table: {message}"
    );
    // The distinction the operator needs: THIS one reads as "a newer schema",
    // the missing-column case above reads as "predates us / corrupt".
    assert!(
        message.contains("newer"),
        "an unknown column must be reported as a NEWER schema, not as corruption: \
         {message}"
    );
    drop(dir);
}

/// A renamed column is refused, and BOTH names appear in the message.
///
/// A rename is the worst of the three for a positional reader — the column count
/// still matches, so a count-only check passes it — and it is the case where the
/// operator most needs both spellings, because "which desktop version renamed
/// this" is the question they will ask next.
#[test]
fn a_renamed_column_refuses_the_open_and_names_both_spellings() {
    let (dir, path) = altered_store(
        "sessions",
        &mutated(SESSIONS_DDL, "    title TEXT,", "    title_text TEXT,"),
    );

    let err = match Db::open(&path) {
        Ok(_) => panic!(
            "a renamed `title` must NOT open: the column COUNT matches, so only a \
             name check catches that `session_row_from` is now reading \
             `title_text` into `title`"
        ),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        matches!(err, DbError::SchemaFork { .. }),
        "must be the dedicated variant; got: {err}"
    );
    assert!(
        message.contains("title_text") && message.contains("title"),
        "must name the found column AND the expected one; got: {message}"
    );
    assert!(
        message.contains("sessions"),
        "must name the table: {message}"
    );
    drop(dir);
}

/// A reordered column is refused, and the message gives both positions.
///
/// `PRAGMA table_info` reports `cid` in declaration order, which is the order the
/// positional mappers assume — so a swap of two existing columns is a fork even
/// though the SET of columns is perfect. This is the case a name-set comparison
/// would wave through.
#[test]
fn a_reordered_column_refuses_the_open_and_reports_both_positions() {
    // Swap `archived` and `context_usage_json` (positions 5 and 6).
    let swapped = mutated(
        SESSIONS_DDL,
        "    archived INTEGER NOT NULL DEFAULT 0,\n    context_usage_json TEXT,\n",
        "    context_usage_json TEXT,\n    archived INTEGER NOT NULL DEFAULT 0,\n",
    );
    let (dir, path) = altered_store("sessions", &swapped);

    let err = match Db::open(&path) {
        Ok(_) => panic!(
            "a reordered `sessions` table must NOT open: `session_row_from` reads \
             by index, so `archived` and `context_usage_json` would trade slots"
        ),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        matches!(err, DbError::SchemaFork { .. }),
        "must be the dedicated variant; got: {err}"
    );
    assert!(
        message.contains("archived") || message.contains("context_usage_json"),
        "must name a column involved in the swap; got: {message}"
    );
    assert!(
        message.contains("5") && message.contains("6"),
        "a reorder must be reported with BOTH positions (expected 5, found 6, or \
         the reverse) so the operator can see it is an ordering problem and not a \
         missing column; got: {message}"
    );
    drop(dir);
}

/// A retyped column is refused: the declared type is part of the shape.
///
/// `session_row_from` does `get::<_, i64>` on `archived` and `is_subagent`, and
/// `get::<_, Option<f64>>` on `cost_usd`. SQLite's dynamic typing means a
/// `TEXT` `cost_usd` often still converts, so the failure is not a crash but a
/// silently different value class — which is why the declared type is checked
/// rather than trusted.
#[test]
fn a_retyped_column_refuses_the_open_and_names_both_types() {
    let (dir, path) = altered_store(
        "sessions",
        &mutated(SESSIONS_DDL, "    cost_usd REAL,", "    cost_usd TEXT,"),
    );

    let err = match Db::open(&path) {
        Ok(_) => panic!(
            "a `cost_usd` declared TEXT instead of REAL must NOT open: the mapper \
             reads it as a float and SQLite would coerce text into that slot"
        ),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        matches!(err, DbError::SchemaFork { .. }),
        "must be the dedicated variant; got: {err}"
    );
    assert!(
        message.contains("cost_usd"),
        "must name the column: {message}"
    );
    assert!(
        message.contains("REAL") && message.to_lowercase().contains("text"),
        "must give the expected type and the found type; got: {message}"
    );
    drop(dir);
}

/// All FOUR tables are checked, not just the two with mappers.
///
/// `native_messages` and `spaces` are read by NAME today, so a fork there is
/// loud rather than silent — but "loud at the first query" is a worse failure
/// than "refused at open", and the daemon owns all four tables' shapes. Each
/// table gets its own mutation, because a check that looped over a one-element
/// list would pass this test if it were written with a `sessions` special case.
#[test]
fn every_one_of_the_four_tables_is_verified_at_open() {
    for (table, ddl) in [
        (
            "sessions",
            mutated(SESSIONS_DDL, "    cwd TEXT NOT NULL,\n", ""),
        ),
        (
            "messages",
            // `payload_json` and not `kind`: `kind` is inside the table-level
            // `UNIQUE(session_id, kind, message_key)`, so dropping it is not
            // even legal DDL and the fixture would fail to build rather than
            // test the check.
            mutated(MESSAGES_DDL, "    payload_json TEXT NOT NULL,\n", ""),
        ),
        (
            "native_messages",
            mutated(NATIVE_MESSAGES_DDL, "    role TEXT NOT NULL,\n", ""),
        ),
        (
            "spaces",
            mutated(SPACES_DDL, "    trusted INTEGER NOT NULL DEFAULT 0,\n", ""),
        ),
    ] {
        let (dir, path) = altered_store(table, &ddl);
        let err = match Db::open(&path) {
            Ok(_) => panic!("a `{table}` table missing one of its columns must be refused"),
            Err(e) => e,
        };
        assert!(
            matches!(err, DbError::SchemaFork { .. }),
            "`{table}`: expected the dedicated variant, got {err}"
        );
        assert!(
            err.to_string().contains(table),
            "`{table}`: the error must name the table it rejected; got: {err}"
        );
        drop(dir);
    }
}

/// A table that is not there at all is refused, not silently treated as empty.
///
/// `PRAGMA table_info` on a missing table returns ZERO rows, which a naive check
/// would read as "no columns to disagree about" and pass. Then `record_session`
/// would fail on the first write, from a daemon that claimed to be healthy.
#[test]
fn a_missing_table_refuses_the_open_rather_than_reading_it_as_empty() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fulcrum.db");
    {
        let conn = rusqlite::Connection::open(&path).expect("create the fixture file");
        conn.execute_batch(SCHEMA).expect("build the four tables");
        conn.execute_batch("DROP TABLE spaces")
            .expect("drop one of them");
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)
            .expect("stamp the version");
    }

    let err = match Db::open(&path) {
        Ok(_) => panic!("a file missing the `spaces` table must be refused"),
        Err(e) => e,
    };
    let message = err.to_string();
    assert!(
        matches!(err, DbError::SchemaFork { .. }),
        "must be the dedicated variant; got: {err}"
    );
    assert!(
        message.contains("spaces") && message.to_lowercase().contains("no columns"),
        "must say the TABLE is absent (zero columns reported), not list a column; \
         got: {message}"
    );
    drop(dir);
}

/// The check runs AFTER `migrate`, so a legitimately-migrated file passes.
///
/// Ordering matters and is invisible from the outside: verify BEFORE the
/// migration and every fresh database is refused, because the tables do not
/// exist yet. `user_version = 0` is the fresh-file state, so this is the fixture
/// that pins the order.
#[test]
fn the_check_runs_after_migrate_so_a_fresh_file_still_opens() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fulcrum.db");
    {
        // An EMPTY file at version 0 — exactly what the first open of a new data
        // dir finds.
        let conn = rusqlite::Connection::open(&path).expect("create the empty file");
        conn.execute_batch("CREATE TABLE unrelated_placeholder (x INTEGER);")
            .expect("some content, so this is not a zero-byte file");
    }

    let db = Db::open(&path).expect("a version-0 file must migrate and then pass the shape check");
    assert_eq!(db.user_version().expect("user_version"), SCHEMA_VERSION);
    assert_eq!(db.count_rows("sessions").expect("count"), 0);
}

// ── Migrations ────────────────────────────────────────────────────────────────

/// One migration slot, stamped in `user_version`, and re-opening a migrated file
/// changes nothing (the desktop's migration chain is deliberately NOT ported —
/// this is a NEW file, not `archimedes.db`).
#[test]
fn user_version_records_the_schema_and_reopening_is_a_no_op() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fulcrum.db");

    let db = Db::open(&path).expect("first open");
    assert_eq!(db.user_version().expect("user_version"), SCHEMA_VERSION);
    db.record_session(&session_info("keep", "/tmp/keep", false))
        .expect("record_session");
    drop(db);

    let db = Db::open(&path).expect("second open must not fail on an existing file");
    assert_eq!(db.user_version().expect("user_version"), SCHEMA_VERSION);
    assert_eq!(
        db.count_rows("sessions").unwrap(),
        1,
        "a row survives reopen"
    );
    assert_eq!(
        db.session("keep")
            .expect("session")
            .expect("the row is still there")
            .cwd,
        "/tmp/keep"
    );
}

// ── The seam: `Db` as `Arc<dyn FulcrumPersistence>` ───────────────────────────

/// The acceptance criterion in one line: the trait is implemented, and the trait
/// object coerces — the only shape `TranscriptPersister::new` accepts.
#[test]
fn db_is_used_through_arc_dyn_fulcrumpersistence() {
    let h = Harness::new();
    let seam = h.seam();

    seam.record_session(&session_info("s1", "/tmp/one", false))
        .expect("record_session through the seam");
    let row = seam.session("s1").expect("session").expect("a row");
    assert_eq!(row.id, "s1");
    assert_eq!(row.cwd, "/tmp/one");
    assert!(!row.archived);
    assert!(!row.is_subagent);
    assert_eq!(row.cost_usd, None, "no cost frame yet is NOT Some(0.0)");
    assert!(
        seam.now_ms() > 1_700_000_000_000,
        "the seam's clock must be the shared unix-ms clock"
    );
}

/// The rest of the read/write surface, driven through the trait object.
#[test]
fn the_seam_round_trips_every_column_it_owns() {
    let h = Harness::new();
    let seam = h.seam();

    seam.record_session(&session_info("s1", "/tmp/one", false))
        .expect("record_session");

    // Native transcript: insert, load in seq order, atomic replace.
    seam.insert_native_message("s1", 1, "user", r#"{"role":"user"}"#)
        .expect("insert 1");
    seam.insert_native_message("s1", 2, "assistant", r#"{"role":"assistant"}"#)
        .expect("insert 2");
    assert_eq!(
        seam.load_native_messages("s1").expect("load"),
        vec![
            r#"{"role":"user"}"#.to_string(),
            r#"{"role":"assistant"}"#.to_string()
        ],
        "the transcript loads in seq order"
    );
    seam.replace_native_messages(
        "s1",
        &[(
            7,
            "user".to_string(),
            r#"{"role":"user","compact":true}"#.to_string(),
        )],
    )
    .expect("replace");
    assert_eq!(
        seam.load_native_messages("s1").expect("load after replace"),
        vec![r#"{"role":"user","compact":true}"#.to_string()],
        "replace is a rewrite, not an append"
    );

    // Display rows.
    seam.record_message("s1", "tool-call", Some("call-1"), r#"{"name":"read"}"#)
        .expect("record_message");
    let msgs = seam.messages_for("s1").expect("messages_for");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].kind, "tool-call");
    assert_eq!(msgs[0].message_key.as_deref(), Some("call-1"));
    assert_eq!(h.count("messages"), 1);

    // Session columns.
    seam.record_session_context_usage("s1", 120, 200_000)
        .expect("context usage");
    seam.record_session_cost("s1", 0.25).expect("cost");
    seam.record_session_todos("s1", Some(r#"[{"content":"x"}]"#))
        .expect("todos");
    assert_eq!(seam.session_cost("s1").expect("cost read"), Some(0.25));
    assert_eq!(
        seam.session_todos("s1").expect("todos read").as_deref(),
        Some(r#"[{"content":"x"}]"#)
    );
    let row = seam.session("s1").expect("session").expect("row");
    assert_eq!(
        row.context_usage_json.as_deref(),
        Some(r#"{"used":120,"window":200000}"#)
    );
    assert_eq!(seam.session_cost("nope").expect("missing row"), None);
    assert_eq!(seam.session_todos("nope").expect("missing row"), None);

    // A `None` todos write NULLs the column.
    seam.record_session_todos("s1", None).expect("todos clear");
    assert_eq!(seam.session_todos("s1").expect("todos after clear"), None);

    // `list_sessions` hides subagents in BOTH arms.
    seam.record_session(&session_info("sub", "/tmp/one", true))
        .expect("record subagent row");
    assert_eq!(h.count("sessions"), 2);
    let listed = seam.list_sessions(true).expect("list include_archived");
    assert_eq!(
        listed.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        vec!["s1"],
        "`list_sessions` filters `is_subagent = 0` in BOTH arms"
    );
    assert_eq!(seam.list_sessions(false).expect("list").len(), 1);
}

/// Spaces (the permission fixtures' seam methods).
#[test]
fn spaces_upsert_and_trust_round_trip() {
    let dir = tempfile::tempdir().expect("temp dir");
    let key = dir.path().to_string_lossy().into_owned();
    let h = Harness::new();
    let seam = h.seam();

    seam.upsert_space(&key, false).expect("upsert_space");
    assert_eq!(h.count("spaces"), 1);
    assert!(
        seam.set_space_trusted(&key, true)
            .expect("set trusted returns whether a row matched"),
        "a row exists, so the update matched"
    );
    assert!(
        !seam
            .set_space_trusted("/does/not/exist/at/all", true)
            .expect("missing row is a no-op"),
        "a missing Space row is a no-op, not an error"
    );
    // A re-upsert refreshes recency; it never changes `trusted` retroactively.
    seam.upsert_space(&key, false).expect("re-upsert");
    assert_eq!(h.count("spaces"), 1, "one row per path");
    assert!(seam.set_space_trusted(&key, true).expect("still trusted"));
}

// ── The persister's two database promises ─────────────────────────────────────

/// Re-applying the SAME frame must not duplicate a row. This is the property the
/// `UNIQUE` constraints exist to give `TranscriptPersister`, and the reason the
/// schema may not be "simplified".
#[test]
fn re_applying_the_same_frame_stores_one_row() {
    let h = Harness::new();
    let seam = h.seam();
    let persister = TranscriptPersister::new(Arc::clone(&seam));
    persister.set_cwd("s1", "/tmp/one");

    let frame = display_frame("s1", "msg-1");
    persister.apply(false, &frame);
    persister.apply(false, &frame);
    persister.apply(false, &frame);

    let msgs = seam.messages_for("s1").expect("messages_for");
    assert_eq!(
        msgs.len(),
        1,
        "three identical Display frames must collapse to ONE row (UNIQUE \
         session_id, kind, message_key)"
    );
    assert_eq!(h.count("messages"), 1, "and the table agrees");

    let native = insert_frame("s1", 3);
    persister.apply(false, &native);
    persister.apply(false, &native);
    assert_eq!(
        seam.load_native_messages("s1").expect("load").len(),
        1,
        "two identical Insert frames must collapse to ONE row (UNIQUE session_id, seq)"
    );

    // The frames created the session row themselves (`ensure_session_row`).
    assert_eq!(seam.list_sessions(true).expect("list").len(), 1);
}

/// The `cost_update` snapshot write is idempotent too (Fulcrum T2's amendment of
/// ADR 0082's limit 7): the same frame twice leaves the same figure.
#[test]
fn a_replayed_cost_frame_leaves_the_same_figure() {
    let h = Harness::new();
    let seam = h.seam();
    let persister = TranscriptPersister::new(Arc::clone(&seam));

    let frame = serde_json::json!({ "event": "cost_update", "payload": { "costUsd": 1.25 } });
    persister.apply_interactive_event("s1", true, &frame);
    persister.apply_interactive_event("s1", true, &frame);

    assert_eq!(
        seam.session_cost("s1").expect("cost"),
        Some(1.25),
        "a snapshot overwrite replayed must not double"
    );
    assert_eq!(
        seam.session("s1").expect("session").map(|r| r.is_subagent),
        Some(true),
        "the row the cost frame created is the subagent's hidden row"
    );
    assert_eq!(h.count("sessions"), 1, "and not two rows");
}

/// Deleting a session takes its `messages` AND its `native_messages` with it —
/// through the FK, because `Db::delete_session` issues ONE `DELETE` on `sessions`
/// (unlike the desktop's belt-and-suspenders version, which pre-deletes the
/// transcript and so could not tell a working cascade from a no-op one).
#[test]
fn deleting_a_session_cascades_to_its_messages() {
    let h = Harness::new();
    let seam = h.seam();
    let persister = TranscriptPersister::new(Arc::clone(&seam));
    persister.set_cwd("s1", "/tmp/one");

    persister.apply(false, &display_frame("s1", "msg-1"));
    persister.apply(false, &insert_frame("s1", 1));
    assert_eq!(seam.messages_for("s1").expect("messages").len(), 1);
    assert_eq!(seam.load_native_messages("s1").expect("native").len(), 1);

    h.db.delete_session("s1").expect("delete_session");

    assert!(seam.session("s1").expect("session").is_none());
    assert_eq!(
        seam.messages_for("s1").expect("messages").len(),
        0,
        "the display rows must go with the session (ON DELETE CASCADE)"
    );
    assert_eq!(
        seam.load_native_messages("s1").expect("native").len(),
        0,
        "the transcript must go with the session (ON DELETE CASCADE)"
    );
    assert_eq!(h.count("messages"), 0, "the table itself is empty");
    assert_eq!(h.count("native_messages"), 0);
}

/// `foreign_keys=ON` is not decorative: a transcript row whose session does not
/// exist is a hard error. This is the constraint `ensure_session_row` exists to
/// satisfy, and the proof the pragma took (SQLite's default is OFF).
#[test]
fn the_foreign_key_rejects_an_orphan_transcript_row() {
    let h = Harness::new();
    let seam = h.seam();
    let err = seam
        .insert_native_message("ghost", 1, "user", "{}")
        .expect_err("a session_id with no sessions row must be refused by the FK");
    let text = err.to_string().to_lowercase();
    assert!(
        text.contains("foreign key") || text.contains("constraint"),
        "the failure must name the constraint, got: {text}"
    );
}

/// A second session's rows are NOT collateral damage of the cascade.
#[test]
fn the_cascade_is_scoped_to_the_deleted_session() {
    let h = Harness::new();
    let seam = h.seam();
    let persister = TranscriptPersister::new(Arc::clone(&seam));
    persister.set_cwd("a", "/tmp/a");
    persister.set_cwd("b", "/tmp/b");

    persister.apply(false, &display_frame("a", "k"));
    persister.apply(false, &display_frame("b", "k"));
    persister.apply(false, &insert_frame("a", 1));
    persister.apply(false, &insert_frame("b", 1));

    h.db.delete_session("a").expect("delete a");

    assert_eq!(seam.messages_for("b").expect("b messages").len(), 1);
    assert_eq!(seam.load_native_messages("b").expect("b native").len(), 1);
    assert_eq!(h.count("messages"), 1);
    assert_eq!(h.count("native_messages"), 1);
}

// ── Opening ───────────────────────────────────────────────────────────────────

/// The mode bits of a path, unix.
///
/// `metadata().permissions().mode()` is unix-only, which is why this sits behind
/// a `cfg` rather than in the assertion: the Windows leg of the suite gets the
/// equivalent claim through the ACL note in `transport::windows`, and a test that
/// simply did not compile over there would leave the claim unclaimed in the
/// places it is hardest to check.
#[cfg(unix)]
fn mode_of(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
        .permissions()
        .mode()
        & 0o7777
}

/// The transcript database is owner-only (finding C).
///
/// `fulcrum.db` holds `native_messages.content_json` — the FULL conversation
/// transcript — plus `sessions.cwd` and `sessions.cost_usd`. Authentication is
/// legitimately deferred to T13, but a file mode is not authentication: it is the
/// difference between "any user on this machine can read your transcripts" and
/// "the user who owns the file can". There is no version of this file that makes
/// 0644 acceptable.
///
/// `rusqlite`'s `Connection::open` creates the file with the process umask, so
/// the default is 0644 — world-readable — and no amount of "the socket is private"
/// reasoning reaches it: the socket guards who may START a session, not who may
/// READ the ones already written.
#[cfg(unix)]
#[test]
fn the_database_file_is_owner_readable_only() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("fulcrum.db");
    let _db = Db::open(&db_path).expect("open the daemon store");

    let mode = mode_of(&db_path);
    assert_eq!(
        mode,
        0o600,
        "{} is mode {mode:04o}: the transcript store must be 0600. Anything with \
         a group or world bit readable exposes every conversation the daemon has \
         persisted (native_messages.content_json), and this failure is exactly the \
         0644 that was observed in a real data dir.",
        db_path.display()
    );
}

/// The directory holding the store is owner-only too.
///
/// The file mode alone is not enough: a 0600 file inside a world-readable,
/// world-writable directory can be unlinked and replaced by another local user,
/// and `fulcrum.db-wal` / `-shm` are created by SQLite itself with their own
/// modes, so the directory is the layer that actually bounds the pair.
#[cfg(unix)]
#[test]
fn the_database_directory_is_owner_only() {
    let dir = tempfile::tempdir().expect("temp dir");
    // A nested parent, because `tempdir()`'s own directory is 0700 already and
    // asserting on it would pass without the daemon having done anything.
    let nested = dir.path().join("fulcrum").join("fulcrum.db");
    let _db = Db::open(&nested).expect("open must create the parent dirs");

    let mode = mode_of(nested.parent().expect("a parent"));
    assert_eq!(
        mode, 0o700,
        "the store's directory is mode {mode:04o}; it must be 0700 so the WAL and \
         SHM siblings cannot be read or swapped by another local user either"
    );
}

/// The mode holds on a file that ALREADY existed with the wrong one.
///
/// A fix that only sets the mode when creating would leave every existing
/// installation world-readable forever — which is the population that has real
/// transcripts in the file. Opening is the only moment the daemon gets to fix it.
#[cfg(unix)]
#[test]
fn opening_an_existing_world_readable_database_narrows_its_mode() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("fulcrum.db");
    std::fs::write(&db_path, []).expect("create the file");
    std::fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o644))
        .expect("widen the mode to the broken one");
    assert_eq!(mode_of(&db_path), 0o644, "set up the broken state");

    let _db = Db::open(&db_path).expect("open the pre-existing store");
    assert_eq!(
        mode_of(&db_path),
        0o600,
        "an upgrade must narrow a store it inherited at 0644, or the fix helps \
         only fresh installs"
    );
}

#[test]
fn open_creates_the_parent_directory() {
    let dir = tempfile::tempdir().expect("temp dir");
    let nested = dir.path().join("nested").join("deeper").join("fulcrum.db");
    let db = Db::open(&nested).expect("open must create the parent dirs");
    assert!(nested.exists());
    assert_eq!(db.journal_mode().expect("mode").to_lowercase(), "wal");
}

/// The shipped schema-shape pin equals the shapes this build pins — the check that
/// reddens the moment a daemon DDL change lands without the matching pin update.
///
/// `schema-sha.txt` is the artifact the desktop compares its own DDL against, so
/// it must not be allowed to become a hopeful transcription of a DDL that has
/// since moved. The canonical form strips the file's comment lines (which carry
/// the contract's prose, not its data) and any blank lines, trims trailing
/// whitespace, and re-terminates every line — the same normalisation
/// `scripts/check-schema-drift.sh` applies, so `cargo test` and CI ask one
/// question in two places rather than two subtly different ones.
///
/// The path is built from `CARGO_MANIFEST_DIR` (which for this member is
/// `<repo-root>/fulcrum`), never the process CWD — an integration test's CWD is
/// not guaranteed to be the manifest dir, and a level-counting `.parent()` is the
/// pattern `AGENTS.md` forbids.
#[test]
fn the_pinned_schema_fingerprint_matches_the_shipped_file() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("schema-sha.txt");
    let file = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read the repo-root schema-sha.txt ({path:?}): {e}"));
    let canonical: String = file
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(|line| format!("{}\n", line.trim_end()))
        .collect();
    assert_eq!(
        canonical,
        schema_shape_fingerprint(),
        "`schema-sha.txt` no longer matches the shapes `TABLE_SHAPES` pins — update \
         the pin in the SAME commit as the DDL change, or this repo has drifted from \
         its own contract"
    );
}

/// The pin names all five keys — the vacuity floor on the fingerprint itself.
///
/// A pin that dropped a section would compare equal to a DDL that dropped the
/// same one, which is a check that passes because it stopped looking. So the five
/// headers are pinned here independently of the body comparison above: the file
/// must name `sessions`, `messages`, `native_messages`, `spaces` and
/// `idx_messages_session`, each exactly once, and no other header line may exist.
#[test]
fn the_schema_fingerprint_covers_all_four_tables_and_the_index() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("schema-sha.txt");
    let file = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read the repo-root schema-sha.txt ({path:?}): {e}"));

    const WANT: [&str; 5] = [
        "sessions",
        "messages",
        "native_messages",
        "spaces",
        "idx_messages_session",
    ];

    // A header line is a line `schema_shape_fingerprint()` would emit as a bare
    // name: no `:` (so not a `name:type` column line) and not the index
    // statement (the only other payload line the pin carries).
    let mut headers: Vec<String> = Vec::new();
    for line in file.lines() {
        let line = line.trim_end();
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        if !line.contains(':') && !line.starts_with("CREATE INDEX ") {
            headers.push(line.to_string());
        }
    }

    for key in WANT {
        let hits = headers.iter().filter(|h| h == &&key.to_string()).count();
        assert_eq!(
            hits, 1,
            "the pin must name `{key}` exactly once — a fingerprint that cannot \
             name every table is not a comparison"
        );
    }
    let mut sorted = headers.clone();
    sorted.sort();
    let mut want = WANT.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    want.sort();
    assert_eq!(
        sorted, want,
        "the pin names {headers:?}; it must name exactly {WANT:?} — an extra \
         header means the pin describes a table this build does not ship, and a \
         missing one means a table escaped the contract"
    );
}

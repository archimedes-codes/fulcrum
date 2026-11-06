//! T9 Task 4: the daemon's `fulcrum.db` — WAL, the desktop's four tables, and
//! `impl FulcrumPersistence for Db`.
//!
//! Every test here asserts a DATABASE property, for the reason `persist.rs`
//! records for its own relocated tests: a double that mimics a constraint proves
//! nothing about it. So these drive the real `rusqlite` connection in a temp dir.

use std::path::Path;
use std::sync::Arc;

use fulcrum::db::schema::{SCHEMA, SCHEMA_VERSION};
use fulcrum::db::Db;
use fulcrum_core::persist::TranscriptPersister;
use fulcrum_core::seams::FulcrumPersistence;
use fulcrum_core::store_rows::{DisplayRow, SessionInfo};
use fulcrum_core::worker_types::StoreFrame;

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

#[test]
fn open_creates_the_parent_directory() {
    let dir = tempfile::tempdir().expect("temp dir");
    let nested = dir.path().join("nested").join("deeper").join("fulcrum.db");
    let db = Db::open(&nested).expect("open must create the parent dirs");
    assert!(nested.exists());
    assert_eq!(db.journal_mode().expect("mode").to_lowercase(), "wal");
}

//! The daemon's schema + migrations (T9 Task 4).
//!
//! # Where this DDL comes from, and why it is verbatim
//!
//! These four tables are the DESKTOP's. They are transcribed from
//! `archimedes-desktop/src-tauri/src/storage/db.rs:1565-1604` (verified against
//! that tree at `4f9b99dc`), column for column, constraint for constraint. The
//! daemon opens a DIFFERENT file (`fulcrum.db`, not `archimedes.db`), so it is
//! free of the desktop's migration chain — but it is NOT free of its shape: the
//! day this store's `sessions` row stops being readable as the desktop's
//! `SessionRow`, the "the daemon owns the same disk the desktop owns" premise of
//! ADR 0091 quietly stops being true. So the DDL is copied, not re-derived.
//!
//! Two things here are deliberately load-bearing and must not be "simplified":
//!
//! - **The `UNIQUE` constraints.** `fulcrum_core::persist::TranscriptPersister`
//!   is idempotent because `INSERT … ON CONFLICT` has something to conflict
//!   with. `UNIQUE(session_id, kind, message_key)` is the display upsert key and
//!   `UNIQUE(session_id, seq)` is the transcript upsert key. Drop either and a
//!   replayed frame silently becomes a duplicate row — and a store that stores
//!   the transcript twice is a corrupted context, not merely a bigger file.
//! - **`ON DELETE CASCADE`** on both transcript tables. The daemon's
//!   `delete_session` issues ONE `DELETE FROM sessions`; the cascade is what
//!   takes the rows with it. (`foreign_keys` is OFF by default in SQLite, so
//!   `Db::open` turns it on and asserts it took — see `super::Db::open`.)
//!
//! # What is NOT here
//!
//! The desktop's migration chain (the `agent_id` rip-out, the `archived` /
//! `context_usage_json` / `todos_json` / `cost_usd` / `trusted` column adds, the
//! `native_messages` FK rebuild). Every one of those exists because a user's
//! `archimedes.db` predates the column. `fulcrum.db` is born with them, so
//! porting the chain would be porting the bug history of a file this daemon does
//! not own.

use rusqlite::Connection;

use super::DbError;

/// The schema version this build writes, stamped into `PRAGMA user_version`.
///
/// `1` = "the four tables, as [`SCHEMA`] spells them". A fresh database is
/// migrated to this; a database already at this version is left alone; a
/// database at a HIGHER version is refused (see [`migrate`]).
pub const SCHEMA_VERSION: i64 = 1;

/// The four tables, verbatim from the desktop (see the module docs).
///
/// `IF NOT EXISTS` throughout, so [`SCHEMA`] is safe to hand to
/// `execute_batch` on a file that already has them — the property the migration
/// slot relies on.
pub const SCHEMA: &str = r#"
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
"#;

/// Bring a database from `from` (its stored `user_version`) to
/// [`SCHEMA_VERSION`], and report the version it reached.
///
/// ONE slot, and the shape is deliberately small: slot 1 builds the four tables
/// from [`SCHEMA`] and stamps the version. Each future slot is one more `if`
/// branch here — a step that runs only when the file is at the version below it,
/// in a transaction of its own, and never renumbering anything the user placed.
///
/// Two properties worth naming, because both are the reason a slot exists at all:
///
/// - **Every step is idempotent by construction.** The version is read ONCE
///   before the loop and written once per step, so a crash between a step's
///   commit and its stamp re-runs a step that is safe to re-run
///   (`IF NOT EXISTS` / a guarded `ALTER`), rather than skipping one that was
///   half-applied.
/// - **A NEWER file is refused, not opened.** `from > SCHEMA_VERSION` is an
///   error, not a shrug. A daemon built against an older schema that opens a
///   database a newer daemon already migrated would write rows the newer one
///   cannot read; "I refuse to open this" is the only honest answer, and it is
///   cheap. Without the version slot, that failure is silent corruption.
///
/// SQLite DDL is transactional, so each step's DDL + stamp commit together.
pub fn migrate(conn: &mut Connection, from: i64) -> Result<i64, DbError> {
    if from > SCHEMA_VERSION {
        return Err(DbError::SchemaTooNew {
            found: from,
            supported: SCHEMA_VERSION,
        });
    }

    if from < 1 {
        // Slot 1: the four tables, and nothing else. One transaction — a crash
        // cannot leave a table half-created and stamped as migrated.
        let tx = conn.transaction()?;
        tx.execute_batch(SCHEMA)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
    }

    Ok(SCHEMA_VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The DDL text is the desktop's, so its shape is asserted here rather than
    /// eyeballed: four tables, the index, both upsert keys, both cascades.
    #[test]
    fn schema_text_holds_the_desktops_shape() {
        assert_eq!(SCHEMA.matches("CREATE TABLE").count(), 4);
        assert_eq!(SCHEMA.matches("ON DELETE CASCADE").count(), 2);
        assert!(SCHEMA.contains("UNIQUE(session_id, kind, message_key)"));
        assert!(SCHEMA.contains("UNIQUE(session_id, seq)"));
        assert!(SCHEMA.contains("CREATE INDEX IF NOT EXISTS idx_messages_session"));
        // `cost_usd` has NO default on purpose: the desktop's docs are explicit
        // that a column default would collapse "no figure" into `Some(0.0)`,
        // which is the D5 defect ADR 0082 exists to avoid.
        assert!(SCHEMA.contains("cost_usd REAL,"));
        assert!(!SCHEMA.contains("cost_usd REAL NOT NULL"));
    }
}

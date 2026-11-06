//! `impl FulcrumPersistence for Db` (T9 Task 4) — the daemon takes the disk.
//!
//! This is the point where ADR 0091 changes hands. `fulcrum-core` holds ZERO
//! `rusqlite` (`boundary_pins::no_code_names_the_sqlite_driver_or_the_desktop_package`
//! forbids it, and the engine's `Cargo.toml` declares none), so the engine's
//! persistence seam has always had exactly one implementation living in the other
//! repo — the desktop's `storage::Db`. Here is the SECOND one, and it is legal
//! precisely because it is not in the engine: the daemon is the thing that owns
//! `fulcrum.db`.
//!
//! ## Why every body is a delegation, not SQL
//!
//! The SQL lives in [`super::Db`]'s inherent methods, which are themselves
//! transcriptions of the desktop's. Each method below is one qualified call plus
//! the [`DbError`] → [`StoreError`] map, so the transaction boundaries, the
//! `QueryReturnedNoRows` arms and the deliberate silent no-ops all stay in ONE
//! place. That is what makes the seam behaviour-identical to the desktop's rather
//! than merely similar.
//!
//! The calls are written `Db::method(self, …)` rather than `self.method(…)`. Rust
//! prefers inherent methods over trait methods, so `self.method()` would work —
//! but the qualified spelling means a future edit that renames a trait method
//! cannot quietly make the body recurse into the trait object instead of reaching
//! the SQL.
//!
//! ## The error map is a loss, and that is inherited
//!
//! [`StoreError`] has two variants (`Io`, `Json`) and no `Sqlite`; the engine's
//! own docs record why collapsing it is admissible — the only crate-side consumer
//! of a persistence error is `TranscriptPersister`, and it does
//! `eprintln!("{e}")` and continues, so nothing branches on the variant. This
//! impl follows the desktop's map (`storage/db.rs:74-90`): SQLite failures
//! stringify THROUGH [`DbError`]'s `Display`, so the message keeps its
//! `sqlite error: ` prefix and degrades to `io error: sqlite error: …` rather
//! than dropping which layer failed. If a daemon-side consumer ever needs to
//! distinguish a locked database, the honest place to add that is a variant here,
//! not a wider `From` impl.

use std::io;

use fulcrum_core::seams::{FulcrumPersistence, StoreError};
use fulcrum_core::store_rows::{MessageRow, SessionInfo, SessionRow};

use super::{Db, DbError};

/// The [`DbError`] → [`StoreError`] map, mirroring the desktop's.
///
/// `Sqlite` and the two daemon-only variants (`NotWal`, `SchemaTooNew`) go
/// through `io::Error::other`, the constructor for "an error that is not an OS
/// errno", preserving the wrapped value's `Display`.
impl From<DbError> for StoreError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::Sqlite(inner) => {
                StoreError::Io(io::Error::other(format!("sqlite error: {inner}")))
            }
            DbError::NotWal(mode) => StoreError::Io(io::Error::other(mode)),
            DbError::SchemaTooNew { found, supported } => {
                StoreError::Io(io::Error::other(format!(
                "database schema version {found} is newer than this build supports ({supported})"
            )))
            }
            DbError::Io(e) => StoreError::Io(e),
            DbError::Json(e) => StoreError::Json(e),
        }
    }
}

impl FulcrumPersistence for Db {
    fn insert_native_message(
        &self,
        session_id: &str,
        seq: i64,
        role: &str,
        content_json: &str,
    ) -> Result<(), StoreError> {
        Db::insert_native_message(self, session_id, seq, role, content_json)?;
        Ok(())
    }

    fn replace_native_messages(
        &self,
        session_id: &str,
        rows: &[(i64, String, String)],
    ) -> Result<(), StoreError> {
        Db::replace_native_messages(self, session_id, rows)?;
        Ok(())
    }

    fn record_session_context_usage(
        &self,
        session_id: &str,
        used: u64,
        window: u64,
    ) -> Result<(), StoreError> {
        Db::record_session_context_usage(self, session_id, used, window)?;
        Ok(())
    }

    fn record_session_cost(&self, session_id: &str, total: f64) -> Result<(), StoreError> {
        Db::record_session_cost(self, session_id, total)?;
        Ok(())
    }

    fn record_session_todos(
        &self,
        session_id: &str,
        todos: Option<&str>,
    ) -> Result<(), StoreError> {
        Db::record_session_todos(self, session_id, todos)?;
        Ok(())
    }

    fn session(&self, session_id: &str) -> Result<Option<SessionRow>, StoreError> {
        Ok(Db::session(self, session_id)?)
    }

    fn load_native_messages(&self, session_id: &str) -> Result<Vec<String>, StoreError> {
        Ok(Db::load_native_messages(self, session_id)?)
    }

    fn messages_for(&self, session_id: &str) -> Result<Vec<MessageRow>, StoreError> {
        Ok(Db::messages_for(self, session_id)?)
    }

    fn session_cost(&self, session_id: &str) -> Result<Option<f64>, StoreError> {
        Ok(Db::session_cost(self, session_id)?)
    }

    fn session_todos(&self, session_id: &str) -> Result<Option<String>, StoreError> {
        Ok(Db::session_todos(self, session_id)?)
    }

    fn record_session(&self, info: &SessionInfo) -> Result<(), StoreError> {
        Db::record_session(self, info)?;
        Ok(())
    }

    fn list_sessions(&self, include_archived: bool) -> Result<Vec<SessionRow>, StoreError> {
        Ok(Db::list_sessions(self, include_archived)?)
    }

    fn set_space_trusted(&self, path: &str, trusted: bool) -> Result<bool, StoreError> {
        Ok(Db::set_space_trusted(self, path, trusted)?)
    }

    fn upsert_space(&self, path: &str, default_trusted: bool) -> Result<(), StoreError> {
        Db::upsert_space(self, path, default_trusted)?;
        Ok(())
    }

    fn record_message(
        &self,
        session_id: &str,
        kind: &str,
        message_key: Option<&str>,
        payload_json: &str,
    ) -> Result<(), StoreError> {
        Db::record_message(self, session_id, kind, message_key, payload_json)?;
        Ok(())
    }

    /// The clock, delegating to the engine's ONE definition.
    ///
    /// Not a shortcut: `now_ms` takes `&self` for dynamism, not because a `Db`
    /// carries a clock — a SQLite connection has no notion of time. A
    /// per-implementation clock would be strictly worse, since two
    /// `FulcrumPersistence` impls could then disagree about what time it is and
    /// the transcript's order would depend on which store answered. `src/clock.rs`
    /// exists precisely to be the only `SystemTime` call in the picture.
    fn now_ms(&self) -> i64 {
        fulcrum_core::clock::now_ms()
    }
}

/// The seam's surface is pinned at compile time by this function: it names every
/// method through `dyn FulcrumPersistence`, so a method added to the engine's
/// trait without an impl here fails to build, and one REMOVED from the trait fails
/// too. A runtime test cannot see either — a missing trait method is not a wrong
/// value, it is a type error.
#[allow(dead_code)]
fn seam_surface_is_complete(db: &dyn FulcrumPersistence) -> Result<(), StoreError> {
    db.insert_native_message("s", 0, "user", "{}")?;
    db.replace_native_messages("s", &[])?;
    db.record_session_context_usage("s", 0, 0)?;
    db.record_session_cost("s", 0.0)?;
    db.record_session_todos("s", None)?;
    db.record_session(&SessionInfo {
        session_id: "s".to_string(),
        cwd: std::path::PathBuf::from("/tmp"),
        capabilities: serde_json::Value::Null,
        config_options: None,
        archived: false,
        title: None,
        context_usage: None,
        cost_usd: None,
        is_subagent: false,
    })?;
    let _ = db.session("s")?;
    let _ = db.load_native_messages("s")?;
    let _ = db.messages_for("s")?;
    let _ = db.session_cost("s")?;
    let _ = db.session_todos("s")?;
    let _ = db.list_sessions(false)?;
    let _ = db.set_space_trusted("p", true)?;
    db.upsert_space("p", false)?;
    db.record_message("s", "user", None, "{}")?;
    let _ = db.now_ms();
    Ok(())
}

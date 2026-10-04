//! The Supervisor-side transcript persister (ADR 0025 Task 4): the
//! `TranscriptPersister` applies the Worker's store frames to SQLite
//! (the Supervisor is the SOLE writer — the Worker has no DB).
//!
//! The store frames are the `Store` seam over IPC (Task 2's
//! `IpcStore` frames every store call): `TranscriptInsert` /
//! `TranscriptReplace` / `DisplayUpsert` / `ContextUsage`. The raw
//! `RpcEvent` stream does NOT drive persistence (it drives the UI
//! bookkeeping) — persistence comes from the store frames, so the
//! transcript the Supervisor persists is byte-for-byte what the loop
//! persists today, just transported.
//!
//! Idempotency: every apply is an UPSERT on the existing `Db` methods
//! (`(session_id, seq)` for `native_messages`, `(session_id, kind,
//! message_key)` for `messages`), so a replayed frame never duplicates
//! a row.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::Value;

use crate::agent::worker::protocol::StoreFrame;
use crate::agent::SessionInfo;
use crate::storage::Db;

/// The Supervisor's sole-writer transcript persister (ADR 0025). Applies
/// the Worker's store frames to SQLite (idempotent upserts) and ensures
/// the `sessions` row exists for ephemeral subagent sessions (their
/// `native_messages` rows FK-reference `sessions(id)`).
pub struct TranscriptPersister {
    db: Arc<Db>,
    /// The `session_id` → `cwd` map (the `ensure_session_row` source —
    /// the `StoreFrame` carries no `cwd`). Populated by the
    /// `SessionManager` (main sessions) and the `WorkerManager`'s
    /// `dispatch_subagent` flow (subagent sessions).
    cwd_map: Arc<StdMutex<HashMap<String, String>>>,
}

impl TranscriptPersister {
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            cwd_map: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Record a session's `cwd` (the `ensure_session_row` source — the
    /// `StoreFrame` carries no `cwd`). Called by the `SessionManager`
    /// (main sessions, at `start`/`resume`) and the `WorkerManager`'s
    /// `dispatch_subagent` flow (subagent sessions).
    pub fn set_cwd(&self, session_id: &str, cwd: &str) {
        self.cwd_map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(session_id.to_string(), cwd.to_string());
    }

    /// Apply one store frame (idempotent — a replayed frame must not
    /// duplicate rows; the upsert semantics are the existing `Db`
    /// methods). `is_subagent` threads the ephemeral-row flag (the
    /// `WorkerManager` knows it per attached Worker: main `false`,
    /// subagent `true`).
    pub fn apply(&self, is_subagent: bool, frame: &StoreFrame) {
        // The frame's OWN session id (the `ensure_session_row` target —
        // for a SUBAGENT frame the outer `session_id` is the PARENT's
        // delivery id (the `WorkerManager`'s pump re-targets the frame
        // to the parent for the router / the UI), but the row belongs
        // to the CHILD (the frame's `sid` — the ephemeral `sessions`
        // row must be created for the CHILD, not the parent — the
        // `native_messages` FK is on the child's id).
        let sid = match frame {
            StoreFrame::Insert { session_id, .. }
            | StoreFrame::Replace { session_id, .. }
            | StoreFrame::Display { session_id, .. }
            | StoreFrame::ContextUsage { session_id, .. } => session_id,
        };
        // Ephemeral subagent rows (the spec's §3 — subagent transcripts
        // persist as hidden rows): ensure the `sessions` row exists
        // BEFORE the `native_messages` insert (the FK).
        self.ensure_session_row(sid, is_subagent);
        match frame {
            StoreFrame::Insert {
                session_id: sid,
                seq,
                role,
                content_json,
            } => {
                // The `native_messages` upsert (the existing idempotent
                // `(session_id, seq)` collapse).
                if let Err(e) = self
                    .db
                    .insert_native_message(sid, *seq as i64, role, content_json)
                {
                    eprintln!("transcript insert: {e}");
                }
            }
            StoreFrame::Replace {
                session_id: sid,
                rows,
            } => {
                // The compaction rewrite (the existing ATOMIC
                // single-transaction method). The `StoreFrame` seq is
                // `u64`; the `Db` method takes `i64` (the transcript seq
                // never exceeds `i64::MAX`).
                let rows: Vec<(i64, String, String)> = rows
                    .iter()
                    .map(|(seq, role, content_json)| {
                        (*seq as i64, role.clone(), content_json.clone())
                    })
                    .collect();
                if let Err(e) = self.db.replace_native_messages(sid, &rows) {
                    eprintln!("transcript replace: {e}");
                }
            }
            StoreFrame::Display {
                session_id: sid,
                rows,
            } => {
                // The `messages` upserts (the existing `(session_id,
                // kind, message_key)` upsert keys).
                for row in rows {
                    let payload =
                        serde_json::to_string(&row.payload_json).unwrap_or_else(|_| "null".into());
                    if let Err(e) =
                        self.db
                            .record_message(sid, &row.kind, Some(&row.message_key), &payload)
                    {
                        eprintln!("display upsert: {e}");
                    }
                }
            }
            StoreFrame::ContextUsage {
                session_id: sid,
                used,
                window,
            } => {
                // The `sessions.context_usage_json` write (the existing
                // method).
                if let Err(e) = self.db.record_session_context_usage(sid, *used, *window) {
                    eprintln!("context usage: {e}");
                }
            }
        }
    }

    /// Ensure the `sessions` row exists (the `native_messages` FK). On
    /// the FIRST frame for an unknown session id, `record_session` with
    /// `SessionInfo { …, is_subagent: <the flag>, archived: false, … }`.
    /// A main session's row was already recorded by the `SessionManager`
    /// (`start`/`resume`), so this is a no-op; a subagent's row is
    /// created here (the ephemeral hidden row).
    fn ensure_session_row(&self, session_id: &str, is_subagent: bool) {
        if self.db.session(session_id).ok().flatten().is_some() {
            return; // already exists.
        }
        let cwd = self
            .cwd_map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(session_id)
            .cloned()
            .unwrap_or_default();
        let info = SessionInfo {
            session_id: session_id.to_string(),
            cwd: PathBuf::from(&cwd),
            // Ephemeral subagent — the `capabilities` envelope is
            // `Value::Null`-ish (the current throwaway-DB path did not
            // set a meaningful envelope).
            capabilities: Value::Null,
            config_options: None,
            archived: false,
            context_usage: None,
            is_subagent,
        };
        if let Err(e) = self.db.record_session(&info) {
            eprintln!("ensure_session_row: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp-dir `Db` + a `sessions` row (the `native_messages` FK
    /// source).
    fn temp_db() -> (Arc<Db>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("persist-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Arc::new(Db::open(&dir.join("t.db")).unwrap());
        (db, dir)
    }

    /// `apply` (Insert) → a `native_messages` row (idempotent — a
    /// replayed frame collapses to one row).
    #[test]
    fn apply_insert_is_idempotent() {
        let (db, dir) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        p.set_cwd("s1", "/tmp/space");
        let frame = StoreFrame::Insert {
            session_id: "s1".to_string(),
            seq: 0,
            role: "system".to_string(),
            content_json: r#"{"role":"system"}"#.to_string(),
        };
        p.apply(false, &frame);
        p.apply(false, &frame); // replay — must not duplicate.
        let rows = db.load_native_messages("s1").unwrap();
        assert_eq!(rows.len(), 1, "a replayed insert collapses to one row");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `apply` (Display) → a `messages` row (idempotent upsert).
    #[test]
    fn apply_display_is_idempotent() {
        let (db, dir) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        p.set_cwd("s1", "/tmp/space");
        let frame = StoreFrame::Display {
            session_id: "s1".to_string(),
            rows: vec![crate::agent::harness::store::DisplayRow {
                kind: "agent-text".to_string(),
                message_key: "m1".to_string(),
                payload_json: serde_json::json!({ "text": "hi" }),
                created_at: 1,
            }],
        };
        p.apply(false, &frame);
        p.apply(false, &frame); // replay.
        let rows = db.messages_for("s1").unwrap();
        assert_eq!(
            rows.len(),
            1,
            "a replayed display upsert collapses to one row"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `apply` (ContextUsage) → the `sessions.context_usage_json` write.
    #[test]
    fn apply_context_usage_writes_the_session_row() {
        let (db, dir) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        p.set_cwd("s1", "/tmp/space");
        let frame = StoreFrame::ContextUsage {
            session_id: "s1".to_string(),
            used: 53_760,
            window: 128_000,
        };
        p.apply(false, &frame);
        let row = db.session("s1").unwrap().unwrap();
        let usage: serde_json::Value =
            serde_json::from_str(&row.context_usage_json.unwrap()).unwrap();
        assert_eq!(usage["used"], 53_760);
        assert_eq!(usage["window"], 128_000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `ensure_session_row` (subagent) → a HIDDEN `sessions` row
    /// (`is_subagent = 1` — absent from `list_sessions`) + the
    /// `native_messages` FK holds (the insert succeeds).
    #[test]
    fn ensure_session_row_creates_a_hidden_subagent_row() {
        let (db, dir) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        p.set_cwd("sub-1", "/tmp/space");
        let frame = StoreFrame::Insert {
            session_id: "sub-1".to_string(),
            seq: 0,
            role: "system".to_string(),
            content_json: r#"{"role":"system"}"#.to_string(),
        };
        p.apply(true, &frame);
        // The `sessions` row exists (the FK held).
        let row = db
            .session("sub-1")
            .unwrap()
            .expect("the subagent row exists");
        assert!(row.is_subagent, "the subagent row is flagged");
        // It is HIDDEN from `list_sessions`.
        let all = db.list_sessions(true).unwrap();
        assert!(
            all.iter().all(|r| r.id != "sub-1"),
            "the subagent row is hidden from list_sessions"
        );
        // Its transcript is loadable.
        assert_eq!(db.load_native_messages("sub-1").unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

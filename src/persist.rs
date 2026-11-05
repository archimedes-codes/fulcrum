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
//!
//! `cost_update` USED to be the one exception to that promise and no longer
//! is (Fulcrum T2, amending ADR 0082's limit 7). It was an INCREMENT
//! (`Db::record_session_cost` ADDED the frame's delta), so re-delivering one
//! frame DOUBLED the stored figure — accepted, not overlooked, and PINNED by
//! `a_replayed_cost_frame_doubles_the_stored_figure` (`db.rs`), whose own
//! docblock said it must be DELETED rather than adjusted the day the write
//! became idempotent. That day is this commit: the frame now carries the
//! session's CUMULATIVE TOTAL, `record_session_cost` OVERWRITES with it, and
//! the promise above is true for this arm too. Nothing was deduped — the wire
//! still hardcodes `"seq": 0` and a frame still carries no identity; a snapshot
//! simply does not need one. The pin's replacement is
//! `a_replayed_cost_frame_overwrites_to_the_same_figure`.
//!
//! What moved instead of being fixed is the ACCUMULATOR, and it moved to each
//! producer: `AgentLoop::cost_acc` (seeded from the stored row on `Resume`),
//! the subagent capture, and the parent roll-up — all three in this one commit,
//! because a snapshot store cannot tolerate a producer that still sends deltas.
//! The bound the old text leaned on ("no production call site replays history")
//! is now moot rather than load-bearing: replaying history is survivable, since
//! every frame restates the total.
//!
//! ONE PRODUCER OF THAT THREE NO LONGER EXISTS. The parent-keyed roll-up frame
//! in `finish_subagent` was deleted in the follow-up commit: with the frame a
//! snapshot and the write an overwrite, TWO writers on the parent's store key
//! make the last frame win, and the parent's accumulator had never seen its
//! child — so the parent's next frame erased the child's money. A child's cost
//! now folds into the PARENT's own `cost_acc` (`AgentLoop::fold_child_cost`,
//! fed by `harness::dispatch::dispatch_subagent`), so the parent's loop is the
//! single writer of the parent's key and #105's "a parent's stored total
//! includes its children" holds. What persists through THIS arm is unchanged:
//! one frame, one overwrite, keyed to whatever session the frame names.
//!
//! # Where this file's tests are (T5/S5b)
//!
//! All eight moved to `src-tauri/tests/persist_frames_db.rs`. They RELOCATED
//! rather than re-pointed at the crate's `MemoryStore` double because every one
//! of them asserts a DATABASE property — the `(session_id, seq)` and
//! `(session_id, kind, message_key)` UNIQUE collapses, the FK from
//! `native_messages` to `sessions` that `ensure_session_row` exists to satisfy,
//! and the `sessions` column reads (`session` / `session_cost` /
//! `session_todos`) — and a double that mimics a constraint proves nothing about
//! it. `#[cfg(test)]` does not survive into an rlib and the crate cannot name
//! `storage::Db`, so this is the only honest home for them. The persister under
//! test is still THIS code, driven through `impl FulcrumPersistence for Db`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::Value;

use crate::seams::FulcrumPersistence;
use crate::store_rows::SessionInfo;
use crate::worker_types::StoreFrame;

/// The Supervisor's sole-writer transcript persister (ADR 0025). Applies
/// the Worker's store frames to SQLite (idempotent upserts) and ensures
/// the `sessions` row exists for ephemeral subagent sessions (their
/// `native_messages` rows FK-reference `sessions(id)`).
pub struct TranscriptPersister {
    /// The persistence seam (T5/S5b). This held a concrete `Arc<Db>` and the
    /// seven calls below were inherent `Db` methods; every one of them is on
    /// [`FulcrumPersistence`]'s surface, so the field is now the trait object
    /// and the crate names no desktop type. The desktop's only construction
    /// site (`session/mod.rs`'s `TranscriptPersister::new(db)`, `db: Arc<Db>`)
    /// compiles UNCHANGED — `Arc<Db>` coerces, because `impl
    /// FulcrumPersistence for storage::Db` is the seam S4 landed and pinned.
    db: Arc<dyn FulcrumPersistence>,
    /// The `session_id` → `cwd` map (the `ensure_session_row` source —
    /// the `StoreFrame` carries no `cwd`). Populated by the
    /// `SessionManager` (main sessions) and the `WorkerManager`'s
    /// `dispatch_subagent` flow (subagent sessions).
    cwd_map: Arc<StdMutex<HashMap<String, String>>>,
}

impl TranscriptPersister {
    pub fn new(db: Arc<dyn FulcrumPersistence>) -> Self {
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

    /// Apply an interactive event payload (e.g. `todos_update`, `todos_clear`,
    /// `cost_update`) to SQLite (ADR 0073). The Supervisor is the sole writer.
    ///
    /// `is_subagent` exists ONLY for the `cost_update` arm's row-ensure (see
    /// there): the `todos_*` arms cannot create a row and never needed the
    /// flag.
    pub fn apply_interactive_event(
        &self,
        session_id: &str,
        is_subagent: bool,
        payload: &serde_json::Value,
    ) {
        let Some(event) = payload.get("event").and_then(serde_json::Value::as_str) else {
            return;
        };
        match event {
            "cost_update" => {
                // A non-number `costUsd` (string / null / object / absent) is
                // "no figure" and writes NOTHING — the same discipline the
                // child's capture holds (`a_child_that_reported_no_cost_reports_no_cost`:
                // *"a non-number `costUsd` must not become money"*). Coercing
                // one to `0.0` would render a phantom `$0.00` (D5 / ADR 0082)
                // and — since the write is now a SNAPSHOT — would also OVERWRITE
                // a session's real stored total with a zero, which is a worse
                // failure than the summing store this arm used to write into.
                let Some(cost) = payload
                    .get("payload")
                    .and_then(|p| p.get("costUsd"))
                    .and_then(Value::as_f64)
                else {
                    return;
                };
                // Trap 1: `record_session_cost` is a bare
                // `UPDATE … WHERE id = ?1`, and an `UPDATE` matching no row is
                // a SILENT `Ok(())` — so the row must exist BEFORE the write.
                // A subagent's row is created lazily here, and its first
                // priced call can arrive before any of its `StoreFrame`s, so
                // without this the child's first frame vanishes. Pinned by
                // `a_cost_frame_for_a_session_with_no_row_yet_still_persists`.
                self.ensure_session_row(session_id, is_subagent);
                // Deliberately NO `!= 0.0` filter: a real zero is a figure and
                // is exactly what turns the column's `NULL` into `Some(0.0)`.
                if let Err(e) = self.db.record_session_cost(session_id, cost) {
                    eprintln!("persist cost_update: {e}");
                }
            }
            "todos_update" => {
                let todos = payload
                    .get("payload")
                    .and_then(|p| p.get("todos"))
                    .filter(|t| t.is_array());
                if let Some(todos) = todos {
                    let serialized = match serde_json::to_string(todos) {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("serialize todos: {e}");
                            return;
                        }
                    };
                    if let Err(e) = self.db.record_session_todos(session_id, Some(&serialized)) {
                        eprintln!("persist todos_update: {e}");
                    }
                }
            }
            "todos_clear" => {
                if let Err(e) = self.db.record_session_todos(session_id, None) {
                    eprintln!("persist todos_clear: {e}");
                }
            }
            _ => {}
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
            // Never titled: a subagent row is hidden from `list_sessions`
            // (the naming pass never sees it), and `record_session` does not
            // write `title` anyway.
            title: None,
            context_usage: None,
            // A row created HERE is brand new — nothing has reported a cost
            // yet, and `record_session`'s conflict arm never touches
            // `cost_usd`, so this `None` can only ever land on a fresh row.
            cost_usd: None,
            is_subagent,
        };
        if let Err(e) = self.db.record_session(&info) {
            eprintln!("ensure_session_row: {e}");
        }
    }
}

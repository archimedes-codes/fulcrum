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
//! THE ONE EXCEPTION: `cost_update`. That arm is not an upsert but an
//! INCREMENT (`Db::record_session_cost` ADDS the frame's delta to the
//! stored figure), because cost is a per-call DELTA and every other
//! persisted field here is a whole-state snapshot that can be
//! overwritten. So the promise above is FALSE for it: re-delivering one
//! `cost_update` DOUBLES the stored figure. The wire gives frames no
//! identity to dedupe on — `emit_cost_update` hardcodes `"seq": 0` — so
//! there is nothing to key an idempotent write on. That is accepted, not
//! overlooked: the doubling is PINNED rather than prevented, by
//! `a_replayed_cost_frame_doubles_the_stored_figure` (`db.rs`), which
//! must be DELETED rather than adjusted if a future change ever makes
//! the write idempotent. The exposure is bounded by `emit_cost_update`'s
//! production call sites — today `turn.rs`'s `Usage` arm and `loop.rs`'s
//! `run_compaction` (the compaction summary's own call), neither of which
//! is a Resume-replay path — so a `Resume` cannot re-emit history.
//! Deliberately not stated as a count: the `CONTEXT.md` Cost channel
//! entry's rule is that a count here goes stale the moment a third emitter
//! lands (the stale claim is issue #110's defect class); the bound that
//! matters is that no call site replays history, which neither site does.

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
                // one to `0.0` would both render a phantom `$0.00` (D5 / ADR
                // 0082) and corrupt the accumulator, which sums deltas.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A temp-dir `Db` + a `sessions` row (the `native_messages` FK
    /// source), OWNED (#96 task 08). Guard FIRST: destructured locals drop in
    /// reverse, so the `Db` — whose SQLite file lives INSIDE `dir` — closes
    /// before `dir` is removed. Windows needs that; Linux cannot show it
    /// (POSIX unlinks under an open handle and `ScratchDir::Drop` swallows the
    /// failure), so this is enforced by reading. Matches the two `temp_db`
    /// siblings tasks 03/06 converted.
    ///
    /// HONEST SIZING: all five callers already ran `remove_dir_all(&dir)` on the
    /// green path, so this file's census delta is 0 BEFORE and 0 AFTER. The win
    /// is the panic path — five dirs that survive a failed assertion today.
    fn temp_db() -> (crate::test_support::ScratchDir, Arc<Db>) {
        let dir = crate::test_support::ScratchDir::in_env_temp("persist-test-", "db");
        let db = Arc::new(Db::open(&dir.join("t.db")).unwrap());
        (dir, db)
    }

    /// `apply` (Insert) → a `native_messages` row (idempotent — a
    /// replayed frame collapses to one row).
    #[test]
    fn apply_insert_is_idempotent() {
        let (_dir, db) = temp_db();
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
    }

    /// `apply` (Display) → a `messages` row (idempotent upsert).
    #[test]
    fn apply_display_is_idempotent() {
        let (_dir, db) = temp_db();
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
    }

    /// `apply` (ContextUsage) → the `sessions.context_usage_json` write.
    #[test]
    fn apply_context_usage_writes_the_session_row() {
        let (_dir, db) = temp_db();
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
    }

    /// `ensure_session_row` (subagent) → a HIDDEN `sessions` row
    /// (`is_subagent = 1` — absent from `list_sessions`) + the
    /// `native_messages` FK holds (the insert succeeds).
    #[test]
    fn ensure_session_row_creates_a_hidden_subagent_row() {
        let (_dir, db) = temp_db();
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
    }

    /// Trap 1's pin: a `cost_update` for a session with **no `sessions` row
    /// and no prior `StoreFrame`** must STILL persist.
    ///
    /// `record_session_cost` is a bare `UPDATE … WHERE id = ?1`, and an
    /// `UPDATE` that matches no row is `Ok(())` with nothing written
    /// (`record_session_cost_on_a_missing_row_is_a_silent_no_op` in `db.rs`).
    /// `apply_interactive_event` therefore has to `ensure_session_row` BEFORE
    /// the write — which is the whole reason it takes `is_subagent`: a
    /// subagent's row is created lazily by the persister, and its FIRST priced
    /// call can easily arrive before any of its `StoreFrame`s. Without the
    /// ensure, that first frame is silently discarded and the child's stored
    /// total starts out short of one call.
    #[test]
    fn a_cost_frame_for_a_session_with_no_row_yet_still_persists() {
        let (_dir, db) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        // The `cwd_map` entry `ensure_session_row` reads (the `StoreFrame`
        // carries no `cwd`) — the same setup
        // `ensure_session_row_creates_a_hidden_subagent_row` uses.
        p.set_cwd("sub-cold", "/tmp/space");
        // NO `record_session`, NO `apply(StoreFrame)`: the row does not exist.
        assert!(db.session("sub-cold").unwrap().is_none());

        p.apply_interactive_event(
            "sub-cold",
            true,
            &serde_json::json!({
                "sessionId": "sub-cold",
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": 0.0000341, "source": "subagent" },
            }),
        );

        // The row exists, flagged as the ephemeral subagent row…
        let row = db
            .session("sub-cold")
            .unwrap()
            .expect("the cost arm must ensure the row before writing (Trap 1)");
        assert!(row.is_subagent, "the flag must reach the created row");
        // …and the figure landed in it rather than vanishing.
        assert_eq!(db.session_cost("sub-cold").unwrap(), Some(0.0000341));
    }

    /// A `costUsd` that is not a number writes NOTHING — the same discipline
    /// the child's capture already holds (`a_child_that_reported_no_cost_reports_no_cost`:
    /// *"a non-number `costUsd` must not become money"*). A string, a `null`,
    /// an object, and an absent key are four spellings of "no figure", and the
    /// column must stay `NULL` for all four: `NULL` renders nothing, whereas a
    /// `0.0` coerced out of `"0.5"` would render a phantom `$0.00` (D5 / ADR
    /// 0082) — and would additionally corrupt the accumulator, since a stored
    /// figure is a SUM of deltas.
    ///
    /// (Named `cost_usd` rather than the plan's `costUsd`: `non_snake_case` is
    /// a warn-by-way lint and this crate runs clippy at 0 warnings with no
    /// `allow(non_snake_case)` anywhere.)
    #[test]
    fn a_non_numeric_cost_usd_writes_nothing() {
        // The four spellings of "no figure", each as the frame's INNER
        // `payload` object. The fourth has no `costUsd` key at all — passing
        // `Value::Null` as a map VALUE would serialise an explicit `null` and
        // so only re-test the second case.
        let cases: [(&str, Value); 4] = [
            ("a string", json!({ "costUsd": "0.5", "source": "main" })),
            ("a null", json!({ "costUsd": null, "source": "main" })),
            ("an object", json!({ "costUsd": { "amount": 0.5 } })),
            ("an absent key", json!({ "source": "main" })),
        ];
        for (label, inner) in cases {
            let (_dir, db) = temp_db();
            let p = TranscriptPersister::new(db.clone());
            p.set_cwd("s1", "/tmp/space");
            p.apply_interactive_event(
                "s1",
                false,
                &serde_json::json!({
                    "sessionId": "s1",
                    "seq": 0,
                    "event": "cost_update",
                    "payload": inner,
                }),
            );
            assert_eq!(
                db.session_cost("s1").unwrap(),
                None,
                "a non-number `costUsd` ({label}: {inner}) must not become money"
            );
        }
    }

    /// A REAL zero is a figure: it must reach the row (turning `NULL` into
    /// `Some(0.0)`, which the renderer prints as `$0.00`). Deliberately NOT
    /// filtered by a `!= 0.0` guard — that guard is what this repo's own ADR
    /// 0082 lesson forbids.
    #[test]
    fn a_real_zero_cost_still_reaches_the_row() {
        let (_dir, db) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        p.set_cwd("s1", "/tmp/space");
        p.apply_interactive_event(
            "s1",
            false,
            &serde_json::json!({
                "sessionId": "s1",
                "seq": 0,
                "event": "cost_update",
                "payload": { "costUsd": 0.0, "source": "main" },
            }),
        );
        assert_eq!(
            db.session_cost("s1").unwrap(),
            Some(0.0),
            "a real zero is a figure, not an absence"
        );
    }

    #[test]
    fn apply_interactive_event_updates_and_clears_todos() {
        let (_dir, db) = temp_db();
        let p = TranscriptPersister::new(db.clone());
        let info = SessionInfo {
            session_id: "s1".to_string(),
            cwd: PathBuf::from("/tmp"),
            capabilities: Value::Null,
            config_options: None,
            archived: false,
            title: None,
            context_usage: None,
            cost_usd: None,
            is_subagent: false,
        };
        db.record_session(&info).unwrap();

        // (a) update todos
        let update_payload = serde_json::json!({
            "sessionId": "s1",
            "seq": 0,
            "event": "todos_update",
            "payload": {
                "source": "native",
                "todos": [
                    { "content": "t1", "status": "pending" }
                ]
            }
        });
        p.apply_interactive_event("s1", false, &update_payload);
        let stored = db
            .session_todos("s1")
            .unwrap()
            .expect("todos should be stored");
        assert!(stored.contains("t1"));

        // (b) call with second update -> replaced
        let update2_payload = serde_json::json!({
            "sessionId": "s1",
            "seq": 1,
            "event": "todos_update",
            "payload": {
                "source": "native",
                "todos": [
                    { "content": "t2", "status": "completed" }
                ]
            }
        });
        p.apply_interactive_event("s1", false, &update2_payload);
        let stored2 = db
            .session_todos("s1")
            .unwrap()
            .expect("todos should be stored");
        assert!(stored2.contains("t2"));
        assert!(!stored2.contains("t1"));

        // (c) call with event: "todos_clear" -> sets None
        let clear_payload = serde_json::json!({
            "sessionId": "s1",
            "seq": 2,
            "event": "todos_clear",
            "payload": {}
        });
        p.apply_interactive_event("s1", false, &clear_payload);
        assert_eq!(db.session_todos("s1").unwrap(), None);

        // (d) call with unrelated event (e.g. state) or bad payload -> does not crash or touch todos_json
        let unrelated = serde_json::json!({
            "sessionId": "s1",
            "event": "state",
            "payload": { "foo": "bar" }
        });
        p.apply_interactive_event("s1", false, &unrelated);
        assert_eq!(db.session_todos("s1").unwrap(), None);

        // Reset with t1, then test bad payload
        p.apply_interactive_event("s1", false, &update_payload);
        assert!(db.session_todos("s1").unwrap().is_some());

        let bad_payload = serde_json::json!({
            "sessionId": "s1",
            "event": "todos_update",
            "payload": { "todos": "not-an-array" }
        });
        p.apply_interactive_event("s1", false, &bad_payload);
        // todos_json was not overwritten with invalid payload
        assert!(db.session_todos("s1").unwrap().unwrap().contains("t1"));
    }
}

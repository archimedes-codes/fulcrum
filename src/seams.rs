//! The crate's seams to the world that stays desktop (T5/S4, plan Steps B, B-bis, E).
//!
//! ## What a seam is for, and what it is not
//!
//! The crate cannot depend on `archimedes_lib` — that edge is the package cycle
//! T5 exists to sever. So every capability the engine needs from the desktop
//! arrives either as DATA that moved here (see [`crate::worker_types`],
//! [`crate::store_rows`], [`crate::subagent_types`]) or as a TRAIT the desktop
//! implements. This module holds the traits.
//!
//! ## Read this before judging S4 by its test count
//!
//! **As written (S4), nothing in this module was called by the crate yet — and
//! that was the plan, not an omission.** The S4 ruling in the status file states
//! the consequence plainly: S4 is the first sub-step that gives the crate outward
//! references, so "the desktop still compiles through the re-exports" — the proof
//! that carried S1–S3 — proves nothing here, and a trait with no caller has no
//! runtime behaviour to test. **The crate-side callers have since arrived, and
//! this paragraph is corrected rather than left to rot: `persist.rs:85` holds
//! `Arc<dyn FulcrumPersistence>` and `subagent.rs:95` holds
//! `Arc<dyn Dispatcher>`, both in production, landed by S5b's re-plumb.** What
//! has NOT changed is what the gate proves for the DESKTOP impls.
//!
//! For those, the proof is still **instantiation**, not execution.
//! `impl FulcrumPersistence for storage::Db` (`src-tauri/src/storage/db.rs:1289`)
//! and `impl Dispatcher for worker::manager::WorkerManager`
//! (`crates/fulcrum-core/src/worker/manager/mod.rs:519`, crate-side since S8.3)
//! must COMPILE, against the real inherent methods, with the real types. A
//! surface-comparison test that never names the impl is void (Step B-bis says so
//! in terms), and `src-tauri/tests/fulcrum_seams.rs` is written to be broken —
//! its own file comment records the deletion that reddens each pin.
//!
//! The seams that are NOT here are deliberate too, and each was measured rather
//! than assumed:
//! - **E5 refresh trait — CANCELLED.** It existed for `refresh.rs:31 →
//!   session::SessionManager`, but S3 measured the crate's need for
//!   `provider_auth` at one item (`headers::expand_map`) and left `refresh.rs`
//!   desktop, so the edge never crosses the boundary. **Still cancelled after
//!   T7:** `refresh.rs` is the ONE file of that directory that stayed desktop
//!   (it holds `tauri::AppHandle` + `session::SessionManager`), so the edge it
//!   was written for is still entirely desktop-side.
//! - **`ProviderSource` — LIVE as of T7/S7.2, and the ONE seam of this set that
//!   the crate CALLS.** It is declared in `budget.rs` rather than here, because
//!   its only consumer is `BudgetPoller` and a trait with one consumer belongs
//!   beside it. S3
//!   cancelled it because `budget.rs` stayed put, and that reasoning was sound
//!   at the time ("a seam is only justified by a measured crossing edge") — the
//!   seam was premature, not wrong. T7 moved `budget.rs`, and the two edges the
//!   review round measured are exactly what it now crosses: `budget.rs:48` was
//!   `use crate::config::load_settings` and `budget.rs:732` was
//!   `load_settings(&self.config_dir)` inside `reload_providers`, a
//!   `settings.json` read on EVERY 15 s poll tick. The trait is
//!   [`crate::provider_auth::budget::ProviderSource`]; the
//!   desktop implements it at
//!   `src-tauri/src/agent/provider_auth/mod.rs::SettingsProviderSource` and
//!   constructs the poller through that file's `budget_poller_for`. **The crate
//!   holds no settings directory at all** — `BudgetPoller`'s `config_dir` field
//!   is gone, so the poller cannot ask for the file even by accident. Unlike the
//!   two traits above, this one does NOT rest on an instantiation pin alone: the
//!   desktop impl is instantiated by the production `lib.rs` call site, so
//!   deleting it fails to compile, and the crate-side behaviour (re-read per arm,
//!   LiteLLM filter crate-side) is pinned in `budget.rs`'s own tests.
//! - **E6 [`crate::mcp::OpenRequester`] — already LIVE**, landed in S3 with three
//!   working pins, because `mcp` moved then. It is the contrast case for the two
//!   traits here: the shape a seam has when it has a caller.
//! - **E.6's executor trait — NOT here.** Inverting `execute_tool` touches the hot
//!   dispatch path and is a separate step, proved by the two-sided e2e tests.

use crate::store_rows::{MessageRow, SessionInfo, SessionRow};
use crate::subagent_types::SubagentOutcome;
use crate::worker_types::SubagentDispatchWire;

/// The crate-side persistence error (plan Step B-bis, first bullet).
///
/// ## Why `DbError` could not simply move
///
/// `storage::DbError` has three variants and one of them is
/// `Sqlite(#[from] rusqlite::Error)` (`storage/db.rs:23-31`). Moving it would put
/// `rusqlite` in the crate's dependency list, which fails the master roadmap's
/// own decoupling criterion for no benefit — the engine has no business linking a
/// SQLite driver it never calls.
///
/// So the crate gets the two variants the engine's own errors can BE: `Io` and
/// `Json`. The desktop's impl maps `DbError::Sqlite(_) → StoreError::Io`.
///
/// ## Why that mapping is safe, and the measurement that licenses it
///
/// Collapsing `Sqlite` into `Io` LOSES information, so it is only admissible if
/// nothing needs that information back. Verified at `8e1d159c`, all three sites
/// the plan names plus an exhaustive grep of `DbError::Sqlite` in `src/`:
/// - `harness/store.rs:220` — CONSTRUCTS `DbError::Json` (moves to the crate);
/// - `worker/store.rs:62` — CONSTRUCTS `DbError::Io` (this file moved into the
///   crate at T8/S8.1 and now builds `StoreError::Io` directly — see the S8.1
///   entry; it was desktop when this measurement was taken);
/// - `agent/errors.rs:171` — the ONE `match` on `DbError::Sqlite`, and it is
///   `impl From<DbError> for HistoryError`, which is desktop-only (the IPC error
///   envelope) and keeps the full three-variant `DbError` untouched;
/// - `worker/store.rs:239` — a `matches!(err, DbError::Io(_))` assertion in a
///   test for the file above; it moved with the file at T8/S8.1 and now reads
///   `matches!(err, StoreError::Io(_))` (`worker/store.rs:259`), so it never
///   needed the `Sqlite` variant either.
///
/// No code that will live in the crate matches on `Sqlite`. The only other
/// `Sqlite` mention under `src/` is `errors.rs:963`, a test fixture constructing
/// a `rusqlite::Error` to drive the desktop's own `From` impl — desktop, unchanged.
///
/// ## What the collapse costs, in writing
///
/// A SQLite failure that reaches the crate surfaces as "io error: sqlite error:
/// no such table: sessions" rather than "sqlite error: …". Today the crate-side
/// consumer of a persistence error is `TranscriptPersister`, and it does the one
/// thing that makes the difference invisible: `eprintln!("{e}")` and continue.
/// Nothing branches on the variant, so no behaviour rides on it. If a future
/// consumer DOES need to distinguish a SQLite failure (say, to retry a locked
/// database), the honest fix is to add a `Sqlite(String)` variant to
/// `StoreError` — NOT to pull `rusqlite` into the crate, and not to widen this
/// `From` impl to pretend `Io` already said "sqlite".
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// An I/O failure — including anything the desktop's SQLite layer reports,
    /// which arrives here through the `DbError::Sqlite(_) → Io` mapping (see the
    /// type's docs for why that is admissible).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A serialization failure. The engine's own error constructor
    /// (`harness/store.rs`'s `.map_err(DbError::Json)` equivalent): a corrupt
    /// transcript row must be an error, never a silently-empty load.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// The crate's persistence seam (plan Step B) — the surface the moved files
/// actually need, measured against the desktop's `storage::Db`, not generalised.
///
/// Distinct from the harness's own `Store` trait (which moves here in S5),
/// and which stays the loop's five-method session-store seam (`insert_message` /
/// `load_messages` / `clear_messages` / `replace_messages` / `persist_display`,
/// with `NoopStore` + `SessionStore` as its implementations). This one is WIDER
/// and serves the other owned-set files — `persist.rs` needs `record_session` and
/// `session_cost`, the permission fixtures need `upsert_space`, `normalize.rs`
/// needs the clock. Two traits because the loop and the Supervisor-side writer
/// ask different questions; collapsing them would force `NoopStore` to grow
/// session-row machinery it has no use for.
///
/// `Db` is the ONLY implementation, and `src-tauri/tests/fulcrum_seams.rs` pins
/// that instantiation. Every method delegates to an existing inherent method of
/// the same name — no behaviour is re-implemented here, which is what makes the
/// seam behaviour-identical by construction.
pub trait FulcrumPersistence: Send + Sync {
    /// The `native_messages` upsert keyed `(session_id, seq)`.
    fn insert_native_message(
        &self,
        session_id: &str,
        seq: i64,
        role: &str,
        content_json: &str,
    ) -> Result<(), StoreError>;

    /// The compaction rewrite — ATOMIC (one transaction), so a crash mid-rewrite
    /// cannot leave a partial transcript.
    fn replace_native_messages(
        &self,
        session_id: &str,
        rows: &[(i64, String, String)],
    ) -> Result<(), StoreError>;

    /// The `sessions.context_usage_json` write.
    fn record_session_context_usage(
        &self,
        session_id: &str,
        used: u64,
        window: u64,
    ) -> Result<(), StoreError>;

    /// The `sessions.cost_usd` write — a SNAPSHOT overwrite, not a delta
    /// (ADR 0082: the frame carries the cumulative total, so the loop is the sole
    /// writer of its own key).
    fn record_session_cost(&self, session_id: &str, total: f64) -> Result<(), StoreError>;

    /// The `sessions.todos_json` write (ADR 0073). A missing row is a no-op.
    fn record_session_todos(&self, session_id: &str, todos: Option<&str>)
        -> Result<(), StoreError>;

    /// Read one session row.
    fn session(&self, session_id: &str) -> Result<Option<SessionRow>, StoreError>;

    /// The provider transcript, `seq`-ordered, as raw `content_json` strings —
    /// deliberately STRINGS: parsing them is the caller's job, so this seam never
    /// has to name `ChatMessage`.
    fn load_native_messages(&self, session_id: &str) -> Result<Vec<String>, StoreError>;

    /// The DISPLAY `messages` rows (the frontend's shape, not the transcript).
    fn messages_for(&self, session_id: &str) -> Result<Vec<MessageRow>, StoreError>;

    /// The stored cost figure. `None` = "no call ever reported a cost", which is
    /// NOT `Some(0.0)` — collapsing them is the D5 defect ADR 0082 exists to avoid.
    fn session_cost(&self, session_id: &str) -> Result<Option<f64>, StoreError>;

    /// The persisted todos JSON (`None` when missing or NULL).
    fn session_todos(&self, session_id: &str) -> Result<Option<String>, StoreError>;

    /// Record a session's row (the fixtures' and `persist.rs`'s
    /// `ensure_session_row` route).
    fn record_session(&self, info: &SessionInfo) -> Result<(), StoreError>;

    /// List sessions (the fixtures' and tests' read).
    fn list_sessions(&self, include_archived: bool) -> Result<Vec<SessionRow>, StoreError>;

    /// Set a Space's trust flag (the permission fixtures). `true` = a row matched.
    fn set_space_trusted(&self, path: &str, trusted: bool) -> Result<bool, StoreError>;

    /// Create-or-touch a Space row (the permission fixtures).
    fn upsert_space(&self, path: &str, default_trusted: bool) -> Result<(), StoreError>;

    /// Upsert one display `messages` row — the `StoreFrame::Display` write.
    ///
    /// Not on the plan's drafted 15-method list, and it belongs: `persist.rs:149`
    /// applies the `Display` arm with `db.record_message(sid, &row.kind,
    /// Some(&row.message_key), &payload)`, and the S4 ruling requires the surface
    /// be checked against what S5's files actually call. Leaving it out would
    /// have made S5 add a trait method — i.e. discover the shape was wrong one
    /// sub-step late, which is exactly the failure mode the ruling warns about.
    ///
    /// The signature keeps the desktop's inherent four-argument shape rather than
    /// taking a `DisplayRow`: `DisplayRow` is a field type of `StoreFrame` and so
    /// lives in [`crate::store_rows`], but `persist.rs` flattens it to a JSON
    /// string before the write, so the row type never actually reaches the store.
    fn record_message(
        &self,
        session_id: &str,
        kind: &str,
        message_key: Option<&str>,
        payload_json: &str,
    ) -> Result<(), StoreError>;

    /// The wall clock, in unix milliseconds.
    ///
    /// Step B-bis requires the clock be reached THROUGH this trait rather than
    /// through a second private copy, and the desktop's two private `now_ms`
    /// functions are gone as a result. See [`crate::clock`] for why the impl
    /// delegates to one shared function rather than owning a clock of its own.
    fn now_ms(&self) -> i64;
}

/// The crate's subagent-dispatch seam (plan Step E.2, edge E2).
///
/// `SubagentSessionManager` (moving in S5) holds `Arc<dyn Dispatcher>` in place
/// of the desktop's `Arc<WorkerManager>`; the desktop's `WorkerManager`
/// implements it in `~10` lines in `worker/manager/mod.rs`, each method a
/// one-line delegation to the inherent method of the same name.
///
/// The two methods are exactly the two the S5 call sites use, read off
/// `subagent.rs:205` (`dispatch_subagent_to`, the in-process `dispatch_native`
/// path whose outcome resolves a oneshot) and `subagent.rs:257`
/// (`cancel_subagent`, behind `SubagentCancel::Worker::cancel`). Deliberately NOT
/// the plain `dispatch_subagent` (the `Outbound::SubagentDispatch` route that
/// answers a requesting Worker instead of a oneshot): nothing in the crate's set
/// calls it, and a seam method with no caller is the thing the S3 RULING tells us
/// to stop writing.
///
/// `SubagentCancel` does NOT come back through this trait. Today
/// `dispatch_native` builds `SubagentCancel::Worker { wm, child_id }`, which is an
/// `Arc<WorkerManager>` in a crate type — the edge re-appearing through the back
/// door. S5 lands the change instead: `SubagentCancel::Worker` becomes a held
/// `Arc<dyn Dispatcher>` beside the existing `Flag` and `None` arms, so `cancel()`
/// dispatches through the same seam. The pre-minted `child_session_id` is why the
/// id is a PARAMETER here rather than something the impl mints: the caller must
/// know it before the dispatch starts, to build its own cancel handle.
pub trait Dispatcher: Send + Sync {
    /// Dispatch a child subagent whose outcome RESOLVES `outcome` (a oneshot, not
    /// a requesting Worker), targeting the PRE-MINTED `child_session_id`.
    fn dispatch_subagent_to(
        &self,
        parent_session_id: &str,
        wire: SubagentDispatchWire,
        child_session_id: String,
        outcome: tokio::sync::oneshot::Sender<SubagentOutcome>,
    );

    /// Cancel the dispatch for a child session id. Idempotent — a gone drive is a
    /// no-op, not an error.
    fn cancel_subagent(&self, child_session_id: &str);
}

#[cfg(test)]
mod tests {
    use super::StoreError;

    /// The one behaviour the crate itself can observe about its error type: the
    /// two variants carry their source's `Display` verbatim, so a message that
    /// reaches the user through `TranscriptPersister`'s `eprintln!` reads the
    /// same as it did with `DbError`.
    #[test]
    fn store_error_renders_its_source_verbatim() {
        let io = StoreError::Io(std::io::Error::other("disk went away"));
        assert_eq!(io.to_string(), "io error: disk went away");
        let json: StoreError = serde_json::from_str::<u32>("not json")
            .expect_err("a parse failure")
            .into();
        assert!(json.to_string().starts_with("json error: "), "got {json}");
    }

    /// `StoreError` must be constructible from both sources by `?`, because the
    /// crate-side call sites that produce it do exactly that (`harness/store.rs`
    /// maps a `serde_json` failure, `worker/store.rs` builds an `Io`).
    #[test]
    fn store_error_converts_from_both_sources_with_the_question_mark() {
        fn reads() -> Result<(), StoreError> {
            let _ = serde_json::from_str::<serde_json::Value>("{")?;
            Ok(())
        }
        assert!(matches!(reads(), Err(StoreError::Json(_))));
    }
}

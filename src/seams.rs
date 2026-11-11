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

// ── (issue #180 F1) the Task-board seam ──────────────────────────────────────
//
// The two halves of one seam, and the split is the crate's existing house shape
// (`RenderRequester`, `SubagentDispatcher`): a SYNC trait the DESKTOP implements,
// plus — here, because this module's doc says it holds the traits — the
// crate-internal ASYNC half the Worker parks on. See
// [`crate::worker::task_store::IpcTaskStore`] for the requester and
// [`crate::tools::exec::task_tools`] for the seven arms that call it.

/// The crate-side Task-seam error (issue #180 F1).
///
/// Deliberately NOT `StoreError`: that type is `#[from] std::io::Error` /
/// `#[from] serde_json::Error` and is NOT serializable, while this one has to ride
/// the wire inside `Inbound::TaskResponse`'s `Result` — the desktop builds it at
/// the same boundary `From<DbError> for StoreError` occupies (Sqlite → `Io`,
/// missing row → `NotFound`, grammar or size refusal → `Invalid`). No `rusqlite`
/// here, exactly the `StoreError` rule that keeps a SQLite driver out of the crate.
///
/// `PartialEq` so a test in EITHER repo can compare an answer instead of
/// string-matching its `Display`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum TaskStoreError {
    /// The store itself failed (the desktop folds every SQLite failure here).
    Io {
        /// What failed, in one sentence — it reaches the model as the tool result.
        message: String,
    },
    /// The row the call named does not exist (a missing Task or artifact).
    NotFound {
        /// Which id or key was not found.
        message: String,
    },
    /// The call was well-formed JSON and still not admissible (a key outside the
    /// artifact grammar, a markdown over the command layer's size cap).
    Invalid {
        /// Why it was refused.
        message: String,
    },
}

/// One board row — a card, and deliberately **key names only** for its artifacts.
///
/// A card payload must NEVER carry markdown: that is the whole point of splitting
/// `BoardRow` from [`Artifact`]. A board of thirty cards each carrying a spec would
/// otherwise put the entire corpus in one tool result.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoardRow {
    /// The task id (`tsk_…`).
    pub id: String,
    /// The Space the card lives in.
    pub space_path: String,
    /// The per-Space card number (what the human sees).
    pub number: u32,
    /// The card's title.
    pub title: String,
    /// The card's prose (one of the two fields that have always carried it).
    pub body: Option<String>,
    /// The spec PATH (a path chip — NOT an artifact, and not rendered as one).
    pub spec_path: Option<String>,
    /// The Stage the card currently sits on.
    pub stage_slug: String,
    /// The card's position within its column.
    pub sort_order: i64,
    /// Creation time, unix millis.
    pub created_at: i64,
    /// Last touch, unix millis.
    pub updated_at: i64,
    /// The names of the Task's artifacts — REQUIRED, never optional, `[]` when
    /// nobody has written one. Fetching the content is
    /// [`TaskStore::get_artifact`]'s job.
    pub artifact_keys: Vec<String>,
}

/// [`TaskStore::get_task`]'s row: the desktop's `TaskRow` 1:1, plus the artifact
/// key names (the detail dialog shows the chips, so the fetch has to answer them).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetail {
    /// The task id (`tsk_…`).
    pub id: String,
    /// The Space the card lives in.
    pub space_path: String,
    /// The per-Space card number.
    pub number: u32,
    /// The card's title.
    pub title: String,
    /// The card's prose.
    pub body: Option<String>,
    /// The spec path.
    pub spec_path: Option<String>,
    /// The Stage the card currently sits on.
    pub stage_slug: String,
    /// The card's position within its column.
    pub sort_order: i64,
    /// Creation time, unix millis.
    pub created_at: i64,
    /// Last touch, unix millis.
    pub updated_at: i64,
    /// The artifact key names (never the content — see [`BoardRow`]).
    pub artifact_keys: Vec<String>,
}

/// The write answer ([`TaskStore::create_task`] / [`TaskStore::edit_task`]): the
/// plain row, no artifact keys (the write did not touch them, so the answer says
/// nothing about them).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRow {
    /// The task id (`tsk_…`).
    pub id: String,
    /// The Space the card lives in.
    pub space_path: String,
    /// The per-Space card number.
    pub number: u32,
    /// The card's title.
    pub title: String,
    /// The card's prose.
    pub body: Option<String>,
    /// The spec path.
    pub spec_path: Option<String>,
    /// The Stage the card sits on after the write.
    pub stage_slug: String,
    /// The card's position within its column.
    pub sort_order: i64,
    /// Creation time, unix millis.
    pub created_at: i64,
    /// Last touch, unix millis.
    pub updated_at: i64,
}

/// An artifact's content — the ONLY DTO that carries `markdown`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    /// The Task the artifact belongs to.
    pub task_id: String,
    /// The artifact's name (the grammar is the desktop's rule).
    pub key: String,
    /// The full markdown, verbatim.
    pub markdown: String,
    /// Creation time, unix millis (survives an upsert).
    pub created_at: i64,
    /// Last write, unix millis.
    pub updated_at: i64,
}

/// The write answer ([`TaskStore::write_artifact`]) — meta ONLY, never the
/// markdown back: the caller just sent it, so echoing it doubles the frame.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactMeta {
    /// The key that was written.
    pub key: String,
    /// Write time, unix millis.
    pub updated_at: i64,
    /// The stored size in bytes (what the command layer capped).
    pub bytes: usize,
}

/// The answer to a move.
///
/// `on_enter` is **OPAQUE string data**: the crate passes it through and never
/// interprets it. The boost it names needs the desktop's `SessionManager`, which
/// storage cannot name, so it stays command-layer — the crate never learns what
/// compaction is, and this type must not grow a field that says otherwise (pinned
/// by `move_outcome_stays_free_of_compaction`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveOutcome {
    /// The Stage the card landed on (the destination the call named, unless the
    /// desktop's rules redirected it — which is what `note` explains).
    pub to_stage: String,
    /// Opaque destination metadata for the CALLER's command layer to act on.
    pub on_enter: Option<String>,
    /// A human-readable note about the move, when there is one worth making.
    pub note: Option<String>,
}

/// The seven verbs as DATA, so the Worker can frame one over
/// [`Outbound::TaskRequest`](crate::worker::protocol::Outbound::TaskRequest) and
/// the Supervisor can decode it into the matching [`TaskStore`] method.
///
/// Tagged by `verb` (camelCase), payload keys camelCase. An optional that is `None`
/// serializes to `null` (serde's own `Option` encoding) and decodes from EITHER
/// `null` or an absent key (`#[serde(default)]`) — a frame from a peer that omits it
/// entirely is a peer saying "no value", not a malformed frame.
///
/// The `rename_all` appears TWICE on purpose, and the second one is load-bearing:
/// serde's enum-level `rename_all` renames the VARIANT names (hence `listBoard`,
/// `getArtifact`) and does NOT reach into a struct variant's fields — measured, not
/// assumed (an enum-level-only spelling emits `{"verb":"listBoard",
/// "space_path":…}`, which would silently break the camelCase contract the desktop
/// decodes against). The per-variant `rename_all` is what makes the PAYLOAD keys
/// camelCase, and `the_task_store_dtos_serialize_to_the_agreed_camel_case_wire_keys`
/// is the pin that keeps it there.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "verb", rename_all = "camelCase")]
pub enum TaskCall {
    /// [`TaskStore::list_board`].
    #[serde(rename_all = "camelCase")]
    ListBoard {
        /// The Space to list.
        space_path: String,
    },
    /// [`TaskStore::get_task`].
    #[serde(rename_all = "camelCase")]
    GetTask {
        /// The task id.
        task_id: String,
    },
    /// [`TaskStore::create_task`].
    #[serde(rename_all = "camelCase")]
    CreateTask {
        /// The Space to create in.
        space_path: String,
        /// The card's title.
        title: String,
        /// The card's prose, when given.
        #[serde(default)]
        body: Option<String>,
        /// The starting Stage, when given (a Space starts on its first stage).
        #[serde(default)]
        stage_slug: Option<String>,
    },
    /// [`TaskStore::edit_task`].
    #[serde(rename_all = "camelCase")]
    EditTask {
        /// The task id.
        task_id: String,
        /// A new title, when given.
        #[serde(default)]
        title: Option<String>,
        /// A new body, when given.
        #[serde(default)]
        body: Option<String>,
        /// A new spec path, when given.
        #[serde(default)]
        spec_path: Option<String>,
    },
    /// [`TaskStore::move_task`].
    #[serde(rename_all = "camelCase")]
    MoveTask {
        /// The Space the card lives in.
        space_path: String,
        /// The task id.
        task_id: String,
        /// The destination stage.
        to_stage: String,
    },
    /// [`TaskStore::get_artifact`].
    #[serde(rename_all = "camelCase")]
    GetArtifact {
        /// The task id.
        task_id: String,
        /// The artifact key.
        key: String,
    },
    /// [`TaskStore::write_artifact`].
    #[serde(rename_all = "camelCase")]
    WriteArtifact {
        /// The task id.
        task_id: String,
        /// The artifact key.
        key: String,
        /// The full markdown to store.
        markdown: String,
    },
}

/// The crate's Task-board seam (ADR 0092, issue #180 F1): every board verb the
/// model can reach.
///
/// SYNC by design — this is the contract the DESKTOP implements
/// (`impl TaskStore for storage::Db`, Supervisor-side, because the Supervisor is
/// the sole SQLite writer and the Worker has no `Db`). The Worker's half is the
/// async [`TaskCaller`], because a sync method receives no turn token and cannot
/// park on a oneshot: the two rules ADR 0092 mandates for this seam (race the turn
/// token, clean the pending map on `Drop`) only exist for a future the executor can
/// drop.
///
/// Every method takes borrows and returns an owned answer: the answer crosses a
/// channel, the arguments do not.
pub trait TaskStore: Send + Sync {
    /// The Space's board, one row per card (artifact KEY NAMES only).
    fn list_board(&self, space_path: &str) -> Result<Vec<BoardRow>, TaskStoreError>;

    /// One task, `None` when the id names nothing (asking for what is not there is
    /// an answer, not an error).
    fn get_task(&self, task_id: &str) -> Result<Option<TaskDetail>, TaskStoreError>;

    /// Create a card. `stage_slug` `None` = the Space's first stage.
    fn create_task(
        &self,
        space_path: &str,
        title: &str,
        body: Option<&str>,
        stage_slug: Option<&str>,
    ) -> Result<TaskRow, TaskStoreError>;

    /// Edit a card's title / body / spec path (`None` = leave that field alone).
    fn edit_task(
        &self,
        task_id: &str,
        title: Option<&str>,
        body: Option<&str>,
        spec_path: Option<&str>,
    ) -> Result<TaskRow, TaskStoreError>;

    /// Move a card to another stage.
    fn move_task(
        &self,
        space_path: &str,
        task_id: &str,
        to_stage: &str,
    ) -> Result<MoveOutcome, TaskStoreError>;

    /// One artifact's markdown, `None` when the key names nothing.
    fn get_artifact(&self, task_id: &str, key: &str) -> Result<Option<Artifact>, TaskStoreError>;

    /// Write one artifact (upsert — one row per `(task_id, key)`).
    fn write_artifact(
        &self,
        task_id: &str,
        key: &str,
        markdown: &str,
    ) -> Result<ArtifactMeta, TaskStoreError>;
}

/// The Worker-side ASYNC half of the Task seam (issue #180 F1): the request-issuer
/// the executor arms park on.
///
/// The same shape as
/// [`RenderRequester::request_render`](crate::web::RenderRequester::request_render)
/// — a SYNC method returning a boxed future — and the same reason: the trait object
/// has to live in an `Arc` the `ToolCtx` can clone, and `async fn` in a trait object
/// is not object safe. The desktop NEVER implements this; it implements
/// [`TaskStore`]. The Worker implements it once, as
/// [`IpcTaskStore`](crate::worker::task_store::IpcTaskStore), which frames the call
/// over the wire and parks until `Inbound::TaskResponse` answers it.
///
/// The `Value` it hands back is the DTO the matching [`TaskStore`] method returns,
/// serialized — each arm deserializes the shape it expects, so the wire needs no
/// per-verb result enum.
pub trait TaskCaller: Send + Sync {
    /// Issue one call. Resolves with the answer, or fails with the store's error.
    ///
    /// The returned future MUST be droppable mid-park: dropping it removes the
    /// pending-map entry (the `TaskRequestCleanup` guard), which is what keeps a
    /// Stop from leaking.
    fn call(
        &self,
        call: TaskCall,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, TaskStoreError>> + Send>,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::sync::Arc;

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

    // ── (issue #180 F1) the Task seam ────────────────────────────────────

    /// The compile proof for the EXACT sync signature.
    ///
    /// The real impl (`impl TaskStore for storage::Db`) lives in the OTHER repo, so
    /// in-clone the only thing that can pin the seven signatures is an impl HERE.
    /// This one answers every verb with the same canned failure — its job is
    /// INSTANTIATION (D2 compiles against these exact parameter orders and
    /// option-ness, or this type stops satisfying the trait), not behaviour, and
    /// [`TestTaskStore`] is named by the object-safety pin below so it cannot rot
    /// into dead code.
    struct TestTaskStore;

    impl TaskStore for TestTaskStore {
        fn list_board(&self, _space_path: &str) -> Result<Vec<BoardRow>, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
        fn get_task(&self, _task_id: &str) -> Result<Option<TaskDetail>, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
        fn create_task(
            &self,
            _space_path: &str,
            _title: &str,
            _body: Option<&str>,
            _stage_slug: Option<&str>,
        ) -> Result<TaskRow, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
        fn edit_task(
            &self,
            _task_id: &str,
            _title: Option<&str>,
            _body: Option<&str>,
            _spec_path: Option<&str>,
        ) -> Result<TaskRow, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
        fn move_task(
            &self,
            _space_path: &str,
            _task_id: &str,
            _to_stage: &str,
        ) -> Result<MoveOutcome, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
        fn get_artifact(
            &self,
            _task_id: &str,
            _key: &str,
        ) -> Result<Option<Artifact>, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
        fn write_artifact(
            &self,
            _task_id: &str,
            _key: &str,
            _markdown: &str,
        ) -> Result<ArtifactMeta, TaskStoreError> {
            Err(TaskStoreError::Io {
                message: "test store".into(),
            })
        }
    }

    /// The seam is usable as a trait object behind the same `Arc` shape every
    /// other seam in this crate is held by — object safety, and the `Send + Sync`
    /// bound that lets a `Worker`-thread handle cross an await point.
    #[test]
    fn task_store_is_object_safe_and_shareable() {
        let store: Arc<dyn TaskStore> = Arc::new(TestTaskStore);
        let err = store
            .list_board("/some/space")
            .expect_err("the test store always fails");
        assert_eq!(
            err,
            TaskStoreError::Io {
                message: "test store".into()
            },
            "the canned failure rides back verbatim (`TaskStoreError` is `PartialEq` \
             so a test in either repo can compare it)"
        );
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&store);
    }

    /// A full board row, every field populated (the fixture the wire-key pin
    /// walks — a field that is missing from the pin's expected key list shows up
    /// as an EXTRA key here, which is how the pin catches a rename in either
    /// direction).
    fn a_board_row() -> BoardRow {
        BoardRow {
            id: "tsk_1".into(),
            space_path: "/space".into(),
            number: 7,
            title: "Ship the seam".into(),
            body: Some("prose".into()),
            spec_path: Some("docs/x.md".into()),
            stage_slug: "doing".into(),
            sort_order: -3,
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_500,
            artifact_keys: vec!["spec".into(), "research".into()],
        }
    }

    /// A full task-detail row, every field populated.
    fn a_task_detail() -> TaskDetail {
        TaskDetail {
            id: "tsk_1".into(),
            space_path: "/space".into(),
            number: 7,
            title: "Ship the seam".into(),
            body: None,
            spec_path: None,
            stage_slug: "approved".into(),
            sort_order: 2,
            created_at: 1,
            updated_at: 2,
            artifact_keys: vec![],
        }
    }

    /// A full task row, every field populated.
    fn a_task_row() -> TaskRow {
        TaskRow {
            id: "tsk_1".into(),
            space_path: "/space".into(),
            number: 7,
            title: "Ship the seam".into(),
            body: Some("prose".into()),
            spec_path: None,
            stage_slug: "approved".into(),
            sort_order: 2,
            created_at: 1,
            updated_at: 2,
        }
    }

    /// The DTO wire keys ARE the cross-repo contract (issue #180 F1's
    /// `## Provides`): the desktop's `impl TaskStore for Db` and its
    /// `toolDetailsRegistry` reader keys are written against these spellings, so
    /// a rename here is a silent contract break in the other repo — the field
    /// still compiles, the JSON just stops matching. This pins the EXACT key set
    /// of every DTO and of `TaskCall`'s verb tags, in the camelCase the protocol
    /// mandates.
    ///
    /// Mutation note: rename `BoardRow::sort_order` to `sortOrder` in the struct
    /// and this test reddens with `sortOrder` in the ACTUAL set (serde would then
    /// emit `sort_order`, because the attribute only rewrites snake_case inputs) —
    /// i.e. it fails for the right reason, in both directions.
    #[test]
    fn the_task_store_dtos_serialize_to_the_agreed_camel_case_wire_keys() {
        let keys_of = |v: &Value| -> Vec<String> {
            v.as_object()
                .expect("a DTO serializes to a JSON object")
                .keys()
                .cloned()
                .collect()
        };
        let sorted = |mut v: Vec<String>| {
            v.sort();
            v
        };

        assert_eq!(
            sorted(keys_of(&serde_json::to_value(a_board_row()).unwrap())),
            sorted(
                [
                    "id",
                    "spacePath",
                    "number",
                    "title",
                    "body",
                    "specPath",
                    "stageSlug",
                    "sortOrder",
                    "createdAt",
                    "updatedAt",
                    "artifactKeys",
                ]
                .map(String::from)
                .to_vec()
            ),
            "`BoardRow` wire keys"
        );
        assert_eq!(
            sorted(keys_of(&serde_json::to_value(a_task_detail()).unwrap())),
            sorted(
                [
                    "id",
                    "spacePath",
                    "number",
                    "title",
                    "body",
                    "specPath",
                    "stageSlug",
                    "sortOrder",
                    "createdAt",
                    "updatedAt",
                    "artifactKeys",
                ]
                .map(String::from)
                .to_vec()
            ),
            "`TaskDetail` wire keys"
        );
        assert_eq!(
            sorted(keys_of(&serde_json::to_value(a_task_row()).unwrap())),
            sorted(
                [
                    "id",
                    "spacePath",
                    "number",
                    "title",
                    "body",
                    "specPath",
                    "stageSlug",
                    "sortOrder",
                    "createdAt",
                    "updatedAt",
                ]
                .map(String::from)
                .to_vec()
            ),
            "`TaskRow` wire keys (no `artifactKeys`)"
        );
        assert_eq!(
            sorted(keys_of(
                &serde_json::to_value(Artifact {
                    task_id: "tsk_1".into(),
                    key: "spec".into(),
                    markdown: "# Spec".into(),
                    created_at: 1,
                    updated_at: 2,
                })
                .unwrap()
            )),
            sorted(
                ["taskId", "key", "markdown", "createdAt", "updatedAt"]
                    .map(String::from)
                    .to_vec()
            ),
            "`Artifact` wire keys — the ONLY DTO that carries `markdown`"
        );
        assert_eq!(
            sorted(keys_of(
                &serde_json::to_value(ArtifactMeta {
                    key: "spec".into(),
                    updated_at: 2,
                    bytes: 6,
                })
                .unwrap()
            )),
            sorted(["key", "updatedAt", "bytes"].map(String::from).to_vec()),
            "`ArtifactMeta` wire keys (never the markdown back)"
        );
        assert_eq!(
            sorted(keys_of(
                &serde_json::to_value(MoveOutcome {
                    to_stage: "done".into(),
                    on_enter: Some("compact".into()),
                    note: None,
                })
                .unwrap()
            )),
            sorted(["toStage", "onEnter", "note"].map(String::from).to_vec()),
            "`MoveOutcome` wire keys — three and only three (`on_enter` is OPAQUE data)"
        );

        // `TaskCall`'s verb tags, and the per-verb payload keys.
        let verb_of = |c: &TaskCall| -> (String, Vec<String>) {
            let v = serde_json::to_value(c).unwrap();
            let tag = v
                .get("verb")
                .and_then(Value::as_str)
                .expect("every TaskCall carries a `verb` tag")
                .to_string();
            (tag, sorted(keys_of(&v)))
        };
        assert_eq!(
            verb_of(&TaskCall::ListBoard {
                space_path: "/s".into()
            }),
            (
                "listBoard".into(),
                sorted(vec!["verb".into(), "spacePath".into()])
            )
        );
        assert_eq!(
            verb_of(&TaskCall::GetTask {
                task_id: "tsk_1".into()
            }),
            (
                "getTask".into(),
                sorted(vec!["verb".into(), "taskId".into()])
            )
        );
        assert_eq!(
            verb_of(&TaskCall::CreateTask {
                space_path: "/s".into(),
                title: "t".into(),
                body: Some("b".into()),
                stage_slug: None,
            }),
            (
                "createTask".into(),
                sorted(vec![
                    "verb".into(),
                    "spacePath".into(),
                    "title".into(),
                    "body".into(),
                    "stageSlug".into()
                ])
            ),
            "`createTask` carries all four keys; an absent optional is `null` (serde's \
             `Option` encoding — the plan's `#[serde(default)]` spelling, which accepts \
             BOTH `null` and an absent key on the way in)"
        );
        assert_eq!(
            serde_json::to_value(&TaskCall::CreateTask {
                space_path: "/s".into(),
                title: "t".into(),
                body: None,
                stage_slug: None,
            })
            .unwrap()["body"],
            Value::Null,
            "the `null` spelling is explicit, so a reader of the contract never has to \
             guess whether an absent key was ever legal"
        );
        assert_eq!(
            verb_of(&TaskCall::EditTask {
                task_id: "tsk_1".into(),
                title: Some("t".into()),
                body: None,
                spec_path: Some("p".into()),
            }),
            (
                "editTask".into(),
                sorted(vec![
                    "verb".into(),
                    "taskId".into(),
                    "title".into(),
                    "body".into(),
                    "specPath".into()
                ])
            )
        );
        assert_eq!(
            verb_of(&TaskCall::MoveTask {
                space_path: "/s".into(),
                task_id: "tsk_1".into(),
                to_stage: "done".into(),
            }),
            (
                "moveTask".into(),
                sorted(vec![
                    "verb".into(),
                    "spacePath".into(),
                    "taskId".into(),
                    "toStage".into()
                ])
            )
        );
        assert_eq!(
            verb_of(&TaskCall::GetArtifact {
                task_id: "tsk_1".into(),
                key: "spec".into(),
            }),
            (
                "getArtifact".into(),
                sorted(vec!["verb".into(), "taskId".into(), "key".into()])
            )
        );
        assert_eq!(
            verb_of(&TaskCall::WriteArtifact {
                task_id: "tsk_1".into(),
                key: "spec".into(),
                markdown: "# Spec".into(),
            }),
            (
                "writeArtifact".into(),
                sorted(vec![
                    "verb".into(),
                    "taskId".into(),
                    "key".into(),
                    "markdown".into()
                ])
            )
        );
    }

    /// `TaskStoreError`'s wire shape (the desktop maps `DbError → TaskStoreError`
    /// at the same boundary `From<DbError> for StoreError` occupies, and the
    /// `Inbound::TaskResponse` result rides serde's `Result` encoding, so this
    /// shape is what the OTHER repo decodes). Tag `kind`, camelCase value:
    /// `{"kind":"notFound","message":"…"}`.
    #[test]
    fn task_store_error_has_the_agreed_wire_shape() {
        assert_eq!(
            serde_json::to_value(TaskStoreError::NotFound {
                message: "no task tsk_9".into()
            })
            .unwrap(),
            serde_json::json!({ "kind": "notFound", "message": "no task tsk_9" })
        );
        let round: TaskStoreError =
            serde_json::from_str(r#"{"kind":"invalid","message":"bad key"}"#).unwrap();
        assert_eq!(
            round,
            TaskStoreError::Invalid {
                message: "bad key".into()
            }
        );
        // serde's `Result` encoding, which is what `TaskResponse.result` carries.
        let ok: Result<Value, TaskStoreError> = Ok(serde_json::json!({"ok": true}));
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            serde_json::json!({ "Ok": { "ok": true } })
        );
        let err: Result<Value, TaskStoreError> = Err(TaskStoreError::Io {
            message: "disk".into(),
        });
        assert_eq!(
            serde_json::to_value(&err).unwrap(),
            serde_json::json!({ "Err": { "kind": "io", "message": "disk" } })
        );
    }

    /// An optional `TaskCall` field that is ABSENT on the wire decodes to `None`
    /// (a pre-F1 or hand-built frame that omits `body` must not fail to decode —
    /// the decoder's permissiveness rule, applied to the payload).
    #[test]
    fn a_task_call_with_om_optionals_decodes() {
        let call: TaskCall =
            serde_json::from_str(r#"{"verb":"createTask","spacePath":"/s","title":"t"}"#).unwrap();
        assert_eq!(
            call,
            TaskCall::CreateTask {
                space_path: "/s".into(),
                title: "t".into(),
                body: None,
                stage_slug: None,
            }
        );
    }

    /// `MoveOutcome` carries exactly three fields and NOTHING that names
    /// compaction: the crate never learns what `on_enter` MEANS (the boost stays
    /// command-layer in the desktop). A field-name census is the only way to pin
    /// an absence that a future "helpful" widening would introduce.
    #[test]
    fn move_outcome_stays_free_of_compaction() {
        let v = serde_json::to_value(MoveOutcome {
            to_stage: "done".into(),
            on_enter: Some("fresh".into()),
            note: None,
        })
        .unwrap();
        let keys = keys_of_names(&v);
        assert_eq!(
            keys,
            vec![
                "note".to_string(),
                "onEnter".to_string(),
                "toStage".to_string()
            ],
            "`MoveOutcome` has exactly its three contract fields"
        );
        let text = v.to_string().to_lowercase();
        for banned in ["compact", "session", "launch", "boost"] {
            assert!(
                !text.contains(banned),
                "`MoveOutcome` must not learn about `{banned}` — the on_enter boost \
                 is the desktop's command layer"
            );
        }
    }

    fn keys_of_names(v: &Value) -> Vec<String> {
        let mut ks: Vec<String> = v.as_object().expect("an object").keys().cloned().collect();
        ks.sort();
        ks
    }
}

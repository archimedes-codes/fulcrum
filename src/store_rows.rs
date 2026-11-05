//! The persistence trait's ROW types (T5/S4, plan Step B-bis).
//!
//! ## Why these live in the crate
//!
//! `FulcrumPersistence::session` returns `SessionRow` and `messages_for` returns
//! `Vec<MessageRow>`, but `storage/` STAYS desktop (it is `rusqlite`). A trait in
//! `fulcrum-core` cannot name a type in `archimedes_lib` — that import is the
//! package cycle Step A exists to sever. So the ROWS move and `storage/db.rs`
//! imports them back (desktop→crate, the allowed direction).
//!
//! This is safe because these two structs are pure data, verified at `8e1d159c`
//! before moving: plain `#[derive(Debug, Clone, Serialize, Deserialize)]` +
//! `#[serde(rename_all = "camelCase")]`, 8 and 6 `pub` fields, and ZERO
//! `rusqlite`/`Connection` references. They do not know they came from SQLite.
//! The SQL lives beside them in `db.rs` (`SESSION_COLUMNS` / `MESSAGE_COLUMNS` /
//! the row mappers), which is where the database-specific knowledge was already.
//!
//! `SessionInfo` and `ContextUsage` join them because the trait's `record_session`
//! takes `&SessionInfo` and `agent/persist.rs:270` CONSTRUCTS it — the moved
//! code builds this type, so the type must be in the crate. Note the correction
//! this carries: plan §2d first filed `SessionInfo` as "a desktop DB row type,
//! decided with S4"; the S4 ruling overruled that — it MOVES, and `src/types.rs`
//! re-exports it.
//!
//! `DisplayRow` moves for the third reason: `StoreFrame::Display` carries a
//! `Vec<DisplayRow>`, and `StoreFrame` is a wire type the crate must own (E1).
//! Leaving it in `harness/store.rs` would have made a crate type name a desktop
//! type — the cycle, arriving from an unexpected direction.
//!
//! Every desktop path (`crate::storage::SessionRow`, `crate::types::SessionInfo`,
//! `crate::agent::harness::store::DisplayRow`) keeps resolving through the
//! re-exports on the desktop side; the field sets and serde attributes are
//! byte-identical, so nothing on the wire changes.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

// ── `SessionRow` ──────────────────────────────────────────────────────────────

/// A row from the `sessions` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRow {
    /// The ACP session id (primary key).
    pub id: String,
    pub cwd: String,
    /// Unix milliseconds.
    pub created_at: i64,
    pub title: Option<String>,
    /// The negotiated `agent_capabilities`, serialized (camelCase).
    pub capabilities_json: String,
    /// The desktop's archived flag (ADR 0016): `true` hides the session from
    /// its Space group into the Archived section; the transcript is kept.
    pub archived: bool,
    /// The session's last known context usage (`{"used": n, "window": n}` —
    /// the `context_usage_update` frame's values: the provider's
    /// `input_tokens` vs the model's window), persisted on every frame so a
    /// CLOSED session's context survives (the frontend's store drops it on
    /// close — the row is the source of truth for the stored session's bar).
    /// `None` until the first frame (a fresh session with no usage yet).
    pub context_usage_json: Option<String>,
    /// The ephemeral-subagent flag (ADR 0025 §3): `true` for a subagent
    /// session (hidden from `list_sessions` — `is_subagent = 0` filter).
    /// `false` for a main session.
    pub is_subagent: bool,
    /// The session's provider-reported running cost in USD (issue #105): the
    /// LAST `cost_update` frame's CUMULATIVE total, stored by
    /// `record_session_cost` (a snapshot write since Fulcrum T2) so a CLOSED
    /// session keeps its figure (the frontend's store drops its entry on close —
    /// the row is the source of truth for the stored figure) and a RESUMED
    /// session continues its total instead of restarting it (the row is seeded
    /// back into the loop via `StartEnv.cost_usd`).
    ///
    /// `None` means NO model call ever reported a cost — which is NOT the same
    /// as `Some(0.0)`, a call that reported a genuine zero. The renderer prints
    /// nothing for the first and `$0.00` for the second; collapsing them is the
    /// D5 defect this column exists to avoid (ADR 0082). The column therefore
    /// has no `DEFAULT` (see the migration in `Db::open`).
    pub cost_usd: Option<f64>,
    /// The session's title generation stage (0 = untitled, 1 = stage 1 title, 2 = stage 2 / settled title).
    pub title_stage: i64,
}

// ── `MessageRow` ──────────────────────────────────────────────────────────────

/// A row from the `messages` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRow {
    pub id: i64,
    pub session_id: String,
    /// `'user' | 'agent-text' | 'agent-thought' | 'tool-call' | 'diff'`.
    pub kind: String,
    /// `ContentChunk.messageId` for agent-text; the tool call id for
    /// tool-call rows; `NULL` otherwise.
    pub message_key: Option<String>,
    /// The message body, serialized (camelCase).
    pub payload_json: String,
    /// Unix milliseconds.
    pub created_at: i64,
}

// ── `SessionInfo` + `ContextUsage` ────────────────────────────────────────────

/// A fully established session, ready to accept prompts.
///
/// `Serialize` so it can cross the IPC boundary as a command return value.
///
/// `capabilities` is the pi session's capability envelope (item 1 of the
/// swap plan): `piSessionId` / `piSessionFile`? / `model`? /
/// `thinkingLevel` / `loadSession` / `promptCapabilities` — the `model` key
/// is ABSENT when `get_state` reports no model, and `piSessionFile` is
/// A live session's identity + its current configuration (what the UI shows
/// in the header / the model selector). Serialized camelCase over IPC.
///
/// `capabilities` is the native session's capability envelope (item 1 of the
/// swap plan): `model`? / `thinkingLevel` / `loadSession` / `promptCapabilities`
/// — the `model` key is ABSENT when the session has no model, and the two
/// trailing keys are load-bearing: `loadSession` gates the frontend's Resume
/// button and `promptCapabilities.image` gates image sending (fail-closed).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub session_id: String,
    /// The session's working directory — the SAME folder as its Space, and
    /// the value the whole Space↔session relationship is built from.
    ///
    /// A `PathBuf` (not a `String`) so the type says "path", but note what
    /// actually crosses the wire: serde stringifies it with
    /// `PathBuf::display()`, and THAT string is load-bearing in three places.
    /// 1. **The frontend joins Spaces to sessions by raw string equality on
    ///    it.** `session.cwd === space.path` at
    ///    `src/store/sessions.ts:466`/`:502`/`:560`/`:565`/`:921`/`:966`/`:1249`
    ///    (grep `cwd ===` — line numbers drift), plus
    ///    `src/components/SpaceTabs.tsx:106` (the tab-close "how many are
    ///    open" gate) and `:162` (the per-tab count) and
    ///    `src/components/SpacesList.tsx:74` (reached with `session.cwd` from
    ///    `:204`/`:758`). `sessions.ts:1038` gates `startSessionInActiveSpace`
    ///    on the same equality, and `addSpace(info.cwd)`
    ///    (`sessions.ts:1053`, `NewSpaceDialog.tsx:93`) derives the Space tab
    ///    from it. `delete_space` / `find_space` take the frontend's string
    ///    raw, so a mismatch there fails in silence.
    /// 2. **`agent/worker/manager.rs:768`** matches Workers with a raw string
    ///    compare between `StartEnv.cwd` and a Space path the frontend gave.
    /// 3. `record_session` persists `info.cwd.display()` verbatim into
    ///    `sessions.cwd`, while `upsert_space` persists its
    ///    RE-canonicalized `space_key()` — two different code paths that must
    ///    keep producing one identical string
    ///    (`tests/storage.rs::the_session_cwd_string_and_the_space_path_key_are_the_same_string`).
    ///
    /// ⚠ So: **do not normalize this field's serialized form on one side
    /// only.** `std::fs::canonicalize` on Windows ADDS a `\\?\` verbatim
    /// prefix (it never removes one) and that string is what users see and
    /// what these comparisons compare — but `src-tauri/windows-app-manifest.xml`
    /// declares no `<longPathAware>` element, which Task 4's constraint 1
    /// records as making the prefix the only thing currently letting a Space
    /// deeper than `MAX_PATH` open (that absence is verified in this repo;
    /// the runtime behaviour behind it is Windows-only and is NOT observed
    /// here).
    /// Stripping the prefix on one side would fix the display and break the
    /// join. The full five-constraint analysis and the evidence status live in
    /// `docs/roadmap/readable-ipc-errors.md` → Task 4 / `## Findings`.
    pub cwd: PathBuf,
    pub capabilities: Value,
    /// The session's configuration options (model / thinking level
    /// selectors) synthesized from the `ModelCatalog` (the native session's
    /// model / thinking-level selectors); `None` when they can't be
    /// synthesized (no catalog / unresolvable stored model).
    pub config_options: Option<Vec<Value>>,
    /// The desktop's archived flag (ADR 0016). `false` for a newly
    /// started or ephemeral session; the resume paths and
    /// `list_sessions` read it from the stored row.
    pub archived: bool,
    /// The session's generated title (`None` until a title is generated).
    /// Auto-generated only in this change — there is no rename affordance,
    /// so `Some` always means "the naming pass wrote this". `#[serde(default)]`
    /// so a pre-feature stored envelope / subagent envelope deserialises.
    #[serde(default)]
    pub title: Option<String>,
    /// The session's last known context usage (the `context_usage_update`
    /// frame's values — the provider's `input_tokens` vs the model's
    /// window), persisted on the `sessions` row on every frame so a
    /// CLOSED session's context survives (the frontend's store drops the
    /// entry on close — the row is the source of truth for the stored
    /// session's context bar). `None` until the first frame (a fresh
    /// session with no usage yet).
    pub context_usage: Option<ContextUsage>,
    /// The ephemeral-subagent flag (ADR 0025 §3): `true` for a subagent
    /// session (its transcript persists as a HIDDEN `sessions` row —
    /// `list_sessions` filters `is_subagent = 0`). `false` for a main
    /// session. `#[serde(default)]` so stored rows pre-dating the column
    /// (and the `Value::Null`-ish ephemeral envelope) deserialize `false`.
    #[serde(default)]
    pub is_subagent: bool,
    /// The session's provider-reported running cost in USD (issue #105).
    ///
    /// `None` means no call ever reported a cost — which is NOT the same as
    /// `Some(0.0)`, a call that reported a genuine zero. The renderer prints
    /// nothing for the first and `$0.00` for the second; collapsing them is
    /// the D5 defect this field exists to avoid (ADR 0082).
    ///
    /// Read straight off the row's typed `cost_usd` column (no parsing —
    /// unlike `context_usage_json`, which stores a JSON blob) at the resume
    /// and list projections, so a CLOSED session keeps its figure: the
    /// frontend's store drops its entry on close, and the row is the source
    /// of truth for the stored session's figure.
    ///
    /// `serde(default)` so a `SessionInfo` built without it still
    /// deserialises; crosses IPC as `costUsd` (the struct's
    /// `rename_all = "camelCase"`).
    #[serde(default)]
    pub cost_usd: Option<f64>,
}

/// A session's last known context usage (the `context_usage_update` frame's
/// values — the provider's `input_tokens` vs the model's `context_window`).
/// Persisted on the `sessions` row on every frame (the row's
/// `context_usage_json` column — `{"used": n, "window": n}`) so a CLOSED
/// session's context survives: the frontend's store drops the entry on
/// close, but the row keeps the last known value for the stored session's
/// context bar.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsage {
    pub used: u64,
    pub window: u64,
}

// ── `DisplayRow` ──────────────────────────────────────────────────────────────

/// One display `messages`-table upsert (the `Db::record_message` shape).
/// `Serialize` / `Deserialize` (ADR 0025 Task 2): the `DisplayUpsert` wire
/// frame carries these verbatim (`IpcStore` → the Supervisor's persister).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DisplayRow {
    pub kind: String,
    pub message_key: String,
    pub payload_json: serde_json::Value,
    pub created_at: i64,
}

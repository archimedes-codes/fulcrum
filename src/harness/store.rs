//! The transcript persistence seam (ADR 0025): the `Store` trait the
//! `AgentLoop` writes its provider transcript through, plus the two
//! implementations that need nothing from the world outside this crate.
//!
//! # Why this file is a SPLIT and not a move (T5/S5b, the S5b RULING's BLOCKER 2)
//!
//! All six trait methods returned `Result<_, DbError>`, and `DbError` wraps
//! `rusqlite::Error` — so the trait could not cross the package boundary
//! UNCHANGED; it crossed with its error type changed to [`StoreError`], the
//! crate's two-variant persistence error (Step B-bis). The plan's S5 addendum
//! claimed "both pure" and that was wrong; this is the correction the S5b RULING
//! records.
//!
//! What moved here: [`Store`] (the trait, signatures re-typed), [`NoopStore`]
//! (the test double — it never errored, so nothing about it changes but the
//! spelling of the error), [`role_str`] (the denormalized `role` column), and
//! the `DisplayRow` re-export (the row type moved in S4).
//!
//! What STAYED DESKTOP: `SessionStore` (`src-tauri/src/agent/harness/store.rs`)
//! and its `impl Store` — it holds the desktop's `storage::Db`, and a crate
//! file cannot name one (`rusqlite` is not in this crate's graph; reaching
//! `storage` would be the `fulcrum-core → archimedes_lib` edge that IS the
//! package cycle). The desktop's impl maps at the boundary with
//! `.map_err(StoreError::from)` / the `From` impl `storage/db.rs` defines.
//! `worker/store.rs`'s `impl Store for IpcStore` re-points at the same error
//! type (it constructs `Io`, which exists on both sides).
//!
//! # ⚠️ THE ONE BEHAVIOUR THIS CHANGES, stated plainly
//!
//! The `DbError::Sqlite(_) → StoreError::Io` mapping changes an ERROR STRING. A
//! SQLite failure that reaches the desktop's
//! `session/mod.rs`'s `.map_err(|e| SessionError::Io(e.to_string()))` now reads
//! `"io error: sqlite error: …"` where it read `"sqlite error: …"`. The S4 child
//! documented the same at `storage/db.rs`; NO test pins the string and nothing
//! branches on the variant, so the change is error-path-only (a corrupt or
//! unreadable database, reported to the user with one extra prefix). This split
//! is not behaviour-identical and does not claim to be.
//!
//! # The pin
//!
//! `impl Store for SessionStore` and `impl Store for IpcStore` both COMPILE is
//! what proves the re-plumbed impls are real — asserted by
//! `src-tauri/tests/fulcrum_seams.rs::session_store_and_ipc_store_satisfy_the_crate_store`
//! (S4's instantiation rule: an impl nobody instantiates proves nothing). Its
//! value is LOWER than S4's pins: it proves the re-plumbed impls still typecheck
//! against the crate trait, not that the SQLite behaviour behind them is
//! unchanged — that rests on the relocated `SessionStore` tests, which are the
//! same tests that ran before the split.

use crate::provider_types::{ChatMessage, ChatRole};
use crate::seams::StoreError;
// Re-exported, not just imported: `crate::agent::harness::store::DisplayRow`
// is the path four desktop files and `harness/mod.rs`'s own block name, and the
// crate's `normalize.rs` / `loop/emit.rs` use it too.
pub use crate::store_rows::DisplayRow;

/// The transcript persistence seam (ADR 0025): `SessionStore` (the desktop's
/// SQLite half — unchanged behaviour, the error mapped at the boundary) /
/// [`NoopStore`] (tests) / `IpcStore` (the Worker: frames every call over the
/// protocol; the Supervisor applies the frames — the sole writer).
pub trait Store: Send + Sync {
    fn insert_message(
        &self,
        session_id: &str,
        seq: u64,
        role: &str,
        content_json: &str,
    ) -> Result<(), StoreError>;
    fn load_messages(&self, session_id: &str) -> Result<Vec<ChatMessage>, StoreError>;
    fn clear_messages(&self, session_id: &str) -> Result<(), StoreError>;
    fn replace_messages(
        &self,
        session_id: &str,
        rows: &[(u64, String, String)],
    ) -> Result<(), StoreError>;
    /// The display persistence. Takes PRE-COMPUTED rows (the loop keeps
    /// the `TurnState` accumulators — `text_acc` / `tool_state` /
    /// `thought_state`): `SqliteStore` writes them via
    /// `db.record_message` (the write half of today's `persist_update`),
    /// `IpcStore` frames them (`DisplayUpsert`), `NoopStore` ignores them.
    ///
    /// Tries EVERY row (a failed row never stops the rest — the old
    /// `persist_update`'s `let _ =` semantics) and reports the first
    /// error (the caller's handling is unchanged: a failure is logged /
    /// ignored per the call site).
    fn persist_display(&self, session_id: &str, rows: &[DisplayRow]) -> Result<(), StoreError>;
    /// The context-usage persistence (the `sessions.context_usage_json`
    /// write — today `db.record_session_context_usage(used, window)`;
    /// a missing `sessions` row is a no-op, not an error).
    fn record_context_usage(
        &self,
        session_id: &str,
        used: u64,
        window: u64,
    ) -> Result<(), StoreError>;
}

/// The no-op store (tests — never errors, never panics; `load_messages`
/// yields an empty transcript, the `IpcStore`'s write-only counterpart is
/// Task 2's).
pub struct NoopStore;

impl Store for NoopStore {
    fn insert_message(
        &self,
        _session_id: &str,
        _seq: u64,
        _role: &str,
        _content_json: &str,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn load_messages(&self, _session_id: &str) -> Result<Vec<ChatMessage>, StoreError> {
        Ok(Vec::new())
    }

    fn clear_messages(&self, _session_id: &str) -> Result<(), StoreError> {
        Ok(())
    }

    fn replace_messages(
        &self,
        _session_id: &str,
        _rows: &[(u64, String, String)],
    ) -> Result<(), StoreError> {
        Ok(())
    }

    fn persist_display(&self, _session_id: &str, _rows: &[DisplayRow]) -> Result<(), StoreError> {
        Ok(())
    }

    fn record_context_usage(
        &self,
        _session_id: &str,
        _used: u64,
        _window: u64,
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

/// The denormalized `role` index (the `native_messages.role` column).
pub(crate) fn role_str(role: ChatRole) -> &'static str {
    match role {
        ChatRole::System => "system",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::Tool => "tool",
    }
}

#[cfg(test)]
mod tests {
    use super::{role_str, NoopStore, Store};
    use crate::provider_types::{ChatRole, MessageContent};
    use crate::store_rows::DisplayRow;
    use std::sync::Arc;

    // The `SessionStore` half of this module's test suite — `temp_db` and the
    // four `SessionStore`-as-a-`Store` tests plus the two #96 dir-ownership
    // pins — stayed with the STAYING half
    // (`src-tauri/src/agent/harness/store.rs`), because a fixture that opens a
    // real `storage::Db` cannot live here at all (Step A.4: `#[cfg(test)]` does
    // not survive into an rlib, and this crate cannot name `storage::Db`
    // regardless). They did not need to RELOCATE to a new integration target:
    // they travel with the code they test, and the desktop lib's own test build
    // sees `storage::Db`. What is here is the half that only names crate types.

    #[test]
    fn role_str_maps_the_roles() {
        assert_eq!(role_str(ChatRole::System), "system");
        assert_eq!(role_str(ChatRole::User), "user");
        assert_eq!(role_str(ChatRole::Assistant), "assistant");
        assert_eq!(role_str(ChatRole::Tool), "tool");
    }

    /// `NoopStore` — all 6 seam methods return `Ok(())` (never errors,
    /// never panics; `load_messages` yields an empty transcript).
    #[test]
    fn noop_store_all_methods_ok() {
        let store: Arc<dyn Store> = Arc::new(NoopStore);
        assert!(store.insert_message("s1", 0, "user", "{}").is_ok());
        let loaded = store.load_messages("s1").expect("load never errors");
        assert!(
            loaded.is_empty(),
            "the noop store yields an empty transcript"
        );
        assert!(store.clear_messages("s1").is_ok());
        assert!(store
            .replace_messages("s1", &[(0, "user".to_string(), "{}".to_string())])
            .is_ok());
        assert!(store
            .persist_display(
                "s1",
                &[
                    DisplayRow {
                        kind: "agent-text".to_string(),
                        message_key: "m1".to_string(),
                        payload_json: serde_json::json!({ "text": "hi" }),
                        created_at: 1,
                    },
                    DisplayRow {
                        kind: "tool-call".to_string(),
                        message_key: "t1".to_string(),
                        payload_json: serde_json::json!({}),
                        created_at: 2,
                    },
                ]
            )
            .is_ok());
        assert!(store.record_context_usage("s1", 100, 10_000).is_ok());
    }

    /// The `MessageContent` import is the `ChatMessage` the noop load could
    /// hand back; pin that the crate's OWN type is what the trait speaks, not a
    /// desktop look-alike (the S4 lesson: two look-alike `ProviderConfig`s
    /// compiled until a packaging site compared them).
    #[test]
    fn the_store_trait_speaks_the_crate_chat_message() {
        let msg = crate::provider_types::ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text("hello".to_string()),
            tool_call_id: None,
            tool_calls: None,
        };
        let store: Arc<dyn Store> = Arc::new(NoopStore);
        let json = serde_json::to_string(&msg).expect("a ChatMessage serializes");
        assert!(store.insert_message("s1", 0, "user", &json).is_ok());
    }
}

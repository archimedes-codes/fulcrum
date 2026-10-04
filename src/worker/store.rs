//! The Worker's `Store` seam implementation (ADR 0025 Task 2): frames
//! every `Store` call over the protocol (`TranscriptInsert` /
//! `TranscriptReplace` / `DisplayUpsert` / `ContextUsage` outbound
//! frames) — the Supervisor's `TranscriptPersister` (Task 4) applies
//! them to SQLite (the sole writer). The transcript the Supervisor
//! persists is byte-for-byte what the loop persists today — just
//! transported.
//!
//! `load_messages` is the DEFENSIVE write-only error (the Worker's
//! loop never calls it: the Supervisor rehydrates and sends the
//! transcript in `Start`; if it IS called, the error is explicit, not
//! a silent empty). `clear_messages` is a no-op (the native resume
//! path's `clear` is external-era; nothing in the Worker world calls
//! it). All sends are unbounded (never fail — nothing is dropped).

use tokio::sync::mpsc;

use crate::agent::harness::provider::ChatMessage;
use crate::agent::harness::store::{DisplayRow, Store};
use crate::agent::worker::protocol::Outbound;
use crate::storage::DbError;

/// The Worker's `Store` seam (ADR 0025 Task 2): frames every `Store`
/// call over the protocol (the Supervisor's `TranscriptPersister` —
/// Task 4 — applies the frames to SQLite, the sole writer).
pub struct IpcStore {
    outbound: mpsc::UnboundedSender<Outbound>,
}

impl IpcStore {
    pub fn new(outbound: mpsc::UnboundedSender<Outbound>) -> Self {
        Self { outbound }
    }
}

impl Store for IpcStore {
    /// `insert_message` → `TranscriptInsert` (the provider transcript
    /// rows — system prompt seq 0, user messages, assistant + tool-role
    /// messages).
    fn insert_message(
        &self,
        session_id: &str,
        seq: u64,
        role: &str,
        content_json: &str,
    ) -> Result<(), DbError> {
        // Unbounded — send never fails (nothing is dropped).
        let _ = self.outbound.send(Outbound::TranscriptInsert {
            session_id: session_id.to_string(),
            seq,
            role: role.to_string(),
            content_json: content_json.to_string(),
        });
        Ok(())
    }

    /// `load_messages` → the DEFENSIVE write-only error (the Worker's
    /// loop never calls it: the Supervisor rehydrates and sends the
    /// transcript in `Start`; if it IS called, the error is explicit,
    /// not a silent empty).
    fn load_messages(&self, _session_id: &str) -> Result<Vec<ChatMessage>, DbError> {
        Err(DbError::Io(std::io::Error::other(
            "worker store is write-only — resume is rehydrated via the start envelope",
        )))
    }

    /// `clear_messages` → a no-op (the native resume path's `clear` is
    /// external-era; nothing in the Worker world calls it).
    fn clear_messages(&self, _session_id: &str) -> Result<(), DbError> {
        Ok(())
    }

    /// `replace_messages` → `TranscriptReplace` (the compaction rewrite).
    fn replace_messages(
        &self,
        session_id: &str,
        rows: &[(u64, String, String)],
    ) -> Result<(), DbError> {
        let _ = self.outbound.send(Outbound::TranscriptReplace {
            session_id: session_id.to_string(),
            rows: rows.to_vec(),
        });
        Ok(())
    }

    /// `persist_display` → `DisplayUpsert` (the display `messages` rows
    /// verbatim).
    fn persist_display(&self, session_id: &str, rows: &[DisplayRow]) -> Result<(), DbError> {
        let _ = self.outbound.send(Outbound::DisplayUpsert {
            session_id: session_id.to_string(),
            rows: rows.to_vec(),
        });
        Ok(())
    }

    /// `record_context_usage` → `ContextUsage { session_id, used,
    /// window }` (the `sessions.context_usage_json` write over the wire).
    fn record_context_usage(
        &self,
        session_id: &str,
        used: u64,
        window: u64,
    ) -> Result<(), DbError> {
        let _ = self.outbound.send(Outbound::ContextUsage {
            session_id: session_id.to_string(),
            used,
            window,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::store::DisplayRow;
    use crate::storage::DbError;
    use tokio::sync::mpsc;

    /// A test-observed outbound channel + the `IpcStore`.
    fn store() -> (IpcStore, mpsc::UnboundedReceiver<Outbound>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (IpcStore::new(tx), rx)
    }

    /// `insert_message` → `TranscriptInsert` with the fields verbatim.
    #[tokio::test]
    async fn insert_message_emits_the_transcript_insert_frame() {
        let (store, mut rx) = store();
        let content_json = r#"{"role":"user","content":"hi"}"#;
        store
            .insert_message("s1", 7, "user", content_json)
            .expect("the insert never fails (unbounded)");
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::TranscriptInsert {
                session_id,
                seq,
                role,
                content_json: cj,
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(seq, 7);
                assert_eq!(role, "user");
                assert_eq!(cj, content_json, "the content rides verbatim");
            }
            other => panic!("expected `TranscriptInsert`, got {other:?}"),
        }
    }

    /// `replace_messages` → `TranscriptReplace` (the compaction rewrite —
    /// the rows verbatim, order preserved).
    #[tokio::test]
    async fn replace_messages_emits_the_transcript_replace_frame() {
        let (store, mut rx) = store();
        let rows: &[(u64, String, String)] = &[
            (0, "system".to_string(), "{}".to_string()),
            (1, "user".to_string(), "{}".to_string()),
        ];
        store
            .replace_messages("s1", rows)
            .expect("the replace never fails (unbounded)");
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::TranscriptReplace {
                session_id,
                rows: back,
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(back, rows.to_vec(), "the rows ride verbatim");
            }
            other => panic!("expected `TranscriptReplace`, got {other:?}"),
        }
    }

    /// `persist_display` → `DisplayUpsert` (the display `messages` rows
    /// verbatim).
    #[tokio::test]
    async fn persist_display_emits_the_display_upsert_frame() {
        let (store, mut rx) = store();
        let rows = [
            DisplayRow {
                kind: "agent-text".to_string(),
                message_key: "m1".to_string(),
                payload_json: serde_json::json!({ "text": "Hello" }),
                created_at: 3,
            },
            DisplayRow {
                kind: "tool-call".to_string(),
                message_key: "t1".to_string(),
                payload_json: serde_json::json!({ "title": "run" }),
                created_at: 4,
            },
        ];
        store
            .persist_display("s1", &rows)
            .expect("the upsert never fails (unbounded)");
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::DisplayUpsert {
                session_id,
                rows: back,
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(back, rows.to_vec(), "the rows ride verbatim");
            }
            other => panic!("expected `DisplayUpsert`, got {other:?}"),
        }
    }

    /// `record_context_usage` → `ContextUsage { used, window }` (the
    /// `sessions.context_usage_json` write over the wire).
    #[tokio::test]
    async fn record_context_usage_emits_the_context_usage_frame() {
        let (store, mut rx) = store();
        store
            .record_context_usage("s1", 53_760, 128_000)
            .expect("the usage write never fails (unbounded)");
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::ContextUsage {
                session_id,
                used,
                window,
            } => {
                assert_eq!(session_id, "s1");
                assert_eq!(used, 53_760);
                assert_eq!(window, 128_000);
            }
            other => panic!("expected `ContextUsage`, got {other:?}"),
        }
    }

    /// `load_messages` — the defensive write-only error (a `DbError::Io`
    /// variant — explicit, NOT a silent empty transcript).
    #[tokio::test]
    async fn load_messages_is_the_defensive_write_only_error() {
        let (store, _rx) = store();
        let err = store
            .load_messages("s1")
            .expect_err("the worker store is write-only");
        assert!(
            matches!(err, DbError::Io(_)),
            "the error is the explicit `DbError::Io` variant, got {err:?}"
        );
    }

    /// `clear_messages` — a no-op (`Ok(())`; nothing in the Worker world
    /// calls it, the native resume path's `clear` is external-era).
    #[tokio::test]
    async fn clear_messages_is_a_noop_ok() {
        let (store, mut rx) = store();
        assert!(
            store.clear_messages("s1").is_ok(),
            "clear is a no-op `Ok(())`"
        );
        assert!(rx.try_recv().is_err(), "clear emits no frame");
    }
}

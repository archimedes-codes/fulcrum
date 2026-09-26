//! The desktop-side todo store (Phase 2, Task 1): the `todo_update`
//! bridge method (the suite's `manage_todo_list`) is serviced by the
//! desktop — this is the shared store it writes to and reads from.
//!
//! One `Arc<TodoStore>` per `SessionDriver` (the main `SessionManager`
//! and the `SubagentSessionManager` each get their OWN store, mirroring
//! how `pending_bridge` is split across the two managers); the inner
//! map is keyed by the listener's session id so it also covers
//! subagent sessions.
//!
//! The wire casing is snake_case (`pending` / `in_progress` /
//! `completed` — the suite's `StringEnum(["pending","in_progress",
//! "completed"])`, `packages/todo/src/tool.ts`); an absent
//! `description` is OMITTED (not `null`) — the suite's `description?:
//! string` wire shape.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// A todo item's status (the wire casing is snake_case — the suite's
/// `StringEnum(["pending", "in_progress", "completed"])`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TodoStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "in_progress")]
    InProgress,
    #[serde(rename = "completed")]
    Completed,
}

/// One todo item (the suite's `TodoItem` wire shape: `content`,
/// `status`, and an OPTIONAL `description` — omitted, not `null`,
/// when absent, matching the TS `description?: string`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TodoItem {
    pub content: String,
    pub status: TodoStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The shared todo store: one `Arc<TodoStore>` per `SessionDriver`
/// (the main `SessionManager` and the `SubagentSessionManager` each
/// get their OWN store, mirroring how `pending_bridge` is split
/// across the two managers); the inner map is keyed by the
/// listener's session id so it also covers subagent sessions.
///
/// `set` REPLACES the whole list (the tool's `write` is "replace
/// entire todo list") and returns the stored list; `get` returns a
/// clone (an empty `Vec` when absent).
pub struct TodoStore {
    inner: Mutex<HashMap<String, Vec<TodoItem>>>,
}

impl TodoStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Replace the session's todo list (the tool's `write`) and return
    /// the stored list.
    pub fn set(&self, session_id: &str, items: Vec<TodoItem>) -> Vec<TodoItem> {
        let mut map = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        map.insert(session_id.to_string(), items.clone());
        items
    }

    /// The session's todo list (a clone; an empty `Vec` when absent).
    pub fn get(&self, session_id: &str) -> Vec<TodoItem> {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(session_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Remove the session's todo list (session teardown — a resumed
    /// session must not read the previous incarnation's todos, and the
    /// map must not grow one entry per session forever).
    pub fn remove(&self, session_id: &str) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(session_id);
    }
}

impl Default for TodoStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{TodoItem, TodoStatus, TodoStore};

    #[test]
    fn set_then_get_round_trips() {
        let store = TodoStore::new();
        let items = vec![
            TodoItem {
                content: "Fix the auth middleware".to_string(),
                status: TodoStatus::InProgress,
                description: Some("src/auth.rs".to_string()),
            },
            TodoItem {
                content: "Write the tests".to_string(),
                status: TodoStatus::Pending,
                description: None,
            },
        ];
        let stored = store.set("s1", items.clone());
        assert_eq!(stored, items, "set returns the stored list");
        assert_eq!(
            store.get("s1"),
            items,
            "get returns a clone of the stored list"
        );
    }

    #[test]
    fn set_replaces_not_appends() {
        let store = TodoStore::new();
        store.set(
            "s1",
            vec![TodoItem {
                content: "old".to_string(),
                status: TodoStatus::Completed,
                description: None,
            }],
        );
        store.set(
            "s1",
            vec![TodoItem {
                content: "new".to_string(),
                status: TodoStatus::Pending,
                description: None,
            }],
        );
        let items = store.get("s1");
        assert_eq!(
            items.len(),
            1,
            "set replaces the whole list (the tool's `write` is replace)"
        );
        assert_eq!(items[0].content, "new");
    }

    #[test]
    fn get_on_an_unknown_session_is_empty() {
        let store = TodoStore::new();
        assert!(store.get("nope").is_empty());
    }

    #[test]
    fn the_serde_casing_is_snake_case() {
        let item = TodoItem {
            content: "x".to_string(),
            status: TodoStatus::InProgress,
            description: None,
        };
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["content"], "x");
        assert_eq!(json["status"], "in_progress");
        assert!(
            json.get("description").is_none(),
            "an absent description is OMITTED, not null (the suite's `description?`)"
        );
        let item = TodoItem {
            content: "x".to_string(),
            status: TodoStatus::Pending,
            description: Some("d".to_string()),
        };
        let json = serde_json::to_value(&item).unwrap();
        assert_eq!(json["description"], "d");

        // Deserialization accepts the wire casing.
        let parsed: TodoItem =
            serde_json::from_str(r#"{"content":"x","status":"completed"}"#).unwrap();
        assert_eq!(parsed.status, TodoStatus::Completed);
    }

    #[test]
    fn two_sessions_are_independent() {
        let store = TodoStore::new();
        store.set(
            "a",
            vec![TodoItem {
                content: "a".to_string(),
                status: TodoStatus::Pending,
                description: None,
            }],
        );
        store.set(
            "b",
            vec![TodoItem {
                content: "b".to_string(),
                status: TodoStatus::Completed,
                description: None,
            }],
        );
        assert_eq!(store.get("a").len(), 1);
        assert_eq!(store.get("a")[0].content, "a");
        assert_eq!(store.get("b")[0].content, "b");
    }

    #[test]
    fn remove_drops_the_session_entry() {
        // Session teardown: the entry is removed (a resumed session must
        // not read the previous incarnation's todos, and the map must not
        // grow one entry per session forever).
        let store = TodoStore::new();
        store.set(
            "a",
            vec![TodoItem {
                content: "a".to_string(),
                status: TodoStatus::Pending,
                description: None,
            }],
        );
        store.remove("a");
        assert!(store.get("a").is_empty(), "remove drops the entry");
        // A `remove` on an unknown session is a no-op (no panic).
        store.remove("nope");
        // Removing one session does not touch the others.
        store.set(
            "b",
            vec![TodoItem {
                content: "b".to_string(),
                status: TodoStatus::Pending,
                description: None,
            }],
        );
        store.remove("a");
        assert_eq!(store.get("b").len(), 1);
    }
}

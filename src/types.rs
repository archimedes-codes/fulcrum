//! Shared agent-layer vocabulary types.
//!
//! Extracted from `session.rs` (Task 5 of the god-file decomposition). A
//! TRUE SIBLING of `session/` — the modules that used to import these items
//! from `session` (`subagent`, `worker/manager`) now import from here, which
//! is what breaks the module cycle (a child of `session/` would leave the
//! `subagent → session` / `worker/manager → session` edges intact).

/// Mint a fresh unique session ID with the `arch_` prefix.
pub fn mint_session_id() -> String {
    format!("arch_{}", uuid::Uuid::new_v4())
}

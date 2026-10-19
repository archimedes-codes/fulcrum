//! Shared agent-layer vocabulary types.
//!
//! Extracted from `session.rs` (Task 5 of the god-file decomposition). A
//! TRUE SIBLING of `session/` — the modules that used to import these items
//! from `session` (`subagent`, `worker/manager`) now import from here, which
//! is what breaks the module cycle (a child of `session/` would leave the
//! `subagent → session` / `worker/manager → session` edges intact).

/// Mint a fresh unique session ID with the `arch_` prefix.
pub fn mint_session_id() -> String {
    crate::agent::ids::SessionId::mint().into_string()
}

/// Mint the id stamp for ONE Worker's life within a Session (a **Session
/// incarnation**, ADR 0049). SIX lowercase hex chars, opaque: it is a
/// namespace, not a value anyone decodes — and being HEX-ONLY is what lets
/// `normalize::is_bookkeeping_message_id` match its bookkeeping WORDS
/// (`system`, `retract`) inside an id without ever matching a stamp: neither
/// word is spelled from the hex alphabet (`system` carries `s`/`y`/`t`/`m`;
/// `retract` is seven chars and carries `r`/`t`), so a stamp can neither BE one
/// of them nor hide one inside itself.
pub fn mint_incarnation() -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    id[..6].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mint_session_id_delegates_to_session_id_mint() {
        let s = mint_session_id();
        assert!(s.starts_with("arch_"));
        assert_eq!(s.len(), 5 + 36); // "arch_" + uuid v4 (36 chars)
    }
}

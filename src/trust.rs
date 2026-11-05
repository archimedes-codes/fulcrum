//! The TRUST lookup seam (ADR 0010/0025): `TrustSource` (the trait the
//! permission gate asks "is this Space trusted?") / `StaticTrustSource`
//! (the Worker — the flag arrives in the `start` envelope, updated by
//! `config` messages AND by the `trust-space` permission outcome — Task 2's
//! `WorkerCore`).
//!
//! The fail-closed semantics live in the sources themselves (a lookup
//! that cannot resolve is `false`); the `Option` at the `AgentLoop`
//! call site adds a second layer (`None` = always prompt).
//!
//! # Why this file is a SPLIT and not a move (T5/S5a, SEAM 1)
//!
//! The desktop's `harness/trust.rs` held TWO implementations beside the
//! trait, and only one of them can cross the package boundary:
//!
//!   * [`TrustSource`] and [`StaticTrustSource`] are pure `std` — a
//!     `Mutex<bool>` — so they live here, which is what lets the crate's
//!     permission gate (`crate::permission`) name the trait at all.
//!   * `SqliteTrustSource` holds the desktop's `storage::Db` and CANNOT
//!     move: `rusqlite` is deliberately not in this crate's graph (the
//!     status file's Step B-bis ruling — `DbError` wraps `rusqlite::Error`),
//!     and a crate that reached `storage::Db` would reach `archimedes_lib`,
//!     which is the exact package cycle T5 exists to sever.
//!
//! So the desktop keeps `SqliteTrustSource` as an implementation of THIS
//! trait — the same shape `web/render.rs` (S2) and `provider_headers` (S3)
//! landed, and the same direction the seams run: the crate declares the
//! need, the desktop satisfies it. Desktop spelling stays
//! `crate::agent::harness::trust::SqliteTrustSource` via that module's
//! re-export, so no consumer needed an edit.
//!
//! The pin that keeps that split honest is the desktop's
//! `tests/fulcrum_seams.rs::sqlite_trust_source_satisfies_the_crate_trait`,
//! plus the relocated `sqlite_trust_source_reads_the_space_flag` — an impl
//! nobody instantiates proves nothing (the S4 ruling), so both construct
//! the type rather than compare signatures.

use std::sync::{Arc, Mutex};

/// The TRUST lookup seam (ADR 0010/0025): `SqliteTrustSource` (the
/// current `db.space_trusted` lookup — the gate's use (a)) /
/// `StaticTrustSource` (the Worker — the flag arrives in the `start`
/// envelope, updated by `config` messages AND by the `trust-space`
/// permission outcome — Task 2's `WorkerCore`).
pub trait TrustSource: Send + Sync {
    /// Is the Space at `cwd` trusted? Fail-closed: a lookup that cannot
    /// resolve is `false` (untrusted — the gate prompts).
    fn is_trusted(&self, cwd: &std::path::Path) -> bool;
}

/// The static trust source (the Worker — the flag arrives in the `start`
/// envelope, updated by `config` messages AND by the `trust-space`
/// permission outcome): a single `bool` behind a `Mutex` (the `config`
/// `{ trusted }` update + the `trust-space` outcome flip). `Clone`
/// shares the flag (the `Arc`) — a clone observes flips.
#[derive(Clone)]
pub struct StaticTrustSource(Arc<Mutex<bool>>);

impl StaticTrustSource {
    pub fn new(trusted: bool) -> Self {
        Self(Arc::new(Mutex::new(trusted)))
    }

    /// The `config { trusted }` update + the `trust-space` outcome flip.
    pub fn set(&self, trusted: bool) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = trusted;
    }
}

impl TrustSource for StaticTrustSource {
    fn is_trusted(&self, _cwd: &std::path::Path) -> bool {
        *self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::{StaticTrustSource, TrustSource};
    use std::sync::Arc;

    /// `StaticTrustSource` — `new(false)` is untrusted; `set(true)`
    /// flips it (the `config { trusted }` update + the `trust-space`
    /// outcome flip); `new(true)` is trusted from the start (the
    /// `start` envelope's `trusted` flag).
    #[test]
    fn static_trust_source_new_and_set_flip_the_flag() {
        let s = StaticTrustSource::new(false);
        assert!(!s.is_trusted(std::path::Path::new("/whatever")));
        s.set(true);
        assert!(
            s.is_trusted(std::path::Path::new("/whatever")),
            "set(true) flips it"
        );
        s.set(false);
        assert!(
            !s.is_trusted(std::path::Path::new("/whatever")),
            "set(false) flips it back"
        );

        let s = StaticTrustSource::new(true);
        assert!(s.is_trusted(std::path::Path::new("/whatever")));
    }

    /// `StaticTrustSource` is `Send + Sync` behind an `Arc` (the
    /// `AgentLoop` holds `Option<Arc<dyn TrustSource>>` — the trait
    /// bound requires it), and the flip is visible through the trait
    /// object.
    #[test]
    fn static_trust_source_is_the_trust_source_seam() {
        let s = Arc::new(StaticTrustSource::new(false));
        let s_dyn: Arc<dyn TrustSource> = s.clone();
        assert!(!s_dyn.is_trusted(std::path::Path::new("/x")));
        s.set(true);
        assert!(s_dyn.is_trusted(std::path::Path::new("/x")));
    }

    // The third test that lived here — `sqlite_trust_source_reads_the_space_flag`
    // — could NOT stay. It opens a real `storage::Db`, and (a) `#[cfg(test)]`
    // does not survive into an rlib so a desktop consumer could not reach a
    // crate-side copy anyway (Step A.4), and (b) this crate cannot name
    // `storage::Db` at all. It relocated VERBATIM to
    // `src-tauri/tests/fulcrum_seams.rs`, which is the side that sees both
    // halves; its siblings above, which touch only the crate's own seam,
    // moved with the file.
}

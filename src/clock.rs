//! The ONE wall-clock source for the engine and the desktop's persistence layer.
//!
//! ## Why this module exists (T5/S4, plan Step B-bis, last bullet)
//!
//! Two independent copies of "unix milliseconds" existed before this module:
//! `src-tauri/src/storage/db.rs`'s private `now_ms()` (which stamps every
//! `sessions.created_at`, `messages.created_at`, `native_messages.created_at`
//! and `spaces.*` write) and `src-tauri/src/agent/normalize.rs`'s `pub(crate)`
//! `now_ms()` (which stamps a `DisplayRow`'s `created_at`). Byte-identical
//! bodies, two definitions — and the persistence trait's `now_ms` method would
//! have been a THIRD. The Step B-bis ruling is explicit about the failure mode:
//! "delete the duplicate or the crate keeps a hidden second clock and the trait
//! is decorative".
//!
//! ## What "called THROUGH the trait" means once the crate owns the clock
//!
//! The trait method stays, and `impl FulcrumPersistence for Db` implements it
//! by delegating HERE. That is the honest shape, and it is worth stating why it
//! is not a cheat: a trait method cannot BE a clock. `now_ms` takes `&self` for
//! dynamism, not because a `Db` carries a clock — the SQLite connection has no
//! notion of time at all. A per-implementation clock would be worse, not better:
//! two `FulcrumPersistence` impls could then disagree about what time it is, and
//! the transcript's ordering would depend on which store answered. One function,
//! one epoch, one `SystemTime` call, is the invariant.
//!
//! So the de-duplication that Step B-bis actually asks for happens here (the
//! desktop's two copies are GONE, both delegate), and the trait method remains
//! the seam S5's `normalize.rs` reaches for the value — which is what keeps the
//! crate's own stamp and the store's stamp on one clock even when the caller is
//! behind `Arc<dyn FulcrumPersistence>` and cannot name `Db` at all.
//!
//! Behaviour is identical by construction: the body below is the body both
//! copies had, including the `unwrap_or(0)` on a clock that predates the epoch.

/// The current unix time in milliseconds.
///
/// `0` when the system clock is before the unix epoch (the pre-existing
/// `unwrap_or(0)` posture of both deleted copies — a clock that wrong is not
/// worth panicking over inside a persistence write).
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::now_ms;

    #[test]
    fn now_ms_is_after_the_2020_epoch_floor() {
        // 2020-01-01 in ms. A clock function that returns `0` because of a
        // unit mistake (seconds instead of millis, a `UNIX_EPOCH` subtraction
        // flipped the wrong way) fails here, which is the whole point of the
        // pin — the deleted duplicates had no test of their own.
        assert!(now_ms() > 1_577_836_800_000, "got {}", now_ms());
    }

    #[test]
    fn now_ms_never_goes_backwards() {
        let a = now_ms();
        let b = now_ms();
        assert!(
            b >= a,
            "{b} < {a}: a monotonic violation from one clock source"
        );
    }
}

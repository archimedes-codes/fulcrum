//! The `fulcrum serve` daemon (T9) — the long-lived worker supervisor.
//!
//! Every module the later T9 tasks fill is declared HERE, now, as a compiling
//! stub. That is deliberate: the module list is the one file Tasks 3-5 would
//! otherwise all have to edit, and a shared `lib.rs` touched by two children in
//! the same wave is the classic fan-in collision. Declaring them up front makes
//! the later wave disjoint.
//!
//! The stubs stay EMPTY or comment-only. A `todo!()` body compiles, and would
//! let a later task ship a function nobody noticed was unimplemented.

pub mod db; // Task 4
pub mod serve; // Task 5
pub mod transport; // Task 3
pub mod worker_factory; // Task 5

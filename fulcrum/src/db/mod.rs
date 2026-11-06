//! The daemon's own store (T9 Task 4): the connection, the migrations, the
//! persistence layer. STUB — empty on purpose (see `lib.rs`).
//!
//! `rusqlite` is legal in this member and forbidden in the engine — ADR 0091
//! keeps disk ownership out of `fulcrum-core`, the daemon owns the disk.

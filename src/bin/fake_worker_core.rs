//! The crate's own `fake_worker` fixture binary (S8b.1).
//!
//! A 3-line shim over `fulcrum_core::fixtures::worker::worker_main` — the body
//! lives in the lib so the desktop's `src-tauri/src/bin/fake_worker.rs` shim
//! shares it instead of duplicating 1,092 lines of fixture.
//!
//! # Why the name carries `_core`
//!
//! Load-bearing, not cosmetic. Two workspace members declaring a bin named
//! `fake_worker` collide on the OUTPUT FILENAME in the shared `target/debug/`:
//! cargo emits only a warning ("`… has the same output filename as the bin
//! target `fake_worker` in package …`", cargo#6313) and the last writer
//! silently wins, so a suite can exec the other package's fixture and pass.
//! Distinct names produce zero collisions. See `crate::fixtures` for the full
//! measured constraint list, including why the desktop's bin keeps the
//! unsuffixed name forever (`CARGO_BIN_EXE_` is package-local).

fn main() {
    fulcrum_core::fixtures::worker::worker_main();
}

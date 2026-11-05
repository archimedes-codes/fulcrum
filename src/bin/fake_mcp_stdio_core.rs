//! The crate's own `fake_mcp_stdio` fixture binary (S8b.1).
//!
//! A 3-line shim over `fulcrum_core::fixtures::mcp_stdio::mcp_stdio_main` —
//! the body lives in the lib so the desktop's
//! `src-tauri/src/bin/fake_mcp_stdio.rs` shim shares it instead of duplicating
//! 282 lines of fixture.
//!
//! # Why the name carries `_core`
//!
//! The same collision rule as `fake_worker_core`: two members with the same bin
//! name share one output file in `target/debug/`, cargo only warns, and the
//! last writer wins. See `crate::fixtures`.

fn main() {
    fulcrum_core::fixtures::mcp_stdio::mcp_stdio_main();
}

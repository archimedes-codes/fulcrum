//! The two process fixtures the crate's OWN tests spawn (S8b.1).
//!
//! # Why this lives in the crate's LIB, not in a `[[bin]]` or a dev-dep crate
//!
//! `crates/fulcrum-core` must be self-sufficient for its own fixtures, because
//! after T8b it is its own repository and the desktop is a downstream consumer:
//! a test that spawns a binary owned by a package that no longer exists fails
//! with `ENOENT`. Measured at `eb8ac18b` by copying this crate outside the
//! workspace: 0 compile errors, 1320 passed, **71 failed** — 66 of them because
//! `fake_worker` / `fake_mcp_stdio` are bins of the DESKTOP package.
//!
//! The bodies cannot live anywhere else, and every alternative was measured:
//!   * a `[[bin]]`'s source is not importable by another package, so sharing
//!     one body between the two packages forces a LIB item;
//!   * a `dev-dependencies` fixture crate is out — a `[[bin]]` cannot see
//!     dev-dependencies at all (verified: `error[E0463]: can't find crate for
//!     serde_json` when a bin tried);
//!   * `[[example]]`s get NO `CARGO_BIN_EXE_` variable (verified: `NOT SET`)
//!     and land in `target/debug/examples/`, which breaks both the desktop's
//!     7 `CARGO_BIN_EXE_fake_worker` tests and this crate's
//!     `target/debug/<bin>` convention;
//!   * a separate LOWER fixture crate is a CYCLE: `fake_worker` needs this
//!     crate's `protocol` / `events` / `harness::store` types, so this crate
//!     would need it as a REGULAR dependency for its own bin to compile.
//!
//! # The honest cost
//!
//! These bodies are NON-GATED lib code — `#[cfg(test)]` does not survive into
//! an rlib, and a `[[bin]]` cannot use a lib's test-only items — so they link
//! into every binary that depends on this crate, the desktop app included.
//! That is the price of one shared body; it is measured and reported, not
//! argued away. `#[doc(hidden)]` keeps it out of the public docs; it does NOT
//! keep it out of the link.
//!
//! # Why the bin NAMES differ
//!
//! The crate's bins are `fake_worker_core` / `fake_mcp_stdio_core`, NOT
//! `fake_worker` / `fake_mcp_stdio`. Two workspace members declaring the same
//! bin name collide on the OUTPUT FILENAME in the shared `target/debug/`;
//! cargo only WARNS ("may become a hard error in the future",
//! cargo#6313) and the LAST WRITER SILENTLY WINS — measured in a scratch
//! workspace, where `target/debug/fake_worker` printed `from A` and then
//! `from B` after building the other member. That is a fail-open of the worst
//! kind: a suite could exec the OTHER package's fixture and pass. Distinct
//! names produce zero collisions.
//!
//! The desktop keeps bins named `fake_worker` / `fake_mcp_stdio` forever:
//! `env!("CARGO_BIN_EXE_<name>")` is PACKAGE-LOCAL (a test in a dependent
//! package cannot read a dependency's bin — verified compile error), and 7
//! desktop integration tests resolve `CARGO_BIN_EXE_fake_worker` by that exact
//! name. Those 7 files need ZERO edits, before or after T8b.

// Both submodules are `#[doc(hidden)]` at the declaration site below: they are
// test fixtures, not engine surface. They are `pub` because the bins in
// `src/bin/` are separate crates and must reach them through the rlib.
#[doc(hidden)]
pub mod mcp_stdio;
#[doc(hidden)]
pub mod worker;

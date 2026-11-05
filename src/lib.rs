//! Fulcrum core — the agent engine extracted from `archimedes`'s `src-tauri/src/agent/`.
//!
//! T5 moves bodies here in ordered, gate-green sub-steps; as each moves,
//! `src-tauri/src/agent/mod.rs` re-exports it so the 20 outside-`agent/` files and
//! 23 integration tests keep compiling unchanged:
//!
//! ```text
//! pub use fulcrum_core::<symbol>;
//! ```
//!
//! (A fenced `text` block, not an indented one: rustdoc compiles an indented block in a
//! doc comment as a doctest, and `<symbol>` is a placeholder, not Rust — the literal
//! shape this file was drafted with failed `cargo test` with
//! `error: expected identifier, found <`.)

// ── T5/S1 — the pure leaf modules (ADR 0091, `docs/roadmap/` T5 plan Step A) ──
// Moved by `git mv` with every `mod`/`#[cfg(test)]` block verbatim; the desktop
// reaches them through the re-export seam in `src-tauri/src/agent/mod.rs`.
pub mod debuglog;
pub mod events;
pub mod fs_backend;
pub mod hostenv;
pub mod ids;
// `#[cfg(test)]` for the reason in the module's own docs: `fulcrum-core` has no
// `tests/` directory yet (T5/Step D keeps every integration test in the desktop),
// so an ungated copy would be `dead_code` in the plain lib build — which the
// gate's `clippy --all-targets -- -D warnings` rejects.
#[cfg(test)]
pub mod test_support;
pub mod todo;
pub mod types;

// ── T5/S2 — the `{tools, web, boundary}` cluster + its co-residents ──────────
// `git mv`'d with every `mod`/`#[cfg(test)]` block verbatim. The cluster is ONE
// unit because `tools ↔ web` and `tools ↔ boundary` are genuine 2-cycles (no
// member compiles alone); `skills`/`agents`/`roots` ride with it because
// `boundary.rs` imports both `skills::{user_roots,space_roots}` and
// `agents::{user_roots,space_roots}`, `tools/types.rs` names `skills::SkillScope`,
// and both of those reach `roots::space_roots_in` — an S2 without them does not
// compile (the status file's MAJOR finding #2).
//
// `web/` is the SPLIT directory: the crate owns `cache`, `config`, `extract`,
// `net`, `providers`, `tools` and NOT `render` (the Tauri webview stays
// desktop). See `src-tauri/src/agent/web.rs` for the desktop half and why the
// two cannot be one module tree.
pub mod agents;
pub mod boundary;
pub mod childproc;
pub mod definition_permission;
pub mod policy;
// `pub(crate)`, NOT `pub`: `roots` is the shared walk-up helper behind
// `skills::space_roots`/`agents::space_roots` and NOTHING outside the crate names
// it (measured: `crate::roots::space_roots_in` appears only in `skills.rs:241`
// and `agents.rs:254`, both of which moved here). The desktop declared it
// `pub(crate) mod roots` too, so this preserves the original surface rather than
// widening it across the package boundary.
pub(crate) mod roots;
pub mod skills;
pub mod tools;
pub mod web;

// ── T5/S3 — the one-way dependents: `interactive`, `codemode`, `mcp` ────────
// `git mv`'d with every `mod`/`#[cfg(test)]` block verbatim. These three depend
// on S2's cluster one-way (`mcp→tools` 1, `interactive→tools` 4) and nothing in
// the crate depends on them for this step except the desktop `harness` (moved
// in S5) and the desktop `worker` (S8.1 opened that directory — see the T8
// block at the bottom of this file).
//
// `provider_auth` is HERE now (T7/S7.1 + S7.2) — see the module's own docs and
// the block below. The S3 RULING that kept it out measured the crate's need at one
// item (`headers::expand_map`) and zero consumers for every other submodule;
// that was true of T5's consumers and false of T9's daemon, which must
// authenticate providers itself (`status.md` RE-SCOPE). The seam S3 cancelled
// with it (`ProviderSource`) landed in S7.2 — `budget.rs` moved, and with it the
// two `load_settings` edges that justified the seam — so the crate now owns
// SEVEN of the desktop's nine files in that directory, and reads no settings
// file to do it.
pub mod codemode;
pub mod interactive;
pub mod mcp;
/// The one file that moved out of the desktop's `provider_auth/` directory
/// BEFORE T7 did the rest of it. Named for what it IS — one file of that
/// directory, not the directory — because when it moved (T5/S3) it was the ONLY
/// part the crate needed, and the crate did not own `provider_auth`. That is no
/// longer the reason for the name (T7/S7.1 owns six more files under
/// `provider_auth`, above), but the name stays: renaming it would churn the
/// desktop's `pub use ... as headers` alias and every
/// `crate::agent::provider_auth::headers::{expand_map, validate_base_url}`
/// spelling in `session/mod.rs`, `commands/settings.rs` and
/// `tests/provider_headers.rs` for zero behavioural gain.
pub mod provider_headers;

/// `LaunchConfig` — the per-dispatch launch DTO, which moved ahead of the module
/// it lives beside (`subagent.rs`, S5) because `interactive` (S3) imports it.
/// Pure data; the E.1 wire-type pattern rather than a seam. See the module docs.
pub mod launch_config;

// ── T5/S4 — the seams, and the types they need in order to be DECLARED ──────
// S4 moves no module bodies. It defines the crate's two traits (`seams`) and
// moves the DATA they must name — which turned out to be more than the plan's
// list, because a package boundary checks signatures rather than bodies:
// `resolve_composed_model`'s signature drags `Model`/`ModelCatalog` (and their
// `ModelKey`/`WireApi` references) across for E4, and `StartEnv`'s fields drag
// `ChatMessage`, the config DTOs, `DisplayRow` and `AgentNameResolution` across
// for E1/E3. None of it is S5 arriving early: `harness`'s loop, launch, dispatch,
// prompt, providers and stores are all still desktop, and every file that moved
// has ZERO production edges to them (measured, and recorded per module).
//
// `catalog` is a SPLIT, not a move — the data types here, the network discovery
// desktop — the same shape §2c applied to `config.rs`.
pub mod catalog;
pub mod clock;
pub mod config_types;
pub mod model_key;
pub mod provider_overflow;
pub mod provider_types;
pub mod seams;
pub mod store_rows;
pub mod subagent_types;
// ── T5/S5a — the two production seams S5b's move depends on ─────────────────
// `trust` is a SPLIT, not a move: the trait + the pure `StaticTrustSource`
// are here, `SqliteTrustSource` stayed desktop (it holds a `storage::Db`,
// which this crate must never name — `rusqlite` is not in its graph). The
// desktop's `harness/trust.rs` implements the trait and re-exports both
// spellings so every pre-S5a path resolves unchanged.
//
// `permission` is the module S2's scope named and S2's commit did not move
// (`git show --name-status 27e367cd`: one rename, not two). It moves here
// because `harness/loop.rs` and `harness/loop/gate.rs` name it and they move
// in S5b, so it cannot stay desktop. Its production dependency on the
// desktop was exactly ONE item — `harness::trust::TrustSource` — which is
// why `trust` had to be split first. Its `Db`-backed tests relocated to
// `src-tauri/tests/permission_gate_db.rs` (A.4: `#[cfg(test)]` does not
// survive into an rlib, and the crate cannot reach `storage::Db` at all).
pub mod permission;
pub mod trust;
pub mod wire;
pub mod worker_types;

/// The crate's seam traits, re-exported at the root so the desktop can name them
/// without reaching into the module (`fulcrum_core::{Dispatcher, FulcrumPersistence,
/// StoreError}`). `mcp::OpenRequester` is NOT here — it stayed beside its only
/// consumer in S3, and moving its spelling now would churn three working call
/// sites for nothing.
/// The crate's boundary pins (Step C's `load_settings` grep, the §2c-bis pin, and
/// the S4 gate's "no `tauri` reference" test). `#[cfg(test)]` because they are
/// tests over the crate's own source and manifest — nothing at runtime needs them,
/// and the module names the forbidden identifiers in its own scan strings.
#[cfg(test)]
mod boundary_pins;

// ── T5/S5b — THE MOVE: the harness, and the three files that reference it ───
//
// `harness` is the last piece of the closure and it moves as ONE unit: `loop`
// / `launch` / `dispatch` / `provider` / `prompt` / `tool_specs` / `catalog`
// reference each other in a tight cycle (the S5 ruling: "moves as one"), and
// `normalize` / `persist` / `subagent` ride with it because they reference
// `harness` (`normalize.rs` imports `harness::store::DisplayRow`,
// `subagent.rs` imports `harness::{Model, ModelKey}`) and are referenced by it.
//
// The two files the DESKTOP keeps beside this directory are the two that hold a
// `storage::Db`: `harness/store.rs` (`SessionStore`) and `harness/trust.rs`
// (`SqliteTrustSource`). Both implement crate traits and re-export the crate
// halves, so `crate::agent::harness::store::SessionStore` and
// `crate::agent::harness::trust::SqliteTrustSource` keep resolving — the S2
// `web/render.rs` shape, one level down inside `harness`.
//
// After this step the crate has ZERO production edges to the desktop: the last
// four (`config`, `storage`, `worker`, the stale `types` path) were closed by
// moving the DTOs, splitting `Store`, taking `Arc<dyn FulcrumPersistence>` and
// holding `Arc<dyn Dispatcher>`. `boundary_pins::
// no_code_reaches_a_desktop_module` is the proof, not this comment.
pub mod harness;
pub mod normalize;
pub mod persist;
pub mod subagent;

// ── T7 — the provider credential machinery (7 files + 3 free functions) ─────
// S7.1 moved `store`, `key_helper`, `gcloud`, `cli_sso`, `litellm_oauth`, `oidc`
// and the `mod.rs` trio (`gcloud_token_auth_enabled`, `resolve_bearer`,
// `resolve_bearer_at`); S7.2 moved `budget.rs`. All by `git mv` into
// `provider_auth/`. The desktop keeps TWO files of that directory — `refresh.rs`
// (it holds `tauri::AppHandle` + `session::SessionManager`) and `headers.rs`
// (moved early, in S3, as `provider_headers`, above) — and its
// `agent/provider_auth/mod.rs` is a re-export shell that declares the staying
// module and re-exports this one, so every desktop spelling resolves.
//
// `budget.rs` is the file that needed a SEAM rather than only a move: it re-read
// the provider rows from `settings.json` on every poll tick. That read is now
// [`provider_auth::budget::ProviderSource`], implemented desktop-side by the
// shell's `SettingsProviderSource`, so the crate gets the ROWS and never the
// FILE — and `BudgetPoller` no longer holds a settings directory at all. See
// `seams`' module docs for why this seam was cancelled in S3 and correct now.
//
// `gcloud.rs:8` names `src-tauri/tests/provider_gcloud_adc.rs` in a `//!` line.
// That is a DOC mention of a desktop TEST path, not a code edge: the crate never
// names `tauri`, `rusqlite` or `archimedes_lib` here, and
// `boundary_pins::no_code_names_the_desktop_ui_framework` strips comments
// before it scans, which is what makes that line legal rather than lucky.
pub mod provider_auth;

// ── T8 — the Worker engine (S8.1 → S8.3, bottom-up on dependency) ───────────
// S8.1 took the three files nothing else in `worker/` compiles without: the wire
// vocabulary (`protocol`) and the two Worker-side trait implementations
// (`store`'s `IpcStore`, `sink`'s `IpcEventSink`). S8.2 added the composition
// point (`core` + `dispatch`) and moved `run_worker_inner` here with the two
// injected seams — the browser opener and the crash panic hook — because
// `open.rs` is `tauri` and `crashlog.rs` stays desktop (ADR 0091: the shape
// crosses, the substance stays). S8.3 added the Supervisor-side half: `client`
// and the WHOLE `manager/` directory (`mod`, `pump`, `registry`, `subagent_flow`,
// `tests`) — `manager/mod.rs` declares `subagent_flow` and `pump`/`tests` import
// it through `super::`, so the directory cannot be split across the boundary.
//
// Every `worker/` file is HERE now. The desktop keeps a re-export shell at
// `src-tauri/src/agent/worker/mod.rs` (module aliases + the named surface), so
// `session/mod.rs`, `lib.rs`, `commands/**`, `bin/fake_worker.rs` and the 11
// integration targets that name `archimedes_lib::agent::worker::…` all resolve
// with zero edits. S8.4 deleted the shell's last definition — the zero-argument
// `run_worker_inner()` wrapper — so the shell is now PURELY re-exports, and the
// desktop's `lib.rs::run_worker` calls `worker::run_worker` below directly,
// supplying the two injected arguments itself. That hand-off (desktop → crate)
// is the direction ADR 0091 allows; the reverse is what the boundary pins catch.
//
// `provider_auth` is no longer absent either — T7/S7.1 moved six of its files
// (declared above, in the T7 block). `worker` is no longer absent: this line is
// where it arrived.
//
// T8/S8.5 EXTRACTION RESULT (the dry-run T8b consumes). The crate was copied OUT
// of the workspace and built as its own package: it COMPILES standalone,
// `--all-targets` clean, and all 9 `boundary_pins` pass there — no `crate::`
// path, no manifest line, and no `test_support` helper reaches the desktop. What
// does NOT survive is TEST FIXTURES, not code.
//
// S8b.1 RE-MEASURED THE FAILURES (the numbers below are the S8b.1 run at
// `eb8ac18b`: **0 compile errors, 1320 passed, 71 failed, 1 ignored** — S8.5
// reported 1231/69 from a run that reached fewer targets, so the split is
// restated from the re-measurement rather than transcribed):
//   * 66 spawn `fake_worker` / `fake_mcp_stdio`, which were BINS OF THE DESKTOP
//     PACKAGE (`src-tauri/src/bin/`) and so did not exist next to an extracted
//     crate — `ENOENT` at the spawn, or the `is_file()` witness in the two
//     fixture-path guards;
//   * 4 are `childproc`'s census pins, which walk BOTH packages' `src/` trees on
//     purpose (a one-root walk reports a clean bill for the half it never
//     opened) and panic naming the missing root rather than scanning nothing;
//   * 1 is `childproc::target_debug_dir_is_where_cargo_actually_builds`, which
//     S8.5 did not separate out: the resolver itself assumed the crate is a
//     workspace MEMBER (it walked the manifest up TWO levels), so in a standalone
//     copy it named a directory no build writes. This one was NOT a fixture
//     problem and renaming the bins did not fix it — S8b.1 had to move the
//     resolver to `current_exe()`.
// ALL 66 FIXTURE FAILURES ARE GONE (measured after S8b.1: 1387 passed, 4 failed).
// The 4 that remain are the census pins, LEFT FAILING ON PURPOSE — that is
// S8b.2's job, and a pin that quietly no-ops when its root is missing is the
// fail-open this design exists to avoid. See `crate::fixtures` for where the two
// bins live now and why the crate's carry a `_core` suffix.
pub mod worker;

// S8b.1 — the two process fixtures this crate's tests spawn, as LIB code (not
// bins, not a dev-dep crate, not examples: every alternative was measured and
// fails; see the module docs). The crate's `src/bin/fake_worker_core.rs` and
// `src/bin/fake_mcp_stdio_core.rs` are 3-line shims over these bodies, and so
// are the desktop's `src-tauri/src/bin/{fake_worker,fake_mcp_stdio}.rs` — one
// body, four bins, and FOUR DISTINCT BIN NAMES so nothing collides on an output
// filename in the shared `target/debug/`.
//
// NON-GATED ON PURPOSE: `#[cfg(test)]` does not survive into an rlib, so a bin
// (a separate crate) could not reach a test-gated item. That is why this code
// links into the desktop binary, and the size cost of it is measured and
// reported rather than argued away. `#[doc(hidden)]` on both submodules keeps
// them out of the public docs. This module is NOT the test-support module
// below (`test_support`, `#[cfg(test)]`, in-process helpers) — the two stay
// separate because one ships and one does not.
#[doc(hidden)]
pub mod fixtures;

// S8.4: the crate's Worker-mode entry, re-exported at the root so
// `fulcrum_core::run_worker()` is the literal spelling. Both parameter types
// are crate types (`Arc<dyn crate::mcp::OpenRequester>` and
// `Option<Arc<dyn Fn(PathBuf) + Send + Sync>>`), so the re-export keeps the
// crate standalone — nothing desktop is named.
pub use worker::run_worker;

pub use seams::{Dispatcher, FulcrumPersistence, StoreError};

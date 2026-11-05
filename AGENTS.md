# AGENTS.md

`fulcrum-core` — the agent engine extracted from
[`archimedes-codes/desktop`](https://github.com/archimedes-codes/desktop): the tool loop, provider clients, tools
and sandbox, the MCP client, and the out-of-process worker. A library. It knows nothing about windows, trays, or
the UI that renders it.

## The one rule that defines this crate

**No code path from here to a desktop module.** Not a convention — `src/boundary_pins.rs` walks every `.rs` file in
the crate and fails on a single reference to `tauri::`, `tauri_plugin_*`, `rusqlite::`, `archimedes_lib::`,
`crate::config::`, `crate::storage::`, or `crate::agent::`. Comment lines are stripped first, so it reads code, not
prose about code. It does **not** skip `#[cfg(test)]` — a test-only import is still an edge.

What the engine needs from the desktop arrives as a trait the desktop implements (`seams.rs`,
`provider_auth::ProviderSource`, `harness::Store`). Adding a direct dependency the other way is the thing this
crate exists to prevent; if you are reaching for it, the answer is a seam or data that moves here.

## Validation bar

The toolchain is pinned in `rust-toolchain.toml`; plain `cargo` uses it, and CI installs from that file rather than
naming a version. Do not float it — a newer rustc adds lints, and a lint CI sees that a local toolchain cannot
reproduce reddens `main`.

```bash
cargo build --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo check --target x86_64-pc-windows-gnu --all-targets   # the only local Windows proof available
```

CI runs the full bar on **ubuntu, windows and macos**. A platform that has never been compiled is an untested
platform: this crate carries deliberate `#[cfg(unix)]` / `#[cfg(target_os = "linux")]` process hygiene, and two
real Windows-only bugs were previously caught only by the CI leg.

## Things that will bite you

**`tests/fixture_bins.rs` is part of the build contract, not a test that happens to be here.** Cargo writes a
package's binaries to the top of `target/debug/` only when it builds an integration-test target, and many tests spawn
the fixture binaries **by path** (a lib's unit tests get no `CARGO_BIN_EXE_*`). Delete that file and a cold
`cargo test` fails with `ENOENT` — **66 failures, re-measured by deleting this file and running a cold suite**.
**Do not "fix" this by
adding a `cargo build --all-targets` step to CI** — that would keep the suite green after the file was deleted,
re-arming the exact trap. Measured: bare `cargo test` on a target dir that never existed passes the whole suite, so no
build step is needed.

**The two fixture bins are `fake_worker_core` and `fake_mcp_stdio_core`, and the `_core` suffix is load-bearing.**
The desktop package builds the same bodies under the unsuffixed names. Two workspace members declaring a bin of the
same name collide on the output filename in a shared `target/debug/` — cargo only *warns* and **the last writer
silently wins**, so a suite can spawn the other package's fixture and never notice. The bodies live in
`src/fixtures/` as ordinary lib code; each bin is a three-line `main`.

**`Path::is_file()` does not append `EXE_SUFFIX`; `CreateProcessW` does.** A fixture path built without the suffix
spawns fine and fails an existence check. `test_support::fixture_bin()` owns the suffix; `target_debug_dir()` names a
*directory* and must not append it.

**`env!("CARGO_MANIFEST_DIR")` depth changes between layouts** — this package is the workspace root here and was a
member inside the desktop. Never write a level-counting `.parent()` path. `target_debug_dir()` resolves from
`current_exe()` for exactly this reason.

**A source-scanning guard does not distinguish code from prose about code.** `production_region`'s own doc comment
once contained `Command::new(` and would have tripped the census's own "one `Command` constructor" invariant when the
helper moved into a non-`tests/` module. `boundary_pins` obfuscates its own needles for the same reason.

**Two scanners, not three.** `boundary_pins::strip_comments` (drops comments, whole file) and
`lint_scan::production_region` (truncates at the first item-position `#[cfg(test)]`) are **not interchangeable** —
`harness/loop.rs` carries a `#[cfg(test)]` on a `use` at line 54, so its production region is 53 lines and contains
neither `McpManager::new` site. Picking the wrong one silently changes what is policed.

## The census (`childproc`)

A scan over production source enforcing the child-process invariants: exactly one `Command` constructor, no raw
command-line appends, process-group kills in one place, no data-dir resolution in the crash-log path. It is
**layout-aware** — it works as a workspace member and standalone, decided by two independent witnesses (an ancestor
`[workspace]` manifest that must *name this package*, plus a filesystem walk for a sibling desktop tree), because
every desktop-conditional floor **vanishes rather than fails** when the discriminator is wrong. Its vacuity floors are
**per root**: an aggregate floor stayed green while one root was entirely blind.

A pin that passes on the broken thing is worse than no pin. When you change one, prove it with a mutation that still
compiles — hoist a value into a local, don't delete it — and report if you cannot.

## Release profile

`[profile.release]` is copied from the desktop workspace root because cargo reads profiles from the workspace root
only; a standalone build silently gets defaults without it. `panic = "unwind"` is the ADR 0025 consequence — a Worker
panic stays in the Worker — and must never become `abort`.

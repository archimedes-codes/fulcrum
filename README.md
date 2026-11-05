# The engine that runs an Archimedes agent session.
![](https://img.shields.io/badge/rust-1.99-pin?style=flat-square)
![](https://img.shields.io/badge/license-MIT-blue?style=flat-square)

`fulcrum-core` is the agent harness: the tool loop, the provider clients, the tool
implementations, the sandbox, and the MCP client. It is a library, not an
application — it decides what an agent does, and knows nothing about windows,
trays, or the UI that shows it.

It was extracted from
[`archimedes-codes/archimedes`](https://github.com/archimedes-codes/archimedes)
(the desktop app, renamed from `archimedes-codes/desktop` after this extraction),
which is its first and primary consumer. That extraction is what makes the same engine
runnable somewhere the desktop is not: a headless daemon on a socket, a worker on
another machine, a test harness that drives a session without a GUI.

## Why it is a separate crate

The desktop is a Tauri app, and the engine spent its whole life inside it. That
was never a boundary the code respected — until it was made one deliberately.

The rule now enforced by tests, not convention: **`fulcrum-core` has no code path
to a desktop module.** Nine boundary pins assert it, and the main one walks every
`.rs` file in the crate and fails on a single reference to `tauri::`,
`rusqlite::`, `crate::config::`, `crate::storage::`, or `archimedes_lib::`
(comment lines are stripped first, so the pin reads code, not prose). What the
engine cannot do itself arrives as a trait the desktop implements:

- **`ProviderSource`** (`provider_auth/budget.rs`) — the engine asks for provider
  configuration and per-session budget; it never opens the settings file or the
  credential store.
- **`FulcrumPersistence`** and **`Dispatcher`** (`seams.rs`) — durable state and
  subagent dispatch, handed back to whoever owns them.
- **`Store`** (`harness/store.rs`) — the frame/row persistence the engine needs, as
  a trait, so the storage layer stays where it lives.

Filesystem and process effects are *inside* this crate (`fs_backend`, `childproc`),
not a seam — they are the engine's own job. `rusqlite` is not a dependency of this
crate, and a pin fails the build if it ever is.

## Build

The toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml) — plain
`cargo` uses it, and CI installs from the same file rather than naming a version.

```bash
cargo build --all-targets
cargo test
```

> [!IMPORTANT]
> **Build before you test.** Cargo writes this package's fixture binaries to
> `target/debug/<name>` only when it builds an integration-test target, and 66 of
> the crate's tests spawn those binaries *by path* (a lib's unit tests get no
> `CARGO_BIN_EXE_*`). On a cold target dir, a bare `cargo test` fails 66 tests
> with `ENOENT`. `cargo build --all-targets` first, or let `cargo test` build the
> whole graph — and note that `tests/fixture_bins.rs` is what
> makes that target exist, so it is part of the build contract, not a test that
> happens to be here.

## The fixture binaries

The engine spawns two out-of-process test fixtures, and both are built from this
package:

| Binary | Speaks | Used for |
| --- | --- | --- |
| `fake_worker_core` | the Worker protocol (`hello`/`ready`, frames, `close` → exit 0, a `prompt` of `__crash__` → exit 137) | worker/supervisor lifecycle tests |
| `fake_mcp_stdio_core` | JSON-RPC over stdio, three canned tools | MCP client tests |

Their bodies live in `src/fixtures/` as ordinary library code, and each binary is
a three-line `main` over one of them. The desktop package builds its own copies
under the unsuffixed names `fake_worker` / `fake_mcp_stdio`, because
`CARGO_BIN_EXE_<name>` is package-local and the desktop's integration tests
resolve them by that name.

> [!WARNING]
> Those two packages must not declare a bin of the **same name**. Cargo only
> *warns* about the output-filename collision in a shared `target/debug/`, and the
> last writer silently wins — so a test suite can end up spawning the other
> package's fixture. That is why the crate's names carry `_core`.

## Tests

`cargo test` runs the crate's own suite. Two of its guards are worth knowing about
before you trip them:

- **`boundary_pins`** — the dependency-direction pins described above. It does not
  skip `#[cfg(test)]` code, because a test-only import is still an edge.
- **`childproc`'s source census** — a scan over the crate's production source that
  enforces the child-process invariants (one `Command` constructor, no raw command
  lines, process-group kills in one place). It is layout-aware: it works both as a
  workspace member and as a standalone package, and it **fails loudly** rather than
  quietly policing less when it cannot resolve a root.

## Consuming it

```toml
[dependencies]
fulcrum-core = { git = "https://github.com/archimedes-codes/fulcrum", rev = "<pinned-rev>" }
```

The engine is pre-1.0 and moves with its consumer; pin a `rev`, not a branch.

For working on the engine and its consumer at the same time, put a `[patch]` in the
**consumer's** `.cargo/config.toml` (not here — a patch on the dependency's own side
does nothing for its consumers):

```toml
# <consumer>/.cargo/config.toml
[patch."https://github.com/archimedes-codes/fulcrum"]
fulcrum-core = { path = "../fulcrum" }
```

## Layout

```
src/
  harness/     the tool loop, prompts, compaction, subagent dispatch —
               and provider/ inside it: the provider clients and wire adapters
  provider_auth/ credentials, OAuth/ADC flows, per-session budgets
  tools/       tool implementations and the sandbox
  mcp/         MCP client: transports, manager, tool bridging
  worker/      the out-of-process worker: protocol, client, supervisor
  codemode/    the in-sandbox JS runtime (rquickjs) and its memory cap
  web/         the web_search / fetch tools and their search providers
  interactive/ the in-process ask / sudo / todo channel the loop calls directly
               (not over a socket) — the user-paced oneshots and the sudo run flow
  fixtures/    the bodies behind the two fixture binaries
  bin/         the fixture binaries' three-line mains
  seams.rs     the traits the desktop implements
  boundary_pins.rs   the dependency-direction pins
  childproc.rs       the child-process census (layout-aware, per-root floors)
  source_lints.rs    structural lints over this crate's own source
  lint_scan.rs       the shared source-region scanner those lints use
tests/
  fixture_bins.rs   builds the fixture binaries; see the note above
```

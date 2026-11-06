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
runnable somewhere the desktop is not: a headless daemon on a socket — the `fulcrum`
member described next, a stub today — a worker on another machine, a test harness
that drives a session without a GUI.

## The two members of this repo

This repo is a Cargo workspace with two members:

- **`fulcrum-core`** — the engine above. Its `[package]` lives at the **repo root**,
  with `src/` and `tests/` beside it. That is not laziness; the shape is forced by
  the `childproc` census and moving it is fatal. `AGENTS.md` carries the finding and
  the reproduction.
- **`fulcrum`** — the `fulcrum serve` daemon: a long-lived supervisor that owns the
  SQLite file and multiplexes clients onto supervised worker processes
  ([`docs/roadmap/fulcrum-daemon.md`](docs/roadmap/fulcrum-daemon.md), ADR 0091).
  **It is a stub right now.** Its module tree is declared and compiles — `db/`,
  `transport/`, `serve`, `worker_factory` — and `fn main` is empty. There is no
  listener, no database, and no supervisor loop yet, so nothing about running
  `fulcrum serve` works today. It is a member rather than a consumer of the rev pin
  so it links the engine by `path = ".."` and can never drift from it.

`fulcrum-core`'s rules still bind `fulcrum-core` alone: the boundary pins do not walk
the sibling member (see *Tests* below).

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
cargo test          # both members; the engine's suite needs no build step first
```

> [!IMPORTANT]
> **`tests/fixture_bins.rs` is part of the build contract, not a test that happens
> to be here.** Cargo writes a package's binaries to `target/debug/<name>` — rather
> than only `target/debug/deps/<name>-<hash>` — when it builds an integration-test
> target, and `tests/fixture_bins.rs` is that target for this package. 66 of the
> crate's tests spawn those two binaries *by path* (a lib's unit tests get no
> `CARGO_BIN_EXE_*`), so the file's existence is what puts the binaries where those
> tests look.
>
> **A bare `cargo test` passes from a cold target dir — do not build first.**
> Measured here on a `target/` that had never existed, no build step: 1406 passed /
> 0 failed / 1 ignored — the same total from `cargo test -p fulcrum-core` (1401 lib +
> the 5 in `fixture_bins.rs`) and from bare `cargo test`, which also runs the
> `fulcrum` member's targets (0 tests there today). The 66 `ENOENT` failures this box
> used to promise on a cold dir are what **deleting `tests/fixture_bins.rs`** does;
> measured the same cold tree with that file removed: 1335 passed / 66 failed, 50 `No
> such file or directory` messages.
>
> So CI has no `cargo build --all-targets` step before the tests, deliberately, and
> adding one is the way to make this trap silent: a build step materialises the bins
> regardless, so the suite would stay green after the file was deleted. The failure
> is meant to be loud. See the note at `.github/workflows/ci.yml:60`.

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
> package's fixture. That is why the crate's names carry `_core`. The same rule
> applies inside this workspace: the `fulcrum` member's bin is `fulcrum`, and it must
> never be renamed to either fixture name, because both members land in one
> `target/debug/`.

## Tests

`cargo test` runs both members (the root package and `fulcrum` — see *Things that
will bite you* in `AGENTS.md` for why that is not the default). Two of the engine's
guards are worth knowing about before you trip them:

- **`boundary_pins`** — the dependency-direction pins described above. It does not
  skip `#[cfg(test)]` code, because a test-only import is still an edge. It walks
  `{CARGO_MANIFEST_DIR}/src` and reads `{CARGO_MANIFEST_DIR}/Cargo.toml`, so it
  polices `fulcrum-core` **only**: `fulcrum/src/` is outside the walk and
  `fulcrum/Cargo.toml` is never read. That is intended for now (`rusqlite` is legal
  in the daemon, which owns the disk) and it is also a gap — `AGENTS.md` records
  both, and T9 Task 6 adds the daemon's own pins.
- **`childproc`'s source census** — a scan over the crate's production source that
  enforces the child-process invariants (one `Command` constructor, no raw command
  lines, process-group kills in one place). It is layout-aware: it works both as a
  workspace member and as a standalone package, and it **fails loudly** rather than
  quietly policing less when it cannot resolve a root. Being layout-aware, it is the
  reason this repo's layout is fixed — `AGENTS.md` has the finding.

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

The workspace root **is** the `fulcrum-core` package — `[package]`, `src/` and
`tests/` all sit here, beside the `[workspace]` table. Do not "tidy" that by moving
them into a `fulcrum-core/` subdirectory; `AGENTS.md` records what breaks, and how to
reproduce it, under *The census (`childproc`)*.

```
Cargo.toml        [workspace] + fulcrum-core's [package] (the forced shape)
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
  fixture_bins.rs   the integration-test target that makes cargo materialise the
                    fixture binaries — the build contract, see the note above
fulcrum/            the `fulcrum serve` daemon (STUB: modules declared, no behaviour)
  Cargo.toml        its own manifest; rusqlite is legal here, and boundary_pins
                    does not read this file
  src/
    lib.rs          declares every module T9's later tasks fill
    main.rs         `fn main` — empty until Task 5
    db/             Task 4: the WAL-mode fulcrum.db behind FulcrumPersistence
    transport/      Task 3: UDS on unix, Named Pipe on Windows, JSONL frames
    serve.rs        Task 5: listener, accept loop, shutdown
    worker_factory.rs Task 5: the supervised worker per session
```

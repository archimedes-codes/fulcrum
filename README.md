# The engine that runs an Archimedes agent session.
![](https://img.shields.io/badge/rust-1.99-pin?style=flat-square)
![](https://img.shields.io/badge/license-MIT-blue?style=flat-square)

`fulcrum-harness` is the agent harness: the tool loop, the provider clients, the tool
implementations, the sandbox, and the MCP client. It is a library, not an
application — it decides what an agent does, and knows nothing about windows,
trays, or the UI that shows it.

It was extracted from
[`archimedes-codes/archimedes`](https://github.com/archimedes-codes/archimedes)
(the desktop app, renamed from `archimedes-codes/desktop` after this extraction),
which is its first and primary consumer. That extraction is what makes the same engine
runnable somewhere the desktop is not: a headless daemon on a socket — the `fulcrum`
member described next — a worker on another machine, a test harness that drives a
session without a GUI.

## The two members of this repo

This repo is a Cargo workspace with two members:

- **`fulcrum-harness`** — the engine above. Its `[package]` lives at the **repo root**,
  with `src/` and `tests/` beside it. That is not laziness; the shape is forced by
  the `childproc` census and moving it is fatal. `AGENTS.md` carries the finding and
  the reproduction.
- **`fulcrum`** — the `fulcrum serve` daemon (ADR 0091): a long-lived supervisor that
  owns the SQLite file and multiplexes clients onto supervised worker processes. It
  runs. One binary, two modes — `fulcrum serve` is the supervisor, `fulcrum --worker`
  is the child it spawns, which is a self-exec of the same executable (that is what
  makes the daemon's default `--worker-exe` correct without guessing a path):

  ```
  fulcrum serve [OPTIONS]     --socket <PATH> | --pipe <NAME>
                              --db <PATH>  --worker-exe <PATH>
  fulcrum --worker            spawned by the daemon; not for humans
  fulcrum help                the usage, on stdout, exit 0
  ```

  A session belongs to the daemon, not to the connection that started it: closing a
  client does not stop a running session, and `close` ends one. `serve::run` never
  returns — there is no shutdown frame yet, so the daemon is killed, and a killed
  daemon's children exit on their own stdin EOF (`src/worker/mod.rs:186` is that
  path). It is a member rather than a consumer of the rev pin so it links the engine
  by `path = ".."` and can never drift from it.

**What the daemon does not do yet**, stated because the gaps are the parts that bite:
no `attach`, no replay, no re-join after a reconnect (T10); no graceful shutdown, so
`WorkerManager::reap_all` is still unwired (`fulcrum/src/serve.rs:89-95`); and **no
authentication** — that is T13, so the socket is a local IPC endpoint, not an access
boundary. Issue #4's hardening (the mandatory `hello` handshake, the session and rate
bounds, peer credentials where the platform provides them) is in, and on Windows peer
credentials are a documented limitation rather than enforcement —
[`docs/features/daemon-hardening.md`](docs/features/daemon-hardening.md) §3.

`fulcrum-harness`'s rules bind `fulcrum-harness` alone: the engine's boundary pins do not
walk the sibling member. That is no longer an open gap — the daemon has its own pins
over its own tree (`fulcrum/tests/boundary.rs`, 10 tests); see *Tests* below.

## Why it is a separate crate

The desktop is a Tauri app, and the engine spent its whole life inside it. That
was never a boundary the code respected — until it was made one deliberately.

The rule now enforced by tests, not convention: **`fulcrum-harness` has no code path
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
> target, and `tests/fixture_bins.rs` is that target for this package. A lib's unit
> tests get no `CARGO_BIN_EXE_*`, so a great many tests resolve the two fixture
> binaries *by path* through `test_support::fixture_bin()` / `target_debug_dir()`,
> and the file's existence is what puts those binaries where they look.
>
> **A bare `cargo test` passes from a cold target dir — do not build first.**
> Measured on a `target/` that had never existed, no build step: **1603 passed /
> 0 failed / 3 ignored**, which is what the bare command prints warm and on CI too.
> It is *not* the same total as `cargo test -p fulcrum-harness` — that is 1420 (1415 lib
> + the 5 in `fixture_bins.rs`), because `-p` leaves the `fulcrum` member's 183 tests
> unbuilt. The two numbers differ by exactly that member, and a box that claimed they
> matched would be describing the pre-daemon tree.
>
> **Deleting `tests/fixture_bins.rs` is what makes this loud.** Re-measured on a cold
> tree with that one file removed: **1531 passed / 67 failed / 3 ignored**, 51 `No such
> file or directory` messages — 67 is the count of tests that resolve those binaries
> by path (to spawn them, or, in `test_support.rs:557`, to assert the resolved path
> exists), so that is the number this box exists to protect. The arithmetic closes:
> 1531 + 67 = 1598 = 1603 − 5, the 5 being `fixture_bins.rs`'s own tests, which cannot
> pass once the file is gone. (An earlier note here recorded 1335 / 66 / 50 — the same
> trap one member ago, when the daemon contributed no tests; the failure count and the
> ENOENT count each move with the tree, so re-measure this box rather than copying it.)
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
will bite you* in `AGENTS.md` for why that is not the default). Measured at `v0.1.0`:
**1603 passed / 0 failed / 3 ignored** for the bare command, which splits 1420
(`fulcrum-harness`: 1415 lib + the 5 in `fixture_bins.rs`) and 183 (`fulcrum`: 45 lib +
138 across `boundary`, `db`, `serve`, `transport`, `worker_mode`). Two of the engine's
guards are worth knowing about before you trip them:

- **`boundary_pins`** — the dependency-direction pins described above. It does not
  skip `#[cfg(test)]` code, because a test-only import is still an edge. It walks
  `{CARGO_MANIFEST_DIR}/src` and reads `{CARGO_MANIFEST_DIR}/Cargo.toml`, so it
  polices `fulcrum-harness` **only**: `fulcrum/src/` is outside the walk and
  `fulcrum/Cargo.toml` is never read. That is intended — `rusqlite` is legal in the
  daemon, which owns the disk, and widening the engine's pin to cover the sibling
  would need an allowlist that weakens the engine's own rule. The gap it left is
  closed: the daemon carries **its own** pins over its own tree in
  `fulcrum/tests/boundary.rs` (10 tests), which forbid the UI framework, its plugin
  crates, the desktop package and paths into the desktop's settings and persistence
  modules, while explicitly allowing the SQLite driver here.
- **`childproc`'s source census** — a scan over the crate's production source that
  enforces the child-process invariants (one `Command` constructor, no raw command
  lines, process-group kills in one place). It is layout-aware: it works both as a
  workspace member and as a standalone package, and it **fails loudly** rather than
  quietly policing less when it cannot resolve a root. Being layout-aware, it is the
  reason this repo's layout is fixed — `AGENTS.md` has the finding.
- **The schema-drift guard pair** —
  `bash scripts/check-schema-drift.sh --self-test` (plants a retyped column, an
  added column, a missing table, a dropped index and an unpinned fifth table, and
  insists each one reddens the guard *and names the table*; plus the refusals: a
  missing pin or a fingerprint that cannot name all five keys exits 2 rather than
  passing) and `bash scripts/check-schema-drift.sh` (the live check: the shipped
  DDL against the `schema-sha.txt` pin). Run the self-test first — a guard that has
  lost the ability to fail prints a green nobody earned. Both run in
  `.github/workflows/schema-drift.yml`; see *Schema-shape contract with the
  desktop* below for what they actually compare.

## Schema-shape contract with the desktop

The daemon's four tables (`fulcrum/src/db/schema.rs`) are a transcription of the
desktop's, and the daemon opens a different file — so nothing at runtime notices the
two owners drifting apart, and the mappers read columns BY INDEX. `schema-sha.txt` at
this repo root is the canonical **shape pin** that fixes that: five sections — the
four tables plus `idx_messages_session` — each a header line and one `name:type` line
per column in declaration order. It pins names, declared types and order, and NOTHING
else: not NULL/DEFAULT/PRIMARY KEY flags, not the two `UNIQUE` upsert keys or the two
`ON DELETE CASCADE`s (pinned by probes that ask the database: `Db::table_has_unique`,
the cascade tests), not migration history, not the SELECT-list subsets
(`SESSION_COLUMNS` / `SPACE_COLUMNS` are subsets — comparing them false-positives on
`spaces.sort_order`). Whitespace is not drift; a renamed, retyped, reordered, added or
removed column, or a lost index, is.

Enforcement, **in this repo only**:
`the_pinned_schema_fingerprint_matches_the_shipped_file` in `fulcrum/tests/db.rs`
(so a daemon DDL change reddens `cargo test` in its own commit) plus
`scripts/check-schema-drift.sh` in `.github/workflows/schema-drift.yml`.

**The desktop half is not wired, and it is worth stating exactly why.** The design
needs the same comparator running in the desktop's CI against the desktop's live DDL;
`ls` that repo's `.github/workflows/` and you get `ci.yml` and `release.yml`, neither
naming it. So only the fulcrum leg runs, `schema-drift.yml`'s green is legitimately
green for the one comparison it really makes, and "a change to the desktop's DDL
makes some check fail" is **not** met.

Do not say the desktop leg "refuses". An earlier version of this paragraph gave that
as the reason, tied to the desktop's then-pin `78e59543` — a pre-T9 rev carrying no
`fulcrum` member and so no fingerprint, which is why the leg really did have to exit 2.
That boundary belongs to our history, not to the desktop's manifest: the `fulcrum/`
member first appears at `38a9986` and `schema-sha.txt` at `26d7648`, so **any ref at or
after `26d7648` — `v0.1.0` included — publishes a fingerprint worth comparing**, and
for those the leg is absent, not refusing. That is the weaker state and the more
actionable one. Re-check rather than re-read this sentence:
`ls <desktop>/.github/workflows/` answers the only question that matters, and it
moves. Silence there is a bug, never a green.

**Update path:** change both repos' DDL in lockstep and update `schema-sha.txt` in the
SAME commit, so the pin diff and the DDL diff are reviewed together.

## The name (and why it changed)

This package was `fulcrum-core` through T5–T9 and is `fulcrum-harness` from the
rename onward. The daemon sibling follows in its own change as `fulcrum-harness-daemon`
(its package is still `fulcrum` here), so the family prefix is `fulcrum-harness-` and
the suffix after it names the thing.

**Why the old name had to go — this half is measured.** `fulcrum-core` is taken on
crates.io, and not by a squatted empty crate: `fulcrum-core`, `fulcrum-assets`,
`fulcrum-guard`, `fulcrum-memory`, `fulcrum-messaging`, `fulcrum-providers`,
`fulcrum-tools` — seven crates described as *"Fulcrum AI agent framework — … module"*,
all published 2026-02-25, one version each, ~28 downloads. crates.io is
first-come-first-served, crate deletion is not possible, and the squatting clause in
its Terms of Use reaches only names held *"without any genuine functionality"*, so
neither taking it nor waiting it out is available. `-core` had also stopped describing
the member, which owns the worker, the provider auth flows and the MCP client.

**Why this name, and not a brand prefix — recorded because it was re-litigated once
already.** Leaving `fulcrum-core` was forced (it is taken, see above). Choosing
`fulcrum-harness` was not: it was free, **and so was the whole family under it** — all
14 probes (`fulcrum-harness-{tools,providers,memory,guard,assets,messaging,core,engine,worker,sandbox,auth,web,mcp,codemode}`)
came back unclaimed, against 6 of 6 TAKEN at the bare `fulcrum-{…}` level. The squat
bites only at the bare name. So the prefix below was a genuine free choice, and these
are the measurements behind making it:

- **A `fulcrum-codes-` prefix (matching the `archimedes-codes` org) was tried and
  reverted.** It rested on owning the `fulcrum.codes` domain, and that domain is **not
  available to register** — per the project owner's registrar check; it could not be
  verified independently from this box, since `.codes` is absent from the IANA RDAP
  bootstrap and port 43 is blocked here.
- **`-codes` has no precedent as a crate suffix**: 0 crates among the top-100 registry
  results for "codes", against `-core` 52, `-engine` 18, `-harness` 15. The only crate
  actually named `codes` is a CBC-XOR cipher, so `fulcrum-codes-tools` reads as a
  character-encoding toolkit.
- The API Guidelines' one explicit suffix rule — no `-rs`/`-rust`, *"Every crate is
  Rust!"* — targets suffixes that restate what the registry already knows. Publisher
  identity is exactly that: crates.io carries it in `owners` and shows it on the crate
  page. `fulcrum-harness` says what it does; a publisher prefix says who shipped it.
- The org analogy inverts. `archimedes.codes` → `archimedes-codes` → repo
  `archimedes-codes/fulcrum`, and the **artifact** there is `fulcrum` — the org name
  does not propagate into artifacts in the existing setup. Followed strictly it yields
  `fulcrum`, which is the profiler crate.
- The mechanism that would actually give a publisher-enforced family is **RFC 3243
  namespaces** (`fulcrum::harness`). Accepted in 2022, still unimplemented as of the
  2026 project goals ("needs contributor"), and it needs the root name — which is taken.

`fulcrum-harness` and the `fulcrum-harness-*` family were all still free at the time of
writing. Nothing is published, so a future rename is still cheap; it stops being cheap
the moment `cargo publish` runs.

The daemon member is untouched by this change — its package is still `fulcrum` here, as
are its bin, lib and directory. `CONTEXT.md` defines **Agent harness** as the model loop
+ tool registry + persistence + provider integration, and lists **Agent** under *Avoid*
for it — "a harness *runs* the agent; it is not the agent".

**The name is on the wire, so this was not a cosmetic change.** `env!("CARGO_PKG_NAME")`
is not read anywhere in the tree, but `env!("CARGO_PKG_VERSION")` — which the rename
left alone — is the daemon's `hello.serverVersion`, the engine's `User-Agent`, and MCP
`clientInfo.version`. `scripts/check-versions.sh` enumerates members from the root
manifest rather than naming them, so it followed the rename with no edit and still
refuses fewer than two version-bearing members.

**What a consumer must do, measured against a scratch two-repo tree:**

| Situation | Result |
| --- | --- |
| Existing pin left alone (`fulcrum-core = { tag = "v0.1.0" }`) | **Keeps working.** A tag's manifest is frozen at that commit, so the rename on `main` cannot reach it. `cargo tree -i fulcrum-core` still prints the `git+…?tag=v0.1.0#<sha>` source. |
| Dep line renamed, tag NOT bumped | **Hard error**, `no matching package named \`fulcrum-harness\` found` — the tag's manifest still declares `fulcrum-core`. Loud, not silent. |
| Stale local `[patch]` naming `fulcrum-core` | **Hard error**, `patch location … does not contain packages matching \`fulcrum-core\``. This is the one that bites locally rather than in CI, because the live `.cargo/config.toml` is gitignored — fix the key in `<consumer>/.cargo/config.toml` too. |

## Consuming it

```toml
[dependencies]
fulcrum-harness = { git = "https://github.com/archimedes-codes/fulcrum", rev = "<pinned-rev>" }
```

The engine is pre-1.0 and moves with its consumer; pin a `rev`, not a branch.

For working on the engine and its consumer at the same time, put a `[patch]` in the
**consumer's** `.cargo/config.toml` (not here — a patch on the dependency's own side
does nothing for its consumers):

```toml
# <consumer>/.cargo/config.toml
[patch."https://github.com/archimedes-codes/fulcrum"]
fulcrum-harness = { path = "../fulcrum" }
```

## Layout

The workspace root **is** the `fulcrum-harness` package — `[package]`, `src/` and
`tests/` all sit here, beside the `[workspace]` table. Do not "tidy" that by moving
them into a `fulcrum-harness/` subdirectory; `AGENTS.md` records what breaks, and how to
reproduce it, under *The census (`childproc`)*.

```
Cargo.toml        [workspace] + fulcrum-harness's [package] (the forced shape)
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
fulcrum/            the `fulcrum serve` daemon — supervisor + socket + the SQLite file
  Cargo.toml        its own manifest; rusqlite is legal here, and the ENGINE's
                    boundary_pins do not read this file (fulcrum/tests/boundary.rs does)
  tests/            per cargo: lib 45, boundary.rs 10, db.rs 31, serve.rs 41,
                    transport.rs 25 (+2 ignored), worker_mode.rs 31 — 183 pass, 2
                    ignored. (serve.rs and worker_mode.rs each `#[path]`-include
                    tests/proctable/mod.rs, whose 20 tests cargo therefore counts in
                    both targets — a grep over the files undercounts serve.rs.)
  src/
    lib.rs          the module tree, and the argv grammar's owner (see main.rs)
    main.rs         the `match` over serve::parse_command: --worker | serve | help
    db/             the WAL-mode fulcrum.db behind FulcrumPersistence (the sole writer)
    transport/      UDS on unix, Named Pipe on Windows, newline-delimited JSON frames
    serve.rs        the accept loop and the client↔session multiplex (one broadcast,
                    one relay per connection); never returns — no shutdown frame yet
    worker_factory.rs the supervised worker per session, and the one child constructor
```

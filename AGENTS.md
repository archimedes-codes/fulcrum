# AGENTS.md

This repo is a two-member Cargo workspace. `fulcrum-core` — the agent engine extracted from
[`archimedes-codes/archimedes`](https://github.com/archimedes-codes/archimedes), the
desktop app (renamed from `archimedes-codes/desktop` after the extraction): the tool loop, provider clients, tools
and sandbox, the MCP client, and the out-of-process worker. A library. It knows nothing about windows, trays, or
the UI that renders it. Beside it sits `fulcrum`, the `fulcrum serve` daemon — **it runs**: one binary with two modes
(`fulcrum serve` is the supervisor, `fulcrum --worker` the child it spawns, a self-exec of the same executable), a
listener on a unix socket or a Windows named pipe, the SQLite file, and the accept loop that multiplexes clients onto
supervised worker processes (ADR 0091). What it does *not* do yet is listed in
`fulcrum/src/serve.rs:89-95` — no `attach`/replay (T10), no graceful shutdown frame so `run` never returns and
`WorkerManager::reap_all` is unwired, and no authentication (T13). It is a member rather than a rev-pinned consumer so
it links the engine by `path = ".."` and cannot drift from it.

Everything below is about `fulcrum-core` unless it says otherwise — and that qualification matters, because the
guards that police it (`boundary_pins`, the manifest pin) stop at its own directory. See
*`boundary_pins` does not see the sibling member*.

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
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo check --target x86_64-pc-windows-gnu --all-targets   # the only local Windows proof available
bash scripts/check-rquickjs-allocator.sh                   # + --self-test to prove it has teeth
bash scripts/check-schema-drift.sh                         # + --self-test: neither repo's tests can see the other's DDL
bash scripts/check-no-stray-processes.sh                   # + --self-test: leak guards ask per-pid, never whole-table
bash scripts/check-versions.sh                             # + --self-test: the version lives in two members and nothing ties them
```

All three guards run in CI, and none is a full-strength gate yet, so do not read the
bar as one: `check-schema-drift.sh` runs in `.github/workflows/schema-drift.yml`, where
the **cross-repo half is not yet wired** — the desktop has no comparator step, so only
the fulcrum-side leg runs, and the desktop leg REFUSES with a named reason (exit 2)
rather than passing on an empty comparison. `check-no-stray-processes.sh` runs in
`ci.yml` after `cargo test` and is **enforced on unix only; the Windows leg is
advisory** — it reports every live match and kills nothing, because no bash-accessible
mechanism there reads another process's environment to attribute it to this run.
`check-versions.sh` runs in `ci.yml`'s `scripts` job (one OS, no cargo — it is pure text
tooling, the shape `schema-drift.yml` reasons for) and without `--tag`; the tag half of
its agreement is enforced by `release.yml`, the only place a tag exists. See
*Releasing* for what that guard is for. In every
workflow the `--self-test` step runs BEFORE the live check, so a guard that has lost the
ability to fail reddens the run instead of printing a green nobody earned.

No `cargo build --all-targets` line, and that is not an oversight: `--all-targets` on the clippy and Windows checks
already builds the whole graph, and CI deliberately has **no** build step before its test step (see the
`fixture_bins` bullet below — a build step there would hide the deletion of the file the fixture tests depend on).

Every bare command above runs **both members**, and that is a manifest guarantee, not cargo's default — see
*`default-members` is what makes the bare gates reach the daemon*.

CI runs the full bar on **ubuntu, windows and macos**. A platform that has never been compiled is an untested
platform: this crate carries deliberate `#[cfg(unix)]` / `#[cfg(target_os = "linux")]` process hygiene, and two
real Windows-only bugs were previously caught only by the CI leg.

## Things that will bite you

**`tests/fixture_bins.rs` is part of the build contract, not a test that happens to be here.** Cargo writes a
package's binaries to the top of `target/debug/` only when it builds an integration-test target, and many tests spawn
the fixture binaries **by path** (a lib's unit tests get no `CARGO_BIN_EXE_*`). That target is this file. Delete it and
the suite goes red with `ENOENT` — **1335 passed / 66 failed, 50 `No such file or directory` messages, re-measured on
this branch by deleting the file and running cold**. That is the trap's only witness, so **do not "fix" it by adding a
`cargo build --all-targets` step to CI**: the build step materialises the bins regardless, and the suite would stay
green after the file was gone — re-arming the exact thing the file exists to prevent. `ci.yml` says the same thing at
line 60 and forbids the step. Nothing here argues for cold-target fragility: measured on this branch, a `target/` that
had never existed and no build step at all gives **1406 passed / 0 failed / 1 ignored** — the same total from
`cargo test -p fulcrum-core` (1401 lib + the 5 in `fixture_bins.rs`) and from bare `cargo test`, which additionally
builds and runs the `fulcrum` member's targets — its totals are whatever the `test result:`
lines of that run print, and this file does not copy them.

**The two fixture bins are `fake_worker_core` and `fake_mcp_stdio_core`.** The bodies live in `src/fixtures/` as
ordinary lib code; each bin is a three-line `main`.

**Why the `_core` suffix, and why it stays.** The desktop builds the same bodies under the *unsuffixed* names. While
this package was a workspace member of the desktop, both members' bins landed in ONE shared `target/debug/`, and two
members declaring a bin of the same name collide on the output filename — cargo only *warns* and **the last writer
silently wins**, so a suite can spawn the other package's fixture and never notice. That is why the names differ. The
desktop now consumes this crate as a git dependency, and **a dependency's bins are not built by the dependent**, so the
collision cannot happen from the desktop's checkout anymore. **The suffix is still load-bearing, because re-adding this
package to the desktop's `members` — a normal move for local engine work — re-arms the collision silently.** Renaming
these bins to drop the suffix is the way to reintroduce a bug that produced wrong test results with a green suite.

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

**`default-members = [".", "fulcrum"]` in the root `Cargo.toml` is what makes the bare gates reach the daemon, and
deleting it fails nothing.** Because the workspace root is *also* a package, cargo resolves `workspace_default_members`
to `fulcrum-core` **alone** — `cargo metadata` reports exactly one default member without that line — so every command
that does not name a package is package-scoped to the engine and compiles the daemon not at all. Measured on this
branch with a planted `E0308` type error in `fulcrum/src/transport/frame.rs`, a module `main.rs` never references:

| Command (the form CI, or a developer, actually types) | without the line | with the line |
| --- | --- | --- |
| `cargo clippy --all-targets -- -D warnings` | **exit 0**, no `E0308` anywhere | **exit 101**, `E0308` twice |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 101 | exit 101 |
| `cargo test` | **exit 0**, the member is never built | **exit 101** |
| `cargo tree -e features` (the allocator guard) | 0 `rusqlite` lines | 6 `rusqlite` lines |

So removing the line silently UN-GATES the entire member: every CI step stays green over a member that does not
compile. It is the same failure class this repo refuses elsewhere — the vacuous `-p` form of
`check-rquickjs-allocator.sh`, a blind census root. Note the asymmetry when you edit the manifest: `members` pulls a
root `[package]` in implicitly, so `"."` there buys nothing (and is banned for the census reason in the manifest
comment), but `default-members` contains no implicit anything — a package absent from it is simply never built by a
bare command, which is exactly how the member went invisible. Fixing this in the manifest rather than by adding
`--workspace` to `ci.yml` is deliberate: the manifest fixes every bare invocation, a developer's local `cargo test`
included; widening CI alone leaves the local run blind. Do not add `--workspace` to CI on top of it.

**The same mechanism, not package breadth, is what puts the daemon's dependencies inside the rquickjs allocator
guard.** A new workspace member's dependencies do **not** automatically fall inside `check-rquickjs-allocator.sh` —
the guard runs a bare `cargo tree -e features`, and with the root package resolving as the sole default member that
tree never contained `rusqlite` or any of the daemon's graph (measured: 0 `rusqlite` lines without the line, 6 with
it). The guard covers the daemon's dependency graph **only because `default-members` lists both packages**. If you
change either half of that pairing, re-run the guard and check it still sees the daemon.

**`boundary_pins` does not see the sibling member.** `crate_code()` walks `{CARGO_MANIFEST_DIR}/src`
(`src/boundary_pins.rs:166-209`) and the manifest pin reads only `{CARGO_MANIFEST_DIR}/Cargo.toml` (`:302-328`), so
`fulcrum/src/` is outside the walk and `fulcrum/Cargo.toml` is never opened. The daemon may import `rusqlite` —
intended, it owns the disk and ADR 0091 keeps the disk out of the engine — and a `tauri` import there would be invisible
to *these* pins. That is the shape, not an open hole: **the daemon carries its own pins over its own tree**, in
`fulcrum/tests/boundary.rs` (10 tests), which forbid the UI framework, its `_plugin` crates, the desktop package and
`crate::config::`/`crate::storage::` paths inside `fulcrum/src/`, while naming the SQLite driver as legal here. So do
not widen `boundary_pins` to walk the sibling: it would fail on the daemon's legitimate `rusqlite`, and the fix for that
would be an allowlist that weakens the engine's pin. Two pin sets, two trees, neither reading the other.

**The schema-shape pin is the fulcrum↔desktop contract — `schema-sha.txt` is an artifact, not a build output.** The
daemon's four tables are a transcription of the desktop's and the daemon opens a different file, so a DDL change on
either side is invisible to the other at runtime; `schema-sha.txt` (five sections: four tables + `idx_messages_session`,
per-column `name:type` in declaration order) is what makes that drift loud, via `the_pinned_schema_fingerprint_matches_the_shipped_file`
in `fulcrum/tests/db.rs` and `scripts/check-schema-drift.sh` in `.github/workflows/schema-drift.yml`. Update it in the
SAME commit as a lockstep DDL change so the two diffs are reviewed together, and do **not** regenerate it from a fork
or a local experiment — both the guard and the tests refuse a comparison that cannot name all five keys, so a
hand-tuned pin reddens the contract rather than blessing the fork. Note what it deliberately does not pin (flags,
the `UNIQUE` keys, the cascades, the SELECT-list subsets — the desktop's `SPACE_COLUMNS` is 4 of `spaces`' 5 columns,
and comparing subsets false-positives on `spaces.sort_order`).

**The cross-repo leg is not running, and the reason it is not running changed under the docs.** Two states, and the
paragraph that used to describe them was true of a pin the desktop no longer holds:

- **Still true:** the desktop has no comparator step. `ls` its `.github/workflows/` — `ci.yml` and `release.yml`,
  neither naming `check-schema-drift` — so only the fulcrum-side leg runs, and `schema-drift.yml`'s green is legitimately
  green for the one comparison it actually makes. "A change to the desktop's DDL makes some check fail" is still not met.
- **Obsolete as of this commit:** the stated *reason*. It explained the absence by naming the desktop's pin —
  `78e59543`, a rev that publishes no `fulcrum` member and no schema, so the desktop leg genuinely had to refuse. That
  was true when written and it is the reason the guard's refusal path exists. It is no longer the reason the leg does
  not run.

State the durable fact, not the desktop's current pin — that sentence has gone stale twice by naming it. The boundary
lives in *our* history, which does not move: the `fulcrum/` member first appears at `38a9986` (PR #2) and
`schema-sha.txt` first appears at `26d7648` (PR #10), so **any fulcrum ref at or after `26d7648` — `v0.1.0` included —
publishes a fingerprint worth comparing.** Only pre-T9 revs like `78e59543` carry no schema at all. That is why
the old refusal reason was not merely stale but already obsolete at the rev the desktop pinned before the tag
(`2ed5f5a6` contains `26d7648`, verified with `merge-base --is-ancestor`): the leg is **absent, not refusing**, the
weaker state and the more actionable one. Verify with `ls <desktop>/.github/workflows/` — the only question that
matters, and the one that can change.

Say "the desktop has no comparator step", not "it refuses". The `--self-test` still runs first in `schema-drift.yml`,
so a guard that has lost the ability to fail reddens the run. None of this widens `boundary_pins` — the daemon's
`rusqlite` is legal and the engine's pin stays exactly where it is.

## The census (`childproc`)

A scan over production source enforcing the child-process invariants: exactly one `Command` constructor, no raw
command-line appends, process-group kills in one place, no data-dir resolution in the crash-log path. It is
**layout-aware** — it works as a workspace member and standalone, decided by two independent witnesses (an ancestor
`[workspace]` manifest that must *name this package*, plus a filesystem walk for a sibling desktop tree), because
every desktop-conditional floor **vanishes rather than fails** when the discriminator is wrong. Its vacuity floors are
**per root**: an aggregate floor stayed green while one root was entirely blind.

A pin that passes on the broken thing is worse than no pin. When you change one, prove it with a mutation that still
compiles — hoist a value into a local, don't delete it — and report if you cannot.

### The layout is forced: do not move `src/` and `tests/` into a `fulcrum-core/` directory

The cargo-idiomatic shape for this repo is a pure `[workspace]` manifest at the root with the engine in a
`fulcrum-core/` subdirectory. **It is the shape the grounding report recommended and it panics the census.** The two
shapes:

- **Layout A (this repo):** `fulcrum-core`'s `[package]` stays at the root, `src/` and `tests/` beside it, plus the
  `fulcrum` member. Census passes.
- **Layout B:** move `src/` and `tests/` into `fulcrum-core/`, put a pure `[workspace]` at the root. Census panics.

Reproduced on this branch in a scratch tree (`fulcrum-core/` moved down one level, root manifest reduced to
`[workspace] resolver = "2"` + `members = ["fulcrum-core", "fulcrum"]` + the `[profile.release]` block, the member's
path dep repointed to `../fulcrum-core`), running `cargo test -p fulcrum-core --lib -- childproc::tests`:

```
test result: FAILED. 5 passed; 5 failed; 1 ignored; 1392 filtered out

the census resolved the wrong root: <root>/src-tauri/src is not a directory
  (layout WorkspaceMember { workspace_root: "<root>" }, CARGO_MANIFEST_DIR=<root>/fulcrum-core)
```

**Why.** `declaring_workspace_root` (`src/childproc.rs:712-714`) walks **ancestors only**, starting at
`manifest_dir().parent()`. A `[workspace]` sitting **at** the manifest dir is therefore never examined, so Layout A
resolves `Layout::SinglePackage` — the correct answer here, since this checkout has no desktop tree to police. Move the
package down one level and the workspace lands on the ancestor path, `member_entry_names_us` matches
`"fulcrum-core"`, the layout becomes `WorkspaceMember`, and `source_roots()` (`:985-988`) then hardcodes
`workspace_root.join("src-tauri").join("src")` — which does not exist in this repo. The census panics loudly instead of
policing nothing, which is why the trap is discoverable at all.

**Four tests are the gate on this** — `childproc::tests::the_two_layout_witnesses_agree` (the witnesses disagree: the
manifest graph says member, the filesystem says no desktop tree), `production_source_has_exactly_one_child_constructor`,
`production_code_never_appends_a_raw_command_line_itself`, and `production_crashlog_never_resolves_a_data_dir`.
Measured, the panic reddens **five**: `production_process_group_kill_lives_only_in_childproc` calls `source_roots()`
too, so treat `the census resolved the wrong root … src-tauri/src is not a directory` as "you have created Layout B"
and move `src/` and `tests/` back to the root.

`members` lists **only `"fulcrum"`, never `"."`** — and the reason is the census, not cargo syntax. Cargo accepts
`"."` in `members` and dedupes it against the implicit root member (measured), so it is not an error; but
`member_entry_names_us` compares `rel == entry || package == entry` and `relative_to_manifest` returns the **empty
string** for a root package, so `"."` matches nothing and would be a lie waiting for the ancestor walk to change shape.
Write the form that is true, not the form that happens not to be read. The root `Cargo.toml`'s `[workspace]` comment
carries the same finding in full; the two must agree.

Wanting Layout B is legitimate — it just is not a move, it is a change to `source_roots()` that must first teach the
census a workspace root need not hold a desktop tree, with its own vacuity proof.

## Release profile

`[profile.release]` is copied from the desktop workspace root because cargo reads profiles from the workspace root
only; a standalone build silently gets defaults without it. `panic = "unwind"` is the ADR 0025 consequence — a Worker
panic stays in the Worker — and must never become `abort`. It is also load-bearing for the `fulcrum` member, which
cannot set its own profile: cargo reads profiles from the workspace root only, so the daemon inherits `unwind` and a
supervisor survives a worker panic. A `panic = "abort"` daemon would take the whole supervisor down with the first
worker that panicked.

## Releasing

**The version lives in TWO manifests plus the lock file, and nothing in the build ties them
together.** `Cargo.toml` (the root, which is the `fulcrum-core` package) and
`fulcrum/Cargo.toml` each declare a `version`, and `Cargo.lock` mirrors both. A path
dependency inside one workspace resolves by PATH and never by version, so two members that
disagree still compile, still pass every test, and still ship — the drift is invisible until
something reads one of the two numbers out loud. Three things do read one: the daemon's
`hello` answer carries `serverVersion` from `env!("CARGO_PKG_VERSION")`
(`fulcrum/src/serve.rs:2309`), the engine sends the same macro as its `User-Agent`
(`src/harness/provider/transport.rs:19`) and as MCP `clientInfo.version`
(`src/mcp/http.rs:125`). So a bump is not cosmetic: it changes bytes on the wire. No test
asserts the literal value — every read goes through the macro — which is what makes a bump
test-safe; a future test that hardcodes `0.1.0` would be the first thing to break on the
next bump, and that is the wrong shape of pin.

`bash scripts/check-versions.sh` is the tie. It enumerates the members from the root
manifest rather than naming the two known paths — a check that hardcodes them reads two
versions and passes green over a third member nobody added, the vacuous-pass class this repo
records at `check-rquickjs-allocator.sh`'s `-p` form and `childproc.rs`'s blind-census
witnesses — and fewer than two version-bearing members is a refusal (exit 2), not a pass.
Both readers are `[package]`-section-aware, because `[workspace.dependencies.<name>]` puts a
line-start `version` in the same file and a naive first-match grep can report a dependency's
version as the crate's own; `--self-test` plants that decoy so the regression is caught
rather than tolerated. It also checks the lock, whose drift cargo self-heals on the next
build but which would fail a `--locked` build on the release commit itself.

**`v0.1.0` is the first release of this repo, and the numbering restarts from the desktop's.**
The manifests carried `0.11.0` because that was the desktop's version at extraction, and one
annotated tag `v0.11.0` was pushed at `e44f382` as a pre-T9 bisect boundary. Both are
superseded: the desktop app is at `0.11.x` and this engine is not, and continuing to inherit
the desktop's number would make the two repos' `0.11.0` mean different trees. `v0.11.0` is
deleted from the remote so there is one version line, not two that sort in the wrong order.
The tag message's own words already said it was "a legibility marker, not a semver contract",
so nothing pinned by it breaks: the desktop's `src-tauri/Cargo.toml` pins by `rev`, and a rev
pin is unaffected by a tag being deleted or renumbered.

**The version is now load-bearing in a way it was not, and that is the point.** The plan is to
move the desktop from `rev = "40-hex-chars"` to `tag` + `version`, which cargo honours only
when the tagged manifest carries that version. That makes the tag and the manifests one
contract, which is why `check-versions.sh --tag vX.Y.Z` is the first step of
`release.yml` and not a nicety: a tag outrunning the files produces a pin nothing can install.

**What `release.yml` publishes: a GitHub release and its notes, and nothing else.** There is
no build stage and no artifact upload, deliberately — no consumer installs a binary today (the
desktop compiles the engine from source as a git dependency), and an unsigned tarball that
looks like a supported install channel is worse than none. No `cargo publish` either: neither
`fulcrum-core` nor `fulcrum` is on crates.io, and a name squat there is not recoverable by
renumbering. The workflow's two jobs are to refuse a version the tree does not carry, and to
create the release. Its `workflow_dispatch` mode rehearses the gate on a branch and creates
nothing — and it prints that it created nothing, so a green rehearsal is never mistaken for a
shipped release.

Two provenance gates run before a release object can exist, both cheap and both about a tag
nobody intended to ship: the tag must be **annotated** (`git cat-file -t` answers `tag`, not
`commit` — this asserts the skill's `git tag -a` rule instead of trusting it), and the tagged
commit must be **reachable from `origin/main`**, so a release is a commit the merge bar already
proved. Not "check the tagged commit's CI status": that gate reddens forever the day someone
adds a workflow that does not run on tag pushes, where the ancestry check gets the same
guarantee without depending on the workflow list staying still.

**Skill-executable step** (the `release` skill reads this file at Step 1 and applies release
instructions at Step 5, so this is written to be executed, not just read): after the skill's
automated bump, run `bash scripts/check-versions.sh` from the repo root and require it to
print `versions match: X.Y.Z` before tagging. The bump commit covers **both manifests and
`Cargo.lock`** — `cargo check` refreshes the lock, and committing it in the same commit is
what the guard then proves. The values stay bare `X.Y.Z`, no `v` prefix (the tag carries the
`v`; the check rejects a `v`-prefixed manifest version because the two are not
interchangeable).

**Skill-executable final step — write the release notes.** `release.yml` puts GitHub's
*generated* notes on the release (`--generate-notes`, at creation, for the tag being created),
so what remains — and what no workflow can do — is notes a person would read. The generated
list is squash-merged PR subjects with `by @user` suffixes: accurate, and not the release text.
Group by what a consumer would notice (New / Fixed / Changed), drop internal-only entries
(`docs`, `chore`, CI, test-only), keep the PR links for what stays, and open with the one
change worth upgrading for. `gh release edit vX.Y.Z --notes-file /tmp/notes.md`.

The desktop's copy of this ceremony ends with an updater manifest (`latest.json`) and a
signature-completeness gate. **Neither exists here and no step for them should be copied
over**: there is no Tauri bundle, no `.sig`, and no `tauri-plugin-updater` client reading a
manifest. `release.yml`'s last step flips the draft and nothing follows it.

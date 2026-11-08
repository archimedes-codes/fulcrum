---
status: committed
done-when: A LiteLLM proxy whose `GET /model/info` omits `model_info.supports_function_calling` has its models selectable (they are no longer fabricated into `supports_tools: false`), and the session model picker lists every catalog model with the ones the harness cannot speak greyed out and labelled with why.
issue: https://github.com/archimedes-codes/fulcrum/issues/9
---

# LiteLLM tool support is three-valued, and the picker says why a model is unselectable

**Goal:** Stop the LiteLLM route reduction fabricating `supports_tools: false` from an
absent `supports_function_calling`, and give the session model picker the two facts it
needs to show an unselectable model instead of offering it silently.

**Architecture:** Two repos, one rule, strictly ordered. `fulcrum-core`'s `reduce_litellm_route`
carries the tool-support gate as `Option<bool>` so an unadvertised marker reaches the desktop as
`None` and `unwrap_or(true)` applies — the shape `supports_thinking` already has. The desktop then
bumps its rev pin, projects selectability onto the `list_models` DTO from ONE authority
(`Model::is_selectable`, extracted from `ModelCatalog::selectable`) plus the reason, and
`SessionConfigBlock` marks those picker rows `disabled` with an inline cue.

**Tech Stack:** Rust (a two-member cargo workspace in fulcrum; a rev-pinned git dep in the
desktop), React + TypeScript + vitest (desktop frontend), ADR 0026 as the decision record.

---

## Read this before Task 1

### The ordering constraint that shapes the whole plan

The desktop consumes `fulcrum-core` as a **rev-pinned git dependency**
(`archimedes/src-tauri/Cargo.toml:47`, `rev = "78e595436ac5612d8ef87f692bee61f48f6e58e7"`,
verified to already contain `reduce_litellm_route`). So:

- Desktop code that names `Model::is_selectable()` (Task 4) **cannot compile until the rev is
  bumped** (Task 3), and Task 3 **cannot name a rev that has not merged to `fulcrum` `main`**.
- A local `.cargo/config.toml` `[patch]` (see Task 3's last section) lets a child DEVELOP against
  an unmerged engine, but the desktop PR's CI compiles the pinned rev — a patched tree cannot
  ship. `.gitignore` ignores the live file precisely so it can never be committed.

**Consequence: the desktop tasks are strictly sequential after Task 1's PR merges.** The desktop
work can be *drafted* in parallel with the engine PR using the patch, but only Task 1's merge
unblocks the desktop PR's existence. Do not open the desktop PR before it.

### The three findings that reframe issue #9

The issue body (ported from archimedes#153, filed 2026-11-03) describes a tree the desktop no
longer has. All three claims below were re-read against both trees on 2026-11-08; they are
load-bearing for the task breakdown, so they are stated here and repeated where they bite.

**1. The right-panel picker no longer reads `selectable()`.** Since desktop commit `13821faf`
(PR #164, issue #154, 2026-11-06) `SessionConfigBlock` passes `items={catalogItems}` into
`SessionConfigSelect`, and `catalogItems` is built from the LIVE `useModelCatalog` store
(`src/components/SessionConfigBlock.tsx:218-227`), which `list_models` feeds from the
UNFILTERED `catalog.models` (`src-tauri/src/commands/settings.rs:99`). `SessionConfigSelect`
prefers `items` over the option's own `options` (`src/components/SessionConfigSelect.tsx:272-287`).
So **the "models completely absent from the picker" symptom is already gone for the picker's row
list** — pinned by `SidePane.test.tsx:1683` ("seeding useModelCatalog changes an ALREADY-OPEN
session's picker options"). `selectable()` is still authoritative for two things, and the engine
fix still matters for both: `resolve_native_model`'s last rung
(`src-tauri/src/agent/session/mod.rs:2869-2876`, the "no models available for the native session"
hard error and the default-model fallback) and `synthesize_catalog_config_options`' `options` list
(`:2910-2919`, the snapshot the harness emits, and the fallback path when no `items` prop is
passed).

**2. The real remaining desktop defect is the opposite of the reported one.** Because the picker
lists every catalog model and `set_config_option` resolves the value against `catalog.models` —
NOT `selectable()` (`src-tauri/src/agent/session/mod.rs:2089-2096`) — a user can pick a model the
harness cannot speak, with no signal at all. `ModelPickerItem` already has `disabled?: boolean`
(`src/components/ModelPickerDialog.tsx:14-19`) and `ModelPickerDialog` honours it fully (`:113`
guard, `:159` `RowButton`, pinned by `ModelPickerDialog.test.tsx:162`), and **nothing sets it**.
So issue proposal #2 is still needed, with a different justification: not "models vanish" but
"models are offered without saying they cannot be used".

**3. Proposal #3 needs no work.** The issue asks to re-synthesize `configOptions` for live
sessions on a provider refresh. That is already solved by finding #1 — the picker's rows come from
the live store, so a Settings refresh reaches an open session with no backend push, and ADR 0076
already propagates the model metadata the loop holds. `refresh_live_models` (`:2418-2463`) copies
`context_window` / `supports_thinking` / `thinking_levels` and not `supports_tools`, and that
stays correct: nothing on the `AgentLoop` side reads `Model.supports_tools`. After this plan the
field has TWO readers and BOTH run on the catalog, never on the loop's copy:
`Model::is_selectable()` is the selectability authority, and the `ModelDto` projection reads the
field directly (`src-tauri/src/commands/settings.rs:130`) as the picker's reason cue. Recorded
again in Task 4's Context so a future reader does not "fix" it.

### No fulcrum-side doc task

This repo has no `docs/decisions/`; ADR 0026 lives in the desktop and Task 2 owns amending it.
`fulcrum/docs/features/*.md` are POST-SHIP durable notes written once the residue is known, not
plans — writing one now would duplicate the ADR and be prose about code that has not landed. The
engine-side record is: the amended doc comments from Task 1, the PR body, and Task 2's ADR
amendment. If a `docs/features/` note is wanted later it belongs in the ship step, not here.

---

## The rule being changed, stated once

`reduce_litellm_route` folds a route's deployment set. Today (`src/harness/catalog.rs:874-877`):

```rust
supports_tools: !deployments.is_empty()
    && deployments
        .iter()
        .all(|d| d.supports_function_calling == Some(true)),
```

`None` and `Some(false)` collapse to the same `false`, and `aart_to_meta` then writes
`Some(route.supports_tools)` (`:918`) — a fabricated claim the proxy never made, in direct
contradiction of this file's own stated principle at `:884-886` ("the reduction fabricates no
claims the proxy never made") and of `src/catalog.rs:243` ("`None` on `supports_tools` preserves
today's hardcoded `true`", which the code has never honored for a reduced route).

**The replacement rule (three-valued, mirroring the `supports_thinking` gate at `:907-916`):**

| Route's deployments | `Aart.supports_tools` | Downstream `Model.supports_tools` (`unwrap_or(true)`) | Selectable |
| --- | --- | --- | --- |
| every one `Some(true)` | `Some(true)` | `true` | yes |
| any one `Some(false)` | `Some(false)` | `false` | no |
| some `Some(true)`, rest absent | `Some(false)` | `false` | no |
| NONE advertises the key | `None` | `true` | yes ← **the fix** |
| empty set | `None` | `true` | (unreachable in production; see Task 1 Step 4) |

Row 3 is deliberate and is NOT relaxed: a proxy that spoke about some deployments and stayed
silent about a sibling is still a route whose peers disagree, and the ALL-deployments gate keeps
its meaning there. The relaxation is exactly and only "a route nobody said anything about is not
a route that said no".

`supports_thinking` is left as `bool` on `Aart`, deliberately: its consumer is
`supports_thinking: meta.supports_thinking.unwrap_or(false)`
(`src-tauri/src/agent/session/mod.rs:555`), so `None` and `Some(false)` are identical downstream
and widening it would churn three tests to change nothing. Tools is the field where the
distinction has an effect **only because the fallback is `true`**. Write that reason in the code
comment — the asymmetry between two sibling fields otherwise reads as an oversight.

---

## Parallelisation

**Verdict:** PARALLEL — one wave of 2, everything else serial, and the whole desktop branch
gated on Task 1's merge.

The two repos are separate git trees with separate branches and separate fan-ins; nothing under
`fulcrum/` shares a path with anything under `../archimedes/`. But the rev-pin dependency makes
this cross-repo **sequential**, not parallel (see *The ordering constraint* above): Task 1 merges
first, and only then can the desktop branch exist as a green PR.

- **fulcrum: SERIAL.** Task 1 is the only code task in this repo.
- **archimedes Wave 1: Tasks 2 and 3.** Pairwise disjoint, checked against both `Files:` lists:
  Task 2 touches `docs/decisions/0026-litellm-api-mode.md` and nothing else; Task 3 touches
  `src-tauri/Cargo.toml`, `Cargo.lock`, and `src-tauri/src/agent/session/tests/mod.rs`. No shared
  fixture, no `mod.rs`, no `pnpm-lock.yaml` (neither adds an npm dependency). Task 2 is prose and
  `use`s nothing Task 3 creates. `Cargo.lock` is named by Task 3 ONLY — Task 2 touches no lockfile,
  so the one shared-mutable-artifact risk is single-owner.
- **archimedes Task 4: serial.** It calls `Model::is_selectable()`, which first becomes reachable
  at Task 3's rev bump. That is a build-graph edge, so it cannot join Wave 1.
- **archimedes Task 5: serial.** It consumes Task 4's `ModelDto.selectable` / `supportsTools`, and
  it edits `src/components/SidePane.test.tsx`, which Task 4 also edits. Two independent
  disqualifiers.

Task 5's frontend half is disjoint from Tasks 2/3/4 and could ride a later wave, but it reads
Task 4's DTO fields and shares `SidePane.test.tsx` with it, so it runs alone — a width-1 wave is
just a serial task.

---

### Task 1: `fulcrum-core` — tool support becomes three-valued in the LiteLLM reduction

**Parallel:** none — the only code task in this repo's branch, and the gate on every other task.

**Context:**
Issue #9. `reduce_litellm_route` collapses an absent `supports_function_calling` into `false`,
`aart_to_meta` writes that as `Some(false)`, and the desktop's `EffectiveCatalog::resolve` turns
it into `Model.supports_tools == false` via `meta.supports_tools.unwrap_or(true)` — so a LiteLLM
route the proxy said nothing about tool calling becomes a model the harness refuses to select.
The fix carries the tri-state from the parse seam (where `supports_function_calling` is already
`Option<bool>`, `:646` / `:738`) through the reduction, which is the one place the tri-state is
currently thrown away. This mirrors the `supports_thinking` gate already in `aart_to_meta`
(`:907-916`) and makes `src/catalog.rs:243`'s documented promise actually true. See *The rule
being changed, stated once* for the full truth table — implement THAT table, not a paraphrase.

`Model::is_selectable()` (item 5) is additive and exists only so the desktop's DTO can project the
state instead of re-deriving it. Task 4 is its first and only consumer.

**Files:**
- Modify: `src/harness/catalog.rs` — the `Aart` field (`:770-771`), the reduction (`:874-877`),
  `aart_to_meta` (`:918`), the reduction's doc comment (`:819-823`), and the tests at
  `:1546-1558` and `:1646-1652`
- Modify: `src/catalog.rs` — extract `Model::is_selectable`, rewrite `selectable()` (`:160-171`),
  fix the `DiscoveredMeta` doc at `:243`
- Test: both files' in-file `#[cfg(test)] mod tests` — `src/harness/catalog.rs:924-2062`,
  `src/catalog.rs:291-350`. **There is no `tests/` integration file for the catalog**; the
  repo-root `tests/fixture_bins.rs` is fixture-bin existence only. Do not create a new integration
  test file — the convention for this code is unit-in-file.

**What to implement:**

1. `src/harness/catalog.rs:770-771` — widen the field and fix its doc:

   ```rust
   /// `Some(true)` when EVERY deployment advertises `supports_function_calling: true`;
   /// `Some(false)` when ANY advertises `false` (or advertises `true` while a sibling
   /// stays silent); `None` when NO deployment advertises the key at all.
   pub(crate) supports_tools: Option<bool>,
   ```

2. `src/harness/catalog.rs:874-877` — the three-valued gate. Compute it from the same two facts
   the `supports_thinking` gate uses, and keep the vacuous case structural rather than a
   special-cased guard:

   ```rust
   // `None` (NOT `false`) when no deployment advertised the key: the reduction must not
   // fabricate a claim the proxy never made, and the downstream fallback is `true`
   // (`EffectiveCatalog::resolve`'s `unwrap_or(true)`), so a fabricated `false` here is what
   // made an unadvertised LiteLLM route unselectable (issue #9). The vacuous-ALL trap is now
   // structurally impossible: an empty set advertises nothing, so it yields `None` rather
   // than cargo-culting "all agree" into `true`.
   supports_tools: deployments
       .iter()
       .any(|d| d.supports_function_calling.is_some())
       .then(|| deployments.iter().all(|d| d.supports_function_calling == Some(true))),
   ```

   Leave `supports_thinking` (`:869-872`) exactly as it is, and add one comment line above the
   `supports_tools` initializer saying why the two differ — the asymmetry is real (the downstream
   fallbacks are `false` for thinking, `true` for tools) and without the sentence the next reader
   will "unify" them.

3. `src/harness/catalog.rs:918` — `supports_tools: route.supports_tools,` (drop the `Some(...)`
   wrapper; the field is already the tri-state, and re-wrapping would be `Option<Option<bool>>`).
   Do NOT add an `advertised` gate here — it moved into the reduction, and a second gate would
   double-compute it.

4. `src/harness/catalog.rs:819-823` — the reduction's doc comment says "the two capability gates
   are ALL-deployments (a route whose peers disagree … is not tool-selectable)". Amend it: peers
   DISAGREEING still gates; peers being SILENT does not. Keep one sentence naming the asymmetry
   reason (fallback `true` vs `false`).

5. `src/catalog.rs` — extract the predicate so the desktop can project it without duplicating the
   rule (Task 4 needs this). `WireApi` is already imported at `:42`; nothing in either repo defines
   this method today, so a `pub fn` on `Model` conflicts with nothing:

   ```rust
   impl Model {
       /// The ONE selectability predicate (ADR 0024 + 0026): tool support AND a wire the
       /// harness speaks. `ModelCatalog::selectable()` filters on this, and the desktop's
       /// `list_models` DTO projects it onto the picker — one authority, so a picker row can
       /// never claim a state the catalog disagrees with.
       pub fn is_selectable(&self) -> bool {
           self.supports_tools
               && self
                   .api
                   .as_deref()
                   .and_then(WireApi::parse)
                   .is_some_and(WireApi::is_supported)
       }
   }
   ```

   Then rewrite `ModelCatalog::selectable()` (`:160-171`) as
   `self.models.iter().filter(|m| m.is_selectable()).collect()`, keeping its doc comment and
   moving the wire-vocabulary prose onto `is_selectable`. The three tests
   `selectable_includes_the_three_wires` (`src/harness/catalog.rs:968`),
   `selectable_excludes_none_and_unknown_api` (`:990`) and `selectable_requires_supports_tools`
   (`:1006`) must pass UNCHANGED — they are the extract-method proof.

6. `src/catalog.rs:243` — the `DiscoveredMeta` doc claim "`None` on `supports_tools` preserves
   today's hardcoded `true`" becomes true with this change; annotate it as now honored for reduced
   LiteLLM routes (it previously described only the OpenAI/Anthropic shapes and the all-bare route).

7. Tests — update the two that pin the old collapse, and add the case the fix exists for:

   - `reduce_litellm_route_gates_tools_on_all_deployments` (`:1546-1558`): rewrite as
     `reduce_litellm_route_tools_are_three_valued_and_never_fabricate_false`, asserting the five
     rows of the truth table. The old `:1554` line
     (`assert!(!reduce_litellm_route("r", &[a.clone(), deployment("dep-c")]).supports_tools)`) IS
     the `None → false` pin being deleted — its replacement asserts `Some(false)` ONLY because `a`
     advertises `true`, and the comment must say so or the case reads as the deleted rule.
   - `reduce_litellm_route_empty_deployment_set_is_all_conservative` (`:1646-1652`):
     `assert!(!route.supports_tools)` no longer compiles on an `Option<bool>`; becomes
     `assert_eq!(route.supports_tools, None)` with the reason (nothing advertised). Leave its
     `supports_thinking` / `context_window` / `derived_wire` assertions alone.
   - Add one wiremock test beside `litellm_derived_wire_and_costs_land_on_the_meta` (`:1973`)
     named `litellm_unadvertised_tools_land_on_none_not_false`: a single `/model/info` entry whose
     `model_info` carries `max_input_tokens` but NO `supports_function_calling` →
     `meta.supports_tools == None` and `meta.context_window == Some(…)`. This is the end-to-end
     proof on the real rung-1 path (the pure tests cannot show that the META, not just the `Aart`,
     stopped lying). `is_bare()` (`:887-897`) does not short-circuit this fixture —
     `context_window` is set, so the route is not bare and it does reach the reduction.
   - `litellm_derived_wire_and_costs_land_on_the_meta` (`:1999`, both deployments advertise `true`)
     and `litellm_rung2_wildcard_ids_are_selectable_with_none_reduction_fields` (`:1944-1945`) must
     pass UNCHANGED — they are the no-regression witnesses.

**Steps:**
- [ ] Rewrite `reduce_litellm_route_gates_tools_on_all_deployments` as
      `reduce_litellm_route_tools_are_three_valued_and_never_fabricate_false` with the five
      truth-table cases, including `assert_eq!(…, None)` for the all-silent route and the empty set.
- [ ] Run `cargo test --lib harness::catalog::tests::reduce_litellm_route_tools`
  - Must FAIL: the `None` cases get `Some(false)` today. If it passes, stop — the assertion is not
    expressing the new rule.
- [ ] Add `litellm_unadvertised_tools_land_on_none_not_false`. Run
      `cargo test --lib litellm_unadvertised_tools_land_on_none_not_false`
  - Must FAIL with `left: Some(false), right: None`.
- [ ] Apply items 1-4 in `src/harness/catalog.rs`; run `cargo test --lib harness::catalog` —
      expect **40 passed, 0 failed** (39 today, measured on `b9babfe`; +1 for the new wiremock
      test, one test renamed).
- [ ] Add `Model::is_selectable` and rewrite `selectable()` in `src/catalog.rs`; run
      `cargo test --lib selectable` — expect the same **3 passed**, unchanged. If any of the three
      needed an edit, the extract was not behaviour-preserving.
- [ ] Update `src/catalog.rs:243`'s doc note.
- [ ] `cargo fmt --all -- --check` (run `cargo fmt --all` first if it fails).
- [ ] `cargo clippy --all-targets -- -D warnings` — this is the gate that catches a leftover
      `assert!()` on a non-`bool`.
- [ ] `cargo test` (bare — it runs BOTH workspace members via `default-members = [".", "fulcrum"]`;
      do NOT add `--workspace`, AGENTS.md forbids it).
- [ ] `cargo check --target x86_64-pc-windows-gnu --all-targets`
- [ ] `bash scripts/check-rquickjs-allocator.sh` and `bash scripts/check-schema-drift.sh` (both
      must stay green; this change touches no DDL and no dependency, so either going red means
      something unintended was edited).
- [ ] Commit on the feature branch:
      `fix(catalog): LiteLLM tool support is three-valued, not fabricated false (#9)`

**Acceptance criteria:**
- [ ] `Aart.supports_tools` is `Option<bool>`; no production line wraps it in `Some(...)`.
- [ ] A `/model/info` route with no `supports_function_calling` anywhere yields
      `DiscoveredMeta.supports_tools == None` (the new wiremock test).
- [ ] A route where one deployment says `false`, or one says `true` while a sibling is silent,
      still yields `Some(false)`.
- [ ] The three `selectable_*` tests pass with no edit to their bodies.
- [ ] `src/catalog.rs:243`'s `None`-means-status-quo claim is true for every discovery shape.
- [ ] Full bar green: `cargo test`, `cargo clippy --all-targets -- -D warnings`,
      `cargo fmt --all -- --check`, the windows-gnu check, both guard scripts.

**After this task: open the fulcrum PR and MERGE it before starting Task 2.** Record the merged
full SHA — Task 3 needs it.

---

### Task 2: desktop — amend ADR 0026

**Parallel:** wave 1 — disjoint from Task 3 (`docs/decisions/0026-litellm-api-mode.md` only).

**Precondition:** Task 1 merged to `fulcrum` `main`. An ADR describing an unlanded engine rule is
a decision record that contradicts the pinned code, and
`archimedes/docs/features/litellm-provider-connections.md:14-18` explicitly redirects readers to
this ADR for the discovery rulings.

**Context:**
ADR 0026's 2026-11-01 amendment (`docs/decisions/0026-litellm-api-mode.md:31`, the sentence at
`:39`) states the rule this plan deletes: "…and tool support only when **ALL** deployments carry
`supports_function_calling` (absent → `false`)". Amend in place; do not write a new ADR — the
decision being changed is a clause inside an accepted decision, and the file already carries two
dated amendment sections (`:31`, `:49`) whose style to follow. (For the record: `docs/decisions/`
runs to 0091 with 0090 absent, so the next free number is **0090**, not 0092 — but this task
creates no file.)

**Files:**
- Modify: `docs/decisions/0026-litellm-api-mode.md` — append an amendment section; amend the `:39`
  sentence in place so a reader cannot stop there and keep believing the wrong rule.

**What to implement:**

1. Append, matching the existing heading style exactly
   (`## Amendment (YYYY-MM-DD) — <what> (#<issue>)`, compare `:31` / `:49`):

   `## Amendment (2026-11-08) — absent `supports_function_calling` is `None`, not `false` (fulcrum#9)`

   It must state:
   - The `2026-11-01` amendment's tool clause is SUPERSEDED; the `min` / `max` / thinking clauses
     are untouched.
   - The rule now: `Some(true)` when every deployment advertises `true`; `Some(false)` when any
     advertises `false` **or** advertises `true` while a sibling stays silent; `None` when no
     deployment advertises the key. Silent ≠ negative; peers disagreeing is still conservative.
   - Why: `DiscoveredMeta.supports_tools`'s consumer resolves `None` to `true`
     (`EffectiveCatalog::resolve`'s `unwrap_or(true)`), so a fabricated `false` silently removed
     every model of a proxy that omits the key from the selectable set while Settings still listed
     it. The reduction's own principle — fabricate no claim the proxy never made — already forbade
     it; tools was the one field that violated it.
   - Why `supports_thinking` keeps its `bool` + `advertised` gate: its consumer resolves `None` to
     `false`, so the tri-state has no downstream effect there. The asymmetry is the fallbacks, not
     an oversight.
   - The desktop consequence: `ModelDto` now projects `Model::is_selectable()` + `supports_tools`
     and the session picker renders unselectable models disabled with the reason, so the two
     surfaces cannot disagree again. Name the picker's row source (the live catalog store fed by
     `list_models`, since #154) as the reason a hidden model became an *offered-but-unusable*
     model instead.

2. At `:39`, change the clause in place to
   "…and tool support only when **ALL** deployments carry `supports_function_calling`
   (absent → `false`; **superseded 2026-11-08 — see the amendment below**)". Keep the rest of the
   sentence so the original reasoning stays readable.

**Steps:**
- [ ] Read `docs/decisions/0026-litellm-api-mode.md` in full, plus `docs/decisions/0076-…` rule 1
      (the propagation rule finding #3 leans on) and
      `docs/features/litellm-provider-connections.md:14-18`.
- [ ] Re-read the LANDED engine rule (`git -C ../fulcrum show origin/main:src/harness/catalog.rs`,
      the `supports_tools` initializer) and write the amendment against THAT text, not against
      this plan's paraphrase. If the landed code differs from the plan, the CODE is right and the
      amendment follows it.
- [ ] Write the amendment; re-read `:39` and confirm a reader stopping there is pointed at the
      correction rather than left holding the old rule.
- [ ] `git diff` prose review: every code claim must match the landed shape.
- [ ] Commit: `docs(adr-0026): absent supports_function_calling is None, not false`

**Acceptance criteria:**
- [ ] A reader who reads only the `:39` clause is told the rule changed and where.
- [ ] The amendment states the full truth table including the "some advertise, one silent" row,
      which is NOT relaxed.
- [ ] It explains the `supports_thinking` asymmetry, so nobody "unifies" the two gates.
- [ ] No new ADR file; heading style matches `:31` / `:49`.

---

### Task 3: desktop — bump the `fulcrum-core` rev and flip the coupling witness

**Parallel:** wave 1 — disjoint from Task 2. `Cargo.lock` is this task's alone (Task 2 touches no
lockfile).

**Precondition:** Task 1 merged to `fulcrum` `main`. This task names that merge SHA, so it cannot
run before it exists.

**Context:**
Until the pin moves, the desktop builds the OLD engine and Task 1 changes nothing for a desktop
user. `src-tauri/Cargo.toml:33-39` states the repo rule: a full 40-char rev, bumped in the same PR
that reviews the engine diff; a branch or short rev would make a green `main` a statement about
whatever the branch pointed at that day.

There is exactly ONE desktop test whose expected value depends on which engine is pinned —
`resolve_propagates_reduced_supports_tools_and_costs_for_litellm`
(`src-tauri/src/agent/session/tests/mod.rs:3030-3050`), whose fixture is a single deployment with
NO `supports_function_calling` and which asserts `!user[0].supports_tools` with the message "a
deployment that never advertised `supports_function_calling` reduces to `false`". That test is the
cross-repo coupling witness: GREEN on the old pin, RED on the new one, so it tells you exactly
whether the bump landed. Verified by search across `src-tauri/tests/catalog.rs`,
`src-tauri/src/agent/session/tests/mod.rs`, `tests/ipc.rs` and `tests/session_native.rs`: the
other `litellm_*` resolve fixtures assert only `api`, `resolve_non_litellm_provider_byte_identical`
(`:3010`) is an Anthropic shape, and `catalog.rs:170` / `catalog.rs:217` assert a shape this
amendment does not touch.

**Files:**
- Modify: `src-tauri/Cargo.toml` (`:47`), `Cargo.lock` (the `fulcrum-core` source line, rewritten
  by cargo — this is the task's only lockfile touch)
- Modify: `src-tauri/src/agent/session/tests/mod.rs:3025-3050` — the assertion, its message, and
  its doc comment

**Steps:**
- [ ] **PRECONDITION GATE.** `gh api repos/archimedes-codes/fulcrum/commits/main --jq .sha`, then
      confirm the engine change is in `main`'s history
      (`git -C ../fulcrum fetch && git -C ../fulcrum log --oneline origin/main | grep -i three-valued`).
      If it is NOT merged, **report BLOCKED** with this precondition and stop — do not bump the pin
      to an unmerged branch head, and do not flip the test (it would go red against the pinned engine).
- [ ] Record the merged FULL 40-char SHA (`gh api repos/archimedes-codes/fulcrum/commits/main
      --jq .sha`, or `git -C ../fulcrum rev-parse origin/main` after a fetch). It MUST be full in
      the manifest (`src-tauri/Cargo.toml:34-39` explains why).
- [ ] Edit `src-tauri/Cargo.toml:47`; run `cargo update -p fulcrum-core`; confirm
      `cargo tree -i fulcrum-core` prints the new `?rev=` source (and NOT a local path — a stale
      `.cargo/config.toml` would make this proof meaningless; delete it first if you used one).
- [ ] Run `cargo test resolve_propagates_reduced_supports_tools_and_costs_for_litellm`
  - It must now FAIL with `left: true, right: false`. **That failure IS the proof the bump
    landed.** If it still passes, the bump did not take effect.
- [ ] Flip the assertion to `assert!(user[0].supports_tools, "a route that never advertised
      \`supports_function_calling\` keeps the status-quo \`true\` (ADR 0026's 2026-11-08
      amendment)")`, and rewrite the doc comment at `:3025-3029` — it currently says "reduces to
      `supports_tools: false` (the ALL-deployments gate)" and "a wrongly-`true` `supports_tools`
      would offer an unselectable wire"; the second clause is now inverted (the wire IS selectable
      and offering it is correct). Keep both cost assertions (`:3044-3049`) — the `max` rule did
      not change.
- [ ] Run `cargo test` from the repo root. Expect EXACTLY this test to have changed state; any
      other failure is a real cross-repo regression — read it, do not paper over it. In particular
      `src-tauri/tests/catalog.rs:217` (`reduction_lands_costs_and_tools_truth_on_discovered_meta`:
      one deployment `true`, one `false`) must STILL assert `Some(false)` and STILL pass — that
      route has an explicit `false`, which this amendment does not relax. `catalog.rs:170` (the
      `/v1/models` shape → `None`) must still pass.
- [ ] `cargo clippy --all-targets` (0 warnings), `cargo fmt --all -- --check`, `pnpm test`,
      `pnpm build`.
- [ ] Commit: `chore(deps): bump fulcrum-core for the three-valued LiteLLM tool gate (#9)`

**Acceptance criteria:**
- [ ] `src-tauri/Cargo.toml` names a full 40-char rev that is an ancestor of `fulcrum` `main`.
- [ ] `resolve_propagates_reduced_supports_tools_and_costs_for_litellm` asserts `true` for an
      unadvertised route, with the ADR clause cited in its message.
- [ ] `reduction_lands_costs_and_tools_truth_on_discovered_meta` still asserts `Some(false)` for a
      route with an explicit `false` — the non-relaxed row.
- [ ] `cargo tree -i fulcrum-core` shows the git rev, not a local path.
- [ ] Both repos' full bars green on the integrated desktop tree.

**Local cross-repo development (only if you must draft before the merge):**
`cp .cargo/config.toml.example .cargo/config.toml` (the `[patch]` form — `[paths]` cannot override
a git source and silently does nothing). The live file is gitignored **precisely so it can never be
committed**: committing it breaks every other machine and defeats the rev pin. Prove it is working
with `cargo tree -i fulcrum-core`, which must print a PATH, not `git+…?rev=`; cargo's
`Patch … was not used in the crate graph` means the table name does not match the dependency URL
character-for-character. **Delete the file before committing anything, and never treat a
patch-built green run as CI-proof** — CI compiles the pinned rev.

---

### Task 4: desktop — `ModelDto` carries selectability and its reason

**Parallel:** none — it calls `Model::is_selectable()`, which is only reachable at Task 3's rev
bump (a build-graph edge), and it edits `src/components/SidePane.test.tsx`, which Task 5 also
edits.

**Context:**
The session model picker's rows come from `list_models` → `ModelDto` → the live `useModelCatalog`
store (finding #1). `ModelDto` today carries `id / provider / contextWindow / supportsThinking /
thinkingLevels` (`src-tauri/src/commands/settings.rs:106-114`) — no tool-support and no wire field
— so the frontend literally cannot tell a usable model from an unusable one. This task puts both
facts on the wire. TWO fields rather than one: `Model::is_selectable()` is the STATE and
`supports_tools` is the REASON, and the picker needs both to label a row ("no tool support
advertised" is a different sentence from "unsupported wire"). One authority for the state, so the
DTO can never disagree with `selectable()`.

**Files:**
- Modify: `src-tauri/src/commands/settings.rs` — `ModelDto` (`:106-126`) + `list_models_dto_mapping`
  (`:993-1021`)
- Modify: `src/lib/tauri/types.ts` — `ModelDto` (`:835-841`)
- Modify: `src/store/modelCatalog.test.ts` — the typed `dto()` helper (`:14-19`)
- Modify: `src/components/AgentsDialog.test.tsx` — the typed `MODELS: ModelDto[]` (`:117-131`)
- Modify: `src/components/SidePane.test.tsx` — the typed `catalogModel()` helper (`:129-137`)
- Test: `list_models_dto_mapping` is the Rust-side test; no new Rust file.

**What to implement:**

1. `ModelDto` gains two fields, both projections, with doc comments naming their single authority:

   ```rust
   /// The tool-support marker — the picker's REASON cue (an unselectable model on a
   /// supported wire is a different sentence from one with no tool support).
   pub supports_tools: bool,
   /// `Model::is_selectable()` — the STATE. Projected, never recomputed here: a second
   /// copy of the predicate is how a picker starts offering a model the catalog refuses.
   pub selectable: bool,
   ```

   `From<&Model>`: `supports_tools: m.supports_tools, selectable: m.is_selectable(),`.
   `#[serde(rename_all = "camelCase")]` is already on the struct, so the wire names are
   `supportsTools` / `selectable` — add no per-field renames.

2. `src/lib/tauri/types.ts:835-841` — mirror both, REQUIRED (not optional). The Rust DTO is
   `Serialize`-only and always emits them, so `?:` would describe a case that cannot happen while
   forcing every reader to invent a fallback for it. (Task 5 still writes the defensive
   `selectable === false`, so a hypothetical missing field degrades to today's behaviour rather
   than disabling every row.)

   ```ts
   export interface ModelDto {
     id: string;
     provider: string;
     contextWindow: number;
     supportsThinking: boolean;
     thinkingLevels: string[];
     supportsTools: boolean;
     selectable: boolean;
   }
   ```

3. `list_models_dto_mapping` (`:993-1021`) — the fixture `Model` there sets
   `supports_tools: true, api: Some("openai-completions")`, so assert `v["supportsTools"] == true`
   and `v["selectable"] == true`, and add a SECOND projection of that fixture with
   `supports_tools: false` asserting `v["selectable"] == false` — the pair pins that `selectable`
   is the model's predicate and not a constant.

4. The three typed literals gain the two fields. Set `supportsTools: true, selectable: true` in all
   three — they are fixtures for provider/agent filtering, not for selectability, and an honest
   `true` keeps them out of the way. `src/lib/toolOutput.test.ts` (structural
   `ContextWindowCatalogEntry`, `src/lib/toolOutput.ts:263-267`) and the
   `vi.fn().mockResolvedValue([...])` mocks in `SettingsPage.test.tsx` / `PermissionsSection.test.tsx`
   / `SpacesList*.test.tsx` are UNTYPED and need NO edit — `tsc` will not flag them, and `pnpm
   build` is the proof.

**What NOT to change:** `list_models`' unfiltered `catalog.models.iter().map(ModelDto::from)`
(`:99`) — that is correct and is what makes Settings show every model. Do not filter it to
`selectable()`; that is the bug the issue reported, not the fix. Do not touch `refresh_live_models`
(`src-tauri/src/agent/session/mod.rs:2418-2463`) — it does not copy `supports_tools`, and after
Task 1 nothing on the loop side reads that field, so widening its gate would add frames and DB
writes to buy nothing (finding #3, and the residual recorded at the foot of this plan).

**Steps:**
- [ ] Confirm Task 3's bump is in this branch: `cargo tree -i fulcrum-core` prints the NEW rev. If
      not, STOP — `m.is_selectable()` does not exist on the pinned rev and the build will die with
      `no method named 'is_selectable'`.
- [ ] Edit the Rust DTO + `From` impl, then run `cargo test list_models_dto_mapping`
  - Compiles (the rev is bumped) but the new fields are unasserted. Add the assertions, re-run,
    expect pass.
- [ ] Edit `src/lib/tauri/types.ts`, then run `pnpm build` (`tsc && vite build`; `noUnusedLocals:
      true` makes a stale import a build failure. Plain `tsc` needs no `TMPDIR` — the tmpfs hazard
      below is vitest's, not tsc's).
  - `tsc` must now error on each typed `ModelDto` literal missing the fields — expect errors at
    `src/store/modelCatalog.test.ts:14`, `src/components/AgentsDialog.test.tsx:117`,
    `src/components/SidePane.test.tsx:129`. **If it names more files, fix what it names** — that
    list was read at `e51b7a08` and typed literals can be added by other work. If it names NONE,
    stop: the field went in optional, not required.
- [ ] Add the two fields to each named fixture; re-run `pnpm build` until clean.
- [ ] `pnpm test src/store/modelCatalog.test.ts` and `pnpm test src/components/SidePane.test.tsx`
      (scoped runs — a full `pnpm test` can report "172 failed / no tests" on a full `/tmp` tmpfs;
      `df -h /tmp` before blaming a test, and scope any `TMPDIR` override to the `pnpm` command
      only, never repo-wide, because four Rust tests need a tmpfs `/tmp`).
- [ ] `cargo fmt --all -- --check`, `cargo clippy --all-targets` (0 warnings), `cargo test` from
      the repo root.
- [ ] Commit: `feat(models): ModelDto carries selectability and its reason`

**Acceptance criteria:**
- [ ] The `list_models` payload carries `supportsTools` and `selectable` (camelCase) for every
      model, and `list_models_dto_mapping` asserts both plus one `selectable == false` projection.
- [ ] `ModelDto.selectable` is produced ONLY by `Model::is_selectable()` — grep proves no inline
      re-derivation of the predicate anywhere in the desktop.
- [ ] `pnpm build` clean, `cargo test` clean, 0 clippy warnings.

---

### Task 5: desktop — the picker marks unselectable models and says why

**Parallel:** none — consumes Task 4's `ModelDto.selectable` / `supportsTools`, and shares
`src/components/SidePane.test.tsx` with Task 4.

**Context:**
Finding #2. `ModelPickerItem.disabled` exists and `ModelPickerDialog` honours it completely (`:113`
refuses the select, `:159` passes it to `RowButton`; pinned by `ModelPickerDialog.test.tsx:162` "a
disabled row is NOT selectable") — but nothing sets it, because `SessionConfigBlock`'s `catalogItems`
mapping (`:218-227`) has no data to set it from. Task 4 supplied the data. This task closes the loop
so the picker is honest: every catalog model stays VISIBLE (that is the point — the issue's
complaint was silent absence), and the unusable ones are greyed with a one-line reason.
`ModelPickerItem` needs a `note` field for the reason; the row already renders a muted `(provider)`
cue to sit beside (`ModelPickerDialog.tsx:176-177`).

**Files:**
- Modify: `src/components/ModelPickerDialog.tsx` — `ModelPickerItem` (`:14-19`) + `renderRow`
  (`:135-181`)
- Modify: `src/components/SessionConfigBlock.tsx` — `catalogItems` (`:218-227`)
- Modify: `src/components/SidePane.test.tsx` — `catalogModel()` gains optional args; new cases in
  the config-block describe block (near `:1683`)
- Test: `src/components/ModelPickerDialog.test.tsx` — a `note` rendering case

**What to implement:**

1. `ModelPickerItem` gains `note?: string` — "shown muted after the provider cue; the caller's
   explanation for a `disabled` row (or a caveat on an enabled one)". Optional, so
   `modelItemsFromCatalog` (`:49-65`), which builds items without a note, keeps compiling, and
   `SessionConfigSelect`'s `items?: ModelPickerItem[]` prop (`:83`, `:91`) is unaffected.

2. `renderRow` (`ModelPickerDialog.tsx:165-180`): render `note` in the same muted span pattern as
   the provider — `{" "}` + `"(" + note + ")"` inside `text-foreground-subtlest`, only when
   `item.note !== undefined`. Keep the `cursor-pointer` / `cursor-not-allowed` template literal
   EXACTLY as it is (the comment at `:139-157` explains why `cn()` must not be introduced there).

3. `SessionConfigBlock.tsx:218-227` — the mapping gains the two derived fields:

   ```tsx
   .map((m) => ({
     value: `${m.provider}/${m.id}`,
     name: m.id,
     provider: m.provider,
     // `=== false`, never `!m.selectable`: the two differ only when the field is
     // ABSENT, and the absent-field failure mode would be a fully disabled picker.
     // Same "absent is not a claim" rule the engine reduction just adopted.
     disabled: m.selectable === false,
     note:
       m.selectable !== false
         ? undefined
         : m.supportsTools === false
           ? "no tool support advertised"
           : "unsupported wire",
   })),
   ```

   Leave the memo's dependency array (`[catalogModels, configuredProviderIds]`) alone — do not add
   to it (`SessionConfigSelect` is `memo`ized; `:212-217` explains why a fresh array is a perf bug).

4. `SidePane.test.tsx` — `catalogModel(id, provider = "tama", selectable = true,
   supportsTools = true)` plus two new cases:
   - an unselectable model (`selectable: false, supportsTools: false`) still appears in the opened
     picker, carries `aria-disabled="true"`, its text includes `no tool support advertised`, and
     clicking it does NOT call `setSessionConfigOption`;
   - a selectable model still selects normally (the guard must not have become a blanket disable).
   Keep the existing `#154` cases at `:1683-1730` passing UNCHANGED — they are the "still visible"
   witnesses.

**What NOT to change:** `SessionConfigSelect` (it forwards `items` untouched, `:272-281`);
`synthesize_catalog_config_options` (its `options` list stays the `selectable()` set — the
harness's own snapshot is a different authority from the live catalog, and it is only the fallback
path); `set_config_option` (it intentionally resolves against `catalog.models` so a stored
session's model still resolves — a disabled row simply never calls it).

**Steps:**
- [ ] Add the `note` case to `ModelPickerDialog.test.tsx` (a `disabled` + `note` item shows the
      note text and keeps `aria-disabled="true"`). Run
      `pnpm test src/components/ModelPickerDialog.test.tsx`
  - Must FAIL: no note is rendered today.
- [ ] Add `note` to `ModelPickerItem` and render it; re-run until pass.
- [ ] Add the two `SidePane.test.tsx` cases. Run `pnpm test src/components/SidePane.test.tsx`
  - Both must FAIL before step 4 (`disabled` is never set today, so the row is clickable). If they
    pass immediately, the fixture is not producing an unselectable model.
- [ ] Update `catalogItems`; re-run `pnpm test src/components/SidePane.test.tsx` until pass.
- [ ] `pnpm build` (clean), then `pnpm test` (full; `TMPDIR` scoped to the command, `df -h /tmp`
      first — "172 failed / no tests" is a full tmpfs, not a regression).
- [ ] Commit: `fix(fe): the model picker marks unselectable models and says why`

**Acceptance criteria:**
- [ ] Every catalog model still appears in the session picker (nothing newly hidden).
- [ ] A `selectable: false` row is `aria-disabled`, suppresses `setSessionConfigOption`, and shows
      `no tool support advertised` when `supportsTools === false`, `unsupported wire` otherwise.
- [ ] A `selectable: true` row still selects.
- [ ] The absent-field failure mode is fail-open (`selectable === false`, not `!selectable`).

---

## Sequencing

```
fulcrum:  feature/9-litellm-tool-support-tristate
            Task 1 ──► PR ──► MERGE ──► SHA R
                                          │  (the rev the desktop must name)
                                          ▼
archimedes: feature/9-picker-selectability
            Wave 1 ─┬─ Task 2 (ADR 0026 amendment)
                    └─ Task 3 (rev bump → R + flip the witness)
                       ▼ fan-in, full desktop suite
                  Task 4 (ModelDto: selectable + supportsTools)
                       ▼
                  Task 5 (picker: disabled + reason) ──► PR ──► merge
```

Tasks 2 and 3 are one wave. Task 1's merge is the gate on the desktop branch's existence, not just
on Task 3 — so the desktop PR cannot be opened before it (see *The ordering constraint*).

## Known residuals (record, do not fix here)

- **A mid-session `supports_tools` flip is not pushed to a live session.** `refresh_live_models`
  copies three fields and not `supports_tools` (`src-tauri/src/agent/session/mod.rs:2441-2444`).
  Correct today — the field's two readers (`Model::is_selectable()` as the selectability authority
  and the `ModelDto` projection as the reason cue) both run on the catalog, and nothing on the
  `AgentLoop` side reads the loop's copy — but if a future consumer reads that copy, this becomes a
  stale-value bug with ADR 0076's exact shape.
- **The disabled row shows a pointer cursor.** `ModelPickerDialog.tsx:139-157` documents that
  `cursor-pointer` wins the tailwind-merge group by CSS source order and that fixing it is a
  behaviour change no test owns. Out of scope; the note is already in the file.
- **`selectable()` remains the authority for `resolve_native_model`'s last rung**
  (`src-tauri/src/agent/session/mod.rs:2869-2876`). After Task 1 an unadvertised LiteLLM route is a
  legitimate first pick there, which is the intended fix — but it means a proxy with ONE model and
  no advertised tool support now becomes the default where it previously produced the "no models
  available for the native session" error. That is the correct outcome (the harness speaks
  `openai-completions` and the proxy serves it), and it deserves a line in the PR body so a
  reviewer is not surprised by a previously-erroring config starting to work.

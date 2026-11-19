# Abandoned tool rows — durable notes

Landed on `feature/11-settle-announced-tool-calls` against
[issue #11](https://github.com/archimedes-codes/fulcrum/issues/11), in two commits: the fix
(`close_announced_tool_calls`, `src/harness/loop/turn.rs`, reached from both settle functions as they then stood) and the
module that pins
each exit (`src/harness/loop/tests/tool_row_settle.rs`, 7 tests). This file is the permanent residue — the invariant,
the derivation behind the exits that close nothing, the three constraints that break if the helper is tidied, and the
residual this crate cannot reach. The plan that drove it (`docs/roadmap/settle-announced-tool-rows.md`) went at ship
like every plan in this family; `git log` has it. §2 below is that plan's settle-site table, which is why the code
comments cite this file and not the plan.

**§2 gained a ninth site later.** [issue #190](https://github.com/archimedes-codes/archimedes/issues/190) added
`settle_truncated` (`turn.rs`), the third settle — a turn the provider's output cap cut off. It is the only settle whose
`announced` argument is neither a hand-written `&[]` nor an abandoned attempt's calls: the calls it closes were
ANNOUNCED NORMALLY and are never dispatched because Decision 2 forbids dispatching them, not because the attempt died.
That is why §1's "one close implementation" still holds and why §2's vacuity derivation needed no repair.

**Provenance, and why the sentence "every line number below was read in this tree" is no longer in this file.** The
original note read every number against `a245180` (tests in `2334d83`), and said so; the plan its §2 table came from had
written its references against `45e576d`, and they had all drifted. Where a claim came from the desktop repo the note
says so and names the commit. Four later passes each re-read the numbers again — the #190 pass (§1–§4, plus §6's desktop
claims), the turn-loop-bound pass (§1–§4, §6–§8), the refusal-guard pass and this one — and each shift made the next
reader's re-reading necessary rather than optional. That is the finding: a claim of the form "these numbers were verified
in the tree at the time of writing" is TRUE for exactly one commit and is the most dangerous sentence in a document,
because it reads like a warranty. There are no engine-side line numbers left here to make that claim about.

**And they rotted again immediately, which is why the §1/§3 numbers went first.** The refusal-guard pass (the one that
added `refused_before_the_model_could_act` and `missing_required_param` to `turn.rs`) shifted every `turn.rs` line below
by up to +159 and every `loop.rs` line by +53, and its two new tests shifted the `tool_row_settle.rs` /
`turn_lifecycle.rs` figures too. Four passes in a row have now each re-read these numbers and each left its own
successors stale, so re-listing them would be maintaining a counter that is wrong on every commit that touches either
file. What survives is what a reader can actually resolve: `fn` names, `match`-arm names, and test names.

**The pass that removed the rest — and the count.** A review pass found that §2's ten-site table and §10 had gone the
way of §1's and §3's: the commit that bounded the malformed-arguments defect added ~45 net lines to `turn.rs` without
touching this file. Re-measured against that tree before the numbers were deleted, **all ten of §2's rows were wrong**,
not the four a reviewer's spot-check named — and every `loop.rs` figure had moved too, so the reviewer's guess that the
`loop.rs` rows had survived was also wrong. The reviewer's own corrected numbers were worse than the ones they replaced:
offered for `settle_truncated`, `close_announced_tool_calls`'s docblock and `run_tool_batch`'s dispatch loop, they were
all **~150 lines low**, because they were taken before the two guard commits that landed after the review. What this
paragraph used to do next is the trap it exists to argue against: it cited the three corrected numbers as evidence, and
two of the three were already stale in the commit that wrote them. They are DELETED here, not renumbered — a
re-derivation is good for exactly one commit, and this document has now demonstrated that four times.

§2's table, §2's prose, §11's table, and the surviving `turn.rs` / `loop.rs` /
`normalize.rs` / `compact.rs` / `retry.rs` citations in §3, §4, §7, §8 and §10 are therefore now **number-free**,
resolved by `fn` name, `match`-arm name or test name — the treatment §1 already had. §11's first column is now §2's site
NUMBER, because that is the stable identifier and it is what the source comments say out loud ("Site 7 of the ten").

**How many were removed, stated as a rule rather than a number.** This sentence used to assert **72** distinct
`turn.rs` / `loop.rs` citations removed, plus a 36/36 split between "provably not the construct they named" and "checked
by hand". Neither figure is reproducible from `git show c34254e -- docs/features/abandoned-tool-rows.md`, and the
reviewer's own recount (67 occurrences / 49 distinct) disagrees with it too — which is the expected outcome, because
"how many citations were removed" has no answer until someone fixes what counts as a citation. The number a reader CAN
reproduce is this one:

    git show c34254e -- docs/features/abandoned-tool-rows.md \
      | grep '^-' | grep -oE '[A-Za-z_/.-]+\.(rs|ts)(:|[[:space:]]+)(line[[:space:]]+)?[0-9]+' | wc -l

**59 occurrences / 45 distinct** on that rule (any `.rs` or `.ts` token immediately followed by a number, counting
ranges once at their head). Change the rule and the number changes — restricting it to the five engine files named above
gives 56 / 42 — so the rule is the claim and the number is only its consequence. The 36/36 sub-claim is dropped rather
than restated: nothing in the diff witnesses it.

This is not a cosmetic preference: production comments cite §2 by number ("site N of the ten in
`docs/features/abandoned-tool-rows.md` §2"), so the table's accuracy is a code-comment property, and a table whose rows
point at prose teaches the next reader to distrust the citation that is supposed to justify a `&[]`. Hence the rule for
anyone editing this file: **add no line number, and none needs re-checking** — name the function.

**§5, §6 and §9 are the exception and remain NOT re-read.** §5's and §9's citations name `pump.rs`,
`worker/client.rs`, `subagent_flow.rs`, `worker/manager/mod.rs`, `config_types.rs`, `worker/store.rs` and the daemon's
`persist.rs` / `db/` paths — files outside the two this branch kept editing, but whose numbers were copied from the plan
and never verified in this tree. Two are already known false: the pump lives at `worker/manager/pump.rs` (not
`worker/pump.rs`), and the ring `client.rs` builds is at its `broadcast::channel(256)` line, which is not the number §5
gives. Treat every number in those two sections as a hint to grep for, never as a location.

**§6 is on this list too, and it is named here explicitly because it was in neither list before.** It carries numbers
that a reader applying the rule above would otherwise have no instruction about: `src/lib/subagentActivity.ts:302-309`
and `:310-317`, `src-tauri/Cargo.toml:121`, and the two engine-side numbers at the end of §5 —
`DEFAULT_SUBAGENT_DEAD_BOUND` at `worker/manager/mod.rs:70-71` and `default_subagent_dead_bound_secs` at
`config_types.rs:258`. They were checked when this paragraph was written and all four are CORRECT — including the two
engine-side ones, which is why they survived the sweep that deleted the rest. Correct-by-luck is not the same as
exempt: §6's `.ts` figures are the least fragile things here because they are read out of a PINNED blob
(`git show 744aac23:src/lib/subagentActivity.ts`, which the section says out loud), and `Cargo.toml:121` is pinned to
`v0.13.0` the same way. The engine-side pair is the genuinely fragile kind — it moves with any commit that touches
either file — so it is the one to grep for rather than to read.

Statuses: **open work** = someone must do it · **recorded** = a known limit, deliberately not fixed ·
**accepted** = decided, will not change here.

---

## 1. The invariant, and the one place the close lives

**A turn never settles with an announced tool row left open.**

The frames that carry it, in order. A `tool_call` ANNOUNCE frame opens a row at `status: "in_progress"` when the model
finishes NAMING a call: `consume_stream`'s `ProviderEvent::ToolCall` arm (`turn.rs`, the `ProviderEvent::ToolCall(tc)`
arm of `consume_stream`'s match) pushes the call onto `acc.tool_calls`, emits `toolcall_end` and emits
`tool_execution_start` — and `normalize` renders EITHER one into a `tool_call` frame at `in_progress` (the
`Some("toolcall_end")` arm of `normalize`'s `message_update` match, and the `RpcEvent::tool_execution_start` arm). The
row leaves `in_progress` on exactly one frame: the `tool_call_update` carrying `completed`/`failed` that `normalize`
derives from `tool_execution_end` alone (the `RpcEvent::tool_execution_end` arm). `normalize` has no arm for `turn_end`
or `agent_settled` — grep over that file finds neither variant named as a `match` pattern anywhere, only in prose (the
doc comment on `normalize` that lists the no-frame events, the comment above the catch-all, and the test that pins those
events produce no frame) — so both fall into the catch-all `_ => Vec::new()` (the last arm of
`normalize`'s own `match`), which is also the shape the display-row scanner carries (`compute_display_rows`'s own final
`_ => Vec::new()`). A settle therefore says nothing about the rows the attempt opened, and the row
spins forever: in the desktop, a Subagent frozen on a stale tool name with a climbing duration while its tool count and
cost keep rising.

**The bare line numbers this section used to carry are GONE, deliberately** — see the note on citation rot at the top of
this file. Arm names are what a reader should grep for here: `normalize`'s arms are named by their patterns, and a
pattern does not move when the file above it grows.

The close now lives in ONE function — `close_announced_tool_calls` (`turn.rs`, `close_announced_tool_calls`; a private
`fn`, see below), which emits `tool_execution_end { tool_call_id, tool_name, result: abandoned_tool_result(), is_error: true }` per call
— and it is reached from five places: FOUR direct calls — `settle_cancelled` and `settle_error` (`turn.rs`, each
calling it as the FIRST statement of its body, BEFORE its own `turn_end`), plus the two abandon paths that `Continue`
instead of settling (`handle_mid_stream_error`'s overflow recovery, and `retry_abandoned_attempt`'s backoff-completed
arm) — and, through `settle_error`, the third settle `settle_truncated` (a delegation; see §10). Every one of the five
emits its close BEFORE `turn_end`. The
ordering is the whole content of the invariant: `normalize` closes a row on `tool_execution_end` and on
nothing that follows it, so a close emitted after `turn_end` would be a close that never lands.

**FIVE, and the tenth settle site does not make it six.** This count is of the close's CALLERS, not of the settle
sites, and §2's site 10 (the turn-loop bound) reaches the close through `settle_error` — already on this list — rather
than by adding a call to `close_announced_tool_calls`. The number that moved with site 10 is §2's (`grep` finds ELEVEN
settle call lines behind TEN sites), not this one; a reader who "fixes" five to six here would be describing a call that
does not exist.

The helper stays a private `fn` (`close_announced_tool_calls`, not `pub(super)`) and that visibility is the invariant: a child module's
private method is invisible to its parent, so `loop.rs` cannot reach a close at all — it must go through a
`pub(super)` settle, and it therefore cannot open a close without settling. Adding a fourth settle costs nothing in
reachability, which is exactly what kept site 9 from becoming a second close implementation.

The bug's clearest statement was an asymmetry inside one function. `retry_abandoned_attempt`'s cancel arm called
`settle_cancelled` and returned, skipping the close loop a few statements below: the path where the backoff COMPLETED
closed this same attempt's rows and the path where it was CANCELLED did not. Same function, same abandoned attempt, same
`acc`. `both_paths_of_retry_abandoned_attempt_close_the_announced_tool_calls_identically` is the pin, written as a PAIR
over one shared assertion so an edit that reintroduces the asymmetry reddens whichever arm it broke, by name.

## 2. The ten settle sites, which five are non-vacuous, and the derivation of the other five

`settle_cancelled` / `settle_error` / `settle_truncated` are the three settles, and there are exactly ten settle SITES
— five in `turn.rs`, five in `loop.rs`. Five pass the attempt's real `acc.tool_calls`; five pass `&[]`, and each of
those carries its derivation at the call site so the emptiness stays a fact rather than becoming a hope a new call
site can invalidate. `grep` over `src/harness/` finds ELEVEN call lines behind those ten: the ten, plus the one inside
`settle_truncated` that delegates to `settle_error` (§10) — the eleventh is not a site, it is how site 9 gets its close,
and it passes `announced` through untouched.

**§2 gained its tenth site with the turn-loop bound.** The bound's settle (`loop.rs`, the top-of-loop round / identical-
refusal guard) is the fourth producer of a new `RpcEvent` (`turn_capped`) and the fifth vacuous site. It is vacuous by
the SAME argument as 5, 6 and 7 — it fires at the top of an iteration, before the model call that declares `acc` — and
its RESULTS argument is the one thing about it that is NOT empty, which is what makes it worth a row: a bounded turn is
the longest turn a prompt can produce, so `settle_error` must be handed `std::mem::take(&mut state.turn_tool_results)`
and a `Vec::new()` there would silently drop every tool result the turn ever earned. Pinned by
`a_capped_turn_closes_nothing_and_carries_the_prior_results` (`tool_row_settle.rs`), which asserts the count of
`turn_end.tool_results` rather than merely its presence, precisely so that regression reddens.

**WHICH of the bound's two guards catches WHICH shape, because the two are
routinely conflated.** The round ceiling (`MAX_MODEL_ROUNDS_PER_PROMPT`) catches
everything eventually and catches nothing early; the identical-refusal run
(`MAX_IDENTICAL_REFUSAL_RUNS`) is the one that stops a stuck turn in four rounds,
and it fires only on the three shapes `refused_before_the_model_could_act` (`turn.rs`)
recognizes — an extraneous top-level key, a parse `defect`, or a dispatched call
that omitted an advertised `required` parameter and came back `is_error`. The
field report this whole change was written for (`bash` with `arguments: {}`, 303
times) is the THIRD shape, and for one commit it was not caught: `{}` has no keys
for the pre-dispatch checker to call extraneous, so the call was dispatched, the
executor refused it, and a tool-RESULT error reset the run to 0 every round — the
burst ran to 64 rounds instead of 3. See `MAX_IDENTICAL_REFUSAL_RUNS`' docblock
for the full statement of what is caught, what is deliberately NOT (`is_error`
alone), and the fact that the bound is PER PROMPT rather than per session.

| # | Site | Announced rows possible? | Why |
| - | ---- | ------------------------ | --- |
| 1 | `turn.rs`, `compact_if_due`, overflow `Skipped` | no | no accumulator in scope |
| 2 | `turn.rs`, `handle_mid_stream_error` fall-through | **yes** | non-retryable mid-stream error |
| 3 | `turn.rs`, `retry_abandoned_attempt`, cancel arm | **yes** | cancelled inside the backoff |
| 4 | `turn.rs`, `retry_abandoned_attempt`, budget exhausted | **yes** | the attempt is abandoned for good |
| 5 | `loop.rs`, `handle_turn`, top-of-loop cancel | no | no accumulator in scope |
| 6 | `loop.rs`, `handle_turn`, model call races the cancel | no | the stream was never opened |
| 7 | `loop.rs`, `handle_turn`, model call returned `Err` | no | no stream to consume |
| 8 | `loop.rs`, `handle_turn`, cancel after `consume_stream` | **yes** | Stop pressed mid-stream — the field report |
| 9 | `turn.rs`, `handle_truncated_turn` → `settle_truncated` → `settle_error` | **yes** | output-cap truncation — issue #190; the same close discipline as site 4, one delegation further down |
| 10 | `loop.rs`, `handle_turn`, the turn-bound guard (round ceiling / identical-refusal run) | no | no accumulator in scope — but its `tool_results` are every prior batch's, not empty |

**Every row is a `fn` (or, for 3 and 4, an arm inside one `fn`) plus the file, and deliberately no line number.** Each
row's function is also the place its own "Site N of the ten" comment lives in the source, so the table and the comments
resolve to the same location by grep alone. Sites 3 and 4 are the two `return`s out of `retry_abandoned_attempt`'s
backoff `select!` — 3 is the `_ = turn.cancelled()` arm, 4 the statement after the `select!` — and both are named in
§1's five-callers list.

Site 8 is the exit the report is about, and it was NOT in the issue's list of four; site 2 is the same shape reached by
error rather than by Stop.

**Site 9 is non-vacuous for a different reason than 2, 3, 4 and 8, and the difference matters to the derivation.** The
other four pass `&acc.tool_calls` because their attempt was ABANDONED — it died, or it was cancelled, or its budget ran
out, and the rows it opened would otherwise never be closed. Site 9's attempt did none of those: `consume_stream` ran to
completion (the `self.consume_stream(&mut stream, turn, &mut acc)` call in `handle_turn`, step "(3) Consume stream") and
announced each call normally, and the calls are never dispatched because Decision 2 (ADR 0108) FORBIDS dispatching a
call whose `arguments` the output cap cut off mid-object — dispatching it would produce the executor's misleading
``missing `path` or `content` parameter`` refusal rather than an honest one. So the close is not a leak-repair here, it
is the ONLY legitimate ending those rows can get, and `announced` cannot be `&[]`: `handle_truncated_turn` reaches no
batch at all (its `TurnFlow::Return` reaches `handle_turn`'s `return` in the `FinishReason::Length` arm, which sits
ahead of the `run_tool_batch` call in step "(4) Tool execution"), so nothing else emits a `tool_execution_end` for them.
Pinned by `a_truncated_turns_tool_calls_are_closed_as_abandoned_and_never_dispatched` (`turn_lifecycle.rs`,
`a_truncated_turns_tool_calls_are_closed_as_abandoned_and_never_dispatched`), which
carries a counting `SubagentDispatcher` and asserts the counter is ZERO — the close is proven, and the non-dispatch is
proven, in the same run.

The vacuity of 1, 5, 6, 7 and 10 is structural, and stronger than the argument the plan carried. `acc` is declared in
`handle_turn` AFTER the model call — after the compaction check (step "(1) Compaction check"), after the model-call
`select!` (step "(2) Model call") and after that `select!`'s `Err` arm — so at those four sites there is no accumulator
to pass, and `consume_stream`, the ONLY place a `tool_call` row is opened, has not run. A previous iteration cannot have
left a row open either, for two independent reasons, both checked in the source rather than assumed:

* **`run_tool_batch`'s dispatch loop has no early exit.** The loop is the
  `for (idx, tc) in tool_calls.iter().enumerate()` in `run_tool_batch` through its `self.persist_transcript_message()`,
  and scanning that whole body finds no `break`, no `return`, no `continue`, no `?`: every iteration reaches the
  `RpcEvent::tool_execution_end` emit at the end of its body, so the batch closes every call it was handed. That
  includes a cancelled turn — a cancelled call still gets a result, `cancelled_tool_result()` in the `subagent` arm's
  `None if turn.is_cancelled()` match arm (a `subagent` whose child never ran) and again in the sequential tools'
  `else if turn.is_cancelled()` arm — and a result reaches the same unconditional emit. The rule "cancellation beats
  rejection" is what keeps this true: the cancel arms precede the rejection arm, so a cancelled call produces
  `cancelled` rather than producing nothing.
* **`acc.tool_calls` is emptied before the batch runs, and `acc` is re-declared every iteration.**
  `std::mem::take(&mut acc.tool_calls)` in `handle_turn` immediately after its `message_end` emit; the batch runs on the
  taken vec at the `run_tool_batch` call in step "(4) Tool execution". Nothing between the take and the end of the
  iteration refills the accumulator — `acc.tool_calls` is pushed to in exactly one place, `consume_stream`'s
  `ProviderEvent::ToolCall` arm — and the only exit in that range is the clean-stop `return`, which is gated on
  `tool_calls.is_empty()` on the taken vec. The next iteration gets a fresh `TurnAccumulator::default()` anyway.

The paths into a later iteration are exhaustible and each is covered: the pre-stream overflow `continue` (the
`Err(ProviderError::ContextOverflow(_))` arm of the `match` on the model call, before any stream exists),
`handle_mid_stream_error`'s `Continue` (→ closes at its own `ContextOverflow` arm, or at
`retry_abandoned_attempt`'s post-`select!` close), `handle_empty_turn`'s `Continue` (→ `retry_abandoned_attempt` →
closes at that same post-`select!` close; and its own batch is empty because the arm is reached only through the
`acc.text.is_empty() && acc.tool_calls.is_empty()` gate), the Length gate's `Continue` arm (`handle_truncated_turn`
returning `TurnFlow::Continue` — DEAD today, because `handle_truncated_turn` ends in `settle_truncated` +
`TurnFlow::Return` unconditionally, and covered even if it were live because the close has already run by the time
`settle_truncated` returns — through `settle_error`, per §10), and the fall-through after the batch.

**Three more `agent_settled` emissions are not among the ten, and they are empty too.** Production emits
`agent_settled` at five source lines (`run()`'s cancel arm, `handle_prompt`'s already-cancelled guard, and the clean-stop
`return` in `handle_turn`, plus the one each in `settle_cancelled` and `settle_error`), behind TEN call sites — the tenth of them to be counted here,
`settle_truncated`, emits it through `settle_error` rather than from a line of its own (§10), so a reader auditing
"every settle" by grepping that frame finds five emissions behind ten call sites and should not have to re-derive
which are which. The clean-stop emission in `handle_turn` is the accepted clean stop, gated on
`tool_calls.is_empty()` on the taken vec; `run()`'s cancel arm settles prompts the Stop drained from the
queue, and `handle_prompt`'s guard settles the prompt it skipped on an already-cancelled token — in both, no turn body
ran, so nothing was announced. That clean-stop emission is also the one a `Length` turn NO LONGER reaches: pre-#190 a
truncated turn fell through the emptiness gate (whose first conjunct then demanded `Some(Stop)`) and settled there as a
success triple, which is the bug — see §8.

**The pointer this note flagged for retargeting has been retargeted.** `settle_cancelled`'s doc comment used to send a
reader to the settle-site table "in `docs/roadmap/settle-announced-tool-rows.md`", and that plan
file is deleted on ship, as every plan in this repo family is. This file is its successor, so every comment that cited
the plan cites §2 here instead: `turn.rs` three times (`compact_if_due`'s site-1 derivation, `handle_truncated_turn`'s
site-9 note, `settle_cancelled`'s `announced` paragraph), `tool_row_settle.rs` once (its module docs) and
`worker/manager/subagent_flow.rs` once (its liveness-set residual note).

## 3. Three constraints on the helper that are load-bearing — **recorded**

Each is here because of what breaks if the helper is tidied, not because the shape is pretty. All three are stated on
the helper itself (`turn.rs`, `close_announced_tool_calls`'s docblock) and each has a test that reddens when it
is violated.

* **Iterate the ordered `Vec<ToolCall>`, never `TurnState::announced_tool_calls`.** That field is a `HashSet<String>`
  (`normalize.rs`, `TurnState::announced_tool_calls`) and it is insert-only: the only writes anywhere in the crate are the four `announced_tool_calls.insert` sites —
  the `Some("toolcall_start")` arm, the `Some("toolcall_end")` arm, the `RpcEvent::tool_execution_start` arm and the
  `RpcEvent::compaction_start` arm of `normalize`'s `match` — and the only `.remove(` calls in `normalize.rs` hit
  `toolcall_args` (the `Some("toolcall_end")` arm clears its buffer) and the display-store accumulators (the
  `Some("message_retracted")` arm of `compute_display_rows`, which clears both the text accumulator and the
  `ThoughtState` segments). Nothing clears it per attempt or per turn either: the production `TurnState` is built once,
  in `AgentLoop`'s own construction (the `turn_state` field of the `AgentLoop` initializer), and lives for the whole
  Worker (the only other construction is the nested-call sink's own fresh state, `loop/emit.rs`, the
  `turn_state: StdMutex::new(TurnState::default())` field of its initializer). And it does not hold only tool calls — `compaction_start` mints a `compact:{n}` card id
  (`normalize`'s `RpcEvent::compaction_start` arm) and inserts it into the same set, because a compaction renders as a tool card. Closing
  from the set would therefore fake a failed tool close for every compaction card the session ever opened, in an
  arbitrary order, and `compute_display_rows`' `tool_call_update` arm `merge_json`s onto the card — so the
  forged close would overwrite the summary `compaction_end` had put there. The `Vec` is transcript order, which is what
  the desktop reads these frames as; `the_closes_arrive_in_accumulator_order_and_every_one_precedes_the_settle` declares
  its ids `zzz-`/`mmm-`/`aaa-` so declaration order is the REVERSE of sorted order and no set, hash, sort or reverse can
  produce the asserted sequence.
* **Frames only, never transcript rows.** No `ChatMessage { role: Tool }` is pushed and nothing is appended to
  `turn_tool_results` — contrast `run_tool_batch`, which pushes the `ChatRole::Tool` row right after its
  `RpcEvent::tool_execution_end` emit and persists it with the `self.persist_transcript_message()` that closes the
  iteration. An
  abandoned attempt persists NO assistant row, so a tool-result message would dangle with nothing for the provider to
  pair it against — the orphan a provider 400s on. Pinned by `an_abandoned_tool_call_appends_no_tool_message`
  (`compaction_overflow.rs`, `an_abandoned_tool_call_appends_no_tool_message`), which reloads the transcript from the store and asserts both that no `Tool` row
  exists and that the persisted assistant row's `tool_calls` is `None`. Site 9 is the one route that DOES persist an
  assistant row, and it does not weaken this: `handle_truncated_turn` pushes its row with `tool_calls: None`
  (the `tool_calls: None` field of its `ChatMessage { role: ChatRole::Assistant }`, with the comment naming Decision 2), so there is still nothing a tool result could pair against, and the same test's `None` assertion is
  what keeps that honest rather than incidental.
* **`abandoned_tool_result()`'s text stays byte-identical** (`loop/tools.rs`, `abandoned_tool_result`). Five places pin
  it verbatim — two assertions inside `compaction_overflow.rs`'s abandoned-attempt tests, `tool_row_settle.rs`'s
  `const ABANDONED` (compared against `close_text(...)` in each of that file's five non-vacuous close assertions), and
  `turn_lifecycle.rs`'s own `const ABANDONED` (asserted inside
  `a_truncated_turns_tool_calls_are_closed_as_abandoned_and_never_dispatched`, added with site 9) — and the
  no-double-close pin asserts the ABSENCE of that string on a batch that ran for
  real (inside `a_settle_after_a_completed_batch_does_not_re_close_its_rows`), which is
  how a re-close would announce itself. The wording is the point: the tool did not FAIL, it was discarded — which on the
  truncation route is also the difference between an honest card and the executor's misleading
  ``missing `path` or `content` parameter`` refusal.

## 4. `message_retracted` does not close rows — **accepted, two reasons**

**Reason one: both emit sites already close first, and the ordering is pinned.** `handle_mid_stream_error`'s
`ContextOverflow` arm calls `close_announced_tool_calls` and emits `message_retracted` on the next statement (guarded by
the "something was streamed" test); `retry_abandoned_attempt`'s post-`select!` close is followed by its own
`message_retracted` the same way. Those two are the only `message_retracted` emissions in production
source. `a_retried_reply_closes_the_tool_calls_it_abandoned` pins the order by construction rather than by inspection:
it waits for the close first, so a retraction emitted BEFORE the close would be missing from what follows and the
count assertion fails. `a_mid_stream_overflow_closes_the_abandoned_tool_call` (`compaction_overflow.rs`,
`a_mid_stream_overflow_closes_the_abandoned_tool_call`) pins
the same order on the second path.

**Reason two: `normalize`'s retraction arm could not do it correctly at all.** That arm (`normalize`'s `RpcEvent::message_retracted` arm) knows
one thing — `st.current_message_id`, the id the retried call REUSES, which is why the retraction is a protocol event.
It has no tool-call id, and `announced_tool_calls` is a flat set with no per-message attribution, so the best such an
arm could do is close EVERY announced id of the turn, including the rows belonging to the same turn's other messages —
a batch that ran and closed legitimately, or an earlier attempt's cards. The display side has the same split: the
retraction arm there (`compute_display_rows`' `Some("message_retracted")` arm) overwrites rows keyed by `messageId`,
while tool-call rows are keyed by `toolCallId` (its `Some("tool_call")` and `Some("tool_call_update")` arms) — two key spaces, no join. Redundant at both sites, unimplementable in the one place someone
would think to write it.

## 5. The dropped-frame residual — **recorded, and not fixable in this crate**

The pump's `Lagged` arm logs and continues (`pump.rs:56-73`): the ring is `tokio::sync::broadcast::channel(256)`
(`worker/client.rs:402`), the `send` is synchronous with no backpressure and the ring simply rotates
(`:463-469`), so a consumer that stalls loses whatever was overwritten and the loop carries on. `client.rs`'s own doc
on `events()` calls that out. This is deliberate and it is tested:
`lagged_frames_log_continue_and_preserve_partial_salvage` (`worker/manager/tests/mod.rs:2714`) plants a real lag over a
capacity-64 copy of production's 256 and pins that the dropped frame is dropped — its liveness count is 66, not 67.

**An engine that emits the close more completely cannot deliver it more reliably.** A close dropped off that ring is
unrecoverable from here: there is no acknowledgement, no replay, and no second copy. Fixing it means changing what the
transport promises, which is a different question than the one issue #11 asked. Recorded, not fixed.

Its consequence is worth the reading, because it is not "a spinner spins". An orphaned announce leaves
`SubagentLiveness.in_flight` non-empty — `note_liveness_event` inserts on `tool_execution_start`
(`subagent_flow.rs:415-417`) and removes on `tool_execution_end` (`:418-419`), so the dropped `end` is the whole story.
The drive's silence clock then reads that set as a guard on its own `select!` arm:

    _ = &mut sleep, if !in_flight => break Race::Dead,     // subagent_flow.rs:1067

`in_flight` is recomputed each pass at `:1004-1021`, and a `select!` guard does not lengthen an arm — it **disables**
it, exactly as the comment at `:1063-1066` says ("the bound is not 'longer'; it does not run"). So a drive holding a
stale id can end only on settle (`:1037`), on the pump's exit (`:1052`), on the process exit after its one-second grace
(`:1057`, `exited_with_pump_grace`, `PUMP_EXIT_GRACE` at `:35`), or on cancel (`:1058`); the activity arm at `:1062`
only re-arms the clock that is already off. What that costs is bounded by what those four arms depend on — the child's
own turn token, its exit, or a parent abort — not by the 30-minute **Dead bound** the disabled arm would have enforced
(`DEFAULT_SUBAGENT_DEAD_BOUND`, `worker/manager/mod.rs:70-71`; `subagent_dead_bound_secs` defaults to 1800,
`config_types.rs:258`). The skew is one-directional by design: it can only keep a child alive longer, never cut a
working one short. A dropped close converts "quiet for 30 minutes" from a reap into "waiting on the child", which is
the honest way to put it.

## 6. The desktop-side mitigation that already shipped, and what this fix changes for it — **recorded**

`archimedes-codes/archimedes` `744aac23` (PR #167) rewrote how the Delegating card picks the row to report in
`src/lib/subagentActivity.ts`: `getActivityDisplay` scans back for the NEWEST SETTLED row (`:302-309`) and takes the
earliest OPEN row after it (`:310-317`), where the old code took the first open row in the transcript. The rule is
positional and the protocol is what makes it sound rather than heuristic: a batch's announces all arrive during
streaming and `run_tool_batch` executes them one at a time afterwards, so nothing is produced WHILE a tool runs — a
settled row after an open one is therefore evidence the open row was abandoned. (Those line numbers are from
`git show 744aac23:src/lib/subagentActivity.ts`; the file in a working tree that predates that commit does not contain
them.)

That commit names three producers of orphans, all in this crate. **This fix removes two.** `settle_error` /
`settle_cancelled` emitting only `turn_end` + `agent_settled` is §1 and §2 above. `message_retracted` dropping the
agent text but never the tool row is covered too, and by §4's reason one rather than by any change to the retraction:
both retraction sites close first, so a retracted attempt's rows are already settled.

The count is still three, not four: the settle this note later gained (§2 site 9) never became a producer, because
`settle_truncated` was written with the close in it — and after §10 it gets that close from `settle_error`, so the
statement holds for a second reason.

**It does not remove the third.** A close dropped by `Lagged` (§5) is still unfixable downstream, and position remains
the only signal the transcript carries in that case — the desktop's own commit records the matching residual: an orphan
followed only by open rows is byte-identical to a legitimate mid-batch transcript, so it still wins there, and `at` does
not separate them because a batch's announces share a stamp. The engine-side cure removes the ambiguity at its source
for the two producers it owns; it does not make the reader's discriminator unnecessary.

And the cure does not reach a user yet — **but not for the reason this paragraph used to give.** It cited the desktop's
pin (`archimedes/src-tauri/Cargo.toml`, then `78e595436…`, a rev whose `turn.rs` has no `close_announced_tool_calls`),
and then, when that moved, a per-commit rev on the desktop's `main`. Both readings were the wrong axis and the second
was unfalsifiable: this repo family squash-merges, so a pre-squash hash goes unreachable the moment its branch is
cleaned up, and a note that cites one cannot be re-checked by the next reader. Cite tags and PRs.

**The cure ships in a desktop release.** Desktop `v0.13.0` (`e0f3811e`, 2026-11-11 — the newest tag as of this note)
carries `fulcrum-harness = "0.2.0"` at `src-tauri/Cargo.toml:121`, and fulcrum's `v0.2.0` contains this fix:
`git merge-base --is-ancestor 33e3ea7 v0.2.0` is true in THIS repo (that commit is
`fix: close announced tool rows on every settle, not just some (#11)` — PR #11, the commit that introduced
`close_announced_tool_calls`). So the sentence this paragraph used to end with — "no desktop tag contains the cure, so
no installed build runs it" — is FALSE, and the residual it was drawing is smaller than it claimed: the reader's
discriminator is now a belt, not the only brace. What remains of the concern is the version, not the presence: a desktop
older than `v0.13.0` still runs an engine without the close, and `v0.11.0` and earlier predate the extraction entirely
(their engine was in-tree at `src-tauri/src/agent/harness/loop/turn.rs`, where
`git show v0.11.0:…/turn.rs` has no `close_announced_tool_calls`). **Recorded, no open work on the engine side.**

## 7. What a truncated turn does with its text, and why it is not this note's orphan — **accepted**

Issue #190's `Length` route persists an assistant row where every other non-success route persists nothing, so it is
worth stating here why that does not create the orphan §2 exists to prevent. `handle_truncated_turn` pushes exactly one
`ChatMessage { role: ChatRole::Assistant }` with `tool_calls: None` (inside its `if !acc.text.is_empty()` block, with the
comment that names why `acc.tool_calls` is NEVER replayed) and persists it with the
`self.persist_transcript_message()` that closes that block;
`settle_truncated` then closes the announced rows as FRAMES only. The provider therefore sees a reply with no tool
calls and no tool result — a consistent transcript — which is the pairing discipline §3's second bullet guards, held by
construction rather than by luck.

Persisting is deliberate: the streamed text is real content the model produced, and a display that shows it while the
transcript does not is the display/transcript split
the DESKTOP repo's `docs/roadmap/compaction-recovery-hardening-follow-ups.md` §4 ("A crashed Worker leaves a
reply on screen that the model never had") documents for crashes. And the counter-case is the #64
row: when NOTHING streamed, `handle_truncated_turn` pushes nothing and persists nothing (that same
`!acc.text.is_empty()` guard, which is the block's only condition), so the 33-byte `{"role":"assistant","content":"","tool_calls":[]}` row is gone on the
`Length` route too (`a_truncated_turn_with_no_text_persists_no_assistant_row`, `turn_lifecycle.rs`).

## 8. Why the `Length` route reached a success triple at all — **recorded, the mechanism**

`FinishReason::Length` was mapped by all three wires (`map_anthropic_stop_reason`'s `"max_tokens"`,
`map_finish_reason`'s `"length"`, the Responses API's `incomplete_details.reason == "max_output_tokens"`) and read by
nobody in the turn loop: `grep -rn "FinishReason::Length" src/harness/loop.rs src/harness/loop/` found nothing outside
tests at `7f21099` (checked: zero hits in both files), and `acc.finish` had exactly two production readers — `turn.rs`
on `Some(Error)` (`consume_stream`'s fabrication guard) and `loop.rs` on `Some(Stop)` (the #64 gate).
So a `Done(Length)` stream failed the emptiness gate's `Some(Stop)` conjunct and fell through to the success triple at
the end of `handle_turn` (the `agent_end`, `turn_end`, `agent_settled` emissions behind the `tool_calls.is_empty()` test
on the taken vec). The user watched a confident half-sentence settle as a
completed turn, the composer unlocked, nothing ran, and no error surfaced. The only place in the engine that understood
truncation was `compact.rs` — the summariser.

The gate now sits ahead of that one (`handle_turn`'s `acc.finish == Some(FinishReason::Length)` arm, after the
`consume_stream` call and deliberately BEFORE the emptiness gate and before `note_success`), and the emptiness gate is keyed on "nothing was produced"
rather than on the reason (the `acc.text.is_empty() && acc.tool_calls.is_empty()` test), which also catches `finish == None` and a provider that answers
`Done(ToolCalls)` while dispatching no call. `a_provider_that_claims_tool_calls_and_dispatches_none_is_retried`
(`turn_lifecycle.rs`, `a_provider_that_claims_tool_calls_and_dispatches_none_is_retried`) is that pin and
`a_claimed_tool_calls_turn_with_a_spent_budget_announces_the_retry_and_persists_nothing` (its neighbour in the same file) its
budget-spent half — the pair proves the catch routes to the RETRY (an `auto_retry_start` is announced) and that neither
arm leaves a 33-byte row behind. `acc.finish`'s production readers are now `Some(Error)` (unchanged) and
`Some(Length)`; the `Some(Stop)` reader is deliberately GONE, and that removal is what the generalisation is — the
reason is no longer what the gate keys on.

## 9. Rows orphaned before the fix stay orphaned — **recorded**

The fix is forward-only, and it is worth naming exactly what is on disk. Each frame persists a display row —
`normalize`'s announce persists a `tool-call` row (`compute_display_rows`' `Some("tool_call")` arm) and each update re-persists it (its `Some("tool_call_update")` arm) —
and in production that write travels: `IpcStore::persist_display` turns it into a `DisplayUpsert` frame
(`worker/store.rs:103-109`), the daemon's `TranscriptPersister` upserts it into `messages` keyed
`(session_id, kind, message_key)` (`src/persist.rs:172-184`). So an `in_progress` payload written by a pre-fix attempt is
sitting in the database, and the upsert key is the reason it is still there: a later close for the same `toolCallId`
would overwrite it, and the whole defect is that no later close ever arrives.

Nothing in this workspace repairs one on the way back out. The engine's own `Store` seam has no display READ at all:
`persist_display` (`harness/store.rs:86`) is its only display method, and its five other methods speak the provider
transcript (`insert_message`, `load_messages`, `clear_messages`, `replace_messages` at `:62-83`) or the context-usage
write (`:90`). The read half lives on the OTHER seam, `FulcrumPersistence::messages_for` (`seams.rs:192-193`,
implemented at `fulcrum/src/db/persistence.rs:128-129` over `Db::messages_for`, `fulcrum/src/db/mod.rs:880-895`), and
the only things that call it are that impl, the `#[allow(dead_code)]` seam-completeness function
(`fulcrum/src/db/persistence.rs:207`, whose job is to fail to BUILD if the seam drifts), and a test comment — no
production reader in either member. Hydration is therefore the desktop's, and its only repair is
`finalizeSessionMessages` (`src/lib/transcript/reducer.ts:228-235`), which flips still-pending rows to `failed` on
session close (`src/store/sessions.ts:2392`) rather than on load. So a session orphaned before this fix and never closed
keeps its spinner; a session closed after the fix gets the repair the desktop already had. Rewriting those rows at
hydration is a desktop decision and belongs in its notes, not here.

## 10. Why site 9's settle is a delegation, and the watermark claim it reverses — **recorded, the correction**

`settle_truncated` (`turn.rs`) used to be a fourth body of its own: `settle_error`'s three statements minus
`compactor.turn_abandoned`, with a docblock arguing the omission was the whole difference. Two claims carried that
argument — that calling `turn_abandoned` here would "discount a real message and reset the count to the bare anchor",
and that on the no-text half it "would be a no-op anyway". **Both are false**, and `compact.rs`'s own arithmetic is what
says so: `record_usage` writes `usage_watermark = messages.len() + 1` — the `+1` anticipates the reply the
caller is about to append — and `turn_abandoned` early-returns on `usage_watermark <= messages.len()`. So:

* **Text streamed.** `handle_truncated_turn` pushes the reply BEFORE the settle (its `if !acc.text.is_empty()` block,
  which is the only thing that pushes, and the settle is at the end of the function), so the transcript
  reached the watermark, `watermark == len`, the guard fires, and the call is a **no-op on every field** — Δ = 0 on
  `context_tokens`, `usage_base`, `usage_watermark` and `pending_output`. The predicted harm is unreachable. That guard
  exists precisely to make the call safe unconditionally, and `turn_abandoned`'s own docblock says so in those words
  ("safe to call on EVERY turn exit"), and the paragraph after it names the guard that buys it;
  `settle_truncated` inverted that reading.
* **Nothing streamed.** Nothing is pushed, `watermark == len + 1 > len`, the guard does NOT fire, and the call **acts**
  — which is correct, and is the half the old text dismissed as free.

Measured cost of leaving it out (`Usage { input_tokens: 100, output_tokens: 50 }` then `Done(Length)`, no `TextDelta`):
`context_tokens` stays **150** instead of **100**, `pending_output` stays 50, and `usage_watermark` stays 2 with **0**
assistant rows in the transcript — the loop believes in a reply that was never persisted. Driven end-to-end at a window
whose threshold falls between the honest 100 and the inflated 150, the NEXT turn emitted **1 `compaction_start` and 3
model calls instead of 0 and 2**: a spurious compaction, an extra summary call, the transcript rewritten for context
pressure that did not exist. The inflation also self-perpetuates — the watermark window swallows the next prompt too,
because `refresh` estimates only `messages[watermark..]`.

The fix is therefore the call, and the body is then `settle_error`'s verbatim, so it IS `settle_error` — one delegation
(`settle_truncated`'s body is a single `self.settle_error(tool_results, announced)`), not two copies of the same settle that had quietly diverged. `settle_truncated` survives as the named
entry point for two reasons that are not implementation details: §2's table names it at site 9 and the `announced`
column's derivation is written against that name, and `handle_truncated_turn` calls it, so the truncation route keeps a
name a reader can grep for. Decision 2 / ADR 0108's no-dispatch discipline is untouched — that half of the docblock was
verified true and is the point of the function.

Three pins, all in `turn_lifecycle.rs`:
`a_truncation_that_streamed_nothing_does_not_leave_phantom_output_tokens` (the counter, and the 0-assistant-row half of
the same lie), `a_truncation_that_streamed_nothing_does_not_compact_the_next_turn` (the spurious compaction — the pin
that would have caught this), and `a_truncation_that_streamed_text_leaves_the_context_count_where_usage_put_it`, which
pins the Δ = 0 fact as an invariant so nobody "fixes" the no-text case back the other way.

## 11. The `RetryPolicy::reported` leak — every settle that never reaches `note_success` — **open work, one route fixed**

**Status: open work.** Issue #190's follow-up closed ONE route into this class (the `Length` settle) and deliberately
left the rest alone, because the other routes have tests pinning their current event streams and this PR does not own
them. This section is the whole class, written so the next reader does not have to re-derive it.

### The mechanism, in one paragraph

`RetryPolicy` (`src/harness/retry.rs`) carries one bit, `reported`. It is SET in exactly two places —
`note_success()` and the new `retire_pending_report()` — and CLEARED in exactly ONE:
`next_retry_delay()`. `note_success()` returns `Some(attempt)` when `attempt > 1 && !reported`, and the loop
turns that into `auto_retry_end { success: true }` (`handle_turn`, the `if let Some(attempt) = self.retry.note_success()`
block, after the emptiness gate), which `normalize` renders as the
one-liner **"Retry succeeded"** on its own `{stamp}:notice:N` id (`normalize.rs`, the `auto_retry_end` arm). So the
bit means "this retry's success frame has been emitted", and it is the ONLY thing standing between a
retried-but-never-settled attempt and a success
banner. Every path that abandons an attempt and settles WITHOUT reaching `note_success()` therefore leaves a report
pending, and the next ordinary successful prompt — a fresh turn, an unrelated prompt, minutes later — walks into
`note_success()`, gets `Some(attempt)` for the PREVIOUS turn's retry, and announces that retry a success.

Two properties make this worse than a cosmetic banner. `attempt` is per-turn-but-never-reset-to-zero (both
`call_with_retry` and `note_success` do `attempt.max(1)`, and only a fresh `next_retry_delay` moves it up), so the
stale `attempt` number rides the frame; and the success line is a one-liner on the `{stamp}:notice:N` family
(`normalize.rs`, `TurnState::notice_id`) — a UNIQUE id per line, so it no longer APPENDS onto whatever system line the
session last wrote — while still carrying no turn reference of its own, so it lands wherever the frame arrives rather
than under the turn it describes.

### Which settle sites leave it pending

Nine of §2's TEN sites — every one but site 9 — settle without falling through to `note_success()`, so all nine leave
a report pending (site 9 is the tenth and the exception: `handle_truncated_turn`'s `retire_pending_report()` retires the
report explicitly, so it is the one site that
already does what §6's open work asks). The four marked *measured* were
driven end-to-end (two prompts, one `AgentLoop`, scripted provider; the assertion is the count of
`auto_retry_end { success: true }` frames on the SECOND turn — every one of them was `1`, i.e. the leak fires):

**The first column is §2's site NUMBER, not a line.** §2's table is the one place a site has a stable identifier, and
the number is what the source comments say ("Site 7 of the ten"); a `file:line` here would have rotted exactly as the
ten numbers this column used to carry did.

| Site | Function | Settle | Evidence |
| --- | --- | --- | --- |
| 1 | `compact_if_due` (overflow `Skipped`) | `settle_error` | mechanism (same predicate; needs a retry earlier in the turn) |
| 2 | `handle_mid_stream_error` (non-retryable mid-stream) | `settle_error` | *measured* — Retryable attempt, then mid-stream `Auth`: turn 1 emits `auto_retry_start` and NO close, turn 2 emits `success: true` |
| 3 | `retry_abandoned_attempt`, backoff CANCELLED arm | `settle_cancelled` | *measured* — turn 1 `auto_retry_start` → `turn_end`/`agent_settled`, turn 2 `success: true` |
| 4 | `retry_abandoned_attempt`, budget EXHAUSTED arm | `settle_error` | *measured* — turn 1 emits `auto_retry_start` + `auto_retry_end { success: false }` and STILL leaks: the close emits the frame without consuming `reported` |
| 5 | `handle_turn`, top-of-loop cancel | `settle_cancelled` | mechanism |
| 6 | `handle_turn`, model call races the cancel | `settle_cancelled` | mechanism |
| 7 | `handle_turn`, the model call returned `Err` | `settle_error` | *measured* — a pre-stream `Retryable` then a pre-stream `Auth`: turn 1 `auto_retry_start` + settle, turn 2 `success: true` |
| 8 | `handle_turn`, Stop mid-stream | `settle_cancelled` | mechanism |
| 10 | `handle_turn`, the turn-bound guard | `settle_error` | mechanism, NOT measured — reachable with a report pending only by the route the guard's own unit allows: `next_retry_delay()` sets `attempt > 1` and clears `reported`, and `retry_abandoned_attempt`'s backoff-COMPLETED arm `Continue`s to the loop top without ever reaching `note_success()` — which is the only thing on the accepted path that consumes the report. The bound then ends the turn at the loop top, ahead of any later `note_success()`. Every other way back to the loop top passes `note_success()` and closes the report, so this is a narrow route and no test drives it. |

The budget-exhausted row is the sharpest of the four, because that arm already emits
`auto_retry_end { success: false }` — the statement immediately after
`retry_abandoned_attempt`'s backoff `select!`, ahead of the site-4 `settle_error` — so the user sees "Retry failed" for the turn and, on the next
prompt, "Retry succeeded" for the SAME retry. The two frames disagree because the close was emitted without retiring
the report.

### The user-visible symptom

A turn that retried and then failed (or was stopped) prints no verdict of its own — or a "Retry failed" line — and the
session looks settled. The user types the next thing they wanted, that prompt succeeds normally, and the transcript
prints **"Retry succeeded"** under the NEW turn: a claim that the failed/stopped attempt recovered, attached to a turn
that never retried anything. Nothing corrects it afterwards; there is no counter-frame.

### What #190's follow-up did, and the shape it chose

`handle_truncated_turn` now calls `RetryPolicy::retire_pending_report()` and emits
`auto_retry_end { success: false }` with a `final_error` naming the truncation, before the settle
(the block between its `persist_transcript_message()` and its `settle_truncated` call). The shape is
`retry_abandoned_attempt`'s budget-exhausted arm's own (the `auto_retry_end { success: false }` that precedes its
`settle_error`), for its
stated reason: a peer that opened a retry card on the `auto_retry_start` must see it CLOSE. The alternative — flipping
`reported` with no frame — was rejected as the same dangling-card class §1 refuses elsewhere. The unit pin is
`harness::retry::tests::retire_pending_report_consumes_the_report_without_reporting_success`; the end-to-end pin is
`harness::r#loop::tests::turn_lifecycle::a_truncated_turn_does_not_leave_a_retry_report_for_the_next_prompt_to_call_success`.

`retire_pending_report` is the SAME predicate as `note_success` minus the `attempt.max(1)` raise, and that omission is
deliberate: a settle that never made a call must not manufacture an attempt number to report.

### The open work, stated for whoever takes it

Closing the class is a per-site decision, not a one-liner in `settle_error`/`settle_cancelled`: those two functions are
reached by sites that have NO `error_message` in scope (the overflow-`Skipped` settle, the three cancel settles), so a
close emitted there needs a reason string invented at the settle — which is exactly what §3's discipline about honest
close text warns against. The candidate shapes are (a) pass an `Option<&str>` reason into the two settle functions and
close-when-pending inside them, or (b) close in each CALLER, where the reason is already in scope, as
`handle_truncated_turn` does. (b) keeps the settle functions as pure event triples and is what the #190 route chose.
Either way the tests to re-check first are the ones that pin these paths' event streams today:
`tool_row_settle.rs` (all ten sites), `turn_lifecycle::a_cancel_during_the_retry_backoff_settles_fast`,
`turn_lifecycle::an_empty_turn_shares_the_retry_budget_with_the_ladder`,
`turn_lifecycle::an_empty_stop_with_an_exhausted_budget_settles_as_an_error`, and
`context_usage::a_failed_turn_still_measures_the_next_prompt` /
`context_usage::a_cancelled_turn_still_measures_the_next_prompt`.

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

Every line number below was read in the tree this note lands in, with the fix already committed (`a245180`, tests in
`2334d83`). The plan's references were written against `45e576d` — the commit it verified its problem statement against —
and they have all drifted; nothing here is copied from it. Line numbers rot, so each citation names the function it sits
in beside the number. Where a claim came from the desktop repo it says so and names the commit. The #190 pass RE-READ
every engine-side number in §1–§4 against its own tree, because its own edits shifted `normalize.rs`, `turn.rs` and
`loop.rs`, and it re-verified §6's desktop claims from the desktop repo; the daemon-side citations in §5 and §9 were not
re-read and may have drifted again.

Statuses: **open work** = someone must do it · **recorded** = a known limit, deliberately not fixed ·
**accepted** = decided, will not change here.

---

## 1. The invariant, and the one place the close lives

**A turn never settles with an announced tool row left open.**

The frames that carry it, in order. A `tool_call` ANNOUNCE frame opens a row at `status: "in_progress"` when the model
finishes NAMING a call: `consume_stream`'s `ProviderEvent::ToolCall` arm (`turn.rs:233-248`, `consume_stream`) pushes
the call onto `acc.tool_calls` (`:235`), emits `toolcall_end` (`:236-242`) and emits `tool_execution_start`
(`:243-247`) — and `normalize` renders EITHER one into a `tool_call` frame at `in_progress` (the `toolcall_end` arm,
`normalize.rs:344-385`, and the `tool_execution_start` arm, `:399-430`). The row leaves `in_progress` on exactly one
frame: the `tool_call_update` carrying `completed`/`failed` that `normalize` derives from `tool_execution_end` alone
(`normalize.rs:445-455`). `normalize` has no arm for `turn_end` or `agent_settled` — grep over that file finds neither
variant named anywhere in its `match`, only in the module docs that list the no-frame events (`:213-218`) — so both fall
into the catch-all `_ => Vec::new()` (`normalize.rs:630`, the arm of `normalize`'s own `match`), which is also the shape
the display-row scanner carries (`:848`). A settle therefore says nothing about the rows the attempt opened, and the row
spins forever: in the desktop, a Subagent frozen on a stale tool name with a climbing duration while its tool count and
cost keep rising.

The close now lives in ONE function — `close_announced_tool_calls` (`turn.rs:764-775`, `close_announced_tool_calls`),
which emits `tool_execution_end { tool_call_id, tool_name, result: abandoned_tool_result(), is_error: true }` per call
— and it is called from five places: the THREE settle functions, each BEFORE its own `turn_end`
(`turn.rs:787` `settle_cancelled`, `:800` `settle_error`, `:843` `settle_truncated`), and the two abandon paths
that `Continue` instead of settling (`:327` the mid-stream overflow recovery, `:522` the retry whose backoff
completed). The ordering is the whole content of the invariant: `normalize` closes a row on `tool_execution_end` and on
nothing that follows it, so a close emitted after `turn_end` would be a close that never lands.

The helper stays a private `fn` (`:764`, not `pub(super)`) and that visibility is the invariant: a child module's
private method is invisible to its parent, so `loop.rs` cannot reach a close at all — it must go through a
`pub(super)` settle, and it therefore cannot open a close without settling. Adding a fourth settle costs nothing in
reachability, which is exactly what kept site 9 from becoming a second close implementation.

The bug's clearest statement was an asymmetry inside one function. `retry_abandoned_attempt`'s cancel arm called
`settle_cancelled` and returned, skipping the close loop a few statements below: the path where the backoff COMPLETED
closed this same attempt's rows and the path where it was CANCELLED did not. Same function, same abandoned attempt, same
`acc`. `both_paths_of_retry_abandoned_attempt_close_the_announced_tool_calls_identically` is the pin, written as a PAIR
over one shared assertion so an edit that reintroduces the asymmetry reddens whichever arm it broke, by name.

## 2. The nine settle sites, which five are non-vacuous, and the derivation of the other four

`settle_cancelled` / `settle_error` / `settle_truncated` are the three settles, and grep finds exactly nine call sites —
five in `turn.rs`, four in `loop.rs`. Five pass the attempt's real `acc.tool_calls`; four pass `&[]`, and each of
those carries its derivation at the call site so the emptiness stays a fact rather than becoming a hope a new call
site can invalidate.

| # | Site | Announced rows possible? | Why |
| - | ---- | ------------------------ | --- |
| 1 | `turn.rs:142` `compact_if_due`, overflow `Skipped` | no | no accumulator in scope |
| 2 | `turn.rs:341` `handle_mid_stream_error` fall-through | **yes** | non-retryable mid-stream error |
| 3 | `turn.rs:514` `retry_abandoned_attempt`, cancel arm | **yes** | cancelled inside the backoff |
| 4 | `turn.rs:537` `retry_abandoned_attempt`, budget exhausted | **yes** | the attempt is abandoned for good |
| 5 | `loop.rs:1283` top-of-loop cancel | no | no accumulator in scope |
| 6 | `loop.rs:1322` model call races the cancel | no | the stream was never opened |
| 7 | `loop.rs:1343` model call returned `Err` | no | no stream to consume |
| 8 | `loop.rs:1366` cancel after `consume_stream` | **yes** | Stop pressed mid-stream — the field report |
| 9 | `turn.rs:469` `handle_truncated_turn` → `settle_truncated` | **yes** | output-cap truncation — issue #190 |

Site 8 is the exit the report is about, and it was NOT in the issue's list of four; site 2 is the same shape reached by
error rather than by Stop.

**Site 9 is non-vacuous for a different reason than 2, 3, 4 and 8, and the difference matters to the derivation.** The
other four pass `&acc.tool_calls` because their attempt was ABANDONED — it died, or it was cancelled, or its budget ran
out, and the rows it opened would otherwise never be closed. Site 9's attempt did none of those: `consume_stream` ran to
completion (`loop.rs:1357`) and announced each call normally, and the calls are never dispatched because Decision 2
(ADR 0108) FORBIDS dispatching a call whose `arguments` the output cap cut off mid-object — dispatching it would produce
the executor's misleading ``missing `path` or `content` parameter`` refusal rather than an honest one. So the close is
not a leak-repair here, it is the ONLY legitimate ending those rows can get, and `announced` cannot be `&[]`:
`handle_truncated_turn` reaches no batch at all (it `return`s from `handle_turn` at `loop.rs:1405`, ahead of
`run_tool_batch` at `:1490`), so nothing else emits a `tool_execution_end` for them. Pinned by
`a_truncated_turns_tool_calls_are_closed_as_abandoned_and_never_dispatched` (`turn_lifecycle.rs:1945-2052`), which
carries a counting `SubagentDispatcher` and asserts the counter is ZERO — the close is proven, and the non-dispatch is
proven, in the same run.

The vacuity of 1, 5, 6 and 7 is structural, and stronger than the argument the plan carried. `acc` is declared at
`loop.rs:1349` — after the compaction check (`:1288-1293`), after the model-call `select!` (`:1300-1325`) and after the
`Err` arm (`:1335-1343`) — so at those four sites there is no accumulator to pass, and `consume_stream`, the ONLY place a
`tool_call` row is opened, has not run. A previous iteration cannot have left a row open either, for two independent
reasons, both checked in the source rather than assumed:

* **`run_tool_batch`'s dispatch loop has no early exit.** The loop runs `turn.rs:609`–`:712` and scanning that range
  finds no `break`, no `return`, no `continue`, no `?`: every iteration reaches the `tool_execution_end` emit at
  `:699-704`, so the batch closes every call it was handed. That includes a cancelled turn — a cancelled call still
  gets a result, `cancelled_tool_result()` at `:636` (a `subagent` whose child never ran) and `:647-649` (the
  sequential tools) — and a result reaches the same unconditional emit. The rule "cancellation beats rejection" is what
  keeps this true: the cancel arms precede the rejection arm, so a cancelled call produces `cancelled` rather than
  producing nothing.
* **`acc.tool_calls` is emptied before the batch runs, and `acc` is re-declared every iteration.**
  `std::mem::take(&mut acc.tool_calls)` at `loop.rs:1465`; the batch runs on the taken vec at `:1490`. Nothing between
  the take and the loop's end (`:1493`) refills the accumulator — `acc.tool_calls` is pushed to in exactly one place,
  `turn.rs:235` — and the only exit in that range is `loop.rs:1486`, the clean-stop return, which is gated on
  `tool_calls.is_empty()` at `:1476`. The next iteration gets a fresh `TurnAccumulator::default()` at `:1349` anyway.

The paths into a later iteration are exhaustible and each is covered: the pre-stream overflow `continue`
(`loop.rs:1333`, before any stream exists), `handle_mid_stream_error`'s `Continue` (`:1377` → closes at
`turn.rs:327` or `:522`), `handle_empty_turn`'s `Continue` (`:1448` → `retry_abandoned_attempt`, `turn.rs:392-397` →
closes at `:522`; and its own batch is empty by the gate at `:1446`), the Length gate's `Continue` arm
(`:1404` — DEAD today, because `handle_truncated_turn` ends in `settle_truncated` + `TurnFlow::Return` unconditionally,
and covered even if it were live because the close has already run inside `settle_truncated` by then), and the
fall-through after the batch.

**Three more `agent_settled` emissions are not among the nine, and they are empty too.** Production emits
`agent_settled` at six places (`loop.rs:1146`, `:1238`, `:1485`, plus `turn.rs:793`, `:806` and `:848` inside the three
settle functions), so a reader auditing "every settle" by grepping that frame finds six emissions behind nine call sites
and should not have to re-derive which are which. `loop.rs:1485` is the accepted clean stop, gated on
`tool_calls.is_empty()` (`:1476`); `loop.rs:1146` settles prompts the Stop drained from the queue in `run()`'s cancel
arm, and `loop.rs:1238` settles the prompt `handle_prompt` skipped on an already-cancelled token — in both, no turn body
ran, so nothing was announced. `loop.rs:1485` is also the emission a `Length` turn NO LONGER reaches: pre-#190 a
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
the helper itself (`turn.rs:717-763`, `close_announced_tool_calls`'s docblock) and each has a test that reddens when it
is violated.

* **Iterate the ordered `Vec<ToolCall>`, never `TurnState::announced_tool_calls`.** That field is a `HashSet<String>`
  (`normalize.rs:72`) and it is insert-only: the only writes anywhere in the crate are `:291`, `:376`, `:421` and `:488`
  — the only `.remove(` calls in `normalize.rs` hit `toolcall_args` (`:355`) and the display-store accumulators (`:766`,
  `:783`). Nothing clears it per attempt or per turn either: the production `TurnState` is built once, at
  `loop.rs:797`, and lives for the whole Worker (the only other construction is the nested-call sink's own fresh state,
  `loop/emit.rs:143`). And it does not hold only tool calls — `compaction_start` mints a `compact:{n}` card id
  (`normalize.rs:486`) and inserts it into the same set (`:488`), because a compaction renders as a tool card. Closing
  from the set would therefore fake a failed tool close for every compaction card the session ever opened, in an
  arbitrary order, and `compute_display_rows`' `tool_call_update` arm `merge_json`s onto the card (`:816-846`) — so the
  forged close would overwrite the summary `compaction_end` had put there. The `Vec` is transcript order, which is what
  the desktop reads these frames as; `the_closes_arrive_in_accumulator_order_and_every_one_precedes_the_settle` declares
  its ids `zzz-`/`mmm-`/`aaa-` so declaration order is the REVERSE of sorted order and no set, hash, sort or reverse can
  produce the asserted sequence.
* **Frames only, never transcript rows.** No `ChatMessage { role: Tool }` is pushed and nothing is appended to
  `turn_tool_results` — contrast `run_tool_batch`, which pushes at `turn.rs:705-710` and persists at `:711`. An
  abandoned attempt persists NO assistant row, so a tool-result message would dangle with nothing for the provider to
  pair it against — the orphan a provider 400s on. Pinned by `an_abandoned_tool_call_appends_no_tool_message`
  (`compaction_overflow.rs:634-695`), which reloads the transcript from the store and asserts both that no `Tool` row
  exists and that the persisted assistant row's `tool_calls` is `None`. Site 9 is the one route that DOES persist an
  assistant row, and it does not weaken this: `handle_truncated_turn` pushes its row with `tool_calls: None`
  (`turn.rs:460`), so there is still nothing a tool result could pair against, and the same test's `None` assertion is
  what keeps that honest rather than incidental.
* **`abandoned_tool_result()`'s text stays byte-identical** (`loop/tools.rs:20-30`). Five places pin it verbatim —
  `compaction_overflow.rs:596-603` and `:750-757`, `tool_row_settle.rs`'s `const ABANDONED` (`:55`, asserted at `:167`,
  `:418`, `:662` and `:774`), and `turn_lifecycle.rs`'s own `const ABANDONED` (`:1757`, asserted at `:2025`, added with
  site 9) — and the no-double-close pin asserts the ABSENCE of that string on a batch that ran for
  real (`tool_row_settle.rs:1018-1022`, inside `a_settle_after_a_completed_batch_does_not_re_close_its_rows`), which is
  how a re-close would announce itself. The wording is the point: the tool did not FAIL, it was discarded — which on the
  truncation route is also the difference between an honest card and the executor's misleading
  ``missing `path` or `content` parameter`` refusal.

## 4. `message_retracted` does not close rows — **accepted, two reasons**

**Reason one: both emit sites already close first, and the ordering is pinned.** `turn.rs:327` closes and `:329`
retracts; `turn.rs:522` closes and `:524` retracts. Those two are the only `message_retracted` emissions in production
source. `a_retried_reply_closes_the_tool_calls_it_abandoned` pins the order by construction rather than by inspection:
it waits for the close first, so a retraction emitted BEFORE the close would be missing from what follows and the
count assertion fails. `a_mid_stream_overflow_closes_the_abandoned_tool_call` (`compaction_overflow.rs:769-774`) pins
the same order on the second path.

**Reason two: `normalize`'s retraction arm could not do it correctly at all.** That arm (`normalize.rs:576-591`) knows
one thing — `st.current_message_id`, the id the retried call REUSES, which is why the retraction is a protocol event.
It has no tool-call id, and `announced_tool_calls` is a flat set with no per-message attribution, so the best such an
arm could do is close EVERY announced id of the turn, including the rows belonging to the same turn's other messages —
a batch that ran and closed legitimately, or an earlier attempt's cards. The display side has the same split: the
retraction arm there (`:757-794`) overwrites rows keyed by `messageId`, while tool-call rows are keyed by `toolCallId`
(`:795-815`, `:816-846`) — two key spaces, no join. Redundant at both sites, unimplementable in the one place someone
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
`settle_truncated` was written with the close inside it.

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
`ChatMessage { role: Assistant }` with `tool_calls: None` (`turn.rs:452-461`) and persists it (`:462`);
`settle_truncated` then closes the announced rows as FRAMES only. The provider therefore sees a reply with no tool
calls and no tool result — a consistent transcript — which is the pairing discipline §3's second bullet guards, held by
construction rather than by luck.

Persisting is deliberate: the streamed text is real content the model produced, and a display that shows it while the
transcript does not is the display/transcript split
the DESKTOP repo's `docs/roadmap/compaction-recovery-hardening-follow-ups.md` §4 ("A crashed Worker leaves a
reply on screen that the model never had") documents for crashes. And the counter-case is the #64
row: when NOTHING streamed, `handle_truncated_turn` pushes nothing and persists nothing (the `!acc.text.is_empty()`
guard at `turn.rs:447`), so the 33-byte `{"role":"assistant","content":"","tool_calls":[]}` row is gone on the
`Length` route too (`a_truncated_turn_with_no_text_persists_no_assistant_row`, `turn_lifecycle.rs:2059-2110`).

## 8. Why the `Length` route reached a success triple at all — **recorded, the mechanism**

`FinishReason::Length` was mapped by all three wires (`map_anthropic_stop_reason`'s `"max_tokens"`,
`map_finish_reason`'s `"length"`, the Responses API's `incomplete_details.reason == "max_output_tokens"`) and read by
nobody in the turn loop: `grep -rn "FinishReason::Length" src/harness/loop.rs src/harness/loop/` found nothing outside
tests at `7f21099` (checked: zero hits in both files), and `acc.finish` had exactly two production readers — `turn.rs`
on `Some(Error)` (`consume_stream`'s fabrication guard) and `loop.rs` on `Some(Stop)` (the #64 gate).
So a `Done(Length)` stream failed the emptiness gate's `Some(Stop)` conjunct and fell through to the success triple at
`loop.rs:1477-1486`: `agent_end`, `turn_end`, `agent_settled`. The user watched a confident half-sentence settle as a
completed turn, the composer unlocked, nothing ran, and no error surfaced. The only place in the engine that understood
truncation was `compact.rs` — the summariser.

The gate now sits ahead of that one (`loop.rs:1399-1407`), and the emptiness gate is keyed on "nothing was produced"
rather than on the reason (`:1446`), which also catches `finish == None` and a provider that answers
`Done(ToolCalls)` while dispatching no call. `a_provider_that_claims_tool_calls_and_dispatches_none_is_retried`
(`turn_lifecycle.rs:2117-2180`) is that pin and
`a_claimed_tool_calls_turn_with_a_spent_budget_announces_the_retry_and_persists_nothing` (`:2188-2253`) its
budget-spent half — the pair proves the catch routes to the RETRY (an `auto_retry_start` is announced) and that neither
arm leaves a 33-byte row behind. `acc.finish`'s production readers are now `Some(Error)` (unchanged) and
`Some(Length)`; the `Some(Stop)` reader is deliberately GONE, and that removal is what the generalisation is — the
reason is no longer what the gate keys on.

## 9. Rows orphaned before the fix stay orphaned — **recorded**

The fix is forward-only, and it is worth naming exactly what is on disk. Each frame persists a display row —
`normalize`'s announce persists a `tool-call` row (`normalize.rs:795-815`) and each update re-persists it (`:816-846`) —
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

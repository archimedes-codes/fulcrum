# Abandoned tool rows — durable notes

Landed on `feature/11-settle-announced-tool-calls` against
[issue #11](https://github.com/archimedes-codes/fulcrum/issues/11), in two commits: the fix
(`close_announced_tool_calls`, `src/harness/loop/turn.rs`, reached from both settle functions) and the module that pins
each exit (`src/harness/loop/tests/tool_row_settle.rs`, 7 tests). This file is the permanent residue — the invariant,
the derivation behind the exits that close nothing, the three constraints that break if the helper is tidied, and the
residual this crate cannot reach. The plan that drove it (`docs/roadmap/settle-announced-tool-rows.md`) goes at ship
like every plan in this family; `git log` has it.

Every line number below was read in the tree this note lands in, with the fix already committed (`a245180`, tests in
`2334d83`). The plan's references were written against `45e576d` — the commit it verified its problem statement against —
and they have all drifted; nothing here is copied from it. Line numbers rot, so each citation names the function it sits
in beside the number. Where a claim came from the desktop repo it says so and names the commit.

Statuses: **open work** = someone must do it · **recorded** = a known limit, deliberately not fixed ·
**accepted** = decided, will not change here.

---

## 1. The invariant, and the one place the close lives

**A turn never settles with an announced tool row left open.**

The frames that carry it, in order. A `tool_call` ANNOUNCE frame opens a row at `status: "in_progress"` when the model
finishes NAMING a call: `consume_stream`'s `ProviderEvent::ToolCall` arm (`turn.rs:233-248`) pushes the call onto
`acc.tool_calls` (`:235`), emits `toolcall_end` (`:236-242`) and emits `tool_execution_start` (`:243-247`) — and
`normalize` renders EITHER one into a `tool_call` frame at `in_progress` (`normalize.rs:312-352`, `:367-398`). The row
leaves `in_progress` on exactly one frame: the `tool_call_update` carrying `completed`/`failed` that `normalize`
derives from `tool_execution_end` alone (`normalize.rs:413-423`). `normalize` has no arm for `turn_end` or
`agent_settled` — grep over that file finds neither variant named anywhere in its `match`, only in the module docs that
list the no-frame events (`:181-186`) — so both fall into the catch-all `_ => Vec::new()` (`normalize.rs:586`), which is
also the arm the display-row scanner carries (`:804`). A settle therefore says nothing about the rows the attempt
opened, and the row spins forever: in the desktop, a Subagent frozen on a stale tool name with a climbing duration while
its tool count and cost keep rising.

The close now lives in ONE function — `close_announced_tool_calls` (`turn.rs:677-688`), which emits
`tool_execution_end { tool_call_id, tool_name, result: abandoned_tool_result(), is_error: true }` per call — and it is
called from four places: both settle functions, BEFORE `turn_end` (`turn.rs:700`, `:713`), and the two abandon paths
that `Continue` instead of settling (`:327` the mid-stream overflow recovery, `:439` the retry whose backoff
completed). The ordering is the whole content of the invariant: `normalize` closes a row on `tool_execution_end` and on
nothing that follows it, so a close emitted after `turn_end` would be a close that never lands.

The bug's clearest statement was an asymmetry inside one function. `retry_abandoned_attempt`'s cancel arm called
`settle_cancelled` and returned, skipping the close loop a few statements below: the path where the backoff COMPLETED
closed this same attempt's rows and the path where it was CANCELLED did not. Same function, same abandoned attempt, same
`acc`. `both_paths_of_retry_abandoned_attempt_close_the_announced_tool_calls_identically` is the pin, written as a PAIR
over one shared assertion so an edit that reintroduces the asymmetry reddens whichever arm it broke, by name.

## 2. The eight settle sites, which four are non-vacuous, and the derivation of the other four

`settle_cancelled` / `settle_error` are the only two settles, and grep finds exactly eight call sites — four in
`turn.rs`, four in `loop.rs`. Four pass the abandoned attempt's real `acc.tool_calls`; four pass `&[]`, and each of
those carries its derivation at the call site so the emptiness stays a fact rather than becoming a hope a new call
site can invalidate.

| # | Site | Announced rows possible? | Why |
| - | ---- | ------------------------ | --- |
| 1 | `turn.rs:142` `compact_if_due`, overflow `Skipped` | no | no accumulator in scope |
| 2 | `turn.rs:341` `handle_mid_stream_error` fall-through | **yes** | non-retryable mid-stream error |
| 3 | `turn.rs:431` `retry_abandoned_attempt`, cancel arm | **yes** | cancelled inside the backoff |
| 4 | `turn.rs:454` `retry_abandoned_attempt`, budget exhausted | **yes** | the attempt is abandoned for good |
| 5 | `loop.rs:1254` top-of-loop cancel | no | no accumulator in scope |
| 6 | `loop.rs:1293` model call races the cancel | no | the stream was never opened |
| 7 | `loop.rs:1314` model call returned `Err` | no | no stream to consume |
| 8 | `loop.rs:1337` cancel after `consume_stream` | **yes** | Stop pressed mid-stream — the field report |

Site 8 is the exit the report is about, and it was NOT in the issue's list of four; site 2 is the same shape reached by
error rather than by Stop.

The vacuity of 1, 5, 6 and 7 is structural, and stronger than the argument the plan carried. `acc` is declared at
`loop.rs:1320` — after the compaction check (`:1259`), after the model-call `select!` (`:1271-1296`) and after the
`Err` arm (`:1306`) — so at those four sites there is no accumulator to pass, and `consume_stream`, the ONLY place a
`tool_call` row is opened, has not run. A previous iteration cannot have left a row open either, for two independent
reasons, both checked in the source rather than assumed:

* **`run_tool_batch`'s dispatch loop has no early exit.** The loop runs `turn.rs:526`–`:629` and scanning that range
  finds no `break`, no `return`, no `continue`, no `?`: every iteration reaches the `tool_execution_end` emit at
  `:616-621`, so the batch closes every call it was handed. That includes a cancelled turn — a cancelled call still
  gets a result, `cancelled_tool_result()` at `:553` (a `subagent` whose child never ran) and `:564-566` (the
  sequential tools) — and a result reaches the same unconditional emit. The rule "cancellation beats rejection" is what
  keeps this true: the cancel arms precede the rejection arm, so a cancelled call produces `cancelled` rather than
  producing nothing.
* **`acc.tool_calls` is emptied before the batch runs, and `acc` is re-declared every iteration.**
  `std::mem::take(&mut acc.tool_calls)` at `loop.rs:1399`; the batch runs on the taken vec at `:1424`. Nothing between
  the take and the loop's end (`:1427`) refills the accumulator — `acc.tool_calls` is pushed to in exactly one place,
  `turn.rs:235` — and the only exit in that range is `loop.rs:1420`, the clean-stop return, which is gated on
  `tool_calls.is_empty()` at `:1410`. The next iteration gets a fresh `TurnAccumulator::default()` at `:1320` anyway.

The paths into a later iteration are exhaustible and each is covered: the pre-stream overflow `continue`
(`loop.rs:1304`, before any stream exists), `handle_mid_stream_error`'s `Continue` (`:1348` → closes at
`turn.rs:327` or `:439`), `handle_empty_turn`'s `Continue` (`:1382` → `retry_abandoned_attempt`, `turn.rs:381-387` →
closes at `:439`; and its own batch is empty by the gate at `:1377-1380`), and the fall-through after the batch.

**Three more `agent_settled` emissions are not among the eight, and they are empty too.** Production emits
`agent_settled` at five places (`loop.rs:1117`, `:1209`, `:1419`, plus `turn.rs:706` and `:719` inside the two settle
functions), so a reader auditing "every settle" by grepping that frame finds five emissions behind eight call sites and
should not have to re-derive which are which. `loop.rs:1419` is the accepted clean stop, gated on
`tool_calls.is_empty()` (`:1410`); `loop.rs:1117` settles prompts the Stop drained from the queue in `run()`'s cancel
arm, and `loop.rs:1209` settles the prompt `handle_prompt` skipped on an already-cancelled token — in both, no turn body
ran, so nothing was announced.

**One pointer to retarget at ship — open work, one line.** `settle_cancelled`'s own doc comment sends a reader to the
eight-site table "in `docs/roadmap/settle-announced-tool-rows.md`" (`turn.rs:695-696`), and that file is deleted on
ship, as every plan in this repo family is. This file is its successor; the comment should name it instead. Not fixed
here because Task 3 owns two files and `turn.rs` is not one of them.

## 3. Three constraints on the helper that are load-bearing — **recorded**

Each is here because of what breaks if the helper is tidied, not because the shape is pretty. All three are stated on
the helper itself (`turn.rs:634-676`) and each has a test that reddens when it is violated.

* **Iterate the ordered `Vec<ToolCall>`, never `TurnState::announced_tool_calls`.** That field is a `HashSet<String>`
  (`normalize.rs:72`) and it is insert-only: the only writes anywhere in the crate are `:259`, `:344`, `:389` and `:456`
  — the only `.remove(` calls in `normalize.rs` hit `toolcall_args` (`:323`) and the display-store accumulators (`:722`,
  `:739`). Nothing clears it per attempt or per turn either: the production `TurnState` is built once, at
  `loop.rs:783`, and lives for the whole Worker (the only other construction is the nested-call sink's own fresh state,
  `loop/emit.rs:143`). And it does not hold only tool calls — `compaction_start` mints a `compact:{n}` card id
  (`normalize.rs:454`) and inserts it into the same set (`:456`), because a compaction renders as a tool card. Closing
  from the set would therefore fake a failed tool close for every compaction card the session ever opened, in an
  arbitrary order, and `compute_display_rows`' `tool_call_update` arm `merge_json`s onto the card (`:772-803`) — so the
  forged close would overwrite the summary `compaction_end` had put there. The `Vec` is transcript order, which is what
  the desktop reads these frames as; `the_closes_arrive_in_accumulator_order_and_every_one_precedes_the_settle` declares
  its ids `zzz-`/`mmm-`/`aaa-` so declaration order is the REVERSE of sorted order and no set, hash, sort or reverse can
  produce the asserted sequence.
* **Frames only, never transcript rows.** No `ChatMessage { role: Tool }` is pushed and nothing is appended to
  `turn_tool_results` — contrast `run_tool_batch`, which pushes at `turn.rs:622-627` and persists at `:628`. An
  abandoned attempt persists NO assistant row, so a tool-result message would dangle with nothing for the provider to
  pair it against — the orphan a provider 400s on. Pinned by `an_abandoned_tool_call_appends_no_tool_message`
  (`compaction_overflow.rs:657-723`), which reloads the transcript from the store and asserts both that no `Tool` row
  exists and that the persisted assistant row's `tool_calls` is `None`.
* **`abandoned_tool_result()`'s text stays byte-identical** (`loop/tools.rs:20-30`). Four places pin it verbatim —
  `compaction_overflow.rs:624-631` and `:778-785`, `turn_lifecycle.rs:743-747`, and `tool_row_settle.rs`'s
  `const ABANDONED` (`:55`) — and the no-double-close pin asserts the ABSENCE of that string on a batch that ran for
  real (`tool_row_settle.rs:1056-1060`), which is how a re-close would announce itself. The wording is the point: the
  tool did not FAIL, it was discarded.

## 4. `message_retracted` does not close rows — **accepted, two reasons**

**Reason one: both emit sites already close first, and the ordering is pinned.** `turn.rs:327` closes and `:329`
retracts; `turn.rs:439` closes and `:441` retracts. Those two are the only `message_retracted` emissions in production
source. `a_retried_reply_closes_the_tool_calls_it_abandoned` pins the order by construction rather than by inspection:
it waits for the close first, so a retraction emitted BEFORE the close would be missing from what follows and the
count assertion fails. `a_mid_stream_overflow_closes_the_abandoned_tool_call` (`:797-802`) pins the same order on the
second path.

**Reason two: `normalize`'s retraction arm could not do it correctly at all.** That arm (`normalize.rs:544-559`) knows
one thing — `st.current_message_id`, the id the retried call REUSES, which is why the retraction is a protocol event.
It has no tool-call id, and `announced_tool_calls` is a flat set with no per-message attribution, so the best such an
arm could do is close EVERY announced id of the turn, including the rows belonging to the same turn's other messages —
a batch that ran and closed legitimately, or an earlier attempt's cards. The display side has the same split: the
retraction arm there (`:713-750`) overwrites rows keyed by `messageId`, while tool-call rows are keyed by `toolCallId`
(`:765-769`, `:797-802`) — two key spaces, no join. Redundant at both sites, unimplementable in the one place someone
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

**It does not remove the third.** A close dropped by `Lagged` (§5) is still unfixable downstream, and position remains
the only signal the transcript carries in that case — the desktop's own commit records the matching residual: an orphan
followed only by open rows is byte-identical to a legitimate mid-batch transcript, so it still wins there, and `at` does
not separate them because a batch's announces share a stamp. The engine-side cure removes the ambiguity at its source
for the two producers it owns; it does not make the reader's discriminator unnecessary.

And the cure does not reach a user yet. `archimedes/src-tauri/Cargo.toml:47` pins `fulcrum-core` at
`78e595436ac5612d8ef87f692bee61f48f6e58e7` (line 47 in the working tree AND on `origin/main`). That rev predates the
fix — `git show 78e59543:src/harness/loop/turn.rs` has no `close_announced_tool_calls` — and, consistent with §1 of
`daemon-hardening.md`, it carries no `fulcrum` member at all, verified the same way `git ls-tree` verified it there.
**Open work:** the desktop re-pins. Until it does, the engine-side cure is dead code as far as any user is concerned
and `744aac23`'s reader is the only thing standing between this bug and them.

## 7. Rows orphaned before the fix stay orphaned — **recorded**

The fix is forward-only, and it is worth naming exactly what is on disk. Each frame persists a display row —
`normalize`'s announce persists a `tool-call` row (`normalize.rs:765-770`) and each update re-persists it (`:797-802`) —
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

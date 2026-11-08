---
status: committed
title: Settle announced tool calls on abandoned attempts (issue #11)
source: https://github.com/archimedes-codes/fulcrum/issues/11
---

# Settle announced tool calls on abandoned attempts

## Problem (verified against `45e576d`)

A `tool_call` ANNOUNCE frame is emitted when the model finishes naming a call:
`consume_stream`'s `ProviderEvent::ToolCall` arm (`src/harness/loop/turn.rs:217-231`) emits
`toolcall_end` + `tool_execution_start`, and `normalize` turns either into a `tool_call` frame with
`status: "in_progress"` (`src/normalize.rs:259-267`, `:336-352`, `:367-398`). The row leaves
`in_progress` **only** on a `tool_call_update` carrying `completed`/`failed`, which `normalize` derives
from `tool_execution_end` alone (`src/normalize.rs:413-423`). `normalize` has no arm for `turn_end` or
`agent_settled` — both fall into the catch-all `_ => Vec::new()` (`src/normalize.rs:586`).

`settle_cancelled` and `settle_error` (`src/harness/loop/turn.rs:615-632`) emit
`turn_abandoned` + `turn_end` + `agent_settled` and close nothing. So every exit that reaches them
without running the inline close loop leaves every tool the abandoned attempt announced permanently
`in_progress`. The clearest statement is an asymmetry inside one function:
`retry_abandoned_attempt`'s cancel arm (`src/harness/loop/turn.rs:409-413`) settles and returns,
**skipping** the close loop a few statements below (the `for tc in &acc.tool_calls` at `:416`).

## Ground truth: the eight settle sites

Line numbers in this table are the PRE-FIX positions at `45e576d` — which is `main` — where every one
of them was read; they are not HEAD positions, and `docs/features/abandoned-tool-rows.md` carries the
post-fix numbers.

| # | Site | `acc` in scope? | Announced rows possible? |
| - | ---- | --------------- | ------------------------ |
| 1 | `turn.rs:126` `compact_if_due` (overflow `Skipped`) | no | no — see the derivation below |
| 2 | `turn.rs:331` `handle_mid_stream_error` fall-through | **yes** | **yes** |
| 3 | `turn.rs:411` `retry_abandoned_attempt` cancel arm | **yes** | **yes** |
| 4 | `turn.rs:437` `retry_abandoned_attempt` budget exhausted | **yes** | **yes** |
| 5 | `loop.rs:1241` top-of-loop cancel | no | no |
| 6 | `loop.rs:1268` model-call cancel arm | no | no |
| 7 | `loop.rs:1283` model call returned `Err` | no | no |
| 8 | `loop.rs:1299` cancel after `consume_stream` | **yes** | **yes** |

Site 8 is the one the field report is about — Stop pressed while the model streams tool calls — and it
is NOT in the issue's list of four. Sites 5–7 and 1 are vacuous, and the reason is a fact, not a hope:
the only paths into a later loop iteration are a completed batch (which closes every call it processed,
`turn.rs:594-600`, including on cancel — a cancelled turn still gets `cancelled_tool_result()` per call)
or a `TurnFlow::Continue` from an abandon path (which closes at `:311` / `:416`). And `acc.tool_calls` is
emptied by `std::mem::take(&mut acc.tool_calls)` — the statement that hands the calls to the batch, in
`handle_turn` (`loop.rs:1358` at this baseline, `:1399` post-fix) — before the batch runs, so a settle
reached after the batch sees an empty accumulator by construction.

## Design

**One close implementation, reached from every settle.** Extract the two byte-identical inline loops
(`turn.rs:311-321`, `turn.rs:416-425`) into one method beside the settle functions:

```text
fn close_announced_tool_calls(&self, tool_calls: &[ToolCall])
```

It emits `tool_execution_end { tool_call_id, tool_name, result, is_error: true }` per call, with
`Self::abandoned_tool_result()` (`src/harness/loop/tools.rs:20`) as `result` — the existing text,
unchanged. Callers: the two existing abandon paths (`:311`, `:416`, which `Continue` and so do not
settle), and `settle_cancelled` / `settle_error`, which take the abandoned attempt's announced calls and
close them before emitting `turn_end`.

Three constraints that are load-bearing, each because of what breaks if it is "tidied":

- **Iterate the ordered `Vec`, never `TurnState::announced_tool_calls`.** That `HashSet`
  (`src/normalize.rs:72`) is insert-only (no removal anywhere) and also holds compaction card ids
  (`compact:N`, `src/normalize.rs:456`) that are closed by `compaction_end`, not by
  `tool_execution_end`. Closing from it would emit a bogus tool close for every compaction card. The
  desktop reads these frames as transcript order; the `Vec` is that order.
- **Frames only, never transcript rows.** Do not push a `ChatMessage { role: Tool }` and do not append to
  `turn_tool_results`. The abandoned attempt persists no assistant row (`compactor.turn_abandoned` +
  `message_retracted`), so a tool-result message would dangle — no preceding assistant `tool_calls` for
  the provider to pair it with. This is what the two inline loops already do; keep it.
- **Sites with no attempt pass `&[]` and say why.** The emptiness is derived from the table above; the
  comment must carry the derivation so the claim stays falsifiable instead of becoming a lie waiting for
  a new call site.

## Decisions the issue asks for, taken

**`message_retracted` does not close rows.** Both emit sites already close first and retract second
(`turn.rs:311→322`, `:416→427`), and the ordering is already pinned
(`src/harness/loop/tests/compaction_overflow.rs:796-800`). And `normalize`'s retraction arm cannot do it
correctly at all: it knows only `messageId` (`src/normalize.rs:544-558`), while `announced_tool_calls` is
a flat set with no per-message attribution, so it could only close rows belonging to other messages of
the same turn. Redundant at every site, unimplementable in the one place it would be written.

**The dropped-frame residual (issue bullet 3) is not fixable here.** The pump's `Lagged` arm drops frames
off the 256-slot broadcast (`src/worker/manager/pump.rs:57-68`, `src/worker/client.rs:402`); an engine
that emitted the close more completely cannot deliver it more reliably. Recorded as a residual, with its
consequence: an orphaned announce holds `SubagentLiveness.in_flight` non-empty
(`src/worker/manager/subagent_flow.rs:399-401`), which **disables** the dead-bound arm outright
(`subagent_flow.rs:1051`) rather than lengthening it, so the drive can then only end on settle, process
exit, or cancel.

## Tasks

### Task 1 — the close helper, hoisted into both settle functions

**Parallel: no — owns `turn.rs`, and Tasks 2–3 depend on it.**

Files:
- Modify `src/harness/loop/turn.rs` (the helper, both settle signatures, sites 2/3/4, both existing
  inline loops replaced by the helper call, site 1 passes `&[]`)
- Modify `src/harness/loop.rs` (sites 5–8)
- Test `src/harness/loop/tests/turn_lifecycle.rs`

RED first: `retry_abandoned_attempt`'s cancel arm — a scripted provider that announces a `ToolCall` and
then yields `ProviderError::Retryable`, with the turn token cancelled during the backoff (the
`CancelAfterUsageProvider` shape at `src/harness/loop/tests/context_usage.rs:580-620` is the working
pattern for cancelling the turn token from inside a stream). Assert a `tool_execution_end` for the
announced id arrives, **before** `turn_end` / `agent_settled`. It fails today.

Then GREEN by the hoist. Also assert, on the same test: `is_error == true`, the result text is the
existing `abandoned_tool_result()` string, no `ChatMessage { role: Tool }` was pushed to
`loop_.messages`, and `turn_end.tool_results` did not gain an entry.

### Task 2 — the remaining exits, each with its own test, none vacuous

**Parallel: no — depends on Task 1.**

Files:
- Test `src/harness/loop/tests/tool_row_settle.rs` (new module)
- Modify `src/harness/loop/tests/mod.rs` (one `mod` line)

One test per exit that can orphan, each with a witness that the exit was actually walked (the
`compaction_overflow.rs:836-842` vacuity guard is the house pattern):

- site 2 — mid-stream `ProviderError::Fatal` after a `ToolCall` announce → closes, then settles
- site 3 — the cancel-during-backoff asymmetry, asserted as the *pair*: the backoff-completes path and
  the cancelled path of the SAME function close identically
- site 4 — retry budget exhausted after a `ToolCall` announce → closes, then settles
- site 8 — cancel after `consume_stream` announced calls → closes, then settles
- **order pin** — every close precedes `turn_end` and `agent_settled`, and the closes arrive in
  `acc.tool_calls` order for a multi-call batch (the ordering the desktop reads)
- **no-double-close pin** — a batch that ran to completion emits exactly one close per call: the settle
  must not re-close what `run_tool_batch` already closed
- **vacuity pin** — an abandoned attempt that announced nothing emits no `tool_execution_end` (this
  already exists as `an_abandoned_attempt_that_announced_nothing_closes_nothing`; prove the new settle
  path keeps it rather than assuming)

### Task 3 — durable notes and the two decisions, with proof

**Parallel: no — describes shipped behaviour.**

Files:
- Create `docs/features/abandoned-tool-rows.md`
- Modify `src/worker/manager/subagent_flow.rs` (doc comment only — `:354-356` cites ADR 0048 for
  "an `end` without a `start` is LEGAL"; after this change the claim is broader and should name both
  mechanisms)

The doc records: the invariant (`a turn never settles with an announced tool row left open`), the eight
site table with which are vacuous and the derivation, the three load-bearing constraints, the
`message_retracted` decision with its two reasons, the `Lagged` residual with its liveness consequence,
and the desktop-side mitigation that already shipped (`archimedes-codes/archimedes`
`src/lib/subagentActivity.ts` — the earliest-open-row-after-newest-settled reader) with what this fix
removes from it and what it does not (a dropped close frame is still unfixable downstream).

## Parallelisation

Fully serial. The issue carries no `## Parallelisation` section, and the dependency is real, not
incidental: Task 2 asserts behaviour Task 1 introduces, and Task 3 documents both. Task 3's two files are
disjoint from Tasks 1–2, but running it against pre-fix behaviour would document the bug.

## Validation bar (every task, in this order)

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
bash scripts/check-rquickjs-allocator.sh
bash scripts/check-schema-drift.sh
```

Baseline at `45e576d`, recorded before Task 1: **1401 passed / 0 failed / 1 ignored** (lib) + **5 passed**
(`tests/fixture_bins.rs`) + 0 elsewhere; clippy, fmt, allocator guard and schema drift all clean.
`check-no-stray-processes.sh` refuses locally **by design** — it requires `FULCRUM_CI_RUN_MARKER` and
refuses to certify a table it was never allowed to read; CI sets the marker. Not a regression.

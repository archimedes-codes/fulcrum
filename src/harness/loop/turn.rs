use std::collections::HashMap;

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::emit::TurnAccumulator;
use super::AgentLoop;
use super::CompactionRan;
use super::CompactionTrigger;
use crate::events::CompactionReason;
use crate::events::RpcEvent;
use crate::harness::provider::openai::content_is_the_sanitise_placeholder;
use crate::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ProviderError, ProviderEvent, ToolCall,
    ToolSpec,
};
use crate::policy::Decision;
use crate::tools::{ContentBlock, ImageRef, ToolResult};

pub(super) enum TurnFlow {
    Continue,
    Return,
}

/// A rejected call's result (issue #93). Mirrors `cancelled_tool_result` /
/// `deny_tool_result`: one text block, no details, `is_error: true`. The text
/// is `unknown_param_error`'s sentence — the model gets the offending key AND
/// the key it should have sent. `is_error` serialises as `isError`
/// (`tools/types.rs`), the wire contract the frontend already reads, which is
/// why "fail properly" needs no wire change.
fn unknown_param_result(message: &str) -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text {
            text: message.to_string(),
        }],
        details: None,
        is_error: true,
    }
}

pub(super) struct TurnLoopState {
    pub(super) turn_tool_results: Vec<Value>,
    pub(super) message_started: bool,
    pub(super) overflow_recovered: bool,
    /// Rounds through the turn loop for THIS prompt (the round ceiling; see
    /// `MAX_MODEL_ROUNDS_PER_PROMPT` in `loop.rs` — its unit is one model call
    /// plus its tool batch, NOT one provider request).
    ///
    /// It lives here and not on `RetryPolicy` because `TurnLoopState` is built
    /// once per prompt at the single construction site in `handle_turn`, which
    /// is exactly the lifetime a per-prompt ceiling needs. `RetryPolicy.attempt`
    /// cannot host it: it is built once per Worker, never reset between prompts,
    /// and its value is already on the wire as the `auto_retry_*` attempt number.
    ///
    /// Deliberately NOT reset on the accepted-attempt path (unlike
    /// `overflow_recovered` below): a round ceiling that forgot the overflow
    /// retries and the empty-turn retries would not bound the thing it exists to
    /// bound.
    pub(super) model_rounds: u32,
    /// How many consecutive batches have been refused with the SAME signature
    /// (see `MAX_IDENTICAL_REFUSAL_RUNS` in `loop.rs` — 3 is the consecutive
    /// run, not a total, and the bound is PER PROMPT because this whole struct
    /// is rebuilt at the top of `handle_turn`). Reset to 0 by any batch that is
    /// not an exact repeat of the one before it, which includes any batch with an
    /// accepted call, and by a `refused` slice whose length disagrees with the
    /// batch (`batch_signature` answers `None` rather than guessing).
    ///
    /// "Refused" is the predicate in `refused_before_the_model_could_act` (this module):
    /// the pre-dispatch param check, a parse `defect`, or a dispatched call the
    /// EXECUTOR refused for omitting an advertised `required` parameter that no
    /// advertised alias supplied. NOT "the tool errored" — a permission DENIAL and
    /// a failing command are both `is_error` and neither is a refusal, so neither
    /// grows this counter; see `MAX_IDENTICAL_REFUSAL_RUNS` for where the line is
    /// drawn and which shapes it deliberately lets through.
    pub(super) identical_refusal_runs: u32,
}

/// The signature of a batch that was refused WHOLESALE — `Some` only when EVERY
/// call in it was refused, `None` the moment one call was accepted.
///
/// The `None`-on-any-accepted rule is what makes a mixed batch RESET the run
/// rather than extend it: an agent that fixes one key and re-sends the rest is
/// making progress, and a guard that counts that as a repeat punishes exactly
/// the behaviour the rejection sentence asks for.
///
/// The parts are sorted and joined with `\n`, each spelled
/// `name + "\u{0}" + arguments`, so the join is unambiguous: `\u{0}` cannot
/// appear in a tool name (the names come from `tool_specs()`), so no pair of
/// `(name, arguments)` can be re-split into a different pair that hashes the
/// same way. Sorting makes the signature independent of the order the provider
/// happened to emit the calls in, which is the difference between catching a
/// model that repeats itself and missing one that permutes — pinned by
/// `batch_signature_is_independent_of_the_call_order`, because that last clause
/// is a claim about the `sort()` and nothing else exercised it.
///
/// Takes `&[bool]`, NOT `&[Option<String>]`: the caller must not have to keep
/// the rejection SENTENCES alive merely to ask whether a call was refused. The
/// vector is index-aligned with `tool_calls`, straight off `run_tool_batch`.
///
/// A LENGTH MISMATCH between the two slices answers `None` — no signature, so
/// the run resets. `zip` would instead stop at the shorter slice and could
/// return `Some` for a batch whose LAST call was accepted, which is a false
/// positive on the one guard whose false positives end the user's prompt. The
/// production caller builds `refused` to the exact length of `tool_calls`, so
/// this arm is unreachable from `handle_turn`; it is here because this is a
/// `pub(super)` function over two INDEPENDENT slices, and the silent early stop
/// is the failure mode that would cost the most and show up the least.
pub(super) fn batch_signature(tool_calls: &[ToolCall], refused: &[bool]) -> Option<String> {
    if tool_calls.len() != refused.len() {
        return None;
    }
    if refused.iter().any(|was_refused| !*was_refused) {
        return None;
    }
    let mut parts: Vec<String> = tool_calls
        .iter()
        .map(|tc| format!("{}\u{0}{}", tc.name, tc.arguments))
        .collect();
    parts.sort();
    Some(parts.join("\n"))
}

/// Whether the ADVERTISED schema for `tc` names a `required` parameter the call
/// does not carry, as a plain yes/no.
///
/// This is the structural half of the guard's second trigger class — see
/// `refused_before_the_model_could_act` for what it is for and why it needs
/// the `is_error` half beside it. It reads the SAME `advertised_specs()` value
/// `unknown_param_error` reads (the same `tools[]` the provider was sent and the
/// same `<tools>` the prompt rendered, ADR 0017's byte-identity), so the guard
/// cannot hold the model to a `required` list it was never shown.
///
/// Deliberately NOT a second reading of `unknown_param_error`. That checker
/// reports EXTRANEOUS keys and answers `None` for a call with no keys at all —
/// which is exactly the shape of the burst this exists for, so reusing it would
/// reuse the wrong question. This asks the other one: is a key the schema says
/// is mandatory absent from what arrived? It is a membership test against the
/// advertised shape, and nothing else — no value-type check, no prose matching.
///
/// Three absences answer `false`, each because a `true` there would be a false
/// positive on a shape that is NOT a stuck loop:
/// * no spec for the (alias-resolved) name — an unknown tool is
///   `dispatch_tool`'s `unknown tool:` arm, and a disabled tool was never
///   advertised, so there is no `required` list to hold the call to;
/// * no `required` array — `list_agents`, `list_skills`, `mcp`, `web_search`
///   declare none, so NOTHING they receive is a missing parameter;
/// * a `required` key that IS present, whatever its type — `bash` with
///   `{"command": 42}` omits nothing, so the guard does not read the executor's
///   type refusal as a re-send instruction. That is a KNOWN UNDER-COUNT: such a
///   run is bounded by the round ceiling instead. Narrower than the truth is
///   the accepted direction here, because the failure mode of the wider reading
///   is ending a user's prompt over a tool call that was fine.
/// * a `required` key the call satisfies through an **advertised alias** the
///   executor accepts (see `declared_alias_of` — the second known exclusion,
///   and the reason satisfaction is not a bare `contains_key`).
fn missing_required_param(specs: &[ToolSpec], tc: &ToolCall) -> bool {
    let Some(spec) = specs
        .iter()
        .find(|s| s.name == crate::harness::dispatch::effective_tool_name(&tc.name))
    else {
        return false;
    };
    let Some(required) = spec.parameters.get("required").and_then(Value::as_array) else {
        return false;
    };
    // A non-object `arguments` (a top-level JSON string) carries no keys, so it
    // omits every required one. `unknown_param_error` cannot see that shape at
    // all — its `as_object()?` answers `None` — which is the same blind spot the
    // extraneous-key rule has, and the reason this half exists beside it.
    let keys = tc.arguments.as_object();
    required
        .iter()
        .filter_map(Value::as_str)
        .any(|key| !required_key_satisfied(spec, keys, key))
}

/// Whether the call carries the required key `key`, directly **or through an
/// advertised alias of it that the executor honours**.
///
/// The bare `contains_key` was the bug: `get_search_content` advertises
/// `required: ["response_id"]` while its `properties` ALSO carries `responseId`,
/// and `exec_get_search_content` reads both spellings
/// (`params.get("response_id").or_else(|| params.get("responseId"))`). A model
/// that calls with `{"responseId": …}` therefore omits the spelling `required`
/// names while sending a key the executor accepts, and when the tool then errors
/// for an unrelated reason (its `Response not found` cache miss) class 2 fired on
/// a call that was built correctly. This is the same shape as the `ls` row of
/// `the_refusal_predicate_is_the_required_key_and_the_error_together` — a call the
/// executor accepts despite the advertised `required` list — and that one is
/// excluded only because `exec_ls` defaults `path` and SUCCEEDS.
///
/// Satisfaction is membership, whatever the value's type or emptiness — the SAME
/// rule the direct key is held to two lines up, so the two paths cannot disagree.
/// `{"responseId": ""}` is therefore a KNOWN UNDER-COUNT (the executor does
/// reject an empty alias with its own missing-parameter sentence); under-counting
/// is the accepted direction.
fn required_key_satisfied(
    spec: &ToolSpec,
    keys: Option<&serde_json::Map<String, Value>>,
    key: &str,
) -> bool {
    let Some(args) = keys else {
        return false;
    };
    if args.contains_key(key) {
        return true;
    }
    let Some(properties) = spec.parameters.get("properties").and_then(Value::as_object) else {
        return false;
    };
    properties.iter().any(|(alias, schema)| {
        declared_alias_of(schema) == Some(key) && args.contains_key(alias.as_str())
    })
}

/// The required key a property schema declares ITSELF an alias of, if it does.
///
/// **READ THIS BEFORE TRUSTING THE GUARD'S ALIAS HALF.** There is NO structural
/// way in this tree to know that one advertised property is an alias of another.
/// The aliases are only *described*: `tool_specs()` spells them
/// `"responseId": { "type": "string", "description": "Alias for response_id" }`,
/// and the pairing that actually matters — the executor reading both spellings —
/// lives in `src/web/tools.rs`, unlinked to the schema by anything a compiler can
/// see. So the narrowest rule available reads that one declaration. It is a
/// prefix test on the ADVERTISED SHAPE (the schema the model was shown), which is
/// a different thing from the RESULT-text matching this guard forbids: a schema
/// description is part of the contract the model is handed and it has a pin
/// (`the_alias_rule_reads_the_wording_tool_specs_actually_uses`), whereas a tool
/// RESULT sentence is prose someone rewords for tone and no guard may lean on.
///
/// What this function returns is only the NAMED key; the caller
/// (`required_key_satisfied`) additionally requires that key to be in the SAME
/// `properties`. That conjunct is what keeps the rule narrow: a description
/// saying `Alias for something-not-advertised` satisfies nothing, and a property
/// whose description merely MENTIONS an alias mid-sentence is not a declaration
/// (the prefix is anchored, case-sensitive, with its trailing space).
///
/// THE COUPLING THIS CUTS BOTH WAYS: rewording one of those descriptions breaks
/// the recognition and the alias stops being excluded — the over-count direction,
/// i.e. the bug coming back.
/// `the_alias_rule_reads_the_wording_tool_specs_actually_uses` walks the wording of
/// every `Alias for` declaration in `tool_specs()` rather than only the one this
/// guard needs, but READ ITS DOCBLOCK: it iterates only descriptions that STILL
/// start `Alias for `, so a single reword makes it see one fewer alias and it
/// passes. The pin that catches a reword is the behavioural
/// `a_call_using_an_advertised_alias_is_never_a_refusal_run`.
pub(super) fn declared_alias_of(schema: &Value) -> Option<&str> {
    let description = schema.get("description").and_then(Value::as_str)?;
    let named = description.strip_prefix("Alias for ")?;
    let named = named.trim().trim_matches('`').trim_end_matches('.').trim();
    if named.is_empty() {
        return None;
    }
    // The caller's `properties` membership check needs the raw name; the trim
    // above only normalises the DECORATION around it.
    Some(named)
}

/// Whether the engine refused call `i` in a way whose WHOLE POINT is "re-send
/// this with correct arguments" — the predicate the identical-refusal run counts.
///
/// Two trigger classes, joined by OR, and nothing else:
///
/// 1. **Refused BEFORE dispatch**: `unknown_param_error` named an extraneous key
///    or the call carried a parse `defect` (rule 0). `param_errors[i]` is the
///    one source of truth for this and it is read, never recomputed.
/// 2. **Dispatched, and refused by the executor for omitting a required
///    parameter**: the advertised `required` list names a key the call does not
///    carry (nor satisfy through an advertised alias) AND the result came back
///    `is_error` AND the call was not DENIED by the permission gate.
///
/// Class 2 is what makes the guard fire on the shape that motivated the whole
/// change. The field report is **303 `{}`-argument `bash` calls in 19 minutes**,
/// and class 1 cannot see it: `{}` has no keys, so there is no extraneous key to
/// name, `unknown_param_error` answers `None`, the call is dispatched, and
/// `exec_bash` answers ``bash: missing `command` parameter``. A tool-RESULT
/// error, invisible to the pre-dispatch checker — so at class 1 alone the run
/// reset to 0 every round and the burst ran to the 64-round ceiling. Class 2 is
/// bounded by two pins:
/// `a_run_of_identical_empty_argument_batches_stops_the_turn_as_capped` (it does
/// fire) and `a_repeated_batch_of_failing_well_formed_calls_is_never_a_refusal_run`
/// (a failing command does NOT fire it).
///
/// Both halves of class 2 are load-bearing, and neither is `is_error` alone —
/// which the plan rejected and still rejects. A `bash` command that fails twice
/// is progress, not a stuck loop, and a guard that ends the user's prompt over
/// one is worse than the loop it prevents; so the `required` half is what makes
/// the sentence "the engine told the model to re-send this" a fact rather than a
/// guess.
///
/// # The exclusions class 2 needs beside `is_error`, and what is left
///
/// Both halves are necessary and, on their own, NOT sufficient. Three real
/// shapes satisfy `is_error` + a missing advertised `required` key without being
/// "the engine told the model to re-send this with correct arguments", and all
/// three are excluded here rather than being argued about in prose:
///
/// * **A permission DENIAL** (`denied`). The gate's refusal arms build their
///   result IN THIS FILE (`run_tool_batch`), with `is_error: true`, so a denied
///   `edit` that omits `old_text`/`new_text` satisfied class 2 and three
///   identical re-sends after a user said **no** settled the turn as
///   `turn_capped`. A denial is a user decision, not a mistake to correct, and
///   `MAX_IDENTICAL_REFUSAL_RUNS`' docblock already said so; the flag is threaded
///   from the two arms that construct the denial, exactly as `codemode`'s
///   `run_nested` threads its own `denied` bool for the same reason. It is
///   structural — no result text is consulted. Pinned by
///   `a_permission_denial_is_never_a_refusal_run`.
/// * **A `sudo_exec` sub-prompt refusal** — the declined confirm, or a cancelled
///   password entry. This is the THIRD corridor, and unlike the gate's two arms
///   it is built outside this file, inside `sudo_run_flow`
///   (`src/interactive/mod.rs`), so it does not arrive through `gate_tool` and
///   the gate's flag cannot cover it. `sudo_run_flow` now returns
///   `(ToolResult, bool)` for exactly that reason and `dispatch_tool` propagates
///   the second half — the same shape `run_nested` already used, so the crate has
///   ONE way to say "a person refused this call". Before it did, this was an
///   **OVER**-count, and the over-count direction is the one that ends a prompt:
///   `sudo_exec {"command": "x"}` — `command` present, `reason` OMITTED, which is
///   what satisfies class 2's `required` half — plus a declined confirm plus
///   three identical re-sends was measured at **4 provider calls and one
///   `turn_capped`**. Pinned by
///   `a_sudo_exec_confirm_refusal_is_never_a_refusal_run`; the shape that stays
///   counted is the pre-prompt empty-`command` refusal, which nobody was asked
///   about and which genuinely is a re-send instruction.
/// * **A call that satisfies a required key through an advertised alias**
///   (`required_key_satisfied`). Pinned by
///   `a_call_using_an_advertised_alias_is_never_a_refusal_run`. Note the direction
///   this exclusion's OWN fragility runs in: the alias is only ever DESCRIBED
///   (`"Alias for <key>"`), never marked, so rewording one description stops the
///   rule seeing it and the call is counted again — an OVER-count, and therefore
///   not a member of the under-count list below. The pin that catches it is the
///   behavioural one just named (measured: rewording one of the six leaves the
///   declaration census green).
///
/// What is DELIBERATELY NOT covered, all in the under-count direction (a stuck
/// run reaches the 64-round ceiling instead of 3 — the accepted cost, because the
/// cost of the opposite error is ending a legitimate prompt):
///
/// * `ls {}` — `path` is advertised `required` and IS absent, but `exec_ls`
///   defaults it and the call SUCCEEDS, so `is_error` excludes it. The case that
///   proves `required` alone is not enough.
/// * A `required` key present with the WRONG TYPE or an empty value.
/// * **The `sudo_exec` shape itself, now that its refusal is excluded.** The
///   exclusion is per CALL, not per reason, so a model that omits `reason`, gets
///   a confirm it ACCEPTS, and then re-sends identically is no longer counted
///   either. That is the residual for this tool and it is 64 rounds of a doomed
///   call — measured, and the accepted direction.
///
/// # Why the flag is a `bool`, and what actually guards a FOURTH corridor
///
/// The flag says "a person refused this call" and nothing else, so it cannot
/// distinguish a declined confirm from a declined password entry from a prompt
/// nobody has written yet — and it does not need to. What it genuinely cannot do
/// is notice a corridor nobody thought of: a new refusal site that forgets to
/// mark itself is invisible to every behavioural pin here, because each pin
/// scripts only the shape it knows about. A non-exhaustive enum would not close
/// that either — an unhandled variant still compiles at every `matches!` site.
///
/// The guard that does survive a fourth corridor is the census
/// [`crate::source_lints::every_denial_marking_site_in_the_batch_is_accounted`]:
/// it counts the denial-marking sites in `run_tool_batch`'s region and fails on a
/// count nobody chose, so an added corridor reddens instead of silently
/// over-counting.
///
/// The `turn.is_cancelled()` half of class 2 keeps the standing
/// **cancellation beats rejection** rule true of the new class too: every
/// synthetic cancel result (`cancelled_tool_result`, `sudo_cancelled_result`)
/// carries `is_error: true`, so without it a Stop pressed during a batch of
/// empty-argument calls would grow a run the user just asked to stop. It gates
/// class 2 and NOT class 1, because class 1's behaviour on a cancelled batch is
/// pre-existing and pinned elsewhere (`param_rejection.rs` deliberately declines
/// to assert `rejected` on a cancelled batch, for exactly this reason).
fn refused_before_the_model_could_act(
    tc: &ToolCall,
    param_error: Option<&String>,
    result: &ToolResult,
    specs: &[ToolSpec],
    turn_cancelled: bool,
    denied: bool,
) -> bool {
    if param_error.is_some() {
        return true;
    }
    // A denial is the USER's decision — never a re-send instruction, whatever the
    // result's `is_error` says. See the docblock's first exclusion.
    !turn_cancelled && !denied && result.is_error && missing_required_param(specs, tc)
}

impl AgentLoop {
    /// Prepare the transcript and bookkeeping for a new turn.
    pub(super) fn begin_turn(&mut self, text: &str, images: &[ImageRef]) {
        // (finding 6) A queued compaction belongs to the turn that queued it,
        // and this turn has queued none: `enter_turn()` is the ONE clear of the
        // latch (see [`TurnControl`]) — calling it HERE is what makes the
        // one-turn lifetime a method rather than a rule this function has to
        // remember. Without it the fresh prompt turn inherits the dead turn's
        // `overflow` trigger, compacts for a reason it never hit, and —
        // `run_compaction` having shrunk nothing — settles as an error without
        // a model call.
        self.turn_control.enter_turn();
        let content = if images.is_empty() {
            MessageContent::Text(text.to_string())
        } else {
            let mut blocks: Vec<ContentBlock> = Vec::new();
            if !text.is_empty() {
                blocks.push(ContentBlock::Text {
                    text: text.to_string(),
                });
            }
            for image in images {
                blocks.push(ContentBlock::Image {
                    image: image.clone(),
                });
            }
            MessageContent::Blocks(blocks)
        };
        self.messages.push(ChatMessage {
            role: ChatRole::User,
            content,
            tool_call_id: None,
            tool_calls: None,
        });
        self.persist_transcript_message();
        // The turn's own opening event FIRST — the context frame below is
        // session-scoped bookkeeping, so it must not lead the turn it belongs to.
        self.emit(RpcEvent::turn_start);
        // The prompt turn is in the transcript: measure it NOW. `should_compact()`
        // is read at the TOP of the loop below, so a push that left the
        // `Compactor` untouched was invisible to the turn's FIRST threshold
        // check — a large pasted prompt turn therefore deferred a compaction that
        // was already due by a whole model call (the call that had to send
        // the oversized context to the provider). Cheap: `refresh` sums only
        // the trailing slice. Measured AFTER `turn_start` (the frame is
        // bookkeeping), still BEFORE the first threshold read.
        self.note_context();
    }

    /// Run context compaction if triggered or past threshold.
    pub(super) async fn compact_if_due(
        &mut self,
        turn: &CancellationToken,
        turn_tool_results: &mut Vec<Value>,
    ) -> TurnFlow {
        let trigger = self.turn_control.take();
        let due = trigger.is_some() || self.compactor.should_compact();
        if due {
            let t = trigger.unwrap_or(CompactionTrigger {
                reason: CompactionReason::Threshold,
                will_retry: false,
            });
            let ran = self
                .run_compaction(t.reason, t.will_retry, None, turn)
                .await;
            // (issue #90) Only a `Skipped` overflow ends the turn. Its kept tail
            // is ALONE over the window, so the oversized request is exactly what
            // just failed and a second one cannot succeed — `run_compaction` has
            // already emitted the single `compaction_end` carrying the
            // actionable `error_message`, so emit NOTHING here.
            //
            // A `Failed` overflow does NOT settle: the SUMMARY call is what
            // errored (a truncated summary, a transport hiccup), which is a fact
            // about that call and not about the turn's request, and the
            // transcript is intact — so the turn continues and the model call is
            // re-issued. Settling here was the bug: it left the user watching
            // the indicator clear and typing "continue", the symptom ADR 0045
            // exists to end.
            if t.reason == CompactionReason::Overflow && ran == CompactionRan::Skipped {
                // Site 1 of the ten in
                // `docs/features/abandoned-tool-rows.md` §2: there is NO
                // `TurnAccumulator` in this function's scope — the compaction
                // check runs at the TOP of the turn loop, before the model call
                // that produces one — so nothing has announced a row yet.
                // Derivation for the claim: the only paths into a later loop
                // iteration are a completed batch (which closes every call it
                // processed, `run_tool_batch`, including on cancel) or a
                // `TurnFlow::Continue` from an abandon path (which closes via
                // `close_announced_tool_calls` before it continues), and
                // `acc.tool_calls` is emptied by `std::mem::take` before the
                // batch runs — so a settle reached after the batch sees an empty
                // accumulator by construction. Hence `&[]` is VACUOUS here, not
                // merely convenient: if a call site ever appears that can reach
                // this line with announced calls open, this argument is false
                // and this literal is what must change.
                self.settle_error(std::mem::take(turn_tool_results), &[]);
                return TurnFlow::Return;
            }
        }
        TurnFlow::Continue
    }

    /// Stream consumption loop: accumulates text, tool calls, and usage deltas,
    /// returning any mid-stream error encountered (or None if completed / cancelled).
    pub(super) async fn consume_stream(
        &mut self,
        stream: &mut (impl futures_util::Stream<Item = ProviderEvent> + Unpin),
        turn: &CancellationToken,
        acc: &mut TurnAccumulator,
    ) -> Option<ProviderError> {
        let mut mid_stream_error = None;
        loop {
            tokio::select! {
                item = stream.next() => {
                    let Some(ev) = item else { break };
                    match ev {
                        ProviderEvent::TextDelta(d) => {
                            // (issue #122) The upstream sanitisation placeholder
                            // must never enter the transcript, the store, or the
                            // user's eyes. FILTER HERE: this is the ONE choke
                            // point every inbound text delta crosses, whatever
                            // the wire — the Anthropic (`anthropic.rs`) and
                            // Responses (`responses.rs`) parsers emit their text
                            // as whole `TextDelta`s and never pass through the
                            // OpenAI SSE parser, so this bolt is their filter.
                            //
                            // EXACT-MATCH ONLY, and deliberately NOT the
                            // empty-check: a whitespace-only delta (`" "`,
                            // `"\n"`) is a chunk providers really send between
                            // words, and dropping it would weld words and
                            // paragraphs together in the transcript — so only
                            // the placeholder itself is dropped, never mere
                            // emptiness (see `content_is_the_sanitise_placeholder`).
                            // A dropped delta is neither EMITTED (the frontend
                            // never sees it) nor ACCUMULATED (it is never
                            // persisted — `acc.text` is the only thing that
                            // becomes the assistant row's content).
                            //
                            // A placeholder SPLIT across deltas is reassembled
                            // and silenced by the OpenAI wire's SSE parser
                            // (`openai.rs::handle_chunk` — the filter of record,
                            // which sees the whole delta stream). Partial
                            // fragments of the literal are never matched HERE: a
                            // run like `"[Sys"` streams, and a banner quoted
                            // inside a longer line streams too.
                            //
                            // Ordering with issue #64: a placeholder-only reply
                            // now accumulates NO text, so it settles as the
                            // genuine empty-turn condition (`Stop` + empty text
                            // + no tool call) and `handle_empty_turn` retries it
                            // on the shared budget, persisting nothing. That is
                            // the desired reading — a placeholder-only model
                            // reply is not a successful turn.
                            if !content_is_the_sanitise_placeholder(&d) {
                                acc.text.push_str(&d);
                                self.emit(RpcEvent::message_update {
                                    usage: acc.usage_value(),
                                    assistant_message_event: json!({
                                        "type": "text_delta",
                                        "contentIndex": 0,
                                        "delta": d,
                                    }),
                                });
                            }
                        }
                        ProviderEvent::ThinkingDelta(d) => {
                            acc.thinking.push_str(&d);
                            self.emit(RpcEvent::message_update {
                                usage: acc.usage_value(),
                                assistant_message_event: json!({
                                    "type": "thinking_delta",
                                    "contentIndex": 0,
                                    "delta": d,
                                }),
                            });
                        }
                        ProviderEvent::ToolCallDelta(d) => {
                            self.emit(RpcEvent::message_update {
                                usage: acc.usage_value(),
                                assistant_message_event: json!({
                                    "type": "toolcall_delta",
                                    "id": d.id,
                                    "delta": d.arguments.unwrap_or_default(),
                                }),
                            });
                        }
                        ProviderEvent::ToolCall(tc) => {
                            let display_tc = self.normalized_for_display(&tc);
                            acc.tool_calls.push(tc.clone());
                            // The frame's `toolCall` is an EXPLICIT three-key
                            // whitelist, not `display_tc` serialized whole.
                            //
                            // (issue #193) `normalized_for_display` is
                            // `tc.clone()` for every non-`ask` tool, so a
                            // `defect` rides that clone, and `ToolCall`'s
                            // DERIVED `Serialize` emits it — `skip_serializing_if`
                            // suppresses it only when it is `None`. That put the
                            // malformed payload's `raw` text (unbounded, and
                            // model-supplied) on a wire this frame reaches:
                            // `EmitPipeline::run` sends every `RpcEvent` on the
                            // events channel and the daemon
                            // (`fulcrum/src/serve.rs`'s `Event` arm) re-broadcasts
                            // the event verbatim to every subscribed socket
                            // client. Building the object from named fields makes
                            // the display wire flat BY CONSTRUCTION — the same
                            // discipline `WireToolCall`'s serializer uses on the
                            // request side — so a field added to `ToolCall` later
                            // cannot quietly join it. Pinned by
                            // `the_toolcall_end_frame_carries_only_id_name_and_arguments`.
                            // `normalize`'s `toolcall_end` arm reads exactly these
                            // three keys and nothing else.
                            self.emit(RpcEvent::message_update {
                                usage: acc.usage_value(),
                                assistant_message_event: json!({
                                    "type": "toolcall_end",
                                    "toolCall": {
                                        "id": &display_tc.id,
                                        "name": &display_tc.name,
                                        "arguments": &display_tc.arguments,
                                    },
                                }),
                            });
                            self.emit(RpcEvent::tool_execution_start {
                                tool_call_id: tc.id.clone(),
                                tool_name: tc.name.clone(),
                                args: display_tc.arguments.clone(),
                            });
                        }
                        ProviderEvent::Usage(u, cost) => {
                            acc.usage = Some(u);
                            self.compactor.record_usage(&u, &self.messages);
                            self.last_context_tokens = self.compactor.context_tokens();
                            self.emit_context_usage();
                            self.emit(RpcEvent::message_update {
                                usage: acc.usage_value(),
                                assistant_message_event: Value::Object(Default::default()),
                            });
                            // Issue #87: the provider's cost rides the SAME
                            // event, so it is emitted HERE — where usage already
                            // accumulates — and NOT at a settle point. An attempt
                            // that reported a `Usage` and then died was still
                            // billed, and its money must not be forfeited by a
                            // turn that never settles. EMISSION POINT UNCHANGED
                            // by Fulcrum T2; what changed is the VALUE: this
                            // passes the call's figure to the emitter, which adds
                            // it to the session accumulator and frames the new
                            // TOTAL. `None` adds nothing and emits nothing.
                            self.emit_cost_update(cost);
                        }
                        ProviderEvent::Done(f) => acc.finish = Some(f),
                        ProviderEvent::Error(e) => mid_stream_error = Some(e),
                    }
                }
                _ = turn.cancelled() => return None,
            }
        }
        // The FABRICATION GUARD: this conversion fires ONLY when the stream
        // reported NO error event at all (`mid_stream_error.is_none()`) but
        // ended on a synthesized / bare `Done(Error)` — a stream that simply
        // stopped. It must never overwrite a real tier: `ProviderEvent::Error`
        // sets `mid_stream_error` in the match above, so a wire that reports
        // its in-stream error as a real event lands here with `Some` and this
        // retryable string is never chosen over it.
        //
        // (Finding 1) That distinction is what makes ADR 0045's
        // `context_overflow_is_never_retried` invariant reachable: an overflow
        // reported MID-STREAM has to arrive as
        // `ProviderError::ContextOverflow` — the ladder's only input is
        // `Retryable`, so a fabricated `Retryable` here would turn a
        // compact-in-the-turn recovery into 4 doomed re-issues of the identical
        // oversized bytes. The OpenAI-completions wire used to be unable to
        // deliver one (its parser had no `error` arm, so every in-stream error
        // fell through to this fabrication); with the parser arm in place all
        // three wires report mid-stream auth / overflow as a real
        // `ProviderEvent::Error` and this branch only ever sees a stream that
        // genuinely carried no error.
        if mid_stream_error.is_none() && acc.finish == Some(FinishReason::Error) {
            mid_stream_error = Some(ProviderError::Retryable(
                "the model stream ended without a finish_reason".to_string(),
            ));
        }
        mid_stream_error
    }

    /// Handles a mid-stream error: retry backoff, mid-stream overflow recovery, or settle as error.
    pub(super) async fn handle_mid_stream_error(
        &mut self,
        err: ProviderError,
        turn: &CancellationToken,
        state: &mut TurnLoopState,
        acc: &TurnAccumulator,
    ) -> TurnFlow {
        // A mid-stream `Retryable` error: a bounded re-try (the SAME
        // attempt budget — `next_retry_delay` shares the counter with
        // `call_with_retry`, so a turn never makes more than
        // `max_attempts` model calls).
        if let ProviderError::Retryable(_) = &err {
            return self
                .retry_abandoned_attempt(&err.to_string(), turn, state, acc)
                .await;
        }

        // (ADR 0045) A mid-stream overflow:
        if let ProviderError::ContextOverflow(_) = &err {
            if !state.overflow_recovered {
                state.overflow_recovered = true;
                self.close_announced_tool_calls(&acc.tool_calls);
                if !acc.text.is_empty() || !acc.thinking.is_empty() || !acc.tool_calls.is_empty() {
                    self.emit(RpcEvent::message_retracted);
                }
                self.compactor.turn_abandoned(&self.messages);
                self.turn_control.arm(CompactionReason::Overflow, true);
                return TurnFlow::Continue;
            }
        }

        eprintln!("harness: mid-stream model error: {err}");
        // Site 2 of the ten: `acc` IS in scope — this attempt announced rows
        // and is about to be abandoned without dispatching them, so its calls
        // close before the settle.
        self.settle_error(
            std::mem::take(&mut state.turn_tool_results),
            &acc.tool_calls,
        );
        TurnFlow::Return
    }

    /// (issue #64) The turn ended `Stop` having streamed NEITHER text NOR a
    /// tool call. That is not a turn: it is the signature of a model that wrote
    /// its tool call as literal `<invoke name="…">` text INTO the reasoning
    /// channel, so the API answers `content: ""` / `tool_calls: null` and the
    /// stream reads as a clean stop.
    ///
    /// The reasoning text is streamed to the UI and it IS persisted — but in the
    /// DISPLAY table, not in the transcript the model reads. `thinking_delta`
    /// becomes an `agent_thought_chunk` frame (`normalize.rs`), which
    /// `compute_display_rows` turns into a `messages.kind = 'agent-thought'`
    /// row, and this loop persists `acc.text` alone into the transcript. So the
    /// intended call is not invisible — it is in the wrong table: nothing that
    /// feeds the next model request, or a transcript reload, ever reads it (the
    /// live database holds 126 713 `agent-thought` rows, 12 of them carrying a
    /// literal `<invoke` block, one of them 11 ms before the empty assistant row
    /// of the SAME session). Pre-fix the loop also accepted this attempt,
    /// persisted the identical 33-byte row `{"role":"assistant","content":"","tool_calls":[]}`
    /// and settled as a clean stop — nothing executed, nothing reported, and the
    /// user re-prompted every single time (13 such turns in 23 519 assistant
    /// rows; ALL 13 followed by a `user` row, 6 of them ending the session).
    ///
    /// A `Retryable` condition like any other, merely detected AFTER the stream
    /// instead of during it, so the recovery is the mid-stream one
    /// ([`Self::retry_abandoned_attempt`]) — the same shared budget, the same
    /// retraction, the same `Continue`; with the budget spent, a VISIBLE
    /// failure. `thinking` deliberately does not gate the detection: non-empty
    /// reasoning IS the shape of this failure. The `eprintln` below is still
    /// mandatory — the Worker never calls `debuglog::init_from_env`, so
    /// `debuglog::log` is a silent no-op in exactly this process — but it is NOT
    /// the only trace the turn leaves: the reasoning text survives in the
    /// display table, and saying otherwise is what stops the next reader from
    /// looking there.
    pub(super) async fn handle_empty_turn(
        &mut self,
        turn: &CancellationToken,
        state: &mut TurnLoopState,
        acc: &TurnAccumulator,
    ) -> TurnFlow {
        eprintln!(
            "harness: empty model turn: `stop` with no text and no tool call \
             (the signature of a tool call emitted as reasoning text — that text is \
             persisted in the DISPLAY table as an `agent-thought` row and never in \
             the transcript, so the display is where the lost call can still be read)"
        );
        self.retry_abandoned_attempt(
            "the model returned an empty reply with no tool call",
            turn,
            state,
            acc,
        )
        .await
    }

    /// (issue #190) The turn's reply was CUT OFF by the provider's output cap
    /// (`FinishReason::Length`) — a terminal disposition, never a retry.
    ///
    /// All three wires map the cap (`map_anthropic_stop_reason`'s `"max_tokens"`,
    /// `map_finish_reason`'s `"length"`, the Responses API's
    /// `incomplete_details.reason == "max_output_tokens"`) and until this function
    /// existed the turn loop read it nowhere: the attempt fell through the #64
    /// gate (whose first conjunct demanded `Some(Stop)`) into the SUCCESS triple,
    /// so the user watched the model's half-sentence — "OK, plans good, I'm going
    /// to write it now" — settle as a completed turn, the composer unlocked, and
    /// nothing happened.
    ///
    /// Not retried, deliberately (Decision 1, ADR 0108): Anthropic documents
    /// re-issuing with a higher `max_tokens`, and that is rejected because
    /// replaying the assistant turn can duplicate a non-idempotent tool call.
    /// openai/codex made the same call (`ResponseIncomplete` →
    /// `RetryPolicy::Never`) for the same stated reason — "blindly replaying a
    /// partially completed response can duplicate tool side effects."
    ///
    /// And its tool calls are NEVER dispatched (Decision 2, see
    /// [`Self::settle_truncated`]). What is kept is the TEXT: it is real content
    /// the model produced, so it is announced, pushed and persisted like any
    /// other reply — a display that shows it while the transcript lacks it
    /// recreates the crash-time display/transcript split
    /// the DESKTOP repo's `docs/roadmap/compaction-recovery-hardening-follow-ups.md` §4
    /// records.
    /// With NO text streamed, nothing is pushed and nothing is persisted: that
    /// is precisely the 33-byte empty row issue #64 exists to prevent, and the
    /// `Length` route has no more claim on it than `Stop` ever had.
    pub(super) async fn handle_truncated_turn(
        &mut self,
        tool_results: Vec<Value>,
        acc: &TurnAccumulator,
    ) -> TurnFlow {
        // `eprintln!`, never `debuglog::log`: the Worker never calls
        // `debuglog::init_from_env`, so `log` is a SILENT NO-OP in exactly the
        // process whose verdict this is (`hostenv.rs` documents the asymmetry).
        eprintln!(
            "harness: turn truncated by the provider's output cap: finish_reason \
             `length` with {} chars of streamed text and {} announced tool call(s) \
             — terminal, not retried (ADR 0108)",
            acc.text.chars().count(),
            acc.tool_calls.len(),
        );
        // BEFORE the settle, so a peer stashes the disposition while it is still
        // listening (`RpcEvent::turn_truncated`'s contract).
        self.emit(RpcEvent::turn_truncated);
        if !acc.text.is_empty() {
            let message_value = self.assistant_message_value(acc);
            self.emit(RpcEvent::message_end {
                message: message_value,
            });
            self.messages.push(ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text(acc.text.clone()),
                tool_call_id: None,
                // NEVER `acc.tool_calls`: a truncated call is an argument list
                // the cap cut off, and replaying it to the provider as a
                // request the model finished making would invite the same call
                // back.
                tool_calls: None,
            });
            self.persist_transcript_message();
        }
        // (issue #190 follow-up, F3) Retire a retry report this turn left
        // pending, and CLOSE it honestly. `RetryPolicy::reported` is set only by
        // `note_success()` and cleared only by `next_retry_delay()`, and this
        // route returns before `note_success()` is ever reached (deliberately — a
        // truncated attempt is not a success), so a turn that retried to get here
        // would leave the report outstanding for the NEXT prompt to call a
        // success.
        //
        // THE SHAPE CHOSEN: one `auto_retry_end { success: false }` whose
        // `final_error` names the truncation — the same close
        // `retry_abandoned_attempt`'s budget-EXHAUSTED arm emits (its
        // `self.emit(RpcEvent::auto_retry_end { success: false, .. })` immediately
        // before its `settle_error`), for the same reason: a peer that opened a
        // retry card on the `auto_retry_start` must see it CLOSE, and `normalize`
        // renders the close as the "Retry failed" line. The alternative — retiring
        // the flag silently — would leave that card spinning until some later turn
        // happened to close it, which is the same class of dangling-row bug this
        // file's tool-row discipline refuses elsewhere.
        //
        // Emitted BEFORE the settle for the same ordering reason as
        // `turn_truncated` above: nothing a peer still needs arrives after
        // `agent_settled`.
        if let Some(attempt) = self.retry.retire_pending_report() {
            self.emit(RpcEvent::auto_retry_end {
                success: false,
                attempt,
                final_error: Some(
                    "the retried turn was truncated by the provider's output cap \
                     (finish_reason `length`) — terminal, not retried"
                        .to_string(),
                ),
            });
        }
        // Site 9 of the ten in `docs/features/abandoned-tool-rows.md` §2:
        // NOT vacuous. `consume_stream` has just run and this route reaches no
        // batch, so `acc.tool_calls` holds every row the cut-off attempt opened
        // at `in_progress` and will never dispatch — Decision 2 forbids the
        // dispatch, so the close is the only thing that can end those rows.
        self.settle_truncated(tool_results, &acc.tool_calls);
        // (issue #190 follow-up, F2) Re-anchor the context count, in the SAME
        // position the clean-stop path uses it: AFTER the assistant row is
        // persisted (`loop.rs:1473`, which follows its own
        // `push` + `persist_transcript_message`). This route is the fourth site
        // that persists an assistant row — Decision 3 keeps the streamed text —
        // and without the refresh the displayed/saved count still described the
        // transcript as it was BEFORE the reply landed, so the context bar and
        // the `sessions` row a reload reads both under-reported a real row until
        // some later turn happened to refresh. It sits after the settle (and so
        // after `settle_truncated`'s `turn_abandoned`) for the same reason the
        // clean stop puts it after its own transcript write: the number emitted
        // must be the one the transcript now actually has — on the no-text half
        // that is the corrected anchor `turn_abandoned` just produced, and
        // emitting the pre-correction figure would show the phantom output this
        // branch's other fix exists to remove.
        //
        // `note_context` emits only when the estimate MOVED, so the half where
        // the provider reported usage and the anchor already covers the reply
        // emits nothing (the guard in `emit.rs:274`'s doc).
        self.note_context();
        TurnFlow::Return
    }

    /// The ONE "abandon this attempt and re-run the turn" recovery, shared by
    /// the two triggers that need it: a mid-stream `Retryable` error and a turn
    /// that stopped empty. It checks the SHARED attempt budget
    /// (`next_retry_delay` shares the counter with `call_with_retry`, so a turn
    /// never makes more than `max_attempts` model calls — no second counter
    /// exists for a reason), announces `auto_retry_start`, races the backoff
    /// against the turn's cancel (finding 8 / the backoff nit — a cancelled turn
    /// must not wait out the delay, up to 8 s), closes any tool call the
    /// abandoned attempt had started, RETRACTS the partial reply when the
    /// attempt streamed anything (ADR 0048: the retry reuses the `messageId`, so
    /// without the retraction the dead attempt welds onto the retry), notes the
    /// abandon with the compactor, and re-runs the turn.
    ///
    /// Budget exhausted: `auto_retry_end { success: false }` + the error settle
    /// — the attempt is NOT replaced by a silently-empty message.
    async fn retry_abandoned_attempt(
        &mut self,
        error_message: &str,
        turn: &CancellationToken,
        state: &mut TurnLoopState,
        acc: &TurnAccumulator,
    ) -> TurnFlow {
        if let Some(delay) = self.retry.next_retry_delay() {
            self.emit(RpcEvent::auto_retry_start {
                attempt: self.retry.attempt(),
                max_attempts: self.retry.max_attempts(),
                delay_ms: delay.as_millis() as u64,
                error_message: error_message.to_string(),
            });
            // The backoff RACES the cancel (finding 8 / the
            // backoff nit — a cancelled turn must not wait out
            // the delay, up to 8 s).
            tokio::select! {
                _ = turn.cancelled() => {
                    // Site 3 of the ten — the asymmetry issue #11 is about.
                    // This arm used to settle and RETURN, skipping the close
                    // loop a few statements below: the path where the backoff
                    // COMPLETED closed this same attempt's announced rows and
                    // the path where it was CANCELLED did not. Same function,
                    // same abandoned attempt, same `acc`. `acc` is in scope
                    // here, so the calls are passed for real.
                    self.settle_cancelled(
                        std::mem::take(&mut state.turn_tool_results),
                        &acc.tool_calls,
                    );
                    return TurnFlow::Return;
                }
                _ = tokio::time::sleep(delay) => {}
            }
            self.close_announced_tool_calls(&acc.tool_calls);
            if !acc.text.is_empty() || !acc.thinking.is_empty() || !acc.tool_calls.is_empty() {
                self.emit(RpcEvent::message_retracted);
            }
            self.compactor.turn_abandoned(&self.messages);
            return TurnFlow::Continue;
        }
        self.emit(RpcEvent::auto_retry_end {
            success: false,
            attempt: self.retry.attempt(),
            final_error: Some(error_message.to_string()),
        });
        // Site 4 of the ten: the budget is spent and this attempt is
        // abandoned for good — `acc` is in scope and its announced rows are
        // never dispatched, so they close before the settle.
        self.settle_error(
            std::mem::take(&mut state.turn_tool_results),
            &acc.tool_calls,
        );
        TurnFlow::Return
    }

    /// Dispatch tool calls (concurrent subagents + sequential tools) and append tool messages.
    ///
    /// Returns `(results, refused)`, BOTH index-aligned with `tool_calls`:
    /// `refused[i]` is true exactly where `refused_before_the_model_could_act`
    /// says the engine told the model to re-send call `i` with correct
    /// arguments — the pre-dispatch `unknown_param_error` refusal (which also
    /// carries a parse `defect`), or a dispatched call the EXECUTOR refused for
    /// omitting a `required` parameter (and no advertised alias of it). The first
    /// half reads the SAME `param_errors` vector the dispatch arms below consult
    /// — it is NOT recomputed here, and a caller must not recompute it either,
    /// because two independent readings of the checker is the drift the plan's
    /// Decision 2 rejects. `refused` exists so the caller can build
    /// [`batch_signature`]: `param_errors` is a local of this function and a
    /// signature computed from a second pass would be a second source of truth.
    ///
    /// Note what `refused[i]` does NOT claim. It is not "this call produced an
    /// error result": a call that was ACCEPTED and then DENIED by the permission
    /// gate, or one that ran and FAILED, is `false` — a denied or failing tool is
    /// a different problem with a different remedy, and bounding it would end the
    /// user's prompt over a command that merely did not work. The one
    /// dispatched-call shape that DOES count is a call whose advertised
    /// `required` keys are missing (with no advertised alias carrying them),
    /// because that sentence's only possible use is "send it again, correctly".
    /// Cancellation is never a refusal, on either class (see the function above
    /// for the arm-by-arm reason).
    ///
    /// THE DENIAL HALF IS STRUCTURAL, NOT INFERRED, and that is the whole of the
    /// first exclusion: the dispatch expression yields `(result, denied)` and the
    /// two arms that CONSTRUCT a gate denial (`gate_tool` answering `false`, and
    /// `Decision::Deny`) are the only ones that set `denied: true`. Reading the
    /// result's text instead would make the guard depend on a sentence someone may
    /// reword, and a rewording would silently re-arm the false positive that ends
    /// a user's prompt over their own "no". `codemode`'s `run_nested` returns the
    /// same pair for the same reason (there it picks `denied` over `error` as the
    /// row's status).
    ///
    /// The vector is built to the exact length of `tool_calls` — one push per
    /// dispatch arm, in the same loop — and [`batch_signature`] refuses to sign a
    /// length mismatch, so the alignment is structural at both ends.
    pub(super) async fn run_tool_batch(
        &mut self,
        tool_calls: &[ToolCall],
        turn: &CancellationToken,
    ) -> (Vec<Value>, Vec<bool>) {
        let mut turn_tool_results = Vec::new();
        // (the turn-loop bound) The refusal vector, index-aligned with
        // `tool_calls`, pushed to once per iteration of the dispatch loop below
        // for the same reason `turn_tool_results` is: both halves of the pair
        // `batch_signature` consumes have to be exactly as long as the batch,
        // and a `zip` that stops early on a short vector is the silent
        // mis-alignment this shape forecloses.
        let mut refused: Vec<bool> = Vec::with_capacity(tool_calls.len());
        // (issue #93) The unknown-param check runs HERE, before the `join_all`
        // below, and NOT inside `dispatch_tool`: a `subagent` call is dispatched
        // CONCURRENTLY ahead of the sequential loop, so a check that only ran
        // inside the loop would PREVENT nothing — the wasted config-less child
        // would already exist by the time the rejection was written (the check
        // used to only ANNOUNCE that waste; it now stops it). One
        // `advertised_specs()` per batch — the SAME value `model_request` sends
        // as `tools[]` and the prompt renders into `<tools>` (ADR 0017's
        // byte-identity), so the checker cannot flag a key the model was told
        // to send.
        let specs = self.advertised_specs();
        let param_errors: Vec<Option<String>> = tool_calls
            .iter()
            .map(|tc| crate::harness::tool_specs::unknown_param_error(&specs, tc))
            .collect();
        let mut subagent_results: HashMap<usize, (ToolResult, Option<f64>)> = if turn.is_cancelled()
        {
            HashMap::new()
        } else {
            let me: &Self = self;
            let futures: Vec<_> = tool_calls
                .iter()
                .enumerate()
                .filter(|(idx, tc)| {
                    // A rejected call must never spawn a child (issue #93).
                    // These futures run BEFORE the dispatch loop below, so
                    // the rejection has to gate them HERE — a rejection that
                    // only lives in the loop would have paid for the child
                    // it then refused to report. Cancellation is not
                    // consulted here: the whole batch is already behind `if
                    // turn.is_cancelled()` above.
                    //
                    // `idx` is the call's ORIGINAL index in `tool_calls` —
                    // it comes from the `.enumerate()` BEFORE this filter
                    // and MUST stay that. The dispatch loop below keys
                    // `subagent_results.remove(&idx)` on the same original
                    // index; re-enumerating after this filter ("tidying" it
                    // into a 0-based position among the survivors) silently
                    // mis-keys every child result whenever a batch mixes
                    // rejected and accepted `subagent` calls. Do not
                    // renumber.
                    param_errors.get(*idx).is_none_or(|err| err.is_none())
                        && crate::harness::dispatch::is_enabled_subagent_call(
                            me.enabled_tools(),
                            tc,
                        )
                })
                .map(|(idx, tc)| {
                    let args = tc.arguments.clone();
                    async move { (idx, me.dispatch_subagent(&args, turn).await) }
                })
                .collect();
            futures_util::future::join_all(futures)
                .await
                .into_iter()
                .collect()
        };

        for (idx, tc) in tool_calls.iter().enumerate() {
            // THE PAIR: the result, and whether the PERMISSION GATE refused it.
            // The second half exists because a gate denial arrives as
            // `is_error: true` and is a USER decision, not a re-send
            // instruction — see `refused_before_the_model_could_act`'s first
            // exclusion. It is threaded from the two arms that CONSTRUCT the
            // denial rather than inferred from the result text, exactly as
            // `codemode`'s `run_nested` returns `(ToolResult, bool)` for the same
            // reason (its `denied` feeds the row's `denied` status, not `error`).
            let (result, denied) = if crate::harness::dispatch::is_subagent_call(&tc.name) {
                match subagent_results.remove(&idx) {
                    // A child that ran (or that the `SubagentWait` select
                    // itself cancelled) keeps its own result — unchanged.
                    //
                    // Its MONEY, however, is handled here and nowhere else.
                    // `fold_child_cost` adds the child's figure to THIS loop's
                    // accumulator and emits the parent's new total, so the
                    // parent's key has exactly ONE writer. The Supervisor used
                    // to emit a second, parent-keyed roll-up frame for every
                    // child; because a frame is a SNAPSHOT the two writers
                    // raced, and the parent's next frame — whose accumulator
                    // had never seen the child — erased the child's money
                    // (0.003 + child 0.002 + 0.006 stored as 0.009). The fold
                    // lives HERE rather than inside `dispatch_subagent` because
                    // the futures above hold a SHARED borrow of the loop; this
                    // loop is the only party that sees both its own calls and
                    // its children's outcomes, so it is the only legal owner of
                    // a total that spans both.
                    Some((result, child_cost)) => {
                        self.fold_child_cost(child_cost);
                        (result, false)
                    }
                    // CANCELLATION BEATS REJECTION: a call that never ran
                    // because the user stopped the turn must not be told to
                    // "fix the key and retry".
                    None if turn.is_cancelled() => (Self::cancelled_tool_result(), false),
                    // A rejected `subagent` was filtered out of the futures
                    // above, so it has NO entry here. This arm MUST precede the
                    // `dispatch_tool` fallback: that fallback routes a subagent
                    // call to `dispatch_subagent` (issue #93), so a rejection
                    // placed after it would dispatch the very child it rejects.
                    None if param_errors.get(idx).is_some_and(Option::is_some) => (
                        unknown_param_result(param_errors[idx].as_deref().unwrap_or_default()),
                        false,
                    ),
                    None => self.dispatch_tool(tc, turn).await,
                }
            } else if turn.is_cancelled() {
                // Cancellation beats rejection (same rule, the sequential tools).
                (Self::cancelled_tool_result(), false)
            } else if let Some(Some(err)) = param_errors.get(idx) {
                // REJECTED, not warned: no dispatch, and deliberately NOT
                // `policy_verdict` / `gate_tool` — a call that will not run
                // must never raise a permission prompt.
                (unknown_param_result(err), false)
            } else {
                match self.policy_verdict(tc) {
                    Some(Decision::Ask) => {
                        // (T03) `guard_target` is `Some` exactly for a ROW-3
                        // guard ask (no rule matched, the write lands in an
                        // agent-definition dir, the Space is untrusted) — the
                        // boundary ask stays `None` and keeps its
                        // `trust-space` option list. The value is the RESOLVED
                        // target, so the prompt can name the real directory
                        // `allow-directory` would allow.
                        let guard = self.guard_target(tc);
                        if self.gate_tool(tc, turn, guard.as_deref()).await {
                            // The flag PROPAGATES rather than being pinned to
                            // `false`: a `sudo_exec` that passes this gate and is
                            // then refused at its OWN confirm prompt is still a
                            // person's "no", and this arm is the one that
                            // dispatches it.
                            self.dispatch_tool(tc, turn).await
                        } else {
                            // THE DENIAL ARM. `gate_tool` answers `false` for a
                            // `reject` click, a `Cancelled` (a Stop, or a closed
                            // session) and any outcome that is not one of the
                            // three ALLOW ids — every one of them a user
                            // decision. `denied: true` is the whole point of this
                            // tuple: the result below satisfies class 2 of the
                            // refusal predicate on its face (`is_error`, and for a
                            // malformed call a missing `required` key), and only
                            // this flag says "the user said no" rather than "the
                            // engine asked for a re-send".
                            (
                                ToolResult {
                                    content: vec![ContentBlock::Text {
                                        text: "permission denied".to_string(),
                                    }],
                                    details: None,
                                    is_error: true,
                                },
                                true,
                            )
                        }
                    }
                    // The OTHER denial arm: a policy/rule `Deny` never prompts, so
                    // no user click is involved, but it is still the gate refusing
                    // rather than the executor — the user's own rule said no, and
                    // `deny_tool_result`'s wording is deliberately the executor's,
                    // which is exactly why `is_error` alone cannot tell the two
                    // apart. `codemode`'s sibling marks this arm `denied` too.
                    Some(Decision::Deny) => (self.deny_tool_result(tc), true),
                    // (finding 6) What reaches here: `Some(Decision::Allow)`,
                    // `None` (an ungated tool), and `Some(Decision::Confine)`
                    // — and `Confine` can ONLY be `bash`, because
                    // `decision_for`'s fourth parameter is the `PolicedTool`
                    // type and its `Confine` arm is spelled for `Shell` alone:
                    // a path tool's `Decision` is structurally incapable of
                    // being `Confine` (pinned by
                    // `path_tool_decisions_can_never_be_confine_at_the_type_level`).
                    // So dispatching IS the confined run: the `bash` executor
                    // reads `ctx.file_policy.shell` and sandboxes itself
                    // (Task 5's Landlock). Before the type, this arm silently
                    // ingested a `Confine` a mis-threaded `is_shell` boolean
                    // could produce for a path tool — an unsandboxed `read`
                    // under a tier that meant refusal; that combination is now
                    // unrepresentable at the SOURCE rather than explained here.
                    //
                    // The flag PROPAGATES. This is the arm a `sudo_exec` takes
                    // (`sudo_exec` is not a `POLICIED_TOOLS` name, so
                    // `policy_verdict` answers `None` and its confirm prompt is
                    // the only human decision in its path), so pinning `false`
                    // here would drop the refusal on the floor and re-arm the
                    // over-count this excludes.
                    _ => self.dispatch_tool(tc, turn).await,
                }
            };
            let result_value = serde_json::to_value(&result).unwrap_or(Value::Null);
            // (the turn-loop bound) THE REFUSAL, decided from the SAME
            // `param_errors` the arms above consulted (class 1 — never a second
            // `unknown_param_error` pass) plus the executor's own verdict and the
            // advertised `required` list (class 2). Computed HERE, per call,
            // inside the loop that produced the result, so the vector cannot
            // drift out of step with the batch.
            refused.push(refused_before_the_model_could_act(
                tc,
                param_errors.get(idx).and_then(Option::as_ref),
                &result,
                &specs,
                turn.is_cancelled(),
                denied,
            ));
            turn_tool_results.push(result_value.clone());
            self.emit(RpcEvent::tool_execution_end {
                tool_call_id: tc.id.clone(),
                tool_name: tc.name.clone(),
                result: result_value,
                is_error: result.is_error,
            });
            self.messages.push(ChatMessage {
                role: ChatRole::Tool,
                content: MessageContent::Blocks(result.content.clone()),
                tool_call_id: Some(tc.id.clone()),
                tool_calls: None,
            });
            self.persist_transcript_message();
        }
        self.note_context();
        // (the turn-loop bound) The refusal vector, index-aligned with
        // `tool_calls` by construction — one `push` per dispatch above, in the
        // same loop — NOT a second `unknown_param_error` pass (see this
        // function's docblock for why a recompute is the drift).
        debug_assert_eq!(
            refused.len(),
            tool_calls.len(),
            "one refusal per call in the batch"
        );
        (turn_tool_results, refused)
    }

    /// Close every tool row an ABANDONED attempt announced — the ONE close
    /// implementation behind issue #11.
    ///
    /// A `tool_call` row is opened when the model finishes NAMING a call
    /// (`consume_stream`'s `ProviderEvent::ToolCall` arm emits `toolcall_end` +
    /// `tool_execution_start`, and `normalize` renders either as a `tool_call`
    /// frame at `status: "in_progress"`). It leaves `in_progress` ONLY on a
    /// `tool_call_update` carrying `completed`/`failed`, which `normalize`
    /// derives from `tool_execution_end` ALONE (`normalize.rs`, the
    /// `RpcEvent::tool_execution_end` arm of `normalize`'s `match`) — it has
    /// no arm for `turn_end` / `agent_settled`. So an attempt that announced
    /// calls and then died without dispatching them leaves those rows spinning
    /// forever: the desktop reads a Subagent frozen on a stale tool name with a
    /// climbing duration while its tool count and cost keep rising.
    ///
    /// Three constraints are load-bearing here, each because of what breaks if
    /// this is "tidied":
    ///
    /// - **The caller passes the ordered `Vec<ToolCall>`, never
    ///   `TurnState::announced_tool_calls`.** That `HashSet` (`normalize.rs:72`)
    ///   is insert-only (no removal anywhere in the file) and it ALSO holds
    ///   compaction card ids (`compact:N`, minted in `normalize`'s
    ///   `RpcEvent::compaction_start` arm) which are closed
    ///   by `compaction_end`, not by `tool_execution_end` — closing from the set
    ///   would emit a bogus failed tool close for every compaction card the
    ///   session ever opened. The `Vec` is transcript order, which is what the
    ///   desktop reads these frames as.
    /// - **Frames only, never transcript rows.** No `ChatMessage { role: Tool }`
    ///   is pushed and nothing is appended to `turn_tool_results`: an abandoned
    ///   attempt persists NO assistant row, because the abandon path reaches
    ///   none of the `persist_transcript_message()` call sites — the user
    ///   prompt (`begin_turn`), the dispatched tool result (`run_tool_batch`),
    ///   the accepted assistant message (`handle_turn` over in `loop.rs`)
    ///   and the truncated reply ([`Self::handle_truncated_turn`], the fourth,
    ///   issue #190) — and neither `compactor.turn_abandoned` (watermark
    ///   arithmetic) nor `message_retracted` (an outbound frame to the Client)
    ///   writes the transcript. So a tool-result message here would dangle with no
    ///   preceding assistant `tool_calls` for the provider to pair against —
    ///   the orphan the provider 400s on. The fourth site does NOT weaken that:
    ///   a truncated turn persists its assistant row with `tool_calls: None`, so
    ///   even on the route that DOES write an assistant row there is nothing for
    ///   a tool result to pair against.
    /// - **The result text is `abandoned_tool_result()`, unchanged.** Several
    ///   tests pin it byte-for-byte; it is honest about the reason — the tool did
    ///   not FAIL, it was discarded.
    ///
    /// This stays a private `fn`, not `pub(super)`, and that visibility is part
    /// of the invariant: the parent module reaches a close only through the
    /// `pub(super)` settle functions, so it cannot open a close without settling.
    fn close_announced_tool_calls(&self, tool_calls: &[ToolCall]) {
        for tc in tool_calls {
            let result = Self::abandoned_tool_result();
            let result_value = serde_json::to_value(&result).unwrap_or(Value::Null);
            self.emit(RpcEvent::tool_execution_end {
                tool_call_id: tc.id.clone(),
                tool_name: tc.name.clone(),
                result: result_value,
                is_error: true,
            });
        }
    }

    /// Settle a CANCELLED turn: close what the abandoned attempt announced,
    /// then `turn_end` followed by `agent_settled`.
    ///
    /// `announced` is the abandoned attempt's `acc.tool_calls` — the calls it
    /// opened rows for and will never dispatch. It is `&[]` wherever no attempt
    /// is in scope (see the ten-site table in
    /// `docs/features/abandoned-tool-rows.md` §2); the close MUST precede
    /// `turn_end`, because `normalize` closes a row on `tool_execution_end` and
    /// on nothing that follows it.
    pub(super) fn settle_cancelled(&mut self, tool_results: Vec<Value>, announced: &[ToolCall]) {
        self.close_announced_tool_calls(announced);
        self.compactor.turn_abandoned(&self.messages);
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }

    /// Settle a FAILED turn: close what the abandoned attempt announced, then
    /// `turn_end` followed by `agent_settled`. See [`Self::settle_cancelled`]
    /// for what `announced` is and why the close comes first.
    pub(super) fn settle_error(&mut self, tool_results: Vec<Value>, announced: &[ToolCall]) {
        self.close_announced_tool_calls(announced);
        self.compactor.turn_abandoned(&self.messages);
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }

    /// Settle a TRUNCATED turn (issue #190): close what the cut-off attempt
    /// announced, then `turn_end` followed by `agent_settled`. See
    /// [`Self::settle_cancelled`] for what `announced` is and why the close comes
    /// first.
    ///
    /// **The body is `settle_error`, delegated.** It used to be `settle_error`
    /// MINUS `compactor.turn_abandoned`, and the docblock justified the omission
    /// with two claims. Both are false, and the arithmetic that says so is in
    /// `compact.rs`: `record_usage` (`:72`) writes `usage_watermark =
    /// messages.len() + 1` — the `+1` anticipates the reply the caller is about
    /// to append — and `turn_abandoned` (`:104`) early-returns on
    /// `usage_watermark <= messages.len()`. So the call does different things on
    /// the two halves of this route, and neither is what the old text claimed:
    ///
    /// * **text streamed** — [`Self::handle_truncated_turn`] pushes the reply
    ///   BEFORE settling, so `watermark == len`, the guard fires, and the call is
    ///   a NO-OP: Δ = 0 on `context_tokens`, `usage_base`, `usage_watermark` and
    ///   `pending_output`. The harm the old docblock predicted — "discount a real
    ///   message and reset the count to the bare anchor" — is unreachable. That
    ///   is what `turn_abandoned`'s own guard exists to prevent, and
    ///   `compact.rs:83-85` says in terms that the call is "safe to call on
    ///   EVERY turn exit" precisely BECAUSE it is a no-op when the append
    ///   happened (`:99-103` is the guard that buys that). This function
    ///   inverted that.
    ///   Pinned as the Δ = 0 fact by
    ///   `a_truncation_that_streamed_text_leaves_the_context_count_where_usage_put_it`.
    /// * **nothing streamed** — nothing is pushed, so `watermark == len + 1 >
    ///   len`, the guard does NOT fire, and the call ACTS. This is the half the
    ///   old text dismissed as "a no-op anyway", and acting on it is the whole
    ///   point. Leaving the call out left `context_tokens` at **150** instead of
    ///   **100** for a `Usage { input: 100, output: 50 }` + `Done(Length)` with no
    ///   text — 50 output tokens for a reply that was never persisted, with **0**
    ///   assistant rows in the transcript and the watermark still pointing at
    ///   index 1. The next turn then reads the threshold against that number: at
    ///   a window whose threshold sits between 100 and 150 it emitted **1
    ///   `compaction_start` and 3 model calls instead of 0 and 2** — a spurious
    ///   compaction, an extra summary call, the transcript rewritten. And the
    ///   inflation is self-perpetuating, because the watermark window swallows
    ///   that next prompt too (`refresh` estimates only `messages[watermark..]`).
    ///   Pinned by
    ///   `a_truncation_that_streamed_nothing_does_not_leave_phantom_output_tokens`
    ///   (the counter) and
    ///   `a_truncation_that_streamed_nothing_does_not_compact_the_next_turn`
    ///   (the spurious compaction — the pin that would have caught this).
    ///
    /// The wrapper therefore stays for its NAME and its docs, not for its body:
    /// the ten-site settle table in `docs/features/abandoned-tool-rows.md` §2
    /// names `settle_truncated` at site 9, and the `announced`-column discipline
    /// there depends on that name existing to carry a derivation. Two divergent
    /// copies of the same settle is what this function was, and the divergence
    /// was the bug.
    ///
    /// What does NOT change is the other half of this function's reason to exist.
    /// It NEVER dispatches. A truncated call's `arguments` arrived cut off
    /// mid-object, so `parse_arguments` hands back `{}` — and since issue #193
    /// that same `{}` arrives WITH a `defect`, which makes
    /// `unknown_param_error`'s HEAD ARM fire first and refuse the call there.
    ///
    /// The route this paragraph used to describe is therefore closed TWICE over,
    /// and both closures point the same way:
    /// * the checker now refuses a defective call before the permission gate and
    ///   the executor are ever consulted, naming the parse error instead of the
    ///   missing parameter (that arm deliberately fires for EVERY defective call,
    ///   disabled or unknown tool included — see rule 0 in `tool_specs`);
    /// * and this route never reaches the checker at all. The old chain —
    ///   `unknown_param_error` PASSING (it checks only EXTRANEOUS keys, and `{}`
    ///   has none), the permission gate returning `Allow` because there is no
    ///   `path` to judge, and the executor then refusing with ``write: missing
    ///   `path` or `content` parameter`` — could not run here, because
    ///   `handle_truncated_turn` settles and returns BEFORE any batch is
    ///   dispatched. So the accurate verb is not that the calls are REFUSED at
    ///   this layer, one station earlier: they are never dispatched, and
    ///   [`Self::close_announced_tool_calls`] CLOSES each one's row with
    ///   `abandoned_tool_result()`'s sentence — the same treatment this
    ///   docblock's own first paragraph states ("close what the cut-off attempt
    ///   announced") and §2's site 9 derives.
    ///
    /// Either way the misleading card never renders: a red refusal blaming the
    /// model for a parameter the output cap never let it send (566 such rows are
    /// measured in this machine's DB). Industry is unanimous that a length stop
    /// never executes — `vercel/ai`'s `isToolExecutionAllowedFinishReason`, pi's
    /// `failToolCallsFromTruncatedMessage`, opencode's `pendingCalls()` returning
    /// `[]`, codex never dispatching `partial_output`, and Anthropic's own SDK
    /// Tool Runner, which maps `max_tokens → "stop"` with the docstring "its
    /// tool_use blocks must not be executed." The gate is therefore per-turn, and
    /// the per-call refinement (cline's "a closed JSON object cannot be the
    /// prefix of a longer one") buys nothing, because
    /// [`Self::close_announced_tool_calls`] already gives each call its own
    /// precise error either way. Decision 2 / ADR 0108. The one thing the defect
    /// does NOT change is that conclusion: a truncated call closes with
    /// `abandoned_tool_result()`'s sentence and NEVER with the head arm's,
    /// because this route settles before the batch that would have run the
    /// checker ever starts — pinned by
    /// `a_truncated_turns_tool_calls_are_closed_as_abandoned_and_never_dispatched`.
    pub(super) fn settle_truncated(&mut self, tool_results: Vec<Value>, announced: &[ToolCall]) {
        // Identical to an errored exit — INCLUDING the `turn_abandoned` this used
        // to skip. See the docblock: skipping it was measured to inflate the
        // count by the phantom reply's output and to buy a spurious compaction on
        // the NEXT turn.
        self.settle_error(tool_results, announced);
    }
}

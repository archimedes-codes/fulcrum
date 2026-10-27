use std::collections::HashMap;

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::emit::TurnAccumulator;
use super::AgentLoop;
use super::CompactionRan;
use super::CompactionTrigger;
use crate::agent::events::RpcEvent;
use crate::agent::harness::provider::{
    ChatMessage, ChatRole, FinishReason, MessageContent, ProviderError, ProviderEvent, ToolCall,
};
use crate::agent::policy::Decision;
use crate::agent::tools::{ContentBlock, ImageRef, ToolResult};

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
}

impl AgentLoop {
    /// Prepare the transcript and bookkeeping for a new turn.
    pub(super) fn begin_turn(&mut self, text: &str, images: &[ImageRef]) {
        // A queued compaction belongs to the turn that queued it, and this turn
        // has queued none: drop whatever a PREVIOUS turn left behind (see the
        // field — a Stop racing an overflow recovery exits BEFORE the trigger is
        // consumed). Without this the fresh prompt turn inherits the dead turn's
        // `overflow` trigger, compacts for a reason it never hit, and — `run_compaction`
        // having shrunk nothing — settles as an error without a model call.
        self.force_compact = None;
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
        let trigger = self.force_compact.take();
        let due = trigger.is_some() || self.compactor.should_compact();
        if due {
            let t = trigger.unwrap_or(CompactionTrigger {
                reason: "threshold",
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
            if t.reason == "overflow" && ran == CompactionRan::Skipped {
                self.settle_error(std::mem::take(turn_tool_results));
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
                            self.emit(RpcEvent::message_update {
                                usage: acc.usage_value(),
                                assistant_message_event: json!({
                                    "type": "toolcall_end",
                                    "toolCall": display_tc,
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
                            // Issue #87: the provider's per-call cost rides the
                            // SAME event, so it is emitted HERE — where usage
                            // already accumulates — and NOT at a settle point.
                            // An attempt that reported a `Usage` and then died
                            // was still billed, and its money must not be
                            // forfeited by a turn that never settles.
                            // `None` emits nothing (see `emit_cost_update`).
                            self.emit_cost_update(cost);
                        }
                        ProviderEvent::Done(f) => acc.finish = Some(f),
                        ProviderEvent::Error(e) => mid_stream_error = Some(e),
                    }
                }
                _ = turn.cancelled() => return None,
            }
        }
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
                for tc in &acc.tool_calls {
                    let result = Self::abandoned_tool_result();
                    let result_value = serde_json::to_value(&result).unwrap_or(Value::Null);
                    self.emit(RpcEvent::tool_execution_end {
                        tool_call_id: tc.id.clone(),
                        tool_name: tc.name.clone(),
                        result: result_value,
                        is_error: true,
                    });
                }
                if !acc.text.is_empty() || !acc.thinking.is_empty() || !acc.tool_calls.is_empty() {
                    self.emit(RpcEvent::message_retracted);
                }
                self.compactor.turn_abandoned(&self.messages);
                self.force_compact = Some(CompactionTrigger {
                    reason: "overflow",
                    will_retry: true,
                });
                return TurnFlow::Continue;
            }
        }

        eprintln!("harness: mid-stream model error: {err}");
        self.settle_error(std::mem::take(&mut state.turn_tool_results));
        TurnFlow::Return
    }

    /// (issue #64) The turn ended `Stop` having streamed NEITHER text NOR a
    /// tool call. That is not a turn: it is the signature of a model that wrote
    /// its tool call as literal `<invoke name="…">` text INTO the reasoning
    /// channel, so the API answers `content: ""` / `tool_calls: null` and the
    /// stream reads as a clean stop.
    ///
    /// The reasoning text is streamed to the UI but is NEVER persisted (the loop
    /// persists `acc.text` alone), so what the model actually asked for is
    /// invisible downstream: pre-fix the loop accepted this attempt, persisted
    /// the identical 33-byte row `{"role":"assistant","content":"","tool_calls":[]}`
    /// and settled as a clean stop — nothing executed, nothing reported, and the
    /// user re-prompted every single time (13 such turns in 23 519 assistant
    /// rows; ALL 13 followed by a `user` row, 6 of them ending the session).
    ///
    /// A `Retryable` condition like any other, merely detected AFTER the stream
    /// instead of during it, so the recovery is the mid-stream one
    /// ([`Self::retry_abandoned_attempt`]) — the same shared budget, the same
    /// retraction, the same `Continue`; with the budget spent, a VISIBLE
    /// failure. `thinking` deliberately does not gate the detection: non-empty
    /// reasoning IS the shape of this failure, and it is exactly what the DB
    /// cannot show, which is why the `eprintln` below is mandatory — it is the
    /// only trace this turn ever leaves behind.
    pub(super) async fn handle_empty_turn(
        &mut self,
        turn: &CancellationToken,
        state: &mut TurnLoopState,
        acc: &TurnAccumulator,
    ) -> TurnFlow {
        eprintln!(
            "harness: empty model turn: `stop` with no text and no tool call \
             (the signature of a tool call emitted as reasoning text — that text is \
             never persisted, so this line is the only trace of the lost call)"
        );
        self.retry_abandoned_attempt(
            "the model returned an empty reply with no tool call",
            turn,
            state,
            acc,
        )
        .await
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
                    self.settle_cancelled(std::mem::take(&mut state.turn_tool_results));
                    return TurnFlow::Return;
                }
                _ = tokio::time::sleep(delay) => {}
            }
            for tc in &acc.tool_calls {
                let result = Self::abandoned_tool_result();
                let result_value = serde_json::to_value(&result).unwrap_or(Value::Null);
                self.emit(RpcEvent::tool_execution_end {
                    tool_call_id: tc.id.clone(),
                    tool_name: tc.name.clone(),
                    result: result_value,
                    is_error: true,
                });
            }
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
        self.settle_error(std::mem::take(&mut state.turn_tool_results));
        TurnFlow::Return
    }

    /// Dispatch tool calls (concurrent subagents + sequential tools) and append tool messages.
    pub(super) async fn run_tool_batch(
        &mut self,
        tool_calls: &[ToolCall],
        turn: &CancellationToken,
    ) -> Vec<Value> {
        let mut turn_tool_results = Vec::new();
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
            .map(|tc| crate::agent::harness::tool_specs::unknown_param_error(&specs, tc))
            .collect();
        let mut subagent_results: HashMap<usize, ToolResult> = if turn.is_cancelled() {
            HashMap::new()
        } else {
            let me: &Self = self;
            let futures: Vec<_> = tool_calls
                .iter()
                .enumerate()
                .filter(|(idx, tc)| {
                    // A rejected call must never spawn a child (issue #93).
                    // These futures run BEFORE the dispatch loop below, so the
                    // rejection has to gate them HERE — a rejection that only
                    // lives in the loop would have paid for the child it then
                    // refused to report. Cancellation is not consulted here:
                    // the whole batch is already behind `if turn.is_cancelled()`
                    // above.
                    //
                    // `idx` is the call's ORIGINAL index in `tool_calls` — it
                    // comes from the `.enumerate()` BEFORE this filter and MUST
                    // stay that. The dispatch loop below keys
                    // `subagent_results.remove(&idx)` on the same original
                    // index; re-enumerating after this filter ("tidying" it
                    // into a 0-based position among the survivors) silently
                    // mis-keys every child result whenever a batch mixes
                    // rejected and accepted `subagent` calls. Do not renumber.
                    param_errors.get(*idx).is_none_or(|err| err.is_none())
                        && crate::agent::harness::dispatch::is_enabled_subagent_call(
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
            let result = if crate::agent::harness::dispatch::is_subagent_call(&tc.name) {
                match subagent_results.remove(&idx) {
                    // A child that ran (or that the `SubagentWait` select
                    // itself cancelled) keeps its own result — unchanged.
                    Some(r) => r,
                    // CANCELLATION BEATS REJECTION: a call that never ran
                    // because the user stopped the turn must not be told to
                    // "fix the key and retry".
                    None if turn.is_cancelled() => Self::cancelled_tool_result(),
                    // A rejected `subagent` was filtered out of the futures
                    // above, so it has NO entry here. This arm MUST precede the
                    // `dispatch_tool` fallback: that fallback routes a subagent
                    // call to `dispatch_subagent` (issue #93), so a rejection
                    // placed after it would dispatch the very child it rejects.
                    None if param_errors.get(idx).is_some_and(Option::is_some) => {
                        unknown_param_result(param_errors[idx].as_deref().unwrap_or_default())
                    }
                    None => self.dispatch_tool(tc, turn).await,
                }
            } else if turn.is_cancelled() {
                // Cancellation beats rejection (same rule, the sequential tools).
                Self::cancelled_tool_result()
            } else if let Some(Some(err)) = param_errors.get(idx) {
                // REJECTED, not warned: no dispatch, and deliberately NOT
                // `policy_verdict` / `gate_tool` — a call that will not run
                // must never raise a permission prompt.
                unknown_param_result(err)
            } else {
                match self.policy_verdict(tc) {
                    Some(Decision::Ask) => {
                        if self.gate_tool(tc, turn).await {
                            self.dispatch_tool(tc, turn).await
                        } else {
                            ToolResult {
                                content: vec![ContentBlock::Text {
                                    text: "permission denied".to_string(),
                                }],
                                details: None,
                                is_error: true,
                            }
                        }
                    }
                    Some(Decision::Deny) => self.deny_tool_result(tc),
                    _ => self.dispatch_tool(tc, turn).await,
                }
            };
            let result_value = serde_json::to_value(&result).unwrap_or(Value::Null);
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
        turn_tool_results
    }

    /// Settle a CANCELLED turn: `turn_end` followed by `agent_settled`.
    pub(super) fn settle_cancelled(&mut self, tool_results: Vec<Value>) {
        self.compactor.turn_abandoned(&self.messages);
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }

    /// Settle a FAILED turn: `turn_end` followed by `agent_settled`.
    pub(super) fn settle_error(&mut self, tool_results: Vec<Value>) {
        self.compactor.turn_abandoned(&self.messages);
        self.emit(RpcEvent::turn_end {
            message: Value::Null,
            tool_results,
        });
        self.emit(RpcEvent::agent_settled);
    }
}

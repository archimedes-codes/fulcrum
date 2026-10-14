//! The context compactor (native-agent-harness Task 6): track
//! `context_tokens` — the context as a SNAPSHOT: the last `Usage` the
//! provider reported (a response's `input + output` IS the whole context
//! the model was shown, plus its own reply) plus a local estimate of the
//! `messages` appended since — and signal when the context exceeds
//! `context_window - reserve_tokens`. The `reserveTokens` /
//! `keepRecentTokens` thresholds come from the `ModelCatalog`'s
//! `CompactionConfig`, which is currently ALWAYS the defaults (`16384` /
//! `20000`): `Settings` has no `compaction` field, so `merge_catalog`
//! carries the base catalog's own config and there is no settings surface
//! to override it from (yet). The keep-recent BUDGET the compaction actually
//! spends is that `keepRecentTokens` capped by what the model's own window can
//! afford — see [`Compactor::keep_budget`]. The snapshot is RE-ANCHORED by
//! `AgentLoop::note_context` wherever the transcript grows, and RETRACTED
//! by [`Compactor::turn_abandoned`] when a turn — or a retried model call
//! within one — ends without the assistant reply `record_usage` had
//! assumed. The summary model call itself is
//! orchestrated by the `AgentLoop` (it owns the `Provider`); this type
//! owns the thresholds + the message split.

use crate::agent::harness::catalog::CompactionConfig;
use crate::agent::harness::provider::{ChatMessage, ChatRole, MessageContent, Usage};
use crate::agent::tools::ContentBlock;

/// The context compactor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compactor {
    config: CompactionConfig,
    context_window: u32,
    context_tokens: u64,
    /// The context as the provider last reported it (`input + output`).
    usage_base: u64,
    /// How many messages that report already accounts for.
    usage_watermark: usize,
    /// The `output_tokens` of the reply the last `record_usage` assumed the
    /// caller would append. Kept so a turn that ENDS without appending it (a
    /// cancel / failure never persists a partial reply) can take it back out
    /// of the anchor — see [`Compactor::turn_abandoned`], which zeroes it in
    /// the same breath so a retracted reply never lingers as if it were still
    /// owed.
    pending_output: u32,
}

impl Compactor {
    pub fn new(config: CompactionConfig, context_window: u32) -> Self {
        Self {
            config,
            context_window,
            context_tokens: 0,
            usage_base: 0,
            usage_watermark: 0,
            pending_output: 0,
        }
    }

    pub fn config(&self) -> CompactionConfig {
        self.config
    }

    /// The tracked context size (tokens).
    pub fn context_tokens(&self) -> u64 {
        self.context_tokens
    }

    /// Record the provider's usage for the model call that produced the
    /// assistant message the caller is about to append. `input + output`
    /// already IS the whole context — the prompt (every message currently in
    /// the transcript) plus that assistant message's own output — so it
    /// REPLACES the count, it is never added to it (the accumulation was the
    /// bug: a re-sent prompt charged once per call). The watermark sits past
    /// that assistant message, so the trailing estimate starts after it.
    pub fn record_usage(&mut self, u: &Usage, messages: &[ChatMessage]) {
        self.usage_base = u64::from(u.input_tokens) + u64::from(u.output_tokens);
        self.usage_watermark = messages.len() + 1;
        self.pending_output = u.output_tokens;
        self.context_tokens = self.usage_base;
    }

    /// The turn ended WITHOUT appending the assistant message the last
    /// `record_usage` assumed: pull the watermark back to what the transcript
    /// actually holds, and drop the discarded reply's output from the anchor
    /// (the provider never produced it, so it is not part of the context).
    /// A no-op when the append DID happen — the transcript reached the
    /// watermark — so it is safe to call on EVERY turn exit AND on the retry
    /// loop-back.
    ///
    /// Why it is needed: the watermark `record_usage` writes is
    /// `messages.len() + 1`, i.e. "the reply lands at index `len`" — an
    /// assumption about the FUTURE. Every cancelled or failed turn-exit path
    /// breaks it (a partial reply is deliberately not persisted), and so does
    /// the INTRA-TURN mid-stream retry, which discards the partial reply and
    /// loops back to `should_compact()` without ever exiting the turn — the one
    /// path the exits cannot cover. Without this reconcile the next turn's
    /// prompt (or, for the retry, this turn's very next threshold check) lands
    /// INSIDE the watermark window and is never estimated: the snapshot swallows
    /// a whole prompt — the exact lag `a846abb` was added to prevent — while the
    /// anchor still carries output the model never generated.
    ///
    /// The guard is what makes it unconditional-callable: `watermark >
    /// len` is precisely "the assumed index is past the end of the
    /// transcript". Reconciling when the reply IS in the transcript would
    /// discount a real message and (worse) reset the count to the bare anchor,
    /// dropping the tool batch the last `refresh` had already measured.
    pub fn turn_abandoned(&mut self, messages: &[ChatMessage]) {
        if self.usage_watermark <= messages.len() {
            return;
        }
        self.usage_base = self
            .usage_base
            .saturating_sub(u64::from(self.pending_output));
        self.usage_watermark = messages.len();
        // The reply this remembered is now definitively discarded, so it is
        // pending for NOBODY: zeroing it here makes the field's invariant local
        // to this method (after a retraction there is nothing left to retract)
        // rather than something a reader has to prove unreachable across
        // `record_usage`, `reestimate` and the guard above.
        self.pending_output = 0;
        self.context_tokens = self.usage_base;
    }

    /// Compaction is due: `context_tokens > context_window -
    /// reserve_tokens` (a disabled config is never due).
    pub fn should_compact(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        let threshold =
            u64::from(self.context_window).saturating_sub(u64::from(self.config.reserve_tokens));
        self.context_tokens > threshold
    }

    /// The keep-recent budget THIS model's window can afford: the configured
    /// `keep_recent_tokens` CAPPED by `window − reserve − headroom`. The catalog
    /// budget is catalog-wide and the window is per-model, so an uncapped budget
    /// can cover the whole transcript — `older` comes back empty, the compaction
    /// is a no-op, and `should_compact()` stays true forever.
    ///
    /// No floor, deliberately: `split_for_compaction` with budget 0 keeps exactly
    /// the last message (it breaks before spending anything, then snaps back off a
    /// `tool` message), which is the shrinking a tiny window needs. A floor would
    /// also override the small budgets the tests use to force a real compaction.
    pub fn keep_budget(&self) -> u32 {
        const HEADROOM_CAP: u32 = 4_096;
        let headroom = self.config.reserve_tokens.min(HEADROOM_CAP);
        let affordable = u64::from(self.context_window)
            .saturating_sub(u64::from(self.config.reserve_tokens))
            .saturating_sub(u64::from(headroom));
        u64::from(self.config.keep_recent_tokens).min(affordable) as u32
    }

    /// The summary's OWN output cap (ADR 0047): `min(floor(0.8 ×
    /// reserve_tokens), 8_192)`. The summary is a model CALL, and a call needs
    /// an output budget it can afford — pi's rule, same shape. The harness's
    /// own model calls stay uncapped: their output is the point.
    pub fn summary_output_cap(&self) -> u32 {
        const SUMMARY_OUTPUT_CAP: u32 = 8_192;
        let reserve = u64::from(self.config.reserve_tokens);
        (reserve * 8 / 10).min(u64::from(SUMMARY_OUTPUT_CAP)) as u32
    }

    /// The Summary input budget (ADR 0047): what the summary call may CARRY —
    /// the window minus its own output cap minus the same headroom
    /// [`Compactor::keep_budget`] charges a small window. A summary built from
    /// the WHOLE summarized range is a call that overflows on its own way in,
    /// and the mechanism that fixes an overflow must not need fixing itself.
    ///
    /// Zero is a real answer (a window already spent on its reserve), and the
    /// caller reads it as "no summary is possible", not as "unbounded".
    pub fn summary_input_budget(&self) -> u32 {
        const SUMMARY_HEADROOM_CAP: u32 = 4_096;
        let headroom = self.config.reserve_tokens.min(SUMMARY_HEADROOM_CAP);
        u64::from(self.context_window)
            .saturating_sub(u64::from(self.summary_output_cap()))
            .saturating_sub(u64::from(headroom)) as u32
    }

    /// Re-estimate the context from the (post-compaction) messages — the
    /// RESET path (after a compaction the transcript genuinely shrank, and
    /// a resume has no provider anchor to keep, so the estimate
    /// REPLACES the count, even downward — and the usage anchor is
    /// cleared too: a compacted / resumed transcript is a new baseline).
    pub fn reestimate(&mut self, messages: &[ChatMessage]) {
        self.usage_base = 0;
        self.usage_watermark = 0;
        self.pending_output = 0;
        self.context_tokens = estimate_context(messages);
    }

    /// The context NOW: the last reported usage plus the local estimate of
    /// every message appended since (pi's `estimateContextTokens`: a usage
    /// anchor + its trailing tail). Called after each message is appended, so
    /// the threshold and the display read ONE number. A usage-less session has
    /// a `0` anchor and the whole transcript is trailing, so this degrades to
    /// the plain estimate — which is why the old floor-raiser is gone: the
    /// anchor only moves when the provider really reports, so the metric can
    /// no longer be overwritten downward by an estimate.
    pub fn refresh(&mut self, messages: &[ChatMessage]) -> u64 {
        let trailing = self.usage_watermark.min(messages.len());
        self.context_tokens = self.usage_base + estimate_context(&messages[trailing..]);
        self.context_tokens
    }
}

/// The transcript's local token estimate (the sum over the messages).
///
/// `pub(crate)` so the `AgentLoop`'s tests can reason about the same number
/// the threshold uses (one source for both the trigger and the frame).
pub(crate) fn estimate_context(messages: &[ChatMessage]) -> u64 {
    messages
        .iter()
        .map(estimate_message_tokens)
        .map(u64::from)
        .sum()
}

/// A local estimate: the text's `chars / 4` + a flat cost per tool call
/// (+1 per message — the role/delimiter overhead).
fn estimate_message_tokens(m: &ChatMessage) -> u32 {
    let text = match &m.content {
        MessageContent::Text(t) => t.chars().count(),
        MessageContent::Blocks(blocks) => {
            blocks
                .iter()
                .map(|b| match b {
                    ContentBlock::Text { text } => text.chars().count(),
                    // A base64 image is a ~1000-token vision chunk.
                    ContentBlock::Image { .. } => 1000,
                })
                .sum()
        }
    };
    let tools = m.tool_calls.as_ref().map(|c| c.len() * 20).unwrap_or(0);
    (text / 4) as u32 + tools as u32 + 1
}

/// What one message actually COSTS the summary call (ADR 0047): the shared
/// [`estimate_message_tokens`] PLUS the serialized tool-call `arguments`.
///
/// The two estimates differ ON PURPOSE. `estimate_message_tokens` backs the
/// context COUNT, which is anchored by the provider's own reported usage
/// (ADR 0028): a call's arguments are already inside the request the provider
/// just charged for, so the flat per-call charge is a rounding detail. The
/// summary budget has NO such anchor — it is the only thing standing between
/// the call and the window compaction is recovering, and [`summarize`] sends
/// the summarized messages verbatim, `arguments` included. A file-writing call
/// costs the shared estimate ~20 tokens and the wire thousands, so measuring
/// the budget with it green-lights the exact overflow ADR 0047 exists to end.
fn estimate_summary_payload(m: &ChatMessage) -> u32 {
    let arguments = m
        .tool_calls
        .as_ref()
        .map(|calls| {
            calls
                .iter()
                .map(|c| c.arguments.to_string().len())
                .sum::<usize>()
        })
        .unwrap_or(0);
    // The per-call flat cost in `estimate_message_tokens` stays (it covers the
    // call's id/name framing); the arguments are ADDED to it.
    estimate_message_tokens(m).saturating_add((arguments / 4) as u32)
}

/// Split the transcript for compaction: keep the most recent
/// `keep_recent_tokens` worth of messages (ALWAYS at least the last
/// message — a zero budget must not drop the latest user message),
/// and return `(older, recent)`. The kept tail NEVER starts on a `tool`
/// message (finding 5: a `tool` message whose `tool_call_id` has no
/// assistant `tool_calls` parent 400s on conformant APIs — the
/// boundary snaps back so the assistant `tool_calls` message is kept
/// with its results).
pub fn split_for_compaction(
    messages: &[ChatMessage],
    keep_recent_tokens: u32,
) -> (Vec<ChatMessage>, Vec<ChatMessage>) {
    if messages.len() <= 1 {
        return (Vec::new(), messages.to_vec());
    }
    // `keep_from` starts at the LAST message (the keep-at-least-last
    // rule) and walks back while the budget allows.
    let mut keep_from = messages.len() - 1;
    let mut acc = 0u32;
    for i in (0..messages.len()).rev() {
        if acc >= keep_recent_tokens {
            break;
        }
        acc += estimate_message_tokens(&messages[i]);
        keep_from = i;
    }
    // The kept tail must NOT start on a `tool` message (finding 5): snap
    // the boundary back so the assistant `tool_calls` parent is kept
    // with its `tool` results (an orphaned `tool` message 400s on
    // conformant APIs). A transcript of ALL `tool` messages degrades to
    // keeping everything (an empty `older` — `run_compaction` skips the
    // summary, so nothing is lost).
    while keep_from > 0 && matches!(messages[keep_from].role, ChatRole::Tool) {
        keep_from -= 1;
    }
    (
        messages[..keep_from].to_vec(),
        messages[keep_from..].to_vec(),
    )
}

/// Trim the summarized range to the Summary input budget (ADR 0047): keep
/// the NEWEST slice that fits — the recent end is what a summary must not
/// lose — and report how many messages were dropped, which the harness
/// writes into the summary it stores.
///
/// The budget is measured against [`estimate_summary_payload`], i.e. what the
/// call actually carries, NOT the context-count estimate: a summarized tool
/// call ships its full `arguments`, and undercounting them would let the very
/// call that caused the overflow through the budget meant to contain it.
///
/// Like [`split_for_compaction`] the kept slice never STARTS on a `tool`
/// message, but here the boundary snaps FORWARD (an orphaned `tool` result
/// 400s on conformant APIs) and the snapped-past messages count as dropped.
/// The result is always a SUFFIX of `older`, so "the oldest N" is literally
/// true of the count. An EMPTY slice means the budget fits nothing: there is
/// no honest summary to ask for and the caller must not send a doomed call.
pub fn trim_for_summary_budget(older: &[ChatMessage], budget: u32) -> (Vec<ChatMessage>, usize) {
    let mut keep_from = older.len();
    let mut acc = 0u32;
    while keep_from > 0 {
        let next = acc.saturating_add(estimate_summary_payload(&older[keep_from - 1]));
        if next > budget {
            break;
        }
        acc = next;
        keep_from -= 1;
    }
    while keep_from < older.len() && matches!(older[keep_from].role, ChatRole::Tool) {
        keep_from += 1;
    }
    (older[keep_from..].to_vec(), keep_from)
}

// ---------------------------------------------------------------------------
// The compaction orchestration (moved from `loop.rs` — free functions of the
// values they need, so this module never depends on `AgentLoop`; `loop.rs`
// keeps a thin wrapper that supplies its fields and does the `RpcEvent`
// emission, which needs the loop's `normalize` / display-persistence state).
// ---------------------------------------------------------------------------

use std::sync::Arc;

use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::agent::harness::catalog::Model;
use crate::agent::harness::provider::{
    FinishReason, ModelOptions, ModelRequest, Provider, ProviderError, ProviderEvent,
};
use crate::agent::harness::store::{role_str, Store};

/// The compaction SUMMARY's prefix (moved from `loop.rs` alongside
/// `run_compaction`, which prepends it to the summarized text): the ONE
/// place the prefix lives (the summary construction +
/// `is_compaction_summary` share it). A summary is a `System` message
/// carrying this prefix — the only reliable way to tell it from a REAL
/// system prompt (a summary is CONVERSATION state, not prompt state: it
/// must be foldable by the next compaction; only a non-summary leading
/// `System` message is prompt state).
pub(crate) const SUMMARY_PREFIX: &str = "Summary of previous conversation:";

/// A message is a compaction SUMMARY (a `System` message with the
/// harness-generated `SUMMARY_PREFIX` text) — conversation state, never
/// prompt state.
pub(crate) fn is_compaction_summary(m: &ChatMessage) -> bool {
    matches!(&m.content, MessageContent::Text(t) if t.starts_with(SUMMARY_PREFIX))
}

/// The values the compaction reads (the former `AgentLoop` fields — passed
/// explicitly so `compact.rs` stays free of `AgentLoop`, the same shape as
/// dispatch.rs's `SubagentParent`).
pub(crate) struct CompactionCtx<'a> {
    pub session_id: &'a str,
    pub model: &'a Model,
    pub provider: &'a dyn Provider,
    pub store: &'a Arc<dyn Store>,
    /// The keep-recent budget. Production passes [`Compactor::keep_budget`]
    /// (the configured budget capped by what THIS model's window can afford);
    /// a caller may pass an explicit one — the seam the tests use to force a
    /// real compaction.
    pub keep_recent_tokens: u32,
    /// The caller's EXTRA summary instructions (`/compact <args>` — the user's
    /// own words on what the summary must keep). `None` (the automatic paths:
    /// the threshold check, an overflow recovery) and a blank string both mean
    /// "no extra instructions" and leave the summary prompt untouched.
    pub instructions: Option<String>,
    /// The session's compactor — RE-ESTIMATED on the compacted transcript
    /// (the post-compaction BASELINE: the re-estimated count of what the
    /// context now is REPLACES the tracked total, and the usage ANCHOR is
    /// CLEARED — the provider's last report covers a transcript that no
    /// longer exists).
    pub compactor: &'a mut Compactor,
}

/// The outcome of the compaction (what happened — the caller does the
/// `compaction_start` / `compaction_end` `RpcEvent` emission and the
/// `context_usage_update` frame, both of which need the loop's emission
/// pipeline).
pub(crate) enum CompactionOutcome {
    /// Nothing to summarize (`older` was empty): the transcript is
    /// UNCHANGED and the caller emits a non-aborted `compaction_end`.
    Skipped,
    /// The summary succeeded: the caller re-emits its context usage from
    /// `context_tokens` (the post-compaction re-estimate).
    Compacted { context_tokens: u64 },
    /// The summary failed: the transcript is UNCHANGED (a failed
    /// compaction is never a lost context).
    Failed { error: String },
}

/// The compaction: the OLDER messages (the most recent
/// `keep_recent_tokens` kept) are summarized and replaced by the summary,
/// the transcript REWRITTEN (the old rows are replaced). The leading
/// system message is prompt state, not conversation, so it is excluded
/// from the compaction target and re-prepended to the compacted
/// transcript.
pub(crate) async fn run_compaction(
    ctx: &mut CompactionCtx<'_>,
    messages: &mut Vec<ChatMessage>,
    turn: &CancellationToken,
) -> CompactionOutcome {
    // The leading system message (if any) is prompt state, not
    // conversation (pi's compaction: "System messages are prompt
    // state, not conversation; the compaction entry carries their
    // replay"): EXCLUDE it from the compaction target and RE-PREPEND
    // it to the compacted transcript. A compaction SUMMARY is NOT
    // prompt state — it is CONVERSATION state (harness-generated,
    // the `SUMMARY_PREFIX` text): it must be foldable, so the next
    // compaction compacts it into the new summary (a summary at
    // index 0 — a legacy pre-ADR-0017 resume / a child with no
    // system message — is NOT a leading system message).
    let system_head = messages
        .first()
        .filter(|m| matches!(m.role, ChatRole::System) && !is_compaction_summary(m))
        .cloned();
    let compactable: &[ChatMessage] = match &system_head {
        Some(_) => &messages[1..],
        None => &messages[..],
    };
    let (older, recent) = split_for_compaction(compactable, ctx.keep_recent_tokens);
    if older.is_empty() {
        return CompactionOutcome::Skipped;
    }
    // (ADR 0047) The summary's own call is bounded: the summarized range is
    // trimmed to the budget this window affords, and the trim is RECORDED —
    // an unmarked summary would be a fabricated memory.
    let budget = ctx.compactor.summary_input_budget();
    let (summarized, dropped) = trim_for_summary_budget(&older, budget);
    if summarized.is_empty() {
        // The budget fits nothing, so there is no honest summary to ask for
        // and NO call is worth sending. `Skipped` is the honest outcome: the
        // transcript is unchanged and the caller says so. (For a window this
        // small this repeats every turn — no request is sent, so it costs the
        // provider nothing.)
        return CompactionOutcome::Skipped;
    }
    match summarize(ctx, &summarized, dropped, turn).await {
        Ok(summary) => {
            // The record of the trim travels IN the summary (ADR 0047): the
            // harness knows the exact count, so the stored row states it —
            // and so does the model, because this same message is replayed.
            let record = if dropped > 0 {
                format!(
                    "\n\n[Note: this summary is not complete. The oldest \
                     {dropped} message(s) of the summarized range were dropped \
                     to fit the summary's own context window.]"
                )
            } else {
                String::new()
            };
            let summary_msg = ChatMessage {
                role: ChatRole::System,
                content: MessageContent::Text(format!("{SUMMARY_PREFIX}\n{summary}{record}")),
                tool_call_id: None,
                tool_calls: None,
            };
            let mut compacted =
                Vec::with_capacity(1 + usize::from(system_head.is_some()) + recent.len());
            if let Some(s) = system_head {
                compacted.push(s);
            }
            compacted.push(summary_msg);
            compacted.extend(recent);
            *messages = compacted;
            ctx.compactor.reestimate(messages);
            let context_tokens = ctx.compactor.context_tokens();
            // Rewrite the transcript (the old rows are replaced —
            // a fresh `seq` run) ATOMICALLY: a single `Db`
            // transaction (clear + reinsert), so a crash
            // mid-rewrite never loses the transcript.
            let rows: Vec<(u64, String, String)> = messages
                .iter()
                .enumerate()
                .map(|(seq, m)| {
                    (
                        seq as u64,
                        role_str(m.role).to_string(),
                        serde_json::to_string(m).unwrap_or_default(),
                    )
                })
                .collect();
            if let Err(e) = ctx.store.replace_messages(ctx.session_id, &rows) {
                eprintln!("harness: transcript rewrite failed: {e}");
            }
            CompactionOutcome::Compacted { context_tokens }
        }
        Err(e) => {
            // The summary failed: the transcript is UNCHANGED (the
            // next model call runs on the full context — a failed
            // compaction is never a lost context).
            eprintln!("harness: compaction failed: {e}");
            CompactionOutcome::Failed {
                error: e.to_string(),
            }
        }
    }
}

/// The summary's system prompt: the base instruction, plus ONE line carrying
/// the caller's extra instructions when there are any (`/compact <args>` —
/// the user's own words on what the summary must keep). The base text is
/// unchanged either way (pinned by the `summarize_*` prompt tests): the
/// automatic paths (the threshold check, an overflow recovery) pass `None`,
/// and args that are only whitespace are no instructions at all.
///
/// `dropped` is the trim's count (ADR 0047): only a range that LOST messages
/// to the Summary input budget gains the extra "this is not the whole story"
/// line, so `dropped == 0` leaves the prompt byte-identical.
fn summary_system_prompt(instructions: Option<&str>, dropped: usize) -> String {
    let base = "Summarize these messages concisely, preserving decisions, \
                file paths, and open tasks. Respond with the summary only."
        .to_string();
    let mut prompt = match instructions {
        Some(s) if !s.trim().is_empty() => {
            format!("{base}\nAdditional instructions for this summary: {s}")
        }
        _ => base,
    };
    // (ADR 0047) A trimmed range is not the whole story, and a summary that
    // does not know it is incomplete writes a summary that reads as complete.
    if dropped > 0 {
        prompt.push_str(&format!(
            "\nThe oldest {dropped} message(s) of this range were dropped to fit \
             this request. Summarize what is here, and say in the summary that it \
             is not complete."
        ));
    }
    prompt
}

/// The summary model call (a "summarize these messages" system prompt
/// over the older messages — NO tools; the stream is consumed
/// silently — the summary is internal, like pi's). The call + stream
/// race the turn's cancel (a cancelled turn must not wait out the
/// summary — the compaction is aborted; the turn settles at the top
/// of the loop). A stream that ends with `Done(Error)` (the provider
/// synthesizes it when the stream ends without a `finish_reason`) or
/// without a `Done` at all is a retryable `Err` — a truncated summary
/// must NEVER rewrite the transcript (finding 4: the `run_compaction`
/// error path keeps the transcript intact).
///
/// (ADR 0047) `older` is the range ALREADY TRIMMED to the Summary input
/// budget by the caller, and `dropped` is how many messages that trim
/// removed — the model is told, so it does not present a partial summary as
/// the whole history, and the call carries its OWN output cap rather than
/// riding the provider's default.
pub(crate) async fn summarize(
    ctx: &CompactionCtx<'_>,
    older: &[ChatMessage],
    dropped: usize,
    turn: &CancellationToken,
) -> Result<String, ProviderError> {
    let system = ChatMessage {
        role: ChatRole::System,
        content: MessageContent::Text(summary_system_prompt(ctx.instructions.as_deref(), dropped)),
        tool_call_id: None,
        tool_calls: None,
    };
    let mut messages = vec![system];
    messages.extend(older.iter().cloned());
    let req = ModelRequest {
        model: ctx.model.id.clone(),
        messages,
        tools: Vec::new(),
        options: ModelOptions {
            temperature: Some(0.0),
            // (ADR 0047) The summary's OWN output budget. Uncapped, an
            // ambitious summary runs to the provider's own default and comes
            // back `Done(Length)` — which the tail of this function correctly
            // rejects as a truncated summary. `.max(1)` because a reserve of
            // 0 must not produce a request that cannot answer with a byte.
            max_tokens: Some(ctx.compactor.summary_output_cap().max(1)),
            reasoning_effort: None,
            stream: true,
        },
        session_id: Some(ctx.session_id.to_string()),
    };
    let mut stream = tokio::select! {
        s = ctx.provider.complete(&req) => s?,
        _ = turn.cancelled() => {
            return Err(ProviderError::Fatal(
                "the turn was cancelled during compaction".to_string(),
            ));
        }
    };
    let mut summary = String::new();
    let mut finished: Option<FinishReason> = None;
    loop {
        tokio::select! {
            ev = stream.next() => {
                let Some(ev) = ev else { break };
                match ev {
                    ProviderEvent::TextDelta(d) => summary.push_str(&d),
                    ProviderEvent::Done(f) => {
                        finished = Some(f);
                        break;
                    }
                    ProviderEvent::Error(e) => return Err(e),
                    _ => {}
                }
            }
            _ = turn.cancelled() => {
                return Err(ProviderError::Fatal(
                    "the turn was cancelled during compaction".to_string(),
                ));
            }
        }
    }
    // A `Done(Error)` (the provider synthesizes it when the stream
    // ends without a `finish_reason`), a `Done(Length)` (the summary
    // hit the OUTPUT TOKEN LIMIT — the wire `finish_reason`
    // `"length"`: the summary is literally TRUNCATED), or a stream
    // that ended WITHOUT a `Done` at all is a truncated summary — a
    // retryable `Err`, NOT a partial `Ok` (a partial summary would
    // rewrite the transcript and permanently lose the tail of the
    // summarized history from a mere transport hiccup).
    match finished {
        Some(FinishReason::Error) => Err(ProviderError::Retryable(
            "the summary stream ended without a finish_reason".to_string(),
        )),
        Some(FinishReason::Length) => Err(ProviderError::Retryable(
            "the summary hit the output token limit".to_string(),
        )),
        Some(_) => Ok(summary),
        None => Err(ProviderError::Retryable(
            "the summary stream ended without a finish_reason".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::agent::harness::catalog::Model;
    use crate::agent::harness::provider::ChatRole;
    use crate::agent::harness::provider::{Provider, ProviderError, ProviderEvent};
    use crate::agent::harness::store::{SessionStore, Store};
    use crate::agent::SessionInfo;
    use crate::storage::Db;

    /// A scripted `Provider` (canned `ProviderEvent`s per `complete` call —
    /// a copy of the `loop.rs` test helper, so the moved compaction tests
    /// are unchanged).
    struct ScriptedProvider {
        calls: Arc<AtomicU32>,
        scripts: Vec<Option<Vec<ProviderEvent>>>,
    }

    impl ScriptedProvider {
        fn new(scripts: Vec<Option<Vec<ProviderEvent>>>) -> (Self, Arc<AtomicU32>) {
            let calls = Arc::new(AtomicU32::new(0));
            (
                Self {
                    calls: calls.clone(),
                    scripts,
                },
                calls,
            )
        }
    }

    #[async_trait::async_trait]
    impl Provider for ScriptedProvider {
        async fn complete(
            &self,
            _req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            let i = self.calls.fetch_add(1, Ordering::SeqCst) as usize;
            let entry = if i < self.scripts.len() {
                &self.scripts[i]
            } else {
                self.scripts.last().expect("at least one script")
            };
            match entry {
                Some(events) => Ok(futures_util::stream::iter(events.clone()).boxed()),
                None => futures_util::future::pending().await,
            }
        }
    }

    /// A `Provider` that records `req.session_id` and returns a canned
    /// `"ok"` (the moved `summarize_model_request_carries_the_session_id`
    /// test).
    struct SessionRecordingProvider {
        recorded: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    impl SessionRecordingProvider {
        fn new(recorded: Arc<std::sync::Mutex<Vec<Option<String>>>>) -> Self {
            Self { recorded }
        }
    }

    #[async_trait::async_trait]
    impl Provider for SessionRecordingProvider {
        async fn complete(
            &self,
            req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            self.recorded
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(req.session_id.clone());
            Ok(futures_util::stream::iter(vec![
                ProviderEvent::TextDelta("ok".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ])
            .boxed())
        }
    }

    /// Records the WHOLE summary `ModelRequest` — the input size and the output
    /// cap are what ADR 0047 pins.
    struct RequestRecordingProvider {
        recorded: Arc<std::sync::Mutex<Vec<crate::agent::harness::provider::ModelRequest>>>,
    }

    #[async_trait::async_trait]
    impl Provider for RequestRecordingProvider {
        async fn complete(
            &self,
            req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            self.recorded
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(req.clone());
            Ok(futures_util::stream::iter(vec![
                ProviderEvent::TextDelta("ok".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ])
            .boxed())
        }
    }

    /// A `Model` for a given bare id (the `loop.rs` test fixture, copied).
    fn fake_model(id: &str) -> Model {
        Model {
            id: id.to_string(),
            provider: "fake".to_string(),
            base_url: "http://fake".to_string(),
            api_key: "k".to_string(),
            context_window: 128000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }

    /// The summary system prompt, byte for byte (`/compact <args>` APPENDS a
    /// line to it — the base text must never move).
    const SUMMARY_SYSTEM: &str = "Summarize these messages concisely, preserving decisions, file paths, and open tasks. Respond with the summary only.";

    /// A `Provider` that records the SYSTEM message of every request (the
    /// observation point for the `/compact` extra instruction line).
    struct SystemPromptRecordingProvider {
        recorded: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl SystemPromptRecordingProvider {
        fn new(recorded: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
            Self { recorded }
        }
    }

    #[async_trait::async_trait]
    impl Provider for SystemPromptRecordingProvider {
        async fn complete(
            &self,
            req: &ModelRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, ProviderEvent>, ProviderError>
        {
            if let Some(ChatMessage {
                role: ChatRole::System,
                content: MessageContent::Text(t),
                ..
            }) = req.messages.first()
            {
                self.recorded
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(t.clone());
            }
            Ok(futures_util::stream::iter(vec![
                ProviderEvent::TextDelta("ok".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ])
            .boxed())
        }
    }

    /// The summary system prompt for one `summarize` run with `instructions`
    /// (the recorded system text).
    async fn recorded_summary_prompt(instructions: Option<String>) -> String {
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let provider = SystemPromptRecordingProvider::new(recorded.clone());
        let (_db, store) = temp_store();
        let model = fake_model("m1");
        let mut compactor = Compactor::new(CompactionConfig::default(), model.context_window);
        let older = vec![text_msg("history")];
        let summary = {
            let ctx = CompactionCtx {
                session_id: "s1",
                model: &model,
                provider: &provider,
                store: &store,
                keep_recent_tokens: 1,
                instructions,
                compactor: &mut compactor,
            };
            summarize(&ctx, &older, 0, &CancellationToken::new())
                .await
                .expect("summarize succeeds")
        };
        assert_eq!(summary, "ok", "the canned summary landed");
        let prompts = recorded.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(prompts.len(), 1, "one model call for the summary");
        prompts[0].clone()
    }

    /// `/compact <args>` (ADR 0044): the user's extra instructions ride the
    /// summary system prompt as ONE appended line, after the unchanged base
    /// text.
    #[tokio::test]
    async fn summarize_appends_the_caller_instructions_as_an_extra_line() {
        let prompt = recorded_summary_prompt(Some("keep every test name".to_string())).await;
        assert_eq!(
            prompt,
            format!(
                "{SUMMARY_SYSTEM}\nAdditional instructions for this summary: keep every test name"
            ),
            "the base prompt is byte-identical and the instructions ride the next line"
        );
    }

    /// Threshold / overflow pass `None`, and a `/compact` whose args are only
    /// whitespace is no instructions at all: the summary prompt stays
    /// byte-identical to the base text either way.
    #[tokio::test]
    async fn summarize_without_instructions_is_the_unchanged_base_prompt() {
        for instructions in [None, Some(String::new()), Some("   ".to_string())] {
            assert_eq!(
                recorded_summary_prompt(instructions.clone()).await,
                SUMMARY_SYSTEM,
                "no instructions ({instructions:?}) must not touch the prompt"
            );
        }
    }

    /// The moved tests' harness: a temp-dir `Db` with the `"s1"` session
    /// row (the `native_messages` FK), a `SessionStore`, and the fake
    /// model (what `build_loop_with_db` gave the `AgentLoop`).
    fn temp_store() -> (Arc<Db>, Arc<dyn Store>) {
        let dir =
            std::env::temp_dir().join(format!("harness-compact-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Arc::new(Db::open(&dir.join("t.db")).expect("db should open"));
        db.record_session(&SessionInfo {
            session_id: "s1".to_string(),
            cwd: std::path::PathBuf::from("/tmp"),
            capabilities: serde_json::json!({}),
            config_options: None,
            archived: false,
            title: None,
            context_usage: None,
            is_subagent: false,
        })
        .expect("record_session");
        let store: Arc<dyn Store> = Arc::new(SessionStore::new(db.clone()));
        (db, store)
    }

    fn text_msg(t: &str) -> ChatMessage {
        ChatMessage {
            role: ChatRole::User,
            content: MessageContent::Text(t.to_string()),
            tool_call_id: None,
            tool_calls: None,
        }
    }

    /// The regression: an agentic loop re-sends the whole context on every
    /// call, so summing per-call usage charges the same tokens N times and
    /// compaction trips at ~window/N. Six calls whose prompts sum to far more
    /// than the threshold, but whose LARGEST prompt is still under it, are a
    /// session that has not filled its window — they must not be due.
    #[test]
    fn repeated_calls_do_not_charge_the_prompt_once_per_iteration() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        let msgs: Vec<ChatMessage> = (0..6).map(|i| text_msg(&format!("m{i}"))).collect();
        for i in 0..6 {
            c.record_usage(
                &Usage {
                    input_tokens: 300,
                    output_tokens: 10,
                },
                &msgs[..=i],
            );
            assert!(
                !c.should_compact(),
                "a 310-token context is not due on a 1000-window with a 100 reserve, \
                 whatever the calls summed to (call {i}: {})",
                c.context_tokens()
            );
        }
    }

    /// A usage report is a SNAPSHOT of the context, not an increment: the
    /// prompt is the ENTIRE transcript, re-sent on every call, so a second
    /// call that reports the same numbers must leave the count where it was
    /// (adding it was the bug that tripped compaction at ~window/N).
    #[test]
    fn a_usage_report_is_the_context_not_an_increment() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        assert!(
            !c.should_compact(),
            "a fresh context is under the threshold"
        );
        c.record_usage(
            &Usage {
                input_tokens: 500,
                output_tokens: 100,
            },
            &[],
        );
        assert_eq!(c.context_tokens(), 600, "the report IS the context");
        assert!(!c.should_compact(), "600 < 1000 - 100");
        c.record_usage(
            &Usage {
                input_tokens: 500,
                output_tokens: 100,
            },
            &[],
        );
        assert_eq!(
            c.context_tokens(),
            600,
            "a second call reporting the same context does not add to it"
        );
        assert!(!c.should_compact(), "still 600 — not due");
        c.record_usage(
            &Usage {
                input_tokens: 950,
                output_tokens: 0,
            },
            &[],
        );
        assert!(c.should_compact(), "950 > 1000 - 100");
    }

    #[test]
    fn a_disabled_config_never_compacts() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: false,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        c.record_usage(
            &Usage {
                input_tokens: 99999,
                output_tokens: 99999,
            },
            &[],
        );
        assert!(!c.should_compact());
    }

    #[test]
    fn a_window_below_the_reserve_saturates_the_threshold() {
        // `saturating_sub` (no underflow): a 50-token window with a
        // 100-token reserve is a threshold of 0 — any positive usage
        // trips it (the arithmetic must not wrap).
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            50,
        );
        c.record_usage(
            &Usage {
                input_tokens: 10,
                output_tokens: 10,
            },
            &[],
        );
        assert!(c.should_compact(), "20 > 0 (the saturated threshold)");
    }

    /// pi's `estimateContextTokens`: the anchor (the last reported usage)
    /// plus the estimate of the messages AFTER it. A call that reports NO
    /// usage must never drop the metric below the anchor — the anchor only
    /// moves when the provider really reports, so the trailing estimate can
    /// only ever ADD to it.
    ///
    /// The transcript is built the way the loop builds it: the two prompt
    /// messages the call was made over, then — AFTER the report — the assistant
    /// reply that report's `output_tokens` already includes. That reply is the
    /// watermark slot (index 2): the snapshot already counts it, so only what
    /// comes after it is estimated.
    #[test]
    fn refresh_keeps_the_anchor_and_adds_the_trailing_messages() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        // The transcript the provider call was made over (its `input_tokens`
        // IS these two messages).
        let prompt = vec![text_msg("first message"), text_msg("second message")];
        c.record_usage(
            &Usage {
                input_tokens: 900,
                output_tokens: 100,
            },
            &prompt,
        );
        assert_eq!(
            c.context_tokens(),
            1000,
            "the anchor is the whole context the provider holds"
        );
        // The loop appends the reply the report already accounts for (index 2,
        // the watermark slot), and then a tool result — the ONLY message a
        // usage-less call can still measure.
        let reply = assistant("the reply the anchor already counts");
        let tool_result = text_msg("a tool result appended after the reply");
        let with_reply = vec![
            text_msg("first message"),
            text_msg("second message"),
            reply.clone(),
        ];
        let grown = vec![
            text_msg("first message"),
            text_msg("second message"),
            reply.clone(),
            tool_result.clone(),
        ];
        // The trailing estimate is the tool result ALONE: 38 chars / 4 = 9,
        // +1 for the role overhead = 10. So the snapshot is
        // `anchor 1000 + trailing 10 = 1010`.
        assert_eq!(estimate_context(&[tool_result]), 10, "the trailing term");
        assert_eq!(
            c.refresh(&grown),
            1000 + 10,
            "the anchor survives and the message AFTER the watermark slot is estimated"
        );
        assert_eq!(c.context_tokens(), 1010, "the tracked count is the sum");
        // And a smaller transcript estimate can never pull it down — the
        // reply's own estimate is NOT re-added (it is inside the watermark) and
        // the anchor is not replaced by what the local estimate thinks.
        assert_eq!(
            c.refresh(&with_reply),
            1000,
            "the anchor is never overwritten by an estimate"
        );
    }

    /// The watermark `record_usage` set ASSUMES the assistant reply lands at
    /// `messages.len()` — but a cancelled / failed turn deliberately does NOT
    /// persist its partial reply, so the watermark points one past reality and
    /// the NEXT turn's prompt lands INSIDE the watermark window (unmeasured
    /// until the next report). `turn_abandoned` reconciles: the watermark
    /// pulls back to what the transcript actually holds and the discarded
    /// reply's output leaves the anchor (the provider never produced it, so it
    /// is not part of the context).
    #[test]
    fn turn_abandoned_retracts_the_reply_that_never_landed() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        let two = vec![text_msg("first message"), text_msg("second message")];
        c.record_usage(
            &Usage {
                input_tokens: 900,
                output_tokens: 100,
            },
            &two,
        );
        assert_eq!(c.context_tokens(), 1000, "the anchor is input + output");
        // The turn died before the reply was pushed: the transcript is still
        // the two messages the report was made over.
        c.turn_abandoned(&two);
        assert_eq!(
            c.context_tokens(),
            900,
            "the discarded reply's output leaves the anchor — the provider never \
             produced it, so it is not part of the context"
        );
        // The watermark is 2 (what the transcript holds), so the message the
        // NEXT turn appends at index 2 is trailing, not covered: 5 chars / 4 +
        // 1 (the role overhead) = 2 tokens on top of the 900 anchor.
        let three = vec![
            text_msg("first message"),
            text_msg("second message"),
            text_msg("short"),
        ];
        assert_eq!(
            c.refresh(&three),
            902,
            "the message at the index the abandoned reply never took is estimated"
        );
        // And the retraction LEAVES NOTHING BEHIND: the reply it retracted is
        // no longer owed to anyone, so the field that remembered it is zero
        // too. Today the stale value is unreachable (`turn_abandoned`'s guard
        // holds until the next `record_usage` overwrites it, and `reestimate`
        // zeroes it), but a LOCAL invariant costs a reader nothing to check,
        // while the global one needs all three methods in their heads.
        assert_eq!(
            c.pending_output, 0,
            "a retracted reply is not pending any more"
        );
    }

    /// The reconcile must be safe to call on EVERY turn exit: when the reply
    /// DID land (the transcript is at least as long as the watermark), the
    /// anchor is exactly right and nothing is retracted — an unconditional
    /// subtraction would discount a reply that IS in the context (a
    /// cancelled tool batch, whose assistant message was persisted, is this
    /// case).
    #[test]
    fn turn_abandoned_is_a_no_op_when_the_reply_landed() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        let two = vec![text_msg("first message"), text_msg("second message")];
        c.record_usage(
            &Usage {
                input_tokens: 900,
                output_tokens: 100,
            },
            &two,
        );
        // The reply landed at index 2 — the watermark (3) is what the
        // transcript now holds.
        let three = vec![
            text_msg("first message"),
            text_msg("second message"),
            assistant("the reply"),
        ];
        assert_eq!(c.refresh(&three), 1000, "the anchor covers the reply");
        c.turn_abandoned(&three);
        assert_eq!(
            c.context_tokens(),
            1000,
            "a reply that is in the transcript is never discounted"
        );
        // …and a later message is still estimated on top of it.
        let four = vec![
            text_msg("first message"),
            text_msg("second message"),
            assistant("the reply"),
            text_msg("short"),
        ];
        assert_eq!(c.refresh(&four), 1002, "the trailing message is estimated");
    }

    /// `reestimate` clears the anchor WHOLESALE (a compaction / resume is a
    /// new baseline) — and so does `turn_abandoned`'s own guard: a post-reset
    /// watermark is `0`, which can never exceed the transcript's length, so an
    /// abandoned turn that predates the compaction cannot discount the new
    /// baseline.
    #[test]
    fn turn_abandoned_after_a_reestimate_does_not_discount_the_new_baseline() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        c.record_usage(
            &Usage {
                input_tokens: 900,
                output_tokens: 100,
            },
            &[],
        );
        c.reestimate(&[text_msg("short")]);
        let baseline = c.context_tokens();
        c.turn_abandoned(&[text_msg("short")]);
        assert_eq!(
            c.context_tokens(),
            baseline,
            "the re-estimate already dropped the anchor, so there is nothing left to \
             retract (\"short\" = 5/4 + 1 = 2 tokens)",
        );
    }

    #[test]
    fn reestimate_replaces_the_usage_anchor() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        c.record_usage(
            &Usage {
                input_tokens: 900,
                output_tokens: 900,
            },
            &[],
        );
        c.reestimate(&[text_msg("short")]);
        assert!(c.context_tokens() < 100, "the estimate replaces the usage");
        c.refresh(&[text_msg("short")]);
        assert!(
            c.context_tokens() < 100,
            "the ANCHOR is cleared too — a compaction / resume invalidates it, \
             not just the total, got {}",
            c.context_tokens()
        );
    }

    /// The keep-recent budget is capped by what the MODEL's window can
    /// afford: the catalog's `keep_recent_tokens` is catalog-wide while the
    /// window is per-model, so an uncapped 20 000 covers the WHOLE transcript
    /// of a small-window model — `older` comes back empty, the compaction is a
    /// no-op and `should_compact()` stays true forever. Pinned against
    /// `CompactionConfig::default()` (16 384 reserve, 20 000 keep).
    #[test]
    fn keep_budget_caps_the_keep_to_what_the_window_affords() {
        let budget_for =
            |window: u32| Compactor::new(CompactionConfig::default(), window).keep_budget();
        // Big models are UNCHANGED: the configured budget still wins.
        assert_eq!(budget_for(200_000), 20_000, "a 200k window affords it all");
        assert_eq!(budget_for(65_536), 20_000, "65 536 − 16 384 − 4 096");
        // The window caps it.
        assert_eq!(budget_for(32_768), 12_288, "32 768 − 16 384 − 4 096");
        // And it has NO floor: a window that cannot afford any keep at all
        // budgets 0, which keeps the last message and still shrinks.
        assert_eq!(budget_for(8_192), 0, "a window under its own reserve");
        // No floor ALSO means no override of a small CONFIGURED budget — the
        // seam the test fixtures use to force a real compaction.
        assert_eq!(
            Compactor::new(
                CompactionConfig {
                    keep_recent_tokens: 1,
                    ..Default::default()
                },
                128_000,
            )
            .keep_budget(),
            1,
            "a configured budget below what the window affords is left alone"
        );
    }

    /// (ADR 0047) The summary's two numbers: an output cap of
    /// `min(floor(0.8 × reserve), 8_192)` and an input budget of the window
    /// minus that cap minus the headroom `keep_budget` charges.
    #[test]
    fn the_summary_budget_is_the_window_minus_its_own_output_and_headroom() {
        // reserve 16 384 → cap min(13 107, 8 192) = 8 192; headroom min(16 384, 4 096).
        let c = Compactor::new(
            CompactionConfig {
                reserve_tokens: 16_384,
                ..Default::default()
            },
            32_000,
        );
        assert_eq!(c.summary_output_cap(), 8_192);
        assert_eq!(c.summary_input_budget(), 32_000 - 8_192 - 4_096);
    }

    /// A reserve below the ceiling caps the summary at 80% of itself — pi's rule.
    #[test]
    fn a_small_reserve_caps_the_summary_output_at_eighty_percent() {
        let c = Compactor::new(
            CompactionConfig {
                reserve_tokens: 1_000,
                ..Default::default()
            },
            32_000,
        );
        assert_eq!(c.summary_output_cap(), 800);
    }

    /// A window already spent on its own reserve has NOTHING left for a
    /// summary, and `saturating_*` makes that ZERO rather than a wrap-around
    /// that reads as a huge budget. A reserve of 0 affords no output at all.
    #[test]
    fn a_window_below_its_own_reserve_has_no_summary_budget() {
        let c = Compactor::new(
            CompactionConfig {
                reserve_tokens: 16_384,
                ..Default::default()
            },
            12_000,
        );
        assert_eq!(c.summary_input_budget(), 0);
        let none = Compactor::new(
            CompactionConfig {
                reserve_tokens: 0,
                ..Default::default()
            },
            128_000,
        );
        assert_eq!(none.summary_output_cap(), 0);
    }

    #[test]
    fn split_keeps_the_recent_tail() {
        let msgs = vec![
            text_msg("aaaaaaaaaa"),
            text_msg("bbbbbbbbbb"),
            text_msg("cccccccccc"),
        ];
        let (older, recent) = split_for_compaction(&msgs, 3);
        assert_eq!(older.len(), 2, "the older messages are summarized");
        assert_eq!(recent.len(), 1, "the recent tail is kept");
    }

    #[test]
    fn split_keeps_at_least_the_last_message() {
        // A zero budget must not drop the latest user message (the
        // model would have no context at all).
        let msgs = vec![text_msg("a"), text_msg("b"), text_msg("c")];
        let (older, recent) = split_for_compaction(&msgs, 0);
        assert_eq!(older.len(), 2);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].content, MessageContent::Text("c".to_string()));
    }

    #[test]
    fn split_of_a_single_message_keeps_it() {
        let msgs = vec![text_msg("only")];
        let (older, recent) = split_for_compaction(&msgs, 5);
        assert!(older.is_empty());
        assert_eq!(recent.len(), 1);
    }

    /// (finding 5) The kept tail must NEVER start on a `tool` message:
    /// with a tail of `[assistant(tool_calls), tool]`, the token budget
    /// may land the boundary ON the `tool` message — the boundary must
    /// snap back so the assistant `tool_calls` parent is kept with its
    /// result (an orphaned `tool` message whose `tool_call_id` has no
    /// assistant `tool_calls` parent 400s on conformant APIs).
    #[test]
    fn split_never_starts_the_kept_tail_on_a_tool_message() {
        let asst = ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text(String::new()),
            tool_call_id: None,
            tool_calls: Some(vec![crate::agent::harness::provider::ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: serde_json::json!({ "command": "ls" }),
            }]),
        };
        let tool = ChatMessage {
            role: ChatRole::Tool,
            content: MessageContent::Text("result".to_string()),
            tool_call_id: Some("t1".to_string()),
            tool_calls: None,
        };
        let msgs = vec![
            text_msg("a".repeat(40000).as_str()),
            text_msg("b".repeat(40000).as_str()),
            asst,
            tool,
        ];
        let (older, recent) = split_for_compaction(&msgs, 10);
        assert_eq!(older.len(), 2, "the older messages are summarized");
        assert_eq!(
            recent.len(),
            2,
            "the assistant `tool_calls` message is kept with its result"
        );
        assert!(
            matches!(recent[0].role, ChatRole::Assistant),
            "the kept tail starts on the assistant message, not an orphaned tool"
        );
        assert!(matches!(recent[1].role, ChatRole::Tool));
    }

    /// (ADR 0047) The recent END of the summarized range is what a summary
    /// must not lose, so the trim drops from the OLD end.
    #[test]
    fn the_trim_keeps_the_newest_slice_that_fits() {
        // Each message: "m{i}" + 3 999 x's = 4 001 chars → 4 001/4 + 1 = 1_001.
        let msgs: Vec<ChatMessage> = (0..4)
            .map(|i| text_msg(&format!("m{i}{}", "x".repeat(3_999))))
            .collect();
        // 2_100 fits two (2_002) but not three (3_003).
        let (kept, dropped) = trim_for_summary_budget(&msgs, 2_100);
        assert_eq!(dropped, 2, "the two oldest are dropped");
        assert_eq!(kept.len(), 2, "the newest two are summarized");
        assert!(
            matches!(&kept[0].content, MessageContent::Text(t) if t.starts_with("m2")),
            "the slice starts at the newest end, got {:?}",
            kept[0].content
        );
    }

    /// The kept slice never STARTS on a `tool` result — the boundary snaps
    /// FORWARD past it, and the snapped-past message still counts as dropped
    /// (an orphaned `tool` message 400s on conformant APIs).
    #[test]
    fn the_trim_never_starts_the_kept_slice_on_a_tool_message() {
        let asst = ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text(String::new()),
            tool_call_id: None,
            tool_calls: Some(vec![crate::agent::harness::provider::ToolCall {
                id: "t1".to_string(),
                name: "bash".to_string(),
                arguments: serde_json::json!({ "command": "ls" }),
            }]),
        };
        let tool = ChatMessage {
            role: ChatRole::Tool,
            content: MessageContent::Text("result".to_string()),
            tool_call_id: Some("t1".to_string()),
            tool_calls: None,
        };
        // Estimates: "oldest" 2, `asst` 21 (one tool_call = 20, + 1), `tool` 2,
        // "newest" 2. Budget 10 takes newest (2) + tool (4), stops at `asst`
        // (25 > 10), so the RAW boundary lands on the tool result at index 2.
        let msgs = vec![text_msg("oldest"), asst, tool, text_msg("newest")];
        let (kept, dropped) = trim_for_summary_budget(&msgs, 10);
        assert_eq!(dropped, 3, "the snapped-past tool result is still dropped");
        assert_eq!(kept.len(), 1, "only the newest message is summarized");
        assert!(
            !matches!(kept[0].role, ChatRole::Tool),
            "the summarized slice does not start on an orphaned tool result"
        );
    }

    /// (ADR 0047) The budget measures the request `summarize` ACTUALLY sends,
    /// and that request carries the tool-call `arguments` IN FULL. The shared
    /// context estimate charges a flat ~20 per call and ignores them, so a
    /// file-writing call — thousands of tokens of `content` — passes a budget
    /// it cannot fit, and the summary overflows the very window compaction
    /// exists to recover (the failure ADR 0047 was written to end).
    #[test]
    fn the_trim_charges_the_tool_call_arguments_the_summary_sends() {
        use crate::agent::harness::provider::ToolCall;
        // ~10 000 tokens of argument — a large `write`, the call that drives
        // compaction in the first place.
        let big = "x".repeat(40_000);
        let writer = ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text(String::new()),
            tool_call_id: None,
            tool_calls: Some(vec![ToolCall {
                id: "t1".to_string(),
                name: "write".to_string(),
                arguments: serde_json::json!({ "path": "a.rs", "content": big }),
            }]),
        };
        // The shared estimate sees ONE tool call here: 21 tokens, which fits
        // 500. The serialized arguments alone are ~10 000, so nothing fits.
        let (kept, dropped) = trim_for_summary_budget(std::slice::from_ref(&writer), 500);
        assert!(
            kept.is_empty(),
            "a call whose arguments alone dwarf the budget is not summarized"
        );
        assert_eq!(dropped, 1, "the unaffordable call counts as dropped");

        // A budget that DOES afford the arguments still keeps the call: the
        // trim measures the payload, it does not veto tool calls.
        let (kept, dropped) = trim_for_summary_budget(&[writer], 20_000);
        assert_eq!(
            kept.len(),
            1,
            "a budget that affords the arguments keeps the call"
        );
        assert_eq!(dropped, 0);
    }

    /// A budget that fits nothing summarizes nothing: the caller must NOT send
    /// a doomed call, so the empty slice is the signal.
    #[test]
    fn a_budget_that_fits_nothing_trims_everything() {
        let msgs = vec![text_msg("a"), text_msg("b")];
        let (kept, dropped) = trim_for_summary_budget(&msgs, 0);
        assert!(kept.is_empty(), "nothing fits a zero budget");
        assert_eq!(dropped, 2);
    }

    // ── (finding 4) a truncated summary stream is NOT a good summary ──

    /// (finding 4) The provider synthesizes `Done(FinishReason::Error)`
    /// when a stream ends without a `finish_reason` — `summarize` must
    /// treat it as a retryable `Err`, NOT return a partial `Ok` (a
    /// partial summary would rewrite the transcript and lose history
    /// from a mere transport hiccup).
    #[tokio::test]
    async fn a_summary_stream_ending_in_an_error_is_not_a_good_summary() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("partial".to_string()),
            ProviderEvent::Done(FinishReason::Error),
        ])]);
        let older = vec![text_msg("the old messages")];
        let r = call_summarize(&provider, &older).await;
        assert!(
            matches!(r, Err(ProviderError::Retryable(_))),
            "a `Done(Error)` stream is a retryable error, got {r:?}"
        );
    }

    /// (finding 4, round 2) A summary that hits the OUTPUT TOKEN LIMIT
    /// (`Done(FinishReason::Length)` — the wire `finish_reason`
    /// `"length"`) is TRUNCATED: it must be a retryable `Err`, NOT an
    /// `Ok` (an accepted truncated summary would rewrite the
    /// transcript and permanently lose the tail of the summarized
    /// history — the same invariant as the `Done(Error)` case above).
    #[tokio::test]
    async fn a_summary_stream_ending_in_length_is_not_a_good_summary() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("partial".to_string()),
            ProviderEvent::Done(FinishReason::Length),
        ])]);
        let older = vec![text_msg("the old messages")];
        let r = call_summarize(&provider, &older).await;
        assert!(
            matches!(r, Err(ProviderError::Retryable(_))),
            "a `Done(Length)` stream is a retryable error, got {r:?}"
        );
    }

    /// (finding 4) A summary stream that ends WITHOUT a `Done` at all
    /// (the transport just dropped) is a retryable `Err` too — no
    /// partial summary is returned.
    #[tokio::test]
    async fn a_summary_stream_without_a_done_is_not_a_good_summary() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![ProviderEvent::TextDelta(
            "partial".to_string(),
        )])]);
        let older = vec![text_msg("the old messages")];
        let r = call_summarize(&provider, &older).await;
        assert!(
            matches!(r, Err(ProviderError::Retryable(_))),
            "a stream that ends without a `Done` is a retryable error, got {r:?}"
        );
    }

    /// (ADR 0017) `run_compaction` PRESERVES the leading system message
    /// (the system message is prompt state, not conversation — it is
    /// excluded from the compaction target and re-prepended to the
    /// compacted transcript; the `replace_messages` rewrite keeps it at
    /// seq 0, the summary following it).
    #[tokio::test]
    async fn compaction_preserves_leading_system_message() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("the summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store);
        harness.messages = transcript_with_system();
        harness.run().await;
        let messages = &harness.messages;
        // The leading system message SURVIVES at index 0 (the ORIGINAL
        // message — NOT replaced by the summary).
        assert!(
            matches!(messages[0].role, ChatRole::System),
            "the system message survives compaction"
        );
        assert_eq!(
            messages[0].content,
            MessageContent::Text("the prompt".to_string()),
            "the system message is the original, verbatim"
        );
        // The summary follows it.
        assert!(
            matches!(messages[1].role, ChatRole::System),
            "the summary is a system message at index 1"
        );
        match &messages[1].content {
            MessageContent::Text(t) => assert!(
                t.starts_with("Summary of previous conversation:"),
                "the summary message, got {t:?}"
            ),
            other => panic!("expected a text summary, got {other:?}"),
        }
        // The `replace_messages` rewrite kept the system message at seq 0.
        let loaded = harness.store.load_messages("s1").unwrap();
        assert!(
            matches!(loaded[0].role, ChatRole::System),
            "the system message is the seq-0 row after the rewrite"
        );
        assert_eq!(
            loaded[0].content,
            MessageContent::Text("the prompt".to_string()),
            "the seq-0 row is the original prompt, verbatim"
        );
    }

    /// (ADR 0017) a transcript WITHOUT a leading system message compacts
    /// EXACTLY as today (the summary leads the compacted transcript —
    /// today's behavior preserved).
    #[tokio::test]
    async fn compaction_without_system_message_unchanged() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("the summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store);
        harness.messages = transcript_without_system();
        harness.run().await;
        // No system message was seeded: the summary leads (today's
        // behavior).
        assert!(
            matches!(harness.messages[0].role, ChatRole::System),
            "the summary leads the compacted transcript"
        );
        match &harness.messages[0].content {
            MessageContent::Text(t) => assert!(
                t.starts_with("Summary of previous conversation:"),
                "the summary message, got {t:?}"
            ),
            other => panic!("expected a text summary, got {other:?}"),
        }
    }

    /// (review finding 1) TWO compactions on a transcript WITHOUT a real
    /// system prompt FOLD the old summary into the new one: the summary
    /// is CONVERSATION state (harness-generated — the `Summary of
    /// previous conversation:` prefix), not prompt state, so the second
    /// compaction compacts it too. (A legacy pre-ADR-0017 resume / a
    /// child with no system message: the first compaction puts the
    /// summary at index 0; the second must NOT freeze it there.)
    #[tokio::test]
    async fn compaction_twice_without_system_message_folds_the_old_summary() {
        // Distinct canned response per `summarize` call: the FIRST
        // compaction gets "the summary one", the SECOND gets "the
        // summary two" (the old summary folded into it).
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::TextDelta("the summary one".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("the summary two".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store);
        harness.messages = vec![
            text_msg("u1"),
            ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text("a1".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            text_msg("u2"),
            ChatMessage {
                role: ChatRole::Assistant,
                content: MessageContent::Text("a2".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
        ];
        harness.run().await;
        harness.run().await;
        // The NEW summary leads (the old summary was FOLDED into it —
        // not preserved verbatim at index 0).
        assert!(
            matches!(harness.messages[0].role, ChatRole::System),
            "the new summary leads the compacted transcript"
        );
        match &harness.messages[0].content {
            MessageContent::Text(t) => assert!(
                t.contains("the summary two"),
                "the leading summary is the NEW one (the old summary was folded into it), got {t:?}"
            ),
            other => panic!("expected a text summary, got {other:?}"),
        }
        assert_eq!(
            harness.messages.len(),
            2,
            "the compacted transcript is the new summary + the kept recent message"
        );
    }

    /// (review finding 1) a transcript WITH a real leading system prompt
    /// keeps it verbatim across TWO compactions (a non-summary leading
    /// System message is prompt state — excluded from the compaction
    /// target and re-prepended), and the second compaction folds the
    /// old summary into the new one behind it.
    #[tokio::test]
    async fn compaction_twice_with_system_message() {
        // Distinct canned response per `summarize` call: the FIRST
        // compaction gets "the summary one", the SECOND gets "the
        // summary two" (the old summary folded into it).
        let (provider, _calls) = ScriptedProvider::new(vec![
            Some(vec![
                ProviderEvent::TextDelta("the summary one".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
            Some(vec![
                ProviderEvent::TextDelta("the summary two".to_string()),
                ProviderEvent::Done(FinishReason::Stop),
            ]),
        ]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store);
        harness.messages = transcript_with_system();
        harness.run().await;
        harness.run().await;
        // The ORIGINAL system message survives verbatim at index 0.
        assert!(
            matches!(harness.messages[0].role, ChatRole::System),
            "the system message survives the second compaction"
        );
        assert_eq!(
            harness.messages[0].content,
            MessageContent::Text("the prompt".to_string()),
            "the system message is the original, verbatim"
        );
        // The NEW summary follows it (the old summary was folded into
        // it — the second compaction compacted the first's summary).
        match &harness.messages[1].content {
            MessageContent::Text(t) => assert!(
                t.contains("the summary two"),
                "the summary at index 1 is the NEW one, got {t:?}"
            ),
            other => panic!("expected a text summary at index 1, got {other:?}"),
        }
    }

    /// The regression the derived budget exists for: a model whose window
    /// cannot afford the catalog's 20 000 keep-recent budget must still SHRINK
    /// when it is over its threshold. Pre-fix the budget was the raw catalog
    /// value, `split_for_compaction` walked the whole transcript without ever
    /// exhausting it, `older` came back empty, and the compaction was a
    /// `Skipped` that left `should_compact()` true forever (and left Task 6's
    /// overflow recovery with nothing to shrink).
    #[tokio::test]
    async fn a_small_window_compaction_actually_shrinks_the_transcript() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("the summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store);
        // A 32k model: 32 768 − 16 384 reserve − 4 096 headroom = 12 288 of the
        // configured 20 000, read from the COMPACTOR — where production reads
        // it (`keep = None` would run the fixture's budget 1 and pin nothing).
        harness.compactor = Compactor::new(CompactionConfig::default(), 32_768);
        harness.keep = Some(harness.compactor.keep_budget());
        // 9 × (8 000 chars / 4 + 1) = 18 009 tokens — the BAND that makes this a
        // regression test: over the 32 768 − 16 384 = 16 384 threshold (a
        // compaction IS due) and under the configured 20 000 (so the OLD
        // uncapped budget walked the whole transcript and skipped).
        harness.messages = (0..9).map(|_| text_msg(&"x".repeat(8_000))).collect();
        let before = harness.messages.len();
        let estimate = estimate_context(&harness.messages);
        assert!(
            estimate > 16_384 && estimate < 20_000,
            "the fixture must sit between the 32k window's threshold and the configured \
             keep, got {estimate}"
        );
        harness.compactor.reestimate(&harness.messages);
        assert!(
            harness.compactor.should_compact(),
            "the transcript is over the 32k window's threshold, so a compaction is due"
        );

        let outcome = harness.run().await;
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "the derived 12 288 budget leaves an older head to summarize; a `Skipped` is \
             the old bug (the 20 000 catalog budget covering the whole transcript)"
        );
        assert!(
            harness.messages.len() < before,
            "the compaction shrank the transcript ({} messages → {})",
            before,
            harness.messages.len(),
        );
        assert!(
            harness.messages.iter().any(is_compaction_summary),
            "one of the resulting messages is the compaction summary"
        );
    }

    #[tokio::test]
    async fn summarize_model_request_carries_the_session_id() {
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let provider = SessionRecordingProvider::new(recorded.clone());
        let older = vec![text_msg("history")];
        let summary = call_summarize(&provider, &older)
            .await
            .expect("summarize succeeds");
        assert_eq!(summary, "ok");

        let sessions = recorded.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(sessions.len(), 1, "expected 1 model call for summarize");
        assert_eq!(sessions[0], Some("s1".to_string()));
    }

    /// (ADR 0047) The summary is a call with an output budget: `max_tokens` is
    /// the cap the Compactor derived, not the provider's own default.
    #[tokio::test]
    async fn the_summary_call_carries_its_own_output_cap() {
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let provider = RequestRecordingProvider {
            recorded: recorded.clone(),
        };
        let summary = call_summarize(&provider, &[text_msg("history")])
            .await
            .expect("summarize succeeds");
        assert_eq!(summary, "ok");
        let reqs = recorded.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(reqs.len(), 1);
        // `call_summarize` builds its Compactor with `CompactionConfig::default()`
        // (reserve 16 384) → min(floor(0.8 × 16 384), 8 192).
        assert_eq!(reqs[0].options.max_tokens, Some(8_192));
    }

    /// A trimmed summary SAYS it is trimmed: the model is told the range is
    /// incomplete, and the stored record carries the harness's exact count.
    #[tokio::test]
    async fn a_trimmed_summary_records_what_it_dropped() {
        let (provider, _calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("the summary".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store.clone());
        // The budget is the binding constraint: 16 384 − 8 192 output − 4 096
        // headroom = 4 096.
        harness.compactor = Compactor::new(CompactionConfig::default(), 16_384);
        harness.keep = Some(1);
        // 8 × (4 000 chars / 4 + 1) = 8 × 1_001 tokens. A keep-budget of 1 keeps
        // ONLY the last message, so `older` is 7 (7 007 tokens) — and 4 096
        // carries four of them (4 004), not five (5 005): 3 are dropped.
        harness.messages = (0..8).map(|_| text_msg(&"y".repeat(4_000))).collect();
        let outcome = harness.run().await;
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "four messages still fit the budget, so the compaction proceeds"
        );
        let rows = store.load_messages("s1").expect("rows load");
        let summary = rows
            .iter()
            .find(|m| is_compaction_summary(m))
            .expect("the summary is in the rewritten transcript");
        let MessageContent::Text(text) = &summary.content else {
            panic!("the summary is a text message");
        };
        assert!(
            text.contains(
                "[Note: this summary is not complete. The oldest 3 message(s) of the \
                 summarized range were dropped"
            ),
            "the stored summary records its own trim verbatim, got {text}"
        );
    }

    /// A budget that fits NOTHING is an honest `Skipped` — and NO call is sent.
    /// The doomed request is exactly what ADR 0047 exists to stop.
    #[tokio::test]
    async fn a_summary_budget_that_fits_nothing_skips_without_a_call() {
        let (provider, calls) = ScriptedProvider::new(vec![Some(vec![
            ProviderEvent::TextDelta("never called".to_string()),
            ProviderEvent::Done(FinishReason::Stop),
        ])]);
        let (_db, store) = temp_store();
        let mut harness = Harness::new(&provider, store);
        // The window is already inside its own reserve: budget 0.
        harness.compactor = Compactor::new(
            CompactionConfig {
                reserve_tokens: 16_384,
                ..Default::default()
            },
            12_000,
        );
        harness.keep = Some(0);
        harness.messages = vec![text_msg("one"), text_msg("two")];
        assert!(matches!(harness.run().await, CompactionOutcome::Skipped));
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no summary call is sent when the budget fits nothing"
        );
    }

    // ── test harness helpers ──

    /// Call [`summarize`] with the harness's fixtures (the moved tests'
    /// `loop_.summarize(...)` equivalent).
    async fn call_summarize(
        provider: &dyn Provider,
        older: &[ChatMessage],
    ) -> Result<String, ProviderError> {
        let (_db, store) = temp_store();
        let model = fake_model("m1");
        let mut compactor = Compactor::new(CompactionConfig::default(), model.context_window);
        let ctx = CompactionCtx {
            session_id: "s1",
            model: &model,
            provider,
            store: &store,
            keep_recent_tokens: 1,
            instructions: None,
            compactor: &mut compactor,
        };
        summarize(&ctx, older, 0, &CancellationToken::new()).await
    }

    /// The moved tests' fixture: the ctx's inputs + the mutable transcript
    /// (what the `AgentLoop` owned). `keep: None` mirrors the old tests' budget
    /// (`loop_.catalog.compaction.keep_recent_tokens = 1`); `Some(k)` runs the
    /// ctx at `k`, which is how a test pins a DERIVED budget.
    struct Harness<'a> {
        model: Model,
        store: Arc<dyn Store>,
        compactor: Compactor,
        pub messages: Vec<ChatMessage>,
        provider: &'a dyn Provider,
        /// The keep-recent budget [`Harness::run`] passes to the ctx — `None`
        /// keeps the fixture's historical `1`, so every existing `Harness`
        /// test is untouched.
        keep: Option<u32>,
    }

    impl<'a> Harness<'a> {
        fn new(provider: &'a impl Provider, store: Arc<dyn Store>) -> Self {
            let model = fake_model("m1");
            let compactor = Compactor::new(CompactionConfig::default(), model.context_window);
            Self {
                model,
                store,
                compactor,
                messages: Vec::new(),
                provider,
                keep: None,
            }
        }

        async fn run(&mut self) -> CompactionOutcome {
            let keep_recent_tokens = self.keep.unwrap_or(1);
            let mut ctx = CompactionCtx {
                session_id: "s1",
                model: &self.model,
                provider: self.provider,
                store: &self.store,
                keep_recent_tokens,
                instructions: None,
                compactor: &mut self.compactor,
            };
            run_compaction(&mut ctx, &mut self.messages, &CancellationToken::new()).await
        }
    }

    /// `[system, u1, a1, u2, a2]` (the old `load_transcript` fixture).
    fn transcript_with_system() -> Vec<ChatMessage> {
        vec![
            ChatMessage {
                role: ChatRole::System,
                content: MessageContent::Text("the prompt".to_string()),
                tool_call_id: None,
                tool_calls: None,
            },
            text_msg("u1"),
            assistant("a1"),
            text_msg("u2"),
            assistant("a2"),
        ]
    }

    /// `[u1, a1, u2, a2]` (the no-system-prompt `load_transcript` fixture).
    fn transcript_without_system() -> Vec<ChatMessage> {
        vec![
            text_msg("u1"),
            assistant("a1"),
            text_msg("u2"),
            assistant("a2"),
        ]
    }

    fn assistant(t: &str) -> ChatMessage {
        ChatMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text(t.to_string()),
            tool_call_id: None,
            tool_calls: None,
        }
    }
}

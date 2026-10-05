//! The context compactor (native-agent-harness Task 6): track
//! `context_tokens` (accumulate `Usage` from responses + a local
//! estimate of the `messages` vec) and signal when the context exceeds
//! `context_window - reserve_tokens` (the `reserveTokens` /
//! `keepRecentTokens` thresholds seeded from `settings.json`'s
//! `compaction` config via the `ModelCatalog` — default
//! `16384` / `20000` when absent). The summary model call itself is
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
}

impl Compactor {
    pub fn new(config: CompactionConfig, context_window: u32) -> Self {
        Self {
            config,
            context_window,
            context_tokens: 0,
        }
    }

    pub fn config(&self) -> CompactionConfig {
        self.config
    }

    /// The tracked context size (tokens).
    pub fn context_tokens(&self) -> u64 {
        self.context_tokens
    }

    /// Accumulate a response's usage (input + output).
    pub fn add_usage(&mut self, u: &Usage) {
        self.context_tokens += u64::from(u.input_tokens) + u64::from(u.output_tokens);
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

    /// Re-estimate the context from the (post-compaction) messages — the
    /// RESET path (after a compaction the transcript genuinely shrank, and
    /// a resume has no accumulated usage to keep, so the estimate
    /// REPLACES the accumulation, even downward).
    pub fn reestimate(&mut self, messages: &[ChatMessage]) {
        self.context_tokens = estimate_context(messages);
    }

    /// The usage-less fallback: re-estimate from the transcript as a
    /// FLOOR-RAISER — adopt the estimate only when it exceeds what is
    /// already known. A session on an endpoint that reports usage on SOME
    /// calls only would otherwise have its authoritative (larger)
    /// usage-based count replaced by the smaller `chars / 4` estimate on
    /// every gapped call — the context bar would move backwards and the
    /// compaction threshold metric would drop (compaction delayed while
    /// the session grows). Returns the tracked count.
    pub fn raise_context_from_estimate(&mut self, messages: &[ChatMessage]) -> u64 {
        self.context_tokens = self.context_tokens.max(estimate_context(messages));
        self.context_tokens
    }
}

/// The transcript's local token estimate (the sum over the messages).
///
/// `pub(crate)` so the `AgentLoop` can raise its DISPLAY metric with the
/// same number the threshold uses (one source for both).
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
    /// The keep-recent budget (the caller's `catalog.compaction`).
    pub keep_recent_tokens: u32,
    /// The session's compactor — RE-ESTIMATED on the compacted transcript
    /// (the post-compaction anchor: the accumulated usage is replaced by
    /// the estimate of what the context now is).
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
    match summarize(ctx, &older, turn).await {
        Ok(summary) => {
            let summary_msg = ChatMessage {
                role: ChatRole::System,
                content: MessageContent::Text(format!("{SUMMARY_PREFIX}\n{summary}")),
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
pub(crate) async fn summarize(
    ctx: &CompactionCtx<'_>,
    older: &[ChatMessage],
    turn: &CancellationToken,
) -> Result<String, ProviderError> {
    let system = ChatMessage {
        role: ChatRole::System,
        content: MessageContent::Text(
            "Summarize these messages concisely, preserving decisions, \
             file paths, and open tasks. Respond with the summary only."
                .to_string(),
        ),
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
            max_tokens: None,
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

    #[test]
    fn usage_accumulates_and_trips_the_threshold() {
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
        c.add_usage(&Usage {
            input_tokens: 500,
            output_tokens: 500,
        });
        assert_eq!(c.context_tokens(), 1000);
        assert!(c.should_compact(), "1000 > 1000 - 100");
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
        c.add_usage(&Usage {
            input_tokens: 99999,
            output_tokens: 99999,
        });
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
        c.add_usage(&Usage {
            input_tokens: 10,
            output_tokens: 10,
        });
        assert!(c.should_compact(), "20 > 0 (the saturated threshold)");
    }

    #[test]
    fn raise_context_from_estimate_never_lowers_the_accumulated_usage() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        c.add_usage(&Usage {
            input_tokens: 900,
            output_tokens: 900,
        });
        // A tiny transcript: the estimate is far below the accumulation.
        assert_eq!(
            c.raise_context_from_estimate(&[text_msg("short")]),
            1800,
            "the estimate is a floor-raiser, not a replacement"
        );
        assert_eq!(c.context_tokens(), 1800, "the accumulation survives");
        // And it DOES move when the transcript outgrows the count.
        let big = text_msg(&"y".repeat(40_000));
        let raised = c.raise_context_from_estimate(&[big]);
        assert!(
            raised > 1800,
            "a transcript larger than the count is adopted, got {raised}"
        );
    }

    #[test]
    fn reestimate_replaces_the_accumulated_usage() {
        let mut c = Compactor::new(
            CompactionConfig {
                enabled: true,
                reserve_tokens: 100,
                keep_recent_tokens: 10,
            },
            1000,
        );
        c.add_usage(&Usage {
            input_tokens: 900,
            output_tokens: 900,
        });
        c.reestimate(&[text_msg("short")]);
        assert!(c.context_tokens() < 100, "the estimate replaces the usage");
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
            compactor: &mut compactor,
        };
        summarize(&ctx, older, &CancellationToken::new()).await
    }

    /// The moved tests' fixture: the ctx's inputs + the mutable transcript
    /// (what the `AgentLoop` owned). `keep_recent_tokens: 1` mirrors the
    /// old tests' `loop_.catalog.compaction.keep_recent_tokens = 1`.
    struct Harness<'a> {
        model: Model,
        store: Arc<dyn Store>,
        compactor: Compactor,
        pub messages: Vec<ChatMessage>,
        provider: &'a dyn Provider,
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
            }
        }

        async fn run(&mut self) -> CompactionOutcome {
            let mut ctx = CompactionCtx {
                session_id: "s1",
                model: &self.model,
                provider: self.provider,
                store: &self.store,
                keep_recent_tokens: 1,
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

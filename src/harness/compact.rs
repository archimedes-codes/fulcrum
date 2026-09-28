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

    /// Re-estimate the context from the (post-compaction) messages.
    pub fn reestimate(&mut self, messages: &[ChatMessage]) {
        self.context_tokens = messages
            .iter()
            .map(estimate_message_tokens)
            .map(u64::from)
            .sum();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::provider::ChatRole;

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
}

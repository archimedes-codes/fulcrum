//! The `ask`-for-user flow and its result shapers (extracted from
//! `r#loop` in the loop.rs decomposition): the in-process interactive
//! `ask` method (a `pending_bridge` oneshot, the `interactive-request`
//! event, NO deadline) plus the suite's `shapeAskResult`,
//! `responseToResults`, and `buildAskSessionContent` shaping. No loop
//! state — the flow takes the bridge, the sink, and the session id.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::interactive::{interactive_key, PendingInteractive};
use crate::agent::tools::{ContentBlock, ToolResult};

/// The in-process `ask` flow (the `ask` interactive method's flow — a
/// `pending_bridge` oneshot + `interactive-request` event): the user's
/// answer is shaped per the suite's `shapeAskResult` (a cancel → the
/// cancelled shape; otherwise the `User answers` content).
///
/// NO DEADLINE (the suite's own `timeoutMs: 300_000` is deliberately NOT
/// carried over): the waiter ends when the PERSON answers or the turn is
/// cancelled — never when a clock runs out. A 5-minute cap answered
/// `"User cancelled the question."` while the modal was still on screen,
/// so stepping away for a coffee silently threw the question away and left
/// a modal answering into the void.
pub(crate) async fn ask_flow(
    pending_bridge: &PendingInteractive,
    sink: &Arc<dyn EventSink>,
    session_id: &str,
    params: &Value,
    request_id: &str,
    turn: &CancellationToken,
) -> ToolResult {
    let key = interactive_key(session_id, request_id);
    let (tx, rx) = oneshot::channel();
    {
        let mut map = pending_bridge.lock().await;
        map.insert(key.clone(), tx);
    }
    sink.emit(
        "interactive-request",
        json!({
            "sessionId": session_id,
            "requestId": request_id,
            "method": "ask",
            "source": "native",
            "toolCallId": Value::Null,
            "params": params,
        }),
    );
    // The two ways this ends: the user answers, or the turn is cancelled
    // (a Stop). There is NO timeout arm — see the flow's doc.
    let response = tokio::select! {
        r = rx => r.ok(),
        _ = turn.cancelled() => None,
    };
    pending_bridge.lock().await.remove(&key);
    shape_ask_result(response, params)
}

/// One question's answer (the suite's `QuestionResult` shape —
/// `packages/ask/src/tool.ts`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AskQuestionResult {
    id: String,
    question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    options: Vec<String>,
    multi: bool,
    selected: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custom: Option<String>,
}

/// The suite's `responseToResults` (`packages/ask/src/tool.ts:192`): the
/// `AskResponsePayload` (`{ cancelled, results: [{ id, selectedOptions,
/// customInput? }] }`) → one `QuestionResult` per question (a missing
/// `results[i]` is an empty answer).
fn response_to_results(response: Option<&Value>, questions: &[Value]) -> Vec<AskQuestionResult> {
    questions
        .iter()
        .enumerate()
        .map(|(i, q)| {
            let r = response
                .and_then(|r| r.get("results"))
                .and_then(Value::as_array)
                .and_then(|a| a.get(i));
            let options = q
                .get("options")
                .and_then(Value::as_array)
                .map(|opts| {
                    opts.iter()
                        .map(|o| {
                            o.get("label")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string()
                        })
                        .collect()
                })
                .unwrap_or_default();
            AskQuestionResult {
                id: q
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                question: q
                    .get("question")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                description: q
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .filter(|s| !s.trim().is_empty()),
                options,
                multi: q.get("multi").and_then(Value::as_bool).unwrap_or(false),
                selected: r
                    .and_then(|r| r.get("selectedOptions"))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
                custom: r
                    .and_then(|r| r.get("customInput"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .filter(|s| !s.is_empty()),
            }
        })
        .collect()
}

/// The suite's `shapeAskResult` (a cancel — `cancelled: true` with no
/// selected options — → the cancelled shape; otherwise the `User
/// answers` content).
fn shape_ask_result(response: Option<Value>, params: &Value) -> ToolResult {
    let questions = params
        .get("questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let results = response_to_results(response.as_ref(), &questions);
    let all_empty = results.iter().all(|r| r.selected.is_empty());
    let cancelled = response
        .as_ref()
        .and_then(|r| r.get("cancelled"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let details = json!({
        "results": serde_json::to_value(&results).unwrap_or(Value::Array(Vec::new())),
        "customInput": Value::Null,
        "description": Value::Null,
    });
    if cancelled && all_empty {
        return ToolResult {
            content: vec![ContentBlock::Text {
                text: "User cancelled the question.".to_string(),
            }],
            details: Some(details),
            is_error: false,
        };
    }
    ToolResult {
        content: vec![ContentBlock::Text {
            text: build_ask_session_content(&results),
        }],
        details: Some(details),
        is_error: false,
    }
}

/// The suite's `buildAskSessionContent` (the summary + the per-question
/// context).
fn build_ask_session_content(results: &[AskQuestionResult]) -> String {
    let summary: Vec<String> = results
        .iter()
        .map(|r| format!("{}: {}", r.id, selection_summary(r)))
        .collect();
    let context: Vec<String> = results
        .iter()
        .enumerate()
        .map(|(i, r)| question_context(r, i))
        .collect();
    format!(
        "User answers:\n{}\n\nAnswer context:\n{}",
        summary.join("\n"),
        context.join("\n\n")
    )
}

/// The suite's `formatSelectionForSummary`.
fn selection_summary(r: &AskQuestionResult) -> String {
    let has_selected = !r.selected.is_empty();
    let has_custom = r.custom.is_some();
    if !has_selected && !has_custom {
        return "(cancelled)".to_string();
    }
    if has_selected && has_custom {
        let selected_part = if r.multi {
            format!("[{}]", r.selected.join(", "))
        } else {
            r.selected.first().cloned().unwrap_or_default()
        };
        return format!(
            "{selected_part} + Other: \"{}\"",
            r.custom.as_deref().unwrap_or("")
        );
    }
    if has_custom {
        return format!("\"{}\"", r.custom.as_deref().unwrap_or(""));
    }
    if r.multi {
        return format!("[{}]", r.selected.join(", "));
    }
    r.selected.first().cloned().unwrap_or_default()
}

/// The suite's `formatQuestionContext`.
fn question_context(r: &AskQuestionResult, index: usize) -> String {
    let mut lines = vec![
        format!("Question {} ({})", index + 1, r.id),
        format!("Prompt: {}", r.question),
    ];
    if let Some(d) = &r.description {
        lines.push("Context:".to_string());
        for line in d.split('\n') {
            lines.push(format!("  {line}"));
        }
    }
    lines.push("Options:".to_string());
    for (i, option) in r.options.iter().enumerate() {
        lines.push(format!("  {}. {option}", i + 1));
    }
    lines.push("Response:".to_string());
    let has_selected = !r.selected.is_empty();
    let has_custom = r.custom.is_some();
    if !has_selected && !has_custom {
        lines.push("  Selected: (cancelled)".to_string());
    } else {
        if has_selected {
            let selected_text = if r.multi {
                format!("[{}]", r.selected.join(", "))
            } else {
                r.selected.first().cloned().unwrap_or_default()
            };
            lines.push(format!("  Selected: {selected_text}"));
        }
        if has_custom {
            if !has_selected {
                lines.push("  Selected: Other (type your own)".to_string());
            }
            lines.push(format!(
                "  Custom input: {}",
                r.custom.as_deref().unwrap_or("")
            ));
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::r#loop::result_text;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;

    /// A no-op `EventSink` (the flow's `sink` seam).
    struct NoopSink;
    impl EventSink for NoopSink {
        fn emit(&self, _event: &str, _payload: Value) {}
    }

    /// A prompt waits for the PERSON, never for a clock. An unanswered
    /// question must not settle however far time moves: the old 300 s cap
    /// answered `"User cancelled the question."` while the modal was still
    /// on screen, so a user who stepped away came back to a question the
    /// app had already thrown away (and a modal answering into the void).
    #[tokio::test(start_paused = true)]
    async fn an_unanswered_question_never_settles_on_a_clock() {
        let pending: PendingInteractive = Arc::new(TokioMutex::new(HashMap::new()));
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let params = json!({
            "questions": [{ "id": "q1", "question": "Which?", "options": [{ "label": "A" }] }]
        });
        let key = interactive_key("s1", "r1");
        let (p, s, prm) = (pending.clone(), sink.clone(), params.clone());
        let turn = CancellationToken::new();
        let flow = tokio::spawn(async move { ask_flow(&p, &s, "s1", &prm, "r1", &turn).await });

        // The request is registered (the flow has reached its wait).
        for _ in 0..100 {
            if pending.lock().await.contains_key(&key) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            pending.lock().await.contains_key(&key),
            "the question is open"
        );

        // 20 minutes: four times the cap the flow used to carry.
        for _ in 0..20 {
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            !flow.is_finished(),
            "an unanswered question must not settle on a clock"
        );

        // And the late answer still lands.
        let tx = pending
            .lock()
            .await
            .remove(&key)
            .expect("the question is still pending");
        tx.send(
            json!({ "cancelled": false, "results": [{ "id": "q1", "selectedOptions": ["A"] }] }),
        )
        .expect("the flow is listening");
        let r = tokio::time::timeout(Duration::from_secs(1), flow)
            .await
            .expect("the answer resolves the flow")
            .expect("the flow did not panic");
        assert!(
            result_text(&r).contains("q1: A"),
            "the answered shape, not the cancelled one: {}",
            result_text(&r)
        );
    }

    #[test]
    fn shape_ask_result_cancelled() {
        let params = json!({
            "questions": [{ "id": "q1", "question": "Which?", "options": [{ "label": "A" }] }]
        });
        // A `cancelled: true` with no selections (or a missing response —
        // a timeout) → the cancelled shape.
        let r = shape_ask_result(
            Some(json!({ "cancelled": true, "results": [{ "id": "q1", "selectedOptions": [] }] })),
            &params,
        );
        assert_eq!(
            result_text(&r),
            "User cancelled the question.",
            "a cancel is the cancelled shape"
        );
        assert!(!r.is_error);
        let r = shape_ask_result(None, &params);
        assert_eq!(result_text(&r), "User cancelled the question.");
    }

    #[test]
    fn shape_ask_result_answered() {
        let params = json!({
            "questions": [{
                "id": "q1",
                "question": "Which framework?",
                "options": [{ "label": "Tauri" }, { "label": "Electron" }],
                "multi": false
            }]
        });
        let r = shape_ask_result(
            Some(json!({
                "cancelled": false,
                "results": [{ "id": "q1", "selectedOptions": ["Tauri"] }]
            })),
            &params,
        );
        let text = result_text(&r);
        assert!(
            text.starts_with("User answers:"),
            "the answered shape, got {text}"
        );
        assert!(text.contains("q1: Tauri"), "the summary line, got {text}");
        assert!(
            text.contains("Prompt: Which framework?"),
            "the context block, got {text}"
        );
        assert!(!r.is_error);
    }
}

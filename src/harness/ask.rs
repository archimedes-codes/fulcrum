//! The `ask`-for-user flow and its result shapers (extracted from
//! `r#loop` in the loop.rs decomposition): the in-process interactive
//! `ask` method (a `pending_bridge` oneshot, the `interactive-request`
//! event, NO deadline) plus the suite's `shapeAskResult`,
//! `responseToResults`, and `buildAskSessionContent` shaping. No loop
//! state — the flow takes the bridge, the sink, and the session id.

use std::collections::HashSet;
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
    // The ONE canonical shape, established before the payload is emitted or
    // stored. The model's raw tool arguments are NOT renderable: an option
    // that is a bare string has no `label` to read (so the card drew blank,
    // unanswerable rows), and a question with no `id` collapses every
    // per-question draft into one `drafts[undefined]` bucket.
    let params = normalize_ask_params(params);
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
    // The ANSWER is shaped from the same normalized list, so the `Options`
    // block and the persisted `details` name the labels the user was shown
    // rather than the empty strings the raw arguments parse to.
    shape_ask_result(response, &params)
}

/// The one canonical option shape: an OBJECT with a non-empty string `label`.
///
/// A bare string becomes `{"label": <trimmed text>}`; an object is kept as-is
/// (only its label trimmed) because 312 of the 395 options recorded in this
/// app's session store carry a `description` — a key the `ask` SCHEMA never
/// mentions and that the TUI renderer still shows, so dropping unknown keys
/// would silently lose content. An option that resolves to no label at all (a
/// blank string, or an object with no `label`) is `None`: it cannot be
/// rendered and cannot be picked, so it is dropped rather than shown blank.
fn canonical_option(option: &Value) -> Option<Value> {
    match option {
        Value::String(s) => {
            let label = s.trim();
            (!label.is_empty()).then(|| json!({ "label": label }))
        }
        Value::Object(m) => {
            let label = m.get("label").and_then(Value::as_str).map(str::trim)?;
            if label.is_empty() {
                return None;
            }
            let mut kept = m.clone();
            kept.insert("label".to_string(), json!(label));
            Some(Value::Object(kept))
        }
        _ => None,
    }
}

/// The `id` for one question: the supplied one when it is a usable string and
/// not already taken, else a positional `q{i}` (bumped past any collision).
///
/// The renderer keys per-question draft state by `id`, so a duplicate id is a
/// correctness bug, not cosmetics: two questions would share one draft and one
/// selection would appear on both. An explicit id wins its first occurrence (a
/// model that repeats an id gets the second one renumbered, since the answer
/// payload also resolves by `id` and the FIRST match is what a renderer finds).
fn question_id(question: &Value, index: usize, used: &mut HashSet<String>) -> String {
    if let Some(id) = question.get("id").and_then(Value::as_str) {
        let id = id.trim();
        if !id.is_empty() && used.insert(id.to_string()) {
            return id.to_string();
        }
    }
    let mut n = index;
    loop {
        let candidate = format!("q{n}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
        n += 1;
    }
}

/// Normalize an `ask` tool-call payload to the one shape every consumer reads.
///
/// WHY this exists: the payload an `ask` actually carries is the model's raw
/// tool arguments, and the parser read `option["label"]` unconditionally. This
/// app's own model sends options as bare STRINGS and omits the question `id`
/// (recovered from the desktop's session store: 66 recorded asks, 80
/// bare-string options, 19 questions with no `id`), and `Value::String`'s
/// `get("label")` is `None` — so every option became `""`, every submitted
/// `selectedOptions` was empty, and the modal looked broken. Leniency at the
/// boundary is the fix, applied ONCE so the interactive-request event, the
/// persisted `tool-call` row and its reload replay cannot disagree about what
/// was asked.
///
/// Rules: options are normalized (see [`canonical_option`]) and every question
/// is guaranteed a unique non-empty `id`; a `recommended` option index follows
/// its option when earlier options are dropped; a question entry that is not an
/// object is dropped (it has no prompt and no option list to draw, and the
/// renderer maps over `options`); a `questions` value that is not an array
/// becomes `[]`, which is what the tool result already reports for it.
///
/// IDEMPOTENT — a payload that is already canonical is returned unchanged, so
/// normalizing at the tool-call boundary and again here cannot drift. It takes
/// `&Value` and clones, so the caller's `ToolCall` is never mutated.
pub(crate) fn normalize_ask_params(params: &Value) -> Value {
    // Only an object carrying a `questions` key has anything to normalize;
    // inventing one would be a second lie about what was asked.
    let Value::Object(map) = params else {
        return params.clone();
    };
    let mut out = map.clone();
    let Some(questions) = map.get("questions") else {
        return Value::Object(out);
    };
    // A `questions` key that is not a list is unreadable: the renderer calls
    // `.map` on it and throws. The result shaper already reports ZERO
    // questions for such a payload, so emit what the result claims.
    let list = questions.as_array().cloned().unwrap_or_default();

    let mut used: HashSet<String> = HashSet::new();
    let mut normalized: Vec<Value> = Vec::with_capacity(list.len());
    for (index, entry) in list.iter().enumerate() {
        // A non-object entry is not a question anyone can answer.
        let Value::Object(question) = entry else {
            continue;
        };
        let mut q = question.clone();

        // Options, keeping the old-index -> new-index map so `recommended` can
        // follow its option across the shift.
        let raw_options = question
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut options: Vec<Value> = Vec::with_capacity(raw_options.len());
        let mut kept_at: Vec<Option<usize>> = Vec::with_capacity(raw_options.len());
        for option in &raw_options {
            match canonical_option(option) {
                Some(canonical) => {
                    kept_at.push(Some(options.len()));
                    options.push(canonical);
                }
                None => kept_at.push(None),
            }
        }
        q.insert("options".to_string(), Value::Array(options));

        if let Some(recommended) = q.get("recommended").and_then(Value::as_u64) {
            match kept_at.get(recommended as usize) {
                // Still there, possibly at a new index.
                Some(Some(new)) => {
                    q.insert("recommended".to_string(), json!(*new));
                }
                // The recommended option was the one dropped: nothing to
                // recommend, and a stale index would highlight a different one.
                Some(None) => {
                    q.remove("recommended");
                }
                // Out of range in the ORIGINAL request: not ours to reinterpret.
                None => {}
            }
        }

        let id = question_id(entry, index, &mut used);
        q.insert("id".to_string(), json!(id));
        normalized.push(Value::Object(q));
    }
    out.insert("questions".to_string(), Value::Array(normalized));
    Value::Object(out)
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
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::sync::Mutex as TokioMutex;

    /// A no-op `EventSink` (the flow's `sink` seam).
    struct NoopSink;
    impl EventSink for NoopSink {
        fn emit(&self, _event: &str, _payload: Value) {}
    }

    /// An `EventSink` that RECORDS (the ask payload the flow EMITS is what
    /// the renderer draws its card from, so the tests assert on it).
    struct CapturingSink {
        emitted: Arc<StdMutex<Vec<(String, Value)>>>,
    }
    impl EventSink for CapturingSink {
        fn emit(&self, event: &str, payload: Value) {
            self.emitted
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((event.to_string(), payload));
        }
    }

    /// Drive `ask_flow` to completion on the seams the flow already takes
    /// (the pending map + the sink): run it, answer the open question with
    /// `answer`, and return the `params` of the `interactive-request` the
    /// flow emitted plus the tool result.
    async fn drive(params: Value, answer: Value) -> (Value, ToolResult) {
        let pending: PendingInteractive = Arc::new(TokioMutex::new(HashMap::new()));
        let emitted = Arc::new(StdMutex::new(Vec::new()));
        let sink: Arc<dyn EventSink> = Arc::new(CapturingSink {
            emitted: emitted.clone(),
        });
        let turn = CancellationToken::new();
        let (p, s, prm) = (pending.clone(), sink.clone(), params.clone());
        let flow = tokio::spawn(async move { ask_flow(&p, &s, "s1", &prm, "r1", &turn).await });

        let key = interactive_key("s1", "r1");
        for _ in 0..100 {
            if pending.lock().await.contains_key(&key) {
                break;
            }
            tokio::task::yield_now().await;
        }
        let tx = pending
            .lock()
            .await
            .remove(&key)
            .expect("the question is open");
        tx.send(answer).expect("the flow is listening");
        let r = tokio::time::timeout(Duration::from_secs(1), flow)
            .await
            .expect("the answer resolves the flow")
            .expect("the flow did not panic");

        let emitted_params = {
            let guard = emitted.lock().unwrap_or_else(|p| p.into_inner());
            guard
                .iter()
                .find(|(event, _)| event == "interactive-request")
                .map(|(_, payload)| payload["params"].clone())
                .expect("the interactive-request was emitted")
        };
        (emitted_params, r)
    }

    /// The `questions` array of an emitted payload, as an array.
    fn questions_of(payload: &Value) -> Vec<Value> {
        payload["questions"].as_array().cloned().unwrap_or_default()
    }

    /// The `label` of every option of one emitted question.
    fn labels_of(question: &Value) -> Vec<String> {
        question["options"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|o| o["label"].as_str().unwrap_or_default().to_string())
            .collect()
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

    // -----------------------------------------------------------------------
    // The normalized ask payload.
    //
    // The payload a live `ask` ACTUALLY carries is the model's raw tool
    // arguments, and this app's own model sends options as bare STRINGS and
    // omits the question `id` entirely (recovered from the desktop's session
    // store: 66 recorded asks, 80 bare-string options, 19 questions with no
    // `id`). Parsing `{label}` off a `Value::String` yields `""`, so every
    // option became a blank unanswerable row, the submitted `selectedOptions`
    // were empty, and every `id`-less question bucketed into one
    // `drafts[undefined]`. The fix normalizes ONCE at the parser boundary so
    // there is exactly one canonical shape downstream — the interactive-request
    // event, the persisted `tool-call` display row and its reload replay all
    // read the same normalized payload.
    // -----------------------------------------------------------------------

    /// The one canonical option shape: an object with a `label`. A bare string
    /// becomes `{label}`; an object keeps its OTHER keys (312 of the recorded
    /// options carry a `description` the TUI renderer shows).
    const BARE_AND_LABELLED: &str = "Which framework?";

    /// A bare-string option list keeps its text in the payload the RENDERER
    /// draws from, and in the answer the tool builds from it.
    #[tokio::test]
    async fn a_bare_string_option_keeps_its_label_end_to_end() {
        let params = json!({
            "questions": [{
                "question": BARE_AND_LABELLED,
                "options": ["Tauri", "Electron"]
            }]
        });
        let (emitted, r) = drive(
            params,
            json!({ "cancelled": false, "results": [{ "selectedOptions": ["Tauri"] }] }),
        )
        .await;

        // EMITTED: the card reads `options[].label`, so that key must exist.
        let qs = questions_of(&emitted);
        assert_eq!(qs.len(), 1, "the question survived");
        assert_eq!(
            labels_of(&qs[0]),
            vec!["Tauri".to_string(), "Electron".to_string()],
            "the bare strings became `{{label}}` objects with their text intact, got {:?}",
            qs[0]
        );

        // ANSWERED: the answer-building side sees the real labels, so the
        // context block and the `details` the transcript persists name the
        // option the user actually picked.
        let text = result_text(&r);
        assert!(
            text.contains("Selected: Tauri"),
            "the selection carries the label, got {text}"
        );
        assert!(
            text.contains("1. Tauri"),
            "the Options block lists the real labels, got {text}"
        );
        let details = r.details.expect("the answered shape carries details");
        assert_eq!(
            details["results"][0]["options"],
            json!(["Tauri", "Electron"]),
            "the persisted details carry the real labels"
        );
    }

    /// No regression: the `{label}` shape the schema advertises still works,
    /// and keeps keys the schema does not mention (an option `description`).
    #[tokio::test]
    async fn a_label_object_option_still_works_and_keeps_its_other_keys() {
        let params = json!({
            "questions": [{
                "id": "q1",
                "question": BARE_AND_LABELLED,
                "options": [
                    { "label": "Tauri", "description": "Rust core" },
                    { "label": "Electron" }
                ]
            }]
        });
        let (emitted, r) = drive(
            params,
            json!({ "cancelled": false, "results": [{ "selectedOptions": ["Tauri"] }] }),
        )
        .await;

        let qs = questions_of(&emitted);
        assert_eq!(
            labels_of(&qs[0]),
            vec!["Tauri".to_string(), "Electron".to_string()],
            "the `{{label}}` shape is unchanged"
        );
        assert_eq!(
            qs[0]["options"][0]["description"],
            json!("Rust core"),
            "normalization must not silently drop the keys it does not understand"
        );
        assert!(
            result_text(&r).contains("Selected: Tauri"),
            "the answered shape, got {}",
            result_text(&r)
        );
    }

    /// The two shapes in ONE request (both are in the recorded store: 80 bare
    /// strings alongside 320 objects) — each option keeps its own text.
    #[tokio::test]
    async fn mixed_option_shapes_in_one_request_each_keep_their_label() {
        let params = json!({
            "questions": [{
                "id": "q1",
                "question": "Which?",
                "multi": true,
                "options": ["Bare", { "label": "Object" }, "Last"]
            }]
        });
        let (emitted, r) = drive(
            params,
            json!({ "cancelled": false, "results": [{ "selectedOptions": ["Bare", "Object"] }] }),
        )
        .await;

        assert_eq!(
            labels_of(&questions_of(&emitted)[0]),
            vec!["Bare".to_string(), "Object".to_string(), "Last".to_string()],
            "mixed shapes normalize without reordering or losing an option"
        );
        let text = result_text(&r);
        assert!(
            text.contains("Selected: [Bare, Object]"),
            "both labels survive into the answer, got {text}"
        );
    }

    /// An option that resolves to no label is DROPPED, not kept as a blank
    /// unanswerable row — and the question it came from survives, because an
    /// option-less question is still answerable free-form (the renderer's
    /// `Other` row).
    #[tokio::test]
    async fn an_option_with_no_label_is_dropped_and_its_question_survives() {
        let params = json!({
            "questions": [
                {
                    "id": "q1",
                    "question": "One blank option?",
                    "options": ["Real", "   ", { "note": "no label at all" }, ""]
                },
                {
                    "id": "q2",
                    "question": "Only blank options?",
                    "options": ["", "  \t "]
                }
            ]
        });
        let (emitted, r) = drive(
            params,
            json!({
                "cancelled": false,
                "results": [
                    { "selectedOptions": ["Real"] },
                    { "customInput": "free-form" }
                ]
            }),
        )
        .await;

        let qs = questions_of(&emitted);
        assert_eq!(qs.len(), 2, "NEITHER question is dropped");
        assert_eq!(
            labels_of(&qs[0]),
            vec!["Real".to_string()],
            "the three unanswerable options are gone, got {:?}",
            qs[0]
        );
        assert_eq!(
            qs[1]["options"],
            json!([]),
            "a question whose options all vanish keeps existing with an EMPTY option list"
        );
        let text = result_text(&r);
        assert!(
            text.contains("Custom input: free-form"),
            "the option-less question is answerable free-form, got {text}"
        );
    }

    /// A missing, empty or non-string `id` falls back to a POSITIONAL id that
    /// is unique within the request: the renderer keys per-question draft
    /// state by `id`, so a collision is a correctness bug (one selection
    /// appearing on every question), not cosmetics.
    #[tokio::test]
    async fn a_missing_empty_or_non_string_id_gets_a_unique_positional_id() {
        let params = json!({
            "questions": [
                { "question": "No id at all?", "options": ["a"] },
                { "id": "", "question": "Empty id?", "options": ["b"] },
                { "id": 7, "question": "Non-string id?", "options": ["c"] },
                { "id": "   ", "question": "Blank id?", "options": ["d"] },
                { "id": "explicit", "question": "Real id?", "options": ["e"] }
            ]
        });
        let (emitted, _) = drive(params, json!({ "cancelled": false, "results": [] })).await;

        let qs = questions_of(&emitted);
        assert_eq!(qs.len(), 5);
        let ids: Vec<&str> = qs
            .iter()
            .map(|q| q["id"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(
            ids,
            vec!["q0", "q1", "q2", "q3", "explicit"],
            "every question has a non-empty id, positional where the request gave none"
        );
        let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "the ids are unique — two questions may NOT share a draft bucket: {ids:?}"
        );
    }

    /// A positional id must not silently COLIDE with an id the request did
    /// supply (`q0` as a literal id is in the model's vocabulary), or the two
    /// questions share one draft bucket again.
    #[tokio::test]
    async fn a_positional_id_never_collides_with_an_explicit_one() {
        let params = json!({
            "questions": [
                { "question": "First (no id)?", "options": ["a"] },
                { "id": "q0", "question": "Explicitly named q0?", "options": ["b"] }
            ]
        });
        let (emitted, _) = drive(params, json!({ "cancelled": false, "results": [] })).await;

        let emitted_qs = questions_of(&emitted);
        let ids: Vec<&str> = emitted_qs
            .iter()
            .map(|q| q["id"].as_str().unwrap_or_default())
            .collect();
        let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            2,
            "the fallback stepped aside rather than duplicating the explicit `q0`: {ids:?}"
        );
    }

    /// Normalization is IDEMPOTENT — a payload that already carries `{label}`
    /// options and real ids is returned byte-for-byte, so normalizing at the
    /// tool-call boundary and again in the flow cannot drift.
    #[test]
    fn normalizing_an_already_canonical_payload_changes_nothing() {
        let canonical = json!({
            "questions": [{
                "id": "q1",
                "question": "Which?",
                "multi": true,
                "recommended": 1,
                "description": "context",
                "options": [{ "label": "A", "description": "d" }, { "label": "B" }]
            }]
        });
        assert_eq!(normalize_ask_params(&canonical), canonical, "idempotent");
        let once = normalize_ask_params(&json!({
            "questions": [{ "question": "Which?", "options": ["A", "", "B"] }]
        }));
        assert_eq!(
            normalize_ask_params(&once),
            once,
            "idempotent after a change"
        );
    }

    /// Anything that is not a question LIST passes through untouched (there is
    /// nothing to normalize, and inventing a `questions` key would be a second
    /// lie).
    #[test]
    fn a_payload_without_a_question_list_is_left_alone() {
        let none = json!({ "method": "ask" });
        assert_eq!(normalize_ask_params(&none), none);
        assert_eq!(normalize_ask_params(&Value::Null), Value::Null);
        assert_eq!(
            normalize_ask_params(&json!(["not", "an", "object"])),
            json!(["not", "an", "object"]),
            "a non-object payload is passed through"
        );
    }

    /// A `questions` key that is present but is not an array is a LIE, and the
    /// renderer would call `.map` on it (`questions.map` on a string throws).
    /// The tool result already reads such a payload as ZERO questions
    /// (`as_array().unwrap_or_default()` → the cancelled shape), so the
    /// emitted payload is normalized to the same empty list the result claims.
    #[test]
    fn a_non_array_question_list_is_normalized_to_the_empty_list_the_result_already_says() {
        assert_eq!(
            normalize_ask_params(&json!({ "questions": "not an array" })),
            json!({ "questions": [] })
        );
    }

    /// A question ENTRY that is not an object has no prompt, no options and no
    /// id slot — it is not a question the user can answer, and
    /// `question.options.map` on it would throw. It is dropped (a request whose
    /// entries are all such is the same request with no questions).
    #[test]
    fn a_question_entry_that_is_not_an_object_is_dropped() {
        assert_eq!(
            normalize_ask_params(&json!({ "questions": ["a bare string question"] })),
            json!({ "questions": [] })
        );
        assert_eq!(
            normalize_ask_params(&json!({
                "questions": ["dropped", { "id": "q1", "question": "Real?", "options": [] }]
            })),
            json!({ "questions": [{ "id": "q1", "question": "Real?", "options": [] }] })
        );
    }

    /// Normalizing an empty / malformed questions list does not invent one.
    #[test]
    fn an_empty_question_list_stays_empty() {
        let empty = json!({ "questions": [] });
        assert_eq!(normalize_ask_params(&empty), empty);
    }

    /// `recommended` is an option INDEX the renderer highlights. Dropping an
    /// option shifts the indices, so the recommendation must follow its option
    /// (15 of the recorded questions carry `recommended`; a stale index would
    /// silently highlight a DIFFERENT option).
    #[tokio::test]
    async fn dropping_an_option_keeps_the_recommended_index_on_its_option() {
        let params = json!({
            "questions": [{
                "id": "q1",
                "question": "Recommended after a drop?",
                "recommended": 2,
                "options": ["", "Second", "Keep me"]
            }]
        });
        let (emitted, _) = drive(
            params,
            json!({ "cancelled": false, "results": [{ "selectedOptions": ["Keep me"] }] }),
        )
        .await;

        let q = &questions_of(&emitted)[0];
        assert_eq!(
            labels_of(q),
            vec!["Second".to_string(), "Keep me".to_string()]
        );
        assert_eq!(
            q["recommended"],
            json!(1),
            "the recommendation followed its option from index 2 to index 1, got {q:?}"
        );
    }

    /// And when the recommended option is itself the one dropped, there is
    /// nothing left to recommend.
    #[tokio::test]
    async fn a_recommendation_on_a_dropped_option_is_removed() {
        let params = json!({
            "questions": [{
                "id": "q1",
                "question": "Recommended option is blank?",
                "recommended": 0,
                "options": ["", "Still here"]
            }]
        });
        let (emitted, _) = drive(
            params,
            json!({ "cancelled": false, "results": [{ "selectedOptions": ["Still here"] }] }),
        )
        .await;

        let q = &questions_of(&emitted)[0];
        assert!(
            q.get("recommended").is_none(),
            "a recommendation pointing at a dropped option is removed, got {q:?}"
        );
    }

    /// Normalization must not mutate the CALLER's payload (the `ToolCall` the
    /// transcript already holds is the same value).
    #[test]
    fn normalizing_does_not_mutate_its_input() {
        let raw = json!({ "questions": [{ "question": "Which?", "options": ["A"] }] });
        let before = raw.clone();
        let _ = normalize_ask_params(&raw);
        assert_eq!(
            raw, before,
            "the raw arguments the model sent are untouched"
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

    /// The advertised `ask` schema declares `options.items` as
    /// `{"type": "object"}` — which FORBIDS the bare-string form this app's
    /// own model actually produces (80 recorded options). The parser now
    /// accepts both, so the schema must not claim the string form is invalid:
    /// an invalid-looking payload is what the model is being told not to send
    /// while the app silently accepts it anyway.
    /// The advertised `ask` schema declares `options.items` as a bare
    /// `{"type": "object"}` — which FORBIDS the bare-string form this app's own
    /// model actually produces (80 recorded options in the production DB).
    /// The parser now accepts both, so the schema must not claim the string
    /// form is invalid: as written, the model is told not to send the very
    /// shape the app has learned to accept.
    #[test]
    fn the_ask_schema_does_not_forbid_the_bare_string_option_form() {
        let spec = crate::agent::harness::r#loop::tool_specs()
            .into_iter()
            .find(|t| t.name == "ask")
            .expect("the `ask` tool is advertised");
        // .../properties/questions/items/properties/options/items
        let options = &spec.parameters["properties"]["questions"]["items"]["properties"]["options"];
        assert!(
            options.is_object(),
            "the probe path resolves to the `options` schema, got {options}"
        );
        let items = &options["items"];
        assert!(
            items.is_object(),
            "`options.items` is a schema object, got {items}"
        );
        // A bare `"type": "object"` is the forbid-the-string-form shape.
        assert_ne!(
            items.get("type").and_then(Value::as_str),
            Some("object"),
            "`options.items` must not be a bare object-only type, which forbids \
             the string form the parser accepts, got {items}"
        );
        // And it must POSITIVELY admit the string form (so the test cannot pass
        // by the schema going missing).
        let admits_string = items
            .get("type")
            .and_then(Value::as_array)
            .is_some_and(|t| t.iter().any(|v| v.as_str() == Some("string")))
            || items
                .get("anyOf")
                .and_then(Value::as_array)
                .is_some_and(|opts| {
                    opts.iter()
                        .any(|o| o.get("type").and_then(Value::as_str) == Some("string"))
                })
            || items.get("type").and_then(Value::as_str) == Some("string");
        assert!(
            admits_string,
            "`options.items` must accept a string option, got {items}"
        );
    }
}

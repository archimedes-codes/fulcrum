//! The seven task-board executors (issue #180 F1): the model-facing verbs for the
//! board and for named markdown artifacts.
//!
//! ## Why these are `execute_tool` arms and not loop-internal tools
//!
//! ADR 0092's rule, and the concrete case is a batch: turning an approved backlog
//! into N tasks with a spec each is ONE codemode script with a loop, and codemode
//! reaches a tool only through `execute_tool` on a cloned [`ToolCtx`]. A
//! loop-internal tool is invisible to it (the third layer `EXCLUDED_FROM_SCRIPT`'s
//! doc describes), which is exactly why these seven are NOT in that list.
//!
//! ## Every arm is the same five parts
//!
//! 1. Take the seam handle — `None` is a `fail` naming the reason, never a panic.
//! 2. Validate the params, naming the offending key (the arms read EXACTLY the keys
//!    `exec_param_keys()` census-claims; `tool_specs()` advertises the same set).
//! 3. Park on the seam, RACED against the turn token (`ctx.cancel`) with the
//!    two-armed `tokio::select!` shape `dispatch_tool`'s loop-internal arms use.
//!    ADR 0092's two rules are enforced one level down, inside
//!    [`IpcTaskStore`](crate::worker::task_store::IpcTaskStore): the registration
//!    order and the `Drop` cleanup. What the ARM owns is the token vote — an arm
//!    that awaited the seam without racing `ctx.cancel` would hang a Stop.
//! 4. Decode the answer into the DTO the verb returns.
//! 5. Shape the result: `content` is what the model sees, `details` is what the UI
//!    renders (the keys ride into the desktop's `toolDetailsRegistry` reader in D2).
//!
//! ## No caps here, on purpose
//!
//! Nothing is truncated or refused by size. The 256 KiB rule is the desktop's
//! command-layer rule (D1/D2) — the crate round-trips whatever the wire carries, and
//! `a_256_kib_markdown_round_trips_byte_for_byte_through_the_executor` pins that at
//! the real size. A crate-side cap would be a second, drift-prone copy of a rule the
//! storage layer already owns.

use serde_json::{json, Value};

use crate::seams::{
    Artifact, ArtifactMeta, BoardRow, MoveOutcome, TaskCall, TaskCaller, TaskDetail, TaskRow,
    TaskStoreError,
};
use crate::tools::types::{ContentBlock, ToolCtx, ToolResult};

/// The cancelled shape — `loop::tools::cancelled_tool_result`'s, re-derived here
/// because that function is `pub(super)` inside `harness::r#loop` and unreachable
/// from this module (the same constraint that made `ToolResult::torn_down` live in
/// `types.rs`). ONE sentence, and it is the sentence every other cancelled tool
/// result in the crate uses, so a model cannot tell a task arm from a todo arm.
fn cancelled() -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text {
            text: "cancelled".to_string(),
        }],
        details: None,
        is_error: true,
    }
}

/// The `None`-handle answer. A task tool cannot work without the seam, and the
/// reason is worth naming: an in-process harness loop has no Supervisor to ask.
fn no_store() -> ToolResult {
    ToolResult::fail(
        "no task store: task tools run worker-side over the TaskStore seam".to_string(),
        None,
    )
}

/// A missing or wrongly-typed string parameter, naming the KEY (a model that gets
/// `spacePath` back can fix it; one that gets `invalid params` cannot).
fn missing(key: &str) -> ToolResult {
    ToolResult::fail(format!("missing or invalid `{key}` string"), None)
}

/// Read a REQUIRED string parameter.
fn req_str<'a>(params: &'a Value, key: &str) -> Result<&'a str, ToolResult> {
    params
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| missing(key))
}

/// Read an OPTIONAL string parameter. `Ok(None)` covers BOTH "absent" and
/// "explicit `null`" — the two spellings the wire can carry for "no value", and the
/// seam treats them identically (mirrors `TaskCall`'s `#[serde(default)]`).
///
/// A present-but-wrong-typed value is an ERROR naming the key, not a silent `None`:
/// `body: 42` means the model meant to send something and sent the wrong thing, and
/// quietly dropping it would write a task without the body it was asked for.
fn opt_str<'a>(params: &'a Value, key: &str) -> Result<Option<&'a str>, ToolResult> {
    match params.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v.as_str().map(Some).ok_or_else(|| missing(key)),
    }
}

/// The model-facing sentence for a seam error.
///
/// A private helper rather than `impl Display for TaskStoreError`, deliberately:
/// `TaskStoreError`'s derive list is part of the cross-repo `## Provides` contract
/// (`Debug, Clone, PartialEq, Serialize, Deserialize`), and a `Display` impl — additive
/// though it would be — is a change to a type the OTHER repo implements against. The
/// `message` is already the diagnosis in all three variants (the desktop composes it:
/// "no task tsk_9", "artifact key `a b` is not a slug"), and the kind is already
/// visible in the `details` of a failed call, so nothing is lost by not prefixing it.
fn error_text(e: &TaskStoreError) -> String {
    match e {
        TaskStoreError::Io { message }
        | TaskStoreError::NotFound { message }
        | TaskStoreError::Invalid { message } => message.clone(),
    }
}

/// Park on the seam, racing the TURN token. Every arm goes through here, so the
/// token vote cannot be forgotten in one arm and present in six.
async fn parked(
    store: &dyn TaskCaller,
    call: TaskCall,
    ctx: &ToolCtx,
) -> Result<Value, ToolResult> {
    let fut = store.call(call);
    tokio::select! {
        r = fut => r,
        _ = ctx.cancel.cancelled() => return Err(cancelled()),
    }
    .map_err(|e| ToolResult::fail(error_text(&e), None))
}

/// The board row's one-line summary (title, number, stage, artifact KEY names).
/// Never any markdown — the row does not carry any, and this is the sentence that
/// tells the model to go fetch one.
fn board_line(r: &BoardRow) -> String {
    let artifacts = if r.artifact_keys.is_empty() {
        "no artifacts".to_string()
    } else {
        format!("artifacts: {}", r.artifact_keys.join(", "))
    };
    format!(
        "#{} [{}] {} — {} ({})",
        r.number, r.stage_slug, r.id, r.title, artifacts
    )
}

/// `list_board` — one line per card, plus the rows in `details` for the UI.
///
/// `params`: `{ space_path: String }`.
pub(super) async fn exec_list_board(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let space_path = match req_str(params, "space_path") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let value = match parked(store.as_ref(), TaskCall::ListBoard { space_path }, ctx).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let rows: Vec<BoardRow> = match serde_json::from_value(value) {
        Ok(rows) => rows,
        Err(e) => return ToolResult::fail(format!("task response was not a board: {e}"), None),
    };
    if rows.is_empty() {
        return ToolResult::ok_text(
            "the board is empty".to_string(),
            Some(json!({ "board": rows })),
        );
    }
    let text = rows.iter().map(board_line).collect::<Vec<_>>().join("\n");
    ToolResult::ok_text(text, Some(json!({ "board": rows })))
}

/// `get_task` — one task's full row.
///
/// `params`: `{ task_id: String }`.
pub(super) async fn exec_get_task(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let task_id = match req_str(params, "task_id") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let value = match parked(
        store.as_ref(),
        TaskCall::GetTask {
            task_id: task_id.clone(),
        },
        ctx,
    )
    .await
    {
        Ok(v) => v,
        Err(r) => return r,
    };
    let task: Option<TaskDetail> = match serde_json::from_value(value) {
        Ok(t) => t,
        Err(e) => return ToolResult::fail(format!("task response was not a task: {e}"), None),
    };
    match task {
        Some(t) => ToolResult::ok_text(
            format!(
                "#{} [{}] {} — {}{}\nbody: {}",
                t.number,
                t.stage_slug,
                t.id,
                t.title,
                if t.artifact_keys.is_empty() {
                    String::new()
                } else {
                    format!(" (artifacts: {})", t.artifact_keys.join(", "))
                },
                t.body.as_deref().unwrap_or("(none)"),
            ),
            Some(json!({ "task": t })),
        ),
        // Asking for what is not there is an ANSWER, not an error: `None` is what
        // the seam returns for a missing row, and an `is_error` result invites the
        // model to retry the same id.
        None => ToolResult::ok_text(format!("no task found for `{task_id}`"), None),
    }
}

/// The shared shaping for the two write verbs' `TaskRow` answer.
fn row_result(verb: &str, row: TaskRow) -> ToolResult {
    ToolResult::ok_text(
        format!(
            "{verb} #{} [{}] {} — {}",
            row.number, row.space_path, row.id, row.title
        ),
        Some(json!({ "task": row })),
    )
}

/// `create_task` — a new card on a Space's board.
///
/// `params`: `{ space_path, title }` required; `body`, `stage_slug` optional.
pub(super) async fn exec_create_task(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let space_path = match req_str(params, "space_path") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let title = match req_str(params, "title") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let body = match opt_str(params, "body") {
        Ok(v) => v.map(str::to_string),
        Err(r) => return r,
    };
    let stage_slug = match opt_str(params, "stage_slug") {
        Ok(v) => v.map(str::to_string),
        Err(r) => return r,
    };
    let value = match parked(
        store.as_ref(),
        TaskCall::CreateTask {
            space_path,
            title,
            body,
            stage_slug,
        },
        ctx,
    )
    .await
    {
        Ok(v) => v,
        Err(r) => return r,
    };
    let row: TaskRow = match serde_json::from_value(value) {
        Ok(r) => r,
        Err(e) => return ToolResult::fail(format!("task response was not a task row: {e}"), None),
    };
    row_result("created", row)
}

/// `edit_task` — change a card's title / body / spec path.
///
/// `params`: `{ task_id }` required; `title`, `body`, `spec_path` optional.
pub(super) async fn exec_edit_task(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let task_id = match req_str(params, "task_id") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let title = match opt_str(params, "title") {
        Ok(v) => v.map(str::to_string),
        Err(r) => return r,
    };
    let body = match opt_str(params, "body") {
        Ok(v) => v.map(str::to_string),
        Err(r) => return r,
    };
    let spec_path = match opt_str(params, "spec_path") {
        Ok(v) => v.map(str::to_string),
        Err(r) => return r,
    };
    let value = match parked(
        store.as_ref(),
        TaskCall::EditTask {
            task_id,
            title,
            body,
            spec_path,
        },
        ctx,
    )
    .await
    {
        Ok(v) => v,
        Err(r) => return r,
    };
    let row: TaskRow = match serde_json::from_value(value) {
        Ok(r) => r,
        Err(e) => return ToolResult::fail(format!("task response was not a task row: {e}"), None),
    };
    row_result("edited", row)
}

/// `move_task` — move a card to another stage.
///
/// `params`: `{ space_path, task_id, to_stage }`, all required.
pub(super) async fn exec_move_task(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let space_path = match req_str(params, "space_path") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let task_id = match req_str(params, "task_id") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let to_stage = match req_str(params, "to_stage") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let value = match parked(
        store.as_ref(),
        TaskCall::MoveTask {
            space_path,
            task_id,
            to_stage: to_stage.clone(),
        },
        ctx,
    )
    .await
    {
        Ok(v) => v,
        Err(r) => return r,
    };
    let outcome: MoveOutcome = match serde_json::from_value(value) {
        Ok(o) => o,
        Err(e) => {
            return ToolResult::fail(format!("task response was not a move outcome: {e}"), None)
        }
    };
    // `on_enter` is deliberately NOT in the model-facing text: it is opaque data
    // for the DESKTOP's command layer (the boost needs the `SessionManager`, which
    // the crate cannot name), and describing it here would be the crate claiming to
    // know what it means. It still rides `details` so D2's reader can act on it.
    let mut text = format!("moved to `{}`", outcome.to_stage);
    if let Some(note) = outcome.note.as_deref() {
        text.push_str(&format!(" — {note}"));
    }
    ToolResult::ok_text(text, Some(json!({ "moveOutcome": outcome })))
}

/// `get_artifact` — one artifact's full markdown.
///
/// `params`: `{ task_id, key }`, both required.
pub(super) async fn exec_get_artifact(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let task_id = match req_str(params, "task_id") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let key = match req_str(params, "key") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let value = match parked(
        store.as_ref(),
        TaskCall::GetArtifact {
            task_id: task_id.clone(),
            key: key.clone(),
        },
        ctx,
    )
    .await
    {
        Ok(v) => v,
        Err(r) => return r,
    };
    let artifact: Option<Artifact> = match serde_json::from_value(value) {
        Ok(a) => a,
        Err(e) => return ToolResult::fail(format!("task response was not an artifact: {e}"), None),
    };
    match artifact {
        // The markdown IS the content — this is the whole point of the seam. No
        // `details`: the markdown would then exist TWICE in one result (the
        // transcript row and the UI payload both persist), for a document the model
        // is about to hold in full anyway.
        Some(a) => ToolResult::ok_text(a.markdown, None),
        None => ToolResult::ok_text(format!("no artifact `{key}` for task `{task_id}`"), None),
    }
}

/// `write_artifact` — upsert one artifact, answering with its META only.
///
/// `params`: `{ task_id, key, markdown }`, all required.
pub(super) async fn exec_write_artifact(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(store) = ctx.task_store.as_ref() else {
        return no_store();
    };
    let task_id = match req_str(params, "task_id") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let key = match req_str(params, "key") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let markdown = match req_str(params, "markdown") {
        Ok(v) => v.to_string(),
        Err(r) => return r,
    };
    let value = match parked(
        store.as_ref(),
        TaskCall::WriteArtifact {
            task_id,
            key,
            markdown,
        },
        ctx,
    )
    .await
    {
        Ok(v) => v,
        Err(r) => return r,
    };
    let meta: ArtifactMeta = match serde_json::from_value(value) {
        Ok(m) => m,
        Err(e) => {
            return ToolResult::fail(format!("task response was not an artifact meta: {e}"), None)
        }
    };
    // The meta, never the markdown back: the caller just sent it.
    ToolResult::ok_text(
        format!(
            "wrote artifact `{}` ({} bytes, updatedAt {})",
            meta.key, meta.bytes, meta.updated_at
        ),
        Some(json!({ "artifactMeta": meta })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    /// The seam double: answers from a canned map keyed by verb name, records every
    /// [`TaskCall`] it was asked for, and can HANG (the shape of a Supervisor that
    /// never answers — the F1→D2 window, and the only way to exercise a cancel).
    struct FakeTaskCaller {
        canned: Mutex<HashMap<String, Result<Value, TaskStoreError>>>,
        calls: Mutex<Vec<TaskCall>>,
        /// How many calls are parked RIGHT NOW. The hang path increments on entry
        /// and a Drop guard decrements when the parked future is dropped, which is
        /// what lets a cancel test assert the arm really abandoned the call.
        parked_now: Arc<AtomicUsize>,
        hang: bool,
    }

    /// Decrements the parked counter when the parked future is dropped (the fake's
    /// stand-in for `TaskRequestCleanup`, so the fake can report a leak).
    struct ParkGuard(Arc<AtomicUsize>);
    impl Drop for ParkGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl FakeTaskCaller {
        fn new() -> Self {
            Self {
                canned: Mutex::new(HashMap::new()),
                calls: Mutex::new(Vec::new()),
                parked_now: Arc::new(AtomicUsize::new(0)),
                hang: false,
            }
        }
        fn hanging() -> Self {
            Self {
                hang: true,
                ..Self::new()
            }
        }
        fn answer(&mut self, verb: &str, value: Value) {
            self.canned
                .get_mut()
                .unwrap()
                .insert(verb.to_string(), Ok(value));
        }
        fn fail_with(&mut self, verb: &str, e: TaskStoreError) {
            self.canned
                .get_mut()
                .unwrap()
                .insert(verb.to_string(), Err(e));
        }
        fn calls(&self) -> Vec<TaskCall> {
            self.calls.lock().unwrap().clone()
        }
        fn parked_now(&self) -> usize {
            self.parked_now.load(Ordering::SeqCst)
        }
    }

    /// The verb name a call is answered under (the fake's key — a fake keyed on the
    /// whole `TaskCall` would have to be told the arguments it is being asked about,
    /// which would make the param tests tautological).
    fn verb_of(call: &TaskCall) -> &'static str {
        match call {
            TaskCall::ListBoard { .. } => "listBoard",
            TaskCall::GetTask { .. } => "getTask",
            TaskCall::CreateTask { .. } => "createTask",
            TaskCall::EditTask { .. } => "editTask",
            TaskCall::MoveTask { .. } => "moveTask",
            TaskCall::GetArtifact { .. } => "getArtifact",
            TaskCall::WriteArtifact { .. } => "writeArtifact",
        }
    }

    impl TaskCaller for FakeTaskCaller {
        fn call(
            &self,
            call: TaskCall,
        ) -> Pin<Box<dyn Future<Output = Result<Value, TaskStoreError>> + Send>> {
            self.calls.lock().unwrap().push(call.clone());
            let verb = verb_of(&call).to_string();
            let canned = self
                .canned
                .lock()
                .unwrap()
                .get(&verb)
                .cloned()
                .unwrap_or_else(|| {
                    Err(TaskStoreError::Io {
                        message: format!("the fake has no canned `{verb}` answer"),
                    })
                });
            if self.hang {
                let guard_count = self.parked_now.clone();
                return Box::pin(async move {
                    // Increment INSIDE the body, mirroring `IpcTaskStore`, which
                    // registers its pending entry inside the async block: a future
                    // that is never polled has registered nothing, and a counter
                    // bumped in the sync half here would report a phantom leak.
                    guard_count.fetch_add(1, Ordering::SeqCst);
                    let _guard = ParkGuard(guard_count);
                    // Never resolves: the caller's turn token is the only exit.
                    std::future::pending::<()>().await;
                    Err(TaskStoreError::Io {
                        message: "unreachable".into(),
                    })
                });
            }
            Box::pin(async move { canned })
        }
    }

    /// A ctx carrying the fake (the production wiring is
    /// `AgentLoop::set_task_store` → `to_tool_ctx`; here it is set directly).
    fn ctx_with(store: Option<Arc<dyn TaskCaller>>) -> ToolCtx {
        let mut ctx = crate::tools::exec::tests::support::ctx(std::path::Path::new("/tmp"));
        ctx.task_store = store;
        ctx
    }

    fn ctx_hanging() -> (ToolCtx, Arc<FakeTaskCaller>) {
        let fake = Arc::new(FakeTaskCaller::hanging());
        let ctx = ctx_with(Some(fake.clone()));
        (ctx, fake)
    }

    fn text(r: &ToolResult) -> String {
        r.content
            .iter()
            .map(|b| match b {
                ContentBlock::Text { text } => text.clone(),
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join("")
    }

    fn a_row() -> BoardRow {
        BoardRow {
            id: "tsk_1".into(),
            space_path: "/s".into(),
            number: 7,
            title: "Ship the seam".into(),
            body: Some("prose".into()),
            spec_path: None,
            stage_slug: "doing".into(),
            sort_order: 0,
            created_at: 1,
            updated_at: 2,
            artifact_keys: vec!["spec".into(), "research".into()],
        }
    }

    // ── the `None` handle ────────────────────────────────────────────────

    /// Every arm answers the SAME way with no seam: a fail naming the reason. A
    /// panic here would take the Worker down over a wiring gap; a silent empty would
    /// tell the model the board is empty when nobody was asked.
    #[tokio::test]
    async fn every_arm_fails_naming_the_seam_when_there_is_no_task_store() {
        let ctx = ctx_with(None);
        let params = json!({
            "space_path": "/s", "task_id": "tsk_1", "title": "t", "body": "b",
            "stage_slug": "todo", "spec_path": "p", "to_stage": "done",
            "key": "spec", "markdown": "# m",
        });
        let results = [
            exec_list_board(&ctx, &params).await,
            exec_get_task(&ctx, &params).await,
            exec_create_task(&ctx, &params).await,
            exec_edit_task(&ctx, &params).await,
            exec_move_task(&ctx, &params).await,
            exec_get_artifact(&ctx, &params).await,
            exec_write_artifact(&ctx, &params).await,
        ];
        assert_eq!(results.len(), 7);
        for (i, r) in results.iter().enumerate() {
            assert!(r.is_error, "arm {i} must fail, not answer");
            let t = text(r);
            assert!(
                t.contains("no task store") && t.contains("TaskStore"),
                "arm {i} says {t:?} — it must name the missing seam"
            );
            assert!(r.details.is_none(), "arm {i} has no details to report");
        }
    }

    // ── param validation ─────────────────────────────────────────────────

    /// A missing required key is a fail that NAMES THE KEY, for every arm. This is
    /// the census-shaped test: the key list is the SAME list `exec_param_keys()`
    /// claims and `tool_specs()` advertises.
    #[tokio::test]
    async fn a_missing_required_param_names_the_key() {
        let mut fake = FakeTaskCaller::new();
        fake.answer("listBoard", json!([]));
        let ctx = ctx_with(Some(Arc::new(fake)));
        let checks: Vec<(&str, ToolResult)> = vec![
            ("space_path", exec_list_board(&ctx, &json!({})).await),
            ("task_id", exec_get_task(&ctx, &json!({})).await),
            ("space_path", exec_create_task(&ctx, &json!({})).await),
            (
                "title",
                exec_create_task(&ctx, &json!({"space_path": "/s"})).await,
            ),
            ("task_id", exec_edit_task(&ctx, &json!({})).await),
            ("space_path", exec_move_task(&ctx, &json!({})).await),
            (
                "task_id",
                exec_move_task(&ctx, &json!({"space_path": "/s"})).await,
            ),
            (
                "to_stage",
                exec_move_task(&ctx, &json!({"space_path": "/s", "task_id": "t"})).await,
            ),
            ("task_id", exec_get_artifact(&ctx, &json!({})).await),
            (
                "key",
                exec_get_artifact(&ctx, &json!({"task_id": "t"})).await,
            ),
            ("task_id", exec_write_artifact(&ctx, &json!({})).await),
            (
                "key",
                exec_write_artifact(&ctx, &json!({"task_id": "t"})).await,
            ),
            (
                "markdown",
                exec_write_artifact(&ctx, &json!({"task_id": "t", "key": "k"})).await,
            ),
        ];
        for (key, r) in checks {
            assert!(r.is_error, "`{key}` missing must be an error");
            assert!(
                text(&r).contains(key),
                "the failure must name `{key}`, got {:?}",
                text(&r)
            );
        }
    }

    /// A present-but-wrong-typed param is refused, not quietly dropped: `body: 42`
    /// would otherwise create a task WITHOUT the body the model asked for.
    #[tokio::test]
    async fn a_wrongly_typed_optional_names_the_key_and_calls_nothing() {
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "createTask",
            json!({"id":"tsk_9","spacePath":"/s","number":9,"title":"t",
                   "body":null,"specPath":null,"stageSlug":"todo","sortOrder":0,
                   "createdAt":1,"updatedAt":2}),
        );
        let fake = Arc::new(fake);
        let ctx = ctx_with(Some(fake.clone()));
        let r =
            exec_create_task(&ctx, &json!({"space_path": "/s", "title": "t", "body": 42})).await;
        assert!(r.is_error);
        assert!(text(&r).contains("body"), "got {:?}", text(&r));
        assert!(
            fake.calls().is_empty(),
            "a refused call must never reach the seam"
        );
        // An explicit `null` is NOT a type error — it is the other spelling of
        // "no value" the wire can carry (mirrors `TaskCall`'s `#[serde(default)]`).
        let r = exec_create_task(
            &ctx,
            &json!({"space_path": "/s", "title": "t", "body": null}),
        )
        .await;
        assert!(
            !r.is_error,
            "an explicit null body is accepted: {:?}",
            text(&r)
        );
        assert_eq!(fake.calls().len(), 1, "the null call DID reach the seam");
        assert_eq!(
            fake.calls()[0],
            TaskCall::CreateTask {
                space_path: "/s".into(),
                title: "t".into(),
                body: None,
                stage_slug: None,
            },
            "an explicit null arrives as None, exactly like an absent key"
        );
    }

    // ── per-verb happy paths ─────────────────────────────────────────────

    /// `list_board`: the rows ride `details.board`, the content is one line per card,
    /// and — the assertion that makes the split real — **no markdown anywhere** in
    /// either half. A test that only checks the keys are present would pass with a
    /// `BoardRow` that carried the whole corpus.
    #[tokio::test]
    async fn list_board_shows_rows_and_never_carries_markdown() {
        let mut fake = FakeTaskCaller::new();
        fake.answer("listBoard", json!([a_row()]));
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_list_board(&ctx, &json!({"space_path": "/s"})).await;
        assert!(!r.is_error, "{:?}", text(&r));
        let body = text(&r);
        assert!(body.contains("#7"), "the card number: {body}");
        assert!(body.contains("doing"), "the stage slug: {body}");
        assert!(body.contains("Ship the seam"), "the title: {body}");
        assert!(
            body.contains("spec") && body.contains("research"),
            "the artifact KEY names: {body}"
        );
        assert!(
            !body.contains("prose"),
            "the body is NOT part of a board line: {body}"
        );
        let details = r.details.expect("the board rides details for the UI");
        assert_eq!(details["board"][0]["spacePath"], "/s");
        assert_eq!(
            details["board"][0]["artifactKeys"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let whole = format!("{body}{}", serde_json::to_string(&details).unwrap());
        assert!(
            !whole.contains("markdown"),
            "a board payload must never carry markdown — the whole point of the split"
        );
    }

    /// An empty board is an answer, not an error, and still carries `details`.
    #[tokio::test]
    async fn an_empty_board_is_an_answer_not_an_error() {
        let mut fake = FakeTaskCaller::new();
        fake.answer("listBoard", json!([]));
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_list_board(&ctx, &json!({"space_path": "/s"})).await;
        assert!(!r.is_error);
        assert!(r.details.is_some(), "the UI still gets its (empty) rows");
    }

    /// A malformed answer (the Supervisor answered a DIFFERENT shape) is a fail that
    /// says so — never a silent empty board.
    #[tokio::test]
    async fn a_malformed_board_answer_fails_loudly() {
        let mut fake = FakeTaskCaller::new();
        fake.answer("listBoard", json!({"not": "a board"}));
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_list_board(&ctx, &json!({"space_path": "/s"})).await;
        assert!(r.is_error);
        assert!(text(&r).contains("was not a board"), "got {:?}", text(&r));
    }

    /// A seam error reaches the model VERBATIM (the message is the diagnosis: a bad
    /// artifact key, a missing task, a size refusal).
    #[tokio::test]
    async fn a_seam_error_reaches_the_model_verbatim() {
        let mut fake = FakeTaskCaller::new();
        fake.fail_with(
            "listBoard",
            TaskStoreError::Invalid {
                message: "artifact key `a b` is not a slug".into(),
            },
        );
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_list_board(&ctx, &json!({"space_path": "/s"})).await;
        assert!(r.is_error);
        assert_eq!(text(&r), "artifact key `a b` is not a slug");
    }

    /// `get_task` on a missing id is `no task found for …`, NOT an error: `None` is
    /// the seam's honest answer, and an `is_error` invites a retry of the same id.
    #[tokio::test]
    async fn get_task_on_a_missing_id_is_an_answer() {
        let mut fake = FakeTaskCaller::new();
        fake.answer("getTask", json!(null));
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_get_task(&ctx, &json!({"task_id": "tsk_gone"})).await;
        assert!(!r.is_error, "a missing row is an answer: {:?}", text(&r));
        assert!(text(&r).contains("tsk_gone"));
    }

    /// `get_task`'s found row: text + `details.task`.
    #[tokio::test]
    async fn get_task_returns_the_row() {
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "getTask",
            json!({"id":"tsk_1","spacePath":"/s","number":3,"title":"T",
                   "body":"b","specPath":null,"stageSlug":"done","sortOrder":1,
                   "createdAt":1,"updatedAt":2,"artifactKeys":["spec"]}),
        );
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_get_task(&ctx, &json!({"task_id": "tsk_1"})).await;
        assert!(!r.is_error);
        assert!(text(&r).contains("spec"), "the artifact keys are shown");
        let d = r.details.expect("details ride for the UI");
        assert_eq!(d["task"]["stageSlug"], "done");
    }

    /// `create_task` forwards the OPTIONAL arguments exactly (a dropped `stage_slug`
    /// would silently land the card on the wrong stage).
    #[tokio::test]
    async fn create_task_forwards_every_argument() {
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "createTask",
            json!({"id":"tsk_9","spacePath":"/s","number":9,"title":"New",
                   "body":"prose","specPath":null,"stageSlug":"approved","sortOrder":0,
                   "createdAt":1,"updatedAt":2}),
        );
        let fake_arc = Arc::new(fake);
        let ctx = ctx_with(Some(fake_arc.clone()));
        let r = exec_create_task(
            &ctx,
            &json!({"space_path": "/s", "title": "New", "body": "prose",
                    "stage_slug": "approved"}),
        )
        .await;
        assert!(!r.is_error, "{:?}", text(&r));
        assert_eq!(
            fake_arc.calls(),
            vec![TaskCall::CreateTask {
                space_path: "/s".into(),
                title: "New".into(),
                body: Some("prose".into()),
                stage_slug: Some("approved".into()),
            }],
            "the call the seam sees is the call the params describe"
        );
        let d = r.details.as_ref().expect("the new row rides details");
        assert_eq!(d["task"]["id"], "tsk_9");
        assert!(text(&r).contains("#9"));
    }

    /// `edit_task` forwards its three optionals.
    #[tokio::test]
    async fn edit_task_forwards_every_argument() {
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "editTask",
            json!({"id":"tsk_1","spacePath":"/s","number":1,"title":"New",
                   "body":null,"specPath":"docs/x.md","stageSlug":"todo","sortOrder":0,
                   "createdAt":1,"updatedAt":3}),
        );
        let fake_arc = Arc::new(fake);
        let ctx = ctx_with(Some(fake_arc.clone()));
        let r = exec_edit_task(
            &ctx,
            &json!({"task_id": "tsk_1", "title": "New", "spec_path": "docs/x.md"}),
        )
        .await;
        assert!(!r.is_error, "{:?}", text(&r));
        assert_eq!(
            fake_arc.calls(),
            vec![TaskCall::EditTask {
                task_id: "tsk_1".into(),
                title: Some("New".into()),
                body: None,
                spec_path: Some("docs/x.md".into()),
            }],
            "an omitted optional stays None (an edit must not blank a field)"
        );
        assert!(text(&r).contains("edited"));
    }

    /// `move_task`: the destination and the note are model-facing; `on_enter` is not,
    /// and the arm must not describe it (the crate never learns what it means). It
    /// still rides `details` for the desktop's command layer.
    #[tokio::test]
    async fn move_task_names_the_stage_and_note_but_not_on_enter() {
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "moveTask",
            json!({"toStage":"doing","onEnter":"fresh","note":"reordered"}),
        );
        let fake_arc = Arc::new(fake);
        let ctx = ctx_with(Some(fake_arc.clone()));
        let r = exec_move_task(
            &ctx,
            &json!({"space_path": "/s", "task_id": "tsk_1", "to_stage": "doing"}),
        )
        .await;
        assert!(!r.is_error, "{:?}", text(&r));
        let body = text(&r);
        assert!(body.contains("doing"), "the destination: {body}");
        assert!(body.contains("reordered"), "the note: {body}");
        assert!(
            !body.contains("fresh"),
            "`on_enter` is opaque command-layer data and must not be narrated: {body}"
        );
        let d = r.details.expect("the outcome rides details for D2");
        assert_eq!(
            d["moveOutcome"]["onEnter"], "fresh",
            "it still reaches the desktop's reader"
        );
        assert_eq!(
            fake_arc.calls(),
            vec![TaskCall::MoveTask {
                space_path: "/s".into(),
                task_id: "tsk_1".into(),
                to_stage: "doing".into(),
            }]
        );
    }

    /// `get_artifact`: the markdown rides the CONTENT (this is the whole point of the
    /// seam), byte-for-byte, and `details` does NOT duplicate it.
    #[tokio::test]
    async fn get_artifact_puts_the_markdown_in_the_content() {
        let md = "# Spec\n\nDo the thing.\n";
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "getArtifact",
            json!({"taskId":"tsk_1","key":"spec","markdown":md,
                   "createdAt":1,"updatedAt":2}),
        );
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_get_artifact(&ctx, &json!({"task_id": "tsk_1", "key": "spec"})).await;
        assert!(!r.is_error);
        assert_eq!(text(&r), md, "the markdown IS the content, verbatim");
        assert!(
            r.details.is_none(),
            "no details: the markdown would otherwise exist twice per row"
        );
    }

    /// A missing artifact is an answer naming both ids, not an error.
    #[tokio::test]
    async fn a_missing_artifact_is_an_answer() {
        let mut fake = FakeTaskCaller::new();
        fake.answer("getArtifact", json!(null));
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_get_artifact(&ctx, &json!({"task_id": "tsk_1", "key": "spec"})).await;
        assert!(!r.is_error);
        let body = text(&r);
        assert!(body.contains("spec") && body.contains("tsk_1"), "{body}");
    }

    /// `write_artifact` answers with the META only — the markdown never comes back
    /// (the caller just sent it; echoing it doubles the transcript row).
    #[tokio::test]
    async fn write_artifact_answers_with_the_meta_only() {
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "writeArtifact",
            json!({"key":"spec","updatedAt":9,"bytes":6}),
        );
        let fake_arc = Arc::new(fake);
        let ctx = ctx_with(Some(fake_arc.clone()));
        let r = exec_write_artifact(
            &ctx,
            &json!({"task_id": "tsk_1", "key": "spec", "markdown": "# Spec"}),
        )
        .await;
        assert!(!r.is_error, "{:?}", text(&r));
        let body = text(&r);
        assert!(
            body.contains("spec") && body.contains("6 bytes") && body.contains("9"),
            "{body}"
        );
        assert!(
            !body.contains("# Spec"),
            "the markdown must not come back: {body}"
        );
        let d = r.details.expect("the meta rides details");
        assert_eq!(d["artifactMeta"]["bytes"], 6);
        assert_eq!(
            fake_arc.calls(),
            vec![TaskCall::WriteArtifact {
                task_id: "tsk_1".into(),
                key: "spec".into(),
                markdown: "# Spec".into(),
            }]
        );
    }

    /// THE FRAME-SIZE PIN (issue #180 Risk 3): a 256 KiB markdown reaches the seam
    /// BYTE FOR BYTE through the executor path — no truncation, no cap, no escape
    /// corruption. The command layer owns the refusal; the crate owns the transport.
    #[tokio::test]
    async fn a_256_kib_markdown_round_trips_byte_for_byte_through_the_executor() {
        let markdown = "x".repeat(256 * 1024);
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "writeArtifact",
            json!({"key":"spec","updatedAt":9,"bytes":markdown.len()}),
        );
        let fake_arc = Arc::new(fake);
        let ctx = ctx_with(Some(fake_arc.clone()));
        let r = exec_write_artifact(
            &ctx,
            &json!({"task_id": "tsk_1", "key": "spec", "markdown": markdown}),
        )
        .await;
        assert!(!r.is_error, "{:?}", text(&r));
        let TaskCall::WriteArtifact { markdown: sent, .. } = &fake_arc.calls()[0] else {
            panic!("expected a WriteArtifact call");
        };
        assert_eq!(sent.len(), 256 * 1024, "no truncation at the cap size");
        assert_eq!(sent, &markdown, "byte-for-byte");
        assert!(text(&r).contains("262144 bytes"), "{:?}", text(&r));
    }

    /// The read side at the same size: a 256 KiB artifact comes back whole.
    #[tokio::test]
    async fn a_256_kib_artifact_comes_back_whole() {
        let markdown = "y".repeat(256 * 1024);
        let mut fake = FakeTaskCaller::new();
        fake.answer(
            "getArtifact",
            json!({"taskId":"tsk_1","key":"spec","markdown":markdown,
                   "createdAt":1,"updatedAt":2}),
        );
        let ctx = ctx_with(Some(Arc::new(fake)));
        let r = exec_get_artifact(&ctx, &json!({"task_id": "tsk_1", "key": "spec"})).await;
        assert!(!r.is_error);
        assert_eq!(text(&r).len(), 256 * 1024);
        assert!(text(&r).bytes().all(|b| b == b'y'));
    }

    // ── the cancel path (issue #180 Risk 4) ──────────────────────────────

    /// A Stop mid-call returns the cancelled shape PROMPTLY. The seam here NEVER
    /// answers — the fake hangs — so the turn token is the only possible exit, and
    /// the deadline is what turns "hangs" into a test failure instead of a stalled
    /// suite. Every arm is covered, because a single arm that forgot to vote the
    /// token would hang a Stop for the whole turn.
    #[tokio::test]
    async fn every_arm_returns_the_cancelled_shape_when_the_turn_stops() {
        let params = json!({
            "space_path": "/s", "task_id": "tsk_1", "title": "t", "body": "b",
            "stage_slug": "todo", "spec_path": "p", "to_stage": "done",
            "key": "spec", "markdown": "# m",
        });
        // Driven through `execute_tool` rather than a table of the seven `exec_*`
        // fns: that also pins the ROUTING (the `exec.rs` match arms), so an arm that
        // exists but is unreachable from the dispatcher cannot pass this test.
        let names = [
            "list_board",
            "get_task",
            "create_task",
            "edit_task",
            "move_task",
            "get_artifact",
            "write_artifact",
        ];
        for name in names {
            let (mut ctx, fake) = ctx_hanging();
            // A FRESH token per arm, already fired: the arms must notice a token
            // that was cancelled before the call, not only one that fires during.
            let cancel = CancellationToken::new();
            cancel.cancel();
            ctx.cancel = cancel;
            let got = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                crate::tools::exec::execute_tool(&ctx, name, &params),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "`{name}` did not return within 5s of the turn token firing \
                        while the seam was unanswered — that is the hang"
                )
            });
            assert!(got.is_error, "`{name}` returned {:?}", text(&got));
            assert_eq!(
                text(&got),
                "cancelled",
                "`{name}` must use the crate's cancelled sentence (an `unknown tool` \
                 answer here means the arm is not routed)"
            );
            assert!(got.details.is_none(), "`{name}` has no result to report");
            assert_eq!(
                fake.parked_now(),
                0,
                "`{name}` left a call parked on the seam (the leak side of Risk 4)"
            );
        }
    }

    /// A cancel that fires WHILE the call is parked (not before it) behaves the same
    /// — the arm must notice mid-flight, which is the case a pre-cancelled token
    /// does not exercise.
    #[tokio::test]
    async fn a_cancel_mid_flight_returns_the_cancelled_shape() {
        let (mut ctx, fake) = ctx_hanging();
        let cancel = CancellationToken::new();
        ctx.cancel = cancel.clone();
        let fired = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                cancel.cancel();
            })
        };
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            exec_list_board(&ctx, &json!({"space_path": "/s"})),
        )
        .await
        .expect("the mid-flight cancel returns promptly");
        assert!(got.is_error);
        assert_eq!(text(&got), "cancelled");
        assert_eq!(fake.parked_now(), 0, "the parked call was abandoned");
        assert_eq!(
            fake.calls().len(),
            1,
            "the call WAS issued before the cancel (so this is not a no-op test)"
        );
        fired.await.expect("the canceller ran");
    }

    /// The cancelled shape is the SAME one every other cancelled tool result in the
    /// crate produces — one sentence, so a model cannot tell a task arm apart.
    #[test]
    fn the_cancelled_shape_matches_the_harnesss_sentence() {
        let r = cancelled();
        assert!(r.is_error);
        assert!(r.details.is_none());
        assert_eq!(text(&r), "cancelled");
    }
}

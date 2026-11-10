//! The Rust tool executors (native-agent-harness Task 1): one per built-in
//! tool, each contained by the session's root set (via the existing
//! [`FsBackend`](crate::fs_backend::FsBackend)) and returning a pi-shaped
//! [`ToolResult`] — `content` is what the LLM sees, `details` is what the UI
//! sees (matching pi's built-in renderers). The native harness (in-process)
//! calls [`execute_tool`].

pub mod access;
pub mod bash;
pub mod fs_tools;
pub mod search;
pub mod skills;
pub mod task_tools;

#[cfg(test)]
mod tests;

use serde_json::Value;

pub use super::types::*;

pub use access::protected_refusal_text;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use bash::{
    bash_exit_report, clamp_timeout_ms, exit_signal, kill_bash_process_group, DEFAULT_BASH_TIMEOUT,
    DRAIN_GRACE, MAX_BASH_TIMEOUT, MAX_OUTPUT_BYTES,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use fs_tools::{
    exec_edit, exec_ls, exec_read, exec_write, image_mime, unified_diff, MAX_READ_LINES,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use search::{exec_find, exec_grep, glob_match, DEFAULT_MAX_RESULTS};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use skills::{exec_list_skills, exec_read_skill, model_skills_for, skills_for};

/// Dispatch a tool call to its executor (the entry point the native
/// harness calls).
pub async fn execute_tool(ctx: &ToolCtx, tool: &str, params: &Value) -> ToolResult {
    match tool {
        "bash" => bash::exec_bash(ctx, params).await,
        "read" => fs_tools::exec_read(ctx, params).await,
        "write" => fs_tools::exec_write(ctx, params).await,
        "edit" => fs_tools::exec_edit(ctx, params).await,
        "find" => search::exec_find(ctx, params).await,
        "grep" => search::exec_grep(ctx, params).await,
        "ls" => fs_tools::exec_ls(ctx, params).await,
        "list_skills" => skills::exec_list_skills(ctx, params).await,
        "read_skill" => skills::exec_read_skill(ctx, params).await,
        "web_search" => crate::web::tools::exec_web_search(ctx, params).await,
        "fetch_content" => crate::web::tools::exec_fetch_content(ctx, params).await,
        "get_search_content" => crate::web::tools::exec_get_search_content(ctx, params).await,
        // (issue #180 F1) The seven board / artifact verbs. Arms of `execute_tool`
        // and NOT loop-internal, so codemode reaches them on a cloned `ToolCtx`
        // (ADR 0092) — and they are absent from `EXCLUDED_FROM_SCRIPT` for the same
        // reason: none of them is interactive or recursive. Each arm races the turn
        // token internally (`task_tools::parked`), which is why `dispatch_tool`
        // needs no `select!` for these names.
        "list_board" => task_tools::exec_list_board(ctx, params).await,
        "get_task" => task_tools::exec_get_task(ctx, params).await,
        "create_task" => task_tools::exec_create_task(ctx, params).await,
        "edit_task" => task_tools::exec_edit_task(ctx, params).await,
        "move_task" => task_tools::exec_move_task(ctx, params).await,
        "get_artifact" => task_tools::exec_get_artifact(ctx, params).await,
        "write_artifact" => task_tools::exec_write_artifact(ctx, params).await,
        other => ToolResult::fail(format!("unknown tool: {other}"), None),
    }
}

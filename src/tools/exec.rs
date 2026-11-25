//! The Rust tool executors (native-agent-harness Task 1): one per built-in
//! tool, each contained by the session's root set (via the existing
//! [`FsBackend`](crate::fs_backend::FsBackend)) and returning a pi-shaped
//! [`ToolResult`] — `content` is what the LLM sees, `details` is what the UI
//! sees (matching pi's built-in renderers). The native harness (in-process)
//! calls [`execute_tool`].

pub mod access;
pub mod bash;
pub mod fs_tools;
pub mod registry;
pub mod search;
pub mod skills;
pub mod task_tools;

#[cfg(test)]
mod tests;

use serde_json::Value;

pub use super::types::*;

pub use access::protected_refusal_text;
// (ADR 0102 F1) The registry's public surface, re-exported alongside
// `execute_tool` so a host composes a registry without reaching into `exec`.
pub use registry::{
    builtins, default_registry, host_entries, lookup, script_access, HostTool, NativeRun,
    NativeTool, ScriptAccess, ToolEntry, ToolExecutor, BUILTIN_TOOLS, LOOP_INTERNAL_SCRIPT,
};

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
///
/// (ADR 0102 / issue #183 F1) This is a THIN WRAPPER over the registry, and it
/// keeps its exact public signature. The `match` that used to live here is now
/// [`registry::BUILTIN_TOOLS`] — the list that BUILDS the default registry rather
/// than the thing that dispatches — so registering the 19 built-ins through
/// [`NativeTool`] is behaviour-neutral by construction: same `exec_*` function,
/// same `&ToolCtx`, same `&Value`, same `unknown tool: {name}` fall-through.
///
/// # Why it stays `pub` with this signature
///
/// Two reasons, and the second is the one that cannot be checked from here. 105
/// of this crate's own test call sites name it, which is churn. But the sibling
/// desktop repo's integration tests (`archimedes/src-tauri/tests/skill_tools_boundary.rs`
/// and `skill_catalog_drift.rs`) import it through
/// `archimedes_lib::agent::tools::execute_tool`, and the desktop consumes a
/// crates.io revision — so **NO test in this workspace can see that breakage**.
/// Removing it or re-signaturing it is a cross-repo breaking change with zero
/// in-repo witness. It is also what a loop-internal-free caller (a test, a host
/// probe) reaches for when it has a `ToolCtx` and no `AgentLoop`.
///
/// # What it does NOT consult
///
/// A LOOP's registry, which after F2 may carry host entries this free function
/// has never seen. Both loop dispatch paths (`dispatch_tool` and the codemode
/// bridge's `dispatch_nested`) resolve through the loop's own registry instead;
/// this one resolves through [`default_registry`], the built-ins.
pub async fn execute_tool(ctx: &ToolCtx, tool: &str, params: &Value) -> ToolResult {
    // The miss sentence is VERBATIM what the old `other =>` arm produced —
    // `unknown_tool_is_error` pins it byte-for-byte, and a registry that drops a
    // name has to be distinguishable from a name that was never a built-in.
    match registry::lookup(registry::default_registry(), tool) {
        Some(entry) => entry.executor.execute(ctx, params.clone()).await,
        None => ToolResult::fail(format!("unknown tool: {tool}"), None),
    }
}

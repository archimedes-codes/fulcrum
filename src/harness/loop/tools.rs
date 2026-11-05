use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::AgentLoop;
use crate::harness::provider::ToolCall;
use crate::interactive::{sudo_run_flow, todo_apply};
use crate::mcp::mcp_tool;
use crate::tools::{execute_tool, ContentBlock, ToolResult};

pub(super) fn cancelled_tool_result() -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text {
            text: "cancelled".to_string(),
        }],
        details: None,
        is_error: true,
    }
}

pub(super) fn abandoned_tool_result() -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text {
            text: "discarded — the model call that asked for this tool was \
                   abandoned before it ran"
                .to_string(),
        }],
        details: None,
        is_error: true,
    }
}

pub(super) fn sudo_cancelled_result() -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text {
            text: "cancelled — a Stop before the command started prevents it; a Stop mid-run kills the started command".to_string(),
        }],
        details: None,
        is_error: true,
    }
}

impl AgentLoop {
    pub(super) fn cancelled_tool_result() -> ToolResult {
        cancelled_tool_result()
    }

    pub(super) fn abandoned_tool_result() -> ToolResult {
        abandoned_tool_result()
    }

    pub(super) fn sudo_cancelled_result() -> ToolResult {
        sudo_cancelled_result()
    }

    /// The native `ToolRegistry`: a `match` on the tool name — the
    /// built-ins (Task 1's `execute_tool`, in-process, `ToolCtx { cwd,
    /// cancel }`) + the suite tools (the in-process cores of the
    /// interactive channel, called in-process — NOT over a socket) +
    /// `subagent` (a native parent spawns an IN-PROCESS native child via
    /// `dispatch_native`).
    pub(super) async fn dispatch_tool(
        &mut self,
        tc: &ToolCall,
        turn: &CancellationToken,
    ) -> ToolResult {
        // The `enabled_tools` filter (`None` = ALL — finding 13b): a
        // disabled tool is a tool-result error, NOT executed.
        // `dispatch_subagent` is an ALIAS of `subagent` (the `match`
        // below routes both to it): the gate must know the alias — a
        // parent with `enabled_tools: Some(["subagent"])` must not get
        // `tool not enabled: dispatch_subagent` when the model emits it.
        if !self.is_tool_enabled(&tc.name) {
            return ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("tool not enabled: {}", tc.name),
                }],
                details: None,
                is_error: true,
            };
        }
        // The `subagent` / `dispatch_subagent` alias (the single source of
        // truth is `is_subagent_call` — the parallel batch + this dispatch
        // share it; a future third alias updates only that one place).
        if crate::harness::dispatch::is_subagent_call(&tc.name) {
            let (result, child_cost) = self.dispatch_subagent(&tc.arguments, turn).await;
            // The child's money joins the session total HERE (the sequential
            // path — `run_tool_batch`'s concurrent batch folds at its own
            // consumption point). `dispatch_tool` is `&mut self`, so this is a
            // legal writer for `cost_acc`; `dispatch_subagent` itself stays
            // `&self` because the concurrent path calls it through a shared
            // borrow inside the batch's futures.
            self.fold_child_cost(child_cost);
            return result;
        }
        match tc.name.as_str() {
            "bash" | "read" | "write" | "edit" | "find" | "grep" | "ls" | "list_skills"
            | "read_skill" | "web_search" | "fetch_content" | "get_search_content" => {
                // The ONE `ToolCtx` construction is `PermissionContext::to_tool_ctx`
                // (finding 6): the frozen boundary + guard list, the per-turn
                // rule list + policy, the web config and the render requester
                // all come from the SAME bundle the gate judged with, so the
                // executor cannot re-decide differently from its gate. `cwd` is
                // the loop's own `space_cwd` (not permission state) and `cancel`
                // is the TURN token.
                execute_tool(
                    &self
                        .permissions
                        .to_tool_ctx(self.space_cwd.clone(), turn.clone()),
                    &tc.name,
                    &tc.arguments,
                )
                .await
            }
            "manage_todo_list" => {
                // RACED against the TURN token (finding 8b, round 2 —
                // a cancel stops the turn, it is never waited out; the
                // flow's own `cancel` is the SESSION teardown token,
                // which a Stop does NOT set).
                tokio::select! {
                    r = todo_apply(
                        &self.todo_store,
                        &self.session_id,
                        "native",
                        &tc.arguments,
                        &self.sink,
                        &self.cancel,
                    ) => r,
                    _ = turn.cancelled() => Self::cancelled_tool_result(),
                }
            }
            "sudo_exec" => {
                // RACED against the TURN token (finding 8b, round 2 — the
                // confirm / password sub-prompts `select!`ed only on the
                // SESSION teardown token: a Stop while a dialog was open
                // waited for the answer, and a confirm answered AFTER the
                // Stop still EXECUTED the elevated command). The prompts
                // now carry no deadline at all, so this turn-token race is
                // the ONLY thing that settles a Stop promptly.
                tokio::select! {
                    r = sudo_run_flow(
                        &self.session_id,
                        &tc.id,
                        "native",
                        &tc.arguments,
                        &self.sink,
                        &self.sudo.runner,
                        &self.sudo.pending_sudo,
                        &self.sudo.sudo_password,
                        &self.cancel,
                    ) => r,
                    _ = turn.cancelled() => Self::sudo_cancelled_result(),
                }
            }
            "ask" => {
                crate::harness::ask::ask_flow(
                    &self.pending_bridge,
                    &self.sink,
                    &self.session_id,
                    &tc.arguments,
                    &tc.id,
                    turn,
                )
                .await
            }
            "list_agents" => self.list_agents_tool().await,
            "mcp" => {
                // RACED against the TURN token (finding 8b, round 2 — a Stop
                // mid-mcp-call cancels the in-flight request + kills the stdio
                // child; the flow's internal `cancel` is the TURN token too).
                tokio::select! {
                    r = mcp_tool(&self.mcp, &tc.arguments, turn) => r,
                    _ = turn.cancelled() => Self::cancelled_tool_result(),
                }
            }
            "codemode" => {
                // The `&mut self` here is exactly WHY the bridge takes `&AgentLoop`
                // and dispatches nested built-ins through `execute_tool` on a
                // CLONED `ToolCtx`: a script's nested call cannot re-enter this
                // method, so the gate corridor is re-implemented in
                // `loop/codemode.rs` against `&self` instead of reused from here.
                // The engine is per-call (no field on the loop) and the turn token
                // is the one cancellation authority.
                crate::harness::r#loop::codemode::exec_codemode(self, &tc.id, &tc.arguments, turn)
                    .await
            }
            other => ToolResult {
                content: vec![ContentBlock::Text {
                    text: format!("unknown tool: {other}"),
                }],
                details: None,
                is_error: true,
            },
        }
    }

    /// Whether `name` passes the `enabled_tools` filter (`None` = all
    /// enabled). The logic lives in `harness::dispatch` (no loop state).
    pub(super) fn is_tool_enabled(&self, name: &str) -> bool {
        crate::harness::dispatch::is_tool_enabled(self.enabled_tools(), name)
    }

    /// `subagent` dispatch — the thin wrapper that supplies the loop's
    /// fields; the dispatch flow itself lives in `harness::dispatch`.
    ///
    /// `&self`, NOT `&mut self`, and the child's cost comes back as the second
    /// half of the return value rather than being folded here: the CONCURRENT
    /// batch in `run_tool_batch` calls this through a shared borrow (`me`) so
    /// several children can be in flight at once, and a `&mut self` fold inside
    /// those futures is not expressible. The CALLER folds — `run_tool_batch` at
    /// the point it consumes each outcome, `dispatch_tool` on the sequential
    /// path — which is also where the fold's frame gets its order: one frame
    /// per child, in the batch's tool-call order.
    pub(super) async fn dispatch_subagent(
        &self,
        params: &Value,
        turn: &CancellationToken,
    ) -> (ToolResult, Option<f64>) {
        crate::harness::dispatch::dispatch_subagent(
            self.subagent.as_ref(),
            &crate::harness::dispatch::SubagentParent {
                session_id: &self.session_id,
                space_cwd: &self.space_cwd,
                model: &self.model,
                enabled_tools: self.enabled_tools(),
                sink: &self.sink,
                catalog: &self.catalog,
                agent_overrides: self.packaged_agent_overrides.as_ref(),
            },
            params,
            turn,
        )
        .await
    }

    /// The `list_agents` tool result: one line per discovered Agent
    /// definition (ADR 0020), or `"none"`.
    pub(super) async fn list_agents_tool(&self) -> ToolResult {
        let agents = crate::agents::discover_agents(Some(&self.space_cwd));
        let text = if agents.is_empty() {
            "none".to_string()
        } else {
            agents
                .iter()
                .map(|a| {
                    let description = a.description.replace('\n', " ");
                    let mut line = format!("{} ({}): {}", a.name, a.scope, description);
                    let mut notes: Vec<String> = Vec::new();
                    if let Some(m) = &a.model {
                        notes.push(format!("model: {m}"));
                    }
                    if let Some(t) = &a.thinking {
                        notes.push(format!("thinking: {t}"));
                    }
                    if let Some(t) = &a.tools {
                        notes.push(format!("tools: {}", t.join(", ")));
                    }
                    if !notes.is_empty() {
                        line.push_str(&format!(" [{}]", notes.join(", ")));
                    }
                    line
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        ToolResult {
            content: vec![ContentBlock::Text { text }],
            details: None,
            is_error: false,
        }
    }
}

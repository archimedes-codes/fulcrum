use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::AgentLoop;
use crate::harness::provider::ToolCall;
use crate::interactive::{sudo_run_flow, todo_apply};
use crate::mcp::mcp_tool;
use crate::tools::{ContentBlock, ToolResult};

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

    /// The native `ToolRegistry`: the tool registry's entries (the built-ins —
    /// in-process, `ToolCtx { cwd, cancel }`) resolved FIRST, then the suite
    /// tools (the in-process cores of the interactive channel, called
    /// in-process — NOT over a socket) + `subagent` (a native parent spawns an
    /// IN-PROCESS native child via `dispatch_native`) as the loop arms below.
    /// Dispatch a single call. Returns the [`ToolResult`], whether a PERSON
    /// refused it, and whether the executor ACCEPTED the arguments and RAN —
    /// the same `(ToolResult, bool)` shape `codemode::run_nested` returns, for
    /// the same reason, plus a third half for the mirror-image false positive.
    ///
    /// The second half, `true`, reaches here from exactly ONE arm: `sudo_exec`,
    /// whose confirm and password sub-prompts are answered by a person and are
    /// refused INSIDE `sudo_run_flow`, so the permission gate never sees the
    /// decision and cannot supply the flag. Every other arm answers `false`,
    /// including the `unknown tool` / `tool not enabled` results and a
    /// `codemode` bridge refusal — those are the engine refusing, which IS a
    /// re-send instruction.
    ///
    /// The third half, `ran`, is FORWARDED and never decided here — no arm of
    /// this `match` can tell "the executor rejected these arguments" from "the
    /// executor accepted them and the command failed", and only `sudo_run_flow`
    /// knows which of those it just did. It matters because `sudo_exec`
    /// advertises `required: ["command", "reason"]` while the flow DEFAULTS an
    /// omitted `reason`: a granted-confirm call whose command then fails is
    /// `is_error` with an advertised-required key absent, which the refusal
    /// predicate's class 2 would read as an argument rejection. It was not one.
    /// `false` for every arm that never got that far, which is every other arm
    /// in this `match` (the built-in executors are reached through
    /// `execute_tool`, whose argument rejections this does NOT claim to see —
    /// see `refused_before_the_model_could_act`'s docblock for what that leaves
    /// as the guard's known weakness).
    ///
    /// Both flags exist for `run_tool_batch`'s refusal predicate and nothing
    /// else; `run_tool_batch` is this function's only production caller.
    pub(super) async fn dispatch_tool(
        &mut self,
        tc: &ToolCall,
        turn: &CancellationToken,
    ) -> (ToolResult, bool, bool) {
        // The `enabled_tools` filter (`None` = ALL — finding 13b): a
        // disabled tool is a tool-result error, NOT executed.
        // `dispatch_subagent` is an ALIAS of `subagent` (the `match`
        // below routes both to it): the gate must know the alias — a
        // parent with `enabled_tools: Some(["subagent"])` must not get
        // `tool not enabled: dispatch_subagent` when the model emits it.
        if !self.is_tool_enabled(&tc.name) {
            // The ENGINE refusing (the tool is not enabled), not a person — so
            // the flag is `false` and the refusal guard still counts it. The
            // third flag is `false` too: nothing ran.
            return (
                ToolResult {
                    content: vec![ContentBlock::Text {
                        text: format!("tool not enabled: {}", tc.name),
                    }],
                    details: None,
                    is_error: true,
                },
                false,
                false,
            );
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
            // A child's own result is never a PERSON's refusal of THIS call; the
            // child's gate ran inside the child, with its own refusal guard. The
            // third flag is `false` as well — the child ran, not THIS call, and a
            // child's result carries no verdict about this call's arguments.
            return (result, false, false);
        }
        // (ADR 0102 / issue #183 F1) THE REGISTRY IS THE DISPATCH AUTHORITY.
        //
        // What used to live here was a 19-name OR-list naming the built-ins, and
        // forgetting a name in it was not a compile error — the `match` fell
        // through to `unknown tool`, so the model was told a tool existed and got
        // that sentence for the whole session. Now the engine asks the registry it
        // was handed, and the set of names it can reach is the registry's own
        // membership plus the loop arms below.
        //
        // The lookup runs BEFORE the `match` and not as one of its arms, because
        // the registry's names and the loop arms' names are DISJOINT — that
        // disjointness is not a hope, it is the census
        // (`every_advertised_tool_reaches_an_executor`), and it is what makes the
        // reordering behaviour-identical rather than a coin flip on precedence.
        //
        // The ONE `ToolCtx` construction is still `PermissionContext::to_tool_ctx`
        // (finding 6): the frozen boundary + guard list, the per-turn rule list +
        // policy, the web config and the render requester all come from the SAME
        // bundle the gate judged with, so the executor cannot re-decide
        // differently from its gate. `cwd` is the loop's own `space_cwd` (not
        // permission state) and `cancel` is the TURN token.
        //
        // An executor's own verdict (a missing or malformed parameter, a failed
        // command) is the ENGINE refusing — counted. No person is involved here:
        // the permission gate decided that BEFORE this point, and its denial is
        // marked in `run_tool_batch`.
        //
        // The third flag is `false` HERE, and that is a deliberate CONSERVATISM,
        // not a claim — the same reason it was `false` as a `match` arm, now with
        // a type-level one behind it. The flag means "the executor accepted these
        // arguments and RAN them", and [`ToolExecutor::execute`] returns a bare
        // [`ToolResult`] that cannot say which of the two happened, so this arm
        // cannot attest it and an unattested call stays inside class 2. That is
        // what keeps the card's headline pin: `bash {}` omits the advertised-
        // required `command`, `exec_bash` answers ``bash: missing `command`
        // parameter``, and the 303-call burst must still cap at 4. Widening the
        // trait to carry a `ran` bool would let an implementer mark built-ins
        // `true` and silence the guard on the exact shape it exists for.
        if let Some(entry) = crate::tools::exec::lookup(&self.tool_registry, &tc.name) {
            let tool_ctx = self
                .permissions
                .to_tool_ctx(self.space_cwd.clone(), turn.clone());
            let params = tc.arguments.clone();
            // `entry.executor` is an `Arc`, so the borrow of `self.tool_registry`
            // ends here and the call itself is on the shared handle.
            let executor = Arc::clone(&entry.executor);
            return (executor.execute(&tool_ctx, params).await, false, false);
        }
        match tc.name.as_str() {
            "manage_todo_list" => {
                // RACED against the TURN token (finding 8b, round 2 —
                // a cancel stops the turn, it is never waited out; the
                // flow's own `cancel` is the SESSION teardown token,
                // which a Stop does NOT set).
                (
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
                    },
                    // The cancel arm is excluded by the predicate's own
                    // `turn.is_cancelled()` half, not by this flag. Neither flag
                    // is attested: `todo_apply` returns a plain `ToolResult`.
                    false,
                    false,
                )
            }
            "sudo_exec" => {
                // RACED against the TURN token (finding 8b, round 2 — the
                // confirm / password sub-prompts `select!`ed only on the
                // SESSION teardown token: a Stop while a dialog was open
                // waited for the answer, and a confirm answered AFTER the
                // Stop still EXECUTED the elevated command). The prompts
                // now carry no deadline at all, so this turn-token race is
                // the ONLY thing that settles a Stop promptly.
                // THE ONE arm in this `match` that can answer `true` on EITHER
                // flag, and the reason the tuple exists at all: a declined confirm
                // or a cancelled password entry is a PERSON'S "no", decided inside
                // `sudo_run_flow` where the permission gate cannot see it, and a
                // command that was confirmed, credentialed and RUN is a verdict on
                // the PROCESS and not on the arguments. Both flags ride out of the
                // flow rather than being read off the flow's sentence. The cancel
                // arm answers `false` on both because a Stop is excluded one layer
                // up, by the predicate's `turn.is_cancelled()` half.
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
                    _ = turn.cancelled() => (Self::sudo_cancelled_result(), false, false),
                }
            }
            "ask" => {
                // An `ask` ANSWER is not a refusal, and `ask_flow`'s only
                // `is_error` arm is an UNUSABLE payload — the engine refusing a
                // malformed call, which IS a re-send instruction. So `false`.
                // (An unanswered/cancelled `ask` is excluded by the predicate's
                // `turn.is_cancelled()` half, one layer up.)
                let result = crate::harness::ask::ask_flow(
                    &self.pending_bridge,
                    &self.sink,
                    &self.session_id,
                    &tc.arguments,
                    &tc.id,
                    turn,
                )
                .await;
                (result, false, false)
            }
            "list_agents" => (self.list_agents_tool().await, false, false),
            "mcp" => {
                // RACED against the TURN token (finding 8b, round 2 — a Stop
                // mid-mcp-call cancels the in-flight request + kills the stdio
                // child; the flow's internal `cancel` is the TURN token too).
                (
                    tokio::select! {
                        r = mcp_tool(&self.mcp, &tc.arguments, turn) => r,
                        _ = turn.cancelled() => Self::cancelled_tool_result(),
                    },
                    false,
                    false,
                )
            }
            "codemode" => {
                // The `&mut self` here is exactly WHY the bridge takes `&AgentLoop`
                // and dispatches nested built-ins through `execute_tool` on a
                // CLONED `ToolCtx`: a script's nested call cannot re-enter this
                // method, so the gate corridor is re-implemented in
                // `loop/codemode.rs` against `&self` instead of reused from here.
                // The engine is per-call (no field on the loop) and the turn token
                // is the one cancellation authority.
                // The bridge's own refusals (a disabled tool, an unknown param,
                // the interactive `mcp auth` action) are the ENGINE refusing.
                // A nested call's gate denial lands in the script's row, not in
                // this batch's refusal vector, and `run_nested` marks it there.
                (
                    crate::harness::r#loop::codemode::exec_codemode(
                        self,
                        &tc.id,
                        &tc.arguments,
                        turn,
                    )
                    .await,
                    false,
                    false,
                )
            }
            // The ENGINE refusing an unreachable name — a re-send instruction.
            other => (
                ToolResult {
                    content: vec![ContentBlock::Text {
                        text: format!("unknown tool: {other}"),
                    }],
                    details: None,
                    is_error: true,
                },
                false,
                false,
            ),
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

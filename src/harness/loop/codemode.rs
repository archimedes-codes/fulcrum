//! The `codemode` bridge (task C2): the `tools.*` globals, the nested-call gate,
//! and the live/final `details.calls` envelope.
//!
//! ONE nested call, in one sentence: a script's `tools.<name>(args)` becomes a
//! synthesized [`ToolCall`] whose id is `"<parentCallId>/<n>"`, and that call goes
//! through the SAME `policy_verdict` → `gate_tool` / `deny_tool_result` corridor the
//! direct batch route runs (`loop/turn.rs::run_tool_batch`, in its decision ORDER),
//! then to `execute_tool` (a built-in, on a CLONED [`ToolCtx`]) or to A1's shared
//! `McpManager` handle. A denial therefore carries the byte-identical wording a
//! direct call gets — the model cannot tell the two apart, which is the feature's
//! whole safety property.
//!
//! # Why the script runs through `context().async_with`, not `CodemodeEngine::eval`
//!
//! `eval` is C1's SYNC `AsyncContext::with` (`EvalOptions { promise: false }`), so
//! it cannot park: a script that `await`s a host call returns immediately with a
//! pending Promise nothing ever resolves, and a top-level `await` is not even a
//! valid program in that mode. C1's own `eval_cancels_on_turn_token` documents
//! exactly this and drives the parked recipe through `context()` + `race_turn`. The
//! bridge needs that recipe for its DAY-ONE path, not for a test corner: `tools.*`
//! returns a Promise, so EVERY script that calls a tool parks.
//!
//! So the bridge runs the script through the frozen `context()` accessor with
//! `eval_promise` (the `promise: true` eval — the only mode in which a top-level
//! `await` compiles), raced by the frozen standalone [`race_turn`]. Two consequences
//! are spelled out where they live:
//!
//! - the wall clock is armed HERE (see [`WallClock`]), because C1's handler clock is
//!   armed inside `eval`, which this bridge never calls;
//! - the exception-path rule is replicated in [`exception_verdict`], because C1's
//!   mapping is a private method of `CodemodeEngine`.
//!
//! # Why nested calls run in THIS task, beside the script
//!
//! A host closure handed to rquickjs must own everything it captures, but the gate
//! is an `AgentLoop` method and the bridge holds only a borrow of the loop. So the
//! host function does the minimum that needs no loop — convert the arguments to
//! owned Rust, hand the request down a channel — and [`Bridge::run_script`] runs it
//! against `loop_` inside a [`FuturesUnordered`]. Same task, same borrow, so no
//! lifetime is fought; several nested calls are genuinely in flight together, which
//! is what makes `Promise.all` batch instead of serialize; and each answer flows
//! back through the `oneshot` the parked JS promise is waiting on. Polling the
//! SCRIPT is what pumps rquickjs's scheduler (and therefore the parked host
//! futures), and the `select!` polls it on every pass, so an answer sent by the
//! nested-call arm is delivered by the script arm.
//!
//! # The live envelope (frozen contract item 1, as amended)
//!
//! Every frame leaves through B1's `ToolCtx::emit_tool_execution_update` with the
//! **PARENT** call's id and a `partial_result` that is `serde_json::to_value` of a
//! [`ToolResult`] whose `details` is `{"calls": [ … ]}` — the same TYPE, the same
//! KEY and the same SHAPE the final `tool_execution_end` writes, so `merge_json`'s
//! shallow top-level overwrite lands live and final on one place
//! (`rawOutput.details.calls`) and the frontend reads one key. A nested id NEVER
//! appears as a frame's `tool_call_id`: `normalize.rs`'s `tool_execution_update` arm
//! has no announced-id guard and `compute_display_rows` mints a display row for a
//! first-seen key, so a nested-keyed frame would spawn a stray top-level transcript
//! row per nested call. The nested `<parent>/<n>` string appears ONLY as the
//! synthesized [`ToolCall`]'s id (the gate's `requestId`) and as each element's
//! `toolCallId`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::super::dispatch::is_tool_enabled;
use super::AgentLoop;
use crate::agent::codemode::{race_turn, CodemodeEngine, EngineLimits, ScriptEnd};
use crate::agent::harness::provider::{ToolCall, ToolSpec};
use crate::agent::harness::r#loop::tools::cancelled_tool_result;
use crate::agent::harness::tool_specs::unknown_param_error;
use crate::agent::mcp::mcp_tool;
use crate::agent::policy::Decision;
use crate::agent::tools::{execute_tool, ContentBlock, ToolCtx, ToolResult};

/// The script's wall-clock bound (spec §7.1's budget for one `codemode` call).
const WALL_CLOCK: Duration = Duration::from_secs(300);
/// The JS stack cap — C1's `EngineLimits` documents 256 KiB as the crate default,
/// and its own tests run at it.
const STACK_BYTES: usize = 256 * 1024;
/// The MCP request/list bound for the bridge's discovery and call paths. The SAME
/// figure `agent/mcp/tool.rs` uses for the proxy tool (its `TIMEOUT` is
/// module-private, so the value is restated rather than the constant re-exported).
const MCP_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the post-script drain keeps UNANSWERED nested calls alive once the
/// script is over. A script may fire a `tools.x(...)` it never awaits; those rows
/// still belong in `details.calls`, but a wedged executor must not hold the turn
/// either — the direct route abandons a tool that outlived its model call rather
/// than waiting for it forever.
const ORPHAN_DRAIN: Duration = Duration::from_secs(30);

/// The tools a script may NOT reach, each with the reason (spec §7.1's decision,
/// plus the recursion the spec could not have listed because it predates the
/// `codemode` tool existing).
///
/// `ask` / `sudo_exec` / `manage_todo_list` are INTERACTIVE: they open a prompt or a
/// dialog the script has no way to answer, and a program that blocks on a human is a
/// stall wearing a costume. `subagent` / `list_agents` are RECURSIVE — a nested child
/// is a whole turn of its own. `codemode` is recursive for exactly the `subagent`
/// reason (a nested `codemode` is a second engine running a second script), so it is
/// excluded here too.
///
/// The exclusion is enforced TWICE on purpose: these names are absent from
/// `ALL_TOOLS`, and `tools.<name>` is built FROM `ALL_TOOLS`, so a name that is not
/// discoverable is also not callable — a script that hard-codes the spelling gets a
/// JS `TypeError`, not a dispatch. A nested built-in then dispatches through
/// `execute_tool`, which cannot reach them at all: the third layer.
const EXCLUDED_FROM_SCRIPT: &[(&str, &str)] = &[
    ("ask", "interactive"),
    ("sudo_exec", "interactive"),
    ("manage_todo_list", "interactive"),
    ("subagent", "recursive"),
    ("list_agents", "recursive"),
    ("codemode", "recursive"),
];

/// The ARGUMENT-level half of [`EXCLUDED_FROM_SCRIPT`].
///
/// The name list above cannot express this case, because the interactivity is not in
/// the tool's NAME — it is in one of `mcp`'s ACTIONS. `mcp` is deliberately NOT
/// excluded: it is the sanctioned way for a script to reach a server. But
/// `action: "auth"` runs the interactive OAuth flow — it opens the user's default
/// browser and then waits up to 300 s on a local callback for a human to finish a
/// consent screen. That is the same "a program that blocks on a human is a stall
/// wearing a costume" the `interactive` names are excluded for, plus it opens a
/// window on the user's desktop to authorize something nobody asked for, and a script
/// can do it in a loop over every configured server in one turn.
///
/// `connect` is NOT refused here, and that is deliberate rather than an oversight:
/// `call_tool`, `list_tools` and `describe` all run `ensure_connected`, so any nested
/// MCP call spawns a stdio child whether or not the script says `connect`. Refusing
/// the word would forbid no process that the same script could not already start —
/// it would only make the failure confusing. The process-spawning surface is a real
/// limit and ADR 0089 records it as one; this refusal is about the HUMAN, which
/// `connect` does not involve.
///
/// Returns the refusal text, or `None` when the arguments are callable.
fn interactive_args(name: &str, args: &Value) -> Option<String> {
    if name != "mcp" {
        return None;
    }
    // A non-object `args` reaches the tool and gets the tool's own error; there is no
    // `action` key to read, so there is nothing interactive to refuse here.
    let action = args.get("action").and_then(Value::as_str)?;
    if action == "auth" {
        return Some(
            "mcp action \"auth\" is not callable from a codemode script (it opens the \
             user's browser and waits on a human)"
                .to_string(),
        );
    }
    None
}

/// One nested call's status, in the frozen element shape. Serialized as `running` /
/// `ok` / `error` / `denied` — the four strings D1's `rowStatus` switches on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum NestedStatus {
    /// Started, not finished — a cancel or a dropped script can leave a row here,
    /// which is the contract's word for "it did not complete".
    Running,
    /// Finished, not an error.
    Ok,
    /// Finished with a failed [`ToolResult`] (an executor's own refusal and a
    /// `cancelled` result both land here, exactly as they do for a direct call).
    Error,
    /// The policy gate refused it: a `Deny`, or an `Ask` the user rejected or that a
    /// Stop cancelled.
    Denied,
}

/// ONE nested call's row — the frozen element shape (status file, "Frozen cross-task
/// contracts" item 1). A TYPE rather than a `json!` literal so the seven keys cannot
/// drift from the contract at compile time, and so the live snapshot and the final
/// `details.calls` are literally the same value.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct NestedRow {
    /// `"<parentCallId>/<n>"` — the SAME string the policy gate received as its
    /// `requestId`, so a row and its permission prompt correlate. NEVER a frame's
    /// `tool_call_id`.
    pub tool_call_id: String,
    /// The nested tool's name (what the script called).
    pub tool_name: String,
    /// The nested call's own `arguments`.
    pub args: Value,
    pub status: NestedStatus,
    /// The result's combined text block(s) joined with `\n` — `""` while running.
    pub text: String,
    pub is_error: bool,
    /// The result's `details`, or `{}` when it has none (the element always carries
    /// the key).
    pub details: Value,
}

impl NestedRow {
    /// A row at the moment its call starts: `running`, no text, no details.
    fn starting(tool_call_id: String, tool_name: String, args: Value) -> Self {
        Self {
            tool_call_id,
            tool_name,
            args,
            status: NestedStatus::Running,
            text: String::new(),
            is_error: false,
            details: json!({}),
        }
    }

    /// Settle the row from a finished [`ToolResult`]: `denied` when the GATE refused
    /// it, otherwise `ok`/`error` from the result's own `is_error`. The `denied` flag
    /// is a parameter rather than an inference from `is_error` because a failed
    /// EXECUTOR result and a gate refusal both arrive with `is_error: true`, and only
    /// the caller knows which one happened.
    fn settle(&mut self, result: &ToolResult, denied: bool) {
        self.text = joined_text(result);
        self.is_error = result.is_error || denied;
        self.status = if denied {
            NestedStatus::Denied
        } else if result.is_error {
            NestedStatus::Error
        } else {
            NestedStatus::Ok
        };
        self.details = result.details.clone().unwrap_or_else(|| json!({}));
    }
}

/// The result's text as ONE block: every `Text` block joined with `\n`, image blocks
/// contributing nothing. `bash` therefore yields its single interleaved
/// stdout+stderr block — there is no stdout/stderr split anywhere in [`ToolResult`],
/// and the bridge does not invent one (frozen contract item 3).
fn joined_text(result: &ToolResult) -> String {
    let parts: Vec<&str> = result
        .content
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            ContentBlock::Image { .. } => None,
        })
        .collect();
    parts.join("\n")
}

/// The JS-visible answer for one settled row — the uniform
/// `{ text, isError, details }` of frozen contract item 3, built from the SAME row
/// the transcript shows, so the script and the UI cannot disagree about an outcome.
fn uniform_json(row: &NestedRow) -> Value {
    json!({
        "text": row.text,
        "isError": row.is_error,
        "details": row.details,
    })
}

/// The row book: settled rows in COMPLETION order, then the rows still in flight.
///
/// Why two lists and not one: the contract wants `details.calls` in COMPLETION order
/// (which is what a script's `Promise.all` results correspond to), while a LIVE
/// snapshot must ALSO show the calls that started and have not finished — otherwise
/// the UI cannot show a row while its tool runs. So a row lives in `running` until it
/// finishes, then moves to the tail of `settled`, and the snapshot is
/// `settled ++ running`: once everything settles the array is purely
/// completion-ordered. D1 reconciles rows by `toolCallId` with last-writer-wins, so a
/// row moving from the tail into its completion slot updates it in place instead of
/// appending a second one.
#[derive(Debug, Default)]
struct RowBook {
    settled: Vec<NestedRow>,
    running: Vec<NestedRow>,
}

impl RowBook {
    /// Record a call that has started.
    fn start(&mut self, row: NestedRow) {
        self.running.push(row);
    }

    /// Record a call that has finished: drop its `running` row, append the settled.
    fn finish(&mut self, row: NestedRow) {
        if let Some(at) = self
            .running
            .iter()
            .position(|r| r.tool_call_id == row.tool_call_id)
        {
            self.running.remove(at);
        }
        self.settled.push(row);
    }

    /// The snapshot the live frames and the final result both carry.
    fn snapshot(&self) -> Vec<Value> {
        self.settled
            .iter()
            .chain(self.running.iter())
            .filter_map(|row| serde_json::to_value(row).ok())
            .collect()
    }
}

/// One host request. Discovery travels the SAME channel as the calls because it needs
/// the loop too — the MCP half asks A1's shared manager — and a host closure cannot
/// borrow the loop. One channel, one variant per JS-visible operation.
enum HostRequest {
    Call {
        name: String,
        arguments: Value,
        /// The answer, as the JSON text of the uniform `{text, isError, details}` JS
        /// result. A dropped sender means the script or the engine is gone, so
        /// nobody is listening — the ROW the call produced is still recorded.
        respond: oneshot::Sender<String>,
    },
    Search {
        query: String,
        limit: Option<usize>,
        respond: oneshot::Sender<String>,
    },
    Describe {
        name: String,
        respond: oneshot::Sender<String>,
    },
}

/// The per-engine wall clock, armed by whoever runs the script and read from the
/// interrupt handler.
///
/// WHY THIS EXISTS HERE AND NOT ONLY IN C1: C1's own `WallClock` is armed inside
/// `CodemodeEngine::eval`, which this bridge never calls (see the module doc). With no
/// deadline of its own, the bridge could only stop a PARKED script — a synchronously
/// spinning one holds the runtime lock and starves `race_turn`'s `sleep` arm forever,
/// which is exactly the defect C1 recorded as a DEVIATION against its own plan. The
/// handler therefore watches the turn token AND this deadline, so `WALL_CLOCK`
/// enforces itself inside bytecode (`while (true) {}`) as well as across a host
/// `await`.
struct WallClock {
    base: Instant,
    /// Nanos after `base`, or [`NO_DEADLINE`]. Written by the one in-flight script
    /// under the runtime lock, read from the handler on the same thread.
    deadline_ns: Arc<AtomicU64>,
}

/// The `deadline_ns` sentinel for "no deadline installed".
const NO_DEADLINE: u64 = u64::MAX;

impl WallClock {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            deadline_ns: Arc::new(AtomicU64::new(NO_DEADLINE)),
        }
    }
}

/// Arm a shared deadline `wall_clock` from `base`, returning the absolute value the
/// exception path reads back. Stored ABSOLUTE (nanos after `base`), so a second run on
/// the same engine cannot inherit the first's expiry.
///
/// A free function rather than a `WallClock` method because the only caller is the
/// script closure, which owns the `Instant` and the `Arc<AtomicU64>` separately — it
/// cannot borrow the `WallClock`, which `exec_codemode` still holds.
fn arm_deadline(base: &Instant, clock: &AtomicU64, wall_clock: Duration) -> u64 {
    let at = base.elapsed().as_nanos() as u64 + wall_clock.as_nanos() as u64;
    clock.store(at, Ordering::Release);
    at
}

/// Has the armed deadline passed? A free function so the handler closure can ask
/// without borrowing the whole [`WallClock`] — it owns only the `Instant` and the
/// `Arc<AtomicU64>`.
fn clock_expired(base: &Instant, at: u64) -> bool {
    at != NO_DEADLINE && base.elapsed().as_nanos() as u64 >= at
}

/// The specs a script may call AND discover: the advertised set minus
/// [`EXCLUDED_FROM_SCRIPT`]. A DISABLED tool is already absent, because
/// `advertised_specs()` applied the `enabled_tools` filter — a tool the model was not
/// allowed to call is not discoverable from a script either.
fn callable_specs(specs: &[ToolSpec]) -> Vec<Value> {
    specs
        .iter()
        .filter(|spec| {
            !EXCLUDED_FROM_SCRIPT
                .iter()
                .any(|(name, _)| *name == spec.name.as_str())
        })
        .map(|spec| {
            json!({
                "name": spec.name,
                "description": spec.description,
                "parameters": spec.parameters,
            })
        })
        .collect()
}

/// The JS source of the script-visible API: `tools.<name>` (one per callable spec, so
/// a typo is a `TypeError` and not a silent no-op), `ALL_TOOLS`, `searchTools`,
/// `describeTool`.
///
/// Everything crosses the boundary as JSON TEXT. `Value<'js>` is neither `Send` nor
/// serde-convertible, and this bridge must hand a NESTED result into a PARKED JS
/// promise, so a string is the one representation both sides can own across the
/// scheduler.
const PRELUDE: &str = r#"
globalThis.tools = {};
function __codemodeUniform(raw) {
  // The host always answers with the uniform {text, isError, details}; a malformed
  // answer is reported as a FAILED call, never thrown at the script.
  if (raw && typeof raw === "object") {
    return {
      text: typeof raw.text === "string" ? raw.text
          : (raw.text === undefined ? "" : String(raw.text)),
      isError: raw.isError === true,
      details: (raw.details && typeof raw.details === "object") ? raw.details : {},
    };
  }
  return { text: raw === undefined ? "" : String(raw), isError: true, details: {} };
}
function __codemodeBind(name) {
  return async function (args) {
    var payload;
    try {
      payload = (args === undefined || args === null) ? "{}" : JSON.stringify(args);
    } catch (e) {
      return { text: "args are not JSON-serializable: " + String(e), isError: true, details: {} };
    }
    if (typeof payload !== "string") {
      return { text: "args must be a plain object of JSON values", isError: true, details: {} };
    }
    return __codemodeUniform(JSON.parse(await __codemodeCall(name, payload)));
  };
}
for (var __i = 0; __i < ALL_TOOLS.length; __i++) {
  globalThis.tools[ALL_TOOLS[__i].name] = __codemodeBind(ALL_TOOLS[__i].name);
}
globalThis.searchTools = async function (query, opts) {
  var options;
  try {
    options = (opts === undefined || opts === null) ? "{}" : JSON.stringify(opts);
  } catch (e) {
    options = "{}";
  }
  if (typeof options !== "string") { options = "{}"; }
  return JSON.parse(await __codemodeSearchTools(String(query), options));
};
globalThis.describeTool = async function (name) {
  return JSON.parse(await __codemodeDescribeTool(String(name)));
};
// The script's completion value as ONE text block: a string stays itself, anything
// else is JSON, and `undefined` (the ordinary "no value" script) is the empty string
// rather than the word "undefined".
globalThis.__codemodeStringify = function (value) {
  if (typeof value === "string") { return value; }
  if (value === undefined || value === null) { return ""; }
  try {
    var encoded = JSON.stringify(value);
    return encoded === undefined ? String(value) : encoded;
  } catch (e) {
    return String(value);
  }
};
"#;

/// Install the host functions and the shim. Runs inside `with_context`, so every value
/// handed to JS is built from owned Rust and nothing is captured by reference: the
/// closures own an [`mpsc::UnboundedSender`] (cloneable, `'static`, and needing no
/// borrow of the loop).
fn install_world(
    ctx: rquickjs::Ctx<'_>,
    callable: &[Value],
    request_tx: mpsc::UnboundedSender<HostRequest>,
) -> rquickjs::Result<()> {
    let globals = ctx.globals();
    // `ALL_TOOLS` — the callable set as `{name, description}` objects, built through
    // QuickJS's OWN JSON parser so no description byte has to survive a Rust-side
    // escaping step. `describeTool` answers the `parameters` from the host side,
    // where the full spec still lives.
    let catalog: Vec<Value> = callable
        .iter()
        .map(|spec| json!({ "name": spec["name"], "description": spec["description"] }))
        .collect();
    let all: rquickjs::Value<'_> =
        ctx.json_parse(serde_json::to_string(&catalog).unwrap_or_else(|_| "[]".into()))?;
    globals.set("ALL_TOOLS", all)?;

    // `__codemodeCall(name, argsJson) -> Promise<string>`: the ONLY path from a script
    // into the host. It does the work that needs no `AgentLoop` — parse the arguments
    // to owned Rust and queue the request — then parks on the answer, which
    // `Bridge::run_script` fills in while the script waits.
    let tx = request_tx.clone();
    globals.set(
        "__codemodeCall",
        rquickjs::Function::new(
            ctx.clone(),
            rquickjs::function::Async(move |name: String, args_json: String| {
                let tx = tx.clone();
                async move {
                    let arguments = match serde_json::from_str::<Value>(&args_json) {
                        // A non-object argument is a script bug, reported in the
                        // uniform shape rather than thrown: the script can branch on
                        // `isError` like any other failed call.
                        Ok(value) if value.is_object() => value,
                        _ => {
                            return rquickjs::Result::Ok(
                                json!({
                                    "text": "args must be a plain object of JSON values",
                                    "isError": true,
                                    "details": {},
                                })
                                .to_string(),
                            )
                        }
                    };
                    let (respond, answer) = oneshot::channel();
                    if tx
                        .send(HostRequest::Call {
                            name,
                            arguments,
                            respond,
                        })
                        .is_err()
                    {
                        return rquickjs::Result::Ok(
                            json!({
                                "text": "codemode: the script host is no longer running",
                                "isError": true,
                                "details": {},
                            })
                            .to_string(),
                        );
                    }
                    // A dropped channel means the turn ended beneath the call; the
                    // script then sees a cancelled result, and the ROW the call
                    // produced is already in the snapshot.
                    let text = answer.await.unwrap_or_else(|_| {
                        json!({"text": "cancelled", "isError": true, "details": {}}).to_string()
                    });
                    rquickjs::Result::Ok(text)
                }
            }),
        ),
    )?;

    // `__codemodeSearchTools(query, optsJson)` / `__codemodeDescribeTool(name)`. Async
    // because the MCP half has to ask the servers, which needs the loop's shared
    // handle — hence the same channel trick as the call path.
    let tx = request_tx.clone();
    globals.set(
        "__codemodeSearchTools",
        rquickjs::Function::new(
            ctx.clone(),
            rquickjs::function::Async(move |query: String, opts_json: String| {
                let tx = tx.clone();
                async move {
                    let limit = serde_json::from_str::<Value>(&opts_json)
                        .ok()
                        .and_then(|opts| {
                            opts.get("limit")
                                .and_then(Value::as_u64)
                                .map(|n| n as usize)
                        });
                    let (respond, answer) = oneshot::channel();
                    if tx
                        .send(HostRequest::Search {
                            query,
                            limit,
                            respond,
                        })
                        .is_err()
                    {
                        return rquickjs::Result::Ok("{}".to_string());
                    }
                    rquickjs::Result::Ok(answer.await.unwrap_or_else(|_| "{}".to_string()))
                }
            }),
        ),
    )?;
    let tx = request_tx;
    globals.set(
        "__codemodeDescribeTool",
        rquickjs::Function::new(
            ctx.clone(),
            rquickjs::function::Async(move |name: String| {
                let tx = tx.clone();
                async move {
                    let (respond, answer) = oneshot::channel();
                    if tx.send(HostRequest::Describe { name, respond }).is_err() {
                        return rquickjs::Result::Ok("null".to_string());
                    }
                    rquickjs::Result::Ok(answer.await.unwrap_or_else(|_| "null".to_string()))
                }
            }),
        ),
    )?;

    ctx.eval::<(), _>(PRELUDE)?;
    Ok(())
}

/// The `codemode` tool: run `args.code` as a JavaScript program in a fresh QuickJS
/// engine, with every nested `tools.<name>(args)` routed through the loop's own policy
/// gate.
///
/// The engine lives and dies HERE — one per call, no engine field on `AgentLoop`.
pub(super) async fn exec_codemode(
    loop_: &AgentLoop,
    parent_call_id: &str,
    args: &Value,
    turn: &CancellationToken,
) -> ToolResult {
    let empty_calls = || json!({ "calls": [] });
    // `unknown_param_error` has already run for the OUTER call in `run_tool_batch`, so
    // a non-`code` TOP-LEVEL key never reaches here. A MISSING or non-string `code` is
    // a different thing — the key IS advertised, and the checker inspects keys and
    // never value types — so the bridge reports it itself.
    let Some(code) = args.get("code").and_then(Value::as_str) else {
        return ToolResult::fail(
            "codemode: the `code` parameter must be a string of JavaScript".to_string(),
            Some(empty_calls()),
        );
    };
    if code.trim().is_empty() {
        return ToolResult::fail("codemode: script is empty".to_string(), Some(empty_calls()));
    }

    let limits = EngineLimits {
        memory_bytes: crate::agent::codemode::engine::CODEMODE_MEMORY_LIMIT,
        stack_bytes: STACK_BYTES,
        wall_clock: WALL_CLOCK,
    };
    let engine = match CodemodeEngine::new(turn.clone(), limits).await {
        Ok(engine) => engine,
        Err(error) => {
            return ToolResult::fail(format!("codemode engine: {error}"), Some(empty_calls()))
        }
    };

    // The world the script sees. The interrupt handler (part 1 of the two-part
    // cancellation) is REPLACED with one that also watches this bridge's deadline,
    // because the clock C1 armed belongs to `eval`, which is never called here.
    let specs = loop_.advertised_specs();
    let callable = callable_specs(&specs);
    let (request_tx, request_rx) = mpsc::unbounded_channel::<HostRequest>();
    let clock = WallClock::new();
    let handler_token = turn.clone();
    let handler_base = clock.base;
    let handler_clock = Arc::clone(&clock.deadline_ns);
    engine
        .runtime()
        .set_interrupt_handler(Some(Box::new(move || {
            handler_token.is_cancelled()
                || clock_expired(&handler_base, handler_clock.load(Ordering::Acquire))
        })))
        .await;

    if let Err(error) = engine
        .with_context(|ctx| install_world(ctx, &callable, request_tx.clone()))
        .await
    {
        return ToolResult::fail(format!("codemode world: {error}"), Some(empty_calls()));
    }
    // The bridge's own sender goes first — each host closure holds a clone, so the
    // channel stays open for the engine's whole life and `run_script` drives it.
    drop(request_tx);

    // The ONE `ToolCtx` for this call, from the same private seam the direct route
    // uses: the frozen boundary, the per-turn rules, the policy, AND B1's event sink.
    // Cloning it (the derived `Clone` shares the `Arc` sink) is what lets a nested
    // built-in stream through the SAME sink as the parent.
    let ctx = loop_
        .permissions
        .to_tool_ctx(loop_.space_cwd.clone(), turn.clone());

    let bridge = Bridge {
        loop_,
        ctx,
        specs: &specs,
        turn,
        parent_call_id: parent_call_id.to_string(),
        parent_args: args.clone(),
        book: Arc::new(StdMutex::new(RowBook::default())),
    };

    // `race_turn` is C1's frozen part-2 race and the SINGLE authority on cancellation
    // here: the turn token and the wall clock. The rows live in `bridge.book` beside
    // it, so losing the race still reports every row the script had started — which is
    // what the contract's `status: "running"` exists for.
    let outcome = race_turn(
        bridge.run_script(&engine, &clock, code, request_rx),
        turn,
        WALL_CLOCK,
    )
    .await;

    let end = match outcome {
        // The completion value IS the parent's text block (frozen contract item 3:
        // the uniform shape, and the script's final value is what the model asked for).
        Ok(ScriptOutcome::Done(text)) => {
            return ToolResult::ok_text(text, Some(json!({ "calls": bridge.snapshot() })));
        }
        Ok(ScriptOutcome::Ended(end)) => end,
        // `race_turn`'s own arms: the script future was dropped mid-flight.
        Err(end) => end,
    };

    let details = json!({ "calls": bridge.snapshot() });
    match end {
        ScriptEnd::Done => ToolResult::ok_text("script completed".to_string(), Some(details)),
        ScriptEnd::Failed(message) => {
            ToolResult::fail(format!("codemode script failed: {message}"), Some(details))
        }
        ScriptEnd::Cancelled => ToolResult::fail("codemode: cancelled".to_string(), Some(details)),
        ScriptEnd::TimedOut => ToolResult::fail("codemode: timed out".to_string(), Some(details)),
    }
}

/// What the script run reported, in owned Rust (nothing JS may escape the
/// `async_with` closure — the `for<'js>` bound).
enum ScriptOutcome {
    /// The script completed; carries its completion value as one text block.
    Done(String),
    /// The run ended some other way; `exec_codemode` words the outcome.
    Ended(ScriptEnd),
}

/// One in-flight nested call: gated, dispatched, and answering its own JS promise
/// before it yields the settled row. Boxed because [`FuturesUnordered`] needs a single
/// concrete future type, and pinned because every one borrows the [`Bridge`].
type NestedFuture<'a> = futures_util::future::BoxFuture<'a, NestedRow>;

/// The bridge's per-call state: the loop borrow the gate needs, the `ToolCtx` clone
/// the executors and the sink need, and the row book.
///
/// A struct rather than eight parameters because this repo gates at zero clippy
/// warnings and `too_many_arguments` is one of them.
struct Bridge<'a> {
    loop_: &'a AgentLoop,
    ctx: ToolCtx,
    specs: &'a [ToolSpec],
    turn: &'a CancellationToken,
    parent_call_id: String,
    /// The PARENT call's own `arguments`, re-sent on every live frame so the frame's
    /// `args` field agrees with what `tool_execution_start` announced for the same id.
    parent_args: Value,
    book: Arc<StdMutex<RowBook>>,
}

impl Bridge<'_> {
    /// The rows the parent result carries (and that every live frame carried).
    fn snapshot(&self) -> Vec<Value> {
        self.book
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot()
    }

    /// Drive one script and its nested calls to completion.
    ///
    /// The shape is a three-way `select!` that only DECIDES, and one place that
    /// mutates: `inflight.next()` borrows the set for the whole `select!` body, so a
    /// handler that also pushed into it would not borrow-check. Polling the SCRIPT is
    /// the arm that pumps rquickjs's scheduler (and therefore the parked host
    /// futures), so it is polled first (`biased`) on every pass. Running the nested
    /// calls in THIS task is what lets them overlap (`Promise.all`) while still
    /// borrowing the loop for the gate.
    async fn run_script(
        &self,
        engine: &CodemodeEngine,
        clock: &WallClock,
        code: &str,
        mut request_rx: mpsc::UnboundedReceiver<HostRequest>,
    ) -> ScriptOutcome {
        let code = code.to_string();
        let turn_in_engine = self.turn.clone();
        let engine_clock = Arc::clone(&clock.deadline_ns);
        let engine_base = clock.base;
        let parent = self.parent_call_id.clone();

        // THE SCRIPT. `async_with` acquires the runtime lock per poll (it does not hold
        // it across `Pending`), and `eval_promise` is the mode in which a top-level
        // `await` is legal. The completion value is converted to owned Rust INSIDE the
        // closure — the `for<'js>` bound forbids a `Value<'js>` leaving it.
        let script_future = engine.context().async_with(async move |ctx| {
            // Arm the wall clock INSIDE the closure so it measures the script and not
            // the queueing before it; read the same value back on the exception path.
            let deadline_at = arm_deadline(&engine_base, &engine_clock, WALL_CLOCK);
            let outcome: rquickjs::Result<String> = async {
                let promise = ctx.eval_promise(code.as_str())?;
                let resolved: rquickjs::Value<'_> =
                    promise.into_future::<rquickjs::Value<'_>>().await?;
                // THE `{value: …}` WRAPPER — quickjs's own doing, not the shim's.
                // `JS_EVAL_FLAG_ASYNC` makes the script body an ASYNC function
                // (`quickjs.c`: `fd->func_kind = JS_FUNC_ASYNC`), and for a non-module
                // async eval the compiler emits `OP_object / put_field("value")`
                // around the completion value (`quickjs.c`, the `eval_ret_idx` return
                // path). So the promise resolves to `{ value: <the script's value> }`
                // ALWAYS, and reading `value` back is the unwrap — the script's own
                // value can never be confused with the wrapper, because it is always
                // nested one level inside it. The fallbacks are for a shape that
                // should not occur (a non-object resolution), not for a second key.
                let completion: rquickjs::Value<'_> = match resolved.as_object() {
                    Some(object) => object.get("value").unwrap_or_else(|_| resolved.clone()),
                    None => resolved.clone(),
                };
                let stringify: rquickjs::Function<'_> = ctx.globals().get("__codemodeStringify")?;
                stringify.call::<_, String>((completion,))
            }
            .await;
            match outcome {
                Ok(text) => ScriptOutcome::Done(text),
                Err(error) => ScriptOutcome::Ended(exception_verdict(
                    ctx,
                    &turn_in_engine,
                    &engine_base,
                    deadline_at,
                    error,
                )),
            }
        });
        let mut script = Box::pin(script_future);
        let mut script_outcome: Option<ScriptOutcome> = None;

        // The per-parent counter that mints `<parent>/<n>` — the ISSUE order, which is
        // what the gate's `requestId` and a row's `toolCallId` both carry.
        let mut seq: u32 = 0;
        // One future per in-flight nested call. Each answers its own JS promise from
        // inside itself and then yields its settled row.
        let mut inflight: FuturesUnordered<NestedFuture<'_>> = FuturesUnordered::new();

        loop {
            // What the pass decided; every mutation of `inflight` happens after the
            // `select!` expression, once its borrows are released.
            enum Step {
                Script(ScriptOutcome),
                Row(NestedRow),
                Request(Option<HostRequest>),
            }
            let step = tokio::select! {
                biased;

                // 1. The script FIRST: polling it is the ONLY thing that pumps
                //    rquickjs's scheduler, so an answer a nested future sent is
                //    delivered to the parked JS promise by THIS arm.
                outcome = &mut script => Step::Script(outcome),

                // 2. A finished nested call.
                Some(row) = inflight.next(), if !inflight.is_empty() => Step::Row(row),

                // 3. A new host request.
                maybe_request = request_rx.recv() => Step::Request(maybe_request),
            };

            match step {
                Step::Script(outcome) => {
                    script_outcome = Some(outcome);
                    self.drain_after_script(&mut inflight, &mut request_rx, &mut seq)
                        .await;
                    break;
                }
                Step::Row(row) => self.settle_row(row),
                Step::Request(Some(HostRequest::Call {
                    name,
                    arguments,
                    respond,
                })) => {
                    let id = format!("{parent}/{seq}");
                    seq += 1;
                    self.start_row(&id, &name, &arguments);
                    // The row APPEARS before the call runs: the UI sees it start.
                    self.emit_live();
                    inflight.push(self.spawn_nested(id, name, arguments, respond));
                }
                Step::Request(Some(HostRequest::Search {
                    query,
                    limit,
                    respond,
                })) => {
                    let text = self.search_tools(&query, limit).await;
                    let _ = respond.send(text);
                }
                Step::Request(Some(HostRequest::Describe { name, respond })) => {
                    let text = self.describe_tool(&name).await;
                    let _ = respond.send(text);
                }
                // Every sender dropped: the engine is gone, so nothing can arrive.
                Step::Request(None) => break,
            }
        }

        script_outcome.unwrap_or(ScriptOutcome::Ended(ScriptEnd::Cancelled))
    }

    /// The script is over. Start whatever it issued but never awaited (its row belongs
    /// in `details.calls` either way), then finish the calls already running — bounded,
    /// because a wedged executor must not hold the turn.
    ///
    /// A call the drain does NOT finish keeps `status: "running"` — the contract's word
    /// for a row that started and did not complete. Its JS promise is answered by the
    /// dropped-sender fallback, to a script that by then no longer exists.
    async fn drain_after_script<'a>(
        &'a self,
        inflight: &mut FuturesUnordered<NestedFuture<'a>>,
        request_rx: &mut mpsc::UnboundedReceiver<HostRequest>,
        seq: &mut u32,
    ) {
        let parent = self.parent_call_id.clone();
        while let Ok(request) = request_rx.try_recv() {
            // Discovery queries are host-side and add no row; with no script left to
            // answer, their senders simply drop.
            let HostRequest::Call {
                name,
                arguments,
                respond,
            } = request
            else {
                continue;
            };
            let id = format!("{parent}/{seq}");
            *seq += 1;
            self.start_row(&id, &name, &arguments);
            inflight.push(self.spawn_nested(id, name, arguments, respond));
        }
        let deadline = tokio::time::Instant::now() + ORPHAN_DRAIN;
        while !inflight.is_empty() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if let Some(row) = tokio::select! {
                biased;
                Some(row) = inflight.next() => Some(row),
                _ = tokio::time::sleep(remaining) => None,
                _ = self.turn.cancelled() => None,
            } {
                self.settle_row(row);
            } else {
                break;
            }
        }
    }

    /// Record a call's `running` row and return it, so the SAME row value drives the
    /// future that settles it (one construction, no field can drift).
    fn start_row(&self, id: &str, name: &str, arguments: &Value) -> NestedRow {
        let row = NestedRow::starting(id.to_string(), name.to_string(), arguments.clone());
        self.book
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .start(row.clone());
        row
    }

    /// Start one nested call: its own future, gated and dispatched against the loop,
    /// which answers the parked JS promise and yields the settled row.
    fn spawn_nested(
        &self,
        id: String,
        name: String,
        arguments: Value,
        respond: oneshot::Sender<String>,
    ) -> NestedFuture<'_> {
        // The row STARTS here — with the real name and args — and is settled in
        // place. Building a fresh row after the call finished would drop the two
        // fields the UI shows while it runs (the row's `toolName` and `args` are
        // what a `running` row is made of), and it would let the settled row
        // disagree with the row the live frame already announced.
        let mut row = NestedRow::starting(id, name, arguments);
        Box::pin(async move {
            let (result, denied) = self.run_nested(&row).await;
            row.settle(&result, denied);
            // The script resumes as soon as the scheduler is pumped, and the row
            // settles in the same step — so script and transcript cannot disagree.
            let _ = respond.send(uniform_json(&row).to_string());
            row
        })
    }

    /// ONE nested call, through the SAME gate the direct batch route applies — in the
    /// SAME order (cancellation beats rejection beats the gate), so a stopped turn can
    /// never raise a permission prompt and a malformed call can never prompt either.
    ///
    /// Returns the [`ToolResult`] and whether the GATE refused it, which is what makes
    /// a row `denied` rather than `error`.
    async fn run_nested(&self, starting: &NestedRow) -> (ToolResult, bool) {
        // The synthesized call: the SAME shape a direct provider call arrives in, so
        // every check below is the check the direct route runs. Its id is the row's
        // `<parent>/<n>` — the gate's `requestId` and the element's `toolCallId` are
        // the same string by construction, which is what lets a row and its prompt
        // be correlated.
        let nested = ToolCall {
            id: starting.tool_call_id.clone(),
            name: starting.tool_name.clone(),
            arguments: starting.args.clone(),
        };
        // 0. CANCELLATION beats everything (`turn.rs`: a stopped turn never prompts,
        //    and the nested call dies with the script).
        if self.turn.is_cancelled() {
            return (cancelled_tool_result(), false);
        }
        // 1. The `enabled_tools` filter — the SAME predicate `dispatch_tool` gates on
        //    and its exact wording, so a disabled tool reads identically.
        if !is_tool_enabled(self.loop_.enabled_tools(), &nested.name) {
            return (
                ToolResult::fail(format!("tool not enabled: {}", nested.name), None),
                false,
            );
        }
        // 2. The unknown-param check — REJECTED, not warned, and deliberately BEFORE
        //    the gate: a call that will not run must never raise a prompt. The message
        //    is `unknown_param_error`'s own, verbatim, against the SAME advertised
        //    specs the model was shown (ADR 0017's one value).
        if let Some(error) = unknown_param_error(self.specs, &nested) {
            return (ToolResult::fail(error, None), false);
        }
        // 3. The argument-level exclusion — the interactive `mcp` `auth` action, which
        //    the NAME-based list cannot catch because `mcp` itself is callable. Like the
        //    two checks above it is a bridge refusal, not a gate refusal, so the row
        //    reads `error` rather than `denied`: `denied` is reserved for what the POLICY
        //    GATE refused, and the gate never sees this call.
        if let Some(refusal) = interactive_args(&nested.name, &nested.arguments) {
            return (ToolResult::fail(refusal, None), false);
        }
        // 4. The gate.
        match self.loop_.policy_verdict(&nested) {
            Some(Decision::Ask) => {
                let guard = self.loop_.guard_target(&nested);
                if self
                    .loop_
                    .gate_tool(&nested, self.turn, guard.as_deref())
                    .await
                {
                    (self.dispatch_nested(&nested).await, false)
                } else {
                    // THE SAME WORDING a direct denial returns (`turn.rs`'s Ask arm), so
                    // the model cannot tell a nested call from a direct one.
                    (
                        ToolResult::fail("permission denied".to_string(), None),
                        true,
                    )
                }
            }
            Some(Decision::Deny) => (self.loop_.deny_tool_result(&nested), true),
            // `Allow`, `None` (an ungated tool) and `Confine`. `Confine` is
            // structurally `bash`-only — `decision_for`'s `Confine` arm is spelled for
            // `PolicedTool::Shell` alone — so dispatching IS the confined run: the
            // executor reads `ctx.file_policy.shell` and sandboxes itself.
            _ => (self.dispatch_nested(&nested).await, false),
        }
    }

    /// Run a gated nested call: a built-in through `execute_tool` on the CLONED
    /// [`ToolCtx`] (NEVER `dispatch_tool`, which holds `&mut self` for the whole turn
    /// and would deadlock a script running inside it), or MCP through A1's shared
    /// handle.
    async fn dispatch_nested(&self, nested: &ToolCall) -> ToolResult {
        if nested.name == "mcp" {
            // RACED against the TURN token exactly as the direct arm races it.
            tokio::select! {
                result = mcp_tool(&self.loop_.mcp, &nested.arguments, self.turn) => result,
                _ = self.turn.cancelled() => cancelled_tool_result(),
            }
        } else {
            execute_tool(&self.ctx, &nested.name, &nested.arguments).await
        }
    }

    /// Move a settled row into the book and stream the new snapshot.
    fn settle_row(&self, row: NestedRow) {
        self.book
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .finish(row);
        self.emit_live();
    }

    /// The live frame: B1's sink, keyed on the PARENT call's id, carrying a serialized
    /// [`ToolResult`] whose `details.calls` is the current snapshot.
    ///
    /// `args` is the PARENT call's own arguments (the same value
    /// `tool_execution_start` announced), so the raw event stream stays
    /// self-consistent for one call across its whole life; `normalize`'s
    /// `tool_execution_update` arm forwards only `tool_call_id` and
    /// `partial_result` to the frame, so this costs the transcript nothing.
    ///
    /// `is_error: false` while the script runs — the parent has not failed yet, and a
    /// frame claiming failure would paint the parent row red for a nested call the
    /// script may still recover from. The final `tool_execution_end` replaces the whole
    /// `rawOutput` (`merge_json`'s shallow top-level overwrite), so nothing here leaks
    /// into the settled state.
    fn emit_live(&self) {
        let partial = ToolResult::ok_text(
            "codemode: running".to_string(),
            Some(json!({ "calls": self.snapshot() })),
        );
        self.ctx.emit_tool_execution_update(
            self.parent_call_id.as_str(),
            "codemode",
            self.parent_args.clone(),
            serde_json::to_value(&partial).unwrap_or(Value::Null),
        );
    }

    /// `searchTools(query, {limit})`: the built-ins (the callable advertised set) and
    /// the MCP tools, matched case-insensitively on name and description — the same
    /// predicate the `mcp` proxy's `search` action applies in `agent/mcp/tool.rs`.
    ///
    /// The predicate is RE-APPLIED rather than CALLED: `collect_tools`, `in_scope` and
    /// `miss_result` in `agent/mcp/tool.rs` are module-private and that file is not
    /// this task's to edit. What the bridge DOES reuse is the manager's own public API
    /// (`server_names` + `list_tools`), so the SERVER SET is the same set the proxy
    /// searches, and a server that will not answer is reported in the answer's
    /// `skipped` rather than silently shrinking the world.
    async fn search_tools(&self, query: &str, limit: Option<usize>) -> String {
        let mut hits: Vec<Value> = Vec::new();
        for spec in callable_specs(&self.loop_.advertised_specs()) {
            let name = spec["name"].as_str().unwrap_or_default();
            let description = spec["description"].as_str().unwrap_or_default();
            if matches_query(query, name, description) {
                hits.push(json!({
                    "name": name,
                    "description": description,
                    "kind": "builtin",
                }));
            }
        }
        let mut skipped: Vec<Value> = Vec::new();
        for server in self.loop_.mcp.server_names() {
            match self
                .loop_
                .mcp
                .list_tools(&server, self.turn, MCP_TIMEOUT)
                .await
            {
                Ok(tools) => {
                    for tool in tools {
                        let description = tool.description.unwrap_or_default();
                        // The CALLABLE form (`<server>_<tool>`) matches too, so a name
                        // copied out of a listing searches back — the proxy's own rule.
                        let callable = format!("{server}_{}", tool.name);
                        if matches_query(query, &callable, &description)
                            || matches_query(query, &tool.name, &description)
                        {
                            hits.push(json!({
                                "name": tool.name,
                                "description": description,
                                "kind": "mcp",
                                "server": server,
                                "callable": callable,
                            }));
                        }
                    }
                }
                Err(reason) => skipped.push(json!({ "server": server, "reason": reason })),
            }
        }
        // `limit` is an optional cap; absent means no cap (a script can always slice).
        if let Some(limit) = limit {
            hits.truncate(limit);
        }
        json!({ "tools": hits, "skipped": skipped }).to_string()
    }

    /// `describeTool(name)`: a built-in's advertised `parameters`, or an MCP tool's
    /// `inputSchema`; `null` when nothing answers that name.
    ///
    /// The MCP half reads the schema as DATA through the manager's public `list_tools`
    /// rather than delegating to the proxy's `describe` action — that action renders a
    /// human-readable text block, and a script needs the schema it can build `args`
    /// from.
    async fn describe_tool(&self, name: &str) -> String {
        if let Some(spec) = self
            .loop_
            .advertised_specs()
            .iter()
            .find(|spec| spec.name == name)
        {
            // An EXCLUDED name still has a spec, and answering it plainly would
            // advertise the very tool the bridge refuses to install.
            if EXCLUDED_FROM_SCRIPT
                .iter()
                .any(|(excluded, _)| *excluded == name)
            {
                return json!({
                    "name": name,
                    "error": "not callable from a codemode script (interactive or recursive)",
                })
                .to_string();
            }
            return json!({
                "name": spec.name,
                "kind": "builtin",
                "description": spec.description,
                "parameters": spec.parameters,
            })
            .to_string();
        }
        for server in self.loop_.mcp.server_names() {
            let Ok(tools) = self
                .loop_
                .mcp
                .list_tools(&server, self.turn, MCP_TIMEOUT)
                .await
            else {
                continue;
            };
            if let Some(tool) = tools.into_iter().find(|tool| tool.name == name) {
                return json!({
                    "name": tool.name,
                    "kind": "mcp",
                    "server": server,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                })
                .to_string();
            }
        }
        "null".to_string()
    }
}

/// The exception-path rule, replicated from C1's private `exception_verdict`.
///
/// WHY IT MUST BE REPLICATED HERE: the interrupt handler raises an UNcatchable
/// exception, so a cancelled or timed-out script arrives as `Error::Exception` —
/// indistinguishable from a genuine `throw`. Mapping every exception to `Failed` would
/// make "hot loop + Stop" a coin-flip against `race_turn`'s arms. The token is checked
/// FIRST and the deadline second, so both outcomes are deterministic. (C1 owns the
/// RULE; this bridge owns the eval, so the rule travels with the eval —
/// `CodemodeEngine::exception_verdict` is private and takes `&self`.)
fn exception_verdict(
    ctx: rquickjs::Ctx<'_>,
    turn: &CancellationToken,
    base: &Instant,
    deadline_at: u64,
    error: rquickjs::Error,
) -> ScriptEnd {
    if turn.is_cancelled() {
        return ScriptEnd::Cancelled;
    }
    if clock_expired(base, deadline_at) {
        return ScriptEnd::TimedOut;
    }
    if !matches!(error, rquickjs::Error::Exception) {
        return ScriptEnd::Failed(error.to_string());
    }
    let message = ctx
        .catch()
        .as_object()
        .cloned()
        .and_then(rquickjs::Exception::from_object)
        // A thrown NON-Error value (a string, a number, …) has no message.
        .and_then(|exception| exception.message())
        .unwrap_or_else(|| "unknown exception".to_string());
    ScriptEnd::Failed(message)
}

/// The `mcp` search predicate: a case-insensitive substring in the name OR in the
/// description (the proxy's rule, `agent/mcp/tool.rs`'s `search` arm).
///
/// Case-insensitivity lives HERE, on all three sides, rather than in the caller: a
/// predicate that only works on a pre-lowercased needle is one refactor away from a
/// silent miss, and the proxy it mirrors lowercases both sides at the point of
/// comparison. An EMPTY query matches everything — a bare `searchTools("")` is a
/// listing, not a miss.
fn matches_query(query: &str, name: &str, description: &str) -> bool {
    let needle = query.to_lowercase();
    needle.is_empty()
        || name.to_lowercase().contains(&needle)
        || description.to_lowercase().contains(&needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frozen element keys — the shape D1 renders and the status file freezes. A
    /// rename in the type reddens this before it reddens the frontend.
    #[test]
    fn the_nested_row_serializes_the_frozen_element_shape() {
        let row = NestedRow::starting(
            "parent42/3".to_string(),
            "read".to_string(),
            json!({ "path": "a.txt" }),
        );
        let value = serde_json::to_value(&row).expect("a row serializes");
        assert_eq!(
            value["toolCallId"], "parent42/3",
            "the element id is `<parent>/<n>`: {value}"
        );
        assert_eq!(value["toolName"], "read", "{value}");
        assert_eq!(value["args"]["path"], "a.txt", "{value}");
        assert_eq!(value["status"], "running", "{value}");
        assert_eq!(value["text"], "", "{value}");
        assert_eq!(value["isError"], false, "{value}");
        assert_eq!(value["details"], json!({}), "{value}");
        let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "args",
                "details",
                "isError",
                "status",
                "text",
                "toolCallId",
                "toolName"
            ],
            "exactly the seven frozen keys, no more: {value}"
        );
    }

    /// A settled row: `ok` for a success, `error` for a failed result, `denied` for a
    /// gate refusal — and `details` carried through, `{}` when absent.
    #[test]
    fn settling_a_row_picks_the_status_from_the_gate_and_the_result() {
        let ok = ToolResult::ok_text("hi".to_string(), Some(json!({ "exitCode": 0 })));
        let mut row = NestedRow::starting("p/0".to_string(), "bash".to_string(), json!({}));
        row.settle(&ok, false);
        assert_eq!(row.status, NestedStatus::Ok);
        assert_eq!(row.text, "hi");
        assert!(!row.is_error);
        assert_eq!(row.details, json!({ "exitCode": 0 }));

        let failed = ToolResult::fail("boom".to_string(), None);
        let mut row = NestedRow::starting("p/1".to_string(), "bash".to_string(), json!({}));
        row.settle(&failed, false);
        assert_eq!(row.status, NestedStatus::Error);
        assert!(row.is_error);
        assert_eq!(
            row.details,
            json!({}),
            "an absent `details` becomes an empty object"
        );

        // The gate's refusal: the SAME `is_error: true` result, a different status.
        let denial = ToolResult::fail("permission denied".to_string(), None);
        let mut row = NestedRow::starting("p/2".to_string(), "read".to_string(), json!({}));
        row.settle(&denial, true);
        assert_eq!(row.status, NestedStatus::Denied);
        assert!(row.is_error);
    }

    /// `bash` yields ONE combined text block — the bridge joins content blocks and
    /// never splits stdout from stderr (there is nothing to split).
    #[test]
    fn the_text_block_is_one_join_and_images_contribute_nothing() {
        let result = ToolResult {
            content: vec![
                ContentBlock::Text {
                    text: "out".to_string(),
                },
                ContentBlock::Image {
                    image: crate::agent::tools::ImageRef {
                        data: "AAA".to_string(),
                        mime_type: "image/png".to_string(),
                    },
                },
                ContentBlock::Text {
                    text: "err".to_string(),
                },
            ],
            details: None,
            is_error: false,
        };
        assert_eq!(joined_text(&result), "out\nerr");
    }

    /// The snapshot is `settled ++ running`: a started-but-unfinished call appears (as
    /// `running`) beside the finished ones, and once everything settles the array is
    /// purely completion-ordered.
    #[test]
    fn the_snapshot_holds_settled_rows_then_the_ones_in_flight() {
        let mut book = RowBook::default();
        book.start(NestedRow::starting(
            "p/0".to_string(),
            "bash".to_string(),
            json!({}),
        ));
        book.start(NestedRow::starting(
            "p/1".to_string(),
            "bash".to_string(),
            json!({}),
        ));
        let mut done = NestedRow::starting("p/1".to_string(), "bash".to_string(), json!({}));
        done.settle(&ToolResult::ok_text("fast".to_string(), None), false);
        book.finish(done);
        let snapshot = book.snapshot();
        assert_eq!(snapshot.len(), 2, "{snapshot:?}");
        assert_eq!(
            snapshot[0]["toolCallId"], "p/1",
            "the settled row comes FIRST: {snapshot:?}"
        );
        assert_eq!(snapshot[0]["status"], "ok");
        assert_eq!(snapshot[1]["toolCallId"], "p/0");
        assert_eq!(snapshot[1]["status"], "running", "in flight reads running");
    }

    /// The excluded set is the spec's §7.1 decision plus the recursion the spec could
    /// not have listed. Every name here is ABSENT from the callable set; every other
    /// advertised tool is present.
    #[test]
    fn the_excluded_tools_are_neither_callable_nor_discoverable() {
        let callable = callable_specs(&crate::agent::harness::r#loop::tool_specs());
        let names: Vec<&str> = callable
            .iter()
            .map(|spec| spec["name"].as_str().unwrap_or_default())
            .collect();
        for (name, _) in EXCLUDED_FROM_SCRIPT {
            assert!(
                !names.contains(name),
                "`{name}` is excluded from scripts but reached the callable set: {names:?}"
            );
        }
        // The interactive/recursive tools ARE advertised to the model — the exclusion
        // is the bridge's, not the census's.
        let advertised: Vec<String> = crate::agent::harness::r#loop::tool_specs()
            .into_iter()
            .map(|spec| spec.name)
            .collect();
        for name in [
            "ask",
            "sudo_exec",
            "manage_todo_list",
            "subagent",
            "list_agents",
        ] {
            assert!(
                advertised.iter().any(|n| n == name),
                "`{name}` should still be advertised to the MODEL: {advertised:?}"
            );
        }
        // And the built-ins a script CAN reach are advertised and callable.
        for name in ["bash", "read", "write", "edit", "grep", "mcp"] {
            assert!(
                names.contains(&name),
                "`{name}` is advertised and must be callable: {names:?}"
            );
        }
    }

    /// The exclusion has an ARGUMENT-level half, because `mcp`'s interactive OAuth
    /// action is not reachable by name — `mcp` itself is callable.
    #[test]
    fn the_interactive_mcp_auth_action_is_refused_while_every_other_action_is_not() {
        // The refusal is scoped to the ONE action that blocks on a human.
        assert!(interactive_args("mcp", &json!({ "action": "auth", "server": "a" })).is_some());
        // A non-object `action`, a missing `action`, and the other actions all pass —
        // `status` is the DEFAULT arm of the tool, so refusing it would break every
        // nested MCP call.
        for args in [
            json!({ "action": "status" }),
            json!({}),
            json!({ "server": "a" }),
            json!({ "search": "echo" }),
            json!({ "tool": "a_echo", "args": {} }),
            json!({ "action": 7 }),
            json!("not an object"),
        ] {
            assert_eq!(
                interactive_args("mcp", &args),
                None,
                "`{args}` is not the interactive action and must dispatch"
            );
        }
        // And it is scoped to `mcp`: no other tool's `action` key means anything here,
        // so a `bash` command merely CONTAINING the word must not be refused.
        assert_eq!(
            interactive_args("bash", &json!({ "action": "auth", "command": "true" })),
            None
        );
        // `connect` is NOT refused, on purpose: `call_tool`/`list_tools` each run
        // `ensure_connected`, so a nested MCP call spawns the same child regardless.
        // The pin is that this stays a deliberate choice, not a silent omission.
        assert_eq!(interactive_args("mcp", &json!({ "connect": "a" })), None);
    }

    /// The search predicate is case-insensitive over BOTH name and description, and an
    /// empty query matches everything (a bare `searchTools("")` lists).
    #[test]
    fn the_search_matcher_is_case_insensitive_over_name_and_description() {
        assert!(matches_query("bash", "bash", ""));
        // BOTH sides are folded HERE, not by the caller — a needle the script
        // typed in caps is the ordinary case, and a predicate that only worked on
        // a pre-lowercased needle would silently miss it.
        assert!(matches_query("BASH", "bash", ""));
        assert!(matches_query("bash", "BASH", ""));
        assert!(matches_query("SHELL", "bash", "Run a Shell Command"));
        assert!(!matches_query("shell", "bash", "no mention here"));
        assert!(matches_query("", "anything", "whatever"));
    }

    /// A `WallClock` armed in the past has expired and the sentinel has not — the two
    /// arms the interrupt handler distinguishes.
    #[test]
    fn the_wall_clock_distinguishes_an_armed_deadline_from_no_deadline() {
        let clock = WallClock::new();
        assert!(
            !clock_expired(&clock.base, NO_DEADLINE),
            "an unarmed clock never expires"
        );
        let at = arm_deadline(&clock.base, &clock.deadline_ns, Duration::ZERO);
        assert_ne!(at, NO_DEADLINE, "arming installs a deadline");
        assert!(
            clock_expired(&clock.base, at),
            "a zero-length deadline is already past"
        );
    }

    /// The description a script discovers is the ONE literal the model sees — the
    /// bridge re-reads `tool_specs()` rather than carrying its own copy (ADR 0017).
    #[test]
    fn the_callable_specs_carry_the_advertised_description_verbatim() {
        let spec = crate::agent::harness::r#loop::tool_specs()
            .into_iter()
            .find(|s| s.name == "bash")
            .expect("`bash` is advertised");
        let callable = callable_specs(std::slice::from_ref(&spec));
        assert_eq!(callable.len(), 1);
        assert_eq!(
            callable[0]["description"].as_str().expect("a description"),
            spec.description,
            "byte-identical, not paraphrased"
        );
        assert_eq!(
            callable[0]["parameters"], spec.parameters,
            "the schema the script discovers IS the advertised schema"
        );
    }

    /// The JS answer for a row is the uniform triple — and it is built from the SAME
    /// row the transcript carries, so script and UI cannot disagree.
    #[test]
    fn the_js_answer_is_the_uniform_triple_built_from_the_row() {
        let mut row = NestedRow::starting("p/0".to_string(), "bash".to_string(), json!({}));
        row.settle(
            &ToolResult::fail("no such file".to_string(), Some(json!({"code": 2}))),
            false,
        );
        assert_eq!(
            uniform_json(&row),
            json!({ "text": "no such file", "isError": true, "details": {"code": 2} })
        );
        let mut keys: Vec<String> = uniform_json(&row)
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["details", "isError", "text"]);
    }

    /// The bridge's `EngineLimits` take the production memory cap from C1's constant
    /// (256 MiB, spec §7.1) rather than a fresh number.
    #[test]
    fn the_engine_limits_use_c1s_memory_constant() {
        assert_eq!(
            crate::agent::codemode::engine::CODEMODE_MEMORY_LIMIT,
            256 * 1024 * 1024
        );
        assert_eq!(STACK_BYTES, 256 * 1024);
    }

    /// A `ToolResult` with NO text blocks (an image-only result) joins to `""` — the
    /// row's `text` is always a string, so D1 never renders `undefined`.
    #[test]
    fn a_result_with_no_text_blocks_joins_to_the_empty_string() {
        let result = ToolResult {
            content: vec![ContentBlock::Image {
                image: crate::agent::tools::ImageRef {
                    data: "AAA".to_string(),
                    mime_type: "image/png".to_string(),
                },
            }],
            details: None,
            is_error: false,
        };
        assert_eq!(joined_text(&result), "");
    }
}

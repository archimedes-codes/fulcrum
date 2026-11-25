//! (ADR 0102 / issue #183 F1) The Tool registry — the engine's sole dispatch
//! authority.
//!
//! # What this module inverts
//!
//! Before this module, the engine KNEW its own tool names in three separate
//! places: `execute_tool`'s `match` dispatched them, `dispatch_tool`'s built-in
//! OR-list routed them, and `EXCLUDED_FROM_SCRIPT` excluded them from scripts by
//! name. The desktop owned none of it, so a host could not add a tool without a
//! crate release.
//!
//! After it, an engine is handed a set of [`ToolEntry`] and dispatches through
//! it. The swappable end is the ENGINE: Archimedes Desktop may one day run a
//! harness that is not fulcrum, so the desktop owns the tool surface and any
//! engine must accept a registry handed to it. F1 was the INVERSION and F2 the
//! generic frame; F3 then deleted the Task subsystem, which is what took
//! `tool_specs()` 26 → 19 while the model kept being shown the same 26 names —
//! 19 from [`crate::harness::tool_specs::tool_specs`] plus 7 arriving as
//! `StartEnv::host_tools`. The invariant is pinned by
//! `harness::tool_specs::tests::the_composed_advertised_set_is_exactly_the_twenty_six_names`.
//!
//! # The bound on the claim, and why it is a bound
//!
//! The registry is the sole authority for **everything it can be**. The seven
//! loop-internal tools (`ask`, `sudo_exec`, `manage_todo_list`, `subagent`,
//! `list_agents`, `mcp`, `codemode`) STAY loop arms, permanently and honestly:
//! their executor is `&mut AgentLoop`, not a `ToolCtx`. `codemode`'s bridge needs
//! the live loop (which is exactly why it is excluded from scripts), `subagent`
//! needs the parent's catalog and the cost fold, `ask` needs the pending-gate map,
//! and `mcp_tool(&McpManager, &Value, &CancellationToken)` is not expressible as a
//! [`ToolExecutor`] over `&ToolCtx` at all. Pretending otherwise would mean
//! holding `Arc`s to loop state for a capability no outside host could implement.
//!
//! So the enforced claim is: **registry set ⊎ loop-internal set = advertised set,
//! and the two are disjoint** — [`crate::harness::loop::tests::tools_enabled::every_advertised_tool_reaches_an_executor`]
//! is that census, and it exists to catch the one failure this refactor can hide:
//! a tool ADVERTISED and UNREACHABLE.
//!
//! # Both dispatch paths resolve through here
//!
//! [`dispatch_tool`](crate::harness::r#loop::AgentLoop)'s built-in arm and
//! [`dispatch_nested`](crate::harness::r#loop::codemode) (the codemode bridge).
//! The second is load-bearing, not tidiness: left calling `execute_tool`, a host
//! entry would be advertised to scripts and DEAD to them — `ALL_TOOLS` is built
//! from the advertised set minus the excluded names, so `tools.<host_verb>(…)`
//! would be discoverable while the dispatch answered `unknown tool`. The
//! advertised-set census does not catch that, because it drives `dispatch_tool`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

// (H7) `ToolSpec` lives in `provider_types.rs`, NOT in `tools/types.rs` — it has
// never been there. The cross-module import is new for THIS module tree and is
// deliberate: an entry pairs a spec with the executor that answers it, and the
// spec type is the provider-facing one the model request carries.
use crate::provider_types::ToolSpec;
use crate::tools::types::{ToolCtx, ToolResult};

/// The `match`-arm-to-adapter shim: wraps a free `async fn(&ToolCtx, &Value) ->
/// ToolResult` as the [`NativeRun`] the registry stores.
///
/// A macro because the body is identical twelve times, and a `fn` ITEM is what makes
/// the coercion to a `fn` POINTER check: the `async move` block OWNS the `ctx` and
/// the `params`, which is what lets the boxed future be `'static` while the `exec_*`
/// function behind it still takes them by reference.
macro_rules! native {
    ($exec:path) => {{
        fn __run(ctx: ToolCtx, params: Value) -> Pin<Box<dyn Future<Output = ToolResult> + Send>> {
            Box::pin(async move { $exec(&ctx, &params).await })
        }
        __run as NativeRun
    }};
}

/// One tool's executable half.
///
/// # Why this shape and not `#[async_trait]`
///
/// This crate spells this shape out by hand rather than reaching for the macro:
/// [`RenderRequester::request_render`](crate::web::RenderRequester) is the live
/// precedent, and `seams::TaskCaller::call` was the second until F3 deleted the
/// Task subsystem. They spell it out for a reason that is a fact about the
/// language, not a style choice: **`async
/// fn` in a trait is not object safe**, because it returns an unnameable
/// `impl Future` whose lifetime is tied to `&self` — so `Arc<dyn ToolExecutor>`
/// (which is exactly what [`ToolEntry::executor`] must be, for the same reason
/// `ToolCtx`'s two `Arc<dyn …>` fields are `Arc` and not `Box`: the derived
/// `Clone` has to SHARE it) cannot be built from one.
///
/// `async-trait` is a declared dependency of this crate and would desugar to the
/// SAME `Pin<Box<dyn Future + Send + 'static>>`, so the choice is not about
/// performance. It is about which of the crate's two existing idioms this
/// hot-path trait joins: `#[async_trait]` is used for [`Provider`] and
/// `SearchProvider`, whose impls are only ever behind `Box<dyn Provider>` at a
/// call site that already costs a network round trip. The one that was hand-
/// rolled (`RenderRequester`) was hand-rolled precisely because a `ToolCtx`-clonable
/// `Arc` was the requirement — which is precisely the requirement here. A second
/// spelling of the same pattern in the same crate is a tax on the next reader for
/// no gain, so this trait copies the closer analogue and stays macro-free.
///
/// # The `'static` bound on the returned future, and what it costs
///
/// `dyn Future<…> + Send` is `+ Send + 'static`, so an executor cannot hold the
/// `&ToolCtx` across an `await`. It must take what it needs OUT of the borrow
/// first — which is what [`NativeTool`] does by cloning the `ToolCtx` (whose
/// derived `Clone` SHARES both `Arc` handles, so that is two `Arc` bumps and
/// three small `Vec` clones, against a body that then does file I/O, spawns a
/// process, or awaits a network round trip). `RenderRequester`'s impls pay the
/// same cost for the same reason. This is also WHY the trait returns a bare
/// [`ToolResult`] and not `(ToolResult, bool /* ran */)`: see
/// [`ToolEntry`]'s field docs and `dispatch_tool`'s docblock — an attester would
/// let an implementer mark built-ins `ran: true` and silence the identical-
/// refusal guard on the exact 303-call shape that guard exists for.
pub trait ToolExecutor: Send + Sync {
    /// Run one call. `params` is the call's arguments, already parsed.
    fn execute(
        &self,
        ctx: &ToolCtx,
        params: serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ToolResult> + Send>>;
}

/// Whether a codemode script may reach a tool.
///
/// Replaces `EXCLUDED_FROM_SCRIPT`'s hard-coded 6-name list (the const that lived
/// in `loop/codemode.rs`). A name-level field is the right shape for five of the
/// six reasons — but NOT for the sixth case, which is why `mcp`'s
/// `action: "auth"` refusal STAYS an argument-level special case in
/// `codemode::interactive_args`: the interactivity is in one of `mcp`'s actions,
/// and no name-level field can express "this name, except when…".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptAccess {
    /// A script may call it AND discover it (it appears in `ALL_TOOLS`, gets a
    /// `tools.<name>` global, and answers `describeTool` / `searchTools`).
    Allowed,
    /// A script may neither call it nor discover it.
    ///
    /// ## The payload is DEAD DATA TODAY, and that is deliberate (H11)
    ///
    /// `"interactive"` / `"recursive"` are never read: every consumer matches the
    /// variant and binds `_`, exactly as the const's second tuple element was
    /// `_` before this change. It is kept because it is the ONLY record of WHY a
    /// name is excluded — and once F2 hands host tools over the wire, a host will
    /// be able to exclude a name for a reason the engine has never seen. Do NOT
    /// invent a user-visible sentence from it without a test pinning that
    /// sentence; there is no consumer to keep honest yet.
    Excluded(&'static str),
}

impl ScriptAccess {
    /// Whether a script may reach this tool. The one predicate every consumer
    /// uses, so a fourth consumer cannot get the polarity backwards.
    pub fn is_callable(&self) -> bool {
        matches!(self, ScriptAccess::Allowed)
    }
}

/// One tool: what the model is told it is, what answers it, and whether a script
/// may reach it.
///
/// The three fields are ONE unit on purpose. Splitting a tool's spec from its
/// executor across a boundary is what breaks the crate's oldest dispatch
/// invariant — `dispatch_tool` builds exactly ONE `ToolCtx` via
/// `PermissionContext::to_tool_ctx` so "the executor cannot re-decide
/// differently from its gate" (ADR 0081 / issue #92). Keeping the spec beside the
/// executor is the same rule one level up.
///
/// `Clone` is derived and it SHARES: cloning an entry bumps one `Arc` and clones
/// three `String`s plus a `Value`, so a loop that wants its own copy of a
/// composed registry costs no executor construction. This is the same reason the
/// `Arc` fields exist at all.
#[derive(Clone)]
pub struct ToolEntry {
    /// The advertised spec — the `name`, `description` and `parameters` the model
    /// is shown, byte-identical to what `tool_specs()` produces today.
    pub spec: ToolSpec,
    /// The code that answers. `Arc`, not `Box`, for the reason
    /// `ToolCtx::event_sink` and `ToolCtx::render_requester` are `Arc`: the derived
    /// `Clone` must SHARE the handle, not duplicate it.
    pub executor: std::sync::Arc<dyn ToolExecutor>,
    /// The script reach. Replaces the 6-name `EXCLUDED_FROM_SCRIPT` list; see
    /// [`ScriptAccess`] for why the payload is dead data today.
    pub script: ScriptAccess,
}

impl ToolEntry {
    /// The entry's advertised name (the dispatch key — `spec.name`, spelled out
    /// because nine call sites read it).
    pub fn name(&self) -> &str {
        &self.spec.name
    }
}

/// The executor for a built-in whose substance is an existing `exec_*` function.
///
/// Registering the 12 built-ins through this adapter is **behaviour-neutral by
/// construction**: the `match` in `execute_tool` becomes the code that BUILDS the
/// default registry rather than the code that DISPATCHES, and every arm body is
/// the same `exec_*` call with the same `&ToolCtx` and the same `&Value`.
#[derive(Clone)]
pub struct NativeTool(
    // A plain `fn` pointer, not a boxed `Fn`: every one of the 12 is a
    // non-capturing adapter over a free `async fn`, so the cheaper type is also
    // the honest one, and `Send + Sync` is then automatic.
    NativeRun,
);

/// The adapter signature [`NativeTool`] wraps. Takes the `ToolCtx` by VALUE for
/// the `'static` reason at [`ToolExecutor`]: the future outlives the borrow.
pub type NativeRun = fn(ToolCtx, Value) -> Pin<Box<dyn Future<Output = ToolResult> + Send>>;

impl NativeTool {
    /// Wrap one `exec_*` function. Prefer the [`native!`] macro, which writes the
    /// adapter body once instead of twelve times.
    pub fn new(run: NativeRun) -> Self {
        NativeTool(run)
    }
}

impl ToolExecutor for NativeTool {
    fn execute(
        &self,
        ctx: &ToolCtx,
        params: Value,
    ) -> Pin<Box<dyn Future<Output = ToolResult> + Send>> {
        // Everything the future needs is MOVED in here, because the boxed future
        // is `'static` and the `&ToolCtx` is not. `ToolCtx: Clone` shares both
        // `Arc` handles (`event_sink`, `render_requester`) exactly as the codemode
        // bridge already does when it clones the ctx into its `Bridge`.
        let ctx = ctx.clone();
        let run = self.0;
        Box::pin(async move { run(ctx, params).await })
    }
}

/// The executor a HOST tool gets (ADR 0102 / issue #183 F2).
///
/// # The body: one generic frame, parked
///
/// A WIRED `HostTool` frames ONE call as
/// [`Outbound::HostToolCall`](crate::worker::protocol::Outbound::HostToolCall) and
/// parks on the matching
/// [`Inbound::HostToolResponse`](crate::worker::protocol::Inbound::HostToolResponse).
/// The mechanism — the pending map, the register-before-emit order, the `Drop` guard
/// that empties the map when the parked future is dropped, and the race against the
/// **TURN** token — lives in
/// [`call_host_tool`](crate::worker::host_tools::call_host_tool), next to the map it
/// owns and the tests that mutate it. This type is the handle that carries the
/// channel; ADR 0092's three rules are enforced there and are not conveniences.
///
/// `ctx` is used for exactly one thing — `ctx.cancel`, the turn token the park races —
/// and that is why the trait signature keeps the `ToolCtx` at all. The tool NAME comes
/// from `self`, not from `ctx`: one `HostTool` is built per advertised spec, so it
/// knows its own name and the engine needed no new `ToolCtx` field to tell it.
///
/// # The DETACHED half, and why it is not a placeholder shrug
///
/// A `HostTool` with no channel ([`HostTool::detached`]) REFUSES LOUDLY. That is not a
/// stub awaiting a real body — it is the answer for a loop handed host specs with no
/// Worker channel to put them on, and the loudness is the point: an unhosted tool must
/// answer a `ToolResult::fail`, never a silent park (the daemon's
/// `host-tool-unsupported` refusal in `fulcrum/src/serve.rs` is the same discipline,
/// and a parked answer on a path with no Supervisor is a hang until the turn is
/// cancelled). [`host_entries`] builds detached entries and that refusal is its pinned
/// contract;
/// [`host_entries_attached`](crate::worker::host_tools::host_entries_attached) — the
/// composition a Worker actually runs — replaces only the executor.
#[derive(Clone)]
pub struct HostTool {
    /// The advertised name, i.e. the frame's `tool` field. Held HERE because one
    /// executor exists per spec, and `ToolCtx` has no notion of the call being made
    /// (nor should it — it is the frozen permission bundle, not a dispatch envelope).
    name: String,
    /// The channel this tool speaks on, or `None` for the DETACHED entry
    /// [`host_entries`] builds. `Clone` is derived and it SHARES the pending map and
    /// the outbound sender — the same reason every other handle in this crate is an
    /// `Arc` or a clonable sender.
    channel: Option<crate::worker::host_tools::HostToolChannel>,
}

impl HostTool {
    /// The DETACHED executor: host specs with no Worker channel. Answers the loud
    /// refusal; never parks. This is what [`host_entries`] builds.
    pub fn detached(name: impl Into<String>) -> Self {
        HostTool {
            name: name.into(),
            channel: None,
        }
    }

    /// The WIRED executor: frames every call over `channel`. Built by
    /// [`host_entries_attached`](crate::worker::host_tools::host_entries_attached),
    /// the only place that has a Worker's outbound sender and pending map to hand it.
    pub fn attached(
        name: impl Into<String>,
        channel: crate::worker::host_tools::HostToolChannel,
    ) -> Self {
        HostTool {
            name: name.into(),
            channel: Some(channel),
        }
    }

    /// The advertised name this executor answers (the frame's `tool` field).
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ToolExecutor for HostTool {
    fn execute(
        &self,
        ctx: &ToolCtx,
        params: Value,
    ) -> Pin<Box<dyn Future<Output = ToolResult> + Send>> {
        match self.channel.as_ref() {
            // The park. `ctx` goes in because the turn token it carries is what the
            // park races; the boxed future this returns is `'static`, so the borrow
            // ends here (the token is CLONED out of it — a clone observes the same
            // cancellation).
            Some(channel) => crate::worker::host_tools::call_host_tool(
                channel.clone(),
                self.name.clone(),
                ctx,
                params,
            ),
            None => {
                // The DETACHED refusal. A host tool with no channel is an advertised
                // tool nobody can serve, and the honest answer to that is a `fail` that
                // names itself — never a park on a frame nobody can answer.
                let name = self.name.clone();
                Box::pin(async move {
                    ToolResult::fail(
                        format!(
                            "`{name}` is host-owned and no host is attached to this \
                             session (the tool was advertised through a host registry \
                             that has no Worker channel)"
                        ),
                        None,
                    )
                })
            }
        }
    }
}

/// The engine's built-ins: the **12** tools whose substance the engine owns.
///
/// The 12 native/web executors (`bash`, the six file/search tools, the two skill
/// tools, the three web tools). Their substance IS engine-side: `exec_*` functions
/// in this crate, reached by codemode on a cloned `ToolCtx`, acting on the Space's
/// files under the Boundary the engine confines.
///
/// # The seven board/artifact verbs are NOT here, and never will be again
///
/// They were here through F1 and F2 — 19 entries — because at F1 their substance
/// WAS engine-side (arms of `execute_tool`). **F3 deleted those arms**: the board is
/// the desktop's SQLite, so by ADR 0102 §1's own rule the seven are host tools, and
/// they arrive per session through [`host_entries`] on the specs in
/// `StartEnv::host_tools`. That is the commit that takes `tool_specs()` 26 → 19, and
/// the model's 26 names are unchanged by it because the composition
/// (`builtins() ++ host_entries_attached(env.host_tools, …)` in
/// `worker/core.rs::default_build_loop`) still reaches all 26.
///
/// Do not read "the seven used to be here" as a to-do to put them back. A name
/// whose substance acts on HOST state does not belong in a registry the engine
/// builds for itself — that is the ownership rule this function encodes, and
/// `the_composed_advertised_set_is_exactly_the_twenty_six_names`
/// (`harness/tool_specs.rs`) is the census that keeps the model's surface whole
/// while ownership moves.
///
/// The SPECS are read from [`tool_specs`](crate::harness::tool_specs::tool_specs),
/// not re-stated: ADR 0102's byte-identity rule is that the model sees ONE
/// description per tool, and a second literal for it is a second owner of the
/// advertised surface.
///
/// A name in [`BUILTIN_TOOLS`] that `tool_specs()` does not carry is SKIPPED, not
/// panicked on — a panic here would be inside `AgentLoop::new`. The drift is
/// caught loudly instead, by the two censuses that read this function:
/// `every_advertised_tool_reaches_an_executor` (advertised and unreachable) and
/// `builtins_are_the_twelve_engine_owned_names` (this set's own membership).
pub fn builtins() -> Vec<ToolEntry> {
    let specs = crate::harness::tool_specs::tool_specs();
    BUILTIN_TOOLS
        .iter()
        .filter_map(|(name, run)| {
            let spec = specs.iter().find(|s| s.name == *name)?.clone();
            Some(ToolEntry {
                spec,
                executor: Arc::new(NativeTool(*run)) as Arc<dyn ToolExecutor>,
                // Every built-in is script-callable today, and that is not an
                // accident to be re-derived: the six names in the old
                // `EXCLUDED_FROM_SCRIPT` are ALL loop-internal, so no `execute_tool`
                // arm was ever excluded. `builtins_exclude_nothing_for_scripts`
                // pins the property from the registry side.
                script: ScriptAccess::Allowed,
            })
        })
        .collect()
}

/// The composition seam: the tools whose SUBSTANCE is the host's, arriving as
/// data. `StartEnv::host_tools` (F2) carries these specs over the wire and the
/// ENGINE composes `builtins() ++ host_entries(env.host_tools)` — the desktop
/// never injects Rust objects into a Worker, because a Worker is a second process
/// of the same binary (ADR 0025), so no desktop type crosses the boundary and ADR
/// 0091's "the desktop drives; the crate never pulls" holds verbatim.
///
/// **The SIGNATURE is F1's and F2 did not change it.** F2 gave
/// [`HostTool`] its real body — the `HostToolCall` frame — and the composition the
/// Worker actually runs is
/// [`host_entries_attached`](crate::worker::host_tools::host_entries_attached),
/// which calls THIS function and replaces only each entry's `executor`, so the
/// spec, the order and the `ScriptAccess::Allowed` reach keep one definition here.
/// What `host_entries` itself returns stays the DETACHED entry, whose loud refusal
/// is its pinned contract (see [`HostTool`]).
///
/// `ScriptAccess::Allowed` for every host entry, and that is a preservation rule
/// rather than a default: the seven board verbs WERE script-reachable as engine
/// arms, and ADR 0092 chose `execute_tool` arms for them SPECIFICALLY so a
/// `roadmap` run could turn an approved backlog into N Tasks in one script with a
/// loop. F3 moves the specs out of the crate and the entries carry that property
/// across with them; a `Default` of `Excluded` here would silently delete it with
/// every existing test green. `a_host_tool_is_script_reachable_through_the_composed_registry`
/// is the pin, and its fixture deliberately uses a name NO built-in owns — with a
/// board verb here, `script_access`'s first-wins lookup would find the built-in's
/// `Allowed` and the test would pass on the broken thing.
pub fn host_entries(tools: Vec<ToolSpec>) -> Vec<ToolEntry> {
    tools
        .into_iter()
        .map(|spec| {
            let name = spec.name.clone();
            ToolEntry {
                spec,
                executor: Arc::new(HostTool::detached(name)) as Arc<dyn ToolExecutor>,
                script: ScriptAccess::Allowed,
            }
        })
        .collect()
}

/// The 12 names [`builtins`] registers, in `tool_specs()`' own order (so the
/// registry's order and the advertised order agree, which is what lets the
/// censuses compare them by position as well as by set).
///
/// This is `execute_tool`'s old `match` list MINUS the 7 board/artifact verbs F3
/// deleted, and the pair is pinned by
/// `builtins_are_the_twelve_engine_owned_names`. The seven loop-internal names are
/// NOT here and never will be — see the module doc's bound — and the seven
/// board/artifact verbs are not here either, because their executor is the host's
/// (see [`builtins`]).
pub const BUILTIN_TOOLS: &[(&str, NativeRun)] = &[
    ("bash", native!(crate::tools::exec::bash::exec_bash)),
    ("read", native!(crate::tools::exec::fs_tools::exec_read)),
    ("write", native!(crate::tools::exec::fs_tools::exec_write)),
    ("edit", native!(crate::tools::exec::fs_tools::exec_edit)),
    ("find", native!(crate::tools::exec::search::exec_find)),
    ("grep", native!(crate::tools::exec::search::exec_grep)),
    ("ls", native!(crate::tools::exec::fs_tools::exec_ls)),
    (
        "list_skills",
        native!(crate::tools::exec::skills::exec_list_skills),
    ),
    (
        "read_skill",
        native!(crate::tools::exec::skills::exec_read_skill),
    ),
    ("web_search", native!(crate::web::tools::exec_web_search)),
    (
        "fetch_content",
        native!(crate::web::tools::exec_fetch_content),
    ),
    (
        "get_search_content",
        native!(crate::web::tools::exec_get_search_content),
    ),
];

/// The seven loop-internal tools and their script reach.
///
/// These are NOT registry entries — their executor is `&mut AgentLoop` — so a
/// per-entry [`ScriptAccess`] field cannot carry them, and this table is where
/// their exclusion now lives. It is the survivor of `EXCLUDED_FROM_SCRIPT`, and it
/// lives HERE rather than back in `loop/codemode.rs` so the registry module is the
/// ONE authority on script reach (the whole point of the field). `codemode`'s
/// bridge reads it through [`script_access`], which consults the entries FIRST and
/// this table second, so the field is the primary record and this is the complement.
///
/// `dispatch_subagent` is deliberately absent: it is an ALIAS resolved before the
/// `match` (`dispatch::is_subagent_call`), and an alias inherits `subagent`'s
/// reach — the bridge never sees the alias name as a separate tool.
pub const LOOP_INTERNAL_SCRIPT: &[(&str, ScriptAccess)] = &[
    ("ask", ScriptAccess::Excluded("interactive")),
    ("sudo_exec", ScriptAccess::Excluded("interactive")),
    ("manage_todo_list", ScriptAccess::Excluded("interactive")),
    ("subagent", ScriptAccess::Excluded("recursive")),
    ("list_agents", ScriptAccess::Excluded("recursive")),
    ("codemode", ScriptAccess::Excluded("recursive")),
];

/// The script reach of an advertised name: the entry's own [`ScriptAccess`] if the
/// registry carries one, else the [`LOOP_INTERNAL_SCRIPT`] table, else `Allowed`.
///
/// The `else Allowed` fall-through is the behaviour-preserving half and it is
/// load-bearing for F2: a host spec the engine has never named must be callable
/// from a script (see [`host_entries`]), and defaulting a name it does not know to
/// `Excluded` would silently delete ADR 0092's motivating property.
///
/// The fall-through is ALSO why an unknown name stays callable here rather than
/// refused: reachability of an unknown name is not this function's question — a
/// name with no entry and no arm answers `unknown tool` at dispatch, which is the
/// same answer it answers today.
pub fn script_access(registry: &[ToolEntry], name: &str) -> ScriptAccess {
    if let Some(entry) = registry.iter().find(|entry| entry.name() == name) {
        return entry.script.clone();
    }
    LOOP_INTERNAL_SCRIPT
        .iter()
        .find(|(excluded, _)| *excluded == name)
        .map(|(_, access)| access.clone())
        .unwrap_or(ScriptAccess::Allowed)
}

/// The default registry: the engine's built-ins, and nothing else.
///
/// `execute_tool` resolves through this, because `execute_tool` is a free
/// `pub` function with no loop to read a registry from — and it must stay that
/// way, both because 105 of this crate's own test call sites name it and because
/// the sibling desktop repo's two integration tests import it through
/// `archimedes_lib::agent::tools::execute_tool`, a breakage NO test in this
/// workspace can see (it consumes a crates.io rev).
///
/// A `LazyLock` rather than a per-call `builtins()`: an entry is a spec CLONE
/// (three `String`s) plus an `Arc`, and `execute_tool` sits on the dispatch path.
/// `provider_auth/gcloud.rs`, `web/net.rs`, `web/cache.rs` and
/// `worker/manager/mod.rs` all use the same static-initialisation shape.
///
/// A loop does NOT read this: it carries its own registry as an `AgentLoop` field
/// built in `AgentLoop::new` (F2 replaces that content with
/// `builtins() ++ host_entries(env.host_tools)`), because building it lazily behind
/// a shared borrow is not expressible — `exec_codemode` and `Bridge` hold `&AgentLoop`.
static DEFAULT_REGISTRY: std::sync::LazyLock<Vec<ToolEntry>> = std::sync::LazyLock::new(builtins);

/// The default registry (the slice `execute_tool` dispatches through).
pub fn default_registry() -> &'static [ToolEntry] {
    &DEFAULT_REGISTRY
}

/// Look an advertised name up in a registry.
///
/// Returns `None` for the loop-internal names and for `dispatch_subagent` — the
/// caller then falls through to whatever it does with an unreachable name, which
/// for both dispatch paths is today's `unknown tool: {name}` sentence. The alias
/// needs no special handling HERE because `dispatch_tool` resolves it BEFORE the
/// `match` (`dispatch::is_subagent_call`) and the codemode bridge never receives
/// it (the bridge's tool set is the advertised set, and the alias is not advertised).
pub fn lookup<'a>(registry: &'a [ToolEntry], name: &str) -> Option<&'a ToolEntry> {
    registry.iter().find(|entry| entry.name() == name)
}

//! The Worker's host-tool requester (ADR 0102 / issue #183 F2): frames ONE tool
//! call over the protocol as [`Outbound::HostToolCall`] and parks until the
//! Supervisor answers with [`Inbound::HostToolResponse`].
//!
//! This carries **all four** of the mechanisms the deleted
//! `worker/task_store.rs` carried — not as a stylistic echo, but because every one
//! of them exists to close a hang or a leak this seam would otherwise reopen. ADR
//! 0092 mandated the first three for the task frame pair and ADR 0102 §2.5 restates
//! them for this one verbatim. F3 deleted that file, so this module is now the only
//! requester of the shape in the crate, and these four rules are the ONLY record of
//! it: a fifth rule nobody has to pay for is not the loss, a rule quietly dropped is.
//!
//! 1. **Register the oneshot BEFORE emitting the frame.** An `HostToolResponse` that
//!    arrives between "emit" and "insert" finds no entry, is (correctly) ignored, and
//!    the caller parks on a oneshot nobody can ever resolve. Emit-first is the order
//!    [`IpcDispatcher::dispatch`](crate::worker::dispatch::IpcDispatcher) uses and it
//!    survives there only because that flow has a second path to resolution; here it
//!    is a hang. Pinned by `register_happens_before_the_frame_is_emitted`.
//! 2. **Dropping the parked future removes the pending entry.** The park races the
//!    turn token, so a Stop drops this future mid-park — and a map entry whose
//!    `Sender` is gone stays for the life of the Worker: one leaked `oneshot::Sender`
//!    per cancelled host call per session. [`HostToolCleanup`] is the guard; pinned
//!    by `dropping_the_parked_call_removes_the_pending_entry` and
//!    `a_cancelled_turn_leaves_the_pending_map_empty`.
//! 3. **Race the TURN token** (`ctx.cancel`), not the session-teardown token. A
//!    Supervisor that never answers — an old peer, or the daemon, whose `on_event`
//!    carries no inbound writer — must cost a cancelled tool, not a hung turn. Pinned
//!    by `the_cancel_arm_returns_promptly_when_the_turn_token_fires`.
//! 4. **The pending map is keyed by the BARE `request_id`.** See
//!    [`PendingHostToolCalls`] for why the compound `"{session_id}/{request_id}"`
//!    shape that `permission_key` and `render_key` use would miss every answer here.
//!
//! # Where the TURN token is voted, and why that is this file's problem
//!
//! For the seven task verbs the token vote used to live in the executor ARM
//! (`task_tools::parked`), one level ABOVE the store. A host tool has no arm —
//! [`HostTool`](crate::tools::exec::HostTool) IS the whole executor — so the vote
//! lives here, in [`call_host_tool`], reading [`ToolCtx::cancel`]. That is the field
//! `PermissionContext::to_tool_ctx` fills with the per-turn token, and it is exactly
//! why [`ToolExecutor::execute`](crate::tools::exec::ToolExecutor) still takes a
//! `ctx`: an executor with no token cannot be stopped. The session token (the one
//! `Close` fires) would NOT do — a Stop does not set it, so a park that voted it
//! hangs on precisely the input a Stop is. The hang pin therefore holds TWO distinct
//! tokens and cancels only the turn one.
//!
//! # The ORIGIN address, and the hang it exists to prevent
//!
//! A Subagent's frames are delivered under the PARENT's session id: the manager's
//! pump re-keys them so the child's UI renders in the parent's scope. ADR 0117's rule
//! is that that id is a UI-scope key and **never a reply address**, so the frame
//! carries its own origin — the id of the Worker that emitted it, taken from
//! `StartEnv::session_id` at the construction site in `core.rs`. This is not
//! theoretical: the recorded 255m55s `get_artifact` stall was a reply sent at the
//! delivery id, which for a subagent is the PARENT's, so the child's oneshot was
//! never resolved. The origin goes on the FRAME only; the map stays keyed by the bare
//! id (rule 4).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};

use crate::provider_types::ToolSpec;
use crate::tools::exec::{host_entries, HostTool, ToolEntry, ToolExecutor};
use crate::tools::types::{ContentBlock, ToolCtx, ToolResult};
use crate::worker::protocol::Outbound;

/// The Worker's parked host-tool calls: `request_id` → the oneshot to resolve.
///
/// Keyed by the BARE `request_id`, exactly like the deleted
/// `task_store::PendingTaskRequests` was, and for the
/// same three reasons: the map is per-Worker (one session per Worker), the id is a
/// fresh uuid v4 this file mints so the inserter and the handler agree on its spelling
/// without either one needing a session id, and the `Inbound::HostToolResponse` carries
/// ONLY that bare id — so a compound `"{session_id}/{request_id}"` key (the
/// `permission_key` / `render_key` shape) could not be built by the answering side and
/// would miss every call. A missed answer is a hang, not an error.
///
/// Pinned from both sides: `the_frame_names_the_worker_that_emitted_it_and_the_map_
/// stays_bare_keyed` (the insert side, with a non-`None` origin) and `core.rs`'s
/// `a_host_tool_response_resolves_by_the_bare_request_id` (the remove side, with
/// `WorkerCore::session_id` still `None` because no `Start` was handled).
///
/// It used to be one of TWO maps, kept separate from `pending_task_requests` because
/// the value types differed (`Result<ToolResult, String>` vs
/// `Result<Value, TaskStoreError>`). F3 deleted the task half, so this is the only
/// pending-tool map on `WorkerCore` — and the `host-` id prefix stays, because a
/// Worker log still has to say which family an id belongs to, and the prefix is what
/// `mint_request_id` exists to provide.
pub type PendingHostToolCalls =
    Arc<TokioMutex<HashMap<String, oneshot::Sender<Result<ToolResult, String>>>>>;

/// What a [`HostTool`] needs to actually speak: the origin to stamp on the frame, the
/// Worker's outbound channel, and the map its answer lands in.
///
/// A struct rather than three constructor arguments for the same reason
/// `SudoDeps` exists: the handle is built once per Worker in `default_build_loop` and
/// cloned onto every host entry, and a named bundle keeps the three from being
/// reordered at the one call site that has all of them. `Clone` is cheap — a `String`
/// bump, a sender clone and an `Arc` bump.
#[derive(Clone)]
pub struct HostToolChannel {
    /// (ADR 0117) The ORIGIN address stamped on every frame: the id of THIS Worker —
    /// the CHILD for a Subagent — so the Supervisor can address its answer back here
    /// instead of to the pump's parent-scoped delivery id. Frame freight ONLY: it is
    /// deliberately not part of the pending map's key ([`PendingHostToolCalls`]).
    ///
    /// `Option` for the same additive reason as the frame's field: `core.rs` passes
    /// `Some(env.session_id)` (the non-optional envelope value), and a channel built
    /// without one is an honest absence rather than an empty string.
    pub session_id: Option<String>,
    /// The Worker's outbound frame channel (the same sender every other `Ipc*` handle
    /// in `core.rs` holds).
    pub outbound: mpsc::UnboundedSender<Outbound>,
    /// The map the `Inbound::HostToolResponse` handler resolves. MUST be the SAME map
    /// the WorkerCore holds — a map built here would mean answers nobody receives,
    /// which is a hang and not an error.
    pub pending: PendingHostToolCalls,
}

impl HostToolChannel {
    pub fn new(
        session_id: Option<String>,
        outbound: mpsc::UnboundedSender<Outbound>,
        pending: PendingHostToolCalls,
    ) -> Self {
        Self {
            session_id,
            outbound,
            pending,
        }
    }
}

/// Mint a request id. The `host-` prefix is what makes one recognisable in a Worker
/// log and — since F3 deleted the `task-` family it was one of two — keeps a stale or
/// hand-inserted answer from resolving an entry of a family this build no longer has.
fn mint_request_id() -> String {
    format!("host-{}", uuid::Uuid::new_v4())
}

/// The host half of the composed registry: the SAME [`host_entries`] entries, with
/// each [`HostTool`]'s executor replaced by the WIRED one, so a call to the name
/// frames an `Outbound::HostToolCall` and parks.
///
/// # Why a sibling of `host_entries` and not a change to it
///
/// `host_entries(Vec<ToolSpec>) -> Vec<ToolEntry>` is F1's signature and the registry
/// module's documented composition seam; F2 is forbidden to change it. It also carries
/// a contract of its own, pinned by `tools/exec/tests/registry.rs::
/// host_entries_carry_their_specs_allowed_and_refuse_loudly` and by
/// `harness/loop/tests/codemode.rs::a_nested_call_reaches_a_host_entry`: an entry it
/// builds REFUSES LOUDLY, because it has no channel to speak on. So this function
/// CALLS `host_entries` and replaces only the `executor` field — the spec, the order
/// and the `ScriptAccess::Allowed` reach all keep ONE definition, in `host_entries`,
/// and a drift in any of them reddens F1's own pins as well as this module's census.
///
/// The engine composes `builtins() ++ host_entries_attached(env.host_tools, …)` in
/// `worker/core.rs::default_build_loop`. The desktop never injects an executor, only
/// the specs — a Worker is a second process of the same binary (ADR 0025), so it could
/// not receive one.
pub fn host_entries_attached(tools: Vec<ToolSpec>, channel: HostToolChannel) -> Vec<ToolEntry> {
    host_entries(tools)
        .into_iter()
        .map(|mut entry| {
            // The name rides the executor: one entry, one spec, one frame `tool`
            // value. `ToolCtx` has no notion of the call being made (it is the frozen
            // permission bundle the gate and the executor share), so this is the only
            // place a per-spec executor can learn its own name.
            let name = entry.name().to_string();
            entry.executor =
                Arc::new(HostTool::attached(name, channel.clone())) as Arc<dyn ToolExecutor>;
            entry
        })
        .collect()
}

/// Frame one host-tool call and park until the Supervisor answers.
///
/// THE body behind a wired [`HostTool`]. It lives here rather than inline in
/// `registry.rs` because this is where the pending map's owner lives, and the four
/// mechanisms (module doc) are all about that map. Every rule is pinned by a test in
/// this module that reddens when the rule is removed.
///
/// `ctx` is used for exactly one thing — `ctx.cancel`, the TURN token this park races —
/// and that single use is why the trait signature carries a `ToolCtx` at all.
pub fn call_host_tool(
    channel: HostToolChannel,
    tool: String,
    ctx: &ToolCtx,
    params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = ToolResult> + Send>> {
    // Everything the boxed future needs is taken OUT of the borrow before it exists:
    // `dyn Future + Send` is `+ 'static`, so it cannot hold the `&ToolCtx`. The token
    // is a CLONE of the turn token (a `CancellationToken` clone observes the same
    // cancellation), which is the whole reason the borrow can end here.
    let turn = ctx.cancel.clone();
    Box::pin(async move {
        let request_id = mint_request_id();
        let (tx, rx) = oneshot::channel();
        // (ADR 0092 rule 1) THE ORDER: register, THEN emit. A `HostToolResponse` that
        // races the emission must always find the entry — see the module doc for why
        // the reversed order (which `IpcDispatcher::dispatch` gets away with) hangs
        // here.
        //
        // The guard is constructed BEFORE the insert on purpose: if the future is
        // dropped between the lock being acquired and the insert landing, the key is
        // already tracked, so the guard removes either an entry or nothing (both
        // correct). Constructing it after the insert leaves a window whose drop leaks.
        let _cleanup = HostToolCleanup::new(&channel.pending, request_id.clone());
        channel.pending.lock().await.insert(request_id.clone(), tx);

        if channel
            .outbound
            .send(Outbound::HostToolCall {
                request_id: request_id.clone(),
                // (ADR 0117) The ORIGIN address. Frame freight only — the insert above
                // keyed the map by the bare `request_id`, and the handler resolves by
                // that bare id alone.
                session_id: channel.session_id.clone(),
                tool,
                params,
            })
            .is_err()
        {
            // The pipe is gone: nothing can EVER answer this call, so say so now
            // instead of parking on a oneshot whose only remaining writer is about to
            // be dropped. The guard removes the entry on the way out.
            return ToolResult::fail(
                "host tool: the worker outbound channel is closed, so no Supervisor \
                 can answer this call"
                    .to_string(),
                None,
            );
        }

        // (ADR 0092 rule 3) Park, racing the TURN token. This is the arm that keeps a
        // Stop from hanging: the daemon answers nothing, and an old Supervisor routes
        // the frame to `Unknown` and ignores it, so in both cases the ONLY thing that
        // ever settles this park is the turn being cancelled.
        let answered = tokio::select! {
            biased;
            // The answer wins when it arrives, which is the normal path.
            rx = rx => rx,
            // The TURN token — NOT the session-teardown token. `dispatch_tool`'s
            // loop-internal arms race this same token, and `task_tools::parked` used
            // to vote it one level up for the seven task verbs; here there is no arm
            // and, since F3, no other family, so the vote is this line alone.
            _ = turn.cancelled() => return cancelled_tool_result(),
        };

        match answered {
            // The Supervisor answered. An `Err` half is prose by design (the frame
            // carries no machine-readable kind, because the crate has no idea what a
            // host tool is) — it becomes a FAILED tool result the model can read.
            Ok(Ok(result)) => result,
            Ok(Err(message)) => ToolResult::fail(message, None),
            // The SENDER was dropped without answering: the Worker is shutting down
            // mid-call. The handler always answers or removes, so this is teardown.
            Err(_) => ToolResult::fail(
                "host tool: the answer channel was dropped before an answer arrived \
                 (the Worker is shutting down)"
                    .to_string(),
                None,
            ),
        }
        // `_cleanup` drops HERE on the settled paths — the handler already removed the
        // entry when it resolved the oneshot, so the guard's removal is a no-op
        // (idempotent). On a cancelled path it drops when the caller drops this future,
        // and that removal is the one that matters.
    })
}

/// The cancelled tool result, in the harness's OWN sentence.
///
/// A local copy of `harness::loop::tools::cancelled_tool_result` rather than a reach
/// into it: that function is `pub(super)` inside `harness::loop`, so nothing outside
/// that module can name it, and widening it is a change to a module F3 is about to
/// reorganise. The TEXT is the contract — every cancelled tool in the crate reports
/// exactly `"cancelled"`, and the hang pin asserts it byte-for-byte so a drift here is
/// a red test and not a silently different sentence the model sees.
fn cancelled_tool_result() -> ToolResult {
    ToolResult {
        content: vec![ContentBlock::Text {
            text: "cancelled".to_string(),
        }],
        details: None,
        is_error: true,
    }
}

/// The Drop guard for one parked host-tool call (rule 2).
///
/// A copy of the deleted `task_store::TaskRequestCleanup`
/// with the map alias swapped, and it keeps that guard's one deliberate departure from
/// `SudoPromptCleanup`: the removal tries a synchronous `try_lock` FIRST and only
/// spawns when the lock is contended. The reason is what makes the leak test
/// trustworthy — a spawned removal is a task the test has to wait for, so "assert the
/// map is empty" would depend on the runtime having polled it, which is exactly the
/// kind of assertion that passes under load and flakes later. With the synchronous
/// fast path the entry is gone the instant the future is dropped.
///
/// Idempotent by construction: the `Inbound::HostToolResponse` handler removes the
/// entry when it resolves the oneshot, so on the success path this guard's later
/// removal finds nothing to do.
pub struct HostToolCleanup {
    pending: PendingHostToolCalls,
    /// `None` once taken (a `Drop` runs at most once, but this keeps the field
    /// obviously single-use and makes a double-drop trivially safe).
    key: Option<String>,
}

impl HostToolCleanup {
    pub fn new(pending: &PendingHostToolCalls, key: String) -> Self {
        Self {
            pending: pending.clone(),
            key: Some(key),
        }
    }
}

impl Drop for HostToolCleanup {
    fn drop(&mut self) {
        let Some(key) = std::mem::take(&mut self.key) else {
            return;
        };
        // Fast path: the lock is free (the normal case — the caller is parked on the
        // oneshot, not holding the map) → remove NOW, no scheduling involved.
        if let Ok(mut map) = self.pending.try_lock() {
            map.remove(&key);
            return;
        }
        // Contended: hand it to the runtime. A dropped runtime cannot run it, and the
        // entry then lives as long the Worker does — bounded, and strictly rarer than
        // the fast path.
        let pending = self.pending.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                pending.lock().await.remove(&key);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::protocol::{
        decode_inbound, decode_outbound, encode_inbound, encode_outbound, Inbound, Outbound,
    };
    use serde_json::json;
    use std::path::PathBuf;
    use std::task::{Context, Poll};
    use tokio_util::sync::CancellationToken;

    /// The host verb under test. A name NO built-in has, so nothing else in the crate
    /// can answer it and every assertion below is about this file.
    fn mark_ready() -> ToolSpec {
        ToolSpec {
            name: "mark_ready".into(),
            description: "Mark the session ready (a host verb).".into(),
            parameters: json!({"type": "object", "properties": {}}),
        }
    }

    /// The composition under test: ONE host entry, attached to this channel.
    fn attached(
        session_id: Option<String>,
        outbound: mpsc::UnboundedSender<Outbound>,
        pending: PendingHostToolCalls,
    ) -> ToolEntry {
        let mut entries = host_entries_attached(
            vec![mark_ready()],
            HostToolChannel::new(session_id, outbound, pending),
        );
        assert_eq!(entries.len(), 1, "one spec in, one entry out");
        entries.remove(0)
    }

    /// A `ToolCtx` whose `cancel` is the token the test controls — the TURN token,
    /// exactly as `PermissionContext::to_tool_ctx` wires it.
    fn ctx_with_cancel(cancel: &CancellationToken) -> ToolCtx {
        let cwd = PathBuf::from("/tmp/host-tools-test");
        ToolCtx {
            cwd: cwd.clone(),
            cancel: cancel.clone(),
            skill_roots: None,
            boundary: vec![cwd],
            file_policy: crate::policy::FilePolicy::default(),
            protected: Vec::new(),
            rules: Vec::new(),
            home: None,
            web: Default::default(),
            render_requester: None,
            event_sink: None,
        }
    }

    /// The first text block's text (the shape every assertion here reads).
    fn text_of(r: &ToolResult) -> String {
        r.content
            .first()
            .map(|block| match block {
                ContentBlock::Text { text } => text.clone(),
                other => format!("<{other:?}>"),
            })
            .unwrap_or_default()
    }

    /// The host's answer: a `ToolResult` in its own serde shape, carrying a `details`
    /// payload so the round-trip proves the UI half rides too.
    fn host_answer() -> ToolResult {
        ToolResult::ok_text(
            "marked ready".to_string(),
            Some(json!({ "board": { "spacePath": "/s" } })),
        )
    }

    /// Poll a future EXACTLY once with a no-op waker (`Ready` = true).
    ///
    /// The reason the order and leak tests below can say anything at all about a
    /// future they never `await`: `execute()` returns a future whose async body has NOT
    /// run yet, so dropping it without polling proves nothing — no entry was ever
    /// inserted and "the map is empty" is vacuously true with the Drop guard DELETED.
    /// One poll drives the body to its park.
    fn poll_once(fut: &mut Pin<Box<dyn Future<Output = ToolResult> + Send>>) -> bool {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        matches!(fut.as_mut().poll(&mut cx), Poll::Ready(_))
    }

    /// THE ORDER PIN (rule 1): the oneshot is registered BEFORE the frame is emitted.
    ///
    /// Why the obvious version is worthless: drive the future, catch the frame, assert
    /// the map holds the entry — that PASSES ON THE WRONG ORDER, because an
    /// uncontended `tokio::Mutex::lock()` does not yield, so on a single-threaded
    /// runtime "insert" and "send" both land inside ONE poll however the source orders
    /// them. So the order is FORCED: hold the pending-map lock, run the call. A correct
    /// implementation blocks at its `lock().await` and emits NOTHING; an emit-first one
    /// puts the frame on the wire while the lock is still held, which is exactly what
    /// the assertion rejects. Deterministic in both directions: no timing window, no
    /// load dependence.
    #[tokio::test]
    async fn register_happens_before_the_frame_is_emitted() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);

        // Hold the map lock for the whole first phase. The call must reach its
        // registration and STOP there.
        let guard = pending.lock().await;
        let caller = tokio::spawn(async move {
            let ctx = ctx_with_cancel(&CancellationToken::new());
            let entry = entry;
            let fut = entry.executor.execute(&ctx, json!({}));
            fut.await
        });
        // Long enough that the task has certainly reached its first await point.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            out_rx.try_recv().is_err(),
            "a `host-tool-call` frame reached the wire while the pending map was still \
             locked — the implementation emits BEFORE it registers the oneshot, so a \
             Supervisor that answers immediately finds no entry and the caller parks \
             forever (register first, like this function does, not like \
             `IpcDispatcher::dispatch`)"
        );
        assert!(!caller.is_finished(), "the call is parked, not resolved");

        // Release: the registration completes and only NOW may the frame appear.
        drop(guard);
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), out_rx.recv())
            .await
            .expect("the frame is emitted once the registration can proceed")
            .expect("the outbound channel is open");
        let Outbound::HostToolCall {
            request_id,
            session_id,
            tool,
            params,
        } = frame
        else {
            panic!("expected a HostToolCall frame, got {frame:?}");
        };
        assert_eq!(tool, "mark_ready", "the frame names the tool");
        assert_eq!(params, json!({}), "and carries the model's params verbatim");
        assert_eq!(
            session_id, None,
            "this channel was built with no origin id — the field is an honest `None`, \
             not an empty string"
        );
        assert!(
            request_id.starts_with("host-"),
            "ids are recognisable in a Worker log, and `host-` keeps them distinct from \
             the `task-` family: {request_id}"
        );
        // THE assertion: the entry exists at the first instant the frame does.
        assert!(
            pending.lock().await.contains_key(&request_id),
            "the frame `{request_id}` is on the wire but the pending map does not hold \
             its oneshot"
        );

        // Answer it on the SAME parked call, so the test also proves the entry it
        // checked is the one the answer resolves.
        let tx = pending
            .lock()
            .await
            .remove(&request_id)
            .expect("the entry the frame raced is present");
        tx.send(Ok(host_answer()))
            .expect("the caller is still parked");
        let got = caller
            .await
            .expect("the call task does not panic and the answer resolved");
        assert_eq!(got, host_answer(), "the ToolResult rides back verbatim");
        assert!(
            pending.lock().await.is_empty(),
            "the handler's removal is the normal path; the guard's later removal is a \
             no-op (idempotent), so a resolved call leaves nothing behind"
        );
        let _ = &ctx;
    }

    /// THE DROP PIN (rule 2), written against its own false positive.
    ///
    /// The trap: an `execute()` future that is never polled has never run its body, so
    /// it never inserted an entry, and "the map is empty after the drop" passes with
    /// the Drop guard DELETED. So this test polls once and — before dropping — asserts
    /// the map holds exactly one entry AND a frame reached the wire. Only then does it
    /// drop, in the SAME runtime, with the map lock FREE, and assert emptiness.
    ///
    /// Mutation note: delete the `Drop` impl (or make it remove nothing) and the
    /// emptiness assertion reddens; move the guard's construction AFTER the insert and
    /// the pre-drop witness still passes while the emptiness one reddens.
    #[tokio::test]
    async fn dropping_the_parked_call_removes_the_pending_entry() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);

        let mut fut = entry.executor.execute(&ctx, json!({}));
        let ready = poll_once(&mut fut);
        assert!(
            !ready,
            "the call must PARK (no answer has been sent), so the drop below really is \
             a drop-mid-park"
        );
        assert!(
            out_rx.try_recv().is_ok(),
            "the poll must have emitted the frame — otherwise the body never ran and \
             the emptiness assertion below proves nothing"
        );
        assert_eq!(
            pending.lock().await.len(),
            1,
            "WITNESS: the poll inserted the entry (without this the test would be \
             vacuous — an empty map after a drop that never inserted is not a cleanup)"
        );

        // Drop the parked future (what the turn-cancel arm does to it), lock NOT held.
        // Then the map must be empty IMMEDIATELY — no spawned task to wait for, which
        // is why the guard prefers `try_lock` over `SudoPromptCleanup`'s always-spawn.
        drop(fut);
        assert!(
            pending.lock().await.is_empty(),
            "dropping the parked future must remove the pending entry — a surviving \
             entry is a `oneshot::Sender` leaked per cancelled host call"
        );
    }

    /// A cancelled turn: the cancelled arm wins AND the map ends up EMPTY. The
    /// two-armed `select!` shape a caller uses, so the leak is pinned at the requester.
    #[tokio::test]
    async fn a_cancelled_turn_leaves_the_pending_map_empty() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();

        // Fire the token a moment after the call is in flight (the shape of a real
        // Stop). Without this the park would wait on the answer forever.
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                cancel.cancel();
            });
        }

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let ctx = ctx_with_cancel(&cancel);
            let fut = entry.executor.execute(&ctx, json!({}));
            fut.await
        })
        .await
        .expect("the parked call returns once the TURN token fires (no hang)");
        assert_eq!(
            text_of(&outcome),
            "cancelled",
            "the turn token's arm answered INSIDE the executor (the caller had no \
             select of its own in this fixture)"
        );
        assert!(
            out_rx.try_recv().is_ok(),
            "the frame was emitted before the cancel (the call really was in flight)"
        );
        assert!(
            pending.lock().await.is_empty(),
            "a cancelled turn must leave no pending entry behind — this is what keeps \
             a Stop from leaking one sender per cancelled host call"
        );
    }

    /// THE HANG PIN, and the one that names WHICH token. The empty-map tests prove no
    /// LEAK; this one proves no HANG — the failure mode a reviewer's eye slides past.
    ///
    /// No `HostToolResponse` is EVER delivered: that is the daemon (no inbound writer
    /// on its `on_event`) and the old-Supervisor window (its decoder routes the frame
    /// to `Unknown` and ignores it). The TURN token fires; the park must return
    /// PROMPTLY.
    ///
    /// Mutation note: vote the SESSION token instead of `ctx.cancel` (or drop the race
    /// entirely) and this test times out — the honest hang, caught. The session token
    /// is a SEPARATE, never-cancelled token below, which is the whole point: a fixture
    /// handing the executor one token for both could not tell the two apart, and a real
    /// Stop sets only the turn one.
    #[tokio::test]
    async fn the_cancel_arm_returns_promptly_when_the_turn_token_fires() {
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        // THE TURN token — what `to_tool_ctx` hands the executor and what a Stop fires.
        let turn = CancellationToken::new();
        // The SESSION teardown token, deliberately alive for the whole test. A park
        // voting THIS instead would still be parked when this test's deadline fires.
        let _session_teardown = CancellationToken::new();

        let fired = {
            let turn = turn.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                turn.cancel();
            })
        };

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let ctx = ctx_with_cancel(&turn);
            let fut = entry.executor.execute(&ctx, json!({}));
            fut.await
        })
        .await
        .expect(
            "the parked host call MUST be settled by the TURN token: the daemon never \
             answers a `host-tool-call`, so an implementation voting the session token \
             (or no token) hangs every host tool on a `fulcrum serve` session until \
             something else cancels the turn — and never returns at all if nothing does",
        );
        assert!(
            outcome.is_error,
            "the cancelled arm is an error result the model can see, got {outcome:?}"
        );
        assert_eq!(
            text_of(&outcome),
            "cancelled",
            "and it is the harness's own cancelled sentence, the one every other \
             cancelled tool reports"
        );
        fired.await.expect("the canceller task ran");
        assert!(
            pending.lock().await.is_empty(),
            "and the park left nothing behind"
        );
    }

    /// Two calls mint two different ids, both `host-`-prefixed. A collision would make
    /// one answer resolve the wrong caller — the wrong tool's result, not an error — and
    /// a `task-`-prefixed id here would put the two families in one namespace.
    #[tokio::test]
    async fn request_ids_are_unique_and_host_prefixed() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);

        let mut a = entry.executor.execute(&ctx, json!({ "a": 1 }));
        let mut b = entry.executor.execute(&ctx, json!({ "b": 2 }));
        poll_once(&mut a);
        poll_once(&mut b);

        let mut ids = Vec::new();
        while let Ok(frame) = out_rx.try_recv() {
            let Outbound::HostToolCall { request_id, .. } = frame else {
                panic!("expected HostToolCall, got {frame:?}");
            };
            ids.push(request_id);
        }
        assert_eq!(ids.len(), 2, "one frame per call");
        for id in &ids {
            assert!(id.starts_with("host-"), "got `{id}`");
            assert!(
                id.len() > "host-".len() + 30,
                "a uuid v4 follows the prefix: {id}"
            );
        }
        assert_ne!(ids[0], ids[1], "two calls, two ids");
        assert_eq!(
            pending
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .len(),
            2,
            "both registered"
        );
    }

    /// A closed outbound channel fails the call IMMEDIATELY instead of parking on a
    /// oneshot nobody will answer, and leaves no entry behind.
    #[tokio::test]
    async fn a_closed_outbound_channel_fails_instead_of_parking() {
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        drop(out_rx);
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);

        let got = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            entry.executor.execute(&ctx, json!({})),
        )
        .await
        .expect("a dead pipe is reported at once, never parked on");
        assert!(got.is_error, "the call fails: {got:?}");
        let text = text_of(&got);
        assert!(
            text.contains("outbound") && text.contains("closed"),
            "and says WHY, in terms a user can act on: {text:?}"
        );
        assert!(
            pending.lock().await.is_empty(),
            "the failed send must not leave an entry behind"
        );
    }

    /// The wire spelling of the frame this file emits, checked through the REAL decoder
    /// (not the Rust enum): this requester and a Supervisor's decoder must agree on
    /// `host-tool-call` / `requestId` / `sessionId` / `tool` / `params`.
    #[tokio::test]
    async fn the_frame_this_file_emits_decodes_as_a_host_tool_call_line() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);
        let mut fut = entry.executor.execute(&ctx, json!({ "spacePath": "/s" }));
        poll_once(&mut fut);
        let frame = out_rx.try_recv().expect("the frame was emitted");
        let line = encode_outbound(&frame);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(line.trim_end()).expect("one JSON line")
                ["type"],
            "host-tool-call",
            "the wire `type` is the allow-listed name, got {line}"
        );
        match decode_outbound(&line).expect("it round-trips") {
            Outbound::HostToolCall { tool, params, .. } => {
                assert_eq!(tool, "mark_ready");
                assert_eq!(
                    params["spacePath"], "/s",
                    "the model's params ride verbatim — no re-keying on the way out"
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    /// THE ORIGIN-ADDRESS PIN (rule 4, both halves): the frame names the Worker that
    /// emitted it, AND the pending map stays keyed by the BARE `request_id`.
    ///
    /// Why: the manager's pump re-keys a Subagent's frames to the PARENT's session id
    /// (`worker/manager/pump.rs`) so the child's UI renders in the parent's scope, and a
    /// Supervisor that replies to THAT id answers the wrong process — the recorded
    /// 255m55s `get_artifact` stall. ADR 0117's rule: **a frame's delivery address is a
    /// UI-scope key and never a reply address.**
    ///
    /// The second half is the half a reviewer is most likely to "fix" into a bug: the
    /// origin travels on the FRAME, and the map stays keyed by the bare id.
    #[tokio::test]
    async fn the_frame_names_the_emitting_worker_and_the_map_stays_bare_keyed() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(Some("child-9".into()), out_tx, pending.clone());

        // Hold the map lock, run the call, release — the registration must complete
        // before the frame may exist (rule 1 again, from the origin side).
        let guard = pending.lock().await;
        let caller = tokio::spawn(async move {
            let ctx = ctx_with_cancel(&CancellationToken::new());
            let fut = entry.executor.execute(&ctx, json!({}));
            fut.await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            out_rx.try_recv().is_err(),
            "the frame reached the wire before the registration (see \
             `register_happens_before_the_frame_is_emitted`)"
        );
        drop(guard);

        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), out_rx.recv())
            .await
            .expect("the frame is emitted once the registration can proceed")
            .expect("the outbound channel is open");
        let Outbound::HostToolCall {
            request_id,
            session_id,
            ..
        } = frame
        else {
            panic!("expected a HostToolCall frame, got {frame:?}");
        };
        assert_eq!(
            session_id.as_deref(),
            Some("child-9"),
            "the frame names the Worker that EMITTED it (the CHILD for a Subagent) — a \
             Supervisor that answers the pump's delivery id answers the parent's process \
             and the caller parks forever"
        );
        // THE NO-COMPOUND-KEY PIN.
        assert!(
            pending.lock().await.contains_key(&request_id),
            "with a NON-`None` origin the entry is still registered under the BARE \
             `{request_id}` — the `Inbound::HostToolResponse` carries only that id, so a \
             `session_id`-compound key would miss every answer (a hang, not an error)"
        );

        // Answer it through the bare key, proving the entry the test checked is the one
        // the answer resolves.
        let tx = pending
            .lock()
            .await
            .remove(&request_id)
            .expect("the entry is keyed by the bare request id");
        tx.send(Ok(host_answer()))
            .expect("the caller is still parked");
        let got = caller
            .await
            .expect("the call task does not panic and resolves through the bare key");
        assert_eq!(got, host_answer(), "the answer rides back verbatim");
    }

    /// THE ROUND TRIP, both directions, through the real decoders — including the `Err`
    /// half, which is prose by design (grounding H8): the model sees the sentence, not a
    /// machine-readable kind, because the crate has no idea what a host tool is.
    #[test]
    fn a_host_answer_round_trips_in_both_arms() {
        let ok = Inbound::HostToolResponse {
            request_id: "host-1".into(),
            result: Ok(host_answer()),
        };
        let line = encode_inbound(&ok);
        assert!(
            line.contains(r#""isError":false"#),
            "`ToolResult`'s OWN camelCase spelling rides the frame — the same shape the \
             frontend's tool-details registry pins — got {line}"
        );
        assert!(
            line.contains(r#""type":"host-tool-response""#),
            "the wire `type` is the kebab-case allow-listed name: {line}"
        );
        match decode_inbound(&line).expect("the Ok arm decodes") {
            Inbound::HostToolResponse { request_id, result } => {
                assert_eq!(request_id, "host-1");
                assert_eq!(
                    result.expect("the Ok arm stays Ok"),
                    host_answer(),
                    "content, details and isError all ride"
                );
            }
            other => panic!("expected `HostToolResponse`, got {other:?}"),
        }

        let err = Inbound::HostToolResponse {
            request_id: "host-2".into(),
            result: Err("this session has no board host".into()),
        };
        match decode_inbound(&encode_inbound(&err)).expect("the Err arm decodes") {
            Inbound::HostToolResponse { request_id, result } => {
                assert_eq!(request_id, "host-2");
                assert_eq!(
                    result.expect_err("the Err arm stays Err"),
                    "this session has no board host",
                    "the prose survives — it is what the model reads"
                );
            }
            other => panic!("expected `HostToolResponse`, got {other:?}"),
        }
    }

    /// An `Err` answer from the Supervisor becomes a FAILED `ToolResult`, not a panic
    /// and not a success — the arm that turns the frame's prose into the model's result.
    #[tokio::test]
    async fn an_err_answer_becomes_a_failed_tool_result_not_a_panic() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);

        let mut fut = entry.executor.execute(&ctx, json!({}));
        poll_once(&mut fut);
        let Outbound::HostToolCall { request_id, .. } = out_rx.try_recv().expect("the frame")
        else {
            panic!("expected a HostToolCall frame");
        };
        let tx = pending
            .lock()
            .await
            .remove(&request_id)
            .expect("the entry is there");
        tx.send(Err("the host refused".into())).expect("parked");
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), fut)
            .await
            .expect("the answer settles the park (no hang)");
        assert!(
            got.is_error,
            "an `Err` answer is a failed tool call: {got:?}"
        );
        assert!(
            text_of(&got).contains("the host refused"),
            "the host's sentence reaches the model: {:?}",
            text_of(&got)
        );
        assert!(
            pending.lock().await.is_empty(),
            "and the entry is gone (the handler removed it; the guard is a no-op)"
        );
    }

    /// A dropped answer channel (the Worker shutting down mid-call) is a `fail`, not a
    /// panic and not a hang.
    #[tokio::test]
    async fn a_dropped_answer_channel_fails_the_call() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entry = attached(None, out_tx, pending.clone());
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);

        let mut fut = entry.executor.execute(&ctx, json!({}));
        poll_once(&mut fut);
        let Outbound::HostToolCall { request_id, .. } = out_rx.try_recv().expect("the frame")
        else {
            panic!("expected a HostToolCall frame");
        };
        // Take the sender and DROP it without answering — the shutdown shape.
        let tx = pending.lock().await.remove(&request_id).expect("there");
        drop(tx);
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), fut)
            .await
            .expect("the dropped oneshot settles the park at once");
        assert!(got.is_error, "a dropped answer fails the call: {got:?}");
    }

    /// The DETACHED [`HostTool`] (what F1's `host_entries` builds, with no channel to
    /// speak on) still refuses LOUDLY, in F1's own pinned sentence, and never parks.
    ///
    /// The pin that keeps `host_entries_attached` from silently becoming the ONLY
    /// constructor: an entry with no channel must answer a `fail` naming itself, never
    /// park on a frame nobody can answer.
    #[tokio::test]
    async fn a_detached_host_tool_still_refuses_loudly() {
        let entry = host_entries(vec![mark_ready()]).remove(0);
        let cancel = CancellationToken::new();
        let ctx = ctx_with_cancel(&cancel);
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), {
            let ctx = &ctx;
            async move { entry.executor.execute(ctx, json!({})).await }
        })
        .await
        .expect("a detached host tool answers at once — it never parks");
        assert!(got.is_error, "the refusal is a fail: {got:?}");
        let text = text_of(&got);
        assert!(
            text.contains("host-owned") && !text.contains("unknown tool"),
            "and it says WHY in F1's own sentence, not the registry's miss sentence: \
             {text:?}"
        );
    }

    /// The attached entries are `ScriptAccess::Allowed` — ADR 0092's preservation rule,
    /// asserted on the ATTACHED half too (the composition production actually runs).
    ///
    /// Mutation note: give a host entry `ScriptAccess::Excluded` and `dispatch_nested`'s
    /// name check refuses it, silently deleting the `roadmap` case that turns an
    /// approved backlog into N Tasks in ONE script with a loop. The census
    /// `the_composed_advertised_set_reaches_its_executors` in
    /// `harness/loop/tests/tools_enabled.rs` is the other half of that pin.
    #[test]
    fn attached_host_entries_are_script_allowed() {
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        let pending: PendingHostToolCalls = Arc::new(TokioMutex::new(HashMap::new()));
        let entries = host_entries_attached(
            vec![
                mark_ready(),
                ToolSpec {
                    name: "list_board".into(),
                    description: "a host-owned board".into(),
                    parameters: json!({"type": "object", "properties": {}}),
                },
            ],
            HostToolChannel::new(Some("s".into()), out_tx, pending),
        );
        assert_eq!(
            entries.iter().map(|e| e.name()).collect::<Vec<_>>(),
            vec!["mark_ready", "list_board"],
            "every spec comes back, in order (the composition inherits `host_entries`)"
        );
        assert!(
            entries.iter().all(|e| e.script.is_callable()),
            "host entries are `Allowed` for scripts, attached or not — an `Excluded` \
             here would advertise a tool to a script and refuse it"
        );
    }
}

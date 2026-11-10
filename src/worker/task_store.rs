//! The Worker's Task-board requester (issue #180 F1, ADR 0092): frames every
//! [`TaskCall`] over the protocol as `Outbound::TaskRequest` and parks until the
//! Supervisor answers with `Inbound::TaskResponse`.
//!
//! ## Why this file exists beside `store.rs` rather than inside it
//!
//! `IpcStore` is the closest relative and the deliberate contrast: every one of its
//! calls is FIRE-AND-FORGET (a write-only frame, no answer, nothing to park on), so
//! it cannot host the two rules this seam lives or dies by — a sync `Store` method
//! has no turn token and cannot be dropped mid-park. This file is the READ round
//! trip, and its shape is `IpcRenderRequester` (`core.rs`), the crate's one
//! pre-existing request/answer pair over this wire: a sync trait method returning a
//! boxed future, an outbound `mpsc` sender, and a pending map the answer resolves.
//!
//! ## The two rules (ADR 0092) and what each one prevents
//!
//! 1. **Register the oneshot BEFORE emitting the frame.** A `TaskResponse` that
//!    arrives between "emit" and "insert" finds no entry, is (correctly) ignored,
//!    and the caller then parks on a oneshot nobody can ever resolve. Emit-first is
//!    the order `IpcDispatcher::dispatch` uses (`dispatch.rs:75-86`) and it survives
//!    there only because that flow has a second path to resolution; here it is a
//!    hang. Pinned by `register_happens_before_the_frame_is_emitted`.
//! 2. **Dropping the parked future removes the pending entry.** The executor's arm
//!    `select!`s this future against the TURN token, so a Stop drops it mid-park —
//!    and a map entry whose `Sender` is gone stays forever. That is a slow leak of
//!    one `oneshot::Sender` per cancelled call per session. [`TaskRequestCleanup`]
//!    is the guard; pinned by `dropping_the_parked_call_removes_the_pending_entry`
//!    and `a_cancelled_turn_leaves_the_pending_map_empty`.
//!
//! Rule 2 has a third failure mode the two above do not cover, and it is the one a
//! reader is most likely to write by accident: a parked future that ignores its
//! token. `the_cancel_arm_returns_promptly_when_the_token_fires` is that pin, and it
//! is why the empty-map tests are paired with a "returns PROMPTLY" test — an empty
//! map after an awaited cancel proves no leak; it does not prove no hang.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};

use crate::seams::{TaskCall, TaskCaller, TaskStoreError};
use crate::worker::protocol::Outbound;

/// The Worker's pending task calls: `request_id` → the oneshot to resolve.
///
/// Keyed by the BARE `request_id`, not a `"{session_id}/{request_id}"` compound key
/// like `permission_key` / `render_key`: the map is per-Worker (one session per
/// Worker), the id is a fresh uuid v4 this file mints, and the requester that
/// inserts and the handler that removes must agree on the spelling WITHOUT either
/// one needing the session id — `IpcTaskStore` never sees it. A compound key here
/// would miss every answer, and a missed answer is a hang, not an error.
pub type PendingTaskRequests =
    Arc<TokioMutex<HashMap<String, oneshot::Sender<Result<Value, TaskStoreError>>>>>;

/// Mint a request id (the `task-` prefix is what makes one recognisable in a log).
fn mint_request_id() -> String {
    format!("task-{}", uuid::Uuid::new_v4())
}

/// The Worker's Task seam (issue #180 F1): frames every call over the protocol.
///
/// Implements the crate-internal [`TaskCaller`], NOT the sync
/// [`TaskStore`](crate::seams::TaskStore) — the desktop's `Db` implements that one,
/// Supervisor-side, because the Supervisor is the sole SQLite writer.
pub struct IpcTaskStore {
    outbound: mpsc::UnboundedSender<Outbound>,
    pending: PendingTaskRequests,
}

impl IpcTaskStore {
    pub fn new(outbound: mpsc::UnboundedSender<Outbound>, pending: PendingTaskRequests) -> Self {
        Self { outbound, pending }
    }
}

impl TaskCaller for IpcTaskStore {
    fn call(
        &self,
        call: TaskCall,
    ) -> Pin<Box<dyn Future<Output = Result<Value, TaskStoreError>> + Send>> {
        let outbound = self.outbound.clone();
        let pending = self.pending.clone();
        Box::pin(async move {
            let request_id = mint_request_id();
            let (tx, rx) = oneshot::channel();
            // (ADR 0092 rule 1) THE ORDER: register, THEN emit. A `TaskResponse`
            // that races the emission must always find the entry — see the module
            // doc for why the reversed order (which `IpcDispatcher::dispatch` gets
            // away with) hangs here. Pinned by
            // `register_happens_before_the_frame_is_emitted`.
            //
            // The guard is constructed BEFORE the insert on purpose: if the future is
            // dropped between the lock being acquired and the insert landing, the key
            // is already tracked, so the guard removes either an entry or nothing (both
            // correct). Constructing it after the insert would leave a window whose drop
            // leaks the entry.
            let _cleanup = TaskRequestCleanup::new(&pending, request_id.clone());
            pending.lock().await.insert(request_id.clone(), tx);

            if outbound
                .send(Outbound::TaskRequest {
                    request_id: request_id.clone(),
                    call,
                })
                .is_err()
            {
                // The pipe is gone: nothing can EVER answer this call, so report it
                // now instead of parking on a oneshot whose only remaining writer is
                // about to be dropped. The guard removes the entry on the way out.
                return Err(TaskStoreError::Io {
                    message: "worker outbound channel closed".into(),
                });
            }

            // Park. The executor's arm `select!`s this against the TURN token, so a
            // Stop drops this future and `cleanup` clears the map (rule 2). An
            // `Err` on the receive side means the SENDER was dropped without
            // answering — the `WorkerCore` handler always answers or removes, so this
            // is the Worker shutting down mid-call.
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(TaskStoreError::Io {
                    message: "task response channel dropped before an answer arrived".into(),
                }),
            }
            // `cleanup` drops HERE on the success path — the handler already removed
            // the entry when it resolved the oneshot, so the guard's removal is a
            // no-op (idempotent). On a cancelled path it drops when the caller drops
            // this future, and that removal is the one that matters.
        })
    }
}

/// The Drop guard for one parked task call (ADR 0092 rule 2).
///
/// Shaped after `SudoPromptCleanup` (`interactive/sudo_exec.rs`), with one
/// deliberate difference: that guard spawns its removal, this one FIRST tries a
/// synchronous `try_lock` removal and only spawns when the lock is contended. The
/// reason is not tidiness — it is what makes the leak test trustworthy. A spawned
/// removal is a task the test has to wait for, and "assert the map is empty" then
/// depends on the runtime having polled that task, which is exactly the kind of
/// assertion that passes under load and flakes later. With the synchronous fast path
/// the entry is gone the instant the future is dropped.
///
/// Idempotent by construction: the `Inbound::TaskResponse` handler removes the entry
/// when it resolves the oneshot, so on the success path the guard's later removal
/// finds nothing to do.
pub struct TaskRequestCleanup {
    pending: PendingTaskRequests,
    /// `None` once taken (a `Drop` runs at most once, but this keeps the field
    /// obviously single-use and makes a double-drop trivially safe).
    key: Option<String>,
}

impl TaskRequestCleanup {
    pub fn new(pending: &PendingTaskRequests, key: String) -> Self {
        Self {
            pending: pending.clone(),
            key: Some(key),
        }
    }
}

impl Drop for TaskRequestCleanup {
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
        // Contended: hand it to the runtime. A dropped runtime cannot run it, and
        // the entry then lives as long the Worker does — bounded, and strictly rarer
        // than the fast path.
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
    use crate::worker::protocol::{decode_outbound, Outbound};
    use serde_json::json;
    use std::task::{Context, Poll};
    use tokio_util::sync::CancellationToken;

    /// The board answer the canned supervisor returns.
    fn board_answer() -> Value {
        json!([
            { "id": "tsk_1", "spacePath": "/s", "number": 1, "title": "One",
              "stageSlug": "todo", "sortOrder": 0, "createdAt": 1, "updatedAt": 2,
              "artifactKeys": ["spec"] }
        ])
    }

    /// Poll a future EXACTLY once with a no-op waker (`Ready` = true).
    ///
    /// This is the whole reason the two order/leak tests below can say anything at
    /// all about a future they never `await`: `call()` returns a future whose async
    /// body has NOT run yet, so a test that drops it without polling proves nothing —
    /// the pending entry was never inserted, and "the map is empty" is vacuously
    /// true even with no Drop guard at all. One poll drives the body to its park.
    fn poll_once(
        fut: &mut Pin<Box<dyn Future<Output = Result<Value, TaskStoreError>> + Send>>,
    ) -> bool {
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        matches!(fut.as_mut().poll(&mut cx), Poll::Ready(_))
    }

    /// A one-verb call that is easy to spot on the wire.
    fn list_board() -> TaskCall {
        TaskCall::ListBoard {
            space_path: "/space/a".into(),
        }
    }

    /// THE ORDER PIN (ADR 0092 rule 1): the oneshot is registered BEFORE the frame
    /// is emitted.
    ///
    /// Why this test is built the way it is, and why the obvious version is worthless.
    /// The obvious version — drive the future, catch the frame, assert the map has the
    /// entry — PASSES ON THE WRONG ORDER. An uncontended `tokio::Mutex::lock()` does
    /// not yield, so on a single-threaded runtime "insert" and "send" both complete
    /// inside ONE poll no matter which comes first in the source, and the entry is
    /// present whenever the frame is observable. That test would be green against a
    /// `dispatch.rs`-style emit-first implementation, which hangs in production the
    /// moment a Supervisor answers fast enough to land between the two statements.
    ///
    /// So the order is forced, not sampled: the test HOLDS the pending-map lock, then
    /// runs the call. A correct implementation blocks at its `lock().await` and
    /// therefore emits NOTHING — no frame can exist before the registration it is
    /// supposed to follow. An emit-first implementation puts the frame on the wire
    /// while the lock is still held, and that is exactly what the assertion rejects.
    /// Deterministic in both directions: no timing window, no load dependence.
    #[tokio::test]
    async fn register_happens_before_the_frame_is_emitted() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());

        // Hold the map lock for the whole first phase. `call()` must reach its
        // registration and STOP there.
        let guard = pending.lock().await;
        let caller = tokio::spawn(async move {
            store
                .call(TaskCall::ListBoard {
                    space_path: "/space/a".into(),
                })
                .await
        });
        // Long enough that the task has certainly reached its first await point.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            out_rx.try_recv().is_err(),
            "a `task-request` frame reached the wire while the pending map was still \
             locked — the implementation emits BEFORE it registers the oneshot, so a \
             Supervisor that answers immediately finds no entry and the caller parks \
             forever (register first, like `native_permission_gate` / `ask_flow`, not \
             like `IpcDispatcher::dispatch`)"
        );
        assert!(!caller.is_finished(), "the call is parked, not resolved");

        // Release: the registration completes and only NOW may the frame appear.
        drop(guard);
        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), out_rx.recv())
            .await
            .expect("the frame is emitted once the registration can proceed")
            .expect("the outbound channel is open");
        let Outbound::TaskRequest { request_id, call } = frame else {
            panic!("expected a TaskRequest frame, got {frame:?}");
        };
        assert_eq!(
            call,
            TaskCall::ListBoard {
                space_path: "/space/a".into()
            }
        );
        assert!(
            request_id.starts_with("task-"),
            "ids are recognisable in a Worker log"
        );
        // THE assertion: the entry exists at the first instant the frame does.
        assert!(
            pending.lock().await.contains_key(&request_id),
            "the frame `{request_id}` is on the wire but the pending map does not hold \
             its oneshot"
        );

        // Answer it, on the SAME parked call, so the test also proves the entry it
        // checked is the one the answer resolves.
        let tx = pending
            .lock()
            .await
            .remove(&request_id)
            .expect("the entry the frame raced is present");
        tx.send(Ok(board_answer()))
            .expect("the caller is still parked");
        let got = caller
            .await
            .expect("the call task does not panic")
            .expect("the call resolves with the Supervisor's answer");
        assert_eq!(got, board_answer(), "the value rides verbatim");
        assert!(
            pending.lock().await.is_empty(),
            "the handler's removal is the normal path; the guard's later removal is a \
             no-op (idempotent), so a resolved call leaves nothing behind"
        );
    }

    /// THE DROP PIN (ADR 0092 rule 2), written against its own false positive.
    ///
    /// The trap: a `call()` future that is never polled has never run its body, so it
    /// never inserted an entry, and "the map is empty after the drop" passes with the
    /// Drop guard DELETED. So this test polls once, and — before dropping — asserts
    /// the map has exactly one entry AND a frame reached the wire. Only then does it
    /// drop, in the SAME runtime, with the map lock FREE, and assert emptiness.
    ///
    /// Mutation note: delete the `Drop` impl (or make it remove nothing) and the
    /// emptiness assertion reddens; move the guard's construction AFTER the insert
    /// and the pre-drop witness still passes while the emptiness one reddens.
    #[tokio::test]
    async fn dropping_the_parked_call_removes_the_pending_entry() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());

        let mut fut = Box::pin(store.call(list_board()));
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

        // Drop the parked future (what the executor's turn-cancel arm does to it),
        // lock NOT held. Then the map must be empty immediately — no spawned task to
        // wait for, which is why `TaskRequestCleanup` prefers `try_lock`.
        drop(fut);
        assert!(
            pending.lock().await.is_empty(),
            "dropping the parked future must remove the pending entry — a surviving \
             entry is a `oneshot::Sender` leaked per cancelled call"
        );
    }

    /// A cancelled turn: the cancelled arm wins AND the map ends up EMPTY.
    ///
    /// The two-armed `select!` shape the executor arms use, reproduced here so the
    /// leak is pinned at the requester rather than only through a tool arm.
    #[tokio::test]
    async fn a_cancelled_turn_leaves_the_pending_map_empty() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());
        let cancel = CancellationToken::new();

        // Fire the token a moment after the call is in flight (the shape of a real
        // Stop). Without this the select would wait on both arms forever.
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                cancel.cancel();
            });
        }

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                r = store.call(list_board()) => panic!(
                    "the call resolved with {r:?} although no answer was ever sent"
                ),
                _ = cancel.cancelled() => {
                    // The arm that returns the cancelled tool result.
                    "cancelled".to_string()
                }
            }
        })
        .await
        .expect("the cancelled arm returns (no hang)");
        assert_eq!(outcome, "cancelled");
        // The select DROPPED the call future on its way out — that is the drop the
        // guard exists for.
        assert!(
            out_rx.try_recv().is_ok(),
            "the frame was emitted before the cancel (the call really was in flight)"
        );
        assert!(
            pending.lock().await.is_empty(),
            "a cancelled turn must leave no pending entry behind"
        );
    }

    /// THE HANG PIN. The empty-map tests above prove no LEAK; this one proves no
    /// HANG, which is the failure mode a reviewer's eye slides past.
    ///
    /// No `TaskResponse` is EVER delivered — the Supervisor is an old build that
    /// routes the frame to `Unknown` and ignores it (the F1→D2 window, Decision B
    /// case ii). The token fires; the select must return PROMPTLY. A variant that
    /// parks on the oneshot and never votes the token into the select hangs here
    /// instead of failing fast, so the assertion is wrapped in a real deadline and
    /// the deadline firing IS the failure being described.
    ///
    /// Mutation note: replace the `select!` with `store.call(..).await` (token
    /// ignored) and this test times out — the honest hang, caught.
    #[tokio::test]
    async fn the_cancel_arm_returns_promptly_when_the_token_fires() {
        let (out_tx, _out_rx) = mpsc::unbounded_channel();
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());
        let cancel = CancellationToken::new();

        let fired = {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                cancel.cancel();
            })
        };

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            // The `async` block is load-bearing, not decoration: `select!` is a
            // macro that EXPANDS to a `match` on a poll, not a future, so it cannot
            // be handed to `timeout` directly.
            async {
                tokio::select! {
                    r = store.call(list_board()) => Err(format!(
                        "the oneshot resolved ({r:?}) although NOTHING ever answered the frame"
                    )),
                    _ = cancel.cancelled() => Ok("cancelled".to_string()),
                }
            },
        )
        .await
        .expect(
            "the cancelled arm returned within the deadline: an unanswered task call \
             MUST be settled by the turn token, or every task tool hangs until the \
             turn is cancelled by something else (and never returns at all if nothing \
             cancels)",
        );
        assert_eq!(
            outcome.expect("the token's arm won"),
            "cancelled",
            "the token's arm is the one that answers"
        );
        fired.await.expect("the canceller task ran");
    }

    /// Two calls mint two different ids, both `task-`-prefixed (a collision would
    /// make one answer resolve the wrong caller — a wrong board, not an error).
    #[tokio::test]
    async fn request_ids_are_unique_and_task_prefixed() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());

        let mut a = Box::pin(store.call(list_board()));
        let mut b = Box::pin(store.call(TaskCall::GetTask {
            task_id: "tsk_2".into(),
        }));
        poll_once(&mut a);
        poll_once(&mut b);

        let mut ids = Vec::new();
        while let Ok(frame) = out_rx.try_recv() {
            let Outbound::TaskRequest { request_id, .. } = frame else {
                panic!("expected TaskRequest, got {frame:?}");
            };
            ids.push(request_id);
        }
        assert_eq!(ids.len(), 2, "one frame per call");
        for id in &ids {
            assert!(id.starts_with("task-"), "got `{id}`");
            assert!(
                id.len() > "task-".len() + 30,
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
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());

        let got = tokio::time::timeout(std::time::Duration::from_secs(5), store.call(list_board()))
            .await
            .expect("a dead pipe is reported at once, never parked on")
            .expect_err("the send failed");
        assert!(matches!(got, TaskStoreError::Io { .. }), "got {got:?}");
        assert!(
            pending.lock().await.is_empty(),
            "the failed send must not leave an entry behind"
        );
    }

    /// The wire spelling of the frame this file emits, checked through the REAL
    /// decoder (not the Rust enum): `IpcTaskStore` and a Supervisor's decoder must
    /// agree on `task-request` / `requestId` / the camelCase verb payload.
    #[tokio::test]
    async fn the_frame_this_file_emits_decodes_as_a_task_request_line() {
        let (out_tx, mut out_rx) = mpsc::unbounded_channel();
        let pending: PendingTaskRequests = Arc::new(TokioMutex::new(HashMap::new()));
        let store = IpcTaskStore::new(out_tx, pending.clone());
        let mut fut = Box::pin(store.call(TaskCall::GetArtifact {
            task_id: "tsk_9".into(),
            key: "spec".into(),
        }));
        poll_once(&mut fut);
        let frame = out_rx.try_recv().expect("the frame was emitted");
        let line = crate::worker::protocol::encode_outbound(&frame);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).expect("one JSON line")["type"],
            "task-request",
            "the wire `type` is the allow-listed name, got {line}"
        );
        match decode_outbound(&line).expect("it round-trips") {
            Outbound::TaskRequest { call, .. } => assert_eq!(
                call,
                TaskCall::GetArtifact {
                    task_id: "tsk_9".into(),
                    key: "spec".into()
                }
            ),
            other => panic!("got {other:?}"),
        }
    }
}

//! The Worker's `SubagentDispatcher` implementation (ADR 0025 Task 2):
//! `dispatch` mints an `id` (uuid), sends a `SubagentDispatch` frame
//! (the RESOLVED model key — the `provider/id` composition the loop's
//! pre-flight resolved against the Worker's catalog) to the core's
//! outbound channel, and returns a `(oneshot::Receiver<SubagentOutcome>,
//! SubagentCancel)` — the receiver is resolved by the core when
//! `Inbound::SubagentResult { id, outcome }` arrives; the
//! `SubagentCancel` (its `cancel()` flips the flag) sends a
//! `SubagentCancel` frame. (Unbounded channel — send never fails.)

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{mpsc, oneshot};

use crate::agent::events::EventSink;
use crate::agent::harness::catalog::Model;
use crate::agent::harness::dispatch::SubagentDispatcher;
use crate::agent::harness::ModelKey;
use crate::agent::subagent::{LaunchConfig, SubagentCancel, SubagentOutcome};
use crate::agent::worker::protocol::Outbound;

/// The `IpcDispatcher`'s waiter map (shared with the `WorkerCore` — the
/// `SubagentResult` handler resolves a waiter by the dispatch `id`).
pub type SubagentWaiters = Arc<StdMutex<HashMap<String, oneshot::Sender<SubagentOutcome>>>>;

/// The Worker's `SubagentDispatcher` (ADR 0025 Task 2): `dispatch` mints
/// an `id` (uuid), sends a `SubagentDispatch` frame (the RESOLVED model
/// key — the `provider/id` composition the loop's pre-flight resolved
/// against the Worker's catalog) to the core's outbound channel, and
/// returns a `(oneshot::Receiver<SubagentOutcome>, SubagentCancel)` —
/// the receiver is resolved by the core when `Inbound::SubagentResult`
/// `{ id, outcome }` arrives; the `SubagentCancel` (its `cancel()`
/// flips the flag) sends a `SubagentCancel` frame.
pub struct IpcDispatcher {
    outbound: mpsc::UnboundedSender<Outbound>,
    waiters: SubagentWaiters,
}

impl IpcDispatcher {
    pub fn new(outbound: mpsc::UnboundedSender<Outbound>, waiters: SubagentWaiters) -> Self {
        Self { outbound, waiters }
    }
}

impl SubagentDispatcher for IpcDispatcher {
    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &self,
        parent_session_id: &str,
        parent_cwd: &std::path::Path,
        parent_model: &Model,
        parent_enabled_tools: Vec<String>,
        agent_name: String,
        launch: LaunchConfig,
        task: String,
        _sink: &Arc<dyn EventSink>,
    ) -> (
        tokio::sync::oneshot::Receiver<SubagentOutcome>,
        SubagentCancel,
    ) {
        let id = uuid::Uuid::new_v4().to_string();
        // The RESOLVED model key — the `provider/id` composition the
        // loop's pre-flight resolved against the Worker's catalog
        // (the Supervisor resolves the key → provider config (it owns
        // the catalog + settings) → the child's `Start`).
        let model_key = ModelKey::from(parent_model).to_string();
        // Unbounded — send never fails (nothing is dropped).
        let _ = self.outbound.send(Outbound::SubagentDispatch {
            id: id.clone(),
            parent_session_id: parent_session_id.to_string(),
            parent_cwd: parent_cwd.display().to_string(),
            parent_enabled_tools,
            agent_name,
            launch,
            task,
            model_key,
        });
        // The receiver is resolved by the core when `Inbound::
        // SubagentResult { id, outcome }` arrives (the waiter is
        // registered under the dispatch `id`).
        let (tx, rx) = oneshot::channel::<SubagentOutcome>();
        self.waiters
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(id.clone(), tx);
        // The `SubagentCancel` handle: its `cancel()` flips the flag — a
        // watcher task turns the flip into the `SubagentCancel` frame
        // (the `SubagentCancel::Flag` variant — the `watch` sender is the
        // flip, the receiver is the watcher's observation point). The
        // task ends when the flag flips OR the receiver is dropped (the
        // `SubagentCancel` held by the loop is dropped — the loop task
        // ended — the `watch` sender dropped → `wait_for` errors out).
        // A cancel that never happens leaks nothing: the task dies with
        // the loop.
        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);
        let cancel = SubagentCancel::new_flag(cancel_tx);
        let outbound = self.outbound.clone();
        tokio::spawn(async move {
            if cancel_rx.wait_for(|v| *v).await.is_ok() {
                let _ = outbound.send(Outbound::SubagentCancel { id });
            }
        });
        (rx, cancel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::events::EventSink;
    use crate::agent::harness::catalog::Model;
    use crate::agent::subagent::{LaunchConfig, SubagentOutcome};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::mpsc;

    /// A no-op `EventSink` (the `dispatch` signature's `sink` parameter —
    /// the child's sink is the Supervisor's, not the Worker's).
    struct NoopSink;
    impl EventSink for NoopSink {
        fn emit(&self, _event: &str, _payload: serde_json::Value) {}
    }

    fn test_model() -> Model {
        Model {
            id: "m1".to_string(),
            provider: "fake".to_string(),
            base_url: "http://fake/v1".to_string(),
            api_key: "k".to_string(),
            context_window: 100_000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: Some("openai-completions".to_string()),
        }
    }

    /// The `(dispatcher, outbound, waiters)` test fixture.
    fn fixture() -> (
        IpcDispatcher,
        mpsc::UnboundedReceiver<Outbound>,
        SubagentWaiters,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let waiters = Arc::new(StdMutex::new(HashMap::new()));
        let dispatcher = IpcDispatcher::new(tx, waiters.clone());
        (dispatcher, rx, waiters)
    }

    /// `dispatch` → a `SubagentDispatch` frame (the `model_key` is the
    /// `provider/id` composition) + a registered waiter under the frame's
    /// `id` (resolving it resolves the returned oneshot).
    #[tokio::test]
    async fn dispatch_emits_the_frame_and_registers_the_waiter() {
        let (dispatcher, mut rx, waiters) = fixture();
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let (outcome_rx, _cancel) = dispatcher.dispatch(
            "s1",
            std::path::Path::new("/tmp/space"),
            &test_model(),
            vec!["bash".to_string()],
            "tester".to_string(),
            LaunchConfig {
                system_prompt: Some("child".to_string()),
                ..Default::default()
            },
            "do the thing".to_string(),
            &sink,
        );
        match rx.try_recv().expect("the frame is on the channel") {
            Outbound::SubagentDispatch {
                id,
                parent_session_id,
                parent_cwd,
                parent_enabled_tools,
                agent_name,
                launch,
                task,
                model_key,
            } => {
                assert_eq!(parent_session_id, "s1");
                assert_eq!(parent_cwd, "/tmp/space");
                assert_eq!(parent_enabled_tools, vec!["bash".to_string()]);
                assert_eq!(agent_name, "tester");
                assert_eq!(task, "do the thing");
                assert_eq!(
                    launch.system_prompt.as_deref(),
                    Some("child"),
                    "the launch config rides verbatim"
                );
                assert_eq!(
                    model_key, "fake/m1",
                    "the model key is the provider/id composition"
                );
                // The waiter is registered under the frame's `id` —
                // resolving it resolves the loop's oneshot.
                let waiter = waiters
                    .lock()
                    .unwrap()
                    .remove(&id)
                    .expect("the waiter is registered under the frame id");
                waiter
                    .send(SubagentOutcome::Failed {
                        error: "boom".to_string(),
                    })
                    .expect("the waiter is live");
            }
            other => panic!("expected `SubagentDispatch`, got {other:?}"),
        }
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), outcome_rx)
            .await
            .expect("the oneshot resolved within the bound")
            .expect("the oneshot was not dropped");
        assert_eq!(
            outcome,
            SubagentOutcome::Failed {
                error: "boom".to_string()
            },
            "resolving the registered waiter resolves the returned oneshot"
        );
    }

    /// The `SubagentCancel` handle → a `SubagentCancel` frame (the
    /// `cancel()` flips the flag; the frame carries the dispatch `id`).
    #[tokio::test]
    async fn cancel_emits_the_subagent_cancel_frame() {
        let (dispatcher, mut rx, _waiters) = fixture();
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let (outcome_rx, cancel) = dispatcher.dispatch(
            "s1",
            std::path::Path::new("/tmp/space"),
            &test_model(),
            Vec::new(),
            "a".to_string(),
            LaunchConfig::default(),
            "t".to_string(),
            &sink,
        );
        // The dispatch frame is first.
        let id = match rx.try_recv().expect("the frame is on the channel") {
            Outbound::SubagentDispatch { id, .. } => id,
            other => panic!("expected `SubagentDispatch`, got {other:?}"),
        };
        drop(outcome_rx);
        cancel.cancel();
        // The cancel frame is emitted by the flag-watcher task — give it
        // a tick (the `watch` `wait_for` fires on the flag flip).
        let mut frame = None;
        for _ in 0..100 {
            if let Ok(f) = rx.try_recv() {
                frame = Some(f);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        match frame.expect("the cancel frame is emitted after `cancel()`") {
            Outbound::SubagentCancel { id: back } => {
                assert_eq!(back, id, "the frame carries the dispatch id")
            }
            other => panic!("expected `SubagentCancel`, got {other:?}"),
        }
    }
}

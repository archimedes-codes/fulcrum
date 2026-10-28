use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::watch;

use crate::agent::events::EventSink;
use crate::agent::harness::catalog::{Model, ModelCatalog};
use crate::agent::subagent::{
    AgentNameResolution, LaunchConfig, SubagentFailureReason, SubagentOutcome,
};
use crate::agent::worker::client::{WorkerError, WorkerHandle, WorkerInboundEvent};
use crate::agent::worker::manager::{
    feed_capture, finish_subagent, note_liveness_event, note_liveness_frame, note_liveness_gate,
    SubagentCapture, SubagentDrive, SubagentLiveness, SubagentOutcomeSink, WorkerFactory,
    WorkerManager,
};
use crate::agent::worker::protocol::{StartEnv, StartMode, SubagentDispatchWire};

/// The test logging type aliases (the `clippy::type_complexity`
/// bound — the `Arc<StdMutex<...>>` shapes are repeated across
/// the test helpers).
type CrashLog = Arc<StdMutex<Vec<(String, Option<i32>, Option<u32>)>>>;
type EventLog = Arc<StdMutex<Vec<(String, bool, WorkerInboundEvent)>>>;
type SinkMap = Arc<StdMutex<HashMap<String, Arc<CollectorSink>>>>;
type SinkEvents = Arc<StdMutex<Vec<(String, Value)>>>;

/// The `fake_worker` fixture (the `fake_mcp_stdio` convention — the
/// `CARGO_MANIFEST_DIR`/`target/debug` path; `cargo test` builds the
/// bin targets).
fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/target/debug/fake_worker"
    ))
}

/// The test factory (the `WorkerFactory` seam — the `fake_worker`
/// binary; production is `WorkerHandle::spawn(std::env::
/// current_exe().as_path())`).
struct FakeWorkerFactory;

impl WorkerFactory for FakeWorkerFactory {
    fn spawn(&self) -> Result<WorkerHandle, WorkerError> {
        WorkerHandle::spawn(&fixture_path())
    }
}

/// A UI sink collector (the parent-scope `TauriSink` test double).
#[derive(Clone)]
struct CollectorSink {
    events: Arc<StdMutex<Vec<(String, Value)>>>,
}

impl CollectorSink {
    fn new() -> (Self, SinkEvents) {
        let events = Arc::new(StdMutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

impl EventSink for CollectorSink {
    fn emit(&self, event: &str, payload: Value) {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((event.to_string(), payload));
    }
}

/// A test `StartEnv` (a fake model the fixture's `SubagentDispatch`
/// `model_key` — `fake/m1` — resolves against).
fn test_env(session_id: &str, enabled_tools: Option<Vec<String>>) -> StartEnv {
    fn model(id: &str) -> Model {
        Model {
            id: id.to_string(),
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
    StartEnv::from_parts(
        session_id.to_string(),
        "/tmp/space".to_string(),
        StartMode::Fresh,
        None,
        model("m1"),
        ModelCatalog {
            models: vec![model("m1")],
            ..Default::default()
        },
        None,
        true,
        enabled_tools,
        "/tmp/config".to_string(),
        true,
        None,
    )
}

/// A `WorkerManager` on the `fake_worker` factory + the collectors
/// (`on_crash` / `on_event` / the parent-scope sink lookup).
fn make_manager(dead_bound: Option<Duration>) -> (WorkerManager, CrashLog, EventLog, SinkMap) {
    let crashes = Arc::new(StdMutex::new(Vec::new()));
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sinks: SinkMap = Arc::new(StdMutex::new(HashMap::new()));
    let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
    let crashes_c = crashes.clone();
    let events_c = events.clone();
    let sinks_c = sinks.clone();
    let manager = WorkerManager::new(
        factory,
        Arc::new(move |id: String, code: Option<i32>, pid: Option<u32>| {
            crashes_c
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((id, code, pid));
        }),
        Arc::new(
            move |id: String, is_subagent: bool, evt: WorkerInboundEvent| {
                events_c
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push((id, is_subagent, evt));
            },
        ),
        Arc::new(move |id: &str| {
            sinks_c
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned()
                .map(|s| s as Arc<dyn EventSink>)
        }),
    );
    let manager = match dead_bound {
        Some(t) => manager.with_dead_bound(t),
        None => manager,
    };
    (manager, crashes, events, sinks)
}

/// The self-releasing PUMP STALL (`make_stalling_manager`): the park length
/// (long enough that the child writes the rest of its burst, its process
/// dies and the `exited` WATCH fires WHILE the pump is parked, short enough
/// that a wrong fix fails an assertion instead of hanging the suite) and the
/// deadline past which the gate stops parking (so a scenario whose stall was
/// never reached cannot stall twice, and cannot park forever).
///
/// `PUMP_STALL` MUST stay SHORTER than the drive's `PUMP_EXIT_GRACE`: the test
/// exists to force the pump's own exit arm, and a stall longer than the grace
/// would drive the wedged-pump FALLBACK instead — which deliberately salvages
/// the capture as-is.
const PUMP_STALL: Duration = Duration::from_millis(250);
const PUMP_STALL_DEADLINE: Duration = Duration::from_secs(10);

/// The **Dead bound** both ADR 0055 liveness tests inject (`#34`).
///
/// The arithmetic, because it is the whole point of the number:
/// - **15×** the fixture's 100 ms `LONG_STREAM_GAP`, so the drive may lose
///   fifteen consecutive frames — or the parent's read → decode → broadcast →
///   pump chain may stall fifteen frames deep — before a streaming child is
///   reaped. It used to be 300 ms, i.e. 3×, and ONE >300 ms observation gap on a
///   loaded `macos-latest` runner reddened CI with 8 of 15 frames observed.
/// - ~2× the observation lag the one recorded CI failure implies (~700 ms — seven
///   100 ms frames the parent had not seen at 1.9 s), which is the quantity that
///   actually breaks these tests: a bound sized against the CADENCE alone is
///   answering the wrong question.
/// - **HALF** the fixture's legitimate turn (30 chunks × 100 ms, and a 3.0 s
///   `IN_TOOL_HOLD`). This is what keeps both tests RED against the pre-ADR 0055
///   drive — the reaping still lands inside the child's production (streaming) or
///   inside its silent tool hold (in-tool). Raise the bound without raising the
///   fixture and the regression stops being caught — that trade is the reason both
///   moved.
const LIVENESS_DEAD_BOUND: Duration = Duration::from_millis(1500);

/// The one-shot pump-stall gate (the state `make_stalling_manager` closes
/// over): `armed` is flipped to `false` by the ONE frame it holds, and
/// `fired` records that the stall really happened — a test asserts it,
/// because a stall that never engaged forced no ordering at all.
struct StallGate {
    armed: StdMutex<bool>,
    fired: StdMutex<bool>,
    started: StdMutex<Option<std::time::Instant>>,
}

impl StallGate {
    fn new() -> Self {
        Self {
            armed: StdMutex::new(true),
            fired: StdMutex::new(false),
            started: StdMutex::new(None),
        }
    }

    /// Park THIS pump frame: one shot, then disarm; a bounded park, and no
    /// park at all once the deadline has passed. Returns `true` when this is
    /// the frame the gate was armed for.
    fn fire_once(&self) -> bool {
        if !*self.armed.lock().unwrap_or_else(|p| p.into_inner()) {
            return false;
        }
        let started = *self.started.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(t) = started {
            if t.elapsed() > PUMP_STALL_DEADLINE {
                return false;
            }
        } else {
            *self.started.lock().unwrap_or_else(|p| p.into_inner()) =
                Some(std::time::Instant::now());
        }
        *self.armed.lock().unwrap_or_else(|p| p.into_inner()) = false;
        *self.fired.lock().unwrap_or_else(|p| p.into_inner()) = true;
        // The park is BLOCKING inside the synchronous `on_event` callback —
        // the pump's own loop cannot advance while it sits here, which is
        // exactly the point: the child's queued frames stay UNCONSUMED while
        // the drive races.
        std::thread::sleep(PUMP_STALL);
        true
    }

    fn fired(&self) -> bool {
        *self.fired.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// `make_manager`'s shape + a ONE-SHOT pump stall: the first child frame
/// matching `stall_on` parks the pump inside the router (`on_event` runs
/// BEFORE the pump's own `match`, so parking it leaves the child's remaining
/// frames unconsumed in the client's `broadcast` ring).
///
/// This is how a test FORCES the ordering race ADR 0055 review thread 3
/// names — the client publishes the process exit through the `exited` WATCH,
/// a DIFFERENT channel from the frame `broadcast` — instead of merely
/// observing a favourable interleaving and hoping it recurs.
fn make_stalling_manager(
    dead_bound: Duration,
    stall_on: impl (Fn(&WorkerInboundEvent) -> bool) + Send + Sync + 'static,
    gate: Arc<StallGate>,
) -> (WorkerManager, EventLog, SinkMap) {
    let events = Arc::new(StdMutex::new(Vec::new()));
    let sinks: SinkMap = Arc::new(StdMutex::new(HashMap::new()));
    let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
    let events_c = events.clone();
    let sinks_c = sinks.clone();
    let manager = WorkerManager::new(
        factory,
        Arc::new(|_id: String, _code: Option<i32>, _pid: Option<u32>| {}),
        Arc::new(
            move |id: String, is_subagent: bool, evt: WorkerInboundEvent| {
                // ONLY a child frame parks the pump, so the parent's own
                // stream — the `subagent-result-ack` included — keeps flowing
                // while the child's is stuck, and the test can still observe
                // the outcome the drive delivered.
                if is_subagent && stall_on(&evt) {
                    gate.fire_once();
                }
                events_c
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push((id, is_subagent, evt));
            },
        ),
        Arc::new(move |id: &str| {
            sinks_c
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .cloned()
                .map(|s| s as Arc<dyn EventSink>)
        }),
    );
    (manager.with_dead_bound(dead_bound), events, sinks)
}

/// A parent-scope sink (registered under `session_id`).
fn register_sink(sinks: &SinkMap, session_id: &str) -> SinkEvents {
    let (sink, events) = CollectorSink::new();
    sinks
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(session_id.to_string(), Arc::new(sink));
    events
}

/// Collect the `on_event` deliveries for `session_id` (no waiting —
/// the callers bounded-wait first).
fn collect_now(events: &EventLog, session_id: &str) -> Vec<(bool, WorkerInboundEvent)> {
    events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(id, _, _)| id == session_id)
        .map(|(_, is_sub, evt)| (*is_sub, evt.clone()))
        .collect()
}

/// Bounded-wait until the `on_event` deliveries for `session_id`
/// contain a frame matching `pred` (a deadline, so a missing frame
/// is a test failure, not a hang).
async fn wait_for_event(
    events: &EventLog,
    session_id: &str,
    pred: impl Fn(&WorkerInboundEvent) -> bool,
) -> Vec<(bool, WorkerInboundEvent)> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got = collect_now(events, session_id);
        if got.iter().any(|(_, e)| pred(e)) || tokio::time::Instant::now() >= deadline {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The `SubagentResult` the supervisor sent to the requester (the
/// fixture's `subagent-result` INBOUND frame → the fixture's
/// `subagent-result-ack` `SinkFrame` — the round-trip observation
/// point; the `outcome` rides verbatim).
fn subagent_result_outcome(events: &EventLog, session_id: &str) -> Option<SubagentOutcome> {
    events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(id, _, _)| id == session_id)
        .find_map(|(_, _, e)| match e {
            WorkerInboundEvent::SinkFrame { event, payload } if event == "subagent-result-ack" => {
                serde_json::from_value::<SubagentOutcome>(payload["outcome"].clone()).ok()
            }
            _ => None,
        })
}

/// Dispatch a child directly with `task` (the ADR 0055 regression
/// harness — the same `dispatch_subagent` entry point the parent's
/// `SubagentDispatch` frame reaches through the pump, so the ONLY thing
/// a test supplies is the child's task; the wire shape mirrors
/// `subagent.rs`'s). The outcome reaches the parent Worker exactly as in
/// the frame-driven tests, so `subagent_result_outcome` stays the
/// observation point.
fn dispatch_child_task(manager: &WorkerManager, parent_session_id: &str, task: &str) {
    dispatch_child_task_with_id(manager, parent_session_id, task, "live-stream");
}

/// The same, with an explicit DISPATCH id. The `__cancel__` fixture emits
/// `SubagentCancel { id: "sub1" }`, so a cancel-driven test must dispatch
/// under `sub1` for that frame to name its drive (the cancel is routed by
/// DISPATCH id, not by child session id — `dispatch_child_task`'s
/// generated ids are unroutable from the fixture).
fn dispatch_child_task_with_id(
    manager: &WorkerManager,
    parent_session_id: &str,
    task: &str,
    dispatch_id: &str,
) {
    manager.dispatch_subagent(
        parent_session_id,
        SubagentDispatchWire {
            id: dispatch_id.to_string(),
            parent_session_id: parent_session_id.to_string(),
            parent_cwd: "/tmp/space".to_string(),
            parent_enabled_tools: vec!["bash".to_string(), "read".to_string()],
            agent_name: "tester".to_string(),
            name_resolution: AgentNameResolution::Matched,
            launch: LaunchConfig::default(),
            task: task.to_string(),
            // The RESOLVED `provider/id` key (what `subagent.rs` composes —
            // it resolves against `test_env`'s `fake/m1` catalog entry).
            model_key: "fake/m1".to_string(),
        },
    );
}

/// Bounded-wait for the child's `subagent-result-ack`, then read the
/// `SubagentOutcome` that rode it (the ack-first order is what lets a
/// caller cancel BEFORE the outcome exists).
async fn subagent_result_outcome_after_ack(events: &EventLog, session_id: &str) -> SubagentOutcome {
    let got = wait_for_event(events, session_id, |e| {
            matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
        })
        .await;
    subagent_result_outcome(events, session_id)
        .unwrap_or_else(|| panic!("the outcome rides the ack verbatim: {got:?}"))
}

/// Bounded-wait for the `subagent-closed` payload on the parent's sink
/// (`finish_subagent` emits it on EVERY exit path; the `SinkEvents`
/// collector is already registered per parent session).
async fn wait_for_closed(sink_events: &SinkEvents) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got: Vec<(String, Value)> = sink_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some((_, payload)) = got.iter().find(|(e, _)| e == "subagent-closed") {
            return payload.clone();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("the `subagent-closed` event never fired");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Every payload the parent's sink received under `name`, in emission
/// order (the observation point for the frames `finish_subagent` emits
/// BESIDES `subagent-closed` — the ADR 0055 `interactive-request-close`).
fn emitted(sink_events: &SinkEvents, name: &str) -> Vec<Value> {
    sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .filter(|(e, _)| e == name)
        .map(|(_, p)| p.clone())
        .collect()
}

/// The CHILD session id (the `subagent-session-started` payload — the
/// only frame that names the child before the teardown; a dispatch whose
/// child id the test cannot guess still gets an exact assertion).
async fn wait_for_child_id(sink_events: &SinkEvents) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(p) = emitted(sink_events, "subagent-session-started").first() {
            return p["sessionId"]
                .as_str()
                .expect("the started payload carries the child sessionId")
                .to_string();
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("the `subagent-session-started` event never fired");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The drive's CURRENT unanswered-gate set (sorted) — the same value
/// `finish_subagent` drains at teardown, read through the pump's `watch`.
fn open_gates_view(manager: &WorkerManager, child_id: &str) -> Vec<String> {
    let drives = manager
        .state
        .drives
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let mut ids: Vec<String> = drives
        .get(child_id)
        .expect("the drive is in flight")
        .liveness_tx
        .borrow()
        .open_gates
        .iter()
        .cloned()
        .collect();
    ids.sort();
    ids
}

/// Bounded-wait until the drive has RECORDED `id` as an unanswered gate
/// (the pump records the frame in its own `match`, which runs AFTER the
/// router delivery a test would otherwise wait on — without this the
/// cancel could win the race against the recording and the test would be
/// flaky rather than false).
async fn wait_for_open_gate(manager: &WorkerManager, child_id: &str, id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if open_gates_view(manager, child_id).iter().any(|g| g == id) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("the pump never recorded `{id}` as an open gate");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `attach` + `detach` bookkeeping (the `workers` map — the
/// `test_support`-exposed registry view): `attach` inserts the
/// session (the `ready` handshake + the `start` envelope); `detach`
/// removes it + the process exits (a `close` → exit 0 — the
/// EXPECTED exit fires NO `on_crash`).
#[tokio::test]
async fn attach_and_detach_bookkeeping() {
    let (manager, crashes, _events, _sinks) = make_manager(None);
    manager
        .attach("s1", &test_env("s1", Some(vec!["bash".to_string()])))
        .await
        .expect("the attach completes (the ready handshake)");
    assert!(
        manager.test_is_attached("s1"),
        "the attach inserted the session"
    );
    assert_eq!(manager.test_session_count(), 1);
    let handle = manager.detach("s1").expect("the detach returns the handle");
    assert!(
        !manager.test_is_attached("s1"),
        "the detach removed the session"
    );
    // The `close` → the Worker self-exits 0 (the EXPECTED exit —
    // NO `on_crash`).
    let code = handle.exited().await;
    assert_eq!(code, Some(0), "the detached worker exited 0");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        crashes.lock().unwrap_or_else(|p| p.into_inner()).is_empty(),
        "an expected detach-then-exit fires NO `on_crash`"
    );
}

/// `reap_all` kills all attached Workers (the `close` + grace +
/// `kill` — the app-exit path; the processes exited, the registry
/// is marked reaping, NO `on_crash`).
#[tokio::test]
async fn reap_all_kills_all_attached_workers() {
    let (manager, crashes, _events, _sinks) = make_manager(None);
    let _ = manager.attach("b1", &test_env("b1", None)).await;
    let _ = manager.attach("b2", &test_env("b2", None)).await;
    assert_eq!(manager.test_session_count(), 2);
    manager.reap_all().await;
    // The processes exited (the `close` → the fixture self-exits
    // 0; the `kill` is the belt-and-braces).
    assert!(
        manager.test_all_reaping(),
        "the reap marked every session reaping"
    );
    assert!(
        crashes.lock().unwrap_or_else(|p| p.into_inner()).is_empty(),
        "a `reap_all` is an EXPECTED exit — NO `on_crash`"
    );
}

/// The crash's PID reaches the Supervisor (the router's attribution
/// source): `on_crash` carries the pid of the child that just died —
/// captured off the handle BEFORE the pump's future is spawned,
/// because `Child::id()` is already `None` once the child is reaped.
/// Without it the router can only guess which of N crashed Workers the
/// banner should name.
#[tokio::test]
async fn the_crash_callback_carries_the_spawned_handles_pid() {
    let (manager, crashes, _events, _sinks) = make_manager(None);
    manager
        .attach("c1", &test_env("c1", None))
        .await
        .expect("the attach completes");
    let spawned = manager
        .handle_for("c1")
        .expect("the handle is reachable")
        .pid()
        .expect("a live child reports its spawn-time pid");
    manager
        .handle_for("c1")
        .expect("the handle is reachable")
        .send_prompt("__crash__", &[])
        .expect("the prompt encodes");

    // The `on_crash` fires with the SAME pid the handle carries (the
    // child exits 137 — `fake_worker`'s `__crash__`).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let fired = loop {
        let c = crashes.lock().unwrap_or_else(|p| p.into_inner()).clone();
        if let Some(hit) = c.iter().find(|(id, _, _)| id == "c1") {
            break Some(hit.clone());
        }
        if tokio::time::Instant::now() >= deadline {
            break None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    .expect("`on_crash` fires for the crashed Worker");
    assert_eq!(
        fired.2,
        Some(spawned),
        "the `on_crash` pid is the pid of the Worker that died (the handle's was {spawned})"
    );
}

/// A crashed Worker fires `on_crash` exactly ONCE with the right
/// code (an unexpected `Exited` — no `detach` was called; the
/// `Exited` frame is terminal, so the pump fires it exactly once).
#[tokio::test]
async fn a_crashed_worker_fires_on_crash_exactly_once() {
    let (manager, crashes, events, _sinks) = make_manager(None);
    manager
        .attach("c1", &test_env("c1", None))
        .await
        .expect("the attach completes");
    manager
        .handle_for("c1")
        .expect("the handle is reachable")
        .send_prompt("__crash__", &[])
        .expect("the prompt encodes");
    // The `Exited` frame reaches the router (the `on_event` — the
    // Task-4 router's `session-closed` handling).
    let got = wait_for_event(&events, "c1", |e| {
        matches!(e, WorkerInboundEvent::Exited(_))
    })
    .await;
    assert!(
        got.iter()
            .any(|(_, e)| { matches!(e, WorkerInboundEvent::Exited(Some(137))) }),
        "the `Exited` frame reached the router: {got:?}"
    );
    // The `on_crash` fires (the crash detection — the process exit
    // 137).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let c = crashes.lock().unwrap_or_else(|p| p.into_inner()).clone();
        if c.iter()
            .any(|(id, code, _)| id == "c1" && *code == Some(137))
            || tokio::time::Instant::now() >= deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A settle (the `on_crash` is fired exactly ONCE — the
    // `Exited` frame is terminal, the pump loop ends).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let c = crashes.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let hits: Vec<_> = c
        .iter()
        .filter(|(id, code, _)| id == "c1" && *code == Some(137))
        .collect();
    assert_eq!(
        hits.len(),
        1,
        "the crash fired `on_crash` exactly once: {c:?}"
    );
    // A LATER `detach` (the cleanup) is an expected exit — no
    // second `on_crash`.
    manager.detach("c1");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let c = crashes.lock().unwrap_or_else(|p| p.into_inner()).clone();
    assert_eq!(
        c.iter().filter(|(id, _, _)| id == "c1").count(),
        1,
        "the `on_crash` is not re-fired by the cleanup detach: {c:?}"
    );
}

/// The `dispatch_subagent` flow: a `SubagentDispatch` from a
/// main-session `fake_worker` (the `__subagent__` prompt) spawns a
/// subagent `fake_worker` (a fresh ephemeral session — the canned
/// settle) and delivers `SubagentResult` to the REQUESTING Worker
/// (two `fake_worker`s — the outcome rides the wire back; the
/// fixture's `subagent-result-ack` is the round-trip observation
/// point).
#[tokio::test]
async fn dispatch_subagent_spawns_a_subagent_and_delivers_the_result() {
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(30)));
    manager
        .attach("p1", &test_env("p1", Some(vec!["bash".to_string()])))
        .await
        .expect("the parent attach completes");
    manager
        .handle_for("p1")
        .expect("the parent handle is reachable")
        .send_prompt("__subagent__", &[])
        .expect("the prompt encodes");
    // The `SubagentResult` frame arrives at the REQUESTING Worker
    // (the supervisor sent `Inbound::SubagentResult` to it — the
    // fixture's `subagent-result-ack` is the round-trip
    // observation point).
    let got = wait_for_event(&events, "p1", |e| {
            matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
        })
        .await;
    assert!(
            got.iter().any(|(_, e)| {
                matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
            }),
            "the `SubagentResult` frame reached the requester: {got:?}"
        );
    // The outcome is `Completed` (the `SubagentCapture`'s final
    // text — the fixture's canned `agent_message_chunk` stream).
    let outcome =
        subagent_result_outcome(&events, "p1").expect("the outcome rides the ack verbatim");
    match outcome {
        SubagentOutcome::Completed { output, metrics } => {
            assert_eq!(
                output, "canned answer",
                "the `SubagentCapture`'s final text"
            );
            assert!(metrics.duration_ms >= 1, "the wall clock (ms)");
        }
        other => panic!("expected `Completed`, got {other:?}"),
    }
    // The subagent Worker ran (a fresh ephemeral session — its
    // `agent_settled` reached the router with `is_subagent: true`).
    // The `cloned` (OWNED) — the `Vec` must not borrow the guard
    // (the guard is dropped at the block's end).
    let sub_events: Vec<(String, bool, WorkerInboundEvent)> = {
        let guard = events.lock().unwrap_or_else(|p| p.into_inner());
        guard
            .iter()
            .filter(|(_, is_sub, _)| *is_sub)
            .cloned()
            .collect()
    };
    assert!(
        !sub_events.is_empty(),
        "a subagent Worker's events reached the router with the flag"
    );
    assert!(
        sub_events
            .iter()
            .any(|(_, is_sub, e)| { *is_sub && e.is_agent_settled() }),
        "the subagent settled (the canned settle): {sub_events:?}"
    );
    // The subagent session is gone (the drive task's UNCONDITIONAL
    // teardown — the `drives` entry is removed).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        manager.test_session_count(),
        1,
        "only the parent remains (the subagent was reaped)"
    );
}

/// The `SubagentCapture`: capture from the subagent's `SinkFrame`
/// `session-update` stream — the enveloped `agent_message_chunk`
/// frames — the last-`messageId`-with-non-empty-text rule (a tool-using
/// turn does NOT concatenate every intermediate message; the
/// `messageId: "system"` bookkeeping chunks are NEVER captured). NOTE the
/// id shape: the bare `"system"` below is FIXTURE INPUT — a frame written
/// by hand into `observe_sink_frame`. What production puts on the wire is
/// the stamped `{stamp}:system` (ADR 0049); the predicate accepts both.
#[test]
fn the_subagent_capture_captures_the_final_text_from_the_sink_frames() {
    let capture = SubagentCapture::new();
    // A first message (a tool-using turn — an intermediate
    // message).
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "m1",
                "content": { "type": "text", "text": "intermediate " }
            }
        }),
    );
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "m1",
                "content": { "type": "text", "text": "text" }
            }
        }),
    );
    // The `system` bookkeeping chunk (NEVER captured — a failed
    // child turn must not settle with a one-liner as its output).
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "system",
                "content": { "type": "text", "text": "Retry failed" }
            }
        }),
    );
    // The LAST message (the final output — a tool-using turn does
    // NOT concatenate every intermediate message).
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "m2",
                "content": { "type": "text", "text": "final " }
            }
        }),
    );
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "m2",
                "content": { "type": "text", "text": "answer" }
            }
        }),
    );
    assert_eq!(
        capture.captured_text(),
        "final answer",
        "the last-`messageId`-with-non-empty-text rule"
    );
    // The usage accumulation (the `message_update` `usage` field —
    // the metrics' token fields).
    capture.observe_event(&crate::agent::events::RpcEvent::message_update {
        usage: serde_json::json!({ "inputTokens": 10, "outputTokens": 5 }),
        assistant_message_event: serde_json::json!({}),
    });
    assert_eq!(capture.input_tokens(), 10);
    assert_eq!(capture.output_tokens(), 5);
}

/// (ADR 0048) The capture is what the PARENT reads. A child that retried
/// must hand back the retry's text alone, never `part` + `done`.
#[test]
fn a_retraction_clears_the_captured_child_text() {
    let capture = SubagentCapture::new();
    let chunk = |id: &str, t: &str| {
        serde_json::json!({ "sessionId": "s", "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": t }, "messageId": id } })
    };
    capture.observe_sink_frame("session-update", &chunk("m1", "part"));
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({ "sessionId": "s", "update": {
                "sessionUpdate": "message_retracted", "messageId": "m1" } }),
    );
    capture.observe_sink_frame("session-update", &chunk("m1", "done"));
    assert_eq!(capture.captured_text(), "done");
}

/// The discard line is harness bookkeeping, not the child's output (ADR 0048
/// gives it a unique id precisely so it can be told apart).
#[test]
fn a_discard_line_is_not_captured_as_child_output() {
    let capture = SubagentCapture::new();
    capture.observe_sink_frame(
        "session-update",
        &serde_json::json!({ "sessionId": "s", "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "Discarded an incomplete reply" },
                "messageId": "retract:1" } }),
    );
    assert_eq!(capture.captured_text(), "");
}

// ── The `SubagentLiveness` signal (ADR 0055) ──────────────────────

/// Seed a real `SubagentDrive` in the real `drives` map under
/// `child_id` (the map the pump feeds through `feed_capture` — the
/// SAME entry production inserts in `dispatch_subagent`; the
/// `handle` is `None` because these tests never talk to a Worker
/// process, they hand the pump's feed the frames).
fn seed_drive(manager: &WorkerManager, child_id: &str) {
    manager
        .state
        .drives
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            child_id.to_string(),
            SubagentDrive {
                parent_session_id: "live-parent".to_string(),
                dispatch_id: "live-dispatch".to_string(),
                settle_tx: watch::channel(false).0,
                cancel_tx: watch::channel(false).0,
                exit_tx: watch::channel(None).0,
                liveness_tx: watch::channel(SubagentLiveness {
                    seq: 0,
                    in_flight: Default::default(),
                    open_gates: Default::default(),
                })
                .0,
                capture: Arc::new(SubagentCapture::new()),
                handle: None,
                reaping: false,
            },
        );
}

/// The drive's CURRENT liveness signal read through the pump's own
/// `watch` — the observation point is the SAME value the drive task's
/// receiver reads (Task 2's silence bound), NOT a copy the test
/// mutated. `(seq, the sorted in-flight ids)`.
fn liveness_view(manager: &WorkerManager, child_id: &str) -> (u64, Vec<String>) {
    let drives = manager
        .state
        .drives
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let drive = drives.get(child_id).expect("the drive is seeded");
    let liveness = drive.liveness_tx.borrow();
    let mut ids: Vec<String> = liveness.in_flight.iter().cloned().collect();
    ids.sort();
    (liveness.seq, ids)
}

/// A canned `tool_execution_start` (the ANNOUNCE frame the pump sees while
/// the Provider still streams the `toolCall`).
fn tool_start(tool_call_id: &str) -> crate::agent::events::RpcEvent {
    crate::agent::events::RpcEvent::tool_execution_start {
        tool_call_id: tool_call_id.to_string(),
        tool_name: "bash".to_string(),
        args: serde_json::json!({ "command": "ls" }),
    }
}

/// A canned `tool_execution_end` (the close of one announced call).
fn tool_end(tool_call_id: &str) -> crate::agent::events::RpcEvent {
    crate::agent::events::RpcEvent::tool_execution_end {
        tool_call_id: tool_call_id.to_string(),
        tool_name: "bash".to_string(),
        result: serde_json::json!({ "content": [], "isError": false }),
        is_error: false,
    }
}

/// (ADR 0055 — configuration) The **Dead bound** production seam:
/// `with_dead_bound_secs` (applied at the ONE `lib.rs` construction site,
/// reading `settings.json`'s `subagentDeadBoundSecs`) sets the same bound
/// the test seam `with_dead_bound` sets — the seconds are converted
/// once, at the boundary, and nowhere else.
#[test]
fn the_dead_bound_secs_seam_sets_the_bound() {
    let (defaulted, _c, _e, _s) = make_manager(None);
    assert_eq!(
        defaulted.dead_bound,
        super::DEFAULT_SUBAGENT_DEAD_BOUND,
        "`new` alone keeps the built-in default"
    );
    let (configured, _c, _e, _s) = make_manager(None);
    let configured = configured.with_dead_bound_secs(45);
    assert_eq!(
        configured.dead_bound,
        Duration::from_secs(45),
        "the setting's SECONDS become the bound verbatim"
    );
}

/// (ADR 0055) The liveness set is "ANNOUNCED and not yet closed": a
/// `tool_execution_start` puts the `tool_call_id` in and the matching
/// `tool_execution_end` takes it out — so an ADR 0055 silence bound can
/// tell a legitimately-silent **Active Subagent** (inside a tool, parked on
/// a **gate**, or awaiting a nested **Subagent session**) from a **Dead
/// Subagent**. The second half is the set-vs-counter guard: ADR 0048 has
/// the cancel / turn-abandoned paths close calls that were announced but
/// never dispatched, so an `end` with NO `start` is a LEGAL frame — on a
/// FRESH drive it must leave the set EMPTY and still count as a frame
/// (`seq` advances). A counter would go negative here and un-block a child
/// that is still working; a set's removal is idempotent.
#[test]
fn the_liveness_set_tracks_announced_and_closed_tool_calls() {
    let (manager, _crashes, _events, _sinks) = make_manager(None);
    seed_drive(&manager, "live1");

    // The announce (the pump's `Event` arm: the boundary bookkeeping runs
    // BEFORE the capture feed).
    let start = tool_start("t1");
    feed_capture(&manager.state, "live1", |capture, liveness| {
        note_liveness_event(liveness, &start);
        capture.observe_event(&start);
        false
    });
    assert_eq!(
        liveness_view(&manager, "live1"),
        (1, vec!["t1".to_string()]),
        "an ANNOUNCED tool call is outstanding"
    );

    // The close.
    let end = tool_end("t1");
    feed_capture(&manager.state, "live1", |capture, liveness| {
        note_liveness_event(liveness, &end);
        capture.observe_event(&end);
        false
    });
    assert_eq!(
        liveness_view(&manager, "live1"),
        (2, Vec::new()),
        "the matching `tool_execution_end` closes it"
    );

    // A DRIVING turn emits plenty of frames that are neither boundary —
    // they advance the liveness clock and leave the set alone.
    let turn = crate::agent::events::RpcEvent::turn_start;
    feed_capture(&manager.state, "live1", |capture, liveness| {
        note_liveness_event(liveness, &turn);
        capture.observe_event(&turn);
        false
    });
    assert_eq!(
        liveness_view(&manager, "live1"),
        (3, Vec::new()),
        "a non-boundary event bumps `seq` and leaves the set alone"
    );

    // A FRESH drive + a lone `tool_execution_end` (ADR 0048: the cancel /
    // turn-abandoned paths close calls that were only ever announced — no
    // `start` ever arrived for `ghost`).
    seed_drive(&manager, "live2");
    let ghost = tool_end("ghost");
    feed_capture(&manager.state, "live2", |capture, liveness| {
        note_liveness_event(liveness, &ghost);
        capture.observe_event(&ghost);
        false
    });
    assert_eq!(
        liveness_view(&manager, "live2"),
        (1, Vec::new()),
        "an `end` without a `start` leaves the set EMPTY (no panic, no \
             underflow) and still counts as a frame"
    );
}

/// (ADR 0055 — a torn-down child closes its own cards) The pump records an
/// unanswered **gate** VERBATIM, and ONLY the answer clears it.
///
/// Two contracts the ADR 0055 review pinned, both about ids the Client can
/// actually match:
/// - `open_gates` holds the `requestId` exactly as the frame carried it,
///   because `finish_subagent` emits it verbatim as a close `requestId` and
///   `removeRequest` / `removePrompt` filter by an EXACT match. The sudo
///   sub-prompts are filed under the DERIVED `"{tool_call_id}:confirm"` /
///   `":password"` ids, so those two cards are TWO records — stripping made
///   both closes unmatchable and the dialogs survived a reap forever.
/// - A `tool_execution_end` is a TOOL close, not an answer: a cancelled gate
///   closes its tool call through the loop's fabricated "permission denied"
///   result while the card stays on screen, so the removal the old code did
///   here turned the leak into a race. `note_gate_answered` is the one
///   clearing point.
#[test]
fn an_unanswered_gate_is_recorded_verbatim_and_only_the_answer_clears_it() {
    let (manager, _crashes, _events, _sinks) = make_manager(None);
    seed_drive(&manager, "gate1");

    // The gate frame (the pump's own `PermissionRequest` /
    // `InteractiveRequest` arms — NOT an `RpcEvent`, so NOT
    // `note_liveness_event`).
    feed_capture(&manager.state, "gate1", |_capture, liveness| {
        note_liveness_gate(liveness, "tc1");
        false
    });
    assert_eq!(
        open_gates_view(&manager, "gate1"),
        vec!["tc1".to_string()],
        "an unanswered gate is outstanding"
    );

    // The tool call closes WITHOUT the user ever answering (the cancel path:
    // the gate resolves `Cancelled` and the loop fabricates the result). The
    // card is still on screen → the record SURVIVES, so teardown closes it.
    let end = tool_end("tc1");
    feed_capture(&manager.state, "gate1", |capture, liveness| {
        note_liveness_event(liveness, &end);
        capture.observe_event(&end);
        false
    });
    assert_eq!(
        open_gates_view(&manager, "gate1"),
        vec!["tc1".to_string()],
        "a tool close is NOT an answer — the unanswered gate stays \
             outstanding so its card gets closed at teardown"
    );
    assert!(
        liveness_view(&manager, "gate1").1.is_empty(),
        "`in_flight` still tracks the TOOL boundary (the silence clock stays \
             disarmed by a fabricated close): {:?}",
        liveness_view(&manager, "gate1")
    );

    // The answer is what clears it.
    manager.note_gate_answered("gate1", "tc1");
    assert_eq!(
        open_gates_view(&manager, "gate1"),
        Vec::<String>::new(),
        "the relayed answer retires the record (teardown must not retract a \
             card the user already answered)"
    );

    // A sudo sub-prompt records the RAW derived id — one record per CARD,
    // not per tool, because the Client filed two cards under two ids.
    feed_capture(&manager.state, "gate1", |_capture, liveness| {
        note_liveness_gate(liveness, "tc2:confirm");
        note_liveness_gate(liveness, "tc2:password");
        false
    });
    assert_eq!(
        open_gates_view(&manager, "gate1"),
        vec!["tc2:confirm".to_string(), "tc2:password".to_string()],
        "the gate ids are recorded EXACTLY as they arrived — `finish_subagent` \
             emits them verbatim as close `requestId`s and the Client's \
             `removeRequest` / `removePrompt` match on an EXACT id, so a stripped \
             id would close nothing"
    );
}

/// (ADR 0055) EVERY frame the pump sees bumps `seq` — including a
/// `SinkFrame`, which is NOT an `RpcEvent`: the child's text stream rides
/// the `session-update` `agent_message_chunk` `SinkFrame`s, so a child that
/// ONLY streams tokens must still register as live or the silence bound of
/// ADR 0055 would reap it mid-answer.
#[test]
fn every_frame_bumps_the_liveness_seq() {
    let (manager, _crashes, _events, _sinks) = make_manager(None);
    seed_drive(&manager, "stream1");
    let chunk = |text: &str| {
        serde_json::json!({ "sessionId": "stream1", "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text },
                "messageId": "m1" } })
    };
    // The pump's `SinkFrame` arm (a `SinkFrame` bumps the liveness clock
    // AND still feeds the capture).
    feed_capture(&manager.state, "stream1", |capture, liveness| {
        note_liveness_frame(liveness);
        capture.observe_sink_frame("session-update", &chunk("a"));
        false
    });
    assert_eq!(
        liveness_view(&manager, "stream1").0,
        1,
        "a `SinkFrame` (NOT an `RpcEvent`) counts as liveness"
    );
    feed_capture(&manager.state, "stream1", |capture, liveness| {
        note_liveness_frame(liveness);
        capture.observe_sink_frame("session-update", &chunk("b"));
        false
    });
    assert_eq!(
        liveness_view(&manager, "stream1").0,
        2,
        "every frame bumps `seq` again"
    );
    assert_eq!(
        manager
            .state
            .drives
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get("stream1")
            .expect("the drive is seeded")
            .capture
            .captured_text(),
        "ab",
        "the SAME call still feeds the capture (the pump's existing \
             contract is untouched)"
    );
}

/// A `"__slow__"` subagent (a 10 s settle) is SILENT with NOTHING in
/// flight, so the **Dead bound** reaps it (the surviving safety net —
/// ADR 0055 pins the *predicate*, not the duration: an unexplained run
/// of silence still ends the dispatch `Failed { reason: Unresponsive,
/// error: "unresponsive" }`, and the subagent is torn down
/// unconditionally).
#[tokio::test]
async fn a_silent_child_with_nothing_in_flight_hits_the_dead_bound() {
    // The **Dead bound** is SHORTER than the fixture's 10 s sleep.
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(1)));
    manager
        .attach("t1", &test_env("t1", None))
        .await
        .expect("the parent attach completes");
    manager
        .handle_for("t1")
        .expect("the parent handle is reachable")
        .send_prompt("__slow_subagent__", &[])
        .expect("the prompt encodes");
    // The `SubagentResult` frame (the **Dead bound** won — the
    // fixture's 10 s settle never arrives in time; the
    // `subagent-result-ack` is the round-trip observation point).
    let got = wait_for_event(&events, "t1", |e| {
            matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
        })
        .await;
    assert!(
            got.iter().any(|(_, e)| {
                matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
            }),
            "the `SubagentResult` frame reached the requester: {got:?}"
        );
    let outcome =
        subagent_result_outcome(&events, "t1").expect("the outcome rides the ack verbatim");
    match outcome {
        SubagentOutcome::Failed { reason, error, .. } => {
            assert_eq!(error, "unresponsive", "the **Dead bound** outcome");
            assert_eq!(
                reason,
                SubagentFailureReason::Unresponsive,
                "the reason is DATA — set from `Race::Dead`, never inferred"
            );
        }
        other => panic!("expected `Failed {{ unresponsive }}`, got {other:?}"),
    }
    // The subagent is torn down (the UNCONDITIONAL teardown — the
    // `drives` entry is removed; the parent's Worker is untouched).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the Dead subagent is torn down"
    );
    assert_eq!(manager.test_session_count(), 1, "the parent survives");
}

/// (ADR 0055 — the reported bug) A child that keeps PRODUCING outlives
/// the bound. The fixture streams 30 `agent_message_chunk` `SinkFrame`s
/// ~100 ms apart (~3.0 s of continuous production — 2× the bound injected
/// here) and only then settles, so a drive that bounds the child's
/// DELEGATION DURATION cuts it off mid-answer. The bound must be a SILENCE
/// bound: every frame rearms it, so a streaming child is an Active
/// Subagent and completes normally.
///
/// RED against the single never-rearmed `sleep(dead_bound)`: the sleep
/// fires at 1500 ms while the child is still streaming, with ~15 of the 30
/// chunks still to come → `Failed { error: "unresponsive" }` and a
/// visibly TRUNCATED `output` (the reaping IS the bug). This scenario has
/// no `tool_call_id` outstanding, so — unlike the in-tool test below — its
/// teeth come from the clock alone, not from the `in_flight` guard.
///
/// What the bound actually measures: the longest run of UNEXPLAINED SILENCE
/// this drive OBSERVES — the inter-frame gap on `liveness_rx`, not the
/// child's own cadence. The test's residual exposure is therefore a
/// >`LIVENESS_DEAD_BOUND` OBSERVATION stall in the parent's read → decode →
/// broadcast → pump chain, NOT the child's speed; 1500 ms is 15× the
/// fixture's cadence and ~2× the observation lag the one recorded CI
/// failure implies.
#[tokio::test]
async fn a_streaming_subagent_outlives_the_dead_bound() {
    // The bound is 1500 ms — 15× the fixture's cadence, half its ~3.0 s stream.
    let (manager, _crashes, events, _sinks) = make_manager(Some(LIVENESS_DEAD_BOUND));
    manager
        .attach("l1", &test_env("l1", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task(&manager, "l1", "__long_stream__");
    // The `SubagentResult` frame (the round-trip observation point — the
    // fixture's `subagent-result-ack` echoes the outcome verbatim).
    let got = wait_for_event(&events, "l1", |e| {
            matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
        })
        .await;
    let outcome = subagent_result_outcome(&events, "l1")
        .unwrap_or_else(|| panic!("the streaming child must resolve the dispatch: {got:?}"));
    match outcome {
        SubagentOutcome::Completed { output, metrics } => {
            assert_eq!(output, "canned answer", "the captured child text");
            // The child genuinely ran PAST the bound — the proof that
            // this is the liveness case, not a race the fixture won.
            assert!(
                metrics.duration_ms >= 2000,
                "the child must have run well past the 1500 ms bound \
                     (its own sleeps are 30 × 100 ms = 3.0 s), got {metrics:?}"
            );
        }
        other => panic!(
            "a streaming child is an Active Subagent and must COMPLETE, got {other:?} \
             (FIRST suspect for a `Failed {{ unresponsive }}` here is OBSERVATION \
             latency, NOT the ADR 0055 fix: the silence clock bounds the inter-frame \
             gap the SUPERVISOR OBSERVES on `liveness_rx` (`subagent_flow.rs`), so a \
             stall longer than this test's {bound} ms bound in the parent's \
             read → decode → broadcast → pump chain reads as child silence while the \
             child keeps producing. Compare the frame count in `output` against the \
             fixture's 30 chunks: a TRUNCATED `output` (the CI failure showed 8 of 15 \
             at 1.9 s) means the child was alive and the parent was starved. The \
             DURATION-instead-of-SILENCE bug this test was written for is only the \
             answer if every frame was observed and the drive still reaped.)",
            bound = LIVENESS_DEAD_BOUND.as_millis(),
        ),
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the settled subagent is torn down"
    );
    assert_eq!(manager.test_session_count(), 1, "the parent survives");
}

/// (ADR 0055) A child INSIDE a tool call is never reaped. The fixture
/// announces a `tool_execution_start`, produces NOTHING for ~3.0 s (the
/// tool is running — the child is silent BY CONSTRUCTION and legitimately
/// blocked), then closes the call and settles. An outstanding
/// `tool_call_id` is precisely the state in which silence means NOTHING,
/// so the bound must not run at all here — not "be longer".
///
/// RED against the pre-ADR 0055 drive, whose SLEEP ARM RAN EVEN WITH a
/// `tool_call_id` outstanding (it had no `in_flight` concept at all): the
/// 1500 ms sleep expires inside the 3.0 s silent tool window →
/// `Failed { error: "unresponsive" }` while `in_flight` holds `t1`, and it
/// does so UNCONDITIONALLY (measured: dropping only the `if !in_flight` guard
/// reddens this test every run). A never-rearmed clock ALONE also reddens it
/// most runs — the guard only DELAYS the reaping, because the frozen deadline
/// has already expired by the time the `tool_execution_end` lifts the guard, so
/// the expired sleep then races the settle arm and usually wins — but the
/// GUARD is this test's own guarantee, so the guard is what the mutation probe
/// removes here. The streaming test is the one whose teeth are the REARM alone.
///
/// The test's load exposure is the COLD START, not the hold: the bound stops
/// being armed only once the pump has OBSERVED the `tool_execution_start`
/// announce, so the one window a stalled parent can lose here is the drive's
/// first deadline vs that single frame.
#[tokio::test]
async fn a_child_inside_a_tool_call_is_never_reaped() {
    // The bound is 1500 ms; the tool call stays open for ~3.0 s.
    let (manager, _crashes, events, _sinks) = make_manager(Some(LIVENESS_DEAD_BOUND));
    manager
        .attach("i1", &test_env("i1", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task(&manager, "i1", "__in_tool__");
    let got = wait_for_event(&events, "i1", |e| {
            matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
        })
        .await;
    let outcome = subagent_result_outcome(&events, "i1")
        .unwrap_or_else(|| panic!("the in-tool child must resolve the dispatch: {got:?}"));
    match outcome {
        SubagentOutcome::Completed { metrics, .. } => {
            assert!(
                metrics.duration_ms >= 2000,
                "the child must have been held by the OPEN tool call well past \
                     the 1500 ms bound, got {metrics:?}"
            );
        }
        other => panic!(
            "a child with an outstanding `tool_call_id` must NEVER be reaped, got \
                 {other:?} (a `Failed {{ unresponsive }}` here is the bug: the bound ran \
                 while the child was blocked inside its tool — first suspect: the \
                 announce frame was not OBSERVED within the bound, see the note on \
                 OBSERVATION latency in a_streaming_subagent_outlives_the_dead_bound)",
        ),
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the settled subagent is torn down"
    );
    assert_eq!(manager.test_session_count(), 1, "the parent survives");
}

/// (ADR 0055 — the salvage) EVERY non-`Settled` drive path carries what
/// the child ACTUALLY produced: the `SubagentCapture`'s partial text, the
/// REAL accumulated usage, and a machine-readable `reason`. Before this,
/// `Failed { error }` held ONLY a string — 29 minutes of findings were
/// thrown away and `finish_subagent` reported `SubagentMetrics::default()`
/// (0 tokens for work that really cost money) on the one channel by which
/// a child's usage reaches the Client.
///
/// Three paths, all at the DRIVE layer (never via `SubagentWait`):
/// - **dead** (`__stream_then_silence__` + a short bound) → `Unresponsive`
/// - **crash** (`__stream_then_crash__`) → `Crashed`
/// - **cancel** (`__stream_then_silence__` + a cancel mid-silence) → `Cancelled`
#[tokio::test]
async fn every_failure_path_carries_the_partial_output_and_real_usage() {
    // ── 1. The Dead bound wins (`Unresponsive`) ────────────────────
    // The bound is well inside the fixture's 10 s silence, and the
    // chunks are NOT inside a tool call, so the silence clock runs.
    let (manager, _crashes, events, sinks) = make_manager(Some(Duration::from_millis(400)));
    let sink_events = register_sink(&sinks, "d1");
    manager
        .attach("d1", &test_env("d1", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task(&manager, "d1", "__stream_then_silence__");
    let outcome = subagent_result_outcome_after_ack(&events, "d1").await;
    match outcome {
        SubagentOutcome::Failed {
            reason,
            error,
            output,
            metrics,
        } => {
            assert_eq!(
                error, "unresponsive",
                "the **Dead bound**'s outcome text (ADR 0055 — the old \
                     timeout wording told the parent nothing about WHICH \
                     predicate fired)"
            );
            assert_eq!(
                reason,
                SubagentFailureReason::Unresponsive,
                "the **Dead bound** firing is DATA, not something to \
                     string-match out of `error`"
            );
            assert_eq!(
                output, "Finding one. Finding two. Finding three.",
                "the **Dead Subagent**'s partial work reaches the parent"
            );
            assert_eq!(
                (metrics.input_tokens, metrics.output_tokens),
                (120, 34),
                "REAL usage — the fixture's `message_update` `usage` frame \
                     (the bug reported 0/0 for work that cost money)"
            );
            assert!(
                metrics.duration_ms >= 1,
                "the wall clock to the failure, got {metrics:?}"
            );
        }
        other => panic!("expected a salvaged `Failed`, got {other:?}"),
    }
    // The `subagent-closed` payload carries the REAL metrics + the
    // `reason` (the sole channel by which a child's usage reaches the
    // Client), but NOT the `output` (the transcript is already persisted).
    let closed = wait_for_closed(&sink_events).await;
    assert_eq!(closed["status"], "failed");
    assert_eq!(closed["reason"], "unresponsive");
    assert_eq!(closed["metrics"]["inputTokens"], 120);
    assert_eq!(closed["metrics"]["outputTokens"], 34);
    assert!(
        closed.get("output").is_none(),
        "the child's transcript does NOT ride a lifecycle event: {closed}"
    );
    assert_eq!(manager.test_drive_count(), 0, "the dead child is torn down");
    drop(sink_events);
    manager.detach("d1");

    // ── 2. The process crashed (`Crashed`) ─────────────────────────
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(30)));
    manager
        .attach("s1", &test_env("s1", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task(&manager, "s1", "__stream_then_crash__");
    let outcome = subagent_result_outcome_after_ack(&events, "s1").await;
    match outcome {
        SubagentOutcome::Failed {
            reason,
            error,
            output,
            metrics,
        } => {
            assert_eq!(
                error, "subagent worker exited unexpectedly (code 137)",
                "the crash still carries the exit code"
            );
            assert_eq!(reason, SubagentFailureReason::Crashed);
            assert_eq!(
                output, "Finding one. Finding two. Finding three.",
                "a crashed child's partial work is not thrown away"
            );
            assert_eq!((metrics.input_tokens, metrics.output_tokens), (120, 34));
        }
        other => panic!("expected a salvaged `Failed` (crash), got {other:?}"),
    }
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the crashed child is torn down"
    );
    manager.detach("s1");

    // ── 3. A cancel mid-silence (`Cancelled`) ──────────────────────
    // A bound far longer than the test: ONLY the cancel may end this one.
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(30)));
    manager
        .attach("k1", &test_env("k1", None))
        .await
        .expect("the parent attach completes");
    // The `sub1` DISPATCH id — the `__cancel__` frame names that id.
    dispatch_child_task_with_id(&manager, "k1", "__stream_then_silence__", "sub1");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while manager.test_drive_count() < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(manager.test_drive_count(), 1, "the child is in flight");
    // Let the partial chunks land in the capture, then cancel (the
    // fixture's `SubagentCancel { id: "sub1" }` frame).
    tokio::time::sleep(Duration::from_millis(300)).await;
    manager
        .handle_for("k1")
        .expect("the parent handle is reachable")
        .send_prompt("__cancel__", &[])
        .expect("the cancel prompt encodes");
    let outcome = subagent_result_outcome_after_ack(&events, "k1").await;
    match outcome {
        SubagentOutcome::Failed {
            reason,
            error,
            output,
            metrics,
        } => {
            assert_eq!(error, "cancelled", "the cancel text is unchanged");
            assert_eq!(reason, SubagentFailureReason::Cancelled);
            assert_eq!(
                output, "Finding one. Finding two. Finding three.",
                "a cancelled child's partial work is not thrown away"
            );
            assert_eq!((metrics.input_tokens, metrics.output_tokens), (120, 34));
        }
        other => panic!("expected a salvaged `Failed` (cancel), got {other:?}"),
    }
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the cancelled child is torn down"
    );
    manager.detach("k1");
}

/// (ADR 0055 review, thread 3 — the crash salvage races the pump) A child
/// that writes a BURST of frames and then dies immediately must still arrive
/// at the parent with EVERY chunk and the REAL usage, because the pump
/// delivers `Exited` through the frame `broadcast` — strictly AFTER every
/// frame that preceded it — while `handle.exited()` is a SEPARATE `watch`
/// that the read task publishes as soon as `child.wait()` returns. Watching
/// the watch is what truncated the salvage: the drive reached
/// `failed_with_salvage` while the pump was still mid-queue, and the `Failed`
/// outcome is FROZEN, so the missing findings could never be repaired.
///
/// The stall makes the race DETERMINISTIC rather than merely likely: the pump
/// parks on the child's FIRST frame, the child finishes writing its burst and
/// exits 137 during the park, so the `exited` watch is already set with the
/// whole burst still queued. Only the pump's own exit signal may end the drive
/// here — the bounded `handle.exited()` fallback is the SECOND arm, and if it
/// won (the pre-fix behaviour) the salvage would be truncated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_childs_salvage_waits_for_the_pump_to_consume_every_frame() {
    // A bound far longer than the test — only the exit may end this dispatch.
    let gate = Arc::new(StallGate::new());
    let (manager, events, sinks) = make_stalling_manager(
        Duration::from_secs(30),
        // The burst's FIRST frame (the child's first `SinkFrame`) is the
        // parked one — everything after it stays in the ring.
        |evt| matches!(evt, WorkerInboundEvent::SinkFrame { .. }),
        gate.clone(),
    );
    let sink_events = register_sink(&sinks, "b1");
    manager
        .attach("b1", &test_env("b1", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task(&manager, "b1", "__crash_burst__");
    let outcome = subagent_result_outcome_after_ack(&events, "b1").await;
    match outcome {
        SubagentOutcome::Failed {
            reason,
            error,
            output,
            metrics,
        } => {
            assert_eq!(reason, SubagentFailureReason::Crashed);
            assert_eq!(error, "subagent worker exited unexpectedly (code 137)");
            // EVERY chunk, in order — the truncation this fix removes loses
            // the tail, and the assertion names how much.
            let mut expected = String::new();
            for i in 0..40 {
                expected.push_str(&format!("chunk{i} "));
            }
            assert_eq!(
                output.trim(),
                expected.trim(),
                "the crashed child's COMPLETE output ({} chars salvaged of {} \
                     expected — a shortfall means the drive read the capture \
                     before the pump had consumed the queued frames)",
                output.trim().chars().count(),
                expected.trim().chars().count()
            );
            assert_eq!(
                (metrics.input_tokens, metrics.output_tokens),
                (777, 909),
                "the usage frame the child wrote LAST is in the salvage (a \
                     too-early read reports 0 for work that really cost tokens)"
            );
        }
        other => panic!("expected a salvaged `Failed` (crash), got {other:?}"),
    }
    assert!(
        gate.fired(),
        "the stall engaged — without it this test forced no ordering at all"
    );
    let closed = wait_for_closed(&sink_events).await;
    assert_eq!(closed["reason"], "crashed");
    assert_eq!(closed["metrics"]["outputTokens"], 909);
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the crashed child is torn down"
    );
    manager.detach("b1");
}

/// A parent's `SubagentCancel` frame (the `IpcDispatcher`'s
/// cancel watcher — a parent turn abort) reaches the DRIVE it names:
/// the child is `send_abort`ed, the outcome is the `Cancelled` one,
/// and the child Worker is REAPED. Pre-fix the pump's `match` had no
/// arm for the frame at all — it was dropped — so the drive raced on
/// to the `dead_bound`: a Stop did not stop the subagent (it kept
/// working after the user asked it to), and its Worker process
/// lingered until the 30-minute bound.
#[tokio::test]
async fn a_parent_subagent_cancel_frame_cancels_and_reaps_the_child() {
    // A bound far longer than the test: ONLY the cancel may end this
    // dispatch (a `Race::Dead` outcome is the bug's signature).
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(30)));
    manager
        .attach("c1", &test_env("c1", None))
        .await
        .expect("the parent attach completes");
    let handle = manager
        .handle_for("c1")
        .expect("the parent handle is reachable");
    // The parent dispatches a SLOW subagent (the fixture's `sub1`
    // dispatch id — the `__cancel__` frame names the same id).
    handle
        .send_prompt("__slow_subagent__", &[])
        .expect("the dispatch prompt encodes");
    // The child is in flight (its `drives` entry has arrived).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while manager.test_drive_count() < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        manager.test_drive_count(),
        1,
        "the subagent dispatch is in flight"
    );
    // The parent aborts the turn (the fixture's `__cancel__` emits the
    // `SubagentCancel { id: "sub1" }` frame).
    handle
        .send_prompt("__cancel__", &[])
        .expect("the cancel prompt encodes");

    let got = wait_for_event(&events, "c1", |e| {
            matches!(e, WorkerInboundEvent::SinkFrame { event, .. } if event == "subagent-result-ack")
        })
        .await;
    let outcome = subagent_result_outcome(&events, "c1")
        .unwrap_or_else(|| panic!("the cancel must resolve the dispatch: {got:?}"));
    match outcome {
        SubagentOutcome::Failed { error, reason, .. } => {
            assert_eq!(
                error, "cancelled",
                "the cancel outcome — `unresponsive` here means the frame was dropped"
            );
            assert_eq!(
                reason,
                SubagentFailureReason::Cancelled,
                "the cancel is DATA — a `Unresponsive` here means the \
                     `SubagentCancel` frame was dropped"
            );
        }
        other => panic!("expected `Failed {{ cancelled }}`, got {other:?}"),
    }
    // The UNCONDITIONAL teardown reaps the child (the `drives` entry
    // is gone — no Worker left behind).
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the cancelled subagent is torn down (no Worker left behind)"
    );
    assert_eq!(manager.test_session_count(), 1, "the parent survives");
}

/// (ADR 0055 — the hole ADR 0040 recorded) A child torn down with an
/// ANSWERED gate leaves a card on screen forever: the child Worker emits NO
/// close frame for a gate it is merely parked on, `router.rs`'s
/// pending-modal cleanup never runs for a child (its `Exited` arm is gated
/// `!is_subagent`, and `cleanup_pending_modals` returns without a `live`
/// entry), and the gate was tracked under the PARENT's delivery id anyway.
/// So `finish_subagent` is the emitter, and the close MUST be keyed to the
/// CHILD id — the Client stored the gate under `payload.sessionId`, which is
/// the child's, so a parent-keyed close is a no-op on the child's slice.
///
/// The teardown here is the CANCEL (a short bound cannot fire: the gate's
/// tool call is outstanding, so the ADR 0055 silence clock is DISARMED).
#[tokio::test]
async fn a_torn_down_child_closes_its_own_outstanding_gates() {
    // A bound far longer than the test — only the cancel may end this.
    let (manager, _crashes, events, sinks) = make_manager(Some(Duration::from_secs(30)));
    let sink_events = register_sink(&sinks, "g1");
    manager
        .attach("g1", &test_env("g1", None))
        .await
        .expect("the parent attach completes");
    // The `sub1` DISPATCH id — the `__cancel__` frame names that id (the
    // cancel is routed by DISPATCH id, so a generated one is unroutable).
    dispatch_child_task_with_id(&manager, "g1", "__permission__", "sub1");
    let child_id = wait_for_child_id(&sink_events).await;

    // The gate is parked (the fixture blocks until it is answered) and the
    // pump has RECORDED it — the router also still sees the frame VERBATIM
    // under the parent's delivery id (the recording is alongside, never
    // instead).
    wait_for_open_gate(&manager, &child_id, "p1").await;
    let delivered = collect_now(&events, "g1");
    let frame = delivered
        .iter()
        .find_map(|(is_sub, e)| match e {
            WorkerInboundEvent::PermissionRequest { id, payload } if *is_sub => {
                Some((id.clone(), payload.clone()))
            }
            _ => None,
        })
        .expect("the router still receives the child's `PermissionRequest`");
    assert_eq!(frame.0, "p1");
    assert_eq!(
        frame.1["sessionId"], child_id,
        "the payload carries the CHILD sessionId (that is the key the \
             Client stored the gate under)"
    );

    // The parent aborts the turn → the child is cancelled and torn down.
    manager
        .handle_for("g1")
        .expect("the parent handle is reachable")
        .send_prompt("__cancel__", &[])
        .expect("the cancel prompt encodes");
    let closed = wait_for_closed(&sink_events).await;
    assert_eq!(closed["status"], "failed");
    assert_eq!(
        closed["reason"], "cancelled",
        "the machine-readable why rides the close payload (the card's \
             short line reads it, never `error`): {closed}"
    );

    let all = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let gates: Vec<Value> = emitted(&sink_events, "interactive-request-close");
    assert_eq!(
        gates.len(),
        1,
        "the child's one unanswered gate is closed exactly once: {all:?}"
    );
    assert_eq!(
        gates[0]["sessionId"], child_id,
        "keyed to the CHILD id — a parent-keyed close is a no-op on the \
             child's slice and the card lives forever"
    );
    assert_eq!(gates[0]["requestId"], "p1");
    // The gate closes precede `subagent-closed` (the teardown drains the
    // gates FIRST). Both are read from one snapshot of the collector.
    let idx = |name: &str| {
        all.iter()
            .position(|(e, _)| e == name)
            .unwrap_or_else(|| panic!("no `{name}` event: {all:?}"))
    };
    assert!(
        idx("interactive-request-close") < idx("subagent-closed"),
        "the gate closes precede the close frame: {all:?}"
    );
    // The `drives` removal runs AFTER the emissions in `finish_subagent`.
    // Poll (rather than sleep a fixed 500 ms like the cancel test above):
    // a child parked on a GATE swallows the reap's `Close` frame in its
    // gate-response read loop, so its reap needs the full 2 s grace +
    // `kill` — a child that is merely slow exits on the first frame.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while manager.test_drive_count() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        manager.test_drive_count(),
        0,
        "the cancelled child is torn down"
    );
    manager.detach("g1");
}

/// (ADR 0055 review, thread a — the RAW id contract, end to end) A child
/// torn down while parked on the sudo sub-prompts closes BOTH cards by their
/// RAW `requestId`s (`"{tool_call_id}:confirm"` / `":password"`), keyed to the
/// CHILD session. The Client stored each card under exactly that derived id
/// (`sudo_run_flow` emits `requestId: "{tc}:confirm"` / `":password"`;
/// `removeRequest` / `removePrompt` in `src/store/interactive.ts` filter on an
/// EXACT `requestId`), so the pre-fix stripping recorded `t2` — a close that
/// matched NOTHING, leaving both sudo dialogs on screen forever after a reap.
#[tokio::test]
async fn a_torn_down_child_closes_the_raw_sudo_card_ids() {
    let (manager, _crashes, _events, sinks) = make_manager(Some(Duration::from_secs(30)));
    let sink_events = register_sink(&sinks, "s1");
    manager
        .attach("s1", &test_env("s1", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task_with_id(&manager, "s1", "__sudo__", "sub1");
    let child_id = wait_for_child_id(&sink_events).await;
    // BOTH raw ids recorded (the pump records the frame `id` verbatim).
    wait_for_open_gate(&manager, &child_id, "t2:confirm").await;
    wait_for_open_gate(&manager, &child_id, "t2:password").await;

    // Teardown: the parent aborts the turn → the child is cancelled.
    manager
        .handle_for("s1")
        .expect("the parent handle is reachable")
        .send_prompt("__cancel__", &[])
        .expect("the cancel prompt encodes");
    let closed = wait_for_closed(&sink_events).await;
    assert_eq!(closed["status"], "failed");

    let gates = emitted(&sink_events, "interactive-request-close");
    let mut ids: Vec<String> = gates
        .iter()
        .map(|p| p["requestId"].as_str().unwrap_or_default().to_string())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["t2:confirm".to_string(), "t2:password".to_string()],
        "BOTH sudo cards are closed by their RAW derived ids — one record per \
             card, since the Client filed two cards under two ids: {gates:?}"
    );
    for payload in &gates {
        assert_eq!(
            payload["sessionId"], child_id,
            "keyed to the CHILD id (where the Client stored the card): {payload}"
        );
    }
    manager.detach("s1");
}

/// (ADR 0055 review, thread b — the ordering hole) A gate whose TOOL closed
/// without an answer is STILL closed at teardown. This test FAILS on the old
/// code, which removed `open_gates` entries on `tool_execution_end`: on Stop
/// the child's permission gate resolves `Cancelled`, the loop FABRICATES a
/// "permission denied" tool result, and that `tool_execution_end` erased the
/// record — so whether the user's dialog leaked was decided by a race between
/// the pump arm and `finish_subagent`'s snapshot. "Tool closed" ≠ "answered".
///
/// The order is FORCED through the pump's own feed (the same
/// `note_liveness_event` the pump runs), and the teardown is driven directly,
/// so no scheduler interleaving can hide the regression.
#[tokio::test]
async fn a_gate_whose_tool_closed_without_an_answer_is_still_closed() {
    let (manager, _crashes, _events, sinks) = make_manager(Some(Duration::from_secs(30)));
    let sink_events = register_sink(&sinks, "cancel-parent");
    seed_drive(&manager, "cancel-child");
    // Record the gate, then let the pump see the FABRICATED tool close for
    // the same id (the cancel path's "permission denied" result).
    feed_capture(&manager.state, "cancel-child", |_c, liveness| {
        note_liveness_gate(liveness, "p9");
        false
    });
    let end = tool_end("p9");
    feed_capture(&manager.state, "cancel-child", |capture, liveness| {
        note_liveness_event(liveness, &end);
        capture.observe_event(&end);
        false
    });

    // Teardown runs THROUGH the real emitter, with the drive still in place.
    let wire = SubagentDispatchWire {
        id: "live-dispatch".to_string(),
        parent_session_id: "cancel-parent".to_string(),
        parent_cwd: "/tmp/space".to_string(),
        parent_enabled_tools: vec!["bash".to_string()],
        agent_name: "tester".to_string(),
        name_resolution: AgentNameResolution::Matched,
        launch: crate::agent::subagent::LaunchConfig::default(),
        task: "the parked task".to_string(),
        model_key: "fake/m1".to_string(),
    };
    finish_subagent(
        &manager.state,
        "cancel-child",
        "cancel-parent",
        &wire,
        SubagentOutcome::Failed {
            reason: SubagentFailureReason::Cancelled,
            error: "cancelled".to_string(),
            output: String::new(),
            metrics: Default::default(),
        },
        &SubagentOutcomeSink::Requester,
    )
    .await;

    let gates = emitted(&sink_events, "interactive-request-close");
    assert_eq!(
        gates.len(),
        1,
        "the unanswered gate survives its own tool close and is closed: {gates:?}"
    );
    assert_eq!(gates[0]["requestId"], "p9");
    assert_eq!(gates[0]["sessionId"], "cancel-child");
}

/// (ADR 0055 review) The ANSWERED half: an answer relayed through the
/// Supervisor retires the record (`note_gate_answered`), so teardown emits NO
/// close for a card the user already answered. The pre-fix contract — "no
/// close because the tool close cleared it" — was UNSOUND: it inferred the
/// user's answer from a tool boundary that a CANCELLED gate also produces, so
/// the same logic silently un-closed unanswered cards. Clearing now happens at
/// the true answer moment; and a duplicate close would in any case be a
/// proven no-op (the Client's listener calls both stores' exact-id `remove`
/// fns, which filter — removing an absent id changes nothing).
#[tokio::test]
async fn an_answered_gates_card_is_not_closed_again_at_teardown() {
    let (manager, _crashes, _events, sinks) = make_manager(Some(Duration::from_secs(30)));
    let sink_events = register_sink(&sinks, "g2");
    manager
        .attach("g2", &test_env("g2", None))
        .await
        .expect("the parent attach completes");
    dispatch_child_task_with_id(&manager, "g2", "__permission__", "sub1");
    let child_id = wait_for_child_id(&sink_events).await;
    wait_for_open_gate(&manager, &child_id, "p1").await;

    // The user ANSWERS (the response routes to the CHILD's handle —
    // `handle_for` resolves the `drives` map's entry).
    manager
        .handle_for(&child_id)
        .expect("the child handle is reachable")
        .send_permission_response(
            "p1",
            &crate::agent::permission::PermissionOutcome::Selected {
                option_id: "allow".to_string(),
            },
        )
        .expect("the permission response encodes");
    // The CHILD-side half of the answer relay — what
    // `SessionManager::respond_permission` does next to the relay above.
    manager.note_gate_answered(&child_id, "p1");

    // The child then closes the tool call and settles → `Completed`.
    let closed = wait_for_closed(&sink_events).await;
    assert_eq!(
        closed["status"], "completed",
        "the answered child settles: {closed}"
    );
    assert!(
        closed.get("reason").is_none(),
        "a `Completed` payload carries NO failure reason: {closed}"
    );
    assert_eq!(
        emitted(&sink_events, "interactive-request-close"),
        Vec::<Value>::new(),
        "the ANSWERED gate is absent at teardown — nothing may retract a \
             card the user already answered"
    );
    manager.detach("g2");
}

/// The `subagent-session-started` + `subagent-closed` UI lifecycle
/// events fire on the PARENT session's sink (the `subagent.rs:635`
/// / `721/743` payload shapes — the `metrics` populated from the
/// subagent's event stream + the wall clock; `subagent-closed` on
/// EVERY exit path).
#[tokio::test]
async fn the_subagent_ui_lifecycle_events_fire_on_the_parent_sink() {
    let (manager, _crashes, _events, sinks) = make_manager(Some(Duration::from_secs(30)));
    let sink_events = register_sink(&sinks, "u1");
    manager
        .attach("u1", &test_env("u1", Some(vec!["bash".to_string()])))
        .await
        .expect("the parent attach completes");
    manager
        .handle_for("u1")
        .expect("the parent handle is reachable")
        .send_prompt("__subagent__", &[])
        .expect("the prompt encodes");
    // Wait for the `subagent-closed` (every exit path emits it).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let got: Vec<(String, Value)> = sink_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if got.iter().any(|(e, _)| e == "subagent-closed")
            || tokio::time::Instant::now() >= deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let got: Vec<(String, Value)> = sink_events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let started = got
        .iter()
        .find(|(e, _)| e == "subagent-session-started")
        .map(|(_, p)| p.clone())
        .expect("the `subagent-session-started` fired on the parent's sink");
    // The `subagent.rs:635` payload shape (`model` / `thinkingLevel`
    // / `enabledTools` / `agentName` / `task` + the session ids).
    assert_eq!(started["parentSessionId"], "u1");
    assert!(started["sessionId"].is_string(), "the child session id");
    assert_eq!(started["agentName"], "tester");
    assert_eq!(
        started["nameResolution"], "matched",
        "the tri-state rides the payload"
    );
    assert_eq!(started["task"], "the task");
    assert_eq!(
        started["model"], "fake/m1",
        "the COMPOSED `provider/id` form"
    );
    assert!(
        started["enabledTools"].is_array(),
        "the child's resolved `enabledTools`"
    );
    let closed = got
        .iter()
        .find(|(e, _)| e == "subagent-closed")
        .map(|(_, p)| p.clone())
        .expect("the `subagent-closed` fired on the parent's sink");
    // The `subagent.rs:721/743` payload shape (`status` / `error`?
    // / `metrics`).
    assert_eq!(closed["status"], "completed");
    assert_eq!(closed["sessionId"], started["sessionId"], "the same child");
    let metrics = &closed["metrics"];
    assert!(metrics["durationMs"].is_u64(), "the wall clock (ms)");
    assert!(
        metrics["durationMs"].as_u64() >= Some(1),
        "the duration is a real elapsed time: {metrics:?}"
    );
    assert!(metrics["output"].is_string(), "the captured final text");
    assert!(metrics["inputTokens"].is_u64());
    assert!(metrics["outputTokens"].is_u64());
    // (issue #107) This pin used to be `metrics["cost"].is_number()`, which
    // pinned the COLLAPSE: a `f64` hand-off could only ever say `0.0`, so
    // "number" was guaranteed. The contract is REPLACED — `null` is now legal
    // and is the ONLY legal spelling of ABSENT. This child
    // (`__subagent__` → `emit_canned_turn`) emits NO `cost_update` frame, so it
    // is the NEVER-PRICED case and arrives as `"cost": null`. `null` is legal
    // and is the ONLY legal spelling of ABSENT — it is NOT the collapse, because
    // a real zero still arrives as the number `0.0` (pinned by
    // `a_real_zero_child_emits_null_metrics_and_a_parent_costs_zero`). What this
    // pin holds is that the key is PRESENT: the wire never omits it
    // (present-and-null, the `SessionInfo.costUsd` spelling).
    assert!(
        metrics.get("cost").is_some(),
        "`cost` is PRESENT on the hand-off — absence is spelled `null`, never a \
         missing key: {metrics:?}"
    );
    assert!(
        metrics["cost"].is_null(),
        "a child whose frames carried NO figure hand-offs `\"cost\": null`, not \
         the collapsed `0.0`: {metrics:?}"
    );
}

/// (issue #107) A REAL-ZERO child, end to end: the `__subagent_cost_0__`
/// fixture emits ONE child-keyed `cost_update` frame carrying `costUsd: 0.0`,
/// so the Supervisor's `SubagentCapture` sums it to `Some(0.0)` — the spelling
/// that is a FIGURE — and BOTH consequences of the `Option` hand-off are
/// observable on real processes:
///
/// 1. the `subagent-closed` hand-off carries the NUMBER `0.0`, not `null` (the
///    never-priced child of the test above proves the other half — the two are
///    NOT interchangeable);
/// 2. a real-zero child CONTRIBUTES a parent-keyed `cost_update` frame: the
///    roll-up gate is the `Option`, not a `> 0.0` value filter, so a real zero
///    still reaches the parent's money (the spec's "`$0.00` for a real zero"
///    claim, pinned Rust-side at the emitter).
///
/// The frame is asserted on the `on_event` observation point (this harness's
/// `on_event` collector — production's `on_event` is `SessionManager::
/// route_event`, which persists the frame and re-emits the SAME payload to the
/// parent's UI sink; the harness has no router, so the sink leg is the
/// production-only half and the `route_event` write is pinned in `router.rs`).
///
/// (On the name: the plan's title for this test, kept verbatim. The "null
/// metrics" half is NOT what it asserts — this child's hand-off is the NUMBER
/// `0.0`; the `null` spelling belongs to the never-priced test above. Read the
/// two together: the pair is what proves the two states are not interchangeable.)
#[tokio::test]
async fn a_real_zero_child_emits_null_metrics_and_a_parent_costs_zero() {
    let (manager, _crashes, events, sinks) = make_manager(Some(Duration::from_secs(30)));
    let sink_events = register_sink(&sinks, "u2");
    manager
        .attach("u2", &test_env("u2", Some(vec!["bash".to_string()])))
        .await
        .expect("the parent attach completes");
    // The CHILD's task is its initial prompt (`subagent_flow.rs`'s
    // `send_prompt(&wire.task, &[])`), so this reaches the fixture's
    // `__subagent_cost_0__` arm and the frame rides the CHILD's stream — the
    // only stream `SubagentCapture` reads.
    dispatch_child_task(&manager, "u2", "__subagent_cost_0__");

    let child_id = wait_for_child_id(&sink_events).await;
    let closed = wait_for_closed(&sink_events).await;

    // 1. The hand-off: a REAL zero, as a JSON NUMBER (never `null`).
    assert_eq!(
        closed["metrics"]["cost"].as_f64(),
        Some(0.0),
        "the child's real `Some(0.0)` reached the capture sum and the wire: {:?}",
        closed["metrics"]
    );
    assert!(
        closed["metrics"]["cost"].is_number(),
        "a real zero is a FIGURE and is never spelled `null`: {:?}",
        closed["metrics"]
    );
    // CHILD-keyed, as every `subagent-closed` is.
    assert_eq!(
        closed["sessionId"], child_id,
        "the hand-off stays keyed to the CHILD"
    );

    // 2. The roll-up: EXACTLY ONE parent-keyed `cost_update` frame. The child's
    // OWN frame rides the same channel (the pump delivers a child's frames to
    // the PARENT's id, flagged `is_subagent: true`, and keyed to the CHILD) —
    // that is the no-double-count half: different store keys. The roll-up is the
    // `is_subagent: false`, PARENT-keyed frame.
    let got = wait_for_event(&events, "u2", |e| {
        matches!(e, WorkerInboundEvent::SinkFrame { event, payload }
            if event == "interactive-event"
                && payload["event"] == "cost_update"
                && payload["sessionId"] == "u2")
    })
    .await;
    let rollups: Vec<&WorkerInboundEvent> = got
        .iter()
        .filter(|(is_sub, e)| {
            !is_sub
                && matches!(e, WorkerInboundEvent::SinkFrame { event, payload }
                    if event == "interactive-event"
                        && payload["event"] == "cost_update"
                        && payload["sessionId"] == "u2")
        })
        .map(|(_, e)| e)
        .collect();
    let child_frames = got
        .iter()
        .filter(|(is_sub, e)| {
            *is_sub
                && matches!(e, WorkerInboundEvent::SinkFrame { event, payload }
                    if event == "interactive-event" && payload["event"] == "cost_update")
        })
        .count();
    assert_eq!(
        rollups.len(),
        1,
        "a real-zero child contributes exactly ONE PARENT-keyed roll-up frame: {got:?}"
    );
    assert_eq!(
        child_frames, 1,
        "the child's OWN frame is the other one (CHILD-keyed, `is_subagent: \
         true`) — the two never share a store key: {got:?}"
    );
    let frame = match rollups[0] {
        WorkerInboundEvent::SinkFrame { payload, .. } => payload.clone(),
        _ => unreachable!("filtered above"),
    };
    assert_eq!(
        frame["sessionId"], "u2",
        "the roll-up is keyed to the PARENT, never the child"
    );
    assert_ne!(
        frame["sessionId"], child_id,
        "the roll-up must NOT re-key the child's money to the child"
    );
    assert_eq!(frame["payload"]["source"], "subagent");
    assert_eq!(
        frame["payload"]["costUsd"].as_f64(),
        Some(0.0),
        "a real zero IS a figure and its frame lands on the parent's money"
    );

    drop(sink_events);
    manager.detach("u2");
}

/// (issue #47) The `subagent-session-started` payload names a resolve MISS, and
/// its key set is pinned EXACTLY.
///
/// Three things this pins in one place, none of which a frontend test can see:
///
/// 1. the `nameResolution` key carries the WIRE spelling (`unmatched` /
///    `no_name`) taken verbatim off the wire — from the REAL emit, not from a
///    serde unit test;
/// 2. the payload's key set is EXACTLY the eight keys — the byte-for-byte
///    payload pin. It lives HERE (at the emitter) rather than in
///    `src/lib/ipc/events.test.ts`, which pins event NAMES and nothing else;
/// 3. the miss is NON-BLOCKING: the drive still reaches `subagent-closed` with
///    `status: "completed"`, which is the acceptance criterion and the only way
///    to prove it without a model.
///
/// Each case runs on its OWN parent session (its own `register_sink` +
/// `attach`) so the two drives' events cannot be confused, and with a DISTINCT
/// DISPATCH id (`subagent-result` / `subagent-cancel` route by DISPATCH id, so
/// a shared id would attribute one drive's outcome to the other). The wire is
/// built inline — a copy of `dispatch_child_task_with_id`'s nine field
/// assignments with `name_resolution` differing per case — because that
/// helper's signature is shared by ten call sites and stays untouched.
#[tokio::test]
async fn the_started_payload_names_a_resolve_miss_and_pins_its_full_key_set() {
    for (parent, dispatch_id, resolution, want) in [
        ("n1", "miss-1", AgentNameResolution::Unmatched, "unmatched"),
        ("n2", "miss-2", AgentNameResolution::NoName, "no_name"),
    ] {
        let (manager, _crashes, _events, sinks) = make_manager(Some(Duration::from_secs(30)));
        let sink_events = register_sink(&sinks, parent);
        manager
            .attach(parent, &test_env(parent, Some(vec!["bash".to_string()])))
            .await
            .expect("the parent attach completes");
        manager.dispatch_subagent(
            parent,
            SubagentDispatchWire {
                id: dispatch_id.to_string(),
                parent_session_id: parent.to_string(),
                parent_cwd: "/tmp/space".to_string(),
                parent_enabled_tools: vec!["bash".to_string(), "read".to_string()],
                agent_name: "tester".to_string(),
                name_resolution: resolution,
                launch: LaunchConfig::default(),
                task: "the task".to_string(),
                model_key: "fake/m1".to_string(),
            },
        );

        // Bounded poll for `subagent-closed` (every exit path emits it) — the
        // same shape as the neighbouring lifecycle test, so a missing frame is
        // a failure and not a hang.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let got: Vec<(String, Value)> = sink_events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if got.iter().any(|(e, _)| e == "subagent-closed")
                || tokio::time::Instant::now() >= deadline
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let got: Vec<(String, Value)> = sink_events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let started = got
            .iter()
            .find(|(e, _)| e == "subagent-session-started")
            .map(|(_, p)| p.clone())
            .expect("the `subagent-session-started` fired on the parent's sink");

        assert_eq!(
            started["nameResolution"], want,
            "the wire spelling rides the payload ({resolution:?})"
        );
        assert_eq!(
            started["agentName"], "tester",
            "the marker is ADDITIVE — the label never disappears"
        );

        // The byte-for-byte payload pin: EXACTLY these eight keys.
        let keys: std::collections::BTreeSet<String> = started
            .as_object()
            .expect("the payload is an object")
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            keys,
            [
                "sessionId",
                "parentSessionId",
                "agentName",
                "nameResolution",
                "task",
                "model",
                "thinkingLevel",
                "enabledTools",
            ]
            .iter()
            .map(|k| (*k).to_string())
            .collect::<std::collections::BTreeSet<String>>(),
            "the `subagent-session-started` key set is exactly eight keys ({want})"
        );

        // NON-BLOCKING: the miss never stops the child.
        let closed = got
            .iter()
            .find(|(e, _)| e == "subagent-closed")
            .map(|(_, p)| p.clone())
            .expect("the `subagent-closed` fired on the parent's sink");
        assert_eq!(
            closed["status"], "completed",
            "a resolve miss never fails the dispatch ({want})"
        );
        assert_eq!(closed["sessionId"], started["sessionId"], "the same child");
        manager.detach(parent);
    }
}

/// The `enabled_tools` wire convention (reviewer-corrected — the
/// round-3 fix): a `StartEnv` with `enabled_tools:
/// Some(vec!["subagent"])` (a `launch.tools` that empties out after
/// the guard minus) → the child Worker's advertised tool set is
/// EMPTY (the `Some(vec![])` = NO tools semantics preserved — NOT
/// re-expanded to all); `enabled_tools: None` → all tools (the
/// settings convention mapped at construction — the `[]` (empty
/// parent list) case expands to ALL tools minus the guard).
#[tokio::test]
async fn the_enabled_tools_wire_convention_is_applied_verbatim() {
    // Case 1: `Some(vec!["subagent"])` — a `launch.tools` that
    // empties out after the guard minus → the child advertises NO
    // tools (NOT re-expanded). The fixture's `__subagent_tools__`
    // prompt emits a `SubagentDispatch` with `launch.tools:
    // Some(["subagent"])` (the `Some`-case trigger).
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(30)));
    manager
        .attach("w1", &test_env("w1", Some(vec!["subagent".to_string()])))
        .await
        .expect("the parent attach completes");
    manager
        .handle_for("w1")
        .expect("the parent handle is reachable")
        .send_prompt("__subagent_tools__", &[])
        .expect("the prompt encodes");
    // The child's advertised tool set (the `Start` → the fixture's
    // `session-update` `enabledTools` frame — the `is_subagent`
    // events).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut child_enabled: Option<Value>;
    loop {
        let got: Vec<(String, bool, WorkerInboundEvent)> =
            events.lock().unwrap_or_else(|p| p.into_inner()).clone();
        child_enabled = got
            .iter()
            .filter(|(id, _, _)| id == "w1")
            .find_map(|(_, is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { event, payload }
                        if event == "session-update" =>
                    {
                        payload
                            .get("update")
                            .and_then(|u| u.get("enabledTools"))
                            .cloned()
                    }
                    _ => None,
                }
            });
        if child_enabled.is_some() || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let child_enabled = child_enabled.expect("the child advertised its tool set");
    assert_eq!(
        child_enabled,
        Value::Array(Vec::new()),
        "a `launch.tools` that empties out yields NO tools — NOT re-expanded: {child_enabled:?}"
    );
    // Case 2: `launch.tools: None` (the `__subagent__` trigger —
    // the inherited-parent branch) → the child advertises the
    // parent's list minus the guard (`subagent` / `list_agents`
    // excluded; the fixture's `parent_enabled_tools` is
    // `["bash", "read"]` — no guard names, so VERBATIM).
    let (manager, _crashes, events, _sinks) = make_manager(Some(Duration::from_secs(30)));
    manager
        .attach("w2", &test_env("w2", None))
        .await
        .expect("the parent attach completes");
    manager
        .handle_for("w2")
        .expect("the parent handle is reachable")
        .send_prompt("__subagent__", &[])
        .expect("the prompt encodes");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut child_enabled: Option<Value>;
    loop {
        let got: Vec<(String, bool, WorkerInboundEvent)> =
            events.lock().unwrap_or_else(|p| p.into_inner()).clone();
        child_enabled = got
            .iter()
            .filter(|(id, _, _)| id == "w2")
            .find_map(|(_, is_sub, e)| {
                if !is_sub {
                    return None;
                }
                match e {
                    WorkerInboundEvent::SinkFrame { event, payload }
                        if event == "session-update" =>
                    {
                        payload
                            .get("update")
                            .and_then(|u| u.get("enabledTools"))
                            .cloned()
                    }
                    _ => None,
                }
            });
        if child_enabled.is_some() || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let child_enabled = child_enabled.expect("the child advertised its tool set");
    let tools = child_enabled
        .as_array()
        .expect("the advertised tool set is an array");
    assert!(
        !tools.is_empty(),
        "the inherited parent list is advertised (minus the guard): {child_enabled:?}"
    );
    assert!(
        !tools.iter().any(|t| t == "subagent"),
        "the guard excludes `subagent`: {child_enabled:?}"
    );
    assert!(
        !tools.iter().any(|t| t == "list_agents"),
        "the guard excludes `list_agents`: {child_enabled:?}"
    );
}

#[tokio::test]
async fn worker_manager_with_hooks_invokes_custom_hooks() {
    use super::WorkerHooks;

    struct CustomHooks {
        events: Arc<StdMutex<Vec<(String, bool, WorkerInboundEvent)>>>,
        crashes: CrashLog,
        registered_cwds: Arc<StdMutex<Vec<(String, String)>>>,
    }
    impl WorkerHooks for CustomHooks {
        fn on_event(&self, session_id: &str, is_subagent: bool, evt: WorkerInboundEvent) {
            self.events.lock().unwrap_or_else(|p| p.into_inner()).push((
                session_id.to_string(),
                is_subagent,
                evt,
            ));
        }
        fn on_crash(&self, session_id: &str, code: Option<i32>, pid: Option<u32>) {
            self.crashes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((session_id.to_string(), code, pid));
        }
        fn sink_for(&self, _parent_session_id: &str) -> Option<Arc<dyn EventSink>> {
            None
        }
        fn register_child_cwd(&self, session_id: &str, cwd: &str) {
            self.registered_cwds
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((session_id.to_string(), cwd.to_string()));
        }
    }

    let hooks = Arc::new(CustomHooks {
        events: Arc::new(StdMutex::new(Vec::new())),
        crashes: Arc::new(StdMutex::new(Vec::new())),
        registered_cwds: Arc::new(StdMutex::new(Vec::new())),
    });
    let factory: Arc<dyn WorkerFactory> = Arc::new(FakeWorkerFactory);
    let manager = WorkerManager::with_hooks(factory, hooks.clone());

    manager.hooks().register_child_cwd("s1", "/tmp/cwd");
    assert_eq!(hooks.registered_cwds.lock().unwrap().len(), 1);
    assert_eq!(
        hooks.registered_cwds.lock().unwrap()[0],
        ("s1".to_string(), "/tmp/cwd".to_string())
    );

    // Spawn a worker that emits events and crashes
    manager
        .attach("s1", &test_env("s1", None))
        .await
        .expect("attach completes");

    manager
        .handle_for("s1")
        .expect("handle is reachable")
        .send_prompt("__crash__", &[])
        .expect("prompt encodes");

    // Bounded-wait until on_crash is called
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let c = hooks
            .crashes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if c.iter()
            .any(|(id, code, _)| id == "s1" && *code == Some(137))
            || tokio::time::Instant::now() >= deadline
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let crashes = hooks
        .crashes
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    assert_eq!(crashes.len(), 1, "on_crash should be called exactly once");
    assert_eq!(crashes[0].0, "s1");
    assert_eq!(crashes[0].1, Some(137));
    assert!(crashes[0].2.is_some(), "crashed pid should be recorded");

    let events = hooks
        .events
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    assert!(
        events.iter().any(|(id, is_sub, evt)| {
            id == "s1" && !is_sub && matches!(evt, WorkerInboundEvent::SinkFrame { .. })
        }),
        "custom hook on_event must receive worker SinkFrame"
    );
    assert!(
        events.iter().any(|(id, is_sub, evt)| {
            id == "s1" && !is_sub && matches!(evt, WorkerInboundEvent::Exited(Some(137)))
        }),
        "custom hook on_event must receive worker Exited event"
    );
}

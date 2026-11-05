//! The `fake_worker` test fixture (ADR 0025 Task 3 — the
//! `fake_mcp_stdio` pattern): a tiny binary that speaks the Worker
//! protocol via the shared `protocol` types — `ready` on start, a
//! canned event + store-frame + `SinkFrame` stream on `start` +
//! `prompt`, a permission round-trip, `close` → exit 0, and a
//! `__crash__` prompt → exit 137.
//!
//! Wire behavior (one JSONL line per frame — the `protocol` types):
//! - startup: a `ready` line (the fixture version — BEFORE `start`).
//! - `start`: stores the `session_id` + `enabled_tools` (the advertised
//!   tool set rides a `session-update` `SinkFrame` — the
//!   `SubagentCapture` / wire-convention test input).
//! - `prompt`: a canned `RpcEvent` stream (`turn_start` →
//!   `message_start` / `message_end` with a canned assistant `Value` →
//!   `turn_end` → `agent_settled`) PLUS canned store frames
//!   (`TranscriptInsert` for a canned user + assistant `ChatMessage`,
//!   `DisplayUpsert` for a canned `agent-text` row) PLUS a canned
//!   `SinkFrame` `session-update` with an `agent_message_chunk` (the
//!   `SubagentCapture` test input — the enveloped shape).
//! - a `prompt` whose text is `"__crash__"`: exits 137 immediately
//!   (NO `agent_settled` — the crash-detection test).
//! - a `prompt` whose text is `"__permission__"`: a
//!   `PermissionRequest` (id `"p1"`, the FULL canned gate payload —
//!   `requestId` + `request: { toolCall: { title }, options }` —
//!   mirroring `permission.rs:135-158`); on a `PermissionResponse`
//!   (ANY `outcome`) → the tool-execution events + `agent_settled`
//!   (the round-trip test).
//! - a `prompt` whose text is `"__sudo__"`: the TWO sudo sub-prompt
//!   `InteractiveRequest` frames with the DERIVED ids (`t2:confirm` /
//!   `t2:password` — the exact `requestId`s `sudo_run_flow` emits), then parks
//!   forever with NO settle and NO answer (the ADR 0055 review's RAW-id input:
//!   a torn-down child must close BOTH cards by those raw ids).
//! - a `prompt` whose text is `"__slow__"`: sleeps 10 s before
//!   settling (the **Dead bound** tests).
//! - a `prompt` whose text is `"__subagent__"` /
//!   `"__slow_subagent__"` / `"__subagent_tools__"`: a canned
//!   `SubagentDispatch` frame (the Supervisor-side dispatch-flow test
//!   input — the `model_key` — `fake/m1` — resolves against the
//!   parent's `StartEnv` catalog; `__slow_subagent__` is the same
//!   frame with the SLOW child task; `__subagent_tools__` is the
//!   frame with `launch.tools: Some(["subagent"])` — the
//!   empties-out-after-the-guard-minus case).
//! - a `prompt` whose text is `"__subagent_cost_0__"`: the CHILD-side
//!   real-zero money turn (issue #107) — ONE `interactive-event`
//!   `cost_update` `SinkFrame` carrying `costUsd: 0.0` (the shape
//!   `emit_cost_update` writes, keyed to the CHILD's own session id),
//!   then `agent_settled`. The e2e reads it as a child's task: the
//!   Supervisor's `SubagentCapture` sums the frame to `Some(0.0)` — a
//!   REAL zero, the other spelling of the `Option` hand-off — and the
//!   test observes both the `subagent-closed` figure and the parent-keyed
//!   roll-up frame.
//! - a `prompt` whose text is `"__long_stream__"`: a LONG `SinkFrame`
//!   `session-update` `agent_message_chunk` stream (the chunks land ~100 ms
//!   apart for ~3.0 s of CONTINUOUS production) and ONLY then the canned
//!   turn + `agent_settled` (the ADR 0055 regression input — a child that
//!   keeps PRODUCING far past a SHORT injected bound must COMPLETE, not be
//!   reaped mid-answer).
//! - a `prompt` whose text is `"__in_tool__"`: a `tool_execution_start`
//!   held OPEN for ~3.0 s before its `tool_execution_end` +
//!   `agent_settled` (the "child inside a tool call, producing NOTHING"
//!   state — legitimately silent, so a duration bound must never cut it).
//! - a `prompt` whose text is `"__stream_then_silence__"`: a SHORT
//!   `agent_message_chunk` stream + a `message_update` `usage` frame (real
//!   capture text AND real tokens), then ~10 s of SILENCE and NO
//!   `agent_settled` — a **Dead Subagent** (the **Dead bound** reaps it, and
//!   a cancel reaches it mid-silence). The salvage input: the drive must
//!   carry the partial text + the real usage instead of discarding them.
//! - a `prompt` whose text is `"__stream_then_crash__"`: the same partial
//!   production, then exit 137 (a **Dead Subagent** by process death — the
//!   crash path's salvage input).
//! - a `prompt` whose text is `"__crash_burst__"`: a BURST of `agent_message_chunk`
//!   `SinkFrame`s + a `message_update` `usage` frame flushed back-to-back, then
//!   exit 137 with NO pause and NO `agent_settled` — the child dies while its
//!   LAST frames are still queued in the pump. The salvage-ordering input (ADR
//!   0055 review, thread 3): the drive must read the `SubagentCapture` only
//!   AFTER the pump consumed every frame the child managed to write.
//! - a `prompt` whose text is `"__cancel__"`: a `SubagentCancel` frame
//!   (the `SubagentCancel` routing test).
//! - a `prompt` whose text is `"__stderr__"`: a line on STDERR and NO
//!   stdout frame (the stderr-drain test — the Supervisor must forward it
//!   to the debug log, then the turn settles).
//! - a `subagent-result` line: a `subagent-result-ack` `SinkFrame`
//!   (the `SubagentResult` round-trip observation point — the
//!   `outcome` rides verbatim).
//! - `close`: exit 0 (the Worker self-exits — the Supervisor does NOT
//!   close the stdin pipe).

use std::io::{BufRead, Write};

use crate::events::RpcEvent;
use crate::harness::store::DisplayRow;
use crate::harness::ModelKey;
use crate::subagent::{AgentNameResolution, LaunchConfig};
use crate::worker::protocol::{
    decode_inbound, encode_outbound, stamp_outbound, Inbound, Outbound, PROTOCOL_VERSION,
};

/// The fixture version (the `ready` frame's `version` field, and the
/// `hello` frame's `clientVersion` — the binary-version analogue).
const VERSION: &str = "fake-worker-0.0.0";

/// The per-session frame cursor (Fulcrum T1) — the fixture must speak the
/// IDENTICAL wire to the real Worker, so it stamps the same flat sibling
/// `frameSeq` the real writers stamp, from the same 1-based count, reset on
/// each `start`. A process `static` (not a local) because `emit` is a free
/// function threaded through a dozen `emit_*` helpers; the fixture is
/// single-threaded, so the counter has no competitor.
static FRAME_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The `__long_stream__` chunk count (ADR 0055) — `LONG_STREAM_CHUNKS` ×
/// `LONG_STREAM_GAP` = 3.0 s of CONTINUOUS production: TWICE the liveness tests'
/// injected 1500 ms bound, so a drive that bounds the delegation's DURATION
/// instead of its OBSERVED silence is still caught mid-stream (issue #34 widened
/// the bound 300 → 1500 ms to buy 15× margin against the cadence below, and the
/// stream had to grow with it or the regression would have stopped being RED).
/// Real sleeps, so this is a floor: `std::thread::sleep` never returns early.
const LONG_STREAM_CHUNKS: u32 = 30;

/// The gap between two `__long_stream__` chunks (~100 ms — a chunk arrives 15×
/// INSIDE the tests' 1500 ms bound, so a self-rearming silence clock cannot
/// expire while the child is talking unless the PARENT stopped observing for
/// longer than the bound (issue #34: that is exactly what a loaded macOS runner
/// does, and it is an observation-stall failure, not a cadence one).
const LONG_STREAM_GAP: std::time::Duration = std::time::Duration::from_millis(100);

/// How long the `__in_tool__` scenario holds its `tool_execution_start`
/// open (3.0 s — TWICE the liveness tests' injected 1500 ms bound, so the
/// pre-ADR 0055 drive this scenario is RED against — whose sleep arm ran even
/// with a `tool_call_id` outstanding — still expires inside the silent window.
/// The bound being LONGER than the old 300 ms is only about the COLD
/// START: once the announce is observed the bound is not armed at all, so the
/// hold itself needs no margin against runner load).
const IN_TOOL_HOLD: std::time::Duration = std::time::Duration::from_millis(3000);

/// How long the `__stream_then_silence__` scenario stays SILENT after its
/// partial production (~10 s — LONGER than any bound the tests inject, so
/// the **Dead bound** is the only thing that can end the dispatch, and a
/// cancel mid-silence is unambiguously the cancel arm).
const SILENCE_AFTER_STREAM: std::time::Duration = std::time::Duration::from_secs(10);

/// The `__crash_burst__` chunk count (ADR 0055 review, thread 3). Enough frames
/// that a salvage read while the pump is still draining the queue is GLARINGLY
/// truncated — every chunk is a distinct word, so a lost tail shows up in the
/// text — while staying far inside the client's 256-frame `broadcast` ring (a
/// `Lagged` pump would be a DIFFERENT defect than the one under test).
const CRASH_BURST_CHUNKS: u32 = 40;

/// The `__crash_burst__` `usage` frame's tokens — deliberately NOT
/// `emit_partial_progress`'s numbers, and deliberately the LAST frame before the
/// exit: a salvage that reads the capture too early reports `0` output tokens
/// for work that really cost money, the understatement the review named.
const CRASH_BURST_USAGE: (u64, u64) = (777, 909);

/// Emit one outbound frame (one JSONL line — the `protocol` encode)
/// STAMPED with the next `frameSeq` (the same helper the real Worker's
/// writers call — the fixture speaks the identical wire).
fn emit<W: Write>(stdout: &mut W, frame: &Outbound) {
    let line = stamp_outbound(&FRAME_SEQ, frame);
    let _ = stdout.write_all(line.as_bytes());
    let _ = stdout.flush();
}

/// Emit one UNSTAMPED outbound frame — the two pre-session handshake
/// frames (`hello`, `ready`), which precede any `start` and therefore
/// carry no `frameSeq` (mirrors the real Worker's `write_frame_unstamped`).
fn emit_unstamped<W: Write>(stdout: &mut W, frame: &Outbound) {
    let line = encode_outbound(frame);
    let _ = stdout.write_all(line.as_bytes());
    let _ = stdout.flush();
}

/// The long `agent_message_chunk` stream (ADR 0055 — the `__long_stream__`
/// scenario's body): `LONG_STREAM_CHUNKS` `SinkFrame` `session-update`
/// frames ~100 ms apart (~3.0 s of CONTINUOUS production). They ride
/// `messageId: "m2"` — a DIFFERENT id from `emit_canned_turn`'s canned `m1` —
/// so the stream is an INTERMEDIATE message in the capture's
/// last-`messageId`-with-non-empty-text rule and never concatenates into the
/// child's final answer (exactly the shape a real tool-using turn has); the
/// `messageId` values are literal because the `SubagentCapture` keys on the
/// frame's id, not on the stamped display row's.
///
/// Ordering invariant (the same one `emit_canned_turn` documents): this is
/// only the child's PRODUCTION phase — `agent_settled` is emitted by
/// `emit_canned_turn`, LAST, after the whole stream, so the `SubagentCapture`
/// is fully fed before the settle signal (no drive-teardown race).
fn emit_long_chunk_stream<W: Write>(stdout: &mut W, session_id: &str) {
    for i in 0..LONG_STREAM_CHUNKS {
        emit(
            stdout,
            &Outbound::SinkFrame {
                event: "session-update".to_string(),
                payload: serde_json::json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "m2",
                        "content": { "type": "text", "text": if i == 0 { "streamed" } else { "…" } }
                    }
                }),
            },
        );
        std::thread::sleep(LONG_STREAM_GAP);
    }
}

/// The PARTIAL production both salvage scenarios emit before they die: a
/// `message_update` `usage` frame (so `SubagentCapture::usage()` reports REAL
/// tokens — the failed path used to report 0 for work that really cost money)
/// followed by three `agent_message_chunk` `SinkFrame`s on one `messageId`
/// (so `SubagentCapture::captured_text()` is non-empty — the salvage the
/// parent must receive).
///
/// The `messageId` is `m9` — neither `emit_canned_turn`'s `m1` nor
/// `emit_long_chunk_stream`'s `m2` — and it is NOT a bookkeeping id, so the
/// capture's last-`messageId`-with-non-empty-text rule picks it up. Neither
/// scenario emits `agent_settled`: a **Dead Subagent** never decided it was
/// finished, and a settle frame here would erase the very failure being tested.
fn emit_partial_progress<W: Write>(stdout: &mut W, session_id: &str) {
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::message_update {
                usage: serde_json::json!({ "inputTokens": 120, "outputTokens": 34 }),
                assistant_message_event: serde_json::json!({
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "partial "
                }),
            },
        },
    );
    for text in ["Finding one. ", "Finding two. ", "Finding three."] {
        emit(
            stdout,
            &Outbound::SinkFrame {
                event: "session-update".to_string(),
                payload: serde_json::json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "m9",
                        "content": { "type": "text", "text": text }
                    }
                }),
            },
        );
    }
}

/// The `__crash_burst__` body (ADR 0055 review, thread 3): `CRASH_BURST_CHUNKS`
/// `agent_message_chunk` `SinkFrame`s on ONE `messageId` plus the closing
/// `message_update` `usage` frame, written back-to-back with NO pauses, then the
/// process exits 137 immediately.
///
/// The shape is deliberate: a child whose Provider is streaming fast does exactly
/// this — many frames land in the client's `broadcast` ring in one go — and the
/// exit is then published by the read task through the SEPARATE `exited` watch
/// while those frames are still queued. The caller exits with NO pause and NO
/// `agent_settled`, so the drive must not be able to reach the capture before the
/// pump has drained everything the child wrote. Every chunk carries a distinct
/// word so a truncated salvage names the lost tail in the assertion.
fn emit_crash_burst<W: Write>(stdout: &mut W, session_id: &str) {
    for i in 0..CRASH_BURST_CHUNKS {
        emit(
            stdout,
            &Outbound::SinkFrame {
                event: "session-update".to_string(),
                payload: serde_json::json!({
                    "sessionId": session_id,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "messageId": "m7",
                        "content": { "type": "text", "text": format!("chunk{i} ") }
                    }
                }),
            },
        );
    }
    // The `usage` frame LAST: it is the token count a too-early salvage loses.
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::message_update {
                usage: serde_json::json!({
                    "inputTokens": CRASH_BURST_USAGE.0,
                    "outputTokens": CRASH_BURST_USAGE.1
                }),
                assistant_message_event: serde_json::json!({
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "burst"
                }),
            },
        },
    );
}

/// The canned `RpcEvent` stream (the Supervisor's bookkeeping input —
/// `turn_start` → `message_start` / `message_end` with a canned
/// assistant `Value` → `turn_end` → `agent_settled`) PLUS the canned
/// store frames (`TranscriptInsert` for a canned user + assistant
/// `ChatMessage`, `DisplayUpsert` for a canned `agent-text` row) PLUS
/// a canned `SinkFrame` `session-update` with an `agent_message_chunk`
/// (the `SubagentCapture` test input — the enveloped shape).
///
/// The emission order mirrors the REAL harness (ADR 0025): the
/// `SinkFrame` `agent_message_chunk`s stream DURING the message
/// (BEFORE `agent_settled` — the `SubagentCapture` is fully fed
/// before the settle signal), and the store frames (the transcript
/// persistence) land with the message completion — ALSO before
/// `agent_settled`.
///
/// `incarnation` is the stamp this Worker's `StartEnv` carried (ADR 0049). It
/// namespaces the `DisplayUpsert` `message_key`, so two Worker lives for one
/// Session write TWO display rows instead of UPSERTing over one key — the
/// shape the two-incarnation e2e assertion reads.
/// (The `SubagentCapture` fixture is a SEPARATE concern: the `SinkFrame`
/// `messageId`s below stay the literal `m1`, because that capture keys on the
/// frame's id, not on the display row's.)
fn emit_canned_turn<W: Write>(stdout: &mut W, session_id: &str, incarnation: &str) {
    // The `RpcEvent` stream (the Supervisor's INTERNAL bookkeeping —
    // settle detection, the `SubagentCapture`'s usage, the debug
    // log).
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::turn_start,
        },
    );
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::message_start {
                message: serde_json::json!({ "role": "assistant" }),
            },
        },
    );
    // The `message_update` usage (the `SubagentCapture`'s token fields
    // — the metrics' token fields).
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::message_update {
                usage: serde_json::json!({ "inputTokens": 10, "outputTokens": 5 }),
                assistant_message_event: serde_json::json!({
                    "type": "text_delta",
                    "contentIndex": 0,
                    "delta": "canned "
                }),
            },
        },
    );
    // The `SinkFrame` `session-update` with the `agent_message_chunk`
    // stream (the `SubagentCapture` test input — the enveloped
    // `{ sessionId, update }` shape — streamed DURING the message, BEFORE
    // `agent_settled`).
    emit(
        stdout,
        &Outbound::SinkFrame {
            event: "session-update".to_string(),
            payload: serde_json::json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "m1",
                    "content": { "type": "text", "text": "canned " }
                }
            }),
        },
    );
    emit(
        stdout,
        &Outbound::SinkFrame {
            event: "session-update".to_string(),
            payload: serde_json::json!({
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": "m1",
                    "content": { "type": "text", "text": "answer" }
                }
            }),
        },
    );
    // The `message_end` with a canned assistant `Value` (the
    // `message_end` shape — carries NO text for tool-call messages,
    // so the `SubagentCapture` does NOT read it).
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::message_end {
                message: serde_json::json!({
                    "role": "assistant",
                    "content": [{ "type": "text", "text": "canned answer" }]
                }),
            },
        },
    );
    // The store frames (the Supervisor's persister input — the
    // `IpcStore`'s `TranscriptInsert` / `DisplayUpsert` frames — the
    // transcript rows land with the message completion, BEFORE
    // `agent_settled`).
    emit(
        stdout,
        &Outbound::TranscriptInsert {
            session_id: session_id.to_string(),
            seq: 1,
            role: "user".to_string(),
            content_json: r#"{"role":"user","content":"canned prompt"}"#.to_string(),
        },
    );
    emit(
        stdout,
        &Outbound::TranscriptInsert {
            session_id: session_id.to_string(),
            seq: 2,
            role: "assistant".to_string(),
            content_json: r#"{"role":"assistant","content":"canned answer"}"#.to_string(),
        },
    );
    emit(
        stdout,
        &Outbound::DisplayUpsert {
            session_id: session_id.to_string(),
            rows: vec![DisplayRow {
                kind: "agent-text".to_string(),
                message_key: format!("{incarnation}:m1"),
                payload_json: serde_json::json!({ "text": "canned answer" }),
                created_at: 1,
            }],
        },
    );
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::turn_end {
                message: serde_json::json!({ "role": "assistant" }),
                tool_results: Vec::new(),
            },
        },
    );
    // The settle (LAST — the `SubagentCapture` is fully fed by now:
    // the `agent_message_chunk` stream + the usage are all delivered
    // before this frame, so the drive task's settle→teardown reads a
    // complete capture — no race).
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::agent_settled,
        },
    );
}

/// The tool-execution events + `agent_settled` (the
/// `__permission__` round-trip's post-response stream — the
/// tool-execution events arrive AFTER the `PermissionResponse`).
///
/// The `tool_call_id` is the GATE's `requestId` (`p1`), NOT an independent
/// `t1`: in production `gate_tool` passes `&tc.id` to `native_permission_gate`,
/// so a **gate**'s id and the `tool_call_id` of the tool it is parked inside are
/// the SAME string. The Supervisor's ADR 0055 teardown depends on that one id
/// space — `tool_execution_end` removes the gate from the outstanding-gate set,
/// which is how an ANSWERED gate is absent at teardown — so a fixture that
/// invented a second id would model a Worker that cannot exist.
fn emit_tool_execution_and_settle<W: Write>(stdout: &mut W, session_id: &str) {
    let _ = session_id;
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::tool_execution_start {
                tool_call_id: "p1".to_string(),
                tool_name: "bash".to_string(),
                args: serde_json::json!({ "command": "ls" }),
            },
        },
    );
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::tool_execution_end {
                tool_call_id: "p1".to_string(),
                tool_name: "bash".to_string(),
                result: serde_json::json!({
                    "content": [{ "type": "text", "text": "ok" }],
                    "details": { "exitCode": 0 },
                    "isError": false
                }),
                is_error: false,
            },
        },
    );
    emit(
        stdout,
        &Outbound::Event {
            event: RpcEvent::agent_settled,
        },
    );
}

/// The canned `SubagentDispatch` frame (the Supervisor-side
/// dispatch-flow test input — the `model_key` — `fake/m1` — resolves
/// against the parent's `StartEnv` catalog; the `task` is the
/// subagent's initial prompt — `"__slow__"` for the **Dead bound**
/// tests, `"__subagent_tools__"`'s `launch.tools` is
/// `Some(["subagent"])` — the empties-out-after-the-guard-minus
/// case).
fn emit_subagent_dispatch<W: Write>(
    stdout: &mut W,
    session_id: &str,
    task: &str,
    tools: Option<Vec<String>>,
    model_key: &str,
) {
    let parent_enabled_tools = if tools.is_some() {
        vec!["subagent".to_string()]
    } else {
        vec!["bash".to_string(), "read".to_string()]
    };
    emit(
        stdout,
        &Outbound::SubagentDispatch {
            id: "sub1".to_string(),
            parent_session_id: session_id.to_string(),
            parent_cwd: "/tmp/space".to_string(),
            parent_enabled_tools,
            agent_name: "tester".to_string(),
            // (issue #47) The fixture NAMES an agent, so `Matched` is the
            // truthful canned value; a manager-level test that wants another
            // leg of the tri-state builds its own wire.
            name_resolution: AgentNameResolution::Matched,
            launch: LaunchConfig {
                system_prompt: Some("be brief".to_string()),
                model: None,
                thinking: None,
                frontmatter_thinking: None,
                tools,
                permission: None,
            },
            task: task.to_string(),
            model_key: model_key.to_string(),
        },
    );
}

/// The fixture entry point (was this file's `fn main()` — moved VERBATIM from
/// `src-tauri/src/bin/fake_worker.rs` in S8b.1 so ONE body backs both the
/// desktop's `fake_worker` bin and the crate's `fake_worker_core` bin; the two
/// bins are 3-line shims over this function).
pub fn worker_main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    // The handshake, in order: `hello` (the protocol version) then
    // `ready` (the fixture version) — BOTH unstamped, exactly like the
    // real Worker, so the fixture speaks the identical wire.
    emit_unstamped(
        &mut stdout,
        &Outbound::Hello {
            protocol_version: PROTOCOL_VERSION,
            client_version: VERSION.to_string(),
        },
    );
    emit_unstamped(
        &mut stdout,
        &Outbound::Ready {
            version: VERSION.to_string(),
            session_id: None,
        },
    );

    let mut reader = stdin.lock();
    let mut line = String::new();
    let mut session_id: Option<String> = None;
    // The parent's `model` key (the `Start` env — the `SubagentDispatch`
    // frame's `model_key` — the Supervisor resolves it against the
    // parent's `StartEnv` catalog; the fixture's hardcoded `fake/m1`
    // would be unknown when the parent's model is a different provider
    // (e.g. the `ipc` test's seeded `tama/m1`)).
    let mut model_key: Option<String> = None;
    // The stamp this Worker's `StartEnv` carried (ADR 0049) — the fixture
    // namespaces its canned display key with it, exactly as the real harness
    // namespaces every id it mints.
    let mut incarnation = String::new();

    while let Ok(n) = reader.read_line(&mut line) {
        if n == 0 {
            break; // EOF — the Supervisor is gone.
        }
        let l = line.trim_end_matches(['\n', '\r']).to_string();
        line.clear();
        if l.is_empty() {
            continue;
        }
        let Ok(msg) = decode_inbound(&l) else {
            // Permissive BY DESIGN (the fixture-side twin of production's LOUD
            // decode — `worker/mod.rs` at least `eprintln!`s the bad line), so an
            // unreadable `start` vanishes here and surfaces only as a failed
            // assertion in the test that spawned this Worker (issue #34/T10
            // handoff: recorded, the fixture's behaviour deliberately unchanged).
            continue; // a malformed line — the fixture is permissive.
        };
        let sid = session_id.clone().unwrap_or_default();
        match msg {
            Inbound::Start(env) => {
                // A new session: the cursor restarts, so the first canned
                // frame after `start` is `frameSeq: 1` (the per-session
                // guarantee the real Worker gives).
                FRAME_SEQ.store(0, std::sync::atomic::Ordering::SeqCst);
                session_id = Some(env.session_id.clone());
                model_key = Some(ModelKey::from(&env.model).to_string());
                incarnation = env.incarnation.clone();
                // The advertised tool set (the `SubagentCapture` /
                // wire-convention test input — a `session-update`
                // `SinkFrame` with the `enabledTools` field — the
                // harness convention verbatim: `None` = all,
                // `Some(v)` = exactly `v`, `Some([])` = NO tools).
                // The `systemPrompt` + `trusted` fields ride the envelope
                // (the ADR 0025 Task 5 test input — the subagent's
                // `StartEnv` envelope assertion: the `systemPrompt` is
                // the `build_child_system_message` output, the `trusted`
                // is the parent's trust flag (the inheritance — ADR
                // 0010)). `systemPrompt` is ALWAYS present (a `null`
                // when the env's field is `None` — the absence-of-a-
                // system-message assertion keys on the `null`).
                let update = serde_json::json!({
                    "sessionUpdate": "session_info",
                    "enabledTools": env.enabled_tools,
                    "systemPrompt": serde_json::to_value(&env.system_prompt).unwrap(),
                    "trusted": env.trusted,
                    // The stamp the Worker mints its ids with (ADR 0049) — the
                    // two-incarnation assertion: the e2e resume test reads the
                    // echo to prove two Workers for one Session get
                    // DIFFERENT stamps.
                    "incarnation": env.incarnation
                });
                emit(
                    &mut stdout,
                    &Outbound::SinkFrame {
                        event: "session-update".to_string(),
                        payload: serde_json::json!({
                            "sessionId": env.session_id,
                            "update": update
                        }),
                    },
                );
            }
            Inbound::Prompt { text, .. } => match text.as_str() {
                // The crash (NO `agent_settled` — the
                // crash-detection test; the process exits 137 —
                // the SIGKILL convention).
                "__crash__" => std::process::exit(137),
                // The permission round-trip: the `PermissionRequest`
                // (id `"p1"`, the FULL canned gate payload —
                // `requestId` + `request: { toolCall: { title },
                // options }` — mirroring `permission.rs:135-158`);
                // on a `PermissionResponse` (ANY `outcome`) → the
                // tool-execution events + `agent_settled`.
                "__permission__" => {
                    emit(
                        &mut stdout,
                        &Outbound::PermissionRequest {
                            id: "p1".to_string(),
                            payload: serde_json::json!({
                                "sessionId": sid,
                                "requestId": "p1",
                                "request": {
                                    "sessionId": sid,
                                    "toolCall": { "title": "Allow bash?" },
                                    "options": [
                                        { "optionId": "allow", "name": "Allow", "kind": "allow" },
                                        { "optionId": "reject", "name": "Block", "kind": "reject" },
                                        { "optionId": "trust-space", "name": "Don't ask again for this Space", "kind": "allow" }
                                    ]
                                }
                            }),
                        },
                    );
                    // Wait for the `PermissionResponse` (ANY
                    // `outcome` — the fixture is permissive; a
                    // different frame while waiting is ignored).
                    loop {
                        let Ok(n) = reader.read_line(&mut line) else {
                            std::process::exit(1); // the Supervisor is gone.
                        };
                        if n == 0 {
                            std::process::exit(1); // EOF — the Supervisor is gone.
                        }
                        let l2 = line.trim_end_matches(['\n', '\r']).to_string();
                        line.clear();
                        if l2.is_empty() {
                            continue;
                        }
                        let Ok(m2) = decode_inbound(&l2) else {
                            continue;
                        };
                        if matches!(m2, Inbound::PermissionResponse { .. }) {
                            break;
                        }
                    }
                    // The tool-execution events + `agent_settled`
                    // (the round-trip completes — the
                    // `PermissionOutcome` was delivered verbatim).
                    emit_tool_execution_and_settle(&mut stdout, &sid);
                }
                // The SUDO sub-prompt pair (ADR 0055 review — the RAW id
                // contract): TWO `InteractiveRequest` frames carrying the
                // DERIVED `"{tool_call_id}:confirm"` / `":password"`
                // `requestId`s exactly as `sudo_run_flow` emits them, then
                // park forever (NO settle, NO answer). The Supervisor must
                // close BOTH cards by those RAW ids at teardown — the Client
                // filed them under exactly those ids.
                "__sudo__" => {
                    for phase in ["confirm", "password"] {
                        let request_id = format!("t2:{phase}");
                        emit(
                            &mut stdout,
                            &Outbound::InteractiveRequest {
                                id: request_id.clone(),
                                payload: serde_json::json!({
                                    "sessionId": sid,
                                    "requestId": request_id,
                                    "method": phase,
                                    "source": "bash",
                                    "toolCallId": serde_json::Value::Null,
                                    "params": { "command": "apt-get update", "reason": "install" }
                                }),
                            },
                        );
                    }
                    // Park UNANSWERED — the read swallows every frame, the
                    // reap's `Close` included, so the reap needs its full
                    // grace + `kill` (the same shape the `__permission__`
                    // park has, minus the answer).
                    loop {
                        let Ok(n) = reader.read_line(&mut line) else {
                            std::process::exit(1); // the Supervisor is gone.
                        };
                        if n == 0 {
                            std::process::exit(1); // EOF — the Supervisor is gone.
                        }
                        line.clear();
                    }
                }
                // The slow settle (the **Dead bound** tests — a 10 s
                // sleep before settling; the **Dead bound** is
                // shorter, so the child is reaped as Dead).
                "__slow__" => {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                    emit_canned_turn(&mut stdout, &sid, &incarnation);
                }
                // The LONG stream (ADR 0055 — the
                // `a_streaming_subagent_outlives_the_dead_bound` regression
                // input): ~3.0 s of CONTINUOUS `agent_message_chunk`
                // production — 2× the tests' injected 1500 ms bound, so the
                // arm-once drive is still caught mid-stream (the rearming
                // drive would have to lose fifteen consecutive frames to reap
                // it) — and ONLY THEN the canned turn (whose `agent_settled`
                // stays
                // LAST, after the whole stream, so the `SubagentCapture` is
                // fully fed before the settle). A child that keeps
                // PRODUCING must COMPLETE, not be reaped mid-answer.
                "__long_stream__" => {
                    emit_long_chunk_stream(&mut stdout, &sid);
                    emit_canned_turn(&mut stdout, &sid, &incarnation);
                }
                // A tool call held OPEN (ADR 0055 — the
                // `a_child_inside_a_tool_call_is_never_reaped` regression
                // input): the `tool_execution_start` ANNOUNCE, then
                // NOTHING for ~3.0 s (the tool runs — the child is silent
                // BY CONSTRUCTION and legitimately blocked), then the
                // matching `tool_execution_end` + `agent_settled`. The
                // frames mirror `emit_tool_execution_and_settle`'s shapes
                // (the same `t1` / `bash`), split around the hold.
                "__in_tool__" => {
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::tool_execution_start {
                                tool_call_id: "t1".to_string(),
                                tool_name: "bash".to_string(),
                                args: serde_json::json!({ "command": "sleep 3" }),
                            },
                        },
                    );
                    std::thread::sleep(IN_TOOL_HOLD);
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::tool_execution_end {
                                tool_call_id: "t1".to_string(),
                                tool_name: "bash".to_string(),
                                result: serde_json::json!({
                                    "content": [{ "type": "text", "text": "ok" }],
                                    "details": { "exitCode": 0 },
                                    "isError": false
                                }),
                                is_error: false,
                            },
                        },
                    );
                    // The settle (LAST — the invariant: NOTHING the
                    // `SubagentCapture` reads arrives after it).
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The salvage input for the **Dead bound** / cancel paths: a
                // little REAL production (capture text + a `usage` frame),
                // then ~10 s of SILENCE and NO `agent_settled`. With a short
                // injected bound the drive arms its silence clock (the chunks
                // are not inside a tool call, so nothing blocks it) and reaps
                // the child — the capture must be non-empty at that moment.
                // The same scenario also serves the CANCEL case (cancel it
                // mid-silence).
                "__stream_then_silence__" => {
                    emit_partial_progress(&mut stdout, &sid);
                    std::thread::sleep(SILENCE_AFTER_STREAM);
                    // Never reached in the tests (the bound / the cancel wins
                    // long before); the settle keeps the fixture well-formed
                    // if the sleep ever does complete.
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The salvage input for the CRASH path: the same partial
                // production, then exit 137 (NO `agent_settled` — the process
                // death is the failure).
                "__stream_then_crash__" => {
                    emit_partial_progress(&mut stdout, &sid);
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    std::process::exit(137);
                }
                // The salvage-ORDERING input (ADR 0055 review, thread 3): a
                // BURST of frames the child wrote before it died, then exit 137
                // with NO pause (NO `agent_settled`). The child is gone while
                // its last frames are still queued in the pump, so a drive that
                // watches the process exit instead of the pump's `Exited` frame
                // salvages a truncated capture.
                "__crash_burst__" => {
                    emit_crash_burst(&mut stdout, &sid);
                    std::process::exit(137);
                }
                // The `interactive-event` `SinkFrame` (the `todos_update`
                // push — the frontend's `useInteractive.applyTodoUpdate`
                // consumes the payload VERBATIM) + `agent_settled` (the
                // `headless` IPC test's `__interactive__` input — the
                // `SinkFrame` re-emit contract observation point).
                "__interactive__" => {
                    emit(
                        &mut stdout,
                        &Outbound::SinkFrame {
                            event: "interactive-event".to_string(),
                            payload: serde_json::json!({
                                "sessionId": sid,
                                "seq": 0,
                                "event": "todos_update",
                                "payload": {
                                    "source": "main",
                                    "todos": [
                                        { "content": "do the thing", "status": "pending" }
                                    ]
                                }
                            }),
                        },
                    );
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The Supervisor-side dispatch-flow test input (a
                // canned `SubagentDispatch` frame — the `model_key`
                // — `fake/m1` — resolves against the parent's
                // `StartEnv` catalog).
                "__subagent__" => {
                    emit_subagent_dispatch(
                        &mut stdout,
                        &sid,
                        "the task",
                        None,
                        &model_key.clone().unwrap_or_else(|| "fake/m1".to_string()),
                    );
                    // The parent's turn settles AFTER the dispatch (the
                    // fixture's simplified model — the `send_prompt`
                    // helper waits for the settle; the `SubagentResult`
                    // arrives later and is acked on receipt).
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // (issue #107) The CHILD-side real-zero money turn. The e2e
                // dispatches this as the CHILD's task, so `sid` is the CHILD's
                // session id and the frame rides the child's own stream — the
                // only stream the Supervisor's `SubagentCapture` reads
                // (`pump.rs`'s `SinkFrame`-when-`is_subagent` arm →
                // `observe_sink_frame`). The envelope is `emit_cost_update`'s
                // shape verbatim (`{ sessionId, seq, event: "cost_update",
                // payload: { costUsd, source } }`) and the figure is a REAL
                // zero: the capture's `>= 0.0` filter keeps it, so the hand-off
                // is `Some(0.0)` — the spelling that is a FIGURE, as against the
                // never-priced child's `null`. BEFORE `agent_settled` (the
                // invariant: nothing the capture reads arrives after it).
                "__subagent_cost_0__" => {
                    emit(
                        &mut stdout,
                        &Outbound::SinkFrame {
                            event: "interactive-event".to_string(),
                            payload: serde_json::json!({
                                "sessionId": sid,
                                "seq": 0,
                                "event": "cost_update",
                                "payload": { "costUsd": 0.0, "source": "main" },
                            }),
                        },
                    );
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The **Dead bound** test input (the same frame with
                // the SLOW child task — the subagent's initial
                // prompt is `"__slow__"` → the 10 s settle).
                "__slow_subagent__" => {
                    emit_subagent_dispatch(
                        &mut stdout,
                        &sid,
                        "__slow__",
                        None,
                        &model_key.clone().unwrap_or_else(|| "fake/m1".to_string()),
                    );
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The wire-convention test input (the frame with
                // `launch.tools: Some(["subagent"])` — the
                // empties-out-after-the-guard-minus case; the
                // `parent_enabled_tools` is the `Some` case's parent
                // list).
                "__subagent_tools__" => {
                    emit_subagent_dispatch(
                        &mut stdout,
                        &sid,
                        "the task",
                        Some(vec!["subagent".to_string()]),
                        &model_key.clone().unwrap_or_else(|| "fake/m1".to_string()),
                    );
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The `SubagentCancel` routing test input (the loop's
                // `SubagentCancel` → the `IpcDispatcher`'s
                // `SubagentCancel` frame).
                "__cancel__" => {
                    emit(
                        &mut stdout,
                        &Outbound::SubagentCancel {
                            id: "sub1".to_string(),
                        },
                    );
                }
                // The stderr-drain test input: a diagnostic on STDERR
                // (what a real Worker does for `bad inbound line`), then a
                // quiet settle — the ONLY observation is the log line.
                "__stderr__" => {
                    eprintln!("fake worker: a diagnostic on stderr");
                    emit(
                        &mut stdout,
                        &Outbound::Event {
                            event: RpcEvent::agent_settled,
                        },
                    );
                }
                // The default prompt (the canned turn — the full
                // event + store-frame + `SinkFrame` stream).
                _ => {
                    emit_canned_turn(&mut stdout, &sid, &incarnation);
                }
            },
            // The `SubagentResult` (the Supervisor's delivery — the
            // `IpcDispatcher` waiter resolves; the fixture's
            // `subagent-result-ack` `SinkFrame` is the round-trip
            // observation point — the `outcome` rides verbatim).
            Inbound::SubagentResult { id, outcome } => {
                emit(
                    &mut stdout,
                    &Outbound::SinkFrame {
                        event: "subagent-result-ack".to_string(),
                        payload: serde_json::json!({
                            "id": id,
                            "outcome": serde_json::to_value(&outcome).unwrap_or_default()
                        }),
                    },
                );
            }
            // The `close` — the Worker self-exits 0 (the Supervisor
            // does NOT close the stdin pipe on `close`).
            Inbound::Close => std::process::exit(0),
            // The `config` — echo the frame (a `config-ack` `SinkFrame`
            // — the `set_space_trusted` mid-session trust-toggle test's
            // delivery observation point: the `trusted` flag rides
            // verbatim; the `model` / `thinking` fields ride when set).
            Inbound::Config {
                model,
                thinking,
                clear_thinking,
                trusted,
            } => {
                let mut payload = serde_json::json!({
                    "sessionId": sid,
                    "trusted": trusted
                });
                if let Some(m) = model {
                    payload["model"] = serde_json::to_value(&m).unwrap_or_default();
                }
                if let Some(t) = thinking {
                    payload["thinking"] = serde_json::Value::String(t);
                }
                if clear_thinking {
                    payload["clearThinking"] = serde_json::Value::Bool(true);
                }
                emit(
                    &mut stdout,
                    &Outbound::SinkFrame {
                        event: "config-ack".to_string(),
                        payload,
                    },
                );
            }
            // The `/compact` command (ADR 0044): WITHOUT this arm the
            // `_ => {}` catch-all swallows it, so the IPC-parity fixture would
            // silently ignore a frame the real Worker forwards to its loop.
            // STDERR is the fixture's observation point for what arrived
            // (stdout is the protocol stream — a non-frame line there would
            // corrupt it; the Supervisor drains stderr to the debug log).
            Inbound::Compact { instructions } => {
                eprintln!("fake worker: compact requested (instructions: {instructions:?})");
            }
            // (Fulcrum T3) The live-reload push. Echoed like the `Config` arm
            // above, because the fixture's job is to prove the frame is
            // ENCODED and DECODED compatibly across the pipe: a Supervisor that
            // sends a `config-update` the fixture cannot decode would silently
            // no-op, and the parity test would pass while the real Worker and
            // the fixture disagreed about the wire.
            Inbound::ConfigUpdate {
                file_policy,
                web,
                agent_overrides,
            } => {
                let mut payload = serde_json::json!({ "sessionId": sid });
                if let Some(p) = file_policy {
                    payload["filePolicy"] = serde_json::to_value(&p).unwrap_or_default();
                }
                if let Some(w) = web {
                    payload["web"] = serde_json::to_value(&w).unwrap_or_default();
                }
                if let Some(o) = agent_overrides {
                    payload["agentOverrides"] = serde_json::to_value(&o).unwrap_or_default();
                }
                emit(
                    &mut stdout,
                    &Outbound::SinkFrame {
                        event: "config-update-ack".to_string(),
                        payload,
                    },
                );
            }
            // `abort` / a `permission-response` to an unknown id / an
            // `interactive-response` / an unknown `type`: no response
            // (the fixture is permissive).
            _ => {}
        }
    }
    // stdin EOF — the Supervisor is gone (exit 0 — the clean-EOF
    // path).
}

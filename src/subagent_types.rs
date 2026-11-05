//! The subagent dispatch vocabulary: what a dispatch CARRIES and what it
//! ENDS AS (T5/S4, plan Step E.1 — the wire-type move, not a seam).
//!
//! ## Why these four move and `subagent.rs` does not
//!
//! `SubagentDispatchWire` (E3, `subagent.rs:29`) names `AgentNameResolution` and
//! `LaunchConfig` in its fields, and `Dispatcher::dispatch_subagent_to` takes a
//! `oneshot::Sender<SubagentOutcome>` — so the `Dispatcher` trait (E2) cannot be
//! DECLARED without `SubagentOutcome` being a crate type. But `subagent.rs`
//! itself cannot move until S5 (it holds the `WorkerManager` edge that E2 exists
//! to sever). The resolution is the same one S3 used for `LaunchConfig`, which
//! the status file records as "the one genuine ordering constraint" of that
//! sub-step: the pure DATA moves ahead of the behaviour that owns it, and the
//! desktop file re-exports it so every existing path keeps resolving.
//!
//! `LaunchConfig` already lives in `crate::launch_config` from S3; these four are
//! its siblings and stay beside it in spirit, but they are the OUTCOME half of
//! the vocabulary rather than the launch half, so they get their own module.
//!
//! All four are pure serde data, verified before moving: no `WorkerManager`, no
//! `EventSink`, no `Path`, no `crate::agent::` reference of any kind in their
//! bodies. `SubagentCancel` — the fifth type in that region of `subagent.rs` —
//! deliberately DOES NOT move: it holds `Arc<WorkerManager>` in its `Worker`
//! variant, which is precisely the edge S5 severs.
//!
//! The desktop's `agent/subagent.rs` re-exports all four, so
//! `crate::agent::subagent::{AgentNameResolution, SubagentOutcome, …}`,
//! `crate::agent::{…}` (via `agent/mod.rs`), and
//! `archimedes_lib::agent::subagent::{…}` all keep resolving with zero edits at
//! the ~40 existing call sites, including the two-sided `worker_e2e` fixtures.

use serde::{Deserialize, Serialize};

// ── `AgentNameResolution` ─────────────────────────────────────────────────────

/// (issue #47) Whether a `subagent` call's `agentName` resolved against the
/// discovered Agent definitions — the REPORT that `resolve_launch`'s
/// never-an-error contract left unsaid.
///
/// The three states are NOT two. `resolve_launch` returns the SAME verbatim
/// `LaunchConfig` for `NoName` and `Unmatched` (ADR 0020: "an UNKNOWN or
/// omitted `agentName` is still a no-op for behavior — label-only, config-less,
/// never an error"), and the child's config is identical in both, so the
/// distinction is invisible downstream unless the resolver says it. It matters
/// because the two cases are opposite stories for whoever is debugging:
/// `NoName` is the designed config-less dispatch, `Unmatched` is a typo in a
/// correctly-spelled key that leaves a child running the PARENT's model, the
/// PARENT's tools and no persona while the UI prints a confident agent label.
/// A `bool` would collapse them and force the UI to flag the deliberate case.
///
/// The wire spelling is the contract (`#[serde(rename_all = "snake_case")]` →
/// `no_name` / `matched` / `unmatched`), pinned by
/// `protocol::tests::the_agent_name_resolution_serialises_in_snake_case`; the
/// frontend mirrors it as `AgentNameResolution` in `src/lib/tauri/types.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentNameResolution {
    /// No `agentName` was passed (or it was blank after `trim()`) — the
    /// config-less dispatch ADR 0020 intends. NOT a miss; the UI stays silent.
    NoName,
    /// A definition matched (`discover_agents` + `eq_ignore_ascii_case`, so a
    /// different case is a MATCH, not a miss).
    Matched,
    /// A name was given and matched NO discovered definition. The launch is
    /// still returned verbatim and the child still runs — but with no persona,
    /// no tool allowlist and no model from the name it was given.
    Unmatched,
}

// ── `SubagentMetrics` ─────────────────────────────────────────────────────────

/// The metrics captured for a subagent session (the child's reported cost +
/// its token usage + the wall-clock duration).
///
/// The cost mechanism is REAL as of issue #87, and it is **per model CALL, not
/// per turn**: the child's own loop emits one `cost_update` frame for every
/// provider call that reported a cost, the Supervisor's `SubagentCapture` keeps
/// the LAST of those frames (they carry totals — see
/// `worker/manager/subagent_flow.rs`), and this field carries that figure. The
/// earlier text here claimed the suite "self-emits a per-turn `cost_update`"
/// derived from a model `turn_end` usage; no such mechanism ever existed, and
/// the grain was wrong besides — a turn spans several model calls.
///
/// **Who spends this figure.** Two readers, and they do different jobs:
/// - the Client, via `metrics_json` on `subagent-closed` — the child card's
///   figure (issue #107's present-and-null rule);
/// - the PARENT's loop, via `harness::dispatch::dispatch_subagent`, which folds
///   `cost` into `AgentLoop::cost_acc` so the parent's own total includes its
///   children (issue #105, Fulcrum T2's one-writer fix). That fold is the ONLY
///   route by which a child's money reaches the parent's row: the Supervisor
///   used to emit a second, parent-keyed `cost_update` frame here instead, and
///   because a frame is a snapshot the parent's next frame erased it.
///
/// **What `cost` says here:** it is an `Option<f64>`, so the field CAN tell the
/// two states apart. `None` means **no call reported a cost** (a preflight
/// failure — a child that never started — or a child whose every call came back
/// unpriced); `Some(0.0)` means **a call reported a real zero**, which is a
/// figure. The wire spells the first `"cost": null` (present-and-null, exactly
/// the `SessionInfo.cost_usd` rule) and the second `0.0`, and the two are never
/// collapsed — a renderer prints nothing for the first and `$0.00` for the
/// second (ADR 0082 / D5). The old `f64` field collapsed both into `0.0` at
/// `unwrap_or(0.0)`; that collapse is what issue #107 removed, and the capture's
/// `Option` now reaches the hand-off untouched. The fold honours the same rule:
/// `None` adds nothing and emits nothing, `Some(0.0)` is a figure and folds.
///
/// `duration_ms` is always the wall clock (see the wire contract's metrics note).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubagentMetrics {
    /// The last message's accumulated text ("" when the turn produced no
    /// assistant text — the stale-carry-over test's EMPTY sentinel).
    pub output: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// The child's summed per-call cost in USD (see the struct's doc). `None` =
    /// "no call reported a cost" — NOT zero; `Some(0.0)` = a call reported a
    /// real zero, which IS a figure. Serde has no `skip_serializing_if` here, so
    /// `None` crosses as `"cost": null` (present-and-null) on both wires.
    pub cost: Option<f64>,
    pub duration_ms: u64,
}

// ── `SubagentFailureReason` ───────────────────────────────────────────────────

/// Why a **Subagent session** ended without settling (ADR 0055). DATA, not
/// prose — the `subagent-closed` payload's `reason` field and the card's short
/// line read this; NOTHING may string-match `error` to find out what happened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubagentFailureReason {
    /// The **Dead bound** won — silent with nothing in flight.
    Unresponsive,
    /// The **Worker process** exited unexpectedly.
    Crashed,
    /// A Stop on the parent (or an app-exit teardown).
    Cancelled,
    /// Anything else (a preflight failure — the child never started).
    #[default]
    Other,
}

// ── `SubagentOutcome` ─────────────────────────────────────────────────────────

/// The outcome of a subagent dispatch. `Serialize` / `Deserialize` /
/// `Clone` (ADR 0025 Task 2): the `SubagentResult` wire frame carries it
/// verbatim (the Supervisor → Worker resolution), and the client-side
/// enum needs both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SubagentOutcome {
    /// The task completed; `output` is the last message's accumulated text,
    /// `metrics` is the accumulated `cost_update` usage + duration.
    Completed {
        output: String,
        metrics: SubagentMetrics,
    },
    /// The task failed (or was cancelled — `error` includes `"cancelled"`).
    /// A **Dead Subagent** is NEVER `Completed`: it never decided it was
    /// finished. The status stays `failed`; the SALVAGE rides alongside.
    Failed {
        /// Why it ended (the machine-readable answer — never parsed out of
        /// `error`). `#[serde(default)]` so a stale Worker's payload decodes.
        #[serde(default)]
        reason: SubagentFailureReason,
        error: String,
        /// Whatever the child had produced (the `SubagentCapture`'s text) — `""`
        /// when there was nothing. Never a substitute for `Completed`.
        #[serde(default)]
        output: String,
        /// REAL accumulated usage + wall clock (the failed path used to report
        /// `SubagentMetrics::default()` — 0 tokens for work that really cost money).
        #[serde(default)]
        metrics: SubagentMetrics,
    },
}

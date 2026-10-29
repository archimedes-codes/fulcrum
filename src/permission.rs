//! The native in-process permission gate (ADR 0010): turns the native
//! `AgentLoop`'s permission requests into a UI prompt, and delivers the
//! user's answer back to the loop.
//!
//! A permission prompt is *user-paced* — the user might take minutes to
//! answer. The gate:
//!
//!   1. registers a oneshot sender in the manager's `pending_permissions`
//!      map (keyed by `"{session_id}/{request_id}"`),
//!   2. emits a `permission-request` Tauri event (the UI shows the prompt),
//!   3. awaits the answer (bounded by a 300 s timeout), and RETURNS the
//!      `PermissionOutcome` to the caller (the `AgentLoop` maps a deny to a
//!      tool-result error `"permission denied"` and a
//!      `Selected { "trust-space" }` to allow).
//!
//! The trusted-Space auto-approve (ADR 0010): a TRUSTED Space's mutating
//! tool is answered `Selected { "allow" }` IMMEDIATELY — no prompt, no
//! event, no oneshot. Fail-closed: no trust source / lookup failure / no
//! space row / canonicalize failure is untrusted (the prompt flow), with
//! the options
//! `[allow/Allow, reject/Block, trust-space/Don't ask again for this
//! Space]`; the user's choice maps to allow / deny / allow
//! (`trust-space` answers `Selected { "trust-space" }`; the flag WRITE is
//! NOT in the gate — ADR 0025: the Supervisor's `respond_permission`
//! relay applies it (Task 4) + the Worker's `StaticTrustSource` is
//! flipped (Task 2));
//! any other outcome, a timeout, a cancel, or a session close maps to
//! `Cancelled` (a deny).
//!
//! The compound key is what lets the driver-task cleanup drain every entry
//! for a closing session: dropping the senders makes the gate observe a
//! `Canceled` oneshot and cancel promptly instead of waiting out the full
//! 300 s timeout.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::agent::events::EventSink;
use crate::agent::harness::trust::TrustSource;

/// The user's decision on a permission prompt, as chosen via the
/// `respond_permission` Tauri command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionOutcome {
    /// The user selected one of the offered options; `option_id` is its id
    /// and `value` is the option's optional payload (`path` — copied back
    /// verbatim by the frontend).
    Selected {
        option_id: String,
        /// The option's payload (`path` for `allow-directory`). The serde
        /// PAIR: `default` (a previously-serialized outcome — one with NO
        /// `value` — still parses; the `SubagentOutcome::Failed` model; the
        /// deliberate contrast is `SubagentDispatch::name_resolution`, which
        /// refuses defaults because every default for it is a LIE — `None`
        /// here is not a lie, it is the honest "no directory") +
        /// `skip_serializing_if = "Option::is_none"` (`None` OMITS the key on
        /// the wire — byte-identical to today's `{"selected":{…}}`, which
        /// bare `default` alone would NOT be: it would serialize
        /// `"value": null`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<String>,
    },
    /// The user dismissed the prompt (or the turn / session was cancelled).
    Cancelled,
}

/// The manager's map of pending permission senders.
///
/// Keyed by `"{session_id}/{request_id}"` so the driver-task cleanup can
/// drain all entries for a closing session at once (dropping the senders
/// cancels the pending gates).
pub type PendingPermissions = Arc<Mutex<HashMap<String, oneshot::Sender<PermissionOutcome>>>>;

/// Build the compound key for a pending permission.
pub fn permission_key(session_id: &str, request_id: &str) -> String {
    format!("{session_id}/{request_id}")
}

/// The human-readable title of a permission prompt (ADR 0030): the user is
/// approving ONE access, so the title names the thing being reached for —
/// the path for the path tools (`read ../../.ssh/config`), the pattern AND
/// the directory for the search tools (`grep SECRET in /tmp`), and the
/// command for `bash`. The frontend (`PermissionPrompt.tsx`) renders this
/// verbatim, so the shape here IS what the user reads.
///
/// It never fails: an argument the tool would default (`path` → `"."`, the
/// same default the executors apply) is defaulted here too, and a tool this
/// function does not know falls back to the pre-ADR-0030
/// `"{tool} {compact args}"` rendering. A title that could not be built
/// would mean a prompt the user never sees, which is a silent deny.
pub fn permission_title(tool: &str, args: &serde_json::Value) -> String {
    // The `path` argument as the executor would read it. `None` only when
    // the key is absent or not a string (a number is rendered, not
    // dropped — the user should see what the model actually asked for).
    let arg = |key: &str| -> Option<String> {
        args.get(key).map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
    };
    // The directory tools default `path` to the `cwd` (".") in the
    // executor; a prompt must name the same directory the tool would use.
    let dir = || arg("path").unwrap_or_else(|| ".".to_string());
    match tool {
        "bash" => match arg("command") {
            Some(c) => format!("{tool} {c}"),
            None => tool.to_string(),
        },
        // The search tools take a pattern AND a directory — both matter to
        // the decision (a `grep` of `/tmp` is not a `grep` of `~/.ssh`).
        "find" | "grep" => match arg("pattern") {
            Some(p) => format!("{tool} {p} in {}", dir()),
            None => format!("{tool} {}", dir()),
        },
        "read" | "write" | "edit" => match arg("path") {
            // These three REQUIRE a path (the executor rejects the call
            // without one), so a missing one is the bare tool name rather
            // than a title pointing at a directory they would never use.
            Some(p) => format!("{tool} {p}"),
            None => tool.to_string(),
        },
        // `ls` defaults its directory to the `cwd` (like `find`/`grep`).
        "ls" => format!("{tool} {}", dir()),
        // Anything else: the verbatim pre-ADR-0030 shape.
        _ => format!("{tool} {args}"),
    }
}

/// The `options` array of a `permission-request` payload, per prompt kind.
/// `boundary: true` — the old three (`allow` "Allow" / `reject` "Block" /
/// `trust-space` "Don't ask again for this Space"), byte-identical to the
/// pre-per-prompt shape; `boundary: false` — the GUARD's three (`allow`
/// "Allow once" / `allow-directory` "Always allow this directory" carrying
/// its `"path"` / `reject` "Block"). A prompt is one kind or the other (the
/// old claim — "always three options, since a prompt only ever appears for an
/// untrusted Space, so `trust-space` is always actionable" — breaks the day
/// the guard prompts: a guard ask is an ask, and an ask can appear for a
/// Space that is trusted enough to make `trust-space` a one-click way to
/// disable the protection the prompt exists to provide).
///
/// `pub(crate)` because Task 3's CALLER picks the kind: the guard ask is
/// raised from the harness gate, which knows the target path this module's
/// gate signature does not carry.
pub(crate) fn permission_options(boundary: bool) -> Vec<serde_json::Value> {
    if boundary {
        vec![
            json!({ "optionId": "allow", "name": "Allow", "kind": "allow" }),
            json!({ "optionId": "reject", "name": "Block", "kind": "reject" }),
            json!({
                "optionId": "trust-space",
                "name": "Don't ask again for this Space",
                "kind": "allow"
            }),
        ]
    } else {
        vec![
            json!({ "optionId": "allow", "name": "Allow once", "kind": "allow" }),
            // THE guard's distinctive option: `allow-directory` carries the
            // resolved path as `path` (echoed back by the frontend as the
            // outcome's `value`). `""` is a PLACEHOLDER — the real
            // nearest-existing-ancestor path is computed at the guard's call
            // site (Task 3), whose prompt signature carries the target path
            // that `native_permission_gate`'s does not.
            //
            // `trust-space` is DELIBERATELY ABSENT here. The guard prompt
            // offers ONE kind of option list: `allow` / `allow-directory`
            // (carrying the resolved path as `path`) / `reject`. Trust still
            // SUPPRESSES the guard — trusting a Space from Space settings
            // silences it, consistent with the precedence table. What is
            // removed is the shortcut INSIDE the guard's own prompt: the
            // guard protects the agent's own instruction directories, and its
            // third option today is a one-click, permanent way to disable the
            // protection the prompt exists to provide — the most likely click
            // when a prompt recurs. Deliberate trust stays available from
            // Space settings; accidental trust does not. (Boundary prompts,
            // which ask about the user's own files beyond the read boundary,
            // keep `trust-space`.)
            json!({
                "optionId": "allow-directory",
                "name": "Always allow this directory",
                "path": "",
                "kind": "allow"
            }),
            json!({ "optionId": "reject", "name": "Block", "kind": "reject" }),
        ]
    }
}

/// The handle-free permission gate for the native `AgentLoop`: it
/// registers a `PendingPermissions` oneshot, emits a `permission-request`
/// event via the `sink`, and RETURNS the `PermissionOutcome` to the
/// caller (the `AgentLoop` maps a deny to a tool-result error
/// `"permission denied"` and a `Selected { "trust-space" }` to allow).
///
/// The trusted-Space auto-approve is here (ADR 0010): a TRUSTED Space's
/// mutating tool is answered `Selected { "allow" }` IMMEDIATELY — no
/// prompt, no event, no oneshot. Fail-closed: no trust source / lookup
/// failure / no space row / canonicalize failure is untrusted (the
/// prompt flow).
///
/// The `trust-space` ("Don't ask again") flag WRITE is NOT here (ADR
/// 0025 — the Worker has no `Db`): the Supervisor's `respond_permission`
/// relay applies it (Task 4: `db.set_space_trusted(cwd, true)` + the
/// Worker's `StaticTrustSource` flip — the very next tool call
/// auto-approves, matching the live-lookup behavior).
#[allow(clippy::too_many_arguments)]
pub async fn native_permission_gate(
    session_id: &str,
    request_id: &str,
    title: &str,
    sink: &Arc<dyn EventSink>,
    pending_permissions: &PendingPermissions,
    trust: Option<&dyn TrustSource>,
    cwd: &std::path::Path,
    cancel: &CancellationToken,
    options: Vec<serde_json::Value>,
) -> PermissionOutcome {
    // A CANCELLED turn must not auto-approve (finding 8a): the cancel
    // token is consulted BEFORE the trusted short-circuit (a Stop beats
    // the trusted-Space auto-approve — the turn is over, no more tools
    // run). The untrusted prompt flow below also races the token (its
    // `select!` arm), so a cancel is a `Cancelled` (a deny) either way.
    if cancel.is_cancelled() {
        return PermissionOutcome::Cancelled;
    }
    // Trusted Space (ADR 0010): auto-approve — no prompt, no event, no
    // oneshot. Fail-closed (`None` = always prompt; a lookup that cannot
    // resolve is `false`).
    let trusted = trust.map(|t| t.is_trusted(cwd)).unwrap_or(false);
    if trusted {
        return PermissionOutcome::Selected {
            option_id: "allow".to_string(),
            value: None,
        };
    }

    // (a) Register the oneshot the user's answer will flow through —
    // BEFORE the event is emitted: a `respond_permission` racing the
    // emission always finds the entry.
    let key = permission_key(session_id, request_id);
    let (tx, rx) = oneshot::channel();
    {
        let mut map = pending_permissions.lock().await;
        map.insert(key.clone(), tx);
    }

    // (b) Tell the UI to show the prompt. The option list is PER-PROMPT and
    // PER-CALLER (`permission_options`, passed in): the boundary caller emits
    // the old three, the guard caller (Task 3's harness gate) emits the
    // guard's three with a real `allow-directory` path. The old claim that a
    // prompt is always three options because it only ever appears for an
    // untrusted Space dies with the guard prompt: see `permission_options`.
    sink.emit(
        "permission-request",
        json!({
            "sessionId": session_id,
            "requestId": request_id,
            "request": {
                "sessionId": session_id,
                "toolCall": { "title": title },
                "options": options,
            },
        }),
    );

    // (c) Await the user's answer — NO DEADLINE: the prompt ends when the
    // PERSON answers or the turn is cancelled (a Stop), never when a clock
    // runs out. The 300 s cap this used to carry resolved `Cancelled`,
    // which the caller maps to a DENY — so a prompt the user never saw (or
    // answered late) became a silent `"permission denied"` five minutes
    // after it opened. A dropped oneshot (the session closed) is likewise a
    // `Cancelled` — the caller maps it to a deny, NOT an auto-allow.
    let outcome = tokio::select! {
        r = rx => r.unwrap_or(PermissionOutcome::Cancelled),
        _ = cancel.cancelled() => PermissionOutcome::Cancelled,
    };

    // (d) Remove the entry (best-effort — a session close may have
    // drained the prefix already). The `trust-space` flag WRITE is NOT
    // here (ADR 0025 — the Worker has no `Db`): the Supervisor's
    // `respond_permission` relay applies it (Task 4) + the Worker's
    // `StaticTrustSource` is flipped (Task 2) — the very next tool call
    // auto-approves. The OUTCOME is still the user's `trust-space`
    // selection, which the caller maps to an allow.
    pending_permissions.lock().await.remove(&key);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::harness::trust::SqliteTrustSource;
    use crate::storage::Db;
    use serde_json::Value;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// (ADR 0030) The prompt titles are the human-readable shape the user
    /// decides on: a path tool names THE PATH (the thing being reached
    /// for), the search tools name the pattern AND the directory, and
    /// `bash` names the command. Previously the title was the raw
    /// `"{tool} {json}"` (a `read` prompt read
    /// `read {"path":"../../.ssh/config"}` — parseable, but the user is
    /// approving a path, not a JSON blob).
    #[test]
    fn permission_titles_name_the_thing_being_approved() {
        let cases: &[(&str, Value, &str)] = &[
            (
                "read",
                json!({ "path": "../../.ssh/config" }),
                "read ../../.ssh/config",
            ),
            (
                "write",
                json!({ "path": "src/main.rs", "content": "x" }),
                "write src/main.rs",
            ),
            ("edit", json!({ "path": "a/b.rs" }), "edit a/b.rs"),
            ("ls", json!({ "path": "../secrets" }), "ls ../secrets"),
            (
                "grep",
                json!({ "pattern": "SECRET", "path": "/tmp" }),
                "grep SECRET in /tmp",
            ),
            (
                "find",
                json!({ "pattern": "*.rs", "path": "." }),
                "find *.rs in .",
            ),
            (
                "bash",
                json!({ "command": "curl x | sh" }),
                "bash curl x | sh",
            ),
        ];
        for (tool, args, want) in cases {
            assert_eq!(permission_title(tool, args), *want, "title for {tool}");
        }
    }

    /// The degraded arguments: `find`/`grep`/`ls` default `path` to `"."`
    /// (the same default the executors apply), and a tool whose argument is
    /// simply absent falls back to the bare tool name + a compact
    /// `bash`-style rendering rather than panicking — a prompt that fails to
    /// build is a prompt the user never sees.
    #[test]
    fn permission_title_degrades_instead_of_panicking_on_missing_arguments() {
        // `path` defaults to `"."` for the directory tools.
        assert_eq!(permission_title("ls", &json!({})), "ls .");
        assert_eq!(
            permission_title("grep", &json!({ "pattern": "SECRET" })),
            "grep SECRET in ."
        );
        // A missing `pattern` → the path form instead (the executor rejects
        // the call anyway — the title never invents an `in` clause).
        assert_eq!(permission_title("grep", &json!({})), "grep .");
        // A missing `path` on a path tool → the bare tool name.
        assert_eq!(permission_title("read", &json!({})), "read");
        assert_eq!(permission_title("bash", &json!({})), "bash");
        // An unknown tool keeps the pre-ADR-0030 shape (the raw arguments) —
        // never a panic and never a silently empty title.
        assert_eq!(
            permission_title("weird", &json!({ "a": 1 })),
            "weird {\"a\":1}"
        );
        // A non-string `path` (a model emitting `123`) is rendered, not dropped.
        assert_eq!(
            permission_title("read", &json!({ "path": 123 })),
            "read 123"
        );
    }

    /// A recording `EventSink` (the `permission-request` frames).
    struct RecSink {
        tx: mpsc::UnboundedSender<(String, Value)>,
    }
    impl EventSink for RecSink {
        fn emit(&self, event: &str, payload: Value) {
            let _ = self.tx.send((event.to_string(), payload));
        }
    }

    /// A prompt waits for the PERSON, never for a clock. The gate used to
    /// carry a 300 s timeout, which resolved to `Cancelled` — the caller
    /// maps that to a DENY — so a prompt the user never saw (or answered
    /// late) became a silent `"permission denied"` five minutes after it
    /// opened. Only a Stop, or the user's own answer, may decide.
    #[tokio::test(start_paused = true)]
    async fn an_unanswered_permission_prompt_never_settles_on_a_clock() {
        let pending: PendingPermissions = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx });
        let cwd = std::path::PathBuf::from("/tmp");
        let (p, s, c) = (pending.clone(), sink.clone(), cwd.clone());
        let cancel = CancellationToken::new();
        let gate = tokio::spawn(async move {
            native_permission_gate(
                "s1",
                "r1",
                "read ../../.ssh/config",
                &s,
                &p,
                None,
                &c,
                &cancel,
                // The BOUNDARY list (this gate's caller picks the option set —
                // Task 3's parameter; the guard list is the other kind).
                permission_options(true),
            )
            .await
        });

        let key = permission_key("s1", "r1");
        for _ in 0..100 {
            if pending.lock().await.contains_key(&key) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            pending.lock().await.contains_key(&key),
            "the prompt is open"
        );

        // 20 minutes — four times the cap the gate used to carry.
        for _ in 0..20 {
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::task::yield_now().await;
        }
        assert!(
            !gate.is_finished(),
            "an unanswered prompt must not become a silent deny on a clock"
        );

        // The late answer still decides.
        pending
            .lock()
            .await
            .remove(&key)
            .expect("the prompt is still pending")
            .send(PermissionOutcome::Selected {
                option_id: "allow".to_string(),
                value: None,
            })
            .expect("the gate is listening");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), gate)
                .await
                .expect("the answer resolves the gate")
                .expect("the gate did not panic"),
            PermissionOutcome::Selected {
                option_id: "allow".to_string(),
                value: None,
            }
        );
    }

    /// A temp `Db` + a temp `cwd` dir (the `spaces` rows are keyed by
    /// the CANONICAL path, so the cwd must exist), OWNED (#96 task 08).
    ///
    /// THE GUARD IS FIRST and the tuple is WIDER than it was
    /// (`(Arc<Db>, PathBuf)` → `(ScratchDir, Arc<Db>, PathBuf)`): destructured
    /// locals drop in REVERSE declaration order, so `cwd` → `db` → `dir`. That
    /// is the order Windows needs — `dir` holds `db.sqlite`, and Windows refuses
    /// to remove a directory containing an open file — and it is the order
    /// `harness/store.rs::temp_db` (task 03) and `tests/harness_store.rs::temp_db`
    /// (task 06) already use. The `cwd` is `dir.join("workspace")`, a CHILD of
    /// the guard, so `Drop` takes it with the tree; it stays a `PathBuf` because
    /// the tests pass it to `db.upsert_space(&cwd.display().to_string(), …)` and
    /// to the gate.
    ///
    /// Linux cannot show a wrong order here (POSIX unlinks out from under an open
    /// handle and `ScratchDir::Drop` swallows the failure), so this ordering is
    /// enforced by reading, not by any gate on this box.
    fn temp_db_cwd() -> (crate::test_support::ScratchDir, Arc<Db>, std::path::PathBuf) {
        let dir = crate::test_support::ScratchDir::in_env_temp("perm-gate-", "db");
        let db = Arc::new(Db::open(&dir.join("db.sqlite")).unwrap());
        let cwd = dir.join("workspace");
        std::fs::create_dir_all(&cwd).unwrap();
        (dir, db, cwd)
    }

    fn fresh_pending() -> PendingPermissions {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn native_gate_trusted_space_auto_approves() {
        let (_dir, db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        db.set_space_trusted(&cwd.display().to_string(), true)
            .unwrap();
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();

        let outcome = native_permission_gate(
            "s1",
            "r1",
            "Allow bash?",
            &sink,
            &pending,
            Some(&SqliteTrustSource::new(db)),
            &cwd,
            &cancel,
            permission_options(true),
        )
        .await;

        assert_eq!(
            outcome,
            PermissionOutcome::Selected {
                option_id: "allow".into(),
                value: None,
            },
            "a trusted Space is auto-approved (ADR 0010)"
        );
        assert!(
            sink_rx.try_recv().is_err(),
            "a trusted Space emits no permission-request"
        );
        assert!(
            pending.lock().await.is_empty(),
            "a trusted Space registers no oneshot"
        );
    }

    #[tokio::test]
    async fn native_gate_untrusted_prompts_and_maps_the_outcome() {
        let (_dir, db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        // Untrusted (the default) → the prompt flow.
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();
        // Clones for the spawned task (the test keeps `pending` to
        // resolve the oneshot + assert the map is drained after).
        let task_pending = pending.clone();
        let task_db = db.clone();
        let task_cwd = cwd.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            native_permission_gate(
                "s1",
                "r1",
                "Allow bash?",
                &sink,
                &task_pending,
                Some(&SqliteTrustSource::new(task_db)),
                &task_cwd,
                &task_cancel,
                permission_options(true),
            )
            .await
        });

        // Wait for the `permission-request` event (the oneshot is
        // registered before it — the entry is already in the map).
        let (_event, payload) = loop {
            if let Ok(m) = sink_rx.try_recv() {
                if m.0 == "permission-request" {
                    break m;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(payload["requestId"], "r1");
        assert_eq!(payload["request"]["toolCall"]["title"], "Allow bash?");
        assert_eq!(payload["request"]["options"].as_array().unwrap().len(), 3);
        // The EMISSION path pins the boundary list: `trust-space` is the
        // third option of the payload the UI actually receives.
        assert_eq!(payload["request"]["options"][0]["optionId"], "allow");
        assert_eq!(payload["request"]["options"][1]["optionId"], "reject");
        assert_eq!(
            payload["request"]["options"][2]["optionId"], "trust-space",
            "the boundary prompt offers `trust-space`"
        );

        // Resolve with `allow` (the `respond_permission` path).
        let key = permission_key("s1", "r1");
        let sender = pending
            .lock()
            .await
            .remove(&key)
            .expect("the oneshot is registered");
        sender
            .send(PermissionOutcome::Selected {
                option_id: "allow".into(),
                value: None,
            })
            .unwrap();
        assert_eq!(
            task.await.unwrap(),
            PermissionOutcome::Selected {
                option_id: "allow".into(),
                value: None,
            }
        );
        // The entry is removed after the answer.
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_gate_trust_space_allows_without_writing_the_flag() {
        let (_dir, db, cwd) = temp_db_cwd();
        db.upsert_space(&cwd.display().to_string(), false).unwrap();
        assert!(!db.space_trusted(&cwd).unwrap(), "untrusted by default");
        let (sink_tx, _sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();
        // Clones for the spawned task (the test keeps `pending` / `db`
        // / `cwd` to resolve the oneshot + assert the flag after).
        let task_pending = pending.clone();
        let task_db = db.clone();
        let task_cwd = cwd.clone();
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            native_permission_gate(
                "s1",
                "r2",
                "Allow write?",
                &sink,
                &task_pending,
                Some(&SqliteTrustSource::new(task_db)),
                &task_cwd,
                &task_cancel,
                permission_options(true),
            )
            .await
        });
        // Wait for the prompt, then resolve with `trust-space`.
        loop {
            let map = pending.lock().await;
            if map.contains_key(&permission_key("s1", "r2")) {
                break;
            }
            drop(map);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let key = permission_key("s1", "r2");
        pending
            .lock()
            .await
            .remove(&key)
            .unwrap()
            .send(PermissionOutcome::Selected {
                option_id: "trust-space".into(),
                value: None,
            })
            .unwrap();
        assert_eq!(
            task.await.unwrap(),
            PermissionOutcome::Selected {
                option_id: "trust-space".into(),
                value: None,
            },
            "the outcome is the user's selection (the caller maps it to allow)"
        );
        // (ADR 0025) The flag write is NOT in the gate anymore (the
        // Worker has no `Db`): the Supervisor's `respond_permission`
        // relay applies it (Task 4) + the Worker's `StaticTrustSource`
        // is flipped (Task 2).
        assert!(
            !db.space_trusted(&cwd).unwrap(),
            "the gate no longer writes the trust flag (the Supervisor does — ADR 0025)"
        );
    }

    /// The BOUNDARY prompt emits the old three options, in order, with
    /// `trust-space` still offered (the boundary prompt asks about the USER's
    /// own files beyond the read boundary, so "Don't ask again for this Space"
    /// stays actionable there — unlike the guard prompt).
    #[test]
    fn a_boundary_prompt_emits_the_old_three_options_with_trust_space() {
        let options = permission_options(true);
        let ids: Vec<&str> = options
            .iter()
            .map(|o| o["optionId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["allow", "reject", "trust-space"]);
        assert_eq!(options.len(), 3);
        assert_eq!(options[0]["name"], "Allow");
        assert_eq!(options[1]["name"], "Block");
        assert_eq!(options[2]["name"], "Don't ask again for this Space");
        // The boundary list carries NO `path` payload (nothing to persist).
        assert!(options.iter().all(|o| o.get("path").is_none()));
    }

    /// The GUARD prompt offers exactly `allow` / `allow-directory` (carrying
    /// the `path` key) / `reject`, and NEVER `trust-space`: the guard protects
    /// the agent's own instruction directories, and a one-click permanent
    /// "trust this Space" inside its own prompt is the most likely click when
    /// the prompt recurs — deliberate trust stays available from Space
    /// settings, accidental trust does not.
    #[test]
    fn a_guard_prompt_offers_exactly_allow_allow_directory_reject_and_no_trust_space() {
        let options = permission_options(false);
        let ids: Vec<&str> = options
            .iter()
            .map(|o| o["optionId"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["allow", "allow-directory", "reject"]);
        let names: Vec<&str> = options
            .iter()
            .map(|o| o["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            vec!["Allow once", "Always allow this directory", "Block",]
        );
        assert!(
            options[1].get("path").is_some(),
            "`allow-directory` carries the `path` key (the placeholder the \
             guard's call site replaces with the resolved ancestor — Task 3)"
        );
        assert!(
            options.iter().all(|o| o["optionId"] != "trust-space"),
            "the guard prompt NEVER offers `trust-space`: {options:?}"
        );
    }

    /// (Cross-version guard, the `SubagentOutcome::Failed` model in
    /// `worker/protocol.rs`) An OLD-shape `Selected` — the pre-`value`
    /// `{"selected":{"option_id":"allow"}}`, with NO `value` — still
    /// DECODES, to `value: None`.
    ///
    /// This is not a nicety: `"permission-response"` is in
    /// `decode_inbound`'s EXPLICIT allow-list, so a structural mismatch is a
    /// hard `ProtoError::Json` and NOT the permissive `Unknown { raw }`
    /// fallback. The asymmetry the serde PAIR on `value` encodes is exactly
    /// what this pins: a missing `option_id` stays a hard error (it has no
    /// default and never will), while a missing `value` is absorbed by
    /// `default` — a stale Worker (or an already-serialized outcome) keeps
    /// working, and the deliberate contrast is
    /// `SubagentDispatch::name_resolution`, which refuses a default because
    /// every default for it would be a LIE. `None` here is not a lie: it is
    /// the honest "no directory carried".
    #[test]
    fn before_value_selected_still_deserializes() {
        let old: PermissionOutcome = serde_json::from_str(r#"{"selected":{"option_id":"allow"}}"#)
            .expect("an OLD shape parses");
        assert_eq!(
            old,
            PermissionOutcome::Selected {
                option_id: "allow".to_string(),
                value: None,
            },
            "the missing `value` falls back to the `serde(default)`"
        );
        // The same shape for the other boundary id.
        let old: PermissionOutcome =
            serde_json::from_str(r#"{"selected":{"option_id":"trust-space"}}"#)
                .expect("an OLD `trust-space` shape parses");
        assert_eq!(
            old,
            PermissionOutcome::Selected {
                option_id: "trust-space".to_string(),
                value: None,
            }
        );
    }

    /// `value: None` OMITS the key on the wire — the serialized outcome is
    /// byte-identical to today's `{"selected":{"option_id":"allow"}}`.
    ///
    /// This is the `skip_serializing_if = "Option::is_none"` proof, the
    /// SECOND half of the pair: bare `#[serde(default)]` alone governs
    /// DESERIALIZATION only and would emit `"value": null`, breaking the
    /// "payload byte-identical to today's wire" claim.
    #[test]
    fn a_selected_without_value_serializes_without_the_key() {
        let encoded = serde_json::to_value(&PermissionOutcome::Selected {
            option_id: "allow".to_string(),
            value: None,
        })
        .unwrap();
        assert_eq!(
            encoded,
            json!({ "selected": { "option_id": "allow" } }),
            "a `None` value must NOT appear on the wire"
        );
        assert!(
            encoded["selected"].get("value").is_none(),
            "no `value` key at all (not `\"value\": null`): {encoded}"
        );
    }

    /// The new shape round-trips: a `Selected` carrying a `value` serializes
    /// WITH the key and parses back to the same value (the `allow-directory`
    /// prompt's `path` rides this field to the Supervisor, which persists it
    /// — Task 4).
    #[test]
    fn a_selected_with_value_round_trips() {
        let outcome = PermissionOutcome::Selected {
            option_id: "allow-directory".to_string(),
            value: Some("/home/u/.pi/agent/agents".to_string()),
        };
        let encoded = serde_json::to_value(&outcome).unwrap();
        assert_eq!(
            encoded,
            json!({
                "selected": {
                    "option_id": "allow-directory",
                    "value": "/home/u/.pi/agent/agents",
                }
            }),
            "the payload key is `value` inside the `selected` variant key"
        );
        let decoded: PermissionOutcome = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, outcome, "the `value` round-trips");
    }

    #[tokio::test]
    async fn native_gate_cancel_is_a_denial() {
        let (_dir, db, cwd) = temp_db_cwd();
        let (sink_tx, _sink_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn EventSink> = Arc::new(RecSink { tx: sink_tx });
        let pending = fresh_pending();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let outcome = native_permission_gate(
            "s1",
            "r3",
            "Allow bash?",
            &sink,
            &pending,
            Some(&SqliteTrustSource::new(db)),
            &cwd,
            &cancel,
            permission_options(true),
        )
        .await;
        assert_eq!(
            outcome,
            PermissionOutcome::Cancelled,
            "a cancel is a denial (never an auto-allow)"
        );
    }
}

//! (ADR 0102 / issue #183 F1) The registry's own censuses.
//!
//! Three things are pinned here and nowhere else, because the registry is the
//! only place all three are visible at once:
//!
//! 1. **Object safety + the `Arc` shape.** The trait is unusable if it is not
//!    object safe, and the failure would be a compile error in `builtins()`
//!    rather than a red test — so the pin copies the shape the crate already used
//!    for `TaskStore` (`seams.rs`'s `task_store_is_object_safe_and_shareable`, which
//!    F3 deleted with the Task subsystem): build an `Arc<dyn …>`, call through it,
//!    assert `Send + Sync`. This is now the crate's only pin of that shape.
//! 2. **The registry's own membership** — the 12 built-in names, and the fact
//!    that no built-in is excluded from scripts.
//! 3. **The composition seam.** `host_entries` is what F2 fills in; its contract
//!    at F1 is "every spec you hand me comes back, `Allowed`, answering loudly".

use std::sync::Arc;

use serde_json::json;

use super::super::{
    builtins, default_registry, host_entries, lookup, script_access, ScriptAccess, ToolEntry,
    ToolExecutor, BUILTIN_TOOLS, LOOP_INTERNAL_SCRIPT,
};
use crate::provider_types::ToolSpec;
use crate::tools::exec::execute_tool;
use crate::tools::exec::registry::NativeTool;

use super::support::{ctx, temp_cwd, text_of};

/// The 12 names [`builtins`] must carry: the native/web executors and nothing
/// else. FROZEN, for the same reason the advertised-set census is frozen — a count
/// cannot say WHICH name vanished, and `builtins()` dropping one is precisely the
/// silent drop this run is most likely to produce (it is the function every wave
/// edits).
///
/// (ADR 0102 / issue #183 F3) This list was 19 through F1 and F2: the 7
/// board/artifact verbs were engine-side then, because their `exec_*` arms lived in
/// this crate. F3 deletes those arms, so the 7 leave the registry too and arrive
/// per session as `StartEnv::host_tools` — engine-owned names are the ones whose
/// SUBSTANCE the engine confines.
const BUILTIN_NAMES: &[&str] = &[
    "bash",
    "read",
    "write",
    "edit",
    "find",
    "grep",
    "ls",
    "list_skills",
    "read_skill",
    "web_search",
    "fetch_content",
    "get_search_content",
];

#[test]
fn builtins_are_the_twelve_engine_owned_names() {
    let names: Vec<String> = builtins().into_iter().map(|e| e.spec.name).collect();
    let expected: Vec<String> = BUILTIN_NAMES.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        names, expected,
        "`builtins()` is no longer the 12 engine-owned names in `tool_specs()` order. \
         A missing name here is a tool the model is STILL advertised (`tool_specs()` \
         carries it, and the host cannot carry an engine name) and which now answers \
         `unknown tool` from `execute_tool` — the advertised-and-unreachable class, \
         and the ENGINE half of the split `tool_specs.rs`'s census reports. An EXTRA \
         name is a tool the registry dispatches that nobody advertises."
    );
    assert_eq!(
        names.len(),
        12,
        "and the count is 12 (the native/web executors; the 7 board verbs left for \
         `StartEnv::host_tools` in F3)"
    );
}

#[test]
fn builtin_tools_table_and_builtins_agree() {
    // The `match`'s successor list and the built registry must be the same list.
    // If they drift, `builtins()` silently SKIPS (a `filter_map` on the spec
    // lookup), so the drift is invisible at runtime and only this census says
    // which of the two moved.
    let table: Vec<&str> = BUILTIN_TOOLS.iter().map(|(name, _)| *name).collect();
    let registry = builtins();
    let built: Vec<&str> = registry.iter().map(|e| e.name()).collect();
    assert_eq!(
        table, built,
        "`BUILTIN_TOOLS` and `builtins()` disagree — a name in the table with no \
         `tool_specs()` entry is silently dropped by `builtins()`'s `filter_map`"
    );
}

#[test]
fn every_builtin_entry_carries_its_advertised_spec_verbatim() {
    // ADR 0102's byte-identity rule: the entry's spec IS the spec `tool_specs()`
    // hands the model, not a re-statement of it. A registry that carries its own
    // description string is a second owner of the advertised surface, which is the
    // thing the whole ADR exists to prevent.
    let advertised = crate::harness::tool_specs::tool_specs();
    for entry in builtins() {
        let spec = advertised
            .iter()
            .find(|s| s.name == entry.spec.name)
            .unwrap_or_else(|| panic!("`{}` is registered but not advertised", entry.spec.name));
        assert_eq!(
            (&entry.spec.description, &entry.spec.parameters),
            (&spec.description, &spec.parameters),
            "`{}`: the registry's spec must be BYTE-IDENTICAL to `tool_specs()`'s",
            entry.spec.name
        );
    }
}

#[test]
fn builtins_exclude_nothing_for_scripts() {
    // The preservation rule, read from the registry side: the six names the old
    // `EXCLUDED_FROM_SCRIPT` carried are ALL loop-internal, so no built-in was ever
    // excluded. A `builtins()` that starts excluding one of its own names silently
    // deletes script reach that ADR 0092 chose `execute_tool` arms to CREATE (the
    // `roadmap` case: one script turning an approved backlog into N Tasks).
    for entry in builtins() {
        assert!(
            entry.script.is_callable(),
            "`{}` is a built-in and `ScriptAccess::Excluded` — no built-in was ever \
             excluded from scripts, and the board verbs in particular were \
             script-reachable ON PURPOSE (ADR 0092); since F3 they reach scripts as \
             HOST entries, which `host_entries` also marks `Allowed`",
            entry.name()
        );
    }
}

#[test]
fn the_loop_internal_script_table_is_the_six_excluded_names() {
    // `EXCLUDED_FROM_SCRIPT`'s successor. Exactly six, exactly those reasons, and
    // `codemode` is among them (a nested `codemode` is a second engine running a
    // second script) while `mcp` is NOT (it is the sanctioned way for a script to
    // reach a server; only its `action: "auth"` is refused, at ARGUMENT level).
    let table: Vec<(&str, &str)> = LOOP_INTERNAL_SCRIPT
        .iter()
        .map(|(name, access)| {
            let reason = match access {
                ScriptAccess::Excluded(reason) => *reason,
                ScriptAccess::Allowed => "<allowed>",
            };
            (*name, reason)
        })
        .collect();
    assert_eq!(
        table,
        vec![
            ("ask", "interactive"),
            ("sudo_exec", "interactive"),
            ("manage_todo_list", "interactive"),
            ("subagent", "recursive"),
            ("list_agents", "recursive"),
            ("codemode", "recursive"),
        ],
        "the excluded set moved. Its membership is a SECURITY-adjacent decision: an \
         added name breaks a script that works today, a removed name lets a script \
         block on a human (`ask`) or fork a whole turn (`subagent`)."
    );
    assert!(
        !LOOP_INTERNAL_SCRIPT.iter().any(|(name, _)| *name == "mcp"),
        "`mcp` must NOT be name-excluded — its `auth` ACTION is refused at argument \
         level by `codemode::interactive_args`, which stays a special case because no \
         name-level field can express \"this name, except when…\""
    );
}

#[test]
fn script_access_reads_the_entry_first_and_the_loop_table_second() {
    let registry = builtins();

    // An entry's own field WINS — including against the loop table, which is what
    // makes the field the primary record rather than a decoration.
    let mut overridden = builtins();
    let bash = overridden
        .iter_mut()
        .find(|e| e.name() == "bash")
        .expect("`bash` is a built-in");
    bash.script = ScriptAccess::Excluded("test-only");
    assert_eq!(
        script_access(&overridden, "bash"),
        ScriptAccess::Excluded("test-only"),
        "the per-entry field must be what `script_access` reports"
    );
    assert_eq!(
        script_access(&registry, "bash"),
        ScriptAccess::Allowed,
        "and the default registry still allows it (the override above was local)"
    );

    // A loop-internal name (no entry) falls to the table.
    for name in [
        "ask",
        "sudo_exec",
        "manage_todo_list",
        "subagent",
        "list_agents",
        "codemode",
    ] {
        assert!(
            !script_access(&registry, name).is_callable(),
            "`{name}` is loop-internal and must not be script-callable"
        );
    }

    // A name NEITHER knows is `Allowed` — the F2 fall-through. A host spec the
    // engine has never seen must be callable, or the `roadmap` case dies quietly.
    assert_eq!(
        script_access(&registry, "mark_ready"),
        ScriptAccess::Allowed,
        "an unknown name defaults to `Allowed`: defaulting a name the engine has \
         never seen to `Excluded` would silently delete the script reach ADR 0092 \
         built these arms to have"
    );
}

#[test]
fn registry_lookup_hits_a_builtin_and_misses_the_loop_internal_names() {
    let registry = builtins();
    assert!(lookup(&registry, "bash").is_some());
    for name in ["ask", "mcp", "codemode", "subagent", "dispatch_subagent"] {
        assert!(
            lookup(&registry, name).is_none(),
            "`{name}` must not resolve through the registry — its executor is \
             `&mut AgentLoop`, not a `ToolCtx`"
        );
    }
}

/// The trait is usable as a trait object behind the `Arc` every entry holds it
/// in, and the future it returns is `Send` — the shape
/// `seams::task_store_is_object_safe_and_shareable` used to pin for `TaskStore`,
/// copied. F3 deleted that one, so this is the crate's only witness that the
/// object-safety of this trait holds (and it is a compile-failure guard as much as a
/// runtime one: `Arc<dyn ToolExecutor>` in `builtins()` stops compiling if it breaks).
#[test]
fn the_tool_executor_trait_is_object_safe_and_shareable() {
    struct AlwaysOk;
    impl ToolExecutor for AlwaysOk {
        fn execute(
            &self,
            _ctx: &crate::tools::types::ToolCtx,
            _params: serde_json::Value,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::tools::types::ToolResult> + Send>,
        > {
            Box::pin(async { crate::tools::types::ToolResult::ok_text("ok".into(), None) })
        }
    }

    let entries: Vec<ToolEntry> = vec![ToolEntry {
        spec: ToolSpec {
            name: "always_ok".into(),
            description: "a fixture".into(),
            parameters: json!({"type": "object"}),
        },
        executor: Arc::new(AlwaysOk) as Arc<dyn ToolExecutor>,
        script: ScriptAccess::Allowed,
    }];

    // Object safety: this line does not compile if the trait stops being object
    // safe (an `async fn` in the trait, or a generic method, breaks it).
    let dyn_handle: Arc<dyn ToolExecutor> = Arc::clone(&entries[0].executor);
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    assert_send_sync(&dyn_handle);

    // And the SHARING is real: an `Arc` clone reaches the SAME executor, which is
    // what lets a `ToolCtx`-shaped clone of an entry cost two refcount bumps.
    let clone = Arc::clone(&entries[0].executor);
    assert!(
        Arc::ptr_eq(&dyn_handle, &clone),
        "the Arc is SHARED, not copied"
    );
}

#[tokio::test]
async fn a_registry_entry_executes_and_the_boxed_future_is_send() {
    // The `tokio::spawn` is the `Send` proof that actually matters: the crate
    // spawns a `RenderRequester` future the same way (`sandbox/tests/mod.rs`), and
    // a `!Send` boxed future would fail to compile HERE rather in a doc comment.
    let registry = default_registry();
    let entry = lookup(registry, "read").expect("`read` is a built-in");
    let cwd = temp_cwd("registry-read");
    let ctx = ctx(&cwd);
    let executor = Arc::clone(&entry.executor);
    let params = json!({ "path": "nope.txt" });
    let handle = tokio::spawn(async move { executor.execute(&ctx, params).await });
    let result = handle.await.expect("the spawned executor ran");
    assert!(
        result.is_error,
        "reading a file that is not there fails: {}",
        text_of(&result)
    );
    assert!(
        !text_of(&result).contains("unknown tool"),
        "it reached `exec_read` and not the miss arm: {}",
        text_of(&result)
    );
}

#[tokio::test]
async fn native_tool_is_the_same_call_the_old_match_arm_made() {
    // Behaviour-neutral by construction, CHECKED: the adapter must produce the
    // SAME answer as `execute_tool` for the same name, because it wraps the same
    // `exec_*` function with the same references. `web_search` with no `query`
    // picks this because its refusal sentence is executor-specific — a name that
    // routed to the wrong arm could not reproduce it.
    let cwd = temp_cwd("registry-neutral");
    let tool_ctx = ctx(&cwd);
    let entry = lookup(default_registry(), "web_search").expect("`web_search` is a built-in");

    let via_entry = entry.executor.execute(&tool_ctx, json!({})).await;
    let via_execute_tool = execute_tool(&tool_ctx, "web_search", &json!({})).await;

    assert_eq!(
        text_of(&via_entry),
        text_of(&via_execute_tool),
        "the registry path and `execute_tool` must answer identically"
    );
    assert!(
        text_of(&via_entry).contains("web_search requires 'query' or 'queries'"),
        "and the answer is `exec_web_search`'s own refusal, not a routing miss: {}",
        text_of(&via_entry)
    );
}

#[tokio::test]
async fn execute_tool_still_answers_the_unknown_tool_sentence() {
    // The registry's MISS sentence is the old `other =>` arm's, byte-for-byte.
    // `unknown_tool_is_error` in `fs_tools.rs` pins the same thing from the
    // dispatcher's side; this pins that the sentence belongs to the registry and
    // not to a `match` that no longer exists.
    let cwd = temp_cwd("registry-miss");
    let r = execute_tool(&ctx(&cwd), "nope", &json!({})).await;
    assert!(r.is_error);
    assert_eq!(text_of(&r), "unknown tool: nope");
}

#[tokio::test]
async fn host_entries_carry_their_specs_allowed_and_refuse_loudly() {
    // `host_entries` is the composition seam F2 fills in. Its F1 contract is
    // three things, all of which F2 must not break: every spec comes back, in
    // order; each is `Allowed` (the preservation rule); and the refusal is a
    // `ToolResult::fail` that NAMES itself, never a silent park. A host tool that
    // parks with no Supervisor answering is a hang until the turn is cancelled —
    // which is why the daemon's `task-request` refusal exists today and why
    // `HostTool` starts out loud.
    let specs = vec![
        ToolSpec {
            name: "mark_ready".into(),
            description: "Mark the session ready.".into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "list_board".into(),
            description: "a host-owned board".into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
    ];
    let entries = host_entries(specs);

    assert_eq!(
        entries.iter().map(|e| e.name()).collect::<Vec<_>>(),
        vec!["mark_ready", "list_board"],
        "every spec comes back, in the order it arrived"
    );
    assert!(
        entries.iter().all(|e| e.script.is_callable()),
        "host entries are `Allowed` — a `Default` of `Excluded` would silently \
         delete the script reach ADR 0092 built these arms to have"
    );

    let cwd = temp_cwd("registry-host");
    for entry in &entries {
        let r = entry.executor.execute(&ctx(&cwd), json!({})).await;
        assert!(
            r.is_error,
            "`{}` refuses rather than pretending to succeed",
            entry.name()
        );
        let text = text_of(&r);
        assert!(
            !text.contains("unknown tool"),
            "`{}` resolves through the registry, so it must NOT answer the miss \
             sentence (that is the bug F2's frame exists to fix): {text:?}",
            entry.name()
        );
        assert!(
            text.contains("host-owned"),
            "and the refusal says WHY in terms a user can act on: {text:?}"
        );
    }
}

#[tokio::test]
async fn a_host_entry_reaches_through_a_composed_registry_where_execute_tool_cannot() {
    // The hazard the advertised-set census CANNOT see, pinned at its smallest:
    // `builtins() ++ host_entries(…)` is the composed set a loop carries, and a
    // host name resolves THERE and not through the free `execute_tool`. This is
    // the exact shape `dispatch_nested` must resolve against — resolving against
    // `execute_tool` instead is the mutation this test reddens on.
    let composed = {
        let mut registry = builtins();
        registry.extend(host_entries(vec![ToolSpec {
            name: "mark_ready".into(),
            description: "Mark the session ready.".into(),
            parameters: json!({"type": "object", "properties": {}}),
        }]));
        registry
    };

    let entry = lookup(&composed, "mark_ready").expect("the composed set carries it");
    let cwd = temp_cwd("registry-composed");
    let r = entry.executor.execute(&ctx(&cwd), json!({})).await;
    assert!(
        !text_of(&r).contains("unknown tool"),
        "the composed registry answers it: {}",
        text_of(&r)
    );

    // …and the free function, which only knows the built-ins, does not.
    let r = execute_tool(&ctx(&cwd), "mark_ready", &json!({})).await;
    assert_eq!(
        text_of(&r),
        "unknown tool: mark_ready",
        "`execute_tool` is the built-ins-only path — a loop must resolve against \
         its OWN registry, which is why `dispatch_nested` was moved"
    );
}

#[test]
fn default_registry_is_the_builtins_and_is_shared() {
    // The `LazyLock` is a SHARED registry (an `Arc` per executor, one set for the
    // process), and it is `builtins()` — the two must not drift.
    assert_eq!(
        default_registry().len(),
        builtins().len(),
        "`default_registry()` is `builtins()`"
    );
    assert!(
        Arc::ptr_eq(
            &default_registry()[0].executor,
            &default_registry()[0].executor
        ),
        "the entries are behind `Arc`s the process shares"
    );
    assert_eq!(
        default_registry().len(),
        12,
        "and the default registry is 12 (the advertised 19 `tool_specs()` names minus \
         the 7 loop arms — the 7 board verbs left `builtins()` in F3 and now arrive as \
         `StartEnv::host_tools`, which `default_registry` cannot carry because it is \
         the process-wide built-ins-only registry)"
    );
}

#[test]
fn the_native_adapter_wraps_a_free_executor_without_capturing() {
    // `NativeRun` is a `fn` POINTER, so a `NativeTool` cannot accidentally capture
    // loop state — the type is the guarantee that a built-in adapter is a
    // reference to a free function and nothing more.
    fn always_ok(
        _ctx: crate::tools::types::ToolCtx,
        _params: serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::tools::types::ToolResult> + Send>>
    {
        Box::pin(async { crate::tools::types::ToolResult::ok_text("ok".into(), None) })
    }
    let tool = NativeTool::new(always_ok);
    let arc: Arc<dyn ToolExecutor> = Arc::new(tool);
    assert_eq!(
        std::mem::size_of::<NativeTool>(),
        std::mem::size_of::<super::super::NativeRun>(),
        "a `NativeTool` is one `fn` pointer wide — no capture, no vtable"
    );
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    assert_send_sync(&arc);
}

// ── the engine-name refusal's OBSERVABILITY half ───────────────────────────────
//
// `host_entries` DROPS a host spec whose name the engine owns rather than failing
// the `Start`, and a silent drop is the fail-open this crate names everywhere else.
// So the drop writes a line, and these tests read it. The message is built by a
// pure function (`refused_host_tool_names`) for the reason `turn.rs` gives for its
// own twin: the sentence is the part that can be WRONG, and a line written straight
// to stderr is unobservable from inside `cargo test`.

use super::super::registry::refused_host_tool_names;

/// The sentence exists ONLY when something was refused, and it NAMES each refused
/// spec — a drop that does not say which name is a drop nobody can act on.
#[test]
fn the_refusal_line_names_every_refused_spec_and_nothing_when_there_is_none() {
    assert_eq!(
        refused_host_tool_names(&[]),
        None,
        "an envelope with no shadow must record NOTHING — a line on every session \
         start would make this channel unreadable, which is the volume rule \
         `diagnostics` states for itself"
    );
    let specs = vec![
        ToolSpec {
            name: "ask".into(),
            description: "d1".into(),
            parameters: json!({}),
        },
        ToolSpec {
            name: "bash".into(),
            description: "d2".into(),
            parameters: json!({}),
        },
    ];
    let line = refused_host_tool_names(&specs).expect("two refusals make a line");
    assert!(
        line.contains("\"ask\"") && line.contains("\"bash\""),
        "the line must name BOTH refused specs, got {line:?}"
    );
    // The description is NOT in the payload. It is host prose and the channel's rule
    // is "no user content"; the name is what a supporter needs.
    assert!(
        !line.contains("d1") && !line.contains("d2"),
        "a refused spec's DESCRIPTION must not reach the diagnostics line: {line:?}"
    );
}

/// A host spec named with a line separator cannot forge a line in
/// `diagnostics.log` — the corridor `provider/mod.rs::one_field_per_line` closes for
/// provider-supplied ids, closed here by `{:?}` escaping on host-supplied names.
#[test]
fn a_refused_name_cannot_forge_a_second_diagnostics_line() {
    let specs = vec![
        ToolSpec {
            name: "ask\nFORGED-LINE".into(),
            description: "d".into(),
            parameters: json!({}),
        },
        ToolSpec {
            name: "bash\u{2028}FORGED".into(),
            description: "d".into(),
            parameters: json!({}),
        },
    ];
    let line = refused_host_tool_names(&specs).expect("two refusals make a line");
    assert!(
        !line.contains('\n') && !line.contains('\u{2028}') && !line.contains('\u{2029}'),
        "the payload carries a raw line terminator and would be TWO lines in the \
         file a supporter reads: {line:?}"
    );
    assert_eq!(
        line.lines().count(),
        1,
        "exactly one line, whatever the name contained: {line:?}"
    );
    // And the name is still READABLE after escaping — the point of the line is to
    // say which name was refused, so escaping must not swallow it.
    assert!(
        line.contains("ask") && line.contains("bash"),
        "the escaped name still identifies the spec: {line:?}"
    );
}

/// THE drop is RECORDED, not just returned: an armed diagnostics channel gets the
/// line when `host_entries` refuses a name.
///
/// This is the pin that makes "the drop is observable" a fact. Without the
/// `record` call the tests above stay green (they read the pure builder) and the
/// only witness left is an `eprintln!` nobody sees in a shipped build.
#[test]
fn a_refused_host_entry_is_recorded_to_diagnostics() {
    let _guard = crate::test_support::env_lock();
    crate::diagnostics::init(None);
    let dir = crate::test_support::ScratchDir::in_env_temp("host-entry-refusal-", "recorded");
    // Declared AFTER the dir so it drops FIRST (locals drop in reverse) — the global
    // is cleared before the dir it names is removed, or a sibling's `record`
    // `create_dir_all`s the deleted dir back into existence.
    struct DisableOnExit;
    impl Drop for DisableOnExit {
        fn drop(&mut self) {
            crate::diagnostics::init(None);
        }
    }
    let _disable = DisableOnExit;
    crate::diagnostics::init(Some(dir.to_path_buf()));

    // The accepted half FIRST, so a vacuous pass (nothing recorded at all) cannot be
    // mistaken for the assertion below: `mark_ready` proves the function ran and
    // wrote about the name it refused rather than staying silent about both.
    let entries = host_entries(vec![
        ToolSpec {
            name: "mark_ready".into(),
            description: "a host verb".into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "ask".into(),
            description: "a host's own `ask`".into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
    ]);
    assert_eq!(
        entries.iter().map(|e| e.name()).collect::<Vec<_>>(),
        vec!["mark_ready"],
        "the fixture's premise: one accepted, one refused"
    );

    let path = dir.join(crate::diagnostics::FILE_NAME);
    let content = std::fs::read_to_string(&path).expect(
        "the refused name is recorded — an unrecorded drop is invisible in a \
                 shipped build, which is the fail-open this channel exists to close",
    );
    assert!(
        content.contains("\"ask\""),
        "the recorded line does not name the refused spec: {content:?}"
    );
    assert!(
        !content.contains("\"mark_ready\""),
        "an ACCEPTED host verb appears in the refusal line: {content:?}"
    );
}

/// And a session with nothing to refuse records NOTHING — the channel's volume rule
/// ("a handful of lines per session") is a promise only if the ordinary path is
/// silent.
#[test]
fn an_envelope_of_legitimate_host_verbs_records_nothing() {
    let _guard = crate::test_support::env_lock();
    crate::diagnostics::init(None);
    let dir = crate::test_support::ScratchDir::in_env_temp("host-entry-refusal-", "silent");
    struct DisableOnExit;
    impl Drop for DisableOnExit {
        fn drop(&mut self) {
            crate::diagnostics::init(None);
        }
    }
    let _disable = DisableOnExit;
    crate::diagnostics::init(Some(dir.to_path_buf()));

    let entries = host_entries(vec![ToolSpec {
        name: "list_board".into(),
        description: "a host-owned board verb".into(),
        parameters: json!({"type": "object", "properties": {}}),
    }]);
    assert_eq!(entries.len(), 1, "the legitimate verb composes");
    assert!(
        !dir.join(crate::diagnostics::FILE_NAME).exists(),
        "a legitimate payload wrote a diagnostics line — the ordinary session start \
         must stay silent"
    );
}

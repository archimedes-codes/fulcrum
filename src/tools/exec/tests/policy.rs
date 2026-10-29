use serde_json::json;

use super::support::*;
use crate::agent::tools::exec::access::PROTECTED_WRITE_ROUTE;
use crate::agent::tools::exec::execute_tool;
#[cfg(unix)]
use crate::agent::tools::exec::protected_refusal_text;
use crate::agent::tools::types::ToolCtx;
// (T03) The rule vocabulary — every protected-dir pin below is now a RULE-row
// pin (`default_rules()` is empty, so a refusal needs a rule) and the resolver
// pins call `rule_for` directly.
use crate::agent::policy::{AccessRule, RuleDecision, RuleDirection};

// ── the policy tiers (ADR 0030) ───────────────────────────────────────

/// The point of the whole feature: with `reads: Allow` the gate is the
/// only decision point, so a read BEYOND the boundary lands. The
/// `Sandboxed` contrast is `read_escape_rejected` above (the same path,
/// rejected) — without it this test would pass vacuously.
#[tokio::test]
async fn read_beyond_the_boundary_succeeds_when_reads_allow() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    // A PARENT dir so the out-of-boundary file stays inside the test's
    // own temp tree (`../outside.txt` from the cwd lands in the parent).
    let parent = temp_cwd("tier-read");
    let cwd = parent.join("space");
    std::fs::create_dir_all(&cwd).unwrap();
    let target = parent.join("outside.txt");
    std::fs::write(&target, "OUTSIDE-OK").unwrap();
    // The premise: the path is genuinely beyond the boundary.
    assert!(!target.starts_with(&cwd));

    let policy = FilePolicy {
        reads: AccessPolicy::Allow,
        ..FilePolicy::default()
    };
    let r = execute_tool(
        &ctx_policy(&cwd, policy.clone()),
        "read",
        &json!({ "path": "../outside.txt" }),
    )
    .await;
    assert!(!r.is_error, "an allowed read must land: {}", text_of(&r));
    assert_eq!(text_of(&r), "OUTSIDE-OK");

    // And the ABSOLUTE form lands too (the unrestricted root is `/`).
    let r = execute_tool(
        &ctx_policy(&cwd, policy),
        "read",
        &json!({ "path": target }),
    )
    .await;
    assert!(!r.is_error, "{}", text_of(&r));
    assert_eq!(text_of(&r), "OUTSIDE-OK");
}

/// (ADR 0030) A read boundary with MULTIPLE roots: a file in the second
/// boundary root (a skill dir at the repo root — outside the session
/// `cwd`) is readable, while a file in neither root is not.
#[tokio::test]
async fn read_reaches_a_second_boundary_root_when_sandboxed() {
    let cwd = temp_cwd("tier-multi-root");
    let skill_dir = temp_cwd("tier-multi-root-skills");
    std::fs::write(skill_dir.join("SKILL.md"), "SKILL-BODY").unwrap();
    let ToolCtx { boundary, .. } = ctx(&cwd);
    // The boundary the harness would hand the tools: cwd FIRST, then the
    // discovery root.
    let c = ToolCtx {
        boundary: vec![cwd.to_path_buf(), skill_dir.to_path_buf()],
        ..ctx(&cwd)
    };
    assert_eq!(boundary, vec![cwd.to_path_buf()]);

    let r = execute_tool(&c, "read", &json!({ "path": skill_dir.join("SKILL.md") })).await;
    assert!(
        !r.is_error,
        "a boundary root must be readable: {}",
        text_of(&r)
    );
    assert_eq!(text_of(&r), "SKILL-BODY");

    // A dir that is neither the cwd nor a root is still rejected.
    let elsewhere = temp_cwd("tier-multi-root-elsewhere");
    std::fs::write(elsewhere.join("no.txt"), "NO").unwrap();
    let r = execute_tool(&c, "read", &json!({ "path": elsewhere.join("no.txt") })).await;
    assert!(r.is_error, "a non-root must stay rejected");
}

/// `writes: Allow` widens the WRITE direction the same way (an approved
/// `Ask` must be able to land, or the prompt lies). The `Sandboxed`
/// contrast is `write_escape_rejected` above.
#[tokio::test]
async fn write_beyond_the_boundary_succeeds_when_writes_allow() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let parent = temp_cwd("tier-write");
    let cwd = parent.join("space");
    std::fs::create_dir_all(&cwd).unwrap();
    let policy = FilePolicy {
        writes: AccessPolicy::Allow,
        ..FilePolicy::default()
    };
    let r = execute_tool(
        &ctx_policy(&cwd, policy),
        "write",
        &json!({ "path": "../outside.txt", "content": "landed" }),
    )
    .await;
    assert!(!r.is_error, "an allowed write must land: {}", text_of(&r));
    let landed = parent.join("outside.txt");
    assert_eq!(std::fs::read_to_string(&landed).unwrap(), "landed");
    // The CONTRAST: the same path under `writes: Sandboxed` is refused
    // and creates nothing.
    let r = execute_tool(
        &ctx(&cwd),
        "write",
        &json!({ "path": "../other.txt", "content": "nope" }),
    )
    .await;
    assert!(r.is_error, "a sandboxed write must NOT land");
    assert!(!parent.join("other.txt").exists());
}

/// (T03 — the churned premise, RENAMED from
/// `write_into_a_protected_dir_is_refused_under_every_policy`) A protected write
/// with NO rule matching is NOT refused by the executor, under ANY posture.
///
/// The old name asserted the claim the permissions spec exists to remove: that
/// the deny-list refuses "in every policy". Under the corrected reading a
/// no-rule write into one of the four dirs is precedence ROW 3 — the GATE's
/// ASK — and an executor that refused anyway would turn that prompt into a
/// lie (the user clicks Allow, the tool fails). So this test now pins the
/// OPPOSITE, and keeps the three-posture sweep because the point is that the
/// executor's answer is posture-INDEPENDENT in the new direction too.
///
/// The refusal half of the old test did not disappear — it moved onto the
/// USER-DENY row, where a rule actually decided it:
/// `a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording`
/// and the two session-snapshot pins below.
#[tokio::test]
async fn a_write_into_a_protected_dir_with_no_rule_is_not_refused() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    for policy_writes in [
        AccessPolicy::Sandboxed,
        AccessPolicy::Ask,
        AccessPolicy::Allow,
    ] {
        let cwd = temp_cwd("tier-protect");
        // A PINNED protected dir INSIDE the cwd, so the boundary check passes
        // for it under `Sandboxed` too and ONLY the guard membership could
        // refuse — which is exactly the thing that must now stay silent.
        let protected = cwd.join(".agents/skills");
        std::fs::create_dir_all(&protected).unwrap();
        std::fs::write(protected.join("SKILL.md"), "OLD").unwrap();
        let c = ToolCtx {
            protected: vec![protected.clone()],
            file_policy: FilePolicy {
                writes: policy_writes,
                ..FilePolicy::default()
            },
            // The hermetic `ctx`'s EMPTY rules list — the shape a session
            // carries at the default posture (`default_rules()` is empty).
            ..ctx(&cwd)
        };
        let r = execute_tool(
            &c,
            "write",
            &json!({ "path": ".agents/skills/SKILL.md", "content": "PWNED" }),
        )
        .await;
        assert!(
            !r.is_error,
            "{policy_writes:?}: a NO-RULE protected write must DISPATCH — the gate \
             owns the row-3 ask, the executor must not refuse it: {}",
            text_of(&r)
        );
        assert_eq!(
            std::fs::read_to_string(protected.join("SKILL.md")).unwrap(),
            "PWNED",
            "{policy_writes:?}: the dispatched write must land"
        );
        // `edit` is a write, so it is dispatched too.
        let r = execute_tool(
            &c,
            "edit",
            &json!({ "path": ".agents/skills/SKILL.md", "old_text": "PWNED", "new_text": "EDITED" }),
        )
        .await;
        assert!(
            !r.is_error,
            "{policy_writes:?}: `edit` must dispatch too: {}",
            text_of(&r)
        );
        // And a NON-protected path in the same cwd is unaffected either way.
        let ok = execute_tool(
            &c,
            "write",
            &json!({ "path": "notes.txt", "content": "fine" }),
        )
        .await;
        assert!(!ok.is_error, "{policy_writes:?}: {}", text_of(&ok));
        // Restore for the next posture (the file is shared across iterations
        // only within this iteration's own fresh `cwd`, so this is hygiene).
        std::fs::write(protected.join("SKILL.md"), "OLD").unwrap();
    }
}

/// (T03 row 1, the EXECUTOR half, session-`cwd` variant) A USER `deny` rule
/// naming a protected dir INSIDE the session `cwd` is refused by the EXECUTOR
/// with the guard-membership wording — the same sentence the gate's
/// `deny_tool_result` produces, because the executor never trusts the gate to
/// have refused (and a direct `exec_write` call has no gate at all).
///
/// This is the successor of the old "refused under EVERY policy" claim: the
/// refusal is real, but it is now RULE-driven, so it needs a rule. The
/// `$HOME`-shaped sibling of this fixture is
/// `a_recreated_protected_dir_is_refused_by_the_executor_with_the_protected_wording`;
/// the GATE's half of the pair is `policy_gate.rs`'s
/// `a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording`
/// — neither can cover the other's refusal site.
#[tokio::test]
async fn a_user_deny_rule_refuses_a_write_inside_the_cwd_with_the_protected_wording() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("user-deny-exec");
    let protected = cwd.join(".agents/skills");
    std::fs::create_dir_all(&protected).unwrap();
    std::fs::write(protected.join("SKILL.md"), "OLD").unwrap();
    let deny = AccessRule {
        // The ABSOLUTE spelling here (this fixture's `protected` entry is the
        // cwd-relative dir, not a `$HOME` dir) — the `~` spelling and its
        // `home` dependency are pinned separately by
        // `a_tilde_rule_with_home_none_matches_nothing_and_with_a_real_home_matches_and_decides`.
        path: protected.to_string_lossy().into_owned(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        protected: vec![protected.clone()],
        // `default_rules()` ++ the user deny — the concatenation contract.
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(deny))
            .collect(),
        file_policy: FilePolicy {
            // `Allow` on purpose: the deny must win over the widening, and the
            // boundary must not be the thing that refuses here.
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": ".agents/skills/SKILL.md", "content": "PWNED" }),
    )
    .await;
    assert!(r.is_error, "the user-deny write must be refused: {r:?}");
    let text = text_of(&r).to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the refusal names the guard-membership reason, got {text:?}"
    );
    assert!(
        text.ends_with(PROTECTED_WRITE_ROUTE),
        "the wording is UNCHANGED from the pre-rule-model sentence (the two layers \
         must not drift, and the route is part of it): {text}"
    );
    assert_eq!(
        std::fs::read_to_string(protected.join("SKILL.md")).unwrap(),
        "OLD",
        "the file must be untouched"
    );
    // `edit` is a write, so the same rule refuses it.
    let r = execute_tool(
        &c,
        "edit",
        &json!({ "path": ".agents/skills/SKILL.md", "old_text": "OLD", "new_text": "PWNED" }),
    )
    .await;
    assert!(r.is_error, "`edit` must be refused too");
}

/// (T03 row 2, the executor) A rule `allow` naming a protected dir makes the
/// write LAND — the ONE place a rule overrides the old unconditional floor, and
/// what makes the guard prompt's `allow-directory` answer mean something.
///
/// HERMETIC like the rest of this module: the guard-membership entry is handed
/// in explicitly (never `protected_dirs()`, which reads `$HOME`), and the `~`
/// spelling of the rule is expanded against `ctx.home` — so the fixture also
/// proves the `home` field is the thing that makes a `~` rule work.
#[tokio::test]
async fn a_rule_allow_makes_the_write_land() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("allow-rule-cwd");
    // The protected dir lives INSIDE the cwd so the `Sandboxed`-tier boundary
    // admits it and only the guard membership could refuse — which is what the
    // allow rule then overrides.
    let guarded = cwd.join(".pi/agent/agents");
    std::fs::create_dir_all(cwd.join(".pi/agent")).unwrap();
    let guard_entry = guarded.canonicalize().unwrap_or_else(|_| guarded.clone());
    let allow = AccessRule {
        // The `~` spelling — the shape a `settings.json` rule carries, and the
        // reason `ctx.home` exists. `home` is the `cwd` below, so this expands
        // to exactly `guard_entry`.
        path: "~/.pi/agent/agents".to_string(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Allow,
    };
    let c = ToolCtx {
        protected: vec![guard_entry.clone()],
        // `default_rules()` ++ the user allow — the concatenation contract.
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(allow))
            .collect(),
        home: Some(cwd.to_path_buf()),
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    // Premise: the target really is under the guard-membership entry, so the
    // success below is the rule overriding the guard and not an unrelated pass.
    assert!(
        guarded.starts_with(&guard_entry),
        "premise: the target must sit under the guard entry {guard_entry:?}"
    );
    let target = guarded.join("new/SKILL.md");
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": target, "content": "ALLOWED" }),
    )
    .await;
    assert!(
        !r.is_error,
        "the allow rule must let the write land: {}",
        text_of(&r)
    );
    assert_eq!(
        std::fs::read_to_string(&target).unwrap_or_default(),
        "ALLOWED",
        "the file landed"
    );

    // THE CONTRAST (so the success above is the RULE and not the absence of a
    // guard): the SAME target and the SAME guard entry with no rule at all is
    // the row-3 shape the executor must dispatch — and with a `deny` rule it is
    // refused. Three rule states, three answers, one fixture.
    let no_rule = ToolCtx {
        rules: Vec::new(),
        ..ctx(&cwd)
    };
    let no_rule = ToolCtx {
        protected: vec![guard_entry.clone()],
        home: Some(cwd.to_path_buf()),
        file_policy: c.file_policy.clone(),
        ..no_rule
    };
    let r = execute_tool(
        &no_rule,
        "write",
        &json!({ "path": guarded.join("no-rule/SKILL.md"), "content": "X" }),
    )
    .await;
    assert!(
        !r.is_error,
        "a NO-RULE guard write dispatches (the gate owns its ask): {}",
        text_of(&r)
    );
    let deny = ToolCtx {
        rules: vec![AccessRule {
            path: format!("{}/.pi/agent/agents", cwd.display()),
            direction: RuleDirection::Write,
            decision: RuleDecision::Deny,
        }],
        protected: vec![guard_entry],
        home: Some(cwd.to_path_buf()),
        file_policy: c.file_policy.clone(),
        ..ctx(&cwd)
    };
    let r = execute_tool(
        &deny,
        "write",
        &json!({ "path": guarded.join("denied/SKILL.md"), "content": "X" }),
    )
    .await;
    assert!(r.is_error, "a DENY rule refuses the same shape: {r:?}");
}

/// (T03 row 1, non-protected) A rule `deny` on an ORDINARY dir refuses with the
/// rule-deny sentence — which names the RULE's path (the thing the user wrote
/// and can go edit), not "escapes the sandbox" (which would send the model
/// hunting for a boundary that is not the problem).
#[tokio::test]
async fn a_rule_deny_refuses_a_non_protected_dir_with_the_deny_wording() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("rule-deny-wording");
    let secret = cwd.join("secrets");
    std::fs::create_dir_all(&secret).unwrap();
    std::fs::write(secret.join("k.txt"), "OLD").unwrap();
    let deny = AccessRule {
        path: secret.to_string_lossy().into_owned(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        rules: vec![deny.clone()],
        // `Allow` so the boundary admits the path and only the rule can refuse.
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": "secrets/k.txt", "content": "PWNED" }),
    )
    .await;
    assert!(r.is_error, "the rule deny must refuse: {r:?}");
    let text = text_of(&r).to_string();
    assert!(text.contains("denied by the access rule"), "{text}");
    assert!(
        text.contains(&deny.path),
        "the refusal names the RULE the user wrote: {text}"
    );
    // The two refusal texts differ ONLY in the reason named — the protected
    // sentence must NOT appear here (this target is not on the guard list).
    assert!(
        !text.contains("protected agent-definition directory"),
        "a non-guard deny must not claim the guard: {text}"
    );
    assert_eq!(
        std::fs::read_to_string(secret.join("k.txt")).unwrap(),
        "OLD",
        "the file must be untouched"
    );
    // READ direction (rows 1–2 apply to a read too): the same-shaped rule with
    // `readWrite` refuses a read of the same dir.
    let rw = AccessRule {
        path: secret.to_string_lossy().into_owned(),
        direction: RuleDirection::ReadWrite,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        rules: vec![rw],
        file_policy: FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(&c, "read", &json!({ "path": "secrets/k.txt" })).await;
    assert!(r.is_error, "a deny rule refuses a READ too: {r:?}");
    assert!(
        text_of(&r).contains("denied by the access rule"),
        "{}",
        text_of(&r)
    );
}

/// (T03, the `home: None` consequence) A `~`-named rule with `ctx.home: None`
/// matches NOTHING — silently — and the SAME list with a real home matches and
/// decides. Every direct `ToolCtx` construction in the fixtures passes
/// `home: None`; this states exactly what that buys and costs.
#[test]
fn a_tilde_rule_with_home_none_matches_nothing_and_with_a_real_home_matches_and_decides() {
    use crate::agent::policy::{rule_for, RuleDecision, RuleDirection};
    let home = temp_cwd("home-none-consequence");
    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let target = home.path().join(".pi/agent/agents/SKILL.md");
    let tilde = AccessRule {
        path: "~/.pi/agent/agents".to_string(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    // `default_rules()` ++ the `~` rule (the concatenation contract).
    let rules = crate::agent::policy::default_rules()
        .into_iter()
        .chain(std::iter::once(tilde.clone()))
        .collect::<Vec<_>>();

    // `None` home: the rule is a SILENT no-op.
    assert_eq!(
        rule_for(&rules, RuleDirection::Write, &target, None),
        None,
        "with no home to expand `~` against, the rule matches nothing"
    );
    // A real home: the SAME list decides the SAME path.
    assert_eq!(
        rule_for(&rules, RuleDirection::Write, &target, Some(home.path()),),
        Some(&tilde),
        "with a real home the same rule matches and decides"
    );
    // The consequence in one sentence: with `None` the caller falls through to
    // the posture select / the row-3 guard ask; with a home the RULE answers.
}

/// (T03, spec: "rules never apply to `bash`") The exclusion works in BOTH
/// directions: a rule can neither WIDEN a confined shell (the load-bearing
/// `an_allow_rule_for_a_protected_dir_never_widens_a_sandboxed_bash`, which lives
/// in `sandbox.rs` — this module's scratch base is `/tmp`, which the sandbox
/// allow-list grants write on, so a confinement DENIAL cannot be observed here)
/// nor NARROW it. A `deny` rule naming everything on earth must not stop a
/// shell command, because `Sandbox::create` takes only the `cwd` and the shell
/// never consults `ctx.rules` — not for a grant and not for a refusal.
///
/// This is the half that can be pinned WITHOUT a Landlock kernel: no confinement
/// is involved at all (`shell: Allow` — the shell runs unconfined), and the
/// assertion is that a rule with maximum reach has zero effect on `bash`.
#[tokio::test]
async fn a_deny_rule_never_narrows_bash_because_rules_do_not_apply_to_the_shell() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("bash-not-narrowed");
    std::fs::write(cwd.join("f.txt"), "READABLE").unwrap();
    let deny_all = AccessRule {
        // The broadest rule the resolver can be handed: every canonical
        // absolute path on this machine is under it. NOT the literal `"/"` —
        // that is the unix root, and on Windows a lone `/` is only the current
        // drive's root in the cwd's own spelling, so it is not a component
        // prefix of the verbatim (`\\?\C:\…`) path the canonicalizer returns
        // and the CONTRAST below would find the `read` tool unrefused. Taking
        // the root from the fixture's own path keeps it on the same drive and
        // in the same spelling as the resolved target on all three platforms.
        path: crate::test_support::filesystem_root(cwd.path())
            .to_string_lossy()
            .into_owned(),
        direction: RuleDirection::ReadWrite,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(deny_all))
            .collect(),
        file_policy: FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Allow,
            rules: Vec::new(),
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(&c, "bash", &json!({ "command": "cat f.txt" })).await;
    assert!(
        !r.is_error,
        "a `deny` rule must not touch the shell — rules never apply to `bash`: {}",
        text_of(&r)
    );
    assert_eq!(text_of(&r), "READABLE");

    // CONTRAST (so the rule is not simply inert): the SAME list refuses the
    // `read` tool on the same file. One list, two answers, split by tool family.
    let c_read = ToolCtx {
        rules: c.rules.clone(),
        ..ctx(&cwd)
    };
    let r = execute_tool(&c_read, "read", &json!({ "path": "f.txt" })).await;
    assert!(
        r.is_error,
        "the same rule must refuse the read tool (the contrast)"
    );
}

/// The deny-list membership is a PREFIX check on the RESOLVED path, so a new
/// file created under a protected dir is inside the guard set too (the dir need
/// not hold the file yet). (T03) WHICH answer that produces is now rule-driven:
/// with a user `deny` it is refused, with no rule it dispatches — this fixture
/// pins the REFUSAL, on the rule row, and keeps the not-yet-existing-parent
/// shape that issue #92 is about.
#[tokio::test]
async fn a_new_file_under_a_denied_protected_dir_is_refused() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("tier-protect-new");
    let protected = cwd.join(".pi/agent/skills");
    std::fs::create_dir_all(&protected).unwrap();
    let deny = AccessRule {
        path: protected.to_string_lossy().into_owned(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        protected: vec![protected.clone()],
        rules: vec![deny],
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": ".pi/agent/skills/new/SKILL.md", "content": "PWNED" }),
    )
    .await;
    assert!(
        r.is_error,
        "a new file under a DENIED protected dir is refused"
    );
    assert!(!protected.join("new").exists(), "nothing is created");
}

/// The deny-list membership compares CANONICALLY, so a RAW (never-canonicalized)
/// protected entry still refuses a write whose `resolved` path reaches it
/// through a symlink.
///
/// (T03) WHICH refusal this is: the membership compare below is what the guard
/// wording keys off, and the refusal is reached through the USER-DENY row — a
/// no-rule protected write now dispatches (the gate owns its ask), so this
/// fixture carries the rule that makes it a deny. The RAW-vs-canonical bug this
/// test exists for is in `protected_refusal_text`'s compare, and it is unchanged
/// by that: a raw entry that missed the compare would produce the RULE-DENY
/// sentence instead of the protected one, which is the assertion below.
///
/// CI evidence for the bug this pins: on macOS `std::env::temp_dir()` lives
/// under `/var`, which is a symlink to `/private/var`, and on Windows
/// `canonicalize` returns the `\\?\C:\…` VERBATIM form while a
/// `temp_dir()`-joined entry is the plain `C:\…` form. Either way the raw
/// entry no longer prefixed the canonical `resolved`, `starts_with` missed,
/// and the protected write SUCCEEDED — i.e. the guard wording silently
/// no-opped for any caller that filled `ctx.protected` from a non-canonicalized
/// source. Linux temp dirs carry no such symlink, so the mismatch is
/// MANUFACTURED here with a symlinked dir (unix); the non-unix leg leans on the
/// platform's own raw-vs-canonical spelling.
#[tokio::test]
async fn a_raw_protected_entry_still_refuses_a_canonical_path_under_it() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let base = temp_cwd("protect-sym");
    let real = base.join("real");
    let skills = real.join(".agents/skills");
    std::fs::create_dir_all(&skills).unwrap();
    std::fs::write(skills.join("SKILL.md"), "OLD").unwrap();

    // The protected entry stays RAW, and the session `cwd` is the path the
    // model would join from — on unix that is the SYMLINK, so the
    // executor's canonical `resolved` comes back through `real` and shares
    // no textual prefix with the `link`-spelled entry.
    #[cfg(unix)]
    let (cwd, protected) = {
        std::os::unix::fs::symlink(&real, base.join("link")).unwrap();
        let link = base.join("link");
        let protected = link.join(".agents/skills");
        (link, protected)
    };
    #[cfg(not(unix))]
    let (cwd, protected) = {
        // No symlink privileges: `temp_dir()`'s own spelling is already
        // the raw one that `canonicalize` rewrites on Windows.
        (real.clone(), real.join(".agents/skills"))
    };

    // The fixture's PREMISE (so a platform where the two spellings agree
    // says so loudly instead of testing nothing): the entry handed to
    // `ctx.protected` is genuinely NOT canonical.
    assert_ne!(
        protected.canonicalize().unwrap(),
        protected,
        "the fixture must hand the deny-list a RAW entry, got {}",
        protected.display()
    );

    // The USER deny that makes this a refusal at all (row 1). Spelled through
    // the same RAW path, so the resolver's own lenient compare is exercised on
    // the same shape.
    let deny = AccessRule {
        path: protected.to_string_lossy().into_owned(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        protected: vec![protected.clone()],
        rules: vec![deny],
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": ".agents/skills/SKILL.md", "content": "PWNED" }),
    )
    .await;
    assert!(
        r.is_error,
        "a raw protected entry must still refuse the write"
    );
    let text = text_of(&r).to_string();
    // THE discriminating assertion: the refusal is the PROTECTED sentence, not
    // the generic rule-deny sentence — i.e. the membership compare fired through
    // the raw spelling.
    assert!(
        text.contains("protected agent-definition directory"),
        "the raw entry must still MATCH the canonical resolved path (a missed \
         compare would fall through to the rule-deny wording): {text}"
    );
    // The refusal stays HONEST about which entry it matched: the ORIGINAL
    // (raw) entry, not the canonicalized form the comparison used.
    assert!(
        text.contains(&protected.to_string_lossy().into_owned()),
        "the refusal must name the entry as the caller spelled it: {text}"
    );
    assert_eq!(
        std::fs::read_to_string(skills.join("SKILL.md")).unwrap(),
        "OLD",
        "the file behind the symlink must be untouched"
    );
}

/// The MIRROR of `a_raw_protected_entry_still_refuses_a_canonical_path_under_it`:
/// the ENTRIES are canonical (what production hands over, via
/// `boundary::protected_dirs`) and the COMPARED path is RAW. That is exactly
/// what the GATE does — `loop.rs::deny_tool_result` passes the model's `path`
/// argument verbatim — so this seam must canonicalize its ARGUMENT side too.
///
/// CI evidence (branch run 38702103063, the `f9b5c17`/`3fd702f` family):
/// macOS `a_gate_denial_of_a_protected_dir_uses_the_protected_wording` (since
/// renamed to `a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording`) failed
/// with `got "write: path escapes the session sandbox: …"` (Windows the same
/// with the `\\?\C:\…` verbatim spelling): the gate's raw argument shared no
/// prefix with its canonical entry, so the deny-list branch never fired and
/// the refusal fell through to the boundary's wording.
///
/// The target does NOT exist (a gate refusal is usually a brand-new write),
/// so a plain `canonicalize` on the argument fails — this is the shape
/// `canonicalize_lenient` (the peel `FsBackend::resolve` uses) exists for.
/// Linux `temp_dir()` is already canonical, so the mismatch is MANUFACTURED
/// with a `link -> real` symlink exactly as 3fd702f did: entries spelled
/// through `real` (canonical), the argument through `link` (raw).
#[cfg(unix)]
#[test]
fn a_raw_argument_still_refuses_against_canonical_protected_entries() {
    let base = temp_cwd("protect-gate-arg");
    let real = base.join("real");
    let skills = real.join(".agents/skills");
    std::fs::create_dir_all(&skills).unwrap();

    #[cfg(unix)]
    std::os::unix::fs::symlink(&real, base.join("link")).unwrap();
    let link = base.join("link");

    // The fixture's PREMISE (so a platform where the two spellings agree
    // fails loudly instead of testing nothing): the path the model would
    // send is genuinely NOT canonical.
    assert_ne!(
        link.canonicalize().unwrap(),
        link,
        "the argument must be spelled through a symlink, got {}",
        link.display()
    );

    // The ENTRIES as production builds them: canonical.
    let entries = vec![skills.clone()];
    // The ARGUMENT as the model sends it: the RAW spelling, under a
    // directory that does not exist yet (the gate's usual case).
    let raw = link.join(".agents/skills/pwn/SKILL.md");

    let refusal = protected_refusal_text(&entries, "write", &raw)
        .expect("a raw argument under a canonical protected entry must refuse");
    assert!(
        refusal.contains("protected agent-definition directory"),
        "{refusal}"
    );
    assert!(
        refusal.ends_with(PROTECTED_WRITE_ROUTE),
        "the gate-seam wording must carry the route too: {refusal}"
    );
    // The refusal names the entry the caller registered (canonical here) —
    // the SAME sentence the executor produces.
    assert!(
        refusal.contains(&skills.to_string_lossy().into_owned()),
        "the refusal must name the protected entry: {refusal}"
    );

    // IDEMPOTENCE (the executor callers): handing the already-canonical
    // form of the same path answers identically — canonical input
    // canonicalizes to itself, so nothing about that seam changed.
    let canon = skills.join("pwn/SKILL.md");
    let again = protected_refusal_text(&entries, "write", &canon)
        .expect("the canonical spelling refuses too (unchanged behaviour)");
    assert!(
        again.contains("protected agent-definition directory"),
        "{again}"
    );

    // And the fix does not turn the deny-list into a blanket refusal: a raw
    // path that resolves OUTSIDE the entries is still `None`.
    let outside = link.join("notes/todo.md");
    assert_eq!(
        protected_refusal_text(&entries, "write", &outside),
        None,
        "a non-protected raw path must not be refused"
    );
}

/// `HOME` restore guard for the session-snapshot tests below. A LOCAL copy
/// of the small `Drop` guard (the way `boundary.rs` and `commands/agents.rs`
/// each keep their own) rather than a new shared fixture — `exec/tests` had
/// no `RestoreHome` before #92 and these are its only `$HOME` readers.
struct RestoreHome(Option<std::ffi::OsString>);
impl Drop for RestoreHome {
    fn drop(&mut self) {
        match self.0.take() {
            // SAFETY: the `ENV_LOCK` is still held at drop time (this guard
            // is declared after the lock guard and outlives it in reverse);
            // no other thread mutates HOME concurrently.
            Some(v) => unsafe { std::env::set_var("HOME", v) },
            // SAFETY: as above.
            None => unsafe { std::env::remove_var("HOME") },
        }
    }
}

/// (issue #92 + T03 row 1, the EXECUTOR half of the churned pair) THE SCENARIO
/// THE ISSUE WAS FILED FOR, on the row that still refuses: a protected dir is
/// missing when the Session starts, the user recreates it mid-session, and the
/// Session that never restarted must still refuse to write into it — with the
/// protected wording, because the guard-membership entry survived the snapshot.
///
/// (T03 renamed this from
/// `a_recreated_protected_dir_is_refused_without_a_new_session` and added the
/// `rules`/`home` fields: with `default_rules()` EMPTY a no-rule protected write
/// now DISPATCHES (the gate owns its row-3 ask), so the refusal asserted here
/// needs the USER `deny` rule that makes it a row-1 deny. What #92 is about is
/// untouched and still discriminating: the WORDING. With a snapshot that dropped
/// the entry, the deny would fall through to the generic rule-deny sentence
/// instead of the protected one — which is what the wording assertion below
/// catches.)
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_recreated_protected_dir_is_refused_by_the_executor_with_the_protected_wording() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};

    let home = temp_cwd("recreated-home");
    let lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let restore = RestoreHome(original);
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span.
    unsafe { std::env::set_var("HOME", home.path()) };

    // `.pi/agent` EXISTS and `.pi/agent/agents` does NOT, so the lenient peel
    // lands on `.pi/agent` and the entry is canonical-with-one-trailing on
    // every platform (the same reason the expectation below is spelled
    // through `canonicalize()` and not the raw temp path: macOS `temp_dir()`
    // is under `/var` → `/private/var`).
    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let agents = home.path().join(".pi/agent/agents");

    // 1. The snapshot, built while the dir is MISSING — exactly what
    //    `AgentLoop::new` does for a real Session.
    let protected = crate::agent::boundary::protected_dirs();

    // 2. THE PREMISE (placed BEFORE the write attempt: a premise check
    //    belongs ahead of the thing it makes non-vacuous, and because
    //    `assert!` panics this ordering is what makes the HEAD red report the
    //    premise instead of `is_error`). It names the SPECIFIC entry — a
    //    `!protected.is_empty()` check passes on a list carrying the WRONG
    //    entries, and the test would then pass on a refusal for an unrelated
    //    reason while claiming to pin #92.
    assert!(
        protected.contains(&home.path().canonicalize().unwrap().join(".pi/agent/agents")),
        "the snapshot must carry the not-yet-existing dir — that is the premise: {protected:?}"
    );

    // 3. The user recreates it. No restart.
    std::fs::create_dir_all(&agents).unwrap();

    // 4. The frozen snapshot goes to the executor as production does, with the
    //    USER deny that makes this a refusal (`default_rules()` ++ user — the
    //    concatenation contract) and a REAL home so the `~` spelling expands.
    let cwd = temp_cwd("recreated-cwd");
    let deny = AccessRule {
        path: "~/.pi/agent/agents".to_string(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        protected,
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(deny))
            .collect(),
        home: Some(home.path().canonicalize().unwrap()),
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let target = agents.join("SKILL.md");
    let r = execute_tool(&c, "write", &json!({ "path": target, "content": "PWNED" })).await;

    // 5. Refused — on `is_error`, NOT on the text alone: a test that asserted
    //    only the wording stays green through this bug, which is the whole
    //    reason the bug survived.
    assert!(
        r.is_error,
        "a write into a protected dir recreated mid-session must be refused: {}",
        text_of(&r)
    );
    let text = text_of(&r).to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the refusal names the guard-membership reason — a snapshot that dropped \
         the entry would fall through to the generic rule-deny sentence: {text}"
    );
    assert!(
        text.ends_with(PROTECTED_WRITE_ROUTE),
        "the refusal must end with the sanctioned route: {text}"
    );
    assert!(
        !target.exists(),
        "the file must not have been created: {}",
        target.display()
    );

    drop((restore, lock));
}

/// (issue #92 + T03 row 1) THE FALSIFIER for the second half of the #92 fix —
/// the test that makes `access.rs`'s entry-side leniency load-bearing.
///
/// Same fixture as
/// [`a_recreated_protected_dir_is_refused_by_the_executor_with_the_protected_wording`]
/// MINUS the recreation: the snapshot is built while `$HOME/.pi/agent/agents` is
/// missing and the dir is NEVER created, so at COMPARE TIME the entry is still
/// missing. With change (1) alone (entries kept, comparison untouched) the entry
/// sits on the list and a strict `canonicalize()` on it fails, so it matches
/// NOTHING: the deny falls through to the generic rule-deny sentence and the file
/// lands — the exact silent no-op D3 was corrected for. So: the session-snapshot
/// tests pin change (1); THIS test pins change (2). A run where only change (1)
/// landed reddens exactly this one test.
///
/// (T03) The refusal needs a USER `deny` rule — a no-rule protected write now
/// dispatches (the gate owns its row-3 ask) — which is what makes the WORDING the
/// discriminating assertion rather than `is_error`: the deny exists either way,
/// and only the entry-side leniency decides WHICH sentence it gets.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_write_into_a_still_missing_protected_dir_is_refused() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};

    let home = temp_cwd("still-missing-home");
    let lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let restore = RestoreHome(original);
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span.
    unsafe { std::env::set_var("HOME", home.path()) };

    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let agents = home.path().join(".pi/agent/agents");

    let protected = crate::agent::boundary::protected_dirs();
    assert!(
        protected.contains(&home.path().canonicalize().unwrap().join(".pi/agent/agents")),
        "the snapshot must carry the not-yet-existing dir — that is the premise: {protected:?}"
    );

    let cwd = temp_cwd("still-missing-cwd");
    let deny = AccessRule {
        path: "~/.pi/agent/agents".to_string(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        protected,
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(deny))
            .collect(),
        home: Some(home.path().canonicalize().unwrap()),
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let target = agents.join("SKILL.md");
    let r = execute_tool(&c, "write", &json!({ "path": target, "content": "PWNED" })).await;

    assert!(
        r.is_error,
        "a write into a protected dir that is STILL missing must be refused: {}",
        text_of(&r)
    );
    let text = text_of(&r).to_string();
    assert!(
        text.contains("protected agent-definition directory"),
        "the still-missing entry must still MATCH (the entry-side lenient \
         compare): a strict `canonicalize()` there answers the deny with the \
         generic rule-deny sentence and lets the write land: {text}"
    );
    assert!(
        text.ends_with(PROTECTED_WRITE_ROUTE),
        "the refusal must end with the sanctioned route: {text}"
    );
    assert!(
        !agents.exists(),
        "nothing was created, so the peel's not-yet-exists premise held"
    );

    drop((restore, lock));
}

/// (issue #104) ADR 0081's missing pin, concretely: with `writes: Allow`, a
/// target that is the PARENT of a protected entry — `$HOME/.pi/agent/allowed.md`
/// while `$HOME/.pi/agent/agents` is missing — must not be refused, and the
/// file must land. An ancestor-peel mutation of the compare would
/// prefix-match `…/.pi/agent` against this target and refuse it — the
/// over-refusal #104 exists to pin. `!r.is_error` below is the assertion that
/// EXPRESSES the property; what keeps THIS half non-vacuous is its premise
/// (the entry really is on the snapshot with its full trailing spelling),
/// which reddens under the snapshot-peel variant of the mutation. `writes:
/// Allow` is essential — under `Sandboxed` the target leaves the `cwd` and
/// dies at the boundary check, which would defeat the fixture (ADR 0081
/// prescribes it for exactly this reason).
///
/// NO rules by design: `default_rules()` is empty and a no-rule protected write
/// DISPATCHES (the gate owns the row-3 ask), so this half pins the literal
/// case the issue describes. The falsifier half that reddens ON the
/// over-refusal (the vector that needs a rule to reach the compare) is the
/// sibling fixture `a_parent_deny_rule_does_not_move_the_guard_onto_the_parent_file`.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_write_into_a_protected_dirs_parent_stays_allowed() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};

    let home = temp_cwd("parent-home");
    let lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let restore = RestoreHome(original);
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span.
    unsafe { std::env::set_var("HOME", home.path()) };

    // `.pi/agent` EXISTS and `.pi/agent/agents` does NOT — the peel lands on
    // `.pi/agent`, so the frozen entry is canonical-with-one-trailing on every
    // platform (spelled through `canonicalize()` because macOS `temp_dir()`
    // is under `/var` → `/private/var`).
    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let home_canon = home.path().canonicalize().unwrap();
    let agents = home_canon.join(".pi/agent/agents");

    let protected = crate::agent::boundary::protected_dirs();
    assert!(
        protected.contains(&agents),
        "the snapshot must carry the not-yet-existing dir — that is the premise: {protected:?}"
    );

    let cwd = temp_cwd("parent-cwd");
    let c = ToolCtx {
        protected,
        // NO rules (`default_rules()` is EMPTY — the concatenation contract
        // has nothing appended). A rule here is deliberately absent: the
        // mutation-observable sibling test carries the `deny`, this half pins
        // the issue's literal case.
        rules: Vec::new(),
        home: Some(home_canon.clone()),
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let target = home_canon.join(".pi/agent/allowed.md");
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": target, "content": "ALLOWED" }),
    )
    .await;

    assert!(
        !r.is_error,
        "a write into a protected dir's own PARENT must stay allowed: {}",
        text_of(&r)
    );
    assert_eq!(
        std::fs::read_to_string(&target).unwrap_or_default(),
        "ALLOWED",
        "the file must land next to the protected entry"
    );

    drop((restore, lock));
}

/// (issue #104 + ADR 0081) THE FALSIFIER's red half: the assertion that makes
/// the ancestor-peel mutation observable. A USER `deny` rule spelling the
/// PARENT (`~/.pi/agent`) forces `write_rule_refusal` row 1 to reach
/// `protected_refusal` — and therefore to RUN the lenient compare that is the
/// mutation under test. The deny itself refuses on BOTH trees, so the
/// discriminator is the SENTENCE: correct code keeps the entry
/// `…/.pi/agent/agents` whole, which does NOT prefix-match
/// `…/.pi/agent/allowed.md`, so the target is OUTSIDE the guard-membership
/// list and the refusal is the RULE-DENY sentence. Under the ancestor-peel the
/// entry collapses to `…/.pi/agent`, which DOES prefix-match the parent file,
/// and the refusal becomes the PROTECTED-GUARD sentence — the wording
/// assertions below redden. Without the rule there is no row-1 path and the
/// compare never runs (a no-rule protected write dispatches by design), which
/// is why the issue's literal half lives in
/// `a_write_into_a_protected_dirs_parent_stays_allowed`.
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn a_parent_deny_rule_does_not_move_the_guard_onto_the_parent_file() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};

    let home = temp_cwd("parent-rule-home");
    let lock = crate::test_support::env_lock();
    let original = std::env::var_os("HOME");
    let restore = RestoreHome(original);
    // SAFETY: the `ENV_LOCK` is held for the whole set→assert→restore span.
    unsafe { std::env::set_var("HOME", home.path()) };

    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let home_canon = home.path().canonicalize().unwrap();
    let agents = home_canon.join(".pi/agent/agents");

    let protected = crate::agent::boundary::protected_dirs();
    assert!(
        protected.contains(&agents),
        "the snapshot must carry the not-yet-existing dir — that is the premise: {protected:?}"
    );

    let cwd = temp_cwd("parent-rule-cwd");
    let deny = AccessRule {
        path: "~/.pi/agent".to_string(),
        direction: RuleDirection::Write,
        decision: RuleDecision::Deny,
    };
    let c = ToolCtx {
        protected,
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(deny))
            .collect(),
        home: Some(home_canon.clone()),
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let target = home_canon.join(".pi/agent/allowed.md");
    // Premise (b): the target is OUTSIDE the guard-membership entry — the
    // exact relationship the compare under test must respect (a parent write,
    // not a write under the entry). THIS is the boundary the mutation breaks.
    assert!(
        !target.starts_with(&agents),
        "premise: the parent file must not sit under the entry itself"
    );
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": target, "content": "ALLOWED" }),
    )
    .await;

    assert!(
        r.is_error,
        "the user deny on the parent dir is a real denial — refusing: {}",
        text_of(&r)
    );
    let text = text_of(&r).to_string();
    assert!(
        text.contains("denied by the access rule"),
        "the parent FILE is outside the guard-membership list, so the refusal \
         must be the RULE-DENY sentence, not the protected-guard sentence: {text}"
    );
    assert!(
        !text.contains("protected agent-definition directory"),
        "the guard-membership compare must NOT fire for the parent file — an \
         ancestor-peel compare matches `…/.pi/agent` here and answers with the \
         protected sentence; THIS assertion reddens under that mutation: {text}"
    );
    assert!(
        !target.exists(),
        "the refusal must be honoured — nothing lands"
    );

    drop((restore, lock));
}

/// `bash` IGNORES the boundary (its `Sandboxed` tier is Landlock, Task 5):
/// a sandboxed read cannot reach a file that `bash` can happily cat, so
/// the boundary must not pretend to bound the shell.
#[tokio::test]
async fn bash_ignores_the_file_boundary() {
    let cwd = temp_cwd("tier-bash");
    let sibling = temp_cwd("tier-bash-sibling");
    std::fs::write(sibling.join("f.txt"), "FROM-OUTSIDE").unwrap();
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": format!("cat {}", sibling.join("f.txt").display()) }),
    )
    .await;
    assert!(
        !r.is_error,
        "bash is not boundary-sandboxed: {}",
        text_of(&r)
    );
    assert_eq!(text_of(&r), "FROM-OUTSIDE");
}

/// (T03, the `~`-residue hazard's OTHER side) A `~`-spelled `path` ARGUMENT is
/// a RELATIVE path to `FsBackend::resolve` — it joins to the session `cwd` and
/// creates a literal directory named `~` INSIDE the Space. It is therefore NOT
/// under the agent-definition dirs, and the guard correctly does not fire for it.
///
/// Recorded as a pin because the tempting "fix" (expand `~` in the gate before
/// resolving) would make the gate resolve the same argument to a DIFFERENT path
/// than the executor writes — the gate-vs-executor split-brain ADR 0029/0030's
/// one-canonicalizer rule exists to prevent, and the reason the seam pin in
/// `policy_gate.rs` drives a non-`~` raw argument and pins the `~` expansion on
/// the RULE side (where `rule_for` does expand, against `ToolCtx::home`).
#[tokio::test]
async fn a_tilde_spelled_write_argument_is_relative_and_does_not_reach_the_guard() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("tilde-arg-cwd");
    let guard_entry = cwd.join(".pi/agent/agents");
    std::fs::create_dir_all(&guard_entry).unwrap();
    let c = ToolCtx {
        protected: vec![guard_entry.clone()],
        home: Some(cwd.to_path_buf()),
        file_policy: FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        },
        ..ctx(&cwd)
    };
    let r = execute_tool(
        &c,
        "write",
        &json!({ "path": "~/.pi/agent/agents/SKILL.md", "content": "TILDE" }),
    )
    .await;
    // Dispatched (no rule, and NOT treated as a guard hit by the executor —
    // which no longer refuses no-rule writes at all), and it landed under a
    // literal `~` dir inside the Space, not under the guard entry.
    assert!(!r.is_error, "{}", text_of(&r));
    assert!(
        cwd.join("~/.pi/agent/agents/SKILL.md").exists(),
        "the `~` became a literal directory under the cwd — the resolution the \
         gate must mirror"
    );
    assert!(
        !guard_entry.join("SKILL.md").exists(),
        "and nothing landed under the guard entry"
    );
}

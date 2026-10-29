use std::path::{Path, PathBuf};

use crate::agent::fs_backend::FsBackend;
use crate::agent::tools::types::ToolCtx;

/// The read-direction backend for this context (ADR 0030): the executor's
/// roots are `boundary::executor_roots` over `ctx.boundary` and
/// `ctx.file_policy.reads`.
pub(crate) fn read_backend(ctx: &ToolCtx) -> FsBackend {
    FsBackend {
        roots: crate::agent::boundary::executor_roots(
            &ctx.cwd,
            &ctx.boundary,
            ctx.file_policy.reads,
        ),
    }
}

/// The write-direction backend: the boundary is the session `cwd` ONLY
/// (`boundary::write_roots` — space-level agent dirs are repo content and
/// stay writable as descendants of a repo-root cwd, and the user-level ones
/// are refused by `ctx.protected` regardless).
pub(crate) fn write_backend(ctx: &ToolCtx) -> FsBackend {
    FsBackend {
        roots: crate::agent::boundary::executor_roots(
            &ctx.cwd,
            std::slice::from_ref(&ctx.cwd),
            ctx.file_policy.writes,
        ),
    }
}

/// The reason + the route the protected-write refusal appends after the
/// refusal sentence: this is by design, and the sanctioned way to get the
/// file there is to stage it in the session and let the HUMAN move it into
/// place. Deliberately the ONLY route the refusal names — `sudo_exec` and the
/// shell can both reach these dirs and both are documented as intended
/// (ADR 0030: a guard-rail, not a security boundary), but the refusal is read
/// at the moment of decision, and naming an elevated route there is what sent
/// the reporting agent reaching for one over a markdown file (issue #33).
/// Pinned by `a_user_deny_rule_refuses_an_agent_dir_write_with_the_unchanged_protected_wording`
/// (executor half) and the gate's twin `a_gate_denial_of_a_deny_rule_uses_the_protected_wording`
/// — the two halves of the pair this spec renamed, whose old names asserted the
/// every-policy claim this change removes.
pub(crate) const PROTECTED_WRITE_ROUTE: &str = "This is by design: these directories are the agent's own instructions. Write the file under your session directory instead and ask the user to move it into place (the `ask` tool).";

/// The write DENY-LIST check (ADR 0030 Deviation 3): a resolved path under
/// one of `ctx.protected` (the user-level agent-definition dirs).
///
/// # What this is AFTER the permissions spec's round-3 correction
///
/// It is no longer an unconditional refusal. A write under one of the four
/// dirs with NO rule matching is the GATE's row-3 ASK (the guard prompt) — the
/// spec's single behavioural change, "today's hardcoded deny becomes an ask" —
/// and the executor must not refuse what the gate was entitled to ask about, or
/// the prompt would be a lie. This function is therefore the WORDING of a deny,
/// reached only once a rule has already decided `Deny` (precedence row 1: a
/// USER deny — the defaults are empty) and the target falls under the
/// guard-membership list. It is also the membership compare the gate's
/// `is_guard_path` mirrors, so the two layers cannot disagree about "under an
/// agent dir".
///
/// Independent of `roots` (it is a deny, not a boundary). `Some(message)` is
/// the refusal, worded like an `FsError`.
pub(crate) fn protected_refusal(ctx: &ToolCtx, tool: &str, resolved: &Path) -> Option<String> {
    protected_refusal_text(&ctx.protected, tool, resolved)
}

/// (T03 rows 1–2) The rule verdict for one RESOLVED WRITE target, consulted by
/// `exec_write` and `exec_edit` BEFORE any deny-list wording — the ONE resolver
/// the harness gate's twin calls. Quoting `policy.rs`'s module doc: *"per ADR
/// 0029/0030's one-canonicalizer rule the gate and the executor must both go
/// through [`crate::agent::policy::rule_for`], never a second matcher."* The
/// path fed here is the SAME `&resolved` the write itself uses
/// (`FsBackend::resolve` under the `write_backend(ctx)` roots, bound straight
/// after `validate`), and `ctx.home` is what expands a leading `~` in a rule
/// (`None` = no expansion, so a `~` rule matches nothing — the pinned
/// consequence).
///
/// `Some(text)` refuses; `None` DISPATCHES. Three cases reach `None`:
/// * an `Allow` rule (row 2) — proceed silently, past the guard-membership
///   wording: the ONE place a rule overrides the old unconditional floor, and
///   what makes an `allow-directory` answer mean something;
/// * NO rule matching, even under a protected dir (rows 3/4) — the GATE owns
///   that decision (row 3 → `Ask` → the user answered → dispatch; a Trusted
///   Space proceeds silently, "Trust suppresses the guard"). The executor's job
///   is to agree with the gate's RULE rows, never to second-guess its ask or
///   its allow;
/// * a rule that does not govern the `Write` direction.
pub(crate) fn write_rule_refusal(ctx: &ToolCtx, tool: &str, resolved: &Path) -> Option<String> {
    match crate::agent::policy::rule_for(
        &ctx.rules,
        crate::agent::policy::RuleDirection::Write,
        resolved,
        ctx.home.as_deref(),
    ) {
        // Row 1: the DENY is real; only the SENTENCE varies with whether the
        // target also falls under the guard-membership list.
        Some(rule) if rule.decision == crate::agent::policy::RuleDecision::Deny => {
            protected_refusal(ctx, tool, resolved)
                .or_else(|| Some(rule_deny_text(tool, rule, resolved)))
        }
        // Row 2 (allow) and rows 3/4 (no rule) both dispatch.
        _ => None,
    }
}

/// (T03 rows 1–2) The READ-direction twin of [`write_rule_refusal`], for
/// `read` / `find` / `grep` / `ls`. A `deny` rule refuses a read exactly as it
/// refuses a write (row 1 is about the access, not the direction); an `allow`
/// rule and the absence of one both proceed to the boundary check, which is
/// what the `Ask`/`Allow` widening already implements.
///
/// There is NO read-side guard ASK anywhere — not here and not at the gate
/// (`gate::is_guard_path` is write-only) — so this function can only refuse or
/// stay out of the way. A defaulted `path` (`"."` for `find`/`grep`/`ls`)
/// resolves inside the `cwd`, which no user rule names, so an absent-path call
/// matches nothing and falls to row 4.
pub(crate) fn read_rule_refusal(ctx: &ToolCtx, tool: &str, resolved: &Path) -> Option<String> {
    crate::agent::policy::rule_for(
        &ctx.rules,
        crate::agent::policy::RuleDirection::Read,
        resolved,
        ctx.home.as_deref(),
    )
    .filter(|rule| rule.decision == crate::agent::policy::RuleDecision::Deny)
    .map(|rule| rule_deny_text(tool, rule, resolved))
}

/// (T03 row 1) The refusal for a rule `Deny` whose target is NOT under the
/// guard-membership list (a user deny on an ordinary dir): the same "state the
/// reason" shape as [`protected_refusal_text`]'s sentence — a denied write must
/// explain, not say "escapes the sandbox" and send the model hunting for a
/// boundary that is not the problem — but naming the RULE's path, which is the
/// thing the user actually wrote and the thing they can go edit.
pub(crate) fn rule_deny_text(
    tool: &str,
    rule: &crate::agent::policy::AccessRule,
    resolved: &Path,
) -> String {
    format!(
        "{tool}: {} is denied by the access rule `{}` (direction {:?}) you configured — refusing. Edit the rule in Settings if this access should be allowed.",
        resolved.display(),
        rule.path,
        rule.direction,
    )
}

/// The refusal text itself, split out so the GATE (`loop.rs`) refuses a
/// protected write in EXACTLY the executor's words — two copies of that
/// sentence would drift, and a gate-level deny that reads differently from
/// the executor-level one is indistinguishable from a bug to a user.
///
/// The refusal carries more than the refusal: after the sentence that states
/// the floor, it appends the reason the floor exists and the one route the
/// guard-rail was designed to have (stage the file under the session directory
/// and let the human move it into place). Both live in
/// [`PROTECTED_WRITE_ROUTE`] so the executor-side and gate-side test families
/// assert the SAME value rather than two literals that can drift, and the gate
/// gets them for free — it calls this function, which is the whole reason the
/// split exists.
///
/// # The comparison is CANONICAL on both sides
///
/// `resolved` arrives canonical from the EXECUTOR callers (both go through
/// [`FsBackend::resolve`], which canonicalizes). A deny-list entry is NOT
/// necessarily canonical: production fills `ctx.protected` from
/// `boundary::protected_dirs()` — which canonicalizes — but nothing in the
/// type system enforces that, and any other caller (a test, a future session
/// bootstrap, a plugin seam) may hand raw paths. Comparing a canonical
/// `resolved` against a raw entry with a plain `starts_with` therefore silently
/// NO-OPS the deny-list, and "refused in EVERY policy" is exactly the property
/// that must not depend on the caller's hygiene. This is a real, observed
/// bypass, not a hypothetical: on macOS CI the temp dir sits under `/var`,
/// which is a symlink to `/private/var`, so `resolved` = `/private/var/…` never
/// prefixed the raw `/var/…` entry and the protected write SUCCEEDED (the same
/// tests reddened macOS and Windows CI, where a canonical `\\?\C:\…` verbatim
/// path never matched a raw `C:\…`).
///
/// So each entry goes through the SAME lenient canonicalization as the
/// compared path (issue #92). A strict `canonicalize()` on an entry that does
/// not exist yet simply FAILS, and a failed compare is a silent NON-MATCH —
/// which is the very hole `boundary::protected_dirs` now keeps the entry to
/// close, so the two halves are one change and neither alone is a fix.
/// Already-canonical entries canonicalize to themselves, so the production
/// path (and Linux CI) is unchanged. The REFUSAL still names the ORIGINAL
/// entry: the message must show the caller the directory it actually
/// registered, not a rewritten one — hence `find` over the caller's slice,
/// never a map onto the canonicalized form.
///
/// The recorded residue (#92, D3): an entry whose whole path cannot be
/// resolved at all comes back RAW, and a raw entry cannot prefix-match a
/// canonical resolved path — so on macOS (`/var` → `/private/var`) or Windows
/// (`\\?\C:\…`) a write into such a dir is NOT refused. That is strictly
/// better than the pre-#92 behaviour, where the dir was dropped from the list
/// outright, and nothing fixes it by peeling harder because there is nothing
/// to peel to. Pinned by `protected_dirs_keeps_an_unresolvable_entry_raw`.
///
/// # The COMPARED path is canonicalized the same way
///
/// The mirror case is just as much a mismatch: the GATE caller
/// (`harness/loop.rs::deny_tool_result`) hands the model's RAW argument here,
/// while its `protected` entries arrive CANONICAL from
/// [`crate::agent::boundary::protected_dirs`]. Canonicalizing the entries
/// cannot reach that at all — the mismatch is now on the other side, and a raw
/// `/var/…` never prefix-matches the canonical `/private/var/…` entry (macOS),
/// and a raw `C:\…` never matches the verbatim `\\?\C:\…` one (Windows).
///
/// What that mismatch costs is worth stating exactly, because the obvious
/// story is wrong. The DENY-LIST is no longer one of the things the policy
/// moves — and since the permissions spec's round-3 correction it is not one
/// of the things that moves AT ALL on its own: the executors consult
/// [`write_rule_refusal`] first, and this sentence is reached only for a rule
/// `Deny` (row 1) whose target falls under the list. What the widening moves is
/// the BOUNDARY check: `write_backend` builds its `roots` from
/// `ctx.file_policy.writes`, so under `Allow` the roots go unrestricted and
/// `validate` stops refusing.
///
/// The gate's copy exists for the DENY path, where it decides WHICH refusal the
/// model reads (the guard-membership sentence, or "escapes the sandbox"). So a
/// raw protected path reaching that seam slipped the membership MATCH and came
/// back worded as a boundary escape — the model sent hunting for a boundary that
/// is not the problem, which is precisely the drift the shared text exists to
/// prevent. That is the bug this canonicalization closes from the gate side.
///
/// So the compared path goes through
/// [`crate::agent::fs_backend::canonicalize_lenient`] — the peel-to-deepest-
/// existing-ancestor seam [`FsBackend::resolve`] itself uses, NOT a second
/// canonicalizer, because the gate must agree with the executor about where a
/// path lands. A not-yet-existing write target is exactly the shape the gate
/// sees, which is why a plain `canonicalize` was never going to work here.
///
/// Two directions of the remaining slop are worth naming:
/// * The re-appended components (below the deepest existing ancestor) are
///   compared as plain strings, so they cannot hide a symlink hop. At THIS
///   seam that only ever errs toward a FALSE POSITIVE — an over-refusal —
///   because the comparison's only power is to DENY (a deny-list match
///   refuses; nothing here can grant anything). Fail-closed in the safe
///   direction, and the symlink-at-an-existing-component case that `resolve`
///   rejects outright cannot make this seam ALLOW: it can only make a path
///   compare differently, and comparing differently here means missing the
///   entry, which is what the canonicalization just fixed.
/// * A path that cannot be resolved at all comes back unchanged, i.e. the raw
///   spelling is compared — the pre-fix behaviour for that (rare, unresolvable)
///   shape, never worse.
///
/// The EXECUTOR callers hand paths that `FsBackend::resolve` already
/// canonicalized: lenient-canonicalizing them again is IDEMPOTENT (a canonical
/// path canonicalizes to itself, so the peel stops on its first step and the
/// comparison is unchanged). Their behaviour, and Linux CI, are byte-identical.
pub(crate) fn protected_refusal_text(
    protected: &[PathBuf],
    tool: &str,
    resolved: &Path,
) -> Option<String> {
    let resolved = crate::agent::fs_backend::canonicalize_lenient(resolved);
    // BOTH sides lenient (issue #92): the entry may name a dir that does not
    // exist yet, and a strict `canonicalize()` on it fails — which under
    // `is_ok_and` matched NOTHING, silently no-opping the deny-list for
    // exactly the entry `protected_dirs()` now keeps. `hit` stays the
    // CALLER's entry so the message names the directory as registered.
    let hit = protected
        .iter()
        .find(|dir| resolved.starts_with(crate::agent::fs_backend::canonicalize_lenient(dir)))?;
    Some(format!(
        "{tool}: {} is a protected agent-definition directory ({}) — writing there is refused in every policy. {PROTECTED_WRITE_ROUTE}",
        resolved.display(),
        hit.display()
    ))
}

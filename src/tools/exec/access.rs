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

/// The write DENY-LIST check (ADR 0030 Deviation 3): a resolved path under
/// one of `ctx.protected` (the user-level agent-definition dirs) is refused
/// in EVERY policy — including `writes: Allow` — because a model that can
/// rewrite `~/.agents/skills/*/SKILL.md` rewrites its own instructions for
/// every future session. Independent of `roots` (it is a deny, not a
/// boundary). `Some(message)` is the refusal, worded like an `FsError`.
pub(crate) fn protected_refusal(ctx: &ToolCtx, tool: &str, resolved: &Path) -> Option<String> {
    protected_refusal_text(&ctx.protected, tool, resolved)
}

/// The refusal text itself, split out so the GATE (`loop.rs`) refuses a
/// protected write in EXACTLY the executor's words — two copies of that
/// sentence would drift, and a gate-level deny that reads differently from
/// the executor-level one is indistinguishable from a bug to a user.
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
/// So each entry is canonicalized at comparison time. An entry that cannot be
/// canonicalized (it does not exist) matches NOTHING — the same semantics
/// `boundary::canon` / `push_roots` already give the production list, where a
/// missing dir is simply not part of the deny-list. Already-canonical entries
/// canonicalize to themselves, so the production path (and Linux CI) is
/// unchanged. The REFUSAL still names the ORIGINAL entry: the message must
/// show the caller the directory it actually registered, not a rewritten one.
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
/// What that mismatch costs is worth stating exactly, because the obvious story
/// is wrong. The DENY-LIST is not one of the things the policy moves: both
/// executors call [`protected_refusal`] unconditionally, straight after
/// `validate` and with no test of `ctx.file_policy.writes`, so a protected write
/// is refused by the EXECUTOR in every mode — `writes: Allow` included. What the
/// widening moves is the BOUNDARY check and nothing else: `write_backend` builds
/// its `roots` from `ctx.file_policy.writes`, so under `Allow` the roots go
/// unrestricted and `validate` stops refusing, while the deny-list (independent
/// of `roots`) keeps refusing. And under `Allow` the gate's copy of this
/// function is not even reached — the verdict is `Allow`, so `dispatch_tool`
/// runs and `deny_tool_result`, which only answers a `Deny`, never fires.
///
/// The gate's copy exists for the DENY path, where it decides WHICH refusal the
/// model reads (the deny-list sentence, or "escapes the sandbox"). So a raw
/// protected path reaching that seam slipped the deny-list MATCH and came back
/// worded as a boundary escape — the model sent hunting for a boundary that is
/// not the problem, which is precisely the drift the shared text exists to
/// prevent. That is the bug this canonicalization closes from the gate side; the
/// executor-side refusal itself has been policy-independent since 3fd702f.
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
    let hit = protected.iter().find(|dir| {
        dir.canonicalize()
            .is_ok_and(|canon| resolved.starts_with(canon))
    })?;
    Some(format!(
        "{tool}: {} is a protected agent-definition directory ({}) — writing there is refused in every policy",
        resolved.display(),
        hit.display()
    ))
}

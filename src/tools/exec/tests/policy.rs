use serde_json::json;

use super::support::*;
use crate::agent::tools::exec::access::PROTECTED_WRITE_ROUTE;
use crate::agent::tools::exec::execute_tool;
#[cfg(unix)]
use crate::agent::tools::exec::protected_refusal_text;
use crate::agent::tools::types::ToolCtx;

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
        &ctx_policy(&cwd, policy),
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

    let _ = std::fs::remove_dir_all(&parent);
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
        boundary: vec![cwd.clone(), skill_dir.clone()],
        ..ctx(&cwd)
    };
    assert_eq!(boundary, vec![cwd.clone()]);

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

    let _ = std::fs::remove_dir_all(&cwd);
    let _ = std::fs::remove_dir_all(&skill_dir);
    let _ = std::fs::remove_dir_all(&elsewhere);
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
    let _ = std::fs::remove_dir_all(&parent);
}

/// The DENY-LIST is unconditional: a write whose resolved path is under a
/// `protected` dir is refused under EVERY policy — `Sandboxed` (where it
/// is also outside the boundary) AND `Allow` (where the boundary admits
/// it). A model that can rewrite `~/.agents/skills/*/SKILL.md` rewrites
/// its own instructions, so this is never a prompt and never widened.
#[tokio::test]
async fn write_into_a_protected_dir_is_refused_under_every_policy() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    for policy_writes in [
        AccessPolicy::Sandboxed,
        AccessPolicy::Ask,
        AccessPolicy::Allow,
    ] {
        let cwd = temp_cwd("tier-protect");
        // A PINNED protected dir INSIDE the cwd: the root check passes
        // for it, so only the deny-list can refuse the write (which is
        // exactly what makes this the deny-list's own test).
        let protected = cwd.join(".agents/skills");
        std::fs::create_dir_all(&protected).unwrap();
        std::fs::write(protected.join("SKILL.md"), "OLD").unwrap();
        let c = ToolCtx {
            protected: vec![protected.clone()],
            file_policy: FilePolicy {
                writes: policy_writes,
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
            "{policy_writes:?}: a write into a protected dir must be refused"
        );
        let text = text_of(&r).to_string();
        assert!(text.contains("protected"), "{policy_writes:?}: {text}");
        // (issue #33) The refusal must not just refuse — it must append the ONE
        // route that ends in a human, and that route must not name an elevated
        // or shell path. Pinned against the const, not a literal, so the
        // executor side and the gate side cannot drift apart.
        assert!(
            text.ends_with(PROTECTED_WRITE_ROUTE),
            "{policy_writes:?}: the refusal must end with the sanctioned route: {text}"
        );
        assert!(
            !PROTECTED_WRITE_ROUTE.contains("sudo")
                && !PROTECTED_WRITE_ROUTE.contains("bash")
                && !PROTECTED_WRITE_ROUTE.contains("shell"),
            "the refusal must not suggest an elevated or shell route: {PROTECTED_WRITE_ROUTE}"
        );
        assert_eq!(
            std::fs::read_to_string(protected.join("SKILL.md")).unwrap(),
            "OLD",
            "{policy_writes:?}: the file must be untouched"
        );
        // `edit` is a write, so the deny-list covers it too.
        let r = execute_tool(
            &c,
            "edit",
            &json!({ "path": ".agents/skills/SKILL.md", "old_text": "OLD", "new_text": "PWNED" }),
        )
        .await;
        assert!(r.is_error, "{policy_writes:?}: `edit` must be refused too");
        // And a NON-protected path in the same cwd is unaffected.
        let ok = execute_tool(
            &c,
            "write",
            &json!({ "path": "notes.txt", "content": "fine" }),
        )
        .await;
        assert!(!ok.is_error, "{policy_writes:?}: {}", text_of(&ok));
        let _ = std::fs::remove_dir_all(&cwd);
    }
}

/// The deny-list is a PREFIX check on the RESOLVED path, so a new file
/// created under a protected dir is refused too (the dir need not hold
/// the file yet).
#[tokio::test]
async fn write_a_new_file_under_a_protected_dir_is_refused() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("tier-protect-new");
    let protected = cwd.join(".pi/agent/skills");
    std::fs::create_dir_all(&protected).unwrap();
    let c = ToolCtx {
        protected: vec![protected.clone()],
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
    assert!(r.is_error, "a new file under a protected dir is refused");
    assert!(!protected.join("new").exists(), "nothing is created");
    let _ = std::fs::remove_dir_all(&cwd);
}

/// The deny-list compares CANONICALLY, so a RAW (never-canonicalized)
/// protected entry still refuses a write whose `resolved` path reaches it
/// through a symlink.
///
/// CI evidence for the bug this pins: on macOS `std::env::temp_dir()` lives
/// under `/var`, which is a symlink to `/private/var`, and on Windows
/// `canonicalize` returns the `\\?\C:\…` VERBATIM form while a
/// `temp_dir()`-joined entry is the plain `C:\…` form. Either way the raw
/// entry no longer prefixed the canonical `resolved`, `starts_with` missed,
/// and the protected write SUCCEEDED — i.e. the "refused in EVERY policy"
/// floor silently no-opped for any caller that filled `ctx.protected` from
/// a non-canonicalized source. Linux temp dirs carry no such symlink, so
/// the mismatch is MANUFACTURED here with a symlinked dir (unix); the
/// non-unix leg leans on the platform's own raw-vs-canonical spelling.
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

    let c = ToolCtx {
        protected: vec![protected.clone()],
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
    assert!(text.contains("protected"), "{text}");
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
    let _ = std::fs::remove_dir_all(&base);
}

/// The MIRROR of `a_raw_protected_entry_still_refuses_a_canonical_path_under_it`:
/// the ENTRIES are canonical (what production hands over, via
/// `boundary::protected_dirs`) and the COMPARED path is RAW. That is exactly
/// what the GATE does — `loop.rs::deny_tool_result` passes the model's `path`
/// argument verbatim — so this seam must canonicalize its ARGUMENT side too.
///
/// CI evidence (branch run 38702103063, the `f9b5c17`/`3fd702f` family):
/// macOS `a_gate_denial_of_a_protected_dir_uses_the_protected_wording` failed
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
    let _ = std::fs::remove_dir_all(&base);
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
    let _ = std::fs::remove_dir_all(&cwd);
    let _ = std::fs::remove_dir_all(&sibling);
}

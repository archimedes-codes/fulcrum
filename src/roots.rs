//! The ONE git-root discovery walk (Finding 10 — "a mirrored pair with four
//! same-named functions").
//!
//! [`space_roots_in`] is the single owner of the walk-up that `agents.rs` and
//! `skills.rs` each used to carry as a byte-identical copy: walk up from the
//! space path to the first ancestor holding a `.git` ENTRY (file OR dir —
//! `Path::exists`), build the level list INNERMOST-FIRST (the space itself
//! first, up to AND including the repo root), and emit both join targets at
//! every level, `.agents/…` first and `.pi/…` second. A space with no `.git`
//! anywhere up the chain contributes exactly one level.
//!
//! `agents::space_roots` and `skills::space_roots` are DELEGATORS around this
//! helper: each keeps its `pub fn` signature and its two join targets, and
//! contains no walk-up loop of its own. That is the whole of the merge — the
//! equivalence proof is that `agents.rs`'s
//! `space_roots_walks_up_to_the_repo_root` and `tests/skills.rs`'s
//! `space_roots_walks_up_to_the_git_marker` pass UNMODIFIED, before and after.
//!
//! # Which of the four same-named functions is NOT a mirror
//!
//! `parse_value` — and only `parse_value`. `skills.rs::parse_value` mirrors
//! pi's `yaml` reader (`parseFrontmatter`); `agents.rs::parse_value` mirrors
//! the hand-rolled regex reader in `pi-subagents/src/agents/frontmatter.ts`.
//! They mirror DIFFERENT upstream readers, so the two copies are LICENSED TO
//! DIFFER, and
//! `agents.rs::the_two_parse_value_copies_diverge_where_their_parity_targets_differ`
//! pins exactly where. Nothing here (and nothing in this task) shares, fixes,
//! or unifies either copy, nor the `: ` / NBSP guards beside them; a
//! "shared frontmatter reader" would be the over-merge that test exists to
//! catch. The other three (`space_roots`, `user_roots`, `discover_in_roots`)
//! ARE mirrors — of those, only `space_roots` is shared here, for the reasons
//! below.
//!
//! # What this file deliberately does NOT contain
//!
//! * **No root-level dedupe, no sort of the roots, no canonicalization.**
//!   The level list is produced once, innermost-first, and handed to the
//!   caller as-is. Both pins fix that exact sequence, and
//!   [`crate::agent::boundary::read_roots`] depends on the RAW spellings
//!   surviving until its own `push_roots` canonicalizes them — so
//!   `fs::canonicalize` appears nowhere in the walk, and the input is walked
//!   as the caller passed it (the "Do NOT canonicalize the input before
//!   walking" contract the two originals documented verbatim). Dedupe and
//!   sorting belong to each module's own `discover_in_roots`, which sorts the
//!   DISCOVERED ROWS, not the roots.
//! * **No `discover_in_roots`.** The two copies share the name-lowercase
//!   first-wins dedupe and the `sort_by_cached_key((name_lower, path))`
//!   SHAPE, but their pipelines do not: skills gates on `is_tag_safe_name`
//!   for `path`/`dir` (absent in agents — `agents.rs` says so at its own dedupe
//!   point) and enumerates through `find_skill_files`, while agents scans flat
//!   `*.md` entries and derives its fallback name from the RAW entry name (a
//!   differently-named symlink reads the target's bytes but keeps the link's
//!   name). Merging them would need a trait plus per-module listing/parse/
//!   validity callbacks — the over-extension this finding names as its own
//!   risk, for no named pin's benefit. They stay per-module.
//! * **No `user_roots`.** Both are trivial two-`join` wrappers over the single
//!   `pub(crate) skills::home_dir`, which is already the module-level
//!   single-owner the tree convention wants (`agents.rs` calls that same
//!   `home_dir`). Consolidating the wrappers would move nothing that was
//!   duplicated, and it has its own perturbation surface (`protected_dirs`'
//!   lenient path and the `~/.agents/…` / `~/.pi/agent/…` spelling pinned by
//!   `agents.rs::user_roots_reads_home`). Left alone, BY DECISION.
//! * **No absorption of `prompt.rs`'s walk — BY DECISION, not by omission.**
//!   `agent::harness::prompt::load_project_context` builds its levels
//!   innermost-first and consumes them REVERSED (outermost-first), so the
//!   OUTERMOST `AGENTS.md` loads first and wins the first-wins dedupe. That
//!   direction is pinned by `prompt.rs`'s `context_order_outermost_first`. The
//!   only behavioural difference between the two walks is graph ORDER, so
//!   unifying them requires an order parameter — the foreseeable extension is
//!   an `Order::{InnermostFirst, OutermostFirst}` argument on
//!   [`space_roots_in`] (or exposing the bounded ancestor list and letting
//!   each caller choose its direction). Until then `prompt.rs` keeps its own
//!   loop, and its `join(".git")` is the KNOWN second copy, not an oversight.
//!
//! No `#[cfg]` lives in this file: the walk is platform-free, so a
//! cross-target check proves the same code shape everywhere, and the runtime
//! differences (`\\?\`-shaped canonical inputs, a `.git` FILE for worktrees,
//! symlinked skill dirs) stay with the platform's own test leg.

use std::path::{Path, PathBuf};

/// The ONE git-root walk-up: every level from `space_path` up to and
/// including the first ancestor holding a `.git` entry, each level joined with
/// `first` and then `second`, INNERMOST-FIRST.
///
/// Returns the raw joined paths (never canonicalized — see the module docs);
/// the caller tags them with its own scope. `first`/`second` are the two
/// spellings at EVERY level, in emit order, which is why both live here: the
/// pinned shape is `level/first, level/second, parent/first, parent/second, …`,
/// NOT one target's whole walk followed by the other's.
///
/// * `!space_path.exists()` → an empty vec (a space that is not on the
///   filesystem has no roots).
/// * A `.git` entry that is a FILE (a worktree / submodule) stops the walk
///   exactly like a directory — the stop is `Path::exists`, never `is_dir` or
///   `metadata`, so an unreadable ancestor cannot change where the walk stops
///   (the originals swallowed permission errors the same way).
/// * The filesystem boundary is `cur.parent()` returning `None` or itself.
pub(crate) fn space_roots_in(space_path: &Path, first: &Path, second: &Path) -> Vec<PathBuf> {
    if !space_path.exists() {
        return Vec::new();
    }
    // Find the repo root: the first ancestor (including `space_path`
    // itself) that contains a `.git` entry; if none exists, only
    // `space_path` is a level.
    let mut repo_root: Option<PathBuf> = None;
    let mut cur = space_path.to_path_buf();
    loop {
        if cur.join(".git").exists() {
            repo_root = Some(cur.clone());
            break;
        }
        match cur.parent() {
            Some(p) if p != cur => {
                cur = p.to_path_buf();
            }
            _ => break,
        }
    }
    // Levels, innermost (the space itself) first, up to and including
    // the repo root.
    let mut levels = vec![space_path.to_path_buf()];
    if let Some(root) = repo_root {
        let mut cur = space_path.to_path_buf();
        while cur != root {
            let next = cur.parent().unwrap().to_path_buf();
            levels.push(next.clone());
            cur = next;
        }
    }
    let mut out = Vec::new();
    for level in &levels {
        out.push(level.join(first));
        out.push(level.join(second));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// The join targets the two delegators pass, spelled as they pass them.
    /// (`&str`, not `&Path`: `Path::new` is not `const` on this toolchain, and
    /// `&str` still satisfies every `join` and `AsRef<Path>` call site.)
    const AGENTS: &str = ".agents/agents";
    const PI_AGENTS: &str = ".pi/agents";

    /// A `.git` DIR at the scratch root, so the walk is BOUNDED at the scratch
    /// (a stray `.git` in a developer's real ancestors must not decide how many
    /// levels a test sees — the same reason `agents.rs::scratch` exists).
    fn scratch_with_git_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("a temp dir");
        fs::create_dir_all(tmp.path().join(".git")).expect("a `.git` dir at the scratch root");
        tmp
    }

    /// The pinned shape, stated at the helper itself: innermost-first, and
    /// BOTH spellings at EVERY level (interleaved, not one target's whole
    /// walk followed by the other's). `agents.rs`'s
    /// `space_roots_walks_up_to_the_repo_root` pins the same sequence through
    /// the delegator; this pins it at the shared owner so a change here has to
    /// answer something even if a caller's test is edited away.
    #[test]
    fn levels_are_innermost_first_with_both_joins_at_every_level() {
        let root = scratch_with_git_dir();
        let space = root.path().join("a/b");
        fs::create_dir_all(&space).unwrap();

        let out = space_roots_in(&space, Path::new(AGENTS), Path::new(PI_AGENTS));

        // Raw joined paths (NO `canon`): the expected values are built with
        // plain `join`, exactly as the two pins build theirs.
        assert_eq!(
            out,
            vec![
                space.join(AGENTS),
                space.join(PI_AGENTS),
                root.path().join("a").join(AGENTS),
                root.path().join("a").join(PI_AGENTS),
                root.path().join(AGENTS),
                root.path().join(PI_AGENTS),
            ],
            "{out:?}"
        );
    }

    /// A `.git` FILE stops the walk just as a `.git` DIR does — the stop is
    /// `Path::exists`, which is what makes a worktree or submodule root a
    /// boundary. Neither pin covers the file spelling (both build a dir), so
    /// this is the case the shared owner has to hold on its own.
    #[test]
    fn a_git_file_stops_the_walk_and_is_included() {
        let root = tempfile::tempdir().expect("a temp dir");
        fs::write(root.path().join(".git"), "gitdir: /elsewhere\n").unwrap();
        let space = root.path().join("sub");
        fs::create_dir_all(&space).unwrap();

        let out = space_roots_in(&space, Path::new(AGENTS), Path::new(PI_AGENTS));

        assert_eq!(
            out,
            vec![
                space.join(AGENTS),
                space.join(PI_AGENTS),
                root.path().join(AGENTS),
                root.path().join(PI_AGENTS),
            ],
            "the `.git` FILE's own dir is the last level: {out:?}"
        );
    }

    /// The space itself is a `.git` entry: one level, two joins, and the walk
    /// does not climb past it.
    #[test]
    fn a_space_that_is_the_repo_root_contributes_one_level() {
        let root = scratch_with_git_dir();

        let out = space_roots_in(root.path(), Path::new(AGENTS), Path::new(PI_AGENTS));

        assert_eq!(
            out,
            vec![root.path().join(AGENTS), root.path().join(PI_AGENTS)],
            "{out:?}"
        );
    }

    /// No `.git` anywhere up the chain: `space_path` ALONE is the level (two
    /// entries). `tests/skills.rs` pins the same `len()==2` case; the point
    /// here is that a missing marker is not an empty result — a Space with no
    /// git history still gets its own roots scanned.
    #[test]
    fn no_git_marker_anywhere_yields_a_single_level() {
        // A scratch with NO `.git` of its own. The walk climbs to the
        // filesystem root looking for a marker, so `len()==2` is only assertable
        // because no ancestor of the ambient temp dir holds one — the SAME
        // precondition `tests/skills.rs`'s `bare_roots.len() == 2` already
        // assumes (it uses the same `tempfile::tempdir()` base), so it is a
        // carried-over precondition here, not a new fragility.
        let bare = tempfile::tempdir().expect("a temp dir");
        let space = bare.path().join("sp");
        fs::create_dir_all(&space).unwrap();

        let out = space_roots_in(&space, Path::new(AGENTS), Path::new(PI_AGENTS));

        assert_eq!(
            out,
            vec![space.join(AGENTS), space.join(PI_AGENTS)],
            "a Space with no git history still gets its own two roots: {out:?}"
        );
    }

    /// A space that is not on the filesystem has NO roots — the guard the
    /// originals carried (`Path::exists`, so a permission-refused path walks
    /// rather than erroring, exactly as before).
    #[test]
    fn a_missing_space_path_has_no_roots() {
        let bare = tempfile::tempdir().expect("a temp dir");
        let missing = bare.path().join("does-not-exist");

        assert!(space_roots_in(&missing, Path::new(AGENTS), Path::new(PI_AGENTS)).is_empty());
    }

    /// **The raw-spelling contract.** `agent::boundary::read_roots` canonicalizes
    /// the roots itself (`push_roots`) and relies on the walk handing back what
    /// it was given; a `canonicalize` inside the walk would rewrite the spelling
    /// under it (and on Windows turn a plain path into a `\\?\`-prefixed one).
    /// A `..` component is the portable way to see the difference — a symlink
    /// would need a `#[cfg]` pair, and this file carries none. The first level
    /// IS the input, so its joins must carry the input's spelling verbatim.
    #[test]
    fn the_walk_never_canonicalizes_what_it_was_given() {
        let root = scratch_with_git_dir();
        let real = root.path().join("a/b");
        fs::create_dir_all(&real).unwrap();
        // `…/a/b/../b` resolves to `real` at the OS level (so `exists()` says
        // yes and the walk runs), but its TEXT is not the canonical spelling.
        let spelled = root.path().join("a/b/../b");
        assert!(
            spelled.exists(),
            "the `..` spelling must resolve: {spelled:?}"
        );
        assert_ne!(
            spelled,
            fs::canonicalize(&spelled).unwrap(),
            "the fixture is only meaningful if the input is NOT already canonical"
        );

        let out = space_roots_in(&spelled, Path::new(AGENTS), Path::new(PI_AGENTS));

        assert_eq!(out[0], spelled.join(AGENTS), "{out:?}");
        assert_eq!(out[1], spelled.join(PI_AGENTS), "{out:?}");
    }

    /// The two join targets are INDEPENDENT of each other and of the walk: a
    /// caller that names `.agents/skills` / `.pi/skills` gets exactly the same
    /// level sequence with the skill spellings, which is what lets both
    /// delegators share one loop.
    #[test]
    fn the_join_targets_are_the_callers_spelling_not_a_hardcoded_pair() {
        let root = scratch_with_git_dir();
        let space = root.path().join("a");
        fs::create_dir_all(&space).unwrap();

        let skills = space_roots_in(&space, Path::new(".agents/skills"), Path::new(".pi/skills"));

        assert_eq!(
            skills,
            vec![
                space.join(".agents/skills"),
                space.join(".pi/skills"),
                root.path().join(".agents/skills"),
                root.path().join(".pi/skills"),
            ],
            "{skills:?}"
        );
    }
}

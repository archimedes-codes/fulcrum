//! A sandboxed client-side file I/O backend (the pre-approval read path).
//!
//! The desktop validates every path a session asks for against the
//! session's root SET before any I/O happens. The validation is done on
//! the *canonicalized* path, which resolves `..` components and symlinks,
//! so neither `../..` traversal nor a symlink that points outside the
//! roots can escape the sandbox.
//!
//! Why a SET (ADR 0030): a session's read boundary is not just its `cwd` —
//! the discovery roots (`~/.agents/skills`, `<repo>/.agents/skills`, …) sit
//! outside the `cwd` yet must be readable. Containment is therefore "under
//! ANY root", and `roots[0]` is the relative-path join base (always the
//! canonical `cwd` for a session). An EMPTY root set is an error, never
//! "no limits" — the unrestricted floor is an explicit floor ROOT SET
//! (`agent::boundary::unrestricted_floor`: `/` on unix, the mounted drive
//! roots on Windows). Containment below stays plain `starts_with` against
//! canonicalized roots — it knows nothing about floors.
//!
//! `FsError` is local to this module (it used to ride on the protocol
//! layer's error type, which died with the pi-RPC swap): `Io` for
//! filesystem failures, `PathEscape` for a path that escaped the sandbox.

use std::fs;
use std::path::{Path, PathBuf};

/// A filesystem failure in the sandboxed backend: an I/O error, or a path
/// that escaped the session's sandbox roots.
#[derive(Debug, thiserror::Error, serde::Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FsError {
    /// A filesystem operation failed (I/O error, permission, encoding, …).
    #[error("file operation failed: {detail}")]
    Io { detail: String },

    /// The requested path escaped every sandbox root (via `..`, a symlink,
    /// or an absolute path outside all of them).
    #[error("path escapes the session sandbox: {path}")]
    PathEscape { path: String },
}

/// A sandboxed view over a SET of directory trees.
///
/// No read or write may ever touch a file that does not live under one of
/// the `roots`. `roots[0]` is the join base for relative paths (the session
/// `cwd`), so the ORDER matters — see [`crate::agent::boundary`].
#[derive(Debug, Clone)]
pub struct FsBackend {
    /// The allowed trees. `roots[0]` is what a RELATIVE path joins to; a
    /// path is in bounds when it is under ANY root. Empty = nothing is in
    /// bounds (fail closed — see [`FsBackend::resolve`]).
    pub roots: Vec<PathBuf>,
}

impl FsBackend {
    /// Read a text file, rejecting any path that escapes the sandbox.
    pub fn read(&self, path: &Path) -> Result<String, FsError> {
        let resolved = self.resolve(path)?;
        fs::read_to_string(&resolved).map_err(|e| FsError::Io {
            detail: e.to_string(),
        })
    }

    /// Write a text file, rejecting any path that escapes the sandbox.
    ///
    /// Parent directories that do not yet exist are created (still inside the
    /// sandbox) so the agent can write to a fresh path.
    pub fn write(&self, path: &Path, content: &str) -> Result<(), FsError> {
        let resolved = self.resolve(path)?;
        if let Some(parent) = resolved.parent() {
            fs::create_dir_all(parent).map_err(|e| FsError::Io {
                detail: e.to_string(),
            })?;
        }
        fs::write(&resolved, content).map_err(|e| FsError::Io {
            detail: e.to_string(),
        })
    }

    /// Canonicalize `path` and verify it stays under any root, returning the
    /// resolved path WITHOUT performing any I/O on the target itself —
    /// the validation seam the tool executors (`ls`/`find`/`grep`/image
    /// `read`) use. Same semantics as the `read`/`write` validation.
    pub fn validate(&self, path: &Path) -> Result<PathBuf, FsError> {
        self.resolve(path)
    }

    /// Read a file's raw bytes, rejecting any path that escapes the sandbox.
    ///
    /// (The existing `read` is `read_to_string`-only; this is the seam for
    /// binary content such as image files.)
    pub fn read_bytes(&self, path: &Path) -> Result<Vec<u8>, FsError> {
        let resolved = self.resolve(path)?;
        fs::read(&resolved).map_err(|e| FsError::Io {
            detail: e.to_string(),
        })
    }

    /// Canonicalize `path` and verify it stays under any root.
    ///
    /// For a path that does not exist yet (a write target), the deepest
    /// existing ancestor is canonicalized and the remaining components are
    /// appended, so a brand-new file can still be validated.
    ///
    /// An EMPTY `roots` is an `Io` error (fail closed): "no roots" must never
    /// degenerate into "everything is in bounds". A root that fails to
    /// canonicalize (a missing dir) simply never matches — it is not an error
    /// (the boundary is allowed to name dirs that do not exist).
    pub(crate) fn resolve(&self, path: &Path) -> Result<PathBuf, FsError> {
        if self.roots.is_empty() {
            return Err(FsError::Io {
                detail: "no sandbox roots configured (an empty root set is not unrestricted)"
                    .to_string(),
            });
        }

        // Absolute paths are used as-is; relative paths are joined to the
        // FIRST root (the canonical `cwd` — the ordering requirement).
        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.roots[0].join(path)
        };

        // Fast path (a path that exists whole) AND slow path (peel to the
        // deepest existing ancestor, canonicalize it, re-append what led to
        // the missing target) both live in [`peel_to_existing`] — ONE
        // implementation of the peel rules, so no second canonicalizer can
        // resolve `..` / a symlink differently from this one.
        let peel = peel_to_existing(&candidate)?;
        if peel.trailing.is_empty() {
            return self.check_under_roots(&peel.canon);
        }

        // Slow path: validate the canonical ancestor, then re-append the
        // components that led to the (missing) target.
        self.check_under_roots(&peel.canon)?;
        // The "not yet exists" premise must hold for EVERY trailing
        // component, not just the deepest canonicalizable ancestor: a
        // component that EXISTS here (it could not be canonicalized as part
        // of the full path, so it is a dangling/broken symlink) would be
        // re-appended as-is and FOLLOWED by the subsequent
        // `fs::write`/`create_dir_all` — creating the target OUTSIDE the
        // sandbox. Reject it.
        let mut probe = peel.canon.clone();
        for component in peel.trailing.iter().rev() {
            probe.push(component);
            if fs::symlink_metadata(&probe).is_ok() {
                return Err(FsError::PathEscape {
                    path: probe.display().to_string(),
                });
            }
        }
        Ok(reapply(peel.canon, &peel.trailing))
    }

    /// Confirm `canon` (already canonicalized) is a root or a descendant of
    /// ANY root. Each root is canonicalized HERE (at check time, not at
    /// construction), so a raw-constructed backend behaves the same as one
    /// whose roots came from `boundary::*`; a root that cannot be
    /// canonicalized does not match.
    pub(crate) fn check_under_roots(&self, canon: &Path) -> Result<PathBuf, FsError> {
        for root in &self.roots {
            // A root that fails to canonicalize (a missing dir) is not part
            // of the boundary for this check.
            let Ok(resolved) = root.canonicalize() else {
                continue;
            };
            if canon.starts_with(&resolved) {
                return Ok(canon.to_path_buf());
            }
        }
        Err(FsError::PathEscape {
            path: canon.display().to_string(),
        })
    }
}

/// The result of [`peel_to_existing`]: the canonical form of the deepest
/// EXISTING ancestor of a path, plus the components that lead from it to the
/// (possibly missing) target.
///
/// Empty `trailing` means the path itself existed, so `canon` IS its
/// canonicalization (the fast path).
struct Peel {
    /// The canonicalized deepest existing ancestor.
    canon: PathBuf,
    /// The components between `canon` and the original path, collected while
    /// walking UP, so the INNERMOST one is last — see [`reapply`].
    trailing: Vec<std::ffi::OsString>,
}

/// Re-append a peel's `trailing` components to `base`, outermost first (they
/// were collected while walking UP, so the walk back DOWN is `.rev()`).
fn reapply(base: PathBuf, trailing: &[std::ffi::OsString]) -> PathBuf {
    let mut result = base;
    for component in trailing.iter().rev() {
        result.push(component);
    }
    result
}

/// Canonicalize `path`, or — when it does not exist yet — canonicalize its
/// deepest existing ancestor and re-append the components that lead to the
/// missing target.
///
/// THE ONE implementation of the peel rules (the shape [`FsBackend::resolve`]
/// has always used): anything else that has to canonicalize a
/// not-yet-existing path MUST go through here, because a second canonicalizer
/// that peeled differently would disagree with the executor about where a path
/// lands — the classic gate-vs-executor split-brain.
///
/// Deliberately root-free: it does not know about containment, only about
/// resolution, which is why `resolve` layers [`FsBackend::check_under_roots`]
/// (and its dangling-symlink probe) on top of its result.
///
/// # Errors
///
/// * `PathEscape` for a trailing `..` under a missing parent — `file_name()`
///   is `None` for `..`, so silently dropping it would turn
///   `root/newdir/../file.txt` (missing `newdir`) into
///   `root/newdir/file.txt` instead of `root/file.txt`: the caller would
///   report one path and land another. (The exists-whole path resolves `..`
///   correctly via `canonicalize`, so only the peel needs this.)
/// * `Io` when no ancestor exists at all (the walk reached the filesystem
///   root) — the path is not under any real directory.
///
/// Note what it does NOT do: it does not re-check the re-appended components
/// for existence. A component that EXISTS here means the full path could not
/// be canonicalized for another reason (a dangling symlink), and following it
/// would land the caller outside wherever `canon` points. `resolve` rejects
/// that (its probe); a non-I/O caller whose only move is a string comparison
/// cannot be fooled into an UNSAFE allow by it — see
/// [`crate::agent::tools::exec::protected_refusal_text`], the one caller that
/// leans on this without the probe.
fn peel_to_existing(path: &Path) -> Result<Peel, FsError> {
    let mut ancestor = path.to_path_buf();
    let mut trailing: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(canon) => return Ok(Peel { canon, trailing }),
            Err(_) => {
                // Peel off the last component and try the parent. Own it as
                // an OsString so it does not borrow `ancestor`.
                let last = ancestor.file_name().map(|n| n.to_os_string());
                if let Some(last) = last {
                    trailing.push(last);
                } else if ancestor.ends_with("..") {
                    return Err(FsError::PathEscape {
                        path: ancestor.display().to_string(),
                    });
                }
                if !ancestor.pop() {
                    // Reached the filesystem root without finding an existing
                    // ancestor: the path is not under a real dir.
                    return Err(FsError::Io {
                        detail: "path does not exist under the sandbox root".to_string(),
                    });
                }
            }
        }
    }
}

/// Canonicalize `path` as far as the filesystem allows: its own canonical form
/// when it exists, otherwise the canonical form of its deepest existing
/// ancestor with the remaining components re-appended (the SAME peel rules
/// [`FsBackend::resolve`] runs — one implementation, see [`peel_to_existing`]).
///
/// A path that cannot be resolved at all (no existing ancestor, or a `..`
/// under a missing parent) comes back UNCHANGED: the caller then compares the
/// raw spelling, which is the best available answer for a caller that cannot
/// fail (unlike `resolve`, this has no error to return).
///
/// Idempotent on canonical input: a path that is already canonical
/// canonicalizes to itself, so passing a resolved path through here is a
/// no-op — which is what makes it safe to call from a seam shared by callers
/// that hand over raw paths and callers that hand over resolved ones.
pub(crate) fn canonicalize_lenient(path: &Path) -> PathBuf {
    match peel_to_existing(path) {
        Ok(peel) if peel.trailing.is_empty() => peel.canon,
        Ok(peel) => reapply(peel.canon, &peel.trailing),
        Err(_) => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh temp dir as the sandbox `root` (returns the root + a unique
    /// outside-sandbox target path that does NOT exist).
    ///
    /// THE ROOT IS CANONICALIZED, the way the production boundary hands one
    /// over: `boundary::read_roots`/`write_roots` canonicalize everything they
    /// give an `FsBackend`, so a raw fixture root was a shape the app never
    /// builds. It stayed green on Linux only because Linux's `temp_dir()` is
    /// already canonical; everywhere else it is not, and the equality
    /// assertions then compare a RAW path against the CANONICAL path `resolve`
    /// returns (`check_under_roots` hands back the canonicalized candidate):
    /// macOS `/var` → `/private/var`, Windows `C:\Users\…\Temp` →
    /// `\\?\C:\Users\…\Temp`. Canonical here makes `resolved == f` hold on all
    /// three because `f` is derived from the root, and it makes two fixtures
    /// mean what they say rather than accidentally pass:
    ///
    /// * `resolve_dotdot_in_missing_parent_rejected` needed it. With a RAW
    ///   (non-verbatim) Windows root the candidate
    ///   `C:\…\fsb-esc-slow-<uuid>\newdir/../a.txt` is a path `GetFullPathNameW`
    ///   resolves LEXICALLY, so the fast canonicalize path SUCCEEDED (`a.txt`
    ///   exists) and the slow path's `..` rejection was never reached — the
    ///   `unwrap_err` fired. Under the canonical `\\?\` root verbatim paths
    ///   disable that lexical resolution, `newdir` is missing, canonicalize
    ///   fails, and the peel rejects the `..` as the test claims.
    /// * `outside` stays RAW on purpose: it is only ever used to create/read a
    ///   target outside the sandbox and to assert `!outside.exists()` — never
    ///   compared for equality with a returned path — and a raw `/var/…` link
    ///   target is the better fixture for the escape tests, since escaping the
    ///   sandbox THROUGH a symlink is what they are about.
    fn temp_root(tag: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("fsb-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();
        let outside =
            std::env::temp_dir().join(format!("fsb-outside-{tag}-{}", uuid::Uuid::new_v4()));
        (dir, outside)
    }

    #[test]
    fn resolve_existing_file_under_root() {
        let (root, _outside) = temp_root("ok");
        let f = root.join("a.txt");
        fs::write(&f, "x").unwrap();
        let backend = FsBackend {
            roots: vec![root.clone()],
        };
        let resolved = backend.validate(Path::new("a.txt")).unwrap();
        assert_eq!(resolved, f);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_missing_file_under_root_ok() {
        let (root, _outside) = temp_root("missing");
        let backend = FsBackend {
            roots: vec![root.clone()],
        };
        // A brand-new file (and its new parents) validate fine.
        let resolved = backend.validate(Path::new("nested/deep/a.txt")).unwrap();
        assert_eq!(resolved, root.join("nested/deep/a.txt"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_dotdot_escape_rejected() {
        let (root, _outside) = temp_root("esc");
        fs::write(root.join("a.txt"), "x").unwrap();
        let backend = FsBackend {
            roots: vec![root.clone()],
        };
        let err = backend.validate(Path::new("../a.txt")).unwrap_err();
        assert!(matches!(err, FsError::PathEscape { .. }));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A trailing `..` in the SLOW path (the target's parent does not
    /// exist — the `..` cannot be canonicalized away) is REJECTED
    /// (`PathEscape`), not silently DROPPED: pre-fix
    /// `resolve("newdir/../file.txt")` (with `newdir` missing) returned
    /// `root/newdir/file.txt` instead of `root/file.txt` (`file_name()`
    /// is `None` for `..`, so the peel dropped it) — the tool would
    /// report one path and land another.
    #[test]
    fn resolve_dotdot_in_missing_parent_rejected() {
        let (root, _outside) = temp_root("esc-slow");
        fs::write(root.join("a.txt"), "x").unwrap();
        let backend = FsBackend {
            roots: vec![root.clone()],
        };
        // `newdir` does NOT exist (the fast / canonicalize path cannot
        // resolve the `..` — the slow path peels it off).
        let outcome = backend.validate(Path::new("newdir/../a.txt"));
        #[cfg(not(windows))]
        {
            let err = outcome.unwrap_err();
            assert!(
                matches!(err, FsError::PathEscape { .. }),
                "a `..` under a missing parent must be rejected, got {err:?}"
            );
        }
        // Windows: `canonicalize()` goes through `GetFullPathNameW`, which
        // collapses `newdir\..\a.txt` LEXICALLY — the `..` is cancelled
        // against a component that never existed, so the whole path exists
        // and the FAST path answers `root\a.txt`. The slow path's `..`
        // rejection is a rule about a peel Windows never performs; forcing
        // it would mean second-guessing the OS before canonicalizing, which
        // buys nothing: the containment property this test actually guards
        // still holds (the surviving path is checked `starts_with` the roots
        // like every other resolve, and pinning THAT is
        // `resolve_dotdot_escape_rejected` and the attack matrix — both run
        // on Windows unchanged). Stated per-OS rather than gated away: the
        // fast path resolving is a Windows BEHAVIOR worth pinning, not a
        // silence.
        #[cfg(windows)]
        {
            let ok = outcome.unwrap();
            assert!(
                ok.starts_with(&root) && ok.ends_with("a.txt"),
                "the OS-level lexical collapse must stay inside the root, got {ok:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_symlink_escape_rejected() {
        let (root, outside) = temp_root("sym");
        fs::write(&outside, "secret").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        // Windows spells the one-argument-free form `symlink_file` /
        // `symlink_dir` — there is no `std::os::windows::fs::symlink`, so the
        // arm below used to be a compile error on the first Windows build.
        // `outside` is a FILE, so `symlink_file` is the honest match.
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&outside, root.join("link")).unwrap();
        let backend = FsBackend {
            roots: vec![root.clone()],
        };
        let err = backend.validate(Path::new("link")).unwrap_err();
        assert!(matches!(err, FsError::PathEscape { .. }));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(&outside);
    }

    /// Regression test for the slow-path dangling-symlink escape: a
    /// trailing component that exists as a DANGLING symlink (canonicalize
    /// fails on it, so the ancestor walk skips past it) used to be
    /// re-appended as-is — `write`/`create_dir_all` then FOLLOWED the link
    /// and created the target OUTSIDE the sandbox. The "not yet exists"
    /// premise must hold for EVERY trailing component.
    #[cfg(unix)]
    #[test]
    fn write_through_dangling_symlink_rejected() {
        let (root, outside) = temp_root("dangle");
        // A dangling symlink under the sandbox pointing at a NON-existent
        // target outside it.
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        let backend = FsBackend {
            roots: vec![root.clone()],
        };
        // Writing THROUGH the link must be rejected — not follow it.
        let err = backend.write(Path::new("link"), "x").unwrap_err();
        assert!(
            matches!(err, FsError::PathEscape { .. }),
            "expected PathEscape, got {err:?}"
        );
        assert!(
            !outside.exists(),
            "the target outside the sandbox must not be created"
        );
        // Nor a path with a component AFTER the link (`create_dir_all`
        // would follow the link to make the parent).
        let err = backend.write(Path::new("link/inner.txt"), "x").unwrap_err();
        assert!(matches!(err, FsError::PathEscape { .. }), "got {err:?}");
        assert!(
            !outside.exists(),
            "the target outside the sandbox must not be created"
        );
        // `validate` (the `ls`/`find`/`grep`/image-`read` seam) agrees.
        let err = backend.validate(Path::new("link/inner.txt")).unwrap_err();
        assert!(matches!(err, FsError::PathEscape { .. }), "got {err:?}");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// The slow path (the dangling-symlink probe + the trailing-`..`
    /// rejection) runs against EVERY root, not only `roots[0]`: a dangling
    /// link inside root B must not become a write channel to outside all
    /// roots.
    #[cfg(unix)]
    #[test]
    fn write_through_a_dangling_symlink_in_a_non_first_root_rejected() {
        let (a, _unused) = temp_root("dangle-a");
        let (b, outside) = temp_root("dangle-b");
        std::os::unix::fs::symlink(&outside, b.join("link")).unwrap();
        let backend = FsBackend {
            roots: vec![a.clone(), b.clone()],
        };
        // Absolute (a RELATIVE path joins to `roots[0]` = `a`, so it could
        // never reach root B's link).
        let err = backend.write(&b.join("link"), "x").unwrap_err();
        assert!(
            matches!(err, FsError::PathEscape { .. }),
            "expected PathEscape, got {err:?}"
        );
        assert!(
            !outside.exists(),
            "the target outside every root must not be created"
        );
        let err = backend.write(&b.join("link/inner.txt"), "x").unwrap_err();
        assert!(
            matches!(err, FsError::PathEscape { .. }),
            "expected PathEscape, got {err:?}"
        );
        assert!(!outside.exists(), "still not created");
        let _ = std::fs::remove_dir_all(&a);
        let _ = std::fs::remove_dir_all(&b);
    }

    /// An EMPTY root set fails closed (an `Io` error) — never "everything is
    /// in bounds".
    #[test]
    fn empty_roots_is_an_error() {
        let backend = FsBackend { roots: Vec::new() };
        let err = backend.validate(Path::new("a.txt")).unwrap_err();
        assert!(matches!(err, FsError::Io { .. }), "got {err:?}");
        let err = backend.write(Path::new("a.txt"), "x").unwrap_err();
        assert!(matches!(err, FsError::Io { .. }), "got {err:?}");
    }

    /// A root that does not exist is not part of the boundary — but it is
    /// not an error either: a path under another (real) root still validates.
    #[test]
    fn a_missing_root_does_not_break_the_other_roots() {
        let (root, _outside) = temp_root("missing-root");
        // A root that will never exist (a sibling of the real root, so it
        // is not incidentally in bounds).
        let gone = std::env::temp_dir().join(format!("fsb-gone-{}", uuid::Uuid::new_v4()));
        let backend = FsBackend {
            roots: vec![root.clone(), gone.clone()],
        };
        fs::write(root.join("a.txt"), "x").unwrap();
        // The real root still works while the other root is missing.
        assert!(backend.validate(Path::new("a.txt")).is_ok());
        // And the missing root grants nothing.
        let err = backend.validate(&gone.join("a.txt")).unwrap_err();
        assert!(matches!(err, FsError::PathEscape { .. }), "got {err:?}");
        let _ = std::fs::remove_dir_all(&root);
    }
}

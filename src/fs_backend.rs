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
//! "no limits" — the unrestricted floor is an explicit `/` root
//! (`agent::boundary::UNRESTRICTED`).
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

        // Fast path: the whole path exists and can be canonicalized directly.
        if let Ok(canon) = candidate.canonicalize() {
            return self.check_under_roots(&canon);
        }

        // Slow path: the target does not exist yet. Walk up to the deepest
        // existing ancestor, canonicalize it, validate it, then re-append the
        // components that led to the (missing) target.
        let mut ancestor = candidate.clone();
        let mut trailing: Vec<std::ffi::OsString> = Vec::new();
        loop {
            match ancestor.canonicalize() {
                Ok(canon) => {
                    self.check_under_roots(&canon)?;
                    // The "not yet exists" premise must hold for EVERY
                    // trailing component, not just the deepest
                    // canonicalizable ancestor: a component that EXISTS
                    // here (it could not be canonicalized as part of the
                    // full path, so it is a dangling/broken symlink) would
                    // be re-appended as-is and FOLLOWED by the subsequent
                    // `fs::write`/`create_dir_all` — creating the target
                    // OUTSIDE the sandbox. Reject it.
                    let mut probe = canon.clone();
                    for component in trailing.iter().rev() {
                        probe.push(component);
                        if fs::symlink_metadata(&probe).is_ok() {
                            return Err(FsError::PathEscape {
                                path: probe.display().to_string(),
                            });
                        }
                    }
                    let mut result = canon;
                    for component in trailing.iter().rev() {
                        result.push(component);
                    }
                    return Ok(result);
                }
                Err(_) => {
                    // Peel off the last component and try the parent. Own it
                    // as an OsString so it does not borrow `ancestor`.
                    let last = ancestor.file_name().map(|n| n.to_os_string());
                    if let Some(last) = last {
                        trailing.push(last);
                    } else if ancestor.ends_with("..") {
                        // A trailing `..` (`file_name()` is `None` for it)
                        // is REJECTED, not silently DROPPED: dropping it
                        // would make `root/newdir/../file.txt` (with
                        // `newdir` missing) resolve to
                        // `root/newdir/file.txt` instead of `root/file.txt`
                        // — the tool would report one path and land another.
                        // (The fast / canonicalize path resolves `..`
                        // correctly, so only the slow path needs this.)
                        return Err(FsError::PathEscape {
                            path: ancestor.display().to_string(),
                        });
                    }
                    if !ancestor.pop() {
                        // Reached the filesystem root without finding an
                        // existing ancestor: the path is not under a real dir.
                        return Err(FsError::Io {
                            detail: "path does not exist under the sandbox root".to_string(),
                        });
                    }
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh temp dir as the sandbox `root` (returns the root + a unique
    /// outside-sandbox target path that does NOT exist).
    fn temp_root(tag: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("fsb-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
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
        let err = backend.validate(Path::new("newdir/../a.txt")).unwrap_err();
        assert!(
            matches!(err, FsError::PathEscape { .. }),
            "a `..` under a missing parent must be rejected, got {err:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_symlink_escape_rejected() {
        let (root, outside) = temp_root("sym");
        fs::write(&outside, "secret").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink(&outside, root.join("link")).unwrap();
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

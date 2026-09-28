//! A sandboxed client-side file I/O backend (the pre-approval read path).
//!
//! The desktop validates every path a session asks for against the
//! session's `cwd` (the sandbox root) before any I/O happens. The
//! validation is done on the *canonicalized* path, which resolves `..`
//! components and symlinks, so neither `../..` traversal nor a symlink
//! that points outside the root can escape the sandbox.
//!
//! `FsError` is local to this module (it used to ride on the protocol
//! layer's error type, which died with the pi-RPC swap): `Io` for
//! filesystem failures, `PathEscape` for a path that escaped the sandbox.

use std::fs;
use std::path::{Path, PathBuf};

/// A filesystem failure in the sandboxed backend: an I/O error, or a path
/// that escaped the session's sandbox root.
#[derive(Debug, thiserror::Error, serde::Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FsError {
    /// A filesystem operation failed (I/O error, permission, encoding, …).
    #[error("file operation failed: {detail}")]
    Io { detail: String },

    /// The requested path escaped the session's sandbox root (via `..`, a
    /// symlink, or an absolute path outside the root).
    #[error("path escapes the session sandbox: {path}")]
    PathEscape { path: String },
}

/// A sandboxed view over a directory tree.
///
/// `root` is the session's working directory; no read or write may ever
/// touch a file that does not live under it.
#[derive(Debug, Clone)]
pub struct FsBackend {
    /// The session's `cwd` — the sandbox boundary.
    pub root: PathBuf,
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

    /// Canonicalize `path` and verify it stays under `root`, returning the
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

    /// Canonicalize `path` and verify it stays under `root`.
    ///
    /// For a path that does not exist yet (a write target), the deepest
    /// existing ancestor is canonicalized and the remaining components are
    /// appended, so a brand-new file can still be validated.
    pub(crate) fn resolve(&self, path: &Path) -> Result<PathBuf, FsError> {
        let root = self.root.canonicalize().map_err(|e| FsError::Io {
            detail: format!("cannot resolve sandbox root: {e}"),
        })?;

        // Absolute paths are used as-is; relative paths are joined to the root.
        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };

        // Fast path: the whole path exists and can be canonicalized directly.
        if let Ok(canon) = candidate.canonicalize() {
            return self.check_under_root(&canon, &root);
        }

        // Slow path: the target does not exist yet. Walk up to the deepest
        // existing ancestor, canonicalize it, validate it, then re-append the
        // components that led to the (missing) target.
        let mut ancestor = candidate.clone();
        let mut trailing: Vec<std::ffi::OsString> = Vec::new();
        loop {
            match ancestor.canonicalize() {
                Ok(canon) => {
                    self.check_under_root(&canon, &root)?;
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

    /// Confirm `canon` (already canonicalized) is `root` or a descendant of it.
    pub(crate) fn check_under_root(&self, canon: &Path, root: &Path) -> Result<PathBuf, FsError> {
        if canon.starts_with(root) {
            Ok(canon.to_path_buf())
        } else {
            Err(FsError::PathEscape {
                path: canon.display().to_string(),
            })
        }
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
        let backend = FsBackend { root: root.clone() };
        let resolved = backend.validate(Path::new("a.txt")).unwrap();
        assert_eq!(resolved, f);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_missing_file_under_root_ok() {
        let (root, _outside) = temp_root("missing");
        let backend = FsBackend { root: root.clone() };
        // A brand-new file (and its new parents) validate fine.
        let resolved = backend.validate(Path::new("nested/deep/a.txt")).unwrap();
        assert_eq!(resolved, root.join("nested/deep/a.txt"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolve_dotdot_escape_rejected() {
        let (root, _outside) = temp_root("esc");
        fs::write(root.join("a.txt"), "x").unwrap();
        let backend = FsBackend { root: root.clone() };
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
        let backend = FsBackend { root: root.clone() };
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
        let backend = FsBackend { root: root.clone() };
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
        let backend = FsBackend { root: root.clone() };
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
}

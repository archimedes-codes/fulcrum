//! A sandboxed client-side file I/O backend (the pre-approval read path).
//!
//! The desktop validates every path a session asks for against the
//! session's `cwd` (the sandbox root) before any I/O happens. The
//! validation is done on the *canonicalized* path, which resolves `..`
//! components and symlinks, so neither `../..` traversal nor a symlink
//! that points outside the root can escape the sandbox.
//!
//! `FsError` is local to this module (it used to ride on the ACP
//! `AcpError`, which died with the pi-RPC swap): `Io` for filesystem
//! failures, `PathEscape` for a path that escaped the sandbox.

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

    /// Canonicalize `path` and verify it stays under `root`.
    ///
    /// For a path that does not exist yet (a write target), the deepest
    /// existing ancestor is canonicalized and the remaining components are
    /// appended, so a brand-new file can still be validated.
    fn resolve(&self, path: &Path) -> Result<PathBuf, FsError> {
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
    fn check_under_root(&self, canon: &Path, root: &Path) -> Result<PathBuf, FsError> {
        if canon.starts_with(root) {
            Ok(canon.to_path_buf())
        } else {
            Err(FsError::PathEscape {
                path: canon.display().to_string(),
            })
        }
    }
}

//! Client-side file I/O backend for the ACP `fs/read_text_file` and
//! `fs/write_text_file` methods.
//!
//! The agent delegates file reads/writes to the client. Because the agent
//! runs as a separate (potentially untrusted) process, every path it asks for
//! is validated against the session's `cwd` (the sandbox root) before any I/O
//! happens. The validation is done on the *canonicalized* path, which
//! resolves `..` components and symlinks, so neither `../..` traversal nor a
//! symlink that points outside the root can escape the sandbox.

use std::fs;
use std::path::{Path, PathBuf};

use crate::agent::errors::AcpError;

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
    pub fn read(&self, path: &Path) -> Result<String, AcpError> {
        let resolved = self.resolve(path)?;
        fs::read_to_string(&resolved).map_err(|e| AcpError::Io {
            detail: e.to_string(),
        })
    }

    /// Write a text file, rejecting any path that escapes the sandbox.
    ///
    /// Parent directories that do not yet exist are created (still inside the
    /// sandbox) so the agent can write to a fresh path.
    pub fn write(&self, path: &Path, content: &str) -> Result<(), AcpError> {
        let resolved = self.resolve(path)?;
        if let Some(parent) = resolved.parent() {
            fs::create_dir_all(parent).map_err(|e| AcpError::Io {
                detail: e.to_string(),
            })?;
        }
        fs::write(&resolved, content).map_err(|e| AcpError::Io {
            detail: e.to_string(),
        })
    }

    /// Canonicalize `path` and verify it stays under `root`.
    ///
    /// For a path that does not exist yet (a write target), the deepest
    /// existing ancestor is canonicalized and the remaining components are
    /// appended, so a brand-new file can still be validated.
    fn resolve(&self, path: &Path) -> Result<PathBuf, AcpError> {
        let root = self.root.canonicalize().map_err(|e| AcpError::Io {
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
                        return Err(AcpError::Io {
                            detail: "path does not exist under the sandbox root".to_string(),
                        });
                    }
                }
            }
        }
    }

    /// Confirm `canon` (already canonicalized) is `root` or a descendant of it.
    fn check_under_root(&self, canon: &Path, root: &Path) -> Result<PathBuf, AcpError> {
        if canon.starts_with(root) {
            Ok(canon.to_path_buf())
        } else {
            Err(AcpError::PathEscape {
                path: canon.display().to_string(),
            })
        }
    }
}

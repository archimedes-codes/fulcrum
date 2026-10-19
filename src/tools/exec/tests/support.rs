use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

use crate::agent::tools::types::{ContentBlock, ToolCtx, ToolResult};

/// A fresh temp dir as the sandbox `cwd`.
pub(crate) fn temp_cwd(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("exec-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The test `ToolCtx`. HERMETIC BY DESIGN: `boundary` is the single
/// `cwd` and `protected` is EMPTY — this must NEVER derive them from the
/// `$HOME`-reading boundary helpers (they would make these tests depend
/// on whatever happens to be in the developer's `~/.agents`, the same
/// trap `ToolCtx::skill_roots: Option<…>` exists to avoid).
///
/// The policy pins `Sandboxed` for reads and writes: these are
/// CONTAINMENT tests, so they pin the tier that enforces the boundary
/// (`shell: Allow` — no test here gates `bash`). The other tiers are
/// covered by the explicit tier tests below.
pub(crate) fn ctx(cwd: &Path) -> ToolCtx {
    ToolCtx {
        cwd: cwd.to_path_buf(),
        cancel: CancellationToken::new(),
        skill_roots: None,
        boundary: vec![cwd.to_path_buf()],
        file_policy: crate::agent::policy::FilePolicy {
            reads: crate::agent::policy::AccessPolicy::Sandboxed,
            writes: crate::agent::policy::AccessPolicy::Sandboxed,
            shell: crate::agent::policy::AccessPolicy::Allow,
        },
        protected: Vec::new(),
        web: Default::default(),
        render_requester: None,
    }
}

/// The same ctx with one policy overridden (the tier tests).
pub(crate) fn ctx_policy(cwd: &Path, policy: crate::agent::policy::FilePolicy) -> ToolCtx {
    ToolCtx {
        file_policy: policy,
        ..ctx(cwd)
    }
}

/// Whether THIS machine can confine a child (Linux + a Landlock
/// kernel). The confined tests branch on it: a kernel that cannot
/// confine must exercise the fail-closed path, and CI on such a kernel
/// stays reproducible instead of reddening.
pub(crate) fn can_confine() -> bool {
    #[cfg(target_os = "linux")]
    {
        crate::agent::tools::sandbox::landlock_available()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// The first text block's text (panics if the shape is wrong).
pub(crate) fn text_of(r: &ToolResult) -> &str {
    match &r.content[0] {
        ContentBlock::Text { text } => text,
        other => panic!("expected a text block, got {other:?}"),
    }
}

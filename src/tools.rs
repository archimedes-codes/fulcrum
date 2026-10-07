//! The Rust tool executors (native) + the shared tool types. The native
//! `AgentLoop` executes its built-ins in-process through
//! [`execute_tool`] (the `tools/exec.rs` executor — a submodule of this
//! file; there is deliberately NO `tools/mod.rs`).

pub mod exec;
// (ADR 0030 Task 5) The Landlock sandbox behind `Shell=Sandboxed`. Linux
// ONLY: `libc` is a `cfg(unix)` dependency and the Landlock syscall numbers
// exist on no other target, so an unconditional declaration breaks the
// Windows/macOS builds (off Linux the tier fails closed in `exec.rs`).
#[cfg(target_os = "linux")]
pub mod sandbox;

/// (ADR 0030 Task 5) Whether a `Sandboxed` `bash` can be confined on THIS
/// machine — the one signal both the Settings grey-out and the executor's
/// fail-closed path read, so the UI can never offer a tier that only
/// produces errors. Off Linux: always `false` (the sandbox is not compiled).
pub fn shell_sandbox_available() -> bool {
    #[cfg(target_os = "linux")]
    {
        sandbox::landlock_available()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

pub use exec::{execute_tool, ContentBlock, ImageRef, ToolCtx, ToolResult};

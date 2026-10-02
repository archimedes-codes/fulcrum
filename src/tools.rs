//! The Rust tool executors (native) + the shared tool types. The native
//! `AgentLoop` executes its built-ins in-process through
//! [`execute_tool`] (the `tools/exec.rs` executor — a submodule of this
//! file; there is deliberately NO `tools/mod.rs`).

pub mod exec;

pub use exec::{execute_tool, ContentBlock, ImageRef, ToolCtx, ToolResult};

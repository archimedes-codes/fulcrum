//! Windows named-pipe listener + client (T9 Task 3). STUB — empty on purpose.
//!
//! This file MUST EXIST even though `transport/mod.rs` declares it under
//! `#[cfg(windows)]`: omitting it gives `error[E0583]: file not found for module
//! 'windows'` on the Windows target ONLY, so a Linux-only developer never sees
//! it and CI is the first to notice. The cross-target clippy run on
//! `x86_64-pc-windows-gnu` is what holds this open.
//!
//! Backed by `tokio::net::windows::named_pipe::{NamedPipeServer,
//! NamedPipeClient}` — tokio's `net` feature provides those on Windows, so no
//! `windows` / `windows-sys` dependency is needed (and none is declared).

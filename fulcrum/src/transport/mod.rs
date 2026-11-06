//! The framed transport between the host and the daemon (T9 Task 3).
//!
//! `frame` is the platform-neutral length-prefixed codec; `unix` and `windows`
//! are the two listeners/clients under their own cfg gates.
//!
//! STUB: Task 3 fills these. They are empty on purpose — see `lib.rs`.

pub mod frame;
#[cfg(unix)]
pub mod unix;
#[cfg(windows)]
pub mod windows;

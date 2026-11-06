// T9 Task 5 replaces this stub.
//
// NOTE: deliberately NOT carrying the desktop's
// `#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]`. That
// attribute exists so a GUI app shows no console; a worker supervisor WANTS its
// console in release, because it is the one place a supervisor-level failure can
// be read.
fn main() {}

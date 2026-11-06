//! The `fulcrum` binary: one executable, three modes (T9 Task 5).
//!
//! ```text
//! fulcrum --worker              the Worker (a child process; the engine's loop)
//! fulcrum serve [OPTIONS]       the daemon (the supervisor + the local socket)
//! fulcrum help | --help         the usage, on stdout, exit 0
//! anything else                 the usage, on stderr, exit 2
//! ```
//!
//! # Why one binary holds both modes
//!
//! It is not tidiness — it is what makes the daemon's default `--worker-exe`
//! correct. `WorkerHandle::spawn(exe)` runs `<exe> --worker`, and the engine's
//! documented production caller passes `std::env::current_exe()`: the Worker is a
//! self-exec of the supervisor. Splitting the two into separate binaries would
//! turn that default into a path the daemon has to guess. The desktop already
//! ships this shape (its GUI binary answers `--worker`), and T12's sidecar
//! decision is an override (`--worker-exe`), not a different arrangement.
//!
//! # Why the whole thing is a `match` over `serve::parse_command`
//!
//! The library target owns the argv grammar (`serve::parse_command`) so it is
//! testable without spawning a process, and this file is the `match` that
//! executes it. An integration test cannot reach a private item in `main.rs`, so
//! a parser here would be a parser no test can call — and the parser is the part
//! with the failure modes worth pinning.
//!
//! # Why an unknown argv is LOUD
//!
//! Exit 2 plus the usage on stderr. The silent alternative is the one that hurts:
//! a typo'd flag that got ignored, so `--worker-exe /wrong/path` became
//! `current_exe()` and the daemon spent the session spawning copies of itself as
//! its own Workers. A daemon that ignores argv it does not recognise is a daemon
//! that will quietly do the wrong thing for hours.

use std::io::Write;

use fulcrum::serve::{self, ServeError};

/// The exit code for "I did not understand what you asked for" (the shell
/// convention for a usage error; `1` is reserved for "I understood and failed").
const USAGE_EXIT: i32 = 2;

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match serve::parse_command(&argv) {
        // The Worker mode: hand the process to the engine. This call does not
        // return — `run_worker` builds its own multi-thread runtime and
        // `block_on`s the stdio loop until `close` / stdin EOF / a panic.
        serve::Command::Worker => fulcrum_core::worker::run_worker(
            // The browser opener. `NoOpenRequester` is the engine's PRODUCTION,
            // ungated no-op impl, and it is the honest answer for this host: a
            // headless supervisor has no browser to open, and an MCP OAuth error
            // still carries its URL for the user to visit by hand. The desktop
            // injects its Tauri opener instead — that is what this parameter
            // exists for.
            std::sync::Arc::new(fulcrum_core::mcp::NoOpenRequester),
            // No crash-logging panic hook. `run_worker_inner`'s own doc names
            // this argument's `None` as "the shape a non-desktop host (the
            // daemon, T9) passes": the hook writes into the desktop's data dir,
            // and a worker that dies is already reported — its `Exited` becomes
            // `on_crash` becomes a frame on the socket.
            None,
        ),
        serve::Command::Serve(config) => {
            if let Err(e) = run_serve(config) {
                eprintln!("fulcrum serve: {e}");
                std::process::exit(1);
            }
        }
        serve::Command::Help => {
            print!("{}", serve::usage());
            // A successful `help` exits 0 even when stdout is a closed pipe, so
            // `fulcrum help | head -1` behaves like every other CLI's.
            let _ = std::io::stdout().flush();
        }
        serve::Command::Usage(reason) => {
            eprintln!("fulcrum: {reason}\n");
            eprint!("{}", serve::usage());
            std::process::exit(USAGE_EXIT);
        }
    }
}

/// Open the store, decide the Worker executable, bind the listener, and run the
/// accept loop until the process is killed (T9 has no shutdown surface — see
/// `serve::run`).
///
/// The database opens and the factory resolves BEFORE the bind, on purpose: a
/// daemon that binds first and then fails to open `fulcrum.db` leaves a live
/// socket behind that answers `hello` and fails every `start`, and Task 3's
/// stale-socket reclaim will not clean it up (a live listener answers the probe,
/// so the file is left alone — correctly). Failing before the socket exists
/// leaves nothing to reclaim. `tests/serve.rs`'s
/// `a_daemon_that_cannot_open_its_database_leaves_no_socket_behind` is the pin —
/// the code had drifted to bind-first while this doc said the opposite, which is
/// the shape of rot a doc comment cannot fix on its own.
///
/// The Worker executable is resolved in the same pre-bind block for the same
/// reason: `DaemonWorkerFactory::resolve` can fail (a `current_exe()` that
/// returns nothing), and a daemon that had already bound a socket then notices
/// the same way — same orphan. A named path that does not exist is deliberately
/// NOT a startup failure, though: that one belongs in the `start-result` of the
/// session that asked for it.
///
/// The runtime is built here rather than inside `serve::run` so `Listener::bind`
/// (a sync fn that needs a tokio reactor for `UnixListener`) and the accept loop
/// share one runtime, and so a runtime failure is a startup message instead of a
/// panic inside the library.
fn run_serve(config: serve::ServeConfig) -> Result<(), ServeError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| ServeError::Runtime(e.to_string()))?;
    // Entered explicitly: `Listener::bind` is synchronous and registers with the
    // reactor, so it needs the runtime's context — `block_on` would give it one
    // a frame too late.
    let _guard = runtime.enter();

    // ONE database handle for the process, shared as `Arc<Db>` (see `serve::run`:
    // a `Db` per client would make "the sole writer of fulcrum.db" untrue the
    // moment two clients started a session). FIRST, because it is the failure
    // most likely to happen on a real machine (a full disk, a read-only data
    // dir, a file owned by another user) and the one whose orphan is hardest for
    // an operator to find.
    let db = std::sync::Arc::new(serve::open_db(&config)?);

    // The Worker executable, decided before the bind too (see above).
    let factory = std::sync::Arc::new(serve::DaemonWorkerFactory::resolve(config.worker_exe)?);

    let listener = fulcrum::transport::Listener::bind(config.endpoint.clone())
        .map_err(|e| ServeError::Bind(e.to_string()))?;

    eprintln!(
        "fulcrum serve: listening on {} (worker exe: {})",
        listener.endpoint(),
        factory.exe().display()
    );

    runtime.block_on(serve::run(listener, db, factory));
    Ok(())
}

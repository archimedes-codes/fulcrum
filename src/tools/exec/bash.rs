use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::AsyncReadExt;

use crate::agent::tools::types::{ContentBlock, ToolCtx, ToolResult};

/// The default `bash` deadline when no `timeout_ms` is given (300 s —
/// matching the interactive channel's `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`: the native
/// `dispatch_tool` has no outer timeout of its own, so the executor must
/// bound the run itself — a no-`timeout_ms` `bash` is NOT unbounded).
pub(crate) const DEFAULT_BASH_TIMEOUT: Duration = Duration::from_secs(300);

/// The cap on a model-supplied `timeout_ms` (15 min — the model controls
/// the timeout: an unclamped `timeout_ms: 10^9` would run ~11 days. The
/// cap bounds a model-supplied timeout without affecting the default).
pub(crate) const MAX_BASH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Clamp a model-supplied `timeout_ms` to [`MAX_BASH_TIMEOUT`] (the model
/// cannot run a command unbounded — see the cap's doc).
pub(crate) fn clamp_timeout_ms(ms: u64) -> Duration {
    Duration::from_millis(ms).min(MAX_BASH_TIMEOUT)
}

/// The max bytes of combined stdout+stderr `exec_bash` captures (100 KB —
/// overflow is drained but capped, flagged `truncated: true`).
pub(crate) const MAX_OUTPUT_BYTES: usize = 100 * 1024;

/// The grace deadline for draining a killed `bash` child's pipes (2 s —
/// the pipes may never close if a grandchild escaped the process group;
/// a bounded wait, not an unbounded wait for EOF).
pub(crate) const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// Deduplicated pipe-buffering collector for combined stdout+stderr streams.
pub(crate) struct OutputCollector {
    buf: Vec<u8>,
    truncated: bool,
    max_bytes: usize,
}

impl OutputCollector {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            buf: Vec::new(),
            truncated: false,
            max_bytes,
        }
    }

    pub(crate) fn ingest(&mut self, chunk: &[u8]) {
        let available = self.max_bytes.saturating_sub(self.buf.len());
        let take = chunk.len().min(available);
        self.buf.extend_from_slice(&chunk[..take]);
        if self.buf.len() >= self.max_bytes {
            self.truncated = true;
        }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub(crate) fn is_truncated(&self) -> bool {
        self.truncated
    }
}

/// Execute a shell command (`sh -c` on Unix, `cmd /d /s /c` on Windows) inside
/// `ctx.cwd`, bounded by an optional `timeout_ms` (default 300 s, max 15 min;
/// clamped by [`clamp_timeout_ms`]).
///
/// Security note: on Unix the child is spawned in its own process group via
/// `process_group(0)`, so backgrounded grandchildren are killed when the
/// parent is killed; writes are confined to `ctx.cwd` when
/// `ctx.file_policy.shell == Sandboxed` (Linux Landlock, ADR 0030 Task 5). Both
/// spawn branches below are built by its constructor, which carries
/// `CREATE_NO_WINDOW` so a bundled run pops no `cmd.exe` window per command.
///
/// `params`: `{ command: String, timeout_ms?: u64 }`.
pub async fn exec_bash(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let command = match params.get("command").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => return ToolResult::fail("bash: missing `command` parameter".to_string(), None),
    };
    let confined = ctx.file_policy.shell == crate::agent::policy::AccessPolicy::Sandboxed;
    let timeout = params
        .get("timeout_ms")
        .and_then(|v| v.as_u64())
        .map(clamp_timeout_ms);

    // `sh -c` takes the command verbatim (no re-parsing). On Unix the
    // child is spawned in its OWN process group (`process_group(0)`), so
    // a backgrounded grandchild (`sh -c 'sleep 1000 & ...'`) can be
    // reaped with the group — `child.kill()` alone kills only `sh` and
    // leaves the grandchild holding the pipes (the read loop hangs until
    // cancel; the orphaned grandchild outlives the call).
    #[cfg(unix)]
    let mut cmd = {
        let mut c = crate::agent::childproc::command("sh");
        c.arg("-c");
        c.process_group(0);
        c
    };
    #[cfg(not(unix))]
    // No `/C` here: the whole `/d /s /c "<command>"` tail is appended as ONE raw
    // argument below, by the module that owns the Windows shape of a child's
    // command line. `arg("/C")` + `arg(command)` is exactly what issue #79 is
    // about — see `childproc::append_cmd_c`.
    let mut cmd = crate::agent::childproc::command("cmd");
    // Tool child, so stdin is NULL, not inherited. Nothing can ever feed this
    // child: the Worker owns the only pipe a stdin could be (`worker/client.rs`
    // holds the `BufWriter<ChildStdin>` for its whole life) and `tokio`'s
    // `output()`/`spawn()` pipe stdout/stderr but never stdin — so an inherited
    // handle is a live, silent pipe, and `cat` / `python -` / a `git credential`
    // helper / an `ssh` password prompt blocks on it until the
    // [`DEFAULT_BASH_TIMEOUT`] 300 s deadline. EOF is always the better answer.
    // One call here covers BOTH `cfg` arms above (they bind the same `cmd`).
    // Deliberately NOT applied to Runtime children (the Worker's own stdin, an
    // MCP server's, `sudo_exec`'s password stdin) — we write those pipes.
    crate::agent::childproc::closed_stdin(&mut cmd);
    // The command string reaches the shell UNMODIFIED on both platforms — that
    // is this tool's whole contract. Unix: `sh -c` takes it verbatim. Windows:
    // `arg` would wrap it in an outer quote pair and escape every embedded `"`
    // as `\"`, and `cmd /s` strips only the OUTER pair, so the escapes stay
    // visible to command parsing — inline quoting becomes unusable and `start`'s
    // title-slot rule turns the wreckage into a modal dialog on the user's
    // desktop (issue #79). Hence the raw tail, appended inside `childproc`
    // (census invariant E).
    #[cfg(unix)]
    cmd.arg(&command);
    #[cfg(not(unix))]
    crate::agent::childproc::append_cmd_c(&mut cmd, &command);
    cmd.current_dir(&ctx.cwd);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);

    // (ADR 0030) The confined tier: build the ruleset and install it in the
    // CHILD (`pre_exec`) so the Worker and the desktop are never confined.
    // FAIL CLOSED, VISIBLY: no ruleset means no run — an unsandboxed run
    // here would break exactly the promise the user selected, so every
    // failure is a tool-result error naming the reason. Off Linux the
    // sandbox module is not compiled at all, so the tier fails closed.
    //
    // The guard MUST outlive `spawn` (the child's forked fd table refers to
    // the parent's ruleset fd), hence a binding rather than a temporary.
    #[cfg(not(target_os = "linux"))]
    if confined {
        return ToolResult::fail(
            "Sandboxed shell is unavailable on this platform".to_string(),
            None,
        );
    }
    #[cfg(target_os = "linux")]
    let sandbox = if confined {
        match crate::agent::tools::sandbox::Sandbox::create(&ctx.cwd) {
            Ok(sandbox) => {
                sandbox.install(&mut cmd);
                Some(sandbox)
            }
            Err(reason) => {
                return ToolResult::fail(format!("Sandboxed shell is unavailable: {reason}"), None)
            }
        }
    } else {
        None
    };

    // The child's env, minus the AppImage's own paths (`docs/research/
    // appimage-child-process-env-leak.md`). A child of a bundled run inherits
    // `LD_LIBRARY_PATH`/`PATH`/`GI_TYPELIB_PATH`/… with the
    // `/tmp/.mount_archim*` dirs at position 0, so `ld.so` resolves a system
    // SONAME to the BUNDLE's copy (a match by name) and then fails the
    // separate `DT_VERNEED` check: fatal for `node` (`version 'OPENSSL_3.4.0'
    // not found` — the agent reading that as "Node is broken in this
    // sandbox" and skipping `tsc`), a warning for `git`'s unversioned
    // `libpcre2-8.so.0`. One `apply` covers both spawn branches above (the
    // Unix `sh` and the Windows `cmd`); the patch is empty by construction off
    // Linux and whenever we are not running from a mount, and an empty patch
    // emits no `env` call at all — a `.deb`, an `.rpm` and a `cargo run` stay
    // byte-identical.
    //
    // Placed AFTER the sandbox block so the two child-affecting steps have one
    // explicit order; that order is COSMETIC — Landlock confines the child's
    // VIEW of the filesystem and never reads or writes its env, so this patch
    // and the sandbox's `pre_exec` cannot interfere in either direction. The
    // note that is NOT cosmetic is the `Sandbox` guard's binding above: it must
    // outlive `spawn` (the child's forked fd table refers to the parent's
    // ruleset fd), and neither this statement nor a future env-aware `Sandbox`
    // may change that.
    crate::agent::hostenv::for_child().apply(&mut cmd);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        // A `pre_exec` failure surfaces here (std reports the closure's
        // errno as the spawn error): still a confined run that never
        // happened, so the message names the sandbox, not a bare "failed
        // to start".
        Err(e) if confined => {
            return ToolResult::fail(
                format!("Sandboxed shell is unavailable: could not start the confined child ({e})"),
                None,
            )
        }
        Err(e) => return ToolResult::fail(format!("bash: failed to start: {e}"), None),
    };

    // The parent's ruleset fd is no longer needed once the child is forked
    // (the child has its own descriptor), so it is closed immediately.
    #[cfg(target_os = "linux")]
    drop(sandbox);

    let mut stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    // The child's pid, captured NOW: `Child::id()` returns `None` once the
    // child has been reaped, and the process-group kill below needs it —
    // capturing the exit status early (which the loop does) would
    // otherwise silently skip the group kill and ORPHAN any grandchild.
    let group_pid = child.id();
    let mut collector = OutputCollector::new(MAX_OUTPUT_BYTES);
    let mut cancelled = false;
    let mut timed_out = false;
    // The child's exit status, as SOON as it is observed — deliberately NOT
    // decided after the drain (see [`bash_exit_report`]: the old code
    // SIGKILLed a child that had closed its pipes but not yet exited and
    // reported that self-inflicted signal death as a failed command).
    let mut status: Option<std::process::ExitStatus> = None;
    // A failed `wait` means the status can never be learned, so the arm is
    // retired (re-polling a permanently failing wait would spin the loop).
    let mut await_status = true;
    let mut stdout_open = true;
    let mut stderr_open = true;
    // One buffer per stream (both select arms borrow them at once).
    let mut out_buf = [0u8; 8192];
    let mut err_buf = [0u8; 8192];
    // Deadline for the timeout. No `timeout_ms` = the 300 s default
    // ([`DEFAULT_BASH_TIMEOUT`] — the interactive channel's
    // `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`; the native `dispatch_tool` has no
    // outer timeout, so the executor bounds the run itself). Re-armed
    // each loop iteration (`Sleep` is not `Unpin`, so it cannot be a
    // reused `&mut` future).
    let deadline = std::time::Instant::now() + timeout.unwrap_or(DEFAULT_BASH_TIMEOUT);

    loop {
        // NOTE: the LAST arm of `tokio::select!` may not carry an `if`
        // guard (macro limitation) — the unguarded arms come last.
        tokio::select! {
            r = stdout.read(&mut out_buf), if stdout_open => {
                match r {
                    Ok(0) => stdout_open = false,
                    Ok(n) => collector.ingest(&out_buf[..n]),
                    Err(_) => stdout_open = false,
                }
            },
            r = stderr.read(&mut err_buf), if stderr_open => {
                match r {
                    Ok(0) => stderr_open = false,
                    Ok(n) => collector.ingest(&err_buf[..n]),
                    Err(_) => stderr_open = false,
                }
            },
            s = child.wait(), if await_status => {
                // The exit status is captured HERE, in the same `select!` as
                // the reads, so what a run reports cannot depend on how long
                // the drain below takes, nor on whether this task gets
                // scheduled again after it. `wait` is cancel-safe, so
                // re-polling it per iteration is safe; the arm is retired
                // once the status lands.
                if let Ok(s) = s {
                    status = Some(s);
                }
                await_status = false;
            },
            _ = ctx.cancel.cancelled() => {
                cancelled = true;
                break;
            },
            _ = tokio::time::sleep_until(deadline.into()) => {
                timed_out = true;
                break;
            },
        }
        // Pipe EOF is NOT the end of the run: a child that closed its
        // pipes may not have exited yet (`exec >&- 2>&-; …; exit N`), and
        // `select!` completes ONE arm per iteration — so on a simultaneous
        // read-EOF the `child.wait()` arm may not even be polled here.
        // Breaking on EOF alone would leave `status: None` and hand the
        // report to the bounded post-loop reap, which expires under CPU
        // contention and reports a run that finished fine as "exit status
        // unknown". Break only once the exit status is SETTLED — seen
        // (`status.is_some()`), or the wait arm retired (`!await_status`):
        // a child that closed its pipes but has not exited is awaited
        // (bounded by the `deadline` arm above), never reported early.
        if !stdout_open && !stderr_open && (status.is_some() || !await_status) {
            break;
        }
    }

    // Kill the WHOLE process group whenever anything may still be alive:
    // the loop broke on cancel/timeout with the child's exit unobserved
    // (`status.is_none()`), or with a pipe still open (a backgrounded
    // grandchild may hold it). A child that CLOSED ITS PIPES has not
    // necessarily exited — pipe EOF is not an exit — so an unobserved exit
    // is a live child until proven otherwise. The kill is skipped ONLY
    // when the exit was observed AND both pipes are closed: nothing is
    // alive then, and signalling the (possibly recycled) process group
    // would be wrong. On Unix the child is in its own group via
    // `process_group(0)`, so the grandchildren are reaped and the pipes
    // close.
    if stdout_open || stderr_open || status.is_none() {
        kill_bash_process_group(group_pid, &mut child).await;
    }
    // Drain what the group's death frees, bounded by a short grace
    // deadline — NOT an unbounded wait for EOF (a grandchild that
    // escaped the group could hold the pipes forever). Only meaningful
    // while a pipe is open: both closed means there is nothing to read.
    if stdout_open || stderr_open {
        let grace = std::time::Instant::now() + DRAIN_GRACE;
        loop {
            tokio::select! {
                r = stdout.read(&mut out_buf), if stdout_open => {
                    match r {
                        Ok(0) => stdout_open = false,
                        Ok(n) => collector.ingest(&out_buf[..n]),
                        Err(_) => stdout_open = false,
                    }
                },
                r = stderr.read(&mut err_buf), if stderr_open => {
                    match r {
                        Ok(0) => stderr_open = false,
                        Ok(n) => collector.ingest(&err_buf[..n]),
                        Err(_) => stderr_open = false,
                    }
                },
                _ = tokio::time::sleep_until(grace.into()) => break,
            }
            if !stdout_open && !stderr_open {
                break;
            }
        }
    }

    // The report is decided by [`bash_exit_report`], from the facts the run
    // actually has. NO unconditional `kill()`: a child that has written all
    // its output and closed both pipes is NOT dead yet (pipe EOF is not an
    // exit), and SIGKILLing it there reported a SUCCESSFUL command as a
    // failure — the flake that made models retry non-idempotent commands.
    // If the status is still unknown, give the reap one bounded chance to
    // land (the child is usually already dead, so this returns at once) —
    // and if it does not, say so instead of inventing -1.
    if status.is_none() {
        if let Ok(Ok(s)) = tokio::time::timeout(DRAIN_GRACE, child.wait()).await {
            status = Some(s);
        }
    }
    let (exit_code, is_error, note) = bash_exit_report(cancelled, timed_out, status);

    let truncated = collector.is_truncated();
    let out = collector.into_bytes();

    // The note goes into the text the MODEL reads: a `null` exit code on
    // the wire is invisible to it, and "unknown — check before re-running"
    // is exactly the thing it must not have to guess.
    let mut text = String::from_utf8_lossy(&out).into_owned();
    if let Some(note) = note {
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&note);
        text.push('\n');
    }

    ToolResult {
        content: vec![ContentBlock::Text { text }],
        details: Some(json!({
            "exitCode": exit_code,
            "truncated": truncated,
            "cancelled": cancelled,
        })),
        is_error,
    }
}

/// The exit status' terminating SIGNAL (the `None` for a normal exit), or
/// `None` on a platform whose `ExitStatus` does not expose one.
#[cfg(unix)]
pub(crate) fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
pub(crate) fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
    None
}

/// What a `bash` run reports, given everything the executor actually knows.
///
/// PURE on purpose — the three inputs are the only facts the run produces
/// (was it cancelled, did it hit its deadline, was the child's exit status
/// observed), so the whole truth table is unit-testable without a child and
/// without timing. This is where the old code fabricated `exitCode: -1`:
/// a child that had already written all its output and closed both pipes
/// was still alive for a few more microseconds (EOF on a pipe does NOT mean
/// the process exited), and the unconditional `kill()` that followed turned
/// a SUCCESS into a signal death reported as `-1` — a model watching a
/// successful `git commit` fail, and retrying a non-idempotent command.
///
/// Returns `(reported exit code, is_error, note)`: the code is `None` when
/// it is genuinely unknowable (never a fabricated number), and the `note`
/// — appended to the text the MODEL reads — explains any outcome the bare
/// code cannot (`bash_output_capped_at_max`'s cap applies to the captured
/// output, not to this note).
pub(crate) fn bash_exit_report(
    cancelled: bool,
    timed_out: bool,
    status: Option<std::process::ExitStatus>,
) -> (Option<i32>, bool, Option<String>) {
    /// The warning for an outcome that cannot be known: the model must not
    /// read silence as success, and must not re-run blind.
    const UNKNOWN: &str = "The command's exit status is unknown (the process was never reaped): it may have SUCCEEDED, and its side effects may already have landed — inspect the resulting state before re-running this command.";

    let code = status.and_then(|s| s.code());
    let signal = status.and_then(|s| exit_signal(&s));

    // Cancel wins over the deadline (whichever arm broke the loop first).
    if cancelled {
        return (
            code,
            true,
            Some(
                "The command was cancelled before it finished; it may have completed part of its work.".to_string(),
            ),
        );
    }
    if timed_out {
        let mut note = "The command timed out and was killed at its deadline.".to_string();
        if status.is_none() {
            note.push(' ');
            note.push_str(UNKNOWN);
        }
        return (code, true, Some(note));
    }
    match (code, signal) {
        // The one non-error row: the child exited 0 and we SAW it do so.
        (Some(0), None) => (Some(0), false, None),
        // A normal non-zero exit needs no note — the code IS the report.
        (Some(n), None) => (Some(n), true, None),
        // A signal death is NOT dressed up as an exit code: -1 would be
        // indistinguishable from "unknown", and a real signal death must
        // stay distinguishable from a non-zero exit.
        (_, Some(sig)) => (
            None,
            true,
            Some(format!(
                "The command died on signal {sig} rather than exiting with a code."
            )),
        ),
        // No status at all: honestly unknown.
        (None, None) => (None, true, Some(UNKNOWN.to_string())),
    }
}

/// Kill a `bash` child's WHOLE process group: on Unix the child was
/// spawned in its own group via `process_group(0)`, so a negative-pid
/// `kill` SIGKILLs the group (including backgrounded grandchildren that
/// hold the pipes open); elsewhere fall back to killing the child only.
///
/// `pid` is the child's pid captured BEFORE it can be reaped: `Child::id()`
/// goes `None` the moment the exit status is collected, and a `None` here
/// skips the group kill and leaves an orphaned grandchild holding the pipes
/// (and writing the marker file it was about to write).
pub(crate) async fn kill_bash_process_group(pid: Option<u32>, child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        // A negative pid SIGKILLs the WHOLE process group (the child is
        // the group leader via `process_group(0)`).
        if let Some(pid) = pid {
            let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        }
        // Fallback: kill the direct child as well.
        let _ = child.kill().await;
    }
    #[cfg(not(unix))]
    {
        let _ = pid; // No process groups to kill here.
        let _ = child.kill().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_collector_capping_and_truncation() {
        let mut collector = OutputCollector::new(10);
        assert!(!collector.is_truncated());
        assert_eq!(collector.buf.len(), 0);

        collector.ingest(b"hello");
        assert!(!collector.is_truncated());
        assert_eq!(collector.buf, b"hello");

        // Ingest 5 more -> reaches 10 bytes exactly
        collector.ingest(b"world");
        assert!(collector.is_truncated());
        assert_eq!(collector.buf, b"helloworld");

        // Ingest beyond cap -> discarded, stays truncated
        collector.ingest(b"overflow");
        assert!(collector.is_truncated());
        assert_eq!(collector.into_bytes(), b"helloworld");
    }
}

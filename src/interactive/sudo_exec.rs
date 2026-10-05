//! The real-sudo process-execution seam: the `SudoRunner` trait, the
//! production `RealSudoRunner` (`sudo -S` spawned on the host in its OWN
//! process group, the password written to stdin, the WHOLE group
//! SIGKILLed on timeout / abrupt drop), and the stream reader the capture
//! path shares. Pure process management — no interactive-channel
//! dependency (the sudo flow that drives this lives in `super`).

use std::future::Future;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::agent::events::EventSink;
use crate::agent::interactive::{PendingSudo, SudoRun};

/// The `sudo_exec` execution seam (mirrors the suite's `SudoSpawner` seam,
/// `packages/sudo/src/tool.ts` `createSudoExecTool({spawner})`): the desktop
/// runs `sudo -S` on the host through this trait, so the tests use a fake
/// runner (no real `sudo` in `cargo test`) and the real runner is the
/// production `SessionDriver` default.
///
/// MUST be dyn-compatible (`Arc<dyn SudoRunner>`) — a plain `async fn` in a
/// trait is NOT dyn-compatible, hence the boxed-future return (the
/// boxed-future pattern).
pub trait SudoRunner: Send + Sync {
    /// Run `sudo -S <argv>` (the full argv from [`build_sudo_argv`] — no
    /// `-p`), writing `password` to stdin (never argv/env), bounded by
    /// `timeout` (on timeout the WHOLE process group is SIGKILLed — the
    /// suite's `killProcessTree`).
    fn run(
        &self,
        argv: Vec<String>,
        password: String,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = SudoRun> + Send + 'static>>;
}

/// The real `SudoRunner`: spawn `sudo -S <argv>` on the desktop host (its
/// OWN process group on Unix — the suite's `detached: true`), write the
/// password to stdin, and SIGKILL the WHOLE process group on timeout (a
/// bare timeout would leave the timed-out root children alive).
pub struct RealSudoRunner;

impl SudoRunner for RealSudoRunner {
    fn run(
        &self,
        argv: Vec<String>,
        password: String,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = SudoRun> + Send + 'static>> {
        Box::pin(run_sudo_real(argv, password, timeout))
    }
}

/// The real sudo run (see [`RealSudoRunner`]).
async fn run_sudo_real(argv: Vec<String>, password: String, timeout: Duration) -> SudoRun {
    let Some((cmd0, rest)) = argv.split_first() else {
        return SudoRun {
            exit_code: -1,
            stdout: String::new(),
            stderr: "no command in argv".to_string(),
            timed_out: false,
            error: Some("no command in argv".to_string()),
        };
    };
    let mut cmd = tokio::process::Command::new(cmd0);
    cmd.args(rest);
    // Pipe ALL THREE streams: the password travels the stdin pipe, and
    // stdout / stderr are captured into the shared buffers below. The
    // tokio default is INHERIT — without the explicit `piped()`,
    // `child.stdout` is `None` (the capture below would panic) and the
    // password is never written at all.
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    // Kill the child (SIGKILL) if the run future is DROPPED mid-flight
    // (finding 3 — a `tokio::select!` dropping the future would otherwise
    // leave the elevated command running detached; `kill_on_drop` alone
    // kills the DIRECT child — the `SudoGroupGuard` below kills the whole
    // group, so a root command that forked further does not survive).
    cmd.kill_on_drop(true);
    // The child owns its process group (the suite's `detached: true`):
    // `setpgid(0, 0)` in the child (a single async-signal-safe libc call
    // — the only `unsafe` here) makes it the leader of a fresh group —
    // on timeout the WHOLE group is SIGKILLed (a root process that forked
    // further survives a bare kill of the child alone).
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return SudoRun {
                exit_code: -1,
                stdout: String::new(),
                stderr: String::new(),
                timed_out: false,
                error: Some(format!("failed to spawn {cmd0}: {e}")),
            }
        }
    };
    // Kill the WHOLE process group if the run is dropped mid-flight (finding
    // 3 — `kill_on_drop` alone kills only the direct child; a root command
    // that forked further survives a bare kill of the child alone). The guard
    // is DISARMED on every return path (a normal completion leaves the group
    // alone — a backgrounded grandchild the user wanted to keep running
    // survives; the timeout / kill paths already reaped the group, so a
    // second `kill(-pgid)` is a redundant double-kill / a pid-reuse hazard):
    // it fires ONLY on an abrupt drop (the future dropped mid-flight, before
    // the child was reaped).
    let mut group_guard = SudoGroupGuard { pid: child.id() };
    // The password travels via stdin ONLY (`sudo -S` reads it from there —
    // it never appears in argv or env). A fatal EPIPE (the child finished
    // without reading it — e.g. a NOPASSWD sudo) is NOT an auth failure:
    // the child's exit settles the run (the suite's EPIPE policy — record
    // only, don't fail), so a write ERROR is ignored here.
    //
    // The write is BOUNDED: if the child never reads stdin AND the
    // password exceeds the pipe buffer (~64 KiB), `write_all` would block
    // unboundedly (no timeout, no close-guard) — a 5 s cap maps a stuck
    // write to a process-level failure (kill + reap + `error: Some(…)`).
    // It is NOT an auth failure: the cached credential is kept (the
    // `sudo_run_flow` maps `error: Some(…)` to a clean tool failure and
    // never clears the cache on it).
    if let Some(mut stdin) = child.stdin.take() {
        let bytes = format!("{password}\n").into_bytes();
        let write = stdin.write_all(&bytes);
        let write = match tokio::time::timeout(Duration::from_secs(5), write).await {
            Ok(r) => r,
            Err(_) => {
                kill_process_group_and_reap(&mut child).await;
                // The group was just killed + reaped — disarm the guard (a
                // second `kill(-pgid)` on a reaped pgid is a redundant
                // double-kill / a pid-reuse hazard).
                group_guard.disarm();
                return SudoRun {
                    exit_code: -1,
                    stdout: String::new(),
                    stderr: String::new(),
                    timed_out: false,
                    error: Some("timed out writing password to sudo stdin".to_string()),
                };
            }
        };
        let _ = write;
        let _ = stdin.shutdown().await;
    }
    let mut stdout = child.stdout.take().expect("stdout handle");
    let mut stderr = child.stderr.take().expect("stderr handle");
    // Shared (chunked) buffers: the PARTIAL data survives a timeout (the
    // read tasks are dropped, the buffers keep what was read so far).
    let stdout_buf = Arc::new(StdMutex::new(Vec::new()));
    let stderr_buf = Arc::new(StdMutex::new(Vec::new()));
    let read = async {
        let (o, e) = tokio::join!(
            read_stream_into(&mut stdout, &stdout_buf),
            read_stream_into(&mut stderr, &stderr_buf),
        );
        (o, e, child.wait().await)
    };
    let result = tokio::time::timeout(timeout, read).await;
    let (stdout, stderr) = (
        String::from_utf8_lossy(&stdout_buf.lock().unwrap_or_else(|p| p.into_inner())).into_owned(),
        String::from_utf8_lossy(&stderr_buf.lock().unwrap_or_else(|p| p.into_inner())).into_owned(),
    );
    match result {
        Ok((o, e, w)) => {
            // A read failure (a broken pipe that is NOT an EPIPE) or a
            // `wait` failure: a clean transport failure (NOT an auth
            // failure — the cache is kept). Kill + reap (no zombie).
            let error = o
                .as_ref()
                .err()
                .or_else(|| e.as_ref().err())
                .or_else(|| w.as_ref().err())
                .map(|e| e.to_string());
            if let Some(error) = error {
                // Kill + reap ONLY when the `wait` did NOT succeed: when
                // `w` is `Ok` the process is already dead AND reaped — a
                // `kill` on a reaped pid is a bare `kill(pid, SIGKILL)`
                // with no reaped-state guard, and a reaped pid is free for
                // reuse (it could SIGKILL an unrelated process). A read
                // failure with a successful `wait` is just a transport
                // glitch (the child's own exit settles the run).
                if w.is_err() {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                }
                // The child is reaped (a `wait` failure, or the read failure
                // with a successful `wait` — the child's own exit settled the
                // run) — disarm the guard (a `kill(-pgid)` on a reaped pid is
                // a pid-reuse hazard; the group is left alone on a transport
                // failure, matching the "no group kill on a normal exit" rule).
                group_guard.disarm();
                return SudoRun {
                    exit_code: -1,
                    stdout,
                    stderr,
                    timed_out: false,
                    error: Some(error),
                };
            }
            let status = w.expect("wait succeeded (checked above)");
            // A NORMAL completion (no read / wait error): the child exited and
            // was reaped inside `read` — disarm the guard so the group is left
            // alone (a backgrounded grandchild the user wanted to keep running
            // survives; a bare group kill here is a pid-reuse hazard too).
            group_guard.disarm();
            SudoRun {
                exit_code: status.code().unwrap_or(-1),
                stdout,
                stderr,
                timed_out: false,
                error: None,
            }
        }
        Err(_) => {
            // Timeout: kill the WHOLE process group + reap (the shared
            // helper — consistent with the write-timeout kill above).
            kill_process_group_and_reap(&mut child).await;
            // The group was just killed + reaped — disarm the guard (a second
            // `kill(-pgid)` on a reaped pgid is a redundant double-kill / a
            // pid-reuse hazard).
            group_guard.disarm();
            SudoRun {
                // The suite's timeout sentinel (124).
                exit_code: 124,
                stdout,
                stderr,
                timed_out: true,
                error: None,
            }
        }
    }
}

/// Kill + reap a `run_sudo_real` child: SIGKILL the WHOLE process group
/// (the child is its own group on Unix — a negative pid targets the group;
/// ESRCH = the group is already gone — done), then the single-process kill
/// (belt-and-braces / non-Unix fallback), then the reap (no zombie). Shared
/// by the write-timeout and run-timeout paths so a stalled-write kill is
/// consistent with a run-timeout kill (a grandchild of the group leader
/// must not survive either).
async fn kill_process_group_and_reap(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

/// A drop guard that KILLS the WHOLE process group of a `run_sudo_real`
/// child (finding 3 — a dropped mid-run future must not leave the elevated
/// command running detached). The child owns its process group (the
/// `setpgid(0, 0)` in `pre_exec`), so a negative pid targets the group;
/// ESRCH = the group is already gone (no-op — a normal completion). It
/// complements `kill_on_drop(true)` (which kills the DIRECT child but not
/// its grandchildren): a root command that forked further survives a bare
/// kill of the child alone, but not a kill of the group.
///
/// The guard fires ONLY on an abrupt drop (the future dropped mid-flight,
/// before the child was reaped): `run_sudo_real` disarms it on every
/// return path (a normal completion leaves the group alone — a backgrounded
/// grandchild the user wanted to keep running survives; the timeout / kill
/// paths already reaped the group via `kill_process_group_and_reap`, so a
/// second `kill(-pgid)` is a redundant double-kill / a pid-reuse hazard).
struct SudoGroupGuard {
    pid: Option<u32>,
}

impl SudoGroupGuard {
    /// Disarm the guard (the child was reaped — a `kill(-pgid)` would be a
    /// pid-reuse hazard, or a redundant double-kill on a group already
    /// reaped by `kill_process_group_and_reap`). Called on a normal
    /// completion (the group is left alone) and on the timeout / kill paths.
    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for SudoGroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            #[cfg(unix)]
            {
                let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
            }
        }
    }
}

/// Read a stream to EOF into `buf` (chunked — bounded; the buffer is
/// shared so the partial data survives a timeout, see
/// [`run_sudo_real`]).
async fn read_stream_into<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &Arc<StdMutex<Vec<u8>>>,
) -> std::io::Result<()> {
    let mut chunk = [0u8; 8192];
    loop {
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend_from_slice(&chunk[..n]);
    }
    Ok(())
}

/// A drop guard that cleans up a `sudo_run_flow`'s `pending_sudo` entry
/// (finding 3 — a DROPPED flow: the `dispatch_tool` `select!`'s turn-cancel
/// arm drops the `sudo_run_flow` future, skipping its exit-path cleanup, so
/// the `:confirm` / `:password` oneshot entry leaked + the `interactive-request`
/// modal stayed open with no pending response). On `Drop` it (a) emits a
/// `interactive-request-close` event (the UI closes the modal) and (b) removes the
/// tracked key(s) (best-effort — a dropped runtime skips the removal). The
/// keys are tracked as they are inserted; a normal exit UNTRACKS them (the
/// entry is removed + the key is untracked — the guard's removal is then a
/// no-op, so it is idempotent), so `interactive-request-close` is emitted ONLY for
/// a genuinely dropped flow (the entry NOT removed — a completed flow, after
/// the user answered both sub-prompts, does NOT emit a stale close).
pub(super) struct SudoPromptCleanup {
    pending_sudo: PendingSudo,
    sink: Arc<dyn EventSink>,
    session_id: String,
    keys: Vec<String>,
}

impl SudoPromptCleanup {
    pub(super) fn new(
        pending_sudo: &PendingSudo,
        sink: &Arc<dyn EventSink>,
        session_id: &str,
    ) -> Self {
        Self {
            pending_sudo: pending_sudo.clone(),
            sink: sink.clone(),
            session_id: session_id.to_string(),
            keys: Vec::new(),
        }
    }
    /// Track a `pending_sudo` key (called as the entry is inserted).
    pub(super) fn track(&mut self, key: &str) {
        self.keys.push(key.to_string());
    }
    /// Untrack a `pending_sudo` key (called when the entry is removed on a
    /// normal exit — the `interactive-request-close` is then NOT emitted for it, so
    /// a completed flow does not emit a stale close for an already-answered
    /// sub-prompt; only a genuinely dropped flow (the entry NOT removed) emits
    /// the close).
    pub(super) fn untrack(&mut self, key: &str) {
        self.keys.retain(|k| k.as_str() != key);
    }
}

impl Drop for SudoPromptCleanup {
    fn drop(&mut self) {
        if self.keys.is_empty() {
            return;
        }
        // (a) Close the modal(s) (a `interactive-request` was emitted for each
        // sub-prompt; a drop must close it — pre-fix the modal stayed open
        // with no pending response, and a late answer got `Ok(true)` with
        // the send silently failing). The `requestId` is the key's suffix
        // after `"{session_id}/"` (`interactive_key` is `"{session_id}/{id}"`).
        let prefix = format!("{}/", self.session_id);
        for key in &self.keys {
            if let Some(request_id) = key.strip_prefix(&prefix) {
                self.sink.emit(
                    "interactive-request-close",
                    json!({
                        "sessionId": self.session_id,
                        "requestId": request_id,
                    }),
                );
            }
        }
        // (b) Remove the entries (best-effort — a dropped runtime skips it).
        let pending_sudo = self.pending_sudo.clone();
        let keys = std::mem::take(&mut self.keys);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let mut map = pending_sudo.lock().await;
                for key in keys {
                    map.remove(&key);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::run_sudo_real;
    use std::time::Duration;

    /// `run_sudo_real` with a HARMLESS binary (no real `sudo`, no root
    /// needed): the `argv[0]` is `/bin/sh` / `/bin/true` / a nonexistent
    /// path (the test asserts the binary exists or the spawn failure is the
    /// expectation).

    #[tokio::test]
    async fn run_sudo_real_an_empty_argv_is_a_failure() {
        let run = run_sudo_real(Vec::new(), "pw".to_string(), Duration::from_secs(5)).await;
        assert_eq!(run.exit_code, -1);
        assert!(!run.timed_out);
        assert_eq!(run.error.as_deref(), Some("no command in argv"));
    }

    #[tokio::test]
    async fn run_sudo_real_a_nonexistent_binary_is_a_spawn_failure() {
        assert!(
            !std::path::Path::new("/nonexistent/archimedes-test-binary-xyz").exists(),
            "the test binary must not exist"
        );
        let run = run_sudo_real(
            vec![
                "/nonexistent/archimedes-test-binary-xyz".to_string(),
                "arg".to_string(),
            ],
            "pw".to_string(),
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(run.exit_code, -1);
        assert!(!run.timed_out);
        assert!(
            run.error
                .as_deref()
                .is_some_and(|e| e.contains("failed to spawn")),
            "a spawn failure is recorded, got {:?}",
            run.error
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_sudo_real_exits_without_reading_stdin_is_not_an_error() {
        // `true` exits immediately without reading stdin. The password is
        // LARGER than the pipe buffer (~64 KiB), so the write cannot sit in
        // the buffer: it blocks until the child exits, then gets a fatal
        // EPIPE. The suite's EPIPE policy: record only, don't fail — the
        // child's exit settles the run (`error: None`), and the (new) 5 s
        // write timeout does NOT trip (the EPIPE arrives in milliseconds).
        assert!(
            std::path::Path::new("/bin/true").exists(),
            "/bin/true must exist"
        );
        let big_password = "x".repeat(200_000);
        let run = run_sudo_real(
            vec!["/bin/true".to_string()],
            big_password,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(run.exit_code, 0, "the child's exit settles the run");
        assert!(!run.timed_out);
        assert!(
            run.error.is_none(),
            "a fatal EPIPE (the child finished without reading) is record-only, got {:?}",
            run.error
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_sudo_real_timeout_kills_the_whole_process_group() {
        assert!(
            std::path::Path::new("/bin/sh").exists(),
            "/bin/sh must exist"
        );
        // The script echoes a marker, backgrounds a `sleep` (the
        // GRANDCHILD), records its pid, then blocks in the foreground — the
        // timeout must kill the WHOLE group (a bare kill of the child alone
        // would leave the grandchild alive), and the PARTIAL stdout (the
        // marker) must survive the timeout (the read tasks are dropped, the
        // shared buffers keep what was read so far).
        let grandchild_pid_file = std::env::temp_dir().join(format!(
            "archimedes-sudo-group-test-{}-{}.pid",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let script = format!(
            "echo partial; sleep 60 & echo $! > {}; sleep 60",
            grandchild_pid_file.display()
        );
        let run = run_sudo_real(
            vec!["/bin/sh".to_string(), "-c".to_string(), script],
            "pw".to_string(),
            Duration::from_millis(800),
        )
        .await;
        // The suite's timeout sentinel (124) + the flag; NO error (a timeout
        // is not a process-level failure).
        assert_eq!(run.exit_code, 124, "the suite's timeout sentinel");
        assert!(run.timed_out);
        assert!(run.error.is_none());
        // The PARTIAL stdout survives the timeout: the read tasks are
        // dropped mid-stream, but the shared buffers keep what was read so
        // far (the `from_utf8_lossy` conversion below the read phase picks
        // it up).
        assert!(
            run.stdout.contains("partial"),
            "the partial stdout (the pre-timeout echo) survived the timeout, got {:?}",
            run.stdout
        );
        // The GRANDCHILD (the backgrounded `sleep`) is dead too: the group
        // kill reached it. (Poll — the kernel reaps the orphaned, killed
        // grandchild promptly, but not instantaneously.)
        let grandchild_pid: u32 = std::fs::read_to_string(&grandchild_pid_file)
            .expect("the script wrote the grandchild's pid")
            .trim()
            .parse()
            .expect("a pid");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut gone = false;
        while std::time::Instant::now() < deadline && !gone {
            if unsafe { libc::kill(grandchild_pid as i32, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                gone = true;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        assert!(
            gone,
            "the grandchild (pid {grandchild_pid}) is dead — the group kill reached it"
        );
        let _ = std::fs::remove_file(&grandchild_pid_file);
    }

    /// (finding 3) A `run_sudo_real` future DROPPED mid-flight (the
    /// `dispatch_tool` `select!`'s turn-cancel arm) must KILL the whole
    /// process group (pre-fix the `Child` had no `kill_on_drop` + no group
    /// kill, so a root command that forked further kept running detached).
    /// `kill_on_drop(true)` kills the direct child; the `SudoGroupGuard`
    /// kills the group (a grandchild of the group leader must not survive).
    #[cfg(unix)]
    #[tokio::test]
    async fn run_sudo_real_a_dropped_future_kills_the_whole_process_group() {
        assert!(
            std::path::Path::new("/bin/sh").exists(),
            "/bin/sh must exist"
        );
        let grandchild_pid_file = std::env::temp_dir().join(format!(
            "archimedes-sudo-drop-test-{}-{}.pid",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        // The script echoes a marker, backgrounds a `sleep` (the GRANDCHILD),
        // records its pid, then blocks in the foreground — a DROP of the run
        // future must kill the WHOLE group (a bare kill of the child alone
        // would leave the grandchild alive).
        let script = format!(
            "echo partial; sleep 60 & echo $! > {}; sleep 60",
            grandchild_pid_file.display()
        );
        // SPAWN the run (a `Box::pin` would not poll the future, so the
        // spawn is what actually starts the command). The run blocks in the
        // foreground `sleep` until aborted.
        let run_task = tokio::spawn(run_sudo_real(
            vec!["/bin/sh".to_string(), "-c".to_string(), script],
            "pw".to_string(),
            Duration::from_secs(30),
        ));
        // Let the run start (the script runs, the grandchild is spawned).
        tokio::time::sleep(Duration::from_millis(300)).await;
        let grandchild_pid: u32 = std::fs::read_to_string(&grandchild_pid_file)
            .expect("the script wrote the grandchild's pid")
            .trim()
            .parse()
            .expect("a pid");
        // DROP the run future mid-flight (abort the task — pre-fix the child
        // kept running detached; `kill_on_drop` + the `SudoGroupGuard`
        // kill it now).
        run_task.abort();
        // The GRANDCHILD (the backgrounded `sleep`) is dead too: the drop
        // killed the WHOLE group. (Poll — the kernel reaps the orphaned,
        // killed grandchild promptly, but not instantaneously.)
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut gone = false;
        while std::time::Instant::now() < deadline && !gone {
            if unsafe { libc::kill(grandchild_pid as i32, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                gone = true;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        assert!(
            gone,
            "the dropped run killed the whole process group (the grandchild is dead)"
        );
        let _ = std::fs::remove_file(&grandchild_pid_file);
    }

    /// (finding 3) A `run_sudo_real` command that BACKGROUNDS a grandchild
    /// and COMPLETES NORMALLY (exit 0) must NOT kill the process group:
    /// the `SudoGroupGuard` is disarmed on a normal completion, so a
    /// backgrounded process the user wanted to keep running survives (a
    /// `nohup`-style background process — `nohup` does not change the
    /// process group, so a bare group kill on completion would reach it).
    /// The mirror of `run_sudo_real_timeout_kills_the_whole_process_group`
    /// (a timeout DOES kill the group; a normal completion does NOT).
    #[cfg(unix)]
    #[tokio::test]
    async fn run_sudo_real_a_normal_completion_does_not_kill_the_process_group() {
        assert!(
            std::path::Path::new("/bin/sh").exists(),
            "/bin/sh must exist"
        );
        // The script backgrounds a `sleep` (the GRANDCHILD), records its
        // pid, then EXITS (a normal completion — exit 0). The `sleep`'s
        // stdout / stderr are redirected to `/dev/null` (a backgrounded
        // process that inherits the piped streams would keep them open
        // until it exits — the run would time out, not complete): the run
        // completes when the `sh` process exits, so the `sleep` is still
        // running (the grandchild must survive a normal completion).
        let grandchild_pid_file = std::env::temp_dir().join(format!(
            "archimedes-sudo-normal-test-{}-{}.pid",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let script = format!(
            "sleep 5 > /dev/null 2>&1 & echo $! > {}; echo done",
            grandchild_pid_file.display()
        );
        let run = run_sudo_real(
            vec!["/bin/sh".to_string(), "-c".to_string(), script],
            "pw".to_string(),
            Duration::from_secs(5),
        )
        .await;
        // A NORMAL completion (exit 0, no timeout, no error).
        assert_eq!(run.exit_code, 0, "a normal completion exits 0");
        assert!(!run.timed_out);
        assert!(run.error.is_none());
        // The GRANDCHILD (the backgrounded `sleep`) is ALIVE: the normal
        // completion did NOT kill the group (the `SudoGroupGuard` is
        // disarmed on a normal completion — a backgrounded process the user
        // wanted to keep running survives). Give the (wrong) group kill a
        // moment to land, then check.
        let grandchild_pid: u32 = std::fs::read_to_string(&grandchild_pid_file)
            .expect("the script wrote the grandchild's pid")
            .trim()
            .parse()
            .expect("a pid");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let alive = unsafe { libc::kill(grandchild_pid as i32, 0) } == 0;
        assert!(
            alive,
            "the grandchild (pid {grandchild_pid}) is ALIVE — a normal completion must not kill the group"
        );
        // Clean up: kill the backgrounded grandchild (the test's process).
        let _ = unsafe { libc::kill(grandchild_pid as i32, libc::SIGKILL) };
        let _ = std::fs::remove_file(&grandchild_pid_file);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_sudo_real_captures_stdout_and_stderr() {
        // The capture path (piped stdout / stderr → shared buffers →
        // `from_utf8_lossy`) with REAL data: a benign command (no `sudo`,
        // no root) that writes to both streams.
        assert!(
            std::path::Path::new("/bin/sh").exists(),
            "/bin/sh must exist"
        );
        let run = run_sudo_real(
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "echo out; echo err >&2".to_string(),
            ],
            "pw".to_string(),
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(run.exit_code, 0, "a benign command exits 0");
        assert!(!run.timed_out);
        assert!(
            run.error.is_none(),
            "the child exits without reading stdin — the EPIPE is record-only, got {:?}",
            run.error
        );
        assert_eq!(run.stdout, "out\n", "stdout was captured end-to-end");
        assert_eq!(run.stderr, "err\n", "stderr was captured end-to-end");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_sudo_real_a_stalled_password_write_times_out() {
        // `sh -c "sleep 30"` never reads stdin, and the password is LARGER
        // than the pipe buffer (~64 KiB): the `write_all` would block
        // unboundedly without the (new) 5 s write timeout. The timeout maps
        // the stalled write to a process-level failure (kill + reap +
        // `error: Some(…)`), NOT an auth failure.
        assert!(
            std::path::Path::new("/bin/sh").exists(),
            "/bin/sh must exist"
        );
        let big_password = "x".repeat(200_000);
        let started = std::time::Instant::now();
        let run = run_sudo_real(
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "sleep 30".to_string(),
            ],
            big_password,
            Duration::from_secs(10),
        )
        .await;
        let elapsed = started.elapsed();
        assert_eq!(run.exit_code, -1);
        assert!(!run.timed_out, "a write timeout is not the run timeout");
        assert_eq!(
            run.error.as_deref(),
            Some("timed out writing password to sudo stdin"),
            "the stalled write is a process-level failure"
        );
        // Bounded by the 5 s write timeout (with a margin) — NOT the 10 s
        // run timeout, NOT unbounded.
        assert!(
            elapsed < Duration::from_secs(8),
            "the write was bounded, took {elapsed:?}"
        );
    }
}

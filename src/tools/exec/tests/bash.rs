use std::time::Duration;

use serde_json::json;
use tokio_util::sync::CancellationToken;

use super::support::*;
use crate::agent::tools::exec::{
    bash_exit_report, clamp_timeout_ms, execute_tool, DEFAULT_BASH_TIMEOUT, MAX_BASH_TIMEOUT,
    MAX_OUTPUT_BYTES,
};
use crate::agent::tools::types::{ContentBlock, ToolCtx, ToolResult};

/// Nanny CPU load: one `sh` busy-loop per logical CPU, SIGKILLed (and
/// reaped) when the guard drops, so no spinner outlives the test.
/// The count is bounded by `available_parallelism()` — this box's own
/// topology, which the load test needs in order to saturate it — and no
/// assertion reads it, so the test stays machine-independent.
#[cfg(target_os = "linux")]
struct CpuLoad {
    supervisor: std::process::Child,
}

#[cfg(target_os = "linux")]
impl CpuLoad {
    /// Spawn one detached busy-loop per logical CPU (`sh -c 'while :; do
    /// :; done'`), capped so a many-core box cannot fork hundreds.
    fn spin() -> Self {
        use std::os::unix::process::CommandExt;
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(32);
        // A single supervisor `sh` owns every spinner, so dropping the
        // guard kills the whole set with one negative-pid group kill.
        let script = format!(
            "trap 'kill 0' EXIT INT TERM; {}; wait",
            vec!["while :; do :; done &"; n].join(" ")
        );
        let supervisor = std::process::Command::new("sh")
            .arg("-c")
            .arg(&script)
            .process_group(0)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("the load spinners start");
        // Give the spinners a moment to actually be running.
        std::thread::sleep(Duration::from_millis(100));
        Self { supervisor }
    }
}

#[cfg(target_os = "linux")]
impl Drop for CpuLoad {
    fn drop(&mut self) {
        // The supervisor traps EXIT and `kill 0`s its own group; the
        // negative-pid kill here covers the case where it never ran.
        let pid = self.supervisor.id();
        unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        let _ = self.supervisor.kill();
        let _ = self.supervisor.wait();
    }
}

/// Pin the CALLING thread to cpu 0 (what `taskset -c 0` does to a
/// process): a child it spawns INHERITS the mask, so both the executor
/// and its `sh` compete for one saturated cpu — the exact shape of the
/// contention bug where EOF arrived before `waitpid` saw the exit.
#[cfg(target_os = "linux")]
fn pin_to_cpu0() {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(0, &mut set);
        let ret =
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set as *const _);
        assert_eq!(ret, 0, "failed to pin thread to cpu 0: {ret}");
    }
}

#[test]
fn tool_result_serializes_is_error_camel_case() {
    let r = ToolResult {
        content: vec![ContentBlock::Text {
            text: "x".to_string(),
        }],
        details: Some(json!({ "exitCode": 0 })),
        is_error: true,
    };
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["isError"], true, "the wire name is `isError`");
    assert!(
        v.get("is_error").is_none(),
        "no snake_case `is_error` on the wire"
    );
    // Round-trip.
    let back: ToolResult = serde_json::from_value(v).unwrap();
    assert_eq!(back, r);
}

// ── bash ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn bash_happy_path_serializes_camel_case() {
    let cwd = temp_cwd("bash");
    let r = execute_tool(&ctx(&cwd), "bash", &json!({ "command": "echo hi" })).await;
    let v = serde_json::to_value(&r).unwrap();
    assert_eq!(v["content"][0]["type"], "text");
    // The NEWLINE is the shell's, not the product's (`sh` writes `hi\n`,
    // the Windows `cmd /C` branch writes `hi\r\n`), so the claim here is
    // the TEXT and nothing else — trailing whitespace belongs to whichever
    // shell the product picked for this OS.
    assert_eq!(v["content"][0]["text"].as_str().unwrap().trim_end(), "hi");
    assert_eq!(v["details"]["exitCode"], 0);
    assert_eq!(v["details"]["truncated"], false);
    assert_eq!(v["details"]["cancelled"], false);
    assert_eq!(v["isError"], false);
}

/// Set on the re-executed test binary to tell it that it IS the child half
/// of [`bash_without_stdin_reaches_eof_instead_of_the_deadline`] — the same
/// sentinel trick `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`
/// uses, for the same reason: the condition under test belongs to the
/// PROCESS, so it has to be built around a fresh one.
const LIVE_STDIN: &str = "ARCHIMIDES_LIVE_STDIN_CHILD";
/// Printed by that child before it asserts anything, so the parent can tell
/// "the child ran and passed" from "the filter matched nothing" — a filter
/// that matches nothing exits 0, the trap the fabricated-mount test documents.
const LIVE_STDIN_RAN: &str = "LIVE-STDIN-CHILD-RAN";

/// (#35) A `bash` run reaches stdin **EOF** instead of the deadline, when it
/// inherits a stdin that is a **live pipe nobody writes to** — the Worker's
/// actual shape.
///
/// `cat` with no arguments reads stdin, so it is the smallest command whose
/// ONLY behaviour here is "block until stdin says stop". `exec_bash` pipes
/// stdout/stderr itself and never writes the child's stdin, and `tokio`'s
/// `output()`/`wait_with_output()` take only stdout/stderr
/// (`tokio-1.53.1/src/process/mod.rs:1457-1462`), never stdin — so before
/// `childproc::closed_stdin` the child inherited OUR stdin handle, which on
/// the desktop is the Worker's JSONL pipe (`worker/client.rs` owns that
/// writer for the Worker's whole life): live, and never written again. The
/// run then sat there until `timeout_ms` fired — and once the console window
/// disappeared (the rest of #35) there was nothing on screen to notice.
///
/// **Why this test re-execs itself.** A review (PR #36) caught that the
/// first version passed for the wrong reason, and it was right,
/// measurably: `exec_bash` does not touch stdin until `closed_stdin`, so the
/// only stdin a Tool child can inherit is **this process's fd 0**, and under
/// `cargo test` that is whatever cargo was started with. A CI step gets
/// `/dev/null`, which is already an EOF source, so the plain version of this
/// test was measured PASSING with `closed_stdin` deleted — a green test
/// guarding nothing. Deleting the call now reddens the census (invariant C)
/// AND this test, because the parent hands the child an fd 0 that is a
/// `std::io::pipe` it holds OPEN and never writes: the one stdin that tells
/// "we nulled it" apart from "we inherited it".
///
/// Observed RED with `closed_stdin` removed — the child reported
/// `exitCode: null`, `isError: true`, "The command timed out and was killed
/// at its deadline." after exactly 2.00 s — and GREEN with it. `cancelled`
/// stays `false` on that path (it is the USER-cancel flag; the deadline sets
/// `timed_out`), so `exitCode`/`isError`/the text are what catch it. Neither
/// half needs a window or a creation-flag getter to observe.
#[tokio::test]
async fn bash_without_stdin_reaches_eof_instead_of_the_deadline() {
    if std::env::var_os(LIVE_STDIN).is_some() {
        live_stdin_child_half().await;
        return;
    }

    // fd 0 for the child is a pipe whose write end stays in OURS, open and
    // silent, for the child's whole life. This is the entire failure
    // condition: handing the write end to the child, or dropping it here,
    // would give it EOF for free and put the vacuous test back.
    let (read_half, write_half) =
        std::io::pipe().expect("a pipe to stand in for the Worker's stdin");
    let exe = std::env::current_exe().expect("a test binary has a path");
    let mut cmd = crate::agent::childproc::command(&exe);
    cmd.arg(format!(
        "agent::tools::exec::tests::bash::{}",
        stringify!(bash_without_stdin_reaches_eof_instead_of_the_deadline)
    ));
    cmd.arg("--exact");
    // Without this the child's libtest harness CAPTURES its stdout and the
    // marker never reaches us — the child then looks like "ran nothing"
    // while having actually passed, which is the false alarm this guard
    // exists to avoid, not a real one.
    cmd.arg("--nocapture");
    cmd.env(LIVE_STDIN, "1");
    cmd.stdin(std::process::Stdio::from(read_half));
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    // The deadline below must not be able to orphan a child holding a pipe.
    cmd.kill_on_drop(true);

    let child = cmd.spawn().expect("the live-stdin test child starts");
    let out = match tokio::time::timeout(Duration::from_secs(180), child.wait_with_output()).await {
        Ok(out) => out.expect("the live-stdin test child is reaped"),
        Err(_) => {
            drop(write_half);
            panic!("the live-stdin test child did not finish within 180 s");
        }
    };
    // Released only after the child is reaped: while it is held the child's
    // `cat` has a writer to wait for.
    drop(write_half);

    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "the child's own assertions failed — on a live stdin the bash tool did NOT reach \
         EOF, which is #35's hang:\n{log}"
    );
    assert!(
        log.contains(LIVE_STDIN_RAN),
        "the child never ran the stdin assertions — a filter that matched nothing exits 0, \
         so the marker is the only proof it ran:\n{log}"
    );
}

/// The child half above: THIS process's fd 0 is the parent's live, silent
/// pipe, so the bash tool's child inherits exactly the stdin the shipped
/// Worker's child inherits. Prints the marker, then makes the assertions
/// `bash_happy_path_serializes_camel_case` makes for `echo hi`.
async fn live_stdin_child_half() {
    println!("{LIVE_STDIN_RAN}");
    let cwd = temp_cwd("bash-eof");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "cat", "timeout_ms": 2000 }),
    )
    .await;
    let details = r.details.as_ref().unwrap();
    assert_eq!(details["cancelled"], false);
    assert_eq!(
        details["exitCode"],
        0,
        "`cat` must reach EOF, not the 2 s deadline (is_error={}, text {:?}) — \
         `closed_stdin` is not reaching the Tool child",
        r.is_error,
        text_of(&r)
    );
    assert_eq!(text_of(&r).trim(), "");
    assert!(!r.is_error);
}

/// (ADR 0030 Task 5) `Shell=Sandboxed` runs the command INSIDE the
/// Landlock ruleset: writes inside the boundary land, and where the
/// kernel cannot confine, the tier FAILS CLOSED with a visible error
/// and the command never runs (the marker file is the proof either way
/// — an error message alone would pass while the command had already
/// landed unsandboxed). The confinement itself is asserted behaviorally
/// in `agent::tools::sandbox`'s tests; this is the executor's wiring.
#[tokio::test]
async fn a_sandboxed_shell_runs_confined_or_fails_closed_visibly() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("bash-sandboxed");
    let confined = ctx_policy(
        &cwd,
        FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Sandboxed,
            rules: Vec::new(),
        },
    );
    let r = execute_tool(&confined, "bash", &json!({ "command": "touch marker.txt" })).await;
    if can_confine() {
        // The kernel confines: the run happens, and a write INSIDE the
        // boundary is allowed (that is what "confined to the project"
        // means — see `sandbox.rs`'s behavioral tests for the deny).
        assert!(
            !r.is_error,
            "a confined run succeeds inside the boundary: {:?}",
            text_of(&r)
        );
        assert!(cwd.join("marker.txt").exists(), "the confined write landed");
    } else {
        // No sandbox available → a VISIBLE error and NO run.
        assert!(r.is_error, "an unavailable sandbox is an error, not a run");
        assert!(
            text_of(&r).contains("Sandboxed shell is unavailable"),
            "the error says why, got {:?}",
            text_of(&r)
        );
        assert!(
            !cwd.join("marker.txt").exists(),
            "the command must NOT have run (fail closed, not fail open)"
        );
    }
    // CONTRAST (so this cannot pass vacuously): the SAME command under
    // the default `shell: Allow` runs.
    let r = execute_tool(&ctx(&cwd), "bash", &json!({ "command": "touch ran.txt" })).await;
    assert!(!r.is_error, "the default policy still runs bash");
    assert!(cwd.join("ran.txt").exists());
}

/// The confined stub is scoped to the SHELL direction: a path-param
/// tool is unaffected by `shell: Sandboxed` (it is neither confined nor
/// refused — its direction is `reads`/`writes`).
#[tokio::test]
async fn a_sandboxed_shell_does_not_touch_the_path_tools() {
    use crate::agent::policy::{AccessPolicy, FilePolicy};
    let cwd = temp_cwd("bash-sandboxed-read");
    std::fs::write(cwd.join("a.txt"), "hello").unwrap();
    let confined = ctx_policy(
        &cwd,
        FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Sandboxed,
            rules: Vec::new(),
        },
    );
    let r = execute_tool(&confined, "read", &json!({ "path": "a.txt" })).await;
    assert!(!r.is_error, "a read is not gated by the shell policy");
    assert!(text_of(&r).contains("hello"));
}

/// The fixture is a POSIX-SHELL fixture, so it runs where a POSIX shell
/// is the one the product picks: `1>&2` + `;` sequencing + `exit 3` are
/// `sh` syntax, and the Windows branch of `exec_bash` spawns `cmd /C`,
/// which reads the same string as one `echo` with a bogus redirect — it
/// exits 0, so `is_error` was false on the Windows runner. The exit-code
/// DECISION is not unix-specific and stays covered everywhere by
/// `bash_exit_report_nonzero_exit_is_an_error_with_its_real_code`.
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_nonzero_exit_is_error() {
    let cwd = temp_cwd("bash-err");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "echo oops 1>&2; exit 3" }),
    )
    .await;
    assert!(r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["exitCode"], 3);
    assert!(text_of(&r).contains("oops"), "stderr is captured");
}

#[tokio::test]
async fn bash_cancel_kills_child() {
    let cwd = temp_cwd("bash-cancel");
    let token = CancellationToken::new();
    let c = ToolCtx {
        cwd: cwd.to_path_buf(),
        cancel: token.clone(),
        skill_roots: None,
        boundary: vec![cwd.to_path_buf()],
        file_policy: crate::agent::policy::FilePolicy::default(),
        protected: Vec::new(),
        // (T03) The two new `ToolCtx` fields (churn-only): no rules, no home.
        rules: Vec::new(),
        home: None,
        web: Default::default(),
        render_requester: None,
    };
    let handle = tokio::spawn(async move {
        let start = std::time::Instant::now();
        let r = execute_tool(&c, "bash", &json!({ "command": "sleep 5" })).await;
        (r, start.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    let (r, elapsed) = handle.await.unwrap();
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], true);
    assert!(r.is_error);
    assert!(
        elapsed < Duration::from_secs(4),
        "the child was killed (took {elapsed:?})"
    );
}

#[tokio::test]
async fn bash_timeout_kills_child() {
    let cwd = temp_cwd("bash-timeout");
    let start = std::time::Instant::now();
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "sleep 5", "timeout_ms": 300 }),
    )
    .await;
    assert!(r.is_error);
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "the timeout killed the child"
    );
}

/// The fixture is POSIX through and through: `( … ) &` backgrounding and
/// the process-group kill it exists to prove (`kill_bash_process_group`
/// SIGKILLs `-pid`, and on Windows there IS no process group — the
/// product's own Windows branch kills the direct child only). Under
/// `cmd /C` the command line does not even parse as written, so the
/// marker got written by a survivor and the assertion inverted.
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_backgrounded_grandchild_killed_with_group() {
    // Regression test for the unbounded-hang / orphaned-grandchild hole:
    // the `sh` parent exits immediately, but the backgrounded subshell
    // holds the pipes. Without a process-group kill, the grandchild
    // outlives `exec_bash` and writes the marker at the 3 s mark (and
    // a no-`timeout_ms` run used to wait ~584 years for the pipes' EOF).
    let cwd = temp_cwd("bash-bg");
    let start = std::time::Instant::now();
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({
            "command": "( sleep 3; echo alive >> marker.txt ) & echo done",
            "timeout_ms": 1000,
        }),
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the deadline ended the run (took {:?})",
        start.elapsed()
    );
    assert!(
        text_of(&r).contains("done"),
        "the captured output is returned: {:?}",
        text_of(&r)
    );
    // Wait past the moment an ORPHANED grandchild would have written the
    // marker (3 s) — with the process-group kill it was SIGKILLed.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        !cwd.join("marker.txt").exists(),
        "the backgrounded grandchild was killed with the process group"
    );
}

/// A child that CLOSES ITS OWN PIPES and then runs past its deadline
/// must still be killed. Pipe EOF is not an exit: with both pipes
/// closed the old kill gate (`stdout_open || stderr_open`) skipped the
/// process-group kill entirely, so `exec_bash` returned while the child
/// was still running — with a note claiming it "was killed at its
/// deadline". The marker is written only by a SURVIVING child.
/// `exec >&- 2>&-` is the fixture — POSIX fd redirection with no `cmd`
/// equivalent (`cmd` rejected the whole line: ">& was unexpected at this
/// time."), so there is no closed-pipe child to reap on Windows and the
/// run reported a parse error instead of a deadline. The Windows kill
/// path itself is covered by `bash_timeout_kills_child`, which passes
/// there with a shell-neutral `sleep 5`.
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_timeout_kills_a_child_that_closed_its_pipes() {
    let cwd = temp_cwd("bash-closed-pipes-timeout");
    let start = std::time::Instant::now();
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({
            "command": "exec >&- 2>&-; sleep 2; echo ALIVE >> marker.txt",
            "timeout_ms": 300,
        }),
    )
    .await;
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "the deadline ended the run (took {:?})",
        start.elapsed()
    );
    assert!(
        r.is_error,
        "a timed-out run is an error: TEXT={:?} DETAILS={:?}",
        text_of(&r),
        r.details
    );
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
    assert!(
        text_of(&r).contains("timed out"),
        "the text says it timed out: {:?}",
        text_of(&r)
    );
    // Wait past the moment an UNKILLED child would have written the
    // marker (2 s after spawn, well past the 300 ms deadline) — with
    // the process-group kill it never gets there.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !cwd.join("marker.txt").exists(),
        "the child was killed at its deadline before it could write"
    );
}

/// The cancel path applies the same rule: a cancelled child that had
/// already closed its pipes is still alive (pipe EOF is not an exit)
/// and must be killed with its group, not left running.
/// The cancel half of the same `exec >&- 2>&-` fixture (see
/// `bash_timeout_kills_a_child_that_closed_its_pipes`): `cmd /C` refuses
/// the redirection, so the child exited at once with `cancelled: false`
/// and the cancel never had a live process to race. The cancel path is
/// covered on Windows by `bash_cancel_kills_child` (`sleep 5`).
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_cancel_kills_a_child_that_closed_its_pipes() {
    let cwd = temp_cwd("bash-closed-pipes-cancel");
    let token = CancellationToken::new();
    let c = ToolCtx {
        cwd: cwd.to_path_buf(),
        cancel: token.clone(),
        skill_roots: None,
        boundary: vec![cwd.to_path_buf()],
        file_policy: crate::agent::policy::FilePolicy::default(),
        protected: Vec::new(),
        // (T03) The two new `ToolCtx` fields (churn-only): no rules, no home.
        rules: Vec::new(),
        home: None,
        web: Default::default(),
        render_requester: None,
    };
    let handle = tokio::spawn(async move {
        execute_tool(
            &c,
            "bash",
            &json!({
                "command": "exec >&- 2>&-; sleep 2; echo ALIVE >> marker.txt",
                "timeout_ms": 10_000,
            }),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    token.cancel();
    let r = handle.await.unwrap();
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], true);
    assert!(
        r.is_error,
        "a cancelled run is an error: TEXT={:?} DETAILS={:?}",
        text_of(&r),
        r.details
    );
    // Wait past the moment an UNKILLED child would have written the
    // marker — with the process-group kill it never gets there.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !cwd.join("marker.txt").exists(),
        "the cancelled child was killed with its process group"
    );
}

/// (issue #79 residual (a)) cmd's own `timeout /t` error — "ERROR: Input redirection is not
/// supported" — is a DOCUMENTED consequence of this tool's child policy, not a real defect:
/// `childproc::closed_stdin` forces stdin to NULL and `CREATE_NO_WINDOW` leaves the child with
/// no console input, so `timeout` cannot sleep. The bare sentence reads like a real error; this
/// test pins that a pure translator turns it into a diagnostic naming the policy and the workaround.
#[test]
fn cmd_input_redirection_is_diagnosed_as_the_no_console_input_tool_policy() {
    let cmd_error = "ERROR: Input redirection is not supported, exiting the process immediately";
    let note = crate::agent::tools::exec::bash::cmd_input_redirection_diagnostic(cmd_error);
    let note = note.unwrap_or_else(|| panic!("cmd's own error sentence must be translated"));
    assert!(
        note.contains("no console input"),
        "the cause is named: {note}"
    );
    assert!(
        note.contains("ping -n 20 127.0.0.1 > NUL"),
        "the workaround the description already recommends is restated: {note}"
    );
    // A line without cmd's sentence is untouched (a shape pin, not the effect).
    assert_eq!(
        crate::agent::tools::exec::bash::cmd_input_redirection_diagnostic("echo hi"),
        None
    );
    // The negative is pinned against FAILURE text, not just a success line
    // (reviewer round 2): a NON-ZERO-exit run whose text carries none of the
    // trigger sentences must stay untranslated too — the translator keys on
    // cmd's sentence alone, never on the exit code (that decision belongs to
    // `bash_exit_report`, whose plain `(Some(n), None)` arm keeps the run an
    // `is_error` with no note).
    assert_eq!(
        crate::agent::tools::exec::bash::cmd_input_redirection_diagnostic(
            "grep: cannot open 'missing.txt': No such file or directory"
        ),
        None,
        "a failing run's text with no trigger sentence must not be translated"
    );
}

// ── the no-console-input diagnostic, on the shell that prints the error ─
//
// `#[cfg(windows)]` for the same reason the quoting tests below carry it: only `cmd.exe` prints
// the sentence this test feeds the translator, so the test cannot exist on another platform and
// runs ONLY on CI's `windows-latest` leg (locally compile-verified by
// `cargo check --target x86_64-pc-windows-gnu --all-targets`). The host-side half of the same
// claim — that `exec_bash` attaches the diagnostic at all — is the cross-platform
// [`a_run_whose_text_carries_the_cmd_redirect_error_gets_the_diagnostic_as_its_first_line`].

/// (issue #79 residual (a), end-to-end) The real `timeout /t` run on Windows surfaces cmd's
/// raw error — and the tool result must ALSO carry the diagnostic the pure translator emits
/// (the no-console-input policy + the `ping` workaround). Deterministic on the CI runner:
/// `closed_stdin` + `CREATE_NO_WINDOW` guarantee the "Input redirection is not supported" path
/// (see the childproc module docs); a GREEN here means the diagnostic was attached to a real
/// run, not to a fixture line.
///
/// WHAT `is_error` ACTUALLY IS (reviewer round 2 — traced from `exec_bash`, not assumed):
/// the TIMEOUT command exits AT ONCE with the error (it cannot sleep without console input —
/// confirmed by MS/Broadcom/IBM write-ups), so `timeout_ms` NEVER fires: the read loop breaks on
/// the observed exit, `timed_out` stays `false`, and the report is built as
/// `bash_exit_report(false, false, Some(status), pipes_held)`. `cancelled` is false, the
/// `timed_out` arm is skipped, and the final `match (code, signal)` arm
/// `(Some(n), None) => (Some(n), true, None)` — "a normal non-zero exit needs no note — the
/// code IS the report" — is the ACTUAL arm handling it (the same arm the Linux `exit 3`
/// fixture drills).
///
/// ACTUAL RESULTING TEXT: `is_error: true`; `details.exitCode` = whatever NON-ZERO code this
/// `cmd` build's `timeout.exe` returned (the arm passes the real code through, no fabrication);
/// NO exit-report note (the arm's note is `None`); `cancelled: false`; and the model-facing
/// text is `cmd`'s own sentence followed by the diagnostic line.
///
/// WHY THE ASSERTION INSISTS ONLY ON NON-ZERO: the report's `(Some(n), None)` arm is an
/// ERROR for EVERY non-zero code, and the specific number `timeout` returns on this abort is
/// not pinned by any authoritative source available from Linux (MS's `timeout` docs list no
/// errorlevel for the redirect-abort; community write-ups say only "exits immediately"). A
/// guessed constant would RED on the CI leg the first time it disagreed with some `cmd` build —
/// exactly the blind spot the "match reality" review is guarding against. `is_error` + a
/// non-zero `exitCode` is what the arm ACTUALLY guarantees on every Windows build; if the
/// implementer observes a specific code on the CI leg, the assertion may be STRENGTHENED to it
/// in the implementing PR, citing the observed value.
#[cfg(windows)]
#[tokio::test]
async fn bash_timeout_t_reports_a_no_console_input_diagnostic() {
    let cwd = temp_cwd("bash-timeout-diag");
    let r = execute_tool(&ctx(&cwd), "bash", &json!({ "command": "timeout /t 1" })).await;
    let text = text_of(&r);
    assert!(
        r.is_error,
        "a failed `timeout /t` run is an error — the report's `(Some(n), None)` arm \
         (the wait did not happen): TEXT={text:?}"
    );
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
    let code = &r.details.as_ref().unwrap()["exitCode"];
    assert!(
        code.is_i64() && code.as_i64().unwrap() != 0,
        "the exit code is the child's REAL non-zero code (never fabricated, never 0): \
         DETAILS={:?}",
        r.details
    );
    assert!(
        text.contains("Input redirection is not supported"),
        "the repro itself must appear (cmd's own sentence): {text:?}"
    );
    assert!(
        text.contains("no console input"),
        "the diagnostic must name the no-console-input policy to the model: {text:?}"
    );
    assert!(
        text.contains("ping -n 20 127.0.0.1 > NUL"),
        "the diagnostic must restate the workaround: {text:?}"
    );
}

/// (issue #79 residual (a), the OTHER half of the pair) The sibling above pins that the failure
/// recommends a replacement, and three tests pin that the replacement STRING is present —
/// [`cmd_input_redirection_is_diagnosed_as_the_no_console_input_tool_policy`], the same assertion
/// inside [`bash_timeout_t_reports_a_no_console_input_diagnostic`], and the first-line pin inside
/// [`a_run_whose_text_carries_the_cmd_redirect_error_gets_the_diagnostic_as_its_first_line`]. None
/// of them ever showed the replacement WORKS. THIS is the behaviour leg: on the SAME child that
/// cannot run `timeout`, `ping -n 3 127.0.0.1 > NUL` must WAIT, EXIT 0, and need NO console input.
/// Without it the tool would be pointing the model at a second dead end on the one platform it
/// advises it for.
///
/// WHY THE LOWER BOUND IS THE LOAD-BEARING ASSERTION: `timeout`'s documented failure mode on this
/// child is to exit AT ONCE — the sibling test's own doc records it, and `cmd` says so in its own
/// sentence ("…exiting the process immediately"). A test that only asserted the command RAN, or
/// only bounded the run from ABOVE, would pass on that broken behaviour and prove nothing. Only a
/// FLOOR distinguishes a real wait from an instant exit, so 1200 ms is what this test is actually
/// about; the other three assertions characterise the wait, they do not establish it.
///
/// FLOOR MARGIN, deliberately loose: `-n 3` sleeps ~2 s (one interval between each reply) and the
/// floor sits at 1200 ms. Ping's delay is a TIMER, not CPU work, so a loaded runner can overshoot
/// but cannot undershoot — there is no contention mechanism that makes a timer fire early. There
/// is therefore NO upper bound here, and one must never be added: an upper bound is how a
/// wall-clock test becomes a flake (the shape this repo already suffers in
/// `src/**/Reasoning.test.tsx`'s ratio assertion — not imported into Rust).
///
/// `-n 3`, NOT the recommended `-n 20`: three pings prove the mechanism in ~2 s; twenty would cost
/// ~19 s of CI time to prove the SAME thing. This test therefore proves the SHAPE — `ping` needs
/// no console input, `> NUL` is honoured by `cmd`, exit code 0 — and NOT the literal count in the
/// recommended string. It deliberately makes NO assertion on that string: presence is already
/// owned by the three tests named above, and repeating it here would be a vacuous second pin.
///
/// ASSERTION 3 IS NOT THE SUCCESS PROOF. Empty text is evidence that the `> NUL` redirection was
/// HONOURED (the reply lines never reached the pipe, so output stays small); it is NOT evidence
/// that the run succeeded, because a mis-reported FAILING run's text would be empty too. Success
/// rests on assertions 1, 2 and 4 between them: exit 0 and `!is_error` (it worked), ≥1200 ms (it
/// waited), `cancelled: false` (the wait ended by itself, the deadline never fired).
///
/// ⚠ `#[cfg(windows)]` — `ping`'s `-n` flag and `> NUL` are `cmd`/Windows shapes, so this test can
/// exist on no other platform. It RUNS only on CI's `windows-latest` leg; locally the ceiling is
/// `cargo check --target x86_64-pc-windows-gnu --all-targets`, a compile gate and not a run. Do NOT
/// relax the gate to make it execute here: a test that silently does not run on the platform it
/// guards is the blind spot issue #79 was filed about (see the warning block below).
#[cfg(windows)]
#[tokio::test]
async fn bash_ping_workaround_actually_waits_without_console_input() {
    let cwd = temp_cwd("bash-ping-waits");
    let start = std::time::Instant::now();
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "ping -n 3 127.0.0.1 > NUL" }),
    )
    .await;
    // 1. It SUCCEEDS, where `timeout` cannot.
    assert!(
        !r.is_error,
        "`ping` is the workaround this tool RECOMMENDS, so it must succeed where `timeout /t` \
         fails: TEXT={:?} DETAILS={:?}",
        text_of(&r),
        r.details
    );
    assert_eq!(
        r.details.as_ref().unwrap()["exitCode"],
        0,
        "`ping` exits 0 on this console-less child (the recommendation would be a second dead \
         end if it did not): DETAILS={:?}",
        r.details
    );
    // 2. THE LOAD-BEARING ASSERTION: it WAITED. `timeout`'s failure is to exit at once, so only a
    //    floor separates the workaround that sleeps from the one that fails instantly.
    assert!(
        start.elapsed() >= Duration::from_millis(1200),
        "`ping` must actually SLEEP (three replies ~2 s apart) — an ELAPSED FLOOR is the only \
         thing that distinguishes a real wait from `timeout`'s exit-immediately failure mode: \
         ELAPSED={:?} TEXT={:?}",
        start.elapsed(),
        text_of(&r)
    );
    // 3. Evidence that `> NUL` was HONOURED (output stays small) — NOT evidence of success.
    assert!(
        text_of(&r).trim().is_empty(),
        "`> NUL` is honoured by `cmd` (our `/d /s /c` raw tail passes the redirection through \
         untouched), so the reply lines never reach the pipe and the model-facing text is empty: \
         TEXT={:?}",
        text_of(&r)
    );
    // 4. It finished on its own — the deadline never fired.
    assert_eq!(
        r.details.as_ref().unwrap()["cancelled"],
        false,
        "the wait ended by itself; the bash deadline never fired: DETAILS={:?}",
        r.details
    );
}

// ── the quoting contract, on the shell that actually re-parses it ─────
//
// `#[cfg(windows)]` rather than `#[cfg(not(unix))]`: these spawn `cmd.exe`,
// which exists on no other platform, and this app ships Windows / macOS /
// Linux only, so `not(unix)` IS Windows for every artifact `release.yml`
// builds (the product arm stays `not(unix)` because that is the predicate
// `exec_bash` branches on).
//
// ⚠ These two tests are the fix's ONLY behavioural proof, and they run ONLY on
// CI's `windows-latest` leg. Locally they are COMPILE-verified by
// `cargo check --target x86_64-pc-windows-gnu --all-targets`. Do NOT re-gate
// them behind a unix gate and do NOT add a `return`-style skip: a test that
// silently does not run on the platform it guards is the exact blind spot issue
// #79 was filed about.
//
// Do NOT "improve" them into an assertion on `cmd.as_std().get_args()`: std
// stores `Arg::Regular` and `Arg::Raw` so that both yield the identical
// `OsStr`, so such a test passes with `arg` AND with `raw_arg` alike. It looks
// like proof and is not. Census invariant E (in `agent/childproc.rs`) plus the
// two tests below are the real pair.

/// Issue #79, the shape the reporter hit first: an inline double quote must
/// reach `cmd.exe` intact.
///
/// `cmd`'s `echo` prints its operand verbatim, QUOTES INCLUDED, so the quotes in
/// the OUTPUT are the evidence that the string arrived with its own quoting
/// untouched. Under `Command::arg` the child received `echo \"a b\"` (std's
/// `append_arg` escapes every embedded `"` as `\"` and `cmd /s` strips only the
/// OUTER quote pair it added) and printed `\"a b\"`.
///
/// PREDICTED RED on Windows before the fix, derived from `append_arg`'s rules
/// (the `2n+1` backslash loop) plus `cmd`'s documented quote handling — the same
/// derivation the issue made and confirmed with its own `echo` reproduction. It
/// cannot be OBSERVED on a Linux host: `cmd.exe` is not here, and std renders the
/// command line inside `CreateProcess` with no getter. Confirmed on CI's
/// `windows-latest` leg.
#[cfg(windows)]
#[tokio::test]
async fn bash_passes_inline_double_quotes_through_to_cmd_verbatim() {
    let cwd = temp_cwd("bash-cmd-quotes");
    let r = execute_tool(&ctx(&cwd), "bash", &json!({ "command": "echo \"a b\"" })).await;
    assert!(!r.is_error, "the run succeeded: {:?}", text_of(&r));
    assert_eq!(r.details.as_ref().unwrap()["exitCode"], 0);
    assert_eq!(
        text_of(&r).trim_end(),
        "\"a b\"",
        "the quotes survive: {:?}",
        text_of(&r)
    );
}

/// Issue #79's FALSE-SUCCESS shape, reproduced with a `cmd` BUILTIN so the test
/// needs no third-party executable and cannot depend on a runner's tooling: a
/// quoted operand inside `if exist`, asserted on the OUTPUT and NOT on the exit
/// code.
///
/// Under escaping the line arrives as `if exist \"%SystemRoot%\System32\cmd.exe"
/// (echo FOUND)`; the literal path then starts with a backslash, the condition is
/// FALSE, `echo FOUND` never runs — **and `cmd` still exits 0**. That is exactly
/// what the issue reported for `start /B "" cmd /c "…"`, which printed
/// `Access is denied.` after its marker and still reported `exitCode: 0`, so the
/// agent told the user a build was "running in the background". The assertion is
/// on the output precisely BECAUSE the exit code is the thing that lies.
///
/// `%SystemRoot%\System32\cmd.exe` exists on every Windows including the CI
/// runners, and `cmd` expands environment variables inside quotes.
///
/// PREDICTED RED on Windows before the fix (same derivation as
/// [`bash_passes_inline_double_quotes_through_to_cmd_verbatim`]); confirmed on
/// CI's `windows-latest` leg. If CI's first run of THIS test comes back RED
/// after the fix, check first whether that runner expands `%SystemRoot%` inside a
/// quoted operand; if it does not, replace the variable with a literal path in
/// the fixture — do NOT weaken the assertion to the exit code.
#[cfg(windows)]
#[tokio::test]
async fn bash_an_inline_quoted_path_still_takes_the_branch() {
    let cwd = temp_cwd("bash-cmd-if-exist");
    let cmd = r#"if exist "%SystemRoot%\System32\cmd.exe" (echo FOUND)"#;
    let r = execute_tool(&ctx(&cwd), "bash", &json!({ "command": cmd })).await;
    assert!(
        text_of(&r).contains("FOUND"),
        "the quoted operand must reach `cmd` un-escaped or the branch is silently \
         skipped WHILE STILL EXITING 0 — issue #79's false success: {:?}",
        text_of(&r)
    );
}

/// (issue #79 residual R1, the contract the false success VIOLATED) A backgrounded launch that
/// reports a clean success must have ACTUALLY STARTED something. Mechanism-free on purpose: it
/// claims nothing about who holds a file lock, only about the observable pairing of a reported
/// success with evidence of a start.
///
/// WHY THIS EXISTS AND THE TEST ABOVE DOES NOT COVER IT: [`bash_an_inline_quoted_path_still_takes_the_branch`]
/// pins the same "the exit code is the thing that lies" shape on the QUOTING path. Nothing pinned
/// the BACKGROUNDED LAUNCH itself — `start /B prog.bat`, the command the issue names verbatim and
/// the symptom the issue says must NOT be closed by the quoting fix. This test is also the repro
/// harness §Gaps records as not existing ("whoever reproduces it on a real Windows box owns the
/// question"): it is the first thing in this repo that drives `start /B` at all.
///
/// THE FIXTURE IS THE REPORTED COMMAND, NOT AN INVENTED ONE. `std::fs::write` drops a
/// `marker.bat` that appends `RAN` to `marker.txt`, and the tool runs `start /B marker.bat` — the
/// issue's `start /B prog.bat` shape with a name local to the scratch dir. `@echo off` keeps the
/// echo of each command line out of the captured text. The line endings are EXPLICIT `\r\n`: a
/// batch file with bare `\n` is not reliably parsed by `cmd`, so a LF-only fixture would be testing
/// the parser instead of the launcher.
///
/// DO NOT ADD A `""` TITLE SLOT. The fixture is verbatim `start /B marker.bat` with NO quotes, so
/// `start`'s "first quoted token is the window title" rule is not in play here at all. That trap is
/// real — it is why the Windows `BASH_DESCRIPTION` (`agent/harness/tool_specs.rs`) tells the model
/// never to pass `""` there — but prepending `""` "helpfully" would change WHAT THIS TEST MEASURES
/// (it would exercise the title-slot path instead of the reported one). Keep the command byte-exact.
///
/// `/B` STAYS. `cmd /c start` with NO `/B` allocates its own console regardless of `CREATE_NO_WINDOW`
/// (§6's standing caveat: a handle-ownership question, not another face of the window bug this doc
/// is about), so dropping `/B` would both change what is measured and pop a window on the runner.
///
/// THE ASSERTION IS A MATERIAL IMPLICATION, AND THAT IS THE WHOLE POINT: `code != Some(0) ||
/// marker_exists` fails on exactly ONE state — **exit 0 with nothing started** — which is precisely
/// what the issue reported (`Access is denied.` printed after the agent's echo marker, `exitCode: 0`
/// reported, no log file, no process, the agent telling the user a build was "running in the
/// background"). Every other cell of the truth table passes: a non-zero code, or a start with no
/// reported status, are both honest outcomes.
///
/// `code == None` ALSO SATISFIES THE LEFT SIDE, AND THAT IS INTENTIONAL. `as_i64()` yields `None`
/// for a `Value::Null` `exitCode` — an UNOBSERVED status, e.g. a run killed at its deadline — and
/// `None != Some(0)` is true, so the implication passes without consulting the marker. An unobserved
/// status is not a *clean success*, so the contract simply does not apply to it. This is exactly why
/// the sibling [`bash_timeout_t_reports_a_no_console_input_diagnostic`] uses the stricter
/// `code.is_i64() && code.as_i64().unwrap() != 0` form: a later author who STRENGTHENS this to a
/// pinned exit code must keep that `is_i64()` guard, so "real non-zero" stays distinguishable from
/// "null".
///
/// THAT SLACK IS ONLY SAFE BECAUSE TWO ASSERTIONS RUN BEFORE IT, and they are the reason it cannot
/// turn a hang into a green test. This test passes no `timeout_ms`, so the default deadline applies;
/// a grandchild inheriting the piped stdout/stderr could hold the run to that deadline, which makes
/// `exitCode` `null` and satisfies the implication WITHOUT the marker being consulted — a VACUOUS
/// pass, a hang reported as coverage. `cancelled == false` closes the CANCEL route (it is written only
/// by the cancel arm of the select loop), and a `!text.contains("timed out")` guard closes the
/// DEADLINE route, which `cancelled` does NOT: the deadline sets a separate `timed_out` flag that
/// reaches the model-facing NOTE but never `details` — measured on this branch, a deadline run reports
/// `{"cancelled": false, "exitCode": null}`. With both in place the `None` the implication forgives is
/// genuinely "an unobserved status that is neither a cancel nor a timeout", which is the only case this
/// paragraph excuses. Do NOT replace either guard with an explicit small `timeout_ms`: that installs
/// the deadline as the expected outcome and manufactures the vacuous path instead of closing it.
///
/// DO NOT OVER-CLAIM WHAT THIS DETECTS. The implication's job is narrow — the R1 contract, exit 0 ⇒
/// something started. It is NOT the detector for a hypothetical "`exec_bash` always reports 0" bug;
/// that is pinned by the many existing asserts in this file that require a SPECIFIC code (e.g.
/// [`bash_passes_inline_double_quotes_through_to_cmd_verbatim`] asserts `exitCode == 0`,
/// [`bash_timeout_t_reports_a_no_console_input_diagnostic`] asserts a non-zero one). What makes THIS
/// test non-vacuous is only that the hard case — exit 0 with no marker — is the reported symptom,
/// and the implication fails on it. Say the narrow thing.
///
/// THE MARKER'S CONTENTS ARE ASSERTED WHEN IT EXISTS, so a phantom zero-byte `marker.txt` created by
/// anything at all cannot satisfy the implication in the batch's place.
///
/// THE FAILURE MESSAGE CARRIES A `read_dir` LISTING OF THE SCRATCH DIR on purpose: if `marker.txt`
/// showed up OUTSIDE it, that listing is what tells us `start` changed the working directory — a
/// different finding, but an equally useful one.
///
/// THIS TEST MAKES TWO ASSERTIONS, NOT THREE — the third one that used to sit here is DELETED, and
/// the reason is worth keeping so nobody re-adds it. It read
/// `if text.contains(X) { assert!(!text.trim().is_empty() && text.contains(X)) }`, and a guard of
/// that shape CANNOT FAIL: `text.contains(X)` is the condition that PRODUCED `X`, and a text holding
/// a non-empty sentence is non-empty, so both halves of the body were true by construction on every
/// path — including the `None` path, which skipped the body outright. It looked like coverage and
/// asserted nothing. The claim it gestured at — that our layer does not swallow one of the issue's
/// error sentences — is real and worth pinning, but the only honest way to pin it is to construct a
/// run whose text is KNOWN to carry the sentence, so the assertion is checked against something the
/// output scan did not supply. That needs no Windows at all, so the claim now lives in the
/// cross-platform [`the_bash_tool_passes_issue_79_error_sentences_through_verbatim`], which runs on
/// Linux and macOS as well as on CI's Windows leg. NEITHER test asks `exec_bash` to PROMOTE an
/// exit-0-with-error-text run into an error — that promotion would be a `bash_exit_report` change
/// (its `(Some(n), None)` arm — "a normal non-zero exit needs no note — the code IS the report") and is
/// explicitly OUT OF SCOPE on both sides of the move. That is NOT the same as "neither looks at
/// `is_error`": this test never touches `is_error`, and the moved-to test asserts `!r.is_error`, but it
/// asserts it because `echo` genuinely exits 0 — that clean code riding alongside an error sentence is
/// what makes the run #79's shape, not a claim about what the tool ought to conclude.
///
/// ⚠ `#[cfg(windows)]`, and BOTH OUTCOMES ON CI ARE FINDINGS — a later reader must not "fix" a red
/// run of this test. Locally the ceiling is `cargo check --target x86_64-pc-windows-gnu
/// --all-targets`, a compile gate and NOT a run: there is no host for it, not Linux and not wine.
/// The proof leg is CI's `windows-latest` run, and §Gaps records which of these it gave:
/// - **RED** → the lock REPRODUCED. Record the verbatim output and the exit code in §Gaps. Do NOT
///   weaken the assertion, do NOT add a retry, do NOT invent a mechanism. The issue then has, for
///   the first time, an observed repro with real text — the artefact a fix needs.
/// - **GREEN** → the lock does NOT reproduce in CI's DEBUG build. That narrows R1 to the packaged
///   RELEASE bundle — the GUI-subsystem binary (`main.rs:2`,
///   `cfg_attr(not(debug_assertions), windows_subsystem = "windows")`) that CI never builds — the
///   same debug-vs-release blind spot §Gaps already records for the window bug.
#[cfg(windows)]
#[tokio::test]
async fn bash_start_b_does_not_report_success_without_starting_something() {
    let cwd = temp_cwd("bash-start-b");
    // EXPLICIT \r\n — `cmd` does not reliably parse a batch file with bare \n.
    std::fs::write(
        cwd.join("marker.bat"),
        "@echo off\r\necho RAN >> marker.txt\r\n",
    )
    .unwrap();
    // Verbatim `start /B marker.bat` — NO `""` title slot, `/B` kept. See the doc comment.
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "start /B marker.bat" }),
    )
    .await;
    let text = text_of(&r);

    // `start /B` is ASYNCHRONOUS: it hands off and returns. A single immediate `exists()` would be
    // a race, not a test, so poll the scratch dir for up to ~5 s.
    let marker = cwd.join("marker.txt");
    let mut marker_exists = marker.exists();
    for _ in 0..50 {
        if marker_exists {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        marker_exists = marker.exists();
    }

    // 0. THE RUN FINISHED ON ITS OWN — this is what makes the `None` slack in the implication below
    //    SAFE instead of VACUOUS. This test passes no `timeout_ms`, so the DEFAULT deadline applies,
    //    and a grandchild that INHERITED the piped stdout/stderr handles and held them open could take
    //    the run to that deadline; on that path `exitCode` becomes `null`, `code == None` satisfies the
    //    left side WITHOUT consulting the marker, and a HANG would be reported as a GREEN test. Two
    //    facts close the two routes into that cell:
    //
    //    (a) `cancelled == false` — the cancel arm of `exec_bash`'s select loop is the ONLY writer of
    //        `cancelled` (`exec/bash.rs`, the `ctx.cancel.cancelled()` arm), so this rules out a run
    //        that was cancelled out from under the test.
    //    (b) the captured text carries NO deadline note — and this half is LOAD-BEARING, because
    //        `cancelled` does NOT cover the deadline. MEASURED on this branch: `sleep 5` with
    //        `timeout_ms: 300` reports `{"cancelled": false, "exitCode": null}` — the deadline arm sets
    //        the SEPARATE `timed_out` flag, which `bash_exit_report` turns into the note
    //        "The command timed out and was killed at its deadline." but which NEVER reaches `details`
    //        (`exec_bash` emits only `exitCode` / `truncated` / `cancelled`). So a deadline run would
    //        sail past (a), and `contains("timed out")` on the model-facing text is the only thing that
    //        sees it. `timed_out`'s every exit-`None` branch carries that sentence, so (a)+(b) together
    //        mean the `None` the implication still forgives is genuinely "an unobserved status that is
    //        neither a cancel nor a timeout" — the case the doc comment describes.
    //
    //    DELIBERATELY NOT AN EXPLICIT `timeout_ms`: setting a small one would MANUFACTURE the vacuous
    //    path these two assertions exist to close — a deadline would become the expected outcome rather
    //    than something the test proves did not happen.
    assert_eq!(
        r.details.as_ref().unwrap()["cancelled"], false,
        "the run must not have been cancelled: a cancelled run reports a `null` exitCode, and \
         `code == None` satisfies the implication below WITHOUT consulting the marker, so a cancelled \
         run would pass this test vacuously: TEXT={text:?} DETAILS={:?}",
        r.details
    );
    assert!(
        !text.contains("timed out"),
        "the run must not have reached its deadline: `timed_out` is NOT in `details` (only \
         `cancelled` is, and the deadline arm leaves it FALSE — measured on this branch), so the \
         deadline note in the captured text is the only way to tell \"the status was never observed\" \
         from \"the run hung and we killed it\". A hang here would make the implication below pass \
         vacuously on `code == None`: TEXT={text:?} DETAILS={:?}",
        r.details
    );

    // 1. THE CONTRACT, as a material implication: a reported exit 0 ⇒ something started.
    let code = r.details.as_ref().unwrap()["exitCode"].as_i64();
    assert!(
        code != Some(0) || marker_exists,
        "a backgrounded launch that reports exit code 0 must have started something (issue #79's \
         false success: exitCode 0 with no process and no log file). The dir listing below is how \
         we tell \"nothing started\" from \"it started somewhere else\": TEXT={text:?} \
         DETAILS={:?} CWD={:?}",
        r.details,
        std::fs::read_dir(&cwd)
            .map(|rd| rd
                .filter_map(Result::ok)
                .map(|e| e.path())
                .collect::<Vec<_>>())
            .unwrap_or_default(),
    );

    // 2. If it started, the BATCH ran — not merely "a file appeared".
    if marker_exists {
        let contents = std::fs::read_to_string(&marker).unwrap_or_default();
        assert!(
            contents.contains("RAN"),
            "the marker's CONTENTS are the proof `marker.bat` actually ran — a zero-byte \
             `marker.txt` from any other source would satisfy the implication above while proving \
             nothing: CONTENTS={contents:?}"
        );
    }
}

/// (issue #79 residual (a), the WIRING) The pure translator is pinned by
/// [`cmd_input_redirection_is_diagnosed_as_the_no_console_input_tool_policy`] and the presence of
/// cmd's sentence in a REAL run is pinned on the Windows CI leg by
/// [`bash_timeout_t_reports_a_no_console_input_diagnostic`]. THIS pins the middle fact — that
/// `exec_bash` ATTACHES the diagnostic to a real run's captured text, as the first thing the model
/// sees and on its own line — on a host that has no `cmd.exe`, by emitting cmd's exact sentence
/// from a plain `printf`. Cross-platform on purpose: the wiring in `exec_bash` is not `cfg`-gated,
/// so it must not rest on a CI-only test.
///
/// The run itself SUCCEEDS (this is `printf`, not `timeout`): the diagnostic is a TEXT-axis fact and
/// never an exit-status claim — `bash_exit_report` decides `is_error`, untouched.
#[tokio::test]
async fn a_run_whose_text_carries_the_cmd_redirect_error_gets_the_diagnostic_as_its_first_line() {
    let cwd = temp_cwd("bash-redirect-wiring");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "printf 'ERROR: Input redirection is not supported, exiting the process immediately\\n'" }),
    )
    .await;
    assert!(
        !r.is_error,
        "the command SUCCEEDED (printf exits 0) — the diagnostic adds no error and no note: {:?}",
        text_of(&r)
    );
    let text = text_of(&r);
    let first_line = text.lines().next().unwrap_or_default();
    assert!(
        first_line.starts_with("(note) `timeout /t` cannot wait here")
            && first_line.contains("no console input")
            && first_line.ends_with("`ping -n 20 127.0.0.1 > NUL`."),
        "the diagnostic is the FIRST LINE of the result (prefixed, complete, on a line of its \
         own — the deliverable is a diagnostic LINE, so it must not glue onto the captured \
         text): {text:?}"
    );
    assert!(
        text.contains("\nERROR: Input redirection is not supported"),
        "the captured text is kept (never replaced) and still starts its own line after the \
         diagnostic: {text:?}"
    );
}

/// (issue #79 residual R1, the claim the false success actually violates) When a command's output
/// CARRIES one of #79's error sentences, `exec_bash` must pass it through to the model INTACT. The
/// false success was never only a wrong number: `exitCode: 0` told the agent the build had started,
/// and `Access is denied.` was the only thing in the result that said otherwise — so if our layer
/// swallowed that sentence, the model would have NOTHING to disbelieve the code with. The model must
/// be able to SEE the error. THIS is the pin for that.
///
/// IT REPLACED A VACUOUS BLOCK. The `#[cfg(windows)]`
/// [`bash_start_b_does_not_report_success_without_starting_something`] used to carry a third
/// assertion shaped `if text.contains(X) { assert!(!text.trim().is_empty() && text.contains(X)) }`.
/// A guard of that form CANNOT FAIL: `text.contains(X)` is the condition that PRODUCED `X`, and a
/// text containing a non-empty sentence is non-empty — so the body was true by construction on every
/// path, including the `None` path that skips it entirely. It read like coverage and asserted
/// nothing. The only honest way to pin "our layer does not swallow the error sentence" is to
/// construct a run whose text is KNOWN to carry it, which needs no Windows at all — so the claim
/// moved HERE, where the input is built independently of the output scan and the assertion can
/// therefore be false.
///
/// UNLIKE THE `start /B` TEST, THIS RUNS ON EVERY PLATFORM — Linux and macOS included, not only on
/// CI's `windows-latest` leg. It is UNGATED, mirroring its precedent
/// [`a_run_whose_text_carries_the_cmd_redirect_error_gets_the_diagnostic_as_its_first_line`], which
/// is ALSO ungated (its doc says so on purpose: the wiring in `exec_bash` it pins is not `cfg`-gated,
/// so it must not rest on a CI-only test). That precedent's technique is what this copies: have the
/// CHILD EMIT the exact sentence from a plain print command, so the test runs on a host that has no
/// `cmd.exe` and needs no `cmd` behaviour beyond "a shell prints what it is told to print".
///
/// ONE DELIBERATE DIFFERENCE FROM THE PRECEDENT, AND IT IS THE PORTABILITY HALF: the precedent
/// emits with `printf`, which is a `sh` builtin and NOT a `cmd` builtin, so its Windows leg rides on
/// whatever `printf.exe` happens to be on that runner's PATH. This test emits with `echo`, which is a
/// builtin on BOTH shells (`sh -c` on Unix, `cmd /d /s /c` on Windows), and both sentences are
/// metacharacter-free on both — no `&`, `|`, `<`, `>`, `(`, `)`, `^`, `%`, `$` or backtick — and
/// carry NO QUOTES AT ALL, so nothing here depends on how `childproc::append_cmd_c` treats an
/// embedded `"`. Both shells therefore PRINT BOTH SENTENCES VERBATIM and the test needs no gate.
/// (The precedent stays as it is; fixing its emission is not this claim.)
///
/// THE TWO PLATFORMS DO NOT PRINT THE SAME BYTES, AND THE ASSERTION KNOWS IT. `cmd`'s `echo`
/// terminates its argument at a metacharacter but does NOT discard the whitespace preceding it, so
/// `echo X && echo Y` prints `X ` — with ONE TRAILING SPACE — where `sh` discards it. Line 1
/// therefore carries a trailing space on Windows and nothing else differs: our layer is byte-exact
/// (`childproc::cmd_c_tail` is `format!("/d /s /c \"{command}\"")`, handed to `raw_arg`), so that
/// space is `cmd`'s own argument parsing and not something we added. Trailing whitespace produced by
/// the SHELL is not what this test pins — the two `contains` checks below pin the sentence bytes — so
/// the exact-body check `trim_end`s each line for precisely that. `trim_end` forgives the shell's
/// whitespace and NOTHING ELSE: the `lines().len() == 2` check still rules out a prepend and an
/// append, so slack on the right edge cannot hide content on either side of the body.
///
/// DO NOT "PORTABILITY-FIX" THE `&&` INTO A BARE `&`. That is the usual `cmd` idiom for dodging
/// exactly this trailing-space quirk, and it is WRONG here: in `sh`, `&` is the BACKGROUND operator,
/// so `echo A& echo B` backgrounds the first `echo` and the two writes to the shared pipe RACE — the
/// order of the two lines becomes non-deterministic, which would make the ordered assertion below
/// flaky on Unix to buy one space on Windows. `&&` stays.
///
/// WHY EVERY ASSERTION BELOW IS NECESSARY, and what each one rules out:
/// - exit 0 + `!is_error` reproduces #79's EXACT shape — an error sentence riding on a clean
///   success. Without this pairing the text pin would be testing the wrong run: a NON-zero run's text
///   arriving intact says nothing about the case where the code is the only thing that lied.
/// - the two sentences arrive as the WHOLE body, in order: line 1 is the first sentence (after
///   `trim_end`, see above) and there are exactly two lines. Line 1 is what rules out the one thing
///   that inserts in front of captured text — `cmd_input_redirection_diagnostic`'s prepend, which keys
///   on `ERROR: Input redirection is not supported`, a sentence this run does not carry, and which
///   would have displaced line 1 AND made the line count three while leaving both `contains` checks
///   green. "Exactly two lines" is what rules out anything APPENDED — an exit-report note in
///   particular — and together the two pins say the captured bytes survived the 100 KB cap and the
///   lossy UTF-8 conversion as the exact body, not merely present somewhere in it.
///
/// NOT A PROMOTION ASSERTION, and deliberately so. The test DOES assert `!r.is_error` (assertion 1
/// below), but that is a fact about the fixture — `echo` exits 0, so a correct tool reports success —
/// and it is exactly what makes the run #79's shape. What the test does NOT do is ask `exec_bash` to
/// PROMOTE an exit-0-with-error-text run into an error: that would be a `bash_exit_report` change (its
/// `(Some(n), None)` arm — "a normal non-zero exit needs no note — the code IS the report") and is out
/// of scope. This test makes NO claim about what the tool should CONCLUDE from the sentence, only that
/// the sentence ARRIVES.
#[tokio::test]
async fn the_bash_tool_passes_issue_79_error_sentences_through_verbatim() {
    let cwd = temp_cwd("bash-issue-79-verbatim");
    // Both sentences #79 quotes, verbatim INCLUDING the trailing period on the second one, from the
    // shell's own `echo` — a builtin on `sh` AND on `cmd`, so this needs no `cmd.exe`, no
    // `printf.exe`, no third-party tool and no `cfg` gate. The `.` is inert in both shells' `echo`
    // arguments (it is not a metacharacter in either), so carrying it costs nothing and makes the
    // claim below true: the constant is now byte-equal to the sentence as the issue writes it, and the
    // period survives both `echo`s to be asserted by the `contains` check and by line 2. No quotes
    // anywhere in the command: the sentences carry none, and an embedded `"` would move the claim from
    // "does our layer swallow the text" to "how does `append_cmd_c` escape a quote", which
    // [`bash_passes_inline_double_quotes_through_to_cmd_verbatim`] already owns.
    const LOCKED: &str =
        "The process cannot access the file because it is being used by another process";
    const DENIED: &str = "Access is denied.";
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": format!("echo {LOCKED} && echo {DENIED}") }),
    )
    .await;
    let text = text_of(&r);

    // 1. THE #79 SHAPE: an error sentence riding on a CLEAN success. This is the half that makes the
    //    text pin test the RIGHT run — on a non-zero run, "the text survived" would say nothing about
    //    the case where the code was the only thing that lied.
    assert!(
        !r.is_error,
        "the run must be a clean success for this to be #79's shape — the whole point is that the \
         TEXT is the only evidence of a problem: TEXT={text:?} DETAILS={:?}",
        r.details
    );
    assert_eq!(
        r.details.as_ref().unwrap()["exitCode"], 0,
        "`echo` exits 0 on both shells, so the exit code is the lying half of the pair: DETAILS={:?}",
        r.details
    );

    // 2. THE CLAIM ITSELF, once per sentence, asserted against constants written here and NOT read
    //    out of the output (that independence is what makes this falsifiable, and what the deleted
    //    block could not have).
    assert!(
        text.contains(LOCKED),
        "issue #79's file-lock sentence must reach the model VERBATIM — swallowed by truncation, by \
         a replacement, or by any rewrite of the captured text, it is the one thing that contradicts \
         the exit 0 above: LOOKING_FOR={LOCKED:?} TEXT={text:?} DETAILS={:?}",
        r.details
    );
    assert!(
        text.contains(DENIED),
        "issue #79's `Access is denied.` sentence must reach the model VERBATIM for the same reason: \
         LOOKING_FOR={DENIED:?} TEXT={text:?} DETAILS={:?}",
        r.details
    );

    // 3. THE BODY IS EXACTLY THOSE TWO SENTENCES, IN THAT ORDER, AND NOTHING ELSE BUT THE SHELL'S OWN
    //    TRAILING WHITESPACE. Line 1 rules out the one thing in `exec_bash` that inserts BEFORE
    //    captured text (the diagnostic prepend, which keys on a sentence this run does not carry — and
    //    which would have displaced line 1 AND turned the count into three while leaving both
    //    `contains` checks above green); the line COUNT rules out anything APPENDED, an exit-report note
    //    included, so assertion 1's "clean success" claim is checked on the text axis too and not only
    //    on `is_error`.
    //
    //    THE `trim_end` IS FOR THE SHELL AND FOR NOTHING ELSE. `cmd`'s `echo` keeps the space that
    //    precedes `&&` in its argument (`sh` discards it), so line 1 arrives as LOCKED + " " on
    //    Windows. Trimming the RIGHT edge cannot cost either property this assertion exists for: a
    //    PREPEND is still caught (line 1 would be the diagnostic, and the count would be three) and an
    //    APPEND is still caught (the count would be three). Both were observed RED by planting them.
    //    See the doc comment for why `&&` is not softened to `&` to dodge the space instead.
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "exactly two lines — a prepend (the diagnostic) or an append (an exit-report note) changes the \
         count: TEXT={text:?} DETAILS={:?}",
        r.details
    );
    assert_eq!(
        lines[0].trim_end(),
        LOCKED,
        "line 1 IS issue #79's file-lock sentence, with nothing inserted in front of it (this is the \
         assertion the deleted vacuous block could not be, because the expected value here was written \
         before the run and not read out of it): TEXT={text:?} DETAILS={:?}",
        r.details
    );
    assert_eq!(
        lines[1].trim_end(),
        DENIED,
        "line 2 IS issue #79's `Access is denied.` sentence, in order and with nothing appended after \
         it: TEXT={text:?} DETAILS={:?}",
        r.details
    );
}

// ── the exit-status truth table (pure — no child, no timing) ─────────
//
// A `sh -c` child reports EOF on its pipes the instant its LAST writing
// fd closes, which happens BEFORE the process has exited. The old code
// therefore SIGKILLed (or raced) a child that had already finished its
// work, and reported the resulting signal death as the fabricated
// `exitCode: -1` — a model watching a successful `git commit` see a
// failure and RETRY a non-idempotent command. These rows pin the
// decision itself, with no timing involved.

/// A wait status from a raw `waitpid` value (`exit N` → `N << 8`; a
/// signal death → the signal number, whose low bits are the cause).
///
/// UNIX-ONLY, deliberately: it is the raw-`waitpid` form, and a raw `waitpid`
/// value is the only thing on any platform that can encode a SIGNAL DEATH (the
/// shape the two signal rows below need). A child that merely EXITED with a
/// code is built by [`exit_status_of`] instead, which exists on both platforms.
#[cfg(unix)]
fn wait_status(raw: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    std::process::ExitStatus::from_raw(raw)
}

/// A `wait` status for a child that EXITED with `code` (never a signal death).
///
/// Unix encodes an exit as `code << 8` in the `waitpid` value; Windows stores
/// the exit code itself. `exit_signal` returns `None` on Windows, so a signal
/// row has no Windows analogue and stays unix-only — which is why the four
/// portable rows below use THIS helper instead of [`wait_status`].
#[cfg(unix)]
fn exit_status_of(code: i32) -> std::process::ExitStatus {
    wait_status(code << 8)
}

#[cfg(not(unix))]
fn exit_status_of(code: i32) -> std::process::ExitStatus {
    use std::os::windows::process::ExitStatusExt as _;
    std::process::ExitStatus::from_raw(code as u32)
}

/// A status the report can give NO code to. On unix that is a real signal
/// death (`waitpid` value 9 → SIGKILL). Windows has no signal deaths and
/// `exit_signal` is `None` for every status there, so `bash_exit_report`'s
/// `(_, Some(sig))` arm is UNREACHABLE on Windows; the nearest thing the
/// platform offers is the code a crash returns, which the report still refuses
/// to call a success. The asymmetry is the point — do not "unify" it.
#[cfg(unix)]
fn a_death_with_no_exit_code() -> std::process::ExitStatus {
    wait_status(9)
}

#[cfg(not(unix))]
fn a_death_with_no_exit_code() -> std::process::ExitStatus {
    // STATUS_ACCESS_VIOLATION, built directly rather than through
    // `exit_status_of` (which takes an `i32` exit code, and this value is only
    // meaningful as a `u32` NTSTATUS).
    use std::os::windows::process::ExitStatusExt as _;
    std::process::ExitStatus::from_raw(0xC000_0005)
}

/// THE BUG: a command that exited 0 and whose exit status was observed
/// is a SUCCESS. Not an error, code 0, no note.
#[test]
fn bash_exit_report_success_is_not_an_error() {
    let (code, is_error, note) = bash_exit_report(false, false, Some(exit_status_of(0)), false);
    assert!(
        !is_error,
        "a successful run must NOT be reported as a failure"
    );
    assert_eq!(code, Some(0));
    assert_eq!(note, None, "a success has nothing to warn about");
}

/// A non-zero exit is an error carrying the REAL code (the shape
/// `bash_nonzero_exit_is_error` asserts on the wire).
#[test]
fn bash_exit_report_nonzero_exit_is_an_error_with_its_real_code() {
    let (code, is_error, note) = bash_exit_report(false, false, Some(exit_status_of(3)), false);
    assert!(is_error);
    assert_eq!(code, Some(3), "the real code, never a fabricated one");
    assert_eq!(note, None);
}

/// A signal death is an error, but is NOT dressed up as exit code -1 —
/// it says which signal killed it (a real signal death must stay
/// distinguishable from a non-zero exit).
///
/// ⚠ STAYS unix-gated, and that is a REAL PLATFORM LIMIT, not a dodge:
/// `exit_signal` is `None` for EVERY status off Unix **by design**
/// (`exec/bash.rs`'s `#[cfg(not(unix))]` arm), so on Windows there is no signal
/// to name and no status that IS a signal death. Un-gating this row would force
/// it either into a tautology or into a Linux RED. The code-less half of the
/// same idea does port, and does — see [`a_death_with_no_exit_code`] and
/// `bash_exit_report_non_error_rows_are_exactly_two`.
#[cfg(unix)]
#[test]
fn bash_exit_report_signal_death_names_the_signal() {
    let (code, is_error, note) = bash_exit_report(false, false, Some(wait_status(9)), false);
    assert!(is_error, "killed by a signal is a failure");
    assert_eq!(code, None, "a signal death has no exit code");
    let note = note.expect("the note names the signal");
    assert!(note.contains("signal 9"), "got {note:?}");
}

/// A cancelled run is an error whatever the status says (the user asked
/// for it to stop, so it did not complete).
///
/// ⚠ STAYS unix-gated for the same reason as the signal row above: its
/// status list contains a real signal death (`wait_status(9)`) and asserts
/// `code.is_none() || code == Some(0)` — a signal death has NO code, and there
/// is no Windows status that is a signal death (`exit_signal` is `None` off
/// Unix by design, `exec/bash.rs`), so that row cannot port. The two statuses
/// that DO port (no status, and exit 0) are covered on both platforms by
/// `bash_exit_report_unknown_status_says_so_and_claims_no_code` and
/// `bash_exit_report_non_error_rows_are_exactly_two`.
#[cfg(unix)]
#[test]
fn bash_exit_report_cancelled_is_always_an_error() {
    // Even a status of 0: the run was interrupted, it did not finish.
    let statuses: [Option<std::process::ExitStatus>; 3] =
        [None, Some(wait_status(0 << 8)), Some(wait_status(9))];
    for status in statuses {
        let (code, is_error, note) = bash_exit_report(true, false, status, false);
        assert!(is_error, "a cancelled run is an error (status {status:?})");
        assert!(
            note.is_some(),
            "the model is told it was cancelled (status {status:?})"
        );
        assert!(code.is_none() || code == Some(0), "got {code:?}");
    }
}

/// A deadline run is an error, and says it TIMED OUT (the old code
/// inferred "timeout" from a fabricated -1, which is indistinguishable
/// from any other unknown outcome).
#[test]
fn bash_exit_report_timeout_is_an_error_that_says_timed_out() {
    let (code, is_error, note) = bash_exit_report(false, true, None, false);
    assert!(is_error);
    assert_eq!(code, None, "no status was seen, so no code is claimed");
    let note = note.expect("the note says the deadline was hit");
    assert!(note.contains("timed out"), "got {note:?}");
    // A timeout that DID see the status still reports it, when the pipes
    // were not what held the run open (a pipe-held exit 0 is R1, not this).
    let (code, is_error, _) = bash_exit_report(false, true, Some(exit_status_of(0)), false);
    assert!(
        is_error,
        "a timed-out run is an error even with a clean status, when the pipes \
         were not what held the run open"
    );
    assert_eq!(code, Some(0));
}

// ── the fourth fact: were the output pipes still HELD at the deadline? ─
//
// Issue #94: a command whose FOREGROUND work finished and which was OBSERVED
// exiting 0, but whose pipes were still held open by a background process, hit
// its deadline and was reported as a TIMEOUT — so the model re-ran a
// non-idempotent command that had already succeeded. The report must be able
// to tell "we ended a hung command" from "it finished; a background process
// was still holding the pipe when the deadline fired". These rows are the
// whole sub-table, decided with no child and no clock.

/// The do-not-re-run phrase, chosen ONCE and asserted by every test that
/// asserts it (`bash_exit_report_pipe_held_exit_zero_is_a_success_that_forbids_rerun`
/// and both end-to-end fixtures). The note is not pinned byte-for-byte — the
/// file's convention is `contains` on a distinctive phrase — but this phrase is
/// the whole point of the report, so it is the one that gets pinned.
const DO_NOT_RERUN: &str = "DO NOT RE-RUN";

/// THE BUG (#94): pipes held at the deadline + an exit 0 WE SAW → a SUCCESS.
/// Not an error, code 0, and a note that forbids re-running.
#[test]
fn bash_exit_report_pipe_held_exit_zero_is_a_success_that_forbids_rerun() {
    let (code, is_error, note) = bash_exit_report(false, true, Some(exit_status_of(0)), true);
    assert!(
        !is_error,
        "#94: a command observed exiting 0 while a pipe was held must NOT be \
         reported as a failure — that report is what sends models re-running \
         non-idempotent commands. TEXT={note:?}"
    );
    assert_eq!(code, Some(0), "the real exit code, still 0");
    let note = note.expect("a pipe-held completion explains itself to the model");
    assert!(note.contains("0"), "the note names the exit code: {note:?}");
    assert!(
        note.contains(DO_NOT_RERUN),
        "the note must forbid re-running: {note:?}"
    );
    assert!(
        !note.contains("timed out"),
        "a completed command is not reported as a timeout: {note:?}"
    );
    assert!(
        !note.contains("in full"),
        "the note must NEVER claim the output was captured in full — the \
         drain after the kill is bounded by DRAIN_GRACE: {note:?}"
    );
}

/// Held pipes rescue an exit 0 and NOTHING ELSE: a non-zero exit inside a
/// deadline run is still an error carrying its real code, and its note must
/// not read as a hang (or the model retries a command that genuinely failed).
#[test]
fn bash_exit_report_pipe_held_nonzero_exit_reports_its_real_code_and_never_says_timed_out() {
    let (code, is_error, note) = bash_exit_report(false, true, Some(exit_status_of(3)), true);
    assert!(is_error, "a real failure stays a failure");
    assert_eq!(code, Some(3), "the real code, never a fabricated one");
    let note = note.expect("the note carries the real exit code");
    assert!(note.contains("3"), "the note names the code: {note:?}");
    assert!(
        !note.contains("timed out"),
        "a genuine failure must not read as a hang (that is #94 wearing the \
         other mask — the model retries it): {note:?}"
    );
}

/// R3: pipes held + NO observed status → unchanged. Held pipes never rescue
/// an exit we never saw — the run is still the unknown the UNKNOWN text
/// describes.
#[test]
fn bash_exit_report_pipe_held_unknown_status_still_says_timed_out_and_unknown() {
    let (code, is_error, note) = bash_exit_report(false, true, None, true);
    assert!(is_error);
    assert_eq!(code, None, "no status was seen, so no code is claimed");
    let note = note.expect("the note says the deadline was hit");
    assert!(note.contains("timed out"), "got {note:?}");
    assert!(note.contains("unknown"), "got {note:?}");
}

/// R4: exit 0 inside a deadline run with the pipes NOT held → unchanged, still
/// an error. This is the row that keeps the success row honest: held pipes are
/// what distinguish the two, and without this assertion the success row would
/// swallow every deadline run that exited 0.
#[test]
fn bash_exit_report_pipe_not_held_exit_zero_timeout_stays_an_error() {
    let (code, is_error, note) = bash_exit_report(false, true, Some(exit_status_of(0)), false);
    assert!(
        is_error,
        "a deadline run whose pipes were NOT held is still a timeout"
    );
    assert_eq!(code, Some(0), "the real exit code is still reported");
    let note = note.expect("the note still tells the truth about the deadline");
    assert!(note.contains("timed out"), "got {note:?}");
}

/// R5: a SIGNAL death inside a deadline run is still an error, names its
/// signal, claims NO exit code, and still says "timed out". This is where
/// every genuine hang lands — the loop breaks with no status, the group kill
/// SIGKILLs the child, and the bounded post-loop reap then observes that
/// signal death — so the deadline sentence must survive. Checking the signal
/// BEFORE the code is what stops a status whose `code()` is `None` from being
/// given a fabricated number, and it is what makes the success row
/// unreachable for a command we had to kill.
///
/// ⚠ STAYS unix-gated for the same reason as
/// `bash_exit_report_signal_death_names_the_signal`: `exit_signal` is `None`
/// for EVERY status off Unix by design, so there is no signal to name there.
#[cfg(unix)]
#[test]
fn bash_exit_report_pipe_held_signal_death_is_still_a_signal_death() {
    let (code, is_error, note) = bash_exit_report(false, true, Some(wait_status(9)), true);
    assert!(is_error, "a run we SIGKILLed is not a success");
    assert_eq!(
        code, None,
        "a signal death has no exit code — never invent one"
    );
    let note = note.expect("the note names the signal");
    assert!(note.contains("signal 9"), "got {note:?}");
    assert!(
        note.contains("timed out"),
        "a genuine hang must still be called one: {note:?}"
    );
}

/// The genuinely-unknowable case: NO status was captured. It stays an
/// error, claims NO code, and tells the model to check the side effects
/// before re-running (re-running a non-idempotent command blindly is
/// the dangerous outcome this whole path exists to avoid).
#[test]
fn bash_exit_report_unknown_status_says_so_and_claims_no_code() {
    let (code, is_error, note) = bash_exit_report(false, false, None, false);
    assert!(is_error, "an unknown outcome is NOT silently a success");
    assert_eq!(code, None, "no status known means no code invented");
    let note = note.expect("the note says the status is unknown");
    assert!(note.contains("unknown"), "got {note:?}");
    assert!(
        note.contains("re-run") || note.contains("rerun"),
        "the note must warn about re-running: {note:?}"
    );
}

/// THE NON-ERROR ROWS OF THE TABLE ARE EXACTLY TWO: (not cancelled, not timed
/// out, exited 0) and the pipe-held completion — a deadline run whose exit
/// status was OBSERVED as 0 while a pipe was still held (issue #94).
/// Everything else is an error. Asserted as a table so a future edit cannot
/// widen the success case.
///
/// WHY THE TABLE GREW: the report gained a fourth input (`pipes_held`), which
/// adds a SUB-TABLE inside the `timed_out` arm — so a one-row census could no
/// longer describe the truth table. Rows 6–8 are that sub-table (held + exit 0,
/// held + non-zero, held + a death the report can give NO code to: on unix a
/// SIGKILL death, which is R5 and names its signal; on Windows a crash code,
/// which lands on R2 — the platform asymmetry [`a_death_with_no_exit_code`]
/// insists on, `is_error: true` either way); row 9 shows the `cancelled` arm is
/// checked first, so held pipes never rescue a cancelled run.
///
/// `ok == vec![0, 6]` is what gives the table its teeth: widening success
/// anywhere — dropping `signal.is_none()` from the success guard, ignoring
/// `pipes_held` (which turns row 5 into a SECOND success), or trusting a `None`
/// status — adds an index and reddens this assert; narrowing the new row
/// removes `6` and reddens it too. Rows 5 and 6 are the sharpest pair: they
/// differ ONLY in `pipes_held`, and exactly one of them is a success.
///
/// The falsifier D2 records, kept here so the row stays falsifiable: if a run
/// is ever observed where a pipe was held AND the killed background work
/// mattered, row 6 was wrong.
#[test]
fn bash_exit_report_non_error_rows_are_exactly_two() {
    let rows = [
        (false, false, Some(exit_status_of(0)), false),
        (false, false, Some(exit_status_of(1)), false),
        (false, false, Some(a_death_with_no_exit_code()), false),
        (false, false, None, false),
        (true, false, Some(exit_status_of(0)), false),
        // exit 0 inside a deadline run, pipes NOT held — a real timeout.
        (false, true, Some(exit_status_of(0)), false),
        // …the same run with a pipe still held: the new success row (#94).
        (false, true, Some(exit_status_of(0)), true),
        (false, true, Some(exit_status_of(3)), true),
        (false, true, Some(a_death_with_no_exit_code()), true),
        (true, false, Some(exit_status_of(0)), true),
    ];
    let ok: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, (c, t, s, p))| !bash_exit_report(*c, *t, *s, *p).1)
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        ok,
        vec![0, 6],
        "the only successes are exit 0 untouched and exit 0 with the pipes held"
    );
}

// ── the same bug, end-to-end ─────────────────────────────────────────

/// A `sh -c` child closes its stdout/stderr as part of exiting, and the
/// read loop breaks on that EOF while the child is still alive for a
/// few more microseconds. The old code SIGKILLed the child at that
/// moment — so a command that had ALREADY succeeded was reported as
/// `exitCode: -1` under CPU contention. This test closes the pipes
/// EXPLICITLY (`>&-`) and then stays alive briefly, which is that same
/// window made deterministic: the output is complete, the pipes are
/// closed, and the process has NOT exited yet.
/// Same `exec >&- 2>&-` fixture (the POSIX fd-close whose EOF-before-exit
/// window the test makes deterministic); `cmd` parses it as a malformed
/// redirect and exits 1, so the run never reached the exit-status path
/// under test. The DECISION — a child already exited 0 is a success, no
/// fabricated -1 — is platform-neutral and stays covered everywhere by
/// `bash_exit_report_success_is_not_an_error`.
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_success_after_closing_its_pipes_is_not_an_error() {
    let cwd = temp_cwd("bash-closed-pipes");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({
            "command": "echo done; exec >&- 2>&-; sleep 0.2; exit 0",
            "timeout_ms": 10_000,
        }),
    )
    .await;
    assert!(
        !r.is_error,
        "a command that succeeded must not be reported as a failure: TEXT={:?} DETAILS={:?}",
        text_of(&r),
        r.details
    );
    assert_eq!(
        r.details.as_ref().unwrap()["exitCode"],
        0,
        "the real exit code, got {:?}",
        r.details
    );
    assert!(
        text_of(&r).contains("done"),
        "the output captured before the pipes closed is returned: {:?}",
        text_of(&r)
    );
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
}

/// The other half of "report the REAL exit status": a child that closed
/// its pipes early and then exited NON-ZERO must come back as an error
/// carrying its real code — never "exit status unknown", never a code
/// guessed after a grace period. Two shapes, same expectation: the
/// short one pins the ordinary case (the bounded post-loop reap usually
/// catches a 0.2 s sleep, so it cannot pin the hole alone), and
/// the long one — a sleep that outlasts [`crate::agent::tools::exec::DRAIN_GRACE`] — pins the
/// read loop's OWN EOF break: it must wait for the exit status itself
/// instead of leaning on the post-loop reap, which expires under CPU
/// contention and reports a run that really exited 7 as "unknown".
/// Both shapes here are `exec >&- 2>&-; sleep 0.2`-style POSIX (the
/// fd-close, and a fractional `sleep` `cmd` cannot run either) — under
/// `cmd /C` the child exited 1 on its parse error, which is exactly the
/// fabricated-code confusion this test exists to forbid, reported as a
/// real 7-vs-1 mismatch. The real-code rule itself is covered on every
/// platform by
/// `bash_exit_report_nonzero_exit_is_an_error_with_its_real_code`.
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_nonzero_exit_after_closing_its_pipes_reports_its_real_code() {
    let cwd = temp_cwd("bash-closed-pipes-nz");
    for cmd in [
        "echo done; exec >&- 2>&-; sleep 0.2; exit 7",
        "echo done; exec >&- 2>&-; sleep 3; exit 7",
    ] {
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({ "command": cmd, "timeout_ms": 10_000 }),
        )
        .await;
        assert!(
            !r.details.as_ref().unwrap()["cancelled"].as_bool().unwrap(),
            "{cmd}: not a cancellation"
        );
        assert!(
            r.is_error,
            "{cmd}: a non-zero exit is an error: TEXT={:?} DETAILS={:?}",
            text_of(&r),
            r.details
        );
        assert_eq!(
            r.details.as_ref().unwrap()["exitCode"],
            7,
            "{cmd}: the real exit code, got {:?}",
            r.details
        );
        assert!(
            text_of(&r).contains("done"),
            "{cmd}: the output captured before the pipes closed is returned: {:?}",
            text_of(&r)
        );
    }
}

/// A6-a — THE BUG (#94), end-to-end: a command whose foreground work
/// finished and which was OBSERVED exiting 0, while a background process
/// still held the output pipe open, must come back as a SUCCESS with a
/// do-not-re-run note — not as a timeout.
///
/// The shape is the DETERMINISTIC one and it is the truth of the two pipe-held
/// fixtures (`bash_issue_94_repro_is_reported_as_success_not_a_timeout` is the
/// issue's own repro, kept as the illustration):
/// - `( sleep 25; exec >&- 2>&- ) &` leaves exactly ONE named holder of the
///   write ends, and gives that holder an explicit fd close — the same
///   `exec >&- 2>&-` idiom `bash_success_after_closing_its_pipes_is_not_an_error`
///   uses to make pipe-close deterministic. `( sleep 25 & )` instead backgrounds
///   a subshell that exits at once and leaves `sleep` orphaned and re-parented,
///   so the pipes would be held at the deadline by a process the fixture never
///   named.
/// - NO foreground `wait`, and that is load-bearing: `wait` keeps the parent
///   alive until the holder finishes (25 s), so the deadline would fire with
///   `status.is_none()`, the kill gate would SIGKILL the group including the
///   parent, and the bounded post-loop reap would hand the report a SIGKILL
///   death — which is the signal row, not this one. Without `wait` the parent
///   `sh` falls off the end of the script immediately, so the `child.wait()`
///   select arm retires the exit status well before the deadline arm breaks the
///   loop, and the run arrives at the report with its status already observed.
/// - The holder's 25 s NEVER enters the wall clock: the deadline ends the run
///   and the process-group kill reaps the holder, so this fixture costs
///   `timeout_ms` plus the bounded drain.
///
/// Deliberately NOT asserted here: the grandchild's death.
/// `bash_backgrounded_grandchild_killed_with_group` owns that claim and pays a
/// real 3 s wait past the marker deadline to prove it; duplicating it would
/// triple this test's cost for a fact already pinned. This fixture asserts the
/// REPORT and nothing else.
/// Both fixtures here are POSIX through and through (`( … ) &` backgrounding
/// and the `>&-` fd close), so `cmd` cannot run them; the DECISION they gate is
/// platform-neutral and stays covered everywhere by
/// `bash_exit_report_pipe_held_exit_zero_is_a_success_that_forbids_rerun`.
/// The #79 quoting fix does NOT make this fixture runnable on `cmd`: the gate is
/// POSIX SYNTAX, not quoting.
#[cfg(unix)]
#[tokio::test]
async fn bash_pipe_held_completion_is_reported_as_success_not_a_timeout() {
    let cwd = temp_cwd("bash-pipe-held");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({
            "command": "echo PIPE-HELD-1; ( sleep 25; exec >&- 2>&- ) & echo PIPE-HELD-2",
            "timeout_ms": 800,
        }),
    )
    .await;
    assert!(
        !r.is_error,
        "#94: a command observed exiting 0 while a background process held the \
         pipe must be reported as COMPLETED, not as a timeout: TEXT={:?} DETAILS={:?}",
        text_of(&r),
        r.details
    );
    assert_eq!(
        r.details.as_ref().unwrap()["exitCode"],
        0,
        "the real exit code, got {:?}",
        r.details
    );
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
    let text = text_of(&r);
    assert!(
        text.contains("PIPE-HELD-1") && text.contains("PIPE-HELD-2"),
        "the output captured before the deadline is returned: {text:?}"
    );
    assert!(
        !text.contains("timed out"),
        "the text the model reads must not call this a timeout: {text:?}"
    );
    assert!(
        text.contains(DO_NOT_RERUN),
        "the text must forbid re-running a command that already succeeded: {text:?}"
    );
}

/// A6-b — the SAME bug, reported by the issue itself: #94's repro block run
/// verbatim. Kept for faithfulness to the report (the repro IS the spec) and
/// because it costs the same `timeout_ms` + bounded drain as A6-a. Where the
/// two disagree, `bash_pipe_held_completion_is_reported_as_success_not_a_timeout`
/// is the truth and THIS one is the illustration — its `( sleep 25 & )` leaves
/// an orphaned subshell holding the write ends, so it is the shape whose
/// holder the fixture never named. Same assertion set, same gate (POSIX
/// syntax), same reason for the `#[cfg(unix)]`.
#[cfg(unix)]
#[tokio::test]
async fn bash_issue_94_repro_is_reported_as_success_not_a_timeout() {
    let cwd = temp_cwd("bash-pipe-held-repro");
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({
            "command": "echo PIPE-HELD-1; ( sleep 25 & ); echo PIPE-HELD-2",
            "timeout_ms": 800,
        }),
    )
    .await;
    assert!(
        !r.is_error,
        "#94's repro, verbatim, must be reported as COMPLETED, not as a \
         timeout: TEXT={:?} DETAILS={:?}",
        text_of(&r),
        r.details
    );
    assert_eq!(
        r.details.as_ref().unwrap()["exitCode"],
        0,
        "the real exit code, got {:?}",
        r.details
    );
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], false);
    let text = text_of(&r);
    assert!(
        text.contains("PIPE-HELD-1") && text.contains("PIPE-HELD-2"),
        "the output captured before the deadline is returned: {text:?}"
    );
    assert!(
        !text.contains("timed out"),
        "the text the model reads must not call this a timeout: {text:?}"
    );
    assert!(
        text.contains(DO_NOT_RERUN),
        "the text must forbid re-running a command that already succeeded: {text:?}"
    );
}

/// The flake's shape, reproduced on purpose: the executor pinned to ONE
/// cpu (`sched_setaffinity`, what `taskset -c 0` does) while every cpu on
/// the box is saturated by busy-loop spinners, running a trivially
/// successful command over and over. Measured against the pre-fix code
/// on this box — 30 runs of this test — every command came back
/// `isError=true, exitCode: -1` (360 of 360) with CORRECT output and
/// `cancelled: false`: a model watching a successful `git commit` fail.
/// The assertions are machine-independent (only "a successful run
/// reports success"), so a faster or slower box cannot redden it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn bash_success_survives_a_saturated_machine() {
    let cwd = temp_cwd("bash-load");
    let _load = CpuLoad::spin();
    let c = ctx(&cwd);
    let bad = std::thread::scope(|s| {
        std::thread::Builder::new()
            .spawn_scoped(s, move || {
                pin_to_cpu0();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let mut bad = Vec::new();
                    for i in 0..12 {
                        let cmd = format!("echo LOAD-{i} > out{i}.txt && cat out{i}.txt");
                        let r = execute_tool(&c, "bash", &json!({ "command": cmd })).await;
                        let code = r.details.as_ref().unwrap()["exitCode"].clone();
                        if r.is_error || code != serde_json::json!(0) {
                            bad.push(format!("{cmd} → isError={} {code}", r.is_error));
                        }
                    }
                    bad
                })
            })
            .unwrap()
            .join()
            .unwrap()
    });
    assert!(
        bad.is_empty(),
        "successful runs reported under load: {bad:?}"
    );
}

/// The native `bash` deadline without a `timeout_ms` is 300 s (matching
/// the interactive channel's `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`) — NOT unbounded.
#[test]
fn bash_default_deadline_is_300s() {
    assert_eq!(DEFAULT_BASH_TIMEOUT, Duration::from_secs(300));
}

/// A model-supplied `timeout_ms` is CLAMPED to the 15 min max (the
/// model controls the timeout — an unclamped `timeout_ms: 10^9` would
/// run ~11 days); a `timeout_ms` under the cap is unchanged.
#[test]
fn bash_timeout_ms_is_clamped_to_the_max() {
    assert_eq!(clamp_timeout_ms(10u64.pow(9)), MAX_BASH_TIMEOUT);
    assert_eq!(MAX_BASH_TIMEOUT, Duration::from_secs(15 * 60));
    assert_eq!(clamp_timeout_ms(1000), Duration::from_millis(1000));
    assert_eq!(clamp_timeout_ms(15 * 60 * 1000), MAX_BASH_TIMEOUT);
}

#[tokio::test]
async fn bash_output_capped_at_max() {
    let cwd = temp_cwd("bash-cap");
    // 300 KB of `x` — well over the 100 KB cap.
    let r = execute_tool(
        &ctx(&cwd),
        "bash",
        &json!({ "command": "head -c 300000 /dev/zero | tr '\\0' 'x'" }),
    )
    .await;
    assert!(
        !r.is_error,
        "a truncated-but-successful run is not an error"
    );
    assert_eq!(r.details.as_ref().unwrap()["truncated"], true);
    assert!(
        text_of(&r).len() <= MAX_OUTPUT_BYTES,
        "output is capped at {MAX_OUTPUT_BYTES} bytes (got {})",
        text_of(&r).len()
    );
}

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

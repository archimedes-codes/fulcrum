//! Who owns a child process's Windows console, and its stdin.
//!
//! The shipped Windows build is a GUI-subsystem process
//! (`src/main.rs:2` — `#![cfg_attr(not(debug_assertions), windows_subsystem =
//! "windows")]`), so it has no console for a console-subsystem child
//! (`cmd.exe`, `fd`, `rg`, an `npx.cmd` shim) to inherit; Windows then
//! allocates a console per child, and creating a console creates a window —
//! one blank `cmd.exe` window per shell command. `CREATE_NO_WINDOW` is the
//! documented lever that suppresses the window; mechanism, citations and the
//! triage are in `docs/research/windows-child-console-window.md`, the choice
//! of a constructor as the owner is ADR 0060.
//!
//! Two invariants this module exists to hold:
//!
//! * **One owner for the creation flags.** `creation_flags` REPLACES rather
//!   than ORs (rust-lang/rust#96794 — there is no getter to read the previous
//!   value back), so a second caller anywhere in the crate silently un-hides
//!   the child and re-opens a window per command. That is why the flag lives in
//!   the constructor rather than at each call site, and it is why any future
//!   `CREATE_SUSPENDED` or `CREATE_NEW_PROCESS_GROUP` belongs **in this module**
//!   and nowhere else: a call site has no `Command` to set a flag on except one
//!   that came from here, so composing the flags has exactly one legal place.
//!   [`command`] / [`command_std`] are therefore the only two constructors, and
//!   [`hide`] / [`hide_tokio`] stay private.
//! * **`closed_stdin` is opt-in, never part of the constructor.** Nulling stdin
//!   is right for a **Tool child** (the Agent asked for `sh -c …`; a command
//!   that reads stdin must reach EOF rather than block on an inherited pipe
//!   until the 300 s bash timeout) and wrong for every **Runtime child** — the
//!   Worker's JSONL stdin, an MCP server's JSON-RPC stdin and `sudo_exec`'s
//!   password stdin are pipes we actively write. The Tool/Runtime split is the
//!   API, not an oversight.
//!
//! `CREATE_NO_WINDOW` is documented as ignored for a child that is not a
//! console application, so the two sites that spawn **our own exe** — the
//! `--worker` self-exec (`worker/client.rs`, in this crate since T8/S8.3) and the
//! updater relaunch
//! (`src/commands/update.rs`) — could never pop a window in the shipped build,
//! where the exe is GUI-subsystem. They still route through here, for two
//! reasons: in a *debug* build the attribute is off and the exe is
//! console-subsystem, so the flag is live there; and a site added later
//! inherits the correct default rather than having to know this paragraph.
//! A reader who spots the apparent no-op should stop here, not "simplify" the
//! call away.
//!
//! What this module deliberately does NOT own: the environment (that is
//! [`crate::hostenv`]), stdout/stderr (each site chooses those), and any
//! `STARTUPINFO`/`SW_HIDE` or `DETACHED_PROCESS` route (the former is
//! nightly-only on stable Rust; the latter is documented to make
//! `CREATE_NO_WINDOW` *ignored*, and leaves the child free to `AllocConsole`).
//!
//! What it DOES also own since #79: **the Windows shape of a Tool child's
//! command line** — [`cmd_c_tail`] / [`append_cmd_c`]. Same reason as the
//! creation flags: `raw_arg` arrives on the same
//! `std::os::windows::process::CommandExt` trait as `creation_flags`, so a call
//! site given one has a one-line `use` away from the other, and census
//! invariant E keeps both call forms in this module. The Unix `sh -c`
//! convention, the env and the child's stdio stay where they were.
//!
//! And since ADR 0080's amendment: **the process-group kill** —
//! [`kill_process_group`]. Four sites used to signal a child's group with four
//! independent implementations under two pid policies, and the two that read
//! `Child::id()` LIVE at kill time skipped the group kill on the reap-ordered
//! crash path (the live read is `None` once the child is reaped) — i.e. exactly
//! where the group kill matters. One owner makes the latched-pid rule the API:
//! the caller passes the pid it captured at spawn, and census invariant F keeps
//! the syscall in this file (with one named carve-out — a synchronous `Drop`
//! cannot call an `async fn`). Like the constructor policy, this is a kill
//! seam: the REAP stays with the caller, because the callers genuinely differ
//! (the elevated path must not leave a zombie; the bash and Worker paths must
//! not reap a child another task owns).
//!
//! Nothing here is provable by running it — `cargo test` is itself a
//! console-subsystem process, so a child inherits the test harness's console
//! whether or not the flag is set, and neither std nor tokio can read a
//! creation flag back off a `Command`. The census test in this module (Task 3)
//! is what enforces the policy: it scans `src/` for a `Command::new(` outside
//! these two constructors and fails the build on one.

/// `CREATE_NO_WINDOW` — "The process is a console application that is being
/// run without a console window" (Process Creation Flags, Win32). A bare
/// `u32`, so this needs no `windows`/`windows-sys`/`winapi` dependency.
pub const CREATE_NO_WINDOW: u32 = 0x0800_0000;

// Blanket import: the std arm needs the trait; the tokio arm resolves to
// tokio's own INHERENT `creation_flags` (tokio-1.53.1/src/process/mod.rs:675).
#[cfg(windows)]
use std::os::windows::process::CommandExt as _;

// The parameter is `_cmd` and the CALL below is unconditional on purpose: with
// a plain `cmd` and the `cfg` on the call (or on the body), the non-Windows
// build has an unused binding or an unnecessary `mut`, and
// `cargo clippy --all-targets -- -D warnings` is red on Linux CI for it.
fn hide(_cmd: &mut std::process::Command) {
    #[cfg(windows)]
    _cmd.creation_flags(CREATE_NO_WINDOW);
}

fn hide_tokio(_cmd: &mut tokio::process::Command) {
    #[cfg(windows)]
    _cmd.creation_flags(CREATE_NO_WINDOW);
}

/// The `tokio::process::Command` constructor — the only one in the crate.
///
/// Carries the child-console policy and nothing else: no `env`, no stdio
/// defaults. Pass the program exactly as you would to
/// `tokio::process::Command::new`.
pub fn command<S: AsRef<std::ffi::OsStr>>(program: S) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(program);
    hide_tokio(&mut cmd);
    cmd
}

/// The `std::process::Command` constructor (the updater relaunch), with the
/// same policy as [`command`].
pub fn command_std<S: AsRef<std::ffi::OsStr>>(program: S) -> std::process::Command {
    let mut cmd = std::process::Command::new(program);
    hide(&mut cmd);
    cmd
}

/// Tool children only. Cross-platform — no `cfg`.
///
/// `tokio`'s `output()` pipes stdout/stderr but NOT stdin, so without this a
/// Tool child inherits the parent's stdin handle: on the Worker that is a live
/// pipe nobody writes to, and `cat` — or `python -`, a `git credential`
/// helper, an `ssh` password prompt — blocks on it until the bash timeout
/// fires. Runtime children must NOT call this; see the module docs.
pub fn closed_stdin(cmd: &mut tokio::process::Command) {
    cmd.stdin(std::process::Stdio::null());
}

/// The `cmd.exe` tail that runs `command` exactly as written: `/d /s /c "<command>"`.
///
/// PURE and cross-platform on purpose — it is a `format!`, so the exact bytes
/// are asserted on Linux and macOS too (`the_cmd_c_tail_is_the_documented_form_and_escapes_nothing`).
/// That is the only honest local proof this fix has: std renders the command
/// line inside `CreateProcess` and exposes no getter (the same introspection
/// hole as `creation_flags`, rust-lang/rust#96794), so nothing runnable can
/// read back what `cmd.exe` was actually handed.
///
/// * `/d` suppresses the `HKCU\Software\Microsoft\Command Processor\AutoRun`
///   command, so a machine-wide AutoRun cannot splice itself in front of every
///   agent command.
/// * `/s` makes `cmd.exe` follow its documented rule for the quotes around the
///   rest of the line: if the first character is a quote, strip it and remove
///   the last quote. Our outer pair is therefore consumed and EVERY INNER QUOTE
///   SURVIVES — which is the entire point of building the tail by hand.
pub fn cmd_c_tail(command: &str) -> String {
    format!("/d /s /c \"{command}\"")
}

/// Appends [`cmd_c_tail`] to a Tool child's `Command` with NO quoting and NO
/// escaping (`raw_arg`). Windows-only in effect; the call is unconditional so a
/// non-Windows build has one code path.
///
/// Why not `arg`: `Command::arg` escapes for a program that follows
/// `CommandLineToArgvW`, and `cmd.exe` does not. `append_arg`
/// (`library/std/src/sys/args/windows.rs:173`) wraps any string containing a
/// space in an outer quote pair and turns every embedded `"` into `\"`; `cmd /s`
/// consumes only the OUTER pair, so the `\"` stay in the text `cmd` parses.
/// `tasklist /FI "IMAGENAME eq x.exe"` therefore arrives as
/// `/FI \"IMAGENAME eq x.exe\"` — and `start`'s title-slot rule turns that
/// wreckage into a modal `Windows cannot find '\'` dialog on the user's desktop
/// (issue #79; CVE-2024-24576 / CVE-2024-43402 are the same contract failure).
///
/// It lives HERE rather than at the call site for the same reason the
/// `CREATE_NO_WINDOW` flag does: `raw_arg` sits on the same
/// `std::os::windows::process::CommandExt` trait as `creation_flags`, and the
/// census keeps that trait's call forms inside this module (invariant E — see
/// the census's docs and ADR 0060's amendment). A call site that could reach
/// `raw_arg` can reach `creation_flags` by the same one-line `use`.
pub fn append_cmd_c(_cmd: &mut tokio::process::Command, _command: &str) {
    #[cfg(windows)]
    {
        // Resolves to tokio's INHERENT `raw_arg`
        // (tokio-1.53.1/src/process/mod.rs:420, inside `cfg_windows!`), which
        // forwards to std's. No second `use` — the module-level
        // `CommandExt as _` above is what `hide` needs and stays as it is.
        _cmd.raw_arg(cmd_c_tail(_command));
    }
}

/// SIGKILL a child's WHOLE process group by the LATCHED pid, then fall back to
/// killing the direct child.
///
/// `pid` is the pid captured BEFORE the child could be reaped: `Child::id()`
/// goes `None` the moment the exit status is collected, and a `None` here skips
/// the group kill and leaves an orphaned grandchild. Every caller therefore
/// latches at spawn — the historical defect this owner replaces was two sites
/// that read `Child::id()` LIVE at kill time, which is exactly the reap-ordered
/// crash path where the group kill matters (ADR 0080's amendment, Finding 3).
///
/// `#[cfg(unix)]`: a negative pid is the whole process group (the children this
/// is used for are group leaders — `process_group(0)` for a Tool child,
/// `setpgid(0, 0)` in `pre_exec` for the Worker and the elevated child).
/// `#[cfg(not(unix))]`: the direct child only — there are no process groups to
/// signal off unix, so nothing is lost, and the claim in the unix note above is
/// simply not made there.
///
/// **Kill-only, by design** (the shape `kill_bash_process_group` already had):
/// reaping stays the caller's decision, because the callers differ — the bash
/// and Worker paths must not reap (a `wait` there would race another owner of
/// the status), while the elevated path wants no zombie and reaps through
/// `kill_process_group_and_reap`. The `SudoGroupGuard::drop` is the one site
/// that keeps its own inline syscall: a `Drop` is synchronous and cannot reach
/// an `async fn`; its pid is already latched and the census carves it out by
/// name.
pub async fn kill_process_group(pid: Option<u32>, child: &mut tokio::process::Child) {
    #[cfg(unix)]
    {
        // A negative pid SIGKILLs the WHOLE process group (the child is the
        // group leader). ESRCH — the group already gone — is the normal case
        // for a child that exited on its own, hence the discarded result.
        if let Some(pid) = pid {
            let _ = unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
        }
        // Fallback: kill the direct child as well (a no-op on a reaped child).
        let _ = child.kill().await;
    }
    #[cfg(not(unix))]
    {
        // Gated on the ARM, not the call, so the `pid` binding does not exist
        // unread on Windows (`unused_variable` under `-D warnings`).
        let _ = pid; // No process groups to kill here.
        let _ = child.kill().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// (ADR 0080 amendment, Finding 3 / A5) The behavioural pin for
    /// "a LATCHED pid group-kills even after the direct child has been
    /// reaped". This is the observable `WorkerHandle::kill()` delegates to, so
    /// this test IS the method's RED (Decision 3 of the task06 plan): the
    /// pre-consolidation code read `Child::id()` LIVE at kill time, and that
    /// returns `None` the moment the exit status is collected — which is
    /// exactly the state a crash (or a reap-ordered teardown) leaves — so the
    /// negative-pid group kill was SKIPPED and an orphaned grandchild survived
    /// the one path that exists to kill it.
    ///
    /// Shape: `/bin/sh -c` spawned as its own process group (the sudo/worker
    /// `setpgid` pattern), backgrounding a `sleep 60` grandchild whose pid it
    /// writes to a file, then exiting 0 IMMEDIATELY. The direct child is
    /// therefore reaped BEFORE the helper is called, so the ONLY way the
    /// grandchild can die is a `kill(-pgid)` built from the pid latched at
    /// spawn.
    ///
    /// The helper is kill-only (matching `kill_bash_process_group`): the wait
    /// stays the caller's, so the test does NOT `child.wait()` again after the
    /// helper — the single reap already happened above.
    #[cfg(unix)]
    #[tokio::test]
    async fn kill_process_group_kills_by_the_latched_pid_after_the_direct_child_is_reaped() {
        use std::process::Stdio;

        assert!(
            std::path::Path::new("/bin/sh").exists(),
            "/bin/sh must exist"
        );
        let scratch =
            crate::test_support::ScratchDir::in_env_temp("archimedes-childproc-kill-test-", "pid");
        let grandchild_pid_file = scratch.join("grandchild.pid");
        let mut cmd = command("sh");
        cmd.arg("-c").arg(format!(
            "( sleep 60 > /dev/null 2>&1 & echo $! > {} ); sleep 0.2",
            grandchild_pid_file.display()
        ));
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
        cmd.stdin(Stdio::null());
        cmd.kill_on_drop(true);
        // The child owns its process group (the sudo/worker pattern) so a
        // negative pid reaches the grandchild.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = cmd.spawn().expect("`sh` starts");
        // The pid is LATCHED at spawn — the whole point of the helper's
        // parameter, and the value a live `Child::id()` can no longer produce.
        let pid = child.id();
        assert!(pid.is_some(), "a freshly spawned child has a pid");

        // REAP the direct child BEFORE the helper runs (the child exits 0 on
        // its own after the backgrounded `sleep` is running).
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), child.wait())
            .await
            .expect("the direct child exits promptly")
            .expect("the wait succeeds");
        assert!(
            status.success(),
            "the fixture child exits 0, got {status:?}"
        );
        assert!(
            child.id().is_none(),
            "the LIVE read is `None` once the child is reaped — the exact state \
             the pre-fix kill sites read, and why they skipped the group kill"
        );

        let grandchild_pid: u32 = std::fs::read_to_string(&grandchild_pid_file)
            .expect("the script wrote the grandchild's pid")
            .trim()
            .parse()
            .expect("a pid");

        // The helper, called with the LATCHED pid on the already-reaped child.
        kill_process_group(pid, &mut child).await;

        // The GRANDCHILD is dead: the negative-pid kill fired off the latched
        // value. (Poll — the kernel reaps the orphaned, killed grandchild
        // promptly, but not instantaneously.)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut gone = false;
        while std::time::Instant::now() < deadline && !gone {
            if unsafe { libc::kill(grandchild_pid as i32, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                gone = true;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        assert!(
            gone,
            "the grandchild (pid {grandchild_pid}) is dead — a latched pid must \
             group-kill even after the direct child was reaped"
        );
    }

    /// Invariant F — the process-group kill lives ONLY in this module's helper
    /// (plus the one carved-out `Drop`).
    ///
    /// **Why this invariant exists.** Four independent
    /// `libc::kill(-(pid as i32), libc::SIGKILL)` implementations existed at
    /// four sites under two pid policies (ADR 0080's amendment, Finding 3):
    /// two latched the pid at spawn (`kill_bash_process_group`'s parameter,
    /// `SudoGroupGuard`) and two re-read `Child::id()` live at kill time
    /// (`WorkerHandle::kill`, `kill_process_group_and_reap`) — and a live read
    /// returns `None` on a reaped child, so the two live sites silently skipped
    /// the group kill on exactly the crash/reap path the group kill exists for.
    /// The consolidation gives the kill ONE owner; this census is what keeps a
    /// fifth site from appearing with either policy.
    ///
    /// **The one carve-out, by name:** `interactive/sudo_exec.rs`'s
    /// `SudoGroupGuard::drop`. A `Drop` impl is SYNCHRONOUS and cannot reach an
    /// `async fn`, so the guard keeps its inline syscall; its pid is already
    /// latched at spawn and its disarm-on-reap semantics are pinned by
    /// `run_sudo_real_a_dropped_future_kills_the_whole_process_group` and
    /// `run_sudo_real_a_normal_completion_does_not_kill_the_process_group`.
    /// That is 1 site in `childproc.rs` + 1 in `sudo_exec.rs`, ZERO everywhere
    /// else — in particular zero in `exec/bash.rs` (now a thin wrapper),
    /// `worker/client.rs` (now delegates) and `interactive/mod.rs`.
    ///
    /// **Armed, and how it could fail.** The guard below asserts the scan FINDS
    /// the two permitted sites before it asserts anything is absent: a scan
    /// whose needle drifted from the code would report a perfect clean bill of
    /// health for an empty corpus, which ADR 0080's standard calls UNVERIFIED,
    /// not green. It is a LINT on text, so a string literal carrying the needle
    /// also trips it — the error goes the safe way (a false alarm costs a
    /// rename).
    #[test]
    fn production_process_group_kill_lives_only_in_childproc() {
        // T5/S2 repointed every path here at its OWN package, and T5/S3 had to
        // do it again: `interactive/` moved into the crate with S3, so both
        // `interactive` paths below carry the `fulcrum-core/` label now. A stale
        // prefix is not a wrong-file bug but a MISSING file — `production_region`
        // panics on an unreadable path, so it fails loudly rather than reading as
        // "0 sites, all clear". That is the property that makes this census the
        // instructive contrast to the `fake_mcp_stdio` spawners (the status file's
        // finding 3): a wrong root here is RED, not silently green.
        const SUDO_EXEC: &str = "fulcrum-core/interactive/sudo_exec.rs";
        // The needle is the negative-pid SIGKILL itself — the syscall shape,
        // not a name that a wrapper could rename.
        const NEEDLE: &str = "libc::kill(-(pid as i32)";
        // The files the consolidation touched, plus the two owners. Anything
        // NOT in this list is out of scope for the sweep (a site elsewhere
        // would be a new conversation, not a silent pass).
        let files = [
            SELF,
            SUDO_EXEC,
            "fulcrum-core/tools/exec/bash.rs",
            // T8/S8.3: `worker/client.rs` moved into this crate, so its census
            // label changed root (`archimedes/` → `fulcrum-core/`). A stale
            // prefix here is a RED, not a skip — `region_of_labeled` panics when
            // a labelled path names no file.
            "fulcrum-core/worker/client.rs",
            // T5/S3: moved with `interactive`.
            "fulcrum-core/interactive/mod.rs",
        ];

        let roots = source_roots();

        let sites: Vec<(String, Vec<usize>)> = files
            .iter()
            .map(|rel| {
                let region = region_of_labeled(&roots, rel);
                let lines: Vec<usize> = region
                    .iter()
                    .filter(|l| l.text.contains(NEEDLE))
                    .map(|l| l.line)
                    .collect();
                ((*rel).to_string(), lines)
            })
            .collect();

        // ── Vacuity / arming guard FIRST: the sweep must SEE both permitted
        // sites, or the "zero everywhere else" claim below has read nothing.
        let in_self = sites
            .iter()
            .find(|(rel, _)| rel == SELF)
            .map(|(_, lines)| lines.len())
            .unwrap_or(0);
        let in_sudo = sites
            .iter()
            .find(|(rel, _)| rel == SUDO_EXEC)
            .map(|(_, lines)| lines.len())
            .unwrap_or(0);
        assert_eq!(
            (in_self, in_sudo),
            (1, 1),
            "Invariant F is BLIND: the sweep found {in_self} copy(ies) of \
             `{NEEDLE}` in `{SELF}` (expected 1 — the helper) and {in_sudo} in \
             `{SUDO_EXEC}` (expected 1 — the `SudoGroupGuard::drop` carve-out). At 0 \
             the needle drifted from the code (or a root resolved wrongly: {}), and \
             the zero-check below would pass having seen nothing.",
            source_roots()
                .iter()
                .map(|r| r.path.display().to_string())
                .collect::<Vec<_>>()
                .join(" + "),
        );

        // ── The invariant: nothing else kills a process group.
        let violations: Vec<String> = sites
            .iter()
            .filter(|(rel, _)| rel != SELF && rel != SUDO_EXEC)
            .flat_map(|(rel, lines)| {
                lines
                    .iter()
                    .map(move |line| format!("{rel}:{line}"))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert!(
            violations.is_empty(),
            "Invariant F (one owner of the process-group kill): production code signals a \
             process group outside `childproc::kill_process_group` at {violations:?} — every \
             extra site re-chooses the pid policy, and the historical mistake was reading \
             `Child::id()` LIVE there (it is `None` once the child is reaped, so the group \
             kill is silently skipped on the crash path and a grandchild survives). Call \
             `childproc::kill_process_group(latched_pid, child)`; the only permitted \
             inline site is `SudoGroupGuard::drop`, which cannot be async (ADR 0080's \
             amendment).",
        );
    }

    /// Claim: the constant is the value Microsoft documents for
    /// `CREATE_NO_WINDOW`, written out here as an independent literal rather
    /// than restating the definition — a typo in the constant must redden this
    /// test, not be mirrored by it.
    #[test]
    fn create_no_window_is_the_documented_value() {
        assert_eq!(
            CREATE_NO_WINDOW, 0x0800_0000_u32,
            "`CREATE_NO_WINDOW` must be the documented 0x0800_0000 \
             (Process Creation Flags, Win32)"
        );
    }

    /// Claim: a Tool child built by [`command`] + [`closed_stdin`] reaches stdin
    /// EOF at once — `sh -c 'cat'` reads EOF immediately, writes nothing and
    /// exits 0, inside the 5 s wall-clock guard. `success()` is the assertion,
    /// NOT `!status.success() || …`: a null stdin makes `cat` exit 0 with empty
    /// stdout, and a negated compound would also pass on an unrelated failure.
    ///
    /// ⚠ **A regression pin, not the proof.** On a host where cargo's own stdin
    /// is already `/dev/null` — which is what a CI step gets — this test passes
    /// with OR without `closed_stdin`, because the inherited handle the call
    /// replaces is already an EOF source. It pins the SHAPE (the API exists, is
    /// cross-platform, and a child built through `childproc` still runs); it
    /// cannot pin the EFFECT. The behaviour proof is Task 3's `execute_tool`
    /// test, which re-execs this binary with fd 0 wired to a `std::io::pipe` the
    /// parent holds OPEN and never writes — the Worker's real shape, and the one
    /// stdin that separates "we nulled it" from "we inherited it". The policy
    /// proof is Task 3's census.
    #[tokio::test]
    async fn tool_child_reads_eof_on_closed_stdin() {
        let mut cmd = command("sh");
        closed_stdin(&mut cmd);
        cmd.args(["-c", "cat"]);
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), cmd.output())
            .await
            .expect("did not time out")
            .expect("`sh` starts");
        assert!(
            out.status.success() && out.stdout.is_empty(),
            "`sh -c 'cat'` on a null stdin must exit 0 with no output, got {out:?}"
        );
    }

    /// Claim: [`cmd_c_tail`] renders the documented `/d /s /c "<command>"` form
    /// with the command inside it BYTE-FOR-BYTE and no escaping of any kind —
    /// in particular no `\"`, which is what `Command::arg` (std's
    /// `append_arg`, `library/std/src/sys/args/windows.rs:173`) produces for
    /// every embedded quote (issue #79).
    ///
    /// This is the strongest proof this fix has on a non-Windows host: the
    /// function is PURE (a `format!`), so the exact bytes are asserted here, on
    /// Linux and macOS, and not only on the leg that can run `cmd.exe`.
    ///
    /// What it does NOT prove: what `cmd.exe` did with those bytes. std renders
    /// the command line inside `CreateProcess` and exposes no getter for it —
    /// the same introspection hole as `creation_flags` (rust-lang/rust#96794) —
    /// so nothing runnable can read back what the child was handed. That half
    /// is the two `#[cfg(windows)]` tests in
    /// `agent/tools/exec/tests/bash.rs`, which run only on CI's
    /// `windows-latest` leg.
    #[test]
    fn the_cmd_c_tail_is_the_documented_form_and_escapes_nothing() {
        // The documented shape: `/d /s /c` + ONE outer quote pair, nothing else.
        assert_eq!(
            cmd_c_tail("echo hi"),
            "/d /s /c \"echo hi\"",
            "the tail is `/d /s /c \"<command>\"` and nothing more"
        );

        // The command issue #79 was reported with, verbatim. Both sides are raw
        // strings on purpose: an escaped-quote expectation is easy to mistype,
        // and a mistyped expectation is a false RED.
        let q = r#"tasklist /FI "IMAGENAME eq x.exe""#;
        assert_eq!(
            cmd_c_tail(q),
            r#"/d /s /c "tasklist /FI "IMAGENAME eq x.exe"""#,
            "the command is wrapped in ONE outer pair and otherwise untouched"
        );
        // The regression assertion, and the reason this test exists: `arg`
        // escapes every embedded `"` as `\"` (std's `append_arg` emits `2n+1`
        // backslashes) and `cmd /s` strips only the OUTER quote pair, so those
        // escapes stay visible to command parsing — which is issue #79. Green
        // here means the seam hands `raw_arg` a string with NONE of that.
        assert!(
            !cmd_c_tail(q).contains("\\\""),
            "the tail must contain NO backslash-escaped quote — `Command::arg` \
             would have produced one for every `\"` in the command, and `cmd /s` \
             strips only the outer pair, so the escapes reach command parsing \
             (issue #79). got {:?}",
            cmd_c_tail(q)
        );
        // The command survives byte-for-byte INSIDE the tail.
        assert!(
            cmd_c_tail(q).contains(q),
            "the command must appear verbatim inside the tail, got {:?}",
            cmd_c_tail(q)
        );
        // An empty command is a no-op tail (and `arg` rendered it `""` too, so
        // nothing moves here — pinned so a future "optimisation" cannot drop
        // the quotes, which would make `cmd /c` inherit the console instead).
        assert_eq!(cmd_c_tail(""), "/d /s /c \"\"");
    }

    // ─────────────────────────────────────────────────────────────────────
    // The census (ADR 0060). Read this before trusting it.
    //
    // **What it does NOT prove:** that `CREATE_NO_WINDOW` suppresses a window.
    // Nothing runnable from `cargo test` can — the test harness is itself a
    // console-subsystem process, so a child inherits the HARNESS's console
    // whether or not the flag is set, and neither std nor tokio exposes a
    // creation-flag getter (rust-lang/rust#96794) so the flag cannot even be
    // read back off the `Command` we configured; and Windows CI builds DEBUG
    // only, where the exe is console-subsystem, so CI never runs the shipped
    // GUI-subsystem binary the bug needed. That
    // half of the claim stays what it is: ONE manual run of the packaged
    // Windows release build, recorded in the PR description. This census is a
    // LINT — it proves no production site bypasses the module that owns the
    // policy, in the spirit of `hostenv::configured_env` ("the only honest
    // introspection available" for the same class of reason).
    //
    // Scanned: BOTH packages' `src/**/*.rs` — `crates/fulcrum-core/src` and
    // `src-tauri/src` (see `source_roots`; since T5/S2 the policed code spans
    // two packages and a one-root scan would certify the other half unseen).
    // `tests/*.rs` is deliberately NOT scanned —
    // those integration fixtures build raw `Command`s ON PURPOSE (they launch
    // the built binary, and a fixture that wanted the policy opts in
    // explicitly), so scanning them would demand a change to code whose job is
    // to be the outside view of the app.
    //
    // And it is a LINT, not a parser: it matches TEXT, so a string literal or a
    // `concat!` fragment containing `Command::new(` would also trip invariant A.
    // That error goes the safe way — a false alarm costs one rename, a false
    // pass costs a window per command on someone's desktop — which is the same
    // trade every grep-level lint makes.

    /// One surviving line of a production region: its 1-based number in the
    /// file (so a violation reads as `file:line`) and its text.
    struct SourceLine {
        line: usize,
        text: String,
    }

    /// The workspace root — `CARGO_MANIFEST_DIR` TWO levels up.
    ///
    /// T5/S2 (Step A-bis) moved this file from `src-tauri/` to
    /// `crates/fulcrum-core/`, so `CARGO_MANIFEST_DIR` is no longer the package
    /// that owns the code the census polices, and its `.parent()` is `crates/`,
    /// not the repo root. Everything below resolves through HERE so there is one
    /// definition of "where the workspace is" in the crate.
    fn workspace_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("crates/fulcrum-core has a grandparent (the repo root)")
            .to_path_buf()
    }

    /// The shared cargo `target/` dir. From `crates/fulcrum-core` that is TWO
    /// levels up, NOT one — the trap T4 left and Step A-bis exists to close.
    ///
    /// T5/S3 MOVED THIS HERE (`test_support::target_debug_dir`) and left a
    /// one-line alias below. The reason is Step A-bis's own instruction — "fix
    /// once, not eight times" — and S3 is where it stops being hypothetical: the
    /// first consumers arrived (`mcp/stdio.rs`, `mcp/tool.rs`, `mcp/manager.rs`,
    /// four `fake_mcp_stdio` spawners). A second definition of "where cargo
    /// builds" inside the same crate is exactly the drift this helper exists to
    /// prevent, so the census keeps its local `workspace_root()` (it needs the
    /// ROOT, to walk two `src/` trees) and defers the `target/debug` half to the
    /// shared one.
    ///
    /// NO S2 FILE CONSUMES THIS, and that was a measured finding, not an
    /// oversight: nothing in `{tools, web, boundary, policy, definition_permission,
    /// childproc, skills, agents, roots}` spawns a built binary (grep for
    /// `target/debug` / `CARGO_BIN_EXE` / `fake_worker` / `fake_mcp_stdio` across
    /// the moved set returns only prose in doc comments). It landed in S2 anyway
    /// because the pin below is what makes the resolution provably right BEFORE a
    /// consumer depends on it — the alternative is S3 discovering it mid-move.
    /// `#[cfg(test)]` for that reason: an ungated `pub(crate)` fn with no
    /// production caller is `dead_code`, which the gate's `-D warnings` rejects.
    #[cfg(test)]
    pub(crate) fn target_debug_dir() -> std::path::PathBuf {
        crate::test_support::target_debug_dir()
    }

    /// The pin Step A-bis demands: `target_debug_dir()` resolves to the directory
    /// cargo ACTUALLY builds into, proved against `current_exe()`.
    ///
    /// # Why the pin had to live in the crate (and what S8.3 changed)
    ///
    /// T4's `fixture_path_resolves_to_the_workspace_root_target` used to live in
    /// the desktop's `worker/client.rs`, which S5 assumed would stay desktop
    /// through T8 — so it would keep passing while every moved fixture path was
    /// broken. **T8/S8.3 moved that file, and the assumption turned out to be the
    /// wrong half:** the moved helper resolved to `<root>/crates/target/debug/…`
    /// and its two `starts_with` asserts PASSED on the broken path (measured — the
    /// RED came only from the `is_file()` witness). `client.rs` now carries an
    /// equality + `current_exe()` pin of its own, so the drift this pin was written
    /// to catch has a second witness.
    ///
    /// This pin stays because it guards the RESOLVER, not one caller: every crate
    /// fixture spawner (`mcp/stdio.rs`, `mcp/tool.rs`, `mcp/manager.rs`,
    /// `worker/client.rs`, `worker/manager/tests`) derives from
    /// `target_debug_dir()`, and comparing against `current_exe()` is the one
    /// comparison that cannot go green on a wrong LEVEL count.
    ///
    /// # How it compares
    ///
    /// The ground truth is `current_exe()`: the test binary IS
    /// `target/debug/deps/<name>-<hash>`, cargo put it there, so walking up twice
    /// from it gives the real `target/debug` with NO manifest arithmetic involved.
    /// If the crate's TWO-level resolution drifts to ONE level
    /// (`crates/target/debug`), it stops equaling the ground truth and this
    /// reddens. Since S8.3 the crate CAN call `worker/client.rs::fixture_path()`
    /// directly (same package, both `#[cfg(test)]`) and does — in that module's
    /// own equality pin — so this pin is now the resolver-level half of a pair
    /// rather than the only witness.
    ///
    /// # Verified non-vacuous
    ///
    /// Collapsing `workspace_root()`'s `.parent().and_then(|p| p.parent())` to a
    /// single `.parent()` makes this fail: the assertion quotes both sides, so the
    /// failure names `crates/target/debug` against the real one. That is the T4
    /// lesson applied — a pin that cannot go red is not a pin.
    #[test]
    fn target_debug_dir_is_where_cargo_actually_builds() {
        let claimed = target_debug_dir();
        let exe = std::env::current_exe().expect("a test binary has a path");
        let ground_truth = exe
            .parent() // target/debug/deps
            .and_then(|deps| deps.parent()) // target/debug
            .expect("the test binary lives in target/debug/deps");

        assert_eq!(
            claimed,
            ground_truth,
            "`target_debug_dir()` resolved to {claimed:?}, but cargo built THIS test \
             binary into {ground_truth:?}/deps — the two disagree, so every fixture \
             path the crate derives from `target_debug_dir()` would spawn an ENOENT \
             path (or, at a `let _ =` site, quietly nothing). The crate manifest is \
             {}; the workspace root must be its GRANDPARENT (T5/S2 Step A-bis).",
            env!("CARGO_MANIFEST_DIR"),
        );

        // The existence witness. It used to carry a different job: while
        // `worker/client.rs` was desktop, this was the crate-side half of a
        // two-member agreement that both packages land on ONE `target/`. S8.3
        // moved that file, so the agreement is now intra-crate and this assert is
        // simply the cheap "a resolution that names a directory no build writes to
        // is wrong on any host that has run a build" check.
        assert!(
            claimed.is_dir(),
            "the resolved {claimed:?} is a real directory — cargo writes build output \
             here, so a resolution that names a non-existent dir is wrong on any host \
             that has run a build",
        );
    }

    /// A census-scanned source tree: a label plus its `src/` directory.
    struct SourceRoot {
        /// The prefix every relative path from this root carries, so a
        /// violation report says WHICH package the offender lives in and two
        /// same-named files in different packages cannot collide in the scan.
        label: &'static str,
        path: std::path::PathBuf,
    }

    /// BOTH packages' `src/` trees.
    ///
    /// THE REASON THIS IS TWO ROOTS AND NOT ONE: the census asserts ABSENCES
    /// ("no `Command::new(` outside `childproc`", "no `.creation_flags(` outside
    /// `childproc`", "no `.raw_arg(` outside `childproc`"). Before S2 one walk of
    /// `src-tauri/src` saw every file those invariants police. After S2 the
    /// policed code sits in TWO packages, and a scan of either one alone reports a
    /// clean bill of health for the half it never opened — the fail-OPEN shape
    /// this repo keeps recording (the same class as the 18c census scan roots and
    /// the five-edge scan). So: walk both, always.
    ///
    /// The workspace root itself is deliberately NOT a walk root: it holds
    /// `target/`, whose `build/` trees carry generated `*.rs` (and tens of GB).
    /// Walking it would both hang and invent violations in generated code. Two
    /// named `src/` dirs is the exact scope.
    fn source_roots() -> Vec<SourceRoot> {
        let root = workspace_root();
        let roots = vec![
            SourceRoot {
                label: "fulcrum-core/",
                path: std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            },
            SourceRoot {
                label: "archimedes/",
                path: root.join("src-tauri").join("src"),
            },
        ];
        for r in &roots {
            // Loud, not fail-open: a root that does not resolve would empty its
            // half of the corpus, and every "zero violations" below would then be
            // true of nothing. The message names the root, per the guard's own
            // convention.
            assert!(
                r.path.is_dir(),
                "the census resolved the wrong root: {} is not a directory \
                 (workspace root resolved to {:?} from CARGO_MANIFEST_DIR={})",
                r.path.display(),
                root,
                env!("CARGO_MANIFEST_DIR"),
            );
        }
        roots
    }

    /// The census's own path, `/`-normalised and prefixed with its ROOT LABEL —
    /// the one file allowed to hold `Command::new(` and `creation_flags`.
    /// Normalised so the comparison holds on Windows, where `strip_prefix`
    /// yields `\`.
    ///
    /// T5/S2 moved this file out of `src-tauri/src/agent/` into the crate, so the
    /// label changed from `agent/childproc.rs` to `fulcrum-core/childproc.rs`:
    /// the label IS the root's identity, so a moved file's label moves with it.
    const SELF: &str = "fulcrum-core/childproc.rs";

    /// Resolve one labelled path (`"<label><rest>"`, as the `files` lists below
    /// spell them) to an absolute path, WITHOUT reading it. Panics if the label
    /// names no root — a typo'd label would otherwise read as "file absent".
    fn resolve_labeled(roots: &[SourceRoot], labeled: &str) -> std::path::PathBuf {
        for root in roots {
            if let Some(rest) = labeled.strip_prefix(root.label) {
                return root.path.join(rest);
            }
        }
        panic!(
            "census path `{labeled}` names no source root ({:?}) — its half of the \
             corpus would be silently missing",
            roots.iter().map(|r| r.label).collect::<Vec<_>>().join(", ")
        );
    }

    /// The production region of one labelled path (`"<label><rest>"`, as the
    /// `files` lists below spell them), resolved against its own root.
    ///
    /// Panics if the label names no root — a typo'd label would otherwise read as
    /// "file absent", and every count-based vacuity guard downstream would then
    /// fail (the good direction) while any absence-based check would pass blind.
    fn region_of_labeled(roots: &[SourceRoot], labeled: &str) -> Vec<SourceLine> {
        production_region(&resolve_labeled(roots, labeled))
    }

    /// Every `*.rs` under BOTH packages' `src/`, as `(labelled path, production
    /// region)` pairs sorted by path so a violation report is reproducible.
    ///
    /// The label is prefixed BEFORE sorting so `fulcrum-core/…` and `archimedes/…`
    /// form two contiguous blocks and a file that exists in both packages (both
    /// have a `web/config.rs` story to tell, and `types.rs` exists on both sides)
    /// can never be conflated in a report.
    fn scan_all(roots: &[SourceRoot]) -> Vec<(String, Vec<SourceLine>)> {
        let mut out: Vec<(String, Vec<SourceLine>)> = Vec::new();
        for root in roots {
            let mut paths = Vec::new();
            rust_files(&root.path, &mut paths);
            for path in paths {
                let rel = path
                    .strip_prefix(&root.path)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push((format!("{}{}", root.label, rel), production_region(&path)));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Whether a trimmed line is COMMENT text, which the census ignores — for
    /// the `#[cfg(test)]` boundary and for every match. Not cosmetic: three
    /// prose occurrences would otherwise be false positives or, worse, cut the
    /// scanned region short (`src/commands/update.rs` has a doc comment
    /// describing Tauri upstream's `Command::new(path)…`, this module's `//!`
    /// header contains `Command::new(`, and `worker/manager.rs`'s doc comment
    /// contains `` `#[cfg(test)]` ``, which under a "line contains" rule would
    /// silently leave ~1,100 lines unscanned forever). `///`, `//!`, `/*` and
    /// `*/` are already covered by `//` and `*`; they are listed anyway so the
    /// rule reads as intended.
    ///
    /// The `*` prefix is the one honest COST of a line-based lint: it also drops
    /// ~29 lines of deref ASSIGNMENTS (`*self.usage.lock()… = …`), so a
    /// `Command::new(` written as the target of a deref-assignment on its own
    /// line would be missed. No such line exists, and a spawn is a call on a
    /// bound `let mut cmd` in this codebase, so the blind spot costs nothing
    /// against the prose it buys — but it is a blind spot, not a subtlety.
    fn is_comment(trimmed: &str) -> bool {
        ["//", "///", "//!", "/*", "*", "*/"]
            .iter()
            .any(|prefix| trimmed.starts_with(prefix))
    }

    /// The PRODUCTION region of one file: every code line before the first line
    /// whose trimmed text STARTS WITH `#[cfg(test)]` (an attribute in item
    /// position — a boundary that merely *contains* the token is prose, and
    /// must also survive [`is_comment`]), with comment lines already dropped.
    /// Returns an empty `Vec` for paths matching `**/tests.rs` or containing
    /// `/tests/`.
    ///
    /// Panics rather than skipping an unreadable/unparseable file: a census
    /// that quietly stops at the first odd file is a census that passes for the
    /// wrong reason.
    fn production_region(path: &std::path::Path) -> Vec<SourceLine> {
        let norm = path.to_string_lossy().replace('\\', "/");
        if norm.ends_with("/tests.rs") || norm == "tests.rs" || norm.contains("/tests/") {
            return Vec::new();
        }
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("census cannot read {}: {e}", path.display()));
        let mut region = Vec::new();
        for (idx, raw) in text.lines().enumerate() {
            let trimmed = raw.trim();
            if trimmed.starts_with("#[cfg(test)]") && !is_comment(trimmed) {
                break;
            }
            if is_comment(trimmed) {
                continue;
            }
            region.push(SourceLine {
                line: idx + 1,
                text: raw.to_string(),
            });
        }
        region
    }

    /// Every `*.rs` under `dir`, recursively — hand-rolled `std::fs::read_dir`
    /// because a census test does not get to add a dependency (`walkdir` is not
    /// in `Cargo.toml`, and adding one to feed a test would be the wrong trade).
    /// Symlinked directories are not followed, so a link cycle cannot hang it.
    fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("census cannot read {dir:?}: {e}"));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|e| panic!("census cannot read an entry of {dir:?}: {e}"));
            let path = entry.path();
            let file_type = entry
                .file_type()
                .unwrap_or_else(|e| panic!("census cannot stat {}: {e}", path.display()));
            if file_type.is_dir() {
                rust_files(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    /// The census. Three invariants over the production regions of `src/`, plus
    /// the vacuity guard that makes the whole thing worth running.
    ///
    /// * **A — one constructor.** No production line outside [`SELF`] contains
    ///   `Command::new(`. This is ADR 0060's structural claim: there is no
    ///   `Command` to forget the flag on.
    /// * **B — one owner of the flag.** No production line outside [`SELF`]
    ///   contains `.creation_flags(` — anchored on the CALL form, not the bare
    ///   token, because prose legitimately names it (this module's docs and the
    ///   `CommandExt` comment; [`is_comment`] filters those anyway). A second
    ///   caller anywhere silently un-hides every child, because
    ///   `creation_flags` REPLACES rather than ORs.
    /// * **C — stdin, Tool children only.** Stated as an algorithm, not a
    ///   count-matching rule, because a naive version false-fails on correct
    ///   code: `exec_bash`'s `#[cfg(unix)]`/`#[cfg(not(unix))]` arms build TWO
    ///   `cmd` bindings that share ONE `spawn()`, so the correct code has 4
    ///   constructors and only 3 `closed_stdin` calls. Hence: every
    ///   `childproc::command(` in `exec.rs` is followed — before the NEXT
    ///   literal `spawn()` or `output()` — by `closed_stdin`; `exec.rs` holds
    ///   exactly 4 `childproc::command(` and at least 3 `closed_stdin` in its
    ///   production region; and no Runtime-child file (`mcp/stdio.rs`,
    ///   `worker/client.rs`, `interactive/sudo_exec.rs`, `commands/update.rs`)
    ///   mentions `closed_stdin` at all — nulling the Worker's JSONL stdin, an
    ///   MCP server's JSON-RPC stdin or `sudo_exec`'s password stdin would be a
    ///   worse bug than the hang this fixes.
    #[test]
    fn production_source_has_exactly_one_child_constructor() {
        // TWO roots, because after T5/S2 the policed code lives in two packages.
        // A one-root version of this test would keep passing while seeing only
        // half the tree — invariants A and B assert ABSENCES, so a missing root
        // is a silent green, not a red.
        let roots = source_roots();
        let regions = scan_all(&roots);

        // ── Vacuity guard, FIRST: a scanner that sees nothing must fail loudly,
        // not report a clean bill of health for an empty corpus (the same trick
        // `hostenv::tainted_patch` plays by asserting its patch is non-empty).
        // Deliberately NOT "files scanned >= N": the walk visits 100+ files
        // whether or not it resolved the root, so that guard can never fail.
        // These counts are the census's own eyesight.
        let mut constructors = 0usize;
        let mut std_constructors = 0usize;
        let mut constructor_files: Vec<&String> = Vec::new();
        for (rel, region) in &regions {
            let tokio = region
                .iter()
                .filter(|l| l.text.contains("childproc::command("))
                .count();
            // `childproc::command(` is not a substring of
            // `childproc::command_std(` (the `(` differs), so the two tallies
            // do not double-count.
            let std = region
                .iter()
                .filter(|l| l.text.contains("childproc::command_std("))
                .count();
            if tokio + std > 0 {
                constructors += tokio + std;
                std_constructors += std;
                constructor_files.push(rel);
            }
        }
        assert!(
            constructors >= 8 && constructor_files.len() >= 4 && std_constructors >= 1,
            "the census is BLIND: it found {constructors} `childproc::command*` calls in {} \
             distinct file(s) ({std_constructors} of them `command_std`) while scanning {} \
             files — the expectation is >= 8 calls in >= 4 files with >= 1 `command_std`, so \
             either the root resolved wrongly or the match strings drifted from the API, and \
             every invariant below would be vacuous",
            constructor_files.len(),
            regions.len()
        );

        // ── Invariant A.
        let mut constructors_outside: Vec<String> = Vec::new();
        for (rel, region) in &regions {
            if rel == SELF {
                continue;
            }
            for line in region.iter().filter(|l| l.text.contains("Command::new(")) {
                constructors_outside.push(format!("{rel}:{}", line.line));
            }
        }
        assert!(
            constructors_outside.is_empty(),
            "Invariant A (one constructor): production code builds a raw `Command` outside \
             `{SELF}` — that child gets NO `CREATE_NO_WINDOW` and pops a console window per \
             command on Windows; route it through `childproc::command`/`command_std`: \
             {constructors_outside:?}",
        );

        // ── Invariant B.
        let mut flag_owners: Vec<String> = Vec::new();
        for (rel, region) in &regions {
            if rel == SELF {
                continue;
            }
            for line in region
                .iter()
                .filter(|l| l.text.contains(".creation_flags("))
            {
                flag_owners.push(format!("{rel}:{}", line.line));
            }
        }
        assert!(
            flag_owners.is_empty(),
            "Invariant B (one owner of the creation flags): production code calls \
             `.creation_flags(` outside `{SELF}` — the call REPLACES rather than ORs \
             (rust-lang/rust#96794), so this silently un-hides every other child: \
             {flag_owners:?}",
        );

        // ── Invariant C.
        let region_of = |want: &str| -> &[SourceLine] {
            regions
                .iter()
                .find(|(rel, _)| rel == want)
                .map(|(_, region)| region.as_slice())
                .unwrap_or_else(|| panic!("the census never saw {want} — its walk is broken"))
        };
        let mentions = |region: &[SourceLine], needle: &str| {
            region.iter().filter(|l| l.text.contains(needle)).count()
        };

        let mut starved: Vec<String> = Vec::new();
        let mut total_tool_ctors = 0usize;
        let mut total_closed = 0usize;

        for (rel, region) in &regions {
            // The Tool-executor subtree, labelled with ITS package: `tools/exec`
            // moved into the crate in T5/S2, so the pre-move `agent/tools/exec`
            // prefix would match nothing and `total_tool_ctors` would be 0 — the
            // `== 4` below catches that, but only because the count is asserted;
            // the prefix is repointed so the invariant reads the real files.
            if !rel.starts_with("fulcrum-core/tools/exec") {
                continue;
            }
            if rel.contains("/tests/") || rel.ends_with("/tests.rs") || rel.ends_with("tests.rs") {
                continue;
            }
            for (idx, line) in region.iter().enumerate() {
                if !line.text.contains("childproc::command(") {
                    continue;
                }
                total_tool_ctors += 1;
                let stop = region[idx + 1..]
                    .iter()
                    .position(|l| l.text.contains("spawn()") || l.text.contains("output()"));
                let end = stop
                    .map(|offset| idx + 1 + offset)
                    .unwrap_or(region.len() - 1);
                if !region[idx..=end]
                    .iter()
                    .any(|l| l.text.contains("closed_stdin"))
                {
                    starved.push(format!(
                        "{}:{} (its `spawn()`/`output()` is at line {})",
                        rel, line.line, region[end].line
                    ));
                }
            }
            total_closed += mentions(region, "closed_stdin");
        }

        assert!(
            starved.is_empty() && total_tool_ctors == 4 && total_closed >= 3,
            "Invariant C (Tool children start at EOF): {starved:?} — every \
             `childproc::command(` under `agent/tools/exec*` must reach `childproc::closed_stdin` before \
             its `spawn()`/`output()`, and `agent/tools/exec*` must hold exactly 4 constructors (found \
             {total_tool_ctors}) with >= 3 `closed_stdin` calls (found {total_closed}) — 4 not 3 because \
             `exec_bash`'s two `cfg` arms share one `spawn()`. A Tool child that inherits \
             stdin instead hangs on it until the 300 s bash timeout; see the module docs.",
        );

        // T5/S3 repointed TWO of these four: `mcp/stdio.rs` and
        // `interactive/sudo_exec.rs` are in the crate now. The label is not
        // cosmetic — `region_of` PANICS when a labelled path names no file, so a
        // stale entry is a red test rather than a file the census skipped.
        for runtime_child in [
            "fulcrum-core/mcp/stdio.rs",
            // T8/S8.3: moved into this crate — the label follows the file.
            "fulcrum-core/worker/client.rs",
            "fulcrum-core/interactive/sudo_exec.rs",
            "archimedes/commands/update.rs",
        ] {
            let found = mentions(region_of(runtime_child), "closed_stdin");
            assert_eq!(
                found, 0,
                "`{runtime_child}` spawns a RUNTIME child whose stdin we write (JSONL / \
                 JSON-RPC / a password) — `closed_stdin` there would hang the Runtime; \
                 found {found} call(s)"
            );
        }
    }

    /// The crash-log module's path, `/`-normalised and LABELLED like [`SELF`] —
    /// it stayed desktop, so it carries the `archimedes/` root label.
    const CRASHLOG: &str = "archimedes/agent/crashlog.rs";

    /// Invariant D — `agent/crashlog.rs` may not resolve a data dir.
    ///
    /// A crash-log resolver in that file's PRODUCTION region is the bug this
    /// whole branch exists to kill: the panic hook would decide where the
    /// user's crash logs go *during the panic*, and because `std::panic` has
    /// ONE process-global hook slot that the `--lib` suite shares across
    /// threads, whichever closure occupies the slot then decides where EVERY
    /// thread's crash log lands — including a test thread's, into the real
    /// `~/.local/share/archimedes`. That is how 862 test panics reached a
    /// user's directory. The hook is handed its directory instead
    /// (`install_panic_hook(tag, dir)`), so this file must never call
    /// `dirs::data_dir()` again — and nothing but this test stops someone
    /// re-adding the perfectly reasonable-looking `let dir = dirs::data_dir();`.
    ///
    /// **Scope is that ONE file, and deliberately so.** A whole-tree version of
    /// this rule would be red on arrival: `lib.rs`, `session/router.rs`,
    /// `mcp/oauth.rs` and `commands/mcp.rs` all resolve directories, correctly,
    /// outside any panic path. The invariant is about the crash-log module
    /// because that is the module that runs while the process is dying.
    ///
    /// **Why the region and not the file.** [`is_comment`] is what makes this
    /// rule usable: `crashlog.rs` contains the literal string
    /// `dirs::data_dir()` THREE times and not one of them is code — twice in
    /// the module `//!` doc, which sits INSIDE the scanned region (so only the
    /// comment filter saves them), and once in a `///` doc comment inside
    /// `mod tests`, which the region boundary excludes before the filter is
    /// even consulted. A naive "the file contains the string" rule would
    /// therefore be a PERMANENT false positive — the kind that gets the test
    /// deleted rather than the code fixed — and a naive "everything above
    /// `mod tests`" rule would scan the module-doc prose and trip on it too.
    /// The region stops at the first non-comment line starting with
    /// `#[cfg(test)]`, which is also why the test-side mentions can never leak
    /// in.
    ///
    /// **No line numbers are quoted here, on purpose.** `crashlog.rs` shifts
    /// with every edit, and a drifted number is a worse guide than none —
    /// locate the occurrences with
    /// `grep -n 'dirs::data_dir()' src/agent/crashlog.rs` when you need them.
    ///
    /// **Inherited blind spot:** this is [`is_comment`] with all its limits, so
    /// the `*`-prefix rule silently drops any line whose trimmed text starts
    /// with `*` (see its docs) — a `dirs::data_dir()` written as the target of
    /// a deref-assignment on its own line would slip through. Name it, do not
    /// imply the region is exact.
    #[test]
    fn production_crashlog_never_resolves_a_data_dir() {
        let crashlog_path = resolve_labeled(&source_roots(), CRASHLOG);
        // One file is the whole scope of this invariant — no tree walk. The
        // call panics rather than skipping if the file is unreadable, so a
        // wrong root fails loudly here instead of silently emptying the region.
        let region = production_region(&crashlog_path);

        // ── Vacuity guard FIRST, in the census's own style: it proves the
        // scanner's EYESIGHT by requiring a token it knows is there, not by
        // counting lines. `pub fn write_crash_log_from_info` is the module's
        // production entry point, so finding it proves we opened the right file
        // AND that the region survived the `#[cfg(test)]` boundary; a region
        // that was empty, truncated at a stray `#[cfg(test)]` in prose, or
        // pointed at some other file would report "no resolvers found" forever.
        let seen = region
            .iter()
            .filter(|l| l.text.contains("pub fn write_crash_log_from_info"))
            .count();
        assert_eq!(
            seen,
            1,
            "the crash-log invariant is BLIND: the production region of `{CRASHLOG}` \
             ({} surviving lines, last at line {}) holds {seen} copy(ies) of \
             `pub fn write_crash_log_from_info`, expected exactly 1 — either the root \
             resolved somewhere else ({}), the file lost its hook adapter, or the region \
             was cut short, and the check below would pass having read nothing",
            region.len(),
            region.last().map_or(0, |l| l.line),
            crashlog_path.display(),
        );

        let resolvers: Vec<String> = region
            .iter()
            .filter(|l| l.text.contains("dirs::data_dir()"))
            .map(|l| format!("{CRASHLOG}:{}", l.line))
            .collect();
        assert!(
            resolvers.is_empty(),
            "Invariant D (the crash-log module resolves nothing): `{CRASHLOG}` resolves a \
             data dir in its production region at {resolvers:?} — a resolver inside the \
             panic path means the hook slot decides where the user's crash logs go, so a \
             test panic on ANY thread writes into the real data dir (the 862-file bug); \
             the directory must come from the caller — `install_panic_hook(tag, dir)`",
        );
    }

    /// Invariant E — only `childproc` may append a raw command line to a child.
    ///
    /// **Why this invariant exists at all.** The `raw_arg` seam is technically
    /// reachable from `exec/bash.rs` without breaking invariants A, B or C:
    /// `raw_arg` APPENDS an argument, so it neither constructs a `Command` (A)
    /// nor sets a creation flag (B) nor touches stdin (C). Without E the next
    /// contributor adds `raw_arg` at a call site, and the
    /// `use std::os::windows::process::CommandExt` that brings it in is the same
    /// one-line `use` that makes a stray `creation_flags(…)` compile there —
    /// silently un-hiding every child and re-opening a console window per
    /// command, which is the bug ADR 0060 exists to prevent. E therefore keeps
    /// the trait's CALL FORMS in one file.
    ///
    /// **What it does NOT prove** (the census header demands this of every
    /// invariant): it is a LINT on text. It does not prove `cmd.exe` received
    /// the right bytes — nothing runnable can (`std` renders the command line
    /// inside `CreateProcess` and has no getter, rust-lang/rust#96794); that
    /// half is the two `#[cfg(windows)]` tests in
    /// `agent/tools/exec/tests/bash.rs`, which run only on CI's `windows-latest`
    /// leg.
    ///
    /// **Inherited blind spots**, both stated rather than implied:
    /// * This matches TEXT, so a string literal containing `.raw_arg(` also
    ///   trips it. That error goes the safe way — a false alarm costs one
    ///   rename, a false pass costs a window per command on someone's desktop.
    /// * [`is_comment`]'s `*`-prefix rule still drops any line whose trimmed
    ///   text starts with `*`, so a `.raw_arg(` written as the target of a
    ///   deref-assignment on its own line would slip through.
    /// * E inherits the census's SCOPE rule: [`production_region`] returns an
    ///   empty `Vec` for `**/tests.rs` and anything under `/tests/`, so test
    ///   fixtures are outside E exactly as they are outside A and B — a fixture
    ///   may build a raw `Command` or append raw text on purpose, and one that
    ///   wants the policy opts in explicitly. E guards production code, not the
    ///   outside view.
    #[test]
    fn production_code_never_appends_a_raw_command_line_itself() {
        // TWO roots (see `source_roots`): invariant E asserts an ABSENCE, so a
        // scan of one package alone would certify the other half unseen.
        let roots = source_roots();
        let regions = scan_all(&roots);

        // ── Vacuity guard FIRST, in the census's own style: a scanner that sees
        // nothing must fail loudly, not report a clean bill. Exactly ONE
        // production caller of [`append_cmd_c`] is expected, and it is
        // `exec/bash.rs` (`crate::childproc::append_cmd_c(` contains the
        // needle). This does double duty: it proves the scanner can SEE the API
        // it polices, and it pins that the `bash` child's command line still
        // goes through its owner. `SELF` is excluded and would score 0 anyway —
        // inside `childproc` the tail is reached as a bare `cmd_c_tail(`, and
        // the definition line reads `pub fn append_cmd_c(`, which does NOT
        // contain `childproc::append_cmd_c(`. The `1` is meant to be brittle: a
        // second site wanting a `cmd.exe` tail must have this conversation, not
        // quietly widen the surface. Comment lines that mention the seam are
        // dropped by [`is_comment`] before any match, so prose cannot inflate
        // the count.
        let caller_count = regions
            .iter()
            .filter(|(rel, _)| rel != SELF)
            .map(|(_, region)| {
                region
                    .iter()
                    .filter(|l| l.text.contains("childproc::append_cmd_c("))
                    .count()
            })
            .sum::<usize>();
        assert_eq!(
            caller_count, 1,
            "Invariant E is BLIND: {} production line(s) outside `{SELF}` call \
             `childproc::append_cmd_c(`, expected exactly 1 (`exec/bash.rs`, the Tool \
             child that owns a `cmd.exe` tail) — at 0 the scanner cannot see the API it \
             polices (a drifted match string would make the check below vacuous), and \
             above 1 something else has acquired a raw `cmd.exe` command line without \
             this conversation",
            caller_count,
        );

        // ── The invariant itself, anchored on the CALL form exactly as
        // invariant B anchors `.creation_flags(` (so prose naming `raw_arg`
        // cannot false-positive; [`is_comment`] filters those anyway).
        let mut violations: Vec<String> = Vec::new();
        for (rel, region) in &regions {
            if rel == SELF {
                continue;
            }
            for line in region.iter().filter(|l| l.text.contains(".raw_arg(")) {
                violations.push(format!("{rel}:{}", line.line));
            }
        }
        assert!(
            violations.is_empty(),
            "Invariant E (one owner of the raw command line): production code calls \
             `.raw_arg(` outside `{SELF}` at {violations:?} — `raw_arg` sits on the SAME \
             `std::os::windows::process::CommandExt` trait as `creation_flags`, so a call \
             site that reaches one is one `use` away from the other, and `creation_flags` \
             REPLACES rather than ORs (rust-lang/rust#96794): the stray call silently \
             un-hides every child and re-opens a console window per command (ADR 0060). \
             Append the tail through `childproc::append_cmd_c` instead.",
        );
    }
}

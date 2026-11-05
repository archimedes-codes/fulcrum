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
    ///
    /// S8b.4 MOVED this (and `is_comment` / `production_region`) into
    /// [`crate::lint_scan`], because the source-text lints that arrived with S8b.4
    /// need the same scanner and a second definition of "read the production
    /// region of a file" inside one crate is the drift the helper exists to
    /// prevent. These three `use` lines are the whole of the census's side of that
    /// move; the scanner itself is unchanged.
    use crate::lint_scan::{production_region, SourceLine};

    /// This package's manifest directory — the one anchor the census reads, and
    /// the ONLY one. Every other path is derived by READING a manifest or from
    /// `current_exe()`, never by counting `.parent()`s.
    fn manifest_dir() -> &'static std::path::Path {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
    }

    /// Which checkout the census is running in. S8b.2.
    ///
    /// # Why this is a DISCRIMINATOR and not a fallback
    ///
    /// `env!("CARGO_MANIFEST_DIR")` is `<repo>/crates/fulcrum-core` while the
    /// crate is a workspace member and `<the copy>` when it stands alone — so
    /// its DEPTH differs between the two layouts, and the pre-S8b.2 resolver
    /// (`.parent().parent()`) silently named the user's home directory in a
    /// standalone checkout. The fix cannot be "use the grandparent when it looks
    /// like a workspace and the manifest dir when it does not": a census that
    /// shrinks its own corpus to whatever resolves is the fail-open
    /// [`source_roots`] exists to prevent, and every "zero violations" claim
    /// below would then be true of half the code. So a layout is a CLAIM with a
    /// witness: `WorkspaceMember` demands the desktop root exists (loud panic if
    /// not), and `SinglePackage` demands no desktop tree sits beside the build
    /// output (loud panic if one does). Neither branch can quietly be blind.
    #[derive(Debug)]
    enum Layout {
        /// An ancestor manifest declares a `[workspace]` that names this
        /// package. The desktop package's `src/` is policed from
        /// `workspace_root/src-tauri/src`, and it MUST resolve.
        WorkspaceMember { workspace_root: std::path::PathBuf },
        /// No ancestor manifest claims this package: a single-package checkout
        /// (the T8b condition — this crate copied out on its own). One root.
        SinglePackage,
    }

    impl Layout {
        /// Whether the desktop package's tree is in this census's scope.
        fn polices_the_desktop(&self) -> bool {
            matches!(self, Layout::WorkspaceMember { .. })
        }
    }

    /// `CARGO_MANIFEST_DIR` relative to `ancestor`, `/`-normalised (so a Windows
    /// `\` cannot defeat a `starts_with` against a manifest's `/`-written member).
    fn relative_to_manifest(ancestor: &std::path::Path) -> Option<String> {
        let rest = manifest_dir().strip_prefix(ancestor).ok()?;
        Some(rest.to_string_lossy().replace('\\', "/"))
    }

    /// Every double-quoted string in a manifest. A deliberately dumb scanner: it
    /// does not parse TOML, it asks one question of it — "does this manifest name
    /// my directory?" — and a manifest that fools it fails LOUD (see
    /// [`declaring_workspace_root`]'s assert), never silently.
    fn quoted_strings(text: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find('"') {
            let after = &rest[open + 1..];
            match after.find('"') {
                Some(close) => {
                    out.push(after[..close].to_string());
                    rest = &after[close + 1..];
                }
                None => break,
            }
        }
        out
    }

    /// Whether the text declares a `[workspace]` table (`[workspace]` itself or
    /// any `[workspace.…]` sub-table, which cargo treats as declaring it too).
    fn declares_workspace_table(text: &str) -> bool {
        text.lines()
            .map(str::trim)
            .any(|line| line == "[workspace]" || line.starts_with("[workspace."))
    }

    /// Whether one `members` entry names this package: a literal path
    /// (`crates/fulcrum-core`), or a glob (`crates/*` — matched on its prefix,
    /// because a glob workspace is the one form that does not spell the name).
    fn member_entry_names_us(entry: &str, rel: &str, package: &str) -> bool {
        if let Some((prefix, _)) = entry.split_once('*') {
            let prefix = prefix.trim_end_matches('/');
            return if prefix.is_empty() {
                true
            } else {
                rel.starts_with(&format!("{prefix}/"))
            };
        }
        rel == entry || package == entry
    }

    /// The nearest ANCESTOR `Cargo.toml` that declares a `[workspace]` naming
    /// this package — the workspace root, WITHOUT counting levels.
    ///
    /// `None` means no manifest anywhere above `CARGO_MANIFEST_DIR` claims us,
    /// which is what a genuine single-package checkout looks like. An ancestor
    /// that declares a workspace and does NOT name us is a LOUD panic, not a
    /// `None`: guessing here is exactly how a census ends up policing less than
    /// it claims. (A `members` entry naming us only via a glob is handled; a
    /// workspace that claims us through `exclude` while pointing `package.workspace`
    /// elsewhere is not — it panics, which is the safe direction.)
    fn declaring_workspace_root() -> Option<std::path::PathBuf> {
        let package = env!("CARGO_PKG_NAME");
        let mut dir = manifest_dir().parent()?;
        loop {
            let candidate = dir.join("Cargo.toml");
            if let Ok(text) = std::fs::read_to_string(&candidate) {
                if declares_workspace_table(&text) {
                    let rel = relative_to_manifest(dir);
                    let names_us = rel.is_some_and(|rel| {
                        quoted_strings(&text)
                            .iter()
                            .any(|entry| member_entry_names_us(entry, &rel, package))
                    });
                    assert!(
                        names_us,
                        "`{}` declares a `[workspace]` but does not name `{}` (nor its \
                         path {:?}) among its members — the census cannot tell whether it \
                         is inside a two-package checkout, and it refuses to guess: either \
                         name the member, or move this package out of that workspace.",
                        candidate.display(),
                        package,
                        manifest_dir(),
                    );
                    return Some(dir.to_path_buf());
                }
            }
            dir = dir.parent()?;
        }
    }

    /// The layout, as measured — never as assumed.
    fn detect_layout() -> Layout {
        match declaring_workspace_root() {
            Some(workspace_root) => Layout::WorkspaceMember { workspace_root },
            None => Layout::SinglePackage,
        }
    }

    /// The second witness of the layout: is there a DESKTOP TREE anywhere above
    /// this package — asked of the FILESYSTEM, with no manifest parsed and no
    /// `[workspace]` consulted?
    ///
    /// `Some(dir)` means `<dir>/src-tauri/src` exists for some ancestor of
    /// `CARGO_MANIFEST_DIR`. That is the shape of a two-package checkout, and it
    /// is the ONLY thing that can catch a broken [`declaring_workspace_root`]:
    /// every desktop-specific floor is conditional on the layout, so if the
    /// discriminator wrongly answers `SinglePackage`, those floors do not fail —
    /// they VANISH, and the census certifies a corpus it never opened. The
    /// `SinglePackage` branch therefore demands this return `None`.
    ///
    /// It is deliberately not derived from `current_exe()`/`target/`: a relocated
    /// `--target-dir` would make a build-output-derived witness answer "no tree
    /// anywhere", which is the silent-no answer this must never be.
    fn desktop_tree_above() -> Option<std::path::PathBuf> {
        let mut dir = Some(manifest_dir());
        while let Some(current) = dir {
            if current.join("src-tauri").join("src").is_dir() {
                return Some(current.to_path_buf());
            }
            dir = current.parent();
        }
        None
    }

    /// The shared cargo `target/` dir — a one-line ALIAS for
    /// `test_support::target_debug_dir`, which since S8b.1 resolves it from
    /// `current_exe()` rather than by counting manifest levels (see that helper for
    /// why: a standalone `cargo test` in this package builds into
    /// `<package>/target/`, so the grandparent names a directory no build writes).
    ///
    /// T5/S3 MOVED THE BODY HERE and left this alias. The reason is Step A-bis's
    /// own instruction — "fix once, not eight times" — and S3 is where it stops
    /// being hypothetical: the first consumers arrived (`mcp/stdio.rs`,
    /// `mcp/tool.rs`, `mcp/manager.rs`, four `fake_mcp_stdio` spawners). A second
    /// definition of "where cargo builds" inside the same crate is exactly the
    /// drift this helper exists to prevent, so the census keeps its local
    /// `workspace_root()` (it needs the ROOT, to walk two `src/` trees — and that
    /// two-package assumption is what S8b.2 must teach to detect a single-package
    /// checkout) and defers the `target/debug` half to the shared one.
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
    /// If the resolver drifts a level in either direction it stops equaling the
    /// ground truth and this reddens. Since S8.3 the crate CAN call
    /// `worker/client.rs::fixture_path()` directly (same package, both
    /// `#[cfg(test)]`) and does — in that module's own equality pin — so this pin
    /// is the resolver-level half of a pair rather than the only witness.
    ///
    /// # Verified non-vacuous
    ///
    /// S8b.1 re-ran the mutation on the resolver's CURRENT body (the helper moved
    /// to `test_support` long ago, so the original wording here named
    /// `workspace_root()`, which this pin does not call — a stale claim, corrected
    /// rather than left). Dropping one `.parent()` from
    /// `test_support::target_debug_dir`'s body reddens THIS pin and three others in
    /// three other files; each of the two shape asserts below was additionally
    /// tripped on its own with the equality disabled, so neither hides behind the
    /// other. That is the T4 lesson applied — a pin that cannot go red is not a
    /// pin, and the claim needs a mutation behind it, not a sentence.
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
             {}.",
            env!("CARGO_MANIFEST_DIR"),
        );

        // ⚠️ **WHY THIS EQUALITY STILL HAS TEETH AFTER S8b.1.** The resolver's
        // BODY moved to `current_exe()` in S8b.1 (see
        // `test_support::target_debug_dir` for why: a standalone `cargo test` in
        // this package builds into `<package>/target/`, so the manifest's
        // grandparent names a directory no build ever writes, and 66 fixture
        // spawns stayed ENOENT until the ROOT was fixed — not just the bin names).
        // That COULD have turned this pin into an identity — two copies of one
        // expression cannot disagree — and it did not, because `ground_truth`
        // above is this pin's OWN `current_exe()` walk, written here and not
        // borrowed from the helper it checks. Vacuity is a claim about mutations,
        // so it was tested rather than asserted: **dropping one `.parent()` from
        // the resolver's body reddens this pin** (measured: it reports
        // `target/debug/deps` against the real `target/debug`), and the same
        // mutation separately reddens `worker/client.rs`,
        // `worker/manager/tests/mod.rs` and `test_support`'s own exe-suffix pin —
        // four witnesses in four files.
        //
        // The two shape asserts below are a SECOND, INDEPENDENT kind of teeth, and
        // they were each tripped on their own (with the equality disabled, so
        // neither one hides behind the other): assert (1) fires on a path named
        // `deps`, assert (2) on one named `target`. They also survive what the
        // equality's own arithmetic cannot promise — they check the resolved NAME,
        // so they hold for a workspace member, a standalone package, a relocated
        // `--target-dir`, and a cross-compiled `target/<triple>/debug`.

        // (1) ONE LEVEL TOO FEW: dropping a `.parent()` leaves
        // `target/debug/deps`, whose file name is `deps`, not `debug`.
        assert_eq!(
            claimed.file_name().and_then(|s| s.to_str()),
            Some("debug"),
            "`target_debug_dir()` must name the `debug` profile directory, not a \
             level inside it or above it — resolved {claimed:?}. A dropped \
             `.parent()` lands on `deps` (where the TEST binaries live, so every \
             fixture spawn would exec a `deps/<name>` that cargo never writes)."
        );
        //
        // (2) ONE LEVEL TOO MANY: adding a `.parent()` lands on `target`, which
        // has no `deps/` child. Together (1) and (2) bracket the level count in
        // BOTH directions without naming a level, which is what lets them survive
        // the layout change instead of encoding it.
        assert!(
            claimed.join("deps").is_dir(),
            "`{claimed:?}` must be the PARENT of `deps/` — the directory cargo \
             built this very test binary's siblings into. A level too high names \
             `target/`, which has no `deps` child."
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

    /// EVERY `src/` tree the census is entitled to police, given the layout it
    /// measured. S8b.2.
    ///
    /// THE REASON THIS IS TWO ROOTS AND NOT ONE: the census asserts ABSENCES
    /// ("no `Command::new(` outside `childproc`", "no `.creation_flags(` outside
    /// `childproc`", "no `.raw_arg(` outside `childproc`"). Before S2 one walk of
    /// `src-tauri/src` saw every file those invariants police. After S2 the
    /// policed code sits in TWO packages, and a scan of either one alone reports a
    /// clean bill of health for the half it never opened — the fail-OPEN shape
    /// this repo keeps recording (the same class as the 18c census scan roots and
    /// the five-edge scan). So: walk both, whenever both exist.
    ///
    /// # Why dropping the desktop root in a standalone checkout is NOT that fail-open
    ///
    /// A root is dropped ONLY when the LAYOUT says the tree is not there, and the
    /// layout is measured twice, independently:
    ///
    ///   1. `cargo`'s own manifest graph — an ancestor `Cargo.toml` declaring a
    ///      `[workspace]` that names this package ([`declaring_workspace_root`]).
    ///   2. `cargo`'s own build output — the directory that owns the `target/`
    ///      this very test binary was built into ([`build_output_owner`], derived
    ///      from `current_exe()`, which shares NO expression with (1)).
    ///
    /// `WorkspaceMember` asserts the desktop root EXISTS (a missing one is a
    /// resolution bug and panics, naming the resolution). `SinglePackage` asserts
    /// no `src-tauri/src` sits beside the build output — so a census that wrongly
    /// concluded "single package" while actually standing in the two-package repo
    /// PANICS instead of quietly policing half the tree. That second assert is the
    /// only thing that can catch a broken discriminator, because every
    /// desktop-specific floor is conditional on the layout and would simply
    /// vanish with it. It is measured, not asserted: mutation 1 of S8b.2 forces
    /// the single-package branch inside the workspace and this is what reddens.
    ///
    /// The workspace root itself is deliberately NOT a walk root: it holds
    /// `target/`, whose `build/` trees carry generated `*.rs` (and tens of GB).
    /// Walking it would both hang and invent violations in generated code. Named
    /// `src/` dirs is the exact scope.
    fn source_roots() -> Vec<SourceRoot> {
        let layout = detect_layout();
        let mut roots = vec![SourceRoot {
            // The crate's own tree. The label is a CONSTANT, not a derived path,
            // so it survives the layout change: `SELF` and every labelled entry
            // below resolve against this root in BOTH layouts.
            label: "fulcrum-core/",
            path: manifest_dir().join("src"),
        }];
        if let Layout::WorkspaceMember { workspace_root } = &layout {
            roots.push(SourceRoot {
                label: "archimedes/",
                path: workspace_root.join("src-tauri").join("src"),
            });
        } else {
            // The filesystem witness. A census that calls this layout
            // `SinglePackage` while an ancestor directory still holds the
            // desktop's `src/` would drop that root and report a clean bill of
            // health for code it never opened — so the claim gets checked
            // against the tree around it, which no manifest parsing can fake.
            // This is the ONLY guard against a broken discriminator: the
            // desktop-specific floors are conditional on the layout and would
            // disappear with it rather than fail.
            let strays = desktop_tree_above();
            assert!(
                strays.is_none(),
                "the census concluded this is a SINGLE-PACKAGE checkout (no ancestor \
                 `Cargo.toml` declares a `[workspace]` naming `{}`), but an ancestor \
                 directory — {:?} — holds a desktop tree at `src-tauri/src`. Two-package \
                 layout with an undetected workspace: dropping the desktop root here would \
                 silently un-police it, so this is a resolution bug and not a layout \
                 (CARGO_MANIFEST_DIR={}).",
                env!("CARGO_PKG_NAME"),
                strays,
                env!("CARGO_MANIFEST_DIR"),
            );
        }
        for r in &roots {
            // Loud, not fail-open: a root that does not resolve would empty its
            // half of the corpus, and every "zero violations" below would then be
            // true of nothing. The message names the root AND the layout that
            // produced it, per the guard's own convention.
            assert!(
                r.path.is_dir(),
                "the census resolved the wrong root: {} is not a directory \
                 (layout {:?}, CARGO_MANIFEST_DIR={})",
                r.path.display(),
                layout,
                env!("CARGO_MANIFEST_DIR"),
            );
        }
        roots
    }

    /// What the census actually SAW inside ONE root. S8b.2 makes the vacuity
    /// floors PER ROOT for the reason measured at `$BASE`: the aggregate floor
    /// (`>= 8` calls, `>= 4` files, `>= 1` `command_std`) is satisfied by the
    /// crate root ALONE in shape — 8 calls across 6 files — so an aggregate-only
    /// guard cannot tell "both trees walked" from "the desktop half never
    /// opened". Each root therefore reports its own eyesight.
    struct RootTally {
        label: &'static str,
        /// Files the walk opened (any `.rs`, whether or not it holds a call).
        files_scanned: usize,
        /// `childproc::command(` + `childproc::command_std(` lines.
        calls: usize,
        /// Distinct files holding at least one of them.
        call_files: usize,
        /// The `command_std(` subset.
        std_calls: usize,
        /// `childproc::append_cmd_c(` lines outside [`SELF`].
        append_callers: usize,
    }

    impl RootTally {
        fn of(label: &'static str, regions: &[(String, Vec<SourceLine>)]) -> RootTally {
            let mine = regions.iter().filter(|(rel, _)| rel.starts_with(label));
            let mut t = RootTally {
                label,
                files_scanned: 0,
                calls: 0,
                call_files: 0,
                std_calls: 0,
                append_callers: 0,
            };
            for (rel, region) in mine {
                t.files_scanned += 1;
                let calls = region
                    .iter()
                    .filter(|l| l.text.contains("childproc::command("))
                    .count();
                // `childproc::command(` is not a substring of
                // `childproc::command_std(` (the `(` differs), so the two
                // tallies do not double-count.
                let std = region
                    .iter()
                    .filter(|l| l.text.contains("childproc::command_std("))
                    .count();
                if calls + std > 0 {
                    t.calls += calls + std;
                    t.std_calls += std;
                    t.call_files += 1;
                }
                if rel != SELF {
                    t.append_callers += region
                        .iter()
                        .filter(|l| l.text.contains("childproc::append_cmd_c("))
                        .count();
                }
            }
            t
        }
    }

    /// The floor one root MUST clear, and the number that was measured there.
    /// Kept as a struct so both roots read through one code path and a floor
    /// cannot be quietly dropped for one of them.
    struct RootFloor {
        label: &'static str,
        files_scanned: usize,
        calls: usize,
        call_files: usize,
        std_calls: usize,
        append_callers: usize,
        measured: &'static str,
    }

    /// The floors, asserted per root, against the corpus the walk actually saw.
    ///
    /// None of these is headroom. Each sits at or just under what the root
    /// contributes TODAY (measured at `$BASE` 75550d0a — see each entry), so a
    /// root that goes blind, stops descending, or loses the call the census
    /// exists to watch reddens the pin instead of shrinking the claim. The
    /// `files_scanned` floor is the DISCRIMINATOR for a walk that stops
    /// descending: a flat `read_dir` of `crates/fulcrum-core/src` reads 36 files
    /// and finds ZERO constructor calls (every caller lives in a subtree), so
    /// the `calls` floor alone already catches it — the file floor is the second
    /// witness, and both are far above the shallow count.
    fn assert_root_floors(tally: &RootTally, floor: &RootFloor) {
        assert_eq!(
            tally.label, floor.label,
            "the census tallied `{}` against `{}`'s floor — a root and its floor \
             must be paired by name or a floor can be swapped onto the wrong half",
            tally.label, floor.label
        );
        let blind = tally.files_scanned < floor.files_scanned
            || tally.calls < floor.calls
            || tally.call_files < floor.call_files
            || tally.std_calls < floor.std_calls
            || tally.append_callers < floor.append_callers;
        if blind {
            panic!(
                "the census is BLIND in `{}`: it found {} `childproc::command*` calls in \
                 {} distinct file(s) ({} of them `command_std`), {} \
                 `childproc::append_cmd_c(` caller(s) outside `{SELF}`, across {} scanned \
                 file(s) — the floor is >= {} calls in >= {} file(s) with >= {} \
                 `command_std` and >= {} `append_cmd_c` caller(s) over >= {} file(s) \
                 (measured at `$BASE`: {}). At or below the floor either the root \
                 resolved somewhere else, the walk stopped descending, or the match \
                 strings drifted from the API — and every ABSENCE invariant in this census \
                 would then be true of nothing.",
                tally.label,
                tally.calls,
                tally.call_files,
                tally.std_calls,
                tally.append_callers,
                tally.files_scanned,
                floor.calls,
                floor.call_files,
                floor.std_calls,
                floor.append_callers,
                floor.files_scanned,
                floor.measured,
            );
        }
    }

    /// `crates/fulcrum-core/src` — measured at `$BASE`: **8 constructor calls
    /// across 6 files** (`interactive/sudo_exec.rs` 1, `mcp/stdio.rs` 1,
    /// `provider_auth/key_helper.rs` 1, `tools/exec/bash.rs` 2,
    /// `tools/exec/search.rs` 2, `worker/client.rs` 1), **0 `command_std`**, **1
    /// `append_cmd_c` caller** (`tools/exec/bash.rs:161`), **148 `.rs` files**.
    ///
    /// `std_calls: 0` is a MEASURED fact and is deliberately NOT raised to 1: no
    /// crate production site calls `command_std` today, and inventing a floor
    /// above the measurement would be a pin that fails on a correct tree. The
    /// desktop root's floor is where `command_std` is watched.
    fn crate_floor() -> RootFloor {
        RootFloor {
            label: "fulcrum-core/",
            files_scanned: 40,
            calls: 8,
            call_files: 4,
            std_calls: 0,
            append_callers: 1,
            measured: "8 calls / 6 files / 0 std / 1 append caller / 148 files",
        }
    }

    /// `src-tauri/src`, POLICIED ONLY in the workspace-member layout — measured
    /// at `$BASE`: **1 `command_std` call in 1 file**
    /// (`commands/update.rs:608`, the Linux relaunch path, verified NOT inside
    /// `#[cfg(test)]`; its other `Command::new` at `:1216` is test code), **0**
    /// `append_cmd_c` callers, **52 `.rs` files**.
    ///
    /// This floor is the reason the desktop root is worth walking at all: it is
    /// ONE PRODUCTION call, and it reaches the crate's API through
    /// `src-tauri/src/agent/mod.rs`'s re-export shell, so the match string still
    /// names the crate's own API. It is NOT conditional on anything but the
    /// layout — a workspace-member census that loses this root fails HERE, which
    /// is the fail-open S8b.2 exists to close.
    fn desktop_floor() -> RootFloor {
        RootFloor {
            label: "archimedes/",
            calls: 1,
            call_files: 1,
            std_calls: 1,
            append_callers: 0,
            files_scanned: 20,
            measured: "1 call / 1 file / 1 std / 0 append callers / 52 files",
        }
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

    // `is_comment` and `production_region` now live in [`crate::lint_scan`] —
    // S8b.4 moved them there so the source-text lints share this scanner instead
    // of duplicating it. `production_region` is imported at the top of this
    // module; `is_comment` is not, because nothing in here calls it directly
    // anymore (the region scanner does, inside `lint_scan`), and an unused import
    // is a clippy error under `-D warnings`. These two lines are plain comments
    // rather than doc comments: a doc comment here documents NOTHING (the next
    // item is `rust_files` below, which has its own), and clippy's
    // `empty_line_after_doc_comments` is exactly the guard that catches a doc
    // comment orphaned by a move like this one.

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
    ///   `CommandExt` comment; `is_comment` filters those anyway). A second
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
        // Roots are LAYOUT-dependent since S8b.2 (see `source_roots`), so the
        // vacuity floors below are PER ROOT: the pre-S8b.2 guard was AGGREGATE
        // across both roots, and the measured split at `$BASE` (crate 8 calls /
        // 6 files, desktop 1 call / 1 file) means the crate half alone satisfies
        // a `>= 8 calls / >= 4 files` shape — so an aggregate-only floor stays
        // GREEN while the desktop half is un-policed. That is the fail-open this
        // step exists to close, and it is why each root reports its own eyesight
        // rather than contributing to one shared sum.
        let roots = source_roots();
        let layout = detect_layout();
        let regions = scan_all(&roots);

        // ── Vacuity guard, FIRST, PER ROOT: a scanner that sees nothing must
        // fail loudly, not report a clean bill of health for an empty corpus (the
        // same trick `hostenv::tainted_patch` plays by asserting its patch is
        // non-empty). Deliberately NOT a bare "files scanned >= N" — the walk
        // visits 100+ files whether or not the CALLS are visible — so each root
        // is floored on the calls it must see as well as on how far it descended.
        assert_root_floors(&RootTally::of("fulcrum-core/", &regions), &crate_floor());
        if layout.polices_the_desktop() {
            assert_root_floors(&RootTally::of("archimedes/", &regions), &desktop_floor());
        }

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
        let mut runtime_children = vec![
            "fulcrum-core/mcp/stdio.rs",
            // T8/S8.3: moved into this crate — the label follows the file.
            "fulcrum-core/worker/client.rs",
            "fulcrum-core/interactive/sudo_exec.rs",
        ];
        // `commands/update.rs` is the DESKTOP's relaunch path, so it is in scope
        // only while the desktop tree is (see `source_roots`). Listing it
        // unconditionally would panic in a single-package checkout — and listing
        // it in a way that SKIPPED it when the label failed to resolve would be
        // the fail-open, which is why `region_of` panics on a missing file and
        // the desktop floors above gate the same root.
        if layout.polices_the_desktop() {
            runtime_children.push("archimedes/commands/update.rs");
        }
        for runtime_child in runtime_children {
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

    /// The CRATE-side half of the same invariant: the seam that installs the
    /// hook. `run_worker_inner` resolves the base dir at INSTALL time and hands
    /// it to the injected closure; the crate itself must never own the hook slot.
    const CRASH_HOOK: &str = "fulcrum-core/worker/mod.rs";

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
    /// **Why the region and not the file.** `is_comment` is what makes this
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
    /// **Inherited blind spot:** this is `is_comment` with all its limits, so
    /// the `*`-prefix rule silently drops any line whose trimmed text starts
    /// with `*` (see its docs) — a `dirs::data_dir()` written as the target of
    /// a deref-assignment on its own line would slip through. Name it, do not
    /// imply the region is exact.
    #[test]
    fn production_crashlog_never_resolves_a_data_dir() {
        let roots = source_roots();
        let layout = detect_layout();

        // ── THE CRATE-SIDE HALF, which runs in BOTH layouts. `crashlog.rs`
        // itself is desktop, so in a single-package checkout it is not merely
        // absent, it is OUT OF THE BUILT PACKAGE — and a pin whose only subject
        // file vanishes is a pin that reports "no resolvers" for a corpus it
        // never opened. So the invariant keeps a subject the crate always has:
        // the seam that resolves the base dir and hands it to the injected hook.
        // Two claims, both measured at `$BASE`:
        //
        //   * `dirs::data_dir()` appears EXACTLY ONCE in `run_worker_inner`'s
        //     production region, at install time, beside the hook it feeds — a
        //     second resolver would be a new one in a new place, which is the
        //     shape of the 862-file bug. The `1` is meant to be brittle.
        //   * the crate claims NO hook slot (`set_hook` / `take_hook`: zero).
        //     The crate resolves the DIRECTORY and passes it across; owning the
        //     slot is what puts a test thread's panic into the user's data dir.
        let hook_region = region_of_labeled(&roots, CRASH_HOOK);
        let installed = hook_region
            .iter()
            .filter(|l| l.text.contains("hook(base.join"))
            .count();
        let resolved = hook_region
            .iter()
            .filter(|l| l.text.contains("dirs::data_dir()"))
            .count();
        assert_eq!(
            (installed, resolved),
            (1, 1),
            "the crash-log invariant is BLIND on its crate-side half: the production \
             region of `{CRASH_HOOK}` ({} surviving lines) holds {installed} copy(ies) of \
             `hook(base.join` and {resolved} of `dirs::data_dir()`, expected (1, 1) — the \
             install-time seam the desktop's hook is fed through. At 0 the region was cut \
             short or the seam moved, and the hook-slot check below would pass having read \
             nothing.",
            hook_region.len(),
        );
        let slot_claims: Vec<String> = hook_region
            .iter()
            .filter(|l| l.text.contains("set_hook") || l.text.contains("take_hook"))
            .map(|l| format!("{CRASH_HOOK}:{}", l.line))
            .collect();
        assert!(
            slot_claims.is_empty(),
            "Invariant D, crate half: `{CRASH_HOOK}` claims the process-global panic hook \
             slot at {slot_claims:?} — the crate hands a DIRECTORY across the seam and the \
             desktop installs the hook. `std::panic` has ONE slot for the whole process, so \
             a hook owned here would decide where every thread's crash log lands, including \
             a test thread's in the real data dir (the 862-file bug).",
        );

        // ── THE DESKTOP-SIDE HALF, in the layout that still contains it.
        // Skipped ONLY when the desktop tree is out of the built package, and
        // `source_roots` has already PANICKED if that conclusion is wrong — so
        // this is a scope boundary with a witness, not a quiet no-op. The gap it
        // leaves after T8b (nothing in the new crate repo polices the desktop's
        // `update.rs` / `crashlog.rs`) is recorded in the roadmap status file for
        // S8b.4 to settle; it is NOT closed here by guessing at a mechanism.
        if !layout.polices_the_desktop() {
            return;
        }
        let crashlog_path = resolve_labeled(&roots, CRASHLOG);
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
    /// * `is_comment`'s `*`-prefix rule still drops any line whose trimmed
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
        // Roots are layout-dependent since S8b.2 (see `source_roots`): invariant
        // E asserts an ABSENCE, so a scan of one package alone would certify the
        // other half unseen. The per-root floors below are what stop that — the
        // `caller_count == 1` under this comment is an AGGREGATE and, measured at
        // `$BASE`, its single hit lives in the CRATE (`tools/exec/bash.rs:161`),
        // so on its own it stays green with the desktop root dropped.
        let roots = source_roots();
        let layout = detect_layout();
        let regions = scan_all(&roots);

        // ── Vacuity guard FIRST, PER ROOT (see `assert_root_floors`), then the
        // aggregate that pins this invariant's own API.
        assert_root_floors(&RootTally::of("fulcrum-core/", &regions), &crate_floor());
        if layout.polices_the_desktop() {
            assert_root_floors(&RootTally::of("archimedes/", &regions), &desktop_floor());
        }

        // Exactly ONE production caller of [`append_cmd_c`] is expected, and it is
        // `exec/bash.rs` (`crate::childproc::append_cmd_c(` contains the
        // needle). This does double duty: it proves the scanner can SEE the API
        // it polices, and it pins that the `bash` child's command line still
        // goes through its owner. `SELF` is excluded and would score 0 anyway —
        // inside `childproc` the tail is reached as a bare `cmd_c_tail(`, and
        // the definition line reads `pub fn append_cmd_c(`, which does NOT
        // contain `childproc::append_cmd_c(`. The `1` is meant to be brittle: a
        // second site wanting a `cmd.exe` tail must have this conversation, not
        // quietly widen the surface. Comment lines that mention the seam are
        // dropped by `is_comment` before any match, so prose cannot inflate
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
        // cannot false-positive; `is_comment` filters those anyway).
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

    /// S8b.2 — the two INDEPENDENT witnesses of the checkout layout must agree.
    ///
    /// Every other pin in this census reads the layout; this one reads it TWICE
    /// and compares, which is what makes a wrong answer LOUD instead of merely
    /// smaller:
    ///
    ///   * **Witness 1 — cargo's manifest graph.** An ancestor `Cargo.toml`
    ///     declaring a `[workspace]` that names this package
    ///     ([`declaring_workspace_root`]).
    ///   * **Witness 2 — the filesystem around the package.** Whether any ancestor
    ///     directory holds `src-tauri/src` ([`desktop_tree_above`]).
    ///
    /// They share no expression and no failure mode: one parses manifests, the
    /// other only stats directories. Disagreement is a panic in BOTH directions:
    ///
    ///   * witness 1 says member but witness 2 sees no desktop tree → the desktop
    ///     root would resolve to a directory that does not exist (the pre-S8b.2
    ///     bug: 4 red pins in a standalone copy);
    ///   * witness 1 says single package but witness 2 sees one → the census would
    ///     DROP that root and certify it unseen. That is the fail-open, and it is
    ///     what mutation 1 of S8b.2 reddens.
    #[test]
    fn the_two_layout_witnesses_agree() {
        let layout = detect_layout();
        let seen = desktop_tree_above();

        match &layout {
            Layout::WorkspaceMember { workspace_root } => {
                let expected = workspace_root.join("src-tauri").join("src");
                assert!(
                    expected.is_dir(),
                    "the census's witnesses DISAGREE: the manifest graph says this package \
                     is a workspace MEMBER (root {workspace_root:?}), but the desktop root \
                     it implies — {} — is not a directory (witness 2 saw {seen:?}; \
                     CARGO_MANIFEST_DIR={}). The desktop half of the census would walk a \
                     path that does not exist.",
                    expected.display(),
                    env!("CARGO_MANIFEST_DIR"),
                );
                assert_eq!(
                    seen.as_deref(),
                    Some(workspace_root.as_path()),
                    "the census's witnesses DISAGREE about WHICH directory holds the \
                     desktop: the manifest graph names {workspace_root:?} as the workspace \
                     root, the nearest ancestor holding `src-tauri/src` is {seen:?}. \
                     Policing one checkout from inside another would certify files that are \
                     not part of this build (CARGO_MANIFEST_DIR={}).",
                    env!("CARGO_MANIFEST_DIR"),
                );
            }
            Layout::SinglePackage => assert!(
                seen.is_none(),
                "the census concluded this is a SINGLE-PACKAGE checkout, but an ancestor \
                 directory {seen:?} holds `src-tauri/src`. Dropping the desktop root on \
                 that conclusion would certify a corpus this scan never opened — the \
                 fail-open `source_roots` documents (CARGO_MANIFEST_DIR={}).",
                env!("CARGO_MANIFEST_DIR"),
            ),
        }

        // ── The pin's own arming: the two witnesses must be capable of
        // DISAGREEING, i.e. witness 2 must be able to answer "no tree" at all.
        // `desktop_tree_above` is an ancestor walk that returns the FIRST hit;
        // if it always returned `Some` (a `join` that matched the manifest dir
        // itself, say) then the `SinglePackage` arm could never fire and this
        // whole comparison would be an identity. So: the answer it gives must be
        // a directory that is NOT inside this package, and in the layout where it
        // answers `Some`, that directory must be an ANCESTOR of the manifest dir
        // rather than the manifest dir itself.
        if let Some(dir) = &seen {
            assert!(
                manifest_dir().starts_with(dir) && dir != manifest_dir(),
                "witness 2 answered {dir:?} as the directory holding `src-tauri/src`, which \
                 is not a STRICT ancestor of `CARGO_MANIFEST_DIR={}` — the walk is matching \
                 something other than what it claims, so a `Some` answer proves nothing and \
                 the agreement asserts above are vacuous.",
                env!("CARGO_MANIFEST_DIR"),
            );
        }
    }
}

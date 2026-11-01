//! `!command` key helpers (issue #129): an `api_key` starting with `!` names a
//! helper PROGRAM whose stdout (trimmed) is the bearer token. NO shell, NO
//! metacharacters, fail-closed (never dispatch through an interpreter). Lives
//! in `agent/` so it may use `crate::agent::childproc` — the crate's ONE
//! sanctioned `tokio::process::Command` constructor (the `CREATE_NO_WINDOW`
//! policy, ADR 0060 + the childproc census).
//!
//! The execution contract, in order (every step fail-closed):
//!
//! 1. strip the leading `!`; an empty/whitespace-only remainder is `Err`;
//! 2. scan the WHOLE REMAINDER for a shell metacharacter BEFORE any split — a
//!    value that merely LOOKS like a shell command is refused outright;
//! 3. split on whitespace into argv (v1 contract: **no quoting processing** — an
//!    argument that must contain a space is unsupported by design, because
//!    adding quote handling is how a parser starts disagreeing with a shell);
//! 4. reject the executed program (the FIRST argv token) when it targets
//!    `.cmd`/`.bat` or names a shell dispatcher (`cmd`, `sh`, `powershell`, …);
//! 5. spawn with NO shell, closed stdin, piped stdout/stderr, `kill_on_drop`;
//! 6. run under [`HELPER_TIMEOUT`];
//! 7. decode stdout (bounded, UTF-8, trimmed) — a non-zero exit, an over-long,
//!    non-UTF-8 or empty output is `Err`.
//!
//! Two deliberate scopes, both pinned by tests:
//!
//! * The metacharacter rejection (step 2) is NOT an injection defence — with no
//!   shell there is no injection vector in the first place. It is the spec's
//!   fail-closed stance: a credential field that reads like a shell pipeline is
//!   a misconfiguration, and refusing it is cheaper than reasoning about it.
//! * Program rejection (step 4) covers the FIRST argv token ONLY, because that
//!   is the only token the spawn executes. `.cmd`/`cmd` appearing in ARGUMENT
//!   position is data handed to a harmless program (`!echo helper.cmd`), and
//!   rejecting it would refuse legitimate helpers without closing any hole — a
//!   helper that could dispatch a string would have to name the dispatcher
//!   first, which step 4 does refuse.

use std::time::Duration;

/// The helper must finish within 10 s (the spec's timeout).
pub const HELPER_TIMEOUT: Duration = Duration::from_secs(10);
/// A token generator is small; a runaway helper's output is capped.
pub const MAX_HELPER_OUTPUT_BYTES: usize = 64 * 1024;
/// How much of a failed helper's stderr is echoed into the error (a diagnostic
/// hint, never a payload).
const MAX_STDERR_DIAGNOSTIC_BYTES: usize = 4 * 1024;
/// The shell metacharacters rejected ANYWHERE in the command line (fail-closed;
/// listed verbatim in the spec): `; & | < > ` ` $ ( ) { }`.
pub const REJECTED_METACHARACTERS: &[char] =
    &[';', '&', '|', '<', '>', '`', '$', '(', ')', '{', '}'];
/// Programs that dispatch a STRING through an interpreter — a `!cmd /c …` argv
/// is itself a shell invocation, so fail-closed covers the name, not just the
/// file extension.
pub const REJECTED_INTERPRETERS: &[&str] = &[
    "cmd",
    "cmd.exe",
    "powershell",
    "pwsh",
    "wsl",
    "bash",
    "sh",
    "zsh",
    "dash",
    "fish",
    "ksh",
    "csh",
];

/// Run the helper named by the `!`-prefixed `api_key` (the leading `!` is
/// stripped here). `Err` on any rejection, failure, timeout or empty output —
/// NEVER on a successful output. The caller (`resolve_bearer`) maps `Err` to
/// `None` + a `debuglog` line (the interface: resolution never errors at the
/// boundary).
pub async fn run(api_key: &str) -> Result<String, String> {
    run_with_timeout(api_key, HELPER_TIMEOUT).await
}

/// The deadline-taking core (so the timeout path is testable against a
/// one-second budget instead of the production ten — same code path, 10x
/// cheaper). Production only ever sees [`HELPER_TIMEOUT`].
async fn run_with_timeout(api_key: &str, timeout: Duration) -> Result<String, String> {
    let remainder = api_key.strip_prefix('!').unwrap_or(api_key);

    // Step 1 — an empty (or whitespace-only) remainder names no program.
    if remainder.trim().is_empty() {
        return Err("key helper rejected: empty command after the `!` prefix".to_string());
    }

    // Step 2 — the WHOLE-remainder metacharacter scan, BEFORE any split, so a
    // metacharacter glued to the program name (`!touch<marker>`) is caught here
    // and never reaches argv at all.
    if let Some(c) = remainder
        .chars()
        .find(|c| REJECTED_METACHARACTERS.contains(c))
    {
        return Err(format!(
            "key helper rejected: shell metacharacter '{c}' in command"
        ));
    }

    // Step 3 — the whitespace argv contract (no quoting; module doc).
    let tokens = split_argv(remainder);
    let Some(program) = tokens.first() else {
        // Unreachable after step 1, but the type cannot see that.
        return Err("key helper rejected: empty command after the `!` prefix".to_string());
    };

    // Step 4 — the executed program only (see the module doc's second scope).
    reject_program(program)?;

    // Step 5 — spawn, NO shell. `childproc::command` owns CREATE_NO_WINDOW
    // (ADR 0060); `closed_stdin` means a stdin-reading helper (`cat`) sees EOF
    // instead of blocking until the deadline. No `current_dir`, no env patch:
    // the helper inherits the Supervisor environment, which is exactly where
    // `VAULT_ADDR` / `VAULT_TOKEN` live.
    let mut cmd = crate::agent::childproc::command(program);
    cmd.args(&tokens[1..]);
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true); // the timeout's teeth — no orphan past the deadline

    // Step 6 — run under the deadline. `kill_on_drop` reaps the child when the
    // `Elapsed` branch drops the command.
    let output = match tokio::time::timeout(timeout, cmd.output()).await {
        Err(_elapsed) => {
            return Err(format!(
                "key helper '{}' timed out after {}s",
                program,
                timeout.as_secs()
            ))
        }
        Ok(Err(e)) => return Err(format!("key helper '{program}' failed to start: {e}")),
        Ok(Ok(output)) => output,
    };

    // Step 7 — the output, fail-closed.
    decode_output(program, output)
}

/// The whitespace split (step 3), exposed for the pure unit test.
fn split_argv(remainder: &str) -> Vec<String> {
    remainder.split_whitespace().map(String::from).collect()
}

/// Trim a helper's stdout into a token (step 7's shape), exposed for the pure
/// unit test.
fn trim_token(stdout: &[u8]) -> Result<String, String> {
    if stdout.len() > MAX_HELPER_OUTPUT_BYTES {
        return Err(format!(
            "key helper produced {} bytes of output (limit {})",
            stdout.len(),
            MAX_HELPER_OUTPUT_BYTES
        ));
    }
    let text = std::str::from_utf8(stdout)
        .map_err(|_| "key helper produced output that is not valid UTF-8 (a token is text)")?
        .trim()
        .to_string();
    if text.is_empty() {
        return Err("key helper produced no output".to_string());
    }
    Ok(text)
}

/// Step 4: reject the executed program by extension or by interpreter name.
fn reject_program(program: &str) -> Result<(), String> {
    let lower = program.to_ascii_lowercase();
    // Take the FILE NAME off any path so `!C:\tools\helper.bat` and
    // `!./bin/helper.CMD` are both caught by the extension rule.
    let file_name = lower.rsplit(['/', '\\']).next().unwrap_or(lower.as_str());
    if file_name.ends_with(".cmd") || file_name.ends_with(".bat") {
        return Err(
            "key helper rejected: .cmd/.bat targets are not executed (no shell dispatch)"
                .to_string(),
        );
    }
    // The file STEM (extension off) against the dispatcher list, so a `sh`
    // named `sh.exe` — or a plain `sh` — is refused either way.
    let stem = match file_name.rfind('.') {
        Some(idx) if idx > 0 => &file_name[..idx],
        _ => file_name,
    };
    if REJECTED_INTERPRETERS.contains(&file_name) || REJECTED_INTERPRETERS.contains(&stem) {
        return Err(format!(
            "key helper rejected: shell dispatcher '{program}' is not a key helper"
        ));
    }
    Ok(())
}

/// Step 7: turn a finished child's output into the token, or into a diagnostic.
fn decode_output(program: &str, output: std::process::Output) -> Result<String, String> {
    if !output.status.success() {
        // The exit status is the fact; the stderr is a bounded hint.
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr: String = stderr.chars().take(MAX_STDERR_DIAGNOSTIC_BYTES).collect();
        let stderr = stderr.trim();
        return Err(if stderr.is_empty() {
            format!("key helper '{program}' exited with {}", output.status)
        } else {
            format!(
                "key helper '{program}' exited with {}: {stderr}",
                output.status
            )
        });
    }
    trim_token(&output.stdout).map_err(|e| format!("key helper '{program}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    // `ScratchDir` is used only by the `#[cfg(unix)]` tests below (every test
    // that spawns a real helper), so the import is gated the same way — on
    // Windows it would be an unused import.
    #[cfg(unix)]
    use crate::test_support::ScratchDir;

    #[test]
    fn strips_the_bang_and_splits_argv_on_whitespace() {
        // The pure splitter (step 3), cross-platform.
        assert_eq!(
            split_argv("vault read -field=token secret/litellm"),
            vec![
                "vault".to_string(),
                "read".to_string(),
                "-field=token".to_string(),
                "secret/litellm".to_string()
            ]
        );
        // Runs of whitespace (tabs included) collapse; leading/trailing drop.
        assert_eq!(
            split_argv("  echo\t\ttok   "),
            vec!["echo".to_string(), "tok".to_string()]
        );
    }

    #[test]
    fn trims_the_token_and_rejects_an_empty_or_oversized_one() {
        // The pure trim (step 7's tail), cross-platform.
        assert_eq!(trim_token(b"  tok\n\r").unwrap(), "tok");
        assert!(trim_token(b"   \n").is_err(), "blank output is no token");
        assert!(trim_token(b"").is_err());
        let big = vec![b'x'; MAX_HELPER_OUTPUT_BYTES + 1];
        let err = trim_token(&big).expect_err("a runaway helper's output is capped");
        assert!(err.contains("limit"), "{err}");
        assert!(trim_token(b"\xff\xfe").is_err(), "a token is UTF-8 text");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_each_shell_metacharacter() {
        // Step 2 precedes the split AND the spawn, so nothing executes for any
        // of these. The marker probe below proves the "never spawned" half.
        let dir = ScratchDir::in_env_temp("-t2a-keyhelper", "metachar");
        let marker = dir.path().join("marker");
        let marker_path = marker.display().to_string();
        for &ch in REJECTED_METACHARACTERS {
            let cmd = format!("!echo a{ch}b");
            let err = run(&cmd).await.expect_err("{cmd} must be rejected");
            assert!(
                err.contains("metacharacter"),
                "{cmd} must be rejected as a metacharacter: {err}"
            );
        }
        // The glue probe: a bare `touch <marker>` (an ordinary argument, no
        // metacharacters) is LEGAL argv — proving the splitter is not what
        // blocks the two rejected shapes below.
        assert_eq!(split_argv(&format!("touch {marker_path}")).len(), 2);
        // `;` glued to the program name: Err at the scan, no file appears.
        let glued_semicolon = format!("!touch;{marker_path}");
        let err = run(&glued_semicolon)
            .await
            .expect_err("a `;` glued to the program name is rejected");
        assert!(err.contains("metacharacter"), "{err}");
        assert!(
            !marker.exists(),
            "the metacharacter scan must reject BEFORE spawning: {marker_path}"
        );
        // `<` glued to the program name: the SAME Err (the whole-remainder scan
        // catches it before the split, so it never becomes argv).
        let glued_redirect = format!("!touch<{marker_path}");
        let err = run(&glued_redirect)
            .await
            .expect_err("a `<` glued to the program name is rejected");
        assert!(err.contains("metacharacter"), "{err}");
        assert!(
            !marker.exists(),
            "the metacharacter scan must reject BEFORE spawning: {marker_path}"
        );
    }

    #[tokio::test]
    async fn rejects_cmd_and_bat_targets_by_extension() {
        // Cross-platform: the extension rule is not Windows-gated.
        for cmd in [
            r"!C:\tools\helper.bat",
            "!helper.cmd",
            "!helper.BAT",
            r"!./bin/HELPER.cmd",
        ] {
            let err = run(cmd).await.expect_err("{cmd} must be rejected");
            assert!(err.contains(".cmd/.bat"), "{cmd}: {err}");
        }
    }

    #[tokio::test]
    async fn rejects_shell_dispatcher_programs() {
        // Cross-platform: the NAME rule is not gated either.
        for cmd in [
            "!cmd /c whoami",
            "!cmd.exe /c whoami",
            "!powershell -Command x",
            "!pwsh -Command x",
            "!sh -c x",
            "!bash -c x",
            "!zsh -c x",
            "!wsl echo tok",
        ] {
            let err = run(cmd).await.expect_err("{cmd} must be rejected");
            assert!(err.contains("dispatcher"), "{cmd}: {err}");
        }
    }

    #[tokio::test]
    async fn rejects_an_empty_or_bang_only_command() {
        // Step 1's empty check.
        for cmd in ["!", "!   ", "!  \t "] {
            let err = run(cmd).await.expect_err("{cmd} must be rejected");
            assert!(err.contains("empty command"), "{cmd}: {err}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executes_a_bare_program_and_returns_trimmed_stdout() {
        // `echo` is a real PATH program on Unix (the trailing newline is the
        // trim's edge; the leading-trim edge is the pure `trim_token` unit).
        assert_eq!(run("!echo tok").await.unwrap(), "tok");
        // Arguments reach the program as argv, uninterpreted.
        assert_eq!(run("!printf %s a-b").await.unwrap(), "a-b");
    }

    /// The absolute path of a trivial exit-status program, per OS. BSD lineage
    /// puts `true`/`false` in `/usr/bin` and Apple's `/bin` carries no such
    /// entry, so the Linux spelling fails to spawn on macOS — the same trap
    /// `interactive::sudo_exec` documents for `/bin/true`. An absolute path is
    /// used deliberately (the fail-closed stance): the test must not depend on
    /// whatever a hostile `PATH` resolves `true` to.
    #[cfg(unix)]
    fn exit_program(name: &str) -> String {
        let dir = if cfg!(target_os = "macos") {
            "/usr/bin"
        } else {
            "/bin"
        };
        let path = format!("{dir}/{name}");
        assert!(std::path::Path::new(&path).exists(), "{path} must exist");
        path
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fails_closed_on_empty_stdout() {
        // `true` exits 0 with no output (the PATH `true` of a hostile PATH
        // is not something a fail-closed test should trust).
        let helper = format!("!{}", exit_program("true"));
        let err = run(&helper).await.expect_err("no output is no token");
        assert!(err.contains("no output"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reports_a_nonzero_exit() {
        let helper = format!("!{}", exit_program("false"));
        let err = run(&helper).await.expect_err("a failing helper is an Err");
        assert!(err.contains("exited with"), "{err}");
        // The exit status is named (the diagnostic contract).
        assert!(err.contains("exit status"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reports_a_spawn_failure_for_an_unknown_program() {
        let err = run("!definitely-not-a-real-key-helper-xyz")
            .await
            .expect_err("an unknown program is an Err");
        assert!(err.contains("failed to start"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn times_out_and_kills_the_child() {
        // BOUND by design: `sleep 30` against a 1 s test-local deadline rather
        // than the production 10 s one — the SAME code path (`Elapsed` → `Err`
        // → `kill_on_drop` reaps the child), 10x cheaper. The production
        // constant is asserted separately so the deadline itself stays pinned.
        assert_eq!(HELPER_TIMEOUT, Duration::from_secs(10));
        let started = std::time::Instant::now();
        let err = run_with_timeout("!sleep 30", Duration::from_secs(1))
            .await
            .expect_err("a hung helper must time out");
        let elapsed = started.elapsed();
        assert!(err.contains("timed out"), "{err}");
        assert!(
            elapsed < Duration::from_secs(5),
            "the deadline must have teeth (kill_on_drop reaps), not wait for the \
             child to finish: {elapsed:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_cmd_shaped_string_in_a_non_first_argument_position_is_not_the_target() {
        // The step-4 boundary, pinned BOTH ways: the first token is the only
        // thing extension/name rejection covers, because it is the only token
        // the spawn executes.
        assert_eq!(run("!echo helper.cmd").await.unwrap(), "helper.cmd");
        assert_eq!(run("!echo cmd").await.unwrap(), "cmd");
        // …while the same names in FIRST position are Err (test above).
        assert!(run("!helper.cmd").await.is_err());
        assert!(run("!cmd").await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_stdin_blocking_helper_sees_eof_instead_of_hanging() {
        // `closed_stdin` is what makes `cat` (a stdin reader) terminate at once.
        let started = std::time::Instant::now();
        let err = run("!cat")
            .await
            .expect_err("`cat` with closed stdin yields no token");
        assert!(err.contains("no output"), "{err}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "closed stdin must EOF immediately, not hit the deadline: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn the_census_shape_holds_no_shell_and_no_raw_command() {
        // The invariant the plan's verification jumps grep for, asserted where
        // it cannot rot: this module never names the raw `Command` constructor,
        // never sets a creation flag, and never appends a shell tail. The
        // forbidden spellings are BUILT here rather than written as literals, so
        // the plan's whole-file grep for the raw constructor stays empty (the CI
        // jump), not merely the code half.
        let src = include_str!("key_helper.rs");
        // Only the CODE is scanned (the doc comment above names the shapes it
        // forbids in prose).
        let code = src.split("#[cfg(test)]").next().unwrap_or(src);
        let raw_ctor = format!("{}::{}", "Command", "new");
        let flags = format!("creation_{}", "flags");
        let no_window = format!("create_no_{}", "window");
        let shell_tail = format!("append_{}c", "cmd_");
        assert!(!code.contains(&raw_ctor), "route through childproc");
        assert!(!code.contains(&flags), "childproc owns the flags");
        assert!(!code.contains(&no_window), "childproc owns the flags");
        assert!(!code.contains(&shell_tail), "no shell-string shape here");
        assert!(code.contains("childproc::command"));
    }
}

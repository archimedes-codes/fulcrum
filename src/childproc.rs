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
//! `--worker` self-exec (`src/agent/worker/client.rs`) and the updater relaunch
//! (`src/commands/update.rs`) — could never pop a window in the shipped build,
//! where the exe is GUI-subsystem. They still route through here, for two
//! reasons: in a *debug* build the attribute is off and the exe is
//! console-subsystem, so the flag is live there; and a site added later
//! inherits the correct default rather than having to know this paragraph.
//! A reader who spots the apparent no-op should stop here, not "simplify" the
//! call away.
//!
//! What this module deliberately does NOT own: the environment (that is
//! [`crate::agent::hostenv`]), stdout/stderr (each site chooses those), and any
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

#[cfg(test)]
mod tests {
    use super::*;

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
    // Scanned: `src/**/*.rs` ONLY. `tests/*.rs` is deliberately NOT scanned —
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

    /// The census's own path, `/`-normalised and relative to `src/` — the one
    /// file allowed to hold `Command::new(` and `creation_flags`. Normalised so
    /// the comparison holds on Windows, where `strip_prefix` yields `\`.
    const SELF: &str = "agent/childproc.rs";

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
        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        assert!(
            src_root.is_dir(),
            "the census resolved the wrong root: {} is not a directory",
            src_root.display()
        );

        let mut paths = Vec::new();
        rust_files(&src_root, &mut paths);
        // Sorted so a violation report is reproducible run to run.
        paths.sort();

        let relative = |path: &std::path::Path| {
            path.strip_prefix(&src_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/")
        };
        let regions: Vec<(String, Vec<SourceLine>)> = paths
            .iter()
            .map(|path| (relative(path), production_region(path)))
            .collect();

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
            if !rel.starts_with("agent/tools/exec") {
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

        for runtime_child in [
            "agent/mcp/stdio.rs",
            "agent/worker/client.rs",
            "agent/interactive/sudo_exec.rs",
            "commands/update.rs",
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

    /// The crash-log module's path, `/`-normalised and relative to `src/` like
    /// [`SELF`], so the violation report reads the same on Windows.
    const CRASHLOG: &str = "agent/crashlog.rs";

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
        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        // One file is the whole scope of this invariant — no tree walk. The
        // call panics rather than skipping if the file is unreadable, so a
        // wrong root fails loudly here instead of silently emptying the region.
        let region = production_region(&src_root.join(CRASHLOG));

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
            src_root.join(CRASHLOG).display(),
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
        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        assert!(
            src_root.is_dir(),
            "the census resolved the wrong root: {} is not a directory",
            src_root.display()
        );
        let mut paths = Vec::new();
        rust_files(&src_root, &mut paths);
        // Sorted so a violation report is reproducible run to run.
        paths.sort();
        let relative = |path: &std::path::Path| {
            path.strip_prefix(&src_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/")
        };
        let regions: Vec<(String, Vec<SourceLine>)> = paths
            .iter()
            .map(|path| (relative(path), production_region(path)))
            .collect();

        // ── Vacuity guard FIRST, in the census's own style: a scanner that sees
        // nothing must fail loudly, not report a clean bill. Exactly ONE
        // production caller of [`append_cmd_c`] is expected, and it is
        // `exec/bash.rs` (`crate::agent::childproc::append_cmd_c(` contains the
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

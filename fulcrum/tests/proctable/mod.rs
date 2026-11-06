//! The shared process-table probe the daemon's integration tests ask "is this
//! pid still running?" with.
//!
//! # Why a shared module instead of one helper per file
//!
//! `tests/serve.rs` and `tests/worker_mode.rs` each grew a private copy of this
//! question (`pid_is_alive` and `pid_is_reaped`), written by different hands, and
//! the two Windows arms had already diverged in shape while agreeing on the same
//! bug: both matched `,` + pid + `,` against `tasklist /FO CSV` output. That is
//! the reason four `serve.rs` assertions failed on CI's `windows-latest` leg
//! against a LIVE child — `tasklist`'s CSV quotes EVERY field, so a live pid
//! appears as `,"8620",` and the unquoted needle matched nothing. A live process
//! was reported dead, and a durability test that believes that fails for a reason
//! nobody can see.
//!
//! Two copies of a platform probe means two chances to get it wrong and no way
//! for a fix in one to reach the other. The parsing lives here; each test file
//! keeps its own thin wrapper, because the unix arms differ legitimately
//! (`kill(pid, 0)` answers "alive" for a zombie, which is what `pid_is_alive`
//! wants, and "not ESRCH" is what `pid_is_reaped` wants) and neither is wrong.
//!
//! # The parsing is pure, and that is the point
//!
//! [`parse_tasklist_filtered`] and [`parse_tasklist_streams`] take the bytes
//! `tasklist` printed and return an answer. They are unconditional — compiled and
//! TESTED on every platform — so the Windows arm of this probe is not the
//! unverifiable blob it used to be: the shapes below (`proctable_tests`) are
//! `tasklist`'s documented output, quoted and unquoted, on stdout and on stderr,
//! and they run on Linux where they can actually be watched. Only the two
//! functions that spawn `tasklist` are `#[cfg(windows)]`, and only they carry the
//! "never run locally" caveat.
//!
//! # Unparseable is a PANIC, never "not alive"
//!
//! Every fallback here that could be written as `false` is written as a panic
//! with the raw output in its message. A probe that answers "not alive" when it
//! could not read the process table turns the durability tests
//! (`a_session_survives_its_clients_disconnect`, the `close` test) and the leak
//! guard into liars: they would report a child that vanished, on a platform where
//! nothing vanished, and the message would blame the daemon. This branch has spent
//! its whole life fighting that shape — a plausible false negative in a helper —
//! so the fail direction is UP, not quiet.
//!
//! # How to verify a change to a `#[cfg(windows)]` arm (READ BEFORE EDITING ONE)
//!
//! The local build gives **zero** signal about anything in this file below that
//! gate. `cargo check` on Linux does not merely skip running those lines, it does
//! not TYPE-CHECK them: `#[cfg(windows)]` is not satisfied, so the arm is not part
//! of the program at all. A type error planted inside one — and one was, during
//! this fix — leaves the local `cargo check --all-targets` and the local test
//! suite completely green while making the Windows leg fail to COMPILE. Measured,
//! not asserted: the same planted `let x: u32 = "…"` returned exit 0 locally and
//! exit 101 naming its own line under the gnu target.
//!
//! So the only local proof that a Windows arm is even well-formed is:
//!
//! ```text
//! cargo clippy --target x86_64-pc-windows-gnu --all-targets -- -D warnings
//! ```
//!
//! and it must be run on EVERY change to a Windows arm, in this file and in
//! `tests/serve.rs` / `tests/worker_mode.rs`. Green there means "compiles and
//! lints", never "works" — `in_process_table` and `assert_probe_works` keep their
//! own notes about what stays unverified until a Windows runner executes them.

// `Command` is named only by the two `#[cfg(windows)]` functions below, so an
// ungated import is an unused-import warning — and under CI's
// `cargo clippy --all-targets -- -D warnings`, a build failure — on unix. Gated,
// not `#[allow]`ed: the import is genuinely absent from the unix build.
#[cfg(windows)]
use std::process::Command;

/// What the process table said about a pid.
#[derive(Debug, PartialEq, Eq)]
pub enum Probe {
    /// A row names this pid: the process is in the table.
    Present,
    /// The lookup succeeded and no row names this pid: it is gone.
    Absent,
    /// The output cannot be read as a process table. Deliberately NOT `Absent` —
    /// see the module header. Carries the raw output for the panic message.
    Unreadable(String),
}

/// Read `tasklist /FI "PID eq <pid>" /NH /FO CSV` output.
///
/// Pure so it is testable on a machine that has no `tasklist` (see the module
/// header). This is the single-stream form; a caller holding both streams of the
/// child should use [`parse_tasklist_streams`], which is what
/// [`in_process_table`] does — see its note for why the report line's stream is
/// not something this suite gets to assume.
///
/// Two passes, in this order — the order is the safety argument, so it is spelled
/// out rather than left to the code:
///
/// 1. **across EVERY line**: a CSV row whose PID field parses to `pid` →
///    [`Probe::Present`]. The field is compared AFTER unquoting, which is the
///    whole fix: the documented shape is
///    `"image.exe","8620","Console","1","9 240 K"`, quoted on every field. This
///    pass runs FIRST and over the WHOLE output rather than returning at the
///    first non-row line, so that neither weirdness in the other columns (an
///    image name that starts `INFO:`, say) nor a stray line printed AHEAD of the
///    rows (a shim's banner, a crash notice) can talk a live process into reading
///    as anything but present.
/// 2. only if no row claimed the pid, classify what was printed:
///    * a line starting `INFO:` whose PID column is not a number → the tool's
///      report about the lookup, i.e. [`Probe::Absent`]. Matched on the marker,
///      not on the English sentence: `tasklist`'s prose is localised, and matching
///      its words would be the same Linux-prose mistake this file exists to stop.
///      The "PID column is not a number" half is what keeps a localised sentence
///      (whose commas make it LOOK like a multi-field row) from being mistaken for
///      a row, and keeps pass 1 as the only thing that can say "alive".
///    * a blank line → nothing.
///    * a row whose PID field parses to some OTHER pid → [`Probe::Unreadable`]:
///      the `/FI` filter did not filter, so the flags are not the flags we think
///      we passed, and an answer built on that is not an answer.
///    * anything else → [`Probe::Unreadable`], and it OUTRANKS the `INFO:` report:
///      a table that also printed something unrecognisable is a table this probe
///      does not understand, and "the process is gone" is the one answer that
///      must not be inferred from a table nobody understands.
pub fn parse_tasklist_filtered(output: &str, pid: u32) -> Probe {
    match classify_stream(output, pid) {
        Stream::Present => Probe::Present,
        Stream::Report => Probe::Absent,
        Stream::Nothing => Probe::Unreadable(
            "tasklist printed neither a row nor the `INFO:` line — an empty stdout is not \
             evidence that a process is gone"
                .to_string(),
        ),
        Stream::Anomaly(why) => Probe::Unreadable(why),
    }
}

/// What one stream of the child said, before it is folded into a [`Probe`].
enum Stream {
    /// A row named the pid.
    Present,
    /// The `INFO:` report, and nothing that contradicted it.
    Report,
    /// Nothing at all (empty, or only blank lines).
    Nothing,
    /// Something unrecognisable, with the reason.
    Anomaly(String),
}

/// Classify ONE stream. Split out so [`parse_tasklist_streams`] can weigh two —
/// and note that weighing is NOT symmetric between them; see the order documented
/// there rather than any ranking on this type.
fn classify_stream(output: &str, pid: u32) -> Stream {
    let mut saw_info = false;
    let mut anomaly: Option<String> = None;

    // Pass 1 — presence, over the whole output. Nothing that comes BEFORE the
    // row can suppress it.
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // The documented CSV row: every field quoted. Split on the commas that
        // separate FIELDS, i.e. those outside a quoted run — splitting blind would
        // shift the PID into the wrong column if a quoted field ever held one.
        let fields = split_csv(line);
        // What the PID column says, if it parses at all. This — the text, not the
        // shape — is the primary signal: a line whose PID column IS this pid is a
        // row, whatever else it looks like (an image name could start `INFO:`, and
        // a row naming the process we asked about must never read as "gone").
        if pid_column(&fields) == Some(pid) {
            return Stream::Present;
        }
    }

    // Pass 2 — nothing claimed the pid, so work out what did.
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let fields = split_csv(line);
        let seen = pid_column(&fields);
        if line.starts_with("INFO:") && seen.is_none() {
            saw_info = true;
            continue;
        }
        if anomaly.is_none() {
            anomaly = Some(if fields.len() < 2 {
                format!("neither a row nor an `INFO:` report: {line:?}")
            } else if let Some(other) = seen {
                format!(
                    "the /FI \"PID eq {pid}\" filter returned a row for pid {other} — the \
                     flags are not being honoured, so no answer can be built from this"
                )
            } else {
                // A header row (whose PID column reads `PID`) is not a number, so
                // it lands here rather than quietly counting as "the filter
                // matched nothing" — see `a_header_row_is_unreadable_not_absent`.
                format!(
                    "unparsable PID column {:?} in {line:?}",
                    fields.get(1).expect("len() >= 2 was just checked")
                )
            });
        }
    }

    if let Some(why) = anomaly {
        Stream::Anomaly(why)
    } else if saw_info {
        Stream::Report
    } else {
        Stream::Nothing
    }
}

/// The PID column of a split CSV line, unquoted, if it parses. Column 2 is
/// `tasklist`'s documented position for `/FO CSV`.
///
/// ⚠ This is the module's one structural assumption about the table, and it is
/// locale-INDEPENDENT: `tasklist` localises the column NAMES (which is why the
/// header row is rejected rather than parsed, and why the report line is matched
/// on its `INFO:` marker and not its sentence) but not the column ORDER, which is
/// fixed by `/FO CSV`'s documented schema — image name, PID, session name,
/// session number, memory. If that ever changed, this helper would find no PID in
/// column 2, every lookup would answer "no row", and the answer would be
/// `Absent`-shaped — i.e. exactly the silent false negative this module refuses.
/// [`assert_probe_works`] is the guard: it asks the same question about a pid that
/// cannot be gone, so a column-order change reddens the control lookup instead of
/// quietly reporting every live child as dead.
fn pid_column(fields: &[String]) -> Option<u32> {
    fields
        .get(1)
        .and_then(|f| f.trim().trim_matches('"').parse::<u32>().ok())
}

/// Read BOTH streams of the `tasklist` child, and say what the process table
/// thinks about `pid`.
///
/// Pure, and testable here, like [`parse_tasklist_filtered`]. It exists because
/// the "no match" report line is NOT reliably on stdout: `tasklist` is documented
/// to print `INFO: No tasks are running…` and has done so on stderr on some
/// Windows versions — which is why the canonical batch idiom everyone copies is
/// `tasklist /FI "…" 2>NUL | find …`, and why one report of the line landing on
/// stderr recommends `2>&1`. Reading stdout alone would then give an EMPTY stdout
/// on the ordinary "the process is gone" answer, this parser would (correctly, in
/// isolation) call an empty stream unreadable, and [`in_process_table`] would
/// panic on every single `Absent` — turning the leak guard and the durability
/// test red on a platform where nothing went wrong, and making `Probe::Absent`
/// unreachable. Reading both streams makes "gone" reachable whichever stream the
/// report arrives on, and keeps the panic for the case that genuinely has no
/// evidence in either.
///
/// # stdout is the primary stream and stderr may not veto it
///
/// The same `2>NUL` idiom exists because `tasklist` also writes non-printing
/// garbage that scripts suppress — so stderr is a stream that is KNOWN to carry
/// noise on a healthy call. Weighing the two streams equally would let that noise
/// outrank a clean `INFO:` on stdout and panic on a normal "the process is gone",
/// which is the false red this function exists to prevent, arriving from the
/// other side. Hence the asymmetry, in this order:
///
/// 1. a row naming `pid` on EITHER stream → [`Probe::Present`]. Positive evidence
///    is positive evidence wherever it appears, and this can never produce a
///    false "gone" — the direction that matters most.
/// 2. a clean `INFO:` report on stdout → [`Probe::Absent`], whatever stderr says.
/// 3. stdout silent, stderr reporting → [`Probe::Absent`] (the case step 2 of the
///    header is about).
/// 4. otherwise → [`Probe::Unreadable`], naming both streams. Notably: garbage on
///    STDOUT is still unreadable even if stderr carries an `INFO:` — the primary
///    stream being unintelligible is a real problem and fails UP.
pub fn parse_tasklist_streams(stdout: &str, stderr: &str, pid: u32) -> Probe {
    let out = classify_stream(stdout, pid);
    let err = classify_stream(stderr, pid);

    // 1. Presence wins from either stream.
    if matches!(out, Stream::Present) || matches!(err, Stream::Present) {
        return Probe::Present;
    }
    // 2/3. A clean report from the primary stream, or from stderr when the
    // primary said nothing at all.
    match (&out, &err) {
        (Stream::Report, _) | (Stream::Nothing, Stream::Report) => return Probe::Absent,
        _ => {}
    }
    // 4. No evidence, or contradictory evidence: report, do not guess.
    let why = match (out, err) {
        // An unintelligible line anywhere outranks silence here — note stdout's
        // own garbage reaching this arm is the intended "fail UP" case.
        (Stream::Anomaly(w), _) | (_, Stream::Anomaly(w)) => w,
        (Stream::Nothing, Stream::Nothing) => {
            "tasklist printed nothing on EITHER stream — neither a row nor the `INFO:` line. \
             An empty process table is not evidence that a process is gone"
                .to_string()
        }
        _ => "neither stream carried a row for the pid or a clean `INFO:` report".to_string(),
    };
    Probe::Unreadable(why)
}

/// Split one CSV line into fields, honouring double quotes (`""` is an escaped
/// quote in CSV, and is un-doubled here).
fn split_csv(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(std::mem::take(&mut current));
            }
            other => current.push(other),
        }
    }
    fields.push(current);
    fields
}

/// Ask `tasklist` whether `pid` is in the process table. Windows only.
///
/// Panics rather than guessing, per the module header — including when
/// `tasklist` itself cannot be run, and when the answer is "absent" for a pid
/// that could not be `tasklist`'s own fault (see [`assert_probe_works`]).
///
/// Feeds BOTH streams to the parser, because the "no match" report line is not
/// reliably on stdout — see [`parse_tasklist_streams`], which is where the
/// evidence and the consequence are spelled out.
#[cfg(windows)]
pub fn in_process_table(pid: u32) -> bool {
    let output = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "cannot ask the process table about pid {pid}: `tasklist` failed to run \
                 ({e}). Answering \"not alive\" here would make every durability and \
                 leak assertion in this binary report a process that never died."
            )
        });
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    match parse_tasklist_streams(&stdout, &stderr, pid) {
        Probe::Present => true,
        Probe::Absent => {
            // Before believing "gone", prove the instrument can see a process we
            // KNOW is live — this one. A probe that answers `Absent` for
            // everything (the `,{pid},` bug, a changed column order, a locale
            // that reshapes the output) would otherwise report every live child
            // as dead, and the tests would blame the daemon.
            assert_probe_works(pid);
            false
        }
        Probe::Unreadable(why) => panic!(
            "cannot read `tasklist`'s answer about pid {pid}: {why}\n\
             exit: {}\nstdout: {stdout:?}\nstderr: {stderr:?}",
            output.status,
        ),
    }
}

/// Prove the probe can see a process that is definitely alive: this one.
///
/// Runs ONCE per test binary (`Once`, because `in_process_table` is polled in a
/// loop and `tasklist` is not cheap). Panics naming both pids, because the
/// failure it detects is "the probe is broken", not "the child died".
///
/// # It cannot recurse, and it cannot deadlock — and that is load-bearing
///
/// * **No recursion.** This function does NOT call `in_process_table`; it spawns
///   `tasklist` and calls the pure parser directly. Routing the control lookup
///   back through `in_process_table` would make the call graph
///   `in_process_table` → `assert_probe_works` → `in_process_table` → … and,
///   because the inner call would also see `Absent` and also try to prove the
///   probe, it would be a `Once` re-entered from inside its own `call_once` —
///   which does not unwind cleanly, it deadlocks the whole test binary. Keep it
///   direct.
/// * **One `tasklist`, ever.** `Once` means the polling loops
///   (`wait_for_death`-style `while pid_is_alive(..)` in `tests/serve.rs`) pay
///   this cost at most once per binary, not once per poll.
/// * **Blocking, deliberately.** `Once::call_once` here runs a synchronous
///   `tasklist` (~50–200 ms) on a tokio worker thread inside an async test. That
///   is acceptable for an integration suite on this scale and is what makes the
///   once-per-binary guarantee possible; it is NOT on any hot path.
/// * **A poisoned `Once` is the second-best outcome, not a silent one.** If the
///   control lookup panics, `Once` poisons and a LATER call panics with
///   "Once instance has previously been poisoned" instead of this message. The
///   test still fails loudly and the first — informative — panic is already in
///   the output, so no wrong answer is ever produced by poisoning.
#[cfg(windows)]
fn assert_probe_works(queried: u32) {
    static CHECK: std::sync::Once = std::sync::Once::new();
    let me = std::process::id();
    CHECK.call_once(|| {
        let out = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {me}"), "/NH", "/FO", "CSV"])
            .output();
        let ok = match &out {
            // Both streams, and `Present` is the ONLY acceptable answer: this
            // lookup is about a process that cannot be gone, so "absent" here is
            // the probe failing, never the truth. Same stream-agnosticism as
            // [`parse_tasklist_streams`], for the same reason.
            Ok(o) => matches!(
                parse_tasklist_streams(
                    &String::from_utf8_lossy(&o.stdout),
                    &String::from_utf8_lossy(&o.stderr),
                    me
                ),
                Probe::Present
            ),
            Err(_) => false,
        };
        assert!(
            ok,
            "`tasklist` cannot see ITS OWN PROCESS (pid {me}), yet reported pid {queried} \
             as gone — the probe is broken, not the child. Raw output of the control \
             lookup: {out:?}. Fix this helper before trusting any pid assertion in this \
             binary.",
        );
    });
}

#[cfg(test)]
mod proctable_tests {
    use super::{parse_tasklist_filtered, parse_tasklist_streams, Probe};

    const PID: u32 = 8620;

    /// `tasklist /FO CSV` quotes EVERY field — the documented shape, and the
    /// reason the `,8620,` substring this file used to search for matched nothing
    /// on a real Windows runner. This is the fixture that reproduces CI's four
    /// false "the child is dead" reports from a live process.
    #[test]
    fn a_quoted_csv_row_names_the_pid() {
        let row = "\"fake_worker_core.exe\",\"8620\",\"Console\",\"1\",\"9 240 K\"\r\n";
        assert_eq!(parse_tasklist_filtered(row, PID), Probe::Present);
    }

    /// The same row without the quotes (a `tasklist` variant, and the shape the
    /// OLD matcher was written against) must read the same way. Tolerating both
    /// is what makes this helper's answer independent of the quoting.
    #[test]
    fn an_unquoted_csv_row_names_the_pid_too() {
        let row = "fake_worker_core.exe,8620,Console,1,9 240 K\n";
        assert_eq!(parse_tasklist_filtered(row, PID), Probe::Present);
    }

    /// The documented "not there" answer. `Absent`, not `Unreadable` — this is
    /// the one output that legitimately means gone, and the leak guard depends on
    /// distinguishing it from a zombie (see `pid_is_reaped`'s note in
    /// `worker_mode.rs`).
    #[test]
    fn the_info_line_means_absent() {
        let info = "INFO: No tasks are running which match the specified criteria.\r\n";
        assert_eq!(parse_tasklist_filtered(info, PID), Probe::Absent);
    }

    /// A localised `tasklist` renders that sentence in the install's language, so
    /// the match is on the `INFO:` marker. Pinning the English words would be the
    /// same mistake as pinning a kernel's error prose.
    #[test]
    fn a_localised_info_line_is_still_absent() {
        let info = "INFO: Es werden keine Aufgaben ausgef\u{fc}hrt, die die angegebenen Kriterien erf\u{fc}llen.\r\n";
        assert_eq!(parse_tasklist_filtered(info, PID), Probe::Absent);
    }

    /// A pid that merely CONTAINS the digits of the one we asked about is a
    /// different process. The old check was a substring over the whole line, so
    /// `18620` would have answered "alive" for `8620`; field-wise comparison is
    /// what makes this the wrong answer.
    #[test]
    fn a_different_pid_that_contains_the_digits_is_not_a_match() {
        let row = "\"other.exe\",\"186205\",\"Console\",\"1\",\"9 240 K\"\r\n";
        assert!(matches!(
            parse_tasklist_filtered(row, PID),
            Probe::Unreadable(_)
        ));
    }

    /// Empty stdout is NOT "absent": with `/FI` the tool prints either a row or
    /// the INFO line, so printing nothing means something else went wrong
    /// (redirect swallowed, tool missing behind a shim, an unexpected flag). The
    /// caller turns this into a panic naming the raw output.
    #[test]
    fn empty_output_is_unreadable_not_absent() {
        assert!(matches!(
            parse_tasklist_filtered("", PID),
            Probe::Unreadable(_)
        ));
        assert!(matches!(
            parse_tasklist_filtered("   \r\n", PID),
            Probe::Unreadable(_)
        ));
    }

    /// A header row (i.e. `/NH` not taking effect, or dropped by a future edit)
    /// must not be read as "the process is gone" — the PID column reads `PID`,
    /// which is not a number, and guessing from that is exactly the quiet failure
    /// this module refuses.
    #[test]
    fn a_header_row_is_unreadable_not_absent() {
        let header = "\"Image Name\",\"PID\",\"Session Name\",\"Session#\",\"Mem Usage\"\r\n";
        assert!(matches!(
            parse_tasklist_filtered(header, PID),
            Probe::Unreadable(_)
        ));
    }

    /// A comma inside a quoted field must not shift the PID into column 3 — a
    /// column shift is how a probe starts answering about the wrong process.
    #[test]
    fn a_comma_inside_a_quoted_field_does_not_shift_the_columns() {
        let row = "\"imag,e.exe\",\"8620\",\"Console\",\"1\",\"9 240 K\"\r\n";
        assert_eq!(parse_tasklist_filtered(row, PID), Probe::Present);
    }

    /// A process whose IMAGE NAME starts with `INFO:` is still a live process.
    ///
    /// The UNQUOTED shape is the one that tests this: a quoted row starts with a
    /// `"` character, so it never looks like the report line at all, and a fixture
    /// written in the quoted shape passes for the wrong reason (both orderings of
    /// the rules pass it — measured). Unquoted, the line begins `INFO:` AND names
    /// the pid, so it is exactly the case where the rule order decides the answer,
    /// and where getting it wrong reports a LIVE process as gone — the direction
    /// that breaks the durability tests silently.
    #[test]
    fn a_row_whose_image_name_starts_like_the_info_report_is_still_present() {
        let row = "INFO: helper.exe,8620,Console,1,9 240 K\r\n";
        assert_eq!(parse_tasklist_filtered(row, PID), Probe::Present);
    }

    /// The other half of the same ordering: an `INFO:`-looking line that carries a
    /// NUMERIC second column naming a DIFFERENT process is a row the filter should
    /// not have returned, not the tool's report. Calling it the report would make
    /// the answer `Absent` — "gone" — off a line that plainly shows a live process,
    /// which is the dangerous direction again.
    #[test]
    fn an_info_shaped_line_naming_another_pid_is_unreadable_not_absent() {
        let row = "INFO: helper.exe,186205,Console,1,9 240 K\r\n";
        assert!(matches!(
            parse_tasklist_filtered(row, PID),
            Probe::Unreadable(_)
        ));
    }

    /// A line that is neither a row nor the report (a crash notice, a shim's
    /// banner, a usage error landing on stdout) is not evidence about a process.
    #[test]
    fn a_line_that_is_neither_a_row_nor_the_report_is_unreadable() {
        assert!(matches!(
            parse_tasklist_filtered("ERROR: The parameter is incorrect.\r\n", PID),
            Probe::Unreadable(_)
        ));
    }

    /// A stray line ahead of the row must not hide a LIVE process.
    ///
    /// The single-pass version classified line-by-line and returned at the FIRST
    /// line that was not a row — so a banner, a crash notice or a shim's preamble
    /// printed before the CSV rows turned "the pid is here, and here is its row"
    /// into a panic. That is the safe direction (it fails up, not quietly), but it
    /// is still a wrong answer about a process the output plainly shows alive, and
    /// on the durability test it reads as "the probe cannot be trusted" rather
    /// than as what it is. Presence is therefore collected over EVERY line before
    /// any single line is allowed to decide the answer; "unreadable" is only
    /// reached once nothing claimed the pid at all.
    #[test]
    fn a_stray_line_before_the_row_does_not_hide_a_live_process() {
        let output = "A vendor shim printed this first.\r\n\
                      \"fake_worker_core.exe\",\"8620\",\"Console\",\"1\",\"9 240 K\"\r\n";
        assert_eq!(parse_tasklist_filtered(output, PID), Probe::Present);
    }

    /// Garbage on the PRIMARY stream outranks the report, and the answer is a
    /// panic — not `Absent`.
    ///
    /// The name of the old test here said "still absent"; the behaviour, once the
    /// parser was made two-pass, is the opposite, and the opposite is the right
    /// way round: if stdout printed something this probe cannot read AND an
    /// `INFO:` line, the table is not intelligible, and "the process is gone" is
    /// the one answer this module refuses to infer from a table it does not
    /// understand. Failing up here costs a red test with the raw output in it;
    /// answering `Absent` would cost a durability test that blames the daemon.
    #[test]
    fn a_stray_line_on_stdout_outranks_the_report_and_is_unreadable() {
        let output = "A vendor shim printed this first.\r\n\
                      INFO: No tasks are running which match the specified criteria.\r\n";
        assert!(matches!(
            parse_tasklist_filtered(output, PID),
            Probe::Unreadable(_)
        ));
    }

    // ── the two-stream question: where does the `INFO:` line actually land? ────

    /// The whole reason `parse_tasklist_streams` exists: on some Windows versions
    /// the report line arrives on STDERR, leaving stdout EMPTY. Reading stdout
    /// alone made that empty stdout `Unreadable`, so `Absent` — "the process is
    /// gone", the answer the leak guard and the durability test both need — was
    /// UNREACHABLE, and every legitimately-dead child panicked the probe.
    #[test]
    fn a_report_on_stderr_with_an_empty_stdout_is_absent() {
        let info = "INFO: No tasks are running which match the specified criteria.\r\n";
        assert_eq!(parse_tasklist_streams("", info, PID), Probe::Absent);
    }

    /// The documented placement (stdout) must keep working — this is the case the
    /// original single-stream parser handled, and the two-stream form must not
    /// regress it.
    #[test]
    fn a_report_on_stdout_with_an_empty_stderr_is_absent() {
        let info = "INFO: No tasks are running which match the specified criteria.\r\n";
        assert_eq!(parse_tasklist_streams(info, "", PID), Probe::Absent);
    }

    /// …and a report on stdout survives NOISE ON STDERR. This is the asymmetry
    /// that matters: `tasklist` is known to emit non-printing garbage that every
    /// batch script suppresses with `2>NUL`, so stderr is a stream a healthy call
    /// dirties. Weighing the streams equally would let that garbage veto a clean
    /// `INFO:` and panic on an ordinary "the process is gone" — the same false red
    /// this function exists to remove, arriving from the other side.
    #[test]
    fn noise_on_stderr_does_not_veto_a_clean_report_on_stdout() {
        let info = "INFO: No tasks are running which match the specified criteria.\r\n";
        let garbage = "\u{1}\u{0}\u{2}x\u{0}y\u{0}\u{1}\u{0}\r\n";
        assert_eq!(parse_tasklist_streams(info, garbage, PID), Probe::Absent);
    }

    /// A row on either stream is PRESENT, and presence outranks everything —
    /// including an `INFO:` on the other stream, which would otherwise be the
    /// dangerous direction (a live process read as gone).
    #[test]
    fn a_row_on_either_stream_is_present() {
        let row = "\"fake_worker_core.exe\",\"8620\",\"Console\",\"1\",\"9 240 K\"\r\n";
        let info = "INFO: No tasks are running which match the specified criteria.\r\n";
        assert_eq!(parse_tasklist_streams(row, info, PID), Probe::Present);
        assert_eq!(parse_tasklist_streams(info, row, PID), Probe::Present);
        assert_eq!(parse_tasklist_streams("", row, PID), Probe::Present);
    }

    /// Garbage on STDOUT is still unreadable even when stderr carries a clean
    /// report: the primary stream being unintelligible is a real fault, and it
    /// fails UP.
    #[test]
    fn garbage_on_stdout_is_unreadable_even_with_a_report_on_stderr() {
        let info = "INFO: No tasks are running which match the specified criteria.\r\n";
        assert!(matches!(
            parse_tasklist_streams("ERROR: The parameter is incorrect.", info, PID),
            Probe::Unreadable(_)
        ));
    }

    /// Nothing anywhere is not "gone".
    #[test]
    fn two_empty_streams_are_unreadable() {
        assert!(matches!(
            parse_tasklist_streams("", "", PID),
            Probe::Unreadable(_)
        ));
    }

    /// The parser's own teeth: the OLD implementation, kept here as the thing
    /// these fixtures must NOT agree with. Feeding it the quoted row reproduces
    /// CI's false negative exactly — which is the evidence that the diagnosis was
    /// the quoting and not the daemon.
    #[test]
    fn the_old_substring_match_is_what_went_wrong() {
        let quoted = "\"fake_worker_core.exe\",\"8620\",\"Console\",\"1\",\"9 240 K\"\r\n";
        let old = quoted.lines().any(|l| l.contains(&format!(",{PID},")));
        assert!(
            !old,
            "if this ever stops holding, `tasklist` stopped quoting and the \
             quoted-row fixture above is no longer the documented shape"
        );
        assert_eq!(parse_tasklist_filtered(quoted, PID), Probe::Present);
    }
}

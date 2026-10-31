use super::*;
use crate::agent::policy::{AccessPolicy, FilePolicy};
use crate::agent::tools::{execute_tool, ContentBlock, ToolCtx, ToolResult};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

// ── skip visibility ──────────────────────────────────────────────────

/// How many behavioral tests SKIPPED (no Landlock / no `/dev/shm`). A
/// green suite where every confinement test skipped proves nothing, so a
/// skip prints the greppable `SKIP-SANDBOX:` marker and this counts them;
/// `sandbox_coverage_is_reported` prints the verdict line. On a kernel
/// that confines, `grep -c SKIP-SANDBOX` over the run output MUST be 0.
static SKIPPED: AtomicUsize = AtomicUsize::new(0);

/// The base dir for the behavioral tests: `/dev/shm`, deliberately NOT
/// under `/tmp` / `/var/tmp` — BOTH of those are in the sandbox
/// allow-list (ordinary commands need temp dirs), so a test rooted
/// there would pass VACUOUSLY. `/dev/shm` sits under `/dev`, which a
/// confined child may read but not write, so a dir there is genuinely
/// OUTSIDE the write boundary while still being writable by the
/// (unconfined) test process.
fn test_base() -> Option<PathBuf> {
    let base = Path::new("/dev/shm");
    base.is_dir().then(|| base.to_path_buf())
}

/// A file the TEST process owns (so `EACCES` proves Landlock did it, not
/// DAC) that is OUTSIDE the sandbox's READ allow-list — the read tests
/// cannot use [`test_base`], because `/dev/shm` sits under `/dev`, which
/// the ruleset grants READ on by design (a shell that cannot reach
/// `/dev/null` is not a shell). Candidates are the user's own runtime dir
/// and `$HOME`; each is checked for being (a) ours, (b) writable, and (c)
/// not under any directory the ruleset makes readable. `None` = no such
/// place on this machine, so the caller skips loudly.
fn private_dir_outside_the_read_allow_list() -> Option<PathBuf> {
    const READABLE: &[&str] = &[
        "/usr", "/bin", "/sbin", "/lib", "/lib64", "/dev", "/proc", "/etc", "/tmp", "/var/tmp",
    ];
    let candidates: Vec<PathBuf> = [
        std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from),
        Some(PathBuf::from(format!("/run/user/{}", unsafe {
            libc::getuid()
        }))),
        std::env::var_os("HOME").map(PathBuf::from),
    ]
    .into_iter()
    .flatten()
    .filter_map(|p| p.canonicalize().ok())
    .filter(|p| p.is_dir() && READABLE.iter().all(|r| !p.starts_with(r)))
    .collect();
    candidates.into_iter().find(|p| {
        // Writable by us, and readable by us (DAC): the file we plant
        // must be one the UNCONFINED process can read, or the confined
        // denial would prove nothing.
        let probe = p.join(format!("archimedes-sandbox-probe-{}", uuid::Uuid::new_v4()));
        let ok = std::fs::write(&probe, b"x").is_ok()
            && std::fs::read(&probe).map(|b| b == b"x").unwrap_or(false);
        let _ = std::fs::remove_file(&probe);
        ok
    })
}

/// SKIP (never fail) when the kernel has no Landlock or `/dev/shm` is
/// absent: CI must stay reproducible on a kernel without Landlock, so
/// these assert only behavior the running kernel can exhibit. The
/// fail-CLOSED path is asserted unconditionally elsewhere (the exec.rs
/// confined tests + the availability test below), and the skip is
/// LOUD (marker + counter) so green cannot be mistaken for verified.
macro_rules! require_sandbox {
    () => {{
        match test_base() {
            Some(base) if landlock_available() => base,
            _ => {
                let n = SKIPPED.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                eprintln!(
                    "SKIP-SANDBOX: {} (no Landlock / no /dev/shm) — skip #{n}",
                    stringify!(file!()
                        : line!())
                );
                return;
            }
        }
    }};
}

/// A fresh dir under [`test_base`], REMOVED on drop — the shared guard's
/// `Drop` also runs when a scope unwinds (see `test_support::ScratchDir`,
/// which is honest about what a per-test panic can pin; whether a FAILING
/// run left anything behind is task 09's census, not this type's promise).
/// The removal is the shared `test_support` guard's; the BASE is still
/// decided here, by [`test_base`] alone, because a sandbox fixture rooted
/// in `/tmp` would pass VACUOUSLY — see [`test_base`].
struct TmpDir(crate::test_support::ScratchDir);

impl TmpDir {
    fn new(tag: &str) -> Option<Self> {
        let base = test_base()?;
        Some(Self(crate::test_support::ScratchDir::in_dir(
            &base,
            "archimedes-sandbox-",
            tag,
        )?))
    }
    fn path(&self) -> &Path {
        self.0.path()
    }
    /// The dir as a shell-quotable string (the tags are uuid-derived, so
    /// no quoting characters can appear).
    fn display(&self) -> String {
        self.0.display()
    }
}

/// Pin `$HOME` to a fresh dir for the duration of a test, so
/// `boundary::protected_dirs()` describes a synthetic layout instead of
/// the developer's real `~/.agents` (the `boundary` tests' `pin_home`
/// pattern). The env lock is held and the previous value is restored on
/// drop — INCLUDING when an assertion panics.
///
/// NOTE: a pinned `$HOME` lives under `/dev/shm`, which the sandbox
/// READS (it sits under `/dev`), so a pinned home is only ever used for
/// WRITE tests. The read test below uses the real `$HOME` instead.
struct PinnedHome {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: Option<std::ffi::OsString>,
    /// The pinned `$HOME` itself (owned: removed when the pin ends).
    home: TmpDir,
}

impl PinnedHome {
    fn new(tag: &str) -> Option<Self> {
        let home = TmpDir::new(tag)?;
        let lock = crate::test_support::env_lock();
        // SAFETY: the env lock above is held for the whole pin, so no
        // other thread reads/writes `HOME` concurrently, and the
        // restore in `Drop` runs while it is still held.
        let previous = std::env::var_os("HOME");
        // SAFETY: as above.
        unsafe { std::env::set_var("HOME", home.path()) };
        Some(Self {
            _lock: lock,
            previous,
            home,
        })
    }
    fn path(&self) -> &Path {
        self.home.path()
    }
}

impl Drop for PinnedHome {
    fn drop(&mut self) {
        match self.previous.take() {
            // SAFETY: the env lock is still held (declared first, so it
            // drops last).
            Some(v) => unsafe { std::env::set_var("HOME", v) },
            // SAFETY: as above.
            None => unsafe { std::env::remove_var("HOME") },
        }
    }
}

/// The test ctx: `shell: Sandboxed` (the tier under test). HERMETIC —
/// single-root boundary and an empty deny-list, NEVER the `$HOME`-
/// reading boundary helpers (the `exec.rs` test-module rule).
fn confined_ctx(cwd: &Path, reads: AccessPolicy) -> ToolCtx {
    ToolCtx {
        cwd: cwd.to_path_buf(),
        cancel: CancellationToken::new(),
        skill_roots: None,
        boundary: vec![cwd.to_path_buf()],
        file_policy: FilePolicy {
            reads,
            writes: AccessPolicy::Sandboxed,
            shell: AccessPolicy::Sandboxed,
            rules: Vec::new(),
        },
        protected: Vec::new(),
        // (T03) HERMETIC: no rules, no home (see `confined_ctx`'s doc).
        rules: Vec::new(),
        home: None,
        web: Default::default(),
        render_requester: None,
    }
}

/// Run `command` confined to `cwd`.
async fn run(ctx: &ToolCtx, command: &str) -> ToolResult {
    execute_tool(ctx, "bash", &json!({ "command": command })).await
}

/// The first text block's text.
fn text_of(r: &ToolResult) -> String {
    match &r.content[0] {
        ContentBlock::Text { text } => text.clone(),
        other => panic!("expected a text block, got {other:?}"),
    }
}

// ── the constant table (the bug class this module existed to hide) ───

/// The values as transcribed from the kernel UAPI header — the same
/// numbers the module's `ACCESS_*` constants claim.
const UAPI_EXPECTED: &[(&str, u64)] = &[
    ("LANDLOCK_ACCESS_FS_EXECUTE", 1 << 0),
    ("LANDLOCK_ACCESS_FS_WRITE_FILE", 1 << 1),
    ("LANDLOCK_ACCESS_FS_READ_FILE", 1 << 2),
    ("LANDLOCK_ACCESS_FS_READ_DIR", 1 << 3),
    ("LANDLOCK_ACCESS_FS_REMOVE_DIR", 1 << 4),
    ("LANDLOCK_ACCESS_FS_REMOVE_FILE", 1 << 5),
    ("LANDLOCK_ACCESS_FS_MAKE_CHAR", 1 << 6),
    ("LANDLOCK_ACCESS_FS_MAKE_DIR", 1 << 7),
    ("LANDLOCK_ACCESS_FS_MAKE_REG", 1 << 8),
    ("LANDLOCK_ACCESS_FS_MAKE_SOCK", 1 << 9),
    ("LANDLOCK_ACCESS_FS_MAKE_FIFO", 1 << 10),
    ("LANDLOCK_ACCESS_FS_MAKE_BLOCK", 1 << 11),
    ("LANDLOCK_ACCESS_FS_MAKE_SYM", 1 << 12),
    ("LANDLOCK_ACCESS_FS_REFER", 1 << 13),
    ("LANDLOCK_ACCESS_FS_TRUNCATE", 1 << 14),
    ("LANDLOCK_ACCESS_FS_IOCTL_DEV", 1 << 15),
    ("LANDLOCK_ACCESS_FS_RESOLVE_UNIX", 1 << 16),
];

/// The constants this module actually uses, by the same names.
const OURS: &[(&str, u64)] = &[
    ("LANDLOCK_ACCESS_FS_EXECUTE", ACCESS_EXECUTE),
    ("LANDLOCK_ACCESS_FS_WRITE_FILE", ACCESS_WRITE_FILE),
    ("LANDLOCK_ACCESS_FS_READ_FILE", ACCESS_READ_FILE),
    ("LANDLOCK_ACCESS_FS_READ_DIR", ACCESS_READ_DIR),
    ("LANDLOCK_ACCESS_FS_REMOVE_DIR", ACCESS_REMOVE_DIR),
    ("LANDLOCK_ACCESS_FS_REMOVE_FILE", ACCESS_REMOVE_FILE),
    ("LANDLOCK_ACCESS_FS_MAKE_CHAR", ACCESS_MAKE_CHAR),
    ("LANDLOCK_ACCESS_FS_MAKE_DIR", ACCESS_MAKE_DIR),
    ("LANDLOCK_ACCESS_FS_MAKE_REG", ACCESS_MAKE_REG),
    ("LANDLOCK_ACCESS_FS_MAKE_SOCK", ACCESS_MAKE_SOCK),
    ("LANDLOCK_ACCESS_FS_MAKE_FIFO", ACCESS_MAKE_FIFO),
    ("LANDLOCK_ACCESS_FS_MAKE_BLOCK", ACCESS_MAKE_BLOCK),
    ("LANDLOCK_ACCESS_FS_MAKE_SYM", ACCESS_MAKE_SYM),
    ("LANDLOCK_ACCESS_FS_REFER", ACCESS_REFER),
    ("LANDLOCK_ACCESS_FS_TRUNCATE", ACCESS_TRUNCATE),
    ("LANDLOCK_ACCESS_FS_IOCTL_DEV", ACCESS_IOCTL_DEV),
    ("LANDLOCK_ACCESS_FS_RESOLVE_UNIX", ACCESS_RESOLVE_UNIX),
];

// ── the header audit: parser + coverage floor ────────────────────────

/// Where the kernel UAPI header lives when the machine has one.
const UAPI_HEADER_PATH: &str = "/usr/include/linux/landlock.h";

/// Parse every `#define LANDLOCK_ACCESS_FS_*  (1ULL << N)` out of header
/// text. Pure — no filesystem — so it can be driven with a synthetic
/// v6.8 header. The name is matched as a whole token, so the NET rights
/// (which reuse bit numbers 0..=3) cannot be mistaken for FS ones.
fn parse_uapi_rights(header_text: &str) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for line in header_text.lines() {
        let Some(rest) = line
            .trim_start()
            .strip_prefix("#define LANDLOCK_ACCESS_FS_")
        else {
            continue;
        };
        let Some(name_end) = rest.find(|c: char| c.is_whitespace()) else {
            continue;
        };
        let name = format!("LANDLOCK_ACCESS_FS_{}", &rest[..name_end]);
        let Some(bits) = rest[name_end..].split("<<").nth(1) else {
            continue;
        };
        let shift = bits
            .trim()
            .trim_end_matches(')')
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<u32>().ok());
        if let Some(shift) = shift.filter(|s| *s < 64) {
            out.push((name, 1u64 << shift));
        }
    }
    out
}

/// What an audit of one header text produced.
#[derive(Debug, Default)]
struct HeaderAudit {
    /// Rights found in the header AND compared against our constants.
    checked: usize,
    /// Ours that the header does not define — NOT failures, simply not
    /// checkable on that header version (e.g. `IOCTL_DEV`, which a v6.8
    /// `linux-libc-dev` does not have).
    not_checkable: Vec<&'static str>,
    /// Everything that IS a failure: a bit mismatch, or an ABI-1 right
    /// missing from a header that defines Landlock at all.
    problems: Vec<String>,
}

/// The rights that have been in the UAPI header since Landlock landed
/// (ABI 1, bits 0..=12): a header that defines `linux/landlock.h` at all
/// MUST define these, so their absence is a broken header, not an old
/// one. This is the coverage floor that keeps the audit from degenerating
/// into checking nothing.
const ABI1_FLOOR: &[&str] = &[
    "LANDLOCK_ACCESS_FS_EXECUTE",
    "LANDLOCK_ACCESS_FS_WRITE_FILE",
    "LANDLOCK_ACCESS_FS_READ_FILE",
    "LANDLOCK_ACCESS_FS_READ_DIR",
    "LANDLOCK_ACCESS_FS_REMOVE_DIR",
    "LANDLOCK_ACCESS_FS_REMOVE_FILE",
    "LANDLOCK_ACCESS_FS_MAKE_CHAR",
    "LANDLOCK_ACCESS_FS_MAKE_DIR",
    "LANDLOCK_ACCESS_FS_MAKE_REG",
    "LANDLOCK_ACCESS_FS_MAKE_SOCK",
    "LANDLOCK_ACCESS_FS_MAKE_FIFO",
    "LANDLOCK_ACCESS_FS_MAKE_BLOCK",
    "LANDLOCK_ACCESS_FS_MAKE_SYM",
];

/// Compare our constants against parsed header rights, honouring the
/// coverage floor. Returns the problems rather than panicking so the
/// synthetic cases can assert on them.
fn audit_uapi_rights(header_text: &str) -> HeaderAudit {
    let parsed = parse_uapi_rights(header_text);
    let mut audit = HeaderAudit::default();
    for (name, ours) in OURS {
        let Some((_, theirs)) = parsed.iter().find(|(n, _)| n == name) else {
            // Not defined: an older header. Only a failure if this right
            // has been in the header since Landlock landed.
            if ABI1_FLOOR.contains(name) {
                audit.problems.push(format!(
                    "{name} is an ABI 1 right (it has been in the UAPI header \
                     since Landlock landed) but this header does not define it \
                     — the header is broken or truncated"
                ));
            } else {
                audit.not_checkable.push(name);
            }
            continue;
        };
        if ours != theirs {
            audit.problems.push(format!(
                "{name}: our constant is {ours:#x} but the header says {theirs:#x} \
                 (bit {})",
                theirs.trailing_zeros()
            ));
        }
        audit.checked += 1;
    }
    audit
}

/// Audit a header AND say out loud what could not be checked — a partial
/// audit must never look like a full pass (this module's `SKIP-SANDBOX` /
/// `SANDBOX-COVERAGE` convention).
fn audit_rights_against_header(header_text: &str) -> HeaderAudit {
    let audit = audit_uapi_rights(header_text);
    if !audit.not_checkable.is_empty() {
        eprintln!(
            "SKIP-UAPI: {} right(s) absent from this kernel header so NOT checkable \
             here: {} — {} right(s) WERE compared to the header",
            audit.not_checkable.len(),
            audit.not_checkable.join(", "),
            audit.checked
        );
    } else {
        eprintln!(
            "UAPI-COVERAGE: all {} right(s) compared to the header, 0 not checkable",
            audit.checked
        );
    }
    audit
}

/// A synthetic header body, in the exact shape the real one has
/// (`#define NAME<TAB>(1ULL << N)`), including the NET rights — which
/// share bit numbers with the FS ones and MUST NOT be matched.
fn synthetic_header(defines: &[(&str, u32)]) -> String {
    let mut text = String::from(
        "/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */\n\
         #ifndef _LINUX_LANDLOCK_H\n#define _LINUX_LANDLOCK_H\n\n",
    );
    for (name, shift) in defines {
        text.push_str(&format!("#define {name}\t\t\t(1ULL << {shift})\n"));
    }
    text.push_str(
        "\n#define LANDLOCK_ACCESS_NET_BIND_TCP\t\t\t(1ULL << 0)\n\
         #define LANDLOCK_ACCESS_NET_CONNECT_TCP\t\t\t(1ULL << 1)\n\
         #endif /* _LINUX_LANDLOCK_H */\n",
    );
    text
}

/// The defines a v6.8 (Ubuntu 24.04 `linux-libc-dev`) header has: every
/// right up to `TRUNCATE`, no `IOCTL_DEV` (v6.10) and no `RESOLVE_UNIX`.
fn v6_8_defines() -> Vec<(&'static str, u32)> {
    OURS.iter()
        .filter(|(n, _)| {
            !matches!(
                *n,
                "LANDLOCK_ACCESS_FS_IOCTL_DEV" | "LANDLOCK_ACCESS_FS_RESOLVE_UNIX"
            )
        })
        .map(|(n, b)| (*n, b.trailing_zeros()))
        .collect()
}

/// (a) A v6.8-style header — the CI header — passes: the 15 rights it
/// defines (the 13 ABI-1 ones plus REFER and TRUNCATE) are compared, and
/// the 2 newer ones are reported as not checkable rather than failed.
#[test]
fn an_older_uapi_header_is_audited_for_what_it_defines() {
    let text = synthetic_header(&v6_8_defines());
    let audit = audit_rights_against_header(&text);
    assert!(
        audit.problems.is_empty(),
        "a v6.8 header must pass, got: {:?}",
        audit.problems
    );
    assert_eq!(
        audit.checked,
        OURS.len() - 2,
        "13 ABI-1 rights + REFER + TRUNCATE must be checked"
    );
    assert_eq!(
        audit.not_checkable,
        vec![
            "LANDLOCK_ACCESS_FS_IOCTL_DEV",
            "LANDLOCK_ACCESS_FS_RESOLVE_UNIX"
        ],
        "the two rights a v6.8 header lacks must be reported, not failed"
    );
}

/// (b) THE load-bearing case: the bug this guard exists for is a wrong
/// BIT, and it must still be caught. `READ_FILE` declared as `1 << 0`
/// is exactly the mistranscription that shipped once.
#[test]
fn a_mistranscribed_bit_in_the_header_fails_the_audit() {
    let mut defines = v6_8_defines();
    for (name, shift) in defines.iter_mut() {
        if *name == "LANDLOCK_ACCESS_FS_READ_FILE" {
            *shift = 0; // the original bug: READ_FILE transcribed as bit 0
        }
    }
    let audit = audit_rights_against_header(&synthetic_header(&defines));
    assert_eq!(audit.problems.len(), 1, "got: {:?}", audit.problems);
    let problem = &audit.problems[0];
    assert!(
        problem.contains("LANDLOCK_ACCESS_FS_READ_FILE"),
        "the failure must name the right, got: {problem}"
    );
    assert!(
        problem.contains("0x4") && problem.contains("0x1"),
        "the failure must show BOTH bits (ours 0x4, header 0x1), got: {problem}"
    );
}

/// (c) The coverage floor: a header that defines Landlock but not the
/// rights that have existed since ABI 1 is a broken header, and an audit
/// that quietly checks less must never go green.
#[test]
fn a_header_missing_an_abi_1_right_fails_the_coverage_floor() {
    let defines: Vec<(&str, u32)> = v6_8_defines()
        .into_iter()
        .filter(|(n, _)| *n != "LANDLOCK_ACCESS_FS_READ_DIR")
        .collect();
    let audit = audit_rights_against_header(&synthetic_header(&defines));
    assert!(
        audit
            .problems
            .iter()
            .any(|p| p.contains("LANDLOCK_ACCESS_FS_READ_DIR") && p.contains("ABI 1")),
        "a missing ABI-1 right must be a problem, got: {:?}",
        audit.problems
    );
}

/// (d) The REAL header on this machine must AGREE with our constants, and
/// must carry the whole ABI 1 floor. What it must NOT be required to do is
/// define the NEWER rights — that is a fact about the machine, not about
/// this table, and asserting it here is exactly how a test passes on a
/// dev box and reddens CI (Ubuntu 24.04's v6.8 header has no `IOCTL_DEV`).
/// Full 17-right coverage is proven where it belongs: deterministically,
/// against a synthetic current header in
/// [`a_current_uapi_header_checks_every_right`].
#[test]
fn the_installed_uapi_header_agrees_with_our_constants() {
    let Ok(text) = std::fs::read_to_string(UAPI_HEADER_PATH) else {
        eprintln!(
            "NOTE: no {UAPI_HEADER_PATH} on this machine — the header audit \
             ran only on synthetic headers"
        );
        return;
    };
    let audit = audit_rights_against_header(&text);
    assert!(
        audit.problems.is_empty(),
        "our constants disagree with {UAPI_HEADER_PATH}: {:?}",
        audit.problems
    );
    assert!(
        audit.checked >= ABI1_FLOOR.len(),
        "{UAPI_HEADER_PATH} let only {} right(s) be checked; every right \
         present since ABI 1 ({}) must be checkable, or the audit proves \
         nothing (not checkable: {:?})",
        audit.checked,
        ABI1_FLOOR.len(),
        audit.not_checkable
    );
}

/// A CURRENT header — one that defines every right we know — lets the
/// audit check ALL of them and skip nothing. Synthetic, so this holds on
/// every machine: the assertion that used to live on the installed header
/// silently turned the dev box's newer `linux-libc-dev` into a green CI
/// and an older runner into a red one.
#[test]
fn a_current_uapi_header_checks_every_right() {
    let mut defines: Vec<(&str, u32)> = v6_8_defines();
    defines.push(("LANDLOCK_ACCESS_FS_IOCTL_DEV", 15));
    defines.push(("LANDLOCK_ACCESS_FS_RESOLVE_UNIX", 16));
    let audit = audit_uapi_rights(&synthetic_header(&defines));
    assert!(
        audit.problems.is_empty(),
        "a current header must agree, got {:?}",
        audit.problems
    );
    assert_eq!(
        audit.checked,
        OURS.len(),
        "a header defining every right must let every one be checked"
    );
    assert!(audit.not_checkable.is_empty());
}

/// THE regression test for the mistranscribed table: the hand-rolled
/// `ACCESS_*` bits are the ABI, and a wrong one does not fail loudly —
/// it enforces a different (weaker) right set while the ruleset still
/// builds. When the UAPI header is on this machine, EVERY right it
/// defines is parsed from it and compared; the table is ALSO checked
/// against the transcribed copy above, so this test never skips.
///
/// An OLD header checks FEWER rights, and that is not a failure: Linux
/// v6.8 (Ubuntu 24.04's `linux-libc-dev`) has no `IOCTL_DEV` (added in
/// v6.10) and no `RESOLVE_UNIX`, so those two are simply not checkable
/// there — the test says so out loud (`SKIP-UAPI:`) instead of passing
/// partially in silence. What keeps "fewer" honest is [`ABI1_FLOOR`]:
/// every right present since ABI 1 MUST be found and compared, or the
/// header is broken and the test fails.
#[test]
fn the_access_right_table_matches_the_kernel_uapi_header() {
    // (a) our constants vs. the transcribed UAPI values: always asserted.
    assert_eq!(
        OURS.len(),
        UAPI_EXPECTED.len(),
        "the two tables must list the same rights"
    );
    for (name, want) in UAPI_EXPECTED {
        let (_, got) = OURS
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("{name} is missing from the module's constants"));
        assert_eq!(got, want, "{name}: the module's bit is not the UAPI value");
    }
    // (b) no duplicate bits, and the handled mask is exactly the union.
    for (i, (name_i, bit_i)) in OURS.iter().enumerate() {
        for (name_j, bit_j) in OURS.iter().skip(i + 1) {
            assert_eq!(bit_i & bit_j, 0, "{name_i} and {name_j} share a bit");
        }
    }
    let union = OURS.iter().fold(0u64, |m, (_, b)| m | b);
    assert_eq!(union, ALL_KNOWN_ACCESS, "ALL_KNOWN_ACCESS is not the union");

    // (c) the real header, when this machine has it: compare whatever it
    // defines, and never pretend the undefined ones were checked.
    let header = match std::fs::read_to_string(UAPI_HEADER_PATH) {
        Ok(text) => text,
        Err(_) => {
            eprintln!(
                "NOTE: no {UAPI_HEADER_PATH} here — checked the transcribed \
                 table only"
            );
            return;
        }
    };
    let audit = audit_rights_against_header(&header);
    assert!(
        audit.problems.is_empty(),
        "our constants disagree with {UAPI_HEADER_PATH}: {:?}",
        audit.problems
    );
}

/// The ABI mask: a bit beyond the kernel's ABI fails ruleset creation
/// `EAGAIN`, so the handled set must be derived from the probe. These
/// floors are the UAPI's own (REFER 2, TRUNCATE 3, IOCTL_DEV 5,
/// RESOLVE_UNIX 9).
#[test]
fn the_handled_mask_follows_the_probed_abi() {
    assert_eq!(
        handled_access_for(1),
        (1u64 << 13) - 1,
        "ABI 1 must handle exactly bits 0..=12"
    );
    assert_eq!(handled_access_for(2) & ACCESS_REFER, ACCESS_REFER);
    assert_eq!(handled_access_for(2) & ACCESS_TRUNCATE, 0);
    assert_eq!(handled_access_for(3) & ACCESS_TRUNCATE, ACCESS_TRUNCATE);
    assert_eq!(handled_access_for(4) & ACCESS_IOCTL_DEV, 0);
    assert_eq!(handled_access_for(5) & ACCESS_IOCTL_DEV, ACCESS_IOCTL_DEV);
    // `RESOLVE_UNIX` is never handled (module doc), even on a kernel
    // that supports it.
    assert_eq!(handled_access_for(99) & ACCESS_RESOLVE_UNIX, 0);
    assert_eq!(
        handled_access_for(99),
        ALL_KNOWN_ACCESS & !ACCESS_RESOLVE_UNIX
    );
    // And the mask must be accepted by THIS kernel (the probe and the
    // mask cannot disagree, or every confined run fails `EAGAIN`).
    let abi = ruleset_abi_version();
    if abi > 0 {
        let mask = handled_access_for(abi as u32);
        let attr = LandlockRulesetAttr {
            handled_access_fs: mask,
        };
        // SAFETY: `attr` is a live attr of the given size.
        let fd = unsafe { create_ruleset(&attr, std::mem::size_of::<LandlockRulesetAttr>(), 0) };
        assert!(
            fd >= 0,
            "the ABI-masked handle set was rejected ({}): the mask is out of sync with the kernel",
            std::io::Error::last_os_error()
        );
        // SAFETY: `fd` is the ruleset fd we just created.
        unsafe { libc::close(fd) };
    }
}

// ── the device nodes ─────────────────────────────────────────────────

/// The devices that are granted READ + WRITE (+ `IOCTL_DEV` where the fd
/// is a device) are ONLY the ones an ordinary command redirects to.
/// `/dev/tty` is NOT one of them, and must never be added back: a
/// confined child shares the Worker's controlling terminal (`exec.rs`
/// spawns with `process_group(0)`, which does not detach the ctty, and
/// pipes only stdout/stderr — stdin is inherited), so `WRITE_FILE` on
/// `/dev/tty` is a direct write channel to the USER's terminal (escape
/// sequences, screen/mouse-state manipulation) and, on kernels < 6.2,
/// `TIOCSTI` keystroke INJECTION via `IOCTL_DEV`. The grant buys nothing:
/// the child's own stdio is piped, so it never opens `/dev/tty` to
/// function. A READ grant is equally unnecessary — `/dev` is already
/// ruled read+execute.
#[test]
fn the_write_granted_devices_exclude_the_controlling_terminal() {
    assert!(
        !READ_WRITE_DEVICES.contains(&"/dev/tty"),
        "/dev/tty must not be in the read-write device list: it is the user's \
         terminal, and WRITE_FILE|IOCTL_DEV on it lets a sandboxed command write \
         escape sequences to it or inject keystrokes (TIOCSTI, kernels < 6.2). \
         List is {READ_WRITE_DEVICES:?}"
    );
    // The nodes ordinary commands genuinely need write access to (a shell
    // redirects to `/dev/null`; `git --version` fails outright without it)
    // must stay granted, or this "fix" is just a broken list.
    for node in [
        "/dev/null",
        "/dev/zero",
        "/dev/full",
        "/dev/random",
        "/dev/urandom",
    ] {
        assert!(
            READ_WRITE_DEVICES.contains(&node),
            "{node} must stay read-write granted — ordinary commands need it"
        );
    }
}

// ── availability ─────────────────────────────────────────────────────

/// The probe is the kernel's answer, and it must agree with whether a
/// ruleset can actually be built: Settings greys the option out from
/// exactly this signal, so a probe that lies in EITHER direction is a
/// bug (offering a tier that would fail, or hiding one that works).
#[test]
fn the_availability_probe_reflects_the_syscall_result() {
    let probe = landlock_available();
    let built = Sandbox::create(Path::new("/")).is_ok();
    assert_eq!(probe, built, "the probe and a ruleset build disagree");
}

/// The always-visible verdict line: either the behavioral tests below
/// ran on this kernel, or the run contains `SKIP-SANDBOX` markers.
/// Green + skips means UNVERIFIED, and this is how you tell from the
/// output without reading the code.
#[test]
fn sandbox_coverage_is_reported() {
    let skips = SKIPPED.load(Ordering::SeqCst);
    if landlock_available() && skips == 0 {
        println!(
            "SANDBOX-COVERAGE: ENFORCED — Landlock ABI {}, 0 behavioral tests skipped",
            ruleset_abi_version()
        );
    } else if !landlock_available() {
        println!(
            "SANDBOX-COVERAGE: NONE — this kernel has no Landlock, the confinement \
             tests above SKIPPED (see SKIP-SANDBOX markers); only the fail-closed \
             path was verified"
        );
    } else {
        println!(
            "SANDBOX-COVERAGE: PARTIAL — {skips} behavioral test(s) SKIPPED \
             (SKIP-SANDBOX markers above); the rest were enforced on ABI {}",
            ruleset_abi_version()
        );
    }
}

/// The PREMISE of every behavioral test in this module: the base the
/// fixtures are rooted in is NOT inside the sandbox's own write
/// allow-list, so a successful denial is a denial and not an artefact of
/// the fixture's location. `/tmp` and `/var/tmp` are write-granted to a
/// confined child (`TEMP_DIRS` is fed to the ruleset as write roots), so a
/// fixture rooted there would see its "denial" succeed because the write
/// was ALLOWED — ~13 confinement tests turned into green no-ops.
///
/// The assertion ties to the real `TEMP_DIRS` const, so making the
/// fixtures vacuous by editing the ruleset (adding `/dev/shm` to that
/// const is the only way) reddens THIS test instead of silencing the
/// suite. Runs on kernels without Landlock — the claim is structural, not
/// behavioral — and skips loudly, without touching `SKIPPED` (the same
/// shape `the_same_command_writes_freely_when_the_sandbox_is_off` uses),
/// when there is no `/dev/shm`.
#[test]
fn the_scratch_base_sits_outside_the_sandbox_write_allow_list() {
    let Some(base) = test_base() else {
        eprintln!("SKIP-SANDBOX: no /dev/shm on this machine");
        return;
    };
    for tmp in TEMP_DIRS {
        assert!(
            !base.starts_with(tmp),
            "the scratch base {base:?} is inside the sandbox write allow-list \
             (`TEMP_DIRS` holds `{tmp}`) — every confinement test below would \
             now pass VACUOUSLY"
        );
    }
    assert!(
        base.starts_with("/dev"),
        "the scratch base {base:?} must sit under `/dev`, which the ruleset \
         grants READ (REQUIRED_READ_DIRS) but never WRITE"
    );
    let d = TmpDir::new("base-proof").expect("base exists");
    for tmp in TEMP_DIRS {
        assert!(
            !d.path().starts_with(tmp),
            "a sandbox scratch dir landed inside the write allow-list: {:?}",
            d.path()
        );
    }
}

// ── confinement (behavioral; skipped on a kernel without Landlock) ───

/// THE point of the tier: a confined command cannot WRITE outside the
/// write boundary. The target is a dir the test process OWNS (a
/// root-owned path would pass vacuously — `EACCES` there would prove
/// nothing about the sandbox).
#[tokio::test]
async fn a_confined_command_cannot_write_outside_the_boundary() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("out-write").expect("base exists");
    let outside = TmpDir::new("out-target").expect("base exists");
    let target = format!("{}/payload.txt", outside.display());
    let r = run(
        &confined_ctx(cwd.path(), AccessPolicy::Allow),
        &format!("echo pwn > {target}"),
    )
    .await;
    assert!(r.is_error, "the write must fail: {:?}", text_of(&r));
    assert!(
        !outside.path().join("payload.txt").exists(),
        "the payload must not have landed outside the boundary"
    );
    let _ = base;
}

/// READS are confined too — this is the half the mistranscribed table
/// silently left UNENFORCED (`READ_FILE` was never in the handled mask,
/// so every read outside the allow-list worked: `cat /etc/passwd`,
/// `cat ~/.bashrc`). Target: a file in a dir the test process owns.
#[tokio::test]
async fn a_confined_command_cannot_read_outside_the_allow_list() {
    let base = require_sandbox!();
    let outside = match private_dir_outside_the_read_allow_list() {
        Some(dir) => dir,
        None => {
            eprintln!("SKIP-SANDBOX: no test-owned dir outside the read allow-list");
            SKIPPED.fetch_add(1, Ordering::SeqCst);
            return;
        }
    };
    let cwd = TmpDir::new("out-read").expect("base exists");
    let secret_name = format!("archimedes-sandbox-secret-{}", uuid::Uuid::new_v4());
    let secret_path = outside.join(&secret_name);
    std::fs::write(&secret_path, "DO-NOT-READ").unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = Cleanup(secret_path.clone());
    let secret = secret_path.display().to_string();
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    // (a) the payload must not come back…
    let r = run(&ctx, &format!("cat {secret}")).await;
    assert!(r.is_error, "the read must fail: {:?}", text_of(&r));
    assert!(
        !text_of(&r).contains("DO-NOT-READ"),
        "the confined child leaked the file content: {:?}",
        text_of(&r)
    );
    // (b) …and the CONTRAST, so this cannot pass vacuously: the same
    // `cat` with the sandbox OFF reads it.
    let loose = ToolCtx {
        file_policy: FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Allow,
            rules: Vec::new(),
        },
        ..confined_ctx(cwd.path(), AccessPolicy::Allow)
    };
    let r = run(&loose, &format!("cat {secret}")).await;
    assert!(
        !r.is_error && text_of(&r).contains("DO-NOT-READ"),
        "the file must be readable when the sandbox is off: {:?}",
        text_of(&r)
    );
    // (c) and the very same confined child still reads its OWN file, so
    // the denial above is about location, not about `cat` being broken.
    std::fs::write(cwd.path().join("mine.txt"), "MINE").unwrap();
    let r = run(&ctx, "cat mine.txt").await;
    assert!(
        !r.is_error && text_of(&r).contains("MINE"),
        "a read inside the boundary still works: {:?}",
        text_of(&r)
    );
    let _ = base;
}

/// `TRUNCATE` (ABI 3+) must be HANDLED and granted only inside the
/// boundary. Unhandled, `truncate(2)` by path is a write primitive that
/// works anywhere DAC allows — including emptying a file outside the
/// session. Inside the boundary it must still work (`echo x > f`
/// truncates, so a missing grant breaks ordinary shell use).
#[tokio::test]
async fn a_confined_command_cannot_truncate_outside_the_boundary() {
    let base = require_sandbox!();
    if ruleset_abi_version() < 3 {
        eprintln!(
            "SKIP-SANDBOX: truncate needs Landlock ABI 3 (this kernel is {})",
            ruleset_abi_version()
        );
        SKIPPED.fetch_add(1, Ordering::SeqCst);
        return;
    }
    let cwd = TmpDir::new("out-trunc").expect("base exists");
    // The enforcement-relevant half, asserted directly: on a kernel that
    // knows `TRUNCATE`, the ruleset must HANDLE it (an unhandled right is
    // simply not enforced) and must GRANT it inside the boundary (without
    // the grant, `echo x > existing_file` — which truncates — breaks).
    // The behavioral probe below cannot isolate this: a `truncate(2)`
    // outside the boundary is already refused by the missing
    // `WRITE_FILE`, so it stays green either way.
    let handled = handled_access_for(ruleset_abi_version() as u32);
    assert_ne!(
        handled & ACCESS_TRUNCATE,
        0,
        "ABI {} knows TRUNCATE, so the ruleset must handle it",
        ruleset_abi_version()
    );
    // (A missing GRANT is caught behaviorally below: `echo new > f.txt`
    // on an existing file needs `O_TRUNC`.)
    let outside = TmpDir::new("trunc-target").expect("base exists");
    std::fs::write(outside.path().join("data.txt"), "KEEP-ME").unwrap();
    let target = format!("{}/data.txt", outside.display());
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    for cmd in [format!("truncate -s 0 {target}"), format!(": > {target}")] {
        let r = run(&ctx, &cmd).await;
        assert!(r.is_error, "{cmd} must fail: {:?}", text_of(&r));
        assert_eq!(
            std::fs::read_to_string(outside.path().join("data.txt")).unwrap(),
            "KEEP-ME",
            "{cmd} truncated a file outside the boundary"
        );
    }
    // Inside the boundary, truncating an existing file is ordinary use.
    std::fs::write(cwd.path().join("f.txt"), "OLD-CONTENT").unwrap();
    let r = run(&ctx, "echo new > f.txt && cat f.txt").await;
    assert!(
        !r.is_error && text_of(&r).contains("new"),
        "overwriting an existing file inside the boundary must work: {:?}",
        text_of(&r)
    );
    let _ = base;
}

/// `MAKE_SYM` must be HANDLED: unhandled, a confined child plants a
/// symlink ANYWHERE DAC allows (`ln -s /etc/passwd ~/.somefile`), which
/// the later unsandboxed `read`/`write` tools then follow out of the
/// boundary.
#[tokio::test]
async fn a_confined_command_cannot_plant_a_symlink_outside_the_boundary() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("out-ln").expect("base exists");
    let outside = TmpDir::new("ln-target").expect("base exists");
    let link = format!("{}/planted", outside.display());
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    let r = run(&ctx, &format!("ln -s /etc/passwd {link}")).await;
    assert!(r.is_error, "the symlink must fail: {:?}", text_of(&r));
    assert!(
        !outside.path().join("planted").exists(),
        "a symlink was planted outside the boundary"
    );
    // And a hardlink (MAKE_REG + REFER) is equally refused.
    std::fs::write(outside.path().join("victim"), "v").unwrap();
    let r = run(&ctx, &format!("ln victim {}/hard", outside.display())).await;
    assert!(
        r.is_error && !outside.path().join("hard").exists(),
        "a hardlink was created outside the boundary: {:?}",
        text_of(&r)
    );
    // CONTRAST: the same `ln -s` inside the boundary works (so the deny
    // above is about the location, not about `ln` missing).
    let r = run(&ctx, "ln -s /etc/passwd mylink && ls -l mylink").await;
    assert!(
        !r.is_error && text_of(&r).contains("mylink"),
        "symlinks inside the boundary must work: TEXT={:?} DETAILS={:?} EXIT={:?}",
        text_of(&r),
        r.details,
        r.content.len()
    );
    let _ = base;
}

/// Whether THIS process can open its controlling terminal for writing.
/// The confined child INHERITS the Worker's controlling terminal (see
/// [`the_write_granted_devices_exclude_the_controlling_terminal`]), so
/// this probe is the confined child's reachability too — and, like the
/// other helpers, it asks what the UNCONFINED process can do, so a later
/// denial proves Landlock did it and not DAC. `O_NOCTTY`: probing must
/// never make the test process acquire a terminal it did not have. Under
/// `cargo test` with no ctty the open fails `ENXIO` and the behavioral
/// test below skips loudly.
fn controlling_terminal_is_writeable() -> bool {
    let path = CString::new("/dev/tty").unwrap();
    // SAFETY: `path` is a live NUL-terminated path; `open` only resolves it.
    let fd = unsafe { libc::open(path.as_ptr(), libc::O_WRONLY | libc::O_NOCTTY) };
    if fd < 0 {
        return false;
    }
    // SAFETY: `fd` is the fd we just opened.
    unsafe { libc::close(fd) };
    true
}

/// THE tty hole: `echo x > /dev/tty` from a confined command must NOT
/// reach the terminal. Without the fix the confined child inherits the
/// Worker's ctty, `/dev/tty` is write-granted, and the redirect succeeds
/// — a sandboxed command writing raw escape sequences into the user's
/// terminal (and `ioctl(TIOCSTI)` keystroke injection on kernels < 6.2).
/// The CONTRAST half (an unconfined run of the same command) proves the
/// refusal is the sandbox and not a machine without a usable tty.
#[tokio::test]
async fn a_confined_command_cannot_write_to_the_controlling_terminal() {
    let base = require_sandbox!();
    if !controlling_terminal_is_writeable() {
        eprintln!(
            "SKIP-SANDBOX: no controlling terminal writable here (`/dev/tty` is \
             not openable) — skip #{}",
            SKIPPED.fetch_add(1, Ordering::SeqCst) + 1
        );
        return;
    }
    let cwd = TmpDir::new("tty-write").expect("base exists");
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    // Plain text only: the command must fail, so nothing is written to
    // the real terminal either way (and a stray byte is harmless).
    let r = run(&ctx, "echo x > /dev/tty").await;
    assert!(
        r.is_error,
        "a confined command wrote to the controlling terminal: {:?}",
        text_of(&r)
    );
    // CONTRAST: the same command unconfined succeeds, so the refusal
    // above is the ruleset (the probe already proved DAC allows it).
    let loose = ToolCtx {
        file_policy: FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Allow,
            rules: Vec::new(),
        },
        ..confined_ctx(cwd.path(), AccessPolicy::Allow)
    };
    let r = run(&loose, "echo x > /dev/tty").await;
    assert!(
        !r.is_error,
        "the unconfined run must be able to open /dev/tty (the tty probe lies): {:?}",
        text_of(&r)
    );
    // And the confined child is not crippled: `/dev/null` — a node it
    // genuinely needs — still works.
    let r = run(&ctx, "echo ok > /dev/null && echo ALL_OK").await;
    assert!(
        !r.is_error && text_of(&r).contains("ALL_OK"),
        "/dev/null must stay writable for a confined command: {:?}",
        text_of(&r)
    );
    let _ = base;
}

/// CONTRAST (so the write test above cannot pass vacuously): the SAME
/// command with `shell: Allow` writes the file.
#[tokio::test]
async fn the_same_command_writes_freely_when_the_sandbox_is_off() {
    let Some(base) = test_base() else {
        eprintln!("SKIP-SANDBOX: no /dev/shm on this machine");
        return;
    };
    let cwd = TmpDir::new("free-write").expect("base exists");
    let outside = TmpDir::new("free-target").expect("base exists");
    let ctx = ToolCtx {
        file_policy: FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Allow,
            rules: Vec::new(),
        },
        ..confined_ctx(cwd.path(), AccessPolicy::Allow)
    };
    let r = run(
        &ctx,
        &format!("echo pwn > {}/payload.txt", outside.display()),
    )
    .await;
    assert!(
        !r.is_error,
        "an unconfined run writes anywhere: {:?}",
        text_of(&r)
    );
    assert!(outside.path().join("payload.txt").exists());
    let _ = base;
}

/// Inside the boundary a confined command works NORMALLY — create,
/// overwrite, rename, unlink, and temp files.
#[tokio::test]
async fn a_confined_command_works_normally_inside_the_boundary() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("in-write").expect("base exists");
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    let r = run(
        &ctx,
        "echo hi > inside.txt && cat inside.txt && mkdir -p sub && \
         mv inside.txt sub/moved.txt && rm -f sub/gone.txt; \
         TMPDIR=/tmp mktemp >/dev/null && echo ALL_OK",
    )
    .await;
    assert!(
        !r.is_error,
        "inside the boundary it runs: {:?}",
        text_of(&r)
    );
    assert!(text_of(&r).contains("hi"));
    assert!(text_of(&r).contains("ALL_OK"));
    assert!(cwd.path().join("sub/moved.txt").exists());
    let _ = base;
}

/// The dynamic linker and the system reads still work: the ruleset
/// exists to confine the CHILD, and a shell that cannot `ld.so` or read
/// `/usr` is not a shell. The doc file is discovered at run time
/// (package names are distro-dependent).
#[tokio::test]
async fn a_confined_command_still_reads_the_system_dirs() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("sys-read").expect("base exists");
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    // The doc file is discovered INSIDE the sandbox (package names are
    // distro-dependent), and the host is checked for the same glob so
    // the test cannot pass vacuously on the "no docs here" branch.
    let docs_exist = std::fs::read_dir("/usr/share/doc")
        .map(|entries| {
            entries
                .flatten()
                .any(|e| e.path().join("copyright").is_file())
        })
        .unwrap_or(false);
    let r = run(
        &ctx,
        "f=$(ls /usr/share/doc/*/copyright 2>/dev/null | head -1); \
         test -n \"$f\" || { echo NO_DOC_ON_THIS_SYSTEM; exit 0; }; \
         head -c 40 \"$f\" >/dev/null && echo DOC_READ_OK",
    )
    .await;
    assert!(!r.is_error, "system reads must work: {:?}", text_of(&r));
    if docs_exist {
        assert!(
            text_of(&r).contains("DOC_READ_OK"),
            "a /usr/share/doc/*/copyright exists on this host, so the confined read must succeed: {:?}",
            text_of(&r)
        );
    } else {
        assert!(text_of(&r).contains("NO_DOC_ON_THIS_SYSTEM"));
    }
    // And the shell itself still runs (the loader path + builtins).
    let r = run(&ctx, "echo hi").await;
    assert_eq!(text_of(&r), "hi\n");
    let r = run(
        &ctx,
        "for i in 1 2; do printf '%s' \"$i\"; done; echo; true",
    )
    .await;
    assert!(!r.is_error, "builtins work: {:?}", text_of(&r));
    let _ = base;
}

/// A confined shell is still a SHELL: `/dev/null` read-write, a name
/// lookup, a TLS fetch, `git`, and temp files. Every one of these broke
/// the moment reads became genuinely confined (they need `/etc` files
/// the ruleset now grants explicitly), so this is the usability
/// regression test for the grants above.
#[tokio::test]
async fn a_confined_shell_can_still_do_ordinary_things() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("usable").expect("base exists");
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    // `/dev/null` + redirection + a temp file.
    let r = run(
        &ctx,
        "echo hi > ./f && test $(cat ./f) = hi && echo REDIRECT_OK; \
         echo to-null > /dev/null && echo DEVNULL_OK; \
         TMPDIR=/tmp mktemp >/dev/null && echo MKTEMP_OK",
    )
    .await;
    assert!(
        text_of(&r).contains("REDIRECT_OK")
            && text_of(&r).contains("DEVNULL_OK")
            && text_of(&r).contains("MKTEMP_OK"),
        "ordinary redirection must work: {:?}",
        text_of(&r)
    );
    // The resolver (`/etc/resolv.conf` + `nsswitch.conf` + `hosts`) and
    // the identity files (`passwd`): `git` and `whoami` are useless
    // without them.
    let r = run(
        &ctx,
        "getent hosts localhost > /dev/null && echo LOOKUP_OK; whoami | grep -q . && echo ID_OK",
    )
    .await;
    assert!(
        text_of(&r).contains("LOOKUP_OK") && text_of(&r).contains("ID_OK"),
        "name lookup and identity must work: {:?}",
        text_of(&r)
    );
    // `git` (the CA bundle is for the TLS check below, git's config for
    // this one: git EXITS 128 when `$HOME/.gitconfig` exists and cannot
    // be read, so a confined shell could not use its own repo).
    let git_ok = std::process::Command::new("git")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    let r = run(&ctx, "git --version > /dev/null && echo GIT_OK").await;
    if git_ok {
        assert!(
            text_of(&r).contains("GIT_OK"),
            "git must run under confinement: {:?}",
            text_of(&r)
        );
    }
    // TLS: needs the CA trust anchors. Skips (loudly) with no network.
    let online = std::process::Command::new("curl")
        .args(["-sS", "-m", "5", "-o", "/dev/null", "https://example.com"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if online {
        let r = run(
            &ctx,
            "curl -sS -m 10 -o /dev/null -w '%{http_code}' https://example.com",
        )
        .await;
        assert!(
            !r.is_error && text_of(&r).starts_with("200"),
            "a confined TLS fetch must work (the CA bundle must be readable): {:?}",
            text_of(&r)
        );
    } else {
        eprintln!("SKIP-SANDBOX: no network for the confined TLS fetch");
        SKIPPED.fetch_add(1, Ordering::SeqCst);
    }
    let _ = base;
}

/// `/proc` is granted READ + EXECUTE — and the grant is LOAD-BEARING:
/// without a `/proc` rule a confined `ls /proc` is refused (`EACCES`),
/// which is exactly the kind of ordinary command the tier must not
/// break. `/proc` is deliberately given NO write rights: a writable
/// `/proc/self/…` is the classic escape hatch (`oom_score_adj`, the
/// `core_pattern`-adjacent paths).
///
/// The no-write half is asserted BEHAVIORALLY: writing the task's own
/// `oom_score_adj` is permitted by DAC (it is the caller's own file), so
/// the ONLY thing that can refuse it is the ruleset. With the previous
/// mistranscribed table the `READ_EXECUTE` mask carried the kernel's
/// real `WRITE_FILE` bit and this write SUCCEEDED (measured); the old
/// test asserted `READ_EXECUTE & FORBIDDEN == 0` — constants against
/// constants — and passed anyway.
#[tokio::test]
async fn proc_is_read_execute_only_and_stays_listable() {
    // The structural half: the mask `/proc` gets carries NO write bit.
    const FORBIDDEN: u64 = ACCESS_WRITE_FILE
        | ACCESS_REMOVE_FILE
        | ACCESS_MAKE_CHAR
        | ACCESS_MAKE_BLOCK
        | ACCESS_MAKE_REG
        | ACCESS_MAKE_FIFO
        | ACCESS_MAKE_SOCK
        | ACCESS_MAKE_SYM;
    assert_eq!(
        READ_EXECUTE & FORBIDDEN,
        0,
        "the mask used for /proc must carry no write right"
    );
    let base = require_sandbox!();
    let cwd = TmpDir::new("proc").expect("base exists");
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    let r = run(
        &ctx,
        "ls /proc > /dev/null && cat /proc/self/status > /dev/null && echo PROC_OK",
    )
    .await;
    assert!(
        !r.is_error && text_of(&r).contains("PROC_OK"),
        "/proc must stay listable+readable under confinement: {:?}",
        text_of(&r)
    );
    // The behavioral half: the task's OWN tunable, writable by DAC, must
    // be refused by the ruleset.
    let writable_by_dac = std::fs::OpenOptions::new()
        .write(true)
        .open("/proc/self/oom_score_adj")
        .is_ok();
    let r = run(&ctx, "echo 100 > /proc/self/oom_score_adj").await;
    if writable_by_dac {
        assert!(
            r.is_error,
            "/proc/self/oom_score_adj is DAC-writable, so only the ruleset \
             can refuse it — and it did NOT: {:?}",
            text_of(&r)
        );
    } else {
        eprintln!("SKIP-SANDBOX: /proc/self/oom_score_adj is not DAC-writable here");
        SKIPPED.fetch_add(1, Ordering::SeqCst);
    }
    let _ = base;
}

/// REAP UNDER CONFINEMENT: a backgrounded grandchild must die with the
/// process group on cancel. Registering a `pre_exec` closure forces
/// std's fork+exec path (it refuses `posix_spawn` with closures), so
/// `process_group(0)` must still make the child a group leader or the
/// negative-pid kill misses the grandchild — and the existing bash
/// tests never activate the sandbox, so they prove nothing here.
///
/// The survivor check is scoped to THIS run's own process GROUP (the
/// child reports its `pgid`, which every grandchild inherits): a
/// machine-wide count of `sleep 1000` reddens whenever anything else on
/// the box happens to sleep for 1000 seconds, and no argv marker works
/// here — `sleep 1000 # marker &` puts the `&` inside the comment (so
/// nothing is backgrounded and the test would pass vacuously) and
/// `sleep 1000 marker` is an invalid interval.
#[tokio::test]
async fn a_confined_backgrounded_grandchild_is_reaped_with_the_group() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("reap").expect("base exists");
    let token = CancellationToken::new();
    let ctx = ToolCtx {
        cancel: token.clone(),
        ..confined_ctx(cwd.path(), AccessPolicy::Allow)
    };
    // The child prints its own pgid — equal to its own pid only if
    // `process_group(0)` made it a GROUP LEADER, which is the exact
    // property under test — then backgrounds a grandchild that nothing
    // but the group kill can reach.
    let cmd = "sleep 1000 & ps -o pgid= -p $$ | tr -d ' ' | sed 's/^/PGID=/' && echo started";
    let handle =
        tokio::spawn(async move { execute_tool(&ctx, "bash", &json!({ "command": cmd })).await });
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    // Sample the group BEFORE the cancel: if the grandchild never joined
    // the group, the "no survivors" check below would pass vacuously.
    let before = group_snapshot();
    token.cancel();
    let r = tokio::time::timeout(std::time::Duration::from_secs(15), handle)
        .await
        .expect("the call returns (no unbounded wait for the pipes' EOF)")
        .unwrap();
    assert_eq!(r.details.as_ref().unwrap()["cancelled"], true);
    assert!(text_of(&r).contains("started"), "output is still captured");
    let pgid: i64 = text_of(&r)
        .lines()
        .find_map(|l| l.strip_prefix("PGID="))
        .and_then(|s| s.trim().parse().ok())
        .expect("the child reported its own pgid");
    let members = group_members(&before, pgid);
    // The non-vacuity check is the GRANDCHILD's own row, not a member
    // COUNT: the executor's read loop collects the child's exit status
    // as soon as it lands, so `sh` is already REAPED at this sample and
    // gone from `ps` — a count of 2 saw the grandchild only because the
    // child used to linger here as an unreaped zombie. The grandchild's
    // argv is exactly `sleep 1000`; the child's own row (`sh -c sleep
    // 1000 & …`) merely CONTAINS that substring and must not count.
    assert!(
        members
            .iter()
            .any(|m| m.split_whitespace().skip(2).eq(["sleep", "1000"])),
        "the backgrounded grandchild never shared the child's process group {pgid}: {members:?}"
    );
    let survivors = group_members(&group_snapshot(), pgid);
    assert!(
        survivors.is_empty(),
        "processes in the confined child's group {pgid} escaped the group kill: {survivors:?}"
    );
    let _ = base;
}

/// `ps` with the process GROUP first (for the scoped reap check above).
fn group_snapshot() -> String {
    let out = std::process::Command::new("ps")
        .args(["-eo", "pgid,pid,args"])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// The lines of a `ps -eo pgid,pid,args` snapshot in process group `pgid`.
fn group_members(snapshot: &str, pgid: i64) -> Vec<String> {
    snapshot
        .lines()
        .filter(|l| {
            l.split_whitespace()
                .next()
                .and_then(|g| g.parse::<i64>().ok())
                == Some(pgid)
        })
        .map(|l| l.to_string())
        .collect()
}

/// The confinement is CHILD-ONLY: the same session's `read` tool still
/// reads beyond the boundary (`reads: Allow`), and the test process
/// itself still writes there. If the ruleset ever leaked into the
/// Worker, every later tool call — and the desktop itself — would be
/// confined.
#[tokio::test]
async fn the_sandbox_confines_the_child_and_never_the_worker() {
    let base = require_sandbox!();
    let cwd = TmpDir::new("child-only").expect("base exists");
    let outside = TmpDir::new("child-only-target").expect("base exists");
    std::fs::write(outside.path().join("secret.txt"), "OUTSIDE").unwrap();
    let ctx = confined_ctx(cwd.path(), AccessPolicy::Allow);
    // First a confined run (it installs the ruleset in ITS child only)…
    let r = run(&ctx, "echo confined").await;
    assert!(!r.is_error, "the confined run works: {:?}", text_of(&r));
    // …then a NON-bash tool in the same ctx reads beyond the boundary.
    let r = execute_tool(
        &ctx,
        "read",
        &json!({ "path": format!("{}/secret.txt", outside.display()) }),
    )
    .await;
    assert!(
        !r.is_error,
        "the Worker must not be confined: {:?}",
        text_of(&r)
    );
    assert!(text_of(&r).contains("OUTSIDE"));
    // And the Worker process itself is unconfined.
    std::fs::write(outside.path().join("worker-wrote.txt"), "ok")
        .expect("the Worker is unconfined");
    let _ = base;
}

// ── the protected dirs ───────────────────────────────────────────────

/// THE exploit the write tools' deny-list exists to stop, at the
/// TIGHTEST tier: a session whose `cwd` IS a protected agent-definition
/// dir must NOT be able to rewrite a `SKILL.md` there. The old code
/// ruled `write_roots(cwd)` read-write unconditionally, so the deny-list
/// was reachable around through `bash`.
#[tokio::test]
async fn a_session_cwd_inside_a_protected_dir_cannot_write_there() {
    let base = require_sandbox!();
    // A PROTECTED dir, laid out exactly like the real one, under a
    // pinned `$HOME` (the `boundary` tests' `pin_home` pattern — never
    // the developer's real `~/.agents`). The pinned home is itself under
    // `/dev/shm`, so the test never touches a root-owned path.
    let pin = match PinnedHome::new("protected-home") {
        Some(pin) => pin,
        None => panic!("no /dev/shm"),
    };
    let skills = pin.path().join(".agents/skills/demo");
    std::fs::create_dir_all(&skills).unwrap();
    std::fs::write(skills.join("SKILL.md"), "original instructions").unwrap();
    // The protected set must actually contain the dir under test, or the
    // test proves nothing.
    assert!(
        crate::agent::boundary::protected_dirs()
            .iter()
            .any(|p| skills.starts_with(p)),
        "the pinned `$HOME` must produce a protected dir containing the skill"
    );
    // The session is rooted IN the protected dir — the tightest tier, and
    // the case the write tools' deny-list exists for.
    let ctx = confined_ctx(&skills, AccessPolicy::Allow);
    let r = run(&ctx, "echo PWNED > SKILL.md").await;
    assert!(
        r.is_error,
        "a confined write to a protected SKILL.md must fail: {:?}",
        text_of(&r)
    );
    assert_eq!(
        std::fs::read_to_string(skills.join("SKILL.md")).unwrap(),
        "original instructions",
        "the confined shell rewrote its own instructions (self-modification)"
    );
    // And the SUBTREE is protected, not just the leaf: creating a file in
    // a sibling dir of the protected tree is refused too.
    let sibling = pin.path().join(".agents/skills/other");
    std::fs::create_dir_all(&sibling).unwrap();
    let r = run(
        &ctx,
        &format!("echo PWNED > {}", sibling.join("x.txt").display()),
    )
    .await;
    assert!(
        r.is_error,
        "a protected subtree must not be writable: {:?}",
        text_of(&r)
    );
    // CONTRAST: the same `echo >` into a NON-protected session dir works,
    // so the refusals above are the protection, not a broken write path.
    let free = TmpDir::new("protected-contrast").expect("base exists");
    let r = run(
        &confined_ctx(free.path(), AccessPolicy::Allow),
        "echo ok > f.txt",
    )
    .await;
    assert!(
        !r.is_error && free.path().join("f.txt").exists(),
        "an ordinary session dir must be writable: {:?}",
        text_of(&r)
    );
    let _ = base;
}

/// A write root that merely CONTAINS a protected dir: the protected
/// subtree stays read-only while the rest of the tree keeps working
/// (Landlock cannot subtract, so the ancestors of the protected dir are
/// ruled read+execute and their siblings read-write — see
/// [`Sandbox::rule_write_root`]).
#[tokio::test]
async fn a_write_root_containing_a_protected_dir_stays_writable_around_it() {
    let base = require_sandbox!();
    let pin = match PinnedHome::new("carve-home") {
        Some(pin) => pin,
        None => panic!("no /dev/shm"),
    };
    let skills = pin.path().join(".agents/skills");
    std::fs::create_dir_all(&skills).unwrap();
    std::fs::write(skills.join("SKILL.md"), "original").unwrap();
    // The session is rooted at `$HOME` itself (a real setup: a session
    // opened on the home directory).
    std::fs::create_dir_all(pin.path().join("project")).unwrap();
    assert!(!crate::agent::boundary::protected_dirs().is_empty());
    let ctx = confined_ctx(pin.path(), AccessPolicy::Allow);
    // The protected file survives every write primitive.
    let skill = skills.join("SKILL.md");
    for cmd in [
        format!("echo PWNED > {}", skill.display()),
        format!("truncate -s 0 {}", skill.display()),
        format!("rm -f {}", skill.display()),
        format!("ln -s /etc/passwd {}", skills.join("planted").display()),
    ] {
        let r = run(&ctx, &cmd).await;
        assert!(r.is_error, "{cmd} must fail: {:?}", text_of(&r));
    }
    assert_eq!(
        std::fs::read_to_string(&skill).unwrap(),
        "original",
        "a protected file was modified through the containing write root"
    );
    // And the rest of the session tree is still usable.
    let r = run(&ctx, "cd project && echo hi > work.txt && cat work.txt").await;
    assert!(
        !r.is_error && text_of(&r).contains("hi"),
        "the writable part of the tree must work: {:?}",
        text_of(&r)
    );
    let _ = base;
}

/// (T03, spec: "rules never apply to `bash`") The Landlock subtraction
/// keeps using `boundary::protected_dirs()` — the four-dir GUARD-MEMBERSHIP
/// list, which is BY CONSTRUCTION unable to see a user rule — so a user
/// `allow` rule for a protected dir can NEVER widen a confined `bash`'s
/// write set.
///
/// This is the load-bearing half of the round-2 blocker's fix: while
/// `protected_dirs()` was derived from `default_rules().filter(Deny)`, a
/// change to the rule set could (and did, under the corrected reading) empty
/// the subtraction and hand the shell write access to the agent's own
/// instructions. It is a plain four-path list now, so no rule can reach it.
///
/// NOT VACUOUS: the contrast half applies the SAME rule to the RESOLVER and
/// asserts it DOES change the path tools' answer. Without that half,
/// "unchanged" would also hold if the list were never consulted at all.
#[test]
fn protected_dirs_ignore_user_rules_because_bash_never_applies_them() {
    let pin = match PinnedHome::new("rules-never-widen") {
        Some(pin) => pin,
        None => panic!("no /dev/shm"),
    };
    std::fs::create_dir_all(pin.path().join(".agents/skills")).unwrap();

    let before = crate::agent::boundary::protected_dirs();
    assert_eq!(
        before.len(),
        4,
        "the guard-membership list is the four dirs: {before:?}"
    );

    // A user `allow` rule naming one of the four — the exact exclusion a
    // rules-derived subtraction WOULD honor.
    let user_allow = crate::agent::policy::AccessRule {
        path: "~/.agents/skills".to_string(),
        direction: crate::agent::policy::RuleDirection::ReadWrite,
        decision: crate::agent::policy::RuleDecision::Allow,
    };
    let after = crate::agent::boundary::protected_dirs();
    assert_eq!(
        before, after,
        "a user rule must not change the Landlock subtraction list"
    );

    // THE CONTRAST (so the assertion above is not vacuous): the SAME rule
    // applied to the RESOLVER — as `default_rules() ++ [user_allow]`, the
    // concatenation contract — DOES decide the write. The path tools honor
    // it; `bash` does not. That asymmetry is the bash exclusion doing its
    // job, not a dead assertion.
    let rules = crate::agent::policy::default_rules()
        .into_iter()
        .chain(std::iter::once(user_allow.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        crate::agent::policy::rule_for(
            &rules,
            crate::agent::policy::RuleDirection::Write,
            &pin.path().join(".agents/skills/anything"),
            Some(pin.path()),
        ),
        Some(&user_allow),
        "on the PATH side the allow WINS — the contrast this pin needs"
    );
}

/// (T03 acceptance 7, the executable half of the non-widening pin) A
/// confined `exec_bash` whose `ctx.rules` carry a user `allow` for a
/// protected dir STILL cannot write that dir.
///
/// `Sandbox::create` takes ONLY the `cwd` — there is no rules parameter to
/// thread, deliberately (DD2) — which is exactly why an allow rule cannot
/// widen the shell. So the observable assertion is the executable one: the
/// same context whose WRITE TOOL would obey the allow runs a shell whose
/// write is refused.
///
/// The fixture is `a_session_cwd_inside_a_protected_dir_cannot_write_there`'s
/// tightest case (the session `cwd` IS the protected dir) plus the allow rule
/// in `ctx.rules`. It lives HERE rather than in `exec/tests/policy.rs`
/// because this module's fixtures are rooted at `/dev/shm`: `exec/tests`'
/// base is `/tmp`, which the sandbox allow-list grants write on
/// (`TEMP_DIRS`), so a confinement DENIAL observed there would be a green
/// no-op — the exact vacuity this module's `test_base` exists to prevent.
#[tokio::test]
async fn an_allow_rule_for_a_protected_dir_never_widens_a_sandboxed_bash() {
    let _base = require_sandbox!();
    let pin = match PinnedHome::new("allow-no-widen") {
        Some(pin) => pin,
        None => panic!("no /dev/shm"),
    };
    let skills = pin.path().join(".agents/skills");
    std::fs::create_dir_all(&skills).unwrap();
    std::fs::write(skills.join("SKILL.md"), "original instructions").unwrap();
    assert!(
        crate::agent::boundary::protected_dirs()
            .iter()
            .any(|p| skills.starts_with(p)),
        "premise: the pinned `$HOME` must protect the skill dir"
    );

    let user_allow = crate::agent::policy::AccessRule {
        path: "~/.agents/skills".to_string(),
        direction: crate::agent::policy::RuleDirection::Write,
        decision: crate::agent::policy::RuleDecision::Allow,
    };
    // The SAME context, one field wider than `confined_ctx`: the rule list
    // carries the allow, and `home` is set so the `~` really expands — a
    // rule that could not even match would make this test vacuous.
    let ctx = ToolCtx {
        rules: crate::agent::policy::default_rules()
            .into_iter()
            .chain(std::iter::once(user_allow.clone()))
            .collect(),
        home: Some(pin.path().to_path_buf()),
        ..confined_ctx(&skills, AccessPolicy::Allow)
    };

    // THE CONTRAST, asserted FIRST: the resolver DOES answer this write with
    // the allow, so the path tools would let it through. The shell's refusal
    // below is therefore the exclusion, not a rule that never matched.
    assert_eq!(
        crate::agent::policy::rule_for(
            &ctx.rules,
            crate::agent::policy::RuleDirection::Write,
            &skills.join("SKILL.md"),
            ctx.home.as_deref(),
        ),
        Some(&user_allow),
        "the allow must genuinely rule this write on the PATH side"
    );

    let r = run(&ctx, "echo PWNED > SKILL.md").await;
    assert!(
        r.is_error,
        "the confined shell must NOT be able to write the allowed-and-protected \
         dir (an allow rule must never widen `bash`): {:?}",
        text_of(&r)
    );
    assert_eq!(
        std::fs::read_to_string(skills.join("SKILL.md")).unwrap(),
        "original instructions",
        "the confined shell rewrote its own instructions (self-modification)"
    );
}

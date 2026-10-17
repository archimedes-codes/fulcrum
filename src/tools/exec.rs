//! The Rust tool executors (native-agent-harness Task 1): one per built-in
//! tool, each contained by the session's root set (via the existing
//! [`FsBackend`]) and returning a pi-shaped [`ToolResult`] — `content` is
//! what the LLM sees, `details` is what the UI sees (matching pi's
//! built-in renderers). The native harness (in-process) calls
//! [`execute_tool`].
//!
//! `powershell` is deliberately absent (Windows-only; the harness is
//! Linux-only) — it lands with the Windows native sessions (Task 6).
//!
//! Security note: `bash` is GATED and — at `Shell=Sandboxed` — CONFINED
//! (ADR 0030 Task 5, `tools/sandbox.rs`): the child runs inside a Landlock
//! ruleset confined to the session's write boundary + the system's program
//! dirs + the temp dirs. It is not isolation (system files stay readable
//! and this ruleset enables no network rules at any ABI — read that
//! module's doc). At
//! `Ask`/`Allow` `sh -c` can still reach anywhere (the permission gate is
//! the control, exactly as in pi's own `bash`). Only the path-param tools
//! (`read`/`write`/`edit`/`find`/`grep`/`ls`) are validated via
//! `FsBackend`, whose roots are POLICY-DERIVED (ADR 0030):
//! `Sandboxed` → the direction's boundary, `Ask`/`Allow` → unrestricted
//! (the gate already decided, and an approved `Ask` must be able to land).
//! Writes carry the [`crate::agent::boundary::protected_dirs`] deny-list on
//! TOP of that, refused in every policy.
//!
//! The skill tools (`list_skills`/`read_skill`) keep their OWN containment
//! regardless of policy: they take a skill NAME, never a path — the
//! discovered skill set is the allowlist — and a bundled-file `path` is
//! scoped to that one skill's directory by a per-skill [`FsBackend`] that
//! sees NEITHER the session boundary NOR the deny-list (a skill must not be
//! able to read a sibling skill), and the set they ADVERTISE is the
//! `model_visible` SUBSET of the discovered set (ADR 0036).

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::agent::fs_backend::{FsBackend, FsError};

/// The default `bash` deadline when no `timeout_ms` is given (300 s —
/// matching the interactive channel's `TOOL_EXEC_BASH_DEFAULT_TIMEOUT`: the native
/// `dispatch_tool` has no outer timeout of its own, so the executor must
/// bound the run itself — a no-`timeout_ms` `bash` is NOT unbounded).
const DEFAULT_BASH_TIMEOUT: Duration = Duration::from_secs(300);

/// The cap on a model-supplied `timeout_ms` (15 min — the model controls
/// the timeout: an unclamped `timeout_ms: 10^9` would run ~11 days. The
/// cap bounds a model-supplied timeout without affecting the default).
const MAX_BASH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// Clamp a model-supplied `timeout_ms` to [`MAX_BASH_TIMEOUT`] (the model
/// cannot run a command unbounded — see the cap's doc).
fn clamp_timeout_ms(ms: u64) -> Duration {
    Duration::from_millis(ms).min(MAX_BASH_TIMEOUT)
}

/// The max bytes of combined stdout+stderr `exec_bash` captures (100 KB —
/// overflow is drained but capped, flagged `truncated: true`).
const MAX_OUTPUT_BYTES: usize = 100 * 1024;

/// The max lines `exec_read` returns for a text file with no `limit`.
const MAX_READ_LINES: usize = 2000;

/// The grace deadline for draining a killed `bash` child's pipes (2 s —
/// the pipes may never close if a grandchild escaped the process group;
/// a bounded wait, not an unbounded wait for EOF).
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The default `max_results` for `exec_find` / `exec_grep`.
const DEFAULT_MAX_RESULTS: u32 = 100;

/// One element of a tool result's `content` (pi's `AgentToolResult.content`
/// = `(Text | Image)[]`): a text block or a base64 image block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// `{ "type": "text", "text": "..." }`.
    Text {
        /// The text.
        text: String,
    },
    /// `{ "type": "image", "data": <base64>, "mimeType": "..." }` (pi's
    /// `ImageContent` shape).
    Image {
        /// The base64 image data (no `data:` prefix) + MIME type.
        image: ImageRef,
    },
}

/// A base64 image reference (pi's `ImageContent` payload).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageRef {
    /// base64 (WITHOUT a `data:` prefix).
    pub data: String,
    /// The MIME type (`image/png`, …).
    #[serde(rename = "mimeType")]
    pub mime_type: String,
}

/// A pi-shaped tool result: `content` (the LLM sees this), `details` (the
/// UI sees it), `is_error` (the Rust field) — serialized as **`isError`**
/// (camelCase, the wire contract the native tool results + pi's
/// `tool_execution_end` use; a snake_case wire name would make pi render
/// every failure as a success).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolResult {
    /// The content blocks the LLM sees.
    pub content: Vec<ContentBlock>,
    /// Structured details the UI renders (omitted when there are none).
    pub details: Option<Value>,
    /// Whether the tool call failed (serialized as `isError`).
    pub is_error: bool,
}

impl ToolResult {
    /// A successful single-text-block result.
    fn ok_text(text: String, details: Option<Value>) -> Self {
        Self {
            content: vec![ContentBlock::Text { text }],
            details,
            is_error: false,
        }
    }

    /// A failed single-text-block result.
    fn fail(text: String, details: Option<Value>) -> Self {
        Self {
            content: vec![ContentBlock::Text { text }],
            details,
            is_error: true,
        }
    }
}

/// The execution context for a tool call: the session's `cwd` (the base of
/// the path-param tools' sandbox), the boundary / policy the executor
/// enforces (ADR 0030), and the cancellation token wired to the tool's
/// AbortSignal.
#[derive(Clone)]
pub struct ToolCtx {
    /// The session's `cwd` — the sandbox boundary.
    pub cwd: PathBuf,
    /// Cancelled when the tool's AbortSignal fires (kills the `bash` child).
    pub cancel: CancellationToken,
    /// The skill roots the `list_skills` / `read_skill` tools resolve
    /// against. `None` (the harness's value) → `discover_skills(Some(cwd))`,
    /// the SAME call the prompt builder makes, so both seams start from the
    /// SAME discovered set. They then filter it DIFFERENTLY, on purpose
    /// (ADR 0036): the ADVERTISED catalog — the `<skills>` prompt section and
    /// `list_skills` — is the `model_visible` subset, while `read_skill`
    /// RESOLVES against the FULL set so it can tell a model-hidden name from
    /// an unknown one and REFUSE it.
    ///
    /// WHAT the seam guarantees is therefore a claim about ONE INSTANT: read at
    /// the same moment, the two catalogs are the same SET — same roots, same
    /// `model_visible()` predicate (`read_skill` resolving the wider set is the
    /// deliberate exception, and it refuses per hit). They are NOT promised to
    /// agree ACROSS time: the `<skills>` section is FROZEN at the transcript's
    /// seq-0 prompt while BOTH skill tools re-discover on EVERY call, so a
    /// session born before a flag flip — or before a skill appeared on disk —
    /// already speaks the tools' live set while its own prompt still carries the
    /// old one. ADR 0036's frozen-vs-live bullet owns that divergence and states
    /// it as accepted; do not restate it here as agreement.
    ///
    /// `Some` pins EXPLICIT roots — the test seam (a
    /// test must never read the real `~/.agents/skills`), mirroring
    /// `skills::discover_in_roots`.
    pub skill_roots: Option<Vec<(PathBuf, crate::skills::SkillScope)>>,
    /// (ADR 0030) The read boundary (frozen at session start; `vec![cwd]` in
    /// tests — hermeticity: NEVER call `boundary::read_roots` here, it reads
    /// `$HOME`).
    pub boundary: Vec<PathBuf>,
    /// (ADR 0030) The policy in force for this call.
    pub file_policy: crate::agent::policy::FilePolicy,
    /// (ADR 0030) The write deny-list (`boundary::protected_dirs()`, frozen
    /// at session start; `vec![]` in tests).
    pub protected: Vec<PathBuf>,
    /// The web tools' configuration for this turn (Task 1). Read once per turn
    /// from `settings.json` + the environment — NOT re-read per call, so a
    /// key added mid-turn lands next turn. `WebConfig::default()` in tests
    /// (HERMETIC: never read `$HOME` here — the same rule as `boundary`).
    pub web: crate::agent::web::config::WebConfig,
    /// Render requester for escalated headless webview fetches (Task 7).
    pub render_requester: Option<std::sync::Arc<dyn crate::agent::web::RenderRequester>>,
}

/// The read-direction backend for this context (ADR 0030): the executor's
/// roots are `boundary::executor_roots` over `ctx.boundary` and
/// `ctx.file_policy.reads`.
fn read_backend(ctx: &ToolCtx) -> FsBackend {
    FsBackend {
        roots: crate::agent::boundary::executor_roots(
            &ctx.cwd,
            &ctx.boundary,
            ctx.file_policy.reads,
        ),
    }
}

/// The write-direction backend: the boundary is the session `cwd` ONLY
/// (`boundary::write_roots` — space-level agent dirs are repo content and
/// stay writable as descendants of a repo-root cwd, and the user-level ones
/// are refused by `ctx.protected` regardless).
fn write_backend(ctx: &ToolCtx) -> FsBackend {
    FsBackend {
        roots: crate::agent::boundary::executor_roots(
            &ctx.cwd,
            std::slice::from_ref(&ctx.cwd),
            ctx.file_policy.writes,
        ),
    }
}

/// The write DENY-LIST check (ADR 0030 Deviation 3): a resolved path under
/// one of `ctx.protected` (the user-level agent-definition dirs) is refused
/// in EVERY policy — including `writes: Allow` — because a model that can
/// rewrite `~/.agents/skills/*/SKILL.md` rewrites its own instructions for
/// every future session. Independent of `roots` (it is a deny, not a
/// boundary). `Some(message)` is the refusal, worded like an `FsError`.
fn protected_refusal(ctx: &ToolCtx, tool: &str, resolved: &Path) -> Option<String> {
    protected_refusal_text(&ctx.protected, tool, resolved)
}

/// The refusal text itself, split out so the GATE (`loop.rs`) refuses a
/// protected write in EXACTLY the executor's words — two copies of that
/// sentence would drift, and a gate-level deny that reads differently from
/// the executor-level one is indistinguishable from a bug to a user.
///
/// # The comparison is CANONICAL on both sides
///
/// `resolved` arrives canonical from the EXECUTOR callers (both go through
/// [`FsBackend::resolve`], which canonicalizes). A deny-list entry is NOT
/// necessarily canonical: production fills `ctx.protected` from
/// `boundary::protected_dirs()` — which canonicalizes — but nothing in the
/// type system enforces that, and any other caller (a test, a future session
/// bootstrap, a plugin seam) may hand raw paths. Comparing a canonical
/// `resolved` against a raw entry with a plain `starts_with` therefore silently
/// NO-OPS the deny-list, and "refused in EVERY policy" is exactly the property
/// that must not depend on the caller's hygiene. This is a real, observed
/// bypass, not a hypothetical: on macOS CI the temp dir sits under `/var`,
/// which is a symlink to `/private/var`, so `resolved` = `/private/var/…` never
/// prefixed the raw `/var/…` entry and the protected write SUCCEEDED (the same
/// tests reddened macOS and Windows CI, where a canonical `\\?\C:\…` verbatim
/// path never matched a raw `C:\…`).
///
/// So each entry is canonicalized at comparison time. An entry that cannot be
/// canonicalized (it does not exist) matches NOTHING — the same semantics
/// `boundary::canon` / `push_roots` already give the production list, where a
/// missing dir is simply not part of the deny-list. Already-canonical entries
/// canonicalize to themselves, so the production path (and Linux CI) is
/// unchanged. The REFUSAL still names the ORIGINAL entry: the message must
/// show the caller the directory it actually registered, not a rewritten one.
///
/// # The COMPARED path is canonicalized the same way
///
/// The mirror case is just as much a mismatch: the GATE caller
/// (`harness/loop.rs::deny_tool_result`) hands the model's RAW argument here,
/// while its `protected` entries arrive CANONICAL from
/// [`crate::agent::boundary::protected_dirs`]. Canonicalizing the entries
/// cannot reach that at all — the mismatch is now on the other side, and a raw
/// `/var/…` never prefix-matches the canonical `/private/var/…` entry (macOS),
/// and a raw `C:\…` never matches the verbatim `\\?\C:\…` one (Windows).
///
/// What that mismatch costs is worth stating exactly, because the obvious story
/// is wrong. The DENY-LIST is not one of the things the policy moves: both
/// executors call [`protected_refusal`] unconditionally, straight after
/// `validate` and with no test of `ctx.file_policy.writes`, so a protected write
/// is refused by the EXECUTOR in every mode — `writes: Allow` included. What the
/// widening moves is the BOUNDARY check and nothing else: `write_backend` builds
/// its `roots` from `ctx.file_policy.writes`, so under `Allow` the roots go
/// unrestricted and `validate` stops refusing, while the deny-list (independent
/// of `roots`) keeps refusing. And under `Allow` the gate's copy of this
/// function is not even reached — the verdict is `Allow`, so `dispatch_tool`
/// runs and `deny_tool_result`, which only answers a `Deny`, never fires.
///
/// The gate's copy exists for the DENY path, where it decides WHICH refusal the
/// model reads (the deny-list sentence, or "escapes the sandbox"). So a raw
/// protected path reaching that seam slipped the deny-list MATCH and came back
/// worded as a boundary escape — the model sent hunting for a boundary that is
/// not the problem, which is precisely the drift the shared text exists to
/// prevent. That is the bug this canonicalization closes from the gate side; the
/// executor-side refusal itself has been policy-independent since 3fd702f.
///
/// So the compared path goes through
/// [`crate::agent::fs_backend::canonicalize_lenient`] — the peel-to-deepest-
/// existing-ancestor seam [`FsBackend::resolve`] itself uses, NOT a second
/// canonicalizer, because the gate must agree with the executor about where a
/// path lands. A not-yet-existing write target is exactly the shape the gate
/// sees, which is why a plain `canonicalize` was never going to work here.
///
/// Two directions of the remaining slop are worth naming:
/// * The re-appended components (below the deepest existing ancestor) are
///   compared as plain strings, so they cannot hide a symlink hop. At THIS
///   seam that only ever errs toward a FALSE POSITIVE — an over-refusal —
///   because the comparison's only power is to DENY (a deny-list match
///   refuses; nothing here can grant anything). Fail-closed in the safe
///   direction, and the symlink-at-an-existing-component case that `resolve`
///   rejects outright cannot make this seam ALLOW: it can only make a path
///   compare differently, and comparing differently here means missing the
///   entry, which is what the canonicalization just fixed.
/// * A path that cannot be resolved at all comes back unchanged, i.e. the raw
///   spelling is compared — the pre-fix behaviour for that (rare, unresolvable)
///   shape, never worse.
///
/// The EXECUTOR callers hand paths that `FsBackend::resolve` already
/// canonicalized: lenient-canonicalizing them again is IDEMPOTENT (a canonical
/// path canonicalizes to itself, so the peel stops on its first step and the
/// comparison is unchanged). Their behaviour, and Linux CI, are byte-identical.
pub(crate) fn protected_refusal_text(
    protected: &[PathBuf],
    tool: &str,
    resolved: &Path,
) -> Option<String> {
    let resolved = crate::agent::fs_backend::canonicalize_lenient(resolved);
    let hit = protected.iter().find(|dir| {
        dir.canonicalize()
            .is_ok_and(|canon| resolved.starts_with(canon))
    })?;
    Some(format!(
        "{tool}: {} is a protected agent-definition directory ({}) — writing there is refused in every policy",
        resolved.display(),
        hit.display()
    ))
}

/// Run a shell command (`sh -c` on Unix; the command is NOT re-parsed) in
/// `ctx.cwd`, capturing interleaved stdout+stderr (capped at
/// [`MAX_OUTPUT_BYTES`]). On `ctx.cancel` or `timeout_ms` elapsing (no
/// `timeout_ms` = the [`DEFAULT_BASH_TIMEOUT`] 300 s default) the child's
/// WHOLE process group is killed (on Unix the child is spawned in its
/// own group via `process_group(0)`, so backgrounded grandchildren are
/// reaped and the pipes close) and the pipes are drained with a bounded
/// [`DRAIN_GRACE`] deadline — never an unbounded wait for EOF.
///
/// `Shell=Sandboxed` (ADR 0030) confines the CHILD in a Landlock ruleset
/// ([`crate::agent::tools::sandbox`]); see the module doc for what that
/// does and does NOT promise. It changes nothing here except one extra
/// `pre_exec` step: the process group and the negative-pid group kill stay
/// exactly as they are, so a confined child is still reapable.
///
/// The child's environment is SCRUBBED of the AppImage's own mount paths
/// before it is spawned (`crate::agent::hostenv`, and the evidence at
/// `docs/research/appimage-child-process-env-leak.md`): without it a bundled
/// run hands every `sh` the bundle's `LD_LIBRARY_PATH`/`PATH`/`GI_TYPELIB_PATH`
/// stack, and `node`/`git`/`tsc` fail inside the sandbox the user did not
/// build.
///
/// The child's Windows console belongs to [`crate::agent::childproc`] — both
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
    let mut cmd = {
        let mut c = crate::agent::childproc::command("cmd");
        c.arg("/C");
        c
    };
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
    cmd.arg(&command);
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
    let mut out: Vec<u8> = Vec::new();
    let mut truncated = false;
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
                    Ok(n) => {
                        let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                        out.extend_from_slice(&out_buf[..take]);
                        if out.len() >= MAX_OUTPUT_BYTES {
                            truncated = true;
                        }
                    }
                    Err(_) => stdout_open = false,
                }
            },
            r = stderr.read(&mut err_buf), if stderr_open => {
                match r {
                    Ok(0) => stderr_open = false,
                    Ok(n) => {
                        let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                        out.extend_from_slice(&err_buf[..take]);
                        if out.len() >= MAX_OUTPUT_BYTES {
                            truncated = true;
                        }
                    }
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
                        Ok(n) => {
                            let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                            out.extend_from_slice(&out_buf[..take]);
                            if out.len() >= MAX_OUTPUT_BYTES {
                                truncated = true;
                            }
                        }
                        Err(_) => stdout_open = false,
                    }
                },
                r = stderr.read(&mut err_buf), if stderr_open => {
                    match r {
                        Ok(0) => stderr_open = false,
                        Ok(n) => {
                            let take = n.min(MAX_OUTPUT_BYTES.saturating_sub(out.len()));
                            out.extend_from_slice(&err_buf[..take]);
                            if out.len() >= MAX_OUTPUT_BYTES {
                                truncated = true;
                            }
                        }
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
fn exit_signal(status: &std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<i32> {
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
fn bash_exit_report(
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
async fn kill_bash_process_group(pid: Option<u32>, child: &mut tokio::process::Child) {
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

/// Read a file (text, with a 1-indexed `offset`/`limit` line range capped at
/// [`MAX_READ_LINES`]) or return an image `ContentBlock` (image extensions).
/// The path is validated against `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path: String, offset?: u64, limit?: u64 }`.
pub async fn exec_read(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let path = match params.get("path").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return ToolResult::fail("read: missing `path` parameter".to_string(), None),
    };
    let offset = params.get("offset").and_then(|v| v.as_u64()).unwrap_or(1);
    let limit = params.get("limit").and_then(|v| v.as_u64());
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("read: {e}"), None),
    };

    if let Some(mime) = image_mime(resolved.extension().and_then(|e| e.to_str())) {
        let bytes = match backend.read_bytes(&resolved) {
            Ok(b) => b,
            Err(e) => return ToolResult::fail(format!("read: {e}"), None),
        };
        return ToolResult {
            content: vec![ContentBlock::Image {
                image: ImageRef {
                    data: base64::engine::general_purpose::STANDARD.encode(&bytes),
                    mime_type: mime.to_string(),
                },
            }],
            details: Some(json!({ "path": path, "lines": 0 })),
            is_error: false,
        };
    }

    let text = match backend.read(&resolved) {
        Ok(t) => t,
        Err(e) => return ToolResult::fail(format!("read: {e}"), None),
    };
    let lines: Vec<&str> = text.lines().collect();
    let start = (offset.saturating_sub(1)) as usize;
    let end = limit
        .map(|l| (start + l as usize).min(lines.len()))
        .unwrap_or_else(|| (start + MAX_READ_LINES).min(lines.len()));
    let slice: Vec<String> = lines
        .get(start..end)
        .map(|s| s.iter().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    ToolResult::ok_text(
        slice.join("\n"),
        Some(json!({ "path": path, "lines": slice.len() })),
    )
}

/// The MIME type for an image file extension (`None` for non-images).
fn image_mime(ext: Option<&str>) -> Option<&'static str> {
    match ext {
        Some("png") => Some("image/png"),
        Some("jpg") | Some("jpeg") => Some("image/jpeg"),
        Some("gif") => Some("image/gif"),
        Some("webp") => Some("image/webp"),
        Some("bmp") => Some("image/bmp"),
        _ => None,
    }
}

/// Write a file (overwrite; parent directories are created, still inside the
/// sandbox). The path is validated against `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path: String, content: String }`.
pub async fn exec_write(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let (path, content) = match (
        params.get("path").and_then(|v| v.as_str()),
        params.get("content").and_then(|v| v.as_str()),
    ) {
        (Some(p), Some(c)) => (p.to_string(), c.to_string()),
        _ => {
            return ToolResult::fail(
                "write: missing `path` or `content` parameter".to_string(),
                None,
            )
        }
    };
    let backend = write_backend(ctx);
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("write: {e}"), None),
    };
    // The deny-list (ADR 0030): unconditional, independent of `roots`.
    if let Some(msg) = protected_refusal(ctx, "write", &resolved) {
        return ToolResult::fail(msg, None);
    }
    let bytes = content.len();
    if let Err(e) = backend.write(&resolved, &content) {
        return ToolResult::fail(format!("write: {e}"), None);
    }
    ToolResult::ok_text(
        format!("Wrote {bytes} bytes to {path}"),
        Some(json!({ "path": path, "bytes": bytes })),
    )
}

/// Replace `old_text` with `new_text` in a file (the first occurrence, or all
/// when `replace_all`), writing back a unified diff in `details`. The path is
/// validated against `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path: String, old_text: String, new_text: String, replace_all?: bool }`.
pub async fn exec_edit(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let (path, old_text, new_text) = match (
        params.get("path").and_then(|v| v.as_str()),
        params.get("old_text").and_then(|v| v.as_str()),
        params.get("new_text").and_then(|v| v.as_str()),
    ) {
        (Some(p), Some(o), Some(n)) => (p.to_string(), o.to_string(), n.to_string()),
        _ => {
            return ToolResult::fail(
                "edit: missing `path`, `old_text`, or `new_text` parameter".to_string(),
                None,
            )
        }
    };
    // `old_text: ""` passes `contains("")` and `replacen`/`replace` misbehave
    // on it (pi rejects an empty `old_text`) — a distinct guard.
    if old_text.is_empty() {
        return ToolResult::fail("edit: `old_text` must not be empty".to_string(), None);
    }
    let replace_all = params
        .get("replace_all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let backend = write_backend(ctx);
    let resolved = match backend.validate(Path::new(&path)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("edit: {e}"), None),
    };
    // The deny-list (ADR 0030): an edit is a write.
    if let Some(msg) = protected_refusal(ctx, "edit", &resolved) {
        return ToolResult::fail(msg, None);
    }
    let old = match backend.read(&resolved) {
        Ok(t) => t,
        Err(e) => return ToolResult::fail(format!("edit: {e}"), None),
    };
    if !old.contains(&old_text) {
        return ToolResult::fail(format!("old_text not found in {path}"), None);
    }
    let (new, occurrences) = if replace_all {
        (
            old.replace(&old_text, &new_text),
            old.matches(&old_text).count(),
        )
    } else {
        (old.replacen(&old_text, &new_text, 1), 1)
    };
    if let Err(e) = backend.write(&resolved, &new) {
        return ToolResult::fail(format!("edit: {e}"), None);
    }
    ToolResult::ok_text(
        format!("Edited {path}"),
        Some(json!({
            "path": path,
            "occurrences": occurrences,
            "diff": unified_diff(&old, &new, &path),
        })),
    )
}

/// A minimal unified diff (a single hunk: the unchanged prefix/suffix lines
/// are elided, the changed middle is emitted as `-`/`+` lines).
fn unified_diff(old: &str, new: &str, path: &str) -> String {
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();
    let mut prefix = 0;
    while prefix < old_lines.len().min(new_lines.len()) && old_lines[prefix] == new_lines[prefix] {
        prefix += 1;
    }
    let max_suffix = (old_lines.len() - prefix).min(new_lines.len() - prefix);
    let mut suffix = 0;
    while suffix < max_suffix
        && old_lines[old_lines.len() - 1 - suffix] == new_lines[new_lines.len() - 1 - suffix]
    {
        suffix += 1;
    }
    let old_start = prefix;
    let old_end = old_lines.len() - suffix;
    let new_start = prefix;
    let new_end = new_lines.len() - suffix;
    let mut d = format!("--- a/{path}\n+++ b/{path}\n");
    d.push_str(&format!(
        "@@ -{},{} +{},{} @@\n",
        old_start + 1,
        old_end - old_start,
        new_start + 1,
        new_end - new_start
    ));
    for line in &old_lines[old_start..old_end] {
        d.push_str(&format!("-{line}\n"));
    }
    for line in &new_lines[new_start..new_end] {
        d.push_str(&format!("+{line}\n"));
    }
    d
}

/// Find files by glob pattern: shells out to `fd` (`fd -g <pattern>
/// <path> --max-results <n>`) when available, else falls back to a `walkdir`
/// walk with a built-in glob matcher. The search path is validated against
/// `ctx.cwd` via `FsBackend`.
///
/// Deliberately NOT shared with `commands::files::collect_files` (the `?`
/// file picker, ADR 0033/0034), even though both walk a tree. The reason they
/// stay apart is unchanged — sharing would mean either a subprocess spawn per
/// keystroke or a slower agent `find` — so they remain cross-referenced by
/// comment instead (the discipline recorded for the mirrored `parse_value`).
/// Do not "unify" them.
///
/// HOW THEY DIFFER TODAY (as of the picker learning the repo's ignore rules):
///
/// * **Ignore rules.** The picker always applies them (`.gitignore` +
///   `.ignore` + `.git/info/exclude`, repo-scoped, never machine-wide).
///   `exec_find` applies them only as a side effect of `fd`, which honours
///   `.gitignore` by default — the `walkdir` fallback applies NONE. So this
///   tool's result set depends on whether `fd` happens to be installed, while
///   the picker's never does. That fd/no-fd divergence is a KNOWN defect,
///   recorded separately and deliberately not fixed here.
/// * **Dot-naming.** The fallback's dot-skip is per-FILE, so dot-named files
///   (`.gitignore`, `.env`) vanish from its results — the opposite of what the
///   completion list wants — and it descends INTO `.git`, listing paths under
///   it. The picker is the mirror image: it keeps every dot-named FILE and
///   prunes DIRECTORIES, but only the VCS metadata ones (`.git`, `.hg`, `.svn`)
///   and only at any depth below the root (a Space opened AT a directory named
///   `.git` is listed, never pruned) — every other dot-directory is walked and
///   listed unless the repo's own ignore rules exclude it. So
///   `.github/workflows/ci.yml` IS
///   completable in the picker, while this tool's `fd` arm never sees it (`fd`
///   hides hidden names by default, and no `--hidden` is passed) even though
///   the fallback walks right into it — another face of the fd/no-fd defect
///   above. `.git/…` paths run the other way: never in the picker, offered by
///   the fallback.
/// * **Separators.** The fallback builds its relative path with
///   `to_string_lossy()`, which emits `\` on Windows; the picker joins
///   `components()` with `/`, because `\` is outside the composer's token
///   charset.
///
/// `params`: `{ pattern: String, path?: String, max_results?: u32 }`.
pub async fn exec_find(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let pattern = match params.get("pattern").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return ToolResult::fail("find: missing `pattern` parameter".to_string(), None),
    };
    let dir = params
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let max = params
        .get("max_results")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_MAX_RESULTS);
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("find: {e}"), None),
    };

    // Prefer `fd` when it is installed (the spawn fails with NotFound
    // otherwise — that is the availability check, no `which` needed).
    // `--` before the path (the `rg --` hardening — a pattern that looks
    // like a flag is not re-parsed as one).
    //
    // The bound `cmd` (not a fluent chain) exists to carry the SAME env scrub
    // `bash` gets: `fd` and `rg` are system binaries whose dependencies
    // (`libpcre2-8.so.0`, `libcrypto`) the bundle also ships, so on a mounted
    // host they bind the bundle's copies unversioned — `git`'s warning line is
    // the same mechanism, and the poisoned `PATH` is what makes the bare-name
    // lookup work at all (research §7's site table). KNOWN REMAINING GAP,
    // deliberately out of scope here: unlike `exec_bash`, `exec_find` and
    // `exec_grep` pass NO `current_dir`, so this child runs with the INHERITED
    // cwd — which under `AppRun.c`'s `chdir("$APPDIR/usr")` is INSIDE the
    // mount. The env half is fixed; the cwd half is not, and a relative path in
    // a `find`/`grep` result can still surprise because of it (it is not a leak
    // of libraries — do not read the scrub as having fixed that).
    //
    // The constructor is `crate::agent::childproc`'s, which owns the child's
    // Windows console window policy (`CREATE_NO_WINDOW`).
    let mut cmd = crate::agent::childproc::command("fd");
    // A Tool child starts at EOF on stdin — see the note in `exec_bash`: an
    // inherited handle is a live pipe nothing can ever feed, so a `fd` that
    // read stdin would hang to the caller's deadline instead of failing.
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.args([
        "--max-results",
        &max.to_string(),
        "-g",
        &pattern,
        "--",
        &resolved.to_string_lossy(),
    ]);
    crate::agent::hostenv::for_child().apply(&mut cmd);
    let fd = cmd.output().await;
    if let Ok(out) = fd {
        // `fd` prints paths relative to its CWD — since the search dir is
        // passed as an absolute (canonicalized) path, strip the prefix so
        // the results are relative to the search dir (like the `walkdir`
        // fallback).
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let prefix = format!("{}/", resolved.to_string_lossy());
        let stdout: String = stdout
            .lines()
            .map(|l| l.strip_prefix(&prefix).unwrap_or(l))
            .collect::<Vec<_>>()
            .join("\n");
        let matches = stdout.lines().filter(|l| !l.is_empty()).count();
        return ToolResult::ok_text(
            stdout,
            Some(json!({
                "matches": matches,
                "truncated": matches as u32 >= max,
            })),
        );
    }

    // Fallback: walk the tree (skip hidden entries, like `fd`'s default)
    // and glob-match the path relative to the search dir. The walk is
    // BLOCKING (a big tree would stall a tokio worker): it runs on the
    // blocking thread pool.
    let (matches, truncated) = match tokio::task::spawn_blocking(move || {
        let mut matches: Vec<String> = Vec::new();
        let mut truncated = false;
        let walker = walkdir::WalkDir::new(&resolved)
            .into_iter()
            .filter_map(|e| e.ok());
        for entry in walker {
            if entry.file_type().is_dir() {
                continue;
            }
            if entry
                .path()
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with('.'))
            {
                continue;
            }
            let rel = entry.path().strip_prefix(&resolved).unwrap_or(entry.path());
            let rel_str = rel.to_string_lossy().to_string();
            if glob_match(&pattern, &rel_str) {
                matches.push(rel_str);
                if matches.len() as u32 >= max {
                    truncated = true;
                    break;
                }
            }
        }
        (matches, truncated)
    })
    .await
    {
        Ok(r) => r,
        Err(_) => {
            return ToolResult::fail(
                "find: the directory walk failed (blocking task join error)".to_string(),
                None,
            )
        }
    };
    ToolResult::ok_text(
        matches.join("\n"),
        Some(json!({ "matches": matches.len(), "truncated": truncated })),
    )
}

/// Match `candidate` against a glob `pattern` (`*` = any run of non-`/`
/// chars, `**` = any run of chars including `/`, `?` = one non-`/` char).
fn glob_match(pattern: &str, candidate: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let c: Vec<char> = candidate.chars().collect();
    glob_match_idx(&p, 0, &c, 0)
}

fn glob_match_idx(p: &[char], i: usize, c: &[char], j: usize) -> bool {
    if i == p.len() {
        return j == c.len();
    }
    match p[i] {
        '*' if p.get(i + 1) == Some(&'*') => {
            // `**` matches any run of chars (including `/`), zero or more.
            let mut k = j;
            loop {
                if glob_match_idx(p, i + 2, c, k) {
                    return true;
                }
                if k == c.len() {
                    return false;
                }
                k += 1;
            }
        }
        '*' => {
            // `*` matches any run of non-`/` chars, zero or more.
            let mut k = j;
            loop {
                if glob_match_idx(p, i + 1, c, k) {
                    return true;
                }
                if k == c.len() || c[k] == '/' {
                    return false;
                }
                k += 1;
            }
        }
        '?' => {
            if j < c.len() && c[j] != '/' {
                glob_match_idx(p, i + 1, c, j + 1)
            } else {
                false
            }
        }
        ch => {
            if j < c.len() && c[j] == ch {
                glob_match_idx(p, i + 1, c, j + 1)
            } else {
                false
            }
        }
    }
}

/// Grep a file tree: shells out to `ripgrep` (`rg <flags> <pattern>
/// <path>`) with flags mapped from the params. The search path is validated
/// against `ctx.cwd` via `FsBackend`. There is no fallback — a missing
/// `rg` is a tool error.
///
/// `params`: `{ pattern: String, path?: String, glob?: String, max_results?: u32, "-i"?: bool }`.
pub async fn exec_grep(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let pattern = match params.get("pattern").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return ToolResult::fail("grep: missing `pattern` parameter".to_string(), None),
    };
    let dir = params
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let glob = params.get("glob").and_then(|v| v.as_str());
    // `max_results` defaults to the cap (like `find` — a grep with no
    // `max_results` is NOT unbounded on a big tree).
    let max = params
        .get("max_results")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(DEFAULT_MAX_RESULTS);
    let ignore_case = params.get("-i").and_then(|v| v.as_bool()).unwrap_or(false);
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("grep: {e}"), None),
    };

    let mut args: Vec<String> = Vec::new();
    if ignore_case {
        args.push("-i".to_string());
    }
    if let Some(g) = glob {
        args.push("--glob".to_string());
        args.push(g.to_string());
    }
    // `-m` bounds matches PER FILE (the total can exceed `max` when more
    // files match) — the OUTPUT is additionally truncated below.
    args.push("-m".to_string());
    args.push(max.to_string());
    // `--` terminates option parsing: the pattern is matched LITERALLY
    // (an option-like pattern such as `--pre=sh` — which would otherwise
    // be parsed by `rg` as a `--pre` option and run a program on every
    // input file, un-gated — is treated as the pattern, and the search
    // path stays the validated `resolved` dir, not `.`).
    args.push("--".to_string());
    args.push(pattern.clone());
    args.push(resolved.to_string_lossy().into_owned());

    // Bound `cmd` for the same reason as the `fd` arm of `exec_find`: the env
    // scrub needs a name. Same KNOWN GAP too — no `current_dir` is passed, so
    // the child's cwd stays the inherited one, which on a mounted host is
    // inside the mount (`AppRun.c`'s `chdir("$APPDIR/usr")`); the env is
    // scrubbed, the cwd is not.
    //
    // Same `crate::agent::childproc` constructor as `fd` — it owns the child's
    // Windows console window policy (`CREATE_NO_WINDOW`).
    let mut cmd = crate::agent::childproc::command("rg");
    // Tool child → null stdin, for the same reason as `fd` and `bash` above.
    crate::agent::childproc::closed_stdin(&mut cmd);
    cmd.args(&args);
    crate::agent::hostenv::for_child().apply(&mut cmd);
    match cmd.output().await {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout).to_string();
            let lines: Vec<&str> = stdout.lines().collect();
            let matches = lines.iter().filter(|l| !l.is_empty()).count();
            // `-m` is PER-FILE, so the total can exceed `max` (one match
            // per file): bound the OUTPUT to `max` lines.
            let truncated = matches as u32 > max;
            let shown: String = lines
                .iter()
                .take(max as usize)
                .copied()
                .collect::<Vec<_>>()
                .join("\n");
            ToolResult::ok_text(
                shown,
                Some(json!({ "matches": matches, "truncated": truncated })),
            )
        }
        Err(e) => ToolResult::fail(format!("grep: ripgrep (rg) is not available: {e}"), None),
    }
}

/// List a directory (`read_dir`; entries sorted, directories get a `/`
/// suffix; `long` prefixes the file size). The path is validated against
/// `ctx.cwd` via `FsBackend`.
///
/// `params`: `{ path?: String, long?: bool }`.
pub async fn exec_ls(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let dir = params
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or(".")
        .to_string();
    let long = params
        .get("long")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let backend = read_backend(ctx);
    let resolved = match backend.validate(Path::new(&dir)) {
        Ok(p) => p,
        Err(e) => return ToolResult::fail(format!("ls: {e}"), None),
    };
    // `read_dir` is BLOCKING (a big directory would stall a tokio
    // worker): it runs on the blocking thread pool.
    let (names, error) = match tokio::task::spawn_blocking(move || {
        let entries = match std::fs::read_dir(&resolved) {
            Ok(e) => e,
            Err(e) => return (Vec::new(), Some(format!("ls: {e}"))),
        };
        let mut names: Vec<String> = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => return (names, Some(format!("ls: {e}"))),
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            let suffix = if is_dir { "/" } else { "" };
            names.push(if long {
                format!("{size} {name}{suffix}")
            } else {
                format!("{name}{suffix}")
            });
        }
        names.sort();
        (names, None)
    })
    .await
    {
        Ok(r) => r,
        Err(_) => (
            Vec::new(),
            Some("ls: the directory listing failed (blocking task join error)".to_string()),
        ),
    };
    if let Some(e) = error {
        return ToolResult::fail(e, None);
    }
    ToolResult::ok_text(names.join("\n"), Some(json!({ "entries": names.len() })))
}

/// The ONE discovery call the skill tools make per tool call (see
/// [`ToolCtx::skill_roots`]): the pinned roots when set (the test seam),
/// otherwise the SAME discovery call the prompt builder makes — ADR 0029's
/// no-drift seam, which is about ADVERTISEMENT: what the tools and the
/// `<skills>` section advertise is exactly the `model_visible` SUBSET of this
/// set (ADR 0036). The set itself is UNFILTERED, and resolution is
/// deliberately WIDER than advertisement: `exec_read_skill` resolves against
/// this FULL set on purpose, so it can tell a model-hidden name from an
/// unknown one and answer each with its own message rather than a lie.
fn skills_for(ctx: &ToolCtx) -> Vec<crate::skills::SkillInfo> {
    match ctx.skill_roots.as_ref() {
        Some(roots) => crate::skills::discover_in_roots(roots),
        None => crate::skills::discover_skills(Some(&ctx.cwd)),
    }
}

/// The skills the tools may ADVERTISE (ADR 0036): `skills_for` minus the
/// model-hidden ones. `read_skill` does NOT use this — it resolves against
/// `skills_for` so it can distinguish hidden from unknown.
fn model_skills_for(ctx: &ToolCtx) -> Vec<crate::skills::SkillInfo> {
    skills_for(ctx)
        .into_iter()
        .filter(|s| s.model_visible())
        .collect()
}

/// List the skills the Agent may use — the catalog the `<skills>` system
/// prompt section advertises, resolved through the SAME discovery call and
/// filtered on the SAME predicate, so the two cannot drift in WHAT they filter
/// (ADR 0029 + ADR 0036): a model-hidden skill is simply ABSENT here. They DO
/// differ in WHEN — this list is re-discovered per call while the prompt
/// section is frozen at seq 0, so a session can list a set its own prompt no
/// longer matches (or does not yet); [`ToolCtx::skill_roots`] and ADR 0036 own
/// that divergence.
///
/// Skills live OUTSIDE the session `cwd` (user skills are in
/// `~/.agents/skills` / `~/.pi/agent/skills`); these tools are the sanctioned
/// route to them — keyed by NAME and contained to the skill's own directory
/// (ADR 0029). Whether a `read` reaches a skill path is NOT decided here: it
/// is the **Boundary**'s call, and ADR 0030 grants the read Boundary the
/// discovery roots. The discovered set is this tool's allowlist.
///
/// `params`: `{}`.
pub async fn exec_list_skills(ctx: &ToolCtx, _params: &Value) -> ToolResult {
    let skills = model_skills_for(ctx);
    if skills.is_empty() {
        return ToolResult::ok_text("none".to_string(), Some(json!({ "skills": 0 })));
    }
    let lines: Vec<String> = skills
        .iter()
        .map(|s| {
            // A block-scalar `description` can hold newlines — flatten it so
            // the one-line-per-skill contract holds (as `list_agents` does).
            let description = s.description.replace('\n', " ");
            format!("{} ({}): {}", s.name, s.scope, description)
        })
        .collect();
    ToolResult::ok_text(lines.join("\n"), Some(json!({ "skills": skills.len() })))
}

/// Read a skill's `SKILL.md` (no `path`) or one of its bundled files
/// (`path`, resolved against the skill's directory).
///
/// The `name` must match a discovered skill — that is the allowlist, so no
/// arbitrary path is ever reachable. A model-hidden skill DOES resolve (so
/// the model is not told a lie) but is REFUSED per hit, before any read, with
/// the sanctioned user route named back at it — the `$name` Mention, but ONLY
/// when the token grammar can carry that name (ADR 0031); otherwise the
/// refusal says the Skill must be loaded explicitly, so it never promises a
/// dead token (ADR 0036) — so neither its `SKILL.md` nor any bundled file is
/// ever read. Unlike `list_skills`, which filters the LIST, this filters the
/// HIT: it needs the unfiltered set to tell hidden
/// apart from unknown. Both filter the same predicate, so the tools' catalog
/// still cannot drift from the prompt's catalog in WHAT is filtered (ADR 0029);
/// the frozen prompt vs the per-call tool (`WHEN`) is the caveat
/// [`ToolCtx::skill_roots`] carries.
///
/// With a `path`, the read is scoped to the skill's OWN directory via a
/// per-skill [`FsBackend`]: `..` traversal, an absolute path, and a symlink
/// out of the dir are all rejected, and a sibling skill's files are NOT
/// reachable (the boundary is the skill dir, not the skill root).
///
/// `params`: `{ name: String, path?: String }`.
pub async fn exec_read_skill(ctx: &ToolCtx, params: &Value) -> ToolResult {
    let Some(name) = params.get("name").and_then(|v| v.as_str()) else {
        return ToolResult::fail("read_skill: missing `name` parameter".to_string(), None);
    };
    let skills = skills_for(ctx);
    let key = name.to_lowercase();
    let Some(skill) = skills.iter().find(|s| s.name.to_lowercase() == key) else {
        return ToolResult::fail(
            format!("read_skill: unknown skill {name:?} — call list_skills for the names"),
            None,
        );
    };
    if !skill.model_visible() {
        // ADR 0029's disclosure rule holds for BOTH shapes: name ONLY what the
        // model sent — no path, no dir, no scope (ADR 0036: this runs BEFORE
        // the `FsBackend` below exists, so nothing is read).
        //
        // The `$mention` route is promised ONLY when the grammar can actually
        // carry the name. The `$` token is `[a-z0-9]+(-[a-z0-9]+)*` (ADR 0031)
        // while discovery screens a name merely for tag-safety, so a legal name
        // like `has space` / `foo_bar` is TYPEABLE NOWHERE: promising
        // `$has space` would send the model (and the user, via the dialog
        // tooltip) to a token that silently expands NOTHING.
        //
        // This predicate filters the `@` and `#` picker catalogs
        // (`commands::agents::list_agent_definitions_for_space`,
        // `agent::mcp::config::server_infos`). It does NOT filter the `$` SKILL
        // catalog: those rows are built from the FULL discovery output with
        // only a name-substring filter (`ChatStream.tsx`), deliberately — the
        // Client's skill catalog must stay total or this flag would delete the
        // user's own route (ADR 0036). So for Skills the predicate is enforced
        // in exactly TWO places — this refusal and the Skills-dialog tooltip —
        // and the `$` picker CAN offer a name this tool refuses. That is the
        // accepted shape, not a drift to fix.
        let route = if crate::skills::is_mentionable_name(&skill.name) {
            // `${key}` is LOWERCASED on purpose: the token grammar is
            // lowercase-only, so `$DeBuG` expands nothing while `$debug` does.
            format!("ask the user to send ${key}")
        } else {
            // NO `$` anywhere in this branch, and no absolute claim. The
            // predicate is the ASCII token grammar, while the Client's match is
            // a JS case-FOLD, so "it cannot be typed" is false for exactly one
            // input (a name holding U+212A lowercases to `k` in JS and WOULD
            // expand, while `is_ascii_alphanumeric` rejects it here) — the
            // accurate statement is that the name is OUTSIDE THE GRAMMAR. And a
            // literal `$` in the sentence is a token the model can still copy.
            "ask the user to load this Skill explicitly (its name is outside the \
             mention token grammar)"
                .to_string()
        };
        return ToolResult::fail(
            format!("read_skill: {name:?} is model-hidden — the Agent may not load it; {route}"),
            None,
        );
    }

    // The per-skill mini-sandbox: the skill's dir is the boundary. NOT the
    // session boundary (ADR 0030): a skill must not be able to read a
    // sibling skill, so this backend sees exactly ONE root and no deny-list.
    let backend = FsBackend {
        roots: vec![PathBuf::from(&skill.dir)],
    };
    let Some(rel) = params.get("path").and_then(|v| v.as_str()) else {
        // No `path` → the `SKILL.md` BODY (discovery already stripped the
        // frontmatter — exactly what progressive disclosure wants).
        return ToolResult::ok_text(
            skill.body.clone(),
            Some(json!({
                "skill": skill.name,
                "scope": skill.scope.to_string(),
                "dir": skill.dir,
            })),
        );
    };

    let resolved = match backend.validate(Path::new(rel)) {
        Ok(p) => p,
        Err(FsError::PathEscape { .. }) => {
            // NEVER surface the `FsError` here: `PathEscape` carries the
            // CANONICALIZED path, which for a symlink INSIDE the skill dir
            // is its target OUTSIDE it — information the model never
            // supplied (it named only `link`). Echoing it turns a
            // repo-shipped skill (`data.txt` -> `~/.ssh/id_rsa`) into a
            // path-disclosure oracle. `read` may echo a path the model
            // itself passed; `read_skill` must never invent one. Name only
            // what the model sent + the boundary it already knows.
            return ToolResult::fail(
                format!(
                    "read_skill: `{rel}` resolves outside the skill directory {} — a skill's `path` must stay inside its own directory (a symlink that leaves it is rejected)",
                    skill.dir
                ),
                None,
            );
        }
        // An `Io` failure (missing file, a directory, bad encoding) discloses
        // nothing the model did not already name — safe to surface verbatim.
        Err(e) => return ToolResult::fail(format!("read_skill: {e}"), None),
    };
    match backend.read(&resolved) {
        Ok(text) => ToolResult::ok_text(
            text,
            Some(json!({
                "skill": skill.name,
                "path": rel,
            })),
        ),
        Err(e) => ToolResult::fail(format!("read_skill: {e}"), None),
    }
}

/// Dispatch a tool call to its executor (the entry point the native
/// harness calls).
pub async fn execute_tool(ctx: &ToolCtx, tool: &str, params: &Value) -> ToolResult {
    match tool {
        "bash" => exec_bash(ctx, params).await,
        "read" => exec_read(ctx, params).await,
        "write" => exec_write(ctx, params).await,
        "edit" => exec_edit(ctx, params).await,
        "find" => exec_find(ctx, params).await,
        "grep" => exec_grep(ctx, params).await,
        "ls" => exec_ls(ctx, params).await,
        "list_skills" => exec_list_skills(ctx, params).await,
        "read_skill" => exec_read_skill(ctx, params).await,
        "web_search" => crate::agent::web::tools::exec_web_search(ctx, params).await,
        "fetch_content" => crate::agent::web::tools::exec_fetch_content(ctx, params).await,
        "get_search_content" => {
            crate::agent::web::tools::exec_get_search_content(ctx, params).await
        }
        other => ToolResult::fail(format!("unknown tool: {other}"), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh temp dir as the sandbox `cwd`.
    fn temp_cwd(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("exec-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The test `ToolCtx`. HERMETIC BY DESIGN: `boundary` is the single
    /// `cwd` and `protected` is EMPTY — this must NEVER derive them from the
    /// `$HOME`-reading boundary helpers (they would make these tests depend
    /// on whatever happens to be in the developer's `~/.agents`, the same
    /// trap `ToolCtx::skill_roots: Option<…>` exists to avoid).
    ///
    /// The policy pins `Sandboxed` for reads and writes: these are
    /// CONTAINMENT tests, so they pin the tier that enforces the boundary
    /// (`shell: Allow` — no test here gates `bash`). The other tiers are
    /// covered by the explicit tier tests below.
    fn ctx(cwd: &Path) -> ToolCtx {
        ToolCtx {
            cwd: cwd.to_path_buf(),
            cancel: CancellationToken::new(),
            skill_roots: None,
            boundary: vec![cwd.to_path_buf()],
            file_policy: crate::agent::policy::FilePolicy {
                reads: crate::agent::policy::AccessPolicy::Sandboxed,
                writes: crate::agent::policy::AccessPolicy::Sandboxed,
                shell: crate::agent::policy::AccessPolicy::Allow,
            },
            protected: Vec::new(),
            web: Default::default(),
            render_requester: None,
        }
    }

    /// The same ctx with one policy overridden (the tier tests).
    fn ctx_policy(cwd: &Path, policy: crate::agent::policy::FilePolicy) -> ToolCtx {
        ToolCtx {
            file_policy: policy,
            ..ctx(cwd)
        }
    }

    /// Whether THIS machine can confine a child (Linux + a Landlock
    /// kernel). The confined tests branch on it: a kernel that cannot
    /// confine must exercise the fail-closed path, and CI on such a kernel
    /// stays reproducible instead of reddening.
    fn can_confine() -> bool {
        #[cfg(target_os = "linux")]
        {
            crate::agent::tools::sandbox::landlock_available()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// The first text block's text (panics if the shape is wrong).
    fn text_of(r: &ToolResult) -> &str {
        match &r.content[0] {
            ContentBlock::Text { text } => text,
            other => panic!("expected a text block, got {other:?}"),
        }
    }

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
    /// measured repro. Linux only (it is the OS this desktop targets for the
    /// sandbox, and the helper is used by a Linux-only test).
    #[cfg(target_os = "linux")]
    fn pin_to_cpu0() {
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe { libc::CPU_ZERO(&mut set) };
        unsafe { libc::CPU_SET(0, &mut set) };
        let rc = unsafe {
            libc::sched_setaffinity(
                0,
                std::mem::size_of::<libc::cpu_set_t>(),
                std::ptr::addr_of!(set),
            )
        };
        assert_eq!(rc, 0, "the test thread must be pinned to cpu 0");
    }

    // ── serialization ────────────────────────────────────────────────────

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
        let _ = std::fs::remove_dir_all(&cwd);
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
            "agent::tools::exec::tests::{}",
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
        let out =
            match tokio::time::timeout(Duration::from_secs(180), child.wait_with_output()).await {
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
        let _ = std::fs::remove_dir_all(&cwd);
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
        let _ = std::fs::remove_dir_all(&cwd);
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
            },
        );
        let r = execute_tool(&confined, "read", &json!({ "path": "a.txt" })).await;
        assert!(!r.is_error, "a read is not gated by the shell policy");
        assert!(text_of(&r).contains("hello"));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The fixture is a POSIX-SHELL fixture, so it runs where a POSIX shell
    /// is the one the product picks: `1>&2` + `;` sequencing + `exit 3` are
    /// `sh` syntax, and the Windows branch of `exec_bash` spawns `cmd /C`,
    /// which reads the same string as one `echo` with a bogus redirect — it
    /// exits 0, so `is_error` was false on the Windows runner. The exit-code
    /// DECISION is not unix-specific and stays covered everywhere by
    /// `bash_exit_report_nonzero_exit_is_an_error_with_its_real_code`.
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
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn bash_cancel_kills_child() {
        let cwd = temp_cwd("bash-cancel");
        let token = CancellationToken::new();
        let c = ToolCtx {
            cwd: cwd.clone(),
            cancel: token.clone(),
            skill_roots: None,
            boundary: vec![cwd.clone()],
            file_policy: crate::agent::policy::FilePolicy::default(),
            protected: Vec::new(),
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
        let _ = std::fs::remove_dir_all(&cwd);
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
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The fixture is POSIX through and through: `( … ) &` backgrounding and
    /// the process-group kill it exists to prove (`kill_bash_process_group`
    /// SIGKILLs `-pid`, and on Windows there IS no process group — the
    /// product's own Windows branch kills the direct child only). Under
    /// `cmd /C` the command line does not even parse as written, so the
    /// marker got written by a survivor and the assertion inverted.
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
        let _ = std::fs::remove_dir_all(&cwd);
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
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The cancel path applies the same rule: a cancelled child that had
    /// already closed its pipes is still alive (pipe EOF is not an exit)
    /// and must be killed with its group, not left running.
    /// The cancel half of the same `exec >&- 2>&-` fixture (see
    /// `bash_timeout_kills_a_child_that_closed_its_pipes`): `cmd /C` refuses
    /// the redirection, so the child exited at once with `cancelled: false`
    /// and the cancel never had a live process to race. The cancel path is
    /// covered on Windows by `bash_cancel_kills_child` (`sleep 5`).
    #[cfg(unix)]
    #[tokio::test]
    async fn bash_cancel_kills_a_child_that_closed_its_pipes() {
        let cwd = temp_cwd("bash-closed-pipes-cancel");
        let token = CancellationToken::new();
        let c = ToolCtx {
            cwd: cwd.clone(),
            cancel: token.clone(),
            skill_roots: None,
            boundary: vec![cwd.clone()],
            file_policy: crate::agent::policy::FilePolicy::default(),
            protected: Vec::new(),
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
        let _ = std::fs::remove_dir_all(&cwd);
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
    #[cfg(unix)]
    fn wait_status(raw: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        std::process::ExitStatus::from_raw(raw)
    }

    /// THE BUG: a command that exited 0 and whose exit status was observed
    /// is a SUCCESS. Not an error, code 0, no note.
    #[cfg(unix)]
    #[test]
    fn bash_exit_report_success_is_not_an_error() {
        let (code, is_error, note) = bash_exit_report(false, false, Some(wait_status(0 << 8)));
        assert!(
            !is_error,
            "a successful run must NOT be reported as a failure"
        );
        assert_eq!(code, Some(0));
        assert_eq!(note, None, "a success has nothing to warn about");
    }

    /// A non-zero exit is an error carrying the REAL code (the shape
    /// `bash_nonzero_exit_is_error` asserts on the wire).
    #[cfg(unix)]
    #[test]
    fn bash_exit_report_nonzero_exit_is_an_error_with_its_real_code() {
        let (code, is_error, note) = bash_exit_report(false, false, Some(wait_status(3 << 8)));
        assert!(is_error);
        assert_eq!(code, Some(3), "the real code, never a fabricated one");
        assert_eq!(note, None);
    }

    /// A signal death is an error, but is NOT dressed up as exit code -1 —
    /// it says which signal killed it (a real signal death must stay
    /// distinguishable from a non-zero exit).
    #[cfg(unix)]
    #[test]
    fn bash_exit_report_signal_death_names_the_signal() {
        let (code, is_error, note) = bash_exit_report(false, false, Some(wait_status(9)));
        assert!(is_error, "killed by a signal is a failure");
        assert_eq!(code, None, "a signal death has no exit code");
        let note = note.expect("the note names the signal");
        assert!(note.contains("signal 9"), "got {note:?}");
    }

    /// A cancelled run is an error whatever the status says (the user asked
    /// for it to stop, so it did not complete).
    #[cfg(unix)]
    #[test]
    fn bash_exit_report_cancelled_is_always_an_error() {
        // Even a status of 0: the run was interrupted, it did not finish.
        let statuses: [Option<std::process::ExitStatus>; 3] =
            [None, Some(wait_status(0 << 8)), Some(wait_status(9))];
        for status in statuses {
            let (code, is_error, note) = bash_exit_report(true, false, status);
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
    #[cfg(unix)]
    #[test]
    fn bash_exit_report_timeout_is_an_error_that_says_timed_out() {
        let (code, is_error, note) = bash_exit_report(false, true, None);
        assert!(is_error);
        assert_eq!(code, None, "no status was seen, so no code is claimed");
        let note = note.expect("the note says the deadline was hit");
        assert!(note.contains("timed out"), "got {note:?}");
        // A timeout that DID see the status still reports it (the kill
        // landed after the child had already exited).
        let (code, is_error, _) = bash_exit_report(false, true, Some(wait_status(0 << 8)));
        assert!(
            is_error,
            "a timed-out run is an error even with a clean status"
        );
        assert_eq!(code, Some(0));
    }

    /// The genuinely-unknowable case: NO status was captured. It stays an
    /// error, claims NO code, and tells the model to check the side effects
    /// before re-running (re-running a non-idempotent command blindly is
    /// the dangerous outcome this whole path exists to avoid).
    #[test]
    fn bash_exit_report_unknown_status_says_so_and_claims_no_code() {
        let (code, is_error, note) = bash_exit_report(false, false, None);
        assert!(is_error, "an unknown outcome is NOT silently a success");
        assert_eq!(code, None, "no status known means no code invented");
        let note = note.expect("the note says the status is unknown");
        assert!(note.contains("unknown"), "got {note:?}");
        assert!(
            note.contains("re-run") || note.contains("rerun"),
            "the note must warn about re-running: {note:?}"
        );
    }

    /// The one non-error row of the table is (not cancelled, not timed out,
    /// exited 0) — everything else is an error. Asserted as a table so a
    /// future edit cannot widen the success case.
    #[cfg(unix)]
    #[test]
    fn bash_exit_report_success_is_the_only_non_error_row() {
        let rows = [
            (false, false, Some(wait_status(0 << 8))),
            (false, false, Some(wait_status(1 << 8))),
            (false, false, Some(wait_status(9))),
            (false, false, None),
            (true, false, Some(wait_status(0 << 8))),
            (false, true, Some(wait_status(0 << 8))),
        ];
        let ok: Vec<usize> = rows
            .iter()
            .enumerate()
            .filter(|(_, (c, t, s))| !bash_exit_report(*c, *t, *s).1)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            ok,
            vec![0],
            "only the first row (exit 0, untouched) is a success"
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
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The other half of "report the REAL exit status": a child that closed
    /// its pipes early and then exited NON-ZERO must come back as an error
    /// carrying its real code — never "exit status unknown", never a code
    /// guessed after a grace period. Two shapes, same expectation: the
    /// short one pins the ordinary case (the bounded post-loop reap usually
    /// catches a 0.2 s sleep, so it cannot pin the hole alone), and
    /// the long one — a sleep that outlasts [`DRAIN_GRACE`] — pins the
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
        let _ = std::fs::remove_dir_all(&cwd);
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
        let _ = std::fs::remove_dir_all(&cwd);
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
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── read ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn read_happy_and_offset_limit() {
        let cwd = temp_cwd("read");
        std::fs::write(cwd.join("f.txt"), "line1\nline2\nline3\nline4\nline5").unwrap();
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "f.txt" })).await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "line1\nline2\nline3\nline4\nline5");
        assert_eq!(r.details.as_ref().unwrap()["lines"], 5);
        // 1-indexed offset + limit.
        let r = execute_tool(
            &ctx(&cwd),
            "read",
            &json!({ "path": "f.txt", "offset": 2, "limit": 2 }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "line2\nline3");
        assert_eq!(r.details.as_ref().unwrap()["lines"], 2);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_image_returns_image_block() {
        let cwd = temp_cwd("read-img");
        // A minimal 1×1 transparent PNG.
        let png: &[u8] = &[
            137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1,
            8, 6, 0, 0, 0, 195, 2, 217, 66, 0, 0, 0, 6, 73, 68, 65, 84, 78, 97, 220, 8, 207, 1,
            100, 0, 24, 156, 7, 150, 4, 1, 0, 0, 0, 0, 71, 69, 78, 67, 81, 209, 169, 190,
        ];
        std::fs::write(cwd.join("p.png"), png).unwrap();
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "p.png" })).await;
        assert!(!r.is_error);
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["content"][0]["type"], "image");
        assert_eq!(v["content"][0]["image"]["mimeType"], "image/png");
        assert_eq!(
            v["content"][0]["image"]["data"],
            base64::engine::general_purpose::STANDARD.encode(png)
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_escape_rejected() {
        let cwd = temp_cwd("read-esc");
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "../etc/passwd" })).await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── write ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn write_happy() {
        let cwd = temp_cwd("write");
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "a.txt", "content": "hello" }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "Wrote 5 bytes to a.txt");
        assert_eq!(r.details.as_ref().unwrap()["bytes"], 5);
        assert_eq!(std::fs::read_to_string(cwd.join("a.txt")).unwrap(), "hello");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn write_creates_parent_dirs() {
        let cwd = temp_cwd("write-nested");
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "nested/deep/a.txt", "content": "x" }),
        )
        .await;
        assert!(!r.is_error);
        assert!(cwd.join("nested/deep/a.txt").exists());
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn write_escape_rejected() {
        let cwd = temp_cwd("write-esc");
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "../evil.txt", "content": "x" }),
        )
        .await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── edit ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn edit_replaces_first_occurrence() {
        let cwd = temp_cwd("edit");
        std::fs::write(cwd.join("e.txt"), "X mid X").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "X", "new_text": "Y" }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "Edited e.txt");
        assert_eq!(r.details.as_ref().unwrap()["occurrences"], 1);
        let diff = r.details.as_ref().unwrap()["diff"].as_str().unwrap();
        assert!(
            diff.contains("-X mid X"),
            "the diff has the old line: {diff}"
        );
        assert!(
            diff.contains("+Y mid X"),
            "the diff has the new line: {diff}"
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("e.txt")).unwrap(),
            "Y mid X"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_replace_all() {
        let cwd = temp_cwd("edit-all");
        std::fs::write(cwd.join("e.txt"), "X mid X").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "X", "new_text": "Y", "replace_all": true }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["occurrences"], 2);
        assert_eq!(
            std::fs::read_to_string(cwd.join("e.txt")).unwrap(),
            "Y mid Y"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_empty_old_text_rejected() {
        // `old_text: ""` passes `contains("")` and `replacen`/`replace`
        // misbehave on it (pi rejects it) — a distinct guard.
        let cwd = temp_cwd("edit-empty");
        std::fs::write(cwd.join("e.txt"), "X mid X").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "", "new_text": "Y" }),
        )
        .await;
        assert!(r.is_error);
        assert!(text_of(&r).contains("must not be empty"));
        // The file is untouched.
        assert_eq!(
            std::fs::read_to_string(cwd.join("e.txt")).unwrap(),
            "X mid X"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_not_found_is_error() {
        let cwd = temp_cwd("edit-miss");
        std::fs::write(cwd.join("e.txt"), "no match here").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "e.txt", "old_text": "Z", "new_text": "W" }),
        )
        .await;
        assert!(r.is_error);
        assert!(text_of(&r).contains("old_text not found in e.txt"));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn edit_escape_rejected() {
        let cwd = temp_cwd("edit-esc");
        let r = execute_tool(
            &ctx(&cwd),
            "edit",
            &json!({ "path": "../e.txt", "old_text": "X", "new_text": "Y" }),
        )
        .await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── find ─────────────────────────────────────────────────────────────

    /// `fd` is available (spawn `fd --version`). Only the gated
    /// POSIX-shape fixture below reads it — hence the same `cfg`, or it is
    /// dead code on a Windows build.
    #[cfg(unix)]
    fn fd_available() -> bool {
        std::process::Command::new("fd")
            .arg("--version")
            .output()
            .is_ok()
    }

    /// The fixture asserts POSIX PATH SHAPE on both halves: the expected
    /// result is the literal `"sub/c.txt"`, and the `**/c.txt` pattern's `/`
    /// is what `glob_match` compares against the walkdir fallback's
    /// platform-relative path — on Windows that is `sub\c.txt`, so `**/…`
    /// matches nothing and the run reported `matches: 0`. The separator is
    /// `find`'s product behavior, not a fixture bug: gating keeps it honest
    /// rather than asserting a shape the tool does not produce on Windows.
    #[cfg(unix)]
    #[tokio::test]
    async fn find_matches_fixture() {
        let cwd = temp_cwd("find");
        std::fs::write(cwd.join("a.txt"), "a").unwrap();
        std::fs::write(cwd.join("b.txt"), "b").unwrap();
        std::fs::create_dir_all(cwd.join("sub")).unwrap();
        std::fs::write(cwd.join("sub/c.txt"), "c").unwrap();
        // Works via `fd` or the `walkdir` fallback.
        let r = execute_tool(&ctx(&cwd), "find", &json!({ "pattern": "a.txt" })).await;
        assert!(!r.is_error, "fd={}", fd_available());
        assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
        assert_eq!(text_of(&r), "a.txt");
        let r = execute_tool(&ctx(&cwd), "find", &json!({ "pattern": "**/c.txt" })).await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
        assert_eq!(text_of(&r), "sub/c.txt");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn find_max_results_truncates() {
        let cwd = temp_cwd("find-max");
        for name in ["a.txt", "b.txt", "c.txt"] {
            std::fs::write(cwd.join(name), name).unwrap();
        }
        let r = execute_tool(
            &ctx(&cwd),
            "find",
            &json!({ "pattern": "*", "max_results": 1 }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
        assert_eq!(r.details.as_ref().unwrap()["truncated"], true);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn find_escape_rejected() {
        let cwd = temp_cwd("find-esc");
        let r = execute_tool(
            &ctx(&cwd),
            "find",
            &json!({ "pattern": "*", "path": "../" }),
        )
        .await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("a*.txt", "a1.txt"));
        assert!(!glob_match("a*", "a/b.txt"));
        assert!(glob_match("**/c.txt", "sub/c.txt"));
        assert!(!glob_match("**/c.txt", "a.txt"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ab"));
    }

    // ── grep ─────────────────────────────────────────────────────────────

    /// `ripgrep` is available (spawn `rg --version`).
    fn rg_available() -> bool {
        std::process::Command::new("rg")
            .arg("--version")
            .output()
            .is_ok()
    }

    #[tokio::test]
    async fn grep_matches_fixture() {
        // Guard on `rg` availability (NOT `#[ignore]`).
        if !rg_available() {
            return;
        }
        let cwd = temp_cwd("grep");
        std::fs::write(cwd.join("a.txt"), "hello needle here\n").unwrap();
        let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "needle" })).await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
        assert!(text_of(&r).contains("a.txt"));
        assert!(text_of(&r).contains("needle"));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn grep_option_like_pattern_is_matched_literally() {
        // Guard on `rg` availability (NOT `#[ignore]`). Regression test for
        // the `rg` argument-injection hole: WITHOUT a `--` separator, a
        // pattern like `--pre=sh` is parsed by `rg` as an OPTION (`--pre`
        // runs a program on every input file before searching — arbitrary
        // process execution, un-gated), and the positional shift turns the
        // validated search dir into the pattern and the search path into
        // `.` (the desktop CWD). With `--` the pattern is matched literally.
        if !rg_available() {
            return;
        }
        let cwd = temp_cwd("grep-inj");
        // The option-like pattern as LITERAL text in a file.
        std::fs::write(cwd.join("a.txt"), "--pre=sh needle\n").unwrap();
        let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "--pre=sh" })).await;
        assert!(
            !r.is_error,
            "an option-like pattern must be searched literally, not parsed as an option: {r:?}"
        );
        assert_eq!(r.details.as_ref().unwrap()["matches"], 1);
        assert!(text_of(&r).contains("a.txt"), "the literal text matched");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn grep_missing_rg_is_error() {
        // Only the missing-`rg` path (skip when `rg` is installed).
        if rg_available() {
            return;
        }
        let cwd = temp_cwd("grep-missing");
        let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "x" })).await;
        assert!(r.is_error, "a missing rg is a tool error");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// (finding) `exec_grep` has a `DEFAULT_MAX_RESULTS`-style cap (like
    /// `find`): a `grep` with no `max_results` is NOT unbounded, and the
    /// OUTPUT is bounded to `max_results` lines (`rg -m` is PER-FILE, so
    /// the total can exceed it — the executor truncates the output).
    #[tokio::test]
    async fn grep_output_bounded_by_max_results() {
        // Guard on `rg` availability (NOT `#[ignore]`).
        if !rg_available() {
            return;
        }
        let cwd = temp_cwd("grep-cap");
        for name in ["a.txt", "b.txt", "c.txt"] {
            std::fs::write(cwd.join(name), "needle\n").unwrap();
        }
        // No `max_results` (the default cap) — 3 matches, not truncated.
        let r = execute_tool(&ctx(&cwd), "grep", &json!({ "pattern": "needle" })).await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["matches"], 3);
        assert_eq!(r.details.as_ref().unwrap()["truncated"], false);
        // `max_results: 2` — `-m` is PER-FILE (all 3 matches still come
        // back from `rg`), but the OUTPUT is bounded to 2 lines.
        let r = execute_tool(
            &ctx(&cwd),
            "grep",
            &json!({ "pattern": "needle", "max_results": 2 }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["matches"], 3);
        assert_eq!(r.details.as_ref().unwrap()["truncated"], true);
        assert_eq!(
            text_of(&r).lines().count(),
            2,
            "the output is bounded to max_results lines"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── ls ───────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ls_lists_entries() {
        let cwd = temp_cwd("ls");
        std::fs::write(cwd.join("a.txt"), "a").unwrap();
        std::fs::write(cwd.join("b.txt"), "b").unwrap();
        std::fs::create_dir_all(cwd.join("d")).unwrap();
        let r = execute_tool(&ctx(&cwd), "ls", &json!({})).await;
        assert!(!r.is_error);
        assert_eq!(r.details.as_ref().unwrap()["entries"], 3);
        let lines: Vec<&str> = text_of(&r).lines().collect();
        assert_eq!(lines, vec!["a.txt", "b.txt", "d/"]);
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn ls_escape_rejected() {
        let cwd = temp_cwd("ls-esc");
        let r = execute_tool(&ctx(&cwd), "ls", &json!({ "path": "../" })).await;
        assert!(r.is_error, "a sandbox escape must be rejected");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── the policy tiers (ADR 0030) ───────────────────────────────────────

    /// The point of the whole feature: with `reads: Allow` the gate is the
    /// only decision point, so a read BEYOND the boundary lands. The
    /// `Sandboxed` contrast is `read_escape_rejected` above (the same path,
    /// rejected) — without it this test would pass vacuously.
    #[tokio::test]
    async fn read_beyond_the_boundary_succeeds_when_reads_allow() {
        use crate::agent::policy::{AccessPolicy, FilePolicy};
        // A PARENT dir so the out-of-boundary file stays inside the test's
        // own temp tree (`../outside.txt` from the cwd lands in the parent).
        let parent = temp_cwd("tier-read");
        let cwd = parent.join("space");
        std::fs::create_dir_all(&cwd).unwrap();
        let target = parent.join("outside.txt");
        std::fs::write(&target, "OUTSIDE-OK").unwrap();
        // The premise: the path is genuinely beyond the boundary.
        assert!(!target.starts_with(&cwd));

        let policy = FilePolicy {
            reads: AccessPolicy::Allow,
            ..FilePolicy::default()
        };
        let r = execute_tool(
            &ctx_policy(&cwd, policy),
            "read",
            &json!({ "path": "../outside.txt" }),
        )
        .await;
        assert!(!r.is_error, "an allowed read must land: {}", text_of(&r));
        assert_eq!(text_of(&r), "OUTSIDE-OK");

        // And the ABSOLUTE form lands too (the unrestricted root is `/`).
        let r = execute_tool(
            &ctx_policy(&cwd, policy),
            "read",
            &json!({ "path": target }),
        )
        .await;
        assert!(!r.is_error, "{}", text_of(&r));

        // The CONTRAST (so this is not vacuous): the same file under
        // `reads: Sandboxed` is rejected.
        let r = execute_tool(&ctx(&cwd), "read", &json!({ "path": "../outside.txt" })).await;
        assert!(r.is_error, "a sandboxed read must NOT land");

        let _ = std::fs::remove_dir_all(&parent);
    }

    /// The read boundary is a SET: under `Sandboxed`, a file in a SECOND
    /// boundary root (a skill dir at the repo root — outside the session
    /// `cwd`) is readable, while a file in neither root is not.
    #[tokio::test]
    async fn read_reaches_a_second_boundary_root_when_sandboxed() {
        let cwd = temp_cwd("tier-multi-root");
        let skill_dir = temp_cwd("tier-multi-root-skills");
        std::fs::write(skill_dir.join("SKILL.md"), "SKILL-BODY").unwrap();
        let ToolCtx { boundary, .. } = ctx(&cwd);
        // The boundary the harness would hand the tools: cwd FIRST, then the
        // discovery root.
        let c = ToolCtx {
            boundary: vec![cwd.clone(), skill_dir.clone()],
            ..ctx(&cwd)
        };
        assert_eq!(boundary, vec![cwd.clone()]);

        let r = execute_tool(&c, "read", &json!({ "path": skill_dir.join("SKILL.md") })).await;
        assert!(
            !r.is_error,
            "a boundary root must be readable: {}",
            text_of(&r)
        );
        assert_eq!(text_of(&r), "SKILL-BODY");

        // A dir that is neither the cwd nor a root is still rejected.
        let elsewhere = temp_cwd("tier-multi-root-elsewhere");
        std::fs::write(elsewhere.join("no.txt"), "NO").unwrap();
        let r = execute_tool(&c, "read", &json!({ "path": elsewhere.join("no.txt") })).await;
        assert!(r.is_error, "a non-root must stay rejected");

        let _ = std::fs::remove_dir_all(&cwd);
        let _ = std::fs::remove_dir_all(&skill_dir);
        let _ = std::fs::remove_dir_all(&elsewhere);
    }

    /// `writes: Allow` widens the WRITE direction the same way (an approved
    /// `Ask` must be able to land, or the prompt lies). The `Sandboxed`
    /// contrast is `write_escape_rejected` above.
    #[tokio::test]
    async fn write_beyond_the_boundary_succeeds_when_writes_allow() {
        use crate::agent::policy::{AccessPolicy, FilePolicy};
        let parent = temp_cwd("tier-write");
        let cwd = parent.join("space");
        std::fs::create_dir_all(&cwd).unwrap();
        let policy = FilePolicy {
            writes: AccessPolicy::Allow,
            ..FilePolicy::default()
        };
        let r = execute_tool(
            &ctx_policy(&cwd, policy),
            "write",
            &json!({ "path": "../outside.txt", "content": "landed" }),
        )
        .await;
        assert!(!r.is_error, "an allowed write must land: {}", text_of(&r));
        let landed = parent.join("outside.txt");
        assert_eq!(std::fs::read_to_string(&landed).unwrap(), "landed");
        // The CONTRAST: the same path under `writes: Sandboxed` is refused
        // and creates nothing.
        let r = execute_tool(
            &ctx(&cwd),
            "write",
            &json!({ "path": "../other.txt", "content": "nope" }),
        )
        .await;
        assert!(r.is_error, "a sandboxed write must NOT land");
        assert!(!parent.join("other.txt").exists());
        let _ = std::fs::remove_dir_all(&parent);
    }

    /// The DENY-LIST is unconditional: a write whose resolved path is under a
    /// `protected` dir is refused under EVERY policy — `Sandboxed` (where it
    /// is also outside the boundary) AND `Allow` (where the boundary admits
    /// it). A model that can rewrite `~/.agents/skills/*/SKILL.md` rewrites
    /// its own instructions, so this is never a prompt and never widened.
    #[tokio::test]
    async fn write_into_a_protected_dir_is_refused_under_every_policy() {
        use crate::agent::policy::{AccessPolicy, FilePolicy};
        for policy_writes in [
            AccessPolicy::Sandboxed,
            AccessPolicy::Ask,
            AccessPolicy::Allow,
        ] {
            let cwd = temp_cwd("tier-protect");
            // A PINNED protected dir INSIDE the cwd: the root check passes
            // for it, so only the deny-list can refuse the write (which is
            // exactly what makes this the deny-list's own test).
            let protected = cwd.join(".agents/skills");
            std::fs::create_dir_all(&protected).unwrap();
            std::fs::write(protected.join("SKILL.md"), "OLD").unwrap();
            let c = ToolCtx {
                protected: vec![protected.clone()],
                file_policy: FilePolicy {
                    writes: policy_writes,
                    ..FilePolicy::default()
                },
                ..ctx(&cwd)
            };
            let r = execute_tool(
                &c,
                "write",
                &json!({ "path": ".agents/skills/SKILL.md", "content": "PWNED" }),
            )
            .await;
            assert!(
                r.is_error,
                "{policy_writes:?}: a write into a protected dir must be refused"
            );
            let text = text_of(&r).to_string();
            assert!(text.contains("protected"), "{policy_writes:?}: {text}");
            assert_eq!(
                std::fs::read_to_string(protected.join("SKILL.md")).unwrap(),
                "OLD",
                "{policy_writes:?}: the file must be untouched"
            );
            // `edit` is a write, so the deny-list covers it too.
            let r = execute_tool(
                &c,
                "edit",
                &json!({ "path": ".agents/skills/SKILL.md", "old_text": "OLD", "new_text": "PWNED" }),
            )
            .await;
            assert!(r.is_error, "{policy_writes:?}: `edit` must be refused too");
            // And a NON-protected path in the same cwd is unaffected.
            let ok = execute_tool(
                &c,
                "write",
                &json!({ "path": "notes.txt", "content": "fine" }),
            )
            .await;
            assert!(!ok.is_error, "{policy_writes:?}: {}", text_of(&ok));
            let _ = std::fs::remove_dir_all(&cwd);
        }
    }

    /// The deny-list is a PREFIX check on the RESOLVED path, so a new file
    /// created under a protected dir is refused too (the dir need not hold
    /// the file yet).
    #[tokio::test]
    async fn write_a_new_file_under_a_protected_dir_is_refused() {
        use crate::agent::policy::{AccessPolicy, FilePolicy};
        let cwd = temp_cwd("tier-protect-new");
        let protected = cwd.join(".pi/agent/skills");
        std::fs::create_dir_all(&protected).unwrap();
        let c = ToolCtx {
            protected: vec![protected.clone()],
            file_policy: FilePolicy {
                writes: AccessPolicy::Allow,
                ..FilePolicy::default()
            },
            ..ctx(&cwd)
        };
        let r = execute_tool(
            &c,
            "write",
            &json!({ "path": ".pi/agent/skills/new/SKILL.md", "content": "PWNED" }),
        )
        .await;
        assert!(r.is_error, "a new file under a protected dir is refused");
        assert!(!protected.join("new").exists(), "nothing is created");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The deny-list compares CANONICALLY, so a RAW (never-canonicalized)
    /// protected entry still refuses a write whose `resolved` path reaches it
    /// through a symlink.
    ///
    /// CI evidence for the bug this pins: on macOS `std::env::temp_dir()` lives
    /// under `/var`, which is a symlink to `/private/var`, and on Windows
    /// `canonicalize` returns the `\\?\C:\…` VERBATIM form while a
    /// `temp_dir()`-joined entry is the plain `C:\…` form. Either way the raw
    /// entry no longer prefixed the canonical `resolved`, `starts_with` missed,
    /// and the protected write SUCCEEDED — i.e. the "refused in EVERY policy"
    /// floor silently no-opped for any caller that filled `ctx.protected` from
    /// a non-canonicalized source. Linux temp dirs carry no such symlink, so
    /// the mismatch is MANUFACTURED here with a symlinked dir (unix); the
    /// non-unix leg leans on the platform's own raw-vs-canonical spelling.
    #[tokio::test]
    async fn a_raw_protected_entry_still_refuses_a_canonical_path_under_it() {
        use crate::agent::policy::{AccessPolicy, FilePolicy};
        let base = temp_cwd("protect-sym");
        let real = base.join("real");
        let skills = real.join(".agents/skills");
        std::fs::create_dir_all(&skills).unwrap();
        std::fs::write(skills.join("SKILL.md"), "OLD").unwrap();

        // The protected entry stays RAW, and the session `cwd` is the path the
        // model would join from — on unix that is the SYMLINK, so the
        // executor's canonical `resolved` comes back through `real` and shares
        // no textual prefix with the `link`-spelled entry.
        #[cfg(unix)]
        let (cwd, protected) = {
            std::os::unix::fs::symlink(&real, base.join("link")).unwrap();
            let link = base.join("link");
            let protected = link.join(".agents/skills");
            (link, protected)
        };
        #[cfg(not(unix))]
        let (cwd, protected) = {
            // No symlink privileges: `temp_dir()`'s own spelling is already
            // the raw one that `canonicalize` rewrites on Windows.
            (real.clone(), real.join(".agents/skills"))
        };

        // The fixture's PREMISE (so a platform where the two spellings agree
        // says so loudly instead of testing nothing): the entry handed to
        // `ctx.protected` is genuinely NOT canonical.
        assert_ne!(
            protected.canonicalize().unwrap(),
            protected,
            "the fixture must hand the deny-list a RAW entry, got {}",
            protected.display()
        );

        let c = ToolCtx {
            protected: vec![protected.clone()],
            file_policy: FilePolicy {
                writes: AccessPolicy::Allow,
                ..FilePolicy::default()
            },
            ..ctx(&cwd)
        };
        let r = execute_tool(
            &c,
            "write",
            &json!({ "path": ".agents/skills/SKILL.md", "content": "PWNED" }),
        )
        .await;
        assert!(
            r.is_error,
            "a raw protected entry must still refuse the write"
        );
        let text = text_of(&r).to_string();
        assert!(text.contains("protected"), "{text}");
        // The refusal stays HONEST about which entry it matched: the ORIGINAL
        // (raw) entry, not the canonicalized form the comparison used.
        assert!(
            text.contains(&protected.to_string_lossy().into_owned()),
            "the refusal must name the entry as the caller spelled it: {text}"
        );
        assert_eq!(
            std::fs::read_to_string(skills.join("SKILL.md")).unwrap(),
            "OLD",
            "the file behind the symlink must be untouched"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The MIRROR of `a_raw_protected_entry_still_refuses_a_canonical_path_under_it`:
    /// the ENTRIES are canonical (what production hands over, via
    /// `boundary::protected_dirs`) and the COMPARED path is RAW. That is exactly
    /// what the GATE does — `loop.rs::deny_tool_result` passes the model's `path`
    /// argument verbatim — so this seam must canonicalize its ARGUMENT side too.
    ///
    /// CI evidence (branch run 38702103063, the `f9b5c17`/`3fd702f` family):
    /// macOS `a_gate_denial_of_a_protected_dir_uses_the_protected_wording` failed
    /// with `got "write: path escapes the session sandbox: …"` (Windows the same
    /// with the `\\?\C:\…` verbatim spelling): the gate's raw argument shared no
    /// prefix with its canonical entry, so the deny-list branch never fired and
    /// the refusal fell through to the boundary's wording.
    ///
    /// The target does NOT exist (a gate refusal is usually a brand-new write),
    /// so a plain `canonicalize` on the argument fails — this is the shape
    /// `canonicalize_lenient` (the peel `FsBackend::resolve` uses) exists for.
    /// Linux `temp_dir()` is already canonical, so the mismatch is MANUFACTURED
    /// with a `link -> real` symlink exactly as 3fd702f did: entries spelled
    /// through `real` (canonical), the argument through `link` (raw).
    #[cfg(unix)]
    #[test]
    fn a_raw_argument_still_refuses_against_canonical_protected_entries() {
        let base = temp_cwd("protect-gate-arg");
        let real = base.join("real");
        let skills = real.join(".agents/skills");
        std::fs::create_dir_all(&skills).unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, base.join("link")).unwrap();
        let link = base.join("link");

        // The fixture's PREMISE (so a platform where the two spellings agree
        // fails loudly instead of testing nothing): the path the model would
        // send is genuinely NOT canonical.
        assert_ne!(
            link.canonicalize().unwrap(),
            link,
            "the argument must be spelled through a symlink, got {}",
            link.display()
        );

        // The ENTRIES as production builds them: canonical.
        let entries = vec![skills.clone()];
        // The ARGUMENT as the model sends it: the RAW spelling, under a
        // directory that does not exist yet (the gate's usual case).
        let raw = link.join(".agents/skills/pwn/SKILL.md");

        let refusal = protected_refusal_text(&entries, "write", &raw)
            .expect("a raw argument under a canonical protected entry must refuse");
        assert!(
            refusal.contains("protected agent-definition directory"),
            "{refusal}"
        );
        // The refusal names the entry the caller registered (canonical here) —
        // the SAME sentence the executor produces.
        assert!(
            refusal.contains(&skills.to_string_lossy().into_owned()),
            "the refusal must name the protected entry: {refusal}"
        );

        // IDEMPOTENCE (the executor callers): handing the already-canonical
        // form of the same path answers identically — canonical input
        // canonicalizes to itself, so nothing about that seam changed.
        let canon = skills.join("pwn/SKILL.md");
        let again = protected_refusal_text(&entries, "write", &canon)
            .expect("the canonical spelling refuses too (unchanged behaviour)");
        assert!(
            again.contains("protected agent-definition directory"),
            "{again}"
        );

        // And the fix does not turn the deny-list into a blanket refusal: a raw
        // path that resolves OUTSIDE the entries is still `None`.
        let outside = link.join("notes/todo.md");
        assert_eq!(
            protected_refusal_text(&entries, "write", &outside),
            None,
            "a non-protected raw path must not be refused"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    /// `bash` IGNORES the boundary (its `Sandboxed` tier is Landlock, Task 5):
    /// a sandboxed read cannot reach a file that `bash` can happily cat, so
    /// the boundary must not pretend to bound the shell.
    #[tokio::test]
    async fn bash_ignores_the_file_boundary() {
        let cwd = temp_cwd("tier-bash");
        let sibling = temp_cwd("tier-bash-sibling");
        std::fs::write(sibling.join("f.txt"), "FROM-OUTSIDE").unwrap();
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({ "command": format!("cat {}", sibling.join("f.txt").display()) }),
        )
        .await;
        assert!(
            !r.is_error,
            "bash is not boundary-sandboxed: {}",
            text_of(&r)
        );
        assert_eq!(text_of(&r), "FROM-OUTSIDE");
        let _ = std::fs::remove_dir_all(&cwd);
        let _ = std::fs::remove_dir_all(&sibling);
    }

    // ── list_skills / read_skill ─────────────────────────────────────────

    /// A skill root holding `<name>/SKILL.md`; returns the root.
    fn skill_root(tag: &str, name: &str, content: &str) -> PathBuf {
        let root = temp_cwd(tag);
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("SKILL.md"), content).unwrap();
        root
    }

    /// `roots` as the ONLY discovery roots — the skill tools take their
    /// roots from `ToolCtx::skill_roots` so a test never reads the real
    /// `~/.agents/skills` (the `discover_in_roots` convention). The policy is
    /// the same `Sandboxed` containment tier as [`ctx`], whose `boundary` is
    /// the `cwd` ALONE — a FIXTURE choice, not the shipped read Boundary,
    /// which ADR 0030 widens with the discovery roots. So a `read` of a skill
    /// path is out of bounds HERE only because of this fixture's root set;
    /// nothing below is a claim about what `read` does in a real session.
    fn ctx_with_roots(cwd: &Path, roots: &[PathBuf]) -> ToolCtx {
        ToolCtx {
            skill_roots: Some(
                roots
                    .iter()
                    .map(|r| (r.clone(), crate::skills::SkillScope::Space))
                    .collect(),
            ),
            ..ctx(cwd)
        }
    }

    #[tokio::test]
    async fn list_skills_lists_name_scope_and_description() {
        let cwd = temp_cwd("skills-list");
        let root = skill_root(
            "skills-list-root",
            "alpha",
            "---\nname: alpha\ndescription: Does alpha.\n---\nBody.\n",
        );
        let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "alpha (space): Does alpha.");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn list_skills_empty_is_none_not_error() {
        let cwd = temp_cwd("skills-list-empty");
        let root = temp_cwd("skills-list-empty-root");
        let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
        assert!(!r.is_error, "an empty catalog is not a failure");
        assert_eq!(text_of(&r), "none");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn list_skills_omits_model_hidden_skills() {
        // ADR 0036: to the Agent a model-hidden skill is NOT in the catalog —
        // its name AND its description (the routing hint) are both absent.
        let cwd = temp_cwd("skills-list-hidden");
        let a = skill_root(
            "tag-a",
            "alpha",
            "---\nname: alpha\ndescription: D\n---\nA.\n",
        );
        let o = skill_root(
            "tag-o",
            "omega",
            "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
        );
        let r = execute_tool(&ctx_with_roots(&cwd, &[a, o]), "list_skills", &json!({})).await;
        assert!(!r.is_error, "{}", text_of(&r));
        assert_eq!(text_of(&r), "alpha (space): D");
        assert!(!text_of(&r).contains("omega"), "{}", text_of(&r));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn list_skills_is_none_when_every_skill_is_model_hidden() {
        // A Space can present an EMPTY catalog to the Agent while the Skills
        // dialog shows a full one — that is the feature, and an empty catalog
        // is still not a failure (cf. `list_skills_empty_is_none_not_error`).
        let cwd = temp_cwd("skills-list-all-hidden");
        let root = skill_root(
            "skills-list-all-hidden-root",
            "omega",
            "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
        );
        let r = execute_tool(&ctx_with_roots(&cwd, &[root]), "list_skills", &json!({})).await;
        assert!(!r.is_error, "an empty catalog is not a failure");
        assert_eq!(text_of(&r), "none");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    /// The names the `<skills>` section ADVERTISES, in the prompt (the
    /// `<name>` values, inside the section ONLY — the rest of the prompt must
    /// not be scanned, or a skill name echoed in another section would count).
    fn advertised_in_prompt(prompt: &str) -> std::collections::BTreeSet<String> {
        let section = prompt
            .split("<skills>\n")
            .nth(1)
            .and_then(|rest| rest.split("\n</skills>").next())
            .expect("the `<skills>` section is present (`read_skill` is advertised)");
        section
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix("<name>")
                    .and_then(|s| s.strip_suffix("</name>"))
                    .map(str::to_string)
            })
            .collect()
    }

    /// The names the `list_skills` output ADVERTISES: one
    /// `{name} ({scope}): {description}` line each (`none` → the empty set).
    fn advertised_by_tool(text: &str) -> std::collections::BTreeSet<String> {
        if text == "none" {
            return std::collections::BTreeSet::new();
        }
        text.lines()
            .map(|line| {
                line.split_once(" (")
                    .map(|(name, _)| name.to_string())
                    .unwrap_or_else(|| panic!("unexpected `list_skills` line {line:?}"))
            })
            .collect()
    }

    /// A `ToolSpec` of the shape `prompt.rs`'s test helper builds (the
    /// `parameters` schema is irrelevant here — only the NAME decides whether
    /// the `<skills>` section is emitted at all).
    fn spec(name: &str) -> crate::agent::harness::provider::ToolSpec {
        crate::agent::harness::provider::ToolSpec {
            name: name.to_string(),
            description: format!("The {name} tool."),
            parameters: serde_json::Value::Null,
        }
    }

    /// THE anti-drift test (ADR 0029's "the tool's catalog cannot drift from
    /// the prompt's catalog" + ADR 0036's "to the Agent a model-hidden Skill
    /// is not in the catalog").
    ///
    /// The two CATALOGS are produced by two SEPARATE seams — the prompt's
    /// `format_skills_for_prompt` (`harness/prompt.rs`) and the tools'
    /// `model_skills_for` → `exec_list_skills` (here) — which today share only
    /// the `model_visible` PREDICATE. Every other ADR 0036 test checks ONE seam,
    /// so either seam growing its own second filter (a scope test, a count cap,
    /// a re-filter of an already-filtered list) would leave the OTHER advertising
    /// a skill the model was never told about — or, worse, promise a skill that
    /// `read_skill` then refuses. This gives BOTH seams the same discovery ROOTS
    /// (the prompt builder is handed the discovered rows directly; the tools
    /// RE-DISCOVER from those same roots, so the rows are equal by construction
    /// and not by sharing a `Vec`), and asserts the two advertised name sets are
    /// EQUAL, so the drift is a test failure and not a transcript surprise. The
    /// mixed visible/hidden × Space/User fixture is what gives the equality its
    /// teeth: a per-seam scope filter, or a seam that stops filtering on the
    /// flag, both break the set equality rather than passing silently.
    #[tokio::test]
    async fn the_prompt_catalog_and_the_list_skills_catalog_agree() {
        use crate::agent::harness::prompt::{build_main_prompt, PromptContext};
        use crate::skills::{discover_in_roots, SkillScope};

        // The prompt builder walks UP from `cwd` for `AGENTS.md`s; an empty
        // `.git` here bounds that walk at this scratch dir (the `prompt.rs`
        // `scratch()` convention) so a stray `AGENTS.md` in `/tmp` cannot
        // inject a `<project_context>` section (which could mention a skill
        // name and pollute nothing here, but keeps the fixture honest).
        let cwd = temp_cwd("skills-catalog-drift");
        std::fs::create_dir_all(cwd.join(".git")).unwrap();

        // One skill per root (`skill_root` makes exactly one), four roots:
        // visible + hidden, each at BOTH scopes.
        let roots: Vec<(PathBuf, SkillScope)> = vec![
            (
                skill_root(
                    "drift-visible-space",
                    "alpha",
                    "---\nname: alpha\ndescription: Does alpha.\n---\nA.\n",
                ),
                SkillScope::Space,
            ),
            (
                skill_root(
                    "drift-visible-user",
                    "beta",
                    "---\nname: beta\ndescription: Does beta.\n---\nB.\n",
                ),
                SkillScope::User,
            ),
            (
                skill_root(
                    "drift-hidden-space",
                    "omega",
                    "---\nname: omega\ndescription: Does omega.\ndisable-model-invocation: true\n---\nO.\n",
                ),
                SkillScope::Space,
            ),
            (
                skill_root(
                    "drift-hidden-user",
                    "zeta",
                    "---\nname: zeta\ndescription: Does zeta.\ndisable-model-invocation: true\n---\nZ.\n",
                ),
                SkillScope::User,
            ),
        ];
        // The SAME rows for both seams: the prompt builder is handed the
        // discovered set directly, and the tool ctx takes the SAME roots (the
        // tools re-discover them through `skills_for` — `skill_roots` is the
        // pinned-root seam, never a pre-filtered list, which is exactly what
        // this test is guarding).
        let skills = discover_in_roots(&roots);
        assert_eq!(
            skills.len(),
            4,
            "the fixture must discover all four skills (the premise)"
        );
        let paths: Vec<PathBuf> = roots.iter().map(|(p, _)| p.clone()).collect();

        // Seam 1: the prompt's `<skills>` section. `read_skill` is advertised so
        // the section is emitted at all (ADR 0017's skill-loading-tool guard).
        let tool_specs = vec![spec("read_skill"), spec("list_skills")];
        let prompt = build_main_prompt(&PromptContext {
            cwd: &cwd,
            // `Path::new("")` skips the global context file (the documented
            // convention: `build_main_prompt` treats an empty dir as "none").
            agent_dir: std::path::Path::new(""),
            tools: &tool_specs,
            skills: &skills,
        });
        let prompt_names = advertised_in_prompt(&prompt);

        // Seam 2: the `list_skills` output, from the SAME roots. Built from the
        // SAME `ctx` helper as `ctx_with_roots` — but pinned per-root instead of
        // all-`Space`, because the fixture needs BOTH scopes (a per-seam scope
        // filter is one of the drifts this test must catch).
        let ctx = ToolCtx {
            skill_roots: Some(roots.clone()),
            ..ctx(&cwd)
        };
        let r = execute_tool(&ctx, "list_skills", &json!({})).await;
        assert!(!r.is_error, "{}", text_of(&r));
        let tool_names = advertised_by_tool(text_of(&r));

        // THE anti-drift assertion.
        assert_eq!(
            prompt_names, tool_names,
            "the `<skills>` section and `list_skills` advertise DIFFERENT catalogs"
        );
        // And neither may name a model-hidden skill (ADR 0036): the equality
        // above alone would hold for two identically-WRONG catalogs.
        let visible: std::collections::BTreeSet<String> = ["alpha".to_string(), "beta".to_string()]
            .into_iter()
            .collect();
        assert_eq!(
            prompt_names, visible,
            "the advertised catalog is not exactly the model-visible set"
        );

        for root in paths {
            let _ = std::fs::remove_dir_all(&root);
        }
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_reads_skill_md_without_touching_the_sandbox() {
        // THE regression: the skill lives OUTSIDE `ctx.cwd` (the real-world
        // case — user skills live in `~/.agents/skills`), so `read` rejects
        // it and `read_skill` must not.
        let cwd = temp_cwd("skill-outside");
        let root = skill_root(
            "skill-outside-root",
            "alpha",
            "---\nname: alpha\ndescription: Does alpha.\n---\nUse the alpha way.\n",
        );
        let skill_md = root.join("alpha/SKILL.md");
        assert!(
            !skill_md.starts_with(&cwd),
            "the fixture must sit outside the session sandbox"
        );
        // The premise: the sandboxed `read` genuinely rejects it.
        let escaped = execute_tool(
            &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
            "read",
            &json!({ "path": skill_md }),
        )
        .await;
        assert!(escaped.is_error, "`read` must still reject the escape");

        let r = execute_tool(
            &ctx_with_roots(&cwd, &[root]),
            "read_skill",
            &json!({ "name": "alpha" }),
        )
        .await;
        assert!(!r.is_error, "{}", text_of(&r));
        assert_eq!(text_of(&r), "Use the alpha way.");
        assert_eq!(r.details.as_ref().unwrap()["skill"], "alpha");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_name_is_case_insensitive_and_flattens_nothing() {
        let cwd = temp_cwd("skill-case");
        let root = skill_root("skill-case-root", "Alpha", "---\nname: Alpha\n---\nBody.\n");
        let r = execute_tool(
            &ctx_with_roots(&cwd, &[root]),
            "read_skill",
            &json!({ "name": "alpha" }),
        )
        .await;
        assert!(!r.is_error);
        assert_eq!(text_of(&r), "Body.");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_bundled_file_resolves_relative_to_the_skill_dir() {
        let cwd = temp_cwd("skill-bundle");
        let root = skill_root(
            "skill-bundle-root",
            "discuss",
            "---\nname: discuss\n---\nSee [adr](./adr-format.md).\n",
        );
        std::fs::write(root.join("discuss/adr-format.md"), "# ADR format\n").unwrap();
        let r = execute_tool(
            &ctx_with_roots(&cwd, &[root]),
            "read_skill",
            &json!({ "name": "discuss", "path": "./adr-format.md" }),
        )
        .await;
        assert!(!r.is_error, "{}", text_of(&r));
        // A bundled file is returned VERBATIM (the file's own bytes, unlike
        // `read`'s line-join) — the whole point is reaching the exact file a
        // skill references.
        assert_eq!(text_of(&r), "# ADR format\n");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_bundled_escape_is_rejected() {
        // A `path` that leaves the SKILL's dir must be rejected even though
        // it stays inside a sibling skill — the boundary is the skill dir,
        // not the skill ROOT.
        let cwd = temp_cwd("skill-esc");
        let root = skill_root("skill-esc-root", "alpha", "---\nname: alpha\n---\nA.\n");
        std::fs::write(root.join("secret.txt"), "top secret").unwrap();
        let r = execute_tool(
            &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
            "read_skill",
            &json!({ "name": "alpha", "path": "../secret.txt" }),
        )
        .await;
        assert!(r.is_error, "an escape from the skill dir must be rejected");
        assert!(
            !text_of(&r).contains("top secret"),
            "the escaped content must not leak: {}",
            text_of(&r)
        );
        // An ABSOLUTE path outside the skill dir is rejected too.
        let outside = std::env::temp_dir().join("definitely-not-in-the-skill.txt");
        let r = execute_tool(
            &ctx_with_roots(&cwd, &[root]),
            "read_skill",
            &json!({ "name": "alpha", "path": outside }),
        )
        .await;
        assert!(
            r.is_error,
            "an absolute path outside the skill dir is rejected"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_unknown_name_is_error_with_the_catalog() {
        let cwd = temp_cwd("skill-unknown");
        let root = skill_root("skill-unknown-root", "alpha", "---\nname: alpha\n---\nA.\n");
        let r = execute_tool(
            &ctx_with_roots(&cwd, &[root]),
            "read_skill",
            &json!({ "name": "nope" }),
        )
        .await;
        assert!(r.is_error);
        let text = text_of(&r);
        assert!(text.contains("unknown skill"), "{text}");
        // The hint points at the tool that WOULD have listed them.
        assert!(text.contains("list_skills"), "{text}");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_rejects_a_model_hidden_skill_with_the_mention_route() {
        // A hidden name resolves (so the model is not lied to) but is refused,
        // and the failure names the ONE route that works: the user's `$name`
        // Mention. The whole string is pinned — it is the model's only
        // instruction about that route (ADR 0036).
        let cwd = temp_cwd("skill-hidden-route");
        let root = skill_root(
            "skill-hidden-route-root",
            "omega",
            "---\nname: omega\ndescription: D\ndisable-model-invocation: true\n---\nO.\n",
        );
        let r = execute_tool(
            &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
            "read_skill",
            &json!({ "name": "omega" }),
        )
        .await;
        assert!(r.is_error, "a model-hidden skill must be refused");
        assert_eq!(
            text_of(&r),
            "read_skill: \"omega\" is model-hidden — the Agent may not load it; ask the user to send $omega"
        );

        // The `$` Mention token grammar is LOWERCASE-only (ADR 0031), so the
        // route echoed back is the lowercased key: `$DeBuG` expands nothing
        // while `$debug` does.
        let dbg = skill_root(
            "skill-hidden-route-debug",
            "DeBuG",
            "---\nname: DeBuG\ndisable-model-invocation: true\n---\nD.\n",
        );
        let r = execute_tool(
            &ctx_with_roots(&cwd, &[dbg]),
            "read_skill",
            &json!({ "name": "debug" }),
        )
        .await;
        assert!(r.is_error);
        assert!(
            text_of(&r).ends_with("ask the user to send $debug"),
            "{}",
            text_of(&r)
        );
        assert!(
            !text_of(&r).contains(&root.to_string_lossy().into_owned()),
            "the failure must not disclose a path: {}",
            text_of(&r)
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_refuses_a_non_mentionable_hidden_skill_without_a_dead_token() {
        // A skill name is only screened by `is_tag_safe_name`, so `has space`
        // IS discovered (a space is tag-safe) — yet the `$` token grammar
        // cannot carry it (ADR 0031), so the usual refusal would point the
        // model AND the user at a `$has space` token that expands NOTHING.
        // The refusal must therefore stay actionable without promising a
        // token: no `$mention` suggestion at all.
        //
        // (Discovery note: the frontmatter `name` WINS over the
        // directory-name fallback, and `is_tag_safe_name` admits a space, so
        // this frontmatter is the path that actually yields a hidden,
        // non-mentionable skill.)
        let cwd = temp_cwd("skill-hidden-unmentionable");
        let root = skill_root(
            "skill-hidden-unmentionable-root",
            "has-space",
            "---\nname: has space\ndescription: D\ndisable-model-invocation: true\n---\nH.\n",
        );
        let r = execute_tool(
            &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
            "read_skill",
            &json!({ "name": "has space" }),
        )
        .await;
        assert!(r.is_error, "a model-hidden skill must be refused");
        let text = text_of(&r);
        assert!(text.contains("model-hidden"), "{text}");
        assert!(text.contains("has space"), "{text}");
        // The branch is REALLY taken: the refusal carries NO `$` at all — not
        // the pinned `send $<name>` tail, and not a `$mention` mentioned in
        // prose (a `$` in a message whose whole point is "this name has no
        // token" is a dead token the model can still copy).
        assert!(
            !text.contains('$'),
            "must not promise an untypeable $mention token: {text}"
        );
        // And no absolute claim: `is_mentionable_name` is the ASCII grammar, so
        // a name it rejects can still be expandable client-side (U+212A
        // lowercases to `k` in JS), which makes "cannot be typed" false.
        assert!(
            !text.contains("cannot be typed"),
            "the refusal must not state an impossibility: {text}"
        );
        assert!(
            text.contains("outside the mention token grammar"),
            "the refusal must name the real reason without a `$`: {text}"
        );
        assert!(
            text.contains("explicitly"),
            "the refusal must still name a route the user can take: {text}"
        );
        // ADR 0029: name ONLY what the model sent — no path, no dir, no scope.
        assert!(
            !text.contains(&root.to_string_lossy().into_owned()),
            "the failure must not disclose a path: {text}"
        );
        assert!(
            !text.contains("has-space"),
            "the failure must not disclose the directory name: {text}"
        );
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_rejects_a_model_hidden_skill_before_reading_a_bundled_file() {
        // Enforcement lands BEFORE any `FsBackend` is built, so a hidden
        // skill's bundled files are never read — not merely reported as
        // escaped.
        let cwd = temp_cwd("skill-hidden-bundle");
        let hidden = skill_root(
            "skill-hidden-bundle-hidden",
            "omega",
            "---\nname: omega\ndisable-model-invocation: true\n---\nO.\n",
        );
        std::fs::write(hidden.join("omega/ref.md"), "HIDDEN-BUNDLED-BODY\n").unwrap();
        // The visible CONTROL: a visible skill's bundled file still reads, so
        // this test cannot pass merely because the bundled path is broken.
        let visible = skill_root(
            "skill-hidden-bundle-visible",
            "alpha",
            "---\nname: alpha\n---\nA.\n",
        );
        std::fs::write(visible.join("alpha/ref.md"), "CONTROL-BODY\n").unwrap();

        let r = execute_tool(
            &ctx_with_roots(&cwd, &[hidden]),
            "read_skill",
            &json!({ "name": "omega", "path": "ref.md" }),
        )
        .await;
        assert!(r.is_error, "a model-hidden skill must be refused");
        assert_eq!(
            text_of(&r),
            "read_skill: \"omega\" is model-hidden — the Agent may not load it; ask the user to send $omega"
        );
        assert!(
            !text_of(&r).contains("HIDDEN-BUNDLED-BODY"),
            "the bundled body must never be returned: {}",
            text_of(&r)
        );
        assert!(
            !text_of(&r).contains("resolves outside the skill directory"),
            "the hidden branch must run before the containment check: {}",
            text_of(&r)
        );

        let c = execute_tool(
            &ctx_with_roots(&cwd, &[visible]),
            "read_skill",
            &json!({ "name": "alpha", "path": "ref.md" }),
        )
        .await;
        assert!(!c.is_error, "{}", text_of(&c));
        assert_eq!(text_of(&c), "CONTROL-BODY\n");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_missing_name_param_is_error() {
        let cwd = temp_cwd("skill-noparam");
        let r = execute_tool(&ctx(&cwd), "read_skill", &json!({})).await;
        assert!(r.is_error);
        assert!(text_of(&r).contains("`name`"), "{}", text_of(&r));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_bundled_binary_is_an_error_not_garbage() {
        // `FsBackend::read` is `read_to_string`, so a bundled IMAGE fails
        // (the documented ADR 0029 limitation — unlike `read`'s image path).
        let cwd = temp_cwd("skill-binary");
        let root = skill_root("skill-binary-root", "alpha", "---\nname: alpha\n---\nA.\n");
        std::fs::write(root.join("alpha/pic.png"), [0x89, 0x50, 0x4e, 0x47, 0x00]).unwrap();
        let r = execute_tool(
            &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
            "read_skill",
            &json!({ "name": "alpha", "path": "./pic.png" }),
        )
        .await;
        assert!(r.is_error, "a non-UTF-8 bundled file is an error");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn read_skill_rejected_symlink_does_not_disclose_its_target() {
        // The model names ONLY `link` (which looks in-bounds). The resolved
        // target is information it does NOT have — echoing it would turn a
        // repo-shipped skill (`data.txt` -> `~/.ssh/id_rsa`) into a
        // path-disclosure oracle. `read` may echo a path the model itself
        // supplied; `read_skill` must not invent one.
        let cwd = temp_cwd("skill-sym-oi");
        let root = skill_root("skill-sym-oi-root", "alpha", "---\nname: alpha\n---\nA.\n");
        let outside = std::env::temp_dir().join(format!("oi-target-{}", uuid::Uuid::new_v4()));
        std::fs::write(&outside, "secret").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, root.join("alpha/link")).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&outside, root.join("alpha/link")).unwrap();

        let r = execute_tool(
            &ctx_with_roots(&cwd, std::slice::from_ref(&root)),
            "read_skill",
            &json!({ "name": "alpha", "path": "link" }),
        )
        .await;
        assert!(r.is_error, "the symlink must be rejected");
        let text = text_of(&r);
        let target = outside.to_string_lossy().into_owned();
        assert!(
            !text.contains(&target),
            "the resolved target must not be disclosed: {text}"
        );
        assert!(
            text.contains("link"),
            "the call must still be diagnosable: {text}"
        );
        let _ = std::fs::remove_dir_all(&cwd);
        let _ = std::fs::remove_file(&outside);
    }

    // ── dispatch ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn unknown_tool_is_error() {
        let cwd = temp_cwd("unknown");
        let r = execute_tool(&ctx(&cwd), "nope", &json!({})).await;
        assert!(r.is_error);
        assert_eq!(text_of(&r), "unknown tool: nope");
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn web_search_dispatches_via_execute_tool() {
        let cwd = temp_cwd("web-search");
        let r = execute_tool(&ctx(&cwd), "web_search", &json!({})).await;
        // Missing query returns error result from exec_web_search, not "unknown tool"
        assert!(r.is_error);
        assert!(text_of(&r).contains("web_search requires 'query' or 'queries'"));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    #[tokio::test]
    async fn fetch_content_and_get_search_content_dispatch_via_execute_tool() {
        let cwd = temp_cwd("web-fetch");
        let r1 = execute_tool(&ctx(&cwd), "fetch_content", &json!({})).await;
        assert!(r1.is_error);
        assert!(text_of(&r1).contains("fetch_content: missing `url` parameter"));

        let r2 = execute_tool(&ctx(&cwd), "get_search_content", &json!({})).await;
        assert!(r2.is_error);
        assert!(text_of(&r2).contains("get_search_content: missing `response_id` parameter"));
        let _ = std::fs::remove_dir_all(&cwd);
    }

    // ── hostenv: the AppImage child-env scrub, at the CALL SITES ────────
    //
    // `crate::agent::hostenv` pins the RULE (what a scrubbed child env looks
    // like). This section pins the three call sites that were leaking it:
    // `exec_bash`, and the `fd` / `rg` fast paths of `exec_find` /
    // `exec_grep`. Evidence for the defect: the research doc,
    // `docs/research/appimage-child-process-env-leak.md`.
    //
    // Every test here is `#[cfg(target_os = "linux")]`, mirroring `hostenv`'s
    // own test module: off Linux `scrub` is DEFINED to return the empty patch,
    // so the same assertions would pass VACUOUSLY on macOS and Windows.
    // Compile-time gating is the honest form of "not applicable here" — a
    // runtime `if` would hide whether anything ran.

    /// How many hostenv tests SKIPPED because the scrub was unobservable on
    /// this host. Counted, not merely printed —
    /// `the_skip_is_a_failure_on_a_capable_host` asserts on the count.
    #[cfg(target_os = "linux")]
    static SKIPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    #[cfg(target_os = "linux")]
    use std::ffi::{OsStr, OsString};

    /// The fabricated mount root: named the way `build_mount_point` in
    /// AppImage's `runtime.c` names a real mount point, which is the shape
    /// `hostenv`'s structural corroboration accepts — so the unit-level tests
    /// need no tree on disk (and no temp-dir cleanup).
    #[cfg(target_os = "linux")]
    const MOUNT: &str = "/tmp/.mount_archimTEST";

    /// The `current_exe()` of a process running FROM [`MOUNT`].
    #[cfg(target_os = "linux")]
    const EXE_INSIDE_MOUNT: &str = "/tmp/.mount_archimTEST/usr/bin/archimedes";

    /// Set on the re-spawned test binary to tell it that it IS running from
    /// inside the fabricated mount (see
    /// `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`).
    #[cfg(target_os = "linux")]
    const FABRICATED: &str = "ARCHIMIDES_HOSTENV_FABRICATED_MOUNT";
    /// The dir holding the `fd` / `rg` shims, passed to that child (whose own
    /// `PATH` is the scrubbed one, so it cannot be located from inside).
    #[cfg(target_os = "linux")]
    const SHIM_DIR: &str = "ARCHIMIDES_HOSTENV_SHIM_DIR";
    /// Which fast-path binaries the parent found on its `PATH` and shimmed
    /// (comma list), so the child knows which assertions it may make.
    #[cfg(target_os = "linux")]
    const FAST_PATHS: &str = "ARCHIMIDES_HOSTENV_FAST_PATHS";

    /// The separator for the `printf` probes (`:` would be ambiguous inside
    /// the values themselves, and no path in these tests contains this).
    #[cfg(target_os = "linux")]
    const SEP: &str = "@HOSTENV@";

    /// Printed by the child once every fabricated-mount assertion passed, and
    /// by the reporter when it saw a host the scrub was live on. The parent
    /// requires BOTH: a child whose filter matched nothing prints
    /// `running 0 tests` and exits 0 — a pass that proves nothing.
    #[cfg(target_os = "linux")]
    const CHILD_RAN: &str = "HOSTENV-CHILD-RAN";
    #[cfg(target_os = "linux")]
    const COVERAGE_FULL: &str = "HOSTENV-COVERAGE: FULL";

    /// Whether `component` IS, or lives under, a directory named like an
    /// AppImage mount point (`build_mount_point`, `runtime.c`).
    #[cfg(target_os = "linux")]
    fn under_a_mount(component: &OsStr) -> bool {
        use std::os::unix::ffi::OsStrExt;
        Path::new(component).ancestors().any(|a| {
            a.file_name()
                .is_some_and(|n| n.as_bytes().starts_with(b".mount_"))
        })
    }

    /// Whether a colon list names a mount path anywhere in it.
    #[cfg(target_os = "linux")]
    fn lists_a_mount_path(value: &OsStr) -> bool {
        std::env::split_paths(value).any(|c| under_a_mount(c.as_os_str()))
    }

    /// What a child SHOULD still see of a colon list: every non-empty
    /// component that is not inside a mount. (The bundle's trailing `:`
    /// carries no entry, and `ld.so` reads an empty component as the CWD, so
    /// dropping it is part of the rule.)
    ///
    /// This ORACLE is written from the rule in the research doc and does NOT
    /// call `hostenv`: an expectation computed by the code under test cannot
    /// catch the rule itself being wrong.
    #[cfg(target_os = "linux")]
    fn kept_entries(value: &OsStr) -> Vec<OsString> {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        value
            .as_bytes()
            .split(|byte| *byte == b':')
            .filter(|c| !c.is_empty() && !under_a_mount(OsStr::from_bytes(c)))
            .map(|c| OsString::from_vec(c.to_vec()))
            .collect()
    }

    /// The value THIS process' child must print for `name`: the inherited
    /// value minus the mount entries, rejoined in order. Empty when the patch
    /// was right to `env_remove` the variable outright (the real `AppRun` sets
    /// `LD_LIBRARY_PATH=$APPDIR/usr/lib/:` and nothing else, so the correct
    /// result for THAT variable is often absence, not a shorter value).
    #[cfg(target_os = "linux")]
    fn expected_child_value(name: &str) -> String {
        let inherited = std::env::var_os(name).unwrap_or_default();
        kept_entries(&inherited)
            .iter()
            .map(|entry| entry.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join(":")
    }

    /// The production gate, recomputed for THIS process: `APPDIR` set, this
    /// exe under it, mount shape corroborated. `for_child()` uses exactly
    /// this, so `false` means the scrub is INERT in this process — which is
    /// the case for every plain `cargo test` (`current_exe()` is
    /// `target/debug/deps/<hash>`), and why a test must NOT assert a scrub the
    /// production gate would never perform (a dev box that merely exports a
    /// stale `APPDIR` would otherwise see a false failure).
    #[cfg(target_os = "linux")]
    fn running_from_an_appimage_mount() -> bool {
        let Some(appdir) = std::env::var_os("APPDIR") else {
            return false;
        };
        if appdir.is_empty() {
            return false;
        }
        let appdir = Path::new(&appdir);
        let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("/"));
        exe.starts_with(appdir)
            && (appdir.join("usr").is_dir()
                || appdir
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".mount_")))
    }

    /// Whether the INHERITED env carries mount paths in the variables a child
    /// inherits, REGARDLESS of whether this process runs from a mount. A shell
    /// opened by `--appimage-run-in-shell`, or one carrying a stale `APPDIR`,
    /// looks tainted while `for_child()` is empty BY DESIGN; only the reporter
    /// interprets that difference (see its doc).
    #[cfg(target_os = "linux")]
    fn inherited_env_has_mount_paths() -> bool {
        [
            "PATH",
            "LD_LIBRARY_PATH",
            "GI_TYPELIB_PATH",
            "XDG_DATA_DIRS",
            "PYTHONHOME",
        ]
        .into_iter()
        .filter_map(std::env::var_os)
        .any(|value| lists_a_mount_path(&value))
    }

    /// Record a skip: the greppable marker AND the count the reporter reads.
    #[cfg(target_os = "linux")]
    fn record_skip(reason: &str) {
        let n = SKIPPED.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        eprintln!("SKIP-HOSTENV: {reason} — skip #{n}");
    }

    /// 1 — the unit-level proof, routed through the SAME shell the tool uses.
    ///
    /// Claim: a `sh` child started the way `exec_bash` starts one (a
    /// `tokio::process::Command` on `sh` with
    /// [`EnvPatch::apply`](crate::agent::hostenv::EnvPatch::apply) on it) sees
    /// no mount component in `LD_LIBRARY_PATH` and keeps its own `PATH`
    /// entries.
    ///
    /// What this proves, honestly: `scrub` + `apply` + what the shell actually
    /// reads after `execve`. It does NOT prove that `exec_bash` CALLS them —
    /// applying an EMPTY patch at the call site passes this test vacuously.
    /// The call-site tests are
    /// `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`
    /// (which runs the tool from inside a mount) and the assertion it shares
    /// with `a_bash_child_on_an_appimage_host_sees_no_mount_paths`.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_scrubbed_sh_child_sees_no_mount_path_but_keeps_the_user_path() {
        // The two `AppRun.c` shapes: mount dir FIRST with a TRAILING SLASH for
        // `LD_LIBRARY_PATH`, mount dir FIRST for `PATH`, one real entry each.
        let inherited = vec![
            (
                OsString::from("LD_LIBRARY_PATH"),
                OsString::from(format!("{MOUNT}/usr/lib/:/usr/lib")),
            ),
            (
                OsString::from("PATH"),
                OsString::from(format!("{MOUNT}/bin:/opt/test-unique/bin")),
            ),
        ];
        let patch = crate::agent::hostenv::scrub(
            inherited,
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE_MOUNT),
        );
        assert!(
            !patch.is_empty(),
            "the fabricated host must produce a patch or this test asserts nothing: {patch:?}"
        );

        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.arg("-c")
            .arg(format!("printf '%s{SEP}%s' \"$LD_LIBRARY_PATH\" \"$PATH\""));
        // `env_clear` so the ONLY thing in the child's env is the patch —
        // otherwise a host whose own env happens to satisfy the expectation
        // could pass this test with a patch that did nothing.
        cmd.env_clear();
        patch.apply(&mut cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        let out = cmd.output().await.expect("the scrubbed sh starts");
        assert!(out.status.success(), "the scrubbed sh failed: {out:?}");
        assert_eq!(
            String::from_utf8_lossy(&out.stdout),
            format!("/usr/lib{SEP}/opt/test-unique/bin"),
            "the mount entry must be gone and the user's entry must survive, in order"
        );
    }

    /// 2 — the integration test through the tool: on a host where the scrub
    /// is LIVE, the `bash` child must print no mount path at all.
    ///
    /// Its precondition is the PRODUCTION gate, not `APPDIR.is_set()`: the
    /// scrub comes from `for_child()`, which also requires this exe to be
    /// under `$APPDIR`. An `APPDIR`-only predicate would assert a scrub that
    /// production cannot perform (a dev box that exports a stale `APPDIR` from
    /// a previous `--appimage-run-in-shell` would fail spuriously while the
    /// code is correct), so it skips — loudly — whenever this process is not
    /// itself mounted.
    ///
    /// That would make it skip on every CI runner, so
    /// `a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env`
    /// re-executes THIS test from inside a fabricated mount: the assertion
    /// then runs on every Linux job, not only on a machine that happens to be
    /// running the bundle.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_bash_child_on_an_appimage_host_sees_no_mount_paths() {
        if !running_from_an_appimage_mount() {
            record_skip(
                "this test binary is not running from inside an AppImage mount, so \
                 for_child() is empty BY DESIGN and the scrub is unobservable here \
                 (the fabricated-mount test runs this test from inside one)",
            );
            return;
        }
        assert_bash_child_env_is_scrubbed().await;
    }

    /// 3 — the CONTRAST, and a test that can fail: a call site that
    /// `env_clear()`s, or a rule that removes `PATH` instead of filtering it,
    /// passes every "no `.mount_`" assertion while silently breaking every
    /// child — std hands a PATH-less child `confstr(_CS_PATH)`
    /// (`/usr/local/bin:/usr/bin`) and it still exits 0, so `git --version`
    /// would NOT catch that. The user's own dir must still be in the child's
    /// `PATH`, and it must be ALL that is left.
    ///
    /// This is the rule-level half (it drives `scrub` + `apply` directly, so it
    /// pins the filter, not a call site); the call-site half of the same
    /// contrast — that the `bash` TOOL's child keeps the user's entry, and that
    /// the `fd`/`rg` children do too — is asserted inside the fabricated mount
    /// (`fabricated_child_half`, `assert_fast_path_children_are_scrubbed`).
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_bash_tool_keeps_the_users_path_entries() {
        let user_bin = temp_cwd("hostenv-path");
        std::fs::write(user_bin.join("marker-of-the-users-own-bin.txt"), "keep me").unwrap();
        let inherited = vec![(
            OsString::from("PATH"),
            OsString::from(format!("{MOUNT}/bin/:{}", user_bin.display())),
        )];
        let patch = crate::agent::hostenv::scrub(
            inherited,
            Some(Path::new(MOUNT)),
            Path::new(EXE_INSIDE_MOUNT),
        );
        assert!(
            !patch.is_empty(),
            "the fabricated host must produce a patch or this test asserts nothing: {patch:?}"
        );

        let mut cmd = tokio::process::Command::new("/bin/sh");
        cmd.arg("-c").arg("printf %s \"$PATH\"");
        cmd.env_clear();
        patch.apply(&mut cmd);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        let out = cmd.output().await.expect("the sh starts");
        let _ = std::fs::remove_dir_all(&user_bin);
        assert!(out.status.success(), "the sh failed: {out:?}");
        let printed = String::from_utf8_lossy(&out.stdout).into_owned();
        assert!(
            printed.contains(&user_bin.display().to_string()),
            "the child's PATH lost the user's own entry ({}) — it printed {printed:?}",
            user_bin.display()
        );
        assert_eq!(
            printed,
            user_bin.display().to_string(),
            "only the mount entry may be dropped: the child's whole PATH must be \
             the user's entries, in order"
        );
    }

    /// 4 — the skip reporter, and an ASSERTION rather than a print.
    ///
    /// Why it must assert: a passing test's `println!`/`eprintln!` output is
    /// CAPTURED by the harness, so `cargo test | grep -c SKIP-HOSTENV` reads 0
    /// even when every hostenv test skipped, and CI runs a bare `cargo test` —
    /// a green suite in which the only call-site test skipped would look like
    /// a full pass.
    ///
    /// The two views are deliberately DIFFERENT, and the difference is the
    /// assertion's teeth:
    ///
    /// * **capable** = [`running_from_an_appimage_mount`] — the production
    ///   gate is open HERE, the scrub is live, so any skip is a coverage hole
    ///   ⇒ PANIC.
    /// * **merely tainted** = [`inherited_env_has_mount_paths`] — the inherited
    ///   env names mount paths but this exe is not under `$APPDIR`, so the
    ///   scrub is inert BY DESIGN: the honest verdict is the printed line, not
    ///   a false failure.
    ///
    /// Cargo does not order tests, so reading a counter is inherently racy
    /// against the test that writes it. It is not the only mechanism: the
    /// fabricated-mount test runs the child with `--test-threads=1` (the
    /// harness runs tests in name order and every other hostenv test sorts
    /// before `the_skip_…`), so this assertion IS enforced on every Linux run
    /// — the child that would skip runs first.
    ///
    /// MUST: on an AppImage host `cargo test -- -- --nocapture` prints ZERO
    /// `SKIP-HOSTENV` markers; on a non-AppImage host it MUST print the
    /// `host is not an AppImage` verdict line.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_skip_is_a_failure_on_a_capable_host() {
        let skips = SKIPPED.load(std::sync::atomic::Ordering::SeqCst);
        if running_from_an_appimage_mount() {
            assert_eq!(
                skips, 0,
                "SKIP-HOSTENV: {skips} hostenv test(s) SKIPPED on a host that runs this \
                 exe from inside an AppImage mount — the scrub is LIVE here, so this run \
                 may not claim coverage of it"
            );
            eprintln!("{COVERAGE_FULL} — the bash child's env was observed from inside a mount");
        } else if inherited_env_has_mount_paths() {
            eprintln!(
                "SKIP-HOSTENV: host is not an AppImage, the scrub is unobservable here \
                 (the inherited env still names mount paths, but this exe is not under \
                 $APPDIR, so for_child() is empty by design — a stale AppRun export)"
            );
        } else {
            eprintln!("SKIP-HOSTENV: host is not an AppImage, the scrub is unobservable here");
        }
    }

    /// 5 — the deterministic call-site test, on ANY Linux host.
    ///
    /// `for_child()` cannot be made non-empty inside a normal test process
    /// (its gate needs `current_exe()` under `$APPDIR`), so THIS test builds
    /// the missing half: an AppDir-shaped tree containing a HARD LINK to this
    /// very test binary, and a re-execution of the binary from that link with
    /// the `AppRun` env stack set. The gate then opens for real, the tool runs
    /// for real, and the child's env is what is asserted — no fixture of the
    /// patch, no host that happens to be tainted.
    ///
    /// Hard-linked, not copied: `target/debug` is by construction the same
    /// filesystem as `target/debug/deps` (where the test binary lives), and
    /// the lib test binary is far too big to copy per test.
    ///
    /// The child runs three tests (`--test-threads=1`): this one's child half
    /// (`fabricated_child_half`), `a_bash_child_on_an_appimage_host_sees_no_mount_paths`
    /// — which no longer skips here — and the reporter, whose capable branch
    /// therefore runs on every Linux CI job. The parent asserts on the child's
    /// exit status AND on both markers, because a filter that matched nothing
    /// exits 0.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env() {
        if std::env::var_os(FABRICATED).is_some() {
            fabricated_child_half().await;
            return;
        }

        let appdir = fabricated_mount();
        // The shims live OUTSIDE the mount: a shim inside `$APPDIR` would be
        // scrubbed away with the rest of it and the fast paths would not find
        // it (which is correct behaviour, but proves nothing).
        let shim = temp_cwd("hostenv-shim");
        let mut installed: Vec<String> = Vec::new();
        for binary in ["fd", "rg"] {
            if let Some(real) = real_binary_on_path(binary) {
                write_shim(&shim, binary, &real);
                installed.push(binary.to_string());
            }
        }

        let mut cmd = tokio::process::Command::new(appdir.join("usr/bin/archimedes"));
        cmd.args([
            "agent::tools::exec::tests::a_bash_child_on_an_appimage_host_sees_no_mount_paths",
            "agent::tools::exec::tests::a_bash_child_under_a_fabricated_appimage_mount_sees_a_scrubbed_env",
            "agent::tools::exec::tests::the_skip_is_a_failure_on_a_capable_host",
            "--test-threads=1",
            "--nocapture",
        ]);
        // The taint `AppRun.c` + `linuxdeploy-plugin-gtk.sh` leave behind, in
        // the byte shapes they actually write (mount dir first, trailing slash,
        // the double slash of the GTK hook).
        cmd.env("APPDIR", &appdir);
        cmd.env("APPIMAGE", appdir.join("archimedes.AppImage"));
        cmd.env(
            "LD_LIBRARY_PATH",
            format!("{}/usr/lib/:/usr/lib", appdir.display()),
        );
        cmd.env(
            "GI_TYPELIB_PATH",
            format!("{}//usr/lib/girepository-1.0", appdir.display()),
        );
        cmd.env("PYTHONHOME", appdir.join("usr"));
        // Mount dir first (with its trailing slash), then the shim dir and
        // `/bin` — the two entries the child must still see.
        cmd.env(
            "PATH",
            format!("{}/usr/bin/:{}:/bin", appdir.display(), shim.display()),
        );
        cmd.env(FABRICATED, "1");
        cmd.env(SHIM_DIR, &shim);
        cmd.env(FAST_PATHS, installed.join(","));
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        // So the deadline below cannot leave an orphan: `wait_with_output`
        // CONSUMES the child, so the only way out of a timeout is dropping the
        // future, and dropping the child has to kill it.
        cmd.kill_on_drop(true);

        let child = cmd.spawn().expect("the fabricated-mount child starts");
        let out =
            match tokio::time::timeout(Duration::from_secs(180), child.wait_with_output()).await {
                Ok(out) => out.expect("the fabricated-mount child is reaped"),
                Err(_) => panic!("the fabricated-mount child did not finish within 180 s"),
            };
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let _ = std::fs::remove_dir_all(&appdir);
        let _ = std::fs::remove_dir_all(&shim);
        assert!(
            out.status.success(),
            "the run inside the fabricated mount failed (its own assertions are the failure):\n{log}"
        );
        assert!(
            log.contains(CHILD_RAN),
            "the child never ran the fabricated assertions — a filter that matches nothing \
             exits 0, so the marker is the only proof it ran:\n{log}"
        );
        assert!(
            log.contains(COVERAGE_FULL),
            "the reporter did not see a capable host inside the child — the fabricated gate \
             did not open, so nothing here was observed:\n{log}"
        );
    }

    /// The AppDir-shaped tree: `$APPDIR/usr/lib`, `$APPDIR/usr/bin` and a hard
    /// link of this test binary at `$APPDIR/usr/bin/archimedes` (the path a
    /// mounted AppImage's `current_exe()` actually reports).
    #[cfg(target_os = "linux")]
    fn fabricated_mount() -> PathBuf {
        let exe = std::env::current_exe().expect("a test binary has a path");
        let under_target = exe
            .parent() // target/<profile>/deps
            .and_then(|deps| deps.parent()) // target/<profile>
            .expect("the test binary lives in target/<profile>/deps");
        let appdir = under_target.join(format!(".mount_archimTEST-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(appdir.join("usr/lib")).unwrap();
        std::fs::create_dir_all(appdir.join("usr/bin")).unwrap();
        std::fs::hard_link(&exe, appdir.join("usr/bin/archimedes")).expect(
            "the fabricated AppDir must hard-link the test binary (same filesystem: \
             target/<profile> holds target/<profile>/deps)",
        );
        appdir
    }

    /// The real `fd` / `rg` on THIS process' `PATH` — the parent's `PATH` is
    /// the clean one, and the shim must exec an absolute path because the
    /// child's `PATH` no longer contains the real binary's dir by name.
    #[cfg(target_os = "linux")]
    fn real_binary_on_path(name: &str) -> Option<PathBuf> {
        let path = std::env::var_os("PATH")?;
        std::env::split_paths(&path)
            .map(|dir| dir.join(name))
            .find(|candidate| candidate.is_file())
    }

    /// An `sh` shim that records the env it was started with and then execs
    /// the real binary. `exec_find` / `exec_grep` spawn `fd` / `rg` BY NAME
    /// and read their stdout, so the record file is the only way to see what
    /// those two children were handed.
    #[cfg(target_os = "linux")]
    fn write_shim(dir: &Path, binary: &str, real: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let record = dir.join(format!("{binary}-env.txt"));
        let script = format!(
            "#!/bin/sh\nprintf '%s{SEP}%s' \"$LD_LIBRARY_PATH\" \"$PATH\" > '{}' 2>/dev/null || true\nexec '{}' \"$@\"\n",
            record.display(),
            real.display()
        );
        let path = dir.join(binary);
        std::fs::write(&path, script).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// The assertions that run INSIDE the fabricated mount. [`CHILD_RAN`] is
    /// printed only after every one of them passes.
    #[cfg(target_os = "linux")]
    async fn fabricated_child_half() {
        assert!(
            running_from_an_appimage_mount(),
            "the fabricated mount did not open the production gate — the child would assert nothing"
        );
        let (_ld, path) = assert_bash_child_env_is_scrubbed().await;

        // The user's own entries survive the TOOL hop too — the same contrast
        // as `the_bash_tool_keeps_the_users_path_entries`, but asserted at the
        // call site that is the one capable of getting it wrong (a site that
        // `env_clear()`s, or a rule that removes `PATH`, leaves the child with
        // std's `confstr(_CS_PATH)` fallback and still exits 0).
        let shim = std::env::var_os(SHIM_DIR).expect("the parent passes the shim dir");
        assert_eq!(
            path,
            format!("{}:/bin", shim.to_string_lossy()),
            "the bash child's PATH must be exactly the inherited PATH minus the \
             mount entry, in order (the shim dir is the user's entry, /bin the \
             system's)"
        );

        let installed: Vec<String> = std::env::var(FAST_PATHS)
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        assert_fast_path_children_are_scrubbed(Path::new(&shim), &installed).await;
        println!("{CHILD_RAN}: PATH={path}");
    }

    /// The shared assertion of tests 2 and 5: run `bash` through the tool and
    /// check the child's own `LD_LIBRARY_PATH` and `PATH` against
    /// [`expected_child_value`] — the inherited env minus the mount entries.
    ///
    /// Comparing the FULL value (not just "no `.mount_` substring") is what
    /// makes a dropped or reordered entry visible, and it is also what makes
    /// the correct absence of `LD_LIBRARY_PATH` pass: the real `AppRun` sets it
    /// to `$APPDIR/usr/lib/:` and nothing else, so scrubbing it leaves NOTHING,
    /// and a "must still be non-empty" assertion here would be a false failure.
    #[cfg(target_os = "linux")]
    async fn assert_bash_child_env_is_scrubbed() -> (String, String) {
        let cwd = temp_cwd("hostenv-bash");
        let r = execute_tool(
            &ctx(&cwd),
            "bash",
            &json!({ "command": format!("printf '%s{SEP}%s' \"$LD_LIBRARY_PATH\" \"$PATH\"") }),
        )
        .await;
        let _ = std::fs::remove_dir_all(&cwd);
        assert!(!r.is_error, "the bash tool failed: {}", text_of(&r));
        let text = text_of(&r).to_string();
        let (ld, path) = text.split_once(SEP).unwrap_or_else(|| {
            panic!("the child printed {text:?}, not the `{SEP}`-separated pair it was asked for")
        });
        for (name, value) in [("LD_LIBRARY_PATH", ld), ("PATH", path)] {
            assert!(
                !lists_a_mount_path(OsStr::new(value)),
                "the bash child inherited a mount path in {name}: {value}"
            );
        }
        let expected = (
            expected_child_value("LD_LIBRARY_PATH"),
            expected_child_value("PATH"),
        );
        assert_eq!(
            (ld, path),
            (expected.0.as_str(), expected.1.as_str()),
            "the bash child's env must be exactly the inherited env minus the mount entries"
        );
        (expected.0, expected.1)
    }

    /// The `fd` / `rg` fast paths, run through the tools, with the env their
    /// shims recorded read back.
    #[cfg(target_os = "linux")]
    async fn assert_fast_path_children_are_scrubbed(shim: &Path, installed: &[String]) {
        for (tool, binary) in [("find", "fd"), ("grep", "rg")] {
            if !installed.iter().any(|b| b == binary) {
                // A NOTE, not a skip: with the binary absent the fast path
                // never runs (`exec_find` falls back to `walkdir`), so there
                // is no fast-path child env to have scrubbed and nothing here
                // went unobserved. Not a `SKIP-HOSTENV` marker for exactly
                // that reason.
                println!("NOTE-HOSTENV: {binary} is not installed — the `{tool}` fast path does not run here");
                continue;
            }
            let cwd = temp_cwd(&format!("hostenv-{tool}"));
            std::fs::write(cwd.join("needle.txt"), "needle\n").unwrap();
            let params = if tool == "find" {
                json!({ "pattern": "needle.txt" })
            } else {
                json!({ "pattern": "needle" })
            };
            let r = execute_tool(&ctx(&cwd), tool, &params).await;
            let text = text_of(&r).to_string();
            let _ = std::fs::remove_dir_all(&cwd);
            assert!(
                !r.is_error,
                "the `{tool}` fast path must still work on a scrubbed env: {text}"
            );
            assert!(
                text.contains("needle"),
                "the `{tool}` fast path found nothing on a scrubbed env: {text}"
            );
            let record = shim.join(format!("{binary}-env.txt"));
            let seen = std::fs::read_to_string(&record).unwrap_or_else(|e| {
                panic!(
                    "the {binary} shim never recorded its env ({e}): the `{tool}` fast path \
                     did not run the shim, so there was no child env to check"
                )
            });
            let (ld, path) = seen.split_once(SEP).unwrap_or_else(|| {
                panic!("unexpected {binary} shim record {seen:?} (no `{SEP}` separator)")
            });
            for (name, value) in [("LD_LIBRARY_PATH", ld), ("PATH", path)] {
                assert!(
                    !lists_a_mount_path(OsStr::new(value)),
                    "the `{tool}` child inherited a mount path in {name}: {value}"
                );
            }
        }
    }
}

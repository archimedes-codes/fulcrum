use std::path::{Path, PathBuf};

use tokio_util::sync::CancellationToken;

use super::AgentLoop;
use crate::agent::fs_backend::{FsBackend, FsError};
use crate::agent::harness::provider::ToolCall;
use crate::agent::permission::{native_permission_gate, permission_title, PermissionOutcome};
use crate::agent::policy::{decision_for, AccessPolicy, Decision};
use crate::agent::tools::exec::protected_refusal_text;
use crate::agent::tools::{ContentBlock, ToolResult};

/// `get_search_content` (network calls and cache reads are not filesystem
/// operations and are not policy-gated, ADR 0056), `subagent` / `mcp` /
/// `manage_todo_list` / the interactive flows (no argument of theirs reaches
/// the filesystem in a way this policy describes).
pub(super) const POLICIED_TOOLS: &[(&str, PolicyDirection)] = &[
    ("bash", PolicyDirection::Shell),
    ("read", PolicyDirection::Read),
    ("find", PolicyDirection::Read),
    ("grep", PolicyDirection::Read),
    ("ls", PolicyDirection::Read),
    ("write", PolicyDirection::Write),
    ("edit", PolicyDirection::Write),
];

/// The direction a policed tool is judged in (ADR 0030: the policy is
/// per-DIRECTION, so `read` obeys `reads` and `edit` obeys `writes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PolicyDirection {
    /// Judged against the read boundary; policy `reads`.
    Read,
    /// Judged against the session `cwd`; policy `writes`.
    Write,
    /// No path: always `beyond`; policy `shell`.
    Shell,
}

/// The tool the file-access policy judges, and in which direction
/// (`None` = ungated — the skill tools, `subagent`, `mcp`, the interactive
/// flows, everything else).
pub(super) fn policed_direction(tool: &str) -> Option<PolicyDirection> {
    POLICIED_TOOLS
        .iter()
        .find(|(name, _)| *name == tool)
        .map(|(_, dir)| *dir)
}

impl AgentLoop {
    /// The policy verdict for a tool call: `Some(Decision)` for a policed
    /// tool (the ADR 0030 matrix — `Allow`, `Ask`, `Deny`), `None` for
    /// ungated tools. Evaluated against the direction's roots (the read
    /// boundary for `read`/`find`/`grep`/`ls`, the session `space_cwd` for
    /// `write`/`edit`, always `beyond` for `bash`).
    ///
    /// "Beyond the boundary" is answered by the SAME canonicalize-then-check
    /// the ADR 0029 attack matrix proved (`FsBackend::validate` over the
    /// direction's boundary roots) — deliberately NOT a second canonicalizer
    /// here: a gate and an executor that resolve `..` / symlinks differently
    /// is the classic sandbox escape, so they share one resolver and can not
    /// drift. `Ok` = in bounds; ANY `Err` (escape, or a root set that cannot
    /// be judged) = `beyond`, so an unjudgeable path is never silently
    /// allowed.
    ///
    /// A tool call with NO usable `path` is in bounds (the executor's own
    /// "missing parameter" error still fires, ungated — a malformed call is
    /// not a policy event), and a `find` / `grep` / `ls` whose `path` is
    /// omitted means the `cwd` (their advertised default), so it too is in
    /// bounds.
    pub(super) fn policy_verdict(&self, tc: &ToolCall) -> Option<Decision> {
        let dir = policed_direction(&tc.name)?;
        let (policy, beyond) = match dir {
            PolicyDirection::Shell => {
                // No path to judge: a command can reach ANYWHERE, so it is
                // always beyond the boundary (see `decision_for`).
                (self.file_policy.shell, true)
            }
            PolicyDirection::Read | PolicyDirection::Write => {
                let Some(raw) = tc.arguments.get("path").and_then(|v| v.as_str()) else {
                    // `read` / `write` / `edit` REQUIRE a path (the executor
                    // rejects the call without one) and the directory tools
                    // default it to the `cwd` — either way there is nothing
                    // to gate.
                    return Some(Decision::Allow);
                };
                let policy = self.direction_policy(dir);
                let beyond = self.beyond_error(dir, Path::new(raw)).is_some();
                (policy, beyond)
            }
        };
        let trusted = self
            .trust
            .as_ref()
            .map(|t| t.is_trusted(&self.space_cwd))
            .unwrap_or(false);
        Some(decision_for(
            policy,
            beyond,
            trusted,
            dir == PolicyDirection::Shell,
        ))
    }

    /// The policy that governs a direction (ADR 0030: the settings are
    /// per-direction, which is why `read` and `edit` can disagree).
    pub(super) fn direction_policy(&self, dir: PolicyDirection) -> AccessPolicy {
        match dir {
            PolicyDirection::Read => self.file_policy.reads,
            PolicyDirection::Write => self.file_policy.writes,
            PolicyDirection::Shell => self.file_policy.shell,
        }
    }

    /// The root set a direction is judged against: the READ boundary (the
    /// `cwd` plus the discovery roots) for reads, the session `cwd` ALONE
    /// for writes (`boundary::write_roots` — a repo-level `.agents/skills`
    /// is repo content and stays writable from a package cwd). NOT the
    /// policy-derived executor roots, which widen to `/` under `Ask`/`Allow`
    /// — the gate must see the real boundary even when the executor is
    /// unrestricted (that widening is the gate's DECISION, not its input).
    pub(super) fn direction_boundary(&self, dir: PolicyDirection) -> Vec<PathBuf> {
        match dir {
            PolicyDirection::Read | PolicyDirection::Shell => self.read_boundary.clone(),
            PolicyDirection::Write => vec![self.space_cwd.clone()],
        }
    }

    /// The error the direction's boundary check produced for a path, or
    /// `None` when the path is IN bounds. This is the single answer the gate
    /// needs: `Some` is exactly "beyond" (which a `Deny` implies), and its
    /// payload is what the rejection quotes back.
    ///
    /// Answered by the SAME canonicalize-then-check the ADR 0029 attack
    /// matrix proved (`FsBackend::validate`), so gate and executor cannot
    /// resolve `..` / a symlink differently — a gate that resolves differently
    /// than the thing it gates is the classic escape, hence ONE resolver
    /// shared by both layers rather than a second canonicalizer here. ANY
    /// error — an escape, or a root set that cannot be judged at all — means
    /// beyond, so an unjudgeable path is never silently allowed.
    pub(super) fn beyond_error(&self, dir: PolicyDirection, path: &Path) -> Option<FsError> {
        let roots = self.direction_boundary(dir);
        FsBackend { roots }.validate(path).err()
    }

    /// The tool-result error for a policy DENY (never a prompt). Worded like
    /// the executor's own rejection — `FsError`'s display, prefixed with the
    /// tool name exactly as the executors prefix it — so a read the GATE
    /// denied looks identical to the model to one the executor would have
    /// rejected: the model's recovery behavior must not depend on which
    /// layer said no. A write into a user-level agent-definition dir gets the
    /// deny-list wording instead (the true reason — refused there in EVERY
    /// policy, so "escapes the sandbox" would send the model off looking for
    /// a boundary that is not the problem).
    pub(super) fn deny_tool_result(&self, tc: &ToolCall) -> ToolResult {
        let dir = policed_direction(&tc.name);
        let text = match tc.arguments.get("path").and_then(|v| v.as_str()) {
            Some(raw) => {
                let path = Path::new(raw);
                // The deny-list is a WRITE-floor question (`protected_dirs`
                // are dirs a WRITE may not enter), and it is checked BEFORE
                // the boundary: the refusal is true in every policy, so its
                // wording must win over "escapes the sandbox", which would
                // send the model hunting for a boundary that is not the
                // problem.
                let protected = (dir == Some(PolicyDirection::Write))
                    .then(|| protected_refusal_text(&self.protected, &tc.name, path))
                    .flatten();
                match protected {
                    Some(msg) => msg,
                    None => self.escape_text(dir, &tc.name, path, raw),
                }
            }
            // `bash` has no path: the policy simply refused the command.
            None => format!("{}: refused by the file-access policy (Sandboxed)", tc.name),
        };
        ToolResult {
            content: vec![ContentBlock::Text { text }],
            details: None,
            is_error: true,
        }
    }

    /// The escape rejection's text, in the executor's own words: the tool
    /// name, then `FsError::PathEscape` — exactly what a `read` of the same
    /// path would have returned. The variant's `path` is the RAW argument
    /// the model passed, which is also what the executor's own rejection
    /// carries when the path canonicalizes to nothing useful; a
    /// `FsError::Io` (no boundary at all) is reported verbatim.
    pub(super) fn escape_text(
        &self,
        dir: Option<PolicyDirection>,
        tool: &str,
        path: &Path,
        raw: &str,
    ) -> String {
        let dir = dir.unwrap_or(PolicyDirection::Read);
        let body = FsError::PathEscape {
            path: match self.beyond_error(dir, path) {
                // The RESOLVED form when the canonicalizer produced one — it
                // tells the user where the path actually landed (`../x` under
                // a symlinked `cwd` is not where it looks).
                Some(FsError::PathEscape { path }) => path,
                _ => raw.to_string(),
            },
        };
        format!("{tool}: {body}")
    }

    /// The permission gate (the handle-free waiter — reviewer-corrected
    /// Major #14): a tool the policy sent to `Ask` on an untrusted Space
    /// prompts (`permission-request` via the sink, a `PendingPermissions`
    /// oneshot; a deny / cancel is a denial); a TRUSTED Space is
    /// auto-approved (ADR 0010 — replicated here: the native path has no
    /// `handle_extension_ui_request` to do it). A CANCELLED turn is a
    /// `Cancelled` (a deny) — the gate consults the token BEFORE the
    /// trusted short-circuit (finding 8a).
    ///
    /// The trust check inside the gate is now REDUNDANT with
    /// `decision_for` (which returns `Allow` for a trusted Space, so an
    /// `Ask` reaching here already implies untrusted); it stays because the
    /// gate is also the seam that answers `trust-space`, and a
    /// fail-closed gate is cheaper than a clever one.
    pub(super) async fn gate_tool(&self, tc: &ToolCall, turn: &CancellationToken) -> bool {
        let outcome = native_permission_gate(
            &self.session_id,
            &tc.id,
            &permission_title(&tc.name, &tc.arguments),
            &self.sink,
            &self.pending_permissions,
            self.trust.as_deref(),
            &self.space_cwd,
            turn,
        )
        .await;
        matches!(
            outcome,
            PermissionOutcome::Selected { option_id }
                if option_id == "allow" || option_id == "trust-space"
        )
    }
}

use std::path::{Path, PathBuf};

use tokio_util::sync::CancellationToken;

use super::AgentLoop;
use crate::agent::definition_permission::{Action, ToolKey};
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

/// (issue #101) The per-agent verdict projected onto the gate's order — the
/// definition's `Action` vocabulary (`Allow | Ask | Deny`, no `Confine`) plus the
/// identity for "this file says nothing about this tool".
///
/// A separate type rather than a `Decision` on purpose: `Decision::Allow` MEANS
/// "run it", which is not what a silent file means, and collapsing the two would
/// let a caller mistake "no opinion" for a grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DefinitionVerdict {
    /// Strictest.
    Deny,
    Ask,
    /// The identity of the merge (`Allow < everything`).
    Allow,
    /// The file does not speak about this tool (or cannot decide it) — the
    /// merge's identity, NOT an `Allow`.
    AsSelf,
}

/// (issue #101) STRICTEST-WINS over the total order **`Allow < Confine < Ask <
/// Deny`** (grounding delta §5). `AsSelf` and `Allow` are both identities, so a
/// definition can only ever NARROW the user's `filePolicy`, never widen it —
/// ADR 0030's "trust suppresses prompts but never widens a deny" applied to
/// definitions.
///
/// **`Confine` is NOT a midpoint of a `min` over the three per-agent actions.**
/// It is a RUNNING sandbox (the Landlock `bash` the user asked for by choosing
/// `shell: sandboxed`), so it ranks ABOVE `Allow` — a per-agent `Allow` cannot
/// un-confine it — and BELOW `Ask`, so a per-agent `Ask` still tightens a
/// confined run into a prompt. A merge written as a rank over `{Allow, Ask,
/// Deny}` alone mis-orders `bash`: it would either let `allow` release the
/// sandbox (a widening, the exact thing this layer forbids) or refuse to tighten
/// a confined shell. The two arms below are that decision, spelled out.
fn stricter_of_two(posture: Decision, definition: DefinitionVerdict) -> Decision {
    match definition {
        // The identities: no opinion, or an `Allow` that can beat nothing.
        DefinitionVerdict::AsSelf | DefinitionVerdict::Allow => posture,
        DefinitionVerdict::Ask => match posture {
            // `Ask` outranks both the silent arms — including `Confine`, which
            // RUNS (see the doc above).
            Decision::Allow | Decision::Confine => Decision::Ask,
            // `Ask` cannot loosen an `Ask`, and a `Deny` is already stricter.
            Decision::Ask | Decision::Deny => posture,
        },
        // `Deny` is the top of the order: it beats every posture arm.
        DefinitionVerdict::Deny => Decision::Deny,
    }
}

/// (issue #101) The ONE place a call's tool name maps onto a per-agent
/// [`ToolKey`]. The recognised keys are the advertised `tool_specs()` names —
/// the same set `subagent_flow` / `core` filter — so a definition's `read:`
/// speaks about the `read` tool and nothing else.
///
/// The per-agent map is PER TOOL KEY, so `read: deny` does not catch a `find`
/// (`find` has its own key) even though both are judged in the Read direction at
/// the gate; that keeps a file's statement as specific as the file wrote it.
/// A `ToolKey::Unrecognized(name)` matches ITS OWN verbatim name only — it has
/// no advertised twin to police, so the match is exact-or-nothing (and the
/// verdict it carries then has no policed direction to reach, see
/// [`AgentLoop::definition_verdict`]).
fn tool_key_matches(key: &ToolKey, tool: &str) -> bool {
    match key {
        ToolKey::Bash => tool == "bash",
        ToolKey::Read => tool == "read",
        ToolKey::Write => tool == "write",
        ToolKey::Edit => tool == "edit",
        ToolKey::Find => tool == "find",
        ToolKey::Grep => tool == "grep",
        ToolKey::Ls => tool == "ls",
        ToolKey::ReadSkill => tool == "read_skill",
        ToolKey::Ask => tool == "ask",
        ToolKey::WebSearch => tool == "web_search",
        ToolKey::FetchContent => tool == "fetch_content",
        ToolKey::Unrecognized(name) => name == tool,
    }
}

/// (T03) The directory an `allow-directory` answer would allow: the RESOLVED
/// write target's nearest EXISTING ANCESTOR DIRECTORY, canonical.
///
/// `canonicalize_lenient` alone is NOT that: it peels to the deepest existing
/// ancestor and RE-APPENDS the missing components, so for
/// `~/.pi/agent/agents/new/SKILL.md` (a brand-new file) it hands back the FILE
/// path with the missing pieces glued on. Persisting THAT as a rule would name
/// a file, and `rule_for` matches by directory prefix — so the "always allow
/// this directory" click would silently never stop a prompt. One `parent()` on
/// the already-resolved value is the whole difference, and it is the ONE thing
/// this seam adds to the shared canonicalizer (no second canonicalizer, per
/// ADR 0029/0030 — the peel still happens exactly once, in `policy_verdict`).
///
/// The result is what `rule_for` can match: component-normal, `~`-free (a
/// `~`-spelled argument has no existing parents, so `canonicalize_lenient`
/// would return the raw residue — and this function's input is the RESOLVED
/// value, never the raw argument), and a directory the user actually meant.
fn guard_ancestor(resolved: &Path) -> PathBuf {
    // `parent()` on a canonical path is the containing directory; a path with
    // no parent (a filesystem root — unreachable for a write target, which is
    // always a file under some dir) falls back to itself rather than to an
    // empty `path`, because an empty value is the placeholder this replaced.
    resolved
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(resolved)
        .to_path_buf()
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
        let (policy, beyond, resolved) = match dir {
            PolicyDirection::Shell => {
                // No path to judge: a command can reach ANYWHERE, so it is
                // always beyond the boundary (see `decision_for`). Rules never
                // apply to `bash` either (the spec's guard-rail), so there is
                // nothing to resolve.
                (self.file_policy.shell, true, None)
            }
            PolicyDirection::Read | PolicyDirection::Write => {
                let Some(raw) = tc.arguments.get("path").and_then(|v| v.as_str()) else {
                    // `read` / `write` / `edit` REQUIRE a path (the executor
                    // rejects the call without one) and the directory tools
                    // default it to the `cwd` — either way there is nothing
                    // to gate, and NOTHING to guard: an absent or empty `path`
                    // never reaches a guard ask.
                    return Some(Decision::Allow);
                };
                let policy = self.direction_policy(dir);
                let beyond = self.beyond_error(dir, Path::new(raw)).is_some();
                // (T03) THE binding resolution, and the ONLY one this task
                // adds. It is the SAME primitive the executor uses —
                // `FsBackend::resolve` resolves under the `write_backend(ctx)`
                // roots through `canonicalize_lenient` — so this value and the
                // executor's `&resolved` are THE SAME PATH (ADR 0029/0030's
                // one-canonicalizer rule). Deliberately NOT strict
                // `Path::canonicalize`: a not-yet-created dir fails strict
                // canonicalization, and a guard ancestor derived from that
                // failure is the silent no-op ADR 0081 / issue #92 exist to
                // close. Emitted guard paths carry THIS value, so a rule T04
                // persists from an `allow-directory` click comes back to
                // `rule_for` in a form it can MATCH.
                //
                // WRITE-ONLY: reads are not rule-gated at the gate (the
                // executor enforces read rules) and the row-3 ASK is a
                // write-direction question, so a read spends nothing here.
                let resolved = (dir == PolicyDirection::Write)
                    .then(|| crate::agent::fs_backend::canonicalize_lenient(Path::new(raw)));
                (policy, beyond, resolved)
            }
        };
        let trusted = self.space_trusted();
        // (T03 rows 1–3) For a WRITE with a usable path, a RULE — or the
        // absence of one under an agent-definition dir — may decide before the
        // posture does. See [`Self::write_rule_decision`] for the rows.
        if dir == PolicyDirection::Write {
            if let Some(resolved) = resolved.as_deref() {
                if let Some((verdict, _)) = self.rule_or_guard_verdict(resolved, trusted) {
                    return Some(verdict);
                }
            }
        }
        Some(stricter_of_two(
            decision_for(policy, beyond, trusted, dir == PolicyDirection::Shell),
            // (issue #101) THE row-4 consultation. This is the ONLY place the
            // per-agent layer enters the gate: the write branch above returned
            // EARLY for rows 1–3 (a rule `Deny`, a rule `Allow`, the guard's
            // `Ask`), so a definition's verdict can never see them and can
            // therefore never bypass them. See [`stricter_of_two`] for the
            // ordering and [`AgentLoop::definition_verdict`] for the projection.
            self.definition_verdict(&tc.name),
        ))
    }

    /// (issue #101) The per-agent verdict for a tool name — `AsSelf` (no
    /// narrowing) when the file does not speak about permissions at all, does
    /// not mention this tool, or the ruleset is a per-pattern object this layer
    /// cannot decide.
    ///
    /// NO PATH RESOLUTION HAPPENS HERE, and that is deliberate (grounding §9 /
    /// ADR 0029's one-canonicalizer rule): the only verdict this layer produces
    /// is a WHOLE-TOOL one (`Rules::catch_all_action()`), which folds onto the
    /// direction's posture verdict — the "beyond the boundary?" question stays
    /// the shared `FsBackend::validate` / `canonicalize_lenient` check the gate
    /// and the executor already agree on, and the boundary ROOT SET (not
    /// opencode's workspace-relative root) is what "relative to what" answers to.
    /// Adding a second pattern resolver here would be the #92 / ADR 0081 escape
    /// class, so there is none.
    ///
    /// A per-pattern ruleset (`whole_tool: false`, e.g. the motivating
    /// `researcher.md`'s `edit:` and `bash:` objects) is `AsSelf` by design:
    /// enforcing those rows needs a command/path matcher nothing has validated
    /// (B1 parses them and announces
    /// `UnsupportedUnvalidatedMatcher(BashPatternRules | PathPatternRules)` —
    /// bash command-text rows and a path-taking tool's path-pattern rows each
    /// announce their own class, #124), so the posture stands —
    /// announced-unsupported, never silently mis-enforced.
    ///
    /// FIRST matching tool key wins (B1's FIRST-WINS per key inside a block).
    fn definition_verdict(&self, tool: &str) -> DefinitionVerdict {
        let Some(block) = self.permission_block.as_ref() else {
            return DefinitionVerdict::AsSelf;
        };
        let Some((_, rules)) = block
            .tools
            .iter()
            .find(|(key, _)| tool_key_matches(key, tool))
        else {
            return DefinitionVerdict::AsSelf;
        };
        match rules.catch_all_action() {
            Some(Action::Deny) => DefinitionVerdict::Deny,
            Some(Action::Ask) => DefinitionVerdict::Ask,
            Some(Action::Allow) => DefinitionVerdict::Allow,
            // An explicit `{"*": action}` pattern ruleset has NO tool-wide
            // verdict (`catch_all_action()` is `None` for it) — see above.
            None => DefinitionVerdict::AsSelf,
        }
    }

    /// Whether this loop's Space is trusted (ADR 0010). ONE place, because
    /// three mechanisms (policy / trust / rules) now consult the same answer
    /// and a fourth copy drifting from the others is the bug class.
    fn space_trusted(&self) -> bool {
        self.trust
            .as_ref()
            .map(|t| t.is_trusted(&self.space_cwd))
            .unwrap_or(false)
    }

    /// (T03 rows 1–3) The rule / guard decision for ONE resolved WRITE target,
    /// shared by `policy_verdict` (which returns the verdict) and
    /// [`Self::guard_target`] (which hands the corridor the target a guard
    /// prompt must name). `rule_for` is the ONE resolver — per `policy.rs`'s
    /// module doc, "per ADR 0029/0030's one-canonicalizer rule the gate and
    /// the executor must both go through [`crate::agent::policy::rule_for`],
    /// never a second matcher" — and the executor's twin in
    /// [`crate::agent::tools::exec::access`] consults it with the SAME list
    /// (`ToolCtx::rules`, the per-turn concat) and the SAME resolved path, so
    /// the two layers cannot decide differently about one write.
    ///
    /// The rows of the spec's precedence table:
    /// 1. a `Deny` rule → [`Decision::Deny`] — a tool-result error, and
    ///    TRUST-IMMUNE (trust suppresses prompts, never widens a deny);
    /// 2. an `Allow` rule → [`Decision::Allow`] SILENTLY (the posture select is
    ///    not reached — this is what makes an `allow-directory` answer mean
    ///    something);
    /// 3. NO rule matches AND the target is under an agent-definition dir →
    ///    [`Decision::Ask`], the guard prompt. Trust suppresses it (the spec:
    ///    "Trust suppresses the guard. A trusted Space's agent can also write
    ///    its own instruction directories without asking"), so on a trusted
    ///    Space this returns `None` and the posture select decides;
    /// 4. otherwise `None` — the caller falls through to `decision_for`.
    ///
    /// The `Option<PathBuf>` in the tuple is the resolved target of a ROW-3
    /// ask — `Some` exactly when the verdict is the guard's `Ask` — so the
    /// prompt can carry the REAL nearest-existing-ancestor instead of T02's
    /// `""` placeholder.
    fn rule_or_guard_verdict(
        &self,
        resolved: &Path,
        trusted: bool,
    ) -> Option<(Decision, Option<PathBuf>)> {
        if let Some(rule) = crate::agent::policy::rule_for(
            &self.rules,
            crate::agent::policy::RuleDirection::Write,
            resolved,
            self.rules_home.as_deref(),
        ) {
            return Some((
                match rule.decision {
                    // Row 1.
                    crate::agent::policy::RuleDecision::Deny => Decision::Deny,
                    // Row 2 — silent; the posture is not consulted.
                    crate::agent::policy::RuleDecision::Allow => Decision::Allow,
                },
                None,
            ));
        }
        // Row 3: no rule speaks to this write, but it lands inside an
        // agent-definition dir. With `default_rules()` EMPTY these two facts
        // are the ORDINARY default posture, not an override — which is the
        // whole point of the round-3 correction.
        if self.is_guard_path(PolicyDirection::Write, resolved) && !trusted {
            return Some((Decision::Ask, Some(resolved.to_path_buf())));
        }
        None
    }

    /// (T03 row 3) The resolved write target of a GUARD ask for this call, or
    /// `None` when the call is not a guard ask (a boundary ask, a rule verdict,
    /// an ungated tool, a call with no usable path). The corridor in `turn.rs`
    /// passes this to `gate_tool` so the guard prompt can name the directory
    /// `allow-directory` would allow.
    ///
    /// It re-derives through the SAME lenient canonicalizer
    /// (`canonicalize_lenient` is idempotent on a canonical path, so this is
    /// the SAME value `policy_verdict` just computed, not a second opinion)
    /// rather than plumbing a path out of the verdict — the verdict type stays
    /// the four-way `Decision` the whole harness already matches on.
    pub(super) fn guard_target(&self, tc: &ToolCall) -> Option<PathBuf> {
        if policed_direction(&tc.name)? != PolicyDirection::Write {
            return None;
        }
        let raw = tc.arguments.get("path").and_then(|v| v.as_str())?;
        let resolved = crate::agent::fs_backend::canonicalize_lenient(Path::new(raw));
        self.rule_or_guard_verdict(&resolved, self.space_trusted())?
            .1
    }

    /// (T03 row 3) Whether a call's RESOLVED path is inside an agent-definition
    /// dir — the four dirs on `boundary::protected_dirs()`, the GUARD-MEMBERSHIP
    /// set that is deliberately NOT a rule set, so reaching it is the ordinary
    /// default posture and not an override.
    ///
    /// The compare is the SAME one the refuse wording uses
    /// (`protected_refusal_text`'s canonicalize-both-sides `starts_with`), and
    /// both sides here are already canonical (the input is `policy_verdict`'s
    /// lenient resolution, each entry comes from `protected_dirs()`), so gate
    /// and executor cannot disagree about "under an agent dir" and there is no
    /// second canonicalizer.
    ///
    /// The ROW-3 ASK is WRITE-only: a READ of an agent-definition file is NOT
    /// an ask at ANY posture — the default denies are gone, so a read with no
    /// matching rule falls to row 4 and the posture select, which under the
    /// default `reads: Allow` proceeds silently. The membership set itself is
    /// direction-agnostic (one set, the four dirs); the DRAWING distinction is
    /// enforced here AND at the write-only call site in `policy_verdict`,
    /// because a helper whose only power is to ASK should not be able to answer
    /// for a direction it must not.
    fn is_guard_path(&self, dir: PolicyDirection, resolved: &Path) -> bool {
        dir == PolicyDirection::Write
            && self.protected.iter().any(|root| resolved.starts_with(root))
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
    /// deny-list wording instead (the true reason).
    ///
    /// (T03) WHICH denials reach here changed; the WORDING did not. With
    /// `default_rules()` EMPTY no default denial exists any more, so a
    /// protected-dir `Deny` is now produced by a USER `deny` RULE (precedence
    /// row 1, trust-immune) — and a denied write must still explain why, in
    /// the same sentence, which is why the deny-list branch stays FIRST: a
    /// rule deny on one of the four dirs must not read as a boundary escape.
    /// A rule deny on an ordinary dir falls to `escape_text` — the boundary
    /// sentence — which is the shape the plan records as an honest limit (the
    /// executor's own row-1 refusal for that case is
    /// `access::rule_deny_text`, and the executor is the layer the model
    /// reaches when the gate's wording is not the boundary's).
    ///
    /// (issue #101) WHICH layers can reach here grew; the WORDING did not, and
    /// nothing here branches on the layer. A per-agent `Deny` arrives through the
    /// SAME `Some(Decision::Deny)` arm in `turn.rs`, so a definition's refusal is
    /// byte-identical to a user-rule or posture refusal and the model's recovery
    /// cannot depend on which layer said no (ADR 0030). No new renderer exists.
    pub(super) fn deny_tool_result(&self, tc: &ToolCall) -> ToolResult {
        let dir = policed_direction(&tc.name);
        let text = match tc.arguments.get("path").and_then(|v| v.as_str()) {
            Some(raw) => {
                let path = Path::new(raw);
                // The guard-membership list is a WRITE-direction question
                // (`protected_dirs` are dirs a WRITE may not enter), and it is
                // checked BEFORE the boundary: a denied write into one of the
                // four dirs must name THAT reason, or the model is sent hunting
                // for a boundary that is not the problem.
                let protected = (dir == Some(PolicyDirection::Write))
                    .then(|| protected_refusal_text(&self.protected, &tc.name, path))
                    .flatten();
                match protected {
                    Some(msg) => msg,
                    // (T03 row 1) A rule `deny` on an ORDINARY dir: the
                    // executor's own sentence for that case is
                    // `access::rule_deny_text`, and the gate must say the same
                    // thing — `deny_tool_result`'s whole reason to exist is that
                    // a gate denial and an executor denial read identically to
                    // the model, and "escapes the session sandbox" for a path
                    // the user explicitly blocked is the drift that design
                    // forbids. Resolved through the SAME canonicalizer and the
                    // SAME list the verdict used, so this cannot disagree with
                    // the check that produced the `Deny`.
                    None => match self.rule_deny_for(dir, path) {
                        Some(rule) => crate::agent::tools::exec::access::rule_deny_text(
                            &tc.name,
                            rule,
                            &crate::agent::fs_backend::canonicalize_lenient(path),
                        ),
                        None => self.escape_text(dir, &tc.name, path, raw),
                    },
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

    /// The `Deny` rule governing this path, in the direction the call is judged
    /// (`None` for `Shell`, which has no path and no rules). The ONE resolver,
    /// with the same per-turn list and the same home the verdict was computed
    /// from — see `policy.rs`'s module doc on the one-canonicalizer rule.
    fn rule_deny_for(
        &self,
        dir: Option<PolicyDirection>,
        path: &Path,
    ) -> Option<&crate::agent::policy::AccessRule> {
        let rdir = match dir? {
            PolicyDirection::Read => crate::agent::policy::RuleDirection::Read,
            PolicyDirection::Write => crate::agent::policy::RuleDirection::Write,
            // Rules never apply to `bash`.
            PolicyDirection::Shell => return None,
        };
        crate::agent::policy::rule_for(
            &self.rules,
            rdir,
            &crate::agent::fs_backend::canonicalize_lenient(path),
            self.rules_home.as_deref(),
        )
        .filter(|rule| rule.decision == crate::agent::policy::RuleDecision::Deny)
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
    ///
    /// # `guard`: which of the TWO prompt kinds this is
    ///
    /// `None` — the BOUNDARY ask (a posture `Ask` beyond the boundary): the
    /// prompt offers `allow` / `reject` / `trust-space`
    /// (`permission_options(true)`), because it asks about the user's own
    /// files and "Don't ask again for this Space" is actionable there.
    ///
    /// `Some(resolved target)` — the GUARD ask (precedence row 3: no rule
    /// matched and the write lands in an agent-definition dir). The prompt
    /// offers `allow` / `allow-directory` / `reject`
    /// (`permission_options(false)`) with the REAL nearest-existing-ancestor
    /// path in `allow-directory`'s `path` — T02's `""` placeholder replaced
    /// HERE, at the only seam that has both the resolved target and the
    /// mindset "this is a guard". `trust-space` is deliberately absent from
    /// that list (T02's rationale: a one-click permanent way to disable the
    /// protection this prompt exists to provide). `allow-directory` is an
    /// ALLOW id — see the predicate below; its `value` rides to the
    /// Supervisor, which persists the rule (Task 4).
    ///
    /// The ancestor is derived from the RESOLVED target the caller hands in
    /// (never from the raw `path` argument): a `~`-spelled argument has no
    /// existing parents, so `canonicalize_lenient` would return the raw
    /// `~`-residue, and a rule persisted from THAT could never be matched by
    /// `rule_for` against the executor's canonicalized target — the click
    /// would silently not stop the next prompt.
    pub(super) async fn gate_tool(
        &self,
        tc: &ToolCall,
        turn: &CancellationToken,
        guard: Option<&Path>,
    ) -> bool {
        let options = match guard {
            None => crate::agent::permission::permission_options(true),
            Some(target) => {
                let mut options = crate::agent::permission::permission_options(false);
                // Index 1 is `allow-directory` BY CONSTRUCTION — the guard
                // list is exactly `[allow, allow-directory, reject]`, pinned
                // by `permission_options`'s own test
                // (`a_guard_prompt_offers_exactly_allow_allow_directory_reject_and_no_trust_space`).
                let ancestor = guard_ancestor(target);
                options[1]["path"] = serde_json::json!(ancestor.to_string_lossy());
                options
            }
        };
        let outcome = native_permission_gate(
            &self.session_id,
            &tc.id,
            &permission_title(&tc.name, &tc.arguments),
            &self.sink,
            &self.pending_permissions,
            self.trust.as_deref(),
            &self.space_cwd,
            turn,
            options,
        )
        .await;
        matches!(
            outcome,
            PermissionOutcome::Selected { option_id, .. }
                if option_id == "allow"
                    || option_id == "trust-space"
                    // (T03) The guard's distinctive answer. `trust-space`
                    // stays approved — it is still in the BOUNDARY list, and
                    // the mid-session flip + the Supervisor's persist key
                    // depend on it.
                    || option_id == "allow-directory"
        )
    }
}

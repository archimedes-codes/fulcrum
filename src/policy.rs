//! The file-access policy vocabulary (ADR 0030): what the user chose for
//! each DIRECTION of file access — reads, writes, `bash`.
//!
//! This module is the vocabulary. It enforces nothing: the boundary (which
//! paths are "inside") is derived elsewhere (Task 2) and the gates read
//! these values (Tasks 3–5). It carries TWO decision points and no others —
//! [`decision_for`] (the per-direction posture select) and [`rule_for`] (the
//! ONE rules resolver, the single decision point the permissions spec adds)
//! — and per ADR 0029/0030's one-canonicalizer rule the gate and the
//! executor must both go through [`rule_for`], never a second matcher. It
//! lives in `agent` because it is domain vocabulary, and `config` imports
//! it — one of the two sanctioned `agent` imports in that leaf module (the
//! other is `harness::WireApi`).

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// What happens to a file access that goes BEYOND the boundary. Inside the
/// boundary everything is allowed in every policy — this enum only decides
/// the beyond-boundary case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AccessPolicy {
    /// Reject it (a tool-result error, never a prompt).
    Sandboxed,
    /// Prompt the user.
    Ask,
    /// Allow it silently. THE derived default — see `FilePolicy`.
    #[default]
    Allow,
}

/// One direction a rule can govern. `ReadWrite` covers both `Read` and
/// `Write` — the wire value a rule written for "this directory" carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RuleDirection {
    Read,
    Write,
    ReadWrite,
}

impl RuleDirection {
    /// Whether a rule with this direction applies to a call of `direction`.
    pub fn covers(self, direction: RuleDirection) -> bool {
        matches!(self, RuleDirection::ReadWrite) || self == direction
    }
}

/// What a matching rule does: allow (proceed silently) or deny (a
/// tool-result error). THERE IS NO `ask` VALUE — asking is what happens
/// when NO rule matches (the self-modification guard), never a decision a
/// rule can carry; a `deny` stays a `deny`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleDecision {
    Allow,
    Deny,
}

/// A user-level EXCEPTION to the posture: `{path, direction, decision}`.
/// `path` supports a leading `~`. Stored as the user wrote it — a leading
/// `~` is expanded to the home dir at MATCH time, never at parse time, so
/// the rule still holds if the home path spelling changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessRule {
    pub path: String,
    pub direction: RuleDirection,
    pub decision: RuleDecision,
}

/// The three per-direction policies (the settings' three selects) plus the
/// user rule list.
///
/// NOT `Copy` — `rules` is a `Vec`. Every consumer that used to copy a
/// `FilePolicy` out of a borrow clones it instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct FilePolicy {
    pub reads: AccessPolicy,
    pub writes: AccessPolicy,
    /// `bash`. Defaults to `Allow` like the others — so this field alone
    /// removes the ADR 0010 untrusted-Space prompt for `bash` unless the
    /// user sets it back to `Ask`. That is the intended product posture
    /// (pi gates nothing), not a bug to "fix" back to `Ask`.
    pub shell: AccessPolicy,
    /// User rules (`path` may lead with `~`). Serde-defaulted: an old
    /// `settings.json` without the key still parses; an EMPTY list is not
    /// serialized, so the three-direction wire shape is byte-identical to
    /// today (keeps `file_policy_serializes_its_three_directions_in_camel_case`
    /// and the config.rs `contains` pins passing unchanged).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<AccessRule>,
}

/// What the GATE does with one tool call (the return of
/// [`decision_for`]) — the four outcomes of the ADR 0030 policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Run the tool as-is.
    Allow,
    /// Prompt via the existing `gate_tool`.
    Ask,
    /// Reject with a tool-result error; never a prompt.
    Deny,
    /// Run, but inside the OS sandbox (Task 5). Produced ONLY for `bash`.
    Confine,
}

/// The ONE policy decision point (ADR 0030): a pure function of the
/// direction's policy, whether the access reaches BEYOND the boundary,
/// whether the Space is trusted, and whether the tool is the shell. It
/// does NO I/O and reads no state — the caller resolves the path and
/// decides `beyond` (via the `FsBackend` the ADR 0029 attack matrix
/// proved), this function only maps that to an outcome.
///
/// Precedence: **Trust suppresses prompts but never widens a deny** —
/// `Sandboxed` ignores `trusted` entirely. That single rule is what the
/// three interacting mechanisms (policy / trust / boundary) hinge on, so
/// it is asserted directly below rather than left to fall out.
///
/// `beyond` is "the resolved path is outside EVERY root of the
/// direction's boundary". `bash` has no path to judge, so the caller
/// passes `beyond = true` UNCONDITIONALLY — a command can reach
/// anywhere. That is what makes the Ask/Allow rows fall out correctly
/// for `bash` and gives `Sandboxed` its bash-specific meaning.
///
/// `Sandboxed` means TWO different things, which is the one subtlety
/// here: for a PATH tool it means *refuse* (there is nothing to confine
/// — the boundary already IS the refusal), while for `bash` it must mean
/// *run confined*: refusing every command would make `Shell=Sandboxed`
/// disable `bash` entirely, and confining is what the user chose. A
/// path-param tool therefore NEVER yields [`Decision::Confine`].
pub fn decision_for(policy: AccessPolicy, beyond: bool, trusted: bool, is_shell: bool) -> Decision {
    // Inside the boundary every policy allows — the policy decides ONLY
    // the beyond-boundary case.
    if !beyond {
        return Decision::Allow;
    }
    match policy {
        // The derived default: silent.
        AccessPolicy::Allow => Decision::Allow,
        // A prompt, unless Trust suppresses it. Trust is checked HERE and
        // nowhere else: it is a suppressor of prompts, never a widener.
        AccessPolicy::Ask => {
            if trusted {
                Decision::Allow
            } else {
                Decision::Ask
            }
        }
        // `bash` is CONFINED, not refused (Task 5's Landlock sandbox); a
        // path tool has nothing to confine, so it is refused. `trusted`
        // is deliberately unread in this arm — Deny is never widened.
        AccessPolicy::Sandboxed if is_shell => Decision::Confine,
        AccessPolicy::Sandboxed => Decision::Deny,
    }
}

/// Expand a leading literal `~` COMPONENT (and only that) to `home`.
///
/// `~user` and an embedded `~` pass through untouched — a directory actually
/// named `~` on disk stays addressable, and this module does not pretend to
/// implement a shell's full tilde grammar. With `home: None` nothing is
/// expanded, so a `~` rule simply matches nothing (the mirror of
/// `protected_dirs()` returning empty when `$HOME` cannot be resolved).
fn expand_rule_path(path: &Path, home: Option<&Path>) -> PathBuf {
    let mut components = path.components();
    match (components.next(), home) {
        (Some(std::path::Component::Normal(raw)), Some(home)) if raw == "~" => {
            home.components().chain(components).collect()
        }
        _ => path.to_path_buf(),
    }
}

/// THE ONE rules resolver (the permissions spec's matching rule): which
/// user rule, if any, governs one access — a pure function of the rule list,
/// the direction, the call path, and the home dir, exactly like
/// [`decision_for`]. It does NO I/O beyond the lenient canonicalization
/// below and reads NO state: `home` is an ARGUMENT (the caller's `$HOME`
/// read), so tests need no env pinning.
///
/// `None` means "no rule speaks to this access", which is NOT a verdict — it
/// is the absence of one, and the caller falls through to the posture select
/// ([`decision_for`]). That is why there is no `ask` in [`RuleDecision`]:
/// asking is what happens when nothing matches.
///
/// **Longest canonical prefix wins, never first-match** — issue #92's failure
/// mode in the rules layer: a broad rule (`~`) listed first must not silently
/// swallow a specific one (`~/.pi/agent/agents`), so the resolver is
/// order-independent for the broad/specific case and the UI needs no rule
/// ordering. The algorithm is FILTER, then RANK, then TIE-BREAK:
///
/// 1. **FILTER** to the rules that are genuine COMPONENT prefixes of the
///    target, via [`Path::starts_with`] — which compares component-wise and
///    is therefore `/tmpfoo`-safe by construction. A STRING `starts_with` has
///    the `/tmpfoo` bug (it calls `/tmpfoo` a prefix of `/tmpfoo/bar`) and
///    must never be used here. Skipping this step and ranking by component
///    count alone has the same bug in a subtler costume: `/tmp` and `/tmpfoo`
///    are both two components, so a non-prefix sibling would win a same-count
///    tie by list order — a silent, order-dependent wrong answer.
/// 2. **RANK** the survivors by canonical component count, longest first.
/// 3. **TIE-BREAK** exact ties by list index, LAST rule wins. After step 1 a
///    tie can only occur between rules naming the SAME canonical path, and
///    last-wins is what makes the concatenation contract work: user rules go
///    AFTER [`default_rules`], so the Supervisor's "always allow this
///    directory" append beats the default deny it ties with.
///
/// Both sides go through the ONE canonicalizer,
/// [`crate::agent::fs_backend::canonicalize_lenient`] — the rule path (after
/// `~` expansion) and the call path — so a rule naming a directory that does
/// not exist YET still matches (ADR 0081's leniency is load-bearing here: a
/// strict `canonicalize()` fails on such a rule path and the rule would match
/// nothing, silently unprotecting the very dirs the deny-list exists for).
///
/// RECORDED RESIDUE (inherited from the canonicalizer, not introduced): a rule
/// whose path cannot be resolved at all — a `~` with `home: None`, a relative
/// ghost `$HOME` — comes back with its raw spelling and therefore matches
/// nothing, exactly the residue `protected_dirs_keeps_an_unresolvable_entry_raw`
/// pins for the deny-list. There is nothing to peel to, so there is no better
/// answer available at this layer.
pub fn rule_for<'a>(
    rules: &'a [AccessRule],
    direction: RuleDirection,
    path: &Path,
    home: Option<&Path>,
) -> Option<&'a AccessRule> {
    // The call path, canonicalized once (leniently — the target of a write
    // into a not-yet-created dir does not exist yet either).
    let target = crate::agent::fs_backend::canonicalize_lenient(path);

    rules
        .iter()
        .enumerate()
        // Direction first (cheap, and a rule that does not govern this
        // direction is not a candidate at all).
        .filter(|(_, rule)| rule.direction.covers(direction))
        .map(|(index, rule)| {
            let expanded = expand_rule_path(Path::new(rule.path.as_str()), home);
            let canon = crate::agent::fs_backend::canonicalize_lenient(&expanded);
            (index, rule, canon)
        })
        // STEP 1 — the FILTER: a genuine component prefix, never a string
        // prefix (`/tmpfoo`), and only among rules that really contain the
        // target.
        .filter(|(_, _, canon)| target.starts_with(canon))
        // STEPS 2 + 3 — longest canonical prefix wins, exact ties to the
        // LAST rule in the list. The key is `(component count, list index)`
        // and `max_by_key` takes the LARGEST key (and returns the last
        // equally-maximum element), so the longest prefix wins and, among
        // rules that tie on length, the highest list index does.
        .max_by_key(|(index, _, canon)| (canon.components().count(), *index))
        .map(|(_, rule, _)| rule)
}

/// The DEFAULT rule set: **EMPTY**.
///
/// # Why empty, when an earlier revision returned four `Write`/`Deny` floors
///
/// The permissions spec's "Step 3 is the only behavioural change in this
/// spec: today's hardcoded deny becomes an ask" is the sentence that decides
/// it. Four `Deny` rules for the agent-definition dirs keep the deny a deny —
/// the refusal stays SILENT, which is the exact defect the spec exists to
/// close. Worse, deriving the write guard-membership set from this rule set
/// (as the previous revision did) makes the guard set and the default-deny set
/// the SAME paths under the SAME prefix predicate, so the spec's row 3
/// ("NO rule matches AND the target is under an agent-definition dir") is
/// `A AND NOT A` and is unreachable at every dir and every nesting depth.
///
/// So the four dirs are a GUARD-MEMBERSHIP set, not a rule set: they live on
/// [`crate::agent::boundary::protected_dirs`], which is its own plain
/// four-path list (never rule-derived, so a settings change can never silently
/// widen the Landlock subtraction that consumes it). A write into one of them
/// with no user rule matching is therefore the ROW-3 ASK — the guard prompt —
/// which is the single behavioural change this spec ships.
///
/// The spec's three "pre-filled defaults" (`~` readWrite `allow`, `/` read
/// `allow`, `~/.pi/agent` read `deny`) are NOT here either: the spec says they
/// are "written only on first write", i.e. a Settings-side draft pre-fill, not
/// a runtime code default — an existing install must never silently gain a
/// restriction. The two `allow` rows are documented there as changing
/// "nothing", since the executor floor is already `["/"]`.
///
/// # The concatenation contract still holds
///
/// Every consumer that puts rules in front of [`rule_for`] MUST emit
/// `default_rules()` FIRST and user-written rules SECOND. `rule_for`'s
/// exact-tie rule (last rule wins) depends on that ordering: the Supervisor's
/// "always allow this directory" action appends one `allow` rule AFTER the
/// defaults. With this function empty the contract is trivially satisfied
/// today, and it stays load-bearing the moment any default is ever reintroduced
/// (or a pre-filled rule is treated as a default) — which is why
/// [`rule_for`]'s tie-break is pinned in BOTH orders by
/// `an_exact_tie_goes_to_the_last_rule_so_an_appended_allow_beats_a_default_deny`,
/// and why the per-turn concat in `AgentLoop::handle_prompt` keeps the
/// defaults-first shape rather than `file_policy.rules` alone.
///
/// CONSEQUENCE, stated plainly: the old count pin (`default_rules().len() == 4`)
/// is now `0`, and
/// `an_empty_rule_set_matches_nothing_so_the_posture_select_decides` (in
/// `boundary.rs`) describes the DEFAULT posture rather than a no-home edge: at
/// defaults `rule_for` answers `None` and the caller falls through — to the
/// guard's row-3 ASK for a write under one of the four dirs, and to the
/// posture select everywhere else.
pub fn default_rules() -> Vec<AccessRule> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use AccessPolicy::*;

    /// `is_shell = false` (a path-param tool) — the default the truth
    /// table is written against.
    const PATH_TOOL: bool = false;

    /// The full 3 × 2 × 2 = 12-case truth table, asserted CASE BY CASE
    /// (one `assert_eq!` per row, each labelled) so a failure names the
    /// row that broke rather than a row index.
    #[test]
    fn the_truth_table_maps_every_policy_beyond_trust_combination() {
        // `beyond = false`: inside the boundary EVERY policy allows, for
        // both trust values (4 rows).
        for policy in [Sandboxed, Ask, Allow] {
            for trusted in [false, true] {
                assert_eq!(
                    decision_for(policy, false, trusted, PATH_TOOL),
                    Decision::Allow,
                    "{policy:?} inside the boundary, trusted={trusted}"
                );
            }
        }
        // `beyond = true` (8 rows).
        for trusted in [false, true] {
            assert_eq!(
                decision_for(Sandboxed, true, trusted, PATH_TOOL),
                Decision::Deny,
                "Sandboxed beyond the boundary is a Deny, trusted={trusted}"
            );
            assert_eq!(
                decision_for(Allow, true, trusted, PATH_TOOL),
                Decision::Allow,
                "Allow beyond the boundary is silent, trusted={trusted}"
            );
        }
        assert_eq!(
            decision_for(Ask, true, false, PATH_TOOL),
            Decision::Ask,
            "Ask beyond the boundary on an untrusted Space prompts"
        );
        assert_eq!(
            decision_for(Ask, true, true, PATH_TOOL),
            Decision::Allow,
            "Ask beyond the boundary on a TRUSTED Space is suppressed"
        );
        // The `bash` rows of the same table (`is_shell = true`): the
        // Ask/Allow rows are IDENTICAL to the path tools — only the
        // Sandboxed row diverges, and it is asserted by name below.
        for trusted in [false, true] {
            assert_eq!(
                decision_for(Ask, true, trusted, true),
                decision_for(Ask, true, trusted, PATH_TOOL),
                "Ask treats the shell like a path tool, trusted={trusted}"
            );
            assert_eq!(
                decision_for(Allow, true, trusted, true),
                Decision::Allow,
                "Allow treats the shell like a path tool, trusted={trusted}"
            );
        }
        // The shell's `beyond = false` row: the caller never passes it for
        // `bash` (a command is always beyond), but the row must still hold —
        // `Sandboxed` gates nothing inside the boundary.
        assert_eq!(
            decision_for(Sandboxed, false, false, true),
            Decision::Allow,
            "`beyond = false` allows even the shell under Sandboxed"
        );
    }

    /// THE precedence rule: Trust suppresses an `Ask` but never widens a
    /// `Sandboxed` Deny — asserted for BOTH directions so neither half of
    /// the sentence can be dropped without reddening this test.
    #[test]
    fn trust_suppresses_ask_but_never_widens_sandboxed_deny() {
        // Trust SUPPRESSES the prompt…
        assert_eq!(decision_for(Ask, true, false, PATH_TOOL), Decision::Ask);
        assert_eq!(decision_for(Ask, true, true, PATH_TOOL), Decision::Allow);
        // …and NEVER turns a Deny into anything else — `Sandboxed` gives
        // the SAME answer for both trust values, for the path tool AND for
        // the shell (the shell's answer being `Confine`, which is not an
        // widening either).
        assert_eq!(
            decision_for(Sandboxed, true, false, PATH_TOOL),
            decision_for(Sandboxed, true, true, PATH_TOOL),
            "Trust must not widen a Sandboxed Deny"
        );
        assert_eq!(
            decision_for(Sandboxed, true, true, PATH_TOOL),
            Decision::Deny,
            "the trusted Sandboxed answer is still a Deny, never a prompt"
        );
        assert_eq!(
            decision_for(Sandboxed, true, false, true),
            decision_for(Sandboxed, true, true, true),
            "Trust must not change the shell's Sandboxed answer either"
        );
        // And a Deny is never `Ask` — a Sandboxed Space emits no prompt at
        // all in any configuration (the acceptance: a denied access never
        // reaches the UI).
        for trusted in [false, true] {
            for is_shell in [false, true] {
                assert_ne!(
                    decision_for(Sandboxed, true, trusted, is_shell),
                    Decision::Ask,
                    "Sandboxed must never prompt, trusted={trusted} is_shell={is_shell}"
                );
            }
        }
    }

    /// `Shell=Sandboxed` means CONFINE, not DENY: `bash` still runs (in
    /// the sandbox), or the tier would disable the shell outright.
    #[test]
    fn sandboxed_shell_confines_instead_of_denying() {
        for trusted in [false, true] {
            assert_eq!(
                decision_for(Sandboxed, true, trusted, true),
                Decision::Confine,
                "bash under Shell=Sandboxed is confined, trusted={trusted}"
            );
        }
        // CONTRAST (so this cannot pass vacuously): the SAME inputs with
        // `is_shell = false` are a Deny.
        assert_eq!(
            decision_for(Sandboxed, true, false, PATH_TOOL),
            Decision::Deny,
            "a path tool with the same inputs is refused"
        );
        // And the other two policies never yield `Confine` for the shell —
        // confining is what `Sandboxed` MEANS, not something Ask/Allow opt
        // into.
        for policy in [Ask, Allow] {
            for trusted in [false, true] {
                assert_ne!(
                    decision_for(policy, true, trusted, true),
                    Decision::Confine,
                    "{policy:?} must not confine, trusted={trusted}"
                );
            }
        }
    }

    /// No PATH-param tool can EVER yield `Confine` — the invariant the
    /// executor relies on (`Confine` means "dispatch, and `bash` will
    /// confine itself"; a path tool reaching it would run unsandboxed
    /// under a tier that meant refusal).
    #[test]
    fn path_tools_never_confine() {
        for policy in [Sandboxed, Ask, Allow] {
            for beyond in [false, true] {
                for trusted in [false, true] {
                    assert_ne!(
                        decision_for(policy, beyond, trusted, PATH_TOOL),
                        Decision::Confine,
                        "{policy:?} beyond={beyond} trusted={trusted} must not confine a path tool"
                    );
                }
            }
        }
        // Exhaustive sweep of the shell column too: `Confine` appears ONLY
        // at (Sandboxed, beyond=true) — 1 of the 16 shell cases.
        let confined: Vec<_> = [Sandboxed, Ask, Allow]
            .iter()
            .flat_map(|p| [false, true].map(|b| (*p, b)))
            .filter(|(p, b)| decision_for(*p, *b, false, true) == Decision::Confine)
            .collect();
        assert_eq!(confined, vec![(Sandboxed, true)]);
    }

    #[test]
    fn access_policy_default_is_allow_not_sandboxed() {
        // The derived default is `Allow` (pi's posture, ADR 0030 Deviation 2).
        // Guards against a regression that puts `#[default]` back on
        // `Sandboxed` — that would silently re-tighten every policy.
        assert_eq!(AccessPolicy::default(), AccessPolicy::Allow);
    }

    #[test]
    fn file_policy_default_allows_every_direction() {
        let policy = FilePolicy::default();
        assert_eq!(policy.reads, AccessPolicy::Allow);
        assert_eq!(policy.writes, AccessPolicy::Allow);
        assert_eq!(policy.shell, AccessPolicy::Allow);
    }

    #[test]
    fn access_policy_wire_values_are_lowercase() {
        // The wire format is exactly `"sandboxed" | "ask" | "allow"` — the UI
        // label ("Don't ask me") stays out of it.
        let cases = [
            (AccessPolicy::Sandboxed, "\"sandboxed\""),
            (AccessPolicy::Ask, "\"ask\""),
            (AccessPolicy::Allow, "\"allow\""),
        ];
        for (policy, wire) in cases {
            assert_eq!(serde_json::to_string(&policy).unwrap(), wire);
            assert_eq!(
                serde_json::from_str::<AccessPolicy>(wire).unwrap(),
                policy,
                "deserializing {wire}"
            );
        }
    }

    #[test]
    fn file_policy_serializes_its_three_directions_in_camel_case() {
        let json = serde_json::to_string(&FilePolicy {
            reads: AccessPolicy::Sandboxed,
            writes: AccessPolicy::Ask,
            shell: AccessPolicy::Allow,
            rules: Vec::new(),
        })
        .unwrap();
        assert_eq!(
            json,
            r#"{"reads":"sandboxed","writes":"ask","shell":"allow"}"#
        );
        let back: FilePolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back.reads, AccessPolicy::Sandboxed);
        assert_eq!(back.writes, AccessPolicy::Ask);
        assert_eq!(back.shell, AccessPolicy::Allow);
    }

    #[test]
    fn a_partial_file_policy_defaults_the_missing_directions_to_allow() {
        // The container-level `#[serde(default)]`: a partial object parses
        // with the omitted directions at `Allow` (never a parse error).
        let policy: FilePolicy = serde_json::from_str(r#"{ "reads": "ask" }"#).unwrap();
        assert_eq!(policy.reads, AccessPolicy::Ask);
        assert_eq!(policy.writes, AccessPolicy::Allow);
        assert_eq!(policy.shell, AccessPolicy::Allow);
        // And an empty object is all-`Allow`.
        let empty: FilePolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(empty, FilePolicy::default());
    }

    /// The rule wire shape: `path` verbatim (a leading `~` is STORED, never
    /// expanded at parse time), `direction` camelCase (`readWrite` is the
    /// `ReadWrite` wire value), `decision` lowercase.
    #[test]
    fn an_access_rule_serializes_its_three_fields_in_camel_case_and_lowercase() {
        let rule = AccessRule {
            path: "~/.pi/agent".to_string(),
            direction: RuleDirection::Read,
            decision: RuleDecision::Deny,
        };
        let json = serde_json::to_string(&rule).unwrap();
        assert_eq!(
            json,
            r#"{"path":"~/.pi/agent","direction":"read","decision":"deny"}"#
        );
        let back: AccessRule = serde_json::from_str(&json).unwrap();
        assert_eq!(back, rule);
        // `ReadWrite` is camelCase on the wire — NOT `read_write` / `READ_WRITE`.
        assert_eq!(
            serde_json::to_string(&RuleDirection::ReadWrite).unwrap(),
            r#""readWrite""#
        );
        let rw: RuleDirection = serde_json::from_str(r#""readWrite""#).unwrap();
        assert_eq!(rw, RuleDirection::ReadWrite);
        for (direction, wire) in [
            (RuleDirection::Read, r#""read""#),
            (RuleDirection::Write, r#""write""#),
        ] {
            assert_eq!(serde_json::to_string(&direction).unwrap(), wire);
            assert_eq!(
                serde_json::from_str::<RuleDirection>(wire).unwrap(),
                direction
            );
        }
    }

    /// THE spec's central rule, pinned as a wire pin: a rule can only ever
    /// ALLOW or DENY. Asking is what happens when NO rule matches, so `"ask"`
    /// is not a value a rule can carry — and a `deny` can never be turned
    /// into a prompt by the UI or by a hand-edited `settings.json`.
    #[test]
    fn there_is_no_ask_decision_value() {
        assert!(
            serde_json::from_str::<RuleDecision>("\"ask\"").is_err(),
            "`ask` must not parse as a rule decision — asking is the ABSENCE of a match"
        );
        // The enum is EXACTLY `"allow" | "deny"` — nothing else parses, and
        // both of those round-trip.
        for wire in [r#""allow""#, r#""deny""#] {
            let parsed: RuleDecision = serde_json::from_str(wire).unwrap();
            assert_eq!(serde_json::to_string(&parsed).unwrap(), wire);
        }
        for bogus in [r#""ASK""#, r#""Ask""#, r#""prompt""#, "null", "0"] {
            assert!(
                serde_json::from_str::<RuleDecision>(bogus).is_err(),
                "{bogus} must not parse as a rule decision"
            );
        }
    }

    /// The back-compat half: an old `settings.json` (no `rules` key) still
    /// parses — both a partial object and `{}` (the container-level
    /// `#[serde(default)]` fills `rules` with the empty `Vec`).
    #[test]
    fn file_policy_without_a_rules_key_parses_with_rules_empty() {
        let policy: FilePolicy =
            serde_json::from_str(r#"{"reads":"allow","writes":"allow"}"#).unwrap();
        assert!(policy.rules.is_empty());
        assert_eq!(policy.reads, AccessPolicy::Allow);
        assert_eq!(policy.writes, AccessPolicy::Allow);
        // `{}` is `FilePolicy::default()` — rules empty, every direction Allow.
        let empty: FilePolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(empty, FilePolicy::default());
        assert!(empty.rules.is_empty());
    }

    /// The forward-compat half: an EMPTY rule list is not serialized, so the
    /// wire bytes for a settings file with no rules are IDENTICAL to today's
    /// three-direction shape (this is what keeps the config.rs `contains`
    /// pins passing untouched).
    #[test]
    fn an_empty_rules_list_stays_out_of_the_wire_format() {
        assert_eq!(
            serde_json::to_string(&FilePolicy::default()).unwrap(),
            r#"{"reads":"allow","writes":"allow","shell":"allow"}"#
        );
    }

    /// And once there IS a rule, it rides inside the same `filePolicy` object
    /// (no new top-level settings key).
    #[test]
    fn a_populated_rules_list_serializes_inside_the_file_policy_object() {
        let policy = FilePolicy {
            reads: AccessPolicy::Allow,
            writes: AccessPolicy::Allow,
            shell: AccessPolicy::Allow,
            rules: vec![AccessRule {
                path: "~/.pi/agent".to_string(),
                direction: RuleDirection::Read,
                decision: RuleDecision::Deny,
            }],
        };
        assert_eq!(
            serde_json::to_string(&policy).unwrap(),
            r#"{"reads":"allow","writes":"allow","shell":"allow","rules":[{"path":"~/.pi/agent","direction":"read","decision":"deny"}]}"#
        );
        let back: FilePolicy =
            serde_json::from_str(&serde_json::to_string(&policy).unwrap()).unwrap();
        assert_eq!(back, policy);
    }

    // ── the rules resolver (`rule_for`) ────────────────────────────────
    //
    // Every test below passes a `ScratchDir` as the resolver's `home`
    // argument — `rule_for` reads NO env, so these need no `$HOME` pinning
    // and no env lock. Assertions compare the RETURNED rule against the
    // fixture rules BY VALUE (never against a canonical path), so nothing
    // here depends on the temp dir's canonical spelling (`/var` →
    // `/private/var` on macOS).

    /// A fresh scratch dir used as the resolver's `home` argument.
    fn scratch_home(tag: &str) -> crate::test_support::ScratchDir {
        crate::test_support::ScratchDir::in_env_temp("policy-rules-", tag)
    }

    /// A fixture rule (the raw user spelling, exactly what a `settings.json`
    /// holds — a leading `~` that is expanded only at MATCH time).
    fn rule(path: &str, direction: RuleDirection, decision: RuleDecision) -> AccessRule {
        AccessRule {
            path: path.to_string(),
            direction,
            decision,
        }
    }

    /// **Spec pin 1 — longest prefix wins, NOT first match** (the issue #92
    /// failure mode in the rules layer: a broad `~` rule must not silently
    /// swallow a specific one). Asserted in BOTH list orders, so a
    /// first-match implementation reddens on the broad-then-specific list.
    ///
    /// The call path is NOT created on disk, which also reddens a resolver
    /// that strict-`canonicalize()`s the rule path (that fails, matches
    /// nothing, and answers `None`).
    #[test]
    fn the_longest_canonical_prefix_wins_over_first_match_regardless_of_rule_order() {
        use RuleDecision::{Allow, Deny};
        use RuleDirection::*;
        let home = scratch_home("longest-prefix");
        let target = home.path().join(".pi/agent/agents/SKILL.md");
        let broad = rule("~", ReadWrite, Allow);
        let specific = rule("~/.pi/agent/agents", Write, Deny);

        // BROAD FIRST — a first-match resolver returns the broad Allow here.
        let rules = [broad.clone(), specific.clone()];
        let hit = rule_for(&rules, Write, &target, Some(home.path()));
        assert_eq!(
            hit,
            Some(&specific),
            "the specific deny must win the broad-first list"
        );

        // REVERSED — the same answer (the order-independence half).
        let rules = [specific.clone(), broad.clone()];
        let hit = rule_for(&rules, Write, &target, Some(home.path()));
        assert_eq!(
            hit,
            Some(&specific),
            "the same deny wins the specific-first list"
        );
    }

    /// **Pin 2 — the `/tmpfoo` class.** Component-count ranking ALONE is
    /// wrong: `~/.pi/agents` and `~/.pi/agent` are both one component under
    /// the home, and a non-prefix sibling would win a same-count tie by list
    /// order. The filter (genuine `Path::components` prefix) must come FIRST,
    /// so the sibling is out of the ranking entirely and list order cannot
    /// rescue it.
    #[test]
    fn a_non_prefix_sibling_path_with_the_same_component_count_never_beats_a_genuine_prefix() {
        use RuleDecision::Deny;
        use RuleDirection::Write;
        let home = scratch_home("sibling-same-count");
        let target = home.path().join(".pi/agent/SKILL.md");
        let sibling = rule("~/.pi/agents", Write, Deny);
        let genuine = rule("~/.pi/agent", Write, Deny);

        // SIBLING FIRST: a `(component_count, index)` tie-break without a
        // prefix filter picks the sibling here.
        let rules = [sibling.clone(), genuine.clone()];
        let hit = rule_for(&rules, Write, &target, Some(home.path()));
        assert_eq!(hit, Some(&genuine), "a non-prefix sibling must never match");

        // And the same result with the list swapped — the winner is the
        // prefix, not the position.
        let rules = [genuine.clone(), sibling.clone()];
        let hit = rule_for(&rules, Write, &target, Some(home.path()));
        assert_eq!(
            hit,
            Some(&genuine),
            "the genuine prefix wins in both orders"
        );
    }

    /// **Pin 3 — the `~` vs `/` surface** the spec's own pre-filled defaults
    /// expose (`~` rw allow / `/` read allow): a path under the home is under
    /// BOTH rules, so one of them must win, and it must be the LAST one in the
    /// list.
    ///
    /// WHAT THIS ACTUALLY PINS (corrected in execution — the plan described
    /// this as a ONE-COMPONENT tie, and that is measurably false for a temp
    /// home): the root rule canonicalizes to ONE component on unix while `~`
    /// expands to the scratch home and canonicalizes to THREE (verified by
    /// probe: `root=1 home=3`). So the `~` Allow below wins on LONGEST PREFIX,
    /// not on the tie-break, and this test's third assertion is decided by step
    /// 2 rather than step 3. It still earns its place — it pins that both
    /// spellings reach a path under the home and that the more specific `~`
    /// beats the broader root regardless of which is listed last — but the
    /// EXACT-TIE contract is pinned non-vacuously by
    /// `an_exact_tie_goes_to_the_last_rule_so_an_appended_allow_beats_a_default_deny`,
    /// which is the only place two rules really do share a component count
    /// (they name the same canonical path). A genuine `~`/root tie would need
    /// `$HOME` itself to be a one-component path (i.e. `HOME=/`), which no
    /// scratch fixture can be.
    ///
    /// THE ROOT IS NOT THE STRING `"/"` (Windows CI): on unix `/` is the one
    /// true filesystem root, but on Windows a lone `/` is only the CURRENT
    /// DRIVE'S root in the cwd's own spelling and is not a component prefix of
    /// the verbatim (`\\?\C:\…`) form `canonicalize` returns, so the first
    /// assertion below — "a root rule covers a path under the home" — failed
    /// there with `None` while staying green on Linux and macOS.
    /// `test_support::filesystem_root` names the root OF THE SCRATCH HOME'S OWN
    /// DRIVE in the spelling the shared canonicalizer produces, which is the
    /// claim this test is actually making. The component-count note above is
    /// the unix probe: on Windows the drive root is two components
    /// (`Prefix` + `RootDir`) against the scratch home's many, so the `~` rule
    /// still wins step 2 there for the same reason.
    #[test]
    fn a_tilde_rule_and_a_slash_rule_tie_at_one_component_and_last_wins() {
        use RuleDecision::{Allow, Deny};
        use RuleDirection::Write;
        let home = scratch_home("tilde-slash-tie");
        let target = home.path().join(".pi/agent/agents/SKILL.md");

        // Each half on its own matches the same path (both really do contain
        // it — the overlap the spec's defaults create).
        let root = rule(
            &crate::test_support::filesystem_root(home.path()).to_string_lossy(),
            Write,
            Deny,
        );
        let rules = [root.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&root),
            "a filesystem-root rule covers a path under the home"
        );
        let home_rule = rule("~", Write, Allow);
        let rules = [home_rule.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&home_rule),
            "a `~` rule covers the same path"
        );

        // BOTH PRESENT, `/` first, `~` last → the `~` Allow wins. (See the
        // doc: this is the longer prefix winning, not the tie-break.)
        let rules = [root.clone(), home_rule.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&home_rule),
            "the `~` rule must beat the `/` rule for a path under the home"
        );
    }

    /// **Pin 2b — the `/tmpfoo` bug in the costume the sibling pin above does
    /// NOT catch: a rule dir that is a strict STRING prefix of the target
    /// without being a COMPONENT prefix of it.** `/a/bc` is a strict string
    /// prefix of `/a/bcd/x` and is NOT a component prefix, so the component
    /// form (`Path::starts_with`, what the FILTER uses) rejects it and a
    /// string `starts_with` accepts it.
    ///
    /// Why this test exists next to
    /// `a_non_prefix_sibling_path_with_the_same_component_count_never_beats_a_genuine_prefix`:
    /// that fixture (`~/.pi/agents` vs `~/.pi/agent/…`) is not a string prefix
    /// either (`agents` vs `agent/`), so it stays GREEN against a string
    /// `starts_with` — the sibling pin guards the ranking, not the string form.
    /// This pair is the case that actually separates the two implementations,
    /// and it is the only one that reddens when the FILTER is mutated to
    /// `str::starts_with` (verified by mutation: this test alone goes red).
    #[test]
    fn a_rule_dir_that_is_only_a_string_prefix_of_the_target_matches_nothing() {
        use RuleDecision::Deny;
        use RuleDirection::Write;
        let home = scratch_home("string-prefix");
        // Both dirs exist, so neither side's canonicalization is doing the
        // work — only the prefix FORM is under test.
        std::fs::create_dir_all(home.path().join("bc")).unwrap();
        std::fs::create_dir_all(home.path().join("bcd")).unwrap();
        let rule_dir = rule("~/bc", Write, Deny);
        let rules = [rule_dir.clone()];

        // `<home>/bc` is a strict string prefix of `<home>/bcd/x.txt` and is
        // NOT a component prefix — the rule says nothing about that path.
        let sibling = home.path().join("bcd/x.txt");
        assert_eq!(
            rule_for(&rules, Write, &sibling, Some(home.path())),
            None,
            "a rule dir that is only a STRING prefix of the target must not match \
             (a string `starts_with` would match it — the `/tmpfoo` bug)"
        );

        // NON-VACUITY: the same rule DOES match a real descendant of itself,
        // so the `None` above came from the prefix form, not from a fixture
        // that matches nothing at all.
        let inside = home.path().join("bc/x.txt");
        assert_eq!(
            rule_for(&rules, Write, &inside, Some(home.path())),
            Some(&rule_dir),
            "a genuine component prefix still matches (the contrast)"
        );
    }

    /// **Spec pin 2 — ADR 0081 leniency is load-bearing for rules:** a rule
    /// naming a directory that does not exist YET still matches its
    /// descendants. A strict `canonicalize()` on the rule path fails here and
    /// the rule would match nothing — silently unprotecting the very dirs the
    /// deny-list exists for (issue #92, again).
    #[test]
    fn a_rule_naming_a_directory_that_does_not_exist_yet_matches_leniently() {
        use RuleDecision::Deny;
        use RuleDirection::Write;
        let home = scratch_home("lenient-rule");
        // NOTE: `home/.pi/agent/agents` is deliberately NEVER created.
        let agents = rule("~/.pi/agent/agents", Write, Deny);
        let rules = [agents.clone()];
        let target = home.path().join(".pi/agent/agents/new/SKILL.md");
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&agents),
            "a rule for a not-yet-existing dir matches inside it (lenient canonicalization)"
        );
    }

    /// The spec's rows 3 and 4, both as `None`: **asking is the absence of a
    /// match.** No rule for this path, and no rule for this direction — each
    /// falls through to the posture select rather than to an invented default.
    #[test]
    fn no_matching_rule_returns_none_the_ask_is_the_absence_of_a_match() {
        use RuleDecision::Deny;
        use RuleDirection::*;
        let home = scratch_home("no-match");

        // Row 3 (the ask row): a rule for an UNRELATED directory — the call
        // path is nowhere under it.
        let rules = [rule("~/.pi/agent", Write, Deny)];
        let unrelated = home.path().join("projects/app/src/main.rs");
        assert_eq!(
            rule_for(&rules, Write, &unrelated, Some(home.path())),
            None,
            "a path outside every rule matches nothing"
        );

        // Row 4 (the posture-select fall-through): the path IS under a rule,
        // but no rule governs this DIRECTION.
        let target = home.path().join(".pi/agent/agents/SKILL.md");
        assert_eq!(
            rule_for(&rules, Read, &target, Some(home.path())),
            None,
            "a write-only rule must not answer a read"
        );
    }

    /// THE exact-tie contract (the seam T03/T04 must not invert): user rules
    /// are concatenated AFTER `default_rules()`, so the Supervisor's
    /// "always allow this directory" append is an EXACT tie with the default
    /// deny and must win it — otherwise allow-directory silently no-ops
    /// against the very rule it is meant to override.
    #[test]
    fn an_exact_tie_goes_to_the_last_rule_so_an_appended_allow_beats_a_default_deny() {
        use RuleDecision::*;
        use RuleDirection::Write;
        let home = scratch_home("exact-tie");
        let target = home.path().join(".pi/agent/agents/SKILL.md");
        let deny = rule("~/.pi/agent/agents", Write, Deny);
        let allow = rule("~/.pi/agent/agents", Write, Allow);

        // DEFAULTS FIRST, user append second (the contract) → the Allow wins.
        let rules = [deny.clone(), allow.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&allow),
            "the appended allow must beat the default deny on an exact tie"
        );

        // INVERTED concatenation → the deny wins, i.e. allow-directory
        // silently no-ops. Pinned so the inversion is a visible failure.
        let rules = [allow.clone(), deny.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&deny),
            "user rules listed BEFORE the defaults invert the contract (see `default_rules`)"
        );
    }

    /// Direction coverage, asserted in BOTH directions independently: `Read`
    /// does not cover a `Write` call, `Write` does not cover a `Read` call,
    /// and `ReadWrite` covers both.
    #[test]
    fn a_read_rule_does_not_cover_a_write_and_read_write_covers_both() {
        use RuleDecision::Deny;
        use RuleDirection::*;
        let home = scratch_home("direction-coverage");
        let target = home.path().join(".pi/agent/agents/SKILL.md");

        // A `Read` rule never answers a `Write` call.
        let read_only = rule("~/.pi/agent", Read, Deny);
        let rules = [read_only.clone()];
        assert_eq!(
            rule_for(&rules, Read, &target, Some(home.path())),
            Some(&read_only)
        );
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            None,
            "a Read rule must not cover a Write call"
        );

        // A `Write` rule never answers a `Read` call (the INDEPENDENT half).
        let write_only = rule("~/.pi/agent", Write, Deny);
        let rules = [write_only.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&write_only)
        );
        assert_eq!(
            rule_for(&rules, Read, &target, Some(home.path())),
            None,
            "a Write rule must not cover a Read call"
        );

        // `ReadWrite` covers both.
        let both = rule("~/.pi/agent", ReadWrite, Deny);
        let rules = [both.clone()];
        assert_eq!(
            rule_for(&rules, Read, &target, Some(home.path())),
            Some(&both)
        );
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&both)
        );
    }

    /// `home` is the ONLY ambient input and it is an ARGUMENT: a leading `~`
    /// expands to the passed home, and with `home: None` the same rule
    /// matches nothing (the mirror of `protected_dirs()` returning empty when
    /// `$HOME` cannot be resolved).
    #[test]
    fn a_tilde_rule_expands_to_the_passed_home_dir() {
        use RuleDecision::Allow;
        use RuleDirection::{ReadWrite, Write};
        let home = scratch_home("tilde-expansion");
        let target = home.path().join("anything/deep.txt");
        let home_rule = rule("~", ReadWrite, Allow);
        let rules = [home_rule.clone()];
        assert_eq!(
            rule_for(&rules, Write, &target, Some(home.path())),
            Some(&home_rule),
            "the `~` rule expands to the PASSED home"
        );
        assert_eq!(
            rule_for(&rules, Write, &target, None),
            None,
            "with no home to expand to, a `~` rule matches nothing"
        );
    }
}

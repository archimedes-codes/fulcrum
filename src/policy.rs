//! The file-access policy vocabulary (ADR 0030): what the user chose for
//! each DIRECTION of file access — reads, writes, `bash`.
//!
//! This module is the vocabulary ONLY. It decides nothing and enforces
//! nothing: the boundary (which paths are "inside") is derived elsewhere
//! (Task 2) and the gates read these values (Tasks 3–5). It lives in
//! `agent` because it is domain vocabulary, and `config` imports it — one
//! of the two sanctioned `agent` imports in that leaf module (the other is
//! `harness::WireApi`).

use serde::{Deserialize, Serialize};

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

/// The three per-direction policies (the settings' three selects).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct FilePolicy {
    pub reads: AccessPolicy,
    pub writes: AccessPolicy,
    /// `bash`. Defaults to `Allow` like the others — so this field alone
    /// removes the ADR 0010 untrusted-Space prompt for `bash` unless the
    /// user sets it back to `Ask`. That is the intended product posture
    /// (pi gates nothing), not a bug to "fix" back to `Ask`.
    pub shell: AccessPolicy,
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
        // The derived default is `Allow` (pi's posture, plan Deviation 2).
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
}

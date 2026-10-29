//! The parsed `permission:` frontmatter block of an Agent definition
//! (issue #101), as a NARROWING layer only: rules here can only make a
//! call MORE restricted than the user's `filePolicy` would — never less.
//! No shape in this module can express "allow beyond the user's policy",
//! and the ONLY whole-tool spellings ever produced are the three actions
//! `allow | ask | deny`.
//!
//! ## The upstream this reader mirrors (ADR 0020 parity — stated up front)
//!
//! This block's upstream is **opencode's markdown-agent `permission:` block**
//! (opencode docs: "permission", "You can also set permissions in Markdown
//! agents" — it mirrors `config.permission`, opencode's `permission` object
//! grammar / last-matching-rule-wins ordering; no version claim is pinned
//! here). Nothing in the `parse_value` "both copies" rule or the ADR 0020
//! consequence applies to it: the block is NOT part of the pi agent format
//! (pi's subagent reader, `pi-subagents/src/agents/frontmatter.ts`, has no
//! such key), both `agents.rs` and `skills.rs`'s `parse_value` copies read
//! scalars only, and the two named divergences between the copies (the
//! trailing-`#` strip and the `''` un-doubling) name DIFFERENT upstreams
//! (pi's `yaml` vs pi's `frontmatter.ts` regex reader). If a shape bug is
//! found in this block reader, it has ONE owner: this block reader alone.
//!
//! The consequence runs BOTH ways, and
//! `the_two_parse_value_copies_are_untouched_by_the_permission_reader`
//! pins it: a bug in either `parse_value` copy (the NBSP/dedent byte-offset
//! guard, the quote pair, the `: ` guard) is NOT applied here, and a bug here
//! is NOT applied there. A change that touches the `parse_value` copies is
//! therefore a test failure by construction, not a silent cross-contamination.
//!
//! ## What this module is NOT
//!
//! * Not [`crate::agent::policy`] (`AccessPolicy` / `FilePolicy` /
//!   `AccessRule`): that vocabulary is per-DIRECTION and global, and its
//!   `Sandboxed` carries the Landlock meaning. This one is per-DEFINITION and
//!   per-TOOL.
//! * Not `crate::agent::permission` (a PRIVATE module, despite the similar
//!   name) — that is the unrelated ADR 0010 native UI prompt gate.
//! * Not an enforcer. This task parses, types and ANNOUNCES; carrying the
//!   block to the gate and deciding the strict-of-two against the user's
//!   policy is the follow-up task's job.

// `Deserialize` is NOT decoration here: `LaunchConfig` (which carries
// `PermissionBlock` across `SubagentDispatchWire.launch`, issue #101 task05a)
// derives `Deserialize`, so a `Serialize`-only block is an E0277 in the parent
// crate. NO serde attribute was added alongside the derive — the pinned
// spellings (externally-tagged notes, lowercase `Action`, camelCase `ToolKey`,
// and the deliberately snake_case `whole_tool`) are exactly what round-trips.
use serde::{Deserialize, Serialize};

/// The catch-all pattern spelling — the pattern opencode's docs put FIRST so
/// the specific rules after it carve out exceptions. PRIVATE on purpose: the
/// public surface of this module is exactly the aggregate the plan pins, and
/// this is a storage detail of `Rules::whole_tool`, not a type to carry.
const CATCH_ALL_PATTERN: &str = "*";

/// The three verdicts opencode's permission model carries (`allow | ask | deny`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Allow,
    Ask,
    Deny,
}

/// One deciding rule for a PATH-TAKING tool (issue #101, opencode shape):
/// a `pattern` that matched and the `action` the LAST matching rule in the
/// tool's (ordered) map carried. ORDERED: opencode resolves a tool's pattern
/// object in order, LAST matching rule wins — which is why the `"*"` catch-all
/// is written FIRST and the specifics after it carve out exceptions.
/// `pattern` is the layer's only place where opencode wildcards (`*`, `?`) can
/// appear. (Despite the generic sound of `pattern`, this is a PATH/PATTERN
/// matcher for path-taking tools ONLY — see [`ToolKey`].)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub pattern: String,
    pub action: Action,
}

/// The per-tool ruleset. ORDERED — opencode applies a tool's pattern object in
/// file order, LAST matching rule wins (the `"*"` catch-all first, the
/// specifics after it). A bare verdict is stored as the catch-all pattern with
/// `whole_tool: true` (see [`Rules::whole_tool`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rules {
    pub patterns: Vec<Rule>,
    /// Whether the action applies to ANY argument (a bare `edit: deny`) or
    /// only to matching patterns. A bare verdict is `patterns == [catch-all]`
    /// with this `true`; an explicit `{"*": action}` map is the same single
    /// pattern with this `false`, so "the file said tool-wide" stays
    /// distinguishable from "the file said star". Never order-sensitive (up to
    /// the follow-up task's matcher).
    pub whole_tool: bool,
}

impl Rules {
    /// THE whole-tool constructor: a bare verdict (`edit: deny`) is the catch-all
    /// pattern carrying that action, flagged tool-wide. This is the ONLY shape
    /// that ever carries `whole_tool: true`, and the only action it can carry is
    /// an [`Action`] — so no constructor here can express a grant beyond the
    /// user's policy.
    pub fn whole_tool(action: Action) -> Self {
        Self {
            patterns: vec![Rule {
                pattern: CATCH_ALL_PATTERN.to_string(),
                action,
            }],
            whole_tool: true,
        }
    }

    /// The per-tool constructor: an ordered list of pattern rows, NOT
    /// tool-wide.
    pub fn per_tool(patterns: Vec<Rule>) -> Self {
        Self {
            patterns,
            whole_tool: false,
        }
    }

    /// The tool-wide verdict, if this ruleset IS a whole-tool catch-all
    /// (`whole_tool` + exactly the one `"*"` pattern). `None` for a per-pattern
    /// ruleset — including one that spells `{"*": action}` explicitly.
    pub fn catch_all_action(&self) -> Option<Action> {
        if self.whole_tool
            && self.patterns.len() == 1
            && self.patterns[0].pattern == CATCH_ALL_PATTERN
        {
            Some(self.patterns[0].action)
        } else {
            None
        }
    }

    /// Whether this tool is denied outright (the whole-tool `deny` the
    /// advertisement-narrowing seam can act on WITHOUT any pattern matcher).
    pub fn is_deny(&self) -> bool {
        self.catch_all_action() == Some(Action::Deny)
    }
}

/// The per-tool discriminant. The frontmatter KEY is the identity: eleven
/// RECOGNISED keys + [`ToolKey::Unrecognized`] which RETAINS any other tool
/// name verbatim (opencode has keys this desktop has no policed twin for —
/// `glob`, `grep`, `list`, `task`, `skill`, `lsp`; an unrecognised key is
/// announced via [`FrontmatterNote::UnrecognizedToolKey`], NEVER dropped).
/// The two announcement spellings name the two CLASSES of unenforced pattern
/// object: [`UnsupportedReason::BashPatternRules`] for `bash` command text and
/// [`UnsupportedReason::PathPatternRules`] for the ten recognised path-taking
/// keys. An `Unrecognized` key gets NEITHER: its path semantics are unknown to
/// this desktop, so a per-pattern object under `glob:` / `task:` / `lsp:` is
/// announced only as an [`FrontmatterNote::UnrecognizedToolKey`] — claiming
/// "path patterns unenforced" about a key whose semantics are unknown would
/// announce a falsehood.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ToolKey {
    Edit,
    Read,
    Bash,
    Write,
    Find,
    Grep,
    Ls,
    ReadSkill,
    /// This is the `permission:`-block `ask:` KEY — a verdict-bearing tool key,
    /// NOT the `ask` TOOL (the interactive prompt). Different spell.
    Ask,
    WebSearch,
    FetchContent,
    /// Any other tool name, kept verbatim so the ruleset stays REPRESENTABLE.
    Unrecognized(String),
}

impl ToolKey {
    /// The frontmatter key → variant fan-out. A `match`, not a map — the only
    /// look-up this layer does, so the ordered-`Vec` discipline above never
    /// meets a map-shaped temptation. Keys are matched EXACTLY (opencode spells
    /// them lowercase); anything else is retained verbatim as
    /// [`ToolKey::Unrecognized`] rather than guessed at or dropped.
    pub fn from_frontmatter_key(key: &str) -> Self {
        match key {
            "edit" => ToolKey::Edit,
            "read" => ToolKey::Read,
            "bash" => ToolKey::Bash,
            "write" => ToolKey::Write,
            "find" => ToolKey::Find,
            "grep" => ToolKey::Grep,
            "ls" => ToolKey::Ls,
            "read_skill" => ToolKey::ReadSkill,
            "ask" => ToolKey::Ask,
            "web_search" => ToolKey::WebSearch,
            "fetch_content" => ToolKey::FetchContent,
            other => ToolKey::Unrecognized(other.to_string()),
        }
    }
}

/// WHY a block could not be used. Produced for a per-pattern pattern OBJECT on
/// a RECOGNISED tool — `bash`-style per-command patterns (reason
/// [`UnsupportedReason::BashPatternRules`]) and path-taking-tool path patterns
/// (reason [`UnsupportedReason::PathPatternRules`]). The scope line: parse +
/// announce as unsupported — never enforce with a matcher nothing has
/// validated. A [`ToolKey::Unrecognized`] key gets NO reason: its path semantics
/// are unknown, so naming either class about it would be a falsehood.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum UnsupportedReason {
    /// opencode `bash`-style patterns over command text (incl. its redirects)
    /// are not a validated matcher in this desktop: a `grep x file 2>/dev/null`
    /// contains a `>`, and no rule language here has ever separated "redirect
    /// to a file" from "redirect to an fd". Whole-tool `bash: deny` (a bare
    /// [`Action`]) has NO patterns and IS supported — only a bash object with
    /// ≥1 pattern row is unsupported.
    BashPatternRules,
    /// A PATH-PATTERN map on a recognised path-taking tool (`edit:` / `read:` /
    /// `write:` / `find:` / `grep:` / `ls:` / `read_skill:` / `ask:` /
    /// `web_search:` / `fetch_content:`): parsed, announced, and NOT enforced.
    /// The gate's `definition_verdict` reads only [`Rules::catch_all_action`]
    /// (whole-tool) and answers `AsSelf` for a per-pattern ruleset (gate.rs),
    /// and the definition layer deliberately does NO path resolution (ADR 0081 /
    /// ADR 0029 one-canonicalizer rule; ADR 0084).
    PathPatternRules,
}

/// The FULL ordered aggregate: every tool the block speaks about, in FILE
/// ORDER. This is the type `AgentDefinition.permission` carries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionBlock {
    /// A multi-tool block (`edit:` + `bash:` + `read:` in one file) is exactly
    /// three ordered elements. Never a HashMap: file order is faithful to how
    /// opencode applies per-tool maps (independently, in file order) and gives
    /// deterministic serialization and test assertions. Unrecognised keys are
    /// PRESERVED as [`ToolKey::Unrecognized`] — the silent-drop class #101
    /// exists to fix has no path in this type.
    pub tools: Vec<(ToolKey, Rules)>,
}

/// One "this file said something the reader does not understand" note — the
/// ANNOUNCE half (issue #101 / #78). A REPORT, never an error: discovery stays
/// total.
///
/// EXTERNALLY tagged (`{"unknownKey": "mode"}`), NOT `#[serde(tag = "kind")]`:
/// every payload here is a bare `String`/unit, and serde's internally-tagged
/// representation can only tag variants whose payload serializes as a MAP —
/// `tag = "kind"` would compile and then fail at RUNTIME the first time a note
/// crossed the wire, which is the silent-no-op class this issue exists to fix.
/// `frontmatter_notes_serialize_as_a_tagged_object_per_note` pins the shape
/// the follow-up (wire) task will ride.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FrontmatterNote {
    /// A top-level frontmatter key the desktop does not read (`mode:`,
    /// `temperature:`, …). `UnknownKey("permission")` is pushed ONLY when a
    /// `permission:` line yielded nothing readable at all — the key itself is
    /// known, so an ordinary `permission:` block never produces it.
    UnknownKey(String),
    /// A recognised-but-unpoliced tool name, kept verbatim (`glob`, `task`,
    /// `lsp`, `subagent`, …).
    UnrecognizedToolKey(String),
    /// A per-pattern pattern OBJECT on a recognised tool: parsed
    /// (`Some(block)`) but announced unsupported — never enforced by a matcher
    /// nothing validated. The payload names WHICH class it is: bash command-text
    /// rows announce [`UnsupportedReason::BashPatternRules`], a path-taking
    /// tool's path-pattern rows announce
    /// [`UnsupportedReason::PathPatternRules`].
    UnsupportedUnvalidatedMatcher(UnsupportedReason),
    /// An action/verdict the parser does not recognise (`allow`/`ask`/`deny`
    /// are the only accepted spellings). The row is dropped and the note
    /// carries the unknown spelling verbatim; parser tolerance, never a crash.
    UnknownAction(String),
}

/// What the block reader returns: the block (`Some` when the file said
/// anything usable) and the notes it ANNOUNCES along the way. A malformed
/// block is `None` + a note — never a skip, never an error, never a panic.
pub type PermissionOut = (Option<PermissionBlock>, Vec<FrontmatterNote>);

/// Read the nested `permission:` block that starts at `fm[idx]` (the
/// `permission:` line itself; `rest` is the text after that prefix).
///
/// Tolerant and total: any shape it cannot read is announced, never a panic
/// and never an error (`agents.rs`'s discovery contract).
///
/// ## The three rules this reader owns (and the parity that governs them)
///
/// * **Blank lines are SKIPPED, never terminators.** The block runs from the
///   line after `permission:` until the first NON-INDENTED, NON-BLANK line (or
///   the end of the frontmatter). This is a DELIBERATE divergence from
///   `parse_value`'s block branch, which BREAKS on an empty line (a blank line
///   matches neither the indent branch nor the break branch, so the loop just
///   stops and a mid-block blank TRUNCATES the value). Copying that here would
///   truncate a legal, readable permission block mid-ruleset — the silent-loss
///   class, so it cannot be copied.
///   `a_blank_line_between_tool_groups_is_skipped_not_a_terminator` pins it.
/// * **Comment lines are SKIPPED, never rules and never announcements.** A line
///   whose first non-whitespace character is `#` is YAML prose and carries no
///   rule, so it is dropped BEFORE any key/value split, in the collector, in the
///   tool-level loop and in the nested pattern-row loop; and like a blank line it
///   terminates neither the block nor a nested map (a comment at column 0 cannot
///   even move the dedent baseline). This is not cosmetic: the real file that
///   motivated #101 (`~/.agents/agents/researcher.md`) puts SEVEN comments inside
///   its block, one of them containing a colon (`# Known gap: a file redirect
///   that …`), which the colon splitter would otherwise read as
///   `ToolKey::Unrecognized("# Known gap")` — pushed into `tools` as a rule —
///   while the rest of the sentence would be announced as an `UnknownKey`.
///   Announcing prose on every discovery is noise that trains people to ignore
///   the announcements, which defeats the feature.
///   `a_comment_line_is_skipped_neither_announced_nor_a_terminator` and
///   `the_real_researcher_block_shape_parses_with_no_comment_noise` pin it, and
///   `a_block_of_only_comments_announces_instead_of_vanishing` pins that skipping
///   comments does NOT turn an unreadable block back into a silent no-op.
/// * **TRAILING `#` comments are kept VERBATIM — only a line that STARTS with
///   `#` (after indentation) is a comment.** The reader owns this rule and it
///   chose NOT to strip: stripping would let a comment decide whether a verdict
///   exists (`"rm *": deny   # destructive` → `deny`), so a typo'd or misplaced
///   comment could silently CHANGE an action's meaning, and a pattern is allowed
///   to contain a real `#` (`"build#1/*.rs"`). An unstripped `deny # …` is
///   therefore not a verdict: the row is dropped and the exact spelling is
///   announced. Note the direction of this divergence: `agents.rs`'s
///   `parse_value` also does NOT strip (its `frontmatter.ts` upstream has no
///   comment rule — see
///   `the_two_parse_value_copies_diverge_where_their_parity_targets_differ`),
///   and this reader does not read that copy at all; it makes the same choice
///   for its own reason, pinned by
///   `a_trailing_comment_on_a_rule_row_is_kept_verbatim_and_announced`.
/// * **ONE surrounding quote pair** is stripped from an exactly-quoted single
///   token (`"*.md"`, `'deny'`); anything else is kept verbatim. The BLOCK
///   READER owns this rule and it governs here; `frontmatter.ts`'s
///   `value.slice(1, -1)` precedent governs `parse_value`'s inline SCALAR, not
///   this reader. Unlike `parse_value`, this reader applies NO trailing-`#`
///   comment strip and NO `''` un-doubling and NO `: `-ambiguity drop — those
///   are the two copies' scalar rules, and
///   `a_pattern_row_keeps_verbatim_what_the_scalar_readers_would_drop` pins
///   that they do not reach here.
pub fn parse_permission_block(fm: &[&str], idx: usize, rest: &str) -> PermissionOut {
    let mut notes: Vec<FrontmatterNote> = Vec::new();

    // The block is a NESTED map. An inline value on the `permission:` line is
    // not that shape (`permission: deny` is no opencode spelling), and
    // inventing a whole-tool verdict from it would manufacture a key the
    // upstream does not have — so an inline value is announced, never guessed.
    if !rest.trim().is_empty() {
        notes.push(FrontmatterNote::UnknownKey("permission".to_string()));
        return (None, notes);
    }

    let Some(dedented) = collect_block(fm, idx) else {
        // Nothing indented under `permission:` (or nothing but blanks): the key
        // spoke but the reader has no block to read. Announce it — a `permission:`
        // that silently reads as nothing is the no-op class #101 exists to fix.
        notes.push(FrontmatterNote::UnknownKey("permission".to_string()));
        return (None, notes);
    };

    let mut tools: Vec<(ToolKey, Rules)> = Vec::new();
    let mut i = 0;
    while i < dedented.len() {
        let line = dedented[i];
        if line.trim().is_empty() {
            i += 1;
            continue;
        }
        // A COMMENT line is skipped: not a rule, not a tool, not a note. The
        // prose in a real block (`# Known gap: a file redirect that …`) carries
        // a colon, so `split_key_value` would otherwise split it into
        // `ToolKey::Unrecognized("# Known gap")` — pushed into `tools` as if it
        // were a rule — and the rest of the sentence would be announced as an
        // `UnknownKey`. Announcing prose on every discovery is noise that trains
        // people to ignore the announcements, which defeats the feature.
        // `collect_block` already blanks these; the check is repeated here (and
        // in the nested loop below) so the rule belongs to the READER and does
        // not silently depend on one collector's placeholder choice.
        if is_comment_line(line) {
            i += 1;
            continue;
        }
        // A tool-level line sits at the block's own indent (0 after dedent).
        // Anything deeper has no key to belong to (the file opened with pattern
        // rows) — announced, never dropped on the floor.
        if line.starts_with(' ') || line.starts_with('\t') {
            notes.push(FrontmatterNote::UnknownKey(line.trim().to_string()));
            i += 1;
            continue;
        }
        let Some((raw_key, inline)) = split_key_value(line) else {
            notes.push(FrontmatterNote::UnknownKey(line.trim().to_string()));
            i += 1;
            continue;
        };
        let key = unquote_token(raw_key.trim());
        let tool = ToolKey::from_frontmatter_key(key);
        let inline = inline.trim();
        if let ToolKey::Unrecognized(kept) = &tool {
            // Retained AND announced — no code path here deletes a tool.
            notes.push(FrontmatterNote::UnrecognizedToolKey(kept.clone()));
        }

        // The rows: either the inline `{pattern → action}` map (opencode's
        // compact markdown spelling) or the nested lines under `key:`.
        let rules = if let Some(inner) = inline.strip_prefix('{').and_then(|s| s.strip_suffix('}'))
        {
            let rows = split_top_level_commas(inner);
            Rules::per_tool(rows_to_rules(&rows, &mut notes))
        } else if inline.is_empty() {
            // The nested spelling: every deeper-indented line is a row. A blank
            // line inside the nested map is SKIPPED like anywhere else in the
            // block, so it cannot truncate the ruleset either — and neither can
            // a comment (`researcher.md` puts one between every group of bash
            // rows), which is likewise neither a row nor a terminator.
            let mut rows: Vec<&str> = Vec::new();
            let mut j = i + 1;
            while j < dedented.len() {
                let l = dedented[j];
                if l.trim().is_empty() || is_comment_line(l) {
                    j += 1;
                    continue;
                }
                if !(l.starts_with(' ') || l.starts_with('\t')) {
                    break;
                }
                rows.push(l.trim());
                j += 1;
            }
            i = j - 1;
            Rules::per_tool(rows_to_rules(&rows, &mut notes))
        } else {
            // The bare-verdict spelling — the ONLY whole-tool spellable form.
            let token = unquote_token(inline);
            match parse_action(token) {
                Some(action) => Rules::whole_tool(action),
                None => {
                    notes.push(FrontmatterNote::UnknownAction(token.to_string()));
                    Rules::default()
                }
            }
        };

        // The scope line: a PATTERN OBJECT with ≥1 row parses (it must) and
        // announces itself unsupported with a reason that says WHY. bash
        // command-text patterns keep their dedicated reason; a recognised
        // PATH-TAKING tool's path patterns announce as path patterns (no
        // matcher here has any business deciding them — ADR 0084, one-
        // canonicalizer rule). A whole-tool bare verdict needs no matcher and
        // is therefore SUPPORTED (no note). An UNRECOGNISED key is not known
        // to be path-taking, so it is excluded — announcing "path patterns
        // unenforced" for `glob:` would be a falsehood.
        if !rules.whole_tool && !rules.patterns.is_empty() {
            let reason = match tool {
                ToolKey::Bash => Some(UnsupportedReason::BashPatternRules),
                ToolKey::Edit
                | ToolKey::Read
                | ToolKey::Write
                | ToolKey::Find
                | ToolKey::Grep
                | ToolKey::Ls
                | ToolKey::ReadSkill
                | ToolKey::Ask
                | ToolKey::WebSearch
                | ToolKey::FetchContent => Some(UnsupportedReason::PathPatternRules),
                ToolKey::Unrecognized(_) => None,
            };
            if let Some(reason) = reason {
                notes.push(FrontmatterNote::UnsupportedUnvalidatedMatcher(reason));
            }
        }

        // FIRST-WINS per tool key — the same rule the top-level scan runs. The
        // tool itself is never removed (its first ruleset is kept); a second
        // block for the same key is the one thing this aggregate cannot hold,
        // because two rulesets for one tool key have no meaning the follow-up
        // task's per-key matcher could resolve. Announcing it needs a note
        // variant this plan does not define — recorded as a hand-off, not a
        // silent loss of the TOOL.
        if tools.iter().any(|(seen, _)| *seen == tool) {
            i += 1;
            continue;
        }
        tools.push((tool, rules));
        i += 1;
    }

    // Nothing usable was read: `None` means "this file says nothing the layer can
    // act on", and the notes above already say WHY.
    if tools.is_empty() {
        return (None, notes);
    }
    (Some(PermissionBlock { tools }), notes)
}

/// Whether a line is a YAML **comment**: its first non-whitespace character is
/// `#`. The block reader SKIPS these — see the "comment" rule in the module
/// docs. `trim_start` (Unicode whitespace) is safe here because the result is
/// only compared, never sliced, so no byte-offset/char-boundary hazard exists.
///
/// The rule is about the LINE, not about the character: a `#` that is not the
/// first non-whitespace character is content (`"*": deny # note`, and a real
/// pattern like `"build#1/*.rs"`), so this never strips anything from a value.
fn is_comment_line(line: &str) -> bool {
    matches!(line.trim_start().chars().next(), Some('#'))
}

/// Collect the indented lines of the block that starts at `fm[idx]`, dedented.
/// `None` when the block holds no readable content line at all.
///
/// BLANK lines (empty or whitespace-only) and COMMENT lines are collected as
/// blank placeholders and are NEVER terminators: the block ends only at the
/// first line that is non-indented, non-blank AND not a comment, or at the end
/// of the frontmatter. Two consequences, both load-bearing:
///
/// * a comment (or blank) in the middle of a ruleset cannot truncate it, and
/// * a comment at column 0 — YAML lets a comment sit at ANY indentation, and
///   `researcher.md`'s neighbours in the wild do exactly that — cannot drag the
///   dedent baseline down to 0, which would turn every tool line into an
///   over-indented stray.
///
/// This is the deliberate divergence from `parse_value`'s block branch (which
/// breaks on a blank line and would truncate a readable permission block
/// mid-ruleset).
fn collect_block<'a>(fm: &[&'a str], idx: usize) -> Option<Vec<&'a str>> {
    let mut collected: Vec<&str> = Vec::new();
    let mut j = idx + 1;
    while j < fm.len() {
        let l = fm[j];
        if l.trim().is_empty() || is_comment_line(l) {
            // A placeholder, NOT a terminator and NOT part of the dedent
            // minimum: a comment carries no structure to align.
            collected.push("");
            j += 1;
            continue;
        }
        if l.starts_with(' ') || l.starts_with('\t') {
            collected.push(l);
            j += 1;
        } else {
            break;
        }
    }
    // Dedent: the block's own indent is not part of any value. ASCII spaces and
    // tabs ONLY — the same byte-offset guard both `parse_value` copies carry
    // (`trim_start` would also trim a non-ASCII space and land the `&l[min..]`
    // slice mid-character, and this reader must stay panic-free). A blank line
    // takes the blank placeholder instead, so it can never drag the minimum down.
    let min = collected
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start_matches([' ', '\t']).len())
        .min()?;
    Some(
        collected
            .iter()
            .map(|l| if l.trim().is_empty() { "" } else { &l[min..] })
            .collect(),
    )
}

/// Turn row texts (the inline map's rows, or the nested lines) into ordered
/// `Rule`s. An unreadable row is announced, never skipped silently; an
/// unreadable verdict drops the ROW (a rule with no verdict cannot decide) and
/// announces the spelling verbatim.
fn rows_to_rules(rows: &[&str], notes: &mut Vec<FrontmatterNote>) -> Vec<Rule> {
    let mut out = Vec::new();
    for row in rows {
        let row = row.trim();
        match split_key_value(row) {
            Some((raw_key, raw_action)) => {
                let pattern = unquote_token(raw_key.trim()).to_string();
                let spelled = unquote_token(raw_action.trim());
                match parse_action(spelled) {
                    Some(action) => out.push(Rule { pattern, action }),
                    None => notes.push(FrontmatterNote::UnknownAction(spelled.to_string())),
                }
            }
            None => notes.push(FrontmatterNote::UnknownKey(row.to_string())),
        }
    }
    out
}

/// Split `key: value` at the first `:` that sits OUTSIDE quotes — so a quoted
/// pattern like `"grep: *"` keeps its colon. `None` when the line carries no
/// separator at all (a list item, a stray word).
fn split_key_value(line: &str) -> Option<(&str, &str)> {
    let bytes = line.as_bytes();
    let mut quote: Option<u8> = None;
    for (i, b) in bytes.iter().enumerate() {
        match quote {
            Some(q) => {
                if *b == q {
                    quote = None;
                }
            }
            None => match b {
                b'"' | b'\'' => quote = Some(*b),
                b':' => return Some((&line[..i], &line[i + 1..])),
                _ => {}
            },
        }
    }
    None
}

/// Split a `{a: b, c: d}` body on commas that sit OUTSIDE quotes (a pattern may
/// legitimately contain a comma).
fn split_top_level_commas(inner: &str) -> Vec<&str> {
    let mut rows = Vec::new();
    let mut quote: Option<char> = None;
    let mut start = 0;
    for (i, ch) in inner.char_indices() {
        match quote {
            Some(q) => {
                if ch == q {
                    quote = None;
                }
            }
            None => match ch {
                '"' | '\'' => quote = Some(ch),
                ',' => {
                    rows.push(&inner[start..i]);
                    start = i + ch.len_utf8();
                }
                _ => {}
            },
        }
    }
    rows.push(&inner[start..]);
    rows
}

/// Strip ONE surrounding quote pair from an exactly-quoted single token
/// (`"*.md"`, `'deny'`); anything else is kept verbatim. Quotes only at the
/// token's own start and end count, so a leading `'` inside a pattern is content.
///
/// This is the BLOCK READER's rule. `parse_value`'s one-pair slice is the SCALAR
/// reader's rule (mirroring `frontmatter.ts`'s `value.slice(1, -1)`) and does not
/// govern here; and unlike either scalar copy this strips NO trailing `#`
/// comment, does NO `''` un-doubling and applies NO `: `-ambiguity drop —
/// `a_pattern_row_keeps_verbatim_what_the_scalar_readers_would_drop` pins that.
fn unquote_token(token: &str) -> &str {
    let bytes = token.as_bytes();
    if bytes.len() >= 2
        && (bytes[0] == b'"' || bytes[0] == b'\'')
        && bytes[bytes.len() - 1] == bytes[0]
    {
        // Both boundary bytes are the same ASCII quote, so the slice is always
        // on a char boundary.
        return &token[1..token.len() - 1];
    }
    token
}

/// The three verdict verbs, and nothing else. Case-tolerant (`Deny` reads); an
/// unknown spelling is announced verbatim by the caller, never coerced.
fn parse_action(token: &str) -> Option<Action> {
    match token.to_ascii_lowercase().as_str() {
        "allow" => Some(Action::Allow),
        "ask" => Some(Action::Ask),
        "deny" => Some(Action::Deny),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run the reader over a frontmatter slice (the text BETWEEN the `---`
    /// fences), locating the `permission:` line the way `agents.rs`'s step-3
    /// scan does.
    fn parse_block(fm_text: &str) -> PermissionOut {
        let lines: Vec<&str> = fm_text.split('\n').collect();
        let idx = lines
            .iter()
            .position(|l| l.starts_with("permission:"))
            .expect("the fixture has a `permission:` line");
        let rest = lines[idx].strip_prefix("permission:").unwrap();
        parse_permission_block(&lines, idx, rest)
    }

    /// The block half of a fixture, panicking with the notes on `None` so a
    /// reader that drops the block fails LOUDLY instead of vacuously.
    /// THE ZERO-NOTE CASE of [`block_announcing`]: a fixture that must announce
    /// NOTHING (a whole-tool bare verdict, an unreadable row, an `Unrecognized`
    /// key's per-pattern map).
    fn block(fm_text: &str) -> PermissionBlock {
        let (block, notes) = parse_block(fm_text);
        assert!(
            notes.is_empty(),
            "this fixture announces nothing: {notes:?}"
        );
        block.expect("the fixture must parse to Some(block)")
    }

    /// The block half of a fixture that announces a KNOWN set of notes. The
    /// notes are asserted EXACTLY — the announce contract is part of the
    /// fixture's meaning, never a by-product — before the block is unwrapped.
    /// #124: a per-pattern pattern OBJECT on a recognised tool announces
    /// itself unsupported, so every such fixture names its note here instead of
    /// riding `block()`'s unconditional empty-note assert.
    fn block_announcing(fm_text: &str, expected: Vec<FrontmatterNote>) -> PermissionBlock {
        let (block, notes) = parse_block(fm_text);
        assert_eq!(notes, expected, "the fixture announces EXACTLY this");
        block.expect("the fixture must parse to Some(block)")
    }

    /// The one note a per-pattern pattern object on a recognised PATH-TAKING
    /// tool must announce (#124: parsed, announced, NOT enforced).
    fn path_note() -> FrontmatterNote {
        FrontmatterNote::UnsupportedUnvalidatedMatcher(UnsupportedReason::PathPatternRules)
    }

    fn rule(pattern: &str, action: Action) -> Rule {
        Rule {
            pattern: pattern.to_string(),
            action,
        }
    }

    /// T1 — the wire spelling the follow-up (carry + enforce) task rides.
    #[test]
    fn permission_action_verbs_are_lowercase_on_the_wire() {
        assert_eq!(serde_json::to_string(&Action::Deny).unwrap(), "\"deny\"");
        assert_eq!(serde_json::to_string(&Action::Allow).unwrap(), "\"allow\"");
        assert_eq!(serde_json::to_string(&Action::Ask).unwrap(), "\"ask\"");
    }

    /// T2 — the whole-tool shape is the catch-all, and it is the ONLY shape
    /// `whole_tool` ever takes (whole-tool `bash: deny` is SUPPORTED precisely
    /// because it needs no matcher).
    #[test]
    fn a_bare_action_is_the_whole_tool_catch_all() {
        let rules = Rules::whole_tool(Action::Deny);
        assert_eq!(
            rules.patterns,
            vec![rule(CATCH_ALL_PATTERN, Action::Deny)],
            "a bare verdict is stored as the catch-all row"
        );
        assert!(rules.whole_tool, "and flagged tool-wide");
        assert_eq!(rules.catch_all_action(), Some(Action::Deny));
        assert!(rules.is_deny());
        // An explicit `{"*": deny}` says the same thing but does NOT say
        // "tool-wide", and the type keeps them apart.
        let spelled = Rules::per_tool(vec![rule(CATCH_ALL_PATTERN, Action::Deny)]);
        assert!(
            !spelled.whole_tool,
            "the explicit star spelling is NOT tool-wide"
        );
        assert_eq!(
            spelled.catch_all_action(),
            None,
            "so it has no tool-wide verdict to report"
        );
        assert!(
            !spelled.is_deny(),
            "and the advertisement seam must not read it as an outright deny"
        );
        // `Allow` is the widest thing this module can say, and even it is only
        // ever a catch-all — no shape here can grant beyond the user's policy.
        assert_eq!(
            Rules::whole_tool(Action::Allow).catch_all_action(),
            Some(Action::Allow)
        );
    }

    /// T3 — the last-matching-rule-wins contract is an ORDERED list, typed as
    /// one. A set/sorted/HashMap representation fails this.
    ///
    /// #124: this `edit:` object is a per-pattern PATH map, so it announces
    /// itself unsupported — the ORDERED-row contract below is unchanged by that.
    #[test]
    fn last_matching_rule_wins_parse_and_type() {
        let b = block_announcing(
            "name: t3\npermission:\n  edit: {\"*\": deny, \"*.md\": allow, \"src/lib/*.ts\": deny}\n",
            vec![path_note()],
        );
        assert_eq!(b.tools.len(), 1, "{b:?}");
        let (key, rules) = &b.tools[0];
        assert_eq!(key, &ToolKey::Edit);
        assert_eq!(
            rules.patterns,
            vec![
                rule("*", Action::Deny),
                rule("*.md", Action::Allow),
                rule("src/lib/*.ts", Action::Deny),
            ],
            "FILE ORDER, duplicates of a verdict allowed, nothing sorted or deduped"
        );
        assert_eq!(
            rules.patterns.last().unwrap().action,
            Action::Deny,
            "the LAST matching rule is the one at the END of the list"
        );
        assert!(
            !rules.whole_tool,
            "an object spelling is per-pattern even when the catch-all is in it"
        );
    }

    /// T4 — pattern rows are typed for the path-taking tools (and the two
    /// no-path tools are the SAME arm: the reader parses, the matcher decides).
    ///
    /// #124: ALL TEN keys here are recognised path-taking tools, so every
    /// iteration announces its own `PathPatternRules` — parsed, announced, not
    /// enforced. The note is asserted per key (exactly one) so a silent arm
    /// cannot hide behind the loop.
    #[test]
    fn pattern_matching_applies_to_path_taking_tools_only() {
        for (key, variant) in [
            ("edit", ToolKey::Edit),
            ("read", ToolKey::Read),
            ("write", ToolKey::Write),
            ("find", ToolKey::Find),
            ("grep", ToolKey::Grep),
            ("ls", ToolKey::Ls),
            ("read_skill", ToolKey::ReadSkill),
            ("ask", ToolKey::Ask),
            ("web_search", ToolKey::WebSearch),
            ("fetch_content", ToolKey::FetchContent),
        ] {
            let b = block_announcing(
                &format!(
                    "name: t4\npermission:\n  {key}: {{\"*.rs\": deny, \"docs/**\": allow}}\n"
                ),
                vec![path_note()],
            );
            assert_eq!(b.tools.len(), 1, "{key}: {b:?}");
            let (got, rules) = &b.tools[0];
            assert_eq!(got, &variant, "`{key}` maps to its own variant");
            assert_eq!(
                rules.patterns,
                vec![rule("*.rs", Action::Deny), rule("docs/**", Action::Allow)],
                "`{key}` carries typed pattern rows"
            );
            assert!(!rules.whole_tool, "`{key}` was spelled as an object");
        }
    }

    /// T5 — THE silent-drop regression: a real file declares THREE tools in one
    /// `permission:` block. A shape that holds one tool per block fails here.
    ///
    /// #124: the `edit:` object is a per-pattern path map (one note); the
    /// whole-tool `bash: deny` / `read: ask` verdicts stay SILENT — a bare
    /// verdict needs no matcher and is therefore supported.
    #[test]
    fn a_multitool_block_is_fully_representable_and_preserved_order() {
        let b = block_announcing(
            "name: t5\npermission:\n  edit: {\"*\": deny, \"*.md\": allow}\n  bash: deny\n  read: ask\n",
            vec![path_note()],
        );
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![&ToolKey::Edit, &ToolKey::Bash, &ToolKey::Read],
            "three tools, IN FILE ORDER — nothing dropped, nothing sorted"
        );
        assert_eq!(b.tools[0].1.patterns.len(), 2, "`edit` kept both rows");
        assert_eq!(b.tools[1].1, Rules::whole_tool(Action::Deny));
        assert!(b.tools[1].1.is_deny(), "whole-tool bash deny is supported");
        assert_eq!(b.tools[2].1, Rules::whole_tool(Action::Ask));
    }

    /// T6 — opencode keys with no archimedes twin are RETAINED (rows parsed)
    /// and ANNOUNCED. No code path deletes a tool from a block.
    #[test]
    fn an_unrecognised_tool_key_is_retained_and_announced_not_dropped() {
        let fm = "name: t6\npermission:\n  glob: {\"*.json\": deny}\n  task: deny\n  lsp: {\"*\": ask}\n";
        let (b, notes) = parse_block(fm);
        let b = b.expect("the block is representable even though its keys are unknown");
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![
                &ToolKey::Unrecognized("glob".to_string()),
                &ToolKey::Unrecognized("task".to_string()),
                &ToolKey::Unrecognized("lsp".to_string()),
            ],
            "every unknown key is kept, verbatim, in file order"
        );
        assert_eq!(
            b.tools[0].1.patterns,
            vec![rule("*.json", Action::Deny)],
            "and its rows are still parsed into `Rule`s"
        );
        assert_eq!(b.tools[1].1, Rules::whole_tool(Action::Deny));
        assert_eq!(
            notes,
            vec![
                FrontmatterNote::UnrecognizedToolKey("glob".to_string()),
                FrontmatterNote::UnrecognizedToolKey("task".to_string()),
                FrontmatterNote::UnrecognizedToolKey("lsp".to_string()),
            ],
            "one note per key, in file order"
        );
    }

    /// The blank-line rule, at the reader level (the discovery-level twin is
    /// `agents.rs`'s `a_permission_block_with_a_blank_line_between_tool_groups_parses_in_full`).
    /// A `parse_value`-style break-on-blank loop fails this.
    ///
    /// #124: the `edit:` object announces its path patterns (one note); the
    /// whole-tool `bash:` / `read:` verdicts stay silent.
    #[test]
    fn a_blank_line_between_tool_groups_is_skipped_not_a_terminator() {
        let b = block_announcing(
            "name: blank\npermission:\n  edit: {\"*\": deny, \"*.md\": allow}\n\n  bash: deny\n   \n  read: ask\n",
            vec![path_note()],
        );
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![&ToolKey::Edit, &ToolKey::Bash, &ToolKey::Read],
            "a blank (or whitespace-only) line inside the block must not truncate the ruleset"
        );
    }

    /// A blank line INSIDE a nested pattern map is skipped too — the rule is
    /// about the block, not about one nesting level.
    ///
    /// #124 — THE PINNED SILENCE, now broken on purpose. This fixture used to
    /// ride `block()`'s unconditional "announces nothing" assert, which is
    /// exactly the silence issue #124 exists to end: a nested `edit:` path map
    /// parsed, rendered as rules in the Agents modal, refused nothing, and never
    /// said so. It now announces ONE `PathPatternRules` note, and the blank-line
    /// contract below is unchanged.
    #[test]
    fn a_blank_line_inside_a_nested_pattern_map_is_skipped_too() {
        let b = block_announcing(
            "name: nested\npermission:\n  edit:\n    \"*\": deny\n\n    \"*.md\": allow\n  bash: deny\n",
            vec![path_note()],
        );
        assert_eq!(b.tools.len(), 2, "{b:?}");
        assert_eq!(
            b.tools[0].1.patterns,
            vec![rule("*", Action::Deny), rule("*.md", Action::Allow)],
            "the rows after the blank still belong to `edit`"
        );
    }

    /// C1 — THE test that would have caught the comment defect: the REAL block
    /// shape of `~/.agents/agents/researcher.md` (the file issue #101's Evidence
    /// section names). Seven comment lines sit inside the block, between tool
    /// groups AND between pattern rows, and one of them carries a colon
    /// (`# Known gap: …`) — which `split_key_value` happily splits, so a reader
    /// without a comment rule turns prose into `ToolKey::Unrecognized("# Known
    /// gap")` and `UnknownKey("<entire sentence>")`.
    ///
    /// The ONLY notes this block may announce are the THREE its CONTENT carries:
    /// the `edit:` nested map announces its unenforced path patterns (#124),
    /// `task` has no policed twin, and a `bash` pattern object is unsupported.
    /// The expected-note list below is written OUT LONG so a comment-derived
    /// note cannot sneak past a `notes.is_empty()` shortcut.
    #[test]
    fn the_real_researcher_block_shape_parses_with_no_comment_noise() {
        // Verbatim in shape (and in its comments) from
        // `~/.agents/agents/researcher.md`: `edit:` nested map, `task: deny`,
        // a `bash:` pattern object, comments between the groups, a comment with
        // a colon, comments between pattern rows, and an em-dash inside one.
        let fm = r#"name: researcher
mode: subagent
permission:
  # Markdown only. In opencode `edit` gates write / edit / patch, patterns match the
  # worktree-relative path, and the LAST matching rule wins — so the catch-all goes first.
  edit:
    "*": deny
    "*.md": allow
    "*.mdx": allow
  task: deny
  # bash is a write hole (`echo x > src/app.ts`, `sed -i`, `tee`). Deny the mutating
  # surface, then re-allow the fd-only redirects researchers lean on (`2>/dev/null`,
  # `2>&1`, `> /dev/null`) — those rules are last because they win by order.
  # Known gap: a file redirect that also carries an fd suffix (`cat x > f.ts 2>/dev/null`)
  # re-matches an allow rule. The prompt contract below is the backstop for that.
  bash:
    # The catch-all first, so the specifics after it win by order.
    "*": allow
    "rm *": deny
    # Redirections are the write hole worth naming.
    ">*": deny
    "2>/dev/null": allow
subtask: "true"
"#;
        let (b, notes) = parse_block(fm);
        let b = b.expect("the file that motivated #101 must parse");

        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![
                &ToolKey::Edit,
                &ToolKey::Unrecognized("task".to_string()),
                &ToolKey::Bash,
            ],
            "ONLY the real tool keys — no `# …` entry, nothing dropped, \
             nothing sorted: {keys:?}"
        );
        assert_eq!(
            b.tools[0].1.patterns,
            vec![
                rule("*", Action::Deny),
                rule("*.md", Action::Allow),
                rule("*.mdx", Action::Allow),
            ],
            "`edit` kept its nested map"
        );
        assert_eq!(
            b.tools[1].1,
            Rules::whole_tool(Action::Deny),
            "`task: deny`"
        );
        assert_eq!(
            b.tools[2].1.patterns,
            vec![
                rule("*", Action::Allow),
                rule("rm *", Action::Deny),
                rule(">*", Action::Deny),
                rule("2>/dev/null", Action::Allow),
            ],
            "the `bash` rows survive the comment BETWEEN pattern rows"
        );

        assert_eq!(
            notes,
            vec![
                // `edit:` is read FIRST (file order), and its nested map is a
                // per-pattern PATH map — #124's announcement, before `task` and
                // `bash` are even reached.
                FrontmatterNote::UnsupportedUnvalidatedMatcher(UnsupportedReason::PathPatternRules),
                FrontmatterNote::UnrecognizedToolKey("task".to_string()),
                FrontmatterNote::UnsupportedUnvalidatedMatcher(UnsupportedReason::BashPatternRules),
            ],
            "the ONLY three notes are the ones the block's CONTENT earns: no \
             comment sentence is announced as an unknown key or a tool"
        );
        for note in &notes {
            let rendered = format!("{note:?}");
            assert!(
                !rendered.contains('#'),
                "a comment leaked into the announcements: {rendered}"
            );
        }
    }

    /// C2 — the comment rule itself, in every position a comment can legally
    /// sit in: between tool groups, between pattern rows, before the first
    /// tool, after the last one, at column 0 (YAML lets a comment sit at ANY
    /// indentation), with a colon, with a quote, and as a whitespace-only /
    /// lone-`#` line. A comment is SKIPPED: never a rule, never a note, never
    /// a terminator.
    #[test]
    fn a_comment_line_is_skipped_neither_announced_nor_a_terminator() {
        let fm = "name: c\npermission:\n  \
                  # before the first tool, and it has a: colon inside\n  \
                  edit: deny\n  \
                  # between the groups\n  \
                  read:\n    \
                    # between pattern rows, with a \"quote\" and a colon: here\n    \
                    \"*\": ask\n    \
                    \"secrets/**\": deny\n  \
                  # a lone hash:\n  #\n   \n  \
                  bash: deny\n  \
                  # after the last tool: trailing prose\n";
        // #124: the nested `read:` map is a per-pattern PATH map → exactly one
        // `PathPatternRules` note. `edit: deny` and `bash: deny` are whole-tool
        // verdicts (silent), and NO comment anywhere in this fixture announces
        // itself — the comment contract below is unchanged.
        let b = block_announcing(fm, vec![path_note()]);
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![&ToolKey::Edit, &ToolKey::Read, &ToolKey::Bash],
            "no comment became a tool, and none truncated the block: {keys:?}"
        );
        assert_eq!(
            b.tools[1].1.patterns,
            vec![rule("*", Action::Ask), rule("secrets/**", Action::Deny)],
            "the rows AFTER the comment still belong to `read` — a comment \
             cannot terminate a nested block any more than a blank line can"
        );
        assert!(b.tools[2].1.is_deny(), "the last tool survived the block");
    }

    /// C3 — a comment at column 0 (no indent at all) inside the block does not
    /// end it either, and — because it is skipped — cannot drag the block's
    /// dedent baseline down to 0 and turn every tool line into an
    /// over-indented stray.
    #[test]
    fn a_comment_at_column_zero_does_not_end_the_block_or_move_its_dedent() {
        let fm =
            "name: c0\npermission:\n  edit: deny\n# a comment at column 0: colon\n  bash: deny\n";
        let b = block(fm);
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![&ToolKey::Edit, &ToolKey::Bash],
            "the block runs past the column-0 comment: {keys:?}"
        );
    }

    /// C4 — the boundary of C1–C3: a `permission:` whose block holds NOTHING
    /// but comments still has no readable content, so it announces — the
    /// "confident no-op" class #101 exists to fix, which a comment-skipping
    /// reader could otherwise reintroduce (skip everything, say nothing).
    #[test]
    fn a_block_of_only_comments_announces_instead_of_vanishing() {
        for fm in [
            "name: o1\npermission:\n  # nothing but prose: with a colon\n",
            "name: o2\npermission:\n  # one\n\n  # two\n",
        ] {
            let (b, notes) = parse_block(fm);
            assert!(b.is_none(), "comments are not a block: {b:?}");
            assert_eq!(
                notes,
                vec![FrontmatterNote::UnknownKey("permission".to_string())],
                "announced exactly once, and NOT as the comment's text"
            );
        }
    }

    /// C5 — the TRAILING-comment rule this reader owns, pinned for all three
    /// rule spellings. The reader keeps the value VERBATIM (see the module
    /// docs): `deny # destructive` is not a verdict, so it is ANNOUNCED and the
    /// row is dropped, rather than the reader guessing which `#` the author
    /// meant. Stripping here would be the second half of a rule that the block
    /// reader explicitly refuses to import from `agents.rs`'s `parse_value`
    /// (which does NOT strip — the divergence pinned by
    /// `the_two_parse_value_copies_diverge_where_their_parity_targets_differ`).
    #[test]
    fn a_trailing_comment_on_a_rule_row_is_kept_verbatim_and_announced() {
        let fm = "name: tc\npermission:\n  \
                  edit: deny   # destructive\n  \
                  read:\n    \"*\": ask # nested row trailing\n  \
                  write: {\"*.tmp\": deny # inline row trailing}\n";
        let (b, notes) = parse_block(fm);
        let b = b.expect("the tools are retained even where the verdict was unreadable");
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![&ToolKey::Edit, &ToolKey::Read, &ToolKey::Write],
            "every tool keeps its slot"
        );
        assert_eq!(
            b.tools[0].1,
            Rules::default(),
            "`deny   # destructive` is NOT stripped into a whole-tool deny — \
             stripping would let a comment decide whether a tool-wide verdict \
             exists at all"
        );
        assert_eq!(
            b.tools[1].1,
            Rules::default(),
            "same for a nested pattern row"
        );
        assert_eq!(b.tools[2].1, Rules::default(), "same for an inline-map row");
        assert_eq!(
            notes,
            vec![
                FrontmatterNote::UnknownAction("deny   # destructive".to_string()),
                FrontmatterNote::UnknownAction("ask # nested row trailing".to_string()),
                FrontmatterNote::UnknownAction("deny # inline row trailing".to_string()),
            ],
            "each unknown verdict is announced VERBATIM, comment and all — \
             the user sees the exact spelling the reader refused to guess at"
        );

        // And the flip side, which the comment rule MUST NOT break: a `#` that
        // is not the first non-whitespace character of the LINE is content. A
        // pattern naming a real file with a `#` in it is a rule, not a comment.
        //
        // #124: this flip-side `edit:` map has ONE readable pattern row, so it is
        // a per-pattern path map and announces itself unsupported — the note is
        // asserted rather than absorbed by `block()`'s empty-note assert. The
        // MAIN half above stays silent-by-shape (every row unreadable → no
        // pattern rows → nothing to announce) and keeps its three-`UnknownAction`
        // vec untouched: an unreadable row announces the verdict, not the class.
        let b = block_announcing(
            "name: hash\npermission:\n  edit:\n    \"build#1/*.rs\": deny\n",
            vec![path_note()],
        );
        assert_eq!(
            b.tools[0].1.patterns,
            vec![rule("build#1/*.rs", Action::Deny)],
            "only a line whose FIRST non-whitespace character is `#` is a comment"
        );
    }

    /// The quote rule this reader owns: ONE surrounding pair off an
    /// exactly-quoted token, nothing else — no un-doubling, no partial strip.
    ///
    /// #124: the `edit:` map is a per-pattern path map → one note; the
    /// `unquote_token` asserts below are untouched.
    #[test]
    fn an_exactly_quoted_token_loses_one_quote_pair_and_nothing_else() {
        let b = block_announcing(
            "name: q\npermission:\n  edit: {'*.md': 'deny', \"*.rs\": allow, \"a b\": deny}\n",
            vec![path_note()],
        );
        assert_eq!(
            b.tools[0].1.patterns,
            vec![
                rule("*.md", Action::Deny),
                rule("*.rs", Action::Allow),
                rule("a b", Action::Deny),
            ],
            "`'*.md'` / `'deny'` / `\"a b\"` each lose exactly ONE surrounding pair \
             (the one-pair slice the scalar reader does too); the unquoted \
             multi-token-looking verdict `allow` is a plain verb; and a pattern \
             containing a SPACE survives intact — the comma/colon splitter never \
             cuts inside quotes"
        );
        assert!(!b.tools[0].1.whole_tool);

        // And a quote that is NOT a pair is content, never a strip: only a token
        // quoted at BOTH ends loses anything.
        assert_eq!(unquote_token("'*.vue"), "'*.vue", "a leading quote only");
        assert_eq!(unquote_token("\"deny"), "\"deny", "a leading quote only");
        assert_eq!(unquote_token("*.md\""), "*.md\"", "a trailing quote only");
        assert_eq!(unquote_token("\"a'"), "\"a'", "mismatched pair");
        assert_eq!(unquote_token("\""), "\"", "a lone quote is a whole token");
        assert_eq!(unquote_token("deny"), "deny", "unquoted stays verbatim");
    }

    /// The D4 U-turn half at the value level: the two scalar copies' rules do
    /// NOT reach this reader. A trailing `#`, a doubled `''` and a `: ` inside a
    /// quoted pattern all stay verbatim here, where `skills.rs`'s copy would
    /// have stripped, un-doubled or dropped them.
    ///
    /// #124 adds a SECOND note here, AFTER the first: two of the three rows
    /// parsed into real pattern rows, so this `edit:` map is a per-pattern path
    /// map and announces itself unsupported. ORDER is load-bearing — the row
    /// reader pushes the `UnknownAction` while reading rows, and the guard runs
    /// after the rows are in hand.
    #[test]
    fn a_pattern_row_keeps_verbatim_what_the_scalar_readers_would_drop() {
        let (b, notes) = parse_block(
            "name: u\ndescription: a # note\npermission:\n  edit: {\"grep: *\": deny, 'it''s': ask, \"x\": deny # trailing}\n",
        );
        let b = b.expect("the block parses");
        assert_eq!(
            b.tools[0].1.patterns,
            vec![rule("grep: *", Action::Deny), rule("it''s", Action::Ask)],
            "the `: ` guard (which `parse_value` applies to an unquoted scalar) \
             never reaches a quoted pattern key, and the `''` un-doubling that \
             `skills.rs`'s yaml-backed copy performs does NOT happen here — \
             `frontmatter.ts` slices one pair and STOPS"
        );
        assert_eq!(
            notes,
            vec![
                FrontmatterNote::UnknownAction("deny # trailing".to_string()),
                FrontmatterNote::UnsupportedUnvalidatedMatcher(UnsupportedReason::PathPatternRules),
            ],
            "the trailing `#` is NOT stripped (that strip belongs to `skills.rs`'s \
             yaml-backed copy alone), so the token is not a verdict and is \
             ANNOUNCED verbatim instead of silently dropped — and the map the two \
             readable rows built then announces its own unenforced path patterns \
             (#124), pushed AFTER the row note because rows are read first"
        );
    }

    /// An unknown verdict is announced with the spelling verbatim; the TOOL is
    /// still retained (empty ruleset) — never dropped with the row.
    #[test]
    fn an_unknown_action_is_announced_and_the_tool_is_still_retained() {
        let fm = "name: a\npermission:\n  edit: maybe\n  read: ask\n";
        let (b, notes) = parse_block(fm);
        let b = b.expect("the block still parses — tolerance, not failure");
        let keys: Vec<&ToolKey> = b.tools.iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![&ToolKey::Edit, &ToolKey::Read],
            "`edit` keeps its slot even though its verdict was unreadable"
        );
        assert_eq!(
            b.tools[0].1,
            Rules::default(),
            "with no rows it carries the empty ruleset"
        );
        assert_eq!(
            notes,
            vec![FrontmatterNote::UnknownAction("maybe".to_string())],
            "and the unknown spelling is announced verbatim"
        );
    }

    /// A `permission:` line with nothing under it is `None` + a note — the file
    /// is still discovered (the discovery-level twin is
    /// `agents.rs`'s `malformed_permission_never_panics_the_discovery`).
    #[test]
    fn a_permission_key_with_no_block_announces_instead_of_vanishing() {
        for fm in [
            "name: e1\npermission:\n",
            "name: e2\npermission:\nname: twice\n",
            // An inline value is not the block shape at all.
            "name: e3\npermission: deny\n",
        ] {
            let (b, notes) = parse_block(fm);
            assert!(b.is_none(), "`{fm:?}` has no readable block: {b:?}");
            assert_eq!(
                notes,
                vec![FrontmatterNote::UnknownKey("permission".to_string())],
                "and the file says so — a `permission:` the reader could not \
                 read is announced, never a silent no-op"
            );
        }
    }

    /// A row that is not a `key: value` pair at all is announced, not skipped
    /// silently (the list-item spelling is the realistic case).
    #[test]
    fn a_row_the_reader_cannot_read_is_announced_not_skipped_silently() {
        let fm = "name: r\npermission:\n  edit:\n    - deny\n  bash: deny\n";
        let (b, notes) = parse_block(fm);
        let b = b.expect("the rest of the block still parses");
        assert_eq!(b.tools.len(), 2, "{b:?}");
        assert_eq!(
            b.tools[0].1,
            Rules::default(),
            "`edit` is retained with no rows"
        );
        assert_eq!(
            notes,
            vec![FrontmatterNote::UnknownKey("- deny".to_string())],
            "the unreadable row is announced verbatim"
        );
    }

    /// The notes' wire shape, pinned BEFORE the wire task lands: externally
    /// tagged, one object per note. An internally tagged (`tag = "kind"`)
    /// representation COMPILES and then fails at runtime on a `String` payload,
    /// which would be the same silent-no-op class one wave later.
    #[test]
    fn frontmatter_notes_serialize_as_a_tagged_object_per_note() {
        let notes = vec![
            FrontmatterNote::UnknownKey("mode".to_string()),
            FrontmatterNote::UnrecognizedToolKey("glob".to_string()),
            FrontmatterNote::UnsupportedUnvalidatedMatcher(UnsupportedReason::BashPatternRules),
            FrontmatterNote::UnknownAction("maybe".to_string()),
        ];
        assert_eq!(
            serde_json::to_string(&notes).unwrap(),
            "[{\"unknownKey\":\"mode\"},{\"unrecognizedToolKey\":\"glob\"},\
             {\"unsupportedUnvalidatedMatcher\":\"bashPatternRules\"},\
             {\"unknownAction\":\"maybe\"}]"
        );
    }

    /// The block's own wire shape (the ordered aggregate, tuples and all) —
    /// the follow-up task carries THIS, not a re-sorted map.
    ///
    /// Note the mixed casing, and that it is the plan's shape rather than an
    /// accident: `ToolKey` / `UnsupportedReason` / `FrontmatterNote` carry
    /// `rename_all = "camelCase"`, `Action` carries `lowercase`, and `Rule` /
    /// `Rules` / `PermissionBlock` carry NO rename — so `whole_tool` is
    /// snake_case even though the parent `AgentDefinition` is camelCase (a
    /// struct-level `rename_all` never reaches a nested type). Nothing from this
    /// module crosses the wire in this task, so the spelling is pinned here and
    /// handed to the wire task: if the modal wants `wholeTool`, `Rules` gains
    /// `rename_all = "camelCase"` THERE, in the change that also renders it —
    /// not silently here.
    #[test]
    fn a_permission_block_serializes_in_file_order() {
        let (b, notes) = parse_block("name: s\npermission:\n  edit: deny\n  glob: deny\n");
        assert_eq!(
            notes,
            vec![FrontmatterNote::UnrecognizedToolKey("glob".to_string())],
            "the unrecognised key is announced AND kept"
        );
        let b = b.expect("the block parses");
        assert_eq!(
            serde_json::to_value(&b).unwrap(),
            serde_json::json!({
                "tools": [
                    ["edit", {
                        "patterns": [{"pattern": "*", "action": "deny"}],
                        "whole_tool": true
                    }],
                    [{"unrecognized": "glob"}, {
                        "patterns": [{"pattern": "*", "action": "deny"}],
                        "whole_tool": true
                    }]
                ]
            }),
            "`tools` stays an ORDERED array of [toolKey, rules] pairs (a \
             recognised key is the camelCase verb, an unrecognised one is the \
             externally-tagged `{{\"unrecognized\": …}}` object so its verbatim name \
             survives). `Rule`/`Rules`/`PermissionBlock` carry no `rename_all`, so \
             `whole_tool` is snake_case — the wire task's decision to change, not \
             this one's"
        );
    }

    /// D4 — the U-TURN PIN. This reader has NO upstream in the two
    /// `parse_value` copies, and neither copy is a consumer of it: a change
    /// that touches the copies' shared value-reading guards is a FAILURE here,
    /// by construction, rather than a silent cross-application of a bug.
    /// Pull one `parse_value` copy out of a source file, as TEXT ONLY.
    ///
    /// The source arrives through `include_str!`, so its line endings are
    /// WHATEVER THE CHECKOUT MATERIALISED — and a Windows runner's git
    /// defaults to `core.autocrlf=true`, which writes CRLF into the working
    /// tree while the object store keeps LF (`git show HEAD:src-tauri/src/agents.rs
    /// | grep -c $'\r'` → 0 even though the Windows job read CRLF bytes). A
    /// needle carrying a bare `\n` therefore matches on Linux/macOS and matches
    /// NOTHING on Windows, which is how this pin failed red on
    /// `windows-latest` while green everywhere else. Normalising first makes
    /// the pin measure the thing it means to measure — the copy's CONTENT —
    /// and never the platform's checkout convention.
    fn parse_value_copy(src: &str) -> String {
        let src = src.replace("\r\n", "\n");
        let start = src.find("fn parse_value(").expect("the copy exists");
        let end = src[start..]
            .find("\n}\n")
            .map(|o| start + o)
            .expect("the copy closes at column 0");
        src[start..end].to_string()
    }

    /// The CRLF half of the pin above, asserted DIRECTLY rather than left to
    /// be discovered by a platform's compiler. Feeding the extractor the
    /// CRLF bytes a Windows checkout hands `include_str!` must yield the same
    /// body the LF bytes do — otherwise the shared-guard assertions below are
    /// vacuous on that platform (they would run over a body that was never
    /// extracted) or the pin fails outright, which is what it did.
    #[test]
    fn the_copy_extractor_survives_a_crlf_checkout() {
        // The fixture mirrors the shape the extractor is given: a header, the
        // copy, its column-0 closing brace, and a following function so the
        // terminator is findable at all. The marker stands in for the shared
        // guard the pin asserts on.
        let lf = concat!(
            "// header\n",
            "fn parse_value(x: &str) -> String {\n",
            "    let shared_guard = NBSP_DEDENT_OFFSET;\n",
            "}\n",
            "\n",
            "fn other() {}\n",
        );
        let crlf = lf.replace("\n", "\r\n");
        assert_ne!(lf, crlf, "the fixture really is two different byte strings");
        assert_eq!(
            parse_value_copy(lf),
            parse_value_copy(&crlf),
            "the extracted copy is the SAME text whichever line ending the \
             checkout materialised — a Windows CRLF checkout must not read as \
             a missing or empty body"
        );
        assert!(
            parse_value_copy(&crlf).contains("NBSP_DEDENT_OFFSET"),
            "and the body the pin asserts on is non-empty on CRLF, so the \
             shared-guard assertions cannot pass vacuously"
        );
        assert!(
            !parse_value_copy(&crlf).contains("fn other"),
            "the extractor still stops at the copy's own closing brace, so \
             normalising the line endings did not widen the slice"
        );
    }

    #[test]
    fn the_two_parse_value_copies_are_untouched_by_the_permission_reader() {
        let agents = include_str!("../agents.rs");
        let skills = include_str!("../skills.rs");
        let agents_body = parse_value_copy(agents);
        let skills_body = parse_value_copy(skills);

        // The guards BOTH copies share — the shape bugs the "apply it to both
        // copies" rule binds. Each must still be present in each copy.
        for (name, src) in [("agents.rs", &agents_body), ("skills.rs", &skills_body)] {
            assert!(
                src.contains("trim_start_matches([' ', '\\t'])"),
                "{name}: the NBSP/dedent byte-offset guard is intact"
            );
            assert!(
                src.contains("inline[1..inline.len() - 1]"),
                "{name}: the one-quote-pair slice is intact"
            );
            assert!(
                src.contains("inline.contains(\": \")"),
                "{name}: the `: ` mapping-ambiguity guard is intact"
            );
        }

        // The two NAMED divergences, still divergent (the behaviour pin is
        // `agents.rs`'s `the_two_parse_value_copies_diverge_where_their_parity_targets_differ`;
        // this is the source-level half that catches an edit even when the
        // fixtures were left alone).
        assert!(
            skills_body.contains("strip_trailing_comment(inline)"),
            "skills.rs still strips the trailing `#` (its yaml-backed upstream)"
        );
        assert!(
            !agents_body.contains("strip_trailing_comment"),
            "agents.rs still does NOT (its upstream is the `frontmatter.ts` regex reader)"
        );

        // And neither copy grew a `permission:` arm: this module is the block's
        // ONLY owner, so a shape bug here has one owner and a shape bug there
        // has two.
        assert!(
            !agents_body.contains("parse_permission_block"),
            "the scalar copy must not read the nested block"
        );
        assert!(
            !skills_body.contains("parse_permission_block"),
            "and neither may the skill-side copy"
        );
    }
}

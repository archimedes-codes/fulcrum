use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

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
    pub fn ok_text(text: String, details: Option<Value>) -> Self {
        Self {
            content: vec![ContentBlock::Text { text }],
            details,
            is_error: false,
        }
    }

    /// A failed single-text-block result.
    pub fn fail(text: String, details: Option<Value>) -> Self {
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
    /// at session start; `vec![]` in tests). Since the permissions spec's
    /// round-3 correction this is the write GUARD-MEMBERSHIP list (the four
    /// agent-definition dirs) — a membership set, NOT a rule set: the guard
    /// hit is an ASK, and this list's only unconditional power left in the
    /// executors is the WORDING of a rule-driven deny.
    pub protected: Vec<PathBuf>,
    /// (T03) The rule list IN FORCE for this call: the concatenated
    /// `default_rules()` FIRST, then the user rules — the CONCATENATION
    /// CONTRACT of [`crate::agent::policy::default_rules`], frozen defaults +
    /// the per-turn user half (ADR 0030 Deviation 4). Every consumer — the
    /// gate's twin in the executors — hands this SAME list to
    /// [`crate::agent::policy::rule_for`], never a partial one, so the gate
    /// and the executor cannot decide differently about one access.
    ///
    /// Hermetic in tests: an EMPTY list means no rules at all — exactly the
    /// semantics `rule_for(&[], …) == None`, i.e. "no rule speaks to this
    /// access", which is the absence of a verdict and never a verdict. With
    /// `default_rules()` EMPTY (the round-3 correction) an empty list is also
    /// what a real session carries until the user writes a rule, so the test
    /// value and the production default agree.
    pub rules: Vec<crate::agent::policy::AccessRule>,
    /// (T03) The home dir `rule_for` expands a leading `~` against — the
    /// crate's `skills::home_dir()` chain, exactly what
    /// `default_rules()`/`protected_dirs()` go through.
    ///
    /// `None` = NO home: `rule_for`'s `expand_rule_path` expands nothing
    /// against `None`, so a `~`-named rule matches NOTHING — a `~` rule is a
    /// SILENT no-op (never wider than the rules actually present). With a
    /// real home the SAME rule matches and decides, and — appended after the
    /// defaults — beats a rule it ties with. The consequence is pinned by
    /// `a_tilde_rule_with_home_none_matches_nothing_and_with_a_real_home_matches_and_decides`.
    ///
    /// `None` is what every direct `ToolCtx` construction in the test
    /// fixtures passes (HERMETIC: the same rule that keeps `boundary` and
    /// `protected` from reading `$HOME` here); only the harness's production
    /// build carries the session's real home.
    pub home: Option<std::path::PathBuf>,
    /// The web tools' configuration for this turn (Task 1). Read once per turn
    /// from `settings.json` + the environment — NOT re-read per call, so a
    /// key added mid-turn lands next turn. `WebConfig::default()` in tests
    /// (HERMETIC: never read `$HOME` here — the same rule as `boundary`).
    pub web: crate::agent::web::config::WebConfig,
    /// Render requester for escalated headless webview fetches (Task 7).
    pub render_requester: Option<std::sync::Arc<dyn crate::agent::web::RenderRequester>>,
}

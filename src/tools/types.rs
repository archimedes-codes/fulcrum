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

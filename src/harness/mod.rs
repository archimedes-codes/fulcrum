//! The native agent harness (native-agent-harness Phase 2): the in-process
//! Rust replacement for the pi agent process — a `Provider` (the model
//! client, Task 4) + the `AgentLoop` (Task 6: the model → tool →
//! retry/compaction loop, the native `ToolRegistry`, the SQLite
//! `SessionStore`, the `Compactor`, the `RetryPolicy`) + the model
//! catalog (Task 5). Grows across Tasks 5–7; the pieces land one per
//! task.
//!
//! # T5/S5b — this is the MOVED module, and the desktop's `harness` is now an
//! alias with two staying files
//!
//! The whole directory moved except `store.rs` and `trust.rs`, which stayed
//! because each holds a `storage::Db` (`SessionStore` / `SqliteTrustSource`)
//! and the crate has no `rusqlite` in its graph. The desktop's
//! `src-tauri/src/agent/harness/mod.rs` declares those two and re-exports this
//! module, so every pre-S5b desktop path still resolves. See those two files
//! for why a `Db` cannot cross, and `crate::harness::store` /
//! [`crate::trust`] for the trait halves that did.

// (Step A.3) RAISED from `pub(crate)` by this move: the STAYING desktop
// re-exports the MODULE (`crate::agent::harness::ask`), and a package boundary
// erases `pub(crate)`. Nothing outside the crate names `ask`'s items today;
// the module alias is what has to be visible.
pub mod ask;
pub mod catalog;
pub mod compact;
pub mod dispatch;
// (Step A.3) RAISED from `pub(crate)`: the STAYING desktop
// `commands/agents.rs` names `crate::agent::harness::launch::{override_layer,
// resolve_agent_launch, resolve_launch, OverrideLayer, OverrideSource, packaged_settings}`
// — five items plus the module, all of which a package boundary would erase.
pub mod launch;
pub mod r#loop;
pub mod prompt;
pub mod provider;
pub mod retry;
pub mod store;
pub mod tool_specs;

// `trust` is NOT declared here. The trait + `StaticTrustSource` live at the
// crate root (`crate::trust`, moved in S5a); `SqliteTrustSource` stayed desktop
// beside the `Db` it holds. The desktop's `harness/trust.rs` re-exports both
// crate halves so `crate::harness::trust::{TrustSource,
// StaticTrustSource, SqliteTrustSource}` all still resolve at that path.

// The two crate-root modules the desktop's `harness/mod.rs` aliased in S4
// (`crate::agent::harness::model_key::ModelKey`, `crate::agent::harness::WireApi`).
// Aliased rather than spelled out so the moved sources below keep the paths they
// had on the desktop — the S1 lesson (re-export the MODULE) applied inward.
pub use crate::{model_key, wire};

pub use catalog::{
    discover_models, merge_catalog, CompactionConfig, DiscoveredMeta, Model, ModelCatalog,
    ProviderDiscovery, DEFAULT_CONTEXT_WINDOW,
};
pub use compact::{split_for_compaction, Compactor};
pub use dispatch::{InProcessDispatcher, MockDispatcher, SubagentDispatcher};
pub use model_key::{ModelKey, ModelRef};
pub use prompt::{
    build_child_system_message, build_main_prompt, load_project_context, PromptContext,
};
pub use provider::{
    build_provider, AnthropicProvider, ChatMessage, ChatRole, FinishReason, MessageContent,
    ModelOptions, ModelRequest, OpenAiCompatibleProvider, OpenAiResponsesProvider, Provider,
    ProviderError, ProviderEvent, ToolCall, ToolCallDelta, ToolSpec, Usage,
};
pub use r#loop::{AgentLoop, ControlCmd, Prompt, PromptTurn, SudoDeps};
pub use retry::RetryPolicy;
// `SessionStore` is deliberately absent: it is the desktop half of the SPLIT
// store module (it holds a `storage::Db`). The desktop's `harness/mod.rs` adds
// it to its own re-export block, so `crate::harness::SessionStore` is
// unchanged there. `crate::harness::SessionStore` does not exist and must not.
pub use store::{DisplayRow, NoopStore, Store};
// (Step A.3) RAISED from `pub(crate)` by this move: the STAYING desktop
// `commands/settings.rs` names `crate::agent::harness::tool_specs` (the free
// function, re-exported through the desktop's block below) and a package
// boundary erases `pub(crate)`. The MODULE of the same name was already `pub`.
pub use tool_specs::tool_specs;
// `TrustSource` / `StaticTrustSource` / `SqliteTrustSource` are NOT re-exported
// at this root. S5a left the two crate halves re-exported from the DESKTOP's
// `harness/trust.rs`, which is where every pre-S5a consumer resolves them; a
// crate-side `harness::trust` alias would name a module that does not exist
// here (the trait lives at the crate root, the Sqlite source stays desktop).
pub use wire::WireApi;

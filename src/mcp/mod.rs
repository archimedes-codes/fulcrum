//! The native-harness MCP client (ADR 0018): the `mcp` tool's machinery —
//! config loading (pi's `mcp.json` files), the stdio / streamable-HTTP
//! transports, OAuth 2.1 (discovery + DCR + PKCE + refresh), the server
//! manager, and the `mcp` proxy tool handler.

pub mod callback;
pub mod config;
pub mod http;
pub mod manager;
pub mod oauth;
pub mod rpc;
pub mod stdio;
pub mod tool;
pub mod types;

pub use config::load_servers;
pub use manager::McpManager;
pub use tool::mcp_tool;
pub use types::{AuthSpec, HttpDef, ServerDef, StdioDef};

/// The crate's E6 seam: "hand this URL to the OS's default handler" (edge list,
/// plan Step E.5; lands in S3, not S4, because `mcp` moves in S3).
///
/// # The edge this exists for
///
/// The OAuth flow's `authorization_code` arm opens the authorization URL in the
/// user's browser. In the desktop that went through `crate::open::open(url,
/// None)` — the ONE External-open policy (ADR 0051). `mcp` moves in T5/S3 and
/// `open.rs` cannot move with it: it names `tauri::Url` (`open.rs:26`) and calls
/// `tauri_plugin_opener::{open_url, open_path}` (`:218,221`), and a crate that
/// needs `tauri` to compile fails the master roadmap's own decoupling criterion.
/// So the call is inverted: the crate declares what it needs, the desktop
/// implements it by calling `crate::open::open`. Desktop → crate: the allowed
/// direction.
///
/// # Why this shape
///
/// It is deliberately the SAME shape as T3's [`crate::web::RenderRequester`]:
/// one method, `Arc<dyn …>` held on the consumer, injected at construction. Two
/// seams of two different shapes for the same "the crate asks the desktop to do a
/// thing" relationship is a cost with no purchase. A bare `fn(&str)` pointer
/// would also work but would not match, and `oauth::authenticate`'s existing
/// `open_browser: &(dyn Fn(&str) + Send + Sync)` parameter stays exactly as it is
/// — the manager adapts this trait to that closure, so the OAuth module is
/// untouched by the seam.
///
/// # Why the `Result` is kept, and why the MCP side still ignores it
///
/// The desktop's `crate::open::open` returns `Result<(), String>`: a refused
/// scheme (`javascript:`, `data:` — the ADR 0051 denylist) is an error, not a
/// panic. The MCP call site has ALWAYS discarded it (`let _ =
/// crate::open::open(url, None)`), because the flow is fire-and-forget and the
/// authorization URL is in the OAuth error text the user gets if the callback
/// never arrives. Keeping the `Result` preserves what the desktop can report
/// elsewhere; discarding it at this one site is behaviour-identical to the pre-
/// seam `let _ =`, not sloppiness preserved.
pub trait OpenRequester: Send + Sync {
    /// Open `url`. `Err` carries the refusal reason (the desktop's
    /// `crate::open::open` error codes); the MCP flow discards it, matching the
    /// pre-seam `let _ =` call site.
    fn open(&self, url: &str) -> Result<(), String>;
}

/// The no-op implementation: nothing is opened.
///
/// Two consumers, both legitimate:
///
///   * **Tests.** A unit test must never launch a real browser; the OAuth tests
///     inject a requester that records or simulates instead.
///   * **A construction site with no opener wired.** [`McpManager::new`] keeps
///     its three-argument signature (fixtures outside the move set call it
///     positionally) and defaults to this. That is the SAFE default: forgetting
///     to wire the seam costs a browser that does not open — and the OAuth error
///     text still carries the URL for a manual visit — whereas a default that
///     guessed could open something unintended.
///
/// The pin that keeps the production path from silently landing on this default
/// is `manager::tests::the_auth_action_opens_the_browser_through_the_seam`, which
/// drives a real `authorization_code` flow through [`McpManager::authenticate`]
/// and fails if the injected requester is never called.
pub struct NoOpenRequester;

impl OpenRequester for NoOpenRequester {
    fn open(&self, _url: &str) -> Result<(), String> {
        Ok(())
    }
}

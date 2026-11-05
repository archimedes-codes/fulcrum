//! The web subsystem (`web_search` and `fetch_content`): configuration, network
//! clients, Search providers, content extraction, caching, and tool definitions.
//! Egress is NOT policy-gated (ADR 0056): outbound web requests are network
//! calls, not file system operations, and do not pass through `FilePolicy`.
//!
//! # This tree has NO `render` — and that is the point
//!
//! T5/S2 moved this directory into the crate EXCEPT `render.rs`, which builds a
//! Tauri `WebviewWindow` and stays desktop (the crate compiling without `tauri`
//! is the decoupling criterion). The desktop's half is `src-tauri/src/agent/web.rs`.
//!
//! `RenderRequester` is declared HERE, not in `render.rs`, and that is what makes
//! the split possible at all: the trait — the crate's side of the seam — moves
//! with the code that CONSUMES it (`web/tools.rs` holds an
//! `Option<Arc<dyn RenderRequester>>` on `ToolCtx`), while the thing that actually
//! opens a webview stays desktop.
//!
//! Who IMPLEMENTS the trait has since changed, and the split survived it:
//! `worker/core.rs::IpcRenderRequester` used to be the desktop implementation
//! (desktop → crate, the allowed direction), and T8/S8.2 moved that file into this
//! crate — forwarding over Worker stdio to the Supervisor, which is the side that
//! really owns the webview. So the implementer is now crate-side and the edge is
//! intra-crate. The split still stands for the original reason: `render.rs` itself
//! names nothing in this module tree, so moving the trait never dragged the
//! renderer.

pub mod cache;
pub mod config;
pub mod extract;
pub mod net;
pub mod providers;
pub mod tools;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderedPage {
    pub html: String,
    pub status: u16,
    pub final_url: String,
}

pub trait RenderRequester: Send + Sync {
    fn request_render(
        &self,
        url: &str,
        timeout_ms: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RenderedPage, String>> + Send>>;
}

//! The desktop's web subsystem (`web_search` and `fetch_content`).
//!
//! Owns everything web: configuration, network clients, Search providers,
//! content extraction, caching, tool definitions, and rendering.
//! Egress is NOT policy-gated (ADR 0056): outbound web requests are network
//! calls, not file system operations, and do not pass through `FilePolicy`.
//! Later tasks add net, providers, extract, cache, tools, and render here.

pub mod cache;
pub mod config;
pub mod extract;
pub mod net;
pub mod providers;
pub mod render;
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

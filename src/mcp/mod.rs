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

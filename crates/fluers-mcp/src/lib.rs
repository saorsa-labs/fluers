//! # fluers-mcp
//!
//! Model Context Protocol (MCP) client integration for Fluers.
//!
//! Mirrors Flue's `connectMcpServer` / `McpTransport` (`packages/runtime/src/mcp.ts`).
//! Lets an agent call out to external MCP servers (stdio / SSE / websocket)
//! and expose their tools as `fluers-core` tools.
//!
//! MVP: defines the [`Transport`] and [`McpServer`] traits only; concrete
//! transports (stdio subprocess, HTTP-SSE) land in MVP 4.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;
use std::sync::Arc;

use fluers_core::tool::ToolDefinition;

/// Errors from the MCP client.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// The transport failed.
    #[error("mcp transport error: {0}")]
    Transport(String),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, McpError>;

/// A transport for talking to an MCP server.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a JSON-RPC request and await the response.
    async fn request(&self, method: &str, params: &serde_json::Value) -> Result<serde_json::Value>;
}

/// A connected MCP server.
pub struct McpServer {
    #[allow(dead_code)]
    transport: Arc<dyn Transport>,
    tools: Vec<ToolDefinition>,
}

impl McpServer {
    /// Wrap a transport as an MCP server (tools discovered later).
    #[must_use]
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self {
            transport,
            tools: Vec::new(),
        }
    }

    /// The tools this server advertises (empty until discovery is wired).
    #[must_use]
    pub fn tools(&self) -> &[ToolDefinition] {
        &self.tools
    }
}

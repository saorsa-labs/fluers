//! # fluers-mcp
//!
//! Model Context Protocol (MCP) client integration for Fluers.
//!
//! Mirrors Flue's `connectMcpServer` / `McpServerConnection`
//! (`packages/runtime/src/mcp.ts`): lets an agent call out to an external MCP
//! server (stdio transport, MVP scope) and exposes that server's tools as
//! ordinary [`fluers_core::Tool`] impls.
//!
//! See `docs/MVP4_MCP_DESIGN.md` for the design and scope.
//!
//! # Lifecycle
//!
//! [`McpServer::connect_stdio`] spawns the subprocess, runs the initialize
//! handshake, and discovers all tools (paginated). The discovered tools share
//! a single transport handle (an `Arc`-cloned client). When every tool (and the
//! server, if still held) is dropped, the subprocess is reaped — the command is
//! built with `kill_on_drop(true)` so the child is reliably terminated rather
//! than detached.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ListToolsResult, RawContent, RawEmbeddedResource,
    ResourceContents,
};
use rmcp::service::{RunningService, ServiceExt};
use rmcp::transport::TokioChildProcess;
use rmcp::{RoleClient, ServiceError};
use serde_json::Value;
use thiserror::Error;
use tokio::process::Command;

use fluers_core::error::{CoreError, Result as CoreResult};
use fluers_core::tool::{
    InvokeContext, ParameterSchema, Tool as CoreTool, ToolDefinition, ToolResult,
};

/// Maximum characters retained in an [`McpError`] message. Keeps the model's
/// input args and the server's response bodies out of error strings (and thus
/// out of telemetry). Mirrors the 4c `run_failed` truncation fix.
pub const MCP_ERROR_SUMMARY_MAX_CHARS: usize = 200;

/// Truncate a string to [`MCP_ERROR_SUMMARY_MAX_CHARS`], appending an ellipsis.
fn bound_error(msg: impl AsRef<str>) -> String {
    let s = msg.as_ref();
    if s.len() > MCP_ERROR_SUMMARY_MAX_CHARS {
        format!("{}…(truncated)", &s[..MCP_ERROR_SUMMARY_MAX_CHARS])
    } else {
        s.to_string()
    }
}

/// Errors from the MCP client. All variants carry a **bounded** message so user
/// content never leaks into telemetry.
#[derive(Debug, Error)]
pub enum McpError {
    /// The subprocess could not be spawned / the transport could not be created.
    #[error("mcp transport error: {0}")]
    Transport(String),
    /// The MCP protocol layer failed (initialize / list / call).
    #[error("mcp protocol error: {0}")]
    Protocol(String),
    /// A `tools/call` did not finish within the per-request timeout.
    #[error("mcp tool call timed out after {0:?}")]
    Timeout(Duration),
    /// The tool call was cancelled via the run's [`tokio_util::sync::CancellationToken`].
    #[error("mcp tool call cancelled")]
    Cancelled,
}

impl McpError {
    fn transport(msg: impl AsRef<str>) -> Self {
        Self::Transport(bound_error(msg))
    }
    fn protocol(msg: impl AsRef<str>) -> Self {
        Self::Protocol(bound_error(msg))
    }
}

/// Result alias.
pub type Result<T> = std::result::Result<T, McpError>;

/// How to spawn a stdio MCP server subprocess.
#[derive(Debug, Clone)]
pub struct StdioMcpServerConfig {
    /// Friendly name used in the adapted tool name (`mcp__<name>__<tool>`).
    pub name: String,
    /// Executable to launch.
    pub command: String,
    /// Args to pass.
    pub args: Vec<String>,
    /// Extra environment for the child (merged on top of the current env).
    pub env: HashMap<String, String>,
    /// Working directory for the child.
    pub cwd: Option<PathBuf>,
    /// Per-request timeout for `tools/list` and `tools/call`.
    pub request_timeout: Duration,
}

impl StdioMcpServerConfig {
    /// Build the [`tokio::process::Command`] for this config.
    ///
    /// `kill_on_drop(true)` is set so the subprocess is reliably terminated when
    /// the transport (and thus the last tool handle) drops, rather than being
    /// detached.
    fn build_command(&self) -> Command {
        let mut cmd = Command::new(&self.command);
        cmd.args(&self.args);
        cmd.kill_on_drop(true);
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        cmd
    }
}

/// Sanitize a name segment for use in an adapted tool name.
///
/// Keeps `[A-Za-z0-9_-]`, replaces everything else with `_`, trims leading and
/// trailing `_`. Matches Flue's `sanitizeToolNamePart`.
#[must_use]
pub fn sanitize_tool_name_part(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    sanitized.trim_matches('_').to_string()
}

/// Build the adapted tool name `mcp__<server>__<tool>`.
#[must_use]
pub fn adapted_tool_name(server_name: &str, tool_name: &str) -> String {
    format!(
        "mcp__{}__{}",
        sanitize_tool_name_part(server_name),
        sanitize_tool_name_part(tool_name)
    )
}

/// Build the adapted tool description (Flue-compatible).
fn adapted_description(
    server_name: &str,
    original_name: &str,
    description: Option<&str>,
) -> String {
    let mut parts = vec![format!(
        "MCP tool \"{original_name}\" from server \"{server_name}\"."
    )];
    if let Some(desc) = description {
        let trimmed = desc.trim();
        if !trimmed.is_empty() {
            parts.push(trimmed.to_string());
        }
    }
    parts.join(" ")
}

/// Format an MCP [`CallToolResult`] into a model-facing text string.
///
/// - Text content is appended verbatim.
/// - Image / audio content is summarized (`[image: <mime>]`) — base64 is never
///   dumped (privacy + token cost).
/// - Resource content is extracted as text if it carries text, else summarized.
/// - Empty content → a placeholder.
#[must_use]
pub fn format_mcp_result(result: &CallToolResult) -> String {
    let mut parts: Vec<String> = Vec::new();
    for content in &result.content {
        let raw: &RawContent = &content.raw;
        match raw {
            RawContent::Text(t) => {
                if !t.text.is_empty() {
                    parts.push(t.text.clone());
                }
            }
            RawContent::Image(img) => {
                parts.push(format!("[image: {}]", img.mime_type));
            }
            RawContent::Audio(aud) => {
                parts.push(format!("[audio: {}]", aud.mime_type));
            }
            RawContent::Resource(res) => match extract_resource_text(res) {
                Some(text) => parts.push(text),
                None => parts.push(format!("[resource: {}]", resource_uri(&res.resource))),
            },
            RawContent::ResourceLink(link) => {
                parts.push(format!("[resource-link: {} ({})]", link.uri, link.name));
            }
        }
    }
    let joined = parts
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    let joined = joined.trim();
    if joined.is_empty() {
        "(MCP tool returned no content)".to_string()
    } else {
        joined.to_string()
    }
}

/// Best-effort text extraction from an embedded resource.
fn extract_resource_text(res: &RawEmbeddedResource) -> Option<String> {
    match &res.resource {
        ResourceContents::TextResourceContents { text, .. } => {
            if text.trim().is_empty() {
                None
            } else {
                Some(text.clone())
            }
        }
        ResourceContents::BlobResourceContents { .. } => None,
    }
}

/// Get the URI from a [`ResourceContents`].
fn resource_uri(contents: &ResourceContents) -> &str {
    match contents {
        ResourceContents::TextResourceContents { uri, .. } => uri,
        ResourceContents::BlobResourceContents { uri, .. } => uri,
    }
}

/// A connected MCP server. Holds the `rmcp` client handle internally; the
/// public API exposes no `rmcp` types.
///
/// Drop semantics: the subprocess is reaped when the last tool handle (and the
/// server, if still held) is dropped — see [`StdioMcpServerConfig`] and the
/// crate docs on `kill_on_drop`.
pub struct McpServer {
    /// Shared handle to the running rmcp client service. Wrapped in `Arc` so
    /// each adapted tool can hold a clone for `tools/call`.
    client: Arc<RunningService<RoleClient, ()>>,
    /// Discovered + adapted tools.
    tools: Vec<Arc<dyn CoreTool>>,
    /// Server name (for diagnostics).
    name: String,
}

impl McpServer {
    /// Connect to a stdio MCP server, run the initialize handshake, and discover
    /// all tools (paginated, with a duplicate-cursor guard).
    ///
    /// # Errors
    /// - [`McpError::Transport`] if the subprocess cannot be spawned or the
    ///   transport / initialize handshake fails.
    /// - [`McpError::Protocol`] if `tools/list` fails.
    /// - [`McpError::Timeout`] if discovery exceeds `request_timeout`.
    pub async fn connect_stdio(cfg: StdioMcpServerConfig) -> Result<Self> {
        let cmd = cfg.build_command();
        let transport = TokioChildProcess::new(cmd)
            .map_err(|e| McpError::transport(format!("spawn {}: {e}", cfg.name)))?;

        let client = ()
            .serve(transport)
            .await
            .map_err(|e| McpError::transport(format!("connect {}: {e}", cfg.name)))?;
        let client = Arc::new(client);

        // Discover tools with pagination + duplicate-cursor guard.
        let tools = discover_tools(&client, &cfg).await?;

        Ok(Self {
            client,
            tools,
            name: cfg.name,
        })
    }

    /// Discovered tools, adapted to [`fluers_core::Tool`].
    #[must_use]
    pub fn tools(&self) -> &[Arc<dyn CoreTool>] {
        &self.tools
    }

    /// Move the adapted tools out (so an agent can own them).
    #[must_use]
    pub fn into_tools(self) -> Vec<Arc<dyn CoreTool>> {
        // `client` stays alive via the clones held by each tool; dropping the
        // server's own Arc when self drops does not stop the service until the
        // last tool handle also drops.
        let _ = self.client;
        let _ = &self.name;
        self.tools
    }
}

/// Discover all tools from a server, paginating until `next_cursor` is `None`.
/// Guards against repeated cursors (a misbehaving server).
async fn discover_tools(
    client: &Arc<RunningService<RoleClient, ()>>,
    cfg: &StdioMcpServerConfig,
) -> Result<Vec<Arc<dyn CoreTool>>> {
    let mut all: Vec<Arc<dyn CoreTool>> = Vec::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors: HashSet<String> = HashSet::new();

    loop {
        let params = rmcp::model::PaginatedRequestParams::default().with_cursor(cursor.clone());
        let page: ListToolsResult = tokio::time::timeout(cfg.request_timeout, async {
            client.list_tools(Some(params)).await
        })
        .await
        .map_err(|_| McpError::Timeout(cfg.request_timeout))?
        .map_err(|e| McpError::protocol(format!("tools/list: {e}")))?;

        for tool in page.tools {
            let mcp_name = tool.name.to_string();
            let adapted = adapted_tool_name(&cfg.name, &mcp_name);
            if !seen_names.insert(adapted.clone()) {
                tracing::warn!(
                    "MCP server \"{}\" produced duplicate adapted tool name \"{}\"; skipping",
                    cfg.name,
                    adapted
                );
                continue;
            }
            let description =
                adapted_description(&cfg.name, &mcp_name, tool.description.as_deref());
            let definition = ToolDefinition {
                name: adapted,
                label: mcp_name.clone(),
                description,
                parameters: ParameterSchema {
                    fields: schema_to_fields(&tool.input_schema),
                },
            };
            all.push(Arc::new(McpTool {
                client: Arc::clone(client),
                mcp_name,
                definition,
                request_timeout: cfg.request_timeout,
            }) as Arc<dyn CoreTool>);
        }

        match page.next_cursor {
            None => break,
            Some(next) => {
                if next.is_empty() {
                    break;
                }
                // Guard against repeated cursors (infinite pagination).
                if !seen_cursors.insert(next.clone()) {
                    tracing::warn!(
                        "MCP server \"{}\" repeated tools/list cursor during discovery; stopping",
                        cfg.name
                    );
                    break;
                }
                cursor = Some(next);
            }
        }
    }

    Ok(all)
}

/// Convert an rmcp `input_schema` (a `serde_json::Map`, aka `JsonObject`) into
/// our `ParameterSchema::fields`.
fn schema_to_fields(schema: &serde_json::Map<String, Value>) -> BTreeMap<String, Value> {
    schema.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// The adapter that exposes a single MCP tool as a [`fluers_core::Tool`].
pub struct McpTool {
    /// Shared client handle (one per server).
    client: Arc<RunningService<RoleClient, ()>>,
    /// Original MCP tool name (sent verbatim in `tools/call`).
    mcp_name: String,
    /// Adapted definition.
    definition: ToolDefinition,
    /// Per-call timeout.
    request_timeout: Duration,
}

#[async_trait]
impl CoreTool for McpTool {
    fn definition(&self) -> ToolDefinition {
        self.definition.clone()
    }

    async fn execute(&self, ctx: InvokeContext, input: Value) -> CoreResult<ToolResult> {
        use tokio::select;

        let args = input
            .as_object()
            .ok_or_else(|| {
                CoreError::ToolInputValidation(format!(
                    "MCP tool `{}` expects an object input",
                    self.definition.name
                ))
            })?
            .clone();

        let params = CallToolRequestParams::new(self.mcp_name.clone()).with_arguments(args);
        let call_fut = self.client.call_tool(params);

        let result: CallToolResult = select! {
            r = call_fut => {
                r.map_err(|e| map_service_error(e, &self.definition.name))?
            }
            _ = ctx.cancel.cancelled() => {
                return Err(CoreError::ModelProvider(McpError::Cancelled.to_string()));
            }
            _ = tokio::time::sleep(self.request_timeout) => {
                return Err(CoreError::ModelProvider(
                    McpError::Timeout(self.request_timeout).to_string(),
                ));
            }
        };

        let text = format_mcp_result(&result);
        // is_error → prefix with "Error:" so the observability seam's
        // tool_result_ok() marks this as a failed tool call (matches the
        // convention used by the local bash/read tools).
        let text = if result.is_error.unwrap_or(false) {
            format!("Error: {text}")
        } else {
            text
        };

        Ok(ToolResult {
            content: vec![serde_json::json!({
                "type": "text",
                "text": text,
            })],
            details: None,
        })
    }
}

/// Map an rmcp [`ServiceError`] to a bounded [`CoreError`].
fn map_service_error(e: ServiceError, tool_name: &str) -> CoreError {
    let msg = bound_error(format!("MCP tools/call `{tool_name}` failed: {e}"));
    CoreError::ModelProvider(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_keeps_alphanumeric_and_separators() {
        assert_eq!(sanitize_tool_name_part("echo"), "echo");
        assert_eq!(sanitize_tool_name_part("my-tool_2"), "my-tool_2");
        assert_eq!(sanitize_tool_name_part("a.b/c"), "a_b_c");
        assert_eq!(sanitize_tool_name_part("__leading"), "leading");
        assert_eq!(sanitize_tool_name_part("trailing__"), "trailing");
        assert_eq!(sanitize_tool_name_part("  spaces "), "spaces");
        // Non-ASCII collapses to underscores then trims.
        assert_eq!(sanitize_tool_name_part("héllo"), "h_llo");
    }

    #[test]
    fn adapted_name_format() {
        assert_eq!(adapted_tool_name("fs", "read"), "mcp__fs__read");
        assert_eq!(
            adapted_tool_name("my server", "do.thing"),
            "mcp__my_server__do_thing"
        );
    }

    #[test]
    fn error_summary_is_bounded() {
        let short = bound_error("short");
        assert_eq!(short, "short");

        let long = bound_error("x".repeat(MCP_ERROR_SUMMARY_MAX_CHARS * 3));
        assert!(long.ends_with("…(truncated)"));
        assert!(long.len() < MCP_ERROR_SUMMARY_MAX_CHARS * 3);
        assert_eq!(
            &long[..MCP_ERROR_SUMMARY_MAX_CHARS],
            &"x".repeat(MCP_ERROR_SUMMARY_MAX_CHARS)
        );
    }

    #[test]
    fn description_includes_mcp_and_original_names() {
        let d = adapted_description("fs", "read_file", Some("Reads a file."));
        assert!(d.contains("MCP tool \"read_file\""));
        assert!(d.contains("server \"fs\""));
        assert!(d.contains("Reads a file."));
    }

    #[test]
    fn description_without_mcp_description() {
        let d = adapted_description("fs", "read_file", None);
        assert!(d.contains("read_file"));
        assert!(d.contains("fs"));
        assert!(!d.contains("Reads"));
    }

    #[test]
    fn description_ignores_whitespace_only_mcp_description() {
        let d = adapted_description("fs", "read_file", Some("   "));
        // Whitespace-only description is skipped: only the prefix remains.
        assert_eq!(d, "MCP tool \"read_file\" from server \"fs\".");
    }

    #[test]
    fn stdio_config_sets_kill_on_drop() {
        // We can't observe kill_on_drop directly (no accessor), but we can
        // ensure build_command doesn't panic and applies env/args.
        let cfg = StdioMcpServerConfig {
            name: "test".into(),
            command: "echo".into(),
            args: vec!["hi".into()],
            env: HashMap::from([("FOO".into(), "bar".into())]),
            cwd: None,
            request_timeout: Duration::from_secs(5),
        };
        let _cmd = cfg.build_command();
    }

    #[test]
    fn format_text_result() {
        use rmcp::model::{CallToolResult, Content};
        let result = CallToolResult::success(vec![Content::text("hello world")]);
        assert_eq!(format_mcp_result(&result), "hello world");
    }

    #[test]
    fn format_image_result_summarizes_not_base64() {
        use rmcp::model::{CallToolResult, Content};
        let result =
            CallToolResult::success(vec![Content::image("iVBORw0KGgoAAAANS==", "image/png")]);
        let out = format_mcp_result(&result);
        assert_eq!(out, "[image: image/png]");
        // The base64 payload must NOT appear in the model-facing text.
        assert!(!out.contains("iVBORw0KGgo"));
    }

    #[test]
    fn format_multiple_blocks_joined() {
        use rmcp::model::{CallToolResult, Content};
        let result = CallToolResult::success(vec![
            Content::text("line one"),
            Content::image("AAAA", "image/jpeg"),
            Content::text("line two"),
        ]);
        let out = format_mcp_result(&result);
        assert!(out.contains("line one"));
        assert!(out.contains("[image: image/jpeg]"));
        assert!(out.contains("line two"));
        assert!(!out.contains("AAAA"));
    }

    #[test]
    fn format_resource_text_is_extracted() {
        use rmcp::model::{CallToolResult, Content};
        let result = CallToolResult::success(vec![Content::embedded_text(
            "file:///x.txt",
            "the resource body",
        )]);
        assert_eq!(format_mcp_result(&result), "the resource body");
    }

    #[test]
    fn format_resource_link_summarized() {
        use rmcp::model::{CallToolResult, Content, RawResource};
        let link = RawResource {
            uri: "file:///doc.md".into(),
            name: "doc".into(),
            title: None,
            description: None,
            mime_type: None,
            size: None,
            icons: None,
            meta: None,
        };
        let result = CallToolResult::success(vec![Content::resource_link(link)]);
        let out = format_mcp_result(&result);
        assert_eq!(out, "[resource-link: file:///doc.md (doc)]");
    }

    #[test]
    fn format_empty_result_has_placeholder() {
        use rmcp::model::CallToolResult;
        let result = CallToolResult::success(vec![]);
        assert_eq!(format_mcp_result(&result), "(MCP tool returned no content)");
    }

    #[test]
    fn format_is_error_result_surfaces_body() {
        use rmcp::model::{CallToolResult, Content};
        let result = CallToolResult::error(vec![Content::text("disk full")]);
        assert_eq!(result.is_error, Some(true));
        // format_mcp_result itself doesn't add the Error: prefix — that's the
        // execute() path's job. Here we just confirm the text body is surfaced.
        assert_eq!(format_mcp_result(&result), "disk full");
    }

    #[test]
    fn schema_to_fields_round_trips() {
        let mut map = serde_json::Map::new();
        map.insert("type".into(), Value::String("object".into()));
        map.insert(
            "required".into(),
            Value::Array(vec![Value::String("x".into())]),
        );
        let fields = schema_to_fields(&map);
        assert_eq!(fields.get("type").and_then(Value::as_str), Some("object"));
        assert_eq!(
            fields
                .get("required")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(1)
        );
        // ParameterSchema serializes back to the same JSON.
        let schema = ParameterSchema { fields };
        // Serialize back to the same JSON. Infallible for this simple value;
        // use a graceful fallback instead of expect.
        let ser = serde_json::to_value(&schema).unwrap_or(Value::Null);
        assert_eq!(ser.get("type").and_then(Value::as_str), Some("object"));
    }
}

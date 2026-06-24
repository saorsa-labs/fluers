//! Hermetic integration test for fluers-mcp.
//!
//! Spawns the in-crate mock MCP server (`fluers-mcp-mock-server` binary) as a
//! real stdio subprocess, connects via `McpServer::connect_stdio`, discovers
//! tools, and exercises the full `tools/list` + `tools/call` path.
//!
//! No Node, Docker, or network required.

// Tests may panic for clarity and speed (project policy).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::time::Duration;

use fluers_core::tool::InvokeContext;
use fluers_mcp::{McpServer, StdioMcpServerConfig};
use tokio_util::sync::CancellationToken;

fn mock_config() -> StdioMcpServerConfig {
    StdioMcpServerConfig {
        name: "mock".into(),
        command: env!("CARGO_BIN_EXE_fluers-mcp-mock-server").into(),
        args: vec![],
        env: HashMap::new(),
        cwd: None,
        request_timeout: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn connect_discovers_adapted_tools() {
    let server = McpServer::connect_stdio(mock_config())
        .await
        .expect("connect to mock MCP server");
    let names: Vec<String> = server.tools().iter().map(|t| t.definition().name).collect();
    assert!(
        names.contains(&"mcp__mock__echo".to_string()),
        "echo not discovered: {names:?}"
    );
    assert!(
        names.contains(&"mcp__mock__failing".to_string()),
        "failing not discovered: {names:?}"
    );
}

#[tokio::test]
async fn echo_tool_round_trips_text() {
    let server = McpServer::connect_stdio(mock_config())
        .await
        .expect("connect");
    let echo = server
        .into_tools()
        .into_iter()
        .find(|t| t.definition().name == "mcp__mock__echo")
        .expect("echo tool present");

    let ctx = InvokeContext {
        tool_call_id: "call_1".into(),
        cancel: CancellationToken::new(),
    };
    let result = echo
        .execute(ctx, serde_json::json!({ "text": "round-trip payload" }))
        .await
        .expect("echo execute");

    // Single text content block carrying the echoed text.
    assert_eq!(result.content.len(), 1);
    let text = result.content[0]
        .get("text")
        .and_then(|t| t.as_str())
        .expect("text field");
    assert_eq!(text, "round-trip payload");
    assert!(!text.starts_with("Error:"), "should not be an error result");
}

#[tokio::test]
async fn failing_tool_is_marked_error() {
    let server = McpServer::connect_stdio(mock_config())
        .await
        .expect("connect");
    let failing = server
        .into_tools()
        .into_iter()
        .find(|t| t.definition().name == "mcp__mock__failing")
        .expect("failing tool present");

    let ctx = InvokeContext {
        tool_call_id: "call_2".into(),
        cancel: CancellationToken::new(),
    };
    let result = failing
        .execute(ctx, serde_json::json!({}))
        .await
        .expect("execute");

    // is_error → execute() prefixes the text with "Error:" so the observability
    // seam's tool_result_ok() marks this as a failed tool call.
    let text = result.content[0]
        .get("text")
        .and_then(|t| t.as_str())
        .expect("text field");
    assert!(
        text.starts_with("Error:"),
        "failing tool result should be Error-prefixed: {text}"
    );
}

#[tokio::test]
async fn tool_definition_carries_server_name_and_schema() {
    let server = McpServer::connect_stdio(mock_config())
        .await
        .expect("connect");
    let def = server
        .tools()
        .iter()
        .find(|t| t.definition().name == "mcp__mock__echo")
        .expect("echo present")
        .definition();

    assert_eq!(def.label, "echo");
    assert!(def.description.contains("MCP tool \"echo\""));
    assert!(def.description.contains("server \"mock\""));
    // The input schema round-trips: required contains "text".
    let required = def
        .parameters
        .fields
        .get("required")
        .and_then(|v| v.as_array())
        .expect("required array");
    assert!(required.iter().any(|v| v.as_str() == Some("text")));
}

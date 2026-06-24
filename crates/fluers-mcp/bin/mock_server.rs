//! Minimal mock MCP server speaking JSON-RPC 2.0 over stdio (newline-delimited
//! JSON). Built only with the `mock-server` feature so the integration test can
//! spawn it as a real subprocess — no Node, Docker, or network required.
//!
//! Advertises a single tool `echo` that returns its `text` argument verbatim,
//! and a `failing` tool that reports `is_error: true`.
//!
//! This is NOT a general-purpose MCP server — it implements just enough of the
//! protocol (initialize, notifications/initialized, tools/list, tools/call) for
//! the test to exercise the full Fluers client path.

use std::io::{self, BufRead, Write};

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue, // ignore malformed lines
        };

        // notifications (no id) get no response.
        let id = req.get("id").cloned();
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");

        let result = match method {
            "initialize" => Some(serde_json::json!({
                "protocolVersion": "2025-06-18",
                "serverInfo": { "name": "fluers-mock", "version": "0.1.0" },
                "capabilities": { "tools": {} },
            })),
            "notifications/initialized" => None, // notification → no response
            "tools/list" => Some(serde_json::json!({
                "tools": [
                    {
                        "name": "echo",
                        "description": "Echoes back the text argument.",
                        "inputSchema": {
                            "type": "object",
                            "properties": { "text": { "type": "string" } },
                            "required": ["text"],
                        },
                    },
                    {
                        "name": "failing",
                        "description": "Always reports a tool error.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                        },
                    },
                ],
            })),
            "tools/call" => {
                let name = req
                    .get("params")
                    .and_then(|p| p.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                match name {
                    "echo" => {
                        let text = req
                            .get("params")
                            .and_then(|p| p.get("arguments"))
                            .and_then(|a| a.get("text"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("(no text)")
                            .to_string();
                        Some(serde_json::json!({
                            "content": [{ "type": "text", "text": text }],
                            "isError": false,
                        }))
                    }
                    "failing" => Some(serde_json::json!({
                        "content": [{ "type": "text", "text": "boom: always fails" }],
                        "isError": true,
                    })),
                    _ => Some(serde_json::json!({
                        "content": [{ "type": "text", "text": "unknown tool" }],
                        "isError": true,
                    })),
                }
            }
            _ => None,
        };

        if let (Some(id), Some(result)) = (id, result) {
            let resp = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            });
            // serde_json serialization of a json! value is infallible in
            // practice, but fall back to a minimal error envelope to stay
            // panic-free.
            let line = serde_json::to_string(&resp).unwrap_or_else(|_| {
                "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603}}".to_string()
            });
            let mut line = line;
            line.push('\n');
            if stdout.write_all(line.as_bytes()).is_err() {
                break;
            }
            let _ = stdout.flush();
        }
    }
}

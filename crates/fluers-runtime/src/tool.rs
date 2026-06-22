//! Built-in tools, wired to a real [`SessionEnv`].
//!
//! Each tool operates purely against the env, so the *same* tools work over a
//! local directory, a virtual fs, or a remote container. Mirrors Flue's tools
//! in `packages/runtime/src/agent.ts`.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use fluers_core::error::CoreError;
use fluers_core::tool::{
    validate_input, InvokeContext, JsonValue, ParameterSchema, Tool, ToolDefinition, ToolResult,
};
use fluers_core::Result;

use crate::env::{Limits, SessionEnv};
use crate::error::RuntimeError;

/// Build the MVP toolset over `env`: `read`, `write`, `bash`, `glob`, `grep`.
///
/// Each tool captures the env (and the resource [`Limits`]) so it can execute
/// sandboxed operations. `edit` arrives in a later phase (string-replace
/// matching is fiddly enough to warrant its own iteration).
#[must_use]
pub fn mvp_tools(env: Arc<dyn SessionEnv>) -> Vec<Arc<dyn Tool>> {
    mvp_tools_with_limits(env, Limits::default())
}

/// Like [`mvp_tools`] but with explicit resource limits.
#[must_use]
pub fn mvp_tools_with_limits(env: Arc<dyn SessionEnv>, limits: Limits) -> Vec<Arc<dyn Tool>> {
    vec![
        Arc::new(ReadTool::new(env.clone(), limits)),
        Arc::new(WriteTool::new(env.clone(), limits)),
        Arc::new(BashTool::new(env.clone(), limits)),
        Arc::new(GlobTool::new(env.clone())),
        Arc::new(GrepTool::new(env)),
    ]
}

// ---------------------------------------------------------------------------
// read
// ---------------------------------------------------------------------------

/// `read` — read a file from the sandbox, bounded by line/byte limits.
pub struct ReadTool {
    env: Arc<dyn SessionEnv>,
    limits: Limits,
    def: ToolDefinition,
}

impl ReadTool {
    /// Construct a `read` tool bound to `env` with the given `limits`.
    #[must_use]
    pub fn new(env: Arc<dyn SessionEnv>, limits: Limits) -> Self {
        let def = ToolDefinition {
            name: "read".into(),
            label: "Read File".into(),
            description: "Read a file from the sandbox. Path must be relative.".into(),
            parameters: ParameterSchema {
                fields: json_schema(&[
                    ("path", "string", true),
                    ("max_lines", "number", false),
                    ("max_bytes", "number", false),
                ]),
            },
        };
        Self { env, limits, def }
    }
}

#[async_trait]
impl Tool for ReadTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        if ctx.cancel.is_cancelled() {
            return Err(CoreError::Cancelled("read cancelled".into()));
        }
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("read: `path` required".into()))?;
        let max_lines = input
            .get("max_lines")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(self.limits.max_read_lines);
        let max_bytes = input
            .get("max_bytes")
            .and_then(Value::as_u64)
            .map(|n| n as usize)
            .unwrap_or(self.limits.max_read_bytes);
        match self
            .env
            .read_file(&PathBuf::from(path), max_lines, max_bytes)
            .await
        {
            Ok(content) => Ok(ToolResult {
                content: vec![json!({ "type": "text", "text": content })],
                details: Some(json!({ "path": path, "bytes": content.len() })),
            }),
            Err(RuntimeError::Io(e)) => Ok(ToolResult {
                content: vec![
                    json!({ "type": "text", "text": format!("Error reading `{path}`: {e}") }),
                ],
                details: None,
            }),
            Err(other) => Err(CoreError::ToolOutput(other.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// write
// ---------------------------------------------------------------------------

/// `write` — write a file, creating parent directories.
pub struct WriteTool {
    env: Arc<dyn SessionEnv>,
    #[allow(dead_code)]
    limits: Limits,
    def: ToolDefinition,
}

impl WriteTool {
    /// Construct a `write` tool bound to `env` with the given `limits`.
    #[must_use]
    pub fn new(env: Arc<dyn SessionEnv>, limits: Limits) -> Self {
        let def = ToolDefinition {
            name: "write".into(),
            label: "Write File".into(),
            description: "Write content to a file. Creates the file and parent directories.".into(),
            parameters: ParameterSchema {
                fields: json_schema(&[("path", "string", true), ("content", "string", true)]),
            },
        };
        Self { env, limits, def }
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, _ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("write: `path` required".into()))?;
        let content = input
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("write: `content` required".into()))?;
        match self.env.write_file(&PathBuf::from(path), content).await {
            Ok(()) => Ok(ToolResult {
                content: vec![json!({
                    "type": "text",
                    "text": format!("Wrote {} bytes to `{}`", content.len(), path)
                })],
                details: Some(json!({ "path": path, "bytes": content.len() })),
            }),
            Err(e) => Err(CoreError::ToolOutput(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// bash
// ---------------------------------------------------------------------------

/// `bash` — run a shell command in the sandbox.
pub struct BashTool {
    env: Arc<dyn SessionEnv>,
    limits: Limits,
    def: ToolDefinition,
}

impl BashTool {
    /// Construct a `bash` tool bound to `env` with the given `limits`.
    #[must_use]
    pub fn new(env: Arc<dyn SessionEnv>, limits: Limits) -> Self {
        let def = ToolDefinition {
            name: "bash".into(),
            label: "Run Shell".into(),
            description: "Run a shell command in the sandbox. Returns stdout, stderr, exit code."
                .into(),
            parameters: ParameterSchema {
                fields: json_schema(&[
                    ("command", "string", true),
                    ("timeout_ms", "number", false),
                ]),
            },
        };
        Self { env, limits, def }
    }
}

#[async_trait]
impl Tool for BashTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        let command = input
            .get("command")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("bash: `command` required".into()))?;
        let timeout_ms = input
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .or(Some(30_000));
        match self
            .env
            .exec(command, &PathBuf::from("."), timeout_ms, &ctx.cancel)
            .await
        {
            Ok(res) => {
                let text = format!(
                    "[exit {}]\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    res.exit_code, res.stdout, res.stderr
                );
                Ok(ToolResult {
                    content: vec![json!({ "type": "text", "text": text })],
                    details: Some(json!({
                        "exit_code": res.exit_code,
                        "max_grep": self.limits.max_grep_matches,
                    })),
                })
            }
            Err(e) => Err(CoreError::ToolOutput(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// glob
// ---------------------------------------------------------------------------

/// `glob` — list files matching a pattern.
pub struct GlobTool {
    env: Arc<dyn SessionEnv>,
    def: ToolDefinition,
}

impl GlobTool {
    /// Construct a `glob` tool bound to `env`.
    #[must_use]
    pub fn new(env: Arc<dyn SessionEnv>) -> Self {
        let def = ToolDefinition {
            name: "glob".into(),
            label: "Glob".into(),
            description: "List files (relative paths) matching a glob pattern.".into(),
            parameters: ParameterSchema {
                fields: json_schema(&[("pattern", "string", true)]),
            },
        };
        Self { env, def }
    }
}

#[async_trait]
impl Tool for GlobTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, _ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        let pattern = input
            .get("pattern")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("glob: `pattern` required".into()))?;
        match self.env.glob(pattern, 1000).await {
            Ok(paths) => {
                let text = if paths.is_empty() {
                    "(no matches)".to_string()
                } else {
                    paths.join("\n")
                };
                Ok(ToolResult {
                    content: vec![json!({ "type": "text", "text": text })],
                    details: Some(json!({ "count": paths.len() })),
                })
            }
            Err(e) => Err(CoreError::ToolOutput(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// grep
// ---------------------------------------------------------------------------

/// `grep` — search file contents.
pub struct GrepTool {
    env: Arc<dyn SessionEnv>,
    def: ToolDefinition,
}

impl GrepTool {
    /// Construct a `grep` tool bound to `env`.
    #[must_use]
    pub fn new(env: Arc<dyn SessionEnv>) -> Self {
        let def = ToolDefinition {
            name: "grep".into(),
            label: "Grep".into(),
            description: "Search file contents for a pattern (regex).".into(),
            parameters: ParameterSchema {
                fields: json_schema(&[("pattern", "string", true), ("paths", "array", false)]),
            },
        };
        Self { env, def }
    }
}

#[async_trait]
impl Tool for GrepTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, _ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        let pattern = input
            .get("pattern")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("grep: `pattern` required".into()))?;
        let paths: Vec<&str> = input
            .get("paths")
            .and_then(Value::as_array)
            .map(|arr| arr.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        match self.env.grep(pattern, &paths, 100).await {
            Ok(matches) => {
                let text = if matches.is_empty() {
                    "(no matches)".to_string()
                } else {
                    matches.join("\n")
                };
                Ok(ToolResult {
                    content: vec![json!({ "type": "text", "text": text })],
                    details: Some(json!({ "count": matches.len() })),
                })
            }
            Err(e) => Err(CoreError::ToolOutput(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// schema helper
// ---------------------------------------------------------------------------

/// Build a JSON-Schema-ish object with typed properties + a `required` list.
fn json_schema(props: &[(&str, &str, bool)]) -> std::collections::BTreeMap<String, Value> {
    let mut fields: std::collections::BTreeMap<String, Value> = std::collections::BTreeMap::new();
    fields.insert("type".into(), json!("object"));
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for (name, ty, req) in props {
        properties.insert(
            (*name).to_string(),
            json!({ "type": ty, "description": format!("`{name}` parameter") }),
        );
        if *req {
            required.push(json!(name));
        }
    }
    fields.insert("properties".into(), Value::Object(properties));
    fields.insert("required".into(), Value::Array(required));
    fields
}

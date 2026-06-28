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

/// Build the MVP toolset over `env`: `read`, `write`, `edit`, `bash`, `glob`, `grep`.
///
/// Each tool captures the env (and the resource [`Limits`]) so it can execute
/// sandboxed operations.
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
        Arc::new(EditTool::new(env.clone(), limits)),
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
// edit
// ---------------------------------------------------------------------------

/// `edit` — replace a **unique** snippet in a file.
///
/// `old_text` must occur exactly once in the file; the tool errors if it is
/// absent (no-op risk) or ambiguous (>1 match). Reads the file in full via
/// [`SessionEnv::read_file_full`] so an oversized file is **rejected** rather
/// than silently truncated (which would lose data on write-back).
pub struct EditTool {
    env: Arc<dyn SessionEnv>,
    limits: Limits,
    def: ToolDefinition,
}

impl EditTool {
    /// Construct an `edit` tool bound to `env` with the given `limits`.
    #[must_use]
    pub fn new(env: Arc<dyn SessionEnv>, limits: Limits) -> Self {
        let def = ToolDefinition {
            name: "edit".into(),
            label: "Edit File".into(),
            description:
                "Replace a unique snippet in a file. `old_text` must match exactly one place."
                    .into(),
            parameters: ParameterSchema {
                fields: json_schema(&[
                    ("path", "string", true),
                    ("old_text", "string", true),
                    ("new_text", "string", true),
                ]),
            },
        };
        Self { env, limits, def }
    }
}

#[async_trait]
impl Tool for EditTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, _ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        let path = input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("edit: `path` required".into()))?;
        let old_text = input
            .get("old_text")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("edit: `old_text` required".into()))?;
        let new_text = input
            .get("new_text")
            .and_then(Value::as_str)
            .ok_or_else(|| CoreError::ToolInputValidation("edit: `new_text` required".into()))?;
        if old_text.is_empty() {
            return Err(CoreError::ToolInputValidation(
                "edit: `old_text` must be non-empty".into(),
            ));
        }
        let content = self
            .env
            .read_file_full(&PathBuf::from(path), self.limits.max_edit_bytes)
            .await
            .map_err(|e| CoreError::ToolOutput(e.to_string()))?;
        let occurrences = content.matches(old_text).count();
        if occurrences == 0 {
            return Err(CoreError::ToolInputValidation(format!(
                "edit: `old_text` not found in `{path}`"
            )));
        }
        if occurrences > 1 {
            return Err(CoreError::ToolInputValidation(format!(
                "edit: `old_text` matches {occurrences} places in `{path}`; it must be unique"
            )));
        }
        let updated = content.replacen(old_text, new_text, 1);
        self.env
            .write_file(&PathBuf::from(path), &updated)
            .await
            .map_err(|e| CoreError::ToolOutput(e.to_string()))?;
        Ok(ToolResult {
            content: vec![json!({
                "type": "text",
                "text": format!("Edited `{}` ({} -> {} bytes)", path, content.len(), updated.len())
            })],
            details: Some(json!({
                "path": path,
                "old_bytes": content.len(),
                "new_bytes": updated.len()
            })),
        })
    }
}

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

#[cfg(test)]
mod edit_tests {
    //! `edit` tool semantics: unique-match replace + data-loss safety.
    use super::*;
    use crate::LocalSessionEnv;
    use std::path::Path;
    use tokio_util::sync::CancellationToken;

    fn ctx() -> InvokeContext {
        InvokeContext {
            tool_call_id: "t1".into(),
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn edit_replaces_unique_match() {
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        env.write_file(Path::new("a.txt"), "hello world")
            .await
            .unwrap();
        let tool = EditTool::new(env.clone(), Limits::default());
        let input = json!({"path":"a.txt","old_text":"world","new_text":"moon"});
        tool.execute(ctx(), input).await.unwrap();
        let after = env.read_file(Path::new("a.txt"), 100, 1024).await.unwrap();
        assert_eq!(after, "hello moon");
    }

    #[tokio::test]
    async fn edit_errors_when_old_text_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        env.write_file(Path::new("a.txt"), "hello").await.unwrap();
        let tool = EditTool::new(env, Limits::default());
        let input = json!({"path":"a.txt","old_text":"xyz","new_text":"abc"});
        let res = tool.execute(ctx(), input).await;
        assert!(matches!(res, Err(CoreError::ToolInputValidation(_))));
    }

    #[tokio::test]
    async fn edit_errors_when_old_text_not_unique() {
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        env.write_file(Path::new("a.txt"), "ha ha ha")
            .await
            .unwrap();
        let tool = EditTool::new(env, Limits::default());
        let input = json!({"path":"a.txt","old_text":"ha","new_text":"ho"});
        let res = tool.execute(ctx(), input).await;
        assert!(matches!(res, Err(CoreError::ToolInputValidation(_))));
    }

    #[tokio::test]
    async fn edit_errors_when_old_text_empty() {
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        env.write_file(Path::new("a.txt"), "hello").await.unwrap();
        let tool = EditTool::new(env, Limits::default());
        let input = json!({"path":"a.txt","old_text":"","new_text":"x"});
        let res = tool.execute(ctx(), input).await;
        assert!(matches!(res, Err(CoreError::ToolInputValidation(_))));
    }

    #[tokio::test]
    async fn edit_rejects_path_escape() {
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        env.write_file(Path::new("a.txt"), "hello").await.unwrap();
        let tool = EditTool::new(env, Limits::default());
        // `..` is rejected at the env containment seam (resolve), before any edit.
        let input = json!({"path":"../escape.txt","old_text":"x","new_text":"y"});
        let res = tool.execute(ctx(), input).await;
        assert!(res.is_err(), "path escape must be rejected");
    }

    #[tokio::test]
    async fn edit_round_trips_multiline_block() {
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        let body = "line one\nTODO: fix me\nline three\n";
        env.write_file(Path::new("m.txt"), body).await.unwrap();
        let tool = EditTool::new(env.clone(), Limits::default());
        let input = json!({"path":"m.txt","old_text":"TODO: fix me\nline three","new_text":"DONE\nline three"});
        tool.execute(ctx(), input).await.unwrap();
        let after = env.read_file(Path::new("m.txt"), 100, 1024).await.unwrap();
        assert_eq!(after, "line one\nDONE\nline three\n");
    }

    #[tokio::test]
    async fn edit_errors_when_file_too_large_and_does_not_destroy() {
        // The data-loss-safety guarantee: an oversized file is REJECTED by
        // read_file_full (FileTooLarge), never silently truncated + written
        // back. The original content must survive untouched.
        let dir = tempfile::tempdir().unwrap();
        let env: Arc<dyn SessionEnv> = Arc::new(
            LocalSessionEnv::new(dir.path(), Limits::default())
                .await
                .unwrap(),
        );
        let original = "a".repeat(100);
        env.write_file(Path::new("big.txt"), &original)
            .await
            .unwrap();
        let small_cap = Limits {
            max_edit_bytes: 50,
            ..Limits::default()
        };
        let tool = EditTool::new(env.clone(), small_cap);
        let input = json!({"path":"big.txt","old_text":"a","new_text":"b"});
        let res = tool.execute(ctx(), input).await;
        assert!(matches!(res, Err(CoreError::ToolOutput(_))));
        // Content is intact — no truncation, no partial write-back.
        let after = env
            .read_file(Path::new("big.txt"), 1000, 4096)
            .await
            .unwrap();
        assert_eq!(after, original);
    }
}

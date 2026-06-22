//! Tool definitions, calls, and the `Tool` trait.
//!
//! Mirrors `AgentTool` / `AgentToolResult` from `pi-agent-core`.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::{CoreError, Result};

/// A JSON value, aliased for ergonomic imports.
pub type JsonValue = Value;

/// JSON-Schema-ish parameter description for a tool.
///
/// Intentionally loose (a serde_json map) so adapters can carry any schema
/// dialect a provider expects.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParameterSchema {
    /// The schema fields.
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

/// Per-invocation context handed to a tool's [`Tool::execute`].
pub struct InvokeContext {
    /// The unique id of this invocation's tool call.
    pub tool_call_id: String,
    /// Cooperative + deadline cancellation. Tools should `select!` on
    /// `cancel.cancelled()` for long-running work. Clonable and `'static`.
    pub cancel: CancellationToken,
}

/// A tool call extracted from a model response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    /// The tool's name.
    pub name: String,
    /// The arguments object the model produced.
    pub input: Value,
}

/// A tool's result returned to the model.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolResult {
    /// Human/model-readable content blocks describing the outcome.
    pub content: Vec<Value>,
    /// Optional structured details (not shown to the model verbatim).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// A tool's static definition (name, schema, description).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    /// Machine name, e.g. `write`.
    pub name: String,
    /// Human label, e.g. `Write File`.
    pub label: String,
    /// Description shown to the model.
    pub description: String,
    /// Input parameter schema.
    pub parameters: ParameterSchema,
}

/// The trait every tool implements.
///
/// Flue's `AgentTool` is an object with a `parameters` schema and an async
/// `execute`. The Rust equivalent is this trait, wrapped in `Arc<dyn Tool>`.
#[async_trait]
pub trait Tool: Send + Sync {
    /// The static definition (name/schema/description).
    fn definition(&self) -> ToolDefinition;

    /// Execute the tool with validated input.
    async fn execute(&self, ctx: InvokeContext, input: Value) -> Result<ToolResult>;
}

/// Validate `input` against a tool's `parameters` schema.
///
/// MVP: only checks top-level required keys are present. Full JSON-Schema
/// validation is layered in later (see `PORTING_PLAN.md`).
pub fn validate_input(def: &ToolDefinition, input: &Value) -> Result<()> {
    let Some(obj) = input.as_object() else {
        return Err(CoreError::ToolInputValidation(format!(
            "tool `{}` expects an object input",
            def.name
        )));
    };
    if let Some(required) = def
        .parameters
        .fields
        .get("required")
        .and_then(Value::as_array)
    {
        for req in required {
            if let Some(key) = req.as_str() {
                if !obj.contains_key(key) {
                    return Err(CoreError::ToolInputValidation(format!(
                        "tool `{}` missing required parameter `{key}`",
                        def.name
                    )));
                }
            }
        }
    }
    Ok(())
}

//! Built-in tools.
//!
//! Mirrors Flue's tools in `packages/runtime/src/agent.ts`:
//! `read`, `write`, `edit`, `bash`, `grep`, `glob`. Each tool operates
//! purely against a [`SessionEnv`](crate::SessionEnv), so it is
//! sandbox-agnostic.
//!
//! MVP returns the tool *definitions*; full implementations land in MVP 0
//! (see `PORTING_PLAN.md`).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use fluers_core::error::CoreError;
use fluers_core::tool::{
    validate_input, InvokeContext, JsonValue, ParameterSchema, Tool, ToolDefinition, ToolResult,
};
use fluers_core::Result;

use crate::env::SessionEnv;

/// Return the names of the built-in tools.
///
/// (The concrete `Arc<dyn Tool>` instances that wrap a `SessionEnv` arrive
/// in MVP 0; this exposes the vocabulary so config/CLI wiring can reference
/// them now.)
#[must_use]
pub fn builtin_tool_names() -> &'static [&'static str] {
    &["read", "write", "edit", "bash", "grep", "glob"]
}

/// A thin tool wrapper that delegates execution to a closure over the env.
///
/// `run` returns a `Result` so errors propagate as tool failures rather than
/// being stringified into "successful" output.
struct EnvTool {
    def: ToolDefinition,
    #[allow(clippy::type_complexity)]
    run: Arc<dyn Fn(&JsonValue) -> Result<String> + Send + Sync>,
}

#[async_trait]
impl Tool for EnvTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        // Cooperative cancellation: surface a cancelled error if the token has
        // already fired before we start. Full `select!` wiring arrives with
        // the real SessionEnv in MVP 0.
        if ctx.cancel.is_cancelled() {
            return Err(CoreError::Cancelled(format!(
                "tool `{}` cancelled",
                self.def.name
            )));
        }
        let text = (self.run)(&input)?;
        Ok(ToolResult {
            content: vec![json!({ "type": "text", "text": text })],
            details: None,
        })
    }
}

/// Build the placeholder built-in tools for the given env.
///
/// These echo a "not yet wired" message until the local `SessionEnv`
/// implementation lands; they exist so the agent/tool graph type-checks
/// end to end today.
#[must_use]
pub fn builtin_tools(_env: Arc<dyn SessionEnv>) -> Vec<Arc<dyn Tool>> {
    let mk = |name: &'static str, label: &'static str, desc: &'static str| -> Arc<dyn Tool> {
        let owned_name = name.to_string();
        Arc::new(EnvTool {
            def: ToolDefinition {
                name: owned_name.clone(),
                label: label.to_string(),
                description: desc.to_string(),
                parameters: ParameterSchema::default(),
            },
            run: Arc::new(move |input| {
                Ok(format!(
                    "`{owned_name}` stub — input was {input} (see PORTING_PLAN.md MVP 0)"
                ))
            }),
        })
    };
    vec![
        mk("read", "Read File", "Read a file from the sandbox."),
        mk("write", "Write File", "Write a file in the sandbox."),
        mk("edit", "Edit File", "Edit a file in the sandbox."),
        mk("bash", "Run Shell", "Run a shell command in the sandbox."),
        mk("grep", "Grep", "Search file contents in the sandbox."),
        mk("glob", "Glob", "List files matching a pattern."),
    ]
}

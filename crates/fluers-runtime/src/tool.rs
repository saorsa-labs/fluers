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
use tokio::sync::Notify;

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
struct EnvTool {
    def: ToolDefinition,
    #[allow(clippy::type_complexity)]
    run: Arc<dyn Fn(&JsonValue) -> String + Send + Sync>,
}

#[async_trait]
impl Tool for EnvTool {
    fn definition(&self) -> ToolDefinition {
        self.def.clone()
    }

    async fn execute(&self, ctx: InvokeContext, input: JsonValue) -> Result<ToolResult> {
        validate_input(&self.def, &input)?;
        // Best-effort cooperative cancellation: check once before running.
        // Full per-tool cancel wiring arrives with the local SessionEnv.
        let _ = &ctx.cancel;
        let text = (self.run)(&input);
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
                format!("`{owned_name}` stub — input was {input} (see PORTING_PLAN.md MVP 0)")
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

// Keep the Notify import used in signatures reachable for future wiring.
const _: fn() = || {
    fn _t(_n: &Notify) {}
};

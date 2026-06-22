//! The pure agent turn-loop.
//!
//! This is the Rust heart of Flue's `agent-coordinator.ts` turn logic — but
//! *only* the pure loop: send messages + tool defs to a [`ModelProvider`],
//! append assistant messages, execute any tool calls, append their results,
//! and repeat until the model stops calling tools or `max_turns` is hit.
//!
//! The loop talks only to [`ModelProvider`] + `Arc<dyn Tool>` and knows nothing
//! about sessions, events, sandboxes, or persistence — those live in
//! `fluers-runtime`'s coordinator (MVP 3+).

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::error::{CoreError, Result};
use crate::message::{AgentMessage, ContentBlock, Role};
use crate::model::{Model, ModelProvider, ModelRequest};
use crate::thinking::ThinkingLevel;
use crate::tool::{InvokeContext, Tool, ToolCall, ToolResult};

/// Configuration for a single agent run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Maximum number of model turns before the loop aborts.
    pub max_turns: usize,
    /// Reasoning effort forwarded to the provider.
    pub thinking: ThinkingLevel,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            max_turns: 12,
            thinking: ThinkingLevel::default(),
        }
    }
}

/// The outcome of a completed agent run.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    /// How many model turns ran.
    pub turns: usize,
    /// The final assistant text (concatenated text blocks of the last
    /// assistant message). Empty if the model ended on a tool call.
    pub final_text: String,
}

/// Run the agent loop.
///
/// `messages` is seeded by the caller (typically a `System` message followed
/// by a `User` message) and mutated in place as the loop appends assistant
/// turns and tool results.
///
/// Tools are executed **sequentially** in MVP for determinism; parallel tool
/// calls arrive in a later phase.
///
/// Cancellation: the loop checks `cancel.is_cancelled()` between turns and
/// before each tool call.
pub async fn run_agent(
    provider: &dyn ModelProvider,
    tools: &[Arc<dyn Tool>],
    messages: &mut Vec<AgentMessage>,
    model: &Model,
    config: &RunConfig,
    cancel: &CancellationToken,
) -> Result<RunOutcome> {
    let mut turns = 0usize;
    loop {
        if cancel.is_cancelled() {
            return Err(CoreError::Cancelled("agent run cancelled".into()));
        }
        if turns >= config.max_turns {
            return Err(CoreError::ModelResponse(format!(
                "max_turns ({}) exceeded — the model kept calling tools",
                config.max_turns
            )));
        }
        turns += 1;

        let request = ModelRequest {
            model: model.clone(),
            messages: messages.clone(),
            tools: tools.iter().map(|t| t.definition()).collect(),
            thinking: config.thinking,
            params: Default::default(),
        };
        let response = provider.invoke(request).await?;
        // Snapshot this turn's tool calls *before* moving the messages into history.
        let tool_calls: Vec<(String, ToolCall)> = response
            .messages
            .iter()
            .flat_map(|m| m.content.iter())
            .filter_map(|block| {
                if let ContentBlock::ToolUse { id, call } = block {
                    Some((id.clone(), call.clone()))
                } else {
                    None
                }
            })
            .collect();
        // Append the assistant turn(s) to the running history.
        messages.extend(response.messages);

        if tool_calls.is_empty() {
            // No tool calls ⇒ the model finished. Extract final text.
            let final_text = extract_final_text(messages);
            return Ok(RunOutcome { turns, final_text });
        }

        // Execute each tool call sequentially and append a Tool message.
        for (id, call) in tool_calls {
            if cancel.is_cancelled() {
                return Err(CoreError::Cancelled(
                    "agent run cancelled between tool calls".into(),
                ));
            }
            let result = execute_tool_call(tools, &id, &call, cancel).await;
            let tool_msg = AgentMessage {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: id,
                    content: serde_json::to_value(&result)
                        .unwrap_or_else(|_| serde_json::json!({ "error": "serialize failed" })),
                }],
            };
            messages.push(tool_msg);
        }
    }
}

/// Find a tool by name and execute it, returning a result even on error
/// (so the model can recover) rather than aborting the whole run.
async fn execute_tool_call(
    tools: &[Arc<dyn Tool>],
    id: &str,
    call: &ToolCall,
    cancel: &CancellationToken,
) -> ToolResult {
    let Some(tool) = tools.iter().find(|t| t.definition().name == call.name) else {
        return error_result(&format!("unknown tool: `{}`", call.name));
    };
    let ctx = InvokeContext {
        tool_call_id: id.to_string(),
        cancel: cancel.clone(),
    };
    match tool.execute(ctx, call.input.clone()).await {
        Ok(result) => result,
        Err(err) => error_result(&err.to_string()),
    }
}

/// Build a `ToolResult` carrying a single error text block.
fn error_result(message: &str) -> ToolResult {
    ToolResult {
        content: vec![serde_json::json!({ "type": "text", "text": format!("Error: {message}") })],
        details: None,
    }
}

/// Concatenate the text blocks of the last assistant message in `messages`.
fn extract_final_text(messages: &[AgentMessage]) -> String {
    messages
        .iter()
        .rev()
        .find(|m| m.role == Role::Assistant)
        .map(|m| {
            m.content
                .iter()
                .filter_map(|b| {
                    if let ContentBlock::Text { text } = b {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    //! Walking-skeleton tests for the agent loop.
    //!
    //! Uses a scripted mock provider and a mock `echo` tool — no network, no
    //! sandbox, no API keys. CI-safe.

    use super::*;
    use crate::model::ModelResponse;
    use async_trait::async_trait;
    use serde_json::{json, Value};

    /// A provider that returns scripted responses in order.
    struct MockProvider {
        responses: std::sync::Mutex<std::collections::VecDeque<ModelResponse>>,
    }

    impl MockProvider {
        fn new(responses: Vec<Vec<AgentMessage>>) -> Self {
            let responses = responses
                .into_iter()
                .map(|msgs| ModelResponse { messages: msgs })
                .collect();
            Self {
                responses: std::sync::Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl ModelProvider for MockProvider {
        async fn invoke(&self, _request: ModelRequest) -> Result<ModelResponse> {
            let next = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ModelResponse { messages: vec![] });
            Ok(next)
        }
    }

    /// A tool that echoes its `text` input back.
    struct EchoTool;

    #[async_trait]
    impl Tool for EchoTool {
        fn definition(&self) -> crate::tool::ToolDefinition {
            crate::tool::ToolDefinition {
                name: "echo".into(),
                label: "Echo".into(),
                description: "Echo back the provided text.".into(),
                parameters: crate::tool::ParameterSchema::default(),
            }
        }

        async fn execute(&self, _ctx: InvokeContext, input: Value) -> Result<ToolResult> {
            let text = input
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("(no text)")
                .to_string();
            Ok(ToolResult {
                content: vec![json!({ "type": "text", "text": format!("echo: {text}") })],
                details: None,
            })
        }
    }

    fn assistant_text(t: &str) -> AgentMessage {
        AgentMessage {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: t.into() }],
        }
    }

    fn assistant_tool_use(id: &str, name: &str, input: Value) -> AgentMessage {
        AgentMessage {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: id.into(),
                call: ToolCall {
                    name: name.into(),
                    input,
                },
            }],
        }
    }

    fn user(t: &str) -> AgentMessage {
        AgentMessage {
            role: Role::User,
            content: vec![ContentBlock::Text { text: t.into() }],
        }
    }

    #[tokio::test]
    async fn loop_runs_tool_then_finishes() {
        // Turn 1: model calls `echo`. Turn 2: model returns final text.
        let provider = MockProvider::new(vec![
            vec![assistant_tool_use(
                "call_1",
                "echo",
                json!({ "text": "hello" }),
            )],
            vec![assistant_text("done")],
        ]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("please echo hello then say done")];

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("loop should complete");

        assert_eq!(outcome.turns, 2);
        assert_eq!(outcome.final_text, "done");

        // History must contain: user, assistant(tool_use), tool(result), assistant(text).
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[2].role, Role::Tool);
        // The tool result content must carry the echoed text.
        match &messages[2].content[0] {
            ContentBlock::ToolResult { content, .. } => {
                let s = serde_json::to_string(content).unwrap_or_default();
                assert!(s.contains("echo: hello"), "tool result was: {s}");
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn loop_stops_when_no_tool_calls() {
        let provider = MockProvider::new(vec![vec![assistant_text("just text, no tools")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![];
        let model = Model::new("mock/test");
        let mut messages = vec![user("hi")];

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("loop should complete");

        assert_eq!(outcome.turns, 1);
        assert_eq!(outcome.final_text, "just text, no tools");
    }

    #[tokio::test]
    async fn loop_recovers_from_unknown_tool() {
        // Model calls a tool that doesn't exist; loop must surface an error
        // result to the model and continue, not abort.
        let provider = MockProvider::new(vec![
            vec![assistant_tool_use("c1", "nonexistent", json!({}))],
            vec![assistant_text("recovered")],
        ]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call a missing tool")];

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
        )
        .await
        .expect("loop should recover");

        assert_eq!(outcome.final_text, "recovered");
        let tool_msg = &messages[2];
        assert_eq!(tool_msg.role, Role::Tool);
    }

    #[tokio::test]
    async fn loop_aborts_on_max_turns() {
        // Every turn calls echo again → never terminates; must hit max_turns.
        let repeat = || vec![assistant_tool_use("c", "echo", json!({ "text": "x" }))];
        let provider = MockProvider::new(vec![repeat(), repeat(), repeat(), repeat()]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("loop forever")];

        let result = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig {
                max_turns: 3,
                thinking: ThinkingLevel::default(),
            },
            &CancellationToken::new(),
        )
        .await;

        assert!(result.is_err(), "must abort on max_turns");
    }

    #[tokio::test]
    async fn loop_respects_cancellation() {
        // Cancel before starting.
        let provider = MockProvider::new(vec![vec![assistant_text("never reached")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![];
        let model = Model::new("mock/test");
        let mut messages = vec![user("hi")];
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &cancel,
        )
        .await;

        assert!(matches!(result, Err(CoreError::Cancelled(_))));
    }
}

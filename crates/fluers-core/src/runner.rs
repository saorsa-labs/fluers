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
use crate::model::{Model, ModelProvider, ModelRequest, StreamEvent};
use crate::thinking::ThinkingLevel;
use crate::tool::{InvokeContext, Tool, ToolCall, ToolResult};

/// Configuration for a single agent run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Maximum number of model turns before the loop aborts.
    pub max_turns: usize,
    /// Reasoning effort forwarded to the provider.
    pub thinking: ThinkingLevel,
    /// Hard deadline for a single provider `invoke` call, in milliseconds.
    /// `None` disables the per-turn timeout (the outer `cancel` still applies).
    pub turn_timeout_ms: Option<u64>,
    /// Maximum number of tool calls the model may issue in a single turn
    /// before the loop rejects the response. Guards against runaway models.
    pub max_tool_calls_per_turn: usize,
    /// How many tool calls may run in parallel within a turn. `1` ⇒ fully
    /// sequential (deterministic). Results are always appended in the order
    /// the model issued them, regardless of concurrency.
    pub tool_concurrency: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            max_turns: 12,
            thinking: ThinkingLevel::default(),
            turn_timeout_ms: Some(120_000),
            max_tool_calls_per_turn: 10,
            tool_concurrency: 1,
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
/// Budgets (from [`RunConfig`]):
/// - `max_turns` caps total model turns.
/// - `turn_timeout_ms` caps each provider `invoke`.
/// - `max_tool_calls_per_turn` rejects runaway responses.
///
/// Concurrency: when `tool_concurrency > 1`, tool calls within a turn run on
/// a `JoinSet` with the configured cap; results are always appended in the
/// order the model issued them. `tool_concurrency == 1` is sequential.
///
/// Cancellation: the loop checks `cancel.is_cancelled()` between turns and
/// composes it into each tool call.
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
        // Compose the per-turn timeout with the caller's cancellation token.
        let response =
            invoke_with_budget(provider, request, config.turn_timeout_ms, cancel).await?;
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

        // Reject runaway responses before executing anything.
        if tool_calls.len() > config.max_tool_calls_per_turn {
            return Err(CoreError::ModelResponse(format!(
                "model issued {} tool calls in one turn (max {})",
                tool_calls.len(),
                config.max_tool_calls_per_turn
            )));
        }

        // Execute the turn's tool calls (sequential or bounded-parallel) and
        // append a Tool message per call, in the original order.
        let results = execute_tool_calls(tools, &tool_calls, cancel, config.tool_concurrency).await;
        for (i, (id, _call)) in tool_calls.iter().enumerate() {
            let result = &results[i];
            let tool_msg = AgentMessage {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: id.clone(),
                    content: serde_json::to_value(result)
                        .unwrap_or_else(|_| serde_json::json!({ "error": "serialize failed" })),
                }],
            };
            messages.push(tool_msg);
        }
    }
}

/// A single turn's streamed events, reassembled into the assistant message +
/// the tool calls it issued. Consumed by [`run_agent_streaming`].
#[derive(Debug, Clone, Default)]
struct StreamedTurn {
    text: String,
    thinking: String,
    tool_calls: Vec<(String, ToolCall)>,
}

/// Reassemble a provider's [`StreamEvent`] stream into a [`StreamedTurn`].
///
/// `on_event` is invoked for every event (so callers can print deltas live);
/// this function still returns the full reassembled turn so the loop can
/// append the assistant message and execute tools.
async fn collect_streamed_turn(
    stream: crate::model::StreamEventStream,
    on_event: &mut (dyn FnMut(&StreamEvent) + Send),
) -> Result<StreamedTurn> {
    use futures::StreamExt;
    let mut turn = StreamedTurn::default();
    let mut tool_index: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    let mut s = stream;
    while let Some(item) = s.next().await {
        match item {
            Ok(StreamEvent::TextDelta(t)) => {
                on_event(&StreamEvent::TextDelta(t.clone()));
                turn.text.push_str(&t);
            }
            Ok(StreamEvent::ThinkingDelta(t)) => {
                turn.thinking.push_str(&t);
            }
            Ok(StreamEvent::ToolCall(call)) => {
                let id = format!("call_{}", turn.tool_calls.len());
                tool_index.insert(call.name.clone(), turn.tool_calls.len());
                turn.tool_calls.push((id, call));
            }
            Ok(StreamEvent::Done) => break,
            Err(e) => return Err(e),
        }
    }
    let _ = tool_index;
    Ok(turn)
}

/// Streaming variant of [`run_agent`].
///
/// Identical loop semantics (budgets, parallel tools, cancellation) but each
/// provider turn is consumed via [`ModelProvider::stream`] and text deltas are
/// forwarded to `on_event` *as they arrive*. Tool calls are reassembled from
/// the stream before execution. Use this when you want live token-by-token
/// output.
pub async fn run_agent_streaming(
    provider: &dyn ModelProvider,
    tools: &[Arc<dyn Tool>],
    messages: &mut Vec<AgentMessage>,
    model: &Model,
    config: &RunConfig,
    cancel: &CancellationToken,
    on_event: &mut (dyn FnMut(&StreamEvent) + Send),
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
        // Stream the turn, reassembling into an assistant message + tool calls.
        let stream = provider.stream(request);
        let turn = collect_streamed_turn(stream, on_event).await?;

        // Build the assistant message from the reassembled turn.
        let mut content: Vec<ContentBlock> = Vec::new();
        if !turn.text.is_empty() {
            content.push(ContentBlock::Text { text: turn.text });
        }
        for (id, call) in &turn.tool_calls {
            content.push(ContentBlock::ToolUse {
                id: id.clone(),
                call: call.clone(),
            });
        }
        messages.push(AgentMessage {
            role: Role::Assistant,
            content,
        });

        if turn.tool_calls.is_empty() {
            let final_text = extract_final_text(messages);
            return Ok(RunOutcome { turns, final_text });
        }
        if turn.tool_calls.len() > config.max_tool_calls_per_turn {
            return Err(CoreError::ModelResponse(format!(
                "model issued {} tool calls in one turn (max {})",
                turn.tool_calls.len(),
                config.max_tool_calls_per_turn
            )));
        }

        let owned_calls: Vec<(String, ToolCall)> = turn.tool_calls.clone();
        let results =
            execute_tool_calls(tools, &owned_calls, cancel, config.tool_concurrency).await;
        for (i, (id, _call)) in owned_calls.iter().enumerate() {
            let result = &results[i];
            let tool_msg = AgentMessage {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: id.clone(),
                    content: serde_json::to_value(result)
                        .unwrap_or_else(|_| serde_json::json!({ "error": "serialize failed" })),
                }],
            };
            messages.push(tool_msg);
        }
    }
}

/// Execute a single tool call, returning a result even on error (so the
/// model can recover) rather than aborting the whole run.
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

/// Invoke the provider with a per-turn timeout composed with the caller's
/// cancellation token.
async fn invoke_with_budget(
    provider: &dyn ModelProvider,
    request: ModelRequest,
    turn_timeout_ms: Option<u64>,
    cancel: &CancellationToken,
) -> Result<crate::model::ModelResponse> {
    // Fast-path cancellation check.
    if cancel.is_cancelled() {
        return Err(CoreError::Cancelled("turn cancelled before invoke".into()));
    }
    let invoke_fut = provider.invoke(request);
    match turn_timeout_ms {
        Some(ms) => {
            let timeout = tokio::time::timeout(std::time::Duration::from_millis(ms), invoke_fut);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    Err(CoreError::Cancelled("turn cancelled during invoke".into()))
                }
                res = timeout => {
                    res.map_err(|_| {
                        CoreError::Cancelled(format!(
                            "turn timed out after {ms}ms"
                        ))
                    })?
                }
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    Err(CoreError::Cancelled("turn cancelled during invoke".into()))
                }
                res = invoke_fut => res,
            }
        }
    }
}

/// Execute all tool calls for a turn, returning results in the *original*
/// order regardless of concurrency.
///
/// - `tool_concurrency <= 1` ⇒ sequential (deterministic, the default).
/// - `tool_concurrency > 1` ⇒ bounded-parallel on a `JoinSet`; each task is
///   handed its own child of the caller's `CancellationToken`.
async fn execute_tool_calls(
    tools: &[Arc<dyn Tool>],
    calls: &[(String, ToolCall)],
    cancel: &CancellationToken,
    tool_concurrency: usize,
) -> Vec<ToolResult> {
    if tool_concurrency <= 1 {
        let mut out = Vec::with_capacity(calls.len());
        for (id, call) in calls {
            out.push(execute_tool_call(tools, id, call, cancel).await);
        }
        return out;
    }

    // Bounded-parallel path. Spawn one task per call, tagged with its index.
    use tokio::task::JoinSet;
    let mut set: JoinSet<(usize, ToolResult)> = JoinSet::new();
    for (i, (id, call)) in calls.iter().enumerate() {
        // Find the tool by name now (cheap) so the task owns an `Arc<dyn Tool>`.
        let tool = tools
            .iter()
            .find(|t| t.definition().name == call.name)
            .cloned();
        let ctx_cancel = cancel.child_token();
        let ctx = InvokeContext {
            tool_call_id: id.clone(),
            cancel: ctx_cancel,
        };
        let input = call.input.clone();
        let id_owned = id.clone();
        set.spawn(async move {
            let result = match tool {
                Some(t) => match t.execute(ctx, input).await {
                    Ok(r) => r,
                    Err(err) => error_result(&err.to_string()),
                },
                None => error_result(&format!("unknown tool: `{id_owned}`")),
            };
            (i, result)
        });
        // Cap concurrency by waiting for a slot to clear.
        while set.len() >= tool_concurrency {
            // We must not stall forever if a task panics; JoinSet aborts on drop.
            if set.join_next().await.is_none() {
                break;
            }
        }
    }
    // Collect and re-order by original index.
    let mut indexed: Vec<(usize, ToolResult)> = Vec::with_capacity(calls.len());
    while let Some(res) = set.join_next().await {
        if let Ok(pair) = res {
            indexed.push(pair);
        }
    }
    indexed.sort_by_key(|(i, _)| *i);
    indexed.into_iter().map(|(_, r)| r).collect()
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
                ..RunConfig::default()
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

    /// A provider that sleeps for a fixed duration before each response,
    /// used to exercise `turn_timeout_ms`.
    struct SlowProvider {
        delay_ms: u64,
        responses: std::sync::Mutex<std::collections::VecDeque<ModelResponse>>,
    }

    impl SlowProvider {
        fn new(delay_ms: u64, responses: Vec<Vec<AgentMessage>>) -> Self {
            let responses = responses
                .into_iter()
                .map(|m| ModelResponse { messages: m })
                .collect();
            Self {
                delay_ms,
                responses: std::sync::Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl ModelProvider for SlowProvider {
        async fn invoke(&self, _request: ModelRequest) -> Result<ModelResponse> {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
            let next = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ModelResponse { messages: vec![] });
            Ok(next)
        }
    }

    #[tokio::test]
    async fn turn_timeout_aborts_slow_provider() {
        // Provider sleeps 500ms; turn timeout is 100ms.
        let provider = SlowProvider::new(500, vec![vec![assistant_text("too slow")]]);
        let model = Model::new("mock/test");
        let mut messages = vec![user("hi")];
        let config = RunConfig {
            turn_timeout_ms: Some(100),
            ..RunConfig::default()
        };

        let result = run_agent(
            &provider,
            &[],
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(result, Err(CoreError::Cancelled(_))),
            "expected cancelled, got {result:?}"
        );
    }

    #[tokio::test]
    async fn max_tool_calls_per_turn_rejects_runaway_response() {
        // Model issues 5 tool calls in one turn; cap is 2.
        let runaway: Vec<AgentMessage> = (0..5)
            .map(|i| assistant_tool_use(&format!("c{i}"), "echo", json!({ "text": "x" })))
            .collect();
        let provider = MockProvider::new(vec![runaway]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call many tools")];
        let config = RunConfig {
            max_tool_calls_per_turn: 2,
            ..RunConfig::default()
        };

        let result = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
        )
        .await;

        assert!(result.is_err(), "runaway tool calls must be rejected");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max"), "error should mention the cap: {err}");
    }

    /// A tool that records the order in which its invocations *complete*.
    struct OrderingTool {
        name: String,
        delay_ms: u64,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Tool for OrderingTool {
        fn definition(&self) -> crate::tool::ToolDefinition {
            crate::tool::ToolDefinition {
                name: self.name.clone(),
                label: "Ordering".into(),
                description: "Records completion order.".into(),
                parameters: crate::tool::ParameterSchema::default(),
            }
        }

        async fn execute(&self, _ctx: InvokeContext, input: Value) -> Result<ToolResult> {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
            self.log.lock().unwrap().push(
                input
                    .get("tag")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string(),
            );
            Ok(ToolResult {
                content: vec![json!({ "type": "text", "text": "ok" })],
                details: None,
            })
        }
    }

    #[tokio::test]
    async fn parallel_tool_calls_preserve_result_order() {
        // Two tool calls in one turn. The first is slow, the second fast.
        // With concurrency > 1 they finish out of order, but the appended
        // Tool messages must remain in the model's issued order.
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let tools: Vec<Arc<dyn Tool>> = vec![
            Arc::new(OrderingTool {
                name: "slow".into(),
                delay_ms: 60,
                log: log.clone(),
            }),
            Arc::new(OrderingTool {
                name: "fast".into(),
                delay_ms: 5,
                log: log.clone(),
            }),
        ];
        let turn = vec![
            assistant_tool_use("c1", "slow", json!({ "tag": "slow" })),
            assistant_tool_use("c2", "fast", json!({ "tag": "fast" })),
        ];
        let provider = MockProvider::new(vec![turn, vec![assistant_text("done")]]);
        let model = Model::new("mock/test");
        let mut messages = vec![user("call both")];
        let config = RunConfig {
            tool_concurrency: 4,
            ..RunConfig::default()
        };

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
        )
        .await
        .expect("loop should complete");

        assert_eq!(outcome.final_text, "done");

        // The completion log is [fast, slow] (fast finished first), proving
        // they actually ran in parallel rather than sequentially.
        let completed = log.lock().unwrap().clone();
        assert_eq!(
            completed,
            vec!["fast", "slow"],
            "tools must have run concurrently: {completed:?}"
        );

        // But the appended Tool messages must be in issued order: c1 then c2.
        let tool_ids: Vec<String> = messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .filter_map(|m| match &m.content[0] {
                ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            tool_ids,
            vec!["c1", "c2"],
            "results must be appended in issued order: {tool_ids:?}"
        );
    }
}

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
use crate::event::{RunEvent, RunHooks};
use crate::message::{AgentMessage, ContentBlock, Role};
use crate::model::{Model, ModelProvider, ModelRequest, StreamEvent};
use crate::policy::{PolicyVerdict, ToolPolicy};
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

/// A sink notified after each turn's messages are appended to the history.
///
/// This is the per-turn **seam** that lets a coordinator (in
/// `fluers-runtime`) persist a session, emit events, or snapshot state
/// between turns — *without* `fluers-core` depending on any of those
/// subsystems. It keeps the loop-home decision intact: the pure turn-loop
/// stays in `fluers-core`; the coordinator that drives persistence/events
/// lives in `fluers-runtime`.
///
/// The sink is `await`ed inside the loop after each turn's messages (both
/// the assistant turn and the tool results) are appended, so persistence of
/// turn *N* completes before turn *N+1* begins. That ordering is what makes
/// "resume-after-kill" faithful: the file on disk always reflects at least
/// all completed turns.
#[async_trait::async_trait]
pub trait TurnSink: Send + Sync {
    /// Called after turn `turn` (1-indexed) with the full message history so
    /// far. Returning `Err` aborts the run with that error.
    async fn after_turn(&self, turn: usize, messages: &[AgentMessage]) -> Result<()>;
}

/// A [`TurnSink`] that fans a turn out to multiple inner sinks, **in order**.
///
/// `run_agent` accepts only one `Option<&dyn TurnSink>`. When two concerns
/// both need per-turn observation (e.g. `SessionRunner` for persistence and
/// a memory sink for semantic recall), wrap them in a `FanoutTurnSink`. The
/// sinks are awaited sequentially: sink *N* completes before sink *N+1* runs.
///
/// Error semantics: the first sink to return `Err` aborts the remaining
/// sinks and propagates. Sinks that should be **fail-open** (never break the
/// run on their own errors — e.g. an optional memory store) must swallow their
/// own errors inside [`TurnSink::after_turn`] rather than returning `Err`.
///
/// Construct with [`FanoutTurnSink::new`] / [`FanoutTurnSink::push`].
pub struct FanoutTurnSink {
    sinks: Vec<Box<dyn TurnSink>>,
}

impl FanoutTurnSink {
    /// Create an empty fanout sink.
    #[must_use]
    pub fn new() -> Self {
        Self { sinks: Vec::new() }
    }

    /// Append an inner sink. Sinks run in insertion order.
    #[must_use]
    pub fn push(mut self, sink: Box<dyn TurnSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    /// Number of inner sinks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sinks.len()
    }

    /// Whether there are no inner sinks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }
}

impl Default for FanoutTurnSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl TurnSink for FanoutTurnSink {
    async fn after_turn(&self, turn: usize, messages: &[AgentMessage]) -> Result<()> {
        for sink in &self.sinks {
            sink.after_turn(turn, messages).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod fanout_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A sink that records every call and optionally fails.
    struct RecordingSink {
        calls: Arc<Mutex<Vec<usize>>>,
        fail_at: Option<usize>,
    }

    #[async_trait::async_trait]
    impl TurnSink for RecordingSink {
        async fn after_turn(&self, turn: usize, _messages: &[AgentMessage]) -> Result<()> {
            self.calls.lock().expect("lock poisoned").push(turn);
            if self.fail_at == Some(turn) {
                return Err(crate::error::CoreError::Transport(format!(
                    "injected failure at turn {turn}"
                )));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn fanout_calls_sinks_in_order() {
        let calls_a = Arc::new(Mutex::new(Vec::new()));
        let calls_b = Arc::new(Mutex::new(Vec::new()));
        let fanout = FanoutTurnSink::new()
            .push(Box::new(RecordingSink {
                calls: calls_a.clone(),
                fail_at: None,
            }))
            .push(Box::new(RecordingSink {
                calls: calls_b.clone(),
                fail_at: None,
            }));
        TurnSink::after_turn(&fanout, 1, &[]).await.unwrap();
        assert_eq!(*calls_a.lock().unwrap(), vec![1]);
        assert_eq!(*calls_b.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn fanout_propagates_error_and_stops() {
        // First sink fails at turn 2; the second sink must not be called.
        let calls_a = Arc::new(Mutex::new(Vec::new()));
        let calls_b = Arc::new(Mutex::new(Vec::new()));
        let fanout = FanoutTurnSink::new()
            .push(Box::new(RecordingSink {
                calls: calls_a.clone(),
                fail_at: Some(2),
            }))
            .push(Box::new(RecordingSink {
                calls: calls_b.clone(),
                fail_at: None,
            }));
        let _ = TurnSink::after_turn(&fanout, 2, &[]).await;
        assert_eq!(*calls_a.lock().unwrap(), vec![2]);
        assert!(calls_b.lock().unwrap().is_empty(), "second sink ran");
    }
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
    hooks: &RunHooks<'_>,
) -> Result<RunOutcome> {
    hooks.emit_event(|sid| RunEvent::SessionStarted { session: sid });
    let mut turns = 0usize;
    loop {
        if cancel.is_cancelled() {
            hooks.emit_event(|sid| crate::event::run_failed(sid, "cancelled"));
            return Err(CoreError::Cancelled("agent run cancelled".into()));
        }
        if turns >= config.max_turns {
            let msg = format!(
                "max_turns ({}) exceeded — the model kept calling tools",
                config.max_turns
            );
            hooks.emit_event(|sid| crate::event::run_failed(sid, msg.clone()));
            return Err(CoreError::ModelResponse(msg));
        }
        turns += 1;
        hooks.emit_event(|sid| RunEvent::TurnStarted {
            session: sid,
            turn: turns,
        });

        let request = ModelRequest {
            model: model.clone(),
            messages: messages.clone(),
            tools: tools.iter().map(|t| t.definition()).collect(),
            thinking: config.thinking,
            params: Default::default(),
        };
        hooks.emit_event(|sid| RunEvent::ModelStarted {
            session: sid,
            turn: turns,
            model: model.id.clone(),
        });
        // Compose the per-turn timeout with the caller's cancellation token.
        let response =
            match invoke_with_budget(provider, request, config.turn_timeout_ms, cancel).await {
                Ok(r) => r,
                Err(e) => {
                    hooks.emit_event(|sid| crate::event::run_failed(sid, e.to_string()));
                    return Err(e);
                }
            };
        hooks.emit_event(|sid| RunEvent::ModelFinished {
            session: sid,
            turn: turns,
        });
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
            // Notify the sink so the final state is persisted before returning.
            if let Some(sink) = hooks.turn_sink {
                sink.after_turn(turns, messages).await?;
            }
            hooks.emit_event(|sid| RunEvent::TurnFinished {
                session: sid,
                turn: turns,
            });
            return Ok(RunOutcome { turns, final_text });
        }

        // Reject runaway responses before executing anything.
        if tool_calls.len() > config.max_tool_calls_per_turn {
            let msg = format!(
                "model issued {} tool calls in one turn (max {})",
                tool_calls.len(),
                config.max_tool_calls_per_turn
            );
            hooks.emit_event(|sid| crate::event::run_failed(sid, msg.clone()));
            return Err(CoreError::ModelResponse(msg));
        }

        // Emit ToolStarted for each call, then execute.
        for (id, call) in &tool_calls {
            hooks.emit_event(|sid| RunEvent::ToolStarted {
                session: sid,
                turn: turns,
                tool: call.name.clone(),
                call_id: id.clone(),
            });
        }

        // Execute the turn's tool calls (sequential or bounded-parallel) and
        // append a Tool message per call, in the original order. The optional
        // policy hook is consulted before each call (see `execute_tool_calls`).
        let results = execute_tool_calls(
            tools,
            &tool_calls,
            cancel,
            config.tool_concurrency,
            hooks.policy,
        )
        .await;

        // Emit ToolFinished and append tool-result messages.
        for (i, (id, call)) in tool_calls.iter().enumerate() {
            let result = &results[i];
            let ok = tool_result_ok(result);
            hooks.emit_event(|sid| RunEvent::ToolFinished {
                session: sid,
                turn: turns,
                tool: call.name.clone(),
                call_id: id.clone(),
                ok,
            });
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
        // End of turn: notify the sink so the coordinator can persist/observe
        // before the next turn begins. Persistence of turn N must complete
        // before turn N+1 starts — this is what makes resume-after-kill faithful.
        if let Some(sink) = hooks.turn_sink {
            sink.after_turn(turns, messages).await?;
        }
        hooks.emit_event(|sid| RunEvent::TurnFinished {
            session: sid,
            turn: turns,
        });
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
                // Streaming tool calls are assigned synthetic `call_N` ids in
                // arrival order. The provider already buffers full argument
                // strings before emitting, so no incremental reassembly here.
                let id = format!("call_{}", turn.tool_calls.len());
                turn.tool_calls.push((id, call));
            }
            Ok(StreamEvent::Done) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(turn)
}

/// Streaming variant of [`run_agent`].
///
/// Identical loop semantics (budgets, parallel tools, cancellation) but each
/// provider turn is consumed via [`ModelProvider::stream`] and text deltas are
/// forwarded to `on_event` *as they arrive*. Tool calls are reassembled from
/// the stream before execution. Use this when you want live token-by-token
/// output.
#[allow(clippy::too_many_arguments)]
pub async fn run_agent_streaming(
    provider: &dyn ModelProvider,
    tools: &[Arc<dyn Tool>],
    messages: &mut Vec<AgentMessage>,
    model: &Model,
    config: &RunConfig,
    cancel: &CancellationToken,
    on_event: &mut (dyn FnMut(&StreamEvent) + Send),
    hooks: &RunHooks<'_>,
) -> Result<RunOutcome> {
    hooks.emit_event(|sid| RunEvent::SessionStarted { session: sid });
    let mut turns = 0usize;
    loop {
        if cancel.is_cancelled() {
            hooks.emit_event(|sid| crate::event::run_failed(sid, "cancelled"));
            return Err(CoreError::Cancelled("agent run cancelled".into()));
        }
        if turns >= config.max_turns {
            let msg = format!(
                "max_turns ({}) exceeded — the model kept calling tools",
                config.max_turns
            );
            hooks.emit_event(|sid| crate::event::run_failed(sid, msg.clone()));
            return Err(CoreError::ModelResponse(msg));
        }
        turns += 1;
        hooks.emit_event(|sid| RunEvent::TurnStarted {
            session: sid,
            turn: turns,
        });

        let request = ModelRequest {
            model: model.clone(),
            messages: messages.clone(),
            tools: tools.iter().map(|t| t.definition()).collect(),
            thinking: config.thinking,
            params: Default::default(),
        };
        hooks.emit_event(|sid| RunEvent::ModelStarted {
            session: sid,
            turn: turns,
            model: model.id.clone(),
        });
        // Stream the turn, reassembling into an assistant message + tool calls.
        let stream = provider.stream(request);
        let turn = match collect_streamed_turn(stream, on_event).await {
            Ok(t) => t,
            Err(e) => {
                hooks.emit_event(|sid| crate::event::run_failed(sid, e.to_string()));
                return Err(e);
            }
        };
        hooks.emit_event(|sid| RunEvent::ModelFinished {
            session: sid,
            turn: turns,
        });

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
            // Notify the sink so the final state is persisted before returning.
            if let Some(sink) = hooks.turn_sink {
                sink.after_turn(turns, messages).await?;
            }
            hooks.emit_event(|sid| RunEvent::TurnFinished {
                session: sid,
                turn: turns,
            });
            return Ok(RunOutcome { turns, final_text });
        }
        if turn.tool_calls.len() > config.max_tool_calls_per_turn {
            let msg = format!(
                "model issued {} tool calls in one turn (max {})",
                turn.tool_calls.len(),
                config.max_tool_calls_per_turn
            );
            hooks.emit_event(|sid| crate::event::run_failed(sid, msg.clone()));
            return Err(CoreError::ModelResponse(msg));
        }

        let owned_calls: Vec<(String, ToolCall)> = turn.tool_calls.clone();

        // Emit ToolStarted for each call, then execute.
        for (id, call) in &owned_calls {
            hooks.emit_event(|sid| RunEvent::ToolStarted {
                session: sid,
                turn: turns,
                tool: call.name.clone(),
                call_id: id.clone(),
            });
        }

        let results = execute_tool_calls(
            tools,
            &owned_calls,
            cancel,
            config.tool_concurrency,
            hooks.policy,
        )
        .await;

        // Emit ToolFinished and append tool-result messages.
        for (i, (id, call)) in owned_calls.iter().enumerate() {
            let result = &results[i];
            let ok = tool_result_ok(result);
            hooks.emit_event(|sid| RunEvent::ToolFinished {
                session: sid,
                turn: turns,
                tool: call.name.clone(),
                call_id: id.clone(),
                ok,
            });
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
        // End of turn: notify the sink (see `run_agent` for rationale).
        if let Some(sink) = hooks.turn_sink {
            sink.after_turn(turns, messages).await?;
        }
        hooks.emit_event(|sid| RunEvent::TurnFinished {
            session: sid,
            turn: turns,
        });
    }
}

/// Maximum length of a panic message we surface to the model (a panic payload
/// may carry arbitrarily large text). Matches `event::ERROR_SUMMARY_MAX_CHARS`.
const PANIC_SUMMARY_MAX_CHARS: usize = 200;

/// Render a panic payload into a bounded, model-safe summary string.
///
/// Panic payloads are `Box<dyn Any + Send>`; the common cases are `&'static str`
/// and `String`. Anything else falls back to a generic marker.
fn summarize_panic(payload: &Box<dyn std::any::Any + Send>) -> String {
    let raw = payload
        .downcast_ref::<&'static str>()
        .map(std::string::ToString::to_string)
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    let chars: Vec<char> = raw.chars().collect();
    if chars.len() <= PANIC_SUMMARY_MAX_CHARS {
        raw
    } else {
        let truncated: String = chars
            .into_iter()
            .take(PANIC_SUMMARY_MAX_CHARS - 1)
            .collect();
        format!("{truncated}…")
    }
}

/// Execute a single tool call, returning a result even on error (so the
/// model can recover) rather than aborting the whole run.
///
/// **Panic safety:** a panic inside `tool.execute` is caught and converted to a
/// model-visible `Error:` result, matching the parallel path's `JoinSet`
/// behaviour. This keeps a buggy / hostile tool from aborting the whole run on
/// the default (`tool_concurrency == 1`) path. `catch_unwind` is best-effort:
/// it catches unwinding panics but not aborts (e.g. `panic = "abort"`, OOM,
/// SIGSEGV).
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
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;
    match AssertUnwindSafe(tool.execute(ctx, call.input.clone()))
        .catch_unwind()
        .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => error_result(&err.to_string()),
        Err(payload) => {
            let summary = summarize_panic(&payload);
            // Log only structural metadata: panic payloads may contain user
            // data/secrets, and `tracing` can export to a remote collector.
            // The bounded `summary` goes only into the model-visible result
            // (the agent's own context), never into telemetry.
            tracing::warn!(
                tool = %call.name,
                call_id = %id,
                "tool panicked; converted to model-visible error result"
            );
            error_result(&format!("tool `{}` panicked: {summary}", call.name))
        }
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

/// The result of consulting the [`ToolPolicy`] for one call.
enum PolicyOutcome {
    /// The call is cleared to execute.
    Execute,
    /// The call is denied; carry the model-visible error result to append in
    /// place of executing the tool.
    Denied(ToolResult),
}

/// Consult the optional policy hook for a single call.
///
/// `None` policy ⇒ allow-all. `Confirm` is treated as `Allow` with a logged
/// note (a confirmation channel is out of scope for the loop itself). `Deny`
/// yields a model-visible error result and the tool is not executed.
async fn policy_check(
    policy: Option<&dyn ToolPolicy>,
    id: &str,
    call: &ToolCall,
    cancel: &CancellationToken,
) -> PolicyOutcome {
    let Some(policy) = policy else {
        return PolicyOutcome::Execute;
    };
    let ctx = InvokeContext {
        tool_call_id: id.to_string(),
        cancel: cancel.clone(),
    };
    match policy.check(&call.name, &call.input, &ctx).await {
        PolicyVerdict::Allow => PolicyOutcome::Execute,
        PolicyVerdict::Confirm(reason) => {
            tracing::info!(
                tool = %call.name,
                call_id = %id,
                "tool policy returned Confirm; treating as Allow for this run: {reason}"
            );
            PolicyOutcome::Execute
        }
        PolicyVerdict::Deny(reason) => {
            PolicyOutcome::Denied(error_result(&format!("denied by policy: {reason}")))
        }
    }
}

/// Execute all tool calls for a turn, returning results in the *original*
/// order regardless of concurrency.
///
/// - `tool_concurrency <= 1` ⇒ sequential (deterministic, the default).
/// - `tool_concurrency > 1` ⇒ bounded-parallel on a `JoinSet`; each task is
///   handed its own child of the caller's `CancellationToken`.
///
/// When `policy` is set it is consulted **before** each call executes; a
/// denied call is never dispatched and its slot carries a model-visible error
/// result instead.
async fn execute_tool_calls(
    tools: &[Arc<dyn Tool>],
    calls: &[(String, ToolCall)],
    cancel: &CancellationToken,
    tool_concurrency: usize,
    policy: Option<&dyn ToolPolicy>,
) -> Vec<ToolResult> {
    if tool_concurrency <= 1 {
        let mut out = Vec::with_capacity(calls.len());
        for (id, call) in calls {
            let result = match policy_check(policy, id, call, cancel).await {
                PolicyOutcome::Execute => execute_tool_call(tools, id, call, cancel).await,
                PolicyOutcome::Denied(result) => result,
            };
            out.push(result);
        }
        return out;
    }

    // Bounded-parallel path. Spawn one task per call, tagged with its index.
    use tokio::task::JoinSet;
    // Initialized up here (not at collection time) so the throttling loop can
    // record early-completing tasks without dropping them. (Previously the
    // throttle `join_next` discarded its drained result — a real bug that lost
    // results whenever `calls.len() > tool_concurrency`.)
    let mut indexed: Vec<Option<ToolResult>> = (0..calls.len()).map(|_| None).collect();
    let mut set: JoinSet<(usize, ToolResult)> = JoinSet::new();
    for (i, (id, call)) in calls.iter().enumerate() {
        // Consult the policy hook before dispatching. A denied call fills its
        // slot with an error result and is never spawned. (Awaited here, not
        // inside the spawned task, so the borrowed `&dyn ToolPolicy` need not
        // be `'static`.)
        if let PolicyOutcome::Denied(result) = policy_check(policy, id, call, cancel).await {
            if let Some(slot) = indexed.get_mut(i) {
                *slot = Some(result);
            }
            continue;
        }
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
        let call_name = call.name.clone();
        set.spawn(async move {
            // Catch panics INSIDE the task so the original index `i` is
            // preserved and the bounded summary reaches the model. (Previously
            // the task could panic, and JoinSet::join_next would return Err
            // with no index — record_join_result would fill the first empty
            // slot, attributing the panic to the wrong call_id.)
            let result = match tool {
                Some(t) => {
                    use futures::FutureExt;
                    use std::panic::AssertUnwindSafe;
                    match AssertUnwindSafe(t.execute(ctx, input)).catch_unwind().await {
                        Ok(Ok(r)) => r,
                        Ok(Err(err)) => error_result(&err.to_string()),
                        Err(payload) => {
                            let summary = summarize_panic(&payload);
                            tracing::warn!(
                                tool = %call_name,
                                call_id = %id_owned,
                                "tool panicked; converted to model-visible error result"
                            );
                            error_result(&format!("tool `{call_name}` panicked: {summary}"))
                        }
                    }
                }
                None => error_result(&format!("unknown tool: `{id_owned}`")),
            };
            (i, result)
        });
        // Cap concurrency by waiting for a slot to clear. Every drained result
        // must be recorded (the throttle and final-collection loops share one
        // recorder so nothing is dropped).
        while set.len() >= tool_concurrency {
            let res = set.join_next().await;
            if res.is_none() {
                break; // set drained (all spawned tasks already completed)
            }
            record_join_result(res, &mut indexed);
        }
    }
    // Collect any remaining results, re-ordered by original index.
    //
    // A spawned task can panic (JoinSet swallows panics, returning `Err` from
    // `join_next`). We must still return exactly `calls.len()` results so the
    // caller's `results[i]` indexing can never panic. Missing/failed slots are
    // filled with an error result.
    while let Some(res) = set.join_next().await {
        record_join_result(Some(res), &mut indexed);
    }
    indexed
        .into_iter()
        .map(|opt| opt.unwrap_or_else(|| error_result("tool task produced no result")))
        // Order is already correct (indexed by position); this is a no-op guard.
        .collect()
}

/// Record a `JoinSet::join_next()` outcome into the indexed results vec. Used
/// by both the throttling loop and the final collection so no result is
/// dropped. On a panic/cancel (`Err`) we fill the first still-empty slot
/// (`join_next` on a panic doesn't report the index).
fn record_join_result(
    res: Option<std::result::Result<(usize, ToolResult), tokio::task::JoinError>>,
    indexed: &mut [Option<ToolResult>],
) {
    match res {
        Some(Ok((i, result))) => {
            if let Some(slot) = indexed.get_mut(i) {
                *slot = Some(result);
            }
        }
        Some(Err(join_err)) => {
            let slot = indexed.iter().position(Option::is_none).unwrap_or(0);
            if let Some(s) = indexed.get_mut(slot) {
                *s = Some(error_result(&format!("tool task failed: {join_err}")));
            }
        }
        None => {}
    }
}

/// Build a `ToolResult` carrying a single error text block.
fn error_result(message: &str) -> ToolResult {
    ToolResult {
        content: vec![serde_json::json!({ "type": "text", "text": format!("Error: {message}") })],
        details: None,
    }
}

/// Heuristic: a [`ToolResult`] is "ok" unless any text block starts with
/// `"Error:"`. This matches the [`error_result`] convention. (A future
/// refactor should add an explicit `ok` field to `ToolResult`.)
fn tool_result_ok(result: &ToolResult) -> bool {
    !result.content.iter().any(|c| {
        c.get("text")
            .and_then(|t| t.as_str())
            .is_some_and(|t| t.starts_with("Error:"))
    })
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
            &RunHooks::default(),
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
            &RunHooks::default(),
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
            &RunHooks::default(),
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
            &RunHooks::default(),
        )
        .await;

        assert!(result.is_err(), "must abort on max_turns");
    }

    /// A policy that denies every tool call (generic; no Fae types).
    struct DenyAllPolicy;

    #[async_trait]
    impl ToolPolicy for DenyAllPolicy {
        async fn check(&self, _tool: &str, _input: &Value, _ctx: &InvokeContext) -> PolicyVerdict {
            PolicyVerdict::Deny("blocked in test".into())
        }
    }

    #[tokio::test]
    async fn policy_deny_blocks_tool_but_run_continues() {
        // Turn 1: model calls echo. The policy denies it: the tool must NOT
        // execute, a denial error result is appended, and the loop continues
        // to turn 2's final text — matching the unknown-tool recovery path.
        let provider = MockProvider::new(vec![
            vec![assistant_tool_use(
                "c1",
                "echo",
                json!({ "text": "secret" }),
            )],
            vec![assistant_text("done")],
        ]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call echo")];
        let policy = DenyAllPolicy;
        let hooks = RunHooks {
            policy: Some(&policy),
            ..RunHooks::default()
        };

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
            &hooks,
        )
        .await
        .expect("loop completes despite denial");

        assert_eq!(outcome.final_text, "done");
        // The tool result must be the denial — proving the tool never ran.
        let s = match &messages[2].content[0] {
            ContentBlock::ToolResult { content, .. } => content.to_string(),
            other => panic!("expected ToolResult, got {other:?}"),
        };
        assert!(s.contains("denied by policy"), "expected denial, got: {s}");
        assert!(
            !s.contains("echo: secret"),
            "denied tool must NOT have executed: {s}"
        );
    }

    #[tokio::test]
    async fn policy_none_is_allow_all() {
        // Default hooks (policy: None) must behave exactly as before: the tool
        // executes and its echoed output reaches the model.
        let provider = MockProvider::new(vec![
            vec![assistant_tool_use("c1", "echo", json!({ "text": "hi" }))],
            vec![assistant_text("done")],
        ]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call echo")];

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
            &RunHooks::default(),
        )
        .await
        .expect("loop completes");
        assert_eq!(outcome.final_text, "done");
        let s = match &messages[2].content[0] {
            ContentBlock::ToolResult { content, .. } => content.to_string(),
            other => panic!("expected ToolResult, got {other:?}"),
        };
        assert!(s.contains("echo: hi"), "tool should have run: {s}");
    }

    /// A tool that records every execution into a shared log. Used to prove a
    /// denied policy call never reaches `execute` — even under the parallel
    /// (`tool_concurrency > 1`) path. (The sequential deny test above inspects
    /// the result text; this one inspects whether `execute` ran at all.)
    struct RecordingTool {
        name: String,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Tool for RecordingTool {
        fn definition(&self) -> crate::tool::ToolDefinition {
            crate::tool::ToolDefinition {
                name: self.name.clone(),
                label: "Recording".into(),
                description: "Records each execution.".into(),
                parameters: crate::tool::ParameterSchema::default(),
            }
        }

        async fn execute(&self, _ctx: InvokeContext, input: Value) -> Result<ToolResult> {
            let tag = input
                .get("tag")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string();
            self.log.lock().expect("lock poisoned").push(tag);
            Ok(ToolResult {
                content: vec![json!({ "type": "text", "text": "ran" })],
                details: None,
            })
        }
    }

    /// Regression guard for the security-critical parallel-path invariant: the
    /// policy hook MUST be consulted under `tool_concurrency > 1`, every denied
    /// call MUST be skipped (its tool never executes), and each denied slot
    /// MUST carry a model-visible denial result so the loop continues.
    ///
    /// (The "checked before `JoinSet::spawn`" placement is enforced by the
    /// borrow checker — the borrowed `&dyn ToolPolicy` is not `'static` and so
    /// cannot move into a spawned task; `policy_check` is awaited outside the
    /// task. This test pins the *behavior* that placement guarantees.)
    #[tokio::test]
    async fn policy_deny_blocks_tools_on_the_parallel_path() {
        // One turn, 3 tool calls, concurrency = 4 (well into the parallel path).
        // DenyAllPolicy refuses every call: none of the 3 tools may execute.
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(RecordingTool {
            name: "rec".into(),
            log: log.clone(),
        })];
        let turn = vec![
            assistant_tool_use("c1", "rec", json!({ "tag": "one" })),
            assistant_tool_use("c2", "rec", json!({ "tag": "two" })),
            assistant_tool_use("c3", "rec", json!({ "tag": "three" })),
        ];
        let provider = MockProvider::new(vec![turn, vec![assistant_text("done")]]);
        let model = Model::new("mock/test");
        let mut messages = vec![user("call all three")];
        let config = RunConfig {
            tool_concurrency: 4,
            ..RunConfig::default()
        };
        let policy = DenyAllPolicy;
        let hooks = RunHooks {
            policy: Some(&policy),
            ..RunHooks::default()
        };

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
            &hooks,
        )
        .await
        .expect("loop completes despite denials");

        assert_eq!(outcome.final_text, "done");

        // Negative control: NO tool executed. If the parallel path skipped the
        // policy check (or checked inside the task after dispatch), the log
        // would be non-empty.
        let executed = log.lock().expect("lock poisoned").clone();
        assert!(
            executed.is_empty(),
            "denied tools must NOT execute on the parallel path: ran {executed:?}"
        );

        // Every denied slot carries a model-visible denial result, in issued
        // order, so the model can recover.
        let results: Vec<String> = messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .filter_map(|m| match &m.content[0] {
                ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    ..
                } => {
                    let text = content.to_string();
                    Some(format!("{tool_use_id}:{text}"))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            results.len(),
            3,
            "all 3 denied calls must produce a result slot: {results:?}"
        );
        for r in &results {
            assert!(
                r.contains("denied by policy"),
                "parallel-path denial must surface to the model: {r}"
            );
        }
        // Slots remain in issued order (c1, c2, c3) — the denial must not
        // reshuffle results under concurrency.
        assert!(
            results[0].starts_with("c1:")
                && results[1].starts_with("c2:")
                && results[2].starts_with("c3:"),
            "denial slots must preserve issued order: {results:?}"
        );
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
            &RunHooks::default(),
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
            &RunHooks::default(),
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
            &RunHooks::default(),
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
            &RunHooks::default(),
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

    /// A tool whose `execute` panics. The parallel path must NOT propagate the
    /// panic; it must fill that slot with an error result and keep going.
    struct PanickingTool;

    #[async_trait]
    impl Tool for PanickingTool {
        fn definition(&self) -> crate::tool::ToolDefinition {
            crate::tool::ToolDefinition {
                name: "boom".into(),
                label: "Boom".into(),
                description: "Always panics.".into(),
                parameters: crate::tool::ParameterSchema::default(),
            }
        }

        async fn execute(&self, _ctx: InvokeContext, _input: Value) -> Result<ToolResult> {
            panic!("deliberate tool panic");
        }
    }

    #[tokio::test]
    async fn parallel_path_survives_a_task_panic() {
        // Two tool calls in one turn: one panics, one succeeds. The loop must
        // not panic; it must append two Tool messages (one error result, one ok).
        let turn = vec![
            assistant_tool_use("c1", "boom", json!({})),
            assistant_tool_use("c2", "echo", json!({ "text": "survived" })),
        ];
        let provider = MockProvider::new(vec![turn, vec![assistant_text("done")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(PanickingTool), Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call both")];
        let config = RunConfig {
            tool_concurrency: 4,
            ..RunConfig::default()
        };

        // This must not panic.
        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
            &RunHooks::default(),
        )
        .await
        .expect("loop must survive a tool panic");

        assert_eq!(outcome.final_text, "done");
        // Exactly two Tool messages appended (one per call), in issued order.
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
            "both results must be present despite the panic: {tool_ids:?}"
        );
    }

    /// The DEFAULT (sequential, `tool_concurrency == 1`) path must ALSO survive
    /// a tool panic — converted to a model-visible `Error:` result, not an
    /// aborting unwind. This is the gap the dev-config-UX red-team flagged:
    /// the parallel path had JoinSet panic isolation, but the sequential path
    /// (the default) did not until `execute_tool_call` grew `catch_unwind`.
    #[tokio::test]
    async fn sequential_path_survives_a_tool_panic() {
        let turn = vec![
            assistant_tool_use("c1", "boom", json!({})),
            assistant_tool_use("c2", "echo", json!({ "text": "survived" })),
        ];
        let provider = MockProvider::new(vec![turn, vec![assistant_text("done")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(PanickingTool), Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call both")];
        // tool_concurrency == 1 → sequential path (the default).
        let config = RunConfig::default();

        // This must not panic.
        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
            &RunHooks::default(),
        )
        .await
        .expect("sequential path must survive a tool panic");
        assert_eq!(outcome.final_text, "done");

        // Both Tool results present, in issued order.
        let results: Vec<&ContentBlock> = messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .flat_map(|m| m.content.iter())
            .collect();
        assert_eq!(results.len(), 2, "both results appended");
        // c1 (the panic) → contains Error: + panicked; c2 (echo) → the echoed text.
        let c1_str = match &results[0] {
            ContentBlock::ToolResult { content, .. } => content.to_string(),
            _ => String::new(),
        };
        assert!(
            c1_str.contains("Error:"),
            "panic must surface as an Error: result, got: {c1_str}"
        );
        assert!(
            c1_str.contains("panicked"),
            "error result should mention the panic: {c1_str}"
        );
    }

    /// Parallel-path panics must be attributed to the CORRECT call_id (not the
    /// first empty slot) and carry the bounded panic summary — matching the
    /// sequential path's quality. Before the fix, the task could panic, and
    /// JoinSet::join_next returned Err with no index, so record_join_result
    /// filled the first empty slot with a generic 'tool task failed' message.
    #[tokio::test]
    async fn parallel_path_panic_preserves_call_id_and_summary() {
        // c1 → boom (panics); c2 → echo (succeeds). Both run in parallel.
        let turn = vec![
            assistant_tool_use("c1", "boom", json!({})),
            assistant_tool_use("c2", "echo", json!({ "text": "ok" })),
        ];
        let provider = MockProvider::new(vec![turn, vec![assistant_text("done")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(PanickingTool), Arc::new(EchoTool)];
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
            &RunHooks::default(),
        )
        .await
        .expect("run survives parallel panic");
        assert_eq!(outcome.final_text, "done");

        // c1 (the panic) must be in the FIRST slot with a 'panicked' summary,
        // NOT a generic 'tool task failed'. c2 (echo) in the second slot.
        let tool_msgs: Vec<(&String, String)> = messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .flat_map(|m| m.content.iter())
            .filter_map(|b| match b {
                ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } => Some((tool_use_id, content.to_string())),
                _ => None,
            })
            .collect();
        assert_eq!(tool_msgs.len(), 2, "both results present");
        // Correct call_id attribution (c1 first, c2 second).
        assert_eq!(tool_msgs[0].0, "c1", "c1 attributed correctly");
        assert_eq!(tool_msgs[1].0, "c2", "c2 attributed correctly");
        // The panic carries the bounded summary (not the generic message).
        assert!(
            tool_msgs[0].1.contains("panicked"),
            "parallel panic should carry bounded summary, got: {}",
            tool_msgs[0].1
        );
        assert!(
            tool_msgs[0].1.contains("Error:"),
            "should be an Error: result, got: {}",
            tool_msgs[0].1
        );
    }

    /// Regression test for a pre-existing bug in the parallel path: the
    /// throttling loop (`while set.len() >= tool_concurrency { join_next }`)
    /// used to **discard** the drained result, so when `calls.len()` exceeded
    /// `tool_concurrency`, early-completing results were lost and the run
    /// ended up with "produced no result" error results in their slots. The
    /// fix records every `join_next` via a shared recorder.
    #[tokio::test]
    async fn parallel_path_keeps_all_results_under_throttling() {
        // 3 calls, concurrency = 2 → the throttle loop must drain-and-record
        // (not drain-and-drop) the first task that completes while the third
        // is waiting for a slot.
        let turn = vec![
            assistant_tool_use("c1", "echo", json!({ "text": "one" })),
            assistant_tool_use("c2", "echo", json!({ "text": "two" })),
            assistant_tool_use("c3", "echo", json!({ "text": "three" })),
        ];
        let provider = MockProvider::new(vec![turn, vec![assistant_text("done")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![Arc::new(EchoTool)];
        let model = Model::new("mock/test");
        let mut messages = vec![user("call all three")];
        let config = RunConfig {
            tool_concurrency: 2,
            ..RunConfig::default()
        };

        let outcome = run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &CancellationToken::new(),
            &RunHooks::default(),
        )
        .await
        .expect("run completes");
        assert_eq!(outcome.final_text, "done");

        // All three Tool results must be present, in issued order, with the
        // correct echoed text in each slot (proving the RIGHT result landed,
        // not a placeholder).
        let results: Vec<String> = messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .flat_map(|m| m.content.iter())
            .filter_map(|b| match b {
                ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } => {
                    let text = content
                        .get("content")
                        .and_then(|c| c.get(0))
                        .and_then(|c| c.get("text"))
                        .and_then(|t| t.as_str())
                        .unwrap_or("<missing>");
                    Some(format!("{tool_use_id}={text}"))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            results,
            vec!["c1=echo: one", "c2=echo: two", "c3=echo: three"],
            "all 3 results must survive throttling, in order, with correct text: {results:?}"
        );
    }

    #[test]
    fn summarize_panic_handles_string_payloads() {
        let p: Box<dyn std::any::Any + Send> = Box::new("boom!".to_string());
        assert_eq!(summarize_panic(&p), "boom!");
    }

    #[test]
    fn summarize_panic_handles_str_payloads() {
        let s: &'static str = "static boom";
        let p: Box<dyn std::any::Any + Send> = Box::new(s);
        assert_eq!(summarize_panic(&p), "static boom");
    }

    #[test]
    fn summarize_panic_bounds_huge_payloads() {
        let huge = "x".repeat(10_000);
        let p: Box<dyn std::any::Any + Send> = Box::new(huge);
        let summary = summarize_panic(&p);
        assert!(
            summary.chars().count() <= PANIC_SUMMARY_MAX_CHARS,
            "summary not bounded: {} chars",
            summary.chars().count()
        );
        assert!(
            summary.ends_with('…'),
            "should end with ellipsis: {summary}"
        );
    }

    #[test]
    fn summarize_panic_falls_back_for_non_string_payloads() {
        let p: Box<dyn std::any::Any + Send> = Box::new(42_i32);
        let summary = summarize_panic(&p);
        assert!(
            summary.contains("non-string"),
            "expected fallback marker: {summary}"
        );
    }

    // ── Event emission tests (MVP 4c) ──

    use crate::event::{EventSink, RunEvent};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    /// A recording sink that collects every emitted event.
    struct RecordingSink {
        events: Arc<Mutex<Vec<RunEvent>>>,
    }

    impl EventSink for RecordingSink {
        fn emit(&self, event: RunEvent) {
            self.events.lock().expect("lock poisoned").push(event);
        }
    }

    #[tokio::test]
    async fn text_only_run_emits_complete_event_sequence() {
        let provider = MockProvider::new(vec![vec![assistant_text("hello")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![];
        let model = Model::new("mock/test");
        let mut messages = vec![user("hi")];
        let sink = Arc::new(Mutex::new(Vec::new()));
        let hooks = RunHooks {
            session_id: Some(Uuid::nil()),
            turn_sink: None,
            event_sink: Some(&RecordingSink {
                events: sink.clone(),
            } as &dyn EventSink),
            policy: None,
        };

        run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
            &hooks,
        )
        .await
        .expect("run");

        let events = sink.lock().expect("lock poisoned").clone();
        // Text-only turn: Session → Turn → Model(S/F) → TurnFinished
        assert!(events
            .iter()
            .any(|e| matches!(e, RunEvent::SessionStarted { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, RunEvent::TurnStarted { turn: 1, .. })));
        assert!(events.iter().any(
            |e| matches!(e, RunEvent::ModelStarted { turn: 1, model, .. } if model == "mock/test")
        ));
        assert!(events
            .iter()
            .any(|e| matches!(e, RunEvent::ModelFinished { turn: 1, .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e, RunEvent::TurnFinished { turn: 1, .. })));
        // No tool events for a text-only turn.
        assert!(!events
            .iter()
            .any(|e| matches!(e, RunEvent::ToolStarted { .. })));
    }

    #[tokio::test]
    async fn tool_run_emits_tool_started_finished() {
        let echo_tool = Arc::new(EchoTool) as Arc<dyn Tool>;
        let tools = vec![echo_tool.clone()];
        let provider = MockProvider::new(vec![
            vec![assistant_tool_use(
                "call-1",
                "echo",
                json!({ "text": "hi" }),
            )],
            vec![assistant_text("done")],
        ]);
        let model = Model::new("mock/test");
        let mut messages = vec![user("echo hi")];
        let sink = Arc::new(Mutex::new(Vec::new()));
        let hooks = RunHooks {
            session_id: Some(Uuid::nil()),
            turn_sink: None,
            event_sink: Some(&RecordingSink {
                events: sink.clone(),
            } as &dyn EventSink),
            policy: None,
        };

        run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
            &hooks,
        )
        .await
        .expect("run");

        let events = sink.lock().expect("lock poisoned").clone();
        // Turn 1 has a tool call → ToolStarted then ToolFinished.
        assert!(
            events.iter().any(|e| matches!(e, RunEvent::ToolStarted { turn: 1, tool, call_id, .. } if tool == "echo" && call_id == "call-1")),
            "missing ToolStarted for echo/call-1"
        );
        assert!(
            events.iter().any(|e| matches!(e, RunEvent::ToolFinished { turn: 1, tool, call_id, ok: true, .. } if tool == "echo" && call_id == "call-1")),
            "missing ToolFinished for echo/call-1"
        );
        // Two turns (tool then text).
        assert!(events
            .iter()
            .any(|e| matches!(e, RunEvent::TurnFinished { turn: 2, .. })));
    }

    #[tokio::test]
    async fn no_events_when_session_id_is_none() {
        let provider = MockProvider::new(vec![vec![assistant_text("hello")]]);
        let tools: Vec<Arc<dyn Tool>> = vec![];
        let model = Model::new("mock/test");
        let mut messages = vec![user("hi")];
        let sink = Arc::new(Mutex::new(Vec::new()));
        let hooks = RunHooks {
            session_id: None, // no session → no events
            turn_sink: None,
            event_sink: Some(&RecordingSink {
                events: sink.clone(),
            } as &dyn EventSink),
            policy: None,
        };

        run_agent(
            &provider,
            &tools,
            &mut messages,
            &model,
            &RunConfig::default(),
            &CancellationToken::new(),
            &hooks,
        )
        .await
        .expect("run");

        assert!(
            sink.lock().expect("lock poisoned").is_empty(),
            "events emitted with no session_id"
        );
    }
}

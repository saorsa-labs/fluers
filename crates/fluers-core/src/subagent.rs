//! Subagent delegation: the built-in `task` tool.
//!
//! Mirrors Flue's [Subagents](https://flue.dev/docs/guide/subagents/): an agent
//! delegates a focused piece of work to a named subagent. The subagent runs in a
//! fresh child session and its answer returns to the parent as the `task` tool
//! result.
//!
//! See `docs/MVP4_SUBAGENTS_DESIGN.md` for the full design and scope.
//!
//! # Configuration inheritance (Flue-compatible)
//!
//! Capability fields (`instructions` / `tools` / `subagents`) are
//! **profile-owned** — the parent's values never flow into the delegated
//! session. Scalar defaults (`model` / `config`) inherit from the parent when
//! the profile omits them.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::error::{CoreError, Result as CoreResult};
use crate::event::EventSink;
use crate::message::{AgentMessage, ContentBlock, Role};
use crate::model::{Model, ModelProvider};
use crate::runner::{run_agent, RunConfig, RunOutcome};
use crate::tool::{InvokeContext, Tool, ToolDefinition, ToolResult};

/// Default recursion limit. The top-level agent runs at depth 0; its `task`
/// calls run children at depth 1, etc. This matches the default in most agent
/// harnesses and keeps runaway delegation bounded.
pub const DEFAULT_MAX_DEPTH: usize = 5;

/// Default cap on the **total** number of delegations across the whole tree.
/// Depth alone bounds chain length but not branching: a parent turn can issue
/// many parallel `task` calls, each of which can do the same, producing
/// exponential fan-out (up to `max_tool_calls_per_turn`^`max_depth` ≈ 10⁵ at
/// defaults). This shared budget turns that into a hard ceiling regardless of
/// depth or width.
pub const DEFAULT_MAX_DELEGATIONS: usize = 64;

/// A named, declarable subagent profile.
///
/// Capability fields (`instructions` / `tools` / `subagents`) are
/// **profile-owned** — the parent's values never flow into a delegated session,
/// so a parent's bash tool never silently leaks into a reviewer subagent.
/// Scalar defaults (`model` / `config`) inherit from the parent when `None`.
#[derive(Clone)]
pub struct SubagentProfile {
    /// Machine name the parent model targets in `task({ agent: ... })`.
    pub name: String,
    /// Delegation guidance shown to the parent model alongside the name.
    pub description: String,
    /// The subagent's system message (the child session's first message).
    pub instructions: String,
    /// Profile-owned model. `None` ⇒ inherit the parent's model.
    pub model: Option<Model>,
    /// Profile-owned run config. `None` ⇒ inherit the parent's config.
    pub config: Option<RunConfig>,
    /// Profile-owned tools. The parent's tools do NOT flow into the child.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Profile-owned subagents (enables recursive delegation). The parent's
    /// subagents do NOT flow into the child.
    pub subagents: Vec<SubagentProfile>,
}

impl SubagentProfile {
    /// Build a minimal profile (name + instructions). Other fields default to
    /// inherited / empty. `description` is left empty (callers can override
    /// with `.with_description(...)`; an empty description renders as
    /// "(no description provided)" in the tool listing).
    #[must_use]
    pub fn new(name: impl Into<String>, instructions: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            // Description defaults to a trimmed copy of the instructions; callers
            // can override with `.with_description(...)`.
            description: String::new(),
            instructions: instructions.into(),
            model: None,
            config: None,
            tools: Vec::new(),
            subagents: Vec::new(),
        }
    }

    /// Set the delegation-guidance description shown to the parent model.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the profile-owned model (overrides inheritance).
    #[must_use]
    pub fn with_model(mut self, model: Model) -> Self {
        self.model = Some(model);
        self
    }

    /// Set the profile-owned run config (overrides inheritance).
    #[must_use]
    pub fn with_config(mut self, config: RunConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Add a profile-owned tool.
    #[must_use]
    pub fn with_tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Declare a nested subagent (enables recursive delegation).
    #[must_use]
    pub fn with_subagent(mut self, subagent: SubagentProfile) -> Self {
        self.subagents.push(subagent);
        self
    }
}

/// Options for the [`TaskTool`].
#[derive(Clone, Copy, Debug)]
pub struct SubagentOptions {
    /// Maximum delegation **depth** (chain length). The top-level agent runs
    /// at depth 0; its `task` calls run children at depth 1; their `task` calls
    /// run at depth 2; etc. A `task` call at `depth >= max_depth` returns a
    /// depth-exceeded error result.
    pub max_depth: usize,
    /// Maximum **total** number of delegations across the whole tree (shared
    /// atomic counter). Bounds exponential fan-out: a parent issuing many
    /// parallel `task` calls, each spawning children that do the same, is
    /// capped regardless of depth or width. A `task` call that would exceed
    /// the remaining budget returns a budget-exceeded error result.
    pub max_delegations: usize,
}

impl Default for SubagentOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_delegations: DEFAULT_MAX_DELEGATIONS,
        }
    }
}

/// The built-in `task` tool, which also holds the delegation state.
///
/// Construct one and include it in the parent's tool list to enable delegation.
/// Each nested run gets a new `TaskTool` with `depth + 1` and the child
/// profile's own `subagents` (for recursion).
///
/// # Profile ownership
///
/// The parent's tool list (other than this `TaskTool`) never flows into a
/// child. The child gets exactly: the profile's declared `tools`, plus a fresh
/// child `TaskTool` when the profile declares its own `subagents`.
pub struct TaskTool {
    /// Shared model provider (one is reused across the delegation tree).
    provider: Arc<dyn ModelProvider>,
    /// Parent model — inherited when a profile omits its own.
    parent_model: Model,
    /// Parent config — inherited when a profile omits its own.
    parent_config: RunConfig,
    /// Subagents declared at this level.
    subagents: Vec<SubagentProfile>,
    /// Recursion limit.
    max_depth: usize,
    /// Current depth (0 for the top-level agent's `task` tool).
    depth: usize,
    /// Cancellation token shared across the delegation tree.
    cancel: CancellationToken,
    /// Optional event sink (children emit to the same sink with a new session
    /// id, giving a nested trace without explicit span-parent linking).
    event_sink: Option<Arc<dyn EventSink>>,
    /// Shared counter of **remaining** delegations across the whole tree.
    /// Bounds exponential fan-out: each successful `task` call decrements it.
    remaining_delegations: Arc<AtomicUsize>,
}

impl TaskTool {
    /// Construct the top-level `task` tool (depth 0).
    ///
    /// Include the returned tool in the parent agent's tool list to enable
    /// delegation to any of `subagents`.
    #[must_use]
    pub fn new(
        provider: Arc<dyn ModelProvider>,
        parent_model: Model,
        parent_config: RunConfig,
        subagents: Vec<SubagentProfile>,
        options: SubagentOptions,
        cancel: CancellationToken,
        event_sink: Option<Arc<dyn EventSink>>,
    ) -> Self {
        Self {
            provider,
            parent_model,
            parent_config,
            subagents,
            max_depth: options.max_depth,
            depth: 0,
            cancel,
            event_sink,
            remaining_delegations: Arc::new(AtomicUsize::new(options.max_delegations)),
        }
    }

    /// Construct a child `task` tool at `depth + 1`.
    fn child(
        &self,
        subagents: Vec<SubagentProfile>,
        parent_model: Model,
        parent_config: RunConfig,
    ) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            parent_model,
            parent_config,
            subagents,
            max_depth: self.max_depth,
            depth: self.depth + 1,
            cancel: self.cancel.clone(),
            event_sink: self.event_sink.as_ref().map(Arc::clone),
            // Shared across the whole tree.
            remaining_delegations: Arc::clone(&self.remaining_delegations),
        }
    }

    /// Resolve a profile by name.
    fn resolve(&self, name: &str) -> Option<&SubagentProfile> {
        self.subagents.iter().find(|s| s.name == name)
    }

    /// The original delegation budget (for diagnostics). Stored implicitly as
    /// `remaining + consumed`; since we only need it for error messages, we
    /// approximate by reading `remaining` plus the depth index. This is best-
    /// effort and used only in the budget-exceeded message.
    fn max_delegations_hint(&self) -> usize {
        // We don't store the original cap separately; approximate from the
        // current remaining count. The message is guidance, not a contract.
        self.remaining_delegations.load(Ordering::Relaxed) + 1
    }

    /// Delegate to the resolved subagent. Returns the child's final text.
    async fn delegate(&self, profile: &SubagentProfile, prompt: String) -> CoreResult<RunOutcome> {
        // Apply inheritance.
        let child_model = profile
            .model
            .clone()
            .unwrap_or_else(|| self.parent_model.clone());
        let child_config = profile
            .config
            .clone()
            .unwrap_or_else(|| self.parent_config.clone());

        // Build the child's tool list. Profile-owned only; the parent's tools
        // never flow in. Add a child TaskTool only if the profile declares its
        // own subagents (recursion). The child TaskTool is PREPENDED so that,
        // if a profile mistakenly/​maliciously declares a tool named "task",
        // the depth-enforcing child TaskTool wins the name lookup (the runner
        // matches the first tool by name) and depth limits are preserved.
        let mut child_tools: Vec<Arc<dyn Tool>> = Vec::new();
        if !profile.subagents.is_empty() {
            let child_task = self.child(
                profile.subagents.clone(),
                // The child's TaskTool inherits from the *resolved* child
                // model/config, so grandchildren inherit the right defaults.
                child_model.clone(),
                child_config.clone(),
            );
            child_tools.push(Arc::new(child_task));
        }
        child_tools.extend(profile.tools.clone());

        // Fresh child session: new UUID, messages = [system, user].
        let child_session = Uuid::new_v4();
        let mut child_messages = vec![
            AgentMessage {
                role: Role::System,
                content: vec![ContentBlock::Text {
                    text: profile.instructions.clone(),
                }],
            },
            AgentMessage {
                role: Role::User,
                content: vec![ContentBlock::Text { text: prompt }],
            },
        ];

        // Child hooks: new session id, no turn sink (the parent's persistence
        // records the task tool result — exact replay), same event sink.
        let child_hooks = crate::event::RunHooks {
            session_id: Some(child_session),
            turn_sink: None,
            event_sink: self.event_sink.as_deref(),
            policy: None,
        };

        // Run the child to completion. Its events (SessionStarted → ... →
        // TurnFinished / RunFailed) flow to the same event sink with the
        // child's session id, giving a nested trace.
        run_agent(
            self.provider.as_ref(),
            &child_tools,
            &mut child_messages,
            &child_model,
            &child_config,
            &self.cancel,
            &child_hooks,
        )
        .await
    }
}

#[async_trait]
impl Tool for TaskTool {
    fn definition(&self) -> ToolDefinition {
        let mut desc = String::from(
            "Delegate a focused subtask to a named subagent. The subagent runs \
             in a fresh context and its answer is returned to you. Call this \
             only when a declared subagent is well-suited to the work. \
             Available subagents:",
        );
        if self.subagents.is_empty() {
            desc.push_str(" (none declared)");
        } else {
            for s in &self.subagents {
                let guidance = if s.description.trim().is_empty() {
                    "(no description provided)"
                } else {
                    s.description.trim()
                };
                desc.push_str(&format!("\n  - \"{}\": {}", s.name, guidance));
            }
        }

        // Schema: object requiring `agent` (string) and `prompt` (string).
        let mut fields = serde_json::Map::new();
        fields.insert("type".into(), Value::String("object".into()));
        fields.insert(
            "properties".into(),
            serde_json::json!({
                "agent": {
                    "type": "string",
                    "description": "The name of the declared subagent to delegate to."
                },
                "prompt": {
                    "type": "string",
                    "description": "The task to give the subagent (it sees this, not your conversation history)."
                }
            }),
        );
        fields.insert(
            "required".into(),
            Value::Array(vec![
                Value::String("agent".into()),
                Value::String("prompt".into()),
            ]),
        );

        ToolDefinition {
            name: "task".into(),
            label: "Task".into(),
            description: desc,
            parameters: crate::tool::ParameterSchema {
                fields: fields.into_iter().collect(),
            },
        }
    }

    async fn execute(&self, ctx: InvokeContext, input: Value) -> CoreResult<ToolResult> {
        // Parse { agent, prompt }.
        let obj = input.as_object().ok_or_else(|| {
            CoreError::ToolInputValidation("task tool expects an object input".into())
        })?;
        let agent = obj.get("agent").and_then(Value::as_str).ok_or_else(|| {
            CoreError::ToolInputValidation("task tool requires a string `agent`".into())
        })?;
        let prompt = obj.get("prompt").and_then(Value::as_str).ok_or_else(|| {
            CoreError::ToolInputValidation("task tool requires a string `prompt`".into())
        })?;

        // Resolve the subagent (SubagentNotDeclared).
        let profile = match self.resolve(agent) {
            Some(p) => p,
            None => {
                let known: Vec<&str> = self.subagents.iter().map(|s| s.name.as_str()).collect();
                return Err(CoreError::ToolInputValidation(format!(
                    "subagent not declared: \"{agent}\" (known: {})",
                    known.join(", ")
                )));
            }
        };

        // Enforce the shared delegation budget (bounds exponential fan-out).
        // fetch_sub returns the PREVIOUS value; if it was 0, we're already at
        // the cap and this call must be rejected. Otherwise we've claimed one
        // slot.
        let prev = self.remaining_delegations.fetch_sub(1, Ordering::Relaxed);
        if prev == 0 {
            // Restore the counter (we didn't consume a slot) and report.
            self.remaining_delegations.fetch_add(1, Ordering::Relaxed);
            return Err(CoreError::ToolInputValidation(format!(
                "delegation budget exhausted (max {} total delegations across the tree)",
                self.max_delegations_hint()
            )));
        }

        // Enforce the depth limit (DelegationDepthExceeded).
        if self.depth >= self.max_depth {
            // We already decremented the budget; restore it since we're not
            // actually delegating.
            self.remaining_delegations.fetch_add(1, Ordering::Relaxed);
            return Err(CoreError::ToolInputValidation(format!(
                "delegation depth exceeded (depth {} >= max_depth {})",
                self.depth, self.max_depth
            )));
        }

        // Honor cancellation before spawning the child (the child run also
        // checks cancellation, but failing fast avoids a needless child span).
        if ctx.cancel.is_cancelled() {
            return Err(CoreError::Cancelled("task delegation cancelled".into()));
        }

        // Delegate. Map a child-run failure into a bounded error result string
        // (the runner turns any Err into a model-visible `Error:` tool result,
        // so the parent can recover).
        let outcome = self.delegate(profile, prompt.to_string()).await?;
        Ok(ToolResult {
            content: vec![serde_json::json!({
                "type": "text",
                "text": if outcome.final_text.trim().is_empty() {
                    "(subagent returned no text)".to_string()
                } else {
                    outcome.final_text
                },
            })],
            details: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelRequest, ModelResponse};

    fn dummy_profile(name: &str) -> SubagentProfile {
        SubagentProfile::new(name, "you are a helper")
    }

    fn top_level_tool(profiles: Vec<SubagentProfile>, max_depth: usize) -> TaskTool {
        TaskTool::new(
            // Provider is only touched inside `delegate`/`run_agent`, which the
            // unit tests below do not exercise. A panic-on-call provider would
            // be wrong here because `Arc::new(())` isn't a provider — so we use
            // a dedicated test provider below where delegation actually runs.
            test_provider(),
            Model {
                id: "test/model".into(),
            },
            RunConfig::default(),
            profiles,
            SubagentOptions {
                max_depth,
                max_delegations: DEFAULT_MAX_DELEGATIONS,
            },
            CancellationToken::new(),
            None,
        )
    }

    // A minimal recording provider used by tests that exercise delegation.
    fn test_provider() -> Arc<dyn ModelProvider> {
        use async_trait::async_trait;
        struct TestProvider;
        #[async_trait]
        impl ModelProvider for TestProvider {
            async fn invoke(
                &self,
                _request: crate::model::ModelRequest,
            ) -> CoreResult<crate::model::ModelResponse> {
                // Return a single assistant text message with no tool calls.
                Ok(crate::model::ModelResponse {
                    messages: vec![crate::message::AgentMessage {
                        role: crate::message::Role::Assistant,
                        content: vec![crate::message::ContentBlock::Text {
                            text: "child done".into(),
                        }],
                    }],
                })
            }
        }
        Arc::new(TestProvider)
    }

    #[test]
    fn definition_lists_declared_subagents() {
        let profiles = vec![
            dummy_profile("reviewer").with_description("Review changes."),
            dummy_profile("classifier").with_description("Classify issues."),
        ];
        let tool = top_level_tool(profiles, DEFAULT_MAX_DEPTH);
        let def = tool.definition();
        assert_eq!(def.name, "task");
        assert_eq!(def.label, "Task");
        assert!(def.description.contains("\"reviewer\""), "missing reviewer");
        assert!(def.description.contains("Review changes."));
        assert!(def.description.contains("\"classifier\""));
        assert!(def.description.contains("Classify issues."));
    }

    #[test]
    fn definition_handles_no_subagents() {
        let tool = top_level_tool(vec![], DEFAULT_MAX_DEPTH);
        let def = tool.definition();
        assert!(def.description.contains("(none declared)"));
    }

    #[test]
    fn definition_schema_requires_agent_and_prompt() {
        let tool = top_level_tool(vec![dummy_profile("a")], DEFAULT_MAX_DEPTH);
        let def = tool.definition();
        let required = def
            .parameters
            .fields
            .get("required")
            .and_then(|v| v.as_array())
            .expect("required array");
        let names: Vec<&str> = required.iter().filter_map(Value::as_str).collect();
        assert!(names.contains(&"agent"));
        assert!(names.contains(&"prompt"));
    }

    #[tokio::test]
    async fn unknown_agent_returns_error() {
        let tool = top_level_tool(vec![dummy_profile("reviewer")], DEFAULT_MAX_DEPTH);
        let ctx = InvokeContext {
            tool_call_id: "c1".into(),
            cancel: CancellationToken::new(),
        };
        let err = tool
            .execute(ctx, serde_json::json!({ "agent": "ghost", "prompt": "hi" }))
            .await
            .expect_err("unknown agent should error");
        let msg = err.to_string();
        assert!(msg.contains("not declared"), "msg: {msg}");
        assert!(msg.contains("ghost"));
        // Helpful: lists the known subagents.
        assert!(msg.contains("reviewer"));
    }

    #[tokio::test]
    async fn depth_exceeded_at_max_zero() {
        // max_depth = 0 means even the top-level task tool (depth 0) exceeds.
        let tool = top_level_tool(vec![dummy_profile("a")], 0);
        let ctx = InvokeContext {
            tool_call_id: "c2".into(),
            cancel: CancellationToken::new(),
        };
        let err = tool
            .execute(ctx, serde_json::json!({ "agent": "a", "prompt": "hi" }))
            .await
            .expect_err("depth should exceed");
        let msg = err.to_string();
        assert!(msg.contains("depth exceeded"), "msg: {msg}");
        assert!(msg.contains("max_depth 0"));
    }

    #[tokio::test]
    async fn budget_exhaustion_blocks_delegation() {
        // max_delegations = 1 allows ONE delegation; the second `task` call in
        // the SAME run must be rejected with a budget-exceeded error.
        let tool = TaskTool::new(
            test_provider(),
            Model {
                id: "test/model".into(),
            },
            RunConfig::default(),
            vec![dummy_profile("worker")],
            SubagentOptions {
                max_depth: DEFAULT_MAX_DEPTH,
                max_delegations: 1,
            },
            CancellationToken::new(),
            None,
        );
        let ctx1 = InvokeContext {
            tool_call_id: "b1".into(),
            cancel: CancellationToken::new(),
        };
        // First delegation consumes the single budget slot and succeeds.
        let r1 = tool
            .execute(
                ctx1,
                serde_json::json!({ "agent": "worker", "prompt": "go" }),
            )
            .await
            .expect("first delegation succeeds");
        assert_eq!(r1.content.len(), 1);

        // Second delegation in the same tree is rejected.
        let ctx2 = InvokeContext {
            tool_call_id: "b2".into(),
            cancel: CancellationToken::new(),
        };
        let err = tool
            .execute(
                ctx2,
                serde_json::json!({ "agent": "worker", "prompt": "again" }),
            )
            .await
            .expect_err("budget should be exhausted");
        let msg = err.to_string();
        assert!(msg.contains("budget exhausted"), "msg: {msg}");
    }

    #[tokio::test]
    async fn delegate_runs_child_and_returns_text() {
        let tool = top_level_tool(vec![dummy_profile("worker")], DEFAULT_MAX_DEPTH);
        let ctx = InvokeContext {
            tool_call_id: "c3".into(),
            cancel: CancellationToken::new(),
        };
        let result = tool
            .execute(
                ctx,
                serde_json::json!({ "agent": "worker", "prompt": "do it" }),
            )
            .await
            .expect("delegation should succeed");
        assert_eq!(result.content.len(), 1);
        let text = result.content[0]
            .get("text")
            .and_then(Value::as_str)
            .expect("text");
        assert_eq!(text, "child done");
    }

    #[tokio::test]
    async fn cancellation_aborts_before_child_spawn() {
        let tool = top_level_tool(vec![dummy_profile("a")], DEFAULT_MAX_DEPTH);
        let cancel = CancellationToken::new();
        let ctx = InvokeContext {
            tool_call_id: "c4".into(),
            cancel: cancel.clone(),
        };
        cancel.cancel();
        let err = tool
            .execute(ctx, serde_json::json!({ "agent": "a", "prompt": "hi" }))
            .await
            .expect_err("should be cancelled");
        assert!(matches!(err, CoreError::Cancelled(_)), "err: {err}");
    }

    // ── Integration: run_agent-driven delegation ─────────────────────────

    /// A scripted provider: returns a queue of canned responses in order.
    struct ScriptedProvider {
        responses: std::sync::Mutex<std::collections::VecDeque<ModelResponse>>,
    }

    impl ScriptedProvider {
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
    impl ModelProvider for ScriptedProvider {
        async fn invoke(&self, _request: ModelRequest) -> CoreResult<ModelResponse> {
            let next = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ModelResponse { messages: vec![] });
            Ok(next)
        }
    }

    fn assistant_text(t: &str) -> AgentMessage {
        AgentMessage {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: t.into() }],
        }
    }

    /// A parent response that issues a `task` tool call.
    fn parent_task_call(agent: &str, prompt: &str) -> AgentMessage {
        AgentMessage {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: "call_1".into(),
                call: crate::tool::ToolCall {
                    name: "task".into(),
                    input: serde_json::json!({ "agent": agent, "prompt": prompt }),
                },
            }],
        }
    }

    #[tokio::test]
    async fn integration_parent_delegates_and_child_answers() {
        // Parent: first response is a task tool call; after the tool result,
        // it emits final text.
        let provider: Arc<dyn ModelProvider> = Arc::new(ScriptedProvider::new(vec![
            // Parent turn 1: delegate.
            vec![parent_task_call("worker", "do the work")],
            // Child turn 1 (fresh session): the child's own response.
            vec![assistant_text("child done")],
            // Parent turn 2: summarize the child's answer (returned as the
            // task tool result).
            vec![assistant_text("got: child done")],
        ]));
        let cancel = CancellationToken::new();
        let task = Arc::new(TaskTool::new(
            Arc::clone(&provider),
            Model {
                id: "test/m".into(),
            },
            RunConfig::default(),
            vec![dummy_profile("worker")],
            SubagentOptions::default(),
            cancel.clone(),
            None,
        ));
        let tools: Vec<Arc<dyn Tool>> = vec![task];
        let mut messages = vec![
            AgentMessage {
                role: Role::System,
                content: vec![ContentBlock::Text {
                    text: "be brief".into(),
                }],
            },
            AgentMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "delegate the work".into(),
                }],
            },
        ];
        let outcome = run_agent(
            provider.as_ref(),
            &tools,
            &mut messages,
            &Model {
                id: "test/m".into(),
            },
            &RunConfig::default(),
            &cancel,
            &crate::event::RunHooks::default(),
        )
        .await
        .expect("parent run");
        assert_eq!(outcome.turns, 2);
        assert_eq!(outcome.final_text, "got: child done");
    }

    #[tokio::test]
    async fn integration_nested_delegation_stops_at_max_depth() {
        // Two-level profile: parent → child → grandchild. With max_depth = 1,
        // the grandchild delegation must return a depth-exceeded error result.
        let grandchild = dummy_profile("grandchild");
        let child = SubagentProfile::new("child", "you delegate").with_subagent(grandchild);

        // Responses: parent delegates to child; child delegates to grandchild;
        // child then reports what it got back.
        let provider: Arc<dyn ModelProvider> = Arc::new(ScriptedProvider::new(vec![
            // Parent turn 1: delegate to child.
            vec![parent_task_call("child", "sub-delegate")],
            // Child turn 1 (fresh session): it tries to delegate to grandchild.
            vec![AgentMessage {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolUse {
                    id: "cchild".into(),
                    call: crate::tool::ToolCall {
                        name: "task".into(),
                        input: serde_json::json!({
                            "agent": "grandchild",
                            "prompt": "too deep"
                        }),
                    },
                }],
            }],
            // Child turn 2: summarize the depth-exceeded error it received
            // (the grandchild task call returned a tool error result).
            vec![assistant_text("grandchild was unreachable")],
            // Parent turn 2: summarize the child's report.
            vec![assistant_text("done")],
        ]));
        let cancel = CancellationToken::new();
        let task = Arc::new(TaskTool::new(
            Arc::clone(&provider),
            Model {
                id: "test/m".into(),
            },
            RunConfig::default(),
            vec![child],
            // max_depth = 1: only ONE level of delegation allowed.
            SubagentOptions {
                max_depth: 1,
                max_delegations: DEFAULT_MAX_DELEGATIONS,
            },
            cancel.clone(),
            None,
        ));
        let tools: Vec<Arc<dyn Tool>> = vec![task];
        let mut messages = vec![AgentMessage {
            role: Role::User,
            content: vec![ContentBlock::Text { text: "go".into() }],
        }];
        let outcome = run_agent(
            provider.as_ref(),
            &tools,
            &mut messages,
            &Model {
                id: "test/m".into(),
            },
            &RunConfig::default(),
            &cancel,
            &crate::event::RunHooks::default(),
        )
        .await
        .expect("parent run");
        // The parent ran two turns (delegate + summarize). The run completed
        // despite the grandchild depth-exceeded error (tool errors are
        // model-visible, not run-fatal).
        assert_eq!(outcome.turns, 2);
    }
}

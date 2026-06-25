# `fluers dev` config UX — MCP + subagents on the server (slice 2 follow-up)

**Status:** design (committed before implementation).
**Scope:** wire the `[agents.*]` + `[mcp.servers.*]` config surface into the
`fluers dev` HTTP server so that MCP tools and subagent delegation work
server-side, not just in `fluers run`.

## Problem

`fluers run` (slice 3) already resolves a config-declared agent: connects MCP
servers, builds the `SubagentProfile` graph, and constructs a top-level
`TaskTool`. `fluers dev` does not — it registers a single static agent with a
fixed tool list.

The blocker is that `TaskTool` holds **per-run** state:

- a `CancellationToken` shared across the delegation tree, and
- an `Arc<dyn EventSink>` (the per-request event bus, used for OTel + SSE).

A `TaskTool` built once at startup and reused across requests would
**cross-wire** concurrent requests: cancelling request A would cancel request
B's tree; every request's events would funnel to one startup-time sink instead
of per-request. So the dev server must build the `task` tool **fresh per
request**, while reusing the static pieces (MCP server connections, the
`SubagentProfile` graph, built-in tools).

## What is static vs per-request

| Resource | Lifetime | Why |
|----------|----------|-----|
| MCP server subprocesses + adapted `McpTool`s | static (startup) | Each holds an `Arc<RunningService>`; cheaply cloneable, safe to share. Connect once. |
| Built-in tools (`mvp_tools`) | static (startup) | Stateless apart from a shared `SessionEnv` (one workdir — accepted for local dev). |
| `SubagentProfile` graph | static (startup) | Embeds each child's static tools; resolved once (cycle-checked at config time). |
| Top-level `TaskTool` | **per-request** | Owns this request's `CancellationToken` + `EventSink` + a fresh delegation budget. |

## Design

### 1. A typed request-scoped tool factory

In `fluers-core` (neutral home — the data is all core/tokio-util types):

```rust
/// Inputs needed to build a request's tool list. Carried by value into the
/// factory so the factory can construct a request-local `TaskTool`.
pub struct ToolRequestContext {
    pub provider: Arc<dyn ModelProvider>,
    pub parent_model: Model,
    pub parent_config: RunConfig,
    pub cancel: CancellationToken,
    pub event_sink: Option<Arc<dyn EventSink>>,
}

/// Builds the full tool list for a single request. Stored on `AgentHandle`;
/// when set, it takes precedence over the static `tools` vec.
pub type ToolFactory =
    Arc<dyn Fn(ToolRequestContext) -> Vec<Arc<dyn Tool>> + Send + Sync>;
```

`fluers-server::AgentHandle` gains `pub tool_factory: Option<ToolFactory>` and a
helper:

```rust
impl AgentHandle {
    pub fn tools_for_request(
        &self,
        cancel: CancellationToken,
        event_sink: Option<Arc<dyn EventSink>>,
    ) -> Vec<Arc<dyn Tool>> {
        match &self.tool_factory {
            Some(f) => f(ToolRequestContext {
                provider: self.provider.clone(),
                parent_model: self.model.clone(),
                parent_config: self.config.clone(),
                cancel,
                event_sink,
            }),
            None => self.tools.clone(),
        }
    }
}
```

### 2. Split `agent_config` into static + dynamic

`resolve_agent` becomes two layers:

```rust
/// Static resolution: MCP connect (once) + SubagentProfile graph (once) +
/// cycle/refs validation. No TaskTool — that is per-request.
pub struct ResolvedAgentSpec {
    pub static_tools: Vec<Arc<dyn Tool>>,   // builtins + MCP
    pub subagents: Vec<SubagentProfile>,
    pub options: SubagentOptions,
    pub instructions: Option<String>,
    pub description: Option<String>,
    // Holds the connected McpServer handles so subprocesses live as long as
    // the spec (and thus the agent registration).
    _mcp: McpCache,
}

impl ResolvedAgentSpec {
    /// Build the tool list for one run: static tools + a fresh top-level
    /// TaskTool (bound to this run's cancel + sink) when subagents exist.
    pub fn tools_for_run(&self, ctx: ToolRequestContext) -> Vec<Arc<dyn Tool>> {
        let mut tools = self.static_tools.clone();
        if !self.subagents.is_empty() {
            tools.insert(0, Arc::new(TaskTool::new(
                ctx.provider, ctx.parent_model, ctx.parent_config,
                self.subagents.clone(), self.options, ctx.cancel, ctx.event_sink,
            )));
        }
        tools
    }
}
```

- `resolve_agent_spec(...)` does the MCP connect + profile-graph build + validation once.
- The existing `resolve_agent(...)` (used by `fluers run`) becomes a thin wrapper: `resolve_agent_spec(...).await?.tools_for_run(ctx)`.

### 3. Server request paths

In `fluers-server/src/lib.rs`, both `invoke` and `stream`:

- wrap the per-request event bus in `Arc`: `let event_bus = Arc::new(EventBus::new_default());`
- after `cancel` + `event_bus` exist, build request tools:
  `let tools = handle.tools_for_request(cancel.clone(), Some(Arc::clone(&event_bus) as _));`
- in `stream`, build tools **inside** the spawned task so the task owns the request-local sink.
- pass `&tools` into `run_agent` / `run_agent_streaming`; `RunHooks.event_sink` uses `event_bus.as_ref()`.

### 4. CLI `dev`

`DevArgs` gains `--config` and `--agent` (same env/merge as `run`). In `dev`:

- If `cfg.has_agents()`: resolve the selected agent **spec once** at startup,
  wrap it in `Arc`, and build a `ToolFactory` closure:
  ```rust
  let spec = Arc::new(spec);
  let factory: ToolFactory = Arc::new(move |ctx| spec.tools_for_run(ctx));
  ```
  Set `AgentHandle.tool_factory = Some(factory)`; use the agent's
  `instructions` as `system_prompt` and `description` for `GET /agents`.
- Else (legacy): static builtins, `tool_factory: None` (unchanged behavior).
- `--no-tools` suppresses builtins + MCP + task.

### 5. Scope & accepted limitations

- **Single registered agent** for MVP (`fluers dev --agent <name>`). A
  multi-agent registry (route `/agents/<name>` for every config agent) is a
  natural follow-up but deferred.
- **Shared `SessionEnv`/workdir** across requests: accepted for local
  single-user dev (matches `fluers dev`'s intent — editing your repo). Not a
  multi-tenant sandbox.
- **No HTTP auth/guard**: still local-only (`127.0.0.1`). Public hosting waits
  on auth middleware.
- MCP server subprocesses live for the dev server's lifetime (connected once at
  startup), shared across all requests.

## Tests

- `agent_config`: `ResolvedAgentSpec.static_tools` does **not** contain `task`;
  `tools_for_run()` prepends `task` only when subagents exist; two
  `tools_for_run()` calls produce **independent** delegation budgets.
- `fluers-server`: a route with a `tool_factory` invokes it per request, and the
  factory's tools reach the provider; legacy `tool_factory: None` path
  unchanged. Existing `AgentHandle` test literals updated.
- No new external dependencies for tests; MCP/subagent paths exercised with
  hermetic builtins + mock providers.

## Exit criteria

- `fluers dev --config fluers.toml --agent <name>` serves an agent whose
  `/invoke` can call builtins, MCP tools, and `task` (subagent delegation).
- Concurrent requests get independent cancellation + delegation budgets.
- `cargo nextest run --workspace` green; fmt + strict clippy clean.

## Accepted risks (post-review)

Folded from the focused reviewer + red-team review of the implementation.

**Fixed (this slice):**
- Removed a spurious second `mark_run(RunStatus::Running)` left in `invoke`
  after `run_agent` completed (copy-paste leftover; the following
  `update_run(Completed)` overrode it, but it was semantically wrong).
- Resume-model consistency: `resolve_session` now returns the model id to use
  (persisted on resume, the handle's for a new session) so the run's model, the
  provider call, and a `task` tool's `parent_model` all agree. Previously the
  persisted model was read into `_model_id` and discarded.
- `dev` now honors config-level budgets (`max_turns` / `turn_timeout_ms` /
  `tool_concurrency`) so the served agent matches `fluers run` semantics.
- Startup `eprintln` notes the active mode (config vs legacy), that config is
  resolved once (restart to reload), and the single-user shared-workdir caveat.

**Pre-existing, accepted (not introduced by this slice):**
- **Panic isolation.** A panicking tool aborts the request task (axum/tokio
  catch it at the task boundary, so the server stays up, but the client gets a
  connection drop rather than a clean 500). `run_agent` only wraps tool
  execution in `catch_unwind` on the parallel path (`tool_concurrency > 1`);
  the default sequential path propagates. Mitigating this is a future
  hardening pass over `fluers-core::runner`.
- **Shared `SessionEnv` / workdir.** `dev` builds one `LocalSessionEnv` shared
  by all requests; concurrent `/invoke`s can race on file writes / shell exec.
  Accepted for local single-user dev (the intent of `fluers dev`). Multi-tenant
  isolation waits on a real `Sandbox` + HTTP auth.
- **Concurrent resume on the same `session_id`.** Two requests resuming the
  same session load identical history and run independently; whichever
  `after_turn` persists last wins (turns from the other may be lost).
- **Detached `/stream` tasks.** The streaming handler spawns and never joins;
  if the client disconnects the task keeps running and writes to the in-memory
  run store.
- **Unbounded in-memory run store.** `ServerState.runs` is never pruned.
  Long-lived dev servers can grow without bound.
- **MCP subprocess reaping on abnormal task leak.** `kill_on_drop` ensures
  clean shutdown reaps subprocesses; an abnormally leaked `/stream` task holds
  a tool `Arc` that keeps the subprocess alive until that task finishes.

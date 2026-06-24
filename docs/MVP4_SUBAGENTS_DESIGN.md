# MVP 4f — Subagent delegation & depth limits

**Status:** design (committed before implementation).
**Scope slice:** in-process `task` tool + declared subagents + depth limits.

## Goal

Let an agent delegate a focused piece of work to a **named subagent** while it
continues to own the interaction, mirroring Flue's
[Subagents](https://flue.dev/docs/guide/subagents/). The parent agent gets a
built-in `task` tool; when it calls `task({ agent: "reviewer", prompt: "..." })`,
Fluers spawns a **fresh child session** for the named subagent, runs it to
completion, and returns the child's final text to the parent as the tool result.

## What we are NOT porting (explicitly deferred)

Flue ships two layers around this:

1. **Profile-based task delegation** — named profiles, a `task` capability,
   fresh child sessions, configuration inheritance, depth limits.
2. **Durable detached dispatch** — the Node/Cloudflare `agent-coordinator`,
   SQL submission stores, leases, claim loops, heartbeats, journal recovery.

**This slice ports layer 1 only.** Layer 2 is the durable multi-process
dispatch infrastructure (`packages/runtime/src/node/agent-coordinator.ts`,
`runtime/agent-submissions.ts`) — it requires the HTTP server, a SQL submission
store, and multi-process coordination that belong to a later "durable runtime"
slice, not the MVP agent loop.

Also deferred: workflow `session.task(...)` orchestration, agent endpoint
addressing, result-schema (valibot) validation, definition-time circular-profile
detection, and CLI/config-file authoring for subagents.

## Conceptual model (sourced from Flue's subagent guide)

A subagent is a **named profile** declared on a parent. Delegated work runs in a
**separate child session** with a fresh context — not the parent's conversation
history. The child's answer is returned to the parent as the `task` tool result.

### Configuration inheritance (Flue-compatible)

| Field | Behavior |
| ----- | -------- |
| `instructions`, `tools`, `subagents` | **Profile-owned.** Only the profile's own declarations apply; an omitted field means none. The parent's values never flow into the delegated session. |
| `model`, `config` (thinking, timeouts, concurrency) | **Inherited as default.** The profile's own value wins when declared; an omitted field uses the parent's value. |

This mirrors Flue exactly: capability fields (`instructions`/`tools`/`subagents`)
are profile-owned so a parent's bash tool never silently leaks into a reviewer
subagent; scalar defaults (`model`/`thinkingLevel`/`compaction`) inherit so you
don't repeat yourself.

## Public API (in `fluers-core`)

```rust
/// A named, declarable subagent profile.
///
/// Capability fields (`instructions`/`tools`/`subagents`) are profile-owned —
/// the parent's values never flow into a delegated session. Scalar defaults
/// (`model`/`config`) inherit from the parent when `None`.
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

/// Options for the `task` tool.
pub struct SubagentOptions {
    /// Maximum delegation depth (recursion limit). Default 5.
    /// The top-level agent runs at depth 0; its `task` calls run children at
    /// depth 1; their `task` calls run at depth 2; etc.
    pub max_depth: usize,
}

/// The built-in `task` tool, which also holds the delegation state.
///
/// Construct one and include it in the parent's tool list to enable delegation.
/// Each nested run gets a new `TaskTool` with `depth + 1` and the child
/// profile's own `subagents` (for recursion).
pub struct TaskTool { /* see Execution contract */ }

impl TaskTool {
    pub fn new(
        provider: Arc<dyn ModelProvider>,
        parent_model: Model,
        parent_config: RunConfig,
        subagents: Vec<SubagentProfile>,
        options: SubagentOptions,
        cancel: CancellationToken,
        event_sink: Option<Arc<dyn EventSink>>,
    ) -> Self;
}
```

`TaskTool: Tool`, so the parent just includes `Arc::new(task_tool)` in its tool
list. The provider/model/config are shared by `Arc` / clone so a child can
construct its own `TaskTool` with the same defaults.

## Execution contract

`TaskTool::execute(ctx, input)`:

1. Parse `{ agent: String, prompt: String }` from `input`. Missing/invalid →
   `Err(CoreError::ToolInputValidation(...))` → the runner converts this to a
   model-visible `Error:` tool result (confirmed in 4d: tool `Err` is never
   run-fatal).
2. Look up `subagents.iter().find(|s| s.name == agent)`:
   - Missing → `Err("subagent not declared: <agent>")` (the
     `SubagentNotDeclared` condition from the plan).
3. Check depth: `if self.depth >= self.max_depth` →
   `Err("delegation depth exceeded (<depth> >= <max_depth>)")` (the
   `DelegationDepthExceeded` condition).
4. Resolve the child profile (apply inheritance):
   - `child_model = profile.model.unwrap_or_else(|| self.parent_model.clone())`
   - `child_config = profile.config.unwrap_or_else(|| self.parent_config.clone())`
   - `child_tools = profile.tools` + (if `profile.subagents` non-empty) a new
     `TaskTool` at `depth + 1` built from the profile's subagents.
5. Build a fresh child session: new `Uuid`, messages =
   `[system(profile.instructions), user(prompt)]`.
6. Build child `RunHooks { session_id: child_id, turn_sink: None,
   event_sink: self.event_sink.as_deref() }`.
   - **No child `TurnSink`** (per Flue: the parent's persistence records the
     task tool result, keeping exact parent replay).
7. `run_agent(&self.provider, &child_tools, &mut child_messages, &child_model,
   &child_config, &self.cancel, &child_hooks).await`.
8. Return `ToolResult` with one text content block carrying the child's
   `final_text`.

### Tool definition (model-facing)

The `task` tool's `definition()` produces:
- **name:** `task`
- **label:** `Task`
- **description:** lists the declared subagents by name + description, so the
  model knows valid targets and what each is good for. Example:
  `Delegate a focused subtask to a named subagent. Available subagents:
  - "reviewer": Review the proposed change and identify correctness risks.
  - "classifier": Classify the issue for routing.`
- **parameters:** JSON Schema requiring `agent` (string) and `prompt` (string).

## Depth & cycle handling

- **Runtime depth limit** (the MVP enforcement): `max_depth` defaults to 5. A
  child `TaskTool` is constructed with `depth + 1`; the check runs before
  spawning the child. Direct cycles (a subagent declaring itself) are caught at
  runtime by the depth limit.
- **Definition-time circular-profile detection** (Flue's `assertAgentProfile`
  `WeakSet` check) is deferred — the runtime limit is sufficient for the MVP and
  keeps the profile type free of registration machinery.
- `max_depth = 0` disables delegation entirely (the top-level agent's `task`
  calls always exceed depth); `max_depth = 1` allows one level of delegation.

## Observability

Child runs emit the full lifecycle event sequence (SessionStarted → TurnStarted
→ ModelStarted/Finished → ToolStarted/Finished → TurnFinished) to the **same
`EventSink`** as the parent. The child uses a **distinct session UUID**, so
events are distinguishable in the trace and the OTel exporter renders a nested
session→turn→tool tree.

The parent's `ToolStarted`/`ToolFinished` for the `task` call naturally brackets
the child's whole session, giving trace context without explicit span-parent
linking (deferred).

## Persistence

The child session has **no `TurnSink`**. The child's final text becomes the
parent's `task` tool result, which the parent's `TurnSink` persists as a normal
tool result. This preserves exact parent-session replay — the only durable
artifact of a delegation is the tool-result text, same as any other tool. (This
matches Flue: "its retained history remains owned by the parent session.")

## Cancellation

The child run shares the parent's `CancellationToken`. If the parent is
cancelled, children are too. The `task` tool's `InvokeContext.cancel` is the
same token (the runner threads the run's token into every tool call), so a
cancelled delegation aborts promptly.

## Tests

**Unit (hermetic, always run):**

- `TaskTool::definition()` includes the tool name `task`, lists each declared
  subagent's name + description, and the schema requires `agent` + `prompt`.
- Unknown agent → `Err` whose message names the agent.
- Depth exceeded → `Err` whose message reports `depth >= max_depth`.
- Inheritance: explicit `model`/`config` on the profile win; omitted fields
  inherit the parent's.
- Profile-owned tools: the parent's tool list does NOT flow into the child
  (construct a child and assert its tool list equals only the profile's tools).

**Integration (hermetic, mock provider):**

- Parent mock provider emits one `task` tool call → child mock provider returns
  text → parent receives the task result and completes.
- Nested delegation stops at `max_depth` (a 2-deep chain with `max_depth = 1` →
  the second `task` call returns a depth-exceeded error result).
- Event sink captures the child's `SessionStarted` with a UUID distinct from the
  parent's.

No network, no credentials, no external model.

## CLI wiring

**Deferred** (per advisor, same as 4d). The library API + tests satisfy 4f's
exit criteria ("an agent that delegates a subtask to a declared subagent, with
depth-limit enforcement"). A config-file format for declaring subagents
(`[agents.<name>]` tables) is a separate UX slice.

## Exit criteria

- An agent can delegate a subtask to a declared subagent via the built-in `task`
  tool, and the child's answer returns to the parent.
- Depth-limit enforcement works (`max_depth` configurable; default 5).
- **Delegation-budget enforcement works** (`max_delegations` configurable;
  default 64) — bounds exponential fan-out (see Accepted risks).
- `cargo nextest run --workspace` stays green with no external deps.
- fmt + strict clippy clean.

## Accepted risks (post-review, documented)

The following were flagged by the 4f adversarial review and assessed as
**accepted** for the MVP scope (with cheap guards added where noted):

- **Delegation budget (added):** depth alone bounds chain length but not
  branching — a parent turn can issue many parallel `task` calls, each spawning
  children that do the same, producing up to
  `max_tool_calls_per_turn`^`max_depth` ≈ 10⁵ runs at defaults. `SubagentOptions::
  max_delegations` (default 64) is a shared `AtomicUsize` counter across the
  whole tree; each `task` call decrements it and a call that would exceed the
  budget returns a budget-exceeded error result.
- **Tool-name collision guard (added):** if a profile declares a tool named
  `task`, the runner's first-match lookup could let it shadow the
  depth-enforcing child `TaskTool`. Mitigated by **prepending** the child
  `TaskTool` to the child tool list so it always wins the lookup; a profile's
  colliding `task` tool becomes unreachable rather than a depth-bypass.
- **Prompt injection (accepted, inherent):** the child's system message is the
  trusted, author-declared `instructions`, but the `prompt` comes from the
  parent model (untrusted — it could inject adversarial instructions). This is
  inherent to delegation (the whole point is to pass the model's subtask to a
  specialist); the instructions are not overridden at the API level. Prompt-
  injection hardening is the memory/EventBus layer's concern, not the tool
  adapter's.
- **Cancellation token choice (verified correct):** `delegate()` passes the
  **run** `CancellationToken` (`self.cancel`) to child runs, not the per-tool
  `ctx.cancel`. `ctx.cancel` is a *child* of the run token (runner line 654),
  so this means subagent children live for the parent **run**, not a single
  per-call window — a sibling tool's cancellation must not kill a subagent.
  A run-level cancellation still propagates to all descendants.
- **Provider sharing (accepted):** parent and children share one
  `Arc<dyn ModelProvider>`. Real HTTP providers are stateless, so this is
  correct. Stateful test providers (e.g. a scripted queue) see interleaved
  parent/child calls — integration tests account for this by sequencing
  responses in call order.
- **Event flood (accepted):** each child emits the full lifecycle event
  sequence. Deep/wide delegation can generate many events, but `EventBus` is a
  bounded `broadcast` channel whose `send` is non-blocking: slow receivers lag
  and drop events rather than blocking the agent or growing memory
  unboundedly.
- **Named error variants (accepted, deferred):** the design doc references
  `SubagentNotDeclared` / `DelegationDepthExceeded` as conditions; the code
  expresses them as `CoreError::ToolInputValidation` with distinguishing
  messages. Behaviour is correct (the runner converts any tool `Err` into a
  model-visible `Error:` result). Distinct `CoreError` variants for structured
  matching are a future refactor.

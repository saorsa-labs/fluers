# Fluers Porting Plan

A phased roadmap for porting Flue (TypeScript) to Fluers (Rust).

> **Guiding principle:** each milestone produces a **runnable, tested**
> artifact. We port *architecture*, not line numbers. The foundation
> (`fluers-core`) comes first because Flue's own harness depends on layers
> that have no Rust equivalent.

## Scope reality check

Flue is large. Indicative sizes from the upstream tree:

| File / area                                  | Size   |
| -------------------------------------------- | ------ |
| `runtime/src/node/agent-coordinator.ts`      | ~27 KB |
| `runtime/test/node-agent-coordinator.test.ts`| ~90 KB |
| `runtime/src/types.ts`                       | ~39 KB |
| `runtime/test/session-operations.test.ts`    | ~50 KB |
| `react/src/agent-reducer.ts`                 | ~16 KB |
| `opentelemetry/src/index.ts`                 | ~17 KB |
| `cli/bin/flue.ts`                            | ~51 KB |

A faithful port is **multi-week, multi-person** work. The phases below make
that tractable and keep `main` green throughout.

---

## MVP 0 — Foundation & local sandbox ✅ *(scaffold done)*

**Goal:** the trait graph and CLI compile and run; the local sandbox actually
executes tools against a real directory.

- [x] Cargo workspace, 7 crates, strict lint policy (`-D warnings`,
      no `unwrap`/`expect`/`panic` in production)
- [x] `fluers-core`: `ModelProvider`, `Tool`, `AgentMessage`, `ThinkingLevel`
- [x] `fluers-runtime`: `SessionEnv`, `Sandbox`, `define_agent`, `Skill`,
      `SessionStore`, `EventBus`
- [x] `fluers-cli`: `version` / `run` / `dev` / `build` / `deploy`
- [ ] **Implement `LocalSessionEnv`** — real `tokio::fs` + `tokio::process`
      behind the `SessionEnv` trait, honouring `Limits`
- [ ] **Wire the 6 built-in tools** to a real `SessionEnv` (replace stubs in
      `runtime/src/tool.rs`)
- [ ] Unit tests for each tool against a temp dir

**Exit criteria:** `fluers run --prompt "list files in ."` executes `bash`/
`glob` against the real filesystem and returns bounded output.

---

## MVP 1 — Single-agent loop + providers

**Goal:** an agent can actually talk to a model and use tools in a loop.

- [ ] Implement the **agent loop** in `fluers-core`: send messages + tools to
      `ModelProvider`, parse `ToolCall`s, execute, append results, repeat
      until stop. (This is the Rust heart of `agent-coordinator.ts`.)
- [ ] **Provider: Anthropic** (`anthropic/...`) via `reqwest` streaming
- [ ] **Provider: OpenAI-compatible** (`openai/...`, local mistralrs)
- [ ] Cancellation via `tokio::sync::Notify` propagated into tools/providers
- [ ] Integration test: a real (or mock) provider round-trip with a tool call

**Exit criteria:** `fluers run --model anthropic/claude-sonnet-4-6 --prompt "read README.md"`
returns the agent's response after one or more tool calls.

---

## MVP 2 — Sessions, skills, events, persistence contract

**Goal:** durable, observable sessions.

- [ ] Full `SKILL.md` frontmatter schema (`name`, `description`, `triggers`,
      `model`, …) + packaged-skill discovery under `/.flue/packaged-skills/`
- [ ] Skill injection into the system prompt (Flue's skill-loading semantics)
- [ ] `PersistenceAdapter` contract finalized; `SessionStore` swap point
- [ ] Event stream: turn/tool lifecycle events + an `observe` subscriber API
- [ ] Resumable sessions (load → continue)

**Exit criteria:** start a session, run several turns, kill the process,
resume from persisted state.

---

## MVP 3 — HTTP dispatch/invoke + dev server + build/deploy

**Goal:** deployable agents, matching Flue's HTTP surface.

- [ ] `axum` server with the `dispatch` / `invoke` / `listAgents` / `getRun`
      endpoints (mirror `runtime/src/runtime/flue-app.ts` + `invoke.ts`)
- [ ] `AgentRouteHandler` equivalent (auth/guard middleware)
- [ ] `fluers dev` boots the local runtime + watches for agent changes
- [ ] `fluers build` bundles an agent into a deployable artifact
- [ ] `fluers deploy` to a first target (container image)
- [ ] `fluers-sdk` streaming client wired to the real protocol

**Exit criteria:** `fluers dev` serves an agent; a remote `fluers-sdk` client
invokes it and receives streamed events.

---

## MVP 4 — MCP, remote sandboxes, postgres, telemetry

**Goal:** feature parity with Flue's adapter ecosystem.

- [ ] `fluers-mcp`: stdio + HTTP-SSE transports; expose MCP servers' tools as
      `fluers-core::Tool`s (mirror `runtime/src/mcp.ts`)
- [ ] Remote container sandbox (E2B / Daytona) behind the `Sandbox` trait
- [ ] `fluers-postgres`: `sqlx`-backed `PersistenceAdapter`
- [ ] `fluers-otel`: OTLP spans/metrics exporter wired to the `EventBus`
- [ ] Subagent delegation & depth limits (the `agent-coordinator` submission/
      dispatch machinery: `SubagentNotDeclared`, `DelegationDepthExceeded`, …)

**Exit criteria:** an agent that delegates to a subagent, runs tools in a
remote container, persists to Postgres, and emits OTel traces.

---

## Cross-cutting: what carries through every phase

- **Panic-free production code.** `unwrap`/`expect`/`panic` only in tests.
  Enforced by `workspace.lints.clippy`.
- **Property tests** (`proptest`) for the tool layer and message (de)serialization.
- **`just check-all`** must stay green on `main`.
- **Attribution:** keep Flue file references in doc comments so each ported
  behavior traces back to its origin.

## Decision log

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Re-implement `pi-agent-core` + `pi-ai` as `fluers-core` | No Rust crate exists; the harness depends on them. |
| 2 | Phased, not big-bang | Keeps `main` green; each milestone ships something runnable. |
| 3 | `axum` over `Hono`, `serde` over `valibot` | Idiomatic Rust, strong ecosystem. |
| 4 | Defer React bindings | React is JS-only; revisit as a separate frontend effort. |
| 5 | Apache-2.0 (matches upstream) | Flue is Apache-2.0; keeps the port compatible. |

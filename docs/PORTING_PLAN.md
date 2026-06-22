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
- [x] **Cancellation uses `CancellationToken`** (not `Notify`), threaded
      through `InvokeContext` + `SessionEnv::exec`
- [x] **`base64` crate** replaces the hand-rolled codec
- [x] **`ModelProvider::stream` returns a real `Stream`** (no buffering default)
- [x] **`PersistenceAdapter` lives in `fluers-runtime`** so `SessionStore`
      can be generic over it
- [x] **`EnvTool` propagates errors** (`Result`, not stringified `String`)
- [x] Property test for image base64 round-trip; unit tests for skills
- [ ] **Implement `LocalSessionEnv`** — real `tokio::fs` + `tokio::process`
      behind the `SessionEnv` trait, honouring `Limits`
- [ ] **Path containment** — canonicalize + reject paths escaping the root
      (see `SECURITY.md`); deny escaping symlinks
- [ ] **Wire the 6 built-in tools** to a real `SessionEnv` (replace stubs in
      `runtime/src/tool.rs`)
- [ ] Unit tests for each tool against a temp dir; mock-provider harness

**Exit criteria:** `fluers run --prompt "list files in ."` executes `bash`/
`glob` against the real filesystem and returns bounded output, with no path
escaping the session root.

---

## MVP 0.5 — Walking skeleton *(new — de-risks the contract)*

**Goal:** one thin end-to-end slice through every layer, *before* committing
the tool/sandbox/streaming signatures to stone.

This milestone exists because the plan-reviewer's #1 finding was that MVP 0
+ MVP 1 are two big-bang integrations with the riskiest signatures (`Tool`,
`InvokeContext`, `ModelProvider::stream`, cancellation) designed *before* a
loop exercises them. A walking skeleton surfaces rework now, not in MVP 1.

- [ ] **Mock `ModelProvider`** that emits a scripted turn (text → tool call
      → text) deterministically — no network, no API keys, CI-safe
- [ ] **Minimal turn loop** in `fluers-core` (see MVP 1 for the loop home):
      send messages+tools → parse tool calls → execute → append results →
      repeat until stop. Hard cap of N turns.
- [ ] **One real tool** (`read`) over `LocalSessionEnv`, with cancellation +
      a `tokio::time::timeout` deadline in the loop
- [ ] Integration test asserting the full round-trip

**Exit criteria:** a test runs a mock provider + `read` tool through the
loop and asserts the final assistant message. Every load-bearing signature
is exercised, so MVP 1 can build providers without trait rework.

---

## MVP 1 — Single-agent loop + providers

**Goal:** an agent can talk to a real model and use tools in a loop.

> **Loop-home decision (from plan review):** the *pure turn-loop* lives in
> `fluers-core` (it talks only to `Arc<dyn Tool>` + `ModelProvider`); the
> *coordinator* — sessions, events, subagents, budgets, dispatch — lives in
> `fluers-runtime`, matching `agent-coordinator.ts`'s actual placement in
> `@flue/runtime` (not `pi-agent-core`).

- [ ] Finalize the **turn loop** in `fluers-core` from the MVP 0.5 spike
- [ ] **Concurrency model** (specify before implementing): parallel tool
      calls per turn (`tokio::task::JoinSet`), max-turns budget, token
      budget, stop conditions, malformed-tool-call recovery
- [ ] **Provider: Anthropic** (`anthropic/...`) via `reqwest` streaming SSE,
      including incremental tool-call JSON assembly
- [ ] **Provider: OpenAI-compatible** (`openai/...`, local mistralrs)
- [ ] **Config / secrets**: env-var + config-file key sources, per-provider
      base URLs, key redaction in `tracing`
- [ ] Cancellation: `CancellationToken` propagated into providers + tools;
      `tokio::time::timeout` deadline per turn
- [ ] Integration tests against the mock provider (CI) + a gated live test

**Exit criteria:** `fluers run --model anthropic/claude-sonnet-4-6 --prompt "read README.md"`
returns the agent's response after one or more tool calls, with budget
enforcement and cancellation working.

---

## MVP 2 — Sessions, skills, events, persistence contract

**Goal:** durable, observable sessions.

- [ ] Full `SKILL.md` frontmatter schema (`name`, `description`, `triggers`,
      `model`, …) + packaged-skill discovery under `/.flue/packaged-skills/`
      (use a real frontmatter parser, not the MVP `split_once` heuristic)
- [ ] Skill injection into the system prompt (Flue's skill-loading semantics)
- [ ] `PersistenceAdapter` finalized; `SessionStore` made generic over it
- [ ] **JSON-file persistence adapter** (so "resume after kill" works without
           Postgres, which is MVP 4)
- [ ] Event stream: turn/tool lifecycle events + an `observe` subscriber API
      (with backpressure — bounded channel, no recursive emit)
- [ ] Resumable sessions (load → continue)

**Exit criteria:** start a session, run several turns, kill the process,
resume from the JSON-file persisted state.

---

## MVP 3 — HTTP dispatch/invoke + dev server *(build/deploy split out)*

**Goal:** deployable agents, matching Flue's HTTP surface.

> **Scope split (from plan review):** HTTP dispatch + dev server + SDK
> streaming stay here; **build/deploy** (container images) moves to a
> separate later milestone — it's orthogonal and alone multi-week.

- [ ] `axum` server with the `dispatch` / `invoke` / `listAgents` / `getRun`
      endpoints (mirror `runtime/src/runtime/flue-app.ts` + `invoke.ts`)
- [ ] `AgentRouteHandler` equivalent (auth/guard middleware)
- [ ] `fluers dev` boots the local runtime + watches for agent changes
- [ ] `fluers-sdk` streaming client wired to the real SSE protocol

**Exit criteria:** `fluers dev` serves an agent; a remote `fluers-sdk` client
invokes it and receives streamed events.

---

## MVP 3.5 — Build & deploy *(split out from MVP 3)*

**Goal:** produce and ship deployable artifacts.

- [ ] `fluers build` bundles an agent into a deployable artifact
- [ ] `fluers deploy` to a first target (container image)
- [ ] Cross-compilation via `cargo-zigbuild` for VPS targets

**Exit criteria:** `fluers deploy --target container` produces a runnable
image.

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
| 5 | Apache-2.0 (matches upstream) + `NOTICE` file | Flue is Apache-2.0; attribution per §4d. |
| 6 | `CancellationToken` over `Notify` *(from review)* | `Notify` is a one-shot waker; cancellation needs a flag + deadlines. |
| 7 | `ModelProvider::stream` returns `Stream` *(from review)* | The `FnMut` default buffered everything — not real streaming. |
| 8 | Walking skeleton (MVP 0.5) before locking signatures *(from review)* | De-risks `Tool`/`InvokeContext`/streaming before MVP 1 builds on them. |
| 9 | Loop in `fluers-core`, coordinator in `fluers-runtime` *(from review)* | Matches `agent-coordinator.ts`'s real home; avoids a dep cycle. |
| 10 | `PersistenceAdapter` in `fluers-runtime`, JSON-file adapter in MVP 2 *(from review)* | The swap target (`SessionStore`) lives there; unblocks resume-without-Postgres. |
| 11 | `LocalSandbox` is NOT a security boundary until isolation lands *(from red-team)* | Documented in `SECURITY.md`; path containment in MVP 0, OS isolation later. |

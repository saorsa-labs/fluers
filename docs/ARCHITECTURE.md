# Fluers Architecture

How Fluers is layered, and how each layer maps back to Flue's TypeScript
packages.

## The dependency that shaped everything

Flue (TypeScript) is **not** a monolith. It is a *harness* layered on two
lower libraries:

```
            ┌─────────────────────────────────────────────┐
            │   your agent code (defineAgent(() => …))    │
            ├─────────────────────────────────────────────┤
            │  @flue/runtime   ← the harness               │  sessions, tools, skills, sandbox,
            │                  (agent.ts, agent-coord., …) │  workflows, dispatch/invoke, HTTP routing
            ├──────────────┬──────────────────────────────┤
            │ pi-agent-core│            pi-ai             │  agent loop: messages, Tool trait,
            │ (no Rust cr.)│       (no Rust crate)        │  model/provider abstraction, streaming
            └──────────────┴──────────────────────────────┘
```

`packages/runtime/src/types.ts` imports `AgentMessage, AgentTool, ThinkingLevel`
from `@earendil-works/pi-agent-core` and `ImageContent, Model` from
`@earendil-works/pi-ai`. **Neither package is published to crates.io.**

**Consequence:** a native Rust port cannot "just" translate Flue — it must
also re-implement the two layers beneath it. That is exactly why
`fluers-core` exists as a first-class crate rather than an afterthought.

## Fluers layering

```
            ┌─────────────────────────────────────────────┐
            │   your agent code (define_agent(|b| …).await)│
            ├─────────────────────────────────────────────┤
            │  fluers-runtime  ← the harness               │  agent.rs (defineAgent),
            │                                            │  env.rs (SessionEnv), sandbox.rs,
            │                                            │  session.rs, skill.rs, event.rs, tool.rs
            ├─────────────────────────────────────────────┤
            │  fluers-core  ← agent loop + model trait     │  model.rs (ModelProvider), tool.rs (Tool),
            │                                            │  message.rs, thinking.rs, error.rs
            └─────────────────────────────────────────────┘
                  ▲                    ▲
        fluers-cli / sdk         fluers-mcp / postgres / otel
        (drive + consume)        (adapters, plug in later)
```

## Package-by-package mapping

| Flue package                         | Fluers crate        | Status        |
| ------------------------------------ | ------------------- | ------------- |
| `pi-agent-core` (dep)                | `fluers-core`       | traits sketched |
| `pi-ai` (dep)                        | `fluers-core`       | `ModelProvider` trait |
| `@flue/runtime`                      | `fluers-runtime`    | traits + stubs |
| `@flue/runtime/node` (`local()`)     | `fluers-runtime::sandbox` | `LocalSandbox` (stub env) |
| `@flue/cli`                          | `fluers-cli`        | `version`/`run`/`dev`/`build`/`deploy` |
| `@flue/sdk`                          | `fluers-sdk`        | `Client` shape |
| `@flue/runtime` MCP (`mcp.ts`)       | `fluers-mcp`        | `Transport`/`McpServer` traits |
| `@flue/postgres`                     | `fluers-postgres`   | `PersistenceAdapter` trait |
| `@flue/opentelemetry`                | `fluers-otel`       | `tracing` subscriber |
| `@flue/react`                        | *(deferred)*        | not ported (React is JS-only) |

## Key abstractions (and where Flue's limits are preserved)

- **`SessionEnv`** (`env.rs`) — the filesystem + process abstraction. Every
  built-in tool (`read`/`write`/`edit`/`bash`/`grep`/`glob`) operates purely
  against it, so the *same tools* work over a local dir, a virtual fs, or a
  remote container. Flue's resource caps (`MAX_READ_LINES = 2000`,
  `MAX_READ_BYTES = 50KiB`, `MAX_GREP_MATCHES = 100`, …) are preserved as
  `Limits::default()`.

- **`Sandbox`** (`sandbox.rs`) — a factory that produces a `SessionEnv` per
  session. Flue's three flavours map to: `LocalSandbox` (done shape, stub
  ops), `VirtualSandbox` (TODO), remote container via providers (TODO — Flue
  mentions E2B/Daytona).

- **`define_agent` / `AgentProfile`** (`agent.rs`) — composes model + tools +
  skills + sandbox + instructions. Mirrors Flue's `defineAgent(() => ({...}))`
  via a builder closure.

- **`ModelProvider`** (`fluers-core::model`) — the provider abstraction. One
  trait; concrete providers (OpenAI, Anthropic, local GGUF via mistralrs) plug
  in behind it. This is where `pi-ai`'s `Model` interface lives in Rust.

- **`Tool`** (`fluers-core::tool`) — the trait every tool implements, plus
  `validate_input` for schema checks.

- **`Skill`** (`skill.rs`) — `SKILL.md` loading with frontmatter parsing and
  the `/.flue/packaged-skills/` convention.

- **`SessionStore` / `EventBus`** (`session.rs`/`event.rs`) — in-memory now;
  replaced by `PersistenceAdapter` (postgres) and an OTLP emitter later.

## What is deliberately NOT ported 1:1

- **React bindings** (`@flue/react`) — React is JS-only; a Rust equivalent
  would be a frontend SDK or a Dioxus/Leptos adapter, out of scope for now.
- **`valibot`** — replaced by `serde` + `schemars` + targeted validation.
- **`Hono`** — replaced by `axum`.
- **`turbo`/`pnpm`** — replaced by Cargo workspaces + `just`.
- **The TS bundler / `tsdown`** — replaced by `cargo build`.

These substitutions are called out in [`PORTING_PLAN.md`](PORTING_PLAN.md)
under each milestone where they matter.

# Fluers

**A native Rust port of [Flue](https://github.com/withastro/flue) — the agent harness framework.**

> Status: 🚧 **Scaffolded.** Crate graph, trait foundations, and CLI compile and run.
> Behavior is intentionally stubbed behind a phased MVP plan — see
> [`docs/PORTING_PLAN.md`](docs/PORTING_PLAN.md).

Flue is *"not another SDK"* — it's a programmable **harness** that gives any
model the context and environment it needs for autonomous work: sessions,
tools, skills, instructions, filesystem access, and a secure sandbox. You
compose an agent from a model + tools + skills + sandbox + instructions and
run it locally via CLI or deploy it to a hosted runtime.

Fluers is a ground-up **Rust** reimplementation of that architecture — fast,
single-binary, panic-free, and embedding-friendly. It is **not** a line-by-line
TypeScript translation. Where Flue leans on its TypeScript runtime (`Hono`,
`valibot`, `turbo`), Fluers uses idiomatic Rust equivalents (`axum`, `serde`,
`cargo`/`just`).

## Why port Flue to Rust?

| Flue (TS)                              | Fluers (Rust)                                   |
| -------------------------------------- | ----------------------------------------------- |
| Runs on Node ≥ 22                       | Single static binary, no runtime                |
| `@earendil-works/pi-agent-core` + `pi-ai` | `fluers-core` (native agent loop + model trait) |
| `Hono` HTTP sub-app                     | `axum`                                          |
| `valibot` schemas                       | `serde` + `schemars`                            |
| V8 sandbox / child processes            | `tokio::process` + sandbox trait (local/virtual/remote) |
| `pnpm`/`turbo` monorepo                 | Cargo workspace                                 |
| `tsc`/`tsdown` build                    | `cargo` / `just`                                |

Native Rust buys us: a single deployable binary, deterministic sandboxing,
trivial embedding into other Rust services (saorsa-core, ant-quic, fae), and
near-zero-overhead tool execution.

## Crate layout

```
crates/
  fluers-core       # agent loop primitives + model abstraction (port of pi-agent-core + pi-ai)
  fluers-runtime    # the harness: defineAgent, SessionEnv, Sandbox, sessions, skills, events
  fluers-cli        # the `fluers` binary (dev / build / deploy / run)
  fluers-sdk        # client SDK for consuming deployed agents
  fluers-mcp        # Model Context Protocol client integration
  fluers-postgres   # Postgres persistence adapter
  fluers-otel       # OpenTelemetry tracing adapter
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for how the layers fit
together and how each maps back to Flue's TypeScript packages.

## Quick start

```bash
# Prerequisites
cargo --version        # 1.96+
just --version         # optional but recommended

# Validate the workspace
just check-all         # fmt → lint (strict) → check → test

# Use the CLI
just run version
just run run --model anthropic/claude-sonnet-4-6 --prompt "hello"
```

## The porting strategy in one paragraph

Flue is a **harness layer** built on two lower layers — `pi-agent-core`
(the agent loop) and `pi-ai` (the model/provider abstraction). Neither has a
published Rust crate, so a faithful native port must **re-implement that
foundation first** (`fluers-core`), then layer the harness on top
(`fluers-runtime`). Everything else — CLI, SDK, MCP, persistence, telemetry —
is comparatively mechanical once the foundation is solid. The work is phased
so that each milestone produces a runnable, tested artifact.

➡️ **Read [`docs/PORTING_PLAN.md`](docs/PORTING_PLAN.md)** for the full
milestone breakdown (MVP 0 → MVP 4).

## Attribution

Fluers is a derivative work inspired by and architecturally modeled on
[Flue](https://github.com/withastro/flue) by Astro. Upstream is licensed
Apache-2.0; Fluers is likewise Apache-2.0.

## License

Apache-2.0. See [`LICENSE`](LICENSE).

# Fluers

**A native Rust port of [Flue](https://github.com/withastro/flue) — the agent harness framework.**

> 🙏 **Credit:** Flue was created by [**Fred K. Schott**](https://github.com/FredKSchott) and the
> [Astro](https://astro.build) team. Fluers is an independent Rust reimplementation of their
> architecture — this project wouldn't exist without their excellent original work. See
> [`NOTICE`](NOTICE) for the full attribution.
>
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

## Deviations from the upstream Flue port

Because Saorsa Labs owns fluers, it may deviate from a faithful Flue port where
a downstream consumer needs a generic seam. Deviations stay **generic** (no
consumer-specific types leak into fluers) so the crate remains independently
shippable.

- **Tool policy hook** (`fluers_core::ToolPolicy` / `PolicyVerdict`, added
  2026-06-28). A generic governance gate the agent loop consults *before* each
  `Tool::execute`. Wired as an optional `policy` field on
  `fluers_core::RunHooks` (chosen over `RunConfig` so the borrowed
  `&dyn ToolPolicy` trait object does not force `RunConfig`'s `Debug`/`Clone`
  derives). The default is `None` (allow-all), so existing consumers are
  unaffected. A `PolicyVerdict::Deny(reason)` skips execution and appends a
  model-visible error result (the loop continues, as with an unknown-tool
  result); `Confirm(reason)` is treated as allow-with-log by callers without a
  confirmation channel. Upstream Flue has no equivalent per-tool gate.

## Attribution

Fluers is a native Rust reimplementation of the architecture of
[**Flue — The Sandbox Agent Framework**](https://github.com/withastro/flue),
created by **[Fred K. Schott](https://github.com/FredKSchott)** and the
[Astro](https://astro.build) team.

**All design credit for the agent-harness architecture belongs to Fred and
the Astro contributors.** Fluers is an independent Rust implementation of
those ideas, not a line-by-line translation of the TypeScript source; this
project would not exist without their excellent original work. See
[`NOTICE`](NOTICE) for the full derivative-work notice.

## License

Dual-licensed under either

- the Apache License, Version 2.0
  ([`LICENSE-APACHE`](LICENSE-APACHE)), or
- the MIT License ([`LICENSE-MIT`](LICENSE-MIT))

at your option (`SPDX-License-Identifier: MIT OR Apache-2.0`). Upstream
Flue is licensed Apache-2.0.

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/saorsa-labs/fluers#license)

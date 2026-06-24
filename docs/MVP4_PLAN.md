# MVP 4 — Execution Plan

> **Status:** in progress (started 2026-06). MVPs 1–3 are complete and
> live-verified; see `PORTING_PLAN.md` for the milestone-level plan. This doc
> breaks MVP 4 into independently shippable slices with explicit ordering and
> prerequisites.

## Goal

Feature parity with Flue's adapter ecosystem: an agent that delegates to a
subagent, runs tools in a remote container, persists sessions to Postgres for
resume, recalls user preferences via mem0, and emits OpenTelemetry traces.

MVP 4 is **too large to land in one pass.** It is split into six slices
(4a–4f). Each slice keeps `main` green (fmt + strict clippy + `cargo nextest
run --workspace`) and ships a runnable, testable capability. External services
(Docker / Postgres / mem0) are **never required** for normal workspace tests —
all such tests are gated behind environment variables.

## Slices

### 4a — Postgres persistence ✅ (target: first) — **DONE**

**Crate:** `fluers-postgres` (already scaffolded as a stub).

- `sqlx`-backed `PersistenceAdapter` (the trait lives in
  `fluers-runtime::persistence`; `SessionStore` already handles the
  `SessionState` envelope + schema-version checks, so the adapter only stores
  opaque JSON).
- **Runtime SQL** (`sqlx::query`), never the `query!` macro — avoids requiring
  `DATABASE_URL` at compile time.
- Schema: single table
  `fluers_sessions(id TEXT PRIMARY KEY, data JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
  created idempotently on connect via `CREATE TABLE IF NOT EXISTS`.
- `save_session` uses `INSERT ... ON CONFLICT (id) DO UPDATE` (upsert).
- JSON stored via explicit casts (`$n::jsonb` on write, `data::text` on read)
  so no sqlx `json` feature is needed.
- **Tests:** integration tests gated behind `FLUERS_POSTGRES_TEST_URL` (skip
  cleanly if unset → workspace tests stay green without a DB). Run live with a
  real Postgres before declaring the slice done.

**Exit criteria:** a session saved via `PostgresAdapter` round-trips and
resumes through the existing `SessionStore::load`; `--list-sessions` works
against the Postgres-backed store.

> **Met (live-verified against Postgres 16.14):** 6/6 adapter + SessionStore
> tests pass (incl. a full save→load→list resume through `SessionStore`, a
> 256 KiB JSONB payload, and an upsert). 3× repeated parallel runs confirm
> the concurrency fix holds. CLI `--database-url` persists + resumes a session
> through Postgres; `--list-sessions` lists it; the DB shows a single row
> (upsert verified). All workspace tests (52) stay green without a DB (the
> postgres tests skip cleanly when `FLUERS_POSTGRES_TEST_URL` is unset).
>
> **Concurrency fix:** concurrent `CREATE TABLE IF NOT EXISTS` races on
> Postgres's type-name catalog (`pg_type_typname_nsp_index`). Schema creation
> is now serialized with a transaction-scoped `pg_advisory_xact_lock`, making
> multi-worker boot safe.

---

### 4b — Semantic memory (mem0) — *new capability, separate from persistence* — **DONE**

**Crate:** `fluers-memory` (new).

- **`MemoryAdapter` trait** (`add` / `search` / `clear`) — **kept strictly
  separate from `PersistenceAdapter`.** mem0 is *lossy by design* (LLM-based
  fact extraction, raw transcripts discarded); it **cannot** serve as a
  `PersistenceAdapter` for faithful resume-after-kill. See the decision box in
  `PORTING_PLAN.md` §MVP 4.
- `Mem0RestAdapter`: hits the mem0 REST API over `reqwest` (no Rust SDK
  exists). Self-host via `docker compose` (Qdrant + dashboard).
- Wired as a `TurnSink` (extract + store after each turn) and injected into the
  system prompt at session start.
- **Tests:** `MemoryAdapter` round-trip against an in-memory mock; live REST
  test gated behind `FLUERS_MEM0_TEST_URL`.

**Exit criteria:** an agent that recalls a user preference stated in an earlier
session via mem0 search, while still resuming exact transcript state from the
persistence adapter.

> **Met:** `fluers-memory` crate ships `MemoryAdapter` (trait, separate from
> `PersistenceAdapter`), `InMemoryMemoryAdapter`, `Mem0RestAdapter` (hosted
> platform wire contract, sourced from primary sources), `MemoryTurnSink`
> (text-pair-only, fail-open), and `FanoutTurnSink` (in `fluers-core`). CLI
> `run` wires `--memory-url`/`--memory-user-id`/`--memory-api-key`/
> `--memory-limit` with env fallbacks; memory injected for new sessions only.
> 18 fluers-memory tests (incl. 6 hermetic mock-server tests), 78/78 workspace,
> fmt + strict clippy clean. Fail-open + red-team security findings folded
> (credential redaction on full-path URLs, prompt-injection bounding, empty-
> user-id rejection, error-body truncation, panic/fail-open contract
> documented). Dev-server memory wiring deferred (needs `fluers-server` to
> accept a memory adapter in `ServerState`).

---

### 4c — Observability (event emission + OTel) — **DONE**

**Crates:** touches `fluers-core` (event emission only), `fluers-otel` (new).

- **Prerequisite:** wire the deferred MVP-2 `EventBus` emit calls into
  `run_agent` / `run_agent_streaming` (turn start/end, tool call start/end,
  model invocation). The `EventBus` (tokio `broadcast`) already exists; only
  the emit sites are missing.
- `fluers-otel`: subscribe to the `EventBus` and emit OTLP spans/metrics. Do
  **not** wire OTel directly into `fluers-core` — route everything through the
  observer/`EventBus` seam.
- **Tests:** a subscriber that asserts the expected event sequence for a known
  run; OTLP export gated behind `FLUERS_OTEL_TEST_ENDPOINT`.

**Exit criteria:** a `fluers dev` run emits a complete OpenTelemetry trace
(visible in Jaeger / an OTLP collector) covering model + tool spans.

> **See [`MVP4_OTEL_DESIGN.md`](MVP4_OTEL_DESIGN.md) for the full design: the
> EventSink seam (solving the core↔runtime dependency cycle), the RunHooks
> API, and the OTLP exporter + tracing fallback.**

---

### 4d — MCP tool adapter

**Crate:** `fluers-mcp` (scaffolded as a stub).

- stdio transport first, then HTTP/SSE.
- Expose an external MCP server's tools as `fluers-core::Tool` impls (mirror
  `runtime/src/mcp.ts`).
- **Tests:** against an in-process mock MCP server over stdio.

**Exit criteria:** an agent that can call a tool provided by an external MCP
server (e.g. the filesystem reference server).

---

### 4e — Remote sandbox

**Behind the existing `Sandbox` trait.**

- E2B or Daytona adapter (env-gated; `LocalSessionEnv` remains the default).
- **Tests:** env-gated; never required for workspace CI.

**Exit criteria:** a tool that shells out inside a remote container sandbox.

---

### 4f — Subagent delegation & depth limits

**Last** — depends on stable persistence + observability seams (4a, 4c).

- Port the `agent-coordinator` submission/dispatch machinery:
  `SubagentNotDeclared`, `DelegationDepthExceeded`, etc.
- Depth limits enforced in the coordinator.

**Exit criteria:** an agent that delegates a subtask to a declared subagent,
with depth-limit enforcement.

---

## Ordering & dependencies

```
4a (Postgres) ──┐
                ├─► 4c (OTel) ──► 4f (subagents)
4b (mem0) ──────┘
4d (MCP)        (independent)
4e (sandbox)    (independent, env-gated)
```

- 4a and 4b are independent and lowest-risk; both unblock real deployments.
- 4c requires the EventBus emission wiring first (deferred from MVP 2).
- 4f requires 4a + 4c to be stable.
- 4d and 4e are self-contained and can proceed in parallel with anything.

## Non-goals (deferred beyond MVP 4)

- Auth / guard middleware on the HTTP server (needed before public deployment;
  noted as deferred from MVP 3).
- React bindings (JS-only; revisit as a separate frontend effort).
- Workflow orchestration (`POST /workflows/:name`).

## Traps to avoid (from advisor review)

- **Do not** couple mem0 to `PersistenceAdapter`; keep `MemoryAdapter`
  separate.
- **Do not** wire OTel directly into `fluers-core`; route through the
  observer/`EventBus` seam.
- **Do not** begin subagent delegation before persistence + observability seams
  are stable.
- **Do not** require Docker / Postgres / mem0 credentials for normal workspace
  tests.

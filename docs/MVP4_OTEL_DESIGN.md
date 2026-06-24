# MVP 4c — Observability (Event Emission + OpenTelemetry)

> **Status:** design (2026-06). Implements slice 4c of
> [`MVP4_PLAN.md`](MVP4_PLAN.md). Depends on 4a (Postgres) being stable for
> subagent delegation (4f) later.

## Goal

Wire the deferred MVP-2 `EventBus` emit calls into the agent loop so every run
emits a complete lifecycle event stream, then add a real OpenTelemetry exporter
(`opentelemetry-otlp`) so a `fluers dev` run produces a full trace (model +
tool spans) visible in Jaeger / any OTLP collector.

## The dependency-cycle problem (why emit sites were deferred in MVP 2)

`EventBus` and `Event` live in `fluers-runtime`. `run_agent` lives in
`fluers-core`. `fluers-core` **cannot** depend on `fluers-runtime` (that would
create a cycle: `fluers-runtime → fluers-core → fluers-runtime`). This is why
the emit sites were never added — there was no clean seam.

## Design: the `EventSink` seam

Mirror the existing `TurnSink` solution (which solved the same cycle for
per-turn persistence):

1. **Move `Event` types to `fluers-core`** as `RunEvent` — plain `Uuid` /
   `String` fields, no `fluers-runtime` dependency. `fluers-runtime::EventBus`
   re-exports and uses the core type.

2. **Define `EventSink` trait in `fluers-core`** (sync, non-blocking — matching
   `tokio::broadcast::Sender::send` which is already sync):
   ```rust
   pub trait EventSink: Send + Sync {
       fn emit(&self, event: RunEvent);
   }
   ```

3. **`EventBus` implements `EventSink`** — its existing `emit()` method already
   matches.

4. **Group hooks into `RunHooks`** — replacing the current `on_turn` parameter
   so the param count stays the same:
   ```rust
   pub struct RunHooks<'a> {
       pub session_id: Option<Uuid>,
       pub turn_sink: Option<&'a dyn TurnSink>,
       pub event_sink: Option<&'a dyn EventSink>,
   }
   ```
   `run_agent(... on_turn: Option<&dyn TurnSink>)` becomes
   `run_agent(... hooks: &RunHooks<'_>)` — same param count, room for future
   hooks without API churn. `RunHooks::default()` is the "no hooks" case.

## Event model

Eight event types. **No content leakage** — no prompt text, tool args, tool
outputs, file contents, or model response text in any event:

| Event | Fields | OTel mapping |
|-------|--------|--------------|
| `SessionStarted` | `session` | Root span start |
| `TurnStarted` | `session, turn` | Child span start |
| `ModelStarted` | `session, turn, model` | Span event on turn span |
| `ModelFinished` | `session, turn` | Span event on turn span |
| `ToolStarted` | `session, turn, tool, call_id` | Child span start |
| `ToolFinished` | `session, turn, tool, call_id, ok` | Child span end |
| `TurnFinished` | `session, turn` | Turn span end |
| `RunFailed` | `session, error` | Span error event + root span end |

`model` is the model id (already public in `RunConfig`). `tool` is the tool
name. `call_id` correlates `ToolStarted`/`ToolFinished`. `ok` is a boolean.

## Emit sites in `run_agent` / `run_agent_streaming`

```
SessionStarted (before the loop)
for turn in 1..=max_turns {
    TurnStarted
    ModelStarted
    (provider.invoke / stream)
    ModelFinished
    if tool_calls {
        for each tool_call { ToolStarted }
        (execute_tool_calls — parallel)
        for each result { ToolFinished { ok } }
    }
    after_turn (TurnSink)
    TurnFinished
}
RunFailed (on error, before propagating)
```

## `fluers-otel`: OTLP exporter + tracing fallback

Two subscriber entry points:

- **`tracing_subscriber(bus)`** (existing) — logs every event via `tracing::info!`.
  The zero-dependency default. Always available.

- **`otlp_subscriber(bus, endpoint)`** (new) — spawns a task that:
  1. Sets up an `opentelemetry-otlp` span exporter pointing at `endpoint`.
  2. Drains the EventBus and builds a span tree: `SessionStarted` → root span,
     `TurnStarted` → child span, `ToolStarted`/`ToolFinished` → nested child
     spans. Uses a per-session span stack (`HashMap<Uuid, Vec<SpanContext>>`).
  3. Exports via OTLP (HTTP/gRPC).
  4. Falls back to the tracing subscriber when `FLUERS_OTEL_ENDPOINT` is unset.

**Gated behind env:** `FLUERS_OTEL_ENDPOINT` (e.g. `http://localhost:4317`).
When unset, `fluers dev` / `fluers run` use the tracing subscriber only. No OTLP
dependency is initialized. Workspace tests never require a collector.

## CLI / dev wiring

- `--otel-endpoint <URL>` (env: `FLUERS_OTEL_ENDPOINT`) — when set, spawn the
  OTLP subscriber; otherwise use tracing.
- `fluers dev` constructs an `EventBus`, passes it to the agent handle, and
  (optionally) spawns the OTLP subscriber.
- `fluers run` constructs an `EventBus` for each run, subscribes (tracing or
  OTLP), and passes it through `RunHooks`.

## Tests

**Always-on (no collector):**
- `fluers-core`: recording `EventSink` asserts the exact event sequence for a
  text-only run (Session→Turn→Model→TurnFinished) and a tool-using run (adds
  ToolStarted/ToolFinished).
- `fluers-runtime`: EventBus broadcast tests adjusted to the core `RunEvent`
  type. `EventBus` implements `EventSink` correctly.
- `fluers-otel`: tracing subscriber drains and logs events (no OTLP needed).

**OTLP (env-gated):**
- A live OTLP export test gated behind `FLUERS_OTEL_TEST_ENDPOINT`; skip cleanly
  when unset (same convention as Postgres/mem0 tests).

**Full gate:** `cargo fmt --all`, strict clippy, `cargo nextest run --workspace`
stays green without an OTLP collector.

## Non-goals / deferred

- Metrics (counters, histograms) — spans first; metrics in a follow-up.
- Distributed tracing across subagent boundaries (needs 4f).
- Self-hosted Jaeger/Qdrant setup scripts — deployment concern (MVP 3.5).
- Event content fields (prompt/response/tool args) — explicit non-goal
  (privacy + span size).

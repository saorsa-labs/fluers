# MVP 4b — Semantic Memory (mem0) Design

> **Status:** design (2026-06). Implements slice 4b of
> [`MVP4_PLAN.md`](MVP4_PLAN.md). **Complements** MVP 4a Postgres persistence —
> it does **not** replace it. See the decision box in `PORTING_PLAN.md` §MVP 4.

## Goal

Give Fluers agents long-term, semantic recall: an agent remembers a user
preference stated in an *earlier* session, without it being in the current
conversation transcript. This is a capability `PersistenceAdapter` (exact
session replay) cannot provide, because mem0 is **lossy by design** (LLM-based
fact extraction; raw transcripts discarded).

## Sources (wire contract derived from primary sources, not memory)

The hosted-platform REST contract below is taken verbatim from the official
Python client and server source. Fetched 2026-06-23:

- **Client (hosted platform API):**
  `https://raw.githubusercontent.com/mem0ai/mem0/main/mem0/client/main.py`
  — `MemoryClient` class, `add`/`search`/`get_all`/`delete`/`delete_all`.
- **Self-hosted server (FastAPI):**
  `https://raw.githubusercontent.com/mem0ai/mem0/main/server/main.py`
  — `/memories`, `/search`, `/memories/{id}` routes.
- **README (overview + self-host setup):**
  `https://raw.githubusercontent.com/mem0ai/mem0/main/README.md`

## Wire contract — hosted platform API (`api.mem0.ai`)

The `Mem0RestAdapter` targets the **hosted platform API** (the stable, documented
surface the official client speaks). The self-hosted `server/` product uses a
*different* route surface (`/memories`, `/search`, no `/v3/`, bearer auth) and is
out of scope for the MVP — a `SelfHostedMem0Adapter` can be added later.

**Base URL:** `https://api.mem0.ai` (configurable).
**Auth:** `Authorization: Token {MEM0_API_KEY}` header. Key from `MEM0_API_KEY`
env.

| Operation | Method + path | Request body | Response |
|-----------|---------------|--------------|----------|
| **Add** | `POST /v3/memories/add/` | `{"messages": [{"role":"user","content":"..."}, ...], "user_id": "...", "metadata": {...}}` | `{"results": [{"id","memory","event":"ADD"\|"UPDATE"\|"DELETE"}], "relations": [...]}` |
| **Search** | `POST /v3/memories/search/` | `{"query": "...", "filters": {"user_id": "..."}, "top_k": N}` | `{"results": [{"id","memory","score","user_id","metadata","created_at","updated_at"}]}` |
| **Get all** | `POST /v3/memories/` | `{"filters": {"user_id": "..."}}` | `{"results": [...]}` |
| **Delete one** | `DELETE /v1/memories/{id}/` | — | `{"message": "..."}` |
| **Delete all** | `DELETE /v1/memories/` | params `user_id` | `{"message": "..."}` |

`top_k` defaults to 5. `query` is trimmed before sending.

## Crate layout

New crate `fluers-memory`:

```
crates/fluers-memory/
  Cargo.toml
  src/
    lib.rs        # MemoryAdapter trait, Memory, request/response types,
                  # InMemoryMemoryAdapter (always-on tests), format_memories()
    mem0.rs       # Mem0RestAdapter (reqwest, redacted errors, fail-open)
    sink.rs       # MemoryTurnSink + FanoutTurnSink
```

## `MemoryAdapter` trait (separate from `PersistenceAdapter`)

```rust
#[async_trait]
pub trait MemoryAdapter: Send + Sync {
    async fn add(&self, req: &MemoryAddRequest) -> Result<MemoryAddResponse>;
    async fn search(&self, req: &MemorySearchRequest) -> Result<Vec<Memory>>;
    async fn clear(&self, user_id: &str) -> Result<()>;
}

pub struct MemoryAddRequest {
    pub user_id: String,
    pub messages: Vec<MemoryMessage>,   // role + text content only
    pub metadata: Option<serde_json::Value>,
}
pub struct MemorySearchRequest {
    pub user_id: String,
    pub query: String,
    pub top_k: usize,
}
pub struct Memory { pub id: String, pub memory: String, pub score: Option<f64>,
                    pub metadata: Option<serde_json::Value> }
```

`MemoryAdapter` lives in `fluers-memory`, **never** in `fluers-runtime`, and
never touches `PersistenceAdapter` or `SessionState`.

## TurnSink integration — `FanoutTurnSink`

`run_agent` accepts only **one** `Option<&dyn TurnSink>`, and `SessionRunner`
already occupies that slot. To wire memory alongside persistence without
changing `run_agent`'s signature, add a generic **fanout** sink in
`fluers-core` (near `TurnSink`):

```rust
pub struct FanoutTurnSink { sinks: Vec<Box<dyn TurnSink>> }
impl TurnSink for FanoutTurnSink {
    async fn on_turn(&self, turn: &TurnSnapshot) {
        for s in &self.sinks { s.on_turn(turn).await; }
    }
}
```

- Calls sinks **sequentially**; persistence sink first (so a memory failure
  can't corrupt the persistence order), memory sink second.
- **Fail-open by default:** `MemoryTurnSink` catches its own errors and logs
  them via `tracing::warn!`; it **never** propagates an error that could break
  session persistence or the agent run.

## `MemoryTurnSink` — what gets stored

Stores **only the latest user + assistant text pair** per turn. Deliberately
excluded (privacy + cost):
- ❌ tool call inputs / outputs (may contain file contents, command output)
- ❌ image / file blocks
- ❌ thinking/reasoning blocks
- ❌ the full transcript (only the new pair for this turn)

A turn with no user→assistant text pair (e.g. a tool-only turn) stores nothing.

## Memory injection — new sessions only

`format_memories(&[Memory]) -> String` produces a deterministic, compact block:

```
Relevant user memories:
- prefers dark mode
- timezone is Europe/Helsinki
```

**Injection rule:** memories are fetched (via `search` on the incoming prompt)
and appended to the system message **only for newly-created sessions**. For
**resumed** sessions, the persisted system message is used unchanged — exact
replay remains the source of truth, and re-injecting could drift the context.

## CLI / dev wiring

Optional flags on both `RunArgs` and `DevArgs` (memory enabled only when
URL + user id are both present):

| Flag | Env | Purpose |
|------|-----|---------|
| `--memory-url` | `FLUERS_MEM0_URL` | mem0 base URL |
| `--memory-api-key` | `FLUERS_MEM0_API_KEY` | API key (`Token` header) |
| `--memory-user-id` | `FLUERS_MEMORY_USER_ID` | per-user partition |
| `--memory-limit` | — | top_k for injection search (default 5) |

## Tests

**Always-on (no external services):**
- `InMemoryMemoryAdapter` add/search/clear round-trip
- `format_memories` deterministic output
- `MemoryTurnSink` stores only the text user/assistant pair (not tool outputs)
- `FanoutTurnSink` calls both sinks in order; memory-sink failure does not
  affect the persistence sink
- CLI flag/env parsing

**HTTP adapter (mock server — no live mem0):**
- `wiremock`-based tests assert the exact request paths/headers/bodies and
  parse the response (`add` path + `Token` auth, `search` body with
  query/filters/top_k, query trimming, `clear` query param, 401 handling,
  empty-key header omission).
- This hermetic mock-server approach superseded an earlier plan for a
  `FLUERS_MEM0_TEST_URL`-gated live test; the mock tests are stricter and
  require no credentials. (Live mem0 verification is a manual, pre-deploy
  step, not part of the workspace suite.)

**Full gate:** `cargo fmt --all`, strict clippy, `cargo nextest run --workspace`
stays green without mem0/Docker/credentials.

## Non-goals / deferred

- Self-hosted `server/` REST surface (`/memories`, `/search`) — future adapter.
- Memory update/delete UI — `clear()` only.
- Per-turn memory re-injection on resumed sessions — explicit non-goal (exact
  replay wins).
- Dumping tool outputs / file contents / images into mem0 — explicit non-goal.
- mem0 outage breaking persistence or runs — explicit non-goal (fail-open).

## Security & failure model (from red-team review)

- **Fail-open is error-only, not panic-safe.** `MemoryTurnSink` swallows `Err`
  from the adapter; a `panic!` inside a custom adapter would unwind through
  `FanoutTurnSink` and abort the turn loop. The shipped adapters
  (`Mem0RestAdapter`, `InMemoryMemoryAdapter`) are panic-free
  (`#![forbid(unsafe_code)]`, no `unwrap`/`expect` in production code), so this
  is safe in practice; custom adapters must uphold the same property.
- **Prompt injection (defense in depth).** Memories come from a third-party
  store. `format_memories` collapses each memory to a single line so a
  malicious memory cannot break out of the bullet list to inject top-level
  instructions. This is not a complete guarantee — treat mem0 as trusted
  infrastructure for your threat model and use per-user isolation.
- **Credential redaction.** Transport-error redaction scans for *any*
  `scheme://user:pass@host` URL in the message (not just the bare base URL),
  because reqwest errors include the full request URL. Self-hosted URLs with
  embedded passwords are redacted before logging.
- **Error-body bounding.** Non-2xx response bodies are truncated to 512 bytes
  in error messages so a backend that echoes request content cannot flood logs
  via the fail-open `warn!` path.

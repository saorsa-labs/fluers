# Config UX for MCP + subagents (MVP 4.6)

**Status:** design (committed before implementation).
**Makes 4d (MCP) + 4f (subagents) user-facing via `fluers run`.**

## Goal

Today `fluers run` wires only the built-in tools (read/write/bash/glob/grep).
The MCP tool adapter (4d) and subagent delegation (4f) are reachable **only**
via library API + tests. This slice adds a **TOML config** surface so a user
can declare MCP servers and subagent profiles, and `fluers run --config
fluers.toml --agent <name>` wires them into a run.

## Scope

**In scope:**

- Extend the existing `Config` TOML schema with `[agents.<name>]` and
  `[mcp.servers.<name>]` tables.
- A new `agent_config` module that resolves the selected agent + its subagents
  + referenced MCP servers into the `fluers-core` types (`SubagentProfile`,
  `TaskTool`) and `fluers-mcp` types (`McpServer`, adapted tools).
- Wire `fluers run` to load config, resolve the agent, connect MCP servers,
  build the tool list (+ `TaskTool` when subagents are declared), and run.
- CLI flag `--agent <name>` (defaults to `"default"` when `[agents.default]`
  exists; falls back to legacy behavior otherwise).
- Config-time validation: unknown subagent references, unknown MCP-server
  references, and recursive subagent cycles (detected while building the graph,
  not left to the runtime depth limit).
- Hermetic unit tests (no Node / Docker / network).

**Out of scope (deferred):**

- `fluers dev` (server-side) wiring. Server-side subagent/MCP config needs a
  per-request tool-builder because `TaskTool` holds a run `CancellationToken`
  — that belongs to a later server-config slice.
- A web/cloud-published "agent registry"; we only read a local TOML file.
- Hot-reload of config; load once at run start.
- Output-schema / result validation for subagent returns.
- Secrets from non-env sources (vault, files). Secrets come **only** from host
  env vars (by name).

## TOML schema

```toml
provider = "openrouter"
model = "minimax/minimax-m3"
api_key_env = "OPENROUTER_API_KEY"

[agents.default]
instructions = "You are a helpful coding agent."
builtin_tools = true          # default true at the top level
mcp_servers = ["fs"]          # references [mcp.servers.fs]
subagents = ["reviewer"]      # references [agents.reviewer]
max_subagent_depth = 5        # optional; default 5
max_subagent_delegations = 64 # optional; default 64

[agents.reviewer]
description = "Reviews changes for correctness risks."
instructions = "Review the work and return concrete risks."
builtin_tools = false         # default false for subagents
mcp_servers = []
subagents = []

[mcp.servers.fs]
transport = "stdio"           # only "stdio" supported (MVP)
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "."]
cwd = "."
request_timeout_ms = 60000
# destination env var -> host env var name. No raw secrets in the config file.
env_from = { GITHUB_TOKEN = "GITHUB_TOKEN" }
```

### Field semantics

| Section | Field | Meaning |
| ------- | ----- | ------- |
| top | `provider` / `model` / `api_key_env` / `base_url` | Existing defaults (unchanged). |
| top | `workdir` / `max_turns` / `turn_timeout_ms` / `tool_concurrency` | Existing defaults (unchanged). |
| `[agents.<name>]` | `instructions` | The agent's system message. |
| | `description` | Delegation guidance shown to the parent model. Required only for subagents (the parent needs it to pick a target). |
| | `builtin_tools` | Include the built-in read/write/bash/glob/grep tools. **Default `true` at the top level** (preserves existing CLI behaviour); **default `false` for subagents** (preserves Flue's profile-owned capability semantics — a parent's bash tool never leaks into a reviewer). |
| | `mcp_servers` | Names of `[mcp.servers.*]` to expose as tools. **Profile-owned** — the parent's MCP tools do not flow into subagents. |
| | `subagents` | Names of `[agents.*]` to declare as subagents. **Profile-owned.** |
| | `max_subagent_depth` | Optional override of the depth limit for this agent's `TaskTool` (default 5). |
| | `max_subagent_delegations` | Optional override of the shared delegation budget (default 64). |
| `[mcp.servers.<name>]` | `transport` | `"stdio"` only (MVP). |
| | `command` / `args` / `cwd` | Subprocess spec. |
| | `request_timeout_ms` | Per-request `tools/list` + `tools/call` timeout (default 60000). |
| | `env_from` | Map of `DEST = "HOST_ENV_NAME"`. Each host env var is read at run start and injected into the child; a missing/empty host var is an error. |

### Backwards compatibility

- If the config file has **no** `[agents.*]` sections, `fluers run` behaves
  exactly as today (legacy single-agent mode). `--agent` is ignored.
- The legacy individual flags (`--model`, `--max-turns`, …) keep working and
  override config-file values (existing precedence is unchanged).

## Resolution algorithm

When `fluers run` resolves the selected agent (default `"default"`):

1. **Parse** the TOML into the extended `Config`.
2. **Connect** every MCP server referenced by the selected agent (transitively,
   by its subagents) once, deduplicating by name. Each yields
   `Vec<Arc<dyn Tool>>`. (Connecting once per server avoids spawning the same
   subprocess multiple times; all profiles sharing a server clone the same
   `Arc<dyn Tool>` handles.)
3. **Build** the `SubagentProfile` graph for the selected agent, recursively:
   - `instructions`, `description` from the `[agents.<name>]` table.
   - `tools` = (builtins if `builtin_tools`) ++ (MCP tools for each name in
     `mcp_servers`). Profile-owned only.
   - `subagents` = recurse for each name in `subagents`.
   - `model` / `config` inherit from the parent (Flue semantics) — omitted
     fields stay `None` so `TaskTool::delegate` inherits at run time.
4. **Detect cycles** during the recursive build with a visited-set keyed by
   agent name. A cycle is a config error (e.g. `default` → `reviewer` →
   `default`).
5. **Validate references**: every `mcp_servers` / `subagents` name must exist;
   missing names are config errors reported with the list of known names.
6. If the selected agent has any subagents, construct a `TaskTool` (sharing the
   run `CancellationToken` and the `EventBus` as an `Arc<dyn EventSink>`) and
   prepend it to the top-level tool list. The built-in `task` tool's
   description lists the declared subagents (already implemented in 4f).
7. Run via the existing `run_agent` path with `--no-tools` still meaning
   **all** tools disabled (builtins + MCP + `task`).

## Provider / EventSink ownership changes

- The provider is currently constructed as a local value in `commands.rs::run`.
  Change it to `Arc<dyn ModelProvider>` so `TaskTool::new` (which takes
  `Arc<dyn ModelProvider>`) can share it. `run_agent(provider.as_ref(), ...)`.
- The `EventBus` becomes `Arc<EventBus>`; pass `Some(event_bus.clone() as
  Arc<dyn EventSink>)` into `TaskTool` and `event_bus.as_ref()` into
  `RunHooks` and the tracing/OTLP setup. `EventBus` already implements
  `EventSink` (4c).

## Files touched

- **New:** `crates/fluers-cli/src/agent_config.rs` — TOML types + resolution +
  validation + cycle detection. Keeps `commands.rs` from bloating.
- **Edit:** `crates/fluers-cli/src/config.rs` — add `agents`, `mcp` fields to
  `Config`; add `AgentToml` / `McpServerToml` / `McpToml` structs.
- **Edit:** `crates/fluers-cli/src/commands.rs` — add `--agent` flag; convert
  provider to `Arc`; build tools via `agent_config`; construct `TaskTool` when
  subagents are declared.
- **Edit:** `crates/fluers-cli/src/lib.rs` / `main.rs` — module + flag plumbing.

## Tests (hermetic)

- Parse a TOML with `[agents.*]` + `[mcp.servers.*]`; assert the resolved
  structures.
- Unknown subagent reference → error listing known names.
- Unknown MCP-server reference → error listing known names.
- Recursive cycle (`a → b → a`) → error naming the cycle.
- `env_from` resolves host env vars at run time; missing host var → error; the
  config file itself contains **no** secret values.
- `builtin_tools` defaults: `true` for the selected (top-level) agent, `false`
  for declared subagents.
- `--no-tools` suppresses builtins, MCP tools, and `task`.
- Resolved `SubagentProfile.tools` for a subagent does **not** include the
  parent's tools (profile-owned isolation).
- MCP transport behaviour is covered by `fluers-mcp`'s existing hermetic mock
  server tests; config UX tests do **not** spawn external MCP servers.

## Exit criteria

- A user can write a `fluers.toml` declaring an agent with built-in tools, one
  MCP server, and one subagent, run `fluers run --config fluers.toml`, and have
  the agent able to call the built-in tools, the MCP tool, and `task`.
- Config errors (unknown refs, cycles, missing env) are reported clearly.
- Legacy mode (no `[agents.*]`) is unchanged.
- `cargo nextest run --workspace` green; fmt + strict clippy clean.

## What's next (separate slice)

`docs/MVP35_BUILD_DEPLOY_DESIGN.md` — Dockerfile + `fluers build` +
`fluers deploy --target docker`. No cloud deploy until HTTP auth/guard exists.

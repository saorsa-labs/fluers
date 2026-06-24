# MVP 4d — Model Context Protocol (MCP) tool adapter

**Status:** design (committed before implementation).
**Scope slice:** stdio transport only.

## Goal

Let a Fluers agent call tools provided by an **external MCP server** (e.g. the
reference filesystem or git server) by exposing those tools as ordinary
`fluers_core::Tool` impls. This mirrors Flue's
`connectMcpServer` / `McpServerConnection` (`packages/runtime/src/mcp.ts`),
which adapts an MCP server's listed tools into Flue tool definitions.

## Decision: use `rmcp` internally, keep the public API SDK-independent

A mature official Rust SDK exists — [`rmcp`](https://crates.io/crates/rmcp)
(v1.8.0, ~13.5M downloads, maintained by the modelcontextprotocol org, updated
daily). Flue uses the official MCP JS SDK for the same job. We do **not** hand-roll
the protocol unless `rmcp` blocks us.

**Spike result (done before this doc):** `rmcp`'s client API is clean and stable:

```text
let client = ().serve(TokioChildProcess::new(cmd)?).await?;   // stdio client
let page: ListToolsResult = client.list_tools(cursor).await?;  // paginated
let res: CallToolResult = client
    .call_tool(CallToolRequestParams::new(name).with_arguments(obj))
    .await?;
client.cancel().await?;                                        // shutdown
```

Key `rmcp` types we consume (all behind our own API):

| `rmcp` type                  | Shape we care about                                                |
| ---------------------------- | ----------------------------------------------------------------- |
| `Tool`                       | `name`, `description: Option<_>`, `input_schema: JsonObject`      |
| `ListToolsResult`            | `tools: Vec<Tool>`, `next_cursor: Option<Cursor>` (`Cursor = String`) |
| `CallToolResult`             | `content: Vec<Content>`, `is_error: Option<bool>`                 |
| `RawContent` (via `Content`) | `Text { text }`, `Image { data, mime_type }`, `Audio`, `Resource`, `ResourceLink` |
| `RmcpError`                  | transport / protocol errors → mapped to bounded `McpError`        |

The public Fluers API exposes **no `rmcp` types**. If `rmcp`'s surface changes
or we ever swap it out, callers are insulated.

## Scope

**In scope:**

- stdio transport (subprocess: `command`, `args`, `env`, `cwd`).
- `initialize` handshake + `notifications/initialized`.
- `tools/list` with pagination + duplicate-cursor guard.
- `tools/call` with per-call timeout.
- Adapting discovered tools into `fluers_core::Tool` impls.
- Result mapping (text → text; image/audio/resource → summarized).
- Hermetic integration test (in-process, no Node/Docker/network).

**Out of scope (deferred):**

- HTTP / Streamable-HTTP / SSE transports.
- Resources, prompts, sampling, elicitation, tasks.
- Auth (OAuth / bearer).
- Multi-server CLI config / config-file server definitions.
- Output-schema validation of MCP results (Flue does this; we skip for MVP).
- `task_support` filtering (MCP tasks) — we skip task-only tools with a warning, as Flue does, but do not implement task execution.

## Public API (SDK-independent)

```rust
/// How to spawn a stdio MCP server subprocess.
pub struct StdioMcpServerConfig {
    /// Friendly name used in the adapted tool name (`mcp__<name>__<tool>`).
    pub name: String,
    /// Executable to launch.
    pub command: String,
    /// Args to pass.
    pub args: Vec<String>,
    /// Extra environment for the child (merged on top of the current env).
    pub env: HashMap<String, String>,
    /// Working directory for the child.
    pub cwd: Option<PathBuf>,
    /// Per-request timeout for `tools/list` and `tools/call`.
    pub request_timeout: Duration,
}

impl McpServer {
    /// Connect to a stdio MCP server, run the initialize handshake, discover tools.
    pub async fn connect_stdio(cfg: StdioMcpServerConfig) -> Result<McpServer>;

    /// Discovered tools, adapted to `fluers_core::Tool`.
    pub fn tools(&self) -> &[Arc<dyn fluers_core::Tool>];

    /// Move the adapted tools out (so an agent can own them).
    pub fn into_tools(self) -> Vec<Arc<dyn fluers_core::Tool>>;

    /// Shut the subprocess down gracefully.
    pub async fn shutdown(self) -> Result<()>;
}
```

`McpServer` holds the `rmcp` client handle internally (behind the public API).
The `McpTool` adapter clones an `Arc`-handle to the client so multiple tools
share one transport.

## Adapter: `McpTool: fluers_core::Tool`

```rust
struct McpTool {
    /// Shared client handle (one per server).
    client: Arc<McpClientHandle>,
    /// Original MCP tool name (sent verbatim in `tools/call`).
    mcp_name: String,
    /// Adapted definition (adapted name + mapped schema + description).
    definition: ToolDefinition,
    /// Per-call timeout.
    request_timeout: Duration,
}
```

- `definition()` returns the adapted `ToolDefinition`.
- `execute(ctx, input)` calls `tools/call` with `mcp_name` and `input` (the
  model's args object), racing the call against `ctx.cancel` and
  `request_timeout`.

### Tool naming (Flue-compatible)

Adapted name: `mcp__<sanitized server name>__<sanitized tool name>`.

`sanitize` replaces any char outside `[A-Za-z0-9_-]` with `_` and trims leading
/trailing `_` (matches Flue's `sanitizeToolNamePart`).

Duplicate adapted names within one server are rejected (a server advertising two
tools that collapse to the same adapted name is a misconfiguration; we keep the
first and log the rest, like Flue).

### Description

`MCP tool "<original name>" from server "<server name>".` plus the MCP
`description` field if present (Flue-compatible).

### Input schema mapping

MCP `input_schema: JsonObject` is a JSON Schema object. We store it verbatim in
`ToolDefinition::parameters::fields` (a `BTreeMap<String, Value>`). This means
the existing `validate_input` required-key check and the provider-facing schema
both Just Work — no dialect translation needed for the MVP.

## Result mapping (`CallToolResult` → `ToolResult`)

Walk `content: Vec<Content>` and build a model-facing text:

| `RawContent` variant | Maps to                                                          |
| -------------------- | --------------------------------------------------------------- |
| `Text { text }`      | the text, appended verbatim                                      |
| `Image { mime_type, data }` | `[image: <mime_type>]` (base64 **not** dumped — privacy + cost) |
| `Audio { mime_type, data }` | `[audio: <mime_type>]` (base64 not dumped)                      |
| `Resource { ... }`   | extracted text if the resource is text, else `[resource: <uri>]`|
| `ResourceLink { uri, name, .. }` | `[resource-link: <uri> (<name>)]`                     |

Empty content → `(MCP tool returned no content)`.

The flattened text becomes a single `content` block of type `text` on the
`ToolResult`. (Multiple blocks could be produced later, but a single text block
matches Flue and keeps the model contract simple.)

### `is_error` handling

`CallToolResult::is_error == Some(true)` means the server reported a logical
tool error (distinct from a transport error). To make this visible to the
existing fluers observability seam — `tool_result_ok()` flags a result as failed
when its first text block starts with `Error:` — we prefix the mapped text with
`Error: ` when `is_error` is set. This is the **same convention** the local
`bash`/`read` tools already use, so turn-end accounting and OTel `tool.ok`
attributes stay consistent without a new code path.

## Errors & privacy

- Transport / protocol / timeout failures return a **bounded** `McpError` (a
  short `Display` summary; we never embed the model's input args or the server's
  full response body in the error string). This mirrors the 4c `run_failed`
  truncation fix — telemetry channels must not carry user content.
- Per-call timeout is enforced via `tokio::time::timeout` racing `tools/call`;
  the subprocess is **not** killed on a single timeout (it may serve other
  calls), but a transport-level break propagates as `McpError`.
- The subprocess is killed on `shutdown()` / drop as a last resort.

## Cancellation

`McpTool::execute` `select!`s on:

1. the `tools/call` future,
2. `ctx.cancel.cancelled()`,
3. `tokio::time::sleep(request_timeout)`.

On cancel/timeout we return a bounded `McpError` ("cancelled" / "timed out after
<Ns"). We do **not** attempt to relay MCP cancellation notifications in the MVP
(the call future is simply dropped; `rmcp` tolerates this).

## Tests

**Unit (hermetic, always run):**

- name sanitization (collisions, unicode, trimming)
- duplicate adapted-name rejection
- schema round-trip (JsonObject → ParameterSchema → provider JSON)
- result mapping: text / image / audio / resource / empty
- `is_error` → `Error:` prefix
- error-string bounding (no args/response leak)

**Integration (hermetic, always run):**

- An **in-process mock MCP server** over stdio: a tiny `#[tokio::test]` helper
  that spawns a child running `fluers-mcp`'s own `examples`-style mock binary,
  OR — preferred — uses `rmcp`'s in-memory/pipe transport if available so we
  need no subprocess. The mock advertises two tools (`echo`, `failing`) and
  replies deterministically.
- `run_agent` integration: an agent configured with an MCP-provided `echo` tool
  + a mock provider that emits one `echo` tool call; assert the tool result is
  surfaced and the turn count is 1.
- **No** Node, Docker, network, or external MCP server required for workspace
  tests.

**Live (optional, gated by env):**

- `FLUERS_MCP_TEST_COMMAND` — if set, `McpServer::connect_stdio` is run against
  that command and a real `tools/list` + `tools/call` round-trip is asserted.
  Skipped (not failed) when unset.

## CLI wiring

Per advisor: **do not over-scope the CLI now.** The library integration test
proving `run_agent` can call an MCP tool is sufficient for 4d's exit criteria.
If we add a user-facing flag, it will be **config-file based** (a `[mcp.servers
.<name>]` table), not shell-string CLI flags (shell parsing is security/UX scope
creep). Defer to a follow-up.

## Exit criteria

- An agent can call a tool provided by an external stdio MCP server.
- `cargo nextest run --workspace` stays green with **no** external deps.
- fmt + strict clippy clean.
- Live `tools/list` + `tools/call` round-trip verified (gated test) when an MCP
  server is available.

## Out of scope reminders

HTTP/SSE transports, resources, prompts, sampling, auth, and multi-server CLI
config are explicitly deferred. See `MVP4_PLAN.md`.

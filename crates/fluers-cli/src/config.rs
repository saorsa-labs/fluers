//! Optional TOML config file support.
//!
//! A `fluers.toml` (or path passed via `--config`) carries defaults so you
//! don't repeat flags. **Keys are never stored in the file** — only the *name*
//! of the env var to read the key from (`api_key_env`). CLI flags override
//! config-file values, which override built-in defaults.
//!
//! See `docs/CONFIG_UX_DESIGN.md` for the `[agents.*]` and `[mcp.servers.*]`
//! agent/MCP config surface.
//!
//! # Trust boundary
//!
//! The config file author is **trusted**: agent `instructions`/`description`
//! are forwarded verbatim to the model (a malicious config could inject
//! prompts), and MCP `command`/`args` are exec'd directly (no shell). This
//! matches the trust model of every local tool runner — treat your
//! `fluers.toml` the same way you treat your `.bashrc`.
//! **No raw secrets appear in the file** — `env_from` maps to host env var
//! *names*, which are resolved at run start and injected into MCP subprocesses.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// The config file schema.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// Provider backend: `openrouter` | `minimax` | `custom`.
    pub provider: Option<String>,
    /// Custom base URL (with `--provider custom`).
    pub base_url: Option<String>,
    /// Default model id, e.g. `minimax/minimax-m3`.
    pub model: Option<String>,
    /// Name of the env var holding the API key (never the key itself).
    pub api_key_env: Option<String>,
    /// Working directory the sandbox is rooted in.
    pub workdir: Option<PathBuf>,
    /// Maximum model turns.
    pub max_turns: Option<usize>,
    /// Per-turn provider deadline, in milliseconds.
    pub turn_timeout_ms: Option<u64>,
    /// How many tool calls may run in parallel within a turn.
    pub tool_concurrency: Option<usize>,
    /// Named agent profiles. Empty in legacy mode (no `[agents.*]`).
    pub agents: BTreeMap<String, AgentToml>,
    /// MCP server declarations.
    pub mcp: McpToml,
}

/// MCP server config.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct McpToml {
    /// `[mcp.servers.<name>]` entries.
    pub servers: BTreeMap<String, McpServerToml>,
}

/// One MCP server declaration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct McpServerToml {
    /// Transport kind. Only `"stdio"` is supported (MVP).
    pub transport: Option<String>,
    /// Executable to launch.
    pub command: String,
    /// Args to pass.
    pub args: Vec<String>,
    /// Working directory for the child.
    pub cwd: Option<PathBuf>,
    /// Per-request timeout, in milliseconds (default 60000).
    pub request_timeout_ms: Option<u64>,
    /// Destination env var → host env var name. Each host env var is read at
    /// run start and injected into the child. No raw secrets in the config.
    pub env_from: BTreeMap<String, String>,
}

/// One agent profile declaration (`[agents.<name>]`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentToml {
    /// Delegation guidance shown to the parent model. Required for subagents
    /// (the parent needs it to pick a target); optional for the top-level
    /// agent.
    pub description: Option<String>,
    /// The agent's system message.
    pub instructions: Option<String>,
    /// Include the built-in read/write/bash/glob/grep tools. Defaults to
    /// `true` at the top level, `false` for subagents (resolved at build time).
    pub builtin_tools: Option<bool>,
    /// Names of `[mcp.servers.*]` to expose as tools. Profile-owned.
    pub mcp_servers: Vec<String>,
    /// Names of `[agents.*]` to declare as subagents. Profile-owned.
    pub subagents: Vec<String>,
    /// Optional override of the subagent recursion depth limit (default 5).
    pub max_subagent_depth: Option<usize>,
    /// Optional override of the shared delegation budget (default 64).
    pub max_subagent_delegations: Option<usize>,
}

impl Config {
    /// Load from a path, if it exists. Returns `Default` if the file is absent.
    ///
    /// # Errors
    /// Returns an error if the file exists but can't be read or parsed.
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        let cfg: Self =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
        Ok(cfg)
    }

    /// Whether any `[agents.*]` profiles are declared (i.e. the new config UX
    /// is active vs. legacy mode).
    #[must_use]
    pub fn has_agents(&self) -> bool {
        !self.agents.is_empty()
    }
}

//! Resolve a selected agent from [`Config`] into the runtime types:
//! the top-level tool list (built-ins + MCP tools + an optional [`TaskTool`])
//! and the declared [`SubagentProfile`] graph.
//!
//! See `docs/CONFIG_UX_DESIGN.md` for the design and TOML schema.
//!
//! # Validation performed at config time
//!
//! - Unknown subagent references (a name in `subagents` with no `[agents.*]`).
//! - Unknown MCP-server references (a name in `mcp_servers` with no
//!   `[mcp.servers.*]`).
//! - Recursive subagent cycles (`a → b → a`), detected with a visited-set while
//!   building the graph.
//! - Missing/empty host env vars referenced by `env_from` (resolved at connect
//!   time, just before the run).

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fluers_core::tool::Tool;
use fluers_core::{ModelProvider, SubagentOptions, SubagentProfile, TaskTool};
use fluers_mcp::{McpServer, StdioMcpServerConfig};
use fluers_runtime::SessionEnv;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, McpServerToml};

/// Default per-request MCP timeout (60s).
const DEFAULT_MCP_TIMEOUT_MS: u64 = 60_000;

/// Errors from agent-config resolution.
#[derive(Debug, Error)]
pub enum AgentConfigError {
    /// The selected agent name is not declared.
    #[error("agent \"{0}\" is not declared (known: {1})")]
    UnknownAgent(String, String),
    /// A subagent reference names an agent that doesn't exist.
    #[error("agent \"{agent}\" references unknown subagent \"{name}\" (known: {known})")]
    UnknownSubagent {
        agent: String,
        name: String,
        known: String,
    },
    /// An MCP-server reference names a server that doesn't exist.
    #[error("agent \"{agent}\" references unknown MCP server \"{name}\" (known: {known})")]
    UnknownMcpServer {
        agent: String,
        name: String,
        known: String,
    },
    /// A recursive subagent cycle was detected.
    #[error("recursive subagent cycle detected: {0}")]
    Cycle(String),
    /// A host env var referenced by `env_from` is missing or empty.
    #[error("MCP server \"{server}\": env_from references missing/empty host env var \"{var}\"")]
    MissingEnvVar { server: String, var: String },
    /// An MCP server transport is unsupported (MVP: stdio only).
    #[error("MCP server \"{0}\": unsupported transport (only \"stdio\" is supported)")]
    UnsupportedTransport(String),
    /// An MCP server failed to connect.
    #[error("MCP server \"{name}\" failed to connect: {source}")]
    McpConnect {
        name: String,
        #[source]
        source: fluers_mcp::McpError,
    },
}

/// Result alias.
pub type AgentConfigResult<T> = std::result::Result<T, AgentConfigError>;

/// The resolved agent: its tool list and, if subagents are declared, a ready
/// [`TaskTool`] already included in the tool list.
pub struct ResolvedAgent {
    /// The complete tool list for the top-level run (built-ins + MCP tools +
    /// `task` when subagents are declared).
    pub tools: Vec<Arc<dyn Tool>>,
}

impl std::fmt::Debug for ResolvedAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedAgent")
            .field("tool_count", &self.tools.len())
            .finish()
    }
}

/// A connection cache for MCP servers, so each referenced server connects once.
struct McpCache {
    /// Server name → adapted tool handles.
    tools: HashMap<String, Vec<Arc<dyn Tool>>>,
    /// Keep the servers alive (they own the subprocesses) until the run ends.
    _servers: Vec<McpServer>,
}

impl McpCache {
    fn new() -> Self {
        Self {
            tools: HashMap::new(),
            _servers: Vec::new(),
        }
    }

    /// Connect `name` if not already connected, returning its adapted tools.
    async fn connect(
        &mut self,
        name: &str,
        server: &McpServerToml,
    ) -> AgentConfigResult<Vec<Arc<dyn Tool>>> {
        if let Some(tools) = self.tools.get(name) {
            return Ok(tools.clone());
        }
        // Validate transport (MVP: stdio only).
        match server.transport.as_deref() {
            None | Some("stdio") => {}
            Some(other) => {
                return Err(AgentConfigError::UnsupportedTransport(format!(
                    "{name}: transport={other}"
                )));
            }
        }
        // Resolve env_from: read each host env var; error if missing/empty.
        let mut env = HashMap::new();
        for (dest, host_var) in &server.env_from {
            let val = std::env::var(host_var)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or(AgentConfigError::MissingEnvVar {
                    server: name.to_string(),
                    var: host_var.clone(),
                })?;
            env.insert(dest.clone(), val);
        }
        let cfg = StdioMcpServerConfig {
            name: name.to_string(),
            command: server.command.clone(),
            args: server.args.clone(),
            env,
            cwd: server.cwd.clone().or_else(|| Some(PathBuf::from("."))),
            request_timeout: Duration::from_millis(
                server.request_timeout_ms.unwrap_or(DEFAULT_MCP_TIMEOUT_MS),
            ),
        };
        let connected =
            McpServer::connect_stdio(cfg)
                .await
                .map_err(|source| AgentConfigError::McpConnect {
                    name: name.to_string(),
                    source,
                })?;
        let tools = connected.tools().to_vec();
        self.tools.insert(name.to_string(), tools);
        self._servers.push(connected);
        Ok(self.tools[name].clone())
    }
}

/// Resolve the selected agent into a [`ResolvedAgent`].
///
/// `builtin_tools` produces the standard read/write/bash/glob/grep set when
/// the agent (or subagent) opts into them. `provider`, `event_sink`, `cancel`,
/// and `env` are shared across the run / delegation tree.
///
/// # Errors
/// - [`AgentConfigError::UnknownAgent`] if `agent` is not declared.
/// - [`AgentConfigError::UnknownSubagent`] / [`AgentConfigError::UnknownMcpServer`]
///   for dangling references.
/// - [`AgentConfigError::Cycle`] for recursive subagent cycles.
/// - [`AgentConfigError::MissingEnvVar`] / [`AgentConfigError::UnsupportedTransport`]
///   / [`AgentConfigError::McpConnect`] for MCP failures.
pub async fn resolve_agent(
    cfg: &Config,
    agent: &str,
    builtin_tools: Vec<Arc<dyn Tool>>,
    provider: Arc<dyn ModelProvider>,
    event_sink: Option<Arc<dyn fluers_core::EventSink>>,
    cancel: CancellationToken,
    env: Arc<dyn SessionEnv>,
) -> AgentConfigResult<ResolvedAgent> {
    if !cfg.agents.contains_key(agent) {
        let known: Vec<&str> = cfg.agents.keys().map(String::as_str).collect();
        return Err(AgentConfigError::UnknownAgent(
            agent.to_string(),
            known.join(", "),
        ));
    }

    let mut mcp_cache = McpCache::new();

    // Build the subagent graph for this agent (with cycle detection).
    let subagents = build_subagents(cfg, agent, &mut mcp_cache, env.clone()).await?;

    // Build the top-level tool list.
    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();

    // Top-level agent: builtin_tools defaults to true.
    let agent_def = &cfg.agents[agent];
    let use_builtins = agent_def.builtin_tools.unwrap_or(true);
    if use_builtins {
        tools.extend(builtin_tools);
    }

    // Top-level MCP tools.
    for name in &agent_def.mcp_servers {
        let mcp_tools = resolve_mcp(cfg, agent, name, &mut mcp_cache).await?;
        tools.extend(mcp_tools);
    }

    // If this agent declares subagents, prepend a TaskTool (so it wins the
    // "task" name lookup) configured from the agent's depth/budget overrides.
    if !subagents.is_empty() {
        let depth = agent_def.max_subagent_depth;
        let delegations = agent_def.max_subagent_delegations;
        // Walk the graph to find the effective defaults (the selected agent's
        // overrides win; fall back to the SubagentOptions defaults).
        let options = SubagentOptions {
            max_depth: depth.unwrap_or(fluers_core::DEFAULT_MAX_DEPTH),
            max_delegations: delegations.unwrap_or(fluers_core::DEFAULT_MAX_DELEGATIONS),
        };
        let model = fluers_core::Model {
            id: cfg
                .model
                .clone()
                .unwrap_or_else(|| "openrouter/auto".to_string()),
        };
        let config = fluers_core::RunConfig::default();
        let task = Arc::new(TaskTool::new(
            provider, model, config, subagents, options, cancel, event_sink,
        ));
        tools.insert(0, task);
    }

    Ok(ResolvedAgent { tools })
}

/// Resolve an MCP server reference for `agent`, with a helpful error listing
/// known server names on failure.
async fn resolve_mcp(
    cfg: &Config,
    agent: &str,
    name: &str,
    cache: &mut McpCache,
) -> AgentConfigResult<Vec<Arc<dyn Tool>>> {
    let server = cfg.mcp.servers.get(name).ok_or_else(|| {
        let known: Vec<&str> = cfg.mcp.servers.keys().map(String::as_str).collect();
        AgentConfigError::UnknownMcpServer {
            agent: agent.to_string(),
            name: name.to_string(),
            known: known.join(", "),
        }
    })?;
    cache.connect(name, server).await
}

/// Recursively build the [`SubagentProfile`] graph for `agent`, with cycle
/// detection (visited-set keyed by agent name).
async fn build_subagents(
    cfg: &Config,
    agent: &str,
    cache: &mut McpCache,
    env: Arc<dyn SessionEnv>,
) -> AgentConfigResult<Vec<SubagentProfile>> {
    let mut visited: BTreeSet<String> = BTreeSet::new();
    visited.insert(agent.to_string());
    build_subagents_inner(cfg, agent, cache, &mut visited, env).await
}

async fn build_subagents_inner(
    cfg: &Config,
    agent: &str,
    cache: &mut McpCache,
    visited: &mut BTreeSet<String>,
    env: Arc<dyn SessionEnv>,
) -> AgentConfigResult<Vec<SubagentProfile>> {
    let agent_def = cfg.agents.get(agent).ok_or_else(|| {
        let known: Vec<&str> = cfg.agents.keys().map(String::as_str).collect();
        AgentConfigError::UnknownAgent(agent.to_string(), known.join(", "))
    })?;

    let mut out = Vec::new();
    for child_name in &agent_def.subagents {
        // Cycle detection.
        if !visited.insert(child_name.clone()) {
            let chain = format_cycle_chain(visited, child_name);
            return Err(AgentConfigError::Cycle(chain));
        }

        let child_def = cfg.agents.get(child_name).ok_or_else(|| {
            let known: Vec<&str> = cfg.agents.keys().map(String::as_str).collect();
            AgentConfigError::UnknownSubagent {
                agent: agent.to_string(),
                name: child_name.clone(),
                known: known.join(", "),
            }
        })?;

        // Subagent tools: profile-owned. builtins default to false.
        let mut child_tools: Vec<Arc<dyn Tool>> = Vec::new();
        if child_def.builtin_tools.unwrap_or(false) {
            child_tools.extend(fluers_runtime::mvp_tools(env.clone()));
        }
        for mcp_name in &child_def.mcp_servers {
            let mcp_tools = resolve_mcp(cfg, child_name, mcp_name, cache).await?;
            child_tools.extend(mcp_tools);
        }

        // Recurse into the child's own subagents.
        let child_subagents = Box::pin(build_subagents_inner(
            cfg,
            child_name,
            cache,
            visited,
            env.clone(),
        ))
        .await?;

        let mut profile = SubagentProfile::new(
            child_name.clone(),
            child_def.instructions.clone().unwrap_or_default(),
        );
        if let Some(desc) = &child_def.description {
            profile = profile.with_description(desc.clone());
        }
        for t in child_tools {
            profile = profile.with_tool(t);
        }
        for s in child_subagents {
            profile = profile.with_subagent(s);
        }
        out.push(profile);

        // Backtrack so siblings aren't flagged as cycles.
        visited.remove(child_name);
    }
    Ok(out)
}

/// Render a human-readable cycle chain for an error message.
fn format_cycle_chain(visited: &BTreeSet<String>, repeated: &str) -> String {
    // Best-effort: show the visited set + the repeated name.
    let mut names: Vec<&str> = visited.iter().map(String::as_str).collect();
    names.push(repeated);
    names.join(" → ")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{AgentToml, Config, McpServerToml, McpToml};
    use std::collections::BTreeMap;

    fn agent(instructions: &str) -> AgentToml {
        AgentToml {
            description: None,
            instructions: Some(instructions.into()),
            builtin_tools: None,
            mcp_servers: vec![],
            subagents: vec![],
            max_subagent_depth: None,
            max_subagent_delegations: None,
        }
    }

    fn cfg_with_agents(agents: Vec<(&str, AgentToml)>) -> Config {
        let mut c = Config::default();
        for (n, a) in agents {
            c.agents.insert(n.into(), a);
        }
        c
    }

    #[test]
    fn parse_full_toml_round_trips() {
        let toml = r#"
provider = "openrouter"
model = "minimax/minimax-m3"

[agents.default]
instructions = "root"
builtin_tools = true
subagents = ["reviewer"]

[agents.reviewer]
description = "reviews"
instructions = "you review"
builtin_tools = false

[mcp.servers.fs]
command = "npx"
args = ["-y", "fs-server"]
env_from = { TOKEN = "MY_TOKEN" }
"#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert_eq!(cfg.provider.as_deref(), Some("openrouter"));
        assert!(cfg.has_agents());
        assert!(cfg.agents.contains_key("default"));
        assert!(cfg.agents.contains_key("reviewer"));
        assert_eq!(
            cfg.agents["default"].subagents,
            vec!["reviewer".to_string()]
        );
        assert_eq!(cfg.mcp.servers["fs"].command, "npx");
        assert_eq!(
            cfg.mcp.servers["fs"]
                .env_from
                .get("TOKEN")
                .map(String::as_str),
            Some("MY_TOKEN")
        );
    }

    #[test]
    fn legacy_config_still_parses() {
        let toml = r#"
provider = "openrouter"
model = "minimax/minimax-m3"
"#;
        let cfg: Config = toml::from_str(toml).expect("parse");
        assert!(!cfg.has_agents());
        assert!(cfg.mcp.servers.is_empty());
    }

    #[tokio::test]
    async fn unknown_selected_agent_errors() {
        let cfg = cfg_with_agents(vec![("default", agent("root"))]);
        let err = resolve_agent(
            &cfg,
            "ghost",
            vec![],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect_err("unknown agent");
        assert!(err.to_string().contains("not declared"));
        assert!(err.to_string().contains("ghost"));
        assert!(err.to_string().contains("default")); // known list
    }

    #[tokio::test]
    async fn unknown_subagent_reference_errors() {
        let mut a = agent("root");
        a.subagents = vec!["ghost".into()];
        let cfg = cfg_with_agents(vec![("default", a)]);
        let err = resolve_agent(
            &cfg,
            "default",
            vec![],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect_err("unknown subagent");
        let msg = err.to_string();
        assert!(msg.contains("unknown subagent"), "msg: {msg}");
        assert!(msg.contains("ghost"));
    }

    #[tokio::test]
    async fn unknown_mcp_server_reference_errors() {
        let mut a = agent("root");
        a.mcp_servers = vec!["ghost-fs".into()];
        let cfg = cfg_with_agents(vec![("default", a)]);
        let err = resolve_agent(
            &cfg,
            "default",
            vec![],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect_err("unknown mcp server");
        let msg = err.to_string();
        assert!(msg.contains("unknown MCP server"), "msg: {msg}");
        assert!(msg.contains("ghost-fs"));
    }

    #[tokio::test]
    async fn recursive_cycle_detected() {
        // default → reviewer → default
        let mut root = agent("root");
        root.subagents = vec!["reviewer".into()];
        let mut reviewer = agent("review");
        reviewer.subagents = vec!["default".into()];
        let cfg = cfg_with_agents(vec![("default", root), ("reviewer", reviewer)]);
        let err = resolve_agent(
            &cfg,
            "default",
            vec![],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect_err("should detect cycle");
        let msg = err.to_string();
        assert!(msg.contains("cycle"), "msg: {msg}");
    }

    #[tokio::test]
    async fn env_from_missing_host_var_errors() {
        let mut a = agent("root");
        a.mcp_servers = vec!["fs".into()];
        let server = McpServerToml {
            transport: Some("stdio".into()),
            command: "echo".into(),
            args: vec![],
            cwd: None,
            request_timeout_ms: None,
            env_from: BTreeMap::from([("TOKEN".into(), "DEFINITELY_UNSET_VAR_XYZ".into())]),
        };
        let mut cfg = cfg_with_agents(vec![("default", a)]);
        cfg.mcp = McpToml {
            servers: BTreeMap::from([("fs".into(), server)]),
        };
        let err = resolve_agent(
            &cfg,
            "default",
            vec![],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect_err("missing env var");
        let msg = err.to_string();
        assert!(msg.contains("missing/empty host env var"), "msg: {msg}");
        assert!(msg.contains("DEFINITELY_UNSET_VAR_XYZ"));
    }

    #[tokio::test]
    async fn unsupported_transport_errors() {
        let mut a = agent("root");
        a.mcp_servers = vec!["fs".into()];
        let server = McpServerToml {
            transport: Some("websocket".into()),
            command: "echo".into(),
            args: vec![],
            cwd: None,
            request_timeout_ms: None,
            env_from: BTreeMap::new(),
        };
        let mut cfg = cfg_with_agents(vec![("default", a)]);
        cfg.mcp = McpToml {
            servers: BTreeMap::from([("fs".into(), server)]),
        };
        let err = resolve_agent(
            &cfg,
            "default",
            vec![],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect_err("unsupported transport");
        assert!(err.to_string().contains("unsupported transport"));
    }

    #[tokio::test]
    async fn top_level_builtin_tools_default_true_no_subagents() {
        // An agent with no subagents and builtin_tools unset should get the
        // passed-in builtin tools and NO task tool.
        let cfg = cfg_with_agents(vec![("default", agent("root"))]);
        let resolved = resolve_agent(
            &cfg,
            "default",
            vec![Arc::new(EchoBuiltin) as Arc<dyn Tool>],
            stub_provider(),
            None,
            cancel(),
            env().await,
        )
        .await
        .expect("resolve");
        assert_eq!(resolved.tools.len(), 1);
        // No "task" tool (no subagents).
        assert!(resolved.tools.iter().all(|t| t.definition().name != "task"));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    fn cancel() -> CancellationToken {
        CancellationToken::new()
    }

    async fn env() -> Arc<dyn SessionEnv> {
        Arc::new(
            fluers_runtime::LocalSessionEnv::new(
                &std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
                fluers_runtime::Limits::default(),
            )
            .await
            .unwrap(),
        )
    }

    fn stub_provider() -> Arc<dyn ModelProvider> {
        use async_trait::async_trait;
        struct Stub;
        #[async_trait]
        impl ModelProvider for Stub {
            async fn invoke(
                &self,
                _r: fluers_core::ModelRequest,
            ) -> fluers_core::error::Result<fluers_core::ModelResponse> {
                Err(fluers_core::error::CoreError::ModelProvider("stub".into()))
            }
        }
        Arc::new(Stub)
    }

    struct EchoBuiltin;
    #[async_trait::async_trait]
    impl Tool for EchoBuiltin {
        fn definition(&self) -> fluers_core::tool::ToolDefinition {
            fluers_core::tool::ToolDefinition {
                name: "echo".into(),
                label: "Echo".into(),
                description: "echo".into(),
                parameters: fluers_core::tool::ParameterSchema::default(),
            }
        }
        async fn execute(
            &self,
            _ctx: fluers_core::tool::InvokeContext,
            _input: serde_json::Value,
        ) -> fluers_core::error::Result<fluers_core::tool::ToolResult> {
            Ok(fluers_core::tool::ToolResult {
                content: vec![],
                details: None,
            })
        }
    }
}

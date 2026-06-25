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

use std::collections::HashMap;
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

/// The static resolution of a config-declared agent: everything that can be
/// built once (at startup or run start) and reused. The per-request
/// `task` tool is **not** here — it is built by [`tools_for_run`] from a
/// [`ToolRequestContext`] so cancellation / events / delegation budget stay
/// scoped to a single run.
///
/// [`tools_for_run`]: ResolvedAgentSpec::tools_for_run
pub struct ResolvedAgentSpec {
    /// Static tools (built-ins + MCP). Never includes `task`.
    pub static_tools: Vec<Arc<dyn Tool>>,
    /// The declared subagent graph (empty when the agent delegates nothing).
    pub subagents: Vec<SubagentProfile>,
    /// Depth / budget overrides for the top-level `task` tool.
    pub options: SubagentOptions,
    /// The agent's system message (used as the run's system prompt).
    pub instructions: Option<String>,
    /// The agent's delegation-guidance description (for `GET /agents`).
    pub description: Option<String>,
    /// Holds connected MCP server handles so their subprocesses live as long as
    /// the spec (and thus the agent registration / run).
    _mcp: McpCache,
}

impl std::fmt::Debug for ResolvedAgentSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedAgentSpec")
            .field("static_tool_count", &self.static_tools.len())
            .field("subagent_count", &self.subagents.len())
            .field("max_depth", &self.options.max_depth)
            .field("max_delegations", &self.options.max_delegations)
            .finish()
    }
}

impl ResolvedAgentSpec {
    /// Build the complete tool list for a single run.
    ///
    /// Returns the static tools, with a fresh top-level `TaskTool` **prepended**
    /// (so it wins the `task` name lookup) when subagents are declared. The
    /// `TaskTool` is bound to this run's cancel token + event sink + a fresh
    /// delegation budget, so concurrent runs are fully isolated.
    pub fn tools_for_run(&self, ctx: fluers_core::ToolRequestContext) -> Vec<Arc<dyn Tool>> {
        let mut tools = self.static_tools.clone();
        if !self.subagents.is_empty() {
            let task = Arc::new(TaskTool::new(
                ctx.provider,
                ctx.parent_model,
                ctx.parent_config,
                self.subagents.clone(),
                self.options,
                ctx.cancel,
                ctx.event_sink,
            ));
            tools.insert(0, task);
        }
        tools
    }
}

/// A connection cache for MCP servers, so each referenced server connects once.
struct McpCache {
    /// Server name → adapted tool handles.
    tools: HashMap<String, Vec<Arc<dyn Tool>>>,
    /// Holds `McpServer` handles so subprocesses aren't dropped while the
    /// cache lives. Note: the actual run-long lifetime comes from the
    /// `Arc<RunningService>` cloned into each adapted tool (in `fluers-mcp`);
    /// this vec is a secondary hold that is dropped when `resolve_agent`
    /// returns. Tools returned to the caller keep the subprocesses alive
    /// for the whole run via their cloned Arcs.
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
                    "server `{name}` declared transport `{other}`"
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

/// Resolve the selected agent into a [`ResolvedAgentSpec`] (static: MCP
/// connections, subagent graph, built-ins). Does **not** build the `task`
/// tool — call [`ResolvedAgentSpec::tools_for_run`] with a per-run
/// [`ToolRequestContext`] to get the full tool list.
///
/// `builtin_tools` is the standard read/write/bash/glob/grep set, included
/// when the agent opts into `builtin_tools` (top-level default `true`).
/// `env` is shared across the subagent graph build (each subagent's own
/// built-ins are constructed from it).
///
/// # Errors
/// - [`AgentConfigError::UnknownAgent`] if `agent` is not declared.
/// - [`AgentConfigError::UnknownSubagent`] / [`AgentConfigError::UnknownMcpServer`]
///   for dangling references.
/// - [`AgentConfigError::Cycle`] for recursive subagent cycles.
/// - [`AgentConfigError::MissingEnvVar`] / [`AgentConfigError::UnsupportedTransport`]
///   / [`AgentConfigError::McpConnect`] for MCP failures.
pub async fn resolve_agent_spec(
    cfg: &Config,
    agent: &str,
    builtin_tools: Vec<Arc<dyn Tool>>,
    env: Arc<dyn SessionEnv>,
) -> AgentConfigResult<ResolvedAgentSpec> {
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

    // Build the static top-level tool list (built-ins + MCP). No `task` here.
    let mut static_tools: Vec<Arc<dyn Tool>> = Vec::new();
    let agent_def = &cfg.agents[agent];
    if agent_def.builtin_tools.unwrap_or(true) {
        static_tools.extend(builtin_tools);
    }
    for name in &agent_def.mcp_servers {
        let mcp_tools = resolve_mcp(cfg, agent, name, &mut mcp_cache).await?;
        static_tools.extend(mcp_tools);
    }

    let options = SubagentOptions {
        max_depth: agent_def
            .max_subagent_depth
            .unwrap_or(fluers_core::DEFAULT_MAX_DEPTH),
        max_delegations: agent_def
            .max_subagent_delegations
            .unwrap_or(fluers_core::DEFAULT_MAX_DELEGATIONS),
    };

    Ok(ResolvedAgentSpec {
        static_tools,
        subagents,
        options,
        instructions: agent_def.instructions.clone(),
        description: agent_def.description.clone(),
        _mcp: mcp_cache,
    })
}

/// Convenience wrapper for the `fluers run` command (single run): resolve the
/// spec, then build the tool list for one run. Pass the **actual** parent
/// model + config — they are inherited by subagents that omit their own.
///
/// Hosts that serve many runs (the `dev` server) should use
/// [`resolve_agent_spec`] + [`ResolvedAgentSpec::tools_for_run`] so the
/// per-request `task` tool is built fresh per request.
#[allow(clippy::too_many_arguments)] // thin convenience wrapper; args mirror inputs
pub async fn resolve_agent(
    cfg: &Config,
    agent: &str,
    builtin_tools: Vec<Arc<dyn Tool>>,
    provider: Arc<dyn ModelProvider>,
    parent_model: fluers_core::Model,
    parent_config: fluers_core::RunConfig,
    event_sink: Option<Arc<dyn fluers_core::EventSink>>,
    cancel: CancellationToken,
    env: Arc<dyn SessionEnv>,
) -> AgentConfigResult<ResolvedAgent> {
    let spec = resolve_agent_spec(cfg, agent, builtin_tools, env).await?;
    let tools = spec.tools_for_run(fluers_core::ToolRequestContext {
        provider,
        parent_model,
        parent_config,
        cancel,
        event_sink,
    });
    Ok(ResolvedAgent { tools })
}

/// The resolved tool list for a single run (the `fluers run` convenience type).
pub struct ResolvedAgent {
    /// The complete tool list for the run (built-ins + MCP + `task` when
    /// subagents are declared).
    pub tools: Vec<Arc<dyn Tool>>,
}

impl std::fmt::Debug for ResolvedAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedAgent")
            .field("tool_count", &self.tools.len())
            .finish()
    }
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
    let mut path: Vec<String> = vec![agent.to_string()];
    build_subagents_inner(cfg, agent, cache, &mut path, env).await
}

async fn build_subagents_inner(
    cfg: &Config,
    agent: &str,
    cache: &mut McpCache,
    path: &mut Vec<String>,
    env: Arc<dyn SessionEnv>,
) -> AgentConfigResult<Vec<SubagentProfile>> {
    let agent_def = cfg.agents.get(agent).ok_or_else(|| {
        let known: Vec<&str> = cfg.agents.keys().map(String::as_str).collect();
        AgentConfigError::UnknownAgent(agent.to_string(), known.join(", "))
    })?;

    let mut out = Vec::new();
    for child_name in &agent_def.subagents {
        // Cycle detection: if the child is already on the current recursion
        // path, we've found a back-edge.
        if path.iter().any(|p| p == child_name) {
            let mut chain = path.clone();
            chain.push(child_name.clone());
            return Err(AgentConfigError::Cycle(chain.join(" → ")));
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

        // Recurse into the child's own subagents (push/pop for path tracking).
        path.push(child_name.clone());
        let child_subagents = Box::pin(build_subagents_inner(
            cfg,
            child_name,
            cache,
            path,
            env.clone(),
        ))
        .await?;
        path.pop();

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
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{AgentToml, Config, McpServerToml, McpToml};
    use fluers_core::InvokeContext;
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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
            fluers_core::Model { id: "test".into() },
            fluers_core::RunConfig::default(),
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

    #[tokio::test]
    async fn spec_static_tools_exclude_task_then_run_prepends_it() {
        // An agent WITH subagents: the spec's static_tools must NOT contain
        // `task` (it's per-request); tools_for_run must prepend it.
        let mut root = agent("root");
        root.subagents = vec!["worker".into()];
        let cfg = cfg_with_agents(vec![("default", root), ("worker", agent("a worker"))]);
        let spec = resolve_agent_spec(
            &cfg,
            "default",
            vec![Arc::new(EchoBuiltin) as Arc<dyn Tool>],
            env().await,
        )
        .await
        .expect("spec");
        // Static list: just the builtin (no task).
        assert_eq!(spec.static_tools.len(), 1);
        assert!(spec
            .static_tools
            .iter()
            .all(|t| t.definition().name != "task"));

        // tools_for_run: builtin + a fresh task prepended.
        let ctx = fluers_core::ToolRequestContext {
            provider: stub_provider(),
            parent_model: fluers_core::Model { id: "test".into() },
            parent_config: fluers_core::RunConfig::default(),
            cancel: cancel(),
            event_sink: None,
        };
        let tools = spec.tools_for_run(ctx);
        assert_eq!(tools.len(), 2);
        // task wins name lookup (prepended at index 0).
        assert_eq!(tools[0].definition().name, "task");
    }

    #[tokio::test]
    async fn two_runs_get_independent_delegation_budgets() {
        // Two tools_for_run calls (two requests) must produce TaskTools with
        // INDEPENDENT delegation budgets — exhausting one must not affect the
        // other. (A static shared TaskTool would fail this.)
        let mut root = agent("root");
        root.subagents = vec!["worker".into()];
        root.max_subagent_delegations = Some(1);
        let cfg = cfg_with_agents(vec![("default", root), ("worker", agent("a worker"))]);
        let spec = resolve_agent_spec(&cfg, "default", vec![], env().await)
            .await
            .expect("spec");
        let mk = || {
            spec.tools_for_run(fluers_core::ToolRequestContext {
                provider: text_provider(),
                parent_model: fluers_core::Model { id: "test".into() },
                parent_config: fluers_core::RunConfig::default(),
                cancel: cancel(),
                event_sink: None,
            })
        };
        let run_a = mk();
        let run_b = mk();
        // Both have a task tool.
        let task_a = run_a
            .iter()
            .find(|t| t.definition().name == "task")
            .expect("run_a has task");
        let task_b = run_b
            .iter()
            .find(|t| t.definition().name == "task")
            .expect("run_b has task");
        // Exhaust run_a's single delegation (the text provider lets the child run complete).
        let ctx = InvokeContext {
            tool_call_id: "a1".into(),
            cancel: cancel(),
        };
        let ok_a1 = task_a
            .execute(
                ctx,
                serde_json::json!({ "agent": "worker", "prompt": "go" }),
            )
            .await;
        assert!(ok_a1.is_ok(), "run_a first delegation: {ok_a1:?}");
        // run_a's next delegation must be budget-exhausted...
        let ctx2 = InvokeContext {
            tool_call_id: "a2".into(),
            cancel: cancel(),
        };
        let err_a = task_a
            .execute(
                ctx2,
                serde_json::json!({ "agent": "worker", "prompt": "again" }),
            )
            .await
            .expect_err("run_a budget exhausted");
        assert!(err_a.to_string().contains("budget exhausted"));
        // ...but run_b's first delegation must STILL succeed (independent budget).
        let ctx3 = InvokeContext {
            tool_call_id: "b1".into(),
            cancel: cancel(),
        };
        let ok_b = task_b
            .execute(
                ctx3,
                serde_json::json!({ "agent": "worker", "prompt": "fresh" }),
            )
            .await;
        assert!(ok_b.is_ok(), "run_b independent budget: {ok_b:?}");
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

    /// A provider that returns a minimal valid assistant text response, so a
    /// delegated child run completes successfully.
    fn text_provider() -> Arc<dyn ModelProvider> {
        use async_trait::async_trait;
        struct Text;
        #[async_trait]
        impl ModelProvider for Text {
            async fn invoke(
                &self,
                _r: fluers_core::ModelRequest,
            ) -> fluers_core::error::Result<fluers_core::ModelResponse> {
                Ok(fluers_core::ModelResponse {
                    messages: vec![fluers_core::message::AgentMessage {
                        role: fluers_core::message::Role::Assistant,
                        content: vec![fluers_core::message::ContentBlock::Text {
                            text: "child done".into(),
                        }],
                    }],
                })
            }
        }
        Arc::new(Text)
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

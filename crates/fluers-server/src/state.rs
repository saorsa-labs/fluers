//! Server state: the agent registry, session adapter, and run store.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use fluers_core::{Model, ModelProvider, RunConfig, Tool, ToolFactory};
use fluers_protocol::RunRecord;
use fluers_runtime::PersistenceAdapter;
use tokio_util::sync::CancellationToken;

/// A fully-resolved agent registered with the server.
///
/// Built by the host (e.g. the CLI `dev` command) and cloned out per request.
#[derive(Clone)]
pub struct AgentHandle {
    /// The model provider (OpenAI-compatible, Anthropic, etc.).
    pub provider: Arc<dyn ModelProvider>,
    /// The model id.
    pub model: Model,
    /// The tools the agent may call (legacy / no-subagents path).
    ///
    /// Ignored when [`AgentHandle::tool_factory`] is set.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Per-request tool builder. When set, takes precedence over [`tools`](AgentHandle::tools)
    /// and is called for every request to produce a fresh tool list (including
    /// a request-local `task` tool bound to that run's cancel token + event
    /// sink). `None` for the legacy static-tools path.
    pub tool_factory: Option<ToolFactory>,
    /// The run configuration (budgets, concurrency).
    pub config: RunConfig,
    /// The system prompt injected at session start.
    pub system_prompt: String,
    /// A short human-readable description (shown in `GET /agents`).
    pub description: String,
}

impl AgentHandle {
    /// Build the tool list for a single request.
    ///
    /// If a [`ToolFactory`] is set, it is called with a [`ToolRequestContext`]
    /// carrying this handle's provider/model/config plus the per-request
    /// `cancel` + `event_sink` — so any `task` tool it builds is correctly
    /// scoped to this run. Otherwise the static [`tools`](AgentHandle::tools)
    /// list is cloned.
    pub fn tools_for_request(
        &self,
        cancel: CancellationToken,
        event_sink: Option<Arc<dyn fluers_core::EventSink>>,
    ) -> Vec<Arc<dyn Tool>> {
        match &self.tool_factory {
            Some(factory) => factory(fluers_core::ToolRequestContext {
                provider: self.provider.clone(),
                parent_model: self.model.clone(),
                parent_config: self.config.clone(),
                cancel,
                event_sink,
                // The server wires no tool policy yet (allow-all).
                policy: None,
            }),
            None => self.tools.clone(),
        }
    }
}

/// Shared server state handed to every route handler.
pub struct ServerState {
    /// Registered agents keyed by route name.
    pub agents: RwLock<HashMap<String, AgentHandle>>,
    /// Session persistence backend (JSON-file, Postgres, …).
    pub sessions: Arc<dyn PersistenceAdapter>,
    /// In-memory run records keyed by run id.
    pub runs: RwLock<HashMap<Uuid, RunRecord>>,
}

impl ServerState {
    /// Create a new server state with the given session adapter and an empty
    /// agent registry + run store.
    #[must_use]
    pub fn new(sessions: Arc<dyn PersistenceAdapter>) -> Self {
        Self {
            agents: RwLock::new(HashMap::new()),
            sessions,
            runs: RwLock::new(HashMap::new()),
        }
    }

    /// Register an agent under `name`. Replaces any existing agent with that name.
    pub fn register(&self, name: impl Into<String>, handle: AgentHandle) {
        self.agents.write().insert(name.into(), handle);
    }

    /// Update a run record under its lock.
    pub async fn update_run(&self, run_id: Uuid, f: impl FnOnce(&mut RunRecord)) {
        if let Some(r) = self.runs.write().get_mut(&run_id) {
            f(r);
        }
    }
}

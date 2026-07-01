//! Server state: the agent registry, session adapter, run store, and the
//! options that gate network exposure (auth / body limit / CORS).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use uuid::Uuid;

use fluers_core::{Model, ModelProvider, RunConfig, Tool, ToolFactory};
use fluers_protocol::RunRecord;
use fluers_runtime::PersistenceAdapter;
use tokio_util::sync::CancellationToken;

/// Server-wide options: auth, request limits, CORS, run retention.
///
/// `serve_with_options` enforces the invariant that a non-loopback bind
/// requires [`ServerOptions::auth_token`] to be set — otherwise a misconfigured
/// `--host 0.0.0.0` would expose the (shell-wielding) agents to anyone who can
/// reach the socket.
#[derive(Clone, Debug)]
pub struct ServerOptions {
    /// Optional bearer token. When set, all routes except `/health` (and CORS
    /// preflight) require `Authorization: Bearer <token>`. When `None`, the
    /// server is open — only safe behind a loopback bind.
    pub auth_token: Option<String>,
    /// Max request body size in bytes. Guards against memory exhaustion from
    /// oversized prompts.
    pub body_limit_bytes: usize,
    /// CORS: when non-empty, only these origins are allowed. Empty = permissive
    /// (any origin) — the local-dev default.
    pub cors_origins: Vec<String>,
    /// Max run records retained in memory; oldest non-running records are
    /// evicted past this so the store cannot grow without bound.
    pub max_run_records: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            auth_token: None,
            body_limit_bytes: 1024 * 1024,
            cors_origins: Vec::new(),
            max_run_records: 4096,
        }
    }
}

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
    /// Active runs' cancel tokens, keyed by run id. Used to cancel a run on
    /// client disconnect (SSE drop) and to drain all runs on graceful shutdown.
    pub active_runs: RwLock<HashMap<Uuid, CancellationToken>>,
    /// Insertion order of run ids, for bounded retention (oldest evicted first).
    run_order: Mutex<VecDeque<Uuid>>,
    /// Server-wide options (auth / limits / CORS).
    pub options: ServerOptions,
}

impl ServerState {
    /// Create server state with the default options and an empty agent registry
    /// + run store.
    #[must_use]
    pub fn new(sessions: Arc<dyn PersistenceAdapter>) -> Self {
        Self::new_with_options(sessions, ServerOptions::default())
    }

    /// Create server state with explicit [`ServerOptions`].
    #[must_use]
    pub fn new_with_options(sessions: Arc<dyn PersistenceAdapter>, options: ServerOptions) -> Self {
        Self {
            agents: RwLock::new(HashMap::new()),
            sessions,
            runs: RwLock::new(HashMap::new()),
            active_runs: RwLock::new(HashMap::new()),
            run_order: Mutex::new(VecDeque::new()),
            options,
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

    /// Insert a run record, evicting oldest non-running records past
    /// `max_run_records` so the store cannot grow without bound.
    pub fn insert_run(&self, run_id: Uuid, record: RunRecord) {
        let max = self.options.max_run_records;
        let mut runs = self.runs.write();
        let mut order = self.run_order.lock();
        runs.insert(run_id, record);
        order.push_back(run_id);
        // Evict oldest non-running records first (never drop an active run if
        // avoidable); fall back to oldest overall if everything is still running.
        while runs.len() > max {
            let victim = order
                .iter()
                .copied()
                .find(|id| runs.get(id).is_some_and(|r| r.status != RunStatus::Running))
                .or_else(|| order.front().copied());
            match victim {
                Some(id) => {
                    runs.remove(&id);
                    order.retain(|x| *x != id);
                }
                None => break, // empty order — nothing to evict
            }
        }
    }

    /// Track an active run's cancel token (removed on completion; cancelled on
    /// shutdown / client disconnect).
    pub fn track_run(&self, run_id: Uuid, token: CancellationToken) {
        self.active_runs.write().insert(run_id, token);
    }

    /// Stop tracking an active run (call on completion).
    pub fn untrack_run(&self, run_id: Uuid) {
        self.active_runs.write().remove(&run_id);
    }

    /// Cancel every active run and flip still-`Running` records to `Failed`.
    /// Called on graceful shutdown so run records never freeze in `Running`.
    pub fn cancel_active_runs(&self) {
        let tokens: Vec<CancellationToken> = self.active_runs.read().values().cloned().collect();
        for t in tokens {
            t.cancel();
        }
        self.active_runs.write().clear();
        for r in self.runs.write().values_mut() {
            if r.status == RunStatus::Running {
                r.status = RunStatus::Failed;
            }
        }
    }
}

// Re-exported so the field reference above resolves without pulling the whole
// protocol crate into this module's use list.
use fluers_protocol::RunStatus;

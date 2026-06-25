//! Request-scoped tool construction.
//!
//! The dev server (and any host that serves many agent runs) must build some
//! tools **per request** because they carry per-run state — most notably the
//! built-in `task` tool ([`crate::subagent::TaskTool`]), which owns the run's
//! cancellation token, event sink, and delegation budget. Reusing one across
//! requests would cross-wire cancellation and events between concurrent runs.
//!
//! [`ToolRequestContext`] bundles exactly the per-run inputs a tool factory
//! needs, and [`ToolFactory`] is the type-erased closure a host stores to turn
//! that context into a concrete tool list. See `docs/DEV_CONFIG_UX_DESIGN.md`.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::event::EventSink;
use crate::model::{Model, ModelProvider};
use crate::runner::RunConfig;
use crate::tool::Tool;

/// Inputs needed to build a single request's tool list. Passed **by value**
/// into a [`ToolFactory`] so the factory can construct request-local tools
/// (e.g. a fresh top-level `TaskTool` bound to this run's cancel token + sink).
///
/// The `provider` / `parent_model` / `parent_config` are the agent's values
/// (the same for every request of that agent); only `cancel` + `event_sink`
/// vary per request. They are bundled together so the factory signature stays
/// a single argument.
#[derive(Clone)]
pub struct ToolRequestContext {
    /// The model provider (shared, stateless — safe to reuse across requests).
    pub provider: Arc<dyn ModelProvider>,
    /// The parent agent's model id. Inherited by subagents that omit their own.
    pub parent_model: Model,
    /// The parent agent's run config. Inherited by subagents that omit their own.
    pub parent_config: RunConfig,
    /// This run's cancellation token. Children inherit it via the `TaskTool`.
    pub cancel: CancellationToken,
    /// This run's event sink (OTel / tracing). Children emit to the same sink.
    pub event_sink: Option<Arc<dyn EventSink>>,
}

/// Builds the full tool list for a single request.
///
/// Stored on a server's agent handle; when set, it takes precedence over any
/// static tool list. The closure must be `Send + Sync` (called concurrently
/// from many request tasks) and `Clone`-cheap (it is an `Arc`).
pub type ToolFactory = Arc<dyn Fn(ToolRequestContext) -> Vec<Arc<dyn Tool>> + Send + Sync>;

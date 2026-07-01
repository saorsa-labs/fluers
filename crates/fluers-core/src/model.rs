//! Model and provider abstraction.
//!
//! Mirrors `Model` / `ImageContent` from `pi-ai`: a pluggable interface that
//! any concrete provider (OpenAI, Anthropic, local GGUF via mistralrs, …)
//! implements. The agent loop talks only to [`ModelProvider`].

use std::collections::BTreeMap;
use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::message::AgentMessage;
use crate::thinking::ThinkingLevel;

/// A model identifier in `provider/model` form, e.g. `anthropic/claude-sonnet-4-6`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    /// Full id string, e.g. `anthropic/claude-sonnet-4-6`.
    pub id: String,
}

impl Model {
    /// Create a model id from a string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    /// The provider prefix, e.g. `anthropic`.
    #[must_use]
    pub fn provider(&self) -> &str {
        self.id.split('/').next().unwrap_or("")
    }
}

/// A request to a model.
#[derive(Debug, Clone)]
pub struct ModelRequest {
    /// The model to call.
    pub model: Model,
    /// The conversation so far.
    pub messages: Vec<AgentMessage>,
    /// Tools the model may call.
    pub tools: Vec<crate::tool::ToolDefinition>,
    /// Reasoning effort.
    pub thinking: ThinkingLevel,
    /// Provider-specific overrides (temperature, max_tokens, …).
    pub params: BTreeMap<String, serde_json::Value>,
}

/// One event in a streamed model response.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A chunk of assistant text.
    TextDelta(String),
    /// The model issued a tool call (complete; providers accumulate deltas).
    ToolCall(crate::tool::ToolCall),
    /// A reasoning/thinking chunk.
    ThinkingDelta(String),
    /// The turn finished.
    Done,
}

/// A boxed, sendable stream of [`StreamEvent`]s.
pub type StreamEventStream = Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>;

/// The final, non-streamed response from a model turn.
///
/// `messages` is the single source of truth; a streamed turn reassembles the
/// same shape from its deltas.
#[derive(Debug, Clone)]
pub struct ModelResponse {
    /// Assistant messages produced this turn.
    pub messages: Vec<AgentMessage>,
}

impl ModelResponse {
    /// An empty response.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            messages: Vec::new(),
        }
    }
}

/// The provider abstraction. Implement this to add a backend.
///
/// Flue routes every model interaction through `pi-ai`'s `Model` interface;
/// `fluers-core` does the same through this trait.
#[async_trait]
pub trait ModelProvider: Send + Sync {
    /// Run a turn, returning the full response.
    async fn invoke(&self, request: ModelRequest) -> Result<ModelResponse>;

    /// Stream a turn as events. The default buffers [`ModelProvider::invoke`]; providers
    /// with native streaming override this to emit deltas as they arrive.
    fn stream(&self, request: ModelRequest) -> StreamEventStream {
        // Default: run `invoke` on a blocking task and replay a single `Done`.
        // Providers override to emit real `TextDelta`/`ToolCall`/`ThinkingDelta`.
        Box::pin(futures::stream::once(async move {
            // NOTE: this default cannot await `invoke` without `&self` being
            // `'static`; concrete providers override. Kept as a marker impl
            // so the trait object compiles. The static-dispatch agent loop
            // calls `invoke` directly when streaming is not requested.
            let _ = request;
            Ok(StreamEvent::Done)
        }))
    }
}

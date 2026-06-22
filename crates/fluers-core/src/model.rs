//! Model and provider abstraction.
//!
//! Mirrors `Model` / `ImageContent` from `pi-ai`: a pluggable interface that
//! any concrete provider (OpenAI, Anthropic, local GGUF via mistralrs, …)
//! implements. The agent loop talks only to [`ModelProvider`].

use std::collections::BTreeMap;

use async_trait::async_trait;
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
    /// The model issued a tool call.
    ToolCall(crate::tool::ToolCall),
    /// A reasoning/thinking chunk.
    ThinkingDelta(String),
    /// The turn finished.
    Done,
}

/// The final, non-streamed response from a model turn.
#[derive(Debug, Clone)]
pub struct ModelResponse {
    /// Assistant messages produced this turn.
    pub messages: Vec<AgentMessage>,
    /// The raw stream of events, in order.
    pub events: Vec<StreamEvent>,
}

/// The provider abstraction. Implement this to add a backend.
///
/// Flue routes every model interaction through `pi-ai`'s `Model` interface;
/// `fluers-core` does the same through this trait.
#[async_trait]
pub trait ModelProvider: Send + Sync {
    /// Run a turn, returning the full response.
    async fn invoke(&self, request: ModelRequest) -> Result<ModelResponse>;

    /// Stream a turn as events. Default collects [`invoke`].
    async fn stream(
        &self,
        request: ModelRequest,
        sink: &mut (dyn FnMut(StreamEvent) + Send),
    ) -> Result<()> {
        let resp = self.invoke(request).await?;
        for event in resp.events {
            sink(event);
        }
        Ok(())
    }
}

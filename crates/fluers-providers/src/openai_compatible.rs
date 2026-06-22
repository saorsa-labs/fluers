//! An OpenAI-compatible Chat Completions provider.
//!
//! Implements [`fluers_core::ModelProvider`] against any endpoint that speaks
//! the OpenAI Chat Completions wire format (`POST /v1/chat/completions` or
//! `/api/v1/chat/completions`). This covers OpenRouter, MiniMax (OpenAI mode),
//! OpenAI itself, vLLM, mistralrs' OpenAI server, and most local servers.
//!
//! MVP uses **non-streaming** `invoke`; streaming arrives in a later phase.

use std::collections::BTreeMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fluers_core::message::{AgentMessage, ContentBlock, Role};
use fluers_core::model::{Model, ModelProvider, ModelRequest, ModelResponse};
use fluers_core::tool::{ToolCall, ToolDefinition};
use fluers_core::{CoreError, Result};

/// A specialized [`Result`] for provider operations.
pub type ProviderResult<T> = std::result::Result<T, ProviderError>;

/// Errors raised by providers.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The HTTP transport failed.
    #[error("transport error: {0}")]
    Transport(String),
    /// The endpoint returned a non-2xx status.
    #[error("http {status}: {body}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Response body (may be truncated in logs).
        body: String,
    },
    /// The response could not be parsed.
    #[error("response parse error: {0}")]
    Parse(String),
}

/// An OpenAI-compatible provider.
///
/// Construct via [`OpenAiCompatibleProvider::new`] with a base URL + API key,
/// or the [`OpenAiCompatibleProvider::openrouter`] / [`OpenAiCompatibleProvider::minimax`]
/// convenience constructors.
pub struct OpenAiCompatibleProvider {
    base_url: String,
    api_key: String,
    client: reqwest::Client,
    extra_headers: BTreeMap<String, String>,
}

impl OpenAiCompatibleProvider {
    /// Create a provider. `base_url` should include the version prefix, e.g.
    /// `https://openrouter.ai/api/v1` or `https://api.minimaxi.com/v1`.
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            client: reqwest::Client::new(),
            extra_headers: BTreeMap::new(),
        }
    }

    /// Provider for [OpenRouter](https://openrouter.ai), reading the key from
    /// `OPENROUTER_API_KEY`.
    #[must_use]
    pub fn openrouter() -> Self {
        Self::new(
            "https://openrouter.ai/api/v1",
            std::env::var("OPENROUTER_API_KEY").unwrap_or_default(),
        )
    }

    /// Provider for the MiniMax international platform, reading the key from
    /// `MINIMAX_API_KEY`.
    #[must_use]
    pub fn minimax() -> Self {
        Self::new(
            "https://api.minimaxi.com/v1",
            std::env::var("MINIMAX_API_KEY").unwrap_or_default(),
        )
    }

    /// Add an extra HTTP header (e.g. OpenRouter's `HTTP-Referer` / `X-Title`).
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.insert(name.into(), value.into());
        self
    }

    /// Run one chat completion (non-streaming). Public so the CLI can call it
    /// directly; the [`ModelProvider`] impl wraps this.
    async fn chat(&self, request: ModelRequest) -> ProviderResult<ModelResponse> {
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));
        let body = ChatRequest::from_request(&request);
        let mut req = self
            .client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body);
        for (k, v) in &self.extra_headers {
            req = req.header(k, v);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ProviderError::Transport(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::HttpStatus {
                status: status.as_u16(),
                body: text,
            });
        }
        let parsed: ChatResponse = resp
            .json()
            .await
            .map_err(|e| ProviderError::Parse(e.to_string()))?;
        parsed.into_response()
    }
}

#[async_trait]
impl ModelProvider for OpenAiCompatibleProvider {
    async fn invoke(&self, request: ModelRequest) -> Result<ModelResponse> {
        self.chat(request).await.map_err(|e| match e {
            ProviderError::Transport(m) => CoreError::Transport(m),
            ProviderError::HttpStatus { body, .. } => CoreError::ModelProvider(body),
            ProviderError::Parse(m) => CoreError::ModelResponse(m),
        })
    }
}

// ---------------------------------------------------------------------------
// Request / response wire types
// ---------------------------------------------------------------------------

/// The OpenAI chat-completions request body.
#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<WireMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<WireTool>,
    tool_choice: &'static str,
    stream: bool,
}

impl<'a> ChatRequest<'a> {
    fn from_request(req: &'a ModelRequest) -> Self {
        let model = req.model.id.as_str();
        let messages = req.messages.iter().map(WireMessage::from_message).collect();
        let tools = req.tools.iter().map(WireTool::from_def).collect();
        Self {
            model,
            messages,
            tools,
            tool_choice: "auto",
            stream: false,
        }
    }
}

/// A single chat message in OpenAI wire format.
#[derive(Serialize, Deserialize)]
struct WireMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<WireToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

impl WireMessage {
    fn from_message(msg: &AgentMessage) -> Self {
        match msg.role {
            Role::System => Self {
                role: "system".into(),
                content: Some(text_of(msg)),
                tool_calls: None,
                tool_call_id: None,
            },
            Role::User => Self {
                role: "user".into(),
                content: Some(text_of(msg)),
                tool_calls: None,
                tool_call_id: None,
            },
            Role::Assistant => {
                let text = text_of(msg);
                let tool_calls: Vec<WireToolCall> = msg
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::ToolUse { id, call } => Some(WireToolCall {
                            id: id.clone(),
                            function: WireFunction {
                                name: call.name.clone(),
                                arguments: call.input.clone(),
                            },
                        }),
                        _ => None,
                    })
                    .collect();
                Self {
                    role: "assistant".into(),
                    content: if text.is_empty() && !tool_calls.is_empty() {
                        None
                    } else {
                        Some(text)
                    },
                    tool_calls: if tool_calls.is_empty() {
                        None
                    } else {
                        Some(tool_calls)
                    },
                    tool_call_id: None,
                }
            }
            Role::Tool => {
                // Tool message: content is the serialized ToolResult.
                let content = msg
                    .content
                    .iter()
                    .filter_map(|b| match b {
                        ContentBlock::ToolResult {
                            content,
                            tool_use_id,
                        } => Some((tool_use_id.clone(), content.clone())),
                        _ => None,
                    })
                    .next();
                let (tool_call_id, content_val) = content.unwrap_or((String::new(), Value::Null));
                Self {
                    role: "tool".into(),
                    content: Some(content_val.to_string()),
                    tool_calls: None,
                    tool_call_id: Some(tool_call_id),
                }
            }
            Role::Signal => Self {
                role: "system".into(),
                content: Some(text_of(msg)),
                tool_calls: None,
                tool_call_id: None,
            },
        }
    }
}

/// Extract concatenated text from a message's text blocks.
fn text_of(msg: &AgentMessage) -> String {
    msg.content
        .iter()
        .filter_map(|b| {
            if let ContentBlock::Text { text } = b {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("")
}

/// A tool definition in OpenAI wire format.
#[derive(Serialize)]
struct WireTool {
    #[serde(rename = "type")]
    kind: &'static str,
    function: WireToolDef,
}

impl WireTool {
    fn from_def(def: &ToolDefinition) -> Self {
        let parameters: Value = if def.parameters.fields.is_empty() {
            serde_json::json!({ "type": "object", "properties": {} })
        } else {
            serde_json::to_value(&def.parameters.fields).unwrap_or_default()
        };
        Self {
            kind: "function",
            function: WireToolDef {
                name: def.name.clone(),
                description: def.description.clone(),
                parameters,
            },
        }
    }
}

#[derive(Serialize)]
struct WireToolDef {
    name: String,
    description: String,
    parameters: Value,
}

/// A tool call in the response.
#[derive(Serialize, Deserialize)]
struct WireToolCall {
    id: String,
    function: WireFunction,
}

#[derive(Serialize, Deserialize)]
struct WireFunction {
    name: String,
    /// OpenAI sends this as a JSON *string*; we accept both string and object.
    arguments: Value,
}

/// The OpenAI chat-completions response body.
#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: RespMessage,
    #[allow(dead_code)]
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct RespMessage {
    content: Option<String>,
    tool_calls: Option<Vec<RespToolCall>>,
}

#[derive(Deserialize)]
struct RespToolCall {
    id: String,
    function: RespFunction,
}

#[derive(Deserialize)]
struct RespFunction {
    name: String,
    /// OpenAI delivers this as a JSON *string*; parse it into a Value.
    arguments: String,
}

impl ChatResponse {
    fn into_response(self) -> ProviderResult<ModelResponse> {
        let choice = self
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| ProviderError::Parse("response had no choices".into()))?;
        let mut blocks: Vec<ContentBlock> = Vec::new();
        if let Some(text) = choice.message.content {
            if !text.is_empty() {
                blocks.push(ContentBlock::Text { text });
            }
        }
        if let Some(tool_calls) = choice.message.tool_calls {
            for tc in tool_calls {
                let input: Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or_else(|_| serde_json::json!({ "_raw": tc.function.arguments }));
                blocks.push(ContentBlock::ToolUse {
                    id: tc.id,
                    call: ToolCall {
                        name: tc.function.name,
                        input,
                    },
                });
            }
        }
        Ok(ModelResponse {
            messages: vec![AgentMessage {
                role: Role::Assistant,
                content: blocks,
            }],
        })
    }
}

// Keep `Model` referenced for documentation / future use.
const _: fn() = || {
    fn _t(_m: &Model) {}
};

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
use fluers_core::model::{
    Model, ModelProvider, ModelRequest, ModelResponse, StreamEvent, StreamEventStream,
};
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
    /// A required API key was missing or empty.
    #[error("missing or empty API key in env var `{0}`")]
    MissingKey(String),
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
    /// `OPENROUTER_API_KEY`. **Panics-free**: returns an empty key silently;
    /// use [`try_openrouter`] to reject missing keys.
    ///
    /// [`try_openrouter`]: OpenAiCompatibleProvider::try_openrouter
    #[must_use]
    pub fn openrouter() -> Self {
        Self::new(
            "https://openrouter.ai/api/v1",
            std::env::var("OPENROUTER_API_KEY").unwrap_or_default(),
        )
    }

    /// Like [`openrouter`] but errors if `OPENROUTER_API_KEY` is unset/empty.
    ///
    /// [`openrouter`]: OpenAiCompatibleProvider::openrouter
    pub fn try_openrouter() -> ProviderResult<Self> {
        let key = std::env::var("OPENROUTER_API_KEY")
            .map_err(|_| ProviderError::MissingKey("OPENROUTER_API_KEY".into()))?;
        if key.trim().is_empty() {
            return Err(ProviderError::MissingKey("OPENROUTER_API_KEY".into()));
        }
        Ok(Self::new("https://openrouter.ai/api/v1", key))
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

    /// Like [`minimax`] but errors if `MINIMAX_API_KEY` is unset/empty.
    ///
    /// [`minimax`]: OpenAiCompatibleProvider::minimax
    pub fn try_minimax() -> ProviderResult<Self> {
        let key = std::env::var("MINIMAX_API_KEY")
            .map_err(|_| ProviderError::MissingKey("MINIMAX_API_KEY".into()))?;
        if key.trim().is_empty() {
            return Err(ProviderError::MissingKey("MINIMAX_API_KEY".into()));
        }
        Ok(Self::new("https://api.minimaxi.com/v1", key))
    }

    /// Like [`new`] but errors if the key is empty.
    pub fn try_new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        key_env_name: String,
    ) -> ProviderResult<Self> {
        let key = api_key.into();
        if key.trim().is_empty() {
            return Err(ProviderError::MissingKey(key_env_name));
        }
        Ok(Self::new(base_url, key))
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
        let body = ChatRequest::from_request(&request, false);
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

    /// Stream a chat completion as [`StreamEvent`]s.
    ///
    /// POSTs with `stream: true` and parses the OpenAI SSE wire format:
    /// `data: {json}\n\n` frames, `:` comments (ignored), and a terminal
    /// `data: [DONE]`. Text deltas yield [`StreamEvent::TextDelta`].
    ///
    /// Tool-call argument fragments are buffered per tool-call index and
    /// emitted as a single [`StreamEvent::ToolCall`] once each call's JSON
    /// arguments are complete (OpenAI streams the arguments string in
    /// fragments). A final [`StreamEvent::Done`] is emitted when the stream
    /// ends.
    fn stream_chat(&self, request: ModelRequest) -> StreamEventStream {
        let base_url = self.base_url.clone();
        let api_key = self.api_key.clone();
        let extra_headers = self.extra_headers.clone();
        let client = self.client.clone();

        let s = async_stream::stream! {
            let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
            let body = ChatRequest::from_request(&request, true);
            let mut req = client.post(&url).bearer_auth(&api_key).json(&body);
            for (k, v) in &extra_headers {
                req = req.header(k, v);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    yield Err(CoreError::Transport(e.to_string()));
                    return;
                }
            };
            let status = resp.status();
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                yield Err(CoreError::ModelProvider(format!("http {}: {}", status.as_u16(), text)));
                return;
            }

            use futures::StreamExt;
            let mut byte_stream = resp.bytes_stream();
            let mut buf = String::new();
            // Per tool-call-index accumulators: (name, arguments_so_far).
            let mut tool_accum: std::collections::BTreeMap<i64, (String, String)> =
                std::collections::BTreeMap::new();

            while let Some(chunk_res) = byte_stream.next().await {
                let chunk = match chunk_res {
                    Ok(c) => c,
                    Err(e) => {
                        yield Err(CoreError::Transport(e.to_string()));
                        return;
                    }
                };
                buf.push_str(&String::from_utf8_lossy(&chunk));

                // Process complete SSE frames delimited by blank lines.
                while let Some(blank) = buf.find("\n\n") {
                    let frame: String = buf.drain(..blank + 2).collect();
                    for line in frame.lines() {
                        let Some(payload) =
                            line.strip_prefix("data: ").or_else(|| line.strip_prefix("data:"))
                        else {
                            continue; // comments (`:`), event lines, blanks
                        };
                        let payload = payload.trim();
                        if payload == "[DONE]" {
                            // Flush buffered tool calls before finishing.
                            for (_, (name, args_json)) in tool_accum.clone().into_iter() {
                                let input: serde_json::Value =
                                    serde_json::from_str(&args_json).unwrap_or_else(|_| {
                                        serde_json::json!({ "_raw": args_json })
                                    });
                                yield Ok(StreamEvent::ToolCall(ToolCall { name, input }));
                            }
                            yield Ok(StreamEvent::Done);
                            return;
                        }
                        let parsed: StreamFrame = match serde_json::from_str(payload) {
                            Ok(p) => p,
                            Err(_) => continue, // skip malformed frames
                        };
                        let Some(delta) =
                            parsed.choices.first().and_then(|c| c.delta.as_ref())
                        else {
                            continue;
                        };
                        if let Some(text) = &delta.content {
                            if !text.is_empty() {
                                yield Ok(StreamEvent::TextDelta(text.clone()));
                            }
                        }
                        if let Some(reasoning) = &delta.reasoning {
                            if !reasoning.is_empty() {
                                yield Ok(StreamEvent::ThinkingDelta(reasoning.clone()));
                            }
                        }
                        if let Some(calls) = &delta.tool_calls {
                            for tc in calls {
                                let entry = tool_accum
                                    .entry(tc.index)
                                    .or_insert_with(|| (String::new(), String::new()));
                                if let Some(name) =
                                    tc.function.as_ref().and_then(|f| f.name.clone())
                                {
                                    entry.0 = name;
                                }
                                if let Some(args) =
                                    tc.function.as_ref().and_then(|f| f.arguments.clone())
                                {
                                    entry.1.push_str(&args);
                                }
                            }
                        }
                    }
                }
            }
            // Stream ended without [DONE]; flush tool calls then signal done.
            for (_, (name, args_json)) in tool_accum {
                let input: serde_json::Value =
                    serde_json::from_str(&args_json).unwrap_or_else(|_| {
                        serde_json::json!({ "_raw": args_json })
                    });
                yield Ok(StreamEvent::ToolCall(ToolCall { name, input }));
            }
            yield Ok(StreamEvent::Done);
        };
        Box::pin(s)
    }
}

#[async_trait]
impl ModelProvider for OpenAiCompatibleProvider {
    async fn invoke(&self, request: ModelRequest) -> Result<ModelResponse> {
        self.chat(request).await.map_err(|e| match e {
            ProviderError::Transport(m) => CoreError::Transport(m),
            ProviderError::HttpStatus { body, .. } => CoreError::ModelProvider(body),
            ProviderError::Parse(m) => CoreError::ModelResponse(m),
            ProviderError::MissingKey(envvar) => {
                CoreError::ModelProvider(format!("missing API key env var `{envvar}`"))
            }
        })
    }

    fn stream(&self, request: ModelRequest) -> StreamEventStream {
        self.stream_chat(request)
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
    fn from_request(req: &'a ModelRequest, stream: bool) -> Self {
        let model = req.model.id.as_str();
        let messages = req.messages.iter().map(WireMessage::from_message).collect();
        let tools = req.tools.iter().map(WireTool::from_def).collect();
        Self {
            model,
            messages,
            tools,
            tool_choice: "auto",
            stream,
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

// ---------------------------------------------------------------------------
// Streaming wire types (SSE frames)
// ---------------------------------------------------------------------------

/// One streamed SSE frame: `{ "choices": [ { "delta": {...}, ... } ] }`.
#[derive(Deserialize)]
struct StreamFrame {
    choices: Vec<StreamChoice>,
}

#[derive(Deserialize)]
struct StreamChoice {
    delta: Option<StreamDelta>,
}

/// A delta in a streamed frame. All fields optional because not every frame
/// carries every field (OpenRouter/MiniMax also send `reasoning` for models
/// with extended thinking).
#[derive(Deserialize)]
struct StreamDelta {
    content: Option<String>,
    reasoning: Option<String>,
    tool_calls: Option<Vec<StreamToolCallDelta>>,
}

/// A tool-call fragment. `index` identifies which tool call this fragment
/// belongs to (arguments arrive in pieces); `function.name` only appears on
/// the first fragment.
#[derive(Deserialize)]
struct StreamToolCallDelta {
    index: i64,
    function: Option<StreamToolFunctionDelta>,
}

#[derive(Deserialize)]
struct StreamToolFunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

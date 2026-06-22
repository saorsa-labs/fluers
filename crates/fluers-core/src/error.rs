//! Error types for the core crate.

use thiserror::Error;

/// A specialized [`Result`] for `fluers-core` operations.
pub type Result<T> = std::result::Result<T, CoreError>;

/// Errors raised by the core agent/model layer.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A tool call referenced an unknown tool.
    #[error("unknown tool: {0}")]
    UnknownTool(String),

    /// Tool input failed JSON-schema validation.
    #[error("tool input validation failed: {0}")]
    ToolInputValidation(String),

    /// Tool output failed to serialize or validate.
    #[error("tool output invalid: {0}")]
    ToolOutput(String),

    /// The model provider rejected the request.
    #[error("model provider error: {0}")]
    ModelProvider(String),

    /// The model response could not be parsed.
    #[error("model response parse error: {0}")]
    ModelResponse(String),

    /// An I/O or transport error talking to a provider.
    #[error("transport error: {0}")]
    Transport(String),

    /// A caller cancelled the operation (e.g. deadline elapsed).
    #[error("operation cancelled: {0}")]
    Cancelled(String),
}

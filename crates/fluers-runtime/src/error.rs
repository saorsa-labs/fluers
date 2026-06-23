//! Runtime error types.

use thiserror::Error;

/// A specialized [`Result`] for `fluers-runtime` operations.
pub type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

/// Errors raised by the runtime harness layer.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// An error originating in the core layer.
    #[error(transparent)]
    Core(#[from] fluers_core::CoreError),

    /// A skill definition was malformed.
    #[error("invalid skill: {0}")]
    InvalidSkill(String),

    /// A session was not found.
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// A persistence operation failed.
    #[error("persistence error: {0}")]
    Persistence(String),

    /// A tool name collided between two definitions.
    #[error("tool name conflict: {0}")]
    ToolNameConflict(String),

    /// A sandbox operation failed.
    #[error("sandbox error: {0}")]
    Sandbox(String),

    /// An I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<crate::persistence::PersistenceError> for RuntimeError {
    fn from(error: crate::persistence::PersistenceError) -> Self {
        match error {
            crate::persistence::PersistenceError::Backend(message) => Self::Persistence(message),
        }
    }
}

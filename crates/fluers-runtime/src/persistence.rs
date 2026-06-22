//! Persistence adapter contract.
//!
//! Lives in `fluers-runtime` (not `fluers-postgres`) so that [`SessionStore`]
//! can be generic over it. A concrete backend (`fluers-postgres`) implements
//! [`PersistenceAdapter`]; a JSON-file adapter ships in MVP 2 so sessions can
//! resume after a process restart without requiring Postgres.
//!
//! [`SessionStore`]: crate::session::SessionStore

use async_trait::async_trait;
use serde_json::Value;

/// The persistence contract for session state.
///
/// The in-memory [`SessionStore`](crate::session::SessionStore) swaps to an
/// impl of this trait when persistence is configured.
#[async_trait]
pub trait PersistenceAdapter: Send + Sync {
    /// Persist a session's serialized state.
    async fn save_session(&self, id: &str, data: &Value) -> Result<()>;

    /// Load a session's serialized state.
    async fn load_session(&self, id: &str) -> Result<Option<Value>>;
}

/// Errors from the persistence layer.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// A database / I/O error.
    #[error("persistence error: {0}")]
    Backend(String),
}

/// Result alias for persistence operations.
pub type Result<T> = std::result::Result<T, PersistenceError>;

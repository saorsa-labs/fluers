//! # fluers-postgres
//!
//! Postgres persistence adapter for Fluers sessions.
//!
//! Mirrors `@flue/postgres` against the persistence contract that lives in
//! `@flue/runtime/adapter`. MVP declares the adapter shape only; the
//! concrete `sqlx`-backed store lands in MVP 4.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;

/// The persistence adapter contract.
///
/// `fluers-runtime`'s in-memory `SessionStore` is swapped for an impl of
/// this trait when Postgres persistence is configured.
#[async_trait]
pub trait PersistenceAdapter: Send + Sync {
    /// Persist a session's serialized state.
    async fn save_session(&self, id: &str, data: &serde_json::Value) -> Result<()>;

    /// Load a session's serialized state.
    async fn load_session(&self, id: &str) -> Result<Option<serde_json::Value>>;
}

/// Errors from the persistence layer.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// A database error.
    #[error("database error: {0}")]
    Database(String),
}

/// Result alias.
pub type Result<T> = std::result::Result<T, PersistenceError>;

/// Placeholder Postgres adapter (no driver yet).
pub struct PostgresAdapter {
    #[allow(dead_code)]
    url: String,
}

impl PostgresAdapter {
    /// Create a placeholder adapter pointed at `url`.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

#[async_trait]
impl PersistenceAdapter for PostgresAdapter {
    async fn save_session(&self, _id: &str, _data: &serde_json::Value) -> Result<()> {
        Err(PersistenceError::Database(
            "PostgresAdapter not yet implemented (see PORTING_PLAN.md MVP 4)".into(),
        ))
    }

    async fn load_session(&self, _id: &str) -> Result<Option<serde_json::Value>> {
        Err(PersistenceError::Database(
            "PostgresAdapter not yet implemented (see PORTING_PLAN.md MVP 4)".into(),
        ))
    }
}

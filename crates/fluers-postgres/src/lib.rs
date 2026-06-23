//! # fluers-postgres
//!
//! Postgres persistence adapter for Fluers sessions.
//!
//! Mirrors `@flue/postgres` against the persistence contract that lives in
//! `fluers-runtime::persistence` (relocated so `SessionStore` can be generic
//! over it). MVP declares the adapter shape only; the concrete `sqlx`-backed
//! store lands in MVP 4.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;
use fluers_runtime::persistence::{PersistenceAdapter, PersistenceError, Result};
use serde_json::Value;

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
    async fn save_session(&self, _id: &str, _data: &Value) -> Result<()> {
        Err(PersistenceError::Backend(
            "PostgresAdapter not yet implemented (see PORTING_PLAN.md MVP 4)".into(),
        ))
    }

    async fn load_session(&self, _id: &str) -> Result<Option<Value>> {
        Err(PersistenceError::Backend(
            "PostgresAdapter not yet implemented (see PORTING_PLAN.md MVP 4)".into(),
        ))
    }

    async fn list_sessions(&self) -> Result<Vec<String>> {
        Err(PersistenceError::Backend(
            "PostgresAdapter not yet implemented (see PORTING_PLAN.md MVP 4)".into(),
        ))
    }
}

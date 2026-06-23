//! # fluers-postgres
//!
//! Postgres persistence adapter for Fluers sessions.
//!
//! Mirrors `@flue/postgres` against the persistence contract in
//! [`fluers_runtime::persistence`]. The [`SessionStore`] already handles the
//! typed [`SessionState`] envelope (schema-version checks, model/max-turns,
//! message log), so this adapter only ever stores **opaque JSON** — it is a
//! JSON-keyed key/value store backed by a `JSONB` column.
//!
//! ## Schema
//!
//! A single table, created idempotently on [`PostgresAdapter::connect`]:
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS fluers_sessions (
//!     id         TEXT        PRIMARY KEY,
//!     data       JSONB       NOT NULL,
//!     updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! ```
//!
//! Writes use `INSERT ... ON CONFLICT (id) DO UPDATE` (an upsert), so the same
//! session id can be saved repeatedly. Reads cast `data::text` so no sqlx
//! `json` type feature is required.
//!
//! ## Compile-time requirements
//!
//! Uses runtime SQL ([`sqlx::query`]) — **not** the `query!` macro — so the
//! crate compiles without a `DATABASE_URL` or `cargo sqlx prepare` step.
//!
//! ## Testing
//!
//! Integration tests are gated behind the `FLUERS_POSTGRES_TEST_URL`
//! environment variable and skip cleanly when it is unset, so
//! `cargo nextest run --workspace` stays green without a running database.
//!
//! [`SessionStore`]: fluers_runtime::session::SessionStore
//! [`SessionState`]: fluers_runtime::session::SessionState

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use async_trait::async_trait;
use fluers_runtime::persistence::{PersistenceAdapter, PersistenceError, Result};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

/// Idempotent schema-creation SQL.
const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS fluers_sessions (\
    id         TEXT        PRIMARY KEY,\
    data       JSONB       NOT NULL,\
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()\
)";

/// Upsert a session row.
const UPSERT_SQL: &str = "\
INSERT INTO fluers_sessions (id, data) VALUES ($1, $2::jsonb)\
ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()";

/// Read one session's data.
const SELECT_SQL: &str = "SELECT data::text AS data FROM fluers_sessions WHERE id = $1";

/// List all session ids (ordered for deterministic output). A defensive cap
/// is applied so an unbounded store cannot exhaust process memory on read.
const LIST_SQL: &str = "SELECT id FROM fluers_sessions ORDER BY id ASC LIMIT 100000";

/// Advisory-lock key used to serialize schema creation across concurrent
/// `connect()` calls. Postgres `CREATE TABLE IF NOT EXISTS` is **not** atomic
/// against concurrent catalog creation and can fail with a duplicate-type
/// error when two connections race; this transaction-scoped lock makes boot
/// safe across workers. `pg_advisory_xact_lock` is released automatically at
/// transaction end.
const SCHEMA_LOCK_KEY: i64 = 0x666C_7565; // "flue"

/// A Postgres-backed persistence adapter.
///
/// Construct with [`PostgresAdapter::connect`]. The held [`sqlx::PgPool`] is
/// cheaply cloneable and connection-pooled, so a single adapter is safe to
/// share (wrap in `Arc<dyn PersistenceAdapter>`) across a server's tasks.
pub struct PostgresAdapter {
    pool: sqlx::PgPool,
}

impl PostgresAdapter {
    /// Connect to `url`, configure the pool, and ensure the schema exists.
    ///
    /// `url` is a standard libpq connection string, e.g.
    /// `postgres://user:pass@localhost:5432/fluers`.
    ///
    /// # Errors
    /// Returns [`PersistenceError::Backend`] if the pool cannot connect, the
    /// URL is invalid, or the schema cannot be created.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect(url)
            .await
            .map_err(sql_err)?;
        let adapter = Self { pool };
        adapter.ensure_schema().await?;
        Ok(adapter)
    }

    /// Wrap an existing pool. The caller is responsible for the schema
    /// (call [`PostgresAdapter::ensure_schema`] if the table may not exist).
    #[must_use]
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Create the `fluers_sessions` table if it does not already exist.
    /// Idempotent and safe to call repeatedly, including concurrently from
    /// other connections (serialized via a transaction-scoped advisory lock).
    ///
    /// # Errors
    /// Returns [`PersistenceError::Backend`] on a database error.
    pub async fn ensure_schema(&self) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(sql_err)?;
        // Serialize schema creation across concurrent connections.
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(SCHEMA_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(sql_err)?;
        sqlx::query(SCHEMA_SQL)
            .execute(&mut *tx)
            .await
            .map_err(sql_err)?;
        tx.commit().await.map_err(sql_err)?;
        Ok(())
    }

    /// Borrow the underlying pool (for callers that want to run their own
    /// queries, e.g. migrations or admin commands).
    #[must_use]
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

#[async_trait]
impl PersistenceAdapter for PostgresAdapter {
    async fn save_session(&self, id: &str, data: &Value) -> Result<()> {
        let payload = serde_json::to_string(data)
            .map_err(|err| PersistenceError::Backend(format!("serialize session {id}: {err}")))?;
        sqlx::query(UPSERT_SQL)
            .bind(id)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(sql_err)?;
        Ok(())
    }

    async fn load_session(&self, id: &str) -> Result<Option<Value>> {
        let row: Option<(String,)> = sqlx::query_as(SELECT_SQL)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(sql_err)?;
        let Some((text,)) = row else {
            return Ok(None);
        };
        let value: Value = serde_json::from_str(&text)
            .map_err(|err| PersistenceError::Backend(format!("deserialize session {id}: {err}")))?;
        Ok(Some(value))
    }

    async fn list_sessions(&self) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(LIST_SQL)
            .fetch_all(&self.pool)
            .await
            .map_err(sql_err)?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

/// Map a [`sqlx::Error`] to a persistence error.
fn sql_err(err: sqlx::Error) -> PersistenceError {
    PersistenceError::Backend(format!("postgres: {err}"))
}

#[cfg(test)]
mod tests {
    //! Integration tests. These require a live Postgres; they are gated
    //! behind the `FLUERS_POSTGRES_TEST_URL` environment variable and skip
    //! cleanly (returning `Ok`) when it is unset, so workspace tests stay
    //! green without a database.
    //!
    //! To run them locally:
    //! ```sh
    //! docker run -d --name fluers-pg -p 5432:5432 \
    //!   -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=fluers postgres:16
    //! export FLUERS_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/fluers
    //! cargo nextest run -p fluers-postgres
    //! ```

    use super::*;
    use serde_json::json;

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    /// Read the test URL, or skip the test by returning `Ok(None)`. An empty
    /// string is treated as unset so a misconfigured CI var can't trigger a
    /// surprise network attempt.
    ///
    /// Each test uses a unique UUID-derived session id for isolation, so tests
    /// can run in parallel against a single shared table without interfering.
    async fn adapter_or_skip() -> TestResult<Option<PostgresAdapter>> {
        let url = std::env::var("FLUERS_POSTGRES_TEST_URL")
            .ok()
            .filter(|s| !s.is_empty());
        let Some(url) = url else {
            return Ok(None);
        };
        Ok(Some(PostgresAdapter::connect(&url).await?))
    }

    #[tokio::test]
    async fn save_then_load_roundtrips() -> TestResult {
        let Some(adapter) = adapter_or_skip().await? else {
            return Ok(());
        };
        let id = uuid::Uuid::new_v4().to_string();
        let data = json!({ "hello": "world", "n": 42, "nested": { "a": [1, 2, 3] } });

        adapter.save_session(&id, &data).await?;
        let loaded = adapter.load_session(&id).await?;

        assert_eq!(loaded, Some(data));
        Ok(())
    }

    #[tokio::test]
    async fn load_missing_returns_none() -> TestResult {
        let Some(adapter) = adapter_or_skip().await? else {
            return Ok(());
        };
        let loaded = adapter
            .load_session(&uuid::Uuid::new_v4().to_string())
            .await?;
        assert_eq!(loaded, None);
        Ok(())
    }

    #[tokio::test]
    async fn list_sessions_returns_ids() -> TestResult {
        let Some(adapter) = adapter_or_skip().await? else {
            return Ok(());
        };
        let id_a = uuid::Uuid::new_v4().to_string();
        let id_b = uuid::Uuid::new_v4().to_string();
        adapter.save_session(&id_a, &json!({ "a": 1 })).await?;
        adapter.save_session(&id_b, &json!({ "b": 2 })).await?;

        let sessions = adapter.list_sessions().await?;
        assert!(sessions.contains(&id_a), "missing {id_a} in {sessions:?}");
        assert!(sessions.contains(&id_b), "missing {id_b} in {sessions:?}");
        Ok(())
    }

    #[tokio::test]
    async fn save_is_upsert() -> TestResult {
        let Some(adapter) = adapter_or_skip().await? else {
            return Ok(());
        };
        let id = uuid::Uuid::new_v4().to_string();
        let first = json!({ "version": 1 });
        let second = json!({ "version": 2 });

        adapter.save_session(&id, &first).await?;
        adapter.save_session(&id, &second).await?;
        let loaded = adapter.load_session(&id).await?;

        assert_eq!(loaded, Some(second));
        Ok(())
    }

    #[tokio::test]
    async fn large_payload_roundtrips() -> TestResult {
        let Some(adapter) = adapter_or_skip().await? else {
            return Ok(());
        };
        let id = uuid::Uuid::new_v4().to_string();
        // ~256 KiB of JSON to exercise JSONB handling beyond a trivial size.
        let big_text = "x".repeat(256 * 1024);
        let data = json!({ "big": big_text });

        adapter.save_session(&id, &data).await?;
        let loaded = adapter.load_session(&id).await?;

        assert_eq!(loaded, Some(data));
        Ok(())
    }

    // ── Full-stack resume through `SessionStore` (the real exit criterion). ──

    /// A session saved via `SessionStore::save` round-trips and resumes through
    /// `SessionStore::load` against the Postgres adapter, including the typed
    /// `SessionState` envelope and schema-version check.
    #[tokio::test]
    async fn session_store_save_load_resume() -> TestResult {
        use fluers_core::{ContentBlock, Role};
        use fluers_runtime::session::SessionStore;

        let Some(adapter) = adapter_or_skip().await? else {
            return Ok(());
        };
        let store = SessionStore::new();
        let id = store.create_with_config("mock/model", 8, Some("be useful".into()));
        store.append(
            id,
            fluers_core::AgentMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: "persist me to postgres".into(),
                }],
            },
        )?;

        store.save(&adapter, id).await?;
        let loaded = SessionStore::load(&adapter, id).await?;
        let Some(session) = loaded else {
            return Err("session did not round-trip through Postgres".into());
        };

        assert_eq!(session.id, id);
        assert_eq!(session.model, "mock/model");
        assert_eq!(session.max_turns, 8);
        assert_eq!(session.system_message.as_deref(), Some("be useful"));
        assert_eq!(session.messages.len(), 1);
        assert!(SessionStore::list(&adapter).await?.contains(&id));
        Ok(())
    }
}

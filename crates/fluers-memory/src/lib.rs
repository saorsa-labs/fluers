//! # fluers-memory
//!
//! Semantic long-term memory for Fluers agents, backed by [mem0].
//!
//! This is a **complement** to [`fluers_runtime::PersistenceAdapter`] (MVP 4a),
//! not a replacement. `PersistenceAdapter` stores exact session state for
//! faithful resume-after-kill; this crate stores *semantic* facts extracted from
//! conversations (user preferences, durable context) so an agent can recall a
//! preference stated in an earlier session without it being in the transcript.
//!
//! ## What lives here
//!
//! - [`MemoryAdapter`] — the storage contract (`add` / `search` / `clear`),
//!   kept strictly separate from `PersistenceAdapter`.
//! - [`InMemoryMemoryAdapter`] — an in-process adapter for tests and local dev.
//! - [`format_memories`] — deterministic formatting for system-prompt injection.
//! - [`Mem0RestAdapter`] — the hosted-platform mem0 REST adapter
//!   (see [`mem0`]).
//! - [`MemoryTurnSink`] / [`FanoutTurnSink`] wiring — see [`sink`].
//!
//! See `docs/MVP4_MEMORY_DESIGN.md` for the full design, wire contract, and
//! sources.
//!
//! [mem0]: https://github.com/mem0ai/mem0

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod mem0;
pub mod sink;

pub use mem0::Mem0RestAdapter;
pub use sink::{compose, fanout_with_memory, MemoryTurnSink};

use std::collections::HashMap;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// A single extracted memory fact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Memory {
    /// The memory id (as returned by the backend).
    pub id: String,
    /// The extracted fact text, e.g. "prefers dark mode".
    pub memory: String,
    /// Relevance score (0.0–1.0), when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    /// Backend-specific metadata.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub metadata: Option<serde_json::Value>,
}

/// A single message contributed to the memory store. Only `role` + text
/// `content` are ever sent — tool outputs, images, and file contents are
/// deliberately excluded (privacy + cost).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MemoryMessage {
    /// The role: `"user"` or `"assistant"`.
    pub role: String,
    /// The text content.
    pub content: String,
}

/// A request to add memories for a user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryAddRequest {
    /// The per-user partition id.
    pub user_id: String,
    /// The messages to extract facts from (text user/assistant pairs only).
    pub messages: Vec<MemoryMessage>,
    /// Optional backend-specific metadata.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub metadata: Option<serde_json::Value>,
}

/// The outcome of an [`MemoryAdapter::add`] call.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryAddResponse {
    /// The ids of memories created/updated (best-effort; backends differ).
    pub ids: Vec<String>,
}

/// A request to search a user's memories.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySearchRequest {
    /// The per-user partition id.
    pub user_id: String,
    /// The search query.
    pub query: String,
    /// Maximum number of results.
    pub top_k: usize,
}

/// The memory-storage contract. **Distinct from** `PersistenceAdapter`: this is
/// a lossy, semantic layer, not an exact-replay store.
///
/// Implementations must be safe to share across tasks (`Send + Sync`). The
/// canonical adapter is [`Mem0RestAdapter`]; use [`InMemoryMemoryAdapter`] for
/// tests and local single-process dev.
#[async_trait]
pub trait MemoryAdapter: Send + Sync {
    /// Extract and store facts from `req`.
    ///
    /// # Errors
    /// Returns [`MemoryError::Backend`] on a storage failure.
    async fn add(&self, req: &MemoryAddRequest) -> Result<MemoryAddResponse>;

    /// Search a user's memories by `query`, ranked by relevance.
    ///
    /// # Errors
    /// Returns [`MemoryError::Backend`] on a storage failure.
    async fn search(&self, req: &MemorySearchRequest) -> Result<Vec<Memory>>;

    /// Delete **all** memories for `user_id`.
    ///
    /// # Errors
    /// Returns [`MemoryError::Backend`] on a storage failure.
    async fn clear(&self, user_id: &str) -> Result<()>;
}

/// Errors from the memory layer.
#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    /// A backend / I/O error.
    #[error("memory backend error: {0}")]
    Backend(String),
}

/// Result alias for memory operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

/// An in-process [`MemoryAdapter`] for tests and local single-process dev.
///
/// Stores memories per `user_id` in a `HashMap`. Search does a simple
/// case-insensitive substring match (no real embeddings) — sufficient for
/// deterministic tests, not for production recall.
#[derive(Default)]
pub struct InMemoryMemoryAdapter {
    inner: Mutex<HashMap<String, Vec<Memory>>>,
}

impl InMemoryMemoryAdapter {
    /// Create an empty in-memory adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl MemoryAdapter for InMemoryMemoryAdapter {
    async fn add(&self, req: &MemoryAddRequest) -> Result<MemoryAddResponse> {
        // Concatenate the contributed text and store a single fact. Real
        // backends run an LLM to extract salient facts; the in-memory adapter
        // stores the raw text so tests can assert deterministic content.
        let text: String = req
            .messages
            .iter()
            .map(|m| m.content.clone())
            .collect::<Vec<_>>()
            .join(" | ");
        let id = format!("mem-{}", uuid_like_id());
        let memory = Memory {
            id: id.clone(),
            memory: text,
            score: None,
            metadata: req.metadata.clone(),
        };
        let mut store = self.inner.lock();
        store.entry(req.user_id.clone()).or_default().push(memory);
        Ok(MemoryAddResponse { ids: vec![id] })
    }

    async fn search(&self, req: &MemorySearchRequest) -> Result<Vec<Memory>> {
        let store = self.inner.lock();
        let Some(all) = store.get(&req.user_id) else {
            return Ok(Vec::new());
        };
        let needle = req.query.to_lowercase();
        // Substring match on the query; deterministic order by id. This is a
        // stand-in for semantic search — real recall needs embeddings.
        let mut hits: Vec<Memory> = all
            .iter()
            .filter(|m| m.memory.to_lowercase().contains(&needle))
            .cloned()
            .collect();
        hits.sort_by(|a, b| a.id.cmp(&b.id));
        hits.truncate(req.top_k);
        Ok(hits)
    }

    async fn clear(&self, user_id: &str) -> Result<()> {
        let mut store = self.inner.lock();
        store.remove(user_id);
        Ok(())
    }
}

/// Generate a cheap, monotone-ish id without pulling a uuid dependency (the
/// workspace uuid is available transitively, but this keeps the adapter
/// self-contained for the in-memory path).
fn uuid_like_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Add a thread-local counter to break ties within the same nanosecond.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{seq:x}")
}

/// Format a slice of memories as a deterministic, compact block suitable for
/// system-prompt injection:
///
/// ```text
/// Relevant user memories:
/// - prefers dark mode
/// - timezone is Europe/Helsinki
/// ```
///
/// Empty input yields an empty string (so injection is a no-op when there are
/// no memories).
#[must_use]
pub fn format_memories(memories: &[Memory]) -> String {
    if memories.is_empty() {
        return String::new();
    }
    // Sort by id for deterministic output regardless of backend ranking order.
    let mut sorted: Vec<&Memory> = memories.iter().collect();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));
    let mut out = String::from("Relevant user memories:\n");
    for m in &sorted {
        out.push_str(&format!("- {}\n", m.memory));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(role: &str, content: &str) -> MemoryMessage {
        MemoryMessage {
            role: role.into(),
            content: content.into(),
        }
    }

    #[tokio::test]
    async fn in_memory_add_search_clear() {
        let adapter = InMemoryMemoryAdapter::new();
        adapter
            .add(&MemoryAddRequest {
                user_id: "alice".into(),
                messages: vec![msg("user", "I prefer dark mode")],
                metadata: None,
            })
            .await
            .unwrap();
        adapter
            .add(&MemoryAddRequest {
                user_id: "alice".into(),
                messages: vec![msg("user", "My timezone is Europe/Helsinki")],
                metadata: None,
            })
            .await
            .unwrap();

        let hits = adapter
            .search(&MemorySearchRequest {
                user_id: "alice".into(),
                query: "dark".into(),
                top_k: 5,
            })
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].memory.contains("dark mode"));

        // Clear wipes the user; subsequent search is empty.
        adapter.clear("alice").await.unwrap();
        let hits = adapter
            .search(&MemorySearchRequest {
                user_id: "alice".into(),
                query: "dark".into(),
                top_k: 5,
            })
            .await
            .unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn in_memory_search_is_user_scoped() {
        let adapter = InMemoryMemoryAdapter::new();
        adapter
            .add(&MemoryAddRequest {
                user_id: "alice".into(),
                messages: vec![msg("user", "alice's secret")],
                metadata: None,
            })
            .await
            .unwrap();
        let bob_hits = adapter
            .search(&MemorySearchRequest {
                user_id: "bob".into(),
                query: "secret".into(),
                top_k: 5,
            })
            .await
            .unwrap();
        assert!(bob_hits.is_empty(), "bob saw alice's memories");
    }

    #[tokio::test]
    async fn in_memory_search_respects_top_k() {
        let adapter = InMemoryMemoryAdapter::new();
        for i in 0..5 {
            adapter
                .add(&MemoryAddRequest {
                    user_id: "alice".into(),
                    messages: vec![msg("user", &format!("secret-{i}"))],
                    metadata: None,
                })
                .await
                .unwrap();
        }
        let hits = adapter
            .search(&MemorySearchRequest {
                user_id: "alice".into(),
                query: "secret".into(),
                top_k: 2,
            })
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn format_memories_empty_is_empty_string() {
        assert_eq!(format_memories(&[]), "");
    }

    #[test]
    fn format_memories_is_deterministic_regardless_of_input_order() {
        let m1 = Memory {
            id: "a".into(),
            memory: "prefers dark mode".into(),
            score: None,
            metadata: None,
        };
        let m2 = Memory {
            id: "b".into(),
            memory: "timezone is Europe/Helsinki".into(),
            score: None,
            metadata: None,
        };
        let one = format_memories(&[m1.clone(), m2.clone()]);
        let two = format_memories(&[m2, m1]);
        assert_eq!(one, two);
        assert!(one.contains("Relevant user memories:\n- prefers dark mode\n"));
    }
}

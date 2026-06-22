//! Sessions and the event store.
//!
//! Mirrors Flue's session machinery (`SessionStore`, `event-stream-store`,
//! `dispatch`/`invoke`). MVP holds sessions in memory; the persistence
//! adapter contract (`fluers-postgres`) replaces the store later.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use fluers_core::AgentMessage;

use crate::error::{RuntimeError, RuntimeResult};

/// A unique session id.
pub type SessionId = Uuid;

/// One session: its id, message log, and metadata.
#[derive(Debug, Clone)]
pub struct Session {
    /// The id.
    pub id: SessionId,
    /// Conversation so far.
    pub messages: Vec<AgentMessage>,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, String>,
}

/// An in-memory session store.
///
/// Replaced by the persistence adapter contract in a later phase.
#[derive(Default)]
pub struct SessionStore {
    inner: RwLock<HashMap<SessionId, Session>>,
}

impl SessionStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new session and return its id.
    pub fn create(&self) -> SessionId {
        let id = Uuid::new_v4();
        let session = Session {
            id,
            messages: Vec::new(),
            metadata: HashMap::new(),
        };
        self.inner.write().insert(id, session);
        id
    }

    /// Append a message to a session.
    pub fn append(&self, id: SessionId, message: AgentMessage) -> RuntimeResult<()> {
        let mut guard = self.inner.write();
        let session = guard
            .get_mut(&id)
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?;
        session.messages.push(message);
        Ok(())
    }

    /// Snapshot a session's messages.
    pub fn messages(&self, id: SessionId) -> RuntimeResult<Vec<AgentMessage>> {
        let guard = self.inner.read();
        guard
            .get(&id)
            .map(|s| s.messages.clone())
            .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))
    }
}

/// Shared pointer to a session store.
pub type SharedSessionStore = Arc<SessionStore>;

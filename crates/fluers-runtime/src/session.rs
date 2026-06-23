//! Sessions and the event store.
//!
//! Mirrors Flue's session machinery (`SessionStore`, `event-stream-store`,
//! `dispatch`/`invoke`). MVP holds sessions in memory while exposing a typed
//! persistence envelope for durable resume.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde_json::Value;
use uuid::Uuid;

use fluers_core::AgentMessage;

use crate::error::{RuntimeError, RuntimeResult};
use crate::persistence::PersistenceAdapter;

/// Current on-disk session envelope schema version.
pub const SCHEMA_VERSION: u32 = 1;

/// A unique session id.
pub type SessionId = Uuid;

/// On-disk envelope for a resumable session. Carries everything the
/// coordinator needs to reconstruct a run after a process restart.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SessionState {
    /// Envelope schema version. Bumped on breaking changes to this struct.
    pub schema_version: u32,
    /// The model id used for this session.
    pub model: String,
    /// Max turns configured for this session.
    pub max_turns: usize,
    /// The system message (instructions) for this session.
    pub system_message: Option<String>,
    /// The full conversation log.
    pub messages: Vec<AgentMessage>,
    /// Arbitrary metadata (e.g. created_at, tags).
    pub metadata: HashMap<String, String>,
}

/// One session: its id, configuration, message log, and metadata.
#[derive(Debug, Clone)]
pub struct Session {
    /// The id.
    pub id: SessionId,
    /// The model id used for this session.
    pub model: String,
    /// Max turns configured for this session.
    pub max_turns: usize,
    /// The system message (instructions) for this session.
    pub system_message: Option<String>,
    /// Conversation so far.
    pub messages: Vec<AgentMessage>,
    /// Arbitrary metadata.
    pub metadata: HashMap<String, String>,
}

impl Session {
    fn to_state(&self) -> SessionState {
        SessionState {
            schema_version: SCHEMA_VERSION,
            model: self.model.clone(),
            max_turns: self.max_turns,
            system_message: self.system_message.clone(),
            messages: self.messages.clone(),
            metadata: self.metadata.clone(),
        }
    }

    fn from_state(id: SessionId, state: SessionState) -> Self {
        Self {
            id,
            model: state.model,
            max_turns: state.max_turns,
            system_message: state.system_message,
            messages: state.messages,
            metadata: state.metadata,
        }
    }
}

/// An in-memory session store.
///
/// The store remains synchronous for in-process mutation (`append`), while
/// explicit `save`/`load` methods bridge to async persistence adapters.
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

    /// Create a new session with default, unspecified configuration and return
    /// its id.
    pub fn create(&self) -> SessionId {
        self.create_with_config(String::new(), 0, None)
    }

    /// Create a new session with explicit run configuration and return its id.
    pub fn create_with_config(
        &self,
        model: impl Into<String>,
        max_turns: usize,
        system_message: Option<String>,
    ) -> SessionId {
        let id = Uuid::new_v4();
        let session = Session {
            id,
            model: model.into(),
            max_turns,
            system_message,
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

    /// Persist a session through the provided adapter.
    pub async fn save(&self, adapter: &dyn PersistenceAdapter, id: SessionId) -> RuntimeResult<()> {
        let session = {
            let guard = self.inner.read();
            guard
                .get(&id)
                .cloned()
                .ok_or_else(|| RuntimeError::SessionNotFound(id.to_string()))?
        };
        let value = state_to_value(&session.to_state(), id)?;
        adapter
            .save_session(&id.to_string(), &value)
            .await
            .map_err(RuntimeError::from)
    }

    /// Load a session from the provided adapter.
    pub async fn load(
        adapter: &dyn PersistenceAdapter,
        id: SessionId,
    ) -> RuntimeResult<Option<Session>> {
        let Some(value) = adapter
            .load_session(&id.to_string())
            .await
            .map_err(RuntimeError::from)?
        else {
            return Ok(None);
        };
        let state = value_to_state(value, id)?;
        Ok(Some(Session::from_state(id, state)))
    }

    /// List all persisted sessions from the provided adapter.
    pub async fn list(adapter: &dyn PersistenceAdapter) -> RuntimeResult<Vec<SessionId>> {
        adapter
            .list_sessions()
            .await
            .map_err(RuntimeError::from)?
            .into_iter()
            .map(|raw| {
                Uuid::parse_str(&raw).map_err(|err| {
                    RuntimeError::Persistence(format!(
                        "invalid persisted session id `{raw}`: {err}"
                    ))
                })
            })
            .collect()
    }
}

fn state_to_value(state: &SessionState, id: SessionId) -> RuntimeResult<Value> {
    serde_json::to_value(state).map_err(|err| {
        RuntimeError::Persistence(format!("failed to serialize session `{id}`: {err}"))
    })
}

fn value_to_state(value: Value, id: SessionId) -> RuntimeResult<SessionState> {
    let state: SessionState = serde_json::from_value(value).map_err(|err| {
        RuntimeError::Persistence(format!("failed to deserialize session `{id}`: {err}"))
    })?;
    if state.schema_version != SCHEMA_VERSION {
        return Err(RuntimeError::Persistence(format!(
            "unsupported session schema version {} for `{id}` (expected {SCHEMA_VERSION})",
            state.schema_version
        )));
    }
    Ok(state)
}

/// Shared pointer to a session store.
pub type SharedSessionStore = Arc<SessionStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fluers_core::{ContentBlock, Role};
    use serde_json::{json, Value};
    use tokio::sync::Mutex;

    use crate::persistence::{PersistenceAdapter, Result as PersistenceResult};

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[derive(Default)]
    struct MockAdapter {
        sessions: Mutex<HashMap<String, Value>>,
    }

    impl MockAdapter {
        async fn put(&self, id: SessionId, value: Value) {
            self.sessions.lock().await.insert(id.to_string(), value);
        }
    }

    #[async_trait]
    impl PersistenceAdapter for MockAdapter {
        async fn save_session(&self, id: &str, data: &Value) -> PersistenceResult<()> {
            self.sessions
                .lock()
                .await
                .insert(id.to_string(), data.clone());
            Ok(())
        }

        async fn load_session(&self, id: &str) -> PersistenceResult<Option<Value>> {
            Ok(self.sessions.lock().await.get(id).cloned())
        }

        async fn list_sessions(&self) -> PersistenceResult<Vec<String>> {
            Ok(self.sessions.lock().await.keys().cloned().collect())
        }
    }

    fn text_message(role: Role, text: &str) -> AgentMessage {
        AgentMessage {
            role,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    fn first_text(messages: &[AgentMessage]) -> Option<&str> {
        messages
            .first()
            .and_then(|message| message.content.first())
            .and_then(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
    }

    #[test]
    fn session_state_roundtrips() -> TestResult {
        let state = SessionState {
            schema_version: SCHEMA_VERSION,
            model: "mock/model".into(),
            max_turns: 4,
            system_message: Some("be useful".into()),
            messages: vec![text_message(Role::User, "hello")],
            metadata: HashMap::from([("tag".into(), "test".into())]),
        };

        let value = serde_json::to_value(&state)?;
        let roundtrip: SessionState = serde_json::from_value(value)?;

        assert_eq!(roundtrip.schema_version, SCHEMA_VERSION);
        assert_eq!(roundtrip.model, "mock/model");
        assert_eq!(roundtrip.max_turns, 4);
        assert_eq!(roundtrip.system_message.as_deref(), Some("be useful"));
        assert_eq!(roundtrip.messages.len(), 1);
        assert_eq!(first_text(&roundtrip.messages), Some("hello"));
        assert_eq!(
            roundtrip.metadata.get("tag").map(String::as_str),
            Some("test")
        );
        Ok(())
    }

    #[tokio::test]
    async fn session_save_then_load() -> TestResult {
        let adapter = MockAdapter::default();
        let store = SessionStore::new();
        let id = store.create_with_config("mock/model", 8, Some("system".into()));
        store.append(id, text_message(Role::User, "persist me"))?;

        store.save(&adapter, id).await?;
        let loaded = SessionStore::load(&adapter, id).await?;
        let Some(session) = loaded else {
            return Err(std::io::Error::other("session was not loaded").into());
        };

        assert_eq!(session.id, id);
        assert_eq!(session.model, "mock/model");
        assert_eq!(session.max_turns, 8);
        assert_eq!(session.system_message.as_deref(), Some("system"));
        assert_eq!(session.messages.len(), 1);
        assert_eq!(first_text(&session.messages), Some("persist me"));
        Ok(())
    }

    #[tokio::test]
    async fn schema_version_mismatch_errors() -> TestResult {
        let adapter = MockAdapter::default();
        let id = Uuid::new_v4();
        adapter
            .put(
                id,
                json!({
                    "schema_version": SCHEMA_VERSION + 1,
                    "model": "mock/model",
                    "max_turns": 4,
                    "system_message": null,
                    "messages": [],
                    "metadata": {}
                }),
            )
            .await;

        let result = SessionStore::load(&adapter, id).await;

        assert!(
            matches!(result, Err(RuntimeError::Persistence(ref message)) if message.contains("unsupported session schema version")),
            "expected schema version error, got {result:?}"
        );
        Ok(())
    }
}

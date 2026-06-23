//! Session-aware agent runner coordination.
//!
//! This module bridges the pure `fluers-core` turn loop with runtime
//! persistence by implementing [`fluers_core::TurnSink`].

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use fluers_core::{AgentMessage, CoreError, Result as CoreResult, TurnSink};
use parking_lot::RwLock;

use crate::error::RuntimeResult;
use crate::persistence::PersistenceAdapter;
use crate::session::{Session, SessionId, SessionState, SessionStore, SCHEMA_VERSION};

/// Coordinator that drives `run_agent` while persisting the session after
/// every turn. Implements [`TurnSink`] so the loop calls back after each turn.
pub struct SessionRunner {
    adapter: Arc<dyn PersistenceAdapter>,
    session_id: SessionId,
    model: String,
    max_turns: usize,
    system_message: Option<String>,
    messages: Arc<RwLock<Vec<AgentMessage>>>,
    metadata: Arc<RwLock<HashMap<String, String>>>,
}

impl SessionRunner {
    /// Create a runner for a new or empty session.
    #[must_use]
    pub fn new(
        adapter: Arc<dyn PersistenceAdapter>,
        session_id: SessionId,
        model: impl Into<String>,
        max_turns: usize,
        system_message: Option<String>,
    ) -> Self {
        Self {
            adapter,
            session_id,
            model: model.into(),
            max_turns,
            system_message,
            messages: Arc::new(RwLock::new(Vec::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load a persisted session into a runner, if the adapter has one.
    pub async fn load(
        adapter: Arc<dyn PersistenceAdapter>,
        session_id: SessionId,
    ) -> RuntimeResult<Option<Self>> {
        let Some(session) = SessionStore::load(adapter.as_ref(), session_id).await? else {
            return Ok(None);
        };
        Ok(Some(Self::from_session(adapter, session)))
    }

    /// Snapshot the runner's current messages.
    #[must_use]
    pub fn messages(&self) -> Vec<AgentMessage> {
        self.messages.read().clone()
    }

    fn from_session(adapter: Arc<dyn PersistenceAdapter>, session: Session) -> Self {
        Self {
            adapter,
            session_id: session.id,
            model: session.model,
            max_turns: session.max_turns,
            system_message: session.system_message,
            messages: Arc::new(RwLock::new(session.messages)),
            metadata: Arc::new(RwLock::new(session.metadata)),
        }
    }

    fn state(&self, messages: Vec<AgentMessage>) -> SessionState {
        SessionState {
            schema_version: SCHEMA_VERSION,
            model: self.model.clone(),
            max_turns: self.max_turns,
            system_message: self.system_message.clone(),
            messages,
            metadata: self.metadata.read().clone(),
        }
    }
}

#[async_trait]
impl TurnSink for SessionRunner {
    async fn after_turn(&self, _turn: usize, messages: &[AgentMessage]) -> CoreResult<()> {
        let snapshot = messages.to_vec();
        {
            let mut current = self.messages.write();
            *current = snapshot.clone();
        }
        let state = self.state(snapshot);
        let value = serde_json::to_value(&state).map_err(|err| {
            CoreError::Transport(format!(
                "failed to serialize session `{}`: {err}",
                self.session_id
            ))
        })?;
        self.adapter
            .save_session(&self.session_id.to_string(), &value)
            .await
            .map_err(|err| {
                CoreError::Transport(format!(
                    "failed to save session `{}`: {err}",
                    self.session_id
                ))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fluers_core::{ContentBlock, Role};
    use serde_json::Value;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::persistence::{PersistenceAdapter, Result as PersistenceResult};

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[derive(Default)]
    struct MockAdapter {
        sessions: Mutex<HashMap<String, Value>>,
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

    #[tokio::test]
    async fn session_runner_persists_after_turn() -> TestResult {
        let adapter = Arc::new(MockAdapter::default());
        let session_id = Uuid::new_v4();
        let runner = SessionRunner::new(
            adapter.clone(),
            session_id,
            "mock/model",
            5,
            Some("be useful".into()),
        );
        let messages = vec![text_message(Role::User, "hello")];

        TurnSink::after_turn(&runner, 1, &messages).await?;

        let saved = adapter.load_session(&session_id.to_string()).await?;
        let Some(value) = saved else {
            return Err(std::io::Error::other("session was not saved").into());
        };
        let state: SessionState = serde_json::from_value(value)?;

        assert_eq!(state.schema_version, SCHEMA_VERSION);
        assert_eq!(state.model, "mock/model");
        assert_eq!(state.max_turns, 5);
        assert_eq!(state.system_message.as_deref(), Some("be useful"));
        assert_eq!(state.messages.len(), 1);
        assert_eq!(first_text(&state.messages), Some("hello"));
        assert_eq!(runner.messages().len(), 1);
        Ok(())
    }
}

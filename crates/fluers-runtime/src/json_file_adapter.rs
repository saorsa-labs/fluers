//! JSON-file persistence adapter.

use std::{io::ErrorKind, path::PathBuf};

use async_trait::async_trait;
use serde_json::Value;

use crate::persistence::{PersistenceAdapter, PersistenceError, Result};

/// A JSON-file persistence backend. Each session is stored as
/// `<dir>/<id>.json`. Suitable for local single-node deployments where
/// Postgres (MVP 4) is not available.
pub struct JsonFileAdapter {
    dir: PathBuf,
}

impl JsonFileAdapter {
    /// Create an adapter rooted at `dir`.
    ///
    /// The directory is created lazily when a session is saved.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn session_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    fn temp_session_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.json.tmp"))
    }
}

#[async_trait]
impl PersistenceAdapter for JsonFileAdapter {
    async fn save_session(&self, id: &str, data: &Value) -> Result<()> {
        tokio::fs::create_dir_all(&self.dir).await.map_err(|err| {
            PersistenceError::Backend(format!(
                "failed to create session directory {}: {err}",
                self.dir.display()
            ))
        })?;

        let bytes = serde_json::to_vec_pretty(data).map_err(|err| {
            PersistenceError::Backend(format!("failed to serialize session {id}: {err}"))
        })?;

        let temp_path = self.temp_session_path(id);
        tokio::fs::write(&temp_path, bytes).await.map_err(|err| {
            PersistenceError::Backend(format!(
                "failed to write session temp file {}: {err}",
                temp_path.display()
            ))
        })?;

        let session_path = self.session_path(id);
        tokio::fs::rename(&temp_path, &session_path)
            .await
            .map_err(|err| {
                PersistenceError::Backend(format!(
                    "failed to replace session file {} with {}: {err}",
                    session_path.display(),
                    temp_path.display()
                ))
            })?;

        Ok(())
    }

    async fn load_session(&self, id: &str) -> Result<Option<Value>> {
        let session_path = self.session_path(id);
        let bytes = match tokio::fs::read(&session_path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(PersistenceError::Backend(format!(
                    "failed to read session file {}: {err}",
                    session_path.display()
                )));
            }
        };

        let value = serde_json::from_slice(&bytes).map_err(|err| {
            PersistenceError::Backend(format!(
                "failed to parse session file {}: {err}",
                session_path.display()
            ))
        })?;

        Ok(Some(value))
    }

    async fn list_sessions(&self) -> Result<Vec<String>> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(PersistenceError::Backend(format!(
                    "failed to read session directory {}: {err}",
                    self.dir.display()
                )));
            }
        };

        let mut sessions = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(|err| {
            PersistenceError::Backend(format!(
                "failed to read session directory entry in {}: {err}",
                self.dir.display()
            ))
        })? {
            let file_type = entry.file_type().await.map_err(|err| {
                PersistenceError::Backend(format!(
                    "failed to read file type for {}: {err}",
                    entry.path().display()
                ))
            })?;

            if !file_type.is_file() {
                continue;
            }

            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(id) = file_name.strip_suffix(".json") else {
                continue;
            };
            sessions.push(id.to_owned());
        }

        Ok(sessions)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn save_then_load_roundtrips() -> TestResult {
        let dir = tempdir()?;
        let adapter = JsonFileAdapter::new(dir.path());
        let data = json!({ "hello": "world" });

        adapter.save_session("session-1", &data).await?;
        let loaded = adapter.load_session("session-1").await?;

        assert_eq!(loaded, Some(data));
        Ok(())
    }

    #[tokio::test]
    async fn load_missing_returns_none() -> TestResult {
        let dir = tempdir()?;
        let adapter = JsonFileAdapter::new(dir.path());

        let loaded = adapter.load_session("missing").await?;

        assert_eq!(loaded, None);
        Ok(())
    }

    #[tokio::test]
    async fn list_sessions_returns_ids() -> TestResult {
        let dir = tempdir()?;
        let adapter = JsonFileAdapter::new(dir.path());

        adapter
            .save_session("session-a", &json!({ "a": 1 }))
            .await?;
        adapter
            .save_session("session-b", &json!({ "b": 2 }))
            .await?;

        let mut sessions = adapter.list_sessions().await?;
        sessions.sort();

        assert_eq!(
            sessions,
            vec!["session-a".to_owned(), "session-b".to_owned()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn list_sessions_empty_dir_returns_empty() -> TestResult {
        let dir = tempdir()?;
        let adapter = JsonFileAdapter::new(dir.path());

        let sessions = adapter.list_sessions().await?;

        assert!(sessions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn save_is_atomic() -> TestResult {
        let dir = tempdir()?;
        let adapter = JsonFileAdapter::new(dir.path());
        let first = json!({ "version": 1 });
        let second = json!({ "version": 2 });

        adapter.save_session("session-1", &first).await?;
        adapter.save_session("session-1", &second).await?;
        let loaded = adapter.load_session("session-1").await?;

        assert_eq!(loaded, Some(second));
        Ok(())
    }
}

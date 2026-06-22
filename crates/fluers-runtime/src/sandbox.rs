//! Sandbox backends.
//!
//! A [`Sandbox`] manufactures a fresh [`SessionEnv`](crate::SessionEnv) for a
//! session. Flue ships three flavours — *virtual*, *local*, and *remote
//! container* — selected via `local()` / container providers. This crate
//! implements the local flavour; virtual + remote are stubbed for later
//! phases (see `PORTING_PLAN.md`).

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::env::{Limits, SessionEnv, ShellResult};
use crate::error::{RuntimeError, RuntimeResult};
use tokio_util::sync::CancellationToken;

/// A factory that produces a [`SessionEnv`] for one session.
#[async_trait]
pub trait Sandbox: Send + Sync {
    /// Human-readable name (e.g. `local`, `virtual`, `e2b`).
    fn name(&self) -> &str;

    /// Build the environment for a session rooted at `workdir`.
    async fn env_for(&self, workdir: &Path) -> RuntimeResult<Arc<dyn SessionEnv>>;
}

/// A local-filesystem sandbox: tools run against a real directory on disk.
///
/// This is the Rust equivalent of Flue's `local()` from
/// `@flue/runtime/node`.
pub struct LocalSandbox {
    root: PathBuf,
    limits: Limits,
}

impl LocalSandbox {
    /// Create a local sandbox rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            limits: Limits::default(),
        }
    }

    /// Override the default resource limits.
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
}

#[async_trait]
impl Sandbox for LocalSandbox {
    fn name(&self) -> &str {
        "local"
    }

    async fn env_for(&self, _workdir: &Path) -> RuntimeResult<Arc<dyn SessionEnv>> {
        // The full local SessionEnv (tokio::process + real fs ops) lands in
        // MVP 0. For now we return a stub so the trait graph compiles.
        Ok(Arc::new(StubEnv {
            root: self.root.clone(),
            limits: self.limits,
        }))
    }
}

/// Convenience constructor matching Flue's `local()` import.
#[must_use]
pub fn local() -> LocalSandbox {
    LocalSandbox::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

/// Placeholder environment. Real fs/exec impl arrives in MVP 0.
struct StubEnv {
    #[allow(dead_code)]
    root: PathBuf,
    #[allow(dead_code)]
    limits: Limits,
}

#[async_trait]
impl SessionEnv for StubEnv {
    async fn read_file(
        &self,
        _path: &Path,
        _max_lines: usize,
        _max_bytes: usize,
    ) -> RuntimeResult<String> {
        Err(RuntimeError::Sandbox(
            "local SessionEnv not yet implemented (see PORTING_PLAN.md MVP 0)".into(),
        ))
    }

    async fn write_file(&self, _path: &Path, _content: &str) -> RuntimeResult<()> {
        Err(RuntimeError::Sandbox(
            "local SessionEnv not yet implemented (see PORTING_PLAN.md MVP 0)".into(),
        ))
    }

    async fn exec(
        &self,
        _command: &str,
        _cwd: &Path,
        _timeout_ms: Option<u64>,
        _cancel: &CancellationToken,
    ) -> RuntimeResult<ShellResult> {
        Err(RuntimeError::Sandbox(
            "local SessionEnv not yet implemented (see PORTING_PLAN.md MVP 0)".into(),
        ))
    }

    async fn glob(&self, _pattern: &str, _limit: usize) -> RuntimeResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn grep(
        &self,
        _pattern: &str,
        _paths: &[&str],
        _max_matches: usize,
    ) -> RuntimeResult<Vec<String>> {
        Ok(Vec::new())
    }
}

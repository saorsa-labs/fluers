//! Sandbox backends.
//!
//! A [`Sandbox`] manufactures a fresh `SessionEnv` for a
//! session. Flue ships three flavours — *virtual*, *local*, and *remote
//! container* — selected via `local()` / container providers. This crate
//! implements the local flavour (see [`LocalSessionEnv`]); virtual + remote
//! are stubbed for later phases (see `PORTING_PLAN.md`).
//!
//! [`LocalSessionEnv`]: crate::LocalSessionEnv

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::env::{Limits, SessionEnv};
use crate::error::RuntimeResult;
use crate::local_env::LocalSessionEnv;

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
        // The sandbox's configured root is the session root; the `workdir`
        // override is honored by direct `LocalSessionEnv::new` callers.
        Ok(Arc::new(
            LocalSessionEnv::new(&self.root, self.limits).await?,
        ))
    }
}

/// Convenience constructor matching Flue's `local()` import.
#[must_use]
pub fn local() -> LocalSandbox {
    LocalSandbox::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

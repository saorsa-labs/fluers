//! The `SessionEnv` trait — the filesystem + process abstraction.
//!
//! This is the central abstraction that sandbox backends implement. Flue's
//! built-in tools (`read`, `write`, `bash`, …) operate purely against a
//! `SessionEnv`, so the same tools work unchanged over a virtual, local,
//! or remote-container sandbox.

use async_trait::async_trait;
use std::path::Path;
use tokio_util::sync::CancellationToken;

use crate::error::RuntimeResult;

/// The outcome of running a shell command.
#[derive(Debug, Clone)]
pub struct ShellResult {
    /// Exit code (124 conventionally denotes a timeout, as in Flue).
    pub exit_code: i32,
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
}

/// The environment a session runs in.
///
/// Every method is async and fallible so it can be backed by anything from a
/// real local directory to a remote container API (E2B, Daytona, …).
#[async_trait]
pub trait SessionEnv: Send + Sync {
    /// Read a file, bounded by `max_lines` / `max_bytes`.
    async fn read_file(
        &self,
        path: &Path,
        max_lines: usize,
        max_bytes: usize,
    ) -> RuntimeResult<String>;

    /// Read a file **in full**, erroring (NOT truncating) if it exceeds
    /// `max_bytes`.
    ///
    /// Use for tools that must operate on the complete file (e.g. `edit`, which
    /// writes the file back): the bounded [`read_file`](Self::read_file) silently
    /// truncates large files and would cause data loss on write-back. This method
    /// checks the file size (via metadata, before reading) and returns
    /// [`RuntimeError::FileTooLarge`] if the file is too big, so the caller never
    /// operates on partial data. Path containment is enforced as for `read_file`.
    async fn read_file_full(&self, path: &Path, max_bytes: usize) -> RuntimeResult<String>;

    /// Write a file, creating parent directories as needed.
    async fn write_file(&self, path: &Path, content: &str) -> RuntimeResult<()>;

    /// Run a shell command, with a `timeout_ms` hint and cancellation.
    ///
    /// Implementations should `select!` on `cancel.cancelled()` and, for
    /// child processes, send `SIGTERM` (then `SIGKILL` after a grace
    /// period) on cancel.
    async fn exec(
        &self,
        command: &str,
        cwd: &Path,
        timeout_ms: Option<u64>,
        cancel: &CancellationToken,
    ) -> RuntimeResult<ShellResult>;

    /// List files matching a glob (bounded by `limit`).
    async fn glob(&self, pattern: &str, limit: usize) -> RuntimeResult<Vec<String>>;

    /// Grep for `pattern`, bounded by `max_matches`.
    async fn grep(
        &self,
        pattern: &str,
        paths: &[&str],
        max_matches: usize,
    ) -> RuntimeResult<Vec<String>>;
}

/// Flue's resource caps, applied uniformly across sandbox backends.
///
/// Values mirror `packages/runtime/src/agent.ts` constants.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Max lines returned by `read`.
    pub max_read_lines: usize,
    /// Max bytes returned by `read`.
    pub max_read_bytes: usize,
    /// Max grep matches.
    pub max_grep_matches: usize,
    /// Max glob results.
    pub max_glob_results: usize,
    /// Max line length before truncation.
    pub max_grep_line_length: usize,
    /// Max file size (bytes) for a non-truncating `edit` read. Files larger
    /// than this are rejected with [`RuntimeError::FileTooLarge`] rather than
    /// edited (which would risk data loss from a truncated read).
    pub max_edit_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        // Mirror Flue's constants exactly so behavior matches.
        Self {
            max_read_lines: 2000,
            max_read_bytes: 50 * 1024,
            max_grep_matches: 100,
            max_glob_results: 1000,
            max_grep_line_length: 500,
            max_edit_bytes: 256 * 1024,
        }
    }
}

/// Read an entire file ignoring the caps (used by internal helpers).
pub async fn read_all(env: &dyn SessionEnv, path: &Path) -> RuntimeResult<String> {
    env.read_file(path, usize::MAX, usize::MAX).await
}

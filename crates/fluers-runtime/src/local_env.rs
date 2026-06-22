//! The real local-filesystem `SessionEnv`.
//!
//! Tools run against a real directory on disk via `tokio::fs` +
//! `tokio::process`. **Path containment is enforced**: model-facing paths are
//! relative, `..` is rejected, and resolved paths must stay under the
//! canonicalized root.
//!
//! See `SECURITY.md`: this is *not* an OS-level sandbox (no chroot/landlock/
//! UID separation). It prevents accidental path escape; it is not a defense
//! against a determined adversary until OS isolation lands.

use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::env::{Limits, SessionEnv, ShellResult};
use crate::error::{RuntimeError, RuntimeResult};

/// A `SessionEnv` backed by a real local directory.
pub struct LocalSessionEnv {
    /// Canonicalized root all relative paths are joined under.
    root: PathBuf,
    #[allow(dead_code)]
    limits: Limits,
}

impl LocalSessionEnv {
    /// Create an env rooted at `root`. The directory is canonicalized; if it
    /// does not exist it is created.
    pub async fn new(root: impl Into<PathBuf>, limits: Limits) -> RuntimeResult<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(RuntimeError::Io)?;
        let canon = tokio::fs::canonicalize(&root)
            .await
            .map_err(RuntimeError::Io)?;
        Ok(Self {
            root: canon,
            limits,
        })
    }

    /// Resolve a model-supplied relative path under the root, rejecting
    /// escapes. Returns the absolute joined path on success.
    fn resolve(&self, rel: &Path) -> RuntimeResult<PathBuf> {
        if rel.is_absolute() {
            return Err(RuntimeError::Sandbox(format!(
                "absolute paths are not allowed: `{}`",
                rel.display()
            )));
        }
        if rel.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(RuntimeError::Sandbox(format!(
                "`..` is not allowed in paths: `{}`",
                rel.display()
            )));
        }
        let joined = self.root.join(rel);
        // Find the deepest existing ancestor (handles not-yet-created write
        // targets and `.`), canonicalize it, and verify containment.
        let mut anchor = joined.clone();
        while !anchor.exists() && anchor.parent().is_some() {
            anchor = match anchor.parent() {
                Some(p) if p.starts_with(&self.root) => p.to_path_buf(),
                _ => break,
            };
        }
        let canon = anchor.canonicalize().map_err(RuntimeError::Io)?;
        if !canon.starts_with(&self.root) {
            return Err(RuntimeError::Sandbox(format!(
                "path escapes sandbox root: `{}`",
                rel.display()
            )));
        }
        // Return the canonical path if the target exists; else the joined path.
        match joined.canonicalize() {
            Ok(c) if c.starts_with(&self.root) => Ok(c),
            Ok(_) => Err(RuntimeError::Sandbox(format!(
                "path escapes sandbox root: `{}`",
                rel.display()
            ))),
            Err(_) => Ok(joined),
        }
    }
}

#[async_trait]
impl SessionEnv for LocalSessionEnv {
    async fn read_file(
        &self,
        path: &Path,
        max_lines: usize,
        max_bytes: usize,
    ) -> RuntimeResult<String> {
        let resolved = self.resolve(path)?;
        let raw = tokio::fs::read_to_string(&resolved)
            .await
            .map_err(RuntimeError::Io)?;
        Ok(apply_read_limits(raw, max_lines, max_bytes))
    }

    async fn write_file(&self, path: &Path, content: &str) -> RuntimeResult<()> {
        let resolved = self.resolve(path)?;
        if let Some(parent) = resolved.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(RuntimeError::Io)?;
        }
        tokio::fs::write(&resolved, content)
            .await
            .map_err(RuntimeError::Io)?;
        Ok(())
    }

    async fn exec(
        &self,
        command: &str,
        cwd: &Path,
        timeout_ms: Option<u64>,
        cancel: &CancellationToken,
    ) -> RuntimeResult<ShellResult> {
        // The cwd is also relative to the root.
        let cwd_resolved = self.resolve(cwd)?;

        let mut child = Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(&cwd_resolved)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(RuntimeError::Io)?;

        let timeout_ms_value = timeout_ms;
        let timeout_fut = match timeout_ms {
            Some(ms) => Box::pin(tokio::time::sleep(std::time::Duration::from_millis(ms)))
                as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
            None => Box::pin(std::future::pending()),
        };
        let cancel_fut = cancel.cancelled();

        tokio::select! {
            _ = timeout_fut => {
                // Timeout: try to kill, then return the 124-shaped result.
                let _ = child.kill().await;
                return Ok(ShellResult {
                    exit_code: 124,
                    stdout: String::new(),
                    stderr: format!("command timed out after {}ms", timeout_ms_value.unwrap_or(0)),
                });
            }
            _ = cancel_fut => {
                let _ = child.kill().await;
                return Err(RuntimeError::Sandbox("command cancelled".into()));
            }
            status = child.wait() => {
                let status = status.map_err(RuntimeError::Io)?;
                let output = child.wait_with_output().await.map_err(RuntimeError::Io)?;
                Ok(ShellResult {
                    exit_code: status.code().unwrap_or(-1),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                })
            }
        }
    }

    async fn glob(&self, pattern: &str, limit: usize) -> RuntimeResult<Vec<String>> {
        // Glob relative to the root.
        let full = self.root.join(pattern);
        let matched: Vec<PathBuf> = glob_match(&full, limit);
        // Strip the root prefix so results are relative.
        let stripped: Vec<String> = matched
            .iter()
            .filter_map(|p| p.strip_prefix(&self.root).ok())
            .map(|p| p.display().to_string())
            .collect();
        Ok(stripped)
    }

    async fn grep(
        &self,
        pattern: &str,
        paths: &[&str],
        max_matches: usize,
    ) -> RuntimeResult<Vec<String>> {
        // Shell out to `rg` if present, else `grep -rn`. Search under root.
        let rg = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "rg -n -- {pat} {search} 2>/dev/null || grep -rn -- {pat} {search} 2>/dev/null",
                pat = shell_quote(pattern),
                search = if paths.is_empty() {
                    ".".to_string()
                } else {
                    paths
                        .iter()
                        .map(|p| shell_quote(p))
                        .collect::<Vec<_>>()
                        .join(" ")
                }
            ))
            .current_dir(&self.root)
            .output()
            .map_err(RuntimeError::Io)?;
        let out = String::from_utf8_lossy(&rg.stdout);
        Ok(out.lines().take(max_matches).map(String::from).collect())
    }
}

/// Truncate `raw` to `max_lines` and `max_bytes`, whichever binds first.
fn apply_read_limits(raw: String, max_lines: usize, max_bytes: usize) -> String {
    let mut bytes_left = max_bytes;
    let mut out = String::new();
    let mut truncated = false;
    for (i, line) in raw.split_inclusive('\n').enumerate() {
        if i >= max_lines {
            out.push_str(&format!("\n[... truncated at {max_lines} lines ...]"));
            truncated = true;
            break;
        }
        if bytes_left < line.len() {
            // Take as many whole bytes as fit on a UTF-8 boundary.
            let take = line
                .char_indices()
                .map(|(i, _)| i)
                .find(|&pos| pos > bytes_left)
                .unwrap_or(line.len());
            out.push_str(line.get(..take).unwrap_or(line));
            out.push_str(&format!("\n[... truncated at {max_bytes} bytes ...]"));
            truncated = true;
            break;
        }
        out.push_str(line);
        bytes_left -= line.len();
    }
    if truncated {
        out
    } else {
        raw
    }
}

/// Minimal recursive glob matcher supporting `*`, `**`, and `?`.
fn glob_match(pattern: &Path, limit: usize) -> Vec<PathBuf> {
    let mut results = Vec::new();
    let base = pattern
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let pat = pattern.file_name().and_then(|s| s.to_str()).unwrap_or("*");
    walk_glob(&base, pat, &mut results, limit);
    results.sort();
    results
}

fn walk_glob(dir: &Path, pat: &str, out: &mut Vec<PathBuf>, limit: usize) {
    if out.len() >= limit {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if out.len() >= limit {
            return;
        }
        let path = entry.path();
        if matches_glob(entry.file_name().to_string_lossy().as_ref(), pat) {
            out.push(path.clone());
        }
        if path.is_dir() {
            walk_glob(&path, pat, out, limit);
        }
    }
}

/// Single-segment glob (`*`/`?`) matcher. `**` is treated as `*` here.
fn matches_glob(name: &str, pat: &str) -> bool {
    let name_b = name.as_bytes();
    let pat_b = pat.as_bytes();
    matches_at(name_b, pat_b, 0, 0)
}

fn matches_at(n: &[u8], p: &[u8], mut ni: usize, mut pi: usize) -> bool {
    let mut star: Option<(usize, usize)> = None;
    while ni < n.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == b'*') {
            if p[pi] == b'*' {
                star = Some((pi, ni));
                pi += 1;
                continue;
            }
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == n[ni] {
            pi += 1;
            ni += 1;
        } else if let Some((sp, sn)) = star {
            pi = sp + 1;
            ni = sn + 1;
            star = Some((sp, sn + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

/// Quote a string for safe inclusion in a `sh -c` command.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    //! Local sandbox path-containment and tool tests against a temp dir.

    use super::*;

    #[tokio::test]
    async fn read_file_within_root_works() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("hello.txt"), "hi there\n")
            .await
            .unwrap();
        let got = env
            .read_file(Path::new("hello.txt"), 100, 1024)
            .await
            .unwrap();
        assert_eq!(got, "hi there\n");
    }

    #[tokio::test]
    async fn read_file_rejects_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.read_file(Path::new("/etc/passwd"), 100, 1024).await;
        assert!(res.is_err(), "absolute paths must be rejected");
    }

    #[tokio::test]
    async fn read_file_rejects_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.read_file(Path::new("../escape.txt"), 100, 1024).await;
        assert!(res.is_err(), "`..` must be rejected");
    }

    #[tokio::test]
    async fn write_then_read_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        env.write_file(Path::new("sub/nested/file.txt"), "deep content")
            .await
            .unwrap();
        let got = env
            .read_file(Path::new("sub/nested/file.txt"), 100, 1024)
            .await
            .unwrap();
        assert_eq!(got, "deep content");
    }

    #[tokio::test]
    async fn exec_runs_shell_command() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env
            .exec(
                "echo hello",
                Path::new("."),
                None,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.exit_code, 0);
        assert_eq!(res.stdout.trim(), "hello");
    }

    #[tokio::test]
    async fn exec_timeout_returns_124() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env
            .exec(
                "sleep 5",
                Path::new("."),
                Some(200),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.exit_code, 124, "timeout must yield exit 124");
    }

    #[test]
    fn glob_matcher_basics() {
        assert!(matches_glob("foo.txt", "*.txt"));
        assert!(matches_glob("foo.txt", "foo.*"));
        assert!(!matches_glob("foo.txt", "*.md"));
        assert!(matches_glob("a", "?"));
    }

    #[test]
    fn read_limit_truncates() {
        let got = apply_read_limits("a\nb\nc\nd\n".into(), 2, 1024);
        assert!(got.contains("a"));
        assert!(got.contains("b"));
        assert!(got.contains("truncated"));
    }
}

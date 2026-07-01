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

use std::os::fd::{AsFd, OwnedFd};
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use rustix::fs::{fstat, open, openat, Mode, OFlags};
use rustix::io::Errno;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::env::{Limits, SessionEnv, ShellResult};
use crate::error::{RuntimeError, RuntimeResult};

/// POSIX `st_mode` masks (stable, platform-independent) for the regular-file
/// check — avoids pulling `libc` just for `S_ISREG`.
const ST_MODE_TYPE_MASK: u32 = 0o170_000; // S_IFMT
const ST_MODE_REGULAR: u32 = 0o100_000; // S_IFREG

/// A `SessionEnv` backed by a real local directory.
pub struct LocalSessionEnv {
    /// Canonicalized root all relative paths are joined under.
    root: PathBuf,
    /// Held fd over the canonical root (B-Swift Phase C1a / #4): the anchor for
    /// `openat`-walked reads. Opened once at construction with
    /// `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`, so root-path re-resolution never
    /// re-enters the read hot path. `OwnedFd` is `Send + Sync` on Unix.
    root_fd: OwnedFd,
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
        // Hold an fd over the canonical root (B-Swift Phase C1a / #4). Opened
        // with O_NOFOLLOW (reject a root swapped to a symlink since construction)
        // + O_DIRECTORY + O_CLOEXEC.
        let root_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let root_fd = match open(&canon, root_flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
        };
        Ok(Self {
            root: canon,
            root_fd,
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

    /// Open `rel` for reading via an fd-anchored walk from the held root fd
    /// (B-Swift Phase C1a / #4). Closes the path-based TOCTOU at the daemon
    /// read: every component is opened with `O_NOFOLLOW` (symlink → `ELOOP`),
    /// and the leaf is `fstat`'d on the SAME fd we hand back for reading — so a
    /// symlink/hardlink swap between confinement and the read cannot exfiltrate.
    /// Mirrors the Swift `readFdAnchored`.
    ///
    /// Returns the opened regular-file `File` and its size in bytes (the size is
    /// authoritative — taken off the open fd, not the path).
    fn open_anchored_read(&self, rel: &Path) -> RuntimeResult<(std::fs::File, u64)> {
        // Input shape checks (the fd walk itself enforces containment — there is
        // no canonicalize-then-contain step, so no path re-resolution).
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

        let oflag = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        // Walk: hold every opened fd in `chain` so intermediates stay alive
        // until the next level is opened; the last element is the leaf.
        let mut chain: Vec<OwnedFd> = Vec::new();
        for comp in rel.components() {
            if let Component::Normal(name) = comp {
                let dir = match chain.last() {
                    Some(f) => f.as_fd(),
                    None => self.root_fd.as_fd(),
                };
                let fd = match openat(dir, name, oflag, Mode::empty()) {
                    Ok(fd) => fd,
                    Err(Errno::LOOP) => {
                        return Err(RuntimeError::Sandbox(format!(
                            "symlinks are not allowed in read paths: `{}`",
                            rel.display()
                        )));
                    }
                    Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
                };
                chain.push(fd);
            }
            // `Component::CurDir` (".") is skipped; `ParentDir`/absolute are
            // pre-rejected above.
        }
        let leaf_owned = chain
            .pop()
            .ok_or_else(|| RuntimeError::Sandbox("read path has no components".to_string()))?;
        // Remaining `chain` (intermediates) drops here → their fds close.

        // Authoritative leaf check: fstat the OPENED fd (not the path).
        let stat = match fstat(leaf_owned.as_fd()) {
            Ok(s) => s,
            Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
        };
        if (stat.st_mode as u32 & ST_MODE_TYPE_MASK) != ST_MODE_REGULAR {
            return Err(RuntimeError::Sandbox(format!(
                "not a regular file: `{}`",
                rel.display()
            )));
        }
        if stat.st_nlink > 1 {
            // Hardlink exfil (`ln secret in_root; read in_root/link`) — mirrors
            // the Swift-side C2/#3 reject. Authoritative here: fstat off the
            // open fd, not the path.
            return Err(RuntimeError::Sandbox(format!(
                "multiple hard links — can't safely confine: `{}`",
                rel.display()
            )));
        }
        let size = stat.st_size.max(0) as u64;
        Ok((std::fs::File::from(leaf_owned), size))
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
        // B-Swift Phase C1a / #4: fd-anchored open + read from the SAME fd
        // (closes the check-then-use TOCTOU the path-based read had).
        let (file, _size) = self.open_anchored_read(path)?;
        let mut file = tokio::fs::File::from_std(file);
        let mut raw = String::new();
        file.read_to_string(&mut raw)
            .await
            .map_err(RuntimeError::Io)?;
        Ok(apply_read_limits(raw, max_lines, max_bytes))
    }

    async fn read_file_full(&self, path: &Path, max_bytes: usize) -> RuntimeResult<String> {
        // B-Swift Phase C1a / #4: size + read off the SAME open fd. The old
        // path-based metadata check raced the read; now the size gate is
        // authoritative (fstat off the open fd) and the read uses that fd.
        let (file, size) = self.open_anchored_read(path)?;
        let size = size as usize;
        if size > max_bytes {
            return Err(RuntimeError::FileTooLarge {
                path: path.display().to_string(),
                size,
                max: max_bytes,
            });
        }
        let mut file = tokio::fs::File::from_std(file);
        let mut raw = String::new();
        file.read_to_string(&mut raw)
            .await
            .map_err(RuntimeError::Io)?;
        Ok(raw)
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
        // Containment: reject absolute patterns and `..` so the model can't
        // list files outside the root (e.g. `../../*` or `/etc/*`).
        validate_search_pattern(pattern)?;
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
        // Containment: validate each search path. Reject absolute/`..` so the
        // model can't search outside the root (e.g. `../.env` or `/etc/passwd`).
        let mut validated: Vec<String> = Vec::new();
        if paths.is_empty() {
            validated.push(".".to_string());
        } else {
            for p in paths {
                validate_search_pattern(p)?;
                validated.push(shell_quote(p));
            }
        }
        let search = validated.join(" ");
        // Shell out to `rg` if present, else `grep -rn`. Search under root.
        let rg = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "rg -n -- {pat} {search} 2>/dev/null || grep -rn -- {pat} {search} 2>/dev/null",
                pat = shell_quote(pattern),
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

/// Validate a glob/grep search pattern/path is contained: reject absolute
/// paths and `..` components so the model can't reach outside the root.
///
/// Patterns may legitimately contain `*`/`?` (glob) — only path-structure
/// escapes are rejected.
fn validate_search_pattern(input: &str) -> RuntimeResult<()> {
    // Reject absolute paths.
    if input.starts_with('/') || input.starts_with('\\') {
        return Err(RuntimeError::Sandbox(format!(
            "absolute paths are not allowed: `{input}`"
        )));
    }
    // Reject any `..` path component. Walk segments, ignoring glob wildcards.
    for seg in input.split('/') {
        if seg == ".." {
            return Err(RuntimeError::Sandbox(format!(
                "`..` is not allowed in search paths: `{input}`"
            )));
        }
    }
    Ok(())
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
    async fn read_file_full_returns_complete_content_without_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        // 10 lines of 60 bytes each = 600 bytes, well under the default cap,
        // but above the *truncating* read's line/byte interplay. Ensure the
        // full-read path returns the whole file verbatim, with no marker.
        let body = (0..10)
            .map(|i| format!("line number {i:02} with some padding text\n"))
            .collect::<String>();
        tokio::fs::write(dir.path().join("big.txt"), &body)
            .await
            .unwrap();
        let got = env
            .read_file_full(Path::new("big.txt"), 1024)
            .await
            .unwrap();
        assert_eq!(got, body);
        assert!(!got.contains("[... truncated"));
    }

    #[tokio::test]
    async fn read_file_full_rejects_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.read_file_full(Path::new("/etc/passwd"), 1024).await;
        assert!(res.is_err(), "absolute paths must be rejected");
    }

    #[tokio::test]
    async fn read_file_full_rejects_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.read_file_full(Path::new("../escape.txt"), 1024).await;
        assert!(res.is_err(), "`..` must be rejected");
    }

    #[tokio::test]
    async fn read_file_full_errors_when_too_large_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        // 100 bytes, cap at 50 -> must ERROR (FileTooLarge), never return a
        // truncated prefix (the whole point vs `read_file`).
        tokio::fs::write(dir.path().join("over.txt"), &"a".repeat(100))
            .await
            .unwrap();
        let res = env.read_file_full(Path::new("over.txt"), 50).await;
        assert!(res.is_err(), "oversized file must error, not truncate");
        match res {
            Err(RuntimeError::FileTooLarge { size, max, .. }) => {
                assert_eq!(size, 100);
                assert_eq!(max, 50);
            }
            other => panic!("expected FileTooLarge, got {other:?}"),
        }
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

    #[tokio::test]
    async fn glob_rejects_absolute_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.glob("/etc/*", 10).await;
        assert!(res.is_err(), "absolute glob patterns must be rejected");
    }

    #[tokio::test]
    async fn glob_rejects_parent_dir_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.glob("../**/*", 10).await;
        assert!(res.is_err(), "`..` in glob patterns must be rejected");
    }

    #[tokio::test]
    async fn grep_rejects_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.grep("foo", &["/etc/passwd"], 10).await;
        assert!(res.is_err(), "absolute grep paths must be rejected");
    }

    #[tokio::test]
    async fn grep_rejects_parent_dir_path() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env.grep("foo", &["../.env"], 10).await;
        assert!(res.is_err(), "`..` grep paths must be rejected");
    }

    // ── B-Swift Phase C1a / #4: fd-anchored read TOCTOU / hardlink coverage ──
    // These prove the fix: the OLD path-based `read_to_string(resolved)` followed
    // symlinks (leaking the target) and ignored `st_nlink`, so each of these
    // would have SUCCEEDED (exfiltrated the secret) before the fix.

    /// Write a secret to a file OUTSIDE the env root (a sibling temp dir) and
    /// return both the held `TempDir` (keep alive for the test) and its path.
    #[cfg(unix)]
    fn outside_secret(body: &str) -> (tempfile::TempDir, PathBuf) {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        (dir, path)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_rejects_symlink_leaf_even_when_target_inside_root() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("inside.txt"), "ok\n")
            .await
            .unwrap();
        symlink("inside.txt", dir.path().join("link.txt")).unwrap();
        let res = env.read_file(Path::new("link.txt"), 100, 1024).await;
        assert!(
            res.is_err(),
            "a symlink leaf must be rejected even if its target is inside the root"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_rejects_symlink_leaf_to_outside_root() {
        // Exfil via symlink: link.txt -> /outside/secret. The OLD read followed
        // it and leaked "TOPSECRET"; the anchored `openat(O_NOFOLLOW)` rejects
        // the symlink leaf outright.
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let (_outside, secret) = outside_secret("TOPSECRET");
        symlink(&secret, dir.path().join("link.txt")).unwrap();
        let res = env.read_file(Path::new("link.txt"), 100, 1024).await;
        assert!(
            res.is_err(),
            "a symlink to outside the root must be rejected"
        );
        if let Ok(s) = res {
            assert!(!s.contains("TOPSECRET"), "the secret must not leak");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_rejects_intermediate_symlink_dir() {
        // Exfil via a symlinked intermediate dir: linkdir -> realdir; reading
        // `linkdir/file.txt` must reject at the `linkdir` component (per-component
        // `openat(O_NOFOLLOW)`).
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("realdir"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("realdir/file.txt"), "ok\n")
            .await
            .unwrap();
        symlink("realdir", dir.path().join("linkdir")).unwrap();
        let res = env
            .read_file(Path::new("linkdir/file.txt"), 100, 1024)
            .await;
        assert!(
            res.is_err(),
            "a symlinked intermediate dir must be rejected"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_rejects_hardlink_to_outside_secret() {
        // Hardlink exfil: `ln /outside/secret root/link.txt`. The file is regular
        // and inside the root, but `st_nlink > 1` → reject (mirrors the Swift
        // C2/#3 decision; authoritative here via post-open `fstat`).
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let (_outside, secret) = outside_secret("TOPSECRET");
        std::fs::hard_link(&secret, dir.path().join("link.txt")).unwrap();
        let res = env.read_file(Path::new("link.txt"), 100, 1024).await;
        assert!(res.is_err(), "a hardlink (st_nlink > 1) must be rejected");
        if let Ok(s) = res {
            assert!(!s.contains("TOPSECRET"), "the secret must not leak");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_full_rejects_symlink_leaf() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let (_outside, secret) = outside_secret("TOPSECRET");
        symlink(&secret, dir.path().join("link.txt")).unwrap();
        let res = env.read_file_full(Path::new("link.txt"), 1024).await;
        assert!(res.is_err(), "read_file_full must reject a symlink leaf");
        if let Ok(s) = res {
            assert!(!s.contains("TOPSECRET"));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_file_full_rejects_hardlink() {
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let (_outside, secret) = outside_secret("TOPSECRET");
        std::fs::hard_link(&secret, dir.path().join("link.txt")).unwrap();
        let res = env.read_file_full(Path::new("link.txt"), 1024).await;
        assert!(
            res.is_err(),
            "read_file_full must reject a hardlink (st_nlink > 1)"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_anchored_nested_relative_path_still_works() {
        // Regression guard: the anchored walk must still read a real nested
        // file (intermediate dirs are opened `O_NOFOLLOW` + read off the leaf fd).
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("a/b"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("a/b/c.txt"), "deep\n")
            .await
            .unwrap();
        let got = env
            .read_file(Path::new("a/b/c.txt"), 100, 1024)
            .await
            .unwrap();
        assert_eq!(got, "deep\n");
    }
}

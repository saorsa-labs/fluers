//! The real local-filesystem `SessionEnv`.
//!
//! Tools run against a real directory on disk via `tokio::fs` +
//! `tokio::process`. **Confinement is fd-anchored**: every read, write, search,
//! and exec cwd is resolved off a single held root fd via `openat`
//! per-component walks with `O_NOFOLLOW` + an authoritative `fstat` on the
//! opened leaf fd. There is no canonicalize-then-contain step in any data path,
//! so a symlink/hardlink swapped between the containment check and the operation
//! cannot redirect a read (exfil) or a write/exec (data loss).
//!
//! See `SECURITY.md`: this is *not* an OS-level sandbox (no chroot/landlock/
//! UID separation). The fd-anchoring closes the TOCTOU class the path-based
//! `resolve()` had; it does not turn this into a security boundary against a
//! determined adversary until OS isolation lands.

use std::ffi::OsStr;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use rustix::fs::{fstat, ftruncate, mkdirat, open, openat, Dir, FileType, Mode, OFlags};
use rustix::io::Errno;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

// `fcntl(F_GETPATH)` is apple-only; it backs `fd_real_path` on macOS.
#[cfg(target_os = "macos")]
use rustix::fs::getpath;
// `/proc/self/fd/N` readlink needs the raw fd int on Linux.
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

use crate::env::{Limits, SessionEnv, ShellResult};
use crate::error::{RuntimeError, RuntimeResult};

/// POSIX `st_mode` masks (stable, platform-independent) for the regular-file
/// check — avoids pulling `libc` just for `S_ISREG`.
const ST_MODE_TYPE_MASK: u32 = 0o170_000; // S_IFMT
const ST_MODE_REGULAR: u32 = 0o100_000; // S_IFREG

/// A `SessionEnv` backed by a real local directory.
pub struct LocalSessionEnv {
    /// Held fd over the canonical root: the anchor for every fd-anchored walk.
    /// Opened once at construction with `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`,
    /// so root-path re-resolution never re-enters any data hot path. Because
    /// the root is pinned by fd (not path), renaming/symlinking the root *path*
    /// after construction cannot redirect a subsequent operation. `OwnedFd` is
    /// `Send + Sync` on Unix.
    root_fd: OwnedFd,
    #[allow(dead_code)]
    limits: Limits,
}

impl LocalSessionEnv {
    /// Create an env rooted at `root`. The directory is canonicalized; if it
    /// does not exist it is created. An fd is held over the canonical root for
    /// the lifetime of the env.
    pub async fn new(root: impl Into<PathBuf>, limits: Limits) -> RuntimeResult<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(RuntimeError::Io)?;
        let canon = tokio::fs::canonicalize(&root)
            .await
            .map_err(RuntimeError::Io)?;
        // Hold an fd over the canonical root. Opened with O_NOFOLLOW (reject a
        // root swapped to a symlink since construction) + O_DIRECTORY +
        // O_CLOEXEC. From here on, no operation re-resolves the root *path* —
        // they all anchor off this fd.
        let root_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let root_fd = open(&canon, root_flags, Mode::empty())
            .map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
        Ok(Self { root_fd, limits })
    }

    /// Validate a model-supplied relative path and return its `Normal`
    /// components (skipping `.`). Rejects absolute paths and any `..`
    /// component up front — the fd walk itself then enforces containment, so
    /// there is no canonicalize-then-contain step anywhere in the data path.
    fn normal_components<'a>(&self, rel: &'a Path) -> RuntimeResult<Vec<&'a OsStr>> {
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
        Ok(rel
            .components()
            .filter_map(|c| match c {
                Component::Normal(name) => Some(name),
                // `CurDir` (".") is skipped; `ParentDir`/absolute are
                // pre-rejected above.
                _ => None,
            })
            .collect())
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
        let names = self.normal_components(rel)?;
        if names.is_empty() {
            return Err(RuntimeError::Sandbox(format!(
                "read path has no components: `{}`",
                rel.display()
            )));
        }

        let oflag = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        // Walk: hold every opened fd in `chain` so intermediates stay alive
        // until the next level is opened; the last element is the leaf.
        let mut chain: Vec<OwnedFd> = Vec::new();
        for name in names {
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
        let leaf_owned = chain
            .pop()
            .ok_or_else(|| RuntimeError::Sandbox("read path has no components".to_string()))?;
        // Remaining `chain` (intermediates) drops here → their fds close.

        // Authoritative leaf check: fstat the OPENED fd (not the path).
        let stat =
            fstat(leaf_owned.as_fd()).map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
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

    /// Open an existing directory `rel` via an fd-anchored walk from the held
    /// root fd (B-Swift Phase C1b). Used to pin an exec `cwd` by fd (passed to
    /// the child as `/dev/fd/N`). Every component is opened with
    /// `O_DIRECTORY | O_NOFOLLOW`, so a symlinked intermediate dir → `ELOOP`
    /// → reject (never followed).
    fn open_anchored_dir(&self, rel: &Path) -> RuntimeResult<OwnedFd> {
        let names = self.normal_components(rel)?;
        let oflag = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        // Open "." relative to the held root → an independent owned starting fd,
        // so we never borrow `root_fd` across the walk.
        let mut cur = openat(self.root_fd.as_fd(), ".", oflag, Mode::empty())
            .map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
        for name in names {
            let next = match openat(cur.as_fd(), name, oflag, Mode::empty()) {
                Ok(fd) => fd,
                Err(Errno::LOOP) => {
                    return Err(RuntimeError::Sandbox(format!(
                        "symlinked directories are not allowed: `{}`",
                        rel.display()
                    )));
                }
                Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
            };
            cur = next;
        }
        Ok(cur)
    }

    /// Derive the real on-disk path of an already-open directory fd — macOS
    /// `fcntl(F_GETPATH)`, Linux `/proc/self/fd/N`. The path comes from the
    /// *inode* the fd names, NOT from any model-supplied input string, so a
    /// symlink swap on the input path between the fd-anchored open and the
    /// spawn/search can't redirect the operation. (`/dev/fd/N` as a `cwd` is
    /// Linux-only — macOS fdescfs rejects `chdir` to it with `ENOTDIR`, so the
    /// inode path is the portable fd-anchored handle.) A post-open *move* of the
    /// directory is a residual race outside the threat model: this is not an OS
    /// sandbox, and moving the dir requires write access under the confined root.
    fn fd_real_path(fd: BorrowedFd<'_>) -> RuntimeResult<PathBuf> {
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::ffi::OsStrExt;
            let c = getpath(fd).map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
            Ok(PathBuf::from(OsStr::from_bytes(c.to_bytes())))
        }
        #[cfg(target_os = "linux")]
        {
            let raw = fd.as_raw_fd();
            std::fs::read_link(format!("/proc/self/fd/{raw}")).map_err(RuntimeError::Io)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = fd;
            Err(RuntimeError::Sandbox(
                "fd-derived directory path is unsupported on this platform".into(),
            ))
        }
    }

    /// Resolve a grep search path to its real INODE path, fd-anchored from the
    /// held root fd. Every component is opened `O_NOFOLLOW`; a symlink anywhere
    /// in the path (including a symlinked dir passed explicitly) is rejected
    /// outright — `rg --no-follow` would otherwise follow an explicit
    /// symlinked-dir argument and leak its contents. The returned path is the
    /// inode's path (from `fd_real_path`), so a swap on the input can't redirect
    /// the search. Handles directory and file leaf targets; `.`/empty → root.
    fn search_path_inode(&self, p: &str) -> RuntimeResult<PathBuf> {
        let names = self.normal_components(Path::new(p))?;
        if names.is_empty() {
            // `.` or empty path → the root.
            return Self::fd_real_path(self.root_fd.as_fd());
        }
        let dir_oflag = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let file_oflag = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let (parents, last) = names.split_at(names.len() - 1);
        let mut parent = openat(self.root_fd.as_fd(), ".", dir_oflag, Mode::empty())
            .map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
        for name in parents.iter().copied() {
            parent = match openat(parent.as_fd(), name, dir_oflag, Mode::empty()) {
                Ok(fd) => fd,
                Err(Errno::LOOP) => {
                    return Err(RuntimeError::Sandbox(format!(
                        "symlinked search path is not allowed: `{p}`"
                    )))
                }
                Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
            };
        }
        let last_name = last[0];
        // Leaf: try dir, fall back to file (a file grep target). `O_NOFOLLOW`
        // in both means a symlink leaf → `ELOOP` → reject.
        let leaf_fd = match openat(parent.as_fd(), last_name, dir_oflag, Mode::empty()) {
            Ok(fd) => fd,
            Err(Errno::NOTDIR) => {
                match openat(parent.as_fd(), last_name, file_oflag, Mode::empty()) {
                    Ok(fd) => fd,
                    Err(Errno::LOOP) => {
                        return Err(RuntimeError::Sandbox(format!(
                            "symlinked search path is not allowed: `{p}`"
                        )))
                    }
                    Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
                }
            }
            Err(Errno::LOOP) => {
                return Err(RuntimeError::Sandbox(format!(
                    "symlinked search path is not allowed: `{p}`"
                )))
            }
            Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
        };
        Self::fd_real_path(leaf_fd.as_fd())
    }

    /// Open `rel` for writing via an fd-anchored walk from the held root fd
    /// (B-Swift Phase C1b — the critical counterpart of `open_anchored_read`).
    ///
    /// Invariants:
    /// - Parent dirs are created with a `mkdirat` walk from the root fd (each
    ///   level opened `O_NOFOLLOW`); `mkdirat` does not follow a symlink at the
    ///   target name, and the follow-up `openat(O_DIRECTORY|O_NOFOLLOW)` rejects
    ///   a symlinked intermediate outright.
    /// - The leaf is opened `WRONLY | CREATE | NOFOLLOW` — `O_NOFOLLOW` rejects
    ///   a symlink leaf outright (`ELOOP`). Critically, `O_TRUNC` is **not**
    ///   passed: truncation is deferred to `ftruncate` *after* the hardlink
    ///   check, so a write through a hardlink can never mutate before the
    ///   confinement decision.
    /// - The opened leaf fd is `fstat`'d (authoritative): non-regular files are
    ///   rejected, and `st_nlink > 1` is rejected — a write through a hardlink
    ///   mutates every name in the set (silent cross-target data loss).
    /// - The caller truncates + writes off the SAME fd.
    fn open_anchored_write(&self, rel: &Path) -> RuntimeResult<OwnedFd> {
        let names = self.normal_components(rel)?;
        let (parents, leaf) = names.split_at(names.len().saturating_sub(1));
        let leaf_name = leaf.first().copied().ok_or_else(|| {
            RuntimeError::Sandbox(format!("write path has no file name: `{}`", rel.display()))
        })?;

        let dir_oflag = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        // mkdirat default mode mirrors std's `create_dir` (0o777 & !umask);
        // files below use 0o666 & !umask (std's `fs::write` default).
        let dir_mode = Mode::RWXU | Mode::RWXG | Mode::RWXO;
        let file_mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP | Mode::ROTH | Mode::WOTH;

        let mut parent = openat(self.root_fd.as_fd(), ".", dir_oflag, Mode::empty())
            .map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
        for name in parents.iter().copied() {
            let next = match openat(parent.as_fd(), name, dir_oflag, Mode::empty()) {
                Ok(fd) => fd,
                Err(Errno::NOENT) => {
                    // Create the missing intermediate dir. `mkdirat` does NOT
                    // follow a symlink at `name` (it would fail EEXIST); the
                    // reopen below re-establishes the fd-anchored position.
                    // EEXIST from mkdirat means another writer created it
                    // concurrently — that's safe; just reopen it.
                    if let Err(e) = mkdirat(parent.as_fd(), name, dir_mode) {
                        if e != Errno::EXIST {
                            return Err(RuntimeError::Io(std::io::Error::from(e)));
                        }
                    }
                    match openat(parent.as_fd(), name, dir_oflag, Mode::empty()) {
                        Ok(fd) => fd,
                        Err(Errno::LOOP) => {
                            return Err(RuntimeError::Sandbox(format!(
                                "symlinked directories are not allowed: `{}`",
                                rel.display()
                            )));
                        }
                        Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
                    }
                }
                Err(Errno::LOOP) => {
                    return Err(RuntimeError::Sandbox(format!(
                        "symlinked directories are not allowed: `{}`",
                        rel.display()
                    )));
                }
                Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
            };
            parent = next;
        }

        // Leaf: CREATE + NOFOLLOW, but deliberately NO TRUNC — truncate after
        // the nlink check so a hardlink can't be mutated pre-decision.
        let leaf_oflag = OFlags::WRONLY | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC;
        let leaf_fd = match openat(parent.as_fd(), leaf_name, leaf_oflag, file_mode) {
            Ok(fd) => fd,
            Err(Errno::LOOP) => {
                return Err(RuntimeError::Sandbox(format!(
                    "symlink leaf is not allowed: `{}`",
                    rel.display()
                )));
            }
            Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
        };

        // Authoritative confinement checks off the OPEN fd (not the path).
        let stat = fstat(leaf_fd.as_fd()).map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
        if (stat.st_mode as u32 & ST_MODE_TYPE_MASK) != ST_MODE_REGULAR {
            return Err(RuntimeError::Sandbox(format!(
                "not a regular file: `{}`",
                rel.display()
            )));
        }
        if stat.st_nlink > 1 {
            // A write through a hardlink mutates every name in the set — reject,
            // mirroring the read-side decision.
            return Err(RuntimeError::Sandbox(format!(
                "multiple hard links — can't safely confine: `{}`",
                rel.display()
            )));
        }
        Ok(leaf_fd)
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
        //
        // Bounded read (0.5.2): the output is capped at `max_bytes` anyway
        // (apply_read_limits truncates beyond it), so reading the whole file
        // first would OOM on a multi-GB file. Read at most `max_bytes` and
        // trim any partial UTF-8 char at the cut. Memory is thus bounded by
        // `max_bytes`, independent of the on-disk size.
        let (file, _size) = self.open_anchored_read(path)?;
        let (raw, truncated_at_cap) = read_bounded_string(file, max_bytes).await?;
        let mut out = apply_read_limits(raw, max_lines, max_bytes);
        // If the bounded read cut the file short (file > max_bytes) and
        // apply_read_limits didn't itself add a truncation marker, surface that
        // the content was capped — preserves the original oversized-file
        // indicator that the unbounded read had.
        if truncated_at_cap && !out.contains("[... truncated") {
            out.push_str(&format!("\n[... truncated at {max_bytes} bytes ...]"));
        }
        Ok(out)
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
        // B-Swift Phase C1b: fd-anchored write. Open the leaf off the held root
        // fd (mkdirat-walking parents), fstat for hardlink confinement, THEN
        // truncate + write off the SAME fd. No path re-resolution in any step.
        let leaf_fd = self.open_anchored_write(path)?;
        // Truncate AFTER the nlink check (the open deliberately omitted O_TRUNC).
        ftruncate(&leaf_fd, 0).map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
        let mut file = tokio::fs::File::from_std(std::fs::File::from(leaf_fd));
        file.write_all(content.as_bytes())
            .await
            .map_err(RuntimeError::Io)?;
        // Flush before returning: a subsequent `fstat` (e.g. a size-gated
        // `read_file_full`) must observe the full new size. `write_all`'s await
        // dispatches the pwrite on the blocking pool, but tokio `File`'s close is
        // deferred on drop — without this barrier the size was intermittently
        // not yet visible to a following `fstat` under parallel load (a rare
        // flake that returned a stale/short size). `flush` completes the pending
        // async write without an `fsync` (no durability/perf cost vs `sync_all`).
        file.flush().await.map_err(RuntimeError::Io)?;
        Ok(())
    }

    async fn exec(
        &self,
        command: &str,
        cwd: &Path,
        timeout_ms: Option<u64>,
        cancel: &CancellationToken,
    ) -> RuntimeResult<ShellResult> {
        // The cwd is opened fd-anchored (`openat(O_DIRECTORY|O_NOFOLLOW)` per
        // component from the held root fd), so a symlinked cwd dir is rejected
        // outright. The child then chdirs to the *inode's* real path — derived
        // from the open fd via `fd_real_path`, not from the input string — so a
        // symlink swap on the cwd path between open and spawn can't redirect it.
        // (`/dev/fd/N` would be the pure-inode handle, but macOS fdescfs rejects
        // `chdir` to it; the inode path is the portable form.) `cwd_fd` is held
        // in scope through `spawn()` so the inode it names stays valid.
        let cwd_fd = self.open_anchored_dir(cwd)?;
        let cwd_path = Self::fd_real_path(cwd_fd.as_fd())?;

        // `kill_on_drop(true)`: on timeout/cancel the in-flight `wait_with_output`
        // future (which owns the child) is dropped, and its `Drop` sends SIGKILL —
        // so a still-running child is never orphaned.
        let child = Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(&cwd_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(RuntimeError::Io)?;
        // `cwd_fd` stays live until end of scope (spawn has run by now).

        let timeout_fut = match timeout_ms {
            Some(ms) => Box::pin(tokio::time::sleep(std::time::Duration::from_millis(ms)))
                as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
            None => Box::pin(std::future::pending()),
        };
        let cancel_fut = cancel.cancelled();

        // `wait_with_output` drains stdout AND stderr concurrently while it waits.
        // The old `child.wait()` did not read the pipes, so a child emitting more
        // than the OS pipe buffer (~64 KB) blocked on a full pipe while `wait()`
        // blocked on the child — a deadlock that only broke on timeout (output
        // lost, misreported as a 124), or hung forever with no timeout set.
        tokio::select! {
            _ = timeout_fut => {
                // `child` (moved into the dropped `wait_with_output` future) is
                // SIGKILLed via `kill_on_drop`. Return the 124-shaped result.
                Ok(ShellResult {
                    exit_code: 124,
                    stdout: String::new(),
                    stderr: format!("command timed out after {}ms", timeout_ms.unwrap_or(0)),
                })
            }
            _ = cancel_fut => {
                Err(RuntimeError::Sandbox("command cancelled".into()))
            }
            output = child.wait_with_output() => {
                let output = output.map_err(RuntimeError::Io)?;
                Ok(ShellResult {
                    exit_code: output.status.code().unwrap_or(-1),
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
        // Split into a base dir (must exist) + a single-segment filename pattern.
        // As in the original matcher, the filename pattern is applied at every
        // depth under the base (the descent is what changed: it is now
        // fd-anchored and never enters a symlinked directory).
        let pat_path = Path::new(pattern);
        let base_rel = pat_path.parent().unwrap_or_else(|| Path::new(""));
        let fname = pat_path.file_name().and_then(|s| s.to_str()).unwrap_or("*");
        // Results are reported relative to the ROOT, but the walk starts at the
        // base dir — so seed the descent with the base's own path relative to
        // root (e.g. `sub/*.txt` → base prefix `sub`, so `sub/nested.txt` is
        // reported, not `nested.txt`).
        let base_prefix = self
            .normal_components(base_rel)?
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/");
        // A missing/symlinked base yields no matches (preserves the original
        // "no results" behavior for non-existent bases after validation).
        let base_fd = match self.open_anchored_dir(base_rel) {
            Ok(fd) => fd,
            Err(_) => return Ok(Vec::new()),
        };
        let dir = match Dir::new(base_fd) {
            Ok(d) => d,
            Err(_) => return Ok(Vec::new()),
        };
        let mut results: Vec<String> = Vec::new();
        walk_glob_fd(dir, fname, &base_prefix, &mut results, limit)?;
        results.sort();
        // De-dup (a `**`/depth-recursion can surface the same relative path).
        results.dedup();
        Ok(results)
    }

    async fn grep(
        &self,
        pattern: &str,
        paths: &[&str],
        max_matches: usize,
    ) -> RuntimeResult<Vec<String>> {
        // Containment: validate each search path's SHAPE (reject absolute/`..`
        // so the model can't reach outside the root), then resolve it fd-anchored
        // to its real INODE path. This is essential: `rg --no-follow` still
        // follows a symlinked dir passed EXPLICITLY as a search path, so passing
        // the input string would leak through `linkdir -> outside`. Resolving to
        // the inode path (and rejecting symlinks outright at `openat(NO_FOLLOW)`)
        // closes that — the search runs against the real confined dir/file.
        let root_path = Self::fd_real_path(self.root_fd.as_fd())?;
        let mut validated: Vec<String> = Vec::new();
        if paths.is_empty() {
            validated.push(shell_quote(&root_path.to_string_lossy()));
        } else {
            for p in paths {
                validate_search_pattern(p)?;
                let inode = self.search_path_inode(p)?;
                validated.push(shell_quote(&inode.to_string_lossy()));
            }
        }
        let search = validated.join(" ");
        // The process cwd is the root's inode path too (belt-and-suspenders);
        // `rg --no-follow` / the `find -P` fallback never follow symlinks.
        let rg = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "rg -n --no-follow -- {pat} {search} 2>/dev/null \
                 || find -P {search} -type f -exec grep -Hn -- {pat} {{}} + 2>/dev/null",
                pat = shell_quote(pattern),
            ))
            .current_dir(&root_path)
            .output()
            .await
            .map_err(RuntimeError::Io)?;
        let out = String::from_utf8_lossy(&rg.stdout);
        // Search paths are absolute inode paths (see above), so `rg`/`grep` emit
        // absolute paths — strip the root's inode prefix so results stay
        // root-relative (as they did pre-fd-anchoring) and don't leak the host
        // temp/root path to the model.
        let root_prefix = format!("{}/", root_path.to_string_lossy());
        Ok(out
            .lines()
            .map(|l| {
                l.strip_prefix(root_prefix.as_str())
                    .unwrap_or(l)
                    .to_string()
            })
            .take(max_matches)
            .collect())
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

/// Read at most `max_bytes` bytes from `file` into a `String`, trimming any
/// partial UTF-8 char at the read boundary.
///
/// Bounds memory at `max_bytes` so a multi-GB file cannot OOM the truncating
/// `read_file` path — the output is already capped at `max_bytes` by
/// [`apply_read_limits`], so reading more than that is pure waste. If the
/// `max_bytes` boundary splits a multibyte char, the partial trailing bytes are
/// trimmed to the last valid char boundary. A genuinely invalid-UTF-8 file that
/// fits within `max_bytes` still errors (mirrors the prior `read_to_string`).
///
/// Returns the decoded prefix and a flag set when the file was larger than
/// `max_bytes` (i.e. the read hit the cap) so the caller can surface a
/// truncation marker.
async fn read_bounded_string(
    file: std::fs::File,
    max_bytes: usize,
) -> RuntimeResult<(String, bool)> {
    let file = tokio::fs::File::from_std(file);
    let mut buf: Vec<u8> = Vec::with_capacity(max_bytes.min(8 * 1024));
    file.take(max_bytes as u64)
        .read_to_end(&mut buf)
        .await
        .map_err(RuntimeError::Io)?;
    // `read_full`: we read the whole file (didn't hit the cap) → any UTF-8
    // error is genuine and should surface, not be silently trimmed.
    let read_full = buf.len() < max_bytes;
    let truncated_at_cap = !read_full;
    match std::str::from_utf8(&buf) {
        Ok(s) => Ok((s.to_string(), truncated_at_cap)),
        Err(e) => {
            let vu = e.valid_up_to();
            if read_full {
                Err(RuntimeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "stream did not contain valid UTF-8",
                )))
            } else {
                // Hit the cap: trim the truncated multibyte suffix. `vu` is, by
                // definition, a valid char boundary, so `&buf[..vu]` is valid.
                Ok((
                    std::str::from_utf8(&buf[..vu])
                        .map(str::to_string)
                        .unwrap_or_default(),
                    truncated_at_cap,
                ))
            }
        }
    }
}

/// fd-anchored recursive glob descent. `dir` is an already-opened directory
/// (opened `O_NOFOLLOW` by the caller). The single-segment filename pattern
/// `fname_pat` (supporting `*`/`?`) is matched against every entry at every
/// depth under `dir`. Recursion into a subdirectory happens ONLY via
/// `openat(O_DIRECTORY | O_NOFOLLOW)` — that gate authoritatively refuses a
/// symlinked directory, so a symlink can never lead the walk out of the root.
/// `rel_prefix` is the path of `dir` relative to the session root ("" at the
/// base); results are accumulated as root-relative strings.
fn walk_glob_fd(
    mut dir: Dir,
    fname_pat: &str,
    rel_prefix: &str,
    out: &mut Vec<String>,
    limit: usize,
) -> RuntimeResult<()> {
    // Phase 1: drain entries into an owned vec. This ends the mutable borrow of
    // `dir` so phase 2 can take an immutable borrow for `dir.fd()` (needed to
    // openat children). `.`/`..` are skipped.
    let mut entries: Vec<(String, FileType)> = Vec::new();
    for res in &mut dir {
        match res {
            Ok(e) => {
                let name = e.file_name().to_string_lossy().into_owned();
                if name == "." || name == ".." {
                    continue;
                }
                entries.push((name, e.file_type()));
            }
            Err(e) => return Err(RuntimeError::Io(std::io::Error::from(e))),
        }
    }
    if out.len() >= limit {
        return Ok(());
    }
    // The parent fd for recursion (immutable borrow — no conflict with the
    // finished iterator).
    let parent_fd = dir
        .fd()
        .map_err(|e| RuntimeError::Io(std::io::Error::from(e)))?;
    for (name, ftype) in entries {
        if out.len() >= limit {
            return Ok(());
        }
        let rel = if rel_prefix.is_empty() {
            name.clone()
        } else {
            format!("{rel_prefix}/{name}")
        };
        if matches_glob(&name, fname_pat) {
            out.push(rel.clone());
        }
        // `is_dir()` is only a *hint* to attempt recursion; the authoritative
        // gate is the `openat(O_DIRECTORY | O_NOFOLLOW)` below — even if d_type
        // lies, a symlinked dir cannot be entered.
        if ftype.is_dir() {
            if let Ok(child_fd) = openat(
                parent_fd,
                name.as_str(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                if let Ok(child_dir) = Dir::new(child_fd) {
                    walk_glob_fd(child_dir, fname_pat, &rel, out, limit)?;
                }
            }
            // openat/Dir failure (symlink, ENOTDIR, race, …) → skip, don't error.
        }
    }
    Ok(())
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
    async fn read_file_bounded_read_does_not_oom_on_large_file() {
        // Regression for 0.5.2 bounded read: a file far larger than `max_bytes`
        // must be read bounded (not fully buffered) and truncated, without
        // erroring or OOMing. The old `read_to_string` of the whole file would
        // allocate the entire multi-MB body.
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        // 100 KB of ASCII, capped at 64 bytes. Only the first ~64 bytes are
        // returned (plus a truncation marker); nothing else is held in memory.
        let body = "a".repeat(100 * 1024);
        tokio::fs::write(dir.path().join("big.txt"), &body)
            .await
            .unwrap();
        let got = env
            .read_file(Path::new("big.txt"), 10_000, 64)
            .await
            .unwrap();
        assert!(
            got.contains("[... truncated at 64 bytes"),
            "expected a byte-cap truncation marker: {got:?}"
        );
        assert!(got.len() < 128, "output must be bounded near max_bytes");
    }

    #[tokio::test]
    async fn read_file_bounded_read_trims_multibyte_boundary() {
        // A multibyte char straddling the `max_bytes` cut must be trimmed to a
        // valid char boundary — no panic, no invalid UTF-8 in the output.
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        // Each `é` is 2 bytes (U+00E9, UTF-8 C3 A9). 10 of them = 20 bytes.
        // Capping at 11 bytes splits the 6th char; the trim drops its trailing
        // byte so the result is 5 chars (10 bytes).
        let body = "é".repeat(10);
        tokio::fs::write(dir.path().join("accent.txt"), body.as_bytes())
            .await
            .unwrap();
        let got = env
            .read_file(Path::new("accent.txt"), 10_000, 11)
            .await
            .unwrap();
        // The bounded prefix must be valid UTF-8 and contain only whole chars.
        assert!(
            got.starts_with("ééééé"),
            "trimmed prefix should be whole chars"
        );
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

    // ── B-Swift Phase C1b: fd-anchored write / exec / glob / grep TOCTOU ──
    // Each of these FAILED (or leaked) on the old path-based `resolve()` and
    // passes on the fd-anchored walk. The inside-target symlink cases are the
    // real TOCTOU proof: the OLD `resolve()` canonicalized a symlink whose
    // target was inside the root → passed containment → the subsequent path-
    // based op followed it. The fd-anchored walk rejects at `openat(NO_FOLLOW)`.

    #[cfg(unix)]
    #[tokio::test]
    async fn write_file_rejects_symlink_leaf_pointing_inside() {
        // OLD: resolve() canonicalized `link.txt` → inside `target.txt`
        // (contained) → `tokio::fs::write` followed the symlink and overwrote
        // the target. NEW: `openat(O_NOFOLLOW)` rejects the symlink leaf; the
        // inside target is untouched.
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("target.txt"), "ORIGINAL")
            .await
            .unwrap();
        symlink("target.txt", dir.path().join("link.txt")).unwrap();
        let res = env.write_file(Path::new("link.txt"), "OVERWRITE").await;
        assert!(
            res.is_err(),
            "writing through a symlink leaf must be rejected"
        );
        let got = tokio::fs::read_to_string(dir.path().join("target.txt"))
            .await
            .unwrap();
        assert_eq!(
            got, "ORIGINAL",
            "the symlink target must not be overwritten"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_file_rejects_symlinked_intermediate_dir() {
        // OLD: resolve() canonicalized `linkdir/file.txt` through the symlink
        // (contained) → wrote through it. NEW: the mkdirat/openat walk rejects
        // the symlinked `linkdir` component.
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("realdir"))
            .await
            .unwrap();
        symlink("realdir", dir.path().join("linkdir")).unwrap();
        let res = env.write_file(Path::new("linkdir/file.txt"), "data").await;
        assert!(
            res.is_err(),
            "writing through a symlinked intermediate dir must be rejected"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn write_file_rejects_hardlink_to_outside_secret() {
        // OLD: resolve() canonicalized the inside link (contained) →
        // `tokio::fs::write` wrote through the shared inode → corrupted
        // /outside/secret. NEW: fstat off the open fd sees `st_nlink > 1` →
        // reject; the outside file is unchanged.
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let (_outside, secret) = outside_secret("ORIGINAL-SECRET");
        std::fs::hard_link(&secret, dir.path().join("link.txt")).unwrap();
        let res = env.write_file(Path::new("link.txt"), "CORRUPTED").await;
        assert!(
            res.is_err(),
            "writing a hardlink (st_nlink > 1) must be rejected"
        );
        let got = std::fs::read_to_string(&secret).unwrap();
        assert_eq!(
            got, "ORIGINAL-SECRET",
            "the outside secret must not be corrupted"
        );
    }

    #[tokio::test]
    async fn write_file_creates_new_nested_path() {
        // Regression: the mkdirat walk + leaf open must still create brand-new
        // nested files (the happy path must not regress).
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        env.write_file(Path::new("a/b/c/new.txt"), "deep")
            .await
            .unwrap();
        let got = env
            .read_file(Path::new("a/b/c/new.txt"), 100, 1024)
            .await
            .unwrap();
        assert_eq!(got, "deep");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_rejects_symlinked_cwd_pointing_inside() {
        // OLD: resolve() canonicalized the symlinked cwd → inside dir
        // (contained) → the child ran there. NEW: open_anchored_dir rejects the
        // symlink at the openat component.
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("realcwd"))
            .await
            .unwrap();
        symlink("realcwd", dir.path().join("linkcwd")).unwrap();
        let res = env
            .exec(
                "echo hi",
                Path::new("linkcwd"),
                None,
                &CancellationToken::new(),
            )
            .await;
        assert!(res.is_err(), "a symlinked cwd must be rejected");
    }

    #[tokio::test]
    async fn exec_large_stdout_does_not_deadlock() {
        // Regression: `exec` used to `child.wait()` WITHOUT draining the stdout
        // pipe, so a child emitting more than the OS pipe buffer (~64 KB) blocked
        // on a full pipe while `wait()` blocked on the child — a deadlock. With no
        // timeout set (as here) the old code hung forever; `wait_with_output` now
        // drains both pipes concurrently, so the full output returns intact.
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let res = env
            .exec(
                "yes a | head -c 200000",
                Path::new("."),
                None,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.exit_code, 0);
        assert_eq!(
            res.stdout.len(),
            200_000,
            "full >64 KB stdout must survive without deadlock"
        );
    }

    #[tokio::test]
    async fn glob_returns_matching_files() {
        // Regression for the fd-anchored rewrite: it must still surface real
        // files at the base and nested under real subdirectories.
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("top.txt"), "x")
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("sub"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("sub/nested.txt"), "x")
            .await
            .unwrap();
        let matched = env.glob("*.txt", 100).await.unwrap();
        assert!(
            matched.iter().any(|m| m == "top.txt"),
            "base file should match: {matched:?}"
        );
        assert!(
            matched.iter().any(|m| m == "sub/nested.txt"),
            "nested file should match: {matched:?}"
        );
    }

    #[tokio::test]
    async fn glob_subdir_pattern_reports_root_relative_paths() {
        // Regression for the base-prefix bug: a pattern with a subdir base must
        // report paths relative to the ROOT, not relative to the base.
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("sub"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("sub/nested.txt"), "x")
            .await
            .unwrap();
        let matched = env.glob("sub/*.txt", 100).await.unwrap();
        assert!(
            matched.iter().any(|m| m == "sub/nested.txt"),
            "must be root-relative (`sub/nested.txt`), not base-relative: {matched:?}"
        );
        assert!(
            !matched.iter().any(|m| m == "nested.txt"),
            "base-relative leak must not happen: {matched:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn glob_does_not_traverse_symlinked_dir_to_outside() {
        // OLD glob's `path.is_dir()` FOLLOWED the symlink → recursed into the
        // outside dir → leaked its `.txt`. NEW: descent is via
        // `openat(O_DIRECTORY|O_NOFOLLOW)` → the symlinked dir is never entered.
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("inside.txt"), "ok")
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("realdir"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("realdir/nested.txt"), "ok")
            .await
            .unwrap();
        // A symlinked dir pointing at the outside temp dir (which holds
        // `secret.txt`).
        let (_outside, secret) = outside_secret("OUTSIDE-SECRET");
        let outside_dir = secret.parent().unwrap();
        symlink(outside_dir, dir.path().join("linkdir")).unwrap();
        let matched = env.glob("*.txt", 100).await.unwrap();
        assert!(
            matched.iter().any(|m| m == "inside.txt"),
            "inside file should match: {matched:?}"
        );
        assert!(
            matched.iter().any(|m| m == "realdir/nested.txt"),
            "real nested file should match: {matched:?}"
        );
        assert!(
            !matched.iter().any(|m| m.starts_with("linkdir")),
            "symlinked dir must not be traversed: {matched:?}"
        );
        for m in &matched {
            assert!(
                !m.contains("secret.txt") && !m.contains("OUTSIDE-SECRET"),
                "outside file must not leak: {m}"
            );
        }
    }

    #[tokio::test]
    async fn grep_returns_matches() {
        // Regression for the inode-anchored search: it must still surface real
        // matches inside the root, AND the output must be root-relative (not the
        // absolute host temp/root path, which the inode-path search would
        // otherwise leak).
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("note.md"), "findme here\n")
            .await
            .unwrap();
        let matched = env.grep("findme", &["."], 100).await.unwrap();
        assert!(
            matched.iter().any(|m| m.contains("findme")),
            "expected a match: {matched:?}"
        );
        // Output paths are root-relative...
        assert!(
            matched.iter().any(|m| m.starts_with("note.md:")),
            "expected a root-relative `note.md:` line: {matched:?}"
        );
        // ...and must NOT leak the host temp/root path.
        let root_str = dir.path().to_string_lossy().into_owned();
        for m in &matched {
            assert!(
                !m.contains(&root_str),
                "grep output must not leak the absolute root path: {m}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn grep_rejects_symlinked_search_path() {
        // `rg --no-follow` still follows a symlinked dir passed EXPLICITLY as a
        // search path, so the path is resolved fd-anchored to its inode and a
        // symlink is rejected outright (no leak via `linkdir -> outside`).
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let env = LocalSessionEnv::new(dir.path(), Limits::default())
            .await
            .unwrap();
        let (_outside, secret) = outside_secret("GREP-LEAK");
        let outside_dir = secret.parent().unwrap();
        symlink(outside_dir, dir.path().join("linkdir")).unwrap();
        // Explicit symlinked path → rejected (Err), never searched.
        let res = env.grep("GREP-LEAK", &["linkdir"], 100).await;
        assert!(
            res.is_err(),
            "an explicit symlinked search path must be rejected"
        );
        // And a `.` search must not traverse the symlinked dir either.
        let matched = env.grep("GREP-LEAK", &["."], 100).await.unwrap();
        assert!(
            matched.is_empty(),
            "the symlinked dir must not be traversed: {matched:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn grep_anchors_to_root_fd_not_root_path() {
        // TOCTOU for grep: after the env is built, move the real root aside and
        // replace the root *path* with a symlink to an outside dir holding a
        // secret. OLD grep used `current_dir(self.root)` (the path) → would
        // chdir through the symlink and surface the secret. NEW grep anchors to
        // `/dev/fd/{root_fd}` → chdir to the real (moved) root → no leak.
        use std::os::unix::fs::symlink;
        // A parent dir we fully control (manual, not TempDir, so the swap + the
        // symlink-over-root don't confuse Drop cleanup).
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let parent = std::env::temp_dir().join(format!("fluers-grep-swap-{nonce}"));
        std::fs::create_dir_all(&parent).unwrap();
        let root_path = parent.join("root");
        std::fs::create_dir_all(&root_path).unwrap();
        let env = LocalSessionEnv::new(&root_path, Limits::default())
            .await
            .unwrap();

        let outside = parent.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("leak.txt"), "PATHSWAP-SECRET\n").unwrap();

        // Swap: move the real root aside (sibling), then symlink the root path
        // → outside.
        let moved = parent.join("moved-real-root");
        std::fs::rename(&root_path, &moved).unwrap();
        symlink(&outside, &root_path).unwrap();

        let matched = env.grep("PATHSWAP-SECRET", &["."], 100).await.unwrap();
        assert!(
            matched.is_empty(),
            "root-fd anchoring must not follow the swapped root path: {matched:?}"
        );

        // We own `parent` fully — clean up everything under it.
        let _ = std::fs::remove_dir_all(&parent);
    }
}

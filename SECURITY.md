# Security Policy

## Sandbox isolation — current status: NOT a security boundary

The **local sandbox is not an OS-level security boundary.** There is no
`chroot`/landlock/seatbelt/UID isolation: `LocalSandbox` and its
`SessionEnv` implementation run tools (`read`, `write`, `bash`, …) against
the host filesystem with the process's own privileges. A model that runs an
arbitrary shell command (`curl … | sh`) **will succeed against the host.**

What the local sandbox *does* provide is **path confinement with no
check-then-use (TOCTOU) window** on the data paths — see below. That is a
meaningful defense against *accidental* path escape and against a confined
model exfiltrating or corrupting data through a swapped symlink/hardlink, but
it is not a defense against a determined adversary with host access.

### Do not

- Point Fluers at a model you do not trust.
- Run `fluers` as a privileged user or inside a sensitive directory.
- Expose the (future) HTTP runtime to untrusted networks until OS isolation
  lands.

### Roadmap to isolation

1. **MVP 0 / 0.5** — fd-anchored path confinement (done): every read, write,
   search, and exec cwd resolves off a single held root fd via `openat`
   per-component walks with `O_NOFOLLOW` + an authoritative `fstat` on the
   opened leaf fd. There is no canonicalize-then-contain step in any data path.
2. **Later** — OS-level isolation: `chroot`/landlock (Linux), seatbelt
   sandbox-exec (macOS), or separate UID execution.
3. **MVP 4** — remote container sandbox (E2B / Daytona) behind the `Sandbox`
   trait, where the `SessionEnv` talks to a disposable VM.

Until item 2 lands, treat `LocalSandbox` as a convenience for local
development, not a containment mechanism.

## fd-anchored confinement (0.5.0) — what is covered

`LocalSessionEnv` holds a single `OwnedFd` over the canonical root (opened once
at construction with `O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`). Every operation
walks from that fd with `openat` — the path string the model supplied is
validated for shape (no absolute paths, no `..`) but is **never re-resolved**
(`canonicalize`/`stat` on the path are absent from the data paths). A symlink
or hardlink swapped between the confinement check and the operation therefore
cannot redirect the operation.

| Path | Mechanism | TOCTOU closed |
|------|-----------|---------------|
| `read_file` / `read_file_full` | `openat(O_NOFOLLOW)` per component; `fstat` the opened leaf fd | symlink leaf / symlinked dir / hardlink exfil |
| `write_file` | `mkdirat`-walk parents; leaf `openat(WRONLY\|CREATE\|NOFOLLOW)`, `fstat` for `st_nlink`, then `ftruncate`+write off the **same** fd | symlink leaf / symlinked dir / hardlink write-through |
| `exec` cwd | `open_anchored_dir` (`openat(O_DIRECTORY\|NOFOLLOW)`); child chdirs to the inode's real path (see note) | symlinked cwd / cwd-path swap |
| `glob` | descent only via `openat(O_DIRECTORY\|NOFOLLOW)`; symlinked dirs are never entered | symlinked-dir traversal escape |
| `grep` | search root = the root fd's inode path; `rg --no-follow` / `find -P` fallback never follow symlinks | root-path swap / symlinked-dir traversal |

### Hardlink decision (read **and** write): reject `st_nlink > 1`

A file with multiple hard links is rejected on both the read and the write
path, decided off the authoritative post-open `fstat` (not the path):

- **Read:** a hardlink into the root (`ln /outside/secret in_root/link`) would
  otherwise exfiltrate the outside target.
- **Write:** writing through a hardlink mutates **every** name in the set —
  silent cross-target data loss. The leaf is opened **without** `O_TRUNC` and
  truncated via `ftruncate` **after** the `st_nlink` check, so a hardlink can
  never be mutated before the confinement decision.

### Note on the exec cwd / grep root handle (`/dev/fd` vs inode path)

The pure-inode handle for a child's cwd would be `/dev/fd/<fd>` (kernel-resolved,
no path). This works on Linux (`/proc/self/fd/<fd>` is a followable symlink) but
**not on macOS**, where fdescfs rejects `chdir("/dev/fd/<fd>")` with `ENOTDIR`.

For portability the child instead chdirs to the **inode's real path**, derived
from the open fd — `fcntl(F_GETPATH)` on macOS, `readlink("/proc/self/fd/<n>")`
on Linux — *not* from the model-supplied input string. A symlink swap on the
input path between the fd-anchored open and the spawn cannot redirect the
operation, because the path tracks the inode the fd names. A post-open *move*
of the directory is a residual race that is out of the threat model (not an OS
sandbox, and moving the dir requires write access under the confined root).

## Reporting

Report security issues privately to security@saorsalabs.com rather than
opening a public issue.

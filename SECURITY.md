# Security Policy

## Sandbox isolation — current status: NOT a security boundary

Until MVP 0 completes, the **local sandbox is not a security boundary.**

`LocalSandbox` / its `SessionEnv` implementation run tools (`read`,
`write`, `bash`, …) directly against the host filesystem with the
process's own privileges. A model that requests a path outside the
session root (`../../etc/passwd`) or runs an arbitrary shell command
(`curl … | sh`) **will succeed against the host.**

### Do not

- Point Fluers at a model you do not trust.
- Run `fluers` as a privileged user or inside a sensitive directory.
- Expose the (future) HTTP runtime to untrusted networks until
  isolation lands.

### Roadmap to isolation

1. **MVP 0** — path canonicalization: reject any tool path that resolves
   outside the session root; deny symlinks that escape.
2. **MVP 0.5 / later** — OS-level isolation: `chroot`/landlock (Linux),
   seatbelt sandbox-exec (macOS), or separate UID execution.
3. **MVP 4** — remote container sandbox (E2B / Daytona) behind the
   `Sandbox` trait, where the `SessionEnv` talks to a disposable VM.

Until item 2 lands, treat `LocalSandbox` as a convenience for local
development, not a containment mechanism.

## Reporting

Report security issues privately to security@saorsalabs.com rather than
opening a public issue.

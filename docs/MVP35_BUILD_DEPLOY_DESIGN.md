# MVP 3.5 — Build & deploy

**Status:** design (committed before implementation).
**Scope:** local binary + Docker image build. No cloud deploy until HTTP auth exists.

## Goal

Give Fluers a story for packaging and running an agent as a deployable artifact:
build a release binary, optionally build a container image, and run it. This
unblocks local/containerized deployment today without committing to a cloud
provider.

## Scope

**In scope:**

- A multi-stage `Dockerfile` that builds the `fluers` binary from source.
- `.dockerignore` to keep the image lean.
- `fluers build` — build a local release binary (`cargo build --release`).
- `fluers build --docker` — build a Docker image from the `Dockerfile`.
- `fluers deploy --target docker` — run the image locally (`docker run`).
- Docker-dependent behaviour **gated**: if Docker isn't available, `--docker` /
  `--target docker` fail with a clear message; workspace tests stay green.

**Out of scope (deferred):**

- Cloud deploy targets (Cloudflare / Fly / Render / AWS). Requires HTTP
  auth/guard middleware to exist first.
- `fluers build` producing per-agent images from a config (future: bundle a
  `fluers.toml` into the image).
- Remote registries / `docker push`.
- Cross-compilation matrixes beyond what the Dockerfile provides.
- Hot reload / zero-downtime.

## Dockerfile

Multi-stage build:

```dockerfile
# ── stage 1: build ──────────────────────────────────────────────────────
FROM rust:1.96-bookworm AS builder
WORKDIR /app
# Cache deps: copy manifests first, fetch.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin fluers

# ── stage 2: runtime ────────────────────────────────────────────────────
FROM debian:bookworm-slim
# ca-certificates for HTTPS to model providers; the binary is statically
# linked against the Rust runtime but needs system CAs for rustls.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/fluers /usr/local/bin/fluers
ENTRYPOINT ["fluers"]
```

The image runs `fluers` (e.g. `docker run fluers run --model …`).

## CLI

### `fluers build`

```text
fluers build                    # cargo build --release --bin fluers
fluers build --docker           # also builds a Docker image (tag: fluers:latest)
```

Flags:
- `--docker` — build the container image after the binary.
- `--tag <name>` — override the image tag (default `fluers:latest`).

### `fluers deploy`

```text
fluers deploy --target docker   # docker run the built image
```

Flags:
- `--target docker` (only `docker` supported for MVP; `cloudflare` stub removed).
- `-- <args…>` — trailing args passed to `fluers` inside the container (e.g.
  `fluers deploy -- run --model minimax/minimax-m3`).

## Implementation

- Replace the `build`/`deploy` stubs in `commands.rs` with real `std::process::Command`
  invocations of `cargo` and `docker`.
- `docker --version` is the availability probe; a missing `docker` produces a
  clear error, not a panic.
- No async needed for `build` (synchronous `cargo`); `deploy` runs `docker run`
  and streams output to stdout/stderr.

## Tests

- `build` with no args is smoke-tested by asserting it shells out to `cargo
  build --release --bin fluers` (use a dry-run mode or assert the command
  construction; do **not** actually invoke cargo in a unit test).
- `deploy --target docker` asserts the `docker run` command is constructed
  correctly.
- Docker-dependent paths are gated: the test suite never requires Docker.

## Exit criteria

- `fluers build` produces a working release binary.
- `fluers build --docker` produces a Docker image (when Docker is available).
- `fluers deploy --target docker -- run --model X` runs the agent in a
  container (when Docker is available).
- `cargo nextest run --workspace` green without Docker; fmt + strict clippy clean.

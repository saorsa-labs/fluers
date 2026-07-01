# Multi-stage build for the Fluers CLI binary.
# See docs/MVP35_BUILD_DEPLOY_DESIGN.md.

# ── stage 1: build ──────────────────────────────────────────────────────
FROM rust:1.96-bookworm AS builder
WORKDIR /app

# Copy manifests + source, then build the release binary. (No separate
# dependency-fetch layer: the workspace is small enough that a full build is
# faster to reason about than a two-stage COPY dance. A future optimization
# can layer Cargo.toml/Cargo.lock first for cargo-chichi-style caching.)
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin fluers

# ── stage 2: runtime ────────────────────────────────────────────────────
FROM debian:bookworm-slim

# ca-certificates for HTTPS to model providers (rustls reads the system CA
# bundle); curl for the HEALTHCHECK. The binary is otherwise self-contained.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user (SECURITY.md: don't run privileged). Home + workdir are
# owned so the agent can write session files under them.
RUN groupadd --system --gid 10001 fluers \
    && useradd --system --uid 10001 --gid fluers \
       --create-home --home-dir /home/fluers --shell /usr/sbin/nologin fluers \
    && mkdir -p /app \
    && chown -R fluers:fluers /home/fluers /app

WORKDIR /app
COPY --from=builder /app/target/release/fluers /usr/local/bin/fluers

# The dev server defaults to port 3000. EXPOSE documents it for `docker run -p`.
EXPOSE 3000

USER fluers

# Liveness probe: the server's `/health` endpoint returns 200.
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:${FLUERS_PORT:-3000}/health || exit 1

ENTRYPOINT ["fluers"]

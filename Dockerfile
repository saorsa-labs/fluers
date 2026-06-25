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
# bundle). The binary is otherwise self-contained.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/fluers /usr/local/bin/fluers

ENTRYPOINT ["fluers"]

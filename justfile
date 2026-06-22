# Fluers — standard Rust recipes (project-wide policy)

default:
    @just --list

# Format all code
fmt:
    cargo fmt --all

# Lint with zero warnings (deny panics/unwrap/expect in production)
lint:
    cargo clippy --workspace --all-targets -- -D warnings -D clippy::panic -D clippy::unwrap_used -D clippy::expect_used

# Type-check only
check:
    cargo check --workspace --all-targets

# Fast tests
test:
    cargo nextest run --workspace

# Verbose tests with output
test-verbose:
    cargo nextest run --workspace --no-capture

# Debug build
build:
    cargo build --workspace

# Release build
build-release:
    cargo build --workspace --release

# Build the CLI binary
build-cli:
    cargo build -p fluers-cli

# Full validation gate (fmt -> lint -> check -> test)
check-all: fmt lint check test
    @echo "✓ All checks passed"

# Documentation
doc:
    cargo doc --workspace --no-deps

# Clean build artifacts
clean:
    cargo clean

# Run the CLI (debug)
run *ARGS:
    cargo run -p fluers-cli -- {{ARGS}}

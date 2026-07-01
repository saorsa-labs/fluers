//! `fluers` — the command-line entry point.
//!
//! Mirrors Flue's `flue` binary (`packages/cli/bin/flue.ts`). Provides a
//! `version`, a `run` command that runs a local agent, and `dev`/`build`/
//! `deploy` for the local dev server, release binary (+ optional Docker image),
//! and Docker deployment respectively.

#![forbid(unsafe_code)]

mod agent_config;
mod commands;
mod config;

use clap::{Parser, Subcommand};

/// The Fluers CLI.
#[derive(Parser, Debug)]
#[command(name = "fluers", version, about = "The native Rust Flue agent harness", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Show version and crate layout info.
    Version,
    /// Run an agent locally.
    Run(Box<commands::RunArgs>),
    /// Start the dev server.
    Dev(Box<commands::DevArgs>),
    /// Build the fluers binary (and optionally a Docker image).
    Build(commands::BuildArgs),
    /// Deploy to a local Docker runtime.
    Deploy(commands::DeployArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Version => commands::version(),
        Command::Run(args) => commands::run(*args).await,
        Command::Dev(args) => commands::dev(*args).await,
        Command::Build(args) => commands::build(args),
        Command::Deploy(args) => commands::deploy(args).await,
    }?;

    Ok(())
}

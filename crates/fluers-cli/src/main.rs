//! `fluers` — the command-line entry point.
//!
//! Mirrors Flue's `flue` binary (`packages/cli/bin/flue.ts`). MVP provides a
//! `version`, a `run` command that boots a local agent, and stubs for
//! `dev`/`build`/`deploy`.

#![forbid(unsafe_code)]

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
    /// Run an agent locally (MVP: prints the resolved profile).
    Run(Box<commands::RunArgs>),
    /// Start the dev server (stub).
    Dev(commands::DevArgs),
    /// Build agents for deployment (stub).
    Build,
    /// Deploy to a hosted runtime (stub).
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
        Command::Dev(args) => commands::dev(args).await,
        Command::Build => commands::build(),
        Command::Deploy(args) => commands::deploy(args).await,
    }?;

    Ok(())
}

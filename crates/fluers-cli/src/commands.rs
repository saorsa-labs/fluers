//! CLI subcommands.

use clap::Args;
use std::sync::Arc;

use fluers_runtime::Sandbox;

/// `fluers version`
pub(crate) fn version() -> anyhow::Result<()> {
    println!("fluers {}", env!("CARGO_PKG_VERSION"));
    println!("  crates: fluers-core fluers-runtime fluers-cli fluers-sdk fluers-mcp fluers-postgres fluers-otel");
    println!("  upstream: Flue (https://github.com/withastro/flue) — Apache-2.0");
    Ok(())
}

/// Args for `run`.
#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    /// Model id, e.g. `anthropic/claude-sonnet-4-6`.
    #[arg(long, default_value = "anthropic/claude-sonnet-4-6")]
    pub model: String,
    /// Prompt to run (MVP: echoed, not yet sent to a model).
    #[arg(long)]
    pub prompt: Option<String>,
}

/// Args for `dev`.
#[derive(Args, Debug)]
pub(crate) struct DevArgs {
    /// Port to serve on.
    #[arg(long, default_value_t = 3000)]
    pub port: u16,
}

/// Args for `deploy`.
#[derive(Args, Debug)]
pub(crate) struct DeployArgs {
    /// Target platform id (stub).
    #[arg(long, default_value = "cloudflare")]
    pub target: String,
}

/// `fluers run`
pub(crate) async fn run(args: RunArgs) -> anyhow::Result<()> {
    let sandbox = Arc::new(fluers_runtime::local());
    let env = sandbox.env_for(std::path::Path::new(".")).await?;
    let tools = fluers_runtime::builtin_tools(env);

    let agent = fluers_runtime::define_agent(|b| {
        b.model(&args.model)
            .instructions("You are a Fluers agent.".to_string());
        for t in &tools {
            b.tool(t.clone());
        }
        b.sandbox(sandbox.clone());
        Ok(())
    })
    .await?;

    println!("✓ resolved agent:");
    println!("  model: {}", agent.profile.model.id);
    println!("  tools: {}", agent.profile.tools.len());
    println!("  sandbox: {}", agent.profile.sandbox.name());
    if let Some(p) = &args.prompt {
        println!("  prompt: {p}");
        println!("  (model provider not yet wired — see PORTING_PLAN.md MVP 1)");
    }
    Ok(())
}

/// `fluers dev` (stub).
pub(crate) async fn dev(args: DevArgs) -> anyhow::Result<()> {
    println!("✓ dev server would listen on :{} (stub — MVP 3)", args.port);
    Ok(())
}

/// `fluers build` (stub).
pub(crate) fn build() -> anyhow::Result<()> {
    println!("✓ build (stub — MVP 3)");
    Ok(())
}

/// `fluers deploy` (stub).
pub(crate) async fn deploy(args: DeployArgs) -> anyhow::Result<()> {
    println!("✓ deploy → {} (stub — MVP 3)", args.target);
    Ok(())
}

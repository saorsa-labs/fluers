//! CLI subcommands.

use clap::Args;
use std::path::PathBuf;
use std::sync::Arc;

use fluers_core::message::{AgentMessage, ContentBlock, Role};
use fluers_core::tool::Tool;
use fluers_core::{run_agent, Model, RunConfig};
use fluers_providers::OpenAiCompatibleProvider;
use fluers_runtime::{Limits, LocalSessionEnv};
use tokio_util::sync::CancellationToken;

/// `fluers version`
pub(crate) fn version() -> anyhow::Result<()> {
    println!("fluers {}", env!("CARGO_PKG_VERSION"));
    println!("  crates: fluers-core fluers-runtime fluers-providers fluers-cli fluers-sdk fluers-mcp fluers-postgres fluers-otel");
    println!("  upstream: Flue (https://github.com/withastro/flue) — Apache-2.0");
    Ok(())
}

/// Args for `run`.
#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    /// Model id. OpenRouter: `minimax/minimax-m3`, `anthropic/claude-sonnet-4`,
    /// etc. MiniMax direct: `MiniMax-M3`.
    #[arg(long, default_value = "minimax/minimax-m3")]
    pub model: String,
    /// Provider backend: `openrouter` (default), `minimax`, or `custom`.
    #[arg(long, default_value = "openrouter")]
    pub provider: String,
    /// Custom base URL (with `--provider custom`). Ignored otherwise.
    #[arg(long)]
    pub base_url: Option<String>,
    /// The prompt to run.
    #[arg(long)]
    pub prompt: Option<String>,
    /// Working directory the sandbox is rooted in (default: cwd).
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    /// Maximum model turns.
    #[arg(long, default_value_t = 12)]
    pub max_turns: usize,
    /// Disable all tools (text-only round-trip).
    #[arg(long, default_value_t = false)]
    pub no_tools: bool,
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

/// Resolve a provider from the chosen backend.
fn build_provider(args: &RunArgs) -> anyhow::Result<OpenAiCompatibleProvider> {
    match args.provider.as_str() {
        "openrouter" => Ok(OpenAiCompatibleProvider::openrouter()
            .with_header("X-Title", "fluers")
            .with_header("HTTP-Referer", "https://github.com/saorsa-labs/fluers")),
        "minimax" => Ok(OpenAiCompatibleProvider::minimax()),
        "custom" => {
            let url = args
                .base_url
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--provider custom requires --base-url"))?;
            // Reads API key from OPENAI_API_KEY by convention for custom.
            Ok(OpenAiCompatibleProvider::new(
                url,
                std::env::var("OPENAI_API_KEY").unwrap_or_default(),
            ))
        }
        other => Err(anyhow::anyhow!(
            "unknown provider `{other}` (openrouter|minimax|custom)"
        )),
    }
}

/// `fluers run`
pub(crate) async fn run(args: RunArgs) -> anyhow::Result<()> {
    let provider = build_provider(&args)?;
    let workdir = args
        .workdir
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let env: Arc<dyn fluers_runtime::SessionEnv> =
        Arc::new(LocalSessionEnv::new(&workdir, Limits::default()).await?);

    let tools: Vec<Arc<dyn Tool>> = if args.no_tools {
        Vec::new()
    } else {
        fluers_runtime::mvp_tools(env.clone())
    };

    // Seed the conversation: a terse system message + the user prompt.
    let system = "You are a Fluers agent. Use the provided tools when they help. \
                  Paths are relative to the working directory. Be concise.";
    let prompt = args
        .prompt
        .clone()
        .unwrap_or_else(|| "No prompt given. Greet the user briefly.".to_string());
    let mut messages = vec![
        AgentMessage {
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: system.into(),
            }],
        },
        AgentMessage {
            role: Role::User,
            content: vec![ContentBlock::Text { text: prompt }],
        },
    ];

    let model = Model::new(&args.model);
    let config = RunConfig {
        max_turns: args.max_turns,
        ..Default::default()
    };
    let cancel = CancellationToken::new();

    eprintln!(
        "→ model: {}   provider: {}   tools: {}",
        args.model,
        args.provider,
        tools.len()
    );
    eprintln!("→ workdir: {}", workdir.display());

    let outcome = run_agent(&provider, &tools, &mut messages, &model, &config, &cancel)
        .await
        .map_err(|e| anyhow::anyhow!("agent run failed: {e}"))?;

    eprintln!("→ done in {} turn(s)", outcome.turns);
    println!("{}", outcome.final_text);
    Ok(())
}

/// `fluers dev` (stub).
pub(crate) async fn dev(args: DevArgs) -> anyhow::Result<()> {
    println!("✓ dev server would listen on :{} (stub — MVP 3)", args.port);
    Ok(())
}

/// `fluers build` (stub).
pub(crate) fn build() -> anyhow::Result<()> {
    println!("✓ build (stub — MVP 3.5)");
    Ok(())
}

/// `fluers deploy` (stub).
pub(crate) async fn deploy(args: DeployArgs) -> anyhow::Result<()> {
    println!("✓ deploy → {} (stub — MVP 3.5)", args.target);
    Ok(())
}

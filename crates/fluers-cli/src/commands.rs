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
    #[arg(long)]
    pub model: Option<String>,
    /// Provider backend: `openrouter` (default), `minimax`, or `custom`.
    #[arg(long)]
    pub provider: Option<String>,
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
    #[arg(long)]
    pub max_turns: Option<usize>,
    /// Per-turn provider deadline, in milliseconds.
    #[arg(long)]
    pub turn_timeout_ms: Option<u64>,
    /// How many tool calls may run in parallel within a turn (1 = sequential).
    #[arg(long)]
    pub tool_concurrency: Option<usize>,
    /// Disable all tools (text-only round-trip).
    #[arg(long, default_value_t = false)]
    pub no_tools: bool,
    /// Stream tokens to stdout as they arrive (text-only; falls back to
    /// non-streaming when tools are enabled).
    #[arg(long, default_value_t = false)]
    pub stream: bool,
    /// Override the env var name the API key is read from (custom provider).
    #[arg(long)]
    pub api_key_env: Option<String>,
    /// Optional TOML config file (see `Config`). Defaults to `fluers.toml` if present.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Session id to resume. If omitted, a new session is created (and its id
    /// printed). If the id exists on disk, the conversation is continued;
    /// otherwise a new session with that id is started.
    #[arg(long)]
    pub session: Option<String>,
    /// Directory for JSON session files (default: `~/.fluers/sessions`).
    #[arg(long)]
    pub sessions_dir: Option<PathBuf>,
    /// List persisted session ids and exit.
    #[arg(long, default_value_t = false)]
    pub list_sessions: bool,
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
    match args.provider.as_deref().unwrap_or("openrouter") {
        "openrouter" => Ok(OpenAiCompatibleProvider::try_openrouter()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .with_header("X-Title", "fluers")
            .with_header("HTTP-Referer", "https://github.com/saorsa-labs/fluers")),
        "minimax" => {
            Ok(OpenAiCompatibleProvider::try_minimax().map_err(|e| anyhow::anyhow!("{e}"))?)
        }
        "custom" => {
            let url = args
                .base_url
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--provider custom requires --base-url"))?;
            let envvar = args
                .api_key_env
                .clone()
                .unwrap_or_else(|| "OPENAI_API_KEY".into());
            let key = std::env::var(&envvar).unwrap_or_default();
            Ok(OpenAiCompatibleProvider::try_new(url, key, envvar)
                .map_err(|e| anyhow::anyhow!("{e}"))?)
        }
        other => Err(anyhow::anyhow!(
            "unknown provider `{other}` (openrouter|minimax|custom)"
        )),
    }
}

/// `fluers run`
pub(crate) async fn run(args: RunArgs) -> anyhow::Result<()> {
    // Load config: explicit --config path, else ./fluers.toml if present.
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| PathBuf::from("fluers.toml"));
    let cfg = crate::config::Config::load(&config_path)?;

    // Resolve effective args: CLI flags override config, config overrides defaults.
    let provider_name = args
        .provider
        .clone()
        .or(cfg.provider)
        .unwrap_or_else(|| "openrouter".into());
    let model = args
        .model
        .clone()
        .or(cfg.model)
        .unwrap_or_else(|| "minimax/minimax-m3".into());
    let workdir = args
        .workdir
        .clone()
        .or(cfg.workdir)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_default());
    let max_turns = args.max_turns.or(cfg.max_turns).unwrap_or(12);
    let turn_timeout_ms = args.turn_timeout_ms.or(cfg.turn_timeout_ms);
    let tool_concurrency = args.tool_concurrency.or(cfg.tool_concurrency).unwrap_or(1);
    let api_key_env = args.api_key_env.clone().or(cfg.api_key_env);

    let merged = RunArgs {
        provider: Some(provider_name),
        model: Some(model),
        base_url: args.base_url.clone().or(cfg.base_url),
        prompt: args.prompt.clone(),
        workdir: Some(workdir.clone()),
        max_turns: Some(max_turns),
        turn_timeout_ms,
        tool_concurrency: Some(tool_concurrency),
        no_tools: args.no_tools,
        stream: args.stream,
        api_key_env,
        config: None,
        session: args.session.clone(),
        sessions_dir: args.sessions_dir.clone(),
        list_sessions: false,
    };

    let provider = build_provider(&merged)?;
    let env: Arc<dyn fluers_runtime::SessionEnv> =
        Arc::new(LocalSessionEnv::new(&workdir, Limits::default()).await?);

    let tools: Vec<Arc<dyn Tool>> = if merged.no_tools {
        Vec::new()
    } else {
        fluers_runtime::mvp_tools(env.clone())
    };

    // ── Session persistence setup ──────────────────────────────────────────
    // Resolve the sessions directory (default ~/.fluers/sessions) and build a
    // JSON-file adapter. Then either resume an existing session or create one.
    let sessions_dir = merged.sessions_dir.clone().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".fluers").join("sessions")
    });
    let adapter: Arc<dyn fluers_runtime::PersistenceAdapter> =
        Arc::new(fluers_runtime::JsonFileAdapter::new(sessions_dir));

    // `--list-sessions`: print ids and exit.
    if args.list_sessions {
        let ids = adapter
            .list_sessions()
            .await
            .map_err(|e| anyhow::anyhow!("list sessions failed: {e}"))?;
        if ids.is_empty() {
            println!("(no saved sessions)");
        } else {
            for id in &ids {
                println!("{id}");
            }
        }
        return Ok(());
    }

    // Resolve the session id: explicit --session, else generate a new one.
    let session_id = match &merged.session {
        Some(s) => uuid::Uuid::parse_str(s)
            .map_err(|e| anyhow::anyhow!("invalid --session id `{s}`: {e}"))?,
        None => uuid::Uuid::new_v4(),
    };

    // Try to load an existing session with this id.
    let runner = fluers_runtime::SessionRunner::load(adapter.clone(), session_id).await?;
    let resuming = runner.is_some();

    let model_id = merged
        .model
        .as_deref()
        .unwrap_or("minimax/minimax-m3")
        .to_string();
    let max_turns = merged.max_turns.unwrap_or(12);

    let mut messages: Vec<AgentMessage> = match runner {
        Some(r) => r.messages(),
        None => Vec::new(),
    };

    if resuming {
        eprintln!(
            "→ resumed session {session_id} ({} prior messages)",
            messages.len()
        );
        // When resuming, append the prompt (if any) as a new user turn.
        if let Some(prompt) = &merged.prompt {
            messages.push(AgentMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: prompt.clone(),
                }],
            });
        }
    } else {
        // New session: seed system message + user prompt.
        let system = "You are a Fluers agent. Use the provided tools when they help. \
                      Paths are relative to the working directory. Be concise.";
        let prompt = merged
            .prompt
            .clone()
            .unwrap_or_else(|| "No prompt given. Greet the user briefly.".to_string());
        messages.push(AgentMessage {
            role: Role::System,
            content: vec![ContentBlock::Text {
                text: system.into(),
            }],
        });
        messages.push(AgentMessage {
            role: Role::User,
            content: vec![ContentBlock::Text { text: prompt }],
        });
    }

    // Build the session runner (TurnSink) that persists after each turn.
    let runner =
        fluers_runtime::SessionRunner::new(adapter, session_id, model_id.clone(), max_turns, None);
    let runner_ref: &dyn fluers_core::TurnSink = &runner;

    let model = Model::new(&model_id);
    let config = RunConfig {
        max_turns,
        turn_timeout_ms: merged.turn_timeout_ms.or(Some(120_000)),
        tool_concurrency: merged.tool_concurrency.unwrap_or(1),
        ..Default::default()
    };
    let cancel = CancellationToken::new();

    eprintln!(
        "→ session: {session_id}   model: {model_id}   provider: {}   tools: {}",
        merged.provider.as_deref().unwrap_or("openrouter"),
        tools.len()
    );
    eprintln!(
        "→ workdir: {}   tool_concurrency: {}",
        workdir.display(),
        config.tool_concurrency
    );

    let outcome = run_agent(
        &provider,
        &tools,
        &mut messages,
        &model,
        &config,
        &cancel,
        Some(runner_ref),
    )
    .await
    .map_err(|e| anyhow::anyhow!("agent run failed: {e}"))?;

    eprintln!("→ done in {} turn(s)", outcome.turns);
    eprintln!("→ session persisted: {session_id}  (resume with --session {session_id})");
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

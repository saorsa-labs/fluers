//! CLI subcommands.

use clap::Args;
use std::path::PathBuf;
use std::sync::Arc;

use fluers_core::message::{AgentMessage, ContentBlock, Role};
use fluers_core::tool::Tool;
use fluers_core::{run_agent, run_agent_streaming, Model, RunConfig, StreamEvent};
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
    /// Provider backend (default `openrouter`).
    #[arg(long)]
    pub provider: Option<String>,
    /// Model id.
    #[arg(long)]
    pub model: Option<String>,
    /// Working directory the sandbox is rooted in.
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    /// Directory for JSON session files (default: `~/.fluers/sessions`).
    #[arg(long)]
    pub sessions_dir: Option<PathBuf>,
    /// Disable all tools (text-only agent).
    #[arg(long, default_value_t = false)]
    pub no_tools: bool,
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

    // ── Session persistence setup ──────────────────────────────────────────
    // Resolve the sessions directory (default ~/.fluers/sessions) and build a
    // JSON-file adapter. `--list-sessions` is handled here, before provider/env
    // setup, so it works without an API key.
    let sessions_dir = merged.sessions_dir.clone().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".fluers").join("sessions")
    });
    let adapter: Arc<dyn fluers_runtime::PersistenceAdapter> =
        Arc::new(fluers_runtime::JsonFileAdapter::new(sessions_dir));

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

    let provider = build_provider(&merged)?;
    let env: Arc<dyn fluers_runtime::SessionEnv> =
        Arc::new(LocalSessionEnv::new(&workdir, Limits::default()).await?);

    let tools: Vec<Arc<dyn Tool>> = if merged.no_tools {
        Vec::new()
    } else {
        fluers_runtime::mvp_tools(env.clone())
    };

    // Resolve the session id: explicit --session, else generate a new one.
    let session_id = match &merged.session {
        Some(s) => uuid::Uuid::parse_str(s)
            .map_err(|e| anyhow::anyhow!("invalid --session id `{s}`: {e}"))?,
        None => uuid::Uuid::new_v4(),
    };

    // Try to load an existing session. If found, the loaded runner becomes
    // the TurnSink — preserving its persisted model, max_turns, system_message,
    // and metadata. On resume, persisted config values win.
    let loaded = fluers_runtime::SessionRunner::load(adapter.clone(), session_id).await?;
    let resuming = loaded.is_some();

    let (model_id, max_turns, system_message, mut messages) = match loaded {
        Some(ref r) => {
            let mut msgs = r.messages();
            if let Some(prompt) = &merged.prompt {
                msgs.push(AgentMessage {
                    role: Role::User,
                    content: vec![ContentBlock::Text {
                        text: prompt.clone(),
                    }],
                });
            }
            (
                r.model_id().to_string(),
                r.max_turns(),
                r.system_message(),
                msgs,
            )
        }
        None => {
            let system = "You are a Fluers agent. Use the provided tools when they help. \
                          Paths are relative to the working directory. Be concise.";
            let prompt = merged
                .prompt
                .clone()
                .unwrap_or_else(|| "No prompt given. Greet the user briefly.".to_string());
            let msgs = vec![
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
            (
                merged
                    .model
                    .as_deref()
                    .unwrap_or("minimax/minimax-m3")
                    .to_string(),
                merged.max_turns.unwrap_or(12),
                Some(system.to_string()),
                msgs,
            )
        }
    };

    // Build the session runner (TurnSink) that persists after each turn.
    // Reuse the loaded runner if we have one; otherwise create a new one.
    let runner = match loaded {
        Some(r) => r,
        None => fluers_runtime::SessionRunner::new(
            adapter,
            session_id,
            model_id.clone(),
            max_turns,
            system_message,
        ),
    };
    let runner_ref: &dyn fluers_core::TurnSink = &runner;

    let model = Model::new(&model_id);
    let config = RunConfig {
        max_turns,
        turn_timeout_ms: merged.turn_timeout_ms.or(Some(120_000)),
        tool_concurrency: merged.tool_concurrency.unwrap_or(1),
        ..Default::default()
    };
    let cancel = CancellationToken::new();

    if resuming {
        let prior = if merged.prompt.is_some() {
            messages.len().saturating_sub(1)
        } else {
            messages.len()
        };
        eprintln!("→ resumed session {session_id} ({prior} prior messages)");
    }
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

    // Streaming path: token-by-token output (text-only). Also persists via
    // the TurnSink after each turn.
    if merged.stream && tools.is_empty() {
        let mut on_event = |ev: &StreamEvent| {
            if let StreamEvent::TextDelta(t) = ev {
                print!("{t}");
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        };
        let outcome = run_agent_streaming(
            &provider,
            &tools,
            &mut messages,
            &model,
            &config,
            &cancel,
            &mut on_event,
            Some(runner_ref),
        )
        .await
        .map_err(|e| anyhow::anyhow!("agent run failed: {e}"))?;
        println!();
        eprintln!("→ done in {} turn(s)", outcome.turns);
        eprintln!("→ session persisted: {session_id}  (resume with --session {session_id})");
        return Ok(());
    }

    if merged.stream && !tools.is_empty() {
        eprintln!("→ note: streaming + tools together falls back to buffered mode");
    }

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

/// `fluers dev` — boot the local HTTP server with a default agent.
pub(crate) async fn dev(args: DevArgs) -> anyhow::Result<()> {
    let provider = build_provider(&RunArgs {
        provider: args.provider.clone(),
        model: args.model.clone(),
        base_url: None,
        prompt: None,
        workdir: None,
        max_turns: None,
        turn_timeout_ms: None,
        tool_concurrency: None,
        no_tools: args.no_tools,
        stream: false,
        api_key_env: None,
        config: None,
        session: None,
        sessions_dir: None,
        list_sessions: false,
    })?;
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
    let sessions_dir = args.sessions_dir.clone().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".fluers").join("sessions")
    });
    let sessions: Arc<dyn fluers_runtime::PersistenceAdapter> =
        Arc::new(fluers_runtime::JsonFileAdapter::new(sessions_dir));
    let state = Arc::new(fluers_server::ServerState::new(sessions));
    let handle = fluers_server::AgentHandle {
        provider: Arc::new(provider),
        model: Model::new(args.model.as_deref().unwrap_or("minimax/minimax-m3")),
        tools,
        config: RunConfig::default(),
        system_prompt: "You are a Fluers agent. Use the provided tools when they help. \
                        Paths are relative to the working directory. Be concise."
            .into(),
        description: "default Fluers agent".into(),
    };
    state.register("default", handle);

    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], args.port));
    eprintln!("→ fluers dev server");
    eprintln!("  endpoints:");
    eprintln!("    GET    /health");
    eprintln!("    GET    /agents");
    eprintln!("    POST   /agents/default/invoke");
    eprintln!("    POST   /agents/default/stream");
    eprintln!("    GET    /runs/{{run_id}}");
    fluers_server::serve(addr, state).await
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

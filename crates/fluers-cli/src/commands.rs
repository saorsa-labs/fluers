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

/// Redact the password component of any `postgres://user:pass@host` URL
/// embedded in a driver error string, so credentials never leak to stderr
/// or downstream logs. Leaves the rest of the message intact for debugging.
fn redact_postgres_url(message: &str) -> String {
    // Match `postgres[ql]://user:pass@` and replace `pass` with `***`.
    // Keep it simple and robust: operate on the whole message.
    let mut out = String::with_capacity(message.len());
    let bytes = message.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if message[i..].to_ascii_lowercase().starts_with("postgres") {
            // Look for `://` after `postgres` or `postgresql`.
            let rest = &message[i..];
            let scheme_len = if rest.to_ascii_lowercase().starts_with("postgresql://") {
                13
            } else if rest.to_ascii_lowercase().starts_with("postgres://") {
                11
            } else {
                out.push(bytes[i] as char);
                i += 1;
                continue;
            };
            // Find the end of this URL (next whitespace or end of string).
            let url_end = message[i..]
                .char_indices()
                .skip(scheme_len)
                .find(|(_, c)| c.is_whitespace())
                .map(|(idx, _)| i + idx)
                .unwrap_or(message.len());
            let url = &message[i..url_end];
            out.push_str(&redact_url_password(url));
            i = url_end;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Replace the password in a single URL with `***`, if present.
fn redact_url_password(url: &str) -> String {
    // Split on `://`, then on the first `@` in the authority.
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some(at_idx) = rest.find('@') else {
        return url.to_string(); // no userinfo
    };
    let userinfo = &rest[..at_idx];
    let host_and_path = &rest[at_idx..]; // includes the '@'
    let Some((user, _password)) = userinfo.split_once(':') else {
        return url.to_string(); // no password
    };
    format!("{scheme}://{user}:***{host_and_path}")
}

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
    /// Ignored when `--database-url` is set.
    #[arg(long)]
    pub sessions_dir: Option<PathBuf>,
    /// Postgres connection URL for session persistence (e.g.
    /// `postgres://user:pass@host:5432/db`). When set, sessions are persisted
    /// to Postgres instead of JSON files. Falls back to the
    /// `FLUERS_DATABASE_URL` environment variable so credentials need not
    /// appear in shell history / `ps` output.
    #[arg(long, env = "FLUERS_DATABASE_URL")]
    pub database_url: Option<String>,
    /// mem0 base URL for semantic long-term memory (e.g.
    /// `https://api.mem0.ai`). Memory is enabled only when this **and**
    /// `--memory-user-id` are both set. Falls back to `FLUERS_MEM0_URL`.
    #[arg(long, env = "FLUERS_MEM0_URL")]
    pub memory_url: Option<String>,
    /// mem0 API key (sent as `Authorization: Token <key>`). Falls back to
    /// `FLUERS_MEM0_API_KEY`.
    #[arg(long, env = "FLUERS_MEM0_API_KEY")]
    pub memory_api_key: Option<String>,
    /// Per-user partition id for semantic memory. Falls back to
    /// `FLUERS_MEMORY_USER_ID`.
    #[arg(long, env = "FLUERS_MEMORY_USER_ID")]
    pub memory_user_id: Option<String>,
    /// Maximum memories to inject into the system prompt for new sessions
    /// (default 5).
    #[arg(long, default_value_t = 5)]
    pub memory_limit: usize,
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
    /// Ignored when `--database-url` is set.
    #[arg(long)]
    pub sessions_dir: Option<PathBuf>,
    /// Postgres connection URL for session persistence. When set, dev-server
    /// sessions are persisted to Postgres instead of JSON files. Falls back to
    /// the `FLUERS_DATABASE_URL` environment variable.
    #[arg(long, env = "FLUERS_DATABASE_URL")]
    pub database_url: Option<String>,
    /// mem0 base URL for semantic long-term memory. Falls back to
    /// `FLUERS_MEM0_URL`.
    #[arg(long, env = "FLUERS_MEM0_URL")]
    pub memory_url: Option<String>,
    /// mem0 API key. Falls back to `FLUERS_MEM0_API_KEY`.
    #[arg(long, env = "FLUERS_MEM0_API_KEY")]
    pub memory_api_key: Option<String>,
    /// Per-user partition id for semantic memory. Falls back to
    /// `FLUERS_MEMORY_USER_ID`.
    #[arg(long, env = "FLUERS_MEMORY_USER_ID")]
    pub memory_user_id: Option<String>,
    /// Maximum memories to inject into the system prompt (default 5).
    #[arg(long, default_value_t = 5)]
    pub memory_limit: usize,
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

/// Build an optional semantic-memory adapter from the memory flags. Memory is
/// enabled only when both `--memory-url` and `--memory-user-id` are present.
/// Returns `Ok(None)` (memory disabled) when either is missing — no error.
fn build_memory_adapter(
    args: &RunArgs,
) -> anyhow::Result<Option<Arc<dyn fluers_memory::MemoryAdapter>>> {
    let (Some(url), Some(user_id)) = (&args.memory_url, &args.memory_user_id) else {
        if args.memory_url.is_some() != args.memory_user_id.is_some() {
            eprintln!("→ memory disabled: both --memory-url and --memory-user-id are required");
        }
        return Ok(None);
    };
    let _ = user_id; // used later by the sink; adapter construction only needs url + key.
    let api_key = args.memory_api_key.clone().unwrap_or_default();
    let adapter = fluers_memory::Mem0RestAdapter::new(url, api_key)
        .map_err(|e| anyhow::anyhow!("memory adapter setup failed: {e}"))?;
    Ok(Some(Arc::new(adapter)))
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
        database_url: args.database_url.clone(),
        memory_url: args.memory_url.clone(),
        memory_api_key: args.memory_api_key.clone(),
        memory_user_id: args.memory_user_id.clone(),
        memory_limit: args.memory_limit,
        list_sessions: false,
    };

    // ── Session persistence setup ──────────────────────────────────────────
    // Resolve the persistence adapter. `--database-url` selects a Postgres
    // backend; otherwise JSON files under the sessions directory are used.
    // `--list-sessions` is handled here, before provider/env setup, so it
    // works without an API key.
    let adapter: Arc<dyn fluers_runtime::PersistenceAdapter> =
        if let Some(url) = merged.database_url.as_ref() {
            Arc::new(
                fluers_postgres::PostgresAdapter::connect(url)
                    .await
                    .map_err(|e| {
                        let msg = redact_postgres_url(&e.to_string());
                        anyhow::anyhow!("postgres connect failed: {msg}")
                    })?,
            )
        } else {
            let sessions_dir = merged.sessions_dir.clone().unwrap_or_else(|| {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home).join(".fluers").join("sessions")
            });
            Arc::new(fluers_runtime::JsonFileAdapter::new(sessions_dir))
        };

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

    // Build the optional semantic-memory adapter. Memory is enabled only when
    // both URL and user id are configured. The adapter is built once and shared
    // between the injection search (new sessions) and the per-turn sink.
    let memory_adapter: Option<Arc<dyn fluers_memory::MemoryAdapter>> =
        build_memory_adapter(&merged)?;

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
            let mut system = String::from(
                "You are a Fluers agent. Use the provided tools when they help. \
                 Paths are relative to the working directory. Be concise.",
            );
            // Memory injection — NEW sessions only. For resumed sessions, the
            // persisted system message is used unchanged (exact replay wins).
            if let Some(adapter) = &memory_adapter {
                let prompt = merged
                    .prompt
                    .clone()
                    .unwrap_or_else(|| "No prompt given.".to_string());
                match adapter
                    .search(&fluers_memory::MemorySearchRequest {
                        user_id: merged
                            .memory_user_id
                            .clone()
                            .unwrap_or_else(|| "default".into()),
                        query: prompt,
                        top_k: merged.memory_limit,
                    })
                    .await
                {
                    Ok(memories) => {
                        let block = fluers_memory::format_memories(&memories);
                        if !block.is_empty() {
                            system.push_str("\n\n");
                            system.push_str(&block);
                            eprintln!(
                                "→ injected {} memory(ies) into system prompt",
                                memories.len()
                            );
                        }
                    }
                    // Fail-open: injection failure is logged and skipped.
                    Err(e) => eprintln!("→ memory search failed (ignored): {e}"),
                }
            }
            let prompt = merged
                .prompt
                .clone()
                .unwrap_or_else(|| "No prompt given. Greet the user briefly.".to_string());
            let msgs = vec![
                AgentMessage {
                    role: Role::System,
                    content: vec![ContentBlock::Text {
                        text: system.clone(),
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
                Some(system),
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

    // Build the effective TurnSink: always fan out through persistence (the
    // runner), and — when memory is configured — append a fail-open memory
    // sink. The persistence sink runs first so a memory outage cannot affect
    // persistence ordering.
    let mut sink = fluers_core::FanoutTurnSink::new().push(Box::new(runner));
    if let Some(memory_adapter) = memory_adapter.as_ref() {
        let memory_sink = fluers_memory::MemoryTurnSink::new(
            memory_adapter.clone(),
            merged
                .memory_user_id
                .clone()
                .unwrap_or_else(|| "default".into()),
        );
        sink = sink.push(Box::new(memory_sink));
        eprintln!("→ semantic memory enabled (per-turn extraction)");
    }
    let sink_ref: &dyn fluers_core::TurnSink = &sink;

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
            Some(sink_ref),
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
        Some(sink_ref),
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
        database_url: None,
        memory_url: None,
        memory_api_key: None,
        memory_user_id: None,
        memory_limit: 5,
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
    let sessions: Arc<dyn fluers_runtime::PersistenceAdapter> =
        if let Some(url) = args.database_url.as_ref() {
            Arc::new(
                fluers_postgres::PostgresAdapter::connect(url)
                    .await
                    .map_err(|e| {
                        let msg = redact_postgres_url(&e.to_string());
                        anyhow::anyhow!("postgres connect failed: {msg}")
                    })?,
            )
        } else {
            let sessions_dir = args.sessions_dir.clone().unwrap_or_else(|| {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home).join(".fluers").join("sessions")
            });
            Arc::new(fluers_runtime::JsonFileAdapter::new(sessions_dir))
        };
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

#[cfg(test)]
mod tests {
    use super::{redact_postgres_url, redact_url_password};

    #[test]
    fn redact_url_password_replaces_password() {
        let url = "postgres://alice:hunter2@localhost:5432/fluers";
        assert_eq!(
            redact_url_password(url),
            "postgres://alice:***@localhost:5432/fluers"
        );
    }

    #[test]
    fn redact_url_password_keeps_url_without_password() {
        assert_eq!(
            redact_url_password("postgres://localhost:5432/fluers"),
            "postgres://localhost:5432/fluers"
        );
        assert_eq!(
            redact_url_password("postgres://alice@localhost:5432/fluers"),
            "postgres://alice@localhost:5432/fluers"
        );
    }

    #[test]
    fn redact_postgres_url_redacts_embedded_url() {
        let msg = "error connecting: postgres://bob:secret@db.example.com:5432/prod";
        let redacted = redact_postgres_url(msg);
        assert!(
            redacted.contains("bob:***@db.example.com"),
            "got: {redacted}"
        );
        assert!(!redacted.contains("secret"), "password leaked: {redacted}");
    }

    #[test]
    fn redact_postgres_url_handles_postgresql_scheme() {
        let msg = "bad url: postgresql://u:p@h:5432/d?sslmode=disable done";
        let redacted = redact_postgres_url(msg);
        assert!(redacted.contains("u:***@h:5432"), "got: {redacted}");
        assert!(
            redacted.ends_with(" done"),
            "trailing text lost: {redacted}"
        );
    }

    #[test]
    fn redact_postgres_url_leaves_non_postgres_text_intact() {
        let msg = "some other error: no url here";
        assert_eq!(redact_postgres_url(msg), msg);
    }
}

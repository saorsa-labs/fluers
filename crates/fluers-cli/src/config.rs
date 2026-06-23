//! Optional TOML config file support.
//!
//! A `fluers.toml` (or path passed via `--config`) carries defaults so you
//! don't repeat flags. **Keys are never stored in the file** — only the *name*
//! of the env var to read the key from (`api_key_env`). CLI flags override
//! config-file values, which override built-in defaults.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// The config file schema.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// Provider backend: `openrouter` | `minimax` | `custom`.
    pub provider: Option<String>,
    /// Custom base URL (with `--provider custom`).
    pub base_url: Option<String>,
    /// Default model id, e.g. `minimax/minimax-m3`.
    pub model: Option<String>,
    /// Name of the env var holding the API key (never the key itself).
    pub api_key_env: Option<String>,
    /// Working directory the sandbox is rooted in.
    pub workdir: Option<PathBuf>,
    /// Maximum model turns.
    pub max_turns: Option<usize>,
    /// Per-turn provider deadline, in milliseconds.
    pub turn_timeout_ms: Option<u64>,
    /// How many tool calls may run in parallel within a turn.
    pub tool_concurrency: Option<usize>,
}

impl Config {
    /// Load from a path, if it exists. Returns `Default` if the file is absent.
    ///
    /// # Errors
    /// Returns an error if the file exists but can't be read or parsed.
    pub fn load(path: &PathBuf) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        let cfg: Self =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
        Ok(cfg)
    }
}

//! Model reasoning effort ("thinking") configuration.
//!
//! Mirrors `ThinkingLevel` from `pi-agent-core`.

use serde::{Deserialize, Serialize};

/// How much extended reasoning the model should emit.
///
/// `Off` disables it, `Minimal` keeps it brief, `High` asks for thorough
/// reasoning. The agent loop maps this onto provider-specific knobs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThinkingLevel {
    /// No extended reasoning.
    Off,
    /// Brief reasoning.
    Minimal,
    /// (default) Balanced reasoning.
    #[default]
    Medium,
    /// Thorough reasoning.
    High,
}

impl ThinkingLevel {
    /// Returns `true` when reasoning is enabled at all.
    #[must_use]
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

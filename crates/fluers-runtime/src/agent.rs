//! Agent definition — `define_agent` / `AgentProfile`.
//!
//! Mirrors Flue's `defineAgent` / `defineAgentProfile` from
//! `@flue/runtime`. Composes a model with tools, skills, a sandbox, and
//! instructions into a runnable [`Agent`].

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use fluers_core::{Model, ThinkingLevel, Tool};

use crate::env::Limits;
use crate::error::{RuntimeError, RuntimeResult};
use crate::sandbox::Sandbox;
use crate::skill::Skill;

/// A fully-resolved agent profile.
///
/// Built incrementally via [`AgentSpec`] and frozen into an [`Agent`] by
/// [`define_agent`]. This is the Rust shape of Flue's `AgentProfile`.
#[derive(Clone)]
pub struct AgentProfile {
    /// Which model to use.
    pub model: Model,
    /// System instructions.
    pub instructions: String,
    /// Tools the agent may call.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Skills (SKILL.md) injected into context.
    pub skills: Vec<Arc<Skill>>,
    /// The sandbox providing the session environment.
    pub sandbox: Arc<dyn Sandbox>,
    /// Reasoning effort.
    pub thinking: ThinkingLevel,
    /// Resource limits forwarded to the sandbox.
    pub limits: Limits,
}

/// A declarative, serializable agent specification.
///
/// Unlike [`AgentProfile`] (which holds trait objects), `AgentSpec` is plain
/// data: it can be read from a config file and resolved at startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSpec {
    /// Model id, e.g. `anthropic/claude-sonnet-4-6`.
    pub model: String,
    /// System instructions.
    #[serde(default)]
    pub instructions: String,
    /// Tool names to enable (resolved against the runtime registry).
    #[serde(default)]
    pub tools: Vec<String>,
    /// Skill directories or packaged-skill ids.
    #[serde(default)]
    pub skills: Vec<String>,
    /// Sandbox flavour: `local` | `virtual` | `container`.
    #[serde(default = "default_sandbox")]
    pub sandbox: String,
    /// Reasoning effort.
    #[serde(default)]
    pub thinking: ThinkingLevel,
}

fn default_sandbox() -> String {
    "local".to_string()
}

/// A runnable agent: a profile plus the resolved runtime handle.
pub struct Agent {
    /// The frozen profile.
    pub profile: AgentProfile,
}

/// Build an [`Agent`] from a closure that configures an [`AgentSpec`]-like
/// profile, mirroring Flue's `defineAgent(() => ({ ... }))`.
///
/// In MVP this resolves the sandbox and wires defaults; tool/skill resolution
/// from the registry arrives in a later phase.
pub async fn define_agent<F>(build: F) -> RuntimeResult<Agent>
where
    F: FnOnce(&mut AgentBuilder) -> RuntimeResult<()>,
{
    let mut b = AgentBuilder::default();
    build(&mut b)?;
    let profile = b.finish()?;
    Ok(Agent { profile })
}

/// Incremental builder used inside [`define_agent`].
#[derive(Default)]
pub struct AgentBuilder {
    /// Model.
    pub model: Option<Model>,
    /// Instructions.
    pub instructions: Option<String>,
    /// Tools.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Skills.
    pub skills: Vec<Arc<Skill>>,
    /// Sandbox.
    pub sandbox: Option<Arc<dyn Sandbox>>,
    /// Thinking level.
    pub thinking: ThinkingLevel,
}

impl AgentBuilder {
    /// Set the model.
    pub fn model(&mut self, model: impl Into<String>) -> &mut Self {
        self.model = Some(Model::new(model));
        self
    }

    /// Set the instructions.
    pub fn instructions(&mut self, text: impl Into<String>) -> &mut Self {
        self.instructions = Some(text.into());
        self
    }

    /// Add a tool.
    pub fn tool(&mut self, tool: Arc<dyn Tool>) -> &mut Self {
        self.tools.push(tool);
        self
    }

    /// Set the sandbox.
    pub fn sandbox(&mut self, sandbox: Arc<dyn Sandbox>) -> &mut Self {
        self.sandbox = Some(sandbox);
        self
    }

    fn finish(self) -> RuntimeResult<AgentProfile> {
        let model = self
            .model
            .ok_or_else(|| RuntimeError::InvalidSkill("agent requires a model".into()))?;
        let sandbox = self
            .sandbox
            .unwrap_or_else(|| Arc::new(crate::sandbox::local()));
        Ok(AgentProfile {
            model,
            instructions: self.instructions.unwrap_or_default(),
            tools: self.tools,
            skills: self.skills,
            sandbox,
            thinking: self.thinking,
            limits: Limits::default(),
        })
    }
}

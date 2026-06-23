//! # fluers-runtime
//!
//! The harness layer — Flue's own contribution on top of the agent core.
//!
//! This is where the bulk of a faithful port lives:
//!
//! - [`agent`] — `define_agent` / `AgentProfile` (model + tools + skills +
//!   sandbox + instructions), mirroring `@flue/runtime`'s `defineAgent`.
//! - [`env`] — the [`SessionEnv`](env::SessionEnv) trait: the filesystem +
//!   process abstraction that every sandbox backend implements.
//! - [`sandbox`] — virtual / local / remote sandbox backends.
//! - [`session`] — session management, event store, dispatch/invoke.
//! - [`runner`] — session-aware coordination and persistence after each turn.
//! - [`skill`] — `SKILL.md` parsing and packaged-skill directories.
//! - [`tool`] — the built-in tools: `read`, `write`, `edit`, `bash`, `grep`,
//!   `glob` (with Flue's byte/line limits).
//! - [`event`] — the event stream observers subscribe to.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Test code may use unwrap/expect/panic for clarity (project policy).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod agent;
pub mod env;
pub mod error;
pub mod event;
pub mod json_file_adapter;
pub mod local_env;
pub mod persistence;
pub mod runner;
pub mod sandbox;
pub mod session;
pub mod skill;
#[cfg(test)]
mod skill_tests;
pub mod tool;

pub use agent::{define_agent, Agent, AgentProfile, AgentSpec};
pub use env::{Limits, SessionEnv};
pub use error::{RuntimeError, RuntimeResult};
pub use event::{Event, EventSubscriber};
pub use json_file_adapter::JsonFileAdapter;
pub use local_env::LocalSessionEnv;
pub use persistence::PersistenceAdapter;
pub use runner::SessionRunner;
pub use sandbox::{local, Sandbox};
pub use session::{Session, SessionId, SessionState, SessionStore};
pub use skill::Skill;
pub use tool::mvp_tools;

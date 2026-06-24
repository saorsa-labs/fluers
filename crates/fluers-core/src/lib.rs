//! # fluers-core
//!
//! Core agent primitives and model abstractions for Fluers.
//!
//! This crate is the native Rust stand-in for Flue's two foundational
//! TypeScript dependencies:
//!
//! - [`@earendil-works/pi-agent-core`][piac] — the agent loop: messages,
//!   tool definitions, tool calls/results, thinking levels.
//! - [`@earendil-works/pi-ai`][piai] — the model/provider abstraction:
//!   `Model`, `ImageContent`, streaming, provider calls.
//!
//! Flue itself is a *harness* layer on top of these two. Neither has a
//! published Rust crate, so a faithful native Rust port must re-implement
//! this foundation first. Everything else (`fluers-runtime`, `fluers-cli`,
//! …) builds on the traits defined here.
//!
//! [piac]: https://github.com/earendil-works/pi-agent-core
//! [piai]: https://github.com/earendil-works/pi-ai

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Test code may use unwrap/expect/panic for clarity (project policy).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod error;
pub mod event;
pub mod message;
#[cfg(test)]
mod message_tests;
pub mod model;
pub mod runner;
pub mod thinking;
pub mod tool;

pub use error::{CoreError, Result};
pub use event::{EventSink, NullEventSink, RunEvent, RunHooks};
pub use message::{AgentMessage, ContentBlock, ImageContent, Role, SignalMessage};
pub use model::{Model, ModelProvider, ModelRequest, ModelResponse, StreamEvent};
pub use runner::{run_agent, run_agent_streaming, FanoutTurnSink, RunConfig, RunOutcome, TurnSink};
pub use thinking::ThinkingLevel;
pub use tool::{
    InvokeContext, JsonValue, ParameterSchema, Tool, ToolCall, ToolDefinition, ToolResult,
};

/// Re-export of [`serde_json::Value`] under a domain-friendly alias.
pub use serde_json::Value as Json;

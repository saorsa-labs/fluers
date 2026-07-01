//! # fluers-protocol
//!
//! Shared HTTP/SSE wire types for the Fluers server ([`fluers-server`]) and
//! the client SDK ([`fluers-sdk`]). Keeping these in a dedicated crate —
//! rather than in `fluers-core` — keeps the core crate focused on
//! model/tool/loop primitives and lets the server and SDK agree on a wire
//! format without a cyclic dependency.
//!
//! [`fluers-server`]: https://github.com/saorsa-labs/fluers
//! [`fluers-sdk`]: https://github.com/saorsa-labs/fluers

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A request to invoke an agent.
///
/// Sent as the JSON body of `POST /agents/:name/invoke` (and `/stream`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeRequest {
    /// The user prompt for this turn.
    pub prompt: String,
    /// An existing session id to resume. If `None`, a new session is created.
    #[serde(default)]
    pub session_id: Option<Uuid>,
}

/// The result of a completed (non-streaming) invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeResponse {
    /// The run id (one per invocation).
    pub run_id: Uuid,
    /// The session id (stable across resumptions).
    pub session_id: Uuid,
    /// The agent's final text output.
    pub output: String,
    /// How many model turns the run took.
    pub turns: usize,
}

/// The status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// The run is in progress.
    Running,
    /// The run completed successfully.
    Completed,
    /// The run failed.
    Failed,
}

/// A persisted record of a single run, retrievable via `GET /runs/:run_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    /// The run id.
    pub run_id: Uuid,
    /// The session id the run belongs to.
    pub session_id: Uuid,
    /// Current status.
    pub status: RunStatus,
    /// The agent's final output (empty while running).
    #[serde(default)]
    pub output: String,
    /// Turn count (0 while running).
    #[serde(default)]
    pub turns: usize,
}

/// A single Server-Sent Event emitted by `POST /agents/:name/stream`.
///
/// Mirrors the in-process `StreamEvent` but adds terminal
/// `Done`/`Error` variants carrying run/session bookkeeping, and is
/// serde-tagged for the SSE `data:` line.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SseEvent {
    /// A chunk of assistant text.
    TextDelta {
        /// The text fragment.
        text: String,
    },
    /// A chunk of reasoning/thinking text.
    ThinkingDelta {
        /// The thinking fragment.
        text: String,
    },
    /// The run completed.
    Done {
        /// The run id.
        run_id: Uuid,
        /// The session id.
        session_id: Uuid,
        /// Turn count.
        turns: usize,
    },
    /// The run failed.
    Error {
        /// The error message.
        message: String,
    },
}

impl SseEvent {
    /// Serialize as the payload of an SSE `data:` line (compact JSON).
    ///
    /// Returns `Err` only if the event is not serializable, which cannot
    /// happen for this type's fields.
    pub fn to_data_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// An entry in the `GET /agents` listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInfo {
    /// The agent's route name (the `:name` segment).
    pub name: String,
    /// A short human-readable description.
    #[serde(default)]
    pub description: String,
}

//! Run-lifecycle events and the [`EventSink`] seam.
//!
//! Mirrors Flue's `observe` / `FlueEventSubscriber`. The [`EventSink`] trait is
//! the dependency-direction seam that lets [`crate::runner::run_agent`] (in
//! `fluers-core`) emit events without depending on `fluers-runtime` (which holds
//! the concrete [`EventBus`] implementation).
//!
//! [`EventBus`]: https://docs.rs/fluers-runtime

use uuid::Uuid;

/// An observable run-lifecycle event.
///
/// Events carry **no content** — no prompt text, tool arguments, tool outputs,
/// file contents, or model response text. They carry only structural metadata
/// (session id, turn number, model id, tool name, call id, success flag) so they
/// are safe to export to any telemetry backend without leaking user data.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RunEvent {
    /// A session started (emitted once, before the first turn).
    SessionStarted {
        /// The session id.
        session: Uuid,
    },
    /// A model turn began.
    TurnStarted {
        /// The session id.
        session: Uuid,
        /// The 1-indexed turn number.
        turn: usize,
    },
    /// The model provider was invoked for this turn.
    ModelStarted {
        /// The session id.
        session: Uuid,
        /// The 1-indexed turn number.
        turn: usize,
        /// The model id (e.g. `"minimax/minimax-m3"`).
        model: String,
    },
    /// The model provider returned for this turn.
    ModelFinished {
        /// The session id.
        session: Uuid,
        /// The 1-indexed turn number.
        turn: usize,
    },
    /// A tool call was dispatched.
    ToolStarted {
        /// The session id.
        session: Uuid,
        /// The 1-indexed turn number.
        turn: usize,
        /// The tool name.
        tool: String,
        /// The tool call id (correlates with [`RunEvent::ToolFinished`]).
        call_id: String,
    },
    /// A tool call completed.
    ToolFinished {
        /// The session id.
        session: Uuid,
        /// The 1-indexed turn number.
        turn: usize,
        /// The tool name.
        tool: String,
        /// The tool call id (correlates with [`RunEvent::ToolStarted`]).
        call_id: String,
        /// Whether the tool succeeded.
        ok: bool,
    },
    /// A turn completed.
    TurnFinished {
        /// The session id.
        session: Uuid,
        /// The 1-indexed turn number.
        turn: usize,
    },
    /// The run failed.
    RunFailed {
        /// The session id.
        session: Uuid,
        /// A **bounded** error summary. Truncated to keep user content out of
        /// telemetry: provider/tool response bodies embedded in error strings
        /// are capped at [`ERROR_SUMMARY_MAX_CHARS`] characters. Use this for
        /// debugging signal, not as a source of truth about user data.
        error: String,
    },
}

/// Maximum number of characters retained in a [`RunEvent::RunFailed`] error
/// summary. Keeps provider/tool response text out of telemetry exports.
pub const ERROR_SUMMARY_MAX_CHARS: usize = 200;

/// Build a [`RunEvent::RunFailed`] with a bounded error summary, so user-facing
/// content embedded in error strings does not leak into telemetry.
#[must_use]
pub fn run_failed(session: Uuid, error: impl AsRef<str>) -> RunEvent {
    let error = error.as_ref();
    let summary = if error.len() > ERROR_SUMMARY_MAX_CHARS {
        format!("{}…(truncated)", &error[..ERROR_SUMMARY_MAX_CHARS])
    } else {
        error.to_string()
    };
    RunEvent::RunFailed {
        session,
        error: summary,
    }
}

/// A sink for [`RunEvent`]s — the dependency-direction seam between
/// `fluers-core` (which emits) and `fluers-runtime::EventBus` (which fans out).
///
/// Implementations must be **non-blocking**: the agent loop calls
/// [`emit`](EventSink::emit) inline, so a slow sink would stall the run. The
/// canonical implementation (`fluers_runtime::EventBus`) is backed by a
/// `tokio::broadcast` channel whose `send` is non-blocking.
///
/// This mirrors the [`crate::TurnSink`] pattern, which solved the same
/// dependency cycle for per-turn persistence.
pub trait EventSink: Send + Sync {
    /// Emit `event` to all subscribers. Non-blocking; returns immediately.
    fn emit(&self, event: RunEvent);
}

/// A no-op sink that discards every event. Used when no event bus is
/// configured.
#[derive(Default)]
pub struct NullEventSink;

impl EventSink for NullEventSink {
    fn emit(&self, _event: RunEvent) {}
}

/// Grouped run hooks passed to [`crate::runner::run_agent`] /
/// [`crate::runner::run_agent_streaming`].
///
/// Bundles the session id (for event correlation), the optional per-turn
/// [`crate::TurnSink`] (for persistence), and the optional [`crate::EventSink`] (for
/// observability). Replacing the old `on_turn: Option<&dyn TurnSink>` parameter
/// with `&RunHooks` keeps the function signature stable while leaving room for
/// future hooks without API churn.
///
/// Use [`RunHooks::default`] for a no-hooks run (no persistence, no events).
#[derive(Default)]
pub struct RunHooks<'a> {
    /// The session id (for event correlation). `None` when the caller has no
    /// session concept (e.g. a stateless one-shot run).
    pub session_id: Option<Uuid>,
    /// Optional per-turn sink (typically `SessionRunner` for persistence).
    pub turn_sink: Option<&'a dyn crate::TurnSink>,
    /// Optional event sink (typically `EventBus` for observability).
    pub event_sink: Option<&'a dyn EventSink>,
    /// Optional **tool policy hook** (Fae deviation; see the README). When set,
    /// the agent loop consults it before each tool call and denies/confirms
    /// per the returned [`crate::policy::PolicyVerdict`]. `None` (the default)
    /// means allow-all — existing consumers are unaffected.
    pub policy: Option<&'a dyn crate::policy::ToolPolicy>,
}

impl<'a> RunHooks<'a> {
    /// Create hooks with only a turn sink (the pre-4c calling convention).
    #[must_use]
    pub fn from_turn_sink(turn_sink: &'a dyn crate::TurnSink) -> Self {
        Self {
            session_id: None,
            turn_sink: Some(turn_sink),
            event_sink: None,
            policy: None,
        }
    }

    /// Conditionally emit an event, constructed only when both a session id
    /// and an event sink are configured. The closure receives the session id
    /// so callers don't need to branch on `Option`:
    ///
    /// ```ignore
    /// hooks.emit_event(|sid| RunEvent::TurnStarted { session: sid, turn: 1 });
    /// ```
    ///
    /// When `session_id` or `event_sink` is `None`, the closure is never
    /// called (zero cost) — no event is constructed or emitted.
    pub fn emit_event(&self, make: impl FnOnce(Uuid) -> RunEvent) {
        if let (Some(sid), Some(sink)) = (self.session_id, self.event_sink) {
            sink.emit(make(sid));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_failed_truncates_long_errors() {
        let long = "x".repeat(ERROR_SUMMARY_MAX_CHARS * 2);
        let event = run_failed(Uuid::nil(), &long);
        match event {
            RunEvent::RunFailed { error, .. } => {
                assert!(error.len() < long.len(), "error not truncated");
                assert!(error.ends_with("…(truncated)"));
            }
            _ => panic!("expected RunFailed"),
        }
    }

    #[test]
    fn run_failed_preserves_short_errors() {
        let event = run_failed(Uuid::nil(), "short error");
        match event {
            RunEvent::RunFailed { error, .. } => {
                assert_eq!(error, "short error");
            }
            _ => panic!("expected RunFailed"),
        }
    }

    #[test]
    fn run_failed_accepts_string_and_str() {
        // Both &str and String should work (impl AsRef<str>).
        let _ = run_failed(Uuid::nil(), "literal");
        let _ = run_failed(Uuid::nil(), String::from("owned"));
    }
}

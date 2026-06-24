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
        /// The tool call id (correlates with [`ToolFinished`]).
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
        /// The tool call id (correlates with [`ToolStarted`]).
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
        /// A short error summary (no full error chain / user data).
        error: String,
    },
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
/// [`TurnSink`] (for persistence), and the optional [`EventSink`] (for
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
}

impl<'a> RunHooks<'a> {
    /// Create hooks with only a turn sink (the pre-4c calling convention).
    #[must_use]
    pub fn from_turn_sink(turn_sink: &'a dyn crate::TurnSink) -> Self {
        Self {
            session_id: None,
            turn_sink: Some(turn_sink),
            event_sink: None,
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

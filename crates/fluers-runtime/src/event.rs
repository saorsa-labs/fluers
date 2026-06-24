//! The event stream.
//!
//! Mirrors Flue's `observe` / `FlueEventSubscriber` and `event-stream-store`.
//! Observers subscribe to a stream of [`Event`]s (= [`fluers_core::RunEvent`])
//! emitted as a session runs.
//!
//! The [`Event`] type and the [`EventSink`](fluers_core::EventSink) trait live
//! in `fluers-core` so that `run_agent` (which lives in core) can emit events
//! without creating a `core → runtime` dependency cycle. [`EventBus`] implements
//! that trait here.

use fluers_core::{EventSink, RunEvent};
use tokio::sync::broadcast;

/// Re-export of the core run-lifecycle event type.
///
/// Kept as a type alias for backward compatibility with code that imports
/// `fluers_runtime::Event`.
pub type Event = RunEvent;

/// A fan-out event bus backed by a bounded broadcast channel.
///
/// Each subscriber receives an independent [`broadcast::Receiver`] to drain on
/// its own task. Sending never blocks and no lock is held while receivers handle
/// events. If a receiver falls behind its bounded buffer, Tokio's broadcast
/// channel reports a lag error to that receiver on its next receive.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<RunEvent>,
}

impl EventBus {
    /// The default event buffer capacity.
    pub const DEFAULT_CAPACITY: usize = 256;

    /// Create a bus with the given bounded buffer capacity.
    ///
    /// Tokio broadcast channels require a non-zero capacity, so a capacity of
    /// zero is treated as one.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Create a bus with the default buffer capacity.
    #[must_use]
    pub fn new_default() -> Self {
        Self::new(Self::DEFAULT_CAPACITY)
    }

    /// Subscribe to future events.
    ///
    /// The caller owns the returned receiver and should drain it, typically on
    /// a dedicated task.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<RunEvent> {
        self.sender.subscribe()
    }

    /// Emit an event to all current subscribers.
    ///
    /// Returns `false` when there are no active receivers. Sending is
    /// non-blocking; slow receivers observe [`broadcast::error::RecvError::Lagged`]
    /// when they next receive.
    pub fn emit(&self, event: RunEvent) -> bool {
        self.sender.send(event).is_ok()
    }
}

/// `EventBus` satisfies the core [`EventSink`] trait so it can be passed to
/// [`run_agent`](fluers_core::run_agent) via [`RunHooks`](fluers_core::RunHooks).
impl EventSink for EventBus {
    fn emit(&self, event: RunEvent) {
        // Ignore the "no receivers" return — the agent loop doesn't care.
        let _ = EventBus::emit(self, event);
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new_default()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::EventBus;
    use fluers_core::{EventSink, RunEvent};
    use uuid::Uuid;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[tokio::test]
    async fn emit_delivers_to_subscriber() -> TestResult {
        let bus = EventBus::new(16);
        let session = Uuid::nil();
        let mut receiver = bus.subscribe();

        let handle = tokio::spawn(async move { receiver.recv().await.ok() });

        assert!(bus.emit(RunEvent::SessionStarted { session }));

        let received = timeout(TEST_TIMEOUT, handle).await??;
        assert!(matches!(
            received,
            Some(RunEvent::SessionStarted { session: s }) if s == session
        ));

        Ok(())
    }

    #[tokio::test]
    async fn event_sink_trait_delegates_to_emit() -> TestResult {
        let bus = EventBus::new(16);
        let session = Uuid::nil();
        let mut receiver = bus.subscribe();

        // Via the EventSink trait (not the inherent method).
        EventSink::emit(&bus, RunEvent::SessionStarted { session });

        let received = timeout(TEST_TIMEOUT, receiver.recv()).await?;
        assert!(matches!(
            received,
            Ok(RunEvent::SessionStarted { session: s }) if s == session
        ));
        Ok(())
    }

    #[tokio::test]
    async fn emit_with_no_receivers_returns_false() {
        let bus = EventBus::new(16);
        let session = Uuid::nil();

        assert!(!bus.emit(RunEvent::SessionStarted { session }));
    }

    #[tokio::test]
    async fn multiple_subscribers_each_receive() -> TestResult {
        let bus = EventBus::new(16);
        let session = Uuid::nil();
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();

        assert!(bus.emit(RunEvent::TurnStarted { session, turn: 1 }));

        let first_event = timeout(TEST_TIMEOUT, first.recv()).await?;
        let second_event = timeout(TEST_TIMEOUT, second.recv()).await?;

        assert!(matches!(
            first_event,
            Ok(RunEvent::TurnStarted { session: s, turn: 1 }) if s == session
        ));
        assert!(matches!(
            second_event,
            Ok(RunEvent::TurnStarted { session: s, turn: 1 }) if s == session
        ));

        Ok(())
    }
}

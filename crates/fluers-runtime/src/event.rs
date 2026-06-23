//! The event stream.
//!
//! Mirrors Flue's `observe` / `FlueEventSubscriber` and `event-stream-store`.
//! Observers subscribe to a stream of [`Event`]s emitted as a session runs.

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::session::SessionId;

/// One observable lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A session started.
    SessionStarted {
        /// Which session.
        session: SessionId,
    },
    /// A model turn began.
    TurnStarted {
        /// Which session.
        session: SessionId,
    },
    /// A tool was invoked.
    ToolInvoked {
        /// Which session.
        session: SessionId,
        /// Tool name.
        tool: String,
    },
    /// A turn completed.
    TurnFinished {
        /// Which session.
        session: SessionId,
    },
}

/// A fan-out event bus backed by a bounded broadcast channel.
///
/// Each subscriber receives an independent [`broadcast::Receiver`] to drain on
/// its own task. Sending never blocks and no lock is held while receivers handle
/// events. If a receiver falls behind its bounded buffer, Tokio's broadcast
/// channel reports a lag error to that receiver on its next receive.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<Event>,
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
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Emit an event to all current subscribers.
    ///
    /// Returns `false` when there are no active receivers. Sending is
    /// non-blocking; slow receivers observe [`broadcast::error::RecvError::Lagged`]
    /// when they next receive.
    pub fn emit(&self, event: Event) -> bool {
        self.sender.send(event).is_ok()
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

    use super::{Event, EventBus};
    use crate::session::SessionId;

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    #[tokio::test]
    async fn emit_delivers_to_subscriber() -> TestResult {
        let bus = EventBus::new(16);
        let expected_session = SessionId::nil();
        let mut receiver = bus.subscribe();

        let handle = tokio::spawn(async move { receiver.recv().await.ok() });

        assert!(bus.emit(Event::SessionStarted {
            session: expected_session,
        }));

        let received = timeout(TEST_TIMEOUT, handle).await??;
        assert!(matches!(
            received,
            Some(Event::SessionStarted { session }) if session == expected_session
        ));

        Ok(())
    }

    #[tokio::test]
    async fn emit_with_no_receivers_returns_false() {
        let bus = EventBus::new(16);
        let session = SessionId::nil();

        assert!(!bus.emit(Event::SessionStarted { session }));
    }

    #[tokio::test]
    async fn multiple_subscribers_each_receive() -> TestResult {
        let bus = EventBus::new(16);
        let expected_session = SessionId::nil();
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();

        assert!(bus.emit(Event::TurnStarted {
            session: expected_session,
        }));

        let first_event = timeout(TEST_TIMEOUT, first.recv()).await?;
        let second_event = timeout(TEST_TIMEOUT, second.recv()).await?;

        assert!(matches!(
            first_event,
            Ok(Event::TurnStarted { session }) if session == expected_session
        ));
        assert!(matches!(
            second_event,
            Ok(Event::TurnStarted { session }) if session == expected_session
        ));

        Ok(())
    }

    #[tokio::test]
    async fn no_deadlock_on_emit_from_receiver_task() -> TestResult {
        let bus = EventBus::new(16);
        let expected_session = SessionId::nil();
        let mut observer = bus.subscribe();
        let mut receiver = bus.subscribe();
        let nested_bus = bus.clone();

        let handle = tokio::spawn(async move {
            match receiver.recv().await {
                Ok(Event::TurnStarted { session }) => {
                    nested_bus.emit(Event::TurnFinished { session })
                }
                Ok(_) | Err(_) => false,
            }
        });

        assert!(bus.emit(Event::TurnStarted {
            session: expected_session,
        }));

        let first_event = timeout(TEST_TIMEOUT, observer.recv()).await?;
        assert!(matches!(
            first_event,
            Ok(Event::TurnStarted { session }) if session == expected_session
        ));

        let nested_emit_succeeded = timeout(TEST_TIMEOUT, handle).await??;
        assert!(nested_emit_succeeded);

        let second_event = timeout(TEST_TIMEOUT, observer.recv()).await?;
        assert!(matches!(
            second_event,
            Ok(Event::TurnFinished { session }) if session == expected_session
        ));

        Ok(())
    }
}

//! The event stream.
//!
//! Mirrors Flue's `observe` / `FlueEventSubscriber` and `event-stream-store`.
//! Observers subscribe to a stream of [`Event`]s emitted as a session runs.

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

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

/// A boxed event subscriber.
pub type EventSubscriber = Arc<dyn Fn(&Event) + Send + Sync>;

/// A small fan-out bus that forwards events to subscribers.
#[derive(Default)]
pub struct EventBus {
    subs: Mutex<Vec<EventSubscriber>>,
}

impl EventBus {
    /// Create an empty bus.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a subscriber.
    pub fn subscribe(&self, sub: EventSubscriber) {
        self.subs.lock().push(sub);
    }

    /// Emit an event to all subscribers.
    pub fn emit(&self, event: &Event) {
        for sub in self.subs.lock().iter() {
            (sub)(event);
        }
    }
}

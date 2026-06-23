//! # fluers-otel
//!
//! OpenTelemetry tracing adapter for Fluers.
//!
//! Mirrors `@flue/opentelemetry`: subscribes to the Fluers [`EventBus`]
//! and emits OTel spans/metrics for turns and tool invocations.
//!
//! MVP logs each event via `tracing`; the full OTLP exporter wiring lands in
//! MVP 4.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use fluers_runtime::EventBus;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

/// Spawn a tracing-backed subscriber for an [`EventBus`].
///
/// The spawned task drains its own event receiver and logs every event via
/// `tracing`. It exits when all event bus senders are dropped.
#[must_use]
pub fn tracing_subscriber(bus: &EventBus) -> JoinHandle<()> {
    let mut receiver = bus.subscribe();

    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Ok(event) => tracing::info!(event = ?event, "fluers.event"),
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "fluers.event_lagged");
                }
            }
        }
    })
}

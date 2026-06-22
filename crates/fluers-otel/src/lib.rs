//! # fluers-otel
//!
//! OpenTelemetry tracing adapter for Fluers.
//!
//! Mirrors `@flue/opentelemetry`: subscribes to the Fluers [`EventBus`]
//! and emits OTel spans/metrics for turns and tool invocations.
//!
//! MVP is a [`EventSubscriber`] that logs events via `tracing`; the full
//! OTLP exporter wiring lands in MVP 4.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::sync::Arc;

use fluers_runtime::EventSubscriber;

/// Build a tracing-backed event subscriber suitable for
/// `EventBus::subscribe`.
///
/// Until the OTLP exporter is wired, this simply logs each event.
#[must_use]
pub fn tracing_subscriber() -> EventSubscriber {
    Arc::new(|event: &fluers_runtime::Event| {
        tracing::info!(event = ?event, "fluers.event");
    })
}

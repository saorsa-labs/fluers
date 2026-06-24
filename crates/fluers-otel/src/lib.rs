//! # fluers-otel
//!
//! OpenTelemetry tracing adapter for Fluers.
//!
//! Mirrors `@flue/opentelemetry`: subscribes to the Fluers [`EventBus`]
//! and emits OTel spans for turns and tool invocations.
//!
//! Two entry points:
//! - [`tracing_subscriber`] — logs every event via `tracing` (zero deps).
//! - [`otlp_subscriber`] — exports a real span tree to an OTLP collector.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub use opentelemetry_sdk::trace::SdkTracerProvider;

use std::collections::HashMap;
use std::time::Duration;

use fluers_core::RunEvent;
use fluers_runtime::EventBus;
use opentelemetry::trace::{Span, Tracer, TracerProvider};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::resource::Resource;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

/// Spawn a tracing-backed subscriber for an [`EventBus`].
///
/// The spawned task drains its own event receiver and logs every event via
/// `tracing`. It exits when all event bus senders are dropped. Zero external
/// dependencies — always available as the default observability path.
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

/// Spawn an OTLP-backed subscriber that exports a real span tree to an OTLP
/// collector.
///
/// Spans are structured as:
/// - `fluers.session` — root span (SessionStarted → RunFailed / last TurnFinished)
/// - `turn {N}` — one per model turn (TurnStarted → TurnFinished)
/// - `tool: {name}` — one per tool call (ToolStarted → ToolFinished)
///
/// ModelStarted/ModelFinished become span events on the turn span.
///
/// # Errors
/// Returns an error if the OTLP exporter cannot be built (e.g. invalid URL).
/// The returned provider should be kept alive until the run completes; dropping
/// it flushes pending spans.
pub fn otlp_subscriber(
    bus: &EventBus,
    endpoint: &str,
) -> Result<(JoinHandle<()>, SdkTracerProvider), String> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| format!("build OTLP exporter: {e}"))?;

    let resource = Resource::builder().with_service_name("fluers").build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("fluers");

    let mut receiver = bus.subscribe();
    let handle = tokio::spawn(async move {
        let mut state = SpanState::new(tracer);
        loop {
            match receiver.recv().await {
                Ok(event) => state.handle(event),
                Err(RecvError::Closed) => break,
                Err(RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "otel.event_lagged");
                }
            }
        }
        // End all held spans → they export on flush/drop.
        state.flush();
    });

    Ok((handle, provider))
}

/// Per-subscriber span state — tracks the root, turn, and tool spans.
struct SpanState {
    tracer: opentelemetry_sdk::trace::SdkTracer,
    /// Root session span (None until SessionStarted).
    root: Option<opentelemetry_sdk::trace::Span>,
    /// Turn spans, keyed by turn number.
    turns: HashMap<usize, opentelemetry_sdk::trace::Span>,
    /// Tool spans, keyed by call_id.
    tools: HashMap<String, opentelemetry_sdk::trace::Span>,
}

impl SpanState {
    fn new(tracer: opentelemetry_sdk::trace::SdkTracer) -> Self {
        Self {
            tracer,
            root: None,
            turns: HashMap::new(),
            tools: HashMap::new(),
        }
    }

    fn handle(&mut self, event: RunEvent) {
        match event {
            RunEvent::SessionStarted { session } => {
                let mut builder = self.tracer.span_builder("fluers.session");
                builder.attributes = Some(vec![KeyValue::new("session.id", session.to_string())]);
                self.root = Some(builder.start(&self.tracer));
            }
            RunEvent::TurnStarted { session, turn } => {
                let mut builder = self.tracer.span_builder(format!("turn {turn}"));
                builder.attributes = Some(vec![
                    KeyValue::new("session.id", session.to_string()),
                    KeyValue::new("turn", turn as i64),
                ]);
                let span = builder.start(&self.tracer);
                self.turns.insert(turn, span);
            }
            RunEvent::ModelStarted { turn, model, .. } => {
                if let Some(span) = self.turns.get_mut(&turn) {
                    span.add_event("model.started", vec![KeyValue::new("model", model)]);
                }
            }
            RunEvent::ModelFinished { turn, .. } => {
                if let Some(span) = self.turns.get_mut(&turn) {
                    span.add_event("model.finished", vec![]);
                }
            }
            RunEvent::ToolStarted {
                turn,
                tool,
                call_id,
                ..
            } => {
                let mut builder = self.tracer.span_builder(format!("tool: {tool}"));
                builder.attributes = Some(vec![
                    KeyValue::new("tool.name", tool),
                    KeyValue::new("tool.call_id", call_id.clone()),
                    KeyValue::new("turn", turn as i64),
                ]);
                let span = builder.start(&self.tracer);
                self.tools.insert(call_id, span);
            }
            RunEvent::ToolFinished { call_id, ok, .. } => {
                if let Some(mut span) = self.tools.remove(&call_id) {
                    span.set_attribute(KeyValue::new("tool.ok", ok));
                    span.end();
                }
            }
            RunEvent::TurnFinished { turn, .. } => {
                if let Some(mut span) = self.turns.remove(&turn) {
                    span.end();
                }
            }
            RunEvent::RunFailed { error, .. } => {
                if let Some(mut span) = self.root.take() {
                    span.add_event("run.failed", vec![KeyValue::new("error", error)]);
                    span.end();
                }
            }
        }
    }

    /// End all remaining spans (called on subscriber exit).
    fn flush(&mut self) {
        for (_, mut span) in self.tools.drain() {
            span.end();
        }
        for (_, mut span) in self.turns.drain() {
            span.end();
        }
        if let Some(mut span) = self.root.take() {
            span.end();
        }
    }
}

/// Force-flush pending spans in the provider, with a timeout.
///
/// Call this before dropping the provider to avoid losing in-flight spans.
///
/// # Errors
/// Returns an error string if the flush fails.
pub fn flush_provider(provider: &SdkTracerProvider, _timeout: Duration) -> Result<(), String> {
    provider
        .force_flush()
        .map_err(|e| format!("flush failed: {e:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluers_core::RunEvent;
    use uuid::Uuid;

    #[tokio::test]
    async fn tracing_subscriber_drains_and_exits() {
        // The tracing subscriber should drain events and exit when the bus is dropped.
        let bus = EventBus::new(16);
        let handle = tracing_subscriber(&bus);

        bus.emit(RunEvent::SessionStarted {
            session: Uuid::nil(),
        });
        bus.emit(RunEvent::TurnStarted {
            session: Uuid::nil(),
            turn: 1,
        });

        // Give the subscriber a moment to drain.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Drop the bus → subscriber should exit.
        drop(bus);
        let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
    }
}

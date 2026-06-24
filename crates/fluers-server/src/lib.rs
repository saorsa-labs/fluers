//! # fluers-server
//!
//! An `axum` HTTP server that exposes Fluers agents over the Flue-compatible
//! HTTP surface. Mirrors `runtime/src/runtime/flue-app.ts` + `invoke.ts` at a
//! minimal level: synchronous invoke, streaming, agent listing, and run
//! records.
//!
//! ## Routes
//!
//! | Method | Path                       | Purpose                              |
//! |--------|----------------------------|--------------------------------------|
//! | GET    | `/health`                  | Liveness probe.                      |
//! | GET    | `/agents`                  | List registered agents.              |
//! | POST   | `/agents/:name/invoke`     | Run an agent, return the final text. |
//! | POST   | `/agents/:name/stream`     | Run an agent, stream SSE events.     |
//! | GET    | `/runs/:run_id`            | Fetch a run record.                  |
//!
//! Sessions are persisted via a [`PersistenceAdapter`]: pass a `session_id` in
//! [`InvokeRequest`] to resume, omit it to start a new session (the id is
//! echoed in the response).

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Test code may use unwrap/expect/panic for clarity (project policy).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json,
    },
    routing::{get, post},
    Router,
};
use futures::stream::Stream;
use futures::StreamExt;
use std::convert::Infallible;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use fluers_core::{
    message::{AgentMessage, ContentBlock, Role},
    run_agent, run_agent_streaming, StreamEvent,
};
use fluers_protocol::{AgentInfo, InvokeRequest, InvokeResponse, RunRecord, RunStatus, SseEvent};
use fluers_runtime::SessionRunner;

pub use state::{AgentHandle, ServerState};

/// Build the [`Router`] for the Fluers server, rooted at `/`.
///
/// The caller is responsible for binding it to an address (see [`serve`]).
pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/agents", get(list_agents))
        .route("/agents/{name}/invoke", post(invoke))
        .route("/agents/{name}/stream", post(stream))
        .route("/runs/{run_id}", get(get_run))
        .with_state(state)
}

/// Bind `router` to `addr` and serve until shutdown. Convenience entry point.
///
/// # Errors
/// Returns an error if the address cannot be bound.
pub async fn serve(addr: SocketAddr, state: Arc<ServerState>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("bind {addr} failed: {e}"))?;
    let app = router(state);
    tracing::info!("fluers dev server listening on http://{addr}");
    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("server error: {e}"))?;
    Ok(())
}

/// `GET /health` — liveness probe.
async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// `GET /agents` — list registered agents.
async fn list_agents(State(state): State<Arc<ServerState>>) -> Json<Vec<AgentInfo>> {
    let agents = state.agents.read();
    let infos = agents
        .iter()
        .map(|(name, h)| AgentInfo {
            name: name.clone(),
            description: h.description.clone(),
        })
        .collect();
    Json(infos)
}

/// `GET /runs/:run_id` — fetch a run record.
async fn get_run(
    State(state): State<Arc<ServerState>>,
    Path(run_id): Path<Uuid>,
) -> Result<Json<RunRecord>, (StatusCode, String)> {
    let runs = state.runs.read();
    match runs.get(&run_id) {
        Some(rec) => Ok(Json(rec.clone())),
        None => Err((StatusCode::NOT_FOUND, format!("run {run_id} not found"))),
    }
}

/// `POST /agents/:name/invoke` — run an agent synchronously, return final text.
async fn invoke(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
    Json(req): Json<InvokeRequest>,
) -> Result<Json<InvokeResponse>, (StatusCode, String)> {
    let handle = {
        let agents = state.agents.read();
        agents
            .get(&name)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("agent `{name}` not found")))?
    };
    let run_id = Uuid::new_v4();

    // Resolve / create the session and build the message history. The runner
    // is only the TurnSink; the loop owns the messages.
    let session_id = req.session_id.unwrap_or_else(Uuid::new_v4);
    let (mut messages, runner) = resolve_session(&state, session_id, &req.prompt, &handle)
        .await
        .map_err(map_err)?;

    mark_run(&state, run_id, session_id, RunStatus::Running);

    let model = fluers_core::Model::new(&handle.model.id);
    let cancel = tokio_util::sync::CancellationToken::new();
    let event_bus = fluers_runtime::EventBus::new_default();
    let hooks = fluers_core::RunHooks {
        session_id: Some(session_id),
        turn_sink: Some(runner.as_ref()),
        event_sink: Some(&event_bus),
    };
    let outcome = run_agent(
        handle.provider.as_ref(),
        &handle.tools,
        &mut messages,
        &model,
        &handle.config,
        &cancel,
        &hooks,
    )
    .await
    .map_err(map_err)?;

    mark_run(&state, run_id, session_id, RunStatus::Running);

    let resp = InvokeResponse {
        run_id,
        session_id,
        output: outcome.final_text.clone(),
        turns: outcome.turns,
    };
    state
        .update_run(run_id, |r| {
            r.status = RunStatus::Completed;
            r.output = outcome.final_text.clone();
            r.turns = outcome.turns;
        })
        .await;
    Ok(Json(resp))
}

/// `POST /agents/:name/stream` — run an agent, streaming SSE events.
async fn stream(
    State(state): State<Arc<ServerState>>,
    Path(name): Path<String>,
    Json(req): Json<InvokeRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, String)> {
    let handle = {
        let agents = state.agents.read();
        agents
            .get(&name)
            .cloned()
            .ok_or_else(|| (StatusCode::NOT_FOUND, format!("agent `{name}` not found")))?
    };
    let run_id = Uuid::new_v4();
    let session_id = req.session_id.unwrap_or_else(Uuid::new_v4);
    let (mut messages, runner) = resolve_session(&state, session_id, &req.prompt, &handle)
        .await
        .map_err(map_err)?;

    mark_run(&state, run_id, session_id, RunStatus::Running);

    // Bridge: run the streaming loop on a task, forwarding events to a channel.
    let (tx, rx) = mpsc::unbounded_channel::<SseEvent>();
    let provider = handle.provider.clone();
    let tools = handle.tools.clone();
    let config = handle.config.clone();
    let model = fluers_core::Model::new(&handle.model.id);
    let state2 = state.clone();
    let cancel = tokio_util::sync::CancellationToken::new();

    tokio::spawn(async move {
        let event_bus = fluers_runtime::EventBus::new_default();
        let hooks = fluers_core::RunHooks {
            session_id: Some(session_id),
            turn_sink: Some(runner.as_ref()),
            event_sink: Some(&event_bus),
        };
        let mut on_event = |ev: &StreamEvent| {
            let sse = match ev {
                StreamEvent::TextDelta(t) => SseEvent::TextDelta { text: t.clone() },
                StreamEvent::ThinkingDelta(t) => SseEvent::ThinkingDelta { text: t.clone() },
                // ToolCall / Done are consumed by the loop; not forwarded over SSE.
                _ => return,
            };
            // Best-effort forward; receiver drop just stops live updates.
            let _ = tx.send(sse);
        };
        let result = run_agent_streaming(
            provider.as_ref(),
            &tools,
            &mut messages,
            &model,
            &config,
            &cancel,
            &mut on_event,
            &hooks,
        )
        .await;
        match result {
            Ok(outcome) => {
                let _ = tx.send(SseEvent::Done {
                    run_id,
                    session_id,
                    turns: outcome.turns,
                });
                let output = outcome.final_text;
                let turns = outcome.turns;
                let mut runs = state2.runs.write();
                if let Some(r) = runs.get_mut(&run_id) {
                    r.status = RunStatus::Completed;
                    r.output = output;
                    r.turns = turns;
                }
            }
            Err(e) => {
                let _ = tx.send(SseEvent::Error {
                    message: e.to_string(),
                });
                let mut runs = state2.runs.write();
                if let Some(r) = runs.get_mut(&run_id) {
                    r.status = RunStatus::Failed;
                }
            }
        }
    });

    // Map the SseEvent stream to axum SSE `Event`s.
    let stream = UnboundedReceiverStream::new(rx).map(|ev| {
        let payload = ev.to_data_line().unwrap_or_else(|_| "{}".into());
        // Tag the event with its serde variant name so clients can switch on it.
        let kind = match &ev {
            SseEvent::TextDelta { .. } => "text_delta",
            SseEvent::ThinkingDelta { .. } => "thinking_delta",
            SseEvent::Done { .. } => "done",
            SseEvent::Error { .. } => "error",
        };
        Ok::<Event, Infallible>(Event::default().event(kind).data(payload))
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Resolve a session: load an existing one (resume) or seed a new one.
///
/// Returns the full message history (which the agent loop owns and mutates)
/// and a boxed [`SessionRunner`] to pass as the [`TurnSink`][fluers_core::TurnSink]
/// so the session is persisted after every turn.
///
/// On resume, persisted `model`/`max_turns` win; on a new session the agent
/// handle's values are used.
async fn resolve_session(
    state: &ServerState,
    session_id: Uuid,
    prompt: &str,
    handle: &AgentHandle,
) -> anyhow::Result<(Vec<AgentMessage>, Box<SessionRunner>)> {
    let adapter = state.sessions.clone();
    let default_model = handle.model.id.clone();
    let default_max_turns = handle.config.max_turns;
    let default_system = handle.system_prompt.clone();
    match SessionRunner::load(adapter.clone(), session_id).await? {
        Some(runner) => {
            // Resume: use the runner's persisted model/max_turns, append the
            // prompt as a fresh user turn.
            let _model_id = runner.model_id().to_string();
            let _max_turns = runner.max_turns();
            let mut messages = runner.messages();
            messages.push(AgentMessage {
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: prompt.into(),
                }],
            });
            Ok((messages, Box::new(runner)))
        }
        None => {
            // New session: seed system + user, build a fresh runner.
            let messages = vec![
                AgentMessage {
                    role: Role::System,
                    content: vec![ContentBlock::Text {
                        text: default_system.clone(),
                    }],
                },
                AgentMessage {
                    role: Role::User,
                    content: vec![ContentBlock::Text {
                        text: prompt.into(),
                    }],
                },
            ];
            let runner = SessionRunner::new(
                adapter,
                session_id,
                default_model,
                default_max_turns,
                Some(default_system),
            );
            Ok((messages, Box::new(runner)))
        }
    }
}

/// Record a run's initial state in the in-memory store.
fn mark_run(state: &ServerState, run_id: Uuid, session_id: Uuid, status: RunStatus) {
    let mut runs = state.runs.write();
    runs.insert(
        run_id,
        RunRecord {
            run_id,
            session_id,
            status,
            output: String::new(),
            turns: 0,
        },
    );
}

/// Map any error to a 500 response body.
fn map_err<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

/// Re-export for callers building an [`AgentHandle`].
pub use fluers_core::{ModelProvider, RunConfig, Tool};

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fluers_core::{
        message::{AgentMessage, ContentBlock, Role},
        model::StreamEventStream,
        ModelRequest, ModelResponse, StreamEvent,
    };
    use fluers_runtime::{JsonFileAdapter, PersistenceAdapter};

    /// A provider that streams a fixed text then completes.
    struct EchoStreamProvider {
        chunks: Vec<String>,
    }

    #[async_trait]
    impl ModelProvider for EchoStreamProvider {
        async fn invoke(
            &self,
            _req: ModelRequest,
        ) -> Result<ModelResponse, fluers_core::CoreError> {
            let text = self.chunks.join("");
            Ok(ModelResponse {
                messages: vec![AgentMessage {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text { text }],
                }],
            })
        }

        fn stream(&self, _req: ModelRequest) -> StreamEventStream {
            let chunks = self.chunks.clone();
            let s = async_stream::stream! {
                for c in chunks {
                    yield Ok(StreamEvent::TextDelta(c));
                }
                yield Ok(StreamEvent::Done);
            };
            Box::pin(s)
        }
    }

    /// Build a test `ServerState` with a single "echo" agent and a temp session dir.
    fn test_state() -> (Arc<ServerState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let adapter: Arc<dyn PersistenceAdapter> = Arc::new(JsonFileAdapter::new(dir.path()));
        let state = Arc::new(ServerState::new(adapter));
        let handle = AgentHandle {
            provider: Arc::new(EchoStreamProvider {
                chunks: vec!["hello".into(), " world".into()],
            }),
            model: fluers_core::Model::new("mock/echo"),
            tools: vec![],
            config: RunConfig {
                max_turns: 2,
                ..Default::default()
            },
            system_prompt: "test".into(),
            description: "echo agent".into(),
        };
        state.register("echo", handle);
        (state, dir)
    }

    #[tokio::test]
    async fn list_agents_returns_registered() {
        let (state, _dir) = test_state();
        let app = router(state);
        // Use oneshot request to avoid binding a real port.
        use tower::ServiceExt;
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/agents")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let infos: Vec<AgentInfo> = serde_json::from_slice(&body).unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].name, "echo");
    }

    #[tokio::test]
    async fn invoke_returns_final_output() {
        let (state, _dir) = test_state();
        let app = router(state);
        use tower::ServiceExt;
        let req = InvokeRequest {
            prompt: "hi".into(),
            session_id: None,
        };
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/agents/echo/invoke")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(serde_json::to_vec(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let resp: InvokeResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(resp.output, "hello world");
        assert_eq!(resp.turns, 1);
    }

    #[tokio::test]
    async fn get_run_after_invoke() {
        let (state, _dir) = test_state();
        let app = router(state.clone());
        use tower::ServiceExt;
        let req = InvokeRequest {
            prompt: "hi".into(),
            session_id: None,
        };
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/agents/echo/invoke")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(serde_json::to_vec(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let resp: InvokeResponse = serde_json::from_slice(&body).unwrap();

        // Now fetch the run record.
        let app2 = router(state);
        let resp = app2
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/runs/{}", resp.run_id))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 8192).await.unwrap();
        let rec: RunRecord = serde_json::from_slice(&body).unwrap();
        assert_eq!(rec.status, RunStatus::Completed);
        assert_eq!(rec.output, "hello world");
    }

    #[tokio::test]
    async fn stream_emits_text_then_done() {
        // The streaming endpoint spawns a task; collect its SSE body and check
        // it contains a text_delta then a done event.
        let (state, _dir) = test_state();
        let app = router(state);
        use tower::ServiceExt;
        let req = InvokeRequest {
            prompt: "hi".into(),
            session_id: None,
        };
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/agents/echo/stream")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(serde_json::to_vec(&req).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // Read the full SSE body (the task completes quickly for the mock).
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        // Should contain a text_delta event with "hello" and a done event.
        assert!(text.contains("text_delta"), "missing text_delta: {text}");
        assert!(text.contains("hello"), "missing first chunk: {text}");
        assert!(text.contains("done"), "missing done: {text}");
    }
}

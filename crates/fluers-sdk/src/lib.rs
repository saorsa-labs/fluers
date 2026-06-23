//! # fluers-sdk
//!
//! Client SDK for consuming deployed Fluers agents. Mirrors `@flue/sdk`.
//!
//! Talks to a [`fluers-server`] instance over HTTP: [`Client::invoke`] for
//! synchronous request/response, [`Client::stream`] for token-by-token SSE.
//!
//! [`fluers-server`]: https://github.com/saorsa-labs/fluers

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;

use futures::Stream;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A receipt returned by [`Client::invoke`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationReceipt {
    /// The run id.
    pub run_id: Uuid,
    /// The session id (stable across resumptions).
    pub session_id: Uuid,
    /// The agent's final text output.
    pub output: String,
    /// Turn count.
    #[serde(default)]
    pub turns: usize,
}

/// A request body sent to the server.
#[derive(Debug, Clone, Serialize)]
struct InvokeBody {
    /// The prompt.
    prompt: String,
    /// Optional session id to resume.
    session_id: Option<Uuid>,
}

/// A client for a deployed Fluers runtime.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    /// Create a client pointed at `base_url` (e.g. `http://localhost:3000`).
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// The base URL this client talks to.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Invoke a deployed agent by name, returning the final output.
    ///
    /// Pass `session_id = Some(id)` to resume an existing session.
    ///
    /// # Errors
    /// Returns an error if the HTTP request fails or the server returns a
    /// non-2xx status.
    pub async fn invoke(
        &self,
        agent: &str,
        prompt: &str,
        session_id: Option<Uuid>,
    ) -> anyhow::Result<InvocationReceipt> {
        let url = format!(
            "{}/agents/{}/invoke",
            self.base_url.trim_end_matches('/'),
            agent
        );
        let body = InvokeBody {
            prompt: prompt.into(),
            session_id,
        };
        let resp = self.http.post(&url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("invoke `{agent}` failed: {status}: {text}");
        }
        Ok(resp.json().await?)
    }

    /// Stream a run as SSE events. Returns a stream of [`fluers_protocol::SseEvent`].
    ///
    /// # Errors
    /// Returns an error if the HTTP request fails.
    pub async fn stream(
        &self,
        agent: &str,
        prompt: &str,
        session_id: Option<Uuid>,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<fluers_protocol::SseEvent>>> {
        let url = format!(
            "{}/agents/{}/stream",
            self.base_url.trim_end_matches('/'),
            agent
        );
        let body = InvokeBody {
            prompt: prompt.into(),
            session_id,
        };
        let resp = self.http.post(&url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("stream `{agent}` failed: {status}: {text}");
        }
        // Parse the SSE byte stream into `data:` lines, then deserialize each.
        use futures::StreamExt;
        let mut byte_stream = resp.bytes_stream();
        let (tx, rx) =
            tokio::sync::mpsc::unbounded_channel::<anyhow::Result<fluers_protocol::SseEvent>>();
        tokio::spawn(async move {
            let mut buf: Vec<u8> = Vec::new();
            while let Some(chunk_res) = byte_stream.next().await {
                let chunk = match chunk_res {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(Err(anyhow::anyhow!("stream transport error: {e}")));
                        return;
                    }
                };
                buf.extend_from_slice(&chunk);
                // Process complete SSE frames (terminated by a blank line).
                while let Some(end) = find_frame_end(&buf) {
                    let frame_bytes: Vec<u8> = buf.drain(..end).collect();
                    let Ok(frame) = String::from_utf8(frame_bytes) else {
                        continue;
                    };
                    for line in frame.lines() {
                        let line = line.strip_prefix('\r').unwrap_or(line);
                        if let Some(payload) = line
                            .strip_prefix("data: ")
                            .or_else(|| line.strip_prefix("data:"))
                        {
                            let payload = payload.trim();
                            if payload.is_empty() {
                                continue;
                            }
                            match serde_json::from_str::<fluers_protocol::SseEvent>(payload) {
                                Ok(ev) => {
                                    let is_done =
                                        matches!(ev, fluers_protocol::SseEvent::Done { .. })
                                            || matches!(
                                                ev,
                                                fluers_protocol::SseEvent::Error { .. }
                                            );
                                    if tx.send(Ok(ev)).is_err() {
                                        return;
                                    }
                                    if is_done {
                                        return;
                                    }
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                }
            }
        });
        Ok(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
    }

    /// Fetch a run record by id.
    ///
    /// # Errors
    /// Returns an error if the HTTP request fails.
    pub async fn get_run(&self, run_id: Uuid) -> anyhow::Result<fluers_protocol::RunRecord> {
        let url = format!("{}/runs/{}", self.base_url.trim_end_matches('/'), run_id);
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("get_run `{run_id}` failed: {status}: {text}");
        }
        Ok(resp.json().await?)
    }

    /// List registered agents.
    ///
    /// # Errors
    /// Returns an error if the HTTP request fails.
    pub async fn list_agents(&self) -> anyhow::Result<Vec<fluers_protocol::AgentInfo>> {
        let url = format!("{}/agents", self.base_url.trim_end_matches('/'));
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("list_agents failed: {status}: {text}");
        }
        Ok(resp.json().await?)
    }
}

/// Find the end of the next SSE frame (LF / CRLF / CR blank line).
fn find_frame_end(buf: &[u8]) -> Option<usize> {
    if let Some(p) = find_sub(buf, b"\r\n\r\n") {
        return Some(p + 4);
    }
    if let Some(p) = find_sub(buf, b"\n\n") {
        return Some(p + 2);
    }
    if let Some(p) = find_sub(buf, b"\r\r") {
        return Some(p + 2);
    }
    None
}

/// Find the first occurrence of `needle` in `hay`.
fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Build a system-prompt snippet from retrieved memories (a convenience for
/// callers wiring mem0-style long-term memory — see PORTING_PLAN MVP 4).
///
/// (Placeholder: the real `MemoryAdapter` trait lands in `fluers-memory` in
/// MVP 4. This helper exists so SDK consumers can format memory lists today.)
#[must_use]
pub fn format_memories(memories: &HashMap<String, String>) -> String {
    if memories.is_empty() {
        return String::new();
    }
    let mut out = String::from("User memories:\n");
    for (k, v) in memories {
        out.push_str(&format!("- {k}: {v}\n"));
    }
    out
}

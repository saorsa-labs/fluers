//! [`TurnSink`] wiring for the memory layer.
//!
//! - [`MemoryTurnSink`] — extracts the latest user→assistant **text** pair per
//!   turn and contributes it to a [`MemoryAdapter`]. **Fail-open**: mem0 errors
//!   are logged and swallowed, so a memory outage can never break session
//!   persistence or the agent run.
//! - [`FanoutMemorySink`] — convenience builder that composes an existing
//!   persistence sink (e.g. `SessionRunner`) with a [`MemoryTurnSink`] behind a
//!   [`fluers_core::FanoutTurnSink`].

use async_trait::async_trait;
use fluers_core::message::{AgentMessage, ContentBlock, Role};
use fluers_core::{FanoutTurnSink, TurnSink};
use std::sync::Arc;
use tracing::warn;

use crate::{MemoryAdapter, MemoryAddRequest, MemoryMessage};

/// A [`TurnSink`] that stores the latest user→assistant text pair into a
/// [`MemoryAdapter`].
///
/// ## What is stored
///
/// Only the **text** content of the most recent user message and the most
/// recent assistant message, for each turn. Deliberately excluded (privacy +
/// cost):
/// - tool call inputs / outputs,
/// - image / file blocks,
/// - thinking / reasoning blocks,
/// - the full transcript (only the new pair for this turn).
///
/// A turn with no user→assistant text pair (e.g. a tool-only turn) stores
/// nothing.
///
/// ## Failure mode
///
/// **Fail-open.** Any error from the adapter is logged via `tracing::warn!` and
/// swallowed; [`TurnSink::after_turn`] always returns `Ok(())`. This guarantees
/// a mem0 outage cannot break session persistence (which runs behind the same
/// [`FanoutTurnSink`]) or abort the agent run.
pub struct MemoryTurnSink {
    adapter: Arc<dyn MemoryAdapter>,
    user_id: String,
}

impl MemoryTurnSink {
    /// Create a sink that stores memories for `user_id` via `adapter`.
    #[must_use]
    pub fn new(adapter: Arc<dyn MemoryAdapter>, user_id: impl Into<String>) -> Self {
        Self {
            adapter,
            user_id: user_id.into(),
        }
    }
}

#[async_trait]
impl TurnSink for MemoryTurnSink {
    async fn after_turn(&self, _turn: usize, messages: &[AgentMessage]) -> fluers_core::Result<()> {
        match extract_latest_text_pair(messages) {
            Some(pair) => {
                let req = MemoryAddRequest {
                    user_id: self.user_id.clone(),
                    messages: pair,
                    metadata: None,
                };
                // Fail-open: never propagate memory errors.
                if let Err(e) = self.adapter.add(&req).await {
                    warn!("memory add failed (ignored): {e}");
                }
            }
            None => {
                // No text pair this turn — nothing to store.
            }
        }
        Ok(())
    }
}

/// Compose a persistence sink and a [`MemoryTurnSink`] behind a single
/// [`FanoutTurnSink`]. The persistence sink runs first, so memory failures
/// (which are fail-open anyway) can never affect persistence ordering.
///
/// Returns `None` if `memory` is `None` (no memory configured → just use the
/// persistence sink directly).
#[must_use]
pub fn fanout_with_memory(
    persistence: Box<dyn TurnSink>,
    memory: Option<MemoryTurnSink>,
) -> Option<FanoutTurnSink> {
    let mut fanout = FanoutTurnSink::new().push(persistence);
    if let Some(m) = memory {
        fanout = fanout.push(Box::new(m));
    }
    Some(fanout)
}

/// Convenience alias kept for API stability; identical to [`fanout_with_memory`].
///
/// Composing a persistence sink with a memory sink yields a [`FanoutTurnSink`]
/// that can be passed as `run_agent`'s single `Option<&dyn TurnSink>`.
#[must_use]
pub fn compose(
    persistence: Box<dyn TurnSink>,
    memory: Option<MemoryTurnSink>,
) -> Option<FanoutTurnSink> {
    fanout_with_memory(persistence, memory)
}

/// Extract the most recent assistant text message and the most recent user text
/// message that precedes it, returning them as a `[user, assistant]` pair. Only
/// `Text` content blocks are considered; tool/ image / thinking blocks are
/// skipped entirely. Returns `None` if either text message is missing.
fn extract_latest_text_pair(messages: &[AgentMessage]) -> Option<Vec<MemoryMessage>> {
    // Find the last assistant message with text content.
    let assistant_text = messages
        .iter()
        .rev()
        .find(|m| m.role == Role::Assistant)
        .and_then(|m| first_text(&m.content));
    let assistant_text = assistant_text?;
    // Find the last user message with text content that comes before that
    // assistant message.
    let assistant_idx = messages
        .iter()
        .rposition(|m| m.role == Role::Assistant && first_text(&m.content).is_some())?;
    let user_text = messages[..assistant_idx]
        .iter()
        .rev()
        .find(|m| m.role == Role::User)
        .and_then(|m| first_text(&m.content))?;
    Some(vec![
        MemoryMessage {
            role: "user".into(),
            content: user_text,
        },
        MemoryMessage {
            role: "assistant".into(),
            content: assistant_text,
        },
    ])
}

/// Return the text of the first `Text` block in `content`, if any.
fn first_text(content: &[ContentBlock]) -> Option<String> {
    for block in content {
        if let ContentBlock::Text { text } = block {
            return Some(text.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryMemoryAdapter;
    use fluers_core::message::{AgentMessage, ContentBlock, Role};
    use parking_lot::Mutex;
    use std::sync::Arc;

    fn text_msg(role: Role, text: &str) -> AgentMessage {
        AgentMessage {
            role,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    fn tool_result_msg() -> AgentMessage {
        AgentMessage {
            role: Role::Tool,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: "call-1".into(),
                content: serde_json::json!({"output": "file contents here"}),
            }],
        }
    }

    #[test]
    fn extract_pair_skips_tool_results_and_images() {
        let messages = vec![
            text_msg(Role::User, "what does the file say?"),
            tool_result_msg(),
            text_msg(Role::Assistant, "it says hello"),
        ];
        let pair = extract_latest_text_pair(&messages).expect("expected a pair");
        assert_eq!(pair.len(), 2);
        assert_eq!(pair[0].role, "user");
        assert_eq!(pair[0].content, "what does the file say?");
        assert_eq!(pair[1].role, "assistant");
        assert_eq!(pair[1].content, "it says hello");
    }

    #[test]
    fn extract_pair_none_without_assistant_text() {
        let messages = vec![text_msg(Role::User, "hello")];
        assert!(extract_latest_text_pair(&messages).is_none());
    }

    #[test]
    fn extract_pair_none_without_user_text() {
        let messages = vec![text_msg(Role::Assistant, "hi")];
        assert!(extract_latest_text_pair(&messages).is_none());
    }

    /// A persistence sink that records calls — used to assert fail-open
    /// ordering when composed behind a fanout.
    struct RecordingSink {
        calls: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl TurnSink for RecordingSink {
        async fn after_turn(
            &self,
            turn: usize,
            _messages: &[AgentMessage],
        ) -> fluers_core::Result<()> {
            self.calls.lock().push(turn);
            Ok(())
        }
    }

    #[tokio::test]
    async fn memory_sink_stores_only_text_pair_and_is_fail_open() {
        let adapter = Arc::new(InMemoryMemoryAdapter::new());
        let sink = MemoryTurnSink::new(adapter.clone(), "alice");

        let messages = vec![
            text_msg(Role::User, "I like vim keybindings"),
            text_msg(Role::Assistant, "Noted your preference for vim"),
        ];
        // The sink always returns Ok (fail-open).
        TurnSink::after_turn(&sink, 1, &messages)
            .await
            .expect("memory sink returned an error");

        // Verify exactly one pair was stored.
        let hits = adapter
            .search(&crate::MemorySearchRequest {
                user_id: "alice".into(),
                query: "vim".into(),
                top_k: 10,
            })
            .await
            .expect("search");
        assert_eq!(hits.len(), 1);
        // The stored memory must contain both the user and assistant text, and
        // must NOT contain tool/file output (there is none here, but the point
        // is only text blocks were contributed).
        assert!(hits[0].memory.contains("vim keybindings"));
        assert!(hits[0].memory.contains("Noted your preference"));
    }

    #[tokio::test]
    async fn memory_sink_fail_open_does_not_affect_persistence() {
        // A memory sink backed by an adapter that always fails. Composed behind
        // a fanout with a recording persistence sink, the persistence sink must
        // still run and the fanout must not error.
        struct FailingAdapter;
        #[async_trait]
        impl MemoryAdapter for FailingAdapter {
            async fn add(
                &self,
                _req: &MemoryAddRequest,
            ) -> crate::Result<crate::MemoryAddResponse> {
                Err(crate::MemoryError::Backend("simulated outage".into()))
            }
            async fn search(
                &self,
                _req: &crate::MemorySearchRequest,
            ) -> crate::Result<Vec<crate::Memory>> {
                Err(crate::MemoryError::Backend("simulated outage".into()))
            }
            async fn clear(&self, _user_id: &str) -> crate::Result<()> {
                Err(crate::MemoryError::Backend("simulated outage".into()))
            }
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let persistence: Box<dyn TurnSink> = Box::new(RecordingSink {
            calls: calls.clone(),
        });
        let memory = MemoryTurnSink::new(Arc::new(FailingAdapter), "alice");
        let fanout = compose(persistence, Some(memory)).expect("fanout built");

        let messages = vec![
            text_msg(Role::User, "hello"),
            text_msg(Role::Assistant, "hi"),
        ];
        // The fanout must succeed despite the memory adapter failing.
        TurnSink::after_turn(&fanout, 1, &messages)
            .await
            .expect("fanout failed despite fail-open memory sink");
        // The persistence sink must have run.
        assert_eq!(*calls.lock(), vec![1]);
    }
}

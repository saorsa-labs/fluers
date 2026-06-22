//! Conversation messages and content blocks.
//!
//! Mirrors `AgentMessage`, `ImageContent`, and Flue's `SignalMessage`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tool::ToolCall;
/// Who authored a message in the conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// System / developer instructions.
    System,
    /// The human user.
    User,
    /// The assistant / model.
    Assistant,
    /// A tool result returned to the model.
    Tool,
    /// A Flue "signal" event (lifecycle / framework-injected).
    Signal,
}

/// A single piece of message content.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// Plain text.
    Text {
        /// The text body.
        text: String,
    },
    /// An image attachment.
    Image {
        /// The image payload.
        image: ImageContent,
    },
    /// A tool call issued by the model.
    ToolUse {
        /// The call id, used to correlate the later result.
        id: String,
        /// The call itself.
        #[serde(flatten)]
        call: ToolCall,
    },
    /// A tool result returned to the model.
    ToolResult {
        /// The call id this result corresponds to.
        tool_use_id: String,
        /// Serialized result content.
        content: Value,
    },
}

/// An image attached to a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageContent {
    /// Media type, e.g. `image/png`.
    #[serde(rename = "media_type")]
    pub media_type: String,
    /// Raw image bytes.
    #[serde(with = "serde_base64")]
    pub data: Vec<u8>,
}

/// Base64 (de)serialization for [`ImageContent::data`], backed by the
/// audited `base64` crate rather than a hand-rolled codec.
mod serde_base64 {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        STANDARD.encode(v).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

/// A Flue "signal" message — a framework-injected lifecycle event that lives
/// in the message stream alongside user/assistant turns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalMessage {
    /// Always `signal`.
    pub role: Role,
    /// The signal type identifier.
    #[serde(rename = "type")]
    pub kind: String,
    /// Optional tag name for structured signals.
    #[serde(rename = "tag_name", skip_serializing_if = "Option::is_none")]
    pub tag_name: Option<String>,
    /// The signal body.
    pub content: String,
    /// Optional attributes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<std::collections::BTreeMap<String, String>>,
    /// When the signal fired.
    pub timestamp: DateTime<Utc>,
}

/// A full conversation message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMessage {
    /// Author role.
    pub role: Role,
    /// Content blocks (text / images / tool use / tool results).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<ContentBlock>,
}

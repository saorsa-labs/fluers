//! # fluers-sdk
//!
//! Client SDK for consuming deployed Fluers agents and workflows.
//!
//! Mirrors `@flue/sdk`. MVP exposes the [`Client`] shape and a typed
//! [`invoke`][Client::invoke] surface; the HTTP wire format is finalized
//! alongside `fluers-runtime`'s server (MVP 3).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use serde::{Deserialize, Serialize};

/// A receipt returned by an invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationReceipt {
    /// The run id.
    pub run_id: String,
    /// The session id.
    pub session_id: String,
}

/// A client for a deployed Fluers runtime.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    #[allow(dead_code)]
    http: reqwest::Client,
}

impl Client {
    /// Create a client pointed at `base_url`.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    /// The base URL this client talks to.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Invoke a deployed agent by name (MVP: returns a placeholder receipt).
    ///
    /// The full streaming + polling protocol lands in MVP 3.
    pub async fn invoke(
        &self,
        agent: &str,
        input: &serde_json::Value,
    ) -> anyhow::Result<InvocationReceipt> {
        // Placeholder until the server wire format is finalized.
        let _ = (agent, input);
        Ok(InvocationReceipt {
            run_id: "stub".into(),
            session_id: "stub".into(),
        })
    }
}

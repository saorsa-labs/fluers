//! The hosted-platform mem0 REST adapter.
//!
//! Implements [`crate::MemoryAdapter`] against the hosted mem0 platform API
//! (`api.mem0.ai`), speaking the exact wire contract the official Python
//! `MemoryClient` uses. See `docs/MVP4_MEMORY_DESIGN.md` for the sourced
//! contract.
//!
//! ## Wire contract (hosted platform)
//!
//! - **Base URL:** `https://api.mem0.ai` (configurable).
//! - **Auth:** `Authorization: Token {api_key}`.
//! - **Add:** `POST /v3/memories/add/` — `{"messages":[...], "user_id":"..."}`
//!   → `{"results":[{"id","memory","event"}, ...], "relations":[...]}`.
//! - **Search:** `POST /v3/memories/search/` —
//!   `{"query":"...", "filters":{"user_id":"..."}, "top_k":N}`
//!   → `{"results":[{"id","memory","score","user_id","metadata", ...}]}`.
//! - **Clear:** `DELETE /v1/memories/?user_id=...`.
//!
//! The self-hosted `server/` product (`/memories`, `/search`, bearer auth) uses
//! a different surface and is not supported by this adapter.

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde::{Deserialize, Serialize};

use crate::{
    Memory, MemoryAdapter, MemoryAddRequest, MemoryAddResponse, MemoryError, MemorySearchRequest,
    Result,
};

/// The default hosted-platform base URL.
pub const DEFAULT_HOST: &str = "https://api.mem0.ai";

/// A hosted-platform mem0 REST adapter.
///
/// Construct with [`Mem0RestAdapter::new`] (or [`Mem0RestAdapter::builder`] for
/// explicit options). The adapter holds a pooled [`reqwest::Client`]; it is
/// cheaply cloneable and safe to share across tasks.
#[derive(Clone)]
pub struct Mem0RestAdapter {
    base_url: String,
    client: reqwest::Client,
}

impl Mem0RestAdapter {
    /// Create an adapter for `base_url` authenticated with `api_key`.
    ///
    /// `api_key` is sent as `Authorization: Token {api_key}`. An empty `api_key`
    /// is permitted (for local/self-hosted setups that disable auth) but
    /// requests to the hosted platform will then fail with a 401.
    ///
    /// # Errors
    /// Returns [`MemoryError::Backend`] only if the HTTP client cannot be built
    /// (e.g. a TLS-backend failure).
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let mut headers = HeaderMap::new();
        let api_key = api_key.into();
        // Empty key is allowed; some self-hosted setups disable auth.
        if !api_key.is_empty() {
            let value = HeaderValue::from_str(&format!("Token {api_key}"))
                .map_err(|e| MemoryError::Backend(format!("invalid api key: {e}")))?;
            headers.insert(AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(30))
            .default_headers(headers)
            .build()
            .map_err(|e| MemoryError::Backend(format!("build http client: {e}")))?;
        Ok(Self { base_url, client })
    }
}

#[async_trait]
impl MemoryAdapter for Mem0RestAdapter {
    async fn add(&self, req: &MemoryAddRequest) -> Result<MemoryAddResponse> {
        #[derive(Serialize)]
        struct Body<'a> {
            messages: &'a [crate::MemoryMessage],
            user_id: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata: &'a Option<serde_json::Value>,
        }
        let body = Body {
            messages: &req.messages,
            user_id: &req.user_id,
            metadata: &req.metadata,
        };
        let resp = self
            .client
            .post(format!("{}/v3/memories/add/", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| redact_reqwest_err(&self.base_url, e))?;
        let payload = decode_json(resp, &self.base_url, "add").await?;
        // The platform returns {"results":[{"id",...}]}.
        let parsed: AddResponse = payload;
        Ok(MemoryAddResponse {
            ids: parsed.results.into_iter().map(|r| r.id).collect(),
        })
    }

    async fn search(&self, req: &MemorySearchRequest) -> Result<Vec<Memory>> {
        #[derive(Serialize)]
        struct Body<'a> {
            query: &'a str,
            filters: Filters<'a>,
            top_k: usize,
        }
        #[derive(Serialize)]
        struct Filters<'a> {
            user_id: &'a str,
        }
        let body = Body {
            query: req.query.trim(),
            filters: Filters {
                user_id: &req.user_id,
            },
            top_k: req.top_k,
        };
        let resp = self
            .client
            .post(format!("{}/v3/memories/search/", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| redact_reqwest_err(&self.base_url, e))?;
        let payload: SearchResponse = decode_json(resp, &self.base_url, "search").await?;
        Ok(payload.results)
    }

    async fn clear(&self, user_id: &str) -> Result<()> {
        let resp = self
            .client
            .delete(format!("{}/v1/memories/", self.base_url))
            .query(&[("user_id", user_id)])
            .send()
            .await
            .map_err(|e| redact_reqwest_err(&self.base_url, e))?;
        if !resp.status().is_success() {
            return Err(reqwest_status_err(resp, &self.base_url, "clear").await);
        }
        Ok(())
    }
}

/// Deserialize a success JSON body, or build a redacted error on failure.
async fn decode_json<T: for<'de> Deserialize<'de>>(
    resp: reqwest::Response,
    base_url: &str,
    op: &str,
) -> Result<T> {
    if !resp.status().is_success() {
        return Err(reqwest_status_err(resp, base_url, op).await);
    }
    resp.json::<T>()
        .await
        .map_err(|e| redact_reqwest_err(base_url, e))
}

/// Build a [`MemoryError`] from a non-2xx response, without leaking the base
/// URL (which may contain a password in self-hosted setups).
async fn reqwest_status_err(resp: reqwest::Response, _base_url: &str, op: &str) -> MemoryError {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    MemoryError::Backend(format!("mem0 {op} returned {status}: {body}"))
}

/// Wrap a transport error, redacting any URL embedded in the message.
fn redact_reqwest_err(base_url: &str, e: reqwest::Error) -> MemoryError {
    // reqwest errors can include the request URL; redact the base URL's
    // userinfo so a password never leaks. We only redact the known base URL
    // (not arbitrary URLs in the message) to keep this cheap.
    let msg = e.to_string();
    let redacted = redact_userinfo(base_url, &msg);
    MemoryError::Backend(format!("mem0 transport: {redacted}"))
}

/// Replace the password in `base_url` with `***` wherever `base_url` (or its
/// authority) appears in `msg`. If `base_url` has no userinfo, returns `msg`
/// unchanged.
fn redact_userinfo(base_url: &str, msg: &str) -> String {
    let Some((_scheme, rest)) = base_url.split_once("://") else {
        return msg.to_string();
    };
    let Some(at_idx) = rest.find('@') else {
        return msg.to_string(); // no userinfo → nothing to redact
    };
    let userinfo = &rest[..at_idx];
    let Some((user, _password)) = userinfo.split_once(':') else {
        return msg.to_string(); // no password
    };
    // Build the redacted base URL and replace occurrences in the message.
    let host_and_rest = &rest[at_idx..]; // includes the '@'
    let redacted_url = if let Some((scheme, _)) = base_url.split_once("://") {
        format!("{scheme}://{user}:***{host_and_rest}")
    } else {
        base_url.to_string()
    };
    msg.replace(base_url, &redacted_url)
}

// ── response shapes ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct AddResponse {
    #[serde(default)]
    results: Vec<AddResult>,
}

#[derive(Deserialize)]
struct AddResult {
    id: String,
}

/// The platform search response is `{"results":[{...}]}`. We deserialize into
/// [`crate::Memory`] directly; unknown fields are ignored.
#[derive(Deserialize)]
struct SearchResponse {
    #[serde(default)]
    results: Vec<Memory>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_userinfo_redacts_password_in_messages() {
        let base = "https://u:secret@host.example/mem0";
        let msg = "failed to connect to https://u:secret@host.example/mem0 (timeout)";
        let redacted = redact_userinfo(base, msg);
        assert!(!redacted.contains("secret"), "password leaked: {redacted}");
        assert!(redacted.contains("u:***@host.example"), "got: {redacted}");
    }

    #[test]
    fn redact_userinfo_noop_without_password() {
        let base = "https://api.mem0.ai";
        let msg = "timeout talking to https://api.mem0.ai";
        assert_eq!(redact_userinfo(base, msg), msg);
    }
}

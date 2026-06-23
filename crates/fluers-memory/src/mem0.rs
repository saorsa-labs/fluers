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
    /// Returns [`MemoryError::Backend`] if the base URL is empty/whitespace or
    /// the HTTP client cannot be built (e.g. a TLS-backend failure).
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Result<Self> {
        let base_url_in = base_url.into();
        if base_url_in.trim().is_empty() {
            return Err(MemoryError::Backend(
                "mem0 base URL must not be empty".into(),
            ));
        }
        let base_url = base_url_in.trim_end_matches('/').to_string();
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
/// URL or a large/unbounded error body. The response body is **truncated** to
/// a small cap so a backend that echoes request content cannot flood logs via
/// the fail-open `warn!` path.
async fn reqwest_status_err(resp: reqwest::Response, _base_url: &str, op: &str) -> MemoryError {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    // Cap the body in the error message so logs stay bounded.
    const BODY_CAP: usize = 512;
    let body = if body.len() > BODY_CAP {
        format!("{}…(truncated)", &body[..BODY_CAP])
    } else {
        body
    };
    MemoryError::Backend(format!("mem0 {op} returned {status}: {body}"))
}

/// Wrap a transport error, redacting any URL embedded in the message.
fn redact_reqwest_err(_base_url: &str, e: reqwest::Error) -> MemoryError {
    // reqwest errors include the **full request URL** (base + API path), not
    // just the adapter's base URL, so we redact userinfo from any URL found in
    // the message rather than string-replacing the bare base.
    let msg = redact_any_url_userinfo(&e.to_string());
    MemoryError::Backend(format!("mem0 transport: {msg}"))
}

/// Redact the password from **every** URL embedded in `msg`. Operates on any
/// `scheme://user:pass@host` occurrence (not just the adapter's base URL), so
/// full request URLs (`base + /v3/memories/add/`) emitted by reqwest are
/// covered too. Leaves the scheme, user, host, and path intact; only `pass` is
/// replaced with `***`.
fn redact_any_url_userinfo(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    let bytes = msg.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Look for the next "scheme://" start at or after `i`.
        if let Some(scheme_colon) = find_scheme(msg, i) {
            let after_scheme = scheme_colon + 3; // skip "://"
            out.push_str(&msg[i..after_scheme]);
            // The authority runs to the next path/query/fragment/whitespace.
            let auth_end = msg[after_scheme..]
                .find(|c: char| c == '/' || c == '?' || c == '#' || c.is_whitespace())
                .map(|idx| after_scheme + idx)
                .unwrap_or(msg.len());
            let authority = &msg[after_scheme..auth_end];
            if let Some(redacted) = redact_authority_password(authority) {
                out.push_str(&redacted);
            } else {
                out.push_str(authority);
            }
            i = auth_end;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Find the index of the `:` in a `scheme://` starting at or after `from`.
/// Returns `None` if no scheme marker is present. A scheme is
/// `[a-zA-Z][a-zA-Z0-9+.-]*` followed by `://`.
fn find_scheme(msg: &str, from: usize) -> Option<usize> {
    let rest = &msg[from..];
    let bytes = rest.as_bytes();
    let mut j = 0;
    while j < bytes.len() {
        let c = bytes[j] as char;
        if c.is_ascii_alphabetic()
            || (j > 0 && (c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.'))
        {
            j += 1;
            continue;
        }
        if c == ':' && j > 0 && rest[j..].starts_with("://") {
            return Some(from + j);
        }
        break;
    }
    None
}

/// Given an authority (`user:pass@host:port` or `host:port`), return a version
/// with the password replaced by `***`, or `None` if there is no userinfo or
/// no password.
fn redact_authority_password(authority: &str) -> Option<String> {
    let at_idx = authority.rfind('@')?;
    let userinfo = &authority[..at_idx];
    let host = &authority[at_idx..]; // includes '@'
    let (user, password) = userinfo.split_once(':')?;
    if password.is_empty() {
        return None;
    }
    Some(format!("{user}:***{host}"))
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
        // The red-team case: reqwest errors include the FULL request URL
        // (base + path), not just the bare base URL. Redaction must cover it.
        let msg = "error sending request for url (https://u:secret@host.example/mem0/v3/memories/add/): connection refused";
        let redacted = redact_any_url_userinfo(msg);
        assert!(!redacted.contains("secret"), "password leaked: {redacted}");
        assert!(
            redacted.contains("u:***@host.example/mem0/v3/memories/add/"),
            "got: {redacted}"
        );
    }

    #[test]
    fn redact_userinfo_handles_multiple_urls() {
        let msg = "redirected from http://a:pw1@h1/x to https://b:pw2@h2:8080/y";
        let redacted = redact_any_url_userinfo(msg);
        assert!(
            !redacted.contains("pw1") && !redacted.contains("pw2"),
            "leaked: {redacted}"
        );
        assert!(
            redacted.contains("a:***@h1") && redacted.contains("b:***@h2:8080"),
            "got: {redacted}"
        );
    }

    #[test]
    fn redact_userinfo_noop_without_password() {
        let msg = "timeout talking to https://api.mem0.ai/v3/memories/search/";
        assert_eq!(redact_any_url_userinfo(msg), msg);
    }
}

#[cfg(test)]
mod mock_tests {
    //! Mock-HTTP-server tests for [`Mem0RestAdapter`]. These start a local
    //! `wiremock` server and assert the adapter sends the exact paths, headers,
    //! and request bodies defined in the wire contract, then parses the
    //! responses correctly. No live mem0 required.
    use super::*;
    use crate::{MemoryAdapter, MemoryAddRequest, MemoryMessage, MemorySearchRequest};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn adapter(server: &MockServer) -> Mem0RestAdapter {
        Mem0RestAdapter::new(server.uri(), "test-key-123").expect("build adapter")
    }

    #[tokio::test]
    async fn add_posts_to_correct_path_with_token_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/memories/add/"))
            .and(header("authorization", "Token test-key-123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [{"id": "mem-1", "memory": "prefers dark mode", "event": "ADD"}],
                "relations": []
            })))
            .mount(&server)
            .await;

        let adapter = adapter(&server);
        let resp = adapter
            .add(&MemoryAddRequest {
                user_id: "alice".into(),
                messages: vec![MemoryMessage {
                    role: "user".into(),
                    content: "I like dark mode".into(),
                }],
                metadata: None,
            })
            .await
            .expect("add");
        assert_eq!(resp.ids, vec!["mem-1".to_string()]);
    }

    #[tokio::test]
    async fn search_posts_query_filters_and_top_k() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/memories/search/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [
                    {"id": "a", "memory": "prefers vim", "score": 0.9},
                    {"id": "b", "memory": "likes rust", "score": 0.7}
                ]
            })))
            .mount(&server)
            .await;

        let adapter = adapter(&server);
        let hits = adapter
            .search(&MemorySearchRequest {
                user_id: "alice".into(),
                query: "editor".into(),
                top_k: 5,
            })
            .await
            .expect("search");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].memory, "prefers vim");
        assert_eq!(hits[0].score, Some(0.9));

        // Assert the request body had the right shape.
        let req = &server.received_requests().await.expect("requests")[0];
        let body: serde_json::Value = serde_json::from_slice(&req.body).expect("body json");
        assert_eq!(body["query"], "editor");
        assert_eq!(body["top_k"], 5);
        assert_eq!(body["filters"]["user_id"], "alice");
    }

    #[tokio::test]
    async fn search_trims_query_before_sending() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/memories/search/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"results": []})),
            )
            .mount(&server)
            .await;

        let adapter = adapter(&server);
        let _ = adapter
            .search(&MemorySearchRequest {
                user_id: "alice".into(),
                query: "  padded query  ".into(),
                top_k: 3,
            })
            .await;

        let req = &server.received_requests().await.expect("requests")[0];
        let body: serde_json::Value = serde_json::from_slice(&req.body).expect("body json");
        assert_eq!(body["query"], "padded query", "query was not trimmed");
    }

    #[tokio::test]
    async fn clear_deletes_with_user_id_query_param() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/memories/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"message": "deleted"})),
            )
            .mount(&server)
            .await;

        let adapter = adapter(&server);
        adapter.clear("alice").await.expect("clear");

        let req = &server.received_requests().await.expect("requests")[0];
        assert!(req.url.query().is_some_and(|q| q.contains("user_id=alice")));
    }

    #[tokio::test]
    async fn non_2xx_returns_backend_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/memories/search/"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;

        let adapter = adapter(&server);
        let err = adapter
            .search(&MemorySearchRequest {
                user_id: "alice".into(),
                query: "x".into(),
                top_k: 1,
            })
            .await;
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("401"), "missing status: {msg}");
        assert!(msg.contains("unauthorized"), "missing body: {msg}");
    }

    #[tokio::test]
    async fn empty_api_key_omits_auth_header() {
        // An empty key is allowed; no Authorization header is sent.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/memories/add/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": []
            })))
            .mount(&server)
            .await;

        let adapter = Mem0RestAdapter::new(server.uri(), "").expect("build adapter");
        let _ = adapter
            .add(&MemoryAddRequest {
                user_id: "alice".into(),
                messages: vec![MemoryMessage {
                    role: "user".into(),
                    content: "hi".into(),
                }],
                metadata: None,
            })
            .await;
        let req = &server.received_requests().await.expect("requests")[0];
        assert!(!req.headers.contains_key("authorization"));
    }
}

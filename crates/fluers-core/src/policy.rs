//! A generic, content-aware **tool policy hook** consulted before a tool runs.
//!
//! This is a Fae-driven deviation from a faithful Flue port (see the README):
//! upstream Flue has no per-tool governance gate. The trait here names **no
//! Fae types** — it is a generic seam any consumer can implement. The default
//! agent run has no policy (allow-all), so existing fluers consumers are
//! unaffected.
//!
//! The [`crate::runner::run_agent`] loop consults the policy *before*
//! [`crate::tool::Tool::execute`]. A [`PolicyVerdict::Deny`] short-circuits the
//! call: the tool does not run, and a model-visible error result carrying the
//! reason is appended so the loop can continue (the model can recover, exactly
//! as with an unknown-tool or tool-error result).

use async_trait::async_trait;
use serde_json::Value;

use crate::tool::InvokeContext;

/// The decision a [`ToolPolicy`] returns for a single tool call.
#[derive(Debug, Clone)]
pub enum PolicyVerdict {
    /// Permit the call to execute.
    Allow,
    /// Refuse the call. The carried reason is surfaced to the model as a
    /// tool-result error; the tool is **not** executed and the loop continues.
    Deny(String),
    /// Require out-of-band confirmation before executing. A full confirmation
    /// UX is out of scope for the seam itself; callers that have no confirmation
    /// channel (e.g. a one-shot/headless run) may treat this as [`Allow`] and
    /// log the reason. The carried string explains what is being confirmed.
    ///
    /// [`Allow`]: PolicyVerdict::Allow
    Confirm(String),
}

/// A governance gate consulted **before** each tool call executes.
///
/// Generic by construction: it carries the tool name, the model-supplied input,
/// and the per-invocation [`InvokeContext`], and returns a [`PolicyVerdict`].
/// Fae's implementation composes its control-plane scope check,
/// `DamageControlPolicy`/`PathPolicy`, and the PII/egress membrane behind this
/// one trait — but none of that leaks into fluers.
#[async_trait]
pub trait ToolPolicy: Send + Sync {
    /// Decide whether `tool` may run with `input` under `ctx`.
    ///
    /// Implementations must be non-panicking and reasonably fast: the loop
    /// `await`s this inline before dispatching the tool.
    async fn check(&self, tool: &str, input: &Value, ctx: &InvokeContext) -> PolicyVerdict;
}

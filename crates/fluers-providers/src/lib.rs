//! # fluers-providers
//!
//! Model providers for Fluers. MVP ships a single [`OpenAiCompatibleProvider`]
//! that speaks the OpenAI Chat Completions wire format — which both
//! [OpenRouter] and [MiniMax] expose — so one implementation covers both
//! backends (and any other OpenAI-compatible endpoint).
//!
//! [OpenRouter]: https://openrouter.ai/docs/api-reference/overview
//! [MiniMax]: https://platform.minimaxi.com/

#![forbid(unsafe_code)]
#![warn(missing_docs)]
// Test code may use unwrap/expect/panic for clarity (project policy).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

pub mod openai_compatible;

pub use openai_compatible::{OpenAiCompatibleProvider, ProviderError, ProviderResult};

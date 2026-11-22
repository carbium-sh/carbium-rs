#![doc = include_str!("../README.md")]

// The crate names itself, so code written against the public API also builds inside it (the served test).
extern crate self as carbium;

pub mod account;
mod cq1;
pub mod dca;
mod error;
#[path = "gen.rs"]
mod generated;
mod http;
mod retry;
#[cfg(feature = "stream")]
pub mod stream;
pub mod tokens;
mod transport;
#[cfg(feature = "webhooks")]
pub mod webhooks;
#[cfg(feature = "ws")]
pub mod ws;

pub use account::CarbiumAccount;
pub use cq1::{Cq1Quote, Cq1RouteLeg, Cq1SwapInfo, REST_ROUTES};
pub use dca::CarbiumDca;
pub use error::{Cause, Code, Error, ErrorKind, Result, Secret, redact};
pub use generated::*;
pub use retry::{
    RetryContext, RetryOptions, RetryPlan, STANDARD, backoff_delay_ms, decide_retry, server_hint_ms,
};
pub use serde_json::{Number, Value};
pub use tokens::CarbiumTokens;
pub use transport::{
    Carbium, ClientOptions, DEFAULT_ENDPOINT, JsonRpcCall, LIMITS, Limits, VERSION,
};
#[cfg(feature = "webhooks")]
pub use webhooks::{verify_webhook, verify_webhook_signature};
#[cfg(feature = "ws")]
pub use ws::CarbiumSocket;
#[cfg(test)]
mod served_tests;
#[cfg(feature = "stream")]
pub use stream::CarbiumStream;

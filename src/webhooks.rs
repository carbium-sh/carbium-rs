//! Checks that a webhook delivery really came from Carbium. Deliveries are signed the Standard Webhooks way:
//! HMAC-SHA256 with the secret's base64 after `whsec_`, over `webhook-id.webhook-timestamp.body`.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use sha2::Sha256;

use crate::{Error, ErrorKind, Result, TransactionVersion};

/// One element of a webhook body: a finalized transaction in the getTransaction shape, a short form over 64 KiB, or
/// the test event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub slot: Number,
    #[serde(rename = "blockTime", default)]
    pub block_time: Option<Number>,
    #[serde(rename = "indexWithinBlock", default)]
    pub index_within_block: Option<Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<TransactionVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub err: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test: Option<bool>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Request headers a delivery arrived with. Names are matched without regard to case.
pub trait WebhookHeaders {
    /// The first value of the named header.
    fn header(&self, name: &str) -> Option<&str>;
}

impl WebhookHeaders for reqwest::header::HeaderMap {
    fn header(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(|v| v.to_str().ok())
    }
}

fn find<'a, K: AsRef<str> + 'a, V: AsRef<str> + 'a>(
    mut pairs: impl Iterator<Item = (&'a K, &'a V)>,
    name: &str,
) -> Option<&'a str> {
    pairs
        .find(|(k, _)| k.as_ref().eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_ref())
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for [(K, V)] {
    fn header(&self, name: &str) -> Option<&str> {
        find(self.iter().map(|(k, v)| (k, v)), name)
    }
}

impl<K: AsRef<str>, V: AsRef<str>, const N: usize> WebhookHeaders for [(K, V); N] {
    fn header(&self, name: &str) -> Option<&str> {
        self.as_slice().header(name)
    }
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for Vec<(K, V)> {
    fn header(&self, name: &str) -> Option<&str> {
        self.as_slice().header(name)
    }
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for HashMap<K, V> {
    fn header(&self, name: &str) -> Option<&str> {
        find(self.iter(), name)
    }
}

impl<K: AsRef<str>, V: AsRef<str>> WebhookHeaders for BTreeMap<K, V> {
    fn header(&self, name: &str) -> Option<&str> {
        find(self.iter(), name)
    }
}

/// Options for the check.
#[derive(Debug, Clone, Copy)]
pub struct VerifyOptions {
    /// How far webhook-timestamp may sit from the clock. Default 300 s.
    pub tolerance: Duration,
    /// The clock; None reads the system clock.
    pub now: Option<SystemTime>,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            tolerance: Duration::from_secs(300),
            now: None,
        }
    }
}

fn fail(reason: &str, message: &str) -> Error {
    Error::new(ErrorKind::Webhook, message).with_reason(reason)
}

/// Returns the body's bytes when one `v1,` entry of webhook-signature matches (two entries during a secret rotation),
/// and an error of kind Webhook otherwise, with reason missing_header, bad_timestamp, stale, bad_secret or no_match.
/// Pass the body exactly as received: a body parsed and encoded again does not verify. A timestamp or clock that
/// cannot be compared is refused as stale.
pub fn verify_webhook_signature<'a, H: WebhookHeaders + ?Sized>(
    raw_body: &'a [u8],
    headers: &H,
    secret: &str,
    options: &VerifyOptions,
) -> Result<&'a [u8]> {
    let (Some(id), Some(stamp), Some(signature)) = (
        headers.header("webhook-id"),
        headers.header("webhook-timestamp"),
        headers.header("webhook-signature"),
    ) else {
        return Err(fail(
            "missing_header",
            "webhook-id, webhook-timestamp and webhook-signature are all required",
        ));
    };
    if id.is_empty() || stamp.is_empty() || signature.is_empty() {
        return Err(fail(
            "missing_header",
            "webhook-id, webhook-timestamp and webhook-signature are all required",
        ));
    }
    if !stamp.bytes().all(|b| b.is_ascii_digit()) {
        return Err(fail(
            "bad_timestamp",
            "webhook-timestamp must be Unix seconds",
        ));
    }
    let now = options
        .now
        .unwrap_or_else(SystemTime::now)
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64());
    let distance = match (stamp.parse::<u64>(), now) {
        (Ok(at), Ok(now)) => (now - at as f64).abs(),
        _ => f64::NAN, // an overflowing timestamp or a clock before 1970 cannot be compared
    };
    if !matches!(
        distance.partial_cmp(&options.tolerance.as_secs_f64()),
        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
    ) {
        return Err(fail(
            "stale",
            &format!(
                "webhook-timestamp is more than {} s from the clock, or cannot be compared with it",
                options.tolerance.as_secs()
            ),
        ));
    }
    let Some(encoded) = secret.strip_prefix("whsec_").filter(|s| !s.is_empty()) else {
        return Err(fail(
            "bad_secret",
            "the secret must be the whsec_ value shown once when the webhook was created or rotated",
        ));
    };
    let key = BASE64
        .decode(encoded)
        .map_err(|_| fail("bad_secret", "the secret after whsec_ is not base64"))?;
    if key.is_empty() {
        return Err(fail("bad_secret", "the secret is empty"));
    }
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&key)
        .map_err(|_| fail("bad_secret", "the secret cannot key an HMAC"))?;
    mac.update(format!("{id}.{stamp}.").as_bytes());
    mac.update(raw_body);
    let matched = signature
        .split(' ')
        .filter_map(|entry| entry.strip_prefix("v1,"))
        .filter_map(|b64| BASE64.decode(b64).ok())
        .any(|candidate| mac.clone().verify_slice(&candidate).is_ok()); // constant-time compare
    if !matched {
        return Err(fail(
            "no_match",
            "no webhook-signature entry matches this body and secret",
        ));
    }
    Ok(raw_body)
}

/// Verifies a delivery (see [`verify_webhook_signature`]) and returns its events: a Carbium body is always a JSON
/// array. Adds reason bad_json.
pub fn verify_webhook<H: WebhookHeaders + ?Sized>(
    raw_body: &[u8],
    headers: &H,
    secret: &str,
    options: &VerifyOptions,
) -> Result<Vec<WebhookEvent>> {
    let body = verify_webhook_signature(raw_body, headers, secret, options)?;
    serde_json::from_slice(body).map_err(|_| {
        fail(
            "bad_json",
            "the body verified but is not a JSON array of events",
        )
    })
}

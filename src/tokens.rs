//! The token metadata API at tokens.carbium.io: its four public routes (search, lookup, batch and the image
//! address). Keyless; 300 requests a minute per client. Never retried.

use std::time::Duration;

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value, json};

use crate::error::{Error, ErrorKind, Result};
use crate::http::{Call, HttpAnswer, base_url, http_json, segment, url};
use crate::transport::http_client;

/// Most mints per batch call.
pub const BATCH_MAX_MINTS: usize = 500;

/// The public routes this client takes, as served.json names them.
pub const ROUTES: &[&str] = &[
    "GET /tokens",
    "GET /tokens/:mint",
    "POST /tokens/batch",
    "GET /img/:mint",
];

/// A token as tokens.carbium.io serves it. Timestamps are Unix milliseconds; supply is a decimal string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Token {
    pub mint: String,
    /// '' until the mint is resolved.
    pub symbol: String,
    /// '' until the mint is resolved.
    pub name: String,
    pub decimals: Number,
    pub logo_url: Option<String>,
    /// ok, broken, broken_perm, missing, unchecked or a later value, kept as sent.
    pub logo_status: String,
    #[serde(default)]
    pub is_token22: Option<bool>,
    pub daily_volume_usd: Option<Number>,
    /// Unix milliseconds.
    pub first_seen_at: Number,
    /// Unix milliseconds.
    pub last_seen_at: Number,
    pub supply: Option<String>,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
    pub metaplex_uri: Option<String>,
    pub update_authority: Option<String>,
    pub seller_fee_basis_points: Option<Number>,
    /// The Metaplex TokenStandard number.
    pub token_standard: Option<Number>,
    pub description: Option<String>,
    pub external_url: Option<String>,
    pub extensions: Option<Map<String, Value>>,
    /// Any JSON values, objects included.
    pub tags: Option<Vec<Value>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenSearch {
    pub items: Vec<Token>,
    pub page: Number,
    pub limit: Number,
    pub total: Option<Number>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenSearchOptions {
    /// 1 to 200, default 50.
    pub limit: Option<u32>,
    /// From 1.
    pub page: Option<u32>,
    pub include_token22: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenBatch {
    pub items: Vec<Token>,
    /// Mints the registry does not hold yet; they are queued for enrichment by the call itself.
    pub unknown: Vec<String>,
}

/// Options for [`CarbiumTokens::new`].
#[derive(Debug, Clone)]
pub struct TokensOptions {
    /// Default https://tokens.carbium.io.
    pub base_url: String,
    /// Default 30 s.
    pub timeout: Duration,
}

impl Default for TokensOptions {
    fn default() -> Self {
        Self {
            base_url: "https://tokens.carbium.io".into(),
            timeout: Duration::from_secs(30),
        }
    }
}

/// The token API client.
#[derive(Debug, Clone)]
pub struct CarbiumTokens {
    base_url: String,
    http: reqwest::Client,
    timeout: Duration,
}

fn tokens_error(a: &HttpAnswer, method: &str) -> Error {
    let hint = a.retry_after_ms().or_else(|| {
        a.field("retryAfterSec")
            .and_then(Value::as_f64)
            .map(|s| (s * 1000.0) as u64)
    });
    a.error(ErrorKind::Tokens, method, "error", &["message", "error"])
        .with_retryable(matches!(a.status, 429 | 503))
        .with_retry_after(hint)
}

impl CarbiumTokens {
    pub fn new(options: TokensOptions) -> Result<Self> {
        Ok(Self {
            base_url: base_url(&options.base_url)?,
            http: http_client()?,
            timeout: options.timeout,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn exchange(
        &self,
        name: &str,
        method: Method,
        path: &str,
        pairs: &[(&str, Option<String>)],
        body: Option<Value>,
    ) -> Result<HttpAnswer> {
        http_json(
            &self.http,
            method,
            &url(&self.base_url, path, pairs)?,
            &[],
            body.as_ref(),
            Call {
                kind: ErrorKind::Tokens,
                name,
                timeout: self.timeout,
            },
        )
        .await
    }

    async fn ok(
        &self,
        name: &str,
        method: Method,
        path: &str,
        pairs: &[(&str, Option<String>)],
        body: Option<Value>,
    ) -> Result<HttpAnswer> {
        let a = self.exchange(name, method, path, pairs, body).await?;
        if a.status >= 400 {
            return Err(tokens_error(&a, name));
        }
        Ok(a)
    }

    /// GET /tokens/:mint. None when the registry does not hold the mint (404 not_found).
    pub async fn lookup(&self, mint: &str) -> Result<Option<Token>> {
        let a = self
            .exchange(
                "lookup",
                Method::GET,
                &format!("/tokens/{}", segment(mint)),
                &[],
                None,
            )
            .await?;
        match a.status {
            404 => Ok(None),
            s if s >= 400 => Err(tokens_error(&a, "lookup")),
            _ => a.read(ErrorKind::Tokens, "lookup").map(Some),
        }
    }

    /// GET /tokens?q=: a ranked search, or the top listing by volume when q is empty.
    pub async fn search(&self, q: &str, options: &TokenSearchOptions) -> Result<TokenSearch> {
        let pairs = [
            ("q", Some(q.to_string())),
            ("limit", options.limit.map(|v| v.to_string())),
            ("page", options.page.map(|v| v.to_string())),
            (
                "include_token22",
                options.include_token22.map(|v| v.to_string()),
            ),
        ];
        self.ok("search", Method::GET, "/tokens", &pairs, None)
            .await?
            .read(ErrorKind::Tokens, "search")
    }

    /// POST /tokens/batch, 1 to 500 mints, checked before sending. Unknown mints come back in `unknown`.
    pub async fn batch(&self, mints: &[String]) -> Result<TokenBatch> {
        let max = BATCH_MAX_MINTS;
        if mints.is_empty() || mints.len() > max {
            return Err(Error::new(
                ErrorKind::Tokens,
                format!("batch takes 1 to {max} mints, not {}", mints.len()),
            )
            .with_method("batch"));
        }
        self.ok(
            "batch",
            Method::POST,
            "/tokens/batch",
            &[],
            Some(json!({ "mints": mints })),
        )
        .await?
        .read(ErrorKind::Tokens, "batch")
    }

    /// The URL of GET /img/:mint, for an image tag. No request is made.
    pub fn image_url(&self, mint: &str) -> String {
        format!("{}/img/{}", self.base_url, segment(mint))
    }
}

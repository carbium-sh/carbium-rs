//! The DCA API on the dex. Reads are keyless; the four build calls return an unsigned transaction for the owner's own
//! wallet; `send` relays bytes the owner's wallet signed. Server-side only: a browser page gets 403 forbidden_origin.
//! Never retried.

use std::fmt;
use std::time::Duration;

use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Map, Number, Value};

use crate::error::{Error, ErrorKind, Result, Secret};
use crate::http::{Call, base_url, http_json, segment, sensitive, url};
use crate::transport::http_client;

/// A u64 sent as a decimal string, as the API takes amounts.
fn decimal<S: Serializer>(v: &u64, s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

fn decimal_opt<S: Serializer>(v: &Option<u64>, s: S) -> std::result::Result<S::Ok, S::Error> {
    match v {
        Some(v) => decimal(v, s),
        None => s.serialize_none(),
    }
}

fn decimal_or_null<S: Serializer>(
    v: &Option<Option<u64>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    match v {
        Some(Some(v)) => decimal(v, s),
        _ => s.serialize_none(),
    }
}

/// The routes this client takes, as served.json names them.
pub const ROUTES: &[&str] = &[
    "POST /open",
    "POST /update",
    "POST /withdraw",
    "POST /close",
    "POST /send",
    "GET /quote",
    "GET /positions",
    "GET /position/{address}",
    "GET /history",
    "GET /health",
];

/// Error codes worth sending again later, whatever the HTTP status.
const RETRYABLE_CODES: [&str; 5] = [
    "rate_limited",
    "rate_limited_open",
    "rpc_unavailable",
    "quote_unavailable",
    "door_busy",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DcaFrequency {
    Hourly,
    Daily,
    Weekly,
    Biweekly,
    Monthly,
}

/// How much a DCA buys: per cycle, or a total split over the cycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DcaAmount {
    #[serde(rename = "amountPerCycle", serialize_with = "decimal")]
    PerCycle(u64),
    #[serde(rename = "totalAmount", serialize_with = "decimal")]
    Total(u64),
}

/// How often a DCA buys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DcaInterval {
    #[serde(rename = "intervalSecs")]
    Seconds(u64),
    #[serde(rename = "frequency")]
    Frequency(DcaFrequency),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaOpenRequest {
    /// The wallet that signs and pays.
    pub owner: String,
    pub input_mint: String,
    pub output_mint: String,
    /// 1 or more buys.
    pub cycles: u32,
    #[serde(flatten)]
    pub amount: DcaAmount,
    #[serde(flatten)]
    pub interval: DcaInterval,
    /// Default 50, cap 300.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_slippage_bps: Option<u32>,
    /// 0 or a Unix time up to 30 days ahead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_at: Option<u64>,
    /// 0 to 255, for several positions on one pair.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<u8>,
    /// A floor on the output per 1e6 of input; exclusive with max_price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "decimal_opt"
    )]
    pub min_out_e6: Option<u64>,
    /// A maximum price; exclusive with min_out_e6.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "decimal_opt"
    )]
    pub max_price: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DcaUpdateRequest {
    pub owner: String,
    pub position: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "decimal_opt"
    )]
    pub min_out_e6: Option<u64>,
    /// Some(None) clears the maximum price.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "decimal_or_null"
    )]
    pub max_price: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_slippage_bps: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DcaWithdrawRequest {
    pub owner: String,
    pub position: String,
    /// 0 or None withdraws the whole escrow.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "decimal_opt"
    )]
    pub amount: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DcaCloseRequest {
    pub owner: String,
    pub position: String,
}

/// A transaction this API built, signed by the owner's own wallet. The crate relays these bytes and never signs.
#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct DcaSendRequest {
    /// The signed transaction, base64.
    pub txn_base64: String,
    /// The build answer's txn_base64, when the wallet changed the message (priority fee, Lighthouse).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub built_txn_base64: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DcaQuoteRequest {
    pub input_mint: String,
    pub output_mint: String,
    pub amount: u64,
    pub slippage_bps: Option<u32>,
    pub owner: Option<String>,
}

/// A built, unsigned v0 transaction (no lookup tables) for the owner to sign and send.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcaBuild {
    pub ok: bool,
    pub txn_base64: String,
    pub blockhash: String,
    /// A decimal string, as the API sends every u64.
    pub last_valid_block_height: String,
    pub position: String,
    pub cost: Map<String, Value>,
    /// kind, owner, feePayer and instructions, with the build's own fields.
    pub summary: Map<String, Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaOpenBuild {
    pub authority: String,
    pub escrow_account: String,
    pub index: Number,
    pub plan: Map<String, Value>,
    #[serde(flatten)]
    pub build: DcaBuild,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcaSent {
    pub ok: bool,
    pub kind: String,
    pub signature: String,
    pub sent: bool,
    pub position: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaQuote {
    pub ok: bool,
    pub out: String,
    pub min_out: String,
    pub rate_e6: String,
    pub fee_bps: Number,
    pub in_decimals: Number,
    pub out_decimals: Number,
    /// The amount asked, echoed.
    pub amount: String,
    /// The amount's value in USDC base units (6 decimals).
    pub value_usdc: String,
    /// minAmountPerCycle (a decimal string, or null when the amount has no USDC value), minValueUsdc (a decimal
    /// string), minIntervalSecs, maxCycles, maxActive.
    pub limits: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<Map<String, Value>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaBuy {
    pub signature: String,
    pub cycle: Number,
    pub in_amount: String,
    pub out_amount: String,
    /// Unix seconds, as a decimal string.
    pub ts: String,
    pub slot: Option<Number>,
    pub opened_slot: Number,
    /// filled or missed.
    pub verdict: String,
    pub detail: Option<String>,
}

/// One position as the keeper sees it; the full field set rides in `fields`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaPosition {
    pub address: String,
    pub owner: String,
    pub in_mint: String,
    pub out_mint: String,
    pub cycles_total: Number,
    pub cycles_done: Number,
    pub interval_secs: Number,
    pub amount_per_cycle: String,
    pub active: bool,
    /// active, waiting, paused, not_served, empty, finished, closed or a later state.
    pub status: String,
    pub opened_slot: Option<Number>,
    pub opened_signature: Option<String>,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

/// A position with its fills.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaPositionDetail {
    pub owner_in: Option<String>,
    pub owner_out: Option<String>,
    pub fills: Vec<DcaBuy>,
    #[serde(flatten)]
    pub position: DcaPosition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaClosedRecord {
    pub address: String,
    pub closed: bool,
    pub closed_slot: Number,
    pub opened_slot: Number,
    /// None when the keeper never found the opening on the chain.
    pub opened_signature: Option<String>,
    pub fills: Vec<DcaBuy>,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

/// GET /position answers an open position with its fills, or a closed record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DcaPositionAnswer {
    Closed(DcaClosedRecord),
    Open(DcaPositionDetail),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcaPositions {
    pub ok: bool,
    pub owner: String,
    pub positions: Vec<DcaPosition>,
    pub next: Option<String>,
}

/// An ended DCA as /history lists it. Times are Unix seconds as decimal strings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcaHistoryOrder {
    pub address: String,
    pub in_mint: String,
    pub out_mint: String,
    pub cycles_done: Number,
    pub cycles_total: Number,
    pub opened_slot: Number,
    pub closed_slot: Number,
    /// finished, emptied or closed_early.
    pub ended: String,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcaHistory {
    pub ok: bool,
    pub owner: String,
    pub orders: Vec<DcaHistoryOrder>,
    pub next: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DcaHealth {
    pub ok: bool,
    pub keeper: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Options for [`CarbiumDca::new`].
#[derive(Clone)]
pub struct DcaOptions {
    /// Default https://dex.carbium.io/api/dca.
    pub base_url: String,
    /// Optional: a DCA API key made in the dex account menu raises the rate limits. Not a Carbium service or account key.
    pub api_key: Option<String>,
    /// Default 30 s.
    pub timeout: Duration,
}

impl Default for DcaOptions {
    fn default() -> Self {
        Self {
            base_url: "https://dex.carbium.io/api/dca".into(),
            api_key: None,
            timeout: Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for DcaOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DcaOptions")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.clone().map(Secret::new))
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// The DCA API client.
pub struct CarbiumDca {
    base_url: String,
    key: Option<(Secret, HeaderValue)>,
    http: reqwest::Client,
    timeout: Duration,
}

impl fmt::Debug for CarbiumDca {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarbiumDca")
            .field("base_url", &self.base_url)
            .field("api_key", &self.key.as_ref().map(|k| &k.0))
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl CarbiumDca {
    pub fn new(options: DcaOptions) -> Result<Self> {
        let key = match options.api_key.filter(|k| !k.is_empty()) {
            Some(k) => Some((Secret::new(k.clone()), sensitive(&k)?)),
            None => None,
        };
        Ok(Self {
            base_url: base_url(&options.base_url)?,
            key,
            http: http_client()?,
            timeout: options.timeout,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn call<T: DeserializeOwned>(
        &self,
        name: &str,
        method: Method,
        path: &str,
        pairs: &[(&str, Option<String>)],
        body: Option<Value>,
    ) -> Result<T> {
        let headers: Vec<(&'static str, HeaderValue)> = self
            .key
            .iter()
            .map(|(_, v)| ("x-api-key", v.clone()))
            .collect();
        let a = http_json(
            &self.http,
            method,
            &url(&self.base_url, path, pairs)?,
            &headers,
            body.as_ref(),
            Call {
                kind: ErrorKind::Dca,
                name,
                timeout: self.timeout,
            },
        )
        .await?;
        let ok = a
            .json
            .as_ref()
            .is_some_and(|j| j.is_object() && j.get("ok") != Some(&Value::Bool(false)));
        if a.status >= 400 || !ok {
            let busy = a
                .str_field("err")
                .is_some_and(|c| RETRYABLE_CODES.contains(&c));
            return Err(a
                .error(ErrorKind::Dca, name, "err", &["detail", "err"])
                .with_retryable(matches!(a.status, 429 | 502 | 503 | 504) || busy)
                .with_retry_after(a.retry_after_ms()));
        }
        a.read(ErrorKind::Dca, name)
    }

    async fn post<T: DeserializeOwned>(
        &self,
        name: &str,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T> {
        let body = serde_json::to_value(body)
            .map_err(|e| Error::new(ErrorKind::Request, e.to_string()))?;
        self.call(name, Method::POST, path, &[], Some(body)).await
    }

    /// GET /quote: a price for one buy, the fee rate and the owner's limits.
    pub async fn quote(&self, request: &DcaQuoteRequest) -> Result<DcaQuote> {
        let pairs = [
            ("inputMint", Some(request.input_mint.clone())),
            ("outputMint", Some(request.output_mint.clone())),
            ("amount", Some(request.amount.to_string())),
            ("slippageBps", request.slippage_bps.map(|v| v.to_string())),
            ("owner", request.owner.clone()),
        ];
        self.call("quote", Method::GET, "/quote", &pairs, None)
            .await
    }
    /// GET /positions: the owner's open positions, at most 100; pass `next` back as `after` for the rest.
    pub async fn positions(&self, owner: &str, after: Option<&str>) -> Result<DcaPositions> {
        self.call(
            "positions",
            Method::GET,
            "/positions",
            &[
                ("owner", Some(owner.into())),
                ("after", after.map(String::from)),
            ],
            None,
        )
        .await
    }
    /// GET /history: the owner's ended DCAs, newest close first.
    pub async fn history(&self, owner: &str, after: Option<&str>) -> Result<DcaHistory> {
        self.call(
            "history",
            Method::GET,
            "/history",
            &[
                ("owner", Some(owner.into())),
                ("after", after.map(String::from)),
            ],
            None,
        )
        .await
    }
    /// GET /position/{address}: one position with its fills, or its closed record.
    pub async fn position(
        &self,
        address: &str,
        opened_slot: Option<u64>,
    ) -> Result<DcaPositionAnswer> {
        self.call(
            "position",
            Method::GET,
            &format!("/position/{}", segment(address)),
            &[("openedSlot", opened_slot.map(|v| v.to_string()))],
            None,
        )
        .await
    }
    /// GET /health: the keeper's state. Never metered.
    pub async fn health(&self) -> Result<DcaHealth> {
        self.call("health", Method::GET, "/health", &[], None).await
    }
    /// POST /open: builds the unsigned open transaction.
    pub async fn open(&self, request: &DcaOpenRequest) -> Result<DcaOpenBuild> {
        self.post("open", "/open", request).await
    }
    /// POST /update: builds the unsigned pause, resume or new-floor transaction.
    pub async fn update(&self, request: &DcaUpdateRequest) -> Result<DcaBuild> {
        self.post("update", "/update", request).await
    }
    /// POST /withdraw: builds the unsigned withdraw transaction.
    pub async fn withdraw(&self, request: &DcaWithdrawRequest) -> Result<DcaBuild> {
        self.post("withdraw", "/withdraw", request).await
    }
    /// POST /close: builds the unsigned close transaction.
    pub async fn close(&self, request: &DcaCloseRequest) -> Result<DcaBuild> {
        self.post("close", "/close", request).await
    }
    /// POST /send: relays a transaction this API built, once the owner's wallet signed it. Sent once, never retried.
    pub async fn send(&self, request: &DcaSendRequest) -> Result<DcaSent> {
        self.post("send", "/send", request).await
    }
}

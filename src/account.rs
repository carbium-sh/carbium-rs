//! The account API at carbium.sh/v1 with an account API key: service keys, webhooks, usage and the account's records.
//! No call costs credits; 600 calls per 10 minutes per key. Never retried, because creates are not safe to repeat:
//! a 429 comes back with retry_after_ms.

use std::fmt;
use std::time::Duration;

use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value, json};

use crate::error::{Error, ErrorKind, Result, Secret};
use crate::http::{Call, base_url, http_json, path_and_query, segment, sensitive};
use crate::transport::{http_client, is_key};

/// A plane a service key may reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyScope {
    Rpc,
    Ws,
    Grpc,
    Quote,
    Das,
}

/// A per-method-class ceiling on a service key. `method_class` is one of o1_cached, bounded_read, bounded_slot,
/// write_send, compute, bounded_disk, range, scan, gpa.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MethodCap {
    pub method_class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rps_ceiling: Option<Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceKey {
    pub key_id: u64,
    pub prefix: Option<String>,
    pub name: Option<String>,
    pub tier: String,
    pub scope: Vec<String>,
    /// active, suspended, revoked, disabled or a later state.
    pub status: String,
    pub per_method_caps: Vec<MethodCap>,
    pub max_connections: Option<Number>,
    pub max_inflight: Option<Number>,
    pub ip_allow: Vec<String>,
    pub created_at: Option<String>,
    pub rotated_at: Option<String>,
    pub revoked_at: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The raw key is shown once: store it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreatedKey {
    pub key_id: u64,
    pub prefix: String,
    pub raw_key: Secret,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RotatedKey {
    pub prefix: String,
    pub raw_key: Secret,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Webhook {
    pub webhook_id: u64,
    pub key_id: Option<u64>,
    pub name: Option<String>,
    pub url: String,
    /// active, paused or disabled.
    pub status: String,
    pub status_reason: Option<String>,
    /// all, success or failed.
    pub tx_status: String,
    pub address_count: Number,
    /// Present on a single read only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<String>>,
    pub secret_hint: String,
    pub has_auth_header: bool,
    pub consecutive_failures: Number,
    pub last_success_at: Option<String>,
    pub last_failure_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Which transactions a webhook delivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TxStatus {
    All,
    Success,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct CreateWebhookRequest {
    /// The service key whose credits pay the deliveries.
    pub key_id: u64,
    /// https on port 443 at a public host name, at most 2048 characters.
    pub url: String,
    /// Base58 addresses to watch.
    pub addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_status: Option<TxStatus>,
    /// Sent as the Authorization header of every delivery. At most 512 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_header: Option<Secret>,
}

/// Paused or active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebhookState {
    Paused,
    Active,
}

#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub struct UpdateWebhookRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_status: Option<TxStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_header: Option<Secret>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<WebhookState>,
}

/// The secret is shown once: store it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreatedWebhook {
    pub webhook: Webhook,
    pub secret: Secret,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RotatedWebhookSecret {
    pub secret: Secret,
    pub secret_hint: String,
    /// Until when the previous secret still signs, about a day.
    pub previous_valid_until: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookDelivery {
    pub ts: Number,
    pub event_id: String,
    pub slot: Number,
    /// delivery or test.
    pub kind: String,
    pub attempt: Number,
    /// delivered, failed, dropped or refused.
    pub outcome: String,
    pub http_status: Option<u16>,
    pub reason: String,
    pub latency_ms: Option<Number>,
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookTestResult {
    pub event_id: String,
    pub ts: Number,
    /// delivered, failed or refused: read this, not the HTTP status.
    pub outcome: String,
    pub http_status: Option<u16>,
    pub reason: String,
    pub latency_ms: Option<Number>,
    /// credits, rate, plan, key, secret, host or refused.
    pub refusal: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DailyUsageRow {
    pub utc_day: String,
    pub method: String,
    pub method_class: String,
    pub call_count: Number,
    pub credits_charged: Number,
    /// With group=key; None for calls made before a key was rotated or by a key since deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyUsage {
    pub key_id: u64,
    pub credits: Number,
    pub calls: Number,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeysUsage {
    pub period_start: String,
    pub period_end: String,
    pub account_credits: Option<Number>,
    pub keys: Vec<KeyUsage>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyLoad {
    pub key_id: u64,
    pub rps_current: Number,
    pub class_rps: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HourlyUsageRow {
    pub hour: String,
    pub key_id: Option<u64>,
    pub method_class: String,
    pub calls: Number,
    pub credits_charged: Number,
    pub errors: Number,
    pub resp_bytes: Number,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyRow {
    pub hour: String,
    pub key_id: Option<u64>,
    pub method_class: String,
    pub calls: Number,
    pub p50_ms: Option<Number>,
    pub p95_ms: Option<Number>,
    pub p99_ms: Option<Number>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub account_id: u64,
    pub email: Option<String>,
    pub wallet: Option<String>,
    pub status: String,
    pub created_at: String,
    pub tier: String,
    #[serde(default)]
    pub term: Value,
    #[serde(default)]
    pub payments: Value,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Balance {
    pub granted: Number,
    pub used: Number,
    pub remaining: Number,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Invoice {
    pub period_start: Option<String>,
    pub period_end: Option<String>,
    pub granted: Number,
    pub used: Number,
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payment {
    pub id: u64,
    pub amount_minor: Number,
    pub currency: String,
    pub status: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub months: Option<Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credits_granted: Option<Number>,
    pub reference: Option<String>,
    pub paid_at: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Every module with its plans and what the account holds, one object per module.
pub type Modules = Vec<Map<String, Value>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CryptoIntent {
    /// open, paid, held or expired.
    pub status: String,
    pub refused: Vec<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountApiKey {
    pub api_key_id: u64,
    pub name: Option<String>,
    pub prefix: String,
    /// full or read.
    pub access: String,
    pub scopes: Vec<String>,
    pub ip_allow: Vec<String>,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountApiKeyActivity {
    pub ts: Number,
    pub action: String,
    pub request_id: Option<String>,
    pub detail: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Applied {
    pub applied: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Deleted {
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyName {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyScopes {
    pub key_id: u64,
    pub scope: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyAllowlist {
    pub key_id: u64,
    pub ip_allow: Vec<String>,
}

/// The limits a key holds after set_limits; None where the plan's own limit applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyLimitsSet {
    pub max_connections: Option<u32>,
    pub max_inflight: Option<u32>,
}

/// Connections and in-flight calls, 0 to 10000 each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyLimits {
    pub max_connections: u32,
    pub max_inflight: u32,
}

/// A time range and key filter for the usage reads; daily takes `group: Some("key")` too.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UsageQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub key_id: Option<u64>,
    pub group: Option<String>,
}

/// A page of a log: at most `limit` lines before `before` (a line's cursor or a Unix-ms time).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PageQuery {
    pub limit: Option<u32>,
    pub before: Option<String>,
}

/// Options for [`CarbiumAccount::new`].
#[derive(Clone)]
pub struct AccountOptions {
    /// An account API key, c_acct_… (made on the dashboard). Never a service key.
    pub api_key: String,
    /// Default https://carbium.sh.
    pub base_url: String,
    /// Default 30 s.
    pub timeout: Duration,
}

impl AccountOptions {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://carbium.sh".into(),
            timeout: Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for AccountOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccountOptions")
            .field("api_key", &Secret::new(self.api_key.clone()))
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// The account API client.
pub struct CarbiumAccount {
    base_url: String,
    key: Secret,
    bearer: HeaderValue,
    http: reqwest::Client,
    timeout: Duration,
}

impl fmt::Debug for CarbiumAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarbiumAccount")
            .field("base_url", &self.base_url)
            .field("api_key", &self.key)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl CarbiumAccount {
    /// Refuses anything but an account key, and a key in the base URL.
    pub fn new(options: AccountOptions) -> Result<Self> {
        if !is_key(&options.api_key, "c_acct_") {
            return Err(Error::new(
                ErrorKind::Auth,
                "api_key must be an account API key (c_acct_…) from the dashboard; a service key (c_serv_) is for services.carbium.sh",
            ));
        }
        let bearer = sensitive(&format!("Bearer {}", options.api_key))?;
        Ok(Self {
            base_url: base_url(&options.base_url)?,
            key: Secret::new(options.api_key),
            bearer,
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
        body: Option<Value>,
        timeout: Option<Duration>,
    ) -> Result<T> {
        let a = http_json(
            &self.http,
            method,
            &format!("{}{path}", self.base_url),
            &[("authorization", self.bearer.clone())],
            body.as_ref(),
            Call {
                kind: ErrorKind::Account,
                name,
                timeout: timeout.unwrap_or(self.timeout),
            },
        )
        .await?;
        if a.status >= 400 {
            return Err(a
                .error(ErrorKind::Account, name, "error", &["message"])
                .with_retryable(a.status == 503)
                .with_retry_after(a.retry_after_ms())
                .with_request_id(a.request_id()));
        }
        a.read(ErrorKind::Account, name)
    }

    fn path(&self, path: &str, pairs: &[(&str, Option<String>)]) -> Result<String> {
        Ok(path_and_query(path, pairs))
    }

    /// Service keys: keys:read and keys:write.
    pub fn keys(&self) -> Keys<'_> {
        Keys(self)
    }

    /// Webhooks: webhooks:read and webhooks:write. DEV plan and above.
    pub fn webhooks(&self) -> Webhooks<'_> {
        Webhooks(self)
    }

    /// Usage: usage:read.
    pub fn usage(&self) -> Usage<'_> {
        Usage(self)
    }

    /// The account's records: account:read.
    pub fn account(&self) -> Records<'_> {
        Records(self)
    }

    /// Modules: modules:write.
    pub fn modules(&self) -> ModulesApi<'_> {
        ModulesApi(self)
    }

    /// GET /v1/api-key: this account API key's own record (`self()` in the TypeScript SDK).
    pub async fn self_record(&self) -> Result<AccountApiKey> {
        self.call("self", Method::GET, "/v1/api-key", None, None)
            .await
    }

    /// GET /v1/account/api-keys/{id}/activity: this key's own activity log (another key's id answers 404).
    pub async fn activity(
        &self,
        id: u64,
        limit: Option<u32>,
        before: Option<u64>,
    ) -> Result<Vec<AccountApiKeyActivity>> {
        let path = self.path(
            &format!("/v1/account/api-keys/{id}/activity"),
            &[
                ("limit", limit.map(|v| v.to_string())),
                ("before", before.map(|v| v.to_string())),
            ],
        )?;
        self.call("activity", Method::GET, &path, None, None).await
    }
}

pub struct Keys<'a>(&'a CarbiumAccount);

impl Keys<'_> {
    /// GET /v1/keys: every key's record, never its value.
    pub async fn list(&self) -> Result<Vec<ServiceKey>> {
        self.0
            .call("keys.list", Method::GET, "/v1/keys", None, None)
            .await
    }
    /// POST /v1/keys: a new service key; raw_key is shown once.
    pub async fn create(
        &self,
        name: Option<&str>,
        scope: Option<&[KeyScope]>,
    ) -> Result<CreatedKey> {
        let mut body = Map::new();
        if let Some(n) = name {
            body.insert("name".into(), json!(n));
        }
        if let Some(s) = scope {
            body.insert("scope".into(), json!(s));
        }
        self.0
            .call(
                "keys.create",
                Method::POST,
                "/v1/keys",
                Some(Value::Object(body)),
                None,
            )
            .await
    }
    /// POST /v1/keys/{id}/rotate: a new raw key for the same record.
    pub async fn rotate(&self, id: u64) -> Result<RotatedKey> {
        self.0
            .call(
                "keys.rotate",
                Method::POST,
                &format!("/v1/keys/{id}/rotate"),
                None,
                None,
            )
            .await
    }
    /// POST /v1/keys/{id}/revoke
    pub async fn revoke(&self, id: u64) -> Result<Applied> {
        self.0
            .call(
                "keys.revoke",
                Method::POST,
                &format!("/v1/keys/{id}/revoke"),
                None,
                None,
            )
            .await
    }
    /// DELETE /v1/keys/{id}
    pub async fn delete(&self, id: u64) -> Result<Deleted> {
        self.0
            .call(
                "keys.delete",
                Method::DELETE,
                &format!("/v1/keys/{id}"),
                None,
                None,
            )
            .await
    }
    /// POST /v1/keys/{id}/name; "" clears it.
    pub async fn rename(&self, id: u64, name: &str) -> Result<KeyName> {
        self.0
            .call(
                "keys.rename",
                Method::POST,
                &format!("/v1/keys/{id}/name"),
                Some(json!({ "name": name })),
                None,
            )
            .await
    }
    /// PATCH /v1/keys/{id}/scope: at least one plane.
    pub async fn set_scope(&self, id: u64, scope: &[KeyScope]) -> Result<KeyScopes> {
        self.0
            .call(
                "keys.setScope",
                Method::PATCH,
                &format!("/v1/keys/{id}/scope"),
                Some(json!({ "scope": scope })),
                None,
            )
            .await
    }
    /// PATCH /v1/keys/{id}/caps: per-method-class ceilings; an empty list clears them.
    pub async fn set_caps(&self, id: u64, per_method_caps: &[MethodCap]) -> Result<Vec<MethodCap>> {
        self.0
            .call(
                "keys.setCaps",
                Method::PATCH,
                &format!("/v1/keys/{id}/caps"),
                Some(json!({ "per_method_caps": per_method_caps })),
                None,
            )
            .await
    }
    /// PATCH /v1/keys/{id}/allowlist: at most 16 entries.
    pub async fn set_allowlist(&self, id: u64, ip_allow: &[String]) -> Result<KeyAllowlist> {
        self.0
            .call(
                "keys.setAllowlist",
                Method::PATCH,
                &format!("/v1/keys/{id}/allowlist"),
                Some(json!({ "ip_allow": ip_allow })),
                None,
            )
            .await
    }
    /// PATCH /v1/keys/{id}/limits: connections and in-flight calls, 0 to 10000 each.
    pub async fn set_limits(&self, id: u64, limits: KeyLimits) -> Result<KeyLimitsSet> {
        self.0
            .call(
                "keys.setLimits",
                Method::PATCH,
                &format!("/v1/keys/{id}/limits"),
                Some(json!(limits)),
                None,
            )
            .await
    }
}

pub struct Webhooks<'a>(&'a CarbiumAccount);

impl Webhooks<'_> {
    /// GET /v1/webhooks
    pub async fn list(&self) -> Result<Vec<Webhook>> {
        self.0
            .call("webhooks.list", Method::GET, "/v1/webhooks", None, None)
            .await
    }
    /// GET /v1/webhooks/{id}, with its addresses.
    pub async fn get(&self, id: u64) -> Result<Webhook> {
        self.0
            .call(
                "webhooks.get",
                Method::GET,
                &format!("/v1/webhooks/{id}"),
                None,
                None,
            )
            .await
    }
    /// GET /v1/webhooks/{id}/deliveries: the log, newest first.
    pub async fn deliveries(&self, id: u64, page: &PageQuery) -> Result<Vec<WebhookDelivery>> {
        let path = self.0.path(
            &format!("/v1/webhooks/{id}/deliveries"),
            &[
                ("limit", page.limit.map(|v| v.to_string())),
                ("before", page.before.clone()),
            ],
        )?;
        self.0
            .call("webhooks.deliveries", Method::GET, &path, None, None)
            .await
    }
    /// POST /v1/webhooks: the secret is shown once.
    pub async fn create(&self, request: &CreateWebhookRequest) -> Result<CreatedWebhook> {
        let body = serde_json::to_value(request)
            .map_err(|e| Error::new(ErrorKind::Request, e.to_string()))?;
        self.0
            .call(
                "webhooks.create",
                Method::POST,
                "/v1/webhooks",
                Some(body),
                None,
            )
            .await
    }
    /// PATCH /v1/webhooks/{id}: any create field, or status paused or active.
    pub async fn update(&self, id: u64, request: &UpdateWebhookRequest) -> Result<Webhook> {
        let body = serde_json::to_value(request)
            .map_err(|e| Error::new(ErrorKind::Request, e.to_string()))?;
        self.0
            .call(
                "webhooks.update",
                Method::PATCH,
                &format!("/v1/webhooks/{id}"),
                Some(body),
                None,
            )
            .await
    }
    /// DELETE /v1/webhooks/{id}
    pub async fn delete(&self, id: u64) -> Result<Deleted> {
        self.0
            .call(
                "webhooks.delete",
                Method::DELETE,
                &format!("/v1/webhooks/{id}"),
                None,
                None,
            )
            .await
    }
    /// POST /v1/webhooks/{id}/rotate: the old secret still signs until previous_valid_until.
    pub async fn rotate_secret(&self, id: u64) -> Result<RotatedWebhookSecret> {
        self.0
            .call(
                "webhooks.rotateSecret",
                Method::POST,
                &format!("/v1/webhooks/{id}/rotate"),
                None,
                None,
            )
            .await
    }
    /// POST /v1/webhooks/{id}/test: one signed test event, tried once within 15 s. Read outcome, not the HTTP status.
    pub async fn test(&self, id: u64) -> Result<WebhookTestResult> {
        let timeout = self.0.timeout.max(Duration::from_secs(25));
        self.0
            .call(
                "webhooks.test",
                Method::POST,
                &format!("/v1/webhooks/{id}/test"),
                None,
                Some(timeout),
            )
            .await
    }
}

pub struct Usage<'a>(&'a CarbiumAccount);

impl Usage<'_> {
    fn pairs(q: &UsageQuery) -> [(&'static str, Option<String>); 4] {
        [
            ("from", q.from.clone()),
            ("to", q.to.clone()),
            ("key_id", q.key_id.map(|v| v.to_string())),
            ("group", q.group.clone()),
        ]
    }
    /// GET /v1/usage: per UTC day and method; from and to as YYYY-MM-DD, at most 90 days.
    pub async fn daily(&self, q: &UsageQuery) -> Result<Vec<DailyUsageRow>> {
        self.0
            .call(
                "usage.daily",
                Method::GET,
                &self.0.path("/v1/usage", &Self::pairs(q))?,
                None,
                None,
            )
            .await
    }
    /// GET /v1/keys/usage: this period's credits per key.
    pub async fn per_key(&self) -> Result<KeysUsage> {
        self.0
            .call("usage.perKey", Method::GET, "/v1/keys/usage", None, None)
            .await
    }
    /// GET /v1/keys/load: current requests per second per key.
    pub async fn load(&self) -> Result<Vec<KeyLoad>> {
        self.0
            .call("usage.load", Method::GET, "/v1/keys/load", None, None)
            .await
    }
    /// GET /v1/usage/hourly: RFC 3339 from and to, at most 7 days.
    pub async fn hourly(&self, q: &UsageQuery) -> Result<Vec<HourlyUsageRow>> {
        self.0
            .call(
                "usage.hourly",
                Method::GET,
                &self.0.path("/v1/usage/hourly", &Self::pairs(q))?,
                None,
                None,
            )
            .await
    }
    /// GET /v1/usage/latency: p50, p95 and p99 per hour and method class.
    pub async fn latency(&self, q: &UsageQuery) -> Result<Vec<LatencyRow>> {
        self.0
            .call(
                "usage.latency",
                Method::GET,
                &self.0.path("/v1/usage/latency", &Self::pairs(q))?,
                None,
                None,
            )
            .await
    }
}

pub struct Records<'a>(&'a CarbiumAccount);

impl Records<'_> {
    /// GET /v1/account
    pub async fn get(&self) -> Result<Account> {
        self.0
            .call("account.get", Method::GET, "/v1/account", None, None)
            .await
    }
    /// GET /v1/balance: credits granted, used and remaining.
    pub async fn balance(&self) -> Result<Balance> {
        self.0
            .call("account.balance", Method::GET, "/v1/balance", None, None)
            .await
    }
    /// GET /v1/invoices
    pub async fn invoices(&self) -> Result<Vec<Invoice>> {
        self.0
            .call("account.invoices", Method::GET, "/v1/invoices", None, None)
            .await
    }
    /// GET /v1/payments
    pub async fn payments(&self) -> Result<Vec<Payment>> {
        self.0
            .call("account.payments", Method::GET, "/v1/payments", None, None)
            .await
    }
    /// GET /v1/modules: every module with its plans and what the account holds.
    pub async fn modules(&self) -> Result<Modules> {
        self.0
            .call("account.modules", Method::GET, "/v1/modules", None, None)
            .await
    }
    /// GET /v1/billing/crypto-intent/{id}: the state of a crypto payment intent.
    pub async fn crypto_intent(&self, id: &str) -> Result<CryptoIntent> {
        self.0
            .call(
                "account.cryptoIntent",
                Method::GET,
                &format!("/v1/billing/crypto-intent/{}", segment(id)),
                None,
                None,
            )
            .await
    }
}

pub struct ModulesApi<'a>(&'a CarbiumAccount);

impl ModulesApi<'_> {
    /// POST /v1/modules/{module}: enables a free plan, for example `enable_free("quote", Some("free"))`.
    pub async fn enable_free(&self, module: &str, plan: Option<&str>) -> Result<Value> {
        let body = plan.map_or_else(|| json!({}), |p| json!({ "plan": p }));
        self.0
            .call(
                "modules.enableFree",
                Method::POST,
                &format!("/v1/modules/{}", segment(module)),
                Some(body),
                None,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_route_is_built_alone_so_a_normalised_base_url_cannot_cut_it() {
        let key = ["c_acct", "_pathtest000000000"].concat();
        let a = CarbiumAccount::new(AccountOptions {
            base_url: "https://carbium.sh:443/".into(),
            ..AccountOptions::new(key)
        })
        .unwrap();
        let path = a
            .path(
                "/v1/usage",
                &[
                    ("from", Some("2026-11-01".into())),
                    ("key_id", None),
                    ("group", Some("key".into())),
                ],
            )
            .unwrap();
        assert_eq!(path, "/v1/usage?from=2026-11-01&group=key");
        assert_eq!(a.path("/v1/keys", &[]).unwrap(), "/v1/keys");
    }
}

//! JSON-RPC 2.0 over HTTPS to services.carbium.sh, with the published retry policy and the door's batch rules
//! checked locally.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::error::{Code, Error, ErrorKind, Result, Secret, decode};
use crate::generated::{Batch, Plane, method_spec};
use crate::retry::{
    RetryContext, RetryOptions, RetryPlan, backoff_delay_ms, decide_retry, server_hint_ms,
};

/// The crate's version, sent in the solana-client header.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const CLIENT_HEADER: &str = concat!("rust/carbium@", env!("CARGO_PKG_VERSION"));

/// The default endpoint for RPC, DAS and the quote plane.
pub const DEFAULT_ENDPOINT: &str = "https://services.carbium.sh";

/// Wire limits of the services.carbium.sh door.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest request body in bytes; a bigger one is refused locally (the door answers 413).
    pub body_max_bytes: usize,
    /// Most calls in one JSON-RPC batch.
    pub max_batch: usize,
    /// Most permit methods in one batch.
    pub max_permit_per_batch: usize,
    /// Most pubkeys per getMultipleAccounts on PRO; below PRO a call over 100 is refused every time.
    pub max_multiple_accounts: usize,
    /// The getMultipleAccounts size every plan admits.
    pub multiple_accounts_chunk: usize,
    /// Most ids per getAssets or getAssetBatch call.
    pub das_max_ids: usize,
    /// Most mints per prices call.
    pub prices_max_mints: usize,
}

pub const LIMITS: Limits = Limits {
    body_max_bytes: 51_200,
    max_batch: 100,
    max_permit_per_batch: 2,
    max_multiple_accounts: 300,
    multiple_accounts_chunk: 100,
    das_max_ids: 1000,
    prices_max_mints: 64,
};

/// Options for [`Carbium::new`].
#[derive(Clone)]
pub struct ClientOptions {
    /// The full service key, c_serv_…. None or empty, calls go out with no key: the service answers its twelve public
    /// methods, 30 calls a minute per IP address, and refuses every other call with 401.
    pub api_key: Option<String>,
    /// Default https://services.carbium.sh.
    pub endpoint: String,
    /// The retry policy; None sends each call once.
    pub retry: Option<RetryOptions>,
    /// Per attempt; default 100 s, above the door's own 95 s deadline so its 504 rows decide.
    pub timeout: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            api_key: None,
            endpoint: DEFAULT_ENDPOINT.into(),
            retry: Some(RetryOptions::default()),
            timeout: Duration::from_secs(100),
        }
    }
}

impl fmt::Debug for ClientOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientOptions")
            .field("api_key", &self.api_key.as_ref().map(Secret::new))
            .field("endpoint", &self.endpoint)
            .field("retry", &self.retry)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// One call of a JSON-RPC batch.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonRpcCall {
    pub method: String,
    pub params: Option<Value>,
}

impl JsonRpcCall {
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            method: method.into(),
            params,
        }
    }
}

struct Inner {
    endpoint: String,
    key: Option<Secret>,
    key_header: Option<HeaderValue>,
    http: reqwest::Client,
    timeout: Duration,
    next_id: AtomicU64,
}

/// The client for services.carbium.sh: RPC, DAS and the quote plane on one key. Cheap to clone.
#[derive(Clone)]
pub struct Carbium {
    inner: Arc<Inner>,
    retry: Option<RetryOptions>,
}

impl fmt::Debug for Carbium {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Carbium")
            .field("endpoint", &self.inner.endpoint)
            .field("api_key", &self.inner.key)
            .field("retry", &self.retry)
            .field("timeout", &self.inner.timeout)
            .finish()
    }
}

/// Refuses a key in a URL: it would end up in logs and referers.
pub(crate) fn refuse_key_in_url(url: &str) -> Result<()> {
    let lower = url.to_ascii_lowercase();
    let in_query = ["api-key=", "api_key=", "apikey="]
        .iter()
        .any(|k| lower.contains(&format!("?{k}")) || lower.contains(&format!("&{k}")));
    if in_query || lower.contains("c_serv_") || lower.contains("c_acct_") {
        return Err(Error::new(
            ErrorKind::Request,
            "put the key in api_key, not in the endpoint URL: a key in a URL ends up in logs and referers",
        ));
    }
    reqwest::Url::parse(url).map_err(|e| {
        Error::new(
            ErrorKind::Request,
            format!("the endpoint is not a URL: {e}"),
        )
    })?;
    Ok(())
}

pub(crate) fn is_key(key: &str, prefix: &str) -> bool {
    key.strip_prefix(prefix)
        .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphanumeric()))
}

/// The HTTP client every Carbium client shares the settings of.
pub(crate) fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| {
            Error::new(ErrorKind::Transport, "could not set up the HTTP client").with_cause(&e)
        })
}

pub(crate) struct Answer {
    pub status: u16,
    pub body: Option<Value>,
    pub headers: HeaderMap,
}

/// What one attempt sends: a JSON-RPC body posted to the endpoint, or a GET of a REST route on it.
pub(crate) enum Wire {
    Post(String),
    Get(String),
}

#[derive(Serialize)]
struct Request<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<&'a Value>,
}

const REQUEST_CODES: [i64; 6] = [-32600, -32601, -32602, -32700, -32012, -32015];
const REQUEST_REASONS: [&str; 4] = [
    "PROGRAM_NOT_PROVISIONED",
    "CLASSIC_SPL_SCAN",
    "METHOD_NOT_SERVED",
    "UNKNOWN_METHOD",
];

fn kind_of(status: u16, code: Option<i64>, reason: Option<&str>) -> ErrorKind {
    match status {
        401 | 403 => ErrorKind::Auth,
        402 => ErrorKind::Credits,
        429 => ErrorKind::RateLimit,
        400 | 413 | 425 | 501 => ErrorKind::Request,
        200 => match code {
            Some(c) if (-32099..=-32051).contains(&c) => ErrorKind::Route,
            Some(c) if REQUEST_CODES.contains(&c) => ErrorKind::Request,
            _ if reason.is_some_and(|r| REQUEST_REASONS.contains(&r)) => ErrorKind::Request,
            _ => ErrorKind::Node,
        },
        _ => ErrorKind::Upstream,
    }
}

fn request_id(headers: &HeaderMap) -> Option<&str> {
    headers.get("x-request-id").and_then(|v| v.to_str().ok())
}

/// The error for one JSON-RPC error answer, and its retry plan.
pub(crate) fn rpc_error(
    method: &str,
    status: u16,
    body: &Value,
    headers: &HeaderMap,
    min_context_slot: Option<u64>,
) -> (Error, Option<RetryPlan>) {
    let code = body.get("code").and_then(Value::as_i64);
    let message = body
        .get("message")
        .and_then(Value::as_str)
        .map_or_else(|| format!("HTTP {status}"), String::from);
    let data = body.get("data");
    let reason = data.and_then(|d| d.get("reason")).and_then(Value::as_str);
    let served = data
        .and_then(|d| d.get("retryable"))
        .and_then(Value::as_bool);
    let plan = decide_retry(&RetryContext {
        method,
        http_status: status,
        code,
        message: &message,
        data,
        min_context_slot,
    });
    let mut e = Error::new(kind_of(status, code, reason), &message)
        .with_status(status)
        .with_method(method)
        .with_retryable(served.unwrap_or(plan.is_some()))
        .with_retry_after(server_hint_ms(headers, data))
        .with_request_id(request_id(headers));
    if let Some(c) = code {
        e = e.with_code(Code::Number(c));
    }
    if let Some(r) = reason {
        e = e.with_reason(r);
    }
    if let Some(d) = data {
        e = e.with_data(d);
    }
    (e, plan)
}

/// The error object of an answer, when it has one.
pub(crate) fn error_of(answer: &Value) -> Option<&Value> {
    answer
        .get("error")
        .filter(|e| !e.is_null() && *e != &Value::Bool(false))
}

fn first_chars(text: &str, n: usize) -> String {
    text.trim().chars().take(n).collect()
}

/// getTransaction and getBlock answer -32015 on a v1 transaction unless maxSupportedTransactionVersion is sent.
fn with_version(method: &str, params: Option<Value>) -> Option<Value> {
    let Some(Value::Array(mut a)) = params else {
        return params;
    };
    if matches!(method, "getTransaction" | "getBlock") && !a.is_empty() {
        match a.get_mut(1) {
            Some(Value::Object(o)) => {
                o.entry("maxSupportedTransactionVersion")
                    .or_insert(json!(1));
            }
            Some(v @ Value::Null) => *v = json!({ "maxSupportedTransactionVersion": 1 }),
            None => a.push(json!({ "maxSupportedTransactionVersion": 1 })),
            Some(_) => {}
        }
    }
    Some(Value::Array(a))
}

/// The minContextSlot a positional call carries in its trailing config object, so a -32016 wait can be sized.
fn min_context_slot_of(params: Option<&Value>) -> Option<u64> {
    params?.as_array()?.last()?.get("minContextSlot")?.as_u64()
}

fn unserved(method: &str) -> Error {
    Error::new(
        ErrorKind::Request,
        format!("{method} is not a served method"),
    )
    .with_method(method)
    .with_code(Code::Number(-32601))
}

impl Carbium {
    /// A client for services.carbium.sh. Refuses an account key, a malformed key and a key in the endpoint URL.
    pub fn new(options: ClientOptions) -> Result<Self> {
        let key = options.api_key.filter(|k| !k.is_empty()); // an empty string (an unset env var) means keyless
        if key.as_deref().is_some_and(|k| !is_key(k, "c_serv_")) {
            return Err(Error::new(
                ErrorKind::Auth,
                "api_key must be the full service key shown once on the dashboard (it starts with c_serv_); an account key (c_acct_) is for carbium.sh/v1 only",
            ));
        }
        refuse_key_in_url(&options.endpoint)?;
        let key_header = match &key {
            Some(k) => {
                let mut v = HeaderValue::from_str(k)
                    .map_err(|_| Error::new(ErrorKind::Auth, "api_key is not a header value"))?;
                v.set_sensitive(true);
                Some(v)
            }
            None => None,
        };
        let inner = Inner {
            endpoint: options.endpoint,
            key: key.map(Secret::new),
            key_header,
            http: http_client()?,
            timeout: options.timeout,
            next_id: AtomicU64::new(1),
        };
        Ok(Self {
            inner: Arc::new(inner),
            retry: options.retry,
        })
    }

    /// A WebSocket subscription client on the same key and host (https becomes wss), and its events.
    #[cfg(feature = "ws")]
    pub fn socket(
        &self,
        carrier: crate::ws::Carrier,
    ) -> Result<(
        crate::ws::CarbiumSocket,
        tokio::sync::mpsc::UnboundedReceiver<crate::ws::SocketEvent>,
    )> {
        let options = crate::ws::SocketOptions {
            api_key: self.inner.key.as_ref().map(|k| k.expose().to_string()),
            url: self.inner.endpoint.clone(),
            carrier,
            ..Default::default()
        };
        crate::ws::CarbiumSocket::new(options)
    }

    /// The endpoint the client posts to.
    pub fn endpoint(&self) -> &str {
        &self.inner.endpoint
    }

    /// The same client with another retry policy (None sends each call once), sharing its connections and ids.
    pub fn with_retry(&self, retry: Option<RetryOptions>) -> Self {
        Self {
            inner: self.inner.clone(),
            retry,
        }
    }

    /// Any served method by name, its result read as `T` (use `serde_json::Value` to read it untyped). A name the
    /// services do not serve is refused locally with code -32601.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> Result<T> {
        self.request(method, params).await
    }

    pub(crate) async fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> Result<T> {
        let value = self.call_value(method, params).await?;
        decode(value, ErrorKind::Upstream, method, "the result")
    }

    async fn call_value(&self, method: &str, params: Option<Value>) -> Result<Value> {
        method_spec(method).ok_or_else(|| unserved(method))?;
        let params = with_version(method, params);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let body = serde_json::to_string(&Request {
            jsonrpc: "2.0",
            id,
            method,
            params: params.as_ref(),
        })
        .map_err(|e| Error::new(ErrorKind::Request, e.to_string()))?;
        let mcs = min_context_slot_of(params.as_ref());
        let a = self.send(Wire::Post(body), method, mcs, false).await?;
        let malformed = |what: &str| {
            Error::new(
                ErrorKind::Upstream,
                format!("malformed JSON-RPC answer{what}"),
            )
            .with_method(method)
            .with_status(a.status)
        };
        match &a.body {
            Some(one @ Value::Object(o)) => {
                if let Some(err) = error_of(one) {
                    return Err(rpc_error(method, a.status, err, &a.headers, mcs).0);
                }
                o.get("result")
                    .cloned()
                    .ok_or_else(|| malformed(": neither result nor error"))
            }
            _ => Err(malformed("")),
        }
    }

    /// One JSON-RPC batch of 1 to 100 calls, answered in request order. Refused locally before sending when it
    /// breaks a door rule: an unserved method, a method that never batches, DAS mixed with other planes, more than
    /// two permit methods, or a body over the cap. A door refusal of the whole batch is an error; a per-call error
    /// comes back in its own place.
    pub async fn batch(&self, calls: &[JsonRpcCall]) -> Result<Vec<Result<Value>>> {
        if calls.is_empty() || calls.len() > LIMITS.max_batch {
            return Err(Error::new(
                ErrorKind::Request,
                format!(
                    "a batch holds 1 to {} calls, not {}",
                    LIMITS.max_batch,
                    calls.len()
                ),
            ));
        }
        let (mut permits, mut das) = (0, 0);
        for c in calls {
            let spec = method_spec(&c.method).ok_or_else(|| unserved(&c.method))?;
            if spec.batch == Batch::Never {
                return Err(Error::new(
                    ErrorKind::Request,
                    format!("{} is not batchable: send it alone", c.method),
                )
                .with_method(&c.method));
            }
            permits += usize::from(spec.permit);
            das += usize::from(spec.plane == Plane::Das);
        }
        if das != 0 && das != calls.len() {
            return Err(Error::new(
                ErrorKind::Request,
                "DAS methods batch only with other DAS methods",
            ));
        }
        if permits > LIMITS.max_permit_per_batch {
            return Err(Error::new(
                ErrorKind::Request,
                format!(
                    "a batch holds at most {} permit methods (getAccountInfo, getMultipleAccounts, getTransaction, ...), not {permits}: split it",
                    LIMITS.max_permit_per_batch
                ),
            ));
        }
        let first_id = self
            .inner
            .next_id
            .fetch_add(calls.len() as u64, Ordering::Relaxed);
        let params: Vec<Option<Value>> = calls
            .iter()
            .map(|c| with_version(&c.method, c.params.clone()))
            .collect();
        let requests: Vec<Request> = calls
            .iter()
            .zip(&params)
            .zip(first_id..)
            .map(|((c, p), id)| Request {
                jsonrpc: "2.0",
                id,
                method: &c.method,
                params: p.as_ref(),
            })
            .collect();
        let body = serde_json::to_string(&requests)
            .map_err(|e| Error::new(ErrorKind::Request, e.to_string()))?;
        let label = if calls.iter().any(|c| c.method == "sendTransaction") {
            "sendTransaction".to_string()
        } else {
            format!("batch({})", calls[0].method)
        };
        let a = self.send(Wire::Post(body), &label, None, true).await?;
        let answers = match &a.body {
            Some(Value::Array(answers)) => answers,
            other => {
                return Err(match other.as_ref().and_then(error_of) {
                    Some(err) => rpc_error(&label, a.status, err, &a.headers, None).0,
                    None => Error::new(
                        ErrorKind::Upstream,
                        "the door answered a batch with a single object",
                    )
                    .with_status(a.status),
                });
            }
        };
        let reason_of = |e: &Value| e.get("data").and_then(|d| d.get("reason")).cloned();
        if let Some(first) = answers.first().and_then(error_of) {
            let same = |el: &Value| {
                error_of(el).is_some_and(|e| {
                    e.get("code") == first.get("code")
                        && reason_of(e).is_some()
                        && reason_of(e) == reason_of(first)
                })
            };
            if answers.iter().all(same) {
                return Err(rpc_error(&label, a.status, first, &a.headers, None).0); // the door refused the whole batch
            }
        }
        let by_id: HashMap<u64, &Value> = answers
            .iter()
            .filter_map(|el| Some((el.get("id")?.as_u64()?, el)))
            .collect();
        Ok(calls
            .iter()
            .zip(first_id..)
            .map(|(c, id)| match by_id.get(&id) {
                None => Err(
                    Error::new(ErrorKind::Upstream, "no answer for this batch element")
                        .with_method(&c.method)
                        .with_status(a.status),
                ),
                Some(el) => match error_of(el) {
                    Some(err) => Err(rpc_error(&c.method, a.status, err, &a.headers, None).0),
                    None => Ok(el.get("result").cloned().unwrap_or(Value::Null)),
                },
            })
            .collect())
    }

    async fn attempt(
        &self,
        wire: &Wire,
    ) -> std::result::Result<(u16, HeaderMap, String), reqwest::Error> {
        let req = match wire {
            Wire::Post(body) => self
                .inner
                .http
                .post(&self.inner.endpoint)
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone()),
            Wire::Get(url) => self.inner.http.get(url),
        };
        let mut req = req
            .header("solana-client", CLIENT_HEADER)
            .timeout(self.inner.timeout);
        if let Some(k) = &self.inner.key_header {
            req = req.header("x-api-key", k.clone());
        }
        let res = req.send().await?;
        let (status, headers) = (res.status().as_u16(), res.headers().clone());
        Ok((status, headers, res.text().await?))
    }

    pub(crate) async fn send(
        &self,
        wire: Wire,
        method: &str,
        min_context_slot: Option<u64>,
        is_batch: bool,
    ) -> Result<Answer> {
        if let Wire::Post(body) = &wire
            && body.len() > LIMITS.body_max_bytes
        {
            return Err(Error::new(
                ErrorKind::Request,
                format!(
                    "request body is {} bytes; the cap is {}: split the batch or narrow the call",
                    body.len(),
                    LIMITS.body_max_bytes
                ),
            )
            .with_method(method));
        }
        let user_max = self.retry.map_or(Some(1), |r| r.max_attempts);
        let max_hint_ms = self.retry.map_or(0, |r| r.max_hint_ms);
        let random = self.retry.unwrap_or_default().random;
        let stop = |plan: Option<RetryPlan>, attempt: u32| {
            plan.is_none_or(|p| {
                attempt >= user_max.map_or(p.max_attempts, |u| u.min(p.max_attempts))
            })
        };
        let wait = |plan: Option<RetryPlan>, attempt: u32, hint: Option<u64>| {
            tokio::time::sleep(Duration::from_millis(
                plan.map_or(0, |p| backoff_delay_ms(&p, attempt, hint, random)),
            ))
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let (status, headers, text) = match self.attempt(&wire).await {
                Ok(answer) => answer,
                Err(e) => {
                    let decided = decide_retry(&RetryContext {
                        method,
                        http_status: 0,
                        code: None,
                        message: "",
                        data: None,
                        min_context_slot: None,
                    });
                    // a read that outran the client timeout is tried once more, as the 504 row says
                    let plan = decided.map(|p| {
                        if e.is_timeout() {
                            RetryPlan {
                                max_attempts: 2,
                                ..p
                            }
                        } else {
                            p
                        }
                    });
                    if stop(plan, attempt) {
                        let message = if e.is_timeout() {
                            format!(
                                "the request timed out after {} ms",
                                self.inner.timeout.as_millis()
                            )
                        } else {
                            e.to_string()
                        };
                        return Err(Error::new(ErrorKind::Transport, message)
                            .with_method(method)
                            .with_retryable(plan.is_some())
                            .with_cause(&e));
                    }
                    wait(plan, attempt, None).await;
                    continue;
                }
            };
            let parsed: Option<Value> = serde_json::from_str(&text).ok();
            if status == 200 && matches!(parsed, Some(Value::Object(_) | Value::Array(_))) {
                let answer = Answer {
                    status,
                    body: parsed,
                    headers,
                };
                let Some(err) = answer
                    .body
                    .as_ref()
                    .filter(|b| b.is_object() && !is_batch)
                    .and_then(error_of)
                else {
                    return Ok(answer); // a result, or a batch whose per-element errors are the caller's
                };
                // a method-level error at 200: retried only on the rows the policy names
                let (error, plan) = rpc_error(method, 200, err, &answer.headers, min_context_slot);
                if stop(plan, attempt) || error.retry_after_ms().is_some_and(|h| h > max_hint_ms) {
                    return Ok(answer); // the caller turns the error answer into its error, hint included
                }
                wait(plan, attempt, error.retry_after_ms()).await;
                continue;
            }
            let err_body = match &parsed {
                Some(Value::Array(a)) => a.first().and_then(error_of),
                Some(o @ Value::Object(_)) => error_of(o),
                _ => None,
            };
            if status == 200 && err_body.is_none() {
                let shown = first_chars(&text, 120);
                let shown = if shown.is_empty() {
                    "empty body".to_string()
                } else {
                    shown
                };
                return Err(Error::new(
                    ErrorKind::Upstream,
                    format!("malformed JSON-RPC answer: {shown}"),
                )
                .with_method(method)
                .with_status(200)
                .with_request_id(request_id(&headers)));
            }
            let fallback = json!({ "message": Some(first_chars(&text, 200)).filter(|t| !t.is_empty()).unwrap_or_else(|| format!("HTTP {status}")) });
            let (error, plan) = rpc_error(
                method,
                status,
                err_body.unwrap_or(&fallback),
                &headers,
                None,
            );
            if stop(plan, attempt) || error.retry_after_ms().is_some_and(|h| h > max_hint_ms) {
                return Err(error);
            }
            wait(plan, attempt, error.retry_after_ms()).await;
        }
    }
}

/// A param as JSON, for the typed calls.
pub(crate) fn arg<T: Serialize + ?Sized>(value: &T) -> Result<Value> {
    serde_json::to_value(value).map_err(|e| {
        Error::new(
            ErrorKind::Request,
            format!("could not write the params: {e}"),
        )
    })
}

/// Positional params without the trailing ones the caller left out; None when none are left.
pub(crate) fn positional(mut args: Vec<Value>) -> Option<Value> {
    while args.last().is_some_and(Value::is_null) {
        args.pop();
    }
    (!args.is_empty()).then_some(Value::Array(args))
}

/// Param `i` of a positional example, read as the typed call's argument.
#[cfg(test)]
pub(crate) fn example<T: DeserializeOwned>(example: &Value, i: usize) -> Result<T> {
    example_named(example.get(i).cloned().unwrap_or(Value::Null))
}

/// A named-params example, read as the typed call's params struct.
#[cfg(test)]
pub(crate) fn example_named<T: DeserializeOwned>(example: Value) -> Result<T> {
    serde_json::from_value(example).map_err(|e| {
        Error::new(
            ErrorKind::Request,
            format!("the example does not read as the call's params: {e}"),
        )
    })
}

//! One JSON HTTP exchange with a timeout, shared by the account, DCA and token clients. None of them retries.

use std::time::Duration;

use reqwest::Method;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{Code, Error, ErrorKind, Result, decode};
use crate::retry::retry_after_header_ms;

pub(crate) struct HttpAnswer {
    pub status: u16,
    pub headers: HeaderMap,
    pub text: String,
    /// The parsed JSON body, or None when the body is not JSON.
    pub json: Option<Value>,
}

impl HttpAnswer {
    pub fn field(&self, key: &str) -> Option<&Value> {
        self.json
            .as_ref()
            .and_then(|j| j.as_object())
            .and_then(|o| o.get(key))
    }

    pub fn str_field(&self, key: &str) -> Option<&str> {
        self.field(key).and_then(Value::as_str)
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        retry_after_header_ms(&self.headers)
    }

    pub fn request_id(&self) -> Option<&str> {
        self.headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
    }

    /// The answer as an API error: its code from `code_key`, its message from the first string among `message_keys`,
    /// else the body text, else the status.
    pub fn error(
        &self,
        kind: ErrorKind,
        method: &str,
        code_key: &str,
        message_keys: &[&str],
    ) -> Error {
        let text = self.text.trim().chars().take(200).collect::<String>();
        let message = message_keys
            .iter()
            .find_map(|k| self.str_field(k))
            .map(String::from)
            .or(Some(text).filter(|t| !t.is_empty()))
            .unwrap_or_else(|| format!("HTTP {}", self.status));
        let mut e = Error::new(kind, message)
            .with_method(method)
            .with_status(self.status);
        if let Some(code) = self.str_field(code_key) {
            e = e.with_code(Code::Text(code.to_string()));
        }
        if let Some(body) = self.json.as_ref().filter(|j| j.is_object()) {
            e = e.with_data(body);
        }
        e
    }

    pub fn read<T: DeserializeOwned>(&self, kind: ErrorKind, method: &str) -> Result<T> {
        decode(
            self.json.clone().unwrap_or(Value::Null),
            kind,
            method,
            "the answer",
        )
        .map_err(|e| e.with_status(self.status))
    }
}

/// Which client makes an exchange, for its errors, and how long it may take.
pub(crate) struct Call<'a> {
    pub kind: ErrorKind,
    pub name: &'a str,
    pub timeout: Duration,
}

/// One exchange. A body is sent as JSON; a failure to get any answer is an error of `kind`, retryable for a GET.
pub(crate) async fn http_json(
    http: &reqwest::Client,
    method: Method,
    url: &str,
    headers: &[(&'static str, HeaderValue)],
    body: Option<&Value>,
    call: Call<'_>,
) -> Result<HttpAnswer> {
    let Call {
        kind,
        name,
        timeout,
    } = call;
    let mut req = http
        .request(method.clone(), url)
        .header(ACCEPT, "application/json")
        .timeout(timeout);
    for (k, v) in headers {
        req = req.header(*k, v.clone());
    }
    if let Some(b) = body {
        req = req
            .header(CONTENT_TYPE, "application/json")
            .body(b.to_string());
    }
    let fail = |e: reqwest::Error| {
        Error::new(
            kind,
            if e.is_timeout() {
                format!("the request timed out after {} ms", timeout.as_millis())
            } else {
                e.to_string()
            },
        )
        .with_method(name)
        .with_retryable(method == Method::GET)
        .with_cause(&e)
    };
    let res = req.send().await.map_err(fail)?;
    let (status, headers) = (res.status().as_u16(), res.headers().clone());
    let text = res.text().await.map_err(fail)?;
    let json = if text.is_empty() {
        None
    } else {
        serde_json::from_str(&text).ok()
    };
    Ok(HttpAnswer {
        status,
        headers,
        text,
        json,
    })
}

/// `base` + `path` with the given query pairs, the ones left out skipped, form-encoded.
pub(crate) fn url(base: &str, path: &str, pairs: &[(&str, Option<String>)]) -> Result<String> {
    let mut url = reqwest::Url::parse(&format!("{base}{path}"))
        .map_err(|e| Error::new(ErrorKind::Request, format!("not a URL: {e}")))?;
    if pairs.iter().any(|(_, v)| v.is_some()) {
        let mut q = url.query_pairs_mut();
        for (k, v) in pairs {
            if let Some(v) = v {
                q.append_pair(k, v);
            }
        }
    }
    Ok(url.to_string())
}

/// `path` with the given query pairs, the ones left out skipped, form-encoded.
pub(crate) fn path_and_query(path: &str, pairs: &[(&str, Option<String>)]) -> String {
    let query: Vec<String> = pairs
        .iter()
        .filter_map(|(k, v)| v.as_ref().map(|v| format!("{}={}", form(k), form(v))))
        .collect();
    if query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{}", query.join("&"))
    }
}

/// One query component, form-encoded as URLSearchParams does (space becomes +).
fn form(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b' ' => "+".to_string(),
            b if b.is_ascii_alphanumeric() || b"*-._".contains(&b) => (b as char).to_string(),
            b => format!("%{b:02X}"),
        })
        .collect()
}

/// One path segment, percent-encoded as encodeURIComponent does.
pub(crate) fn segment(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~!'()*".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// A base URL without its trailing slash, refused when it carries a key.
pub(crate) fn base_url(url: &str) -> Result<String> {
    crate::transport::refuse_key_in_url(url)?;
    Ok(url.trim_end_matches('/').to_string())
}

pub(crate) fn sensitive(value: &str) -> Result<HeaderValue> {
    let mut v = HeaderValue::from_str(value)
        .map_err(|_| Error::new(ErrorKind::Auth, "the key is not a header value"))?;
    v.set_sensitive(true);
    Ok(v)
}

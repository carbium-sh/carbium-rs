//! One error type for every failure. Whatever it shows has API keys and webhook secrets cut out.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The coarse category of a failure; `code` and `reason` carry the service's own detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// 401 or 403: the key is unknown, suspended, revoked, lacks the plane or module, or the address is not allowed.
    Auth,
    /// 402: credits exhausted. Waiting never helps.
    Credits,
    /// 429: a rate, slot or load refusal, or credits still loading. Waiting helps.
    RateLimit,
    /// The request itself must change: bad params, unknown method, body or batch over a cap, unsupported shape.
    Request,
    /// 5xx, or an answer that is not JSON-RPC. Waiting may fix it.
    Upstream,
    /// HTTP 200 with an error from the node, the index or the DAS store.
    Node,
    /// Quote and swap plane: no route, unknown venue, reserves, pool state, expired quote.
    Route,
    /// No HTTP answer at all: a network error or a timeout.
    Transport,
    /// A WebSocket handshake refusal, in-socket refusal or terminal close.
    Socket,
    /// A Carbon Stream (gRPC) refusal that ends the stream or a call.
    Stream,
    /// The account API on carbium.sh answered with an error.
    Account,
    /// The DCA API answered ok:false.
    Dca,
    /// The token API answered with an error.
    Tokens,
    /// A webhook delivery failed its signature check.
    Webhook,
}

impl ErrorKind {
    /// The kind's name, as the TypeScript and Python packages spell it.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::Credits => "credits",
            Self::RateLimit => "rate_limit",
            Self::Request => "request",
            Self::Upstream => "upstream",
            Self::Node => "node",
            Self::Route => "route",
            Self::Transport => "transport",
            Self::Socket => "socket",
            Self::Stream => "stream",
            Self::Account => "account",
            Self::Dca => "dca",
            Self::Tokens => "tokens",
            Self::Webhook => "webhook",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The service's own error code: a JSON-RPC number, an HTTP API's error string, a gRPC status or a close code.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Code {
    Number(i64),
    Text(String),
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{n}"),
            Self::Text(s) => f.write_str(s),
        }
    }
}

/// Every failure of the crate. Its message, data and cause are redacted when it is made, so Display and Debug are
/// safe to log.
#[derive(thiserror::Error, Debug, Clone)]
#[error(transparent)]
pub struct Error(Box<Inner>);

/// The fields of an [`Error`], boxed so a `Result` stays small.
#[derive(thiserror::Error, Debug, Clone)]
#[error("{message}")]
struct Inner {
    kind: ErrorKind,
    message: String,
    http_status: u16,
    code: Option<Code>,
    reason: Option<String>,
    retryable: bool,
    retry_after_ms: Option<u64>,
    request_id: Option<String>,
    method: Option<String>,
    data: Option<Value>,
    #[source]
    cause: Option<Cause>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl AsRef<str>) -> Self {
        Self(Box::new(Inner {
            kind,
            message: redact(message.as_ref()),
            http_status: 0,
            code: None,
            reason: None,
            retryable: false,
            retry_after_ms: None,
            request_id: None,
            method: None,
            data: None,
            cause: None,
        }))
    }

    pub(crate) fn with_kind(mut self, kind: ErrorKind) -> Self {
        self.0.kind = kind;
        self
    }

    pub(crate) fn with_status(mut self, status: u16) -> Self {
        self.0.http_status = status;
        self
    }

    pub(crate) fn with_code(mut self, code: Code) -> Self {
        self.0.code = Some(match code {
            Code::Text(s) => Code::Text(redact(&s)),
            n => n,
        });
        self
    }

    pub(crate) fn with_reason(mut self, reason: &str) -> Self {
        self.0.reason = Some(redact(reason));
        self
    }

    pub(crate) fn with_retryable(mut self, retryable: bool) -> Self {
        self.0.retryable = retryable;
        self
    }

    pub(crate) fn with_retry_after(mut self, ms: Option<u64>) -> Self {
        self.0.retry_after_ms = ms;
        self
    }

    /// Adds the answer's x-request-id, and quotes it at the end of the message.
    pub(crate) fn with_request_id(mut self, id: Option<&str>) -> Self {
        if let Some(id) = id.filter(|id| !id.is_empty()) {
            let id = redact(id);
            self.0.message = format!("{} (x-request-id: {id})", self.0.message);
            self.0.request_id = Some(id);
        }
        self
    }

    pub(crate) fn with_method(mut self, method: &str) -> Self {
        self.0.method = Some(redact(method));
        self
    }

    pub(crate) fn with_data(mut self, data: &Value) -> Self {
        self.0.data = Some(redact_value(data));
        self
    }

    pub(crate) fn with_cause(mut self, cause: &(dyn std::error::Error + 'static)) -> Self {
        self.0.cause = Some(Cause::of(cause, 0));
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.0.kind
    }

    /// The redacted message, ending with ` (x-request-id: …)` when the answer carried one.
    pub fn message(&self) -> &str {
        &self.0.message
    }

    /// The HTTP status of the answer; 0 when there was none.
    pub fn http_status(&self) -> u16 {
        self.0.http_status
    }

    pub fn code(&self) -> Option<&Code> {
        self.0.code.as_ref()
    }

    /// The door's error.data.reason (UPPER_SNAKE), a close reason, a gRPC status name or a webhook check's reason.
    pub fn reason(&self) -> Option<&str> {
        self.0.reason.as_deref()
    }

    /// Whether waiting and sending the same request again can change the answer.
    pub fn is_retryable(&self) -> bool {
        self.0.retryable
    }

    /// The server's wait hint in milliseconds, when it gave one.
    pub fn retry_after_ms(&self) -> Option<u64> {
        self.0.retry_after_ms
    }

    /// The answer's x-request-id, to quote to support.
    pub fn request_id(&self) -> Option<&str> {
        self.0.request_id.as_deref()
    }

    /// The method, subscription, route or filter the error belongs to.
    pub fn method(&self) -> Option<&str> {
        self.0.method.as_deref()
    }

    /// The error's data object as served, redacted.
    pub fn data(&self) -> Option<&Value> {
        self.0.data.as_ref()
    }
}

/// The redacted copy of an underlying error and its chain, at most six deep.
#[derive(Debug, Clone)]
pub struct Cause {
    message: String,
    source: Option<Box<Cause>>,
}

impl Cause {
    fn of(e: &(dyn std::error::Error + 'static), depth: usize) -> Self {
        Self {
            message: redact(&e.to_string()),
            source: e
                .source()
                .filter(|_| depth < 6)
                .map(|s| Box::new(Self::of(s, depth + 1))),
        }
    }
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Cause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|c| c as _)
    }
}

/// A key or secret: Debug shows its prefix only, and it has no Display. Read it with `expose`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The raw value. Store it; never print it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = ["c_serv_", "c_acct_", "whsec_"]
            .into_iter()
            .find(|p| self.0.starts_with(p))
            .unwrap_or("");
        write!(f, "\"{prefix}…\"")
    }
}

/// Replaces service keys, account keys, webhook secrets and key-bearing URL parts with their prefix and an ellipsis.
pub fn redact(text: &str) -> String {
    let secret = |c: char| c.is_ascii_alphanumeric() || "+/=_-".contains(c);
    let url_value = |c: char| !(c == '&' || c.is_whitespace() || "\"'<>".contains(c));
    let subprotocol = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-';
    let text = cut_after(text, &["c_serv_", "c_acct_", "whsec_"], false, secret);
    let text = cut_after(
        &text,
        &["api-key=", "api_key=", "apikey=", "token="],
        true,
        url_value,
    );
    cut_after(&text, &["api-key."], false, subprotocol)
}

/// Each prefix followed by at least one `keep` character becomes the prefix and an ellipsis.
fn cut_after(
    text: &str,
    prefixes: &[&str],
    ignore_case: bool,
    keep: impl Fn(char) -> bool,
) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    'scan: while let Some(c) = rest.chars().next() {
        for p in prefixes {
            let n = p.len();
            let found = rest.is_char_boundary(n.min(rest.len()))
                && rest.len() >= n
                && if ignore_case {
                    rest[..n].eq_ignore_ascii_case(p)
                } else {
                    rest.starts_with(p)
                };
            if !found {
                continue;
            }
            let tail = &rest[n..];
            let cut = tail
                .char_indices()
                .find(|&(_, c)| !keep(c))
                .map_or(tail.len(), |(i, _)| i);
            if cut > 0 {
                out.push_str(&rest[..n]);
                out.push('…');
                rest = &tail[cut..];
                continue 'scan;
            }
        }
        out.push(c);
        rest = &rest[c.len_utf8()..];
    }
    out
}

/// A copy of a JSON value with every string, keys included, redacted.
pub(crate) fn redact_value(value: &Value) -> Value {
    match value {
        Value::String(s) => Value::String(redact(s)),
        Value::Array(a) => Value::Array(a.iter().map(redact_value).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (redact(k), redact_value(v)))
                .collect::<Map<_, _>>(),
        ),
        other => other.clone(),
    }
}

/// Reads a JSON value as `T`. A mismatch names the field path, and the raw value rides in the error's data.
pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    value: Value,
    kind: ErrorKind,
    method: &str,
    what: &str,
) -> Result<T> {
    serde_path_to_error::deserialize(&value).map_err(|e| {
        Error::new(
            kind,
            format!(
                "{method}: {what} does not read as its typed shape at `{}`: {}",
                e.path(),
                e.inner()
            ),
        )
        .with_method(method)
        .with_data(&value)
    })
}

/// The crate's result type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Leaky {
        text: String,
        source: Option<Box<Leaky>>,
    }

    impl fmt::Display for Leaky {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.text)
        }
    }

    impl std::error::Error for Leaky {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source.as_deref().map(|c| c as _)
        }
    }

    #[test]
    fn a_key_in_a_cause_chain_shows_in_neither_display_debug_nor_the_chain() {
        let key = ["c_serv", "_causekey0000000000"].concat();
        let inner = Leaky {
            text: format!("dns for https://x.test/?api-key={key}"),
            source: None,
        };
        let outer = Leaky {
            text: format!("request with {key} failed"),
            source: Some(Box::new(inner)),
        };
        let e = Error::new(ErrorKind::Transport, "failed").with_cause(&outer);
        let mut shown = format!("{e} {e:?}");
        let mut cause = std::error::Error::source(&e);
        while let Some(c) = cause {
            shown.push_str(&format!(" {c} {c:?}"));
            cause = c.source();
        }
        assert!(!shown.contains("causekey"), "{shown}");
        assert!(
            shown.contains("c_serv_…") && shown.contains("api-key=…"),
            "{shown}"
        );
    }
}

//! The one retry and backoff policy carbium.sh/skill.md states for every plane: wait the larger of the server's hint
//! (retry-after-ms, else data.retryAfterMs, else Retry-After) and 250 ms × 2^(attempt−1) capped at 4 s, plus 0 to 250 ms of jitter; never cut
//! a server hint; stop after 4 attempts unless the row says otherwise. sendTransaction is never retried.

use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::header::HeaderMap;
use serde_json::Value;

/// How a client retries. `None` in place of a policy sends each call once.
#[derive(Debug, Clone, Copy)]
pub struct RetryOptions {
    /// A cap on the total calls including the first. Unset, each row's own count applies (4 for most).
    pub max_attempts: Option<u32>,
    /// A server hint above this is not waited for: the error comes back at once with retry_after_ms set.
    pub max_hint_ms: u64,
    /// Random source for the jitter, 0 ≤ r < 1.
    pub random: fn() -> f64,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_attempts: None,
            max_hint_ms: 30_000,
            random: default_random,
        }
    }
}

/// A retry row: the backoff base and cap, and the most attempts including the first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPlan {
    pub base_ms: u64,
    pub cap_ms: u64,
    pub max_attempts: u32,
}

/// The standard row: 250 ms doubling to 4 s, 4 attempts.
pub const STANDARD: RetryPlan = RetryPlan {
    base_ms: 250,
    cap_ms: 4000,
    max_attempts: 4,
};

const fn plan(base_ms: u64, cap_ms: u64, max_attempts: u32) -> RetryPlan {
    RetryPlan {
        base_ms,
        cap_ms,
        max_attempts,
    }
}

/// What a failed attempt looked like.
#[derive(Debug, Clone, Copy)]
pub struct RetryContext<'a> {
    pub method: &'a str,
    /// HTTP status, 0 for no answer.
    pub http_status: u16,
    /// JSON-RPC error code, None when the body had none.
    pub code: Option<i64>,
    pub message: &'a str,
    /// error.data as served.
    pub data: Option<&'a Value>,
    /// The caller's minContextSlot, for -32016.
    pub min_context_slot: Option<u64>,
}

fn field<'a>(data: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    data.and_then(|d| d.as_object()).and_then(|o| o.get(key))
}

/// Whether a failed call is retried and with which backoff; None means it is not sent again. Rules in order.
pub fn decide_retry(ctx: &RetryContext) -> Option<RetryPlan> {
    let (status, code, msg) = (ctx.http_status, ctx.code, ctx.message);
    if ctx.method == "sendTransaction" {
        return None; // a send may have landed: check the signature, never resend blindly
    }
    if status == 0 {
        return Some(STANDARD); // no answer: network error or timeout
    }
    if field(ctx.data, "retryable") == Some(&Value::Bool(false)) {
        return None; // the door says waiting cannot help
    }
    match status {
        429 => Some(if code == Some(-32010) {
            plan(100, 5000, 6)
        } else {
            STANDARD
        }),
        425 => Some(plan(0, 0, 2)), // early data refused, nothing was forwarded
        502 => (!msg.contains("response too large")).then_some(STANDARD),
        503 if msg.starts_with("upstream RPC unavailable") => Some(plan(2000, 8000, 4)),
        503 if msg == "compat verbs not initialised" => Some(plan(250, 4000, 3)),
        503 => Some(STANDARD),
        504 => (!msg.starts_with("upstream_slow")).then_some(plan(250, 4000, 2)),
        200 if field(ctx.data, "reason").is_some() => None, // a door policy answer at 200 is final
        200 => match code? {
            -32602 => msg
                .starts_with("the walk read no history")
                .then_some(STANDARD),
            -32016 => {
                let behind = match (
                    field(ctx.data, "contextSlot").and_then(Value::as_f64),
                    ctx.min_context_slot,
                ) {
                    (Some(slot), Some(min)) if min != 0 => min as f64 - slot,
                    _ => 1.0,
                };
                let wait = (220.0 * behind.max(1.0)).clamp(250.0, 30_000.0) as u64; // about 220 ms per missing slot
                Some(plan(wait, wait, 4))
            }
            -32004 => Some(plan(1000, 2000, 4)), // block not available yet
            -32014 => Some(plan(1000, 1000, 4)), // block status not yet available
            -32005 | -32021 | -32024 | -32026 => Some(STANDARD),
            -32011 => (msg == "das upstream timed out").then_some(STANDARD),
            -32603 => matches!(
                msg,
                "Internal error" | "das upstream refused the request" | "upstream internal error"
            )
            .then_some(plan(250, 4000, 2)),
            -32010 => msg
                .contains("peer refused the copy")
                .then_some(plan(20_000, 20_000, 2)),
            _ => None,
        },
        _ => None, // 400, 401, 402, 403, 413, 501 and anything unknown
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

fn digits(s: &str) -> Option<u64> {
    s.bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| s.parse().ok())
        .flatten()
}

/// The server's wait hint in ms: retry-after-ms, else data.retryAfterMs, else Retry-After (seconds or an HTTP date).
pub fn server_hint_ms(headers: &HeaderMap, data: Option<&Value>) -> Option<u64> {
    if let Some(ms) = header(headers, "retry-after-ms").and_then(digits) {
        return Some(ms);
    }
    if let Some(ms) = field(data, "retryAfterMs")
        .and_then(Value::as_f64)
        .filter(|ms| *ms >= 0.0)
    {
        return Some(ms as u64);
    }
    retry_after_header_ms(headers)
}

/// Milliseconds from a Retry-After header: whole seconds or an HTTP date.
pub(crate) fn retry_after_header_ms(headers: &HeaderMap) -> Option<u64> {
    let ra = header(headers, "retry-after")?;
    if let Some(s) = digits(ra) {
        return Some(s.saturating_mul(1000));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;
    http_date_ms(ra).map(|at| at.saturating_sub(now))
}

/// An IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) as Unix milliseconds.
fn http_date_ms(s: &str) -> Option<u64> {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() != 6 || p[5] != "GMT" {
        return None;
    }
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let (d, m, y): (i64, i64, i64) = (
        p[1].parse().ok()?,
        MONTHS.iter().position(|x| *x == p[2])? as i64 + 1,
        p[3].parse().ok()?,
    );
    let t: Vec<i64> = p[4]
        .split(':')
        .map(|x| x.parse().ok())
        .collect::<Option<_>>()?;
    if t.len() != 3
        || !(1..=31).contains(&d)
        || !(1970..=9999).contains(&y)
        || !(0..=23).contains(&t[0])
        || !(0..=59).contains(&t[1])
        || !(0..=60).contains(&t[2])
    {
        return None; // out of range: no hint, and no arithmetic on it
    }
    let y = if m <= 2 { y - 1 } else { y }; // days from 1970-01-01, H. Hinnant's civil-from-days inverse
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    u64::try_from((days * 86_400 + t[0] * 3600 + t[1] * 60 + t[2]) * 1000).ok()
}

/// The wait before retry number `attempt` (1 for the first retry): max(hint, backoff) plus 0 to 250 ms of jitter.
pub fn backoff_delay_ms(
    plan: &RetryPlan,
    attempt: u32,
    hint_ms: Option<u64>,
    random: fn() -> f64,
) -> u64 {
    let computed = plan
        .base_ms
        .saturating_mul(1u64 << (attempt.saturating_sub(1)).min(30))
        .min(plan.cap_ms);
    hint_ms
        .unwrap_or(0)
        .max(computed)
        .saturating_add((random() * 251.0).floor() as u64)
}

pub(crate) fn default_random() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    (h.finish() >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_http_date_reads_as_unix_time() {
        assert_eq!(
            http_date_ms("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777_000)
        );
        assert_eq!(http_date_ms("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        assert_eq!(http_date_ms("Sunday, 06-Nov-94 08:49:37 GMT"), None);
        assert_eq!(http_date_ms("Sun, 06 Nov 9999999999 08:49:37 GMT"), None);
        assert_eq!(http_date_ms("Sun, 06 Nov 1994 99:49:37 GMT"), None);
        assert_eq!(http_date_ms("Sun, 06 Nov 1994 08:60:37 GMT"), None);
        assert_eq!(
            http_date_ms("Fri, 31 Dec 9999 23:59:60 GMT"),
            Some(253_402_300_800_000)
        );
    }

    #[test]
    fn a_hint_of_u64_max_neither_overflows_nor_panics() {
        assert_eq!(
            backoff_delay_ms(&STANDARD, 1, Some(u64::MAX), || 0.999),
            u64::MAX
        );
        assert_eq!(backoff_delay_ms(&STANDARD, 200, None, || 0.0), 4000);
    }
}

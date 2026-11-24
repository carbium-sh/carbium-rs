//! A planted 429 with Retry-After waits at least the header before one retry; a planted 402 -32002 (credits
//! exhausted) is sent once. The numbered cases port the TypeScript SDK's retry tests, with the jitter set to zero
//! and wall-clock gaps read on the planted server.

mod common;

use carbium::{
    Carbium, ClientOptions, Code, Error, ErrorKind, JsonRpcCall, QuoteParams, QuoteParamsAmountIn,
    RetryContext, RetryOptions, RetryPlan, STANDARD, Value, decide_retry,
};
use common::{Planted, Reply, Step, key, rpc_err, rpc_ok};
use serde_json::json;

fn no_jitter() -> RetryOptions {
    RetryOptions {
        random: || 0.0,
        ..Default::default()
    }
}

fn client(server: &Planted, retry: Option<RetryOptions>) -> Carbium {
    Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: server.url.clone(),
        retry,
        ..Default::default()
    })
    .unwrap()
}

fn steps(replies: Vec<Reply>) -> Vec<Step> {
    replies.into_iter().map(Step::from).collect()
}

fn rate_limited(reason: &str) -> Option<Value> {
    Some(json!({ "reason": reason, "retryable": true }))
}

#[tokio::test]
async fn case_01_a_429_with_retry_after_2_waits_at_least_2_s_then_one_retry_succeeds() {
    let s = Planted::start(steps(vec![
        rpc_err(
            429,
            -32005,
            "rate limit exceeded",
            rate_limited("RATE_LIMITED"),
        )
        .header("retry-after", "2"),
        rpc_ok(json!(123)),
    ]))
    .await;
    let slot = client(&s, Some(no_jitter())).get_slot(None).await.unwrap();
    assert_eq!(slot.as_u64(), Some(123));
    assert_eq!(s.count(), 2);
    assert!(
        s.gap_ms(0) >= 2000,
        "waited {} ms, less than the 2000 ms header",
        s.gap_ms(0)
    );
    assert!(
        s.gap_ms(0) < 2600,
        "waited {} ms, far over the header",
        s.gap_ms(0)
    );
}

#[tokio::test]
async fn case_02_retry_after_ms_wins_over_retry_after() {
    let s = Planted::start(steps(vec![
        rpc_err(
            429,
            -32005,
            "rate limit exceeded",
            rate_limited("RATE_LIMITED"),
        )
        .header("retry-after", "2")
        .header("retry-after-ms", "1200"),
        rpc_ok(json!(1)),
    ]))
    .await;
    client(&s, Some(no_jitter())).get_slot(None).await.unwrap();
    assert_eq!(s.count(), 2);
    assert!(
        (1200..1800).contains(&s.gap_ms(0)),
        "waited {} ms",
        s.gap_ms(0)
    );
}

#[tokio::test]
async fn case_03_a_402_credits_exhausted_is_sent_once_even_with_retry_after() {
    let s = Planted::start(steps(vec![
        rpc_err(
            402,
            -32002,
            "credits exhausted",
            Some(json!({ "reason": "CREDITS_EXHAUSTED", "retryable": false })),
        )
        .header("retry-after", "1"),
        rpc_ok(json!(1)),
    ]))
    .await;
    let e = client(&s, Some(no_jitter()))
        .get_slot(None)
        .await
        .unwrap_err();
    assert_eq!(s.count(), 1, "the client sent the call again");
    assert_eq!(e.kind(), ErrorKind::Credits);
    assert_eq!(e.http_status(), 402);
    assert_eq!(e.code(), Some(&Code::Number(-32002)));
    assert_eq!(e.reason(), Some("CREDITS_EXHAUSTED"));
    assert!(!e.is_retryable());
    assert_eq!(e.retry_after_ms(), Some(1000));
    assert_eq!(e.request_id(), Some("rid-1"));
    assert!(
        e.to_string()
            .ends_with("credits exhausted (x-request-id: rid-1)"),
        "{e}"
    );
}

#[tokio::test]
async fn case_04_a_200_preflight_failure_is_a_node_error_and_is_not_retried() {
    let s = Planted::start(steps(vec![
        rpc_err(
            200,
            -32002,
            "Transaction simulation failed: Blockhash not found",
            Some(json!({ "err": "BlockhashNotFound", "logs": [] })),
        ),
        rpc_ok(json!(1)),
    ]))
    .await;
    let config = serde_json::from_value(json!({ "encoding": "base64" })).unwrap();
    let e = client(&s, Some(no_jitter()))
        .simulate_transaction("AQ==", Some(&config))
        .await
        .unwrap_err();
    assert_eq!(s.count(), 1);
    assert_eq!(e.kind(), ErrorKind::Node);
    assert_eq!(e.code(), Some(&Code::Number(-32002)));
    assert_eq!(e.reason(), None);
}

#[tokio::test]
async fn case_05_a_429_credits_pending_is_retried_after_retry_after_ms() {
    let s = Planted::start(steps(vec![
        rpc_err(
            429,
            -32005,
            "credits are still loading",
            rate_limited("CREDITS_PENDING"),
        )
        .header("retry-after-ms", "300")
        .header("retry-after", "1"),
        rpc_ok(json!("ok")),
    ]))
    .await;
    assert_eq!(
        client(&s, Some(no_jitter())).get_health().await.unwrap(),
        "ok"
    );
    assert_eq!(s.count(), 2);
    assert!(
        (300..900).contains(&s.gap_ms(0)),
        "waited {} ms",
        s.gap_ms(0)
    );
}

#[tokio::test]
async fn case_06_a_503_that_never_clears_is_tried_4_times_then_returned_as_upstream() {
    let s = Planted::start(steps(vec![
        rpc_err(503, -32005, "overloaded", rate_limited("LOAD_SHED"))
            .header("retry-after-ms", "100"),
    ]))
    .await;
    let e = client(&s, Some(no_jitter()))
        .get_slot(None)
        .await
        .unwrap_err();
    assert_eq!(s.count(), 4);
    assert_eq!(
        (e.kind(), e.http_status(), e.is_retryable()),
        (ErrorKind::Upstream, 503, true)
    );
}

#[tokio::test]
async fn case_07_send_transaction_is_never_retried_even_on_a_503() {
    let s = Planted::start(steps(vec![
        rpc_err(503, -32011, "upstream RPC unavailable, retry later", None)
            .header("retry-after", "1"),
        rpc_ok(json!("sig")),
    ]))
    .await;
    let config = serde_json::from_value(json!({ "encoding": "base64" })).unwrap();
    let e = client(&s, Some(no_jitter()))
        .send_transaction("AQ==", Some(&config))
        .await
        .unwrap_err();
    assert_eq!(s.count(), 1);
    assert_eq!(e.kind(), ErrorKind::Upstream);
    assert_eq!(e.method(), Some("sendTransaction"));
}

#[tokio::test]
async fn case_08_no_retry_policy_sends_once_and_returns_the_429() {
    let s = Planted::start(steps(vec![
        rpc_err(
            429,
            -32005,
            "rate limit exceeded",
            rate_limited("RATE_LIMITED"),
        )
        .header("retry-after-ms", "50"),
        rpc_ok(json!(1)),
    ]))
    .await;
    let e = client(&s, None).get_slot(None).await.unwrap_err();
    assert_eq!(s.count(), 1);
    assert_eq!(
        (e.kind(), e.retry_after_ms()),
        (ErrorKind::RateLimit, Some(50))
    );
}

#[tokio::test]
async fn case_09_a_hint_above_max_hint_ms_is_not_waited_for_and_the_error_carries_it() {
    let s = Planted::start(steps(vec![
        rpc_err(
            503,
            -32005,
            "served from cache only; the cache is filling, retry after Retry-After",
            None,
        )
        .header("retry-after", "60"),
        rpc_ok(json!(1)),
    ]))
    .await;
    let e = client(&s, Some(no_jitter()))
        .get_supply(None)
        .await
        .unwrap_err();
    assert_eq!(s.count(), 1);
    assert_eq!(e.retry_after_ms(), Some(60_000));
    assert!(e.is_retryable());
}

#[tokio::test]
async fn case_12_the_scan_index_row_gets_6_tries_by_default_and_the_caller_cap_lowers_it() {
    let row = rpc_err(
        429,
        -32010,
        "scan index temporarily unavailable, retry",
        rate_limited("CHOT_INDEX_UNAVAILABLE"),
    )
    .header("retry-after-ms", "100");
    let s = Planted::start(steps(vec![row])).await;
    let e = client(&s, Some(no_jitter()))
        .get_program_accounts("p", None)
        .await
        .unwrap_err();
    assert_eq!((e.kind(), s.count()), (ErrorKind::RateLimit, 6));
    let capped = RetryOptions {
        max_attempts: Some(2),
        ..no_jitter()
    };
    client(&s, Some(capped))
        .get_program_accounts("p", None)
        .await
        .unwrap_err();
    assert_eq!(s.count(), 8);
}

#[tokio::test]
async fn case_13_minus_32016_waits_about_220_ms_per_missing_slot_read_from_the_call() {
    let s = Planted::start(steps(vec![rpc_err(
        200,
        -32016,
        "Minimum context slot has not been reached",
        Some(json!({ "contextSlot": 100 })),
    )]))
    .await;
    let config = serde_json::from_value(json!({ "minContextSlot": 105 })).unwrap();
    let e = client(
        &s,
        Some(RetryOptions {
            max_attempts: Some(2),
            ..no_jitter()
        }),
    )
    .get_account_info("x", Some(&config))
    .await
    .unwrap_err();
    assert_eq!(
        (e.kind(), e.code()),
        (ErrorKind::Node, Some(&Code::Number(-32016)))
    );
    assert_eq!(s.count(), 2);
    assert!(
        (1100..1700).contains(&s.gap_ms(0)),
        "waited {} ms for 5 missing slots",
        s.gap_ms(0)
    );
}

#[tokio::test]
async fn case_14_a_client_timeout_is_tried_once_more_then_returned_as_transport_with_its_cause() {
    let s = Planted::start(steps(vec![rpc_ok(json!(1)).delay_ms(1500)])).await;
    let options = ClientOptions {
        api_key: Some(key()),
        endpoint: s.url.clone(),
        retry: Some(no_jitter()),
        timeout: std::time::Duration::from_millis(150),
    };
    let e: Error = Carbium::new(options)
        .unwrap()
        .get_slot(None)
        .await
        .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Transport);
    assert!(e.message().contains("timed out after 150 ms"), "{e}");
    assert!(
        std::error::Error::source(&e).is_some(),
        "the cause was dropped"
    );
    assert_eq!(s.count(), 2);
}

fn row(
    method: &str,
    status: u16,
    code: Option<i64>,
    message: &str,
    data: Option<Value>,
    min_context_slot: Option<u64>,
) -> Option<RetryPlan> {
    decide_retry(&RetryContext {
        method,
        http_status: status,
        code,
        message,
        data: data.as_ref(),
        min_context_slot,
    })
}

const fn p(base_ms: u64, cap_ms: u64, max_attempts: u32) -> Option<RetryPlan> {
    Some(RetryPlan {
        base_ms,
        cap_ms,
        max_attempts,
    })
}

#[test]
fn every_row_of_the_retry_table() {
    let std = Some(STANDARD);
    let reason = Some(json!({ "reason": "RATE_LIMITED" }));
    let rows: Vec<(&str, Option<RetryPlan>, Option<RetryPlan>)> = vec![
        (
            "sendTransaction on a 503",
            row("sendTransaction", 503, None, "", None, None),
            None,
        ),
        (
            "sendTransaction with no answer",
            row("sendTransaction", 0, None, "", None, None),
            None,
        ),
        ("no answer", row("getSlot", 0, None, "", None, None), std),
        (
            "retryable false on a 503",
            row(
                "getSlot",
                503,
                None,
                "",
                Some(json!({ "retryable": false })),
                None,
            ),
            None,
        ),
        (
            "retryable false on a 429",
            row(
                "getSlot",
                429,
                Some(-32005),
                "",
                Some(json!({ "retryable": false })),
                None,
            ),
            None,
        ),
        (
            "429 -32010",
            row("getSlot", 429, Some(-32010), "", None, None),
            p(100, 5000, 6),
        ),
        (
            "429 other",
            row("getSlot", 429, Some(-32005), "", None, None),
            std,
        ),
        ("425", row("getSlot", 425, None, "", None, None), p(0, 0, 2)),
        (
            "502 too large",
            row("getSlot", 502, None, "response too large", None, None),
            None,
        ),
        (
            "502",
            row("getSlot", 502, None, "bad gateway", None, None),
            std,
        ),
        (
            "503 upstream RPC unavailable",
            row(
                "getSlot",
                503,
                None,
                "upstream RPC unavailable, retry later",
                None,
                None,
            ),
            p(2000, 8000, 4),
        ),
        (
            "503 compat verbs",
            row(
                "getSlot",
                503,
                None,
                "compat verbs not initialised",
                None,
                None,
            ),
            p(250, 4000, 3),
        ),
        (
            "503",
            row("getSlot", 503, None, "overloaded", None, None),
            std,
        ),
        (
            "504 upstream_slow",
            row("getSlot", 504, None, "upstream_slow: scan", None, None),
            None,
        ),
        (
            "504",
            row("getSlot", 504, None, "timeout", None, None),
            p(250, 4000, 2),
        ),
        (
            "200 with a reason",
            row("getSlot", 200, Some(-32005), "", reason, None),
            None,
        ),
        (
            "200 -32602 walk",
            row(
                "getSlot",
                200,
                Some(-32602),
                "the walk read no history yet",
                None,
                None,
            ),
            std,
        ),
        (
            "200 -32602 other",
            row("getSlot", 200, Some(-32602), "invalid params", None, None),
            None,
        ),
        (
            "200 -32016 five behind",
            row(
                "getSlot",
                200,
                Some(-32016),
                "",
                Some(json!({ "contextSlot": 100 })),
                Some(105),
            ),
            p(1100, 1100, 4),
        ),
        (
            "200 -32016 no slot",
            row("getSlot", 200, Some(-32016), "", None, None),
            p(250, 250, 4),
        ),
        (
            "200 -32016 far behind",
            row(
                "getSlot",
                200,
                Some(-32016),
                "",
                Some(json!({ "contextSlot": 0 })),
                Some(1_000_000),
            ),
            p(30_000, 30_000, 4),
        ),
        (
            "200 -32004",
            row("getSlot", 200, Some(-32004), "", None, None),
            p(1000, 2000, 4),
        ),
        (
            "200 -32014",
            row("getSlot", 200, Some(-32014), "", None, None),
            p(1000, 1000, 4),
        ),
        (
            "200 -32005",
            row("getSlot", 200, Some(-32005), "", None, None),
            std,
        ),
        (
            "200 -32021",
            row("getSlot", 200, Some(-32021), "", None, None),
            std,
        ),
        (
            "200 -32024",
            row("getSlot", 200, Some(-32024), "", None, None),
            std,
        ),
        (
            "200 -32026",
            row("getSlot", 200, Some(-32026), "", None, None),
            std,
        ),
        (
            "200 -32011 das timeout",
            row(
                "getAsset",
                200,
                Some(-32011),
                "das upstream timed out",
                None,
                None,
            ),
            std,
        ),
        (
            "200 -32011 other",
            row(
                "getAsset",
                200,
                Some(-32011),
                "das upstream timed out!",
                None,
                None,
            ),
            None,
        ),
        (
            "200 -32603 internal",
            row("getSlot", 200, Some(-32603), "Internal error", None, None),
            p(250, 4000, 2),
        ),
        (
            "200 -32603 das refused",
            row(
                "getSlot",
                200,
                Some(-32603),
                "das upstream refused the request",
                None,
                None,
            ),
            p(250, 4000, 2),
        ),
        (
            "200 -32603 upstream internal",
            row(
                "getSlot",
                200,
                Some(-32603),
                "upstream internal error",
                None,
                None,
            ),
            p(250, 4000, 2),
        ),
        (
            "200 -32603 other",
            row(
                "getSlot",
                200,
                Some(-32603),
                "Internal error: x",
                None,
                None,
            ),
            None,
        ),
        (
            "200 -32010 copy",
            row(
                "getSlot",
                200,
                Some(-32010),
                "the peer refused the copy",
                None,
                None,
            ),
            p(20_000, 20_000, 2),
        ),
        (
            "200 -32010 other",
            row("getSlot", 200, Some(-32010), "scan index", None, None),
            None,
        ),
        (
            "200 unknown code",
            row("getSlot", 200, Some(-1), "", None, None),
            None,
        ),
        (
            "200 no code",
            row("getSlot", 200, None, "", None, None),
            None,
        ),
        ("400", row("getSlot", 400, None, "", None, None), None),
        ("401", row("getSlot", 401, None, "", None, None), None),
        (
            "402",
            row("getSlot", 402, Some(-32002), "", None, None),
            None,
        ),
        ("403", row("getSlot", 403, None, "", None, None), None),
        ("413", row("getSlot", 413, None, "", None, None), None),
        ("501", row("getSlot", 501, None, "", None, None), None),
    ];
    let wrong: Vec<String> = rows
        .iter()
        .filter(|(_, got, want)| got != want)
        .map(|(name, got, want)| format!("{name}: {got:?}, want {want:?}"))
        .collect();
    assert_eq!(wrong, Vec::<String>::new());
}

#[tokio::test]
async fn retryable_false_on_a_503_is_sent_once() {
    let s = Planted::start(steps(vec![rpc_err(
        503,
        -32005,
        "overloaded",
        Some(json!({ "reason": "LOAD_SHED", "retryable": false })),
    )]))
    .await;
    let e = client(&s, Some(no_jitter()))
        .get_slot(None)
        .await
        .unwrap_err();
    assert_eq!((s.count(), e.is_retryable()), (1, false));
}

#[tokio::test]
async fn the_data_retry_after_ms_hint_is_waited_for() {
    let s = Planted::start(steps(vec![
        rpc_err(
            503,
            -32005,
            "overloaded",
            Some(json!({ "retryAfterMs": 700 })),
        ),
        rpc_ok(json!(1)),
    ]))
    .await;
    client(&s, Some(no_jitter())).get_slot(None).await.unwrap();
    assert!(
        (700..1300).contains(&s.gap_ms(0)),
        "waited {} ms",
        s.gap_ms(0)
    );
}

#[tokio::test]
async fn send_transaction_is_sent_once_after_a_timeout_and_inside_a_batch() {
    let s = Planted::start(steps(vec![rpc_ok(json!("sig")).delay_ms(1500)])).await;
    let options = ClientOptions {
        api_key: Some(key()),
        endpoint: s.url.clone(),
        retry: Some(no_jitter()),
        timeout: std::time::Duration::from_millis(150),
    };
    let e = Carbium::new(options)
        .unwrap()
        .send_transaction("AQ==", None)
        .await
        .unwrap_err();
    assert_eq!((e.kind(), s.count()), (ErrorKind::Transport, 1));
    let b = Planted::start(steps(vec![
        rpc_err(503, -32005, "overloaded", None).header("retry-after-ms", "100"),
    ]))
    .await;
    let calls = [
        JsonRpcCall::new("getSlot", None),
        JsonRpcCall::new("sendTransaction", Some(json!(["AQ=="]))),
    ];
    let e = client(&b, Some(no_jitter()))
        .batch(&calls)
        .await
        .unwrap_err();
    assert_eq!((e.method(), b.count()), (Some("sendTransaction"), 1));
}

#[tokio::test]
async fn a_hint_of_u64_max_comes_back_at_once_on_both_paths() {
    let max = u64::MAX.to_string();
    let s = Planted::start(steps(vec![
        rpc_err(
            429,
            -32005,
            "rate limit exceeded",
            rate_limited("RATE_LIMITED"),
        )
        .header("retry-after-ms", &max),
    ]))
    .await;
    let t0 = std::time::Instant::now();
    let e = client(&s, Some(no_jitter()))
        .get_slot(None)
        .await
        .unwrap_err();
    assert_eq!((e.retry_after_ms(), s.count()), (Some(u64::MAX), 1));
    let at_200 = Planted::start(steps(vec![rpc_err(
        200,
        -32005,
        "busy",
        Some(json!({ "retryAfterMs": 30_001 })),
    )]))
    .await;
    let e = client(&at_200, Some(no_jitter()))
        .get_slot(None)
        .await
        .unwrap_err();
    assert_eq!(
        (e.retry_after_ms(), at_200.count()),
        (Some(30_001), 1),
        "a 200 hint over max_hint_ms comes back at once"
    );
    assert!(
        t0.elapsed() < std::time::Duration::from_secs(2),
        "took {:?}",
        t0.elapsed()
    );
}

#[tokio::test]
async fn a_quote_with_no_route_under_the_account_cap_is_a_route_error_sent_once() {
    let s = Planted::start(steps(vec![
        rpc_err(
            200,
            -32055,
            "no route fits the account cap",
            Some(json!({ "why": "accounts_over_cap" })),
        ),
        rpc_ok(json!({})),
    ]))
    .await;
    let mut ask = QuoteParams::new(
        "So11111111111111111111111111111111111111112",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        QuoteParamsAmountIn::Text("1".into()),
        50,
    );
    ask.swap_tag = Some(1);
    let e = client(&s, Some(no_jitter())).quote(&ask).await.unwrap_err();
    assert_eq!(
        (
            e.kind(),
            e.code(),
            e.http_status(),
            e.is_retryable(),
            s.count()
        ),
        (ErrorKind::Route, Some(&Code::Number(-32055)), 200, false, 1)
    );
}

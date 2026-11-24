//! The REST quote, GET /cq1/quote, against a planted server: what it sends, the plain-text 400, a retried 429 and a
//! key refusal in the door's JSON-RPC error shape.
#![allow(deprecated)] // the old quote path is still served and still tested

mod common;

use carbium::{Carbium, ClientOptions, Code, ErrorKind, RetryOptions};
use common::{Planted, Reply, key, rpc_err};
use serde_json::{Value, json};

const SOL: &str = "So11111111111111111111111111111111111111112";
const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

fn client(s: &Planted) -> Carbium {
    let retry = RetryOptions {
        random: || 0.0,
        ..Default::default()
    };
    Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: s.url.clone(),
        retry: Some(retry),
        ..Default::default()
    })
    .unwrap()
}

fn answer() -> Value {
    let leg = json!({
        "swapInfo": { "ammKey": "k", "label": "Pool", "inputMint": SOL, "outputMint": USDC, "inAmount": "1000000000", "outAmount": "150000000", "feeAmount": "25", "feeMint": SOL },
        "percent": "100", "bps": "10000"
    });
    json!({
        "srcAmountIn": "1000000000", "destAmountOut": "150000000", "destAmountOutMin": "149250000", "swapMode": "ExactIn",
        "slippage": "50", "priceImpactPct": "0.01", "routePlan": [leg], "feeMint": SOL, "feeBps": "0",
        "signatureFeeLamports": "5000", "prioritizationFeeLamports": "0", "rentFeeLamports": "0", "inputMint": SOL,
        "outputMint": USDC, "swapType": "aggregator", "router": "cq1", "requestId": "r-1", "priceImpact": "0.0001",
        "aFieldAddedLater": "kept"
    })
}

#[tokio::test]
async fn it_gets_the_route_with_the_four_params_and_the_key_in_its_header_only() {
    let s = Planted::start(vec![Reply::json(200, answer()).into()]).await;
    let q = client(&s)
        .cq1_quote(SOL, USDC, 1_000_000_000, 50)
        .await
        .unwrap();
    assert_eq!(
        (
            q.dest_amount_out.as_str(),
            q.route_plan[0].swap_info.label.as_str()
        ),
        ("150000000", "Pool")
    );
    assert_eq!(
        q.extra.get("aFieldAddedLater"),
        Some(&json!("kept")),
        "an extra field is tolerated and kept"
    );
    let req = &s.seen()[0];
    assert_eq!(req.method, "GET");
    assert_eq!(
        req.path,
        format!("/cq1/quote?src_mint={SOL}&dst_mint={USDC}&amount_in=1000000000&slippage_bps=50")
    );
    assert_eq!(req.headers.get("x-api-key"), Some(&key()));
    assert!(
        req.headers
            .get("solana-client")
            .is_some_and(|v| v.starts_with("rust/carbium@"))
    );
    assert!(!req.path.contains("c_serv_"), "the key went into the URL");
    assert_eq!(req.body, "");
}

#[tokio::test]
async fn a_parameter_fault_is_a_plain_text_400_read_as_a_request_error_and_redacted() {
    let text = format!(
        "Failed to deserialize query string: missing field `dst_mint` (key {})",
        key()
    );
    let s = Planted::start(vec![
        Reply::raw(400, &text)
            .header("content-type", "text/plain; charset=utf-8")
            .into(),
    ])
    .await;
    let e = client(&s).cq1_quote(SOL, "", 1, 50).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.http_status(), s.count()),
        (ErrorKind::Request, 400, 1)
    );
    assert!(
        e.message()
            .starts_with("Failed to deserialize query string: missing field `dst_mint`"),
        "{e}"
    );
    assert!(
        e.message().contains("c_serv_…") && !e.message().contains(&key()),
        "{e}"
    );
}

#[tokio::test]
async fn a_429_is_retried_after_its_hint_as_the_quote_is() {
    let busy = rpc_err(
        429,
        -32005,
        "rate limit exceeded",
        Some(json!({ "reason": "RATE_LIMITED", "retryable": true })),
    )
    .header("retry-after-ms", "300");
    let s = Planted::start(vec![busy.into(), Reply::json(200, answer()).into()]).await;
    let q = client(&s).cq1_quote(SOL, USDC, 1, 50).await.unwrap();
    assert_eq!((q.request_id.as_str(), s.count()), ("r-1", 2));
    assert!(
        (300..900).contains(&s.gap_ms(0)),
        "waited {} ms",
        s.gap_ms(0)
    );
}

#[tokio::test]
async fn a_key_refusal_in_the_json_rpc_shape_maps_as_for_the_quote() {
    let s = Planted::start(vec![
        rpc_err(
            401,
            -32001,
            "unknown api key",
            Some(json!({ "reason": "UNKNOWN_KEY" })),
        )
        .into(),
    ])
    .await;
    let e = client(&s).cq1_quote(SOL, USDC, 1, 50).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.code(), e.reason(), s.count()),
        (
            ErrorKind::Auth,
            Some(&Code::Number(-32001)),
            Some("UNKNOWN_KEY"),
            1
        )
    );
    assert_eq!(e.request_id(), Some("rid-1"));
}

#[tokio::test]
async fn a_pair_with_no_route_is_a_422_route_error_and_is_not_retried() {
    let s = Planted::start(vec![
        Reply::raw(422, "No valid routes found")
            .header("content-type", "text/plain")
            .into(),
    ])
    .await;
    let e = client(&s)
        .cq1_quote(SOL, "not-a-mint", 1, 50)
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind(), e.http_status(), s.count()),
        (ErrorKind::Route, 422, 1)
    );
    assert!(e.message().starts_with("No valid routes found"), "{e}");
}

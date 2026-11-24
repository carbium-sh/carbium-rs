//! Decoding against live-shaped answers: the fields the rebuilt spec fixed, and the tolerance rules. An extra field
//! never fails, an unknown enum value is kept, and a wrong type names its field path with the raw answer in data.

mod common;

use carbium::{
    Carbium, ClientOptions, Commitment, ErrorKind, PricesParams, QuoteParams, QuoteParamsAmountIn,
    QuoteParamsIntegratorFeeBps, Value,
};
use common::{Planted, key, rpc_ok};
use serde_json::json;

async fn answer(result: Value) -> (Planted, Carbium) {
    let s = Planted::start(vec![rpc_ok(result).into()]).await;
    let c = Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: s.url.clone(),
        retry: None,
        ..Default::default()
    })
    .unwrap();
    (s, c)
}

#[tokio::test]
async fn prices_and_health_read_stale_as_a_count() {
    let prices = json!({ "solUsd": 150.5, "builtUnix": 1_763_000_000, "priced": 2, "stale": 0, "noroute": 0, "total": 2,
        "rows": [["So11111111111111111111111111111111111111112", 150.5, 1.0e9, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]] });
    let (_s, c) = answer(prices).await;
    let p = c.prices(&PricesParams::new()).await.unwrap();
    assert_eq!((p.stale.as_u64(), p.rows.len()), (Some(0), 1));
    let health: Value = serde_json::from_str(include_health()).unwrap();
    let (_s, c) = answer(health).await;
    let h = c.health().await.unwrap();
    assert_eq!(h.stale.as_u64(), Some(0));
}

/// A health answer as the service sends it, fields beyond the typed ones included.
fn include_health() -> &'static str {
    r#"{"ok": true, "quoteUpstream": {"ok": true}, "canBuild": true, "families": ["amm"], "poolStore": {"ok": true},
        "chainRpc": {"ok": true}, "priced": 2, "total": 2, "stale": 0, "tableAgeSecs": 3, "quote_refusal_ratio": 0.01,
        "aNewField": "kept"}"#
}

#[tokio::test]
async fn account_data_sent_as_a_bare_base58_string_reads() {
    let info = json!({ "context": { "slot": 1 }, "value": { "lamports": 1, "owner": "11111111111111111111111111111111", "data": "3Bxs4h24hBtQy9rw", "executable": false, "rentEpoch": 0, "space": 12 } });
    let (_s, c) = answer(info).await;
    let a = c.get_account_info("x", None).await.unwrap().value.unwrap();
    assert_eq!(
        serde_json::to_value(&a.data).unwrap(),
        json!("3Bxs4h24hBtQy9rw")
    );
}

#[tokio::test]
async fn an_extra_field_never_fails_and_an_unknown_enum_value_is_kept() {
    let statuses = json!({ "context": { "slot": 9, "brandNew": true }, "value": [{ "slot": 8, "confirmations": null, "err": null, "confirmationStatus": "rooted", "extra": 1 }] });
    let (_s, c) = answer(statuses).await;
    let v = c
        .get_signature_statuses(&["sig".into()], None)
        .await
        .unwrap();
    let status = v.value[0].as_ref().unwrap();
    assert_eq!(
        status.confirmation_status,
        Some(Commitment::Other("rooted".into()))
    );
}

#[tokio::test]
async fn a_wrong_type_names_its_field_path_and_keeps_the_raw_answer() {
    let raw =
        json!({ "context": { "slot": 1 }, "value": { "blockhash": 7, "lastValidBlockHeight": 2 } });
    let (_s, c) = answer(raw.clone()).await;
    let e = c.get_latest_blockhash(None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Upstream);
    assert!(e.message().contains("at `value.blockhash`"), "{e}");
    assert_eq!(e.data(), Some(&raw));
}

#[tokio::test]
async fn a_quote_sends_the_account_cap_and_the_integrator_fee_by_their_long_names_and_types_the_fee()
 {
    const SOL: &str = "So11111111111111111111111111111111111111112";
    const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    const WALLET: &str = "11111111111111111111111111111111";
    let quote = json!({ "srcAmountIn": "100000000", "destAmountOut": "14250000", "minAmountOut": "14178750", "slippageBps": 50,
        "swapMode": "ExactIn", "inputMint": SOL, "outputMint": USDC, "routePlan": [], "quoteId": "q",
        "integratorFee": { "amount": "35625", "feeBps": 25, "mint": USDC, "account": WALLET } });
    let s = Planted::start(vec![rpc_ok(quote.clone()).into(), rpc_ok(quote).into()]).await;
    let c = Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: s.url.clone(),
        retry: None,
        ..Default::default()
    })
    .unwrap();
    let plain = QuoteParams::new(SOL, USDC, QuoteParamsAmountIn::Text("100000000".into()), 50);
    let mut ask = plain.clone();
    ask.max_accounts = Some(8);
    ask.integrator_fee_bps = Some(QuoteParamsIntegratorFeeBps::Number(25));
    ask.integrator_fee_account = Some(WALLET.into());
    let q = c.quote(&ask).await.unwrap();
    c.quote(&plain).await.unwrap();
    let base =
        json!({ "src_mint": SOL, "dst_mint": USDC, "amount_in": "100000000", "slippage_bps": 50 });
    let mut asked = base.clone();
    asked["maxAccounts"] = json!(8);
    asked["integrator_fee_bps"] = json!(25);
    asked["integrator_fee_account"] = json!(WALLET);
    let sent: Vec<Value> = s
        .seen()
        .iter()
        .map(|r| r.json()["params"].clone())
        .collect();
    assert_eq!(sent, vec![asked, base]);
    let fee = q.integrator_fee.as_ref().unwrap();
    assert_eq!(
        (
            fee.amount.as_str(),
            fee.fee_bps.as_u64(),
            fee.mint.as_str(),
            fee.account.as_str()
        ),
        ("35625", Some(25), USDC, WALLET)
    );
    assert!(
        !q.extra.contains_key("integratorFee"),
        "the fee is typed, not left in extra"
    );
}

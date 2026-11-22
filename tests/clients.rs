//! The account, DCA and token clients against planted servers: the TypeScript SDK's client cases, the route each
//! function takes, and keys kept out of Debug output.

mod common;

use carbium::account::{AccountOptions, CreatedKey};
use carbium::dca::{
    DcaAmount, DcaFrequency, DcaInterval, DcaOpenRequest, DcaOptions, DcaQuoteRequest,
    DcaSendRequest, DcaSent, DcaWithdrawRequest,
};
use carbium::tokens::{TokenSearchOptions, TokensOptions};
use carbium::{CarbiumAccount, CarbiumDca, CarbiumTokens, Code, ErrorKind};
use common::{Planted, Reply, account_key, key};
use serde_json::json;

fn service_key_record(id: u64) -> serde_json::Value {
    json!({
        "key_id": id, "prefix": null, "name": null, "tier": "DEV", "scope": ["rpc"], "status": "active",
        "per_method_caps": [], "max_connections": null, "max_inflight": null, "ip_allow": [], "created_at": null,
        "rotated_at": null, "revoked_at": null
    })
}

#[tokio::test]
async fn account_api_sends_bearer_maps_json_and_text_errors_and_takes_the_test_route() {
    let s = Planted::start(vec![
        Reply::json(200, json!([service_key_record(1)])).into(),
        Reply::json(429, json!({ "error": "rate_limited", "message": "too many calls: wait Retry-After seconds" })).header("retry-after", "37").into(),
        Reply::raw(415, "Expected request with `Content-Type: application/json`").header("content-type", "text/plain").into(),
        Reply::json(200, json!({ "event_id": "evt_test_1", "outcome": "delivered", "http_status": 200, "reason": "", "latency_ms": 90, "refusal": null, "ts": 1 })).into(),
    ])
    .await;
    let a = CarbiumAccount::new(AccountOptions {
        base_url: s.url.clone(),
        ..AccountOptions::new(account_key())
    })
    .unwrap();
    let keys = a.keys().list().await.unwrap();
    assert_eq!(keys[0].key_id, 1);
    let first = &s.seen()[0];
    assert_eq!(
        first.headers.get("authorization"),
        Some(&format!("Bearer {}", account_key()))
    );
    assert_eq!(
        (first.method.as_str(), first.path.as_str()),
        ("GET", "/v1/keys")
    );
    let e = a.account().balance().await.unwrap_err();
    assert_eq!(
        (e.kind(), e.http_status(), e.code()),
        (
            ErrorKind::Account,
            429,
            Some(&Code::Text("rate_limited".into()))
        )
    );
    assert_eq!(
        (e.retry_after_ms(), e.request_id()),
        (Some(37_000), Some("rid-2"))
    );
    let e = a.keys().create(Some("x"), None).await.unwrap_err();
    assert_eq!((e.kind(), e.http_status()), (ErrorKind::Account, 415));
    assert!(e.message().contains("Content-Type"), "{e}");
    assert_eq!(s.seen()[2].json(), json!({ "name": "x" }));
    let r = a.webhooks().test(7).await.unwrap();
    assert_eq!(r.outcome, "delivered");
    assert_eq!(
        (s.seen()[3].method.as_str(), s.seen()[3].path.as_str()),
        ("POST", "/v1/webhooks/7/test")
    );
    assert_eq!(
        CarbiumAccount::new(AccountOptions::new(key()))
            .unwrap_err()
            .kind(),
        ErrorKind::Auth
    );
    assert!(!format!("{a:?}").contains(&account_key()), "{a:?}");
}

#[tokio::test]
async fn account_routes_carry_their_query_and_a_shown_once_key_stays_out_of_debug() {
    let s = Planted::start(vec![Reply::json(200, json!([])).into()]).await;
    let a = CarbiumAccount::new(AccountOptions {
        base_url: format!("{}/", s.url),
        ..AccountOptions::new(account_key())
    })
    .unwrap();
    let q = carbium::account::UsageQuery {
        from: Some("2026-11-01".into()),
        key_id: Some(3),
        ..Default::default()
    };
    a.usage().daily(&q).await.unwrap();
    a.activity(9, Some(5), None).await.unwrap();
    a.webhooks()
        .deliveries(
            4,
            &carbium::account::PageQuery {
                limit: Some(2),
                before: Some("c 1".into()),
            },
        )
        .await
        .unwrap();
    let paths: Vec<String> = s.seen().iter().map(|r| r.path.clone()).collect();
    assert_eq!(
        paths,
        [
            "/v1/usage?from=2026-11-01&key_id=3",
            "/v1/account/api-keys/9/activity?limit=5",
            "/v1/webhooks/4/deliveries?limit=2&before=c+1"
        ]
    );
    let created: CreatedKey = serde_json::from_value(
        json!({ "key_id": 2, "prefix": "c_serv_ab", "raw_key": key(), "name": null }),
    )
    .unwrap();
    assert!(!format!("{created:?}").contains(&key()), "{created:?}");
    assert_eq!(created.raw_key.expose(), key());
}

#[tokio::test]
async fn dca_api_maps_ok_false_sends_amounts_as_strings_and_the_key_in_x_api_key() {
    let build = json!({
        "ok": true, "txn_base64": "AQ==", "blockhash": "h", "last_valid_block_height": "5", "position": "p", "cost": {},
        "summary": { "kind": "open", "owner": "o", "feePayer": "o", "instructions": [] }, "authority": "a", "escrowAccount": "e", "index": 0, "plan": {}
    });
    let s = Planted::start(vec![
        Reply::json(400, json!({ "ok": false, "err": "same_mint", "detail": "inputMint and outputMint are the same" })).into(),
        Reply::json(200, build).into(),
        Reply::json(429, json!({ "ok": false, "err": "rate_limited", "detail": "slow down" })).header("retry-after", "3").into(),
        Reply::json(200, json!({ "ok": true, "keeper": "k" })).into(),
    ])
    .await;
    let dca_key = ["dca-test", "-key-1"].concat();
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        api_key: Some(dca_key.clone()),
        ..Default::default()
    })
    .unwrap();
    let open = |input: &str, output: &str, amount, interval| DcaOpenRequest {
        owner: "o".into(),
        input_mint: input.into(),
        output_mint: output.into(),
        cycles: 2,
        amount,
        interval,
        max_slippage_bps: None,
        start_at: None,
        index: None,
        min_out_e6: None,
        max_price: None,
    };
    let e = d
        .open(&open(
            "m",
            "m",
            DcaAmount::PerCycle(10),
            DcaInterval::Frequency(DcaFrequency::Daily),
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind(), e.code(), e.http_status(), e.is_retryable()),
        (
            ErrorKind::Dca,
            Some(&Code::Text("same_mint".into())),
            400,
            false
        )
    );
    assert_eq!(
        s.seen()[0].json(),
        json!({ "owner": "o", "inputMint": "m", "outputMint": "m", "cycles": 2, "amountPerCycle": "10", "frequency": "daily" })
    );
    let built = d
        .open(&open(
            "a",
            "b",
            DcaAmount::Total(12_345_678_901_234_567_890),
            DcaInterval::Seconds(3600),
        ))
        .await
        .unwrap();
    assert_eq!(
        (
            built.build.txn_base64.as_str(),
            built.escrow_account.as_str()
        ),
        ("AQ==", "e")
    );
    let sent = &s.seen()[1];
    assert_eq!(sent.json()["totalAmount"], json!("12345678901234567890"));
    assert_eq!(sent.json()["intervalSecs"], json!(3600));
    assert_eq!(
        (sent.headers.get("x-api-key"), sent.path.as_str()),
        (Some(&dca_key), "/open")
    );
    let e = d
        .quote(&DcaQuoteRequest {
            input_mint: "a".into(),
            output_mint: "b".into(),
            amount: 5,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(
        (e.code(), e.is_retryable(), e.retry_after_ms()),
        (Some(&Code::Text("rate_limited".into())), true, Some(3000))
    );
    assert_eq!(s.seen()[2].path, "/quote?inputMint=a&outputMint=b&amount=5");
    assert_eq!(d.health().await.unwrap().keeper, "k");
    assert!(!format!("{d:?}").contains(&dca_key), "{d:?}");
}

#[tokio::test]
async fn dca_send_relays_signed_bytes_once_and_is_never_retried() {
    let s = Planted::start(vec![
        Reply::json(
            503,
            json!({ "ok": false, "err": "door_busy", "detail": "busy" }),
        )
        .header("retry-after", "1")
        .into(),
    ])
    .await;
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let e = d
        .send(&DcaSendRequest {
            txn_base64: "AQ==".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind(), e.is_retryable(), s.count()),
        (ErrorKind::Dca, true, 1)
    );
    assert_eq!(
        (s.seen()[0].path.as_str(), s.seen()[0].json()),
        ("/send", json!({ "txn_base64": "AQ==" }))
    );
}

#[tokio::test]
async fn dca_send_carries_exactly_the_documented_fields() {
    let s = Planted::start(vec![
        Reply::json(200, json!({ "ok": true, "kind": "open", "signature": "sig", "sent": true, "position": "p" })).into(),
    ])
    .await;
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    // Every field named and no `..Default::default()`: the request holds the documented fields and nothing else.
    let request = DcaSendRequest {
        txn_base64: "AQ==".into(),
        built_txn_base64: Some("AA==".into()),
    };
    let DcaSent {
        ok,
        kind,
        signature,
        sent,
        position,
        extra,
    } = d.send(&request).await.unwrap();
    assert_eq!(
        (
            ok,
            kind.as_str(),
            signature.as_str(),
            sent,
            position.as_str(),
            extra.len()
        ),
        (true, "open", "sig", true, "p", 0)
    );
    assert_eq!(
        (s.seen()[0].path.as_str(), s.seen()[0].json()),
        (
            "/send",
            json!({ "txn_base64": "AQ==", "built_txn_base64": "AA==" })
        )
    );
}

#[tokio::test]
async fn token_api_reads_404_as_none_carries_the_wait_checks_batch_size_and_builds_image_urls() {
    let s = Planted::start(vec![
        Reply::json(404, json!({ "error": "not_found", "mint": "x" })).into(),
        Reply::json(
            429,
            json!({ "error": "rate_limited", "message": "slow down", "retryAfterSec": 12 }),
        )
        .header("retry-after", "12")
        .into(),
        Reply::json(
            200,
            json!({ "items": [{ "mint": "m", "symbol": "M", "name": "Em", "decimals": 6, "logo_status": "ok", "first_seen_at": 1, "last_seen_at": 2 }], "unknown": ["u"] }),
        )
        .into(),
    ])
    .await;
    let t = CarbiumTokens::new(TokensOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(t.lookup("x").await.unwrap(), None);
    let e = t
        .search(
            "sol",
            &TokenSearchOptions {
                limit: Some(5),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind(), e.code(), e.retry_after_ms()),
        (
            ErrorKind::Tokens,
            Some(&Code::Text("rate_limited".into())),
            Some(12_000)
        )
    );
    assert_eq!(s.seen()[1].path, "/tokens?q=sol&limit=5");
    let b = t.batch(&["m".into(), "u".into()]).await.unwrap();
    assert_eq!(
        (b.unknown, Some(b.items[0].symbol.as_str())),
        (vec!["u".to_string()], Some("M"))
    );
    let e = t.batch(&vec!["m".to_string(); 501]).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Tokens);
    assert!(e.message().contains("not 501"), "{e}");
    assert_eq!(s.count(), 3);
    let sol = "So11111111111111111111111111111111111111112";
    assert_eq!(t.image_url(sol), format!("{}/img/{sol}", s.url));
}

#[tokio::test]
async fn account_answers_read_with_every_field_the_description_allows_as_null() {
    let webhook = json!({ "webhook_id": 3, "key_id": null, "name": null, "url": "https://hooks.example.com/c", "status": "active", "status_reason": null,
        "tx_status": "all", "address_count": 1, "secret_hint": "ab", "has_auth_header": false, "consecutive_failures": 0, "last_success_at": null,
        "last_failure_at": null, "created_at": "2026-11-01T00:00:00Z", "updated_at": "2026-11-01T00:00:00Z" });
    let api_key = json!({ "api_key_id": 4, "name": null, "prefix": "c_acct_ab", "access": "read", "scopes": [], "ip_allow": [], "created_at": "2026-11-01T00:00:00Z", "expires_at": null, "last_used_at": null });
    let s = Planted::start(vec![
        Reply::json(200, json!({ "max_connections": null, "max_inflight": null })).into(),
        Reply::json(200, json!({ "name": null })).into(),
        Reply::json(200, webhook).into(),
        Reply::json(200, json!([{ "hour": "2026-11-01T00:00:00Z", "key_id": null, "method_class": "scan", "calls": 1, "p50_ms": null, "p95_ms": null, "p99_ms": null }])).into(),
        Reply::json(200, json!([{ "period_start": null, "period_end": null, "granted": 1, "used": 0, "current": true }])).into(),
        Reply::json(200, json!([{ "ts": 1, "action": "rotate", "request_id": null, "detail": null }])).into(),
        Reply::json(200, api_key).into(),
        Reply::json(200, json!([{ "module": "quote", "plans": [] }])).into(),
        Reply::json(200, json!({ "key_id": 1, "scope": ["rpc", "das"] })).into(),
    ])
    .await;
    let a = CarbiumAccount::new(AccountOptions {
        base_url: s.url.clone(),
        ..AccountOptions::new(account_key())
    })
    .unwrap();
    let limits = a
        .keys()
        .set_limits(
            1,
            carbium::account::KeyLimits {
                max_connections: 5,
                max_inflight: 5,
            },
        )
        .await
        .unwrap();
    assert_eq!((limits.max_connections, limits.max_inflight), (None, None));
    assert_eq!(a.keys().rename(1, "").await.unwrap().name, None);
    assert_eq!(a.webhooks().get(3).await.unwrap().key_id, None);
    assert_eq!(
        a.usage().latency(&Default::default()).await.unwrap()[0].p50_ms,
        None
    );
    assert_eq!(a.account().invoices().await.unwrap()[0].period_start, None);
    assert_eq!(a.activity(4, None, None).await.unwrap()[0].request_id, None);
    assert_eq!(a.self_record().await.unwrap().name, None);
    assert_eq!(
        a.account().modules().await.unwrap()[0]["module"],
        json!("quote")
    );
    let scopes = [
        carbium::account::KeyScope::Rpc,
        carbium::account::KeyScope::Das,
    ];
    assert_eq!(
        a.keys().set_scope(1, &scopes).await.unwrap().scope,
        ["rpc", "das"]
    );
    assert_eq!(s.seen()[8].json(), json!({ "scope": ["rpc", "das"] }));
}

#[tokio::test]
async fn a_closed_dca_record_may_have_no_opening_signature() {
    let closed = json!({ "address": "p", "closed": true, "closedSlot": 9, "openedSlot": 1, "openedSignature": null, "status": "closed",
        "statusCode": "closed", "statusDetail": null, "lastTry": null, "fills": [] });
    let s = Planted::start(vec![Reply::json(200, closed).into()]).await;
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    match d.position("p", None).await.unwrap() {
        carbium::dca::DcaPositionAnswer::Closed(r) => assert_eq!(r.opened_signature, None),
        other => panic!("read as {other:?}"),
    }
}

fn served(group: &str) -> std::collections::BTreeSet<String> {
    let doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("spec/served.json"),
        )
        .unwrap(),
    )
    .unwrap();
    doc[group]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

#[test]
fn the_token_and_dca_routes_are_exactly_the_served_ones() {
    let set = |r: &[&str]| {
        r.iter()
            .map(|s| s.to_string())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(set(carbium::tokens::ROUTES), served("tokens"));
    assert_eq!(set(carbium::dca::ROUTES), served("dca"));
}

/// A DCA quote as the live service answers it: amounts and USDC values as decimal strings.
fn live_quote() -> serde_json::Value {
    json!({ "ok": true, "amount": "5000000", "out": "37000000", "minOut": "36815000", "rateE6": "7400000", "feeBps": 10,
        "inDecimals": 6, "outDecimals": 9, "valueUsdc": "5000000",
        "limits": { "minAmountPerCycle": null, "minValueUsdc": "1000000", "minIntervalSecs": 3600, "maxCycles": 1000, "maxActive": 10 } })
}

#[tokio::test]
async fn dca_reads_decode_the_live_shapes() {
    let position = json!({ "address": "p", "owner": "o", "inMint": "a", "outMint": "b", "cyclesTotal": 2, "cyclesDone": 0, "intervalSecs": 3600,
        "amountPerCycle": "10", "active": true, "status": "active", "openedSlot": null, "openedSignature": null, "ownerIn": null, "ownerOut": null, "fills": [] });
    let order = json!({ "address": "p", "inMint": "a", "outMint": "b", "cyclesDone": 2, "cyclesTotal": 2, "openedSlot": 100, "closedSlot": 200, "ended": "finished" });
    let s = Planted::start(vec![
        Reply::json(200, live_quote()).into(),
        Reply::json(
            200,
            json!({ "ok": true, "owner": "o", "positions": [], "next": null }),
        )
        .into(),
        Reply::json(
            200,
            json!({ "ok": true, "owner": "o", "orders": [order], "next": null }),
        )
        .into(),
        Reply::json(200, position).into(),
    ])
    .await;
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let q = d
        .quote(&DcaQuoteRequest {
            input_mint: "a".into(),
            output_mint: "b".into(),
            amount: 5_000_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        (
            q.amount.as_str(),
            q.value_usdc.as_str(),
            q.limits["minAmountPerCycle"].is_null()
        ),
        ("5000000", "5000000", true)
    );
    assert_eq!(d.positions("o", None).await.unwrap().owner, "o");
    let h = d.history("o", None).await.unwrap();
    assert_eq!(
        (h.owner.as_str(), h.orders[0].opened_slot.as_u64()),
        ("o", Some(100))
    );
    match d.position("p", None).await.unwrap() {
        carbium::dca::DcaPositionAnswer::Open(p) => {
            assert_eq!((p.owner_in, p.owner_out), (None, None))
        }
        other => panic!("read as {other:?}"),
    }
}

#[tokio::test]
async fn dca_builds_carry_the_height_as_a_string_and_withdraw_may_leave_the_amount_out() {
    let built = json!({ "ok": true, "txn_base64": "AQ==", "blockhash": "h", "last_valid_block_height": "123456789", "position": "p", "cost": {}, "summary": {} });
    let s = Planted::start(vec![Reply::json(200, built).into()]).await;
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let b = d
        .withdraw(&DcaWithdrawRequest {
            owner: "o".into(),
            position: "p".into(),
            amount: None,
        })
        .await
        .unwrap();
    assert_eq!(b.last_valid_block_height, "123456789");
    assert_eq!(s.seen()[0].json(), json!({ "owner": "o", "position": "p" }));
}

#[tokio::test]
async fn a_dca_refusal_at_200_is_an_error_its_codes_retryable_and_an_empty_body_reads_its_status() {
    let s = Planted::start(vec![
        Reply::json(200, json!({ "ok": false, "err": "rpc_unavailable", "detail": "the chain RPC did not answer" })).into(),
        Reply::json(200, json!({ "ok": false, "err": "rate_limited_open" })).into(),
        Reply::raw(503, "").into(),
    ])
    .await;
    let d = CarbiumDca::new(DcaOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let e = d.health().await.unwrap_err();
    assert_eq!(
        (e.kind(), e.http_status(), e.is_retryable()),
        (ErrorKind::Dca, 200, true)
    );
    assert!(
        d.health().await.unwrap_err().is_retryable(),
        "rate_limited_open"
    );
    let e = d.health().await.unwrap_err();
    assert_eq!((e.message(), e.is_retryable()), ("HTTP 503", true));
}

#[tokio::test]
async fn a_token_row_decodes_the_live_shape() {
    let row = json!({ "mint": "m", "symbol": "", "name": "", "decimals": 6, "logo_url": null, "logo_status": "broken_perm", "is_token22": false,
        "daily_volume_usd": null, "first_seen_at": 1_763_000_000_000u64, "last_seen_at": 1_763_000_100_000u64, "supply": "1000", "mint_authority": null,
        "freeze_authority": null, "metaplex_uri": null, "update_authority": null, "seller_fee_basis_points": null, "token_standard": 2,
        "description": null, "external_url": null, "extensions": null, "tags": [{ "kind": "verified" }, "community"] });
    let s = Planted::start(vec![Reply::json(200, row).into()]).await;
    let t = CarbiumTokens::new(TokensOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let token = t.lookup("m").await.unwrap().unwrap();
    assert_eq!(
        (
            token.symbol.as_str(),
            token.logo_status.as_str(),
            token.token_standard.and_then(|n| n.as_u64())
        ),
        ("", "broken_perm", Some(2))
    );
    assert_eq!(
        (token.first_seen_at.as_u64(), token.tags.map(|t| t.len())),
        (Some(1_763_000_000_000), Some(2))
    );
}

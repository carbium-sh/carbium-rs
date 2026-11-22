//! The request rules, the batch rules checked locally, malformed answers and redaction: the TypeScript SDK's
//! request, batch and redaction cases, against a planted local server.

mod common;

use carbium::{Carbium, ClientOptions, Code, ErrorKind, JsonRpcCall, Value, redact};
use common::{Planted, Reply, account_key, echo, key, rpc_err, rpc_ok};
use serde_json::json;

fn client(server: &Planted) -> Carbium {
    Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: server.url.clone(),
        retry: None,
        ..Default::default()
    })
    .unwrap()
}

fn call(method: &str, params: Option<Value>) -> JsonRpcCall {
    JsonRpcCall::new(method, params)
}

/// True when `text` holds `prefix` followed by five or more key characters.
fn holds(text: &str, prefix: &str) -> bool {
    text.match_indices(prefix).any(|(i, _)| {
        text[i + prefix.len()..]
            .chars()
            .take(5)
            .filter(|c| c.is_ascii_alphanumeric())
            .count()
            == 5
    })
}

#[tokio::test]
async fn case_10_the_key_travels_in_x_api_key_only_with_the_client_header_and_never_in_the_url() {
    let s = Planted::start(vec![rpc_ok(json!(1)).into()]).await;
    client(&s).get_slot(None).await.unwrap();
    let req = &s.seen()[0];
    assert_eq!(req.headers.get("x-api-key"), Some(&key()));
    assert_eq!(
        req.headers.get("solana-client").map(String::as_str),
        Some("rust/carbium@0.1.0")
    );
    assert_eq!((req.method.as_str(), req.path.as_str()), ("POST", "/"));
    assert_eq!(req.headers.get("authorization"), None);
    assert_eq!(
        req.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    let id = req.json()["id"].as_u64().unwrap();
    assert_eq!(
        req.body,
        format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"getSlot"}}"#)
    );
}

#[tokio::test]
async fn case_11_get_transaction_and_get_block_send_version_1_by_default_and_keep_a_caller_value() {
    let s = Planted::start(vec![rpc_ok(Value::Null).into()]).await;
    let c = client(&s);
    c.get_transaction("sig", None).await.unwrap();
    let config =
        serde_json::from_value(json!({ "encoding": "json", "maxSupportedTransactionVersion": 0 }))
            .unwrap();
    c.get_block(5, Some(&config)).await.unwrap();
    c.get_slot(None).await.ok();
    let batch = c
        .batch(&[call("getTransaction", Some(json!(["sig2"])))])
        .await;
    assert!(
        batch.is_err(),
        "a single-object answer to a batch is an error"
    );
    let sent: Vec<Value> = s.seen().iter().map(|r| r.json()).collect();
    assert_eq!(
        sent[0]["params"],
        json!(["sig", { "maxSupportedTransactionVersion": 1 }])
    );
    assert_eq!(
        sent[1]["params"],
        json!([5, { "maxSupportedTransactionVersion": 0, "encoding": "json" }])
    );
    assert_eq!(sent[2].get("params"), None);
    assert_eq!(
        sent[3][0]["params"],
        json!(["sig2", { "maxSupportedTransactionVersion": 1 }])
    );
}

#[tokio::test]
async fn case_15_a_batch_the_door_refuses_as_a_whole_is_one_error() {
    let refused = json!([{ "jsonrpc": "2.0", "id": null, "error": { "code": -32600, "message": "batch exceeds the cardinality cap", "data": { "reason": "BATCH_TOO_LARGE", "retryable": false } } }]);
    let s = Planted::start(vec![
        Reply::json(400, refused).into(),
        echo(|req| {
            let ids: Vec<Value> = req.json().as_array().unwrap().iter().map(|c| c["id"].clone()).collect();
            let all = ids.iter().map(|id| json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32600, "message": "scan is not batchable", "data": { "reason": "SCAN_NOT_BATCHABLE", "retryable": false } } }));
            Reply::json(200, Value::Array(all.collect()))
        }),
    ])
    .await;
    let c = Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let pair = [call("getSlot", None), call("getHealth", None)];
    let e = c.batch(&pair).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.reason()),
        (ErrorKind::Request, Some("BATCH_TOO_LARGE"))
    );
    let e = c.batch(&pair).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.reason()),
        (ErrorKind::Request, Some("SCAN_NOT_BATCHABLE"))
    );
    assert_eq!(s.count(), 2);
    let e = c
        .batch(&[call("getSlot", None), call("notAMethod", None)])
        .await
        .unwrap_err();
    assert_eq!(
        (e.kind(), e.code()),
        (ErrorKind::Request, Some(&Code::Number(-32601)))
    );
    let e = c.call::<Value>("notAMethod", None).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.code()),
        (ErrorKind::Request, Some(&Code::Number(-32601)))
    );
    assert_eq!(s.count(), 2);
}

#[tokio::test]
async fn case_16_a_200_answer_that_is_not_json_rpc_is_an_upstream_error() {
    let s = Planted::start(vec![
        Reply::raw(200, "null").into(),
        Reply::raw(200, "<html>proxy</html>")
            .header("content-type", "text/html")
            .into(),
    ])
    .await;
    let c = client(&s);
    let e = c.get_slot(None).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.request_id()),
        (ErrorKind::Upstream, Some("rid-1"))
    );
    let e = c.get_slot(None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Upstream);
    assert!(e.message().contains("proxy"), "{e}");
}

#[tokio::test]
async fn case_17_a_network_error_keeps_its_cause_chain() {
    let c = Carbium::new(ClientOptions {
        api_key: Some(key()),
        endpoint: "http://127.0.0.1:9".into(),
        retry: None,
        ..Default::default()
    })
    .unwrap();
    let e = c.get_slot(None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Transport);
    let cause = std::error::Error::source(&e).expect("a cause");
    assert!(
        std::error::Error::source(cause).is_some(),
        "the cause chain stops at {cause}"
    );
}

#[tokio::test]
async fn case_18_door_rules_are_refused_locally_before_any_request() {
    let s = Planted::start(vec![rpc_ok(json!(1)).into()]).await;
    let c = client(&s);
    let message = |r: carbium::Result<Vec<carbium::Result<Value>>>| {
        let e = r.unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Request);
        e.message().to_string()
    };
    assert!(message(c.batch(&[]).await).contains("1 to 100"));
    assert!(message(c.batch(&vec![call("getSlot", None); 101]).await).contains("not 101"));
    assert!(
        message(
            c.batch(&[
                call("getSlot", None),
                call("getProgramAccounts", Some(json!(["x"])))
            ])
            .await
        )
        .contains("not batchable")
    );
    let quote = json!({ "src_mint": "a", "dst_mint": "b", "amount_in": 1, "slippage_bps": 50 });
    assert!(message(c.batch(&[call("quote", Some(quote))]).await).contains("not batchable"));
    assert!(
        message(
            c.batch(&[
                call("getAsset", Some(json!({ "id": "x" }))),
                call("getSlot", None)
            ])
            .await
        )
        .contains("DAS")
    );
    let three = [
        call("getAccountInfo", Some(json!(["a"]))),
        call("getTransaction", Some(json!(["b"]))),
        call("getMultipleAccounts", Some(json!([["c"]]))),
    ];
    assert!(message(c.batch(&three).await).contains("permit"));
    let many = vec!["x".repeat(44); 2000];
    let e = c.get_multiple_accounts(&many, None).await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Request);
    assert!(e.message().contains("51200"), "{e}");
    assert_eq!(s.count(), 0);
}

#[tokio::test]
async fn case_19_a_batch_answered_out_of_order_comes_back_in_request_order() {
    let s = Planted::start(vec![echo(|req| {
        let calls = req.json();
        Reply::json(
            200,
            json!([
                { "jsonrpc": "2.0", "id": calls[1]["id"], "error": { "code": -32602, "message": "invalid params" } },
                { "jsonrpc": "2.0", "id": calls[0]["id"], "result": 7 },
                { "jsonrpc": "2.0", "id": calls[2]["id"], "result": "ok" },
            ]),
        )
    })])
    .await;
    let r = client(&s)
        .batch(&[
            call("getSlot", None),
            call("getBalance", Some(json!(["x"]))),
            call("getHealth", None),
        ])
        .await
        .unwrap();
    assert_eq!(r[0].as_ref().unwrap(), &json!(7));
    let e = r[1].as_ref().unwrap_err();
    assert_eq!(
        (e.kind(), e.method()),
        (ErrorKind::Request, Some("getBalance"))
    );
    assert_eq!(r[2].as_ref().unwrap(), &json!("ok"));
    let sent = s.seen()[0].json();
    let methods: Vec<&str> = sent
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["method"].as_str().unwrap())
        .collect();
    assert_eq!(methods, ["getSlot", "getBalance", "getHealth"]);
    let ids: Vec<u64> = sent
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_u64().unwrap())
        .collect();
    assert!(ids[0] < ids[1] && ids[1] < ids[2]);
}

#[tokio::test]
async fn case_20_no_key_or_secret_survives_in_an_error_or_the_client() {
    let secret = format!("whsec_{}", "Q".repeat(20));
    let leak = format!(
        "the door saw {} at https://x.test/?api-key={} and {} and {secret}; subprotocol api-key.{}",
        key(),
        key(),
        account_key(),
        "Z".repeat(12)
    );
    let mut keyed = serde_json::Map::new();
    keyed.insert(leak.clone(), json!(leak));
    let data = json!({ "reason": "MALFORMED_BODY", "retryable": false, "echo": leak, "nested": [[keyed]] });
    let s = Planted::start(vec![rpc_err(400, -32600, &leak, Some(data)).into()]).await;
    let c = client(&s);
    let e = c.get_slot(None).await.unwrap_err();
    let everywhere = [
        e.to_string(),
        format!("{e:?}"),
        e.data().unwrap().to_string(),
        format!("{c:?}"),
        format!(
            "{:?}",
            ClientOptions {
                api_key: Some(key()),
                ..Default::default()
            }
        ),
    ]
    .join("\n");
    for prefix in ["c_serv_", "c_acct_", "whsec_", "api-key=", "api-key."] {
        assert!(
            !holds(&everywhere, prefix),
            "{prefix} survives in: {everywhere}"
        );
    }
    assert!(e.message().contains("c_serv_…"), "{e}");
    assert!(format!("{c:?}").contains("c_serv_…"));
    assert_eq!(
        redact(&format!("Bearer {}", account_key())),
        "Bearer c_acct_…"
    );
    assert_eq!(
        redact("x?token=abc&b=1 é TOKEN=zz"),
        "x?token=…&b=1 é TOKEN=…"
    );
    assert_eq!(redact("c_serv_…"), "c_serv_…");
}

#[test]
fn case_21_an_account_key_or_a_key_in_the_endpoint_is_refused_at_construction() {
    let with = |api_key: String, endpoint: String| {
        Carbium::new(ClientOptions {
            api_key: Some(api_key),
            endpoint,
            ..Default::default()
        })
        .map(drop)
        .unwrap_err()
        .kind()
    };
    assert_eq!(
        with(account_key(), "https://x.test".into()),
        ErrorKind::Auth
    );
    assert_eq!(
        with(format!("{} space", key()), "https://x.test".into()),
        ErrorKind::Auth
    );
    assert_eq!(
        with(key(), format!("https://x.test/?api-key={}", key())),
        ErrorKind::Request
    );
    assert_eq!(
        with(key(), format!("https://x.test/{}", key())),
        ErrorKind::Request
    );
    assert_eq!(with(key(), "not a url".into()), ErrorKind::Request);
    let keyless = Carbium::new(ClientOptions {
        api_key: Some(String::new()),
        ..Default::default()
    })
    .unwrap();
    assert!(format!("{keyless:?}").contains("api_key: None"));
}

#[tokio::test]
async fn a_redirect_is_not_followed_so_the_key_never_reaches_another_host() {
    let elsewhere = Planted::start(vec![rpc_ok(json!(1)).into()]).await;
    let s = Planted::start(vec![
        Reply::raw(302, "")
            .header("location", &format!("{}/", elsewhere.url))
            .into(),
    ])
    .await;
    let e = client(&s).get_slot(None).await.unwrap_err();
    assert_eq!(
        (e.kind(), e.http_status(), s.count(), elsewhere.count()),
        (ErrorKind::Upstream, 302, 1, 0)
    );
    let tokens = carbium::CarbiumTokens::new(carbium::tokens::TokensOptions {
        base_url: s.url.clone(),
        ..Default::default()
    })
    .unwrap();
    let e = tokens.lookup("x").await.unwrap_err();
    assert_eq!(
        (e.kind(), e.http_status(), elsewhere.count()),
        (ErrorKind::Tokens, 302, 0)
    );
}

#[tokio::test]
async fn a_batch_element_error_carries_retryable_the_wait_and_the_request_id() {
    let s = Planted::start(vec![echo(|req| {
        let calls = req.json();
        Reply::json(200, json!([
            { "jsonrpc": "2.0", "id": calls[0]["id"], "result": 1 },
            { "jsonrpc": "2.0", "id": calls[1]["id"], "error": { "code": -32005, "message": "busy", "data": { "retryable": true, "retryAfterMs": 500 } } },
        ]))
        .header("retry-after", "2")
    })])
    .await;
    let r = client(&s)
        .batch(&[call("getSlot", None), call("getHealth", None)])
        .await
        .unwrap();
    let e = r[1].as_ref().unwrap_err();
    assert_eq!(
        (
            e.is_retryable(),
            e.retry_after_ms(),
            e.request_id(),
            e.method()
        ),
        (true, Some(500), Some("rid-1"), Some("getHealth"))
    );
}

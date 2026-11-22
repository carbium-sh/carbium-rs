//! The WebSocket client against a fake subscription server: the key carriers, routing by server id, and the
//! close-code reconnect table, ported from the TypeScript SDK's ws tests.
#![cfg(feature = "ws")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use carbium::ws::{Carrier, SocketEvent, SocketOptions, SocketState};
use carbium::{CarbiumSocket, ErrorKind};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Message, http};

fn key() -> String {
    ["c_serv", "_wskey00000000000000000"].concat()
}

#[derive(Clone)]
enum Ctl {
    Close(u16, String),
    Notify(u64, String, Value),
}

#[derive(Default, Clone)]
struct FakeOptions {
    refuse: Option<(u16, Vec<(&'static str, &'static str)>)>,
    refuse_first: Option<usize>,
    ack_delay_ms: u64,
    refuse_binds: usize,
}

/// Each handshake's path and headers.
type Handshakes = Arc<Mutex<Vec<(String, HashMap<String, String>)>>>;

struct Fake {
    url: String,
    received: Arc<Mutex<Vec<Value>>>,
    handshakes: Handshakes,
    connections: Arc<AtomicUsize>,
    ctl: broadcast::Sender<Ctl>,
}

impl Fake {
    async fn start(opts: FakeOptions) -> Fake {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (ctl, _) = broadcast::channel(16);
        let fake = Fake {
            url,
            received: Arc::default(),
            handshakes: Arc::default(),
            connections: Arc::default(),
            ctl: ctl.clone(),
        };
        let (received, handshakes, connections) = (
            fake.received.clone(),
            fake.handshakes.clone(),
            fake.connections.clone(),
        );
        let next_id = Arc::new(AtomicU64::new(100));
        let binds_refused = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (received, handshakes, connections, next_id, binds_refused, opts) = (
                    received.clone(),
                    handshakes.clone(),
                    connections.clone(),
                    next_id.clone(),
                    binds_refused.clone(),
                    opts.clone(),
                );
                let mut ctl_rx = ctl.subscribe();
                tokio::spawn(async move {
                    let shakes = handshakes.clone();
                    #[allow(clippy::result_large_err)]
                    // the shape of tungstenite's handshake Callback
                    let callback = move |req: &Request,
                                         mut res: Response|
                          -> Result<Response, ErrorResponse> {
                        let headers = req
                            .headers()
                            .iter()
                            .map(|(k, v)| {
                                (k.as_str().to_string(), v.to_str().unwrap_or("").to_string())
                            })
                            .collect();
                        let n = {
                            let mut all = shakes.lock().unwrap();
                            all.push((req.uri().path().to_string(), headers));
                            all.len()
                        };
                        if let Some((status, extra)) = &opts.refuse
                            && opts.refuse_first.is_none_or(|first| n <= first)
                        {
                            let mut refused = http::Response::builder().status(*status);
                            for (k, v) in extra {
                                refused = refused.header(*k, *v);
                            }
                            return Err(refused.body(Some("refused".into())).unwrap());
                        }
                        if req
                            .headers()
                            .get("sec-websocket-protocol")
                            .is_some_and(|v| {
                                v.to_str()
                                    .unwrap_or("")
                                    .split(',')
                                    .any(|p| p.trim() == "carbium")
                            })
                        {
                            res.headers_mut()
                                .insert("sec-websocket-protocol", "carbium".parse().unwrap());
                        }
                        Ok(res)
                    };
                    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await
                    else {
                        return;
                    };
                    connections.fetch_add(1, Ordering::SeqCst);
                    loop {
                        tokio::select! {
                            frame = ws.next() => {
                                let Some(Ok(Message::Text(text))) = frame else {
                                    if matches!(frame, Some(Ok(_))) { continue }
                                    return;
                                };
                                let msg: Value = serde_json::from_str(text.as_str()).unwrap();
                                let method = msg["method"].as_str().unwrap().to_string();
                                let mut seen = json!({ "method": method });
                                if let Some(p) = msg.get("params") { seen["params"] = p.clone(); }
                                received.lock().unwrap().push(seen);
                                let reply = if method.ends_with("Subscribe") {
                                    tokio::time::sleep(Duration::from_millis(opts.ack_delay_ms)).await;
                                    if binds_refused.fetch_add(1, Ordering::SeqCst) < opts.refuse_binds {
                                        json!({ "jsonrpc": "2.0", "id": msg["id"], "error": { "code": -32005, "message": "rate limit exceeded", "data": { "reason": "RATE_LIMITED" } } })
                                    } else {
                                        json!({ "jsonrpc": "2.0", "id": msg["id"], "result": next_id.fetch_add(1, Ordering::SeqCst) })
                                    }
                                } else {
                                    json!({ "jsonrpc": "2.0", "id": msg["id"], "result": true })
                                };
                                let _ = ws.send(Message::text(reply.to_string())).await;
                            }
                            c = ctl_rx.recv() => match c {
                                Ok(Ctl::Close(code, reason)) => { let _ = ws.send(Message::Close(Some(CloseFrame { code: code.into(), reason: reason.into() }))).await; }
                                Ok(Ctl::Notify(sub, method, result)) => { let _ = ws.send(Message::text(json!({ "jsonrpc": "2.0", "method": method, "params": { "subscription": sub, "result": result } }).to_string())).await; }
                                Err(_) => return,
                            },
                        }
                    }
                });
            }
        });
        fake
    }

    fn close_all(&self, code: u16, reason: &str) {
        let _ = self.ctl.send(Ctl::Close(code, reason.into()));
    }

    fn notify(&self, sub: u64, method: &str, result: Value) {
        let _ = self.ctl.send(Ctl::Notify(sub, method.into(), result));
    }

    fn received(&self) -> Vec<Value> {
        self.received.lock().unwrap().clone()
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn handshakes(&self) -> Vec<(String, HashMap<String, String>)> {
        self.handshakes.lock().unwrap().clone()
    }
}

type Events = Arc<Mutex<Vec<(Instant, SocketEvent)>>>;

fn socket(url: &str, carrier: Carrier) -> (CarbiumSocket, Events) {
    let (socket, mut rx): (CarbiumSocket, mpsc::UnboundedReceiver<SocketEvent>) =
        CarbiumSocket::new(SocketOptions {
            api_key: Some(key()),
            url: url.into(),
            carrier,
            random: || 0.0,
        })
        .unwrap();
    let events: Events = Arc::default();
    let sink = events.clone();
    tokio::spawn(async move {
        while let Some(e) = rx.recv().await {
            sink.lock().unwrap().push((Instant::now(), e));
        }
    });
    (socket, events)
}

async fn until(what: &str, ms: u64, cond: impl Fn() -> bool) {
    let t0 = Instant::now();
    while !cond() {
        assert!(
            t0.elapsed() < Duration::from_millis(ms),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn reseeds(events: &Events) -> Vec<(u16, String, bool)> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, e)| {
            if let SocketEvent::Reseed { close, gap } = e {
                Some((close.code, close.reason.clone(), *gap))
            } else {
                None
            }
        })
        .collect()
}

fn fatals(events: &Events) -> Vec<carbium::Error> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, e)| {
            if let SocketEvent::Fatal(err) = e {
                Some(err.clone())
            } else {
                None
            }
        })
        .collect()
}

fn account(lamports: u64) -> Value {
    json!({ "context": { "slot": 1 }, "value": { "lamports": lamports, "owner": "11111111111111111111111111111111", "data": ["", "base64"], "executable": false, "rentEpoch": 0 } })
}

#[tokio::test]
async fn the_subprotocol_carrier_acks_routes_by_server_id_and_keeps_the_key_out_of_the_url() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, _) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket
        .account_subscribe(
            "11111111111111111111111111111111",
            Some(json!({ "commitment": "confirmed" })),
        )
        .unwrap();
    assert_eq!(sub.ready().await.unwrap(), 100);
    let (path, headers) = &f.handshakes()[0];
    assert_eq!(
        headers.get("sec-websocket-protocol"),
        Some(&format!("carbium, api-key.{}", key()))
    );
    assert_eq!((path.as_str(), headers.get("x-api-key")), ("/", None));
    assert_eq!(
        f.received()[0],
        json!({ "method": "accountSubscribe", "params": ["11111111111111111111111111111111", { "commitment": "confirmed" }] })
    );
    f.notify(100, "accountNotification", account(5));
    let n = sub.next().await.unwrap().unwrap();
    assert_eq!(n.value.lamports.as_u64(), Some(5));
    sub.unsubscribe();
    until("accountUnsubscribe", 3000, || {
        f.received()
            .last()
            .is_some_and(|r| r["method"] == "accountUnsubscribe")
    })
    .await;
    assert_eq!(f.received().last().unwrap()["params"], json!([100]));
    until("idle", 3000, || socket.state() == SocketState::Idle).await;
    socket.close();
}

#[tokio::test]
async fn the_header_carrier_sends_x_api_key_and_no_subprotocol() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, _) = socket(&f.url, Carrier::Header);
    socket.slot_subscribe().unwrap().ready().await.unwrap();
    let (_, headers) = &f.handshakes()[0];
    assert_eq!(
        (
            headers.get("x-api-key"),
            headers.get("sec-websocket-protocol")
        ),
        (Some(&key()), None)
    );
    socket.close();
}

#[tokio::test]
async fn close_1013_credits_pending_reconnects_after_about_1_s_and_binds_the_same_params_again() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, events) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket
        .logs_subscribe(
            json!({ "mentions": ["11111111111111111111111111111111"] }),
            None,
        )
        .unwrap();
    sub.ready().await.unwrap();
    let t0 = Instant::now();
    f.close_all(1013, "credits_pending");
    until("the rebind", 5000, || {
        f.connections() == 2 && f.received().len() == 2
    })
    .await;
    assert!(
        t0.elapsed() >= Duration::from_millis(1000),
        "reconnected after {:?}",
        t0.elapsed()
    );
    assert_eq!(f.received()[1], f.received()[0]);
    until("the reseed", 3000, || reseeds(&events).len() == 1).await;
    assert_eq!(
        reseeds(&events),
        [(1013, "credits_pending".to_string(), false)]
    );
    socket.close();
}

#[tokio::test]
async fn close_1012_stream_gap_reconnects_at_once_and_the_reseed_says_gap() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, events) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket.slot_subscribe().unwrap();
    sub.ready().await.unwrap();
    let t0 = Instant::now();
    f.close_all(1012, "stream_gap_reseed_required");
    until("the reseed", 3000, || reseeds(&events).len() == 1).await;
    assert!(
        t0.elapsed() < Duration::from_millis(1200),
        "took {:?}",
        t0.elapsed()
    );
    assert!(reseeds(&events)[0].2, "gap");
    assert_eq!(f.connections(), 2);
    socket.close();
}

#[tokio::test]
async fn close_1008_key_revoked_is_fatal_with_an_auth_error_and_no_reconnect() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, events) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket.root_subscribe().unwrap(); // held: dropping it would end the last subscription
    sub.ready().await.unwrap();
    f.close_all(1008, "key_revoked");
    until("the fatal event", 3000, || fatals(&events).len() == 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let e = &fatals(&events)[0];
    assert_eq!(
        (e.kind(), e.reason(), f.connections(), socket.state()),
        (ErrorKind::Auth, Some("key_revoked"), 1, SocketState::Closed)
    );
}

#[tokio::test]
async fn close_1008_credits_exhausted_reconnects_once_after_5_s_and_a_second_is_fatal() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, events) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket.slots_updates_subscribe().unwrap();
    sub.ready().await.unwrap();
    let t0 = Instant::now();
    f.close_all(1008, "credits_exhausted");
    until("the reconnect", 9000, || f.connections() == 2).await;
    assert!(
        t0.elapsed() >= Duration::from_millis(5000),
        "reconnected after {:?}",
        t0.elapsed()
    );
    until("the rebind", 3000, || f.received().len() == 2).await;
    f.close_all(1008, "credits_exhausted");
    until("the fatal event", 3000, || fatals(&events).len() == 1).await;
    assert_eq!(
        (fatals(&events)[0].kind(), f.connections()),
        (ErrorKind::Credits, 2)
    );
}

#[tokio::test]
async fn a_401_handshake_is_fatal_at_once_with_its_status() {
    let f = Fake::start(FakeOptions {
        refuse: Some((401, vec![])),
        ..Default::default()
    })
    .await;
    let (socket, events) = socket(&f.url, Carrier::Header);
    let e = socket.slot_subscribe().unwrap().ready().await.unwrap_err();
    assert_eq!((e.kind(), e.http_status()), (ErrorKind::Auth, 401));
    until("the fatal event", 3000, || fatals(&events).len() == 1).await;
    assert_eq!(f.handshakes().len(), 1);
}

#[tokio::test]
async fn five_handshakes_in_a_row_with_no_answer_are_fatal() {
    let closed = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap(); // bound, then dropped: nothing listens
    let (socket, events) = socket(&format!("ws://{closed}"), Carrier::Subprotocol);
    let e = socket.slot_subscribe().unwrap().ready().await.unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Auth);
    let connecting = events
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, e)| matches!(e, SocketEvent::State(SocketState::Connecting, _)))
        .count();
    assert_eq!((connecting, fatals(&events).len()), (5, 1));
}

#[tokio::test]
async fn a_429_handshake_is_retried_after_retry_after_and_never_counted_as_a_key_refusal() {
    let f = Fake::start(FakeOptions {
        refuse: Some((429, vec![("retry-after", "1")])),
        refuse_first: Some(3),
        ..Default::default()
    })
    .await;
    let (socket, events) = socket(&f.url, Carrier::Header);
    let t0 = Instant::now();
    socket.slot_subscribe().unwrap().ready().await.unwrap();
    assert_eq!(f.handshakes().len(), 4);
    assert!(
        t0.elapsed() >= Duration::from_millis(3000),
        "connected after {:?}, three Retry-After waits of 1 s",
        t0.elapsed()
    );
    assert!(fatals(&events).is_empty());
    socket.close();
}

#[tokio::test]
async fn signature_subscribe_fires_once_and_block_subscribe_sends_version_1() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, _) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket.signature_subscribe(&"5".repeat(64), None).unwrap();
    let id = sub.ready().await.unwrap();
    assert_eq!(socket.size(), 1);
    f.notify(
        id,
        "signatureNotification",
        json!({ "context": { "slot": 2 }, "value": { "err": null } }),
    );
    assert!(sub.next().await.unwrap().is_ok());
    until("the local drop", 3000, || socket.size() == 0).await;
    assert!(sub.next().await.is_none(), "the subscription ended");
    sub.unsubscribe();
    let mut block = socket
        .block_subscribe(json!("all"), Some(json!({ "commitment": "confirmed" })))
        .unwrap();
    block.ready().await.unwrap();
    assert!(
        !f.received()
            .iter()
            .any(|r| r["method"] == "signatureUnsubscribe")
    );
    assert_eq!(
        f.received().last().unwrap()["params"],
        json!(["all", { "maxSupportedTransactionVersion": 1, "commitment": "confirmed" }])
    );
    socket.close();
}

#[tokio::test]
async fn an_account_key_a_key_in_the_url_and_an_unserved_name_are_refused_before_any_connection() {
    let account_key = ["c_acct", "_", &"x".repeat(10)].concat();
    let e = CarbiumSocket::new(SocketOptions {
        api_key: Some(account_key),
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Auth);
    let url = format!("wss://example.test/?api-key={}", "x".repeat(10));
    assert_eq!(
        CarbiumSocket::new(SocketOptions {
            url,
            ..Default::default()
        })
        .unwrap_err()
        .kind(),
        ErrorKind::Request
    );
    let (socket, _) = CarbiumSocket::new(SocketOptions::default()).unwrap();
    assert_eq!(
        socket
            .subscribe::<Value>("notASubscription", vec![])
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::Request)
    );
    assert_eq!(socket.state(), SocketState::Idle);
}

#[tokio::test]
async fn unsubscribe_before_the_ack_frees_the_server_slot_once_the_ack_arrives() {
    let f = Fake::start(FakeOptions {
        ack_delay_ms: 150,
        ..Default::default()
    })
    .await;
    let (socket, _) = socket(&f.url, Carrier::Subprotocol);
    let mut keep = socket.slot_subscribe().unwrap();
    let mut dropped = socket
        .account_subscribe("11111111111111111111111111111111", None)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    dropped.unsubscribe();
    assert!(dropped.ready().await.is_err());
    keep.ready().await.unwrap();
    until("accountUnsubscribe", 3000, || {
        f.received()
            .iter()
            .any(|r| r["method"] == "accountUnsubscribe")
    })
    .await;
    let unsub = f
        .received()
        .into_iter()
        .find(|r| r["method"] == "accountUnsubscribe")
        .unwrap();
    assert_eq!((unsub["params"].clone(), socket.size()), (json!([101]), 1));
    socket.close();
}

#[tokio::test]
async fn a_bind_refused_with_rate_limited_is_tried_again_after_at_least_1_s() {
    let f = Fake::start(FakeOptions {
        refuse_binds: 1,
        ..Default::default()
    })
    .await;
    let (socket, _) = socket(&f.url, Carrier::Subprotocol);
    let t0 = Instant::now();
    socket.slot_subscribe().unwrap().ready().await.unwrap();
    assert!(
        t0.elapsed() >= Duration::from_millis(1000),
        "bound after {:?}",
        t0.elapsed()
    );
    assert_eq!(f.received().len(), 2);
    socket.close();
}

#[tokio::test]
async fn close_1013_slow_consumer_waits_at_least_1_s_and_further_on_each_close_in_a_row() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, events) = socket(&f.url, Carrier::Subprotocol);
    let mut sub = socket.slot_subscribe().unwrap();
    sub.ready().await.unwrap();
    let opens = || {
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, e)| matches!(e, SocketEvent::State(SocketState::Open, _)))
            .map(|(at, _)| *at)
            .collect::<Vec<_>>()
    };
    for n in [2, 3, 4] {
        until("the rebind", 12000, || {
            f.connections() == n - 1 && f.received().len() == n - 1
        })
        .await;
        f.close_all(1013, "slow_consumer");
        until("the reopen", 12000, || opens().len() == n).await;
    }
    let at = opens();
    let gaps: Vec<u128> = at.windows(2).map(|w| (w[1] - w[0]).as_millis()).collect();
    assert!(gaps.iter().all(|g| *g >= 1000), "gaps {gaps:?} ms");
    assert!(gaps[2] >= 2000, "third gap {} ms", gaps[2]);
    socket.close();
}

#[tokio::test]
async fn the_default_carrier_is_the_x_api_key_header_with_no_key_in_the_subprotocol() {
    let f = Fake::start(FakeOptions::default()).await;
    let (socket, _) = CarbiumSocket::new(SocketOptions {
        api_key: Some(key()),
        url: f.url.clone(),
        random: || 0.0,
        ..Default::default()
    })
    .unwrap();
    let mut sub = socket.slot_subscribe().unwrap();
    sub.ready().await.unwrap();
    let (_, headers) = &f.handshakes()[0];
    assert_eq!(headers.get("x-api-key"), Some(&key()));
    assert!(
        !headers
            .get("sec-websocket-protocol")
            .is_some_and(|v| v.contains("api-key")),
        "the key went in the subprotocol"
    );
    socket.close();
}

//! Carbon Stream against an in-process fake Geyser gRPC server: the key as x-token, the served filters and unary
//! calls counted both ways, the refusals before sending, and each reconnect rule of the TypeScript SDK's stream.
#![cfg(feature = "stream")]

use std::collections::{BTreeSet, VecDeque};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use carbium::ErrorKind;
use carbium::stream::{
    CarbiumStream, ReconnectOptions, STREAM_FILTERS, STREAM_UNARY, StreamEvent, StreamFilters,
    StreamOptions, build_request, next_backoff_ms, retry_delay_ms,
};
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;
use yellowstone_grpc_proto::prelude::geyser_server::{Geyser, GeyserServer};
use yellowstone_grpc_proto::prelude::*;
use yellowstone_grpc_proto::tonic::metadata::MetadataMap;
use yellowstone_grpc_proto::tonic::{
    self, Code, Request, Response, Status, Streaming, async_trait,
};

fn key() -> String {
    ["c_serv", "_streamkey0000000000"].concat()
}

#[derive(Clone, Debug)]
enum Act {
    Slot(u64),
    TxStatus(u64),
    End(Code, &'static str),
    /// An end whose status carries a retry-after (seconds) in its metadata.
    EndRetryAfter(Code, &'static str, &'static str),
}

#[derive(Default)]
struct State {
    calls: Vec<String>,
    tokens: Vec<String>,
    subscribes: Vec<(Instant, SubscribeRequest)>,
    pings: usize,
    script: VecDeque<Vec<Act>>,
    pong: bool,
    first_available: u64,
    refuse_open: Option<(Code, &'static str)>,
    replay_fails: bool,
    incoming_closed: usize,
    slot_delay_ms: u64,
    in_flight: usize,
    max_in_flight: usize,
}

#[derive(Clone, Default)]
struct Fake(Arc<Mutex<State>>);

type Updates = Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;

impl Fake {
    fn note<T>(&self, method: &str, req: &Request<T>) {
        let mut s = self.0.lock().unwrap();
        s.calls.push(method.into());
        s.tokens.push(
            req.metadata()
                .get("x-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string(),
        );
    }
}

fn channel_stream(rx: mpsc::Receiver<Result<SubscribeUpdate, Status>>) -> Updates {
    Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

fn slot_update(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["s".into()],
        update_oneof: Some(subscribe_update::UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[async_trait]
impl Geyser for Fake {
    type SubscribeStream = Updates;
    type SubscribeDeshredStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeUpdateDeshred, Status>> + Send>>;
    type SubscribeGossipStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeUpdateGossip, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Updates>, Status> {
        self.note("Subscribe", &request);
        if let Some((code, message)) = self.0.lock().unwrap().refuse_open {
            return Err(Status::new(code, message)); // refused at open, as a refused key is
        }
        let mut incoming = request.into_inner();
        let first = incoming
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("no request"))??;
        let (acts, pong) = {
            let mut s = self.0.lock().unwrap();
            s.subscribes.push((Instant::now(), first));
            (s.script.pop_front().unwrap_or_default(), s.pong)
        };
        let (tx, rx) = mpsc::channel(64);
        let state = self.0.clone();
        tokio::spawn(async move {
            for act in acts {
                match act {
                    Act::Slot(n) => drop(tx.send(Ok(slot_update(n))).await),
                    Act::TxStatus(n) => {
                        let status = SubscribeUpdateTransactionStatus {
                            slot: n,
                            ..Default::default()
                        };
                        let update = SubscribeUpdate {
                            update_oneof: Some(subscribe_update::UpdateOneof::TransactionStatus(
                                status,
                            )),
                            ..Default::default()
                        };
                        let _ = tx.send(Ok(update)).await;
                    }
                    Act::EndRetryAfter(code, message, seconds) => {
                        let mut meta = MetadataMap::new();
                        meta.insert("retry-after", seconds.parse().unwrap());
                        let _ = tx
                            .send(Err(Status::with_metadata(code, message, meta)))
                            .await;
                        return;
                    }
                    Act::End(code, message) => {
                        let _ = tx.send(Err(Status::new(code, message))).await;
                        return;
                    }
                }
            }
            while let Some(Ok(msg)) = incoming.next().await {
                if let Some(ping) = msg.ping {
                    state.lock().unwrap().pings += 1;
                    if pong {
                        let update = SubscribeUpdate {
                            update_oneof: Some(subscribe_update::UpdateOneof::Pong(
                                SubscribeUpdatePong { id: ping.id },
                            )),
                            ..Default::default()
                        };
                        let _ = tx.send(Ok(update)).await;
                    }
                }
            }
            state.lock().unwrap().incoming_closed += 1;
        });
        Ok(Response::new(channel_stream(rx)))
    }
    async fn subscribe_deshred(
        &self,
        _: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented("not served"))
    }
    async fn subscribe_gossip(
        &self,
        _: Request<SubscribeGossipRequest>,
    ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
        Err(Status::unimplemented("not served"))
    }
    async fn subscribe_replay_info(
        &self,
        r: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        self.note("SubscribeReplayInfo", &r);
        if self.0.lock().unwrap().replay_fails {
            return Err(Status::unavailable("replay info is not available"));
        }
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available: Some(self.0.lock().unwrap().first_available),
        }))
    }
    async fn ping(&self, r: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        self.note("Ping", &r);
        Ok(Response::new(PongResponse {
            count: r.into_inner().count,
        }))
    }
    async fn get_latest_blockhash(
        &self,
        r: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        self.note("GetLatestBlockhash", &r);
        Ok(Response::new(GetLatestBlockhashResponse {
            slot: 7,
            blockhash: "h".into(),
            last_valid_block_height: 9,
        }))
    }
    async fn get_block_height(
        &self,
        r: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        self.note("GetBlockHeight", &r);
        Ok(Response::new(GetBlockHeightResponse { block_height: 6 }))
    }
    async fn get_slot(
        &self,
        r: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        self.note("GetSlot", &r);
        let delay = {
            let mut st = self.0.lock().unwrap();
            st.in_flight += 1;
            st.max_in_flight = st.max_in_flight.max(st.in_flight);
            st.slot_delay_ms
        };
        tokio::time::sleep(Duration::from_millis(delay)).await;
        self.0.lock().unwrap().in_flight -= 1;
        Ok(Response::new(GetSlotResponse { slot: 7 }))
    }
    async fn is_blockhash_valid(
        &self,
        r: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        self.note("IsBlockhashValid", &r);
        Ok(Response::new(IsBlockhashValidResponse {
            slot: 7,
            valid: true,
        }))
    }
    async fn get_version(
        &self,
        r: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        self.note("GetVersion", &r);
        Ok(Response::new(GetVersionResponse {
            version: "fake".into(),
        }))
    }
}

async fn start(fake: Fake) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures_util::stream::unfold(listener, |l| async move {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(GeyserServer::new(fake))
            .serve_with_incoming(incoming),
    );
    url
}

async fn client(fake: &Fake) -> CarbiumStream {
    let url = start(fake.clone()).await;
    CarbiumStream::connect(StreamOptions {
        endpoint: url,
        ..StreamOptions::new(key())
    })
    .await
    .unwrap()
}

fn slots() -> StreamFilters {
    let mut f = StreamFilters::default();
    f.slots
        .insert("s".into(), SubscribeRequestFilterSlots::default());
    f
}

fn no_jitter() -> ReconnectOptions {
    ReconnectOptions {
        random: || 0.0,
        ..Default::default()
    }
}

fn served(kind: &str) -> BTreeSet<String> {
    let doc: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("spec/served.json"))
            .unwrap(),
    )
    .unwrap();
    doc["stream"][kind]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

/// The served-name of every filter map a request carries.
fn filter_names(r: &SubscribeRequest) -> BTreeSet<String> {
    let maps = [
        ("accounts", r.accounts.len()),
        ("blocks", r.blocks.len()),
        ("blocksMeta", r.blocks_meta.len()),
        ("entry", r.entry.len()),
        ("slots", r.slots.len()),
        ("transactions", r.transactions.len()),
        ("transactionsStatus", r.transactions_status.len()),
        ("blockFooter", r.block_footer.len()),
    ];
    maps.iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, _)| k.to_string())
        .collect()
}

async fn events_until(
    stream: &mut carbium::stream::ReconnectingStream,
    ms: u64,
    stop: impl Fn(&[StreamEvent]) -> bool,
) -> Vec<StreamEvent> {
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
    while !stop(&seen) {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(e)) => seen.push(e),
            Ok(None) => break,
            Err(_) => panic!("timed out; saw {seen:?}"),
        }
    }
    seen
}

fn tx(include: &[&str]) -> SubscribeRequestFilterTransactions {
    SubscribeRequestFilterTransactions {
        account_include: include.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_served_unary_calls_and_filters_both_ways_with_the_key_as_x_token() {
    let as_set = |names: &[&str]| names.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();
    assert_eq!(as_set(STREAM_UNARY), served("unary"));
    assert_eq!(as_set(STREAM_FILTERS), served("filters"));
    let fake = Fake::default();
    let c = client(&fake).await;
    c.get_block_height(None).await.unwrap();
    c.get_latest_blockhash(None).await.unwrap();
    assert_eq!(
        c.get_slot(Some(CommitmentLevel::Confirmed))
            .await
            .unwrap()
            .slot,
        7
    );
    c.get_version().await.unwrap();
    assert!(c.is_blockhash_valid("h", None).await.unwrap().valid);
    c.ping(3).await.unwrap();
    c.subscribe_replay_info().await.unwrap();
    let (calls, tokens) = {
        let s = fake.0.lock().unwrap();
        (s.calls.clone(), s.tokens.clone())
    };
    assert_eq!(
        calls.iter().cloned().collect::<BTreeSet<_>>(),
        served("unary"),
        "the wire names of the unary calls"
    );
    assert!(tokens.iter().all(|t| *t == key()), "x-token on every call");
    assert!(!format!("{c:?}").contains(&key()), "{c:?}");
    let mut every = slots();
    every.accounts.insert(
        "a".into(),
        SubscribeRequestFilterAccounts {
            account: vec!["x".into()],
            ..Default::default()
        },
    );
    every.transactions.insert("t".into(), tx(&["p"]));
    every.transactions_status.insert("ts".into(), tx(&["p"]));
    every
        .blocks
        .insert("b".into(), SubscribeRequestFilterBlocks::default());
    every
        .blocks_meta
        .insert("bm".into(), SubscribeRequestFilterBlocksMeta::default());
    every
        .entry
        .insert("e".into(), SubscribeRequestFilterEntry::default());
    let mut s = c.subscribe_with_reconnect(&every, no_jitter()).unwrap();
    events_until(&mut s, 3000, |e| {
        e.iter().any(|x| matches!(x, StreamEvent::Connected { .. }))
    })
    .await;
    let sent = fake.0.lock().unwrap().subscribes[0].1.clone();
    assert_eq!(filter_names(&sent), served("filters"));
}

#[tokio::test]
async fn shapes_the_service_refuses_are_refused_before_anything_is_sent() {
    let fake = Fake::default();
    let c = client(&fake).await;
    let mut cases: Vec<(&str, StreamFilters)> = Vec::new();
    let mut f = slots();
    f.transactions
        .insert("all".into(), SubscribeRequestFilterTransactions::default());
    cases.push(("bare transaction filter", f));
    let mut f = slots();
    f.transactions_status.insert(
        "t".into(),
        SubscribeRequestFilterTransactions {
            token_accounts: Some(1),
            ..tx(&["p"])
        },
    );
    cases.push(("tokenAccounts", f));
    let mut f = slots();
    f.transactions.insert(
        "t".into(),
        SubscribeRequestFilterTransactions {
            cuckoo_account_include: Some(Default::default()),
            ..tx(&["p"])
        },
    );
    cases.push(("cuckooAccountInclude", f));
    let mut f = slots();
    f.accounts.insert(
        "a".into(),
        SubscribeRequestFilterAccounts {
            cuckoo_accounts_filter: Some(Default::default()),
            ..Default::default()
        },
    );
    cases.push(("cuckooAccountsFilter", f));
    let mut f = slots();
    f.blocks.insert(
        "b".into(),
        SubscribeRequestFilterBlocks {
            cuckoo_account_include: Some(Default::default()),
            ..Default::default()
        },
    );
    cases.push(("blocks cuckooAccountInclude", f));
    let mut f = slots();
    f.entry.insert(
        "e".into(),
        SubscribeRequestFilterEntry {
            include_update_parent: Some(true),
        },
    );
    cases.push(("includeUpdateParent", f));
    let mut f = slots();
    f.from_slot = Some(5);
    f.commitment = Some(CommitmentLevel::Confirmed);
    cases.push(("fromSlot above processed", f));
    for (what, filters) in &cases {
        assert_eq!(
            build_request(filters).err().map(|e| e.kind()),
            Some(ErrorKind::Request),
            "{what}"
        );
        assert!(
            c.subscribe_with_reconnect(filters, no_jitter()).is_err(),
            "{what}"
        );
    }
    let mut fine = slots();
    fine.transactions.insert(
        "t".into(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            ..Default::default()
        },
    );
    assert!(build_request(&fine).is_ok(), "a narrowed filter passes");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fake.0.lock().unwrap().subscribes.len(), 0);
}

#[tokio::test]
async fn a_key_plan_or_request_refusal_stops_the_stream_without_a_retry() {
    for (code, message, reason) in [
        (Code::Unauthenticated, "unknown key", "UNAUTHENTICATED"),
        (
            Code::PermissionDenied,
            "heavy streams are not included in this plan; upgrade to BUSINESS",
            "PERMISSION_DENIED",
        ),
        (Code::InvalidArgument, "bad filter", "INVALID_ARGUMENT"),
        (Code::Unimplemented, "not served", "UNIMPLEMENTED"),
        (
            Code::ResourceExhausted,
            "credits exhausted",
            "RESOURCE_EXHAUSTED",
        ),
        (
            Code::ResourceExhausted,
            "consumer_credits_exhausted",
            "RESOURCE_EXHAUSTED",
        ),
    ] {
        let fake = Fake::default();
        fake.0
            .lock()
            .unwrap()
            .script
            .push_back(vec![Act::End(code, message)]);
        let c = client(&fake).await;
        let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
        let seen = events_until(&mut s, 3000, |e| {
            e.iter()
                .any(|x| matches!(x, StreamEvent::Stopped(_) | StreamEvent::Retry { .. }))
        })
        .await;
        match seen.last() {
            Some(StreamEvent::Stopped(Some(e))) => assert_eq!(
                (e.kind(), e.reason(), e.is_retryable()),
                (ErrorKind::Stream, Some(reason), false),
                "{message}"
            ),
            other => panic!("{message}: {other:?}"),
        }
        assert_eq!(fake.0.lock().unwrap().subscribes.len(), 1, "{message}");
    }
}

#[tokio::test]
async fn a_busy_refusal_is_retried_after_at_least_2_s() {
    let fake = Fake::default();
    fake.0
        .lock()
        .unwrap()
        .script
        .push_back(vec![Act::End(Code::ResourceExhausted, "overloaded")]);
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    let seen = events_until(&mut s, 6000, |e| {
        e.iter()
            .filter(|x| matches!(x, StreamEvent::Connected { .. }))
            .count()
            == 2
    })
    .await;
    assert!(
        seen.iter()
            .any(|x| matches!(x, StreamEvent::Retry { delay_ms: 2000, .. })),
        "{seen:?}"
    );
    let at: Vec<Instant> = fake
        .0
        .lock()
        .unwrap()
        .subscribes
        .iter()
        .map(|(t, _)| *t)
        .collect();
    assert!(
        at[1] - at[0] >= Duration::from_millis(2000),
        "reconnected after {:?}",
        at[1] - at[0]
    );
}

#[tokio::test]
async fn out_of_range_reseeds_and_goes_live_at_once() {
    let fake = Fake::default();
    fake.0.lock().unwrap().script.push_back(vec![
        Act::Slot(41),
        Act::End(Code::OutOfRange, "the slot is past the retained range"),
    ]);
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    let seen = events_until(&mut s, 3000, |e| {
        e.iter()
            .filter(|x| matches!(x, StreamEvent::Connected { .. }))
            .count()
            == 2
    })
    .await;
    assert!(
        seen.iter().any(|x| matches!(
            x,
            StreamEvent::Reseed {
                last_slot: Some(41),
                error: Some(_)
            }
        )),
        "{seen:?}"
    );
    let subs = fake.0.lock().unwrap().subscribes.clone();
    assert!(
        subs[1].0 - subs[0].0 < Duration::from_millis(1000),
        "waited {:?}",
        subs[1].0 - subs[0].0
    );
    assert_eq!(
        subs[1].1.from_slot, None,
        "a re-seed goes live, not from the old slot"
    );
}

#[tokio::test]
async fn the_backoff_starts_at_2_s_doubles_and_resets_after_data() {
    let fake = Fake::default();
    {
        let mut st = fake.0.lock().unwrap();
        st.script
            .push_back(vec![Act::End(Code::Unavailable, "restart")]);
        st.script
            .push_back(vec![Act::End(Code::Unavailable, "restart")]);
        st.script
            .push_back(vec![Act::Slot(9), Act::End(Code::Unavailable, "restart")]);
        st.first_available = 0;
    }
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    let seen = events_until(&mut s, 15000, |e| {
        e.iter()
            .filter(|x| matches!(x, StreamEvent::Retry { .. }))
            .count()
            == 3
    })
    .await;
    let delays: Vec<u64> = seen
        .iter()
        .filter_map(|x| {
            if let StreamEvent::Retry { delay_ms, .. } = x {
                Some(*delay_ms)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(delays, [2000, 4000, 2000]);
    let mut b = 2000;
    let caps: Vec<u64> = (0..6)
        .map(|_| {
            b = next_backoff_ms(b);
            b
        })
        .collect();
    assert_eq!(caps, [4000, 8000, 16000, 30000, 30000, 30000]);
    assert_eq!(
        retry_delay_ms(1000, 2000, || 0.999),
        2999,
        "the larger wait plus up to 1 s of jitter"
    );
}

#[tokio::test]
async fn a_retry_resumes_from_the_last_slot_when_the_floor_allows_and_reseeds_when_it_does_not() {
    for (floor, want_from, want_reseed) in [(100, Some(105), false), (200, None, true)] {
        let fake = Fake::default();
        {
            let mut st = fake.0.lock().unwrap();
            st.script.push_back(vec![
                Act::Slot(104),
                Act::Slot(105),
                Act::End(Code::Unavailable, "restart"),
            ]);
            st.first_available = floor;
        }
        let c = client(&fake).await;
        let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
        let seen = events_until(&mut s, 6000, |e| {
            e.iter()
                .filter(|x| matches!(x, StreamEvent::Connected { .. }))
                .count()
                == 2
        })
        .await;
        let subs = fake.0.lock().unwrap().subscribes.clone();
        assert_eq!(subs[1].1.from_slot, want_from, "floor {floor}");
        assert_eq!(
            seen.iter().any(|x| matches!(
                x,
                StreamEvent::Reseed {
                    error: None,
                    last_slot: Some(105)
                }
            )),
            want_reseed,
            "floor {floor}: {seen:?}"
        );
        assert!(
            fake.0
                .lock()
                .unwrap()
                .calls
                .contains(&"SubscribeReplayInfo".to_string())
        );
    }
    let fake = Fake::default();
    fake.0
        .lock()
        .unwrap()
        .script
        .push_back(vec![Act::Slot(105), Act::End(Code::Unavailable, "restart")]);
    let c = client(&fake).await;
    let mut confirmed = slots();
    confirmed.commitment = Some(CommitmentLevel::Confirmed);
    let mut s = c.subscribe_with_reconnect(&confirmed, no_jitter()).unwrap();
    events_until(&mut s, 6000, |e| {
        e.iter()
            .filter(|x| matches!(x, StreamEvent::Connected { .. }))
            .count()
            == 2
    })
    .await;
    let st = fake.0.lock().unwrap();
    assert_eq!(
        (
            st.subscribes[1].1.from_slot,
            st.calls.contains(&"SubscribeReplayInfo".to_string())
        ),
        (None, false),
        "no replay above processed"
    );
}

#[tokio::test]
async fn pings_go_out_on_their_interval_never_more_than_one_unanswered() {
    for (pong, at_least, at_most) in [(false, 1, 1), (true, 3, 20)] {
        let fake = Fake::default();
        fake.0.lock().unwrap().pong = pong;
        let c = client(&fake).await;
        let options = ReconnectOptions {
            ping_every: Duration::from_millis(150),
            ..no_jitter()
        };
        let mut s = c.subscribe_with_reconnect(&slots(), options).unwrap();
        events_until(&mut s, 3000, |e| {
            e.iter().any(|x| matches!(x, StreamEvent::Connected { .. }))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let pings = fake.0.lock().unwrap().pings;
        assert!(
            (at_least..=at_most).contains(&pings),
            "pong {pong}: {pings} pings"
        );
        s.close();
    }
}

#[tokio::test]
async fn an_account_key_or_a_key_in_the_endpoint_is_refused_before_connecting() {
    let acct = ["c_acct", "_x0000000000"].concat();
    assert_eq!(
        CarbiumStream::connect(StreamOptions::new(acct))
            .await
            .err()
            .map(|e| e.kind()),
        Some(ErrorKind::Auth)
    );
    let endpoint = format!("https://x.test:9443/?api-key={}", "x".repeat(10));
    let e = CarbiumStream::connect(StreamOptions {
        endpoint,
        ..StreamOptions::new(key())
    })
    .await
    .err()
    .map(|e| e.kind());
    assert_eq!(e, Some(ErrorKind::Request));
}

#[tokio::test]
async fn a_transaction_status_update_records_the_slot_a_retry_resumes_from() {
    let fake = Fake::default();
    {
        let mut st = fake.0.lock().unwrap();
        st.script.push_back(vec![
            Act::TxStatus(105),
            Act::End(Code::Unavailable, "restart"),
        ]);
        st.first_available = 100;
    }
    let c = client(&fake).await;
    let mut statuses = StreamFilters::default();
    statuses.transactions_status.insert("ts".into(), tx(&["p"]));
    let mut s = c.subscribe_with_reconnect(&statuses, no_jitter()).unwrap();
    events_until(&mut s, 6000, |e| {
        e.iter()
            .filter(|x| matches!(x, StreamEvent::Connected { .. }))
            .count()
            == 2
    })
    .await;
    assert_eq!(fake.0.lock().unwrap().subscribes[1].1.from_slot, Some(105));
}

#[tokio::test]
async fn a_failed_floor_read_reseeds_before_going_live() {
    let fake = Fake::default();
    {
        let mut st = fake.0.lock().unwrap();
        st.script
            .push_back(vec![Act::Slot(105), Act::End(Code::Unavailable, "restart")]);
        st.replay_fails = true;
    }
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    let seen = events_until(&mut s, 6000, |e| {
        e.iter()
            .filter(|x| matches!(x, StreamEvent::Connected { .. }))
            .count()
            == 2
    })
    .await;
    assert!(
        seen.iter().any(|x| matches!(
            x,
            StreamEvent::Reseed {
                error: Some(_),
                last_slot: Some(105)
            }
        )),
        "{seen:?}"
    );
    assert_eq!(fake.0.lock().unwrap().subscribes[1].1.from_slot, None);
}

#[tokio::test]
async fn unary_calls_on_one_client_run_at_the_same_time() {
    let fake = Fake::default();
    fake.0.lock().unwrap().slot_delay_ms = 300;
    let c = client(&fake).await;
    let (a, b) = tokio::join!(c.get_slot(None), c.get_slot(None));
    assert!(a.is_ok() && b.is_ok());
    assert_eq!(
        fake.0.lock().unwrap().max_in_flight,
        2,
        "the second call waited for the first"
    );
}

#[tokio::test]
async fn a_plain_stream_keeps_its_request_side_open_pings_and_ends_when_dropped() {
    let fake = Fake::default();
    let c = client(&fake).await;
    let mut h = c.subscribe(&slots()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        fake.0.lock().unwrap().incoming_closed,
        0,
        "the request side closed after the first message"
    );
    h.ping(7).await.unwrap();
    let t0 = Instant::now();
    while fake.0.lock().unwrap().pings < 1 {
        assert!(t0.elapsed() < Duration::from_secs(3), "no ping arrived");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(h);
    while fake.0.lock().unwrap().incoming_closed < 1 {
        assert!(
            t0.elapsed() < Duration::from_secs(3),
            "the dropped stream stayed open"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn close_ends_the_events_with_stopped_none() {
    let fake = Fake::default();
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    events_until(&mut s, 3000, |e| {
        e.iter().any(|x| matches!(x, StreamEvent::Connected { .. }))
    })
    .await;
    s.close();
    assert!(matches!(s.next().await, Some(StreamEvent::Stopped(None))));
    assert!(s.next().await.is_none());
}

#[tokio::test]
async fn a_retry_after_in_the_status_metadata_lengthens_the_wait() {
    let fake = Fake::default();
    fake.0
        .lock()
        .unwrap()
        .script
        .push_back(vec![Act::EndRetryAfter(
            Code::ResourceExhausted,
            "too many streams are resuming from a past slot; retry shortly",
            "3",
        )]);
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    let seen = events_until(&mut s, 3000, |e| {
        e.iter().any(|x| matches!(x, StreamEvent::Retry { .. }))
    })
    .await;
    assert!(
        seen.iter()
            .any(|x| matches!(x, StreamEvent::Retry { delay_ms: 3000, .. })),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_refusal_at_open_stops_the_stream() {
    let fake = Fake::default();
    fake.0.lock().unwrap().refuse_open = Some((Code::Unauthenticated, "unknown api key"));
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    let seen = events_until(&mut s, 3000, |e| {
        e.iter()
            .any(|x| matches!(x, StreamEvent::Stopped(_) | StreamEvent::Retry { .. }))
    })
    .await;
    match seen.last() {
        Some(StreamEvent::Stopped(Some(e))) => assert_eq!(e.reason(), Some("UNAUTHENTICATED")),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        fake.0
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|c| *c == "Subscribe")
            .count(),
        1
    );
}

#[tokio::test]
async fn dropping_a_reconnecting_stream_ends_the_call_on_the_server() {
    let fake = Fake::default();
    let c = client(&fake).await;
    let mut s = c.subscribe_with_reconnect(&slots(), no_jitter()).unwrap();
    events_until(&mut s, 3000, |e| {
        e.iter().any(|x| matches!(x, StreamEvent::Connected { .. }))
    })
    .await;
    drop(s);
    let t0 = Instant::now();
    while fake.0.lock().unwrap().incoming_closed < 1 {
        assert!(
            t0.elapsed() < Duration::from_secs(3),
            "the server still holds the stream"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[test]
fn the_jitter_adds_0_to_999_ms() {
    assert_eq!(retry_delay_ms(2000, 2000, || 0.0), 2000);
    assert_eq!(retry_delay_ms(2000, 2000, || 0.999_999), 2999);
    assert_eq!(retry_delay_ms(1000, 4000, || 0.5), 4500);
}

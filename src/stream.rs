//! Carbon Stream, the Yellowstone (Geyser) gRPC stream on services.carbium.sh:9443, on the stock
//! yellowstone-grpc-client. The key travels as x-token metadata. `subscribe_with_reconnect` follows the published
//! rules: stop on a key, plan or request refusal, re-seed on OUT_OF_RANGE, back off from 2 s to 30 s otherwise, and
//! resume from the last slot seen when the retained floor allows.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, GetBlockHeightResponse, GetLatestBlockhashResponse, GetSlotResponse,
    GetVersionResponse, IsBlockhashValidResponse, PongResponse, SubscribeReplayInfoResponse,
    SubscribeRequest, SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
    SubscribeUpdate, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::{Code, Status};

/// The stock Yellowstone message types a caller needs. The generated client and server are not offered: they would
/// expose calls the service does not serve.
///
/// ```compile_fail
/// use carbium::stream::proto::geyser_client;
/// ```
///
/// ```compile_fail
/// use carbium::stream::proto::geyser_server;
/// ```
pub mod proto {
    pub use yellowstone_grpc_proto::geyser::{
        CommitmentLevel, GetBlockHeightResponse, GetLatestBlockhashResponse, GetSlotResponse,
        GetVersionResponse, IsBlockhashValidResponse, PongResponse, SlotStatus,
        SubscribeReplayInfoResponse, SubscribeRequest, SubscribeRequestAccountsDataSlice,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterAccountsFilterLamports, SubscribeRequestFilterAccountsFilterMemcmp,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
        SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
        SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdatePing, SubscribeUpdatePong,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        SubscribeUpdateTransactionStatus, subscribe_request_filter_accounts_filter,
        subscribe_request_filter_accounts_filter_lamports,
        subscribe_request_filter_accounts_filter_memcmp, subscribe_update,
    };
    pub use yellowstone_grpc_proto::solana::storage::confirmed_block;
}

use crate::error::{Code as ErrorCode, Error, ErrorKind, Result, Secret};
use crate::transport::{is_key, refuse_key_in_url};

pub const DEFAULT_STREAM_ENDPOINT: &str = "https://services.carbium.sh:9443";

/// The filter kinds the service serves, as served.json names them. Nothing else can be sent.
pub const STREAM_FILTERS: &[&str] = &[
    "accounts",
    "blocks",
    "blocksMeta",
    "entry",
    "slots",
    "transactions",
    "transactionsStatus",
];

/// The unary calls the service serves, as served.json names them. The crate offers no other.
pub const STREAM_UNARY: &[&str] = &[
    "GetBlockHeight",
    "GetLatestBlockhash",
    "GetSlot",
    "GetVersion",
    "IsBlockhashValid",
    "Ping",
    "SubscribeReplayInfo",
];

const PING_EVERY: Duration = Duration::from_secs(10);

/// Options for [`CarbiumStream::connect`].
#[derive(Clone)]
pub struct StreamOptions {
    /// The full service key (c_serv_…) with the grpc scope.
    pub api_key: String,
    /// Default https://services.carbium.sh:9443.
    pub endpoint: String,
    /// The largest message the client reads. Default 64 MiB.
    pub max_decoding_message_size: usize,
}

impl StreamOptions {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: DEFAULT_STREAM_ENDPOINT.into(),
            max_decoding_message_size: 64 * 1024 * 1024,
        }
    }
}

impl fmt::Debug for StreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamOptions")
            .field("api_key", &Secret::new(self.api_key.clone()))
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// The served filter kinds and the request-level fields. Unset commitment means processed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamFilters {
    pub accounts: HashMap<String, SubscribeRequestFilterAccounts>,
    pub slots: HashMap<String, SubscribeRequestFilterSlots>,
    pub transactions: HashMap<String, SubscribeRequestFilterTransactions>,
    pub transactions_status: HashMap<String, SubscribeRequestFilterTransactions>,
    pub blocks: HashMap<String, SubscribeRequestFilterBlocks>,
    pub blocks_meta: HashMap<String, SubscribeRequestFilterBlocksMeta>,
    pub entry: HashMap<String, SubscribeRequestFilterEntry>,
    pub commitment: Option<CommitmentLevel>,
    pub accounts_data_slice: Vec<SubscribeRequestAccountsDataSlice>,
    /// Replay from this slot: processed commitment only.
    pub from_slot: Option<u64>,
}

fn refused(what: &str) -> Error {
    Error::new(
        ErrorKind::Request,
        format!("{what} is refused by Carbon Stream: remove the field"),
    )
}

/// The SubscribeRequest for `filters`, refusing before anything is sent what the service refuses.
pub fn build_request(filters: &StreamFilters) -> Result<SubscribeRequest> {
    for (name, f) in &filters.accounts {
        if f.cuckoo_accounts_filter.is_some() {
            return Err(refused(&format!("accounts.{name}.cuckooAccountsFilter")));
        }
    }
    for (kind, map) in [
        ("transactions", &filters.transactions),
        ("transactionsStatus", &filters.transactions_status),
    ] {
        for (name, t) in map {
            if t.token_accounts.is_some() {
                return Err(refused(&format!("{kind}.{name}.tokenAccounts")));
            }
            if t.cuckoo_account_include.is_some() {
                return Err(refused(&format!("{kind}.{name}.cuckooAccountInclude")));
            }
            let bare = t.vote.is_none()
                && t.failed.is_none()
                && t.signature.is_none()
                && t.account_include.is_empty()
                && t.account_exclude.is_empty()
                && t.account_required.is_empty();
            if bare {
                return Err(Error::new(
                    ErrorKind::Request,
                    format!(
                        "{kind}.{name} is a bare transaction filter: name accountInclude, accountRequired, accountExclude, signature, vote or failed"
                    ),
                ));
            }
        }
    }
    for (name, f) in &filters.blocks {
        if f.cuckoo_account_include.is_some() {
            return Err(refused(&format!("blocks.{name}.cuckooAccountInclude")));
        }
    }
    for (name, f) in &filters.entry {
        if f.include_update_parent == Some(true) {
            return Err(refused(&format!("entry.{name}.includeUpdateParent")));
        }
    }
    if filters.from_slot.is_some()
        && filters
            .commitment
            .is_some_and(|c| c != CommitmentLevel::Processed)
    {
        return Err(Error::new(
            ErrorKind::Request,
            "fromSlot replay is available only at processed commitment",
        ));
    }
    Ok(SubscribeRequest {
        accounts: filters.accounts.clone(),
        slots: filters.slots.clone(),
        transactions: filters.transactions.clone(),
        transactions_status: filters.transactions_status.clone(),
        blocks: filters.blocks.clone(),
        blocks_meta: filters.blocks_meta.clone(),
        entry: filters.entry.clone(),
        commitment: filters.commitment.map(|c| c as i32),
        accounts_data_slice: filters.accounts_data_slice.clone(),
        ping: None,
        from_slot: filters.from_slot,
        block_footer: HashMap::new(),
    })
}

/// What to do after a stream ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamAction {
    Stop,
    Reseed,
    Retry,
}

/// A gRPC status read the published way: its name, its message, the action and the table wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamErrorInfo {
    pub status: &'static str,
    pub message: String,
    pub action: StreamAction,
    pub retry_after_ms: u64,
}

fn status_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// Stop, re-seed or retry, and the wait the published table gives, for a status that ended a stream or a call.
pub fn classify(status: &Status) -> StreamErrorInfo {
    let name = status_name(status.code());
    let message = if status.message().is_empty() {
        "stream ended".to_string()
    } else {
        status.message().to_string()
    };
    let (mut action, mut retry_after_ms) = (StreamAction::Retry, 2000);
    match status.code() {
        Code::Unauthenticated
        | Code::PermissionDenied
        | Code::InvalidArgument
        | Code::Unimplemented => action = StreamAction::Stop,
        Code::OutOfRange => action = StreamAction::Reseed,
        Code::ResourceExhausted
            if message
                .to_ascii_lowercase()
                .replace('_', " ")
                .contains("credits exhausted") =>
        {
            action = StreamAction::Stop
        }
        Code::Unavailable if message.to_ascii_lowercase().contains("still loading") => {
            retry_after_ms = 1000
        }
        _ => {}
    }
    let hinted = status
        .metadata()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if let Some(seconds) = hinted {
        retry_after_ms = retry_after_ms.max(seconds.saturating_mul(1000));
    }
    StreamErrorInfo {
        status: name,
        message,
        action,
        retry_after_ms,
    }
}

fn stream_error(method: Option<&str>, info: &StreamErrorInfo) -> Error {
    let e = Error::new(ErrorKind::Stream, &info.message)
        .with_reason(info.status)
        .with_code(ErrorCode::Text(info.status.to_string()))
        .with_retryable(info.action != StreamAction::Stop)
        .with_retry_after(Some(info.retry_after_ms));
    match method {
        Some(m) => e.with_method(m),
        None => e,
    }
}

fn client_error(method: Option<&str>, e: GeyserGrpcClientError) -> Error {
    let status = match e {
        GeyserGrpcClientError::TonicStatus(s) => s,
        GeyserGrpcClientError::TransportError(t) => Status::unavailable(t.to_string()),
    };
    stream_error(method, &classify(&status))
}

/// The Carbon Stream client: the seven unary calls and Subscribe, with or without reconnect.
pub struct CarbiumStream {
    endpoint: String,
    client: GeyserGrpcClient,
}

impl fmt::Debug for CarbiumStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarbiumStream")
            .field("endpoint", &self.endpoint)
            .field("x_token", &"c_serv_…")
            .finish()
    }
}

/// One plain stream: both halves of the call, kept open. It offers the in-stream ping and nothing that changes the
/// filters, which the service ignores after the first message. Dropping it ends the stream.
pub struct StreamHandle {
    sink: yellowstone_grpc_client::SubscribeRequestSink,
    stream: yellowstone_grpc_client::GeyserStream,
    live: SubscribeRequest,
}

impl StreamHandle {
    /// The next update, or None when the stream ended; an error ends it too.
    pub async fn next(&mut self) -> Option<Result<SubscribeUpdate>> {
        let item = self.stream.next().await?;
        Some(item.map_err(|s| stream_error(Some("Subscribe"), &classify(&s))))
    }

    /// Sends an in-stream ping; the service answers with a pong update carrying `id`.
    pub async fn ping(&mut self, id: i32) -> Result<()> {
        let ping = SubscribeRequest {
            ping: Some(SubscribeRequestPing { id }),
            ..self.live.clone()
        };
        self.sink.send(ping).await.map_err(|e| {
            stream_error(
                Some("Subscribe"),
                &classify(&Status::unavailable(e.to_string())),
            )
        })
    }
}

/// What a reconnecting subscription yields.
#[derive(Debug)]
pub enum StreamEvent {
    Update(Box<SubscribeUpdate>),
    /// A stream opened; `from_slot` is set when it resumed.
    Connected {
        attempt: u32,
        from_slot: Option<u64>,
    },
    /// The stream ended and is opened again after `delay_ms`.
    Retry {
        error: Error,
        delay_ms: u64,
        attempt: u32,
    },
    /// The service cannot replay what was missed (OUT_OF_RANGE, or the floor passed the last slot): re-read state,
    /// then the stream goes on live.
    Reseed {
        error: Option<Error>,
        last_slot: Option<u64>,
    },
    /// The stream stopped for good: a key, plan or request refusal, or None after `close`.
    Stopped(Option<Error>),
}

/// Options for [`CarbiumStream::subscribe_with_reconnect`].
#[derive(Debug, Clone, Copy)]
pub struct ReconnectOptions {
    /// Random source for the jitter, 0 ≤ r < 1.
    pub random: fn() -> f64,
    /// An in-stream ping this often, never with more than one pong outstanding. Default 10 s.
    pub ping_every: Duration,
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        Self {
            random: crate::retry::default_random,
            ping_every: PING_EVERY,
        }
    }
}

/// A subscription that reconnects on its own. Dropping it, or `close`, ends it.
///
/// Updates wait in a small queue (32 frames): a slow reader slows the stream, and the service cuts a reader that takes
/// nothing for 10 s.
pub struct ReconnectingStream {
    events: mpsc::Receiver<StreamEvent>,
    task: tokio::task::JoinHandle<()>,
    closed: bool,
    stopped_sent: bool,
}

impl ReconnectingStream {
    /// The next update or status, or None once the stream stopped.
    pub async fn next(&mut self) -> Option<StreamEvent> {
        if self.closed {
            if self.stopped_sent {
                return None;
            }
            self.stopped_sent = true;
            return Some(StreamEvent::Stopped(None));
        }
        self.events.recv().await
    }

    /// Ends the stream: the next event is `Stopped(None)`, then the events end.
    pub fn close(&mut self) {
        self.task.abort();
        self.closed = true;
    }
}

impl Drop for ReconnectingStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The wait before the next attempt: the larger of the table wait and the backoff, plus up to 1 s of jitter.
pub fn retry_delay_ms(retry_after_ms: u64, backoff_ms: u64, random: fn() -> f64) -> u64 {
    retry_after_ms
        .max(backoff_ms)
        .saturating_add((random() * 1000.0).floor() as u64)
}

/// The backoff after one more failure in a row: doubled, at most 30 s.
pub fn next_backoff_ms(backoff_ms: u64) -> u64 {
    backoff_ms.saturating_mul(2).min(30_000)
}

fn slot_of(update: &SubscribeUpdate) -> Option<u64> {
    match update.update_oneof.as_ref()? {
        UpdateOneof::Slot(s) => Some(s.slot),
        UpdateOneof::Transaction(t) => Some(t.slot),
        UpdateOneof::Account(a) => Some(a.slot),
        UpdateOneof::BlockMeta(b) => Some(b.slot),
        UpdateOneof::Block(b) => Some(b.slot),
        UpdateOneof::Entry(e) => Some(e.slot),
        UpdateOneof::TransactionStatus(t) => Some(t.slot),
        _ => None,
    }
}

impl CarbiumStream {
    /// Connects with the key as x-token metadata. Refuses a malformed key and a key in the endpoint.
    pub async fn connect(options: StreamOptions) -> Result<Self> {
        if !is_key(&options.api_key, "c_serv_") {
            return Err(Error::new(
                ErrorKind::Auth,
                "api_key must be the full service key (c_serv_…) with the grpc scope",
            ));
        }
        refuse_key_in_url(&options.endpoint)?;
        let builder_error = |e: yellowstone_grpc_client::GeyserGrpcBuilderError| {
            Error::new(ErrorKind::Stream, e.to_string()).with_cause(&e)
        };
        let mut builder = GeyserGrpcClient::build_from_shared(options.endpoint.clone())
            .map_err(builder_error)?
            .x_token(Some(options.api_key.clone()))
            .map_err(|_| Error::new(ErrorKind::Auth, "api_key is not a metadata value"))?
            .max_decoding_message_size(options.max_decoding_message_size)
            .connect_timeout(Duration::from_secs(10));
        if options.endpoint.starts_with("https") {
            builder = builder
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(builder_error)?;
        }
        let client = builder.connect().await.map_err(builder_error)?;
        Ok(Self {
            endpoint: options.endpoint,
            client,
        })
    }

    pub async fn get_slot(&self, commitment: Option<CommitmentLevel>) -> Result<GetSlotResponse> {
        self.client
            .clone()
            .get_slot(commitment)
            .await
            .map_err(|e| client_error(Some("GetSlot"), e))
    }
    pub async fn get_block_height(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetBlockHeightResponse> {
        self.client
            .clone()
            .get_block_height(commitment)
            .await
            .map_err(|e| client_error(Some("GetBlockHeight"), e))
    }
    pub async fn get_latest_blockhash(
        &self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetLatestBlockhashResponse> {
        self.client
            .clone()
            .get_latest_blockhash(commitment)
            .await
            .map_err(|e| client_error(Some("GetLatestBlockhash"), e))
    }
    pub async fn is_blockhash_valid(
        &self,
        blockhash: &str,
        commitment: Option<CommitmentLevel>,
    ) -> Result<IsBlockhashValidResponse> {
        self.client
            .clone()
            .is_blockhash_valid(blockhash.to_string(), commitment)
            .await
            .map_err(|e| client_error(Some("IsBlockhashValid"), e))
    }
    pub async fn get_version(&self) -> Result<GetVersionResponse> {
        self.client
            .clone()
            .get_version()
            .await
            .map_err(|e| client_error(Some("GetVersion"), e))
    }
    pub async fn ping(&self, count: i32) -> Result<PongResponse> {
        self.client
            .clone()
            .ping(count)
            .await
            .map_err(|e| client_error(Some("Ping"), e))
    }
    pub async fn subscribe_replay_info(&self) -> Result<SubscribeReplayInfoResponse> {
        self.client
            .clone()
            .subscribe_replay_info()
            .await
            .map_err(|e| client_error(Some("SubscribeReplayInfo"), e))
    }

    /// One plain stream with no reconnect: the request is checked first.
    pub async fn subscribe(&self, filters: &StreamFilters) -> Result<StreamHandle> {
        let request = build_request(filters)?;
        let live = SubscribeRequest {
            from_slot: None,
            ..request.clone()
        };
        let (sink, stream) = self
            .client
            .clone()
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| client_error(Some("Subscribe"), e))?;
        Ok(StreamHandle { sink, stream, live })
    }

    /// A stream that follows the published rules (see the module docs). The request is checked before any call.
    /// Updates and status changes come in order from `next`.
    pub fn subscribe_with_reconnect(
        &self,
        filters: &StreamFilters,
        options: ReconnectOptions,
    ) -> Result<ReconnectingStream> {
        let base = build_request(filters)?;
        let (tx, events) = mpsc::channel(32);
        let task = tokio::spawn(run(self.client.clone(), base, options, tx));
        Ok(ReconnectingStream {
            events,
            task,
            closed: false,
            stopped_sent: false,
        })
    }
}

async fn run(
    client: GeyserGrpcClient,
    base: SubscribeRequest,
    options: ReconnectOptions,
    tx: mpsc::Sender<StreamEvent>,
) {
    let can_resume = base
        .commitment
        .is_none_or(|c| c == CommitmentLevel::Processed as i32);
    let live = SubscribeRequest {
        from_slot: None,
        ..base.clone()
    };
    let mut last_slot = base.from_slot;
    let mut request = base;
    let (mut backoff_ms, mut attempt) = (2000u64, 0u32);
    loop {
        let mut got_data = false;
        let mut ended: Option<Status> = None;
        let opened = client
            .clone()
            .subscribe_with_request(Some(request.clone()))
            .await;
        match opened {
            Err(GeyserGrpcClientError::TonicStatus(s)) => ended = Some(s),
            Err(GeyserGrpcClientError::TransportError(t)) => {
                ended = Some(Status::unavailable(t.to_string()))
            }
            Ok((mut sink, mut stream)) => {
                if tx
                    .send(StreamEvent::Connected {
                        attempt,
                        from_slot: request.from_slot,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                let mut pinger = tokio::time::interval_at(
                    tokio::time::Instant::now() + options.ping_every,
                    options.ping_every,
                );
                let (mut ping_id, mut pong_pending) = (0i32, false);
                loop {
                    tokio::select! {
                        _ = pinger.tick() => {
                            if pong_pending {
                                continue; // the control queue holds 8 messages: never pile pings up
                            }
                            pong_pending = true;
                            ping_id += 1;
                            let ping = SubscribeRequest { ping: Some(SubscribeRequestPing { id: ping_id }), ..live.clone() };
                            if let Err(e) = sink.send(ping).await {
                                ended = Some(Status::unavailable(e.to_string()));
                                break;
                            }
                        }
                        item = stream.next() => match item {
                            Some(Ok(update)) => match update.update_oneof {
                                Some(UpdateOneof::Pong(_)) => pong_pending = false,
                                Some(UpdateOneof::Ping(_)) => {}
                                _ => {
                                    got_data = true;
                                    if let Some(slot) = slot_of(&update) {
                                        last_slot = Some(slot);
                                    }
                                    if tx.send(StreamEvent::Update(Box::new(update))).await.is_err() {
                                        return;
                                    }
                                }
                            },
                            Some(Err(status)) => {
                                ended = Some(status);
                                break;
                            }
                            None => break,
                        },
                    }
                }
            }
        }
        let info = classify(&ended.unwrap_or_else(|| Status::unknown("stream ended")));
        let error = stream_error(None, &info);
        if info.action == StreamAction::Stop {
            let _ = tx.send(StreamEvent::Stopped(Some(error))).await;
            return;
        }
        if got_data {
            backoff_ms = 2000;
            attempt = 0;
        }
        attempt += 1;
        if info.action == StreamAction::Reseed {
            if tx
                .send(StreamEvent::Reseed {
                    error: Some(error),
                    last_slot,
                })
                .await
                .is_err()
            {
                return;
            }
            request = live.clone();
            continue;
        }
        let delay_ms = retry_delay_ms(info.retry_after_ms, backoff_ms, options.random);
        backoff_ms = next_backoff_ms(backoff_ms);
        if tx
            .send(StreamEvent::Retry {
                error,
                delay_ms,
                attempt,
            })
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        request = live.clone();
        if let (true, Some(slot)) = (can_resume, last_slot) {
            let floor = client
                .clone()
                .subscribe_replay_info()
                .await
                .map(|r| r.first_available.unwrap_or(0));
            match floor {
                Ok(first) if slot >= first => request.from_slot = Some(slot),
                Ok(_) => {
                    let gone = tx
                        .send(StreamEvent::Reseed {
                            error: None,
                            last_slot,
                        })
                        .await
                        .is_err();
                    if gone {
                        return;
                    }
                }
                Err(e) => {
                    // the floor cannot be read: the gap cannot be checked, so the caller re-reads before going live
                    let error = Some(client_error(Some("SubscribeReplayInfo"), e));
                    if tx
                        .send(StreamEvent::Reseed { error, last_slot })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }
}

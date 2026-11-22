//! JSON-RPC subscriptions over wss://services.carbium.sh with the published reconnect rules: every live subscription
//! is bound again after a reconnect, server ids are remapped, and a `Reseed` event tells you to re-read state. Nothing
//! is replayed by the server after a disconnect. One background task owns the connection.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, sleep_until};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, Message};

use crate::error::{Code, Error, ErrorKind, Result, Secret, decode};
use crate::transport::{is_key, refuse_key_in_url, rpc_error};
use crate::{
    SUBSCRIPTIONS, WithContextAccountInfo, WithContextBlockNotification, WithContextKeyedAccount,
    WithContextLogsNotification, WithContextSignatureNotification,
};

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type Connecting = Pin<Box<dyn Future<Output = std::result::Result<Ws, tungstenite::Error>> + Send>>;

const RETRY_REASONS: [&str; 5] = [
    "RATE_LIMITED",
    "LOAD_SHED",
    "FREE_SUBNET_LIMITED",
    "FREE_TIER_BUSY",
    "CREDITS_PENDING",
];
const BIND_BATCH: usize = 32;
const PING_WATCHDOG: Duration = Duration::from_secs(75);

/// How the key travels in the handshake. It never goes in the URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Carrier {
    /// The x-api-key handshake header: the default, as the guide lists it first.
    #[default]
    Header,
    /// Sec-WebSocket-Protocol `carbium, api-key.<key>`, the browsers' form; on request only.
    Subprotocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketState {
    Idle,
    Connecting,
    Open,
    Backoff,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CloseInfo {
    pub code: u16,
    pub reason: String,
}

/// What the socket tells its owner, on the receiver [`CarbiumSocket::new`] returns.
#[derive(Debug, Clone)]
pub enum SocketEvent {
    State(SocketState, Option<CloseInfo>),
    /// After every reconnect, once every live subscription is bound again. `gap` is true after close 1012
    /// stream_gap_reseed_required. Re-read state you may have missed.
    Reseed {
        close: CloseInfo,
        gap: bool,
    },
    /// A subscription the server refused when it was bound again after a reconnect.
    Error(Error),
    /// The socket gave up: a key refusal, a second credits_exhausted, or five handshakes in a row with no answer.
    Fatal(Error),
}

/// Options for [`CarbiumSocket::new`].
#[derive(Clone)]
pub struct SocketOptions {
    /// The full service key (c_serv_…) with the websocket scope.
    pub api_key: Option<String>,
    /// Default wss://services.carbium.sh.
    pub url: String,
    pub carrier: Carrier,
    /// Random source for the jitter, 0 ≤ r < 1.
    pub random: fn() -> f64,
}

impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            api_key: None,
            url: "wss://services.carbium.sh".into(),
            carrier: Carrier::Header,
            random: crate::retry::default_random,
        }
    }
}

impl fmt::Debug for SocketOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SocketOptions")
            .field("api_key", &self.api_key.clone().map(Secret::new))
            .field("url", &self.url)
            .field("carrier", &self.carrier)
            .finish()
    }
}

/// A slot notification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotInfo {
    pub slot: Number,
    pub parent: Number,
    pub root: Number,
}

enum Command {
    Subscribe {
        local: u64,
        method: String,
        params: Vec<Value>,
        notify: mpsc::UnboundedSender<Value>,
        ack: oneshot::Sender<Result<u64>>,
    },
    Unsubscribe {
        local: u64,
    },
    Close,
}

fn socket_error(message: &str) -> Error {
    Error::new(ErrorKind::Socket, message)
}

/// The subscription client. Cheap handles: dropping it keeps the connection while a [`Subscription`] lives.
pub struct CarbiumSocket {
    url: String,
    cmd: mpsc::UnboundedSender<Command>,
    state: watch::Receiver<SocketState>,
    size: Arc<AtomicUsize>,
    next_local: AtomicU64,
}

impl fmt::Debug for CarbiumSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarbiumSocket")
            .field("url", &self.url)
            .field("state", &self.state())
            .field("size", &self.size())
            .finish()
    }
}

/// One subscription: `ready` resolves with the server's id, `next` yields each notification. Dropping it, or calling
/// `unsubscribe`, ends it.
pub struct Subscription<T> {
    local: u64,
    method: String,
    cmd: mpsc::UnboundedSender<Command>,
    ack: Option<oneshot::Receiver<Result<u64>>>,
    id: Option<u64>,
    notes: mpsc::UnboundedReceiver<Value>,
    done: bool,
    _t: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> Subscription<T> {
    /// The server's subscription id once acknowledged, or the refusal.
    pub async fn ready(&mut self) -> Result<u64> {
        if let Some(id) = self.id {
            return Ok(id);
        }
        let Some(ack) = self.ack.take() else {
            return Err(
                socket_error("the subscription was refused or ended").with_method(&self.method)
            );
        };
        match ack.await {
            Ok(Ok(id)) => {
                self.id = Some(id);
                Ok(id)
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(socket_error("the socket was closed").with_method(&self.method)),
        }
    }

    /// The next notification, or None once the subscription ended (unsubscribed, signature delivered, socket gone).
    pub async fn next(&mut self) -> Option<Result<T>> {
        let v = self.notes.recv().await?;
        Some(decode(v, ErrorKind::Socket, &self.method, "a notification"))
    }

    /// Ends the subscription: the server is told when the socket is open. It returns before that frame is
    /// necessarily written, so a program that exits at once may not have told the server; closing the socket ends
    /// its subscriptions either way.
    pub fn unsubscribe(&mut self) {
        if !self.done {
            self.done = true;
            let _ = self.cmd.send(Command::Unsubscribe { local: self.local });
        }
    }
}

impl<T> Drop for Subscription<T> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.cmd.send(Command::Unsubscribe { local: self.local });
        }
    }
}

impl CarbiumSocket {
    /// A socket and the receiver of its events. Refuses an account key, a malformed key and a key in the URL. Needs a
    /// tokio runtime; nothing connects until the first subscription.
    pub fn new(options: SocketOptions) -> Result<(Self, mpsc::UnboundedReceiver<SocketEvent>)> {
        let key = options.api_key.clone().filter(|k| !k.is_empty());
        if key.as_deref().is_some_and(|k| !is_key(k, "c_serv_")) {
            return Err(Error::new(
                ErrorKind::Auth,
                "api_key must be the full service key (c_serv_…) with the websocket scope",
            ));
        }
        let url = options
            .url
            .replacen("http", "ws", usize::from(options.url.starts_with("http")))
            .trim_end_matches('/')
            .to_string();
        refuse_key_in_url(&url)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| socket_error("CarbiumSocket needs a tokio runtime"))?;
        let carrier = match key {
            None => None,
            Some(k) => {
                let (name, value) = match options.carrier {
                    Carrier::Subprotocol => {
                        ("sec-websocket-protocol", format!("carbium, api-key.{k}"))
                    }
                    Carrier::Header => ("x-api-key", k),
                };
                let mut v = HeaderValue::from_str(&value)
                    .map_err(|_| Error::new(ErrorKind::Auth, "api_key is not a header value"))?;
                v.set_sensitive(true);
                Some((name, v))
            }
        };
        let (cmd, cmd_rx) = mpsc::unbounded_channel();
        let (events, events_rx) = mpsc::unbounded_channel();
        let (state_tx, state) = watch::channel(SocketState::Idle);
        let size = Arc::new(AtomicUsize::new(0));
        let task = Task::new(
            url.clone(),
            carrier,
            options.random,
            cmd_rx,
            events,
            state_tx,
            size.clone(),
        );
        runtime.spawn(task.run());
        Ok((
            Self {
                url,
                cmd,
                state,
                size,
                next_local: AtomicU64::new(1),
            },
            events_rx,
        ))
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn state(&self) -> SocketState {
        *self.state.borrow()
    }

    /// How many subscriptions this socket holds.
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    /// Any served subscription by name; the typed methods below call this.
    pub fn subscribe<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Subscription<T>> {
        if !SUBSCRIPTIONS.contains(&method) {
            return Err(Error::new(
                ErrorKind::Request,
                format!("{method} is not a served subscription"),
            )
            .with_method(method));
        }
        let local = self.next_local.fetch_add(1, Ordering::SeqCst);
        let (notify, notes) = mpsc::unbounded_channel();
        let (ack, ack_rx) = oneshot::channel();
        self.cmd
            .send(Command::Subscribe {
                local,
                method: method.into(),
                params,
                notify,
                ack,
            })
            .map_err(|_| socket_error("the socket was closed").with_method(method))?;
        Ok(Subscription {
            local,
            method: method.into(),
            cmd: self.cmd.clone(),
            ack: Some(ack_rx),
            id: None,
            notes,
            done: false,
            _t: PhantomData,
        })
    }

    fn with_config(first: Value, config: Option<Value>) -> Vec<Value> {
        match config {
            Some(c) => vec![first, c],
            None => vec![first],
        }
    }

    pub fn account_subscribe(
        &self,
        pubkey: &str,
        config: Option<Value>,
    ) -> Result<Subscription<WithContextAccountInfo>> {
        self.subscribe("accountSubscribe", Self::with_config(json!(pubkey), config))
    }
    pub fn program_subscribe(
        &self,
        program_id: &str,
        config: Option<Value>,
    ) -> Result<Subscription<WithContextKeyedAccount>> {
        self.subscribe(
            "programSubscribe",
            Self::with_config(json!(program_id), config),
        )
    }
    /// `filter`: "all", "allWithVotes" or `{"mentions": [address]}`.
    pub fn logs_subscribe(
        &self,
        filter: Value,
        config: Option<Value>,
    ) -> Result<Subscription<WithContextLogsNotification>> {
        self.subscribe("logsSubscribe", Self::with_config(filter, config))
    }
    /// Fires once, then the server ends the subscription. After a reconnect, check getSignatureStatuses.
    pub fn signature_subscribe(
        &self,
        signature: &str,
        config: Option<Value>,
    ) -> Result<Subscription<WithContextSignatureNotification>> {
        self.subscribe(
            "signatureSubscribe",
            Self::with_config(json!(signature), config),
        )
    }
    pub fn slot_subscribe(&self) -> Result<Subscription<SlotInfo>> {
        self.subscribe("slotSubscribe", vec![])
    }
    pub fn root_subscribe(&self) -> Result<Subscription<Number>> {
        self.subscribe("rootSubscribe", vec![])
    }
    pub fn slots_updates_subscribe(&self) -> Result<Subscription<Value>> {
        self.subscribe("slotsUpdatesSubscribe", vec![])
    }
    /// `filter`: "all" or `{"mentionsAccountOrProgram": address}`. maxSupportedTransactionVersion defaults to 1:
    /// without it a block holding a v1 transaction arrives as block: null.
    pub fn block_subscribe(
        &self,
        filter: Value,
        config: Option<Value>,
    ) -> Result<Subscription<WithContextBlockNotification>> {
        let mut c = json!({ "maxSupportedTransactionVersion": 1 });
        if let (Some(Value::Object(given)), Value::Object(base)) = (config, &mut c) {
            base.extend(given);
        }
        self.subscribe("blockSubscribe", vec![filter, c])
    }

    /// Closes the socket; no reconnect follows. Subscriptions not yet acknowledged are refused.
    pub fn close(&self) {
        let _ = self.cmd.send(Command::Close);
    }
}

struct Handle {
    method: String,
    params: Vec<Value>,
    notify: mpsc::UnboundedSender<Value>,
    ack: Option<oneshot::Sender<Result<u64>>>,
    server_id: Option<u64>,
}

enum Pending {
    Bind {
        local: u64,
        attempt: u32,
        method: String,
    },
    Unsubscribe,
}

enum Timer {
    Reconnect,
    Bind { local: u64, attempt: u32 },
}

enum Wake {
    Command(Option<Command>),
    Connected(Box<std::result::Result<Ws, tungstenite::Error>>),
    Frame(Option<std::result::Result<Message, tungstenite::Error>>),
    Timer,
    Watchdog,
}

struct Task {
    url: String,
    carrier: Option<(&'static str, HeaderValue)>,
    random: fn() -> f64,
    cmd: mpsc::UnboundedReceiver<Command>,
    events: mpsc::UnboundedSender<SocketEvent>,
    state_tx: watch::Sender<SocketState>,
    size: Arc<AtomicUsize>,
    state: SocketState,
    ws: Option<Ws>,
    connecting: Option<Connecting>,
    handles: BTreeMap<u64, Handle>,
    by_server: HashMap<u64, u64>,
    pending: HashMap<u64, Pending>,
    timers: Vec<(Instant, Timer)>,
    bind_queue: VecDeque<(u64, u32)>,
    binds_in_flight: usize,
    next_id: u64,
    attempt: u32,
    pre_open_failures: u32,
    overload_closes: u32,
    credits_exhausted_once: bool,
    ever_opened: bool,
    opened_this_connection: bool,
    handshake_status: Option<u16>,
    handshake_retry_after_ms: Option<u64>,
    fatal: Option<Error>,
    closed_by_user: bool,
    last_close: CloseInfo,
    reseed_due: bool,
    watchdog: Instant,
}

impl Task {
    fn new(
        url: String,
        carrier: Option<(&'static str, HeaderValue)>,
        random: fn() -> f64,
        cmd: mpsc::UnboundedReceiver<Command>,
        events: mpsc::UnboundedSender<SocketEvent>,
        state_tx: watch::Sender<SocketState>,
        size: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            url,
            carrier,
            random,
            cmd,
            events,
            state_tx,
            size,
            state: SocketState::Idle,
            ws: None,
            connecting: None,
            handles: BTreeMap::new(),
            by_server: HashMap::new(),
            pending: HashMap::new(),
            timers: Vec::new(),
            bind_queue: VecDeque::new(),
            binds_in_flight: 0,
            next_id: 1,
            attempt: 0,
            pre_open_failures: 0,
            overload_closes: 0,
            credits_exhausted_once: false,
            ever_opened: false,
            opened_this_connection: false,
            handshake_status: None,
            handshake_retry_after_ms: None,
            fatal: None,
            closed_by_user: false,
            last_close: CloseInfo::default(),
            reseed_due: false,
            watchdog: Instant::now() + PING_WATCHDOG,
        }
    }

    async fn run(mut self) {
        loop {
            let next_timer = self.timers.iter().map(|(at, _)| *at).min();
            let (has_connecting, has_ws, watchdog) =
                (self.connecting.is_some(), self.ws.is_some(), self.watchdog);
            let (cmd, connecting, ws) = (&mut self.cmd, &mut self.connecting, &mut self.ws);
            let wake = tokio::select! {
                c = cmd.recv() => Wake::Command(c),
                r = async { connecting.as_mut().expect("guarded").await }, if has_connecting => Wake::Connected(Box::new(r)),
                m = async { ws.as_mut().expect("guarded").next().await }, if has_ws => Wake::Frame(m),
                _ = sleep_until(next_timer.unwrap_or_else(Instant::now)), if next_timer.is_some() => Wake::Timer,
                _ = sleep_until(watchdog), if has_ws => Wake::Watchdog,
            };
            match wake {
                Wake::Command(Some(Command::Close)) | Wake::Command(None) => {
                    return self.close().await;
                }
                Wake::Command(Some(c)) => self.command(c).await,
                Wake::Connected(r) => {
                    self.connecting = None;
                    self.connected(*r).await;
                }
                Wake::Frame(m) => self.frame(m).await,
                Wake::Timer => self.fire_timers().await,
                Wake::Watchdog => self.on_close(CloseInfo {
                    code: 1006,
                    reason: "no frame for 75 s".into(),
                }),
            }
            self.size.store(self.handles.len(), Ordering::SeqCst);
        }
    }

    fn set_state(&mut self, state: SocketState, info: Option<CloseInfo>) {
        self.state = state;
        let _ = self.state_tx.send(state);
        let _ = self.events.send(SocketEvent::State(state, info));
    }

    async fn command(&mut self, c: Command) {
        match c {
            Command::Subscribe {
                local,
                method,
                params,
                notify,
                ack,
            } => {
                if let Some(f) = &self.fatal {
                    let _ = ack.send(Err(f.clone()));
                    return;
                }
                if self.closed_by_user {
                    let _ = ack.send(Err(
                        socket_error("the socket was closed").with_method(&method)
                    ));
                    return;
                }
                self.handles.insert(
                    local,
                    Handle {
                        method,
                        params,
                        notify,
                        ack: Some(ack),
                        server_id: None,
                    },
                );
                match self.state {
                    SocketState::Open => {
                        self.bind_queue.push_back((local, 1));
                        self.pump_binds().await;
                    }
                    SocketState::Idle | SocketState::Closed => self.connect(),
                    _ => {} // bound once the connection opens
                }
            }
            Command::Unsubscribe { local } => {
                let Some(mut h) = self.handles.remove(&local) else {
                    return;
                };
                if let Some(id) = h.server_id {
                    self.by_server.remove(&id);
                    if self.state == SocketState::Open {
                        self.request(
                            &h.method.replace("Subscribe", "Unsubscribe"),
                            vec![json!(id)],
                            Pending::Unsubscribe,
                        )
                        .await; // false or -32602 both mean gone
                    }
                }
                if let Some(ack) = h.ack.take() {
                    let _ = ack.send(Err(socket_error("unsubscribed before the acknowledgement")
                        .with_method(&h.method)));
                }
                if self.handles.is_empty() && self.state == SocketState::Open {
                    self.send_close(1000, "no subscriptions").await;
                }
            }
            Command::Close => {}
        }
    }

    fn connect(&mut self) {
        self.set_state(SocketState::Connecting, None);
        self.opened_this_connection = false;
        self.handshake_retry_after_ms = None;
        self.handshake_status = None;
        let mut request = match self.url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => return self.die(socket_error(&format!("could not open the socket: {e}"))),
        };
        if let Some((name, value)) = &self.carrier {
            request.headers_mut().insert(*name, value.clone());
        }
        self.connecting = Some(Box::pin(async move {
            tokio_tungstenite::connect_async(request)
                .await
                .map(|(ws, _)| ws)
        }));
    }

    async fn connected(&mut self, result: std::result::Result<Ws, tungstenite::Error>) {
        match result {
            Ok(ws) => {
                self.ws = Some(ws);
                self.opened_this_connection = true;
                self.attempt = 0;
                self.pre_open_failures = 0;
                self.reseed_due = self.ever_opened;
                self.ever_opened = true;
                self.watchdog = Instant::now() + PING_WATCHDOG;
                self.set_state(SocketState::Open, None);
                self.bind_queue = self.handles.keys().map(|l| (*l, 1)).collect();
                self.pump_binds().await;
                self.check_reseed();
            }
            Err(tungstenite::Error::Http(res)) => {
                let status = res.status().as_u16();
                self.handshake_status = Some(status);
                if status == 401 || status == 403 {
                    let message = if status == 401 {
                        "websocket handshake refused: unknown, suspended or revoked key, or no websocket scope"
                    } else {
                        "websocket handshake refused: address not allowed or no websocket scope"
                    };
                    return self.die(Error::new(ErrorKind::Auth, message).with_status(status));
                }
                let ra = res
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok());
                if status == 429 {
                    self.handshake_retry_after_ms =
                        ra.and_then(|v| v.parse::<u64>().ok()).map(|s| s * 1000);
                }
                self.on_close(CloseInfo {
                    code: 1006,
                    reason: "handshake failed".into(),
                });
            }
            Err(_) => self.on_close(CloseInfo {
                code: 1006,
                reason: "handshake failed".into(),
            }),
        }
    }

    async fn request(&mut self, method: &str, params: Vec<Value>, pending: Pending) -> bool {
        let Some(ws) = self.ws.as_mut() else {
            return false;
        };
        let id = self.next_id;
        self.next_id += 1;
        let body = if params.is_empty() {
            json!({ "jsonrpc": "2.0", "id": id, "method": method })
        } else {
            json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
        };
        if ws.send(Message::text(body.to_string())).await.is_err() {
            return false; // the read side sees the close and decides
        }
        self.pending.insert(id, pending);
        true
    }

    async fn send_close(&mut self, code: u16, reason: &str) {
        if let Some(ws) = self.ws.as_mut() {
            let _ = ws
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::from(code),
                    reason: reason.into(),
                })))
                .await;
        }
    }

    /// Binds queued subscriptions, at most 32 at a time.
    async fn pump_binds(&mut self) {
        while self.binds_in_flight < BIND_BATCH && self.state == SocketState::Open {
            let Some((local, attempt)) = self.bind_queue.pop_front() else {
                return;
            };
            let Some(h) = self.handles.get(&local).filter(|h| h.server_id.is_none()) else {
                continue;
            };
            let (method, params) = (h.method.clone(), h.params.clone());
            if self
                .request(
                    &method,
                    params,
                    Pending::Bind {
                        local,
                        attempt,
                        method: method.clone(),
                    },
                )
                .await
            {
                self.binds_in_flight += 1;
            }
        }
    }

    fn check_reseed(&mut self) {
        let binding = self.binds_in_flight > 0
            || !self.bind_queue.is_empty()
            || self
                .timers
                .iter()
                .any(|(_, t)| matches!(t, Timer::Bind { .. }));
        if self.reseed_due && !binding && self.state == SocketState::Open {
            self.reseed_due = false;
            let close = self.last_close.clone();
            let gap = close.code == 1012 && close.reason == "stream_gap_reseed_required";
            let _ = self.events.send(SocketEvent::Reseed { close, gap });
        }
    }

    async fn frame(&mut self, frame: Option<std::result::Result<Message, tungstenite::Error>>) {
        match frame {
            Some(Ok(Message::Text(text))) => {
                self.watchdog = Instant::now() + PING_WATCHDOG;
                if let Ok(msg) = serde_json::from_str::<Value>(text.as_str()) {
                    self.message(msg).await;
                }
            }
            Some(Ok(Message::Close(frame))) => {
                let info = frame.map_or(
                    CloseInfo {
                        code: 1005,
                        reason: String::new(),
                    },
                    |f| CloseInfo {
                        code: u16::from(f.code),
                        reason: f.reason.as_str().to_string(),
                    },
                );
                self.on_close(info);
            }
            Some(Ok(_)) => self.watchdog = Instant::now() + PING_WATCHDOG, // ping, pong, binary
            Some(Err(_)) | None => self.on_close(CloseInfo {
                code: 1006,
                reason: String::new(),
            }),
        }
    }

    async fn message(&mut self, msg: Value) {
        if let Some(pending) = msg
            .get("id")
            .and_then(Value::as_u64)
            .and_then(|id| self.pending.remove(&id))
        {
            let Pending::Bind {
                local,
                attempt,
                method,
            } = pending
            else {
                return;
            };
            self.binds_in_flight -= 1;
            self.bound(local, attempt, &method, &msg).await;
            self.pump_binds().await;
            self.check_reseed();
            return;
        }
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let sub = msg
            .get("params")
            .and_then(|p| p.get("subscription"))
            .and_then(Value::as_u64);
        if let (true, Some(sub)) = (method.ends_with("Notification"), sub) {
            let Some(local) = self.by_server.get(&sub).copied() else {
                return;
            };
            self.overload_closes = 0; // a connection that delivers is not overloaded
            let result = msg["params"].get("result").cloned().unwrap_or(Value::Null);
            if let Some(h) = self.handles.get(&local) {
                let _ = h.notify.send(result);
            }
            if method == "signatureNotification" {
                self.by_server.remove(&sub);
                self.handles.remove(&local);
            }
        }
    }

    async fn bound(&mut self, local: u64, attempt: u32, method: &str, msg: &Value) {
        if let Some(error) = msg.get("error").filter(|e| !e.is_null()) {
            let err = rpc_error(method, 200, error, &HeaderMap::new(), None).0;
            if !self.handles.contains_key(&local) {
                return;
            }
            let retryable = err.code() == Some(&Code::Number(-32005))
                && err.reason().is_none_or(|r| RETRY_REASONS.contains(&r));
            if retryable && attempt < 4 {
                let backoff = (250u64 << (attempt - 1)).min(4000);
                let wait = err.retry_after_ms().unwrap_or(1000).max(backoff)
                    + ((self.random)() * 251.0).floor() as u64;
                self.timers.push((
                    Instant::now() + Duration::from_millis(wait),
                    Timer::Bind {
                        local,
                        attempt: attempt + 1,
                    },
                ));
            } else if err.code() == Some(&Code::Number(-32002)) {
                self.die(err);
            } else if let Some(mut h) = self.handles.remove(&local) {
                match h.ack.take() {
                    Some(ack) => drop(ack.send(Err(err))),
                    None => drop(self.events.send(SocketEvent::Error(err))),
                }
            }
            return;
        }
        let Some(id) = msg.get("result").and_then(Value::as_u64) else {
            return;
        };
        let Some(h) = self.handles.get_mut(&local) else {
            // unsubscribed before the acknowledgement: free the server's slot
            self.request(
                &method.replace("Subscribe", "Unsubscribe"),
                vec![json!(id)],
                Pending::Unsubscribe,
            )
            .await;
            return;
        };
        h.server_id = Some(id);
        if let Some(ack) = h.ack.take() {
            let _ = ack.send(Ok(id));
        }
        self.by_server.insert(id, local);
    }

    async fn fire_timers(&mut self) {
        let now = Instant::now();
        let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut self.timers)
            .into_iter()
            .partition(|(at, _)| *at <= now);
        self.timers = later;
        for (_, timer) in due {
            match timer {
                Timer::Reconnect => self.connect(),
                Timer::Bind { local, attempt } => self.bind_queue.push_front((local, attempt)),
            }
        }
        self.pump_binds().await;
        self.check_reseed();
    }

    fn die(&mut self, error: Error) {
        self.fatal = Some(error.clone());
        for (_, mut h) in std::mem::take(&mut self.handles) {
            if let Some(ack) = h.ack.take() {
                let _ = ack.send(Err(error.clone()));
            }
        }
        self.by_server.clear();
        self.timers.clear();
        self.bind_queue.clear();
        self.pending.clear();
        self.binds_in_flight = 0;
        self.ws = None; // dropping the stream closes the connection
        self.connecting = None;
        self.set_state(SocketState::Closed, None);
        let _ = self.events.send(SocketEvent::Fatal(error));
    }

    async fn close(&mut self) {
        self.closed_by_user = true;
        for (_, mut h) in std::mem::take(&mut self.handles) {
            if let Some(ack) = h.ack.take() {
                let _ = ack.send(Err(
                    socket_error("the socket was closed").with_method(&h.method)
                ));
            }
        }
        self.send_close(1000, "client close").await;
        self.ws = None;
        self.connecting = None;
        self.size.store(0, Ordering::SeqCst);
        self.set_state(SocketState::Closed, None);
    }

    fn on_close(&mut self, info: CloseInfo) {
        self.last_close = info.clone();
        self.ws = None;
        self.connecting = None;
        self.pending.clear();
        self.binds_in_flight = 0;
        self.bind_queue.clear();
        self.timers
            .retain(|(_, t)| !matches!(t, Timer::Bind { .. }));
        self.by_server.clear();
        self.reseed_due = false;
        for h in self.handles.values_mut() {
            h.server_id = None;
        }
        if self.closed_by_user || self.fatal.is_some() {
            return;
        }
        if self.handles.is_empty() {
            return self.set_state(SocketState::Idle, Some(info));
        }
        let r = (self.random)();
        let close_error = |kind, message: String| {
            Error::new(kind, message)
                .with_code(Code::Number(i64::from(info.code)))
                .with_reason(&info.reason)
        };
        let delay_ms = match (info.code, info.reason.as_str()) {
            (1008, reason) if reason.starts_with("key_") => {
                return self.die(close_error(
                    ErrorKind::Auth,
                    format!("the server closed the socket: {reason}; a human must fix the key"),
                ));
            }
            (1008, "credits_exhausted") if self.credits_exhausted_once => {
                return self.die(close_error(
                    ErrorKind::Credits,
                    "credits exhausted: buy a plan or upgrade".into(),
                ));
            }
            (1008, "credits_exhausted") => {
                self.credits_exhausted_once = true;
                5000.0
            }
            (1008, reason) => {
                return self.die(close_error(
                    ErrorKind::Socket,
                    format!("the server closed the socket: {reason}"),
                ));
            }
            (1013, "credits_pending") => 1000.0 + r * 500.0,
            (1013, _) => {
                // slow_consumer or delivery_overload_retry: at least 1 s, and further back after each close in a row
                let cap = (1000.0 * 2f64.powi(self.overload_closes as i32)).min(30_000.0);
                self.overload_closes += 1;
                (cap / 2.0 + r * cap / 2.0).max(1000.0)
            }
            (1012 | 1001, _) => 250.0 + r * 750.0,
            _ => {
                // a close before open with no status seen: after five in a row the key is the likely cause
                if !self.opened_this_connection && self.handshake_status.is_none() {
                    self.pre_open_failures += 1;
                    if self.pre_open_failures >= 5 {
                        return self.die(close_error(ErrorKind::Auth, "the handshake failed five times in a row with no status: check the key, its websocket scope and the IP allowlist".into()));
                    }
                }
                let cap = (500.0 * 2f64.powi(self.attempt as i32)).min(30_000.0);
                self.attempt += 1;
                let delay = cap / 2.0 + r * cap / 2.0;
                self.handshake_retry_after_ms
                    .map_or(delay, |h| delay.max(h as f64))
            }
        };
        self.set_state(SocketState::Backoff, Some(info));
        self.timers.push((
            Instant::now() + Duration::from_millis(delay_ms as u64),
            Timer::Reconnect,
        ));
    }
}

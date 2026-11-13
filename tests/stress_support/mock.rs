//! Loopback mock of the Kalshi REST + WebSocket protocols.
//!
//! The mock speaks just enough of the real wire to drive the production
//! adapter end-to-end:
//!
//! - REST `/markets` with cursor pagination and the exact field names the
//!   adapter decodes (full paginated discovery, never shortcuts);
//! - WebSocket `orderbook_delta` subscribe (`subscribed` ack with per-sid
//!   `seq`, inline snapshots on subscribe);
//! - `update_subscription` (`ok` ack; full-tickers folding; `get_snapshot`);
//! - `market_lifecycle_v2` subscribe and notifications;
//! - per-connection sid/seq, connection drop for fault scenarios.
//!
//! The mock tracks every transmitted frame in a broadcast ledger so the
//! harness can reconcile generated vs delivered vs applied events.
//!
//! This module is the largest piece of the laboratory. It is deliberately
//! built first and exercised by its own tests before the stress suite uses it.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use engineering_challenge::Side;

use super::rng::SplitMix64;
use super::universe::SyntheticUniverse;

/// Kind of frame transmitted by the mock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FrameKind {
    OrderbookSnapshot,
    OrderbookDelta,
    Lifecycle,
    SubscribeAck,
    OkAck,
}

/// One transmitted frame, recorded for the event ledger.
#[allow(dead_code)] // fields are read by the reconciliation checks landing with the workload phases
#[derive(Debug, Clone)]
pub struct TransmittedFrame {
    pub connection: usize,
    pub market: Option<String>,
    pub kind: FrameKind,
    /// Monotonic per-connection sequence emission was skipped: tracked at the
    /// connection level by the mock itself.
    pub seq: u64,
}

struct MockBook {
    /// price level (micros) -> quantity (micros)
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl MockBook {
    fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }
}

struct Connection {
    outbound: tokio::sync::mpsc::UnboundedSender<Message>,
    /// Snapshot bid/ask price and quantity levels as strings.
    subscriptions: HashSet<String>,
    sid_orderbook: Option<i64>,
    seq: u64,
}

/// Shared mock state, read by both the REST and WS servers and the driver.
pub struct MockState {
    universe: SyntheticUniverse,
    books: Mutex<HashMap<String, MockBook>>,
    http_requests: AtomicUsize,
    ws_connections: AtomicUsize,
    connection_ids: AtomicUsize,
    book_subscribed_count: AtomicUsize,
    lifecycle_subscribed_count: AtomicUsize,
    connections: Mutex<HashMap<usize, Connection>>,
    transmitted: Mutex<Vec<TransmittedFrame>>,
    transmitted_count: AtomicU64,
    transmitted_bytes: AtomicU64,
    lifecycle_tx: broadcast::Sender<LifecycleMsg>,
    close: Mutex<HashMap<usize, oneshot::Sender<()>>>,
    start_send_ts_ms: i64,
}

#[derive(Clone)]
struct LifecycleMsg {
    ts_ms: i64,
    msg: Value,
}

impl MockState {
    fn new(universe: SyntheticUniverse) -> Self {
        let books = universe
            .markets
            .iter()
            .map(|market| (market.ticker.clone(), MockBook::new()))
            .collect();
        let (lifecycle_tx, _) = broadcast::channel(1024);
        Self {
            universe,
            books: Mutex::new(books),
            http_requests: AtomicUsize::new(0),
            ws_connections: AtomicUsize::new(0),
            connection_ids: AtomicUsize::new(1),
            book_subscribed_count: AtomicUsize::new(0),
            lifecycle_subscribed_count: AtomicUsize::new(0),
            connections: Mutex::new(HashMap::new()),
            transmitted: Mutex::new(Vec::new()),
            transmitted_count: AtomicU64::new(0),
            transmitted_bytes: AtomicU64::new(0),
            lifecycle_tx,
            close: Mutex::new(HashMap::new()),
            start_send_ts_ms: 1_800_000_000_000,
        }
    }

    #[must_use]
    pub fn universe(&self) -> &SyntheticUniverse {
        &self.universe
    }

    /// REST discovery record count served so far (for pagination accounting).
    #[allow(dead_code)]
    pub fn http_requests(&self) -> usize {
        self.http_requests.load(Ordering::SeqCst)
    }

    /// Total websocket connections accepted since start (including reconnects).
    #[allow(dead_code)]
    pub fn ws_connections(&self) -> usize {
        self.ws_connections.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn book_subscribed_count(&self) -> usize {
        self.book_subscribed_count.load(Ordering::SeqCst)
    }

    #[allow(dead_code)]
    pub fn lifecycle_subscribed_count(&self) -> usize {
        self.lifecycle_subscribed_count.load(Ordering::SeqCst)
    }

    pub fn transmitted_count(&self) -> u64 {
        self.transmitted_count.load(Ordering::SeqCst)
    }

    #[allow(dead_code)] // used by recovery/backpressure phases of the matrix
    pub fn transmitted_bytes(&self) -> u64 {
        self.transmitted_bytes.load(Ordering::SeqCst)
    }

    /// Transmitted frames matching `kind`, in emission order.
    #[allow(dead_code)]
    pub fn transmitted(&self, kind: FrameKind) -> Vec<TransmittedFrame> {
        self.transmitted
            .lock()
            .map(|all| {
                all.iter()
                    .filter(|frame| frame.kind == kind)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Frames transmitted for one market, across connections.
    #[allow(dead_code)]
    pub fn transmitted_for(&self, market: &str, kind: FrameKind) -> usize {
        self.transmitted
            .lock()
            .map(|all| {
                all.iter()
                    .filter(|frame| frame.kind == kind && frame.market.as_deref() == Some(market))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Seed the synthetic book for one market with deterministic levels.
    ///
    /// Seed the synthetic book with one bid level and one no-side level
    /// whose complement stays strictly above the bid, keeping every book
    /// uncrossed by construction.
    pub fn seed_book(&self, market: &str, rng: &mut SplitMix64) {
        let bid = 250_000_i64 + i64::try_from(rng.below(50_000) / 10_000).unwrap_or(0) * 10_000;
        let bid_qty = 10_000_000 + i64::try_from(rng.below(10_000_000)).unwrap_or(0);
        let ask_qty = 5_000_000 + i64::try_from(rng.below(5_000_000)).unwrap_or(0);
        let mut books = self.books.lock().unwrap();
        if let Some(book) = books.get_mut(market) {
            book.bids.insert(bid, bid_qty);
            book.asks.insert(700_000, ask_qty);
        }
    }

    fn record_frame(
        &self,
        connection: usize,
        market: Option<String>,
        kind: FrameKind,
        seq: u64,
        bytes: usize,
    ) {
        self.transmitted_count.fetch_add(1, Ordering::SeqCst);
        self.transmitted_bytes
            .fetch_add(bytes as u64, Ordering::SeqCst);
        if let Ok(mut log) = self.transmitted.lock() {
            log.push(TransmittedFrame {
                connection,
                market,
                kind,
                seq,
            });
        }
    }

    fn send_json(&self, connection: usize, value: Value) {
        let text = value.to_string();
        let bytes = text.len();
        let kind = match value["type"].as_str() {
            Some("orderbook_snapshot") => FrameKind::OrderbookSnapshot,
            Some("orderbook_delta") => FrameKind::OrderbookDelta,
            Some("market_lifecycle_v2") => FrameKind::Lifecycle,
            Some("subscribed") => FrameKind::SubscribeAck,
            Some("ok") => FrameKind::OkAck,
            _ => FrameKind::OkAck,
        };
        let market = value["msg"]["market_ticker"].as_str().map(str::to_owned);
        let seq = value["seq"].as_u64().unwrap_or(0);
        if let Ok(mut connections) = self.connections.lock()
            && let Some(entry) = connections.get_mut(&connection)
            && entry.outbound.send(Message::Text(text.into())).is_ok()
        {
            self.record_frame(connection, market, kind, seq, bytes);
        }
    }

    fn snapshot_body(&self, market: &str, sid: i64, seq: u64) -> Value {
        let (yes, no) = {
            let books = self.books.lock().unwrap();
            match books.get(market) {
                Some(book) => (
                    book.bids
                        .iter()
                        .map(|(price, qty)| json!([format_micros(*price), format_micros(*qty)]))
                        .collect::<Vec<_>>(),
                    book.asks
                        .iter()
                        .map(|(price, qty)| json!([format_micros(*price), format_micros(*qty)]))
                        .collect::<Vec<_>>(),
                ),
                None => (Vec::new(), Vec::new()),
            }
        };
        json!({
            "type": "orderbook_snapshot",
            "sid": sid,
            "seq": seq,
            "sending_ts_ms": self.next_ts(),
            "msg": {
                "market_ticker": market,
                "clock": seq,
                "yes_dollars_fp": yes,
                "no_dollars_fp": no,
            }
        })
    }

    fn next_ts(&self) -> i64 {
        // Deterministic but monotonic: offset from a fixed base by frames sent.
        self.start_send_ts_ms + self.transmitted_count.load(Ordering::SeqCst) as i64
    }

    /// Publish one seeded price-level change for `market` to every connection
    /// subscribed to it. Returns the number of frames transmitted.
    pub fn publish_delta(
        &self,
        market: &str,
        side: Side,
        price_micros: i64,
        delta_micros: i64,
    ) -> usize {
        // Apply to the mock book first so snapshots stay coherent.
        {
            let mut books = self.books.lock().unwrap();
            if let Some(book) = books.get_mut(market) {
                let levels = if side == Side::Buy {
                    &mut book.bids
                } else {
                    &mut book.asks
                };
                let entry = levels.entry(price_micros).or_insert(0);
                *entry = entry.saturating_add(delta_micros).max(1_000);
            }
        }
        let mut sent = 0usize;
        let connections: Vec<usize> = match self.connections.lock() {
            Ok(connections) => connections
                .iter()
                .filter(|(_, entry)| entry.subscriptions.contains(market))
                .map(|(id, _)| *id)
                .collect(),
            Err(_) => Vec::new(),
        };
        for connection in connections {
            let sid = match self.connections.lock() {
                Ok(connections) => connections
                    .get(&connection)
                    .and_then(|entry| entry.sid_orderbook)
                    .unwrap_or(0),
                Err(_) => 0,
            };
            let seq = match self.connections.lock() {
                Ok(mut connections) => {
                    if let Some(entry) = connections.get_mut(&connection) {
                        entry.seq += 1;
                        entry.seq
                    } else {
                        drop(connections);
                        continue;
                    }
                }
                Err(_) => continue,
            };
            self.send_json(
                connection,
                json!({
                    "type": "orderbook_delta",
                    "sid": sid,
                    "seq": seq,
                    "sending_ts_ms": self.next_ts(),
                    "msg": {
                        "market_ticker": market,
                        "clock": seq,
                        "price_dollars": format_micros(price_micros),
                        "delta_fp": format_micros(delta_micros),
                        "side": side_label(side),
                    }
                }),
            );
            sent += 1;
        }
        sent
    }

    /// Publish one lifecycle notification to every lifecycle connection with a
    /// shared timestamp (both replicas see the same venue event).
    #[allow(dead_code)] // used by the lifecycle-churn scenario cells
    pub fn publish_lifecycle(&self, event_type: &str, market: &str, extra: Value) {
        let ts = 1_700_000_000_000 + self.transmitted_count.load(Ordering::SeqCst) as i64;
        let mut msg = json!({
            "market_ticker": market,
            "event_type": event_type,
        });
        if let Value::Object(extra) = extra {
            for (key, value) in extra {
                msg[key] = value;
            }
        }
        let _ = self.lifecycle_tx.send(LifecycleMsg { ts_ms: ts, msg });
    }

    /// Close all book connections (or only a subset) to inject a connection
    /// loss. Book sockets only; lifecycle sockets stay up.
    #[allow(dead_code)] // used by the recovery-sweep cells
    pub fn close_book_connections(&self) {
        self.close_connections_by(|entry| entry.sid_orderbook.is_some());
    }

    #[allow(dead_code)] // used by the recovery-sweep cells
    pub fn close_all_connections(&self) {
        self.close_connections_by(|_| true);
    }

    fn close_connections_by(&self, predicate: fn(&Connection) -> bool) {
        let ids: Vec<usize> = self
            .connections
            .lock()
            .map(|connections| {
                connections
                    .iter()
                    .filter(|(_, entry)| predicate(entry))
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default();
        let mut close = self.close.lock().unwrap();
        for id in ids {
            if let Some(sender) = close.remove(&id) {
                let _ = sender.send(());
            }
        }
    }

    fn next_connection_id(&self) -> usize {
        self.connection_ids.fetch_add(1, Ordering::SeqCst)
    }
}

fn side_label(side: Side) -> &'static str {
    if side == Side::Buy { "yes" } else { "no" }
}

fn format_micros(value: i64) -> String {
    engineering_challenge::fixed::format_micros(value)
}

/// The running mock: REST + WS servers on ephemeral ports.
pub struct MockKalshi {
    state: Arc<MockState>,
    http_task: JoinHandle<()>,
    ws_task: JoinHandle<()>,
    http_addr: SocketAddr,
    ws_addr: SocketAddr,
}

impl MockKalshi {
    pub async fn start(universe: SyntheticUniverse) -> Self {
        let state = Arc::new(MockState::new(universe));
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let http_state = Arc::clone(&state);
        let http_task = tokio::spawn(run_http(http_listener, http_state));
        let ws_state = Arc::clone(&state);
        let ws_task = tokio::spawn(run_ws(ws_listener, ws_state));
        Self {
            state,
            http_task,
            ws_task,
            http_addr,
            ws_addr,
        }
    }

    #[must_use]
    pub fn http_url(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.ws_addr)
    }

    #[must_use]
    pub fn state(&self) -> &Arc<MockState> {
        &self.state
    }
}

impl Drop for MockKalshi {
    fn drop(&mut self) {
        self.http_task.abort();
        self.ws_task.abort();
    }
}

async fn run_http(listener: TcpListener, state: Arc<MockState>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut request = Vec::new();
            let mut buffer = [0u8; 8192];
            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) => break,
                    Ok(read) => {
                        request.extend_from_slice(&buffer[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(_) => return,
                }
                if request.len() > 64 * 1024 {
                    return;
                }
            }
            let text = String::from_utf8_lossy(&request);
            let target = text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_owned();
            state.http_requests.fetch_add(1, Ordering::SeqCst);
            let (status, body) = route(&state, &target);
            let reason = if status == 200 { "OK" } else { "Not Found" };
            let head = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

fn route(state: &MockState, target: &str) -> (u16, String) {
    let query = target.split_once('?').map(|(_, query)| query).unwrap_or("");
    let mut limit = 100usize;
    let mut cursor: Option<usize> = None;
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        match (parts.next(), parts.next()) {
            (Some("limit"), Some(value)) => limit = value.parse().unwrap_or(100).min(1000),
            (Some("cursor"), Some(value)) => cursor = value.parse().ok(),
            _ => {}
        }
    }
    if !target.starts_with("/markets") {
        return (404, "{}".to_owned());
    }
    let page = cursor.unwrap_or(0);
    let page = match cursor {
        Some(offset) => offset / limit.max(1),
        None => page,
    };
    let (records, next_cursor) = state.universe.rest_page(page, limit.max(1));
    let body = json!({
        "markets": records,
        "cursor": next_cursor,
    });
    (200, body.to_string())
}

async fn run_ws(listener: TcpListener, state: Arc<MockState>) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            continue;
        };
        let id = state.next_connection_id();
        let state = Arc::clone(&state);
        tokio::spawn(async move { handle_ws_connection(id, socket, state).await });
    }
}

async fn handle_ws_connection(id: usize, socket: TcpStream, state: Arc<MockState>) {
    state.ws_connections.fetch_add(1, Ordering::SeqCst);
    let Ok(websocket) = accept_async(socket).await else {
        return;
    };
    let (mut sink, mut stream) = websocket.split();
    let (outbound, mut inbound) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let (close_tx, mut close_rx) = oneshot::channel();
    state.connections.lock().unwrap().insert(
        id,
        Connection {
            outbound,
            subscriptions: HashSet::new(),
            sid_orderbook: None,
            seq: 0,
        },
    );
    state.close.lock().unwrap().insert(id, close_tx);

    // Forwarder: messages from the outbound channel to the websocket.
    let forward_state = Arc::clone(&state);
    let _forward = tokio::spawn(async move {
        while let Some(message) = inbound.recv().await {
            if sink.send(message).await.is_err() {
                break;
            }
        }
        drop(forward_state);
    });

    let mut lifecycle_rx = state.lifecycle_tx.subscribe();

    loop {
        tokio::select! {
            message = stream.next() => {
                let Some(message) = message else { break };
                let message = match message {
                    Ok(message) => message,
                    Err(_) => break,
                };
                let Message::Text(text) = message else {
                    continue;
                };
                let command: Value = serde_json::from_str(text.as_str()).unwrap_or(Value::Null);
                let id_num = command["id"].as_u64().unwrap_or(0);
                let channels = command["params"]["channels"]
                    .as_array()
                    .map(|array| {
                        array.iter().filter_map(|value| value.as_str().map(str::to_owned)).collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let cmd = command["cmd"].as_str().unwrap_or("");

                match cmd {
                    "subscribe" => {
                        for channel in &channels {
                            if channel == "orderbook_delta" {
                                let tickers: Vec<String> = command["params"]["market_tickers"]
                                    .as_array()
                                    .map(|array| {
                                        array.iter()
                                            .filter_map(|value| value.as_str().map(str::to_owned))
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                let sid = 1000 + id as i64;
                                {
                                    let mut connections = state.connections.lock().unwrap();
                                    if let Some(entry) = connections.get_mut(&id) {
                                        entry.sid_orderbook = Some(sid);
                                        entry.subscriptions.extend(tickers.iter().cloned());
                                    }
                                }
                                state.book_subscribed_count.fetch_add(tickers.len(), Ordering::SeqCst);
                                send_subscribe_ack(&state, id, id_num, "orderbook_delta", sid);
                                // Initial snapshot per requested ticker.
                                for ticker in &tickers {
                                    let seq = next_seq(&state, id);
                                    state.send_json(id, state.snapshot_body(ticker, sid, seq));
                                }
                            } else if channel == "market_lifecycle_v2" {
                                let sid = 9000 + id as i64;
                                state.lifecycle_subscribed_count.fetch_add(1, Ordering::SeqCst);
                                send_subscribe_ack(&state, id, id_num, "market_lifecycle_v2", sid);
                            }
                        }
                    }
                    "update_subscription" => {
                        let action = command["params"]["action"].as_str().unwrap_or("").to_owned();
                        let sid = command["params"]["sid"].as_i64().unwrap_or(0);
                        match action.as_str() {
                            "add_markets" => {
                                let markets: Vec<String> = command["params"]["market_tickers"]
                                    .as_array()
                                    .map(|array| {
                                        array.iter().filter_map(|value| value.as_str().map(str::to_owned)).collect()
                                    })
                                    .unwrap_or_default();
                                let full = {
                                    let mut connections = state.connections.lock().unwrap();
                                    if let Some(entry) = connections.get_mut(&id) {
                                        entry.subscriptions.extend(markets.iter().cloned());
                                        entry.subscriptions.iter().cloned().collect::<Vec<_>>()
                                    } else {
                                        Vec::new()
                                    }
                                };
                                state.book_subscribed_count.fetch_add(markets.len(), Ordering::SeqCst);
                                let seq = next_seq(&state, id);
                                state.send_json(
                                    id,
                                    json!({
                                        "id": id_num, "type": "ok", "sid": sid, "seq": seq,
                                        "msg": {"market_tickers": full}
                                    }),
                                );
                            }
                            "delete_markets" | "get_snapshot" => {
                                let markets: Vec<String> = command["params"]["market_tickers"]
                                    .as_array()
                                    .map(|array| {
                                        array.iter().filter_map(|value| value.as_str().map(str::to_owned)).collect()
                                    })
                                    .unwrap_or_default();
                                if action == "delete_markets"
                                    && let Ok(mut connections) = state.connections.lock()
                                    && let Some(entry) = connections.get_mut(&id)
                                {
                                    for ticker in &markets {
                                        entry.subscriptions.remove(ticker);
                                    }
                                } else {
                                    for ticker in &markets {
                                        let seq = next_seq(&state, id);
                                        let sid = state.connections.lock().unwrap().get(&id).and_then(|e| e.sid_orderbook).unwrap_or(sid);
                                        state.send_json(id, state.snapshot_body(ticker, sid, seq));
                                    }
                                }
                                let seq = next_seq(&state, id);
                                state.send_json(
                                    id,
                                    json!({
                                        "id": id_num, "type": "ok", "sid": sid, "seq": seq,
                                        "msg": {"market_tickers": markets}
                                    }),
                                );
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            frame = lifecycle_rx.recv() => {
                let Ok(frame) = frame else { continue };
                let (sid, is_lifecycle) = match state.connections.lock() {
                    Ok(connections) => match connections.get(&id) {
                        Some(entry) if entry.sid_orderbook.is_none() => (9000 + id as i64, true),
                        _ => (0, false),
                    },
                    Err(_) => (0, false),
                };
                if is_lifecycle {
                    let seq = next_seq(&state, id);
                    state.send_json(
                        id,
                        json!({
                            "type": "market_lifecycle_v2",
                            "sid": sid,
                            "seq": seq,
                            "sending_ts_ms": frame.ts_ms,
                            "msg": frame.msg,
                        }),
                    );
                }
            }
            _ = &mut close_rx => break,
        }
    }

    state.connections.lock().unwrap().remove(&id);
    state.close.lock().unwrap().remove(&id);
}

fn send_subscribe_ack(state: &Arc<MockState>, id: usize, request_id: u64, channel: &str, sid: i64) {
    let seq = next_seq(state, id);
    state.send_json(
        id,
        json!({
            "id": request_id, "type": "subscribed", "seq": seq,
            "msg": {"channel": channel, "sid": sid}
        }),
    );
}

fn next_seq(state: &Arc<MockState>, id: usize) -> u64 {
    let mut connections = state.connections.lock().unwrap();
    if let Some(entry) = connections.get_mut(&id) {
        entry.seq += 1;
        entry.seq
    } else {
        0
    }
}

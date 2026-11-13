#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

//! End-to-end tests for the dedicated Kalshi lifecycle control plane.
//!
//! A loopback mock speaks just enough of Kalshi's REST and websocket
//! protocols to prove the connection topology and the event flow:
//!
//! - order-book websockets never subscribe to `market_lifecycle_v2`;
//! - exactly `lifecycle_replicas` supervised sockets subscribe to it;
//! - duplicates across lifecycle replicas are applied once;
//! - an `created`/`activated` notification for an untracked market forces an
//!   out-of-cycle REST reconciliation;
//! - a terminal `settled` notification retires the market exactly once;
//! - after the lifecycle socket reconnects, the REST catalog is consulted
//!   again before the event stream is trusted.

use std::{
    fs,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, broadcast, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

use engineering_challenge::{
    config::{ExchangeKind, Settings},
    coordinator,
    report::RunReport,
};

const LIFECYCLE_SHARD: u16 = u16::MAX;
const WAIT_LIMIT: Duration = Duration::from_secs(10);
const TICKER: &str = "KXTEST-1";

fn active_market() -> Value {
    json!({
        "ticker": TICKER,
        "event_ticker": "KXTEST",
        "title": "Test market",
        "status": "active",
        "close_time": "2027-01-01T00:00:00Z",
        "price_ranges": [{"start": "0", "end": "1", "step": "0.01"}],
        "price_level_structure": "linear_cent"
    })
}

#[derive(Clone)]
struct LifecycleFrame {
    ts_ms: i64,
    msg: Value,
}

struct MockState {
    markets: Mutex<Vec<Value>>,
    http_requests: AtomicUsize,
    book_connections: AtomicUsize,
    lifecycle_connections: AtomicUsize,
    book_saw_lifecycle_subscribe: AtomicBool,
    lifecycle_saw_orderbook_subscribe: AtomicBool,
    lifecycle_tx: broadcast::Sender<LifecycleFrame>,
    lifecycle_close: Mutex<Vec<oneshot::Sender<()>>>,
    next_id: AtomicUsize,
    send_seq: AtomicI64,
}

impl MockState {
    fn new(markets: Vec<Value>) -> Self {
        let (lifecycle_tx, _) = broadcast::channel(16);
        Self {
            markets: Mutex::new(markets),
            http_requests: AtomicUsize::new(0),
            book_connections: AtomicUsize::new(0),
            lifecycle_connections: AtomicUsize::new(0),
            book_saw_lifecycle_subscribe: AtomicBool::new(false),
            lifecycle_saw_orderbook_subscribe: AtomicBool::new(false),
            lifecycle_tx,
            lifecycle_close: Mutex::new(Vec::new()),
            next_id: AtomicUsize::new(1),
            send_seq: AtomicI64::new(1_700_000_000_000),
        }
    }

    fn publish_lifecycle(&self, event_type: &str, market_ticker: &str) {
        let ts = self.send_seq.fetch_add(1_000, Ordering::SeqCst);
        let msg = match event_type {
            "created" | "activated" => json!({
                "market_ticker": market_ticker,
                "event_type": event_type,
                "close_ts": 1_800_000_000i64,
                "additional_metadata": {"title": "Mock lifecycle market"},
                "price_ranges": [{"start": "0", "end": "1", "step": "0.01"}],
                "price_level_structure": "linear_cent"
            }),
            _ => json!({
                "market_ticker": market_ticker,
                "event_type": event_type,
            }),
        };
        let _ = self.lifecycle_tx.send(LifecycleFrame { ts_ms: ts, msg });
    }

    fn close_lifecycle_connections(&self) {
        let senders = std::mem::take(&mut *self.lifecycle_close.lock().unwrap());
        for sender in senders {
            let _ = sender.send(());
        }
    }
}

struct MockKalshi {
    state: Arc<MockState>,
    http_task: JoinHandle<()>,
    ws_task: JoinHandle<()>,
    http_addr: SocketAddr,
    ws_addr: SocketAddr,
}

impl MockKalshi {
    async fn spawn(markets: Vec<Value>) -> Self {
        let state = Arc::new(MockState::new(markets));
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let ws_addr = ws_listener.local_addr().unwrap();
        let http_state = Arc::clone(&state);
        let http_task = tokio::spawn(async move { run_http(http_listener, http_state).await });
        let ws_state = Arc::clone(&state);
        let ws_task = tokio::spawn(async move { run_ws(ws_listener, ws_state).await });
        Self {
            state,
            http_task,
            ws_task,
            http_addr,
            ws_addr,
        }
    }

    fn http_url(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    fn ws_url(&self) -> String {
        format!("ws://{}", self.ws_addr)
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
            let (status, body) = if target.starts_with("/markets") || target.contains("/markets?") {
                let markets = state.markets.lock().unwrap().clone();
                (
                    200u16,
                    json!({"markets": markets, "cursor": null}).to_string(),
                )
            } else {
                (404u16, "{}".to_owned())
            };
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

async fn run_ws(listener: TcpListener, state: Arc<MockState>) {
    loop {
        let Ok((socket, _)) = listener.accept().await else {
            continue;
        };
        let id = state.next_id.fetch_add(1, Ordering::SeqCst);
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            handle_connection(id, socket, state).await;
        });
    }
}

fn snapshot_frame(sid: i64, seq: i64, ticker: &str, yes: &[Value], no: &[Value]) -> Value {
    json!({
        "type": "orderbook_snapshot",
        "sid": sid,
        "seq": seq,
        "sending_ts_ms": 1_700_000_000_001i64,
        "msg": {
            "market_ticker": ticker,
            "clock": 1,
            "yes_dollars_fp": yes,
            "no_dollars_fp": no,
        }
    })
}

async fn send_json(
    sink: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        Message,
    >,
    value: Value,
) -> std::io::Result<()> {
    let text = value.to_string();
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "websocket closed"))
}

async fn handle_connection(id: usize, socket: TcpStream, state: Arc<MockState>) {
    let Ok(websocket) = accept_async(socket).await else {
        return;
    };
    let (mut sink, mut stream) = websocket.split();
    let mut next_seq = 1i64;
    let mut role: Option<&'static str> = None;
    // Registered before any ack is sent so no lifecycle frame is missed.
    let mut lifecycle_rx = state.lifecycle_tx.subscribe();
    let (close_tx, mut close_rx) = oneshot::channel();
    let mut close_tx = Some(close_tx);

    loop {
        tokio::select! {
            message = stream.next() => {
                let Some(message) = message else { break };
                let Message::Text(text) = message.unwrap_or(Message::Close(None)) else {
                    continue;
                };
                let command: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                let channel = command["params"]["channels"][0]
                    .as_str()
                    .unwrap_or("")
                    .to_owned();
                let request_id = command["id"].as_u64().unwrap_or(0);
                match channel.as_str() {
                    "orderbook_delta" => {
                        match role {
                            Some("lifecycle") => state
                                .lifecycle_saw_orderbook_subscribe
                                .store(true, Ordering::SeqCst),
                            _ => role = Some("book"),
                        }
                        state.book_connections.fetch_add(1, Ordering::SeqCst);
                        let tickers: Vec<String> = command["params"]["market_tickers"]
                            .as_array()
                            .map(|array| {
                                array.iter().filter_map(|value| value.as_str().map(str::to_owned)).collect()
                            })
                            .unwrap_or_default();
                        // Book sockets must never see a lifecycle command.
                        if !tickers.is_empty() {
                            let sid = 100 + id as i64;
                            let _ = send_json(&mut sink, json!({
                                "id": request_id, "type": "subscribed",
                                "msg": {"channel": "orderbook_delta", "sid": sid}
                            })).await;
                            for ticker in &tickers {
                                let _ = send_json(&mut sink, snapshot_frame(sid, next_seq, ticker, &[], &[])).await;
                                next_seq += 1;
                            }
                        }
                    }
                    "market_lifecycle_v2" => {
                        match role {
                            Some("book") => state
                                .book_saw_lifecycle_subscribe
                                .store(true, Ordering::SeqCst),
                            _ => role = Some("lifecycle"),
                        }
                        state.lifecycle_connections.fetch_add(1, Ordering::SeqCst);
                        let sid = 900 + id as i64;
                        let _ = send_json(&mut sink, json!({
                            "id": request_id, "type": "subscribed",
                            "msg": {"channel": "market_lifecycle_v2", "sid": sid}
                        })).await;
                        if let Some(sender) = close_tx.take() {
                            state.lifecycle_close.lock().unwrap().push(sender);
                        }
                        next_seq = 1;
                    }
                    _ => {}
                }
            }
            frame = lifecycle_rx.recv() => {
                let Ok(frame) = frame else { continue };
                let sid = 900 + id as i64;
                if role == Some("lifecycle") {
                    // Only lifecycle connections forward lifecycle frames;
                    // book connections never receive the broadcast.
                    let message = json!({
                        "type": "market_lifecycle_v2",
                        "sid": sid,
                        "seq": next_seq,
                        "sending_ts_ms": frame.ts_ms,
                        "msg": frame.msg,
                    });
                    next_seq += 1;
                    if send_json(&mut sink, message).await.is_err() {
                        break;
                    }
                }
            }
            _ = &mut close_rx => {
                break;
            }
        }
    }
}

fn settings(mock: &MockKalshi, directory: &Path) -> Settings {
    let mut settings = Settings::for_exchange(ExchangeKind::Kalshi);
    settings.kalshi.rest_url = Some(mock.http_url());
    settings.kalshi.ws_url = Some(mock.ws_url());
    settings.kalshi.key_id = "test-key".to_owned();
    settings.kalshi.discovery_page_size = 10;
    settings.kalshi.discovery_page_delay_ms = 0;
    settings.kalshi.request_timeout_secs = 5;

    settings.collector.shards = 1;
    settings.collector.replicas = 2;
    settings.collector.lifecycle_replicas = 2;
    settings.collector.snapshot_interval_secs = 60;
    settings.collector.discovery_interval_secs = 3600;
    settings.collector.reconnect_after_secs = 1800;
    settings.collector.connection_idle_timeout_secs = 45;
    settings.collector.stale_probe_after_secs = 120;
    settings.collector.probe_interval_secs = 30;
    settings.collector.max_probes = 3;
    settings.collector.max_markets = 0;
    settings.collector.duration_secs = 0;
    settings.collector.writer_send_timeout_secs = 5;
    settings.collector.connect_concurrency = 4;
    settings.collector.max_total_connections = 16;

    settings.output.path = directory.join("orderbook.sbe").display().to_string();
    settings.output.chunk_size = 16 * 1024;
    settings.output.metadata_path = Some(directory.join("markets.json"));
    settings.output.report_path = Some(directory.join("report.json"));
    settings.output.sync_on_close = false;
    settings.metrics.listen = None;
    settings.network = Default::default();
    settings
}

fn spawn_collector(settings: Settings) -> (Arc<Notify>, JoinHandle<anyhow::Result<RunReport>>) {
    let done = Arc::new(Notify::new());
    let done_for_run = Arc::clone(&done);
    let handle = tokio::spawn(coordinator::run_with_shutdown(settings, async move {
        tokio::select! {
            _ = done_for_run.notified() => "test scenario complete".to_owned(),
            _ = sleep(Duration::from_secs(30)) => "test scenario safety timeout".to_owned(),
        }
    }));
    (done, handle)
}

async fn finish(handle: JoinHandle<anyhow::Result<RunReport>>) -> RunReport {
    timeout(Duration::from_secs(35), handle)
        .await
        .expect("collector did not stop within the limit")
        .expect("collector task failed")
        .expect("collector failed")
}

async fn wait_until(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + WAIT_LIMIT;
    loop {
        if predicate() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "condition not met within {WAIT_LIMIT:?}: {what}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn book_sockets_carry_no_lifecycle_subscription_and_the_plane_is_accounted() {
    let directory = tempfile::tempdir().unwrap();
    let key_path = directory.path().join("key.pem");
    let signing = SigningKey::from_bytes(&[7u8; 32]);
    fs::write(
        &key_path,
        ed25519_dalek::pkcs8::EncodePrivateKey::to_pkcs8_pem(
            &signing,
            ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF,
        )
        .unwrap(),
    )
    .unwrap();

    let mock = MockKalshi::spawn(vec![active_market()]).await;
    let mut settings = settings(&mock, directory.path());
    settings.kalshi.private_key_path = Some(key_path);

    let (done, handle) = spawn_collector(settings);
    wait_until("book and lifecycle planes connected", || {
        mock.state.book_connections.load(Ordering::SeqCst) == 2
            && mock.state.lifecycle_connections.load(Ordering::SeqCst) == 2
    })
    .await;

    mock.state.publish_lifecycle("created", "KXNEW-1");
    wait_until("out-of-cycle discovery reconciliation", || {
        mock.state.http_requests.load(Ordering::SeqCst) >= 2
    })
    .await;

    done.notify_one();
    let report = finish(handle).await;

    assert!(
        !mock
            .state
            .book_saw_lifecycle_subscribe
            .load(Ordering::SeqCst)
    );
    assert!(
        !mock
            .state
            .lifecycle_saw_orderbook_subscribe
            .load(Ordering::SeqCst)
    );

    let capacity = report
        .capacity
        .expect("live report must carry a capacity plan");
    assert_eq!(capacity.lifecycle_connections, 2);
    assert_eq!(capacity.total_connections, 4);

    let lifecycle_sources: Vec<_> = report
        .sources
        .iter()
        .filter(|status| status.source.shard == LIFECYCLE_SHARD)
        .collect();
    assert_eq!(lifecycle_sources.len(), 2);
    assert_eq!(report.sources.len(), 4);

    // One active-plane exchange snapshot is emitted; the shadow replica's
    // snapshot validates its own book but is not counted as active-plane
    // output by the core.
    assert_eq!(report.core.exchange_snapshots, 1);
    assert_eq!(report.core.lifecycle_events, 1);
    assert_eq!(report.core.lifecycle_duplicates, 1);
    assert_eq!(report.core.lifecycle_unknown_symbols, 1);
    assert_eq!(report.core.discovery_rounds, 2);
}

#[tokio::test]
async fn terminal_settled_retires_the_market_exactly_once() {
    let directory = tempfile::tempdir().unwrap();
    let key_path = directory.path().join("key.pem");
    let signing = SigningKey::from_bytes(&[9u8; 32]);
    fs::write(
        &key_path,
        ed25519_dalek::pkcs8::EncodePrivateKey::to_pkcs8_pem(
            &signing,
            ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF,
        )
        .unwrap(),
    )
    .unwrap();

    let mock = MockKalshi::spawn(vec![active_market()]).await;
    let mut settings = settings(&mock, directory.path());
    settings.kalshi.private_key_path = Some(key_path);

    let (done, handle) = spawn_collector(settings);
    wait_until("book and lifecycle planes connected", || {
        mock.state.book_connections.load(Ordering::SeqCst) == 2
            && mock.state.lifecycle_connections.load(Ordering::SeqCst) == 2
    })
    .await;

    mock.state.publish_lifecycle("settled", TICKER);
    sleep(Duration::from_millis(1500)).await;
    done.notify_one();
    let report = finish(handle).await;

    assert_eq!(report.core.lifecycle_events, 1);
    assert_eq!(report.core.lifecycle_duplicates, 1);
    assert_eq!(report.core.removed_symbols, 1);
    assert_eq!(report.core.current_symbols, 0);
    assert_eq!(report.core.empty_snapshots, 1);
}

#[tokio::test]
async fn lifecycle_reconnect_consults_the_rest_catalog() {
    let directory = tempfile::tempdir().unwrap();
    let key_path = directory.path().join("key.pem");
    let signing = SigningKey::from_bytes(&[11u8; 32]);
    fs::write(
        &key_path,
        ed25519_dalek::pkcs8::EncodePrivateKey::to_pkcs8_pem(
            &signing,
            ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF,
        )
        .unwrap(),
    )
    .unwrap();

    let mock = MockKalshi::spawn(vec![active_market()]).await;
    let mut settings = settings(&mock, directory.path());
    settings.kalshi.private_key_path = Some(key_path);

    let (done, handle) = spawn_collector(settings);
    wait_until("lifecycle plane connected", || {
        mock.state.lifecycle_connections.load(Ordering::SeqCst) == 2
    })
    .await;

    mock.state.close_lifecycle_connections();
    wait_until("lifecycle plane reconnected", || {
        mock.state.lifecycle_connections.load(Ordering::SeqCst) >= 3
    })
    .await;
    wait_until("reconnect triggered a REST reconciliation", || {
        mock.state.http_requests.load(Ordering::SeqCst) >= 2
    })
    .await;

    done.notify_one();
    let report = finish(handle).await;
    assert!(report.core.discovery_rounds >= 2);
}

#[tokio::test]
async fn accepted_lifecycle_events_are_applied_before_exit() {
    let directory = tempfile::tempdir().unwrap();
    let key_path = directory.path().join("key.pem");
    let signing = SigningKey::from_bytes(&[13u8; 32]);
    fs::write(
        &key_path,
        ed25519_dalek::pkcs8::EncodePrivateKey::to_pkcs8_pem(
            &signing,
            ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF,
        )
        .unwrap(),
    )
    .unwrap();

    let mock = MockKalshi::spawn(vec![active_market()]).await;
    let mut settings = settings(&mock, directory.path());
    settings.kalshi.private_key_path = Some(key_path);

    let (done, handle) = spawn_collector(settings);
    wait_until("book and lifecycle planes connected", || {
        mock.state.book_connections.load(Ordering::SeqCst) == 2
            && mock.state.lifecycle_connections.load(Ordering::SeqCst) == 2
    })
    .await;

    mock.state.publish_lifecycle("created", "KXNEW-1");
    wait_until("first lifecycle event applied", || {
        mock.state.http_requests.load(Ordering::SeqCst) >= 2
    })
    .await;
    // A second event published immediately before the stop signal must be
    // applied during the shutdown drain, not lost with it.
    mock.state.publish_lifecycle("created", "KXNEW-2");
    sleep(Duration::from_millis(500)).await;
    done.notify_one();
    let report = finish(handle).await;
    assert_eq!(report.core.lifecycle_unknown_symbols, 2);
}

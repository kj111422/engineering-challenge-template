//! Auditable egress laboratory.
//!
//! Every test in this file runs against loopback test doubles: a mock HTTP
//! CONNECT proxy, a mock SOCKS5 server, and a real websocket acceptor. No
//! venue is ever contacted. The lab proves the parts that matter:
//!
//! - direct connections are unchanged;
//! - an explicit proxy carries the websocket handshake end-to-end;
//! - a failing proxy never falls back to direct networking;
//! - venue authentication still goes only to the venue;
//! - route provenance in the journal manifest is complete and credential-free.
//!
//! Live venue validation is out of scope: it requires a route and an account
//! the venue permits, which cannot be established here.

// Test helpers accept unwraps the same way `#[cfg(test)]` modules do.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    collections::VecDeque,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use base64::Engine as _;
use engineering_challenge::{
    config::NetworkSettings,
    egress::{EgressRoute, http_client_with},
    model::SourceId,
    ws::{self, ConnectOptions},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// ---------------------------------------------------------------- venue side

struct Venue {
    addr: SocketAddr,
    accepted: Arc<AtomicU64>,
}

/// Start a websocket acceptor that records connections and answers one
/// message. If `accept` is never reached the counter stays at zero.
async fn start_venue() -> Venue {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicU64::new(0));
    let accepted_task = Arc::clone(&accepted);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            accepted_task.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                    let _ = ws
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            "venue-ok".into(),
                        ))
                        .await;
                    // Keep the socket open until the client disconnects so the
                    // handshake and first frame both observe a healthy peer.
                    let _ = ws.next().await;
                }
            });
        }
    });
    Venue { addr, accepted }
}

// ------------------------------------------------------------- proxy infra

#[derive(Clone)]
struct ProxyBehavior {
    /// Respond with this status after a valid CONNECT request.
    status: u16,
    /// Require this Proxy-Authorization header (user, password).
    require_basic_auth: Option<(String, String)>,
    /// Append junk after the CONNECT response terminator.
    trailing_bytes: bool,
}

impl Default for ProxyBehavior {
    fn default() -> Self {
        Self {
            status: 200,
            require_basic_auth: None,
            trailing_bytes: false,
        }
    }
}

struct CapturingProxy {
    addr: SocketAddr,
    /// Every captured request head, in arrival order.
    requests: Arc<Mutex<VecDeque<String>>>,
    accepted: Arc<AtomicU64>,
}

/// A bounded HTTP CONNECT proxy. It never forwards anywhere unless the
/// behavior table allows it; every request is captured for assertions.
async fn start_http_connect_proxy(
    behavior: ProxyBehavior,
    upstream: Option<SocketAddr>,
) -> CapturingProxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(VecDeque::new()));
    let accepted = Arc::new(AtomicU64::new(0));
    let tasks_requests = Arc::clone(&requests);
    let tasks_accepted = Arc::clone(&accepted);
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            tasks_accepted.fetch_add(1, Ordering::SeqCst);
            let behavior = behavior.clone();
            let requests = Arc::clone(&tasks_requests);
            tokio::spawn(async move {
                let head = match read_head(&mut client, 8 * 1024).await {
                    Ok(head) => head,
                    Err(_) => return,
                };
                let text = String::from_utf8_lossy(&head).into_owned();
                requests.lock().unwrap().push_back(text.clone());

                let basic = extract_basic_auth(&text);
                if let Some((want_user, want_pass)) = &behavior.require_basic_auth {
                    let expected = base64::engine::general_purpose::STANDARD
                        .encode(format!("{want_user}:{want_pass}").as_bytes());
                    if basic.as_deref() != Some(expected.as_str()) {
                        let _ = client
                            .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n")
                            .await;
                        return;
                    }
                }
                if behavior.status != 200 {
                    let body = b"refused";
                    let _ = client
                        .write_all(
                            format!(
                                "HTTP/1.1 {} REFUSED\r\nContent-Length: {}\r\n\r\n",
                                behavior.status,
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await;
                    let _ = client.write_all(body).await;
                    return;
                }
                let Some(upstream) = upstream else {
                    let _ = client
                        .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    return;
                };
                let Ok(mut target) = TcpStream::connect(upstream).await else {
                    let _ = client
                        .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                        .await;
                    return;
                };
                let mut response = b"HTTP/1.1 200 Connection Established\r\n\r\n".to_vec();
                if behavior.trailing_bytes {
                    response.extend_from_slice(b"leak");
                }
                if client.write_all(&response).await.is_err() {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
            });
        }
    });
    CapturingProxy {
        addr,
        requests,
        accepted,
    }
}

async fn read_head(stream: &mut TcpStream, limit: usize) -> io::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(512);
    let mut buf = [0u8; 1024];
    loop {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "eof before head terminator",
            ));
        }
        head.extend_from_slice(&buf[..read]);
        if head.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(head);
        }
        if head.len() > limit {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "head too large"));
        }
    }
}

fn extract_basic_auth(head: &str) -> Option<String> {
    for line in head.lines() {
        let lowered = line.to_ascii_lowercase();
        if let Some(rest) = lowered.strip_prefix("proxy-authorization: basic ") {
            return Some(rest.trim().to_owned());
        }
    }
    None
}

#[derive(Clone)]
struct SocksBehavior {
    /// Require username/password negotiation (RFC 1929) with these credentials.
    require_password: Option<(String, String)>,
    /// Reject the CONNECT command (reply host unreachable).
    refuse_connect: bool,
}

struct SocksProxy {
    addr: SocketAddr,
    /// Atypical address type of the first CONNECT request, if any: 1, 3 or 4.
    atyp: Arc<Mutex<Option<u8>>>,
    accepted: Arc<AtomicU64>,
}

/// A minimal SOCKS5 server: greeting, optional RFC 1929 password auth, and a
/// CONNECT command. It never forwards to arbitrary destinations.
async fn start_socks5_proxy(behavior: SocksBehavior, upstream: SocketAddr) -> SocksProxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let atyp_seen = Arc::new(Mutex::new(None));
    let accepted = Arc::new(AtomicU64::new(0));
    let task_atyp = Arc::clone(&atyp_seen);
    let task_accepted = Arc::clone(&accepted);
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            task_accepted.fetch_add(1, Ordering::SeqCst);
            let behavior = behavior.clone();
            let atyp_seen = Arc::clone(&task_atyp);
            tokio::spawn(async move {
                let mut greeting = [0u8; 2];
                if client.read_exact(&mut greeting).await.is_err() {
                    return;
                }
                let mut methods = vec![0u8; greeting[1] as usize];
                if client.read_exact(&mut methods).await.is_err() {
                    return;
                }
                match &behavior.require_password {
                    Some(_) => {
                        // Advertise username/password only (RFC 1929).
                        if client.write_all(&[0x05, 0x02]).await.is_err() {
                            return;
                        }
                        let mut version = [0u8; 1];
                        if client.read_exact(&mut version).await.is_err() {
                            return;
                        }
                        let mut ulen = [0u8; 1];
                        if client.read_exact(&mut ulen).await.is_err() {
                            return;
                        }
                        let mut user = vec![0u8; ulen[0] as usize];
                        if client.read_exact(&mut user).await.is_err() {
                            return;
                        }
                        let mut plen = [0u8; 1];
                        if client.read_exact(&mut plen).await.is_err() {
                            return;
                        }
                        let mut pass = vec![0u8; plen[0] as usize];
                        if client.read_exact(&mut pass).await.is_err() {
                            return;
                        }
                        let ok = matches!(&behavior.require_password, Some((u, p)) if user == u.as_bytes() && pass == p.as_bytes());
                        if client
                            .write_all(&[0x01, if ok { 0x00 } else { 0x01 }])
                            .await
                            .is_err()
                        {
                            return;
                        }
                        if !ok {
                            return;
                        }
                    }
                    None => {
                        if client.write_all(&[0x05, 0x00]).await.is_err() {
                            return;
                        }
                    }
                }
                let mut request = [0u8; 4];
                if client.read_exact(&mut request).await.is_err() {
                    return;
                }
                let atyp = request[3];
                *atyp_seen.lock().unwrap() = Some(atyp);
                match atyp {
                    0x01 => {
                        let mut skip = [0u8; 4];
                        if client.read_exact(&mut skip).await.is_err() {
                            return;
                        }
                    }
                    0x03 => {
                        let mut len = [0u8; 1];
                        if client.read_exact(&mut len).await.is_err() {
                            return;
                        }
                        let mut domain = vec![0u8; len[0] as usize];
                        if client.read_exact(&mut domain).await.is_err() {
                            return;
                        }
                    }
                    0x04 => {
                        let mut skip = [0u8; 16];
                        if client.read_exact(&mut skip).await.is_err() {
                            return;
                        }
                    }
                    _ => return,
                }
                let mut port = [0u8; 2];
                if client.read_exact(&mut port).await.is_err() {
                    return;
                }
                if behavior.refuse_connect {
                    let _ = client
                        .write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                        .await;
                    return;
                }
                let Ok(mut target) = TcpStream::connect(upstream).await else {
                    let _ = client
                        .write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                        .await;
                    return;
                };
                if client
                    .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
            });
        }
    });
    SocksProxy {
        addr,
        atyp: atyp_seen,
        accepted,
    }
}

fn proxy_settings(remote_dns: bool) -> NetworkSettings {
    NetworkSettings {
        egress_mode: "explicit_proxy".to_owned(),
        proxy_url_env: "MARKETDATA_EGRESSLAB_LOOKUP".to_owned(),
        proxy_remote_dns: remote_dns,
        connect_timeout_secs: 5,
        ..NetworkSettings::default()
    }
}

/// Remote DNS is meaningful only for SOCKS5; derive it from the scheme so
/// HTTP CONNECT tests do not trip the typed validation by accident.
fn dns_for(proxy_url: &str) -> bool {
    proxy_url.starts_with("socks5h://")
}

/// Resolve an explicit-proxy route with an injected lookup; no environment
/// variables are read or written anywhere in this test binary.
fn route_for(proxy_url: &str) -> EgressRoute {
    let network = proxy_settings(dns_for(proxy_url));
    EgressRoute::from_network_with(&network, |name| {
        (name == "MARKETDATA_EGRESSLAB_LOOKUP").then(|| proxy_url.to_owned())
    })
    .expect("proxy route must resolve")
}

fn proxy_options(proxy_url: &str) -> ConnectOptions {
    ConnectOptions {
        connect_timeout: Duration::from_secs(5),
        egress: route_for(proxy_url),
        ..ConnectOptions::default()
    }
}

/// Build the exact HTTP client the adapters build, with the lookup
/// injected so no process environment is touched.
fn rest_client(proxy_url: &str) -> reqwest::Client {
    let network = proxy_settings(dns_for(proxy_url));
    http_client_with("egress-lab/0.1", Duration::from_secs(5), &network, |name| {
        (name == "MARKETDATA_EGRESSLAB_LOOKUP").then(|| proxy_url.to_owned())
    })
    .expect("proxy HTTP client must build")
}

// --------------------------------------------------------------- tests

#[tokio::test]
async fn direct_connection_reaches_a_loopback_venue() {
    let venue = start_venue().await;
    let options = ConnectOptions::default();
    let mut stream = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect("direct connect must succeed against the loopback venue");
    match ws::next_payload(&mut stream).await.expect("venue frames") {
        ws::WsPayload::Text(text) => assert_eq!(text, "venue-ok"),
        other => panic!("unexpected payload: {other:?}"),
    }
    assert_eq!(venue.accepted.load(Ordering::SeqCst), 1);
    let _ = ws::close(stream).await;
}

#[tokio::test]
async fn http_connect_tunnel_carries_the_websocket_handshake() {
    let venue = start_venue().await;
    let proxy = start_http_connect_proxy(ProxyBehavior::default(), Some(venue.addr)).await;
    let options = proxy_options(&format!("http://{}", proxy.addr));

    let mut stream = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect("CONNECT tunnel must carry the websocket handshake");
    match ws::next_payload(&mut stream).await.expect("venue frames") {
        ws::WsPayload::Text(text) => assert_eq!(text, "venue-ok"),
        other => panic!("unexpected payload: {other:?}"),
    }
    {
        let requests = proxy.requests.lock().unwrap();
        let head = requests
            .front()
            .expect("proxy must capture the CONNECT request");
        assert!(head.starts_with("CONNECT "), "proxy received: {head}");
        assert!(head.contains(&venue.addr.to_string()));
        assert_eq!(venue.accepted.load(Ordering::SeqCst), 1);
    }
    let _ = ws::close(stream).await;
}

#[tokio::test]
async fn http_connect_refusal_never_falls_back_to_direct() {
    let venue = start_venue().await;
    let proxy = start_http_connect_proxy(
        ProxyBehavior {
            status: 407,
            ..ProxyBehavior::default()
        },
        Some(venue.addr),
    )
    .await;
    let options = proxy_options(&format!("http://{}", proxy.addr));

    let error = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect_err("a refused tunnel must fail");
    assert!(
        format!("{error:#}").contains("407"),
        "error must name the proxy refusal: {error:#}"
    );
    // Fail-closed: the venue must not be dialled at all when the proxy
    // refuses the tunnel.
    assert_eq!(venue.accepted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn http_connect_wrong_proxy_password_is_rejected() {
    let venue = start_venue().await;
    let proxy = start_http_connect_proxy(
        ProxyBehavior {
            require_basic_auth: Some(("egress-operator".to_owned(), "egress-secret".to_owned())),
            ..ProxyBehavior::default()
        },
        Some(venue.addr),
    )
    .await;
    let options = proxy_options(&format!(
        "http://egress-operator:wrong-password@{}",
        proxy.addr
    ));

    let error = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect_err("wrong proxy credentials must fail");
    assert!(
        format!("{error:#}").contains("407"),
        "error must name the proxy auth refusal: {error:#}"
    );
    assert_eq!(venue.accepted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn http_connect_trailing_bytes_are_rejected() {
    let venue = start_venue().await;
    let proxy = start_http_connect_proxy(
        ProxyBehavior {
            trailing_bytes: true,
            ..ProxyBehavior::default()
        },
        Some(venue.addr),
    )
    .await;
    let options = proxy_options(&format!("http://{}", proxy.addr));

    let error = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect_err("a proxy that emits trailing bytes must fail");
    assert!(
        format!("{error:#}").contains("unexpected byte"),
        "error must reject the extra bytes: {error:#}"
    );
    // The proxy relays regardless of what the client later discards; the
    // invariant is that the client, not the venue handshake, rejected it.
    assert_eq!(proxy.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn socks5_tunnel_carries_the_websocket_handshake_with_remote_dns() {
    let venue = start_venue().await;
    let proxy = start_socks5_proxy(
        SocksBehavior {
            require_password: None,
            refuse_connect: false,
        },
        venue.addr,
    )
    .await;
    let options = proxy_options(&format!("socks5h://{}", proxy.addr));

    let mut stream = ws::connect(
        &format!("ws://localhost:{}", venue.addr.port()),
        &[],
        &options,
    )
    .await
    .expect("SOCKS5 with remote DNS must carry the websocket handshake");
    match ws::next_payload(&mut stream).await.expect("venue frames") {
        ws::WsPayload::Text(text) => assert_eq!(text, "venue-ok"),
        other => panic!("unexpected payload: {other:?}"),
    }
    {
        let atyp = *proxy.atyp.lock().unwrap();
        assert_eq!(atyp, Some(0x03), "socks5h must send a domain-name target");
    }
    assert_eq!(venue.accepted.load(Ordering::SeqCst), 1);
    let _ = ws::close(stream).await;
}

#[tokio::test]
async fn socks5_local_dns_sends_an_ip_literal_target() {
    let venue = start_venue().await;
    let proxy = start_socks5_proxy(
        SocksBehavior {
            require_password: None,
            refuse_connect: false,
        },
        venue.addr,
    )
    .await;
    let options = proxy_options(&format!("socks5://{}", proxy.addr));

    let mut stream = ws::connect(
        &format!("ws://localhost:{}", venue.addr.port()),
        &[],
        &options,
    )
    .await
    .expect("SOCKS5 with local DNS must carry the websocket handshake");
    match ws::next_payload(&mut stream).await.expect("venue frames") {
        ws::WsPayload::Text(text) => assert_eq!(text, "venue-ok"),
        other => panic!("unexpected payload: {other:?}"),
    }
    {
        let atyp = proxy.atyp.lock().unwrap();
        assert!(
            matches!(*atyp, Some(0x01) | Some(0x04)),
            "local DNS must send an IP literal, got {atyp:?}"
        );
    }
    let _ = ws::close(stream).await;
}

#[tokio::test]
async fn socks5_wrong_password_is_rejected() {
    let venue = start_venue().await;
    let proxy = start_socks5_proxy(
        SocksBehavior {
            require_password: Some(("egress-operator".to_owned(), "egress-secret".to_owned())),
            refuse_connect: false,
        },
        venue.addr,
    )
    .await;
    let options = proxy_options(&format!(
        "socks5h://egress-operator:wrong-password@{}",
        proxy.addr
    ));

    let error = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect_err("wrong SOCKS5 credentials must fail");
    assert!(
        format!("{error:#}").contains("SOCKS5"),
        "error must name the SOCKS5 negotiation: {error:#}"
    );
    assert_eq!(
        *proxy.atyp.lock().unwrap(),
        None,
        "the CONNECT request must not be parsed after auth failure"
    );
    assert_eq!(venue.accepted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn socks5_refused_connect_is_a_proxy_stage_failure() {
    let venue = start_venue().await;
    let proxy = start_socks5_proxy(
        SocksBehavior {
            require_password: None,
            refuse_connect: true,
        },
        venue.addr,
    )
    .await;
    let options = proxy_options(&format!("socks5h://{}", proxy.addr));

    let error = ws::connect(&format!("ws://{}", venue.addr), &[], &options)
        .await
        .expect_err("a refused SOCKS5 CONNECT must fail");
    assert!(
        format!("{error:#}").contains("SOCKS5"),
        "error must name the SOCKS5 negotiation: {error:#}"
    );
    assert_eq!(venue.accepted.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rest_requests_fail_closed_through_a_dead_proxy() {
    // The HTTP client the adapters use, with a proxy endpoint that cannot be
    // reached. The request must fail fast; there is no fallback to direct.
    let client = rest_client("http://127.0.0.1:1");
    let start = std::time::Instant::now();
    let error = client
        .get("http://127.0.0.1:2/should-never-be-dialed")
        .send()
        .await
        .expect_err("REST through a dead proxy must fail");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "fail-closed must be prompt, took {elapsed:?}"
    );
    let _ = error;
}

#[tokio::test]
async fn rest_and_websocket_share_the_same_egress_route() {
    // One proxy serves both transports. The websocket leg runs end to end
    // through it; the REST leg is asserted to enter the same CONNECT route
    // (its TLS handshake then fails against the websocket acceptor, which is
    // expected: capture of the CONNECT line is the invariant under test).
    let venue = start_venue().await;
    let proxy = start_http_connect_proxy(ProxyBehavior::default(), Some(venue.addr)).await;

    let ws_options = proxy_options(&format!("http://{}", proxy.addr));
    let mut stream = ws::connect(&format!("ws://{}", venue.addr), &[], &ws_options)
        .await
        .expect("websocket through the shared proxy must work");
    match ws::next_payload(&mut stream).await.expect("venue frames") {
        ws::WsPayload::Text(text) => assert_eq!(text, "venue-ok"),
        other => panic!("unexpected payload: {other:?}"),
    }

    let client = rest_client(&format!("http://{}", proxy.addr));
    let _ = client
        .get("https://gamma-api.polymarket.com/markets/keyset")
        .send()
        .await
        .expect_err("the upstream is a websocket acceptor, not a TLS venue; failure is expected");

    {
        let requests = proxy.requests.lock().unwrap();
        assert!(
            requests
                .iter()
                .any(|head| head.starts_with("CONNECT gamma-api.polymarket.com:443")),
            "the REST request must traverse the same proxy; captured: {requests:?}"
        );
        assert_eq!(
            proxy.accepted.load(Ordering::SeqCst),
            2,
            "one websocket session and one REST attempt must both traverse the proxy"
        );
    }
    let _ = ws::close(stream).await;
}

#[tokio::test]
async fn journal_manifest_records_route_provenance_without_credentials() {
    use engineering_challenge::journal::{
        self, DecodeOutcome, FrameKind, JournalConfig, JournalMeta, RawFrame, SyncMode,
    };

    let directory = tempfile::tempdir().expect("tempdir");
    let proxy_url = "http://egress-operator:egress-secret@127.0.0.1:9";
    let network = proxy_settings(false);
    let route = EgressRoute::from_network_with(&network, |name| {
        (name == "MARKETDATA_EGRESSLAB_LOOKUP").then(|| proxy_url.to_owned())
    })
    .expect("proxy configuration must resolve")
    .provenance(&network);

    let meta = JournalMeta {
        run_id: "lab-run".to_owned(),
        venue: "test-venue".to_owned(),
        code_commit: "lab".to_owned(),
        config_sha256: "ab".repeat(32),
        route: Some(route),
    };
    let handle = journal::JournalHandle::start(
        JournalConfig {
            directory: directory.path().to_path_buf(),
            queue_capacity: 16,
            enqueue_timeout: Duration::from_millis(250),
            flush_interval: Duration::from_millis(10),
            sync_interval: Duration::from_millis(20),
            segment_max_bytes: 0,
            segment_max_seconds: 0,
            sync_mode: SyncMode::Interval,
        },
        meta,
    )
    .expect("journal start");

    handle
        .record(
            SourceId::new(0, 0),
            1,
            1,
            1_000_000,
            RawFrame {
                kind: FrameKind::Text,
                payload: b"{}".to_vec(),
            },
            DecodeOutcome::decoded(1),
        )
        .expect("record accepted");
    let summary = handle
        .shutdown(Duration::from_secs(5))
        .expect("journal finalizes");
    assert_eq!(summary.record_count, 1);

    let manifest_text =
        std::fs::read_to_string(directory.path().join("lab-run").join("manifest.json"))
            .expect("manifest");
    let manifest: Value = serde_json::from_str(&manifest_text).expect("manifest is JSON");
    let route = &manifest["route"];
    assert_eq!(route["mode"], "explicit_proxy");
    assert_eq!(route["kind"], "http_connect");
    assert_eq!(route["host"], "127.0.0.1");
    assert_eq!(route["port"].as_u64(), Some(9));
    assert_eq!(route["fingerprint_sha256"].as_str().map(str::len), Some(64));
    assert!(
        !manifest_text.contains("egress-secret"),
        "manifest must never contain the proxy password"
    );
    assert!(
        !manifest_text.contains("egress-operator"),
        "manifest must never contain the proxy username"
    );
}

#[tokio::test]
async fn http_client_builds_in_direct_and_proxy_modes() {
    http_client_with(
        "lab-agent",
        Duration::from_secs(1),
        &NetworkSettings::default(),
        |_| None,
    )
    .expect("direct client must build");

    http_client_with(
        "lab-agent",
        Duration::from_secs(1),
        &proxy_settings(false),
        |name| (name == "MARKETDATA_EGRESSLAB_LOOKUP").then(|| "http://127.0.0.1:9".to_owned()),
    )
    .expect("proxy client must build");
}

/// A one-shot Gamma markets stub returning a single-market page.
async fn start_gamma_stub() -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    use serde_json::json;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let mut buffer = vec![0u8; 16 * 1024];
        let Ok(read) = socket.read(&mut buffer).await else {
            return;
        };
        recorded
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(&buffer[..read]).into_owned());
        let body = json!({
            "markets": [{
                "conditionId": "0xcondition",
                "question": "Will the egress lab pass?",
                "active": true,
                "closed": false,
                "archived": false,
                "enableOrderBook": true,
                "acceptingOrders": true,
                "version": "v1",
                "clobTokenIds": r#"["111"]"#,
                "outcomes": r#"["Yes"]"#,
                "orderPriceMinTickSize": "0.01",
            }],
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
    });
    (address, requests)
}

#[tokio::test]
async fn rest_through_socks5_tunnels_to_the_gamma_stub() {
    let (stub_addr, requests) = start_gamma_stub().await;
    // The mock SOCKS5 server forwards every tunnel to the Gamma stub.
    let proxy = start_socks5_proxy(
        SocksBehavior {
            require_password: None,
            refuse_connect: false,
        },
        stub_addr,
    )
    .await;
    let network = proxy_settings(true);
    let client = http_client_with("egress-lab/0.1", Duration::from_secs(5), &network, |name| {
        (name == "MARKETDATA_EGRESSLAB_LOOKUP").then(|| format!("socks5h://{}", proxy.addr))
    })
    .expect("proxy HTTP client must build");

    let response = client
        .get(format!(
            "http://localhost:{}/markets/keyset?limit=1",
            stub_addr.port()
        ))
        .send()
        .await
        .expect("REST through SOCKS5 must succeed against the loopback stub");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body = response.text().await.expect("gamma stub body");
    assert!(body.contains("0xcondition"));
    assert_eq!(
        requests.lock().unwrap().len(),
        1,
        "exactly one Gamma request must be tunneled"
    );
    assert_eq!(*proxy.atyp.lock().unwrap(), Some(0x03));
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1);
    let _ = network;
}

// ------------------------------------------------------------- probe

#[tokio::test]
async fn network_probe_reports_transport_ok_direct() {
    use engineering_challenge::{config::Settings, probe};

    let venue = start_venue().await;
    let settings = Settings::default();
    let report = probe::run_with_route(
        &settings,
        engineering_challenge::egress::EgressRoute::Direct,
        Some(&format!("ws://{}", venue.addr)),
    )
    .await
    .expect("probe must run");

    assert_eq!(report.result, "transport_ok");
    assert!(report.is_transport_ok());
    assert_eq!(report.reason, None);
    let stages: Vec<&str> = report
        .stages
        .iter()
        .map(|stage| stage.stage.as_str())
        .collect();
    assert!(
        stages.contains(&"dns"),
        "direct probe must report dns: {stages:?}"
    );
    assert!(
        stages.contains(&"venue_tcp"),
        "direct probe must report venue_tcp: {stages:?}"
    );
    assert!(
        stages.contains(&"ws_upgrade"),
        "direct probe must report ws_upgrade: {stages:?}"
    );
    assert!(report.total_us > 0);
    assert_eq!(report.route.mode, "direct");
    assert_eq!(report.route.fingerprint_sha256.len(), 64);

    let json = serde_json::to_string(&report).expect("report serializes");
    assert!(!json.contains("password"));
    let _ = venue; // acceptor tasks end with the test runtime
}

#[tokio::test]
async fn network_probe_reports_proxy_tcp_connect_failure() {
    use engineering_challenge::{config::Settings, probe};

    let settings = Settings::default();
    let report = probe::run_with_route(
        &settings,
        route_for("http://127.0.0.1:1"),
        Some("ws://venue.invalid:99/feed"),
    )
    .await
    .expect("probe must run");

    assert_eq!(report.result, "failed");
    assert_eq!(report.reason.as_deref(), Some("proxy_tcp_connect"));
    assert_eq!(report.route.kind, "http_connect");
    assert_eq!(report.route.host.as_deref(), Some("127.0.0.1"));
}

#[tokio::test]
async fn network_probe_reports_proxy_tunnel_failure() {
    use engineering_challenge::{config::Settings, probe};

    let venue = start_venue().await;
    let proxy = start_http_connect_proxy(
        ProxyBehavior {
            status: 407,
            ..ProxyBehavior::default()
        },
        Some(venue.addr),
    )
    .await;
    let settings = Settings::default();
    let report = probe::run_with_route(
        &settings,
        route_for(&format!("http://{}", proxy.addr)),
        Some(&format!("ws://{}", venue.addr)),
    )
    .await
    .expect("probe must run");

    assert_eq!(report.result, "failed");
    assert_eq!(report.reason.as_deref(), Some("proxy_tunnel"));
    let stages: Vec<&str> = report
        .stages
        .iter()
        .map(|stage| stage.stage.as_str())
        .collect();
    assert!(
        stages.contains(&"proxy_dns"),
        "probe must reach the proxy-dns stage: {stages:?}"
    );
    assert!(
        stages.contains(&"proxy_tcp"),
        "probe must reach the proxy-tcp stage: {stages:?}"
    );
    assert!(
        stages.contains(&"tunnel"),
        "probe must reach the tunnel stage: {stages:?}"
    );
    assert_eq!(
        venue.accepted.load(Ordering::SeqCst),
        0,
        "the venue must never be dialled"
    );
}

#[tokio::test]
async fn network_probe_reports_socks5_transport_ok() {
    use engineering_challenge::{config::Settings, probe};

    let venue = start_venue().await;
    let proxy = start_socks5_proxy(
        SocksBehavior {
            require_password: None,
            refuse_connect: false,
        },
        venue.addr,
    )
    .await;
    let settings = Settings::default();
    let report = probe::run_with_route(
        &settings,
        route_for(&format!("socks5h://{}", proxy.addr)),
        Some(&format!("ws://localhost:{}", venue.addr.port())),
    )
    .await
    .expect("probe must run");

    assert_eq!(report.result, "transport_ok");
    let stages: Vec<&str> = report
        .stages
        .iter()
        .map(|stage| stage.stage.as_str())
        .collect();
    for expected in ["proxy_dns", "proxy_tcp", "tunnel", "ws_upgrade"] {
        assert!(
            stages.contains(&expected),
            "missing stage {expected}: {stages:?}"
        );
    }
}

#[tokio::test]
async fn network_probe_config_failure_is_a_failed_report_not_a_panic() {
    use engineering_challenge::{config::Settings, probe};

    let settings = Settings::default();
    // An explicit_proxy route whose environment variable does not exist.
    let mut settings = settings;
    settings.network.egress_mode = "explicit_proxy".to_owned();
    settings.network.proxy_url_env = "MARKETDATA_EGRESSLAB_DEFINITELY_UNSET_PROXY".to_owned();
    let report = probe::run(&settings, Some("ws://venue.invalid:99/feed"))
        .await
        .expect("configuration failure must produce a report, not an error");

    assert_eq!(report.result, "failed");
    assert_eq!(report.reason.as_deref(), Some("config"));
    assert_eq!(report.route.mode, "unresolved");
    assert!(
        std::env::var("MARKETDATA_EGRESSLAB_DEFINITELY_UNSET_PROXY").is_err(),
        "the lab must never set this variable; the failure path depends on it being unset"
    );
}

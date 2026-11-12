//! Small helpers shared by the exchange websocket sessions.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use anyhow::{Context, bail};
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt, stream::FuturesUnordered};
use tokio::net::{TcpSocket, TcpStream, lookup_host};
use tokio_socks::tcp::socks5::Socks5Stream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{self, Message},
};
use url::Url;

use crate::egress::{self, EgressRoute, ProxyEndpoint, ProxyScheme};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// One application payload read from a websocket.
#[derive(Debug)]
pub enum WsPayload {
    Text(String),
    /// Binary data frame, preserved as received.
    Binary(Vec<u8>),
    /// Control/heartbeat frame; the connection is still alive.
    Idle,
}

/// Transport controls for a websocket connection.
#[derive(Clone)]
pub struct ConnectOptions {
    /// Set `TCP_NODELAY` on the connected socket.
    pub tcp_nodelay: bool,
    /// Local IP to bind before connecting. The address family must
    /// match the resolved remote address.
    pub source_ip: Option<IpAddr>,
    /// Fixed local source port, or an ephemeral port when absent.
    pub source_port: Option<u16>,
    /// Number of simultaneous handshakes to race. The first success wins and
    /// every other attempt is dropped, closing its socket.
    pub attempts: usize,
    /// Deadline for the whole attempt: DNS resolution, TCP connect, TLS
    /// negotiation, and the websocket upgrade, across every raced candidate.
    pub connect_timeout: Duration,
    /// How this connection reaches the venue: direct, or one explicitly
    /// configured proxy. Resolved from `[network]` at settings load time;
    /// never falls back from a proxy to direct networking.
    pub egress: EgressRoute,
    /// Optional stage recorder used by `network-probe`. Zero-cost when unset.
    pub trace: Option<ProbeTrace>,
}

/// One recorded phase of a connection attempt.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ProbeStage {
    /// Stage name: `dns`, `proxy_tcp`, `tunnel`, `venue_tcp`, `ws_upgrade`.
    pub stage: String,
    /// Time from the start of the enclosing attempt to the completion of the
    /// stage, in microseconds.
    pub elapsed_us: u64,
}

/// Shared stage recorder. `Clone` shares the same buffer; drain it with
/// [`ProbeTrace::finish`] exactly once after the attempt ends.
#[derive(Clone, Default)]
pub struct ProbeTrace {
    inner: std::sync::Arc<std::sync::Mutex<Vec<ProbeStage>>>,
    started: Option<std::time::Instant>,
}

impl ProbeTrace {
    /// Start a trace with the wall-clock reference for stage timings.
    pub fn begin() -> Self {
        Self {
            inner: Default::default(),
            started: Some(std::time::Instant::now()),
        }
    }

    fn record(&self, stage: &str) {
        let elapsed_us = self
            .started
            .map(|started| started.elapsed().as_micros() as u64)
            .unwrap_or(0);
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProbeStage {
                stage: stage.to_owned(),
                elapsed_us,
            });
    }

    /// Consume the trace, returning the ordered stage list.
    pub fn finish(self) -> Vec<ProbeStage> {
        std::mem::take(
            &mut self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl std::fmt::Debug for ProbeTrace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProbeTrace")
    }
}

impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectOptions")
            .field("tcp_nodelay", &self.tcp_nodelay)
            .field("source_ip", &self.source_ip)
            .field("source_port", &self.source_port)
            .field("attempts", &self.attempts)
            .field("connect_timeout", &self.connect_timeout)
            .field("egress", &self.egress)
            .finish()
    }
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            source_ip: None,
            source_port: None,
            attempts: 1,
            connect_timeout: Duration::from_secs(30),
            egress: EgressRoute::Direct,
            trace: None,
        }
    }
}

impl ConnectOptions {
    /// Derive the options for one connection identified by `ordinal`.
    ///
    /// A configured source port is a base: connection *n* binds
    /// `base + ordinal`, so parallel connections never collide on one local
    /// port. The caller must pass a distinct ordinal per connection (for
    /// example `shard * 256 + replica`).
    pub fn for_ordinal(&self, ordinal: u32) -> anyhow::Result<Self> {
        let mut options = self.clone();
        if let Some(base) = options.source_port {
            let port = u32::from(base)
                .checked_add(ordinal)
                .and_then(|port| u16::try_from(port).ok())
                .with_context(|| {
                    format!(
                        "source port base {base} leaves no room for connection ordinal {ordinal} \
                         (the effective port must stay within 65535)"
                    )
                })?;
            options.source_port = Some(port);
        }
        Ok(options)
    }
}

/// Open a websocket, optionally adding handshake headers.
///
/// The TCP connection is established before the upgrade so the collector can apply
/// `TCP_NODELAY`, bind a source address, and race several full handshakes.
/// Racing covers TLS and the websocket upgrade as well as the TCP handshake,
/// so a connection is only selected after it is usable.
///
/// The entire attempt -- name resolution through the websocket upgrade,
/// across every raced candidate -- is bounded by
/// [`ConnectOptions::connect_timeout`]; exceeding it returns an error so the
/// caller can retry with backoff like any other transient failure.
///
/// With an explicit proxy, both the venue-upgrade and the proxy negotiation
/// are inside the same deadline, and failure never falls back to direct
/// networking.
pub async fn connect(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    options: &ConnectOptions,
) -> anyhow::Result<WsStream> {
    match tokio::time::timeout(
        options.connect_timeout,
        connect_inner(url, headers, options),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "websocket connection to {url} timed out after {:?}",
            options.connect_timeout
        ),
    }
}

async fn connect_inner(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    options: &ConnectOptions,
) -> anyhow::Result<WsStream> {
    let parsed = Url::parse(url).with_context(|| format!("parsing websocket URL {url}"))?;
    let raw_host = parsed
        .host_str()
        .with_context(|| format!("websocket URL {url} has no host"))?;
    let host = egress::strip_brackets(raw_host);
    let port = parsed
        .port_or_known_default()
        .with_context(|| format!("websocket URL {url} has no known port"))?;

    // Resolve the right target set for the selected route: the venue itself
    // (direct) or the proxy endpoint. The venue's own hostname is resolved
    // by the proxy when SOCKS5 remote DNS is in effect.
    let addresses: Vec<SocketAddr> = match &options.egress {
        EgressRoute::Direct => resolve_host(host, port, options.source_ip)
            .await
            .with_context(|| format!("resolving {host}:{port}"))?,
        EgressRoute::ExplicitProxy(endpoint) => {
            resolve_host(&endpoint.host, endpoint.port, options.source_ip)
                .await
                .with_context(|| {
                    format!("resolving proxy host {}:{}", endpoint.host, endpoint.port)
                })?
        }
    };
    match &options.egress {
        EgressRoute::Direct => record_stage(options, "dns"),
        EgressRoute::ExplicitProxy(_) => record_stage(options, "proxy_dns"),
    }

    let attempts = options.attempts.max(1);
    let mut pending = FuturesUnordered::new();
    for attempt in 0..attempts {
        let address = addresses[attempt % addresses.len()];
        let options = options.clone();
        let url = url.to_owned();
        let headers = headers.to_vec();
        let host = host.to_owned();
        pending.push(async move {
            let stream = establish_stream(&host, port, address, &options)
                .await
                .with_context(|| format!("connection attempt {} to {address}", attempt + 1))?;
            connect_once(&url, &headers, &options, stream)
                .await
                .with_context(|| format!("connection attempt {} to {address}", attempt + 1))
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = pending.next().await {
        match result {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                // An authentication rejection is definitive; do not spend the
                // remaining attempts or hide the HTTP status in an aggregate.
                if matches!(http_status(&error), Some(401 | 403)) {
                    return Err(error);
                }
                errors.push(format!("{error:#}"));
            }
        }
    }
    bail!(
        "all {attempts} websocket connection attempt(s) to {url} failed: {}",
        errors.join("; ")
    )
}

/// Resolve a host, keeping only the families that can be bound by the
/// configured source IP, and ensure the result is non-empty.
fn record_stage(options: &ConnectOptions, stage: &str) {
    if let Some(trace) = options.trace.as_ref() {
        trace.record(stage);
    }
}

async fn resolve_host(
    host: &str,
    port: u16,
    source_ip: Option<IpAddr>,
) -> anyhow::Result<Vec<SocketAddr>> {
    let mut addresses: Vec<SocketAddr> = lookup_host((host, port)).await?.collect();
    if let Some(source_ip) = source_ip {
        addresses.retain(|address| address.is_ipv4() == source_ip.is_ipv4());
        anyhow::ensure!(
            !addresses.is_empty(),
            "no resolved address for {host}:{port} matches source IP {source_ip}"
        );
    }
    anyhow::ensure!(
        !addresses.is_empty(),
        "resolving {host}:{port} returned no addresses"
    );
    Ok(addresses)
}

/// Bind and connect the client-side socket to `address`, applying the
/// configured local bind and `TCP_NODELAY`.
async fn establish_tcp(address: SocketAddr, options: &ConnectOptions) -> anyhow::Result<TcpStream> {
    let socket = match address {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }
    .context("creating TCP socket")?;
    let local_ip = options.source_ip.unwrap_or(match address {
        SocketAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        SocketAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    });
    socket
        .bind(SocketAddr::new(local_ip, options.source_port.unwrap_or(0)))
        .with_context(|| {
            format!(
                "binding local address {local_ip}:{}",
                options.source_port.unwrap_or(0)
            )
        })?;
    let stream = socket
        .connect(address)
        .await
        .with_context(|| format!("TCP connect to {address}"))?;
    stream
        .set_nodelay(options.tcp_nodelay)
        .context("setting TCP_NODELAY")?;
    Ok(stream)
}

/// Establish the stream the TLS/WebSocket handshake will run on: a direct
/// TCP connection to the venue, or a tunnel through the configured proxy.
async fn establish_stream(
    venue_host: &str,
    venue_port: u16,
    address: SocketAddr,
    options: &ConnectOptions,
) -> anyhow::Result<TcpStream> {
    match &options.egress {
        EgressRoute::Direct => {
            let stream = establish_tcp(address, options).await?;
            record_stage(options, "venue_tcp");
            Ok(stream)
        }
        EgressRoute::ExplicitProxy(endpoint) => {
            let stream = establish_tcp(address, options).await.with_context(|| {
                format!("proxy host connect {}:{}", endpoint.host, endpoint.port)
            })?;
            record_stage(options, "proxy_tcp");
            let tunneled = match endpoint.scheme {
                ProxyScheme::HttpConnect => {
                    http_connect_tunnel(stream, venue_host, venue_port, endpoint).await
                }
                ProxyScheme::Socks5 => {
                    socks5_tunnel(stream, venue_host, venue_port, endpoint).await
                }
            };
            record_stage(options, "tunnel");
            tunneled
        }
    }
}

/// Perform the HTTP CONNECT negotiation and return the bare tunnel stream.
///
/// The status line is validated, a successful 2xx is required, and any bytes
/// after the header block are rejected so an intercepting or buggy proxy can
/// never stream unsolicited data into the venue handshake.
async fn http_connect_tunnel(
    mut stream: TcpStream,
    venue_host: &str,
    venue_port: u16,
    endpoint: &ProxyEndpoint,
) -> anyhow::Result<TcpStream> {
    let authority = if venue_host.contains(':') && !venue_host.starts_with('[') {
        format!("[{venue_host}]:{venue_port}")
    } else {
        format!("{venue_host}:{venue_port}")
    };
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(proxy_auth) = endpoint_auth(endpoint) {
        let token = base64::engine::general_purpose::STANDARD.encode(proxy_auth.0.as_bytes());
        // The header is consumed by the proxy only and never logged.
        request.push_str(&format!("Proxy-Authorization: Basic {token}\r\n"));
    }
    request.push_str("User-Agent: voluntas-collector\r\n\r\n");
    tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes())
        .await
        .context("writing proxy CONNECT request")?;
    tokio::io::AsyncWriteExt::flush(&mut stream)
        .await
        .context("flushing proxy CONNECT request")?;

    let head = read_http_head(&mut stream, 8 * 1024).await?;
    let status = parse_connect_status(&head)?;
    if !(200..300).contains(&status) {
        // A non-2xx reply may legitimately carry a body; its contents are
        // discarded with the connection. The status is the diagnostic.
        bail!("proxy CONNECT refused the tunnel: HTTP {status}");
    }
    if let Some(end) = find_subslice(&head, b"\r\n\r\n") {
        // A 2xx response to CONNECT carries no body; anything extra means an
        // intercepting or buggy proxy and must not flow into the venue
        // handshake.
        anyhow::ensure!(
            end + 4 == head.len(),
            "proxy sent {0} unexpected byte(s) after the CONNECT response headers",
            head.len() - (end + 4)
        );
    }
    Ok(stream)
}

/// Read an HTTP/1.x header block, bounded in size and terminated by CRLFCRLF.
///
/// The returned buffer may include bytes already received after the header
/// block; the caller decides whether they are acceptable for the response's
/// status class.
async fn read_http_head(stream: &mut TcpStream, limit: usize) -> anyhow::Result<Vec<u8>> {
    let mut head = Vec::with_capacity(512);
    let mut buf = [0u8; 2048];
    loop {
        let read = tokio::io::AsyncReadExt::read(stream, &mut buf)
            .await
            .context("reading proxy CONNECT response")?;
        if read == 0 {
            bail!("proxy closed the connection before answering CONNECT");
        }
        head.extend_from_slice(&buf[..read]);
        if find_subslice(&head, b"\r\n\r\n").is_some() {
            return Ok(head);
        }
        anyhow::ensure!(
            head.len() <= limit,
            "proxy CONNECT response exceeded {limit} bytes without a header terminator"
        );
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_connect_status(head: &[u8]) -> anyhow::Result<u16> {
    let text = std::str::from_utf8(head).context("proxy CONNECT response was not UTF-8")?;
    let status_line = text
        .lines()
        .next()
        .with_context(|| "proxy CONNECT response had no status line")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .with_context(|| "proxy CONNECT status line had no status code")?
        .parse::<u16>()
        .context("proxy CONNECT status code was not a number")?;
    Ok(status)
}

/// Perform the SOCKS5 negotiation through the pre-bound stream and return the
/// bare venue stream.
async fn socks5_tunnel(
    stream: TcpStream,
    venue_host: &str,
    venue_port: u16,
    endpoint: &ProxyEndpoint,
) -> anyhow::Result<TcpStream> {
    let result = match (endpoint.remote_dns, endpoint_auth(endpoint)) {
        (true, None) => {
            Socks5Stream::<TcpStream>::connect_with_socket(stream, (venue_host, venue_port)).await
        }
        (true, Some((user, password))) => {
            Socks5Stream::<TcpStream>::connect_with_password_and_socket(
                stream,
                (venue_host, venue_port),
                user,
                password,
            )
            .await
        }
        (false, None) => {
            let target = local_resolve((venue_host, venue_port)).await?;
            Socks5Stream::<TcpStream>::connect_with_socket(stream, target).await
        }
        (false, Some((user, password))) => {
            let target = local_resolve((venue_host, venue_port)).await?;
            Socks5Stream::<TcpStream>::connect_with_password_and_socket(
                stream, target, user, password,
            )
            .await
        }
    };
    let socks_stream = result.map_err(|error| {
        // tokio-socks errors carry protocol/stage detail but never the
        // negotiated credentials; keep the chain readable and short.
        anyhow::anyhow!("SOCKS5 proxy negotiation failed: {error}")
    })?;
    // The socket was bound and marked nodelay in establish_tcp.
    Ok(socks_stream.into_inner())
}

/// Resolve the venue locally for SOCKS5 local-DNS mode.
async fn local_resolve(target: (&str, u16)) -> anyhow::Result<SocketAddr> {
    lookup_host(target)
        .await
        .context("resolving venue host for SOCKS5 local DNS")?
        .next()
        .context("resolving venue host for SOCKS5 local DNS returned no addresses")
}

fn endpoint_auth(endpoint: &ProxyEndpoint) -> Option<(&str, &str)> {
    endpoint.authentication()
}

/// Bind a client socket for `address` and return it connected. Kept separate
/// from [`establish_stream`] so the CONNECT/SOCKS5 stages observe the raw
/// stream.
async fn connect_once(
    url: &str,
    headers: &[(http::HeaderName, http::HeaderValue)],
    options: &ConnectOptions,
    stream: TcpStream,
) -> anyhow::Result<WsStream> {
    let mut request = url
        .into_client_request()
        .with_context(|| format!("building websocket request for {url}"))?;
    for (name, value) in headers {
        request.headers_mut().insert(name, value.clone());
    }
    let (stream, response) = client_async_tls_with_config(request, stream, None, None)
        .await
        .with_context(|| format!("websocket handshake with {url}"))?;
    record_stage(options, "ws_upgrade");
    let status = response.status();
    if status != http::StatusCode::SWITCHING_PROTOCOLS {
        bail!("websocket handshake returned HTTP {status}");
    }
    Ok(stream)
}

/// Extract an HTTP status from a failed websocket handshake, if the underlying
/// tungstenite error carried one. Authentication failures can then be treated as
/// fatal instead of being retried forever.
pub fn http_status(error: &anyhow::Error) -> Option<u16> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<tungstenite::Error>()
            .and_then(|error| match error {
                tungstenite::Error::Http(response) => Some(response.status().as_u16()),
                _ => None,
            })
    })
}

/// Read the next data payload, transparently answering control-frame pings.
pub async fn next_payload(stream: &mut WsStream) -> anyhow::Result<WsPayload> {
    loop {
        let message = stream.next().await.context("websocket stream ended")??;
        match message {
            Message::Text(text) => return Ok(WsPayload::Text(text.to_string())),
            Message::Binary(bytes) => return Ok(WsPayload::Binary(bytes.to_vec())),
            Message::Ping(payload) => {
                stream.send(Message::Pong(payload)).await?;
                return Ok(WsPayload::Idle);
            }
            Message::Pong(_) => return Ok(WsPayload::Idle),
            Message::Close(frame) => {
                let detail = frame.map_or_else(
                    || "without frame".to_owned(),
                    |f| format!("code={} reason={}", f.code, f.reason),
                );
                bail!("websocket closed by peer ({detail})");
            }
            Message::Frame(_) => {}
        }
    }
}

pub async fn send_text(stream: &mut WsStream, text: String) -> anyhow::Result<()> {
    stream.send(Message::Text(text.into())).await?;
    Ok(())
}

pub async fn send_ping(stream: &mut WsStream) -> anyhow::Result<()> {
    stream.send(Message::Ping(Vec::new().into())).await?;
    Ok(())
}

pub async fn close(mut stream: WsStream) -> anyhow::Result<()> {
    // Best effort: a peer that already vanished must not turn shutdown into an
    // error path.
    let _ = stream.close(None).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_connect_options_disable_nagle_and_do_not_race() {
        let options = ConnectOptions::default();
        assert!(options.tcp_nodelay);
        assert_eq!(options.attempts, 1);
        assert!(options.source_ip.is_none());
        assert!(options.source_port.is_none());
        assert!(options.connect_timeout > Duration::ZERO);
        assert!(matches!(options.egress, EgressRoute::Direct));
    }

    #[tokio::test]
    async fn stalled_handshake_is_bounded_by_the_connect_timeout() {
        // A server that accepts the TCP connection but never answers the
        // websocket upgrade must not hang the caller past the deadline.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                use tokio::io::AsyncReadExt as _;
                let mut buffer = [0u8; 1024];
                let _ = stream.read(&mut buffer).await;
                std::future::pending::<()>().await;
            }
        });

        let options = ConnectOptions {
            connect_timeout: Duration::from_millis(250),
            ..ConnectOptions::default()
        };
        let started = tokio::time::Instant::now();
        let error = connect(&format!("ws://{address}"), &[], &options)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("timed out"),
            "unexpected error: {error:#}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the deadline must bound the attempt, took {:?}",
            started.elapsed()
        );
        server.abort();
    }

    #[tokio::test]
    async fn dropping_a_read_future_loses_no_frame() {
        // The session supervisor may drop a frame future when another
        // `select!` branch wins. The read state lives in the stream, so the
        // dropped poll must not consume or lose either frame.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            ws.send(Message::Text("first".into())).await.unwrap();
            ws.send(Message::Text("second".into())).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut stream = connect(&format!("ws://{address}"), &[], &ConnectOptions::default())
            .await
            .unwrap();
        // Poll once against an immediately-ready branch and drop the read
        // while it is still waiting for the first frame.
        tokio::select! {
            biased;
            _ = next_payload(&mut stream) => panic!("no frame should be ready yet"),
            _ = std::future::ready(()) => {}
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Text(text) => assert_eq!(text, "first"),
            WsPayload::Binary(_) | WsPayload::Idle => panic!("expected a text frame"),
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Text(text) => assert_eq!(text, "second"),
            WsPayload::Binary(_) | WsPayload::Idle => panic!("expected a text frame"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn websocket_ping_and_pong_are_reported_as_idle() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            ws.send(Message::Ping(b"keepalive".to_vec().into()))
                .await
                .unwrap();
            ws.send(Message::Pong(b"ack".to_vec().into()))
                .await
                .unwrap();
            ws.send(Message::Text("after-control".into()))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut stream = connect(&format!("ws://{address}"), &[], &ConnectOptions::default())
            .await
            .unwrap();
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Idle => {}
            WsPayload::Text(_) | WsPayload::Binary(_) => panic!("a ping must be idle"),
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Idle => {}
            WsPayload::Text(_) | WsPayload::Binary(_) => panic!("a pong must be idle"),
        }
        match next_payload(&mut stream).await.unwrap() {
            WsPayload::Text(text) => assert_eq!(text, "after-control"),
            WsPayload::Binary(_) | WsPayload::Idle => panic!("expected a text frame"),
        }
        server.await.unwrap();
    }

    #[test]
    fn pinned_source_ports_are_derived_per_connection() {
        let options = ConnectOptions {
            source_port: Some(40_000),
            ..ConnectOptions::default()
        };
        assert_eq!(
            options.for_ordinal(0).unwrap().source_port,
            Some(40_000),
            "the first connection keeps the base port"
        );
        assert_eq!(
            options.for_ordinal(3).unwrap().source_port,
            Some(40_003),
            "later connections shift by their ordinal"
        );
        assert_eq!(
            options.for_ordinal(65_536).unwrap_err().to_string(),
            "source port base 40000 leaves no room for connection ordinal 65536 (the effective port must stay within 65535)"
        );
    }

    #[test]
    fn ephemeral_ports_ignore_the_ordinal() {
        let options = ConnectOptions::default();
        assert_eq!(options.for_ordinal(500).unwrap().source_port, None);
    }

    #[test]
    fn find_subslice_locates_terminator() {
        assert_eq!(
            find_subslice(b"HTTP/1.1 200 OK\r\n\r\n", b"\r\n\r\n"),
            Some(15)
        );
        assert_eq!(find_subslice(b"no terminator", b"\r\n\r\n"), None);
    }

    #[test]
    fn parse_connect_status_reads_the_status_code() {
        assert_eq!(
            parse_connect_status(b"HTTP/1.1 200 Connection Established").unwrap(),
            200
        );
        assert_eq!(
            parse_connect_status(b"HTTP/1.0 407 Proxy Authentication Required").unwrap(),
            407
        );
        assert!(parse_connect_status(b"garbage").is_err());
    }
}

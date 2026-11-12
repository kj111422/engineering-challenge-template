//! `network-probe`: a transport diagnostic for the collector's egress path.
//!
//! The probe never authenticates to a venue and never collects market data.
//! It exercises exactly one real stage sequence — DNS, TCP, optional proxy
//! negotiation, TLS and the websocket upgrade — and reports them separately,
//! so "connected" cannot be mistaken for "a feed session is healthy".
//!
//! Result levels (see ADR 0002): the probe implements levels 0–2 (config
//! parses, route is usable, TLS validates and the upgrade completes). Levels
//! 3–4 require a venue-approved session, live subscription, capture, journal
//! verification and deterministic replay, which are out of scope here.
//!
//! Credentials never appear in the report. The venue URL is operator input;
//! proxy credentials are read from the environment and are not printed,
//! logged, or hashed.

use std::time::Instant;

use serde::Serialize;

use crate::{
    config::{ExchangeKind, Settings},
    egress::{EgressRoute, RouteProvenance},
    util::sha256_hex,
    ws::{self, ConnectOptions, ProbeStage, ProbeTrace},
};

/// Outcome taxonomy. `transport_ok` means the websocket handshake completed
/// (HTTP 101 or a closed-but-reached peer for non-HTTP upgrades); it is not a
/// statement about feed completeness.
const RESULT_OK: &str = "transport_ok";
const RESULT_FAILED: &str = "failed";

/// Machine-readable probe report, written to stdout or `--out`.
#[derive(Debug, Clone, Serialize)]
pub struct ProbeReport {
    pub schema_version: u32,
    pub tool: &'static str,
    pub binary_version: &'static str,
    pub git_commit: &'static str,
    /// Non-secret route provenance; credentials can never appear.
    pub route: RouteProvenance,
    /// The target websocket URL the operator supplied (or the configured
    /// exchange endpoint).
    pub target: String,
    /// Stage timings in attempt order. Missing stages mean the attempt did
    /// not reach them.
    pub stages: Vec<ProbeStage>,
    /// Wall time of the whole attempt, in microseconds.
    pub total_us: u64,
    /// `transport_ok` or `failed`.
    pub result: String,
    /// Deterministic classifier label for failures; null on success.
    pub reason: Option<String>,
}

impl ProbeReport {
    /// Whether the websocket handshake completed.
    #[must_use]
    pub fn is_transport_ok(&self) -> bool {
        self.result == RESULT_OK
    }
}

/// Map a transport failure onto a deterministic reason label.
///
/// The match is intentionally string-based on the anyhow chain we build in
/// `ws.rs`; it is a routing aid for humans and CI, not a parser contract.
/// Authentication rejections win over every other class.
#[must_use]
pub fn classify_reason(error: &anyhow::Error) -> String {
    if let Some(status) = ws::http_status(error)
        && matches!(status, 401 | 403)
    {
        return "venue_auth".to_owned();
    }
    let text = format!("{error:#}");
    for (needle, reason) in [
        ("timed out", "timeout"),
        ("resolving", "dns"),
        ("proxy host connect", "proxy_tcp_connect"),
        ("proxy CONNECT refused", "proxy_tunnel"),
        ("proxy closed", "proxy_tunnel"),
        ("SOCKS5 proxy negotiation", "proxy_tunnel"),
        ("Socks5", "proxy_tunnel"),
        ("websocket handshake", "websocket_upgrade"),
        ("binding local address", "tcp_bind"),
        ("TCP connect to", "tcp_connect"),
        ("creating TCP socket", "tcp_socket"),
    ] {
        if text.contains(needle) {
            return reason.to_owned();
        }
    }
    "unknown".to_owned()
}

/// Run the probe against `target` using the loaded settings, resolving the
/// route from the process environment.
///
/// The default target is the websocket endpoint of the configured exchange;
/// `--target` overrides it verbatim. A configuration-level failure is a
/// failed report (`reason: "config"`), not a panic.
pub async fn run(settings: &Settings, target: Option<&str>) -> anyhow::Result<ProbeReport> {
    match EgressRoute::from_network(&settings.network) {
        Ok(route) => run_with_route(settings, route, target).await,
        Err(_error) => {
            let target_report = target.map_or_else(|| default_target(settings), str::to_owned);
            Ok(failed_config_report(&target_report))
        }
    }
}

/// Run the probe with an explicit route. Semantically identical to [`run`]
/// once the route exists; it is the deterministic entry point for tests and
/// for callers that resolved the route themselves.
pub async fn run_with_route(
    settings: &Settings,
    route: EgressRoute,
    target: Option<&str>,
) -> anyhow::Result<ProbeReport> {
    let target = target.map_or_else(|| default_target(settings), str::to_owned);
    let provenance = route.provenance(&settings.network);

    let mut options = ConnectOptions {
        tcp_nodelay: settings.network.tcp_nodelay,
        source_ip: settings.network.source_ip,
        source_port: settings.network.source_port,
        attempts: 1,
        connect_timeout: std::time::Duration::from_secs(
            settings.network.connect_timeout_secs.max(1),
        ),
        egress: route,
        trace: Some(ProbeTrace::begin()),
    };

    let started = Instant::now();
    let outcome = ws::connect(&target, &[], &options).await;
    let total_us = started.elapsed().as_micros() as u64;
    let trace = options.trace.take();
    let stages = trace.map(ProbeTrace::finish).unwrap_or_default();

    let (result, reason) = match &outcome {
        Ok(_) => (RESULT_OK.to_owned(), None),
        Err(error) => (RESULT_FAILED.to_owned(), Some(classify_reason(error))),
    };
    let report = ProbeReport {
        schema_version: 1,
        tool: "engineering-challenge network-probe",
        binary_version: env!("CARGO_PKG_VERSION"),
        git_commit: option_env!("GIT_COMMIT")
            .or(option_env!("GITHUB_SHA"))
            .unwrap_or("unknown"),
        route: provenance,
        target,
        stages,
        total_us,
        result,
        reason,
    };
    Ok(report)
}

fn failed_config_report(target: &str) -> ProbeReport {
    // The reason label is deliberately coarse: configuration failures are
    // corrected by the operator, not classified for machines.
    ProbeReport {
        schema_version: 1,
        tool: "engineering-challenge network-probe",
        binary_version: env!("CARGO_PKG_VERSION"),
        git_commit: option_env!("GIT_COMMIT")
            .or(option_env!("GITHUB_SHA"))
            .unwrap_or("unknown"),
        route: RouteProvenance {
            mode: "unresolved".to_owned(),
            kind: "unresolved".to_owned(),
            host: None,
            port: None,
            remote_dns: None,
            label: None,
            fingerprint_sha256: sha256_hex(b"egress-route/v1|unresolved"),
        },
        target: target.to_owned(),
        stages: Vec::new(),
        total_us: 0,
        result: RESULT_FAILED.to_owned(),
        reason: Some("config".to_owned()),
    }
}

fn default_target(settings: &Settings) -> String {
    match settings.exchange {
        ExchangeKind::Kalshi => settings.kalshi.environment.ws_url().to_owned(),
        ExchangeKind::Polymarket => settings.polymarket.ws_url.clone(),
    }
}

/// Serialize a report to JSON text. Serialization of this fixed schema
/// cannot fail; failure collapses to a minimal marker instead of panicking.
#[must_use]
pub fn to_json_string_lossy(report: &ProbeReport) -> String {
    match serde_json::to_string_pretty(report) {
        Ok(text) => text,
        Err(_) => "{\"result\":\"failed\"}".to_owned(),
    }
}

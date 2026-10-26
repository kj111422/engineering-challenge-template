//! Lightweight Prometheus endpoint and shared counters.
//!
//! The collector avoids a metrics framework here: one lock-protected
//! snapshot is updated once per second, and the HTTP handler formats it on
//! demand. No data-path counter is ever blocked on the network.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
};
use tracing::{debug, warn};

use crate::{
    exchange::DiscoveryStats,
    model::{SourceId, SourceStatus},
    persistence::PersistenceStatus,
    time::now_us,
};

/// Collector data-path counters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectorStats {
    pub discovery_rounds: u64,
    pub discovered_symbols: u64,
    pub added_symbols: u64,
    pub removed_symbols: u64,
    pub exchange_snapshots: u64,
    pub periodic_snapshots: u64,
    pub failover_snapshots: u64,
    pub final_snapshots: u64,
    pub empty_snapshots: u64,
    pub deltas_emitted: u64,
    pub deltas_rejected: u64,
    pub sequence_resets: u64,
    /// Kalshi exchange-clock decreases observed on an active book. These are
    /// expected across exchange maintenance and start a new comparison epoch.
    pub clock_resets: u64,
    pub source_switches: u64,
    pub stale_probes: u64,
    pub stale_failovers: u64,
    pub crossed_snapshots_skipped: u64,
    pub duplicate_events: u64,
    pub lifecycle_events: u64,
    pub current_symbols: u64,
    pub shard_count: u64,
    /// Discontinuity incidents detected (gaps in event continuity)
    pub discontinuity_incidents: u64,
    /// Discontinuity incidents where the gap interval is unquantifiable (no sequence/clock)
    pub unquantifiable_gaps: u64,
    /// Symbols currently in Resyncing continuity state
    pub resyncing_symbols: u64,
    /// Symbols currently in Stale continuity state
    pub stale_symbols: u64,
}

/// Counters owned by the writer thread.
#[derive(Debug, Default)]
pub struct WriterCounters {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    pub flushes: AtomicU64,
    pub errors: AtomicU64,
    pub queue_depth: AtomicU64,
    pub segments: AtomicU64,
}

impl WriterCounters {
    #[must_use]
    pub fn snapshot(&self) -> WriterStats {
        WriterStats {
            messages: self.messages.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            segments: self.segments.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriterStats {
    pub messages: u64,
    pub bytes: u64,
    pub flushes: u64,
    pub errors: u64,
    pub queue_depth: u64,
    pub segments: u64,
}

/// Coverage gauges derived from the collector core.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageStats {
    pub expected_symbols: u64,
    /// Symbols with an active book established by an exchange snapshot.
    pub anchored_symbols: u64,
    /// Anchored books whose last valid update exceeded the freshness window.
    pub stale_symbols: u64,
    /// Symbols in Unseeded continuity state (no snapshot yet)
    pub unseeded_symbols: u64,
    /// Symbols in Live continuity state (anchored, fresh, no known gap)
    pub live_symbols: u64,
    /// Symbols in Stale continuity state (freshness window exceeded)
    pub stale_symbols_detail: u64,
    /// Symbols in Resyncing continuity state (gap incident open)
    pub resyncing_symbols: u64,
    /// Symbols in Terminal continuity state (venue lifecycle terminal)
    pub terminal_symbols: u64,
    /// Subscription entries the configured sources should hold.
    pub expected_subscriptions: u64,
    /// Subscription entries acknowledged as sent by connected sources.
    pub subscribed_symbols: u64,
    pub shards_with_connected_source: u64,
    pub shards: u64,
    pub sources_connected: u64,
    pub sources_total: u64,
    pub oldest_anchor_us: Option<i64>,
}

/// Catalog and discovery freshness exposed to readiness and metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CatalogHealth {
    pub mode: String,
    /// `cold`, `warm`, or `warm-stale`.
    pub source: String,
    pub generated_at_us: Option<i64>,
    pub last_discovery_finished_at_us: Option<i64>,
    pub last_discovery_error: Option<String>,
}

/// Monotonic discovery counters. Per-round values are gauges; these accumulate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryTotals {
    pub rounds: u64,
    pub pages: u64,
    pub selected_markets: u64,
    pub outcome_symbols: u64,
    pub malformed_records: u64,
    pub retries: u64,
}

/// Thresholds used by `/readyz`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessPolicy {
    pub min_anchored_fraction: f64,
    pub max_catalog_age_secs: u64,
    pub max_stale_secs: u64,
}

impl Default for ReadinessPolicy {
    fn default() -> Self {
        Self {
            min_anchored_fraction: 0.99,
            max_catalog_age_secs: 3600,
            max_stale_secs: 120,
        }
    }
}

/// Live inputs to readiness and metrics that are not part of the core state.
#[derive(Debug, Clone, Default)]
pub struct RuntimeStatus {
    pub coverage: CoverageStats,
    pub catalog: CatalogHealth,
    pub discovery: Option<DiscoveryStats>,
    pub discovery_failures: u64,
    pub persistence: PersistenceStatus,
    pub writer_healthy: bool,
    pub fatal_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessCheck {
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessReport {
    pub status: String,
    pub ready: bool,
    pub uptime_seconds: f64,
    pub checks: BTreeMap<String, ReadinessCheck>,
    pub coverage: CoverageStats,
    pub catalog: CatalogHealth,
    pub persistence: PersistenceStatus,
    pub writer_healthy: bool,
    pub fatal_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub exchange: String,
    pub started_at_us: i64,
    pub updated_at_us: i64,
    pub core: CollectorStats,
    pub writer: WriterStats,
    pub sources: Vec<SourceStatus>,
    pub coverage: CoverageStats,
    pub catalog: CatalogHealth,
    pub persistence: PersistenceStatus,
    pub writer_healthy: bool,
    pub fatal_error: Option<String>,
    pub discovery: Option<DiscoveryStats>,
    pub discovery_failures: u64,
    pub discovery_totals: DiscoveryTotals,
    #[serde(skip)]
    pub discovery_counted_at_us: i64,
}

/// Shared state read by the HTTP handler and run-report writer.
#[derive(Clone)]
pub struct MetricsRegistry {
    exchange: Arc<str>,
    started_at_us: i64,
    policy: ReadinessPolicy,
    state: Arc<RwLock<MetricsSnapshot>>,
}

impl MetricsRegistry {
    #[must_use]
    pub fn new(exchange: &str, policy: ReadinessPolicy) -> Self {
        let now = now_us().unwrap_or(0);
        Self {
            exchange: Arc::from(exchange),
            started_at_us: now,
            policy,
            state: Arc::new(RwLock::new(MetricsSnapshot {
                exchange: exchange.to_owned(),
                started_at_us: now,
                updated_at_us: now,
                core: CollectorStats::default(),
                writer: WriterStats::default(),
                sources: Vec::new(),
                coverage: CoverageStats::default(),
                catalog: CatalogHealth::default(),
                persistence: PersistenceStatus::default(),
                writer_healthy: true,
                fatal_error: None,
                discovery: None,
                discovery_failures: 0,
                discovery_totals: DiscoveryTotals::default(),
                discovery_counted_at_us: 0,
            })),
        }
    }

    pub fn update(
        &self,
        core: CollectorStats,
        writer: WriterStats,
        sources: &BTreeMap<SourceId, SourceStatus>,
        status: RuntimeStatus,
    ) {
        let Ok(mut state) = self.state.write() else {
            return;
        };
        state.updated_at_us = now_us().unwrap_or(state.updated_at_us);
        state.core = core;
        state.writer = writer;
        state.sources = sources.values().cloned().collect();
        state.coverage = status.coverage;
        state.catalog = status.catalog;
        state.persistence = status.persistence;
        state.writer_healthy = status.writer_healthy;
        state.fatal_error = status.fatal_error;
        state.discovery_failures = status.discovery_failures;
        if let Some(discovery) = status.discovery {
            if discovery.finished_at_us > state.discovery_counted_at_us {
                state.discovery_totals.rounds = state.discovery_totals.rounds.saturating_add(1);
                state.discovery_totals.pages =
                    state.discovery_totals.pages.saturating_add(discovery.pages);
                state.discovery_totals.selected_markets = state
                    .discovery_totals
                    .selected_markets
                    .saturating_add(discovery.selected_markets);
                state.discovery_totals.outcome_symbols = state
                    .discovery_totals
                    .outcome_symbols
                    .saturating_add(discovery.outcome_symbols);
                state.discovery_totals.malformed_records = state
                    .discovery_totals
                    .malformed_records
                    .saturating_add(discovery.malformed_records);
                state.discovery_totals.retries = state
                    .discovery_totals
                    .retries
                    .saturating_add(discovery.retries);
                state.discovery_counted_at_us = discovery.finished_at_us;
            }
            state.discovery = Some(discovery);
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.state
            .read()
            .map(|state| state.clone())
            .unwrap_or_else(|_| MetricsSnapshot {
                exchange: self.exchange.to_string(),
                started_at_us: self.started_at_us,
                updated_at_us: self.started_at_us,
                core: CollectorStats::default(),
                writer: WriterStats::default(),
                sources: Vec::new(),
                coverage: CoverageStats::default(),
                catalog: CatalogHealth::default(),
                persistence: PersistenceStatus::default(),
                writer_healthy: false,
                fatal_error: Some("metrics state lock poisoned".to_owned()),
                discovery: None,
                discovery_failures: 0,
                discovery_totals: DiscoveryTotals::default(),
                discovery_counted_at_us: 0,
            })
    }

    /// Evaluate every readiness invariant against one consistent snapshot.
    #[must_use]
    pub fn readiness(&self) -> ReadinessReport {
        let snapshot = self.snapshot();
        let now = now_us().unwrap_or(snapshot.updated_at_us);
        let coverage = &snapshot.coverage;
        let mut checks = BTreeMap::new();

        let sources_ok = coverage.sources_total > 0
            && coverage.shards > 0
            && coverage.shards_with_connected_source == coverage.shards;
        checks.insert(
            "sources".to_owned(),
            ReadinessCheck {
                ok: sources_ok,
                detail: format!(
                    "{}/{} sources connected across {}/{} shards",
                    coverage.sources_connected,
                    coverage.sources_total,
                    coverage.shards_with_connected_source,
                    coverage.shards
                ),
            },
        );

        let anchored_fraction = if coverage.expected_symbols == 0 {
            0.0
        } else {
            coverage.anchored_symbols as f64 / coverage.expected_symbols as f64
        };
        let anchored_ok =
            coverage.expected_symbols > 0 && anchored_fraction >= self.policy.min_anchored_fraction;
        checks.insert(
            "anchored".to_owned(),
            ReadinessCheck {
                ok: anchored_ok,
                detail: format!(
                    "{}/{} symbols anchored ({:.4} required >= {:.4})",
                    coverage.anchored_symbols,
                    coverage.expected_symbols,
                    anchored_fraction,
                    self.policy.min_anchored_fraction
                ),
            },
        );

        let freshness_ok = coverage.anchored_symbols > 0 && coverage.stale_symbols == 0;
        checks.insert(
            "freshness".to_owned(),
            ReadinessCheck {
                ok: freshness_ok,
                detail: format!(
                    "{} anchored symbols older than {} seconds",
                    coverage.stale_symbols, self.policy.max_stale_secs
                ),
            },
        );

        let last_refresh = snapshot
            .catalog
            .last_discovery_finished_at_us
            .or(snapshot.catalog.generated_at_us);
        let catalog_age_us = last_refresh.map(|time| now.saturating_sub(time).max(0));
        let max_catalog_age_us =
            i64::try_from(self.policy.max_catalog_age_secs.saturating_mul(1_000_000))
                .unwrap_or(i64::MAX);
        let catalog_ok = catalog_age_us.is_some_and(|age| age <= max_catalog_age_us)
            && snapshot.catalog.last_discovery_error.is_none();
        checks.insert(
            "catalog".to_owned(),
            ReadinessCheck {
                ok: catalog_ok,
                detail: match catalog_age_us {
                    Some(age) if snapshot.catalog.last_discovery_error.is_none() => format!(
                        "last accepted catalog round was {} seconds ago (limit {})",
                        age / 1_000_000, self.policy.max_catalog_age_secs
                    ),
                    Some(age) => format!(
                        "last discovery failed ({:?}); last accepted catalog round was {} seconds ago",
                        snapshot.catalog.last_discovery_error, age / 1_000_000
                    ),
                    None => "no accepted catalog round".to_owned(),
                },
            },
        );

        let writer_ok = snapshot.writer_healthy && snapshot.writer.errors == 0;
        checks.insert(
            "writer".to_owned(),
            ReadinessCheck {
                ok: writer_ok,
                detail: format!(
                    "healthy={}, errors={}, queue_depth={}",
                    snapshot.writer_healthy, snapshot.writer.errors, snapshot.writer.queue_depth
                ),
            },
        );

        let persistence_ok = snapshot.persistence.last_error.is_none();
        checks.insert(
            "persistence".to_owned(),
            ReadinessCheck {
                ok: persistence_ok,
                detail: snapshot
                    .persistence
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "no persistence failure".to_owned()),
            },
        );

        let fatal_ok = snapshot.fatal_error.is_none();
        checks.insert(
            "fatal".to_owned(),
            ReadinessCheck {
                ok: fatal_ok,
                detail: snapshot
                    .fatal_error
                    .clone()
                    .unwrap_or_else(|| "no fatal error".to_owned()),
            },
        );

        let ready = checks.values().all(|check| check.ok);
        ReadinessReport {
            status: if ready { "ready" } else { "not_ready" }.to_owned(),
            ready,
            uptime_seconds: (now.saturating_sub(self.started_at_us)) as f64 / 1_000_000.0,
            checks,
            coverage: snapshot.coverage,
            catalog: snapshot.catalog,
            persistence: snapshot.persistence,
            writer_healthy: snapshot.writer_healthy,
            fatal_error: snapshot.fatal_error,
        }
    }

    /// Liveness is intentionally narrower than readiness: the process can
    /// answer HTTP and make progress even when coverage is degraded.
    #[must_use]
    pub fn live_payload(&self) -> String {
        format!(
            "{{\"status\":\"alive\",\"uptime_seconds\":{:.3}}}\n",
            self.uptime_seconds()
        )
    }

    /// HTTP status and body for `/readyz`.
    #[must_use]
    pub fn readiness_payload(&self) -> (bool, String) {
        let report = self.readiness();
        let mut body = serde_json::to_string(&report)
            .unwrap_or_else(|_| "{\"status\":\"not_ready\",\"ready\":false}".to_owned());
        body.push('\n');
        (report.ready, body)
    }

    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let readiness = self.readiness();
        let now = now_us().unwrap_or(snapshot.updated_at_us);
        let catalog_age_seconds = snapshot
            .catalog
            .last_discovery_finished_at_us
            .or(snapshot.catalog.generated_at_us)
            .map_or(-1.0, |time| {
                now.saturating_sub(time).max(0) as f64 / 1_000_000.0
            });
        let discovery_age_seconds = snapshot
            .catalog
            .last_discovery_finished_at_us
            .map_or(-1.0, |time| {
                now.saturating_sub(time).max(0) as f64 / 1_000_000.0
            });
        let anchored_fraction = if snapshot.coverage.expected_symbols == 0 {
            0.0
        } else {
            snapshot.coverage.anchored_symbols as f64 / snapshot.coverage.expected_symbols as f64
        };
        let mut out = String::with_capacity(4096);
        let core = &snapshot.core;
        macro_rules! metric {
            ($type:literal, $name:literal, $help:literal, $value:expr) => {
                out.push_str(concat!("# HELP ", $name, " ", $help, "\n"));
                out.push_str(concat!("# TYPE ", $name, " ", $type, "\n"));
                out.push_str($name);
                out.push(' ');
                out.push_str(&$value.to_string());
                out.push('\n');
            };
        }
        macro_rules! gauge {
            ($name:literal, $help:literal, $value:expr) => {
                metric!("gauge", $name, $help, $value)
            };
        }
        macro_rules! counter {
            ($name:literal, $help:literal, $value:expr) => {
                metric!("counter", $name, $help, $value)
            };
        }
        gauge!(
            "collector_symbols_active",
            "Symbols currently subscribed",
            core.current_symbols
        );
        gauge!("collector_shards", "Configured shards", core.shard_count);
        counter!(
            "collector_exchange_snapshots_total",
            "Snapshots received from exchanges",
            core.exchange_snapshots
        );
        counter!(
            "collector_periodic_snapshots_total",
            "Locally generated periodic snapshots",
            core.periodic_snapshots
        );
        counter!(
            "collector_failover_snapshots_total",
            "Snapshots emitted on source switch",
            core.failover_snapshots
        );
        counter!(
            "collector_final_snapshots_total",
            "Snapshots emitted during shutdown",
            core.final_snapshots
        );
        counter!(
            "collector_empty_snapshots_total",
            "Empty snapshots emitted on removal",
            core.empty_snapshots
        );
        counter!(
            "collector_deltas_emitted_total",
            "Absolute price-level updates emitted",
            core.deltas_emitted
        );
        counter!(
            "collector_deltas_rejected_total",
            "Rejected inconsistent updates",
            core.deltas_rejected
        );
        counter!(
            "collector_sequence_resets_total",
            "Source resets caused by sequence/stream faults",
            core.sequence_resets
        );
        counter!(
            "collector_clock_resets_total",
            "Exchange clock regressions treated as maintenance epochs",
            core.clock_resets
        );
        counter!(
            "collector_source_switches_total",
            "Active-source failovers",
            core.source_switches
        );
        counter!(
            "collector_stale_probes_total",
            "Quiet-symbol probes",
            core.stale_probes
        );
        counter!(
            "collector_stale_failovers_total",
            "Failovers caused by failed probes",
            core.stale_failovers
        );
        counter!(
            "collector_crossed_snapshots_skipped_total",
            "Crossed snapshots skipped",
            core.crossed_snapshots_skipped
        );
        counter!(
            "collector_discovery_rounds_total",
            "Market discovery rounds",
            core.discovery_rounds
        );
        counter!(
            "collector_writer_messages_total",
            "SBE messages written",
            snapshot.writer.messages
        );
        counter!(
            "collector_writer_bytes_total",
            "Bytes written including padding",
            snapshot.writer.bytes
        );
        counter!(
            "collector_writer_errors_total",
            "Writer errors",
            snapshot.writer.errors
        );
        gauge!(
            "collector_writer_queue_depth",
            "Messages waiting for the writer",
            snapshot.writer.queue_depth
        );
        gauge!(
            "collector_sources_connected",
            "Connected websocket sources",
            snapshot.sources.iter().filter(|s| s.connected).count()
        );
        gauge!(
            "collector_sources_configured",
            "Configured websocket sources",
            snapshot.sources.len()
        );
        gauge!(
            "collector_symbols_expected",
            "Symbols in the accepted catalog",
            snapshot.coverage.expected_symbols
        );
        gauge!(
            "collector_symbols_anchored",
            "Symbols with an exchange-anchored active book",
            snapshot.coverage.anchored_symbols
        );
        gauge!(
            "collector_symbols_stale",
            "Anchored symbols outside the freshness window",
            snapshot.coverage.stale_symbols
        );
        gauge!(
            "collector_anchor_fraction",
            "Anchored symbols divided by expected symbols",
            anchored_fraction
        );
        gauge!(
            "collector_subscriptions_expected",
            "Subscription entries the configured sources should hold",
            snapshot.coverage.expected_subscriptions
        );
        gauge!(
            "collector_subscriptions_sent",
            "Subscription entries reported as sent by connected sources",
            snapshot.coverage.subscribed_symbols
        );
        gauge!(
            "collector_shards_with_source",
            "Shards with at least one connected source",
            snapshot.coverage.shards_with_connected_source
        );
        gauge!(
            "collector_ready",
            "One when every readiness invariant passes",
            u8::from(readiness.ready)
        );
        gauge!(
            "collector_writer_healthy",
            "One when the writer has no fatal error",
            u8::from(snapshot.writer_healthy)
        );
        gauge!(
            "collector_writer_segments",
            "Finalized rotation segments",
            snapshot.writer.segments
        );
        gauge!(
            "collector_persistence_pending",
            "One while a newer metadata or catalog snapshot is queued",
            u8::from(snapshot.persistence.pending)
        );
        gauge!(
            "collector_persistence_last_success_timestamp_seconds",
            "Unix time of the last successful persistence write",
            snapshot
                .persistence
                .last_success_us
                .map_or(0.0, |time| time as f64 / 1_000_000.0)
        );
        gauge!(
            "collector_catalog_age_seconds",
            "Age of the last accepted catalog round; -1 when none",
            catalog_age_seconds
        );
        gauge!(
            "collector_discovery_age_seconds",
            "Age of the last complete discovery round; -1 when none",
            discovery_age_seconds
        );
        gauge!(
            "collector_discovery_last_duration_ms",
            "Duration of the last complete discovery round",
            snapshot
                .discovery
                .as_ref()
                .map_or(0, |round| round.duration_ms)
        );
        gauge!(
            "collector_discovery_last_pages",
            "Pages read by the last complete discovery round",
            snapshot.discovery.as_ref().map_or(0, |round| round.pages)
        );
        counter!(
            "collector_discovery_pages_total",
            "Discovery pages read",
            snapshot.discovery_totals.pages
        );
        counter!(
            "collector_discovery_selected_markets_total",
            "Venue market records selected by discovery",
            snapshot.discovery_totals.selected_markets
        );
        counter!(
            "collector_discovery_outcome_symbols_total",
            "Outcome symbols produced by discovery",
            snapshot.discovery_totals.outcome_symbols
        );
        counter!(
            "collector_discovery_malformed_records_total",
            "Discovery records rejected as malformed",
            snapshot.discovery_totals.malformed_records
        );
        counter!(
            "collector_discovery_retries_total",
            "Discovery page retries",
            snapshot.discovery_totals.retries
        );
        counter!(
            "collector_discovery_failures_total",
            "Discovery rounds that returned an error",
            snapshot.discovery_failures
        );

        for source in &snapshot.sources {
            out.push_str(&format!(
                "collector_source_connected{{source=\"{}\"}} {}\n",
                source.source,
                u8::from(source.connected)
            ));
            out.push_str(&format!(
                "collector_source_reconnects{{source=\"{}\"}} {}\n",
                source.source, source.reconnects
            ));
            out.push_str(&format!(
                "collector_source_messages{{source=\"{}\"}} {}\n",
                source.source, source.messages
            ));
            out.push_str(&format!(
                "collector_source_decode_errors{{source=\"{}\"}} {}\n",
                source.source, source.decode_errors
            ));
            out.push_str(&format!(
                "collector_source_protocol_errors{{source=\"{}\"}} {}\n",
                source.source, source.protocol_errors
            ));
            if let Some(latency) = source.latency_ewma_us {
                out.push_str(&format!(
                    "collector_source_latency_ms{{source=\"{}\"}} {:.3}\n",
                    source.source,
                    latency as f64 / 1000.0
                ));
            }
        }
        out
    }

    #[must_use]
    pub fn uptime_seconds(&self) -> f64 {
        let now = now_us().unwrap_or(self.started_at_us);
        (now.saturating_sub(self.started_at_us)) as f64 / 1_000_000.0
    }

    /// JSON readiness payload served by `/healthz`.
    ///
    /// `status` is `starting` before any source has reported, `ok` when every
    /// configured source is connected, and `degraded` when at least one source
    /// is down. The endpoint always answers 200 OK: the process is alive
    /// whenever it can answer, and the payload states whether it is also
    /// fully serving.
    #[must_use]
    pub fn health_payload(&self) -> String {
        let snapshot = self.snapshot();
        let total = snapshot.sources.len();
        let connected = snapshot
            .sources
            .iter()
            .filter(|source| source.connected)
            .count();
        let status = if total == 0 {
            "starting"
        } else if connected == total {
            "ok"
        } else {
            "degraded"
        };
        let ready = self.readiness().ready;
        format!(
            "{{\"status\":\"{status}\",\"ready\":{ready},\"uptime_seconds\":{:.3},\
             \"sources_connected\":{connected},\"sources_total\":{total}}}\n",
            self.uptime_seconds()
        )
    }
}

/// Serve on an already-bound listener. The coordinator binds during startup so
/// a port conflict fails the run instead of surfacing in a detached task.
pub async fn run_server_on(
    listener: TcpListener,
    registry: MetricsRegistry,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> anyhow::Result<()> {
    const MAX_METRICS_CLIENTS: usize = 128;
    debug!(address = %listener.local_addr()?, "metrics server listening");
    let client_slots = Arc::new(Semaphore::new(MAX_METRICS_CLIENTS));
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accepted = listener.accept() => {
                let (mut socket, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        warn!(%error, "metrics accept failed; retrying after backoff");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let Ok(permit) = Arc::clone(&client_slots).try_acquire_owned() else {
                    warn!(%peer, "metrics connection limit reached");
                    continue;
                };
                let registry = registry.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut request = [0u8; 2048];
                    let _ = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut request)).await;
                    let request = String::from_utf8_lossy(&request);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let (status, content_type, body) = match path {
                        "/metrics" => (
                            "200 OK",
                            "text/plain; version=0.0.4; charset=utf-8",
                            registry.render_prometheus(),
                        ),
                        "/healthz" => (
                            "200 OK",
                            "application/json",
                            registry.health_payload(),
                        ),
                        "/livez" => (
                            "200 OK",
                            "application/json",
                            registry.live_payload(),
                        ),
                        "/readyz" => {
                            let (ready, body) = registry.readiness_payload();
                            (
                                if ready {
                                    "200 OK"
                                } else {
                                    "503 Service Unavailable"
                                },
                                "application/json",
                                body,
                            )
                        }
                        _ => ("404 Not Found", "text/plain", "not found\n".to_owned()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    if socket.write_all(response.as_bytes()).await.is_err() {
                        warn!(%peer, "metrics client disconnected");
                    }
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy_status(now: i64) -> RuntimeStatus {
        RuntimeStatus {
            coverage: CoverageStats {
                expected_symbols: 10,
                anchored_symbols: 10,
                stale_symbols: 0,
                unseeded_symbols: 0,
                live_symbols: 10,
                stale_symbols_detail: 0,
                resyncing_symbols: 0,
                terminal_symbols: 0,
                expected_subscriptions: 20,
                subscribed_symbols: 20,
                shards_with_connected_source: 2,
                shards: 2,
                sources_connected: 2,
                sources_total: 2,
                oldest_anchor_us: Some(now),
            },
            catalog: CatalogHealth {
                mode: "warm".to_owned(),
                source: "warm".to_owned(),
                generated_at_us: Some(now),
                last_discovery_finished_at_us: Some(now),
                last_discovery_error: None,
            },
            discovery: Some(DiscoveryStats {
                pages: 2,
                selected_markets: 5,
                outcome_symbols: 10,
                duration_ms: 25,
                finished_at_us: now,
                ..DiscoveryStats::default()
            }),
            discovery_failures: 0,
            persistence: PersistenceStatus {
                last_success_us: Some(now),
                ..PersistenceStatus::default()
            },
            writer_healthy: true,
            fatal_error: None,
        }
    }

    fn registry_with(sources: &[(u16, u16, bool)], status: RuntimeStatus) -> MetricsRegistry {
        let registry = MetricsRegistry::new("polymarket", ReadinessPolicy::default());
        let mut statuses = BTreeMap::new();
        for (shard, replica, connected) in sources {
            let source = SourceId::new(*shard, *replica);
            statuses.insert(
                source,
                SourceStatus {
                    source,
                    connected: *connected,
                    ..SourceStatus::new(source)
                },
            );
        }
        registry.update(
            CollectorStats::default(),
            WriterStats::default(),
            &statuses,
            status,
        );
        registry
    }

    fn healthy_registry(sources: &[(u16, u16, bool)]) -> MetricsRegistry {
        let now = now_us().unwrap_or(0);
        registry_with(sources, healthy_status(now))
    }

    fn readiness_with(mutate: impl FnOnce(&mut RuntimeStatus)) -> ReadinessReport {
        let now = now_us().unwrap_or(0);
        let mut status = healthy_status(now);
        mutate(&mut status);
        registry_with(&[(0, 0, true), (1, 0, true)], status).readiness()
    }

    #[test]
    fn health_status_reflects_source_connectivity() {
        let starting = registry_with(&[], healthy_status(now_us().unwrap_or(0))).health_payload();
        assert!(starting.contains("\"status\":\"starting\""));
        assert!(starting.contains("\"sources_total\":0"));

        let healthy = healthy_registry(&[(0, 0, true), (0, 1, true)]).health_payload();
        assert!(healthy.contains("\"status\":\"ok\""));
        assert!(healthy.contains("\"sources_connected\":2"));
        assert!(healthy.contains("\"sources_total\":2"));

        // One shard with both replicas down must not report ok.
        let degraded = registry_with(
            &[(0, 0, true), (1, 0, false), (1, 1, false)],
            healthy_status(now_us().unwrap_or(0)),
        )
        .health_payload();
        assert!(degraded.contains("\"status\":\"degraded\""));
        assert!(degraded.contains("\"sources_connected\":1"));
        assert!(degraded.contains("\"sources_total\":3"));

        // Every source failing must also report degraded, not ok.
        let down = registry_with(
            &[(0, 0, false), (0, 1, false)],
            healthy_status(now_us().unwrap_or(0)),
        )
        .health_payload();
        assert!(down.contains("\"status\":\"degraded\""));
        assert!(down.contains("\"sources_connected\":0"));
        assert!(down.contains("\"sources_total\":2"));
    }

    #[test]
    fn readiness_passes_only_when_every_invariant_holds() {
        let ready = readiness_with(|_| {});
        assert!(ready.ready, "unexpected checks: {:?}", ready.checks);
        assert_eq!(ready.status, "ready");

        type Mutator = Box<dyn FnOnce(&mut RuntimeStatus)>;
        let cases: Vec<(&str, Mutator)> = vec![
            (
                "sources",
                Box::new(|status| status.coverage.shards_with_connected_source = 1),
            ),
            (
                "anchored",
                Box::new(|status| status.coverage.anchored_symbols = 9),
            ),
            (
                "freshness",
                Box::new(|status| status.coverage.stale_symbols = 1),
            ),
            (
                "catalog",
                Box::new(|status| {
                    let old = now_us().unwrap_or(0) - 2 * 3600 * 1_000_000;
                    status.catalog.generated_at_us = Some(old);
                    status.catalog.last_discovery_finished_at_us = Some(old);
                }),
            ),
            ("writer", Box::new(|status| status.writer_healthy = false)),
            (
                "persistence",
                Box::new(|status| {
                    status.persistence.last_error = Some("injected failure".to_owned())
                }),
            ),
            (
                "fatal",
                Box::new(|status| status.fatal_error = Some("injected fatal".to_owned())),
            ),
        ];
        for (name, mutate) in cases {
            let report = readiness_with(mutate);
            assert!(!report.ready, "{name} did not fail readiness");
            assert!(
                !report.checks[name].ok,
                "{name} check stayed ok: {:?}",
                report.checks[name]
            );
        }
    }

    #[test]
    fn readiness_rejects_a_missing_catalog_round() {
        let report = readiness_with(|status| {
            status.catalog.generated_at_us = None;
            status.catalog.last_discovery_finished_at_us = None;
        });
        assert!(!report.ready);
        assert!(!report.checks["catalog"].ok);
    }

    #[test]
    fn prometheus_types_split_gauges_and_counters() {
        let registry = healthy_registry(&[(0, 0, true)]);
        let text = registry.render_prometheus();
        assert!(text.contains("# TYPE collector_symbols_active gauge"));
        assert!(text.contains("# TYPE collector_sources_connected gauge"));
        assert!(text.contains("# TYPE collector_writer_queue_depth gauge"));
        assert!(text.contains("# TYPE collector_deltas_emitted_total counter"));
        assert!(text.contains("# TYPE collector_writer_errors_total counter"));
        assert!(text.contains("# TYPE collector_discovery_rounds_total counter"));
        assert!(text.contains("# TYPE collector_symbols_anchored gauge"));
        assert!(text.contains("# TYPE collector_ready gauge"));
        assert!(text.contains("# TYPE collector_discovery_pages_total counter"));
        assert!(text.contains("collector_discovery_pages_total 2"));
    }

    async fn request(address: std::net::SocketAddr, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    async fn serve(
        registry: MetricsRegistry,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::broadcast::Sender<()>,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, receiver) = tokio::sync::broadcast::channel(1);
        let handle = tokio::spawn(run_server_on(listener, registry, receiver));
        (address, shutdown, handle)
    }

    #[tokio::test]
    async fn livez_is_always_200_and_readyz_reflects_coverage() {
        let (address, shutdown, handle) =
            serve(healthy_registry(&[(0, 0, true), (1, 0, true)])).await;
        let live = request(address, "/livez").await;
        assert!(live.starts_with("HTTP/1.1 200"), "unexpected: {live}");
        let ready = request(address, "/readyz").await;
        assert!(ready.starts_with("HTTP/1.1 200"), "unexpected: {ready}");
        assert!(ready.contains("\"status\":\"ready\""));
        let _ = shutdown.send(());
        let _ = handle.await;

        let now = now_us().unwrap_or(0);
        let mut unhealthy = healthy_status(now);
        unhealthy.coverage.stale_symbols = 1;
        let (address, shutdown, handle) =
            serve(registry_with(&[(0, 0, true), (1, 0, true)], unhealthy)).await;
        let live = request(address, "/livez").await;
        assert!(live.starts_with("HTTP/1.1 200"), "unexpected: {live}");
        let ready = request(address, "/readyz").await;
        assert!(ready.starts_with("HTTP/1.1 503"), "unexpected: {ready}");
        assert!(ready.contains("\"status\":\"not_ready\""));
        assert!(ready.contains("\"freshness\":{\"ok\":false"));
        let _ = shutdown.send(());
        let _ = handle.await;
    }
}

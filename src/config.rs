//! Configuration loading, CLI overrides, and validation.
//!
//! Precedence is: built-in defaults, then an optional TOML file, then
//! environment variables, then explicit CLI flags.

use std::{
    net::{IpAddr, SocketAddr},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeKind {
    #[default]
    Kalshi,
    Polymarket,
}

impl std::fmt::Display for ExchangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kalshi => f.write_str("kalshi"),
            Self::Polymarket => f.write_str("polymarket"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum KalshiEnvironment {
    /// Live exchange. The collector is read-only, and the default targets
    /// live markets; production credentials do not authenticate against
    /// the demo host.
    #[default]
    Production,
    /// Simulated exchange with demo credentials.
    Demo,
}

impl KalshiEnvironment {
    #[must_use]
    pub fn rest_base(self) -> &'static str {
        match self {
            Self::Production => "https://external-api.kalshi.com/trade-api/v2",
            Self::Demo => "https://demo-api.kalshi.co/trade-api/v2",
        }
    }

    #[must_use]
    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Production => "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
            Self::Demo => "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KalshiSettings {
    pub environment: KalshiEnvironment,
    pub key_id: String,
    pub private_key_path: Option<PathBuf>,
    /// Number of market tickers per orderbook subscription.
    pub batch_size: usize,
    /// REST page size for market discovery (Kalshi maximum: 1000).
    pub discovery_page_size: usize,
    pub request_timeout_secs: u64,
    /// Delay between discovery pages, to stay well inside rate limits.
    pub discovery_page_delay_ms: u64,
}

impl Default for KalshiSettings {
    fn default() -> Self {
        Self {
            environment: KalshiEnvironment::Production,
            key_id: String::new(),
            private_key_path: None,
            batch_size: 100,
            discovery_page_size: 1000,
            request_timeout_secs: 20,
            discovery_page_delay_ms: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketSettings {
    pub gamma_url: String,
    pub ws_url: String,
    /// Number of asset ids per dynamic subscribe/unsubscribe frame. The
    /// initial snapshot subscription is always sent as one frame.
    pub batch_size: usize,
    /// Gamma keyset page size. The endpoint accepts at most 100 records.
    pub discovery_page_size: usize,
    pub request_timeout_secs: u64,
    /// Delay between discovery pages.
    pub discovery_page_delay_ms: u64,
    /// Require an explicit `acceptingOrders == true`.
    ///
    /// A missing field is unconfirmed and is excluded while this is enabled.
    /// Disable it to include records whose accepting state is unknown, while
    /// retaining the active/closed/archived/order-book filters.
    pub require_accepting_orders: bool,
}

impl Default for PolymarketSettings {
    fn default() -> Self {
        Self {
            gamma_url: "https://gamma-api.polymarket.com".to_owned(),
            ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            batch_size: 500,
            discovery_page_size: 100,
            request_timeout_secs: 20,
            discovery_page_delay_ms: 50,
            require_accepting_orders: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorSettings {
    /// Number of shards. Set 0 to derive a shard count from the initial symbol
    /// count and `max_symbols_per_socket`.
    pub shards: usize,
    /// Redundant websocket connections per shard.
    pub replicas: usize,
    /// Soft capacity used to size automatic shards.
    pub max_symbols_per_socket: usize,
    /// Upper bound when deriving shards automatically.
    pub max_shards: usize,
    /// Full local snapshot cadence.
    pub snapshot_interval_secs: u64,
    /// Market discovery reconciliation cadence.
    pub discovery_interval_secs: u64,
    /// Refresh a source connection after this long, with jitter.
    pub reconnect_after_secs: u64,
    /// Reconnect if no frame (data or heartbeat) arrives for this long.
    pub connection_idle_timeout_secs: u64,
    /// Probe a quiet symbol after this long without an event.
    pub stale_probe_after_secs: u64,
    /// Minimum spacing between probes for one symbol.
    pub probe_interval_secs: u64,
    /// Failed probes before failing over/reconnecting.
    pub max_probes: u32,
    /// Emit final snapshots for live books during graceful shutdown.
    pub final_snapshots: bool,
    /// Bounded writer queue depth, in messages.
    pub writer_queue: usize,
    /// Flush cadence for partial SBE chunks.
    pub flush_interval_secs: u64,
    /// Cap on selected venue market records (0 means no cap).
    ///
    /// One Polymarket Gamma record can contribute several outcome tokens; the
    /// cap is applied between records, never inside one record's outcome set.
    /// Kalshi publishes one symbol per market, so the same contract applies.
    pub max_markets: usize,
    /// Optional run duration; 0 means run until SIGINT/SIGTERM.
    pub duration_secs: u64,
    /// Maximum accepted outbound writer backlog before the service fails.
    pub writer_send_timeout_secs: u64,
    /// Hard ceiling on `shards * replicas` websocket connections.
    pub max_total_connections: usize,
    /// Hard ceiling on the estimated bytes of one initial subscription frame.
    pub max_initial_subscription_bytes: usize,
    /// Hard ceiling on estimated resident memory for book state, in MiB.
    pub max_estimated_book_memory_mb: u64,
    /// Hard ceiling on estimated snapshot output, in bytes per second.
    pub max_snapshot_bytes_per_sec: u64,
    /// Maximum concurrent websocket connection attempts.
    pub connect_concurrency: usize,
    /// Last-known-good catalog checkpoint. `None` disables persistence.
    pub catalog_path: Option<PathBuf>,
    /// Catalog bootstrap policy.
    pub catalog_mode: CatalogMode,
    /// Maximum accepted catalog age for `warm` and `require`.
    pub catalog_max_age_secs: u64,
    /// Accept a complete discovery round that retires every symbol.
    pub allow_empty_catalog: bool,
    /// Minimum anchored-symbol fraction required by `/readyz`.
    pub readiness_min_anchored_fraction: f64,
    /// Maximum catalog age required by `/readyz`.
    pub readiness_max_catalog_age_secs: u64,
    /// Maximum time since a book's last valid update before `/readyz` fails.
    pub readiness_max_stale_secs: u64,
}

/// How a persisted catalog is used during startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum CatalogMode {
    /// Discover before starting workers; never read a checkpoint.
    #[default]
    Off,
    /// Start from a fresh checkpoint when available, then refresh in the
    /// background. A missing, stale, or empty checkpoint falls back to
    /// discovery.
    Warm,
    /// Require a valid, fresh checkpoint; fail startup otherwise.
    Require,
    /// Require any valid, non-empty checkpoint regardless of age. This is the
    /// explicit degraded boot mode for a total discovery outage.
    Stale,
}

impl Default for CollectorSettings {
    fn default() -> Self {
        Self {
            shards: 4,
            replicas: 2,
            max_symbols_per_socket: 5000,
            max_shards: 64,
            snapshot_interval_secs: 60,
            discovery_interval_secs: 30,
            reconnect_after_secs: 1800,
            connection_idle_timeout_secs: 45,
            stale_probe_after_secs: 120,
            probe_interval_secs: 30,
            max_probes: 3,
            final_snapshots: true,
            writer_queue: 16_384,
            // Output-visibility contract: every interval that received any
            // message ends with a flush, so a downstream reader sees an
            // accepted delta within ~1 second instead of waiting for a full
            // chunk or shutdown. Flushes write only the encoded bytes;
            // padding is written on chunk rotation and at close.
            flush_interval_secs: 1,
            max_markets: 0,
            duration_secs: 0,
            writer_send_timeout_secs: 30,
            max_total_connections: 64,
            max_initial_subscription_bytes: 1 << 20,
            max_estimated_book_memory_mb: 1024,
            max_snapshot_bytes_per_sec: 16 << 20,
            connect_concurrency: 8,
            catalog_path: None,
            catalog_mode: CatalogMode::Off,
            catalog_max_age_secs: 3600,
            allow_empty_catalog: false,
            readiness_min_anchored_fraction: 0.99,
            readiness_max_catalog_age_secs: 3600,
            readiness_max_stale_secs: 120,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OutputSettings {
    /// File path, or `-` for binary stdout.
    pub path: String,
    /// Aligned chunk size. Must be a power of two.
    pub chunk_size: usize,
    /// Optional path for a JSON market-metadata sidecar.
    pub metadata_path: Option<PathBuf>,
    /// Optional path for a JSON run report.
    pub report_path: Option<PathBuf>,
    /// Ask the OS to persist the output file on shutdown.
    pub sync_on_close: bool,
    /// Truncate and replace existing capture, sidecar, and segment files.
    ///
    /// The default is false: an existing artifact is an error, so a restart
    /// or a mistyped path cannot destroy a previous run's data.
    pub overwrite: bool,
    /// Rotate after this many bytes (0 disables size rotation).
    pub rotate_bytes: u64,
    /// Rotate after this many seconds (0 disables time rotation).
    pub rotate_secs: u64,
    /// Stop before free space on the output filesystem drops below this many
    /// bytes (0 disables the watermark).
    pub min_free_bytes: u64,
    /// Periodically fsync the active segment (0 syncs only on rotation and
    /// close).
    pub fsync_interval_secs: u64,
}

impl Default for OutputSettings {
    fn default() -> Self {
        Self {
            path: "-".to_owned(),
            // The template's AlignedSbeReader::open() hardcodes 64 MiB. Keep
            // the default output chunk identical so a graded capture is
            // readable by the unmodified helper.
            chunk_size: 64 << 20,
            metadata_path: None,
            report_path: None,
            sync_on_close: true,
            overwrite: false,
            rotate_bytes: 0,
            rotate_secs: 0,
            min_free_bytes: 512 << 20,
            fsync_interval_secs: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsSettings {
    /// Optional `host:port` for Prometheus text and `/healthz`.
    pub listen: Option<SocketAddr>,
}

/// Transport controls shared by every websocket adapter.
///
/// These options exist for latency experiments as well as production tuning.
/// The defaults are conservative: Nagle's algorithm is disabled because
/// order-book frames are small and latency-sensitive, and a single connection
/// attempt is made.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkSettings {
    /// Set `TCP_NODELAY` on every accepted websocket TCP socket.
    pub tcp_nodelay: bool,
    /// Optional local IP to bind before connecting. The address family must
    /// match the resolved remote address.
    pub source_ip: Option<IpAddr>,
    /// Optional fixed local source port, used as a base: connection
    /// `shard * 256 + replica` binds `base + ordinal`, so parallel
    /// connections never collide. Cannot be combined with `connect_race > 1`,
    /// because racing attempts for one connection would share one port.
    pub source_port: Option<u16>,
    /// Number of simultaneous TCP/TLS/websocket handshakes. The first
    /// successful connection is retained and the rest are closed. One means
    /// no racing.
    pub connect_race: usize,
    /// Deadline for one full connection attempt: DNS resolution, TCP connect,
    /// TLS negotiation, and the websocket upgrade, including every raced
    /// candidate. A stalled handshake fails like any other transient error
    /// instead of wedging the source.
    pub connect_timeout_secs: u64,
}

impl Default for NetworkSettings {
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            source_ip: None,
            source_port: None,
            connect_race: 1,
            connect_timeout_secs: 30,
        }
    }
}

impl NetworkSettings {
    /// Convert validated settings into transport options.
    #[must_use]
    pub fn connect_options(&self) -> crate::ws::ConnectOptions {
        crate::ws::ConnectOptions {
            tcp_nodelay: self.tcp_nodelay,
            source_ip: self.source_ip,
            source_port: self.source_port,
            attempts: self.connect_race,
            connect_timeout: std::time::Duration::from_secs(self.connect_timeout_secs.max(1)),
        }
    }
}

/// Flush and sync policy for the raw-frame journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum JournalSyncMode {
    /// Flush and fsync on the configured intervals and at finalization.
    #[default]
    Interval,
    /// Flush and fsync every record.
    Always,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JournalSettings {
    /// Root directory for raw-frame runs. `None` disables the journal.
    pub directory: Option<PathBuf>,
    /// Records buffered between session workers and the journal writer.
    pub queue_capacity: usize,
    /// Maximum wait for queue space before the run fails.
    pub enqueue_timeout_ms: u64,
    /// Maximum time records may remain in the process buffer.
    pub flush_interval_ms: u64,
    /// Maximum time between storage syncs in interval mode.
    pub sync_interval_ms: u64,
    /// Rotate after this many record bytes (0 disables size rotation).
    pub segment_max_bytes: u64,
    /// Rotate after this many seconds (0 disables time rotation).
    pub segment_max_secs: u64,
    pub sync_mode: JournalSyncMode,
}

impl Default for JournalSettings {
    fn default() -> Self {
        Self {
            directory: None,
            queue_capacity: 4096,
            enqueue_timeout_ms: 250,
            flush_interval_ms: 1000,
            sync_interval_ms: 5000,
            segment_max_bytes: 256 << 20,
            segment_max_secs: 300,
            sync_mode: JournalSyncMode::Interval,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub exchange: ExchangeKind,
    pub kalshi: KalshiSettings,
    pub polymarket: PolymarketSettings,
    pub collector: CollectorSettings,
    pub network: NetworkSettings,
    pub output: OutputSettings,
    pub metrics: MetricsSettings,
    pub journal: JournalSettings,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub exchange: ExchangeKind,
    pub kalshi: KalshiSettings,
    pub polymarket: PolymarketSettings,
    pub collector: CollectorSettings,
    pub network: NetworkSettings,
    pub output: OutputSettings,
    pub metrics: MetricsSettings,
    pub journal: JournalSettings,
}

impl Settings {
    /// Built-in defaults for one explicit venue, without reading configuration
    /// files or environment variables. Tests use this so their behavior does
    /// not depend on a developer's shell.
    #[must_use]
    pub fn for_exchange(exchange: ExchangeKind) -> Self {
        let file = FileConfig {
            exchange,
            ..FileConfig::default()
        };
        Self {
            exchange: file.exchange,
            kalshi: file.kalshi,
            polymarket: file.polymarket,
            collector: file.collector,
            network: file.network,
            output: file.output,
            metrics: file.metrics,
            journal: file.journal,
        }
    }

    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let file = match path {
            Some(path) => {
                let text = std::fs::read_to_string(path)
                    .with_context(|| format!("reading config `{}`", path.display()))?;
                toml::from_str::<FileConfig>(&text)
                    .with_context(|| format!("parsing config `{}`", path.display()))?
            }
            None => FileConfig::default(),
        };
        let mut settings = Self {
            exchange: file.exchange,
            kalshi: file.kalshi,
            polymarket: file.polymarket,
            collector: file.collector,
            network: file.network,
            output: file.output,
            metrics: file.metrics,
            journal: file.journal,
        };
        settings.apply_environment()?;
        settings.validate()?;
        Ok(settings)
    }

    fn apply_environment(&mut self) -> anyhow::Result<()> {
        if let Ok(exchange) = std::env::var("COLLECTOR_EXCHANGE") {
            self.exchange = match exchange.to_ascii_lowercase().as_str() {
                "kalshi" => ExchangeKind::Kalshi,
                "polymarket" => ExchangeKind::Polymarket,
                other => bail!("unknown COLLECTOR_EXCHANGE `{other}`"),
            };
        }
        if let Ok(value) = std::env::var("KALSHI_ENVIRONMENT") {
            self.kalshi.environment = match value.to_ascii_lowercase().as_str() {
                "production" | "prod" => KalshiEnvironment::Production,
                "demo" => KalshiEnvironment::Demo,
                other => bail!("unknown KALSHI_ENVIRONMENT `{other}`"),
            };
        }
        if let Ok(value) = std::env::var("KALSHI_API_KEY_ID") {
            self.kalshi.key_id = value;
        }
        if let Ok(value) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
            self.kalshi.private_key_path = Some(PathBuf::from(value));
        }
        if let Ok(value) = std::env::var("COLLECTOR_OUTPUT") {
            self.output.path = value;
        }
        if let Ok(value) = std::env::var("COLLECTOR_JOURNAL_DIR") {
            self.journal.directory = Some(PathBuf::from(value));
        }
        if let Ok(value) = std::env::var("COLLECTOR_METRICS_LISTEN") {
            self.metrics.listen = Some(
                value
                    .parse()
                    .with_context(|| format!("invalid COLLECTOR_METRICS_LISTEN `{value}`"))?,
            );
        }
        Ok(())
    }

    /// SHA-256 of the capture-affecting configuration.
    ///
    /// The digest covers the exchange, collector, network, output, and journal
    /// sections. Credentials are excluded so the value can be published in a
    /// journal manifest without exposing secrets.
    pub fn capture_config_sha256(&self) -> anyhow::Result<String> {
        let canonical = serde_json::json!({
            "exchange": self.exchange,
            "collector": self.collector,
            "network": self.network,
            "output": self.output,
            "journal": self.journal,
        });
        let bytes = serde_json::to_vec(&canonical)
            .context("serializing capture configuration for hashing")?;
        Ok(crate::util::sha256_hex(&bytes))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.collector.shards <= 256, "shards must be <= 256");
        anyhow::ensure!(self.collector.replicas >= 1, "replicas must be >= 1");
        anyhow::ensure!(self.collector.replicas <= 4, "replicas must be <= 4");
        anyhow::ensure!(
            self.collector.max_symbols_per_socket > 0,
            "max_symbols_per_socket must be > 0"
        );
        anyhow::ensure!(
            self.collector.max_shards >= self.collector.shards.max(1),
            "max_shards must be >= shards"
        );
        anyhow::ensure!(
            self.collector.snapshot_interval_secs >= 1,
            "snapshot_interval_secs must be >= 1"
        );
        anyhow::ensure!(
            self.collector.discovery_interval_secs >= 1,
            "discovery_interval_secs must be >= 1"
        );
        anyhow::ensure!(
            self.collector.reconnect_after_secs >= 10,
            "reconnect_after_secs must be >= 10"
        );
        anyhow::ensure!(
            self.collector.connection_idle_timeout_secs >= 10,
            "connection_idle_timeout_secs must be >= 10"
        );
        anyhow::ensure!(
            self.collector.stale_probe_after_secs >= 10,
            "stale_probe_after_secs must be >= 10"
        );
        anyhow::ensure!(
            self.collector.probe_interval_secs >= 1,
            "probe_interval_secs must be >= 1"
        );
        anyhow::ensure!(self.collector.max_probes >= 1, "max_probes must be >= 1");
        anyhow::ensure!(
            self.collector.max_total_connections >= self.collector.replicas.max(1),
            "max_total_connections must be >= replicas"
        );
        anyhow::ensure!(
            self.collector.max_total_connections <= 4096,
            "max_total_connections must be <= 4096"
        );
        anyhow::ensure!(
            self.collector.max_initial_subscription_bytes >= 1024,
            "max_initial_subscription_bytes must be >= 1024"
        );
        anyhow::ensure!(
            self.collector.max_estimated_book_memory_mb >= 1,
            "max_estimated_book_memory_mb must be >= 1"
        );
        anyhow::ensure!(
            self.collector.max_snapshot_bytes_per_sec >= 1024,
            "max_snapshot_bytes_per_sec must be >= 1024"
        );
        anyhow::ensure!(
            (1..=256).contains(&self.collector.connect_concurrency),
            "connect_concurrency must be in [1, 256]"
        );
        anyhow::ensure!(
            self.collector.catalog_mode == CatalogMode::Off
                || self
                    .collector
                    .catalog_path
                    .as_deref()
                    .is_some_and(|path| !path.as_os_str().is_empty()),
            "catalog_mode {:?} requires collector.catalog_path",
            self.collector.catalog_mode
        );
        anyhow::ensure!(
            self.collector.catalog_max_age_secs >= 1,
            "catalog_max_age_secs must be >= 1"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.collector.readiness_min_anchored_fraction),
            "readiness_min_anchored_fraction must be in [0, 1]"
        );
        anyhow::ensure!(
            self.collector.readiness_max_catalog_age_secs >= 1,
            "readiness_max_catalog_age_secs must be >= 1"
        );
        anyhow::ensure!(
            self.collector.readiness_max_stale_secs >= 1,
            "readiness_max_stale_secs must be >= 1"
        );
        anyhow::ensure!(
            self.collector.writer_queue >= 128,
            "writer_queue must be >= 128"
        );
        if let Some(directory) = self.journal.directory.as_ref() {
            anyhow::ensure!(
                !directory.as_os_str().is_empty(),
                "journal.directory must not be empty"
            );
            anyhow::ensure!(
                self.journal.queue_capacity >= 1,
                "journal.queue_capacity must be >= 1"
            );
            anyhow::ensure!(
                self.journal.enqueue_timeout_ms >= 1,
                "journal.enqueue_timeout_ms must be >= 1"
            );
            anyhow::ensure!(
                self.journal.flush_interval_ms >= 1,
                "journal.flush_interval_ms must be >= 1"
            );
            anyhow::ensure!(
                self.journal.sync_interval_ms >= 1,
                "journal.sync_interval_ms must be >= 1"
            );
            anyhow::ensure!(
                self.journal.segment_max_bytes == 0 || self.journal.segment_max_bytes >= 4096,
                "journal.segment_max_bytes must be zero or at least 4096"
            );
        }
        anyhow::ensure!(
            self.collector.flush_interval_secs >= 1,
            "flush_interval_secs must be >= 1"
        );
        anyhow::ensure!(
            (1..=8).contains(&self.network.connect_race),
            "network.connect_race must be in [1, 8]"
        );
        anyhow::ensure!(
            self.network.connect_timeout_secs >= 1,
            "network.connect_timeout_secs must be >= 1"
        );
        anyhow::ensure!(
            self.network.connect_timeout_secs <= 600,
            "network.connect_timeout_secs must be <= 600"
        );
        anyhow::ensure!(
            self.network.source_port != Some(0),
            "network.source_port must be non-zero"
        );
        anyhow::ensure!(
            self.network.connect_race == 1 || self.network.source_port.is_none(),
            "network.source_port cannot be combined with connect_race > 1"
        );
        anyhow::ensure!(
            self.output.chunk_size >= 1024 && self.output.chunk_size.is_power_of_two(),
            "output.chunk_size must be a power of two >= 1024"
        );
        anyhow::ensure!(
            self.output.rotate_bytes == 0
                || self.output.rotate_bytes >= self.output.chunk_size as u64,
            "output.rotate_bytes must be 0 or at least one aligned chunk"
        );
        anyhow::ensure!(!self.output.path.is_empty(), "output path is empty");
        validate_output_paths(self)?;
        anyhow::ensure!(
            self.kalshi.batch_size >= 1 && self.kalshi.batch_size <= 1000,
            "kalshi.batch_size must be in [1, 1000]"
        );
        anyhow::ensure!(
            self.kalshi.discovery_page_size >= 1 && self.kalshi.discovery_page_size <= 1000,
            "kalshi.discovery_page_size must be in [1, 1000]"
        );
        anyhow::ensure!(
            self.polymarket.batch_size >= 1,
            "polymarket.batch_size must be >= 1"
        );
        anyhow::ensure!(
            self.polymarket.discovery_page_size >= 1 && self.polymarket.discovery_page_size <= 100,
            "polymarket.discovery_page_size must be in [1, 100]"
        );
        validate_endpoint(
            "polymarket.gamma_url",
            &self.polymarket.gamma_url,
            "https",
            Some("http"),
        )?;
        validate_endpoint(
            "polymarket.ws_url",
            &self.polymarket.ws_url,
            "wss",
            Some("ws"),
        )?;
        validate_endpoint(
            "Kalshi REST base URL",
            self.kalshi.environment.rest_base(),
            "https",
            None,
        )?;
        validate_endpoint(
            "Kalshi websocket URL",
            self.kalshi.environment.ws_url(),
            "wss",
            None,
        )?;
        Ok(())
    }

    #[must_use]
    pub fn discovery_interval(&self) -> Duration {
        Duration::from_secs(self.collector.discovery_interval_secs)
    }
}

/// Validate a network endpoint without assuming a particular hostname.
///
/// Production endpoints must use TLS. Plain HTTP/WS is accepted only for a
/// loopback host, which keeps local integration tests possible without
/// permitting accidental plaintext traffic to a remote exchange or proxy.
fn validate_endpoint(
    name: &str,
    value: &str,
    secure_scheme: &str,
    plain_scheme: Option<&str>,
) -> anyhow::Result<Url> {
    let url = Url::parse(value).with_context(|| format!("invalid {name}"))?;
    anyhow::ensure!(!url.cannot_be_a_base(), "{name} must be a hierarchical URL");
    anyhow::ensure!(
        url.host_str().is_some_and(|host| !host.is_empty()),
        "{name} must include a host"
    );
    anyhow::ensure!(
        url.username().is_empty() && url.password().is_none(),
        "{name} must not embed credentials"
    );
    anyhow::ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "{name} must not contain a query or fragment"
    );
    anyhow::ensure!(url.port() != Some(0), "{name} must not use port 0");
    let scheme = url.scheme();
    let secure = scheme == secure_scheme;
    let plain = plain_scheme.is_some_and(|plain| scheme == plain);
    let loopback = url.host_str().is_some_and(|host| {
        let host = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host);
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    anyhow::ensure!(
        secure || (plain && loopback),
        "{name} must use {secure_scheme}{}",
        plain_scheme.map_or_else(String::new, |plain| format!(
            " (or {plain} only for a loopback host)"
        ))
    );
    Ok(url)
}

/// Validate that the capture and every sidecar resolve to distinct files.
///
/// A string comparison is insufficient: `./capture.sbe`, `capture.sbe`, and an
/// absolute path can all name the same file. Existing paths are canonicalised
/// (resolving symlinks), nonexistent paths are made absolute lexically, and
/// existing hard links are compared by device/inode where the platform exposes
/// them. This prevents a sidecar write from truncating the capture after a run.
fn validate_output_paths(settings: &Settings) -> anyhow::Result<()> {
    let mut paths: Vec<(&'static str, PathBuf)> = Vec::new();
    if settings.output.path != "-" {
        let output = PathBuf::from(&settings.output.path);
        ensure_not_directory("output.path", &output)?;
        paths.push(("output.path", output));
        if settings.output.rotate_bytes > 0 || settings.output.rotate_secs > 0 {
            paths.push((
                "output manifest",
                rotation_manifest_path(&settings.output.path),
            ));
        }
    }
    for (name, path) in [
        ("metadata_path", settings.output.metadata_path.as_deref()),
        ("report_path", settings.output.report_path.as_deref()),
        ("catalog_path", settings.collector.catalog_path.as_deref()),
    ] {
        let Some(path) = path.filter(|path| !path.as_os_str().is_empty()) else {
            continue;
        };
        anyhow::ensure!(
            path.as_os_str() != "-",
            "{name} must be a file path, not stdout"
        );
        ensure_not_directory(name, path)?;
        paths.push((name, path.to_path_buf()));
    }
    for left in 0..paths.len() {
        for right in left + 1..paths.len() {
            let (left_name, left_path) = &paths[left];
            let (right_name, right_path) = &paths[right];
            if paths_alias(left_path, right_path)? {
                bail!("{left_name} and {right_name} refer to the same file");
            }
        }
    }
    Ok(())
}

/// Manifest path for a rotated capture. Segments are named
/// `<output>.<sequence>.sbe`, so the manifest sits beside them without
/// colliding with the unrotated capture path.
#[must_use]
pub fn rotation_manifest_path(output_path: &str) -> PathBuf {
    let path = Path::new(output_path);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("output");
    path.with_file_name(format!("{name}.manifest.json"))
}

/// Reject every pre-existing artifact that this run would replace.
///
/// This is a startup check, not a configuration-syntax check: discovery,
/// replay, and latency commands may legitimately share a config while writing
/// somewhere else. Collection and replay call it before doing any work, so a
/// failed startup leaves the previous capture and sidecars untouched.
pub fn preflight_output(settings: &Settings) -> anyhow::Result<()> {
    if settings.output.overwrite {
        return Ok(());
    }
    let mut artifacts: Vec<(&'static str, PathBuf)> = Vec::new();
    if settings.output.path != "-" {
        artifacts.push(("output.path", PathBuf::from(&settings.output.path)));
        if settings.output.rotate_bytes > 0 || settings.output.rotate_secs > 0 {
            artifacts.push((
                "output manifest",
                rotation_manifest_path(&settings.output.path),
            ));
            for segment in rotation_segments(&settings.output.path)? {
                artifacts.push(("output segment", segment));
            }
        }
    }
    for (name, path) in [
        ("metadata_path", settings.output.metadata_path.as_deref()),
        ("report_path", settings.output.report_path.as_deref()),
    ] {
        if let Some(path) = path.filter(|path| !path.as_os_str().is_empty()) {
            artifacts.push((name, path.to_path_buf()));
        }
    }
    // In warm, require, and stale modes the catalog is an input. Replacing it
    // after a complete discovery round is the intended behavior, not data
    // loss. In cold-only mode an existing checkpoint is an output artifact and
    // follows the same overwrite rule as the capture.
    if settings.collector.catalog_mode == CatalogMode::Off
        && let Some(path) = settings
            .collector
            .catalog_path
            .as_deref()
            .filter(|path| !path.as_os_str().is_empty())
    {
        artifacts.push(("catalog_path", path.to_path_buf()));
    }
    for (name, path) in artifacts {
        anyhow::ensure!(
            !path.exists(),
            "{name} already exists at `{}`; refusing to replace it. \
             Set output.overwrite = true to replace existing artifacts",
            path.display()
        );
    }
    Ok(())
}

/// Existing finalized or partial segments for a rotated capture.
fn rotation_segments(output_path: &str) -> anyhow::Result<Vec<PathBuf>> {
    let path = Path::new(output_path);
    let (Some(parent), Some(name)) = (path.parent(), path.file_name().and_then(|n| n.to_str()))
    else {
        return Ok(Vec::new());
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let prefix = format!("{name}.");
    let entries = std::fs::read_dir(parent)
        .with_context(|| format!("scanning rotation directory `{}`", parent.display()))?;
    Ok(entries
        .flatten()
        .filter_map(|entry| {
            let file_name = entry.file_name();
            let file_name = file_name.to_str()?;
            rotation_artifact_name(&prefix, file_name).then(|| entry.path())
        })
        .collect())
}

fn rotation_artifact_name(prefix: &str, name: &str) -> bool {
    let stem = name.strip_prefix(prefix).and_then(|value| {
        value
            .strip_suffix(".sbe")
            .or_else(|| value.strip_suffix(".sbe.partial"))
    });
    stem.is_some_and(|sequence| {
        sequence.len() == 6 && sequence.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn ensure_not_directory(name: &str, path: &Path) -> anyhow::Result<()> {
    if let Ok(metadata) = std::fs::metadata(path) {
        anyhow::ensure!(
            !metadata.is_dir(),
            "{name} points to a directory: `{}`",
            path.display()
        );
    }
    Ok(())
}

fn paths_alias(left: &Path, right: &Path) -> anyhow::Result<bool> {
    if left == right {
        return Ok(true);
    }
    let left_normal = comparison_path(left)?;
    let right_normal = comparison_path(right)?;
    if left_normal == right_normal {
        return Ok(true);
    }
    Ok(same_existing_file(left, right))
}

/// Canonical form for alias detection. Nonexistent files still compare
/// correctly when their parent directory exists, including through symlinked
/// directories.
fn comparison_path(path: &Path) -> anyhow::Result<PathBuf> {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return Ok(canonical);
    }
    let absolute = std::path::absolute(path)
        .with_context(|| format!("making `{}` absolute", path.display()))?;
    let normalized = normalize_lexically(&absolute);
    let Some(parent) = normalized.parent() else {
        return Ok(normalized);
    };
    let Some(file_name) = normalized.file_name() else {
        return Ok(normalized);
    };
    match std::fs::canonicalize(parent) {
        Ok(parent) => Ok(parent.join(file_name)),
        Err(_) => Ok(normalized),
    }
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

#[cfg(unix)]
fn same_existing_file(left: &Path, right: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    match (std::fs::metadata(left), std::fs::metadata(right)) {
        (Ok(left), Ok(right)) => left.dev() == right.dev() && left.ino() == right.ino(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_existing_file(_left: &Path, _right: &Path) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_fingerprint_tracks_capture_settings_not_credentials() {
        let mut settings = Settings::for_exchange(ExchangeKind::Polymarket);
        let first = settings.capture_config_sha256().expect("fingerprint");
        settings.kalshi.key_id = "not-part-of-the-fingerprint".to_owned();
        assert_eq!(
            first,
            settings.capture_config_sha256().expect("fingerprint")
        );
        settings.journal.queue_capacity = settings.journal.queue_capacity.saturating_add(1);
        assert_ne!(
            first,
            settings.capture_config_sha256().expect("fingerprint")
        );
    }

    #[test]
    fn kalshi_defaults_to_production() {
        assert_eq!(KalshiEnvironment::default(), KalshiEnvironment::Production);
        let settings = KalshiSettings::default();
        assert_eq!(settings.environment, KalshiEnvironment::Production);
        assert!(settings.environment.ws_url().contains("external-api-ws"));
        assert!(!settings.environment.ws_url().contains("demo"));
        assert!(settings.environment.rest_base().contains("external-api"));
    }

    #[test]
    fn rejects_output_and_sidecar_path_collisions() {
        let mut settings = Settings::load(None).unwrap();
        settings.output.path = "capture.sbe".to_owned();
        settings.output.metadata_path = Some(PathBuf::from("capture.sbe"));
        assert!(settings.validate().is_err());

        settings.output.metadata_path = Some(PathBuf::from("metadata.json"));
        settings.output.report_path = Some(PathBuf::from("capture.sbe"));
        assert!(settings.validate().is_err());

        settings.output.report_path = Some(PathBuf::from("metadata.json"));
        assert!(settings.validate().is_err());

        settings.output.report_path = Some(PathBuf::from("-"));
        assert!(settings.validate().is_err());
    }

    #[test]
    fn detects_lexical_output_path_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture.sbe");
        let alias = directory.path().join(".").join("capture.sbe");
        assert!(paths_alias(&output, &alias).unwrap());

        let dotdot = directory.path().join("sub").join("..").join("capture.sbe");
        assert!(paths_alias(&output, &dotdot).unwrap());

        let mut settings = Settings::load(None).unwrap();
        settings.output.path = output.to_string_lossy().into_owned();
        settings.output.metadata_path = Some(alias);
        assert!(settings.validate().is_err());
    }

    #[test]
    fn detects_symlink_and_hardlink_output_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let output = real.join("capture.sbe");
        #[cfg(unix)]
        {
            let link = directory.path().join("link");
            std::os::unix::fs::symlink(&real, &link).unwrap();
            assert!(paths_alias(&output, &link.join("capture.sbe")).unwrap());

            std::fs::write(&output, b"capture").unwrap();
            let hardlink = directory.path().join("metadata.json");
            std::fs::hard_link(&output, &hardlink).unwrap();
            assert!(paths_alias(&output, &hardlink).unwrap());

            let mut settings = Settings::load(None).unwrap();
            settings.output.path = output.to_string_lossy().into_owned();
            settings.output.metadata_path = Some(hardlink);
            assert!(settings.validate().is_err());
        }
    }

    #[test]
    fn preflight_preserves_existing_artifacts_until_overwrite_is_explicit() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture.sbe");
        let metadata = directory.path().join("metadata.json");
        std::fs::write(&output, b"previous capture").unwrap();
        std::fs::write(&metadata, b"previous metadata").unwrap();

        let mut settings = Settings::load(None).unwrap();
        settings.output.path = output.to_string_lossy().into_owned();
        settings.output.metadata_path = Some(metadata.clone());

        let error = preflight_output(&settings).unwrap_err();
        assert!(
            error.to_string().contains("already exists"),
            "unexpected error: {error}"
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"previous capture");
        assert_eq!(std::fs::read(&metadata).unwrap(), b"previous metadata");

        settings.output.overwrite = true;
        preflight_output(&settings).unwrap();
        // The preflight itself is non-destructive even with overwrite; the
        // writer performs the truncation only after startup succeeds.
        assert_eq!(std::fs::read(&output).unwrap(), b"previous capture");
    }

    #[test]
    fn preflight_rejects_existing_rotation_segments_and_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture.sbe");
        let manifest = rotation_manifest_path(output.to_str().unwrap());
        std::fs::write(directory.path().join("capture.sbe.000001.sbe"), b"old").unwrap();
        std::fs::write(&manifest, b"old").unwrap();

        let mut settings = Settings::load(None).unwrap();
        settings.output.path = output.to_string_lossy().into_owned();
        settings.output.rotate_bytes = settings.output.chunk_size as u64;
        let error = preflight_output(&settings).unwrap_err();
        assert!(error.to_string().contains("already exists"));

        settings.output.overwrite = true;
        preflight_output(&settings).unwrap();
    }

    #[test]
    fn rotation_scan_owns_only_exactly_named_segments() {
        assert!(rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.000001.sbe"
        ));
        assert!(rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.000001.sbe.partial"
        ));
        assert!(!rotation_artifact_name(
            "capture.sbe.",
            "capture.sbe.backup.sbe"
        ));
        assert!(!rotation_artifact_name("capture.sbe.", "capture.sbe.1.sbe"));
    }

    #[test]
    fn rejects_directory_output_and_stdout_sidecars() {
        let directory = tempfile::tempdir().unwrap();
        let mut settings = Settings::load(None).unwrap();
        settings.output.path = directory.path().to_string_lossy().into_owned();
        assert!(settings.validate().is_err());

        settings.output.path = "-".to_owned();
        settings.output.metadata_path = Some(PathBuf::from("-"));
        assert!(settings.validate().is_err());
    }

    fn settings_with_urls(gamma: &str, ws: &str) -> Settings {
        let mut settings = Settings::load(None).unwrap();
        settings.polymarket.gamma_url = gamma.to_owned();
        settings.polymarket.ws_url = ws.to_owned();
        settings
    }

    #[test]
    fn checked_in_examples_parse_and_validate() {
        for path in ["config.example.toml", "config.polymarket.example.toml"] {
            let settings = Settings::load(Some(Path::new(path)))
                .unwrap_or_else(|error| panic!("{path} must load: {error:#}"));
            assert_eq!(settings.collector.catalog_mode, CatalogMode::Off);
            assert!(!settings.output.overwrite);
        }
    }

    #[test]
    fn validates_capacity_catalog_and_rotation_bounds() {
        let mut settings = Settings::load(None).unwrap();
        assert!(settings.validate().is_ok());

        settings.collector.max_total_connections = settings.collector.replicas - 1;
        assert!(settings.validate().is_err());
        settings.collector.max_total_connections = 64;

        settings.collector.connect_concurrency = 0;
        assert!(settings.validate().is_err());
        settings.collector.connect_concurrency = 8;

        settings.collector.catalog_mode = CatalogMode::Require;
        assert!(settings.validate().is_err());
        settings.collector.catalog_path = Some(PathBuf::from("catalog.json"));
        assert!(settings.validate().is_ok());
        settings.collector.catalog_mode = CatalogMode::Off;

        settings.output.rotate_bytes = settings.output.chunk_size as u64 - 1;
        assert!(settings.validate().is_err());
        settings.output.rotate_bytes = settings.output.chunk_size as u64;
        assert!(settings.validate().is_ok());

        settings.collector.readiness_min_anchored_fraction = 1.01;
        assert!(settings.validate().is_err());
        settings.collector.readiness_min_anchored_fraction = 0.99;
        assert!(settings.validate().is_ok());
    }

    #[test]
    fn validates_network_experiment_bounds() {
        let mut settings = Settings::load(None).unwrap();
        assert!(settings.validate().is_ok());
        assert!(settings.network.tcp_nodelay);
        assert_eq!(settings.network.connect_race, 1);

        settings.network.connect_race = 0;
        assert!(settings.validate().is_err());
        settings.network.connect_race = 9;
        assert!(settings.validate().is_err());

        settings.network.connect_race = 2;
        settings.network.source_port = Some(0);
        assert!(settings.validate().is_err());
        settings.network.source_port = Some(40_000);
        assert!(settings.validate().is_err());
        settings.network.source_port = None;
        assert!(settings.validate().is_ok());

        settings.network.source_ip = Some("127.0.0.1".parse().unwrap());
        settings.network.source_port = Some(40_000);
        settings.network.connect_race = 1;
        assert!(settings.validate().is_ok());
    }

    #[test]
    fn validates_url_scheme_host_and_loopback_policy() {
        assert!(
            settings_with_urls(
                "https://gamma-api.polymarket.com",
                "wss://ws-subscriptions-clob.polymarket.com/ws/market"
            )
            .validate()
            .is_ok()
        );
        assert!(
            settings_with_urls("http://127.0.0.1:8080", "ws://[::1]:8080/ws")
                .validate()
                .is_ok()
        );
        assert!(
            settings_with_urls("http://localhost:8080", "ws://localhost:8080/ws")
                .validate()
                .is_ok()
        );
        for (gamma, ws) in [
            ("file:///etc/passwd", "wss://example.com/ws"),
            ("https://", "wss://example.com/ws"),
            ("https://user:pass@example.com", "wss://example.com/ws"),
            ("https://example.com?token=secret", "wss://example.com/ws"),
            ("ftp://example.com", "wss://example.com/ws"),
            ("http://example.com", "wss://example.com/ws"),
            ("http://localhost.evil", "wss://example.com/ws"),
            ("https://example.com:0", "wss://example.com/ws"),
            ("https://gamma-api.polymarket.com", "ws://example.com/ws"),
            (
                "https://gamma-api.polymarket.com",
                "wss://example.com/ws#fragment",
            ),
        ] {
            assert!(
                settings_with_urls(gamma, ws).validate().is_err(),
                "expected rejection for {gamma} / {ws}"
            );
        }
    }

    #[test]
    fn enforces_the_gamma_keyset_page_limit() {
        let mut settings = Settings::load(None).unwrap();
        settings.polymarket.discovery_page_size = 101;
        assert!(settings.validate().is_err());
        settings.polymarket.discovery_page_size = 100;
        assert!(settings.validate().is_ok());
    }
}

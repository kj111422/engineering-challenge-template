//! Cell runner for the stress laboratory. Drives the production collector
//! against the loopback Kalshi mock and produces one machine-readable cell
//! outcome: a result *and* an explicit acceptance verdict, computed inside
//! the runner before any manifest is written. Everything here runs inside the
//! test process; no production code changes for the harness.
//!
//! Acceptance is fail-closed: a manifest with `run_status: success` can only
//! exist when anchoring, drain, coverage, SBE verification, completeness,
//! journal finalization, decoy exclusion and independent event accounting all
//! passed inside `run_cell` itself.

use std::{
    collections::{BTreeSet, HashMap},
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::SigningKey;
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
    time::sleep,
};

use engineering_challenge::{
    Side, completeness,
    config::{ExchangeKind, Settings},
    coordinator, coverage,
    report::RunReport,
    util::sha256_hex,
    verify,
};

use super::{
    host::HostInfo,
    mock::{MockKalshi, MockLedger},
    rng::SplitMix64,
    universe::SyntheticUniverse,
};

const ANCHOR_LIMIT: Duration = Duration::from_secs(45);
const FINISH_LIMIT: Duration = Duration::from_secs(60);
const DRAIN_LIMIT: Duration = Duration::from_secs(10);
const BASELINE_LIMIT: Duration = Duration::from_secs(5);
const METRICS_POLL: Duration = Duration::from_millis(25);
/// Once anchoring has started (`anchored > 0`), a window with zero progress
/// means the missing-anchor state has been reached; waiting for the full
/// deadline would only burn CI time. The hard deadline remains the backstop.
const ANCHOR_PLATEAU: Duration = Duration::from_secs(3);

/// Failure-injection knobs for harness self-tests. Each variant maps to one
/// named failure mode with a distinct `failure_class` in the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailInject {
    /// Skip the snapshot for one eligible market (by 1-based ordinal).
    MissingSnapshot { skip_one: usize },
    /// Skip the snapshot for one eligible market *and* publish exactly one
    /// delta for it: the symbol is observed but never snapshot-anchored.
    UpdateOnly { skip_one: usize },
    /// Drop the first `n` delta frames before the wire (no sequence number
    /// consumed; protocol-invisible loss only event accounting can catch).
    DroppedFrames { count: usize },
    /// Bound the mock's outbound queue hard and stall the forwarder so the
    /// drain barrier cannot complete in time.
    MockQueueSaturate { capacity: usize },
    /// Publish each logical delta a second time on the wire. A dedup-correct
    /// collector still applies exactly one copy; the extra application is the
    /// over-application the ledger must detect.
    OverApplication { duplicates: usize },
    /// Notify shutdown immediately after publishing, without any drain wait.
    ShutdownBeforeDrain,
}

/// One experiment cell.
#[derive(Debug, Clone)]
pub struct CellConfig {
    pub seed: u64,
    pub symbols: usize,
    pub decoy_fraction_ppm: u32,
    /// `None` derives shards via capacity::plan (settings.shards = 0).
    pub shards: Option<usize>,
    pub replicas: usize,
    pub lifecycle_replicas: usize,
    /// Number of order-book deltas to publish after anchoring.
    pub events: usize,
    /// Zipf-like skew: the first 10% of markets receive 80% of events.
    pub hot_pair: bool,
    /// Reserved for the workload-shape matrix (unused by the first cells).
    #[expect(
        dead_code,
        reason = "read by the workload-shape cells, which this increment does not wire yet"
    )]
    pub uniform_spread: bool,
    pub journal: bool,
    /// Overrides `ANCHOR_LIMIT`/`DRAIN_LIMIT` for fast failure tests.
    pub limits: Option<(u64, u64)>,
    /// Forces a capacity rejection (budget of 1 connection).
    pub force_capacity_reject: bool,
    /// One named failure-injection mode; `None` for ordinary cells.
    pub fail_inject: Option<FailInject>,
    /// Override the writer queue bound (harness self-test knob).
    pub writer_queue: Option<usize>,
    /// Repository directory the clean-worktree gate inspects. Defaults to the
    /// crate root; fixtures point this at a scratch git repository.
    pub worktree_dir: Option<PathBuf>,
    /// `Some(true)` enforces the clean-worktree gate regardless of the
    /// `STRESS_REQUIRE_CLEAN` environment variable.
    pub require_clean_worktree: Option<bool>,
}

impl Default for CellConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            symbols: 1_000,
            decoy_fraction_ppm: 20_000,
            shards: None,
            replicas: 2,
            lifecycle_replicas: 1,
            events: 300,
            hot_pair: false,
            uniform_spread: true,
            journal: true,
            limits: None,
            force_capacity_reject: false,
            fail_inject: None,
            writer_queue: None,
            worktree_dir: None,
            require_clean_worktree: None,
        }
    }
}

/// Acceptance verdict for one cell. Any failure lists every reason found; the
/// first reason is the primary machine-readable `failure_class`.
#[derive(Debug, Clone)]
pub struct CellAcceptance {
    pub valid: bool,
    pub reasons: Vec<String>,
    /// Primary machine-readable failure class for the manifest.
    pub failure_class: Option<String>,
}

impl CellAcceptance {
    fn valid() -> Self {
        Self {
            valid: true,
            reasons: Vec::new(),
            failure_class: None,
        }
    }
    fn from_reasons(reasons: Vec<String>) -> Self {
        let failure_class = reasons.first().cloned();
        Self {
            valid: false,
            reasons,
            failure_class,
        }
    }

    /// Design vocabulary: `success | failed | incomplete | interrupted`.
    /// `interrupted` is reserved for supervisor finalization of a run whose
    /// process died; everything the runner itself observes is `failed` unless
    /// every reason is a timeout-class observation, which is `incomplete`.
    #[must_use]
    pub fn status(&self) -> &'static str {
        if self.valid {
            return "success";
        }
        const TIMEOUT_CLASSES: [&str; 3] = ["anchor_timeout", "anchor_plateau", "drain_incomplete"];
        if self
            .reasons
            .iter()
            .all(|reason| TIMEOUT_CLASSES.contains(&reason.as_str()))
        {
            "incomplete"
        } else {
            "failed"
        }
    }
}

/// Incident counters scraped from the collector's `/metrics` endpoint at the
/// end of the run. `None` means the evidence is unavailable, not zero.
#[derive(Debug, Clone, Copy, Default)]
pub struct IncidentCounters {
    pub discontinuity_incidents: Option<u64>,
    pub unquantifiable_gaps: Option<u64>,
    pub suppressed_replica_duplicates: Option<u64>,
}

/// Per-stage event accounting, derived from independent counters:
/// the harness publish loop, the mock transmit ledger, the collector's
/// `/metrics` counters and the decoded SBE capture. Nothing here is
/// hard-coded to success; a stage that cannot be observed stays `None` and
/// makes the cell uncertifiable.
#[derive(Debug, Clone, Default)]
pub struct EventLedger {
    /// Logical deltas the harness generated and offered to the mock.
    pub generated_events: u64,
    /// Applied count the ledger expects from the injection arithmetic.
    pub expected_applied: u64,
    /// `collector_deltas_emitted_total` increase observed by the runner.
    /// `None` when the metrics evidence is unavailable.
    pub applied_events: Option<u64>,
    /// Delta messages in the decoded SBE capture. `None` on scan failure.
    pub sbe_deltas: Option<u64>,
    /// Journal record count when the journal is enabled.
    pub journaled_records: Option<u64>,
    /// Frames the mock dropped before the wire (injected fixture).
    pub injected_pre_wire_drops: u64,
    /// Deltas applied to a non-active replica and deliberately suppressed.
    /// `None` when the dedup counter could not be scraped. This is a
    /// transport duplicate, not a duplicate application.
    pub suppressed_replica_duplicates: Option<u64>,
    /// `generated - applied`, derived; `None` when applied is unobservable.
    /// This coarse residual also absorbs injected pre-wire drops.
    pub unexplained_drops: Option<u64>,
    /// `expected_applied - applied`: the downward half of the application
    /// balance. `Some(0)` is the only acceptable value.
    pub application_shortfall: Option<u64>,
    /// `applied - expected_applied`: the upward half. A positive value means
    /// one logical event was applied more than once.
    pub over_applied: Option<u64>,
    /// `applied - sbe_deltas`, derived; `None` when either side is missing.
    pub persistence_gap: Option<u64>,
    /// `sbe_deltas - applied`: persistence cannot contain more records than
    /// were applied. `Some(0)` is the only acceptable value.
    pub persistence_excess: Option<u64>,
}

impl EventLedger {
    /// A ledger reconciles only when every stage is observed and every
    /// two-sided derived gap is exactly zero. Saturating arithmetic is never
    /// used here to hide a negative difference: each direction is its own
    /// field, so over-application cannot pass as a clean run.
    #[must_use]
    pub fn reconciled(&self) -> bool {
        self.unexplained_drops == Some(0)
            && self.application_shortfall == Some(0)
            && self.over_applied == Some(0)
            && self.persistence_gap == Some(0)
            && self.persistence_excess == Some(0)
    }
}

/// Full result of one cell run — raw verifier digests included for the manifest.
pub struct CellOutput {
    pub report: RunReport,
    pub coverage: Option<coverage::CoverageSummary>,
    pub coverage_error: Option<String>,
    pub completeness_error: Option<String>,
    pub verify_error: Option<String>,
    pub completeness_valid: bool,
    pub verify_valid: bool,
    pub anchored_time: Duration,
    pub harness_process_rss_sample_bytes: Option<u64>,
    /// Independently generated eligible universe.
    pub expected_symbols: BTreeSet<String>,
    /// Symbols the collector acknowledged via the report.
    pub acknowledged_symbols: BTreeSet<String>,
    /// Symbols with any snapshot or price-level update in the capture.
    pub observed_symbols: BTreeSet<String>,
    /// Symbols anchored by at least one *snapshot* (never by deltas alone).
    pub anchored_symbols: BTreeSet<String>,
    pub eligible_count: usize,
    pub decoy_absent: bool,
    pub journal_complete: bool,
    pub journaled_records: Option<u64>,
    /// Mock transmit-side counters (enqueued vs socket-written vs in flight).
    pub mock: MockLedger,
    /// Frames whose websocket send completed (= `mock.sent`).
    pub transmitted_frames: u64,
    pub events_published: usize,
    pub applied_events: Option<u64>,
    pub capacity_connections: usize,
    pub anchor_wait_expired: bool,
    pub drain_completed: bool,
    pub drain_detail: Option<String>,
    pub sbe_deltas: u64,
    pub sbe_snapshots: u64,
    pub sbe_messages: u64,
    /// Production writer queue high-water mark (from `WriterStats`).
    pub writer_queue_high_water: u64,
    /// Continuity incidents observed at the end of the run.
    pub incidents: IncidentCounters,
    pub ledger: EventLedger,
}

/// One finished cell: output plus the runner's acceptance verdict plus the
/// worktree provenance captured before the run started.
pub struct CellOutcome {
    pub output: Option<CellOutput>,
    pub acceptance: CellAcceptance,
    pub provenance: Option<WorktreeProvenance>,
}

/// Worktree provenance recorded for every manifest. Untracked files are
/// counted informationally; the gate only ever considers tracked changes.
#[derive(Debug, Clone, Default)]
pub struct WorktreeProvenance {
    pub commit: Option<String>,
    /// `Some(false)` when tracked files differ from `HEAD`.
    pub tracked_clean: Option<bool>,
    pub tracked_modified: Vec<String>,
    /// SHA-256 of `git diff HEAD` output while the worktree is dirty.
    pub tracked_diff_sha256: Option<String>,
    pub untracked_count: Option<usize>,
    pub error: Option<String>,
}

fn git(repo: &Path, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Inspect `repo` and record provenance. Never fails the run by itself.
#[must_use]
pub fn worktree_provenance(repo: &Path) -> WorktreeProvenance {
    let commit = git(repo, &["rev-parse", "HEAD"]);
    let tracked_status = git(repo, &["status", "--porcelain", "--untracked-files=no"]);
    let full_status = git(repo, &["status", "--porcelain"]);
    let diff = git(repo, &["diff", "HEAD"]);
    let error = if commit.is_none() && tracked_status.is_none() {
        Some("git unavailable or not a repository".to_owned())
    } else {
        None
    };
    let tracked_modified: Vec<String> = tracked_status
        .as_ref()
        .map(|text| {
            text.lines()
                .filter(|line| !line.trim().is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let tracked_clean = tracked_status.map(|_| tracked_modified.is_empty());
    let tracked_diff_sha256 = diff
        .filter(|text| !text.is_empty())
        .map(|text| sha256_hex(text.as_bytes()));
    let untracked_count =
        full_status.map(|text| text.lines().filter(|line| line.starts_with("?? ")).count());
    WorktreeProvenance {
        commit,
        tracked_clean,
        tracked_modified,
        tracked_diff_sha256,
        untracked_count,
        error,
    }
}

fn require_clean_flag(config: &CellConfig) -> bool {
    config
        .require_clean_worktree
        .unwrap_or_else(|| std::env::var("STRESS_REQUIRE_CLEAN").is_ok_and(|v| v == "1"))
}

/// Preflight gate. Fails closed when a clean worktree is required and the
/// tracked state is dirty — or cannot be verified at all.
fn worktree_preflight(config: &CellConfig) -> (WorktreeProvenance, Option<String>) {
    let repo = config
        .worktree_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    let provenance = worktree_provenance(&repo);
    if !require_clean_flag(config) {
        return (provenance, None);
    }
    if provenance.error.is_some() {
        return (provenance, Some("worktree_unverifiable".to_owned()));
    }
    if provenance.tracked_clean == Some(false) {
        return (provenance, Some("dirty_worktree".to_owned()));
    }
    (provenance, None)
}

fn unused_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binding an ephemeral port");
    listener
        .local_addr()
        .expect("reading the ephemeral address")
}

fn write_test_key(directory: &Path) -> PathBuf {
    let key_path = directory.join("stress-key.pem");
    let signing = SigningKey::from_bytes(&[0x5A; 32]);
    fs::write(
        &key_path,
        ed25519_dalek::pkcs8::EncodePrivateKey::to_pkcs8_pem(
            &signing,
            ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF,
        )
        .unwrap(),
    )
    .unwrap();
    key_path
}

fn settings_for(mock: &MockKalshi, directory: &Path, config: &CellConfig) -> Settings {
    let mut settings = Settings::for_exchange(ExchangeKind::Kalshi);
    settings.kalshi.rest_url = Some(mock.http_url());
    settings.kalshi.ws_url = Some(mock.ws_url());
    settings.kalshi.key_id = "stress-key".to_owned();
    settings.kalshi.private_key_path = Some(write_test_key(directory));
    settings.kalshi.discovery_page_size = 1_000;
    settings.kalshi.discovery_page_delay_ms = 0;
    settings.kalshi.request_timeout_secs = 5;
    settings.kalshi.batch_size = 500;

    settings.collector.shards = config.shards.unwrap_or(0);
    settings.collector.replicas = config.replicas;
    settings.collector.lifecycle_replicas = config.lifecycle_replicas;
    settings.collector.discovery_interval_secs = 3_600;
    settings.collector.reconnect_after_secs = 1_800;
    settings.collector.connection_idle_timeout_secs = 45;
    settings.collector.stale_probe_after_secs = 120;
    settings.collector.probe_interval_secs = 30;
    settings.collector.max_probes = 3;
    settings.collector.max_markets = 0;
    settings.collector.duration_secs = 0;
    settings.collector.writer_queue = config.writer_queue.unwrap_or(4_096);
    settings.collector.writer_send_timeout_secs = 1;
    settings.collector.max_total_connections = if config.force_capacity_reject { 1 } else { 512 };
    settings.collector.max_shards = 256;
    settings.collector.max_estimated_book_memory_mb = 4_096;
    settings.collector.max_snapshot_bytes_per_sec = 64 << 20;
    settings.collector.final_snapshots = true;
    settings.output.path = directory.join("orderbook.sbe").display().to_string();
    settings.output.chunk_size = 16 * 1024;
    settings.output.metadata_path = Some(directory.join("markets.json"));
    settings.output.report_path = Some(directory.join("report.json"));
    settings.output.sync_on_close = false;
    settings.metrics.listen = Some(unused_address());
    settings.journal.directory = if config.journal {
        Some(directory.join("journal"))
    } else {
        None
    };
    settings.network = Default::default();
    settings
}

/// Outcome of reading one metric family from a scrape.
#[derive(Debug, Clone, Copy, PartialEq)]
enum MetricRead {
    Present(f64),
    Absent,
    Malformed,
}

/// One parsed `/metrics` body. Labeled families are folded to the family name;
/// a family that cannot be parsed is recorded as malformed instead of being
/// treated as zero.
#[derive(Debug, Default, Clone)]
struct MetricSample {
    values: HashMap<String, f64>,
    malformed: BTreeSet<String>,
}

impl MetricSample {
    fn read(&self, name: &str) -> MetricRead {
        if let Some(value) = self.values.get(name) {
            return MetricRead::Present(*value);
        }
        if self.malformed.contains(name) {
            return MetricRead::Malformed;
        }
        MetricRead::Absent
    }

    /// A required metric is an error when it is absent or unparseable. This is
    /// the difference between "the collector emitted zero" and "the evidence
    /// path is broken".
    fn required(&self, name: &str) -> Result<f64, String> {
        match self.read(name) {
            MetricRead::Present(value) => Ok(value),
            MetricRead::Absent => Err(format!("required metric `{name}` is absent")),
            MetricRead::Malformed => Err(format!("required metric `{name}` is malformed")),
        }
    }

    /// An exact non-negative count, or `None` when the sample is not one.
    fn count(&self, name: &str) -> Option<u64> {
        match self.read(name) {
            MetricRead::Present(value)
                if value.is_finite() && value >= 0.0 && value.fract() == 0.0 =>
            {
                Some(value as u64)
            }
            _ => None,
        }
    }
}

fn parse_metrics(body: &str) -> MetricSample {
    let mut sample = MetricSample::default();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once(' ') else {
            continue;
        };
        let name = name.split('{').next().unwrap_or(name);
        match value.trim().parse::<f64>() {
            Ok(parsed) => {
                sample.values.insert(name.to_owned(), parsed);
            }
            Err(_) => {
                sample.malformed.insert(name.to_owned());
            }
        }
    }
    sample
}

/// Scrape every plain metric sample from the collector's `/metrics` endpoint.
async fn scrape_metrics(address: SocketAddr) -> Option<MetricSample> {
    let mut stream = TcpStream::connect(address).await.ok()?;
    let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await.ok()?;
    let body = response.split_once("\r\n\r\n")?.1;
    Some(parse_metrics(body))
}

async fn scrape_metric(address: SocketAddr, metric: &str) -> Option<f64> {
    match scrape_metrics(address).await?.read(metric) {
        MetricRead::Present(value) => Some(value),
        _ => None,
    }
}

fn rss_sample_bytes() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    text.trim()
        .parse::<u64>()
        .ok()
        .map(|kilobytes| kilobytes * 1024)
}

/// The eligible market a snapshot-skip injection targets, resolved through
/// the eligible universe (1-based ordinal), never a raw universe index.
fn skip_target(config: &CellConfig, tickers: &[String]) -> Option<String> {
    match config.fail_inject {
        Some(FailInject::MissingSnapshot { skip_one })
        | Some(FailInject::UpdateOnly { skip_one }) => {
            tickers.get(skip_one.saturating_sub(1)).cloned()
        }
        _ => None,
    }
}

fn apply_failure_injection(mock: &MockKalshi, config: &CellConfig, tickers: &[String]) {
    match config.fail_inject {
        Some(FailInject::MissingSnapshot { .. }) | Some(FailInject::UpdateOnly { .. }) => {
            if let Some(ticker) = skip_target(config, tickers)
                && let Some(index) = mock.state().market_index(&ticker)
            {
                mock.state().set_skip_snapshots([index]);
            }
        }
        Some(FailInject::DroppedFrames { count }) => {
            mock.state().set_drop_delta_count(count as u64);
        }
        Some(FailInject::MockQueueSaturate { capacity }) => {
            mock.state().set_outbound_capacity(capacity);
            mock.state().stall_forwarder();
        }
        Some(FailInject::OverApplication { .. }) | Some(FailInject::ShutdownBeforeDrain) | None => {
        }
    }
}

fn publish_delta_event(mock: &MockKalshi, ticker: &str, rng: &mut SplitMix64) {
    let side = if rng.below(2) == 0 {
        Side::Buy
    } else {
        Side::Sell
    };
    let price = 400_000 + i64::try_from(rng.below(200_000 / 10_000)).unwrap_or(0) * 10_000;
    let delta = 2_000_000 + i64::try_from(rng.below(3_000_000)).unwrap_or(0);
    mock.state().publish_delta(ticker, side, price, delta);
}

/// Fixture-aware publish plan. `expected_applied` is derived from the
/// injection arithmetic before the run observes anything:
///
/// - `UpdateOnly`: exactly one delta hits the unanchored market (silently
///   dropped by the collector's continuity guard) plus `events - 1` normal
///   deltas on publishable markets;
/// - `MissingSnapshot`: all deltas avoid the skip target;
/// - `DroppedFrames { count }`: the first `count` frames are dropped before
///   the wire in replica-pair order, so `count / replicas` events vanish;
/// - `OverApplication { duplicates }`: `duplicates` extra delta frames are
///   published for already-generated logical events, so `expected_applied`
///   stays correct while a dedup regression applies more than expected.
struct PublishPlan {
    generated: usize,
    /// The count a dedup-correct collector must apply. This is the ledger
    /// baseline, not the number of wire frames.
    expected_applied: u64,
    /// The count the drain barrier waits for. It exceeds `expected_applied`
    /// only for a fixture that deliberately publishes extra frames.
    drain_target: u64,
}

fn publish_events(
    mock: &MockKalshi,
    tickers: &[String],
    config: &CellConfig,
    rng: &mut SplitMix64,
    skip_ticker: Option<&str>,
) -> PublishPlan {
    let mut generated = 0usize;
    if matches!(config.fail_inject, Some(FailInject::UpdateOnly { .. }))
        && let Some(ticker) = skip_ticker
    {
        publish_delta_event(mock, ticker, rng);
        generated += 1;
    }
    let publishable: Vec<&str> = tickers
        .iter()
        .map(String::as_str)
        .filter(|ticker| Some(*ticker) != skip_ticker)
        .collect();
    let hot_cut = (publishable.len() / 10).max(2);
    while generated < config.events {
        let index = if config.hot_pair {
            if rng.below(100) < 80 {
                rng.below_usize(hot_cut)
            } else {
                rng.below_usize(publishable.len())
            }
        } else {
            rng.below_usize(publishable.len())
        };
        publish_delta_event(mock, publishable[index], rng);
        generated += 1;
    }
    // Over-application fixture: publish the same logical events again without
    // increasing `generated`. The expected ledger stays correct; a collector
    // that applies both copies exceeds it and must fail two-sided accounting.
    if let Some(FailInject::OverApplication { duplicates }) = config.fail_inject {
        for _ in 0..duplicates {
            if publishable.is_empty() {
                break;
            }
            let index = rng.below_usize(publishable.len());
            publish_delta_event(mock, publishable[index], rng);
        }
    }
    let injected_frames = match config.fail_inject {
        Some(FailInject::DroppedFrames { count }) => count as u64,
        _ => 0,
    };
    let replicas = (config.replicas.max(1)) as u64;
    let unanchored_update = u64::from(matches!(
        config.fail_inject,
        Some(FailInject::UpdateOnly { .. })
    ));
    let expected_applied = (generated as u64)
        .saturating_sub(injected_frames / replicas)
        .saturating_sub(unanchored_update);
    let extra_wire_frames = match config.fail_inject {
        Some(FailInject::OverApplication { duplicates }) => duplicates as u64,
        _ => 0,
    };
    PublishPlan {
        generated,
        expected_applied,
        // The barrier waits for every frame the fixture actually put on the
        // wire. For an over-application fixture this is deliberately greater
        // than the correct ledger, so the run drains before the ledger is
        // evaluated and the excess is measured rather than raced.
        drain_target: expected_applied.saturating_add(extra_wire_frames),
    }
}

/// Writer-side baseline captured after anchoring and before publishing. The
/// writer must be quiescent (queue drained, message count stable across two
/// polls) so the drain barrier can attribute every later message to the
/// published workload.
#[derive(Debug, Clone, Copy, Default)]
struct DrainBaselines {
    deltas_emitted: f64,
    writer_messages: f64,
    writer_errors: f64,
}

/// The fixed set of metrics the drain barrier requires. Every field is read
/// explicitly; there is no `unwrap_or(0.0)` on this path.
#[derive(Debug, Clone, Copy)]
struct DrainSample {
    deltas_emitted: f64,
    writer_messages: f64,
    writer_queue_depth: f64,
    writer_errors: f64,
}

fn required_drain_sample(sample: &MetricSample) -> Result<DrainSample, String> {
    Ok(DrainSample {
        deltas_emitted: sample.required("collector_deltas_emitted_total")?,
        writer_messages: sample.required("collector_writer_messages_total")?,
        writer_queue_depth: sample.required("collector_writer_queue_depth")?,
        writer_errors: sample.required("collector_writer_errors_total")?,
    })
}

/// State of the drain barrier for one scrape.
#[derive(Debug, PartialEq)]
enum BarrierState {
    Satisfied,
    Waiting,
    /// A required metric is absent or malformed. The evidence path is
    /// unavailable; the cell must not report zero and must not certify.
    Unavailable(String),
    /// A counter moved backwards. Distinguishing a reset from absence keeps a
    /// process restart from looking like a slow drain.
    CounterReset(&'static str),
}

fn evaluate_barrier(
    sample: &MetricSample,
    base: &DrainBaselines,
    expected: f64,
    in_flight: u64,
) -> BarrierState {
    let required = match required_drain_sample(sample) {
        Ok(required) => required,
        Err(error) => return BarrierState::Unavailable(error),
    };
    if required.deltas_emitted < base.deltas_emitted {
        return BarrierState::CounterReset("collector_deltas_emitted_total");
    }
    if required.writer_messages < base.writer_messages {
        return BarrierState::CounterReset("collector_writer_messages_total");
    }
    if required.writer_errors < base.writer_errors {
        return BarrierState::CounterReset("collector_writer_errors_total");
    }
    if in_flight == 0
        && required.deltas_emitted >= base.deltas_emitted + expected
        && required.writer_messages >= base.writer_messages + expected
        && required.writer_queue_depth == 0.0
        && required.writer_errors == base.writer_errors
    {
        BarrierState::Satisfied
    } else {
        BarrierState::Waiting
    }
}

async fn capture_baseline(address: SocketAddr) -> Result<DrainBaselines, String> {
    capture_baseline_until(address, Instant::now() + BASELINE_LIMIT).await
}

async fn capture_baseline_until(
    address: SocketAddr,
    deadline: Instant,
) -> Result<DrainBaselines, String> {
    let mut previous: Option<(f64, f64)> = None;
    let mut last_error = "metrics endpoint unavailable".to_owned();
    loop {
        match scrape_metrics(address).await {
            Some(sample) => match required_drain_sample(&sample) {
                Ok(required) => {
                    if let Some((previous_messages, previous_depth)) = previous
                        && previous_depth == 0.0
                        && required.writer_queue_depth == 0.0
                        && previous_messages == required.writer_messages
                    {
                        return Ok(DrainBaselines {
                            deltas_emitted: required.deltas_emitted,
                            writer_messages: required.writer_messages,
                            writer_errors: required.writer_errors,
                        });
                    }
                    previous = Some((required.writer_messages, required.writer_queue_depth));
                }
                Err(error) => {
                    previous = None;
                    last_error = error;
                }
            },
            None => {
                previous = None;
                last_error = "metrics endpoint unavailable".to_owned();
            }
        }
        if Instant::now() >= deadline {
            return Err(last_error);
        }
        sleep(METRICS_POLL).await;
    }
}

pub async fn run_cell(
    directory: &Path,
    config: &CellConfig,
    manifest_out: Option<&Path>,
) -> CellOutcome {
    let outcome = run_cell_inner(directory, config).await;
    finalize_outcome(outcome, config, manifest_out)
}

/// The filename carries a filesystem-safe summary; the exact verdict stays in
/// the body and in the index entry.
fn cell_id_for(outcome: &CellOutcome, config: &CellConfig) -> String {
    if outcome.acceptance.valid {
        return format!("seed-{:04x}-{}-uniform", config.seed, config.symbols);
    }
    let class = outcome
        .acceptance
        .failure_class
        .as_deref()
        .unwrap_or("unknown");
    let summary: String = class
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    format!(
        "{}-seed-{:04x}-{}-{}",
        outcome.acceptance.status(),
        config.seed,
        config.symbols,
        summary
    )
}

/// Validate the manifest contract, then publish. A success verdict is only
/// kept when every required field is present; a missing field converts the
/// verdict to `manifest_incomplete` and the artifact records that failure.
fn finalize_outcome(
    outcome: CellOutcome,
    config: &CellConfig,
    manifest_out: Option<&Path>,
) -> CellOutcome {
    let cell_id = cell_id_for(&outcome, config);
    let document = build_cell_manifest(&outcome, config, &cell_id);

    if outcome.acceptance.valid {
        let missing = super::manifest::missing_required_fields(&document);
        if !missing.is_empty() {
            let mut reasons = vec!["manifest_incomplete".to_owned()];
            reasons.extend(
                missing
                    .into_iter()
                    .map(|path| format!("missing_field: {path}")),
            );
            let failed = CellOutcome {
                output: outcome.output,
                provenance: outcome.provenance,
                acceptance: CellAcceptance::from_reasons(reasons),
            };
            let failed_id = cell_id_for(&failed, config);
            let failed_document = build_cell_manifest(&failed, config, &failed_id);
            return publish(failed, config, manifest_out, &failed_id, failed_document);
        }
    }
    publish(outcome, config, manifest_out, &cell_id, document)
}

/// Publish index-first through [`super::manifest::publish_cell`]. Recovery of
/// an interrupted earlier attempt runs before the new publication, so a
/// pending entry never survives into the next cell's view of the index.
fn publish(
    outcome: CellOutcome,
    config: &CellConfig,
    manifest_out: Option<&Path>,
    cell_id: &str,
    document: Map<String, Value>,
) -> CellOutcome {
    let Some(out_dir) = manifest_out else {
        return outcome;
    };
    let index_path = out_dir.join("index.json");
    if let Err(error) = super::manifest::recover(&index_path) {
        return with_manifest_failure(outcome, "manifest_index_error", &error);
    }
    let entry = json!({
        "run_status": outcome.acceptance.status(),
        "seed": config.seed,
        "symbols": config.symbols,
    });
    let entry = match entry {
        Value::Object(entry) => entry,
        _ => Map::new(),
    };
    match super::manifest::publish_cell(&index_path, cell_id, entry, &Value::Object(document)) {
        Ok(()) => outcome,
        Err(super::manifest::PublishError::Index { source }) => {
            with_manifest_failure(outcome, "manifest_index_error", &source)
        }
        Err(super::manifest::PublishError::Body { source }) => {
            with_manifest_failure(outcome, "manifest_write_error", &source)
        }
        Err(super::manifest::PublishError::Serialize { source }) => {
            with_manifest_failure(outcome, "manifest_serialize_error", &source)
        }
    }
}

fn with_manifest_failure(
    outcome: CellOutcome,
    class: &str,
    error: &dyn std::fmt::Display,
) -> CellOutcome {
    CellOutcome {
        output: outcome.output,
        provenance: outcome.provenance,
        acceptance: CellAcceptance::from_reasons(vec![class.to_owned(), format!("{error}")]),
    }
}

async fn run_cell_inner(directory: &Path, config: &CellConfig) -> CellOutcome {
    // Worktree provenance is captured before anything else runs so it
    // describes the state the cell actually executed in.
    let (provenance, preflight_error) = worktree_preflight(config);
    if let Some(reason) = preflight_error {
        return CellOutcome {
            output: None,
            provenance: Some(provenance),
            acceptance: CellAcceptance::from_reasons(vec![reason]),
        };
    }

    let universe =
        SyntheticUniverse::generate(config.seed, config.symbols, config.decoy_fraction_ppm);
    let eligible = universe.eligible_tickers_sorted();
    let mock = MockKalshi::start(universe).await;
    let mut seed_rng = SplitMix64::new(config.seed ^ 0x51E5_ED5E_ED5E_0001);
    for market in mock.state().universe().markets.iter() {
        mock.state().seed_book(&market.ticker, &mut seed_rng);
    }
    apply_failure_injection(&mock, config, &eligible);

    let settings = settings_for(&mock, directory, config);
    let metrics_addr = settings.metrics.listen;
    let done = Arc::new(Notify::new());
    let done_for_run = Arc::clone(&done);
    let anchor_timeout = config
        .limits
        .map(|(anchor, _)| anchor)
        .unwrap_or(ANCHOR_LIMIT.as_secs());
    let drain_timeout = config
        .limits
        .map(|(_, drain)| drain)
        .unwrap_or(DRAIN_LIMIT.as_secs());
    let anchor_limit = Duration::from_secs(anchor_timeout);
    let drain_limit = Duration::from_secs(drain_timeout);
    let mut handle = tokio::spawn(coordinator::run_with_shutdown(settings, async move {
        tokio::select! {
            _ = done_for_run.notified() => "stress scenario complete".to_owned(),
            _ = sleep(Duration::from_secs(120)) => "stress scenario safety timeout".to_owned(),
        }
    }));

    // Wait until every eligible symbol has an anchored book. A collector that
    // terminates before anchoring is detected immediately so the failure
    // manifest names the real cause. A plateau below the target (the
    // missing-anchor injections) short-circuits the wait; the hard deadline
    // remains the backstop.
    let anchor_start = Instant::now();
    let mut anchor_wait_expired = true;
    let mut anchor_plateau = false;
    let mut last_anchored = 0.0f64;
    let mut last_progress = Instant::now();
    loop {
        if handle.is_finished() {
            let outcome = (&mut handle)
                .await
                .unwrap_or_else(|error| Err(anyhow::anyhow!("collector task failed: {error}")));
            let reason = match outcome {
                Ok(_report) => "collector_terminated_before_anchor".to_owned(),
                Err(error) => format!("collector_terminated_early: {error:#}"),
            };
            return CellOutcome {
                output: None,
                provenance: Some(provenance),
                acceptance: CellAcceptance::from_reasons(vec![reason]),
            };
        }
        if let Some(addr) = metrics_addr
            && let Some(value) = scrape_metric(addr, "collector_symbols_anchored").await
        {
            if value >= eligible.len() as f64 {
                anchor_wait_expired = false;
                break;
            }
            if value > last_anchored {
                last_anchored = value;
                last_progress = Instant::now();
            } else if value > 0.0 && last_progress.elapsed() >= ANCHOR_PLATEAU {
                anchor_plateau = true;
                break;
            }
        }
        if anchor_start.elapsed() >= anchor_limit {
            break;
        }
        sleep(METRICS_POLL).await;
    }
    let anchored_time = anchor_start.elapsed();

    // Writer-side baseline: the writer must be quiescent before the workload
    // is offered so the drain barrier can attribute every message. A missing
    // or malformed mandatory metric is an evidence failure, not a zero.
    let mut baseline_error: Option<String> = None;
    let baseline = if matches!(config.fail_inject, Some(FailInject::ShutdownBeforeDrain)) {
        None
    } else {
        match metrics_addr {
            Some(addr) => match capture_baseline(addr).await {
                Ok(baseline) => Some(baseline),
                Err(error) => {
                    baseline_error = Some(error);
                    None
                }
            },
            None => {
                baseline_error = Some("metrics endpoint is not configured".to_owned());
                None
            }
        }
    };

    // Publish the synthetic workload and derive what *must* be applied.
    let skip_ticker = skip_target(config, &eligible);
    let plan = publish_events(
        &mock,
        &eligible,
        config,
        &mut SplitMix64::new(config.seed ^ 0xBEE7_6F00_0D15_EA5E),
        skip_ticker.as_deref(),
    );

    // Drain barrier: every frame must leave the mock's outbound queue, be
    // socket-written, reach the collector and be accounted for by the core
    // and the writer before shutdown is requested. A timeout classifies the
    // run as incomplete; a missing or malformed metric classifies the
    // evidence path as unavailable. No fixed sleep is involved.
    let mut drain_completed = false;
    let mut drain_detail: Option<String> = None;
    let mut drain_evidence_error: Option<String> = None;
    let mut last_deltas = baseline.map(|base| base.deltas_emitted);
    if matches!(config.fail_inject, Some(FailInject::ShutdownBeforeDrain)) {
        drain_detail = Some("shutdown requested before the drain barrier".to_owned());
    } else if let Some(error) = &baseline_error {
        drain_detail = Some(format!("writer baseline unavailable: {error}"));
        drain_evidence_error = Some("drain_metrics_unavailable".to_owned());
    } else if let (Some(addr), Some(base)) = (metrics_addr, baseline) {
        let deadline = Instant::now() + drain_limit;
        let mut unavailable: Option<String> = None;
        loop {
            if let Some(sample) = scrape_metrics(addr).await {
                if let MetricRead::Present(deltas) = sample.read("collector_deltas_emitted_total") {
                    last_deltas = Some(deltas);
                }
                match evaluate_barrier(
                    &sample,
                    &base,
                    plan.drain_target as f64,
                    mock.state().in_flight(),
                ) {
                    BarrierState::Satisfied => {
                        drain_completed = true;
                        break;
                    }
                    BarrierState::Waiting => {
                        unavailable = None;
                    }
                    BarrierState::Unavailable(error) => {
                        unavailable = Some(error);
                    }
                    BarrierState::CounterReset(metric) => {
                        drain_detail =
                            Some(format!("counter `{metric}` moved backwards during the run"));
                        drain_evidence_error = Some("counter_reset".to_owned());
                        break;
                    }
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            sleep(METRICS_POLL).await;
        }
        if !drain_completed && drain_evidence_error.is_none() {
            match unavailable {
                Some(error) => {
                    drain_detail = Some(error);
                    drain_evidence_error = Some("drain_metrics_unavailable".to_owned());
                }
                None => {
                    drain_detail = Some(
                        "drain deadline exceeded before every frame was accounted for".to_owned(),
                    );
                }
            }
        }
    }

    // Final scrape before shutdown: applied counter and incident counters.
    let final_sample = match metrics_addr {
        Some(addr) => scrape_metrics(addr).await,
        None => None,
    };
    if let Some(deltas) = final_sample.as_ref().and_then(|sample| {
        match sample.read("collector_deltas_emitted_total") {
            MetricRead::Present(value) => Some(value),
            _ => None,
        }
    }) {
        last_deltas = Some(deltas);
    }
    let incidents = IncidentCounters {
        discontinuity_incidents: final_sample
            .as_ref()
            .and_then(|sample| sample.count("collector_discontinuity_incidents_total")),
        unquantifiable_gaps: final_sample
            .as_ref()
            .and_then(|sample| sample.count("collector_unquantifiable_gaps_total")),
        suppressed_replica_duplicates: final_sample
            .as_ref()
            .and_then(|sample| sample.count("collector_suppressed_replica_duplicates_total")),
    };

    done.notify_one();
    let report = match tokio::time::timeout(FINISH_LIMIT, &mut handle).await {
        Ok(Ok(Ok(report))) => report,
        Ok(Ok(Err(error))) => {
            return CellOutcome {
                output: None,
                provenance: Some(provenance),
                acceptance: CellAcceptance::from_reasons(vec![format!(
                    "collector_task_failed: {error:#}"
                )]),
            };
        }
        Ok(Err(error)) => {
            return CellOutcome {
                output: None,
                provenance: Some(provenance),
                acceptance: CellAcceptance::from_reasons(vec![format!(
                    "collector_task_join_failed: {error}"
                )]),
            };
        }
        Err(_) => {
            return CellOutcome {
                output: None,
                provenance: Some(provenance),
                acceptance: CellAcceptance::from_reasons(vec![
                    "collector_did_not_stop_within_limit".to_owned(),
                ]),
            };
        }
    };

    let capture = PathBuf::from(&report.output_path);
    let metadata_path = directory.join("markets.json");
    let report_path = directory.join("report.json");
    let coverage_result = coverage::run(&capture, &metadata_path, &report_path, "auto");
    let completeness_result = completeness::run(&capture, "auto", false);
    let verify_result = verify::run(&capture, "auto");
    let completeness_valid = completeness_result
        .as_ref()
        .map(|summary| summary.valid)
        .unwrap_or(false);
    let verify_valid = verify_result
        .as_ref()
        .map(|summary| summary.valid)
        .unwrap_or(false);
    let coverage_error = coverage_result.as_ref().err().map(|e| format!("{e:#}"));
    let completeness_error = completeness_result.as_ref().err().map(|e| format!("{e:#}"));
    let verify_error = verify_result.as_ref().err().map(|e| format!("{e:#}"));

    // Decode the capture for the observed (any message) and snapshot-anchored
    // symbol sets, mirroring the definitions in `coverage.rs`.
    let mut observed = BTreeSet::new();
    let mut anchored = BTreeSet::new();
    let mut sbe_deltas = 0u64;
    let mut sbe_snapshots = 0u64;
    let mut sbe_messages = 0u64;
    let scan = (|| -> anyhow::Result<()> {
        use engineering_challenge::util::{
            AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage,
        };
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open_auto(&capture)?;
        while let Some(message) = reader.read_msg()? {
            sbe_messages += 1;
            match message {
                AnyMessage::Snapshot(decoder) => {
                    sbe_snapshots += 1;
                    let snapshot = engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain::try_from_decoder(decoder)?;
                    observed.insert(snapshot.symbol.clone());
                    anchored.insert(snapshot.symbol);
                }
                AnyMessage::PriceLevelUpdate(decoder) => {
                    sbe_deltas += 1;
                    let update = engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                    observed.insert(update.symbol);
                }
                AnyMessage::Unknown { .. } => {}
                _ => {}
            }
        }
        Ok(())
    })();
    let sbe_scan_ok = scan.is_ok();
    let sbe_scan_error = scan.as_ref().err().map(|e| format!("{e:#}"));

    let decoy_absent = mock
        .state()
        .universe()
        .excluded_tickers_sorted()
        .iter()
        .all(|ticker| !observed.contains(ticker));

    let journal_enabled = report.journal.is_some();
    let journal_complete = if journal_enabled {
        report
            .journal_verification
            .as_ref()
            .map(|verification| verification.complete)
            .unwrap_or(false)
    } else {
        true
    };
    let journaled_records = if journal_enabled {
        report
            .journal_verification
            .as_ref()
            .map(|verification| verification.record_count)
    } else {
        None
    };

    // Event accounting: derived from the publish plan, the mock ledger, the
    // collector's metric counters and the decoded capture. No count is
    // hard-coded to success.
    let transmit = mock.state().ledger_snapshot();
    let applied_events = match (baseline, last_deltas) {
        (Some(base), Some(final_deltas)) => {
            Some(((final_deltas - base.deltas_emitted).max(0.0)) as u64)
        }
        _ => None,
    };
    let sbe_deltas_observed = if sbe_scan_ok { Some(sbe_deltas) } else { None };
    let ledger = EventLedger {
        generated_events: plan.generated as u64,
        expected_applied: plan.expected_applied,
        applied_events,
        sbe_deltas: sbe_deltas_observed,
        journaled_records,
        injected_pre_wire_drops: transmit.injected_pre_wire_drops,
        suppressed_replica_duplicates: incidents.suppressed_replica_duplicates,
        unexplained_drops: applied_events
            .map(|applied| (plan.generated as u64).saturating_sub(applied)),
        application_shortfall: applied_events
            .map(|applied| plan.expected_applied.saturating_sub(applied)),
        over_applied: applied_events.map(|applied| applied.saturating_sub(plan.expected_applied)),
        persistence_gap: match (applied_events, sbe_deltas_observed) {
            (Some(applied), Some(sbe)) => Some(applied.saturating_sub(sbe)),
            _ => None,
        },
        persistence_excess: match (applied_events, sbe_deltas_observed) {
            (Some(applied), Some(sbe)) => Some(sbe.saturating_sub(applied)),
            _ => None,
        },
    };

    // Exact three-way set comparison against the independently generated
    // eligible universe.
    let expected_set: BTreeSet<String> = eligible.iter().cloned().collect();
    let acknowledged_set: BTreeSet<String> = report.acknowledged_symbols.iter().cloned().collect();
    let set_mismatch = expected_set != acknowledged_set || expected_set != anchored;

    let capacity_connections = report
        .capacity
        .as_ref()
        .map(|plan| plan.total_connections)
        .unwrap_or(0);
    let rss_sample = rss_sample_bytes();

    // Compose the acceptance verdict: every condition must pass. Data-integrity
    // reasons come first so `failure_class` names the real cause; timeout
    // observations come last.
    let mut reasons: Vec<String> = Vec::new();
    if set_mismatch {
        reasons.push("coverage_set_mismatch".to_owned());
    }
    match &coverage_result {
        Ok(summary) if summary.valid => {}
        Ok(_) => reasons.push("coverage_invalid".to_owned()),
        Err(_) => reasons.push("coverage_unreadable".to_owned()),
    }
    match &completeness_result {
        Ok(summary) if summary.valid => {}
        Ok(_) => reasons.push("completeness_invalid".to_owned()),
        Err(_) => reasons.push("completeness_unreadable".to_owned()),
    }
    match &verify_result {
        Ok(summary) if summary.valid => {}
        Ok(_) => reasons.push("sbe_invalid".to_owned()),
        Err(_) => reasons.push("sbe_unreadable".to_owned()),
    }
    if !decoy_absent {
        reasons.push("decoy_present".to_owned());
    }
    if !journal_complete {
        reasons.push("journal_incomplete".to_owned());
    }
    if let Some(unexplained) = ledger.unexplained_drops
        && unexplained > 0
    {
        reasons.push("unexplained_drops".to_owned());
    }
    if ledger.unexplained_drops.is_none() {
        reasons.push("event_accounting_unavailable".to_owned());
    }
    if let Some(shortfall) = ledger.application_shortfall
        && shortfall > 0
    {
        reasons.push("application_shortfall".to_owned());
    }
    if let Some(excess) = ledger.over_applied
        && excess > 0
    {
        // Every logical event is published once, so an applied count above
        // the expected ledger is a duplicate application. Both names are
        // recorded: `over_application` states the accounting defect and
        // `duplicate_application` states the mechanism the audit names.
        reasons.push("over_application".to_owned());
        reasons.push("duplicate_application".to_owned());
    }
    if let Some(gap) = ledger.persistence_gap
        && gap > 0
    {
        reasons.push("persistence_gap".to_owned());
    }
    if let Some(excess) = ledger.persistence_excess
        && excess > 0
    {
        reasons.push("persistence_excess".to_owned());
    }
    if transmit.queue_saturated > 0 {
        reasons.push("mock_throughput_limited".to_owned());
        reasons.push("mock_queue_saturation".to_owned());
    }
    if let Some(error) = &sbe_scan_error {
        reasons.push(format!("sbe_scan_error: {error}"));
    }
    if anchor_plateau {
        reasons.push("anchor_plateau".to_owned());
    } else if anchor_wait_expired {
        reasons.push("anchor_timeout".to_owned());
    }
    if !drain_completed {
        match &drain_evidence_error {
            Some(class) => reasons.push(class.clone()),
            None => reasons.push("drain_incomplete".to_owned()),
        }
    }

    let acceptance = if reasons.is_empty() {
        CellAcceptance::valid()
    } else {
        CellAcceptance::from_reasons(reasons)
    };

    CellOutcome {
        provenance: Some(provenance),
        acceptance,
        output: Some(CellOutput {
            writer_queue_high_water: report.writer.queue_high_water,
            report,
            coverage: coverage_result.ok(),
            coverage_error,
            completeness_error,
            verify_error,
            completeness_valid,
            verify_valid,
            anchored_time,
            harness_process_rss_sample_bytes: rss_sample,
            expected_symbols: expected_set,
            acknowledged_symbols: acknowledged_set,
            observed_symbols: observed,
            anchored_symbols: anchored,
            eligible_count: eligible.len(),
            decoy_absent,
            journal_complete,
            journaled_records,
            transmitted_frames: transmit.sent,
            events_published: plan.generated,
            applied_events,
            capacity_connections,
            anchor_wait_expired,
            drain_completed,
            drain_detail,
            sbe_deltas,
            sbe_snapshots,
            sbe_messages,
            incidents,
            ledger,
            mock: transmit,
        }),
    }
}

// ---------- manifest writer ----------

/// Canonical set SHA-256 (sorted, newline-separated, trailing newline) — the
/// same definition as `coverage.rs`.
#[must_use]
pub fn canonical_set_sha256(values: &BTreeSet<String>) -> String {
    let mut text = String::new();
    for value in values {
        text.push_str(value);
        text.push('\n');
    }
    sha256_hex(text.as_bytes())
}

fn harness_binary_sha256() -> Option<String> {
    let path = std::env::current_exe().ok()?;
    fs::read(&path).ok().map(|bytes| sha256_hex(&bytes))
}

/// The build profile of the running harness binary, derived from the actual
/// compilation — never hard-coded.
fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn target_triple() -> String {
    if let Ok(compilation) = std::process::Command::new("rustc").arg("-vV").output() {
        let text = String::from_utf8_lossy(&compilation.stdout);
        if let Some(host) = text.lines().find_map(|line| line.strip_prefix("host: ")) {
            return host.to_owned();
        }
    }
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("stress-{millis}-{}", std::process::id())
}

fn host_json() -> Value {
    let host = HostInfo::probe();
    json!({
        "os": host.os.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("uname -sr failed")),
        "kernel": host.kernel.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("uname -r failed")),
        "cpu_model": host.cpu_model.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("cpu model unavailable")),
        "cpu_count": host.cpu_count.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("available_parallelism failed")),
        "ram_mb": host.ram_mb.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("ram unavailable")),
        "fd_limit": host.fd_limit.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("ulimit -n unavailable")),
        "disk_free_bytes": host.disk_free_bytes.map(Value::from).unwrap_or_else(|| super::manifest::unavailable(host.disk_free_reason.clone().unwrap_or_else(|| "df unavailable".to_owned()).as_str())),
        "target_triple": target_triple(),
        "rustc_version": host.rustc_version.map(Value::from).unwrap_or_else(|| super::manifest::unavailable("rustc unavailable")),
        "clock_source": host.clock_source,
        "clock_resolution_ns": host.clock_resolution_ns.map(Value::from).unwrap_or_else(|| {
            super::manifest::unavailable(
                host.clock_resolution_reason
                    .as_deref()
                    .unwrap_or("clock resolution could not be measured"),
            )
        }),
    })
}

fn provenance_json(provenance: Option<&WorktreeProvenance>) -> Value {
    let Some(provenance) = provenance else {
        return json!({
            "tracked_clean": super::manifest::unavailable("provenance not captured"),
        });
    };
    if let Some(error) = &provenance.error {
        return json!({
            "commit": provenance.commit,
            "tracked_clean": super::manifest::unavailable(error),
            "untracked_file_count": Value::Null,
        });
    }
    json!({
        "commit": provenance.commit,
        "tracked_clean": provenance.tracked_clean,
        "tracked_modified_files": provenance.tracked_modified,
        "tracked_diff_sha256": provenance.tracked_diff_sha256,
        "untracked_file_count": provenance.untracked_count,
        "clean_run_required": require_clean_flag_from_env_note(),
    })
}

/// Whether the *last* preflight enforced cleanliness. The gate itself reads
/// the config/env at run time; manifests only record the environment default.
fn require_clean_flag_from_env_note() -> bool {
    std::env::var("STRESS_REQUIRE_CLEAN").is_ok_and(|v| v == "1")
}

/// Build one reference-matrix manifest document for a finished outcome.
pub fn build_cell_manifest(
    outcome: &CellOutcome,
    config: &CellConfig,
    cell_id: &str,
) -> Map<String, Value> {
    let host = host_json();
    let output = outcome.output.as_ref();
    let binary_sha256 = harness_binary_sha256();

    let mut document = Map::new();
    document.insert(
        "schema_version".to_owned(),
        json!(super::manifest::STRESS_MANIFEST_SCHEMA_VERSION),
    );
    document.insert("run_id".to_owned(), json!(run_id()));
    document.insert("cell_id".to_owned(), json!(cell_id));
    document.insert("execution_mode".to_owned(), json!("stress"));
    document.insert("evidence_class".to_owned(), json!("synthetic-loopback"));
    document.insert("run_status".to_owned(), json!(outcome.acceptance.status()));
    document.insert(
        "failure_class".to_owned(),
        outcome
            .acceptance
            .failure_class
            .clone()
            .map(Value::from)
            .unwrap_or(Value::Null),
    );
    document.insert(
        "acceptance_reasons".to_owned(),
        Value::Array(
            outcome
                .acceptance
                .reasons
                .iter()
                .cloned()
                .map(Value::from)
                .collect(),
        ),
    );
    document.insert(
        "generator_version".to_owned(),
        json!(super::universe::GENERATOR_VERSION),
    );
    document.insert(
        "prng_algorithm_version".to_owned(),
        json!(super::rng::PRNG_ALGORITHM_VERSION),
    );
    document.insert("scenario_version".to_owned(), json!(1));
    document.insert("build_profile".to_owned(), json!(build_profile()));
    document.insert("target_triple".to_owned(), json!(target_triple()));
    document.insert(
        "stress_harness_binary_sha256".to_owned(),
        binary_sha256
            .clone()
            .map(Value::from)
            .unwrap_or_else(|| super::manifest::unavailable("current_exe unreadable")),
    );
    document.insert(
        "collector_binary_sha256".to_owned(),
        binary_sha256
            .map(Value::from)
            .unwrap_or_else(|| super::manifest::unavailable("current_exe unreadable")),
    );
    document.insert("host".to_owned(), host);
    document.insert(
        "provenance".to_owned(),
        provenance_json(outcome.provenance.as_ref()),
    );
    document.insert(
        "source_commit".to_owned(),
        json!(
            outcome
                .provenance
                .as_ref()
                .and_then(|provenance| provenance.commit.clone())
                .unwrap_or_else(|| "unknown".to_owned())
        ),
    );
    let scenario = json!({
        "seed": config.seed,
        "symbols": config.symbols,
        "eligible_count": output.map(|o| o.eligible_count).unwrap_or(0),
        "events_published": output.map(|o| o.events_published).unwrap_or(0),
        "shards_requested": config.shards.unwrap_or(0),
        "shards_actual": output.and_then(|o| o.report.capacity.as_ref().map(|plan| plan.shards)).unwrap_or(0),
        "replicas_requested": config.replicas,
        "replicas_actual": output.and_then(|o| o.report.capacity.as_ref().map(|plan| plan.replicas)).unwrap_or(0),
        "lifecycle_replicas": config.lifecycle_replicas,
        "distribution": if config.hot_pair { "zipf_hot_pair" } else { "uniform" },
        "journal": config.journal,
        "failure_injection": config.fail_inject.map(|f| format!("{f:?}")),
    });
    document.insert(
        "configuration_sha256".to_owned(),
        Value::from(sha256_hex(scenario.to_string().as_bytes())),
    );
    document.insert("scenario".to_owned(), scenario);

    let desired_topology = json!({
        "shards": config.shards.unwrap_or(0),
        "replicas": config.replicas,
        "lifecycle_replicas": config.lifecycle_replicas,
        "writer_queue": config.writer_queue.unwrap_or(4_096),
    });
    let actual_topology = match output.and_then(|o| o.report.capacity.as_ref()) {
        Some(plan) => json!({
            "shards": plan.shards,
            "replicas": plan.replicas,
            "lifecycle_connections": plan.lifecycle_connections,
            "total_connections": plan.total_connections,
        }),
        None => super::manifest::unavailable("capacity plan unavailable"),
    };
    let capacity_plan = output
        .and_then(|o| o.report.capacity.as_ref())
        .and_then(|plan| serde_json::to_value(plan).ok())
        .unwrap_or_else(|| super::manifest::unavailable("capacity plan unavailable"));
    document.insert(
        "collector".to_owned(),
        json!({
            "desired_topology": desired_topology,
            "actual_topology": actual_topology,
            "capacity_plan": capacity_plan,
            "in_process": true,
        }),
    );

    document.insert(
        "counts".to_owned(),
        json!({
            "generated_events": output.map(|o| o.ledger.generated_events).unwrap_or(0),
            "transmitted_frames": output.map(|o| o.transmitted_frames),
            "applied_events": output.and_then(|o| o.applied_events),
            "writer_messages": output.map(|o| o.report.writer.messages),
            "writer_bytes": output.map(|o| o.report.writer.bytes),
            "sbe_messages": output.map(|o| o.sbe_messages),
            "sbe_deltas": output.map(|o| o.sbe_deltas),
            "sbe_snapshots": output.map(|o| o.sbe_snapshots),
            "journaled_frames": match (config.journal, output.and_then(|o| o.journaled_records)) {
                (false, _) => Value::Null,
                (true, Some(count)) => json!(count),
                (true, None) => super::manifest::unavailable("journal verification unavailable"),
            },
            "unexplained_drops": output.map(|o| o.ledger.unexplained_drops),
            "persistence_gap": output.map(|o| o.ledger.persistence_gap),
            "mock_queue_high_water": output.map(|o| o.mock.queue_high_water),
            "mock_queue_saturated": output.map(|o| o.mock.queue_saturated),
            "writer_queue_high_water": output.map(|o| o.writer_queue_high_water),
        }),
    );

    // Transport-stage distinction required by the design: enqueued vs
    // socket-written vs still in flight, plus every accounted drop.
    document.insert(
        "mock_transport".to_owned(),
        json!({
            "outbound_capacity": output.map(|o| o.mock.outbound_capacity),
            "enqueued": output.map(|o| o.mock.enqueued),
            "socket_written": output.map(|o| o.mock.sent),
            "in_flight_at_end": output.map(|o| o.mock.in_flight),
            "snapshots_sent": output.map(|o| o.mock.snapshots_sent),
            "deltas_sent": output.map(|o| o.mock.deltas_sent),
            "acks_sent": output.map(|o| o.mock.acks_sent),
            "queue_high_water": output.map(|o| o.mock.queue_high_water),
            "queue_saturated": output.map(|o| o.mock.queue_saturated),
            "injected_pre_wire_drops": output.map(|o| o.mock.injected_pre_wire_drops),
        }),
    );

    let queue_high_water = output
        .map(|output| output.mock.queue_high_water)
        .unwrap_or(0);
    let queue_capacity = output
        .map(|output| output.mock.outbound_capacity)
        .unwrap_or(0);
    let pressure_ppm = queue_high_water
        .saturating_mul(1_000_000)
        .checked_div(queue_capacity)
        .unwrap_or(0);
    document.insert(
        "harness".to_owned(),
        json!({
            "mock_queue_high_water": output.map(|output| output.mock.queue_high_water),
            "mock_queue_capacity": output.map(|output| output.mock.outbound_capacity),
            "mock_queue_pressure_ppm": pressure_ppm,
            "throughput_limited": output
                .map(|output| output.mock.queue_saturated > 0 || pressure_ppm >= 900_000)
                .unwrap_or(false),
        }),
    );

    document.insert("event_ledger".to_owned(), json!({
        "generated_events": output.map(|o| o.ledger.generated_events).unwrap_or(0),
        "expected_applied": output.map(|o| o.ledger.expected_applied).unwrap_or(0),
        "applied_events": output.and_then(|o| o.ledger.applied_events),
        "sbe_deltas": output.and_then(|o| o.ledger.sbe_deltas),
        "journaled_records": match (config.journal, output.and_then(|o| o.ledger.journaled_records)) {
            (false, _) => Value::Null,
            (true, count) => count.map(Value::from).unwrap_or_else(|| {
                super::manifest::unavailable("journal verification unavailable")
            }),
        },
        "injected_pre_wire_drops": output.map(|o| o.ledger.injected_pre_wire_drops).unwrap_or(0),
        "suppressed_replica_duplicates": output
            .and_then(|o| o.ledger.suppressed_replica_duplicates),
        "unexplained_drops": output.and_then(|o| o.ledger.unexplained_drops),
        "application_shortfall": output.and_then(|o| o.ledger.application_shortfall),
        "over_applied": output.and_then(|o| o.ledger.over_applied),
        "persistence_gap": output.and_then(|o| o.ledger.persistence_gap),
        "persistence_excess": output.and_then(|o| o.ledger.persistence_excess),
        "reconciled": output.map(|o| o.ledger.reconciled()).unwrap_or(false),
    }));

    let expected_set_sha256 = match output {
        Some(o) => match &o.coverage {
            Some(c) => Value::from(c.expected_set_sha256.clone()),
            None => Value::from(canonical_set_sha256(&o.expected_symbols)),
        },
        None => super::manifest::unavailable("capture not produced"),
    };
    let acknowledged_set_sha256 = output
        .map(|o| Value::from(canonical_set_sha256(&o.acknowledged_symbols)))
        .unwrap_or_else(|| super::manifest::unavailable("capture not produced"));
    let anchored_set_sha256 = output
        .map(|o| Value::from(canonical_set_sha256(&o.anchored_symbols)))
        .unwrap_or_else(|| super::manifest::unavailable("capture not produced"));
    let observed_set_sha256 = output
        .map(|o| Value::from(canonical_set_sha256(&o.observed_symbols)))
        .unwrap_or_else(|| super::manifest::unavailable("capture not produced"));

    document.insert("coverage".to_owned(), json!({
        "expected": output.map(|o| o.coverage.as_ref().map(|c| c.expected)),
        "acknowledged": output.map(|o| o.coverage.as_ref().map(|c| c.acknowledged)),
        "anchored": output.map(|o| o.coverage.as_ref().map(|c| c.anchored)),
        "observed": output.map(|o| o.coverage.as_ref().map(|c| c.observed)),
        "missing_ack": output.and_then(|o| o.coverage.as_ref().map(|c| c.missing_ack.count)),
        "missing_anchor": output.and_then(|o| o.coverage.as_ref().map(|c| c.missing_anchor.count)),
        "expected_set_sha256": expected_set_sha256,
        "acknowledged_set_sha256": acknowledged_set_sha256,
        "anchored_set_sha256": anchored_set_sha256,
        "observed_set_sha256": observed_set_sha256,
        "decoys_absent": output.map(|o| o.decoy_absent),
        "coverage_valid": output.map(|o| o.coverage.as_ref().map(|c| c.valid).unwrap_or(false)),
        "per_epoch": [],
    }));

    document.insert("verify".to_owned(), json!({
        "verify_valid": output.map(|o| o.verify_valid),
        "completeness_valid": output.map(|o| o.completeness_valid),
        "coverage_valid": output.map(|o| o.coverage.as_ref().map(|c| c.valid).unwrap_or(false)),
        "verify_error": output.and_then(|o| o.verify_error.clone()).map(Value::from).unwrap_or(Value::Null),
        "completeness_error": output.and_then(|o| o.completeness_error.clone()).map(Value::from).unwrap_or(Value::Null),
        "coverage_error": output.and_then(|o| o.coverage_error.clone()).map(Value::from).unwrap_or(Value::Null),
        "journal_complete": output.map(|o| o.journal_complete),
        "journal_record_count": output.and_then(|o| o.journaled_records).map(Value::from).unwrap_or(Value::Null),
    }));

    document.insert("delivery".to_owned(), json!({
        "anchor_completed": !output.map(|o| o.anchor_wait_expired).unwrap_or(true),
        "anchor_ms": output.map(|o| o.anchored_time.as_millis() as u64),
        "drain_completed": output.map(|o| o.drain_completed).unwrap_or(false),
        "drain_detail": output.and_then(|o| o.drain_detail.clone()).map(Value::from).unwrap_or(Value::Null),
        "capacity_connections": output.map(|o| o.capacity_connections).unwrap_or(0),
    }));

    let incidents = output.map(|output| output.incidents);
    let disk_free = HostInfo::probe().disk_free_bytes;
    document.insert(
        "disk".to_owned(),
        json!({
            "free_bytes_before_start": disk_free
                .map(Value::from)
                .unwrap_or_else(|| super::manifest::unavailable("df unavailable")),
            "bytes_written": output
                .map(|output| output.report.writer.bytes)
                .map(Value::from)
                .unwrap_or_else(|| super::manifest::unavailable("writer report unavailable")),
            "disk_ceiling_bytes": super::manifest::unavailable(
                "STRESS_DISK_CEILING is not configured for this cell"
            ),
            "disk_ceiling_reached": false,
        }),
    );
    document.insert(
        "incidents".to_owned(),
        json!({
            "discontinuity_incidents": incidents
                .and_then(|incidents| incidents.discontinuity_incidents)
                .map(Value::from)
                .unwrap_or_else(|| super::manifest::unavailable(
                    "collector_discontinuity_incidents_total unavailable"
                )),
            "unquantifiable_gaps": incidents
                .and_then(|incidents| incidents.unquantifiable_gaps)
                .map(Value::from)
                .unwrap_or_else(|| super::manifest::unavailable(
                    "collector_unquantifiable_gaps_total unavailable"
                )),
            "queue_saturated_events": output.map(|output| output.mock.queue_saturated),
            "injected_pre_wire_drops": output.map(|output| output.mock.injected_pre_wire_drops),
            "writer_errors": output.map(|output| output.report.writer.errors),
        }),
    );

    document.insert("performance".to_owned(), json!({
        "msgs_per_sec_p50": super::manifest::unavailable("binned throughput lands with the reference matrix"),
        "msgs_per_sec_p95": super::manifest::unavailable("binned throughput lands with the reference matrix"),
        "msgs_per_sec_p99": super::manifest::unavailable("binned throughput lands with the reference matrix"),
        "msgs_per_sec_max": super::manifest::unavailable("binned throughput lands with the reference matrix"),
        "harness_process_rss_sample_bytes": output
            .and_then(|o| o.harness_process_rss_sample_bytes)
            .map(Value::from)
            .unwrap_or_else(|| super::manifest::unavailable("rss not readable on this host")),
        "snapshot_anchor_ms": output.map(|o| o.anchored_time.as_millis() as u64).unwrap_or(0),
        "writer_queue_high_water_mark": output
            .map(|o| o.writer_queue_high_water)
            .map(Value::from)
            .unwrap_or_else(|| super::manifest::unavailable("writer report unavailable")),
        "cpu_seconds": super::manifest::unavailable("cpu_seconds not measured (no libc dependency)"),
    }));

    document
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::net::TcpListener;

    use super::*;

    fn base() -> DrainBaselines {
        DrainBaselines {
            deltas_emitted: 0.0,
            writer_messages: 0.0,
            writer_errors: 0.0,
        }
    }

    #[test]
    fn absent_metric_is_not_zero() {
        let sample = parse_metrics("collector_deltas_emitted_total 5\n");
        assert_eq!(
            sample.read("collector_writer_messages_total"),
            MetricRead::Absent
        );
        let error = sample
            .required("collector_writer_messages_total")
            .expect_err("absence must be an error");
        assert!(error.contains("absent"), "{error}");
    }

    #[test]
    fn malformed_metric_is_not_zero() {
        let sample = parse_metrics("collector_writer_messages_total not-a-number\n");
        assert_eq!(
            sample.read("collector_writer_messages_total"),
            MetricRead::Malformed
        );
        let error = sample
            .required("collector_writer_messages_total")
            .expect_err("malformed must be an error");
        assert!(error.contains("malformed"), "{error}");
    }

    #[test]
    fn labeled_families_fold_to_the_family_name() {
        let sample = parse_metrics("collector_deltas_emitted_total{shard=\"0\"} 7\n");
        assert_eq!(
            sample.read("collector_deltas_emitted_total"),
            MetricRead::Present(7.0)
        );
    }

    #[test]
    fn barrier_rejects_a_missing_mandatory_metric() {
        let sample = parse_metrics(
            "collector_deltas_emitted_total 10\n\
             collector_writer_queue_depth 0\n\
             collector_writer_errors_total 0\n",
        );
        match evaluate_barrier(&sample, &base(), 10.0, 0) {
            BarrierState::Unavailable(message) => {
                assert!(
                    message.contains("collector_writer_messages_total"),
                    "{message}"
                );
            }
            other => panic!("expected unavailable, got {other:?}"),
        }
    }

    #[test]
    fn barrier_rejects_a_malformed_mandatory_metric() {
        let sample = parse_metrics(
            "collector_deltas_emitted_total 10\n\
             collector_writer_messages_total nope\n\
             collector_writer_queue_depth 0\n\
             collector_writer_errors_total 0\n",
        );
        assert!(matches!(
            evaluate_barrier(&sample, &base(), 10.0, 0),
            BarrierState::Unavailable(_)
        ));
    }

    #[test]
    fn barrier_distinguishes_a_counter_reset() {
        let base = DrainBaselines {
            deltas_emitted: 100.0,
            writer_messages: 100.0,
            writer_errors: 0.0,
        };
        let sample = parse_metrics(
            "collector_deltas_emitted_total 5\n\
             collector_writer_messages_total 5\n\
             collector_writer_queue_depth 0\n\
             collector_writer_errors_total 0\n",
        );
        assert_eq!(
            evaluate_barrier(&sample, &base, 10.0, 0),
            BarrierState::CounterReset("collector_deltas_emitted_total")
        );
    }

    #[test]
    fn barrier_satisfies_only_when_every_metric_agrees() {
        let sample = parse_metrics(
            "collector_deltas_emitted_total 10\n\
             collector_writer_messages_total 10\n\
             collector_writer_queue_depth 0\n\
             collector_writer_errors_total 0\n",
        );
        assert_eq!(
            evaluate_barrier(&sample, &base(), 10.0, 0),
            BarrierState::Satisfied
        );
        assert_eq!(
            evaluate_barrier(&sample, &base(), 10.0, 1),
            BarrierState::Waiting
        );
    }

    /// Serve a scripted sequence of `/metrics` bodies, repeating the last body
    /// once the script is exhausted.
    async fn serve_metrics(bodies: Vec<String>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub metrics server");
        let address = listener.local_addr().expect("stub address");
        let bodies = Arc::new(bodies);
        tokio::spawn(async move {
            let mut index = 0usize;
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = bodies
                    .get(index)
                    .or_else(|| bodies.last())
                    .cloned()
                    .unwrap_or_default();
                index = index.saturating_add(1);
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        address
    }

    const DELAYED_BODY: &str = "collector_deltas_emitted_total 10\n\
        collector_writer_queue_depth 0\n\
        collector_writer_errors_total 0\n";
    const COMPLETE_BODY: &str = "collector_deltas_emitted_total 10\n\
        collector_writer_messages_total 20\n\
        collector_writer_queue_depth 0\n\
        collector_writer_errors_total 0\n";
    const MALFORMED_BODY: &str = "collector_deltas_emitted_total 10\n\
        collector_writer_messages_total nope\n\
        collector_writer_queue_depth 0\n\
        collector_writer_errors_total 0\n";

    #[tokio::test]
    async fn baseline_fails_closed_on_a_missing_metric() {
        let address = serve_metrics(vec![DELAYED_BODY.to_owned()]).await;
        let error = capture_baseline_until(address, Instant::now() + Duration::from_millis(150))
            .await
            .expect_err("a persistent absence must fail the baseline");
        assert!(error.contains("collector_writer_messages_total"), "{error}");
    }

    #[tokio::test]
    async fn baseline_fails_closed_on_a_malformed_metric() {
        let address = serve_metrics(vec![MALFORMED_BODY.to_owned()]).await;
        let error = capture_baseline_until(address, Instant::now() + Duration::from_millis(150))
            .await
            .expect_err("a malformed metric must fail the baseline");
        assert!(error.contains("malformed"), "{error}");
    }

    #[tokio::test]
    async fn baseline_recovers_from_a_delayed_metric() {
        let address = serve_metrics(vec![
            DELAYED_BODY.to_owned(),
            COMPLETE_BODY.to_owned(),
            COMPLETE_BODY.to_owned(),
        ])
        .await;
        let baseline = capture_baseline_until(address, Instant::now() + Duration::from_secs(2))
            .await
            .expect("a metric that appears later must be accepted");
        assert_eq!(baseline.writer_messages, 20.0);
        assert_eq!(baseline.deltas_emitted, 10.0);
    }
}

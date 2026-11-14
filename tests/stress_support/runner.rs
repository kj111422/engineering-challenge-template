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
    #[allow(dead_code)]
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
    /// `generated - applied`, derived; `None` when applied is unobservable.
    pub unexplained_drops: Option<u64>,
    /// `applied - sbe_deltas`, derived; `None` when either side is missing.
    pub persistence_gap: Option<u64>,
}

impl EventLedger {
    /// A ledger reconciles only when every stage is observed and every
    /// derived gap is exactly zero.
    #[must_use]
    pub fn reconciled(&self) -> bool {
        self.unexplained_drops == Some(0) && self.persistence_gap == Some(0)
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

/// Scrape every plain metric sample from the collector's `/metrics` endpoint.
/// Labeled families are folded to their family name; samples that do not
/// parse as floats are ignored.
async fn scrape_metrics(address: SocketAddr) -> Option<HashMap<String, f64>> {
    let mut stream = TcpStream::connect(address).await.ok()?;
    let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await.ok()?;
    let body = response.split_once("\r\n\r\n")?.1;
    let mut metrics = HashMap::new();
    for line in body.lines() {
        let Some((name, value)) = line.split_once(' ') else {
            continue;
        };
        let name = name.split('{').next().unwrap_or(name);
        if let Ok(parsed) = value.parse::<f64>() {
            metrics.insert(name.to_owned(), parsed);
        }
    }
    Some(metrics)
}

async fn scrape_metric(address: SocketAddr, metric: &str) -> Option<f64> {
    scrape_metrics(address).await?.get(metric).copied()
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
        Some(FailInject::ShutdownBeforeDrain) => {}
        None => {}
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
///   the wire in replica-pair order, so `count / replicas` events vanish.
struct PublishPlan {
    generated: usize,
    expected_applied: u64,
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
    PublishPlan {
        generated,
        expected_applied,
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

async fn capture_baseline(address: SocketAddr) -> Option<DrainBaselines> {
    let deadline = Instant::now() + BASELINE_LIMIT;
    let mut previous: Option<(f64, f64)> = None;
    loop {
        let metrics = scrape_metrics(address).await?;
        let messages = metrics
            .get("collector_writer_messages_total")
            .copied()
            .unwrap_or(0.0);
        let depth = metrics
            .get("collector_writer_queue_depth")
            .copied()
            .unwrap_or(f64::INFINITY);
        if let Some((prev_messages, prev_depth)) = previous
            && prev_depth == 0.0
            && depth == 0.0
            && prev_messages == messages
        {
            return Some(DrainBaselines {
                deltas_emitted: metrics
                    .get("collector_deltas_emitted_total")
                    .copied()
                    .unwrap_or(0.0),
                writer_messages: messages,
                writer_errors: metrics
                    .get("collector_writer_errors_total")
                    .copied()
                    .unwrap_or(0.0),
            });
        }
        previous = Some((messages, depth));
        if Instant::now() >= deadline {
            return None;
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
    match manifest_out {
        Some(out_dir) => write_cell_manifest(outcome, config, out_dir),
        None => outcome,
    }
}

/// Write the cell manifest (and index entry when the cell is healthy).
/// The index is validated *before* any artifact is written: a malformed or
/// unreadable index flips the outcome to `manifest_index_error` and leaves
/// every existing byte untouched — an unverifiable artifact is never
/// presented as success.
fn write_cell_manifest(outcome: CellOutcome, config: &CellConfig, out_dir: &Path) -> CellOutcome {
    if let Err(error) = super::manifest::validate_index(&out_dir.join("index.json")) {
        return CellOutcome {
            provenance: outcome.provenance,
            output: outcome.output,
            acceptance: CellAcceptance::from_reasons(vec![
                "manifest_index_error".to_owned(),
                format!("{error}"),
            ]),
        };
    }
    let acceptance_valid = outcome.acceptance.valid;
    let status = outcome.acceptance.status();
    let cell_id = if acceptance_valid {
        format!("seed-{:04x}-{}-uniform", config.seed, config.symbols)
    } else {
        format!(
            "{status}-seed-{:04x}-{}-{}",
            config.seed,
            config.symbols,
            outcome
                .acceptance
                .failure_class
                .as_deref()
                .unwrap_or("unknown")
        )
    };
    let manifest = build_cell_manifest(&outcome, config, &cell_id);
    if let Err(error) = super::manifest::write_atomic(
        &out_dir.join(format!("{cell_id}.json")),
        &Value::Object(manifest),
    ) {
        return CellOutcome {
            provenance: outcome.provenance,
            output: outcome.output,
            acceptance: CellAcceptance::from_reasons(vec![
                "manifest_write_error".to_owned(),
                format!("{error}"),
            ]),
        };
    }
    let entry = json!({
        "path": format!("{cell_id}.json"),
        "run_status": status,
        "seed": config.seed,
        "symbols": config.symbols,
    });
    if let Err(error) = super::manifest::upsert_cell(&out_dir.join("index.json"), &cell_id, entry) {
        // Artifact cannot be trusted; fail closed but keep the cell output.
        return CellOutcome {
            provenance: outcome.provenance,
            output: outcome.output,
            acceptance: CellAcceptance::from_reasons(vec![
                "manifest_index_error".to_owned(),
                format!("{error}"),
            ]),
        };
    }
    outcome
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
    // is offered so the drain barrier can attribute every message.
    let baseline = if matches!(config.fail_inject, Some(FailInject::ShutdownBeforeDrain)) {
        None
    } else {
        match metrics_addr {
            Some(addr) => capture_baseline(addr).await,
            None => None,
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
    // run as incomplete; no fixed sleep is involved.
    let mut drain_completed = false;
    let mut drain_detail: Option<String> = None;
    let mut last_deltas = baseline.map(|base| base.deltas_emitted);
    if matches!(config.fail_inject, Some(FailInject::ShutdownBeforeDrain)) {
        drain_detail = Some("shutdown requested before the drain barrier".to_owned());
    } else if baseline.is_none() {
        drain_detail = Some("writer baseline unavailable".to_owned());
    } else if let (Some(addr), Some(base)) = (metrics_addr, baseline) {
        let deadline = Instant::now() + drain_limit;
        loop {
            if let Some(metrics) = scrape_metrics(addr).await {
                let deltas = metrics
                    .get("collector_deltas_emitted_total")
                    .copied()
                    .unwrap_or(0.0);
                let messages = metrics
                    .get("collector_writer_messages_total")
                    .copied()
                    .unwrap_or(0.0);
                let depth = metrics
                    .get("collector_writer_queue_depth")
                    .copied()
                    .unwrap_or(f64::INFINITY);
                let errors = metrics
                    .get("collector_writer_errors_total")
                    .copied()
                    .unwrap_or(f64::INFINITY);
                last_deltas = Some(deltas);
                let expected = plan.expected_applied as f64;
                if mock.state().in_flight() == 0
                    && deltas >= base.deltas_emitted + expected
                    && messages >= base.writer_messages + expected
                    && depth == 0.0
                    && errors == base.writer_errors
                {
                    drain_completed = true;
                    break;
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            sleep(METRICS_POLL).await;
        }
        if !drain_completed {
            drain_detail =
                Some("drain deadline exceeded before every frame was accounted for".to_owned());
        }
    }

    // Last observation of the applied counter before shutdown.
    if let Some(addr) = metrics_addr
        && let Some(deltas) = scrape_metric(addr, "collector_deltas_emitted_total").await
    {
        last_deltas = Some(deltas);
    }

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
        unexplained_drops: applied_events
            .map(|applied| (plan.generated as u64).saturating_sub(applied)),
        persistence_gap: match (applied_events, sbe_deltas_observed) {
            (Some(applied), Some(sbe)) => Some(applied.saturating_sub(sbe)),
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
    if let Some(gap) = ledger.persistence_gap
        && gap > 0
    {
        reasons.push("persistence_gap".to_owned());
    }
    if transmit.queue_saturated > 0 {
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
        reasons.push("drain_incomplete".to_owned());
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

    let mut document = Map::new();
    document.insert("schema_version".to_owned(), json!(1));
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
        harness_binary_sha256()
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
    document.insert("scenario".to_owned(), json!({
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
    }));

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
        "unexplained_drops": output.and_then(|o| o.ledger.unexplained_drops),
        "persistence_gap": output.and_then(|o| o.ledger.persistence_gap),
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

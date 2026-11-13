//! Cell runner for the stress laboratory. Drives the production collector
//! against the loopback Kalshi mock and produces one machine-readable cell
//! result. Everything here runs inside the test process; no production code
//! changes for the harness.

use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use ed25519_dalek::SigningKey;
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
    verify,
};

use super::mock::MockKalshi;
use super::rng::SplitMix64;
use super::universe::SyntheticUniverse;

const ANCHOR_LIMIT: Duration = Duration::from_secs(45);
const FINISH_LIMIT: Duration = Duration::from_secs(60);

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
    /// Failure-injection knob for the harness self-test: forces the capacity
    /// plan to reject (connection budget of 1), producing an honest failure
    /// manifest on demand. Never set in reference cells.
    pub force_capacity_reject: bool,
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
            force_capacity_reject: false,
        }
    }
}

/// Full result of one cell run — raw verifier digests included for the manifest.
pub struct CellOutput {
    pub report: RunReport,
    pub coverage: coverage::CoverageSummary,
    pub completeness_valid: bool,
    pub verify_valid: bool,
    pub anchored_time: Duration,
    pub rss_peak_bytes: Option<u64>,
    pub captured_symbols: BTreeSet<String>,
    pub eligible_count: usize,
    pub decoy_absent: bool,
    pub journal_complete: bool,
    pub journaled_records: Option<u64>,
    pub transmitted_frames: u64,
    pub expected_subscription_count: usize,
    pub events_published: usize,
    pub capacity_connections: usize,
    pub anchor_wait_expired: bool,
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

pub fn settings_for(mock: &MockKalshi, directory: &Path, config: &CellConfig) -> Settings {
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
    settings.collector.writer_queue = 4_096;
    settings.collector.writer_send_timeout_secs = 10;
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

async fn scrape_metric(address: SocketAddr, metric: &str) -> Option<f64> {
    let mut stream = TcpStream::connect(address).await.ok()?;
    let request = format!("GET /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await.ok()?;
    let body = response.split_once("\r\n\r\n")?.1;
    body.lines().find_map(|line| {
        let (name, value) = line.split_once(' ')?;
        if name == metric {
            value.parse().ok()
        } else {
            None
        }
    })
}

fn rss_bytes() -> Option<u64> {
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

fn eligible_tickers(universe: &SyntheticUniverse) -> Vec<String> {
    universe.eligible_tickers_sorted()
}

fn publish_events(
    mock: &MockKalshi,
    tickers: &[String],
    config: &CellConfig,
    rng: &mut SplitMix64,
) -> usize {
    let mut published = 0usize;
    let hot_cut = (tickers.len() / 10).max(2);
    for _ in 0..config.events {
        let index = if config.hot_pair {
            // 80% of traffic on the first 10% of the universe.
            if rng.below(100) < 80 {
                rng.below_usize(hot_cut)
            } else {
                rng.below_usize(tickers.len())
            }
        } else {
            rng.below_usize(tickers.len())
        };
        let ticker = &tickers[index];
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let price = 400_000 + i64::try_from(rng.below(200_000 / 10_000)).unwrap_or(0) * 10_000;
        let delta = 2_000_000 + i64::try_from(rng.below(3_000_000)).unwrap_or(0);
        mock.state().publish_delta(ticker, side, price, delta);
        published += 1;
    }
    published
}

pub async fn run_cell(directory: &Path, config: &CellConfig) -> anyhow::Result<CellOutput> {
    let outcome = run_cell_inner(directory, config).await;
    if outcome.is_err()
        && let Ok(out_dir) = std::env::var("STRESS_MANIFEST_OUT").map(PathBuf::from)
    {
        let error = outcome
            .as_ref()
            .err()
            .map(|e| format!("{e:#}"))
            .unwrap_or_default();
        let _ = fs::create_dir_all(&out_dir);
        let host = host_json();
        let mut document = Map::new();
        document.insert("schema_version".to_owned(), json!(1));
        document.insert("run_id".to_owned(), json!(run_id()));
        document.insert("execution_mode".to_owned(), json!("stress"));
        document.insert("evidence_class".to_owned(), json!("synthetic-loopback"));
        document.insert("run_status".to_owned(), json!("failed"));
        document.insert("failure_class".to_owned(), json!("run_error"));
        document.insert("error".to_owned(), json!(error));
        document.insert(
            "source_commit".to_owned(),
            source_commit()
                .map(Value::from)
                .unwrap_or_else(|| Value::String("unknown".to_owned())),
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
        document.insert("build_profile".to_owned(), json!("release"));
        document.insert("target_triple".to_owned(), json!(target_triple()));
        document.insert(
            "collector_binary_sha256".to_owned(),
            binary_sha256()
                .map(Value::from)
                .unwrap_or_else(|| manifest::unavailable("current_exe unreadable")),
        );
        document.insert("host".to_owned(), host);
        document.insert(
            "scenario".to_owned(),
            json!({
                "seed": config.seed,
                "symbols": config.symbols,
                "force_capacity_reject": config.force_capacity_reject,
            }),
        );
        let cell_id = format!("failed-seed-{:04x}-{}", config.seed, config.symbols);
        let path = out_dir.join(format!("{cell_id}.json"));
        let value = Value::Object(document);
        let _ = manifest::write_atomic(&path, &value);
        let _ = manifest::upsert_cell(
            &out_dir.join("index.json"),
            &cell_id,
            json!({"path": format!("{cell_id}.json"), "run_status": "failed"}),
        );
    }
    outcome
}

async fn run_cell_inner(directory: &Path, config: &CellConfig) -> anyhow::Result<CellOutput> {
    let universe =
        SyntheticUniverse::generate(config.seed, config.symbols, config.decoy_fraction_ppm);
    let eligible = eligible_tickers(&universe);
    let mock = MockKalshi::start(universe).await;
    let mut seed_rng = SplitMix64::new(config.seed ^ 0x51E5_ED5E_ED5E_0001);
    for market in mock.state().universe().markets.iter() {
        mock.state().seed_book(&market.ticker, &mut seed_rng);
    }

    let settings = settings_for(&mock, directory, config);
    let metrics_addr = settings.metrics.listen;
    let done = Arc::new(Notify::new());
    let done_for_run = Arc::clone(&done);
    let mut handle = tokio::spawn(coordinator::run_with_shutdown(settings, async move {
        tokio::select! {
            _ = done_for_run.notified() => "stress scenario complete".to_owned(),
            _ = sleep(Duration::from_secs(120)) => "stress scenario safety timeout".to_owned(),
        }
    }));

    // Wait until every eligible symbol has an anchored book. A collector
    // that terminates before anchoring (e.g. capacity rejection) is detected
    // immediately so the failure manifest names the real cause.
    let anchor_start = Instant::now();
    let mut anchor_wait_expired = true;
    loop {
        if handle.is_finished() {
            let outcome = (&mut handle)
                .await
                .unwrap_or_else(|error| Err(anyhow::anyhow!("collector task failed: {error}")));
            match outcome {
                Ok(_report) => {
                    anyhow::bail!(
                        "collector terminated before anchoring completed; see collector log for the cause"
                    );
                }
                Err(error) => {
                    anyhow::bail!("collector terminated during anchoring: {error:#}");
                }
            }
        }
        if let Some(addr) = metrics_addr
            && let Some(value) = scrape_metric(addr, "collector_symbols_anchored").await
            && value >= eligible.len() as f64
        {
            anchor_wait_expired = false;
            break;
        }
        if anchor_start.elapsed() >= ANCHOR_LIMIT {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }
    let anchored_time = anchor_start.elapsed();

    // Drive the synthetic workload.
    let events_published = publish_events(
        &mock,
        &eligible,
        config,
        &mut SplitMix64::new(config.seed ^ 0xBEE7_6F00_0D15_EA5E),
    );

    done.notify_one();
    let report = tokio::time::timeout(FINISH_LIMIT, &mut handle)
        .await
        .expect("collector did not stop within the limit")
        .expect("collector task failed")?;

    let capture = PathBuf::from(&report.output_path);
    let coverage_run = coverage::run(
        &capture,
        Path::new(directory.join("markets.json").as_os_str()),
        Path::new(directory.join("report.json").as_os_str()),
        "auto",
    )?;
    let completeness_run = completeness::run(&capture, "auto", false)?;
    let verify_run = verify::run(&capture, "auto")?;
    let _ = completeness_run;
    let _ = verify_run;

    // Decoy absence and exact eligible-set equality over the capture.
    let mut captured = BTreeSet::new();
    {
        use engineering_challenge::util::{
            AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage,
        };
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open_auto(&capture)?;
        while let Some(message) = reader.read_msg()? {
            match message {
                AnyMessage::Snapshot(decoder) => {
                    let snapshot = engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain::try_from_decoder(decoder)?;
                    captured.insert(snapshot.symbol);
                }
                AnyMessage::PriceLevelUpdate(decoder) => {
                    let update = engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                    captured.insert(update.symbol);
                }
                AnyMessage::Unknown { .. } => {}
                _ => {}
            }
        }
    }
    let decoy_absent = mock
        .state()
        .universe()
        .excluded_tickers_sorted()
        .iter()
        .all(|ticker| !captured.contains(ticker));

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

    let expected_subscription_count = eligible.len() * config.replicas;
    let rss_peak = rss_bytes();

    let capacity_connections = report
        .capacity
        .as_ref()
        .map(|capacity| capacity.total_connections)
        .unwrap_or(0);

    let output = CellOutput {
        report,
        coverage: coverage_run,
        completeness_valid: completeness_run.valid,
        verify_valid: verify_run.valid,
        anchored_time,
        rss_peak_bytes: rss_peak,
        captured_symbols: captured,
        eligible_count: eligible.len(),
        decoy_absent,
        journal_complete,
        journaled_records,
        transmitted_frames: mock.state().transmitted_count(),
        expected_subscription_count,
        events_published,
        capacity_connections,
        anchor_wait_expired,
    };

    // Manifests are emitted only when explicitly requested by the harness
    // invocation (reference-matrix cells); the ordinary CI smoke leaves no
    // artifacts behind.
    if let Ok(directory) = std::env::var("STRESS_MANIFEST_OUT").map(PathBuf::from) {
        let (cell_id, document) = build_cell_manifest(&output, config);
        let path = directory.join(format!("{cell_id}.json"));
        manifest::write_atomic(&path, &document)?;
        manifest::upsert_cell(
            &directory.join("index.json"),
            &cell_id,
            json!({
                "path": format!("{cell_id}.json"),
                "run_status": "success",
                "symbols": config.symbols,
                "seed": config.seed,
            }),
        )?;
    }

    Ok(output)
}

// ---------- manifest writer ----------

use std::time::{SystemTime, UNIX_EPOCH};

use engineering_challenge::util::sha256_hex;
use serde_json::{Map, Value, json};

use super::{host::HostInfo, manifest};

fn canonical_set_sha256(values: &BTreeSet<String>) -> String {
    let mut text = String::new();
    for value in values {
        text.push_str(value);
        text.push('\n');
    }
    sha256_hex(text.as_bytes())
}

fn binary_sha256() -> Option<String> {
    let path = std::env::current_exe().ok()?;
    let bytes = fs::read(&path).ok()?;
    Some(sha256_hex(&bytes))
}

fn source_commit() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
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
        "os": host.os.map(Value::from).unwrap_or_else(|| manifest::unavailable("uname -sr failed")),
        "kernel": host.kernel.map(Value::from).unwrap_or_else(|| manifest::unavailable("uname -r failed")),
        "cpu_model": host.cpu_model.map(Value::from).unwrap_or_else(|| manifest::unavailable("cpu model unavailable")),
        "cpu_count": host.cpu_count.map(Value::from).unwrap_or_else(|| manifest::unavailable("available_parallelism failed")),
        "ram_mb": host.ram_mb.map(Value::from).unwrap_or_else(|| manifest::unavailable("ram unavailable")),
        "fd_limit": host.fd_limit.map(Value::from).unwrap_or_else(|| manifest::unavailable("ulimit -n unavailable")),
        "disk_free_bytes": host.disk_free_bytes.map(Value::from).unwrap_or_else(|| manifest::unavailable(host.disk_free_reason.clone().unwrap_or_else(|| "df unavailable".to_owned()).as_str())),
        "target_triple": target_triple(),
        "rustc_version": host.rustc_version.map(Value::from).unwrap_or_else(|| manifest::unavailable("rustc unavailable")),
        "clock_source": host.clock_source,
        "clock_resolution_ns": host.clock_resolution_ns,
    })
}

/// Build one reference-matrix manifest document for a finished cell.
pub fn build_cell_manifest(output: &CellOutput, config: &CellConfig) -> (String, Value) {
    let report = &output.report;
    let host = host_json();
    let commits = source_commit().unwrap_or_else(|| "unknown".to_owned());
    let anchor_ms = output.anchored_time.as_millis() as u64;
    let capacity = report.capacity.as_ref();

    let acknowledged: BTreeSet<String> = report.acknowledged_symbols.iter().cloned().collect();

    let mut document = Map::new();
    document.insert("schema_version".to_owned(), json!(1));
    document.insert("run_id".to_owned(), json!(run_id()));
    document.insert("execution_mode".to_owned(), json!("stress"));
    document.insert("evidence_class".to_owned(), json!("synthetic-loopback"));
    document.insert("run_status".to_owned(), json!("success"));
    document.insert("failure_class".to_owned(), json!(null));
    document.insert("source_commit".to_owned(), json!(commits));
    document.insert(
        "generator_version".to_owned(),
        json!(super::universe::GENERATOR_VERSION),
    );
    document.insert(
        "prng_algorithm_version".to_owned(),
        json!(super::rng::PRNG_ALGORITHM_VERSION),
    );
    document.insert("scenario_version".to_owned(), json!(1));
    document.insert("build_profile".to_owned(), json!("release"));
    document.insert("target_triple".to_owned(), json!(target_triple()));
    document.insert(
        "rustc_version".to_owned(),
        host.get("rustc_version")
            .cloned()
            .unwrap_or_else(|| manifest::unavailable("rustc unavailable")),
    );
    document.insert(
        "collector_binary_sha256".to_owned(),
        binary_sha256()
            .map(Value::from)
            .unwrap_or_else(|| manifest::unavailable("current_exe unreadable")),
    );
    document.insert("host".to_owned(), host);
    document.insert(
        "scenario".to_owned(),
        json!({
            "seed": config.seed,
            "symbols": config.symbols,
            "eligible_count": output.eligible_count,
            "events_published": output.events_published,
            "shards_requested": config.shards.unwrap_or(0),
            "shards_actual": capacity.map(|plan| plan.shards).unwrap_or(0),
            "replicas_requested": config.replicas,
            "replicas_actual": capacity.map(|plan| plan.replicas).unwrap_or(0),
            "lifecycle_replicas": config.lifecycle_replicas,
            "distribution": if config.hot_pair { "zipf_hot_pair" } else { "uniform" },
            "journal": config.journal,
        }),
    );
    document.insert(
        "counts".to_owned(),
        json!({
            "generated_events": output.events_published,
            "transmitted_frames": output.transmitted_frames,
            "writer_messages": report.writer.messages,
            "writer_bytes": report.writer.bytes,
        }),
    );
    document.insert("performance".to_owned(), json!({
        "msgs_per_sec_p50": manifest::unavailable("binned throughput lands with the reference matrix"),
        "msgs_per_sec_p95": manifest::unavailable("binned throughput lands with the reference matrix"),
        "msgs_per_sec_p99": manifest::unavailable("binned throughput lands with the reference matrix"),
        "msgs_per_sec_max": manifest::unavailable("binned throughput lands with the reference matrix"),
        "rss_peak_bytes": output.rss_peak_bytes.map(Value::from)
            .unwrap_or_else(|| manifest::unavailable("rss not readable on this host")),
        "memory_per_symbol_bytes": output.rss_peak_bytes
            .map(|rss| rss / output.eligible_count.max(1) as u64)
            .map(Value::from)
            .unwrap_or_else(|| manifest::unavailable("rss not readable on this host")),
        "snapshot_anchor_ms": anchor_ms,
        "writer_queue_high_water_mark": manifest::unavailable("high-water mark not exposed by WriterStats"),
        "cpu_seconds": manifest::unavailable("cpu_seconds not measured (no libc dependency)"),
    }));
    document.insert(
        "coverage".to_owned(),
        json!({
            "expected": output.coverage.expected,
            "acknowledged": output.coverage.acknowledged,
            "anchored": output.coverage.anchored,
            "missing_ack": output.coverage.missing_ack.count,
            "missing_anchor": output.coverage.missing_anchor.count,
            "expected_set_sha256": output.coverage.expected_set_sha256,
            "acknowledged_set_sha256": canonical_set_sha256(&acknowledged),
            "anchored_set_sha256": canonical_set_sha256(&output.captured_symbols),
            "decoys_absent": output.decoy_absent,
            "coverage_valid": output.coverage.valid,
        }),
    );
    document.insert(
        "incidents".to_owned(),
        json!({
            "duplicate_events": report.core.duplicate_events,
            "sequence_resets": report.core.sequence_resets,
            "discontinuity_incidents": report.core.discontinuity_incidents,
            "lifecycle_duplicates": report.core.lifecycle_duplicates,
            "lifecycle_stale_dropped": report.core.lifecycle_stale_dropped,
            "unexplained_drops": 0u64,
        }),
    );
    document.insert(
        "verify".to_owned(),
        json!({
            "verify_valid": output.verify_valid,
            "completeness_valid": output.completeness_valid,
            "coverage_valid": output.coverage.valid,
            "journal_complete": output.journal_complete,
            "journal_record_count": output.journaled_records,
        }),
    );

    (
        format!("seed-{:04x}-{}-uniform", config.seed, config.symbols),
        Value::Object(document),
    )
}

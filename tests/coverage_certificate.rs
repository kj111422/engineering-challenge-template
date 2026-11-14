#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout,
    reason = "integration tests drive failure paths with unwrap/expect/panic and print diagnostics"
)]
#![allow(
    clippy::allow_attributes,
    reason = "test crates relax the production lint set wholesale; `expect` cannot express lints that may not fire"
)]

//! End-to-end coverage certification over real SBE captures and sidecars.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use engineering_challenge::{
    Side,
    config::{ExchangeKind, Settings},
    coverage,
    metrics::{CollectorStats, WriterStats},
    model::{MarketInfo, NumericSpec, OutputMessage},
    report::{MarketMetadata, ReportConfig, RunReport},
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
    writer::DirectWriter,
};

fn timestamp(us: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(us).unwrap()
}

fn snapshot(symbol: &str, received_us: i64, empty: bool) -> OutputMessage {
    let (bids, asks) = if empty {
        (Vec::new(), Vec::new())
    } else {
        (
            vec![SnapshotBidsEntryDomain {
                price_level: 400_000,
                quantity: 10,
            }],
            vec![SnapshotAsksEntryDomain {
                price_level: 600_000,
                quantity: 20,
            }],
        )
    };
    OutputMessage::Snapshot(SnapshotDomain {
        received_at: timestamp(received_us),
        exchange_timestamp: timestamp(received_us),
        bids,
        asks,
        symbol: symbol.to_owned(),
    })
}

fn update(symbol: &str, received_us: i64) -> OutputMessage {
    OutputMessage::PriceLevelUpdate(PriceLevelUpdateDomain {
        backtest_timestamp: timestamp(received_us),
        exchange_timestamp: timestamp(received_us),
        price_level: 400_000,
        quantity: 15,
        side: Side::Buy,
        symbol: symbol.to_owned(),
    })
}

fn write_capture(directory: &Path, messages: Vec<OutputMessage>) -> (PathBuf, usize) {
    let path = directory.join("capture.sbe");
    let count = messages.len();
    let mut writer = DirectWriter::create(engineering_challenge::writer::WriterOptions {
        path: path.to_str().unwrap().to_owned(),
        chunk_size: 4096,
        queue: 32,
        sync_on_close: false,
        overwrite: false,
        rotate_bytes: 0,
        rotate_secs: 0,
        min_free_bytes: 0,
        fsync_interval_secs: 0,
    })
    .unwrap();
    for message in messages {
        writer.write(&message).unwrap();
    }
    writer.shutdown().unwrap();
    (path, count)
}

fn market(symbol: &str) -> MarketInfo {
    MarketInfo {
        exchange: "kalshi".to_owned(),
        native_symbol: symbol.to_owned(),
        symbol: symbol.to_owned(),
        title: None,
        outcome: None,
        exchange_market_id: None,
        close_time_us: None,
        tick_size_us: Some(10_000),
        price_ranges: Vec::new(),
        price_level_structure: None,
        numeric_spec: NumericSpec::default(),
    }
}

fn write_metadata(directory: &Path, symbols: &[&str]) -> PathBuf {
    let path = directory.join("markets.json");
    MarketMetadata::new(
        "kalshi",
        symbols.iter().map(|symbol| market(symbol)).collect(),
    )
    .write(&path)
    .unwrap();
    path
}

fn write_report(directory: &Path, acknowledged: &[&str], writer_messages: usize) -> PathBuf {
    let path = directory.join("report.json");
    let settings = Settings::for_exchange(ExchangeKind::Kalshi);
    let mut report = RunReport::new(
        "live",
        "kalshi",
        "production",
        0,
        1_000_000,
        "duration elapsed",
        "capture.sbe",
        1,
        1,
        acknowledged.len(),
        acknowledged.len(),
        CollectorStats::default(),
        WriterStats {
            messages: writer_messages as u64,
            ..WriterStats::default()
        },
        Vec::new(),
        ReportConfig::from_settings(&settings, 1),
    );
    report.acknowledged_symbols = acknowledged
        .iter()
        .map(|value| (*value).to_owned())
        .collect();
    report.write(&path).unwrap();
    path
}

#[test]
fn complete_universe_including_empty_book_snapshot_is_certified() {
    let directory = tempfile::tempdir().unwrap();
    let (capture, capture_messages) = write_capture(
        directory.path(),
        vec![
            snapshot("A", 10, false),
            // B has no levels but still provides an anchor.
            snapshot("B", 20, true),
        ],
    );
    let metadata = write_metadata(directory.path(), &["A", "B"]);
    let report = write_report(directory.path(), &["A", "B"], capture_messages);

    let summary = coverage::run(&capture, &metadata, &report, "4096").unwrap();
    assert!(summary.valid, "{summary:?}");
    assert_eq!(summary.expected, 2);
    assert_eq!(summary.acknowledged, 2);
    assert_eq!(summary.anchored, 2);
    assert_eq!(summary.missing_anchor.count, 0);
    assert_eq!(summary.observed_not_anchored.count, 0);
}

#[test]
fn acknowledged_symbol_without_snapshot_fails_certification() {
    let directory = tempfile::tempdir().unwrap();
    let (capture, capture_messages) =
        write_capture(directory.path(), vec![snapshot("A", 10, false)]);
    let metadata = write_metadata(directory.path(), &["A", "B"]);
    let report = write_report(directory.path(), &["A", "B"], capture_messages);

    let summary = coverage::run(&capture, &metadata, &report, "4096").unwrap();
    assert!(!summary.valid);
    assert_eq!(summary.missing_ack.count, 0);
    assert_eq!(summary.acknowledged, 2);
    assert_eq!(summary.missing_anchor.count, 1);
    assert_eq!(summary.missing_anchor.sample, vec!["B".to_owned()]);
    assert_eq!(summary.ack_not_anchored.sample, vec!["B".to_owned()]);
}

#[test]
fn update_only_symbol_is_not_anchored() {
    let directory = tempfile::tempdir().unwrap();
    let (capture, capture_messages) = write_capture(
        directory.path(),
        vec![snapshot("A", 10, false), update("B", 20)],
    );
    let metadata = write_metadata(directory.path(), &["A", "B"]);
    let report = write_report(directory.path(), &["A", "B"], capture_messages);

    let summary = coverage::run(&capture, &metadata, &report, "4096").unwrap();
    assert!(!summary.valid);
    assert_eq!(summary.observed, 2);
    assert_eq!(summary.anchored, 1);
    assert_eq!(summary.observed_not_anchored.sample, vec!["B".to_owned()]);
    assert_eq!(summary.missing_anchor.sample, vec!["B".to_owned()]);
}

#[test]
fn retired_captured_symbol_is_reported_but_does_not_fail() {
    let directory = tempfile::tempdir().unwrap();
    let (capture, capture_messages) = write_capture(
        directory.path(),
        vec![snapshot("A", 10, false), snapshot("B", 20, true)],
    );
    let metadata = write_metadata(directory.path(), &["A"]);
    let report = write_report(directory.path(), &["A"], capture_messages);

    let summary = coverage::run(&capture, &metadata, &report, "4096").unwrap();
    assert!(summary.valid);
    assert_eq!(summary.anchored_not_expected.sample, vec!["B".to_owned()]);
}

#[test]
fn cli_exits_zero_for_a_complete_certificate() {
    let directory = tempfile::tempdir().unwrap();
    let (capture, capture_messages) =
        write_capture(directory.path(), vec![snapshot("A", 10, true)]);
    let metadata = write_metadata(directory.path(), &["A"]);
    let report = write_report(directory.path(), &["A"], capture_messages);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
        .args(["coverage"])
        .arg(&capture)
        .arg(&metadata)
        .arg(&report)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"valid\": true"));
}

#[test]
fn cli_exits_non_zero_when_a_selected_symbol_is_unanchored() {
    let directory = tempfile::tempdir().unwrap();
    let (capture, capture_messages) =
        write_capture(directory.path(), vec![snapshot("A", 10, false)]);
    let metadata = write_metadata(directory.path(), &["A", "B"]);
    let report = write_report(directory.path(), &["A", "B"], capture_messages);

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
        .args(["coverage"])
        .arg(&capture)
        .arg(&metadata)
        .arg(&report)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("coverage failed"), "stderr: {stderr}");
}

#[test]
fn truncated_capture_cannot_certify_complete_coverage() {
    let directory = tempfile::tempdir().unwrap();
    // Write enough data that an abrupt end removes messages, not just
    // trailing pad bytes.
    let messages: Vec<_> = (0..100)
        .map(|index| snapshot("A", index, index % 2 == 0))
        .collect();
    let (capture, capture_messages) = write_capture(directory.path(), messages);
    let len = std::fs::metadata(&capture).unwrap().len();
    assert!(len > 4096);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&capture)
        .unwrap();
    file.set_len(4096).unwrap();
    drop(file);

    let metadata = write_metadata(directory.path(), &["A"]);
    let report = write_report(directory.path(), &["A"], capture_messages);

    let summary = coverage::run(&capture, &metadata, &report, "4096").unwrap();
    assert!(!summary.valid);
    assert!(!summary.capture_messages_match_report);
    assert!(summary.capture_messages < summary.report_writer_messages);
}

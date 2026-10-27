#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::path::PathBuf;

use engineering_challenge::{
    config::Settings,
    replay,
    util::{AlignedSbeReader, OrderbookSchema, dtos::challenge_orderbook::AnyMessage},
};

#[test]
fn sample_fixture_replays_through_the_same_core_and_decodes() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("replay.sbe");
    let mut settings = Settings::for_exchange(engineering_challenge::config::ExchangeKind::Kalshi);
    settings.output.path = output.display().to_string();
    settings.output.chunk_size = 4096;
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/sample_replay.jsonl");
    let report = replay::run(settings, &fixture).unwrap();
    assert!(report.core.exchange_snapshots >= 2);
    assert!(report.core.deltas_emitted >= 2);
    assert!(report.core.failover_snapshots >= 1);
    assert_eq!(report.final_symbols, 1);
    assert_eq!(report.writer.errors, 0);

    let mut reader =
        AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(&output, 4096).unwrap();
    let mut messages = 0usize;
    while let Some(message) = reader.read_msg().unwrap() {
        match message {
            AnyMessage::Snapshot(_) | AnyMessage::PriceLevelUpdate(_) => messages += 1,
            AnyMessage::Unknown { .. } => panic!("unexpected unknown SBE message"),
            _ => panic!("unexpected future SBE message"),
        }
    }
    assert_eq!(messages, report.writer.messages as usize);
}

#[test]
fn replay_rejects_out_of_range_timestamps_before_serialization() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("replay.sbe");
    let fixture = directory.path().join("bad-timestamp.jsonl");
    std::fs::write(
        &fixture,
        concat!(
            "{\"type\":\"discovery\",\"received_at_us\":1700000000000000,",
            "\"markets\":[{\"exchange\":\"test\",\"native_symbol\":\"A\",\"symbol\":\"A\"}]}\n",
            "{\"type\":\"snapshot\",\"received_at_us\":9223372036854775807,",
            "\"source\":{\"shard\":0,\"replica\":0},\"native_symbol\":\"A\",",
            "\"bids\":[{\"price\":400000,\"quantity\":100}],",
            "\"asks\":[{\"price\":600000,\"quantity\":100}]}\n"
        ),
    )
    .unwrap();
    let mut settings = Settings::for_exchange(engineering_challenge::config::ExchangeKind::Kalshi);
    settings.output.path = output.display().to_string();
    settings.output.chunk_size = 4096;
    let error = replay::run(settings, &fixture).unwrap_err();
    assert!(
        format!("{error:#}").contains("out of range"),
        "unexpected error: {error:#}"
    );
}

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

//! Deterministic synthetic full-universe stress laboratory.
//!
//! The synthetic Kalshi venue serves a seeded universe of
//! `STRESS-{seed:04x}-{i:06}` markets (regular challenge-eligible markets plus
//! MVE/closed decoys as negative controls) over real REST pagination and
//! WebSocket subscriptions. Every run drives the production collector path
//! (adapter → discovery → capacity planning → coordinator → books → writer →
//! journal) and is certified with `verify`, `completeness` and `coverage`.
//!
//! Evidence class: synthetic/loopback. See `docs/stress-lab-design.md`.
//!
//! CI runs one small correctness smoke. The remaining cases are opt-in
//! (`--ignored`) and parameterized by `STRESS_*` environment variables.

mod stress_support;

use stress_support::{
    rng::SplitMix64,
    runner::CellConfig,
    universe::{GENERATOR_VERSION, SyntheticUniverse},
};

/// Deterministic RNG contract, exercised against pinned vectors.
#[test]
fn splitmix64_is_pinned() {
    let mut rng = SplitMix64::new(0);
    let first: Vec<u64> = (0..4).map(|_| rng.next_u64()).collect();
    assert_eq!(
        first,
        vec![
            0xE220_A839_7B1D_CDAF,
            0x6E78_9E6A_A1B9_65F4,
            0x06C4_5D18_8009_454F,
            0xF88B_B8A8_724C_81EC,
        ]
    );
    assert_eq!(stress_support::rng::PRNG_ALGORITHM_VERSION, 1);
}

/// Universe contract: eligible and decoy sets are disjoint and complete.
#[test]
fn universe_generation_is_disjoint_and_deterministic() {
    let universe = SyntheticUniverse::generate(42, 64, 100_000);
    assert_eq!(GENERATOR_VERSION, 1);
    let eligible = universe.eligible_tickers_sorted();
    let excluded = universe.excluded_tickers_sorted();
    assert_eq!(eligible.len(), 64);
    assert!(!excluded.is_empty());
    for ticker in &excluded {
        assert!(!eligible.contains(ticker));
    }
}

/// One small smoke cell: anchor a 1k-eligible-symbol universe through the real
/// pipeline, publish deltas, stop and certify. This is the CI regression
/// gate — invariants only, no throughput claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smoke_cell_anchors_applies_and_certifies() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        seed: 7,
        symbols: 1_000,
        decoy_fraction_ppm: 20_000,
        shards: Some(1),
        replicas: 2,
        lifecycle_replicas: 1,
        events: 200,
        hot_pair: false,
        uniform_spread: true,
        journal: true,
        force_capacity_reject: false,
    };
    let output = stress_support::runner::run_cell(directory.path(), &config)
        .await
        .expect("stress cell failed");

    assert!(
        !output.anchor_wait_expired,
        "anchoring did not complete in time"
    );
    assert!(
        output.decoy_absent,
        "an MVE/closed decoy entered the capture"
    );
    assert!(output.verify_valid, "SBE verification failed");
    assert!(
        output.completeness_valid,
        "completeness verification failed"
    );
    assert!(
        output.journal_complete,
        "journal did not finalize completely"
    );
    assert!(
        output.coverage.valid,
        "coverage not valid: {:?}",
        output.coverage
    );
    assert_eq!(output.coverage.expected, output.eligible_count);
    assert_eq!(output.events_published, 200);
    // One active-plane exchange snapshot per eligible symbol.
    assert_eq!(output.coverage.anchored, output.eligible_count);
    assert_eq!(output.capacity_connections, 3); // 1 shard x 2 replicas + 1 lifecycle
    assert_eq!(
        output.expected_subscription_count,
        output.eligible_count * 2
    );
}

/// Deliberately-failing reference cell: forces the capacity plan to reject so
/// a failure manifest is produced on demand (ignored in CI; run locally to
/// regenerate `measurements/stress/failed-*.json`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "reference artifact generator; run explicitly"]
async fn reference_failed_cell_produces_manifest() {
    assert!(
        std::env::var("STRESS_MANIFEST_OUT").is_ok(),
        "set STRESS_MANIFEST_OUT to produce the artifact"
    );
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        seed: 9,
        symbols: 1_000,
        force_capacity_reject: true,
        ..CellConfig::default()
    };
    let result = stress_support::runner::run_cell(directory.path(), &config).await;
    assert!(result.is_err(), "capacity rejection must fail the cell");
}

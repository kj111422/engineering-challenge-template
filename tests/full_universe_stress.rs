#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

//! Deterministic synthetic full-universe stress laboratory.
//!
//! The synthetic Kalshi venue serves a seeded universe of
//! `STRESS-{seed:04x}-{i:06}` markets (regular challenge-eligible markets
//! plus MVE/closed decoys as negative controls) over real REST pagination
//! and WebSocket subscriptions. Every run drives the production collector
//! path (adapter → discovery → capacity planning → coordinator → books →
//! writer → journal) and is certified with `verify`, `completeness` and
//! `coverage` inside the runner, fail-closed, before any manifest exists.
//!
//! Evidence class: synthetic/loopback. See `docs/stress-lab-design.md`.
//!
//! CI runs the 4k correctness smoke plus deterministic self-tests of the
//! failure fixtures. The full matrix is opt-in (`--ignored`).

mod stress_support;

use std::collections::BTreeSet;

use stress_support::{
    rng::SplitMix64,
    runner::{CellConfig, FailInject, canonical_set_sha256, worktree_provenance},
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

fn headless_cell_config(seed: u64, symbols: usize) -> CellConfig {
    CellConfig {
        seed,
        symbols,
        decoy_fraction_ppm: 20_000,
        shards: Some(1),
        replicas: 2,
        lifecycle_replicas: 1,
        events: 40,
        hot_pair: false,
        uniform_spread: true,
        journal: true,
        limits: Some((20, 2)),
        force_capacity_reject: false,
        fail_inject: None,
        writer_queue: None,
        worktree_dir: None,
        require_clean_worktree: None,
    }
}

/// One small smoke cell: anchor a 4k-eligible-symbol universe through the
/// real pipeline, publish deltas, prove delivery with the drain barrier,
/// stop and certify. This is the CI regression gate — invariants only, no
/// throughput claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn smoke_cell_anchors_applies_and_certifies() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        seed: 7,
        symbols: 4_000,
        decoy_fraction_ppm: 20_000,
        shards: Some(1),
        replicas: 2,
        lifecycle_replicas: 1,
        events: 200,
        hot_pair: false,
        uniform_spread: true,
        journal: true,
        limits: Some((45, 10)),
        force_capacity_reject: false,
        fail_inject: None,
        writer_queue: None,
        worktree_dir: None,
        require_clean_worktree: None,
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        outcome.acceptance.valid,
        "smoke cell failed acceptance: reasons={:?}",
        outcome.acceptance.reasons
    );
    let output = outcome.output.expect("valid cell must carry output");

    assert!(
        !output.anchor_wait_expired,
        "anchoring did not complete in time"
    );
    assert!(output.drain_completed, "drain barrier did not complete");
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
    let coverage = output.coverage.as_ref().expect("coverage must be present");
    assert!(coverage.valid, "coverage not valid: {coverage:?}");
    assert_eq!(coverage.expected, output.eligible_count);
    assert_eq!(coverage.anchored, output.eligible_count);
    assert_eq!(output.capacity_connections, 3); // 1 shard x 2 replicas + 1 lifecycle

    // Independent event accounting must reconcile exactly.
    assert_eq!(output.events_published, 200);
    assert_eq!(output.sbe_deltas, 200);
    assert!(output.ledger.reconciled(), "ledger: {:?}", output.ledger);
    assert_eq!(output.ledger.unexplained_drops, Some(0));
    assert_eq!(output.ledger.persistence_gap, Some(0));

    // The three canonical set hashes must agree, and the expected hash must
    // equal the hash of the independently regenerated universe.
    let universe = SyntheticUniverse::generate(7, 4_000, 20_000);
    let independent: BTreeSet<String> = universe.eligible_tickers_sorted().into_iter().collect();
    let expected_hash = canonical_set_sha256(&independent);
    assert_eq!(coverage.expected_set_sha256, expected_hash);
    assert_eq!(
        canonical_set_sha256(&output.acknowledged_symbols),
        expected_hash
    );
    assert_eq!(
        canonical_set_sha256(&output.anchored_symbols),
        expected_hash
    );
}

/// Fixture: one eligible market receives no snapshot. It must never enter the
/// snapshot-anchored set and the cell must fail closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_snapshot_fixture_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::MissingSnapshot { skip_one: 1 }),
        events: 30,
        limits: Some((10, 2)),
        ..headless_cell_config(11, 200)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        !outcome.acceptance.valid,
        "missing snapshot must fail acceptance"
    );
    assert!(
        outcome
            .acceptance
            .reasons
            .iter()
            .any(|reason| reason.contains("anchor") || reason.contains("coverage")),
        "expected anchor/coverage failure, got {:?}",
        outcome.acceptance.reasons
    );
    if let Some(output) = &outcome.output {
        let universe = SyntheticUniverse::generate(11, 200, 20_000);
        let skipped = universe.eligible_tickers_sorted()[0].clone();
        assert!(
            !output.anchored_symbols.contains(&skipped),
            "the skipped market must not be snapshot-anchored"
        );
        assert!(
            output.acknowledged_symbols.contains(&skipped),
            "the skipped market must still be acknowledged"
        );
    }
}

/// Fixture: one market receives a delta but no snapshot. The delta must not
/// anchor it, the applied count must fall by exactly one, and the event
/// ledger must report the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_without_snapshot_fixture_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::UpdateOnly { skip_one: 1 }),
        events: 40,
        limits: Some((10, 2)),
        ..headless_cell_config(12, 200)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        !outcome.acceptance.valid,
        "update without anchor must fail acceptance"
    );
    assert!(
        outcome
            .acceptance
            .reasons
            .iter()
            .any(|reason| reason == "unexplained_drops" || reason.contains("coverage")),
        "expected accounting/coverage failure, got {:?}",
        outcome.acceptance.reasons
    );
    let output = outcome.output.expect("fixture cell must produce output");
    let universe = SyntheticUniverse::generate(12, 200, 20_000);
    let skipped = universe.eligible_tickers_sorted()[0].clone();
    assert!(
        !output.anchored_symbols.contains(&skipped),
        "an update-only symbol must not be counted as snapshot-anchored"
    );
    assert_eq!(
        output.sbe_deltas,
        output.events_published as u64 - 1,
        "exactly the update-only delta must be missing from the capture"
    );
    assert!(
        output
            .ledger
            .unexplained_drops
            .is_some_and(|drops| drops >= 1),
        "ledger must report the unaccounted delta: {:?}",
        output.ledger
    );
}

/// Fixture: delta frames dropped before the wire. With two replicas the
/// first `count` frames vanish in replica-pair order, so exactly
/// `count / 2` events are lost and only event accounting can catch it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_delta_fixture_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::DroppedFrames { count: 10 }),
        events: 100,
        ..headless_cell_config(13, 300)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        !outcome.acceptance.valid,
        "dropped frames must fail acceptance"
    );
    assert!(
        outcome
            .acceptance
            .reasons
            .iter()
            .any(|reason| reason == "unexplained_drops"),
        "expected unexplained_drops, got {:?}",
        outcome.acceptance.reasons
    );
    let output = outcome.output.expect("fixture cell must produce output");
    assert_eq!(
        output.sbe_deltas,
        output.events_published as u64 - 5,
        "exactly the injected events must be missing from the capture"
    );
    assert_eq!(output.mock.injected_pre_wire_drops, 10);
    assert_eq!(output.ledger.unexplained_drops, Some(5));
}

/// Fixture: the production writer queue bound is set to its minimum and the
/// final-snapshot burst must drive it to saturation. The channel bound must
/// hold (the counter observes it with at most one slot of lag), the run must
/// either reconcile under the visible backpressure (success with evidence)
/// or fail — never silently pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_queue_saturation_fixture_preserves_accounting() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        writer_queue: Some(128),
        events: 2_000,
        hot_pair: true,
        ..headless_cell_config(15, 2_000)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    let output = outcome.output.expect("writer fixture must produce output");
    assert!(
        output.writer_queue_high_water >= 128,
        "the writer queue must reach its configured bound (high-water {})",
        output.writer_queue_high_water
    );
    assert!(
        output.writer_queue_high_water <= 129,
        "the channel bound must hold; only the counter's one-slot \
         observation lag may exceed it (high-water {})",
        output.writer_queue_high_water
    );
    assert_eq!(
        outcome.acceptance.valid,
        output.ledger.reconciled(),
        "valid ⟺ reconciled; reasons={:?} ledger={:?}",
        outcome.acceptance.reasons,
        output.ledger
    );
}

/// Fixture: the mock's outbound queue is bounded hard and the forwarder is
/// stalled. Saturation must be visible and the drain barrier must refuse to
/// certify the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_queue_saturation_fixture_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::MockQueueSaturate { capacity: 8 }),
        events: 500,
        limits: Some((8, 2)),
        ..headless_cell_config(14, 200)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        !outcome.acceptance.valid,
        "stalled mock queue must fail acceptance"
    );
    let output = outcome
        .output
        .expect("saturation fixture must produce output");
    assert!(
        output.mock.queue_saturated > 0,
        "the bounded queue must record saturation events"
    );
    assert!(
        output.mock.queue_high_water <= 8,
        "the mock queue bound must be respected"
    );
    assert!(
        outcome
            .acceptance
            .reasons
            .iter()
            .any(|reason| reason == "drain_incomplete"),
        "unexpected reasons: {:?}",
        outcome.acceptance.reasons
    );
}

/// Fixture: shutdown requested before the drain barrier completes. The cell
/// must never certify as success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_before_drain_fixture_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::ShutdownBeforeDrain),
        events: 50,
        ..headless_cell_config(16, 100)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        !outcome.acceptance.valid,
        "shutdown before drain must not produce a success manifest"
    );
    assert!(
        !outcome.acceptance.valid
            && outcome
                .acceptance
                .reasons
                .iter()
                .any(|reason| reason == "drain_incomplete" || reason == "unexplained_drops"),
        "unexpected reasons: {:?}",
        outcome.acceptance.reasons
    );
}

/// Fixture: the index document is corrupt; the cell must not produce a
/// success artifact and the run must fail closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_index_fixture_fails_closed() {
    let directory = tempfile::tempdir().unwrap();
    let out = directory.path().join("manifests");
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("index.json"), b"this is not json").unwrap();

    let config = CellConfig {
        seed: 17,
        symbols: 60,
        decoy_fraction_ppm: 20_000,
        shards: Some(1),
        replicas: 2,
        lifecycle_replicas: 1,
        events: 5,
        hot_pair: false,
        uniform_spread: true,
        journal: false,
        limits: Some((20, 2)),
        force_capacity_reject: false,
        fail_inject: None,
        writer_queue: None,
        worktree_dir: None,
        require_clean_worktree: None,
    };
    let outcome =
        stress_support::runner::run_cell(directory.path(), &config, Some(out.as_path())).await;
    assert!(
        !outcome.acceptance.valid,
        "a corrupt index must flip the cell to a failed artifact, got {:?}",
        outcome.acceptance
    );
    assert_eq!(
        outcome.acceptance.failure_class.as_deref(),
        Some("manifest_index_error"),
        "corrupt index must be reported as manifest_index_error"
    );
    let manifest_files: Vec<_> = std::fs::read_dir(&out)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        manifest_files,
        vec!["index.json".to_owned()],
        "no manifest artifact may be written when the index is malformed"
    );
    let corrupt_index = std::fs::read_to_string(out.join("index.json")).unwrap();
    assert_eq!(
        corrupt_index, "this is not json",
        "corrupt index must not be overwritten"
    );
}

/// The clean-worktree gate must detect tracked modifications and hash the
/// diff, while staying silent about untracked files.
#[test]
fn worktree_gate_detects_dirty_tracked_files() {
    fn git(repo: &std::path::Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .status()
            .expect("git must be available");
        assert!(status.success(), "git {args:?} failed");
    }
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q"]);
    std::fs::write(repo.path().join("tracked.txt"), "original\n").unwrap();
    git(repo.path(), &["add", "tracked.txt"]);
    git(
        repo.path(),
        &[
            "-c",
            "user.email=stress@example.invalid",
            "-c",
            "user.name=stress",
            "commit",
            "-q",
            "-m",
            "init",
        ],
    );

    let clean = worktree_provenance(repo.path());
    assert_eq!(clean.tracked_clean, Some(true));
    assert!(clean.tracked_modified.is_empty());
    assert!(clean.tracked_diff_sha256.is_none());

    std::fs::write(repo.path().join("tracked.txt"), "modified\n").unwrap();
    let dirty = worktree_provenance(repo.path());
    assert_eq!(dirty.tracked_clean, Some(false));
    assert!(
        dirty
            .tracked_modified
            .iter()
            .any(|entry| entry.contains("tracked.txt"))
    );
    assert!(dirty.tracked_diff_sha256.is_some());
    // Untracked files are recorded but never mark the worktree dirty.
    std::fs::write(repo.path().join("untracked.txt"), "x\n").unwrap();
    let with_untracked = worktree_provenance(repo.path());
    assert_eq!(with_untracked.tracked_clean, Some(false));
    assert_eq!(with_untracked.untracked_count, Some(1));
}

/// Fixture: a run executed from a dirty worktree must refuse to start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dirty_worktree_fixture_fails_closed() {
    let repo = tempfile::tempdir().unwrap();
    let init = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo.path())
            .args(args)
            .status()
            .expect("git must be available");
        assert!(status.success());
    };
    init(&["init", "-q"]);
    std::fs::write(repo.path().join("tracked.txt"), "original\n").unwrap();
    init(&["add", "tracked.txt"]);
    init(&[
        "-c",
        "user.email=stress@example.invalid",
        "-c",
        "user.name=stress",
        "commit",
        "-q",
        "-m",
        "init",
    ]);
    std::fs::write(repo.path().join("tracked.txt"), "dirty\n").unwrap();

    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        symbols: 60,
        events: 5,
        limits: Some((5, 2)),
        worktree_dir: Some(repo.path().to_path_buf()),
        require_clean_worktree: Some(true),
        ..headless_cell_config(18, 60)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        !outcome.acceptance.valid,
        "dirty worktree must fail acceptance"
    );
    assert_eq!(
        outcome.acceptance.failure_class.as_deref(),
        Some("dirty_worktree")
    );
    assert!(
        outcome.output.is_none(),
        "a gated-off cell must not produce collector evidence"
    );
}

/// The same gate must let a clean repository run to completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worktree_gate_accepts_clean_repository() {
    let repo = tempfile::tempdir().unwrap();
    let init = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(repo.path())
            .args(args)
            .status()
            .expect("git must be available");
        assert!(status.success());
    };
    init(&["init", "-q"]);
    std::fs::write(repo.path().join("tracked.txt"), "clean\n").unwrap();
    init(&["add", "tracked.txt"]);
    init(&[
        "-c",
        "user.email=stress@example.invalid",
        "-c",
        "user.name=stress",
        "commit",
        "-q",
        "-m",
        "init",
    ]);

    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        symbols: 60,
        events: 5,
        limits: Some((20, 2)),
        worktree_dir: Some(repo.path().to_path_buf()),
        require_clean_worktree: Some(true),
        ..headless_cell_config(19, 60)
    };
    let outcome = stress_support::runner::run_cell(directory.path(), &config, None).await;
    assert!(
        outcome.acceptance.valid,
        "a clean worktree must pass the gate: {:?}",
        outcome.acceptance.reasons
    );
}

/// Deliberately-failing reference cell: forces a capacity-plan rejection so
/// a failure manifest is produced on demand (ignored in CI; run locally to
/// regenerate `measurements/stress/reference-*.json`). The manifest preserves
/// the exact planner rejection reason — an infeasible cell is recorded, not
/// silently skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
    let outcome =
        stress_support::runner::run_cell(directory.path(), &config, Some(manifest_out().as_path()))
            .await;
    assert!(
        !outcome.acceptance.valid,
        "capacity rejection must fail acceptance"
    );
    assert!(
        outcome
            .acceptance
            .failure_class
            .as_deref()
            .is_some_and(|class| class.starts_with("collector_terminated")),
        "unexpected failure class: {:?}",
        outcome.acceptance.failure_class
    );
}

/// Reference generator: the successful 4k smoke manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "reference artifact generator; run explicitly"]
async fn reference_success_manifest_is_reproducible() {
    assert!(
        std::env::var("STRESS_MANIFEST_OUT").is_ok(),
        "set STRESS_MANIFEST_OUT to produce the artifact"
    );
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        seed: 7,
        symbols: 4_000,
        decoy_fraction_ppm: 20_000,
        shards: Some(1),
        replicas: 2,
        lifecycle_replicas: 1,
        events: 200,
        hot_pair: false,
        uniform_spread: true,
        journal: true,
        limits: Some((45, 10)),
        force_capacity_reject: false,
        fail_inject: None,
        writer_queue: None,
        worktree_dir: None,
        require_clean_worktree: None,
    };
    let outcome =
        stress_support::runner::run_cell(directory.path(), &config, Some(manifest_out().as_path()))
            .await;
    assert!(
        outcome.acceptance.valid,
        "reference smoke must pass: {:?}",
        outcome.acceptance.reasons
    );
}

/// Reference generator: failed manifest for the missing-anchor injection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "reference artifact generator; run explicitly"]
async fn reference_missing_snapshot_manifest() {
    assert!(
        std::env::var("STRESS_MANIFEST_OUT").is_ok(),
        "set STRESS_MANIFEST_OUT to produce the artifact"
    );
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::MissingSnapshot { skip_one: 1 }),
        events: 30,
        limits: Some((10, 2)),
        ..headless_cell_config(11, 200)
    };
    let outcome =
        stress_support::runner::run_cell(directory.path(), &config, Some(manifest_out().as_path()))
            .await;
    assert!(!outcome.acceptance.valid);
}

/// Reference generator: failed manifest for the dropped-frame injection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "reference artifact generator; run explicitly"]
async fn reference_dropped_frame_manifest() {
    assert!(
        std::env::var("STRESS_MANIFEST_OUT").is_ok(),
        "set STRESS_MANIFEST_OUT to produce the artifact"
    );
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::DroppedFrames { count: 10 }),
        events: 100,
        ..headless_cell_config(13, 300)
    };
    let outcome =
        stress_support::runner::run_cell(directory.path(), &config, Some(manifest_out().as_path()))
            .await;
    assert!(!outcome.acceptance.valid);
}

/// Reference generator: bounded-queue saturation result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "reference artifact generator; run explicitly"]
async fn reference_queue_saturation_manifest() {
    assert!(
        std::env::var("STRESS_MANIFEST_OUT").is_ok(),
        "set STRESS_MANIFEST_OUT to produce the artifact"
    );
    let directory = tempfile::tempdir().unwrap();
    let config = CellConfig {
        fail_inject: Some(FailInject::MockQueueSaturate { capacity: 8 }),
        events: 500,
        limits: Some((8, 2)),
        ..headless_cell_config(14, 200)
    };
    let outcome =
        stress_support::runner::run_cell(directory.path(), &config, Some(manifest_out().as_path()))
            .await;
    assert!(!outcome.acceptance.valid);
    let output = outcome
        .output
        .expect("saturation fixture must produce output");
    assert!(output.mock.queue_saturated > 0);
}

fn manifest_out() -> std::path::PathBuf {
    std::path::PathBuf::from(
        std::env::var("STRESS_MANIFEST_OUT").expect("STRESS_MANIFEST_OUT is checked by callers"),
    )
}

# Verification — 2026-10-30

Gate evidence for the continuity work and the raw-frame journal on
`polymarket-continuity-failure`.

- Implementation commit: `4362283d2fc2bb813c8feef391c3f882db0f8222`
  (`Add checksummed raw-frame journal for public data frames`).
- Continuity commit reviewed earlier: `13bc7e0dde5afc36eb350434eb5332c5abb23783`.
- This record is added by a documentation-only follow-up commit. Source and
  test files at that commit are identical to the implementation commit.

Environment: macOS (arm64), Rust 1.99.1 pinned by `rust-toolchain.toml`,
release profile for the binary.

## Gate results

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --locked -- -D warnings` | Pass |
| `cargo test --all-targets --locked` | Pass: **209 library tests + 9 mock-exchange E2E tests + 2 replay integration tests = 220 passed**, 3 ignored |
| `cargo test --doc --locked` | Pass: 0 documentation tests |
| `cargo build --release --locked` | Pass |
| `python3 scripts/check_architecture.py --check` | Pass: 42 Rust source files, 223 attributed test functions, 42 files reachable from crate roots, 9 structurally checked Mermaid diagrams |

Rust CI run
[40956405372](https://github.com/kj111422/engineering-challenge-template/actions/runs/40956405372)
passed formatting, lint, tests, documentation tests, and the release build on
`4362283`. Architecture documentation run
[40956405414](https://github.com/kj111422/engineering-challenge-template/actions/runs/40956405414)
passed on the same commit.

The earlier continuity commit `13bc7e0` has 205 passing and 3 ignored tests,
recorded by Rust CI run
[40522906788](https://github.com/kj111422/engineering-challenge-template/actions/runs/40522906788)
and architecture run
[40522907015](https://github.com/kj111422/engineering-challenge-template/actions/runs/40522907015).
The journal adds 13 library tests and one mock-exchange scenario, which accounts
for the 220 count.

## Ignored tests

- `tests/e2e_mock_exchange.rs::opt_in_scale_run` requires `SCALE_SYMBOLS` and
  `--ignored`; it is run separately at 1k/5k/25k/100k symbols.
- `tests/live_polymarket.rs::discovers_and_subscribes_to_public_polymarket_books`
  and `resubscribing_an_asset_returns_a_fresh_book` require
  `RUN_LIVE_TESTS=1` and public network access.

## Raw-frame journal evidence

The journal is a separate versioned artifact. The supplied SBE schema, SBE
writer, and SBE output are unchanged; the full existing suite and the mock
exchange scenarios continue to pass.

| Plan check | Test |
|---|---|
| Exact payload survives the write/read path | `journal::tests::payload_bytes_survive_write_and_read` |
| Rejected and unknown frames are recorded | `journal::tests::rejected_and_unknown_frames_are_recorded` |
| Checked-in fixtures round-trip byte for byte | `journal::tests::checked_in_fixtures_round_trip_byte_for_byte` |
| Per-source epoch/sequence order, including across rotated segments | `journal::tests::record_order_matches_source_epoch_and_sequence`, `journal::tests::record_order_is_checked_across_segments` |
| Truncated final record is detected | `journal::tests::truncated_final_record_is_detected` |
| Record checksum or manifest corruption fails verification | `journal::tests::checksum_mismatch_and_corrupt_manifest_fail_verification` |
| Crash leaves an incomplete run, not a complete claim | `journal::tests::unfinalized_segment_is_reported_incomplete` |
| Write failure marks the run failed | `journal::tests::storage_write_failure_marks_run_failed` |
| Sync failure marks the run failed | `journal::tests::storage_sync_failure_marks_run_failed` |
| Queue saturation fails instead of dropping | `journal::tests::queue_saturation_fails_instead_of_dropping` |
| Graceful shutdown finalizes rotated segments | `journal::tests::graceful_shutdown_finalizes_rotated_segments`, E2E `journal_finalizes_and_verifies_on_graceful_shutdown` |
| Identical input produces identical records | `journal::tests::identical_input_produces_identical_records` |
| Coordinator starts, finalizes, reports, and verifies a live path | E2E `journal_finalizes_and_verifies_on_graceful_shutdown` |

The E2E scenario drives the real coordinator, Polymarket mock adapters,
sessions, SBE writer, journal writer, and verification reader. It asserts that
the report carries a finalized journal status, that every record checksum
verifies, that receive sequences advance per source across rotated segments,
and that all delivered `price_change` payloads are present.

Fault injection uses the journal writer's test-only fault hook for write, flush,
sync, and finalize failures. Disk exhaustion is represented by the write and
sync failure paths; no physical filesystem was filled during this run. Queue
saturation uses a blocked writer and a bounded queue. Truncation, partial files,
unlisted segments, checksum mismatches, and corrupt manifests are produced by
mutating files on disk and then running the production verifier.

## Scope and limits

- The journal is enabled only when `journal.directory` is configured. Replay and
  offline inspection commands do not write it.
- No strategy, authentication, order management, order submission, or live
  order transmission exists in this change.
- No retention policy, disk watermark, compression, or cross-run index is
  implemented. Directory sizing and old-run removal remain operator tasks.
- `read_records` loads a complete run into memory.
- The journal was not exercised against a live Polymarket feed in this gate.
  Live-feed CI remains opt-in and separate.
- A 24-hour soak with rotating media, real disk exhaustion, and power-loss
  testing remains open. The current evidence covers deterministic fault
  injection, truncation, queue saturation, and graceful and interrupted
  shutdown paths.

## Reproduction

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
python3 scripts/check_architecture.py --check
```

Journal-specific tests:

```bash
cargo test --lib journal::
cargo test --test e2e_mock_exchange journal_finalizes_and_verifies_on_graceful_shutdown
```

Mock-exchange tests bind loopback ports only and do not contact an exchange.

# PR #42 — Deterministic full-universe stress laboratory (design)

**Status:** design for review — no production code in this commit.
**Branch:** `feat/full-universe-stress-lab`
**Base:** `b66fdbd` (`main` after PR #40 and PR #41 merged)
**Author date:** 2026-11-14
**Evidence class:** synthetic/loopback. No live venue is contacted and no venue data is used.

## 1. Objective

Establish a reproducible capacity envelope for the collector by exercising the **production ingest path** — not a parallel benchmark implementation:

```
frame → decoder → venue normalization → coordinator/shard routing → order book
      → writer → SBE → journal → `verify` / `completeness` / `coverage`
```

A run that completes with impressive throughput but loses, duplicates, or misanchors symbols is a failure, not a result. The pass condition is **exact coverage reconciliation plus bounded resources**, measured repeatedly.

## 2. Scope and non-goals

In scope:

- A deterministic synthetic-universe generator (seeded) producing Kalshi-shaped market metadata and books.
- A loopback wire mock that speaks the Kalshi order-book and lifecycle protocol (REST discovery paginated; WebSocket `orderbook_delta` snapshots/deltas; `market_lifecycle_v2`) at 10,000–175,000 symbols.
- Workload matrices for uniform, skewed/hot-market, quiet, deep-book, snapshot-storm, lifecycle-burst, burst, backpressure, disconnect/reconnect, and market churn scenarios.
- Machine-readable run manifests committed as `measurements/stress-*.json`.

Explicitly out of scope (do not pull into this PR):

- Adaptive sharding / automatic capacity control (PR #44).
- Failover/continuity hardening beyond the existing epoch and dedup behavior (PR #43).
- Any venue protocol migration, host rewrite, or dependency adoption.
- 24-hour soak runs (PR #45).
- Live venue load testing. Kalshi production permission remains unresolved; this PR is synthetic.
- Polymarket full-universe runs as a correctness gate; Polymarket flavor is kept for continuity with the 100k baseline only.

Venue-native accounting is mandatory: Kalshi counts **tickers/markets**; Polymarket counts **outcome tokens**. Reports must state both the venue-native count and the connection/subscription counts, and must never present the two venues' counts as the same unit.

## 3. Deterministic synthetic universe

Seeded generator with an in-tree PRNG (SplitMix64; **no new dependencies**). All randomness flows from the seed recorded in the manifest.

`MarketSpec` for index `i` in `0..symbols`:

- ticker: `STRESS-{seed:04}-{i:06}` (native id; SBE symbol identical)
- `event_ticker`, title, `status: "active"`, `market_type` excluded from MVE filter path exercised deliberately in the churn scenarios (an MVE-typed subset is generated for the rejection path)
- `close_time` staggered so churn and retirement scenarios can be driven deterministically
- `price_ranges` default `0..1 @ 0.01` — a deep-book scenario uses fine-tick variants (`0.001`)
- numeric spec: USD, contracts, `price_scale = 6`

Book shape families (per market, chosen by sub-seed):

- `sparse`: 1 bid/ask level (baseline from the 100k topology)
- `deep`: 10 levels per side
- `locked-wide`: wide spread (quiet market)
- `one-sided`: empty side (empty-book retirement propagation testing)

The generator is **not** a replay of captured data: captures from venues would be a rights problem and would defeat determinism. Synthetic only.

## 4. Workload matrix

| Axis | Declared values |
|---|---|
| Symbol count | 10k, 50k, 100k, 150k, 175k |
| Shards | derived from `max_symbols_per_socket ∈ {500, 1000, 2000, 5000, 10000}` |
| Replicas | 1, 2 |
| Distributions | `uniform`, `zipf` (≈10% of markets ≈ 80% of deltas), `burst` (rate ×N for a window), `quiet` (no deltas, keepalives only), `hot-pair` (2 books carrying most volume) |
| Structural | `empty-book`, `deep-book`, `lifecycle-churn` (create/activate/close/settle), `snapshot-storm` (simultaneous reconnect + mass snapshot), `backpressure` (induced writer stall), `disconnect` (one shard socket loss), `market-retirement` (discovery retires a set mid-run) |
| Lifecycle mix | created/activated/price-update/settled/deactivated, seeded, both replicas receiving |

Scenario case definitions are committed as literal fixtures (`fixtures/stress/*.json`) with `{ seed, symbols, shard_size_cap, replicas, distribution, structural, duration_secs }` — a case is reproducible from a single manifest without regenerating the generator.

## 5. Execution protocol

1. Build the collector once (`--release`) with the pinned toolchain; record `git rev-parse HEAD`, `sha256(target/release/engineering-challenge)`, `rustc --version`.
2. Record host: OS kernel, CPU model + count, RAM, file-descriptor limit (`ulimit -n`), disk free, clock-sync state.
3. For each case:
   - start collector against the loopback mock with fixed config fingerprint
   - wait for full anchor (`collector_symbols_anchored == expected`, bounded; if exceeded, the cell is a failure, not a slow pass)
   - run the workload for the defined duration with the receive clock recorded per frame
   - induce each structural fault per scenario
   - stop under SIGINT; confirm final snapshots + drain
4. Verify per run: `verify` (valid), `completeness` (valid), `coverage` (valid, exact counts), journal manifest (`complete=true`), report counters. Every unmatched counter is an acceptance failure.
5. Emit one manifest per cell.

### Manifest schema (minimum fields)

```json
{
  "schema_version": 1,
  "source_commit": "…",
  "binary_sha256": "…",
  "toolchain": "…",
  "environment": { "os": "…", "kernel": "…", "cpu": "…", "ram_mb": 0, "fd_limit": 0, "clock_sync": "…" },
  "config_fingerprint": "…",
  "scenario": { "seed": 0, "symbols": 0, "shard_size_cap": 0, "shards": 0, "replicas": 1,
                "lifecycle_replicas": 1, "distribution": "uniform", "structural": "none",
                "duration_s": 0 },
  "counts": { "markets_native": 0, "subscriptions": 0, "messages": 0, "snapshots": 0,
              "deltas": 0, "bytes_in": 0, "bytes_out": 0 },
  "performance": { "msgs_per_sec_p50": 0, "msgs_per_sec_p95": 0, "msgs_per_sec_p99": 0,
                   "msgs_per_sec_max": 0, "receive_to_apply_p50_us": 0, "receive_to_apply_p99_us": 0,
                   "bytes_per_sec_out": 0, "rss_peak_bytes": 0, "cpu_seconds": 0,
                   "snapshot_anchor_ms": 0, "writer_queue_high_water": 0, "writer_dropped": 0 },
  "coverage": { "expected": 0, "acknowledged": 0, "anchored": 0, "missing_ack": 0,
                "missing_anchor": 0, "expected_set_sha256": "…", "coverage_valid": true },
  "incidents": { "reconnects": 0, "failovers": 0, "sequence_gaps": 0, "stale_drops": 0,
                 "lifecycle_duplicates": 0, "unexplained_drops": 0 },
  "verify": { "verify_valid": true, "completeness_valid": true, "journal_complete": true },
  "repetition": 1, "git_dirty": false
}
```

Every manifest is written by the harness itself (not hand-copied) into `measurements/stress-<cell>-repN.json` and checked in only after the cell passes acceptance; failing cells are preserved in a local `measurements/stress-failed/` and never checked in.

## 6. Measurements

Per cell (median over 3 repetitions; spread reported, never cherry-picked):

- **msgs/s** and **bytes/s** (in and out), message-rate distribution
- **receive→apply latency**, p50/p95/p99/max, with clock metadata (`chrony`/`systemsetup` measurement on the reference machine)
- **RSS peak**, **CPU seconds**, memory/symbol, allocator behaviour (system allocator; no custom allocator in this PR)
- **writer queue high-water mark** and **drop count** (must be 0 for non-fault scenarios)
- **snapshot anchor time** (connect → last symbol anchored)
- **coverage counts** exactly as `coverage` computes them
- **writer dropped / rejected** must be 0 for non-fault scenarios
- correction counts (out-of-order nonterminal drops, lifecycle duplicates) for lifecycle scenarios

## 7. Acceptance criteria

A cell **fails** if any holds:

1. `coverage.valid ≠ true`, or `expected/acknowledged/anchored` do not reconcile exactly
2. `verify.valid ≠ true` or `completeness.valid ≠ true` or journal not complete/finalized
3. Any unexplained drop, duplicate application, negative quantity, snapshot-before-delta violation, or corrupted SBE message
4. Writer queue exceeds the configured bound, or writer reports dropped/rejected writes in a non-fault scenario
5. RSS grows unboundedly across repetitions (trend check over the 175k cell) or any `u64` counter wraps
6. Anchor time exceeds the declared deadline for the symbol count (declared per cell; miss = fail, not "slow machine")

The **smoke cell** runs in ordinary CI: 4k symbols, uniform + one zipf case, one lifecycle burst, one disconnect, ~10–20 s, must pass identically on every PR (no wall-clock thresholds — only invariants and coverage). Full matrix is opt-in (`#[ignore]`) and run on the operator's machine with a recorded host profile; results are checked in as measurement manifests, never produced in PR CI.

## 8. Deliverables in the PR

1. `tests/synthetic_kalshi_mock.rs` — seeded Kalshi wire mock (REST discovery with cursor pagination; WS `orderbook_delta` with batching; `market_lifecycle_v2`; reconnectable connections)
2. `tests/full_universe_stress.rs` — scenario cases; `#[ignore]`d full matrix + CI smoke; per-cell JSON manifest writer
3. `measurements/README.md` — how to reproduce a cell (exact command, host stamp, repetition policy)
4. `docs/requirement-test-matrix.md` — new "Synthetic stress capacity (opt-in)" row with measured results, explicitly labelled **synthetic**
5. `docs/operating-envelope.md` — updated capacity table only from measured cells; no projection presented as measurement

## 9. Open questions (decisions requested)

1. **Repo venue identity for stress fixtures**: generate `STRESS-…` tickers (synthetic, clearly marked) vs realistic `KX-…` tickers — recommend the former (never confused for venue data); say the word otherwise.
2. **Disk budget for 150k/175k runs**: cap duration at anchor+60 s with mandatory chunk rotation; acceptable?
3. **Report location**: `measurements/` checked in vs git-ignored local-only — recommend checked in (reproducibility is the point of the exercise).
4. **Checksum of captures in the manifest**: include `sha256` of the run's SBE capture only for cells the operator marks releasable (capturing 150k-symbol output could be hundreds of MB); default = counts only, not hashes of multi-GB files. Acceptable?

## 10. Verification (this design commit only)

Docs only; no build impact. Gates to remain green: `cargo fmt --check`, `clippy --all-targets --locked -- -D warnings`, full test suite, `check_architecture.py`, `check_matrix_tests.py`, Mermaid. No venue contact, no dependencies, no schedule risk.

## 11. Sequencing after this design is approved

Implementation of sections 8.1–8.2 behind `#[ignore]` first; smoke case enabled in CI; then the first real 100k cell and 150k cells on the reference machine; then docs updated from measured data; then PR opened from `main` as usual.

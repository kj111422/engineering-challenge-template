# PR #42 — Deterministic full-universe stress laboratory (design)

**Status:** design approved in principle; **this revision incorporates every review amendment**.
**Branch:** `feat/full-universe-stress-lab` · **Base:** `b66fdbd`
**Author date:** 2026-11-14 · **Evidence class:** synthetic/loopback only

## 1. Objective

Benchmark the collector that ships. Every synthetic frame enters through the
real venue adapter (Kalshi first, Polymarket second), the real coordinator,
books, bounded writer, SBE output, journal, and the real verifier:

```
Synthetic venue frames
        │
        v
Real REST / WebSocket adapter (production code path)
        │
        v
Production decoding and normalization
        │
        v
Discovery + capacity::plan (recorded, not reimplemented)
        │
        v
Real coordinator and shards
        │
        v
Order-book reconstruction (production core)
        │
        v
Bounded writer ─────────► SBE capture
        │                 Raw journal
        v
Independent verification ─► coverage certificate + run manifest
```

A run is a **pass** only when exact coverage reconciles and every event is
accounted for. A throughput claim without that is noise.

## 2. Universe definition (mandatory correction)

Two disjoint sets, both generated deterministically:

1. **Eligible markets** — regular challenge-in-scope markets that the
   production MVE filter accepts. These are discovered, subscribed and
   snapshot-anchored; they are the expected universe.
2. **Ineligible decoys** — MVE markets plus other excluded classes
   (inactive/non-orderbook/invalid records), generated as negative controls.
   They MUST be excluded by discovery and MUST NOT appear in the expected
   sets or hashes.

The synthetic identity format stays `STRESS-{seed:04}-{i:06}` for both sets;
eligibility, not appearance, distinguishes them. Reports record three exact
counts — `generated`, `eligible`, and `excluded` — plus the number actually
discovered. Vector-accuracy tests prove the filter rejects the decoys.

## 3. Bounded experiment phases (not a Cartesian product)

| Phase | Universe | Workloads | Repetitions | Purpose |
|---|---|---|---|---|
| CI smoke | 4k | uniform + zipf + lifecycle-burst + disconnect | 1 | Regression gate; correctness only |
| Correctness suite | 1k–10k | all failure scenarios, every distribution | deterministic | Invariant proofs |
| Capacity sweep | 10k, 50k, 100k, 150k, 175k | uniform | 3 | Sustainable capacity |
| Workload-shape sweep | 10k and 50k | uniform, zipf (≈10% of markets ≈80% of deltas), burst, quiet, deep-book, lifecycle-burst, hot-pair | 3 | Bottleneck identification |
| Recovery sweep | 10k and 50k | interruption (1-replica and total outage), snapshot storm, retirement, backpressure | 3 | Bounded recovery |
| Polymarket regression | 100k, opt-in | uniform | 3 | Preserve existing baseline |

The capacity sweep exercises `max_symbols_per_socket ∈ {500, 1000, 2000,
5000, 10000}` at one and two book replicas against every listed universe
size — capacities and replicas are the only fully crossed axes. All other
workload runs use one declared representative topology per universe size
(e.g. `max_symbols_per_socket = 5000`, `replicas = 2`).

The full matrix is **opt-in and manual** on a recorded reference machine; it
is never an ordinary CI gate, and no wall-clock threshold is gated on in CI.

## 4. Event accounting ledger (mandatory correction)

The harness keeps an independent expected-event ledger and reconciles it
against production-counter evidence. Counters, at minimum:

| Stage | Counter |
|---|---|
| Generator | `generated_logical_events` |
| Mock server | `transmitted_frames` (per connection and in aggregate) |
| Collector ingress | `received_frames` |
| Decoder | `decoded_frames` |
| Core | `applied_events` (post-dedup) |
| Deduplication | `suppressed_replica_duplicates` |
| Writer | `writer_accepted` (deltas and snapshots) |
| SBE persistence | `persisted_sbe_records` |
| Journal | `journaled_frames` (raw) |

Definitions:

- A logical event delivered to both replicas is a **transport duplicate**,
  not a duplicate application. `suppressed_replica_duplicates` counts exactly
  those merges (or the core-side drops in the #41 dedup/monotonic paths,
  labelled by reason).
- A frame never delivered during an injected outage is an **availability
  gap**, not an unexplained decoder drop; it is reported as such with time
  bounds.
- Writer-accepted but absent from the finalized SBE file is a **persistence
  failure**.
- Every discrepancy is classified exactly one way: `unexplained_drops`,
  `availability_gap`, `persistence_failure`, `overflow`, `order_violation`,
  `replica_transport_duplicate`. `unexplained_drops` is derived, never
  hard-coded to zero. Any non-zero value fails the cell.

## 5. Exact coverage certification

For every universe-complete cell, the harness computes:

```
expected_set_sha256
acknowledged_set_sha256
anchored_set_sha256
```

over the canonical encoding (same definition as `coverage.rs`: sorted,
newline-joined, SHA-256) **and** performs the explicit three-way set
comparison. The sets must be equal exactly; count equality is not accepted.
The decoy set must be disjoint from all three; a positive hit is a filter
failure and fails the cell.

For lifecycle-churn cells, coverage is computed **per universe epoch**:
newly created markets join the expected set once discovered and anchored;
retired markets exit the expected set at the retirement boundary. Hashes are
recorded per epoch.

## 6. Clocks and percentile methodology

- Intra-process intervals use a monotonic clock measured at both points.
  Wall-clock synchronization is required only where timestamps from
  independent clocks are compared (venue `sending_ts_ms` vs local receive).
- Recorded per run: clock type (`CLOCK_MONOTONIC`/equivalent) and resolution.
- Throughput percentiles (`p50/p95/p99/max`) are computed over **fixed-width
  one-second bins** over the measured interval, not arbitrary sampling
  windows. The binning rule is recorded in the manifest.
- Latency histograms are reported for: ingress→decode, decode→apply,
  apply→writer-accept, writer-accept→persistence, each with explicit units
  (`us`). Synthetic source→collector latency is never conflated with venue
  feed lag; the two are labelled separately.
- Correctness-mode runs use controlled event order (virtual event time);
  benchmark runs never rely on virtual time. Virtual time is not used to
  claim throughput or latency.

## 7. Failure semantics (mandatory explicit outcomes)

| Scenario | Expected outcome |
|---|---|
| One-replica loss | Standby stays active; failover snapshot anchors before any promoted deltas; incident recorded; no continuity gap for unaffected books |
| Total source outage | Continuity declared unavailable for affected symbols until reconnect + fresh snapshot re-anchoring; no event-history completeness claim; gap incident recorded with bounds |
| Snapshot storm | Re-anchoring bounded in time and queue; recovery duration and resync counts measured; disk stays under ceiling |
| Backpressure | Writer queue bounded; overload results in documented backpressure or visible failure; accepted events never silently dropped; run failure classified (`backpressure` or `queue_saturated`) |
| SIGINT (child process) | Real signal delivered to a separate collector process; drain, final snapshots, journal finalize and exit code verified; accepted data not lost |
| Lifecycle bursts | Created/Activated/Updated/Settled/unknown-symbol events exercised; `#41` lifecycle plane used, not bypassed; REST reconciliation races resolved per #41 ordering semantics |
| Deep/empty books | One-sided or empty book is a valid book state, never a retirement; unmatched verification would fail |
| RSS drift | Repeated same-process workload cycles must show no unbounded trend (repetition independence does not prove this; in-process repetition does) |

## 8. Manifest contract (versioned)

One manifest per cell, plus `measurements/stress/index.json` and per-cell
`summary.json`. Compact manifests for both successful **and failed** runs
are checked into the predeclared reference matrix. Raw captures stay out of
Git. The schema is `schema_version: 1` from its first release.

Required fields (in addition to names in §4, §5, §6):

```
run_id                    configuration_sha256
execution_mode            build_profile (`release`)
evidence_class            target_triple
generator_version         rustc_version
prng_algorithm_version    collector_binary_sha256
scenario_version          mock_server_binary_sha256   (if separate mock process)
collector                 { desired_topology, actual_topology, book_replicas,
                            lifecycle_replicas, capacity_plan ... }
performance               { msgs_per_sec_p50/p95/p99/max over 1s bins,
                            ingress_decode_p50_us, decode_apply_p50_us,
                            apply_writer_p50_us, writer_persist_p50_us,
                            bytes_per_sec_in/out, rss_peak_bytes, cpu_seconds,
                            memory_per_symbol_bytes, snapshot_anchor_ms,
                            stream_duration_s }
disk                      { bytes_written, write_latency_p50_us,
                            sync_latency_p50_us, chunks_rotated, disk_ceiling_bytes,
                            disk_ceiling_reached: false }
coverage                  { expected, acknowledged, anchored, missing_ack,
                            missing_anchor, expected_set_sha256,
                            acknowledged_set_sha256, anchored_set_sha256,
                            decoy_set_absent: true, per_epoch: [...],
                            coverage_valid }
incidents                 { reconnects, failovers, sequence_gaps,
                            availability_gaps, persistence_failures,
                            unexplained_drops, stale_drops, lifecycle_duplicates,
                            backpressure_events, queue_saturated_events }
verify                    { verify_valid, completeness_valid, coverage_valid,
                            journal_complete }
run_status                success | failed | incomplete | interrupted
failure_class             null on success; classified string otherwise
disk                      { free_bytes_before_start, bytes_written,
                            disk_ceiling_bytes, disk_ceiling_reached }
max_tick_time_s?          n/a
```

`host` measurements use `null` with a `reason` when unavailable — never `0`.
Manifests are written atomically (temp file + rename) and on every exit path,
including failures, so the index never degenerates to a truncated valid-looking
record. A supervisor can finalize an `interrupted` manifest even when the
collector dies abnormally.

The mock reports its own generator throughput independently; if the mock
cannot sustain the configured offered rate, the cell is invalid as a
collector-capacity measurement and is labelled `mock_throughput_limited`.

## 9. Implementation placement and constraints

- `tests/stress_support/*` — synthetic generator, workload definitions, the
  event ledger, manifest writer, host helpers, the Kalshi wire mock. No
  production module is added. `src/` is untouched unless shared production
  code is genuinely required.
- No new external dependencies. SplitMix64 is specified inline with test
  vectors and a fixed algorithm version constant; generation order and
  ordering are stable across Rust versions.
- The mock is a real WS/REST server per test process (loopback), not a
  shortcut through discovery helpers. REST pagination (cursor), subscribe
  acknowledgements with per-connection sid and seq, snapshots, deltas,
  lifecycle frames, and connection-failure races are all implemented.
- `capacity::plan` is called by the collector under test; the harness records
  the desired vs actual topology and fails on a mismatch.
- CI runs exactly one small correctness smoke (seeded, fixed cases:
  uniform + zipf + lifecycle burst + disconnect; no wall-clock thresholds).
- Raw captures go to a configured run directory (git-ignored) or are emitted
  as run artifacts; compact manifests and summaries are checked in under
  `measurements/stress/`.

## 10. Disk and resource limits

Per run: a total-byte ceiling (`STRESS_DISK_CEILING`) is checked before
start (free space must exceed ceiling + reserved-for-finalization bytes),
includes the initial snapshot phase, and stops the run visibly when reached.
A storage-ceiling interruption produces `run_status: failed` with
`failure_class: disk_ceiling`, not a shortened passing capture. Manifests
record `free_bytes_before_start`, `disk_ceiling_bytes`, `disk_ceiling_reached`.
Chunk rotation is mandatory. Raw captures are retained outside Git.

## 11. Execution sequence (reviewable increments)

1. Ledger + generator + deterministic RNG vectors (unit tests)
2. Kalshi wire mock (REST pagination, WS subscribe/ack/snapshot/delta/lifecycle, reconnect) + its own tests
3. Manifest writer + host helpers + index (unit tests incl. failure manifest)
4. CI smoke case (4k) — the only thing that runs in Ordinary CI
5. Correctness suite cases (1k–10k)
6. Opt-in capacity/workload/recovery matrix wiring (`STRESS_*` env), `measurements/stress/` layout, README
7. Sample run + sample deliberately-failed run (for review)

## 12. Acceptance (as enforced by the suite)

1. Production adapter/pipeline is the only ingest path; both mock flavours
   exercise the same code as production.
2. Regular eligible universe discovered, subscribed, anchored; decoys absent;
   exact set equality on expected/acknowledged/anchored.
3. Every generated event accounted for or explicitly classified.
4. Replica transport duplicates distinguished from duplicate applications.
5. Backpressure bounded/visible; no accepted event silently dropped.
6. Snapshot re-anchoring and incident accounting correct across
   one-replica and total-outage scenarios.
7. SIGINT-child-process shutdown drains accepted data and finalizes.
8. `verify`, `completeness`, `coverage` and journal pass on release runs.
9. Three repetitions; medians and spread reported; no cherry-picking.
10. Versioned manifests on success and failure; index stays consistent.

## 13. Success artifacts (for review)

- One successful reference-matrix manifest (e.g. 100k uniform, 5000/socket)
- One deliberately failed manifest (e.g. induced backpressure or dropped anchor)
- One reproduced backpressure scenario and one reproduced recovery scenario
- Exact test commands and counts

## 14. Explicitly not in this PR

Adaptive sharding, continuity redesign, protocol migrations/hosts,
dependency governance, CLI flags, new external dependencies, venue-data use,
#43/#44 work.

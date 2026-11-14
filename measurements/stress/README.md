# Stress laboratory reference manifests

These are the stability anchors for `tests/full_universe_stress.rs`. Each
manifest records a real run of the collector against the synthetic loopback
Kalshi venue; they name the exact source commit and binary they were built
from and carry the worktree provenance that proves a clean generation state.
They are small enough to review.

Every manifest records `evidence_class: synthetic-loopback`. **Nothing here is
venue data, nothing here is a live result, and none of it is a capacity or
performance claim** — the 1k artifact that previously lived here was an
initial harness smoke demonstration and has been retired; the correctness
anchors below are the 4k CI smoke target and deterministic failure
injections.

## Contents

| File | Purpose |
|---|---|
| `index.json` | Cell index; every entry points at one manifest. |
| `seed-0007-4000-uniform.json` | Successful 4,000-symbol uniform smoke cell: 1 shard × 2 replicas + 1 lifecycle socket, 200 deltas. Exact universe reconciliation is visible: expected / acknowledged / snapshot-anchored / observed sets hash to one value; the event ledger reconciles (`generated = applied = written`, `unexplained_drops = 0`); the drain barrier completed before shutdown; mock `enqueued = socket_written`, `queue_saturated = 0`. |
| `failed-seed-000b-200-coverage_set_mismatch.json` | Missing-anchor injection: one eligible market receives no snapshot. It is acknowledged but never snapshot-anchored, the three-set comparison fails, and the runner classifies the cell `failed` (`coverage_set_mismatch`) — the event ledger itself still reconciles, which is exactly the point: the loss is a coverage loss, not an accounting loss. |
| `failed-seed-000d-300-unexplained_drops.json` | Dropped-frame injection: 10 delta frames vanish before the wire (no sequence consumed, invisible to the protocol). Only event accounting catches it: `unexplained_drops = 5` (10 frames / 2 replicas), `injected_pre_wire_drops = 10`, classified `failed`. |
| `failed-seed-000e-200-coverage_set_mismatch.json` | Bounded-queue saturation: the mock's outbound queue is capped at 8 and its forwarder stalled. `queue_saturated > 0`, nothing drains, the barrier refuses to certify; the cell is `failed`, never silently degraded. |
| `failed-seed-0009-1000-collector_terminated_early__max_total_connections_must_be____replicas.json` | Capacity-planner rejection (`force_capacity_reject`): the exact planner reason (`max_total_connections must be >= replicas`) is preserved verbatim in `failure_class`. An infeasible topology is recorded, never skipped. |

## What the manifests prove (and what they do not)

- The runner computes the acceptance verdict itself; a `run_status: success`
  manifest is impossible without completed anchoring, a proven delivery/drain
  barrier over production `/metrics` counters, coverage, SBE `verify`,
  completeness, journal finalization, decoy exclusion, and a reconciled event
  ledger.
- `unexplained_drops` is always derived from independent stage counters, never
  hard-coded; an unobservable stage leaves the value `null` and blocks
  success.
- Resource and provenance fields are honest: `harness_process_rss_sample_bytes`
  (a single sample of the whole test process — collector, mock and harness
  share it, so there is deliberately no memory-per-symbol figure), measured
  `clock_resolution_ns` (or `null` with a reason), derived `build_profile`,
  `stress_harness_binary_sha256`, and worktree provenance (tracked changes +
  diff hash) captured before the run started.
- This directory does not establish live venue behavior, throughput, or
  capacity envelopes. Those require the bounded phase matrix, which has not
  been authorized to run yet.

## Reproducing

Generate into a scratch directory from a committed, clean worktree (the
manifests embed the provenance, so a dirty tree shows up in them):

```bash
SCRATCH=$(mktemp -d)
STRESS_MANIFEST_OUT=$SCRATCH \
  cargo test --release --locked --test full_universe_stress -- \
  --ignored reference_ --test-threads=1 --nocapture

# Inspect, then copy the manifests and index.json here.
cp $SCRATCH/*.json measurements/stress/
```

Release mode is used for the reference cells (recorded as
`build_profile: release`). `--test-threads=1` keeps the per-cell index
upserts serialized. The ordinary CI smoke runs the same code path without
setting `STRESS_MANIFEST_OUT`, so it leaves no files behind.

## Rules

- No large captures are ever checked into this directory. Compact manifests
  and the index only.
- A missing host measurement is serialized as `null` with a reason, never as
  `0`.
- `index.json` is updated by the harness writer (`upsert_cell`), which
  validates the existing index before any artifact is written; a malformed
  index fails closed with its bytes untouched.

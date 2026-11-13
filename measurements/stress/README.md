# Stress laboratory reference manifests

These are the stability anchors for `tests/full_universe_stress.rs`. Each
manifest records a real run of the collector against the synthetic loopback
venues; they were produced on the reference machine noted inside and name the
exact source commit they were built from. They are small enough to review.

## Contents

| File | Purpose |
|---|---|
| `index.json` | Cell index; every entry points at one manifest. |
| `reference-1000-uniform.json` | Successful 1,000-symbol uniform cell, 1 shard x 2 replicas + 1 lifecycle socket. Exact universe reconciliation is visible: expected/acknowledged/anchored sets hash to the same value. |
| `reference-failed.json` | Deliberately injected failure (`force_capacity_reject`): the capacity plan rejected the configuration before any socket opened. The harness still wrote a failure manifest naming the cause. |

## Reproducing

```bash
# Success artifact (small)
STRESS_MANIFEST_OUT=measurements/stress \
  cargo test --locked --release --test full_universe_stress -- smoke_cell --nocapture

# Failure artifact
STRESS_MANIFEST_OUT=measurements/stress \
  cargo test --locked --release --test full_universe_stress -- reference_failed --ignored --nocapture
```

Release mode is used for the reference cells. The ordinary CI smoke uses the
same code path without setting `STRESS_MANIFEST_OUT`, so it leaves no files
behind.

## Rules

- No large captures are ever checked into this directory. Compact manifests
  and the index only.
- Every manifest carries `evidence_class: synthetic-loopback`. Nothing here
  is venue data, and nothing here is a live result.
- A missing host measurement is serialized as `null` with a reason, never as
  `0`.
- `index.json` is updated by the harness writer (`upsert_cell`); manual edits
  are expected only for renaming convenience entries.

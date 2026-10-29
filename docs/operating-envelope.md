# Operating Envelope

This document records the measured capacity envelope of the collector, the capacity math that derives hard limits, and the gate status for production readiness.

## Measured Scale Evidence

All measurements taken on macOS (Apple Silicon) with the collector built in `--release` mode, using the in-process mock exchange (`opt_in_scale_run`). The mock exchange delivers one initial snapshot per subscribed asset and one price-level delta per asset.

| Symbols | Shards | Replicas | Connections | Subscriptions | Connect+Subscribe | Anchor | Settle | Total | RSS (KB) | Output (MB) |
|---------|--------|----------|-------------|---------------|-------------------|--------|--------|-------|----------|-------------|
| 1,000   | 1      | 2        | 2           | 2,000         | 11 ms             | 1,016 ms | 3,025 ms | 3,025 ms | 24,832   | 0.26        |
| 5,000   | 1      | 2        | 2           | 10,000        | 47 ms             | 1,053 ms | 3,084 ms | 3,084 ms | 77,120   | 1.28        |
| 25,000  | 5      | 2        | 10          | 50,000        | 288 ms            | 1,318 ms | 3,438 ms | 3,438 ms | 275,360  | 6.34        |
| 100,000 | 20     | 2        | 40          | 200,000       | 1,202 ms          | 2,256 ms | 5,053 ms | 5,053 ms | 696,448  | 25.33       |

**Key observations:**

- Connection establishment and subscription handshake scale linearly with connection count (1.2s for 40 connections).
- Initial snapshot processing (anchoring) dominates the settle phase and scales with total subscriptions.
- Memory grows roughly linearly with outcome symbols (~7 KB/symbol at 100k).
- Writer throughput is not a bottleneck at these scales (no rotation segments created; all output fits in the first chunk).
- The collector processes 200k subscriptions and 100k deltas in ~5 seconds end-to-end.

## Capacity Math

The collector enforces hard startup gates via [`capacity::plan`](../src/capacity.rs). A configuration is rejected if any of the following bounds would be exceeded:

| Limit | Formula | Default | Rationale |
|-------|---------|---------|-----------|
| `max_symbols_per_socket` | `symbols / shards` | 5,000 | Polymarket WebSocket frame size limits; empirical stability ceiling. |
| `max_total_connections` | `shards × replicas` | 64 | Built-in planning default; deployment overrides must be stated separately. |
| `max_initial_subscription_bytes` | `overhead + max_symbols_per_socket × (longest_symbol + 3)` | 1 MiB | Initial `market` subscribe frame must fit in one WebSocket message. |
| `max_estimated_book_memory_mb` | `outcome_symbols × 4096 bytes` | 1 GiB | Planning estimate, not an RSS ceiling: book level maps are unbounded. |
| `max_snapshot_bytes_per_sec` | `outcome_symbols × snapshot_size / snapshot_interval` | 16 MiB/s | Planning estimate for periodic snapshot bandwidth. |
| `max_shards` | — | 64 | Coordinator task sprawl; each shard adds a `tick()` iteration. |

### Derived Shard Count

The coordinator derives the shard count from the selected market set:

```rust
let shards = (outcome_symbols + max_symbols_per_socket - 1) / max_symbols_per_socket;
shards = shards.clamp(1, max_shards);
```

If the derived shard count exceeds `max_shards`, the collector refuses to start. This is a hard gate, not a soft degradation.

### Full-Universe Rejection

Polymarket Gamma currently publishes **~274,471 active conditions** with **2 outcomes each = 548,942 outcome tokens**.

At the default `max_symbols_per_socket = 5,000`:

```
required_shards = ceil(548,942 / 5,000) = 110
```

With `replicas = 2`:
```
required_connections = 110 × 2 = 220
```

The default `max_shards = 64` rejects this configuration at startup:

```
error: derived shard count 110 exceeds max_shards 64
```

**Two-replica full-universe collection is unsupported** with current defaults. To approach full-universe coverage, the following would need measured validation:

1. Increase `max_shards` to ≥ 110 (requires coordinator scalability validation).
2. Increase `max_total_connections` to ≥ 220 (requires FD limit validation).
3. Validate a deployment override for `max_estimated_book_memory_mb` ≥ 2.2 GiB (548,942 × 4 KiB planning estimate).
4. Validate a deployment override for `max_snapshot_bytes_per_sec` ≥ 74.9 MB/s (548,942 × 8 KiB / 60s planning estimate).
5. Run 24-hour soak at full scale with fault injection (unexecuted gate).

Until these are measured, the collector **must not claim production readiness for full-universe collection**.

## Configuration Reference

All capacity knobs live under `collector.*` in the settings file:

```toml
[collector]
shards = 0              # 0 = derive from max_symbols_per_socket
replicas = 2
max_symbols_per_socket = 5000
max_shards = 64
max_total_connections = 512
max_initial_subscription_bytes = 1048576
max_estimated_book_memory_mb = 4096
max_snapshot_bytes_per_sec = 67108864
snapshot_interval_secs = 60
```

## Readiness Gates

The `/readyz` endpoint enforces the following invariants before returning `200 OK`:

| Check | Threshold | Metric |
|-------|-----------|--------|
| Sources | All shards have ≥1 connected replica | `collector_shards_with_source == collector_shards` |
| Anchored | ≥ 99% of expected symbols have an exchange-anchored book | `collector_anchor_fraction ≥ 0.99` |
| Freshness | Zero anchored symbols stale (> 120s by default) | `collector_symbols_stale == 0` |
| Catalog | Last accepted discovery round ≤ 1 hour old | `collector_catalog_age_seconds ≤ 3600` |
| Writer | Healthy, zero errors | `collector_writer_healthy && collector_writer_errors_total == 0` |
| Persistence | No pending write failure | `collector_persistence_last_error == null` |
| Fatal | No fatal error recorded | `collector_fatal_error == null` |

The `/livez` endpoint returns `200 OK` as long as the process can answer HTTP. The `/healthz` endpoint returns source connectivity status (`starting`/`ok`/`degraded`) and the `ready` flag from `/readyz`.

## Unresolved Gates

| Gate | Status | Evidence Required |
|------|--------|-------------------|
| 24-hour soak (no faults) | **Not executed** | Continuous run with live or replay source, zero data loss, zero restarts. |
| 24-hour soak (with faults) | **Not executed** | Same with injected socket loss, exchange maintenance resets, network partitions. |
| Security review | **Not executed** | Dependency audit (`cargo audit`), supply chain review, secrets handling. |
| Deployment review | **Not executed** | systemd unit, log rotation, alerting rules, runbook validation. |
| CI on exact commit | **Pending** | All gates green on the commit that will be tagged/released. |

## Claim Boundaries

| Claim | Supported? | Evidence |
|-------|------------|----------|
| "Collects 100k symbols with 2× replication" | ✅ | Measured: 100k/20/2 in 5s, 696 MB RSS |
| "Collects 548k symbols (full Polymarket)" | ❌ | Rejected by `max_shards` gate; unmeasured beyond 100k |
| "Survives socket loss with <1s failover" | ✅ | E2E test: `single_socket_loss_fails_over_and_emits_anchoring_snapshot` |
| "Survives full outage with uncovered interval reported" | ✅ | E2E test: `full_outage_recovery_exposes_the_uncovered_interval` |
| "Durable output with atomic rotation and checksums" | ✅ | E2E test: `rotation_produces_a_complete_manifest_and_readable_segments` |
| "Observability: liveness, readiness, Prometheus metrics" | ✅ | `/livez`, `/readyz`, `/metrics` endpoints tested |
| "24-hour production soak validated" | ❌ | **Gate not executed** |
| "Security reviewed and deployment hardened" | ❌ | **Gate not executed** |

Do not deploy to production without clearing all **Unresolved Gates**.

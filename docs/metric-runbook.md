# Metric Runbook

This runbook maps every Prometheus metric exposed by the collector to its meaning, expected range, and operational response.

## Endpoint

```
GET /metrics
```

Content-Type: `text/plain; version=0.0.4; charset=utf-8`

All metrics are prefixed with `collector_`.

---

## Counter Metrics (Monotonic)

| Metric | Type | Meaning | Expected Behavior | Alert If |
|--------|------|---------|-------------------|----------|
| `collector_exchange_snapshots_total` | Counter | Snapshots received from exchange WebSocket | Increases on every full book snapshot | Rate drops to 0 for > 60s (source stalled) |
| `collector_periodic_snapshots_total` | Counter | Locally generated periodic snapshots | Increases every `snapshot_interval_secs` per anchored symbol | Gap > 2× interval (tick loop stalled) |
| `collector_failover_snapshots_total` | Counter | Snapshots emitted on active source switch | Increases on failover; should be rare | Spike > 10/min (flapping sources) |
| `collector_final_snapshots_total` | Counter | Snapshots emitted during graceful shutdown | Exactly 1 per anchored symbol at shutdown | Non-zero outside shutdown |
| `collector_empty_snapshots_total` | Counter | Empty snapshots on symbol retirement | Increases on discovery churn (market close) | Unexpected increase (catalog corruption) |
| `collector_deltas_emitted_total` | **Counter** | **Price-level updates emitted to output** | **Primary throughput metric; should match exchange delta rate** | **Rate < 10% of expected (data loss)** |
| `collector_deltas_rejected_total` | Counter | Deltas rejected (crossed book, clock regression, invalid apply) | Low baseline; spikes on exchange maintenance | Rate > 1% of emitted (data quality issue) |
| `collector_sequence_resets_total` | Counter | Source resets from sequence gaps or protocol errors | Rare; only on exchange reset frames | Any increase (investigate source health) |
| `collector_clock_resets_total` | Counter | Exchange clock regressions treated as maintenance epochs | Expected on Kalshi maintenance; rare on Polymarket | Sustained increase (clock sync issue) |
| `collector_source_switches_total` | Counter | Active source failovers (same as failover snapshots + initial) | Low; one per symbol at startup, then on failover | Same as failover snapshots |
| `collector_stale_probes_total` | Counter | Explicit snapshot requests sent to quiet sources | Increases when symbols go stale | High rate = sources not delivering updates |
| `collector_stale_failovers_total` | Counter | Failovers after max probes exhausted | Should be zero in steady state | Any increase (source permanently lost) |
| `collector_crossed_snapshots_skipped_total` | Counter | Exchange snapshots rejected for crossed book | Rare; indicates exchange anomaly | Spike (exchange issue) |
| `collector_discovery_rounds_total` | Counter | Market discovery rounds completed | One per `discovery_interval_secs` | Gap > 2× interval (discovery stalled) |
| `collector_writer_messages_total` | Counter | SBE messages written to output | = snapshots + deltas | Gap vs `deltas_emitted` + snapshots |
| `collector_writer_bytes_total` | Counter | Bytes written (including chunk padding) | Monotonic | Stalled = writer blocked |
| `collector_writer_errors_total` | Counter | Writer errors (disk full, fsync, permission) | **Must remain 0** | **Any increase = data loss risk** |
| `collector_discovery_pages_total` | Counter | Gamma/Kalshi discovery pages read | Increases per discovery round | — |
| `collector_discovery_selected_markets_total` | Counter | Venue market records selected | Increases per discovery round | — |
| `collector_discovery_outcome_symbols_total` | Counter | Outcome tokens produced (Polymarket: 2 per condition) | Increases per discovery round | — |
| `collector_discovery_malformed_records_total` | Counter | Discovery records rejected as malformed | Should be 0 | Any increase (venue schema change) |
| `collector_discovery_retries_total` | Counter | Discovery HTTP retries (408/429/5xx) | Low; spikes on venue rate limiting | Sustained high rate (backoff not working) |
| `collector_discovery_failures_total` | Counter | Discovery rounds that returned an error | **Must remain 0** | **Any increase = catalog stale** |

---

## Gauge Metrics (Point-in-Time)

| Metric | Type | Meaning | Healthy Range | Alert If |
|--------|------|---------|---------------|----------|
| `collector_symbols_active` | Gauge | Symbols currently in the accepted catalog | = expected symbols | < expected (discovery shrinkage) |
| `collector_shards` | Gauge | Configured shard count | Constant after startup | Changes at runtime (bug) |
| `collector_symbols_expected` | Gauge | Symbols in the accepted catalog | = discovered symbols | < discovered (catalog filter bug) |
| `collector_symbols_anchored` | **Gauge** | **Symbols with an exchange-anchored active book** | **= expected (after anchor phase)** | **< 99% of expected (/readyz fails)** |
| `collector_symbols_stale` | **Gauge** | **Anchored symbols older than `readiness_max_stale_secs`** | **0** | **> 0 (/readyz fails)** |
| `collector_anchor_fraction` | Gauge | `anchored / expected` | ≥ 0.99 | < 0.99 (/readyz fails) |
| `collector_subscriptions_expected` | Gauge | Subscription entries configured sources should hold | = symbols × replicas | — |
| `collector_subscriptions_sent` | Gauge | Subscription entries acknowledged by connected sources | = expected (after handshake) | < expected (subscription gap) |
| `collector_shards_with_source` | **Gauge** | Shards with ≥1 connected replica | = shards | < shards (/readyz fails) |
| `collector_sources_connected` | Gauge | WebSocket sources currently connected | = shards × replicas | < expected (source down) |
| `collector_sources_configured` | Gauge | Total configured sources | Constant | — |
| `collector_ready` | Gauge | 1 if all readiness invariants pass, 0 otherwise | **1 in production** | **0 = not ready** |
| `collector_writer_healthy` | Gauge | 1 if writer has no fatal error | **1** | **0 = writer failed** |
| `collector_writer_queue_depth` | Gauge | Messages waiting for writer thread | < 1000 typically | > 10000 (backpressure) |
| `collector_writer_segments` | Gauge | Finalized rotation segments | Increases on rotation | — |
| `collector_persistence_pending` | Gauge | 1 while metadata/catalog write is queued | 0 or 1 (brief) | Stuck at 1 (worker stalled) |
| `collector_persistence_last_success_timestamp_seconds` | Gauge | Unix time of last successful persistence write | Recent (< 60s) | Stale > 300s |
| `collector_catalog_age_seconds` | Gauge | Age of last accepted catalog round; -1 if none | < 3600 (default) | > `readiness_max_catalog_age_secs` |
| `collector_discovery_age_seconds` | Gauge | Age of last complete discovery round; -1 if none | < `discovery_interval_secs` | > 2× interval |
| `collector_discovery_last_duration_ms` | Gauge | Duration of last discovery round | < 10,000 typically | > 30,000 (slow discovery) |
| `collector_discovery_last_pages` | Gauge | Pages read by last discovery round | > 0 | 0 (empty catalog) |

---

## Per-Source Metrics

| Metric | Labels | Meaning | Alert If |
|--------|--------|---------|----------|
| `collector_source_connected` | `source="shard/replica"` | 1 if connected, 0 otherwise | 0 for > 30s |
| `collector_source_reconnects` | `source="shard/replica"` | Total reconnect count | Rapid increase (flapping) |
| `collector_source_messages` | `source="shard/replica"` | Total messages received from this source | Stalled = source silent |
| `collector_source_decode_errors` | `source="shard/replica"` | Frames that failed SBE/JSON decode | Any increase (protocol drift) |
| `collector_source_protocol_errors` | `source="shard/replica"` | Protocol-level errors (unexpected frames) | Any increase |
| `collector_source_latency_ms` | `source="shard/replica"` | EWMA of receive-to-process latency | > 100ms (slow path) |

---

## Readiness Endpoints

### `/livez` — Liveness

```json
{"status":"alive","uptime_seconds":123.456}
```

- **Always returns 200 OK** if the process can answer HTTP.
- Use for Kubernetes `livenessProbe`.
- No dependency on data-path health.

### `/readyz` — Readiness

```json
{
  "status": "ready",
  "ready": true,
  "uptime_seconds": 123.456,
  "checks": {
    "sources": {"ok": true, "detail": "40/40 sources connected across 20/20 shards"},
    "anchored": {"ok": true, "detail": "100000/100000 symbols anchored (1.0000 required >= 0.9900)"},
    "freshness": {"ok": true, "detail": "0 anchored symbols older than 120 seconds"},
    "catalog": {"ok": true, "detail": "last accepted catalog round was 45 seconds ago (limit 3600)"},
    "writer": {"ok": true, "detail": "healthy=true, errors=0, queue_depth=0"},
    "persistence": {"ok": true, "detail": "no persistence failure"},
    "fatal": {"ok": true, "detail": "no fatal error"}
  },
  "coverage": { ... },
  "catalog": { ... },
  "persistence": { ... },
  "writer_healthy": true,
  "fatal_error": null
}
```

- Returns **200 OK** if `ready: true`, **503 Service Unavailable** otherwise.
- Use for Kubernetes `readinessProbe` and load balancer health checks.
- All checks must pass; no degraded mode.

### `/healthz` — Source Health (Legacy)

```json
{"status":"ok","ready":true,"uptime_seconds":123.456,"sources_connected":40,"sources_total":40}
```

- Returns **200 OK** always (liveness).
- `status`: `starting` (no sources yet), `ok` (all connected), `degraded` (some down).
- `ready`: mirrors `/readyz` readiness flag.
- Kept for backward compatibility; prefer `/readyz` for traffic routing.

---

## Dashboard Queries (PromQL)

### Throughput Health

```promql
# Delta emission rate (per second)
rate(collector_deltas_emitted_total[1m])

# Delta rejection rate
rate(collector_deltas_rejected_total[1m])

# Rejection ratio (should be < 0.01)
rate(collector_deltas_rejected_total[5m]) / rate(collector_deltas_emitted_total[5m])
```

### Coverage Health

```promql
# Anchor fraction
collector_symbols_anchored / collector_symbols_expected

# Stale symbols (should be 0)
collector_symbols_stale

# Shard coverage
collector_shards_with_source / collector_shards
```

### Source Health

```promql
# Connected sources
collector_sources_connected

# Per-source message rate
rate(collector_source_messages{source=~".*"}[1m])

# Per-source decode errors
rate(collector_source_decode_errors{source=~".*"}[5m])
```

### Writer Health

```promql
# Writer queue depth
collector_writer_queue_depth

# Writer errors (must be 0)
collector_writer_errors_total

# Write throughput
rate(collector_writer_bytes_total[1m])
```

### Discovery Health

```promql
# Discovery round rate
rate(collector_discovery_rounds_total[1h])

# Discovery failures (must be 0)
collector_discovery_failures_total

# Discovery retry rate
rate(collector_discovery_retries_total[5m])
```

### Persistence Health

```promql
# Time since last successful persistence write
time() - collector_persistence_last_success_timestamp_seconds

# Pending write stuck
collector_persistence_pending
```

---

## Alert Rules (Suggested)

```yaml
groups:
- name: collector
  rules:
  - alert: CollectorNotReady
    expr: collector_ready == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Collector not ready"
      description: "Readiness checks failing for {{ $labels.instance }}"

  - alert: CollectorDeltaRateLow
    expr: rate(collector_deltas_emitted_total[5m]) < 0.1 * rate(collector_deltas_emitted_total[1h])
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Delta emission rate dropped"
      description: "Current rate {{ $value }} is < 10% of 1h average"

  - alert: CollectorDeltasRejectedHigh
    expr: rate(collector_deltas_rejected_total[5m]) / rate(collector_deltas_emitted_total[5m]) > 0.01
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High delta rejection rate"
      description: "{{ $value | humanizePercentage }} of deltas rejected"

  - alert: CollectorSourceDown
    expr: collector_sources_connected < collector_sources_configured
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "WebSocket source down"
      description: "{{ $value }} of {{ $labels.collector_sources_configured }} sources connected"

  - alert: CollectorWriterErrors
    expr: increase(collector_writer_errors_total[1m]) > 0
    labels:
      severity: critical
    annotations:
      summary: "Writer errors detected"
      description: "Writer reported errors; data loss likely"

  - alert: CollectorCatalogStale
    expr: collector_catalog_age_seconds > 3600
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Catalog checkpoint stale"
      description: "Last accepted discovery round was {{ $value }} seconds ago"

  - alert: CollectorDiscoveryFailed
    expr: increase(collector_discovery_failures_total[5m]) > 0
    labels:
      severity: critical
    annotations:
      summary: "Market discovery failed"
      description: "Discovery round returned an error; catalog not refreshed"

  - alert: CollectorPersistenceStalled
    expr: collector_persistence_pending == 1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Persistence write stuck"
      description: "Metadata/catalog write has been pending for > 5 minutes"
```

---

## Troubleshooting Flowcharts

### `collector_ready == 0`

1. Check `/readyz` JSON for which check fails.
2. **sources**: Verify `collector_sources_connected == collector_sources_configured`. Check source logs for connection errors.
3. **anchored**: Check `collector_symbols_anchored` vs `collector_symbols_expected`. If gap, verify initial snapshots are being received (`collector_exchange_snapshots_total` increasing).
4. **freshness**: Check `collector_symbols_stale > 0`. Sources may be silent; check `collector_source_messages` rate per source.
5. **catalog**: Check `collector_catalog_age_seconds`. If > threshold, discovery may be failing (`collector_discovery_failures_total`).
6. **writer**: Check `collector_writer_errors_total > 0` or `collector_writer_healthy == 0`. Disk full? Permission?
7. **persistence**: Check `collector_persistence_last_error`. Disk full? JSON serialization error?
8. **fatal**: Check `collector_fatal_error`. Panic in coordinator loop?

### `collector_deltas_emitted_total` Stalled

1. Verify exchange is sending deltas: `rate(collector_source_messages[1m])` per source.
2. Check for rejections: `rate(collector_deltas_rejected_total[1m])`.
3. Check for source switches: `rate(collector_source_switches_total[1m])` — failovers pause emission.
4. Verify symbols are anchored: `collector_symbols_anchored == collector_symbols_expected`.
5. Check writer queue: `collector_writer_queue_depth` — backpressure from disk?

### `collector_writer_errors_total > 0`

1. Check logs for `writer` errors (disk full, fsync, permission denied).
2. Check `df -h` on output mount.
3. Check `collector_writer_queue_depth` — if growing, writer is blocked.
4. If rotation enabled, check segment finalization (`.partial` files left behind).

### `collector_discovery_failures_total > 0`

1. Check `collector_discovery_last_error` from `/readyz` or logs.
2. Common causes: Gamma/Kalshi API timeout, auth expiry, schema change.
3. Verify network connectivity to discovery endpoint.
4. Check `collector_discovery_retries_total` — high retries = rate limited or flaky.

---

## Metric Cardinality

| Metric | Cardinality | Notes |
|--------|-------------|-------|
| Global counters/gauges | O(1) | Fixed set |
| Per-source metrics | O(shards × replicas) | Max 512 by default |
| No per-symbol metrics | — | Avoided for scale |

Total metric count at max config (64 shards × 2 replicas = 128 sources): ~800 time series. Well within Prometheus default limits.

---

## Metric Lifecycle

| Event | Metrics Updated |
|-------|-----------------|
| Exchange snapshot received | `exchange_snapshots_total`, `source_messages` |
| Delta emitted to output | `deltas_emitted_total`, `writer_messages_total`, `writer_bytes_total` |
| Delta rejected | `deltas_rejected_total` |
| Source connect | `sources_connected`, `source_connected{source}`, `subscriptions_sent` |
| Source disconnect | `sources_connected`, `source_connected{source}`, `source_reconnects{source}` (on reconnect) |
| Failover | `failover_snapshots_total`, `source_switches_total` |
| Periodic snapshot | `periodic_snapshots_total`, `writer_messages_total`, `writer_bytes_total` |
| Discovery round | `discovery_rounds_total`, `discovery_pages_total`, `discovery_selected_markets_total`, `discovery_outcome_symbols_total`, `discovery_age_seconds`, `catalog_age_seconds` |
| Discovery retry | `discovery_retries_total` |
| Discovery failure | `discovery_failures_total` |
| Writer rotation | `writer_segments`, `writer_bytes_total` (includes padding) |
| Persistence success | `persistence_last_success_timestamp_seconds`, `persistence_pending` (0) |
| Persistence failure | `persistence_last_error`, `persistence_pending` (0) |
| Shutdown | `final_snapshots_total`, `writer_messages_total` (final flush) |

---

## Verification

Run the collector with `metrics.listen = "127.0.0.1:9090"` and verify:

```bash
# Liveness
curl -s http://127.0.0.1:9090/livez | jq .

# Readiness
curl -s http://127.0.0.1:9090/readyz | jq .

# Prometheus metrics
curl -s http://127.0.0.1:9090/metrics | head -50
```

All endpoints must respond within 100ms under load.
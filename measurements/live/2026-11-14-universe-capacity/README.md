# 2026-11-14 — full-universe live capacity measurement

Raw artifacts from a `LIVE_EVIDENCE_SCOPE=universe` attempt against the live
Polymarket public feed (`scripts/run-live-evidence.sh`, direct egress, no
proxy, read-only market data, order entry not used). Source commit
`756197558e38f307590b6bd44f81510cc0ba69f5`.

## Finding

Live Polymarket discovery returned **387,392 outcome symbols**. The capacity
planner then refused to start, fail-closed, with the precise reason preserved
verbatim in `collector.log`:

```
discovered 387392 symbols: keeping every socket at or below 5000 symbols
needs 78 shards, above max_shards = 64; raise max_shards or the
per-socket bound
```

This is a real capacity measurement, not a synthetic projection: the live
universe is two orders of magnitude beyond the 4,000-symbol CI smoke target,
and the default startup envelope (`max_shards = 64`,
`max_total_connections = 64`, `max_estimated_book_memory_mb = 1024`) cannot
cover it. The planner rejected the topology before opening a single socket —
the fail-closed startup gate working as designed on real data.

Discovery itself (387k symbols, paged from the Gamma API at the default page
size) completed in ~27 minutes on this host; that is the measured discovery
cost for the full universe.

## What this file is not

- No capture was produced: the run never reached the collection phase.
- No performance claim is made; see `deploy/deployment-notes.md` for the
  resource projection and `config.live-universe.toml` for the reviewed
  envelope that makes a full-universe capture feasible.

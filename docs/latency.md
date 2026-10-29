# Latency experiment (Extension 4)

## What is measured

> **Evidence level.** The four 2026-10-24 runs below are single 60-second
> observations on one laptop and one network path. They are not a controlled
> benchmark, a distribution across network conditions, or causal identification.
> In particular, the source-port/CDN-hashing and Nagle/delayed-ACK explanations
> are hypotheses consistent with the samples, not isolated mechanisms.

The `latency` command measures four different things and never invents values:

1. **Discovery RTT**: wall-clock time around one `Exchange::discover_markets`
   call. This includes HTTP connection setup, keyset pagination, and JSON
   decoding.
2. **Exchange-to-local timestamp latency**: for each decoded snapshot, delta, or
   lifecycle event that has an exchange timestamp:

   ```text
   latency_us = local_receive_at_us - exchange_timestamp_us
   ```

   `local_receive_at_us` is captured immediately after the websocket frame is
   read and before decoding. The result includes any exchange/local clock offset
   and whatever timestamp semantics the exchange uses.

3. **Connection setup**: `replica_connect_ms` is the wall-clock time of one
   session's `connect()` — TCP, TLS, and the websocket upgrade, including any
   racing — and `replica_first_frame_ms` is the time from the replica task
   starting (immediately after connect and subscribe) to its first frame.
4. **Cross-connection arrival skew**: with `--replicas N`, every replica
   subscribes to the same symbols on its own connection. Identical events — the
   same symbol, exchange timestamp, exchange clock, and event shape (side,
   price, and quantity for a level change) — are matched across connections, and
   each reported delta is the later arrival minus the earliest arrival of that
   event. This comparison is **clock-independent**: it never subtracts an
   exchange timestamp from a local one, so exchange/local clock offset cannot
   distort it. `replica_first_arrivals` counts which connection saw each matched
   event first.

Negative timestamp-latency values are counted separately and excluded from
percentiles. If no timestamped events arrive, all latency fields are `null`.
Percentiles use nearest-rank selection on the collected samples, with no
interpolation or extrapolation.

With more than one replica, the first two seconds after every subscription is
established are excluded from the measurement window, because the initial
snapshot dump arrives at a different time on each connection and would otherwise
dominate the skew distribution with setup artefacts. Events that only one
replica ever observes are counted in `replica_abandoned_events` rather than
being silently dropped. A one-replica run reports no skew samples at all.

## Commands

All runs below were measured on 2026-10-24 on macOS 27.0.1 (arm64) over the
public internet, using the release build, Polymarket's public feed, five
outcome tokens, two replicas, and a 60-second window:

```bash
cargo run --release -- --exchange polymarket --max-markets 5 --duration-secs 60 \
  --replicas 2 latency --out measurements/polymarket-latency-baseline-2026-10-24.json

cargo run --release -- --exchange polymarket --max-markets 5 --duration-secs 60 \
  --replicas 2 --connect-race 4 latency \
  --out measurements/polymarket-latency-race4-2026-10-24.json

cargo run --release -- --exchange polymarket --max-markets 5 --duration-secs 60 \
  --replicas 2 --source-port 51800 latency \
  --out measurements/polymarket-latency-portpin-2026-10-24.json

cargo run --release -- --exchange polymarket --max-markets 5 --duration-secs 60 \
  --replicas 2 --no-tcp-nodelay latency \
  --out measurements/polymarket-latency-nodelay-off-2026-10-24.json
```

The pinned-port run derives ports 51800 and 51801 for the two connections
(`base + shard * 256 + replica`). The raw outputs are checked in under
`measurements/`. The 2026-10-23 single-connection observation
(`measurements/polymarket-latency-2026-10-23.json`) predates cross-connection
skew and is retained as history.

## Results of those four runs

| Metric | Baseline | `--connect-race 4` | `--source-port 51800` | `--no-tcp-nodelay` |
|---|---:|---:|---:|---:|
| `tcp_nodelay` | on | on | on | off |
| `replica_connect_ms` | 1487 / 1007 | 636 / 759 | 3167 / 5313 | 1164 / 1432 |
| `replica_first_frame_ms` | 2.8 / 434 | 2.9 / 271 | 2.7 / 272 | 2.8 / 617 |
| Measured events | 352 | 440 | 714 | 792 |
| Matched cross-connection events | 176 | 216 | 354 | 396 |
| Abandoned events | 0 | 4 | 6 | 0 |
| Skew p50 | 65 µs | 59 µs | 67,845 µs | 10,543 µs |
| Skew p95 | 27,004 µs | 5,496 µs | 74,093 µs | 15,280 µs |
| Skew max | 43,984 µs | 12,586 µs | 188,859 µs | 429,821 µs |
| First arrivals (replica 0 / 1) | 36 / 140 | 196 / 20 | 8 / 346 | 36 / 360 |
| Timestamp-latency p50 | 229,524 µs | 147,670 µs | 192,387 µs | 179,050 µs |

The four runs happened at different minutes of the same day on the same
network, so network conditions varied between them. Each configuration was
measured once.

## Findings

1. **Two independent connections usually see the same update within tens of
   microseconds.** With `TCP_NODELAY` on and ephemeral ports, the matched-event
   skew p50 was 65 µs (baseline) and 59 µs (racing). The tail is another matter:
   p95 reached 27 ms in the baseline run. Redundant connections therefore
   deliver most events near-simultaneously while the tail is dominated by
   occasional slow paths — exactly the distribution Extension 1's failover
   design assumes.
2. **Racing four full handshakes looked better in these runs.** The
   winning-of-four connect completed in 636/759 ms versus 1487/1007 ms without
   racing, and the skew tail tightened (p95 5.5 ms versus 27 ms). Racing
   includes the TLS and websocket upgrade, so the winner is the first
   *usable* connection, selected after the full handshake. One pair of runs is weak
   evidence, and the comparison is not controlled for network variation. A
   plausible, unproven explanation is that an anycast/CDN edge routes
   otherwise-identical handshakes onto different origin paths, so selecting the
   first success discards the slow ones.
3. **Source-port pinning was the worst configuration in this sample.** Pinning
   ports 51800/51801 produced the slowest connects (3.2 s / 5.3 s), the worst
   skew (p50 67.8 ms), and a lopsided arrival race (replica 1 first on 346 of
   354 matched events). **Hypothesis, not established:** the venue's CDN may
   hash the source port when choosing an origin path, so a pinned pair could
   land on a consistently worse path than two ephemeral ports. These four runs
   do not isolate CDN routing from other time-varying network conditions. The
   supported conclusion is narrower: in this sample, pinning source ports was
   not a free win and can correlate with a worse path. Measure before adopting
   it, and prefer racing or ephemeral ports when a CDN is involved.
4. **Disabling `TCP_NODELAY` correlated with worse cross-connection arrival.**
   The no-NODELAY run's skew p50 was 10.5 ms versus 65 µs for the NODELAY-on
   baseline, and its worst matched event arrived 430 ms late. **Hypothesis, not
   established:** this is consistent with a Nagle/delayed-ACK interaction, in
   which small order-book frames wait for coalescing, but the runs did not
   capture packet-level evidence or control for network variation.
   `TCP_NODELAY` remains the default because it avoids one plausible source of
   delay at negligible cost.
5. **The venue's initial dump treats the second identical subscription
   differently.** Replica 0's first frame consistently arrived ~3 ms after its
   task started, while replica 1 — subscribed to exactly the same assets a
   fraction of a second later — waited 271–617 ms for its first frame on every
   run. Whatever the cause (server-side subscription batching or deduplication
   of identical subscriptions), it means the first *measured* frames are not
   representative of steady state, which is why the two-second settle window
   exists.
6. **The venue edge intermittently rejects handshakes with HTTP 525.** Two of
   the eight first-attempt latency connects in this session were rejected with
   Cloudflare's "SSL handshake failed" status before any subscription was sent.
   The live collector already retries with backoff; the `latency` command now
   applies the same bounded three-attempt retry, and never retries a fatal
   (authentication) rejection. A retried handshake cannot bias any reported
   value because the measurement window starts only after every session is
   connected.
7. **Timestamp latency is dominated by clock offset and venue semantics.**
   The `local_receive_us - exchange_timestamp_us` p50 across runs was
   148–230 ms with a p99 up to 511 ms, so it is not a network latency
   measurement; the cross-connection skew is the number to read.

## Interpretation and caveats

- Polymarket's `book` snapshots can carry a timestamp older than the local
  receive time, especially for an initial dump. Those samples inflate the tail.
- Polymarket's `timestamp` is an exchange event timestamp, not a guaranteed
  send-time clock. A nonzero clock offset between the exchange and this host is
  included in every sample.
- Discovery RTT (0.7–5.0 s across these runs, and ~9m52s for the full
  2,746-page universe sweep) includes Gamma pagination and JSON decoding; it is
  not a websocket handshake measurement.
- No Kalshi latency result is recorded here because live Kalshi order-book
  subscriptions require credentials that were not supplied. The same command
  works once `KALSHI_API_KEY_ID` and a private key are configured, and it should
  be run against demo first.

## Reproducing

The command writes the JSON report to `--out` and prints the same JSON to
stdout. Run each configuration several times if you need a distribution across
network conditions; do not average separate runs unless the aggregate carries
a label.

# Design decisions

This record explains the choices a reviewer is most likely to question. Each
entry states the context, the decision, the alternatives considered, the
consequences, and the implementation and verification references. The
requirement-to-test matrix remains the authoritative evidence index.

## 1. Exchange abstraction and explicit capabilities

**Context.** Kalshi and Polymarket expose different wire protocols and sequence
semantics. The collector core previously identified a venue by name to decide
whether a sequence clock was available.

**Decision.** Venue behavior sits behind the `Exchange` trait. The adapter
reports `ExchangeCapabilities`, currently `has_sequence_clock`; the core
consumes that value and never matches on a venue name. The coordinator obtains
capabilities from the constructed adapter, and the replay composition root uses
the same `capabilities_for` mapping. Both adapters currently report `false`:
Kalshi's per-subscription `seq` cannot compare replicas, and its wire `clock`
is optional and not treated as cross-replica comparable until verified. They
therefore use the clockless recovery fence; synthetic clocked tests exercise
the capable path directly.

**Alternatives.** Keep `ExchangeKind` checks inside the core; build a separate
state machine per venue; assume every venue has a sequence clock.

**Consequences.** Adding a venue requires an adapter and a capability
declaration, not a change to the state machine. Capability semantics are
explicit and testable, but the capability set must grow deliberately when a new
core decision depends on a venue feature.

**Implementation.** `src/exchange/mod.rs`, `src/exchange/kalshi/mod.rs`,
`src/exchange/polymarket/mod.rs`, `src/collector/state.rs` (`CoreConfig`).

**Verification.** `src/exchange/mod.rs::configured_venue_capabilities_are_explicit`,
`src/collector/state/tests.rs::capabilities_are_not_derived_from_the_venue_name`,
`src/collector/state/tests.rs::kalshi_without_clock_requires_clockless_recovery`.

## 2. Canonical market model

**Context.** Discovery, codecs, replay, and output must agree on market
identity, source identity, event shape, and output shape. Venue payloads differ
in field names, numeric encodings, and identifier policy.

**Decision.** Normalize at the adapter boundary into `MarketInfo`,
`ExchangeEvent`, `EventMeta`, and `OutputMessage` in `src/model.rs`. Discovery
and codec modules own venue-specific parsing and eligibility. The core and
writer consume only the normalized model.

**Alternatives.** Pass raw JSON through the core; keep per-venue core types;
normalize only at the writer.

**Consequences.** The correctness-critical core is deterministic and reusable by
live, replay, and mock paths. Normalization can discard venue detail, so the
adapters must preserve fields that affect eligibility, identity, or timing, and
their tests must check the mapping explicitly.

**Implementation.** `src/model.rs`, `src/exchange/*/discovery.rs`,
`src/exchange/*/codec.rs`, `src/replay.rs`.

**Verification.** `src/exchange/kalshi/codec.rs::maps_yes_and_no_snapshot_levels`,
`src/exchange/polymarket/codec.rs::decodes_book_and_timestamp`,
`src/exchange/polymarket/discovery.rs::expands_outcome_tokens`,
`tests/replay_fixture.rs::sample_fixture_replays_through_the_same_core_and_decodes`.

## 3. Fixed-point representation

**Context.** The supplied SBE schema represents dollars and contract counts as
signed 64-bit integers scaled by 1,000,000. Exchange APIs publish decimal
strings with venue-specific precision, and the data path must not introduce
binary floating-point rounding.

**Decision.** Parse decimal strings exactly into micros in `src/fixed.rs`, reject
more than six fractional digits instead of silently rounding, and keep all
price and quantity arithmetic integral. The SBE bridge converts the normalized
integer values directly.

**Alternatives.** Use `f64`; add a general-purpose decimal crate; parse inside
each adapter.

**Consequences.** Comparisons, aggregation, and serialization are exact and
reproducible. Malformed or over-precise input is rejected at the boundary.
Every conversion must go through the fixed-point helpers rather than ad hoc
parsing.

**Implementation.** `src/fixed.rs`, `src/orderbook.rs`,
`src/util/dtos.rs`, `src/util/orderbook_schema.rs`.

**Verification.** `src/fixed.rs::parses_exact_decimals`,
`src/fixed.rs::rejects_inexact_or_malformed_values`,
`src/orderbook.rs::applies_absolute_and_delta_updates`,
`src/orderbook.rs::rejects_negative_results`.

## 4. Collector state ownership

**Context.** Book maintenance, source selection, failover, snapshot scheduling,
and discovery reconciliation are correctness-critical. Websocket I/O, retries,
and heartbeats are asynchronous and venue-specific.

**Decision.** One synchronous `CollectorCore` owns the books and decision state.
Session workers decode frames and deliver normalized events; the coordinator
owns worker and output lifecycles. The core performs no I/O and no sleeping.

**Alternatives.** A task per symbol mutating shared books; an actor per symbol;
a lock-protected global book; putting failure handling in the session layer.

**Consequences.** Decisions are serialized, invariants are local to one state
machine, and the same core runs under live, replay, and mock timestamps. The
core must remain efficient at the measured 100,000-symbol envelope, and any
blocking work must stay outside it.

**Implementation.** `src/collector/state.rs`, `src/session.rs`,
`src/coordinator.rs`, `src/replay.rs`.

**Verification.** `src/collector/state/tests.rs::snapshot_anchors_delta_application_in_per_symbol_order`,
`src/collector/state/tests.rs::cross_symbol_output_order_is_processing_order_and_not_globally_sorted`,
`tests/replay_fixture.rs::sample_fixture_replays_through_the_same_core_and_decodes`.

## 5. Redundant source architecture

**Context.** Public websocket sources can drop connections, stop delivering, or
fall behind. A single connection per shard would make continuity depend on one
venue path.

**Decision.** Assign symbols to fixed shards at startup and open `replicas`
connections per shard. Every replica maintains a shadow book; one source is
active per symbol. A backup is promoted only when its book is usable and, for
sequenced venues, not older than the last emitted clock. Promotion is followed
by an anchoring full snapshot. The per-socket symbol bound is enforced before
workers start.

**Alternatives.** One connection per shard; reconnect-only continuity;
active-active merging of sources; promote any backup without a freshness check.

**Consequences.** Redundancy costs connections and shadow memory. Failover can
emit an extra snapshot, which is intentional. Clockless venues need a stricter
recovery fence because they cannot prove shadow freshness from a sequence
number. The tested envelope is documented in `operating-envelope.md`; it is not
a claim of full-universe simultaneous subscription.

**Implementation.** `src/capacity.rs`, `src/collector/state.rs`,
`src/session.rs`.

**Verification.** `src/collector/state/tests.rs::clocked_shadow_replica_takes_over_after_source_loss`,
`src/collector/state/tests.rs::clocked_failover_anchors_resumed_deltas_and_preserves_receive_times`,
`src/collector/state/tests.rs::clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot`,
`tests/e2e_mock_exchange/failover.rs::single_socket_loss_fails_over_and_emits_anchoring_snapshot`.

## 6. SBE compatibility

**Context.** The challenge supplies a FIX SBE schema and template-compatible
aligned reader/writer helpers. A submission that changes the schema or framing
would no longer be verifiable with the supplied tooling.

**Decision.** Keep `sbe_schemas/` unchanged, generate DTOs through the
configured build script, and write through the aligned writer with explicit
chunk sentinels and padding. The SBE capture remains the primary output; the
raw-frame journal is a separate artifact and does not alter it.

**Alternatives.** Use a custom binary format; emit JSON; extend the supplied
schema for journal or metadata fields.

**Consequences.** Captures remain readable by the pristine template reader and
by the offline `inspect`/`verify` commands. Chunk size and padding rules are
part of the output contract. New observability data must live in a sidecar or a
separate directory rather than changing the schema.

**Implementation.** `sbe_schemas/`, `src/util/build.rs`,
`src/util/aligned_sbe_writer.rs`, `src/util/aligned_sbe_reader.rs`,
`src/writer.rs`.

**Verification.** `src/util/aligned_sbe_writer.rs::aligned_writer_pads_only_on_finish`,
`src/util/aligned_sbe_reader.rs::rejects_non_zero_bytes_after_a_chunk_sentinel`,
`src/writer.rs::default_chunk_output_is_readable_by_default_reader`,
`tests/replay_fixture.rs::sample_fixture_replays_through_the_same_core_and_decodes`.

## 7. Raw-frame journaling

**Context.** Normalized SBE output cannot reconstruct the exact public frames,
including frames the decoder rejected. Debugging a venue contract change or a
decode decision requires the original bytes.

**Decision.** Add an optional append-only journal that captures the exact
payload at the receive boundary before normalization. Records carry per-source
epoch and receive sequence, checksums, and receive time. Segments receive a
header, footer, and manifest entry; verification recomputes every checksum and
compares footer and manifest counts. The queue is bounded, and a write, flush,
sync, rename, or saturation failure latches the run as failed.

**Alternatives.** Text logging; store raw bytes inside the SBE stream; rely on
venue history; use an unbounded queue.

**Consequences.** The journal adds disk I/O and can fail a run, which is the
intended durability contract. A checksum-valid journal proves the integrity of
recorded payloads, not that the venue delivered every event. Retention and
disk-watermark policies are not implemented.

**Implementation.** `src/journal.rs`, `src/session.rs`, `src/coordinator.rs`,
`src/cli.rs` (`journal verify`, `journal recover`).

**Verification.** `src/journal/tests.rs::payload_bytes_survive_write_and_read`,
`src/journal/tests.rs::checksum_mismatch_and_corrupt_manifest_fail_verification`,
`src/journal/tests.rs::queue_saturation_then_shutdown_stays_failed`,
`tests/e2e_mock_exchange.rs::journal_accounts_for_every_delivered_payload`,
`tests/e2e_mock_exchange.rs::journal_finalizes_and_verifies_on_graceful_shutdown`.

## 8. Recovery and continuity semantics

**Context.** A recovery snapshot that is applied to a stale book can hide a gap
and produce a plausible but wrong history. Sequenced and clockless venues need
different evidence before recovery is safe.

**Decision.** A sequence gap triggers a reset or resubscribe. For sequenced
venues, a shadow is promoted only when it is at least as new as the last emitted
clock. For clockless venues, recovery requires an acknowledged fence and a
snapshot received strictly after that acknowledgement; a recovery snapshot
before the acknowledgement is rejected without mutating the active book.
Promotion and reset are followed by an anchoring snapshot. An empty snapshot is
a retirement boundary. Cross-symbol output is processing order, not global
timestamp order.

**Alternatives.** Best-effort application of any snapshot; timestamp-only
ordering; sort the output globally; drop a gap without reporting it.

**Consequences.** Gaps are reported rather than hidden, and `verify` can prove
that a later snapshot disagrees with reconstructed deltas. `verify` is not a
completeness oracle by itself. Consumers that need global time order must
buffer downstream and preserve file order.

**Implementation.** `src/collector/state.rs`, `src/verify.rs`, `src/cli.rs`.

**Verification.** `src/collector/state/tests.rs::recovery_snapshot_before_ack_is_rejected_without_mutation`,
`src/collector/state/tests.rs::clockless_full_recovery_requires_acknowledged_fence_and_fresh_snapshot`,
`tests/e2e_mock_exchange/failover.rs::full_outage_recovery_exposes_the_uncovered_interval`,
`tests/e2e_mock_exchange.rs::negative_control_missing_update_is_detected_by_verify`,
`src/verify.rs::reports_a_snapshot_that_disagrees_with_reconstructed_deltas`.

## 9. Instrument and venue reference model

**Status.** Accepted 2026-11-07. The full decision, entity definitions,
cardinalities, temporal rules, and worked scenarios are in
[ADR 0001](adr/0001-instrument-and-venue-reference-model.md).

**Context.** `MarketInfo` combines instrument, venue, listing, mapping, output
symbol, and prediction-market outcome metadata. Kalshi stores a parent
`event_ticker` in `exchange_market_id`, and `count_selected_markets`
deduplicates by that field, so several markets under one event can be counted as
one. Polymarket repeats a parent condition ID for every outcome token. The
current runtime `SourceId { shard, replica }` identifies a worker, not an
external data source.

**Decision.** An instrument has a persistent platform identity independent of
its ticker, listing, venue, feed, connection, or output symbol. Instrument,
instrument identifier, venue, venue operator, optional market segment, listing,
venue-assigned mapping, provider/feed-assigned mapping, data provider, feed,
feed session, and entitlement are separate concepts. Identifiers are namespaced,
scoped, and effective-dated. Runtime worker identity is not external source
identity. Events and conditions are groupings, not instruments.

**Alternatives.** Keep `MarketInfo` as universal identity; key by ticker; use
ISIN as a universal key; model every source as a public exchange; extend
`ExchangeKind` into a venue registry.

**Consequences.** Cross-venue and OTC/dark-pool identity becomes representable
without rewriting venue behavior into the core. Migration is incremental and
must preserve existing SBE output. The Kalshi selected-market undercount remains
until a later implementation PR. This decision adds no code and no dependency.

**Implementation.** None in this decision. The migration sequence and
non-goals are in
[ADR 0001](adr/0001-instrument-and-venue-reference-model.md).

**Verification.** Documentation review against the ADR acceptance criteria;
the requirement matrix and architecture inventory are unchanged.

## 10. Kalshi subscription acknowledgement folding

**Context.** Kalshi keeps one `orderbook_delta` subscription per websocket
connection. A repeated `subscribe` on the same connection adds its markets to
the existing `sid`; the venue acknowledges it with `ok`, which carries the
connection's `sid`, the subscription `seq`, and, for current responses, the
full `msg.market_tickers` list, rather than issuing a fresh `subscribed`.
Chunked initial loads and later top-ups therefore complete against one sid,
not one sid per command. The earlier adapter expected a `subscribed` per
command: every chunk after the first stayed in the pending map until the
acknowledgement deadline, the tracked market set covered only the first
chunk, and the session cycled until a seq failure forced a reconnect.

**Decision.** `KalshiMessage::Ok` resolves the pending `subscribe` it
acknowledges. The markets fold into the subscription the venue names,
creating it when the venue has not acknowledged it yet; the venue's
`msg.market_tickers`, when present, replaces the tracked set; an `ok` without
a sid folds into the single tracked subscription; an `ok` that can be
unambiguously assigned to nothing fails the connection so the next reconnect
resubscribes cleanly instead of running on divergent local state. A
`subscribed` that names an existing sid is folded the same way rather than
rejected. `ok` sequence numbers still feed the sequence tracker so a
following delta is not misread as a gap.

**Alternatives.** Issue one `subscribe` per connection and drop the chunking;
send `unsubscribe` before resubscribing on every terminal error; treat a
missing acknowledgement as fatal without resolving it.

**Consequences.** The tracked subscription matches the venue's actual sid
state, the pending map drains, and newly added markets reconcile against the
authoritative market list. A loss of a market from the venue's list now
re-subscribes exactly that market on the next reconcile. Terminal subscription
errors still rely on seq-gap detection and reconnect for a full resync; a
venue that answers a repeated `subscribe` with a fresh sid is still handled,
as is a venue that puts `sid` only on `subscribed`.

**Implementation.** `src/exchange/kalshi/codec.rs` (`Ok::full_tickers`),
`src/exchange/kalshi/session.rs` (`merge_subscription`, the `Ok` and
`Subscribed` arms).

**Verification.** `src/exchange/kalshi/session.rs::repeated_subscribe_acked_with_ok_merges_into_the_connection_sid`,
`src/exchange/kalshi/session.rs::merged_ok_without_sid_uses_the_single_orderbook_subscription`,
`src/exchange/kalshi/session.rs::merged_ok_market_list_replaces_the_tracked_set_authoritatively`,
`src/exchange/kalshi/session.rs::subscribed_with_an_existing_sid_merges_instead_of_failing`.

## 11. Egress policy: direct by default, explicit proxy, fail closed

**Context.** A collector may legitimately need a controlled egress path —
corporate network policy, a proxy appliance, a venue that restricts where its
feed is consumed — and silently routing market data through an unverified
route is unacceptable for an evidence-first system. The collector previously
dialed the venue directly with no declared route, and REST discovery and
websocket sessions had no shared transport policy.

**Decision.** `src/egress.rs` owns the route. `direct` is the default and is
unchanged: direct sockets never inherit unrelated shell proxy variables. A
single explicitly configured proxy (`http://` CONNECT or SOCKS5/SOCKS5H) is
resolved from environment variables named by `[network]` at settings load
time, never from the TOML document. Fail closed: a failed proxy never falls
back to direct networking, at any stage. SOCKS5 DNS resolution mode is a typed
choice (`proxy_remote_dns`, forced on for `socks5h://`). HTTP CONNECT
requires a successful 2xx and rejects trailing bytes on a 2xx response; SOCKS5
passes the target as a domain name (remote DNS) or a locally resolved IP
literal (local DNS). Venue TLS is always end-to-end and always verifies the
venue hostname; proxy authentication is a separate credential domain read
from the environment, redacted in every log, manifest, fingerprint and error
path, and covered by a regression test. REST clients are built from the same
policy (`no_proxy` blocks ambient variables in both modes; exactly one proxy
is attached in explicit mode). The non-secret route identity is persisted in
the journal manifest (mode, kind, host, port, DNS mode, label, SHA-256
fingerprint) as provenance; credentials are not part of the fingerprint.

`network-probe` (`src/probe.rs`) measures the path stage by stage — DNS,
TCP, proxy negotiation, TLS, websocket upgrade — and emits a JSON report with
a stable schema and deterministic failure classes; it never authenticates to a
venue. A stage is never inferred from another stage's success.

**Alternatives.** Per-process shell proxy variables (`HTTP_PROXY`/`ALL_PROXY`
inherited silently — violates the no-surprise-egress property); one proxy
setting shared by everything with no fail-closed semantics (would leak
partial failures into misleading "connected" states); TLS to the proxy itself
(not required for the supported transports; rejected until there is a
reviewed, non-interceptable use case); location-disguising egress of any
kind (rejected outright; a proxy is a network path, not an entitlement, and
must never bypass a venue's eligibility rules).

**Consequences.** Every capture records how its bytes reached the venue
(direct or explicit-proxy provenance) with a hashable identity. REST and
websocket paths can no longer diverge onto different routes than the one
configured. A failed proxy surfaces as a classified, attributable error
before any market data is observed. The proxy path is verified against
loopback mocks only; live-venue use remains permission-gated by venue terms
and account eligibility. `network-probe` proves transport, not feed
health: subscription acknowledgement and sustained continuity remain the
collector's concern and are verified by the run-time journal and verifier.

**Implementation.** `src/egress.rs`, `src/probe.rs`, `src/ws.rs` (`connect`,
`ProbeTrace`/`ProbeStage`, `ConnectOptions::trace`), `src/config.rs`
(`[network] egress_mode`, `proxy_url_env`, `proxy_auth_user_env`,
`proxy_auth_password_env`, `proxy_remote_dns`, `proxy_label`, plus
load-time validation), `src/exchange/*/mod.rs` and
`src/exchange/*/session.rs` (route resolution at startup),
`src/journal.rs` (manifest `route` provenance, additive and
backwards-compatible).

**Verification.** `tests/egress_lab.rs` (19 lab tests: direct baseline,
CONNECT and SOCKS5 tunnels, remote/local DNS, wrong credentials, 2xx with
trailing bytes, fail-closed rejection with no venue dial, manifest
provenance without credentials, `network-probe` direct/proxy/config
failure), `src/egress.rs` unit tests (env-lookup injection; no process-global
state is touched by the tests).

## 12. Dedicated Kalshi lifecycle control plane

**Context.** Every Kalshi order-book websocket sent its own
`market_lifecycle_v2` subscription, so a 74-shard, two-replica run opened
roughly 148 lifecycle subscriptions and applied each notification up to 148
times. A control-channel error could also fail the book data path, and no
component owned the lifecycle stream's reconnect semantics.

**Decision.** Book shards carry only `orderbook_delta`. A dedicated,
supervised lifecycle plane (`collector.lifecycle_replicas`, Kalshi-only,
default 1, maximum 4) owns `market_lifecycle_v2`. The core deduplicates
notifications across replicas by `(market, kind, exchange timestamp)` with a
bounded window; unknown Create/Activate notifications force an out-of-cycle
REST reconciliation (debounced and shared with the periodic ticker) instead
of silently diverging from the authoritative catalog; lifecycle reconnects
trigger a REST reconciliation so notifications missed while disconnected are
recovered; terminal notifications retire a tracked market exactly once, and
terminal notifications for untracked markets are counted, not dropped.
Lifecycle sockets are charged to the capacity plan's connection budget
(`max_total_connections`), and carry no order-book subscriptions into the
subscription universe.

**Alternatives.** Per-socket lifecycle subscriptions (status quo — fan-out
and misattribution); one lifecycle socket with no redundancy; subscribing
book replicas anyway and filtering client-side; or treating periodic REST
as the only lifecycle signal (up to a full reconciliation interval of
staleness for terminal events).

**Consequences.** One (or a few) supervised control sockets replace every
book shard subscribing. Duplicate deliveries across lifecycle replicas are
applied once. The subscription universe can still temporarily diverge from
the truth when the venue creates a market mid-interval, but the divergence
is surfaced (counter and a forced reconciliation), never silent. Lifecycle
sockets consume the shared connection ceiling; operators budget for them the
same as book sockets. Polymarket is unaffected: it publishes lifecycle
notifications on the market channel, not a separate one.

**Implementation.** `src/exchange/kalshi/session.rs` (`SessionMode`,
`LifecycleSessionFactory`), `src/exchange/kalshi/mod.rs` (optional
`Exchange::lifecycle_session_factory`), `src/exchange/mod.rs`,
`src/coordinator.rs` (lifecycle spawn, `DiscoveryPlane`, drain and fatal-check
semantics, `LIFECYCLE_SHARD`), `src/collector/state.rs` (dedup window,
reconcile request flag, out-of-book-plane status handling),
`src/metrics.rs` (`lifecycle_duplicates`, `lifecycle_unknown_symbols`),
`src/config.rs` (`collector.lifecycle_replicas`), `src/capacity.rs`.

**Verification.** `tests/kalshi_lifecycle_plane.rs` (book sockets never
subscribe; exactly the configured lifecycle connection count; capacity and
report accounting; unknown Create forces an out-of-cycle reconciliation;
terminal `settled` retires the market exactly once; lifecycle reconnect
consults REST), `src/exchange/kalshi/session.rs` (`SessionMode` book and
lifecycle command discipline), `src/collector/state/tests.rs` (dedup,
reconciliation request, untracked terminal accounting, out-of-book-plane
status), `src/capacity.rs` (Kalshi connection budget including the plane),
`tests/session_recovery.rs`, `scripts/check_architecture.py`.

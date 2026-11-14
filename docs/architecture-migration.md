# Market-data architecture migration

**Status:** Proposed  
**Base:** `c0eaea24718930510eb3341a5558c0b8f9bd3c60` (`feat/full-universe-stress-lab`)  
**Branch:** `architecture/market-data-contracts`  
**Scope:** Architecture contracts and migration sequence. No production code or venue adapter changes are authorized by this document.

## Objective

Build a venue-neutral market-data and research platform whose data, event, recovery, persistence, and experiment claims can be reproduced from retained evidence.

Initial venue workstreams have equal engineering standing and independent certification:

- Kalshi: event-contract markets.
- Polymarket: outcome-token markets.
- Hyperliquid: perpetual and spot instruments.

A venue's access or certification status must not block another venue's development or evidence.

## Current baseline and explicit non-claims

The repository is a read-only market-data collector. It does not submit orders, hold positions, or implement strategy or pre-trade risk.

The accepted ADR 0001 defines a persistent instrument identity independent of ticker, listing, venue, feed, connection, and serialization symbol. Its scope explicitly says documentation/design only; the Rust model has not implemented the migration.

Current code still has:
- `MarketInfo` combining venue, native symbol, output symbol, grouping and instrument metadata.
- Collector books keyed by venue-native symbol.
- `MarketDataSession::apply_subscriptions(&BTreeSet<String>)`, which represents symbols rather than data products.
- `ExchangeCapabilities::has_sequence_clock`, which does not describe update model, sequence scope, recovery, subscription topology, limits, or event products.
- `ExchangeEvent` variants for snapshots, level changes, lifecycle and stream reset, but no normalized Trade, BBO, or asset-context event.

PR #42's synthetic/loopback evidence is not live-venue evidence, capacity evidence, or a production readiness certificate. The checked-in 4,000-symbol fixture is a correctness smoke, not proof of 548,942-token live collection or a 24-hour soak.

The verification record documents a Polymarket discovery run of 548,942 unique tokens across 2,746 pages in 9m52s, a 10-token/90-second live capture, and a 100,000-symbol synthetic scale run with 696,448 KB RSS. These are separate evidence classes and must remain separate. They do not prove full-universe live collection.

## Five engineering principles

1. **Correctness:** each state transition has explicit preconditions, postconditions, invariants and failure outcomes.
2. **Reproducibility:** each experiment can be reconstructed from immutable configuration, code identity, data fingerprints, seeds, toolchain and retained inputs.
3. **Determinism:** identical normalized inputs and configuration produce identical outputs, modulo explicitly excluded environmental metadata.
4. **Observability:** a failure is diagnosable from retained evidence; unobservable stages fail closed rather than being assigned zero.
5. **Risk containment:** research cannot submit orders; execution cannot bypass independent risk admission and kill controls.

Every PR must state the problem, design choice, invariants, failure modes, tests, reproducible commands, evidence class, limitations and migration impact.

## Architecture boundaries

```
Venue adapters (Kalshi | Polymarket | Hyperliquid)
                |
      Discovery + reference data
                |
       Instrument identity
                |
 Typed data-product subscriptions
                |
 Raw-frame journal (source evidence)
                |
 Versioned normalized market events
        /       |       |       \
      Books   Trades   BBO   Asset context
        \       |       |       /
          Replay + normalized store
                    |
             Research datasets
                    |
             Hypothesis registry
                    |
            Feature/experiment lab
                    |
            Statistical validation
                    |
             Strategy registry
                    |
              Event backtest
                    |
           Approved strategy artifact
                    |
         Independent risk engine
                    |
         Broker-neutral OMS
                    |
             Venue adapters
```

Keep one venue collector per process for now. Normalize at durable boundaries; do not force all venue protocols into one state machine. Keep legacy challenge SBE as a projection, not as the canonical representation of every market event.

Research and execution must have separate processes, permissions, credentials, and APIs. Research artifacts cannot contain live-order capabilities. Execution accepts only signed/versioned strategy artifacts that pass policy and independent risk admission.

## Contract A — instrument and reference identity

Implement only the identity boundaries required for live market data first; do not attempt the entire reference-data ontology in one PR.

Required concepts:
- `InstrumentId`: platform-assigned persistent identity.
- `VenueId` and `VenueOperatorId`: distinct identities.
- `ListingId`: time-bounded admission of an instrument to a venue.
- `FeedId` and `DataProviderId`: source identities.
- `VenueInstrumentMapping`: venue-native identifier scoped by venue/feed/protocol and effective time.
- `FeedInstrumentMapping`: provider/feed identifier that need not imply a public listing.
- Parent relationships such as Kalshi event, Polymarket condition, and Hyperliquid DEX are metadata/relationships, never substitutes for instrument identity.
- `SerializationSymbol`: compatibility projection only.

Identity must not be inferred from display ticker. Identifier resolution must be namespaced, scoped, effective-dated and provenance-bearing. Ambiguous resolution is an explicit unresolved state.

Acceptance tests:
- Same ticker on two venues produces distinct mappings.
- Symbol reuse across time preserves two effective intervals.
- Kalshi event grouping does not collapse distinct market instruments.
- Polymarket condition does not collapse distinct outcome tokens.
- Hyperliquid spot pair, perpetual, DEX-scoped instrument and builder-deployed instrument remain distinct.
- Serialization symbol changes do not mutate canonical instrument identity.
- Unknown/ambiguous mappings fail closed and preserve raw evidence.

## Contract B — typed data-product subscriptions

Replace symbol-only subscription semantics with a typed set of data products. Initial conceptual variants:

- OrderBookL2(InstrumentId)
- Trades(InstrumentId)
- BestBidOffer(InstrumentId)
- AssetContext(InstrumentId)
- AllMids(DexId or venue scope)
- AllAssetContexts(venue scope)
- VenueLifecycle(scope)

A subscription has a stable key and explicit lifecycle:
`Desired -> Sent -> Acknowledged -> Active -> Removing -> Removed`, with `Failed` and `UnknownAfterDisconnect` outcomes.

Acknowledgement means the venue/session confirmed the request according to that protocol. Local write success is not venue acknowledgement. Reconnect creates a new connection epoch; prior active state cannot silently carry over.

Capability metadata must describe:
- Product types and update model (snapshot, delta, partial update).
- Sequence or ordering domain and reset behavior.
- Recovery procedure and whether an authoritative snapshot exists.
- Per-connection, per-IP and shared-egress budgets.
- Subscription topology and rate limits.
- Precision and schema version.
- Market status/lifecycle coverage.

Do not represent a venue-wide/global feed as one subscription per instrument. Hyperliquid's documented per-IP limits must be enforced by a shared egress budget, not per-process estimates.

## Contract C — normalized event semantics

Keep event types semantically distinct:
- BookSnapshot
- BookDelta
- Trade
- BestBidOffer
- AssetContext
- InstrumentStatus/Lifecycle
- StreamReset/Gap

Every event carries:
- Canonical InstrumentId where resolvable.
- Venue, provider, feed and source identity.
- Connection epoch and sequence domain/value where available.
- Exchange timestamp with source precision and semantics.
- Local receive timestamp from a monotonic clock plus wall-clock timestamp where needed.
- Raw-frame reference and decoder/schema version.
- Event-specific exact numeric fields.

Trade is not a book delta. BBO is not an L2 book. allMids is not an executable quote or L2 snapshot. An L2 full snapshot must enter as a snapshot; never disguise it as a delta. Partial asset-context updates must merge by instrument; they must not replace the whole state map.

Sequence numbers are meaningful only within their documented scope. Trade IDs must use the venue-defined composite identity rather than assuming one field is globally unique.

## Contract D — numeric and persistence semantics

- Preserve raw venue frames before normalization.
- Persist versioned normalized events with raw-frame references.
- Retain exact decimal coefficients and scales; never round during adapter normalization.
- Reject an inexact projection into legacy SBE.
- Keep SBE challenge output as a versioned compatibility projection.
- A failed projection must not discard the raw frame or normalized event.
- Journal offsets, hashes, source identity and event identity must support deterministic replay.
- Schema evolution must be explicit and backward-compatible or version-bumped.

The journal is evidence of what the collector received, not proof that the venue delivered every event. Venue completeness needs independent universe/reconciliation and feed-specific continuity evidence.

## Contract E — replay and experiment identity

An experiment manifest must identify:
- Immutable configuration bytes and SHA-256.
- Source commit and dirty-tree state.
- Binary hashes and build profile/toolchain.
- Dataset version, partition list and cryptographic fingerprints.
- Venue universe snapshot and effective-time policy.
- Random seeds and RNG algorithm version.
- Feature/strategy artifact hashes.
- Cost model, execution model and parameter search space.
- Train/validation/test time boundaries and embargo policy.
- All attempted hypotheses/configurations, including failures.
- Output artifacts and verification results.

Identical inputs/configuration must yield deterministic event ordering and result hashes where environmental measurements are excluded explicitly.

Manifest publication must be transactional: unique temp files, flush/sync where required, atomic rename, directory durability where supported, and concurrency protection for shared indexes. Index updates must not lose concurrent cells. A failure between manifest and index publication must leave a recoverable state that cannot be misread as a complete success.

## Contract F — evidence classes

Every result must declare exactly one evidence class:

1. `unit-fixture`: deterministic decoder/state tests.
2. `synthetic-loopback`: production code path against a synthetic venue.
3. `historical-replay`: recorded historical inputs with provenance.
4. `live-smoke`: selected live instruments and bounded duration.
5. `live-universe`: complete discovered eligible set anchored for a declared product.
6. `live-soak`: continuous operation for a declared duration with loss/recovery accounting.

Evidence classes are not interchangeable. A green synthetic test cannot imply a live result. A live-universe result is product-specific: Hyperliquid allMids coverage does not imply full L2 coverage.

Each venue and data product has its own certification manifest. One venue must never block another venue's evidence.

## Contract G — failure and recovery

All data products define:
- Connection/session state.
- Subscription state.
- Sequence/order domain.
- Duplicate policy.
- Gap detection capability and limits.
- Snapshot authority and re-anchoring rules.
- Queue/backpressure policy.
- Journal failure policy.
- Shutdown and restart behavior.

Known gaps are measured and classified. Unknown gaps are marked unknown; never fabricate exact loss counts. A recovered book is not considered continuous until its protocol-defined recovery fence and authoritative snapshot have passed.

## Contract H — statistical and economic validation

The future research lab must require:
- Hypothesis and falsification criterion before parameter search.
- Point-in-time universe membership and delisted/expired instruments.
- Corporate actions where applicable.
- No look-ahead or survivorship leakage.
- Chronological train/validation/test with purging/embargo where leakage is possible.
- Multiple-testing accounting and experiment registry.
- Selection-bias diagnostics, including Probability of Backtest Overfitting and Deflated Sharpe Ratio where assumptions/data support them.
- Walk-forward and out-of-sample evaluation.
- Brokerage, exchange fees, taxes, spread, slippage, market impact, funding/borrow and liquidity constraints appropriate to venue/instrument.
- Capacity and participation limits.
- Cost sensitivity and adverse scenarios.
- Rejection as a first-class outcome.

A backtest cannot be accepted without data provenance, execution assumptions, cost schedule and complete experiment history.

## Migration plan

### M0 — Baseline and evidence freeze
- Preserve PR #42 draft and its existing CI evidence.
- Label current stress manifests as synthetic/loopback and identify their source commit.
- Do not launch the full matrix until the manifest, ledger and runner contracts are audited.
- Record exact baseline commit and tests on a clean worktree.

Exit: baseline reproducible; evidence classes explicit; no claim of live full-universe coverage.

### M1 — Correctness contracts and PR #42 disposition
- Review the stress runner ledger against generated -> mock enqueue -> socket write -> collector receive -> decode -> applied -> writer accepted -> SBE persisted -> journaled stages.
- Use typed metric parsing: missing required metrics are unavailable, never zero.
- Verify all required stage counts and discrepancy categories before success.
- Make manifest/index publication concurrency-safe and recoverable.
- Bind manifests to the exact source commit and binary that generated them.
- Validate config and output paths before starting sockets.
- Add multi-process concurrency/crash-injection tests.

Exit: every success manifest is mechanically derivable from independent stage evidence; no race loses cells; artifacts cannot claim success with incomplete provenance.

### M2 — Instrument identity minimum viable migration
- Add newtypes and reference records alongside old structures.
- Add explicit mapping from current adapter `MarketInfo` into the new catalog.
- Migrate collector maps to InstrumentId plus source/feed scoping.
- Preserve output symbol compatibility through a projection.
- Retain old APIs only as a bounded migration seam; do not spread dual identity further.

Exit: Kalshi and Polymarket fixtures prove identity and grouping semantics; existing replay/SBE fixtures remain stable.

### M3 — Subscription and feed capability contracts
- Add typed data-product subscriptions and lifecycle.
- Add feed capability declarations for snapshot/delta, ordering, recovery, topology and limits.
- Keep one process per venue.
- Add fake-session tests for reconnect and uncertain subscription state.

Exit: no symbol-only subscription API remains on the core adapter boundary; product-specific evidence possible.

### M4 — Event model and normalized persistence
- Add typed Trade, BBO, AssetContext and Status events without changing existing semantics.
- Add versioned normalized journal alongside raw frames.
- Add replay equality tests from raw to normalized to core state.
- Preserve SBE as compatibility projection.

Exit: exact numeric round-trip; replay output hashes match; unsupported projections fail without data loss.

### M5 — Hyperliquid adapter, staged
- H0 reference data: perp metadata, DEX scoping and spot metadata stored separately; precision/specifications pinned.
- H1 allMids + fastAssetCtxs; partial updates merge correctly; global-feed evidence separate.
- H2 10–20 selected perps for l2Book snapshots and trades; measure feed rates, latency, precision, heartbeats, queue pressure and disconnect recovery.
- H3 expand products/instruments only after shared per-IP budget admission and measured capacity.
- Do not claim all-L2 coverage from allMids or asset contexts.

Exit: decoder fixtures, protocol tests, live-smoke manifest, reconnect evidence, exact precision round-trip and separate product coverage.

### M6 — Venue-specific certification and scale
- Polymarket discovery fingerprint and point-in-time eligibility definition.
- Hyperliquid universe and product-specific coverage.
- Kalshi authenticated discovery and coverage when permission exists.
- Synthetic capacity sweep first: 10k, 50k, 100k, 175k, 320k, 550k+ with measured resources.
- Validate shard count, connections, queue memory, RSS peak, CPU, descriptors, disk, subscriptions, snapshot rates and recovery.
- Then run live-universe on independently available venues.
- Test 24-hour soak only after bounded live-universe operation.

Exit: separate signed certification records for each venue/product. No forced dual-venue all-or-nothing result.

### M7 — Research platform
- Immutable historical datasets and point-in-time universe snapshots.
- Feature computation with lineage.
- Hypothesis registry and experiment tracking.
- Statistical validation gates.
- Event-driven backtest with venue-specific execution and fees.
- Accepted/rejected strategy artifacts.

Exit: independently reproducible OOS result with all attempted experiments and costs recorded.

### M8 — Execution boundary
- Separate execution process and deployment identity.
- Broker-neutral OMS.
- Independent risk engine with hard limits and kill switch.
- Venue adapters with idempotent client order IDs and reconciliation.
- Paper/shadow operations before any restricted live canary.

Exit: risk admission cannot be bypassed by research or strategy code; kill control independently verified.

## PR #42 disposition

Keep #42 draft. It is useful as an initial synthetic stress correctness increment and has successful CI, but it is not a complete capacity or certification system.

Before phase matrix execution, resolve the M1 findings:
- Event accounting does not yet evidence every stage named in the design.
- Required metrics must be typed and absence must fail closed.
- The manifest and shared index need concurrency and crash consistency tests.
- Current checked-in reference manifests identify source commit `5c9e45a`, not PR head `c0eaea2`; regenerate evidence only after the final runner code is frozen.
- The malformed-index fixture correctly asserts no artifact is written, but failure evidence/recovery semantics must be specified.
- Per-cell configuration and output path validation must be explicit.
- Writer queue accounting, mock socket completion and journal counts need separate semantics.

No merge, phase matrix, live access request, large-scale run, or production claim is authorized by this migration document.

## Definition of done

A feature is done only when:
1. Its invariants are written as executable tests.
2. Failure modes produce explicit states and retained evidence.
3. Exact inputs, version and provenance are recorded.
4. Replay reproduces the result.
5. The correct evidence class supports the claim.
6. Independent reviewers can reproduce the verification commands.
7. Known limits are stated.
8. The feature does not cross research/execution or venue certification boundaries.

The platform succeeds by rejecting unsupported claims and invalid hypotheses, not by maximizing model count, symbol count, or benchmark headlines.

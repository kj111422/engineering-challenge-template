//! Synchronous collector state machine.
//!
//! Exchange sessions and transport concerns live outside this module. The core
//! consumes normalized events, maintains one shadow book per source, chooses
//! the active source for each symbol, and produces serialized SBE messages.
//! This makes the correctness-critical logic deterministic and testable without
//! a network.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use tracing::{error, warn};

use crate::{
    Side,
    config::{ExchangeKind, Settings},
    fixed::SCALE,
    metrics::CollectorStats,
    model::{
        BookSnapshot, Control, DiscontinuityReason, EventMeta, ExchangeEvent, LifecycleKind,
        MarketInfo, OutputMessage, QuantityChange, ResetReason, SourceId, SourceStatus,
    },
    orderbook::OrderBook,
    time::utc_from_us,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};

/// Tuning for the synchronous core. Durations are microseconds so the core can
/// be driven by replay timestamps as well as wall-clock ticks.
#[derive(Debug, Clone)]
pub struct CoreConfig {
    pub exchange: String,
    pub shard_count: usize,
    pub replica_count: usize,
    pub snapshot_interval_us: i64,
    pub stale_probe_after_us: i64,
    pub probe_interval_us: i64,
    pub max_probes: u32,
    pub crossed_grace_us: i64,
    /// Whether the exchange provides a sequence number or clock that can
    /// be used to verify backup book freshness. True for Kalshi, false for Polymarket.
    pub exchange_has_clock: bool,
}

impl CoreConfig {
    pub fn from_settings(settings: &Settings, shard_count: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(shard_count >= 1, "shard count must be >= 1");
        anyhow::ensure!(
            settings.collector.replicas >= 1,
            "replica count must be >= 1"
        );
        let seconds_to_us = |seconds: u64| -> anyhow::Result<i64> {
            i64::try_from(seconds)
                .context("duration exceeds i64 seconds")?
                .checked_mul(1_000_000)
                .context("duration overflows microseconds")
        };
        let exchange_has_clock = settings.exchange == ExchangeKind::Kalshi;
        Ok(Self {
            exchange: settings.exchange.to_string(),
            shard_count,
            replica_count: settings.collector.replicas,
            snapshot_interval_us: seconds_to_us(settings.collector.snapshot_interval_secs)?,
            stale_probe_after_us: seconds_to_us(settings.collector.stale_probe_after_secs)?,
            probe_interval_us: seconds_to_us(settings.collector.probe_interval_secs)?,
            max_probes: settings.collector.max_probes,
            crossed_grace_us: 2_000_000,
            exchange_has_clock,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct SourceBook {
    book: OrderBook,
    valid: bool,
    /// Clock attached to the currently valid book.
    last_clock: Option<i64>,
    /// Most recent clock observed from this connection, retained even after
    /// the book is invalidated. This distinguishes a shadow that was already
    /// behind from a source that was current and then reported a lower clock
    /// after exchange maintenance.
    last_observed_clock: Option<i64>,
    last_exchange_ts_us: Option<i64>,
    last_received_at_us: Option<i64>,
}

impl SourceBook {
    fn invalidate(&mut self) {
        self.book = OrderBook::new();
        self.valid = false;
        self.last_clock = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscontinuityIncident {
    /// Symbol name
    symbol: String,
    /// Start of the gap (when continuity was lost)
    started_at_us: i64,
    /// End of the gap (when re-anchored), if resolved
    resolved_at_us: Option<i64>,
    /// Reason continuity was lost
    reason: crate::model::DiscontinuityReason,
    /// Whether the gap interval is quantifiable (Kalshi has seq/clock; Polymarket does not)
    quantifiable: bool,
}

#[derive(Debug)]
struct MarketState {
    info: MarketInfo,
    shard: usize,
    active: Option<SourceId>,
    books: BTreeMap<SourceId, SourceBook>,
    /// Sources with an outstanding explicit snapshot request. A lower clock on
    /// one of these snapshots is evidence of a Kalshi maintenance reset, not a
    /// stale shadow frame.
    recovery_sources: BTreeSet<SourceId>,
    emitted_clock: Option<i64>,
    emitted_exchange_ts_us: Option<i64>,
    emitted_any: bool,
    last_emit_us: Option<i64>,
    next_snapshot_us: i64,
    probes: u32,
    last_probe_us: i64,
    crossed_since_us: Option<i64>,
    /// Per-symbol continuity state
    continuity: crate::model::ContinuityState,
    /// Open discontinuity incident (None if no active gap)
    incident: Option<DiscontinuityIncident>,
    /// All resolved incidents for this symbol (retained for reporting)
    resolved_incidents: Vec<DiscontinuityIncident>,
}

impl MarketState {
    fn active_book(&self) -> Option<&SourceBook> {
        self.active
            .and_then(|source| self.books.get(&source))
            .filter(|book| book.valid)
    }

    fn continuity(&self) -> crate::model::ContinuityState {
        self.continuity
    }

    fn set_continuity(&mut self, new_state: crate::model::ContinuityState) {
        self.continuity = new_state;
    }

    fn start_incident(&mut self, reason: DiscontinuityReason, now_us: i64, quantifiable: bool) {
        if self.incident.is_some() {
            // Incident already open; update reason if more specific
            return;
        }
        self.incident = Some(DiscontinuityIncident {
            symbol: self.info.symbol.clone(),
            started_at_us: now_us,
            resolved_at_us: None,
            reason,
            quantifiable,
        });
        // Transition continuity to Resyncing unless Terminal
        if self.continuity != crate::model::ContinuityState::Terminal {
            self.continuity = crate::model::ContinuityState::Resyncing;
        }
    }

    fn resolve_incident(&mut self, now_us: i64) {
        if let Some(mut incident) = self.incident.take() {
            incident.resolved_at_us = Some(now_us);
            self.resolved_incidents.push(incident);
        }
        // If no other blocker, go Live; otherwise stay in current state
        if self.continuity == crate::model::ContinuityState::Resyncing {
            self.continuity = crate::model::ContinuityState::Live;
        }
    }

    fn mark_terminal(&mut self, now_us: i64) {
        self.start_incident(DiscontinuityReason::Terminal, now_us, true);
        self.continuity = crate::model::ContinuityState::Terminal;
    }
}

#[derive(Debug, Default)]
struct LifecycleMetadata {
    close_time_us: Option<i64>,
    title: Option<String>,
    tick_size_us: Option<i64>,
    price_ranges: Option<Vec<crate::model::PriceRange>>,
    price_level_structure: Option<String>,
}

#[derive(Debug, Default)]
struct ShardState {
    symbols: BTreeSet<String>,
    sources: Vec<SourceId>,
}

/// The collector core. All methods are synchronous; callers batch outputs and
/// apply control messages to the appropriate source workers.
pub struct CollectorCore {
    config: CoreConfig,
    markets: BTreeMap<String, MarketState>,
    shards: Vec<ShardState>,
    statuses: BTreeMap<SourceId, SourceStatus>,
    dirty_shards: BTreeSet<usize>,
    snapshot_requests: BTreeMap<SourceId, BTreeSet<String>>,
    reconnect_requests: Vec<(SourceId, &'static str)>,
    shutting_down: bool,
    stats: CollectorStats,
}

impl CollectorCore {
    pub fn new(config: CoreConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(config.shard_count >= 1, "at least one shard is required");
        anyhow::ensure!(
            config.replica_count >= 1,
            "at least one replica is required"
        );
        anyhow::ensure!(
            config.shard_count <= u16::MAX as usize,
            "too many shards for SourceId"
        );
        anyhow::ensure!(
            config.replica_count <= u16::MAX as usize,
            "too many replicas for SourceId"
        );
        anyhow::ensure!(
            config.snapshot_interval_us > 0,
            "snapshot interval must be positive"
        );
        let shards = (0..config.shard_count)
            .map(|shard| ShardState {
                symbols: BTreeSet::new(),
                sources: (0..config.replica_count)
                    .map(|replica| SourceId::new(shard as u16, replica as u16))
                    .collect(),
            })
            .collect();
        let stats = CollectorStats {
            shard_count: config.shard_count as u64,
            ..CollectorStats::default()
        };
        Ok(Self {
            config,
            markets: BTreeMap::new(),
            shards,
            statuses: BTreeMap::new(),
            dirty_shards: BTreeSet::new(),
            snapshot_requests: BTreeMap::new(),
            reconnect_requests: Vec::new(),
            shutting_down: false,
            stats,
        })
    }

    #[must_use]
    pub fn config(&self) -> &CoreConfig {
        &self.config
    }

    #[must_use]
    pub fn stats(&self) -> CollectorStats {
        self.stats.clone()
    }

    /// Preview the exact shard loads that [`Self::apply_discovery`] would
    /// produce. This is deliberately side-effect free so the coordinator can
    /// reject an over-capacity discovery round before it retires books,
    /// dispatches controls, emits snapshots, or persists a checkpoint.
    pub fn projected_shard_loads(&self, markets: &[MarketInfo]) -> anyhow::Result<Vec<usize>> {
        let mut desired = BTreeMap::new();
        let mut output_symbols = BTreeSet::new();
        for info in markets {
            info.validate().with_context(|| {
                format!(
                    "invalid discovery metadata for `{}` ({})",
                    info.native_symbol, info.symbol
                )
            })?;
            anyhow::ensure!(
                output_symbols.insert(info.symbol.clone()),
                "duplicate output symbol `{}` in discovery result",
                info.symbol
            );
            anyhow::ensure!(
                desired.insert(info.native_symbol.clone(), info).is_none(),
                "duplicate native symbol `{}` in discovery result",
                info.native_symbol
            );
        }
        let mut loads: Vec<usize> = self
            .shards
            .iter()
            .map(|shard| shard.symbols.len())
            .collect();
        // Existing identities retain their shard. Remove anything that will
        // disappear or be reidentified, exactly as apply_discovery does.
        for (native, state) in &self.markets {
            if desired
                .get(native)
                .is_none_or(|info| info.symbol != state.info.symbol)
            {
                loads[state.shard] = loads[state.shard].saturating_sub(1);
            }
        }
        let mut additions: Vec<_> = desired
            .iter()
            .filter(|(native, info)| {
                self.markets
                    .get(*native)
                    .is_none_or(|state| state.info.symbol != info.symbol)
            })
            .collect();
        additions.sort_by(|left, right| left.0.cmp(right.0));
        for _ in additions {
            let shard = loads
                .iter()
                .enumerate()
                .min_by_key(|(index, load)| (**load, *index))
                .map_or(0, |(index, _)| index);
            loads[shard] = loads[shard].saturating_add(1);
        }
        Ok(loads)
    }

    #[must_use]
    pub fn statuses(&self) -> &BTreeMap<SourceId, SourceStatus> {
        &self.statuses
    }

    #[must_use]
    pub fn market_infos(&self) -> Vec<MarketInfo> {
        let mut infos: Vec<MarketInfo> = self
            .markets
            .values()
            .map(|state| state.info.clone())
            .collect();
        infos.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        infos
    }

    #[must_use]
    pub fn symbol_count(&self) -> usize {
        self.markets.len()
    }

    /// Coverage gauges for readiness.
    ///
    /// A symbol is anchored only when an active source has a valid book that
    /// was established by an exchange snapshot. A subscription command that
    /// was sent but not answered therefore never counts as anchored. Stale
    /// symbols are anchored books whose last valid update is older than
    /// `stale_after_us`.
    #[must_use]
    pub fn coverage(&self, now_us: i64, stale_after_us: i64) -> crate::metrics::CoverageStats {
        let mut anchored = 0u64;
        let mut stale = 0u64;
        let mut oldest_anchor_us: Option<i64> = None;
        let mut unseeded = 0u64;
        let mut live = 0u64;
        let mut stale_detail = 0u64;
        let mut resyncing = 0u64;
        let mut terminal = 0u64;
        for state in self.markets.values() {
            match state.continuity {
                crate::model::ContinuityState::Unseeded => unseeded = unseeded.saturating_add(1),
                crate::model::ContinuityState::Live => live = live.saturating_add(1),
                crate::model::ContinuityState::Stale => {
                    stale_detail = stale_detail.saturating_add(1)
                }
                crate::model::ContinuityState::Resyncing => resyncing = resyncing.saturating_add(1),
                crate::model::ContinuityState::Terminal => terminal = terminal.saturating_add(1),
            }
            let Some(book) = state.active_book() else {
                continue;
            };
            anchored = anchored.saturating_add(1);
            let last = book.last_received_at_us.unwrap_or(0);
            if now_us.saturating_sub(last) > stale_after_us {
                stale = stale.saturating_add(1);
            }
            oldest_anchor_us = Some(oldest_anchor_us.map_or(last, |oldest| oldest.min(last)));
        }
        let expected_subscriptions = self
            .shards
            .iter()
            .map(|shard| shard.symbols.len().saturating_mul(shard.sources.len()))
            .sum::<usize>() as u64;
        let subscribed_symbols = self
            .statuses
            .values()
            .filter(|status| status.connected)
            .map(|status| status.subscriptions as u64)
            .sum();
        let shards_with_connected_source = self
            .shards
            .iter()
            .filter(|shard| {
                shard.sources.iter().any(|source| {
                    self.statuses
                        .get(source)
                        .is_some_and(|status| status.connected)
                })
            })
            .count() as u64;
        crate::metrics::CoverageStats {
            expected_symbols: self.markets.len() as u64,
            anchored_symbols: anchored,
            stale_symbols: stale,
            unseeded_symbols: unseeded,
            live_symbols: live,
            stale_symbols_detail: stale_detail,
            resyncing_symbols: resyncing,
            terminal_symbols: terminal,
            expected_subscriptions,
            subscribed_symbols,
            shards_with_connected_source,
            shards: self.shards.len() as u64,
            sources_connected: self
                .statuses
                .values()
                .filter(|status| status.connected)
                .count() as u64,
            sources_total: self
                .shards
                .iter()
                .map(|shard| shard.sources.len())
                .sum::<usize>() as u64,
            oldest_anchor_us,
        }
    }

    pub fn begin_shutdown(&mut self) {
        self.shutting_down = true;
    }

    /// Reconcile the desired symbol set.
    ///
    /// A discovery result is accepted or rejected as a whole. Silently skipping
    /// one malformed market would make every other current symbol look absent
    /// and could tear down a healthy subscription set, so callers retain the
    /// previous set when this returns an error.
    ///
    /// Output-symbol identity is treated as immutable per native symbol. A
    /// rediscovery that keeps the native symbol but changes the output symbol
    /// (for example after a venue identifier migration) is applied as an
    /// explicit retirement of the old identity — emitting a final empty
    /// snapshot under the old symbol — followed by a fresh market state under
    /// the new symbol. Downstream consumers therefore never see deltas for a
    /// symbol whose full snapshot they have not observed.
    pub fn apply_discovery(
        &mut self,
        now_us: i64,
        markets: Vec<MarketInfo>,
    ) -> anyhow::Result<Vec<OutputMessage>> {
        self.stats.discovery_rounds = self.stats.discovery_rounds.saturating_add(1);
        let mut desired: BTreeMap<String, MarketInfo> = BTreeMap::new();
        let mut symbols: BTreeSet<String> = BTreeSet::new();
        for info in markets {
            info.validate().with_context(|| {
                format!(
                    "invalid discovery metadata for `{}` ({})",
                    info.native_symbol, info.symbol
                )
            })?;
            if !symbols.insert(info.symbol.clone()) {
                bail!(
                    "duplicate output symbol `{}` in discovery result",
                    info.symbol
                );
            }
            let native = info.native_symbol.clone();
            if desired.contains_key(&native) {
                bail!("duplicate native symbol `{native}` in discovery result");
            }
            desired.insert(native, info);
        }
        self.stats.discovered_symbols = desired.len() as u64;

        let mut outputs = Vec::new();
        let removals: Vec<String> = self
            .markets
            .keys()
            .filter(|native| !desired.contains_key(*native))
            .cloned()
            .collect();
        for native in removals {
            outputs.extend(self.remove_market(&native, now_us, true));
        }

        let mut additions = Vec::new();
        let mut reidentities: Vec<(String, MarketInfo)> = Vec::new();
        for (native, info) in desired {
            if let Some(state) = self.markets.get_mut(&native) {
                if state.info.symbol == info.symbol {
                    // Same identity: refresh mutable metadata only.
                    state.info = info;
                } else {
                    // Changed output identity: retire the old symbol first.
                    reidentities.push((native, info));
                }
            } else {
                additions.push((native, info));
            }
        }
        for (native, info) in reidentities {
            outputs.extend(self.remove_market(&native, now_us, true));
            self.add_market(native, info, now_us);
        }
        additions.sort_by(|left, right| left.0.cmp(&right.0));
        for (native, info) in additions {
            self.add_market(native, info, now_us);
        }

        self.stats.current_symbols = self.markets.len() as u64;
        Ok(outputs)
    }

    fn add_market(&mut self, native: String, info: MarketInfo, now_us: i64) {
        let shard = self
            .shards
            .iter()
            .enumerate()
            .min_by_key(|(index, shard)| (shard.symbols.len(), *index))
            .map_or(0, |(index, _)| index);
        let sources = self.shards[shard].sources.clone();
        let mut books = BTreeMap::new();
        for source in sources {
            books.insert(source, SourceBook::default());
        }
        let state = MarketState {
            info,
            shard,
            active: None,
            books,
            recovery_sources: BTreeSet::new(),
            emitted_clock: None,
            emitted_exchange_ts_us: None,
            emitted_any: false,
            last_emit_us: None,
            next_snapshot_us: now_us.saturating_add(self.config.snapshot_interval_us),
            probes: 0,
            last_probe_us: now_us,
            crossed_since_us: None,
            continuity: crate::model::ContinuityState::Unseeded,
            incident: None,
            resolved_incidents: Vec::new(),
        };
        self.shards[shard].symbols.insert(native.clone());
        self.markets.insert(native, state);
        self.dirty_shards.insert(shard);
        self.stats.added_symbols = self.stats.added_symbols.saturating_add(1);
        self.stats.current_symbols = self.markets.len() as u64;
    }

    fn remove_market(&mut self, native: &str, now_us: i64, emit_empty: bool) -> Vec<OutputMessage> {
        let Some(state) = self.markets.remove(native) else {
            return Vec::new();
        };
        self.shards[state.shard].symbols.remove(native);
        self.dirty_shards.insert(state.shard);
        self.stats.removed_symbols = self.stats.removed_symbols.saturating_add(1);
        self.stats.current_symbols = self.markets.len() as u64;

        let mut outputs = Vec::new();
        if emit_empty && state.emitted_any {
            let exchange_ts = state.emitted_exchange_ts_us.unwrap_or(now_us);
            outputs.push(OutputMessage::Snapshot(build_empty_snapshot(
                &state.info,
                now_us,
                exchange_ts,
            )));
            self.stats.empty_snapshots = self.stats.empty_snapshots.saturating_add(1);
        }
        outputs
    }

    /// Consume one normalized event from a source.
    pub fn handle_event(&mut self, source: SourceId, event: ExchangeEvent) -> Vec<OutputMessage> {
        match event {
            ExchangeEvent::Snapshot {
                native_symbol,
                book,
                meta,
            } => self.handle_snapshot(source, &native_symbol, book, meta),
            ExchangeEvent::LevelChange {
                native_symbol,
                side,
                price,
                change,
                meta,
            } => self.handle_level_change(source, &native_symbol, side, price, change, meta),
            ExchangeEvent::Lifecycle {
                native_symbol,
                kind,
                close_time_us,
                title,
                tick_size_us,
                price_ranges,
                price_level_structure,
                meta,
            } => self.handle_lifecycle(
                &native_symbol,
                kind,
                LifecycleMetadata {
                    close_time_us,
                    title,
                    tick_size_us,
                    price_ranges,
                    price_level_structure,
                },
                meta,
            ),
            ExchangeEvent::StreamReset {
                native_symbols,
                reason,
                received_at_us,
            } => self.handle_stream_reset(source, native_symbols, reason, received_at_us),
        }
    }

    fn handle_snapshot(
        &mut self,
        source: SourceId,
        native: &str,
        snapshot: BookSnapshot,
        meta: EventMeta,
    ) -> Vec<OutputMessage> {
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        if !state.books.contains_key(&source) {
            return Vec::new();
        }

        let mut book = OrderBook::new();
        if let Err(error) = book.replace(&snapshot) {
            warn!(%source, native, error = %error, "rejecting malformed snapshot");
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
            }
            if state.active == Some(source) {
                state.active = None;
            }
            self.queue_snapshot(source, native);
            return self.reseat(native, meta.received_at_us);
        }
        if book.is_crossed() {
            self.stats.crossed_snapshots_skipped =
                self.stats.crossed_snapshots_skipped.saturating_add(1);
            warn!(%source, native, "rejecting crossed snapshot");
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
            }
            if state.active == Some(source) {
                state.active = None;
            }
            self.queue_snapshot(source, native);
            return self.reseat(native, meta.received_at_us);
        }

        let exchange_ts = meta.exchange_ts_or_received();
        let was_emitted = state.emitted_any;
        let was_active = state.active;
        let recovery = state.recovery_sources.contains(&source);
        let source_book_was_valid = state.books.get(&source).is_some_and(|book| book.valid);
        let clock_regression = clock_regressed(state.emitted_clock, meta.clock);

        // Kalshi's clock is not comparable across exchange maintenance. A lower
        // clock is trusted only from the active connection, or from a requested
        // snapshot on a source whose old book was already invalidated (for
        // example after reconnecting). A lower clock on a still-valid shadow
        // book is stale and must not be promoted.
        let recovery_trusted = recovery
            && !source_book_was_valid
            && match (
                state.books[&source].last_observed_clock,
                state.emitted_clock,
            ) {
                (None, _) => true,
                (Some(observed), Some(emitted)) => observed >= emitted,
                (Some(_), None) => true,
            };
        if clock_regression && was_active != Some(source) && !recovery_trusted {
            if let Some(source_book) = state.books.get_mut(&source) {
                if let Some(clock) = meta.clock {
                    source_book.last_observed_clock = Some(
                        source_book
                            .last_observed_clock
                            .map_or(clock, |observed| observed.max(clock)),
                    );
                }
                // A lower-clock snapshot on a shadow cannot establish the
                // current epoch. Invalidate it so a later failover requires a
                // fresh connection instead of trusting this stale book.
                source_book.invalidate();
            }
            return Vec::new();
        }
        if was_active.is_none()
            && was_emitted
            && state.emitted_clock.is_some()
            && meta.clock.is_none()
        {
            return Vec::new();
        }

        if clock_regression {
            self.stats.clock_resets = self.stats.clock_resets.saturating_add(1);
            for (book_source, source_book) in &mut state.books {
                if *book_source != source {
                    source_book.invalidate();
                }
            }
            state.recovery_sources.clear();
            // Clock regression on active source creates a discontinuity
            if was_active == Some(source) {
                state.start_incident(
                    DiscontinuityReason::SequenceGap,
                    meta.received_at_us,
                    meta.clock.is_some(),
                );
                if !meta.clock.is_some() {
                    self.stats.unquantifiable_gaps =
                        self.stats.unquantifiable_gaps.saturating_add(1);
                }
                self.stats.discontinuity_incidents =
                    self.stats.discontinuity_incidents.saturating_add(1);
            }
        }

        {
            let Some(source_book) = state.books.get_mut(&source) else {
                error!(%source, native, "source book vanished while applying a snapshot");
                return Vec::new();
            };
            source_book.book = book;
            source_book.valid = true;
            // A snapshot establishes the complete clock state. Retaining an
            // older clock when the snapshot omits one would let a stale shadow
            // book win a later failover comparison.
            source_book.last_clock = meta.clock;
            if meta.clock.is_some() {
                source_book.last_observed_clock = meta.clock;
            }
            source_book.last_exchange_ts_us = Some(exchange_ts);
            source_book.last_received_at_us = Some(meta.received_at_us);
        }

        let should_emit = was_active.is_none() || was_active == Some(source);
        if !should_emit {
            state.recovery_sources.remove(&source);
            return Vec::new();
        }
        if was_active.is_none() || clock_regression || recovery {
            state.recovery_sources.remove(&source);
        }
        if was_active.is_none() {
            state.recovery_sources.clear();
        }
        // Probe and crossed-book state belong to the active source. A healthy
        // shadow replica must not reset them, or a quiet active source would
        // never reach the stale-probe threshold.
        state.probes = 0;
        state.last_probe_us = meta.received_at_us;
        state.crossed_since_us = None;

        // Manage continuity state
        if was_active.is_none() && !was_emitted {
            // First snapshot for this symbol: Unseeded -> Live
            state.set_continuity(crate::model::ContinuityState::Live);
        } else if was_active.is_none() && was_emitted {
            // Failover/recovery: Resyncing -> Live (if incident resolves)
            state.resolve_incident(meta.received_at_us);
        } else if was_active == Some(source) {
            // Snapshot on active source: any Stale/Resyncing incident resolves
            // This handles repair after stale probe on the same connection
            if state.continuity == crate::model::ContinuityState::Stale
                || state.continuity == crate::model::ContinuityState::Resyncing
            {
                state.resolve_incident(meta.received_at_us);
            }
        } else if clock_regression && was_active == Some(source) {
            // Active source clock regression: incident already started above
            // Stay in Resyncing until a new snapshot resolves it
        }
        // Update discontinuity stats on resolution
        if was_active.is_none() && was_emitted {
            self.stats.discontinuity_incidents =
                self.stats.discontinuity_incidents.saturating_add(1);
        }

        let message = build_snapshot(
            &state.info,
            &state.books[&source].book,
            meta.received_at_us,
            exchange_ts,
        );
        if was_active.is_none() {
            state.active = Some(source);
            if was_emitted {
                self.stats.failover_snapshots = self.stats.failover_snapshots.saturating_add(1);
                self.stats.source_switches = self.stats.source_switches.saturating_add(1);
            } else {
                self.stats.exchange_snapshots = self.stats.exchange_snapshots.saturating_add(1);
            }
        } else {
            self.stats.exchange_snapshots = self.stats.exchange_snapshots.saturating_add(1);
        }
        state.emitted_any = true;
        state.emitted_clock = if clock_regression {
            meta.clock
        } else {
            meta.clock.or(state.emitted_clock)
        };
        state.emitted_exchange_ts_us = Some(exchange_ts);
        state.last_emit_us = Some(meta.received_at_us);
        state.next_snapshot_us = if was_emitted {
            meta.received_at_us
                .saturating_add(self.config.snapshot_interval_us)
        } else {
            first_snapshot_deadline(
                meta.received_at_us,
                self.config.snapshot_interval_us,
                &state.info.symbol,
            )
        };
        vec![OutputMessage::Snapshot(message)]
    }

    fn handle_level_change(
        &mut self,
        source: SourceId,
        native: &str,
        side: Side,
        price: i64,
        change: QuantityChange,
        meta: EventMeta,
    ) -> Vec<OutputMessage> {
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        // Drop deltas for symbols in Resyncing/Stale/Unseeded/Terminal continuity state,
        // but allow deltas from the active source (which may be sending a repair snapshot).
        if state.continuity != crate::model::ContinuityState::Live && state.active != Some(source) {
            return Vec::new();
        }
        let clock_regression = clock_regressed(state.emitted_clock, meta.clock);
        let (quantity, crossed) = {
            let Some(source_book) = state.books.get_mut(&source) else {
                return Vec::new();
            };
            if !source_book.valid {
                return Vec::new();
            }
            // A lower clock on a shadow connection is either stale or belongs
            // to a new maintenance epoch that this source has not established
            // with a snapshot. Never apply it to the shadow book.
            if clock_regression && state.active != Some(source) {
                return Vec::new();
            }
            if let Err(error) = source_book.book.apply(side, price, change) {
                self.stats.deltas_rejected = self.stats.deltas_rejected.saturating_add(1);
                warn!(%source, native, error = %error, "rejecting order-book delta");
                source_book.invalidate();
                if state.active == Some(source) {
                    state.active = None;
                }
                self.queue_snapshot(source, native);
                return self.reseat(native, meta.received_at_us);
            }

            let exchange_ts = meta.exchange_ts_or_received();
            source_book.last_clock = meta.clock.or(source_book.last_clock);
            if meta.clock.is_some() {
                source_book.last_observed_clock = meta.clock;
            }
            source_book.last_exchange_ts_us = Some(exchange_ts);
            source_book.last_received_at_us = Some(meta.received_at_us);
            (
                source_book.book.quantity(side, price).unwrap_or(0),
                source_book.book.is_crossed(),
            )
        };

        if clock_regression {
            self.stats.clock_resets = self.stats.clock_resets.saturating_add(1);
            for (book_source, source_book) in &mut state.books {
                if *book_source != source {
                    source_book.invalidate();
                }
            }
            state.recovery_sources.clear();
        }

        let exchange_ts = meta.exchange_ts_or_received();
        if state.active == Some(source) {
            state.probes = 0;
            state.last_probe_us = meta.received_at_us;
            if matches!(change, QuantityChange::Delta(0)) {
                if clock_regression {
                    state.emitted_clock = meta.clock;
                }
                return Vec::new();
            }
            state.emitted_clock = if clock_regression {
                meta.clock
            } else {
                meta.clock.or(state.emitted_clock)
            };
            state.emitted_exchange_ts_us = Some(exchange_ts);
            state.last_emit_us = Some(meta.received_at_us);
            if crossed {
                state.crossed_since_us.get_or_insert(meta.received_at_us);
            } else {
                state.crossed_since_us = None;
            }
            let symbol = state.info.symbol.clone();
            let update = build_delta(
                symbol,
                side,
                price,
                quantity,
                meta.received_at_us,
                exchange_ts,
            );
            self.stats.deltas_emitted = self.stats.deltas_emitted.saturating_add(1);
            vec![OutputMessage::PriceLevelUpdate(update)]
        } else {
            Vec::new()
        }
    }

    fn handle_lifecycle(
        &mut self,
        native: &str,
        kind: LifecycleKind,
        update: LifecycleMetadata,
        meta: EventMeta,
    ) -> Vec<OutputMessage> {
        self.stats.lifecycle_events = self.stats.lifecycle_events.saturating_add(1);
        if kind.is_terminal() || matches!(kind, LifecycleKind::Deactivated) {
            if let Some(state) = self.markets.get_mut(native) {
                state.mark_terminal(meta.received_at_us);
            }
            return self.remove_market(native, meta.received_at_us, true);
        }
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        let LifecycleMetadata {
            close_time_us,
            title,
            tick_size_us,
            price_ranges,
            price_level_structure,
        } = update;
        match kind {
            LifecycleKind::CloseDateUpdated => {
                state.info.close_time_us = close_time_us;
            }
            LifecycleKind::MetadataUpdated => {
                if title.is_some() {
                    state.info.title = title;
                }
            }
            LifecycleKind::PriceStructureUpdated => {
                if let Some(ranges) = price_ranges {
                    state.info.tick_size_us = tick_size_us.or_else(|| min_tick(&ranges));
                    state.info.price_ranges = ranges;
                } else if tick_size_us.is_some() {
                    state.info.tick_size_us = tick_size_us;
                }
                if price_level_structure.is_some() {
                    state.info.price_level_structure = price_level_structure;
                }
            }
            LifecycleKind::Created
            | LifecycleKind::Activated
            | LifecycleKind::NewMarket
            | LifecycleKind::Other(_) => {
                if let Some(close_time_us) = close_time_us {
                    state.info.close_time_us = Some(close_time_us);
                }
                if title.is_some() {
                    state.info.title = title;
                }
                if tick_size_us.is_some() {
                    state.info.tick_size_us = tick_size_us;
                }
                if let Some(ranges) = price_ranges {
                    state.info.tick_size_us = tick_size_us.or_else(|| min_tick(&ranges));
                    state.info.price_ranges = ranges;
                }
                if price_level_structure.is_some() {
                    state.info.price_level_structure = price_level_structure;
                }
            }
            LifecycleKind::Determined
            | LifecycleKind::Settled
            | LifecycleKind::Resolved
            | LifecycleKind::Deactivated => {}
        }
        Vec::new()
    }

    fn handle_stream_reset(
        &mut self,
        source: SourceId,
        native_symbols: Vec<String>,
        reason: ResetReason,
        received_at_us: i64,
    ) -> Vec<OutputMessage> {
        if matches!(
            reason,
            ResetReason::SequenceGap | ResetReason::ProtocolError
        ) {
            self.stats.sequence_resets = self.stats.sequence_resets.saturating_add(1);
        }
        let affected: Vec<String> = if native_symbols.is_empty() {
            self.markets
                .keys()
                .filter(|native| {
                    self.markets
                        .get(*native)
                        .is_some_and(|state| state.shard == source.shard as usize)
                })
                .cloned()
                .collect()
        } else {
            native_symbols
        };

        let mut outputs = Vec::new();
        for native in affected {
            let Some(state) = self.markets.get_mut(&native) else {
                continue;
            };
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
                if matches!(reason, ResetReason::Connected) {
                    // A new connection has no cross-epoch clock history.
                    book.last_observed_clock = None;
                }
            }
            if state.active == Some(source) {
                state.active = None;
                // Start discontinuity incident on active source loss
                let disc_reason = match reason {
                    ResetReason::SequenceGap => DiscontinuityReason::SequenceGap,
                    ResetReason::ProtocolError => DiscontinuityReason::ProtocolError,
                    ResetReason::SubscriptionRefresh => DiscontinuityReason::StreamReset,
                    ResetReason::RequestedRefresh => DiscontinuityReason::StreamReset,
                    ResetReason::Connected => DiscontinuityReason::ActiveSourceLost,
                };
                state.start_incident(disc_reason, received_at_us, self.config.exchange_has_clock);
                self.stats.discontinuity_incidents =
                    self.stats.discontinuity_incidents.saturating_add(1);
                if matches!(reason, ResetReason::ProtocolError) {
                    self.queue_snapshot(source, &native);
                }
                if !self.shutting_down {
                    outputs.extend(self.reseat(&native, received_at_us));
                }
            } else if matches!(reason, ResetReason::ProtocolError) {
                self.queue_snapshot(source, &native);
            }
        }
        outputs
    }

    /// Apply a status transition. Losing a source immediately invalidates all of
    /// its shadow books and fails over any symbols where it was active.
    ///
    /// `now_us` is the generation time of this transition. Generated failover
    /// snapshots carry it, so their local timestamp never precedes the receive
    /// time of any update included in the replacement book. The lost source's
    /// own frame time stays in its [`SourceStatus`] and is not reused as a
    /// snapshot timestamp.
    pub fn handle_status(&mut self, status: SourceStatus, now_us: i64) -> Vec<OutputMessage> {
        let source = status.source;
        if source.shard as usize >= self.shards.len() {
            return Vec::new();
        }
        let was_connected = self.statuses.get(&source).is_some_and(|old| old.connected);
        self.statuses.insert(source, status.clone());
        if status.connected || !was_connected {
            return Vec::new();
        }

        let mut affected = Vec::new();
        for (native, state) in &mut self.markets {
            if state.shard != source.shard as usize {
                continue;
            }
            if let Some(book) = state.books.get_mut(&source) {
                book.invalidate();
            }
            if state.active == Some(source) {
                state.active = None;
                // Start discontinuity incident on active source loss
                let disc_reason = DiscontinuityReason::ActiveSourceLost;
                state.start_incident(disc_reason, now_us, self.config.exchange_has_clock);
                self.stats.discontinuity_incidents =
                    self.stats.discontinuity_incidents.saturating_add(1);
                affected.push(native.clone());
            }
        }
        if self.shutting_down {
            return Vec::new();
        }
        let mut outputs = Vec::new();
        for native in affected {
            outputs.extend(self.reseat(&native, now_us));
        }
        outputs
    }

    /// Periodic maintenance: local snapshots, stale probes, and failover.
    pub fn tick(&mut self, now_us: i64) -> Vec<OutputMessage> {
        let statuses = &self.statuses;
        let interval = self.config.snapshot_interval_us;
        let stale_after = self.config.stale_probe_after_us;
        let probe_spacing = self.config.probe_interval_us;
        let max_probes = self.config.max_probes;
        let crossed_grace = self.config.crossed_grace_us;
        let mut outputs = Vec::new();
        let mut probes: Vec<(SourceId, String)> = Vec::new();
        let mut reseats: Vec<String> = Vec::new();
        let mut reconnect: Vec<SourceId> = Vec::new();
        let mut periodic_count = 0u64;
        let mut crossed_count = 0u64;
        let mut probe_count = 0u64;

        for (native, state) in &mut self.markets {
            let Some(active) = state.active else {
                if now_us.saturating_sub(state.last_probe_us) >= probe_spacing {
                    let candidates: Vec<SourceId> = state
                        .books
                        .keys()
                        .copied()
                        .filter(|source| source_healthy(statuses, *source))
                        .collect();
                    if !candidates.is_empty() {
                        for source in candidates {
                            probes.push((source, native.clone()));
                        }
                        state.last_probe_us = now_us;
                    }
                }
                continue;
            };
            let Some(book) = state.books.get(&active).filter(|book| book.valid) else {
                state.active = None;
                reseats.push(native.clone());
                continue;
            };

            if now_us >= state.next_snapshot_us {
                if book.book.is_crossed() {
                    crossed_count = crossed_count.saturating_add(1);
                    state.next_snapshot_us = now_us.saturating_add(1_000_000);
                    if now_us.saturating_sub(state.last_probe_us) >= probe_spacing {
                        probes.push((active, native.clone()));
                        state.last_probe_us = now_us;
                    }
                } else {
                    let exchange_ts = book.last_exchange_ts_us.unwrap_or(now_us);
                    outputs.push(OutputMessage::Snapshot(build_snapshot(
                        &state.info,
                        &book.book,
                        now_us,
                        exchange_ts,
                    )));
                    state.emitted_any = true;
                    state.last_emit_us = Some(now_us);
                    state.emitted_exchange_ts_us = Some(exchange_ts);
                    state.next_snapshot_us = now_us.saturating_add(interval);
                    state.crossed_since_us = None;
                    periodic_count = periodic_count.saturating_add(1);
                }
            }

            if let Some(crossed_since) = state.crossed_since_us
                && now_us.saturating_sub(crossed_since) >= crossed_grace
                && now_us.saturating_sub(state.last_probe_us) >= probe_spacing
            {
                probes.push((active, native.clone()));
                state.last_probe_us = now_us;
            }

            let last_activity = book.last_received_at_us.unwrap_or(0);
            if last_activity > 0 && now_us.saturating_sub(last_activity) >= stale_after {
                // Transition Live -> Stale when freshness window exceeded
                if state.continuity == crate::model::ContinuityState::Live {
                    state.set_continuity(crate::model::ContinuityState::Stale);
                }
                if now_us.saturating_sub(state.last_probe_us) >= probe_spacing {
                    if state.probes < max_probes {
                        probes.push((active, native.clone()));
                        state.probes = state.probes.saturating_add(1);
                        state.last_probe_us = now_us;
                        probe_count = probe_count.saturating_add(1);
                    } else {
                        // Max probes exceeded: Stale -> Resyncing
                        state.set_continuity(crate::model::ContinuityState::Resyncing);
                        state.start_incident(
                            DiscontinuityReason::StaleProbeEscalation,
                            now_us,
                            false,
                        );
                        self.stats.discontinuity_incidents =
                            self.stats.discontinuity_incidents.saturating_add(1);
                        self.stats.unquantifiable_gaps =
                            self.stats.unquantifiable_gaps.saturating_add(1);
                        state.probes = 0;
                        state.last_probe_us = now_us;
                        reconnect.push(active);
                        reseats.push(native.clone());
                        probe_count = probe_count.saturating_add(1);
                    }
                }
            } else if last_activity > state.last_probe_us {
                state.probes = 0;
            }
        }

        for (source, native) in probes {
            self.queue_snapshot(source, &native);
        }
        self.stats.stale_probes = self.stats.stale_probes.saturating_add(probe_count);
        self.stats.periodic_snapshots =
            self.stats.periodic_snapshots.saturating_add(periodic_count);
        self.stats.crossed_snapshots_skipped = self
            .stats
            .crossed_snapshots_skipped
            .saturating_add(crossed_count);
        self.stats.stale_failovers = self
            .stats
            .stale_failovers
            .saturating_add(reseats.len() as u64);
        for source in reconnect {
            self.queue_reconnect(source, "stale source");
        }
        for native in reseats {
            outputs.extend(self.reseat(&native, now_us));
        }
        self.stats.current_symbols = self.markets.len() as u64;
        // Update continuity state breakdown
        let mut resyncing = 0u64;
        let mut stale = 0u64;
        for state in self.markets.values() {
            match state.continuity {
                crate::model::ContinuityState::Resyncing => resyncing = resyncing.saturating_add(1),
                crate::model::ContinuityState::Stale => stale = stale.saturating_add(1),
                _ => {}
            }
        }
        self.stats.resyncing_symbols = resyncing;
        self.stats.stale_symbols = stale;
        outputs
    }

    /// Emit one final snapshot for every live active book and suppress further
    /// failover activity.
    pub fn finalize(&mut self, now_us: i64) -> Vec<OutputMessage> {
        self.shutting_down = true;
        let mut outputs = Vec::new();
        let mut count = 0u64;
        for state in self.markets.values_mut() {
            let Some(book) = state.active_book() else {
                continue;
            };
            let exchange_ts = book.last_exchange_ts_us.unwrap_or(now_us);
            outputs.push(OutputMessage::Snapshot(build_snapshot(
                &state.info,
                &book.book,
                now_us,
                exchange_ts,
            )));
            state.last_emit_us = Some(now_us);
            count = count.saturating_add(1);
        }
        self.stats.final_snapshots = self.stats.final_snapshots.saturating_add(count);
        outputs
    }

    /// Drain queued subscription, snapshot, and reconnect controls.
    pub fn drain_controls(&mut self) -> Vec<(SourceId, Control)> {
        let mut controls = Vec::new();
        let dirty: Vec<usize> = self.dirty_shards.iter().copied().collect();
        for shard_index in dirty {
            let Some(shard) = self.shards.get(shard_index) else {
                continue;
            };
            let symbols = shard.symbols.clone();
            for source in &shard.sources {
                controls.push((*source, Control::SetSubscriptions(symbols.clone())));
            }
        }
        self.dirty_shards.clear();

        let requests: Vec<(SourceId, BTreeSet<String>)> =
            std::mem::take(&mut self.snapshot_requests)
                .into_iter()
                .collect();
        for (source, symbols) in requests {
            controls.push((
                source,
                Control::RequestSnapshot(symbols.into_iter().collect()),
            ));
        }
        for (source, reason) in std::mem::take(&mut self.reconnect_requests) {
            controls.push((source, Control::Reconnect(reason)));
        }
        controls
    }

    fn queue_snapshot(&mut self, source: SourceId, native: &str) {
        if let Some(state) = self.markets.get_mut(native) {
            state.recovery_sources.insert(source);
        }
        self.snapshot_requests
            .entry(source)
            .or_default()
            .insert(native.to_owned());
    }

    fn queue_reconnect(&mut self, source: SourceId, reason: &'static str) {
        if !self
            .reconnect_requests
            .iter()
            .any(|(queued, _)| *queued == source)
        {
            self.reconnect_requests.push((source, reason));
        }
    }

    /// Choose a replacement source with a book at least as new as the last
    /// emitted state. If no replacement is ready, request fresh snapshots from
    /// every healthy replica and wait.
    fn reseat(&mut self, native: &str, now_us: i64) -> Vec<OutputMessage> {
        let statuses = &self.statuses;
        let Some(state) = self.markets.get_mut(native) else {
            return Vec::new();
        };
        let previous_active = state.active;
        let was_emitted = state.emitted_any;
        let backup = best_backup(state, statuses, self.config.exchange_has_clock);
        let Some(backup) = backup else {
            state.active = None;
            let candidates: Vec<SourceId> = state
                .books
                .keys()
                .copied()
                .filter(|source| source_healthy(statuses, *source))
                .collect();
            let mut reconnects = Vec::new();
            for source in &candidates {
                let Some(book) = state.books.get(source) else {
                    continue;
                };
                let observed_behind = match (book.last_observed_clock, state.emitted_clock) {
                    (Some(observed), Some(emitted)) => observed < emitted,
                    _ => false,
                };
                if book.valid || observed_behind {
                    // A still-valid book that is not a valid backup is either
                    // behind the lost source or crossed. A fresh connection is
                    // the only way to prove its clock belongs to the current
                    // epoch, especially after Kalshi maintenance.
                    reconnects.push(*source);
                } else {
                    state.recovery_sources.insert(*source);
                }
            }
            for source in candidates {
                if reconnects.contains(&source) {
                    self.queue_reconnect(source, "stale recovery candidate");
                } else {
                    self.snapshot_requests
                        .entry(source)
                        .or_default()
                        .insert(native.to_owned());
                }
            }
            return Vec::new();
        };

        let exchange_ts = state.books[&backup].last_exchange_ts_us.unwrap_or(now_us);
        let message = build_snapshot(&state.info, &state.books[&backup].book, now_us, exchange_ts);
        state.active = Some(backup);
        state.emitted_any = true;
        state.emitted_clock = state.books[&backup].last_clock.or(state.emitted_clock);
        state.recovery_sources.clear();
        state.emitted_exchange_ts_us = Some(exchange_ts);
        state.last_emit_us = Some(now_us);
        state.next_snapshot_us = now_us.saturating_add(self.config.snapshot_interval_us);
        state.probes = 0;
        state.last_probe_us = now_us;
        state.crossed_since_us = None;
        // Failover/recovery: resolve any open incident
        if was_emitted || previous_active.is_some() {
            state.resolve_incident(now_us);
        }
        if was_emitted || previous_active.is_some() {
            self.stats.source_switches = self.stats.source_switches.saturating_add(1);
            self.stats.failover_snapshots = self.stats.failover_snapshots.saturating_add(1);
        } else {
            self.stats.exchange_snapshots = self.stats.exchange_snapshots.saturating_add(1);
        }
        vec![OutputMessage::Snapshot(message)]
    }
}

fn clock_regressed(emitted: Option<i64>, incoming: Option<i64>) -> bool {
    matches!((emitted, incoming), (Some(emitted), Some(incoming)) if incoming < emitted)
}

fn min_tick(ranges: &[crate::model::PriceRange]) -> Option<i64> {
    ranges.iter().map(|range| range.step_us).min()
}

fn source_healthy(statuses: &BTreeMap<SourceId, SourceStatus>, source: SourceId) -> bool {
    statuses.get(&source).is_none_or(|status| status.connected)
}

fn best_backup(
    state: &MarketState,
    statuses: &BTreeMap<SourceId, SourceStatus>,
    exchange_has_clock: bool,
) -> Option<SourceId> {
    let emitted_clock = state.emitted_clock;
    state
        .books
        .iter()
        .filter(|(source, book)| {
            book.valid
                && !book.book.is_crossed()
                && Some(**source) != state.active
                && source_healthy(statuses, **source)
                && match emitted_clock {
                    Some(clock) => book
                        .last_clock
                        .is_some_and(|book_clock| book_clock >= clock),
                    None => {
                        // For exchanges without clock (Polymarket), only accept a backup
                        // if a fresh snapshot was explicitly requested (in recovery_sources).
                        // This prevents promoting a stale shadow book that has no
                        // sequence/clock to prove its freshness.
                        exchange_has_clock || state.recovery_sources.contains(*source)
                    }
                }
        })
        .max_by_key(|(source, book)| {
            let clock = book.last_clock.unwrap_or(i64::MIN);
            let latency = statuses
                .get(*source)
                .and_then(|status| status.latency_ewma_us)
                .unwrap_or(i64::MAX / 2);
            (
                clock,
                std::cmp::Reverse(latency),
                std::cmp::Reverse(**source),
            )
        })
        .map(|(source, _)| *source)
}

fn first_snapshot_deadline(now_us: i64, interval_us: i64, symbol: &str) -> i64 {
    // Spread the first periodic snapshots over the second half of the interval
    // while guaranteeing that no symbol waits longer than one full interval.
    let hash = stable_hash(symbol);
    let fraction = (hash % 5_000) as i64; // 0..4999 => 0..49.99%
    let stagger = interval_us.saturating_mul(fraction) / 10_000;
    now_us
        .saturating_add(interval_us.saturating_sub(stagger))
        .max(now_us.saturating_add(1_000_000))
}

fn stable_hash(text: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn build_snapshot(
    info: &MarketInfo,
    book: &OrderBook,
    received_at_us: i64,
    exchange_ts_us: i64,
) -> SnapshotDomain {
    let received_at = timestamp(received_at_us);
    let exchange_timestamp = timestamp_or(exchange_ts_us, received_at);
    let levels = book.to_snapshot();
    SnapshotDomain {
        received_at,
        exchange_timestamp,
        bids: levels
            .bids
            .into_iter()
            .map(|level| SnapshotBidsEntryDomain {
                price_level: level.price,
                quantity: level.quantity,
            })
            .collect(),
        asks: levels
            .asks
            .into_iter()
            .map(|level| SnapshotAsksEntryDomain {
                price_level: level.price,
                quantity: level.quantity,
            })
            .collect(),
        symbol: info.symbol.clone(),
    }
}

fn build_empty_snapshot(
    info: &MarketInfo,
    received_at_us: i64,
    exchange_ts_us: i64,
) -> SnapshotDomain {
    let received_at = timestamp(received_at_us);
    SnapshotDomain {
        received_at,
        exchange_timestamp: timestamp_or(exchange_ts_us, received_at),
        bids: Vec::new(),
        asks: Vec::new(),
        symbol: info.symbol.clone(),
    }
}

fn build_delta(
    symbol: String,
    side: Side,
    price: i64,
    quantity: i64,
    received_at_us: i64,
    exchange_ts_us: i64,
) -> PriceLevelUpdateDomain {
    debug_assert!((0..=SCALE).contains(&price));
    let backtest_timestamp = timestamp(received_at_us);
    PriceLevelUpdateDomain {
        backtest_timestamp,
        exchange_timestamp: timestamp_or(exchange_ts_us, backtest_timestamp),
        price_level: price,
        quantity,
        side,
        symbol,
    }
}

/// Convert a validated timestamp for SBE serialization.
///
/// Timestamps must be range-validated at adapter or replay boundaries before
/// they reach the core. The epoch fallback exists only so a conversion failure
/// can never inject wall-clock time into deterministic (replayed) output; it
/// is unreachable for valid inputs.
fn timestamp(timestamp_us: i64) -> DateTime<Utc> {
    utc_from_us(timestamp_us).unwrap_or(DateTime::UNIX_EPOCH)
}

fn timestamp_or(timestamp_us: i64, fallback: DateTime<Utc>) -> DateTime<Utc> {
    utc_from_us(timestamp_us).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Level, MarketInfo};

    const NOW: i64 = 1_700_000_000_000_000;
    const SOURCE_A: SourceId = SourceId::new(0, 0);
    const SOURCE_B: SourceId = SourceId::new(0, 1);

    fn settings() -> Settings {
        let mut settings = Settings::load(None).unwrap();
        settings.collector.shards = 1;
        settings.collector.replicas = 2;
        settings.collector.snapshot_interval_secs = 60;
        settings.collector.stale_probe_after_secs = 120;
        settings.collector.probe_interval_secs = 30;
        settings.collector.max_probes = 3;
        settings
    }

    fn core() -> CollectorCore {
        CollectorCore::new(CoreConfig::from_settings(&settings(), 1).unwrap()).unwrap()
    }

    fn market(native: &str) -> MarketInfo {
        MarketInfo {
            exchange: "test".to_owned(),
            native_symbol: native.to_owned(),
            symbol: native.to_owned(),
            title: Some(native.to_owned()),
            outcome: None,
            exchange_market_id: None,
            close_time_us: None,
            tick_size_us: Some(10_000),
            price_ranges: Vec::new(),
            price_level_structure: None,
        }
    }

    fn book(bid: i64, ask: i64) -> BookSnapshot {
        BookSnapshot {
            bids: vec![Level {
                price: bid,
                quantity: 10_000_000,
            }],
            asks: vec![Level {
                price: ask,
                quantity: 5_000_000,
            }],
        }
    }

    fn snapshot_event(native: &str, bid: i64, ask: i64, clock: i64) -> ExchangeEvent {
        snapshot_event_at(native, bid, ask, clock, NOW)
    }

    fn snapshot_event_at(
        native: &str,
        bid: i64,
        ask: i64,
        clock: i64,
        received_at_us: i64,
    ) -> ExchangeEvent {
        ExchangeEvent::Snapshot {
            native_symbol: native.to_owned(),
            book: book(bid, ask),
            meta: EventMeta {
                received_at_us,
                exchange_ts_us: Some(received_at_us - 1_000),
                seq: Some(1),
                clock: Some(clock),
            },
        }
    }

    fn received_at(message: &OutputMessage) -> i64 {
        match message {
            OutputMessage::Snapshot(snapshot) => snapshot.received_at.timestamp_micros(),
            OutputMessage::PriceLevelUpdate(update) => update.backtest_timestamp.timestamp_micros(),
        }
    }

    fn delta_event_at(
        native: &str,
        side: Side,
        price: i64,
        quantity: i64,
        clock: i64,
        received_at_us: i64,
    ) -> ExchangeEvent {
        ExchangeEvent::LevelChange {
            native_symbol: native.to_owned(),
            side,
            price,
            change: QuantityChange::Absolute(quantity),
            meta: EventMeta {
                received_at_us,
                exchange_ts_us: Some(received_at_us - 1_000),
                seq: Some(2),
                clock: Some(clock),
            },
        }
    }

    #[test]
    fn discovery_adds_and_removes_with_controls() {
        let mut core = core();
        let outputs = core
            .apply_discovery(NOW, vec![market("A"), market("B")])
            .unwrap();
        assert!(outputs.is_empty());
        assert_eq!(core.symbol_count(), 2);
        assert_eq!(core.stats().added_symbols, 2);
        let controls = core.drain_controls();
        assert_eq!(controls.len(), 2);
        assert!(matches!(
            &controls[0].1,
            Control::SetSubscriptions(symbols) if symbols.len() == 2
        ));

        let outputs = core.apply_discovery(NOW + 1, vec![market("A")]).unwrap();
        assert_eq!(core.symbol_count(), 1);
        assert!(outputs.is_empty());
        let controls = core.drain_controls();
        assert!(controls.iter().any(|(_, control)| matches!(
            control,
            Control::SetSubscriptions(symbols) if symbols.len() == 1
        )));
    }

    #[test]
    fn coverage_separates_sent_subscriptions_from_anchored_books() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A"), market("B")])
            .unwrap();
        core.drain_controls();
        for source in [SOURCE_A, SOURCE_B] {
            let mut status = SourceStatus::new(source);
            status.connected = true;
            status.subscriptions = 2;
            core.handle_status(status, NOW);
        }

        let coverage = core.coverage(NOW, 1_000_000);
        assert_eq!(coverage.expected_symbols, 2);
        assert_eq!(coverage.anchored_symbols, 0);
        assert_eq!(coverage.expected_subscriptions, 4);
        assert_eq!(coverage.subscribed_symbols, 4);
        assert_eq!(coverage.shards_with_connected_source, 1);
        assert_eq!(coverage.sources_connected, 2);
        assert_eq!(coverage.sources_total, 2);
        assert_eq!(coverage.oldest_anchor_us, None);

        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        assert_eq!(outputs.len(), 1);
        let coverage = core.coverage(NOW, 1_000_000);
        assert_eq!(coverage.anchored_symbols, 1);
        assert_eq!(coverage.stale_symbols, 0);
        assert_eq!(coverage.oldest_anchor_us, Some(NOW));

        // An anchored book that has not received a valid update within the
        // freshness window is counted as stale, not silently healthy.
        let coverage = core.coverage(NOW + 2_000_000, 1_000_000);
        assert_eq!(coverage.anchored_symbols, 1);
        assert_eq!(coverage.stale_symbols, 1);
    }

    #[test]
    fn snapshot_anchors_delta_application_in_per_symbol_order() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected snapshot");
        };
        assert_eq!(snapshot.symbol, "A");
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW);
        assert_eq!(snapshot.bids[0].price_level, 400_000);
        assert_eq!(snapshot.asks[0].price_level, 600_000);

        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::LevelChange {
                native_symbol: "A".to_owned(),
                side: Side::Buy,
                price: 400_000,
                change: QuantityChange::Delta(2_000_000),
                meta: EventMeta {
                    received_at_us: NOW + 1,
                    exchange_ts_us: Some(NOW),
                    seq: Some(2),
                    clock: Some(11),
                },
            },
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
            panic!("expected delta");
        };
        assert_eq!(update.quantity, 12_000_000);
        assert_eq!(update.price_level, 400_000);
        // The writer must not clamp or rewrite the receive timestamp.
        assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 1);

        // A later delta applies to the state produced by the snapshot plus the
        // first delta, in processing order, without re-emitting a snapshot.
        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::LevelChange {
                native_symbol: "A".to_owned(),
                side: Side::Buy,
                price: 400_000,
                change: QuantityChange::Absolute(9_000_000),
                meta: EventMeta {
                    received_at_us: NOW + 2,
                    exchange_ts_us: Some(NOW + 1),
                    seq: Some(3),
                    clock: Some(12),
                },
            },
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
            panic!("expected delta");
        };
        assert_eq!(update.quantity, 9_000_000);
        assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 2);
        assert_eq!(core.stats().deltas_emitted, 2);
        assert_eq!(core.stats().exchange_snapshots, 1);
    }

    #[test]
    fn failover_snapshot_anchors_resumed_deltas_and_preserves_receive_times() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        assert_eq!(
            received_at(&outputs[0]),
            NOW,
            "initial snapshot must keep the frame receive time"
        );

        // The shadow applies its own updates without emitting. Its book is
        // current when the active source is lost, and its last update
        // (NOW + 200) is newer than the active source's last frame (NOW + 50).
        assert!(
            core.handle_event(
                SOURCE_B,
                snapshot_event_at("A", 405_000, 595_000, 11, NOW + 100)
            )
            .is_empty()
        );
        assert!(
            core.handle_event(
                SOURCE_B,
                delta_event_at("A", Side::Buy, 405_000, 11_000_000, 12, NOW + 200)
            )
            .is_empty()
        );

        assert!(
            core.handle_status(
                SourceStatus {
                    source: SOURCE_A,
                    connected: true,
                    ..SourceStatus::new(SOURCE_A)
                },
                NOW + 250,
            )
            .is_empty()
        );
        let outputs = core.handle_status(
            SourceStatus {
                source: SOURCE_A,
                connected: false,
                last_frame_us: Some(NOW + 50),
                ..SourceStatus::new(SOURCE_A)
            },
            NOW + 300,
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected failover snapshot");
        };
        // The generated snapshot carries the status-processing time, never
        // the lost source's older last frame: its content includes the
        // shadow's update from NOW + 200.
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 300);
        assert!(snapshot.received_at.timestamp_micros() >= NOW + 200);
        // The exchange timestamp is the shadow's own last exchange time, kept
        // separately from the generated local timestamp.
        assert_eq!(
            snapshot.exchange_timestamp.timestamp_micros(),
            NOW + 200 - 1_000
        );
        assert_eq!(snapshot.bids[0].price_level, 405_000);
        assert_eq!(snapshot.bids[0].quantity, 11_000_000);

        let outputs = core.handle_event(
            SOURCE_B,
            delta_event_at("A", Side::Sell, 595_000, 4_000_000, 13, NOW + 400),
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::PriceLevelUpdate(update) = &outputs[0] else {
            panic!("expected resumed delta");
        };
        assert_eq!(update.backtest_timestamp.timestamp_micros(), NOW + 400);
        assert_eq!(update.quantity, 4_000_000);
    }

    #[test]
    fn generated_snapshots_follow_the_supplied_wall_clock_across_a_step() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));

        // The first periodic snapshot is stamped with the wall-clock time the
        // caller supplies.
        let outputs = core.tick(NOW + 61_000_000);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected periodic snapshot");
        };
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 61_000_000);

        // A backward wall-clock step must not emit early: the stored deadline
        // is still ahead of the stepped time.
        assert!(core.tick(NOW + 30_000_000).is_empty());

        // Once wall time passes the deadline the snapshot fires with the
        // supplied time; nothing is clamped to hide the step.
        let outputs = core.tick(NOW + 122_000_000);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected periodic snapshot");
        };
        assert_eq!(snapshot.received_at.timestamp_micros(), NOW + 122_000_000);
        assert_eq!(core.stats().periodic_snapshots, 2);
    }

    #[test]
    fn cross_symbol_output_order_is_processing_order_and_not_globally_sorted() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A"), market("B")])
            .unwrap();
        core.drain_controls();

        // Source A processes B's older frame after A's newer frame. The core
        // preserves processing order and each frame's actual receive time;
        // global chronological order is not a contract.
        let first = core.handle_event(
            SOURCE_A,
            snapshot_event_at("A", 400_000, 600_000, 10, NOW + 20),
        );
        let second = core.handle_event(
            SOURCE_A,
            snapshot_event_at("B", 410_000, 590_000, 11, NOW + 10),
        );
        let first_at = received_at(&first[0]);
        let second_at = received_at(&second[0]);
        assert_eq!((first_at, second_at), (NOW + 20, NOW + 10));
        assert!(
            first_at > second_at,
            "the test must demonstrate a cross-symbol timestamp regression"
        );
    }

    #[test]
    fn shadow_replica_takes_over_after_source_loss() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 12));
        assert!(
            core.handle_status(
                SourceStatus {
                    source: SOURCE_A,
                    connected: true,
                    ..SourceStatus::new(SOURCE_A)
                },
                NOW,
            )
            .is_empty()
        );

        let outputs = core.handle_status(
            SourceStatus {
                source: SOURCE_A,
                connected: false,
                ..SourceStatus::new(SOURCE_A)
            },
            NOW + 1,
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected failover snapshot");
        };
        assert_eq!(snapshot.bids[0].price_level, 410_000);
        assert_eq!(core.stats().source_switches, 1);
    }

    #[test]
    fn shadow_snapshot_does_not_reset_active_probe_state() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));

        let mut shadow = snapshot_event("A", 405_000, 595_000, 11);
        if let ExchangeEvent::Snapshot { meta, .. } = &mut shadow {
            meta.received_at_us = NOW + 100;
        }
        core.handle_event(SOURCE_B, shadow);

        let state = core.markets.get("A").unwrap();
        assert_eq!(state.probes, 0);
        assert_eq!(state.last_probe_us, NOW);
    }

    #[test]
    fn removal_emits_empty_snapshot() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        let outputs = core.apply_discovery(NOW + 10, Vec::new()).unwrap();
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected removal snapshot");
        };
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert_eq!(core.stats().empty_snapshots, 1);
    }

    #[test]
    fn periodic_snapshot_is_spread_and_emitted() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        let outputs = core.tick(NOW + 61_000_000);
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected periodic snapshot");
        };
        assert_eq!(snapshot.symbol, "A");
        assert_eq!(core.stats().periodic_snapshots, 1);
    }

    #[test]
    fn stale_shadow_snapshot_does_not_emit_after_source_loss() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 9));

        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 2,
            },
        );
        assert!(outputs.is_empty());

        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 9));
        assert!(outputs.is_empty());
        assert!(core.markets["A"].active.is_none());
    }

    #[test]
    fn maintenance_clock_regression_starts_a_new_epoch() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 100));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 110));

        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::LevelChange {
                native_symbol: "A".to_owned(),
                side: Side::Buy,
                price: 400_000,
                change: QuantityChange::Delta(1_000_000),
                meta: EventMeta {
                    received_at_us: NOW + 1,
                    exchange_ts_us: Some(NOW),
                    seq: Some(2),
                    clock: Some(50),
                },
            },
        );
        assert_eq!(outputs.len(), 1);
        assert_eq!(core.stats().clock_resets, 1);
        assert_eq!(core.markets["A"].emitted_clock, Some(50));
        assert!(!core.markets["A"].books[&SOURCE_B].valid);

        // A lower-clock shadow snapshot is stale in the new epoch and must not
        // replace the active book.
        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 40));
        assert!(outputs.is_empty());
        assert!(!core.markets["A"].books[&SOURCE_B].valid);

        // Once the shadow catches up to the new epoch it can be a backup again.
        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 60));
        assert!(outputs.is_empty());
        assert!(core.markets["A"].books[&SOURCE_B].valid);
    }

    #[test]
    fn requested_recovery_snapshot_after_reconnect_accepts_new_lower_epoch() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 100));
        // B is stale and its book is rejected, leaving an observed clock behind.
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 90));
        // A fresh connection clears that cross-epoch history.
        core.handle_event(
            SOURCE_B,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::Connected,
                received_at_us: NOW + 1,
            },
        );
        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 2,
            },
        );
        assert!(outputs.is_empty());

        let outputs = core.handle_event(SOURCE_B, snapshot_event("A", 410_000, 590_000, 50));
        assert_eq!(outputs.len(), 1);
        assert_eq!(core.stats().clock_resets, 1);
        assert_eq!(core.markets["A"].active, Some(SOURCE_B));
        assert_eq!(core.markets["A"].emitted_clock, Some(50));
    }

    #[test]
    fn lifecycle_price_structure_updates_market_metadata() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(
            SOURCE_A,
            ExchangeEvent::Lifecycle {
                native_symbol: "A".to_owned(),
                kind: LifecycleKind::PriceStructureUpdated,
                close_time_us: None,
                title: None,
                tick_size_us: Some(1_000),
                price_ranges: Some(vec![
                    crate::model::PriceRange {
                        start_us: 0,
                        end_us: 100_000,
                        step_us: 1_000,
                    },
                    crate::model::PriceRange {
                        start_us: 100_000,
                        end_us: 900_000,
                        step_us: 10_000,
                    },
                ]),
                price_level_structure: Some("tapered_deci_cent".to_owned()),
                meta: EventMeta {
                    received_at_us: NOW,
                    exchange_ts_us: None,
                    seq: Some(2),
                    clock: None,
                },
            },
        );
        let info = &core.market_infos()[0];
        assert_eq!(info.tick_size_us, Some(1_000));
        assert_eq!(info.price_ranges.len(), 2);
        assert_eq!(
            info.price_level_structure.as_deref(),
            Some("tapered_deci_cent")
        );
    }

    #[test]
    fn invalid_discovery_is_rejected_without_removing_current_symbols() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        let duplicate = core
            .apply_discovery(NOW + 1, vec![market("A"), market("A")])
            .unwrap_err();
        assert!(duplicate.to_string().contains("duplicate"));
        assert_eq!(core.symbol_count(), 1);

        let mut invalid = market("B");
        invalid.symbol.clear();
        assert!(
            core.apply_discovery(NOW + 2, vec![market("A"), invalid])
                .is_err()
        );
        assert_eq!(core.symbol_count(), 1);
        assert!(core.markets.contains_key("A"));
    }

    #[test]
    fn duplicate_output_symbols_are_rejected() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        let mut impostor = market("B");
        impostor.symbol = "A".to_owned();
        let error = core
            .apply_discovery(NOW + 1, vec![market("A"), impostor])
            .unwrap_err();
        assert!(error.to_string().contains("duplicate output symbol"));
        assert_eq!(core.symbol_count(), 1);
    }

    #[test]
    fn rediscovery_with_changed_metadata_keeps_symbol_stable() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));

        let mut renamed = market("A");
        renamed.title = Some("renamed".to_owned());
        // The output symbol must stay exactly the same.
        let outputs = core.apply_discovery(NOW + 1, vec![renamed]).unwrap();
        assert!(outputs.is_empty());
        assert_eq!(core.markets["A"].info.title.as_deref(), Some("renamed"));
        assert_eq!(core.markets["A"].info.symbol, "A");
        assert!(core.markets["A"].emitted_any);
    }

    #[test]
    fn identity_change_retires_old_symbol_and_reseeds_state() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));

        // The same native symbol now maps to a new output symbol.
        let mut migrated = market("A");
        migrated.symbol = "A-v2".to_owned();
        let outputs = core.apply_discovery(NOW + 1, vec![migrated]).unwrap();
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected retirement snapshot");
        };
        assert_eq!(snapshot.symbol, "A");
        assert!(snapshot.bids.is_empty());
        assert!(snapshot.asks.is_empty());
        assert_eq!(core.stats().empty_snapshots, 1);

        // The market is reseeded under the new identity with no emitted state.
        assert_eq!(core.markets["A"].info.symbol, "A-v2");
        assert!(!core.markets["A"].emitted_any);
        assert!(core.markets["A"].active.is_none());

        // A fresh book must be published under the new symbol before any
        // delta can be emitted for it.
        let outputs = core.handle_event(SOURCE_A, snapshot_event("A", 410_000, 590_000, 11));
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected fresh snapshot under the new symbol");
        };
        assert_eq!(snapshot.symbol, "A-v2");
    }

    #[test]
    fn sequence_reset_fails_over_to_valid_shadow() {
        let mut core = core();
        core.apply_discovery(NOW, vec![market("A")]).unwrap();
        core.drain_controls();
        core.handle_event(SOURCE_A, snapshot_event("A", 400_000, 600_000, 10));
        core.handle_event(SOURCE_B, snapshot_event("A", 405_000, 595_000, 11));
        let outputs = core.handle_event(
            SOURCE_A,
            ExchangeEvent::StreamReset {
                native_symbols: vec!["A".to_owned()],
                reason: ResetReason::SequenceGap,
                received_at_us: NOW + 2,
            },
        );
        assert_eq!(outputs.len(), 1);
        let OutputMessage::Snapshot(snapshot) = &outputs[0] else {
            panic!("expected failover snapshot");
        };
        assert_eq!(snapshot.bids[0].price_level, 405_000);
        assert_eq!(core.stats().sequence_resets, 1);
    }
}

//! Normalized market-data domain types shared by exchanges, the collector core,
//! replay, and inspection.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Side;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// Stable identifier of one websocket source (a shard replica).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct SourceId {
    pub shard: u16,
    pub replica: u16,
}

impl SourceId {
    #[must_use]
    pub const fn new(shard: u16, replica: u16) -> Self {
        Self { shard, replica }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s{}r{}", self.shard, self.replica)
    }
}

/// One exchange-defined band of valid prices.
///
/// Kalshi exposes `price_ranges` as fixed-point dollar strings. Keeping every
/// band (rather than collapsing to one tick size) preserves tapered grids such
/// as `$0.00-$0.10` at `$0.001` and `$0.10-$0.90` at `$0.01`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceRange {
    pub start_us: i64,
    pub end_us: i64,
    pub step_us: i64,
}

impl PriceRange {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (0..=crate::fixed::SCALE).contains(&self.start_us),
            "price range start outside [0, 1]"
        );
        anyhow::ensure!(
            (0..=crate::fixed::SCALE).contains(&self.end_us),
            "price range end outside [0, 1]"
        );
        anyhow::ensure!(
            self.start_us <= self.end_us,
            "price range start exceeds end"
        );
        anyhow::ensure!(self.step_us > 0, "non-positive price range step");
        Ok(())
    }
}

/// Exchange-level metadata for one subscribable symbol.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketInfo {
    /// Exchange name (`kalshi` or `polymarket`).
    pub exchange: String,
    /// Identifier used on the exchange wire (ticker or asset id).
    pub native_symbol: String,
    /// Namespaced identifier written to SBE.
    pub symbol: String,
    pub title: Option<String>,
    pub outcome: Option<String>,
    pub exchange_market_id: Option<String>,
    pub close_time_us: Option<i64>,
    /// Finest valid tick across all bands, in millionths of a dollar.
    pub tick_size_us: Option<i64>,
    /// Complete exchange price grid when the exchange publishes one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub price_ranges: Vec<PriceRange>,
    /// Human-readable exchange grid label. Never use this for pricing logic;
    /// `price_ranges` is authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_level_structure: Option<String>,
}

impl MarketInfo {
    /// Validate fields that every adapter must supply.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.exchange.is_empty(), "empty exchange name");
        anyhow::ensure!(!self.native_symbol.is_empty(), "empty native symbol");
        anyhow::ensure!(!self.symbol.is_empty(), "empty output symbol");
        if let Some(close_time_us) = self.close_time_us {
            anyhow::ensure!(close_time_us >= 0, "negative close timestamp");
        }
        if let Some(tick_size_us) = self.tick_size_us {
            anyhow::ensure!(tick_size_us > 0, "non-positive tick size");
        }
        for range in &self.price_ranges {
            range.validate()?;
        }
        Ok(())
    }
}

/// One aggregated price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

/// A complete two-sided book state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookSnapshot {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl BookSnapshot {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }
}

/// Metadata carried by a decoded exchange action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventMeta {
    /// Local time captured immediately after the websocket frame was read.
    pub received_at_us: i64,
    /// Best exchange-provided timestamp, if the feed supplied one.
    pub exchange_ts_us: Option<i64>,
    /// Per-subscription sequence number, if the feed supplies one.
    pub seq: Option<u64>,
    /// Exchange-wide ordering clock, if the feed supplies one.
    pub clock: Option<i64>,
}

impl EventMeta {
    #[must_use]
    pub const fn new(received_at_us: i64) -> Self {
        Self {
            received_at_us,
            exchange_ts_us: None,
            seq: None,
            clock: None,
        }
    }

    #[must_use]
    pub fn exchange_ts_or_received(&self) -> i64 {
        self.exchange_ts_us.unwrap_or(self.received_at_us)
    }
}

/// How a level's quantity changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantityChange {
    /// Replace the level with this absolute quantity.
    Absolute(i64),
    /// Add this signed quantity to the current level.
    Delta(i64),
}

/// Exchange lifecycle notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleKind {
    Created,
    Activated,
    Deactivated,
    CloseDateUpdated,
    Determined,
    Settled,
    PriceStructureUpdated,
    MetadataUpdated,
    NewMarket,
    Resolved,
    Other(String),
}

impl LifecycleKind {
    /// Whether the market is no longer tradable and should be removed from the
    /// live collection until discovery/lifecycle says otherwise.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Determined | Self::Settled | Self::Resolved)
    }
}

/// Continuity state of a symbol's order book.
///
/// This tracks whether a book is seeded, live, stale, resyncing after a
/// discontinuity, or terminal. It is separate from source connectivity: a
/// connected socket does not imply a live book, and a self-consistent SBE
/// capture does not prove complete event receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityState {
    /// No accepted exchange snapshot has established the book.
    Unseeded,
    /// An accepted snapshot anchors the active source; no known gap is
    /// currently unresolved.
    Live,
    /// Freshness policy has fired; the book must not be considered eligible
    /// for downstream risk-increasing use.
    Stale,
    /// Continuity uncertain: active-source loss, reset, disconnect, malformed
    /// frame, or stale-probe escalation. Deltas cannot restore state by
    /// themselves. A fresh authoritative book is required.
    Resyncing,
    /// Venue lifecycle marks the instrument resolved/terminal. Do not
    /// silently re-enable from a late delta.
    Terminal,
}

impl ContinuityState {
    /// Whether the book may be used for downstream risk-increasing decisions.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }

    /// Whether the book is in a state that blocks risk-increasing actions.
    #[must_use]
    pub fn blocks_risk(&self) -> bool {
        matches!(
            self,
            Self::Unseeded | Self::Stale | Self::Resyncing | Self::Terminal
        )
    }
}

/// Reason for a continuity gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscontinuityReason {
    /// Active source lost (disconnect, protocol error)
    ActiveSourceLost,
    /// Sequence/clock gap detected
    SequenceGap,
    /// Malformed frame rejected
    MalformedFrame,
    /// Stale probe escalation (max probes exceeded)
    StaleProbeEscalation,
    /// Active source switched (failover)
    SourceSwitched,
    /// Stream reset
    StreamReset,
    /// Protocol error on the wire
    ProtocolError,
    /// Lifecycle terminal event
    Terminal,
}

impl fmt::Display for DiscontinuityReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ActiveSourceLost => "active_source_lost",
            Self::SequenceGap => "sequence_gap",
            Self::MalformedFrame => "malformed_frame",
            Self::StaleProbeEscalation => "stale_probe_escalation",
            Self::SourceSwitched => "source_switched",
            Self::StreamReset => "stream_reset",
            Self::ProtocolError => "protocol_error",
            Self::Terminal => "terminal",
        })
    }
}

/// Why a source's books must be considered invalid until fresh snapshots
/// arrive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetReason {
    Connected,
    SequenceGap,
    ProtocolError,
    SubscriptionRefresh,
    RequestedRefresh,
}

impl fmt::Display for ResetReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => f.write_str("connected"),
            Self::SequenceGap => f.write_str("sequence-gap"),
            Self::ProtocolError => f.write_str("protocol-error"),
            Self::SubscriptionRefresh => f.write_str("subscription-refresh"),
            Self::RequestedRefresh => f.write_str("requested-refresh"),
        }
    }
}

/// Normalized event emitted by an exchange adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeEvent {
    Snapshot {
        native_symbol: String,
        book: BookSnapshot,
        meta: EventMeta,
    },
    LevelChange {
        native_symbol: String,
        side: Side,
        price: i64,
        change: QuantityChange,
        meta: EventMeta,
    },
    Lifecycle {
        native_symbol: String,
        kind: LifecycleKind,
        close_time_us: Option<i64>,
        title: Option<String>,
        /// Updated finest tick size when the exchange publishes one.
        tick_size_us: Option<i64>,
        /// Updated full price grid when the exchange publishes one.
        price_ranges: Option<Vec<PriceRange>>,
        /// Updated human-readable grid label, never used for pricing.
        price_level_structure: Option<String>,
        meta: EventMeta,
    },
    /// Invalidate some or all source-local books and wait for snapshots.
    StreamReset {
        native_symbols: Vec<String>,
        reason: ResetReason,
        /// Local time at which the reset was observed.
        received_at_us: i64,
    },
}

/// Command sent from the collector core/coordinator to a source worker.
#[derive(Debug, Clone)]
pub enum Control {
    /// Replace this source's desired native symbol set.
    SetSubscriptions(BTreeSet<String>),
    /// Ask the source to emit fresh snapshots for these symbols.
    RequestSnapshot(Vec<String>),
    /// Drop the connection and reconnect with exponential backoff.
    Reconnect(&'static str),
    /// Close the websocket and stop the worker.
    Shutdown,
}

/// One SBE message ready for the writer.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputMessage {
    Snapshot(SnapshotDomain),
    PriceLevelUpdate(PriceLevelUpdateDomain),
}

impl OutputMessage {
    #[must_use]
    pub fn symbol(&self) -> &str {
        match self {
            Self::Snapshot(snapshot) => &snapshot.symbol,
            Self::PriceLevelUpdate(update) => &update.symbol,
        }
    }
}

/// Per-source connectivity and protocol counters surfaced by a worker.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceStatus {
    pub source: SourceId,
    pub connected: bool,
    pub reconnects: u64,
    pub messages: u64,
    pub decode_errors: u64,
    pub protocol_errors: u64,
    pub bytes_received: u64,
    pub last_frame_us: Option<i64>,
    pub last_keepalive_us: Option<i64>,
    pub latency_ewma_us: Option<i64>,
    pub subscriptions: usize,
    pub last_error: Option<String>,
}

impl SourceStatus {
    #[must_use]
    pub fn new(source: SourceId) -> Self {
        Self {
            source,
            ..Self::default()
        }
    }
}

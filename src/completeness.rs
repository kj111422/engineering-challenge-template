//! Per-symbol feed-completeness accounting for an aligned SBE capture.
//!
//! `verify` answers whether deltas reconstruct the next snapshots exactly.
//! This report answers the adjacent question a backtest actually depends on:
//! **which symbols never became anchored**, how much of the stream was
//! captured before the first snapshot for each symbol, and whether any
//! symbol's book state was ever accepted without a complete baseline.
//!
//! What this does **not** establish:
//!
//! - that the venue delivered every event (a checksum-valid capture can
//!   still be missing events the exchange dropped upstream);
//! - that observed receive buffers had no holes (reordering between sources
//!   and across sockets is visible only through sequence evidence, which is
//!   what the journal and `verify` record);
//! - causation for silent intervals. Long quiet gaps are reported as
//!   observations with their bounds, not as missing data.
//!
//! A capture is `valid` when no delta arrives before the symbol's first
//! snapshot. Unanchored deltas are itemized per symbol in
//! [`CompletenessSummary::worst_unanchored`] (bounded sample), and the full
//! per-symbol table is available with `--include-symbols`.

use std::{collections::BTreeMap, path::Path};

use anyhow::Context;
use serde::Serialize;

use crate::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};

/// Cap on per-symbol entries in [`CompletenessSummary::per_symbol`] and on
/// the unanchored sample list. Keeps the JSON bounded on universe-scale
/// captures; the full population still contributes to the aggregate counts.
const MAX_SAMPLED_SYMBOLS: usize = 1_000;
/// Cap on the unanchored sample rendered under `worst_unanchored`.
const MAX_WORST_UNANCHORED: usize = 25;
/// Silent intervals at or above this length are listed as observations.
const QUIET_INTERVAL_REPORT_US: i64 = 60_000_000;
/// Cap on reported quiet intervals so the JSON stays bounded.
const MAX_QUIET_INTERVALS: usize = 25;

/// Per-symbol completeness ledger.
#[derive(Debug, Clone, Serialize)]
pub struct PerSymbolCompleteness {
    pub symbol: String,
    /// Receive/exchange micros of the first captured event for the symbol.
    pub first_event_at_us: Option<i64>,
    /// Receive/exchange micros of the last captured event for the symbol.
    pub last_event_at_us: Option<i64>,
    /// First event observed was a price-level update rather than a snapshot.
    pub first_event_was_update: bool,
    pub updates: u64,
    pub snapshots: u64,
    /// Deltas counted before the first snapshot arrived for this symbol.
    pub deltas_before_snapshot: u64,
    /// Receive micros of the first unanchored delta (null when anchored).
    pub first_unanchored_at_us: Option<i64>,
    /// Receive micros of the last unanchored delta (null when anchored).
    pub last_unanchored_at_us: Option<i64>,
    /// Longest silence between consecutive events for this symbol, with the
    /// receive bounds of the interval. Observation only; not missing-data
    /// evidence.
    pub longest_quiet_interval_us: i64,
    /// Receive micros where the longest quiet interval starts (after the
    /// preceding event). Null when fewer than two events were captured.
    pub longest_quiet_starts_at_us: Option<i64>,
}

/// Aggregate completeness accounting for one capture.
#[derive(Debug, Clone, Serialize)]
pub struct CompletenessSummary {
    pub file: String,
    pub chunk_size: usize,
    pub messages: u64,
    pub updates: u64,
    pub snapshots: u64,
    pub symbols: usize,
    /// Symbols whose first captured event was a price-level update.
    pub symbols_first_event_update: u64,
    /// Deltas arriving before the symbol's first snapshot, across the capture.
    pub deltas_before_snapshot: u64,
    /// Longest receive-time span between the first and last unanchored delta
    /// in any single symbol.
    pub longest_unanchored_span_us: i64,
    /// Number of symbols with at least one unanchored delta.
    pub unanchored_symbols: u64,
    /// Silent intervals of at least 60 s observed, bounded sample.
    pub quiet_intervals: Vec<QuietInterval>,
    /// The most severe unanchored symbols, capped at [`MAX_WORST_UNANCHORED`].
    pub worst_unanchored: Vec<PerSymbolCompleteness>,
    /// Full per-symbol ledger, capped at [`MAX_SAMPLED_SYMBOLS`] by message
    /// count, only when `include_symbols` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_symbol: Option<Vec<PerSymbolCompleteness>>,
    /// True when `deltas_before_snapshot == 0`.
    pub valid: bool,
    pub notes: Vec<String>,
}

/// One observed silent interval between consecutive events of a symbol.
#[derive(Debug, Clone, Serialize)]
pub struct QuietInterval {
    pub symbol: String,
    pub starts_after_us: i64,
    pub lasts_us: i64,
}

struct SymbolState {
    first_event_at_us: i64,
    last_event_at_us: i64,
    last_event_us: i64,
    longest_quiet_us: i64,
    longest_quiet_starts_us: i64,
    first_event_was_update: bool,
    updates: u64,
    snapshots: u64,
    deltas_before_snapshot: u64,
    anchored: bool,
    first_unanchored_at_us: Option<i64>,
    last_unanchored_at_us: Option<i64>,
}

impl SymbolState {
    fn observe(&mut self, received_at_us: i64, is_snapshot: bool) {
        if self.first_event_at_us == 0 {
            self.first_event_at_us = received_at_us;
        }
        self.last_event_at_us = received_at_us;
        if self.last_event_us != 0 {
            let gap = received_at_us.saturating_sub(self.last_event_us);
            if gap > self.longest_quiet_us {
                self.longest_quiet_us = gap;
                self.longest_quiet_starts_us = self.last_event_us;
            }
        }
        self.last_event_us = received_at_us;
        if is_snapshot {
            self.snapshots += 1;
            self.anchored = true;
        } else {
            self.updates += 1;
            if !self.anchored {
                self.deltas_before_snapshot += 1;
                if self.first_unanchored_at_us.is_none() {
                    self.first_unanchored_at_us = Some(received_at_us);
                }
                self.last_unanchored_at_us = Some(received_at_us);
            }
        }
    }
}

impl SymbolState {
    fn into_per_symbol(self, symbol: String) -> PerSymbolCompleteness {
        PerSymbolCompleteness {
            symbol,
            first_event_at_us: (self.first_event_at_us != 0).then_some(self.first_event_at_us),
            last_event_at_us: (self.last_event_at_us != 0).then_some(self.last_event_at_us),
            first_event_was_update: self.first_event_was_update,
            updates: self.updates,
            snapshots: self.snapshots,
            deltas_before_snapshot: self.deltas_before_snapshot,
            first_unanchored_at_us: self.first_unanchored_at_us,
            last_unanchored_at_us: self.last_unanchored_at_us,
            longest_quiet_interval_us: self.longest_quiet_us,
            longest_quiet_starts_at_us: (self.longest_quiet_us > 0)
                .then_some(self.longest_quiet_starts_us),
        }
    }
}

/// Produce the completeness report for one capture.
///
/// `chunk_size` is either an integer or `auto`. `include_symbols` adds the
/// full per-symbol ledger, capped at [`MAX_SAMPLED_SYMBOLS`] entries ranked
/// by message count.
pub fn run(
    input: &Path,
    chunk_size: &str,
    include_symbols: bool,
) -> anyhow::Result<CompletenessSummary> {
    let mut reader = if chunk_size.eq_ignore_ascii_case("auto") {
        AlignedSbeReader::<OrderbookSchema>::open_auto(input)
            .with_context(|| format!("opening `{}` with auto chunk detection", input.display()))?
    } else {
        let chunk_size: usize = chunk_size
            .parse()
            .with_context(|| format!("invalid chunk size `{chunk_size}`"))?;
        AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(input, chunk_size)
            .with_context(|| format!("opening `{}`", input.display()))?
    };

    let mut messages: u64 = 0;
    let mut updates: u64 = 0;
    let mut snapshots: u64 = 0;
    let mut states: BTreeMap<String, SymbolState> = BTreeMap::new();

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot =
                    SnapshotDomain::try_from_decoder(decoder).context("decoding SBE snapshot")?;
                messages += 1;
                snapshots += 1;
                let state = states
                    .entry(snapshot.symbol.clone())
                    .or_insert_with(|| SymbolState {
                        first_event_at_us: 0,
                        last_event_at_us: 0,
                        last_event_us: 0,
                        longest_quiet_us: 0,
                        longest_quiet_starts_us: 0,
                        first_event_was_update: false,
                        updates: 0,
                        snapshots: 0,
                        deltas_before_snapshot: 0,
                        anchored: false,
                        first_unanchored_at_us: None,
                        last_unanchored_at_us: None,
                    });
                state.observe(snapshot.received_at.timestamp_micros(), true);
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)
                    .context("decoding SBE price-level update")?;
                messages += 1;
                updates += 1;
                let state = states
                    .entry(update.symbol.clone())
                    .or_insert_with(|| SymbolState {
                        first_event_at_us: 0,
                        last_event_at_us: 0,
                        last_event_us: 0,
                        longest_quiet_us: 0,
                        longest_quiet_starts_us: 0,
                        first_event_was_update: true,
                        updates: 0,
                        snapshots: 0,
                        deltas_before_snapshot: 0,
                        anchored: false,
                        first_unanchored_at_us: None,
                        last_unanchored_at_us: None,
                    });
                state.observe(update.backtest_timestamp.timestamp_micros(), false);
            }
            AnyMessage::Unknown { .. } => {
                messages += 1;
            }
        }
    }

    // Aggregate and render. Ordering by message count keeps the bounded
    // per-symbol sample honest and deterministic.
    let mut per_symbol: Vec<PerSymbolCompleteness> = states
        .into_iter()
        .map(|(symbol, state)| state.into_per_symbol(symbol))
        .collect();
    per_symbol.sort_by(|left, right| {
        (right.updates + right.snapshots)
            .cmp(&(left.updates + left.snapshots))
            .then_with(|| left.symbol.cmp(&right.symbol))
    });

    let symbols = per_symbol.len();
    let symbols_first_event_update = per_symbol
        .iter()
        .filter(|entry| entry.first_event_was_update)
        .count() as u64;
    let deltas_before_snapshot: u64 = per_symbol
        .iter()
        .map(|entry| entry.deltas_before_snapshot)
        .sum();
    let unanchored_symbols = per_symbol
        .iter()
        .filter(|entry| entry.deltas_before_snapshot > 0)
        .count() as u64;
    let longest_unanchored_span_us = per_symbol
        .iter()
        .filter_map(
            |entry| match (entry.first_unanchored_at_us, entry.last_unanchored_at_us) {
                (Some(first), Some(last)) => Some(last.saturating_sub(first)),
                _ => None,
            },
        )
        .max()
        .unwrap_or(0);

    let mut quiet_intervals: Vec<QuietInterval> = per_symbol
        .iter()
        .filter(|entry| entry.longest_quiet_interval_us >= QUIET_INTERVAL_REPORT_US)
        .filter_map(|entry| {
            entry
                .longest_quiet_starts_at_us
                .map(|starts_after_us| QuietInterval {
                    symbol: entry.symbol.clone(),
                    starts_after_us,
                    lasts_us: entry.longest_quiet_interval_us,
                })
        })
        .collect();
    quiet_intervals.sort_by_key(|interval| std::cmp::Reverse(interval.lasts_us));
    quiet_intervals.truncate(MAX_QUIET_INTERVALS);

    let mut worst_unanchored: Vec<PerSymbolCompleteness> = per_symbol
        .iter()
        .filter(|entry| entry.deltas_before_snapshot > 0)
        .cloned()
        .collect();
    worst_unanchored.sort_by(|left, right| {
        right
            .deltas_before_snapshot
            .cmp(&left.deltas_before_snapshot)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });
    worst_unanchored.truncate(MAX_WORST_UNANCHORED);

    let per_symbol_output = include_symbols.then(|| {
        per_symbol
            .into_iter()
            .take(MAX_SAMPLED_SYMBOLS)
            .collect::<Vec<_>>()
    });

    Ok(CompletenessSummary {
        file: input.display().to_string(),
        chunk_size: reader.chunk_size(),
        messages,
        updates,
        snapshots,
        symbols,
        symbols_first_event_update,
        deltas_before_snapshot,
        longest_unanchored_span_us,
        unanchored_symbols,
        quiet_intervals,
        worst_unanchored,
        per_symbol: per_symbol_output,
        valid: deltas_before_snapshot == 0,
        notes: vec![
            "valid means every delta was anchored to a prior snapshot for its symbol".to_owned(),
            "unanchored deltas are itemized in worst_unanchored (capped sample) and counted in full in deltas_before_snapshot".to_owned(),
            "quiet intervals are observations with bounds; they are not, by themselves, evidence of missing data".to_owned(),
            "captures prove what was recorded, not that the venue delivered every event; see verify for book reconstruction".to_owned(),
        ],
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{DateTime, Utc};

    use super::*;
    use crate::{
        Side,
        model::OutputMessage,
        util::dtos::challenge_orderbook::{
            PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
            SnapshotDomain,
        },
        writer::DirectWriter,
    };

    const SYMBOL_A: &str = "test:alpha";
    const SYMBOL_B: &str = "test:beta";

    fn timestamp(us: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(us).expect("timestamp")
    }

    fn snapshot(symbol: &str, us: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> OutputMessage {
        OutputMessage::Snapshot(SnapshotDomain {
            received_at: timestamp(us),
            exchange_timestamp: timestamp(us),
            bids: bids
                .iter()
                .map(|(price, quantity)| SnapshotBidsEntryDomain {
                    price_level: *price,
                    quantity: *quantity,
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(price, quantity)| SnapshotAsksEntryDomain {
                    price_level: *price,
                    quantity: *quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        })
    }

    fn update(symbol: &str, us: i64, side: Side, price: i64, quantity: i64) -> OutputMessage {
        OutputMessage::PriceLevelUpdate(PriceLevelUpdateDomain {
            backtest_timestamp: timestamp(us),
            exchange_timestamp: timestamp(us),
            price_level: price,
            quantity,
            side,
            symbol: symbol.to_owned(),
        })
    }

    fn write_capture(messages: Vec<OutputMessage>) -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("capture.sbe");
        let mut writer = DirectWriter::create(crate::writer::WriterOptions {
            path: path.to_str().expect("path").to_owned(),
            chunk_size: 4096,
            queue: 16,
            sync_on_close: false,
            overwrite: false,
            rotate_bytes: 0,
            rotate_secs: 0,
            min_free_bytes: 0,
            fsync_interval_secs: 0,
        })
        .expect("writer");
        for message in messages {
            writer.write(&message).expect("write");
        }
        writer.shutdown().expect("shutdown");
        (directory, path)
    }

    #[test]
    fn a_capture_with_every_delta_anchored_is_valid() {
        let (_directory, path) = write_capture(vec![
            snapshot(SYMBOL_A, 1, &[(400_000, 10)], &[(600_000, 20)]),
            update(SYMBOL_A, 2, Side::Buy, 400_000, 15),
            snapshot(SYMBOL_A, 3, &[(400_000, 15)], &[(600_000, 20)]),
            update(SYMBOL_A, 4, Side::Sell, 600_000, 0),
            snapshot(SYMBOL_B, 5, &[(500_000, 30)], &[(700_000, 40)]),
            update(SYMBOL_B, 6, Side::Buy, 500_000, 35),
        ]);
        let summary = run(&path, "auto", true).expect("report");
        assert!(summary.valid);
        assert_eq!(summary.messages, 6);
        assert_eq!(summary.updates, 3);
        assert_eq!(summary.snapshots, 3);
        assert_eq!(summary.symbols, 2);
        assert_eq!(summary.deltas_before_snapshot, 0);
        assert_eq!(summary.symbols_first_event_update, 0);
        assert_eq!(summary.unanchored_symbols, 0);
        let per_symbol = summary.per_symbol.expect("per-symbol ledger");
        assert_eq!(per_symbol.len(), 2);
        let beta = per_symbol
            .iter()
            .find(|entry| entry.symbol == SYMBOL_B)
            .expect("beta present");
        assert_eq!(beta.updates, 1);
        assert_eq!(beta.snapshots, 1);
        assert_eq!(beta.deltas_before_snapshot, 0);
    }

    #[test]
    fn unanchored_deltas_are_itemized_per_symbol() {
        let (_directory, path) = write_capture(vec![
            update(SYMBOL_A, 10, Side::Buy, 400_000, 15),
            update(SYMBOL_A, 20, Side::Buy, 400_000, 20),
            update(SYMBOL_A, 30, Side::Buy, 400_000, 25),
            update(SYMBOL_A, 40, Side::Sell, 600_000, 0),
            snapshot(SYMBOL_A, 50, &[(400_000, 25)], &[]),
            update(SYMBOL_A, 60, Side::Buy, 400_000, 30),
            update(SYMBOL_B, 5, Side::Buy, 500_000, 1),
        ]);
        let summary = run(&path, "auto", true).expect("report");
        assert!(!summary.valid);
        assert_eq!(summary.deltas_before_snapshot, 5);
        assert_eq!(summary.unanchored_symbols, 2);
        assert_eq!(summary.symbols_first_event_update, 2);
        assert_eq!(summary.longest_unanchored_span_us, 30);
        assert_eq!(summary.worst_unanchored.len(), 2);
        let worst = &summary.worst_unanchored[0];
        assert_eq!(worst.symbol, SYMBOL_A);
        assert_eq!(worst.deltas_before_snapshot, 4);
        assert_eq!(worst.first_unanchored_at_us, Some(10));
        assert_eq!(worst.last_unanchored_at_us, Some(40));
        let per_symbol = summary.per_symbol.expect("per-symbol ledger");
        let beta = per_symbol
            .iter()
            .find(|entry| entry.symbol == SYMBOL_B)
            .expect("beta present");
        assert_eq!(beta.deltas_before_snapshot, 1);
        assert_eq!(beta.first_unanchored_at_us, Some(5));
    }

    #[test]
    fn unanchored_counts_include_deltas_after_the_last_snapshot_when_unanchored() {
        // A symbol that never receives a snapshot is unanchored forever;
        // later snapshots for other symbols do not help it.
        let (_directory, path) = write_capture(vec![
            update(SYMBOL_A, 1, Side::Buy, 400_000, 10),
            snapshot(SYMBOL_B, 2, &[(500_000, 10)], &[]),
            update(SYMBOL_A, 3, Side::Buy, 400_000, 11),
        ]);
        let summary = run(&path, "auto", false).expect("report");
        assert_eq!(summary.deltas_before_snapshot, 2);
        assert_eq!(summary.unanchored_symbols, 1);
        assert!(!summary.valid);
        assert!(summary.per_symbol.is_none());
    }

    #[test]
    fn quiet_intervals_are_reported_as_observations_with_bounds() {
        let (_directory, path) = write_capture(vec![
            snapshot(SYMBOL_A, 1, &[(400_000, 10)], &[]),
            update(SYMBOL_A, 2, Side::Buy, 400_000, 11),
            update(SYMBOL_A, 61_000_001, Side::Buy, 400_000, 12),
            update(SYMBOL_A, 61_000_002, Side::Buy, 400_000, 13),
        ]);
        let summary = run(&path, "auto", true).expect("report");
        assert_eq!(summary.quiet_intervals.len(), 1);
        let entry = &summary.quiet_intervals[0];
        assert_eq!(entry.symbol, SYMBOL_A);
        assert_eq!(entry.lasts_us, 60_999_999);
        assert_eq!(entry.starts_after_us, 2);
        let per_symbol = summary.per_symbol.expect("per-symbol ledger");
        assert_eq!(per_symbol[0].longest_quiet_interval_us, 60_999_999);
        assert_eq!(per_symbol[0].longest_quiet_starts_at_us, Some(2));
    }

    #[test]
    fn an_empty_capture_is_complete_vacuously() {
        let (_directory, path) = write_capture(Vec::new());
        let summary = run(&path, "4096", false).expect("report");
        assert!(summary.valid);
        assert_eq!(summary.messages, 0);
        assert_eq!(summary.symbols, 0);
    }
}

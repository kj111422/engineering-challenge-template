//! Deterministic synthetic Kalshi-shaped universe for the stress laboratory.
//!
//! Two disjoint sets are generated from one seed:
//!
//! 1. **Eligible** — regular, challenge-in-scope markets. The production MVE
//!    filter accepts them; they are discovered, subscribed and anchored.
//! 2. **Ineligible decoys** — MVE records and non-active records. The same
//!    filter must reject them; they must never enter the expected universe.
//!
//! Tickers are synthetic `STRESS-{seed:04x}-{index:06}` values and are never
//! presented as real venue markets. `generated`, `eligible` and `excluded`
//! counts are reported separately; the expected universe is the eligible set.

use serde_json::{Value, json};

use super::rng::SplitMix64;

/// Generator version, recorded in every manifest. Bump on any semantic change.
pub const GENERATOR_VERSION: u32 = 1;

/// One generated market record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticMarket {
    pub index: usize,
    pub ticker: String,
    pub event_ticker: String,
    pub title: String,
    pub close_time: String,
    pub eligible: bool,
    /// `mve` for MVE decoys; empty for regular eligible markets.
    pub market_type: &'static str,
    /// `active` or `closed` (closed records are non-eligible by status).
    pub status: &'static str,
}

impl SyntheticMarket {
    #[must_use]
    pub fn is_mve(&self) -> bool {
        !self.market_type.is_empty()
    }

    /// REST discovery record for the mock's `/markets` pages.
    ///
    /// The synthetic records mirror the field names our adapter reads:
    /// `ticker`, `event_ticker`, `title`, `status`, `close_time`,
    /// `price_ranges`, `price_level_structure`. Eligible records carry no
    /// `market_type`/`mve_*` fields at all.
    #[must_use]
    pub fn rest_record(&self) -> Value {
        let mut record = json!({
            "ticker": self.ticker,
            "event_ticker": self.event_ticker,
            "title": self.title,
            "status": self.status,
            "close_time": self.close_time,
            "price_ranges": [{"start": "0", "end": "1", "step": "0.01"}],
            "price_level_structure": "linear_cent",
        });
        if self.is_mve() {
            record["market_type"] = json!(self.market_type);
        }
        record
    }
}

/// One generated universe: the eligible markets and the decoy set.
#[derive(Debug, Clone)]
pub struct SyntheticUniverse {
    /// All generated records, eligible first, then decoys.
    pub markets: Vec<SyntheticMarket>,
    pub eligible_count: usize,
    pub excluded_count: usize,
}

impl SyntheticUniverse {
    /// Deterministic generator.
    ///
    /// `symbols` is the eligible target. `decoy_fraction_ppm` controls the
    /// count of MVE decoys (parts per million of `symbols`, minimum 16);
    /// a small number of closed (non-active) records is added on top.
    #[must_use]
    pub fn generate(seed: u64, symbols: usize, decoy_fraction_ppm: u32) -> Self {
        let mut rng = SplitMix64::new(seed ^ 0x5D3C_0FFE_E5AA_5AA5);
        let mut markets = Vec::with_capacity(symbols + 64);
        let eligible = decoy_eligible(seed, symbols, decoy_fraction_ppm, &mut rng, &mut markets);
        let excluded = markets.len() - eligible;
        Self {
            markets,
            eligible_count: eligible,
            excluded_count: excluded,
        }
    }

    /// The eligible universe only (in index order).
    pub fn eligible(&self) -> impl Iterator<Item = &SyntheticMarket> + '_ {
        self.markets.iter().filter(|market| market.eligible)
    }

    /// Exact tickers of the eligible set, sorted.
    #[must_use]
    pub fn eligible_tickers_sorted(&self) -> Vec<String> {
        let mut tickers: Vec<String> = self.eligible().map(|m| m.ticker.clone()).collect();
        tickers.sort();
        tickers
    }

    /// Exact tickers of everything the MVE/status filter must reject.
    #[must_use]
    pub fn excluded_tickers_sorted(&self) -> Vec<String> {
        let mut tickers: Vec<String> = self
            .markets
            .iter()
            .filter(|market| !market.eligible)
            .map(|m| m.ticker.clone())
            .collect();
        tickers.sort();
        tickers
    }

    /// REST page for `/markets` (1-indexed `page` starting at 0).
    ///
    /// Returns `(records, next_cursor)`; the cursor is a string offset and is
    /// `None` at the end. Records are returned in a stable order so pages are
    /// deterministic and re-pagination is exercised honestly.
    #[must_use]
    pub fn rest_page(&self, page: usize, page_size: usize) -> (Vec<Value>, Option<String>) {
        let start = page.saturating_mul(page_size);
        let end = (start + page_size).min(self.markets.len());
        let records = self.markets[start..end]
            .iter()
            .map(SyntheticMarket::rest_record)
            .collect::<Vec<_>>();
        let next = if end < self.markets.len() {
            Some(end.to_string())
        } else {
            None
        };
        (records, next)
    }
}

fn decoy_eligible(
    seed: u64,
    symbols: usize,
    decoy_fraction_ppm: u32,
    rng: &mut SplitMix64,
    markets: &mut Vec<SyntheticMarket>,
) -> usize {
    let target_eligible = symbols.max(1);
    let mve_decoys = ((symbols as u64).saturating_mul(u64::from(decoy_fraction_ppm)) / 1_000_000)
        .max(16) as usize;
    let closed_decoys = (mve_decoys / 2).max(4);

    let mut eligible = 0usize;
    let mut created = 0usize;
    while eligible < target_eligible {
        let index = created;
        created += 1;
        let offset = rng.below(86_400);
        markets.push(SyntheticMarket {
            index,
            ticker: ticker(seed, index),
            event_ticker: format!("STRESS-EV-{seed:04x}-{index:06}"),
            title: format!("Synthetic stress market {index}"),
            close_time: format_close_time(index, offset),
            eligible: true,
            market_type: "",
            status: "active",
        });
        eligible += 1;
    }

    for decoy in 0..mve_decoys {
        let index = created;
        created += 1;
        markets.push(SyntheticMarket {
            index,
            ticker: ticker(seed, index),
            event_ticker: format!("STRESS-EV-{seed:04x}-{index:06}"),
            title: format!("Synthetic MVE decoy {decoy}"),
            close_time: format_close_time(index, rng.below(86_400)),
            eligible: false,
            market_type: "mve",
            status: "active",
        });
    }
    for decoy in 0..closed_decoys {
        let _ = decoy;
        let index = created;
        created += 1;
        markets.push(SyntheticMarket {
            index,
            ticker: ticker(seed, index),
            event_ticker: format!("STRESS-EV-{seed:04x}-{index:06}"),
            title: format!("Synthetic closed decoy {index}"),
            close_time: format_close_time(index, rng.below(86_400)),
            eligible: false,
            market_type: "",
            status: "closed",
        });
    }
    eligible
}

fn ticker(seed: u64, index: usize) -> String {
    format!("STRESS-{seed:04x}-{index:06}")
}

fn format_close_time(index: usize, offset_secs: u64) -> String {
    // Stagger closes deterministically within a ten-day window starting
    // 2027-01-01T00:00:00Z, so churn scenarios can be driven deterministically.
    const BASE_US: i64 = 1_798_521_600_000_000;
    let ts_us = BASE_US
        + (i64::try_from(offset_secs).unwrap_or(0) * 1_000_000)
        + i64::try_from(index % 600).unwrap_or(0) * 1_000;
    engineering_challenge::time::utc_from_us(ts_us)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|_| "2027-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_is_deterministic_for_same_seed() {
        let a = SyntheticUniverse::generate(42, 128, 20_000);
        let b = SyntheticUniverse::generate(42, 128, 20_000);
        assert_eq!(a.markets, b.markets);
        assert_eq!(a.eligible_count, b.eligible_count);
        assert_eq!(a.excluded_count, b.excluded_count);
    }

    #[test]
    fn different_seeds_produce_different_universes() {
        let a = SyntheticUniverse::generate(42, 128, 20_000);
        let b = SyntheticUniverse::generate(43, 128, 20_000);
        assert_ne!(a.markets[0].ticker, b.markets[0].ticker);
    }

    #[test]
    fn eligible_and_decoy_sets_are_disjoint_and_complete() {
        let universe = SyntheticUniverse::generate(7, 64, 100_000);
        let eligible = universe.eligible_tickers_sorted();
        let excluded = universe.excluded_tickers_sorted();
        assert_eq!(eligible.len(), 64);
        assert!(!excluded.is_empty());
        for ticker in &excluded {
            assert!(!eligible.contains(ticker));
        }
        // Every excluded market is an MVE or a non-active record.
        for market in universe.markets.iter().filter(|m| !m.eligible) {
            assert!(market.is_mve() || market.status == "closed");
        }
    }

    #[test]
    fn rest_pages_are_stable_and_paginate_fully() {
        let universe = SyntheticUniverse::generate(9, 37, 50_000);
        let total = universe.markets.len();
        let page_size = 7;
        let mut seen = Vec::new();
        let mut page = 0usize;
        loop {
            let (records, next) = universe.rest_page(page, page_size);
            seen.extend(records);
            match next {
                Some(cursor) => {
                    assert_eq!(cursor, ((page + 1) * page_size).to_string());
                    page += 1;
                }
                None => break,
            }
        }
        assert_eq!(seen.len(), total);
        for (index, record) in seen.iter().enumerate() {
            assert_eq!(record["ticker"], json!(ticker(9, index)));
        }
    }

    #[test]
    fn eligible_records_carry_no_mve_markers() {
        let universe = SyntheticUniverse::generate(11, 32, 50_000);
        for market in universe.markets.iter().filter(|m| m.eligible) {
            let record = market.rest_record();
            assert!(
                record.get("market_type").is_none(),
                "eligible record carried market_type: {record}"
            );
            assert!(record.get("mve_selected_legs").is_none());
        }
    }
}

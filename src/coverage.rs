//! Per-symbol live-coverage certification for one aligned SBE capture.
//!
//! `completeness` answers whether deltas arrived after a baseline. This report
//! answers the question a live-evidence claim depends on: **did every symbol
//! the run was supposed to collect actually get subscribed and snapshot
//! anchored?** It compares three sets and cross-checks the capture against
//! the run report's writer counter (a truncated capture cannot certify as
//! complete):
//!
//! - **expected**: the normalized symbols in the metadata sidecar written by
//!   the collector (`markets.json`);
//! - **acknowledged**: the union of each worker's last successfully applied
//!   subscription set, mapped from native identifiers through that sidecar;
//! - **anchored**: symbols with at least one snapshot in the capture.
//!
//! The certificate is `valid` only when expected, acknowledged, and anchored
//! agree for every expected symbol, with no duplicate expected entry, no
//! acknowledged symbol outside the expected set, and a non-empty expected set.
//!
//! An empty-book snapshot counts as an anchor because the SBE snapshot still
//! names the symbol and establishes the baseline. A symbol that appears only
//! in price-level updates is reported under `observed_not_anchored` and is not
//! anchored.
//!
//! Retired symbols are reported separately: a symbol that was captured before
//! discovery removed it can appear in `anchored_not_expected` without
//! invalidating the certificate, because the expected set is the final
//! metadata and the central invariant is about that set.
//!
//! ## What this does not establish
//!
//! - Venue wire acknowledgement. `acknowledged_symbols` records that the
//!   session successfully applied and transmitted a subscription request; the
//!   snapshot anchor is the independent proof that data actually arrived.
//! - That the venue delivered every event. Checksums and sequence continuity
//!   belong to `verify` and the raw-frame journal.
//! - That a periodic snapshot was generated from exchange state rather than
//!   synthesized locally. The collector only emits generated snapshots for a
//!   book that already accepted an exchange snapshot, so generated captures
//!   still imply a prior exchange anchor.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::BufReader,
    path::Path,
};

use anyhow::Context;
use serde::Serialize;

use crate::{
    report::{MarketMetadata, RunReport},
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, SnapshotDomain},
        sha256_hex,
    },
};

/// Cap on entries listed under each diagnostic. The counts stay exact; only
/// the rendered sample is bounded so universe-scale reports remain readable.
pub const MAX_REPORTED_SYMBOLS: usize = 50;

const COVERAGE_SCHEMA_VERSION: u32 = 1;

/// An exact count plus a deterministic, bounded sample of the offending
/// symbols.
#[derive(Debug, Clone, Serialize)]
pub struct BoundedList {
    pub count: usize,
    pub truncated: bool,
    pub sample: Vec<String>,
}

impl BoundedList {
    #[must_use]
    pub fn from_set(set: &BTreeSet<String>) -> Self {
        Self {
            count: set.len(),
            truncated: set.len() > MAX_REPORTED_SYMBOLS,
            sample: set.iter().take(MAX_REPORTED_SYMBOLS).cloned().collect(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Machine-readable coverage certificate for one capture.
#[derive(Debug, Clone, Serialize)]
pub struct CoverageSummary {
    pub schema_version: u32,
    pub capture: String,
    pub metadata: String,
    pub report: String,
    /// Symbols the metadata sidecar says this run selected.
    pub expected: usize,
    /// Expected symbols with a successfully applied subscription request.
    pub acknowledged: usize,
    /// Expected symbols with at least one snapshot in the capture.
    pub anchored: usize,
    /// Distinct symbols with any snapshot or price-level update.
    pub observed: usize,
    /// `true` only when every expected symbol was acknowledged and anchored
    /// and no contradictory symbol was observed.
    pub valid: bool,
    /// SHA-256 of the sorted expected symbols, newline-separated with a
    /// trailing newline. Pins the exact expected universe to the evidence.
    pub expected_set_sha256: String,
    /// SBE messages read while certifying the capture.
    pub capture_messages: u64,
    /// SBE messages the run report's writer accepted. Must equal
    /// `capture_messages`; a truncated capture cannot certify as complete.
    pub report_writer_messages: u64,
    /// `true` when the capture decodes exactly as many messages as the run
    /// report's writer accepted.
    pub capture_messages_match_report: bool,
    /// Expected symbols with no successful subscription request.
    pub missing_ack: BoundedList,
    /// Expected symbols with no snapshot in the capture.
    pub missing_anchor: BoundedList,
    /// Acknowledged expected symbols with no snapshot in the capture.
    pub ack_not_anchored: BoundedList,
    /// Acknowledged symbols absent from the metadata sidecar.
    pub ack_not_expected: BoundedList,
    /// Captured snapshots absent from the metadata sidecar. Retirements and
    /// symbols captured before a later discovery round appear here.
    pub anchored_not_expected: BoundedList,
    /// Symbols observed only in price-level updates, never in a snapshot.
    pub observed_not_anchored: BoundedList,
    /// Normalized symbols that occur more than once in the metadata sidecar.
    pub duplicate_expected: BoundedList,
    /// Acknowledged native identifiers with no metadata mapping and no exact
    /// normalized match. These cannot be attributed to an expected symbol.
    pub unknown_ack_native: BoundedList,
}

impl CoverageSummary {
    /// Return the JSON representation used by the CLI and evidence manifest.
    pub fn to_json(&self) -> anyhow::Result<serde_json::Value> {
        serde_json::to_value(self).context("serializing coverage summary")
    }
}

/// Certify one capture against its metadata sidecar and run report.
pub fn run(
    input: &Path,
    metadata_path: &Path,
    report_path: &Path,
    chunk_size: &str,
) -> anyhow::Result<CoverageSummary> {
    let metadata: MarketMetadata = read_json(metadata_path, "metadata sidecar")?;
    let report: RunReport = read_json(report_path, "run report")?;
    let (observed, capture_messages) = scan_capture(input, chunk_size)?;
    let report_writer_messages = report.writer.messages;

    let mut expected_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut native_to_symbol: BTreeMap<String, String> = BTreeMap::new();
    for market in &metadata.markets {
        *expected_counts.entry(market.symbol.clone()).or_default() += 1;
        native_to_symbol
            .entry(market.native_symbol.clone())
            .or_insert_with(|| market.symbol.clone());
    }
    Ok(certify(
        &expected_counts,
        &native_to_symbol,
        &report.acknowledged_symbols,
        &observed,
        capture_messages,
        report_writer_messages,
        input.display().to_string(),
        metadata_path.display().to_string(),
        report_path.display().to_string(),
    ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "certification reads four independent evidence sets; a context struct would only rename them"
)]
fn certify(
    expected_counts: &BTreeMap<String, usize>,
    native_to_symbol: &BTreeMap<String, String>,
    ack_native: &[String],
    observed: &ObservedSymbols,
    capture_messages: u64,
    report_writer_messages: u64,
    capture: String,
    metadata: String,
    report: String,
) -> CoverageSummary {
    let expected: BTreeSet<String> = expected_counts.keys().cloned().collect();
    let duplicate_expected: BTreeSet<String> = expected_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(symbol, _)| symbol.clone())
        .collect();

    let mut acknowledged = BTreeSet::new();
    let mut unknown_ack_native = BTreeSet::new();
    for native in ack_native {
        if let Some(symbol) = native_to_symbol.get(native) {
            acknowledged.insert(symbol.clone());
        } else if expected.contains(native) {
            // Venues whose native identifier is already the SBE symbol.
            acknowledged.insert(native.clone());
        } else {
            unknown_ack_native.insert(native.clone());
        }
    }

    let anchored = &observed.anchored;
    let observed_not_anchored: BTreeSet<String> =
        observed.all.difference(anchored).cloned().collect();

    let missing_ack: BTreeSet<String> = expected.difference(&acknowledged).cloned().collect();
    let ack_not_expected: BTreeSet<String> = acknowledged.difference(&expected).cloned().collect();
    let missing_anchor: BTreeSet<String> = expected.difference(anchored).cloned().collect();
    let ack_not_anchored: BTreeSet<String> = acknowledged
        .intersection(&expected)
        .filter(|symbol| !anchored.contains(*symbol))
        .cloned()
        .collect();
    let anchored_not_expected: BTreeSet<String> = anchored.difference(&expected).cloned().collect();

    let missing_ack = BoundedList::from_set(&missing_ack);
    let missing_anchor = BoundedList::from_set(&missing_anchor);
    let ack_not_anchored = BoundedList::from_set(&ack_not_anchored);
    let ack_not_expected = BoundedList::from_set(&ack_not_expected);
    let anchored_not_expected = BoundedList::from_set(&anchored_not_expected);
    let observed_not_anchored = BoundedList::from_set(&observed_not_anchored);
    let duplicate_expected = BoundedList::from_set(&duplicate_expected);
    let unknown_ack_native = BoundedList::from_set(&unknown_ack_native);

    // A capture whose decoded message count differs from the run report's
    // writer counter cannot establish complete per-symbol coverage: bytes
    // were lost or the report came from a different run.
    let capture_messages_match_report = capture_messages == report_writer_messages;

    let valid = !expected.is_empty()
        && missing_ack.is_empty()
        && missing_anchor.is_empty()
        && ack_not_anchored.is_empty()
        && ack_not_expected.is_empty()
        && duplicate_expected.is_empty()
        && unknown_ack_native.is_empty()
        && capture_messages_match_report;

    CoverageSummary {
        schema_version: COVERAGE_SCHEMA_VERSION,
        capture,
        metadata,
        report,
        expected: expected.len(),
        acknowledged: acknowledged.len(),
        anchored: expected.intersection(anchored).count(),
        observed: observed.all.len(),
        valid,
        expected_set_sha256: digest_expected(&expected),
        capture_messages,
        report_writer_messages,
        capture_messages_match_report,
        missing_ack,
        missing_anchor,
        ack_not_anchored,
        ack_not_expected,
        anchored_not_expected,
        observed_not_anchored,
        duplicate_expected,
        unknown_ack_native,
    }
}

struct ObservedSymbols {
    anchored: BTreeSet<String>,
    all: BTreeSet<String>,
}

fn scan_capture(input: &Path, chunk_size: &str) -> anyhow::Result<(ObservedSymbols, u64)> {
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

    let mut anchored = BTreeSet::new();
    let mut all = BTreeSet::new();
    let mut messages = 0u64;
    while let Some(message) = reader.read_msg()? {
        messages = messages.saturating_add(1);
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot =
                    SnapshotDomain::try_from_decoder(decoder).context("decoding SBE snapshot")?;
                all.insert(snapshot.symbol.clone());
                anchored.insert(snapshot.symbol);
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain::try_from_decoder(
                    decoder,
                )
                .context("decoding SBE price-level update")?;
                all.insert(update.symbol);
            }
            AnyMessage::Unknown { .. } => {}
        }
    }
    Ok((ObservedSymbols { anchored, all }, messages))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path, label: &str) -> anyhow::Result<T> {
    let file = File::open(path).with_context(|| format!("opening {label} `{}`", path.display()))?;
    serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("parsing {label} `{}`", path.display()))
}

fn digest_expected(expected: &BTreeSet<String>) -> String {
    let mut text = String::new();
    for symbol in expected {
        text.push_str(symbol);
        text.push('\n');
    }
    sha256_hex(text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn counts(values: &[&str]) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for value in values {
            *counts.entry((*value).to_owned()).or_default() += 1;
        }
        counts
    }

    fn native_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(native, symbol)| ((*native).to_owned(), (*symbol).to_owned()))
            .collect()
    }

    fn observed(anchored: &[&str], updates_only: &[&str]) -> ObservedSymbols {
        let mut all = set(anchored);
        all.extend(updates_only.iter().map(|value| (*value).to_owned()));
        ObservedSymbols {
            anchored: set(anchored),
            all,
        }
    }

    fn summary(
        expected: &BTreeMap<String, usize>,
        ack: &[&str],
        observed: &ObservedSymbols,
    ) -> CoverageSummary {
        certify(
            expected,
            &BTreeMap::new(),
            &ack.iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
            observed,
            2,
            2,
            "capture.sbe".to_owned(),
            "markets.json".to_owned(),
            "report.json".to_owned(),
        )
    }

    #[test]
    fn fully_acknowledged_and_anchored_universe_is_valid() {
        // Presence of a snapshot is the anchor; an empty book is still a
        // snapshot message and therefore still counts.
        let result = summary(
            &counts(&["A", "B"]),
            &["A", "B"],
            &observed(&["A", "B"], &[]),
        );
        assert!(result.valid);
        assert_eq!(result.expected, 2);
        assert_eq!(result.acknowledged, 2);
        assert_eq!(result.anchored, 2);
        assert_eq!(result.missing_ack.count, 0);
        assert_eq!(result.missing_anchor.count, 0);
    }

    #[test]
    fn missing_acknowledgement_is_reported() {
        let result = summary(&counts(&["A", "B"]), &["A"], &observed(&["A", "B"], &[]));
        assert!(!result.valid);
        assert_eq!(result.missing_ack.count, 1);
        assert_eq!(result.missing_ack.sample, vec!["B".to_owned()]);
        assert_eq!(result.missing_anchor.count, 0);
    }

    #[test]
    fn acknowledged_but_unanchored_symbol_is_reported() {
        let result = summary(&counts(&["A", "B"]), &["A", "B"], &observed(&["A"], &[]));
        assert!(!result.valid);
        assert_eq!(result.missing_anchor.count, 1);
        assert_eq!(result.ack_not_anchored.count, 1);
        assert_eq!(result.ack_not_anchored.sample, vec!["B".to_owned()]);
    }

    #[test]
    fn acknowledged_symbol_outside_metadata_is_reported() {
        let result = certify(
            &counts(&["A"]),
            &native_map(&[("a", "A"), ("z", "Z")]),
            &["a".to_owned(), "z".to_owned()],
            &observed(&["A", "Z"], &[]),
            2,
            2,
            "capture.sbe".to_owned(),
            "markets.json".to_owned(),
            "report.json".to_owned(),
        );
        assert!(!result.valid);
        assert_eq!(result.ack_not_expected.count, 1);
        assert_eq!(result.ack_not_expected.sample, vec!["Z".to_owned()]);
    }

    #[test]
    fn duplicate_expected_symbol_is_reported() {
        let result = summary(&counts(&["A", "A"]), &["A"], &observed(&["A"], &[]));
        assert!(!result.valid);
        assert_eq!(result.duplicate_expected.count, 1);
        assert_eq!(result.duplicate_expected.sample, vec!["A".to_owned()]);
    }

    #[test]
    fn retired_anchored_symbol_is_accounted_separately_without_failing() {
        let result = summary(&counts(&["A"]), &["A"], &observed(&["A", "B"], &[]));
        assert!(result.valid);
        assert_eq!(result.anchored_not_expected.count, 1);
        assert_eq!(result.anchored_not_expected.sample, vec!["B".to_owned()]);
    }

    #[test]
    fn unknown_native_acknowledgement_is_reported() {
        let result = certify(
            &counts(&["polymarket:condition:0"]),
            &native_map(&[("asset", "polymarket:condition:0")]),
            &["unknown-asset".to_owned()],
            &observed(&["polymarket:condition:0"], &[]),
            1,
            1,
            "capture.sbe".to_owned(),
            "markets.json".to_owned(),
            "report.json".to_owned(),
        );
        assert!(!result.valid);
        assert_eq!(result.unknown_ack_native.count, 1);
        // The expected symbol was neither acknowledged nor anchored.
        assert_eq!(result.missing_ack.count, 1);
        assert_eq!(result.missing_anchor.count, 0);
    }

    #[test]
    fn empty_expected_universe_is_not_a_certificate() {
        let result = summary(&counts(&[]), &[], &observed(&[], &[]));
        assert!(!result.valid);
    }

    #[test]
    fn update_only_symbol_is_observed_but_not_anchored() {
        let result = summary(&counts(&["A"]), &["A"], &observed(&[], &["A"]));
        assert!(!result.valid);
        assert_eq!(result.observed, 1);
        assert_eq!(result.anchored, 0);
        assert_eq!(result.observed_not_anchored.count, 1);
        assert_eq!(result.missing_anchor.count, 1);
    }

    #[test]
    fn bounded_list_reports_exact_count_and_truncates_sample() {
        let symbols: BTreeSet<String> = (0..MAX_REPORTED_SYMBOLS + 5)
            .map(|index| format!("S{index:04}"))
            .collect();
        let list = BoundedList::from_set(&symbols);
        assert_eq!(list.count, MAX_REPORTED_SYMBOLS + 5);
        assert!(list.truncated);
        assert_eq!(list.sample.len(), MAX_REPORTED_SYMBOLS);
        assert_eq!(list.sample.first().map(String::as_str), Some("S0000"));
    }

    #[test]
    fn small_list_is_not_truncated() {
        let list = BoundedList::from_set(&set(&["B", "A"]));
        assert_eq!(list.count, 2);
        assert!(!list.truncated);
        assert_eq!(list.sample, vec!["A".to_owned(), "B".to_owned()]);
    }

    #[test]
    fn truncated_capture_cannot_certify_complete_coverage() {
        let result = certify(
            &counts(&["A"]),
            &BTreeMap::new(),
            &["A".to_owned()],
            &observed(&["A"], &[]),
            1,
            2,
            "capture.sbe".to_owned(),
            "markets.json".to_owned(),
            "report.json".to_owned(),
        );
        assert!(!result.valid);
        assert!(!result.capture_messages_match_report);
        assert_eq!(result.capture_messages, 1);
        assert_eq!(result.report_writer_messages, 2);
    }

    #[test]
    fn expected_digest_is_order_independent_and_trailing_newline_terminated() {
        let a = set(&["B", "A"]);
        let b = set(&["A", "B"]);
        assert_eq!(digest_expected(&a), digest_expected(&b));
        assert_eq!(digest_expected(&set(&[])), sha256_hex(b""));
        assert_eq!(digest_expected(&set(&["A"])), sha256_hex(b"A\n"));
    }
}

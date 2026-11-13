//! Opt-in authenticated, read-only Kalshi production market-data integration test.
//!
//! Required environment:
//! - `RUN_LIVE_KALSHI_TESTS=1`
//! - `KALSHI_READ_ONLY_ACCESS_CONFIRMED=1` after confirming that this
//!   account/location is permitted to access the market-data API without
//!   placing event-contract orders.
//! - `KALSHI_API_KEY_ID`
//! - `KALSHI_PRIVATE_KEY_PATH` or `KALSHI_PRIVATE_KEY_PEM`
//!
//! Run with:
//! `RUN_LIVE_KALSHI_TESTS=1 KALSHI_READ_ONLY_ACCESS_CONFIRMED=1 \
//!  cargo test --test live_kalshi --locked -- --ignored --nocapture`

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use engineering_challenge::{
    config::{KalshiSettings, NetworkSettings},
    exchange::{Exchange, KalshiExchange},
    model::{ExchangeEvent, SourceId},
};
use tokio::time::{Instant, timeout, timeout_at};

#[tokio::test]
#[ignore = "requires authorized Kalshi read-only market-data access and production API credentials"]
async fn discovers_and_receives_live_production_orderbook_snapshots() {
    assert_eq!(
        std::env::var("RUN_LIVE_KALSHI_TESTS").as_deref(),
        Ok("1"),
        "set RUN_LIVE_KALSHI_TESTS=1 to opt in"
    );
    assert_eq!(
        std::env::var("KALSHI_READ_ONLY_ACCESS_CONFIRMED").as_deref(),
        Ok("1"),
        "confirm read-only Kalshi market-data access is permitted for this account and location"
    );

    let mut settings = KalshiSettings {
        key_id: std::env::var("KALSHI_API_KEY_ID")
            .expect("KALSHI_API_KEY_ID must be set for the authenticated websocket handshake"),
        ..KalshiSettings::default()
    };
    if let Ok(path) = std::env::var("KALSHI_PRIVATE_KEY_PATH") {
        settings.private_key_path = Some(PathBuf::from(path));
    }
    // When no path is set, KalshiAuth reads KALSHI_PRIVATE_KEY_PEM directly.
    assert!(
        settings.private_key_path.is_some() || std::env::var_os("KALSHI_PRIVATE_KEY_PEM").is_some(),
        "set KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM"
    );

    let exchange = Arc::new(
        KalshiExchange::new(settings, NetworkSettings::default(), true)
            .expect("constructing authenticated Kalshi production adapter"),
    );
    assert_eq!(exchange.environment(), "production");

    let markets = timeout(Duration::from_secs(45), exchange.discover_markets(5))
        .await
        .expect("Kalshi production market discovery timed out")
        .expect("Kalshi production market discovery failed");
    assert!(
        !markets.is_empty(),
        "Kalshi returned no eligible open markets"
    );
    assert!(markets.iter().all(|market| {
        market.exchange == "kalshi" && !market.native_symbol.is_empty() && !market.symbol.is_empty()
    }));

    let desired: BTreeSet<String> = markets
        .iter()
        .take(3)
        .map(|market| market.native_symbol.clone())
        .collect();
    assert!(!desired.is_empty());

    let factory = Arc::clone(&exchange).session_factory();
    let mut session = factory
        .create_session(SourceId::new(0, 0))
        .await
        .expect("creating Kalshi production session");
    timeout(Duration::from_secs(30), session.connect())
        .await
        .expect("Kalshi authenticated websocket handshake timed out")
        .expect("Kalshi authenticated websocket handshake failed");
    timeout(
        Duration::from_secs(15),
        session.apply_subscriptions(&desired),
    )
    .await
    .expect("Kalshi subscription command timed out")
    .expect("Kalshi subscription command failed");

    let deadline = Instant::now() + Duration::from_secs(75);
    let mut snapshots = 0u64;
    let mut level_changes = 0u64;
    let mut observed_symbols = BTreeSet::new();
    let mut first_snapshot_at = None;

    while Instant::now() < deadline {
        let frame = match timeout_at(deadline, session.next_frame()).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(error)) => panic!("Kalshi websocket frame failed: {error:?}"),
            Err(_) => break,
        };
        for event in frame.events {
            match event {
                ExchangeEvent::Snapshot { native_symbol, .. }
                    if desired.contains(&native_symbol) =>
                {
                    snapshots = snapshots.saturating_add(1);
                    observed_symbols.insert(native_symbol);
                    first_snapshot_at.get_or_insert_with(Instant::now);
                }
                ExchangeEvent::LevelChange { native_symbol, .. }
                    if desired.contains(&native_symbol) =>
                {
                    level_changes = level_changes.saturating_add(1);
                    observed_symbols.insert(native_symbol);
                }
                _ => {}
            }
        }
        // Once a real snapshot has anchored at least one selected market, keep
        // the socket open briefly to observe any subsequent deltas without
        // making a quiet market fail solely because no trade occurred.
        if first_snapshot_at.is_some_and(|started| started.elapsed() >= Duration::from_secs(15)) {
            break;
        }
    }

    session.close().await;
    eprintln!(
        "KALSHI_LIVE_EVIDENCE exchange=kalshi environment=production \
         discovered_markets={} subscribed_markets={} observed_symbols={} \
         snapshots={} level_changes={}",
        markets.len(),
        desired.len(),
        observed_symbols.len(),
        snapshots,
        level_changes
    );
    assert!(
        snapshots > 0,
        "authenticated Kalshi websocket delivered no order-book snapshot for the selected markets"
    );
}

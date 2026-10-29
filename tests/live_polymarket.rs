//! Opt-in public Polymarket integration test.
//!
//! This is ignored by default so offline CI never depends on a
//! third-party network. Run it with:
//! `RUN_LIVE_TESTS=1 cargo test --test live_polymarket -- --ignored --nocapture`

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stdout
)]

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use engineering_challenge::{
    config::PolymarketSettings,
    exchange::{Exchange, PolymarketExchange},
    model::{ExchangeEvent, SourceId},
};
use tokio::time::timeout;

#[tokio::test]
#[ignore = "requires RUN_LIVE_TESTS=1 and public network access"]
async fn discovers_and_subscribes_to_public_polymarket_books() {
    assert_eq!(
        std::env::var("RUN_LIVE_TESTS").as_deref(),
        Ok("1"),
        "set RUN_LIVE_TESTS=1 to run the public-feed integration test"
    );

    let exchange = Arc::new(
        PolymarketExchange::new(
            PolymarketSettings::default(),
            engineering_challenge::config::NetworkSettings::default(),
        )
        .unwrap(),
    );
    let markets = timeout(Duration::from_secs(30), exchange.discover_markets(5))
        .await
        .expect("discovery timed out")
        .expect("discovery failed");
    assert!(!markets.is_empty(), "no public markets discovered");
    assert!(
        markets
            .iter()
            .all(|market| !market.native_symbol.is_empty() && !market.symbol.is_empty())
    );

    let factory = Arc::clone(&exchange).session_factory();
    let mut session = factory
        .create_session(SourceId::new(0, 0))
        .await
        .expect("creating session");
    session.connect().await.expect("connecting websocket");
    let desired: BTreeSet<String> = markets
        .iter()
        .take(3)
        .map(|market| market.native_symbol.clone())
        .collect();
    session
        .apply_subscriptions(&desired)
        .await
        .expect("subscribing");

    let mut saw_snapshot = false;
    for _ in 0..20 {
        let frame = timeout(Duration::from_secs(10), session.next_frame())
            .await
            .expect("timed out waiting for a public frame")
            .expect("reading public frame");
        if frame.events.iter().any(|event| {
            matches!(
                event,
                ExchangeEvent::Snapshot { native_symbol, .. }
                    if desired.contains(native_symbol)
            )
        }) {
            saw_snapshot = true;
            break;
        }
    }
    session.close().await;
    assert!(saw_snapshot, "no snapshot arrived for a subscribed asset");
}

#[tokio::test]
#[ignore = "requires RUN_LIVE_TESTS=1 and public network access"]
async fn resubscribing_an_asset_returns_a_fresh_book() {
    assert_eq!(
        std::env::var("RUN_LIVE_TESTS").as_deref(),
        Ok("1"),
        "set RUN_LIVE_TESTS=1 to run the public-feed integration test"
    );

    let exchange = Arc::new(
        PolymarketExchange::new(
            PolymarketSettings::default(),
            engineering_challenge::config::NetworkSettings::default(),
        )
        .unwrap(),
    );
    let markets = timeout(Duration::from_secs(30), exchange.discover_markets(5))
        .await
        .expect("discovery timed out")
        .expect("discovery failed");
    assert!(!markets.is_empty(), "no public markets discovered");

    let factory = Arc::clone(&exchange).session_factory();
    let mut session = factory
        .create_session(SourceId::new(0, 0))
        .await
        .expect("creating session");
    session.connect().await.expect("connecting websocket");
    let asset = markets[0].native_symbol.clone();
    let desired: BTreeSet<String> = markets
        .iter()
        .take(2)
        .map(|market| market.native_symbol.clone())
        .collect();
    session
        .apply_subscriptions(&desired)
        .await
        .expect("subscribing");

    let mut saw_initial = false;
    for _ in 0..20 {
        let frame = timeout(Duration::from_secs(10), session.next_frame())
            .await
            .expect("timed out waiting for the initial book")
            .expect("reading public frame");
        if frame.events.iter().any(|event| {
            matches!(
                event,
                ExchangeEvent::Snapshot { native_symbol, .. } if *native_symbol == asset
            )
        }) {
            saw_initial = true;
            break;
        }
    }
    assert!(saw_initial, "no initial book for {asset}");

    // A stale probe cycles the subscription; the venue must answer with a
    // fresh authoritative book on the same connection.
    session
        .request_snapshot(std::slice::from_ref(&asset))
        .await
        .expect("requesting a fresh snapshot");

    let mut saw_repair = false;
    for _ in 0..20 {
        let frame = timeout(Duration::from_secs(10), session.next_frame())
            .await
            .expect("timed out waiting for the fresh book")
            .expect("reading public frame");
        if frame.events.iter().any(|event| {
            matches!(
                event,
                ExchangeEvent::Snapshot { native_symbol, .. } if *native_symbol == asset
            )
        }) {
            saw_repair = true;
            break;
        }
    }
    session.close().await;
    assert!(
        saw_repair,
        "resubscribing {asset} did not produce a fresh book"
    );
}

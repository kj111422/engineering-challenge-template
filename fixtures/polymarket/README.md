# Polymarket raw market WebSocket fixtures

These fixtures pin the **legacy raw WebSocket wire format** consumed by
`src/exchange/polymarket/session.rs` at
`wss://ws-subscriptions-clob.polymarket.com/ws/market`. They are not fixtures
for the newer unified-SDK envelope (`topic` / `type` / `payload`). Do not mix
those two contracts in a decoder without an explicit protocol adapter.

## Provenance

- Contract reference: <https://docs.polymarket.com/market-data/realtime-data>
- Sections: raw market WebSocket connection, Order Book, Price Change, Tick
  Size Change, Best Bid and Ask, New Market, and Market Resolved.
- The documentation page was checked on 2026-10-27.
- Values and identifiers are illustrative/sanitized examples taken from the
  published examples or adapted for deterministic tests. These files are not
  claimed to be captured live traffic.

## Coverage

| Fixture | Expected behavior |
|---|---|
| `book_snapshot.json` | Decode an authoritative two-sided snapshot and timestamp. |
| `price_change_multi_asset.json` | Decode every absolute level update in one frame; preserve each asset and side. |
| `tick_size_change.json` | Decode a positive new tick size as a lifecycle event. |
| `new_market.json` | Expand a lifecycle event over its asset identifiers. |
| `market_resolved.json` | Emit terminal lifecycle events for each asset identifier. |
| `best_bid_ask.json` | Ignore the event for book mutation; it is not a sequence number. |
| `unknown_additive_event.json` | Count/log an unknown event type without tearing down the feed. |
| `malformed_price_change_atomic.json` | Reject the entire malformed batch; do not return a partial vector. |

Control text frames (`PING` / `PONG`) are tested at the session layer; they are
not JSON and must not be sent to the JSON event decoder.

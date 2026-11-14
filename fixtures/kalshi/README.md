# Kalshi raw market WebSocket fixtures

These fixtures pin the **event-contract WebSocket wire format** consumed by
`src/exchange/kalshi/codec.rs` over the `orderbook_delta` channel. Every
fixture is `include_str!`-pinned into the decoder tests, so a format change
either updates these files together with the decoder or fails the build.

## Provenance

- Contract reference: the Kalshi Exchange API WebSocket documentation
  (`orderbook_delta`, `ticker`, and `market_lifecycle_v2` channels, plus the
  `subscribed` / `ok` / `error` / `unsubscribed` control acknowledgements).
- Checked against the published documentation on 2026-11-14.
- Values and identifiers are illustrative/sanitized examples adapted for
  deterministic tests. These files are not claimed to be captured live
  traffic; live-captured evidence lives under `measurements/` and
  `captures/` with its own provenance chain.

## Coverage

| Fixture | Expected behavior |
|---|---|
| `book_snapshot.json` | Decode a modern two-sided `yes_dollars_fp`/`no_dollars_fp` snapshot; NO prices invert to the YES-ask scale (`1 - p`); `clock` and `sending_ts_ms` map to the event metadata. |
| `book_snapshot_legacy.json` | Decode the legacy integer cent/contract level arrays; absent timestamps leave `exchange_ts_us` unset rather than zero. |
| `orderbook_delta_yes.json` | Decode a YES-side delta; `msg.ts_ms` takes precedence over the envelope `sending_ts_ms`. |
| `orderbook_delta_no_inverted.json` | Decode a NO-side delta with a negative quantity and the price inversion to the YES scale. |
| `market_lifecycle_activated.json` | Decode a lifecycle event: `event_type`, `close_ts` (epoch seconds) and the title from `additional_metadata`. |
| `ticker_channel_ignored.json` | A documented non-order-book channel frame is classified `Ignore` and never mutates a book or tears down the feed. |
| `control_error.json` | A control error frame decodes to its code and message instead of failing the connection. |
| `malformed_delta_missing_fields.json` | A delta missing the mandatory `delta_fp` field is rejected outright; a partial application is impossible. |

Control text frames are JSON in this protocol and are decoded; there is no
PING/PONG text frame at this layer.

## Not covered here: the margin (perps) API

Kalshi's perpetual-futures surface is a **separate API** that shares
authentication and message-shape conventions but differs where it matters:

- REST base `.../trade-api/v2/margin/` (demo:
  `external-api.demo.kalshi.co`), WebSocket
  `wss://external-api-margin-ws.*.kalshi.co/trade-api/ws/v2/margin`, and
  FIX hosts of its own.
- All timestamps are Unix epoch milliseconds with an `_ms` suffix
  (`ts_ms`, `created_ts_ms`, ...); the event-contract frames above mix
  RFC3339 datetimes and epoch seconds.
- Margin-only channels (`corporate_actions`, `order_group_updates`) and
  margin-only REST endpoints (balance, funding, transfers) do not exist on
  the event-contract API.

Do not feed margin frames into this decoder without an explicit protocol
adapter; these fixtures pin only the event-contract channel that
`src/exchange/kalshi/session.rs` consumes today.

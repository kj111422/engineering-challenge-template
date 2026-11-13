# Live market-data evidence runbook

This runbook is for **read-only market-data validation**. The collector does
not place orders. It connects directly to each venue, discovers a small set of
open markets, subscribes to one websocket source, records an SBE capture and a
raw-frame journal, and verifies both outputs.

## Access conditions

Run from the machine and network on which you are physically located. This
procedure does not use a VPN, proxy, hosted runner, or other alternate egress
path to bypass a venue's geographic restrictions. The application defaults to
direct egress.

Before running, check the venues' current terms and applicable local law:

- [Polymarket geographic restrictions](https://help.polymarket.com/en/articles/13364163-geographic-restrictions)
  lists countries and regions where access is blocked and explicitly forbids
  VPN-based circumvention. India is not listed on the current published blocked
  country list, but that is not legal advice or a substitute for checking the
  current terms and local requirements.
- [Kalshi Member Agreement](https://kalshi.com/docs/kalshi-member-agreement.pdf),
  Section VI, lists India as restricted for event-contract trading. The same
  section says those restrictions apply solely to trading and do not, by
  themselves, prohibit non-trading access, subject to applicable law and
  Kalshi policy. This repository therefore requires an explicit operator
  confirmation for Kalshi read-only API access; obtain clarification from
  Kalshi where necessary. Do not use the event-contract API to place, modify,
  or cancel orders.

## Prerequisites

- Rust toolchain pinned by `rust-toolchain.toml` (1.99.1).
- `cargo`, `git`, `python3`, Bash, and a direct network path to each venue.
- For Kalshi: a valid API key ID and private key, plus confirmation that
  authenticated read-only market-data API use is permitted for the account and
  the machine's location.

Set the two explicit operator attestations only after checking the current
venue terms and applicable law:

```bash
export POLYMARKET_MARKET_DATA_ACCESS_CONFIRMED=1
export KALSHI_MARKET_DATA_ACCESS_CONFIRMED=1
```

Kalshi credentials are read from environment variables. Prefer a protected
PEM file path; a PEM environment variable is also supported:

```bash
export KALSHI_API_KEY_ID='your-api-key-id'
export KALSHI_PRIVATE_KEY_PATH="$HOME/.config/kalshi/private-key.pem"
# Alternatively, use KALSHI_PRIVATE_KEY_PEM from a secret manager.
```

Never put the private key in TOML, source control, a command-line argument, or
an evidence artifact. Do not paste secrets into logs or issue reports.

## Run both venues

From the repository root:

```bash
LIVE_DURATION_SECS=120 LIVE_MAX_MARKETS=5 scripts/run-live-evidence.sh
```

The script builds the release binary once, then runs the two venue captures
sequentially with one shard and one websocket replica. It creates a timestamped
directory under `captures/` containing, per venue:

- `orderbook.sbe` — normalized FIX SBE market-data capture;
- `markets.json` — native-to-normalized symbol metadata;
- `report.json` — collector run and source counters;
- `journal/` — raw websocket frames and checksummed journal manifest;
- `sbe-verify.json` — snapshot/delta reconstruction verification;
- `completeness.json` — per-symbol snapshot anchoring verification;
- `journal-verify.json` — journal checksum and finalization verification;
- `evidence.json` — SHA-256 hashes and summarized run counters.

The root `run-manifest.json` records the source commit, run times, venue
statuses, and hashes of the two evidence manifests. It contains no API keys,
private keys, or proxy credentials.

The script only marks a venue `verified` when discovery returned symbols,
at least one exchange-originated snapshot was processed, SBE messages were
written, snapshot/delta reconstruction is valid, each symbol's observed delta
stream is anchored, and the raw journal finalized with nonzero verified
records. A short run may legitimately observe zero subsequent price-level
deltas in a quiet market; that count is recorded rather than fabricated.

A skipped venue is not a successful dual-venue run. A failed verification is
not a successful capture. Keep the entire output directory and publish its
`run-manifest.json` together with the source commit when reporting results.

## Opt-in live integration tests

The test suite has separate venue tests:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket --locked -- --ignored --nocapture
RUN_LIVE_KALSHI_TESTS=1 \
KALSHI_READ_ONLY_ACCESS_CONFIRMED=1 \
cargo test --test live_kalshi --locked -- --ignored --nocapture
```

The workflow `.github/workflows/live.yml` uses an operator-managed self-hosted
runner labelled `market-data-eligible`. The runner must be physically located
in a jurisdiction where the relevant data access is permitted. The workflow
requires `refs/heads/main` and targets the protected GitHub Environment
`live-market-data`. In repository Settings → Environments, configure that
environment to allow only `main` and require reviewer approval. Store these as
environment-scoped secrets/variables rather than general repository secrets:

- Variable `POLYMARKET_MARKET_DATA_ACCESS_CONFIRMED=1`, set only after checking
  current Polymarket terms and applicable local law.
- Secret `KALSHI_API_KEY_ID`.
- Secret `KALSHI_PRIVATE_KEY_PEM`.
- Variable `KALSHI_READ_ONLY_ACCESS_CONFIRMED=1`, set only after confirming
  that Kalshi permits read-only market-data access for the account and runner
  location.

A self-hosted runner label is an operator control, not a geographic
verification service. Environment branch restrictions and required reviewers
must be configured in GitHub Settings; the YAML alone cannot enforce them.

## Evidence boundary

A verified capture proves that this collector received and recorded the frames
visible on that connection during the stated interval. It does not prove that
the venue delivered every market event, that the whole venue universe was
subscribed, that the feed will remain available, or that order entry is
permitted or safe. A `network-probe` success alone is transport evidence, not
live market-data evidence.

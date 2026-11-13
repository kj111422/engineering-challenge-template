# Live market-data evidence runbook

This runbook is for **read-only market-data validation**. The collector does
not place orders. It connects directly to each venue, discovers a small set of
open markets, subscribes to one websocket source, records an SBE capture and a
raw-frame journal, and verifies both outputs.

## Access conditions

Run only from the machine and network on which you are physically located, and
only where access to the venue is permitted. This procedure does not use a VPN,
proxy, alternate DNS, hosted runner, or other alternate egress path to bypass a
venue's geographic restrictions. The application defaults to direct egress.

**A venue's published blocklist is not proof that access is permitted.**
Geoblocking lists are incomplete, change independently of the law, and do not
establish that read-only market-data access is allowed. Check the current
official documents and any applicable local law before running; this runbook is
not legal advice.

- [Polymarket geographic restrictions](https://help.polymarket.com/en/articles/13364163-geographic-restrictions)
  currently lists 40 blocked countries and regions and explicitly prohibits
  VPN-based circumvention. India is not on that list, but that absence does not
  establish permission. An Indian Ministry of Electronics and Information
  Technology advisory dated 25 April 2026 names Polymarket and similar
  prediction-market platforms as already blocked for access in India under
  section 69A of the Information Technology Act, 2000, and warns VPN providers
  and intermediaries against facilitating access or circumvention. If you are
  in India, do not run the Polymarket venue unless you have documented
  confirmation that the specific read-only access is permitted.
- The [Kalshi Member Agreement](https://kalshi.com/docs/kalshi-member-agreement.pdf)
  (v1.6 at the time of writing) prohibits access, use, or trading where
  applicable law or Kalshi policy prohibits it, but it does not enumerate every
  restricted jurisdiction. The absence of India from its Restricted
  Jurisdictions list therefore does not establish permission either. Treat
  event-contract trading as restricted in India, and obtain explicit
  confirmation from Kalshi that authenticated read-only market-data access is
  permitted for your account and location before enabling it. Do not use the
  event-contract API to place, modify, or cancel orders.

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
export KALSHI_READ_ONLY_ACCESS_CONFIRMED=1
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

From the repository root, for a small smoke run:

```bash
LIVE_DURATION_SECS=120 LIVE_MAX_MARKETS=5 scripts/run-live-evidence.sh
```

For the full discovered universe (operator and venue limits permitting):

```bash
LIVE_EVIDENCE_SCOPE=universe LIVE_MAX_MARKETS=0 \
LIVE_DURATION_SECS=900 scripts/run-live-evidence.sh
```

The script builds the release binary once, then runs the two venue captures
sequentially. By default a smoke run uses one shard and one websocket replica.
`LIVE_SHARDS=0` derives the shard count from the discovered symbol set, and
`LIVE_REPLICAS` may be raised to test redundancy. It creates a timestamped
directory under `captures/` containing, per venue:

- `orderbook.sbe` — normalized FIX SBE market-data capture;
- `markets.json` — native-to-normalized symbol metadata;
- `report.json` — collector run, source counters, and the acknowledged
  subscription set;
- `journal/` — raw websocket frames and checksummed journal manifest;
- `sbe-verify.json` — snapshot/delta reconstruction verification;
- `completeness.json` — per-symbol snapshot anchoring verification;
- `coverage.json` — expected vs acknowledged vs snapshot-anchored per-symbol
  certificate, including explicit missing and extra symbol lists;
- `journal-verify.json` — journal checksum and finalization verification;
- `evidence.json` — SHA-256 hashes and summarized run counters.

The root `run-manifest.json` records the source commit, run times, evidence
tier, scope, venue statuses, and hashes of the two evidence manifests. It
contains no API keys, private keys, or proxy credentials.

The script only marks a venue `verified` when discovery returned symbols,
at least one exchange-originated snapshot was processed, SBE messages were
written, snapshot/delta reconstruction is valid, `coverage` reports
`valid = true` for every expected symbol, and the raw journal finalized with
nonzero verified records. A short run may legitimately observe zero subsequent
price-level deltas in a quiet market; that count is recorded rather than
fabricated.

### Evidence tiers

The script never upgrades a small run into a universe claim:

| Tier | Emitted when | Scope |
|---|---|---|
| `LIVE_SMOKE_OK` | Both venues pass for the selected small market set (`LIVE_EVIDENCE_SCOPE=smoke`, the default) | Selected symbols only |
| `LIVE_UNIVERSE_OK` | Both venues pass with `LIVE_EVIDENCE_SCOPE=universe` and `LIVE_MAX_MARKETS=0`, and `coverage.expected` equals the full discovered universe | Every eligible discovered symbol |
| `LIVE_SOAK_OK` | A full-universe run holds correctness and stability for the stated soak duration | Reserved; this script does not emit it |

`LIVE_UNIVERSE_OK` requires the universe scope explicitly; the five-market
smoke path cannot produce it. A green integration test is not an evidence
tier either. A skipped venue is not a successful dual-venue run. A failed
verification is not a successful capture. Keep the entire output directory and
publish its `run-manifest.json` together with the source commit when reporting
results.

### Per-symbol coverage certificate

`coverage.json` enforces one invariant for each venue:

```
expected symbols = acknowledged symbols = snapshot-anchored symbols
```

- **Expected** comes from `markets.json`, the metadata sidecar for the run's
  selected market set.
- **Acknowledged** is the union of each worker's last successfully applied
  subscription set, recorded in `report.json` and mapped from native exchange
  identifiers to normalized SBE symbols through `markets.json`. It proves the
  session transmitted the subscription request; it is not a claim that the
  venue replied with a wire acknowledgement.
- **Snapshot-anchored** means the symbol appears in at least one snapshot in
  `orderbook.sbe`. A snapshot with an empty book still counts because it
  establishes the baseline. A symbol seen only in price-level updates does not
  count and is listed under `observed_not_anchored`.

The certificate is `valid` only when the expected set is non-empty, every
expected symbol was acknowledged and anchored, no acknowledged symbol is
outside the expected set, no expected symbol is duplicated, every
acknowledged native identifier maps to metadata, and the capture decodes
exactly as many messages as the run report's writer accepted. A truncated
capture cannot certify as complete: bytes lost before shutdown fail the
certificate instead of being silently absorbed. Captured symbols outside the
expected set are listed under `anchored_not_expected` and accounted separately;
these are typically markets retired by a later discovery round.

Run the same check offline:

```bash
target/release/engineering-challenge coverage \
  captures/live-<run>/<venue>/orderbook.sbe \
  captures/live-<run>/<venue>/markets.json \
  captures/live-<run>/<venue>/report.json \
  --out captures/live-<run>/<venue>/coverage.json
```

The command exits non-zero unless the certificate is valid. A five-market
smoke capture can therefore pass `coverage` while still being only
`LIVE_SMOKE_OK`; the tier is about scope, not per-symbol correctness.

## Opt-in live integration tests

The test suite has separate venue tests:

```bash
RUN_LIVE_TESTS=1 cargo test --test live_polymarket --locked -- --ignored --nocapture
RUN_LIVE_KALSHI_TESTS=1 \
KALSHI_READ_ONLY_ACCESS_CONFIRMED=1 \
cargo test --test live_kalshi --locked -- --ignored --nocapture
```

Both venue tests require every selected symbol to receive its own snapshot. A
single snapshot for one symbol is not a pass, even if the socket is otherwise
healthy.

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

After both venue tests, the workflow runs `scripts/run-live-evidence.sh` and
uploads the complete `captures/live-*` directory as an artifact named
`live-evidence-<run_id>-<run_attempt>`, retained for 90 days. That artifact is
the evidence bundle: the SBE captures, raw journals, verification reports, and
`run-manifest.json`. A green test step alone is not a retained live-evidence
result, and a failed run still uploads whatever it produced for review.

## Evidence boundary

A verified capture proves that this collector received and recorded the frames
visible on that connection during the stated interval. It does not prove that
the venue delivered every market event, that the whole venue universe was
subscribed, that the feed will remain available, or that order entry is
permitted or safe. A `network-probe` success alone is transport evidence, not
live market-data evidence.

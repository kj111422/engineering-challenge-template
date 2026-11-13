#!/usr/bin/env bash
# Collect short, read-only production/public market-data captures from both
# venues and verify SBE, per-symbol anchoring, and raw-frame journal integrity.
# No proxy is configured: the application defaults to direct egress.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DURATION_SECS="${LIVE_DURATION_SECS:-120}"
MAX_MARKETS="${LIVE_MAX_MARKETS:-5}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_ROOT="${LIVE_EVIDENCE_DIR:-$ROOT/captures/live-$STAMP}"
case "$OUT_ROOT" in /*) ;; *) OUT_ROOT="$ROOT/$OUT_ROOT" ;; esac
BIN="$ROOT/target/release/engineering-challenge"

if ! command -v cargo >/dev/null 2>&1; then
  echo "ERROR: cargo is required (install the repository-pinned Rust 1.99.1 toolchain)." >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required to assemble the evidence manifest." >&2
  exit 2
fi
if ! command -v git >/dev/null 2>&1; then
  echo "ERROR: git is required to record the source commit." >&2
  exit 2
fi
if [[ ! "$DURATION_SECS" =~ ^[0-9]+$ ]] || (( DURATION_SECS < 30 )); then
  echo "ERROR: LIVE_DURATION_SECS must be an integer >= 30." >&2
  exit 2
fi
if [[ ! "$MAX_MARKETS" =~ ^[0-9]+$ ]] || (( MAX_MARKETS < 1 )); then
  echo "ERROR: LIVE_MAX_MARKETS must be an integer >= 1." >&2
  exit 2
fi

COMMIT="$(git rev-parse HEAD 2>/dev/null || true)"
if [[ -z "$COMMIT" ]]; then
  echo "ERROR: run this script from a Git working copy so evidence can name the exact commit." >&2
  exit 2
fi
if [[ -n "$(git status --porcelain)" ]]; then
  echo "ERROR: the working tree must be clean so the evidence commit identifies the exact source tree." >&2
  git status --short >&2
  exit 2
fi

mkdir -p "$OUT_ROOT" || { echo "ERROR: cannot create evidence directory $OUT_ROOT" >&2; exit 1; }
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export KALSHI_ENVIRONMENT=production

# Build once. Keep the compiler output as part of the run directory.
if ! cargo build --release --locked >"$OUT_ROOT/build.log" 2>&1; then
  echo "ERROR: release build failed; see $OUT_ROOT/build.log" >&2
  tail -n 80 "$OUT_ROOT/build.log" >&2
  exit 1
fi
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: release binary was not produced at $BIN" >&2
  exit 1
fi

run_venue() {
  local venue="$1"
  local directory="$OUT_ROOT/$venue"
  local capture="$directory/orderbook.sbe"
  local metadata="$directory/markets.json"
  local report="$directory/report.json"
  local journal_root="$directory/journal"
  local journal_run

  mkdir -p "$directory" || { echo "[$venue] cannot create $directory" >&2; return 1; }
  echo "[$venue] starting $DURATION_SECS-second market-data capture"
  if ! "$BIN" \
    --exchange "$venue" \
    --output "$capture" \
    --metadata "$metadata" \
    --report "$report" \
    --journal-dir "$journal_root" \
    --max-markets "$MAX_MARKETS" \
    --duration-secs "$DURATION_SECS" \
    --shards 1 \
    --replicas 1 \
    collect >"$directory/collector.log" 2>&1; then
    echo "[$venue] collector failed; see $directory/collector.log" >&2
    tail -n 60 "$directory/collector.log" >&2
    return 1
  fi

  if [[ ! -s "$capture" || ! -s "$metadata" || ! -s "$report" ]]; then
    echo "[$venue] expected capture, metadata, or run report is missing/empty" >&2
    return 1
  fi

  if ! "$BIN" verify "$capture" >"$directory/sbe-verify.json" 2>"$directory/sbe-verify.log"; then
    echo "[$venue] SBE verification failed; see $directory/sbe-verify.json and .log" >&2
    return 1
  fi
  if ! "$BIN" completeness "$capture" --out "$directory/completeness.json" \
      >"$directory/completeness.stdout.json" 2>"$directory/completeness.log"; then
    echo "[$venue] per-symbol completeness failed; see $directory/completeness.json" >&2
    return 1
  fi

  journal_run="$(python3 - "$report" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as stream:
    report = json.load(stream)
journal = report.get("journal") or {}
print(journal.get("run_directory", ""))
PY
)" || { echo "[$venue] could not read journal path from report" >&2; return 1; }
  if [[ -z "$journal_run" ]]; then
    echo "[$venue] report does not name a raw-frame journal run" >&2
    return 1
  fi
  if [[ "$journal_run" != /* ]]; then
    journal_run="$ROOT/$journal_run"
  fi
  if ! "$BIN" journal verify "$journal_run" >"$directory/journal-verify.json" 2>"$directory/journal-verify.log"; then
    echo "[$venue] raw-frame journal verification failed; see $directory/journal-verify.json" >&2
    return 1
  fi

  if ! python3 - "$venue" "$report" "$directory/sbe-verify.json" \
      "$directory/completeness.json" "$directory/journal-verify.json" <<'PY'
import json, sys
venue, report_path, sbe_path, completeness_path, journal_path = sys.argv[1:]
load = lambda path: json.load(open(path, encoding="utf-8"))
report, sbe, completeness, journal = map(load, (report_path, sbe_path, completeness_path, journal_path))
checks = {
    "venue_matches": report.get("exchange") == venue,
    "discovery_nonempty": int(report.get("discovered_symbols", 0)) > 0,
    "exchange_snapshot_observed": int(report.get("core", {}).get("exchange_snapshots", 0)) > 0,
    "sbe_messages_written": int(report.get("writer", {}).get("messages", 0)) > 0,
    "sbe_reconstruction_valid": sbe.get("valid") is True,
    "symbol_stream_anchored": completeness.get("valid") is True,
    "journal_complete": journal.get("complete") is True,
    "journal_records_nonzero": int(journal.get("record_count", 0)) > 0,
}
for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")
if not all(checks.values()):
    sys.exit(1)
print(
    f"LIVE_FEED_OK venue={venue} discovered={report['discovered_symbols']} "
    f"exchange_snapshots={report['core']['exchange_snapshots']} "
    f"deltas={report['core'].get('deltas_emitted', 0)} "
    f"sbe_messages={report['writer']['messages']} "
    f"journal_records={journal['record_count']}"
)
PY
  then
    echo "[$venue] one or more evidence checks failed" >&2
    return 1
  fi

  if ! python3 - "$directory" "$venue" "$journal_run" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
venue = sys.argv[2]
journal_run = pathlib.Path(sys.argv[3])
def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
report = json.loads((root / "report.json").read_text(encoding="utf-8"))
verify = json.loads((root / "sbe-verify.json").read_text(encoding="utf-8"))
complete = json.loads((root / "completeness.json").read_text(encoding="utf-8"))
journal = json.loads((root / "journal-verify.json").read_text(encoding="utf-8"))
journal_files = [journal_run / "manifest.json", *sorted(journal_run.glob("*.pmj"))]
manifest = {
    "schema_version": 1,
    "venue": venue,
    "environment": report.get("environment"),
    "exchange": report.get("exchange"),
    "discovered_symbols": report.get("discovered_symbols"),
    "exchange_snapshots": report.get("core", {}).get("exchange_snapshots"),
    "deltas_emitted": report.get("core", {}).get("deltas_emitted"),
    "sbe_messages": report.get("writer", {}).get("messages"),
    "journal_records": journal.get("record_count"),
    "sbe_valid": verify.get("valid"),
    "completeness_valid": complete.get("valid"),
    "journal_complete": journal.get("complete"),
    "journal_directory": str(journal_run),
    "journal_files": [
        {"path": str(path.relative_to(journal_run)), "sha256": sha(path)}
        for path in journal_files if path.is_file()
    ],
    "artifacts": {
        name: {"path": name, "sha256": sha(root / name)}
        for name in ("orderbook.sbe", "markets.json", "report.json", "sbe-verify.json", "completeness.json", "journal-verify.json")
    },
}
(root / "evidence.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
PY
  then
    echo "[$venue] could not assemble evidence manifest" >&2
    return 1
  fi
  echo "[$venue] VERIFIED — artifacts: $directory"
  return 0
}

POLYMARKET_STATUS="not_run"
KALSHI_STATUS="not_run"
FAILURES=0

if [[ "${POLYMARKET_MARKET_DATA_ACCESS_CONFIRMED:-0}" == "1" ]]; then
  if run_venue polymarket; then POLYMARKET_STATUS="verified"; else POLYMARKET_STATUS="failed"; FAILURES=$((FAILURES + 1)); fi
else
  POLYMARKET_STATUS="skipped_permission_not_confirmed"
  echo "[polymarket] skipped: set POLYMARKET_MARKET_DATA_ACCESS_CONFIRMED=1 only after checking current venue terms and local law"
fi

if [[ "${KALSHI_MARKET_DATA_ACCESS_CONFIRMED:-0}" == "1" ]]; then
  if [[ -z "${KALSHI_API_KEY_ID:-}" ]]; then
    KALSHI_STATUS="blocked_missing_api_key_id"
    echo "[kalshi] blocked: KALSHI_API_KEY_ID is not set" >&2
    FAILURES=$((FAILURES + 1))
  elif [[ -z "${KALSHI_PRIVATE_KEY_PATH:-}" && -z "${KALSHI_PRIVATE_KEY_PEM:-}" ]]; then
    KALSHI_STATUS="blocked_missing_private_key"
    echo "[kalshi] blocked: set KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY_PEM" >&2
    FAILURES=$((FAILURES + 1))
  elif [[ -n "${KALSHI_PRIVATE_KEY_PATH:-}" && ! -r "$KALSHI_PRIVATE_KEY_PATH" ]]; then
    KALSHI_STATUS="blocked_unreadable_private_key"
    echo "[kalshi] blocked: KALSHI_PRIVATE_KEY_PATH is not readable" >&2
    FAILURES=$((FAILURES + 1))
  else
    if run_venue kalshi; then KALSHI_STATUS="verified"; else KALSHI_STATUS="failed"; FAILURES=$((FAILURES + 1)); fi
  fi
else
  KALSHI_STATUS="skipped_permission_not_confirmed"
  echo "[kalshi] skipped: set KALSHI_MARKET_DATA_ACCESS_CONFIRMED=1 only after confirming read-only API access is allowed for this location/account"
fi

COMPLETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if ! python3 - "$OUT_ROOT" "$COMMIT" "$STARTED_AT" "$COMPLETED_AT" "$DURATION_SECS" "$MAX_MARKETS" "$POLYMARKET_STATUS" "$KALSHI_STATUS" <<'PY'
import json, pathlib, sys
out, commit, started, completed, duration, max_markets, pm_status, kalshi_status = sys.argv[1:]
root = pathlib.Path(out)
manifest = {
    "schema_version": 1,
    "source_commit": commit,
    "started_at_utc": started,
    "completed_at_utc": completed,
    "duration_seconds_per_venue": int(duration),
    "max_markets_per_venue": int(max_markets),
    "egress_policy": "direct (application default; no proxy configured by this script)",
    "order_entry": "not used",
    "venues": {},
}
for venue, status in (("polymarket", pm_status), ("kalshi", kalshi_status)):
    item = {"status": status}
    evidence = root / venue / "evidence.json"
    if evidence.is_file():
        item["evidence"] = str(evidence.relative_to(root))
        item["evidence_manifest_sha256"] = __import__("hashlib").sha256(evidence.read_bytes()).hexdigest()
    manifest["venues"][venue] = item
(root / "run-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print(json.dumps(manifest, indent=2))
PY
then
  echo "ERROR: could not assemble the dual-venue run manifest" >&2
  exit 1
fi

if (( FAILURES > 0 )); then
  echo "Live evidence run incomplete; inspect $OUT_ROOT/run-manifest.json" >&2
  exit 1
fi
if [[ "$POLYMARKET_STATUS" != "verified" || "$KALSHI_STATUS" != "verified" ]]; then
  echo "No dual-venue evidence yet: one or both venues were skipped. See $OUT_ROOT/run-manifest.json" >&2
  exit 2
fi

echo "DUAL_VENUE_LIVE_EVIDENCE_OK: $OUT_ROOT"
